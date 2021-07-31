package rabbitmq

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

/**
 * @author : Donald Trieu
 * @created : 7/31/21, Saturday
**/

//MessageBody is the struct for the body passed in the AMQP message. The type will be set on the Request header
type RabbitMessage struct {
	Body []byte `json:"body"`
}

type RabbitService struct {
	name               string
	exchangeName       string
	addr               string
	logger             *log.Logger
	connection         *amqp.Connection
	channel            *amqp.Channel
	messageChannel     chan amqp.Delivery
	done               chan bool
	notifyConnClose    chan *amqp.Error
	notifyChanClose    chan *amqp.Error
	notifyConfirm      chan amqp.Confirmation
	isReady            bool
	subscribeQueueName map[string]bool
	routingKey         map[string][]string
}

type IRabbitService interface {
	Subscribe(queueName string, bindingKey []string, rePush bool) error
	FetchMessage() (amqp.Delivery, RabbitMessage, bool)
	Push(routingKey string, data []byte) error
	Close() error
	IsReady() bool
}

const (
	MaxInFlyMessage = 20

	// When reconnecting to the server after connection failure
	reconnectDelay = 5 * time.Second

	// When setting up the channel after a channel exception
	reInitDelay = 2 * time.Second

	// When resending messages the server didn't confirm
	resendDelay = 5 * time.Second
)

var (
	errNotConnected  = errors.New("not connected to a server")
	errAlreadyClosed = errors.New("already closed: not connected to the server")
	errShutdown      = errors.New("session is shutting down")
)

func NewRabbitService(name string, exchangeName string, addr string) *RabbitService {
	rabbitService := RabbitService{
		logger:       log.New(os.Stdout, "", log.LstdFlags),
		name:         name,
		exchangeName: exchangeName,
		addr:         addr,
	}
	rabbitService.setupData()
	go rabbitService.HandleReconnect()
	return &rabbitService
}

func (r *RabbitService) setupData() {
	r.messageChannel = make(chan amqp.Delivery, MaxInFlyMessage)
	r.subscribeQueueName = make(map[string]bool)
	r.routingKey = map[string][]string{}
	r.done = make(chan bool)
	r.isReady = false
}

func (r *RabbitService) IsReady() bool {
	return r.isReady
}

func (r *RabbitService) HandleReconnect() {
	for {
		r.isReady = false
		log.Println("Attempting to connect")

		// close connection if exists
		if r.connection != nil {
			r.isReady = false
			_ = r.Close()
		}

		conn, err := r.connect(r.addr)

		if err != nil {
			log.Println("Failed to connect. Retrying...")

			select {
			case <-r.done:
				return
			case <-time.After(reconnectDelay):
			}
			continue
		}
		done := r.handleReInit(conn)
		if done {
			break
		}
	}
}

// handleReconnect will wait for a channel error
// and then continuously attempt to re-initialize both channels
func (r *RabbitService) handleReInit(conn *amqp.Connection) bool {
	for {
		r.isReady = false

		err := r.init(conn)

		if err != nil {
			log.Println("Failed to initialize channel. Retrying...")

			select {
			case <-r.done:
				return true
			case <-time.After(reInitDelay):
			}
			continue
		}

		// resubscribe all queue
		log.Println("ReSubscribe queue")
		for queueName, rePush := range r.subscribeQueueName {
			_ = r.Subscribe(queueName, r.routingKey[queueName], rePush)
		}

		select {
		case <-r.done:
			return true
		case <-r.notifyConnClose:
			log.Println("Connection closed. Reconnecting...")
			return false
		case <-r.notifyChanClose:
			log.Println("Channel closed. Re-running init...")
		}
	}
}

// init will initialize channel & declare exchange
func (r *RabbitService) init(conn *amqp.Connection) error {

	ch, err := conn.Channel()

	if err != nil {
		return err
	}

	if err = ch.ExchangeDeclare(
		r.exchangeName, // name
		"direct",       // type
		true,           // durable
		false,          // auto-deleted
		false,          // internal
		false,          // noWait
		nil,            // arguments
	); err != nil {
		return fmt.Errorf("Error in Exchange Declare: %s", err)
	}

	err = ch.Confirm(false)

	if err != nil {
		return err
	}

	r.changeChannel(ch)
	r.isReady = true

	log.Println("Setup!")

	return nil
}

// connect will create a new AMQP connection
func (r *RabbitService) connect(addr string) (*amqp.Connection, error) {
	conn, err := amqp.Dial(addr)

	if err != nil {
		return nil, err
	}

	r.changeConnection(conn)
	log.Println("Connected!")
	return conn, nil
}

// changeConnection takes a new connection to the queue,
// and updates the close listener to reflect this.
func (r *RabbitService) changeConnection(connection *amqp.Connection) {
	r.connection = connection
	r.notifyConnClose = make(chan *amqp.Error)
	r.connection.NotifyClose(r.notifyConnClose)
}

// changeChannel takes a new channel to the queue,
// and updates the channel listeners to reflect this.
func (r *RabbitService) changeChannel(channel *amqp.Channel) {
	r.channel = channel
	r.notifyChanClose = make(chan *amqp.Error)
	r.notifyConfirm = make(chan amqp.Confirmation, 1)
	r.channel.NotifyClose(r.notifyChanClose)
	r.channel.NotifyPublish(r.notifyConfirm)
}

func (r *RabbitService) Subscribe(queueName string, bindingKey []string, rePush bool) error {
	r.subscribeQueueName[queueName] = rePush
	r.routingKey[queueName] = bindingKey
	_, err := r.channel.QueueDeclare(
		queueName,
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)

	if err != nil {
		return err
	}

	for _, key := range bindingKey {
		if err := r.channel.QueueBind(queueName, key, r.exchangeName, false, nil); err != nil {
			return fmt.Errorf("Queue  Bind error: %s", err)
		}
	}

	c, err := r.channel.Consume(
		queueName, // queue
		"",        // consumer
		false,     // auto-ack
		false,     // exclusive
		false,     // no-local
		false,     // no-wait
		nil,       // args
	)

	if err != nil {
		return err
	}
	fmt.Printf(`
	---------
	[PullerRabbitService] subscribe for %v
	---------

	`, queueName)
	go func() {
		for r.isReady {
			select {
			case msg := <-c:
				r.messageChannel <- msg
			case e := <-r.notifyConnClose:
				fmt.Printf(`
					---------
					[PullerRabbitService] stop subscribe for %v %v
					---------
			
					`, queueName, e,
				)
				return
			case <-r.done:
				return
			}
		}
	}()
	return nil
}

func (r *RabbitService) FetchMessage() (amqp.Delivery, RabbitMessage, bool) {
	for {
		delivery := <-r.messageChannel
		var message RabbitMessage
		err := json.Unmarshal(delivery.Body, &message)
		if err != nil {
			return amqp.Delivery{}, RabbitMessage{}, false
		}
		return delivery, message, true
	}
}

func (r *RabbitService) Push(routingKey string, data []byte) error {
	if !r.isReady {
		return errors.New("failed to push: not connected")
	}
	for {
		err := r.UnsafePush(routingKey, data)
		if err != nil {
			r.logger.Println("Push failed. Retrying...")
			select {
			case <-r.done:
				return errShutdown
			case <-time.After(resendDelay):
			}
			continue
		}
		select {
		case confirm := <-r.notifyConfirm:
			if confirm.Ack {
				r.logger.Println("Push confirmed!")
				return nil
			}
		case <-time.After(resendDelay):
		}
		r.logger.Println("Push didn't confirm. Retrying...")
	}
}

func (r *RabbitService) UnsafePush(routingKey string, data []byte) error {
	if !r.isReady {
		return errNotConnected
	}

	return r.channel.Publish(
		r.exchangeName, // Exchange
		routingKey,     // Routing key
		false,          // Mandatory
		false,          // Immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		})
}

// Close will cleanly shutdown the channel and connection.
func (r *RabbitService) Close() error {
	if !r.isReady {
		return errAlreadyClosed
	}
	err := r.channel.Close()
	if err != nil {
		return err
	}
	err = r.connection.Close()
	if err != nil {
		return err
	}
	close(r.done)
	r.isReady = false
	return nil
}

