package rabbitmq

import (
	"fmt"
	"github.com/reactivex/rxgo/observable"
	"github.com/reactivex/rxgo/observer"
	"github.com/reactivex/rxgo/subscription"
	"github.com/streadway/amqp"
	"sync"
)

/**
 * @author : Donald Trieu
 * @created : 7/31/21, Saturday
**/

type IRabbitMessageHandler interface {
	HandleMessage(queue string, body []byte) observable.Observable
}

type Consumer struct {
	rabbitService        IRabbitService
	rabbitMessageHandler IRabbitMessageHandler
	pulling              bool
}

func NewRabbitConsumer(rabbitService IRabbitService, queues []string, bindingKeyQueue map[string][]string, handler IRabbitMessageHandler) *Consumer {
	p := Consumer{}
	p.rabbitService = rabbitService
	p.rabbitMessageHandler = handler
	if !rabbitService.IsReady() {
		fmt.Printf("%s : \n", errNotConnected)
	}
	go p.Subscribe(queues, bindingKeyQueue)
	return &p
}

func (c Consumer) Subscribe(queues []string, bindingKeyQueue map[string][]string) {
	for {
		if !c.rabbitService.IsReady() {
			continue
		}
		for _, queue := range queues {
			bindingKey := bindingKeyQueue[queue]
			fmt.Printf("%s\n", bindingKey)
			c.rabbitService.Subscribe(queue, bindingKey, false)
		}
		go c.StartPull()
		break
	}
}

func (c *Consumer) StartPull() {
	/*
		create 3 goroutine
		- one listen for local queue
		- one for re push message queue
		- the last one for normal message queue
		while receiving from re push queue, stop listen on normal queue. when all message on re push queue done, listen normal queue again
	*/
	var wg sync.WaitGroup
	defer c.rabbitService.Close()
	c.pulling = true
	wg.Add(2)

	go func() {
		defer wg.Done()
		fmt.Println("[PullerRabbit] Start Fetch Message From Rabbit")
		for c.pulling {
			if !c.rabbitService.IsReady() {
				continue
			}
			delivery, message, ok := c.rabbitService.FetchMessage()
			if ok {
				c.handleMessage(delivery, message)
			}
		}
	}()
	wg.Wait() // wait forever
}

func (c *Consumer) handleMessage(delivery amqp.Delivery, message RabbitMessage) <-chan subscription.Subscription {
	return c.rabbitMessageHandler.HandleMessage(delivery.RoutingKey, message.Body).
		Subscribe(observer.Observer{
			ErrHandler: func(e error) {
				delivery.Ack(false)
			},
			DoneHandler: func() {
				delivery.Ack(false)
			},
		})
}