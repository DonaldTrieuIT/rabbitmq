package rabbitmq

/**
 * @author : Donald Trieu
 * @created : 7/31/21, Saturday
**/

type Publisher struct {
	rabbitService IRabbitService
}

func NewRabbitPublisher(service IRabbitService) *Publisher {
	return &Publisher{rabbitService: service}
}

func (p *Publisher) Push(routingKey string, data []byte) error {
	err := p.rabbitService.Push(routingKey, data)
	return err
}

