package rabbitmq

import (
	"github.com/reactivex/rxgo/observable"
	"log"
)

/**
 * @author : Donald Trieu
 * @created : 7/31/21, Saturday
**/

type RabbitResponse struct {
	Action string `json:"action"`
	Data string `json:"data"`
}

type RemoteMessageHandler struct {
	Log *log.Logger
}

func (s *RemoteMessageHandler) HandleMessage(queue string, body []byte) observable.Observable {
	return observable.Start(func() interface{} {
		s.Log.Printf("Receive Rabbit Message From Routing Key : %s body %s\n", queue, string(body))
		return nil
	})
}
