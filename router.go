package nsqworker

import (
	"github.com/nsqio/go-nsq"
)

type Router struct {
	log	*logWrapper
}

// implement go-nsq's Handler interface
func (r *Router) HandleMessage(message *nsq.Message) error {

	r.log.Info(string(message.Body))
	return nil
}
