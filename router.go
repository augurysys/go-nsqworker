package nsqworker

import (
	"github.com/nsqio/go-nsq"
	"sync"
	"time"
	"github.com/Sirupsen/logrus"
)

type Deserialized interface{}

type router struct {
	log	*logWrapper

	routes	[]Route
}

// implement go-nsq's Handler interface
func (r *router) HandleMessage(message *nsq.Message) error {

	r.log.WithFields(logrus.Fields{"body": string(message.Body)}).
	Debug("received message")

	message.DisableAutoResponse()
	go r.touchLoop(message)

	var wg sync.WaitGroup
	for _, route := range r.routes {
		wg.Add(1)
		go func(route Route) {
			r.ProcessRoute(route, message)
			wg.Done()
		} (route)
	}

	wg.Wait()
	message.Finish()
	return nil
}

func (r *router) ProcessRoute(route Route, message *nsq.Message) {
	content, err := route.Deserialize(message.Body)
	if err != nil {
		route.HandleError(err)
	} else {
		if route.Filter(content) {
			route.HandleError(route.HandleMessage(content))
		}
	}
}

func (r *router) touchLoop(m *nsq.Message) {
	touchInterval := 30 * time.Second
	for {
		if m.HasResponded() {
			r.log.Debugf("message %v has responded, exiting touch loop", m.ID)
			break
		} else {
			r.log.Debugf("touching message %v", m.ID)
			m.Touch()
			time.Sleep(touchInterval)
		}
	}
}

type Route interface {
	HandleMessage(Deserialized) error
	Filter(Deserialized) bool
	Deserialize([]byte) (Deserialized, error)
	HandleError(error)
}