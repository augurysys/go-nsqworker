package nsqworker

import (
	"github.com/nsqio/go-nsq"
	"sync"
	"time"
	"github.com/Sirupsen/logrus"
)

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
			defer wg.Done()

			route.ProcessMessage(message)
		} (route)
	}

	wg.Wait()
	message.Finish()
	return nil
}

func (r *router) touchLoop(m *nsq.Message) {
	touchInterval := 30 * time.Second
	for {
		time.Sleep(touchInterval)
		if m.HasResponded() {
			r.log.Debugf("message %s has responded, exiting touch loop", m.ID)
			break
		}
		r.log.Debugf("touching message %s", m.ID)
		m.Touch()
	}
}

