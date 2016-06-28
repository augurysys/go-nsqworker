package nsqworker

import (
	"github.com/nsqio/go-nsq"
	"sync"
	"time"
	"github.com/Sirupsen/logrus"
)

type dispatcher struct {
	nsqworker	*NsqWorker
}

// implement go-nsq's Handler interface
func (d *dispatcher) HandleMessage(message *nsq.Message) error {

	d.nsqworker.log.WithFields(logrus.Fields{"body": string(message.Body)}).
	Debug("received message")

	message.DisableAutoResponse()
	go d.touchLoop(message)

	msg := newMessage(message, d.nsqworker.topic, d.nsqworker.channel, d.nsqworker.log)
	var wg sync.WaitGroup
	for _, router := range d.nsqworker.routers {
		wg.Add(1)
		go func(rtr Router) {
			defer wg.Done()

			rtr.ProcessMessage(msg)
		} (router)
	}

	wg.Wait()
	message.Finish()
	return nil
}

func (d *dispatcher) touchLoop(m *nsq.Message) {
	touchInterval := 30 * time.Second
	lastTouch := time.Now()

	for {
		time.Sleep(time.Second)
		if m.HasResponded() {
			d.nsqworker.log.Debugf("message %s has responded, exiting touch loop", m.ID)
			break
		} else if time.Now().After(lastTouch.Add(touchInterval)) {
			d.nsqworker.log.Debugf("touching message %s", m.ID)
			m.Touch()
			lastTouch = time.Now()
		}
	}
}

