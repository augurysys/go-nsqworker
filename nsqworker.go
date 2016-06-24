package nsqworker

import (
	"github.com/nsqio/go-nsq"
	"sync"
)

type NsqWorker struct {
	consumer *nsq.Consumer
	consumerLock sync.Mutex

	log	*logWrapper
}

func New(topic, channel string) (*NsqWorker, error) {

	nsqw := NsqWorker{}
	nsqw.log = newLogWrapper(topic, channel)
	config := nsq.NewConfig()

	var err error
	nsqw.consumer, err = nsq.NewConsumer(topic, channel, config)
	if err != nil {
		nsqw.log.Error(err)
		return nil, err
	}

	nsqw.consumer.SetLogger(nsqw.log, nsq.LogLevelInfo)

	nsqw.consumer.AddHandler(&Router{nsqw.log})

	return &nsqw, nil
}

func (nsqw *NsqWorker) Start(nsqlookupd string) error {
	nsqw.log.Infof("connecting nsqworker to nsqlookupd host [%s]", nsqlookupd)
	return nsqw.consumer.ConnectToNSQLookupd(nsqlookupd)
}

func (nsqw *NsqWorker) Stop() {
	nsqw.consumerLock.Lock()
	defer nsqw.consumerLock.Unlock()

	nsqw.log.Debug("try to stop nsq consumer")
	nsqw.consumer.Stop()
	n := <- nsqw.consumer.StopChan

	if n == 0 {
		nsqw.log.Debug("nsq consumer is already stopped")
	} else {
		nsqw.log.Debug("nsq consumer stopped successfully")
	}
}
