package nsqworker

import (
	"github.com/nsqio/go-nsq"
)

type NsqWorker struct {
	consumer *nsq.Consumer
	topic	string
	channel	string
	lookupds  []string

	log      *logWrapper
	dispatcher   *dispatcher


	routers	[]Router

	running	bool
}

func New(topic, channel string, lookupds []string) (*NsqWorker, error) {

	nw := NsqWorker{topic:topic, channel:channel}
	nw.log = newLogWrapper(topic, channel)
	config := nsq.NewConfig()

	var err error
	nw.consumer, err = nsq.NewConsumer(topic, channel, config)
	if err != nil {
		nw.log.Error(err)
		return nil, err
	}

	nw.consumer.SetLogger(nw.log, nsq.LogLevelInfo)

	nw.lookupds = lookupds
	nw.routers = make([]Router, 0)
	nw.dispatcher = &dispatcher{&nw}

	return &nw, nil
}

func (nw *NsqWorker) RegisterRouter(router Router) error {

	nw.routers = append(nw.routers, router)
	return nil
}

func (nw *NsqWorker) Start() error {
	nw.consumer.AddHandler(nw.dispatcher)

	nw.running = true
	nw.log.Infof("connecting nsqworker to nsqlookupd host [%s]", nw.lookupds)

	return  nw.consumer.ConnectToNSQLookupds(nw.lookupds)
}

// Implement the Closer interface
func (nw *NsqWorker) Close() error{
	nw.stopConsumer()
	return nil
}

func (nw *NsqWorker) stopConsumer() {

	nw.log.Debug("try to stop nsq consumer")
	nw.consumer.Stop()
	<- nw.consumer.StopChan

	nw.log.Info("nsq consumer stopped successfully")
}
