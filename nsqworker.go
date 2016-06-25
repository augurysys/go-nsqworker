package nsqworker

import (
	"github.com/nsqio/go-nsq"
)

type NsqWorker struct {
	consumer *nsq.Consumer

	lookupd  string

	log      *logWrapper
	router   *router

	running	bool
}

func New(topic, channel, lookupd string) (*NsqWorker, error) {

	nw := NsqWorker{}
	nw.log = newLogWrapper(topic, channel)
	config := nsq.NewConfig()

	var err error
	nw.consumer, err = nsq.NewConsumer(topic, channel, config)
	if err != nil {
		nw.log.Error(err)
		return nil, err
	}

	nw.consumer.SetLogger(nw.log, nsq.LogLevelInfo)

	nw.lookupd = lookupd
	nw.router = &router{log: nw.log}

	return &nw, nil
}

func (nw *NsqWorker) RegisterRoute(route Route) error {

	nw.router.routes = append(nw.router.routes, route)
	return nil
}

func (nw *NsqWorker) Start() error {
	nw.consumer.AddHandler(nw.router)

	nw.running = true

	nw.log.Infof("connecting nsqworker to nsqlookupd host [%s]", nw.lookupd)
	return  nw.consumer.ConnectToNSQLookupd(nw.lookupd)
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
