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

	nsqw.lookupd = lookupd
	nsqw.router = &router{log: nsqw.log}

	return &nsqw, nil
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
