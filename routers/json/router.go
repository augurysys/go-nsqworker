package json

import (
	"github.com/augurysys/go-nsqworker"
	"sync"
	"github.com/Sirupsen/logrus"
)

type Router struct {
	routes []*Route
	persistor	Persistor
}

func NewRouter(routes []*Route, persistor Persistor) *Router {

	router := new(Router)
	router.routes = routes
	router.persistor = persistor
	return router
}


// implement Router interface
func (jr Router) ProcessMessage(message *nsqworker.Message) error {

	jsnMessage, err := newJsonMessage(message)
	if err != nil {
		message.Log.Error(err)
		return err
	}

	var wg sync.WaitGroup
	for _, route := range jr.routes {
		wg.Add(1)
		go func(rt *Route) {
			defer wg.Done()

			if !jr.persistor.ShouldHandle(jsnMessage, rt.H) {
				message.Log.Debugf("%s shouldn't handle message", rt.H)
				return
			}

			var match bool
			var err error
			for _, jc := range rt.M {

				match, err = jc.Match(jsnMessage)

				if err != nil {
					message.Log.Error(err)
					jr.persistor.PersistMessage(jsnMessage, rt.H, err)
					return
				}

				if match {
					message.Log.WithFields(logrus.Fields{"route": rt.H,
									"condition": jc}).Infof("match found")
					break
				}
			}

			if !match {
				return
			}

			if err = rt.H(jsnMessage); err != nil {
				message.Log.Error(err)
				jr.persistor.PersistMessage(jsnMessage, rt.H, err)
			}

		}(route)
	}

	wg.Wait()

	return nil
}

func (jr Router) String() string {
	return "json router"
}

type Route struct {
	M []Matcher
	H Handler
}

type Handler func(*Message) error
func (jh Handler) String() string {
	return nsqworker.GetFunctionName(jh)
}
