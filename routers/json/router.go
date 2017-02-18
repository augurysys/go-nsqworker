package json

import (
	"github.com/augurysys/go-nsqworker"
	"sync"
	"github.com/Sirupsen/logrus"
	"fmt"
)

type Router struct {
	routes []Route
	persistor	Persistor
}

func NewRouter() *Router {

	router := new(Router)
	router.persistor = newRedisPersistor()
	router.routes = make([]Route, 0)
	return router
}

func (r *Router) AddRoute(route Route) {
	r.routes = append(r.routes, route)
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
		go func(rt Route) {
			defer wg.Done()

			defer func() {
				if r := recover(); r != nil {
					var err error
					switch r.(type) {
					case error:
						err = r.(error)
					default:
						err = fmt.Errorf("panic: %v", r)

					}
					jr.persistor.PersistMessage(jsnMessage, rt.H, err)
				}
			}()

			if !rt.ShouldHandle(jsnMessage) {
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

func (r Route) ShouldHandle(message *Message) bool {

	recipients, exists := message.JsonBody.GetObject("recipients")
	if !exists {
		message.Log.Debug("%+v is not a persisted event", message.JsonBody)
		return true
	}

	for channel, _routes := range recipients {
		routes, ok := _routes.([]interface{})
		if !ok {
			message.Log.Errorf("error converting to list: %v, %T", _routes, _routes)
			return false
		}
		if channel == message.Channel {
			for _, _route := range routes {
				route, ok := _route.(string)
				if !ok {
					message.Log.Errorf("error converting to string: %v, %T", _route, _route)
					return false
				}
				if route == r.H.String() {
					return true
				}
			}
		}
	}

	return false
}


type Handler func(*Message) error
func (jh Handler) String() string {
	return nsqworker.GetFunctionName(jh)
}
