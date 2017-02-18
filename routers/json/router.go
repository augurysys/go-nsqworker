package json

import (
	"github.com/augurysys/go-nsqworker"
	"sync"
	"github.com/Sirupsen/logrus"
	"fmt"
	"time"
	"golang.org/x/net/context"
	"bitbucket.org/augury/go-clients/utils"
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

			var err error

			rID, ok := jsnMessage.JsonBody.GetString("RID")
			if !ok {
				rID = utils.GenerateUID(6)
			}

			eventName, _ := jsnMessage.JsonBody.GetString("name")

			ctx := context.WithValue(context.Background(), "RID", rID)
			match := true
			start := time.Now()

			defer func() {

				if !match {
					return
				}

				var status,message string
				if err == nil {
					status = "OK"
				} else {
					status = "FAILED"
					message = err.Error()
				}

				span := time.Now().Sub(start)

				jsnMessage.Log.WithFields(logrus.Fields{
					"RID": rID,
					"route": rt.H.String(),
					"event": eventName,
					"status": status,
					"time": span,
					"state": "END",
				}).Infoln(message)
			}()

			defer func() {
				if r := recover(); r != nil {
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
				match = false
				return
			}

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

			jsnMessage.Log.WithFields(logrus.Fields{
				"RID": rID,
				"route": rt.H.String(),
				"event": eventName,
				"state": "START",
			}).Infoln(message)

			if err = rt.H(ctx, jsnMessage); err != nil {
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


type Handler func(context.Context, *Message) error
func (jh Handler) String() string {
	return nsqworker.GetFunctionName(jh)
}
