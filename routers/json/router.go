package json

import (
	"fmt"
	"sync"
	"time"

	"github.com/augurysys/go-clients/utils"
	"github.com/augurysys/go-nsqworker"
	"github.com/sirupsen/logrus"
	"golang.org/x/net/context"
)

type Router struct {
	routes    []Route
	persistor Persistor
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

			match := true
			start := time.Now()
			eventName, _ := jsnMessage.JsonBody.GetString("name")

			rID, ok := jsnMessage.JsonBody.GetString("RID")
			if !ok {
				rID = utils.GenerateUID(6)
			}

			ctx := context.WithValue(context.Background(), "RID", rID)

			defer func() {

				if !match {
					return
				}

				var status, message string
				if err == nil {
					status = "OK"
				} else {
					status = "FAILED"
					message = err.Error()
				}

				span := time.Now().Sub(start)

				jsnMessage.Log.WithFields(logrus.Fields{
					"RID":     rID,
					"topic":   jsnMessage.Topic,
					"channel": jsnMessage.Channel,
					"route":   rt.Name,
					"event":   eventName,
					"status":  status,
					"time":    int64(span / time.Millisecond),
					"state":   "FINISH",
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
					message.Log.WithField("RID", rID).Errorf("panic error: %v", err.Error())
					jr.persistor.PersistMessage(jsnMessage, rt.Name, err)
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
					jr.persistor.PersistMessage(jsnMessage, rt.Name, err)
					return
				}

			}

			if !match {
				return
			}

			jsnMessage.Log.WithFields(logrus.Fields{
				"RID":     rID,
				"topic":   jsnMessage.Topic,
				"channel": jsnMessage.Channel,
				"route":   rt.Name,
				"event":   eventName,
				"state":   "START",
			}).Infoln("")

			if err = rt.H(ctx, jsnMessage); err != nil {
				message.Log.WithField("RID", rID).Error(err)
				jr.persistor.PersistMessage(jsnMessage, rt.Name, err)
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
	M    []Matcher
	H    Handler
	Name string
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
				if route == r.Name {
					return true
				}
			}
		}
	}

	return false
}

type Handler func(context.Context, *Message) error
