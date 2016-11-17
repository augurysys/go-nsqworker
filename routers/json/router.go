package json

import (
	"github.com/augurysys/go-nsqworker"
	"fmt"
	"sync"
	"github.com/Sirupsen/logrus"
)

type routeRes struct {
	err	error
	status	string
}

func (rr routeRes) Error() string {
	return fmt.Sprintf("an error occured while %s: error=%s", rr.status, rr.err.Error())
}

type Router struct {
	routes []*Route
	persistor	Persistor
}

func NewRouter(routes ...*Route ) *Router


// implement Router interface
func (jr Router) ProcessMessage(message *nsqworker.Message) error {

	routesRes := make(chan routeRes, len(jr))
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
			res := routeRes{status: "matching"}

			var match bool
			for _, jc := range rt.M {

				match, res.err = jc.Match(jsnMessage)

				if res.err != nil {
					message.Log.Error(res.err)
					routesRes <- res
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

			res.status = "processing"
			if res.err = rt.H(jsnMessage); res.err != nil {
				message.Log.Error(res.err)
				routesRes <- res

			}
		}(route)
	}

	wg.Wait()
	close(routesRes)

	if len(routesRes) > 0 {
		message.Log.Warningf("message processing ended with %d errors", len(routesRes))
	}


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
