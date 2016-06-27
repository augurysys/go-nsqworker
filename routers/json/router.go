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

type JsonRouter struct {
	Routes	[]JsonRoute
}

// implement Router interface
func (jr *JsonRouter) ProcessMessage(message *nsqworker.Message) error {

	routesRes := make(chan routeRes, len(jr.Routes))
	jsnMessage, err := newJsonMessage(message)
	if err != nil {
		message.Log.Error(err)
		return err
	}

	var wg sync.WaitGroup
	for _, route := range jr.Routes {
		wg.Add(1)
		go func(rt JsonRoute) {
			defer wg.Done()
			res := routeRes{status: "matching"}

			var match bool
			for _, jc := range rt.JCs {

				match, res.err = jc.Match(message.Body)

				if res.err != nil {
					message.Log.Error(res.err)
					routesRes <- res
					return
				}

				if match {
					message.Log.WithFields(logrus.Fields{"route": rt.JH,
									"condition": jc}).Infof("match found")
					break
				}
			}

			if !match {
				return
			}

			res.status = "processing"
			if res.err = rt.JH(jsnMessage); res.err != nil {
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

func (jr *JsonRouter) String() string {
	return "json router"
}

type JsonRoute struct {
	JCs	[]JsonMatcher
	JH	JsonHandler
}

type JsonHandler func(*JsonMessage) error
func (jh JsonHandler) String() string {
	return nsqworker.GetFunctionName(jh)
}
