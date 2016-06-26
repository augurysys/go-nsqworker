package router

import (
	"github.com/augurysys/go-nsqworker"
	"github.com/jmespath/go-jmespath"
	"fmt"
	"sync"
	"encoding/json"
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
					break
				}
			}

			if !match {
				return
			}

			res.status = "processing"
			if res.err = rt.Route(message); res.err != nil {
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

type JsonRoute struct {
	JCs	[]JC
	Route	func(msg *nsqworker.Message) error
}

type JC interface {
	Match(body []byte) (bool, error)
}

type FieldMatch struct {
	Field	string
	Value	string
}

func (fm FieldMatch) Match(body []byte) (bool, error) {

	var jsn interface{}
	if err := json.Unmarshal(body, &jsn); err != nil {
		return false, err
	}

	res, err := jmespath.Search(fm.Field, jsn)
	if err != nil {
		return false, err
	}

	resString := fmt.Sprintf("%s", res)
	return resString == fm.Value, nil
}