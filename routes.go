package nsqworker

import "github.com/nsqio/go-nsq"


type Route interface {
	ProcessMessage(*nsq.Message) error
	//HandleMessage(Deserialized) error
	//Filter(Deserialized) bool
	//Deserialize([]byte) (Deserialized, error)
	//HandleError(error)
}

type RouteFunc func(message *nsq.Message) error

func (rf RouteFunc) ProcessMessage(message *nsq.Message) error {
	return rf(message)
}

//func (r *router) ProcessRoute(route Route, message *nsq.Message) {
//	content, err := route.Deserialize(message.Body)
//	if err != nil {
//		route.HandleError(err)
//	} else {
//		if route.Filter(content) {
//			route.HandleError(route.HandleMessage(content))
//		}
//	}
//}
