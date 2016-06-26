package nsqworker

import (
	"reflect"
	"runtime"
)

type Router interface {
	ProcessMessage(*Message) error
	String() string
}

type RouterFunc func(message *Message) error

func (rf RouterFunc) ProcessMessage(message *Message) error {
	return rf(message)
}

func (rf RouterFunc) String() string {
	return getFunctionName(rf)
}

func getFunctionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}