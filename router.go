package nsqworker


type Router interface {
	ProcessMessage(*Message) error
	String() string
}

type RouterFunc func(message *Message) error

func (rf RouterFunc) ProcessMessage(message *Message) error {
	return rf(message)
}

func (rf RouterFunc) String() string {
	return GetFunctionName(rf)
}
