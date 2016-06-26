package nsqworker

type Router interface {
	ProcessMessage(*Message) error
}

type RouterFunc func(message *Message) error

func (rf RouterFunc) ProcessMessage(message *Message) error {
	return rf(message)
}
