package nsqworker

import (
	"github.com/Sirupsen/logrus"
	"github.com/augurysys/timestamp"

	"github.com/nsqio/go-nsq"
	"fmt"
)

type Message struct {
	Topic     string
	Channel   string

	Body      []byte
	ID        string
	Timestamp *timestamp.Timestamp

	Log       logrus.FieldLogger
}

func newMessage(original *nsq.Message, topic, channel string, log logrus.FieldLogger) *Message {

	message := Message{
		Topic:        topic,
		Channel:channel,
		Log:log,
		Body:        original.Body,
		ID:        fmt.Sprintf("%s", original.ID),
		Timestamp: timestamp.Unix(0, original.Timestamp),
	}

	return &message
}