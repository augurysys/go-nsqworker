package nsqworker

import (
	"github.com/Sirupsen/logrus"
	"github.com/augurysys/timestamp"

	"github.com/nsqio/go-nsq"
	"fmt"
)

type Message struct {
	Topic	string
	Channel	string

	Body	[]byte
	ID	string
	Timestamp	*timestamp.Timestamp

	Log	*logrus.Entry
}


func newMessage(original *nsq.Message, topic, channel string, log *logrus.Logger) *Message {

	message := Message{
		Topic:	topic,
		Channel:channel,

		Body:	original.Body,
		ID:	fmt.Sprintf("%s", original.ID),
		Timestamp: timestamp.Unix(0, original.Timestamp),
	}

	message.Log = log.WithFields(logrus.Fields{
		"id":	message.ID,
		"message_timestamp":	message.Timestamp.String(),
		"channel":	channel,
		"topic":	topic,
	})

	return &message
}