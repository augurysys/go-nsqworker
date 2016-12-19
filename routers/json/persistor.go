package json

import (
	"bitbucket.org/augury/go-clients/clients/redis"
	stdredis "github.com/garyburd/redigo/redis"
	"time"
	"encoding/json"
)

type Persistor interface {
	PersistMessage(*Message, Handler, error)
	ShouldHandle(*Message, Handler) bool
}

const (
	failedMessagesKey = "eh:messages:failed"
)

type failedEvent struct {
	Route       string    `json:"route"`
	Topic       string    `json:"topic"`
	PersistedAt time.Time `json:"persisted_at"`
	Message     string    `json:"message"`
	Channel     string    `json:"channel"`
	ErrorStr	string	`json:"error_str"`
}

type redisPersistor struct {
	pool *stdredis.Pool
}

func NewRedisPersistor() *redisPersistor {
	redis.Init()
	return &redisPersistor{pool: redis.GetNewRedisPool()}
}

func (rp *redisPersistor) PersistMessage(message *Message, handler Handler, reason error) {

	persistTime := time.Now()

	event := failedEvent{
		Route: handler.String(),
		Topic: message.Topic,
		PersistedAt: persistTime,
		Message: string(message.Body),
		Channel: message.Channel,
		ErrorStr: reason.Error(),
	}

	b, err := json.Marshal(&event)
	if err != nil {
		message.Log.Errorf("error persisting failed event %+v: %v", event, err)
	}

	conn := rp.pool.Get()
	defer conn.Close()

	if _, err := conn.Do("ZADD", failedMessagesKey, persistTime.Unix(), b);err != nil {
		message.Log.Errorf("error adding failed event to queue: %v", err)
	}
}

func (rp *redisPersistor) ShouldHandle(message *Message, handler Handler) bool {

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
				if route == handler.String() {
					return true
				}
			}
		}
	}

	return false
}


