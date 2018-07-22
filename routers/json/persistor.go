package json

import (
	"encoding/json"
	"time"

	"bitbucket.org/augury/go-clients/clients/redis"
	"bitbucket.org/augury/go-clients/utils"
	stdredis "github.com/garyburd/redigo/redis"
)

type Persistor interface {
	PersistMessage(*Message, string, error)
}

const (
	failedMessagesKey = "eh:messages:failed:hash"
)

type failedEvent struct {
	Route       string    `json:"route"`
	Topic       string    `json:"topic"`
	PersistedAt time.Time `json:"persisted_at"`
	Message     string    `json:"message"`
	Channel     string    `json:"channel"`
	ErrorStr    string    `json:"error_str"`
}

type redisPersistor struct {
	pool *stdredis.Pool
}

func newRedisPersistor() *redisPersistor {
	redis.Init()
	return &redisPersistor{pool: redis.GetNewRedisPool()}
}

func (rp *redisPersistor) PersistMessage(message *Message, name string, reason error) {

	persistTime := time.Now()

	event := failedEvent{
		Route:       name,
		Topic:       message.Topic,
		PersistedAt: persistTime,
		Message:     string(message.Body),
		Channel:     message.Channel,
		ErrorStr:    reason.Error(),
	}

	b, err := json.Marshal(&event)
	if err != nil {
		message.Log.Errorf("error persisting failed event %+v: %v", event, err)
	}

	conn := rp.pool.Get()
	defer conn.Close()

	if _, err := conn.Do("HSET", failedMessagesKey, utils.GenerateUID(6), b); err != nil {
		message.Log.Errorf("error adding failed event to queue: %v", err)
	}
}
