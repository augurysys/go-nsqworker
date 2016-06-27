package json

import (
	"github.com/augurysys/go-nsqworker"
	"encoding/json"
	"strings"
)

const separator = ":"

type JsonMessage struct {
	*nsqworker.Message
	JsonBody	*JsonBody
}

type JsonBody map[string]interface{}

func newJsonMessage(message *nsqworker.Message) (*JsonMessage, error) {

	jsb, err := newJsonBody(message.Body)
	if err != nil {
		return nil, err
	}

	return &JsonMessage{message, jsb}, nil

}

func newJsonBody(body []byte) (*JsonBody, error) {

	jsb := make(JsonBody)

	if err := json.Unmarshal(body, &jsb); err != nil {
		return nil, err
	}

	return &jsb, nil
}

func (jsb JsonBody) get(mkey string) (interface{}, bool) {

	var currObj interface{}
	var ok bool

	keys := strings.Split(mkey, separator)
	for i, key := range keys {
		if i == 0 {
			currObj, ok = jsb[key]
			if !ok {
				return nil, ok
			}
		} else if i == (len(keys) - 1) {
			switch currObj.(type) {
			case map[string]interface{}:
				currObj, ok = currObj.(map[string]interface{})[key]
				return currObj, ok
			default:
				return nil, false

			}
		} else {
			currObj, ok = currObj.(map[string]interface{})[key]
			if !ok {
				return nil, ok
			}

			switch currObj.(type) {
			case map[string]interface{}:
				continue
			default:
				return nil, false
			}
		}
	}

	return currObj, ok
}

func (jsb JsonBody) GetBool(mkey string) (ret, ok bool) {
	val, ok := jsb.get(mkey)
	if ok {
		switch val.(type) {
		case bool:
			ret = val.(bool)
		default:
			ok = false
		}
	}
	return
}

func (jsb JsonBody) GetString(mkey string) (ret string, ok bool) {
	val, ok := jsb.get(mkey)
	if ok {
		switch val.(type) {
		case string:
			ret = val.(string)
		default:
			ok = false
		}
	}
	return
}

func (jsb JsonBody) GetFloat(mkey string) (ret float64, ok bool) {
	val, ok := jsb.get(mkey)
	if ok {
		switch val.(type) {
		case float64:
			ret = val.(float64)
		default:
			ok = false
		}
	}
	return
}

func (jsb JsonBody) GetArray(mkey string) (ret []interface{}, ok bool) {
	val, ok := jsb.get(mkey)
	if ok {
		switch val.(type) {
		case []interface{}:
			ret = val.([]interface{})
		default:
			ok = false
		}
	}
	return
}

func (jsb JsonBody) GetObject(mkey string) (ret map[string]interface{}, ok bool) {
	val, ok := jsb.get(mkey)
	if ok {
		switch val.(type) {
		case map[string]interface{}:
			ret = val.(map[string]interface{})
		default:
			ok = false
		}
	}
	return
}

