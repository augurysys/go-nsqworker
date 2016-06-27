package json

import (
	"encoding/json"
	"fmt"
	"github.com/jmespath/go-jmespath"
)

type JsonMatcher interface {
	Match(body []byte) (bool, error)
	String() string
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

func (fm FieldMatch) String() string {
	return fmt.Sprintf("%s:%s", fm.Field, fm.Value)
}
