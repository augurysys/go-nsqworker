package json

import (
	"fmt"
	"reflect"
)

type JsonMatcher interface {
	Match(*JsonMessage) (bool, error)
	String() string
}

type FieldMatch struct {
	Field	string
	Value	interface{}
}

func (fm FieldMatch) Match(m *JsonMessage) (match bool, err error) {

	res, ok := m.JsonBody.Get(fm.Field)
	if ok && reflect.DeepEqual(res, fm.Value) {
		match = true
	}
	return
}

func (fm FieldMatch) String() string {
	return fmt.Sprintf("%s:%s", fm.Field, fm.Value)
}
