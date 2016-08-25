package json

import (
	"fmt"
	"reflect"
	"strings"
)

type Matcher interface {
	Match(*Message) (bool, error)
	String() string
}

type FieldMatch struct {
	Field	string
	Value	interface{}
}

func (fm FieldMatch) Match(m *Message) (match bool, err error) {

	res, ok := m.JsonBody.Get(fm.Field)
	if ok && reflect.DeepEqual(res, fm.Value) {
		match = true
	}
	return
}

func (fm FieldMatch) String() string {
	return fmt.Sprintf("%s==%v", fm.Field, fm.Value)
}


type Predicate string
const (
	All Predicate = "&&"
	Any Predicate = "||"
)

func (p Predicate) Op(xs []bool) bool {

	switch p {
	case All:
		for _, x := range xs {
			if !x {
				return false
			}
		}
		return true
	case Any:
		for _, x := range xs {
			if x {
				return true
			}
		}
	}

	return false
}


type MultiFieldMatch struct {
	FieldMatches []FieldMatch
	P            Predicate
}

func (mfm MultiFieldMatch) String() string {
	fieldMatchStrings := make([]string, len(mfm.FieldMatches))
	for idx, fm := range mfm.FieldMatches {
		fieldMatchStrings[idx] = fm.String()
	}
	return strings.Join(fieldMatchStrings, fmt.Sprintf(" %v ", mfm.P))
}

func (mfm MultiFieldMatch) Match(m *Message) (match bool, err error) {

	matches := make([]bool, len(mfm.FieldMatches))
	for idx, fm := range mfm.FieldMatches {
		matches[idx], err = fm.Match(m)
		if err != nil {
			return
		}
	}

	match = mfm.P.Op(matches)
	return
}



