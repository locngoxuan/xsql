package xsql

import (
	"fmt"
	"reflect"
	"strings"
)

type Statement struct {
	b            strings.Builder
	lastRune     rune
	expectedRows int64
	params       map[string]interface{}
	finalString  string
	args         []interface{}
}

func NewStmt(str string) *Statement {
	s := &Statement{
		b:        strings.Builder{},
		params:   make(map[string]interface{}),
		args:     make([]interface{}, 0),
		lastRune: 0,
	}
	return s.AppendSql(str)
}

func (s *Statement) ExpectedResult(i int) *Statement {
	s.expectedRows = int64(i)
	return s
}

func (s *Statement) Get() Statement {
	return *s
}

func (s *Statement) AppendSql(str string) *Statement {
	if str == "" {
		return s
	}
	if s.b.Len() > 0 && s.lastRune != rune(' ') {
		_, _ = s.b.WriteString(" ")
	}
	_, _ = s.b.WriteString(str)
	s.lastRune = rune(str[len(str)-1:][0])
	return s
}

func (s *Statement) RawSql() string {
	return s.b.String()
}

func (s *Statement) GetParams() []interface{} {
	var rs []interface{}
	for _, a := range s.args {
		val := reflect.ValueOf(a)
		if val.Kind() == reflect.Ptr {
			val = val.Elem()
		}
		if val.Kind() == reflect.Array || val.Kind() == reflect.Slice {
			tmp := sliceFromValue(val)
			rs = append(rs, tmp...)
		} else {
			rs = append(rs, a)
		}
	}
	return rs
}

func (s *Statement) String() string {
	if s.finalString != "" {
		return s.finalString
	}
	s.finalString = s.b.String()
	if s.params == nil || len(s.params) == 0 {
		return s.finalString
	}
	numberOfValues := 0
	runes := []rune(s.finalString)
	var b strings.Builder
	for i := 0; i < len(runes); i++ {
		r := runes[i]
		if r == ':' {
			//start parameter place holder
			j := i + 1
			for j = i + 1; j < len(runes); j++ {
				nr := runes[j]
				if ('a' <= nr && 'z' >= nr) ||
					('A' <= nr && 'Z' >= nr) ||
					('0' <= nr && '9' >= nr) ||
					nr == '_' || nr == '-' {
					continue
				}
				break
			}
			if j == i+1 {
				b.WriteRune(r)
				continue
			}
			key := runes[i+1 : j]
			prm, ok := s.params[string(key)]
			if !ok {
				b.WriteString(":")
				continue
			}
			size := getParamLen(prm)
			s.args = append(s.args, prm)
			numberOfValues += size
			strHolder := strRepeat("", "", "%s", ",", size)
			b.WriteString(strHolder)
			i = j - 1
			continue
		}
		b.WriteRune(r)
	}
	s.finalString = fmt.Sprintf(b.String(), strToIntf(dialect.Parameterizie(numberOfValues))...)
	return s.finalString
}

func getParamLen(p interface{}) int {
	val := reflect.ValueOf(p)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() == reflect.Slice || val.Kind() == reflect.Array {
		return val.Len()
	}
	return 1
}

func sliceFromValue(val reflect.Value) []interface{} {
	var rs []interface{}
	for i := 0; i < val.Len(); i++ {
		e := val.Index(i)
		if e.Kind() == reflect.Ptr {
			e = e.Elem()
		}
		intf := e.Interface()
		t := reflect.TypeOf(intf)
		if t.Kind() == reflect.Array || t.Kind() == reflect.Slice {
			tmp := sliceFromValue(reflect.ValueOf(intf))
			rs = append(rs, tmp...)
		} else {
			rs = append(rs, e.Interface())
		}
	}
	return rs
}

func (s *Statement) With(args map[string]interface{}) *Statement {
	s.params = args
	return s
}
