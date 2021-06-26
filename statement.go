package xsql

import (
	"fmt"
	"reflect"
	"strings"
)

type Statement struct {
	b      strings.Builder
	params []interface{}
}

func NewStmt(str string) *Statement {
	s := &Statement{
		b:      strings.Builder{},
		params: make([]interface{}, 0),
	}
	return s.AppendSql(str)
}

func (s *Statement) AppendSql(str string) *Statement {
	if str == "" {
		return s
	}
	_, _ = s.b.WriteString(str)
	return s
}

func (s *Statement) String() string {
	numberOfValues := len(s.params)
	return fmt.Sprintf(s.b.String(), strToIntf(dialect.Parameterizie(numberOfValues))...)
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

func (s *Statement) With(args ...interface{}) *Statement {
	var rs []interface{}
	for _, a := range args {
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
	s.params = append(s.params, rs...)
	return s
}
