package xsql

import (
	"fmt"
	"reflect"
	"time"
)

var (
	ErrNotFound = fmt.Errorf(`record not found`)
)

type Dialect interface {
	Parameterizie(numberOfValue int) []string
}

type BaseModel struct {
	Id      int64     `column:"id"`
	Created time.Time `column:"created"`
	Updated time.Time `column:"updated"`
}

type ResultMapper struct {
	reflect.Type
	Col2Field map[string]string
	Field2Col map[string]string
}
