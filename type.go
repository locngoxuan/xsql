package xsql

import (
	"database/sql"
	"fmt"
	"reflect"
	"time"
)

var (
	ErrNotFound               = fmt.Errorf(`record not found`)
	ErrWrongNumberAffectedRow = fmt.Errorf(`wrong number affected rows`)
	ErrWrongNumberInserted    = fmt.Errorf(`number of inserted recods is smaller than expectation`)
	ErrArgNotArrayAndSlice    = fmt.Errorf(`given argument is neither array nor slice`)
	ErrArgIsArrayOrSlice      = fmt.Errorf(`given argument is either array or slice`)
)

type Dialect interface {
	Parameterizie(numberOfValue int) []string
}

type BaseModel struct {
	Id      int64     `column:"id"`
	Created time.Time `column:"created"`
	Updated time.Time `column:"updated"`
}

type DbOption struct {
	*sql.DB
	Driver       string
	DSN          string
	MaxOpenConns int
	MaxIdleConns int
	MaxIdleTime  time.Duration
	MaxLifeTime  time.Duration
	IsoLevel     sql.IsolationLevel
	ReadOnly     bool
	Dialect
	Logger
}

type ResultMapper struct {
	reflect.Type
	Col2Field map[string]string
	Field2Col map[string]string
}

type Logger interface {
	// Debug uses fmt.Sprint to construct and log a message.
	Debug(args ...interface{})

	// Info uses fmt.Sprint to construct and log a message.
	Info(args ...interface{})

	// Warn uses fmt.Sprint to construct and log a message.
	Warn(args ...interface{})

	// Error uses fmt.Sprint to construct and log a message.
	Error(args ...interface{})

	// Panic uses fmt.Sprint to construct and log a message, then panics.
	Panic(args ...interface{})

	// Fatal uses fmt.Sprint to construct and log a message, then calls os.Exit.
	Fatal(args ...interface{})

	// Debugf uses fmt.Sprintf to log a templated message.
	Debugf(template string, args ...interface{})

	// Infof uses fmt.Sprintf to log a templated message.
	Infof(template string, args ...interface{})

	// Warnf uses fmt.Sprintf to log a templated message.
	Warnf(template string, args ...interface{})

	// Errorf uses fmt.Sprintf to log a templated message.
	Errorf(template string, args ...interface{})

	// Panicf uses fmt.Sprintf to log a templated message, then panics.
	Panicf(template string, args ...interface{})

	// Fatalf uses fmt.Sprintf to log a templated message, then calls os.Exit.
	Fatalf(template string, args ...interface{})

	// Debugw logs a message with some additional context. The variadic key-value
	// pairs are treated as they are in With.
	Debugw(msg string, keysAndValues ...interface{})

	// Infow logs a message with some additional context. The variadic key-value
	// pairs are treated as they are in With.
	Infow(msg string, keysAndValues ...interface{})

	// Warnw logs a message with some additional context. The variadic key-value
	// pairs are treated as they are in With.
	Warnw(msg string, keysAndValues ...interface{})

	// Errorw logs a message with some additional context. The variadic key-value
	// pairs are treated as they are in With.
	Errorw(msg string, keysAndValues ...interface{})

	// Panicw logs a message with some additional context, then panics. The
	// variadic key-value pairs are treated as they are in With.
	Panicw(msg string, keysAndValues ...interface{})

	// Fatalw logs a message with some additional context, then calls os.Exit. The
	// variadic key-value pairs are treated as they are in With.
	Fatalw(msg string, keysAndValues ...interface{})
}
