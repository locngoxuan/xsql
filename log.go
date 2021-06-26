package xsql

import (
	"log"
)

type DefaultLogger struct {
}

// Debug uses fmt.Sprint to construct and log a message.
func (s DefaultLogger) Debug(args ...interface{}) {
	log.Println(args...)
}

// Info uses fmt.Sprint to construct and log a message.
func (s DefaultLogger) Info(args ...interface{}) {
	log.Println(args...)
}

// Warn uses fmt.Sprint to construct and log a message.
func (s DefaultLogger) Warn(args ...interface{}) {
	log.Println(args...)
}

// Error uses fmt.Sprint to construct and log a message.
func (s DefaultLogger) Error(args ...interface{}) {
	log.Println(args...)
}

// Panic uses fmt.Sprint to construct and log a message, then panics.
func (s DefaultLogger) Panic(args ...interface{}) {
	log.Panicln(args...)
}

// Fatal uses fmt.Sprint to construct and log a message, then calls os.Exit.
func (s DefaultLogger) Fatal(args ...interface{}) {
	log.Fatalln(args...)
}

// Debugf uses fmt.Sprintf to log a templated message.
func (s DefaultLogger) Debugf(template string, args ...interface{}) {
	log.Printf(template, args...)
}

// Infof uses fmt.Sprintf to log a templated message.
func (s DefaultLogger) Infof(template string, args ...interface{}) {
	log.Printf(template, args...)
}

// Warnf uses fmt.Sprintf to log a templated message.
func (s DefaultLogger) Warnf(template string, args ...interface{}) {
	log.Printf(template, args...)
}

// Errorf uses fmt.Sprintf to log a templated message.
func (s DefaultLogger) Errorf(template string, args ...interface{}) {
	log.Printf(template, args...)
}

// Panicf uses fmt.Sprintf to log a templated message, then panics.
func (s DefaultLogger) Panicf(template string, args ...interface{}) {
	log.Panicf(template, args...)
}

// Fatalf uses fmt.Sprintf to log a templated message, then calls os.Exit.
func (s DefaultLogger) Fatalf(template string, args ...interface{}) {
	log.Fatalf(template, args...)
}

// Debugw logs a message with some additional context. The variadic key-value
// pairs are treated as they are in With.
//
func (s DefaultLogger) Debugw(msg string, keysAndValues ...interface{}) {
	log.Println(msg, keysAndValues)
}

// Infow logs a message with some additional context. The variadic key-value
// pairs are treated as they are in With.
func (s DefaultLogger) Infow(msg string, keysAndValues ...interface{}) {
	log.Println(msg, keysAndValues)
}

// Warnw logs a message with some additional context. The variadic key-value
// pairs are treated as they are in With.
func (s DefaultLogger) Warnw(msg string, keysAndValues ...interface{}) {
	log.Println(msg, keysAndValues)
}

// Errorw logs a message with some additional context. The variadic key-value
// pairs are treated as they are in With.
func (s DefaultLogger) Errorw(msg string, keysAndValues ...interface{}) {
	log.Println(msg, keysAndValues)
}

// Panicw logs a message with some additional context, then panics. The
// variadic key-value pairs are treated as they are in With.
func (s DefaultLogger) Panicw(msg string, keysAndValues ...interface{}) {
	log.Panicln(msg, keysAndValues)
}

// Fatalw logs a message with some additional context, then calls os.Exit. The
// variadic key-value pairs are treated as they are in With.
func (s DefaultLogger) Fatalw(msg string, keysAndValues ...interface{}) {
	log.Fatalln(msg, keysAndValues)
}
