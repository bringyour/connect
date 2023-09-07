package connect

import (
	"errors"
	"log"
	"fmt"

	"bringyour.com/protocol"
)


type Id [16]byte

func (self Id) Bytes() []byte {
	return self[0:16]
}


func IdFromProto(idBytes []byte) (Id, error) {
	if len(idBytes) != 16 {
		return Id{}, errors.New("Id must be 16 bytes")
	}
	return Id(idBytes), nil
}

func ToFrame(message any) *protocol.Frame {
	// FIXME
	return nil
}


var logger = log.Default()

func Logger() *log.Logger {
	return logger
}

func LogFn(tag string) LogFunction {
	return func(format string, a ...any) {
		m := fmt.Sprintf(format, a...)
		Logger().Printf("%s: %s\n", tag, m)
	}
}

func SubLogFn(log LogFunction, tag string) LogFunction {
	return func(format string, a ...any) {
		m := fmt.Sprintf(format, a...)
		log("%s: %s", tag, m)
	}
}

type LogFunction func(string, ...any)

