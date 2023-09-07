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



const LogLevelUrgent = 0
const LogLevelInfo = 50
const LogLevelDebug = 100


var GlobalLogLevel = LogLevelInfo


var logger = log.Default()

func Logger() *log.Logger {
	return logger
}


type Log struct {
	level int
} 

func LogFn(level int, tag string) LogFunction {
	return func(format string, a ...any) {
		if level <= GlobalLogLevel {
			m := fmt.Sprintf(format, a...)
			Logger().Printf("%s: %s\n", tag, m)
		}
	}
}

func SubLogFn(level int, log LogFunction, tag string) LogFunction {
	return func(format string, a ...any) {
		if level <= GlobalLogLevel {
			m := fmt.Sprintf(format, a...)
			log("%s: %s", tag, m)
		}
	}
}

type LogFunction func(string, ...any)

func (self *LogFunction) Set() {

}

