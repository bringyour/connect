package connect

import (
    "os"
    "log"
    "fmt"
)



const LogLevelUrgent = 0
const LogLevelInfo = 50
const LogLevelDebug = 100


var GlobalLogLevel = LogLevelInfo


var logger = log.New(os.Stderr, "", log.Ldate | log.Ltime | log.Lshortfile)

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