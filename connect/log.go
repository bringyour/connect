package connect

import (
    "os"
    "log"
    "fmt"
)


// Logging convention in the `connect` package and generally for BringYour network components:
// Info: 
//     essential events for abnormal behavior. This level should be silent on normal operation,
//     with the exception of one time (infrequent) initialization data that is useful for monitoring
//     this includes:
//     - backpressure and connectivity timeouts
//     - abnormal exits
// Error:
//     unrecoverable crash details
//     this includes:
//     - unexpected panics even if handled and suppressed for partial operation
// Debug:
//     key events for trace debuggung and statistics
//     this includes:
//     - key system events with ids that can be used to filter
//     - frequent events - e.g. send, retry, forward, receive, ack - 
//       should be summarized as statistics printed every "n seconds" 
//       rather than logging each individual data point


const LogLevelUrgent = 0
const LogLevelInfo = 50
const LogLevelDebug = 100


var GlobalLogLevel = LogLevelUrgent


var logger = log.New(os.Stderr, "", log.Ldate | log.Ltime | log.Lshortfile)

func Logger() *log.Logger {
    return logger
}


type Log struct {
    level int
} 

// FIXME just use a leveled logger interface
// FIXME source code line not correct with these wrappers
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