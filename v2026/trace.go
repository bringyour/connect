// Panic containment helpers convert recovered failures into explicit lifecycle
// cleanup without allowing a broken rescue handler to crash the process.
package connect

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"runtime/debug"
	"strings"
	"time"
)

func IsDoneError(r any) bool {
	isDoneMessage := func(message string) bool {
		switch message {
		case "Done":
			return true
		default:
			return false
		}
	}
	switch v := r.(type) {
	case error:
		return isDoneMessage(v.Error())
	case string:
		return isDoneMessage(v)
	default:
		return false
	}
}

// safeAck invokes an ack callback with the same nil-check, done-error handling,
// and panic isolation that wrapping it in HandleError would give, but without
// allocating a closure. The per-packet send path stores the caller's raw
// AckFunction in SendPack and passes it here at ack time, instead of allocating
// a wrapper closure per send. Allocation-free in the happy path: the deferred
// recover closure captures no variables, so it is a static func value.
func safeAck(ackCallback AckFunction, err error) {
	if ackCallback == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			if IsDoneError(r) {
				// the context was canceled and raised; standard pattern, do not log
			} else {
				DefaultLogger().Warningf("Unexpected error: %s\n", ErrorJson(r, debug.Stack()))
			}
		}
	}()
	ackCallback(err)
}

// Recovers one panic, logs it, and invokes every compatible rescue handler.
func HandleError(do func(), handlers ...any) (r any) {
	defer func() {
		if r = recover(); r != nil {
			if IsDoneError(r) {
				// the context was canceled and raised. this is a standard pattern, do not log
			} else {
				DefaultLogger().Warningf("Unexpected error: %s\n", ErrorJson(r, debug.Stack()))
			}
			err, ok := r.(error)
			if !ok {
				err = fmt.Errorf("%s", r)
			}
			for _, handler := range handlers {
				// a rescue handler that panics (e.g. a double-registered
				// wg.Done) must not escalate the recovery into a process
				// crash; contain it and log loudly, then run the remaining
				// handlers
				func() {
					defer func() {
						if handlerR := recover(); handlerR != nil {
							DefaultLogger().Warningf("Rescue handler panic (contained): %s\n", ErrorJson(handlerR, debug.Stack()))
						}
					}()
					switch v := handler.(type) {
					case func():
						v()
					case func(error):
						v(err)
					case context.CancelFunc:
						v()
					case context.CancelCauseFunc:
						v(err)
					}
				}()
			}
		}
	}()
	do()
	return
}

func HandleError1[R any](do func() R, handlers ...any) (result R) {
	HandleError(func() {
		result = do()
	}, func(err error) {
		for _, handler := range handlers {
			switch v := handler.(type) {
			case func() R:
				result = v()
			case func(error) R:
				result = v(err)
			}
		}
	})
	return
}

func HandleError2[R1 any, R2 any](do func() (R1, R2), handlers ...any) (result1 R1, result2 R2) {
	HandleError(func() {
		result1, result2 = do()
	}, func(err error) {
		for _, handler := range handlers {
			switch v := handler.(type) {
			case func() (R1, R2):
				result1, result2 = v()
			case func(error) (R1, R2):
				result1, result2 = v(err)
			}
		}
	})
	return
}

func ExpectErrorWithReturn[R any](do func() R, defaultValue R, handlers ...any) (returnValue R, r any) {
	defer func() {
		if r = recover(); r != nil {
			err, ok := r.(error)
			if !ok {
				err = fmt.Errorf("%s", r)
			}
			for _, handler := range handlers {
				switch v := handler.(type) {
				case func():
					v()
				case func(error):
					v(err)
				}
			}
		}
	}()
	returnValue = defaultValue
	returnValue = do()
	return
}

func ErrorJson(err any, stack []byte) string {
	stackLines := []string{}
	for _, line := range strings.Split(string(stack), "\n") {
		stackLines = append(stackLines, strings.TrimSpace(line))
	}
	errorJson, _ := json.Marshal(map[string]any{
		"error": fmt.Sprintf("%T=%s", err, err),
		"stack": stackLines,
	})
	return string(errorJson)
}

func Trace(tag string, do func()) {
	trace(tag, func() string {
		do()
		return ""
	})
}

func TraceWithReturn[R any](tag string, do func() R) (result R) {
	trace(tag, func() string {
		result = do()
		return fmt.Sprintf(" = %v", result)
	})
	return
}

func TraceWithReturnError[R any](tag string, do func() (R, error)) (result R, returnErr error) {
	trace(tag, func() string {
		result, returnErr = do()
		if returnErr != nil {
			return fmt.Sprintf(" err = %s", returnErr)
		}
		return fmt.Sprintf(" = %v", result)
	})
	return
}

func trace(tag string, do func() string) {
	start := time.Now()
	DefaultLogger().Infof("[%-8s]%s (%d)\n", "start", tag, start.UnixMilli())
	doTag := do()
	end := time.Now()
	millis := float32(end.Sub(start)) / float32(time.Millisecond)
	DefaultLogger().Infof("[%-8s]%s (%.2fms) (%d)%s\n", "end", tag, millis, end.UnixMilli(), doTag)
}

func CallbackName(f any) string {
	return runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
}
