package connect

import (
    // "context"
	// "sync"
	"time"
	// "slices"
    // "os"
    // "os/signal"
    // "syscall"
    "fmt"
    "runtime/debug"
    "strings"
    "encoding/json"
    // "reflect"
    // mathrand "math/rand"
)


func IsDoneError(r any) bool {
    switch v := r.(type) {
    case error:
        switch v.Error() {
        case "Done":
            return true
        default:
            return false
        }
    case string:
        switch v {
        case "Done":
            return true
        default:
            return false
        }
    } 
    return false
}


func HandleError(do func(), handlers ...any) (r any) {
    defer func() {
        if r = recover(); r != nil {
        	if IsDoneError(r) {
                // the context was canceled and raised. this is a standard pattern, do not log
            } else {
	            fmt.Printf("Unexpected error: %s\n", ErrorJson(r, debug.Stack()))
	        }
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
    do()
    return
}


func ErrorJson(err any, stack []byte) string {
    stackLines := []string{}
    for _, line := range strings.Split(string(stack), "\n") {
        stackLines = append(stackLines, strings.TrimSpace(line))
    }
    errorJson, _ := json.Marshal(map[string]any{
        "error": fmt.Sprintf("%s", err),
        "stack": stackLines,
    })
    return string(errorJson)
}


func Trace(tag string, do func()) {
	trace(tag, func()(string) {
		do()
		return ""
	})
}


func TraceWithReturn[R any](tag string, do func()(R)) (result R) {
	trace(tag, func()(string) {
		result = do()
		return fmt.Sprintf(" = %v", result)
	})
	return
}


func trace(tag string, do func()(string)) {
	start := time.Now()
	fmt.Printf("[%-8s]%s (%d)\n", "start", tag, start.UnixMilli())
	doTag := do()
	end := time.Now()
	millis := float32(end.Sub(start)) / float32(time.Millisecond)
	fmt.Printf("[%-8s]%s (%.2fms) (%d)%s\n", "end", tag, millis, end.UnixMilli(), doTag)
}