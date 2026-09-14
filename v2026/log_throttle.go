package connect

import (
	"sync/atomic"
	"time"
)

// logThrottle rate-limits a high-volume log line to at most one emission per
// interval, counting how many emissions were suppressed in between so the next
// allowed line can print a "(N suppressed)" tail.
//
// This exists for lines that are individually useful but become a flood under a
// sustained fault. During a control-API outage every client retries auth and
// every contract request fails, so `[t]auth error`, `[contract]oob err`,
// `[ts]->error` and `[r]drop` are emitted continuously by every sequence at
// once. Throttling keeps the signal (the first line, plus a running suppressed
// count) while bounding the volume.
//
// Concurrency: lock-free. When multiple goroutines race the same interval
// boundary, exactly one wins the CompareAndSwap and emits; the rest are counted
// as suppressed. That is the intended rate-limiting behavior, not a lost line.
type logThrottle struct {
	intervalNanos int64
	lastNanos     atomic.Int64
	suppressed    atomic.Int64
}

// newLogThrottle creates a log throttle configured with the specified interval.
func newLogThrottle(interval time.Duration) *logThrottle {
	return &logThrottle{intervalNanos: int64(interval)}
}

// Allow reports whether a log line may be emitted as of now. When it returns
// true, the second value is the number of lines suppressed since the previous
// allowed line (reset to 0 by this call). When false, the caller should stay
// quiet; the suppression is counted internally.
func (t *logThrottle) Allow(now time.Time) (bool, int64) {
	nowNanos := now.UnixNano()
	last := t.lastNanos.Load()
	if nowNanos-last < t.intervalNanos {
		t.suppressed.Add(1)
		return false, 0
	}
	if !t.lastNanos.CompareAndSwap(last, nowNanos) {
		t.suppressed.Add(1)
		return false, 0
	}
	return true, t.suppressed.Swap(0)
}
