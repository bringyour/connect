// Tests nil comparisons in the shared assertion helpers without inspecting a
// live pointer's concurrently changing pointee.
package connect

import (
	"sync/atomic"
	"testing"
)

// A non-nil check is an identity question and must not deep-read live state.
func TestAssertNotEqualNonNilPointerDoesNotInspectPointee(t *testing.T) {
	pointee := &struct {
		counter uint64
	}{}
	started := make(chan struct{})
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		close(started)
		for {
			select {
			case <-stop:
				return
			default:
				atomic.AddUint64(&pointee.counter, 1)
			}
		}
	}()
	<-started
	for range 10_000 {
		AssertNotEqual(t, pointee, nil)
	}
	close(stop)
	<-done
}
