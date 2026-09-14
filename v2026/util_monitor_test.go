package connect

import (
	"sync"
	"testing"
	"time"
)

func TestMonitorValueGetSubscribesAtomically(t *testing.T) {
	v := NewMonitorValue(0)

	value, notify := v.Get()
	if value != 0 {
		t.Fatalf("value = %d, want 0", value)
	}
	select {
	case <-notify:
		t.Fatalf("notify fired with no change")
	default:
	}

	if !v.Set(1) {
		t.Fatalf("Set reported no change")
	}
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatalf("the subscribed channel did not close on a change")
	}
	// the closed channel is not reused: a fresh Get subscribes to the next change
	value, notify = v.Get()
	if value != 1 {
		t.Fatalf("value = %d, want 1", value)
	}
	select {
	case <-notify:
		t.Fatalf("a fresh subscribe is already closed")
	default:
	}
}

// TestMonitorValueSetOnlyNotifiesOnChange pins the property that makes a loop
// which both reads and writes the value converge: setting the same value again
// must not wake the loop that just set it.
func TestMonitorValueSetOnlyNotifiesOnChange(t *testing.T) {
	v := NewMonitorValue("a")

	_, notify := v.Get()
	if v.Set("a") {
		t.Fatalf("Set reported a change for an identical value")
	}
	select {
	case <-notify:
		t.Fatalf("setting an identical value notified — a loop that elects the value it watches would spin")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestMonitorValueNoLostWakeup drives the ordering the bare Monitor gets wrong:
// a change landing between a consumer's read and its wait must still wake it.
// Get takes the value and the channel atomically, so there is no such window.
func TestMonitorValueNoLostWakeup(t *testing.T) {
	v := NewMonitorValue(0)

	woke := make(chan int, 1)
	go func() {
		for {
			value, notify := v.Get()
			if 0 < value {
				woke <- value
				return
			}
			<-notify
		}
	}()

	// race the writer against the consumer's read/wait window
	select {
	case <-time.After(10 * time.Millisecond):
	}
	v.Set(7)

	select {
	case value := <-woke:
		if value != 7 {
			t.Fatalf("woke with %d, want 7", value)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("lost wakeup: the consumer never observed the change")
	}
}

func TestMonitorValueUpdate(t *testing.T) {
	v := NewMonitorValue(1)

	_, notify := v.Get()
	if !v.Update(func(value int) int { return value + 1 }) {
		t.Fatalf("Update reported no change")
	}
	if got := v.Value(); got != 2 {
		t.Fatalf("Value = %d, want 2", got)
	}
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatalf("Update did not notify")
	}

	// an update that lands on the same value must not notify
	_, notify = v.Get()
	if v.Update(func(value int) int { return value }) {
		t.Fatalf("Update reported a change for an identical value")
	}
	select {
	case <-notify:
		t.Fatalf("an identity Update notified")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestMonitorValueConcurrent exercises the value under concurrent readers and
// writers with the race detector.
func TestMonitorValueConcurrent(t *testing.T) {
	v := NewMonitorValue(0)

	done := make(chan struct{})
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				value, notify := v.Get()
				_ = value
				select {
				case <-notify:
				case <-time.After(time.Millisecond):
				}
			}
		}()
	}
	for i := 1; i <= 2000; i += 1 {
		v.Set(i)
	}
	close(done)
	wg.Wait()
}
