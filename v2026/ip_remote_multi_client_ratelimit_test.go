package connect

import (
	"context"
	"errors"
	"testing"
	"time"
)

// The storm this guards against, measured on a real client 2026-08-17: 3,726
// platform 429s over 39 minutes, climbing 327 -> 672 per minute against a
// 1000/min budget, because every expander retried on a flat 1s cadence and
// nothing consulted the rate-limit class that classifyWindowFailure had
// already computed.

func newRateLimitTestWindow() *multiClientWindow {
	// windowRetryDelay/windowRetryReset touch only settings and the streak;
	// recordEvaluationFailure additionally needs the recorder and the monitor
	// that publishStallStatus reports through.
	return &multiClientWindow{
		settings: DefaultMultiClientSettings(),
		monitor:  NewRemoteUserNatMultiClientMonitorWithDefaults(),
		clients:  map[Id]*multiClientChannel{},
		failures: &windowFailureRecorder{},
	}
}

func TestWindowRetryDelayNonRateLimitIsUnchanged(t *testing.T) {
	w := newRateLimitTestWindow()
	for _, class := range []windowFailureClass{windowFailurePlatform, windowFailureProvider, windowFailureAuth} {
		if d := w.windowRetryDelay(class); d != w.settings.WindowEnumerateErrorTimeout {
			t.Fatalf("class %v must keep the flat cadence, got %s", class, d)
		}
	}
}

func TestWindowRetryDelayRateLimitBacksOffAndIsBounded(t *testing.T) {
	w := newRateLimitTestWindow()
	min := w.settings.WindowRateLimitBackoffMin
	max := w.settings.WindowRateLimitBackoffMax

	first := w.windowRetryDelay(windowFailureRateLimit)
	if first < min/2 || first >= min {
		t.Fatalf("first backoff must be jittered within [min/2, min), got %s (min %s)", first, min)
	}
	// every delay stays under the ceiling no matter how long the outage runs
	for i := 0; i < 200; i++ {
		d := w.windowRetryDelay(windowFailureRateLimit)
		if d >= max {
			t.Fatalf("backoff exceeded the ceiling at iteration %d: %s >= %s", i, d, max)
		}
		if d <= 0 {
			t.Fatalf("non-positive delay at iteration %d: %s", i, d)
		}
	}
	// and it did grow rather than sitting at the floor
	if late := w.windowRetryDelay(windowFailureRateLimit); late <= min {
		t.Fatalf("backoff never grew: %s <= min %s", late, min)
	}
}

func TestWindowRetryResetReturnsToTheFloor(t *testing.T) {
	w := newRateLimitTestWindow()
	for i := 0; i < 8; i++ {
		w.windowRetryDelay(windowFailureRateLimit)
	}
	w.windowRetryReset()
	d := w.windowRetryDelay(windowFailureRateLimit)
	if d >= w.settings.WindowRateLimitBackoffMin {
		t.Fatalf("after reset the backoff must start from the floor again, got %s", d)
	}
}

// Jitter is not decoration: dozens of expanders fail in the same instant, and
// without it they would retry in lockstep however long the delay.
func TestWindowRetryDelayIsJittered(t *testing.T) {
	w := newRateLimitTestWindow()
	seen := map[time.Duration]bool{}
	for i := 0; i < 32; i++ {
		w.windowRetryReset()
		seen[w.windowRetryDelay(windowFailureRateLimit)] = true
	}
	if len(seen) < 8 {
		t.Fatalf("expected jittered delays, got only %d distinct values in 32 draws", len(seen))
	}
}

// The verdict must actually reach the caller — it used to be computed and
// discarded, which is the whole reason the storm was possible.
func TestRecordEvaluationFailureReturnsTheRateLimitClass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := outcomeTestWindow(ctx, newRecordingLogger())
	class := w.recordEvaluationFailure(windowFailurePlatform, errors.New("429 Too Many Requests: <html>"))
	if class != windowFailureRateLimit {
		t.Fatalf("a 429 must classify as a rate limit, got %v", class)
	}
}
