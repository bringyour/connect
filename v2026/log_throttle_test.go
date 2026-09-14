package connect

import (
	"testing"
	"time"
)

func TestLogThrottle_FirstCallAllowedZeroSuppressed(t *testing.T) {
	th := newLogThrottle(time.Minute)
	ok, suppressed := th.Allow(time.Unix(100, 0))
	if !ok {
		t.Fatal("first call should be allowed")
	}
	if suppressed != 0 {
		t.Fatalf("first call should report 0 suppressed, got %d", suppressed)
	}
}

func TestLogThrottle_SuppressesWithinIntervalThenReportsCount(t *testing.T) {
	th := newLogThrottle(time.Minute)
	base := time.Unix(1000, 0)

	if ok, _ := th.Allow(base); !ok {
		t.Fatal("first call should be allowed")
	}
	for i := 1; i <= 3; i++ {
		if ok, _ := th.Allow(base.Add(time.Duration(i) * time.Second)); ok {
			t.Fatalf("call %ds in (within interval) should be suppressed", i)
		}
	}

	ok, suppressed := th.Allow(base.Add(2 * time.Minute))
	if !ok {
		t.Fatal("call after the interval elapsed should be allowed")
	}
	if suppressed != 3 {
		t.Fatalf("expected 3 suppressed since the last allowed line, got %d", suppressed)
	}
}

func TestLogThrottle_SuppressedCountResetsAfterEmit(t *testing.T) {
	th := newLogThrottle(time.Minute)
	base := time.Unix(1000, 0)

	th.Allow(base)                      // allowed
	th.Allow(base.Add(1 * time.Second)) // suppressed -> count 1
	th.Allow(base.Add(2 * time.Minute)) // allowed, reports & resets count

	// A subsequent allowed line with no suppression in between reports 0.
	ok, suppressed := th.Allow(base.Add(4 * time.Minute))
	if !ok || suppressed != 0 {
		t.Fatalf("expected allowed with 0 suppressed, got ok=%v suppressed=%d", ok, suppressed)
	}
}

// The boundary is inclusive: a call landing exactly `interval` after the last
// allowed one is allowed, not suppressed. `Allow` computes this from a plain
// duration subtraction ("now - last < interval"), so this pins the boundary
// against an off-by-one in either direction.
func TestLogThrottle_ExactIntervalBoundaryIsAllowed(t *testing.T) {
	th := newLogThrottle(time.Minute)
	base := time.Unix(1000, 0)

	if ok, _ := th.Allow(base); !ok {
		t.Fatal("first call should be allowed")
	}
	// One nanosecond short of the interval: still suppressed.
	if ok, _ := th.Allow(base.Add(time.Minute - time.Nanosecond)); ok {
		t.Fatal("call 1ns short of the interval should be suppressed")
	}
	// Exactly the interval: allowed.
	ok, suppressed := th.Allow(base.Add(time.Minute))
	if !ok {
		t.Fatal("call exactly at the interval boundary should be allowed")
	}
	if suppressed != 1 {
		t.Fatalf("expected 1 suppressed (the short call above), got %d", suppressed)
	}
}

// A negative interval (misconfiguration) must not panic or wedge the
// throttle into always-suppress: `Allow` should degrade to "always allow"
// since `now - last` can never be less than a negative interval.
func TestLogThrottle_NonPositiveIntervalAlwaysAllows(t *testing.T) {
	th := newLogThrottle(0)
	base := time.Unix(1000, 0)

	for i := 0; i < 3; i++ {
		ok, _ := th.Allow(base.Add(time.Duration(i) * time.Nanosecond))
		if !ok {
			t.Fatalf("call %d with a zero interval should be allowed, was suppressed", i)
		}
	}
}

// A real outage drives every sequence at the fault simultaneously. Exactly one
// caller may emit per interval; the rest must be counted, not lost.
func TestLogThrottle_ConcurrentCallersEmitOncePerInterval(t *testing.T) {
	th := newLogThrottle(time.Minute)
	now := time.Unix(2000, 0)

	const callers = 64
	allowed := make(chan int64, callers)
	start := make(chan struct{})
	done := make(chan struct{})

	for i := 0; i < callers; i++ {
		go func() {
			defer func() { done <- struct{}{} }()
			<-start
			if ok, suppressed := th.Allow(now); ok {
				allowed <- suppressed
			}
		}()
	}
	close(start)
	for i := 0; i < callers; i++ {
		<-done
	}
	close(allowed)

	emitted := 0
	var reportedByWinner int64
	for suppressed := range allowed {
		emitted++
		reportedByWinner += suppressed
	}
	if emitted != 1 {
		t.Fatalf("expected exactly 1 emission across %d concurrent callers, got %d", callers, emitted)
	}

	// The winner's Swap(0) races the losers' Add(1): both happen after the
	// winner's CompareAndSwap, so a loser that increments first is reported by
	// the winning call rather than the next one. Either split is correct, so
	// assert on the total. What must hold is that no suppression is lost.
	ok, suppressed := th.Allow(now.Add(2 * time.Minute))
	if !ok {
		t.Fatal("call after the interval elapsed should be allowed")
	}
	if total := reportedByWinner + suppressed; total != callers-1 {
		t.Fatalf("expected %d suppressed in total, got %d (%d reported by the winning call, %d by the next)",
			callers-1, total, reportedByWinner, suppressed)
	}
}
