package connect

import (
	"testing"
	"time"
)

// The package-level throttles (dropErrThrottle in transfer.go, oobErrThrottle
// in transfer_contract_manager.go, authErrThrottle/writeErrThrottle in
// transport.go) exist to bound exactly the log lines named in log_throttle.go's
// doc comment. These tests pin the wiring: each `shouldLog*Err` wrapper must
// read/write the same persistent, package-level *logThrottle on every call,
// not construct a fresh one — a fresh one would defeat throttling entirely,
// since every call would see a zero `lastNanos` and be allowed.
//
// The throttles are process-wide state shared with other tests (e.g. any test
// that drives a real auth/OOB/write failure through the client), so these
// tests only assert invariants that hold regardless of what came before:
// "of two calls made back-to-back, at most one is allowed" is true whether
// the throttle starts fresh or mid-window, but is false for a throttle that
// resets on every call.

// assertAtMostOneAllowed calls fn twice with no delay and fails if both calls
// report ok=true. With a one-minute interval, two calls nanoseconds apart can
// never both fall on the allowed side of a correctly persisted throttle.
func assertAtMostOneAllowed(t *testing.T, name string, fn func() (bool, int64)) {
	t.Helper()
	ok1, _ := fn()
	ok2, _ := fn()
	if ok1 && ok2 {
		t.Fatalf("%s: two back-to-back calls were both allowed; the throttle is not persisted across calls", name)
	}
}

func TestShouldLogDropErr_PersistsThrottleAcrossCalls(t *testing.T) {
	assertAtMostOneAllowed(t, "shouldLogDropErr", shouldLogDropErr)
}

func TestShouldLogOobErr_PersistsThrottleAcrossCalls(t *testing.T) {
	assertAtMostOneAllowed(t, "shouldLogOobErr", shouldLogOobErr)
}

func TestShouldLogAuthErr_PersistsThrottleAcrossCalls(t *testing.T) {
	assertAtMostOneAllowed(t, "shouldLogAuthErr", shouldLogAuthErr)
}

func TestShouldLogWriteErr_PersistsThrottleAcrossCalls(t *testing.T) {
	assertAtMostOneAllowed(t, "shouldLogWriteErr", shouldLogWriteErr)
}

// A suppressed call never reports a nonzero count itself (only the next
// *allowed* call reports how many were suppressed before it). This holds for
// every wrapper's second, necessarily-suppressed-or-equal call above; assert
// it explicitly for one wrapper as a regression guard on the return contract.
func TestShouldLogDropErr_SuppressedCallReportsZero(t *testing.T) {
	_, _ = shouldLogDropErr()
	ok, suppressed := shouldLogDropErr()
	if ok {
		// Extremely unlikely (would require crossing a minute boundary
		// between the two calls above), but if it happens there is nothing
		// to assert about "suppressed".
		t.Skip("second call unexpectedly allowed; interval boundary race")
	}
	if suppressed != 0 {
		t.Fatalf("a suppressed call must report 0, got %d", suppressed)
	}
}

// Each throttle is configured for the one-minute window documented in
// log_throttle.go and in the throttle declarations themselves. This is a
// same-package white-box check on the exact configured value, guarding
// against an accidental change to a much shorter (spammy) or longer
// (over-suppressing) interval.
func TestPackageLogThrottles_ConfiguredForOneMinute(t *testing.T) {
	throttles := map[string]*logThrottle{
		"dropErrThrottle":  dropErrThrottle,
		"oobErrThrottle":   oobErrThrottle,
		"authErrThrottle":  authErrThrottle,
		"writeErrThrottle": writeErrThrottle,
	}
	for name, th := range throttles {
		if th == nil {
			t.Fatalf("%s is nil", name)
		}
		if th.intervalNanos != int64(time.Minute) {
			t.Fatalf("%s interval = %s, want %s", name, time.Duration(th.intervalNanos), time.Minute)
		}
	}
}

// The four throttles must be four distinct instances. Sharing an instance
// between call sites (e.g. a copy-paste that reused a variable) would let an
// auth-error flood suppress an unrelated write-error line, silently losing
// signal on a fault the throttle was never meant to touch.
func TestPackageLogThrottles_AreDistinctInstances(t *testing.T) {
	all := []*logThrottle{dropErrThrottle, oobErrThrottle, authErrThrottle, writeErrThrottle}
	for i := range all {
		for j := range all {
			if i == j {
				continue
			}
			if all[i] == all[j] {
				t.Fatalf("throttle at index %d and %d are the same instance", i, j)
			}
		}
	}
}
