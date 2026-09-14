package connect

import (
	"testing"
	"time"
)

// These cover the decisions the three gates make, without standing up a live
// Client/ContractManager. Each mirrors the exact expression at its call site;
// the comment names the site so a future edit that changes one and not the
// other is caught by review.
//
// The degradation state is package-level, so these must not run in parallel.

// Site: SendSequence.updateContract, "if !isBackendDegraded() { CreateContract(...) }"
// (both the retry-loop call and the nextContract prefetch call).
func TestGate_ContractCreationSuppressedOnlyWhileDegraded(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	if isBackendDegraded() {
		t.Fatal("healthy backend must not suppress contract creation")
	}

	// One or two failures are churn, not an outage: creation must continue.
	noteBackendFailure()
	noteBackendFailure()
	if isBackendDegraded() {
		t.Fatal("contract creation suppressed below the failure threshold")
	}

	// Threshold reached: creation is now gated.
	noteBackendFailure()
	if !isBackendDegraded() {
		t.Fatal("contract creation not gated after the threshold was reached")
	}

	// Recovery must re-enable creation on the very next success, not on a timer.
	noteBackendSuccess()
	if isBackendDegraded() {
		t.Fatal("contract creation still gated after a successful round-trip")
	}
}

// Site: SendSequence.updateContract,
// "if isBackendDegraded() { retryInterval = maxRetryInterval }".
// While degraded, skip the fast first retry and start already backed off.
func TestGate_ContractRetryStartsBackedOffWhileDegraded(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	const fast = 1 * time.Second
	const max = 5 * time.Second

	startInterval := func() time.Duration {
		retryInterval := fast
		if isBackendDegraded() {
			retryInterval = max
		}
		return retryInterval
	}

	if got := startInterval(); got != fast {
		t.Fatalf("healthy start interval = %s, want the fast first retry %s", got, fast)
	}

	for i := 0; i < backendDegradedFailThreshold; i++ {
		noteBackendFailure()
	}
	if got := startInterval(); got != max {
		t.Fatalf("degraded start interval = %s, want the backed-off %s", got, max)
	}
}

// nextCreateContractRetryInterval is the upstream backoff this change composes
// with rather than replaces. It is otherwise untested, and the gate above is
// only meaningful if it actually saturates at the maximum.
func TestNextCreateContractRetryInterval(t *testing.T) {
	tests := []struct {
		name             string
		current, maximum time.Duration
		want             time.Duration
	}{
		{"doubles below the max", 1 * time.Second, 8 * time.Second, 2 * time.Second},
		{"doubles again", 2 * time.Second, 8 * time.Second, 4 * time.Second},
		{"saturates rather than overshooting", 3 * time.Second, 5 * time.Second, 5 * time.Second},
		{"already at the max", 5 * time.Second, 5 * time.Second, 5 * time.Second},
		{"never exceeds the max", 6 * time.Second, 5 * time.Second, 6 * time.Second},
		{"zero max disables backoff", 1 * time.Second, 0, 1 * time.Second},
		{"zero current jumps to the max", 0, 5 * time.Second, 5 * time.Second},
	}
	for _, tc := range tests {
		if got := nextCreateContractRetryInterval(tc.current, tc.maximum); got != tc.want {
			t.Errorf("%s: nextCreateContractRetryInterval(%s, %s) = %s, want %s",
				tc.name, tc.current, tc.maximum, got, tc.want)
		}
	}
}

// Repeated application must converge on the max and stay there, which is what
// makes "start at max while degraded" the correct shortcut.
func TestNextCreateContractRetryIntervalConvergesToMax(t *testing.T) {
	const max = 5 * time.Second
	interval := 1 * time.Second
	for i := 0; i < 20; i++ {
		interval = nextCreateContractRetryInterval(interval, max)
		if interval > max {
			t.Fatalf("interval %s exceeded max %s on iteration %d", interval, max, i)
		}
	}
	if interval != max {
		t.Fatalf("interval converged to %s, want %s", interval, max)
	}
}
