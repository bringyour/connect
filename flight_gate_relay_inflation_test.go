package connect

// FLIGHTGATEFIX §21.5, measurement 1. The campaign's
// mixed-relay-queue-inflation schedule in process: one reliable lane whose
// drain rate steps down mid-transfer, a queue in front of it deep enough
// that a write does not block, and the default resend budget. It records
// what the storm depends on, so a later change can be judged against the
// same instrument.

import (
	"testing"
	"time"
)

// relayInflationResult is one arm of the schedule.
type relayInflationResult struct {
	elapsed     time.Duration
	deadWindows int
	windows     int
	peakQueue   ByteCount
	stats       ClientSendRecoveryStatsSnapshot
}

// runRelayInflation offers the payload over a single reliable lane that
// slows at stepAfter, sampling the resend queue while it runs.
func runRelayInflation(
	t testing.TB,
	messageCount int,
	queueFrames int,
	deferTimeoutResend bool,
) relayInflationResult {
	t.Helper()
	harness := newMixedLaneHarnessWithOptions(t, mixedLaneOptions{
		slowLatency:                50 * time.Millisecond,
		slowSerialization:          500 * time.Microsecond,
		slowStepAfter:              time.Second,
		slowSerializationAfterStep: 12 * time.Millisecond,
		slowQueueFrames:            queueFrames,
		directLaneDisabled:         true,
		deferTimeoutResend:         deferTimeoutResend,
	})
	start := time.Now()
	var peakQueue ByteCount
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			case <-time.After(50 * time.Millisecond):
				if _, byteCount, _ := harness.sender.ResendQueueSize(
					harness.receiverId, MultiHopId{}, false, false); peakQueue < byteCount {
					peakQueue = byteCount
				}
			}
		}
	}()
	stats := harness.run(t, messageCount)
	close(stop)
	<-done
	elapsed := time.Since(start)

	harness.deliveryLock.Lock()
	times := append([]time.Time(nil), harness.deliveryTimes...)
	harness.deliveryLock.Unlock()
	windows := max(1, int(elapsed/time.Second))
	perWindow := make([]int, windows)
	for _, at := range times {
		index := min(max(int(at.Sub(start)/time.Second), 0), windows-1)
		perWindow[index] += 1
	}
	mean := float64(len(times)) / float64(windows)
	deadWindows := 0
	for _, count := range perWindow {
		if float64(count) < mean/4 {
			deadWindows += 1
		}
	}
	return relayInflationResult{
		elapsed:     elapsed,
		deadWindows: deadWindows,
		windows:     windows,
		peakQueue:   peakQueue,
		stats:       stats,
	}
}

// What the storm depends on. The queue in front of the lane is the
// variable: while it is shallow a full route channel blocks the sequence's
// own writes and throttles admission, and while it is deep nothing does,
// so the resend queue fills to its budget against a lane that has just
// proved it cannot drain what it holds. That happens with the deferred
// retransmit and without it alike, so the budget is not reached because
// the deferral withholds a write.
func TestInflatedRelayQueueDepthDominatesTheStorm(t *testing.T) {
	if testing.Short() {
		t.Skip("relay queue inflation schedule")
	}
	const (
		messageCount    = 6000
		shallowQueue    = 64
		deepQueue       = 4096
		resendBudgetBar = 2 << 20
	)
	report := func(name string, result relayInflationResult) {
		t.Logf(
			"%s: %s, %d of %d windows dead, peak resend queue %d B, rto=%d deferred=%d gap=%d carrier-change=%d",
			name, result.elapsed.Truncate(time.Millisecond),
			result.deadWindows, result.windows, result.peakQueue,
			result.stats.TimeoutResendWriteCount,
			result.stats.TimeoutResendDeferCount,
			result.stats.SelectiveGapWriteCount,
			result.stats.CarrierChangeWriteCount,
		)
	}
	shallow := runRelayInflation(t, messageCount, shallowQueue, true)
	report("shallow queue, defer on ", shallow)
	deepOff := runRelayInflation(t, messageCount, deepQueue, false)
	report("deep queue,    defer off", deepOff)
	deepOn := runRelayInflation(t, messageCount, deepQueue, true)
	report("deep queue,    defer on ", deepOn)

	// the instrument still reproduces the storm it was built for
	if deepOff.stats.TimeoutResendWriteCount < 500 {
		t.Fatalf(
			"the deep-queue arm wrote only %d whole-window timeouts, so this no longer reproduces the storm",
			deepOff.stats.TimeoutResendWriteCount,
		)
	}
	// a shallow queue holds the resend queue far below the budget the deep
	// one reaches: the route channel is the throttle
	if resendBudgetBar/2 <= shallow.peakQueue {
		t.Errorf("the shallow-queue arm reached %d B of resend queue, so the channel is not throttling it",
			shallow.peakQueue)
	}
	// and the budget is reached with the deferral and without it alike
	for name, result := range map[string]relayInflationResult{
		"with the deferred retransmit":    deepOn,
		"without the deferred retransmit": deepOff,
	} {
		if result.peakQueue < resendBudgetBar/2 {
			t.Errorf(
				"the deep-queue arm %s peaked at %d B of resend queue, under half the budget: "+
					"the claim under test is that a deep queue lets admission reach the budget either way",
				name, result.peakQueue,
			)
		}
	}
}
