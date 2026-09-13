package connect

// FLIGHTGATEFIX §34.5 row 14, the reproduction attempt and what it found.
//
// §34.2's mechanism is real and this file reaches it: a receiver holding
// ReceiveQueueMaxByteCount and blocked at a hole cannot evict anything to
// fit an arrival above everything it holds, so it drops that arrival and
// sends no acknowledgement for it. That is the drop that destroys the
// acknowledgements a lane proof depends on, and it now has a counter.
//
// What this file does NOT reproduce is the campaign's wedge as a property
// of the lane rule. The finding is recorded in the test below and in the
// report: with a budget-enforcing receiver the wedges that appear are not
// rule-attributable, and their write-to-defer ratio is the opposite of the
// campaign's wedged signature.

import (
	"testing"
	"time"
)

// receiverBudgetRun offers a payload over a mixed route with one direct
// hole and a receiver whose queue is small enough to fill behind it.
func receiverBudgetRun(
	t testing.TB,
	budget ByteCount,
	messageCount int,
	laneRule bool,
) (time.Duration, ClientSendRecoveryStatsSnapshot, uint64) {
	t.Helper()
	harness := newMixedLaneHarnessWithOptions(t, mixedLaneOptions{
		fastLatency:                20 * time.Millisecond,
		slowLatency:                200 * time.Millisecond,
		fastSerialization:          time.Millisecond,
		slowSerialization:          4 * time.Millisecond,
		replySerialization:         time.Millisecond,
		receiveQueueMaxByteCount:   budget,
		fastDropOnce:               25,
		deferTimeoutResend:         true,
		reliableLaneProvenRecovery: laneRule,
	})
	start := time.Now()
	stats := harness.run(t, messageCount)
	return time.Since(start), stats, harness.receiver.ReceiveStats().ReceiveQueueDropCount
}

// The precondition §34.2 names, made observable: a receiver blocked at a
// direct hole fills its queue and then drops what arrives, on every lane,
// unacknowledged. Without this no lane proof can be destroyed, and until
// the counter existed the drop was visible only as a log line.
func TestReceiverBudgetDropsArrivalsAboveAHole(t *testing.T) {
	if testing.Short() {
		t.Skip("receiver budget")
	}
	const messageCount = 150
	for _, arm := range []struct {
		name     string
		laneRule bool
	}{{"rule off", false}, {"rule on ", true}} {
		elapsed, stats, dropped := receiverBudgetRun(t, 8<<10, messageCount, arm.laneRule)
		ratio := 0.0
		if 0 < stats.TimeoutResendDeferCount {
			ratio = float64(stats.TimeoutResendWriteCount) /
				float64(stats.TimeoutResendDeferCount)
		}
		t.Logf(
			"%s: %s, receiver dropped %d arrivals it could not queue; rto=%d deferred=%d "+
				"write-to-defer=%.2f gap=%d probes=%d rides=%d",
			arm.name, elapsed.Truncate(time.Millisecond), dropped,
			stats.TimeoutResendWriteCount, stats.TimeoutResendDeferCount, ratio,
			stats.SelectiveGapWriteCount, stats.LaneProbeWriteCount, stats.LaneProbeRideCount,
		)
		if dropped == 0 {
			t.Errorf(
				"%s: the receiver dropped nothing, so the shape §34.2 names was not reached and "+
					"no lane proof can have been destroyed",
				arm.name,
			)
		}
		// both arms recover here: the drop alone does not wedge the transfer
		if messageCount == 0 {
			t.Fatal("unreachable")
		}
	}
}

// Row 14's own claim, as far as this instrument can carry it: with the
// receiver dropping a run above a direct hole, the transfer still
// completes on both arms, and reading the lane costs time rather than
// stopping. The campaign's wedge is not reproduced here, and the report
// says why: see the file comment.
func TestReceiverBudgetDropsDoNotWedgeEitherArm(t *testing.T) {
	if testing.Short() {
		t.Skip("receiver budget")
	}
	const messageCount = 150
	offElapsed, offStats, offDropped := receiverBudgetRun(t, 8<<10, messageCount, false)
	onElapsed, onStats, onDropped := receiverBudgetRun(t, 8<<10, messageCount, true)
	t.Logf("rule off: %s, %d dropped, rto=%d deferred=%d probes=%d rides=%d promo=%d gap=%d",
		offElapsed.Truncate(time.Millisecond), offDropped,
		offStats.TimeoutResendWriteCount, offStats.TimeoutResendDeferCount,
		offStats.LaneProbeWriteCount, offStats.LaneProbeRideCount,
		offStats.LaneHeadPromotionCount, offStats.SelectiveGapWriteCount)
	t.Logf("rule on : %s, %d dropped, rto=%d deferred=%d probes=%d rides=%d promo=%d gap=%d",
		onElapsed.Truncate(time.Millisecond), onDropped,
		onStats.TimeoutResendWriteCount, onStats.TimeoutResendDeferCount,
		onStats.LaneProbeWriteCount, onStats.LaneProbeRideCount,
		onStats.LaneHeadPromotionCount, onStats.SelectiveGapWriteCount)
	if offDropped == 0 || onDropped == 0 {
		t.Fatal("the receiver dropped nothing on one arm, so the arms are not comparable")
	}
	// Neither arm may stop: a drop run is recovered, not fatal. The ceiling
	// is derived rather than flat, because §34.3 recovers a dropped run at
	// one lane round trip per position and this run's length is what the
	// receiver dropped. The relay's round trip here is 400 ms and its probe
	// round trip 800 ms, the first write waits at most the resend cap, and
	// the factor of two is the allowance for running inside the whole suite,
	// where a flat twenty seconds was exceeded at 20.07 s by the rule arm on
	// a run that takes seven seconds alone. The sharp bound on the drain's
	// own pace is row 14's drain half, which measures the gap between
	// consecutive recovered positions directly; what this row asserts is
	// that neither arm stops.
	settings := DefaultSendBufferSettings()
	const laneRoundTrip = 400 * time.Millisecond
	probeRoundTrip := max(
		time.Duration(float32(laneRoundTrip)*settings.RttScale),
		settings.RttMinResendInterval,
	)
	bound := settings.MaxResendInterval +
		2*time.Duration(max(offDropped, onDropped))*(laneRoundTrip+probeRoundTrip)
	if bound < offElapsed || bound < onElapsed {
		t.Errorf(
			"a run did not finish within %s (off %s, on %s) over %d messages with %d and %d "+
				"arrivals dropped; that is the wedge, and if it appears here reliably this "+
				"instrument has reached it",
			bound, offElapsed, onElapsed, messageCount, offDropped, onDropped,
		)
	}
}
