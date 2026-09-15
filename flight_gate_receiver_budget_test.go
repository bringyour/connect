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
		fastLatency:              20 * time.Millisecond,
		slowLatency:              200 * time.Millisecond,
		fastSerialization:        time.Millisecond,
		slowSerialization:        4 * time.Millisecond,
		replySerialization:       time.Millisecond,
		receiveQueueMaxByteCount: budget,
		// The hole opens at the 25th data frame on either lane rather than
		// the 25th on the direct lane. The flight gate routes this payload
		// onto the relay: measured, the direct lane carried 24 frames and the
		// relay 144, so a direct-lane hole at 25 was unreachable by one frame
		// and this row's precondition came from lane reordering, not from the
		// hole. A lane-agnostic hole is reached on every run. The receive-side
		// mechanism the row reads — a full hold blocked at a gap refusing what
		// arrives above it — does not depend on which lane lost the frame.
		dataDropOnce: 25,
		// Force the overflow rather than racing for it. The hole at frame 25
		// is deterministic already; this holds the arrivals behind it until
		// enough have piled up to carry the queue past its cap, so the
		// precondition this row needs is a counted event rather than one
		// goroutine outpacing another (THROUGHPUTFIX 23).
		//
		// Sized just past the cap, not far past it. The queue holds 8 KiB
		// against ~900 byte messages, so about nine frames fill it and ten
		// carries it over by one. Depth costs recovery time superlinearly
		// here: sixty-four — seven times the cap — ran to 117 timeout resends
		// and 88 seconds while the drop count stayed at fifteen; sixteen put
		// the lane-rule arm at 79 to 96 seconds and past the harness's old
		// flat two-minute wait in two of four runs; twelve did not finish the
		// lane-rule arm in ten minutes on the post-flip tree or on a copy with
		// the window rule forced off, so the depth and not the rule was the
		// cost. Ten, with the row's message count below cut from 150 to 60,
		// finishes both rows in 95 and 73 seconds. The row's ceiling is
		// derived from the drop count rather than the overflow depth, so a
		// depth that buys more recovery than drops takes the two apart.
		holdAfterFastDrop:          10,
		deferTimeoutResend:         true,
		reliableLaneProvenRecovery: laneRule,
	})
	start := time.Now()
	stats := harness.run(t, messageCount)
	// The barrier is this row's precondition, so the row proves it fired
	// rather than inferring it from the drops it was meant to force. This is
	// what makes the row fail deterministically with the barrier removed:
	// without it the overflow is a race and the drop count alone cannot say
	// which way the race went.
	if released := harness.holdReleasedCount.Load(); released < 10 {
		// Per-lane carriage is in the message because the likeliest reason the
		// barrier does not fire is that the hole never opens: the induced drop
		// counts data frames on the fast lane only, and if the flight gate
		// routes the data onto the slow lane that count never reaches its
		// index. A run that finishes quickly with 0 released and a fast lane
		// that carried fewer data frames than the drop index is that case.
		t.Fatalf(
			"the overflow barrier released %d frames against the 10 it holds, so the receive queue was not carried past its cap by a counted event and this run's drops, if any, are scheduling luck; fast lane carried %d and dropped %d, slow lane carried %d and dropped %d",
			released,
			harness.fastCarried.Load(), harness.fastDropped.Load(),
			harness.slowCarried.Load(), harness.slowDropped.Load(),
		)
	}
	// Drops and tentative evictions are complementary readings of the same
	// hold pressure since THROUGHPUTFIX §37.20. Under committed-prefix
	// acknowledgement a full hold keeps the sequence-earliest items and
	// removes the latest while it is still tentative, so pressure that used to
	// appear entirely as a refused arrival now appears partly as a tentative
	// eviction. Reading only one of them made this arm look as though nothing
	// happened.
	receiveStats := harness.receiver.ReceiveStats()
	return time.Since(start), stats,
		receiveStats.ReceiveQueueDropCount + receiveStats.ReceiveQueueTentativeEvictionCount
}

// The precondition §34.2 names, made observable: a receiver blocked at a
// direct hole fills its queue and then drops what arrives, on every lane,
// unacknowledged. Without this no lane proof can be destroyed, and until
// the counter existed the drop was visible only as a log line.
func TestReceiverBudgetDropsArrivalsAboveAHole(t *testing.T) {
	if testing.Short() {
		t.Skip("receiver budget")
	}
	// Sixty rather than 150: the mechanism needs only enough traffic to fill
	// an 8 KiB hold behind the hole, which a dozen frames do, and every
	// message past that is recovery the lane-rule arm pays over a 400 ms
	// round trip without adding to what the row reads.
	const messageCount = 60
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
	// Sixty rather than 150: the mechanism needs only enough traffic to fill
	// an 8 KiB hold behind the hole, which a dozen frames do, and every
	// message past that is recovery the lane-rule arm pays over a 400 ms
	// round trip without adding to what the row reads.
	const messageCount = 60
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
