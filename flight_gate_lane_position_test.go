package connect

// FLIGHTGATEFIX §34.5, the rows that state what deciding by lane position
// does. §34.3 replaced the unconditional re-arm with three positional rules
// and one promotion, and these are the behaviours that separate it from
// both merged and the rule it replaced:
//
//   - a batch the receiver never acknowledged drains at one lane round trip
//     per item, because each recovered item's acknowledgement promotes the
//     next position to one probe round trip (row 14's drain half);
//   - the same promotion writes nothing while a lane is really draining,
//     because the item is acknowledged before its promoted firing arrives
//     (row 15);
//   - acknowledgements below the head keep the head unwritten however far
//     the estimate lags, which is M4's profile with the rule on (row 16).

import (
	"testing"
	"time"
)

// laneDrainRun offers a payload over one reliable lane, with every position
// from dropFrom onward dropped once, and reports how the lane recovered it.
// Dropping to the end of the sequence is what makes it the wedge's shape:
// no later same-lane item survives to prove any of the dropped ones, so the
// scoreboard has nothing to act on and only the timer can recover them.
func laneDrainRun(
	t testing.TB,
	messageCount int,
	dropFrom int,
	laneRule bool,
) (time.Duration, ClientSendRecoveryStatsSnapshot, []time.Time, uint64) {
	t.Helper()
	harness := newMixedLaneHarnessWithOptions(t, mixedLaneOptions{
		directLaneDisabled:         true,
		slowLatency:                200 * time.Millisecond,
		slowSerialization:          2 * time.Millisecond,
		replySerialization:         time.Millisecond,
		slowDropRunAt:              dropFrom,
		slowDropRunCount:           1000,
		deferTimeoutResend:         true,
		reliableLaneProvenRecovery: laneRule,
	})
	start := time.Now()
	stats := harness.run(t, messageCount)
	elapsed := time.Since(start)
	harness.deliveryLock.Lock()
	deliveries := append([]time.Time(nil), harness.deliveryTimes...)
	harness.deliveryLock.Unlock()
	return elapsed, stats, deliveries, harness.slowDropped.Load()
}

// laneDrainSteps collapses the delivery times into the moments the receiver
// released something, and returns the gaps between the moments that follow
// the first stretch longer than settle. The receiver's stream is ordered, so
// each recovered position releases exactly one such moment: those gaps are
// the drain's own pacing.
func laneDrainSteps(deliveries []time.Time, settle time.Duration) []time.Duration {
	moments := []time.Time{}
	for index, at := range deliveries {
		if index == 0 || settle <= at.Sub(deliveries[index-1]) {
			moments = append(moments, at)
		}
	}
	steps := []time.Duration{}
	for index := 1; index < len(moments); index += 1 {
		steps = append(steps, moments[index].Sub(moments[index-1]))
	}
	return steps
}

func laneDrainMeanAndLongest(steps []time.Duration) (time.Duration, time.Duration) {
	if len(steps) == 0 {
		return 0, 0
	}
	total := time.Duration(0)
	longest := time.Duration(0)
	for _, step := range steps {
		total += step
		if longest < step {
			longest = step
		}
	}
	return total / time.Duration(len(steps)), longest
}

// Row 14's drain half. Every position from a point onward is dropped once,
// so no later same-lane item survives to prove any of them: this is the
// shape §34.2 says wedges the unconditional rule, and the scoreboard has
// nothing to act on, which the row checks by requiring no gap write. Under
// §34.3 the oldest dropped position is written on its own backed-off timer,
// its acknowledgement promotes the next to one probe round trip, and the
// batch drains a position at a time at that pace.
//
// The row records the trade as well as the bound. merged rewrites the whole
// window and recovers the batch in one round, which is faster here and is
// the duplicate storm this program exists to remove; the lane rule pays one
// round trip per position for writing only what its lane's own
// acknowledgements place.
func TestLaneDrainDroppedBatchDrainsAtOneRoundTripPerItem(t *testing.T) {
	if testing.Short() {
		t.Skip("lane drain, live link")
	}
	const (
		messageCount = 60
		dropFrom     = 13
		// the lane's own round trip, which the harness fixes
		laneRoundTrip = 400 * time.Millisecond
	)
	for _, arm := range []struct {
		name     string
		laneRule bool
	}{{"rule off", false}, {"rule on ", true}} {
		elapsed, stats, deliveries, dropped := laneDrainRun(
			t, messageCount, dropFrom, arm.laneRule)
		steps := laneDrainSteps(deliveries, 500*time.Millisecond)
		// the first step is the oldest dropped position waiting out its own
		// backed-off timer, which §34.3 states separately; the drain is what
		// follows it
		first := time.Duration(0)
		drain := steps
		if 0 < len(steps) {
			first = steps[0]
			drain = steps[1:]
		}
		mean, longest := laneDrainMeanAndLongest(drain)
		t.Logf(
			"%s: %s for %d messages, %d positions dropped once; rto=%d probes=%d rides=%d "+
				"promotions=%d endpoint=%d gap=%d; first write at %s, then %d drain steps, "+
				"mean %s longest %s",
			arm.name, elapsed.Truncate(time.Millisecond), messageCount, dropped,
			stats.TimeoutResendWriteCount, stats.LaneProbeWriteCount, stats.LaneProbeRideCount,
			stats.LaneHeadPromotionCount, stats.LaneProvenTimeoutWriteCount,
			stats.SelectiveGapWriteCount, first.Truncate(time.Millisecond), len(drain),
			mean.Truncate(time.Millisecond), longest.Truncate(time.Millisecond),
		)
		if dropped < 4 {
			t.Fatalf("%s: only %d positions were dropped, so the row measured nothing",
				arm.name, dropped)
		}
		if !arm.laneRule {
			continue
		}
		if stats.LaneHeadPromotionCount == 0 {
			t.Errorf("%s: no acknowledgement promoted a lane head, so the drain's pacing is not "+
				"the promotion's", arm.name)
		}
		if len(drain) < 2 {
			t.Fatalf("%s: the recovery released %d times, too few to measure its pacing",
				arm.name, len(steps))
		}
		// The bound: one probe round trip to look plus one to carry the
		// write and its acknowledgement, with slack for the scheduler. A
		// drain paced by each position's own backed-off timer doubles past
		// this by the third position and reaches the eight second cap.
		settings := DefaultSendBufferSettings()
		probeRoundTrip := max(
			time.Duration(float32(laneRoundTrip)*settings.RttScale),
			settings.RttMinResendInterval,
		)
		bound := 2 * (laneRoundTrip + probeRoundTrip)
		if settings.MaxResendInterval < first {
			t.Errorf(
				"%s: the oldest dropped position waited %s for its first write, past the resend "+
					"cap %s", arm.name, first, settings.MaxResendInterval)
		}
		if bound < mean {
			t.Errorf(
				"%s: the dropped batch drained at a mean %s per position, want at most %s: the "+
					"promotion is not pacing it and each position is waiting out a backed-off "+
					"timer",
				arm.name, mean, bound,
			)
		}
		// and it is one round trip per position, not a backoff that grows:
		// an even drain has every step at the mean, a doubling one ends far
		// above it
		if 2*mean < longest {
			t.Errorf(
				"%s: the longest drain step was %s against a mean of %s, so the batch is not "+
					"draining at one round trip per position; that is a backoff growing under it",
				arm.name, longest, mean,
			)
		}
	}
}

// Row 16 (FLIGHTGATEFIX §34.5). M4's profile with the rule on. The lane
// keeps acknowledging while its delay grows past what the estimate has
// learned, so every firing is early; rule 2 holds at each of them, because
// something below the item was acknowledged since it last looked, and
// nothing is written. This is the regime in which every threshold tried
// before, the scaled round trip (§22), the deviation term (§25) and progress
// since the last deferral (§32.5), wrote the window, and it is held here by
// a fact about position rather than by a test on a gap.
//
// The second half is the bound §34.3 states and owns: a lane that stops
// answering writes its head once per pause longer than twice the head's own
// interval, and only its head. That is the price of a rule that owes nothing
// to proof, and the row asserts it is paid once rather than per item.
func TestLanePositionM4ProfileHoldsAndPausesWriteTheHeadOnce(t *testing.T) {
	settings := flightGateSettings(kib(64))
	settings.SendBufferSettings.DeferTimeoutResendWhileCumulativeProgress = true
	settings.SendBufferSettings.ReliableLaneProvenRecovery = true
	// There is no platform behind this sender, so the client key it
	// publishes to the control destination would sit unacknowledged for the
	// whole test and be written by rule 3 on the control sequence's own
	// timer. That write is the control sequence's, not this lane's, and it
	// would count in the totals below; park the publication until the test
	// ends.
	published := make(chan struct{})
	settings.beforeClientKeyPublishForTest = func() { <-published }
	client, peerId, fromPeer, _ := newFlightGateSender(t, settings)
	t.Cleanup(func() { close(published) })
	_, reliable := addFlightGateRoute(t, client, TransportTypeH1, 16, false)
	for index, delay := range []time.Duration{
		100 * time.Millisecond,
		250 * time.Millisecond,
		400 * time.Millisecond,
		700 * time.Millisecond,
	} {
		sendFlightGateMessage(t, client, peerId, index)
		pack := takeFlightGatePack(t, reliable, 5*time.Second)
		time.Sleep(delay)
		ackFlightGatePack(t, client, peerId, fromPeer, pack, false)
	}
	time.Sleep(100 * time.Millisecond)
	if recovery := client.SendRecoveryStats(); recovery.TimeoutResendWriteCount != 0 {
		t.Fatalf(
			"the lane was written while its own acknowledgements kept arriving below the head: %+v",
			recovery,
		)
	}
	drainFlightGateRoute(reliable)

	// the lane stops answering: the head is written, once
	sendFlightGateMessage(t, client, peerId, 8)
	takeFlightGatePack(t, reliable, 5*time.Second)
	takeFlightGatePack(t, reliable, 10*time.Second)
	afterFirst := client.SendRecoveryStats()
	if afterFirst.TimeoutResendWriteCount != 1 {
		t.Fatalf("a pause on the lane wrote %d retransmits, want exactly one: %+v",
			afterFirst.TimeoutResendWriteCount, afterFirst)
	}
	sequence := flightGateSendSequence(t, client, peerId)
	interval := sequence.rttWindow.ScaledRtt()
	// well inside the interval the rewrite armed, which is at least the
	// scaled round trip doubled
	time.Sleep(min(interval, time.Second))
	if second := client.SendRecoveryStats(); second.TimeoutResendWriteCount != 1 {
		t.Errorf(
			"the paused lane wrote %d retransmits within one interval (%s) of the first, want "+
				"one per pause longer than twice the head's interval: %+v",
			second.TimeoutResendWriteCount, interval, second,
		)
	}
}
