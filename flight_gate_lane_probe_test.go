package connect

// FLIGHTGATEFIX §26.2. A reliable lane's silence is a lane event, and the
// sender can read it by lane without any estimate: a reliable carrier
// retransmits below Transfer, so an item unacknowledged while later items
// on the same route are acknowledged was dropped at an endpoint, and one
// unacknowledged while nothing later on that route is acknowledged is
// queued or stalled. So gap recovery of a reliable-carried hole counts
// only its own route's later acknowledgements, and a timer firing on a
// route that has acknowledged nothing past the item probes that route's
// oldest outstanding item and holds the rest behind it.

import (
	"testing"
	"time"
)

// laneScoreboard is a scoreboard whose items carry two distinct routes: a
// relay and a direct lane.
func laneScoreboard(t testing.TB, laneRule bool) (*SendSequence, []*sendItem, Route, Route) {
	t.Helper()
	sendTime := time.Unix(1_700_000_000, 0)
	sequence, items := newSelectiveAckRecoveryTestSequence(8, sendTime)
	sequence.client = &Client{}
	sequence.sendBufferSettings.ReliableLaneProvenRecovery = laneRule
	sequence.flightController = newSendFlightController(sequence.sendBufferSettings)
	sequence.flightController.applyPolicy(transferFlightPolicySnapshot{
		generation:             1,
		limited:                true,
		reliableRouteAvailable: true,
	})
	sequence.rttWindow.CloseSendTime(uint64(time.Now().Add(-300 * time.Millisecond).UnixMilli()))
	relay := make(Route, 8)
	direct := make(Route, 8)
	return sequence, items, relay, direct
}

// laneHole makes item 0 a hole the relay carried, and acknowledges the
// later items on the stated route.
func laneHole(sequence *SendSequence, items []*sendItem, relay Route, provingRoute Route) {
	hole := items[0]
	hole.reliableCarrierObserved = true
	hole.unreliableFlightTracked = false
	hole.carrierRoute = relay
	for index := 1; index < len(items); index += 1 {
		items[index].selectiveAcked = true
		items[index].carrierRoute = provingRoute
		sequence.observeLaneAck(items[index], time.Now())
	}
}

// A relay-carried hole overtaken by acknowledgements from the direct lane
// is not proven: those replies say nothing about the relay's own leg.
func TestRelayHoleOvertakenByDirectAcksIsNotWritten(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	// past any time grace, so only the lane rule can hold it
	currentTime := sendTime.Add(10 * time.Second)
	for _, arm := range []struct {
		name     string
		laneRule bool
		written  bool
	}{
		{"as landed", false, true},
		{"reading the lane", true, false},
	} {
		sequence, items, relay, direct := laneScoreboard(t, arm.laneRule)
		laneHole(sequence, items, relay, direct)
		sequence.scheduleSelectiveAckRecovery(currentTime)
		if items[0].selectiveGapRecovered != arm.written {
			t.Errorf(
				"%s: a relay-carried hole overtaken by three direct-lane acknowledgements was "+
					"recovered=%v, want %v; the relay retransmits below Transfer, so only its own "+
					"acknowledgements prove a hole on it",
				arm.name, items[0].selectiveGapRecovered, arm.written,
			)
		}
	}
}

// A relay-carried hole proven by three later acknowledgements from the
// relay itself is written, on either arm: that is an endpoint drop.
func TestRelayHoleProvenByRelayAcksIsWritten(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(10 * time.Second)
	for _, laneRule := range []bool{false, true} {
		sequence, items, relay, _ := laneScoreboard(t, laneRule)
		laneHole(sequence, items, relay, relay)
		sequence.scheduleSelectiveAckRecovery(currentTime)
		if !items[0].selectiveGapRecovered {
			t.Errorf("lane rule=%v: a relay-carried hole with three later relay acknowledgements "+
				"was not recovered; that is an endpoint drop and it must be", laneRule)
		}
	}
}

// The route's own acknowledgement history is what separates the two, and a
// generation change forgets it.
func TestLaneAcksAreRecordedPerRouteAndResetOnAGeneration(t *testing.T) {
	sequence, items, relay, direct := laneScoreboard(t, true)
	items[1].carrierRoute = relay
	items[1].sequenceNumber = 5
	sequence.observeLaneAck(items[1], time.Now())
	items[2].carrierRoute = direct
	items[2].sequenceNumber = 9
	sequence.observeLaneAck(items[2], time.Now())

	if highest, acked := sequence.laneHighestAcked(relay); !acked || highest != 5 {
		t.Fatalf("the relay's highest acknowledged number reads %d (acked=%v), want 5", highest, acked)
	}
	if highest, acked := sequence.laneHighestAcked(direct); !acked || highest != 9 {
		t.Fatalf("the direct lane's highest reads %d (acked=%v), want 9", highest, acked)
	}
	// it only ever advances
	items[1].sequenceNumber = 2
	sequence.observeLaneAck(items[1], time.Now())
	if highest, _ := sequence.laneHighestAcked(relay); highest != 5 {
		t.Fatalf("the relay's highest moved backwards to %d", highest)
	}
	sequence.resetLaneAcks(2)
	if _, acked := sequence.laneHighestAcked(relay); acked {
		t.Fatal("a route generation change did not forget the route's acknowledgement history")
	}
}

// The rule allocates nothing on the acknowledgement path.
func TestLaneAckTableAllocatesNothing(t *testing.T) {
	sequence, items, relay, _ := laneScoreboard(t, true)
	item := items[1]
	item.carrierRoute = relay
	number := uint64(0)
	if allocs := testing.AllocsPerRun(1000, func() {
		number += 1
		item.sequenceNumber = number
		sequence.observeLaneAck(item, time.Now())
		sequence.laneHighestAcked(relay)
		sequence.laneOldestOutstanding(relay)
	}); allocs != 0 {
		t.Fatalf("the lane acknowledgement table allocates %.1f per acknowledgement", allocs)
	}
}

// The rule is off by default in this commit. It no longer regresses M4's
// contract (ten of ten under the race detector with it on, FLIGHTGATEFIX
// §32.6); the flip is the campaign's decision, and it is one boolean.
func TestLaneProvenRecoveryIsOffByDefault(t *testing.T) {
	if DefaultSendBufferSettings().ReliableLaneProvenRecovery {
		t.Fatal("the lane rule is on by default; the flip is the campaign's decision")
	}
	// the setting is still the way to turn it off
	settings := DefaultSendBufferSettings()
	settings.ReliableLaneProvenRecovery = false
	sequence, _ := newSelectiveAckRecoveryTestSequence(1, time.Unix(1_700_000_000, 0))
	sequence.client = &Client{}
	sequence.sendBufferSettings = settings
	item := &sendItem{reliableCarrierObserved: true, carrierRoute: make(Route, 1)}
	if sequence.laneProvenRecovery(item) {
		t.Fatal("the setting no longer turns the lane rule off")
	}
}

// End to end on the stall the campaign's gap export measures: a relay with
// a 200 ms lane that holds everything for 2.75 s mid-transfer. Reading the
// lane replaces a firing per item with one probe per backoff interval.
func TestLaneProbeReplacesTheWholeWindowOnASilentLane(t *testing.T) {
	if testing.Short() {
		t.Skip("relay stall")
	}
	measure := func(laneRule bool) ClientSendRecoveryStatsSnapshot {
		harness := newMixedLaneHarnessWithOptions(t, mixedLaneOptions{
			slowLatency:                200 * time.Millisecond,
			slowSerialization:          time.Millisecond,
			slowQueueFrames:            1024,
			directLaneDisabled:         true,
			deferTimeoutResend:         true,
			slowStallAfter:             1500 * time.Millisecond,
			slowStallFor:               2750 * time.Millisecond,
			reliableLaneProvenRecovery: laneRule,
		})
		return harness.run(t, 3000)
	}
	perItem := measure(false)
	perLane := measure(true)
	t.Logf("per item: rto=%d deferred=%d", perItem.TimeoutResendWriteCount, perItem.TimeoutResendDeferCount)
	t.Logf("per lane: rto=%d deferred=%d probes=%d held=%d",
		perLane.TimeoutResendWriteCount, perLane.TimeoutResendDeferCount,
		perLane.LaneProbeWriteCount, perLane.LaneProbeRideCount)

	if perItem.TimeoutResendWriteCount < 100 {
		t.Fatalf("only %d whole-window writes through the stall: this no longer reproduces the "+
			"excursion the gap export measures", perItem.TimeoutResendWriteCount)
	}
	// §27.2's falsification bar: more than ten writes during the stall
	if 10 < perLane.TimeoutResendWriteCount {
		t.Fatalf("reading the lane still wrote %d whole-window retransmits through the stall, "+
			"want at most ten", perLane.TimeoutResendWriteCount)
	}
	if perLane.LaneProbeWriteCount == 0 {
		t.Fatal("no probe was written, so the head was never retransmitted")
	}
	// every write is the route head's probe: a write that is not a probe is
	// a second firing, which is what the rule exists to remove
	if perLane.TimeoutResendWriteCount != perLane.LaneProbeWriteCount {
		t.Fatalf(
			"%d whole-window writes against %d probes: the difference is a second firing written "+
				"into a lane that is not draining, which §27.2 forbids",
			perLane.TimeoutResendWriteCount, perLane.LaneProbeWriteCount,
		)
	}
	if perLane.LaneProvenTimeoutWriteCount != 0 {
		t.Fatalf("%d writes were charged to an endpoint drop during a stall that drops nothing",
			perLane.LaneProvenTimeoutWriteCount)
	}
	// Deferrals are re-arms, not the metric; the writes above are. §27.2
	// expected no item deferred twice inside the stall, which held under
	// §27.3 because its draining window was one scaled round trip and a
	// backed-off re-arm landed past it. §32.4 collapsed that window into the
	// cold floor, so an item held through the stall's first two seconds is
	// re-armed more than once by design: nothing on the round-trip scale
	// reads whether the lane is silent. What the backoff still guarantees
	// is that those re-arms are logarithmic, at most one per doubling of the
	// item's interval inside the floor, never one per interval and never one
	// per pass of the loop. The per-item arm defers each item it holds at
	// onset exactly once before writing it, so its count is the items
	// outstanding at onset, and the per-lane count must stay within the
	// backoff's factor of that.
	settings := DefaultSendBufferSettings()
	reArmsPerItem := uint64(1)
	for interval := settings.RttMinResendInterval; interval < settings.MinResendInterval; interval *= 2 {
		reArmsPerItem += 1
	}
	if reArmsPerItem*perItem.TimeoutResendDeferCount+10 < perLane.TimeoutResendDeferCount {
		t.Fatalf(
			"reading the lane deferred %d times against %d per item: more than %d re-arms per "+
				"item inside the cold floor, so the re-arm is not backing off",
			perLane.TimeoutResendDeferCount, perItem.TimeoutResendDeferCount, reArmsPerItem,
		)
	}
	// the hold must not spin: one hold per item per probe interval, not per
	// pass of the resend loop
	if 100*perLane.LaneProbeWriteCount < perLane.LaneProbeRideCount/100 {
		t.Fatalf("%d holds against %d probes: the hold is re-arming into the past and spinning",
			perLane.LaneProbeRideCount, perLane.LaneProbeWriteCount)
	}
}

// A genuine endpoint drop on a reliable lane must still be recovered, and
// not materially later than today.
func TestEndpointDropOnAReliableLaneIsStillRecovered(t *testing.T) {
	if testing.Short() {
		t.Skip("reliable-lane endpoint drop")
	}
	measure := func(laneRule bool) (time.Duration, ClientSendRecoveryStatsSnapshot) {
		harness := newMixedLaneHarnessWithOptions(t, mixedLaneOptions{
			slowLatency:                50 * time.Millisecond,
			slowSerialization:          time.Millisecond,
			directLaneDisabled:         true,
			deferTimeoutResend:         true,
			slowDropFraction:           0.01,
			reliableLaneProvenRecovery: laneRule,
		})
		start := time.Now()
		stats := harness.run(t, 1500)
		return time.Since(start), stats
	}
	asLanded, landedStats := measure(false)
	perLane, laneStats := measure(true)
	t.Logf("as landed: %s, gap=%d rto=%d", asLanded.Truncate(time.Millisecond),
		landedStats.SelectiveGapWriteCount, landedStats.TimeoutResendWriteCount)
	t.Logf("per lane : %s, gap=%d rto=%d probes=%d", perLane.Truncate(time.Millisecond),
		laneStats.SelectiveGapWriteCount, laneStats.TimeoutResendWriteCount,
		laneStats.LaneProbeWriteCount)

	if landedStats.SelectiveGapWriteCount == 0 {
		t.Fatal("no gap recovery on a dropping reliable lane: this no longer reproduces a drop")
	}
	if laneStats.SelectiveGapWriteCount == 0 {
		t.Fatal("reading the lane recovered nothing on a dropping reliable lane")
	}
	// a single lane is unchanged by construction: every later ack is its own
	if tolerance := asLanded + asLanded/2; tolerance < perLane {
		t.Fatalf("reading the lane took %s against %s, so a genuine drop waits materially longer",
			perLane, asLanded)
	}
}

// FLIGHTGATEFIX §33. The campaign's deep wedge, in process. A relay that
// goes silent for longer than the liveness cadence's cap leaves the lane
// rule with no reachable release condition: the rule withholds a firing
// until a later same-lane acknowledgement proves it, and a silent lane
// produces none by construction. The campaign measured what that costs,
// five runs over a hundred seconds in eighty with the rule on against none
// in eighty with it off, one of them twelve minutes on a link carrying
// nothing worse than one per cent loss.
//
// The row holds the relay for twenty seconds, well past the eight-second
// cap on the head probe's cadence, and asks what the two arms do once the
// lane comes back. The stall is common to both, so it is subtracted: what
// is measured is recovery after the impairment ends, which is where the
// campaign's wedges lived.
func TestSilentLaneLongerThanTheProbeCadenceStillDrains(t *testing.T) {
	if testing.Short() {
		t.Skip("relay stall")
	}
	const stall = 20 * time.Second
	measure := func(laneRule bool) (time.Duration, ClientSendRecoveryStatsSnapshot) {
		harness := newMixedLaneHarnessWithOptions(t, mixedLaneOptions{
			slowLatency:                200 * time.Millisecond,
			slowSerialization:          time.Millisecond,
			slowQueueFrames:            1024,
			directLaneDisabled:         true,
			deferTimeoutResend:         true,
			slowStallAfter:             1500 * time.Millisecond,
			slowStallFor:               stall,
			reliableLaneProvenRecovery: laneRule,
		})
		start := time.Now()
		snapshot := harness.run(t, 3000)
		return time.Since(start), snapshot
	}
	perItemElapsed, perItem := measure(false)
	perLaneElapsed, perLane := measure(true)
	t.Logf("per item: %s rto=%d deferred=%d", perItemElapsed,
		perItem.TimeoutResendWriteCount, perItem.TimeoutResendDeferCount)
	t.Logf("per lane: %s rto=%d deferred=%d probes=%d held=%d", perLaneElapsed,
		perLane.TimeoutResendWriteCount, perLane.TimeoutResendDeferCount,
		perLane.LaneProbeWriteCount, perLane.LaneProbeRideCount)

	// Both arms wait out the stall; only what follows it is theirs.
	perItemAfter := perItemElapsed - stall
	perLaneAfter := perLaneElapsed - stall
	if perItemAfter <= 0 || perLaneAfter <= 0 {
		t.Fatalf("the stall did not bind: per item %s, per lane %s against a %s stall",
			perItemElapsed, perLaneElapsed, stall)
	}
	// The campaign's wedges ran one to two orders of magnitude past the
	// rule-off runs of the same cell. A factor of four is well clear of
	// run-to-run spread here and well under anything the campaign saw.
	if 4*perItemAfter < perLaneAfter {
		t.Fatalf(
			"reading the lane took %s to drain after the stall against %s per item, more than "+
				"four times: the rule's release condition is a later same-lane acknowledgement, "+
				"which a silent lane cannot produce, so the sender has no bound of its own "+
				"(FLIGHTGATEFIX §33.9)",
			perLaneAfter, perItemAfter,
		)
	}
	// A wedge is visible in the counters even when the clock happens to
	// escape: the sender holds recovery work it never writes.
	if perLane.TimeoutResendDeferCount != 0 &&
		100*perLane.TimeoutResendWriteCount < perLane.TimeoutResendDeferCount {
		t.Fatalf(
			"reading the lane wrote %d recovery messages against %d deferred, a write-to-defer "+
				"ratio under one per cent: the campaign's failed wedge sat at 0.01 for twelve "+
				"minutes (FLIGHTGATEFIX §33.9)",
			perLane.TimeoutResendWriteCount, perLane.TimeoutResendDeferCount,
		)
	}
}
