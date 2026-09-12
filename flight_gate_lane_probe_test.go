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
		sequence.observeLaneAck(items[index])
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
	sequence.observeLaneAck(items[1])
	items[2].carrierRoute = direct
	items[2].sequenceNumber = 9
	sequence.observeLaneAck(items[2])

	if highest, acked := sequence.laneHighestAcked(relay); !acked || highest != 5 {
		t.Fatalf("the relay's highest acknowledged number reads %d (acked=%v), want 5", highest, acked)
	}
	if highest, acked := sequence.laneHighestAcked(direct); !acked || highest != 9 {
		t.Fatalf("the direct lane's highest reads %d (acked=%v), want 9", highest, acked)
	}
	// it only ever advances
	items[1].sequenceNumber = 2
	sequence.observeLaneAck(items[1])
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
		sequence.observeLaneAck(item)
		sequence.laneHighestAcked(relay)
		sequence.laneOldestOutstanding(relay)
	}); allocs != 0 {
		t.Fatalf("the lane acknowledgement table allocates %.1f per acknowledgement", allocs)
	}
}

// The rule is off: a candidate.
func TestLaneProvenRecoveryIsOffByDefault(t *testing.T) {
	if DefaultSendBufferSettings().ReliableLaneProvenRecovery {
		t.Fatal("the lane rule is on by default, but it has not been read in a campaign")
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
		perLane.LaneProbeWriteCount, perLane.LaneProbeHeldCount)

	if perItem.TimeoutResendWriteCount < 100 {
		t.Fatalf("only %d whole-window writes through the stall: this no longer reproduces the "+
			"excursion the gap export measures", perItem.TimeoutResendWriteCount)
	}
	if 20 < perLane.TimeoutResendWriteCount {
		t.Fatalf("reading the lane still wrote %d whole-window retransmits through the stall, "+
			"want single digits", perLane.TimeoutResendWriteCount)
	}
	if 0 != perLane.TimeoutResendDeferCount {
		t.Fatalf("reading the lane deferred %d times through the stall; a silent lane is the "+
			"probe's case, not the deferral's", perLane.TimeoutResendDeferCount)
	}
	if perLane.LaneProbeWriteCount == 0 {
		t.Fatal("no probe was written, so the head was never retransmitted")
	}
	// the hold must not spin: one hold per item per probe interval, not per
	// pass of the resend loop
	if 100*perLane.LaneProbeWriteCount < perLane.LaneProbeHeldCount/100 {
		t.Fatalf("%d holds against %d probes: the hold is re-arming into the past and spinning",
			perLane.LaneProbeHeldCount, perLane.LaneProbeWriteCount)
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
