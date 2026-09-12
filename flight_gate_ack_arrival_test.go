package connect

// FLIGHTGATEFIX §19 D4, D5, D6. A hole is proven by acknowledgements that
// could not have overtaken its own reply, the lane latch drives that one
// judgement, and expired deferrals halve the flight once per pass.

import (
	"testing"
	"time"
	"unsafe"
)

// arrivalSequence is a scoreboard on a mixed route whose relay has answered
// at 300 ms, so a deferral is a measurable 600 ms.
func arrivalSequence(t testing.TB, itemCount int, sendTime time.Time) (*SendSequence, []*sendItem) {
	t.Helper()
	sequence, items := newSelectiveAckRecoveryTestSequence(itemCount, sendTime)
	sequence.client = &Client{}
	sequence.flightController = newSendFlightController(sequence.sendBufferSettings)
	sequence.flightController.applyPolicy(transferFlightPolicySnapshot{
		generation:             1,
		limited:                true,
		reliableRouteAvailable: true,
	})
	sequence.rttWindow.CloseSendTime(uint64(time.Now().Add(-300 * time.Millisecond).UnixMilli()))
	return sequence, items
}

// arrivalHole marks one item as a hole the given lane carried, with the
// later items selectively acknowledged over the given arrival lane.
func arrivalHole(items []*sendItem, holeIndex int, direct bool, conclusive bool) {
	hole := items[holeIndex]
	hole.unreliableCarrierObserved = direct
	hole.unreliableFlightTracked = direct
	hole.reliableCarrierObserved = !direct
	hole.selectiveAcked = false
	hole.selectiveAckConclusive = false
	hole.selectiveGapRecovered = false
	hole.recoveryKind = sendRecoveryNone
	for index := range items {
		if holeIndex < index {
			items[index].selectiveAcked = true
			items[index].selectiveAckConclusive = conclusive
		}
	}
}

// D4. The arrival lane reaches the scoreboard through the real ack path.
func TestAckArrivalLaneReachesTheScoreboard(t *testing.T) {
	for _, arm := range []struct {
		name            string
		arrival         CarrierReliability
		reliableSibling bool
		conclusive      bool
	}{
		{"a reply that took the relay", CarrierReliabilityReliable, true, true},
		{"a reply that took the direct lane", CarrierReliabilityUnreliable, true, false},
		{"a reply from a reader with no carrier information", CarrierReliabilityUnknown, true, false},
		{"a reply on a route with no relay to take", CarrierReliabilityUnreliable, false, true},
	} {
		sequence, items := newSelectiveAckRecoveryTestSequence(2, time.Unix(1_700_000_000, 0))
		sequence.client = &Client{}
		sequence.flightController = newSendFlightController(sequence.sendBufferSettings)
		sequence.flightController.applyPolicy(transferFlightPolicySnapshot{
			generation:             1,
			limited:                true,
			reliableRouteAvailable: arm.reliableSibling,
		})
		item := items[0]
		sequence.receiveAck(item.messageId, true, sequenceTag{}, false, arm.arrival)
		if !item.selectiveAcked {
			t.Errorf("%s: the item was not selectively acknowledged", arm.name)
			continue
		}
		if item.selectiveAckConclusive != arm.conclusive {
			t.Errorf(
				"%s: conclusive=%v, want %v: only an acknowledgement that could not have "+
					"overtaken a relay-borne reply proves a hole below it",
				arm.name, item.selectiveAckConclusive, arm.conclusive,
			)
		}
	}
}

// D4. A direct-lane hole proven by acknowledgements that took the relay is
// recovered at once: those replies could not have overtaken this hole's own.
func TestGapProvenByRelayLaneAcksRecoversAtOnce(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(time.Millisecond)
	sequence, items := arrivalSequence(t, 8, sendTime)
	for range 64 {
		sequence.flightController.send(1024)
		sequence.flightController.acknowledge(1024)
	}
	before := sequence.flightController.byteLimit
	arrivalHole(items, 0, true, true)
	reduce := sequence.scheduleSelectiveAckRecovery(currentTime)
	if items[0].resendTime.After(currentTime) {
		t.Fatalf(
			"a direct-lane hole proven by three relay-borne acknowledgements still waits %s: "+
				"a reply that took the relay cannot have overtaken this hole's own reply",
			items[0].resendTime.Sub(currentTime),
		)
	}
	if items[0].gapRecoveryDeferred {
		t.Fatal("the recovery was marked deferred though the hole was proven")
	}
	if !reduce {
		t.Fatal("a proven direct-lane hole did not ask for the flight to be reduced")
	}
	sequence.flightController.reduceForLoss()
	if before <= sequence.flightController.byteLimit {
		t.Fatalf("the window is still %d bytes after a proven hole, was %d",
			sequence.flightController.byteLimit, before)
	}
	if !sequence.unreliableLaneLosing() {
		t.Fatal("a proven direct-lane hole did not latch the lane")
	}
}

// D4. A direct-lane hole proven only by acknowledgements that took the fast
// lane waits one sequence round trip, the longest its own reply can take,
// and the reply cancels it without a write.
func TestGapProvenOnlyByFastLaneAcksWaitsForTheRelayClock(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(time.Millisecond)
	sequence, items := arrivalSequence(t, 8, sendTime)
	for range 64 {
		sequence.flightController.send(1024)
		sequence.flightController.acknowledge(1024)
	}
	before := sequence.flightController.byteLimit
	arrivalHole(items, 0, true, false)
	sequenceClock := sequence.rttWindow.ScaledRtt()
	reduce := sequence.scheduleSelectiveAckRecovery(currentTime)
	if due := items[0].resendTime.Sub(sendTime); due != sequenceClock {
		t.Fatalf(
			"a hole proven only by fast-lane acknowledgements is due %s after its send, want the "+
				"sequence clock %s: those acknowledgements can have overtaken a relay-borne reply",
			due, sequenceClock,
		)
	}
	if !items[0].gapRecoveryDeferred {
		t.Fatal("the deferred recovery was not marked, so its expiry cannot be attributed")
	}
	if reduce {
		t.Fatal("a hole that is not yet proven asked for the flight to be reduced")
	}
	if sequence.flightController.byteLimit != before {
		t.Fatalf("the window moved from %d to %d on evidence that has not arrived",
			before, sequence.flightController.byteLimit)
	}
	if sequence.unreliableLaneLosing() {
		t.Fatal("an unproven hole latched the lane")
	}
	// the item's own acknowledgement cancels the deferral without a write
	sequence.receiveAck(items[0].messageId, true, sequenceTag{}, false, CarrierReliabilityReliable)
	if items[0].gapRecoveryDeferred {
		t.Fatal("the acknowledgement did not cancel the deferred recovery")
	}
	if sequence.client.SendRecoveryStats().SelectiveGapWriteCount != 0 {
		t.Fatal("a deferred recovery that was cancelled still wrote")
	}
}

// D5. The lane latch no longer withholds the timeout deferral: it drives
// conclusiveness and nothing else.
func TestTimeoutDeferIsNotWithheldByTheLaneLatch(t *testing.T) {
	settings := DefaultSendBufferSettings()
	if !settings.DeferTimeoutResendWhileCumulativeProgress {
		t.Skip("the timeout deferral is off by default")
	}
	sendTime := time.Unix(1_700_000_000, 0)
	sequence, _ := arrivalSequence(t, 1, sendTime)
	sequence.noteUnreliableLaneLoss()
	if !sequence.unreliableLaneLosing() {
		t.Fatal("the lane did not latch")
	}
	scaledRtt := sequence.rttWindow.ScaledRtt()
	now := time.Now()
	relayItem := &sendItem{sendTime: now.Add(-scaledRtt / 2), reliableCarrierObserved: true}
	sequence.lastCumulativeAckTime = now
	if !sequence.shouldDeferTimeoutResend(relayItem, scaledRtt) {
		t.Fatal(
			"a relay-carried timeout with cumulative progress since its send is not deferred " +
				"while the direct lane is latched: the latch must drive conclusiveness only",
		)
	}
	// the deferral still stops for an item the direct lane carried, and for
	// one nothing has acknowledged since its last deferral
	directItem := &sendItem{sendTime: now.Add(-scaledRtt / 2), unreliableCarrierObserved: true}
	if sequence.shouldDeferTimeoutResend(directItem, scaledRtt) {
		t.Fatal("a direct-carried timeout was deferred")
	}
	relayItem.timeoutDeferAckTime = now
	if sequence.shouldDeferTimeoutResend(relayItem, scaledRtt) {
		t.Fatal("a timeout was deferred again with no cumulative progress since the last deferral")
	}
	relayItem.timeoutDeferAckTime = time.Time{}
	relayItem.timeoutDeferCount = settings.TimeoutResendDeferLimit
	if sequence.shouldDeferTimeoutResend(relayItem, scaledRtt) {
		t.Fatalf("a timeout was deferred past the limit of %d", settings.TimeoutResendDeferLimit)
	}
}

// D5. A relay-carried hole whose deferral expires says nothing about the
// direct lane.
func TestRelayHoleGraceExpiryDoesNotLatchTheDirectLane(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(time.Millisecond)
	sequence, items := arrivalSequence(t, 8, sendTime)
	// a relay-carried hole, deferred because it is younger than the clock
	hole := items[0]
	hole.reliableCarrierObserved = true
	hole.unreliableCarrierObserved = false
	hole.unreliableFlightTracked = false
	hole.sendTime = currentTime
	for index := 1; index < len(items); index += 1 {
		items[index].selectiveAcked = true
		items[index].selectiveAckConclusive = true
	}
	sequence.scheduleSelectiveAckRecovery(currentTime)
	if !hole.gapRecoveryDeferred {
		t.Fatal("a fresh relay-carried hole was not deferred")
	}
	// the deferral expires: this is the branch that used to latch the lane
	hole.gapRecoveryDeferred = false
	if hole.unreliableCarrierObserved {
		sequence.noteUnreliableLaneLoss()
	}
	if sequence.unreliableLaneLosing() {
		t.Fatal(
			"a relay-carried hole whose deferral expired latched the direct lane: its delivery " +
				"outran the deferral, which says nothing about the direct lane",
		)
	}
}

// D6. However many deferrals expire in one pass of the resend queue, the
// flight halves once, merged's per-round cadence.
func TestDeferredExpiriesReduceOncePerPass(t *testing.T) {
	sequence, _ := arrivalSequence(t, 1, time.Unix(1_700_000_000, 0))
	for range 64 {
		sequence.flightController.send(1024)
		sequence.flightController.acknowledge(1024)
	}
	before := sequence.flightController.byteLimit
	reducedThisPass := false
	for range 4 {
		item := &sendItem{
			unreliableCarrierObserved: true,
			unreliableFlightTracked:   true,
			gapRecoveryDeferred:       true,
		}
		reducedThisPass = sequence.observeDeferredRecoveryExpiry(item, reducedThisPass)
	}
	stats := sequence.client.SendRecoveryStats()
	if stats.UnreliableFlightGapCount != 4 {
		t.Fatalf("counted %d gaps for four expired deferrals, want 4", stats.UnreliableFlightGapCount)
	}
	if stats.UnreliableFlightReductionCount != 1 {
		t.Fatalf("four expired deferrals in one pass reduced the flight %d times, want 1",
			stats.UnreliableFlightReductionCount)
	}
	if stats.DeferredExpiriesByHoleCarrier[holeCarrierUnreliable] != 4 {
		t.Fatalf("attributed %d expiries to the direct lane, want 4",
			stats.DeferredExpiriesByHoleCarrier[holeCarrierUnreliable])
	}
	if want := before / 2; sequence.flightController.byteLimit != want {
		t.Fatalf(
			"four expired deferrals in one pass left the window at %d, want one halving to %d from %d",
			sequence.flightController.byteLimit, want, before,
		)
	}
}

// The arrival lane rides in existing padding: neither ack-path struct grew.
// The baselines are the sizes at c289a7e, before D4 added the bool to
// sendItem and the byte to the two ack structs.
const (
	sendItemByteCountBaseline          = 560
	sequenceAckByteCountBaseline       = 88
	receiveAckMessageByteCountBaseline = 72
)

func TestAckStructsGainNoBytes(t *testing.T) {
	if got, want := unsafe.Sizeof(sendItem{}), uintptr(sendItemByteCountBaseline); got != want {
		t.Errorf("sendItem is %d bytes, want %d: the arrival-lane bool must sit in existing padding",
			got, want)
	}
	if got, want := unsafe.Sizeof(sequenceAck{}), uintptr(sequenceAckByteCountBaseline); got != want {
		t.Errorf("sequenceAck is %d bytes, want %d", got, want)
	}
	if got, want := unsafe.Sizeof(receiveAckMessage{}), uintptr(receiveAckMessageByteCountBaseline); got != want {
		t.Errorf("receiveAckMessage is %d bytes, want %d", got, want)
	}
}

// FLIGHTGATEFIX §19.7. A deferral holds the receiver's ordered stream at
// the hole while the sender's resend queue fills behind it, so it must
// never wait the 2 s cold floor on a window that has measured nothing.
func TestGapDeferralNeverWaitsTheColdFloor(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(time.Millisecond)
	newSequence := func(sampled bool) (*SendSequence, []*sendItem) {
		sequence, items := newSelectiveAckRecoveryTestSequence(8, sendTime)
		sequence.client = &Client{}
		sequence.flightController = newSendFlightController(sequence.sendBufferSettings)
		sequence.flightController.applyPolicy(transferFlightPolicySnapshot{
			generation:             1,
			limited:                true,
			reliableRouteAvailable: true,
		})
		if sampled {
			sequence.rttWindow.CloseSendTime(
				uint64(time.Now().Add(-300 * time.Millisecond).UnixMilli()))
		}
		arrivalHole(items, 0, true, false)
		return sequence, items
	}

	unsampled, unsampledItems := newSequence(false)
	settings := unsampled.sendBufferSettings
	unsampled.scheduleSelectiveAckRecovery(currentTime)
	if due := unsampledItems[0].resendTime.Sub(sendTime); due != settings.RttMinResendInterval {
		t.Fatalf(
			"with a relay that has carried nothing the deferral is %s, want the pacing floor %s: "+
				"the %s cold floor would stall the ordered stream with no evidence behind it",
			due, settings.RttMinResendInterval, settings.MinResendInterval,
		)
	}

	sampled, sampledItems := newSequence(true)
	sampled.scheduleSelectiveAckRecovery(currentTime)
	if due := sampledItems[0].resendTime.Sub(sendTime); due != 600*time.Millisecond {
		t.Fatalf("with the relay measured at 300 ms the deferral is %s, want 600ms", due)
	}
}
