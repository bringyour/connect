//go:build flightgate_next

package connect

// FLIGHTGATEFIX §20.3. The §14 and §16 rows of the fallback tests: the
// mixed-lane grace, what it defers and when a losing lane withdraws it.
// The landing's scoreboard is merged's, so these are the specification of
// the affinity candidate in §20.5 rather than a gate.

import (
	"testing"
	"time"
)

// FLIGHTGATEFIX §14 (M3). While acknowledgements arrive over two lanes of
// different latency, "three later selective acks" says nothing about this
// item: the later acks may simply have taken the faster lane. The merged
// rule granted that grace only to reliable-carried items, so an item the
// direct lane carried whose ack took the relay (its bounded reply route was
// full) was read as lost and resent. With a single ack lane the ordering
// rule is unchanged, so datagram tail recovery keeps its pace.
func TestSelectiveAckGapSkipsUnreliableItemsWhileBothLanesCarryAcks(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(100 * time.Millisecond)
	newMixed := func() (*SendSequence, []*sendItem) {
		sequence, items := newSelectiveAckRecoveryTestSequence(8, sendTime)
		sequence.client = &Client{}
		sequence.flightController = newSendFlightController(sequence.sendBufferSettings)
		sequence.flightController.applyPolicy(transferFlightPolicySnapshot{
			generation:             1,
			limited:                true,
			reliableRouteAvailable: true,
		})
		for _, index := range []int{1, 2, 3, 5, 6, 7} {
			items[index].selectiveAcked = true
		}
		return sequence, items
	}
	// item 0 rode the direct lane 100 ms ago and its ack is still in the air
	// on the relay; item 4 rode it long ago and is really missing
	sequence, items := newMixed()
	for _, index := range []int{0, 4} {
		items[index].unreliableCarrierObserved = true
		items[index].unreliableFlightTracked = true
	}
	items[4].sendTime = sendTime.Add(-5 * time.Second)
	sequence.scheduleSelectiveAckRecovery(currentTime)
	// nothing is written for the fresh item now: its recovery is due only
	// once the relay could have delivered its ack, and an ack arriving
	// first takes the item out of the queue
	if !items[0].resendTime.After(currentTime) {
		t.Fatalf("a fresh direct-lane item was gap-resent on ack-lane reordering: due %s",
			items[0].resendTime.Sub(currentTime))
	}
	if !items[4].selectiveGapRecovered || items[4].recoveryKind != sendRecoverySelectiveGap ||
		items[4].resendTime.After(currentTime) {
		t.Fatalf("a stale direct-lane item was not gap-resent: recovered=%t kind=%d",
			items[4].selectiveGapRecovered, items[4].recoveryKind)
	}

	// one ack lane only: the ordering rule is untouched, whichever lane.
	// With no reliable sibling every acknowledgement travels the only lane
	// there is, so every one is conclusive (FLIGHTGATEFIX §19 D4).
	sequenceOne, itemsOne := newSelectiveAckRecoveryTestSequence(8, sendTime)
	sequenceOne.client = &Client{}
	sequenceOne.flightController = newSendFlightController(sequenceOne.sendBufferSettings)
	sequenceOne.flightController.applyPolicy(transferFlightPolicySnapshot{generation: 1, limited: true})
	itemsOne[0].unreliableCarrierObserved = true
	itemsOne[0].unreliableFlightTracked = true
	for _, index := range []int{1, 2, 3, 5, 6, 7} {
		itemsOne[index].selectiveAcked = true
		itemsOne[index].selectiveAckConclusive = true
	}
	sequenceOne.scheduleSelectiveAckRecovery(currentTime)
	if !itemsOne[0].selectiveGapRecovered || itemsOne[0].resendTime.After(currentTime) {
		t.Fatal("datagram tail recovery regressed on a single-lane route")
	}
}

// FLIGHTGATEFIX §19 D1/D3. The deferral of a hole is to the slowest lane an
// acknowledgement can take, which is the sequence window's clock. The
// direct lane's own estimate cannot bound a wait for a reply the receiver
// may choose to send by the relay, so the per-carrier grace is gone with
// the per-carrier window.
func TestMixedLaneHoleWaitsForTheSequenceClock(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(10 * time.Millisecond)
	sequence, items := newSelectiveAckRecoveryTestSequence(8, sendTime)
	sequence.client = &Client{}
	sequence.flightController = newSendFlightController(sequence.sendBufferSettings)
	sequence.flightController.applyPolicy(transferFlightPolicySnapshot{
		generation:             1,
		limited:                true,
		reliableRouteAvailable: true,
	})
	// item 0 is a hole the direct lane carried
	items[0].unreliableCarrierObserved = true
	items[0].unreliableFlightTracked = true
	for _, index := range []int{1, 2, 3, 5, 6, 7} {
		items[index].selectiveAcked = true
	}
	// the relay has answered at 300 ms, the device rig's figure
	now := time.Now()
	sequence.rttWindow.CloseSendTime(uint64(now.Add(-300 * time.Millisecond).UnixMilli()))
	sequenceGrace := sequence.rttWindow.ScaledRtt()

	sequence.scheduleSelectiveAckRecovery(currentTime)
	if !items[0].selectiveGapRecovered || items[0].recoveryKind != sendRecoverySelectiveGap {
		t.Fatalf("a direct-lane hole was not scheduled for recovery: recovered=%t kind=%d",
			items[0].selectiveGapRecovered, items[0].recoveryKind)
	}
	if due := items[0].resendTime.Sub(sendTime); due != sequenceGrace {
		t.Fatalf(
			"recovery of a direct-lane hole is due %s after the send, want the sequence clock %s: "+
				"the reply may take the relay, so no carrier's own estimate bounds the wait",
			due, sequenceGrace,
		)
	}

	// a relay-carried hole waits the same clock
	relayItem := items[4]
	relayItem.sendTime = sendTime
	relayItem.reliableCarrierObserved = true
	relayItem.selectiveGapRecovered = false
	relayItem.recoveryKind = sendRecoveryNone
	relayItem.resendTime = sendTime.Add(sequence.sendBufferSettings.SelectiveAckTimeout)
	sequence.scheduleSelectiveAckRecovery(currentTime)
	if due := relayItem.resendTime.Sub(sendTime); due != sequenceGrace {
		t.Fatalf("a relay-carried hole is due %s after the send, want the sequence clock %s",
			due, sequenceGrace)
	}
	// the wait must never exceed what the item's own timeout would have cost,
	// or the trade stops paying
	if ceiling := sequence.sendBufferSettings.UnreliableMaxResendInterval; 0 < ceiling && ceiling < sequenceGrace {
		t.Fatalf("the wait %s is longer than the unreliable lane's own resend ceiling %s",
			sequenceGrace, ceiling)
	}
}

func TestMixedLaneGraceIsWithdrawnFromALosingLane(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(10 * time.Millisecond)
	newSequence := func() (*SendSequence, []*sendItem) {
		sequence, items := newSelectiveAckRecoveryTestSequence(8, sendTime)
		sequence.client = &Client{}
		sequence.flightController = newSendFlightController(sequence.sendBufferSettings)
		sequence.flightController.applyPolicy(transferFlightPolicySnapshot{
			generation:             1,
			limited:                true,
			reliableRouteAvailable: true,
		})
		sequence.rttWindow.CloseSendTime(uint64(time.Now().Add(-300 * time.Millisecond).UnixMilli()))
		items[0].unreliableCarrierObserved = true
		items[0].unreliableFlightTracked = true
		for _, index := range []int{1, 2, 3, 5, 6, 7} {
			items[index].selectiveAcked = true
		}
		return sequence, items
	}
	// nothing proven lost: the lane is reordering and the grace holds
	reordering, reorderingItems := newSequence()
	if reordering.unreliableLaneLosing() {
		t.Fatal("a lane with no proven loss is classified losing")
	}
	reordering.scheduleSelectiveAckRecovery(currentTime)
	if !reorderingItems[0].resendTime.After(currentTime) {
		t.Fatal("the grace was withdrawn from a lane with no proven loss")
	}
	if !reorderingItems[0].gapRecoveryDeferred {
		t.Fatal("a deferred recovery was not marked, so its outcome cannot be counted")
	}

	// one proven loss latches the lane, and the next hole waits for nothing
	losing, losingItems := newSequence()
	losing.noteUnreliableLaneLoss()
	if !losing.unreliableLaneLosing() {
		t.Fatal("one proven loss did not classify the lane as losing")
	}
	losing.scheduleSelectiveAckRecovery(currentTime)
	if losingItems[0].resendTime.After(currentTime) {
		t.Fatalf(
			"a hole on a losing lane still waits %s for a grace: the ordered stream stalls for it",
			losingItems[0].resendTime.Sub(currentTime),
		)
	}
	if losingItems[0].gapRecoveryDeferred {
		t.Fatal("an immediate recovery was marked deferred")
	}

	// hysteresis: the latch clears after a stated run of clean
	// acknowledgements, so the signal neither flaps nor sticks
	recovering, recoveringItems := newSequence()
	recovering.noteUnreliableLaneLoss()
	for range unreliableLaneLossHold - 1 {
		recovering.noteUnreliableLaneProgress()
	}
	if !recovering.unreliableLaneLosing() {
		t.Fatalf("the latch cleared in under %d clean acknowledgements", unreliableLaneLossHold)
	}
	recovering.noteUnreliableLaneProgress()
	if recovering.unreliableLaneLosing() {
		t.Fatalf("the latch did not clear after %d clean acknowledgements, so the signal sticks",
			unreliableLaneLossHold)
	}
	recovering.scheduleSelectiveAckRecovery(currentTime)
	if !recoveringItems[0].resendTime.After(currentTime) {
		t.Fatal("a lane that stopped losing did not get the grace back")
	}
}
