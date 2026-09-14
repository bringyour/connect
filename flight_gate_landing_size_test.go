package connect

// FLIGHTGATEFIX §20.3. The landing's memory against merged's. Everything
// this program added to the ack path is gone, so the two acknowledgement
// structs are merged's to the byte. sendItem carries §13.5's per-item
// deferral state and §34.3's per-item lane position, which are the two
// mechanisms the landing keeps, and that state is what the extra bytes are.

import (
	"testing"
	"unsafe"
)

const (
	mergedSendItemByteCount          = 520
	mergedSequenceAckByteCount       = 88
	mergedReceiveAckMessageByteCount = 72
	// §13.5's timeoutDeferCount and timeoutDeferAckTime
	deferStateByteCount = 32
	// §34.3's laneAckedAtLastFiring: where this item's own lane had got to
	// when it last looked. §34.5 expected the rule to need no new bytes,
	// because the per-route slots already hold the sequence numbers its
	// rules read. They do not hold this one: rule 2 asks what moved on the
	// lane since this item last looked, which is per item and per position,
	// and a slot holds one number for the whole lane. The field is placed
	// against the struct's 8-byte tail, so it costs its own 8 bytes and no
	// padding.
	lanePositionStateByteCount = 8
	// THROUGHPUTFIX §37.3's receive advertisement: what the receiver said it
	// can still hold out of order, and whether it said anything at all. It is
	// a mechanism the landing keeps rather than a field left behind, and it
	// has to live on this struct because the sender clamps its window to the
	// latest advertised value and both wire paths decode into it. The count
	// is the narrow type and the flag packs against the existing bools, so
	// the pair costs one word rather than two.
	receiveAdvertisementStateByteCount = 8
)

func TestLandingStructsMatchMergedLessTheDeferState(t *testing.T) {
	if got, want := unsafe.Sizeof(sequenceAck{}), uintptr(mergedSequenceAckByteCount); got != want {
		t.Errorf("sequenceAck is %d bytes, want merged's %d: the ack path carries nothing of this program's",
			got, want)
	}
	if got, want := unsafe.Sizeof(receiveAckMessage{}),
		uintptr(mergedReceiveAckMessageByteCount+receiveAdvertisementStateByteCount); got != want {
		t.Errorf(
			"receiveAckMessage is %d bytes, want merged's %d plus %d for the receiver's advertised remaining capacity",
			got, mergedReceiveAckMessageByteCount, receiveAdvertisementStateByteCount,
		)
	}
	want := uintptr(
		mergedSendItemByteCount + deferStateByteCount + lanePositionStateByteCount)
	if got := unsafe.Sizeof(sendItem{}); got != want {
		t.Errorf(
			"sendItem is %d bytes, want merged's %d plus %d for the deferred retransmit's own state "+
				"and %d for the lane position it last looked at; "+
				"anything else means a removed mechanism left a field behind",
			got, mergedSendItemByteCount, deferStateByteCount, lanePositionStateByteCount,
		)
	}
}
