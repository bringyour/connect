package connect

// FLIGHTGATEFIX §20.3. The landing's memory against merged's. Everything
// this program added to the ack path is gone, so the two acknowledgement
// structs are merged's to the byte. sendItem carries §13.5's per-item
// deferral state, which is the one mechanism the landing keeps, and that
// state is what the extra bytes are.

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
)

func TestLandingStructsMatchMergedLessTheDeferState(t *testing.T) {
	if got, want := unsafe.Sizeof(sequenceAck{}), uintptr(mergedSequenceAckByteCount); got != want {
		t.Errorf("sequenceAck is %d bytes, want merged's %d: the ack path carries nothing of this program's",
			got, want)
	}
	if got, want := unsafe.Sizeof(receiveAckMessage{}), uintptr(mergedReceiveAckMessageByteCount); got != want {
		t.Errorf("receiveAckMessage is %d bytes, want merged's %d", got, want)
	}
	want := uintptr(mergedSendItemByteCount + deferStateByteCount)
	if got := unsafe.Sizeof(sendItem{}); got != want {
		t.Errorf(
			"sendItem is %d bytes, want merged's %d plus %d for the deferred retransmit's own state; "+
				"anything else means a removed mechanism left a field behind",
			got, mergedSendItemByteCount, deferStateByteCount,
		)
	}
}
