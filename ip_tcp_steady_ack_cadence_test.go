package connect

import (
	"io"
	"sync/atomic"
	"testing"
	"time"
)

// THROUGHPUTFIX §45.2. The acknowledgement clock a saturated upload runs on,
// and the reason upload reached nothing at any memory budget without it.
//
// The NAT has two steady-state acknowledgement triggers: the half-window
// signal, which fires when the unacknowledged bytes reach half the current
// window rung, and the compression timer at `AckCompressTimeout`. The signal
// keys on the advertised window, so it cannot fire while the sender keeps less
// than half a rung in flight; at the ladder's 16 MiB rung that half is 8 MiB,
// 67 ms of data at a gigabit, so the timer fires first. A sender is bounded by
// its send buffer over its round trip, and that round trip is then the path
// plus the compression interval rather than the path: 4 MiB over `P + 60 ms`
// is under a gigabit at every path length, so the timer and not the window is
// the ceiling. A cadence in segments is a clock that does not wait on the
// window having grown.
//
// This is not §26's recovery quickack, which is a separate mechanism with its
// own rows in ip_tcp_ack_starvation_test.go. That phase is entered only on
// evidence that the peer's window is small — loss, connection start,
// resumption after idle — and is bounded so that it cannot run in steady
// state, which is the regime this row is in. It is off here, and this row
// still holds.
//
// Driven by ordering rather than by a clock, because the defect is an ordering
// property: k in-order segments are a count, and on the tree before this the
// nothing counts them. The row asserts the decisions the send loop takes and
// not the acknowledgements that leave, because the signal those share is a
// one-deep channel that coalesces and the packet is built on another
// goroutine — counting packets would be counting the scheduler. Nothing here
// sleeps, measures elapsed time, or waits on a timeout that a slow machine
// could lose.
func TestSteadyUploadIsAcknowledgedOnACadenceNotOnTheWindowHavingGrown(t *testing.T) {
	assertMessagePoolOwnership(t)

	const steadyAckEverySegments = 16
	const segmentByteCount = 1400
	const segmentCount = 64

	var halfWindowCount atomic.Int64
	var cadenceCount atomic.Int64
	harness := newTcpReorderTestHarnessWithSetup(t, 1000, 128, 0, func(sequence *TcpSequence) {
		// the shipping compression clock. Whether it fires during this row
		// changes nothing it asserts: an acknowledgement it sends only shrinks
		// the unacknowledged bytes, which moves the half-window trigger
		// further away, and the cadence counts segments rather than time.
		sequence.tcpBufferSettings.AckCompressTimeout = 50 * time.Millisecond
		sequence.tcpBufferSettings.SteadyAckEverySegments = steadyAckEverySegments
		// §26's phase is off: this is the steady state, which that phase is
		// bounded to stay out of
		sequence.tcpBufferSettings.QuickackEverySegments = 0
		sequence.afterAckClockForTest = func(clock tcpAckClock) {
			switch clock {
			case tcpAckClockHalfWindow:
				halfWindowCount.Add(1)
			case tcpAckClockSteadyCadence:
				cadenceCount.Add(1)
			}
		}
	})
	go io.Copy(io.Discard, harness.upstreamSocket)

	// The regime, asserted from the live window rather than assumed: every
	// byte this row sends, unacknowledged, stays below half the rung, so the
	// half-window signal cannot fire and the timer is the only clock the tree
	// had. The window cannot grow here either, since growth needs a full
	// window of payload through the ladder.
	harness.sequence.mutex.Lock()
	windowSize := harness.sequence.windowSize
	harness.sequence.mutex.Unlock()
	if windowSize/2 <= segmentCount*segmentByteCount {
		t.Fatalf(
			"%d segments of %d bytes reach half the %d byte rung, so the half-window signal could fire and this row would not be in the regime it is about",
			segmentCount,
			segmentByteCount,
			windowSize,
		)
	}

	payload := string(make([]byte, segmentByteCount))
	seq := harness.nextSeq
	for range segmentCount {
		harness.sendPayload(seq, payload, false)
		seq += segmentByteCount
	}

	// The barrier, which is what makes the count deterministic. One send loop
	// consumes these items in arrival order, so a retransmission of bytes it
	// has already accepted is dispositioned strictly after every segment above
	// has been through the acknowledgement rules, and the disposition is
	// reported before that path acknowledges anything of its own.
	harness.sendPayload(harness.nextSeq, payload, false)
	harness.waitReorderDecision(tcpReorderDispositionStale)

	wantCadenceCount := int64(segmentCount / steadyAckEverySegments)
	if cadenceCount.Load() != wantCadenceCount {
		t.Errorf(
			"%d in-order segments drew %d cadence acknowledgements, want %d, one per %d segments; with none of them the sender's only clock is the %s compression timer, its round trip is the path plus that interval, and its send buffer over that is what bounds the upload",
			segmentCount,
			cadenceCount.Load(),
			wantCadenceCount,
			steadyAckEverySegments,
			50*time.Millisecond,
		)
	}
	// the contract, stated as itself: the clock did not come from the window
	if halfWindowCount.Load() != 0 {
		t.Errorf(
			"the half-window rule asked for %d acknowledgements, so this row was not in the regime where it cannot fire and proves nothing about a clock independent of the window",
			halfWindowCount.Load(),
		)
	}
}

// §45.2, the other half of the cadence's contract: it is a cadence and not an
// acknowledgement per segment. A sender that stops short of k draws nothing
// from it and is left to the backstops, the half-window signal and the
// compression timer, which is what bounds what the rule costs in
// acknowledgement traffic. Driven and asserted exactly as the row above.
func TestSteadyCadenceDoesNotAcknowledgeShortOfItsSpacing(t *testing.T) {
	assertMessagePoolOwnership(t)

	const steadyAckEverySegments = 16
	const segmentByteCount = 1400
	// one short of the spacing, so the cadence must not have fired
	const segmentCount = steadyAckEverySegments - 1

	var cadenceCount atomic.Int64
	harness := newTcpReorderTestHarnessWithSetup(t, 1000, 128, 0, func(sequence *TcpSequence) {
		sequence.tcpBufferSettings.AckCompressTimeout = 50 * time.Millisecond
		sequence.tcpBufferSettings.SteadyAckEverySegments = steadyAckEverySegments
		sequence.tcpBufferSettings.QuickackEverySegments = 0
		sequence.afterAckClockForTest = func(clock tcpAckClock) {
			if clock == tcpAckClockSteadyCadence {
				cadenceCount.Add(1)
			}
		}
	})
	go io.Copy(io.Discard, harness.upstreamSocket)

	payload := string(make([]byte, segmentByteCount))
	seq := harness.nextSeq
	for range segmentCount {
		harness.sendPayload(seq, payload, false)
		seq += segmentByteCount
	}
	harness.sendPayload(harness.nextSeq, payload, false)
	harness.waitReorderDecision(tcpReorderDispositionStale)

	if cadenceCount.Load() != 0 {
		t.Errorf(
			"%d segments, one short of a %d segment spacing, drew %d cadence acknowledgements; a rule that fires short of k is an acknowledgement per segment wearing a spacing, and the cost of the cadence is exactly the acknowledgements it sends",
			segmentCount,
			steadyAckEverySegments,
			cadenceCount.Load(),
		)
	}
}
