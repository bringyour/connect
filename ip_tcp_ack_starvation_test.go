package connect

import (
	"io"
	"sync/atomic"
	"testing"
	"time"
)

// The acknowledgement loop sends one acknowledgement and then waits for
// whichever comes first of the compression timer and a signal the send loop
// raises when the unacknowledged bytes reach half the current window rung. The
// signal is the fast clock and the timer is the slow one.
//
// The signal keys on the window rung, so it cannot fire while the peer keeps
// less than half a rung in flight — which is every slow start and every
// post-loss recovery, because those are exactly the periods when the peer's
// congestion window is small. During them the timer is the only clock, and
// since a peer grows its congestion window per acknowledgement received rather
// than per byte acknowledged, its recovery is throttled to one segment of
// growth per compression interval.
//
// Measured consequence on the rig: a run that normally finishes in 1.4 s took
// 48.7, at one acknowledgement per 198 ms against one per 74 ms healthy.
//
// The severity scales the wrong way round, which is worth stating because it
// is counterintuitive: the penalty per growth step is about twice the healthy
// interval on a 50 ms path and about fifty-one times on a 1 ms path. Fast paths
// suffer far more per event; lossy paths merely trigger it more often.
//
// This pins the starvation rather than the collapse it produces, and it is
// written as the property that must hold rather than the one that does: it
// fails on the tree as built, where 107 in-order segments over 602 ms drew
// exactly 12 acknowledgements, one per 50.19 ms, with the half-window never
// reached. It passes on a tree whose acknowledgements follow the peer.
func TestAckCompressionIsTheOnlyClockBelowHalfAWindowRung(t *testing.T) {
	assertMessagePoolOwnership(t)

	const ackCompressTimeout = 50 * time.Millisecond
	const segmentByteCount = 100
	const segmentInterval = 5 * time.Millisecond
	const observationWindow = 600 * time.Millisecond

	harness := newTcpReorderTestHarnessWithSetup(t, 1000, 32, 0, func(sequence *TcpSequence) {
		sequence.tcpBufferSettings.AckCompressTimeout = ackCompressTimeout
	})
	// the upstream is a pipe: nothing may block the send loop behind it
	go io.Copy(io.Discard, harness.upstreamSocket)

	windowSize := harness.sequence.tcpBufferSettings.InitialWindowSize
	// by construction the signal cannot fire: at one segment per interval the
	// most that can accumulate between two timer ticks is far below half a rung
	maxUnackedByteCount := uint32(segmentByteCount) * uint32(ackCompressTimeout/segmentInterval+1)
	if windowSize/2 <= maxUnackedByteCount {
		t.Fatalf(
			"at most %d bytes go unacknowledged between ticks, which reaches half the %d byte rung; this row needs the signal to stay silent",
			maxUnackedByteCount,
			windowSize,
		)
	}

	// the harness's ack channel is bounded and drops, so the count is taken
	// as they arrive rather than from what is left in it
	var ackCount atomic.Int64
	acksDrained := make(chan struct{})
	go func() {
		defer close(acksDrained)
		for range harness.acks {
			ackCount.Add(1)
		}
	}()

	payload := string(make([]byte, segmentByteCount))
	// in-order segments: a repeated sequence number would be a retransmission
	// and draw an immediate duplicate acknowledgement, which is a different
	// rule from the one under test
	seq := harness.nextSeq
	segmentCount := 0
	started := time.Now()
	for time.Since(started) < observationWindow {
		harness.sendPayload(seq, payload, false)
		seq += segmentByteCount
		segmentCount += 1
		time.Sleep(segmentInterval)
	}
	elapsed := time.Since(started)

	// The remedy's property, asserted as the thing that must hold rather than
	// as the starvation it replaces: a peer below half a rung must be
	// acknowledged faster than the timer alone allows. Parameter free, so it
	// does not depend on a spacing this row has no business choosing.
	timerAckCount := int(elapsed / ackCompressTimeout)
	if int(ackCount.Load()) <= 2*timerAckCount {
		t.Errorf(
			"%d in-order segments over %s drew %d acknowledgements, one per %s, against the %d the %s compression timer alone gives; the peer's window is below half the %d byte rung for all of it, so the half-window signal cannot fire and the timer is the only clock, and a peer that grows one segment per acknowledgement recovers at that rate",
			segmentCount,
			elapsed,
			ackCount.Load(),
			elapsed/time.Duration(max(1, ackCount.Load())),
			timerAckCount,
			ackCompressTimeout,
			windowSize,
		)
	}
	if ackCount.Load() <= 0 {
		t.Fatal("no acknowledgement was observed, so this window measured nothing")
	}

	t.Logf(
		"%d segments of %d bytes over %s drew %d acknowledgements, one per %s, against a %s compression timer and a %d byte rung whose half was never reached",
		segmentCount,
		segmentByteCount,
		elapsed,
		ackCount.Load(),
		elapsed/time.Duration(max(1, ackCount.Load())),
		ackCompressTimeout,
		windowSize,
	)
}
