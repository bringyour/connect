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
	const segmentByteCount = 1400
	const segmentInterval = 5 * time.Millisecond
	const observationWindow = 600 * time.Millisecond
	const quickackEverySegments = 2
	// more than this window sends, so the connection-start phase covers it
	const startQuickackByteCount = ByteCount(1024 * 1024)

	harness := newTcpReorderTestHarnessWithSetup(t, 1000, 32, 0, func(sequence *TcpSequence) {
		sequence.tcpBufferSettings.AckCompressTimeout = ackCompressTimeout
		// the recovery phase, which the shipping default leaves off; the whole
		// window of this row is connection start, which is E2
		sequence.tcpBufferSettings.QuickackEverySegments = quickackEverySegments
		sequence.tcpBufferSettings.StartQuickackByteCount = startQuickackByteCount
		sequence.tcpBufferSettings.RecoveryQuickackByteBound = startQuickackByteCount
		// the peer's segment size, which the spacing is counted in; the
		// harness's SYN carries no MSS option
		sequence.peerMss = segmentByteCount
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

// drains whatever acknowledgements are already queued, so a row measures only
// what follows
func drainHarnessAcks(harness *tcpReorderTestHarness) {
	for {
		select {
		case <-harness.acks:
		default:
			return
		}
	}
}

// waits for one acknowledgement, reporting whether it arrived inside the bound
func waitHarnessAck(harness *tcpReorderTestHarness, timeout time.Duration) bool {
	select {
	case <-harness.acks:
		return true
	case <-time.After(timeout):
		return false
	}
}

// THROUGHPUTFIX §26.9 row Q7. With the counting rule alone, the first round
// after a timeout cannot be acknowledged by it: the peer's window is one
// segment, fewer than the spacing, so no counting acknowledgement fires and
// the round waits on a timer. That round is the critical path of the whole
// recovery. The phase's first segments are therefore acknowledged at once.
func TestFirstSegmentAfterATimeoutIsAckedAtOnce(t *testing.T) {
	assertMessagePoolOwnership(t)

	const ackCompressTimeout = 2 * time.Second
	const segmentByteCount = 1400
	// far more than one segment, so the counting rule cannot reach it
	const quickackEverySegments = 8

	harness := newTcpReorderTestHarnessWithSetup(t, 1000, 32, 0, func(sequence *TcpSequence) {
		sequence.tcpBufferSettings.AckCompressTimeout = ackCompressTimeout
		sequence.tcpBufferSettings.QuickackEverySegments = quickackEverySegments
		sequence.tcpBufferSettings.QuickackImmediateSegmentCount = 1
		// the burst-end trigger is off: only the immediate rule may answer
		sequence.tcpBufferSettings.QuiescenceBound = 0
		sequence.tcpBufferSettings.StartQuickackByteCount = 0
		sequence.peerMss = segmentByteCount
	})
	go io.Copy(io.Discard, harness.upstreamSocket)

	payload := string(make([]byte, segmentByteCount))
	seq := harness.nextSeq
	harness.sendPayload(seq, payload, false)
	if !waitHarnessAck(harness, 5*time.Second) {
		t.Fatal("the first in-order segment was never acknowledged")
	}
	// the loss evidence: a retransmission of bytes already accepted
	harness.sendPayload(seq, payload, false)
	drainHarnessAcks(harness)

	// one in-order segment, which is the first round of the recovery
	harness.sendPayload(seq+segmentByteCount, payload, false)
	if !waitHarnessAck(harness, ackCompressTimeout/4) {
		t.Errorf(
			"the first in-order segment after loss evidence was not acknowledged within %s; with a spacing of %d segments the counting rule cannot reach one segment, so this round waits on the %s timer and it is the critical path of the recovery",
			ackCompressTimeout/4,
			quickackEverySegments,
			ackCompressTimeout,
		)
	}
}

// §26.9 row Q8. The burst-end trigger asks only whether the last arrival was
// longer ago than the bound, so it needs no inter-arrival estimate and works
// on a flow with no history. It arms on the first arrival after entry, re-arms
// on every arrival while anything is outstanding, and disarms when an
// acknowledgement covers everything.
func TestBurstEndTriggerArmsOnFirstArrivalAndRearms(t *testing.T) {
	assertMessagePoolOwnership(t)

	const ackCompressTimeout = 2 * time.Second
	const quiescenceBound = 50 * time.Millisecond
	const segmentByteCount = 1400
	const arrivalInterval = 10 * time.Millisecond

	harness := newTcpReorderTestHarnessWithSetup(t, 1000, 32, 0, func(sequence *TcpSequence) {
		sequence.tcpBufferSettings.AckCompressTimeout = ackCompressTimeout
		// the counting rule is out of reach, and nothing is acknowledged at
		// once, so only the burst-end trigger can answer
		sequence.tcpBufferSettings.QuickackEverySegments = 1024
		sequence.tcpBufferSettings.QuickackImmediateSegmentCount = 0
		sequence.tcpBufferSettings.QuiescenceBound = quiescenceBound
		sequence.tcpBufferSettings.StartQuickackByteCount = ByteCount(1024 * 1024)
		sequence.peerMss = segmentByteCount
	})
	go io.Copy(io.Discard, harness.upstreamSocket)

	payload := string(make([]byte, segmentByteCount))
	seq := harness.nextSeq
	// the establishing segment's acknowledgement is not what this row measures
	harness.sendPayload(seq, payload, false)
	seq += segmentByteCount
	waitHarnessAck(harness, 5*time.Second)
	drainHarnessAcks(harness)

	// arrivals closer together than the bound: the trigger re-arms on each and
	// never fires mid-burst
	burstStarted := time.Now()
	for time.Since(burstStarted) < 6*quiescenceBound {
		harness.sendPayload(seq, payload, false)
		seq += segmentByteCount
		time.Sleep(arrivalInterval)
	}
	if waitHarnessAck(harness, 0) {
		t.Error("an acknowledgement left mid-burst; the trigger re-arms on every arrival while anything is outstanding")
	}

	// the burst ends with bytes outstanding: exactly one acknowledgement
	if !waitHarnessAck(harness, 4*quiescenceBound) {
		t.Fatalf("no acknowledgement left within %s of the burst ending, although bytes were outstanding", 4*quiescenceBound)
	}
	// and nothing is outstanding now, so the trigger is disarmed
	time.Sleep(4 * quiescenceBound)
	if waitHarnessAck(harness, 0) {
		t.Error("a second acknowledgement left with nothing outstanding; the trigger must disarm when an acknowledgement covers everything")
	}
}

// THROUGHPUTFIX §26.8 row Q6, the row that guards steady state while the
// others guard recovery. The entry condition must be evidence that the peer's
// window is small — loss, connection start, resumption after idle — and never
// a byte count. "Bytes since the last acknowledgement are under half the rung"
// is true at the start of every interval of every flow: it would satisfy every
// recovery row and acknowledge every k segments of a saturated upload for
// ever, twenty thousand a second at 465 Mb/s, on the client's downlink.
//
// So: a flow past its start phase, sending continuously in order with no loss
// and no idle, must fall back to the timer. It is allowed the start phase's
// own acknowledgements and no more.
func TestSteadyStateUploadEmitsNoQuickacks(t *testing.T) {
	assertMessagePoolOwnership(t)

	const ackCompressTimeout = 50 * time.Millisecond
	const segmentByteCount = 1400
	const segmentInterval = 5 * time.Millisecond
	const observationWindow = 600 * time.Millisecond
	const quickackEverySegments = 2
	// the start phase and its bound, both short, so the flow is in steady
	// state for nearly all of the window
	const startQuickackByteCount = ByteCount(10 * 1024)

	harness := newTcpReorderTestHarnessWithSetup(t, 1000, 32, 0, func(sequence *TcpSequence) {
		sequence.tcpBufferSettings.AckCompressTimeout = ackCompressTimeout
		sequence.tcpBufferSettings.QuickackEverySegments = quickackEverySegments
		sequence.tcpBufferSettings.QuickackImmediateSegmentCount = 1
		sequence.tcpBufferSettings.StartQuickackByteCount = startQuickackByteCount
		sequence.tcpBufferSettings.RecoveryQuickackByteBound = startQuickackByteCount
		sequence.tcpBufferSettings.QuiescenceBound = 0
		sequence.peerMss = segmentByteCount
	})
	go io.Copy(io.Discard, harness.upstreamSocket)

	var ackCount atomic.Int64
	acksDrained := make(chan struct{})
	go func() {
		defer close(acksDrained)
		for range harness.acks {
			ackCount.Add(1)
		}
	}()

	payload := string(make([]byte, segmentByteCount))
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

	timerAckCount := int(elapsed / ackCompressTimeout)
	// the start phase's own, at the configured spacing, plus its immediate one
	startAckCount := int(startQuickackByteCount)/(quickackEverySegments*segmentByteCount) + 2
	countingRuleAckCount := segmentCount / quickackEverySegments
	allowedAckCount := 2*timerAckCount + startAckCount

	if allowedAckCount < int(ackCount.Load()) {
		t.Errorf(
			"%d segments over %s drew %d acknowledgements, above the %d a %s timer plus a %d byte start phase allow; the counting rule alone would give %d, so the phase is being entered on something other than loss, start or idle and steady state is paying for recovery",
			segmentCount,
			elapsed,
			ackCount.Load(),
			allowedAckCount,
			ackCompressTimeout,
			startQuickackByteCount,
			countingRuleAckCount,
		)
	}
	t.Logf(
		"%d segments drew %d acknowledgements against %d allowed (%d from the timer, %d from the start phase); the counting rule alone would give %d",
		segmentCount,
		ackCount.Load(),
		allowedAckCount,
		timerAckCount,
		startAckCount,
		countingRuleAckCount,
	)
}
