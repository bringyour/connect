package connect

import (
	"math"
	"sync"
	"testing"
	"time"
)

// --- exitMetricsSnapshot: the plumbing ---

// TestExitMetricsSnapshotNoDataIsNotZero pins the trap this whole task exists
// to avoid: exitScore sanitizes a bare 0 RTT/Jitter into the BEST possible
// sub-score (routing_score.go), so a fresh channel with no window activity
// and no timed round trip must NOT report 0 there. It must report NaN, which
// exitScore's existing hostile-input guard already reads as the WORST
// sub-score. Flows passes straight through (it is the caller's own read, not
// this function's job), and Goodput/StallEvents are legitimately 0 for an
// untouched channel -- 0 is the honest, non-exploitable reading for both
// (see exitScore's own comments on why those two need no NaN guard).
func TestExitMetricsSnapshotNoDataIsNotZero(t *testing.T) {
	client := stallTestChannel()

	m := exitMetricsSnapshot(client, 7)
	AssertEqual(t, m.Flows, 7)
	AssertEqual(t, m.GoodputBytesPerSec, float64(0))
	AssertEqual(t, m.StallEvents, 0)
	if !math.IsNaN(m.RttMillis) {
		t.Fatalf("unmeasured RTT must read NaN (exitScore's worst case), got %v", m.RttMillis)
	}
	if !math.IsNaN(m.Jitter) {
		t.Fatalf("unmeasured jitter must read NaN (exitScore's worst case), got %v", m.Jitter)
	}
}

// TestExitMetricsSnapshotNilClientIsSafe: the placement path must never panic
// on a nil candidate. Flows still passes through, and the telemetry fields
// read exactly like the no-data case above.
func TestExitMetricsSnapshotNilClientIsSafe(t *testing.T) {
	m := exitMetricsSnapshot(nil, 3)
	AssertEqual(t, m.Flows, 3)
	AssertEqual(t, m.GoodputBytesPerSec, float64(0))
	AssertEqual(t, m.StallEvents, 0)
	if !math.IsNaN(m.RttMillis) || !math.IsNaN(m.Jitter) {
		t.Fatalf("a nil client must read as unmeasured, got rtt=%v jitter=%v", m.RttMillis, m.Jitter)
	}
}

// TestExitMetricsSnapshotGoodputFromWindowStats proves the GoodputBytesPerSec
// wire: known, deterministic window stats (buckets and byte counts set
// directly, the same technique TestLifetimeJitterRemoveTimeUsesEffectiveLifetime
// uses, so the expected value does not depend on real sleep timing) must
// produce the EXACT value windowStatsWithCoalesce(false).EffectiveByteCountPerSecond()
// itself would compute -- proving exitMetricsSnapshot reads the real accessor
// rather than reimplementing or approximating it.
func TestExitMetricsSnapshotGoodputFromWindowStats(t *testing.T) {
	client := stallTestChannel()

	now := time.Now()
	client.stateLock.Lock()
	// two "settled" buckets spanning 2s, plus two "may be partial" buckets
	// windowStatsWithCoalesce(false) trims off -- this is the exact shape
	// windowStatsWithCoalesce's own doc describes.
	client.eventBuckets = []*multiClientEventBucket{
		{createTime: now.Add(-3 * time.Second), eventTime: now.Add(-3 * time.Second)},
		{createTime: now.Add(-2 * time.Second), eventTime: now.Add(-1 * time.Second)},
		{createTime: now, eventTime: now},
		{createTime: now, eventTime: now},
	}
	client.packetStats.sendAckByteCount = 500000
	client.packetStats.receiveAckByteCount = 500000
	client.stateLock.Unlock()

	wantStats, err := client.windowStatsWithCoalesce(false)
	if err != nil {
		t.Fatalf("windowStatsWithCoalesce: %v", err)
	}
	want := float64(wantStats.EffectiveByteCountPerSecond())
	if want <= 0 {
		t.Fatalf("test fixture is not exercising the goodput path: want %v", want)
	}

	m := exitMetricsSnapshot(client, 0)
	AssertEqual(t, m.GoodputBytesPerSec, want)
}

// TestExitMetricsSnapshotStallEventsFromReconvictions proves the StallEvents
// wire: quarantineReconvictionCount() is this channel's completed
// bench-then-lift cycle count -- a hard send-stall conviction removes the
// channel outright (convictSendStalls), so it is never a placement candidate
// at all, and reconvictions are the chronic-misbehavior signal that DOES
// survive while an exit is still in the window.
func TestExitMetricsSnapshotStallEventsFromReconvictions(t *testing.T) {
	client := stallTestChannel()
	AssertEqual(t, exitMetricsSnapshot(client, 0).StallEvents, 0)

	client.setQuarantined(blackholeNoReceiveAck)
	client.clearQuarantine()
	AssertEqual(t, exitMetricsSnapshot(client, 0).StallEvents, 1)

	client.setQuarantined(blackholeNoReceiveSyn)
	client.clearQuarantine()
	AssertEqual(t, exitMetricsSnapshot(client, 0).StallEvents, 2)
}

// Placement telemetry must not prune the event window at new-flow frequency.
// The subsequent public stats read proves this fixture would detect accidental
// use of the coalescing accessor.
func TestExitMetricsSnapshotDoesNotCoalesceEventBuckets(t *testing.T) {
	client := stallTestChannel()
	client.settings.StatsWindowDuration = time.Second
	oldTime := time.Now().Add(-time.Hour)
	client.eventBuckets = []*multiClientEventBucket{
		{createTime: oldTime, eventTime: oldTime},
		{createTime: oldTime, eventTime: oldTime},
		{createTime: oldTime, eventTime: oldTime},
		{createTime: oldTime, eventTime: oldTime},
	}

	exitMetricsSnapshot(client, 0)
	if got := len(client.eventBuckets); got != 4 {
		t.Fatalf("placement snapshot retained %d event buckets, want 4", got)
	}

	if _, err := client.WindowStats(); err != nil {
		t.Fatalf("coalescing stats read: %v", err)
	}
	if got := len(client.eventBuckets); 4 <= got {
		t.Fatalf("fixture did not distinguish the coalescing accessor: retained %d buckets", got)
	}
}

// --- the RTT/jitter EWMA ---

// telemetryTestEwma returns a fresh channel's rttEwmaSnapshot fields for
// readability in the table test below.
func telemetryTestEwma(client *multiClientChannel) (rtt float64, jitter float64, rttOk bool, jitterOk bool) {
	return client.rttEwmaSnapshot()
}

// Builds the accounting fixture on the raw-frame path so a successful fake
// admission does not strand a legacy wrapper buffer.
func newPacketTransferTestChannel() *multiClientChannel {
	client := stallTestChannel()
	client.settings.ProtocolVersion = DefaultProtocolVersion
	return client
}

func almostEqual(a, b float64) bool {
	const epsilon = 1e-9
	return math.Abs(a-b) < epsilon
}

// TestAddSendRttSampleEwmaAndJitter hand-verifies the smoothing math: the
// classic 1/8 TCP SRTT weight for the mean, and the matching RTTVAR-style
// mean-absolute-deviation for jitter, seeded (not smoothed) on the very
// first deviation. Every operand here is a multiple of 0.125, so the
// expected values are exact in float64 -- no accumulated rounding to chase.
func TestAddSendRttSampleEwmaAndJitter(t *testing.T) {
	client := stallTestChannel()

	// before any sample: unmeasured
	if _, _, rttOk, jitterOk := telemetryTestEwma(client); rttOk || jitterOk {
		t.Fatal("a fresh channel must report no RTT/jitter measurement")
	}

	// sample 1: 100ms -- seeds the EWMA, no deviation to report yet
	client.addSendRttSample(100 * time.Millisecond)
	rtt, _, rttOk, jitterOk := telemetryTestEwma(client)
	if !rttOk || jitterOk {
		t.Fatalf("after one sample: want rttOk=true jitterOk=false, got rttOk=%v jitterOk=%v", rttOk, jitterOk)
	}
	if !almostEqual(rtt, 100) {
		t.Fatalf("first sample must seed the EWMA exactly: got %v want 100", rtt)
	}

	// sample 2: 140ms -- diff=40, ewma=100+0.125*40=105; jitter seeds at |40|=40
	client.addSendRttSample(140 * time.Millisecond)
	rtt, jitter, rttOk, jitterOk := telemetryTestEwma(client)
	if !rttOk || !jitterOk {
		t.Fatalf("after two samples: want both ok, got rttOk=%v jitterOk=%v", rttOk, jitterOk)
	}
	if !almostEqual(rtt, 105) || !almostEqual(jitter, 40) {
		t.Fatalf("got rtt=%v jitter=%v, want rtt=105 jitter=40", rtt, jitter)
	}

	// sample 3: 90ms -- diff=-15, ewma=105+0.125*(-15)=103.125;
	// jitter=40+0.125*(15-40)=36.875
	client.addSendRttSample(90 * time.Millisecond)
	rtt, jitter, rttOk, jitterOk = telemetryTestEwma(client)
	if !rttOk || !jitterOk {
		t.Fatal("after three samples both must stay ok")
	}
	if !almostEqual(rtt, 103.125) || !almostEqual(jitter, 36.875) {
		t.Fatalf("got rtt=%v jitter=%v, want rtt=103.125 jitter=36.875", rtt, jitter)
	}
}

// TestAddSendRttSampleDropsNonPositiveDurations: clock skew or a synthetic
// zero/negative duration must never be folded in -- exitScore reads a 0 RTT
// as the BEST possible sub-score, so recording a bogus one here would be the
// same trap exitMetricsSnapshot's NaN gating exists to avoid, one layer down.
func TestAddSendRttSampleDropsNonPositiveDurations(t *testing.T) {
	client := stallTestChannel()
	client.addSendRttSample(0)
	client.addSendRttSample(-5 * time.Millisecond)
	if _, _, rttOk, _ := telemetryTestEwma(client); rttOk {
		t.Fatal("a non-positive duration must not count as a measurement")
	}

	client.addSendRttSample(50 * time.Millisecond)
	client.addSendRttSample(-1 * time.Millisecond)
	rtt, _, rttOk, jitterOk := telemetryTestEwma(client)
	if !rttOk || jitterOk {
		t.Fatal("the dropped negative sample must not count as a second sample")
	}
	if !almostEqual(rtt, 50) {
		t.Fatalf("the one real sample must be unaffected by the dropped one: got %v want 50", rtt)
	}
}

// TestExitMetricsSnapshotOneRttSampleHasNoJitterYet: a single round trip has
// no deviation to report, so exitMetricsSnapshot must report a real RttMillis
// but NaN Jitter -- one sample is not enough to fabricate a "0 jitter"
// reading, which would trip the exact same zero-is-best trap as RTT itself.
func TestExitMetricsSnapshotOneRttSampleHasNoJitterYet(t *testing.T) {
	client := stallTestChannel()
	client.addSendRttSample(75 * time.Millisecond)

	m := exitMetricsSnapshot(client, 0)
	if math.IsNaN(m.RttMillis) {
		t.Fatal("one completed round trip must report a real RttMillis")
	}
	if !almostEqual(m.RttMillis, 75) {
		t.Fatalf("got RttMillis=%v want 75", m.RttMillis)
	}
	if !math.IsNaN(m.Jitter) {
		t.Fatalf("one sample has no deviation yet -- Jitter must read NaN, got %v", m.Jitter)
	}
}

// Two successful Ack-required sends drive the real callback boundary. Exact
// clock instants prove the packet-local RTTs feed the EWMA, while concurrent
// duplicate terminal attempts prove each callback can retire accounting once.
func TestSendDetailedWithAckRecordsRttSample(t *testing.T) {
	client := newPacketTransferTestChannel()
	baseTime := time.Unix(1700000000, 0)
	orderedTimes := []time.Time{
		baseTime,
		baseTime.Add(100 * time.Millisecond),
		baseTime.Add(time.Second),
		baseTime.Add(time.Second + 140*time.Millisecond),
	}
	var clockLock sync.Mutex
	clockCallCount := 0
	client.packetTransferNowForTest = func() time.Time {
		clockLock.Lock()
		defer clockLock.Unlock()
		index := clockCallCount
		clockCallCount += 1
		if len(orderedTimes) <= index {
			return orderedTimes[len(orderedTimes)-1]
		}
		return orderedTimes[index]
	}

	const concurrentCompletions = 16
	sendCallCount := 0
	client.sendTransferForTest = func(ackCallback AckFunction) (bool, error) {
		sendCallCount += 1
		start := make(chan struct{})
		var waitGroup sync.WaitGroup
		waitGroup.Add(concurrentCompletions)
		for range concurrentCompletions {
			go func() {
				defer waitGroup.Done()
				<-start
				ackCallback(nil)
			}()
		}
		close(start)
		waitGroup.Wait()
		return true, nil
	}

	send := func(packetByteCount int) {
		success, err := client.SendDetailedWithAck(&parsedPacket{
			packet: make([]byte, packetByteCount),
			ipPath: udpTestPath(4),
		}, time.Second, true)
		if err != nil || !success {
			t.Fatalf("Ack send = %t, %v; want true, nil", success, err)
		}
	}
	send(100)
	send(200)

	clockLock.Lock()
	gotClockCallCount := clockCallCount
	clockLock.Unlock()
	if gotClockCallCount != len(orderedTimes) {
		t.Errorf("packet clock calls = %d, want %d", gotClockCallCount, len(orderedTimes))
	}
	if sendCallCount != 2 {
		t.Errorf("transport admissions = %d, want 2", sendCallCount)
	}

	client.stateLock.Lock()
	sendNackCount := client.packetStats.sendNackCount
	sendNackByteCount := client.packetStats.sendNackByteCount
	sendAckCount := client.packetStats.sendAckCount
	sendAckByteCount := client.packetStats.sendAckByteCount
	rttSamples := client.rttSamples
	client.stateLock.Unlock()
	if sendNackCount != 0 || sendNackByteCount != 0 {
		t.Errorf("outstanding sends = %d/%dB, want 0/0B", sendNackCount, sendNackByteCount)
	}
	if sendAckCount != 2 || sendAckByteCount != 300 {
		t.Errorf("acknowledged sends = %d/%dB, want 2/300B", sendAckCount, sendAckByteCount)
	}
	if rttSamples != 2 {
		t.Fatalf("RTT samples = %d, want one per Ack callback", rttSamples)
	}
	rtt, jitter, rttOk, jitterOk := telemetryTestEwma(client)
	if !rttOk || !jitterOk || !almostEqual(rtt, 105) || !almostEqual(jitter, 40) {
		t.Fatalf("Ack EWMA = rtt:%v jitter:%v ok:%t/%t, want 105/40 true/true", rtt, jitter, rttOk, jitterOk)
	}
}

// A NoAck success is an initial route-write disposition. It retires the
// outstanding send but cannot fabricate a peer round-trip measurement.
func TestSendDetailedWithoutAckDoesNotRecordRttSample(t *testing.T) {
	client := newPacketTransferTestChannel()
	clockCallCount := 0
	client.packetTransferNowForTest = func() time.Time {
		clockCallCount += 1
		return time.Unix(1700000000, 0)
	}
	client.sendTransferForTest = func(ackCallback AckFunction) (bool, error) {
		ackCallback(nil)
		return true, nil
	}

	const packetByteCount = 256
	success, err := client.SendDetailedWithAck(&parsedPacket{
		packet: make([]byte, packetByteCount),
		ipPath: udpTestPath(4),
	}, time.Second, false)
	if err != nil || !success {
		t.Fatalf("NoAck send = %t, %v; want true, nil", success, err)
	}
	if clockCallCount != 0 {
		t.Errorf("NoAck completion read the RTT clock %d times, want 0", clockCallCount)
	}
	if _, _, rttOk, jitterOk := telemetryTestEwma(client); rttOk || jitterOk {
		t.Fatal("NoAck route-write success fabricated an RTT measurement")
	}

	client.stateLock.Lock()
	sendNackCount := client.packetStats.sendNackCount
	sendNackByteCount := client.packetStats.sendNackByteCount
	sendAckCount := client.packetStats.sendAckCount
	sendAckByteCount := client.packetStats.sendAckByteCount
	client.stateLock.Unlock()
	if sendNackCount != 0 || sendNackByteCount != 0 || sendAckCount != 1 || sendAckByteCount != packetByteCount {
		t.Errorf(
			"NoAck completion accounting = outstanding:%d/%dB ack:%d/%dB, want 0/0B and 1/%dB",
			sendNackCount,
			sendNackByteCount,
			sendAckCount,
			sendAckByteCount,
			packetByteCount,
		)
	}
}

// --- the zero-RTT trap, end to end ---

// TestExitScoreUnmeasuredDoesNotBeatMeasured is the brief's central
// assertion, at the exitScore level: an exit with a real (even mediocre)
// RTT measurement must outscore one with none, for an RTT-weighted class.
// Before this task, exitMetricsSnapshot left RttMillis at its zero value for
// an unmeasured exit, and exitScore sanitizes a bare 0 into the BEST
// possible sub-score -- so the unmeasured exit would have WON here. The NaN
// convention this task uses instead sanitizes to the WORST sub-score,
// matching exitScore's documented hostile-input handling exactly.
func TestExitScoreUnmeasuredDoesNotBeatMeasured(t *testing.T) {
	unmeasured := stallTestChannel()
	measured := stallTestChannel()
	measured.addSendRttSample(80 * time.Millisecond) // real, but not great

	weights := classWeights(ClassLatency) // heavily RTT-weighted: Rtt=1.0

	scoreUnmeasured := exitScore(exitMetricsSnapshot(unmeasured, 0), weights)
	scoreMeasured := exitScore(exitMetricsSnapshot(measured, 0), weights)

	if scoreUnmeasured >= scoreMeasured {
		t.Fatalf(
			"unmeasured exit (score=%v) must not beat a real 80ms measurement (score=%v) -- the zero-RTT trap",
			scoreUnmeasured, scoreMeasured,
		)
	}

	// and it must read exactly as exitScore's own documented hostile-input
	// case does: the same worst-case contribution a NaN/Inf/negative RTT
	// gets, not some intermediate "neutral" value
	hostile := exitScore(ExitMetrics{RttMillis: math.NaN(), Jitter: math.NaN()}, weights)
	AssertEqual(t, scoreUnmeasured, hostile)
}

// TestScoredPlacementReorderDoesNotPromoteUnmeasuredExitOnRtt is the same
// proof through the real call path: two otherwise-identical candidates (both
// start at 0 flows, so the less-loaded tie-break cannot explain the
// outcome), one with a real RTT sample and one with none. A latency-class
// flow must prefer the MEASURED exit. Before this task's NaN convention, the
// unmeasured exit's fabricated 0ms would have looked better than a real
// measurement and won the reorder instead -- the exact regression this test
// exists to catch.
func TestScoredPlacementReorderDoesNotPromoteUnmeasuredExitOnRtt(t *testing.T) {
	parent, clients := flowCapTestParent(t, 0, 0, 0)
	parent.SetFlowClassifier(fixedClassifier{class: ClassLatency})
	clients[1].addSendRttSample(30 * time.Millisecond)
	ipPath := &IpPath{Version: 4, Protocol: IpProtocolTcp}

	got := parent.scoredPlacementReorder(clients, ipPath, "")
	if len(got) != 2 || got[0] != clients[1] {
		t.Fatalf("the measured exit (clients[1]) must be promoted to the front, not the unmeasured one")
	}
}
