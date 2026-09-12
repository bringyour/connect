package connect

// The root-cause contract for every metric this program introduced, in the
// same shape as the recovery contract: each row names a metric, states the
// regime and the behaviour it measures, is built from API that predates
// this program, and runs unchanged against merged 89e1633. Where merged
// trades against a metric the row makes it fail by construction. Where
// merged does not trade against it the row records that plainly, because
// an honest matrix with equal columns is the point rather than a
// scoreboard. Where a metric cannot be pinned in process the row says which
// and why instead of inventing a shape.

import (
	"context"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// metricArms reuses the recovery contract's arms, so the two files report
// the same columns.
func metricArms() []laneRecoveryArm { return laneRecoveryArms() }

// M1 and M2. Total recovery writes, and whole-window timeout writes on a
// lossless link. A relay that stalls mid-transfer drops nothing; every
// retransmit written into it is a duplicate that buys no delivery, because
// the receive stream is ordered and nothing past the stalled head can be
// delivered however much is rewritten. merged rewrites its window each
// interval.
func TestMetricTotalRecoveryWritesOnALosslessStall(t *testing.T) {
	if testing.Short() {
		t.Skip("metric contract, live link")
	}
	const messageCount = 4000
	for _, arm := range metricArms() {
		link := newLaneRecoveryLink(
			t, 100*time.Millisecond, 3*time.Millisecond,
			1500*time.Millisecond, 2750*time.Millisecond, 2048, 0, 0, 0, arm.configure)
		stats := laneRecoverySend(t, link, messageCount)
		total := stats.TimeoutResendWriteCount + stats.SelectiveGapWriteCount +
			stats.AckTailProbeWriteCount + stats.CumulativeProbeWriteCount
		t.Logf(
			"%s: M1 total recovery writes %d (whole-window %d, gap %d, tail probe %d, cumulative probe %d) "+
				"over %d messages on a link that drops nothing",
			arm.name, total, stats.TimeoutResendWriteCount, stats.SelectiveGapWriteCount,
			stats.AckTailProbeWriteCount, stats.CumulativeProbeWriteCount, messageCount,
		)
		// M2: a lossless link must not need whole-window rewrites at all
		const bound = 20
		if arm.readsLanes && bound < int(stats.TimeoutResendWriteCount) {
			t.Errorf(
				"%s: M2: %d whole-window retransmits on a link that dropped nothing, want at "+
					"most %d; each one is a duplicate the ordered stream cannot use",
				arm.name, stats.TimeoutResendWriteCount, bound,
			)
		}
	}
}

// M3. Deferred-item gap writes by hole carrier. This one merged cannot
// trade against: it has no deferral, so the metric is zero on it by
// construction, and the row records that rather than manufacturing a
// failure. What it separates is our own arms, and it is how the six mixed
// cells' gap resends were attributed.
func TestMetricDeferredItemGapWritesByHoleCarrier(t *testing.T) {
	for _, arm := range metricArms() {
		relay, direct, byCarrier, supported := metricDeferredGapSplit(t, arm)
		if !supported {
			t.Logf("%s: M3: this tree keeps no deferral, so deferred-item gap writes are zero "+
				"by construction and the metric does not separate it", arm.name)
			continue
		}
		t.Logf("%s: M3: deferred-item gap writes relay %d, direct %d (%v)",
			arm.name, relay, direct, byCarrier)
	}
}

// metricDeferredGapSplit reports the split where the tree keeps one.
func metricDeferredGapSplit(
	t testing.TB,
	arm laneRecoveryArm,
) (uint64, uint64, bool, bool) {
	t.Helper()
	sequence, items, sendTime := laneRecoveryScoreboard(t, 8, arm.configure)
	relay := make(Route, 4)
	hole := items[0]
	hole.reliableCarrierObserved = true
	hole.carrierRoute = relay
	for index := 1; index < len(items); index += 1 {
		items[index].selectiveAcked = true
		items[index].carrierRoute = relay
	}
	laneRecoveryRecordSends(sequence, items[:1])
	laneRecoveryRecordAcks(sequence, items[1:])
	sequence.scheduleSelectiveAckRecovery(sendTime.Add(10 * time.Second))
	return metricDeferredGapForTree(sequence.client.SendRecoveryStats())
}

// M4. Route-generation changes. Not pinnable in process: a route
// generation changes when the route manager publishes a new snapshot,
// which the instruments never do, and §30 places those changes with the
// transport rather than with the recovery path. The row asserts the
// counter reads zero rather than pretending to measure it.
func TestMetricRouteGenerationChangesAreNotPinnableInProcess(t *testing.T) {
	if testing.Short() {
		t.Skip("metric contract, live link")
	}
	for _, arm := range metricArms() {
		link := newLaneRecoveryLink(
			t, 20*time.Millisecond, 2*time.Millisecond, 0, 0, 64, 0, 0, 0, arm.configure)
		stats := laneRecoverySend(t, link, 300)
		changes, supported := metricRouteGenerationsForTree(stats)
		if !supported {
			t.Logf("%s: M4: this tree does not count route generations", arm.name)
			continue
		}
		t.Logf("%s: M4: %d route generation changes over a run whose routes never change",
			arm.name, changes)
		if changes != 0 {
			t.Errorf("%s: M4: the instrument reported %d route generation changes, but it never "+
				"republishes a route snapshot, so the counter is measuring something else",
				arm.name, changes)
		}
	}
}

// M5 and M6. The relay's longest unacknowledged gap, and the round trip
// the timer read at its first firing. Both describe the lane rather than
// the tree, so the row's claim is that they read the same on every arm of
// the same shape: a metric that moved with the arm would be measuring the
// recovery path instead of the relay, which is what §30 says it must not.
func TestMetricLaneGapAndOnsetIntervalDescribeTheLaneNotTheTree(t *testing.T) {
	if testing.Short() {
		t.Skip("metric contract, live link")
	}
	gaps := map[string]time.Duration{}
	exported := 0
	for _, arm := range metricArms() {
		link := newLaneRecoveryLink(
			t, 100*time.Millisecond, 3*time.Millisecond,
			1500*time.Millisecond, 2750*time.Millisecond, 2048, 0, 0, 0, arm.configure)
		stats := laneRecoverySend(t, link, 4000)
		gap, supported := laneRecoveryLongestGapForTree(stats)
		if !supported {
			t.Logf("%s: M5 and M6: this tree exports neither, so the row cannot be measured on "+
				"it; the exports were added by this program", arm.name)
			continue
		}
		exported += 1
		t.Logf("%s: M5 longest unacknowledged gap %s; M6%s", arm.name,
			gap.Truncate(time.Millisecond), laneRecoveryDetailForTree(stats))
		if gap < 2*time.Second {
			// the transfer outran the stall's onset, so this arm's run says
			// nothing about the stall; recorded rather than counted
			t.Logf("%s: M5: the run did not span the stall, so it is left out of the comparison",
				arm.name)
			continue
		}
		gaps[arm.name] = gap
	}
	if exported == 0 {
		return
	}
	if len(gaps) == 0 {
		t.Fatal("M5: no arm's run spanned the stall, so the row measured nothing")
	}
	// every arm that saw the stall must read the same one, within the
	// instrument's own jitter
	var smallest, largest time.Duration
	for _, gap := range gaps {
		if smallest == 0 || gap < smallest {
			smallest = gap
		}
		if largest < gap {
			largest = gap
		}
	}
	if 500*time.Millisecond < largest-smallest {
		t.Errorf("M5: the longest gap ranged %s to %s across arms of the same shape, so it is "+
			"measuring the recovery path rather than the relay", smallest, largest)
	}
}

// M7. The original gate metric: an unreliable flight that blocks while a
// reliable carrier has capacity. The failing column here is neither merged
// nor this tree but the pre-merge tree, whose full flight stalled the
// sequence instead of overflowing; merged's own fix is what this counter
// records the absence of. The row states that and asserts the absence,
// which is the part that can be checked here.
func TestMetricFlightBlockedWithReliableCapacity(t *testing.T) {
	if testing.Short() {
		t.Skip("metric contract, live link")
	}
	for _, arm := range metricArms() {
		// a small direct flight beside a working relay: the flight fills at
		// once and every further Pack must take the relay
		link := newLaneRecoveryLink(
			t, 50*time.Millisecond, 2*time.Millisecond, 0, 0, 256, 0, 0, 2, arm.configure)
		stats := laneRecoverySend(t, link, 800)
		t.Logf("%s: M7: flight blocked with reliable capacity %d over 800 messages with a "+
			"two-message direct flight beside a live relay",
			arm.name, stats.UnreliableFlightBlockedWithReliableCapacity)
		if stats.UnreliableFlightBlockedWithReliableCapacity != 0 {
			t.Errorf(
				"%s: M7: the sequence stalled on a full unreliable flight %d times while the "+
					"relay had capacity; the pre-merge tree is the failing column for this "+
					"metric and neither merged nor this tree may join it",
				arm.name, stats.UnreliableFlightBlockedWithReliableCapacity,
			)
		}
	}
}

// M8. The reply-lane carrier-written counter. merged records the carrier
// of the Pack being answered rather than the carrier the acknowledgement
// left on, so an acknowledgement that takes the H1 priority companion is
// filed under the Pack's lane and the reply lane is judged by a blind
// counter. This row drives exactly that write and reads the attribution.
func TestMetricAckWriteIsAttributedToTheCarrierItLeftOn(t *testing.T) {
	for _, arm := range metricArms() {
		counts, ok := metricAckWriteAttribution(t, arm)
		if !ok {
			t.Logf("%s: M8: no acknowledgement was written, so the row measured nothing", arm.name)
			continue
		}
		t.Logf("%s: M8: acknowledgement writes by transport %v", arm.name, counts)
		if 0 < counts[TransportTypeP2p] && counts[TransportTypeH1] == 0 {
			t.Errorf(
				"%s: M8: every acknowledgement write is filed under the direct lane the Pack "+
					"arrived on, though the reply took the H1 priority companion; the reply lane "+
					"is then judged by a counter that cannot see it",
				arm.name,
			)
		}
	}
}

// metricAckWriteAttribution delivers one Pack on a direct lane to a
// receiver whose reply can take an H1 companion, and reports which
// transport the acknowledgement write was recorded under.
func metricAckWriteAttribution(
	t testing.TB,
	arm laneRecoveryArm,
) (map[TransportType]uint64, bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	if arm.configure != nil {
		arm.configure(settings.SendBufferSettings)
	}
	receiverId := NewId()
	senderId := NewId()
	receiver := NewClient(ctx, receiverId, NewNoContractClientOob(), settings)
	receiver.ContractManager().AddNoContractPeer(senderId)
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		receiver.CloseAndWait(closeCtx)
	})
	directIn := make(Route, 8)
	h1Out := make(Route, 64)
	receiver.RouteManager().UpdateTransportWithProperties(
		NewReceiveGatewayTransportWithType(TransportTypeP2p),
		[]Route{directIn},
		TransferCarrierProperties{Unreliable: true},
	)
	// the only way out is the H1 companion, so the acknowledgement must
	// leave on it whatever lane the Pack arrived on
	receiver.RouteManager().UpdateTransport(
		NewSendGatewayTransportWithType(TransportTypeH1), []Route{h1Out})
	receiver.AddReceiveCallback(func(TransferPath, []*protocol.Frame, Peer) {})

	frame, err := ToFrame(&protocol.SimpleMessage{Content: "attribution"}, DefaultProtocolVersion)
	if err != nil {
		t.Fatal(err)
	}
	packBytes, err := ProtoMarshal(&protocol.TransferFrame{
		TransferPath: TransferPath{SourceId: senderId, DestinationId: receiverId}.ToProtobuf(),
		Pack: &protocol.Pack{
			MessageId:      NewId().Bytes(),
			SequenceId:     NewId().Bytes(),
			SequenceNumber: 0,
			Head:           true,
			Frames:         []*protocol.Frame{frame},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case directIn <- packBytes:
	case <-time.After(5 * time.Second):
		MessagePoolReturn(packBytes)
		t.Fatal("the receiver did not accept the Pack")
	}
	deadline := time.After(5 * time.Second)
	for {
		select {
		case written := <-h1Out:
			if written != nil {
				MessagePoolReturn(written)
			}
			// give the recorder a moment past the write
			time.Sleep(100 * time.Millisecond)
			counts := receiver.ReceiveStats().AckRouteWriteCountByTransport
			return counts, 0 < len(counts)
		case <-deadline:
			return nil, false
		}
	}
}
