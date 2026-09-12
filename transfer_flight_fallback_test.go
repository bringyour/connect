package connect

import (
	"context"
	"testing"
	"time"
)

// A full unreliable flight must not stall a sequence that still has a
// reliable carrier: the overflow is written reliable-only instead.

func TestRouteSnapshotReliableOnlyWritesExcludeUnreliableCarrier(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selector := NewMultiRouteSelector(ctx, "reliable-only", nil, TransferPath{}, true)
	defer selector.Close()

	h1Transport := NewSendGatewayTransportWithType(TransportTypeH1)
	h1Route := make(Route, 16)
	selector.updateTransportWithProperties(h1Transport, []Route{h1Route}, TransferCarrierProperties{})

	if policy := selector.transferFlightPolicy(); !policy.reliableRouteAvailable {
		t.Fatalf("H1-only policy = %+v, want reliable route available", policy)
	}

	p2pTransport := NewSendGatewayTransportWithType(TransportTypeP2p)
	p2pRoute := make(Route, 16)
	selector.updateTransportWithProperties(
		p2pTransport,
		[]Route{p2pRoute},
		TransferCarrierProperties{Unreliable: true},
	)

	policy := selector.transferFlightPolicy()
	if !policy.limited || !policy.reliableRouteAvailable {
		t.Fatalf("mixed policy = %+v, want limited with a reliable route", policy)
	}

	// every reliable-only write lands on the reliable carrier even though the
	// unreliable route has room and is the weighted first choice
	for i := 0; i < 8; i++ {
		success, disposition, err := selector.writeDetailedReliableOnly(ctx, []byte{byte(i)}, time.Second)
		if err != nil || !success {
			t.Fatalf("reliable-only write %d: success=%t err=%v", i, success, err)
		}
		if disposition.transportType != TransportTypeH1 || disposition.unreliable {
			t.Fatalf("reliable-only write %d disposition = %+v, want H1 reliable", i, disposition)
		}
	}
	if len(h1Route) != 8 || len(p2pRoute) != 0 {
		t.Fatalf("routes after reliable-only writes: h1=%d p2p=%d, want 8/0", len(h1Route), len(p2pRoute))
	}

	// without any reliable carrier the reliable-only write falls back to the
	// ordinary route set instead of failing
	selector.updateTransport(h1Transport, nil)
	if policy := selector.transferFlightPolicy(); policy.reliableRouteAvailable {
		t.Fatalf("P2P-only policy = %+v, want no reliable route", policy)
	}
	success, disposition, err := selector.writeDetailedReliableOnly(ctx, []byte{9}, time.Second)
	if err != nil || !success || disposition.transportType != TransportTypeP2p {
		t.Fatalf("fallback write: success=%t disposition=%+v err=%v", success, disposition, err)
	}
	if len(p2pRoute) != 1 {
		t.Fatalf("p2p route after fallback = %d, want 1", len(p2pRoute))
	}
}

func TestSendSequenceFullUnreliableFlightWritesReliableOnlyInsteadOfStalling(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.UnreliableInitialFlightByteCount = 512
	settings.UnreliableMinimumFlightByteCount = 512
	settings.UnreliableMaximumFlightByteCount = 512
	sequence := testUnreliableRecoverySequence(settings)
	sequence.client = &Client{}
	sequence.flightController = newSendFlightController(settings)

	withReliable := transferFlightPolicySnapshot{
		generation:             1,
		limited:                true,
		reliableRouteAvailable: true,
	}
	sequence.flightController.applyPolicy(withReliable)

	// an open flight never gates admission and never forces reliable-only writes
	if sequence.unreliableFlightGates(withReliable) {
		t.Fatal("open flight with a reliable route gated admission")
	}
	if sequence.reliableOnlyWrite(withReliable) {
		t.Fatal("open flight forced reliable-only writes")
	}

	item := &sendItem{transferFrameBytes: make([]byte, 512)}
	sequence.observeCarrierWrite(item, transferWriteDisposition{unreliable: true})
	if sequence.flightController.canSend() {
		t.Fatal("flight was not full after tracking the limit")
	}

	// a full flight with a reliable carrier: keep admitting packs, write them reliable-only
	if sequence.unreliableFlightGates(withReliable) {
		t.Fatal("full flight gated admission although a reliable route is available")
	}
	if !sequence.reliableOnlyWrite(withReliable) {
		t.Fatal("full flight did not force reliable-only writes")
	}

	// a full flight with only unreliable carriers keeps the original gate
	unreliableOnly := transferFlightPolicySnapshot{generation: 2, limited: true}
	sequence.flightController.applyPolicy(unreliableOnly)
	sequence.observeCarrierWrite(&sendItem{transferFrameBytes: make([]byte, 512)}, transferWriteDisposition{unreliable: true})
	if !sequence.unreliableFlightGates(unreliableOnly) {
		t.Fatal("full flight without a reliable route did not gate admission")
	}
	if sequence.reliableOnlyWrite(unreliableOnly) {
		t.Fatal("reliable-only write requested without a reliable route")
	}
}

func TestSendSequenceUnreliableResendTimeoutReleasesFlightWhenReliableRouteAvailable(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.UnreliableInitialFlightByteCount = 1024
	settings.UnreliableMinimumFlightByteCount = 1024
	settings.UnreliableMaximumFlightByteCount = 1024
	sequence := testUnreliableRecoverySequence(settings)
	sequence.client = &Client{}
	sequence.flightController = newSendFlightController(settings)

	unreliableOnly := transferFlightPolicySnapshot{generation: 1, limited: true}
	sequence.flightController.applyPolicy(unreliableOnly)
	item := &sendItem{transferFrameBytes: make([]byte, 512)}
	sequence.observeCarrierWrite(item, transferWriteDisposition{unreliable: true})
	if sequence.flightController.byteCount != 512 {
		t.Fatalf("tracked flight = %d, want 512", sequence.flightController.byteCount)
	}

	// no reliable carrier: the timeout halves admission but the item stays in flight
	if sequence.observeUnreliableResendTimeout(item, unreliableOnly) {
		t.Fatal("resend was marked reliable-only without a reliable route")
	}
	if sequence.flightController.byteCount != 512 || !item.unreliableFlightTracked {
		t.Fatalf("flight after unreliable-only timeout = %d tracked=%t, want 512/true", sequence.flightController.byteCount, item.unreliableFlightTracked)
	}
	stats := sequence.client.SendRecoveryStats()
	if stats.UnreliableFlightTimeoutCount != 1 {
		t.Fatalf("timeout count = %d, want 1", stats.UnreliableFlightTimeoutCount)
	}

	// a reliable carrier takes the resend: release the flight so new packs are not stalled
	withReliable := transferFlightPolicySnapshot{generation: 2, limited: true, reliableRouteAvailable: true}
	sequence.flightController.applyPolicy(withReliable)
	if !sequence.observeUnreliableResendTimeout(item, withReliable) {
		t.Fatal("resend was not marked reliable-only with a reliable route")
	}
	if sequence.flightController.byteCount != 0 || item.unreliableFlightTracked {
		t.Fatalf("flight after reliable-fallback timeout = %d tracked=%t, want 0/false", sequence.flightController.byteCount, item.unreliableFlightTracked)
	}
	if !sequence.flightController.canSend() {
		t.Fatal("flight stayed closed after the timed-out item was released")
	}
}

func TestSendSequenceFloorSingleFlightKeepsOneMessageOnLossyCarrier(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.UnreliableInitialFlightByteCount = 2048
	settings.UnreliableMinimumFlightByteCount = 1024
	settings.UnreliableMaximumFlightByteCount = 4096
	settings.UnreliableInitialFlightMessageCount = 8
	settings.UnreliableMinimumFlightMessageCount = 4
	settings.UnreliableMaximumFlightMessageCount = 16
	settings.UnreliableFloorSingleFlight = true
	sequence := testUnreliableRecoverySequence(settings)
	sequence.client = &Client{}
	sequence.flightController = newSendFlightController(settings)
	policy := transferFlightPolicySnapshot{generation: 1, limited: true, reliableRouteAvailable: true}
	sequence.flightController.applyPolicy(policy)

	// healthy carrier (limit above the floor): the flight decides alone
	first := &sendItem{transferFrameBytes: make([]byte, 512)}
	sequence.observeCarrierWrite(first, transferWriteDisposition{unreliable: true})
	if sequence.flightController.atFloor() || sequence.reliableOnlyWrite(policy) {
		t.Fatalf("healthy carrier forced reliable-only: floor=%t", sequence.flightController.atFloor())
	}

	// repeated loss pins the limit to the floor: one message may stay in
	// flight, everything else goes reliable-only
	for i := 0; i < 8; i++ {
		sequence.flightController.reduceForLoss()
	}
	if !sequence.flightController.atFloor() {
		t.Fatalf("flight not at floor after reductions: %d/%d", sequence.flightController.byteLimit, sequence.flightController.activeMinimumByteCount)
	}
	if !sequence.reliableOnlyWrite(policy) {
		t.Fatal("floor carrier with a message in flight did not force reliable-only")
	}
	sequence.releaseUnreliableFlight(first)
	if sequence.reliableOnlyWrite(policy) {
		t.Fatal("floor carrier with an empty flight refused its single probe message")
	}

	// the rule is opt-in: without it only a full flight forces reliable-only
	settings.UnreliableFloorSingleFlight = false
	sequence.observeCarrierWrite(first, transferWriteDisposition{unreliable: true})
	if sequence.reliableOnlyWrite(policy) {
		t.Fatal("floor rule applied while disabled")
	}
	sequence.releaseUnreliableFlight(first)
}

// A reply keeps its carrier affinity on reliable carriers but never rides a
// potentially unreliable one while a reliable route is active.
func TestMultiRouteSelectorReplyAvoidsUnreliableCarrier(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selector := NewMultiRouteSelector(ctx, "reply-affinity", nil, TransferPath{}, true)
	defer selector.Close()
	h1Transport := NewSendGatewayTransportWithType(TransportTypeH1)
	h1Route := make(Route, 16)
	selector.updateTransportWithProperties(h1Transport, []Route{h1Route}, TransferCarrierProperties{})
	p2pTransport := NewSendGatewayTransportWithType(TransportTypeP2p)
	p2pRoute := make(Route, 16)
	selector.updateTransportWithProperties(p2pTransport, []Route{p2pRoute}, TransferCarrierProperties{Unreliable: true})

	if !selector.transportPotentiallyUnreliable(TransportTypeP2p) || selector.transportPotentiallyUnreliable(TransportTypeH1) {
		t.Fatal("carrier reliability classification is wrong")
	}
	for i := 0; i < 6; i++ {
		success, disposition, err := selector.writeDetailedReplyWithCarrierPreference(ctx, []byte{byte(i)}, time.Second, TransportTypeP2p)
		if err != nil || !success || disposition.transportType != TransportTypeH1 {
			t.Fatalf("reply %d with p2p affinity: success=%t disposition=%+v err=%v; want H1", i, success, disposition, err)
		}
	}
	if len(h1Route) != 6 || len(p2pRoute) != 0 {
		t.Fatalf("routes after replies: h1=%d p2p=%d, want 6/0", len(h1Route), len(p2pRoute))
	}

	// with only the unreliable carrier the reply keeps using it
	selector.updateTransport(h1Transport, nil)
	if selector.transportPotentiallyUnreliable(TransportTypeP2p) {
		t.Fatal("p2p flagged unreliable-replaceable without a reliable route")
	}
	success, disposition, err := selector.writeDetailedReplyWithCarrierPreference(ctx, []byte{9}, time.Second, TransportTypeP2p)
	if err != nil || !success || disposition.transportType != TransportTypeP2p {
		t.Fatalf("p2p-only reply: success=%t disposition=%+v err=%v", success, disposition, err)
	}
}

// Acks for unreliable-carried items must not feed the sequence RTT window.
func TestSendSequenceAckRttIgnoresUnreliableCarrier(t *testing.T) {
	settings := DefaultSendBufferSettings()
	sequence := testUnreliableRecoverySequence(settings)
	sequence.client = &Client{}
	sequence.flightController = newSendFlightController(settings)
	sequence.flightController.applyPolicy(transferFlightPolicySnapshot{generation: 1, limited: true, reliableRouteAvailable: true})
	before := sequence.rttWindow.ScaledRtt()

	unreliableItem := &sendItem{transferFrameBytes: make([]byte, 64)}
	sequence.observeCarrierWrite(unreliableItem, transferWriteDisposition{unreliable: true})
	tag := sequenceTag{sendTime: uint64(time.Now().Add(-5 * time.Second).UnixMilli()), set: true}
	sequence.observeAckRtt(unreliableItem, tag)
	if after := sequence.rttWindow.ScaledRtt(); after != before {
		t.Fatalf("unreliable-carrier ack moved the RTT window: %s -> %s", before, after)
	}

	reliableItem := &sendItem{transferFrameBytes: make([]byte, 64)}
	sequence.observeCarrierWrite(reliableItem, transferWriteDisposition{reliable: true})
	sequence.observeAckRtt(reliableItem, tag)
	if after := sequence.rttWindow.ScaledRtt(); after <= before {
		t.Fatalf("reliable-carrier ack did not move the RTT window: %s -> %s", before, after)
	}
}

// With a datagram lane active, a reliable-carried item that is not yet older
// than the reliable lane's RTT is late, not lost: no gap resend for it.
func TestSelectiveAckGapSkipsReliableItemsNotYetLateInMixedLanes(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(100 * time.Millisecond)
	sequence, items := newSelectiveAckRecoveryTestSequence(8, sendTime)
	sequence.client = &Client{}
	sequence.flightController = newSendFlightController(sequence.sendBufferSettings)
	sequence.flightController.applyPolicy(transferFlightPolicySnapshot{generation: 1, limited: true, reliableRouteAvailable: true})
	// items 0 and 4 are gaps; 0 rode the reliable lane 100 ms ago, 4 rode it long ago
	items[0].reliableCarrierObserved = true
	items[4].reliableCarrierObserved = true
	items[4].sendTime = sendTime.Add(-5 * time.Second)
	for _, index := range []int{1, 2, 3, 5, 6, 7} {
		items[index].selectiveAcked = true
	}
	sequence.scheduleSelectiveAckRecovery(currentTime)
	if items[0].selectiveGapRecovered {
		t.Fatal("fresh reliable-carried gap item was gap-resent behind fast-lane acks")
	}
	if !items[4].selectiveGapRecovered || items[4].recoveryKind != sendRecoverySelectiveGap {
		t.Fatalf("stale reliable-carried gap item was not gap-resent: recovered=%t kind=%d", items[4].selectiveGapRecovered, items[4].recoveryKind)
	}

	// single reliable lane (no unreliable carrier): behaviour unchanged
	sequence2, items2 := newSelectiveAckRecoveryTestSequence(8, sendTime)
	sequence2.client = &Client{}
	sequence2.flightController = newSendFlightController(sequence2.sendBufferSettings)
	items2[0].reliableCarrierObserved = true
	for _, index := range []int{1, 2, 3, 5, 6, 7} {
		items2[index].selectiveAcked = true
	}
	sequence2.scheduleSelectiveAckRecovery(currentTime)
	if !items2[0].selectiveGapRecovered {
		t.Fatal("single-lane gap recovery regressed")
	}
}

// Main's TestSendSequenceDefersTimeoutResendWhileAcksProgress, rewritten
// against this branch's implementation of the same rule. Main landed
// deferTimeoutResend, keyed on the head-ack clock with a hardcoded limit of
// two; this branch evolved the same design into shouldDeferTimeoutResend,
// keyed on the cumulative-ack clock with the limit and the
// since-last-deferral term as settings, and the backoff of §24. The
// behaviour main asserted is preserved here: a timed-out reliable-carried
// item waits while cumulative acknowledgements are still advancing, is
// re-sent at once when they have stalled, and is never deferred when it
// rode the unreliable lane.
func TestSendSequenceDefersTimeoutResendWhileAcksProgress(t *testing.T) {
	settings := DefaultSendBufferSettings()
	sequence := testUnreliableRecoverySequence(settings)
	sequence.client = &Client{}
	sequence.flightController = newSendFlightController(settings)
	now := time.Now()
	scaledRtt := sequence.rttWindow.ScaledRtt()

	item := &sendItem{transferFrameBytes: make([]byte, 64), sendTime: now}
	sequence.observeCarrierWrite(item, transferWriteDisposition{reliable: true})
	sequence.lastCumulativeAckTime = now.Add(-50 * time.Millisecond)
	if !sequence.shouldDeferTimeoutResend(item, scaledRtt) {
		t.Fatal("first deferral was refused while the cumulative ack was still advancing")
	}
	// each further deferral requires the ack to have advanced since the last
	item.timeoutDeferCount = 1
	item.timeoutDeferAckTime = sequence.lastCumulativeAckTime
	if sequence.shouldDeferTimeoutResend(item, scaledRtt) {
		t.Fatal("a second deferral was granted with no cumulative progress since the first")
	}
	sequence.lastCumulativeAckTime = now.Add(-10 * time.Millisecond)
	if !sequence.shouldDeferTimeoutResend(item, scaledRtt) {
		t.Fatal("a second deferral was refused though the cumulative ack had advanced")
	}
	item.timeoutDeferCount = settings.TimeoutResendDeferLimit
	if sequence.shouldDeferTimeoutResend(item, scaledRtt) {
		t.Fatalf("a deferral was granted past the limit of %d", settings.TimeoutResendDeferLimit)
	}

	stalled := &sendItem{transferFrameBytes: make([]byte, 64), sendTime: now}
	sequence.observeCarrierWrite(stalled, transferWriteDisposition{reliable: true})
	sequence.lastCumulativeAckTime = now.Add(-30 * time.Second)
	if sequence.shouldDeferTimeoutResend(stalled, scaledRtt) {
		t.Fatal("deferred a timeout while acks were stalled")
	}

	unreliable := &sendItem{transferFrameBytes: make([]byte, 64), sendTime: now}
	sequence.observeCarrierWrite(unreliable, transferWriteDisposition{unreliable: true})
	sequence.lastCumulativeAckTime = now
	if sequence.shouldDeferTimeoutResend(unreliable, scaledRtt) {
		t.Fatal("deferred a timeout of an unreliable-carried item")
	}
	sequence.releaseUnreliableFlight(unreliable)
}
