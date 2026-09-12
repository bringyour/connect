package connect

import (
	"context"
	"sync/atomic"
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
	if sequence.reliableOnlyWrite(withReliable, 0) {
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
	if !sequence.reliableOnlyWrite(withReliable, 0) {
		t.Fatal("full flight did not force reliable-only writes")
	}

	// a full flight with only unreliable carriers keeps the original gate
	unreliableOnly := transferFlightPolicySnapshot{generation: 2, limited: true}
	sequence.flightController.applyPolicy(unreliableOnly)
	sequence.observeCarrierWrite(&sendItem{transferFrameBytes: make([]byte, 512)}, transferWriteDisposition{unreliable: true})
	if !sequence.unreliableFlightGates(unreliableOnly) {
		t.Fatal("full flight without a reliable route did not gate admission")
	}
	if sequence.reliableOnlyWrite(unreliableOnly, 0) {
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
	if sequence.flightController.atFloor() || sequence.reliableOnlyWrite(policy, 0) {
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
	if !sequence.reliableOnlyWrite(policy, 0) {
		t.Fatal("floor carrier with a message in flight did not force reliable-only")
	}
	sequence.releaseUnreliableFlight(first)
	if sequence.reliableOnlyWrite(policy, 0) {
		t.Fatal("floor carrier with an empty flight refused its single probe message")
	}

	// the rule is opt-in: without it only a full flight forces reliable-only
	settings.UnreliableFloorSingleFlight = false
	sequence.observeCarrierWrite(first, transferWriteDisposition{unreliable: true})
	if sequence.reliableOnlyWrite(policy, 0) {
		t.Fatal("floor rule applied while disabled")
	}
	sequence.releaseUnreliableFlight(first)
}

// A reply keeps its carrier affinity on reliable carriers but never rides a
// potentially unreliable one while a reliable route is active.
func TestMultiRouteSelectorReplyAvoidsUnreliableCarrier(t *testing.T) {
	// FLIGHTGATEFIX §13.2 replaced the blanket rule this test first encoded
	// ("never pin a reply to a potentially unreliable carrier while a
	// reliable one is active") with a scoped one: the affine unreliable lane
	// keeps the reply while it has channel room and its ack clock is fresh;
	// a full or stale lane hands the reply to the reliable lanes.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	selector := NewMultiRouteSelector(ctx, "reply-affinity", nil, TransferPath{}, true)
	defer selector.Close()
	h1Transport := NewSendGatewayTransportWithType(TransportTypeH1)
	h1Route := make(Route, 16)
	selector.updateTransportWithProperties(h1Transport, []Route{h1Route}, TransferCarrierProperties{})
	p2pTransport := NewSendGatewayTransportWithType(TransportTypeP2p)
	p2pRoute := make(Route, 2)
	selector.updateTransportWithProperties(p2pTransport, []Route{p2pRoute}, TransferCarrierProperties{Unreliable: true})
	const staleAfter = time.Second

	// a healthy affine lane with room keeps the reply
	for i := 0; i < 2; i++ {
		success, disposition, err := selector.writeDetailedReplyWithCarrierPreference(ctx, []byte{byte(i)}, time.Second, TransportTypeP2p, staleAfter, false)
		if err != nil || !success || disposition.transportType != TransportTypeP2p {
			t.Fatalf("reply %d with healthy p2p affinity: success=%t disposition=%+v err=%v; want p2p", i, success, disposition, err)
		}
	}
	// the affine lane is full: the reply leaves on the reliable lane at once
	for i := 2; i < 6; i++ {
		success, disposition, err := selector.writeDetailedReplyWithCarrierPreference(ctx, []byte{byte(i)}, time.Second, TransportTypeP2p, staleAfter, false)
		if err != nil || !success || disposition.transportType != TransportTypeH1 {
			t.Fatalf("reply %d with full p2p affinity: success=%t disposition=%+v err=%v; want H1", i, success, disposition, err)
		}
	}
	if len(h1Route) != 4 || len(p2pRoute) != 2 {
		t.Fatalf("routes after replies: h1=%d p2p=%d, want 4/2", len(h1Route), len(p2pRoute))
	}
	// room again, but the lane's ack clock is stale: reliable first
	<-p2pRoute
	<-p2pRoute
	selector.observeRouteAckProgress(p2pRoute)
	clock, _ := selector.routeAckProgress.Load(p2pRoute)
	clock.(*atomic.Int64).Store(time.Now().Add(-2 * staleAfter).UnixNano())
	success, disposition, err := selector.writeDetailedReplyWithCarrierPreference(ctx, []byte{6}, time.Second, TransportTypeP2p, staleAfter, false)
	if err != nil || !success || disposition.transportType != TransportTypeH1 {
		t.Fatalf("reply with stale p2p affinity: success=%t disposition=%+v err=%v; want H1", success, disposition, err)
	}
	// a zero stale bound disables the clock rule
	success, disposition, err = selector.writeDetailedReplyWithCarrierPreference(ctx, []byte{7}, time.Second, TransportTypeP2p, 0, false)
	if err != nil || !success || disposition.transportType != TransportTypeP2p {
		t.Fatalf("reply with the clock rule off: success=%t disposition=%+v err=%v; want p2p", success, disposition, err)
	}

	// with only the unreliable carrier the reply keeps using it
	selector.updateTransport(h1Transport, nil)
	success, disposition, err = selector.writeDetailedReplyWithCarrierPreference(ctx, []byte{9}, time.Second, TransportTypeP2p, staleAfter, false)
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
	// §14: the fresh item's recovery is deferred to the moment the slowest
	// ack lane has had its chance, so nothing is written now and an ack
	// arriving first removes it; the stale item is written immediately.
	if !items[0].resendTime.After(currentTime) {
		t.Fatalf("fresh reliable-carried gap item was gap-resent behind fast-lane acks: due %s", items[0].resendTime.Sub(currentTime))
	}
	if !items[4].selectiveGapRecovered || items[4].recoveryKind != sendRecoverySelectiveGap ||
		items[4].resendTime.After(currentTime) {
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

// A timed-out reliable-carried item waits (at most twice) while cumulative
// acks are still advancing; it is re-sent at once when acks have stalled or
// when it rode the unreliable lane.
func TestSendSequenceDefersTimeoutResendWhileAcksProgress(t *testing.T) {
	settings := DefaultSendBufferSettings()
	sequence := testUnreliableRecoverySequence(settings)
	sequence.client = &Client{}
	sequence.flightController = newSendFlightController(settings)
	now := time.Now()

	item := &sendItem{transferFrameBytes: make([]byte, 64)}
	sequence.observeCarrierWrite(item, transferWriteDisposition{reliable: true})
	sequence.lastHeadAckTime = now.Add(-50 * time.Millisecond)
	if !sequence.deferTimeoutResend(item, now) || item.timeoutDeferCount != 1 || !item.resendTime.After(now) {
		t.Fatalf("first deferral: count=%d resendTime=%s", item.timeoutDeferCount, item.resendTime)
	}
	if !sequence.deferTimeoutResend(item, now) || item.timeoutDeferCount != 2 {
		t.Fatalf("second deferral: count=%d", item.timeoutDeferCount)
	}
	if sequence.deferTimeoutResend(item, now) {
		t.Fatal("third deferral granted; must resend")
	}
	if sequence.client.SendRecoveryStats().TimeoutResendDeferCount != 2 {
		t.Fatalf("deferral stat = %d, want 2", sequence.client.SendRecoveryStats().TimeoutResendDeferCount)
	}

	stalled := &sendItem{transferFrameBytes: make([]byte, 64)}
	sequence.observeCarrierWrite(stalled, transferWriteDisposition{reliable: true})
	sequence.lastHeadAckTime = now.Add(-30 * time.Second)
	if sequence.deferTimeoutResend(stalled, now) {
		t.Fatal("deferred a timeout while acks were stalled")
	}

	unreliable := &sendItem{transferFrameBytes: make([]byte, 64)}
	sequence.observeCarrierWrite(unreliable, transferWriteDisposition{unreliable: true})
	sequence.lastHeadAckTime = now
	if sequence.deferTimeoutResend(unreliable, now) {
		t.Fatal("deferred a timeout of an unreliable-carried item")
	}
	sequence.releaseUnreliableFlight(unreliable)
}

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

	// one ack lane only: the ordering rule is untouched, whichever lane
	sequenceOne, itemsOne := newSelectiveAckRecoveryTestSequence(8, sendTime)
	sequenceOne.client = &Client{}
	sequenceOne.flightController = newSendFlightController(sequenceOne.sendBufferSettings)
	sequenceOne.flightController.applyPolicy(transferFlightPolicySnapshot{generation: 1, limited: true})
	itemsOne[0].unreliableCarrierObserved = true
	itemsOne[0].unreliableFlightTracked = true
	for _, index := range []int{1, 2, 3, 5, 6, 7} {
		itemsOne[index].selectiveAcked = true
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
