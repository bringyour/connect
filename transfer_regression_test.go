package connect

// Regression tests for defects found by the 2026-08 flake hunt. Each fails
// when its fix is removed — that is the point of the file: a fix without a
// failing-first test is a defect waiting to come back.

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/protocol"
)

// TestFlushDeliverReleasesBatchBeforeCallback covers the delivery batch's
// ownership rule.
//
// The receive sequence buffers consecutive in-order items and dispatches their
// frames through one internal callback. That dispatcher can panic after
// partially dispatching a batch, so the batch must already be out of the
// sequence's fields when it runs. Otherwise the exit path's flush re-delivers
// the partial batch or acks frames whose dispatch failed. Client.receive
// separately isolates each registered application callback panic; the direct
// callback here models a failure outside that isolation boundary.
//
// Without the fix (batch cleared only after the callback returns) the panicking
// batch is still buffered, so the second flush re-delivers it and this fails.
func TestFlushDeliverReleasesBatchBeforeCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Cancel()

	seq := NewReceiveSequence(
		ctx,
		client,
		SourceId(NewId()),
		NewId(),
		sequenceTlsRoleServer,
		false,
		DefaultReceiveBufferSettings(),
	)

	deliveries := 0
	seq.deliverItems = append(seq.deliverItems, &receiveItem{
		receiveCallback: func(source TransferPath, frames []*protocol.Frame, peer Peer) {
			deliveries += 1
			panic("receive callback failed")
		},
	})
	seq.deliverFrames = append(seq.deliverFrames, &protocol.Frame{
		MessageType: protocol.MessageType_TransferPack,
	})

	func() {
		defer func() { recover() }()
		seq.flushDeliver()
	}()

	// the batch must be gone: a second flush (the exit path always runs one)
	// must have nothing left to deliver
	if 0 < len(seq.deliverItems) || 0 < len(seq.deliverFrames) {
		t.Fatal("a panicking callback left its batch buffered: the exit flush would re-deliver a half-processed batch")
	}
	func() {
		defer func() { recover() }()
		seq.flushDeliver()
	}()
	if deliveries != 1 {
		t.Fatalf("batch delivered %d times, want exactly 1", deliveries)
	}
}

// Application callback failures are isolated at the real Client.receive
// dispatch boundary. A failed observer cannot suppress later observers, and
// callback completion still constitutes transport delivery, so flushDeliver
// advances the reliable item's ACK window. This uses the exact cached callback
// installed in production ReceivePacks; injecting a raw callback into the item
// would bypass the behavior under test.
func TestFlushDeliverProductionDispatcherIsolatesApplicationPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &Client{
		log:              NewNoopLogger(),
		receiveCallbacks: NewCallbackList[ReceiveFunction](),
	}
	client.receiveCallback = client.receive

	panickingCallbackCalled := false
	client.AddReceiveCallback(func(TransferPath, []*protocol.Frame, Peer) {
		panickingCallbackCalled = true
		panic("application callback failed")
	})
	laterCallbackCalled := false
	client.AddReceiveCallback(func(TransferPath, []*protocol.Frame, Peer) {
		laterCallbackCalled = true
	})

	sequenceNumber := uint64(7)
	messageId := NewId()
	frame := &protocol.Frame{MessageType: protocol.MessageType_TransferExchangeSignals}
	seq := NewReceiveSequence(
		ctx,
		client,
		SourceId(NewId()),
		NewId(),
		sequenceTlsRoleServer,
		false,
		DefaultReceiveBufferSettings(),
	)
	seq.deliverItems = []*receiveItem{
		{
			transferItem: transferItem{
				messageId:      messageId,
				sequenceNumber: sequenceNumber,
			},
			receiveCallback: client.receiveCallback,
			ack:             true,
		},
	}
	seq.deliverFrames = []*protocol.Frame{frame}

	seq.flushDeliver()

	if !panickingCallbackCalled {
		t.Fatal("panicking application callback was not dispatched")
	}
	if !laterCallbackCalled {
		t.Fatal("application callback panic suppressed a later callback")
	}
	ackSnapshot := seq.ackWindow.Snapshot(true)
	if ackSnapshot.ackUpdateCount != 1 {
		t.Fatalf("ACK update count = %d, want 1", ackSnapshot.ackUpdateCount)
	}
	if ackSnapshot.headAck.sequenceNumber != sequenceNumber {
		t.Fatalf("ACK sequence number = %d, want %d", ackSnapshot.headAck.sequenceNumber, sequenceNumber)
	}
	if ackSnapshot.headAck.messageId != messageId {
		t.Fatalf("ACK message id = %s, want %s", ackSnapshot.headAck.messageId, messageId)
	}
}

// TestIdentityTimeoutHealsOnLateProof covers the identity-proof tombstone
// distinction.
//
// An epoch whose identity window expires is marked failed. That mark is a
// LIVENESS bound, not a security decision: the peer's proof may simply have
// been lost to transport churn, and a late one is bound to the same exporter,
// so verifying it is exactly as sound as verifying a timely one. Refusing it
// left the peer encrypting into a session that could never authenticate — a
// permanent stall until teardown.
//
// A verified-bad or malformed proof is different: that mark is terminal and
// must never heal, which the second half asserts.
//
// Without the fix the timed-out epoch refuses the late proof and this fails.
func TestIdentityTimeoutHealsOnLateProof(t *testing.T) {
	sess, cleanup := newTestEncryptionSession(t, sequenceTlsRoleClient)
	defer cleanup()

	e := injectTestEpoch(sess, true, nil)
	sess.stateLock.Lock()
	e.tlsExporter = make([]byte, 32)
	// the identity window expired with no proof in hand (the timeout path)
	e.identityFailed = true
	e.identityFailedTerminal = false
	sess.stateLock.Unlock()

	sess.receivePeerIdentityProof(make([]byte, 64))

	sess.stateLock.Lock()
	healed := !e.identityFailed
	buffered := 0 < len(e.pendingPeerIdentityProof)
	sess.stateLock.Unlock()
	if !healed || !buffered {
		t.Fatal("a late proof must clear the identity TIMEOUT and be evaluated; refusing it strands the peer encrypting into an unauthenticated session")
	}

	// a terminal failure (verified-bad or malformed proof) must not heal
	e2 := injectTestEpoch(sess, true, nil)
	sess.stateLock.Lock()
	e2.tlsExporter = make([]byte, 32)
	e2.identityFailed = true
	e2.identityFailedTerminal = true
	sess.stateLock.Unlock()

	sess.receivePeerIdentityProof(make([]byte, 64))

	sess.stateLock.Lock()
	stillFailed := e2.identityFailed
	stillEmpty := len(e2.pendingPeerIdentityProof) == 0
	sess.stateLock.Unlock()
	if !stillFailed || !stillEmpty {
		t.Fatal("a terminal identity failure must never heal: it marks a proof that was verified bad or malformed")
	}
}

// TestControlSyncRetriesAfterAckFailure covers the control-sync retry
// lifecycle.
//
// ControlSync owns a message until it is acknowledged and retries on failure.
// Its per-send handle context used to be cancelled by a defer that fired as
// soon as the message was ENQUEUED, which both doomed the queued frame (it
// carries that context) and made every retry exit immediately as "done". Any
// control message that enqueued and then failed — a contract close, a provide
// update, a key sync, all routine under transport churn — was lost silently.
//
// The message here is never acknowledged, so the send sequence times out and
// nacks it. With the fix that failure re-enters the retry and the frame is
// sent again; without it the first send is the only one and this fails.
func TestControlSyncRetriesAfterAckFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	// fail the send quickly instead of after the production ack timeout; the
	// assertion window below is far longer, so the test cannot race
	settings.SendBufferSettings.AckTimeout = 1 * time.Second
	settings.SendBufferSettings.IdleTimeout = 1 * time.Second

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	// a route that accepts (so the enqueue and write succeed) but never acks
	toControl := make(chan []byte, 64)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(ControlId)),
		[]Route{toControl},
	)

	sync := NewControlSync(ctx, client, "retry-scope")
	frame := &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: []byte("sync"),
	}
	go sync.Send(frame, nil, func(err error) {})

	// Count DISTINCT message ids. The transfer layer resends an unacked item
	// on its own — same message id — so counting writes would pass whether or
	// not ControlSync retried. A ControlSync retry builds a NEW pack, so a
	// second message id is the signature of the retry itself.
	// Require THREE: the initial send and its first retry happen even without
	// the fix (the initial send sits outside controlSync, so its failure
	// spawns one retry before the premature cancel takes effect). It is the
	// SECOND retry that the cancelled handle context kills.
	messageIds := map[string]bool{}
	deadline := time.Now().Add(45 * time.Second)
	for len(messageIds) < 3 && time.Now().Before(deadline) {
		select {
		case b := <-toControl:
			var tf protocol.TransferFrame
			if err := proto.Unmarshal(b, &tf); err != nil {
				continue
			}
			pack := tf.Pack
			if pack == nil {
				if f := tf.GetFrame(); f != nil && f.GetMessageType() == protocol.MessageType_TransferPack {
					pack = &protocol.Pack{}
					if err := proto.Unmarshal(f.MessageBytes, pack); err != nil {
						continue
					}
				}
			}
			if pack == nil || len(pack.MessageId) == 0 {
				continue
			}
			// the client also sends its own control traffic (pings, key
			// publishes) to ControlId; count only this sync's message
			mine := false
			for _, f := range pack.Frames {
				if string(f.GetMessageBytes()) == "sync" {
					mine = true
					break
				}
			}
			if mine {
				messageIds[string(pack.MessageId)] = true
			}
		case <-time.After(500 * time.Millisecond):
		}
	}
	if len(messageIds) < 3 {
		t.Fatalf("control message produced %d distinct send(s), want 3+: retries stop after the first, so a message that keeps failing is abandoned silently", len(messageIds))
	}
}

// Builds inert in-flight state for deterministic selective-ACK scheduling
// tests. No goroutine or wall-clock timer participates in these assertions.
func newSelectiveAckRecoveryTestSequence(
	itemCount int,
	sendTime time.Time,
) (*SendSequence, []*sendItem) {
	settings := DefaultSendBufferSettings()
	sequence := &SendSequence{
		sendBufferSettings: settings,
		resendQueue:        newResendQueue(nil, 0),
		rttWindow: NewRttWindow(
			NewNoopLogger(),
			settings.RttWindowSize,
			settings.RttWindowTimeout,
			settings.RttScale,
			settings.MinResendInterval,
			settings.RttMinResendInterval,
			settings.MaxResendInterval,
		),
	}
	items := make([]*sendItem, 0, itemCount)
	for i := range itemCount {
		item := &sendItem{
			transferItem: transferItem{
				messageId:      NewId(),
				sequenceNumber: uint64(i),
			},
			sendTime:           sendTime,
			resendTime:         sendTime.Add(settings.SelectiveAckTimeout),
			sendCount:          1,
			transferFrameBytes: []byte{byte(i)},
		}
		items = append(items, item)
		sequence.sendItems = append(sequence.sendItems, item)
		sequence.resendQueue.Add(item)
	}
	return sequence, items
}

// Three distinct later selective ACKs are receiver evidence for every earlier
// hole. One ACK round may release a bounded scoreboard of such holes instead of
// serializing each loss behind another full RTT.
func TestSelectiveAckGapSchedulesBoundedMissingPacks(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(100 * time.Millisecond)
	sequence, items := newSelectiveAckRecoveryTestSequence(8, sendTime)
	for _, index := range []int{1, 2, 3, 5, 6, 7} {
		items[index].selectiveAcked = true
	}

	sequence.scheduleSelectiveAckRecovery(currentTime)

	if items[0].resendTime != currentTime {
		t.Fatalf("earliest gap resend time = %s, want %s", items[0].resendTime, currentTime)
	}
	if !items[0].selectiveGapRecovered || items[0].recoveryKind != sendRecoverySelectiveGap {
		t.Fatal("earliest gap was not marked as one bounded selective recovery")
	}
	if items[4].resendTime != currentTime ||
		items[4].recoveryKind != sendRecoverySelectiveGap {
		t.Fatal("a second receiver-proven gap was serialized behind another RTT")
	}
	if first := sequence.resendQueue.PeekFirst(); first.resendTime != currentTime ||
		first.recoveryKind != sendRecoverySelectiveGap {
		t.Fatal("selective recovery items are not first in the resend queue")
	}
}

// A selective scoreboard never releases the whole missing window at once. The
// configured burst cap leaves later holes on their ordinary timers until ACK
// progress opens another recovery round.
func TestSelectiveAckGapRecoveryHonorsBurstBound(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(100 * time.Millisecond)
	sequence, items := newSelectiveAckRecoveryTestSequence(10, sendTime)
	for i := 5; i < len(items); i += 1 {
		items[i].selectiveAcked = true
	}

	sequence.scheduleSelectiveAckRecovery(currentTime)

	for i := 0; i < sequence.sendBufferSettings.SelectiveAckGapBurstSize; i += 1 {
		if items[i].resendTime != currentTime ||
			items[i].recoveryKind != sendRecoverySelectiveGap {
			t.Fatalf("proven gap %d was not released inside the burst", i)
		}
	}
	boundedItem := items[sequence.sendBufferSettings.SelectiveAckGapBurstSize]
	if boundedItem.resendTime != sendTime.Add(sequence.sendBufferSettings.SelectiveAckTimeout) ||
		boundedItem.recoveryKind != sendRecoveryNone {
		t.Fatal("selective gap recovery exceeded its burst bound")
	}
}

// One or two later deliveries are plausible packet reordering and must not
// trigger a fast retransmit. The production default deliberately requires
// three distinct selectively acknowledged Packs.
func TestSelectiveAckGapRequiresThreeDistinctLaterPacks(t *testing.T) {
	if threshold := DefaultSendBufferSettings().SelectiveAckGapThreshold; threshold != 3 {
		t.Fatalf("default selective gap threshold = %d, want 3", threshold)
	}
	if burstSize := DefaultSendBufferSettings().SelectiveAckGapBurstSize; burstSize != 4 {
		t.Fatalf("default selective gap burst size = %d, want 4", burstSize)
	}
	if limit := DefaultSendBufferSettings().AckTailProbeLimit; limit != 2 {
		t.Fatalf("default ACK tail probe limit = %d, want 2", limit)
	}
	sendTime := time.Unix(1_700_000_000, 0)
	for _, selectiveAckCount := range []int{1, 2} {
		sequence, items := newSelectiveAckRecoveryTestSequence(4, sendTime)
		sequence.sendBufferSettings.AckTailProbeLimit = 0
		for i := 1; i <= selectiveAckCount; i += 1 {
			items[i].selectiveAcked = true
		}
		originalResendTime := items[0].resendTime

		sequence.scheduleSelectiveAckRecovery(sendTime.Add(time.Second))

		if items[0].resendTime != originalResendTime ||
			items[0].selectiveGapRecovered || items[0].recoveryKind != sendRecoveryNone {
			t.Fatalf("%d later selective ACK(s) triggered gap recovery", selectiveAckCount)
		}
	}
}

// Cumulative progress with too few later ACKs to prove a gap is the tail-loss
// case. Only the oldest remaining Pack gets a bounded pair of delayed probes,
// and the delay uses minimum path RTT rather than a serialization-queue-inflated
// mean.
func TestAckTailProbeUsesMinimumRttAndSchedulesOnePack(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(300 * time.Millisecond)
	sequence, items := newSelectiveAckRecoveryTestSequence(4, sendTime)
	sequence.selectiveGapRecoveryActive = true
	sequence.rttWindow.closeSendTime(
		uint64(sendTime.Add(-250*time.Millisecond).UnixMilli()),
		sendTime,
	)
	sequence.rttWindow.closeSendTime(
		uint64(sendTime.Add(-2250*time.Millisecond).UnixMilli()),
		sendTime,
	)

	sequence.scheduleSelectiveAckRecovery(currentTime)

	expectedProbeTime := currentTime.Add(500 * time.Millisecond)
	if items[0].resendTime != expectedProbeTime {
		t.Fatalf("tail probe time = %s, want minimum-RTT time %s", items[0].resendTime, expectedProbeTime)
	}
	if items[0].ackTailProbeCount != 1 || items[0].recoveryKind != sendRecoveryAckTailProbe {
		t.Fatal("oldest remaining Pack was not marked for one tail probe")
	}
	for i := 1; i < len(items); i += 1 {
		if items[i].resendTime != sendTime.Add(sequence.sendBufferSettings.SelectiveAckTimeout) {
			t.Fatalf("remaining Pack %d was also tail-probed", i)
		}
	}

	sequence.scheduleSelectiveAckRecovery(currentTime.Add(100 * time.Millisecond))
	if items[0].resendTime != expectedProbeTime {
		t.Fatal("ACK progress moved a tail probe that was already pending")
	}

	removed := sequence.resendQueue.RemoveByMessageId(items[0].messageId)
	if removed != items[0] {
		t.Fatal("missing first tail probe item")
	}
	items[0].recoveryKind = sendRecoveryNone
	items[0].resendTime = expectedProbeTime.Add(5 * time.Second)
	sequence.resendQueue.Add(items[0])
	secondProgressTime := expectedProbeTime.Add(100 * time.Millisecond)
	sequence.scheduleSelectiveAckRecovery(secondProgressTime)
	secondProbeTime := secondProgressTime.Add(500 * time.Millisecond)
	if items[0].resendTime != secondProbeTime || items[0].ackTailProbeCount != 2 {
		t.Fatalf("second tail probe = %s/count %d, want %s/count 2", items[0].resendTime, items[0].ackTailProbeCount, secondProbeTime)
	}

	removed = sequence.resendQueue.RemoveByMessageId(items[0].messageId)
	if removed != items[0] {
		t.Fatal("missing second tail probe item")
	}
	items[0].recoveryKind = sendRecoveryNone
	items[0].resendTime = secondProbeTime.Add(5 * time.Second)
	sequence.resendQueue.Add(items[0])
	sequence.scheduleSelectiveAckRecovery(secondProbeTime.Add(100 * time.Millisecond))
	if items[0].resendTime != secondProbeTime.Add(5*time.Second) || items[0].ackTailProbeCount != 2 {
		t.Fatal("tail probing exceeded its two-attempt bound")
	}
}

// Ordinary cumulative progress on a reliable ordered carrier is not loss
// evidence. Tail probing stays dormant until this sequence has observed a
// threshold-qualified selective gap.
func TestAckTailProbeRequiresSelectiveGapEvidence(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	sequence, items := newSelectiveAckRecoveryTestSequence(2, sendTime)
	originalResendTime := items[0].resendTime
	sequence.rttWindow.closeSendTime(
		uint64(sendTime.Add(-250*time.Millisecond).UnixMilli()),
		sendTime,
	)

	sequence.scheduleSelectiveAckRecovery(sendTime.Add(300 * time.Millisecond))

	if items[0].resendTime != originalResendTime ||
		items[0].ackTailProbeCount != 0 || items[0].recoveryKind != sendRecoveryNone {
		t.Fatal("cumulative progress without a selective gap triggered a tail probe")
	}
}

// Repeated ACK snapshots retain the same later-delivery evidence. They must
// not repeatedly release one missing Pack and recreate a sender-side storm.
func TestSelectiveAckGapRecoveryIsOncePerMissingPack(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	firstRecoveryTime := sendTime.Add(100 * time.Millisecond)
	sequence, items := newSelectiveAckRecoveryTestSequence(4, sendTime)
	for i := 1; i < len(items); i += 1 {
		items[i].selectiveAcked = true
	}
	sequence.scheduleSelectiveAckRecovery(firstRecoveryTime)

	removed := sequence.resendQueue.RemoveByMessageId(items[0].messageId)
	if removed != items[0] {
		t.Fatal("missing scheduled gap item")
	}
	items[0].recoveryKind = sendRecoveryNone
	items[0].resendTime = firstRecoveryTime.Add(2 * time.Second)
	sequence.resendQueue.Add(items[0])

	sequence.scheduleSelectiveAckRecovery(firstRecoveryTime.Add(10 * time.Millisecond))

	if items[0].resendTime != firstRecoveryTime.Add(2*time.Second) {
		t.Fatal("the same gap received more than one immediate recovery")
	}
	if items[0].recoveryKind != sendRecoveryNone {
		t.Fatal("repeated selective evidence re-armed immediate recovery")
	}
}

// A fast gap retransmit can itself be lost on a saturated uplink. Continued ACK
// progress may shorten its queue-inflated ordinary RTO once, but only with a
// minimum-RTT-delayed tail probe—not another immediate fast retransmit.
func TestSelectiveAckGapRecoveryAllowsOnePacedFollowup(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	firstRecoveryTime := sendTime.Add(100 * time.Millisecond)
	sequence, items := newSelectiveAckRecoveryTestSequence(4, sendTime)
	for i := 1; i < len(items); i += 1 {
		items[i].selectiveAcked = true
	}
	sequence.scheduleSelectiveAckRecovery(firstRecoveryTime)

	removed := sequence.resendQueue.RemoveByMessageId(items[0].messageId)
	if removed != items[0] {
		t.Fatal("missing fast gap recovery item")
	}
	items[0].recoveryKind = sendRecoveryNone
	items[0].resendTime = firstRecoveryTime.Add(5 * time.Second)
	sequence.resendQueue.Add(items[0])
	sequence.rttWindow.closeSendTime(
		uint64(firstRecoveryTime.Add(-250*time.Millisecond).UnixMilli()),
		firstRecoveryTime,
	)
	sequence.rttWindow.closeSendTime(
		uint64(firstRecoveryTime.Add(-2250*time.Millisecond).UnixMilli()),
		firstRecoveryTime,
	)

	ackProgressTime := firstRecoveryTime.Add(100 * time.Millisecond)
	sequence.scheduleSelectiveAckRecovery(ackProgressTime)

	expectedProbeTime := ackProgressTime.Add(500 * time.Millisecond)
	if items[0].resendTime != expectedProbeTime {
		t.Fatalf("paced follow-up time = %s, want %s", items[0].resendTime, expectedProbeTime)
	}
	if items[0].ackTailProbeCount != 1 || items[0].recoveryKind != sendRecoveryAckTailProbe {
		t.Fatal("lost fast gap recovery did not receive one paced follow-up")
	}

	sequence.scheduleSelectiveAckRecovery(ackProgressTime.Add(100 * time.Millisecond))
	if items[0].resendTime != expectedProbeTime {
		t.Fatal("ACK progress moved a paced follow-up that was already pending")
	}
}

// A written gap recovery gets one conservative fallback without waiting for a
// new ACK edge. It is delayed by one minimum probe interval so a successful
// gap write can be acknowledged before any duplicate enters QUIC.
func TestSelectiveGapWriteSchedulesOneConservativeProbe(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	sequence, items := newSelectiveAckRecoveryTestSequence(1, sendTime)
	item := sequence.resendQueue.RemoveByMessageId(items[0].messageId)
	if item != items[0] {
		t.Fatal("missing gap recovery item")
	}
	sequence.rttWindow.closeSendTime(
		uint64(sendTime.Add(-250*time.Millisecond).UnixMilli()),
		sendTime,
	)
	sequence.rttWindow.closeSendTime(
		uint64(sendTime.Add(-2250*time.Millisecond).UnixMilli()),
		sendTime,
	)
	ackDeadline := sendTime.Add(time.Minute)

	if !sequence.scheduleGapRecoveryProbe(item, sendTime, ackDeadline) {
		t.Fatal("gap recovery did not schedule its conservative fallback")
	}
	expectedProbeTime := sendTime.Add(500 * time.Millisecond)
	if item.resendTime != expectedProbeTime || item.ackTailProbeCount != 1 ||
		item.recoveryKind != sendRecoveryAckTailProbe || !item.gapFollowupScheduled {
		t.Fatalf("gap fallback = %s/count %d/kind %d", item.resendTime, item.ackTailProbeCount, item.recoveryKind)
	}
	if sequence.scheduleGapRecoveryProbe(item, expectedProbeTime, ackDeadline) {
		t.Fatal("one gap write scheduled more than one automatic fallback")
	}
}

// When every outstanding Pack was selectively acknowledged, data is already
// present and the missing state is the cumulative ACK. Probe only the oldest
// item at the measured RTT cadence, rather than waiting for the maximum resend
// interval or duplicating the entire window.
func TestSelectiveAckCumulativeProbeUsesMeasuredRtt(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	sequence, items := newSelectiveAckRecoveryTestSequence(4, sendTime)
	for _, item := range items {
		item.selectiveAcked = true
	}
	sequence.rttWindow.closeSendTime(
		uint64(sendTime.Add(-250*time.Millisecond).UnixMilli()),
		sendTime,
	)

	sequence.scheduleSelectiveAckRecovery(sendTime)

	expectedProbeTime := sendTime.Add(500 * time.Millisecond)
	if items[0].resendTime != expectedProbeTime {
		t.Fatalf("cumulative ACK probe time = %s, want sampled RTT time %s", items[0].resendTime, expectedProbeTime)
	}
	if items[0].recoveryKind != sendRecoveryCumulativeProbe {
		t.Fatal("oldest selectively acknowledged item was not marked as the probe")
	}
	for i := 1; i < len(items); i += 1 {
		if items[i].resendTime != sendTime.Add(sequence.sendBufferSettings.SelectiveAckTimeout) {
			t.Fatalf("selectively acknowledged item %d was also scheduled", i)
		}
	}

	sequence.scheduleSelectiveAckRecovery(sendTime.Add(100 * time.Millisecond))
	if items[0].resendTime != expectedProbeTime {
		t.Fatal("re-reading the same ACK evidence moved the existing probe")
	}
}

// The real SendSequence must apply the scheduler after ACK-window coalescing,
// not merely pass its isolated state tests. Four separately observed Packs and
// three explicit later selective ACKs force retransmission of the first Pack
// while its ordinary timeout is one hour away.
func TestSelectiveAckGapRecoversEarliestMissingPackEndToEnd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.MinResendInterval = time.Hour
	settings.SendBufferSettings.RttMinResendInterval = time.Hour
	settings.SendBufferSettings.MaxResendInterval = time.Hour
	settings.SendBufferSettings.AckTimeout = 2 * time.Hour
	settings.SendBufferSettings.SelectiveAckTimeout = 2 * time.Hour
	settings.SendBufferSettings.SelectiveAckGapThreshold = 3

	peerId := NewId()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()
	client.ContractManager().AddNoContractPeer(peerId)

	toPeer := make(chan []byte, 64)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(peerId)),
		[]Route{toPeer},
	)
	fromPeer := make(chan []byte, 64)
	client.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{fromPeer})

	readPack := func(timeout time.Duration) *protocol.Pack {
		for {
			select {
			case transferFrameBytes := <-toPeer:
				var transferFrame protocol.TransferFrame
				err := proto.Unmarshal(transferFrameBytes, &transferFrame)
				MessagePoolReturn(transferFrameBytes)
				if err != nil {
					continue
				}
				if transferFrame.Pack != nil {
					return transferFrame.Pack
				}
				frame := transferFrame.GetFrame()
				if frame == nil || frame.GetMessageType() != protocol.MessageType_TransferPack {
					continue
				}
				pack := &protocol.Pack{}
				if err := proto.Unmarshal(frame.MessageBytes, pack); err == nil {
					return pack
				}
			case <-time.After(timeout):
				return nil
			}
		}
	}

	initialPacks := make([]*protocol.Pack, 0, 4)
	for i := range 4 {
		frame := &protocol.Frame{
			MessageType:  protocol.MessageType_TransferExchangeSignals,
			MessageBytes: []byte(fmt.Sprintf("selective-gap-%d", i)),
		}
		if !client.SendWithTimeout(frame, peerId, nil, 5*time.Second) {
			t.Fatalf("send %d failed", i)
		}
		pack := readPack(20 * time.Second)
		if pack == nil {
			t.Fatalf("no initial Pack %d", i)
		}
		initialPacks = append(initialPacks, pack)
	}

	for _, pack := range initialPacks[1:] {
		ackBytes, err := proto.Marshal(&protocol.TransferFrame{
			TransferPath: TransferPath{
				SourceId:      peerId,
				DestinationId: client.ClientId(),
			}.ToProtobuf(),
			Ack: &protocol.Ack{
				MessageId:  pack.MessageId,
				SequenceId: pack.SequenceId,
				Selective:  true,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		select {
		case fromPeer <- ackBytes:
		case <-time.After(5 * time.Second):
			t.Fatal("could not deliver selective ACK")
		}
	}

	recoveryPack := readPack(20 * time.Second)
	if recoveryPack == nil {
		t.Fatal("three later selective ACKs did not recover the earliest gap")
	}
	if string(recoveryPack.MessageId) != string(initialPacks[0].MessageId) ||
		recoveryPack.SequenceNumber != initialPacks[0].SequenceNumber {
		t.Fatalf(
			"recovered Pack = %d/%x, want earliest %d/%x",
			recoveryPack.SequenceNumber,
			recoveryPack.MessageId,
			initialPacks[0].SequenceNumber,
			initialPacks[0].MessageId,
		)
	}
}

// TestSelectiveAckPauseKeepsAProbeScheduled covers the selective-ack pause
// probe.
//
// A selective ack pauses that item's resend for SelectiveAckTimeout (a
// minute). When every in-flight item is paused, nothing is scheduled: the
// receiver re-acks only on duplicates, and none are coming, so both sides go
// quiet for the whole pause — long enough to look like a dead path. The fix
// keeps ONE probe scheduled on the ordinary resend cadence; its duplicate
// re-elicits the receiver's ack state.
//
// Without the fix no resend follows the selective ack and this fails.
func TestSelectiveAckPauseKeepsAProbeScheduled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	// a short probe cadence; the assertion window is many times longer
	settings.SendBufferSettings.MinResendInterval = 500 * time.Millisecond
	settings.SendBufferSettings.RttMinResendInterval = 500 * time.Millisecond
	settings.SendBufferSettings.MaxResendInterval = 1 * time.Second
	settings.SendBufferSettings.AckTimeout = 120 * time.Second
	settings.SendBufferSettings.SelectiveAckTimeout = 120 * time.Second

	peerId := NewId()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()
	client.ContractManager().AddNoContractPeer(peerId)

	toPeer := make(chan []byte, 64)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(peerId)),
		[]Route{toPeer},
	)
	fromPeer := make(chan []byte, 64)
	client.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{fromPeer})

	frame := &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: []byte("probe"),
	}
	if !client.SendWithTimeout(frame, peerId, nil, 5*time.Second) {
		t.Fatal("send failed")
	}

	// take the first transmission and selectively ack it: the sender now
	// treats the item as parked for SelectiveAckTimeout
	readPack := func(timeout time.Duration) *protocol.Pack {
		for {
			select {
			case b := <-toPeer:
				var tf protocol.TransferFrame
				if err := proto.Unmarshal(b, &tf); err != nil {
					continue
				}
				if tf.Pack != nil {
					return tf.Pack
				}
				if f := tf.GetFrame(); f != nil && f.GetMessageType() == protocol.MessageType_TransferPack {
					pack := &protocol.Pack{}
					if err := proto.Unmarshal(f.MessageBytes, pack); err == nil {
						return pack
					}
				}
			case <-time.After(timeout):
				return nil
			}
		}
	}
	first := readPack(20 * time.Second)
	if first == nil {
		t.Fatal("no first transmission")
	}

	ackBytes, err := proto.Marshal(&protocol.TransferFrame{
		TransferPath: TransferPath{
			SourceId:      peerId,
			DestinationId: client.ClientId(),
		}.ToProtobuf(),
		Ack: &protocol.Ack{
			MessageId:  first.MessageId,
			SequenceId: first.SequenceId,
			Selective:  true,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case fromPeer <- ackBytes:
	case <-time.After(5 * time.Second):
		t.Fatal("could not deliver the selective ack")
	}

	// a probe must follow well inside the pause window
	if probe := readPack(30 * time.Second); probe == nil {
		t.Fatal("no resend after a selective ack: with every in-flight item paused the sequence goes silent for the whole pause, and a lost cumulative ack has nothing to heal it")
	}
}

// TestIdentityProofResendsDuringEstablishment is the regression test for a
// lost identity proof stalling a peer's whole establishment window.
//
// The proof is otherwise sent exactly once. A one-shot control message can be
// lost to transport or sequence churn — a wire-indistinguishable sequence
// retire mid-handshake dropped one in practice — and the peer then waits out
// its full TlsTimeout for a proof that will never arrive, while data frames
// under the new cipher stay undecryptable. The fix resends on
// IdentityProofResendInterval for the establishment window; the resend is
// unilateral and cheap because the receive side dedups verified proofs.
//
// Two clients run the real handshake over real contracts, and the A->B wire is
// tapped. EncryptedControl rides ForceUnwrapped (it bootstraps the cipher), so
// proofs stay readable on the wire even after establishment.
//
// Distinct PACK IDS are counted, not writes: the send sequence retransmits an
// unacked pack under its original message id, so counting writes would pass
// with or without the resend. Each application-level resend enqueues a new
// pack, so a new message id is the signature of the resend itself. The count
// is filtered to A's client-role session, since A also runs a server-role
// session for B's handshake and each would contribute its own one-shot proof.
//
// Without the fix A emits exactly one client-role proof, and this fails.
func TestIdentityProofResendsDuringEstablishment(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aClientId := NewId()
	bClientId := NewId()

	aSend := make(chan []byte)
	bSend := make(chan []byte)

	// tap A->B and count this session's proofs; B receives everything
	var proofPackIds sync.Map
	var proofCount atomic.Int64
	bReceive := make(chan []byte)
	go func() {
		for {
			var b []byte
			select {
			case <-ctx.Done():
				return
			case b = <-aSend:
			}
			countClientRoleIdentityProofs(b, &proofPackIds, &proofCount)
			select {
			case <-ctx.Done():
				return
			case bReceive <- b:
			}
		}
	}()
	bConditioner, aReceive := newConditioner(ctx, bSend)
	bConditioner.update(func() { bConditioner.randomDelay = 20 * time.Millisecond })

	provideModes := map[protocol.ProvideMode]bool{protocol.ProvideMode_Network: true}

	makeSettings := func() *ClientSettings {
		s := DefaultClientSettings()
		s.SendBufferSettings.SequenceBufferSize = 0
		s.SendBufferSettings.AckBufferSize = 0
		s.SendBufferSettings.AckTimeout = 60 * time.Second
		s.SendBufferSettings.IdleTimeout = 60 * time.Second
		s.SendBufferSettings.MinResendInterval = 10 * time.Millisecond
		s.ReceiveBufferSettings.SequenceBufferSize = 0
		s.ReceiveBufferSettings.GapTimeout = 60 * time.Second
		s.ReceiveBufferSettings.IdleTimeout = 60 * time.Second
		s.ForwardBufferSettings.SequenceBufferSize = 0
		s.ForwardBufferSettings.IdleTimeout = 1 * time.Second
		s.ContractManagerSettings.LegacyCreateContract = false
		s.EncryptionSettings.Mode = EncryptionModeOpportunistic
		s.EncryptionSettings.TlsTimeout = 30 * time.Second
		s.EncryptionSettings.EncryptionControlUseCompanion = false
		// a fast cadence against a much longer assertion window, so the test
		// cannot become flaky on the passing path
		s.EncryptionSettings.IdentityProofResendInterval = 200 * time.Millisecond
		return s
	}

	var a, b *Client
	aOob := &grantingClientOob{
		sourceId: aClientId,
		settings: DefaultContractManagerSettings(),
		destSecretKey: func(destinationId Id) ([]byte, bool) {
			return b.ContractManager().GetProvideSecretKey(protocol.ProvideMode_Network)
		},
		destClientPublicKey: func(destinationId Id) []byte {
			return b.ClientKeyManager().PublicKey()
		},
	}
	bOob := &grantingClientOob{
		sourceId: bClientId,
		settings: DefaultContractManagerSettings(),
		destSecretKey: func(destinationId Id) ([]byte, bool) {
			return a.ContractManager().GetProvideSecretKey(protocol.ProvideMode_Network)
		},
		destClientPublicKey: func(destinationId Id) []byte {
			return a.ClientKeyManager().PublicKey()
		},
	}

	a = NewClient(ctx, aClientId, aOob, makeSettings())
	defer a.Cancel()
	a.RouteManager().UpdateTransport(newDataGatewayTransport(), []Route{aSend})
	a.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{aReceive})
	blackholeControlId(ctx, a.RouteManager())
	a.ContractManager().SetProvideModes(provideModes)

	b = NewClient(ctx, bClientId, bOob, makeSettings())
	defer b.Cancel()
	b.RouteManager().UpdateTransport(newDataGatewayTransport(), []Route{bSend})
	b.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{bReceive})
	blackholeControlId(ctx, b.RouteManager())
	b.ContractManager().SetProvideModes(provideModes)

	// drive both directions so each client acquires its per-peer session and
	// the handshakes run
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		send := func(client *Client, dst Id, label string) {
			m := &protocol.SimpleMessage{Content: label}
			if frame, err := ToFrame(m, DefaultProtocolVersion); err == nil {
				client.Send(frame, dst, func(error) {})
			}
		}
		for i := 0; ; i += 1 {
			select {
			case <-stop:
				return
			case <-ctx.Done():
				return
			default:
			}
			send(a, bClientId, fmt.Sprintf("a%d", i))
			send(b, aClientId, fmt.Sprintf("b%d", i))
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}()

	// the resend interval backs off x2 per round (0.2s, 0.6s, 1.4s, ...), so
	// three proofs land within ~2s of the handshake completing
	deadline := time.Now().Add(25 * time.Second)
	for proofCount.Load() < 3 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if got := proofCount.Load(); got < 3 {
		t.Fatalf("A emitted %d distinct client-role identity proof(s), want 3+: a proof lost to churn is never replaced and the peer waits out its whole establishment window", got)
	}
}

// countClientRoleIdentityProofs records the pack id of a transfer frame that
// carries a client-role identity proof, counting each pack once.
func countClientRoleIdentityProofs(b []byte, seen *sync.Map, count *atomic.Int64) {
	var tf protocol.TransferFrame
	if proto.Unmarshal(b, &tf) != nil {
		return
	}
	pack := tf.Pack
	if pack == nil {
		if f := tf.GetFrame(); f != nil && f.GetMessageType() == protocol.MessageType_TransferPack {
			pack = &protocol.Pack{}
			if proto.Unmarshal(f.MessageBytes, pack) != nil {
				return
			}
		}
	}
	if pack == nil || len(pack.MessageId) == 0 {
		return
	}
	for _, frame := range pack.Frames {
		if frame.GetMessageType() != protocol.MessageType_TransferEncryptedControl {
			continue
		}
		ec := &protocol.EncryptedControl{}
		if proto.Unmarshal(frame.GetMessageBytes(), ec) != nil {
			continue
		}
		if ec.ControlType != protocol.EncryptedControlType_EncryptedControlIdentityProof {
			continue
		}
		if ec.SessionRole != protocol.SequenceRole_SequenceRoleClient {
			continue
		}
		if _, loaded := seen.LoadOrStore(string(pack.MessageId), true); !loaded {
			count.Add(1)
		}
	}
}
