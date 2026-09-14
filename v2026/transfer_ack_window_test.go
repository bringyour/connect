package connect

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// immediateAckTestWriter returns a peer Ack from inside the initial route
// write, before the sender can execute anything after that call.
type immediateAckTestWriter struct {
	t           *testing.T
	sequence    *SendSequence
	handoff     receiveAckHandoffResult
	handoffErr  error
	writeCalled bool
}

// Write consumes the successful route share after synchronously handing its
// decoded Pack back to the sequence as a cumulative Ack.
func (self *immediateAckTestWriter) Write(
	ctx context.Context,
	transferFrameBytes []byte,
	timeout time.Duration,
) error {
	defer MessagePoolReturn(transferFrameBytes)
	self.writeCalled = true
	pack := decodeSendPackLifecycleWirePack(self.t, transferFrameBytes)
	messageId, err := IdFromBytes(pack.MessageId)
	if err != nil {
		self.t.Fatalf("decode immediate Ack message id: %v", err)
	}
	sequenceId, err := IdFromBytes(pack.SequenceId)
	if err != nil {
		self.t.Fatalf("decode immediate Ack sequence id: %v", err)
	}
	self.handoff, self.handoffErr = self.sequence.ackMessageDetailed(
		receiveAckMessage{
			messageId:  messageId,
			sequenceId: sequenceId,
		},
		0,
	)
	return nil
}

// WriteDetailed implements the compatibility writer path used outside this
// regression; this test's sender calls Write directly.
func (self *immediateAckTestWriter) WriteDetailed(
	ctx context.Context,
	transferFrameBytes []byte,
	timeout time.Duration,
) (bool, error) {
	err := self.Write(ctx, transferFrameBytes, timeout)
	return err == nil, err
}

// GetActiveRoutes reports no physical routes for this synchronous test writer.
func (self *immediateAckTestWriter) GetActiveRoutes() []Route {
	return nil
}

// GetInactiveRoutes reports no physical routes for this synchronous test writer.
func (self *immediateAckTestWriter) GetInactiveRoutes() []Route {
	return nil
}

// An Ack can return before the initial route write does. The sender must index
// the item before publication so that Ack becomes pending progress rather than
// a permanent miss that waits for Transfer recovery.
func TestInitialWritePublishesAckIdentityBeforePeerExposure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := &Client{
		ctx:       ctx,
		clientId:  NewId(),
		clientTag: "immediate-ack",
		log:       NewNoopLogger(),
	}
	settings := DefaultSendBufferSettings()
	sequence := NewSendSequence(
		ctx,
		client,
		nil,
		NewId(),
		MultiHopId{},
		false,
		false,
		false,
		sequenceTlsRoleClient,
		false,
		settings,
	)
	sequence.acks = make(chan receiveAckMessage)
	writer := &immediateAckTestWriter{t: t, sequence: sequence}
	sequence.contractMultiRouteWriter = writer
	sequence.contractMultiRouteWriterDestination = DestinationId(sequence.destination)

	var callbackCount int
	var callbackErr error
	messageBytes := MessagePoolCopy([]byte("immediate Ack"))
	sequence.send(
		[]*protocol.Frame{{
			MessageType:  protocol.MessageType_TransferExchangeSignals,
			MessageBytes: messageBytes,
		}},
		func(err error) {
			callbackCount += 1
			callbackErr = err
		},
		true,
		false,
	)

	if !writer.writeCalled || writer.handoffErr != nil ||
		writer.handoff != receiveAckHandoffAccepted {
		t.Fatalf(
			"synchronous Ack write=(called=%t handoff=%d err=%v)",
			writer.writeCalled,
			writer.handoff,
			writer.handoffErr,
		)
	}
	snapshot := sequence.ackWindow.Snapshot(true)
	if snapshot.ackUpdateCount != 1 {
		t.Fatalf("synchronous Ack updates=%d, want 1", snapshot.ackUpdateCount)
	}
	sequence.receiveAck(
		snapshot.headAck.messageId,
		false,
		snapshot.headAck.tag,
		snapshot.headAck.compactContractRecoverySupported,
	)
	if queueCount, _ := sequence.resendQueue.QueueSize(); queueCount != 0 {
		t.Fatalf("synchronous Ack left %d resend items", queueCount)
	}
	if len(sequence.sendItems) != 0 {
		t.Fatalf("synchronous Ack left %d send items", len(sequence.sendItems))
	}
	if callbackCount != 1 || callbackErr != nil {
		t.Fatalf("synchronous Ack callback=(count=%d err=%v)", callbackCount, callbackErr)
	}
}

func TestReceiveAckH1HandoffWaitRescuesFullCompactQueue(t *testing.T) {
	settings := DefaultReceiveBufferSettingsWithBufferSize(1)
	settings.H1AckHandoffTimeout = 200 * time.Millisecond
	if got := settings.ackHandoffTimeout(TransportTypeH1); got != 200*time.Millisecond {
		t.Fatalf("H1 ACK handoff timeout = %s, want 200ms", got)
	}
	for _, transportType := range []TransportType{TransportTypeUnknown, TransportTypeH3} {
		if got := settings.ackHandoffTimeout(transportType); got != 0 {
			t.Fatalf("non-H1 ACK handoff timeout = %s, want zero", got)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sequenceId := NewId()
	sequence := &SendSequence{
		ctx:        ctx,
		sequenceId: sequenceId,
		acks:       make(chan receiveAckMessage, 1),
	}
	sequence.acks <- receiveAckMessage{sequenceId: sequenceId, messageId: NewId()}

	result := make(chan receiveAckHandoffResult, 1)
	go func() {
		handoff, _ := sequence.ackMessageDetailed(
			receiveAckMessage{sequenceId: sequenceId, messageId: NewId()},
			settings.ackHandoffTimeout(TransportTypeH1),
		)
		result <- handoff
	}()
	select {
	case premature := <-result:
		t.Fatalf("H1 ACK wait completed before queue space: %v", premature)
	case <-time.After(10 * time.Millisecond):
	}
	<-sequence.acks
	select {
	case got := <-result:
		if got != receiveAckHandoffAcceptedAfterWait {
			t.Fatalf("H1 ACK handoff result = %v, want accepted after wait", got)
		}
		client := &Client{}
		client.recordReceiveAckHandoff(got)
		stats := client.ReceiveStats()
		if stats.AckHandoffWaitCount != 1 || stats.AckHandoffWaitSuccess != 1 ||
			stats.AckHandoffDropCount != 0 {
			t.Fatalf("rescued ACK handoff stats = %+v", stats)
		}
	case <-time.After(time.Second):
		t.Fatal("H1 ACK handoff did not wake after queue space")
	}
}

func TestReceiveAckFullHandoffCoalescesWithoutWaitingOrAllocatingDepth(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sequenceId := NewId()
	olderMessageId := NewId()
	messageId := NewId()
	ackWindow := newSequenceAckWindow()
	resendQueue := newResendQueue(nil, 0)
	resendQueue.Add(&sendItem{transferItem: transferItem{
		messageId:        olderMessageId,
		messageByteCount: 1,
		sequenceNumber:   6,
	}})
	resendQueue.Add(&sendItem{transferItem: transferItem{
		messageId:        messageId,
		messageByteCount: 1,
		sequenceNumber:   7,
	}})
	sequence := &SendSequence{
		ctx:         ctx,
		client:      &Client{},
		sequenceId:  sequenceId,
		acks:        make(chan receiveAckMessage, 1),
		ackWindow:   ackWindow,
		resendQueue: resendQueue,
	}
	sequence.acks <- receiveAckMessage{sequenceId: sequenceId, messageId: olderMessageId}

	start := time.Now()
	handoff, err := sequence.ackMessageDetailed(
		receiveAckMessage{sequenceId: sequenceId, messageId: messageId},
		200*time.Millisecond,
	)
	if err != nil || handoff != receiveAckHandoffAccepted {
		t.Fatalf("coalesced ACK handoff = (%v, %v), want accepted", handoff, err)
	}
	if elapsed := time.Since(start); elapsed >= 100*time.Millisecond {
		t.Fatalf("coalesced ACK handoff waited %s on a full channel", elapsed)
	}
	if len(sequence.acks) != 1 {
		t.Fatalf("coalesced ACK changed compact queue depth to %d, want 1", len(sequence.acks))
	}
	// The worker may process the older queued cumulative ACK after the newer
	// fallback update. The shared window must remain monotonic in that order.
	sequence.coalesceReceivedAck(ackWindow, <-sequence.acks)
	snapshot := ackWindow.Snapshot(true)
	if snapshot.ackUpdateCount != 2 || snapshot.headAck.messageId != messageId ||
		snapshot.headAck.sequenceNumber != 7 {
		t.Fatalf("coalesced ACK snapshot = %+v", snapshot)
	}
}

func TestReceiveAckHandoffClassifiesQueueFullAndMissingSequence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	destination := NewId()
	sequenceId := NewId()
	sequence := &SendSequence{
		ctx:         ctx,
		destination: destination,
		sequenceId:  sequenceId,
		acks:        make(chan receiveAckMessage, 1),
	}
	sequence.acks <- receiveAckMessage{sequenceId: sequenceId, messageId: NewId()}
	buffer := &SendBuffer{
		ctx: ctx,
		log: NewNoopLogger(),
		sendSequencesBySequenceId: map[Id]*SendSequence{
			sequenceId: sequence,
		},
	}
	client := &Client{log: NewNoopLogger()}
	full := buffer.ackMessageDetailed(
		destination,
		receiveAckMessage{sequenceId: sequenceId, messageId: NewId()},
		0,
	)
	client.recordReceiveAckHandoff(full)
	missing := buffer.ackMessageDetailed(
		destination,
		receiveAckMessage{sequenceId: NewId(), messageId: NewId()},
		0,
	)
	client.recordReceiveAckHandoff(missing)
	stats := client.ReceiveStats()
	if full != receiveAckHandoffQueueFull ||
		missing != receiveAckHandoffSequenceMissing ||
		stats.AckHandoffDropCount != 2 ||
		stats.AckHandoffQueueFullCount != 1 ||
		stats.AckHandoffMissCount != 1 {
		t.Fatalf("classified ACK handoff stats = %+v (full=%v missing=%v)", stats, full, missing)
	}
}

func TestReceiveAckRouteWriteStatsSeparateBlockedWaitsAndErrors(t *testing.T) {
	client := &Client{}
	client.recordReceiveAckRouteWrite(TransportTypeH1, 2*time.Millisecond, false, true, nil)
	client.recordReceiveAckRouteWrite(TransportTypeP2p, 5*time.Millisecond, true, false, nil)
	client.recordReceiveAckRouteWrite(TransportTypeP2p, 7*time.Millisecond, true, false, errors.New("write"))
	stats := client.ReceiveStats()
	if stats.AckRouteWriteCount != 3 ||
		stats.AckRoutePriorityWriteCount != 1 ||
		stats.AckRouteWriteBlockedCount != 2 ||
		stats.AckRouteWriteErrorCount != 1 ||
		stats.AckRouteWriteWaitDuration != 12*time.Millisecond ||
		stats.AckRouteWriteMaxWait != 7*time.Millisecond {
		t.Fatalf("ACK route-write stats = %+v", stats)
	}
	// FLIGHTGATEFIX §8 (M2): the same writes broken out by the carrier the
	// answered Pack arrived on.
	if stats.AckRouteWriteCountByTransport[TransportTypeH1] != 1 ||
		stats.AckRouteWriteCountByTransport[TransportTypeP2p] != 2 ||
		stats.AckRouteWriteWaitByTransport[TransportTypeH1] != 0 ||
		stats.AckRouteWriteWaitByTransport[TransportTypeP2p] != 12*time.Millisecond ||
		stats.AckRouteWriteTimeoutByTransport[TransportTypeH1] != 0 ||
		stats.AckRouteWriteTimeoutByTransport[TransportTypeP2p] != 1 {
		t.Fatalf("ACK route-write stats by transport = %+v", stats)
	}
	if _, ok := stats.AckRouteWriteCountByTransport[TransportTypeH3]; ok {
		t.Fatalf("carrier without writes appeared in the snapshot: %+v", stats)
	}
}

func BenchmarkReceiveAckRouteWriteStatsImmediate(b *testing.B) {
	client := &Client{}
	b.ReportAllocs()
	for range b.N {
		client.recordReceiveAckRouteWrite(TransportTypeUnknown, 0, false, false, nil)
	}
}

func TestSequenceAckWindowCoalescesReusableNotification(t *testing.T) {
	window := newSequenceAckWindow()
	empty := window.Snapshot(false)

	window.Update(sequenceAck{sequenceNumber: 1, messageId: NewId()})
	select {
	case <-empty.ackNotify:
	case <-time.After(time.Second):
		t.Fatal("ack update did not notify snapshot waiter")
	}

	first := window.Snapshot(true)
	if first.ackUpdateCount != 1 {
		t.Fatalf("first snapshot update count = %d, want 1", first.ackUpdateCount)
	}
	select {
	case <-first.ackNotify:
		t.Fatal("reset left a stale ack notification")
	default:
	}

	window.Update(sequenceAck{sequenceNumber: 2, messageId: NewId()})
	window.Update(sequenceAck{sequenceNumber: 3, messageId: NewId()})
	select {
	case <-first.ackNotify:
	default:
		t.Fatal("coalesced updates did not notify")
	}
	select {
	case <-first.ackNotify:
		t.Fatal("coalesced updates queued more than one notification")
	default:
	}

	coalesced := window.Snapshot(true)
	if coalesced.ackUpdateCount != 2 {
		t.Fatalf("coalesced snapshot update count = %d, want 2", coalesced.ackUpdateCount)
	}
}

func TestSequenceAckWindowSteadyStateDoesNotAllocate(t *testing.T) {
	window := newSequenceAckWindow()
	ack := sequenceAck{
		sequenceNumber: 1,
		messageId:      NewId(),
		tag:            sequenceTag{sendTime: 1_700_000_000_000, set: true},
	}

	allocs := testing.AllocsPerRun(1000, func() {
		window.Update(ack)
		if !window.Pending() {
			t.Fatal("updated ack window is not pending")
		}
		window.Snapshot(true)
	})
	if allocs != 0 {
		t.Fatalf("ack update + reset allocated %.0f times, want 0", allocs)
	}
}

func TestSequenceAckWindowPendingDoesNotCloneSelectiveAcks(t *testing.T) {
	window := newSequenceAckWindow()
	for i := range 64 {
		window.Update(sequenceAck{
			sequenceNumber: uint64(i + 2),
			messageId:      NewId(),
			selective:      true,
		})
	}

	allocs := testing.AllocsPerRun(1000, func() {
		if !window.Pending() {
			t.Fatal("selective ack window is not pending")
		}
	})
	if allocs != 0 {
		t.Fatalf("pending check allocated %.0f times, want 0", allocs)
	}
	if got := len(window.Snapshot(true).selectiveAcks); got != 64 {
		t.Fatalf("selective snapshot size = %d, want 64", got)
	}
	if window.Pending() {
		t.Fatal("reset ack window remained pending")
	}
}

func TestSequenceAckWindowDueDispositionIgnoresUnrelatedProgress(t *testing.T) {
	window := newSequenceAckWindow()
	targetMessageId := NewId()
	unrelatedMessageId := NewId()

	window.Update(sequenceAck{
		sequenceNumber: 6,
		messageId:      unrelatedMessageId,
		selective:      true,
	})
	if !window.Pending() {
		t.Fatal("unrelated selective ACK was not pending")
	}
	if window.PendingDispositionFor(5, targetMessageId) {
		t.Fatal("unrelated selective ACK postponed the due target")
	}
	if !window.PendingDispositionFor(6, unrelatedMessageId) {
		t.Fatal("exact selective ACK did not cover its due item")
	}
	window.Snapshot(true)

	window.Update(sequenceAck{sequenceNumber: 4, messageId: NewId()})
	if window.PendingDispositionFor(5, targetMessageId) {
		t.Fatal("lower cumulative ACK postponed a higher due item")
	}
	window.Update(sequenceAck{sequenceNumber: 5, messageId: targetMessageId})
	if !window.PendingDispositionFor(5, targetMessageId) {
		t.Fatal("covering cumulative ACK did not preempt the due item")
	}
	window.Snapshot(true)

	window.UpdateContractMissing(sequenceAck{
		messageId:         unrelatedMessageId,
		contractMissing:   true,
		missingContractId: NewId(),
	})
	if window.PendingDispositionFor(5, targetMessageId) {
		t.Fatal("unrelated missing-contract request postponed the due target")
	}
	window.UpdateContractMissing(sequenceAck{
		messageId:         targetMessageId,
		contractMissing:   true,
		missingContractId: NewId(),
	})
	if !window.PendingDispositionFor(5, targetMessageId) {
		t.Fatal("exact missing-contract request did not preempt the due item")
	}

	allocs := testing.AllocsPerRun(1_000, func() {
		if !window.PendingDispositionFor(5, targetMessageId) {
			t.Fatal("exact pending disposition disappeared")
		}
	})
	if allocs != 0 {
		t.Fatalf("exact pending-disposition check allocated %.0f times, want 0", allocs)
	}
}

func TestSequenceAckWindowPreservesCompactRecoveryCapability(t *testing.T) {
	window := newSequenceAckWindow()
	window.Update(sequenceAck{
		sequenceNumber:                   1,
		messageId:                        NewId(),
		compactContractRecoverySupported: true,
	})
	window.Update(sequenceAck{
		sequenceNumber: 2,
		messageId:      NewId(),
	})

	snapshot := window.Snapshot(true)
	if snapshot.ackUpdateCount != 2 ||
		!snapshot.headAck.compactContractRecoverySupported {
		t.Fatalf("coalesced capability snapshot=%+v", snapshot)
	}
}

func TestSequenceAckWindowTracksCarrierOfLatestAckTrigger(t *testing.T) {
	window := newSequenceAckWindow()
	window.Update(sequenceAck{
		sequenceNumber: 1,
		messageId:      NewId(),
		transportType:  TransportTypeH1,
	})
	window.Update(sequenceAck{
		sequenceNumber: 2,
		messageId:      NewId(),
		transportType:  TransportTypeH3,
	})
	if got := window.Snapshot(true).headAck.transportType; got != TransportTypeH3 {
		t.Fatalf("coalesced head carrier=%s, want H3", got)
	}

	// A retransmit below the cumulative head causes that head ACK to be sent
	// again, but cannot move an ACK covering newer H3 data onto the old H1 lane.
	window.Update(sequenceAck{
		sequenceNumber: 1,
		messageId:      NewId(),
		transportType:  TransportTypeH1,
	})
	if got := window.Snapshot(true).headAck.transportType; got != TransportTypeH3 {
		t.Fatalf("retransmit-triggered head carrier=%s, want preserved H3", got)
	}

	selectiveId := NewId()
	window.Update(sequenceAck{
		sequenceNumber: 3,
		messageId:      selectiveId,
		selective:      true,
		transportType:  TransportTypeH3,
	})
	window.Update(sequenceAck{
		sequenceNumber: 3,
		messageId:      selectiveId,
		selective:      true,
		transportType:  TransportTypeUnknown,
	})
	snapshot := window.Snapshot(true)
	if got := snapshot.selectiveAcks[selectiveId].transportType; got != TransportTypeH3 {
		t.Fatalf("selective ACK carrier=%s, want preserved H3", got)
	}
}
