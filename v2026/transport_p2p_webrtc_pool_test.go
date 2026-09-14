// Signal-dispatch ownership tests account for every pooled compatibility copy
// across rejection, delivery, panic recovery, and teardown.
package connect

import (
	"context"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Waits for exact-frame final-return signals and rejects any extra close.
func requireSignalFrameReturns(t *testing.T, closed <-chan bool, want int) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for index := 0; index < want; index += 1 {
		select {
		case returned := <-closed:
			if !returned {
				t.Fatalf("compatibility frame %d was not returned by its final owner", index)
			}
		case <-timer.C:
			t.Fatalf("received %d/%d compatibility-frame returns", index, want)
		}
	}
	select {
	case returned := <-closed:
		t.Fatalf("unexpected extra compatibility-frame close returned=%t", returned)
	default:
	}
}

// Records one borrowed compatibility frame and closes its shard before
// returning.
type testingCompatibilitySignalDelivery struct {
	closeReceiver func()
	delivered     chan testingCompatibilitySignalResult
}

// Preserves observations made while bytes are still borrowed by the callback.
type testingCompatibilitySignalResult struct {
	pooled       bool
	messageBytes []byte
}

// ReceiveSignal records compatibility bytes during their borrowed lifetime.
func (self *testingCompatibilitySignalDelivery) ReceiveSignal(
	_ TransferPath,
	_ TransferKey,
	frame *protocol.Frame,
) error {
	pooled, _ := MessagePoolCheck(frame.MessageBytes)
	self.delivered <- testingCompatibilitySignalResult{
		pooled:       pooled,
		messageBytes: append([]byte(nil), frame.MessageBytes...),
	}
	self.closeReceiver()
	return nil
}

// Exposes entry before panicking in the compatibility callback.
type testingPanickingSignalReceiver struct {
	entered chan struct{}
}

// ReceiveSignal deterministically aborts the worker after it owns one frame.
func (self *testingPanickingSignalReceiver) ReceiveSignal(
	TransferPath,
	TransferKey,
	*protocol.Frame,
) error {
	close(self.entered)
	panic("testing signal receiver panic")
}

// Creates a valid borrowed signal frame.
func newTestingCompatibilitySignalFrame(t *testing.T, streamId Id) *protocol.Frame {
	t.Helper()
	messageBytes, err := ProtoMarshal(&protocol.ExchangeSignals{
		StreamId: streamId.Bytes(),
		Signals: []*protocol.ExchangeSignal{{
			SignalType: protocol.SignalType_WaitingForSdpOffer,
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: messageBytes,
	}
}

// A full shard returns its pooled fallback copy immediately and exactly once.
func TestClientSignalDispatcherFullShardReturnsCompatibilityCopy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	frameClosed := make(chan bool, 1)
	shard := &clientSignalReceiver{
		client:             &Client{log: NewNoopLogger()},
		ctx:                ctx,
		cancel:             cancel,
		queueLimit:         1,
		queueMonitor:       NewMonitor(),
		dropWarnings:       make(chan signalDropWarning, 1),
		frameClosedForTest: frameClosed,
	}
	// Keep this exact rejection test independent of worker scheduling.
	shard.runOnce.Do(func() {})
	if !shard.enqueue(&receivedSignalFrame{frame: &protocol.Frame{}}) {
		t.Fatal("failed to fill signal shard")
	}
	dispatcher := &clientSignalDispatcher{
		client:   shard.client,
		receiver: &testingPanickingSignalReceiver{entered: make(chan struct{})},
		ctx:      ctx,
		cancel:   cancel,
		shards:   []*clientSignalReceiver{shard},
	}
	defer dispatcher.Close()

	borrowedBytes := MessagePoolCopy([]byte{0xff, 0xff})
	defer MessagePoolReturn(borrowedBytes)
	dispatcher.handleControlFrame(SourceId(NewId()), TransferKey{}, &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: borrowedBytes,
	})
	requireSignalFrameReturns(t, frameClosed, 1)
	if pooled, _ := MessagePoolCheck(borrowedBytes); !pooled {
		t.Fatal("dispatcher returned the callback's borrowed input")
	}
	if got := shard.droppedSignalCount.Load(); got != 1 {
		t.Fatalf("full-shard drop count=%d, want 1", got)
	}
}

// Teardown returns a queued pooled fallback copy exactly once.
func TestClientSignalReceiverCloseReturnsQueuedCompatibilityCopy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	frameClosed := make(chan bool, 1)
	shard := &clientSignalReceiver{
		client:             &Client{log: NewNoopLogger()},
		ctx:                ctx,
		cancel:             cancel,
		queueLimit:         1,
		queueMonitor:       NewMonitor(),
		frameClosedForTest: frameClosed,
	}
	defer shard.Close()
	borrowedBytes := MessagePoolCopy([]byte{0xff, 0xff})
	defer MessagePoolReturn(borrowedBytes)
	received, err := newReceivedSignalFrame(SourceId(NewId()), TransferKey{}, &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: borrowedBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !shard.enqueue(received) {
		t.Fatal("failed to enqueue compatibility copy")
	}
	select {
	case <-frameClosed:
		t.Fatal("queued compatibility copy was returned before Close")
	default:
	}
	shard.Close()
	requireSignalFrameReturns(t, frameClosed, 1)
}

// Lazy framing owns one pooled buffer through the callback and returns it
// immediately after delivery.
func TestClientSignalReceiverCompatibilityDeliveryBalancesPreparedBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	frameClosed := make(chan bool, 1)
	delivery := &testingCompatibilitySignalDelivery{
		delivered: make(chan testingCompatibilitySignalResult, 1),
	}
	shard := &clientSignalReceiver{
		client:             &Client{log: NewNoopLogger()},
		receiver:           delivery,
		ctx:                ctx,
		cancel:             cancel,
		queueLimit:         1,
		queueMonitor:       NewMonitor(),
		frameClosedForTest: frameClosed,
	}
	defer shard.Close()
	delivery.closeReceiver = shard.Close
	frame := newTestingCompatibilitySignalFrame(t, NewId())
	defer MessagePoolReturn(frame.MessageBytes)
	received, err := newReceivedSignalFrame(SourceId(NewId()), TransferKey{}, frame)
	if err != nil {
		t.Fatal(err)
	}
	if !shard.enqueue(received) {
		t.Fatal("failed to enqueue decoded signal")
	}
	shard.run()
	requireSignalFrameReturns(t, frameClosed, 1)
	result := <-delivery.delivered
	if !result.pooled {
		t.Fatal("compatibility delivery did not borrow pooled framed bytes")
	}
	decoded := &protocol.ExchangeSignals{}
	if err := ProtoUnmarshal(result.messageBytes, decoded); err != nil {
		t.Fatalf("compatibility delivery bytes are invalid: %v", err)
	}
	if len(decoded.Signals) != 1 || decoded.Signals[0].SignalType != protocol.SignalType_WaitingForSdpOffer {
		t.Fatal("compatibility delivery changed the decoded signal")
	}
}

// Panic cleanup returns the in-flight owner and Close returns its queued
// sibling, each exactly once.
func TestClientSignalReceiverPanicThenCloseReturnsAllCompatibilityCopies(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	frameClosed := make(chan bool, 2)
	receiver := &testingPanickingSignalReceiver{entered: make(chan struct{})}
	shard := &clientSignalReceiver{
		client:             &Client{log: NewNoopLogger()},
		receiver:           receiver,
		ctx:                ctx,
		cancel:             cancel,
		queueLimit:         2,
		queueMonitor:       NewMonitor(),
		frameClosedForTest: frameClosed,
	}
	defer shard.Close()
	borrowedBytes := MessagePoolCopy([]byte{0xff, 0xff})
	defer MessagePoolReturn(borrowedBytes)
	for range 2 {
		received, err := newReceivedSignalFrame(SourceId(NewId()), TransferKey{}, &protocol.Frame{
			MessageType:  protocol.MessageType_TransferExchangeSignals,
			MessageBytes: borrowedBytes,
		})
		if err != nil {
			t.Fatal(err)
		}
		if !shard.enqueue(received) {
			t.Fatal("failed to enqueue compatibility copy")
		}
	}
	shard.start()
	select {
	case <-receiver.entered:
	case <-time.After(time.Second):
		t.Fatal("panicking compatibility receiver was not entered")
	}
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("signal worker panic did not cancel its shard")
	}
	shard.Close()
	requireSignalFrameReturns(t, frameClosed, 2)
}
