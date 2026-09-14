// This file verifies ownership of the one Pack the send coalescer may remove
// from its channel and retain locally while an earlier Ack waits for capacity.
package connect

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// pendingSendPackCancellationHarness holds both scheduling boundaries needed
// to reproduce cancellation with an incompatible Pack owned by Run's local.
type pendingSendPackCancellationHarness struct {
	client                *Client
	destinationId         Id
	route                 chan []byte
	sequenceEntered       chan struct{}
	releaseSequence       chan struct{}
	resendCapacityEntered chan struct{}
	releaseResendCapacity chan struct{}
	sequenceDone          chan struct{}
	releaseSequenceOnce   sync.Once
	releaseCapacityOnce   sync.Once
}

// newPendingSendPackCancellationHarness makes one plaintext no-contract lane.
// Its first Ack item always fills the configured resend queue.
func newPendingSendPackCancellationHarness(
	t *testing.T,
	ctx context.Context,
	destinationId Id,
	noAckObserver func(NoAckSendObservation),
	lifecycleObserver func(SendPackLifecycleObservation),
) *pendingSendPackCancellationHarness {
	t.Helper()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.NoAckSendObserver = noAckObserver
	settings.SendBufferSettings.SendPackLifecycleObserver = lifecycleObserver
	settings.SendBufferSettings.ResendQueueMaxByteCount = 1
	settings.SendBufferSettings.AckTimeout = time.Minute
	settings.SendBufferSettings.IdleTimeout = time.Minute
	settings.SendBufferSettings.MinResendInterval = time.Minute
	settings.SendBufferSettings.RttMinResendInterval = time.Minute
	settings.SendBufferSettings.MaxResendInterval = time.Minute

	harness := &pendingSendPackCancellationHarness{
		destinationId:         destinationId,
		route:                 make(chan []byte, 1),
		sequenceEntered:       make(chan struct{}),
		releaseSequence:       make(chan struct{}),
		resendCapacityEntered: make(chan struct{}),
		releaseResendCapacity: make(chan struct{}),
		sequenceDone:          make(chan struct{}),
	}
	var sequenceEnteredOnce sync.Once
	var resendCapacityEnteredOnce sync.Once
	var sequenceDoneOnce sync.Once
	settings.SendBufferSettings.beforeRunSendSequenceForTest = func(id sendSequenceId) {
		if id.Destination != destinationId {
			return
		}
		sequenceEnteredOnce.Do(func() { close(harness.sequenceEntered) })
		<-harness.releaseSequence
	}
	settings.SendBufferSettings.beforeResendCapacityWaitForTest = func(id sendSequenceId) {
		if id.Destination != destinationId {
			return
		}
		resendCapacityEnteredOnce.Do(func() { close(harness.resendCapacityEntered) })
		<-harness.releaseResendCapacity
	}
	settings.SendBufferSettings.afterRunSendSequenceForTest = func(id sendSequenceId) {
		if id.Destination != destinationId {
			return
		}
		sequenceDoneOnce.Do(func() { close(harness.sequenceDone) })
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	harness.client = client
	client.ContractManager().AddNoContractPeer(destinationId)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{harness.route},
	)
	return harness
}

// pendingSendPackLifecyclePhases collects an exact joined phase set and
// rejects duplicate phases for the same original Pack token.
func pendingSendPackLifecyclePhases(
	t *testing.T,
	ctx context.Context,
	events <-chan SendPackLifecycleObservation,
	observationCount int,
) map[uint64]map[SendPackLifecyclePhase]SendPackLifecycleObservation {
	t.Helper()
	phases := map[uint64]map[SendPackLifecyclePhase]SendPackLifecycleObservation{}
	for range observationCount {
		observation := waitSendPackLifecycleObservation(t, ctx, events)
		if phases[observation.Token] == nil {
			phases[observation.Token] = map[SendPackLifecyclePhase]SendPackLifecycleObservation{}
		}
		if _, duplicate := phases[observation.Token][observation.Phase]; duplicate {
			t.Fatalf("duplicate pending lifecycle observation=%+v", observation)
		}
		phases[observation.Token][observation.Phase] = observation
	}
	return phases
}

// releaseStartup lets Run consume the two packs already admitted to its lane.
func (self *pendingSendPackCancellationHarness) releaseStartup() {
	self.releaseSequenceOnce.Do(func() { close(self.releaseSequence) })
}

// releaseCapacity lets the canceled sequence observe its canceled context.
func (self *pendingSendPackCancellationHarness) releaseCapacity() {
	self.releaseCapacityOnce.Do(func() { close(self.releaseResendCapacity) })
}

// cancelAtCapacity cancels only after Run owns the incompatible pending Pack,
// then joins the sequence lifecycle before assertions inspect callback counts.
func (self *pendingSendPackCancellationHarness) cancelAtCapacity(
	t *testing.T,
	ctx context.Context,
) {
	t.Helper()
	self.releaseStartup()
	waitPendingSendPackBarrier(t, ctx, self.resendCapacityEntered, "resend capacity")
	self.client.Cancel()
	self.releaseCapacity()
	waitPendingSendPackBarrier(t, ctx, self.sequenceDone, "send sequence completion")
}

// close releases every barrier, joins when possible, and returns route bytes.
func (self *pendingSendPackCancellationHarness) close(t *testing.T, ctx context.Context) {
	t.Helper()
	self.client.Cancel()
	self.releaseStartup()
	self.releaseCapacity()
	select {
	case <-self.sequenceDone:
	case <-ctx.Done():
		t.Errorf("cleanup send sequence: %v", ctx.Err())
	}
	for {
		select {
		case transferFrameBytes := <-self.route:
			MessagePoolReturn(transferFrameBytes)
		default:
			return
		}
	}
}

// waitPendingSendPackBarrier waits only for liveness; channels establish all
// ordering required by the regression.
func waitPendingSendPackBarrier(
	t *testing.T,
	ctx context.Context,
	barrier <-chan struct{},
	name string,
) {
	t.Helper()
	select {
	case <-barrier:
	case <-ctx.Done():
		t.Fatalf("wait for %s barrier: %v", name, ctx.Err())
	}
}

// pendingSendPackTestMessage creates a pooled buffer plus one witness reference
// that remains owned by the test while the successful send owns the original.
func pendingSendPackTestMessage(t *testing.T, byteCount int) ([]byte, []byte) {
	t.Helper()
	messageBytes := MessagePoolGet(byteCount)
	pooled, _ := MessagePoolCheck(messageBytes)
	if !pooled {
		MessagePoolReturn(messageBytes)
		t.Fatalf("test message byte count %d did not use the message pool", byteCount)
	}
	return messageBytes, MessagePoolShareReadOnly(messageBytes)
}

// releasePendingSendPackWitness verifies that the sequence already returned
// its owner. A false result means the removed pending Pack leaked that owner.
func releasePendingSendPackWitness(t *testing.T, messageBytes []byte, witness []byte) {
	t.Helper()
	if MessagePoolReturn(witness) {
		return
	}
	// Release the leaked owner so a failing regression does not contaminate
	// later pool-balance tests in the same process.
	MessagePoolReturn(messageBytes)
	t.Fatal("pending Pack retained its message-pool owner after sequence completion")
}

// requirePendingSendPackError requires exactly one terminal error after the
// joined sequence proves that no callback producer remains.
func requirePendingSendPackError(t *testing.T, results <-chan error, name string) {
	t.Helper()
	if resultCount := len(results); resultCount != 1 {
		t.Fatalf("%s completion count=%d, want 1", name, resultCount)
	}
	if err := <-results; err == nil {
		t.Fatalf("%s completion had no cancellation error", name)
	}
}

// A mismatched Ack policy makes the coalescer retain the second Pack locally.
// Cancellation at resend capacity must complete both of its independent
// dispositions and return its source buffer exactly once.
func TestSendSequenceCancellationDisposesPendingNoAckPack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observer, events := noAckObserverTestEvents()
	destinationId := NewId()
	lifecycleObserver, lifecycleEvents := sendPackLifecycleTestObserver(destinationId)
	harness := newPendingSendPackCancellationHarness(
		t,
		ctx,
		destinationId,
		observer,
		lifecycleObserver,
	)
	defer harness.close(t, ctx)

	firstResults := make(chan error, 1)
	firstFrame := RequireToFrameWithDefaultProtocolVersion(
		&protocol.SimpleMessage{Content: "first Ack pack"},
	)
	if !harness.client.SendWithTimeout(
		firstFrame,
		harness.destinationId,
		func(err error) { firstResults <- err },
		time.Second,
	) {
		MessagePoolReturn(firstFrame.MessageBytes)
		t.Fatal("first Ack pack was not admitted")
	}
	firstLifecycleStarted := waitSendPackLifecycleObservation(t, ctx, lifecycleEvents)
	waitPendingSendPackBarrier(t, ctx, harness.sequenceEntered, "send sequence startup")

	pendingResults := make(chan error, 1)
	pendingMessageBytes, pendingWitness := pendingSendPackTestMessage(t, 64)
	pendingFrame := &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: pendingMessageBytes,
	}
	if !harness.client.SendWithTimeout(
		pendingFrame,
		harness.destinationId,
		func(err error) { pendingResults <- err },
		time.Second,
		NoAck(),
	) {
		MessagePoolReturn(pendingMessageBytes)
		MessagePoolReturn(pendingWitness)
		t.Fatal("pending NoAck pack was not admitted")
	}
	started := waitNoAckObservation(t, ctx, events)
	pendingLifecycleStarted := waitSendPackLifecycleObservation(t, ctx, lifecycleEvents)
	if started.Phase != NoAckSendPhaseStarted ||
		started.ClientId != harness.client.ClientId() ||
		started.DestinationId != harness.destinationId {
		t.Fatalf("pending NoAck start=%+v", started)
	}

	harness.cancelAtCapacity(t, ctx)
	requirePendingSendPackError(t, firstResults, "first Ack pack")
	requirePendingSendPackError(t, pendingResults, "pending NoAck pack")
	if completionCount := len(events); completionCount != 1 {
		t.Fatalf("pending NoAck completion count=%d, want 1", completionCount)
	}
	completed := <-events
	if completed.Phase != NoAckSendPhaseCompleted ||
		completed.Token != started.Token || completed.Err == nil {
		t.Fatalf("pending NoAck completion=%+v start=%+v", completed, started)
	}
	phases := pendingSendPackLifecyclePhases(t, ctx, lifecycleEvents, 4)
	requireSendPackLifecycleObservation(
		t, firstLifecycleStarted, harness.client.ClientId(), harness.destinationId,
		firstLifecycleStarted.Token, SendPackLifecyclePhaseStarted, true, false,
	)
	requireSendPackLifecycleObservation(
		t, phases[firstLifecycleStarted.Token][SendPackLifecyclePhaseFirstRouteWrite],
		harness.client.ClientId(), harness.destinationId, firstLifecycleStarted.Token,
		SendPackLifecyclePhaseFirstRouteWrite, true, false,
	)
	requireSendPackLifecycleObservation(
		t, phases[firstLifecycleStarted.Token][SendPackLifecyclePhaseTerminal],
		harness.client.ClientId(), harness.destinationId, firstLifecycleStarted.Token,
		SendPackLifecyclePhaseTerminal, true, true,
	)
	requireSendPackLifecycleObservation(
		t, pendingLifecycleStarted, harness.client.ClientId(), harness.destinationId,
		pendingLifecycleStarted.Token, SendPackLifecyclePhaseStarted, false, false,
	)
	requireSendPackLifecycleObservation(
		t, phases[pendingLifecycleStarted.Token][SendPackLifecyclePhaseFirstRouteWrite],
		harness.client.ClientId(), harness.destinationId, pendingLifecycleStarted.Token,
		SendPackLifecyclePhaseFirstRouteWrite, false, true,
	)
	requireSendPackLifecycleObservation(
		t, phases[pendingLifecycleStarted.Token][SendPackLifecyclePhaseTerminal],
		harness.client.ClientId(), harness.destinationId, pendingLifecycleStarted.Token,
		SendPackLifecyclePhaseTerminal, false, true,
	)
	requireNoSendPackLifecycleObservations(t, lifecycleEvents, "pending NoAck Pack")
	releasePendingSendPackWitness(t, pendingMessageBytes, pendingWitness)
}

// pendingSendPackAckResult records the value and terminal error delivered to a
// raw Ack target without introducing a closure into the production send path.
type pendingSendPackAckResult struct {
	value ByteCount
	err   error
}

// pendingSendPackAckTarget records exactly one raw-send disposition.
type pendingSendPackAckTarget struct {
	results chan pendingSendPackAckResult
}

// sendAckResult implements sendAckTarget for the cancellation regression.
func (self *pendingSendPackAckTarget) sendAckResult(value ByteCount, err error) {
	self.results <- pendingSendPackAckResult{value: value, err: err}
}

// Two Ack packs larger than the coalescing byte cap are incompatible. The raw
// second Pack must return its envelope, callback target, and source bytes when
// cancellation occurs while the first Pack occupies resend capacity.
func TestSendSequenceCancellationDisposesPendingRawAckPack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	destinationId := NewId()
	lifecycleObserver, lifecycleEvents := sendPackLifecycleTestObserver(destinationId)
	harness := newPendingSendPackCancellationHarness(
		t,
		ctx,
		destinationId,
		nil,
		lifecycleObserver,
	)
	defer harness.close(t, ctx)

	firstResults := make(chan error, 1)
	firstMessageBytes := MessagePoolGet(2 * 1024)
	firstFrame := &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: firstMessageBytes,
	}
	if !harness.client.SendWithTimeout(
		firstFrame,
		harness.destinationId,
		func(err error) { firstResults <- err },
		time.Second,
	) {
		MessagePoolReturn(firstMessageBytes)
		t.Fatal("first oversized Ack pack was not admitted")
	}
	firstLifecycleStarted := waitSendPackLifecycleObservation(t, ctx, lifecycleEvents)
	waitPendingSendPackBarrier(t, ctx, harness.sequenceEntered, "send sequence startup")

	target := &pendingSendPackAckTarget{
		results: make(chan pendingSendPackAckResult, 1),
	}
	pendingMessageBytes, pendingWitness := pendingSendPackTestMessage(t, 2*1024)
	const pendingAckValue = ByteCount(41)
	success, err := harness.client.sendRawWithTimeoutDetailed(
		protocol.MessageType_IpIpPacketFromProvider,
		pendingMessageBytes,
		harness.destinationId,
		target,
		pendingAckValue,
		time.Second,
	)
	if !success || err != nil {
		MessagePoolReturn(pendingMessageBytes)
		MessagePoolReturn(pendingWitness)
		t.Fatalf("pending raw Ack pack admission success=%t err=%v", success, err)
	}
	pendingLifecycleStarted := waitSendPackLifecycleObservation(t, ctx, lifecycleEvents)

	harness.cancelAtCapacity(t, ctx)
	requirePendingSendPackError(t, firstResults, "first oversized Ack pack")
	if resultCount := len(target.results); resultCount != 1 {
		t.Fatalf("pending raw Ack target completion count=%d, want 1", resultCount)
	}
	result := <-target.results
	if result.value != pendingAckValue || result.err == nil {
		t.Fatalf("pending raw Ack result=%+v, want value=%d and cancellation error", result, pendingAckValue)
	}
	if envelopeCount := len(harness.client.rawSendPacks); envelopeCount != 1 {
		t.Fatalf("returned raw Pack envelope count=%d, want 1", envelopeCount)
	}
	phases := pendingSendPackLifecyclePhases(t, ctx, lifecycleEvents, 4)
	requireSendPackLifecycleObservation(
		t, firstLifecycleStarted, harness.client.ClientId(), harness.destinationId,
		firstLifecycleStarted.Token, SendPackLifecyclePhaseStarted, true, false,
	)
	requireSendPackLifecycleObservation(
		t, phases[firstLifecycleStarted.Token][SendPackLifecyclePhaseFirstRouteWrite],
		harness.client.ClientId(), harness.destinationId, firstLifecycleStarted.Token,
		SendPackLifecyclePhaseFirstRouteWrite, true, false,
	)
	requireSendPackLifecycleObservation(
		t, phases[firstLifecycleStarted.Token][SendPackLifecyclePhaseTerminal],
		harness.client.ClientId(), harness.destinationId, firstLifecycleStarted.Token,
		SendPackLifecyclePhaseTerminal, true, true,
	)
	requireSendPackLifecycleObservation(
		t, pendingLifecycleStarted, harness.client.ClientId(), harness.destinationId,
		pendingLifecycleStarted.Token, SendPackLifecyclePhaseStarted, true, false,
	)
	requireSendPackLifecycleObservation(
		t, phases[pendingLifecycleStarted.Token][SendPackLifecyclePhaseFirstRouteWrite],
		harness.client.ClientId(), harness.destinationId, pendingLifecycleStarted.Token,
		SendPackLifecyclePhaseFirstRouteWrite, true, true,
	)
	requireSendPackLifecycleObservation(
		t, phases[pendingLifecycleStarted.Token][SendPackLifecyclePhaseTerminal],
		harness.client.ClientId(), harness.destinationId, pendingLifecycleStarted.Token,
		SendPackLifecyclePhaseTerminal, true, true,
	)
	requireNoSendPackLifecycleObservations(t, lifecycleEvents, "pending raw Ack Pack")
	releasePendingSendPackWitness(t, pendingMessageBytes, pendingWitness)
}
