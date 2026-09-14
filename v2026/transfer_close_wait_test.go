// This file deterministically verifies Client.CloseAndWait ownership joins and
// the admission boundary that separates caller-owned from client-owned work.
package connect

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// closeWaitPoolWitness holds one test owner and one witness reference to the
// same pooled message. Returning the witness reports whether the lifecycle
// under test already returned its owner.
type closeWaitPoolWitness struct {
	owner    []byte
	witness  []byte
	released bool
}

// newCloseWaitPoolWitness creates a pooled message in the 2 KiB class used by
// the construction leak that motivated Client.CloseAndWait.
func newCloseWaitPoolWitness(t *testing.T) *closeWaitPoolWitness {
	t.Helper()
	owner := MessagePoolGet(2 * 1024)
	if pooled, _ := MessagePoolCheck(owner); !pooled {
		MessagePoolReturn(owner)
		t.Fatal("close-wait test message did not use the message pool")
	}
	return &closeWaitPoolWitness{
		owner:   owner,
		witness: MessagePoolShareReadOnly(owner),
	}
}

// returnCallerOwner returns ownership after an admission rejection, where the
// Send or Forward API deliberately left the message with its caller.
func (self *closeWaitPoolWitness) returnCallerOwner() {
	MessagePoolReturn(self.owner)
}

// requireOwnerReleased verifies that only the witness reference remains.
func (self *closeWaitPoolWitness) requireOwnerReleased(t *testing.T, name string) {
	t.Helper()
	if MessagePoolReturn(self.witness) {
		self.released = true
		return
	}
	MessagePoolReturn(self.owner)
	self.released = true
	t.Fatalf("%s retained its message-pool owner after joined cleanup", name)
}

// cleanup releases both references after an earlier assertion failure without
// contaminating later pool-ownership tests in the same process.
func (self *closeWaitPoolWitness) cleanup() {
	if self.released {
		return
	}
	if !MessagePoolReturn(self.witness) {
		MessagePoolReturn(self.owner)
	}
	self.released = true
}

// closeWaitClientSettings disables encryption and diagnostics so lifecycle
// tests exercise only transfer ownership.
func closeWaitClientSettings() *ClientSettings {
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.ControlPingTimeout = 0
	settings.Log = NewNoopLogger()
	return settings
}

// waitCloseWaitBarrier waits only for liveness; the barrier itself establishes
// the ordering asserted by each regression.
func waitCloseWaitBarrier(
	t *testing.T,
	ctx context.Context,
	barrier <-chan struct{},
	name string,
) {
	t.Helper()
	select {
	case <-barrier:
	case <-ctx.Done():
		t.Fatalf("wait for %s: %v", name, ctx.Err())
	}
}

// requireCloseWaitBlocked proves the join cannot publish completion while a
// lifecycle worker is held before its final message-pool return.
func requireCloseWaitBlocked(t *testing.T, result <-chan error, name string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("%s returned before held cleanup: %v", name, err)
	default:
	}
}

// waitCloseWaitResult joins an asynchronous CloseAndWait call.
func waitCloseWaitResult(
	t *testing.T,
	ctx context.Context,
	result <-chan error,
	name string,
) {
	t.Helper()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	case <-ctx.Done():
		t.Fatalf("%s: %v", name, ctx.Err())
	}
}

// closeWaitReceivePack creates the smallest valid receive Pack whose outer
// transfer bytes remain independently observable through a pool witness.
func closeWaitReceivePack(
	ctx context.Context,
	source TransferPath,
	sequenceId Id,
	transferFrameBytes []byte,
) *ReceivePack {
	return &ReceivePack{
		Source:     source,
		SequenceId: sequenceId,
		Pack: &protocol.Pack{
			MessageId:      NewId().Bytes(),
			SequenceId:     sequenceId.Bytes(),
			SequenceNumber: 0,
			Head:           true,
			Nack:           true,
		},
		ReceiveCallback:    func(TransferPath, []*protocol.Frame, Peer) {},
		TransferFrameBytes: transferFrameBytes,
		Ctx:                ctx,
		Unwrapped:          true,
		EncryptionRole:     sequenceTlsRoleServer,
	}
}

// TestClientCloseAndWaitJoinsHeldSendSequence proves that an admitted SendPack
// is returned before the client publishes joined completion.
func TestClientCloseAndWaitJoinsHeldSendSequence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	destinationId := NewId()
	sequenceEntered := make(chan struct{})
	releaseSequence := make(chan struct{})
	waitEntered := make(chan struct{})
	var sequenceEnteredOnce sync.Once
	var releaseSequenceOnce sync.Once
	var waitEnteredOnce sync.Once
	settings := closeWaitClientSettings()
	settings.SendBufferSettings.beforeRunSendSequenceForTest = func(id sendSequenceId) {
		if id.Destination != destinationId {
			return
		}
		sequenceEnteredOnce.Do(func() { close(sequenceEntered) })
		<-releaseSequence
	}
	settings.SendBufferSettings.beforeCloseWaitForTest = func(id sendSequenceId) {
		if id.Destination == destinationId {
			waitEnteredOnce.Do(func() { close(waitEntered) })
		}
	}

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	message := newCloseWaitPoolWitness(t)
	clientJoined := false
	defer func() {
		releaseSequenceOnce.Do(func() { close(releaseSequence) })
		if !clientJoined {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_ = client.CloseAndWait(cleanupCtx)
		}
		message.cleanup()
	}()

	success, err := client.sendBuffer.Pack(&SendPack{
		TransferOptions: TransferOptions{Ack: true},
		Frame: &protocol.Frame{
			MessageType:  protocol.MessageType_TransferPack,
			MessageBytes: message.owner,
		},
		Destination:      destinationId,
		AckCallback:      func(error) {},
		MessageByteCount: ByteCount(len(message.owner)),
		Ctx:              ctx,
	}, 0)
	if err != nil || !success {
		t.Fatalf("admit held send Pack = (%t, %v)", success, err)
	}
	waitCloseWaitBarrier(t, ctx, sequenceEntered, "send sequence startup")

	result := make(chan error, 1)
	go func() { result <- client.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, waitEntered, "send sequence close join")
	requireCloseWaitBlocked(t, result, "Client.CloseAndWait send join")
	releaseSequenceOnce.Do(func() { close(releaseSequence) })
	waitCloseWaitResult(t, ctx, result, "join held send sequence")
	clientJoined = true

	client.sendBuffer.mutex.Lock()
	activeCount := len(client.sendBuffer.activeSendSequences)
	client.sendBuffer.mutex.Unlock()
	if activeCount != 0 {
		t.Fatalf("active send sequences after join = %d, want 0", activeCount)
	}
	message.requireOwnerReleased(t, "held send sequence")
}

// TestClientCloseAndWaitJoinsHeldReceiveSequence proves ReceiveSequence.Close
// drains its queued Pack before joined completion.
func TestClientCloseAndWaitJoinsHeldReceiveSequence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	source := SourceId(NewId())
	sequenceId := NewId()
	sequenceEntered := make(chan struct{})
	releaseSequence := make(chan struct{})
	waitEntered := make(chan struct{})
	var sequenceEnteredOnce sync.Once
	var releaseSequenceOnce sync.Once
	var waitEnteredOnce sync.Once
	settings := closeWaitClientSettings()
	settings.ReceiveBufferSettings.beforeRunReceiveSequenceForTest = func(id receiveSequenceId) {
		if id.Source != source || id.SequenceId != sequenceId {
			return
		}
		sequenceEnteredOnce.Do(func() { close(sequenceEntered) })
		<-releaseSequence
	}
	settings.ReceiveBufferSettings.beforeCloseWaitForTest = func(id receiveSequenceId) {
		if id.Source == source && id.SequenceId == sequenceId {
			waitEnteredOnce.Do(func() { close(waitEntered) })
		}
	}

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	client.ContractManager().AddNoContractPeer(source.SourceId)
	message := newCloseWaitPoolWitness(t)
	clientJoined := false
	defer func() {
		releaseSequenceOnce.Do(func() { close(releaseSequence) })
		if !clientJoined {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_ = client.CloseAndWait(cleanupCtx)
		}
		message.cleanup()
	}()

	success, err := client.receiveBuffer.Pack(
		closeWaitReceivePack(ctx, source, sequenceId, message.owner),
		0,
	)
	if err != nil || !success {
		t.Fatalf("admit held receive Pack = (%t, %v)", success, err)
	}
	waitCloseWaitBarrier(t, ctx, sequenceEntered, "receive sequence startup")

	result := make(chan error, 1)
	go func() { result <- client.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, waitEntered, "receive sequence close join")
	requireCloseWaitBlocked(t, result, "Client.CloseAndWait receive join")
	releaseSequenceOnce.Do(func() { close(releaseSequence) })
	waitCloseWaitResult(t, ctx, result, "join held receive sequence")
	clientJoined = true

	client.receiveBuffer.mutex.Lock()
	activeCount := len(client.receiveBuffer.activeReceiveSequences)
	client.receiveBuffer.mutex.Unlock()
	if activeCount != 0 {
		t.Fatalf("active receive sequences after join = %d, want 0", activeCount)
	}
	message.requireOwnerReleased(t, "held receive sequence")
}

// TestClientCloseAndWaitJoinsHeldForwardSequence proves a queued forwarded
// TransferFrame is returned before joined completion.
func TestClientCloseAndWaitJoinsHeldForwardSequence(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	destination := DestinationId(NewId())
	sequenceEntered := make(chan struct{})
	releaseSequence := make(chan struct{})
	waitEntered := make(chan struct{})
	var sequenceEnteredOnce sync.Once
	var releaseSequenceOnce sync.Once
	var waitEnteredOnce sync.Once
	settings := closeWaitClientSettings()
	settings.ForwardBufferSettings.beforeRunForwardSequenceForTest = func(path TransferPath) {
		if path != destination {
			return
		}
		sequenceEnteredOnce.Do(func() { close(sequenceEntered) })
		<-releaseSequence
	}
	settings.ForwardBufferSettings.beforeCloseWaitForTest = func(path TransferPath) {
		if path == destination {
			waitEnteredOnce.Do(func() { close(waitEntered) })
		}
	}

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	message := newCloseWaitPoolWitness(t)
	clientJoined := false
	defer func() {
		releaseSequenceOnce.Do(func() { close(releaseSequence) })
		if !clientJoined {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_ = client.CloseAndWait(cleanupCtx)
		}
		message.cleanup()
	}()

	success, err := client.forwardBuffer.Pack(&ForwardPack{
		Destination:        destination,
		TransferFrameBytes: message.owner,
		Ctx:                ctx,
	}, 0)
	if err != nil || !success {
		t.Fatalf("admit held forward Pack = (%t, %v)", success, err)
	}
	waitCloseWaitBarrier(t, ctx, sequenceEntered, "forward sequence startup")

	result := make(chan error, 1)
	go func() { result <- client.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, waitEntered, "forward sequence close join")
	requireCloseWaitBlocked(t, result, "Client.CloseAndWait forward join")
	releaseSequenceOnce.Do(func() { close(releaseSequence) })
	waitCloseWaitResult(t, ctx, result, "join held forward sequence")
	clientJoined = true

	client.forwardBuffer.mutex.Lock()
	activeCount := len(client.forwardBuffer.activeForwardSequences)
	client.forwardBuffer.mutex.Unlock()
	if activeCount != 0 {
		t.Fatalf("active forward sequences after join = %d, want 0", activeCount)
	}
	message.requireOwnerReleased(t, "held forward sequence")
}

// TestClientCloseAndWaitJoinsHeldLoopbackRelease proves the reader tree join
// includes the loopback child and its final SendPack ownership return.
func TestClientCloseAndWaitJoinsHeldLoopbackRelease(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	releaseEntered := make(chan struct{})
	releaseLoopback := make(chan struct{})
	waitEntered := make(chan struct{})
	var releaseEnteredOnce sync.Once
	var releaseLoopbackOnce sync.Once
	var waitEnteredOnce sync.Once
	settings := closeWaitClientSettings()
	settings.beforeLoopbackReleaseForTest = func() {
		releaseEnteredOnce.Do(func() { close(releaseEntered) })
		<-releaseLoopback
	}
	settings.beforeRunDoneWaitForTest = func() {
		waitEnteredOnce.Do(func() { close(waitEntered) })
	}

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	message := newCloseWaitPoolWitness(t)
	clientJoined := false
	defer func() {
		releaseLoopbackOnce.Do(func() { close(releaseLoopback) })
		if !clientJoined {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_ = client.CloseAndWait(cleanupCtx)
		}
		message.cleanup()
	}()

	success := client.SendWithTimeout(
		&protocol.Frame{
			MessageType:  protocol.MessageType_TransferPack,
			MessageBytes: message.owner,
		},
		client.ClientId(),
		func(error) {},
		time.Second,
	)
	if !success {
		t.Fatal("loopback Pack was not admitted")
	}
	waitCloseWaitBarrier(t, ctx, releaseEntered, "loopback release")

	result := make(chan error, 1)
	go func() { result <- client.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, waitEntered, "client reader close join")
	requireCloseWaitBlocked(t, result, "Client.CloseAndWait loopback join")
	releaseLoopbackOnce.Do(func() { close(releaseLoopback) })
	waitCloseWaitResult(t, ctx, result, "join held loopback release")
	clientJoined = true
	message.requireOwnerReleased(t, "held loopback Pack")
}

// TestTransferBufferCloseRejectsPausedSequenceAdmissions fixes the ownership
// boundary at each creation lock. Close wins while callers are paused before
// admission; resuming them cannot create a post-close worker.
func TestTransferBufferCloseRejectsPausedSequenceAdmissions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sendDestinationId := NewId()
	receiveSource := SourceId(NewId())
	receiveSequenceValue := NewId()
	forwardDestination := DestinationId(NewId())
	sendCreateEntered := make(chan struct{})
	receiveCreateEntered := make(chan struct{})
	forwardCreateEntered := make(chan struct{})
	releaseSendCreate := make(chan struct{})
	releaseReceiveCreate := make(chan struct{})
	releaseForwardCreate := make(chan struct{})
	var sendCreateEnteredOnce sync.Once
	var receiveCreateEnteredOnce sync.Once
	var forwardCreateEnteredOnce sync.Once
	settings := closeWaitClientSettings()
	settings.SendBufferSettings.beforeCreateSendSequenceForTest = func(id sendSequenceId) {
		if id.Destination != sendDestinationId {
			return
		}
		sendCreateEnteredOnce.Do(func() { close(sendCreateEntered) })
		<-releaseSendCreate
	}
	settings.ReceiveBufferSettings.beforeCreateReceiveSequenceForTest = func(id receiveSequenceId) {
		if id.Source != receiveSource || id.SequenceId != receiveSequenceValue {
			return
		}
		receiveCreateEnteredOnce.Do(func() { close(receiveCreateEntered) })
		<-releaseReceiveCreate
	}
	settings.ForwardBufferSettings.beforeCreateForwardSequenceForTest = func(path TransferPath) {
		if path != forwardDestination {
			return
		}
		forwardCreateEnteredOnce.Do(func() { close(forwardCreateEntered) })
		<-releaseForwardCreate
	}

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	sendMessage := newCloseWaitPoolWitness(t)
	receiveMessage := newCloseWaitPoolWitness(t)
	forwardMessage := newCloseWaitPoolWitness(t)
	clientJoined := false
	defer func() {
		select {
		case <-releaseSendCreate:
		default:
			close(releaseSendCreate)
		}
		select {
		case <-releaseReceiveCreate:
		default:
			close(releaseReceiveCreate)
		}
		select {
		case <-releaseForwardCreate:
		default:
			close(releaseForwardCreate)
		}
		if !clientJoined {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_ = client.CloseAndWait(cleanupCtx)
		}
		sendMessage.cleanup()
		receiveMessage.cleanup()
		forwardMessage.cleanup()
	}()

	type packResult struct {
		success bool
		err     error
	}
	sendResult := make(chan packResult, 1)
	go func() {
		success, err := client.sendBuffer.Pack(&SendPack{
			TransferOptions: TransferOptions{Ack: true},
			Frame: &protocol.Frame{
				MessageType:  protocol.MessageType_TransferPack,
				MessageBytes: sendMessage.owner,
			},
			Destination:      sendDestinationId,
			AckCallback:      func(error) {},
			MessageByteCount: ByteCount(len(sendMessage.owner)),
			Ctx:              ctx,
		}, 0)
		sendResult <- packResult{success: success, err: err}
	}()
	waitCloseWaitBarrier(t, ctx, sendCreateEntered, "paused send admission")
	client.sendBuffer.Close()
	close(releaseSendCreate)
	send := <-sendResult
	if send.success || send.err == nil {
		t.Fatalf("post-close send admission = (%t, %v), want rejection", send.success, send.err)
	}
	sendMessage.returnCallerOwner()
	sendMessage.requireOwnerReleased(t, "rejected send admission")

	receiveResult := make(chan packResult, 1)
	go func() {
		success, err := client.receiveBuffer.Pack(
			closeWaitReceivePack(
				ctx,
				receiveSource,
				receiveSequenceValue,
				receiveMessage.owner,
			),
			0,
		)
		receiveResult <- packResult{success: success, err: err}
	}()
	waitCloseWaitBarrier(t, ctx, receiveCreateEntered, "paused receive admission")
	client.receiveBuffer.Close()
	close(releaseReceiveCreate)
	receive := <-receiveResult
	if !receive.success || receive.err != nil {
		t.Fatalf("post-close receive drop = (%t, %v), want owned silent drop", receive.success, receive.err)
	}
	receiveMessage.requireOwnerReleased(t, "rejected receive admission")

	forwardResult := make(chan packResult, 1)
	go func() {
		success, err := client.forwardBuffer.Pack(&ForwardPack{
			Destination:        forwardDestination,
			TransferFrameBytes: forwardMessage.owner,
			Ctx:                ctx,
		}, 0)
		forwardResult <- packResult{success: success, err: err}
	}()
	waitCloseWaitBarrier(t, ctx, forwardCreateEntered, "paused forward admission")
	client.forwardBuffer.Close()
	close(releaseForwardCreate)
	forward := <-forwardResult
	if forward.success || forward.err == nil {
		t.Fatalf("post-close forward admission = (%t, %v), want rejection", forward.success, forward.err)
	}
	forwardMessage.returnCallerOwner()
	forwardMessage.requireOwnerReleased(t, "rejected forward admission")

	client.sendBuffer.mutex.Lock()
	activeSendCount := 0
	for sendSequence := range client.sendBuffer.activeSendSequences {
		if sendSequence.destination == sendDestinationId {
			activeSendCount += 1
		}
	}
	client.sendBuffer.mutex.Unlock()
	client.receiveBuffer.mutex.Lock()
	activeReceiveCount := 0
	for sequence := range client.receiveBuffer.activeReceiveSequences {
		if sequence.source == receiveSource &&
			sequence.sequenceId == receiveSequenceValue {
			activeReceiveCount += 1
		}
	}
	client.receiveBuffer.mutex.Unlock()
	client.forwardBuffer.mutex.Lock()
	activeForwardCount := 0
	for forwardSequence := range client.forwardBuffer.activeForwardSequences {
		if forwardSequence.destination == forwardDestination {
			activeForwardCount += 1
		}
	}
	client.forwardBuffer.mutex.Unlock()
	if activeSendCount != 0 || activeReceiveCount != 0 || activeForwardCount != 0 {
		t.Fatalf(
			"post-close active sequences = send:%d receive:%d forward:%d, want all zero",
			activeSendCount,
			activeReceiveCount,
			activeForwardCount,
		)
	}

	if err := client.CloseAndWait(ctx); err != nil {
		t.Fatalf("join client after admission rejections: %v", err)
	}
	clientJoined = true
}

// TestClientCloseAndWaitPrefersCompletedLifecycleOverCanceledWaitContext pins
// deterministic completion precedence when both channels are already closed.
func TestClientCloseAndWaitPrefersCompletedLifecycleOverCanceledWaitContext(t *testing.T) {
	client := NewClient(
		context.Background(),
		NewId(),
		NewNoContractClientOob(),
		closeWaitClientSettings(),
	)
	closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := client.CloseAndWait(closeCtx); err != nil {
		closeCancel()
		t.Fatalf("initial client join: %v", err)
	}
	closeCancel()

	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := client.CloseAndWait(canceledCtx); err != nil {
		t.Fatalf("completed client returned canceled wait error: %v", err)
	}

	done := make(chan struct{})
	close(done)
	for range 1000 {
		if err := waitForLifecycleDone(canceledCtx, done, "completed test worker"); err != nil {
			t.Fatalf("completed lifecycle lost done precedence: %v", err)
		}
	}
	if !errors.Is(canceledCtx.Err(), context.Canceled) {
		t.Fatalf("test context error = %v, want context.Canceled", canceledCtx.Err())
	}
}
