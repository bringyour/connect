// Receive-sequence ACK cancellation tests pin the worker and route-writer
// ownership boundaries that teardown must drain before publishing exit.
package connect

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Cancel may race the ACK-compression window, but sequence shutdown must not
// publish exit until every already-delivered reliable item has emitted its
// final cumulative ACK. The barriers pin cleanup before its explicit worker
// stop, making a worker that exits directly on the sequence context lose the
// ACK deterministically.
func TestReceiveSequenceCancelDrainsFinalAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientSettings := DefaultClientSettings()
	clientSettings.Log = NewNoopLogger()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), clientSettings)
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := client.CloseAndWait(closeCtx); err != nil {
			t.Errorf("join final-ACK test client: %v", err)
		}
	})

	gatewayRoute := make(chan []byte, 32)
	gatewayTransport := NewSendGatewayTransport()
	client.RouteManager().UpdateTransport(gatewayTransport, []Route{gatewayRoute})
	defer client.RouteManager().RemoveTransport(gatewayTransport)

	receiveBufferSettings := DefaultReceiveBufferSettings()
	receiveBufferSettings.IdleTimeout = time.Hour
	receiveBufferSettings.AckCompressTimeout = time.Hour
	receiveBufferSettings.WriteTimeout = time.Second
	compressWaiting := make(chan struct{})
	primeWritten := make(chan struct{})
	cleanupPaused := make(chan struct{})
	releaseCleanup := make(chan struct{})
	var compressWaitingOnce sync.Once
	var primeWrittenOnce sync.Once
	var cleanupPausedOnce sync.Once
	var releaseCleanupOnce sync.Once
	receiveBufferSettings.beforeAckCompressWaitForTest = func(receiveSequenceId) {
		compressWaitingOnce.Do(func() { close(compressWaiting) })
	}
	receiveBufferSettings.afterAckWriteForTest = func(receiveSequenceId) {
		primeWrittenOnce.Do(func() { close(primeWritten) })
	}
	receiveBufferSettings.beforeAckWorkerStopForTest = func(receiveSequenceId) {
		cleanupPausedOnce.Do(func() { close(cleanupPaused) })
		<-releaseCleanup
	}
	release := func() {
		releaseCleanupOnce.Do(func() { close(releaseCleanup) })
	}
	defer release()

	messageId := NewId()
	sourceId := NewId()
	receiveSequence := NewReceiveSequence(
		ctx,
		client,
		SourceId(sourceId),
		NewId(),
		sequenceTlsRoleServer,
		false,
		receiveBufferSettings,
	)
	go receiveSequence.Run()
	// Prime the rate limiter with one idle-burst ACK. The optimized worker sends
	// that first ACK immediately; cancellation below publishes the final item
	// inside the one-hour interval and therefore pins the compression wait.
	primeMessageId := NewId()
	receiveSequence.ackWindow.Update(sequenceAck{
		sequenceNumber: 0,
		messageId:      primeMessageId,
	})
	select {
	case transferFrameBytes := <-gatewayRoute:
		MessagePoolReturn(transferFrameBytes)
	case <-time.After(5 * time.Second):
		t.Fatal("idle-burst ACK waited for the compression interval")
	}
	select {
	case <-primeWritten:
	case <-time.After(5 * time.Second):
		t.Fatal("idle-burst ACK write did not complete")
	}

	receiveSequence.ackWindow.Update(sequenceAck{
		sequenceNumber: 1,
		messageId:      messageId,
	})
	select {
	case <-compressWaiting:
	case <-time.After(5 * time.Second):
		t.Fatal("ACK worker did not enter the compression wait")
	}

	receiveSequence.Cancel()
	select {
	case <-cleanupPaused:
	case <-time.After(5 * time.Second):
		t.Fatal("receive sequence cleanup did not reach the ACK-worker stop barrier")
	}

	ackDeadline := time.After(5 * time.Second)

waitForAck:
	for {
		select {
		case transferFrameBytes := <-gatewayRoute:
			transferFrame := &protocol.TransferFrame{}
			err := proto.Unmarshal(transferFrameBytes, transferFrame)
			MessagePoolReturn(transferFrameBytes)
			if err == nil && transferFrame.Ack != nil &&
				slices.Equal(transferFrame.Ack.MessageId, messageId.Bytes()) {
				break waitForAck
			}
		case <-ackDeadline:
			t.Fatal("cancel discarded the final compressed ACK")
		}
	}

	release()
	select {
	case <-receiveSequence.exit:
	case <-time.After(5 * time.Second):
		t.Fatal("receive sequence did not publish exit after ACK drain")
	}
}

// A final ACK may already own an indefinitely blocked route write when the
// sequence begins teardown. Exact writer and cancellation barriers prove
// cleanup interrupts that write before joining the ACK worker.
func TestReceiveSequenceCancelInterruptsBlockedFinalAckWrite(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientSettings := DefaultClientSettings()
	clientSettings.Log = NewNoopLogger()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), clientSettings)
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := client.CloseAndWait(closeCtx); err != nil {
			t.Errorf("join blocked-ACK test client: %v", err)
		}
	})

	blockedRoute := make(chan []byte, 1)
	blockedRoute <- nil
	gatewayTransport := NewSendGatewayTransport()
	client.RouteManager().UpdateTransport(
		gatewayTransport,
		[]Route{blockedRoute},
	)
	defer client.RouteManager().RemoveTransport(gatewayTransport)

	ackWriterReady := make(chan struct{})
	ackWritesCanceled := make(chan struct{})
	var writerSnapshotAcquired <-chan struct{}
	var resumeWriter func()
	var ackWriterReadyOnce sync.Once
	var ackWritesCanceledOnce sync.Once
	receiveBufferSettings := DefaultReceiveBufferSettings()
	receiveBufferSettings.IdleTimeout = time.Hour
	receiveBufferSettings.AckCompressTimeout = time.Hour
	receiveBufferSettings.WriteTimeout = time.Duration(-1)
	receiveBufferSettings.afterAckWriterOpenForTest = func(
		_ receiveSequenceId,
		writer MultiRouteWriter,
	) {
		acquired, _, resume := TestingPauseMultiRouteWriterSnapshot(writer)
		writerSnapshotAcquired = acquired
		resumeWriter = resume
		ackWriterReadyOnce.Do(func() { close(ackWriterReady) })
	}
	receiveBufferSettings.afterAckWritesCanceledForTest = func(
		_ receiveSequenceId,
	) {
		ackWritesCanceledOnce.Do(func() { close(ackWritesCanceled) })
	}

	messageId := NewId()
	receiveSequence := NewReceiveSequence(
		ctx,
		client,
		SourceId(NewId()),
		NewId(),
		sequenceTlsRoleServer,
		false,
		receiveBufferSettings,
	)
	receiveSequence.deliverItems = []*receiveItem{
		{
			transferItem: transferItem{
				messageId:      messageId,
				sequenceNumber: 1,
			},
			ack: true,
		},
	}

	go receiveSequence.Run()
	select {
	case <-ackWriterReady:
	case <-time.After(5 * time.Second):
		t.Fatal("final ACK route writer did not open")
	}
	defer resumeWriter()

	receiveSequence.Cancel()
	select {
	case <-writerSnapshotAcquired:
	case <-time.After(5 * time.Second):
		t.Fatal("final ACK writer did not acquire its blocked route snapshot")
	}
	select {
	case <-ackWritesCanceled:
	case <-time.After(5 * time.Second):
		// Recover the deliberately blocked old ordering before failing: free one
		// route slot, let its final ACK finish, and join the sequence.
		<-blockedRoute
		resumeWriter()
		select {
		case transferFrameBytes := <-blockedRoute:
			MessagePoolReturn(transferFrameBytes)
		case <-time.After(5 * time.Second):
			t.Fatal("blocked final ACK did not finish during failure cleanup")
		}
		select {
		case <-receiveSequence.exit:
		case <-time.After(5 * time.Second):
			t.Fatal("receive sequence did not exit during failure cleanup")
		}
		t.Fatal("receive sequence waited for the blocked final ACK before canceling its route write")
	}

	resumeWriter()
	select {
	case <-receiveSequence.exit:
	case <-time.After(5 * time.Second):
		t.Fatal("receive sequence did not exit after canceling its blocked final ACK")
	}

	if sentinel := <-blockedRoute; sentinel != nil {
		MessagePoolReturn(sentinel)
		t.Fatal("blocked-route sentinel was replaced before final ACK cancellation")
	}

	func() {
		client.RouteManager().mutex.Lock()
		defer client.RouteManager().mutex.Unlock()

		ackDestination := DestinationId(receiveSequence.source.SourceId)
		if 0 != len(client.RouteManager().writerMatchState.destinationMultiRouteSelectors[ackDestination]) {
			t.Fatal("blocked final ACK route writer remained open after receive sequence exit")
		}
	}()
}
