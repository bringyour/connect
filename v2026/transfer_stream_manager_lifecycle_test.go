// Stream lifecycle tests model control callbacks as a nonblocking mutation
// boundary and place every blocking generation join in the owned worker.
package connect

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// waitForStreamLifecycleSignal waits for one deterministic lifecycle barrier.
func waitForStreamLifecycleSignal(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-signal:
	case <-timer.C:
		t.Fatal(message)
	}
}

// waitForStreamLifecycleIdSignal waits for one exact processed-stream event.
func waitForStreamLifecycleIdSignal(t *testing.T, signal <-chan Id, message string) Id {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case streamId := <-signal:
		return streamId
	case <-timer.C:
		t.Fatal(message)
		return Id{}
	}
}

// streamLifecycleTestClient creates a quiet client with ordinary destination
// stream policy, so focused lifecycle controls are admitted.
func streamLifecycleTestClient(t *testing.T) (*Client, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	return client, cancel
}

// installStreamLifecycleSequence publishes one active sequence with a
// generation-owned alias scope and an optional teardown barrier.
func installStreamLifecycleSequence(
	t *testing.T,
	client *Client,
	sourceId *Id,
	destinationId *Id,
	streamId Id,
	exitBarrier func(),
) *StreamSequence {
	t.Helper()
	streamBuffer := client.streamManager.streamBuffer
	generation, ok := client.RouteManager().beginWriterStreamAliasGeneration(streamId)
	if !ok {
		t.Fatal("could not allocate test stream generation")
	}
	sequence := NewStreamSequence(
		streamBuffer.ctx,
		client.streamManager,
		sourceId,
		destinationId,
		streamId,
		streamBuffer.streamBufferSettings,
	)
	sequence.exitBarrierForTest = exitBarrier
	if !sequence.reserveLifecycleGeneration(generation) {
		t.Fatal("could not reserve test stream generation")
	}
	id := newStreamSequenceId(sourceId, destinationId, streamId)
	streamBuffer.mutex.Lock()
	streamBuffer.streamSequences[id] = sequence
	streamBuffer.streamSequencesByStreamId[streamId] = sequence
	streamBuffer.mutex.Unlock()
	if !sequence.activateWriterStreamAliasScope(generation) {
		t.Fatal("could not activate test stream alias scope")
	}
	client.RouteManager().finishWriterStreamAliasGeneration(streamId, generation)
	go HandleError(func() {
		defer func() {
			streamBuffer.mutex.Lock()
			defer streamBuffer.mutex.Unlock()
			streamBuffer.removeStreamSequenceWithLock(sequence)
		}()
		sequence.Run()
	})
	return sequence
}

// streamLifecycleSequence returns the current indexed sequence.
func streamLifecycleSequence(client *Client, streamId Id) *StreamSequence {
	streamBuffer := client.streamManager.streamBuffer
	streamBuffer.mutex.Lock()
	defer streamBuffer.mutex.Unlock()
	return streamBuffer.streamSequencesByStreamId[streamId]
}

// streamLifecycleAliasScope returns the current endpoint scope.
func streamLifecycleAliasScope(client *Client, streamId Id) *writerStreamAliasScope {
	routeManager := client.RouteManager()
	routeManager.mutex.Lock()
	defer routeManager.mutex.Unlock()
	return routeManager.writerStreamAliasScopes[streamId]
}

// TestStreamCloseDoesNotJoinStalledSequence pins the receive-callback boundary:
// Close cancels the Run worker and clears aliases synchronously, but does not
// wait for transport teardown.
func TestStreamCloseDoesNotJoinStalledSequence(t *testing.T) {
	client, cancel := streamLifecycleTestClient(t)
	defer cancel()
	defer client.Close()

	streamId := NewId()
	adjacentId := NewId()
	finalId := NewId()
	client.RouteManager().authenticateWriterStreamDestination(streamId, finalId)
	exitEntered := make(chan struct{})
	exitRelease := make(chan struct{})
	releaseOnce := sync.Once{}
	defer releaseOnce.Do(func() { close(exitRelease) })
	sequence := installStreamLifecycleSequence(
		t,
		client,
		nil,
		&adjacentId,
		streamId,
		func() {
			close(exitEntered)
			<-exitRelease
		},
	)
	if streamLifecycleAliasScope(client, streamId) == nil {
		t.Fatal("test stream did not activate its alias scope")
	}

	closed := make(chan struct{})
	go func() {
		client.streamManager.streamBuffer.CloseStream(streamId)
		close(closed)
	}()
	waitForStreamLifecycleSignal(t, closed, "StreamClose joined a stalled Run worker")
	waitForStreamLifecycleSignal(t, exitEntered, "canceled sequence did not reach its exit barrier")
	if streamLifecycleAliasScope(client, streamId) != nil {
		t.Fatal("StreamClose returned before synchronously clearing its alias scope")
	}
	select {
	case <-sequence.done:
		t.Fatal("stalled sequence finished before its barrier was released")
	default:
	}
	releaseOnce.Do(func() { close(exitRelease) })
	sequence.Join()
}

// TestStreamReplacementReceiveDoesNotJoinAndPublishesAfterOldExit verifies
// that StreamOpen receive returns promptly while the owned worker joins the old
// generation, and that the replacement is invisible until that join finishes.
func TestStreamReplacementReceiveDoesNotJoinAndPublishesAfterOldExit(t *testing.T) {
	client, cancel := streamLifecycleTestClient(t)
	defer cancel()
	defer client.Close()

	streamId := NewId()
	oldAdjacentId := NewId()
	newAdjacentId := NewId()
	finalId := NewId()
	client.RouteManager().authenticateWriterStreamDestination(streamId, finalId)
	exitEntered := make(chan struct{})
	exitRelease := make(chan struct{})
	releaseOnce := sync.Once{}
	defer releaseOnce.Do(func() { close(exitRelease) })
	oldSequence := installStreamLifecycleSequence(
		t,
		client,
		nil,
		&oldAdjacentId,
		streamId,
		func() {
			close(exitEntered)
			<-exitRelease
		},
	)
	oldScope := streamLifecycleAliasScope(client, streamId)
	joinStarted := make(chan struct{})
	joinOnce := sync.Once{}
	replacementProcessed := make(chan struct{}, 1)
	client.streamManager.streamBuffer.beforeRetiredStreamJoinForTest = func(retired *StreamSequence) {
		if retired == oldSequence {
			joinOnce.Do(func() { close(joinStarted) })
		}
	}
	client.streamManager.streamBuffer.afterStreamOpenProcessedForTest = func(request *streamOpenRequest) {
		if request.streamId == streamId {
			replacementProcessed <- struct{}{}
		}
	}

	frame, err := ToFrame(&protocol.StreamOpen{
		SourceId: newAdjacentId.Bytes(),
		StreamId: streamId.Bytes(),
	}, DefaultProtocolVersion)
	if err != nil {
		t.Fatalf("encode replacement StreamOpen: %v", err)
	}
	received := make(chan struct{})
	go func() {
		client.streamManager.Receive(SourceId(ControlId), []*protocol.Frame{frame}, Peer{})
		close(received)
	}()
	waitForStreamLifecycleSignal(t, received, "replacement StreamOpen receive joined the old generation")
	waitForStreamLifecycleSignal(t, joinStarted, "replacement worker did not begin the owned join")
	waitForStreamLifecycleSignal(t, exitEntered, "replacement did not cancel the old generation")
	if streamLifecycleSequence(client, streamId) != nil {
		t.Fatal("replacement sequence published before the old generation joined")
	}
	if streamLifecycleAliasScope(client, streamId) != oldScope {
		t.Fatal("replacement alias scope published before the old generation joined")
	}

	releaseOnce.Do(func() { close(exitRelease) })
	waitForStreamLifecycleSignal(t, replacementProcessed, "replacement did not finish activation after the old generation joined")
	replacementSequence := streamLifecycleSequence(client, streamId)
	if replacementSequence == nil || replacementSequence == oldSequence ||
		replacementSequence.sourceId == nil || *replacementSequence.sourceId != newAdjacentId ||
		replacementSequence.destinationId != nil {
		t.Fatal("replacement publication did not index the new directional identity")
	}
	if scope := streamLifecycleAliasScope(client, streamId); scope == nil || scope == oldScope {
		t.Fatal("replacement did not install an isolated alias scope")
	}
	client.streamManager.streamBuffer.CloseStream(streamId)
}

// TestStreamCloseDuringConstructionCannotReopenAlias deterministically pauses
// before construction, closes the authoritative stream, then proves stale
// construction cannot reactivate its authenticated destination.
func TestStreamCloseDuringConstructionCannotReopenAlias(t *testing.T) {
	client, cancel := streamLifecycleTestClient(t)
	defer cancel()
	defer client.Close()
	streamBuffer := client.streamManager.streamBuffer

	streamId := NewId()
	adjacentId := NewId()
	finalId := NewId()
	client.RouteManager().authenticateWriterStreamDestination(streamId, finalId)
	constructEntered := make(chan struct{})
	constructRelease := make(chan struct{})
	releaseOnce := sync.Once{}
	defer releaseOnce.Do(func() { close(constructRelease) })
	streamBuffer.beforeStreamSequenceConstructForTest = func(request *streamOpenRequest) {
		if request.streamId == streamId {
			close(constructEntered)
			<-constructRelease
		}
	}
	if _, err := streamBuffer.OpenStream(nil, &adjacentId, streamId); err != nil {
		t.Fatalf("queue StreamOpen: %v", err)
	}
	waitForStreamLifecycleSignal(t, constructEntered, "worker did not reach construction barrier")
	streamBuffer.CloseStream(streamId)
	if streamLifecycleAliasScope(client, streamId) != nil {
		t.Fatal("close retained an alias before stale construction resumed")
	}
	processed := make(chan struct{}, 1)
	streamBuffer.afterStreamOpenProcessedForTest = func(request *streamOpenRequest) {
		if request.streamId == streamId {
			processed <- struct{}{}
		}
	}
	releaseOnce.Do(func() { close(constructRelease) })
	waitForStreamLifecycleSignal(t, processed, "stale construction request did not terminate")
	if streamLifecycleSequence(client, streamId) != nil {
		t.Fatal("stale construction published a sequence after StreamClose")
	}
	if streamLifecycleAliasScope(client, streamId) != nil {
		t.Fatal("stale construction reopened the cleared alias scope")
	}
}

// TestOlderCloseCannotRemoveNewerGeneration pauses an old close, fully opens a
// newer same-id generation, then proves its epoch protects the replacement.
func TestOlderCloseCannotRemoveNewerGeneration(t *testing.T) {
	client, cancel := streamLifecycleTestClient(t)
	defer cancel()
	defer client.Close()
	streamBuffer := client.streamManager.streamBuffer
	streamId := NewId()
	oldAdjacentId := NewId()
	finalId := NewId()
	client.RouteManager().authenticateWriterStreamDestination(streamId, finalId)
	oldSequence := installStreamLifecycleSequence(t, client, nil, &oldAdjacentId, streamId, nil)
	oldGeneration := oldSequence.LifecycleGeneration()
	oldScope := streamLifecycleAliasScope(client, streamId)
	aliasEntered := make(chan struct{})
	aliasRelease := make(chan struct{})
	aliasReleaseOnce := sync.Once{}
	defer aliasReleaseOnce.Do(func() { close(aliasRelease) })
	oldSequence.afterAliasScopeOpenForTest = func() {
		close(aliasEntered)
		<-aliasRelease
	}
	clearEntered := make(chan struct{})
	clearRelease := make(chan struct{})
	clearReleaseOnce := sync.Once{}
	defer clearReleaseOnce.Do(func() { close(clearRelease) })
	streamBuffer.beforeCloseAliasClearForTest = func() {
		close(clearEntered)
		<-clearRelease
	}
	refreshProcessed := make(chan struct{}, 1)
	streamBuffer.afterStreamOpenProcessedForTest = func(request *streamOpenRequest) {
		if request.streamId == streamId {
			refreshProcessed <- struct{}{}
		}
	}
	closeDone := make(chan struct{})
	go func() {
		streamBuffer.CloseStream(streamId)
		close(closeDone)
	}()
	waitForStreamLifecycleSignal(t, clearEntered, "close did not reach its alias-clear barrier")
	if _, err := streamBuffer.OpenStream(nil, &oldAdjacentId, streamId); err != nil {
		t.Fatalf("open generation after close epoch: %v", err)
	}
	waitForStreamLifecycleSignal(t, aliasEntered, "exact refresh did not reach alias ownership barrier")
	if oldSequence.LifecycleGeneration() <= oldGeneration {
		t.Fatal("exact Open did not reserve its generation before activation")
	}
	newScope := streamLifecycleAliasScope(client, streamId)
	if newScope == nil || newScope == oldScope {
		t.Fatal("newer generation did not open its generation-gated scope")
	}
	clearReleaseOnce.Do(func() { close(clearRelease) })
	waitForStreamLifecycleSignal(t, closeDone, "older close did not finish")
	if streamLifecycleAliasScope(client, streamId) != newScope {
		t.Fatal("older close cleared the newer alias scope")
	}
	if streamLifecycleSequence(client, streamId) != oldSequence || oldSequence.ctx.Err() != nil {
		t.Fatal("older close canceled the newer sequence")
	}
	aliasReleaseOnce.Do(func() { close(aliasRelease) })
	waitForStreamLifecycleSignal(t, refreshProcessed, "exact refresh did not finish after alias ownership release")
	streamBuffer.beforeCloseAliasClearForTest = nil
	oldSequence.afterAliasScopeOpenForTest = nil
	streamBuffer.CloseStream(streamId)
}

// TestStalePolicyClearCannotRemoveAllowedGeneration pauses a policy snapshot,
// opens a newer allowed hop, and proves the stale conditional clear is inert.
func TestStalePolicyClearCannotRemoveAllowedGeneration(t *testing.T) {
	client, cancel := streamLifecycleTestClient(t)
	defer cancel()
	defer client.Close()
	streamBuffer := client.streamManager.streamBuffer
	streamId := NewId()
	oldAdjacentId := NewId()
	finalId := NewId()
	client.RouteManager().authenticateWriterStreamDestination(streamId, finalId)
	oldSequence := installStreamLifecycleSequence(t, client, &oldAdjacentId, nil, streamId, nil)
	oldGeneration := oldSequence.LifecycleGeneration()
	oldScope := streamLifecycleAliasScope(client, streamId)
	clearEntered := make(chan struct{})
	clearRelease := make(chan struct{})
	releaseOnce := sync.Once{}
	defer releaseOnce.Do(func() { close(clearRelease) })
	allowPeer := false
	streamBuffer.beforePolicyAliasClearForTest = func(id Id, generation uint64) {
		if id == streamId && generation != 0 {
			close(clearEntered)
			<-clearRelease
		}
	}
	policyDone := make(chan struct{})
	go func() {
		streamBuffer.CloseDisallowedInboundProviderStreams(
			false,
			true,
			true,
			func(id Id) bool { return allowPeer && id == oldAdjacentId },
		)
		close(policyDone)
	}()
	waitForStreamLifecycleSignal(t, clearEntered, "policy did not reach its alias-clear barrier")
	allowPeer = true
	refreshProcessed := make(chan struct{}, 1)
	streamBuffer.afterStreamOpenProcessedForTest = func(request *streamOpenRequest) {
		if request.streamId == streamId {
			refreshProcessed <- struct{}{}
		}
	}
	if _, err := streamBuffer.OpenStream(&oldAdjacentId, nil, streamId); err != nil {
		t.Fatalf("open allowed generation after policy snapshot: %v", err)
	}
	waitForStreamLifecycleSignal(t, refreshProcessed, "same-pointer policy refresh did not finish")
	refreshedSequence := streamLifecycleSequence(client, streamId)
	if refreshedSequence != oldSequence || refreshedSequence.LifecycleGeneration() <= oldGeneration ||
		streamLifecycleAliasScope(client, streamId) == oldScope {
		t.Fatal("same sequence pointer did not refresh while stale policy clear was paused")
	}
	newScope := streamLifecycleAliasScope(client, streamId)
	releaseOnce.Do(func() { close(clearRelease) })
	waitForStreamLifecycleSignal(t, policyDone, "stale policy retirement did not finish")
	if newScope == nil || streamLifecycleAliasScope(client, streamId) != newScope {
		t.Fatal("stale policy clear removed the allowed replacement scope")
	}
	streamBuffer.beforePolicyAliasClearForTest = nil
	streamBuffer.CloseStream(streamId)
}

// TestStaleResetCannotCancelExactRefresh pauses after the reset snapshot,
// refreshes the same sequence pointer, then proves both alias and worker
// ownership survive the older conditional retirement.
func TestStaleResetCannotCancelExactRefresh(t *testing.T) {
	client, cancel := streamLifecycleTestClient(t)
	defer cancel()
	defer client.Close()
	streamBuffer := client.streamManager.streamBuffer

	streamId := NewId()
	adjacentId := NewId()
	finalId := NewId()
	client.RouteManager().authenticateWriterStreamDestination(streamId, finalId)
	sequence := installStreamLifecycleSequence(t, client, nil, &adjacentId, streamId, nil)
	oldGeneration := sequence.LifecycleGeneration()
	oldScope := streamLifecycleAliasScope(client, streamId)
	resetEntered := make(chan struct{})
	resetRelease := make(chan struct{})
	releaseOnce := sync.Once{}
	defer releaseOnce.Do(func() { close(resetRelease) })
	streamBuffer.afterResetLifecycleSnapshotForTest = func() {
		close(resetEntered)
		<-resetRelease
	}
	resetDone := make(chan struct{})
	go func() {
		streamBuffer.ResetStreams(map[streamSequenceId]bool{})
		close(resetDone)
	}()
	waitForStreamLifecycleSignal(t, resetEntered, "reset did not pause after its lifecycle snapshot")

	refreshProcessed := make(chan struct{}, 1)
	streamBuffer.afterStreamOpenProcessedForTest = func(request *streamOpenRequest) {
		if request.streamId == streamId {
			refreshProcessed <- struct{}{}
		}
	}
	if _, err := streamBuffer.OpenStream(nil, &adjacentId, streamId); err != nil {
		t.Fatalf("queue exact refresh during reset: %v", err)
	}
	waitForStreamLifecycleSignal(t, refreshProcessed, "exact refresh did not finish while reset was paused")
	newScope := streamLifecycleAliasScope(client, streamId)
	if sequence.LifecycleGeneration() <= oldGeneration || newScope == nil || newScope == oldScope {
		t.Fatal("exact refresh did not publish newer same-pointer ownership")
	}

	releaseOnce.Do(func() { close(resetRelease) })
	waitForStreamLifecycleSignal(t, resetDone, "stale reset did not finish")
	if streamLifecycleSequence(client, streamId) != sequence || sequence.ctx.Err() != nil {
		t.Fatal("stale reset canceled the newer same-pointer sequence")
	}
	if streamLifecycleAliasScope(client, streamId) != newScope {
		t.Fatal("stale reset removed the newer alias scope")
	}
	streamBuffer.afterResetLifecycleSnapshotForTest = nil
	streamBuffer.afterStreamOpenProcessedForTest = nil
	streamBuffer.CloseStream(streamId)
}

// TestStaleDisconnectCannotCancelExactRefresh pauses a disconnected-peer
// snapshot, refreshes the same stream generation, and proves the stale clear
// cannot retire the newly authoritative sequence.
func TestStaleDisconnectCannotCancelExactRefresh(t *testing.T) {
	client, cancel := streamLifecycleTestClient(t)
	defer cancel()
	defer client.Close()
	streamBuffer := client.streamManager.streamBuffer

	streamId := NewId()
	adjacentId := NewId()
	finalId := NewId()
	client.RouteManager().authenticateWriterStreamDestination(streamId, finalId)
	sequence := installStreamLifecycleSequence(t, client, nil, &adjacentId, streamId, nil)
	oldGeneration := sequence.LifecycleGeneration()
	oldScope := streamLifecycleAliasScope(client, streamId)
	disconnectEntered := make(chan struct{})
	disconnectRelease := make(chan struct{})
	releaseOnce := sync.Once{}
	defer releaseOnce.Do(func() { close(disconnectRelease) })
	streamBuffer.beforeDisconnectedAliasClearForTest = func(id Id, generation uint64) {
		if id == streamId && generation == oldGeneration {
			close(disconnectEntered)
			<-disconnectRelease
		}
	}
	disconnectDone := make(chan struct{})
	go func() {
		streamBuffer.CloseDisconnectedPeerStreams(map[Id]bool{adjacentId: true})
		close(disconnectDone)
	}()
	waitForStreamLifecycleSignal(
		t,
		disconnectEntered,
		"disconnect did not pause before its conditional alias clear",
	)

	refreshProcessed := make(chan struct{}, 1)
	streamBuffer.afterStreamOpenProcessedForTest = func(request *streamOpenRequest) {
		if request.streamId == streamId {
			refreshProcessed <- struct{}{}
		}
	}
	if _, err := streamBuffer.OpenStream(nil, &adjacentId, streamId); err != nil {
		t.Fatalf("queue exact refresh during disconnect: %v", err)
	}
	waitForStreamLifecycleSignal(
		t,
		refreshProcessed,
		"exact refresh did not finish while disconnect was paused",
	)
	newScope := streamLifecycleAliasScope(client, streamId)
	if sequence.LifecycleGeneration() <= oldGeneration || newScope == nil || newScope == oldScope {
		t.Fatal("disconnect refresh did not publish newer same-pointer ownership")
	}

	releaseOnce.Do(func() { close(disconnectRelease) })
	waitForStreamLifecycleSignal(t, disconnectDone, "stale disconnect did not finish")
	if streamLifecycleSequence(client, streamId) != sequence || sequence.ctx.Err() != nil {
		t.Fatal("stale disconnect canceled the newer same-pointer sequence")
	}
	if streamLifecycleAliasScope(client, streamId) != newScope {
		t.Fatal("stale disconnect removed the newer alias scope")
	}
	streamBuffer.beforeDisconnectedAliasClearForTest = nil
	streamBuffer.afterStreamOpenProcessedForTest = nil
	streamBuffer.CloseStream(streamId)
}

// verifyStreamCallbackRetirementDoesNotWait holds one real old-route writer,
// invokes a callback-owned alias retirement, then proves only a later physical
// transport teardown joins that writer generation.
func verifyStreamCallbackRetirementDoesNotWait(
	t *testing.T,
	invoke func(*StreamBuffer, Id, Id),
) {
	t.Helper()
	client, cancel := streamLifecycleTestClient(t)
	defer cancel()
	defer client.Close()
	streamBuffer := client.streamManager.streamBuffer
	routeManager := client.RouteManager()

	streamId := NewId()
	adjacentId := NewId()
	finalId := NewId()
	routeManager.authenticateWriterStreamDestination(streamId, finalId)
	generation, ok := routeManager.beginWriterStreamAliasGeneration(streamId)
	if !ok {
		t.Fatal("could not allocate callback-retirement generation")
	}
	sequence := NewStreamSequence(
		streamBuffer.ctx,
		client.streamManager,
		&adjacentId,
		nil,
		streamId,
		streamBuffer.streamBufferSettings,
	)
	if !sequence.reserveLifecycleGeneration(generation) {
		t.Fatal("could not reserve callback-retirement generation")
	}
	sequenceId := newStreamSequenceId(&adjacentId, nil, streamId)
	streamBuffer.mutex.Lock()
	streamBuffer.streamSequences[sequenceId] = sequence
	streamBuffer.streamSequencesByStreamId[streamId] = sequence
	streamBuffer.mutex.Unlock()
	if !sequence.activateWriterStreamAliasScope(generation) {
		t.Fatal("could not activate callback-retirement alias")
	}
	routeManager.finishWriterStreamAliasGeneration(streamId, generation)
	defer func() {
		streamBuffer.mutex.Lock()
		streamBuffer.removeStreamSequenceWithLock(sequence)
		streamBuffer.mutex.Unlock()
		sequence.Cancel()
		sequence.closeWriterStreamAliases()
	}()

	alias := StreamId(streamId)
	destination := DestinationId(finalId)
	transport := NewSendClientTransport(alias)
	route := make(chan []byte, 1)
	routeManager.UpdateTransport(transport, []Route{route})
	writer := routeManager.OpenMultiRouteWriter(destination)
	defer routeManager.CloseMultiRouteWriter(writer)
	if len(writer.GetActiveRoutes()) != 1 {
		t.Fatal("callback-retirement alias route was not active")
	}

	message := MessagePoolGet(128)
	witness := MessagePoolShareReadOnly(message)
	snapshotAcquired, retirementWaiting, resumeWriter := TestingPauseMultiRouteWriterSnapshot(writer)
	resumeOnce := sync.Once{}
	resume := func() {
		resumeOnce.Do(resumeWriter)
	}
	defer resume()
	writeDone := make(chan error, 1)
	go func() {
		success, err := writer.WriteDetailed(client.ctx, message, time.Second)
		if err == nil && !success {
			err = errors.New("paused callback-retirement writer did not send")
		}
		writeDone <- err
	}()
	waitForStreamLifecycleSignal(t, snapshotAcquired, "writer did not acquire callback-retirement snapshot")

	callbackDone := make(chan struct{})
	go func() {
		invoke(streamBuffer, streamId, adjacentId)
		close(callbackDone)
	}()
	waitForStreamLifecycleSignal(t, callbackDone, "stream callback waited for an admitted writer")
	select {
	case <-retirementWaiting:
		t.Fatal("callback-owned alias retirement entered a physical writer wait")
	default:
	}

	removeDone := make(chan struct{})
	go func() {
		routeManager.RemoveTransport(transport)
		close(removeDone)
	}()
	waitForStreamLifecycleSignal(t, retirementWaiting, "physical removal did not join callback retirement")
	select {
	case <-removeDone:
		t.Fatal("physical removal bypassed the admitted old-route writer")
	default:
	}

	resume()
	writeTimer := time.NewTimer(5 * time.Second)
	defer writeTimer.Stop()
	select {
	case err := <-writeDone:
		if err != nil {
			MessagePoolReturn(message)
			MessagePoolReturn(witness)
			t.Fatal(err)
		}
	case <-writeTimer.C:
		t.Fatal("callback-retirement writer did not resume")
	}
	waitForStreamLifecycleSignal(t, removeDone, "physical removal did not finish after writer release")
	routeTimer := time.NewTimer(5 * time.Second)
	defer routeTimer.Stop()
	select {
	case sentMessage := <-route:
		if len(sentMessage) == 0 || len(witness) == 0 || &sentMessage[0] != &witness[0] {
			MessagePoolReturn(sentMessage)
			MessagePoolReturn(witness)
			t.Fatal("old-route writer delivered a different message owner")
		}
		MessagePoolReturn(sentMessage)
	case <-routeTimer.C:
		MessagePoolReturn(witness)
		t.Fatal("old-route writer did not deliver its owned message")
	}
	if !MessagePoolReturn(witness) {
		t.Fatal("callback retirement did not release the delivered message owner")
	}
}

// TestStreamCloseCallbackDoesNotWaitForWriterRetirement covers StreamClose's
// inline receive path while an old route still owns a message.
func TestStreamCloseCallbackDoesNotWaitForWriterRetirement(t *testing.T) {
	verifyStreamCallbackRetirementDoesNotWait(t, func(streamBuffer *StreamBuffer, streamId Id, _ Id) {
		streamBuffer.CloseStream(streamId)
	})
}

// TestStreamResetCallbackDoesNotWaitForWriterRetirement covers StreamReset's
// inline receive path while an old route still owns a message.
func TestStreamResetCallbackDoesNotWaitForWriterRetirement(t *testing.T) {
	verifyStreamCallbackRetirementDoesNotWait(t, func(streamBuffer *StreamBuffer, _ Id, _ Id) {
		streamBuffer.ResetStreams(map[streamSequenceId]bool{})
	})
}

// TestStreamPolicyCallbackDoesNotWaitForWriterRetirement covers provider
// policy reconciliation while an old route still owns a message.
func TestStreamPolicyCallbackDoesNotWaitForWriterRetirement(t *testing.T) {
	verifyStreamCallbackRetirementDoesNotWait(t, func(streamBuffer *StreamBuffer, _ Id, _ Id) {
		streamBuffer.CloseDisallowedInboundProviderStreams(
			false,
			false,
			true,
			func(Id) bool { return false },
		)
	})
}

// TestStreamDisconnectCallbackDoesNotWaitForWriterRetirement covers peer
// disconnect reconciliation while an old route still owns a message.
func TestStreamDisconnectCallbackDoesNotWaitForWriterRetirement(t *testing.T) {
	verifyStreamCallbackRetirementDoesNotWait(t, func(streamBuffer *StreamBuffer, _ Id, adjacentId Id) {
		streamBuffer.CloseDisconnectedPeerStreams(map[Id]bool{adjacentId: true})
	})
}

// TestStreamResetUsesExactIdentityAndDoesNotJoin verifies same-id/different-hop
// reset retirement and prompt return while the old Run worker is stalled.
func TestStreamResetUsesExactIdentityAndDoesNotJoin(t *testing.T) {
	client, cancel := streamLifecycleTestClient(t)
	defer cancel()
	defer client.Close()
	streamBuffer := client.streamManager.streamBuffer
	streamId := NewId()
	oldAdjacentId := NewId()
	newAdjacentId := NewId()
	finalId := NewId()
	client.RouteManager().authenticateWriterStreamDestination(streamId, finalId)
	exitEntered := make(chan struct{})
	exitRelease := make(chan struct{})
	releaseOnce := sync.Once{}
	defer releaseOnce.Do(func() { close(exitRelease) })
	oldSequence := installStreamLifecycleSequence(
		t,
		client,
		nil,
		&oldAdjacentId,
		streamId,
		func() {
			close(exitEntered)
			<-exitRelease
		},
	)
	keep := map[streamSequenceId]bool{
		newStreamSequenceId(&newAdjacentId, nil, streamId): true,
	}
	resetDone := make(chan struct{})
	go func() {
		streamBuffer.ResetStreams(keep)
		close(resetDone)
	}()
	waitForStreamLifecycleSignal(t, resetDone, "StreamReset joined a stalled Run worker")
	waitForStreamLifecycleSignal(t, exitEntered, "StreamReset did not cancel the mismatched identity")
	if streamLifecycleAliasScope(client, streamId) != nil {
		t.Fatal("same-id identity reset retained the old alias scope")
	}
	joinEntered := make(chan struct{}, 1)
	replacementPublished := make(chan struct{}, 1)
	streamBuffer.beforeRetiredStreamJoinForTest = func(retired *StreamSequence) {
		if retired == oldSequence {
			joinEntered <- struct{}{}
		}
	}
	streamBuffer.afterStreamSequencePublishForTest = func(sequence *StreamSequence) {
		if sequence.streamId == streamId && sequence != oldSequence {
			replacementPublished <- struct{}{}
		}
	}
	if _, err := streamBuffer.OpenStream(&newAdjacentId, nil, streamId); err != nil {
		t.Fatalf("queue reset replacement: %v", err)
	}
	waitForStreamLifecycleSignal(t, joinEntered, "reset replacement did not enter the old-generation join")
	if sequence := streamLifecycleSequence(client, streamId); sequence != nil {
		t.Fatal("replacement became visible before reset-retired generation joined")
	}
	releaseOnce.Do(func() { close(exitRelease) })
	waitForStreamLifecycleSignal(t, replacementPublished, "reset replacement did not publish after old teardown")
	replacementSequence := streamLifecycleSequence(client, streamId)
	if replacementSequence == nil || replacementSequence == oldSequence ||
		replacementSequence.sourceId == nil || *replacementSequence.sourceId != newAdjacentId {
		t.Fatal("reset replacement did not index its new directional identity")
	}
	streamBuffer.CloseStream(streamId)
}

// TestStreamResetInvalidatesKeptOpenBeforePublication pauses an exact relisted
// Open after generation allocation but before StreamBuffer publication. Reset
// must retire that token, and only its post-reset relist may publish.
func TestStreamResetInvalidatesKeptOpenBeforePublication(t *testing.T) {
	client, cancel := streamLifecycleTestClient(t)
	defer cancel()
	defer client.Close()
	streamBuffer := client.streamManager.streamBuffer
	streamId := NewId()
	adjacentId := NewId()
	openEntered := make(chan struct{})
	openRelease := make(chan struct{})
	releaseOnce := sync.Once{}
	defer releaseOnce.Do(func() { close(openRelease) })
	hookStateLock := sync.Mutex{}
	blockFirst := true
	streamBuffer.afterStreamOpenGenerationForTest = func(request *streamOpenRequest) {
		hookStateLock.Lock()
		if request.streamId != streamId || !blockFirst {
			hookStateLock.Unlock()
			return
		}
		blockFirst = false
		hookStateLock.Unlock()
		close(openEntered)
		<-openRelease
	}

	firstDone := make(chan error, 1)
	go func() {
		_, err := streamBuffer.OpenStream(nil, &adjacentId, streamId)
		firstDone <- err
	}()
	waitForStreamLifecycleSignal(t, openEntered, "first Open did not reach prepublication barrier")
	keep := map[streamSequenceId]bool{
		newStreamSequenceId(nil, &adjacentId, streamId): true,
	}
	streamBuffer.ResetStreams(keep)
	postResetPublished := make(chan *StreamSequence, 1)
	postResetRelease := make(chan struct{})
	postResetReleaseOnce := sync.Once{}
	defer postResetReleaseOnce.Do(func() { close(postResetRelease) })
	streamBuffer.afterStreamSequencePublishForTest = func(sequence *StreamSequence) {
		if sequence.streamId == streamId {
			postResetPublished <- sequence
			<-postResetRelease
		}
	}
	if _, err := streamBuffer.OpenStream(nil, &adjacentId, streamId); err != nil {
		t.Fatalf("post-reset relist: %v", err)
	}
	var postResetSequence *StreamSequence
	postResetTimer := time.NewTimer(5 * time.Second)
	defer postResetTimer.Stop()
	select {
	case postResetSequence = <-postResetPublished:
	case <-postResetTimer.C:
		t.Fatal("post-reset relist did not publish while stale Open was paused")
	}
	releaseOnce.Do(func() { close(openRelease) })
	firstTimer := time.NewTimer(5 * time.Second)
	defer firstTimer.Stop()
	select {
	case err := <-firstDone:
		if err != nil {
			t.Fatalf("pre-reset Open after release: %v", err)
		}
	case <-firstTimer.C:
		t.Fatal("pre-reset Open did not finish after release")
	}
	if streamLifecycleSequence(client, streamId) != postResetSequence {
		t.Fatal("pre-reset Open displaced the post-reset relist")
	}
	postResetReleaseOnce.Do(func() { close(postResetRelease) })
	streamBuffer.CloseStream(streamId)
}

// TestCurrentActivationFailureRetriesFreshSequence proves a canceled reused
// generation is joined and rebuilt while its Open token is still current.
func TestCurrentActivationFailureRetriesFreshSequence(t *testing.T) {
	client, cancel := streamLifecycleTestClient(t)
	defer cancel()
	defer client.Close()
	streamBuffer := client.streamManager.streamBuffer
	configureStateLock := sync.Mutex{}
	configureCount := 0
	freshPublished := make(chan struct{}, 1)
	freshRelease := make(chan struct{})
	freshReleaseOnce := sync.Once{}
	defer freshReleaseOnce.Do(func() { close(freshRelease) })
	streamBuffer.configureStreamSequenceForTest = func(sequence *StreamSequence) {
		configureStateLock.Lock()
		defer configureStateLock.Unlock()
		configureCount += 1
		if configureCount == 1 {
			sequence.Cancel()
		}
	}
	streamBuffer.afterStreamSequencePublishForTest = func(sequence *StreamSequence) {
		if sequence.ctx.Err() == nil {
			freshPublished <- struct{}{}
			<-freshRelease
		}
	}
	processed := make(chan struct{}, 1)
	streamBuffer.afterStreamOpenProcessedForTest = func(request *streamOpenRequest) {
		processed <- struct{}{}
	}
	streamId := NewId()
	adjacentId := NewId()
	if _, err := streamBuffer.OpenStream(nil, &adjacentId, streamId); err != nil {
		t.Fatalf("queue activation retry: %v", err)
	}
	waitForStreamLifecycleSignal(t, freshPublished, "current canceled generation was not rebuilt")
	configureStateLock.Lock()
	count := configureCount
	configureStateLock.Unlock()
	sequence := streamLifecycleSequence(client, streamId)
	if count < 2 || sequence == nil || sequence.ctx.Err() != nil {
		t.Fatal("fresh retry was not live at its publication barrier")
	}
	freshReleaseOnce.Do(func() { close(freshRelease) })
	waitForStreamLifecycleSignal(t, processed, "fresh retry did not finish activation")
	streamBuffer.CloseStream(streamId)
}

// TestManagedStreamOpenRegistrySurvivesSequentialUniqueChurn proves terminal
// worker requests leave both bounded registries instead of exhausting them.
func TestManagedStreamOpenRegistrySurvivesSequentialUniqueChurn(t *testing.T) {
	client, cancel := streamLifecycleTestClient(t)
	defer cancel()
	defer client.Close()
	streamBuffer := client.streamManager.streamBuffer
	streamBuffer.configureStreamSequenceForTest = func(sequence *StreamSequence) {
		sequence.Cancel()
	}
	processedStreamIds := make(chan Id, 1)
	streamBuffer.afterStreamOpenProcessedForTest = func(request *streamOpenRequest) {
		processedStreamIds <- request.streamId
	}

	for index := 0; index < maxManagedStreamOpenRequests+1; index += 1 {
		streamId := NewId()
		adjacentId := NewId()
		if _, err := streamBuffer.OpenStream(nil, &adjacentId, streamId); err != nil {
			t.Fatalf("sequential open %d: %v", index, err)
		}
		processedStreamId := waitForStreamLifecycleIdSignal(
			t,
			processedStreamIds,
			"terminal open did not finish",
		)
		if processedStreamId != streamId {
			t.Fatalf("processed stream %s, expected %s", processedStreamId, streamId)
		}
		streamBuffer.managementStateLock.Lock()
		managedCount := len(streamBuffer.managedOpenRequests)
		pendingCount := len(streamBuffer.pendingOpenRequests)
		streamBuffer.managementStateLock.Unlock()
		if managedCount != 0 || pendingCount != 0 {
			t.Fatalf(
				"terminal open retained lifecycle state: managed=%d pending=%d",
				managedCount,
				pendingCount,
			)
		}
	}
}

// TestStalledReplacementDoesNotBlockUnrelatedOpen proves lifecycle joins are
// partitioned by stream id while their aggregate registries stay hard bounded.
func TestStalledReplacementDoesNotBlockUnrelatedOpen(t *testing.T) {
	client, cancel := streamLifecycleTestClient(t)
	defer cancel()
	defer client.Close()
	streamBuffer := client.streamManager.streamBuffer

	stalledStreamId := NewId()
	oldAdjacentId := NewId()
	newAdjacentId := NewId()
	stalledFinalId := NewId()
	client.RouteManager().authenticateWriterStreamDestination(stalledStreamId, stalledFinalId)
	exitEntered := make(chan struct{})
	exitRelease := make(chan struct{})
	releaseOnce := sync.Once{}
	defer releaseOnce.Do(func() { close(exitRelease) })
	oldSequence := installStreamLifecycleSequence(
		t,
		client,
		nil,
		&oldAdjacentId,
		stalledStreamId,
		func() {
			close(exitEntered)
			<-exitRelease
		},
	)
	joinEntered := make(chan struct{})
	streamBuffer.beforeRetiredStreamJoinForTest = func(sequence *StreamSequence) {
		if sequence == oldSequence {
			close(joinEntered)
		}
	}
	if _, err := streamBuffer.OpenStream(nil, &newAdjacentId, stalledStreamId); err != nil {
		t.Fatalf("queue stalled replacement: %v", err)
	}
	waitForStreamLifecycleSignal(t, joinEntered, "replacement did not enter its owned join")
	waitForStreamLifecycleSignal(t, exitEntered, "replacement did not reach stalled teardown")

	freshStreamId := NewId()
	freshAdjacentId := NewId()
	freshFinalId := NewId()
	client.RouteManager().authenticateWriterStreamDestination(freshStreamId, freshFinalId)
	freshPublished := make(chan struct{})
	streamBuffer.afterStreamSequencePublishForTest = func(sequence *StreamSequence) {
		if sequence.streamId == freshStreamId {
			close(freshPublished)
		}
	}
	if _, err := streamBuffer.OpenStream(nil, &freshAdjacentId, freshStreamId); err != nil {
		t.Fatalf("queue unrelated open: %v", err)
	}
	waitForStreamLifecycleSignal(
		t,
		freshPublished,
		"unrelated open was blocked behind a stalled replacement",
	)
	if sequence := streamLifecycleSequence(client, freshStreamId); sequence == nil {
		t.Fatal("unrelated worker signaled publication without indexing its sequence")
	}
	streamBuffer.managementStateLock.Lock()
	managedCount := len(streamBuffer.managedOpenRequests)
	pendingCount := len(streamBuffer.pendingOpenRequests)
	workerCount := len(streamBuffer.openWorkers)
	streamBuffer.managementStateLock.Unlock()
	if maxManagedStreamOpenRequests < managedCount || maxPendingStreamOpenRequests < pendingCount {
		t.Fatalf("lifecycle registry exceeded bounds: managed=%d pending=%d", managedCount, pendingCount)
	}
	if maxManagedStreamOpenRequests < workerCount {
		t.Fatalf("per-stream worker registry exceeded its bound: workers=%d", workerCount)
	}

	streamBuffer.CloseStream(freshStreamId)
	releaseOnce.Do(func() { close(exitRelease) })
	streamBuffer.CloseStream(stalledStreamId)
}

// TestExactRefreshReservesGenerationWithoutStreamBufferLocks proves the alias
// lifecycle lock never nests under either StreamBuffer state lock.
func TestExactRefreshReservesGenerationWithoutStreamBufferLocks(t *testing.T) {
	client, cancel := streamLifecycleTestClient(t)
	defer cancel()
	defer client.Close()
	streamBuffer := client.streamManager.streamBuffer

	streamId := NewId()
	adjacentId := NewId()
	finalId := NewId()
	client.RouteManager().authenticateWriterStreamDestination(streamId, finalId)
	sequence := installStreamLifecycleSequence(t, client, nil, &adjacentId, streamId, nil)
	lockCheck := make(chan bool, 1)
	sequence.afterLifecycleStateLockForTest = func() {
		managementAvailable := streamBuffer.managementStateLock.TryLock()
		if managementAvailable {
			streamBuffer.managementStateLock.Unlock()
		}
		sequenceMapAvailable := streamBuffer.mutex.TryLock()
		if sequenceMapAvailable {
			streamBuffer.mutex.Unlock()
		}
		lockCheck <- managementAvailable && sequenceMapAvailable
	}
	if _, err := streamBuffer.OpenStream(nil, &adjacentId, streamId); err != nil {
		t.Fatalf("queue exact refresh: %v", err)
	}
	checkTimer := time.NewTimer(5 * time.Second)
	defer checkTimer.Stop()
	select {
	case available := <-lockCheck:
		if !available {
			t.Fatal("exact refresh acquired the lifecycle lock under a StreamBuffer lock")
		}
	case <-checkTimer.C:
		t.Fatal("exact refresh did not reach the lifecycle lock seam")
	}
	sequence.afterLifecycleStateLockForTest = nil
	streamBuffer.CloseStream(streamId)
}

// TestLiveStreamLimitRejectsAndRecovers deterministically fills published
// state, rejects one excess id, then proves released capacity can be reused.
func TestLiveStreamLimitRejectsAndRecovers(t *testing.T) {
	client, cancel := streamLifecycleTestClient(t)
	defer cancel()
	defer client.Close()
	streamBuffer := client.streamManager.streamBuffer
	fakeSequences := make([]*StreamSequence, 0, maxLiveStreamSequences)

	streamBuffer.mutex.Lock()
	for index := 0; index < maxLiveStreamSequences; index += 1 {
		streamId := NewId()
		sourceId := NewId()
		destinationId := NewId()
		sequence := NewStreamSequence(
			streamBuffer.ctx,
			client.streamManager,
			&sourceId,
			&destinationId,
			streamId,
			streamBuffer.streamBufferSettings,
		)
		fakeSequences = append(fakeSequences, sequence)
		streamBuffer.streamSequences[newStreamSequenceId(&sourceId, &destinationId, streamId)] = sequence
		streamBuffer.streamSequencesByStreamId[streamId] = sequence
	}
	streamBuffer.mutex.Unlock()

	excessStreamId := NewId()
	excessSourceId := NewId()
	excessDestinationId := NewId()
	opened, err := streamBuffer.OpenStream(&excessSourceId, &excessDestinationId, excessStreamId)
	if err == nil || opened {
		t.Fatalf("live limit accepted excess stream: opened=%t err=%v", opened, err)
	}

	released := fakeSequences[len(fakeSequences)-1]
	streamBuffer.mutex.Lock()
	streamBuffer.removeStreamSequenceWithLock(released)
	streamBuffer.mutex.Unlock()
	released.Cancel()
	processed := make(chan struct{})
	streamBuffer.afterStreamOpenProcessedForTest = func(request *streamOpenRequest) {
		if request.streamId == excessStreamId {
			close(processed)
		}
	}
	streamBuffer.configureStreamSequenceForTest = func(sequence *StreamSequence) {
		if sequence.streamId == excessStreamId {
			sequence.Cancel()
		}
	}
	opened, err = streamBuffer.OpenStream(&excessSourceId, &excessDestinationId, excessStreamId)
	if err != nil || !opened {
		t.Fatalf("released live capacity was not reusable: opened=%t err=%v", opened, err)
	}
	waitForStreamLifecycleSignal(t, processed, "accepted stream did not release lifecycle state")

	streamBuffer.managementStateLock.Lock()
	managedCount := len(streamBuffer.managedOpenRequests)
	pendingCount := len(streamBuffer.pendingOpenRequests)
	streamBuffer.managementStateLock.Unlock()
	streamBuffer.mutex.Lock()
	liveCount := len(streamBuffer.streamSequencesByStreamId)
	for _, sequence := range fakeSequences {
		streamBuffer.removeStreamSequenceWithLock(sequence)
	}
	streamBuffer.mutex.Unlock()
	for _, sequence := range fakeSequences {
		sequence.Cancel()
	}
	if managedCount != 0 || pendingCount != 0 || maxLiveStreamSequences < liveCount {
		t.Fatalf(
			"released lifecycle state exceeded bounds: managed=%d pending=%d live=%d",
			managedCount,
			pendingCount,
			liveCount,
		)
	}
}
