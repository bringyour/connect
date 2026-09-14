package connect

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

type h1SendClientTransportForGroupTest struct {
	*sendClientTransport
}

func (self *h1SendClientTransportForGroupTest) TransportType() TransportType {
	return TransportTypeH1
}

type h3SendClientTransportForGroupTest struct {
	*sendClientTransport
}

func (self *h3SendClientTransportForGroupTest) TransportType() TransportType {
	return TransportTypeH3
}

type h1ReadyDrainAckTarget struct {
	results chan ByteCount
}

func (self *h1ReadyDrainAckTarget) sendAckResult(value ByteCount, err error) {
	if err != nil {
		self.results <- -value - 1
		return
	}
	self.results <- value
}

func closeTransferGroupTestClient(t *testing.T, client *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.CloseAndWait(ctx); err != nil {
		t.Errorf("close logical-group test client: %v", err)
	}
}

// transferGroupTestFrames returns independently owned raw frames so each
// chunk disposition can be verified with message-pool witnesses.
func transferGroupTestFrames(t *testing.T, count int, byteCount int) ([]*protocol.Frame, [][]byte) {
	t.Helper()
	frames := make([]*protocol.Frame, 0, count)
	witnesses := make([][]byte, 0, count)
	for index := range count {
		messageBytes := MessagePoolGet(byteCount)
		for byteIndex := range messageBytes {
			messageBytes[byteIndex] = byte(index + 1)
		}
		pooled, _ := MessagePoolCheck(messageBytes)
		if !pooled {
			for _, frame := range frames {
				MessagePoolReturn(frame.MessageBytes)
			}
			for _, witness := range witnesses {
				MessagePoolReturn(witness)
			}
			MessagePoolReturn(messageBytes)
			t.Fatalf("group frame byte count %d did not use the message pool", byteCount)
		}
		frames = append(frames, &protocol.Frame{
			MessageType:  protocol.MessageType_TransferExchangeSignals,
			MessageBytes: messageBytes,
			Raw:          true,
		})
		witnesses = append(witnesses, MessagePoolShareReadOnly(messageBytes))
	}
	return frames, witnesses
}

// releaseTransferGroupTestWitnesses proves every successfully admitted source
// frame has reached a terminal owner before the test releases its witness.
func releaseTransferGroupTestWitnesses(t *testing.T, frames []*protocol.Frame, witnesses [][]byte) {
	t.Helper()
	for index, witness := range witnesses {
		if MessagePoolReturn(witness) {
			continue
		}
		// Release a leaked owner so a failing test does not contaminate later
		// message-pool accounting in the same process.
		MessagePoolReturn(frames[index].MessageBytes)
		t.Fatalf("logical group retained frame %d after terminal completion", index)
	}
}

// One logical group is admitted once, split only at Transfer's wire bounds,
// and produces one Ack, NoAck, and lifecycle completion after every chunk has
// reached its initial writer disposition.
func TestSendMultiHopGroupChunksOneLogicalAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	destinationId := NewId()
	noAckObserver, noAckEvents := noAckObserverTestEvents()
	lifecycleObserver, lifecycleEvents := sendPackLifecycleTestObserver(destinationId)
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.NoAckSendObserver = noAckObserver
	settings.SendBufferSettings.SendPackLifecycleObserver = lifecycleObserver
	var sequenceCreateCount atomic.Int32
	var groupCompletionCreateCount atomic.Int32
	var groupCompletionChunkCount atomic.Int32
	settings.SendBufferSettings.beforeCreateSendSequenceForTest = func(id sendSequenceId) {
		if id.Destination == destinationId {
			sequenceCreateCount.Add(1)
		}
	}
	settings.SendBufferSettings.afterCreateSendGroupCompletionForTest = func(
		id sendSequenceId,
		chunkCount int,
	) {
		if id.Destination == destinationId {
			groupCompletionChunkCount.Store(int32(chunkCount))
			groupCompletionCreateCount.Add(1)
		}
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer closeTransferGroupTestClient(t, client)
	client.ContractManager().AddNoContractPeer(destinationId)
	route := make(chan []byte, 8)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)

	frames, witnesses := transferGroupTestFrames(t, 5, 64)
	result := make(chan error, 2)
	var resultCount atomic.Int32
	destination := RequireMultiHopId(NewId(), destinationId)
	success, err := client.sendMultiHopGroupWithTimeoutDetailed(
		frames,
		destination,
		func(err error) {
			resultCount.Add(1)
			result <- err
		},
		time.Second,
		NoAck(),
	)
	if !success || err != nil {
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
		for _, witness := range witnesses {
			MessagePoolReturn(witness)
		}
		t.Fatalf("logical group admission success=%t err=%v", success, err)
	}

	started := waitNoAckObservation(t, ctx, noAckEvents)
	if started.Phase != NoAckSendPhaseStarted {
		t.Fatalf("NoAck start=%+v", started)
	}
	lifecycleStarted := waitSendPackLifecycleObservation(t, ctx, lifecycleEvents)
	requireSendPackLifecycleObservation(
		t,
		lifecycleStarted,
		client.ClientId(),
		destinationId,
		lifecycleStarted.Token,
		SendPackLifecyclePhaseStarted,
		false,
		false,
	)

	// The terminal callback is a positive causality barrier: when it fires,
	// every chunk has completed its first route-write disposition. Queue length
	// is therefore an exact assertion, not a scheduler-dependent negative wait.
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("logical group completion: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for logical group completion: %v", ctx.Err())
	}
	if count := resultCount.Load(); count != 1 {
		t.Fatalf("logical group completion count=%d, want 1", count)
	}
	if routeCount := len(route); routeCount != 3 {
		t.Fatalf("logical group wire chunk count=%d, want 3", routeCount)
	}
	if createCount := sequenceCreateCount.Load(); createCount != 1 {
		t.Fatalf("logical group sequence creation count=%d, want 1", createCount)
	}
	if createCount := groupCompletionCreateCount.Load(); createCount != 1 {
		t.Fatalf("logical group completion creation count=%d, want 1", createCount)
	}
	if chunkCount := groupCompletionChunkCount.Load(); chunkCount != 3 {
		t.Fatalf("logical group completion chunk count=%d, want 3", chunkCount)
	}

	wantFrameCounts := []int{2, 2, 1}
	nextFrameIndex := 0
	for chunkIndex, wantFrameCount := range wantFrameCounts {
		transferFrameBytes := <-route
		pack := decodeSendPackLifecycleWirePack(t, transferFrameBytes)
		if frameCount := len(pack.Frames); frameCount != wantFrameCount {
			MessagePoolReturn(transferFrameBytes)
			t.Fatalf(
				"logical group chunk %d frame count=%d, want %d",
				chunkIndex,
				frameCount,
				wantFrameCount,
			)
		}
		for _, frame := range pack.Frames {
			if len(frame.MessageBytes) != 64 || frame.MessageBytes[0] != byte(nextFrameIndex+1) {
				MessagePoolReturn(transferFrameBytes)
				t.Fatalf(
					"logical group chunk %d frame %d did not preserve order",
					chunkIndex,
					nextFrameIndex,
				)
			}
			nextFrameIndex += 1
		}
		MessagePoolReturn(transferFrameBytes)
	}

	completed := waitNoAckObservation(t, ctx, noAckEvents)
	if completed.Phase != NoAckSendPhaseCompleted ||
		completed.Token != started.Token || completed.Err != nil {
		t.Fatalf("NoAck completion=%+v, start=%+v", completed, started)
	}
	firstRoute := waitSendPackLifecycleObservation(t, ctx, lifecycleEvents)
	terminal := waitSendPackLifecycleObservation(t, ctx, lifecycleEvents)
	requireSendPackLifecycleObservation(
		t,
		firstRoute,
		client.ClientId(),
		destinationId,
		lifecycleStarted.Token,
		SendPackLifecyclePhaseFirstRouteWrite,
		false,
		false,
	)
	requireSendPackLifecycleObservation(
		t,
		terminal,
		client.ClientId(),
		destinationId,
		lifecycleStarted.Token,
		SendPackLifecyclePhaseTerminal,
		false,
		false,
	)
	if count := len(noAckEvents); count != 0 {
		t.Fatalf("logical group left %d extra NoAck observations", count)
	}
	requireNoSendPackLifecycleObservations(t, lifecycleEvents, "logical group")
	releaseTransferGroupTestWitnesses(t, frames, witnesses)
}

// A group that fits one wire Pack uses the original SendPack completion
// records directly. The terminal callback is an exact barrier proving the
// multi-chunk aggregator could no longer be constructed afterward.
func TestSendMultiHopOneChunkGroupBypassesCompletionAggregator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	destinationId := NewId()
	var groupCompletionCreateCount atomic.Int32
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.afterCreateSendGroupCompletionForTest = func(
		id sendSequenceId,
		chunkCount int,
	) {
		if id.Destination == destinationId {
			groupCompletionCreateCount.Add(1)
		}
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer closeTransferGroupTestClient(t, client)
	client.ContractManager().AddNoContractPeer(destinationId)
	route := make(chan []byte, 1)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)

	frames, witnesses := transferGroupTestFrames(t, 2, 64)
	result := make(chan error, 1)
	success, err := client.sendMultiHopGroupWithTimeoutDetailed(
		frames,
		RequireMultiHopId(NewId(), destinationId),
		func(err error) { result <- err },
		time.Second,
		NoAck(),
	)
	if !success || err != nil {
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
		for _, witness := range witnesses {
			MessagePoolReturn(witness)
		}
		t.Fatalf("one-chunk group admission success=%t err=%v", success, err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("one-chunk group completion: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for one-chunk group completion: %v", ctx.Err())
	}
	if routeCount := len(route); routeCount != 1 {
		t.Fatalf("one-chunk group wire Pack count=%d, want 1", routeCount)
	}
	if createCount := groupCompletionCreateCount.Load(); createCount != 0 {
		t.Fatalf("one-chunk group constructed %d completion aggregators", createCount)
	}
	MessagePoolReturn(<-route)
	releaseTransferGroupTestWitnesses(t, frames, witnesses)
}

// Public SendMultiWithTimeout remains an explicit one-Pack API. Logical-group
// chunking is opt-in through the internal multi-hop group entry point.
func TestSendMultiWithTimeoutPreservesOneWirePack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	destinationId := NewId()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer closeTransferGroupTestClient(t, client)
	client.ContractManager().AddNoContractPeer(destinationId)
	route := make(chan []byte, 2)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)

	frames, witnesses := transferGroupTestFrames(t, 5, 64)
	result := make(chan error, 1)
	if !client.SendMultiWithTimeout(
		frames,
		destinationId,
		func(err error) { result <- err },
		time.Second,
		NoAck(),
	) {
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
		for _, witness := range witnesses {
			MessagePoolReturn(witness)
		}
		t.Fatal("explicit one-Pack send was not admitted")
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("explicit one-Pack completion: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for explicit one-Pack completion: %v", ctx.Err())
	}
	if routeCount := len(route); routeCount != 1 {
		t.Fatalf("explicit SendMulti wire Pack count=%d, want 1", routeCount)
	}
	transferFrameBytes := <-route
	pack := decodeSendPackLifecycleWirePack(t, transferFrameBytes)
	if frameCount := len(pack.Frames); frameCount != 5 {
		MessagePoolReturn(transferFrameBytes)
		t.Fatalf("explicit SendMulti frame count=%d, want 5", frameCount)
	}
	MessagePoolReturn(transferFrameBytes)
	releaseTransferGroupTestWitnesses(t, frames, witnesses)
}

// Cumulative acknowledgement of an early chunk must not complete the logical
// group. The test hook runs synchronously after that exact send item is removed,
// proving any premature callback would already be observable at the barrier.
func TestSendMultiHopGroupAckWaitsForEveryChunk(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	destinationId := NewId()
	firstChunkAcked := make(chan struct{})
	releaseFirstChunkAck := make(chan struct{})
	var firstChunkAckedOnce sync.Once
	var releaseFirstChunkAckOnce sync.Once
	releaseAck := func() {
		releaseFirstChunkAckOnce.Do(func() { close(releaseFirstChunkAck) })
	}
	defer releaseAck()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.afterAckSendItemForTest = func(id sendSequenceId, sequenceNumber uint64) {
		if id.Destination != destinationId || sequenceNumber != 0 {
			return
		}
		firstChunkAckedOnce.Do(func() { close(firstChunkAcked) })
		<-releaseFirstChunkAck
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer closeTransferGroupTestClient(t, client)
	client.ContractManager().AddNoContractPeer(destinationId)
	route := make(chan []byte, 8)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)

	frames, witnesses := transferGroupTestFrames(t, 5, 64)
	result := make(chan error, 2)
	var resultCount atomic.Int32
	destination := RequireMultiHopId(NewId(), destinationId)
	success, err := client.sendMultiHopGroupWithTimeoutDetailed(
		frames,
		destination,
		func(err error) {
			resultCount.Add(1)
			result <- err
		},
		time.Second,
	)
	if !success || err != nil {
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
		for _, witness := range witnesses {
			MessagePoolReturn(witness)
		}
		t.Fatalf("reliable logical group admission success=%t err=%v", success, err)
	}

	packs := make([]*protocol.Pack, 0, 3)
	for range 3 {
		select {
		case transferFrameBytes := <-route:
			packs = append(packs, decodeSendPackLifecycleWirePack(t, transferFrameBytes))
			MessagePoolReturn(transferFrameBytes)
		case <-ctx.Done():
			t.Fatalf("wait for reliable logical-group chunk: %v", ctx.Err())
		}
	}
	acknowledgeSendPackLifecycleWirePack(t, client, destinationId, packs[0])
	select {
	case <-firstChunkAcked:
	case <-ctx.Done():
		t.Fatalf("wait for first chunk Ack barrier: %v", ctx.Err())
	}
	if completionCount := len(result); completionCount != 0 {
		t.Fatalf("first of three chunk Acks completed logical group %d times", completionCount)
	}
	releaseAck()
	acknowledgeSendPackLifecycleWirePack(t, client, destinationId, packs[2])
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("reliable logical group completion: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for reliable logical group completion: %v", ctx.Err())
	}
	if count := resultCount.Load(); count != 1 {
		t.Fatalf("reliable logical group completion count=%d, want 1", count)
	}
	releaseTransferGroupTestWitnesses(t, frames, witnesses)
}

// A failure after the first chunk was materialized must dispose every later
// source frame while the already-written chunk converges on the same one-shot
// completion. The forced second contract update is an exact production seam.
func TestSendMultiHopGroupPartialMaterializationDisposesRemainder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	destinationId := NewId()
	var contractUpdateCount atomic.Int32
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.forceContractFailureForTest = func(id sendSequenceId) bool {
		return id.Destination == destinationId && contractUpdateCount.Add(1) == 2
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer closeTransferGroupTestClient(t, client)
	client.ContractManager().AddNoContractPeer(destinationId)
	route := make(chan []byte, 8)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)

	frames, witnesses := transferGroupTestFrames(t, 5, 64)
	result := make(chan error, 2)
	var resultCount atomic.Int32
	destination := RequireMultiHopId(NewId(), destinationId)
	success, err := client.sendMultiHopGroupWithTimeoutDetailed(
		frames,
		destination,
		func(err error) {
			resultCount.Add(1)
			result <- err
		},
		time.Second,
		NoAck(),
	)
	if !success || err != nil {
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
		for _, witness := range witnesses {
			MessagePoolReturn(witness)
		}
		t.Fatalf("partial logical group admission success=%t err=%v", success, err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("partial logical group completed without the forced error")
		}
	case <-ctx.Done():
		t.Fatalf("wait for partial logical group completion: %v", ctx.Err())
	}
	if count := resultCount.Load(); count != 1 {
		t.Fatalf("partial logical group completion count=%d, want 1", count)
	}
	if routeCount := len(route); routeCount != 1 {
		t.Fatalf("partial logical group wire chunk count=%d, want 1", routeCount)
	}
	MessagePoolReturn(<-route)
	releaseTransferGroupTestWitnesses(t, frames, witnesses)
}

// A group refused before SendSequence ownership leaves every source frame with
// the caller. The witness transitions prove Transfer neither returned nor kept
// an owner on the canceled admission path.
func TestSendMultiHopGroupRefusalRetainsWholeGroupOwnership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer closeTransferGroupTestClient(t, client)
	destinationId := NewId()
	client.ContractManager().AddNoContractPeer(destinationId)

	frames, witnesses := transferGroupTestFrames(t, 5, 64)
	sendCtx, cancelSend := context.WithCancel(ctx)
	cancelSend()
	success, err := client.sendMultiHopGroupWithTimeoutDetailed(
		frames,
		RequireMultiHopId(NewId(), destinationId),
		nil,
		time.Second,
		Ctx(sendCtx),
	)
	if success || err == nil {
		t.Fatalf("canceled logical group admission success=%t err=%v", success, err)
	}
	for index, frame := range frames {
		if MessagePoolReturn(frame.MessageBytes) {
			t.Fatalf("Transfer returned caller-owned frame %d after refusal", index)
		}
		if !MessagePoolReturn(witnesses[index]) {
			t.Fatalf("caller-owned frame %d retained another owner after refusal", index)
		}
	}
}

// Closing after the first chunk materializes joins its reliable owner with the
// unsent cursor. The original callback must observe one terminal error only
// after all five frame owners are released.
func TestSendMultiHopGroupCloseJoinsMaterializedAndPendingChunks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	destinationId := NewId()
	sequenceEntered := make(chan struct{})
	releaseSequence := make(chan struct{})
	resendCapacityWait := make(chan struct{})
	releaseResendCapacityWait := make(chan struct{})
	var sequenceEnteredOnce sync.Once
	var releaseSequenceOnce sync.Once
	var resendCapacityWaitOnce sync.Once
	var releaseResendCapacityWaitOnce sync.Once
	release := func() {
		releaseSequenceOnce.Do(func() { close(releaseSequence) })
		releaseResendCapacityWaitOnce.Do(func() { close(releaseResendCapacityWait) })
	}
	defer release()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.SequenceBufferSize = 1
	settings.SendBufferSettings.ResendQueueMaxByteCount = 1
	settings.SendBufferSettings.beforeResendCapacityWaitForTest = func(id sendSequenceId) {
		if id.Destination == destinationId {
			resendCapacityWaitOnce.Do(func() { close(resendCapacityWait) })
			<-releaseResendCapacityWait
		}
	}
	settings.SendBufferSettings.beforeRunSendSequenceForTest = func(id sendSequenceId) {
		if id.Destination != destinationId {
			return
		}
		sequenceEnteredOnce.Do(func() { close(sequenceEntered) })
		<-releaseSequence
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	client.ContractManager().AddNoContractPeer(destinationId)
	route := make(chan []byte, 1)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)

	frames, witnesses := transferGroupTestFrames(t, 5, 64)
	result := make(chan error, 2)
	var resultCount atomic.Int32
	success, err := client.sendMultiHopGroupWithTimeoutDetailed(
		frames,
		RequireMultiHopId(NewId(), destinationId),
		func(err error) {
			resultCount.Add(1)
			result <- err
		},
		time.Second,
	)
	if !success || err != nil {
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
		for _, witness := range witnesses {
			MessagePoolReturn(witness)
		}
		t.Fatalf("close logical group admission success=%t err=%v", success, err)
	}
	select {
	case <-sequenceEntered:
	case <-ctx.Done():
		t.Fatalf("wait for held logical-group sequence: %v", ctx.Err())
	}
	releaseSequenceOnce.Do(func() { close(releaseSequence) })
	select {
	case transferFrameBytes := <-route:
		MessagePoolReturn(transferFrameBytes)
	case <-ctx.Done():
		t.Fatalf("wait for first logical-group chunk: %v", ctx.Err())
	}
	select {
	case <-resendCapacityWait:
	case <-ctx.Done():
		t.Fatalf("wait for logical-group resend-capacity barrier: %v", ctx.Err())
	}
	client.Cancel()
	release()
	if err := client.CloseAndWait(ctx); err != nil {
		t.Fatalf("close logical-group client: %v", err)
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("closed logical group completed without an error")
		}
	default:
		t.Fatal("closed logical group did not publish terminal completion")
	}
	if count := resultCount.Load(); count != 1 {
		t.Fatalf("closed logical group completion count=%d, want 1", count)
	}
	releaseTransferGroupTestWitnesses(t, frames, witnesses)
}

func TestNextSendGroupChunkEndUsesCompatibilityBounds(t *testing.T) {
	frames := []*protocol.Frame{
		{MessageBytes: make([]byte, int(sendPackBatchMaxMessageByteCount)+1)},
		{MessageBytes: make([]byte, int(sendPackBatchMaxMessageByteCount)-64)},
		{MessageBytes: make([]byte, 64)},
	}
	if end := nextSendGroupChunkEnd(frames, 0); end != 1 {
		t.Fatalf("payload bound first chunk end=%d, want 1", end)
	}
	if end := nextSendGroupChunkEnd(frames, 1); end != 3 {
		t.Fatalf("2-frame compatibility bound second chunk end=%d, want 3", end)
	}
}

func TestH1LogicalGroupChunkCombinesAckSizedBurst(t *testing.T) {
	frames := make([]*protocol.Frame, sendPackH1GroupMaxFrames+1)
	for index := range frames {
		frames[index] = &protocol.Frame{MessageBytes: make([]byte, 64)}
	}
	if end := nextSendGroupChunkEndWithLimits(
		frames,
		0,
		sendPackH1GroupMaxFrames,
		sendPackH1GroupMaxMessageByteCount,
	); end != sendPackH1GroupMaxFrames {
		t.Fatalf("H1 ACK-sized first chunk end=%d, want %d", end, sendPackH1GroupMaxFrames)
	}
	if chunks := sendGroupChunkCountWithLimits(
		frames,
		sendPackH1GroupMaxFrames,
		sendPackH1GroupMaxMessageByteCount,
	); chunks != 2 {
		t.Fatalf("H1 ACK-sized chunk count=%d, want 2", chunks)
	}
	if end := nextSendGroupChunkEnd(frames, 0); end != sendPackBatchMaxFrames {
		t.Fatalf("H3-compatible first chunk end=%d, want %d", end, sendPackBatchMaxFrames)
	}
}

func TestH1LogicalGroupWritesAckSizedBurstAsOnePack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	destinationId := NewId()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer closeTransferGroupTestClient(t, client)
	client.ContractManager().AddNoContractPeer(destinationId)
	route := make(chan []byte, 32)
	client.RouteManager().UpdateTransport(
		&h1SendClientTransportForGroupTest{
			sendClientTransport: NewSendClientTransport(DestinationId(destinationId)),
		},
		[]Route{route},
	)
	policyWriter := client.RouteManager().OpenMultiRouteWriter(DestinationId(destinationId))
	h1Only := policyWriter.(transferFlightPolicyProvider).transferFlightPolicy().h1Only
	client.RouteManager().CloseMultiRouteWriter(policyWriter)
	if !h1Only {
		t.Fatal("typed H1 client route did not publish H1-only policy")
	}

	// The first response burst on a new sequence must already see the published
	// H1 policy; requiring a warm Pack here would put the conservative H3 chunk
	// bound directly on the first-byte path.
	frames, witnesses := transferGroupTestFrames(t, sendPackH1GroupMaxFrames, 64)
	result := make(chan error, 1)
	success, err := client.sendMultiHopGroupWithTimeoutDetailed(
		frames,
		RequireMultiHopId(NewId(), destinationId),
		func(err error) { result <- err },
		time.Second,
		NoAck(),
	)
	if !success || err != nil {
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
		for _, witness := range witnesses {
			MessagePoolReturn(witness)
		}
		t.Fatalf("H1 logical group admission success=%t err=%v", success, err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("H1 logical group completion: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf(
			"wait for H1 logical group completion: %v (wire=%d recovery=%+v)",
			ctx.Err(),
			len(route),
			client.SendRecoveryStats(),
		)
	}
	groupPackCount := 0
	frameCounts := []int{}
	for 0 < len(route) {
		transferFrameBytes := <-route
		pack := decodeSendPackLifecycleWirePack(t, transferFrameBytes)
		frameCounts = append(frameCounts, len(pack.Frames))
		if len(pack.Frames) == sendPackH1GroupMaxFrames {
			groupPackCount++
		}
		MessagePoolReturn(transferFrameBytes)
	}
	if groupPackCount != 1 {
		t.Fatalf(
			"H1 logical group matching wire Pack count=%d, want 1 (wire frame counts=%v)",
			groupPackCount,
			frameCounts,
		)
	}
	releaseTransferGroupTestWitnesses(t, frames, witnesses)
}

func TestH1EstablishedLogicalGroupWritesThreeTunnelPacketsPerPack(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	destinationId := NewId()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer closeTransferGroupTestClient(t, client)
	client.ContractManager().AddNoContractPeer(destinationId)
	route := make(chan []byte, 32)
	client.RouteManager().UpdateTransport(
		&h1SendClientTransportForGroupTest{
			sendClientTransport: NewSendClientTransport(DestinationId(destinationId)),
		},
		[]Route{route},
	)

	frames, witnesses := transferGroupTestFrames(t, 7, DefaultMtu)
	result := make(chan error, 1)
	success, err := client.sendGroupWithTimeoutDetailed(
		frames,
		destinationId,
		func(err error) { result <- err },
		time.Second,
		NoAck(),
	)
	if !success || err != nil {
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
		for _, witness := range witnesses {
			MessagePoolReturn(witness)
		}
		t.Fatalf("H1 tunnel-MTU group admission=(%t, %v)", success, err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("H1 tunnel-MTU group completion: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for H1 tunnel-MTU group: %v", ctx.Err())
	}

	frameCounts := make([]int, 0, 3)
	for 0 < len(route) {
		transferFrameBytes := <-route
		pack := decodeSendPackLifecycleWirePack(t, transferFrameBytes)
		frameCounts = append(frameCounts, len(pack.Frames))
		MessagePoolReturn(transferFrameBytes)
	}
	if !slices.Equal(frameCounts, []int{3, 3, 1}) {
		t.Fatalf("H1 established tunnel-MTU frame counts=%v, want [3 3 1]", frameCounts)
	}
	releaseTransferGroupTestWitnesses(t, frames, witnesses)
}

// H3 may rotate between active flows after each physical Pack, but a partially
// materialized logical group remains the head of its own flow. Otherwise two
// adjacent provider-return groups from one TCP connection are striped on the
// wire and can overflow the receiver's out-of-order segment queue.
func TestH3LogicalGroupsPreserveSameFlowFrameOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	destinationId := NewId()
	sequenceEntered := make(chan struct{})
	releaseSequence := make(chan struct{})
	var sequenceEnteredOnce sync.Once
	var releaseSequenceOnce sync.Once
	release := func() { releaseSequenceOnce.Do(func() { close(releaseSequence) }) }

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.beforeRunSendSequenceForTest = func(id sendSequenceId) {
		if id.Destination != destinationId {
			return
		}
		sequenceEnteredOnce.Do(func() { close(sequenceEntered) })
		<-releaseSequence
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer func() {
		release()
		closeTransferGroupTestClient(t, client)
	}()
	client.ContractManager().AddNoContractPeer(destinationId)
	route := make(chan []byte, 16)
	client.RouteManager().UpdateTransportWithProperties(
		&h3SendClientTransportForGroupTest{
			sendClientTransport: NewSendClientTransport(DestinationId(destinationId)),
		},
		[]Route{route},
		TransferCarrierProperties{
			Unreliable:              true,
			UnreliableFlowIsolation: true,
		},
	)

	firstFrames, firstWitnesses := transferGroupTestFrames(t, 4, DefaultMtu)
	secondFrames, secondWitnesses := transferGroupTestFrames(t, 4, DefaultMtu)
	results := make(chan error, 2)
	flowKey := testSendSchedulingKey(3000)
	admit := func(frames []*protocol.Frame, witnesses [][]byte) {
		success, err := client.sendGroupWithTimeoutDetailed(
			frames,
			destinationId,
			func(err error) { results <- err },
			time.Second,
			NoAck(),
			sendSchedulingKeyOption{key: flowKey},
		)
		if success && err == nil {
			return
		}
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
		for _, witness := range witnesses {
			MessagePoolReturn(witness)
		}
		t.Fatalf("H3 logical-group admission success=%t err=%v", success, err)
	}

	admit(firstFrames, firstWitnesses)
	select {
	case <-sequenceEntered:
	case <-ctx.Done():
		t.Fatalf("wait for held H3 logical-group sequence: %v", ctx.Err())
	}
	admit(secondFrames, secondWitnesses)
	release()

	for groupIndex := range 2 {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("H3 logical group %d completion: %v", groupIndex, err)
			}
		case <-ctx.Done():
			t.Fatalf("wait for H3 logical group %d: %v", groupIndex, ctx.Err())
		}
	}

	wantMarkers := []byte{1, 2, 3, 4, 1, 2, 3, 4}
	for wireIndex, wantMarker := range wantMarkers {
		var transferFrameBytes []byte
		select {
		case transferFrameBytes = <-route:
		case <-ctx.Done():
			t.Fatalf("wait for H3 logical-group wire Pack %d: %v", wireIndex, ctx.Err())
		}
		pack := decodeSendPackLifecycleWirePack(t, transferFrameBytes)
		gotMarker := byte(0)
		validShape := len(pack.Frames) == 1 &&
			len(pack.Frames[0].MessageBytes) == DefaultMtu
		if validShape {
			gotMarker = pack.Frames[0].MessageBytes[0]
		}
		if !validShape || gotMarker != wantMarker {
			MessagePoolReturn(transferFrameBytes)
			t.Fatalf(
				"H3 logical-group wire Pack %d frames=%d marker=%d, want one frame with marker %d",
				wireIndex,
				len(pack.Frames),
				gotMarker,
				wantMarker,
			)
		}
		MessagePoolReturn(transferFrameBytes)
	}
	if len(route) != 0 {
		t.Fatalf("H3 logical groups emitted %d extra wire Packs", len(route))
	}
	releaseTransferGroupTestWitnesses(t, firstFrames, firstWitnesses)
	releaseTransferGroupTestWitnesses(t, secondFrames, secondWitnesses)
}

func TestH1EstablishedLogicalGroupRequiresContractForWholeRemainder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer closeTransferGroupTestClient(t, client)

	sequence := &SendSequence{
		client:                         client,
		destination:                    NewId(),
		sendContractAcked:              true,
		sendContractMetadataGeneration: 0,
		sendContract: &sequenceContract{
			minUpdateByteCount:         1,
			effectiveTransferByteCount: 3 * DefaultMtu,
		},
	}
	frames := make([]*protocol.Frame, 4)
	for index := range frames {
		frames[index] = &protocol.Frame{MessageBytes: make([]byte, DefaultMtu)}
	}
	constrained := &SendPack{Frames: frames}
	sequence.pinLogicalGroupChunkLimits(
		constrained,
		transferFlightPolicySnapshot{h1Only: true},
	)
	_, constrainedBytes := constrained.groupChunkLimits()
	if constrainedBytes != sendPackH1GroupMaxMessageByteCount {
		t.Fatalf(
			"contract-short H1 group byte limit=%d, want %d",
			constrainedBytes,
			sendPackH1GroupMaxMessageByteCount,
		)
	}

	// The same contract can use the larger established envelope only when it
	// can debit every remaining frame. This keeps the pinned chunk policy from
	// crossing a later contract rotation that must carry its proof on the wire.
	sequence.sendContract.effectiveTransferByteCount = 4 * DefaultMtu
	available := &SendPack{Frames: frames}
	sequence.pinLogicalGroupChunkLimits(
		available,
		transferFlightPolicySnapshot{h1Only: true},
	)
	_, availableBytes := available.groupChunkLimits()
	if availableBytes != sendPackH1EstablishedMaxMessageByteCount {
		t.Fatalf(
			"contract-covered H1 group byte limit=%d, want %d",
			availableBytes,
			sendPackH1EstablishedMaxMessageByteCount,
		)
	}
}

// Independently admitted packets from different browser flows meet only after
// destination/sequence selection. H1 may drain all of them when they are
// already queued, but it must retain one callback and owner per original Pack.
// This is the TCP-ACK-heavy download case that exact-flow grouping alone cannot
// collapse; the startup barrier proves the implementation adds no batching
// wait to manufacture the burst.
func TestH1ReadyDrainCoalescesSixteenIndependentPacks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	destinationId := NewId()
	sequenceEntered := make(chan struct{})
	releaseSequence := make(chan struct{})
	var sequenceEnteredOnce sync.Once
	var releaseSequenceOnce sync.Once
	release := func() { releaseSequenceOnce.Do(func() { close(releaseSequence) }) }

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.beforeRunSendSequenceForTest = func(id sendSequenceId) {
		if id.Destination != destinationId {
			return
		}
		sequenceEnteredOnce.Do(func() { close(sequenceEntered) })
		<-releaseSequence
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer func() {
		release()
		closeTransferGroupTestClient(t, client)
	}()
	client.ContractManager().AddNoContractPeer(destinationId)
	route := make(chan []byte, 32)
	client.RouteManager().UpdateTransport(
		&h1SendClientTransportForGroupTest{
			sendClientTransport: NewSendClientTransport(DestinationId(destinationId)),
		},
		[]Route{route},
	)

	frames, witnesses := transferGroupTestFrames(t, sendPackH1GroupMaxFrames, 64)
	results := make(chan int, len(frames))
	for index, frame := range frames {
		index := index
		if !client.SendWithTimeout(
			frame,
			destinationId,
			func(err error) {
				if err != nil {
					results <- -index - 1
					return
				}
				results <- index
			},
			time.Second,
		) {
			for frameIndex := index; frameIndex < len(frames); frameIndex++ {
				MessagePoolReturn(frames[frameIndex].MessageBytes)
			}
			for _, witness := range witnesses {
				MessagePoolReturn(witness)
			}
			t.Fatalf("H1 ready Pack %d was not admitted", index)
		}
		if index == 0 {
			select {
			case <-sequenceEntered:
			case <-ctx.Done():
				t.Fatalf("wait for H1 ready-drain startup: %v", ctx.Err())
			}
		}
	}

	release()
	var transferFrameBytes []byte
	select {
	case transferFrameBytes = <-route:
	case <-ctx.Done():
		t.Fatalf("wait for H1 ready-drain wire Pack: %v", ctx.Err())
	}
	pack := decodeSendPackLifecycleWirePack(t, transferFrameBytes)
	if len(pack.Frames) != sendPackH1GroupMaxFrames {
		MessagePoolReturn(transferFrameBytes)
		t.Fatalf(
			"H1 ready-drain frame count=%d, want %d",
			len(pack.Frames),
			sendPackH1GroupMaxFrames,
		)
	}
	acknowledgeSendPackLifecycleWirePack(t, client, destinationId, pack)
	MessagePoolReturn(transferFrameBytes)

	for want := range len(frames) {
		select {
		case got := <-results:
			if got != want {
				t.Fatalf("H1 ready-drain callback %d=%d", want, got)
			}
		case <-ctx.Done():
			t.Fatalf("wait for H1 ready-drain callback %d: %v", want, ctx.Err())
		}
	}
	if len(route) != 0 {
		t.Fatalf("H1 ready drain emitted %d extra wire Packs", len(route))
	}
	releaseTransferGroupTestWitnesses(t, frames, witnesses)
}

// A cumulative Ack that arrives after the sender's snapshot but before an
// already-due recovery write must retire the ready-drained H1 write first. The
// two explicit barriers make the snapshot/arrival ordering independent of
// wall-clock and scheduler timing.
func TestH1ReadyHotPathAckSuppressesEveryRecoveryWrite(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	destinationId := NewId()
	sequenceEntered := make(chan struct{})
	releaseSequence := make(chan struct{})
	initialWriteQueued := make(chan struct{})
	releaseInitialWrite := make(chan struct{})
	ackCoalesced := make(chan struct{})
	ackItemRemoved := make(chan struct{})
	dueResendReached := make(chan struct{})
	releaseDueResend := make(chan struct{})
	var sequenceEnteredOnce sync.Once
	var releaseSequenceOnce sync.Once
	var initialWriteQueuedOnce sync.Once
	var releaseInitialWriteOnce sync.Once
	var ackCoalescedOnce sync.Once
	var ackItemRemovedOnce sync.Once
	var dueResendReachedOnce sync.Once
	var releaseDueResendOnce sync.Once
	releaseSequenceBarrier := func() {
		releaseSequenceOnce.Do(func() { close(releaseSequence) })
	}
	releaseInitialWriteBarrier := func() {
		releaseInitialWriteOnce.Do(func() { close(releaseInitialWrite) })
	}
	releaseDueResendBarrier := func() {
		releaseDueResendOnce.Do(func() { close(releaseDueResend) })
	}

	var forceDue atomic.Bool
	type wireEvent struct {
		messageId Id
		sendCount int
		resend    bool
	}
	var wireEventsMutex sync.Mutex
	var wireEvents []wireEvent
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.TransferWireMessageObserver = func(
		observation TransferWireMessageObservation,
	) {
		wireEventsMutex.Lock()
		wireEvents = append(wireEvents, wireEvent{
			messageId: observation.MessageId,
			sendCount: observation.SendCount,
			resend:    observation.Resend,
		})
		wireEventsMutex.Unlock()
	}
	settings.SendBufferSettings.beforeRunSendSequenceForTest = func(id sendSequenceId) {
		if id.Destination != destinationId {
			return
		}
		sequenceEnteredOnce.Do(func() { close(sequenceEntered) })
		<-releaseSequence
	}
	settings.SendBufferSettings.afterInitialWriteQueuedForTest = func(
		id sendSequenceId,
		sequenceNumber uint64,
	) {
		if id.Destination != destinationId || sequenceNumber != 0 {
			return
		}
		initialWriteQueuedOnce.Do(func() { close(initialWriteQueued) })
		<-releaseInitialWrite
	}
	settings.SendBufferSettings.afterAckSendItemForTest = func(
		id sendSequenceId,
		sequenceNumber uint64,
	) {
		if id.Destination == destinationId && sequenceNumber == 0 {
			ackItemRemovedOnce.Do(func() { close(ackItemRemoved) })
		}
	}
	settings.SendBufferSettings.afterAckCoalescedForTest = func(
		id sendSequenceId,
		sequenceNumber uint64,
	) {
		if id.Destination == destinationId && sequenceNumber == 0 {
			ackCoalescedOnce.Do(func() { close(ackCoalesced) })
		}
	}
	settings.SendBufferSettings.beforeDueResendForTest = func(
		id sendSequenceId,
		sequenceNumber uint64,
	) {
		if id.Destination != destinationId || sequenceNumber != 0 {
			return
		}
		dueResendReachedOnce.Do(func() { close(dueResendReached) })
		<-releaseDueResend
	}
	settings.SendBufferSettings.forceResendForTest = func(id sendSequenceId) bool {
		return id.Destination == destinationId && forceDue.Load()
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer func() {
		releaseSequenceBarrier()
		releaseInitialWriteBarrier()
		releaseDueResendBarrier()
		closeTransferGroupTestClient(t, client)
	}()
	client.ContractManager().AddNoContractPeer(destinationId)
	route := make(chan []byte, 32)
	client.RouteManager().UpdateTransport(
		&h1SendClientTransportForGroupTest{
			sendClientTransport: NewSendClientTransport(DestinationId(destinationId)),
		},
		[]Route{route},
	)

	frames, witnesses := transferGroupTestFrames(t, sendPackH1GroupMaxFrames, 64)
	results := make(chan int, len(frames))
	for index, frame := range frames {
		index := index
		if !client.SendWithTimeout(
			frame,
			destinationId,
			func(err error) {
				if err != nil {
					results <- -index - 1
					return
				}
				results <- index
			},
			time.Second,
		) {
			for frameIndex := index; frameIndex < len(frames); frameIndex++ {
				MessagePoolReturn(frames[frameIndex].MessageBytes)
			}
			for _, witness := range witnesses {
				MessagePoolReturn(witness)
			}
			t.Fatalf("H1 retransmit-regression Pack %d was not admitted", index)
		}
		if index == 0 {
			select {
			case <-sequenceEntered:
			case <-ctx.Done():
				t.Fatalf("wait for H1 retransmit-regression startup: %v", ctx.Err())
			}
		}
	}

	releaseSequenceBarrier()
	var transferFrameBytes []byte
	select {
	case transferFrameBytes = <-route:
	case <-ctx.Done():
		t.Fatalf("wait for initial H1 wire Pack: %v", ctx.Err())
	}
	pack := decodeSendPackLifecycleWirePack(t, transferFrameBytes)
	packMessageId, err := IdFromBytes(pack.MessageId)
	if err != nil {
		MessagePoolReturn(transferFrameBytes)
		t.Fatalf("decode initial H1 message id: %v", err)
	}
	if len(pack.Frames) != sendPackH1GroupMaxFrames {
		MessagePoolReturn(transferFrameBytes)
		t.Fatalf(
			"initial H1 frame count=%d, want %d",
			len(pack.Frames),
			sendPackH1GroupMaxFrames,
		)
	}
	select {
	case <-initialWriteQueued:
	case <-ctx.Done():
		MessagePoolReturn(transferFrameBytes)
		t.Fatalf("wait for initial H1 resend ownership: %v", ctx.Err())
	}

	// Force the first recovery deadline due, then stop immediately after the
	// sender has already consumed its empty Ack snapshot. Only now admit the
	// cumulative Ack. Releasing the due-write barrier must return to the Ack
	// snapshot instead of putting a duplicate Pack on the physical route.
	forceDue.Store(true)
	releaseInitialWriteBarrier()
	select {
	case <-dueResendReached:
	case <-ctx.Done():
		MessagePoolReturn(transferFrameBytes)
		t.Fatalf("wait for due H1 recovery write: %v", ctx.Err())
	}
	acknowledgeSendPackLifecycleWirePack(t, client, destinationId, pack)
	select {
	case <-ackCoalesced:
	case <-ctx.Done():
		MessagePoolReturn(transferFrameBytes)
		t.Fatalf("wait for H1 Ack coalescing: %v", ctx.Err())
	}
	MessagePoolReturn(transferFrameBytes)
	releaseDueResendBarrier()
	select {
	case <-ackItemRemoved:
	case <-ctx.Done():
		t.Fatalf("wait for H1 Ack retirement: %v", ctx.Err())
	}

	for want := range len(frames) {
		select {
		case got := <-results:
			if got != want {
				t.Fatalf("H1 retransmit-regression callback %d=%d", want, got)
			}
		case <-ctx.Done():
			t.Fatalf("wait for H1 retransmit-regression callback %d: %v", want, ctx.Err())
		}
	}
	queueCount, queueByteCount, _ := client.ResendQueueSize(
		destinationId,
		MultiHopId{},
		false,
		false,
	)
	if queueCount != 0 || queueByteCount != 0 {
		t.Fatalf("Acked H1 resend queue=%d/%dB, want empty", queueCount, queueByteCount)
	}
	wireEventsMutex.Lock()
	matchingWriteCount := 0
	matchingResendCount := 0
	matchingNonInitialSendCount := 0
	for _, event := range wireEvents {
		if event.messageId != packMessageId {
			continue
		}
		matchingWriteCount++
		if event.resend {
			matchingResendCount++
		}
		if event.sendCount != 1 {
			matchingNonInitialSendCount++
		}
	}
	allWireEvents := append([]wireEvent{}, wireEvents...)
	wireEventsMutex.Unlock()
	if matchingWriteCount != 1 || matchingResendCount != 0 {
		t.Fatalf(
			"H1 Pack physical wire writes/resends=%d/%d, want 1/0; all=%+v",
			matchingWriteCount,
			matchingResendCount,
			allWireEvents,
		)
	}
	if got := matchingNonInitialSendCount; got != 0 {
		t.Fatalf("H1 observations with send_count != 1: %d", got)
	}
	stats := client.SendRecoveryStats()
	if stats.InitialWriteCount < 1 ||
		stats.InitialFrameCount < uint64(sendPackH1GroupMaxFrames) {
		t.Fatalf(
			"H1 initial writes/frames=%d/%d, want at least 1/%d",
			stats.InitialWriteCount,
			stats.InitialFrameCount,
			sendPackH1GroupMaxFrames,
		)
	}
	if stats.AckPendingResendPreemptCount != 1 {
		t.Fatalf(
			"H1 Ack-pending due-resend preemptions=%d, want 1",
			stats.AckPendingResendPreemptCount,
		)
	}
	if stats.TimeoutResendWriteCount != 0 ||
		stats.CarrierChangeWriteCount != 0 ||
		stats.SelectiveGapWriteCount != 0 ||
		stats.AckTailProbeWriteCount != 0 ||
		stats.CumulativeProbeWriteCount != 0 ||
		stats.RecoveryWriteErrorCount != 0 ||
		stats.MissingContractWriteCount != 0 {
		t.Fatalf("clean H1 path emitted recovery writes: %+v", stats)
	}
	if len(route) != 0 {
		t.Fatalf("clean H1 path emitted %d extra wire Packs", len(route))
	}
	releaseTransferGroupTestWitnesses(t, frames, witnesses)
}

func TestH1ReadyDrainCallbackOverflowIsBoundedAndReclaimable(t *testing.T) {
	clearSendItemPool()
	t.Cleanup(clearSendItemPool)

	target := &h1ReadyDrainAckTarget{
		results: make(chan ByteCount, sendPackH1GroupMaxFrames),
	}
	var acks sendAckSet
	for index := range sendPackH1GroupMaxFrames {
		acks.add(sendAckRecord{target: target, value: ByteCount(index)})
	}
	if acks.overflow == nil {
		t.Fatal("H1 callback tail stayed inline past the two-record common case")
	}
	acks.invoke(nil)
	if acks.overflow != nil {
		t.Fatal("terminal H1 callback tail was not released")
	}
	for want := range sendPackH1GroupMaxFrames {
		if got := <-target.results; got != ByteCount(want) {
			t.Fatalf("H1 pooled callback %d=%d", want, got)
		}
	}
	if len(sendAckSetOverflowPool) != 1 {
		t.Fatalf("H1 Ack overflow pool size=%d, want 1", len(sendAckSetOverflowPool))
	}

	completed := make(chan uint64, sendPackH1GroupMaxFrames)
	var noAckSends noAckSendSet
	for index := range sendPackH1GroupMaxFrames {
		noAckSends.add(noAckSendRecord{
			observer: func(observation NoAckSendObservation) {
				completed <- observation.Token
			},
			token: uint64(index),
		})
	}
	if noAckSends.overflow == nil {
		t.Fatal("H1 NoAck callback tail stayed inline past the common case")
	}
	noAckSends.complete(nil)
	if noAckSends.overflow != nil {
		t.Fatal("completed H1 NoAck callback tail was not released")
	}
	for want := range sendPackH1GroupMaxFrames {
		if got := <-completed; got != uint64(want) {
			t.Fatalf("H1 pooled NoAck callback %d=%d", want, got)
		}
	}
	if len(noAckSendSetOverflowPool) != 1 {
		t.Fatalf(
			"H1 NoAck overflow pool size=%d, want 1",
			len(noAckSendSetOverflowPool),
		)
	}

	ClearMessagePools()
	if len(sendAckSetOverflowPool) != 0 || len(noAckSendSetOverflowPool) != 0 {
		t.Fatalf(
			"memory pressure retained H1 callback tails Ack=%d NoAck=%d",
			len(sendAckSetOverflowPool),
			len(noAckSendSetOverflowPool),
		)
	}
}

func TestSendAckSetRetainsNonRegenerableRecordPastAckTimeout(t *testing.T) {
	var ordinary sendAckSet
	ordinary.add(sendAckRecord{callback: func(error) {}})
	if ordinary.retainPastAckTimeout() {
		t.Fatal("ordinary Ack record acquired an infinite recovery lifetime")
	}
	ordinary.invoke(nil)

	var retained sendAckSet
	retained.add(sendAckRecord{retainAfterAckTimeout: true})
	if !retained.retainPastAckTimeout() {
		t.Fatal("callback-free retained Ack record was discarded")
	}
	retained.invoke(nil)

	var coalesced sendAckSet
	coalesced.add(sendAckRecord{callback: func(error) {}})
	coalesced.add(sendAckRecord{callback: func(error) {}})
	coalesced.add(sendAckRecord{retainAfterAckTimeout: true})
	if coalesced.overflow == nil {
		t.Fatal("retained coalesced record did not exercise the overflow path")
	}
	if !coalesced.retainPastAckTimeout() {
		t.Fatal("coalesced overflow lost its retained record")
	}
	coalesced.invoke(nil)

	group := &sendGroupCompletion{
		ack: sendAckRecord{retainAfterAckTimeout: true},
	}
	var chunk sendAckSet
	chunk.add(group.chunkAckRecord())
	if !chunk.retainPastAckTimeout() {
		t.Fatal("logical-group chunk lost its parent recovery lifetime")
	}
}

func TestSendPackPinsLogicalGroupChunkLimits(t *testing.T) {
	pack := &SendPack{logicalGroup: true}
	pack.pinGroupChunkLimits(transferFlightPolicySnapshot{h1Only: true})
	if frames, bytes := pack.groupChunkLimits(); frames != sendPackH1GroupMaxFrames || bytes != sendPackH1GroupMaxMessageByteCount {
		t.Fatalf("H1 group limits=(%d, %d), want (%d, %d)", frames, bytes, sendPackH1GroupMaxFrames, sendPackH1GroupMaxMessageByteCount)
	}
	pack.pinGroupChunkLimits(transferFlightPolicySnapshot{})
	if frames, bytes := pack.groupChunkLimits(); frames != sendPackH1GroupMaxFrames || bytes != sendPackH1GroupMaxMessageByteCount {
		t.Fatalf("pinned H1 group limits changed to (%d, %d)", frames, bytes)
	}
}

// benchmarkH1Burst compares one logical-group admission with independently
// admitted Packs after the H1 ready-drain coalescer. A fully ready independent
// burst can reach the same wire minimum; a concurrently drained leading
// singleton can add Packs. The comparison therefore includes real scheduler
// readiness as well as routing/admission and callback work.
func benchmarkH1Burst(b *testing.B, logicalGroup bool, frameBytes int) {
	const frameCount = 16
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	destinationId := NewId()
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	b.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := client.CloseAndWait(closeCtx); err != nil {
			b.Errorf("close H1 group benchmark client: %v", err)
		}
	})
	client.ContractManager().AddNoContractPeer(destinationId)
	route := make(chan []byte, 64)
	client.RouteManager().UpdateTransport(
		&h1SendClientTransportForGroupTest{
			sendClientTransport: NewSendClientTransport(DestinationId(destinationId)),
		},
		[]Route{route},
	)

	// Open the sequence and publish its carrier policy outside the measurement.
	warmFrames := []*protocol.Frame{{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: MessagePoolGet(64),
		Raw:          true,
	}}
	warmDone := make(chan error, 1)
	success, err := client.sendGroupWithTimeoutDetailed(
		warmFrames,
		destinationId,
		func(err error) { warmDone <- err },
		time.Second,
		NoAck(),
	)
	if !success || err != nil {
		MessagePoolReturn(warmFrames[0].MessageBytes)
		b.Fatalf("warm H1 logical group success=%t err=%v", success, err)
	}
	if err := <-warmDone; err != nil {
		b.Fatalf("warm H1 logical group completion: %v", err)
	}
	for 0 < len(route) {
		MessagePoolReturn(<-route)
	}

	framesPerWirePack := min(
		sendPackH1GroupMaxFrames,
		int(sendPackH1EstablishedMaxMessageByteCount)/frameBytes,
	)
	wantWirePacks := (frameCount + framesPerWirePack - 1) / framesPerWirePack
	b.ReportAllocs()
	b.SetBytes(int64(frameCount * frameBytes))
	wirePackTotal := 0
	b.ResetTimer()
	for range b.N {
		frames := make([]*protocol.Frame, frameCount)
		for frameIndex := range frames {
			frames[frameIndex] = &protocol.Frame{
				MessageType:  protocol.MessageType_TransferExchangeSignals,
				MessageBytes: MessagePoolGet(frameBytes),
				Raw:          true,
			}
		}
		completed := make(chan error, frameCount)
		if logicalGroup {
			success, err := client.sendGroupWithTimeoutDetailed(
				frames,
				destinationId,
				func(err error) { completed <- err },
				time.Second,
				NoAck(),
			)
			if !success || err != nil {
				b.Fatalf("H1 logical burst success=%t err=%v", success, err)
			}
		} else {
			for frameIndex := range frames {
				if !client.SendMultiWithTimeout(
					frames[frameIndex:frameIndex+1],
					destinationId,
					func(err error) { completed <- err },
					time.Second,
					NoAck(),
				) {
					b.Fatalf("H1 singleton burst rejected frame %d", frameIndex)
				}
			}
		}
		completionCount := frameCount
		if logicalGroup {
			completionCount = 1
		}
		for range completionCount {
			if err := <-completed; err != nil {
				b.Fatalf("H1 burst completion: %v", err)
			}
		}
		wirePackCount := len(route)
		if logicalGroup && wirePackCount != wantWirePacks {
			b.Fatalf("H1 logical burst wire Packs=%d, want %d", wirePackCount, wantWirePacks)
		}
		if !logicalGroup && (wirePackCount < wantWirePacks || frameCount < wirePackCount) {
			b.Fatalf(
				"H1 independent burst wire Packs=%d, want range %d..%d",
				wirePackCount,
				wantWirePacks,
				frameCount,
			)
		}
		wirePackTotal += wirePackCount
		for range wirePackCount {
			MessagePoolReturn(<-route)
		}
	}
	b.ReportMetric(float64(wirePackTotal)/float64(b.N), "wire-packs/op")
}

func BenchmarkH1FullMtuBurstSingletonGroups(b *testing.B) {
	benchmarkH1Burst(b, false, 1500)
}

func BenchmarkH1FullMtuBurstLogicalGroup(b *testing.B) {
	benchmarkH1Burst(b, true, 1500)
}

func BenchmarkH1TunnelMtuBurstLogicalGroup(b *testing.B) {
	benchmarkH1Burst(b, true, DefaultMtu)
}

func BenchmarkH1AckSizedReadyBurstIndependentPacks(b *testing.B) {
	benchmarkH1Burst(b, false, 64)
}
