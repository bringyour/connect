// Callback backpressure regressions keep receive pumps nonblocking and keep
// sender-owned waits from holding state shared by unrelated peers.
package connect

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestSendSequenceCloseCallbackBackpressureIsDestinationLocal verifies that an
// intentionally blocking acknowledgement callback retains backpressure only
// for the sequence it belongs to. Sequence cleanup must publish map removal
// before invoking the callback and must not hold SendBuffer.mutex while the
// callback is blocked.
func TestSendSequenceCloseCallbackBackpressureIsDestinationLocal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sendBuffer := &SendBuffer{
		ctx:                        ctx,
		sendSequences:              map[sendSequenceId]*SendSequence{},
		wireSendSequences:          map[sendSequenceWireId]*SendSequence{},
		sendSequencesByDestination: map[Id]map[*SendSequence]bool{},
		sendSequenceDestinations:   map[*SendSequence]map[Id]bool{},
	}
	sequenceCtx, sequenceCancel := context.WithCancel(ctx)
	destination := NewId()
	sequence := &SendSequence{
		ctx:         sequenceCtx,
		cancel:      sequenceCancel,
		destination: destination,
		packs:       make(chan *SendPack, 1),
		acks:        make(chan receiveAckMessage, 1),
	}
	id := sequence.id()
	wireId := id.wireId()
	sendBuffer.sendSequences[id] = sequence
	sendBuffer.wireSendSequences[wireId] = sequence

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			close(releaseCallback)
		})
	}
	defer release()

	messageBytes := MessagePoolGet(1)
	messageBytes[0] = 1
	sequence.packs <- &SendPack{
		Frame: &protocol.Frame{
			MessageType:  protocol.MessageType_TransferExchangeSignals,
			MessageBytes: messageBytes,
		},
		AckCallback: func(error) {
			close(callbackStarted)
			<-releaseCallback
		},
		Ctx: ctx,
	}

	closeDone := make(chan struct{})
	go func() {
		defer close(closeDone)
		sendBuffer.closeSendSequence(id, wireId, sequence)
	}()

	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("sequence close did not reach its queued acknowledgement callback")
	}
	select {
	case <-closeDone:
		t.Fatal("sequence close escaped an intentionally blocked callback")
	default:
	}

	lookupDone := make(chan struct{})
	go func() {
		defer close(lookupDone)
		if found := sendBuffer.lookupSendSequence(
			sendSequenceId{Destination: NewId()},
			nil,
		); found != nil {
			t.Error("unrelated destination unexpectedly found a send sequence")
		}
	}()
	select {
	case <-lookupDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("blocked acknowledgement callback retained the buffer-wide send lock")
	}

	release()
	select {
	case <-closeDone:
	case <-time.After(time.Second):
		t.Fatal("sequence close did not finish after callback backpressure released")
	}
}

// TestReceiveSequenceReplacementDropsWithoutWaiting models a receive worker
// parked in a callback. A newer generation cancels that worker but drops its
// first Pack instead of parking the shared receive pump on worker exit. The
// sender retains the Pack until Transfer acknowledges a later retry.
func TestReceiveSequenceReplacementDropsWithoutWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	receiveBuffer := client.receiveBuffer
	sourceId := NewId()
	source := SourceId(sourceId)
	client.ContractManager().AddNoContractPeer(sourceId)
	oldSequenceId := NewId()
	newSequenceId := NewId()
	if !oldSequenceId.LessThan(newSequenceId) {
		t.Fatal("NewId did not preserve sequence ordering")
	}

	oldCtx, oldCancel := context.WithCancel(ctx)
	oldSequence := &ReceiveSequence{
		ctx:    oldCtx,
		cancel: oldCancel,
		exit:   make(chan struct{}),
	}
	oldId := receiveSequenceId{
		Source:         source,
		SequenceId:     oldSequenceId,
		EncryptionRole: sequenceTlsRoleServer,
	}
	headKey := receiveSequenceHeadKey{
		Source:         source,
		EncryptionRole: sequenceTlsRoleServer,
	}
	receiveBuffer.mutex.Lock()
	receiveBuffer.receiveSequences[oldId] = oldSequence
	receiveBuffer.headReceiveSequenceIds[headKey] = oldId
	receiveBuffer.mutex.Unlock()
	defer func() {
		receiveBuffer.mutex.Lock()
		delete(receiveBuffer.receiveSequences, oldId)
		delete(receiveBuffer.headReceiveSequenceIds, headKey)
		receiveBuffer.mutex.Unlock()
	}()

	transferFrameBytes := MessagePoolGet(1)
	transferFrameBytes[0] = 1
	replacementResult := make(chan struct {
		success bool
		err     error
	}, 1)
	go func() {
		success, packErr := receiveBuffer.Pack(
			&ReceivePack{
				Source:     source,
				SequenceId: newSequenceId,
				Pack: &protocol.Pack{
					MessageId:      NewId().Bytes(),
					SequenceId:     newSequenceId.Bytes(),
					SequenceNumber: 0,
					Head:           true,
					Nack:           true,
				},
				ReceiveCallback:    func(TransferPath, []*protocol.Frame, Peer) {},
				TransferFrameBytes: transferFrameBytes,
				Ctx:                ctx,
				Unwrapped:          true,
				EncryptionRole:     sequenceTlsRoleServer,
			},
			0,
		)
		replacementResult <- struct {
			success bool
			err     error
		}{success: success, err: packErr}
	}()

	select {
	case result := <-replacementResult:
		if result.err != nil || !result.success {
			t.Fatalf(
				"nonblocking replacement drop = (%t, %v), want accepted drop",
				result.success,
				result.err,
			)
		}
	case <-time.After(time.Second):
		t.Fatal("zero-timeout replacement waited for the old receive callback")
	}
	select {
	case <-oldCtx.Done():
		// The replacement reached and canceled the old generation.
	default:
		t.Fatal("new generation did not retire the old receive sequence")
	}
	if stats := client.ReceiveStats(); stats.PackHandoffDropCount != 1 ||
		stats.PackHandoffDropByteCount != 0 || stats.AckHandoffDropCount != 0 {
		t.Fatalf("replacement receive stats = %+v", stats)
	}
}

// TestReceiveSequenceClosingGenerationDropsWithoutWaiting covers the other
// generation race: Pack first finds the exact indexed worker, but that worker
// is already canceled when admission runs. The retry must not wait for the
// worker's exit before returning to the shared receive pump.
func TestReceiveSequenceClosingGenerationDropsWithoutWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	receiveBuffer := client.receiveBuffer
	source := SourceId(NewId())
	sequenceId := NewId()
	sequenceCtx, sequenceCancel := context.WithCancel(ctx)
	sequenceCancel()
	sequence := &ReceiveSequence{
		ctx:    sequenceCtx,
		cancel: sequenceCancel,
		exit:   make(chan struct{}),
	}
	id := receiveSequenceId{
		Source:         source,
		SequenceId:     sequenceId,
		EncryptionRole: sequenceTlsRoleServer,
	}
	headKey := receiveSequenceHeadKey{
		Source:         source,
		EncryptionRole: sequenceTlsRoleServer,
	}
	receiveBuffer.mutex.Lock()
	receiveBuffer.receiveSequences[id] = sequence
	receiveBuffer.headReceiveSequenceIds[headKey] = id
	receiveBuffer.mutex.Unlock()
	defer func() {
		receiveBuffer.mutex.Lock()
		delete(receiveBuffer.receiveSequences, id)
		delete(receiveBuffer.headReceiveSequenceIds, headKey)
		receiveBuffer.mutex.Unlock()
	}()

	transferFrameBytes := MessagePoolGet(1)
	transferFrameBytes[0] = 1
	result := make(chan struct {
		success bool
		err     error
	}, 1)
	go func() {
		success, packErr := receiveBuffer.Pack(
			&ReceivePack{
				Source:     source,
				SequenceId: sequenceId,
				Pack: &protocol.Pack{
					MessageId:      NewId().Bytes(),
					SequenceId:     sequenceId.Bytes(),
					SequenceNumber: 0,
					Head:           true,
				},
				ReceiveCallback:    func(TransferPath, []*protocol.Frame, Peer) {},
				TransferFrameBytes: transferFrameBytes,
				Ctx:                ctx,
				Unwrapped:          true,
				EncryptionRole:     sequenceTlsRoleServer,
			},
			0,
		)
		result <- struct {
			success bool
			err     error
		}{success: success, err: packErr}
	}()

	select {
	case packResult := <-result:
		if packResult.err != nil || !packResult.success {
			t.Fatalf(
				"closing-generation drop = (%t, %v), want accepted drop",
				packResult.success,
				packResult.err,
			)
		}
	case <-time.After(time.Second):
		t.Fatal("zero-timeout Pack waited for the closing receive generation")
	}
	if stats := client.ReceiveStats(); stats.PackHandoffDropCount != 1 ||
		stats.PackHandoffDropByteCount != 0 || stats.AckHandoffDropCount != 0 {
		t.Fatalf("closing-generation receive stats = %+v", stats)
	}
}

// callbackBackpressurePackBytes builds one plaintext, contract-free reliable
// Pack for deterministic injection through the production Client receive pump.
func callbackBackpressurePackBytes(
	t *testing.T,
	sourceId Id,
	destinationId Id,
	sequenceId Id,
	sequenceNumber uint64,
	head bool,
	content string,
) []byte {
	t.Helper()
	transferFrameBytes, err := proto.Marshal(&protocol.TransferFrame{
		TransferPath: TransferPath{
			SourceId:      sourceId,
			DestinationId: destinationId,
		}.ToProtobuf(),
		Pack: &protocol.Pack{
			MessageId:      NewId().Bytes(),
			SequenceId:     sequenceId.Bytes(),
			SequenceNumber: sequenceNumber,
			Head:           head,
			Frames: []*protocol.Frame{{
				MessageType:  protocol.MessageType_TransferExchangeSignals,
				MessageBytes: []byte(content),
			}},
		},
	})
	if err != nil {
		t.Fatalf("marshal receive Pack: %v", err)
	}
	return transferFrameBytes
}

// TestClientReceivePackHandoffDoesNotBlockUnrelatedSource fills one receive
// sequence while its callback is parked, then puts another Pack for that
// sequence ahead of an unrelated source. The full handoff must drop
// immediately; using ClientSettings.BufferTimeout here stalls the unrelated
// source for that entire timeout.
func TestClientReceivePackHandoffDoesNotBlockUnrelatedSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	settings.BufferTimeout = time.Hour
	settings.ReceiveBufferSettings.SequenceBufferSize = 1
	settings.ReceiveBufferSettings.IdleTimeout = time.Hour
	settings.EncryptionSettings.Mode = EncryptionModeOff
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	blockedSourceId := NewId()
	unrelatedSourceId := NewId()
	client.ContractManager().AddNoContractPeer(blockedSourceId)
	client.ContractManager().AddNoContractPeer(unrelatedSourceId)

	incoming := make(chan []byte, 8)
	receiveTransport := NewReceiveGatewayTransport()
	client.RouteManager().UpdateTransport(receiveTransport, []Route{incoming})
	defer client.RouteManager().RemoveTransport(receiveTransport)

	callbackStarted := make(chan struct{})
	releaseCallback := make(chan struct{})
	unrelatedDelivered := make(chan struct{})
	var callbackStartedOnce sync.Once
	var unrelatedDeliveredOnce sync.Once
	var releaseCallbackOnce sync.Once
	release := func() {
		releaseCallbackOnce.Do(func() { close(releaseCallback) })
	}
	defer release()
	client.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, peer Peer) {
		for _, frame := range frames {
			switch string(frame.MessageBytes) {
			case "blocked":
				callbackStartedOnce.Do(func() { close(callbackStarted) })
				<-releaseCallback
			case "unrelated":
				unrelatedDeliveredOnce.Do(func() { close(unrelatedDelivered) })
			}
		}
	})

	blockedSequenceId := NewId()
	incoming <- callbackBackpressurePackBytes(
		t,
		blockedSourceId,
		client.ClientId(),
		blockedSequenceId,
		0,
		true,
		"blocked",
	)
	select {
	case <-callbackStarted:
	case <-time.After(time.Second):
		t.Fatal("first receive callback did not start")
	}

	// The blocked sequence can retain one queued Pack. Its next Pack is the
	// deterministic full-handoff case, and it precedes the unrelated source in
	// the single shared receive pump.
	incoming <- callbackBackpressurePackBytes(
		t,
		blockedSourceId,
		client.ClientId(),
		blockedSequenceId,
		1,
		false,
		"queued",
	)
	incoming <- callbackBackpressurePackBytes(
		t,
		blockedSourceId,
		client.ClientId(),
		blockedSequenceId,
		2,
		false,
		"dropped",
	)
	incoming <- callbackBackpressurePackBytes(
		t,
		unrelatedSourceId,
		client.ClientId(),
		NewId(),
		0,
		true,
		"unrelated",
	)

	select {
	case <-unrelatedDelivered:
	case <-time.After(time.Second):
		t.Fatal("a full receive Pack handoff blocked an unrelated source")
	}
	if stats := client.ReceiveStats(); stats.PackHandoffDropCount != 1 ||
		stats.PackHandoffDropByteCount == 0 || stats.AckHandoffDropCount != 0 {
		t.Fatalf("full Pack handoff receive stats = %+v", stats)
	}
}

// TestClientReceiveAckHandoffDoesNotBlockUnrelatedSource fills one send
// sequence's ACK channel, then injects another ACK ahead of an unrelated Pack.
// ACK admission is part of the shared receive pump and must also use a zero
// timeout.
func TestClientReceiveAckHandoffDoesNotBlockUnrelatedSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	settings.BufferTimeout = time.Hour
	settings.ReceiveBufferSettings.SequenceBufferSize = 1
	settings.EncryptionSettings.Mode = EncryptionModeOff
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	ackSourceId := NewId()
	unrelatedSourceId := NewId()
	client.ContractManager().AddNoContractPeer(unrelatedSourceId)

	sequenceCtx, sequenceCancel := context.WithCancel(ctx)
	sequenceId := NewId()
	sequence := &SendSequence{
		ctx:         sequenceCtx,
		cancel:      sequenceCancel,
		destination: ackSourceId,
		sequenceId:  sequenceId,
		acks:        make(chan receiveAckMessage, 1),
	}
	sequence.acks <- receiveAckMessage{}
	client.sendBuffer.mutex.Lock()
	client.sendBuffer.sendSequencesBySequenceId[sequenceId] = sequence
	client.sendBuffer.mutex.Unlock()
	defer func() {
		client.sendBuffer.mutex.Lock()
		delete(client.sendBuffer.sendSequencesBySequenceId, sequenceId)
		client.sendBuffer.mutex.Unlock()
		sequenceCancel()
	}()

	incoming := make(chan []byte, 4)
	receiveTransport := NewReceiveGatewayTransport()
	client.RouteManager().UpdateTransport(receiveTransport, []Route{incoming})
	defer client.RouteManager().RemoveTransport(receiveTransport)

	unrelatedDelivered := make(chan struct{})
	var unrelatedDeliveredOnce sync.Once
	client.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, peer Peer) {
		for _, frame := range frames {
			if string(frame.MessageBytes) == "unrelated" {
				unrelatedDeliveredOnce.Do(func() { close(unrelatedDelivered) })
			}
		}
	})

	ackBytes, err := proto.Marshal(&protocol.TransferFrame{
		TransferPath: TransferPath{
			SourceId:      ackSourceId,
			DestinationId: client.ClientId(),
		}.ToProtobuf(),
		Ack: &protocol.Ack{
			MessageId:  NewId().Bytes(),
			SequenceId: sequenceId.Bytes(),
		},
	})
	if err != nil {
		t.Fatalf("marshal receive ACK: %v", err)
	}
	incoming <- ackBytes
	incoming <- callbackBackpressurePackBytes(
		t,
		unrelatedSourceId,
		client.ClientId(),
		NewId(),
		0,
		true,
		"unrelated",
	)

	select {
	case <-unrelatedDelivered:
	case <-time.After(time.Second):
		t.Fatal("a full receive ACK handoff blocked an unrelated source")
	}
	if stats := client.ReceiveStats(); stats.PackHandoffDropCount != 0 ||
		stats.PackHandoffDropByteCount != 0 || stats.AckHandoffDropCount != 1 ||
		stats.AckHandoffQueueFullCount != 1 || stats.AckHandoffMissCount != 0 {
		t.Fatalf("full ACK handoff receive stats = %+v", stats)
	}
}
