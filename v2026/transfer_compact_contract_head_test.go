package connect

// This file pins negotiated compact contract heads and explicit state recovery.
// The compact form keeps ordinary tunnel packets on one H3 DATAGRAM without
// making receiver state or a reliable carrier authoritative.

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

func TestContractOpenAckErrorDoesNotPromoteContract(t *testing.T) {
	contract := &sequenceContract{}
	sequence := &SendSequence{sendContract: contract}
	callback := sequence.contractOpenAckCallback(contract)

	callback(errors.New("opening contract send failed"))
	if sequence.sendContractAcked {
		t.Fatal("failed contract-open send promoted the contract")
	}

	callback(nil)
	if !sequence.sendContractAcked {
		t.Fatal("successful contract-open Ack did not promote the contract")
	}

	replacement := &sequenceContract{}
	sequence.sendContract = replacement
	sequence.sendContractAcked = false
	callback(nil)
	if sequence.sendContractAcked {
		t.Fatal("late contract-open Ack promoted a replacement contract")
	}
}

// Decodes one routed frame borrowed from a test route.
func decodeCompactContractTestPack(t *testing.T, transferFrameBytes []byte) *protocol.Pack {
	t.Helper()
	var transferFrame protocol.TransferFrame
	if err := ProtoUnmarshal(transferFrameBytes, &transferFrame); err != nil {
		t.Fatalf("decode compact contract TransferFrame: %v", err)
	}
	pack := transferFrame.GetPack()
	if pack == nil {
		t.Fatal("compact contract route did not carry a Pack")
	}
	return pack
}

// Once the complete contract is acknowledged, a new head carries only its id.
// An explicit receiver recovery request reconstructs the complete contract.
// The size assertion is the low-bar regression: ordinary loss remains on one
// DATAGRAM instead of leaking to stream.
func TestNegotiatedContractHeadUsesCompactIdAndRequestRestoresContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, sequence, destinationId, contract := newSendNoContractHarness(t, ctx)
	// The shared direct-drive harness does not normally write a route, so it
	// leaves the owning buffer nil. This regression exercises the physical
	// writer and therefore needs the client's real buffer association.
	sequence.sendBuffer = client.sendBuffer
	contract.compactContractRecoverySupported = true
	contract.contract = &protocol.Contract{
		StoredContractBytes: bytes.Repeat([]byte{0x5a}, 2048),
	}
	route := make(chan []byte, 1)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)

	frame := RequireToFrameWithDefaultProtocolVersion(&protocol.SimpleMessage{
		Content: string(bytes.Repeat([]byte{'x'}, 128)),
	})
	sequence.sendRecord([]*protocol.Frame{frame}, sendAckRecord{}, noAckSendRecord{}, true, false)

	var compactBytes []byte
	select {
	case compactBytes = <-route:
	case <-ctx.Done():
		t.Fatalf("wait for compact contract head: %v", ctx.Err())
	}
	compactPack := decodeCompactContractTestPack(t, compactBytes)
	if !compactPack.GetHead() || compactPack.GetContractFrame() != nil ||
		!bytes.Equal(compactPack.GetContractId(), contract.contractId.Bytes()) ||
		len(compactPack.GetFrames()) != 1 {
		t.Fatalf("compact head=%+v", compactPack)
	}
	if !DefaultH3DatagramSettings().UseDatagram(len(compactBytes)) {
		t.Fatalf("compact head has %d bytes and missed one-DATAGRAM lane", len(compactBytes))
	}
	MessagePoolReturn(compactBytes)

	item := sequence.resendQueue.PeekFirst()
	if item == nil || !item.head || item.hasContractFrame || item.contractId == nil {
		t.Fatalf("compact resend item=%+v", item)
	}
	if sequence.receiveContractMissing(item.messageId, NewId()) {
		t.Fatal("mismatched contract recovery request was accepted")
	}
	if !sequence.receiveContractMissing(item.messageId, contract.contractId) {
		t.Fatal("matching contract recovery request was ignored")
	}
	item = sequence.resendQueue.PeekFirst()
	if item == nil || !item.hasContractFrame ||
		item.recoveryKind != sendRecoveryContractMissing || time.Now().Before(item.resendTime) {
		t.Fatalf("restored resend item=%+v", item)
	}
	fullPack := decodeCompactContractTestPack(t, item.transferFrameBytes)
	if fullPack.GetContractFrame() == nil ||
		!bytes.Equal(fullPack.GetContractId(), contract.contractId.Bytes()) {
		t.Fatalf("retry did not restore complete contract: %+v", fullPack)
	}
	if DefaultH3DatagramSettings().UseDatagram(len(item.transferFrameBytes)) {
		t.Fatalf(
			"full retry unexpectedly fit one DATAGRAM at %d bytes",
			len(item.transferFrameBytes),
		)
	}

	sequence.resendQueue.RemoveByMessageId(item.messageId)
	sequence.sendItems = nil
	item.messagePoolReturn()
}

// Cumulative progress can make an already-sent non-head packet the oldest
// outstanding item. Promoting it must use the same negotiated compact proof as
// a newly created head; attaching the full contract moves an MTU packet to H3
// stream and recreates head-of-line blocking on every ACK window.
func TestPromotedHeadUsesNegotiatedCompactContract(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, sequence, destinationId, contract := newSendNoContractHarness(t, ctx)
	sequence.sendBuffer = client.sendBuffer
	contract.compactContractRecoverySupported = true
	contract.contract = &protocol.Contract{
		StoredContractBytes: bytes.Repeat([]byte{0x4d}, 2048),
	}
	route := make(chan []byte, 2)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)

	for _, content := range []string{"window-head", "promoted-packet"} {
		frame := RequireToFrameWithDefaultProtocolVersion(&protocol.SimpleMessage{
			Content: content,
		})
		if !contract.update(MessageByteCount([]*protocol.Frame{frame})) {
			t.Fatalf("contract rejected promoted-head frame %q", content)
		}
		sequence.sendRecord(
			[]*protocol.Frame{frame},
			sendAckRecord{},
			noAckSendRecord{},
			true,
			false,
		)
	}
	for range 2 {
		select {
		case transferFrameBytes := <-route:
			MessagePoolReturn(transferFrameBytes)
		case <-ctx.Done():
			t.Fatalf("wait for promoted-head setup: %v", ctx.Err())
		}
	}
	if len(sequence.sendItems) != 2 || sequence.sendItems[1].head {
		t.Fatalf("promoted-head setup items=%+v", sequence.sendItems)
	}
	firstItem := sequence.sendItems[0]
	secondItem := sequence.sendItems[1]
	sequence.receiveAck(firstItem.messageId, false, sequenceTag{}, true)
	if len(sequence.sendItems) != 1 || sequence.sendItems[0] != secondItem {
		t.Fatalf("promoted-head remaining items=%+v", sequence.sendItems)
	}

	promotedBytes, hasContractFrame, err := sequence.setHead(secondItem, false)
	if err != nil {
		t.Fatalf("promote negotiated head: %v", err)
	}
	defer MessagePoolReturn(promotedBytes)
	promotedPack := decodeCompactContractTestPack(t, promotedBytes)
	if !promotedPack.GetHead() || hasContractFrame ||
		promotedPack.GetContractFrame() != nil ||
		!bytes.Equal(promotedPack.GetContractId(), contract.contractId.Bytes()) {
		t.Fatalf("promoted compact head=%+v", promotedPack)
	}
	if !DefaultH3DatagramSettings().UseDatagram(len(promotedBytes)) {
		t.Fatalf("promoted compact head has %d bytes and missed DATAGRAM", len(promotedBytes))
	}

	sequence.receiveAck(secondItem.messageId, false, sequenceTag{}, true)
}

// A legacy receiver never advertises missing-contract recovery. Even after the
// contract is otherwise acknowledged, its next head retains the complete proof
// and therefore cannot depend on a fallback timeout for interoperability.
func TestAcknowledgedContractHeadStaysFullWithoutRecoveryCapability(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, sequence, destinationId, contract := newSendNoContractHarness(t, ctx)
	sequence.sendBuffer = client.sendBuffer
	contract.contract = &protocol.Contract{
		StoredContractBytes: bytes.Repeat([]byte{0x6b}, 2048),
	}
	route := make(chan []byte, 1)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)

	frame := RequireToFrameWithDefaultProtocolVersion(&protocol.SimpleMessage{
		Content: "legacy-full-contract",
	})
	sequence.sendRecord(
		[]*protocol.Frame{frame},
		sendAckRecord{},
		noAckSendRecord{},
		true,
		false,
	)

	var fullBytes []byte
	select {
	case fullBytes = <-route:
	case <-ctx.Done():
		t.Fatalf("wait for legacy full contract head: %v", ctx.Err())
	}
	fullPack := decodeCompactContractTestPack(t, fullBytes)
	if !fullPack.GetHead() || fullPack.GetContractFrame() == nil ||
		len(fullPack.GetContractId()) != 0 || len(fullPack.GetFrames()) != 1 {
		t.Fatalf("legacy full head=%+v", fullPack)
	}
	if DefaultH3DatagramSettings().UseDatagram(len(fullBytes)) {
		t.Fatalf("legacy full head unexpectedly fit one DATAGRAM at %d bytes", len(fullBytes))
	}
	MessagePoolReturn(fullBytes)

	item := sequence.resendQueue.PeekFirst()
	if item == nil {
		t.Fatal("legacy full head missing resend item")
	}
	sequence.resendQueue.RemoveByMessageId(item.messageId)
	sequence.sendItems = nil
	item.messagePoolReturn()
}

// Capability is learned only from a delivery Ack that covers the complete
// current contract. This makes compact heads an end-to-end negotiated feature,
// rather than an assumption based on the local SDK version or carrier.
func TestFullContractDeliveryAckNegotiatesCompactRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, sequence, destinationId, contract := newSendNoContractHarness(t, ctx)
	sequence.sendBuffer = client.sendBuffer
	sequence.sendContractAcked = false
	contract.contract = &protocol.Contract{
		StoredContractBytes: bytes.Repeat([]byte{0x7c}, 128),
	}
	route := make(chan []byte, 1)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)

	ackCallbackCalled := false
	sequence.sendWithSetContract(
		nil,
		func(err error) {
			if err != nil {
				t.Errorf("full contract Ack callback: %v", err)
				return
			}
			ackCallbackCalled = true
			sequence.setContractAcked(contract, true)
		},
		true,
		true,
		false,
	)

	var fullBytes []byte
	select {
	case fullBytes = <-route:
	case <-ctx.Done():
		t.Fatalf("wait for negotiated full contract: %v", ctx.Err())
	}
	defer MessagePoolReturn(fullBytes)
	item := sequence.resendQueue.PeekFirst()
	if item == nil || !item.hasContractFrame {
		t.Fatalf("negotiated full contract item=%+v", item)
	}
	sequence.receiveAck(item.messageId, false, sequenceTag{}, true)
	if !ackCallbackCalled || !sequence.sendContractAcked ||
		!contract.compactContractRecoverySupported || len(sequence.sendItems) != 0 ||
		sequence.resendQueue.Len() != 0 {
		t.Fatalf(
			"negotiated state callback=%t contract_ack=%t recovery=%t pending=%d/%d",
			ackCallbackCalled,
			sequence.sendContractAcked,
			contract.compactContractRecoverySupported,
			len(sequence.sendItems),
			sequence.resendQueue.Len(),
		)
	}
}

// An unknown compact head is not delivery and does not advance the receive
// sequence. It emits a distinct recovery request that preserves plaintext
// mirroring without masquerading as a cumulative or selective Ack.
func TestReceiveUnknownCompactHeadRequestsContractWithoutAcknowledging(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	sequence := NewReceiveSequence(
		ctx,
		client,
		SourceId(NewId()),
		NewId(),
		sequenceTlsRoleServer,
		false,
		settings.ReceiveBufferSettings,
	)
	messageId := NewId()
	contractId := NewId()
	receivePack := &ReceivePack{
		Pack: &protocol.Pack{
			MessageId:      messageId.Bytes(),
			SequenceId:     sequence.sequenceId.Bytes(),
			SequenceNumber: 0,
			Head:           true,
			ContractId:     contractId.Bytes(),
		},
		MessageByteCount: 1,
		Unwrapped:        true,
	}
	received, err := sequence.receive(receivePack)
	if err != nil || received {
		t.Fatalf("unknown compact head receive=(%t, %v), want (false, nil)", received, err)
	}
	receivePack.messagePoolReturn()
	if sequence.nextSequenceNumber != 0 || sequence.receiveQueue.Len() != 0 {
		t.Fatalf(
			"unknown compact head advanced sequence=%d queue=%d",
			sequence.nextSequenceNumber,
			sequence.receiveQueue.Len(),
		)
	}

	snapshot := sequence.ackWindow.Snapshot(true)
	if snapshot.ackUpdateCount != 0 || len(snapshot.selectiveAcks) != 0 ||
		len(snapshot.contractMissingAcks) != 1 {
		t.Fatalf("unknown compact head Ack snapshot=%+v", snapshot)
	}
	request, ok := snapshot.contractMissingAcks[messageId]
	if !ok || request.messageId != messageId ||
		request.missingContractId != contractId || !request.contractMissing ||
		!request.compactContractRecoverySupported || !request.unwrapped {
		t.Fatalf("unknown compact head recovery request=%+v", request)
	}
	resetSnapshot := sequence.ackWindow.Snapshot(false)
	if resetSnapshot.ackUpdateCount != 0 || len(resetSnapshot.selectiveAcks) != 0 ||
		len(resetSnapshot.contractMissingAcks) != 0 {
		t.Fatalf("recovery request remained after reset: %+v", resetSnapshot)
	}
}

// The receiver resolves an acknowledged head's contract id only through its
// sequence-local set of contracts that were previously verified in full.
func TestReceiveAcknowledgedHeadUsesPreviouslyVerifiedContractId(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	source := SourceId(NewId())
	sequence := NewReceiveSequence(
		ctx,
		client,
		source,
		NewId(),
		sequenceTlsRoleServer,
		false,
		settings.ReceiveBufferSettings,
	)
	sequence.peerAudit = NewSequencePeerAudit(client, source, 0)
	contract := &sequenceContract{
		log:                        client.log,
		localId:                    NewId(),
		contractId:                 NewId(),
		transferByteCount:          1024,
		effectiveTransferByteCount: 1024,
		path: TransferPath{
			SourceId:      source.SourceId,
			DestinationId: client.ClientId(),
		},
	}
	if err := sequence.setContract(contract); err != nil {
		t.Fatalf("install verified receive contract: %v", err)
	}

	delivered := 0
	pack := &protocol.Pack{
		MessageId:      NewId().Bytes(),
		SequenceId:     sequence.sequenceId.Bytes(),
		SequenceNumber: 0,
		Head:           true,
		ContractId:     contract.contractId.Bytes(),
		Frames: []*protocol.Frame{{
			MessageType:  protocol.MessageType_TestSimpleMessage,
			MessageBytes: []byte("compact"),
		}},
	}
	received, err := sequence.receive(&ReceivePack{
		Pack:             pack,
		MessageByteCount: ByteCount(len("compact")),
		ReceiveCallback: func(_ TransferPath, frames []*protocol.Frame, _ Peer) {
			delivered += len(frames)
		},
	})
	if err != nil || !received {
		t.Fatalf("receive compact contract head=(%t, %v)", received, err)
	}
	sequence.flushDeliver()
	if delivered != 1 || contract.ackedByteCount != ByteCount(len("compact")) ||
		contract.unackedByteCount != 0 {
		t.Fatalf(
			"compact delivery=%d contract bytes=%d/%d, want 1/%d/0",
			delivered,
			contract.ackedByteCount,
			contract.unackedByteCount,
			len("compact"),
		)
	}
}
