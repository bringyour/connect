// Receive rejection tests ensure deterministic contract failures cannot create
// resend-paced sequence recreation and logging loops.
package connect

import (
	"context"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestReceiveSequenceMalformedContractRejectsRetransmits verifies that every
// deterministic contract decode failure asks the owning buffer for a tombstone.
func TestReceiveSequenceMalformedContractRejectsRetransmits(t *testing.T) {
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
	defer sequence.Close()
	sequence.peerAudit = NewSequencePeerAudit(
		client,
		source,
		settings.ReceiveBufferSettings.MaxPeerAuditDuration,
	)
	err := sequence.registerContracts(&receiveItem{
		contractFrame: &protocol.Frame{
			MessageType:  protocol.MessageType_TransferContract,
			MessageBytes: []byte{0xff},
		},
	})
	if err == nil {
		t.Fatal("malformed contract was accepted")
	}
	if !sequence.rejectRetransmits {
		t.Fatal("malformed contract did not mark its sequence for rejection")
	}
}

// TestReceiveBufferRejectedSequenceDropsRetransmits verifies that equal and
// older sequence ids are discarded before constructing another receive worker.
func TestReceiveBufferRejectedSequenceDropsRetransmits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &Client{
		ctx: ctx,
		log: NewNoopLogger(),
	}
	buffer := NewReceiveBuffer(ctx, client, DefaultReceiveBufferSettings())
	source := SourceId(NewId())
	olderSequenceId := NewId()
	rejectedSequenceId := NewId()
	headKey := receiveSequenceHeadKey{
		Source:         source,
		EncryptionRole: sequenceTlsRoleServer,
	}
	buffer.mutex.Lock()
	buffer.rejectReceiveSequenceWithLock(headKey, rejectedSequenceId)
	buffer.mutex.Unlock()

	for _, sequenceId := range []Id{olderSequenceId, rejectedSequenceId} {
		success, err := buffer.Pack(
			&ReceivePack{
				Source:         source,
				SequenceId:     sequenceId,
				EncryptionRole: sequenceTlsRoleServer,
			},
			0,
		)
		if err != nil || !success {
			t.Fatalf(
				"rejected retransmit result = (%t, %v), want silent successful drop",
				success,
				err,
			)
		}
	}
	buffer.mutex.Lock()
	receiveSequenceCount := len(buffer.receiveSequences)
	buffer.mutex.Unlock()
	if receiveSequenceCount != 0 {
		t.Fatalf("rejected retransmits created %d receive sequences", receiveSequenceCount)
	}
}

// TestReceiveBufferNewerSequenceClearsRejection verifies that a peer can
// recover by reforming with a genuinely newer sequence and fresh contract.
func TestReceiveBufferNewerSequenceClearsRejection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buffer := NewReceiveBuffer(
		ctx,
		&Client{
			ctx: ctx,
			log: NewNoopLogger(),
		},
		DefaultReceiveBufferSettings(),
	)
	headKey := receiveSequenceHeadKey{
		Source:         SourceId(NewId()),
		EncryptionRole: sequenceTlsRoleClient,
	}
	rejectedSequenceId := NewId()
	newSequenceId := NewId()

	buffer.mutex.Lock()
	buffer.rejectReceiveSequenceWithLock(headKey, rejectedSequenceId)
	rejected := buffer.rejectReceiveSequenceRetransmitWithLock(
		headKey,
		newSequenceId,
	)
	_, remains := buffer.rejectedReceiveSequenceIds[headKey]
	buffer.mutex.Unlock()
	if rejected {
		t.Fatal("newer sequence was rejected")
	}
	if remains {
		t.Fatal("newer sequence did not clear the old rejection")
	}
}

// TestReceiveBufferRejectedSequenceTombstonesAreBounded verifies predictable
// memory even if many distinct peers present invalid contracts.
func TestReceiveBufferRejectedSequenceTombstonesAreBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	buffer := NewReceiveBuffer(
		ctx,
		&Client{
			ctx: ctx,
			log: NewNoopLogger(),
		},
		DefaultReceiveBufferSettings(),
	)
	if buffer.rejectedReceiveSequenceIds != nil ||
		buffer.rejectedReceiveSequenceOrder != nil {
		t.Fatal("unused rejection tombstones were allocated eagerly")
	}
	firstHeadKey := receiveSequenceHeadKey{
		Source:         SourceId(NewId()),
		EncryptionRole: sequenceTlsRoleServer,
	}
	buffer.mutex.Lock()
	buffer.rejectReceiveSequenceWithLock(firstHeadKey, NewId())
	if cap(buffer.rejectedReceiveSequenceOrder) !=
		rejectedReceiveSequenceCapacity {
		t.Fatalf(
			"rejected sequence order capacity = %d, want %d",
			cap(buffer.rejectedReceiveSequenceOrder),
			rejectedReceiveSequenceCapacity,
		)
	}
	for i := 1; i <= rejectedReceiveSequenceCapacity; i++ {
		buffer.rejectReceiveSequenceWithLock(
			receiveSequenceHeadKey{
				Source:         SourceId(NewId()),
				EncryptionRole: sequenceTlsRoleServer,
			},
			NewId(),
		)
	}
	tombstoneCount := len(buffer.rejectedReceiveSequenceIds)
	orderCount := len(buffer.rejectedReceiveSequenceOrder)
	_, firstRemains := buffer.rejectedReceiveSequenceIds[firstHeadKey]
	buffer.mutex.Unlock()

	if tombstoneCount != rejectedReceiveSequenceCapacity {
		t.Fatalf(
			"rejected sequence tombstones = %d, want %d",
			tombstoneCount,
			rejectedReceiveSequenceCapacity,
		)
	}
	if orderCount != rejectedReceiveSequenceCapacity {
		t.Fatalf(
			"rejected sequence order = %d, want %d",
			orderCount,
			rejectedReceiveSequenceCapacity,
		)
	}
	if firstRemains {
		t.Fatal("oldest rejected sequence was not evicted at capacity")
	}
}

// TestReceiveBufferInvalidContractTombstonesItsSequence exercises the complete
// receive-worker cleanup path that records the rejection.
func TestReceiveBufferInvalidContractTombstonesItsSequence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	source := SourceId(NewId())
	sequenceId := NewId()
	contractFrame, err := ToFrame(
		&protocol.Contract{
			StoredContractBytes: []byte{1},
			StoredContractHmac:  []byte{2},
			ProvideMode:         protocol.ProvideMode_Stream,
		},
		DefaultProtocolVersion,
	)
	if err != nil {
		t.Fatalf("encode invalid contract: %v", err)
	}
	receivePack := &ReceivePack{
		Source:     source,
		SequenceId: sequenceId,
		Pack: &protocol.Pack{
			MessageId:      NewId().Bytes(),
			SequenceId:     sequenceId.Bytes(),
			SequenceNumber: 0,
			Head:           true,
			ContractFrame:  contractFrame,
		},
		ReceiveCallback: func(
			source TransferPath,
			frames []*protocol.Frame,
			peer Peer,
		) {
			t.Error("invalid contract reached receive callback")
		},
		Unwrapped:      true,
		EncryptionRole: sequenceTlsRoleServer,
	}
	success, err := client.receiveBuffer.Pack(receivePack, time.Second)
	if err != nil || !success {
		t.Fatalf("queue invalid contract = (%t, %v), want worker admission", success, err)
	}

	headKey := receiveSequenceHeadKey{
		Source:         source,
		EncryptionRole: sequenceTlsRoleServer,
	}
	deadline := time.Now().Add(time.Second)
	for {
		client.receiveBuffer.mutex.Lock()
		rejectedSequenceId, rejected :=
			client.receiveBuffer.rejectedReceiveSequenceIds[headKey]
		client.receiveBuffer.mutex.Unlock()
		if rejected && rejectedSequenceId == sequenceId {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("invalid contract sequence was not tombstoned")
		}
		time.Sleep(time.Millisecond)
	}

	success, err = client.receiveBuffer.Pack(
		&ReceivePack{
			Source:         source,
			SequenceId:     sequenceId,
			EncryptionRole: sequenceTlsRoleServer,
		},
		0,
	)
	if err != nil || !success {
		t.Fatalf(
			"invalid-contract retransmit result = (%t, %v), want silent drop",
			success,
			err,
		)
	}
}
