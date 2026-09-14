package connect

// SendNoContract mid-sequence drop (see `SendSequence.updateContract`): when
// the destination newly requires no contract, the active contract must be
// dropped from attribution so items sent WITHOUT a contract debit are never
// credited to it on ack — the historical over-credit "Bad accounting" panic.
// Two shapes:
//   - no pending data (unacked == 0): the contract closes immediately with
//     its true counts;
//   - pending data (unacked > 0): the contract parks in `openSendContracts`
//     and closes with its true counts once the in-flight items drain (ack).

import (
	"context"
	"testing"
)

// newSendNoContractHarness builds a client + directly-driven SendSequence
// with a hand-built active contract (mirroring `setContract` without the
// stats/session wiring), following the direct-drive pattern of
// TestReceiveContractSupersedeClosesStats.
func newSendNoContractHarness(t *testing.T, ctx context.Context) (client *Client, ss *SendSequence, destinationId Id, contract *sequenceContract) {
	t.Helper()

	client = NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	t.Cleanup(client.Cancel)

	destinationId = NewId()
	ss = NewSendSequence(
		ctx,
		client,
		nil,
		destinationId,
		MultiHopId{},
		false,
		false,
		false,
		sequenceTlsRoleClient,
		false,
		DefaultSendBufferSettings(),
	)

	contract = &sequenceContract{
		log:                        client.log,
		localId:                    NewId(),
		tag:                        "s",
		contractId:                 NewId(),
		transferByteCount:          ByteCount(100000),
		effectiveTransferByteCount: ByteCount(100000),
		// exact accounting: effective byte count == item byte count
		minUpdateByteCount: 0,
		path: TransferPath{
			SourceId:      client.ClientId(),
			DestinationId: destinationId,
		},
	}
	ss.openSendContracts[contract.contractId] = contract
	ss.sendContract = contract
	ss.sendContractAcked = true
	return
}

// newContractSendItem models an in-flight item debited against `contractId`
// (nil for an item sent after the no-contract flip, which carries no
// contract attribution).
func newContractSendItem(contractId *Id, sequenceNumber uint64, messageByteCount ByteCount) *sendItem {
	return &sendItem{
		transferItem: transferItem{
			messageId:        NewId(),
			sequenceNumber:   sequenceNumber,
			messageByteCount: messageByteCount,
		},
		contractId: contractId,
	}
}

func TestSendNoContractMidSequenceDropDrained(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, ss, destinationId, contract := newSendNoContractHarness(t, ctx)
	contractManager := client.ContractManager()

	// two items debit the contract, and both ack BEFORE the flip: the
	// contract stays current and open (ackItem never closes the current
	// contract), with unacked == 0 and acked == 300
	AssertEqual(t, true, contract.update(100))
	AssertEqual(t, true, contract.update(200))
	itemA := newContractSendItem(&contract.contractId, 1, 100)
	itemB := newContractSendItem(&contract.contractId, 2, 200)
	ss.ackItem(itemA)
	ss.ackItem(itemB)
	AssertEqual(t, ByteCount(0), contract.unackedByteCount)
	AssertEqual(t, ByteCount(300), contract.ackedByteCount)
	AssertEqual(t, 1, len(ss.openSendContracts))
	AssertEqual(t, ByteCount(0), contractManager.LocalStats().ReceiveContractCloseByteCount)

	// the destination newly requires no contract: with nothing pending the
	// drained contract closes IMMEDIATELY with its true counts
	contractManager.AddNoContractPeer(destinationId)
	AssertEqual(t, true, ss.updateContract(50))
	AssertEqual(t, true, ss.sendContract == nil)
	AssertEqual(t, 0, len(ss.openSendContracts))
	// the close carried (acked=300, unacked=0)
	AssertEqual(t, ByteCount(300), contractManager.LocalStats().ReceiveContractCloseByteCount)
	AssertEqual(t, ByteCount(300), contract.ackedByteCount)
	AssertEqual(t, ByteCount(0), contract.unackedByteCount)

	// post-flip items carry no contract id, so their acks never touch the
	// closed contract — the historical over-credit/"Bad accounting" shape
	ss.ackItem(newContractSendItem(nil, 3, 50))
	AssertEqual(t, ByteCount(300), contract.ackedByteCount)
	AssertEqual(t, ByteCount(0), contract.unackedByteCount)

	// subsequent sends keep flowing contract-free
	AssertEqual(t, true, ss.updateContract(75))
	AssertEqual(t, true, ss.sendContract == nil)
	AssertEqual(t, 0, len(ss.openSendContracts))
}

func TestSendNoContractMidSequenceDropWithUnacked(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, ss, destinationId, contract := newSendNoContractHarness(t, ctx)
	contractManager := client.ContractManager()

	// two items debit the contract and are still in flight (unacked == 300)
	AssertEqual(t, true, contract.update(100))
	AssertEqual(t, true, contract.update(200))
	itemA := newContractSendItem(&contract.contractId, 1, 100)
	itemB := newContractSendItem(&contract.contractId, 2, 200)
	AssertEqual(t, ByteCount(300), contract.unackedByteCount)

	// the destination newly requires no contract: the contract has pending
	// data, so it PARKS in openSendContracts (no close with false counts)
	// and only detaches from attribution
	contractManager.AddNoContractPeer(destinationId)
	AssertEqual(t, true, ss.updateContract(50))
	AssertEqual(t, true, ss.sendContract == nil)
	AssertEqual(t, 1, len(ss.openSendContracts))
	AssertEqual(t, ByteCount(0), contractManager.LocalStats().ReceiveContractCloseByteCount)

	// an item sent AFTER the flip acks without touching the parked contract
	ss.ackItem(newContractSendItem(nil, 3, 50))
	AssertEqual(t, ByteCount(300), contract.unackedByteCount)
	AssertEqual(t, ByteCount(0), contract.ackedByteCount)

	// the first pre-flip item drains: still pending, still parked
	ss.ackItem(itemA)
	AssertEqual(t, ByteCount(200), contract.unackedByteCount)
	AssertEqual(t, ByteCount(100), contract.ackedByteCount)
	AssertEqual(t, 1, len(ss.openSendContracts))
	AssertEqual(t, ByteCount(0), contractManager.LocalStats().ReceiveContractCloseByteCount)

	// the last pre-flip item drains: the parked contract closes with its
	// TRUE counts (acked=300, unacked=0) — no over-credit, no panic
	ss.ackItem(itemB)
	AssertEqual(t, ByteCount(0), contract.unackedByteCount)
	AssertEqual(t, ByteCount(300), contract.ackedByteCount)
	AssertEqual(t, 0, len(ss.openSendContracts))
	AssertEqual(t, ByteCount(300), contractManager.LocalStats().ReceiveContractCloseByteCount)
}
