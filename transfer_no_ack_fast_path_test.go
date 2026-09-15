package connect

// THROUGHPUTFIX §38.12's first stage, pinned as the user's contract: a
// no-acknowledgement Pack is never queued or dropped behind a full retransmit
// buffer it will never occupy. It is written first, on the caller's goroutine,
// by one non-blocking try at the writer; queued second, with the caller's whole
// timeout, only if no route took it; dropped third, at the deadline, counted.
//
// Every row here is deterministic in the strict sense. The retransmit buffer is
// full by a barrier — the loop's own published capacity flag — rather than by
// outpacing anything; the writer is full because its route has no room, not
// because something raced to fill it; and the assertions are on the counters
// the tree carries and on the timeout the pack carries into admission, read
// through a per-site hook, never on how long anything took. No send loop runs
// in any of them: the stage under test is the one that runs before the loop.

import (
	"context"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// a sequence a client's real send path reaches, with a published fast path:
// an acknowledged contract, a writer over `route`, and the sequence registered
// in the client's send buffer under the id the client computes for it
type noAckFastPathHarness struct {
	client        *Client
	sequence      *SendSequence
	destinationId Id
	contract      *sequenceContract
	route         chan []byte
	// what the caller stage reported through the hook, last call
	attempted bool
	written   bool
	admission time.Duration
	hooked    int
}

func newNoAckFastPathHarness(t *testing.T, ctx context.Context, routeCapacity int) *noAckFastPathHarness {
	t.Helper()
	client, sequence, destinationId, contract := newSendNoContractHarness(t, ctx)
	harness := &noAckFastPathHarness{
		client:        client,
		sequence:      sequence,
		destinationId: destinationId,
		contract:      contract,
		route:         make(chan []byte, routeCapacity),
	}
	sequence.sendBuffer = client.sendBuffer
	client.sendBuffer.afterNoAckFastPathForTest = func(_ sendSequenceId, attempted bool, written bool, admission time.Duration) {
		harness.attempted, harness.written, harness.admission = attempted, written, admission
		harness.hooked += 1
	}
	t.Cleanup(func() { client.sendBuffer.afterNoAckFastPathForTest = nil })
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{harness.route},
	)
	// the writer, opened where the loop opens it; opening publishes
	sequence.openContractMultiRouteWriter()
	if sequence.noAckFastPath.Load() == nil {
		t.Fatal("the fixture published no fast path, so nothing here reads the stage")
	}
	// registered so the client's own send path finds it
	id := sendSequenceId{Destination: destinationId, EncryptionRole: sequenceTlsRoleClient}
	client.sendBuffer.mutex.Lock()
	client.sendBuffer.sendSequences[id] = sequence
	client.sendBuffer.wireSendSequences[id.wireId()] = sequence
	client.sendBuffer.mutex.Unlock()
	t.Cleanup(func() {
		client.sendBuffer.mutex.Lock()
		if client.sendBuffer.sendSequences[id] == sequence {
			delete(client.sendBuffer.sendSequences, id)
		}
		if client.sendBuffer.wireSendSequences[id.wireId()] == sequence {
			delete(client.sendBuffer.wireSendSequences, id.wireId())
		}
		client.sendBuffer.mutex.Unlock()
		harness.drainQueue()
		for {
			select {
			case transferFrameBytes := <-harness.route:
				MessagePoolReturn(transferFrameBytes)
			default:
				return
			}
		}
	})
	return harness
}

func noAckFastPathTestFrame(t *testing.T) *protocol.Frame {
	t.Helper()
	return RequireToFrameWithDefaultProtocolVersion(&protocol.SimpleMessage{Content: "no-ack fast path"})
}

// The barrier: the loop has published that the resend queue cannot take a
// pack, and the pre-write queue is full to its last slot. A reliable pack is
// refused on the spot, which is what proves the barrier stands.
func (self *noAckFastPathHarness) raiseBarrier(t *testing.T) {
	t.Helper()
	self.sequence.resendCapacityUnavailable.Store(true)
	for len(self.sequence.packs) < cap(self.sequence.packs) {
		self.sequence.packs <- &SendPack{Ctx: context.Background()}
	}
	frame := noAckFastPathTestFrame(t)
	admitted, _ := self.client.SendWithTimeoutDetailed(frame, self.destinationId, nil, 0)
	if admitted {
		t.Fatal("a reliable pack was admitted through the barrier, so the retransmit buffer is not full and this row reads nothing")
	}
	MessagePoolReturn(frame.MessageBytes)
}

func (self *noAckFastPathHarness) drainQueue() {
	for {
		select {
		case queued := <-self.sequence.packs:
			queued.returnFrames()
		default:
			return
		}
	}
}

// the one frame the route holds, decoded; fails if there is none
func (self *noAckFastPathHarness) takeWrittenPack(t *testing.T) *protocol.Pack {
	t.Helper()
	select {
	case transferFrameBytes := <-self.route:
		pack := decodeCompactContractTestPack(t, transferFrameBytes)
		MessagePoolReturn(transferFrameBytes)
		return pack
	default:
		t.Fatal("no frame reached the route")
		return nil
	}
}

// The user's contract, and the row that matters most: with the retransmit
// buffer full by a barrier, a no-acknowledgement pack still goes straight
// through — neither delayed behind the queue nor discarded — and the counters
// say so: offered equals written, nothing refused, nothing discarded, and the
// write was the fast path's rather than a loop's, because no loop is running.
//
// The fail-before: on a tree without the caller stage the same pack sits in
// the pre-write queue behind the barrier, or is refused by it, and written
// stays at zero.
func TestANoAckPackGoesStraightThroughAFullRetransmitBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := newNoAckFastPathHarness(t, ctx, 1)
	harness.raiseBarrier(t)

	frame := noAckFastPathTestFrame(t)
	admitted, err := harness.client.SendWithTimeoutDetailed(frame, harness.destinationId, nil, 0, NoAck())
	if err != nil {
		t.Fatalf("the no-acknowledgement pack returned an error: %v", err)
	}
	if !admitted {
		MessagePoolReturn(frame.MessageBytes)
		t.Fatal("a no-acknowledgement pack was refused behind a full retransmit buffer it will never occupy")
	}
	stats := harness.client.ReceiveStats()
	if stats.SendNoAckOfferedCount != 1 || stats.SendNoAckWriteCount != 1 ||
		stats.SendNoAckRefusedCount != 0 || stats.SendNoAckDiscardCount != 0 {
		t.Errorf(
			"offered %d written %d refused %d discarded %d, want 1 1 0 0; a forwarder discarding traffic because of a reliability buffer it does not use has to show up here rather than as an inference",
			stats.SendNoAckOfferedCount, stats.SendNoAckWriteCount,
			stats.SendNoAckRefusedCount, stats.SendNoAckDiscardCount,
		)
	}
	if stats.SendNoAckFastPathWriteCount != 1 {
		t.Errorf(
			"%d fast-path writes; with no loop running the only way to the wire is the caller stage, so anything but one means the pack was queued",
			stats.SendNoAckFastPathWriteCount,
		)
	}
	pack := harness.takeWrittenPack(t)
	if !pack.GetNack() {
		t.Error("the written pack asks for an acknowledgement")
	}
	if pack.GetHead() || pack.GetSequenceNumber() != 0 {
		t.Error("the written pack took a place in the ordered sequence, which is the queue it must never enter")
	}
	if contractId, err := IdFromBytes(pack.GetContractId()); err != nil || contractId != harness.contract.contractId {
		t.Error("the written pack does not carry the contract it was charged to")
	}
	if !harness.attempted || !harness.written {
		t.Errorf("the stage reported attempted %t written %t", harness.attempted, harness.written)
	}
}

// Timeout above zero, the write fails, and no time remains: admission is still
// tried, at zero, rather than not at all. And the write consumed none of the
// deadline — the deadline the pack carries is the one it entered with.
func TestANoAckPackWithNoTimeLeftStillTriesTheQueueAtZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// a route with no room, so the immediate try fails
	harness := newNoAckFastPathHarness(t, ctx, 0)

	// the pack entered with an hour and spent it all before reaching the
	// stage: the deadline it recorded has passed
	expired := time.Now().Add(-time.Second)
	pack := &SendPack{
		TransferOptions:  TransferOptions{Ack: false},
		Frame:            noAckFastPathTestFrame(t),
		Destination:      harness.destinationId,
		MessageByteCount: 16,
		Ctx:              ctx,
		deadline:         expired,
	}
	admitted, err := harness.sequence.Pack(pack, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !harness.attempted || harness.written {
		t.Fatalf("the stage reported attempted %t written %t against a route with no room", harness.attempted, harness.written)
	}
	if harness.admission != 0 {
		t.Errorf(
			"the pack went to admission with %s left; with its deadline passed it has nothing left and must still be tried at zero rather than blocking for the hour it was offered with",
			harness.admission,
		)
	}
	if !admitted {
		t.Fatal("admission at zero refused a pack the queue had room for")
	}
	if !pack.deadline.Equal(expired) {
		t.Errorf("the failed write moved the deadline from %s to %s", expired, pack.deadline)
	}
	harness.drainQueue()

	// and with the queue full too, the try at zero returns at once rather
	// than waiting: refused, not blocked, which is the third stage
	for len(harness.sequence.packs) < cap(harness.sequence.packs) {
		harness.sequence.packs <- &SendPack{Ctx: context.Background()}
	}
	full := &SendPack{
		TransferOptions:  TransferOptions{Ack: false},
		Frame:            noAckFastPathTestFrame(t),
		Destination:      harness.destinationId,
		MessageByteCount: 16,
		Ctx:              ctx,
		deadline:         expired,
	}
	admitted, err = harness.sequence.Pack(full, time.Hour)
	MessagePoolReturn(full.Frame.MessageBytes)
	if err != nil {
		t.Fatal(err)
	}
	if admitted {
		t.Fatal("a full queue admitted a pack")
	}
	if harness.admission != 0 {
		t.Errorf("the refused pack was admitted with %s rather than at zero", harness.admission)
	}
}

// Timeout above zero with time remaining: the write is at zero and admission
// gets the whole timeout, because the write consumed none of it.
func TestANoAckPackWithTimeLeftAdmitsWithAllOfIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := newNoAckFastPathHarness(t, ctx, 0)

	deadline := time.Now().Add(time.Hour)
	pack := &SendPack{
		TransferOptions:  TransferOptions{Ack: false},
		Frame:            noAckFastPathTestFrame(t),
		Destination:      harness.destinationId,
		MessageByteCount: 16,
		Ctx:              ctx,
		deadline:         deadline,
	}
	admitted, err := harness.sequence.Pack(pack, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if !admitted {
		t.Fatal("the queue refused a pack it had room for")
	}
	if !harness.attempted || harness.written {
		t.Fatalf("the stage reported attempted %t written %t against a route with no room", harness.attempted, harness.written)
	}
	if harness.admission != time.Hour {
		t.Errorf("the pack went to admission with %s of its hour; a blocked write must leave the deadline intact for the queue", harness.admission)
	}
	if !pack.deadline.Equal(deadline) {
		t.Errorf("the failed write moved the deadline from %s to %s", deadline, pack.deadline)
	}
}

// Timeout zero: writes at zero and admits at zero. With room on the route the
// pack is written and never queued; with none it is admitted at zero.
func TestANoAckPackWithAZeroTimeoutWritesAtZeroAndAdmitsAtZero(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := newNoAckFastPathHarness(t, ctx, 1)

	written := &SendPack{
		TransferOptions:  TransferOptions{Ack: false},
		Frame:            noAckFastPathTestFrame(t),
		Destination:      harness.destinationId,
		MessageByteCount: 16,
		Ctx:              ctx,
	}
	admitted, err := harness.sequence.Pack(written, 0)
	if err != nil || !admitted {
		t.Fatalf("a pack with room on the route was not written: admitted %t err %v", admitted, err)
	}
	if !harness.written {
		t.Fatal("the stage did not write a pack the route had room for")
	}
	if len(harness.sequence.packs) != 0 {
		t.Fatal("a written pack was also queued")
	}
	harness.takeWrittenPack(t)

	// the route is now full; the second try fails at zero and admits at zero
	harness.route <- MessagePoolCopy([]byte{0})
	queued := &SendPack{
		TransferOptions:  TransferOptions{Ack: false},
		Frame:            noAckFastPathTestFrame(t),
		Destination:      harness.destinationId,
		MessageByteCount: 16,
		Ctx:              ctx,
	}
	admitted, err = harness.sequence.Pack(queued, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !harness.attempted || harness.written {
		t.Fatalf("the stage reported attempted %t written %t against a full route", harness.attempted, harness.written)
	}
	if harness.admission != 0 {
		t.Errorf("a zero timeout went to admission as %s", harness.admission)
	}
	if !admitted {
		t.Fatal("admission at zero refused a pack the queue had room for")
	}
	if !queued.deadline.IsZero() {
		t.Error("a zero timeout acquired a deadline")
	}
}

// Negative timeout: writes at zero and admits with the negative timeout. The
// caller is willing to wait forever, and it waits on the queue — which is on
// the sequence goroutine — never on a write in its own goroutine.
func TestANoAckPackWithANegativeTimeoutWritesAtZeroAndAdmitsNegative(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := newNoAckFastPathHarness(t, ctx, 0)

	pack := &SendPack{
		TransferOptions:  TransferOptions{Ack: false},
		Frame:            noAckFastPathTestFrame(t),
		Destination:      harness.destinationId,
		MessageByteCount: 16,
		Ctx:              ctx,
	}
	admitted, err := harness.sequence.Pack(pack, -1)
	if err != nil {
		t.Fatal(err)
	}
	if !admitted {
		t.Fatal("the queue refused a pack it had room for")
	}
	if !harness.attempted || harness.written {
		t.Fatalf("the stage reported attempted %t written %t against a route with no room", harness.attempted, harness.written)
	}
	if harness.admission >= 0 {
		t.Errorf("a negative timeout went to admission as %s; the caller asked to wait on the queue without bound", harness.admission)
	}
	if !pack.deadline.IsZero() {
		t.Error("a negative timeout acquired a deadline")
	}
}

// The tear this shape has to rule out: a contract switch published between a
// caller's read and its write. The write is attributed to the contract the
// caller read, on the wire and in the loop's accounting, and the contract the
// sequence switched to is untouched.
func TestAFastPathWriteIsChargedToTheContractItRead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := newNoAckFastPathHarness(t, ctx, 1)
	sequence := harness.sequence
	first := harness.contract

	pack := &SendPack{
		TransferOptions:  TransferOptions{Ack: false},
		Frame:            noAckFastPathTestFrame(t),
		Destination:      harness.destinationId,
		MessageByteCount: 16,
		Ctx:              ctx,
	}
	byteCount := ByteCount(len(pack.Frame.MessageBytes))

	// the caller reads
	read := sequence.readNoAckFastPath(pack)
	if read == nil || read.contract != first {
		t.Fatal("the caller did not read a snapshot of the first contract")
	}

	// the loop switches under it and publishes
	second := newContractAheadTestContract(t, harness.client, harness.destinationId)
	second.minUpdateByteCount = 0
	sequence.setContract(second, sequence.contractMetadata().generation)
	sequence.setContractAcked(second, true)
	if published := sequence.noAckFastPath.Load(); published == read || published.contract != second {
		t.Fatal("the switch did not publish a snapshot of the second contract")
	}

	// the caller writes with what it read
	if !sequence.writeNoAckFastPath(read, pack) {
		t.Fatal("the write against the snapshot the caller read failed with room on the route")
	}
	written := harness.takeWrittenPack(t)
	if contractId, err := IdFromBytes(written.GetContractId()); err != nil || contractId != first.contractId {
		t.Error("the wire names a contract other than the one the caller read")
	}

	// and the loop's accounting agrees with the wire
	firstBefore, secondBefore := first.ackedByteCount, second.ackedByteCount
	sequence.applyNoAckFastPathAccounting()
	if first.ackedByteCount != firstBefore+byteCount {
		t.Errorf(
			"the first contract was charged %d bytes for a %d byte write attributed to it",
			first.ackedByteCount-firstBefore, byteCount,
		)
	}
	if second.ackedByteCount != secondBefore {
		t.Errorf("the second contract was charged %d bytes it never carried", second.ackedByteCount-secondBefore)
	}
	if read.reservedByteCount.Load() != read.appliedByteCount {
		t.Errorf(
			"the snapshot the caller read has %d bytes reserved and %d applied; every reservation that was written has to be charged exactly once",
			read.reservedByteCount.Load(), read.appliedByteCount,
		)
	}
}

// The reservation is what keeps concurrent fast-path writes within the
// contract the loop published: a snapshot with room for one write takes one and
// refuses the next, and a refused write returns to the queue rather than
// overdrawing.
func TestTheFastPathReservesTheContractItWasPublishedWith(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := newNoAckFastPathHarness(t, ctx, 2)
	snapshot := harness.sequence.noAckFastPath.Load()
	frame := noAckFastPathTestFrame(t)
	byteCount := ByteCount(len(frame.MessageBytes))
	MessagePoolReturn(frame.MessageBytes)
	// room for exactly one
	snapshot.remainingByteCount.Store(int64(byteCount))

	first := &SendPack{
		TransferOptions:  TransferOptions{Ack: false},
		Frame:            noAckFastPathTestFrame(t),
		Destination:      harness.destinationId,
		MessageByteCount: byteCount,
		Ctx:              ctx,
	}
	if admitted, _ := harness.sequence.Pack(first, 0); !admitted || !harness.written {
		t.Fatal("the first write against a contract with room for it was not written")
	}
	secondPack := &SendPack{
		TransferOptions:  TransferOptions{Ack: false},
		Frame:            noAckFastPathTestFrame(t),
		Destination:      harness.destinationId,
		MessageByteCount: byteCount,
		Ctx:              ctx,
	}
	if admitted, _ := harness.sequence.Pack(secondPack, 0); !admitted {
		t.Fatal("the second pack was refused by the queue rather than by the reservation")
	}
	if harness.written {
		t.Fatal("the second write overdrew the contract the snapshot was published with")
	}
	if len(harness.route) != 1 {
		t.Errorf("%d frames on the route, want the one the reservation allowed", len(harness.route))
	}
}
