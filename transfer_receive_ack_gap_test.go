// Receive-sequence gap ack tests pin the two receiver-side behaviors that keep
// one relay-dropped item from costing the sender spurious resends and a full
// compression interval of head blocking: selective acks leave in ascending
// sequence order, and the ack-compression wait ends early once per snapshot
// when a hole becomes provable to the sender or a head ack fills one.
package connect

import (
	"context"
	mathrand "math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/protocol"
)

// ackGapTestSequence is a receive sequence whose ack writes land on one
// captured gateway route. The compression interval is long so that any write
// inside it must have come from the gap wake.
type ackGapTestSequence struct {
	receiveSequence *ReceiveSequence
	route           chan []byte
	settings        *ReceiveBufferSettings
}

func newAckGapTestSequence(
	t *testing.T,
	configure func(settings *ReceiveBufferSettings),
) *ackGapTestSequence {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	clientSettings := DefaultClientSettings()
	clientSettings.Log = NewNoopLogger()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), clientSettings)
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := client.CloseAndWait(closeCtx); err != nil {
			t.Errorf("join gap ack test client: %v", err)
		}
	})

	route := make(chan []byte, 512)
	gatewayTransport := NewSendGatewayTransport()
	client.RouteManager().UpdateTransport(gatewayTransport, []Route{route})
	t.Cleanup(func() { client.RouteManager().RemoveTransport(gatewayTransport) })

	settings := DefaultReceiveBufferSettings()
	settings.IdleTimeout = time.Hour
	settings.AckCompressTimeout = 2 * time.Second
	settings.WriteTimeout = time.Second
	if configure != nil {
		configure(settings)
	}

	receiveSequence := NewReceiveSequence(
		ctx,
		client,
		SourceId(NewId()),
		NewId(),
		sequenceTlsRoleServer,
		false,
		settings,
	)
	go receiveSequence.Run()
	t.Cleanup(func() {
		receiveSequence.Cancel()
		select {
		case <-receiveSequence.exit:
		case <-time.After(5 * time.Second):
			t.Error("gap ack test receive sequence did not exit")
		}
	})

	return &ackGapTestSequence{
		receiveSequence: receiveSequence,
		route:           route,
		settings:        settings,
	}
}

// readAck returns the message id of the next ack frame on the route, or false
// when none arrives within timeout.
func (self *ackGapTestSequence) readAck(t *testing.T, timeout time.Duration) (Id, bool) {
	deadline := time.After(timeout)
	for {
		select {
		case transferFrameBytes := <-self.route:
			transferFrame := &protocol.TransferFrame{}
			err := proto.Unmarshal(transferFrameBytes, transferFrame)
			MessagePoolReturn(transferFrameBytes)
			if err != nil {
				t.Fatalf("unmarshal ack transfer frame: %v", err)
			}
			if transferFrame.Ack == nil {
				continue
			}
			messageId, err := IdFromBytes(transferFrame.Ack.MessageId)
			if err != nil {
				t.Fatalf("ack message id: %v", err)
			}
			return messageId, true
		case <-deadline:
			return Id{}, false
		}
	}
}

// prime writes one idle-burst head ack, which the worker publishes at once,
// so that every later write inside the compression interval is rate limited.
func (self *ackGapTestSequence) prime(t *testing.T) {
	primeMessageId := NewId()
	self.receiveSequence.ackWindow.Update(sequenceAck{
		sequenceNumber: 0,
		messageId:      primeMessageId,
	})
	messageId, ok := self.readAck(t, 5*time.Second)
	if !ok {
		t.Fatal("idle-burst head ack waited for the compression interval")
	}
	if messageId != primeMessageId {
		t.Fatalf("idle-burst ack message id = %s, want %s", messageId, primeMessageId)
	}
}

func (self *ackGapTestSequence) updateSelective(sequenceNumber uint64) Id {
	messageId := NewId()
	self.receiveSequence.ackWindow.Update(sequenceAck{
		sequenceNumber: sequenceNumber,
		messageId:      messageId,
		selective:      true,
	})
	return messageId
}

func (self *ackGapTestSequence) updateHead(sequenceNumber uint64) Id {
	messageId := NewId()
	self.receiveSequence.ackWindow.Update(sequenceAck{
		sequenceNumber: sequenceNumber,
		messageId:      messageId,
	})
	return messageId
}

// Sixty-four selective acks inserted in shuffled order within one snapshot
// must be written in ascending sequence order. Map iteration passing this by
// chance is astronomically unlikely. The worker is held at its compression
// wait barrier until every ack is inserted so the snapshot is exactly one.
func TestReceiveSequenceSelectiveAcksWrittenInSequenceOrder(t *testing.T) {
	compressWaiting := make(chan struct{})
	releaseCompressWait := make(chan struct{})
	var compressWaitingOnce sync.Once
	sequence := newAckGapTestSequence(t, func(settings *ReceiveBufferSettings) {
		settings.beforeAckCompressWaitForTest = func(receiveSequenceId) {
			compressWaitingOnce.Do(func() { close(compressWaiting) })
			<-releaseCompressWait
		}
	})
	sequence.prime(t)

	const selectiveAckCount = 64
	sequenceNumbers := make([]uint64, 0, selectiveAckCount)
	for i := range selectiveAckCount {
		sequenceNumbers = append(sequenceNumbers, uint64(i+2))
	}
	mathrand.New(mathrand.NewSource(1)).Shuffle(len(sequenceNumbers), func(i int, j int) {
		sequenceNumbers[i], sequenceNumbers[j] = sequenceNumbers[j], sequenceNumbers[i]
	})

	messageIdSequenceNumbers := map[Id]uint64{}
	messageIdSequenceNumbers[sequence.updateSelective(sequenceNumbers[0])] = sequenceNumbers[0]
	select {
	case <-compressWaiting:
	case <-time.After(5 * time.Second):
		t.Fatal("ack worker did not enter the compression wait")
	}
	for _, sequenceNumber := range sequenceNumbers[1:] {
		messageIdSequenceNumbers[sequence.updateSelective(sequenceNumber)] = sequenceNumber
	}
	close(releaseCompressWait)

	written := make([]uint64, 0, selectiveAckCount)
	for range selectiveAckCount {
		messageId, ok := sequence.readAck(t, 5*time.Second)
		if !ok {
			t.Fatalf("selective acks written = %d, want %d", len(written), selectiveAckCount)
		}
		sequenceNumber, ok := messageIdSequenceNumbers[messageId]
		if !ok {
			t.Fatalf("unexpected ack message id %s", messageId)
		}
		written = append(written, sequenceNumber)
	}
	if !slices.IsSorted(written) {
		t.Fatalf("selective acks were not written in sequence order: %v", written)
	}
	if len(slices.Compact(slices.Clone(written))) != selectiveAckCount {
		t.Fatalf("selective acks were written with duplicates: %v", written)
	}
}

// Within a two second compression interval, the third pending selective ack
// makes the hole provable to the sender and must end the wait at once.
func TestReceiveSequenceGapWakeWritesProvableHoleEarly(t *testing.T) {
	sequence := newAckGapTestSequence(t, nil)
	if sequence.settings.AckGapWakeSelectiveCount != 3 {
		t.Fatalf("default AckGapWakeSelectiveCount = %d, want 3", sequence.settings.AckGapWakeSelectiveCount)
	}
	sequence.prime(t)

	messageIds := []Id{}
	for sequenceNumber := uint64(2); sequenceNumber <= 4; sequenceNumber += 1 {
		messageIds = append(messageIds, sequence.updateSelective(sequenceNumber))
	}
	startTime := time.Now()
	for _, wantMessageId := range messageIds {
		messageId, ok := sequence.readAck(t, 500*time.Millisecond)
		if !ok {
			t.Fatalf("provable hole acks waited for the compression interval (%s elapsed)", time.Since(startTime))
		}
		if messageId != wantMessageId {
			t.Fatalf("selective ack message id = %s, want %s", messageId, wantMessageId)
		}
	}
}

// After selective acks above a hole have been written, the head ack that
// fills the hole is the one the sender's flight is blocked on. It must not
// wait for the rest of the compression interval.
func TestReceiveSequenceGapWakeWritesHoleFillHeadAckEarly(t *testing.T) {
	sequence := newAckGapTestSequence(t, nil)
	sequence.prime(t)

	for sequenceNumber := uint64(3); sequenceNumber <= 5; sequenceNumber += 1 {
		sequence.updateSelective(sequenceNumber)
	}
	for range 3 {
		if _, ok := sequence.readAck(t, 5*time.Second); !ok {
			t.Fatal("selective acks above the hole were not written")
		}
	}

	headMessageId := sequence.updateHead(5)
	startTime := time.Now()
	messageId, ok := sequence.readAck(t, 500*time.Millisecond)
	if !ok {
		t.Fatalf("hole-fill head ack waited for the compression interval (%s elapsed)", time.Since(startTime))
	}
	if messageId != headMessageId {
		t.Fatalf("hole-fill ack message id = %s, want head %s", messageId, headMessageId)
	}
}

// The gap wake must not raise the steady in-order ack rate: in-order head
// acks alone, selective acks below the threshold, and a disabled threshold
// all keep the full compression interval.
func TestReceiveSequenceGapWakeGuards(t *testing.T) {
	cases := []struct {
		name                     string
		ackGapWakeSelectiveCount int
		update                   func(sequence *ackGapTestSequence)
	}{
		{
			name:                     "steady in-order head acks",
			ackGapWakeSelectiveCount: 3,
			update: func(sequence *ackGapTestSequence) {
				for sequenceNumber := uint64(1); sequenceNumber <= 8; sequenceNumber += 1 {
					sequence.updateHead(sequenceNumber)
				}
			},
		},
		{
			name:                     "selective acks below the threshold",
			ackGapWakeSelectiveCount: 3,
			update: func(sequence *ackGapTestSequence) {
				sequence.updateSelective(2)
				sequence.updateSelective(3)
			},
		},
		{
			name:                     "disabled threshold",
			ackGapWakeSelectiveCount: 0,
			update: func(sequence *ackGapTestSequence) {
				for sequenceNumber := uint64(3); sequenceNumber <= 10; sequenceNumber += 1 {
					sequence.updateSelective(sequenceNumber)
				}
				sequence.updateHead(1)
			},
		},
	}
	for _, c := range cases {
		sequence := newAckGapTestSequence(t, func(settings *ReceiveBufferSettings) {
			settings.AckGapWakeSelectiveCount = c.ackGapWakeSelectiveCount
		})
		sequence.prime(t)
		c.update(sequence)
		if messageId, ok := sequence.readAck(t, 300*time.Millisecond); ok {
			t.Errorf("%s: ack %s was written inside the compression interval", c.name, messageId)
		}
	}
}

// The window signals the gap wake at most once per reset snapshot, only when
// the pending selective acks reach the threshold or a head advances under
// outstanding selective acks, and never once the head has passed them all.
func TestSequenceAckWindowGapWakeOncePerSnapshot(t *testing.T) {
	window := newSequenceAckWindowWithGapWake(3)
	signaled := func() bool {
		select {
		case <-window.GapNotify():
			return true
		default:
			return false
		}
	}

	window.Update(sequenceAck{sequenceNumber: 0, messageId: NewId()})
	window.Update(sequenceAck{sequenceNumber: 2, messageId: NewId(), selective: true})
	window.Update(sequenceAck{sequenceNumber: 3, messageId: NewId(), selective: true})
	if signaled() {
		t.Fatal("two selective acks signaled the gap wake below the threshold")
	}
	window.Update(sequenceAck{sequenceNumber: 4, messageId: NewId(), selective: true})
	if !signaled() {
		t.Fatal("third selective ack did not signal the gap wake")
	}
	window.Update(sequenceAck{sequenceNumber: 5, messageId: NewId(), selective: true})
	window.Update(sequenceAck{sequenceNumber: 6, messageId: NewId(), selective: true})
	if signaled() {
		t.Fatal("gap wake signaled twice in one snapshot")
	}
	window.Update(sequenceAck{sequenceNumber: 7, messageId: NewId(), selective: true})
	window.Snapshot(true)
	if signaled() {
		t.Fatal("reset snapshot left a stale gap wake token")
	}

	// pending count restarts per snapshot
	window.Update(sequenceAck{sequenceNumber: 8, messageId: NewId(), selective: true})
	if signaled() {
		t.Fatal("one selective ack after reset signaled the gap wake")
	}
	window.Snapshot(true)

	// a head advance under outstanding (already written) selective acks
	window.Update(sequenceAck{sequenceNumber: 1, messageId: NewId()})
	if !signaled() {
		t.Fatal("head advance under outstanding selective acks did not signal the gap wake")
	}
	window.Snapshot(true)
	window.Update(sequenceAck{sequenceNumber: 8, messageId: NewId()})
	if !signaled() {
		t.Fatal("hole-fill head advance did not signal the gap wake")
	}
	window.Snapshot(true)

	// the head has passed every selective ack: steady state again
	window.Update(sequenceAck{sequenceNumber: 9, messageId: NewId()})
	window.Update(sequenceAck{sequenceNumber: 10, messageId: NewId()})
	if signaled() {
		t.Fatal("in-order head acks signaled the gap wake")
	}
	// a late ack below the head is a head resend, not a hole event
	window.Update(sequenceAck{sequenceNumber: 4, messageId: NewId()})
	if signaled() {
		t.Fatal("late ack below the head signaled the gap wake")
	}
}

func TestSequenceAckWindowGapWakeDisabled(t *testing.T) {
	window := newSequenceAckWindow()
	window.Update(sequenceAck{sequenceNumber: 0, messageId: NewId()})
	for sequenceNumber := uint64(2); sequenceNumber <= 10; sequenceNumber += 1 {
		window.Update(sequenceAck{sequenceNumber: sequenceNumber, messageId: NewId(), selective: true})
	}
	window.Update(sequenceAck{sequenceNumber: 10, messageId: NewId()})
	select {
	case <-window.GapNotify():
		t.Fatal("disabled gap wake signaled")
	default:
	}
}

func TestSequenceAckWindowGapWakeSteadyStateDoesNotAllocate(t *testing.T) {
	window := newSequenceAckWindowWithGapWake(3)
	ack := sequenceAck{
		sequenceNumber: 0,
		messageId:      NewId(),
		tag:            sequenceTag{sendTime: 1_700_000_000_000, set: true},
	}
	allocs := testing.AllocsPerRun(1000, func() {
		ack.sequenceNumber += 1
		window.Update(ack)
		window.Snapshot(true)
	})
	if allocs != 0 {
		t.Fatalf("gap-wake ack update + reset allocated %.0f times, want 0", allocs)
	}
}
