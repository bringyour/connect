// Receive-sequence gap ack tests pin the receiver-side behaviors that keep one
// relay-dropped item from costing the sender spurious resends and a full
// compression interval of head blocking: selective acks leave in ascending
// sequence order, and the ack-compression wait ends early when a hole becomes
// provable to the sender or a head ack fills one.
//
// The early wake has an explicit budget, and these rows are its contract. Each
// of the two reasons (a provable hole, a filled hole) ends at most one wait per
// snapshot and at most one per AckCompressTimeout, measured from the early
// write it caused. A wake inside that interval is declined and its acks ride
// the timer. So a newly provable hole and a hole-fill head ack are each written
// early, a hole-fill right after a proof is written early (different reason),
// and a sustained hole under a continuous stream costs at most one extra write
// per interval rather than one write per three later acks.
//
// The worker rows run in a synctest bubble, so the two second compression
// interval is virtual: a wait that returns nothing costs no real time, and an
// early write is one that lands before the virtual clock moves.
package connect

import (
	"context"
	mathrand "math/rand"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
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

	route := make(chan []byte, 1024)
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

// readAcks reads exactly the acks for the given message ids, in order, each
// within timeout of the previous one.
func (self *ackGapTestSequence) readAcks(t *testing.T, timeout time.Duration, wantMessageIds []Id, what string) {
	t.Helper()
	startTime := time.Now()
	for i, wantMessageId := range wantMessageIds {
		messageId, ok := self.readAck(t, timeout)
		if !ok {
			t.Fatalf("%s: ack %d of %d was not written within %s (%s elapsed)", what, i+1, len(wantMessageIds), timeout, time.Since(startTime))
		}
		if messageId != wantMessageId {
			t.Fatalf("%s: ack %d message id = %s, want %s", what, i+1, messageId, wantMessageId)
		}
	}
}

// expectNoAck fails when an ack is written within timeout.
func (self *ackGapTestSequence) expectNoAck(t *testing.T, timeout time.Duration, what string) {
	t.Helper()
	if messageId, ok := self.readAck(t, timeout); ok {
		t.Fatalf("%s: ack %s was written inside the compression interval", what, messageId)
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

// updateSelectiveRun inserts selective acks for [first, first+count).
func (self *ackGapTestSequence) updateSelectiveRun(first uint64, count int) []Id {
	messageIds := make([]Id, 0, count)
	for sequenceNumber := first; sequenceNumber < first+uint64(count); sequenceNumber += 1 {
		messageIds = append(messageIds, self.updateSelective(sequenceNumber))
	}
	return messageIds
}

func (self *ackGapTestSequence) updateHead(sequenceNumber uint64) Id {
	messageId := NewId()
	self.updateHeadOf(sequenceNumber, messageId)
	return messageId
}

// updateHeadOf publishes the cumulative ack for one delivered item. As in
// the receive loop, a head ack carries the delivered item's own message id,
// which is how a selectively acked item that the head later passes is
// recognised.
func (self *ackGapTestSequence) updateHeadOf(sequenceNumber uint64, messageId Id) {
	self.receiveSequence.ackWindow.Update(sequenceAck{
		sequenceNumber: sequenceNumber,
		messageId:      messageId,
	})
}

// Sixty-four selective acks inserted in shuffled order within one snapshot
// must be written in ascending sequence order. Map iteration passing this by
// chance is astronomically unlikely. The worker is held at its compression
// wait barrier until every ack is inserted so the snapshot is exactly one.
func TestReceiveSequenceSelectiveAcksWrittenInSequenceOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
	})
}

// Within a two second compression interval, the third pending selective ack
// makes the hole provable to the sender and must end the wait at once.
func TestReceiveSequenceGapWakeWritesProvableHoleEarly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sequence := newAckGapTestSequence(t, nil)
		if sequence.settings.AckGapWakeSelectiveCount != 3 {
			t.Fatalf("default AckGapWakeSelectiveCount = %d, want 3", sequence.settings.AckGapWakeSelectiveCount)
		}
		sequence.prime(t)

		messageIds := sequence.updateSelectiveRun(2, 3)
		startTime := time.Now()
		sequence.readAcks(t, 500*time.Millisecond, messageIds, "provable hole")
		if elapsed := time.Since(startTime); elapsed != 0 {
			t.Fatalf("provable hole acks were written %s into the compression interval, want at once", elapsed)
		}
	})
}

// After selective acks above a hole have been written, the head ack that
// fills the hole is the one the sender's flight is blocked on. It must not
// wait for the rest of the compression interval, even though the provable
// hole ended a wait in this same interval: the two reasons are budgeted
// separately.
func TestReceiveSequenceGapWakeWritesHoleFillHeadAckEarly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sequence := newAckGapTestSequence(t, nil)
		sequence.prime(t)

		sequence.readAcks(t, 500*time.Millisecond, sequence.updateSelectiveRun(3, 3), "provable hole")

		headMessageId := sequence.updateHead(5)
		startTime := time.Now()
		sequence.readAcks(t, 500*time.Millisecond, []Id{headMessageId}, "hole-fill head")
		if elapsed := time.Since(startTime); elapsed != 0 {
			t.Fatalf("hole-fill head ack was written %s into the compression interval, want at once", elapsed)
		}
	})
}

// A sequence whose opening item is missing has no head at all while the
// selective acks above it are written. The first cumulative head ack is then
// the hole filling, and it must be written early like any other.
func TestReceiveSequenceGapWakeWritesOpeningHoleFillEarly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sequence := newAckGapTestSequence(t, nil)

		// the idle burst publishes the first selective ack at once; the next
		// three prove the hole
		sequence.readAcks(t, 5*time.Second, []Id{sequence.updateSelective(1)}, "idle-burst selective")
		sequence.readAcks(t, 500*time.Millisecond, sequence.updateSelectiveRun(2, 3), "provable opening hole")

		headMessageId := sequence.updateHead(0)
		startTime := time.Now()
		sequence.readAcks(t, 500*time.Millisecond, []Id{headMessageId}, "first head")
		if elapsed := time.Since(startTime); elapsed != 0 {
			t.Fatalf("first head ack was written %s into the compression interval, want at once", elapsed)
		}
	})
}

// The wake budget, step by step in virtual time. A second proof inside the
// interval is declined and rides the timer; once the interval has passed
// since the early write, a proof wakes again; a hole fill inside that
// interval wakes because its reason has not fired; a second hole fill inside
// the interval is declined.
func TestReceiveSequenceGapWakeCadenceContract(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		sequence := newAckGapTestSequence(t, nil)
		interval := sequence.settings.AckCompressTimeout
		sequence.prime(t)

		// the first proof wakes
		firstEarlyWrite := time.Now()
		sequence.readAcks(t, interval/4, sequence.updateSelectiveRun(2, 3), "first proof")

		// a second proof inside the interval is declined ...
		secondProof := sequence.updateSelectiveRun(5, 3)
		sequence.expectNoAck(t, interval/2, "second proof inside the interval")
		// ... and rides the timer, one interval after the early write
		sequence.readAcks(t, interval, secondProof, "second proof on the timer")
		if elapsed := time.Since(firstEarlyWrite); elapsed != interval {
			t.Fatalf("declined proof was written %s after the early write, want the %s interval", elapsed, interval)
		}

		// the interval has passed since the last early write: a proof wakes
		timerWrite := time.Now()
		sequence.readAcks(t, interval/4, sequence.updateSelectiveRun(8, 3), "proof after the interval")
		if elapsed := time.Since(timerWrite); elapsed != 0 {
			t.Fatalf("proof after the interval was written %s late, want at once", elapsed)
		}

		// a hole fill inside that interval is the other reason: it wakes
		sequence.readAcks(t, interval/4, []Id{sequence.updateHead(10)}, "hole fill")

		// a second hole fill inside the interval is declined and rides the
		// timer; the selective acks it absorbed are not written at all
		sequence.updateSelectiveRun(12, 2)
		secondFill := sequence.updateHead(13)
		sequence.expectNoAck(t, interval/2, "second hole fill inside the interval")
		sequence.readAcks(t, interval, []Id{secondFill}, "second hole fill on the timer")
		sequence.expectNoAck(t, interval/4, "absorbed selective acks")
	})
}

// A sustained hole under a continuous stream: one head is held and later
// items keep arriving in threes, each three a fresh proof. Without a budget
// every third ack ends a wait, one write per three packs. The contract is at
// most one early write per interval per reason, so with one reason active no
// half-open interval holds more than two writes (the timer's and one early),
// while the first proof is still written at once and nothing is lost.
func TestReceiveSequenceGapWakeSustainedHoleWritesAreBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var writeTimesLock sync.Mutex
		writeTimes := []time.Time{}
		sequence := newAckGapTestSequence(t, func(settings *ReceiveBufferSettings) {
			settings.afterAckWriteForTest = func(receiveSequenceId) {
				writeTimesLock.Lock()
				defer writeTimesLock.Unlock()
				writeTimes = append(writeTimes, time.Now())
			}
		})
		interval := sequence.settings.AckCompressTimeout
		const intervalCount = 4
		const triplesPerInterval = 20
		tripleSpacing := interval / triplesPerInterval

		startTime := time.Now()
		sequence.prime(t)

		messageIdSequenceNumbers := map[Id]uint64{}
		nextSequenceNumber := uint64(2)
		feedTriple := func() []Id {
			messageIds := sequence.updateSelectiveRun(nextSequenceNumber, 3)
			for i, messageId := range messageIds {
				messageIdSequenceNumbers[messageId] = nextSequenceNumber + uint64(i)
			}
			nextSequenceNumber += 3
			return messageIds
		}

		// the first proof is written at once
		sequence.readAcks(t, interval/4, feedTriple(), "first proof")
		if elapsed := time.Since(startTime); elapsed != 0 {
			t.Fatalf("first proof was written %s into the interval, want at once", elapsed)
		}

		// then a continuous stream of proofs for several intervals
		const tripleCount = intervalCount * triplesPerInterval
		for range tripleCount - 1 {
			time.Sleep(tripleSpacing)
			feedTriple()
		}

		// nothing is lost and the wire order is the sequence order
		written := make([]uint64, 0, 3*tripleCount)
		for range 3*tripleCount - 3 {
			messageId, ok := sequence.readAck(t, 2*interval)
			if !ok {
				t.Fatalf("selective acks written = %d, want %d", 3+len(written), 3*tripleCount)
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
		// the write hook runs after the route write; let the worker finish
		synctest.Wait()

		writeTimesLock.Lock()
		defer writeTimesLock.Unlock()
		buckets := map[int]int{}
		for _, writeTime := range writeTimes {
			buckets[int(writeTime.Sub(startTime)/interval)] += 1
		}
		for bucket, count := range buckets {
			if 2 < count {
				t.Errorf("interval %d had %d ack writes, want at most 2 (the timer's and one early); writes at %v", bucket, count, writeTimes)
			}
		}
		// the stream ran for intervalCount intervals plus the tail the timer
		// drains; the prime and the first early write share interval zero
		if maxWrites := 2*intervalCount + 2; maxWrites < len(writeTimes) {
			t.Errorf("a sustained hole cost %d ack writes over %d intervals, want at most %d; writes at %v", len(writeTimes), intervalCount, maxWrites, writeTimes)
		}
		if len(writeTimes) < intervalCount {
			t.Errorf("only %d ack writes over %d intervals of continuous acks", len(writeTimes), intervalCount)
		}
		t.Logf("%d ack writes for %d selective acks over %d intervals: %v", len(writeTimes), 3*tripleCount, intervalCount, buckets)
	})
}

// The gap wake must not raise the steady in-order ack rate: in-order head
// acks alone, selective acks below the threshold, a head that absorbed the
// pending selective acks followed by fewer than the threshold, and a
// disabled threshold all keep the full compression interval.
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
		t.Run(c.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				sequence := newAckGapTestSequence(t, func(settings *ReceiveBufferSettings) {
					settings.AckGapWakeSelectiveCount = c.ackGapWakeSelectiveCount
				})
				sequence.prime(t)
				c.update(sequence)
				sequence.expectNoAck(t, 300*time.Millisecond, c.name)
			})
		})
	}
}

// A head that absorbs pending selective acks leaves them in the live map
// until the snapshot filters them. If they still counted, two later acks
// would read as a proof in the same snapshot and its early write would spend
// the proof budget for the interval, so a genuine proof right after would be
// declined and wait out the timer. The worker is held at its compression
// wait barrier so the absorption and the two later acks land in one snapshot.
// The hole at 1 fills and items 1, 2 and 3 are delivered in turn, each with
// its own head ack, as the receive loop does.
func TestReceiveSequenceGapWakeHeadAbsorbedEvidenceDoesNotSpendTheProofBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		compressWaiting := make(chan struct{})
		releaseCompressWait := make(chan struct{})
		var compressWaitingOnce sync.Once
		sequence := newAckGapTestSequence(t, func(settings *ReceiveBufferSettings) {
			settings.beforeAckCompressWaitForTest = func(receiveSequenceId) {
				compressWaitingOnce.Do(func() {
					close(compressWaiting)
					<-releaseCompressWait
				})
			}
		})
		sequence.prime(t)

		held2 := sequence.updateSelective(2)
		select {
		case <-compressWaiting:
		case <-time.After(5 * time.Second):
			t.Fatal("ack worker did not enter the compression wait")
		}
		held3 := sequence.updateSelective(3)
		sequence.updateHead(1)
		sequence.updateHeadOf(2, held2)
		absorbingHead := held3
		sequence.updateHeadOf(3, held3)
		later := sequence.updateSelectiveRun(5, 2)
		close(releaseCompressWait)

		// the hole fill is written early, carrying the two later acks
		sequence.readAcks(t, 500*time.Millisecond, append([]Id{absorbingHead}, later...), "absorbing head")

		// a genuine proof in the same interval must still wake: the two
		// absorbed acks were not a proof and spent nothing
		sequence.readAcks(t, 500*time.Millisecond, sequence.updateSelectiveRun(7, 3), "proof after absorption")
	})
}

func gapWakeSignaled(window *sequenceAckWindow) bool {
	select {
	case <-window.GapNotify():
		return true
	default:
		return false
	}
}

// The window signals the gap wake at most once per reason per reset snapshot,
// only when the pending selective acks reach the threshold or a head advances
// under outstanding selective acks, and never once the head has passed them
// all.
func TestSequenceAckWindowGapWakeOncePerSnapshot(t *testing.T) {
	window := newSequenceAckWindowWithGapWake(3)

	window.Update(sequenceAck{sequenceNumber: 0, messageId: NewId()})
	window.Update(sequenceAck{sequenceNumber: 2, messageId: NewId(), selective: true})
	window.Update(sequenceAck{sequenceNumber: 3, messageId: NewId(), selective: true})
	if gapWakeSignaled(window) {
		t.Fatal("two selective acks signaled the gap wake below the threshold")
	}
	window.Update(sequenceAck{sequenceNumber: 4, messageId: NewId(), selective: true})
	if !gapWakeSignaled(window) {
		t.Fatal("third selective ack did not signal the gap wake")
	}
	if reasons := window.GapWakeReasons(); reasons != gapWakeHoleProvable {
		t.Fatalf("gap wake reasons = %b, want provable hole", reasons)
	}
	window.Update(sequenceAck{sequenceNumber: 5, messageId: NewId(), selective: true})
	window.Update(sequenceAck{sequenceNumber: 6, messageId: NewId(), selective: true})
	if gapWakeSignaled(window) {
		t.Fatal("provable hole signaled twice in one snapshot")
	}
	window.Update(sequenceAck{sequenceNumber: 7, messageId: NewId(), selective: true})
	window.Snapshot(true)
	if gapWakeSignaled(window) {
		t.Fatal("reset snapshot left a stale gap wake token")
	}
	if reasons := window.GapWakeReasons(); reasons != 0 {
		t.Fatalf("gap wake reasons after reset = %b, want none", reasons)
	}

	// pending count restarts per snapshot
	window.Update(sequenceAck{sequenceNumber: 8, messageId: NewId(), selective: true})
	if gapWakeSignaled(window) {
		t.Fatal("one selective ack after reset signaled the gap wake")
	}
	window.Snapshot(true)

	// a head advance under outstanding (already written) selective acks
	window.Update(sequenceAck{sequenceNumber: 1, messageId: NewId()})
	if !gapWakeSignaled(window) {
		t.Fatal("head advance under outstanding selective acks did not signal the gap wake")
	}
	if reasons := window.GapWakeReasons(); reasons != gapWakeHoleFilled {
		t.Fatalf("gap wake reasons = %b, want filled hole", reasons)
	}
	window.Snapshot(true)
	window.Update(sequenceAck{sequenceNumber: 8, messageId: NewId()})
	if !gapWakeSignaled(window) {
		t.Fatal("hole-fill head advance did not signal the gap wake")
	}
	window.Snapshot(true)

	// the head has passed every selective ack: steady state again
	window.Update(sequenceAck{sequenceNumber: 9, messageId: NewId()})
	window.Update(sequenceAck{sequenceNumber: 10, messageId: NewId()})
	if gapWakeSignaled(window) {
		t.Fatal("in-order head acks signaled the gap wake")
	}
	// a late ack below the head is a head resend, not a hole event
	window.Update(sequenceAck{sequenceNumber: 4, messageId: NewId()})
	if gapWakeSignaled(window) {
		t.Fatal("late ack below the head signaled the gap wake")
	}
}

// The two reasons are signaled independently within one snapshot: a hole
// fill after a proof sends a second token, so a consumer that declined the
// proof can still take the fill.
func TestSequenceAckWindowGapWakeSignalsEachReasonOncePerSnapshot(t *testing.T) {
	window := newSequenceAckWindowWithGapWake(3)
	window.Update(sequenceAck{sequenceNumber: 0, messageId: NewId()})
	for sequenceNumber := uint64(2); sequenceNumber <= 4; sequenceNumber += 1 {
		window.Update(sequenceAck{sequenceNumber: sequenceNumber, messageId: NewId(), selective: true})
	}
	if !gapWakeSignaled(window) {
		t.Fatal("provable hole did not signal")
	}
	window.Update(sequenceAck{sequenceNumber: 4, messageId: NewId()})
	if !gapWakeSignaled(window) {
		t.Fatal("hole fill after a consumed proof did not signal again")
	}
	if reasons := window.GapWakeReasons(); reasons != gapWakeHoleProvable|gapWakeHoleFilled {
		t.Fatalf("gap wake reasons = %b, want both", reasons)
	}
	// neither reason signals a second time in the snapshot
	for sequenceNumber := uint64(6); sequenceNumber <= 8; sequenceNumber += 1 {
		window.Update(sequenceAck{sequenceNumber: sequenceNumber, messageId: NewId(), selective: true})
	}
	window.Update(sequenceAck{sequenceNumber: 7, messageId: NewId()})
	if gapWakeSignaled(window) {
		t.Fatal("a reason signaled twice in one snapshot")
	}
	window.Snapshot(true)
	if reasons := window.GapWakeReasons(); reasons != 0 {
		t.Fatalf("gap wake reasons after reset = %b, want none", reasons)
	}
}

// With no head yet, selective acks above the missing opening item prove the
// hole, and the first head ack is the hole filling.
func TestSequenceAckWindowGapWakeOpeningHole(t *testing.T) {
	window := newSequenceAckWindowWithGapWake(3)
	for sequenceNumber := uint64(1); sequenceNumber <= 3; sequenceNumber += 1 {
		window.Update(sequenceAck{sequenceNumber: sequenceNumber, messageId: NewId(), selective: true})
	}
	if !gapWakeSignaled(window) {
		t.Fatal("selective acks with no head did not signal the provable hole")
	}
	window.Snapshot(true)

	window.Update(sequenceAck{sequenceNumber: 0, messageId: NewId()})
	if !gapWakeSignaled(window) {
		t.Fatal("first head ack under outstanding selective acks did not signal the hole fill")
	}
	if reasons := window.GapWakeReasons(); reasons != gapWakeHoleFilled {
		t.Fatalf("gap wake reasons = %b, want filled hole", reasons)
	}
	window.Snapshot(true)

	// a sequence whose first head ack arrives with no selective acks at all
	// is the ordinary case and signals nothing
	fresh := newSequenceAckWindowWithGapWake(3)
	fresh.Update(sequenceAck{sequenceNumber: 0, messageId: NewId()})
	if gapWakeSignaled(fresh) {
		t.Fatal("an ordinary first head ack signaled the gap wake")
	}
}

// The proof counts only selective acks above the current head. Entries a
// head absorbed stay in the live map until the snapshot filters them, and
// must not count; the count restarts with each snapshot, so evidence spread
// across snapshots does not accumulate. A delivered item's head ack carries
// that item's message id, as the receive loop's per-item acks do.
func TestSequenceAckWindowGapWakeCountsOnlyEvidenceAboveTheHead(t *testing.T) {
	selective := func(window *sequenceAckWindow, sequenceNumber uint64) Id {
		messageId := NewId()
		window.Update(sequenceAck{sequenceNumber: sequenceNumber, messageId: messageId, selective: true})
		return messageId
	}
	deliver := func(window *sequenceAckWindow, sequenceNumber uint64, messageId Id) {
		window.Update(sequenceAck{sequenceNumber: sequenceNumber, messageId: messageId})
	}
	t.Run("the hole fills and the head passes every pending ack", func(t *testing.T) {
		window := newSequenceAckWindowWithGapWake(3)
		deliver(window, 0, NewId())
		held2 := selective(window, 2)
		held3 := selective(window, 3)
		deliver(window, 1, NewId())
		if !gapWakeSignaled(window) {
			t.Fatal("the hole filling did not signal")
		}
		deliver(window, 2, held2)
		deliver(window, 3, held3)
		selective(window, 5)
		selective(window, 6)
		if gapWakeSignaled(window) {
			t.Fatal("two selective acks above the head plus two absorbed ones signaled a provable hole")
		}
		if reasons := window.GapWakeReasons(); reasons != gapWakeHoleFilled {
			t.Fatalf("gap wake reasons = %b, want filled hole only", reasons)
		}
		selective(window, 7)
		if !gapWakeSignaled(window) {
			t.Fatal("three selective acks above the head did not signal a provable hole")
		}
	})
	t.Run("the head passes some pending acks and stops under another", func(t *testing.T) {
		window := newSequenceAckWindowWithGapWake(3)
		deliver(window, 0, NewId())
		held2 := selective(window, 2)
		selective(window, 5)
		deliver(window, 1, NewId())
		deliver(window, 2, held2)
		deliver(window, 3, NewId())
		if !gapWakeSignaled(window) {
			t.Fatal("the hole filling did not signal")
		}
		selective(window, 6)
		if gapWakeSignaled(window) {
			t.Fatal("two selective acks above the head plus one absorbed signaled a provable hole")
		}
		selective(window, 7)
		if !gapWakeSignaled(window) {
			t.Fatal("three selective acks above the head did not signal a provable hole")
		}
	})
	t.Run("the head advances below everything pending and absorbs none", func(t *testing.T) {
		window := newSequenceAckWindowWithGapWake(3)
		deliver(window, 0, NewId())
		selective(window, 5)
		selective(window, 6)
		deliver(window, 1, NewId())
		deliver(window, 2, NewId())
		if !gapWakeSignaled(window) {
			t.Fatal("head advance under outstanding selective acks did not signal the hole fill")
		}
		selective(window, 7)
		if !gapWakeSignaled(window) {
			t.Fatal("three selective acks above a head below them did not signal a provable hole")
		}
	})
	t.Run("a head re-established at the highest pending ack absorbs all at once", func(t *testing.T) {
		window := newSequenceAckWindowWithGapWake(3)
		deliver(window, 0, NewId())
		selective(window, 2)
		selective(window, 3)
		deliver(window, 3, NewId())
		if !gapWakeSignaled(window) {
			t.Fatal("the hole filling did not signal")
		}
		selective(window, 5)
		selective(window, 6)
		if gapWakeSignaled(window) {
			t.Fatal("two selective acks above a re-established head signaled a provable hole")
		}
	})
	t.Run("duplicate selective ack is one piece of evidence", func(t *testing.T) {
		window := newSequenceAckWindowWithGapWake(3)
		window.Update(sequenceAck{sequenceNumber: 0, messageId: NewId()})
		duplicateId := NewId()
		window.Update(sequenceAck{sequenceNumber: 2, messageId: duplicateId, selective: true})
		window.Update(sequenceAck{sequenceNumber: 2, messageId: duplicateId, selective: true})
		window.Update(sequenceAck{sequenceNumber: 3, messageId: NewId(), selective: true})
		if gapWakeSignaled(window) {
			t.Fatal("a duplicate selective ack counted as evidence")
		}
	})
	t.Run("evidence does not accumulate across snapshots", func(t *testing.T) {
		window := newSequenceAckWindowWithGapWake(3)
		window.Update(sequenceAck{sequenceNumber: 0, messageId: NewId()})
		window.Update(sequenceAck{sequenceNumber: 2, messageId: NewId(), selective: true})
		window.Update(sequenceAck{sequenceNumber: 3, messageId: NewId(), selective: true})
		window.Snapshot(true)
		window.Update(sequenceAck{sequenceNumber: 4, messageId: NewId(), selective: true})
		window.Update(sequenceAck{sequenceNumber: 5, messageId: NewId(), selective: true})
		if gapWakeSignaled(window) {
			t.Fatal("selective acks from the previous snapshot counted toward the proof")
		}
		window.Update(sequenceAck{sequenceNumber: 6, messageId: NewId(), selective: true})
		if !gapWakeSignaled(window) {
			t.Fatal("three selective acks in one snapshot did not signal a provable hole")
		}
	})
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
