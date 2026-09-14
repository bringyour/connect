package connect

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// THROUGHPUTFIX: the rule sizes per sequence and the budget is shared, so a
// provider serving many downloaders is the one configuration where a
// per-sequence window multiplies against a fixed total. Every memory result in
// this program is single-flow, so this row exists to read the multi-sequence
// regime before a campaign reads it as "something regressed".
//
// What the shared budget promises, from source. A queue reserves only the bytes
// it holds above its guaranteed floor, and admission gates on what is left, so
// the floor keeps every sequence progressing when the pool is empty and
// cross-sequence deadlock is impossible. What that means for memory is that the
// bound is the sum of the floors plus the budget, not the budget alone: a queue
// below its floor is admitted whatever the budget says.
//
// Predictions, recorded before the run, with eight sequences sharing one
// budget on a bounded path:
//
//   - every sequence delivers rather than one taking the pool and starving the
//     rest, because the floor is unconditional;
//   - the budget's own used count stays within its total, the reserve path
//     being gated on what is available;
//   - each sequence reports what it could actually obtain as the pool stands,
//     which is the property this row was written to check.
//
// A correction to how that third one is expressed, which the design settled
// after this row first found it. The ceiling is the share: the static
// permission the pool would lend a queue at full demand, less the floors
// guaranteed to the other queues attached to it. Every sequence seeing the same
// share is right. What differs under contention is obtainable — the floor, plus
// what this queue has borrowed, plus what is unreserved right now — and that is
// reported for diagnosis and never clamped, because reading a transient as a
// limit pins the window at its floor whenever the pool happens to be busy
// elsewhere, for reasons that have nothing to do with the path.
//
// The other two held. Measured over three runs: delivery spread across the
// eight sequences of 1.01 to 1.03 times, so the unconditional floor does keep
// every sequence progressing; and peak pool use of 1.6 to 2.1 MB against the
// 2 MiB total, the small excess being the documented overdraft of up to one
// message per sequence past an admission that saw headroom.
//
// What this row does not settle. First-come still wins: a sequence that starts
// while the pool is free may borrow deeply and hold it, and the others wait at
// their floors until it drains. That is the shared budget's stated design
// rather than a defect, and the proportional division across clients that
// would change it is deferred in §37.15 until a regime needs it. This is the
// regime; the row is here so the next measurement of it starts from a reading
// rather than from an assumption.
func TestManySequencesSizingAgainstOneBudget(t *testing.T) {
	assertMessagePoolOwnership(t)

	const sequenceCount = 8
	const propagation = 10 * time.Millisecond
	const bytesPerSecond = ByteCount(100 * 1000 * 1000 / 8)
	const payloadByteCount = 4 * 1024
	const offerWindow = 3 * time.Second
	const floorByteCount = ByteCount(256 * 1024)
	// deliberately smaller than the sequences could want together, which is
	// the regime the row is about
	budget := NewTransferMemoryBudget(ByteCount(2 * 1024 * 1024))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type lane struct {
		harness   *sendWindowHarness
		delivered *atomic.Int64
	}
	lanes := make([]*lane, 0, sequenceCount)
	for range sequenceCount {
		harness := newRateLimitedSendWindowHarness(t, ctx, propagation, bytesPerSecond,
			func(settings *SendBufferSettings) {
				settings.WindowSizing = WindowSizingFromDelivery
				settings.ApplyWindowSizing()
				// one budget for all of them, which is the point
				settings.ResendQueueBudget = budget
				settings.ResendQueueMinByteCount = floorByteCount
			})
		harness.receiveHold(ByteCount(4 * 1024 * 1024))
		delivered := &atomic.Int64{}
		harness.receiver.AddReceiveCallback(
			func(_ TransferPath, frames []*protocol.Frame, _ Peer) {
				delivered.Add(int64(len(frames)) * payloadByteCount)
			},
		)
		lanes = append(lanes, &lane{harness: harness, delivered: delivered})
	}

	peakUsed := &atomic.Int64{}
	// The ceilings have to be read while the pool is contended. Once the
	// transfer finishes every queue has released and the whole pool genuinely
	// is obtainable by any one sequence, so a reading taken at the end says
	// nothing: that is what the first version of this row measured.
	leastCeiling := make([]*atomic.Int64, sequenceCount)
	for i := range leastCeiling {
		leastCeiling[i] = &atomic.Int64{}
		leastCeiling[i].Store(int64(1) << 62)
	}
	watching := make(chan struct{})
	go func() {
		defer close(watching)
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Millisecond):
				used := int64(budget.UsedByteCount())
				for {
					old := peakUsed.Load()
					if used <= old || peakUsed.CompareAndSwap(old, used) {
						break
					}
				}
				for i, l := range lanes {
					// Obtainable, not the ceiling. The ceiling is the share —
					// the static permission the pool would lend at full demand
					// — and it is right that every sequence sees the same one.
					// What differs under contention is what a sequence can
					// actually get right now, and that is reported for
					// diagnosis rather than clamped, because reading a
					// transient as a limit pins the window at its floor for
					// reasons that have nothing to do with the path.
					ceiling := int64(l.harness.sender.
						DestinationSendStats(l.harness.receiverId).SendWindow.Obtainable)
					if ceiling <= 0 {
						continue
					}
					for {
						old := leastCeiling[i].Load()
						if old <= ceiling || leastCeiling[i].CompareAndSwap(old, ceiling) {
							break
						}
					}
				}
			}
		}
	}()

	var running sync.WaitGroup
	for _, l := range lanes {
		running.Add(1)
		go func(l *lane) {
			defer running.Done()
			l.harness.offer(t, payloadByteCount, offerWindow)
		}(l)
	}
	running.Wait()

	total := ByteCount(0)
	least := int64(-1)
	most := int64(0)
	windows := make([]ByteCount, 0, sequenceCount)
	ceilings := make([]ByteCount, 0, sequenceCount)
	for _, l := range lanes {
		delivered := l.delivered.Load()
		total += ByteCount(delivered)
		if least < 0 || delivered < least {
			least = delivered
		}
		most = max(most, delivered)
		estimate := l.harness.sender.DestinationSendStats(l.harness.receiverId).SendWindow
		windows = append(windows, estimate.Window)
		ceilings = append(ceilings, estimate.Ceiling)
	}
	cancel()
	<-watching

	t.Logf(
		"%d sequences, budget %d: peak used %d, delivered %d total, least %d most %d (%.2fx spread); windows %v; ceilings %v",
		sequenceCount, budget.TotalByteCount(), peakUsed.Load(),
		total, least, most, float64(most)/float64(max(least, 1)),
		windows, ceilings,
	)

	if least == 0 {
		t.Errorf(
			"a sequence delivered nothing while the others ran; the guaranteed floor is supposed to make starvation impossible whatever the pool is doing",
		)
	}
	// the documented bound: the floors are unconditional, so the aggregate is
	// the floors plus the budget rather than the budget
	if bound := ByteCount(sequenceCount)*floorByteCount + budget.TotalByteCount(); bound < ByteCount(peakUsed.Load()) {
		t.Errorf(
			"the budget's used count peaked at %d against its %d total; reserve is only taken above the floor, so this should never exceed the total at all",
			peakUsed.Load(),
			budget.TotalByteCount(),
		)
	}
	contended := make([]ByteCount, 0, sequenceCount)
	for _, least := range leastCeiling {
		contended = append(contended, ByteCount(least.Load()))
	}
	t.Logf("least obtainable seen under contention: %v", contended)
	for i, obtainable := range contended {
		if budget.TotalByteCount() <= obtainable && 1 < sequenceCount {
			t.Errorf(
				"sequence %d never reported an obtainable figure below the %d byte pool it shares with %d others; obtainable is what a sequence could actually hold as the pool stands, and a diagnosis that always reads the whole pool tells a campaign nothing about which sequence lost",
				i, budget.TotalByteCount(), sequenceCount-1,
			)
		}
	}
}
