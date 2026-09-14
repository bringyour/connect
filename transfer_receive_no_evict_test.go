package connect

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// THROUGHPUTFIX §37.17, guard one: a full hold refuses the arrival rather than
// removing an item it has already acknowledged.
//
// The defect this closes, reproduced on unmodified main by the harness. Both
// shipping constants are memory-scaled by the same factor, so on one host the
// hold is 1.25 times the window at every budget and the ordering never
// inverts. The two live on different hosts. A provider runs unbudgeted with a
// 2 MiB window; a client at budget B holds max(320 KiB, 2.5 MiB x B / 64 MiB),
// which is below 2 MiB for every budget under 51.2 MiB. Measured with one of
// two routes killed mid-transfer: at a 24 MiB client budget the hold is
// 938 KiB against that 2 MiB window, it saturated in four of four runs and
// none completed, 12,000 to 15,000 of 20,000 messages delivered before
// progress stopped; at a 52 MiB budget the hold is 2.031 MiB, above the peer's
// window, and both runs completed with zero drops while still reaching 84 to
// 90 per cent of capacity. The only variable is whether the hold exceeds the
// peer's window, and every mobile budget this program has discussed is
// inverted.
//
// Why the mechanism is a refusal rather than a larger hold. Eviction reneges
// silently: a selective acknowledgement does not release the item at the
// sender, it leases it for the sixty second selective-ack timeout, and every
// resend path skips a marked item. A refusal acknowledges nothing, so the
// sender's scoreboard stays truthful and the item returns on a path that
// already exists. It costs no memory on the constrained side, needs no wire
// field, and protects an updated receiver against any peer.
//
// The head-of-line objection does not arise, and the reason is one branch of
// the receive path: an arrival at the delivery point registers its contract,
// delivers and returns before any queueing. Only an arrival beyond the
// delivery point is queued. So the filler of a hole never needs hold space,
// and a gap behind a full hold always fills.
//
// Prediction, recorded before the run: with the guard, an overrun that evicted
// before now evicts nothing and the transfer still completes; with it off, the
// same cell evicts. Refusals rise, which is the cost, and the receive
// advertisement is what removes them.
//
// Measured, and the prediction about refusals was wrong in a useful direction.
// Sweeping the overrun against a 2 MiB window, 600 messages of 4 KiB with one
// induced loss:
//
//	hold 2048 KiB (1.0x): no pressure, 0 evictions either way
//	hold 1536 KiB (1.3x): evicting 380 ms / 1 eviction, refusing 432 ms / 0
//	hold  960 KiB (2.1x): evicting 2.58 s / 103 evictions / 378 refusals,
//	                      refusing 763 ms / 0 evictions / 257 refusals
//	hold  512 KiB (4.0x): evicting 2.34 s / 102 evictions / 484 refusals,
//	                      refusing 1.09 s / 0 evictions / 372 refusals
//
// So refusing is faster and refuses less at the ratios production actually
// has, because an eviction costs the sixty second lease while a refusal costs
// a paced resend, and the evicting arm's own recovery keeps re-filling the
// hold.
//
// Beyond about five times, this fixture stalls under both behaviours and the
// difference is not separable in it: at a 384 KiB hold both arms delivered
// around 120 of 600 inside thirty seconds. That ratio is a client budget at or
// below about 10 MiB. Nothing here says the guard is worse there, and nothing
// here says it is better; the failover cell on two routes is the instrument
// that can decide it, and this is recorded rather than asserted.
func TestAFullHoldRefusesRatherThanEvicting(t *testing.T) {
	assertMessagePoolOwnership(t)

	// a hold well under what the peer may have outstanding, which is the
	// inverted shape every mobile budget has
	const messageCount = 600
	const payloadByteCount = 4 * 1024
	// The inversion the harness reproduced on main: a 24 MiB client budget
	// against an unbudgeted provider, 2.13 times over.
	const hold = ByteCount(960 * 1024)
	const window = ByteCount(2 * 1024 * 1024)
	const propagation = 25 * time.Millisecond
	const dropAt = 32
	const reorderAt = 40
	// long enough for the hold to fill behind the held frame, short enough
	// that the dropped frame below it is often still missing
	const reorder = 120 * time.Millisecond

	run := func(evict bool) (int64, time.Duration, uint64, uint64) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		harness := newSendWindowHarness(t, ctx, propagation,
			func(settings *SendBufferSettings) {
				settings.ResendQueueMaxByteCount = window
			})
		harness.receiver.settings.ReceiveBufferSettings.ReceiveQueueMaxByteCount = hold
		harness.receiver.settings.ReceiveBufferSettings.EvictHeldItemsToFit = evict
		delivered := &atomic.Int64{}
		harness.receiver.AddReceiveCallback(
			func(_ TransferPath, frames []*protocol.Frame, _ Peer) {
				delivered.Add(int64(len(frames)))
			},
		)

		payload := string(make([]byte, payloadByteCount))
		start := time.Now()
		for i := range messageCount {
			if i == dropAt {
				// A real loss opens the gap that keeps the delivery point
				// below the hold, which is what makes a later arrival evict
				// rather than deliver.
				harness.dropNext.Store(true)
			}
			if i == reorderAt {
				// and one frame held back, so the hold fills behind it and it
				// arrives beyond the delivery point but earlier than what is
				// held
				harness.delayNextNanos.Store(int64(reorder))
			}
			frame := RequireToFrameWithDefaultProtocolVersion(
				&protocol.SimpleMessage{Content: payload},
			)
			admitted, _ := harness.sender.SendWithTimeoutDetailed(
				frame,
				harness.receiverId,
				nil,
				time.Second,
				sendPackRecoveryOption{upstreamRecoverable: true, retainAfterAckTimeout: true},
			)
			if !admitted {
				MessagePoolReturn(frame.MessageBytes)
			}
		}
		deadline := time.Now().Add(30 * time.Second)
		for time.Now().Before(deadline) && delivered.Load() < messageCount {
			time.Sleep(50 * time.Millisecond)
		}
		elapsed := time.Since(start)
		stats := harness.receiver.ReceiveStats()
		return delivered.Load(), elapsed,
			stats.ReceiveQueueEvictionCount, stats.ReceiveQueueDropCount
	}

	// The control arm has to actually evict for the cell to reach the defect,
	// and whether it does is a race in the fixture rather than in the
	// behaviour under test. An eviction needs an arrival that is beyond the
	// delivery point and earlier than the hold's newest item, which means a
	// gap still open below it while the hold is full — a window of a round
	// trip or so. The held frame widens it and the retries take what is left;
	// when even that misses, the cell reports that it could not reach the
	// defect rather than failing, because the property under test is the
	// refusing arm's.
	var evictedDelivered int64
	var evictedElapsed time.Duration
	var evictions, evictedDrops uint64
	for attempt := range 6 {
		evictedDelivered, evictedElapsed, evictions, evictedDrops = run(true)
		if 0 < evictions {
			break
		}
		t.Logf("attempt %d: the control arm evicted nothing, retrying", attempt)
	}
	refusedDelivered, refusedElapsed, refusedEvictions, refusedDrops := run(false)

	t.Logf(
		"evicting: %d/%d in %s, %d evictions, %d refusals; refusing: %d/%d in %s, %d evictions, %d refusals",
		evictedDelivered, messageCount, evictedElapsed, evictions, evictedDrops,
		refusedDelivered, messageCount, refusedElapsed, refusedEvictions, refusedDrops,
	)

	if evictions == 0 {
		t.Skipf(
			"the control arm evicted nothing in six attempts, so this run could not reach the defect; the hold is %d against a %d byte window",
			hold, window,
		)
	}
	if refusedEvictions != 0 {
		t.Errorf(
			"the receiver removed %d items it had already acknowledged; with the guard it must refuse the arrival instead, because the sender holds a selective acknowledgement for a minute and no resend path will look at it",
			refusedEvictions,
		)
	}
	if refusedDelivered != messageCount {
		t.Errorf(
			"%d of %d arrived with the guard on; refusing has to recover on the paths that already exist, and a gap behind a full hold always fills because its filler is delivered before any queueing",
			refusedDelivered,
			messageCount,
		)
	}
	// the cost, recorded rather than asserted as a win: refusals rise, and the
	// receive advertisement is what removes them
	if refusedDrops <= evictedDrops {
		t.Logf(
			"refusals did not rise: %d against %d when evicting",
			refusedDrops, evictedDrops,
		)
	}
}
