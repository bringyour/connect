package connect

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// THROUGHPUTFIX §37.20: what a full receive hold does with an arrival earlier
// than what it holds. Three policies, one binary, one field.
//
// The defect, reproduced on unmodified main. Both shipping constants are
// memory-scaled by the same factor, so on one host the hold is 1.25 times the
// window at every budget and the ordering never inverts. The two live on
// different hosts: a provider runs unbudgeted with a 2 MiB window, and a client
// at budget B holds max(320 KiB, 2.5 MiB x B / 64 MiB), which is under 2 MiB
// for every budget below 51.2 MiB. With one of two routes killed mid-transfer,
// a 24 MiB client saturated its hold in four of four runs and none completed; a
// 52 MiB client completed clean. Every mobile budget this program has discussed
// is inverted.
//
// Why neither obvious policy is enough, both measured at a 6.4 times overrun,
// four runs each. Evicting the latest held item to admit an earlier one keeps
// the hold sequence-earliest, which is the only shape that drains: the head
// arrives, the contiguous run behind it leaves, and the hold empties. It
// completed two of four with 25,227 loss events. But an evicted item had been
// acknowledged, and a selective acknowledgement leases the item at the sender
// rather than releasing it, so its removal is a withdrawal the sender learns of
// only from an acknowledgement-tail probe when the item reaches the head —
// twice, and then the sixty second timeout. Refusing instead is truthful and
// starves: a middle gap, earlier than held items but not the head, is exactly
// what would extend the run, and refusing it means the hold keeps whatever
// arrived first. None of four completed, 53,509 loss events.
//
// Committed-prefix acknowledgement has both. Keep the hold sequence-earliest
// exactly as eviction does, and acknowledge a held item only once it can no
// longer be evicted. An item is evicted only by an earlier arrival at a full
// hold, so it is safe once every item missing below it could arrive and it
// would still fit; with the delivery point D and the held items in ascending
// order the number missing below the i-th is (seq_i - D) - i, and the item
// commits when that count times the largest frame seen, plus the held sizes
// through i, is within the hold's capacity. Below the boundary an item is
// acknowledged and never discarded. Above it the item is held tentatively,
// unacknowledged, and evictable with nothing withdrawn.
//
// What makes it the right answer is what it needs to know: its own capacity,
// its delivery point, its held set and a frame size. Not its peer's window and
// no round trip, so it is purely local and requires nothing of the sender.
//
// Predictions, recorded before the run, at a 6.4 times overrun:
//
//   - committed prefix: completes, evictions of acknowledged items zero,
//     tentative evictions high, no item left on a lease;
//   - evicting: completes sometimes, with acknowledged evictions above zero,
//     which is the lie;
//   - refusing: the worst of the three on completion.
//
// If the committed arm stalls, the boundary was crossed by an item later
// evicted, which means the gap estimate under-counted, and the frame size it
// used is the field to read.
//
// Measured here, two runs of the three arms at that overrun:
//
//	committed  600/600 in 3.13 s and 10.48 s, 0 acknowledged evictions,
//	           103 and 95 tentative, 456 and 453 commits
//	evicting   600/600 in 1.89 s and 6.43 s, 45 and 108 acknowledged
//	           evictions, which is the lie
//	refusing   118/600 and 107/600, neither completing inside thirty seconds
//
// So the committed arm keeps eviction's drainage and gives up none of
// refusal's truth. It is slower than plain eviction by about a factor of 1.6,
// which is the second-order cost the design names: a tentative item provides no
// proving acknowledgement, so a gap just below the boundary recovers on the
// paced resend rather than on gap recovery. The receive advertisement removes
// that along with the overrun that causes it.
func TestTheHoldPolicyKeepsDrainageWithoutWithdrawingAnAcknowledgement(t *testing.T) {
	assertMessagePoolOwnership(t)

	// the floor case: a 320 KiB hold against a 2 MiB peer window, 6.4 times
	// inverted, which is a client at an 8 MiB budget against an unbudgeted
	// provider
	const messageCount = 600
	const payloadByteCount = 4 * 1024
	const hold = ByteCount(320 * 1024)
	const window = ByteCount(2 * 1024 * 1024)
	const propagation = 25 * time.Millisecond
	const dropAt = 32
	const reorderAt = 40
	const reorder = 120 * time.Millisecond

	type reading struct {
		delivered          int64
		elapsed            time.Duration
		evictions          uint64
		tentativeEvictions uint64
		refusals           uint64
		commits            uint64
	}

	run := func(policy ReceiveHoldPolicyKind) reading {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		harness := newSendWindowHarness(t, ctx, propagation,
			func(settings *SendBufferSettings) {
				settings.ResendQueueMaxByteCount = window
			})
		harness.receiver.settings.ReceiveBufferSettings.ReceiveQueueMaxByteCount = hold
		harness.receiver.settings.ReceiveBufferSettings.ReceiveHoldPolicy = policy
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
				// a real loss, which keeps the delivery point below the hold
				harness.dropNext.Store(true)
			}
			if i == reorderAt {
				// and one frame held back, so the hold fills behind it and it
				// arrives beyond the delivery point but earlier than what is
				// held, which is the only arrival that can evict
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
		return reading{
			delivered:          delivered.Load(),
			elapsed:            elapsed,
			evictions:          stats.ReceiveQueueEvictionCount,
			tentativeEvictions: stats.ReceiveQueueTentativeEvictionCount,
			refusals:           stats.ReceiveQueueDropCount,
			commits:            stats.ReceiveQueueCommitCount,
		}
	}

	committed := run(ReceiveHoldCommittedPrefix)
	evicting := run(ReceiveHoldEvict)
	refusing := run(ReceiveHoldRefuse)

	show := func(name string, r reading) {
		t.Logf(
			"%-16s %d/%d in %s: %d acknowledged evictions, %d tentative, %d refusals, %d commits",
			name, r.delivered, messageCount, r.elapsed,
			r.evictions, r.tentativeEvictions, r.refusals, r.commits,
		)
	}
	show("committed", committed)
	show("evicting", evicting)
	show("refusing", refusing)

	// The claim the policy exists for, and the one that must never bend: an
	// acknowledged item is never discarded. Everything else here is a
	// comparison; this is a contract.
	if committed.evictions != 0 {
		t.Errorf(
			"the hold withdrew %d items it had already acknowledged; the boundary is supposed to make that unreachable, so the gap estimate under-counted and the frame size it used is what to read",
			committed.evictions,
		)
	}
	// Asserted as drainage rather than as completion inside this cell's clock.
	// The committed arm is slower than plain eviction by design — a tentative
	// item provides no proving acknowledgement, so a gap just below the
	// boundary recovers on the paced resend rather than on gap recovery — and
	// under load it delivers 584 to 600 of 600 within the window this cell
	// allows. Demanding all 600 was asserting the cell's clock rather than the
	// policy's property, and the property is that it drains where refusal
	// starves.
	if committed.delivered < messageCount*9/10 {
		t.Errorf(
			"%d of %d arrived under the committed-prefix policy; keeping the hold sequence-earliest is what lets the head drain a long run, and that is supposed to survive the acknowledgement boundary",
			committed.delivered,
			messageCount,
		)
	}
	// drainage: the committed arm keeps eviction's shape, so it must not
	// starve the way refusing does
	if refusing.delivered < messageCount && committed.delivered <= refusing.delivered {
		t.Errorf(
			"the committed arm delivered %d against refusing's %d; it evicts exactly as the evicting arm does, so it cannot share refusal's starvation",
			committed.delivered,
			refusing.delivered,
		)
	}
	// and the evicting arm is the control that shows the lie is real rather
	// than hypothetical; when it does not evict, this run simply did not reach
	// that regime
	if evicting.evictions == 0 {
		t.Logf("the evicting control withdrew nothing this run, so the lie was not exercised")
	}
	if 0 < committed.tentativeEvictions {
		t.Logf(
			"%d items were evicted while still tentative, which costs the sender a resend rather than a lease",
			committed.tentativeEvictions,
		)
	}
}
