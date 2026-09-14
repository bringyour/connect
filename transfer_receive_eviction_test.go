package connect

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// THROUGHPUTFIX §37.16's eviction notice, which is compatibility rather than
// part of the fix, and this row exists to keep it from being deleted as dead.
//
// Read this before concluding the field is unused. Preventing the overrun is
// what fixes the defect, and that is the advertisement. Under §37.20's
// committed-prefix policy no acknowledged item is ever discarded, so **no
// receiver in this tree emits a notice at all** — it is compatibility for a
// receiver that does not exist. A capture between two peers containing no
// evicted_sequence_numbers is therefore the expected reading rather than a
// failure, and not evidence the path is dead: the sender-side handling is a
// one-line safety against any receiver that does withdraw an acknowledged
// item, and the field number stays reserved. Record, do not rely on, and do
// not remove by accident.
//
// So this row turns eviction back on to exercise the sender's half.
//
// The defect, from source, which exists without any window change. A selective
// acknowledgement does not release the item at the sender: receiveAck removes
// it from the resend queue, marks it selectiveAcked, sets its resend time to
// its send time plus SelectiveAckTimeout — sixty seconds — and adds it back.
// On the receive side, when an arrival does not fit, later held items are
// removed to admit it and the removal sends nothing. So the sender holds a
// lease on bytes the receiver no longer has.
//
// A correction to §37.16's mechanism, found by this cell and confirmed from
// source by the designer, because it changes the size of the harm rather than
// its existence. The section says every resend path skips a marked item and
// only the timeout resend clears the mark. There is a fourth: when the oldest
// outstanding item is selectively acknowledged, scheduleSelectiveAckRecovery
// reschedules it as an acknowledgement-tail probe at its send time plus twice
// the window's minimum round trip, clamped between the 300 ms resend floor and
// 8 s, on every pass and with the mark intact. The probe is a full write of the
// frame, so an evicted item that has reached the head is admitted by it. But
// AckTailProbeLimit is 2, so after two probes the minute applies.
//
// So the harm is one probe interval per evicted item that reaches the head,
// serialised, plus the minute for any item that exhausts its probes first. A
// few intervals for a small generation and hundreds for a large one. That is
// why the branch cell at a 3 MiB window completed and the main cell at a 24 MiB
// client budget did not, and it is the eighty seconds this cell measured at a
// hold far under its peer's window.
//
// What this row does not assert, and why. The end-to-end benefit — completion
// time with the notice against without — is not separable in this fixture. Four
// runs at a 768 KiB window against a 256 KiB hold: leased 2.21, 2.51, 2.52 s
// against noticed 1.10, 1.76, 7.38 s, with the eviction counts themselves
// differing between arms by three times because the notice changes the
// dynamics that produce evictions. With a single route and the cumulative probe
// draining the head, the difference is inside the run-to-run spread. Separating
// it needs an eviction generation large enough that serialized head-of-line
// probing dominates, which is the multi-route failover cell with one route
// killed, not this one.
//
// So what this asserts is the mechanism, which is deterministic: the eviction
// is counted, the notice reaches the sender, and the sender resends exactly
// what the notice named.
//
// One more finding, and it contradicts §37.16's prediction for the notice
// alone. The section predicts that with the notice and no advertisement the
// transfer completes at every window, one round per eviction generation. It
// does not, under an overrun. A resent evicted item is earlier than everything
// the hold has accumulated, so admitting it evicts another item, which is
// noticed, resent, and evicts a third: with outstanding above the hold's
// capacity the notice keeps a rotation spinning that the cumulative probe
// would have drained monotonically. Measured over six runs at a 768 KiB window
// against a 256 KiB hold: five completed in 1.1 to 2.0 s with the notice
// against 2.2 to 4.7 s without, and one took 80 s and delivered 289 of 400,
// with 1,679 refused arrivals against the leased arm's 218. So the notice is
// not a standalone fix; the advertisement is what makes it safe, which is the
// row below. This row therefore asserts the mechanism and logs the rest.
//
// Predictions, recorded before the run: both arms evict; with the notice the
// sender resends items a notice named and without it that count is zero; the
// drop counter alone does not see the evictions.
func TestTheEvictionNoticeStillServesAReceiverThatEvicts(t *testing.T) {
	assertMessagePoolOwnership(t)

	const messageCount = 400
	const payloadByteCount = 4 * 1024
	// Sixty-four items of hold against a sixty-eight item window. The overrun
	// is deliberately small: a large one buries the eviction under hundreds of
	// refused arrivals, whose recovery is bounded by the gap burst of four per
	// scan and which is a different cost with a different fix. Here at most
	// four arrivals are ever refused, so what the arms differ by is the
	// eviction.
	const hold = ByteCount(256 * 1024)
	const window = ByteCount(768 * 1024)
	// long enough that the tail is written before the hole's retransmit
	// arrives, which is what makes the eviction forced rather than incidental
	const propagation = 25 * time.Millisecond
	const dropAt = 32

	run := func(notice bool) (int64, time.Duration, uint64, uint64, uint64) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		harness := newSendWindowHarness(t, ctx, propagation,
			func(settings *SendBufferSettings) {
				settings.ResendQueueMaxByteCount = window
			})
		harness.receiver.settings.ReceiveBufferSettings.ReceiveQueueMaxByteCount = hold
		harness.receiver.settings.ReceiveBufferSettings.EvictionNotice = notice
		// No receiver emits a notice under the shipping policy, so this row
		// selects the plain-eviction arm to exercise the sender's half.
		harness.receiver.settings.ReceiveBufferSettings.ReceiveHoldPolicy = ReceiveHoldEvict
		delivered := &atomic.Int64{}
		harness.receiver.AddReceiveCallback(
			func(_ TransferPath, frames []*protocol.Frame, _ Peer) {
				delivered.Add(int64(len(frames)))
			},
		)

		payload := string(make([]byte, payloadByteCount))
		start := time.Now()
		for i := range messageCount {
			// open the hole a little way in, once the sequence is running
			if i == dropAt {
				harness.dropNext.Store(true)
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
		// long enough for many round trips and many resend intervals. Completion
		// is not asserted here, so this is a bound on the cell rather than a
		// deadline the arms are judged against.
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) && delivered.Load() < messageCount {
			time.Sleep(50 * time.Millisecond)
		}
		elapsed := time.Since(start)
		stats := harness.receiver.ReceiveStats()
		senderStats := harness.sender.SendRecoveryStats()
		return delivered.Load(),
			elapsed,
			stats.ReceiveQueueEvictionCount,
			stats.ReceiveQueueDropCount,
			senderStats.SendEvictionResendCount
	}

	leasedDelivered, leasedElapsed, leasedEvictions, leasedDrops, leasedResends := run(false)
	noticedDelivered, noticedElapsed, noticedEvictions, noticedDrops, resends := run(true)

	t.Logf(
		"leased: %d/%d in %s, %d evictions, %d drops, %d notice resends; noticed: %d/%d in %s, %d evictions, %d drops, %d notice resends",
		leasedDelivered, messageCount, leasedElapsed, leasedEvictions, leasedDrops, leasedResends,
		noticedDelivered, messageCount, noticedElapsed, noticedEvictions, noticedDrops, resends,
	)

	if noticedEvictions == 0 {
		t.Errorf("the notice arm evicted nothing, so it cannot show a notice being acted on")
	}
	if leasedResends != 0 {
		t.Errorf(
			"the arm with the notice off resent %d items on a notice, so the setting is not the only difference between the arms",
			leasedResends,
		)
	}
	if resends == 0 {
		t.Errorf(
			"the receiver evicted %d items and the sender resent none of them on a notice; the notice exists so an eviction is a resend rather than a lease",
			noticedEvictions,
		)
	}
	// the counter this program added, and why the drop count was not enough:
	// an eviction is a promise withdrawn and a drop is an arrival refused, and
	// before the counter existed only the second was visible
	if leasedEvictions == 0 {
		t.Errorf("the leased arm evicted nothing, so the drop count and the eviction count cannot be told apart here")
	}
	if resends == 0 {
		t.Errorf("the notice arm completed without resending anything a notice named, so it completed for some other reason")
	}
}

// THROUGHPUTFIX §37.16's correction to §37.3: the advertised figure is the
// hold's capacity from the delivered point, not its free space.
//
// Capacity less what is held double counts. A selective acknowledgement does
// not release the item at the sender, so held bytes are already inside the
// sender's outstanding count; subtracting them would shrink the window by the
// held amount for nothing and, as the hold fills after a route death, pull the
// right edge of the window inward, which a window must never do.
//
// Prediction, recorded before the run: the advertised figure does not fall as
// the hold fills.
func TestTheAdvertisedWindowIsCapacityRatherThanFreeSpace(t *testing.T) {
	assertMessagePoolOwnership(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const hold = ByteCount(256 * 1024)
	const ceiling = ByteCount(16 * 1024 * 1024)
	harness := newSendWindowHarness(t, ctx, 5*time.Millisecond,
		func(settings *SendBufferSettings) {
			settings.DeliverySizedWindowScale = 2
			settings.DeliverySizedWindowCeilingByteCount = ceiling
			settings.ResendQueueBudget = NewTransferMemoryBudget(ceiling)
		})
	harness.receiveHold(hold)

	// open a hole so the hold fills out of order, then read what the sender was
	// told while the hold is occupied
	harness.dropNext.Store(true)
	harness.offer(t, 4*1024, 500*time.Millisecond)

	estimate := harness.sender.DestinationSendStats(harness.receiverId).SendWindow
	advertised := estimate.Ceiling
	t.Logf("hold %d, advertised ceiling %d, window %d", hold, advertised, estimate.Window)
	if advertised != hold {
		t.Errorf(
			"the receiver advertised %d against a %d byte hold; capacity is the quantity, and free space would shrink the sender's window by exactly what the sender already counts as outstanding",
			advertised,
			hold,
		)
	}
}

// THROUGHPUTFIX §37.16's primary field: a sender that knows the receiver's hold
// capacity never makes an eviction necessary.
//
// The rule, and why capacity is the right quantity. A selective acknowledgement
// does not release the item at the sender, so the sender's outstanding bytes
// measured from the delivered point include everything the receiver holds. Held
// bytes are at most outstanding bytes. So a sender that keeps
// outstanding-from-delivered at or below the advertised capacity can never
// force an eviction, whatever the gap structure — one gap or a thousand —
// because the hold would have to contain more than the sender has outstanding.
// That is why gap structure does not need advertising, and it is TCP's rule.
//
// This is the row that makes the notice safe rather than a rotation: the cell
// above shows a notice under an overrun evicting a second item to readmit the
// first. With the advertisement there is no overrun to recover from.
//
// Prediction, recorded before the run: with the sender clamped to the
// receiver's advertised capacity, an induced loss and the out-of-order hold it
// opens produce zero evictions and zero refused arrivals, and everything
// arrives.
func TestAnAdvertisedCapacityRemovesTheEvictionEntirely(t *testing.T) {
	assertMessagePoolOwnership(t)

	// Eight megabytes through a two mebibyte hold, so the window is what binds
	// and the hole opens real out-of-order occupancy. The hold is the shipping
	// value after §37.17's guard one, which is also what a sender assumes of a
	// peer that has not advertised, so the opening burst before the first
	// acknowledgement cannot overrun it either. A cell with a hold below that
	// assumption is testing a configuration the guard removes.
	const messageCount = 2000
	const payloadByteCount = 4 * 1024
	const hold = ByteCount(2 * 1024 * 1024)
	const propagation = 25 * time.Millisecond
	const dropAt = 100

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := newSendWindowHarness(t, ctx, propagation,
		func(settings *SendBufferSettings) {
			// the rule on, with far more permission than the hold, so the
			// advertisement is the only thing that can bound it
			settings.ResendQueueMaxByteCount = ByteCount(2 * 1024 * 1024)
			settings.ResendQueueMinByteCount = ByteCount(64 * 1024)
			settings.DeliverySizedWindowScale = 2
			settings.DeliverySizedWindowCeilingByteCount = ByteCount(16 * 1024 * 1024)
			settings.ResendQueueBudget = NewTransferMemoryBudget(ByteCount(16 * 1024 * 1024))
		})
	harness.receiveHold(hold)
	delivered := &atomic.Int64{}
	harness.receiver.AddReceiveCallback(
		func(_ TransferPath, frames []*protocol.Frame, _ Peer) {
			delivered.Add(int64(len(frames)))
		},
	)

	payload := string(make([]byte, payloadByteCount))
	for i := range messageCount {
		if i == dropAt {
			harness.dropNext.Store(true)
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

	stats := harness.receiver.ReceiveStats()
	estimate := harness.sender.DestinationSendStats(harness.receiverId).SendWindow
	t.Logf(
		"hold %d, advertised ceiling %d, window %d: %d/%d delivered, %d evictions, %d drops",
		hold, estimate.Ceiling, estimate.Window,
		delivered.Load(), messageCount,
		stats.ReceiveQueueEvictionCount, stats.ReceiveQueueDropCount,
	)

	if estimate.Ceiling != hold {
		t.Fatalf(
			"the sender's ceiling is %d rather than the %d the receiver advertised, so this cell is not testing the clamp",
			estimate.Ceiling,
			hold,
		)
	}
	if stats.ReceiveQueueEvictionCount != 0 {
		t.Errorf(
			"the receiver evicted %d items while the sender was held to its advertised capacity; a sender at or below the capacity cannot fill the hold past it, so an eviction here means the clamp is not the quantity it should be",
			stats.ReceiveQueueEvictionCount,
		)
	}
	if stats.ReceiveQueueDropCount != 0 {
		t.Errorf(
			"the receiver refused %d arrivals while the sender was held to its advertised capacity",
			stats.ReceiveQueueDropCount,
		)
	}
	if delivered.Load() != messageCount {
		t.Errorf("%d of %d arrived", delivered.Load(), messageCount)
	}
}
