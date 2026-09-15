package connect

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// The clamped regime: what a window does when the memory budget, not the path,
// is what bounds it.
//
// Why this needed asserting rather than following from what was already known.
// Every cell in this program has run with the ceiling an order of magnitude
// above the settled window — twenty-eight mebibytes against a window near two —
// so the clamped path has no coverage at all, and the identity that a flow
// rests at about half its window was established only where the window was free
// to grow. At two hundred to four hundred milliseconds every platform is
// budget-limited, because a gigabit at those path lengths needs twenty-five to
// fifty megabytes in flight per layer and no platform supplies it. So the
// clamped case is the operating condition at the common path, and it was the
// one regime nothing had exercised.
//
// Why it holds, from the rule's own shape. The scale multiplies the delivery
// term and nothing else: the ceiling is formed from the share, the peer's
// advertised capacity and the target, the window is set to it, and only then is
// the delivery term compared and applied if it is smaller. So when twice the
// delivery is at or above the ceiling, the window is the ceiling outright
// rather than the ceiling times anything. And the pre-sample window is the
// ceiling too, so on a path where the share is the smallest term the sender
// fills its share on the first round trip rather than climbing to it.
//
// The scale still earns its place here even though the window equals the
// ceiling: at 1.95 times the ceiling the delivery term absorbs a transient fall
// of up to half — a delayed acknowledgement batch, a ring sample across a lull —
// without dropping the window. At a scale of one the window would follow every
// dip and the sender would under-fill its own share.
//
// These rows assert relationships rather than byte counts, so they survive a
// change to either side.
//
// Predictions, recorded before the run: with the ceiling set below twice the
// delivery the window equals the ceiling and the binding reason never reads as
// delivery; occupancy reaches the ceiling rather than a fraction of it.
func TestAClampedWindowIsTheCeilingAndFillsIt(t *testing.T) {
	assertMessagePoolOwnership(t)

	const propagation = 25 * time.Millisecond
	// Small enough that twice the delivery is comfortably above it in every
	// run, so the clamp binds hard. A larger pool put this cell on the
	// boundary and it crossed run to run: with delivery at 272,872 bytes over
	// 52 ms the window correctly became the delivery term at 270,542 against a
	// 393,216 ceiling. That transition is clean — occupancy still reached
	// 394,612, and nothing was evicted or refused — which is the marginal case
	// answering for itself, but it is an observation rather than something a
	// row can assert without straddling the boundary.
	const ceiling = ByteCount(192 * 1024)
	const payloadByteCount = 4 * 1024

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	budget := NewTransferMemoryBudget(ceiling)
	harness := newSendWindowHarness(t, ctx, propagation,
		func(settings *SendBufferSettings) {
			settings.DeliverySizedWindowScale = deliverySizedWindowScale
			settings.ResendQueueBudget = budget
			settings.ResendQueueMinByteCount = ByteCount(32 * 1024)
		})
	// the peer can hold far more, so the share is the smallest term
	harness.receiveHold(ByteCount(16 * 1024 * 1024))

	// Sampled during the transfer rather than after it. Reading occupancy once
	// the offer stops measures a queue that is draining with nothing being
	// admitted behind it — 81 per cent of the ceiling on the first attempt,
	// which said nothing about whether the flow fills what it has.
	peak := &atomic.Int64{}
	watching := make(chan struct{})
	go func() {
		defer close(watching)
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Millisecond):
				_, queued, _ := harness.sender.ResendQueueSize(
					harness.receiverId, MultiHopId{}, false, false)
				for {
					old := peak.Load()
					if int64(queued) <= old || peak.CompareAndSwap(old, int64(queued)) {
						break
					}
				}
			}
		}
	}()
	harness.offer(t, payloadByteCount, 2*time.Second)

	estimate := harness.sender.DestinationSendStats(harness.receiverId).SendWindow
	cancel()
	<-watching
	occupancy := ByteCount(peak.Load())
	t.Logf(
		"ceiling %d, window %d, peak occupancy %d, delivered %d over %s, reason %q",
		estimate.Ceiling, estimate.Window, occupancy,
		estimate.DeliveredByteCount, estimate.Interval, estimate.Reason,
	)

	// The share is the pool less the floors guaranteed to the other queues
	// attached to it, and a client has control sequences besides this one, so
	// the ceiling is below the pool's total by their floors. That is the share
	// being computed correctly, and the row asserts the relationship rather
	// than the total.
	// Asserted as a relationship rather than a recomputed number: how many
	// queues share the pool changes as control sequences come and go, so the
	// exact lendable figure is not stable enough to equate against, while the
	// relationship is.
	if ceiling <= estimate.Ceiling {
		t.Fatalf(
			"the ceiling is %d against a %d byte pool, so the other attached queues' floors are not being accounted and this cell is not reading the share",
			estimate.Ceiling, ceiling,
		)
	}
	if estimate.Ceiling < ceiling/2 {
		t.Fatalf(
			"the ceiling is %d against a %d byte pool, far below what the other floors could account for, so this cell is measuring something else",
			estimate.Ceiling, ceiling,
		)
	}
	if estimate.Window != estimate.Ceiling {
		t.Errorf(
			"the window is %d against a %d byte ceiling; when the clamp binds the window is the ceiling outright, because the scale multiplies the delivery term and nothing else",
			estimate.Window, estimate.Ceiling,
		)
	}
	if estimate.Reason == "delivery" {
		t.Errorf(
			"the window's binding term reads as delivery in the clamped regime; that would mean something downstream is still deriving the limit from the delivery estimate rather than from the clamped value, which leaves the sender at a fraction while the ceiling reads correctly",
		)
	}
	// occupancy against the ceiling, not a byte count: the flow must use the
	// room it has rather than reserving headroom it cannot grow into
	if occupancy < estimate.Ceiling-2*payloadByteCount {
		t.Errorf(
			"occupancy settled at %d against a %d byte ceiling; when there is no room to grow, the scale's reservation is waste rather than headroom and the flow has to fill what it has",
			occupancy, estimate.Ceiling,
		)
	}
}

// The dynamic cases of a divided budget: a share that shrinks when other
// clients arrive and grows when they leave.
//
// These matter more than they look. Both work from a full window, and it is the
// half-filled unclamped case that stalls, which is the same asymmetry the
// clamped regime turns out to have in its favour. A shrinking share has to
// drain cleanly rather than evicting, and a growing one has to be taken up
// rather than crept toward.
//
// Predictions, recorded before the run: lowering the budget lowers the window
// to the new ceiling with no eviction at the receiver; raising it back takes
// the window up to the new ceiling.
func TestAClampedWindowFollowsItsShareDownAndUp(t *testing.T) {
	assertMessagePoolOwnership(t)

	const propagation = 25 * time.Millisecond
	// both small enough that the share binds rather than the delivery term,
	// for the reason the row above records
	const wide = ByteCount(384 * 1024)
	const narrow = ByteCount(192 * 1024)
	const payloadByteCount = 4 * 1024

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	budget := NewTransferMemoryBudget(wide)
	harness := newSendWindowHarness(t, ctx, propagation,
		func(settings *SendBufferSettings) {
			settings.DeliverySizedWindowScale = deliverySizedWindowScale
			settings.ResendQueueBudget = budget
			settings.ResendQueueMinByteCount = ByteCount(32 * 1024)
		})
	harness.receiveHold(ByteCount(16 * 1024 * 1024))

	harness.offer(t, payloadByteCount, time.Second)
	atWide := harness.sender.DestinationSendStats(harness.receiverId).SendWindow

	// other clients arrive: the share shrinks
	budget.SetTotalByteCount(narrow)
	harness.offer(t, payloadByteCount, time.Second)
	atNarrow := harness.sender.DestinationSendStats(harness.receiverId).SendWindow
	narrowStats := harness.receiver.ReceiveStats()

	// and leave again
	budget.SetTotalByteCount(wide)
	harness.offer(t, payloadByteCount, time.Second)
	atWideAgain := harness.sender.DestinationSendStats(harness.receiverId).SendWindow

	t.Logf(
		"share %d: window %d; share %d: window %d; share %d again: window %d. evictions %d, refusals %d",
		wide, atWide.Window, narrow, atNarrow.Window, wide, atWideAgain.Window,
		narrowStats.ReceiveQueueEvictionCount, narrowStats.ReceiveQueueDropCount,
	)

	// as above, the share is the pool less the other attached queues' floors
	if atWide.Window != atWide.Ceiling {
		t.Fatalf(
			"the window is %d against a %d byte ceiling, so this cell is not clamped",
			atWide.Window, atWide.Ceiling,
		)
	}
	if atNarrow.Window != atNarrow.Ceiling || atWide.Window <= atNarrow.Window {
		t.Errorf(
			"the share fell from %d to %d and the window went %d to %d against ceilings %d and %d; a window clamped by memory has to follow its share down when other clients arrive",
			wide, narrow, atWide.Window, atNarrow.Window,
			atWide.Ceiling, atNarrow.Ceiling,
		)
	}
	if 0 < narrowStats.ReceiveQueueEvictionCount {
		t.Errorf(
			"the receiver evicted %d items while the share was shrinking; draining to a lower ceiling is the sender's work and must not cost the receiver anything",
			narrowStats.ReceiveQueueEvictionCount,
		)
	}
	// Asserted as taking the room back rather than as an identical byte count.
	// On the way back up the window is the lesser of the share and twice the
	// delivery, and the delivery term varies run to run, so an equality here
	// demands a quantity that is not stable: it read 324,398 against the
	// 327,680 held before, a one per cent shortfall that is the delivery term
	// rather than a failure to grow.
	if atWideAgain.Window <= atNarrow.Window {
		t.Errorf(
			"the share returned to %d and the window is %d, no better than the %d it held while the share was narrow; a window clamped by memory has to take the room back when other clients leave",
			wide, atWideAgain.Window, atNarrow.Window,
		)
	}
	if float64(atWideAgain.Window) < 0.9*float64(atWide.Window) {
		t.Errorf(
			"the share returned to %d and the window recovered only to %d against the %d it held before, more than a tenth short; the room is back and the window has to take it",
			wide, atWideAgain.Window, atWide.Window,
		)
	}
}

// The rule and delivery-bounded reliable admission do not coexist, and the tree
// refuses rather than documents it.
//
// That admission bound computes its limit as what the lane delivered over a
// round trip, so it derives from delivery rather than from the window. With it
// on the sender sits below the ceiling whatever the ceiling says, which is
// precisely the failure the clamped regime exists to rule out: the ceiling
// reads correctly and the sender is at a fraction of it.
//
// A comment saying two settings conflict is the weakest protection there is,
// and this program has already found a setting pair recorded only in a comment
// separated by whoever came next. So the refusal is structural, at both the
// point of configuration and the point of use.
//
// Prediction, recorded before the run: with delivery-bounded admission on, the
// rule does not engage and says why, whether it was configured through the
// switch or by setting the fields directly.
func TestTheRuleRefusesToCoexistWithDeliveryBoundedAdmission(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })
	SetMemoryBudget(mib(256))

	// through the switch
	settings := DefaultSendBufferSettings()
	settings.ReliableAdmissionBoundedByDelivery = true
	settings.WindowSizing = WindowSizingFromDelivery
	settings.ApplyWindowSizing()
	if settings.WindowSizingActive() {
		t.Error("the switch engaged the rule alongside delivery-bounded admission")
	}
	if 0 < settings.DeliverySizedWindowScale {
		t.Errorf("the switch left the scale at %d", settings.DeliverySizedWindowScale)
	}

	// and set directly, which is how a cell configures it
	direct := DefaultSendBufferSettings()
	direct.ReliableAdmissionBoundedByDelivery = true
	direct.DeliverySizedWindowScale = deliverySizedWindowScale
	direct.ResendQueueBudget = NewTransferMemoryBudget(mib(8))
	estimate := windowEstimateForSettings(direct)
	t.Logf("configured directly: window %d, reason %q", estimate.Window, estimate.Reason)
	if estimate.Sized {
		t.Errorf(
			"the rule sized a window with delivery-bounded admission on; that bound derives its limit from delivery rather than from this window, so the sender would sit below the ceiling whatever the ceiling says",
		)
	}
	if estimate.Window != direct.ResendQueueMaxByteCount {
		t.Errorf(
			"the window is %d rather than the %d byte constant it should hold at",
			estimate.Window, direct.ResendQueueMaxByteCount,
		)
	}
}
