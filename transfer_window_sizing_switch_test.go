package connect

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/urnetwork/connect/protocol"
)

// THROUGHPUTFIX §37.4's surface as one switch, and the property that decides
// whether it can ship at all: with the switch off the tree behaves exactly as a
// tree without this program, so turning it on is reversible in production by
// one change and the rollback is trustworthy.
//
// Asserted rather than assumed, in the two places a difference could hide: the
// settings the rule derives, and the bytes on the wire.
//
// Predictions, recorded before the run: under the constant policy the scale,
// the ceiling, the target and the budget are all off; the window rule reports
// the constant and says the rule is off whatever the path does; and an
// acknowledgement carries neither of the fields this program added.
func TestTheWindowSizingSwitchOffIsTodaysBehaviour(t *testing.T) {
	settings := DefaultSendBufferSettings()
	if settings.WindowSizing != WindowSizingConstant {
		t.Fatalf("the shipping policy is %d rather than the constant window", settings.WindowSizing)
	}
	if settings.DeliverySizedWindowScale != 0 ||
		settings.DeliverySizedWindowCeilingByteCount != 0 ||
		settings.TargetGoodputByteRate != 0 ||
		settings.ResendQueueBudget != nil {
		t.Errorf(
			"the constant policy left the rule's quantities set: scale %d, ceiling %d, target %d, budget %v; every one of them has to be off or the off path is not today's",
			settings.DeliverySizedWindowScale,
			settings.DeliverySizedWindowCeilingByteCount,
			settings.TargetGoodputByteRate,
			settings.ResendQueueBudget,
		)
	}

	// the wire: an acknowledgement under the shipping configuration carries
	// neither the advertised capacity nor an eviction notice
	receiveSettings := DefaultReceiveBufferSettings()
	if receiveSettings.AdvertiseReceiveWindow {
		t.Error("the receiver advertises its capacity by default, which changes the wire")
	}
	saf := sendAckFrame{
		path:               DestinationId(NewId()).AddSource(NewId()),
		messageId:          NewId(),
		sequenceId:         NewId(),
		selective:          true,
		logicalLaneVersion: transferLogicalLaneVersion,
	}
	frameBytes := marshalSendAckTransferFrame(&saf)
	defer MessagePoolReturn(frameBytes)
	for _, field := range []protowire.Number{8, 9} {
		if ackFrameHasField(t, frameBytes, field) {
			t.Errorf(
				"an acknowledgement carries field %d under the shipping configuration; the off path has to be byte for byte what a tree without this program writes",
				field,
			)
		}
	}
}

// reports whether the Ack inside a marshalled TransferFrame carries a field
func ackFrameHasField(t *testing.T, frameBytes []byte, want protowire.Number) bool {
	t.Helper()
	frame := &protocol.TransferFrame{}
	if !unmarshalTransferFrame(frameBytes, frame, true) {
		t.Fatal("could not decode the acknowledgement frame")
	}
	ack := frame.GetAck()
	if ack == nil {
		t.Fatal("the frame carried no acknowledgement")
	}
	switch want {
	case 8:
		return ack.ReceiveWindowByteCount != nil
	case 9:
		return 0 < len(ack.EvictedSequenceNumbers)
	}
	return false
}

// THROUGHPUTFIX: the rule has to be inert where it should be inert, because a
// rule that changes nothing on a short path is what makes it safe to enable
// everywhere rather than selectively.
//
// On a short path the window is small whatever the permission, because the
// path cannot fill a large one: permission is not occupancy. The claim is
// therefore about throughput rather than about the window, and it is measured
// as such here.
//
// The carrier has to be the binder for this cell to mean anything, which the
// first attempt got wrong and is recorded because it is a trap for the
// campaign too. Against an unpaced in-process carrier, throughput rises with
// permission far above the bandwidth-delay product — the fixture runs a
// goroutine per frame, so a larger window buys parallelism rather than
// pipelining — and the rule measured 19.5 MB/s against the constant's 49.0 on
// a 5 ms path purely because it computed a smaller number. Nothing about a real
// path was being read. With the carrier paced below what either window
// permits, both arms are bound by the carrier and the question the row asks is
// the one it means to ask.
//
// Prediction, recorded before the run: on a 5 ms path bound at 100 Mb/s, where
// the bandwidth-delay product is about 62 KiB and both windows are several
// times that, the rule delivers within a tenth of the constant window's
// throughput, and its window does not exceed what the receiver advertised.
//
// Measured, three runs: 0.96, 1.07 and 0.95 times the constant, with the rule
// computing a window of 440 to 731 KiB against the constant's 2 MiB. The null
// band was taken rather than assumed — two constant arms against two sized arms
// in one run — and it is about one per cent, so the few per cent here is the
// cell's own spread and the rule is inert, which is what makes it safe to
// enable everywhere rather than selectively.
func TestTheWindowRuleIsInertOnAShortPath(t *testing.T) {
	assertMessagePoolOwnership(t)

	const propagation = 5 * time.Millisecond
	// bound well below what either window permits, so the carrier is what
	// decides throughput and the window is not the binder
	const bytesPerSecond = ByteCount(100 * 1000 * 1000 / 8)
	const hold = ByteCount(2 * 1024 * 1024)
	const offerWindow = 3 * time.Second
	const payloadByteCount = 4 * 1024

	run := func(sizing WindowSizingPolicyKind) (float64, SendWindowEstimate) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		harness := newRateLimitedSendWindowHarness(t, ctx, propagation, bytesPerSecond,
			func(settings *SendBufferSettings) {
				settings.WindowSizing = sizing
				settings.ApplyWindowSizing()
			})
		harness.receiveHold(hold)
		// Counted at the receiver. The sender's write count is admission
		// rather than goodput, and against a carrier that queues, a larger
		// window scores higher on it while delivering exactly the same bytes:
		// measuring that way had the rule 29 per cent "slower" on a path where
		// both windows were more than thirty times the bandwidth-delay
		// product, which was the instrument and not the rule.
		delivered := &atomic.Int64{}
		harness.receiver.AddReceiveCallback(
			func(_ TransferPath, frames []*protocol.Frame, _ Peer) {
				delivered.Add(int64(len(frames)) * payloadByteCount)
			},
		)
		start := time.Now()
		harness.offer(t, payloadByteCount, offerWindow)
		elapsed := time.Since(start)
		stats := harness.sender.DestinationSendStats(harness.receiverId)
		return float64(delivered.Load()) / elapsed.Seconds(), stats.SendWindow
	}

	constantRate, constantEstimate := run(WindowSizingConstant)
	sizedRate, sizedEstimate := run(WindowSizingFromDelivery)

	t.Logf(
		"constant window %d: %.1f MB/s; sized window %d (ceiling %d, reason %q): %.1f MB/s (%.2fx)",
		constantEstimate.Window, constantRate/1e6,
		sizedEstimate.Window, sizedEstimate.Ceiling, sizedEstimate.Reason,
		sizedRate/1e6, sizedRate/constantRate,
	)

	if sizedRate < 0.9*constantRate {
		t.Errorf(
			"the rule delivered %.1f MB/s against the constant's %.1f on a %s path, %.2f times; a rule that costs throughput on short paths cannot be enabled everywhere and would have to be enabled selectively, which is the defect this program is removing",
			sizedRate/1e6, constantRate/1e6, propagation, sizedRate/constantRate,
		)
	}
	if hold < sizedEstimate.Window {
		t.Errorf(
			"the window is %d against a %d byte advertised capacity; the peer's capacity is the outermost clamp",
			sizedEstimate.Window,
			hold,
		)
	}
}

// THROUGHPUTFIX §37.22: the share must be a draw on the budget, proportional
// to it, and must never be a memory-scaled constant.
//
// The finding this guards. Every window and hold in the enumeration is a
// memory-scaled constant, and the scale returns one at or above the 64 MiB
// reference and a fraction below. So all of them were sized for the reference
// host and can only shrink from it: a provider with eight gigabytes runs a
// 64 MiB device's window, which is why no amount of memory has ever made this
// system faster. It is also the root of both configuration asymmetries — an
// unbudgeted provider sits exactly at the reference and a budgeted client
// below it, and the download-only inversion is the two ends differing rather
// than the rule differing.
//
// A share computed by scaling a constant would carry that defect forward under
// a new name. This row exists because the wrong pattern is the local idiom:
// every adjacent line in the settings file scales a constant, and copying one
// is the natural way to write this. An assertion is the only thing that holds.
//
// Prediction, recorded before the run: the share doubles when the budget
// doubles, at budgets above the reference as well as below. A memory-scaled
// constant is flat above the reference and fails here.
func TestTheTransferShareIsADrawOnTheBudget(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	type sample struct {
		budget ByteCount
		share  ByteCount
	}
	samples := []sample{}
	for _, budget := range []ByteCount{mib(16), mib(64), mib(256), mib(1024)} {
		SetMemoryBudget(budget)
		samples = append(samples, sample{budget: budget, share: transferBudgetShareByteCount()})
	}
	for _, s := range samples {
		t.Logf("budget %d: share %d (%.3f of it)", s.budget, s.share,
			float64(s.share)/float64(s.budget))
	}

	for i := 1; i < len(samples); i += 1 {
		previous, current := samples[i-1], samples[i]
		wantRatio := float64(current.budget) / float64(previous.budget)
		gotRatio := float64(current.share) / float64(max(previous.share, 1))
		if gotRatio < 0.99*wantRatio || 1.01*wantRatio < gotRatio {
			t.Errorf(
				"the budget went from %d to %d, %.1f times, and the share went %d to %d, %.2f times; a share has to be a fraction of the budget, and one that stops growing above the reference is a memory-scaled constant wearing a new name",
				previous.budget, current.budget, wantRatio,
				previous.share, current.share, gotRatio,
			)
		}
	}

	// and the absence of a budget is the absence of the surface, not a small
	// share: an unbudgeted process keeps today's constant
	SetMemoryBudget(0)
	if share := transferBudgetShareByteCount(); share != 0 {
		t.Errorf("an unbudgeted process computed a share of %d; it has no budget to draw on", share)
	}
	settings := DefaultSendBufferSettingsWithBufferSize(defaultTransferBufferSize)
	settings.WindowSizing = WindowSizingFromDelivery
	settings.ApplyWindowSizing()
	if settings.ResendQueueBudget != nil {
		t.Errorf(
			"an unbudgeted process was given a %d byte transfer budget out of nothing",
			settings.ResendQueueBudget.TotalByteCount(),
		)
	}
	if settings.ResendQueueMaxByteCount != MemoryScaledByteCount(mib(2), kib(256)) {
		t.Errorf(
			"an unbudgeted process's constant window is %d rather than today's %d; falling to a floor here would make every unbudgeted provider slower the moment the rule is turned on, which is the opposite of the point",
			settings.ResendQueueMaxByteCount,
			MemoryScaledByteCount(mib(2), kib(256)),
		)
	}

	// the hold moves with the window, or it is the binder the moment windows
	// can grow: 2.5 MiB is 90 Mb/s at a 200 ms round trip
	SetMemoryBudget(mib(256))
	receive := DefaultReceiveBufferSettingsWithBufferSize(defaultTransferBufferSize)
	receive.WindowSizing = WindowSizingFromDelivery
	receive.ApplyWindowSizing()
	if receive.ReceiveQueueMaxByteCount != transferBudgetShareByteCount() {
		t.Errorf(
			"the hold is %d against a %d byte share; if windows can grow and holds cannot, the hold binds and the whole raise is inert above it",
			receive.ReceiveQueueMaxByteCount,
			transferBudgetShareByteCount(),
		)
	}
}
