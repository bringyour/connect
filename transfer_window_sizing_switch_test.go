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
//
// The rule now ships on, so this row sets the policy rather than reading the
// default: it is the rollback that is pinned here, and the rollback is one
// SetWindowSizing call in the other direction. The row below owns the default.
func TestTheWindowSizingSwitchOffIsTodaysBehaviour(t *testing.T) {
	// Both facts in one place: what the tree ships today, and that the
	// rollback still reaches exactly what it shipped before. Reading the
	// default and asserting it was off is what made this row red the moment
	// the rule landed, which §0.2 of THROUGHPUT-TESTGAPS is about — the
	// workflow runs the whole package on every push, so one row red by
	// construction masks every genuine failure in a full run.
	if shipped := DefaultSendBufferSettings().WindowSizing; shipped != WindowSizingFromDelivery {
		t.Errorf(
			"the shipping window sizing policy is %d rather than from-delivery. If the rule was rolled back deliberately, this row's framing is what needs updating; if it changed by accident, the whole program's landing is off",
			shipped,
		)
	}

	// The rollback as a host performs it: one process-wide call, after which
	// every settings constructor builds the constant regime.
	defer SetWindowSizing(DefaultWindowSizing())
	SetWindowSizing(WindowSizingConstant)
	settings := DefaultSendBufferSettings()
	if settings.WindowSizing != WindowSizingConstant {
		t.Fatalf("the rolled-back policy is %d rather than the constant window", settings.WindowSizing)
	}
	// the constant window itself, which is what a tree without this program
	// sizes every sequence at
	if want := MemoryScaledByteCount(mib(2), kib(256)); settings.ResendQueueMaxByteCount != want {
		t.Errorf(
			"the rolled-back window is %d rather than the %d a tree without this program uses",
			settings.ResendQueueMaxByteCount,
			want,
		)
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

	// the wire: an acknowledgement under the ROLLED-BACK configuration carries
	// neither the advertised capacity nor an eviction notice, which is what
	// makes the rollback byte for byte what a tree without this program writes
	receiveSettings := DefaultReceiveBufferSettings()
	if receiveSettings.AdvertiseReceiveWindow {
		t.Error("the receiver still advertises its capacity under the constant policy, which changes the wire and makes the rollback untrustworthy")
	}
	if want := MemoryScaledByteCount(mib(2)+kib(512), kib(320)); receiveSettings.ReceiveQueueMaxByteCount != want {
		t.Errorf(
			"the rolled-back receive hold is %d rather than the %d a tree without this program holds",
			receiveSettings.ReceiveQueueMaxByteCount,
			want,
		)
	}
	if receiveSettings.ReceiveQueueBudget != nil {
		t.Error("the rolled-back receiver kept a shared budget, so the off path is not today's")
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

// The switch, thrown. Every ceiling this program raises sits above the
// transfer window, so with the rule off the raises are unreachable: the
// constant window is `MemoryScaledByteCount(mib(2), kib(256))`, which is 2 MiB
// at or above the reference budget whatever the host has, and the share a
// sequence may draw is not consulted at all under the constant policy.
//
// This row fails the day the default goes back to the constant without anyone
// meaning it, which is how the switch came to be built, wired and never
// thrown. It also pins the thing that makes the flip safe: zero still means
// constant, so a stored policy keeps its meaning.
func TestTheShippingWindowSizingDefaultIsTheRule(t *testing.T) {
	if policy := DefaultWindowSizing(); policy != WindowSizingFromDelivery {
		t.Fatalf(
			"the process ships with window sizing policy %d rather than the delivery-sized rule; every ceiling above the transfer window is inert while this is the constant",
			policy,
		)
	}
	var stored WindowSizingPolicyKind
	if stored != WindowSizingConstant {
		t.Fatal("the zero policy is no longer the constant, so anything that stored a policy has silently changed meaning")
	}

	settings := DefaultSendBufferSettings()
	if settings.WindowSizing != WindowSizingFromDelivery {
		t.Fatalf("the shipping send settings carry policy %d", settings.WindowSizing)
	}
	if settings.DeliverySizedWindowScale != deliverySizedWindowScale {
		t.Errorf(
			"the shipping scale is %d rather than the derived %d",
			settings.DeliverySizedWindowScale,
			deliverySizedWindowScale,
		)
	}
	if settings.TargetGoodputByteRate != targetGoodputByteRate {
		t.Errorf(
			"the shipping target is %d rather than the derived %d bytes per second",
			settings.TargetGoodputByteRate,
			targetGoodputByteRate,
		)
	}
	// Left unset deliberately: the ceiling is the share, read from the queue's
	// own budget at estimate time, and a total frozen here is what made a
	// budget attached after apply read zero.
	if settings.DeliverySizedWindowCeilingByteCount != 0 {
		t.Errorf(
			"the shipping settings froze a %d byte ceiling, which a budget attached after apply cannot correct",
			settings.DeliverySizedWindowCeilingByteCount,
		)
	}
	if !DefaultReceiveBufferSettings().AdvertiseReceiveWindow {
		t.Error("the shipping receiver does not advertise its capacity, so every sender stays blind and holds the initial bet")
	}
}

// The unbudgeted process, which is the one that has to be exactly today's
// rather than nearly: the hosted proxy and every host that never calls
// SetMemoryBudget take this path, and a default that changes them cannot be
// rolled back by a host that does not know it is on it.
//
// Exactly, on the send side, and this row states where it stops. The window a
// sequence admits against is the constant, by the same number, because the
// share is zero, the queue has no pool, and the estimate returns before any
// derived term is applied — so admission calls CanAdd with the same bytes it
// called with under the constant policy. What is not identical, named here so
// the difference is not discovered later: the sequence allocates the delivery
// ring the rule samples into, and the receiver advertises its hold on every
// acknowledgement, which is a wire change on an unbudgeted process too. The
// advertisement is the rule working as designed — the hold it advertises is
// the same constant hold — but it is a difference, and "exactly today's"
// applies to the window and the admission, not to the bytes on the wire.
func TestTheUnbudgetedProcessKeepsTodaysWindowUnderTheRule(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })
	SetMemoryBudget(0)

	ruled := DefaultSendBufferSettings()
	if ruled.WindowSizing != WindowSizingFromDelivery {
		t.Fatalf("the shipping policy is %d rather than the rule", ruled.WindowSizing)
	}
	if ruled.ResendQueueBudget != nil {
		t.Fatal("the rule attached a pool to a process with no budget, so the share is being read as a small number rather than as the absence of the surface")
	}
	if ruled.WindowSizingActive() {
		t.Fatal("the rule reports itself active with nothing to draw on")
	}

	constant := DefaultSendBufferSettings()
	constant.WindowSizing = WindowSizingConstant
	constant.ApplyWindowSizing()

	ruledEstimate := windowEstimateForSettings(ruled)
	constantEstimate := windowEstimateForSettings(constant)
	t.Logf(
		"unbudgeted: rule window %d reason %q against constant window %d reason %q",
		ruledEstimate.Window, ruledEstimate.Reason,
		constantEstimate.Window, constantEstimate.Reason,
	)
	if ruledEstimate.Window != constantEstimate.Window {
		t.Errorf(
			"the rule admits against a %d byte window where the constant admits against %d; an unbudgeted process must not change when the rule is turned on",
			ruledEstimate.Window,
			constantEstimate.Window,
		)
	}
	if ruledEstimate.Window != ruled.ResendQueueMaxByteCount {
		t.Errorf(
			"the window is %d rather than the constant %d the settings carry",
			ruledEstimate.Window,
			ruled.ResendQueueMaxByteCount,
		)
	}
	if ruledEstimate.Sized {
		t.Error("the rule reported a sized window with no budget to size against")
	}
	// the reason is the difference a reader has: same number, and it says why
	if ruledEstimate.Reason == constantEstimate.Reason {
		t.Errorf(
			"both policies report %q; an unbudgeted process under the rule should say it is holding the constant for want of a budget",
			ruledEstimate.Reason,
		)
	}

	// and the receive side, where it is not identical: the hold is the same
	// constant and it is now advertised
	receive := DefaultReceiveBufferSettings()
	if !receive.AdvertiseReceiveWindow {
		t.Error("the unbudgeted receiver does not advertise, so a peer cannot size to it at all")
	}
	if receive.ReceiveQueueBudget != nil {
		t.Error("the unbudgeted receiver drew a pool from a budget that does not exist")
	}
	if want := MemoryScaledByteCount(mib(2)+kib(512), kib(320)); receive.ReceiveQueueMaxByteCount != want {
		t.Errorf(
			"the unbudgeted hold is %d rather than today's constant %d",
			receive.ReceiveQueueMaxByteCount,
			want,
		)
	}
}

// What the rule derives at the budgets that ship, so a later change to a
// divisor cannot quietly produce a window smaller than the constant it
// replaced. Every expected value here is computed from the budget by the same
// arithmetic the code uses, not copied from a run.
//
// The comparison that matters is at one budget: the rule's ceiling against the
// constant window the same host would otherwise have used. A raise has to hold
// at every budget, including the ones where the memory scale has already cut
// the constant down.
//
// The unbudgeted process is the case to read carefully, because the hosted
// proxy and any host that never calls SetMemoryBudget take it. There the share
// is zero — the absence of the surface, not a small share — so the rule stays
// inert and the sequence keeps today's constant rather than falling to a
// floor.
func TestTheDerivedWindowQuantitiesAtTheShippedBudgets(t *testing.T) {
	defer SetMemoryBudget(0)
	budgets := []struct {
		name            string
		budgetByteCount ByteCount
	}{
		{"unbudgeted", 0},
		{"the 8 MiB legacy host target", mib(8)},
		{"a 20 MiB device target", mib(20)},
		{"a 24 MiB device target", mib(24)},
		{"the 32 MiB phone budget", mib(32)},
		{"the 64 MiB reference", mib(64)},
		{"a 256 MiB desktop budget", mib(256)},
		{"an 8 GiB provider", gib(8)},
	}
	for _, budget := range budgets {
		SetMemoryBudget(budget.budgetByteCount)
		// today's window and hold at this same budget: what the host would
		// have had with the rule off
		constantWindow := MemoryScaledByteCount(mib(2), kib(256))
		constantHold := MemoryScaledByteCount(mib(2)+kib(512), kib(320))
		share := transferBudgetShareByteCount()
		send := DefaultSendBufferSettings()
		receive := DefaultReceiveBufferSettings()

		if budget.budgetByteCount <= 0 {
			if 0 != share {
				t.Errorf("%s: the share is %d rather than nothing", budget.name, share)
			}
			if send.ResendQueueBudget != nil {
				t.Errorf("%s: the rule attached a budget to a process that has none", budget.name)
			}
			if send.WindowSizingActive() {
				t.Errorf("%s: the rule reports itself active with no budget to draw on", budget.name)
			}
			// the initial bet on this path, which is the whole window there
			if send.ResendQueueMaxByteCount != constantWindow {
				t.Errorf(
					"%s: the window is %d rather than today's constant %d, so an unbudgeted host changed when the rule was turned on",
					budget.name,
					send.ResendQueueMaxByteCount,
					constantWindow,
				)
			}
			if receive.ReceiveQueueMaxByteCount != constantHold {
				t.Errorf(
					"%s: the hold is %d rather than today's constant %d",
					budget.name,
					receive.ReceiveQueueMaxByteCount,
					constantHold,
				)
			}
			t.Logf(
				"%s: share none, window %d (today's constant), hold %d, rule inert",
				budget.name, send.ResendQueueMaxByteCount, receive.ReceiveQueueMaxByteCount,
			)
			continue
		}

		if want := budget.budgetByteCount / transferBudgetShareDivisor; share != want {
			t.Errorf("%s: the share is %d rather than the budget's eighth %d", budget.name, share, want)
		}
		if send.ResendQueueBudget == nil {
			t.Fatalf("%s: the rule attached no budget to a budgeted process", budget.name)
		}
		if total := send.ResendQueueBudget.TotalByteCount(); total != share {
			t.Errorf("%s: the send pool holds %d rather than the share %d", budget.name, total, share)
		}
		if !send.WindowSizingActive() {
			t.Errorf("%s: the rule is inert at a budget that has a share", budget.name)
		}
		// The ceiling a lone sequence reads: the pool less the floors
		// guaranteed to other attached queues, of which there are none here.
		ceiling := send.ResendQueueBudget.LendableByteCount(send.ResendQueueMinByteCount)
		if ceiling != share {
			t.Errorf("%s: a lone queue's ceiling is %d rather than the share %d", budget.name, ceiling, share)
		}
		if ceiling < constantWindow {
			t.Errorf(
				"%s: the rule's ceiling %d is below the constant window %d it replaces, which is a regression rather than a raise",
				budget.name,
				ceiling,
				constantWindow,
			)
		}
		// the hold moves with the window, or it becomes the binder the moment
		// windows can grow
		if want := max(share, receive.ReceiveQueueMinByteCount); receive.ReceiveQueueMaxByteCount != want {
			t.Errorf("%s: the hold is %d rather than %d", budget.name, receive.ReceiveQueueMaxByteCount, want)
		}
		if receive.ReceiveQueueMaxByteCount < constantHold {
			t.Errorf(
				"%s: the hold %d is below the constant hold %d it replaces",
				budget.name,
				receive.ReceiveQueueMaxByteCount,
				constantHold,
			)
		}
		if receive.ReceiveQueueBudget == nil {
			t.Errorf("%s: the receiver drew no shared budget", budget.name)
		}
		// the blind bet, which is what a sender may hold before it has heard
		// anything: the receive hold's floor, never more than its own constant
		blind := min(defaultInitialWindowByteCount(), send.ResendQueueMaxByteCount)
		if blind <= 0 || send.ResendQueueMaxByteCount < blind {
			t.Errorf(
				"%s: the blind bet is %d against a constant window of %d",
				budget.name,
				blind,
				send.ResendQueueMaxByteCount,
			)
		}
		t.Logf(
			"%s: share %d, ceiling %d against today's window %d, hold %d against today's %d, initial %d, blind %d",
			budget.name, share, ceiling, constantWindow,
			receive.ReceiveQueueMaxByteCount, constantHold,
			send.ResendQueueMaxByteCount, blind,
		)
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

	// The rule needs a process budget to draw on or it is not active at all,
	// and the first version of this row did not set one: its "sized" arm was
	// the constant arm and the row measured nothing. Asserted below rather
	// than assumed, because that is exactly how it went unnoticed.
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })
	SetMemoryBudget(mib(256))

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
		if sizing == WindowSizingFromDelivery {
			probe := DefaultSendBufferSettings()
			probe.WindowSizing = sizing
			probe.ApplyWindowSizing()
			if !probe.WindowSizingActive() {
				t.Fatal("the rule is not active, so the sized arm is the constant arm and this row measures nothing")
			}
		}
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

// The row that would have caught the defect that made the whole rule inert.
//
// The resolved ceiling was derived inside a test on the process share, so a
// caller that attached its own budget got no ceiling at all: the estimate fell
// back to the initial size, the resolved ceiling read exactly 2 MiB at process
// budgets of 16, 64, 256 and 1024 MiB alike, and the rule computed about eight
// megabytes from delivery and clamped straight back to where it started. A
// sixty-four-fold increase in memory moved the window not at all, and measured
// end to end the whole fix came in four per cent below the constant it was
// replacing.
//
// The lesson for the row rather than for the code: every other term was
// behaving — the scale, the interval, the target, the budget attached — so a
// test on any of them passed. What no row asserted was the one number the rule
// actually clamps to, and that is the number to assert.
//
// Predictions, recorded before the run: the resolved ceiling equals the
// attached budget's total and moves with it, at every process budget and for a
// budget the caller supplies as well as one derived; and a switch turned on
// with nothing to draw on reports that it is not active rather than reading on
// and doing nothing.
func TestTheResolvedCeilingMovesWithTheBudget(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	for _, processBudget := range []ByteCount{mib(16), mib(64), mib(256), mib(1024)} {
		SetMemoryBudget(processBudget)
		settings := DefaultSendBufferSettings()
		settings.WindowSizing = WindowSizingFromDelivery
		settings.ApplyWindowSizing()
		share := transferBudgetShareByteCount()
		// resolved, not configured: the ceiling is the share read from the
		// queue's own budget at estimate time, and the configured setting is
		// deliberately left unset
		estimate := windowEstimateForSettings(settings)
		t.Logf(
			"process budget %d: share %d, resolved ceiling %d, window %d, active %t",
			processBudget, share, estimate.Ceiling, estimate.Window,
			settings.WindowSizingActive(),
		)
		if !settings.WindowSizingActive() {
			t.Errorf("the rule is not active at a %d byte process budget", processBudget)
		}
		if estimate.Ceiling != share {
			t.Errorf(
				"the resolved ceiling is %d against a %d byte share at a %d byte process budget; the ceiling is the number the rule clamps to, so a ceiling that does not move with the budget is a rule that cannot grow whatever else is right",
				estimate.Ceiling, share, processBudget,
			)
		}
	}

	// a budget the caller attaches, with no process budget set at all: this is
	// the exact configuration the defect hid in
	SetMemoryBudget(0)
	attached := DefaultSendBufferSettings()
	attached.WindowSizing = WindowSizingFromDelivery
	attached.ResendQueueBudget = NewTransferMemoryBudget(mib(32))
	attached.ApplyWindowSizing()
	if !attached.WindowSizingActive() {
		t.Error("a caller's own attached budget did not make the rule active")
	}
	if ceiling := windowEstimateForSettings(attached).Ceiling; ceiling != mib(32) {
		t.Errorf(
			"an attached %d byte budget resolved to a %d byte ceiling; the ceiling comes from whatever budget is attached, derived or given",
			mib(32), ceiling,
		)
	}

	// and nothing to draw on has to say so rather than reading on
	SetMemoryBudget(0)
	empty := DefaultSendBufferSettings()
	empty.WindowSizing = WindowSizingFromDelivery
	empty.ApplyWindowSizing()
	if empty.WindowSizingActive() {
		t.Error("the rule reports active with no budget to draw on")
	}
	if empty.ResendQueueMaxByteCount != MemoryScaledByteCount(mib(2), kib(256)) {
		t.Errorf("an unbudgeted process lost today's constant: %d", empty.ResendQueueMaxByteCount)
	}
}

// The test that would have caught both faults in the ceiling term, written
// before the fix and expected to fail on the tree as it stands.
//
// The harness attached its budgets after calling apply, which is the ordinary
// order for a caller that builds settings and then decides what pool to give
// them. Apply froze the budget total into the configured ceiling at apply
// time, so a budget attached afterwards left that setting at zero; the
// configured ceiling then defaulted to the initial size, and every later term
// became a minimum taken against two mebibytes. Either fault alone is enough.
//
// Prediction, recorded before the run: with the budget attached after apply,
// the resolved ceiling reads 2, 8, 32 and 128 MiB as the budget grows, and the
// window follows it.
func TestTheCeilingReadsABudgetAttachedAfterApply(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })
	SetMemoryBudget(0)

	for _, share := range []ByteCount{mib(2), mib(8), mib(32), mib(128)} {
		settings := DefaultSendBufferSettings()
		settings.WindowSizing = WindowSizingFromDelivery
		settings.ApplyWindowSizing()
		// attached after apply, which is the order that broke it
		settings.ResendQueueBudget = NewTransferMemoryBudget(share)

		estimate := windowEstimateForSettings(settings)
		t.Logf(
			"budget %d attached after apply: configured ceiling %d, resolved ceiling %d, window %d, reason %q",
			share, settings.DeliverySizedWindowCeilingByteCount,
			estimate.Ceiling, estimate.Window, estimate.Reason,
		)
		if estimate.Ceiling != share {
			t.Errorf(
				"the resolved ceiling is %d against a %d byte budget attached after apply; the share is the queue's own budget read at estimate time, not a total frozen into a setting when apply happened to run",
				estimate.Ceiling, share,
			)
		}
	}
}

// The resolved estimate for one settings object, with the peer's advertised
// capacity set far above anything under test so the share is the binding term.
// A sequence that has heard nothing takes the blind bet, which is correct and
// is not what these rows are about.
func windowEstimateForSettings(settings *SendBufferSettings) SendWindowEstimate {
	sequence := &SendSequence{
		sendBufferSettings: settings,
		resendQueue: newResendQueue(
			settings.ResendQueueBudget,
			settings.ResendQueueMinByteCount,
		),
		deliveredBytes: make([]deliveredBytesSample, deliveredBytesRingSize),
		rttWindow: NewRttWindow(
			NewNoopLogger(),
			settings.RttWindowSize,
			settings.RttWindowTimeout,
			settings.RttScale,
			settings.MinResendInterval,
			settings.RttMinResendInterval,
			settings.MaxResendInterval,
		),
	}
	sequence.ackSeen.Store(true)
	sequence.receiveWindowByteCount.Store(uint64(mib(4096)))
	sequence.receiveWindowSet.Store(true)
	return sequence.sendWindowEstimate(time.Now())
}

// The same interval defect, found in a second admission bound while tracing
// which term governs admission.
//
// THROUGHPUTFIX §36.7 corrected the window rule to multiply by the measured
// minimum round trip rather than by ScaledRtt, the retransmit pacing estimate,
// which is floored at RttMinResendInterval, 300 ms. The reliable admission
// bound of FLIGHTGATEFIX §22 read the same floored timer: it admits what the
// lane delivered over one horizon, and with that horizon at 300 ms on a 25 ms
// path it admits twelve round trips of delivery rather than one. Measured
// across three paths before the window correction: 300 ms flat against real
// round trips of 6.7, 27 and 102 ms, 2.9 to 44.8 times over.
//
// It ships off, so it has never been the binder. That is why it survived: a
// value derived correctly in one place and read from the wrong source in
// another, which is the fourth instance of that shape in this rule and the
// reason the remedy is one owner of the effective window rather than four
// separate corrections.
//
// Prediction, recorded before the run: on a path whose minimum round trip is
// well under the resend floor, the admission bound reads the path, so it is far
// below what the floored timer would have admitted.
func TestTheReliableAdmissionBoundReadsThePathNotTheResendFloor(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.ReliableAdmissionBoundedByDelivery = true
	sequence := &SendSequence{
		sendBufferSettings: settings,
		resendQueue:        newResendQueue(nil, 0),
		deliveredBytes:     make([]deliveredBytesSample, deliveredBytesRingSize),
		rttWindow: NewRttWindow(
			NewNoopLogger(),
			settings.RttWindowSize,
			settings.RttWindowTimeout,
			settings.RttScale,
			settings.MinResendInterval,
			settings.RttMinResendInterval,
			settings.MaxResendInterval,
		),
	}

	// a 20 ms path, closed against the wall clock the window reads, and
	// delivery spread evenly across 400 ms of history ending now
	const roundTrip = 20 * time.Millisecond
	const perSample = ByteCount(100 * 1024)
	now := time.Now()
	for i := range 40 {
		at := now.Add(-400*time.Millisecond + time.Duration(i)*10*time.Millisecond)
		sequence.observeDeliveredBytes(perSample, at)
	}
	for range 8 {
		sequence.rttWindow.CloseSendTime(uint64(time.Now().Add(-roundTrip).UnixMilli()))
	}
	if estimate := sequence.rttWindow.Estimate(); !estimate.Sampled() ||
		roundTrip*2 < estimate.Min {
		t.Fatalf(
			"the fixture's own round trip reads %s against the %s it meant to set, so this cell cannot say which horizon the bound used",
			estimate.Min, roundTrip,
		)
	}

	limit := sequence.reliableAdmissionByteLimit(now)
	overTheFloor := sequence.deliveredBytesOver(settings.RttMinResendInterval, now)
	t.Logf(
		"admission limit %d over a %s path; the %s resend floor would have admitted %d",
		limit, roundTrip, settings.RttMinResendInterval, overTheFloor,
	)
	if overTheFloor <= limit {
		t.Errorf(
			"the admission bound admitted %d against the %d the floored resend timer would have; on a %s path the timer reads fifteen round trips, and a bound that admits fifteen round trips of delivery is not a bandwidth-delay product",
			limit, overTheFloor, roundTrip,
		)
	}
}
