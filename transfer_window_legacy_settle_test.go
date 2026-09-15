package connect

import (
	"testing"
	"time"
)

// The property a modern client talking to an older client actually depends on,
// which is not any single branch of the window consumer: against a peer that
// never advertises, the window settles once and stays there.
//
// The failure it rules out is the cycle - grow, overrun the peer's hold,
// provoke an eviction, retransmit, shrink, regrow. That cycle is worse than a
// small window, because each turn of it costs a retransmission and, against a
// legacy peer, the eviction is silent: `evicted_sequence_numbers` is how a
// receiver confesses an eviction and a legacy receiver cannot send it, so the
// sender holds a lease on bytes already discarded until its selective
// acknowledgement timeout expires. Branch two is the mitigation for that, and
// it is the load-bearing part: by clamping the window to this sender's own
// shipping hold, the sender never offers more than a receiver of its own
// generation can take, so the eviction is not provoked and the silence never
// matters. Nothing pinned it.
//
// So the assertion is a bound over a trajectory rather than a value at a
// point: driven with a delivery rate that rises past what the clamp permits,
// no estimate at any point exceeds the clamp, the window is non-decreasing,
// and once it reaches the clamp it stays. A tree that oscillates fails on the
// monotonicity; a tree that lets the delivery term raise the window above the
// peer branch fails on the bound; a tree that never reaches the clamp fails on
// the last check and is the regression the legacy peer would feel as a
// permanently small window.
//
// Deterministic in the strict sense: delivery is recorded at explicit
// timestamps, every estimate is taken at an explicit time, the round trip is
// sampled from explicit send and receive times, and nothing sleeps or races.
// The round trip is 50 ms so that the shipped one-gigabit target clamp sits
// well above the legacy clamp and the row reads the branch under test rather
// than the target; `TargetBound` is asserted false at every step so that a
// change to the target cannot silently turn this into a row about something
// else.
func TestAgainstALegacyPeerTheWindowSettlesOnceAndStays(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })
	SetMemoryBudget(0)

	const roundTrip = 50 * time.Millisecond
	const sampleInterval = deliverySizedWindowSampleInterval
	// twenty samples per step spans 200 ms, which is twice the rate window's
	// own minimum span at this round trip, so each step's rate is measured
	// wholly within that step
	const samplesPerStep = 20

	sequence := newEstimatorFixture(t, func(settings *SendBufferSettings) {
		settings.DeliverySizedWindowScale = deliverySizedWindowScale
		settings.TargetGoodputByteRate = targetGoodputByteRate
		// far above the legacy clamp, so the share is not the binding term
		settings.ResendQueueBudget = NewTransferMemoryBudget(mib(64))
	})
	legacyClamp := sequence.sendBufferSettings.ResendQueueMaxByteCount
	floor := sequence.sendBufferSettings.ResendQueueMinByteCount

	// the peer answers, and never carries the field: this is the whole of what
	// makes it a legacy peer
	sequence.observeReceiveWindowAdvertisement(receiveAckMessage{receiveWindowSet: false})

	// a whole second, so every millisecond conversion below is exact
	base := time.Unix(time.Now().Unix(), 0)
	for i := range 8 {
		receiveTime := base.Add(time.Duration(i) * time.Millisecond)
		sequence.rttWindow.closeSendTime(
			uint64(receiveTime.Add(-roundTrip).UnixMilli()),
			receiveTime,
		)
	}
	if sampled := sequence.rttWindow.estimate(base); sampled.Min != roundTrip {
		t.Fatalf(
			"the fixture's round trip minimum is %s rather than %s, so the delivery arithmetic below is not the arithmetic asserted",
			sampled.Min,
			roundTrip,
		)
	}

	// A delivery rate that rises past what the clamp permits. At this round
	// trip and sample interval the rate window reads ten samples, so the
	// window the delivery term allows is ten times the per-sample bytes; the
	// last three steps are above the clamp and are where a tree that lets
	// delivery raise the window would show it.
	perSampleSteps := []ByteCount{
		kib(8), kib(16), kib(32), kib(64), kib(128), kib(256), kib(512), mib(1),
	}

	at := base
	windows := make([]ByteCount, 0, len(perSampleSteps))
	reachedAt := -1
	for step, perSample := range perSampleSteps {
		for range samplesPerStep {
			at = at.Add(sampleInterval)
			sequence.observeDeliveredBytes(perSample, at)
		}
		estimate := sequence.sendWindowEstimate(at)
		windows = append(windows, estimate.Window)
		t.Logf(
			"step %d: %d per sample gives window %d ceiling %d reason %q targetBound %t sized %t",
			step, perSample, estimate.Window, estimate.Ceiling,
			estimate.Reason, estimate.TargetBound, estimate.Sized,
		)

		if !estimate.Sized {
			t.Errorf(
				"step %d reports an unsized estimate (%q); the delivery term is not acting, so this row is not measuring what it claims",
				step, estimate.Reason,
			)
		}
		if estimate.TargetBound {
			t.Errorf(
				"step %d is bound by the one-gigabit target rather than by the peer branch; this row is about the legacy clamp and a target-clamped step measures the target",
				step,
			)
		}
		if estimate.Ceiling != legacyClamp {
			t.Errorf(
				"step %d resolves a ceiling of %d rather than the legacy clamp %d; against a peer that never advertises the ceiling is this sender's own shipping hold at every point of the trajectory",
				step, estimate.Ceiling, legacyClamp,
			)
		}
		// The bound, checked at every point and not only at the end. This is
		// the mitigation: the sender never offers a legacy peer more than a
		// receiver of its own generation can hold, so the eviction it could
		// not be told about is never provoked.
		if legacyClamp < estimate.Window {
			t.Errorf(
				"step %d offers a window of %d against a legacy clamp of %d. A legacy receiver cannot confess an eviction, so an overrun here is a silent withdrawal the sender learns of only when its selective acknowledgement timeout expires",
				step, estimate.Window, legacyClamp,
			)
		}
		if estimate.Window < floor {
			t.Errorf("step %d is below the working floor: %d against %d", step, estimate.Window, floor)
		}
		if 0 < step && estimate.Window < windows[step-1] {
			t.Errorf(
				"step %d shrank from %d to %d on a rising delivery rate. The window must settle once and stay: a grow-overrun-shrink-regrow cycle costs a retransmission per turn and is the failure the clamp exists to prevent",
				step, windows[step-1], estimate.Window,
			)
		}
		if reachedAt < 0 && estimate.Window == legacyClamp {
			reachedAt = step
		}
		if 0 <= reachedAt && estimate.Window != legacyClamp {
			t.Errorf(
				"step %d left the legacy clamp, reading %d against the %d it had settled at by step %d",
				step, estimate.Window, legacyClamp, reachedAt,
			)
		}
	}

	if reachedAt < 0 {
		t.Errorf(
			"the window never reached the legacy clamp %d over a delivery rate rising to %d per sample; it ended at %d. A legacy peer that can hold the sender's own shipping hold must be offered it, or branch two is a regression rather than a status quo",
			legacyClamp,
			perSampleSteps[len(perSampleSteps)-1],
			windows[len(windows)-1],
		)
	} else {
		t.Logf(
			"settled at the legacy clamp %d from step %d of %d, and held it for the remaining %d steps",
			legacyClamp, reachedAt, len(perSampleSteps), len(perSampleSteps)-1-reachedAt,
		)
	}
	if windows[0] >= legacyClamp {
		t.Errorf(
			"the trajectory started at %d, already at or above the clamp %d, so it never rose and the settling this row asserts was not exercised",
			windows[0], legacyClamp,
		)
	}
}
