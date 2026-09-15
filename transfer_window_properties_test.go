package connect

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// Builds a sequence whose estimator can be driven by injected values rather
// than by racing a real transfer. Deterministic by construction: no timers, no
// goroutines, no dependence on how fast the machine is.
func newEstimatorFixture(
	t *testing.T,
	configure func(*SendBufferSettings),
) *SendSequence {
	t.Helper()
	settings := DefaultSendBufferSettings()
	if configure != nil {
		configure(settings)
	}
	return &SendSequence{
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
}

// Closes enough round trips against the wall clock the window reads that the
// estimate is sampled at about the given value.
func sampleRoundTrip(sequence *SendSequence, roundTrip time.Duration) {
	for range 8 {
		sequence.rttWindow.CloseSendTime(
			uint64(time.Now().Add(-roundTrip).UnixMilli()))
	}
}

// THROUGHPUTFIX §37.21: the window steps to the peer's advertised capacity the
// moment it is learned, rather than climbing toward it.
//
// The step is what makes the derived initial size worth having. A blind sender
// assumes only the receive hold's floor, which every receiver already ships;
// the moment the first acknowledgement carries a capacity the sender knows the
// real bound, and the largest harmless window is that capacity, because
// permission is not occupancy and the one harm of an oversized window is the
// overrun the advertisement bounds. A sender that ramped toward it instead
// would spend the ramp for nothing.
//
// Predictions, recorded before the run: blind, the window is the receive hold's
// floor; with a capacity advertised it is that capacity on the very next
// estimate, with no intermediate value.
func TestTheWindowStepsToTheAdvertisedCapacity(t *testing.T) {
	const advertised = ByteCount(4 * 1024 * 1024)
	sequence := newEstimatorFixture(t, func(settings *SendBufferSettings) {
		settings.DeliverySizedWindowScale = deliverySizedWindowScale
		settings.ResendQueueBudget = NewTransferMemoryBudget(mib(64))
	})

	blind := sequence.sendWindowEstimate(time.Now())
	if blind.Window != defaultInitialWindowByteCount() {
		t.Errorf(
			"a blind sender's window is %d rather than the %d byte receive hold floor it may assume of any peer",
			blind.Window, defaultInitialWindowByteCount(),
		)
	}

	// the first acknowledgement carries a capacity
	sequence.observeReceiveWindowAdvertisement(receiveAckMessage{
		receiveWindowSet:       true,
		receiveWindowByteCount: uint32(advertised),
	})
	stepped := sequence.sendWindowEstimate(time.Now())
	t.Logf(
		"blind %d, then advertised %d gives window %d ceiling %d reason %q",
		blind.Window, advertised, stepped.Window, stepped.Ceiling, stepped.Reason,
	)
	if stepped.Window != advertised {
		t.Errorf(
			"the window is %d on the estimate after a %d byte capacity was advertised; it steps to the peer's capacity rather than climbing toward it, and a ramp here is spent for nothing",
			stepped.Window, advertised,
		)
	}
}

// THROUGHPUTFIX §37.21: the delivery term is one-sided and lagged.
//
// One-sided, so it may lower the window below the peer's capacity but never
// raise it above. Lagged, so it acts only on delivery measured wholly after the
// window stepped. Both are needed together: after the step, the delivery
// measured during the blind round trip is small, and a two-sided cap reading it
// would drag the window straight back down and reimpose the ramp the step
// exists to remove.
//
// Predictions, recorded before the run: delivery recorded before the step does
// not lower the window, and the estimate says so; delivery recorded after the
// step does lower it.
func TestTheDeliveryCapIsOneSidedAndLagged(t *testing.T) {
	const advertised = ByteCount(4 * 1024 * 1024)
	const roundTrip = 50 * time.Millisecond
	sequence := newEstimatorFixture(t, func(settings *SendBufferSettings) {
		settings.DeliverySizedWindowScale = deliverySizedWindowScale
		settings.ResendQueueBudget = NewTransferMemoryBudget(mib(64))
	})
	sampleRoundTrip(sequence, roundTrip)

	// delivery from before the step: a small blind round trip's worth
	now := time.Now()
	for i := range 40 {
		sequence.observeDeliveredBytes(
			ByteCount(8*1024), now.Add(-400*time.Millisecond+time.Duration(i)*10*time.Millisecond))
	}
	sequence.observeReceiveWindowAdvertisement(receiveAckMessage{
		receiveWindowSet:       true,
		receiveWindowByteCount: uint32(advertised),
	})

	lagged := sequence.sendWindowEstimate(time.Now())
	t.Logf("with delivery from before the step: window %d, reason %q", lagged.Window, lagged.Reason)
	if lagged.Window != advertised {
		t.Errorf(
			"the window is %d against the %d advertised, lowered by delivery that was measured before it stepped; without the lag the step is undone by the very round trip it replaced",
			lagged.Window, advertised,
		)
	}
	if lagged.Reason != "delivery measured before the window stepped" {
		t.Errorf(
			"the estimate says %q rather than naming the lag; a window that holds for a reason it does not report is one a campaign cannot read",
			lagged.Reason,
		)
	}

	// and delivery measured after the step does lower it
	after := time.Now()
	for i := range 40 {
		sequence.observeDeliveredBytes(
			ByteCount(8*1024), after.Add(time.Duration(i)*10*time.Millisecond))
	}
	lowered := sequence.sendWindowEstimate(after.Add(400 * time.Millisecond))
	t.Logf("with delivery from after the step: window %d, reason %q", lowered.Window, lowered.Reason)
	if advertised <= lowered.Window {
		t.Errorf(
			"the window is %d against the %d advertised; delivery measured wholly after the step is the evidence the cap exists to act on",
			lowered.Window, advertised,
		)
	}
}

// The identity that resolved a whole afternoon's hunt, and which nothing
// asserted: at equilibrium with the ceiling above twice the delivery, occupancy
// is about half the window.
//
// It is not a defect. A window of twice the delivery per round trip, filled at
// the delivery rate, rests with about one round trip's worth outstanding, which
// is half of what it permits. The scale is what puts it there and the reason it
// is two rather than one is stability under dips. Asserting it matters because
// the half was read as a shortfall for most of a day, and a tree where it does
// not hold has a defect somewhere in the loop.
//
// Derived rather than counted, and named as such: occupancy is the resend
// queue's own byte count sampled during the transfer, while the window is the
// estimator's reported value, so the ratio is a ratio of two measured
// quantities rather than a modelled one.
//
// Prediction, recorded before the run: with the ceiling far above twice the
// delivery, mean occupancy is near half the reported window.
//
// Measured, and the prediction is not what the fixture shows. Paired at six
// hundred ticks over four runs it reads 0.71, 0.68, 0.68, 0.68 — stable to two
// figures, and about two thirds rather than one half. The half is the design's
// idealisation of the fixed point, which assumes the window is recomputed from
// a delivery rate that is itself the window over exactly one round trip; the
// estimator recomputes continuously from a ring spanning several round trips,
// so the rested occupancy sits above the idealised value. The row asserts the
// band the measurement supports and says so, rather than ratifying a number the
// fixture does not produce.
//
// What it still pins is the thing worth pinning: occupancy rests well below the
// window and well above zero. A tree where it approaches the window has lost
// the delivery term, and one where it collapses toward zero has lost the
// window.
//
// Two instrument defects were found writing this row, both of the class this
// program keeps paying for. The first measured the peak and read 0.77 to 0.94:
// at the fixed point the queue oscillates between nearly empty and nearly full
// as each burst is admitted and acknowledged, so the peak approaches the window
// by construction and says nothing about where the flow rests. The second
// averaged occupancy over the whole run and divided by a single end-of-run
// window, which reads 1.76, 0.87 and 0.60 across three runs while the window
// itself swings from 262 KB to 1.07 MB — a mean over one population divided by
// a sample from another, which is exactly the comparison behind the figure this
// program retracted. The ratio is formed pairwise at each tick now, from the
// occupancy and the window as they stand together, and averaged over those
// ratios.
func TestAtEquilibriumOccupancyIsHalfTheWindow(t *testing.T) {
	assertMessagePoolOwnership(t)

	const propagation = 25 * time.Millisecond
	const payloadByteCount = 4 * 1024

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := newSendWindowHarness(t, ctx, propagation,
		func(settings *SendBufferSettings) {
			settings.DeliverySizedWindowScale = deliverySizedWindowScale
			// far above twice the delivery, so the clamp never binds and the
			// delivery term is what sets the window
			settings.ResendQueueBudget = NewTransferMemoryBudget(mib(64))
			settings.DeliverySizedWindowCeilingByteCount = mib(64)
			// the target clamp is a separate mechanism with its own rows,
			// derived whenever the rule is switched on; this row measures the
			// fixed point, so the clamp is held out
			settings.TargetGoodputByteRate = 0
		})
	harness.receiveHold(mib(64))

	// paired at each tick: occupancy and the window as they stand together
	ratioTotal := &atomic.Int64{}
	ratioSamples := &atomic.Int64{}
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
				window := harness.sender.
					DestinationSendStats(harness.receiverId).SendWindow
				if window.Window <= 0 || !window.Sized {
					continue
				}
				// in parts per thousand, so the average is integer arithmetic
				ratioTotal.Add(int64(queued) * 1000 / int64(window.Window))
				ratioSamples.Add(1)
			}
		}
	}()
	harness.offer(t, payloadByteCount, 3*time.Second)
	estimate := harness.sender.DestinationSendStats(harness.receiverId).SendWindow
	cancel()
	<-watching

	if ratioSamples.Load() == 0 {
		t.Fatal("occupancy was never sampled against a sized window, so this cell reads nothing")
	}
	ratio := float64(ratioTotal.Load()) / float64(ratioSamples.Load()) / 1000
	t.Logf(
		"mean of occupancy over window, paired at %d ticks: %.2f; last window %d, ceiling %d, reason %q",
		ratioSamples.Load(), ratio, estimate.Window,
		estimate.Ceiling, estimate.Reason,
	)

	if estimate.Reason != "delivery" {
		t.Fatalf(
			"the window's binding term is %q rather than delivery, so this cell is not at the unclamped equilibrium the identity describes",
			estimate.Reason,
		)
	}
	if ratio < 0.45 || 0.85 < ratio {
		t.Errorf(
			"occupancy rests at %.2f of the window, outside the 0.45 to 0.85 band four runs put it in at 0.68 to 0.71; approaching the window means the delivery term has stopped binding, and collapsing toward zero means the window has, and either is a defect in the loop",
			ratio,
		)
	}
}

// A fixed window is filled exactly, and growth doubles from a full window while
// it stalls from a half-filled one.
//
// The asymmetry is the point. Growth is the scale times what the lane delivered
// per round trip, and delivery is what the window let through, so a window that
// is full delivers its whole self per round trip and doubles; a window that is
// half full delivers half of itself and computes the window it already has.
// That is why the clamped regime, where the window is the ceiling outright and
// the sender fills it on the first round trip, grows cleanly, and why the
// unclamped equilibrium rests at half instead of climbing. It was established
// by campaign measurement in a fixture that has retracted two of its own
// numbers, and nothing in process asserted it.
//
// Derived rather than counted, and named as such: the per-round-trip delivery
// below is the estimator's own reported DeliveredByteCount scaled by
// RoundTrip/Interval, which is the same arithmetic the rule uses, so this
// asserts the rule against its own published evidence rather than against an
// independently measured rate.
//
// Predictions, recorded before the run: from a full window the next window is
// about twice it; from a half-filled one it is about the window itself.
func TestGrowthDoublesFromAFullWindowAndStallsFromAHalfFilledOne(t *testing.T) {
	const roundTrip = 50 * time.Millisecond
	const window = ByteCount(1024 * 1024)

	grown := func(deliveredPerRoundTrip ByteCount) SendWindowEstimate {
		sequence := newEstimatorFixture(t, func(settings *SendBufferSettings) {
			settings.DeliverySizedWindowScale = deliverySizedWindowScale
			settings.ResendQueueBudget = NewTransferMemoryBudget(mib(64))
			settings.ResendQueueMinByteCount = ByteCount(64 * 1024)
		})
		sampleRoundTrip(sequence, roundTrip)
		sequence.observeReceiveWindowAdvertisement(receiveAckMessage{
			receiveWindowSet:       true,
			receiveWindowByteCount: uint32(mib(64)),
		})
		// delivery laid down after the step, at the given rate per round trip
		now := time.Now()
		const samples = 40
		const spacing = 10 * time.Millisecond
		perSample := ByteCount(
			int64(deliveredPerRoundTrip) * int64(spacing) / int64(roundTrip))
		for i := range samples {
			sequence.observeDeliveredBytes(perSample, now.Add(time.Duration(i)*spacing))
		}
		return sequence.sendWindowEstimate(now.Add(samples * spacing))
	}

	full := grown(window)
	half := grown(window / 2)
	t.Logf(
		"delivering a full %d window per round trip gives %d (%.2fx); delivering half gives %d (%.2fx)",
		window, full.Window, float64(full.Window)/float64(window),
		half.Window, float64(half.Window)/float64(window),
	)

	if ratio := float64(full.Window) / float64(window); ratio < 1.8 || 2.2 < ratio {
		t.Errorf(
			"a full window delivered per round trip gives a next window of %d, %.2f times it, rather than about twice; growth is the scale times delivery and a full window delivers its whole self",
			full.Window, ratio,
		)
	}
	if ratio := float64(half.Window) / float64(window); ratio < 0.8 || 1.2 < ratio {
		t.Errorf(
			"a half-filled window gives a next window of %d, %.2f times the window it already had, rather than about the same; that stall is why the unclamped equilibrium rests at half and does not climb",
			half.Window, ratio,
		)
	}
	if half.Window >= full.Window {
		t.Errorf(
			"the half-filled window grew to %d against the full window's %d; the asymmetry between them is the whole reason a clamped window grows cleanly and an unclamped one rests",
			half.Window, full.Window,
		)
	}
}
