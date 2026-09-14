package connect

// FLIGHTGATEFIX §25.2. The retransmit timer as RFC 6298 reads it: the mean
// round trip plus four deviations. The relay of the exchange cells has a
// mean under a second and routinely goes 2.75 s without acknowledging, an
// excursion of three times the mean that a fixed margin cannot cover
// without lengthening every retransmit on every lane.

import (
	"testing"
	"time"
)

// deviationWindow is a window fed a stated set of samples, in order.
func deviationWindow(t testing.TB, samples ...time.Duration) *RttWindow {
	t.Helper()
	settings := DefaultSendBufferSettings()
	window := NewRttWindow(
		NewNoopLogger(),
		settings.RttWindowSize,
		settings.RttWindowTimeout,
		settings.RttScale,
		settings.MinResendInterval,
		settings.RttMinResendInterval,
		settings.MaxResendInterval,
	)
	now := time.Now()
	for index, sample := range samples {
		at := now.Add(time.Duration(index) * time.Millisecond)
		window.closeSendTime(uint64(at.Add(-sample).UnixMilli()), at)
	}
	return window
}

// A lane whose samples are spread covers its excursion; a lane whose
// samples are tight recovers a real loss sooner than the scaled mean does.
func TestDeviationTimerCoversAnExcursionAndTightensOnAStableLane(t *testing.T) {
	// a jittery relay: a mean near 400 ms with samples from 150 to 900 ms
	jittery := []time.Duration{}
	for range 8 {
		jittery = append(jittery,
			150*time.Millisecond, 900*time.Millisecond,
			250*time.Millisecond, 700*time.Millisecond)
	}
	spread := deviationWindow(t, jittery...)
	spreadMean := spread.ScaledRtt()
	spreadDeviation := spread.DeviationRtt()
	t.Logf("jittery lane: scaled mean %s, mean plus four deviations %s", spreadMean, spreadDeviation)
	if spreadDeviation <= spreadMean {
		t.Errorf(
			"on a lane whose samples run 150 to 900 ms the deviation timer reads %s, at or under "+
				"the scaled mean %s: it exists to cover the spread",
			spreadDeviation, spreadMean,
		)
	}

	// a stable lane: every sample 400 ms
	stableSamples := []time.Duration{}
	for range 32 {
		stableSamples = append(stableSamples, 400*time.Millisecond)
	}
	stable := deviationWindow(t, stableSamples...)
	stableMean := stable.ScaledRtt()
	stableDeviation := stable.DeviationRtt()
	t.Logf("stable lane:  scaled mean %s, mean plus four deviations %s", stableMean, stableDeviation)
	if stableMean <= stableDeviation {
		t.Errorf(
			"on a lane every one of whose samples is 400 ms the deviation timer reads %s, at or "+
				"over the scaled mean %s: a tight lane must recover a lost tail sooner, not later",
			stableDeviation, stableMean,
		)
	}
}

// An empty window answers with the cold floor either way, so a cold start
// is unchanged.
func TestDeviationTimerKeepsTheColdFloor(t *testing.T) {
	window := deviationWindow(t)
	settings := DefaultSendBufferSettings()
	if got := window.DeviationRtt(); got != settings.MinResendInterval {
		t.Fatalf("an unsampled window reads %s, want the cold floor %s", got, settings.MinResendInterval)
	}
	if got := window.ScaledRtt(); got != settings.MinResendInterval {
		t.Fatalf("the scaled mean of an unsampled window reads %s, want %s",
			got, settings.MinResendInterval)
	}
}

// The estimator sets the first interval and the backoff multiplies it, so
// the two compound. Whatever they compound to, no interval may exceed the
// overall maximum, which is what bounds how long a real loss goes
// unrecovered against merged.
func TestDeviationTimerAndBackoffStayUnderTheCap(t *testing.T) {
	settings := DefaultSendBufferSettings()
	sequence, _ := newSelectiveAckRecoveryTestSequence(1, time.Unix(1_700_000_000, 0))
	sequence.client = &Client{}
	sequence.sendBufferSettings.ReliableTimerUsesDeviation = true
	sequence.sendBufferSettings.DeferTimeoutResendBackoff = true
	sequence.flightController = newSendFlightController(sequence.sendBufferSettings)
	// the widest spread this lane could show, so the deviation term is large
	now := time.Now()
	for index := range 64 {
		sample := 50 * time.Millisecond
		if index%2 == 0 {
			sample = 3 * time.Second
		}
		at := now.Add(time.Duration(index) * time.Millisecond)
		sequence.rttWindow.closeSendTime(uint64(at.Add(-sample).UnixMilli()), at)
	}
	t.Logf("widest lane: scaled mean %s, mean plus four deviations %s",
		sequence.rttWindow.ScaledRtt(), sequence.rttWindow.DeviationRtt())

	// every interval the two mechanisms can produce together, across the
	// whole range of sends and deferrals
	for sendCount := 1; sendCount <= 8; sendCount += 1 {
		for deferCount := 0; deferCount <= settings.TimeoutResendDeferLimit; deferCount += 1 {
			item := &sendItem{
				reliableCarrierObserved: true,
				sendCount:               sendCount,
				timeoutDeferCount:       deferCount,
			}
			interval := sequence.deferredResendInterval(item, sequence.rttWindow.ScaledRtt())
			if settings.MaxResendInterval < interval {
				t.Fatalf(
					"a send count of %d with %d deferrals waits %s, past the overall maximum %s: "+
						"the estimator and the backoff must not compound past the cap",
					sendCount, deferCount, interval, settings.MaxResendInterval,
				)
			}
			if ordinary := sequence.resendIntervalForItem(item, sendCount); settings.MaxResendInterval < ordinary {
				t.Fatalf("an ordinary interval at send count %d is %s, past %s",
					sendCount, ordinary, settings.MaxResendInterval)
			}
		}
	}
}

// The flag is off: this is a candidate.
func TestDeviationTimerIsOffByDefault(t *testing.T) {
	if DefaultSendBufferSettings().ReliableTimerUsesDeviation {
		t.Fatal("the deviation timer is on by default, but it has not been read in a campaign")
	}
}

// FLIGHTGATEFIX §25.1's stall, in process: a relay with a 200 ms lane that
// holds everything for 2.75 s mid-transfer, which is the excursion the gap
// export measures. The deviation timer does not help here, and the reason
// is structural: a stall produces no acknowledgement, so no sample, so the
// deviation term never learns about it. What it has sampled is the tight
// pre-stall lane, against which four deviations read shorter than the
// scaled mean, so the timer fires sooner and more often during the stall.
func TestDeviationTimerDoesNotCoverAnUnsampledStall(t *testing.T) {
	if testing.Short() {
		t.Skip("relay stall")
	}
	measure := func(deviation bool) ClientSendRecoveryStatsSnapshot {
		harness := newMixedLaneHarnessWithOptions(t, mixedLaneOptions{
			slowLatency:                200 * time.Millisecond,
			slowSerialization:          time.Millisecond,
			slowQueueFrames:            1024,
			directLaneDisabled:         true,
			deferTimeoutResend:         true,
			slowStallAfter:             1500 * time.Millisecond,
			slowStallFor:               2750 * time.Millisecond,
			reliableTimerUsesDeviation: deviation,
		})
		return harness.run(t, 3000)
	}
	mean := measure(false)
	deviation := measure(true)
	t.Logf("scaled mean:        rto=%d deferred=%d of which against a live cumulative ack=%d",
		mean.TimeoutResendWriteCount, mean.TimeoutResendDeferCount,
		mean.TimeoutResendWithRecentCumulativeProgress)
	t.Logf("mean plus four dev: rto=%d deferred=%d of which against a live cumulative ack=%d",
		deviation.TimeoutResendWriteCount, deviation.TimeoutResendDeferCount,
		deviation.TimeoutResendWithRecentCumulativeProgress)

	if mean.TimeoutResendWriteCount < 50 {
		t.Fatalf("only %d whole-window timeouts through the stall: this no longer reproduces the "+
			"excursion the gap export measures", mean.TimeoutResendWriteCount)
	}
	// the finding: it does not reduce them, because the stall is unsampled
	if deviation.TimeoutResendWriteCount < mean.TimeoutResendWriteCount/2 {
		t.Fatalf(
			"the deviation timer cut whole-window writes through an unsampled stall from %d to %d; "+
				"if that is now true the finding in §25.2 has changed and the flag should be re-read",
			mean.TimeoutResendWriteCount, deviation.TimeoutResendWriteCount,
		)
	}
}
