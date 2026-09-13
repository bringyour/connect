//go:build flightgate_next

package connect

// FLIGHTGATEFIX §20.3. These measure the mixed-lane behaviour the removed
// mechanisms were built for. They are the specification the affinity
// candidate of §20.5 is measured against, not a gate on this landing, so
// they build only under the flightgate_next tag.

import (
	"fmt"
	"runtime"
	"sort"
	"strings"
	"testing"
	"time"
)

// A clean mixed route, no loss: acknowledgements that ride the healthy
// direct lane arrive far ahead of the ones that ride the relay, and the
// sender's selective-gap scoreboard must not read that ack-lane spread as
// loss. The relay-only arm is the merged behaviour; the affine arm must not
// be worse than it (FLIGHTGATEFIX §14).
func TestMixedLaneAckAffinityDoesNotRaiseGapResends(t *testing.T) {
	if testing.Short() {
		t.Skip("mixed-lane gap reproduction")
	}
	const (
		fastDelay    = 2 * time.Millisecond
		slowDelay    = 40 * time.Millisecond
		messageCount = 600
	)
	relayOnlyHarness := newMixedLaneGapHarness(t, fastDelay, slowDelay, true)
	relayOnlyHarness.startReverseLoad(time.Millisecond)
	relayOnly := relayOnlyHarness.run(t, messageCount)
	affineHarness := newMixedLaneGapHarness(t, fastDelay, slowDelay, false)
	affineHarness.startReverseLoad(time.Millisecond)
	affine := affineHarness.run(t, messageCount)
	report := func(name string, stats ClientSendRecoveryStatsSnapshot) {
		t.Logf(
			"%s: gap=%d unreliable-gap=%d reorder-suspected=%d rto=%d tail-probe=%d cumulative-probe=%d reductions=%d",
			name,
			stats.SelectiveGapWriteCount,
			stats.UnreliableFlightGapCount,
			stats.UnreliableFlightGapReorderSuspected,
			stats.TimeoutResendWriteCount,
			stats.AckTailProbeWriteCount,
			stats.CumulativeProbeWriteCount,
			stats.UnreliableFlightReductionCount,
		)
	}
	report("relay-only acks", relayOnly)
	report("affine acks    ", affine)
	if relayOnly.SelectiveGapWriteCount < affine.SelectiveGapWriteCount {
		t.Fatalf(
			"ack affinity raised selective-gap resends from %d to %d over %d messages on a lossless link",
			relayOnly.SelectiveGapWriteCount,
			affine.SelectiveGapWriteCount,
			messageCount,
		)
	}
}

// TestMixedLaneGapResendBaseline reports the sender's recovery counters for
// one arm, so the same file can be run at any commit of the program to
// attribute a rise to one item. It never fails.
func TestMixedLaneGapResendBaseline(t *testing.T) {
	if testing.Short() {
		t.Skip("mixed-lane gap baseline")
	}
	const (
		fastDelay    = 2 * time.Millisecond
		slowDelay    = 40 * time.Millisecond
		messageCount = 600
	)
	harness := newMixedLaneGapHarness(t, fastDelay, slowDelay, false)
	harness.startReverseLoad(time.Millisecond)
	stats := harness.run(t, messageCount)
	t.Logf(
		"BASELINE gap=%d unreliable-gap=%d reorder-suspected=%d rto=%d tail-probe=%d cumulative-probe=%d reductions=%d initial=%d",
		stats.SelectiveGapWriteCount,
		stats.UnreliableFlightGapCount,
		stats.UnreliableFlightGapReorderSuspected,
		stats.TimeoutResendWriteCount,
		stats.AckTailProbeWriteCount,
		stats.CumulativeProbeWriteCount,
		stats.UnreliableFlightReductionCount,
		stats.InitialWriteCount,
	)
}

// FLIGHTGATEFIX §15. The device runs show a retransmit storm that has
// nothing to do with the flight gate: thirteen to nineteen thousand timeout
// resends in three minutes on builds where the flight never waited once,
// including runs carried entirely by the reliable peer lane. A lane with a
// bandwidth reproduces it: the sender writes a window into a route that
// drains at link rate, so an item's acknowledgement cannot come back inside
// the retransmit timer that started when the item was queued, and the whole
// window is rewritten every interval.

// FLIGHTGATEFIX §19.6 pre-flight. A lossy direct lane at the campaign's
// two loss rates must still carry the payload to completion, and the run
// records what it cost. The grace is no longer a setting to compare
// against: D3 deleted MixedLaneAckReorderGrace because the deferral is a
// sound rule rather than optional insurance, so this is a single arm.
func TestMixedLaneLossyDirectLaneCompletes(t *testing.T) {
	if testing.Short() {
		t.Skip("lossy mixed-lane goodput")
	}
	const messageCount = 400
	for _, dropFraction := range []float64{0.01, 0.03} {
		harness := newMixedLaneHarnessWithOptions(t, mixedLaneOptions{
			fastLatency:        2 * time.Millisecond,
			slowLatency:        40 * time.Millisecond,
			fastDropFraction:   dropFraction,
			deferTimeoutResend: true,
		})
		start := time.Now()
		stats := harness.run(t, messageCount)
		t.Logf("drop %.0f%%: %s (gap=%d rto=%d deferred=%d reductions=%d)",
			100*dropFraction, time.Since(start).Truncate(time.Millisecond),
			stats.SelectiveGapWriteCount, stats.TimeoutResendWriteCount,
			stats.TimeoutResendDeferCount, stats.UnreliableFlightReductionCount)
	}
}

type mixedLaneRunResult struct {
	elapsed     time.Duration
	deadWindows int
	windows     int
	stats       ClientSendRecoveryStatsSnapshot
	fastCarried uint64
	slowCarried uint64
	fastDropped uint64
	// stalled is set when the run did not finish inside the deadline;
	// delivered is how far it got. A stalled run is the collapse the
	// campaign's dead windows record, so it is a result, not a test error.
	stalled   bool
	delivered int
}

// describe is the one-line reading of a run.
func (self mixedLaneRunResult) describe(messageCount int) string {
	if self.stalled {
		return fmt.Sprintf("STALLED after %d of %d messages", self.delivered, messageCount)
	}
	return fmt.Sprintf(
		"%s, %.1f Mb/s, %d of %d windows dead",
		self.elapsed.Truncate(time.Millisecond),
		self.megabitsPerSecond(messageCount),
		self.deadWindows, self.windows,
	)
}

// megabitsPerSecond is the goodput a campaign cell would report for this run
// at the tunnel's typical message size.
func (self mixedLaneRunResult) megabitsPerSecond(messageCount int) float64 {
	if self.elapsed <= 0 {
		return 0
	}
	return float64(messageCount) * tunnelTypicalMessageByteCount * 8 /
		self.elapsed.Seconds() / 1e6
}

// runMultiFlow offers the payload from several concurrent producers, the
// shape of the harness's tcp-parallel workload, and reports the run as a
// rate over one-second windows. A window carrying under a quarter of the
// run's mean is dead, the same shape as the campaign's under-5-Mb/s rule
// against its roughly 20-Mb/s cells.
func (self *mixedLaneGapHarness) runMultiFlow(
	t testing.TB,
	flows int,
	messagesPerFlow int,
) mixedLaneRunResult {
	t.Helper()
	content := ""
	for len(content) < tunnelTypicalMessageByteCount-16 {
		content += "mixed-lane-flow-"
	}
	messageCount := flows * messagesPerFlow
	start := time.Now()
	var producers sync.WaitGroup
	for flow := range flows {
		producers.Add(1)
		go func(flow int) {
			defer producers.Done()
			for index := range messagesPerFlow {
				frame, err := ToFrame(
					&protocol.SimpleMessage{Content: fmt.Sprintf("%s%d-%d", content, flow, index)},
					DefaultProtocolVersion,
				)
				if err != nil {
					return
				}
				if !self.sender.SendWithTimeout(frame, self.receiverId, nil, 60*time.Second) {
					MessagePoolReturn(frame.MessageBytes)
					return
				}
			}
		}(flow)
	}
	producers.Wait()
	delivered := 0
	deadline := time.After(self.runDeadline)
	stall := time.NewTimer(30 * time.Second)
	defer stall.Stop()
	diagnosed := false
	stalled := false
waiting:
	for delivered < messageCount {
		select {
		case frames := <-self.received:
			delivered += frames
			if !stall.Stop() {
				<-stall.C
			}
			stall.Reset(30 * time.Second)
		case <-stall.C:
			if !diagnosed {
				diagnosed = true
				self.stallDiagnostics(t, delivered, messageCount)
			}
		case <-deadline:
			stalled = true
			break waiting
		}
	}
	elapsed := time.Since(start)
	if stalled {
		return mixedLaneRunResult{
			elapsed:   elapsed,
			stalled:   true,
			delivered: delivered,
			stats:     self.sender.SendRecoveryStats(),
		}
	}
	time.Sleep(300 * time.Millisecond)

	self.deliveryLock.Lock()
	times := append([]time.Time(nil), self.deliveryTimes...)
	self.deliveryLock.Unlock()
	windowCount := max(1, int(elapsed/time.Second))
	perWindow := make([]int, windowCount)
	for _, at := range times {
		index := int(at.Sub(start) / time.Second)
		if index < 0 {
			index = 0
		}
		if windowCount <= index {
			index = windowCount - 1
		}
		perWindow[index] += 1
	}
	mean := float64(len(times)) / float64(windowCount)
	deadWindows := 0
	for _, count := range perWindow {
		if float64(count) < mean/4 {
			deadWindows += 1
		}
	}
	return mixedLaneRunResult{
		elapsed:     elapsed,
		deadWindows: deadWindows,
		windows:     windowCount,
		stats:       self.sender.SendRecoveryStats(),
	}
}

// FLIGHTGATEFIX §19.6 pre-flight. The campaign's regime in process: a
// direct lane at 20 ms losing one or three per cent or the campaign's own
// burst chain, a relay at 200 ms, a bounded direct reply route the
// receiver's uplink contends for, and four concurrent producers. The run
// must complete, and it records goodput, dead windows and the recovery
// counters for the next campaign to compare.
func TestMixedLaneCampaignRegimeCompletes(t *testing.T) {
	if testing.Short() {
		t.Skip("campaign-regime mixed-lane goodput")
	}
	const (
		flows           = 4
		messagesPerFlow = 500
		messageCount    = flows * messagesPerFlow
	)
	for _, shape := range []struct {
		name  string
		drop  float64
		burst *laneBurstLoss
	}{
		{"1% independent loss", 0.01, nil},
		{"3% independent loss", 0.03, nil},
		{"campaign two-state burst loss", 0, &campaignBurstLoss},
	} {
		harness := newMixedLaneHarnessWithOptions(t, mixedLaneOptions{
			fastLatency:        20 * time.Millisecond,
			slowLatency:        200 * time.Millisecond,
			fastSerialization:  time.Millisecond,
			slowSerialization:  4 * time.Millisecond,
			fastDropFraction:   shape.drop,
			fastBurstLoss:      shape.burst,
			replySerialization: time.Millisecond,
			deferTimeoutResend: true,
		})
		harness.startReverseLoad(5 * time.Millisecond)
		result := harness.runMultiFlow(t, flows, messagesPerFlow)
		result.fastCarried = harness.fastCarried.Load()
		result.slowCarried = harness.slowCarried.Load()
		result.fastDropped = harness.fastDropped.Load()
		t.Logf(
			"%s: %s, gap=%d rto=%d deferred=%d reductions=%d, direct carried %d lost %d, relay %d",
			shape.name, result.describe(messageCount),
			result.stats.SelectiveGapWriteCount,
			result.stats.TimeoutResendWriteCount,
			result.stats.TimeoutResendDeferCount,
			result.stats.UnreliableFlightReductionCount,
			result.fastCarried, result.fastDropped, result.slowCarried,
		)
		if result.stalled {
			t.Errorf("%s: the run stalled after %d of %d messages",
				shape.name, result.delivered, messageCount)
		}
	}
}
