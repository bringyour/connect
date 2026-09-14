// Exercises busy-flow probe decisions and the window gates that admit them.
// Virtual clocks and explicit waiter barriers keep scheduling out of the proof.
package connect

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// The busy-flow liveness probe (concept ported from upstream main e05ecee)
// interposes between the send-stall bar tripping and the conviction that used
// to follow it immediately. These tests drive the interposition from the
// window's conviction pass using the stall suite's bare fixtures. Scheduler
// controls inspect the probe's own verdict: window admission can correctly
// hold that verdict if corroborating receive evidence ages out during a pause.

// busyProbeTestChannel is a stalled-capable bare channel with a context and a
// probe seam: `send` stands in for what the exit does about the control ping.
func busyProbeTestChannel(t *testing.T, send func(timeout time.Duration, ackCallback func(error)) (bool, error)) *multiClientChannel {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	client := stallTestChannel()
	client.ctx, client.cancel = context.WithCancel(ctx)
	client.busyProbeSendFunc = send
	return client
}

// busyProbeTestWindow wraps channels in a bare window with the probe enabled
// and an explicit budget, so the tests do not have to wait out the derived
// max(1s, bar/2).
func busyProbeTestWindow(budget time.Duration, clients ...*multiClientChannel) *multiClientWindow {
	window := watchdogTestWindow(clients...)
	window.reliabilitySettingsFunc = func() *ReliabilitySettings {
		return &ReliabilitySettings{BusyProbe: true, BusyProbeBudget: budget}
	}
	return window
}

// stallPast puts the channel past the stall bar with an outstanding, unacked
// send -- the exact state convictSendStalls acts on.
func stallPast(client *multiClientChannel, stallTimeout time.Duration) {
	client.addSend(1440, udpTestPath(4))
	time.Sleep(stallTimeout + 30*time.Millisecond)
}

// The acquittal. An exit whose flow acks have stopped but which answers a
// control ping is congested, not dead: the probe's positive answer must call
// the conviction off entirely, and it must refresh the stall bar so the next
// watchdog pass does not simply re-convict a millisecond later.
func TestBusyProbeAcquitsOnAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stallTimeout := 20 * time.Millisecond

		client := busyProbeTestChannel(t, func(timeout time.Duration, ackCallback func(error)) (bool, error) {
			go ackCallback(nil)
			return true, nil
		})
		stallPast(client, stallTimeout)

		// the bar has genuinely tripped before the probe runs
		AssertEqual(t, client.sendStalled(stallTimeout), true)

		client.stateLock.Lock()
		pendingBefore := client.pendingSendTime
		client.stateLock.Unlock()

		window := busyProbeTestWindow(500*time.Millisecond, client)

		AssertEqual(t, window.convictSendStalls(stallTimeout), false)
		AssertEqual(t, client.IsDone(), false)

		client.stateLock.Lock()
		endErr := client.endErr
		probeAckTime := client.busyProbeAckTime
		pendingAfter := client.pendingSendTime
		outstanding := client.busyProbeOutstanding
		client.stateLock.Unlock()

		AssertEqual(t, endErr == nil, true)
		// the liveness is recorded on its own field...
		AssertEqual(t, probeAckTime.IsZero(), false)
		// ...and pendingSendTime is NOT forged into an ack that never happened:
		// the outstanding run's true start survives the acquittal
		AssertEqual(t, pendingAfter.Equal(pendingBefore), true)
		// the outstanding send is still outstanding -- nothing about the stalled
		// data changed, only what we now know about the exit
		client.stateLock.Lock()
		AssertEqual(t, 0 < client.packetStats.sendNackCount, true)
		client.stateLock.Unlock()
		// the suspect demerit does not outlive the probe
		AssertEqual(t, outstanding, false)

		// and the bar really is refreshed: the same evidence must not re-convict
		// on the very next pass
		AssertEqual(t, client.sendStalled(stallTimeout), false)
		AssertEqual(t, window.convictSendStalls(stallTimeout), false)
	})
}

// The conviction. An exit that does not answer inside the budget is judged
// exactly as it was before the probe existed -- errored with the "send stalled"
// reason and cancelled -- with the reason extended so a field capture can see
// the probe was asked and did not answer. The "Blackhole " prefix must stay
// off it: this is hard evidence and the storm breaker must not budget it.
func TestBusyProbeConvictsOnTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stallTimeout := 20 * time.Millisecond

		client := busyProbeTestChannel(t, func(timeout time.Duration, ackCallback func(error)) (bool, error) {
			// queued, never answered
			return true, nil
		})
		stallPast(client, stallTimeout)

		window := busyProbeTestWindow(40*time.Millisecond, client, receivingSibling())

		AssertEqual(t, window.convictSendStalls(stallTimeout), true)
		AssertEqual(t, client.IsDone(), true)

		client.stateLock.Lock()
		endErr := client.endErr
		client.stateLock.Unlock()

		AssertEqual(t, endErr != nil, true)
		AssertEqual(t, strings.HasPrefix(endErr.Error(), "send stalled"), true)
		if !strings.Contains(endErr.Error(), "liveness probe timed out after") {
			t.Errorf("the reason does not name the probe outcome: %q", endErr.Error())
		}
		AssertEqual(t, blackholeVerdictErr(endErr), false)
	})
}

// The probe write failing is weak evidence on its own: the send path being
// wedged full of the same unacked data the probe is investigating is what a
// congested exit looks like too, and a live one drains enough between polls for
// the next probe to queue. Two consecutive failures inside one stale episode
// convict; one does not.
func TestBusyProbeConvictsOnTwoUnsendable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stallTimeout := 20 * time.Millisecond

		sends := atomic.Int32{}
		client := busyProbeTestChannel(t, func(timeout time.Duration, ackCallback func(error)) (bool, error) {
			sends.Add(1)
			// (fixture note: the window below carries a receiving sibling, so
			// the sibling-corroboration gate is open and the unsendable-count
			// logic is what decides)
			// backpressure: reported unsuccessful, never queued, no ack possible
			return false, nil
		})
		stallPast(client, stallTimeout)

		window := busyProbeTestWindow(40*time.Millisecond, client, receivingSibling())

		// first failure: no verdict, the episode continues
		AssertEqual(t, window.convictSendStalls(stallTimeout), false)
		AssertEqual(t, client.IsDone(), false)
		client.stateLock.Lock()
		AssertEqual(t, client.endErr == nil, true)
		AssertEqual(t, client.busyProbeSendFailures, 1)
		client.stateLock.Unlock()

		// second failure in the same episode: convicted, and the reason says why
		AssertEqual(t, window.convictSendStalls(stallTimeout), true)
		AssertEqual(t, client.IsDone(), true)
		AssertEqual(t, sends.Load(), int32(2))

		client.stateLock.Lock()
		endErr := client.endErr
		client.stateLock.Unlock()
		AssertEqual(t, strings.HasPrefix(endErr.Error(), "send stalled"), true)
		if !strings.Contains(endErr.Error(), "liveness probe unsendable") {
			t.Errorf("the reason does not name the probe outcome: %q", endErr.Error())
		}
		AssertEqual(t, blackholeVerdictErr(endErr), false)
	})
}

// "Consecutive" is scoped to one stale episode. A transient queue-full result
// that survived a healthy interval would make the next episode convict on its
// first unsendable probe -- an exit removed for one bad moment minutes ago.
func TestBusyProbeUnsendableRunResetsBetweenEpisodes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stallTimeout := 20 * time.Millisecond

		client := busyProbeTestChannel(t, func(timeout time.Duration, ackCallback func(error)) (bool, error) {
			return false, nil
		})
		stallPast(client, stallTimeout)

		window := busyProbeTestWindow(40*time.Millisecond, client)
		AssertEqual(t, window.convictSendStalls(stallTimeout), false)

		// the exit delivers: the episode is over
		client.addSendAck(1440)
		AssertEqual(t, client.sendStalled(stallTimeout), false)

		client.stateLock.Lock()
		AssertEqual(t, client.busyProbeSendFailures, 0)
		client.stateLock.Unlock()
	})
}

// The transport gate holds BEFORE any probe. A channel whose carrier is down
// holds outstanding sends because nothing can leave the device, so it is not
// stalled, must not be asked, and must not be convicted. sendStalled owns that
// ordering and the probe rides behind it.
func TestBusyProbeTransportDownHoldsBeforeAnyProbe(t *testing.T) {
	stallTimeout := 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	probes := atomic.Int32{}
	client := busyProbeTestChannel(t, func(timeout time.Duration, ackCallback func(error)) (bool, error) {
		probes.Add(1)
		return true, nil
	})
	// a real client for its route manager; nothing registers a transport, so
	// the channel's carrier is down
	client.client = NewClientWithDefaults(ctx, NewId(), NewNoContractClientOob())
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := client.client.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close probe test client: %v", err)
		}
	})

	stallPast(client, stallTimeout)

	window := busyProbeTestWindow(40*time.Millisecond, client)

	AssertEqual(t, window.convictSendStalls(stallTimeout), false)
	AssertEqual(t, client.IsDone(), false)
	if probes.Load() != 0 {
		t.Error("a channel with no active transport was probed: the gate must hold before the question is asked")
	}
}

// With the probe off -- the zero value, and every fixture built before this
// port -- the pass is the one that shipped: no question, immediate conviction,
// today's exact reason string.
func TestBusyProbeDisabledConvictsImmediately(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stallTimeout := 20 * time.Millisecond

		probes := atomic.Int32{}
		client := busyProbeTestChannel(t, func(timeout time.Duration, ackCallback func(error)) (bool, error) {
			probes.Add(1)
			go ackCallback(nil)
			return true, nil
		})
		stallPast(client, stallTimeout)

		// watchdogTestWindow carries zero-value settings: BusyProbe off
		window := watchdogTestWindow(client, receivingSibling())

		AssertEqual(t, window.convictSendStalls(stallTimeout), true)
		AssertEqual(t, client.IsDone(), true)
		AssertEqual(t, probes.Load(), int32(0))

		client.stateLock.Lock()
		endErr := client.endErr
		client.stateLock.Unlock()
		AssertEqual(t, endErr.Error(), "send stalled: no ack progress for 20ms")
	})
}

// A channel with no probe plumbing under it (a bare fixture, a channel whose
// client is gone) must fall back to the pre-probe verdict rather than acquit on
// the absence of a mechanism.
func TestBusyProbeUnavailableConvictsAsBefore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stallTimeout := 20 * time.Millisecond

		// no busyProbeSendFunc and no client: sendBusyProbe reports unavailable
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		client := stallTestChannel()
		client.ctx, client.cancel = context.WithCancel(ctx)
		stallPast(client, stallTimeout)

		window := busyProbeTestWindow(40*time.Millisecond, client, receivingSibling())

		AssertEqual(t, window.convictSendStalls(stallTimeout), true)
		client.stateLock.Lock()
		endErr := client.endErr
		failures := client.busyProbeSendFailures
		client.stateLock.Unlock()
		AssertEqual(t, endErr.Error(), "send stalled: no ack progress for 20ms")
		// absence of the mechanism is not evidence, so it must not be booked as an
		// unsendable probe
		AssertEqual(t, failures, 0)
	})
}

// Parks the probe after its timer is armed but before it can observe expiry.
// Only the test owns resume; cancellation still belongs to the wrapped context.
type busyProbeTestPauseContext struct {
	context.Context
	resume <-chan struct{}
}

// Models a descheduled waiter without depending on real timer lateness.
func (self *busyProbeTestPauseContext) Done() <-chan struct{} {
	<-self.resume
	return self.Context.Done()
}

// Starts one probe inside the caller's synctest bubble, lets its budget expire
// while the waiter is parked, then resumes it before any ack can arrive.
func busyProbeTestPausedProbe(t *testing.T, budget time.Duration, tolerance time.Duration) (*multiClientChannel, <-chan busyProbeVerdict, func(error)) {
	t.Helper()
	var ackCallback func(error)
	client := busyProbeTestChannel(t, func(_ time.Duration, ack func(error)) (bool, error) {
		ackCallback = ack
		return true, nil
	})
	client.settings.SchedulerPauseTolerance = tolerance
	resume := make(chan struct{})
	client.ctx = &busyProbeTestPauseContext{Context: client.ctx, resume: resume}
	verdicts := make(chan busyProbeVerdict, 1)
	go func() {
		verdicts <- client.busyLivenessProbe(budget)
	}()
	synctest.Wait()
	if ackCallback == nil || !client.busyProbeOutstandingNow() {
		t.Fatal("probe did not arm before the simulated scheduler pause")
	}

	time.Sleep(budget + budget/2)
	close(resume)
	synctest.Wait()
	return client, verdicts, ackCallback
}

// A probe armed before a scheduler pause gets one fresh budget. The ack is
// delivered explicitly inside that budget, after the expired timer was handled.
func TestBusyProbeSchedulerPauseRefreshesBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		budget := 50 * time.Millisecond
		client, verdicts, ackCallback := busyProbeTestPausedProbe(t, budget, 10*time.Millisecond)
		select {
		case verdict := <-verdicts:
			t.Fatalf("suspended probe returned before its refreshed budget: %+v", verdict)
		default:
		}
		if !client.busyProbeOutstandingNow() {
			t.Fatal("refresh disarmed the unanswered probe")
		}

		time.Sleep(budget / 2)
		ackTime := time.Now()
		ackCallback(nil)
		synctest.Wait()
		select {
		case verdict := <-verdicts:
			if verdict.convict || verdict.detail != "liveness probe answered" {
				t.Fatalf("ack inside the refreshed budget did not acquit: %+v", verdict)
			}
		default:
			t.Fatal("probe did not finish after its ack")
		}
		AssertEqual(t, client.IsDone(), false)
		AssertEqual(t, client.busyProbeOutstandingNow(), false)
		client.stateLock.Lock()
		probeAckTime := client.busyProbeAckTime
		client.stateLock.Unlock()
		AssertEqual(t, probeAckTime.Equal(ackTime), true)
	})
}

// With the detector off, the same paused probe returns a conviction at its
// first expiry. Window admission is tested separately: a sibling stamped once
// at construction may correctly age out while this waiter is descheduled.
func TestBusyProbeSchedulerPauseOffConvictsAtTheBudget(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		budget := 50 * time.Millisecond
		client, verdicts, ackCallback := busyProbeTestPausedProbe(t, budget, 0)
		select {
		case verdict := <-verdicts:
			if !verdict.convict || verdict.detail != "liveness probe timed out after 50ms" {
				t.Fatalf("disabled pause detector did not convict at the first expiry: %+v", verdict)
			}
		default:
			t.Fatal("disabled pause detector incorrectly refreshed the expired budget")
		}

		// Late or duplicate callbacks must neither block nor change the
		// completed probe's decision, and no callback goroutine outlives us.
		ackCallback(nil)
		ackCallback(nil)
		AssertEqual(t, client.busyProbeOutstandingNow(), false)
		client.stateLock.Lock()
		probeAckTime := client.busyProbeAckTime
		client.stateLock.Unlock()
		AssertEqual(t, probeAckTime.IsZero(), true)
	})
}

// A construction-to-dispatch delay can age a one-shot sibling proof out during
// the probe. Hold without refreshing the stall; a fresh receive admits the next
// pass. This forces the full-suite failure's ordering without predecessor load.
func TestBusyProbeHoldsWhenSiblingProofExpiresDuringProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stallTimeout := 20 * time.Millisecond
		budget := 50 * time.Millisecond
		client := busyProbeTestChannel(t, func(_ time.Duration, _ func(error)) (bool, error) {
			return true, nil
		})
		stallPast(client, stallTimeout)
		sibling := receivingSibling()
		window := busyProbeTestWindow(budget, client, sibling)
		client.stateLock.Lock()
		pendingBefore := client.pendingSendTime
		client.stateLock.Unlock()

		// Construction-to-dispatch delay plus the probe budget now exceeds
		// the sibling proof's stallTimeout+budget lifetime by one nanosecond.
		time.Sleep(stallTimeout + time.Nanosecond)
		AssertEqual(t, sibling.hasRecentReceive(stallTimeout+budget), true)
		AssertEqual(t, window.convictSendStalls(stallTimeout), false)
		AssertEqual(t, sibling.hasRecentReceive(stallTimeout+budget), false)
		AssertEqual(t, client.IsDone(), false)
		AssertEqual(t, client.sendStalled(stallTimeout), true)
		client.stateLock.Lock()
		pendingHeld := client.pendingSendTime
		endErr := client.endErr
		client.stateLock.Unlock()
		AssertEqual(t, endErr == nil, true)
		AssertEqual(t, pendingHeld.Equal(pendingBefore), true)

		sibling.stateLock.Lock()
		sibling.lastReceiveAckTime = time.Now()
		sibling.stateLock.Unlock()
		AssertEqual(t, window.convictSendStalls(stallTimeout), true)
		AssertEqual(t, client.IsDone(), true)
		client.stateLock.Lock()
		pendingAfter := client.pendingSendTime
		client.stateLock.Unlock()
		AssertEqual(t, pendingAfter.Equal(pendingBefore), true)
	})
}

// An ack callback that reports an error is a failed question, not an answer:
// the send sequence gave up carrying the ping. It convicts, named distinctly
// from a plain timeout.
func TestBusyProbeAckErrorConvicts(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stallTimeout := 20 * time.Millisecond

		client := busyProbeTestChannel(t, func(timeout time.Duration, ackCallback func(error)) (bool, error) {
			go ackCallback(errors.New("sequence closed"))
			return true, nil
		})
		stallPast(client, stallTimeout)

		window := busyProbeTestWindow(500*time.Millisecond, client, receivingSibling())

		AssertEqual(t, window.convictSendStalls(stallTimeout), true)
		client.stateLock.Lock()
		endErr := client.endErr
		client.stateLock.Unlock()
		if !strings.Contains(endErr.Error(), "liveness probe failed") {
			t.Errorf("the reason does not name the probe outcome: %q", endErr.Error())
		}
		AssertEqual(t, blackholeVerdictErr(endErr), false)
	})
}

// The probe budget derivation: 0 means max(1s, bar/2), so the shipped 3s bar
// gets 1.5s and total detection stays inside ~4.5s, while a tiny bar cannot ask
// a question it does not wait for an answer to.
func TestBusyProbeBudgetDerivation(t *testing.T) {
	AssertEqual(t, busyProbeBudget(3*time.Second, 0), 1500*time.Millisecond)
	AssertEqual(t, busyProbeBudget(10*time.Second, 0), 5*time.Second)
	// the floor
	AssertEqual(t, busyProbeBudget(1*time.Second, 0), 1*time.Second)
	AssertEqual(t, busyProbeBudget(100*time.Millisecond, 0), 1*time.Second)
	// an explicit budget wins outright
	AssertEqual(t, busyProbeBudget(3*time.Second, 250*time.Millisecond), 250*time.Millisecond)
}

// The probe write must fail fast: blocking for the idle CPingWriteTimeout (15s)
// while the send queue is wedged would push detection out past every path the
// probe exists to beat.
func TestBusyProbeWriteTimeoutIsSnappy(t *testing.T) {
	// a quarter of the budget, well inside the idle write timeout
	AssertEqual(t, busyProbeWriteTimeout(1500*time.Millisecond, 15*time.Second), 375*time.Millisecond)
	// floored
	AssertEqual(t, busyProbeWriteTimeout(400*time.Millisecond, 15*time.Second), 250*time.Millisecond)
	// never longer than the configured idle write timeout
	AssertEqual(t, busyProbeWriteTimeout(40*time.Second, 5*time.Second), 5*time.Second)
	// an unset idle write timeout does not clamp to zero
	AssertEqual(t, busyProbeWriteTimeout(1500*time.Millisecond, 0), 375*time.Millisecond)
}

// The interposition must be confined to the send-stall path. The no-send-ack
// verdict, the transport-down hold and the cping loop keep their exact
// behavior; a probe that leaked into any of them would delay a hard verdict or
// resurrect the cping conviction that once executed every fixture client.
func TestBusyProbeInterposesOnlyOnTheSendStallPath(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	// exactly two mentions of the probe entry point: its definition and the
	// single call site in the conviction pass
	if got := strings.Count(source, "busyLivenessProbe("); got != 2 {
		t.Errorf("busyLivenessProbe( appears %d times, want 2 (the definition and convictSendStalls): the probe has leaked onto another path", got)
	}

	body, ok := functionBody(source, "func (self *multiClientWindow) convictSendStalls(")
	if !ok {
		t.Fatal("could not find convictSendStalls")
	}
	if !strings.Contains(body, "client.busyLivenessProbe(") {
		t.Error("convictSendStalls does not run the probe, so BusyProbe is inert")
	}
	if !strings.Contains(body, "reliabilitySettings.BusyProbe") {
		t.Error("convictSendStalls does not gate on BusyProbe: the kill switch has no effect")
	}
	if strings.Contains(body, `"Blackhole `) {
		t.Error("the stall reason must never carry the budgeted verdict prefix")
	}

	for _, name := range []string{
		"func (self *multiClientChannel) detectBlackhole()",
		"func (self *multiClientChannel) ping(",
		"func blackholeReasonFromStats(",
	} {
		body, ok := functionBody(source, name)
		if !ok {
			t.Fatalf("could not find %s", name)
		}
		if strings.Contains(body, "busyLivenessProbe") {
			t.Errorf("%s runs the busy probe: the probe must interpose on the send-stall path only", name)
		}
	}
}

// The probe must ride the existing control-ping plumbing rather than invent a
// second one -- the seam above exists for the tests, not for production.
func TestBusyProbeUsesTheControlPingPlumbing(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *multiClientChannel) sendBusyProbe(")
	if !ok {
		t.Fatal("could not find sendBusyProbe")
	}
	if !strings.Contains(body, "self.SendDetailedMessage(&protocol.IpPing{}") {
		t.Error("the probe does not use the SendDetailedMessage(&protocol.IpPing{}) plumbing the cping loop uses")
	}
	if !strings.Contains(body, "errBusyProbeUnavailable") {
		t.Error("a channel with no plumbing under it does not report unavailable, so absence of the mechanism could be read as evidence")
	}
}

// sendStalled must measure its bar from the LATER of the outstanding-run start
// and the last probe ack, so an acquitted exit gets a full fresh bar without
// anything forging a send ack.
func TestBusyProbeAckRefreshesTheStallBarOnly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stallTimeout := 20 * time.Millisecond
		client := stallTestChannel()

		client.addSend(1440, udpTestPath(4))
		time.Sleep(stallTimeout + 30*time.Millisecond)
		AssertEqual(t, client.sendStalled(stallTimeout), true)

		client.addBusyProbeAck()

		// refreshed
		AssertEqual(t, client.sendStalled(stallTimeout), false)
		// the send is still outstanding and its clock still records when it began
		client.stateLock.Lock()
		AssertEqual(t, client.packetStats.sendNackCount, 1)
		AssertEqual(t, client.pendingSendTime.IsZero(), false)
		AssertEqual(t, stallTimeout <= time.Since(client.pendingSendTime), true)
		client.stateLock.Unlock()

		// and only for one bar: a still-dead exit convicts on the next round
		time.Sleep(stallTimeout + 30*time.Millisecond)
		AssertEqual(t, client.sendStalled(stallTimeout), true)
	})
}

// The shipped defaults, and the override round trip. A knob dropped by
// ReliabilitySettingsFrom would be silently turned off by every developer-menu
// write.
func TestBusyProbeDefaults(t *testing.T) {
	settings := DefaultMultiClientSettings()

	AssertEqual(t, settings.BusyProbe, true)
	// 0 derives max(1s, SendStallTimeout/2)
	AssertEqual(t, settings.BusyProbeBudget, time.Duration(0))
	AssertEqual(t, busyProbeBudget(settings.SendStallTimeout, settings.BusyProbeBudget), 1500*time.Millisecond)

	reliabilitySettings := ReliabilitySettingsFrom(settings)
	AssertEqual(t, reliabilitySettings.BusyProbe, settings.BusyProbe)
	AssertEqual(t, reliabilitySettings.BusyProbeBudget, settings.BusyProbeBudget)

	// the zero value is the pre-change behavior
	AssertEqual(t, ReliabilitySettingsFrom(nil).BusyProbe, false)
}

// --- scheduler pause exculpation ---

// The whole detection rule: a timer armed for `expected` that took `elapsed` to
// come back was not late, it was not running.
func TestSchedulerPauseDetectedRule(t *testing.T) {
	// ordinary jitter, well inside the tolerance
	AssertEqual(t, schedulerPauseDetected(1200*time.Millisecond, time.Second, 2*time.Second), false)
	// exactly at the bound is not past it
	AssertEqual(t, schedulerPauseDetected(3*time.Second, time.Second, 2*time.Second), false)
	// a doze
	AssertEqual(t, schedulerPauseDetected(30*time.Second, time.Second, 2*time.Second), true)

	// zero tolerance is the detector off, which is the pre-change behavior
	AssertEqual(t, schedulerPauseDetected(30*time.Second, time.Second, 0), false)
	// a nonsensical arming window detects nothing rather than everything
	AssertEqual(t, schedulerPauseDetected(30*time.Second, 0, 2*time.Second), false)
}

// schedulerPauseTestParent is a bare parent with no ingress baseline, so the
// ingress-staleness half of the gate is quiet and only the pause hold can make
// it report stale.
func schedulerPauseTestParent() *RemoteUserNatMultiClient {
	return &RemoteUserNatMultiClient{
		settings:      DefaultMultiClientSettings(),
		clientUpdates: map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
	}
}

// A detected suspend feeds the SAME hold path a network migration does: the
// receive verdicts are held for the recovery window and the verdict clocks
// rebase to the resume instant, so evidence accumulated before the host stopped
// cannot convict on the first pass after wake.
func TestSchedulerPauseHoldsVerdictsAndRebases(t *testing.T) {
	mc := schedulerPauseTestParent()

	// nothing detected yet: the gate is quiet
	if stale, freshSince := mc.uplinkGate(time.Now()); stale || !freshSince.IsZero() {
		t.Fatalf("a quiet client reported stale=%v freshSince=%v", stale, freshSince)
	}

	before := time.Now()
	mc.notifySchedulerPause(30 * time.Second)

	stale, freshSince := mc.uplinkGate(time.Now())
	if !stale {
		t.Error("the receive verdicts are not held after a detected suspend")
	}
	if freshSince.Before(before) {
		t.Errorf("the verdict clocks were not rebased to the resume instant: freshSince = %v", freshSince)
	}

	// past the recovery window the hold lifts on its own -- a suspend is a
	// bounded excuse, not a permanent one
	recovered := time.Now().Add(DefaultMultiClientSettings().SchedulerPauseRecoveryTimeout + time.Second)
	if stale, _ := mc.uplinkGate(recovered); stale {
		t.Error("the pause hold outlived its recovery window")
	}
}

// A zero recovery timeout rebases the clocks without holding anything: the
// suspend is still not counted as silence, but no verdict is deferred.
func TestSchedulerPauseZeroRecoveryRebasesWithoutHolding(t *testing.T) {
	mc := schedulerPauseTestParent()
	mc.settings.SchedulerPauseRecoveryTimeout = 0

	before := time.Now()
	mc.notifySchedulerPause(30 * time.Second)

	stale, freshSince := mc.uplinkGate(time.Now())
	if stale {
		t.Error("a zero recovery timeout still held verdicts")
	}
	if freshSince.Before(before) {
		t.Errorf("the verdict clocks were not rebased: freshSince = %v", freshSince)
	}
}

// A suspend detected mid-migration must close the open uplink-stale epoch too,
// so the hold-cap window restarts rather than being inherited from an epoch
// that is about to expire.
func TestSchedulerPauseClosesTheUplinkStaleEpoch(t *testing.T) {
	mc := uplinkGateTestParent(2)
	now := time.Now()

	mc.uplinkLastIngressNanos.Store(now.Add(-6 * time.Second).UnixNano())
	if stale, _ := mc.uplinkGate(now); !stale {
		t.Fatal("tunnel-wide silence past the gate did not read as stale")
	}

	mc.notifySchedulerPause(30 * time.Second)

	mc.uplinkStateLock.Lock()
	staleSince := mc.uplinkStaleSince
	freshSince := mc.uplinkFreshSince
	mc.uplinkStateLock.Unlock()

	if !staleSince.IsZero() {
		t.Errorf("the stale epoch survived the pause: staleSince = %v", staleSince)
	}
	if freshSince.Before(now) {
		t.Errorf("the rebase point was not advanced: freshSince = %v", freshSince)
	}
}

// The detector must measure real elapsed time, so it cannot use the wakeup
// scheduler: WakeupAfter deliberately coalesces timers to save radio wakeups,
// and a coalesced fire is exactly the "late" this loop would misread as a
// suspend. It must also be gated on the constructed tolerance, so a client
// built with the detector off never runs the goroutine.
func TestSchedulerPauseDetectorSourceAnchors(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	body, ok := functionBody(source, "func (self *RemoteUserNatMultiClient) runSchedulerPauseDetector(")
	if !ok {
		t.Fatal("could not find runSchedulerPauseDetector")
	}
	if !strings.Contains(body, "time.After(schedulerPauseProbeInterval)") {
		t.Error("the detector does not arm a plain timer, so it cannot measure the pause")
	}
	if strings.Contains(body, "WakeupAfter") {
		t.Error("the detector uses the coalescing wakeup scheduler: a batched wakeup would read as a suspend")
	}
	if !strings.Contains(body, "schedulerPauseDetected(") || !strings.Contains(body, "self.notifySchedulerPause(") {
		t.Error("the detector does not route through the detection rule and the hold")
	}

	body, ok = functionBody(source, "func (self *RemoteUserNatMultiClient) notifySchedulerPause(")
	if !ok {
		t.Fatal("could not find notifySchedulerPause")
	}
	if !strings.Contains(body, "self.uplinkFreshSince = now") {
		t.Error("the pause does not rebase the verdict clocks, so held verdicts would all mature on wake")
	}
	if strings.Contains(body, "NetworkChanged()") {
		t.Error("a resume must not kick every transport into a re-dial: that is the churn this layer exists to avoid")
	}

	body, ok = functionBody(source, "func NewRemoteUserNatMultiClient(")
	if !ok {
		t.Fatal("could not find NewRemoteUserNatMultiClient")
	}
	if !strings.Contains(body, "0 < settings.SchedulerPauseTolerance") {
		t.Error("the detector goroutine is not gated on the constructed tolerance")
	}
}

// The shipped defaults and the override round trip.
func TestSchedulerPauseDefaults(t *testing.T) {
	settings := DefaultMultiClientSettings()

	if settings.SchedulerPauseTolerance != 2*time.Second {
		t.Errorf("SchedulerPauseTolerance = %v, want 2s", settings.SchedulerPauseTolerance)
	}
	if settings.SchedulerPauseRecoveryTimeout != 5*time.Second {
		t.Errorf("SchedulerPauseRecoveryTimeout = %v, want 5s", settings.SchedulerPauseRecoveryTimeout)
	}
	// the arming window has to be shorter than the tolerance, or ordinary
	// jitter across a long arm would read as a suspend
	if schedulerPauseProbeInterval >= settings.SchedulerPauseTolerance {
		t.Errorf("the arming window (%v) is not shorter than the tolerance (%v)", schedulerPauseProbeInterval, settings.SchedulerPauseTolerance)
	}

	reliabilitySettings := ReliabilitySettingsFrom(settings)
	AssertEqual(t, reliabilitySettings.SchedulerPauseTolerance, settings.SchedulerPauseTolerance)
	AssertEqual(t, reliabilitySettings.SchedulerPauseRecoveryTimeout, settings.SchedulerPauseRecoveryTimeout)

	AssertEqual(t, ReliabilitySettingsFrom(nil).SchedulerPauseTolerance, time.Duration(0))
}
