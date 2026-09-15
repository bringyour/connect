package connect

import (
	"context"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// THROUGHPUTFIX H5, the part of the zombie coupling that is decidable in
// process. The report says forty flows whose client is gone take a provider
// from 639 to 180 Mb/s and puts each zombie at about 8 Mb/s, and its own
// arithmetic does not close: forty times eight is 320 against a loss of 460.
// The aggregate is a rig measurement and stays one. What a single dead
// destination is *allowed* to put on the wire is not: it is fixed by two
// settings and nothing else.
//
// The bound turns out to derive the report's own figure: at the shipped
// 2 MiB queue and 2 s minimum interval the ceiling is 8.4 Mb/s per dead
// destination, which is the 8 Mb/s the report measured. So that figure is this
// bound saturated rather than an independent observation.
//
// Which interval a zombie actually sits at was a recorded disagreement, and
// `TestDeadDestinationResendIntervalGrows` below settled it: the backoff
// climbs to `MaxResendInterval` after two doublings, so about six seconds
// after a kill at the shipped constants. The queue over the 2 s floor gives
// 8.4 Mb/s and is what this row's short window measures; the same queue over
// the 8 s ceiling gives 2.1 Mb/s and is the steady state. The reporter's 8
// Mb/s is therefore the first few seconds after a kill, not the condition
// their 40-zombie provider was in.
//
// The contribution, with that settled: against a measured loss of about
// 460 Mb/s, forty zombies put about 84 Mb/s on the wire in steady state, so
// the remainder is about 376 Mb/s. It is a quantified gap rather than an
// unexplained one, and a larger one than either reading of the report
// suggested.
//
// A destination that never acknowledges holds at most
// `ResendQueueMaxByteCount` of unacknowledged items, and the sequence rewrites
// an item no more often than its resend interval, which starts at
// `MinResendInterval` and backs off toward `MaxResendInterval`. So over a
// window the rewrite bytes one destination can produce are at most the queue
// bound times the number of minimum intervals in the window. That ceiling is
// what turns the report's per-zombie figure into something checkable, and this
// asserts it and reports the observed rate beside it.
//
// The assertion is one-sided on purpose. The observed rate moves with
// scheduling; the ceiling does not, and a change that removed the queue's byte
// bound or the backoff would cross it.
// Two structural concessions belong beside this row. The aggregate coupling
// (H5), a provider falling from 639 to 180 Mb/s with forty zombies, is a rate
// summed over every flow of a real provider with real sockets and real peers;
// in process there is no such quantity to read, only this per-destination
// bound, and no arrangement of one client and one route produces the
// contention that the loss is made of. The threshold shape (H6), 8 flows at
// 719, 16 at 587, 40 at 180, is the same quantity swept, and deciding whether
// it is a resource being crossed needs that resource instrumented on the
// provider under load rather than a bound computed from settings. The
// 40-zombie provider cell owns both. What this row removes from them is the
// arithmetic: the per-zombie figure is no longer a measurement to be explained
// but a bound to be computed.
func TestZombieFlowEgressIsBoundedByItsResendQueueAndInterval(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	assertMessagePoolOwnership(t)

	const resendQueueMaxByteCount = ByteCount(128 * 1024)
	const minResendInterval = 20 * time.Millisecond
	const observationWindow = 500 * time.Millisecond

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.ResendQueueMaxByteCount = resendQueueMaxByteCount
	settings.SendBufferSettings.MinResendInterval = minResendInterval
	settings.SendBufferSettings.RttMinResendInterval = minResendInterval
	settings.SendBufferSettings.MaxResendInterval = 5 * minResendInterval
	// nothing may leave the queue by timing out inside the window
	settings.SendBufferSettings.AckTimeout = time.Minute
	settings.SendBufferSettings.IdleTimeout = time.Minute

	destinationId := NewId()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	route := make(chan []byte, 64)
	client.ContractManager().AddNoContractPeer(destinationId)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)

	// the destination is gone: everything written is drained off the route and
	// nothing is ever acknowledged
	drainCtx, drainCancel := context.WithCancel(ctx)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case transferFrameBytes := <-route:
				MessagePoolReturn(transferFrameBytes)
			case <-drainCtx.Done():
				return
			}
		}
	}()
	// registered after the ownership assertion, so it runs before it: the
	// resend queue returns its retained roots as the sequence exits, and a
	// reconciliation that does not join that shutdown reads them as leaked
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := client.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close the client: %v", err)
		}
		drainCancel()
		<-drained
		for {
			select {
			case transferFrameBytes := <-route:
				MessagePoolReturn(transferFrameBytes)
			default:
				return
			}
		}
	})

	// fill the resend queue: admission stops once its byte bound is reached
	content := make([]byte, 1024)
	for i := range content {
		content[i] = 'z'
	}
	admittedByteCount := ByteCount(0)
	for admittedByteCount < 4*resendQueueMaxByteCount {
		frame := RequireToFrameWithDefaultProtocolVersion(
			&protocol.SimpleMessage{Content: string(content)},
		)
		admitted, _ := client.SendWithTimeoutDetailed(
			frame,
			destinationId,
			nil,
			50*time.Millisecond,
			sendPackRecoveryOption{upstreamRecoverable: true, retainAfterAckTimeout: true},
		)
		if !admitted {
			MessagePoolReturn(frame.MessageBytes)
			break
		}
		admittedByteCount += ByteCount(len(content))
	}
	if admittedByteCount <= 0 {
		t.Fatal("no return was admitted toward the dead destination")
	}

	// let the sequence rewrite what it holds, then read what it wrote
	before := client.DestinationSendStats(destinationId)
	time.Sleep(observationWindow)
	after := client.DestinationSendStats(destinationId)

	resendWriteByteCount := ByteCount(after.ResendWriteByteCount - before.ResendWriteByteCount)
	// one queue's worth per minimum interval, plus one queue for the rewrite
	// already in flight when the window opened
	intervalCount := ByteCount(observationWindow / minResendInterval)
	ceilingByteCount := resendQueueMaxByteCount * (intervalCount + 1)
	if ceilingByteCount < resendWriteByteCount {
		t.Errorf(
			"one dead destination rewrote %d bytes in %s, above the %d byte ceiling its %d byte resend queue and %s minimum interval allow; the queue's byte bound or its backoff is no longer holding",
			resendWriteByteCount,
			observationWindow,
			ceilingByteCount,
			resendQueueMaxByteCount,
			minResendInterval,
		)
	}
	if after.SequenceCount <= 0 {
		t.Fatal("the dead destination has no live send sequence, so this window measured nothing")
	}

	// what the same bound is at the settings a provider ships with, which is
	// the number the report's arithmetic needed and did not have
	shipped := DefaultSendBufferSettings()
	shippedInterval := shipped.MinResendInterval
	if shippedInterval <= 0 {
		shippedInterval = minResendInterval
	}
	t.Logf(
		"at the shipped settings (%d byte resend queue, %s minimum interval) one dead destination's opening rate is %.1f Mb/s, which is the report's 8 Mb/s per zombie; the backoff reaches the maximum interval about two doublings later, so forty zombies put about %.0f Mb/s on the wire only for the first seconds and far less after that",
		shipped.ResendQueueMaxByteCount,
		shippedInterval,
		float64(shipped.ResendQueueMaxByteCount)*8/shippedInterval.Seconds()/1e6,
		40*float64(shipped.ResendQueueMaxByteCount)*8/shippedInterval.Seconds()/1e6,
	)
	t.Logf(
		"one dead destination holding %d admitted bytes rewrote %d bytes in %s, %.2f Mb/s, against a ceiling of %d bytes (%.2f Mb/s) from a %d byte queue at a %s minimum interval",
		admittedByteCount,
		resendWriteByteCount,
		observationWindow,
		float64(resendWriteByteCount)*8/observationWindow.Seconds()/1e6,
		ceilingByteCount,
		float64(ceilingByteCount)*8/observationWindow.Seconds()/1e6,
		resendQueueMaxByteCount,
		minResendInterval,
	)
}

// THROUGHPUTFIX row Z1, and the row most likely to find a defect rather than
// confirm a fix. It decides a recorded disagreement about what a dead
// destination costs, and it decides it by reading the interval rather than by
// measuring a rate.
//
// The disagreement: §13.1 predicted 2.1 Mb/s per zombie, the resend queue over
// the 8 s `MaxResendInterval` an exponential backoff reaches after six
// rewrites. The bound derived from the queue over the 2 s `MinResendInterval`
// floor gives 8.4 Mb/s, and the reporter measured about 8. Both cannot
// describe the same steady state.
//
// The prediction, stated before the run. If the backoff climbs, the intervals
// grow from the floor toward the ceiling and the reporter caught an early
// window, inside the first twenty seconds after the kill. If they stay at the
// floor, that is a defect: `sendCount` advances only on one recovery path, so
// a destination that never acknowledges anything may never back off, and a
// zombie then emits at its maximum rate indefinitely rather than decaying.
//
// The answer, from the run: the backoff climbs, and fast. The interval
// sequence at this scale is 205 ms, 400 ms, 801 ms, and then the ceiling for
// every rewrite after that — two doublings and done, not the six §13.1
// assumed. At the shipped 2 s floor a zombie therefore sits at the 8 s ceiling
// about six seconds after the kill.
//
// So there is no defect here, and the disagreement resolves in the designer's
// favour: 2.1 Mb/s per zombie is the steady state and the reporter's 8 Mb/s
// describes only the first few seconds after a kill. The honest figure for
// forty zombies is about 84 Mb/s, not 336, so the unexplained remainder of the
// reporter's 460 Mb/s loss is about 376 Mb/s and grows rather than shrinks.
// Bandwidth accounts for less of the coupling than either reading assumed.
//
// The intervals are asserted as a sequence rather than an average, because an
// average over a climbing backoff and one over a pinned floor look alike while
// the sequences do not. The intervals are scaled down from the shipped 2 s and
// 8 s at the same ratio: a multiplicative backoff has the same shape at any
// scale, and what advances its count does not depend on the constants.
func TestDeadDestinationResendIntervalGrows(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	assertMessagePoolOwnership(t)

	// the shipped 2 s floor and 8 s ceiling, scaled by ten
	const minResendInterval = 200 * time.Millisecond
	const maxResendInterval = 800 * time.Millisecond
	const observationWindow = 12 * time.Second

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.MinResendInterval = minResendInterval
	settings.SendBufferSettings.RttMinResendInterval = minResendInterval
	settings.SendBufferSettings.MaxResendInterval = maxResendInterval
	// nothing may leave the queue by timing out inside the window
	settings.SendBufferSettings.AckTimeout = 5 * time.Minute
	settings.SendBufferSettings.IdleTimeout = 5 * time.Minute

	destinationId := NewId()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	route := make(chan []byte, 256)
	client.ContractManager().AddNoContractPeer(destinationId)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)

	// one item, never acknowledged: every write after the first is a rewrite
	// of it, so the gaps between writes are the resend intervals themselves
	writeNanos := make(chan int64, 256)
	drainCtx, drainCancel := context.WithCancel(ctx)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case transferFrameBytes := <-route:
				MessagePoolReturn(transferFrameBytes)
				select {
				case writeNanos <- monotonicNanos():
				default:
				}
			case <-drainCtx.Done():
				return
			}
		}
	}()
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := client.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close the client: %v", err)
		}
		drainCancel()
		<-drained
		for {
			select {
			case transferFrameBytes := <-route:
				MessagePoolReturn(transferFrameBytes)
			default:
				return
			}
		}
	})

	frame := RequireToFrameWithDefaultProtocolVersion(
		&protocol.SimpleMessage{Content: "the destination is gone"},
	)
	admitted, _ := client.SendWithTimeoutDetailed(
		frame,
		destinationId,
		nil,
		2*time.Second,
		sendPackRecoveryOption{upstreamRecoverable: true, retainAfterAckTimeout: true},
	)
	if !admitted {
		MessagePoolReturn(frame.MessageBytes)
		t.Fatal("the return toward the dead destination was not admitted")
	}

	time.Sleep(observationWindow)

	timestamps := []int64{}
	for draining := true; draining; {
		select {
		case writeNano := <-writeNanos:
			timestamps = append(timestamps, writeNano)
		default:
			draining = false
		}
	}
	if len(timestamps) < 4 {
		t.Fatalf("%d writes in %s, too few to read an interval sequence from", len(timestamps), observationWindow)
	}
	intervals := make([]time.Duration, 0, len(timestamps)-1)
	for i := 1; i < len(timestamps); i += 1 {
		intervals = append(intervals, time.Duration(timestamps[i]-timestamps[i-1]))
	}
	t.Logf("%d rewrites in %s, intervals %v", len(intervals), observationWindow, intervals)

	// the shape, not the average: a climbing backoff ends well above where it
	// began, a pinned one ends where it began
	firstInterval := intervals[0]
	lastInterval := intervals[len(intervals)-1]
	if lastInterval < 2*firstInterval {
		t.Errorf(
			"a destination that never acknowledged anything rewrote at %v and still at %v after %s; the backoff is not climbing, so a zombie emits at its floor rate indefinitely rather than decaying toward the %v ceiling. The steady-state figure for forty zombies is then the floor's 336 Mb/s rather than the ceiling's 84, and the remainder of the reporter's 460 Mb/s loss grows rather than shrinks",
			firstInterval,
			lastInterval,
			observationWindow,
			maxResendInterval,
		)
	}
	if lastInterval < maxResendInterval/2 {
		t.Errorf(
			"the last interval is %v against a %v ceiling; after %s of rewrites a backoff that climbs should be at or near it",
			lastInterval,
			maxResendInterval,
			observationWindow,
		)
	}
}
