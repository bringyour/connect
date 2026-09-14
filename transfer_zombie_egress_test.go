package connect

import (
	"context"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
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
// Both ends of a recorded disagreement belong here, because a reader who sees
// one will think the question was settled. The queue over the 2 s
// `MinResendInterval` floor gives 8.4 Mb/s per zombie and about 336 Mb/s at
// forty. The same queue over the 8 s `MaxResendInterval` that an exponential
// backoff reaches after six rewrites gives 2.1 Mb/s and about 84. §13.1
// predicted the ceiling; the report measured the floor. Either the reporter
// measured inside the first twenty seconds after the kill, before the backoff
// had climbed, or a destination that never acknowledges anything never backs
// off at all. Row Z1 decides it by reading the interval directly.
//
// What both readings agree on is the contribution: against a measured loss of
// about 460 Mb/s, zombie egress accounts for 336 at most and possibly only 84,
// so the remainder of 124 to 376 Mb/s is a quantified gap rather than an
// unexplained one.
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
		"at the shipped settings (%d byte resend queue, %s minimum interval) one dead destination's ceiling is %.1f Mb/s; the report's 8 Mb/s per zombie is that bound saturated rather than an independent measurement, so forty zombies can account for at most about %.0f Mb/s and the rest of the 460 Mb/s loss is not their egress",
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
