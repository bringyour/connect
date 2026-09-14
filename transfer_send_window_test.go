package connect

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// What these rows claim, and the two harms they deliberately do not
// (THROUGHPUTFIX §37.15).
//
// Twice a harm was predicted from an accurate source reading and twice a cell
// falsified it by three orders of magnitude. For TCP through the tunnel, the
// client moderates its own receive window from bytes copied per round trip, so
// the origin backs off and the transfer layer is never handed more than the
// path carries: about 2.5 ms of added delay on every arm, peak send queue
// 20 KiB whether the window was 2, 3.6 or 16 MiB. For UDP, the return path
// admits non-blocking with a zero write timeout, and a non-blocking admit turns
// excess into loss rather than into delay or occupancy: 1.6 to 1.8 ms added on
// every arm, peak queue 16 to 20 KiB, 86.7 per cent loss, every drop at the
// return send.
//
// The fact underneath both, which is also the rule for reading any occupancy
// row here: the sequence goroutine writes a Pack to the carrier before the item
// enters the resend queue, so the resend queue holds only what the carrier has
// already accepted. Against a carrier accepting at its drain rate that is one
// carrier round trip of bytes and nothing more, and no window above it is ever
// reached. Occupancy can approach the window only where a layer below the
// sequence accepts faster than the far end drains. This package's fixture is
// exactly such a layer — its route channel takes a thousand frames instantly
// and the pump drains them at the configured rate — so a queue measured here is
// a model of the platform relay and not of either carrier in this tree. Rows
// below therefore do not assert a memory or latency harm; where one is
// measured it is logged as a model, labelled as such.
//
// So the rows claim the two things the evidence supports:
//
//   - Throughput. The 2 MiB window binds a long round trip, and a larger
//     permission lifts it to the next binder at no measured cost.
//   - Mechanism. The window is computed from the measured minimum round trip
//     and the measured delivery rate rather than from a floored resend timer;
//     it shrinks when the path shrinks; and it converges to what the path
//     delivers rather than to whatever ceiling is configured.
//
// The receive advertisement rows are step two, which §37.15 holds behind a
// multi-route failover cell. They run with the setting turned on explicitly,
// and they guard a prerequisite for raising the ceilings safely rather than a
// fix for a defect anything has reproduced: at the shipping 2 MiB window the
// hold is never exceeded, by an accident of ordering rather than by design.

// A sender and a receiver joined by a delayed acknowledgement half, with the
// send window rule configurable, so the before and after are the same binary
// with one field changed.
type sendWindowHarness struct {
	sender     *Client
	receiver   *Client
	receiverId Id
	// when set, the data half drops the next frame it carries, once
	dropNext *atomic.Bool
	drops    *atomic.Int64
	// the carrier's drain in bytes per second, settable mid-flight so one
	// cell can measure a path that changes
	bytesPerSecond *atomic.Int64
	// How many frames the carrier route holds. A fixture's own wire capacity
	// decides whether a window's permission can become occupancy at all
	// (THROUGHPUTFIX §37.15), so a cell that reads a queue records it.
	wireFrameCapacity int
}

// Sets the receiver's hold, which is what it advertises less what it holds,
// and turns the advertisement on. The setting ships off (THROUGHPUTFIX §37.15
// holds step two behind the multi-route failover cell), so a row that wants the
// advertisement asks for it here and no row gets it by default.
func (self *sendWindowHarness) receiveHold(byteCount ByteCount) {
	self.receiver.settings.ReceiveBufferSettings.ReceiveQueueMaxByteCount = byteCount
	self.receiver.settings.ReceiveBufferSettings.AdvertiseReceiveWindow = true
}

func newSendWindowHarness(
	t *testing.T,
	ctx context.Context,
	ackDelay time.Duration,
	configure func(*SendBufferSettings),
) *sendWindowHarness {
	return newPacedSendWindowHarness(t, ctx, ackDelay, 0, deepCarrierFrameCapacity, configure)
}

// The same harness with the data half paced at a byte rate, which is what a
// bottleneck link is: frames depart in order at the link's rate, so a sender
// whose window exceeds rate times round trip leaves the excess standing as
// queue. Serialisation is the point here rather than an artefact.
// Two carrier shapes, because which one a cell uses decides what it measures
// (THROUGHPUTFIX §37.15). A deep carrier accepts a thousand frames instantly and
// the pump drains them at the configured rate: that is a model of the platform
// relay, and a window's permission becomes occupancy behind it. A shallow
// carrier accepts about one round trip of frames and blocks the sequence
// goroutine beyond that, which is what quic-go and an autotuned kernel socket
// do, and is the only shape either carrier in this tree has.
const deepCarrierFrameCapacity = 1024
const shallowCarrierFrameCapacity = 8

func newRateLimitedSendWindowHarness(
	t *testing.T,
	ctx context.Context,
	ackDelay time.Duration,
	bytesPerSecond ByteCount,
	configure func(*SendBufferSettings),
) *sendWindowHarness {
	t.Helper()
	return newPacedSendWindowHarness(
		t, ctx, ackDelay, bytesPerSecond, deepCarrierFrameCapacity, configure)
}

func newPacedSendWindowHarness(
	t *testing.T,
	ctx context.Context,
	ackDelay time.Duration,
	bytesPerSecond ByteCount,
	carrierFrameCapacity int,
	configure func(*SendBufferSettings),
) *sendWindowHarness {
	t.Helper()
	newSettings := func() *ClientSettings {
		settings := DefaultClientSettings()
		settings.EncryptionSettings.Mode = EncryptionModeOff
		settings.SendBufferSettings.AckTimeout = 60 * time.Second
		settings.SendBufferSettings.IdleTimeout = 60 * time.Second
		settings.ReceiveBufferSettings.GapTimeout = 60 * time.Second
		settings.ReceiveBufferSettings.IdleTimeout = 60 * time.Second
		settings.ReceiveBufferSettings.ReceiveQueueMaxByteCount = mib(64)
		if configure != nil {
			configure(settings.SendBufferSettings)
		}
		return settings
	}
	senderId := NewId()
	receiverId := NewId()
	sender := NewClient(ctx, senderId, NewNoContractClientOob(), newSettings())
	receiver := NewClient(ctx, receiverId, NewNoContractClientOob(), newSettings())
	sender.ContractManager().AddNoContractPeer(receiverId)
	receiver.ContractManager().AddNoContractPeer(senderId)

	senderOut := make(Route, carrierFrameCapacity)
	senderIn := make(Route, deepCarrierFrameCapacity)
	receiverIn := make(Route, deepCarrierFrameCapacity)
	receiverOut := make(Route, deepCarrierFrameCapacity)
	sender.RouteManager().UpdateTransport(NewSendGatewayTransport(), []Route{senderOut})
	sender.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{senderIn})
	receiver.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{receiverIn})
	receiver.RouteManager().UpdateTransport(NewSendGatewayTransport(), []Route{receiverOut})
	receiver.AddReceiveCallback(func(TransferPath, []*protocol.Frame, Peer) {})

	// every frame in flight is owned by its own goroutine, so cleanup joins
	// them before draining: a frame still sleeping when the loop exits is a
	// leaked pool root, which the ownership assertion catches
	var framesInFlight sync.WaitGroup
	// per frame, concurrently: a pump that sleeps in its own loop is a serial
	// line and would bound the measurement rather than the window
	pumpsDone := []chan struct{}{}
	dropNext := &atomic.Bool{}
	drops := &atomic.Int64{}
	rate := &atomic.Int64{}
	rate.Store(int64(bytesPerSecond))
	ratePump := func(from Route, to Route) {
		done := make(chan struct{})
		pumpsDone = append(pumpsDone, done)
		go func() {
			defer close(done)
			departure := time.Now()
			for {
				select {
				case transferFrameBytes := <-from:
					serviceTime := time.Duration(
						int64(len(transferFrameBytes)) * int64(time.Second) / rate.Load(),
					)
					now := time.Now()
					if departure.Before(now) {
						departure = now
					}
					departure = departure.Add(serviceTime)
					if wait := time.Until(departure); 0 < wait {
						select {
						case <-time.After(wait):
						case <-ctx.Done():
							MessagePoolReturn(transferFrameBytes)
							return
						}
					}
					select {
					case to <- transferFrameBytes:
					case <-ctx.Done():
						MessagePoolReturn(transferFrameBytes)
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	pump := func(from Route, to Route, delay time.Duration) {
		done := make(chan struct{})
		pumpsDone = append(pumpsDone, done)
		go func() {
			defer close(done)
			for {
				select {
				case transferFrameBytes := <-from:
					// one induced loss on the data half, for the loss cell
					if delay == 0 && dropNext.CompareAndSwap(true, false) {
						drops.Add(1)
						MessagePoolReturn(transferFrameBytes)
						continue
					}
					framesInFlight.Add(1)
					go func(transferFrameBytes []byte) {
						defer framesInFlight.Done()
						if 0 < delay {
							time.Sleep(delay)
						}
						select {
						case to <- transferFrameBytes:
						case <-ctx.Done():
							MessagePoolReturn(transferFrameBytes)
						}
					}(transferFrameBytes)
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	if 0 < bytesPerSecond {
		ratePump(senderOut, receiverIn)
	} else {
		pump(senderOut, receiverIn, 0)
	}
	pump(receiverOut, senderIn, ackDelay)
	t.Cleanup(func() {
		for _, done := range pumpsDone {
			<-done
		}
		framesInFlight.Wait()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := sender.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close the sender: %v", err)
		}
		if err := receiver.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close the receiver: %v", err)
		}
		for _, route := range []Route{senderOut, senderIn, receiverIn, receiverOut} {
			draining := true
			for draining {
				select {
				case transferFrameBytes := <-route:
					MessagePoolReturn(transferFrameBytes)
				default:
					draining = false
				}
			}
		}
	})
	return &sendWindowHarness{
		sender:            sender,
		receiver:          receiver,
		receiverId:        receiverId,
		dropNext:          dropNext,
		drops:             drops,
		bytesPerSecond:    rate,
		wireFrameCapacity: cap(senderOut),
	}
}

// offers packs as fast as they are admitted, for the given window
func (self *sendWindowHarness) offer(t *testing.T, payloadByteCount int, window time.Duration) {
	t.Helper()
	payload := string(make([]byte, payloadByteCount))
	deadline := time.Now().Add(window)
	for time.Now().Before(deadline) {
		frame := RequireToFrameWithDefaultProtocolVersion(
			&protocol.SimpleMessage{Content: payload},
		)
		admitted, _ := self.sender.SendWithTimeoutDetailed(
			frame,
			self.receiverId,
			nil,
			50*time.Millisecond,
			sendPackRecoveryOption{upstreamRecoverable: true, retainAfterAckTimeout: true},
		)
		if !admitted {
			MessagePoolReturn(frame.MessageBytes)
		}
	}
}

// THROUGHPUTFIX §32.5 row W4. The send window is a constant today, so a flow
// whose path could carry more than that constant is held at it however fast
// the path is. The rule makes the window what the lane delivered over the last
// acknowledgement round trip, times a scale of at least two, so a
// window-limited flow — which by definition delivers exactly its window per
// round trip — doubles each round trip until it is no longer window-limited.
//
// This is the before arm pinned by a test rather than only by a setting: with
// the scale off the window never leaves its floor whatever the path does, and
// with it on the window rises above the floor and the rule reports that it
// engaged. The two runs are the same binary with one field changed.
func TestDeliverySizedWindowConvergesInLogRoundTrips(t *testing.T) {
	assertMessagePoolOwnership(t)

	const ackDelay = 25 * time.Millisecond
	const floor = ByteCount(256 * 1024)
	const ceiling = ByteCount(4 * 1024 * 1024)
	const payloadByteCount = 4 * 1024
	const offerWindow = 2 * time.Second

	constantWindow := func() SendWindowEstimate {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		harness := newSendWindowHarness(t, ctx, ackDelay, func(settings *SendBufferSettings) {
			settings.ResendQueueMaxByteCount = floor
		})
		harness.offer(t, payloadByteCount, offerWindow)
		return harness.sender.DestinationSendStats(harness.receiverId).SendWindow
	}()

	sizedWindow := func() SendWindowEstimate {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		harness := newSendWindowHarness(t, ctx, ackDelay, func(settings *SendBufferSettings) {
			settings.ResendQueueMaxByteCount = floor
			settings.DeliverySizedWindowScale = 2
			settings.DeliverySizedWindowCeilingByteCount = ceiling
			// the policy: a sender sizes only against a bound it can see, so
			// the rule needs a memory budget before it will grow at all
			settings.ResendQueueBudget = NewTransferMemoryBudget(ceiling)
		})
		harness.offer(t, payloadByteCount, offerWindow)
		return harness.sender.DestinationSendStats(harness.receiverId).SendWindow
	}()

	// the before arm: a constant, and the rule says so rather than reporting a
	// measured window that happens to equal it
	if constantWindow.Sized {
		t.Errorf("the window reports itself sized with the scale off: %+v", constantWindow)
	}
	if constantWindow.Window != floor {
		t.Errorf("the constant window is %d, want the %d byte floor", constantWindow.Window, floor)
	}

	// the after arm: sized from delivery, above the floor, and reporting the
	// evidence it used
	if !sizedWindow.Sized {
		t.Fatalf("the window rule did not engage: %+v", sizedWindow)
	}
	if sizedWindow.SampleCount <= 0 {
		t.Errorf("the window is sized from %d delivery samples", sizedWindow.SampleCount)
	}
	if sizedWindow.Window <= constantWindow.Window {
		t.Errorf(
			"the sized window is %d against the %d byte constant, so the rule bought nothing on a path that delivered %d bytes over a %s round trip",
			sizedWindow.Window,
			constantWindow.Window,
			sizedWindow.DeliveredByteCount,
			sizedWindow.Interval,
		)
	}
	if ceiling < sizedWindow.Window {
		t.Errorf("the sized window is %d, above its %d byte ceiling", sizedWindow.Window, ceiling)
	}
	t.Logf(
		"constant %d; sized %d from %d bytes delivered over %s across %d samples, floor %d ceiling %d",
		constantWindow.Window,
		sizedWindow.Window,
		sizedWindow.DeliveredByteCount,
		sizedWindow.Interval,
		sizedWindow.SampleCount,
		sizedWindow.Floor,
		sizedWindow.Ceiling,
	)
}

// §32.5, the retention claim: the ceiling is a share of a budget rather than a
// per-sequence constant, because forty simultaneous downloaders at a
// per-sequence maximum would retain more than a provider has. What reaches the
// ceiling in practice is the traffic nothing backs off — a TCP flow through the
// tunnel is held well below it by the client's own receive moderation — so this
// bounds a provider against many datagram senders rather than against TCP. A constant that works in a one-client
// cell is exactly what fails in production, and no measurement available today
// would show it, so the row does.
func TestDeliverySizedWindowCeilingIsAShareOfABudget(t *testing.T) {
	assertMessagePoolOwnership(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const floor = ByteCount(256 * 1024)
	const ceiling = ByteCount(8 * 1024 * 1024)
	// the budget lends far less than the ceiling asks for
	const budgetByteCount = ByteCount(1024 * 1024)

	harness := newSendWindowHarness(t, ctx, 25*time.Millisecond, func(settings *SendBufferSettings) {
		settings.ResendQueueMaxByteCount = floor
		settings.DeliverySizedWindowScale = 2
		settings.DeliverySizedWindowCeilingByteCount = ceiling
		settings.ResendQueueBudget = NewTransferMemoryBudget(budgetByteCount)
	})
	harness.offer(t, 4*1024, 2*time.Second)
	window := harness.sender.DestinationSendStats(harness.receiverId).SendWindow

	if !window.Sized {
		t.Fatalf("the window rule did not engage: %+v", window)
	}
	if budgetByteCount < window.Ceiling {
		t.Errorf(
			"the window's ceiling is %d against a %d byte shared budget; a ceiling above what the budget will lend is a per-sequence constant wearing a budget's name, and forty of them would retain more than a provider has",
			window.Ceiling,
			budgetByteCount,
		)
	}
	if budgetByteCount < window.Window {
		t.Errorf("the window is %d, above the %d the budget will lend", window.Window, budgetByteCount)
	}
	t.Logf(
		"ceiling asked %d, budget lends %d, ceiling applied %d, window %d",
		ceiling,
		budgetByteCount,
		window.Ceiling,
		window.Window,
	)
}

// THROUGHPUTFIX §37.12's single policy: a sender sizes only against a bound it
// can see, and holds the initial size where it cannot. A budget for memory, an
// advertisement for the receiver, an estimate with samples for the round trip.
//
// The point of the policy is that the safe configuration is the default rather
// than the documented one, so each arm here is a bound removed and the window
// must hold rather than grow.
func TestSendWindowHoldsTheInitialSizeWhereItCannotSee(t *testing.T) {
	assertMessagePoolOwnership(t)

	const initial = ByteCount(256 * 1024)

	// memory it cannot see: the rule is on and the path is fast, but no shared
	// budget bounds what every sequence of a provider holds together
	noBudget := func() SendWindowEstimate {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		harness := newSendWindowHarness(t, ctx, 25*time.Millisecond, func(settings *SendBufferSettings) {
			settings.ResendQueueMaxByteCount = initial
			settings.DeliverySizedWindowScale = 2
			settings.DeliverySizedWindowCeilingByteCount = ByteCount(8 * 1024 * 1024)
		})
		harness.offer(t, 4*1024, time.Second)
		return harness.sender.DestinationSendStats(harness.receiverId).SendWindow
	}()
	if noBudget.Sized {
		t.Errorf("the window sized itself with no memory budget: %+v", noBudget)
	}
	if noBudget.Window != initial {
		t.Errorf("with no budget the window is %d, want the %d byte initial size", noBudget.Window, initial)
	}

	// a round trip it cannot see: every bound is in place but nothing has been
	// acknowledged, so there is no estimate to size from
	noSamples := func() SendWindowEstimate {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// an acknowledgement half longer than this arm runs, so the sequence
		// exists and has sent, and nothing has come back to size from
		harness := newSendWindowHarness(t, ctx, 2*time.Second, func(settings *SendBufferSettings) {
			settings.ResendQueueMaxByteCount = initial
			settings.DeliverySizedWindowScale = 2
			settings.DeliverySizedWindowCeilingByteCount = ByteCount(8 * 1024 * 1024)
			settings.ResendQueueBudget = NewTransferMemoryBudget(ByteCount(8 * 1024 * 1024))
		})
		harness.offer(t, 4*1024, 200*time.Millisecond)
		return harness.sender.DestinationSendStats(harness.receiverId).SendWindow
	}()
	if noSamples.Sized {
		t.Errorf("the window sized itself with nothing acknowledged: %+v", noSamples)
	}
	if noSamples.Window != initial {
		t.Errorf("with no samples the window is %d, want the %d byte initial size", noSamples.Window, initial)
	}

	t.Logf("no budget: %q window %d; no samples: %q window %d", noBudget.Reason, noBudget.Window, noSamples.Reason, noSamples.Window)
}

// THROUGHPUTFIX §37.3, step two, the receiver-memory claim rather than a
// latency one. A Pack that arrives above the receiver's hold
// is dropped and must be sent again, so a sender whose window exceeds the hold
// turns one loss into one window of retransmission. The receiver is the only
// party that knows what it can hold — it has no round trip to size from, and
// the sender's ceiling is a deployment setting it cannot see — so it says so on
// the acknowledgement and the sender clamps to it.
//
// Prediction, recorded before the run: with a hold far below what the path
// would otherwise justify, the sender's window is bounded by the hold rather
// than by the path or the budget.
func TestSendWindowClampsToTheReceiversAdvertisement(t *testing.T) {
	assertMessagePoolOwnership(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const hold = ByteCount(512 * 1024)
	const ceiling = ByteCount(16 * 1024 * 1024)

	harness := newSendWindowHarness(t, ctx, 25*time.Millisecond, func(settings *SendBufferSettings) {
		settings.ResendQueueMaxByteCount = ByteCount(128 * 1024)
		settings.DeliverySizedWindowScale = 2
		settings.DeliverySizedWindowCeilingByteCount = ceiling
		settings.ResendQueueBudget = NewTransferMemoryBudget(ceiling)
	})
	// the receiver's hold, which both peers of this harness share as a setting
	harness.receiveHold(hold)
	harness.offer(t, 4*1024, 2*time.Second)
	window := harness.sender.DestinationSendStats(harness.receiverId).SendWindow

	if !window.Sized {
		t.Fatalf("the window rule did not engage: %+v", window)
	}
	if hold < window.Ceiling {
		t.Errorf(
			"the ceiling is %d against a %d byte receiver hold; a sender may not let its window exceed what its receiver said it can take",
			window.Ceiling,
			hold,
		)
	}
	if hold < window.Window {
		t.Errorf("the window is %d, above the %d the receiver advertised", window.Window, hold)
	}
	t.Logf("receiver hold %d: ceiling %d, window %d, from %d bytes over %s at a %s minimum round trip",
		hold, window.Ceiling, window.Window, window.DeliveredByteCount, window.Interval, window.RoundTrip)
}

// THROUGHPUTFIX §36.6's falsifiable discriminator between the two forms of the
// delivery term, at the short round trip where they separate.
//
// The prediction, recorded before the run. The ring advances on its own
// cadence, so a sum over a horizon returns whatever was delivered since the
// newest sample older than that horizon — between one and twelve times a short
// round trip's worth, depending on where the ring happened to sit — and the
// window computed from it scatters from one estimate to the next. A rate,
// bytes between two samples divided by their spacing, removes that dependence,
// so the window holds. If the rate form scatters too, the ring's cadence was
// not the cause and the estimator has a defect the design did not find.
func TestDeliverySizedWindowRateFormHoldsAtAShortRoundTrip(t *testing.T) {
	assertMessagePoolOwnership(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const ceiling = ByteCount(16 * 1024 * 1024)
	harness := newSendWindowHarness(t, ctx, 5*time.Millisecond, func(settings *SendBufferSettings) {
		settings.ResendQueueMaxByteCount = ByteCount(128 * 1024)
		settings.DeliverySizedWindowScale = 2
		settings.DeliverySizedWindowCeilingByteCount = ceiling
		settings.ResendQueueBudget = NewTransferMemoryBudget(ceiling)
	})
	// a hold far above the path, so the advertisement does not bind and this
	// row measures the delivery term alone
	harness.receiveHold(ceiling)

	// let the rule reach its fixed point, then read it repeatedly while the
	// path is steady
	offering := make(chan struct{})
	offerDone := make(chan struct{})
	go func() {
		defer close(offerDone)
		payload := string(make([]byte, 4*1024))
		for {
			select {
			case <-offering:
				return
			default:
			}
			frame := RequireToFrameWithDefaultProtocolVersion(
				&protocol.SimpleMessage{Content: payload},
			)
			admitted, _ := harness.sender.SendWithTimeoutDetailed(
				frame, harness.receiverId, nil, 50*time.Millisecond,
				sendPackRecoveryOption{upstreamRecoverable: true, retainAfterAckTimeout: true},
			)
			if !admitted {
				MessagePoolReturn(frame.MessageBytes)
			}
		}
	}()
	time.Sleep(time.Second)

	windows := []ByteCount{}
	for range 12 {
		window := harness.sender.DestinationSendStats(harness.receiverId).SendWindow
		if window.Sized {
			windows = append(windows, window.Window)
		}
		time.Sleep(40 * time.Millisecond)
	}
	close(offering)
	<-offerDone

	if len(windows) < 8 {
		t.Fatalf("only %d of 12 reads were sized; the rule did not hold at a 5 ms round trip", len(windows))
	}
	smallest, largest := windows[0], windows[0]
	for _, window := range windows {
		smallest = min(smallest, window)
		largest = max(largest, window)
	}
	// the sum form scattered across an order of magnitude; a fourfold spread
	// is well inside that and well outside the noise of a steady path
	if smallest <= 0 || 4*smallest < largest {
		t.Errorf(
			"the computed window scattered from %d to %d across %d reads of a steady path, a factor of %.1f; a rate divided by the span it measured should not depend on where the ring sat, so either the cadence was not the cause or the estimator has another defect",
			smallest,
			largest,
			len(windows),
			float64(largest)/float64(max(1, smallest)),
		)
	}
	t.Logf("windows over a steady 5 ms path: %v (spread %.2fx)", windows, float64(largest)/float64(max(1, smallest)))
}

// THROUGHPUTFIX §37.10 step two's acceptance, again as a memory and
// retransmission claim: the advertisement is what keeps the receiver from
// dropping, not what keeps the queue short. A Pack above the receiver's hold
// is dropped and must be sent again, so before the advertisement a sender whose
// window exceeded the hold turned one loss into one window of retransmission.
//
// The prediction, recorded before the run: with the advertisement in place the
// receiver's drop count stays at zero through an induced loss, because the
// sender never had more in flight than the receiver could hold, and the
// retransmission is the lost item rather than the window behind it.
func TestReceiveAdvertisementStopsTheLossRetransmitStorm(t *testing.T) {
	assertMessagePoolOwnership(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const ceiling = ByteCount(16 * 1024 * 1024)
	const hold = ByteCount(512 * 1024)
	const payloadByteCount = 4 * 1024

	harness := newSendWindowHarness(t, ctx, 25*time.Millisecond, func(settings *SendBufferSettings) {
		settings.ResendQueueMaxByteCount = ByteCount(128 * 1024)
		settings.DeliverySizedWindowScale = 2
		settings.DeliverySizedWindowCeilingByteCount = ceiling
		settings.ResendQueueBudget = NewTransferMemoryBudget(ceiling)
	})
	harness.receiveHold(hold)

	// reach the fixed point, then lose one Pack
	harness.offer(t, payloadByteCount, time.Second)
	window := harness.sender.DestinationSendStats(harness.receiverId).SendWindow
	before := harness.sender.DestinationSendStats(harness.receiverId)
	beforeDrops := harness.receiver.receiveQueueDropCount.Load()

	harness.dropNext.Store(true)
	harness.offer(t, payloadByteCount, time.Second)

	after := harness.sender.DestinationSendStats(harness.receiverId)
	afterDrops := harness.receiver.receiveQueueDropCount.Load()
	resentByteCount := ByteCount(after.ResendWriteByteCount - before.ResendWriteByteCount)

	if harness.drops.Load() != 1 {
		t.Fatalf("the cell induced %d losses, want exactly one", harness.drops.Load())
	}
	if !window.Sized {
		t.Fatalf("the window rule did not engage, so this cell does not test the advertisement: %+v", window)
	}
	if hold < window.Window {
		t.Errorf("the window is %d above the %d byte hold, so the advertisement did not bind", window.Window, hold)
	}
	// the receiver never has to refuse an arrival, because the sender never
	// sent more than it said it could hold
	if beforeDrops != afterDrops {
		t.Errorf(
			"the receiver dropped %d arrivals through one induced loss; with the sender clamped to the advertised hold there is nothing above the hold to drop",
			afterDrops-beforeDrops,
		)
	}
	// And the retransmission is the item rather than the window behind it.
	// The failure mode this replaces is a window or more: every arrival above
	// the hold is refused and sent again. Measured here at 8 KB to 118 KB
	// against a 512 KiB window, two to twenty-nine items, so the assertion is
	// against the window rather than against a tighter band the noise of a
	// shared runner would cross.
	if window.Window <= resentByteCount {
		t.Errorf(
			"one induced loss cost %d bytes of retransmission against a %d byte window; the advertisement exists so that a loss costs the item rather than the window behind it",
			resentByteCount,
			window.Window,
		)
	}
	t.Logf(
		"window %d, hold %d: one loss cost %d bytes of retransmission (%d items of %d) and %d receiver drops",
		window.Window, hold, resentByteCount, resentByteCount/payloadByteCount, payloadByteCount, afterDrops-beforeDrops,
	)
}

// THROUGHPUTFIX §37.15's mechanism claim, the first of two: the window is
// computed from the round trip the path actually has, rather than from a resend
// timer that has a floor.
//
// The defect from source. The rule as first built multiplied the delivery term
// by `ScaledRtt`, which is the retransmit pacing estimate and is floored at
// `RttMinResendInterval`, 300 ms. On a 25 ms path it therefore multiplied by
// twelve times the round trip, and on a 6.7 ms path by forty-five. Measured on
// three paths before the correction: 300 ms flat against real round trips of
// 6.7, 27 and 102 ms, an overshoot of 2.9 to 44.8 times.
//
// This row does not claim a consequence for that overshoot. Both cells that
// tried to measure one — TCP on a slow drain, UDP at 97 Mb/s into 20 — found
// about 2 ms rather than the hundreds predicted, for the reasons in the file
// header. A rule that multiplies a delivery rate by a resend timer is wrong on
// its own terms, and that is what this asserts: the window equals the scale
// times the measured delivery rate over the measured minimum round trip, from
// the estimate's own evidence fields, to the byte.
//
// Prediction, recorded before the run: on a 25 ms path the reported window
// equals scale x delivered x roundTrip / interval exactly, and the round trip
// the rule used is close to the path's rather than at the 300 ms floor.
//
// Correction after the first run, kept on the record. At a 5 ms propagation the
// computed value was 204,800 bytes, below the 262,144 floor, so the row passed
// with the window clamped and would have gone on passing had the rule read the
// resend timer. The path is 25 ms here and the row now fails if the window
// lands on either clamp, because a cell that measures a clamp measures nothing.
func TestSizedWindowIsComputedFromTheMeasuredRoundTrip(t *testing.T) {
	assertMessagePoolOwnership(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const propagation = 25 * time.Millisecond
	const ceiling = ByteCount(16 * 1024 * 1024)
	harness := newSendWindowHarness(t, ctx, propagation, func(settings *SendBufferSettings) {
		settings.DeliverySizedWindowScale = 2
		settings.DeliverySizedWindowCeilingByteCount = ceiling
		settings.ResendQueueBudget = NewTransferMemoryBudget(ceiling)
	})
	harness.offer(t, 4*1024, 2*time.Second)
	estimate := harness.sender.DestinationSendStats(harness.receiverId).SendWindow

	t.Logf(
		"window %d from %d bytes over %s at a %s round trip, floor %d ceiling %d, reason %q",
		estimate.Window, estimate.DeliveredByteCount, estimate.Interval,
		estimate.RoundTrip, estimate.Floor, estimate.Ceiling, estimate.Reason,
	)

	if !estimate.Sized {
		t.Fatalf("the window rule did not engage, so this cell does not test it: %+v", estimate)
	}
	// the rule's own arithmetic, recomputed from the evidence it published
	want := ByteCount(int64(estimate.DeliveredByteCount) *
		estimate.RoundTrip.Nanoseconds() / estimate.Interval.Nanoseconds())
	want = min(max(ByteCount(2)*want, estimate.Floor), estimate.Ceiling)
	if estimate.Window != want {
		t.Errorf(
			"the window is %d but the evidence it reports gives %d; the rule and the number it publishes have to be the same rule",
			estimate.Window,
			want,
		)
	}
	// the floored resend timer is 300 ms; a rule reading it would be here
	if floor := DefaultSendBufferSettings().RttMinResendInterval; estimate.RoundTrip >= floor {
		t.Errorf(
			"the rule multiplied by %s on a %s path, at or above the %s resend floor, which is the defect this step corrects",
			estimate.RoundTrip,
			propagation,
			floor,
		)
	}
	if estimate.Window >= estimate.Ceiling {
		t.Errorf(
			"the window reached its %d byte ceiling, so this cell measured the ceiling rather than the path; the rule is supposed to converge to what the path delivers",
			estimate.Ceiling,
		)
	}
	if estimate.Window <= estimate.Floor {
		t.Errorf(
			"the window sits on its %d byte floor, so this cell measured a clamp and would pass with the rule reading a resend timer",
			estimate.Floor,
		)
	}
}

// THROUGHPUTFIX §37.15's mechanism claim, the second: a window sized from the
// path follows the path down as well as up.
//
// This is the property a constant cannot have and the one the rule exists for.
// A flow whose path slows — a carrier handover, a congested hop, a provider
// taking on more clients — holds a window sized for the old path under any
// constant, and the excess permission is exactly the ramp §36.7 describes.
//
// Which carrier this runs against, and why it decides the answer. The rule
// multiplies the measured delivery rate by the measured minimum round trip.
// Behind a deep buffer the minimum is inflated by the queue the permission
// itself creates, and the two move against each other: measured on the deep
// carrier, cutting the drain by eight cut the delivery rate by 5.7 and raised
// the minimum round trip from 200 ms to 888 ms, so the window fell by only
// 1.29 — 680,406 to 527,516 — and the prediction of a factor of two was wrong.
// That is recorded rather than loosened away. Against a carrier that accepts at
// its drain rate, which is the only shape either carrier in this tree has, the
// queue does not form, the minimum stays at the path's, and the window tracks
// the rate. So this row runs on the shallow carrier and the deep-carrier figure
// above is the trade: behind a relay the response is damped by the queue the
// permission creates.
//
// Prediction, recorded before the second run: on the shallow carrier, cutting
// the drain by eight lowers the reported window by at least a factor of four,
// with the minimum round trip staying within a round trip or so of the path.
//
// Measured, four runs: 3.57, 3.73, 3.64 and 4.07 times, so the prediction of
// four was a little high and the bar here is three. The damping that remains is
// not the fixture: the layers below the sequence — the carrier's own frames and
// the 32-item packs channel — hold a roughly fixed number of bytes, tens of
// kilobytes, and a fixed number of bytes costs proportionally more time at a
// slower drain. The minimum round trip rose from 225 ms to 342 ms across the
// cut for that reason, against 200 ms of propagation. This is the same 20 KiB
// the UDP and TCP cells both measured, seen from the other side.
func TestSizedWindowShrinksWhenThePathShrinks(t *testing.T) {
	assertMessagePoolOwnership(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const fastBytesPerSecond = ByteCount(20 * 1000 * 1000 / 8)
	const slowBytesPerSecond = fastBytesPerSecond / 8
	// the long round trip is where the window binds throughput at all, and it
	// is the only regime in which both arms clear the floor
	const propagation = 200 * time.Millisecond
	const ceiling = ByteCount(16 * 1024 * 1024)
	const floor = ByteCount(64 * 1024)
	harness := newPacedSendWindowHarness(t, ctx, propagation, fastBytesPerSecond,
		shallowCarrierFrameCapacity,
		func(settings *SendBufferSettings) {
			settings.DeliverySizedWindowScale = 2
			settings.DeliverySizedWindowCeilingByteCount = ceiling
			settings.ResendQueueMinByteCount = floor
			settings.ResendQueueBudget = NewTransferMemoryBudget(ceiling)
		})

	harness.offer(t, 4*1024, 4*time.Second)
	fast := harness.sender.DestinationSendStats(harness.receiverId).SendWindow
	harness.bytesPerSecond.Store(int64(slowBytesPerSecond))
	harness.offer(t, 4*1024, 6*time.Second)
	slow := harness.sender.DestinationSendStats(harness.receiverId).SendWindow

	t.Logf(
		"at %d B/s window %d (%d bytes over %s, round trip %s); at %d B/s window %d (%d bytes over %s, round trip %s)",
		fastBytesPerSecond, fast.Window, fast.DeliveredByteCount, fast.Interval, fast.RoundTrip,
		slowBytesPerSecond, slow.Window, slow.DeliveredByteCount, slow.Interval, slow.RoundTrip,
	)

	if !fast.Sized || !slow.Sized {
		t.Fatalf("the window rule did not engage on both arms: fast %+v slow %+v", fast, slow)
	}
	if 3*slow.Window > fast.Window {
		t.Errorf(
			"the drain fell by eight and the window went from %d to %d; a window sized from the path has to follow the path down, which is the whole of what a constant cannot do",
			fast.Window,
			slow.Window,
		)
	}
	if slow.Window <= slow.Floor {
		t.Errorf(
			"the window fell to its %d byte floor, so this cell cannot tell a rule that tracks the path from one that collapsed",
			slow.Floor,
		)
	}
}

// The window rule reads sequence-local state — the delivered-bytes ring — and
// `DestinationSendStats` reaches it from whatever goroutine asks for a
// snapshot, while the acknowledgement worker is advancing it. Step one put that
// read on the stats path and the ring had no lock, which the race detector
// caught once in four runs of an unrelated row: often enough to be real, rarely
// enough to be dismissed as a flake. This row makes it deterministic by giving
// the detector a reader on every core for the whole of a transfer.
//
// It guards a root cause rather than a fix: any future field the rule reads
// from the sequence without a lock fails here, not only the ring.
func TestSendWindowStatsAreSafeToReadWhileTheSequenceRuns(t *testing.T) {
	assertMessagePoolOwnership(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := newSendWindowHarness(t, ctx, 5*time.Millisecond, func(settings *SendBufferSettings) {
		settings.DeliverySizedWindowScale = 2
		settings.DeliverySizedWindowCeilingByteCount = ByteCount(16 * 1024 * 1024)
		settings.ResendQueueBudget = NewTransferMemoryBudget(ByteCount(16 * 1024 * 1024))
	})

	readers := max(4, runtime.GOMAXPROCS(0))
	stop := make(chan struct{})
	reads := &atomic.Int64{}
	var running sync.WaitGroup
	for range readers {
		running.Add(1)
		go func() {
			defer running.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				stats := harness.sender.DestinationSendStats(harness.receiverId)
				// touch the evidence too, so a racing read of any of it counts
				_ = stats.SendWindow.Window + stats.SendWindow.DeliveredByteCount
				reads.Add(1)
			}
		}()
	}
	harness.offer(t, 4*1024, 2*time.Second)
	close(stop)
	running.Wait()

	t.Logf("%d readers took %d snapshots while the sequence ran", readers, reads.Load())
	if reads.Load() < 1000 {
		t.Errorf(
			"only %d snapshots, which is too few to give the detector its chance; this row is only a guard while it reads hard",
			reads.Load(),
		)
	}
}
