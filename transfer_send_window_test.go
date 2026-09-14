package connect

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// What these rows do and do not claim (THROUGHPUTFIX §37.10 steps one and two).
//
// None of them claims the window bounds memory or latency for TCP carried
// through the tunnel. That claim would pass for the wrong reason and would keep
// passing with the rule deleted: the client tunnel moderates its own receive
// window from bytes copied per round trip, so a slow carrier closes it, the
// origin backs off, and the transfer layer is never handed more than the path
// can carry. Measured in the slow-drain cell: about 2.5 ms of added delay on
// every arm against predictions of hundreds, and a peak send queue of 20 KiB
// whether the window was 2, 3.6 or 16 MiB. A window cannot cost what it is
// never given.
//
// The traffic that has no such governor is the datagram return path, which
// admits non-blocking at every stage and drops beyond: per-shard return
// channels bounded by item count, a zero write timeout for non-TCP items, a
// pack call that refuses at timeout zero, and a retry path covering only TCP
// socket items. A source that offers regardless of admission fills whatever
// window it is given. And because a destination's traffic rides one ordered
// sequence, the queue it builds is the delay an unrelated TCP flow to that same
// client waits behind. So:
//
//   - the delivery term is the only latency bound in the design, and it is what
//     keeps a datagram source from inflating a co-resident flow's latency;
//   - the advertisement reaches datagram traffic too, but the job it does there
//     is the receiver's memory and the retransmission a drop costs, not latency;
//   - the ceiling is a bound on what one provider retains across many senders.
//
// Each row below says which of the three it is asserting.

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
	// How many frames the carrier route holds. A fixture's own wire capacity
	// can absorb the backpressure a cell is measuring, which is one of the
	// three instrument faults §37.14 found, so every cell that reads a queue
	// records it and checks the wire is not the binding constraint.
	wireFrameCapacity int
}

// sets the receiver's hold, which is what it advertises less what it holds
func (self *sendWindowHarness) receiveHold(byteCount ByteCount) {
	self.receiver.settings.ReceiveBufferSettings.ReceiveQueueMaxByteCount = byteCount
}

func newSendWindowHarness(
	t *testing.T,
	ctx context.Context,
	ackDelay time.Duration,
	configure func(*SendBufferSettings),
) *sendWindowHarness {
	return newRateLimitedSendWindowHarness(t, ctx, ackDelay, 0, configure)
}

// The same harness with the data half paced at a byte rate, which is what a
// bottleneck link is: frames depart in order at the link's rate, so a sender
// whose window exceeds rate times round trip leaves the excess standing as
// queue. Serialisation is the point here rather than an artefact.
func newRateLimitedSendWindowHarness(
	t *testing.T,
	ctx context.Context,
	ackDelay time.Duration,
	bytesPerSecond ByteCount,
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

	senderOut := make(Route, 1024)
	senderIn := make(Route, 1024)
	receiverIn := make(Route, 1024)
	receiverOut := make(Route, 1024)
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
	ratePump := func(from Route, to Route, bytesPerSecond ByteCount) {
		done := make(chan struct{})
		pumpsDone = append(pumpsDone, done)
		go func() {
			defer close(done)
			departure := time.Now()
			for {
				select {
				case transferFrameBytes := <-from:
					serviceTime := time.Duration(
						int64(len(transferFrameBytes)) * int64(time.Second) / int64(bytesPerSecond),
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
		ratePump(senderOut, receiverIn, bytesPerSecond)
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

// THROUGHPUTFIX §37.13, step one's acceptance, claimed for the traffic the
// rule is actually load-bearing for.
//
// What this row must not claim. A test asserting that the window bounds memory
// or latency for TCP through the tunnel would pass for the wrong reason and
// would keep passing with the rule removed entirely: the client tunnel's
// receive moderation sizes its window from bytes copied per round trip, so a
// slow carrier closes it, the origin's TCP backs off, and the transfer layer
// is never handed more than the path can carry. The slow-drain cell measured
// that at about 2.5 ms of added delay on every arm against predictions of
// hundreds of milliseconds, with peak send queue at 20 KiB whether the window
// was 2, 3.6 or 16 MiB. A window cannot cost what it is never given.
//
// What it does claim. Nothing backs a datagram source off: the UDP return path
// admits non-blocking at every stage and drops beyond — per-shard return
// channels bounded by item count, a zero write timeout for non-TCP items, the
// pack call refusing at timeout zero, and a retry path that covers only TCP
// socket items. So a source that offers regardless of admission fills the
// window, and the window over the drain rate is the latency every flow sharing
// that per-destination sequence must wait behind, including an unrelated TCP
// flow to the same client. The delivery term is what drops the excess at
// admission, and that is the only latency bound in the design.
//
// So the offerer here is the shape that cannot be backed off: it offers as
// fast as it is admitted and discards what is refused, exactly as the datagram
// return path does. The measurement is the standing queue any co-resident flow
// inherits.
//
// Two corrections to the design's stated reading, both measured here.
//
// The reading cannot be `Rtt.Mean − Rtt.Min`. On a link bound below the sender
// every sample queues, so the minimum carries the standing queue too and the
// difference measures only the variation in it. Measured: the constant arm's
// mean is 1.264 s and its minimum 1.231 s, a difference of 33 ms, while its
// actual standing queue is about 1.2 s. The reading that works is the minimum
// less the propagation the link imposes. This is the same trap as a serial
// delay element raising a measured floor.
//
// And the sized arm does not land at one propagation round trip. The minimum
// is the right multiplier relative to the mean, but on a path bound below the
// sender even the minimum is inflated by the queue the window creates, so the
// fixed point is not twice the bandwidth-delay product of the propagation
// delay: 129 to 178 ms of standing queue against a 25 ms propagation, about
// five to seven round trips rather than one. What holds, and what this
// asserts, is the comparison the step exists for.
func TestSizedWindowBoundsTheQueueADatagramSourceImposesOnASharedSequence(t *testing.T) {
	assertMessagePoolOwnership(t)

	// a slow last mile: 20 Mb/s at a 25 ms propagation round trip
	const bytesPerSecond = ByteCount(20 * 1000 * 1000 / 8)
	const propagation = 25 * time.Millisecond
	const constantWindow = ByteCount(2 * 1024 * 1024)
	const ceiling = ByteCount(16 * 1024 * 1024)
	const payloadByteCount = 4 * 1024
	const offerWindow = 3 * time.Second

	wireFrameCapacity := 0
	standingQueue := func(sized bool) (time.Duration, RttEstimate, SendWindowEstimate) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		harness := newRateLimitedSendWindowHarness(t, ctx, propagation, bytesPerSecond,
			func(settings *SendBufferSettings) {
				settings.ResendQueueMaxByteCount = constantWindow
				if sized {
					settings.DeliverySizedWindowScale = 2
					settings.DeliverySizedWindowCeilingByteCount = ceiling
					settings.ResendQueueBudget = NewTransferMemoryBudget(ceiling)
				}
			})
		harness.receiveHold(ceiling)
		wireFrameCapacity = harness.wireFrameCapacity
		// the source that cannot be backed off: offers as fast as it is
		// admitted and discards refusals
		harness.offer(t, payloadByteCount, offerWindow)
		stats := harness.sender.DestinationSendStats(harness.receiverId)
		// the standing queue in time: what the round trip carries above the
		// propagation the link imposes, which is what a co-resident flow waits
		return max(0, stats.Rtt.Min-propagation), stats.Rtt, stats.SendWindow
	}

	constantQueue, constantRtt, constantEstimate := standingQueue(false)
	sizedQueue, sizedRtt, sizedEstimate := standingQueue(true)

	wireByteCapacity := ByteCount(wireFrameCapacity * payloadByteCount)
	t.Logf(
		"constant %d: standing queue %s (min %s, mean %s); sized %d: standing queue %s (min %s, mean %s), from %d bytes over %s; wire holds %d frames, at least %d bytes",
		constantEstimate.Window, constantQueue, constantRtt.Min, constantRtt.Mean,
		sizedEstimate.Window, sizedQueue, sizedRtt.Min, sizedRtt.Mean,
		sizedEstimate.DeliveredByteCount, sizedEstimate.Interval,
		wireFrameCapacity, wireByteCapacity,
	)

	if wireByteCapacity <= constantWindow {
		t.Fatalf(
			"the carrier holds at least %d bytes against a %d byte window, so the wire rather than the window would be the binding queue and this cell would measure the fixture",
			wireByteCapacity,
			constantWindow,
		)
	}
	if !sizedEstimate.Sized {
		t.Fatalf("the window rule did not engage, so this cell does not test it: %+v", sizedEstimate)
	}
	if constantEstimate.Window <= sizedEstimate.Window {
		t.Errorf(
			"the sized window is %d against the %d byte constant; on a link slower than the sender the rule is supposed to size below the constant, not above it",
			sizedEstimate.Window,
			constantEstimate.Window,
		)
	}
	// measured at about a seventh to a ninth; a factor of three is well inside
	// that and well outside the run-to-run spread of a shared runner
	if constantQueue < 3*sizedQueue {
		t.Errorf(
			"a source that cannot be backed off left %s of standing queue with the sized window against %s with the constant; every flow sharing this destination's sequence waits behind that, so the delivery term is what keeps a datagram source from inflating an unrelated flow's latency",
			sizedQueue,
			constantQueue,
		)
	}
}
