package connect

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// A sender and a receiver joined by a delayed acknowledgement half, with the
// send window rule configurable, so the before and after are the same binary
// with one field changed.
type sendWindowHarness struct {
	sender        *Client
	receiverId    Id
	deliveredByte func() ByteCount
}

func newSendWindowHarness(
	t *testing.T,
	ctx context.Context,
	ackDelay time.Duration,
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

	pumpsDone := []chan struct{}{}
	// every frame in flight is owned by its own goroutine, so cleanup joins
	// them before draining: a frame still sleeping when the loop exits is a
	// leaked pool root, which the ownership assertion catches
	var framesInFlight sync.WaitGroup
	// per frame, concurrently: a pump that sleeps in its own loop is a serial
	// line and would bound the measurement rather than the window
	pump := func(from Route, to Route, delay time.Duration) {
		done := make(chan struct{})
		pumpsDone = append(pumpsDone, done)
		go func() {
			defer close(done)
			for {
				select {
				case transferFrameBytes := <-from:
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
	pump(senderOut, receiverIn, 0)
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
	return &sendWindowHarness{sender: sender, receiverId: receiverId}
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

// §32.5: the ceiling is a share of a budget rather than a per-sequence
// constant, because forty simultaneous downloaders at a per-sequence maximum
// would retain more than a provider has. A constant that works in a one-client
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
