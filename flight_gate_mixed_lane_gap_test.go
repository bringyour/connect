package connect

// FLIGHTGATEFIX §14: the PERFVAR mixed route showed our tree raising
// selective-gap resends far above the merged PRs on clean-lan, where there
// is no loss at all. The only thing that differs between the two arms on a
// clean link is which lane each acknowledgement rides, so the rise must be
// reproducible in process from lane latency alone.
//
// The harness is two real Clients wired through four routes the test owns:
// a fast direct lane and a slow relay lane in each direction, no drops, no
// reordering of its own. The receiver's own code chooses the ack lane, so
// the reply policy is under test; the sender's scoreboard is what counts.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

type mixedLaneGapHarness struct {
	sender     *Client
	receiver   *Client
	senderId   Id
	receiverId Id
	received   chan int
	forwarders sync.WaitGroup
	ctx        context.Context
}

// newMixedLaneGapHarness connects a sender to a receiver over a fast
// unreliable lane and a slow reliable lane in both directions. When
// blockFastReplies is set the receiver's fast reply route is left full, the
// shape the merged blanket rule produced: every ack takes the relay.
func newMixedLaneGapHarness(
	t testing.TB,
	fastDelay time.Duration,
	slowDelay time.Duration,
	blockFastReplies bool,
) *mixedLaneGapHarness {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	newSettings := func() *ClientSettings {
		settings := DefaultClientSettings()
		settings.EncryptionSettings.Mode = EncryptionModeOff
		settings.SendBufferSettings.AckTimeout = 120 * time.Second
		settings.SendBufferSettings.IdleTimeout = 120 * time.Second
		settings.ReceiveBufferSettings.GapTimeout = 120 * time.Second
		settings.ReceiveBufferSettings.IdleTimeout = 120 * time.Second
		return settings
	}
	harness := &mixedLaneGapHarness{
		senderId:   NewId(),
		receiverId: NewId(),
		received:   make(chan int, 4096),
	}
	harness.ctx = ctx
	harness.sender = NewClient(ctx, harness.senderId, NewNoContractClientOob(), newSettings())
	harness.receiver = NewClient(ctx, harness.receiverId, NewNoContractClientOob(), newSettings())
	harness.sender.ContractManager().AddNoContractPeer(harness.receiverId)
	harness.receiver.ContractManager().AddNoContractPeer(harness.senderId)

	// sender out: the direct lane is a small bounded channel like the
	// production p2p route, so the flight overflow reaches the relay
	senderOutFast := make(Route, 4)
	senderOutSlow := make(Route, 64)
	senderIn := make(Route, 64)
	receiverInFast := make(Route, 64)
	receiverInSlow := make(Route, 64)
	receiverOutFast := make(Route, 4)
	receiverOutSlow := make(Route, 64)

	harness.sender.RouteManager().UpdateTransportWithProperties(
		NewSendGatewayTransportWithType(TransportTypeP2p),
		[]Route{senderOutFast},
		TransferCarrierProperties{Unreliable: true},
	)
	harness.sender.RouteManager().UpdateTransport(
		NewSendGatewayTransportWithType(TransportTypeH1),
		[]Route{senderOutSlow},
	)
	harness.sender.RouteManager().UpdateTransport(
		NewReceiveGatewayTransport(),
		[]Route{senderIn},
	)
	harness.receiver.RouteManager().UpdateTransportWithProperties(
		NewReceiveGatewayTransportWithType(TransportTypeP2p),
		[]Route{receiverInFast},
		TransferCarrierProperties{Unreliable: true},
	)
	harness.receiver.RouteManager().UpdateTransportWithProperties(
		NewReceiveGatewayTransportWithType(TransportTypeH1),
		[]Route{receiverInSlow},
		TransferCarrierProperties{},
	)
	harness.receiver.RouteManager().UpdateTransportWithProperties(
		NewSendGatewayTransportWithType(TransportTypeP2p),
		[]Route{receiverOutFast},
		TransferCarrierProperties{Unreliable: true},
	)
	harness.receiver.RouteManager().UpdateTransport(
		NewSendGatewayTransportWithType(TransportTypeH1),
		[]Route{receiverOutSlow},
	)
	if blockFastReplies {
		for len(receiverOutFast) < cap(receiverOutFast) {
			receiverOutFast <- nil
		}
	}

	harness.receiver.AddReceiveCallback(func(_ TransferPath, frames []*protocol.Frame, _ Peer) {
		select {
		case harness.received <- len(frames):
		default:
		}
	})
	harness.sender.AddReceiveCallback(func(TransferPath, []*protocol.Frame, Peer) {})

	// one forwarder per physical lane: a fixed latency, no loss, no drops
	forward := func(from Route, to Route, delay time.Duration) {
		harness.forwarders.Add(1)
		go func() {
			defer harness.forwarders.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case transferFrameBytes := <-from:
					if transferFrameBytes == nil {
						continue
					}
					harness.forwarders.Add(1)
					go func(b []byte) {
						defer harness.forwarders.Done()
						timer := time.NewTimer(delay)
						defer timer.Stop()
						select {
						case <-ctx.Done():
							MessagePoolReturn(b)
							return
						case <-timer.C:
						}
						select {
						case <-ctx.Done():
							MessagePoolReturn(b)
						case to <- b:
						}
					}(transferFrameBytes)
				}
			}
		}()
	}
	forward(senderOutFast, receiverInFast, fastDelay)
	forward(senderOutSlow, receiverInSlow, slowDelay)
	forward(receiverOutFast, senderIn, fastDelay)
	forward(receiverOutSlow, senderIn, slowDelay)

	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		if err := harness.sender.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close mixed-lane sender: %v", err)
		}
		if err := harness.receiver.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close mixed-lane receiver: %v", err)
		}
		harness.forwarders.Wait()
		for _, route := range []Route{
			senderOutFast, senderOutSlow, senderIn,
			receiverInFast, receiverInSlow, receiverOutFast, receiverOutSlow,
		} {
			for {
				select {
				case message := <-route:
					if message != nil {
						MessagePoolReturn(message)
					}
				default:
					goto next
				}
			}
		next:
		}
	})
	return harness
}

// startReverseLoad models the uplink of a download: the receiver sends its
// own small frames back over the same routes its acknowledgements use, so
// the bounded direct lane is contended and the reply lane choice is made
// under real pressure rather than on an idle channel.
func (self *mixedLaneGapHarness) startReverseLoad(interval time.Duration) {
	self.forwarders.Add(1)
	go func() {
		defer self.forwarders.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		index := 0
		for {
			select {
			case <-self.ctx.Done():
				return
			case <-ticker.C:
			}
			frame, err := ToFrame(
				&protocol.SimpleMessage{Content: fmt.Sprintf("uplink-%d", index)},
				DefaultProtocolVersion,
			)
			if err != nil {
				return
			}
			index += 1
			if !self.receiver.SendWithTimeout(frame, self.senderId, nil, time.Second) {
				MessagePoolReturn(frame.MessageBytes)
			}
		}
	}()
}

// run sends messageCount application frames and waits for the receiver to
// deliver every one, then returns the sender's recovery counters.
func (self *mixedLaneGapHarness) run(
	t testing.TB,
	messageCount int,
) ClientSendRecoveryStatsSnapshot {
	t.Helper()
	content := ""
	for len(content) < 900 {
		content += "mixed-lane-gap-"
	}
	for index := 0; index < messageCount; index += 1 {
		frame, err := ToFrame(
			&protocol.SimpleMessage{Content: fmt.Sprintf("%s%d", content, index)},
			DefaultProtocolVersion,
		)
		if err != nil {
			t.Fatal(err)
		}
		if !self.sender.SendWithTimeout(frame, self.receiverId, nil, 30*time.Second) {
			MessagePoolReturn(frame.MessageBytes)
			t.Fatalf("message %d was not admitted", index)
		}
	}
	delivered := 0
	deadline := time.After(120 * time.Second)
	for delivered < messageCount {
		select {
		case frames := <-self.received:
			delivered += frames
		case <-deadline:
			t.Fatalf("only %d of %d messages were delivered", delivered, messageCount)
		}
	}
	// let the last acknowledgements and any scheduled recovery settle
	time.Sleep(500 * time.Millisecond)
	return self.sender.SendRecoveryStats()
}

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
