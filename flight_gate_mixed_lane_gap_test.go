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
	"math/rand"
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

// mixedLaneOptions describes the two lanes. A lane has a latency and,
// optionally, a bandwidth: one frame per serialization interval. A lane
// with a bandwidth genuinely backs up when it is offered more than it
// carries, which is what a phone's uplink does and what puts a real queue
// in front of the sender's retransmit timer.
type mixedLaneOptions struct {
	fastLatency        time.Duration
	slowLatency        time.Duration
	fastSerialization  time.Duration
	slowSerialization  time.Duration
	replySerialization time.Duration
	blockFastReplies   bool
	directLaneDisabled bool
	deferTimeoutResend bool
	// fastDropFraction drops that share of the direct lane's frames, from a
	// seeded source so a run repeats. The relay never drops.
	fastDropFraction float64
	// graceDisabled removes the mixed-lane reordering grace, the shape the
	// merged tree had.
	graceDisabled bool
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
	return newMixedLaneHarnessWithOptions(t, mixedLaneOptions{
		fastLatency:      fastDelay,
		slowLatency:      slowDelay,
		blockFastReplies: blockFastReplies,
	})
}

func newMixedLaneHarnessWithOptions(
	t testing.TB,
	options mixedLaneOptions,
) *mixedLaneGapHarness {
	t.Helper()
	fastDelay := options.fastLatency
	slowDelay := options.slowLatency
	blockFastReplies := options.blockFastReplies
	ctx, cancel := context.WithCancel(context.Background())
	newSettings := func() *ClientSettings {
		settings := DefaultClientSettings()
		settings.EncryptionSettings.Mode = EncryptionModeOff
		settings.SendBufferSettings.AckTimeout = 120 * time.Second
		settings.SendBufferSettings.IdleTimeout = 120 * time.Second
		settings.ReceiveBufferSettings.GapTimeout = 120 * time.Second
		settings.ReceiveBufferSettings.IdleTimeout = 120 * time.Second
		settings.SendBufferSettings.DeferTimeoutResendWhileCumulativeProgress =
			options.deferTimeoutResend
		settings.SendBufferSettings.MixedLaneAckReorderGrace = !options.graceDisabled
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

	if !options.directLaneDisabled {
		harness.sender.RouteManager().UpdateTransportWithProperties(
			NewSendGatewayTransportWithType(TransportTypeP2p),
			[]Route{senderOutFast},
			TransferCarrierProperties{Unreliable: true},
		)
	}
	harness.sender.RouteManager().UpdateTransport(
		NewSendGatewayTransportWithType(TransportTypeH1),
		[]Route{senderOutSlow},
	)
	harness.sender.RouteManager().UpdateTransport(
		NewReceiveGatewayTransport(),
		[]Route{senderIn},
	)
	if !options.directLaneDisabled {
		harness.receiver.RouteManager().UpdateTransportWithProperties(
			NewReceiveGatewayTransportWithType(TransportTypeP2p),
			[]Route{receiverInFast},
			TransferCarrierProperties{Unreliable: true},
		)
	}
	harness.receiver.RouteManager().UpdateTransportWithProperties(
		NewReceiveGatewayTransportWithType(TransportTypeH1),
		[]Route{receiverInSlow},
		TransferCarrierProperties{},
	)
	if !options.directLaneDisabled {
		harness.receiver.RouteManager().UpdateTransportWithProperties(
			NewSendGatewayTransportWithType(TransportTypeP2p),
			[]Route{receiverOutFast},
			TransferCarrierProperties{Unreliable: true},
		)
	}
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

	// One pipeline per physical lane: a latency, and optionally a bandwidth
	// of one frame per serialization interval. Order within a lane is kept
	// and nothing is dropped.
	dropRandom := rand.New(rand.NewSource(20260911))
	var dropLock sync.Mutex
	forward := func(
		from Route,
		to Route,
		latency time.Duration,
		serialization time.Duration,
		dropFraction float64,
	) {
		deliver := func(b []byte) {
			if 0 < dropFraction {
				dropLock.Lock()
				dropIt := dropRandom.Float64() < dropFraction
				dropLock.Unlock()
				if dropIt {
					MessagePoolReturn(b)
					return
				}
			}
			timer := time.NewTimer(latency)
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
		}
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
					if serialization <= 0 {
						// unpaced: the lane has latency but no queue of its own
						harness.forwarders.Add(1)
						go func(b []byte) {
							defer harness.forwarders.Done()
							deliver(b)
						}(transferFrameBytes)
						continue
					}
					timer := time.NewTimer(serialization)
					select {
					case <-ctx.Done():
						timer.Stop()
						MessagePoolReturn(transferFrameBytes)
						return
					case <-timer.C:
					}
					timer.Stop()
					harness.forwarders.Add(1)
					go func(b []byte) {
						defer harness.forwarders.Done()
						deliver(b)
					}(transferFrameBytes)
				}
			}
		}()
	}
	forward(senderOutFast, receiverInFast, fastDelay, options.fastSerialization, options.fastDropFraction)
	forward(senderOutSlow, receiverInSlow, slowDelay, options.slowSerialization, 0)
	forward(receiverOutFast, senderIn, fastDelay, options.replySerialization, options.fastDropFraction)
	forward(receiverOutSlow, senderIn, slowDelay, 0, 0)

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

// FLIGHTGATEFIX §15. The device runs show a retransmit storm that has
// nothing to do with the flight gate: thirteen to nineteen thousand timeout
// resends in three minutes on builds where the flight never waited once,
// including runs carried entirely by the reliable peer lane. A lane with a
// bandwidth reproduces it: the sender writes a window into a route that
// drains at link rate, so an item's acknowledgement cannot come back inside
// the retransmit timer that started when the item was queued, and the whole
// window is rewritten every interval.
func TestSingleReliableLaneQueueInflatedRttDoesNotStorm(t *testing.T) {
	if testing.Short() {
		t.Skip("single-lane retransmit storm reproduction")
	}
	const messageCount = 300
	measure := func(deferTimeoutResend bool) ClientSendRecoveryStatsSnapshot {
		harness := newMixedLaneHarnessWithOptions(t, mixedLaneOptions{
			slowLatency:        20 * time.Millisecond,
			slowSerialization:  12 * time.Millisecond,
			directLaneDisabled: true,
			deferTimeoutResend: deferTimeoutResend,
		})
		return harness.run(t, messageCount)
	}
	report := func(name string, stats ClientSendRecoveryStatsSnapshot) {
		t.Logf(
			"%s: initial=%d rto=%d deferred=%d recent-progress=%d gap=%d tail-probe=%d cumulative-probe=%d",
			name,
			stats.InitialWriteCount,
			stats.TimeoutResendWriteCount,
			stats.TimeoutResendDeferCount,
			stats.TimeoutResendWithRecentCumulativeProgress,
			stats.SelectiveGapWriteCount,
			stats.AckTailProbeWriteCount,
			stats.CumulativeProbeWriteCount,
		)
	}
	// the mechanism: without the defer the whole window is rewritten against
	// a lane that is still delivering, and every one of those timeouts fires
	// while the cumulative ack is advancing
	off := measure(false)
	report("defer off", off)
	if off.TimeoutResendWriteCount == 0 {
		t.Fatal("the harness no longer reproduces the retransmit storm")
	}
	// the tail of a run can time out after the last ack, so the claim is
	// that the storm is overwhelmingly against a live lane, not every one
	if live := 4 * off.TimeoutResendWithRecentCumulativeProgress; live < 3*off.TimeoutResendWriteCount {
		t.Fatalf(
			"only %d of %d timeout resends fired against a live cumulative ack, so the storm has another cause here",
			off.TimeoutResendWithRecentCumulativeProgress,
			off.TimeoutResendWriteCount,
		)
	}
	// the contract: the shipped default keeps the storm to a rounding error
	on := measure(true)
	report("defer on ", on)
	if bound := off.TimeoutResendWriteCount / 4; bound < on.TimeoutResendWriteCount {
		t.Fatalf(
			"the default settings still storm: %d timeout resends against %d with the defer off, over %d messages",
			on.TimeoutResendWriteCount,
			off.TimeoutResendWriteCount,
			messageCount,
		)
	}
	if on.SelectiveGapWriteCount > off.SelectiveGapWriteCount ||
		on.AckTailProbeWriteCount > off.AckTailProbeWriteCount {
		t.Fatalf("the defer moved recovery onto another mechanism: %+v against %+v", on, off)
	}
}

// The shipped defaults must carry the contract above, not only the
// explicitly enabled configuration.
func TestDefaultSendBufferSettingsDeferTimeoutResendWhileProgressing(t *testing.T) {
	settings := DefaultSendBufferSettings()
	if !settings.DeferTimeoutResendWhileCumulativeProgress {
		t.Fatal("the retransmit defer is off by default")
	}
	if settings.TimeoutResendDeferLimit <= 0 {
		t.Fatalf("the defer limit is %d, so a stalled lane would never resend", settings.TimeoutResendDeferLimit)
	}
}

// FLIGHTGATEFIX §16. The decisive campaign measured our tree behind the
// merged base on every lossy cell while ahead on every clean one, which is
// the signature of insurance that costs more than it saves once the packets
// are really gone. The property that must hold: on a direct lane that is
// dropping, the grace may not make the stream slower than having no grace
// at all.
func TestMixedLaneLossyDirectLaneGoodputIsNotWorseWithTheGrace(t *testing.T) {
	if testing.Short() {
		t.Skip("lossy mixed-lane goodput")
	}
	const messageCount = 400
	for _, dropFraction := range []float64{0.01, 0.03} {
		measure := func(graceDisabled bool) (time.Duration, ClientSendRecoveryStatsSnapshot) {
			harness := newMixedLaneHarnessWithOptions(t, mixedLaneOptions{
				fastLatency:        2 * time.Millisecond,
				slowLatency:        40 * time.Millisecond,
				fastDropFraction:   dropFraction,
				graceDisabled:      graceDisabled,
				deferTimeoutResend: true,
			})
			start := time.Now()
			stats := harness.run(t, messageCount)
			return time.Since(start), stats
		}
		withoutGrace, withoutStats := measure(true)
		withGrace, withStats := measure(false)
		t.Logf(
			"drop %.0f%%: without the grace %s (gap=%d rto=%d), with it %s (gap=%d rto=%d)",
			100*dropFraction,
			withoutGrace.Truncate(time.Millisecond), withoutStats.SelectiveGapWriteCount, withoutStats.TimeoutResendWriteCount,
			withGrace.Truncate(time.Millisecond), withStats.SelectiveGapWriteCount, withStats.TimeoutResendWriteCount,
		)
		// the grace may cost a little scheduling noise, not a regime change
		if tolerance := withoutGrace + withoutGrace/4; tolerance < withGrace {
			t.Fatalf(
				"at %.0f%% loss the grace made the stream slower: %s against %s without it, over %d messages",
				100*dropFraction, withGrace, withoutGrace, messageCount,
			)
		}
	}
}
