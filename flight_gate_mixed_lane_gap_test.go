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
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// laneBurstLoss is the campaign's two-state loss model (server/connect/perfvar
// mixed-direct-burst-loss): a good state with a small background loss that
// enters a bad state, where most packets are lost, and leaves it again a few
// packets later. Bursts arrive about every hundred packets, so the model is
// clustered loss, not long clean stretches between bursts.
type laneBurstLoss struct {
	goodToBad float64
	badToGood float64
	goodLoss  float64
	badLoss   float64
}

// campaignBurstLoss are the campaign's parameters verbatim.
var campaignBurstLoss = laneBurstLoss{
	goodToBad: 0.01,
	badToGood: 0.35,
	goodLoss:  0.002,
	badLoss:   0.65,
}

// laneLossProcess is a seeded per-packet loss process, independent with one
// probability or the two-state burst chain, so a classification test is
// deterministic and repeatable.
type laneLossProcess struct {
	random      *rand.Rand
	independent float64
	burst       *laneBurstLoss
	bad         bool
}

func newLaneLossProcess(seed int64, independent float64, burst *laneBurstLoss) *laneLossProcess {
	return &laneLossProcess{
		random:      rand.New(rand.NewSource(seed)),
		independent: independent,
		burst:       burst,
	}
}

// lost reports whether the next packet is lost.
func (self *laneLossProcess) lost() bool {
	if self.burst == nil {
		return self.random.Float64() < self.independent
	}
	if self.bad {
		if self.random.Float64() < self.burst.badToGood {
			self.bad = false
		}
	} else if self.random.Float64() < self.burst.goodToBad {
		self.bad = true
	}
	probability := self.burst.goodLoss
	if self.bad {
		probability = self.burst.badLoss
	}
	return self.random.Float64() < probability
}

type mixedLaneGapHarness struct {
	sender     *Client
	receiver   *Client
	senderId   Id
	receiverId Id
	received   chan int
	forwarders sync.WaitGroup
	ctx        context.Context
	// deliveries records when each frame reached the receiver, so a run can
	// be read as a rate over time rather than only as a total.
	deliveryLock  sync.Mutex
	deliveryTimes []time.Time
	// per-lane carriage, so a run can say which lane actually carried the
	// payload and how much of it the lane lost
	fastCarried atomic.Uint64
	slowCarried atomic.Uint64
	fastDropped atomic.Uint64
	// laneRoutes are the seven routes by name, so a stalled run can say
	// whether a route is full, which is the harness wedging, or empty,
	// which is the transfer layer stalling.
	laneRoutes []mixedLaneRoute
	// runDeadline bounds a multi-flow run; past it the run is reported as
	// stalled rather than failing the test, so a stall can be compared.
	runDeadline time.Duration
}

type mixedLaneRoute struct {
	name  string
	route Route
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
	// fastBurstLoss replaces the independent drop on the direct lane with a
	// two-state chain, each direction with its own state, the campaign's
	// burst-loss shape.
	fastBurstLoss *laneBurstLoss
	// slowStepAfter and slowSerializationAfterStep model the campaign's
	// mixed-relay-queue-inflation schedule: at slowStepAfter the relay's
	// drain rate drops to slowSerializationAfterStep and stays there. The
	// queue in front of it is slowQueueFrames deep, so the sender can push
	// far past what the lane drains before any write blocks, which is the
	// condition the accidental throttle of merged's rewrites hid
	// (FLIGHTGATEFIX §21).
	slowStepAfter              time.Duration
	slowSerializationAfterStep time.Duration
	slowQueueFrames            int
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
		return settings
	}
	harness := &mixedLaneGapHarness{
		senderId:    NewId(),
		receiverId:  NewId(),
		received:    make(chan int, 4096),
		runDeadline: 180 * time.Second,
	}
	harness.ctx = ctx
	harness.sender = NewClient(ctx, harness.senderId, NewNoContractClientOob(), newSettings())
	harness.receiver = NewClient(ctx, harness.receiverId, NewNoContractClientOob(), newSettings())
	harness.sender.ContractManager().AddNoContractPeer(harness.receiverId)
	harness.receiver.ContractManager().AddNoContractPeer(harness.senderId)

	// sender out: the direct lane is a small bounded channel like the
	// production p2p route, so the flight overflow reaches the relay
	senderOutFast := make(Route, 4)
	slowQueueFrames := options.slowQueueFrames
	if slowQueueFrames <= 0 {
		slowQueueFrames = 64
	}
	senderOutSlow := make(Route, slowQueueFrames)
	senderIn := make(Route, 64)
	receiverInFast := make(Route, 64)
	receiverInSlow := make(Route, 64)
	receiverOutFast := make(Route, 4)
	receiverOutSlow := make(Route, 64)
	harness.laneRoutes = []mixedLaneRoute{
		{"senderOutFast", senderOutFast},
		{"senderOutSlow", senderOutSlow},
		{"senderIn", senderIn},
		{"receiverInFast", receiverInFast},
		{"receiverInSlow", receiverInSlow},
		{"receiverOutFast", receiverOutFast},
		{"receiverOutSlow", receiverOutSlow},
	}

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
		now := time.Now()
		harness.deliveryLock.Lock()
		for range frames {
			harness.deliveryTimes = append(harness.deliveryTimes, now)
		}
		harness.deliveryLock.Unlock()
		select {
		case harness.received <- len(frames):
		default:
		}
	})
	harness.sender.AddReceiveCallback(func(TransferPath, []*protocol.Frame, Peer) {})

	// One pipeline per physical lane: a latency, and optionally a bandwidth
	// of one frame per serialization interval. Order within a lane is kept
	// and nothing is dropped.
	var fastLoss, fastReplyLoss *laneLossProcess
	if options.fastBurstLoss != nil {
		fastLoss = newLaneLossProcess(20260911, 0, options.fastBurstLoss)
		fastReplyLoss = newLaneLossProcess(20260912, 0, options.fastBurstLoss)
	} else if 0 < options.fastDropFraction {
		fastLoss = newLaneLossProcess(20260911, options.fastDropFraction, nil)
		fastReplyLoss = newLaneLossProcess(20260912, options.fastDropFraction, nil)
	}
	var dropLock sync.Mutex
	// the relay's drain rate steps down once, at a known time
	harnessStart := time.Now()
	slowSerializationNow := func() time.Duration {
		if 0 < options.slowStepAfter && 0 < options.slowSerializationAfterStep &&
			options.slowStepAfter <= time.Since(harnessStart) {
			return options.slowSerializationAfterStep
		}
		return options.slowSerialization
	}
	forward := func(
		from Route,
		to Route,
		latency time.Duration,
		serialization time.Duration,
		loss *laneLossProcess,
		carried *atomic.Uint64,
	) {
		deliver := func(b []byte) {
			if loss != nil {
				dropLock.Lock()
				dropIt := loss.lost()
				dropLock.Unlock()
				if dropIt {
					harness.fastDropped.Add(1)
					MessagePoolReturn(b)
					return
				}
			}
			if carried != nil {
				carried.Add(1)
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
					pace := serialization
					if stepped := slowSerializationNow(); to == receiverInSlow && 0 < stepped {
						pace = stepped
					}
					timer := time.NewTimer(pace)
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
	forward(senderOutFast, receiverInFast, fastDelay, options.fastSerialization, fastLoss, &harness.fastCarried)
	forward(senderOutSlow, receiverInSlow, slowDelay, options.slowSerialization, nil, &harness.slowCarried)
	forward(receiverOutFast, senderIn, fastDelay, options.replySerialization, fastReplyLoss, nil)
	forward(receiverOutSlow, senderIn, slowDelay, 0, nil, nil)

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

// FLIGHTGATEFIX §19.6 pre-flight. A lossy direct lane at the campaign's
// two loss rates must still carry the payload to completion, and the run
// records what it cost. The grace is no longer a setting to compare
// against: D3 deleted MixedLaneAckReorderGrace because the deferral is a
// sound rule rather than optional insurance, so this is a single arm.
