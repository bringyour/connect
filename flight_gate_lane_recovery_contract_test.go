package connect

// FLIGHTGATEFIX §27.4. The root-cause contract for the lane-recovery
// metrics, in the seven-row shape. Each row states a regime and a
// behaviour, is built from API that predates this program, and is written
// so it runs unchanged against merged 89e1633 as well as against this
// tree. Two rows are ones merged fails by construction, buying ordered
// stream progress with duplicates, and one is the trade the other way,
// where merged is faster and the lane rule pays.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// laneRecoveryArm is one tree's configuration. A tree without this
// program's settings offers one arm and the file records what that tree
// does, which is how merged's column is filled.
type laneRecoveryArm struct {
	name      string
	configure func(*SendBufferSettings)
	// readsLanes is whether this arm reads a reliable lane's own
	// acknowledgements. A tree without the rule reports false for its one
	// arm, and the rows then record what that tree does rather than
	// asserting the candidate's behaviour of it.
	readsLanes bool
}

// laneRecoveryArmsForTree is replaced by a tree that has the candidate
// settings. Left alone it reports the tree's own defaults under one name.
var laneRecoveryArmsForTree = func() []laneRecoveryArm {
	return []laneRecoveryArm{{name: "this tree"}}
}

func laneRecoveryArms() []laneRecoveryArm { return laneRecoveryArmsForTree() }

// laneRecoveryRecordAcksForTree lets a tree that keeps a per-route
// acknowledgement history populate it; a tree without one does nothing,
// which is merged's case.
var laneRecoveryRecordAcksForTree = func(*SendSequence, []*sendItem) {}

// laneRecoveryRecordSendsForTree lets a tree that tracks what each route
// has carried populate it; a tree without one does nothing.
var laneRecoveryRecordSendsForTree = func(*SendSequence, []*sendItem) {}

// metricDeferredGapForTree reports the deferred-item gap split where the
// tree keeps one; a tree with no deferral reports unsupported.
var metricDeferredGapForTree = func(
	ClientSendRecoveryStatsSnapshot,
) (uint64, uint64, bool, bool) {
	return 0, 0, false, false
}

// metricRouteGenerationsForTree reports the route generation change count
// where the tree keeps one.
var metricRouteGenerationsForTree = func(ClientSendRecoveryStatsSnapshot) (uint64, bool) {
	return 0, false
}

func laneRecoveryRecordSends(sequence *SendSequence, items []*sendItem) {
	laneRecoveryRecordSendsForTree(sequence, items)
}

func laneRecoveryRecordAcks(sequence *SendSequence, items []*sendItem) {
	laneRecoveryRecordAcksForTree(sequence, items)
}

// laneRecoveryReadsLanesForTree reports whether this arm reads a reliable
// lane's own acknowledgements. A tree without the rule reports false, and
// the rows then record what that tree does rather than asserting the
// candidate's behaviour of it.
var laneRecoveryReadsLanesForTree = func(*SendSequence) bool { return false }

// laneRecoveryLongestGapForTree reports the longest stretch a reliable lane
// went without acknowledging, where the tree exports it. Without it the
// rows cannot check that a stall actually bit, and say so.
var laneRecoveryLongestGapForTree = func(ClientSendRecoveryStatsSnapshot) (time.Duration, bool) {
	return 0, false
}

// laneRecoveryDetailForTree adds the tree's own recovery counters to a row's
// log line.
var laneRecoveryDetailForTree = func(ClientSendRecoveryStatsSnapshot) string { return "" }

// laneRecoveryTimerVerdictForTree names what a tree's timer would make of
// this item's firing. A tree without the lane rule reports "as today".
var laneRecoveryTimerVerdictForTree = func(*SendSequence, *sendItem) string { return "as today" }

func laneRecoveryTimerVerdict(sequence *SendSequence, item *sendItem) string {
	return laneRecoveryTimerVerdictForTree(sequence, item)
}

// laneRecoveryRidesAndProbes reports the ride and probe counts where the
// tree keeps them.
var laneRecoveryRidesAndProbesForTree = func(ClientSendRecoveryStatsSnapshot) (uint64, uint64) {
	return 0, 0
}

func laneRecoveryRidesAndProbes(stats ClientSendRecoveryStatsSnapshot) (uint64, uint64) {
	return laneRecoveryRidesAndProbesForTree(stats)
}

func laneRecoveryReadsLanes(sequence *SendSequence) bool {
	return laneRecoveryReadsLanesForTree(sequence)
}

// ---- a single reliable lane between two real clients, with a stall ----

type laneRecoveryLink struct {
	sender     *Client
	receiver   *Client
	receiverId Id
	received   chan int
	// deliveries records when each frame reached the receiver, so a row can
	// ask what was delivered during a stall rather than only in total.
	deliveryLock  sync.Mutex
	deliveryTimes []time.Time
	start         time.Time
}

// newLaneRecoveryLink connects a sender to a receiver over one reliable
// route in each direction, with a forwarder between them so the test owns
// the lane: one frame per serialization, a fixed latency, and one stretch
// where the lane carries nothing, which is the in-flight stall the
// campaign's gap export measures.
func newLaneRecoveryLink(
	t testing.TB,
	latency time.Duration,
	serialization time.Duration,
	stallAfter time.Duration,
	stallFor time.Duration,
	queueFrames int,
	stepAfter time.Duration,
	stepSerialization time.Duration,
	directFlightMessageLimit int,
	configure func(*SendBufferSettings),
) *laneRecoveryLink {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	newSettings := func() *ClientSettings {
		settings := DefaultClientSettings()
		settings.EncryptionSettings.Mode = EncryptionModeOff
		settings.SendBufferSettings.AckTimeout = 120 * time.Second
		settings.SendBufferSettings.IdleTimeout = 120 * time.Second
		settings.ReceiveBufferSettings.GapTimeout = 120 * time.Second
		settings.ReceiveBufferSettings.IdleTimeout = 120 * time.Second
		if configure != nil {
			configure(settings.SendBufferSettings)
		}
		return settings
	}
	link := &laneRecoveryLink{receiverId: NewId(), received: make(chan int, 8192)}
	senderId := NewId()
	link.sender = NewClient(ctx, senderId, NewNoContractClientOob(), newSettings())
	link.receiver = NewClient(ctx, link.receiverId, NewNoContractClientOob(), newSettings())
	link.sender.ContractManager().AddNoContractPeer(link.receiverId)
	link.receiver.ContractManager().AddNoContractPeer(senderId)

	senderOut := make(Route, queueFrames)
	receiverIn := make(Route, 256)
	receiverOut := make(Route, 256)
	senderIn := make(Route, 256)
	link.sender.RouteManager().UpdateTransport(
		NewSendGatewayTransportWithType(TransportTypeH1), []Route{senderOut})
	link.sender.RouteManager().UpdateTransport(
		NewReceiveGatewayTransportWithType(TransportTypeH1), []Route{senderIn})
	link.receiver.RouteManager().UpdateTransport(
		NewSendGatewayTransportWithType(TransportTypeH1), []Route{receiverOut})
	link.receiver.RouteManager().UpdateTransport(
		NewReceiveGatewayTransportWithType(TransportTypeH1), []Route{receiverIn})
	// an optional direct lane beside the relay, so a row can ask what a
	// healthy unreliable carrier delivers while the relay is stalled
	var directOut, directIn Route
	if 0 < directFlightMessageLimit {
		directOut = make(Route, 64)
		directIn = make(Route, 256)
		properties := TransferCarrierProperties{Unreliable: true}
		properties.unreliableFlightMessageLimit = directFlightMessageLimit
		link.sender.RouteManager().UpdateTransportWithProperties(
			NewSendGatewayTransportWithType(TransportTypeP2p),
			[]Route{directOut},
			properties,
		)
		link.receiver.RouteManager().UpdateTransportWithProperties(
			NewReceiveGatewayTransportWithType(TransportTypeP2p),
			[]Route{directIn},
			properties,
		)
	}
	link.receiver.AddReceiveCallback(func(_ TransferPath, frames []*protocol.Frame, _ Peer) {
		now := time.Now()
		link.deliveryLock.Lock()
		for range frames {
			link.deliveryTimes = append(link.deliveryTimes, now)
		}
		link.deliveryLock.Unlock()
		select {
		case link.received <- len(frames):
		default:
		}
	})

	start := time.Now()
	link.start = start
	var forwarders sync.WaitGroup
	forward := func(from Route, to Route, paced bool) {
		forwarders.Add(1)
		go func() {
			defer forwarders.Done()
			for {
				var frameBytes []byte
				select {
				case <-ctx.Done():
					return
				case frameBytes = <-from:
				}
				if frameBytes == nil {
					continue
				}
				if paced {
					pace := serialization
					if 0 < stepSerialization && stepAfter <= time.Since(start) {
						pace = stepSerialization
					}
					if 0 < stallFor {
						since := time.Since(start)
						if stallAfter <= since && since < stallAfter+stallFor {
							pace += stallAfter + stallFor - since
						}
					}
					select {
					case <-ctx.Done():
						MessagePoolReturn(frameBytes)
						return
					case <-time.After(pace):
					}
				}
				forwarders.Add(1)
				go func(b []byte) {
					defer forwarders.Done()
					select {
					case <-ctx.Done():
						MessagePoolReturn(b)
						return
					case <-time.After(latency):
					}
					select {
					case <-ctx.Done():
						MessagePoolReturn(b)
					case to <- b:
					}
				}(frameBytes)
			}
		}()
	}
	forward(senderOut, receiverIn, true)
	forward(receiverOut, senderIn, false)
	if directOut != nil {
		forward(directOut, directIn, false)
	}

	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		link.sender.CloseAndWait(closeCtx)
		link.receiver.CloseAndWait(closeCtx)
		forwarders.Wait()
		for _, route := range []Route{senderOut, receiverIn, receiverOut, senderIn, directOut, directIn} {
			if route == nil {
				continue
			}
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
	return link
}

// ---- hand-built scoreboards, for the rows about the gap rule ----

// laneRecoveryScoreboard is a send sequence with itemCount items on a
// limited mixed-lane policy, built from the helper that predates this
// program.
func laneRecoveryScoreboard(
	t testing.TB,
	itemCount int,
	configure func(*SendBufferSettings),
) (*SendSequence, []*sendItem, time.Time) {
	t.Helper()
	sendTime := time.Unix(1_700_000_000, 0)
	sequence, items := newSelectiveAckRecoveryTestSequence(itemCount, sendTime)
	sequence.client = &Client{}
	if configure != nil {
		configure(sequence.sendBufferSettings)
	}
	sequence.flightController = newSendFlightController(sequence.sendBufferSettings)
	sequence.flightController.applyPolicy(transferFlightPolicySnapshot{
		generation:             1,
		limited:                true,
		reliableRouteAvailable: true,
	})
	sequence.rttWindow.CloseSendTime(uint64(time.Now().Add(-300 * time.Millisecond).UnixMilli()))
	return sequence, items, sendTime
}

// Row 4. A single reliable lane, one endpoint drop, three later
// acknowledgements from that same lane: recovered in one gap round. Every
// tree holds this, and it is the row that proves the others are not
// suppressing real recovery.
func TestLaneRecoveryRow4EndpointDropIsRecovered(t *testing.T) {
	for _, arm := range laneRecoveryArms() {
		sequence, items, sendTime := laneRecoveryScoreboard(t, 8, arm.configure)
		relay := make(Route, 4)
		hole := items[0]
		hole.reliableCarrierObserved = true
		hole.carrierRoute = relay
		for index := 1; index < len(items); index += 1 {
			items[index].selectiveAcked = true
			items[index].carrierRoute = relay
		}
		// the lane's own acknowledgement history, where the tree keeps one
		laneRecoveryRecordAcks(sequence, items[1:])
		sequence.scheduleSelectiveAckRecovery(sendTime.Add(10 * time.Second))
		if !hole.selectiveGapRecovered {
			t.Errorf("%s: row 4: an endpoint drop with three later same-lane acknowledgements was "+
				"not recovered", arm.name)
		}
	}
}

// Row 5. A mixed route: a relay item overtaken by three direct-lane
// acknowledgements while the relay is draining. It must not be written,
// because the relay retransmits below Transfer and those replies say
// nothing about its leg. merged and the landed tree fail this by
// construction: F11b's grace expires and the scoreboard writes.
func TestLaneRecoveryRow5RelayItemOvertakenByDirectAcks(t *testing.T) {
	for _, arm := range laneRecoveryArms() {
		sequence, items, sendTime := laneRecoveryScoreboard(t, 8, arm.configure)
		relay := make(Route, 4)
		direct := make(Route, 4)
		hole := items[0]
		hole.reliableCarrierObserved = true
		hole.carrierRoute = relay
		for index := 1; index < len(items); index += 1 {
			items[index].selectiveAcked = true
			items[index].carrierRoute = direct
		}
		laneRecoveryRecordAcks(sequence, items[1:])
		// well past F11b's grace, so only a lane rule can hold it
		sequence.scheduleSelectiveAckRecovery(sendTime.Add(10 * time.Second))
		t.Logf("%s: row 5: relay item overtaken by direct-lane acks written=%v",
			arm.name, hole.selectiveGapRecovered)
		if laneRecoveryReadsLanes(sequence) && hole.selectiveGapRecovered {
			t.Errorf("%s: row 5: a tree that reads lanes still wrote a relay item that only "+
				"direct-lane acknowledgements overtook", arm.name)
		}
	}
}

// Row 6. The same mixed route, but the three later acknowledgements are the
// relay's own: that is an endpoint drop on the relay and it is written in
// that round. Every tree holds it.
func TestLaneRecoveryRow6RelayItemProvenByRelayAcks(t *testing.T) {
	for _, arm := range laneRecoveryArms() {
		sequence, items, sendTime := laneRecoveryScoreboard(t, 8, arm.configure)
		relay := make(Route, 4)
		hole := items[0]
		hole.reliableCarrierObserved = true
		hole.carrierRoute = relay
		for index := 1; index < len(items); index += 1 {
			items[index].selectiveAcked = true
			items[index].carrierRoute = relay
		}
		laneRecoveryRecordAcks(sequence, items[1:])
		sequence.scheduleSelectiveAckRecovery(sendTime.Add(10 * time.Second))
		if !hole.selectiveGapRecovered {
			t.Errorf("%s: row 6: a relay item proven by three later relay acknowledgements was "+
				"not written", arm.name)
		}
	}
}

// Row 7, restated by §29.3. A relay endpoint drop with direct-lane
// acknowledgements only and no later relay item: the hole is its route's
// one tail, and a lane has exactly one, so it is probed at the window's
// minimum rather than waiting for the route head. The bound is that it is
// scheduled no later than merged's own timer would fire, which is its send
// time plus the scaled round trip, and earlier whenever the relay's
// minimum is under its mean. The trade column is gone.
func TestLaneRecoveryRow7RelayTailDropIsProbedNoLaterThanMerged(t *testing.T) {
	for _, arm := range laneRecoveryArms() {
		sequence, items, sendTime := laneRecoveryScoreboard(t, 8, arm.configure)
		relay := make(Route, 4)
		direct := make(Route, 4)
		// the relay carried only the hole, and it is the newest thing the
		// relay carried; everything later took the direct lane, so no later
		// relay acknowledgement will ever prove it
		hole := items[0]
		hole.reliableCarrierObserved = true
		hole.carrierRoute = relay
		for index := 1; index < len(items); index += 1 {
			items[index].selectiveAcked = true
			items[index].carrierRoute = direct
		}
		laneRecoveryRecordSends(sequence, items[:1])
		laneRecoveryRecordAcks(sequence, items[1:])

		// merged and the landed tree cannot act before F11b's grace expires,
		// which is the item's send time plus the scaled round trip. A tree
		// that reads lanes probes at the window's minimum instead.
		mergedWouldFire := sendTime.Add(sequence.rttWindow.ScaledRtt())
		sequence.scheduleSelectiveAckRecovery(sendTime.Add(time.Millisecond))
		early := hole.selectiveGapRecovered || hole.ackTailProbeCount != 0
		earlyAt := hole.resendTime
		if !early {
			// past the grace, where every tree acts
			sequence.scheduleSelectiveAckRecovery(mergedWouldFire)
		}
		recovered := hole.selectiveGapRecovered || hole.ackTailProbeCount != 0
		scheduled := hole.resendTime
		if early {
			scheduled = earlyAt
		}
		t.Logf("%s: row 7: relay tail drop scheduled %s after its send (merged's timer at %s), "+
			"acted before the grace=%v, recovered=%v",
			arm.name, scheduled.Sub(sendTime).Truncate(time.Millisecond),
			mergedWouldFire.Sub(sendTime).Truncate(time.Millisecond), early, recovered)
		if !recovered {
			t.Errorf("%s: row 7: a relay tail drop with three later acknowledgements was not "+
				"scheduled for recovery at all", arm.name)
			continue
		}
		if mergedWouldFire.Before(scheduled) {
			t.Errorf(
				"%s: row 7: the tail drop is scheduled %s after its send, later than merged's own "+
					"timer at %s; the probe round trip never exceeds the scaled round trip, so "+
					"this must never be later",
				arm.name, scheduled.Sub(sendTime), mergedWouldFire.Sub(sendTime),
			)
		}
		if arm.readsLanes && !early {
			t.Errorf("%s: row 7: a tree that reads lanes waited for F11b's grace instead of "+
				"probing its route's one tail", arm.name)
		}
	}
}

// The probe round trip never exceeds the scaled round trip, in every state
// of the window, which is what makes row 7's bound hold by construction.
func TestLaneRecoveryProbeRttNeverExceedsTheScaledRtt(t *testing.T) {
	settings := DefaultSendBufferSettings()
	for _, samples := range [][]time.Duration{
		{},
		{400 * time.Millisecond},
		{50 * time.Millisecond, 3 * time.Second},
		{3 * time.Second, 50 * time.Millisecond},
		{10 * time.Millisecond, 10 * time.Millisecond, 10 * time.Millisecond},
	} {
		window := NewRttWindow(
			NewNoopLogger(), settings.RttWindowSize, settings.RttWindowTimeout,
			settings.RttScale, settings.MinResendInterval,
			settings.RttMinResendInterval, settings.MaxResendInterval,
		)
		now := time.Now()
		for index, sample := range samples {
			at := now.Add(time.Duration(index) * time.Millisecond)
			window.closeSendTime(uint64(at.Add(-sample).UnixMilli()), at)
		}
		probe := window.ProbeRtt()
		scaled := window.ScaledRtt()
		if scaled < probe {
			t.Errorf("with samples %v the probe round trip is %s, past the scaled round trip %s",
				samples, probe, scaled)
		}
	}
}

// Rows 1 to 3 are the timer's, measured on a real link. Row 3 first,
// because it is the one the landed tree already holds: a lane that is
// draining must not be rewritten, however deep its queue.
func TestLaneRecoveryRow3DrainingLaneIsNotRewritten(t *testing.T) {
	if testing.Short() {
		t.Skip("lane recovery contract, live link")
	}
	const messageCount = 1200
	for _, arm := range laneRecoveryArms() {
		link := newLaneRecoveryLink(
			t, 20*time.Millisecond, 12*time.Millisecond, 0, 0, 64, 0, 0, 0, arm.configure)
		stats := laneRecoverySend(t, link, messageCount)
		t.Logf("%s: row 3: draining lane wrote %d whole-window retransmits, %d gap recoveries",
			arm.name, stats.TimeoutResendWriteCount, stats.SelectiveGapWriteCount)
		// a lane whose acknowledgement is still advancing must not be
		// rewritten at all beyond the tail's own timeout, which is §15.1
		const bound = 10
		if bound < int(stats.TimeoutResendWriteCount) {
			t.Errorf(
				"%s: row 3: a lane that is draining was rewritten %d times over %d messages, "+
					"want at most %d; a retransmit while the acknowledgement is still advancing "+
					"is a duplicate",
				arm.name, stats.TimeoutResendWriteCount, messageCount, bound,
			)
		}
	}
}

// Rows 1 and 2. A stall mid-transfer: the writes during it must be at most
// the head's probes at doubling intervals, which is a logarithm of the
// stall over the interval, not one per item. merged fails by construction,
// rewriting its window; the landed tree fails row 1 on its second firings.
func TestLaneRecoveryRows1And2StallWritesAreLogarithmic(t *testing.T) {
	if testing.Short() {
		t.Skip("lane recovery contract, live link")
	}
	// §28 restated the bound: with the deferral's limit and since-last term
	// removed for reliable-carried items, the writes are the route head's
	// probes and nothing else, so the bound is absolute and independent of
	// the items outstanding at onset, which is the quantity the parallel-flow
	// cells and the collapsing seed scale with. The deferrals are
	// irreducible and free, one per item outstanding at onset, since the
	// stall's first interval is indistinguishable from slow draining by the
	// route's own clock.
	writesByRowAndArm := map[string]uint64{}
	for _, row := range []struct {
		name          string
		serialization time.Duration
		messageCount  int
	}{
		{"row 1, tight pre-stall interval", 3 * time.Millisecond, 3000},
		{"row 2, queue-inflated pre-stall interval", 12 * time.Millisecond, 1500},
	} {
		for _, arm := range laneRecoveryArms() {
			link := newLaneRecoveryLink(
				t, 100*time.Millisecond, row.serialization,
				1500*time.Millisecond, 2750*time.Millisecond, 1024, 0, 0, 0, arm.configure)
			stats := laneRecoverySend(t, link, row.messageCount)
			const bound = 10
			t.Logf("%s: %s: wrote %d whole-window retransmits through the stall (bound %d)%s",
				arm.name, row.name, stats.TimeoutResendWriteCount, bound,
				laneRecoveryDetailForTree(stats))
			if gap, ok := laneRecoveryLongestGapForTree(stats); ok && gap < time.Second {
				t.Errorf("%s: %s: the longest lane gap was %s, so the transfer did not outlast "+
					"the stall and the row measured nothing", arm.name, row.name, gap)
				continue
			}
			writesByRowAndArm[row.name+"/"+arm.name] = stats.TimeoutResendWriteCount
			if arm.readsLanes && bound < int(stats.TimeoutResendWriteCount) {
				t.Errorf("%s: %s: a tree that reads lanes wrote %d retransmits through the stall, "+
					"want at most %d", arm.name, row.name, stats.TimeoutResendWriteCount, bound)
			}
		}
		// the reduction, which is the property that holds on both rows: a
		// tree that reads lanes writes an order of magnitude fewer
		var perItem, perLane uint64
		var havePerLane bool
		for _, arm := range laneRecoveryArms() {
			if written, ok := writesByRowAndArm[row.name+"/"+arm.name]; ok {
				if arm.readsLanes {
					perLane, havePerLane = written, true
				} else {
					perItem = written
				}
			}
		}
		if havePerLane && 100 < perItem && perItem/10 < perLane {
			t.Errorf(
				"%s: reading the lane wrote %d retransmits against %d per item, less than a "+
					"tenfold reduction",
				row.name, perLane, perItem,
			)
		}
	}
}

// laneRecoverySend offers messageCount messages and waits for them.
func laneRecoverySend(
	t testing.TB,
	link *laneRecoveryLink,
	messageCount int,
) ClientSendRecoveryStatsSnapshot {
	t.Helper()
	content := ""
	for len(content) < 900 {
		content += "lane-recovery-"
	}
	for index := range messageCount {
		frame, err := ToFrame(&protocol.SimpleMessage{
			Content: fmt.Sprintf("%s%d", content, index),
		}, DefaultProtocolVersion)
		if err != nil {
			t.Fatal(err)
		}
		if !link.sender.SendWithTimeout(frame, link.receiverId, nil, 60*time.Second) {
			MessagePoolReturn(frame.MessageBytes)
			t.Fatal("the sender refused a message")
		}
	}
	delivered := 0
	deadline := time.After(180 * time.Second)
	for delivered < messageCount {
		select {
		case count := <-link.received:
			delivered += count
		case <-deadline:
			t.Fatalf("only %d of %d messages were delivered", delivered, messageCount)
		}
	}
	time.Sleep(300 * time.Millisecond)
	return link.sender.SendRecoveryStats()
}

// Row 1's other half, and the claim §28 rests on: the write bound does not
// move with the items outstanding at onset, while the deferral count does,
// being one per item the lane held.
func TestLaneRecoveryRow1BoundIsIndependentOfOutstanding(t *testing.T) {
	if testing.Short() {
		t.Skip("lane recovery contract, live link")
	}
	for _, arm := range laneRecoveryArms() {
		var smallest, largest uint64
		for index, messageCount := range []int{1500, 5000} {
			link := newLaneRecoveryLink(
				t, 100*time.Millisecond, 3*time.Millisecond,
				1500*time.Millisecond, 2750*time.Millisecond, 2048, 0, 0, 0, arm.configure)
			stats := laneRecoverySend(t, link, messageCount)
			t.Logf("%s: row 1 at %d messages: wrote %d%s",
				arm.name, messageCount, stats.TimeoutResendWriteCount,
				laneRecoveryDetailForTree(stats))
			if index == 0 {
				smallest = stats.TimeoutResendWriteCount
			} else {
				largest = stats.TimeoutResendWriteCount
			}
		}
		if !arm.readsLanes {
			continue
		}
		if 4*max(smallest, 1) < largest {
			t.Errorf(
				"%s: row 1: the write count moved from %d to %d as the outstanding window grew; "+
					"under §28 the writes are the route head's probes and nothing else, so the "+
					"bound must not scale with it",
				arm.name, smallest, largest,
			)
		}
	}
}

// Row 8. A single reliable lane draining under deep queue inflation, no
// drop anywhere: a late item must never be written while the lane keeps
// acknowledging items sent before it. merged writes its whole window, and
// the landed tree releases late items at the deferral limit, which is the
// relay-only cell's own residue.
func TestLaneRecoveryRow8LateItemOnADrainingLaneIsNeverWritten(t *testing.T) {
	if testing.Short() {
		t.Skip("lane recovery contract, live link")
	}
	const messageCount = 6000
	for _, arm := range laneRecoveryArms() {
		// the lane slows mid-transfer behind a queue deep enough that no
		// write blocks, so every item's timer fires early while the lane is
		// still delivering and never stops
		link := newLaneRecoveryLink(
			t, 50*time.Millisecond, 500*time.Microsecond, 0, 0, 4096,
			time.Second, 12*time.Millisecond, 0, arm.configure)
		stats := laneRecoverySend(t, link, messageCount)
		t.Logf("%s: row 8: draining under inflation wrote %d whole-window retransmits%s",
			arm.name, stats.TimeoutResendWriteCount, laneRecoveryDetailForTree(stats))
		const bound = 10
		if arm.readsLanes && bound < int(stats.TimeoutResendWriteCount) {
			t.Errorf(
				"%s: row 8: a tree that reads lanes wrote %d retransmits of late items on a lane "+
					"that never stopped acknowledging, want at most %d",
				arm.name, stats.TimeoutResendWriteCount, bound,
			)
		}
	}
}

// Row 9, the trade. One endpoint drop with exactly one later same-lane
// item, which is below the gap rule's threshold, so the hole is proven
// only by that item's acknowledgement moving the route's highest. It is
// then written at its own next timer firing rather than at the moment of
// proof, and the row states the bound: one interval past the proof.
func TestLaneRecoveryRow9ProvenDropWaitsAtMostOneInterval(t *testing.T) {
	for _, arm := range laneRecoveryArms() {
		sequence, items, _ := laneRecoveryScoreboard(t, 3, arm.configure)
		relay := make(Route, 4)
		hole := items[0]
		hole.reliableCarrierObserved = true
		hole.carrierRoute = relay
		hole.sequenceNumber = 1
		// one later item on the same lane, acknowledged
		proof := items[1]
		proof.selectiveAcked = true
		proof.carrierRoute = relay
		proof.sequenceNumber = 2
		laneRecoveryRecordAcks(sequence, items[1:2])

		interval := sequence.resendIntervalForItem(hole, hole.sendCount)
		verdict := laneRecoveryTimerVerdict(sequence, hole)
		t.Logf("%s: row 9: one same-lane proof, timer verdict=%s, its own interval %s",
			arm.name, verdict, interval.Truncate(time.Millisecond))
		if arm.readsLanes && verdict != "endpoint drop" {
			t.Errorf("%s: row 9: a hole its own lane acknowledged past reads as %q, want an "+
				"endpoint drop written at its next firing", arm.name, verdict)
		}
		// the bound: the wait is the item's own interval, never the cap
		if max := sequence.sendBufferSettings.MaxResendInterval; max < interval {
			t.Errorf("%s: row 9: the proven hole waits %s, past the overall maximum %s",
				arm.name, interval, max)
		}
	}
}

// Row 11. The held-item re-arm: an item riding behind the route head must
// never be re-armed to a time already past, which spins it through the
// resend loop. The ratio of rides to probes is the check.
func TestLaneRecoveryRow11HeldItemsDoNotSpin(t *testing.T) {
	if testing.Short() {
		t.Skip("lane recovery contract, live link")
	}
	for _, arm := range laneRecoveryArms() {
		if !arm.readsLanes {
			continue
		}
		link := newLaneRecoveryLink(
			t, 100*time.Millisecond, 3*time.Millisecond,
			1500*time.Millisecond, 2750*time.Millisecond, 2048, 0, 0, 0, arm.configure)
		stats := laneRecoverySend(t, link, 3000)
		rides, probes := laneRecoveryRidesAndProbes(stats)
		t.Logf("%s: row 11: %d rides against %d probes", arm.name, rides, probes)
		if probes == 0 {
			t.Errorf("%s: row 11: no probe was written, so the row measured nothing", arm.name)
			continue
		}
		// a ride costs one queue re-arm per item per probe interval; orders of
		// magnitude more than that is the loop spinning
		if 10_000*probes < rides {
			t.Errorf("%s: row 11: %d rides against %d probes, so a held item is being re-armed "+
				"into the past and spinning", arm.name, rides, probes)
		}
	}
}

// Row 10, both halves asserted separately so each column's failure is
// legible. During a stall of the relay while the direct lane stays
// healthy: delivery must continue, and nothing extra must be written into
// the stalled lane. §29.4 expected merged to lead the first half, because
// its whole-window rewrite goes p2p-first and the direct lane carries what
// it can. Measured, it does not: the receiver's stream is ordered, so
// nothing past the relay-carried head can be delivered however much is
// rewritten, and the delivered count is the same on every arm at every
// flight size. So the first half does not separate the trees, and the
// second half does, by two orders of magnitude.
func TestLaneRecoveryRow10StallHealingAndWritesIntoTheStalledLane(t *testing.T) {
	if testing.Short() {
		t.Skip("lane recovery contract, live link")
	}
	const (
		messageCount = 2500
		stallAfter   = 1500 * time.Millisecond
		stallFor     = 2750 * time.Millisecond
	)
	// more than one direct-flight size, since the claim under test was that
	// the healing rate should follow the flight
	for _, flight := range []int{4, 32} {
		delivered := map[string]int{}
		written := map[string]uint64{}
		for _, arm := range laneRecoveryArms() {
			link := newLaneRecoveryLink(
				t, 100*time.Millisecond, 2*time.Millisecond,
				stallAfter, stallFor, 2048, 0, 0, flight, arm.configure)
			stats := laneRecoverySend(t, link, messageCount)
			link.deliveryLock.Lock()
			times := append([]time.Time(nil), link.deliveryTimes...)
			link.deliveryLock.Unlock()
			during := 0
			for _, at := range times {
				if since := at.Sub(link.start); stallAfter <= since &&
					since < stallAfter+stallFor {
					during += 1
				}
			}
			delivered[arm.name] = during
			written[arm.name] = stats.TimeoutResendWriteCount
			t.Logf("%s: row 10 at flight %d: delivered %d frames during the stall, wrote %d%s",
				arm.name, flight, during, stats.TimeoutResendWriteCount,
				laneRecoveryDetailForTree(stats))

			// the second half: nothing extra written into the stalled lane
			const bound = 10
			if arm.readsLanes && bound < int(stats.TimeoutResendWriteCount) {
				t.Errorf(
					"%s: row 10 at flight %d: wrote %d retransmits into a stalled lane, want at "+
						"most %d; the bound must hold at every flight size",
					arm.name, flight, stats.TimeoutResendWriteCount, bound,
				)
			}
		}
		// the first half: a tree that reads lanes must not deliver less
		var perItem, perLane int
		var havePerLane bool
		for _, arm := range laneRecoveryArms() {
			if arm.readsLanes {
				perLane, havePerLane = delivered[arm.name], true
			} else {
				perItem = delivered[arm.name]
			}
		}
		if havePerLane && 0 < perItem && perLane < perItem/2 {
			t.Errorf(
				"at flight %d a tree that reads lanes delivered %d frames during the stall "+
					"against %d, so probing one item costs delivery the rewrite would have bought",
				flight, perLane, perItem,
			)
		}
	}
}
