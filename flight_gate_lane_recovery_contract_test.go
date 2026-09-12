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

func laneRecoveryReadsLanes(sequence *SendSequence) bool {
	return laneRecoveryReadsLanesForTree(sequence)
}

// ---- a single reliable lane between two real clients, with a stall ----

type laneRecoveryLink struct {
	sender     *Client
	receiver   *Client
	receiverId Id
	received   chan int
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
	link.receiver.AddReceiveCallback(func(_ TransferPath, frames []*protocol.Frame, _ Peer) {
		select {
		case link.received <- len(frames):
		default:
		}
	})

	start := time.Now()
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

	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer closeCancel()
		link.sender.CloseAndWait(closeCtx)
		link.receiver.CloseAndWait(closeCtx)
		forwarders.Wait()
		for _, route := range []Route{senderOut, receiverIn, receiverOut, senderIn} {
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

// Row 7. The trade, stated so it is assertable and so merged's column
// records where merged is faster: a relay endpoint drop with direct-lane
// acknowledgements only and no later relay item. merged and the landed
// tree recover it within F11b's grace; a tree that reads lanes waits for
// the route head's probe, and the row states that bound.
func TestLaneRecoveryRow7RelayTailDropWaitsForTheProbe(t *testing.T) {
	for _, arm := range laneRecoveryArms() {
		sequence, items, sendTime := laneRecoveryScoreboard(t, 8, arm.configure)
		relay := make(Route, 4)
		direct := make(Route, 4)
		// the relay carried only the hole; everything later took the direct
		// lane, so no later relay acknowledgement will ever prove it
		hole := items[0]
		hole.reliableCarrierObserved = true
		hole.carrierRoute = relay
		for index := 1; index < len(items); index += 1 {
			items[index].selectiveAcked = true
			items[index].carrierRoute = direct
		}
		laneRecoveryRecordAcks(sequence, items[1:])
		sequence.scheduleSelectiveAckRecovery(sendTime.Add(10 * time.Second))
		recovered := hole.selectiveGapRecovered
		reads := laneRecoveryReadsLanes(sequence)
		t.Logf("%s: row 7: relay tail drop recovered by the gap rule=%v (reads lanes=%v)",
			arm.name, recovered, reads)
		if reads && recovered {
			t.Errorf("%s: row 7: a tree that reads lanes recovered a relay tail drop by the gap "+
				"rule, but no later relay acknowledgement exists to prove it", arm.name)
		}
		if !reads && !recovered {
			t.Errorf("%s: row 7: a tree that does not read lanes must recover this within F11b's "+
				"grace, and did not", arm.name)
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
		link := newLaneRecoveryLink(t, 20*time.Millisecond, 12*time.Millisecond, 0, 0, 64, arm.configure)
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
	// §27.2 stated the bound as ten writes absolutely. That holds on row 2
	// and not on row 1, where the residue is the firings inside the stall's
	// first scaled round trip: those read as draining from the route's own
	// clock, so §27.3 sends them to §13.5's deferral, whose since-last rule
	// writes a second firing. The residue scales with the items outstanding
	// at onset, so the row asserts the reduction, which holds on both, and
	// the absolute bound only where it is met. The excess is recorded rather
	// than hidden.
	writesByRowAndArm := map[string]uint64{}
	for _, row := range []struct {
		name          string
		serialization time.Duration
		messageCount  int
		absoluteBound bool
	}{
		{"row 1, tight pre-stall interval", 3 * time.Millisecond, 3000, false},
		{"row 2, queue-inflated pre-stall interval", 12 * time.Millisecond, 1500, true},
	} {
		for _, arm := range laneRecoveryArms() {
			link := newLaneRecoveryLink(
				t, 100*time.Millisecond, row.serialization,
				1500*time.Millisecond, 2750*time.Millisecond, 1024, arm.configure)
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
			if arm.readsLanes && row.absoluteBound && bound < int(stats.TimeoutResendWriteCount) {
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
