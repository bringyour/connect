//go:build flightgate_next

// FLIGHTGATEFIX §20.3. The seven-row mixed-lane contract. It is the
// specification of the affinity candidate in §20.5, no longer a gate on
// this landing, so it builds only under the flightgate_next tag.

package connect

// FLIGHTGATEFIX §18. The contract for mixed-lane behaviour, written as what
// is correct rather than as what any one tree does today. A lane's recent
// evidence classifies it as losing or not, and that one classification
// drives three behaviours: whether a proven gap reduces the unreliable
// flight, whether replies keep the lane's affinity, and whether a due
// retransmit is written or deferred. Each test states the regime it is in
// and which behaviour it holds, so a reader who trips one knows both.
//
// These use only the transfer API that predates the program, so the same
// file runs against the merged base and against this tree and the report
// can say which behaviours a tree has by design and which by accident.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

const laneContractMessageByteCount = 930

// laneContractSequence builds one send sequence over a mixed-lane route
// generation, with the direct lane's items already marked as carried by it.
// The scoreboard is then driven directly, which is deterministic and needs
// no goroutine, so the regime is exactly what the test states.
func laneContractSequence(
	t testing.TB,
	itemCount int,
	sendTime time.Time,
) (*SendSequence, []*sendItem) {
	t.Helper()
	sequence, items := newSelectiveAckRecoveryTestSequence(itemCount, sendTime)
	sequence.client = &Client{}
	sequence.flightController = newSendFlightController(sequence.sendBufferSettings)
	sequence.flightController.applyPolicy(transferFlightPolicySnapshot{
		generation:             1,
		limited:                true,
		reliableRouteAvailable: true,
	})
	return sequence, items
}

// laneContractHole marks one item as a hole the direct lane carried, with
// three later items selectively acknowledged: the evidence the scoreboard
// acts on.
func laneContractHole(items []*sendItem, holeIndex int) {
	items[holeIndex].unreliableCarrierObserved = true
	items[holeIndex].unreliableFlightTracked = true
	items[holeIndex].selectiveAcked = false
	items[holeIndex].selectiveGapRecovered = false
	items[holeIndex].recoveryKind = sendRecoveryNone
	for index := range items {
		if holeIndex < index {
			items[index].selectiveAcked = true
		}
	}
}

// laneContractGrownFlight opens the unreliable flight past its floor, so a
// reduction is visible as a smaller window rather than hidden by the floor.
func laneContractGrownFlight(sequence *SendSequence) ByteCount {
	controller := sequence.flightController
	for range 64 {
		controller.send(1024)
		controller.acknowledge(1024)
	}
	return controller.byteLimit
}

// 1. Classification. A lane that really drops is classified losing within a
// small number of events, stated here as one proven loss; a lane whose
// acknowledgements are merely late, none lost, is not. The regime is
// established through behaviour rather than through any tree's internals: a
// hole older than any grace is a proven loss, a fresh hole is not.
func TestLaneContractClassificationTipsEarly(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(time.Millisecond)

	// regime: the lane is dropping. The first hole is old enough that no
	// grace can cover it, so recovering it is proven loss; the next hole,
	// fresh, must then be recovered at once too.
	losing, losingItems := laneContractSequence(t, 8, sendTime)
	losingItems[0].sendTime = sendTime.Add(-30 * time.Second)
	laneContractHole(losingItems, 0)
	losing.scheduleSelectiveAckRecovery(currentTime)
	if losingItems[0].resendTime.After(currentTime) {
		t.Fatalf("a hole older than any grace was still postponed by %s",
			losingItems[0].resendTime.Sub(currentTime))
	}
	laneContractHole(losingItems, 4)
	losing.scheduleSelectiveAckRecovery(currentTime)
	if losingItems[4].resendTime.After(currentTime) {
		t.Fatalf(
			"losing regime: after one proven loss the next hole still waits %s before recovery, "+
				"so the lane was not classified losing within one event",
			losingItems[4].resendTime.Sub(currentTime),
		)
	}

	// regime: the lane is reordering. Nothing has been lost, so a fresh hole
	// must not be recovered at once: its acknowledgement is still in the air
	// on a slower lane, and recovering now costs traffic and window for
	// nothing.
	reordering, reorderingItems := laneContractSequence(t, 8, sendTime)
	laneContractHole(reorderingItems, 0)
	reordering.scheduleSelectiveAckRecovery(currentTime)
	if !reorderingItems[0].resendTime.After(currentTime) {
		t.Fatal(
			"reordering regime: a hole whose acknowledgement could still arrive was recovered at " +
				"once, so reordering is charged as loss",
		)
	}
}

// 2. Window. A proven gap on a losing lane reduces the unreliable flight,
// so the direct window collapses toward its floor and the reliable lane
// carries the bulk. A hole that is merely late reduces nothing.
func TestLaneContractWindowReducesOnProvenLossOnly(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(time.Millisecond)

	losing, losingItems := laneContractSequence(t, 8, sendTime)
	losingItems[0].sendTime = sendTime.Add(-30 * time.Second)
	before := laneContractGrownFlight(losing)
	if before <= losing.flightController.activeMinimumByteCount {
		t.Fatalf("the flight never opened past its floor: %d", before)
	}
	laneContractHole(losingItems, 0)
	if !losing.scheduleSelectiveAckRecovery(currentTime) {
		t.Fatal("losing regime: a proven gap did not ask for the flight to be reduced")
	}
	losing.flightController.reduceForLoss()
	if before <= losing.flightController.byteLimit {
		t.Fatalf(
			"losing regime: the unreliable window is still %d bytes after a proven gap, was %d: "+
				"the direct lane keeps feeding a path that is dropping",
			losing.flightController.byteLimit, before,
		)
	}

	reordering, reorderingItems := laneContractSequence(t, 8, sendTime)
	reorderingBefore := laneContractGrownFlight(reordering)
	laneContractHole(reorderingItems, 0)
	if reordering.scheduleSelectiveAckRecovery(currentTime) {
		t.Fatal(
			"reordering regime: a hole whose acknowledgement is still in the air asked for the " +
				"flight to be reduced, so reordering costs window as well as traffic",
		)
	}
	if reordering.flightController.byteLimit != reorderingBefore {
		t.Fatalf("reordering regime: the window moved from %d to %d with nothing lost",
			reorderingBefore, reordering.flightController.byteLimit)
	}
}

// laneContractPair is two real clients over a direct lane and a relay, so a
// test can assert the route an acknowledgement actually took.
type laneContractPair struct {
	sender      *Client
	receiver    *Client
	receiverId  Id
	directIn    Route
	relayIn     Route
	directReply Route
	relayReply  Route
}

func newLaneContractPair(t testing.TB, directType TransportType, hybrid bool) *laneContractPair {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	newSettings := func() *ClientSettings {
		settings := DefaultClientSettings()
		settings.EncryptionSettings.Mode = EncryptionModeOff
		settings.ReceiveBufferSettings.GapTimeout = 60 * time.Second
		settings.ReceiveBufferSettings.IdleTimeout = 60 * time.Second
		settings.SendBufferSettings.AckTimeout = 60 * time.Second
		settings.SendBufferSettings.IdleTimeout = 60 * time.Second
		return settings
	}
	pair := &laneContractPair{
		receiverId:  NewId(),
		directIn:    make(Route, 32),
		relayIn:     make(Route, 32),
		directReply: make(Route, 32),
		relayReply:  make(Route, 32),
	}
	senderId := NewId()
	pair.sender = NewClient(ctx, senderId, NewNoContractClientOob(), newSettings())
	pair.receiver = NewClient(ctx, pair.receiverId, NewNoContractClientOob(), newSettings())
	pair.sender.ContractManager().AddNoContractPeer(pair.receiverId)
	pair.receiver.ContractManager().AddNoContractPeer(senderId)

	directProperties := TransferCarrierProperties{Unreliable: true}
	if hybrid {
		// a hybrid carrier: the stream lane of a connection that also has a
		// datagram lane, which must keep its affinity
		directProperties.unreliableForMessageByteCount = func(int) bool { return false }
	}
	pair.receiver.RouteManager().UpdateTransportWithProperties(
		NewReceiveGatewayTransportWithType(directType),
		[]Route{pair.directIn},
		directProperties,
	)
	pair.receiver.RouteManager().UpdateTransportWithProperties(
		NewSendGatewayTransportWithType(directType),
		[]Route{pair.directReply},
		directProperties,
	)
	pair.receiver.RouteManager().UpdateTransport(
		NewSendGatewayTransportWithType(TransportTypeH1),
		[]Route{pair.relayReply},
	)
	pair.receiver.RouteManager().UpdateTransportWithProperties(
		NewReceiveGatewayTransportWithType(TransportTypeH1),
		[]Route{pair.relayIn},
		TransferCarrierProperties{},
	)
	pair.receiver.AddReceiveCallback(func(TransferPath, []*protocol.Frame, Peer) {})
	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := pair.sender.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close lane-contract sender: %v", err)
		}
		if err := pair.receiver.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close lane-contract receiver: %v", err)
		}
		for _, route := range []Route{pair.directIn, pair.relayIn, pair.directReply, pair.relayReply} {
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
	return pair
}

// deliverPack hands one Pack to the receiver on the chosen inbound route.
// A gap in the sequence numbers is what a dropped Pack looks like to the
// receiver, which is how a test puts the inbound lane in the losing regime.
func (self *laneContractPair) deliverPack(
	t testing.TB,
	inbound Route,
	sequenceId Id,
	sequenceNumber uint64,
	head bool,
) Id {
	t.Helper()
	frame, err := ToFrame(
		&protocol.SimpleMessage{Content: fmt.Sprintf("lane-contract-%d", sequenceNumber)},
		DefaultProtocolVersion,
	)
	if err != nil {
		t.Fatal(err)
	}
	messageId := NewId()
	packBytes, err := ProtoMarshal(&protocol.TransferFrame{
		TransferPath: TransferPath{
			SourceId:      self.sender.ClientId(),
			DestinationId: self.receiver.ClientId(),
		}.ToProtobuf(),
		Pack: &protocol.Pack{
			MessageId:      messageId.Bytes(),
			SequenceId:     sequenceId.Bytes(),
			SequenceNumber: sequenceNumber,
			Head:           head,
			Frames:         []*protocol.Frame{frame},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case inbound <- packBytes:
	case <-time.After(5 * time.Second):
		MessagePoolReturn(packBytes)
		t.Fatal("the receiver did not accept the Pack")
	}
	return messageId
}

// awaitAckRoute reports which route carried an acknowledgement first.
func awaitLaneContractAck(
	t testing.TB,
	direct Route,
	relay Route,
	timeout time.Duration,
) string {
	t.Helper()
	deadline := time.After(timeout)
	isAck := func(b []byte) bool {
		var transferFrame protocol.TransferFrame
		if err := ProtoUnmarshal(b, &transferFrame); err != nil {
			return false
		}
		if transferFrame.GetAck() != nil {
			return true
		}
		frame := transferFrame.GetFrame()
		return frame != nil && frame.GetMessageType() == protocol.MessageType_TransferAck
	}
	for {
		select {
		case b := <-direct:
			ack := isAck(b)
			MessagePoolReturn(b)
			if ack {
				return "direct"
			}
		case b := <-relay:
			ack := isAck(b)
			MessagePoolReturn(b)
			if ack {
				return "relay"
			}
		case <-deadline:
			return "none"
		}
	}
}

// 3. Replies. While the lane a Pack arrived on is losing, its
// acknowledgements leave for the reliable lane, because a lost cumulative
// ack costs the sender a window. While it is healthy they keep affinity,
// which is the win the clean cells measure, and a hybrid carrier keeps
// affinity through the same rule.
func TestLaneContractRepliesLeaveALosingLane(t *testing.T) {
	t.Run("healthy direct lane keeps affinity", func(t *testing.T) {
		pair := newLaneContractPair(t, TransportTypeP2p, false)
		sequenceId := NewId()
		pair.deliverPack(t, pair.directIn, sequenceId, 0, true)
		if route := awaitLaneContractAck(t, pair.directReply, pair.relayReply, 5*time.Second); route != "direct" {
			t.Fatalf(
				"clean regime: the acknowledgement for a Pack from a healthy direct lane took the %s route; "+
					"affinity is the clean-cell win",
				route,
			)
		}
	})
	t.Run("losing direct lane hands replies to the relay", func(t *testing.T) {
		pair := newLaneContractPair(t, TransportTypeP2p, false)
		sequenceId := NewId()
		pair.deliverPack(t, pair.directIn, sequenceId, 0, true)
		if route := awaitLaneContractAck(t, pair.directReply, pair.relayReply, 5*time.Second); route == "none" {
			t.Fatal("no acknowledgement for the head Pack")
		}
		// sequence number 1 never arrives: the lane dropped it, and the
		// receiver's selective acknowledgement is the local evidence
		pair.deliverPack(t, pair.directIn, sequenceId, 2, false)
		pair.deliverPack(t, pair.directIn, sequenceId, 3, false)
		if route := awaitLaneContractAck(t, pair.directReply, pair.relayReply, 5*time.Second); route != "relay" {
			t.Fatalf(
				"losing regime: the acknowledgement for a Pack from a lane that just dropped one took "+
					"the %s route; on a dropping lane a lost cumulative ack costs the sender a window",
				route,
			)
		}
	})
	t.Run("hybrid carrier keeps affinity while its own lane is clean", func(t *testing.T) {
		pair := newLaneContractPair(t, TransportTypeH3, true)
		sequenceId := NewId()
		pair.deliverPack(t, pair.directIn, sequenceId, 0, true)
		if route := awaitLaneContractAck(t, pair.directReply, pair.relayReply, 5*time.Second); route != "direct" {
			t.Fatalf(
				"clean regime: a hybrid carrier's acknowledgement took the %s route; a hybrid lane "+
					"moves only when its own lane shows loss",
				route,
			)
		}
	})
}
