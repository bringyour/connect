package connect

// Deterministic reproductions of the pinned-provider collapse mechanisms
// named in FLIGHTGATEFIX.md §5. Each test states its mechanism (M1..M7) and
// whether it is expected to fail on the tree it was written against; a test
// that defines a candidate's contract before the candidate exists skips and
// names it.

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// flightGateSettings returns the flight-test client settings with a
// configurable unreliable flight so several Packs can be in the air.
func flightGateSettings(flightByteCount ByteCount) *ClientSettings {
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.UnreliableInitialFlightByteCount = flightByteCount
	settings.SendBufferSettings.UnreliableMinimumFlightByteCount = flightByteCount
	settings.SendBufferSettings.UnreliableMaximumFlightByteCount = flightByteCount
	settings.SendBufferSettings.UnreliableFlightIncreaseByteCount = 1
	settings.SendBufferSettings.UnreliableInitialFlightMessageCount = 0
	return settings
}

// newFlightGateSender builds one no-contract sender whose routes the test
// adds itself, plus the receive route its acknowledgements arrive on and the
// admission-wait barrier signal.
func newFlightGateSender(
	t testing.TB,
	settings *ClientSettings,
) (*Client, Id, Route, <-chan sendSequenceId) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	peerId := NewId()
	client.ContractManager().AddNoContractPeer(peerId)
	fromPeer := make(chan []byte, 16)
	waits := make(chan sendSequenceId, 16)
	client.sendBuffer.beforeResendCapacityWaitForTest = func(sequenceId sendSequenceId) {
		select {
		case waits <- sequenceId:
		default:
		}
	}
	client.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{fromPeer})
	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := client.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close flight-gate sender: %v", err)
		}
		drainFlightGateRoute(fromPeer)
	})
	return client, peerId, fromPeer, waits
}

func drainFlightGateRoute(route Route) {
	for {
		select {
		case message := <-route:
			if message != nil {
				MessagePoolReturn(message)
			}
		default:
			return
		}
	}
}

// addFlightGateRoute publishes one send route of the given carrier type and
// capacity; unreliable routes carry the p2p fast-lane properties.
func addFlightGateRoute(
	t testing.TB,
	client *Client,
	transportType TransportType,
	capacity int,
	unreliable bool,
) (Transport, Route) {
	t.Helper()
	route := make(Route, capacity)
	transport := NewSendGatewayTransportWithType(transportType)
	if unreliable {
		client.RouteManager().UpdateTransportWithProperties(
			transport,
			[]Route{route},
			TransferCarrierProperties{Unreliable: true},
		)
	} else {
		client.RouteManager().UpdateTransport(transport, []Route{route})
	}
	t.Cleanup(func() {
		drainFlightGateRoute(route)
	})
	return transport, route
}

// fillFlightGateRoute leaves a route with no free slot so the selector's
// non-blocking pass must fall through to another route.
func fillFlightGateRoute(route Route) {
	for {
		select {
		case route <- nil:
		default:
			return
		}
	}
}

func sendFlightGateMessage(t testing.TB, client *Client, peerId Id, index int) {
	t.Helper()
	frame, err := ToFrame(
		&protocol.SimpleMessage{Content: fmt.Sprintf("gate-%d", index)},
		DefaultProtocolVersion,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !client.SendWithTimeout(frame, peerId, nil, 5*time.Second) {
		MessagePoolReturn(frame.MessageBytes)
		t.Fatalf("message %d was not admitted", index)
	}
}

// decodeFlightGatePack decodes one routed Transfer frame and releases the
// carrier bytes. It returns nil for anything that is not an application
// Pack: nil sentinels placed by fillFlightGateRoute, ACKs, and the client
// key announcement a Client writes to its first route.
func decodeFlightGatePack(t testing.TB, transferFrameBytes []byte) *protocol.Pack {
	t.Helper()
	if transferFrameBytes == nil {
		return nil
	}
	defer MessagePoolReturn(transferFrameBytes)
	var transferFrame protocol.TransferFrame
	if err := ProtoUnmarshal(transferFrameBytes, &transferFrame); err != nil {
		t.Fatalf("decode TransferFrame: %v", err)
	}
	pack := transferFrame.Pack
	if pack == nil {
		frame := transferFrame.GetFrame()
		if frame == nil || frame.GetMessageType() != protocol.MessageType_TransferPack {
			return nil
		}
		pack = &protocol.Pack{}
		if err := ProtoUnmarshal(frame.MessageBytes, pack); err != nil {
			t.Fatalf("decode Pack: %v", err)
		}
	}
	for _, frame := range pack.Frames {
		if frame.GetMessageType() == protocol.MessageType_TransferClientKey {
			return nil
		}
	}
	return pack
}

func takeFlightGatePack(t testing.TB, route Route, timeout time.Duration) *protocol.Pack {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case transferFrameBytes := <-route:
			if pack := decodeFlightGatePack(t, transferFrameBytes); pack != nil {
				return pack
			}
		case <-deadline:
			t.Fatal("timed out waiting for a Pack")
			return nil
		}
	}
}

// takeFlightGatePackAndFill takes the next application Pack off a route and
// leaves the route full, so the selector's non-blocking pass keeps falling
// through to other routes as if the Pack were still queued there.
func takeFlightGatePackAndFill(t testing.TB, route Route, timeout time.Duration) *protocol.Pack {
	t.Helper()
	pack := takeFlightGatePack(t, route, timeout)
	fillFlightGateRoute(route)
	return pack
}

// ackFlightGatePack delivers one ACK for the Pack through the sender's
// receive pump. The Pack's tag is echoed so the sender records an RTT sample.
func ackFlightGatePack(
	t testing.TB,
	client *Client,
	peerId Id,
	fromPeer Route,
	pack *protocol.Pack,
	selective bool,
) {
	t.Helper()
	ackBytes, err := ProtoMarshal(&protocol.TransferFrame{
		TransferPath: TransferPath{
			SourceId:      peerId,
			DestinationId: client.ClientId(),
		}.ToProtobuf(),
		Ack: &protocol.Ack{
			MessageId:  pack.MessageId,
			SequenceId: pack.SequenceId,
			Selective:  selective,
			Tag:        pack.Tag,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case fromPeer <- ackBytes:
	case <-time.After(5 * time.Second):
		MessagePoolReturn(ackBytes)
		t.Fatal("could not deliver the ACK")
	}
}

func flightGateSendSequence(t testing.TB, client *Client, peerId Id) *SendSequence {
	t.Helper()
	sequence := client.sendBuffer.lookupSendSequence(sendSequenceId{Destination: peerId}, nil)
	if sequence == nil {
		t.Fatal("no send sequence for the peer")
	}
	return sequence
}

// M1. The unreliable flight is full and never acknowledged; a reliable route
// is also active. The next Pack must be written on the reliable route without
// the sequence waiting on the flight. Expected red on the tree this was
// written against: the admission gate is route-wide.
func TestSendSequenceUnreliableFlightDoesNotGateReliableSibling(t *testing.T) {
	client, peerId, _, waits := newFlightGateSender(t, flightGateSettings(1))
	_, unreliable := addFlightGateRoute(t, client, TransportTypeP2p, 4, true)

	sendFlightGateMessage(t, client, peerId, 0)
	first := takeFlightGatePack(t, unreliable, 5*time.Second)

	_, reliable := addFlightGateRoute(t, client, TransportTypeH1, 16, false)
	// the idle sequence may already have parked on its full flight before
	// the reliable route existed; a gated sequence stops reading its pack
	// channel, so the wait barrier is not the signal here: the second Pack
	// reaching the reliable route within one resend interval is.
	time.Sleep(50 * time.Millisecond)
	for len(waits) > 0 {
		<-waits
	}
	before := client.SendRecoveryStats()
	sendFlightGateMessage(t, client, peerId, 1)
	deadline := time.After(1500 * time.Millisecond)
awaitSecond:
	for {
		select {
		case transferFrameBytes := <-reliable:
			if pack := decodeFlightGatePack(t, transferFrameBytes); pack != nil &&
				pack.SequenceNumber == first.SequenceNumber+1 {
				break awaitSecond
			}
		case transferFrameBytes := <-unreliable:
			if pack := decodeFlightGatePack(t, transferFrameBytes); pack != nil &&
				pack.SequenceNumber == first.SequenceNumber+1 {
				t.Fatal("second Pack rode the full unreliable lane")
			}
		case <-deadline:
			t.Fatal("second Pack was gated by the full unreliable flight while a reliable route had capacity")
		}
	}
	after := client.SendRecoveryStats()
	if after.UnreliableFlightBlockedWithReliableCapacity != before.UnreliableFlightBlockedWithReliableCapacity {
		t.Fatalf("flight gated a Pack while a reliable route had capacity: %+v", after)
	}
}

// M1 guard. Only writes the unreliable lane actually accepted are counted in
// its flight; Packs the reliable route carried never enter it. Passes today
// and must keep passing under every candidate.
func TestSendSequenceUnreliableFlightTracksOnlyUnreliableWrites(t *testing.T) {
	client, peerId, _, _ := newFlightGateSender(t, flightGateSettings(kib(64)))
	_, unreliable := addFlightGateRoute(t, client, TransportTypeP2p, 1, true)

	sendFlightGateMessage(t, client, peerId, 0)
	first := takeFlightGatePackAndFill(t, unreliable, 5*time.Second)
	// the unreliable route is now full; every later Pack must fall through
	_, reliable := addFlightGateRoute(t, client, TransportTypeH1, 16, false)
	for index := 1; index <= 3; index += 1 {
		sendFlightGateMessage(t, client, peerId, index)
		takeFlightGatePack(t, reliable, 5*time.Second)
	}
	recovery := client.SendRecoveryStats()
	if recovery.UnreliableFlightMaximumMessageCount != 1 {
		t.Fatalf("reliable writes entered the unreliable flight: %+v", recovery)
	}
	if recovery.UnreliableFlightMaximumByteCount == 0 ||
		uint64(2*len(first.Frames[0].MessageBytes)+256) < recovery.UnreliableFlightMaximumByteCount {
		t.Fatalf("unreliable flight bytes do not match the single tracked Pack: %+v", recovery)
	}
}

// flightGatePeerPair wires a sender to a receiver through routes the test
// owns, so the test is the wire and decides which carrier each Pack arrives
// on and where the receiver's acknowledgements can go.
type flightGatePeerPair struct {
	sender     *Client
	receiver   *Client
	senderId   Id
	receiverId Id
	senderOut  Route
	senderIn   Route
}

func newFlightGatePeerPair(
	t testing.TB,
	receiverWriteTimeout time.Duration,
) *flightGatePeerPair {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	newSettings := func() *ClientSettings {
		settings := DefaultClientSettings()
		settings.EncryptionSettings.Mode = EncryptionModeOff
		settings.SendBufferSettings.AckTimeout = 60 * time.Second
		settings.SendBufferSettings.IdleTimeout = 60 * time.Second
		settings.ReceiveBufferSettings.GapTimeout = 60 * time.Second
		settings.ReceiveBufferSettings.IdleTimeout = 60 * time.Second
		settings.ReceiveBufferSettings.WriteTimeout = receiverWriteTimeout
		return settings
	}
	pair := &flightGatePeerPair{
		senderId:   NewId(),
		receiverId: NewId(),
		senderOut:  make(Route, 32),
		senderIn:   make(Route, 32),
	}
	pair.sender = NewClient(ctx, pair.senderId, NewNoContractClientOob(), newSettings())
	pair.receiver = NewClient(ctx, pair.receiverId, NewNoContractClientOob(), newSettings())
	pair.sender.ContractManager().AddNoContractPeer(pair.receiverId)
	pair.receiver.ContractManager().AddNoContractPeer(pair.senderId)
	pair.sender.RouteManager().UpdateTransport(NewSendGatewayTransport(), []Route{pair.senderOut})
	pair.sender.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{pair.senderIn})
	pair.receiver.AddReceiveCallback(func(TransferPath, []*protocol.Frame, Peer) {})
	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := pair.sender.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close sender: %v", err)
		}
		if err := pair.receiver.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close receiver: %v", err)
		}
		drainFlightGateRoute(pair.senderOut)
		drainFlightGateRoute(pair.senderIn)
	})
	return pair
}

// receiveRoute publishes a receiver inbound route of the given carrier type.
func (self *flightGatePeerPair) receiveRoute(t testing.TB, transportType TransportType) Route {
	t.Helper()
	route := make(Route, 32)
	self.receiver.RouteManager().UpdateTransport(
		NewReceiveGatewayTransportWithType(transportType),
		[]Route{route},
	)
	t.Cleanup(func() { drainFlightGateRoute(route) })
	return route
}

// ackRoute publishes a receiver outbound route (where its ACKs go) of the
// given carrier type and capacity.
func (self *flightGatePeerPair) ackRoute(
	t testing.TB,
	transportType TransportType,
	capacity int,
	properties TransferCarrierProperties,
) Route {
	t.Helper()
	route := make(Route, capacity)
	transport := NewSendGatewayTransportWithType(transportType)
	if properties.Unreliable {
		self.receiver.RouteManager().UpdateTransportWithProperties(transport, []Route{route}, properties)
	} else {
		self.receiver.RouteManager().UpdateTransport(transport, []Route{route})
	}
	t.Cleanup(func() { drainFlightGateRoute(route) })
	return route
}

// deliver sends one message from the sender and hands its first wire frame
// to the receiver on the chosen inbound route, returning the decoded Pack.
func (self *flightGatePeerPair) deliver(
	t testing.TB,
	index int,
	inbound Route,
) *protocol.Pack {
	t.Helper()
	sendFlightGateMessage(t, self.sender, self.receiverId, index)
	deadline := time.After(5 * time.Second)
	for {
		select {
		case transferFrameBytes := <-self.senderOut:
			copied := MessagePoolCopy(transferFrameBytes)
			pack := decodeFlightGatePack(t, transferFrameBytes)
			if pack == nil {
				// the sender's client key announcement, or a resend of an
				// earlier Pack whose ACK the test withheld: not this delivery
				MessagePoolReturn(copied)
				continue
			}
			select {
			case inbound <- copied:
			case <-time.After(5 * time.Second):
				MessagePoolReturn(copied)
				t.Fatal("receiver inbound route did not accept the Pack")
			}
			return pack
		case <-deadline:
			t.Fatal("sender wrote nothing")
			return nil
		}
	}
}

// awaitAck waits for an ACK naming the Pack on the route; extra frames
// (other ACKs, duplicates) are consumed.
func awaitFlightGateAck(t testing.TB, route Route, pack *protocol.Pack, timeout time.Duration) bool {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case transferFrameBytes := <-route:
			if transferFrameBytes == nil {
				continue
			}
			var transferFrame protocol.TransferFrame
			err := ProtoUnmarshal(transferFrameBytes, &transferFrame)
			MessagePoolReturn(transferFrameBytes)
			if err != nil {
				t.Fatalf("decode ACK frame: %v", err)
			}
			if ack := transferFrame.Ack; ack != nil &&
				Id(ack.MessageId) == Id(pack.MessageId) {
				return true
			}
		case <-deadline:
			return false
		}
	}
}

// M2. A Pack received over the p2p lane pins its ACK to p2p; the p2p route is
// full. The receiver's ack worker is one serial goroutine, so a later ACK
// for a Pack received over h1 waits behind the pinned write. It must not.
// Expected red on the tree this was written against.
func TestReceiveSequenceAckAffinityDoesNotHeadOfLineBlock(t *testing.T) {
	pair := newFlightGatePeerPair(t, 3*time.Second)
	inP2p := pair.receiveRoute(t, TransportTypeP2p)
	inH1 := pair.receiveRoute(t, TransportTypeH1)
	outP2p := pair.ackRoute(t, TransportTypeP2p, 1, TransferCarrierProperties{Unreliable: true})
	outH1 := pair.ackRoute(t, TransportTypeH1, 16, TransferCarrierProperties{})
	fillFlightGateRoute(outP2p)

	pair.deliver(t, 0, inP2p)
	// let the receiver's ACK for Pack 0 reach the full p2p route and block
	time.Sleep(300 * time.Millisecond)
	second := pair.deliver(t, 1, inH1)
	if !awaitFlightGateAck(t, outH1, second, time.Second) {
		t.Fatal("ACK for the h1-received Pack waited behind the pinned p2p ACK")
	}
}

// M2 fix contract (candidate A1). The ACK for a p2p-received Pack falls
// through to the reliable route when the p2p route is full instead of
// waiting for its write timeout. Expected red on the tree this was written
// against.
func TestReceiveSequenceAckFallsThroughWhenUnreliableIsFull(t *testing.T) {
	pair := newFlightGatePeerPair(t, 3*time.Second)
	inP2p := pair.receiveRoute(t, TransportTypeP2p)
	outP2p := pair.ackRoute(t, TransportTypeP2p, 1, TransferCarrierProperties{Unreliable: true})
	outH1 := pair.ackRoute(t, TransportTypeH1, 16, TransferCarrierProperties{})
	fillFlightGateRoute(outP2p)

	first := pair.deliver(t, 0, inP2p)
	if !awaitFlightGateAck(t, outH1, first, time.Second) {
		t.Fatal("ACK for the p2p-received Pack did not fall through to the reliable route")
	}
}

// M3. Pack 0 rides the reliable lane, Packs 1..3 the unreliable lane and are
// acknowledged first. That is reordering across carriers, not loss: no gap
// resend may be written for Pack 0. A real drop on the unreliable lane must
// still produce exactly one gap recovery. Expected red on the tree this was
// written against: the scoreboard counts three later selective ACKs as a
// hole regardless of lane.
func TestSendSequenceReorderingAcrossCarriersIsNotLoss(t *testing.T) {
	client, peerId, fromPeer, _ := newFlightGateSender(t, flightGateSettings(kib(64)))
	_, reliable := addFlightGateRoute(t, client, TransportTypeH1, 1, false)
	sendFlightGateMessage(t, client, peerId, 0)
	first := takeFlightGatePackAndFill(t, reliable, 5*time.Second)
	_, unreliable := addFlightGateRoute(t, client, TransportTypeP2p, 16, true)
	var laterPacks []*protocol.Pack
	for index := 1; index <= 3; index += 1 {
		sendFlightGateMessage(t, client, peerId, index)
		laterPacks = append(laterPacks, takeFlightGatePack(t, unreliable, 5*time.Second))
	}
	for _, pack := range laterPacks {
		ackFlightGatePack(t, client, peerId, fromPeer, pack, true)
	}
	// the reliable lane answers within its own RTT
	time.Sleep(100 * time.Millisecond)
	ackFlightGatePack(t, client, peerId, fromPeer, first, false)
	time.Sleep(200 * time.Millisecond)
	recovery := client.SendRecoveryStats()
	if recovery.SelectiveGapWriteCount != 0 {
		t.Fatalf("reordering across carriers was recovered as a gap: %+v", recovery)
	}
	// settle the first phase: a cumulative ack for Pack 3 retires the
	// selectively acknowledged items, so their cumulative probes cannot be
	// mistaken for the second phase's gap recovery
	ackFlightGatePack(t, client, peerId, fromPeer, laterPacks[2], false)
	time.Sleep(100 * time.Millisecond)
	drainFlightGateRoute(unreliable)
	gapWritesBefore := client.SendRecoveryStats().SelectiveGapWriteCount

	// a real drop: Pack 4 is lost on the unreliable lane, 5..7 are acknowledged
	var afterDrop []*protocol.Pack
	for index := 4; index <= 7; index += 1 {
		sendFlightGateMessage(t, client, peerId, index)
		afterDrop = append(afterDrop, takeFlightGatePack(t, unreliable, 5*time.Second))
	}
	for _, pack := range afterDrop[1:] {
		ackFlightGatePack(t, client, peerId, fromPeer, pack, true)
	}
	resent := takeFlightGatePack(t, unreliable, 5*time.Second)
	if Id(resent.MessageId) != Id(afterDrop[0].MessageId) {
		t.Fatalf("gap recovery resent %v, want the dropped Pack", resent.MessageId)
	}
	recovery = client.SendRecoveryStats()
	if recovery.SelectiveGapWriteCount != gapWritesBefore+1 {
		t.Fatalf("real loss must produce exactly one gap recovery: %+v", recovery)
	}
}

// M4. Acknowledgements from the unreliable lane arrive in ~20 ms and from the
// reliable lane in ~200 ms. The sequence's scaled RTT must describe the
// reliable lane, since that is the lane whose RTO it drives. Expected red on
// the tree this was written against: one window averages both lanes and
// lands on its floor.
func TestSendSequenceRttWindowDescribesReliableLane(t *testing.T) {
	client, peerId, fromPeer, _ := newFlightGateSender(t, flightGateSettings(kib(64)))
	_, unreliable := addFlightGateRoute(t, client, TransportTypeP2p, 8, true)
	var unreliablePacks []*protocol.Pack
	for index := 0; index < 4; index += 1 {
		sendFlightGateMessage(t, client, peerId, index)
		unreliablePacks = append(unreliablePacks, takeFlightGatePack(t, unreliable, 5*time.Second))
	}
	// the direct lane answers in tens of milliseconds
	time.Sleep(20 * time.Millisecond)
	for _, pack := range unreliablePacks {
		ackFlightGatePack(t, client, peerId, fromPeer, pack, true)
	}
	fillFlightGateRoute(unreliable)
	_, reliable := addFlightGateRoute(t, client, TransportTypeH1, 16, false)
	var reliablePacks []*protocol.Pack
	sentAt := time.Now()
	for index := 4; index < 8; index += 1 {
		sendFlightGateMessage(t, client, peerId, index)
		reliablePacks = append(reliablePacks, takeFlightGatePack(t, reliable, 5*time.Second))
	}
	// the relay lane answers in a couple of hundred milliseconds
	time.Sleep(200*time.Millisecond - time.Since(sentAt))
	for _, pack := range reliablePacks[:3] {
		ackFlightGatePack(t, client, peerId, fromPeer, pack, true)
	}
	ackFlightGatePack(t, client, peerId, fromPeer, reliablePacks[3], false)
	time.Sleep(100 * time.Millisecond)

	sequence := flightGateSendSequence(t, client, peerId)
	scaled := sequence.rttWindow.ScaledRtt()
	minimum := time.Duration(float64(200*time.Millisecond) * 0.95 *
		float64(client.settings.SendBufferSettings.RttScale))
	if scaled < minimum {
		t.Fatalf("scaled RTT %s describes a blend of both lanes; want at least %s for the reliable lane", scaled, minimum)
	}
}

// M4 (F12 contract). Acknowledgements on a single reliable lane keep
// advancing the cumulative ack while their delay grows with a queue. No
// whole-window timeout resend may fire while that progress is recent. A lane
// that really stops must still resend. Expected red on the tree this was
// written against.
func TestSendSequenceQueueInflatedRelayRttDoesNotFireWholeWindowTimeouts(t *testing.T) {
	settings := flightGateSettings(kib(64))
	// the §13.5 contract, off by default until its PERFVAR A/B
	settings.SendBufferSettings.DeferTimeoutResendWhileCumulativeProgress = true
	client, peerId, fromPeer, _ := newFlightGateSender(t, settings)
	_, reliable := addFlightGateRoute(t, client, TransportTypeH1, 16, false)
	for index, delay := range []time.Duration{
		100 * time.Millisecond,
		250 * time.Millisecond,
		400 * time.Millisecond,
		700 * time.Millisecond,
	} {
		sendFlightGateMessage(t, client, peerId, index)
		pack := takeFlightGatePack(t, reliable, 5*time.Second)
		time.Sleep(delay)
		ackFlightGatePack(t, client, peerId, fromPeer, pack, false)
	}
	time.Sleep(100 * time.Millisecond)
	recovery := client.SendRecoveryStats()
	if recovery.TimeoutResendWriteCount != 0 {
		t.Fatalf("whole-window timeouts fired while the cumulative ack was advancing: %+v", recovery)
	}
	drainFlightGateRoute(reliable)

	// a stalled lane is still recovered
	sendFlightGateMessage(t, client, peerId, 8)
	takeFlightGatePack(t, reliable, 5*time.Second)
	takeFlightGatePack(t, reliable, 10*time.Second)
	if recovery := client.SendRecoveryStats(); recovery.TimeoutResendWriteCount == 0 {
		t.Fatalf("a lane with no acknowledgement was never resent: %+v", recovery)
	}
}

// Finding 1 of FLIGHTGATEFIX §4 (candidate G2). Forgetting an item from the
// unreliable flight on RTO must not grow the window the way an
// acknowledgement does. The primitive does not exist on this tree.
func TestSendFlightControllerForgetDoesNotGrowWindow(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.UnreliableInitialFlightByteCount = 4096
	settings.UnreliableMinimumFlightByteCount = 4096
	settings.UnreliableMaximumFlightByteCount = 65536
	settings.UnreliableInitialFlightMessageCount = 4
	settings.UnreliableMinimumFlightMessageCount = 2
	settings.UnreliableMaximumFlightMessageCount = 64
	controller := newSendFlightController(settings)
	controller.applyPolicy(transferFlightPolicySnapshot{generation: 1, limited: true})
	sequence := &SendSequence{client: &Client{}, flightController: controller}
	item := &sendItem{
		transferFrameBytes:      make([]byte, 1000),
		unreliableFlightTracked: true,
	}
	controller.send(item.MessageByteCount())
	controller.send(1000)
	// the RTO halves admission; whatever the release does afterwards must
	// not exceed that reduced limit, because a timeout is not delivery
	byteLimitBefore, messageLimitBefore := controller.byteLimit, controller.messageLimit
	sequence.observeUnreliableResendTimeout(
		item,
		transferFlightPolicySnapshot{limited: true, reliableRouteAvailable: true},
	)
	reducedByteLimit := max(controller.activeMinimumByteCount, byteLimitBefore/2)
	reducedMessageLimit := max(controller.activeMinimumMessageCount, messageLimitBefore/2)
	if item.unreliableFlightTracked || controller.byteCount != 1000 || controller.messageCount != 1 {
		t.Fatalf("RTO release did not remove the item: tracked=%v bytes=%d messages=%d",
			item.unreliableFlightTracked, controller.byteCount, controller.messageCount)
	}
	if reducedByteLimit < controller.byteLimit || reducedMessageLimit < controller.messageLimit {
		t.Fatalf("RTO release grew the window as if the item had been delivered: limits %d/%d, want at most %d/%d",
			controller.byteLimit, controller.messageLimit, reducedByteLimit, reducedMessageLimit)
	}
}

// R1 contract (M7). A receive callback that blocks must not block SendPacket:
// race-commit delivery has to be asynchronous. The reproduction needs a
// provider answer to arrive between race publication and commit, which the
// in-process multi-client harness cannot yet schedule deterministically.
func TestMultiClientRaceCommitDeliversAsynchronously(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	providerClient := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer providerClient.Cancel()
	release := make(chan struct{})
	delivered := make(chan []byte, 4)
	natClient, err := testingNewMultiClient(
		ctx,
		providerClient,
		func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
			// the consumer is parked, the way an injecting tun reader is
			<-release
			delivered <- append([]byte(nil), packet...)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer natClient.Close()
	multi := natClient.(*RemoteUserNatMultiClient)
	observed := make(chan int, 1)
	multi.settings.beforeRaceCommitDeliveryForTest = func(_ *multiClientChannel, packetCount int) {
		observed <- packetCount
	}
	template, _ := tcp4Packet(1, 0, 0, 0)
	burst := []*receivePacket{
		{ProvideMode: protocol.ProvideMode_Network, Packet: MessagePoolCopy(template), Pooled: true},
		{ProvideMode: protocol.ProvideMode_Network, Packet: MessagePoolCopy(template), Pooled: true},
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		multi.deliverRaceCommitPackets(nil, nil, burst)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("race-commit delivery blocked on the parked consumer")
	}
	if count := <-observed; count != 2 {
		t.Fatalf("burst observed = %d packets, want 2", count)
	}
	close(release)
	for index := 0; index < 2; index += 1 {
		select {
		case packet := <-delivered:
			if !bytes.Equal(packet, template) {
				t.Fatal("delivered packet differs from the buffered response")
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("buffered response %d was never delivered", index)
		}
	}
	if drops := multi.RaceCommitDeliveryDropCount(); drops != 0 {
		t.Fatalf("race-commit deliveries dropped: %d", drops)
	}
}

// P1 contract. Readiness of the direct lane must depend on measured probe
// quality relative to the platform path. No such gate exists on this tree.
func TestP2pReadinessRequiresProbeQuality(t *testing.T) {
	t.Skip("candidate P1 defines a probe-quality readiness gate in transport_p2p_probe")
}

// The §8 counters ride the send loop and the ack hot path; they must not
// allocate. reliableRouteHasCapacity is called once per gated iteration and
// recordReceiveAckRouteWrite once per ack write.
func TestFlightGateCountersAreAllocationFree(t *testing.T) {
	client, peerId, _, _ := newFlightGateSender(t, flightGateSettings(kib(64)))
	addFlightGateRoute(t, client, TransportTypeP2p, 4, true)
	addFlightGateRoute(t, client, TransportTypeH1, 4, false)
	// an independent writer for the destination sees the same route snapshot
	// the sequence's writer does, without touching the sequence's own field
	writer := client.RouteManager().OpenMultiRouteWriter(DestinationId(peerId))
	defer client.RouteManager().CloseMultiRouteWriter(writer)
	provider, ok := writer.(transferReliableCapacityProvider)
	if !ok {
		t.Fatal("writer does not report reliable capacity")
	}
	if !provider.reliableRouteHasCapacity() {
		t.Fatal("an empty reliable route reports no capacity")
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		provider.reliableRouteHasCapacity()
	}); allocs != 0 {
		t.Fatalf("reliableRouteHasCapacity allocates %.1f per call", allocs)
	}
	if allocs := testing.AllocsPerRun(1000, func() {
		client.recordReceiveAckRouteWrite(TransportTypeP2p, time.Millisecond, true, false, nil)
	}); allocs != 0 {
		t.Fatalf("recordReceiveAckRouteWrite allocates %.1f per call", allocs)
	}
	sequence := &SendSequence{client: client, contractMultiRouteWriter: writer}
	item := &sendItem{unreliableCarrierObserved: true, carrierRoute: make(Route, 1)}
	if allocs := testing.AllocsPerRun(1000, func() {
		sequence.observeItemAck(item)
	}); allocs != 0 {
		t.Fatalf("observeItemAck allocates %.1f per call", allocs)
	}
}

// MEMSTEADY gate for §13: the paths the items touch per packet or per ack
// allocate nothing. The reply decision (13.2), the flight forget (13.1),
// the reliable-only decision with the lossy cap (13.6), and the race-commit
// handoff (13.4) are measured here; the fast-path report (13.3) is measured
// against the warmup marker in flight_gate_p2p_test.go.
func TestFlightGateItemsAreAllocationFree(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	selector := NewMultiRouteSelector(ctx, "alloc-reply", nil, TransferPath{}, true)
	defer selector.Close()
	h1Route := make(Route, 16)
	p2pRoute := make(Route, 16)
	selector.updateTransportWithProperties(NewSendGatewayTransportWithType(TransportTypeH1), []Route{h1Route}, TransferCarrierProperties{})
	selector.updateTransportWithProperties(NewSendGatewayTransportWithType(TransportTypeP2p), []Route{p2pRoute}, TransferCarrierProperties{Unreliable: true})
	selector.observeRouteAckProgress(p2pRoute)
	frame := []byte{1}
	drain := func() {
		for _, route := range []Route{h1Route, p2pRoute} {
			for len(route) > 0 {
				<-route
			}
		}
	}
	if allocs := testing.AllocsPerRun(200, func() {
		drain()
		selector.writeDetailedReplyWithCarrierPreference(ctx, frame, time.Second, TransportTypeP2p)
	}); allocs != 0 {
		t.Fatalf("reply route decision allocates %.1f per reply", allocs)
	}
	fillFlightGateRoute(p2pRoute)
	if allocs := testing.AllocsPerRun(200, func() {
		for len(h1Route) > 0 {
			<-h1Route
		}
		selector.writeDetailedReplyWithCarrierPreference(ctx, frame, time.Second, TransportTypeP2p)
	}); allocs != 0 {
		t.Fatalf("reply fall-through allocates %.1f per reply", allocs)
	}
	drain()

	settings := DefaultSendBufferSettings()
	controller := newSendFlightController(settings)
	policy := transferFlightPolicySnapshot{generation: 1, limited: true, reliableRouteAvailable: true}
	controller.applyPolicy(policy)
	key := sendSchedulingKey{valid: true}
	if allocs := testing.AllocsPerRun(1000, func() {
		controller.sendForKey(1000, key)
		controller.forget(1000, key, false)
	}); allocs != 0 {
		t.Fatalf("forget allocates %.1f per call", allocs)
	}
	sequence := &SendSequence{client: &Client{}, flightController: controller, sendBufferSettings: settings}
	if allocs := testing.AllocsPerRun(1000, func() {
		sequence.reliableOnlyWrite(policy)
	}); allocs != 0 {
		t.Fatalf("reliable-only decision allocates %.1f per write", allocs)
	}

	// the handoff alone, measured on a bare multi-client whose queue no
	// worker drains: the caller's side of the race-commit path allocates
	// nothing (the worker's own delivery closure is not on this goroutine)
	bare := &RemoteUserNatMultiClient{
		ctx:                 ctx,
		settings:            DefaultMultiClientSettings(),
		removalReceiveQueue: make(chan receivePacket, 256),
	}
	template, _ := tcp4Packet(1, 0, 0, 0)
	burst := []*receivePacket{{ProvideMode: protocol.ProvideMode_Network, Packet: template}}
	if allocs := testing.AllocsPerRun(200, func() {
		bare.deliverRaceCommitPackets(nil, nil, burst)
		<-bare.removalReceiveQueue
	}); allocs != 0 {
		t.Fatalf("race-commit handoff allocates %.1f per burst", allocs)
	}
}

// FLIGHTGATEFIX §15 memory gate: the race-commit handoff (§13.4) holds its
// burst in the removal receive queue, so what it can retain is the queue's
// own bound, not the number of flows. On a phone the mobile memory policy
// clamps that queue to sixteen entries.
func TestRaceCommitHandoffRetentionIsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const queueSize = 8
	const burstCount = 64
	bare := &RemoteUserNatMultiClient{
		ctx:                 ctx,
		log:                 NewNoopLogger(),
		settings:            DefaultMultiClientSettings(),
		removalReceiveQueue: make(chan receivePacket, queueSize),
	}
	template, _ := tcp4Packet(1, 0, 0, 0)
	burst := make([]*receivePacket, 0, burstCount)
	for range burstCount {
		burst = append(burst, &receivePacket{
			ProvideMode: protocol.ProvideMode_Network,
			Packet:      MessagePoolCopy(template),
		})
	}
	bare.deliverRaceCommitPackets(nil, nil, burst)
	if retained := len(bare.removalReceiveQueue); retained != queueSize {
		t.Fatalf("the handoff retained %d packets, want the queue's %d", retained, queueSize)
	}
	if drops := bare.RaceCommitDeliveryDropCount(); drops != burstCount-queueSize {
		t.Fatalf("dropped %d packets past the bound, want %d", drops, burstCount-queueSize)
	}
	for len(bare.removalReceiveQueue) > 0 {
		packet := <-bare.removalReceiveQueue
		MessagePoolReturn(packet.Packet)
	}
}

// FLIGHTGATEFIX §15: the unreliable flight's byte ceiling is the memory
// budget and it binds on its own, so the message ceiling can only decide
// how much of that budget small messages are allowed to use. Raising the
// message ceiling therefore cannot retain more bytes than the byte ceiling
// already grants, which is what makes it the one window change that costs
// no retained memory.
func TestUnreliableFlightRetainedBytesAreBoundedByTheByteLimit(t *testing.T) {
	const byteLimit = 128 * 1024
	settings := DefaultSendBufferSettings()
	settings.UnreliableInitialFlightByteCount = byteLimit
	settings.UnreliableMinimumFlightByteCount = byteLimit
	settings.UnreliableMaximumFlightByteCount = byteLimit
	for _, messageLimit := range []int{16, 128, 1024} {
		settings.UnreliableInitialFlightMessageCount = messageLimit
		settings.UnreliableMinimumFlightMessageCount = messageLimit
		settings.UnreliableMaximumFlightMessageCount = messageLimit
		controller := newSendFlightController(settings)
		controller.applyPolicy(transferFlightPolicySnapshot{generation: 1, limited: true})
		// admit small messages until the flight refuses; nothing acknowledges
		admitted := 0
		for controller.canSend() && admitted < 4*messageLimit {
			controller.send(256)
			admitted += 1
		}
		if int(controller.byteCount) > byteLimit+256 {
			t.Fatalf(
				"message limit %d let the flight retain %d bytes, past the %d byte budget",
				messageLimit, controller.byteCount, byteLimit,
			)
		}
		if messageLimit <= admitted && controller.messageCount > messageLimit {
			t.Fatalf("message limit %d admitted %d messages", messageLimit, controller.messageCount)
		}
		t.Logf("message limit %d: admitted %d messages holding %d bytes of the %d byte budget",
			messageLimit, controller.messageCount, controller.byteCount, byteLimit)
	}
}
