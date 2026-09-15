// The sender declares a hole lost once SelectiveAckGapThreshold later
// selective acks are visible, and it snapshots its acks between Pack writes,
// so the order the receiver writes selective acks in decides which Packs a
// partial batch "proves" lost. These rows drive a real receiver and a real
// sender through captured wire frames: one Pack is dropped on the data half,
// the receiver's acknowledgement frames are captured off its route, and the
// sender is handed them one at a time with a full turn between each, which is
// the worst prefix split a busy sender can see. In the receiver's own wire
// order only the hole is retransmitted; the same acks fed in descending order
// are the control that shows the harness sees a spurious retransmit when the
// order permits one.
package connect

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

type gapAckWirePack struct {
	sequenceNumber uint64
	messageId      Id
}

type gapAckWireAck struct {
	frame     []byte
	messageId Id
}

// gapAckWireFixture joins a sender and a receiver by four routes the test
// relays by hand, so it owns every frame in flight on both halves.
type gapAckWireFixture struct {
	sender      *Client
	receiver    *Client
	receiverId  Id
	senderOut   Route
	senderIn    Route
	receiverIn  Route
	receiverOut Route
	// one token per sender pass that applied acknowledgements, after that
	// pass's recovery scans and due resend writes
	ackPasses chan struct{}
	// the sequence number of every item the sender resent
	resends chan uint64
}

func newGapAckWireFixture(t *testing.T) *gapAckWireFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	newSettings := func() *ClientSettings {
		settings := DefaultClientSettings()
		settings.Log = NewNoopLogger()
		settings.EncryptionSettings.Mode = EncryptionModeOff
		settings.SendBufferSettings.AckTimeout = 60 * time.Second
		settings.SendBufferSettings.IdleTimeout = 60 * time.Second
		// the rows are about receiver evidence: no timer may retransmit
		// anything while the sender is being handed acknowledgements
		settings.SendBufferSettings.RttMinResendInterval = 30 * time.Second
		settings.SendBufferSettings.MaxResendInterval = 60 * time.Second
		settings.ReceiveBufferSettings.GapTimeout = 60 * time.Second
		settings.ReceiveBufferSettings.IdleTimeout = 60 * time.Second
		return settings
	}
	ackPasses := make(chan struct{}, 256)
	resends := make(chan uint64, 256)
	senderSettings := newSettings()
	senderSettings.SendBufferSettings.afterAckPassForTest = func(sendSequenceId) {
		select {
		case ackPasses <- struct{}{}:
		default:
		}
	}
	senderSettings.SendBufferSettings.beforeDueResendForTest = func(_ sendSequenceId, sequenceNumber uint64) {
		select {
		case resends <- sequenceNumber:
		default:
		}
	}

	senderId := NewId()
	receiverId := NewId()
	sender := NewClient(ctx, senderId, NewNoContractClientOob(), senderSettings)
	receiver := NewClient(ctx, receiverId, NewNoContractClientOob(), newSettings())
	sender.ContractManager().AddNoContractPeer(receiverId)
	receiver.ContractManager().AddNoContractPeer(senderId)

	senderOut := make(Route, 256)
	senderIn := make(Route, 256)
	receiverIn := make(Route, 256)
	receiverOut := make(Route, 256)
	sender.RouteManager().UpdateTransport(NewSendGatewayTransport(), []Route{senderOut})
	sender.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{senderIn})
	receiver.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{receiverIn})
	receiver.RouteManager().UpdateTransport(NewSendGatewayTransport(), []Route{receiverOut})
	receiver.AddReceiveCallback(func(TransferPath, []*protocol.Frame, Peer) {})

	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := sender.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close the sender: %v", err)
		}
		if err := receiver.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close the receiver: %v", err)
		}
		for _, route := range []Route{senderOut, senderIn, receiverIn, receiverOut} {
			for draining := true; draining; {
				select {
				case transferFrameBytes := <-route:
					MessagePoolReturn(transferFrameBytes)
				default:
					draining = false
				}
			}
		}
	})

	return &gapAckWireFixture{
		sender:      sender,
		receiver:    receiver,
		receiverId:  receiverId,
		senderOut:   senderOut,
		senderIn:    senderIn,
		receiverIn:  receiverIn,
		receiverOut: receiverOut,
		ackPasses:   ackPasses,
		resends:     resends,
	}
}

func (self *gapAckWireFixture) sendMessage(t *testing.T, index int) {
	t.Helper()
	frame, err := ToFrame(
		&protocol.SimpleMessage{Content: fmt.Sprintf("gap-%d", index)},
		DefaultProtocolVersion,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !self.sender.SendWithTimeout(frame, self.receiverId, nil, 5*time.Second) {
		MessagePoolReturn(frame.MessageBytes)
		t.Fatalf("message %d was not admitted", index)
	}
}

func (self *gapAckWireFixture) takeFrame(t *testing.T, route Route, what string) []byte {
	t.Helper()
	select {
	case transferFrameBytes := <-route:
		return transferFrameBytes
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return nil
	}
}

func (self *gapAckWireFixture) forward(t *testing.T, route Route, transferFrameBytes []byte) {
	t.Helper()
	select {
	case route <- transferFrameBytes:
	case <-time.After(5 * time.Second):
		MessagePoolReturn(transferFrameBytes)
		t.Fatal("could not forward a frame")
	}
}

// feedAck hands one acknowledgement frame to the sender and waits for the
// pass that applied it, so the next frame lands in a fresh snapshot.
func (self *gapAckWireFixture) feedAck(t *testing.T, transferFrameBytes []byte) {
	t.Helper()
	self.forward(t, self.senderIn, transferFrameBytes)
	select {
	case <-self.ackPasses:
	case <-time.After(5 * time.Second):
		t.Fatal("the sender did not take a pass on the acknowledgement")
	}
}

// takeResends returns the sequence numbers resent so far, as a sorted set.
func (self *gapAckWireFixture) takeResends() []uint64 {
	resent := []uint64{}
	for {
		select {
		case sequenceNumber := <-self.resends:
			resent = append(resent, sequenceNumber)
		default:
			slices.Sort(resent)
			return slices.Compact(resent)
		}
	}
}

// decodeGapAckWirePack decodes a data-half frame without releasing it. The
// index is that of the application message the Pack carries, or -1 for a
// frame carrying none (an empty head Pack or a client key announcement).
func decodeGapAckWirePack(t *testing.T, transferFrameBytes []byte) (*protocol.Pack, int) {
	t.Helper()
	var transferFrame protocol.TransferFrame
	if err := ProtoUnmarshal(transferFrameBytes, &transferFrame); err != nil {
		t.Fatalf("decode TransferFrame: %v", err)
	}
	pack := transferFrame.Pack
	if pack == nil {
		frame := transferFrame.GetFrame()
		if frame == nil || frame.GetMessageType() != protocol.MessageType_TransferPack {
			return nil, -1
		}
		pack = &protocol.Pack{}
		if err := ProtoUnmarshal(frame.MessageBytes, pack); err != nil {
			t.Fatalf("decode Pack: %v", err)
		}
	}
	for _, frame := range pack.Frames {
		if frame.GetMessageType() != protocol.MessageType_TestSimpleMessage {
			continue
		}
		message, err := FromFrame(frame)
		if err != nil {
			t.Fatalf("decode application frame: %v", err)
		}
		simpleMessage, ok := message.(*protocol.SimpleMessage)
		if !ok {
			continue
		}
		var index int
		if n, _ := fmt.Sscanf(simpleMessage.Content, "gap-%d", &index); n == 1 {
			return pack, index
		}
	}
	return pack, -1
}

// decodeGapAckWireAck decodes an acknowledgement-half frame without releasing
// it. ok is false for a frame that is not an acknowledgement.
func decodeGapAckWireAck(t *testing.T, transferFrameBytes []byte) (messageId Id, selective bool, ok bool) {
	t.Helper()
	var transferFrame protocol.TransferFrame
	if err := ProtoUnmarshal(transferFrameBytes, &transferFrame); err != nil {
		t.Fatalf("decode ack TransferFrame: %v", err)
	}
	if transferFrame.Ack == nil {
		return Id{}, false, false
	}
	messageId, err := IdFromBytes(transferFrame.Ack.MessageId)
	if err != nil {
		t.Fatalf("ack message id: %v", err)
	}
	return messageId, transferFrame.Ack.Selective, true
}

// runGapAckWireRound sends packCount messages, drops the Pack at hole on the
// data half, captures the receiver's acknowledgements, feeds the sender the
// cumulative ones and then the selective ones in the given order with a turn
// between each, and reports which sequence numbers the sender resent. The
// hole's retransmit is then delivered so the round completes.
func runGapAckWireRound(
	t *testing.T,
	packCount int,
	hole int,
	order func(selective []gapAckWireAck) []gapAckWireAck,
) (resent []uint64, packs []gapAckWirePack) {
	t.Helper()
	fixture := newGapAckWireFixture(t)

	packs = make([]gapAckWirePack, packCount)
	for index := range packCount {
		fixture.sendMessage(t, index)
		for {
			transferFrameBytes := fixture.takeFrame(t, fixture.senderOut, fmt.Sprintf("Pack %d", index))
			pack, packIndex := decodeGapAckWirePack(t, transferFrameBytes)
			if packIndex < 0 {
				fixture.forward(t, fixture.receiverIn, transferFrameBytes)
				continue
			}
			if packIndex != index {
				t.Fatalf("Pack carried message %d, want %d", packIndex, index)
			}
			messageId, err := IdFromBytes(pack.MessageId)
			if err != nil {
				t.Fatal(err)
			}
			packs[index] = gapAckWirePack{sequenceNumber: pack.SequenceNumber, messageId: messageId}
			if index == hole {
				MessagePoolReturn(transferFrameBytes)
			} else {
				fixture.forward(t, fixture.receiverIn, transferFrameBytes)
			}
			break
		}
	}
	sequenceNumberOf := map[Id]uint64{}
	for _, pack := range packs {
		sequenceNumberOf[pack.messageId] = pack.sequenceNumber
	}

	// The receiver's acknowledgements: the cumulative ack for the Pack below
	// the hole and one selective ack per Pack it holds above the hole. A held
	// Pack is acknowledged on arrival and a delivered one only after delivery,
	// so the cumulative ack can land anywhere among the selective ones; the
	// capture waits for all of them in whatever order they were written.
	cumulative := [][]byte{}
	selective := []gapAckWireAck{}
	sawDeliveredAck := false
	for len(selective) < packCount-hole-1 || !sawDeliveredAck {
		transferFrameBytes := fixture.takeFrame(t, fixture.receiverOut, "a receiver acknowledgement")
		messageId, isSelective, isAck := decodeGapAckWireAck(t, transferFrameBytes)
		if !isAck {
			fixture.forward(t, fixture.senderIn, transferFrameBytes)
			continue
		}
		if isSelective {
			selective = append(selective, gapAckWireAck{frame: transferFrameBytes, messageId: messageId})
		} else {
			cumulative = append(cumulative, transferFrameBytes)
			if messageId == packs[hole-1].messageId {
				sawDeliveredAck = true
			}
		}
	}
	wireOrder := make([]uint64, 0, len(selective))
	for _, ack := range selective {
		sequenceNumber, known := sequenceNumberOf[ack.messageId]
		if !known {
			t.Fatalf("selective ack for an unknown message %s", ack.messageId)
		}
		wireOrder = append(wireOrder, sequenceNumber)
	}
	if !slices.IsSorted(wireOrder) {
		t.Fatalf("the receiver wrote selective acks out of sequence order: %v", wireOrder)
	}
	if packs[hole].sequenceNumber >= wireOrder[0] {
		t.Fatalf("selective acks %v are not all above the hole %d", wireOrder, packs[hole].sequenceNumber)
	}

	// the sender takes the delivered prefix first, then one selective ack per
	// turn in the order under test
	for _, transferFrameBytes := range cumulative {
		fixture.feedAck(t, transferFrameBytes)
	}
	if early := fixture.takeResends(); len(early) != 0 {
		t.Fatalf("the cumulative acknowledgements alone resent %v", early)
	}
	for _, ack := range order(selective) {
		fixture.feedAck(t, ack.frame)
	}
	resent = fixture.takeResends()

	// complete the round: the hole's retransmit reaches the receiver, whose
	// cumulative ack for the last Pack reaches the sender
	for {
		transferFrameBytes := fixture.takeFrame(t, fixture.senderOut, "the hole's retransmit")
		pack, _ := decodeGapAckWirePack(t, transferFrameBytes)
		if pack != nil {
			if messageId, err := IdFromBytes(pack.MessageId); err == nil && messageId == packs[hole].messageId {
				fixture.forward(t, fixture.receiverIn, transferFrameBytes)
				break
			}
		}
		MessagePoolReturn(transferFrameBytes)
	}
	for {
		transferFrameBytes := fixture.takeFrame(t, fixture.receiverOut, "the final cumulative acknowledgement")
		messageId, isSelective, isAck := decodeGapAckWireAck(t, transferFrameBytes)
		if !isAck || isSelective {
			MessagePoolReturn(transferFrameBytes)
			continue
		}
		fixture.forward(t, fixture.senderIn, transferFrameBytes)
		if messageId == packs[packCount-1].messageId {
			break
		}
	}
	select {
	case <-fixture.ackPasses:
	case <-time.After(5 * time.Second):
		t.Fatal("the sender did not take a pass on the final acknowledgement")
	}
	return resent, packs
}

// One real hole under five later selective acks. In the receiver's wire order
// each prefix the sender sees proves only the hole, so only the hole is
// retransmitted; fed in descending order the third prefix proves the two
// Packs above the hole as well, and the sender retransmits them needlessly.
// The two arms are the same fixture with the order changed.
func TestGapAckPrefixesRecoverOnlyMissingPacks(t *testing.T) {
	const packCount = 7
	const hole = 1

	t.Run("receiver wire order", func(t *testing.T) {
		resent, packs := runGapAckWireRound(t, packCount, hole, func(selective []gapAckWireAck) []gapAckWireAck {
			return selective
		})
		want := []uint64{packs[hole].sequenceNumber}
		if !slices.Equal(resent, want) {
			t.Fatalf("the sender resent %v, want only the hole %v", resent, want)
		}
	})

	t.Run("descending control", func(t *testing.T) {
		resent, packs := runGapAckWireRound(t, packCount, hole, func(selective []gapAckWireAck) []gapAckWireAck {
			descending := slices.Clone(selective)
			slices.Reverse(descending)
			return descending
		})
		want := []uint64{packs[hole].sequenceNumber, packs[hole+1].sequenceNumber, packs[hole+2].sequenceNumber}
		if !slices.Equal(resent, want) {
			t.Fatalf("with descending acks the sender resent %v, want the hole and its two neighbours %v", resent, want)
		}
	})
}
