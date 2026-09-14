package connect

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Two clients over the in-process gateway transports (the SimpleMessage
// pattern of transfer_test.go), exchanging a hand-rolled subprotocol and a
// protobuf one, with a raw listener alongside the typed handler, an
// unregistered id, the peer query over the companion reply, and an old-peer
// simulation where the receiver has no registry entry.
func TestSubprotocolTransfer(t *testing.T) {
	timeout := 2 * time.Minute
	n := 64

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	aClientId := NewId()
	bClientId := NewId()

	aSend := make(chan []byte)
	bSend := make(chan []byte)

	_, bReceive := newConditioner(ctx, aSend)
	_, aReceive := newConditioner(ctx, bSend)

	aSendTransport := NewSendGatewayTransport()
	aReceiveTransport := NewReceiveGatewayTransport()
	bSendTransport := NewSendGatewayTransport()
	bReceiveTransport := NewReceiveGatewayTransport()

	provideModes := map[protocol.ProvideMode]bool{
		protocol.ProvideMode_Network: true,
	}

	newTestClient := func(clientId Id) *Client {
		clientSettings := DefaultClientSettingsWithBufferSize(n)
		clientSettings.SendBufferSettings.AckTimeout = 300 * time.Second
		clientSettings.SendBufferSettings.IdleTimeout = 300 * time.Second
		clientSettings.ReceiveBufferSettings.GapTimeout = 300 * time.Second
		clientSettings.ReceiveBufferSettings.IdleTimeout = 300 * time.Second
		clientSettings.ForwardBufferSettings.IdleTimeout = 300 * time.Second
		clientSettings.ContractManagerSettings.LegacyCreateContract = true
		applyTestEncryptionSettings(clientSettings, encryptionModeOff)
		return NewClient(ctx, clientId, NewNoContractClientOob(), clientSettings)
	}

	a := newTestClient(aClientId)
	defer a.Cancel()
	a.RouteManager().UpdateTransport(aSendTransport, []Route{aSend})
	a.RouteManager().UpdateTransport(aReceiveTransport, []Route{aReceive})
	a.ContractManager().SetProvideModes(provideModes)

	b := newTestClient(bClientId)
	defer b.Cancel()
	b.RouteManager().UpdateTransport(bSendTransport, []Route{bSend})
	b.RouteManager().UpdateTransport(bReceiveTransport, []Route{bReceive})
	b.ContractManager().SetProvideModes(provideModes)

	// contracts in both directions so b can reply to a
	for _, direction := range []struct{ from, to *Client }{{a, b}, {b, a}} {
		err := direction.from.ContractManager().HandleControlFrame(
			ContractKey{Destination: DestinationId(direction.to.ClientId())},
			requireContractResult(
				protocol.ProvideMode_Network,
				direction.to.ContractManager().RequireProvideSecretKey(protocol.ProvideMode_Network),
				direction.from.ClientId(),
				direction.to.ClientId(),
			),
		)
		AssertEqual(t, err, nil)
	}

	asyncErrors := make(chan error, 8)
	recordAsyncError := func(err error) {
		select {
		case asyncErrors <- err:
		default:
		}
	}

	const fixedId SubprotocolId = 4000
	const protoId SubprotocolId = 4001
	const unregisteredId SubprotocolId = 4002

	rawReceives := make(chan string, 4*n)
	typedReceives := make(chan string, 4*n)
	protoReceives := make(chan string, 4*n)
	plainReceives := make(chan string, 4*n)

	fixedCodecB := &fixedCodec{}
	removeRaw, err := b.AddSubprotocolRawCallback(fixedId, func(source TransferPath, id SubprotocolId, messageBytes []byte, peer Peer) {
		if source.SourceId != aClientId || id != fixedId {
			recordAsyncError(fmt.Errorf("raw listener: source %s id %d", source.SourceId, id))
			return
		}
		select {
		case rawReceives <- string(messageBytes):
		default:
			recordAsyncError(fmt.Errorf("raw collector overflow"))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	unregisterFixed, err := RegisterSubprotocol[*fixedMessage](b, fixedId, fixedCodecB, func(source TransferPath, m *fixedMessage, peer Peer) {
		select {
		case typedReceives <- string(m.bytes()):
		default:
			recordAsyncError(fmt.Errorf("typed collector overflow"))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	unregisterProto, err := RegisterSubprotocol[*protocol.SimpleMessage](b, protoId, ProtoCodec[*protocol.SimpleMessage](), func(source TransferPath, m *protocol.SimpleMessage, peer Peer) {
		select {
		case protoReceives <- m.Content:
		default:
			recordAsyncError(fmt.Errorf("proto collector overflow"))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	// the generic callback must never see a subprotocol frame or a query
	b.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, peer Peer) {
		for _, frame := range frames {
			switch frame.MessageType {
			case protocol.MessageType_Subprotocol, protocol.MessageType_TransferSubprotocolsQuery, protocol.MessageType_TransferSubprotocolsQueryResult:
				recordAsyncError(fmt.Errorf("generic callback saw %s", frame.MessageType))
			case protocol.MessageType_TestSimpleMessage:
				m, err := FromFrame(frame)
				if err != nil {
					recordAsyncError(err)
					return
				}
				select {
				case plainReceives <- m.(*protocol.SimpleMessage).Content:
				default:
				}
			}
		}
	})

	acks := make(chan error, 8*n)
	ackCallback := func(err error) {
		select {
		case acks <- err:
		default:
			recordAsyncError(fmt.Errorf("ack collector overflow"))
		}
	}

	fixedCodecA := &fixedCodec{}
	sent := 0
	for i := 0; i < n; i += 1 {
		m := &fixedMessage{}
		m.set([]byte(fmt.Sprintf("fixed %d", i)))
		if !SendSubprotocol[*fixedMessage](a, fixedId, m, bClientId, ackCallback, WithSubprotocolCodec[*fixedMessage](fixedCodecA)) {
			t.Fatalf("send fixed %d", i)
		}
		sent += 1
		if !SendSubprotocol[*protocol.SimpleMessage](a, protoId, &protocol.SimpleMessage{Content: fmt.Sprintf("proto %d", i)}, bClientId, ackCallback, WithSubprotocolCodec[*protocol.SimpleMessage](ProtoCodec[*protocol.SimpleMessage]())) {
			t.Fatalf("send proto %d", i)
		}
		sent += 1
	}
	// bytes the receiver has no listener for: dropped and counted
	for i := 0; i < 4; i += 1 {
		if !a.SendSubprotocolBytes(unregisteredId, []byte("nobody"), bClientId, ackCallback) {
			t.Fatalf("send unregistered %d", i)
		}
		sent += 1
	}
	// a plain frame after them: the sequence stays healthy
	plainFrame, err := ToFrame(&protocol.SimpleMessage{Content: "plain"}, DefaultProtocolVersion)
	if err != nil {
		t.Fatal(err)
	}
	if !a.Send(plainFrame, bClientId, ackCallback) {
		t.Fatal("send plain")
	}
	sent += 1

	deadline := time.After(timeout)
	ackCount := 0
	rawCount, typedCount, protoCount := 0, 0, 0
	plainSeen := false
	for ackCount < sent || rawCount < n || typedCount < n || protoCount < n || !plainSeen {
		select {
		case err := <-asyncErrors:
			t.Fatal(err)
		case <-deadline:
			t.Fatalf("timeout: acks %d/%d raw %d typed %d proto %d plain %v", ackCount, sent, rawCount, typedCount, protoCount, plainSeen)
		case err := <-acks:
			AssertEqual(t, err, nil)
			ackCount += 1
		case content := <-rawReceives:
			AssertEqual(t, fmt.Sprintf("fixed %d", rawCount), content)
			rawCount += 1
		case content := <-typedReceives:
			AssertEqual(t, fmt.Sprintf("fixed %d", typedCount), content)
			typedCount += 1
		case content := <-protoReceives:
			AssertEqual(t, fmt.Sprintf("proto %d", protoCount), content)
			protoCount += 1
		case content := <-plainReceives:
			AssertEqual(t, "plain", content)
			plainSeen = true
		}
	}

	stats := b.SubprotocolStats()
	if stats.Received != uint64(2*n) || stats.DroppedUnregistered != 4 || stats.DroppedDecode != 0 {
		t.Fatalf("receiver stats %+v", stats)
	}
	if stats.ReceivedById[fixedId] != uint64(n) || stats.ReceivedById[protoId] != uint64(n) {
		t.Fatalf("receiver per-id stats %+v", stats.ReceivedById)
	}
	if aStats := a.SubprotocolStats(); aStats.Sent != uint64(2*n+4) {
		t.Fatalf("sender stats %+v", aStats)
	}

	// the peer query, answered over the companion reply path
	queryCtx, queryCancel := context.WithTimeout(ctx, timeout)
	defer queryCancel()
	ids, err := a.QuerySubprotocols(queryCtx, bClientId)
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(ids) != 2 || ids[0] != fixedId || ids[1] != protoId {
		t.Fatalf("query ids %v", ids)
	}
	if bStats := b.SubprotocolStats(); bStats.QueriesAnswered != 1 || bStats.QueryReplyDrops != 0 {
		t.Fatalf("query stats %+v", bStats)
	}

	// an old peer: b forgets its registry, so subprotocol frames from a are
	// dropped, and the sequence keeps delivering the plain frame after them
	removeRaw()
	unregisterFixed()
	unregisterProto()
	oldPeerAcks := make(chan error, 8)
	oldPeerAck := func(err error) {
		select {
		case oldPeerAcks <- err:
		default:
		}
	}
	for i := 0; i < 3; i += 1 {
		if !a.SendSubprotocolBytes(fixedId, []byte("unknown to b now"), bClientId, oldPeerAck) {
			t.Fatalf("send to old peer %d", i)
		}
	}
	afterFrame, err := ToFrame(&protocol.SimpleMessage{Content: "after"}, DefaultProtocolVersion)
	if err != nil {
		t.Fatal(err)
	}
	if !a.Send(afterFrame, bClientId, oldPeerAck) {
		t.Fatal("send after")
	}
	oldPeerAckCount := 0
	afterSeen := false
	for oldPeerAckCount < 4 || !afterSeen {
		select {
		case err := <-asyncErrors:
			t.Fatal(err)
		case <-deadline:
			t.Fatalf("old peer timeout: acks %d after %v", oldPeerAckCount, afterSeen)
		case err := <-oldPeerAcks:
			AssertEqual(t, err, nil)
			oldPeerAckCount += 1
		case content := <-plainReceives:
			AssertEqual(t, "after", content)
			afterSeen = true
		}
	}
	if bStats := b.SubprotocolStats(); bStats.DroppedUnregistered != 4+3 {
		t.Fatalf("old peer stats %+v", bStats)
	}
}
