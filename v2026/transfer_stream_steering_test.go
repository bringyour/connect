// Tests pin path-independent destination routing, receiver-visible lane keys,
// acknowledgement routing, and teardown ordering around stream fast paths.
package connect

import (
	"context"
	"net"
	"slices"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Keeps the writer keyed by final destination while a streamed contract adds
// a temporary stream alias for a P2P transport terminating at an adjacent hop.
func TestSendSequenceContractUsesFinalDestinationWithStreamAlias(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	finalDestinationId := NewId()
	adjacentPeerId := NewId()
	streamId := NewId()
	destination := DestinationId(finalDestinationId)

	sendBufferSettings := DefaultSendBufferSettings()
	sendBuffer := NewSendBuffer(ctx, client, sendBufferSettings)
	seq := NewSendSequence(
		ctx,
		client,
		sendBuffer,
		finalDestinationId,
		MultiHopId{},
		false,
		false,
		false,
		sequenceTlsRoleClient,
		false,
		sendBufferSettings,
	)
	defer seq.closeContractMultiRouteWriter()

	gatewayRoute := make(chan []byte, 32)
	gatewayTransport := NewSendGatewayTransport()
	client.RouteManager().UpdateTransport(gatewayTransport, []Route{gatewayRoute})
	defer client.RouteManager().RemoveTransport(gatewayTransport)

	localConn, remoteConn := net.Pipe()
	defer localConn.Close()
	defer remoteConn.Close()
	transportCtx, transportCancel := context.WithCancel(ctx)
	defer transportCancel()
	p2pTransport, p2pRoute := NewP2pSendTransportForPeer(
		transportCtx,
		transportCancel,
		localConn,
		adjacentPeerId,
		streamId,
		DefaultP2pTransportSettings(),
	)
	client.RouteManager().UpdateTransport(p2pTransport, []Route{p2pRoute})
	defer client.RouteManager().RemoveTransport(p2pTransport)

	writer := seq.openContractMultiRouteWriter()
	initialWriter := writer
	AssertEqual(t, destination, seq.contractMultiRouteWriterDestination)
	AssertEqual(t, 1, len(writer.GetActiveRoutes()))

	newContract := func(withStream bool) *sequenceContract {
		stored := &protocol.StoredContract{
			ContractId:        NewId().Bytes(),
			TransferByteCount: uint64(1024 * 1024),
			SourceId:          client.ClientId().Bytes(),
			DestinationId:     finalDestinationId.Bytes(),
		}
		if withStream {
			stored.StreamId = streamId.Bytes()
		}
		storedBytes, err := proto.Marshal(stored)
		AssertEqual(t, err, nil)
		contract, err := newSequenceContract(
			client.log,
			"s",
			&protocol.Contract{StoredContractBytes: storedBytes},
			sendBufferSettings.MinMessageByteCount,
			1.0,
		)
		AssertEqual(t, err, nil)
		return contract
	}

	seq.sendContract = newContract(true)
	writer = seq.openContractMultiRouteWriter()
	if initialWriter != writer {
		t.Fatal("stream alias replaced the destination writer")
	}
	AssertEqual(t, destination, seq.contractMultiRouteWriterDestination)
	AssertEqual(t, true, seq.sendContract.path.IsStream())
	AssertEqual(t, 2, len(writer.GetActiveRoutes()))
	func() {
		client.RouteManager().mutex.Lock()
		defer client.RouteManager().mutex.Unlock()

		aliases := client.RouteManager().writerMatchState.destinationAliases[destination]
		AssertEqual(t, 1, aliases[StreamId(streamId)])
	}()

	message := []byte("final destination through adjacent stream")
	err := writer.Write(ctx, MessagePoolCopy(message), time.Second)
	AssertEqual(t, nil, err)
	remoteConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buffer := make([]byte, 64)
	readByteCount, err := remoteConn.Read(buffer)
	AssertEqual(t, nil, err)
	AssertEqual(t, message, buffer[:readByteCount])

	seq.sendContract = newContract(false)
	writer = seq.openContractMultiRouteWriter()
	if initialWriter != writer {
		t.Fatal("stream alias removal replaced the destination writer")
	}
	AssertEqual(t, destination, seq.contractMultiRouteWriterDestination)
	AssertEqual(t, 1, len(writer.GetActiveRoutes()))
	func() {
		client.RouteManager().mutex.Lock()
		defer client.RouteManager().mutex.Unlock()

		if _, ok := client.RouteManager().writerMatchState.destinationAliases[destination]; ok {
			t.Fatal("unstamped contract retained the stream alias")
		}
	}()
}

// Maps every ReceiveBuffer discriminator into immutable callback metadata.
// Transport-local StreamId is normalized away, while every receiver-visible
// lane field remains distinct.
func TestReceiveTransferKeySeparatesReceiverVisibleLanes(t *testing.T) {
	sourceId := NewId()
	streamId := NewId()
	base := receiveSequenceHeadKey{
		Source:         TransferPath{SourceId: sourceId, StreamId: streamId},
		EncryptionRole: sequenceTlsRoleClient,
	}
	baseKey := base.transferKey()
	AssertEqual(t, protocol.SequenceRole_SequenceRoleClient, baseKey.EncryptionRole)

	keys := []TransferKey{baseKey}
	variants := []receiveSequenceHeadKey{base, base, base, base, base}
	variants[0].ForceStream = true
	variants[1].CompanionContract = true
	variants[2].EncryptionRole = sequenceTlsRoleServer
	variants[3].EncryptionCompanion = true
	variants[4].LogicalLane = 4
	for _, variant := range variants {
		key := variant.transferKey()
		for _, prior := range keys {
			if key == prior {
				t.Fatal("distinct receiver-visible lanes collapsed to one TransferKey")
			}
		}
		keys = append(keys, key)
	}
}

// Preserves one sequence key through the optimistic head path, buffered
// delivery, and the combined callback used for a drained batch.
func TestReceiveTransferKeySurvivesBufferedBatch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Cancel()
	expectedSource := SourceId(NewId())
	key := TransferKey{
		ForceStream:         true,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: true,
		LogicalLane:         4,
	}
	receiveSequence := newReceiveSequence(
		ctx,
		client,
		expectedSource,
		NewId(),
		key,
		DefaultReceiveBufferSettings(),
	)
	receiveSequence.peerAudit = NewSequencePeerAudit(client, expectedSource, 0)

	callbackCount := 0
	callback := func(source TransferPath, frames []*protocol.Frame, peer Peer) {
		callbackCount += 1
		AssertEqual(t, expectedSource, source)
		AssertEqual(t, key, peer.TransferKey)
		AssertEqual(t, 2, len(frames))
	}
	for i := 0; i < 2; i += 1 {
		receiveSequence.receiveHead(&receiveItem{
			frames: []*protocol.Frame{{
				MessageType: protocol.MessageType_IpIpPacketToProvider,
			}},
			receiveCallback: callback,
		})
	}
	receiveSequence.flushDeliver()
	AssertEqual(t, 1, callbackCount)
}

// ACKs select an active P2P route by peer destination alone. The local
// StreamId never enters the destination or serialized TransferPath.
func TestReceiveAckUsesDestinationOnlyRoute(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientId := NewId()
	peerId := NewId()
	streamId := NewId()
	source := SourceId(peerId)
	ackDestination := source.Reverse()
	AssertEqual(t, DestinationId(peerId), ackDestination)
	AssertEqual(t, NewTransferPath(clientId, peerId, Id{}), sendTransferPath(clientId, ackDestination))

	routeManager := NewRouteManager(ctx, "ack stream route")
	gatewayRoute := make(chan []byte, 32)
	gatewayTransport := NewSendGatewayTransport()
	routeManager.UpdateTransport(gatewayTransport, []Route{gatewayRoute})
	defer routeManager.RemoveTransport(gatewayTransport)

	localConn, remoteConn := net.Pipe()
	defer localConn.Close()
	defer remoteConn.Close()
	transportCtx, transportCancel := context.WithCancel(ctx)
	defer transportCancel()
	p2pTransport, p2pRoute := NewP2pSendTransportForPeer(
		transportCtx,
		transportCancel,
		localConn,
		peerId,
		streamId,
		DefaultP2pTransportSettings(),
	)
	routeManager.UpdateTransport(p2pTransport, []Route{p2pRoute})
	defer routeManager.RemoveTransport(p2pTransport)

	ackWriter := routeManager.OpenMultiRouteWriter(ackDestination)
	defer routeManager.CloseMultiRouteWriter(ackWriter)
	AssertEqual(t, 2, len(ackWriter.GetActiveRoutes()))
	AssertEqual(t, 0, len(ackWriter.GetInactiveRoutes()))

	message := []byte("ack by destination")
	err := ackWriter.Write(ctx, MessagePoolCopy(message), time.Second)
	AssertEqual(t, nil, err)

	remoteConn.SetReadDeadline(time.Now().Add(5 * time.Second))
	buffer := make([]byte, 64)
	readByteCount, err := remoteConn.Read(buffer)
	AssertEqual(t, nil, err)
	AssertEqual(t, message, buffer[:readByteCount])
	select {
	case gatewayMessage := <-gatewayRoute:
		MessagePoolReturn(gatewayMessage)
		t.Fatal("destination-matched ACK spilled onto the platform route")
	default:
	}
}

// The first verified streamed receive contract exposes its final source to the
// destination-only ACK writer before any return send sequence or contract
// exists. Sequence teardown removes that shared route reference.
func TestReceiveFirstStreamedContractRoutesAckAndRemovesAlias(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	settings.ReceiveBufferSettings.AckCompressTimeout = time.Millisecond
	settings.ReceiveBufferSettings.WriteTimeout = time.Second
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()
	client.ContractManager().SetProvideModesWithReturnTraffic(
		map[protocol.ProvideMode]bool{
			protocol.ProvideMode_Network: true,
		},
	)

	sourceId := NewId()
	streamId := NewId()
	source := SourceId(sourceId)
	receiveSequence := NewReceiveSequence(
		ctx,
		client,
		source,
		NewId(),
		sequenceTlsRoleServer,
		false,
		settings.ReceiveBufferSettings,
	)
	receiveSequence.peerAudit = NewSequencePeerAudit(client, source, 0)
	contract := streamManagerVerifiedContract(
		t,
		client,
		protocol.ProvideMode_Network,
		sourceId,
		streamId,
	)
	contractFrame, err := ToFrame(contract, DefaultProtocolVersion)
	AssertEqual(t, nil, err)
	AssertEqual(t, nil, receiveSequence.registerContracts(&receiveItem{
		contractFrame: contractFrame,
	}))

	ackDestination := DestinationId(sourceId)
	func() {
		client.RouteManager().mutex.Lock()
		defer client.RouteManager().mutex.Unlock()

		aliases := client.RouteManager().writerMatchState.destinationAliases[ackDestination]
		AssertEqual(t, 1, aliases[StreamId(streamId)])
	}()
	func() {
		client.sendBuffer.mutex.Lock()
		defer client.sendBuffer.mutex.Unlock()

		for id := range client.sendBuffer.sendSequences {
			if id.Destination == sourceId {
				t.Fatal("receive contract unexpectedly created a return send sequence")
			}
		}
	}()

	localConn, remoteConn := net.Pipe()
	defer localConn.Close()
	defer remoteConn.Close()
	transportCtx, transportCancel := context.WithCancel(ctx)
	defer transportCancel()
	transport, route := NewP2pSendTransportForPeer(
		transportCtx,
		transportCancel,
		localConn,
		NewId(),
		streamId,
		DefaultP2pTransportSettings(),
	)
	client.RouteManager().UpdateTransport(transport, []Route{route})
	defer client.RouteManager().RemoveTransport(transport)

	go receiveSequence.Run()
	messageId := NewId()
	receiveSequence.sendAck(
		1,
		messageId,
		false,
		sequenceTag{},
		true,
		TransportTypeUnknown,
	)
	AssertEqual(t, nil, remoteConn.SetReadDeadline(time.Now().Add(5*time.Second)))
	buffer := make([]byte, 4096)
	readByteCount, err := remoteConn.Read(buffer)
	AssertEqual(t, nil, err)
	transferFrame := &protocol.TransferFrame{}
	AssertEqual(t, nil, ProtoUnmarshal(buffer[:readByteCount], transferFrame))
	if transferFrame.Ack == nil {
		t.Fatal("stream route did not carry an ACK")
	}
	AssertEqual(t, messageId.Bytes(), transferFrame.Ack.MessageId)

	receiveSequence.Cancel()
	receiveSequence.WaitForExit()
	func() {
		client.RouteManager().mutex.Lock()
		defer client.RouteManager().mutex.Unlock()

		if _, ok := client.RouteManager().writerMatchState.destinationAliases[ackDestination]; ok {
			t.Fatal("receive sequence teardown retained its ACK stream alias")
		}
	}()
}

// A Transfer ACK follows the carrier that delivered the covered Pack. This
// avoids feeding an H3 download's control loop through an independently chosen
// H1 queue. If that carrier is withdrawn before the ACK is written, the
// selector immediately falls back to the remaining healthy route.
func TestReceiveAckFollowsInboundTransportAndFallsBackAfterWithdrawal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	settings.ReceiveBufferSettings.AckCompressTimeout = time.Millisecond
	settings.ReceiveBufferSettings.WriteTimeout = time.Second
	ackWriterOpened := make(chan struct{})
	settings.ReceiveBufferSettings.afterAckWriterOpenForTest = func(
		receiveSequenceId,
		MultiRouteWriter,
	) {
		close(ackWriterOpened)
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	h1Transport := NewSendGatewayTransportWithType(TransportTypeH1)
	h1Route := make(Route, 8)
	client.RouteManager().UpdateTransport(h1Transport, []Route{h1Route})
	defer client.RouteManager().RemoveTransport(h1Transport)
	h3Transport := NewSendGatewayTransportWithType(TransportTypeH3)
	h3Route := make(Route, 8)
	client.RouteManager().UpdateTransport(h3Transport, []Route{h3Route})

	receiveSequence := NewReceiveSequence(
		ctx,
		client,
		SourceId(NewId()),
		NewId(),
		sequenceTlsRoleServer,
		false,
		settings.ReceiveBufferSettings,
	)
	go receiveSequence.Run()
	defer func() {
		receiveSequence.Cancel()
		receiveSequence.WaitForExit()
	}()
	select {
	case <-ackWriterOpened:
	case <-time.After(time.Second):
		t.Fatal("ACK writer did not open")
	}
	isAckFor := func(message []byte, messageId Id) bool {
		transferFrame := &protocol.TransferFrame{}
		if err := ProtoUnmarshal(message, transferFrame); err != nil || transferFrame.Ack == nil {
			return false
		}
		ackMessageId, err := IdFromBytes(transferFrame.Ack.MessageId)
		return err == nil && ackMessageId == messageId
	}

	h3AckMessageId := NewId()
	receiveSequence.sendAck(
		1,
		h3AckMessageId,
		false,
		sequenceTag{},
		true,
		TransportTypeH3,
	)
	h3Deadline := time.After(time.Second)
	for {
		select {
		case message := <-h3Route:
			matched := isAckFor(message, h3AckMessageId)
			MessagePoolReturn(message)
			if matched {
				goto h3AckReceived
			}
		case message := <-h1Route:
			matched := isAckFor(message, h3AckMessageId)
			MessagePoolReturn(message)
			if matched {
				t.Fatal("H3-delivered Pack emitted its ACK on H1")
			}
		case <-h3Deadline:
			t.Fatal("H3-affine ACK was not written")
		}
	}

h3AckReceived:

	client.RouteManager().RemoveTransport(h3Transport)
	fallbackAckMessageId := NewId()
	receiveSequence.sendAck(
		2,
		fallbackAckMessageId,
		false,
		sequenceTag{},
		true,
		TransportTypeH3,
	)
	fallbackDeadline := time.After(time.Second)
	for {
		select {
		case message := <-h1Route:
			matched := isAckFor(message, fallbackAckMessageId)
			MessagePoolReturn(message)
			if matched {
				return
			}
		case message := <-h3Route:
			matched := isAckFor(message, fallbackAckMessageId)
			MessagePoolReturn(message)
			if matched {
				t.Fatal("withdrawn H3 route still carried an ACK")
			}
		case <-fallbackDeadline:
			t.Fatal("ACK did not fall back after H3 withdrawal")
		}
	}
}

// A verified wire contract records the final peer on the endpoint stream.
// That route knowledge must outlive the transient ReceiveSequence, transfer
// across overlapping StreamSequence generations, and rematch a replacement
// endpoint transport until an authoritative stream retirement clears it.
func TestVerifiedReceiveContractAliasSurvivesSequenceAndStreamGenerations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()
	client.ContractManager().SetProvideModesWithReturnTraffic(
		map[protocol.ProvideMode]bool{
			protocol.ProvideMode_Network: true,
		},
	)

	finalSourceId := NewId()
	adjacentPeerId := NewId()
	streamId := NewId()
	routeManager := client.RouteManager()
	gatewayRoute := make(Route, 8)
	gatewayTransport := NewSendGatewayTransport()
	routeManager.UpdateTransport(gatewayTransport, []Route{gatewayRoute})
	defer routeManager.RemoveTransport(gatewayTransport)
	writer := routeManager.OpenMultiRouteWriter(DestinationId(finalSourceId))
	defer routeManager.CloseMultiRouteWriter(writer)
	if got := len(writer.GetActiveRoutes()); got != 1 {
		t.Fatalf("initial platform routes=%d, want 1", got)
	}

	closeScope1 := routeManager.openWriterStreamAliasScope(streamId)
	probeSettings := DefaultP2pTransportSettings()
	probeSettings.EndToEndProbeInterval = time.Hour
	probeSettings.EndToEndProbeTimeout = 2 * time.Hour
	streamRoute1 := make(Route, 8)
	streamTransport1 := newP2pProbeTestSendTransport(
		adjacentPeerId,
		streamId,
		streamRoute1,
		probeSettings,
	)
	routeManager.UpdateTransport(streamTransport1, []Route{streamRoute1})
	probe := newP2pStreamProbe(ctx, routeManager, streamId, probeSettings)
	probe.setSendRoute(streamTransport1, streamRoute1)
	probeOpen := true
	defer func() {
		if probeOpen {
			probe.close()
		}
	}()

	receiveSequence := NewReceiveSequence(
		ctx,
		client,
		SourceId(finalSourceId),
		NewId(),
		sequenceTlsRoleServer,
		false,
		settings.ReceiveBufferSettings,
	)
	receiveSequence.peerAudit = NewSequencePeerAudit(
		client,
		SourceId(finalSourceId),
		0,
	)
	contract := streamManagerVerifiedContract(
		t,
		client,
		protocol.ProvideMode_Network,
		finalSourceId,
		streamId,
	)
	contractFrame, err := ToFrame(contract, DefaultProtocolVersion)
	AssertEqual(t, nil, err)
	if err := receiveSequence.registerContracts(&receiveItem{contractFrame: contractFrame}); err != nil {
		t.Fatalf("register serialized stream contract: %v", err)
	}
	if got := len(writer.GetActiveRoutes()); got != 1 {
		t.Fatalf("unready stream routes=%d, want platform only", got)
	}

	var request []byte
	select {
	case request = <-streamRoute1:
	case <-time.After(time.Second):
		t.Fatal("endpoint probe did not issue a readiness challenge")
	}
	recognized, messageType, requestStreamId, nonce := decodeP2pStreamProbe(request)
	MessagePoolReturn(request)
	if !recognized || messageType != p2pStreamProbeRequestType || requestStreamId != streamId {
		t.Fatalf(
			"readiness challenge recognized=%t type=%d stream=%s",
			recognized,
			messageType,
			requestStreamId,
		)
	}
	response := encodeP2pStreamProbe(p2pStreamProbeResponseType, streamId, nonce)
	if !probe.handle(response) {
		MessagePoolReturn(response)
		t.Fatal("endpoint probe rejected its matching response")
	}
	MessagePoolReturn(response)
	if got, ok := waitForP2pProbeRouteCount(writer, 2, time.Second); !ok {
		t.Fatalf("ready stream routes=%d, want platform and P2P", got)
	}

	go receiveSequence.Run()
	receiveSequence.Cancel()
	receiveSequence.WaitForExit()
	if got := len(writer.GetActiveRoutes()); got != 2 {
		t.Fatalf("ReceiveSequence retirement routes=%d, want persistent P2P", got)
	}
	func() {
		routeManager.mutex.Lock()
		defer routeManager.mutex.Unlock()

		aliases := routeManager.writerMatchState.destinationAliases[DestinationId(finalSourceId)]
		if aliases[StreamId(streamId)] != 1 {
			t.Fatalf("persistent alias refs=%d, want 1", aliases[StreamId(streamId)])
		}
	}()

	closeScope2 := routeManager.openWriterStreamAliasScope(streamId)
	closeScope1()
	if got := len(writer.GetActiveRoutes()); got != 2 {
		t.Fatalf("old overlapping scope close routes=%d, want persistent P2P", got)
	}
	closeScope2()
	if got := len(writer.GetActiveRoutes()); got != 1 {
		t.Fatalf("last scope close routes=%d, want platform only", got)
	}
	closeScope3 := routeManager.openWriterStreamAliasScope(streamId)
	if got := len(writer.GetActiveRoutes()); got != 2 {
		t.Fatalf("replacement scope routes=%d, want restored P2P", got)
	}

	probe.close()
	probeOpen = false
	if got := len(writer.GetActiveRoutes()); got != 1 {
		t.Fatalf("first transport withdrawal routes=%d, want platform only", got)
	}
	streamRoute2 := make(Route, 8)
	streamTransport2 := newP2pProbeTestSendTransport(
		adjacentPeerId,
		streamId,
		streamRoute2,
		probeSettings,
	)
	streamTransport2.setEndToEndReady(true)
	routeManager.UpdateTransport(streamTransport2, []Route{streamRoute2})
	defer routeManager.RemoveTransport(streamTransport2)
	if got := len(writer.GetActiveRoutes()); got != 2 {
		t.Fatalf("replacement transport routes=%d, want restored P2P", got)
	}

	routeManager.clearWriterStreamAliasScope(streamId)
	if got := len(writer.GetActiveRoutes()); got != 1 {
		t.Fatalf("authoritative stream retirement routes=%d, want platform only", got)
	}
	closeScope3()
	func() {
		routeManager.mutex.Lock()
		defer routeManager.mutex.Unlock()

		if _, ok := routeManager.writerStreamAuthenticatedDestinations[streamId]; ok {
			t.Fatal("authoritative stream retirement retained authenticated destinations")
		}
		if _, ok := routeManager.writerStreamAliasScopes[streamId]; ok {
			t.Fatal("authoritative stream retirement retained a live alias scope")
		}
	}()
}

// Verified contracts may arrive before StreamOpen, but remote input cannot
// retain unbounded pending alias knowledge. An authoritative empty reset must
// also clear both pending records and a live alias scope.
func TestWriterStreamAliasPendingBoundAndReset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Cancel()
	routeManager := client.RouteManager()
	streamIds := make([]Id, maxPendingWriterStreamAliases+2)
	destinationIds := make([]Id, len(streamIds))
	for i := range streamIds {
		streamIds[i] = NewId()
		destinationIds[i] = NewId()
		if routeManager.authenticateWriterStreamDestination(streamIds[i], destinationIds[i]) {
			t.Fatal("authentication without StreamOpen reported a live scope")
		}
	}

	func() {
		routeManager.mutex.Lock()
		defer routeManager.mutex.Unlock()

		if got := len(routeManager.writerPendingAuthenticatedStreams); got != maxPendingWriterStreamAliases {
			t.Fatalf("pending stream order length=%d, want %d", got, maxPendingWriterStreamAliases)
		}
		if got := len(routeManager.writerStreamAuthenticatedDestinations); got != maxPendingWriterStreamAliases {
			t.Fatalf("pending authentication count=%d, want %d", got, maxPendingWriterStreamAliases)
		}
		for _, evictedStreamId := range streamIds[:2] {
			if _, ok := routeManager.writerStreamAuthenticatedDestinations[evictedStreamId]; ok {
				t.Fatal("oldest pending stream authentication survived the hard bound")
			}
		}
	}()

	liveIndex := len(streamIds) - 1
	liveStreamId := streamIds[liveIndex]
	liveDestination := DestinationId(destinationIds[liveIndex])
	closeScope := routeManager.openWriterStreamAliasScope(liveStreamId)
	defer closeScope()
	func() {
		routeManager.mutex.Lock()
		defer routeManager.mutex.Unlock()

		aliases := routeManager.writerMatchState.destinationAliases[liveDestination]
		if aliases[StreamId(liveStreamId)] != 1 {
			t.Fatal("opening a retained pending stream did not activate its alias")
		}
	}()

	client.streamManager.streamBuffer.ResetStreams(map[streamSequenceId]bool{})
	func() {
		routeManager.mutex.Lock()
		defer routeManager.mutex.Unlock()

		if got := len(routeManager.writerPendingAuthenticatedStreams); got != 0 {
			t.Fatalf("empty reset retained %d pending stream aliases", got)
		}
		if got := len(routeManager.writerStreamAuthenticatedDestinations); got != 0 {
			t.Fatalf("empty reset retained %d authenticated stream aliases", got)
		}
		if got := len(routeManager.writerStreamAliasScopes); got != 0 {
			t.Fatalf("empty reset retained %d live stream alias scopes", got)
		}
		if _, ok := routeManager.writerMatchState.destinationAliases[liveDestination]; ok {
			t.Fatal("empty reset retained the live destination alias")
		}
	}()
}

// Focused scope helpers release their construction token after activation, so
// sequential test scopes do not consume the concurrent-generation bound.
func TestWriterStreamAliasFocusedScopeChurnReleasesGenerations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	routeManager := NewRouteManager(ctx, "test")

	for range maxWriterStreamAliasGenerations + 1 {
		closeScope := routeManager.openWriterStreamAliasScope(NewId())
		closeScope()
	}

	routeManager.mutex.Lock()
	defer routeManager.mutex.Unlock()
	if got := len(routeManager.writerStreamAliasGenerations); got != 0 {
		t.Fatalf("focused scope churn retained %d generation tokens", got)
	}
}

// A received key reproduces the reverse lane on every send shape. Its local
// receive role is also the local send role for a same-session reply; the remote
// receiver performs the complement when it unwraps the frame.
func TestTransferKeySendOptionsThreadAllPaths(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Cancel()
	key := TransferKey{
		ForceStream:         true,
		CompanionContract:   false,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: true,
	}
	baseOptions := DefaultTransferOpts()
	baseOptions.Ack = false
	baseOptions.NetworkPeer = true
	baseOptions.CompanionContract = true
	resolved := client.resolveSendOptions([]any{baseOptions, key})
	AssertEqual(t, false, resolved.transferOptions.Ack)
	AssertEqual(t, true, resolved.transferOptions.NetworkPeer)
	AssertEqual(t, true, resolved.transferOptions.ForceStream)
	AssertEqual(t, false, resolved.transferOptions.CompanionContract)
	AssertEqual(t, sequenceTlsRoleServer, resolved.encryptionRole)
	AssertEqual(t, true, resolved.encryptionCompanion)

	// Later policy intentionally derives a non-stream companion reply while the
	// same local server session remains selected.
	derivedOptions := baseOptions
	derivedOptions.ForceStream = false
	derivedOptions.CompanionContract = true
	resolved = client.resolveSendOptions([]any{key, derivedOptions})
	AssertEqual(t, false, resolved.transferOptions.ForceStream)
	AssertEqual(t, true, resolved.transferOptions.CompanionContract)
	AssertEqual(t, sequenceTlsRoleServer, resolved.encryptionRole)
	AssertEqual(t, true, resolved.encryptionCompanion)

	// Contract policy cannot silently switch the identity companion selected
	// by a received key. Only another key may select another session.
	nonCompanionSessionKey := key
	nonCompanionSessionKey.EncryptionCompanion = false
	resolved = client.resolveSendOptions([]any{nonCompanionSessionKey, derivedOptions})
	AssertEqual(t, true, resolved.transferOptions.CompanionContract)
	AssertEqual(t, false, resolved.encryptionCompanion)
	resolved = client.resolveSendOptions([]any{nonCompanionSessionKey, CompanionContract()})
	AssertEqual(t, true, resolved.transferOptions.CompanionContract)
	AssertEqual(t, false, resolved.encryptionCompanion)
	resolved = client.resolveSendOptions([]any{nonCompanionSessionKey, derivedOptions, key})
	AssertEqual(t, true, resolved.encryptionCompanion)

	destinations := []Id{NewId(), NewId(), NewId()}
	if !client.Send(
		&protocol.Frame{MessageBytes: MessagePoolCopy([]byte("single"))},
		destinations[0],
		func(error) {},
		key,
		NoAck(),
	) {
		t.Fatal("single send rejected TransferKey")
	}
	if !client.SendMultiWithTimeout(
		[]*protocol.Frame{{MessageBytes: MessagePoolCopy([]byte("batch"))}},
		destinations[1],
		func(error) {},
		time.Second,
		key,
		NoAck(),
	) {
		t.Fatal("batch send rejected TransferKey")
	}
	rawSent, err := client.sendRawWithTimeoutDetailed(
		protocol.MessageType_IpIpPacketFromProvider,
		MessagePoolCopy([]byte("raw")),
		destinations[2],
		nil,
		0,
		time.Second,
		key,
		NoAck(),
	)
	if err != nil || !rawSent {
		t.Fatalf("raw send rejected TransferKey: sent=%t err=%v", rawSent, err)
	}

	client.sendBuffer.mutex.Lock()
	defer client.sendBuffer.mutex.Unlock()
	for _, destinationId := range destinations {
		found := false
		for id := range client.sendBuffer.sendSequences {
			if id.Destination != destinationId {
				continue
			}
			found = true
			AssertEqual(t, true, id.ForceStream)
			AssertEqual(t, false, id.CompanionContract)
			AssertEqual(t, sequenceTlsRoleServer, id.EncryptionRole)
			AssertEqual(t, true, id.EncryptionCompanion)
		}
		if !found {
			t.Fatalf("send entry path did not create a sequence for %s", destinationId)
		}
	}
}

// Direct and convenience control sends retain the same zero control
// destination; the zero Id is a real endpoint rather than a missing value.
func TestDirectAndExplicitControlSendUseControlId(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Cancel()
	directFrame := &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: MessagePoolCopy([]byte("direct control")),
	}
	if !client.SendWithTimeout(directFrame, ControlId, nil, time.Second, NoAck()) {
		MessagePoolReturn(directFrame.MessageBytes)
		t.Fatal("direct send rejected ControlId")
	}
	explicitFrame := &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: MessagePoolCopy([]byte("explicit control")),
	}
	if !client.SendControlWithTimeout(explicitFrame, nil, time.Second, NoAck()) {
		MessagePoolReturn(explicitFrame.MessageBytes)
		t.Fatal("explicit control send rejected ControlId")
	}

	client.sendBuffer.mutex.Lock()
	defer client.sendBuffer.mutex.Unlock()
	found := false
	for id := range client.sendBuffer.sendSequences {
		if id.Destination == ControlId {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("explicit control send did not create a control sequence")
	}
}

// Pins the sender/receiver key contract. Local route metadata is absent from
// both logical and wire identity; only receiver-visible lanes may distinguish
// sequences to the same peer.
func TestSendSequenceIdIsPathIndependent(t *testing.T) {
	destinationId := NewId()
	plain := sendSequenceId{Destination: destinationId}
	routed := sendSequenceId{Destination: destinationId}
	if plain != routed {
		t.Fatal("local route metadata changed logical sequence identity")
	}
	AssertEqual(t, plain.wireId(), routed.wireId())

	forceStream := routed
	forceStream.ForceStream = true
	if plain.wireId() == forceStream.wireId() {
		t.Fatal("force-stream lane must remain receiver-visible")
	}
	companion := routed
	companion.CompanionContract = true
	if plain.wireId() == companion.wireId() {
		t.Fatal("companion-contract lane must remain receiver-visible")
	}
	serverRole := routed
	serverRole.EncryptionRole = sequenceTlsRoleServer
	if plain.wireId() == serverRole.wireId() {
		t.Fatal("encryption role must remain receiver-visible")
	}
	identityCompanion := routed
	identityCompanion.EncryptionCompanion = true
	if plain.wireId() == identityCompanion.wireId() {
		t.Fatal("identity companion must remain receiver-visible")
	}
}

// Proves local route metadata cannot fork a logical sender sequence. A
// direct-created handshake/control sequence adopts the first explicit
// multihop data route, retires its direct contract queue, and remains routed
// when later direct controls join it.
func TestSendBufferSharesSequenceAcrossLocalRoutes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Cancel()
	sendBuffer := client.sendBuffer
	destinationId := NewId()
	client.ContractManager().AddNoContractPeer(destinationId)
	sequenceId := sendSequenceId{Destination: destinationId}
	intermediaryIds := RequireMultiHopId(NewId())
	directSequence := sendBuffer.createSendSequence(
		sequenceId,
		&SendPack{Destination: destinationId},
	)
	directMetadata := directSequence.contractMetadata()
	if directMetadata.key.IntermediaryIds.Len() != 0 {
		t.Fatal("direct-created sequence started with multihop contract metadata")
	}

	// Leave a contract in the direct-key queue. Invalid stored bytes keep this
	// unit regression local: flushing must remove the queued object, but there is
	// no platform contract id to close through the OOB path.
	directQueue := client.ContractManager().openContractQueue(directMetadata.key)
	queuedContractId := NewId()
	err := directQueue.Add(
		&protocol.Contract{StoredContractBytes: []byte{0xff}},
		&protocol.StoredContract{ContractId: queuedContractId.Bytes()},
	)
	AssertEqual(t, nil, err)
	client.ContractManager().closeContractQueue(directMetadata.key, directQueue)

	routedPack := &SendPack{
		Destination:     destinationId,
		IntermediaryIds: intermediaryIds,
		Ctx:             ctx,
		ForceUnwrapped:  true,
	}
	success, err := sendBuffer.Pack(routedPack, 0)
	AssertEqual(t, true, success)
	AssertEqual(t, nil, err)
	routedSequence := sendBuffer.lookupSendSequence(sequenceId, nil)

	if directSequence != routedSequence {
		t.Fatal("multihop data forked the direct-created sequence")
	}
	routedMetadata := routedSequence.contractMetadata()
	if routedMetadata.key.IntermediaryIds != intermediaryIds {
		t.Fatalf("contract intermediaries = %s, want %s", routedMetadata.key.IntermediaryIds, intermediaryIds)
	}
	if routedMetadata.generation != directMetadata.generation+1 {
		t.Fatalf("contract metadata generation = %d, want %d", routedMetadata.generation, directMetadata.generation+1)
	}
	select {
	case <-directMetadata.ctx.Done():
	default:
		t.Fatal("direct contract acquisition remained active after multihop promotion")
	}
	client.ContractManager().mutex.Lock()
	_, retainedDirectQueue := client.ContractManager().destinationContracts[directMetadata.key]
	client.ContractManager().mutex.Unlock()
	if retainedDirectQueue {
		t.Fatal("obsolete direct contract queue remained indexed")
	}
	if !directQueue.Drained() {
		t.Fatal("obsolete direct contract queue did not wake its waiters")
	}
	select {
	case <-routedSequence.ctx.Done():
		t.Fatal("metadata promotion canceled the shared sequence")
	default:
	}

	// A later direct control must reuse the sequence without erasing the explicit
	// contract route selected by the application data.
	directPack := &SendPack{
		Destination:    destinationId,
		Ctx:            ctx,
		ForceUnwrapped: true,
	}
	success, err = sendBuffer.Pack(directPack, 0)
	AssertEqual(t, true, success)
	AssertEqual(t, nil, err)
	AssertEqual(t, routedMetadata, routedSequence.contractMetadata())

	func() {
		sendBuffer.mutex.Lock()
		defer sendBuffer.mutex.Unlock()

		if routedSequence != sendBuffer.sendSequences[sequenceId] {
			t.Fatal("shared sequence is missing from the logical sequence map")
		}
		if routedSequence != sendBuffer.wireSendSequences[sequenceId.wireId()] {
			t.Fatal("shared sequence is missing from the wire sequence map")
		}
		logicalCount := 0
		for candidateId := range sendBuffer.sendSequences {
			if candidateId.Destination == destinationId {
				logicalCount += 1
			}
		}
		wireCount := 0
		for candidateId := range sendBuffer.wireSendSequences {
			if candidateId.Destination == destinationId {
				wireCount += 1
			}
		}
		if logicalCount != 1 || wireCount != 1 {
			t.Fatalf("destination sequence indexes = %d/%d, want one shared entry", logicalCount, wireCount)
		}
	}()
}

// A response that completes after route promotion belongs only to the drained
// direct queue generation. It must be closed instead of recreating that key.
func TestContractResponseAfterRoutePromotionDoesNotReopenDirectQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oob := &delayedOobControl{attempts: make(chan delayedOobAttempt, 1)}
	settings := DefaultClientSettings()
	settings.ControlPingTimeout = 0
	client := NewClient(ctx, NewId(), oob, settings)
	defer client.Cancel()
	destinationId := NewId()
	directKey := ContractKey{Destination: DestinationId(destinationId)}

	client.ContractManager().CreateContract(directKey, 0, 1024)
	var attempt delayedOobAttempt
	select {
	case attempt = <-oob.attempts:
	case <-time.After(5 * time.Second):
		t.Fatal("direct contract request did not reach the OOB barrier")
	}
	client.ContractManager().mutex.Lock()
	directQueue := client.ContractManager().destinationContracts[directKey]
	client.ContractManager().mutex.Unlock()
	if directQueue == nil {
		t.Fatal("in-flight request did not retain its queue generation")
	}

	client.ContractManager().FlushContractQueue(directKey, true)
	if !directQueue.Drained() {
		t.Fatal("route promotion did not drain the request's queue generation")
	}
	result := requireContractResult(
		protocol.ProvideMode_Network,
		[]byte("route-promotion-test-key"),
		client.ClientId(),
		destinationId,
	)
	attempt.callback([]*protocol.Frame{result}, nil)
	MessagePoolReturn(result.MessageBytes)

	client.ContractManager().mutex.Lock()
	_, retainedDirectQueue := client.ContractManager().destinationContracts[directKey]
	client.ContractManager().mutex.Unlock()
	if retainedDirectQueue {
		t.Fatal("late direct result reopened the retired contract key")
	}
	if !directQueue.Drained() {
		t.Fatal("late direct result revived the drained queue generation")
	}
}

// A direct contract already taken when promotion wins may authenticate that
// in-flight pack, but its remaining capacity cannot carry future multihop
// packs. The next pack acquires a stream-stamped contract, which gives the
// provider enough verified metadata to bind its reverse destination alias.
func TestRoutePromotionRetiresCapaciousDirectContractHead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	senderId := NewId()
	providerId := NewId()
	streamId := NewId()
	intermediaryIds := RequireMultiHopId(NewId())
	settings := DefaultClientSettings()
	settings.ControlPingTimeout = 0
	settings.ContractManagerSettings.NetworkEventTimeEnableContracts = time.Time{}
	sender := NewClient(ctx, senderId, alwaysSuccessOob{}, settings)
	defer sender.Cancel()
	gatewayRoute := make(chan []byte, 16)
	gatewayTransport := NewSendGatewayTransport()
	sender.RouteManager().UpdateTransport(gatewayTransport, []Route{gatewayRoute})
	defer sender.RouteManager().RemoveTransport(gatewayTransport)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case message := <-gatewayRoute:
				MessagePoolReturn(message)
			}
		}
	}()
	sendBufferSettings := settings.SendBufferSettings
	sequence := NewSendSequence(
		ctx,
		sender,
		sender.sendBuffer,
		providerId,
		MultiHopId{},
		false,
		false,
		false,
		sequenceTlsRoleClient,
		false,
		sendBufferSettings,
	)
	defer sequence.cancel()
	defer func() {
		for _, item := range sequence.resendQueue.Clear() {
			item.messagePoolReturn()
		}
		sequence.sendItems = nil
	}()

	makeContract := func(stream Id) *protocol.Contract {
		storedContract := &protocol.StoredContract{
			ContractId:        NewId().Bytes(),
			TransferByteCount: uint64(1024 * 1024),
			SourceId:          senderId.Bytes(),
			DestinationId:     providerId.Bytes(),
		}
		if stream != (Id{}) {
			storedContract.StreamId = stream.Bytes()
		}
		storedContractBytes, err := proto.Marshal(storedContract)
		AssertEqual(t, nil, err)
		return &protocol.Contract{
			StoredContractBytes: storedContractBytes,
			ProvideMode:         protocol.ProvideMode_Network,
		}
	}

	directMetadata := sequence.contractMetadata()
	directContract := makeContract(Id{})
	AssertEqual(t, nil, sender.ContractManager().addContract(directMetadata.key, directContract))
	taken := make(chan sendContractMetadata, 1)
	releaseTaken := make(chan struct{})
	sequence.contractTakenForTest = func(metadata sendContractMetadata) {
		taken <- metadata
		<-releaseTaken
	}
	firstResult := make(chan bool, 1)
	go func() {
		firstResult <- sequence.updateContract(1024)
	}()

	select {
	case metadata := <-taken:
		AssertEqual(t, directMetadata.generation, metadata.generation)
	case <-time.After(5 * time.Second):
		t.Fatal("direct contract was not taken before the promotion barrier")
	}
	sequence.adoptContractIntermediaryIds(intermediaryIds)
	routedMetadata := sequence.contractMetadata()
	routedContract := makeContract(streamId)
	AssertEqual(t, nil, sender.ContractManager().addContract(routedMetadata.key, routedContract))
	close(releaseTaken)
	select {
	case success := <-firstResult:
		AssertEqual(t, true, success)
	case <-time.After(5 * time.Second):
		t.Fatal("already-taken direct contract did not finish")
	}
	sequence.contractTakenForTest = nil

	directHead := sequence.sendContract
	if directHead == nil || directHead.path.StreamId != (Id{}) {
		t.Fatal("in-flight direct contract was not accepted as the old head")
	}
	if sequence.sendContractMetadataGeneration != directMetadata.generation {
		t.Fatal("direct head lost its acquisition generation")
	}
	if !directHead.canUpdate(1024) {
		t.Fatal("direct head lacks the spare capacity required by the regression")
	}
	if !sequence.updateContract(1024) {
		t.Fatal("promoted pack could not acquire its stream contract")
	}
	if sequence.sendContract == nil || sequence.sendContract.path.StreamId != streamId {
		t.Fatalf("promoted head stream = %s, want %s", sequence.sendContract.path.StreamId, streamId)
	}
	if sequence.sendContractMetadataGeneration != routedMetadata.generation {
		t.Fatal("promoted head was stamped with the wrong metadata generation")
	}
	if sequence.openSendContracts[directHead.contractId] != directHead {
		t.Fatal("retired direct head was discarded before outstanding ACK accounting")
	}

	providerSettings := DefaultClientSettings()
	providerSettings.ControlPingTimeout = 0
	provider := NewClient(ctx, providerId, alwaysSuccessOob{}, providerSettings)
	defer provider.Cancel()
	receiveSequence := NewReceiveSequence(
		ctx,
		provider,
		SourceId(senderId),
		NewId(),
		sequenceTlsRoleServer,
		false,
		providerSettings.ReceiveBufferSettings,
	)
	receiveSequence.receiveContract = sequence.sendContract
	receiveSequence.updateContractWriterAlias()
	defer receiveSequence.closeContractWriterAlias()
	func() {
		provider.RouteManager().mutex.Lock()
		defer provider.RouteManager().mutex.Unlock()

		aliases := provider.RouteManager().writerMatchState.destinationAliases[DestinationId(senderId)]
		if aliases[StreamId(streamId)] != 1 {
			t.Fatalf("provider reverse alias refs = %d, want 1", aliases[StreamId(streamId)])
		}
	}()
}

// Proves teardown preserves the deliver-then-ack contract while compression is
// waiting. Exit is published only after the final window and route writer are
// closed.
func TestReceiveSequenceExitDrainsFinalAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Cancel()
	gatewayRoute := make(chan []byte, 32)
	gatewayTransport := NewSendGatewayTransport()
	client.RouteManager().UpdateTransport(gatewayTransport, []Route{gatewayRoute})
	defer client.RouteManager().RemoveTransport(gatewayTransport)

	receiveBufferSettings := DefaultReceiveBufferSettings()
	receiveBufferSettings.IdleTimeout = time.Millisecond
	receiveBufferSettings.AckCompressTimeout = time.Hour
	receiveBufferSettings.WriteTimeout = time.Second
	messageId := NewId()
	sourceId := NewId()
	receiveSequence := NewReceiveSequence(
		ctx,
		client,
		SourceId(sourceId),
		NewId(),
		sequenceTlsRoleServer,
		false,
		receiveBufferSettings,
	)
	receiveSequence.deliverItems = []*receiveItem{
		{
			transferItem: transferItem{
				messageId:      messageId,
				sequenceNumber: 1,
			},
			ack: true,
		},
	}

	go receiveSequence.Run()
	select {
	case <-receiveSequence.exit:
	case <-time.After(5 * time.Second):
		t.Fatal("receive sequence did not publish exit")
	}

	var transferFrame *protocol.TransferFrame
	deadline := time.After(time.Second)
	for transferFrame == nil {
		select {
		case transferFrameBytes := <-gatewayRoute:
			candidate := &protocol.TransferFrame{}
			err := proto.Unmarshal(transferFrameBytes, candidate)
			MessagePoolReturn(transferFrameBytes)
			if err == nil && candidate.Ack != nil &&
				slices.Equal(candidate.Ack.MessageId, messageId.Bytes()) {
				transferFrame = candidate
			}
		case <-deadline:
			t.Fatal("final compressed ACK was not drained before exit")
		}
	}
	AssertEqual(t, messageId.Bytes(), transferFrame.Ack.MessageId)
	path, err := TransferPathFromProtobuf(transferFrame.TransferPath)
	AssertEqual(t, nil, err)
	AssertEqual(t, true, path.IsLocalMask())

	func() {
		client.RouteManager().mutex.Lock()
		defer client.RouteManager().mutex.Unlock()

		ackDestination := DestinationId(sourceId)
		if 0 != len(client.RouteManager().writerMatchState.destinationMultiRouteSelectors[ackDestination]) {
			t.Fatal("ACK route writer remained open after receive sequence exit")
		}
	}()
}

// Pins cleanup under an internal receive-dispatch panic. Registered application
// callback panics are isolated one level up by Client.receive; this direct
// callback models failure of the dispatcher itself. The panic is rethrown only
// after the ACK worker, route writer, context, and exit notification finish.
func TestReceiveSequenceDispatcherPanicStillTearsDownAckWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Cancel()
	gatewayRoute := make(chan []byte, 32)
	gatewayTransport := NewSendGatewayTransport()
	client.RouteManager().UpdateTransport(gatewayTransport, []Route{gatewayRoute})
	defer client.RouteManager().RemoveTransport(gatewayTransport)

	receiveBufferSettings := DefaultReceiveBufferSettings()
	receiveBufferSettings.IdleTimeout = time.Millisecond
	receiveBufferSettings.WriteTimeout = time.Second
	sourceId := NewId()
	receiveSequence := NewReceiveSequence(
		ctx,
		client,
		SourceId(sourceId),
		NewId(),
		sequenceTlsRoleServer,
		false,
		receiveBufferSettings,
	)
	frame := &protocol.Frame{MessageType: protocol.MessageType_TransferExchangeSignals}
	receiveSequence.deliverItems = []*receiveItem{
		{
			frames: []*protocol.Frame{frame},
			receiveCallback: func(TransferPath, []*protocol.Frame, Peer) {
				panic("callback panic")
			},
			ack: true,
		},
	}
	receiveSequence.deliverFrames = []*protocol.Frame{frame}

	panicValues := make(chan any, 1)
	go func() {
		defer func() {
			panicValues <- recover()
		}()
		receiveSequence.Run()
	}()
	select {
	case <-receiveSequence.exit:
	case <-time.After(5 * time.Second):
		t.Fatal("callback panic bypassed receive sequence exit")
	}
	select {
	case panicValue := <-panicValues:
		AssertEqual(t, "callback panic", panicValue)
	case <-time.After(time.Second):
		t.Fatal("receive sequence did not rethrow callback panic")
	}
	select {
	case <-receiveSequence.ctx.Done():
	default:
		t.Fatal("callback panic left receive sequence context active")
	}

	func() {
		client.RouteManager().mutex.Lock()
		defer client.RouteManager().mutex.Unlock()

		ackDestination := DestinationId(sourceId)
		if 0 != len(client.RouteManager().writerMatchState.destinationMultiRouteSelectors[ackDestination]) {
			t.Fatal("callback panic left an ACK route writer open")
		}
	}()
}
