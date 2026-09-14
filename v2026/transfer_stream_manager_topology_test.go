// Intermediary topology tests pin adjacent destination route keys across both
// private forwarding directions with real loopback WebRTC associations.
package connect

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Delivers owned WebRTC signals between three in-memory clients while keeping
// the callback source path destination-free and stream-free. The signal
// payload itself retains its protocol StreamId.
type streamIntermediarySignalRouter struct {
	stateLock sync.Mutex
	receivers map[Id]SignalReceiver
	errors    chan error
}

// Fixes the authenticated source identity for every signal emitted by one
// client.
type streamIntermediarySignalSender struct {
	sourceId Id
	router   *streamIntermediarySignalRouter
}

// Allocates an empty bounded signal router.
func newStreamIntermediarySignalRouter() *streamIntermediarySignalRouter {
	return &streamIntermediarySignalRouter{
		receivers: map[Id]SignalReceiver{},
		errors:    make(chan error, 32),
	}
}

// Publishes one signal receiver before its StreamSequence starts.
func (self *streamIntermediarySignalRouter) register(
	clientId Id,
	receiver SignalReceiver,
) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.receivers[clientId] = receiver
}

// Binds subsequent signaling to one authenticated source identity.
func (self *streamIntermediarySignalRouter) sender(
	sourceId Id,
) SignalSender {
	return &streamIntermediarySignalSender{
		sourceId: sourceId,
		router:   self,
	}
}

// Retains the first bounded set of asynchronous signaling failures.
func (self *streamIntermediarySignalRouter) report(err error) {
	select {
	case self.errors <- err:
	default:
	}
}

// Synchronously borrows the frame during delivery and then returns the
// sender-owned bytes. A source callback path carries identity only; the
// receiver reads StreamId from ExchangeSignals and TransferKey carries no path.
func (self *streamIntermediarySignalSender) SendSignal(
	destinationId Id,
	signal *protocol.Frame,
	opts ...any,
) {
	defer MessagePoolReturn(signal.MessageBytes)

	receiver := func() SignalReceiver {
		self.router.stateLock.Lock()
		defer self.router.stateLock.Unlock()
		return self.router.receivers[destinationId]
	}()
	if receiver == nil {
		self.router.report(fmt.Errorf("signal destination %s is not registered", destinationId))
		return
	}
	source := SourceId(self.sourceId)
	if source.StreamId != (Id{}) || source.DestinationId != (Id{}) {
		self.router.report(fmt.Errorf("signal source path contains transport routing state: %s", source))
		return
	}
	if err := receiver.ReceiveSignal(
		source,
		testingSignalTransferKey(opts),
		signal,
	); err != nil {
		self.router.report(err)
	}
}

// Owns the selectors opened before either private intermediary association can
// publish a route.
type streamIntermediaryRouteManagers struct {
	toDestination *RouteManager
	toSource      *RouteManager

	destinationPeerWriter   MultiRouteWriter
	destinationStreamWriter MultiRouteWriter
	destinationWrongWriter  MultiRouteWriter
	sourcePeerWriter        MultiRouteWriter
	sourceStreamWriter      MultiRouteWriter
	sourceWrongWriter       MultiRouteWriter
}

// Identifies one expected physical half and the private manager into which
// production published it.
type streamIntermediaryRouteStateKey struct {
	peerId          Id
	peerType        PeerType
	routeManagerTag string
	send            bool
}

// A production three-client StreamSequence topology proves that the
// intermediary publishes each physical direction under both its local
// StreamId relay key and its adjacent peer DestinationId. Receive registration
// crosses into the opposite private manager, preserving source/destination
// symmetry without putting StreamId in an application TransferPath.
func TestIntermediaryStreamSequenceMatchesAdjacentDestinationsBothDirections(
	t *testing.T,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	sourceId := NewId()
	intermediaryId := NewId()
	destinationId := NewId()
	streamId := NewId()
	router := newStreamIntermediarySignalRouter()

	newClient := func(clientId Id) *Client {
		settings := DefaultClientSettings()
		settings.Log = NewNoopLogger()
		return NewClient(ctx, clientId, NewNoContractClientOob(), settings)
	}
	sourceClient := newClient(sourceId)
	intermediaryClient := newClient(intermediaryId)
	destinationClient := newClient(destinationId)

	newWebRtcManager := func(clientId Id) *WebRtcManager {
		settings := DefaultWebRtcSettings()
		settings.Log = NewNoopLogger()
		settings.IceServerUrls = nil
		settings.UseLoopbackOnlyIceInterfaces = true
		return newTestWebRtcManager(t, ctx, router.sender(clientId), settings)
	}
	sourceWebRtcManager := newWebRtcManager(sourceId)
	intermediaryWebRtcManager := newWebRtcManager(intermediaryId)
	destinationWebRtcManager := newWebRtcManager(destinationId)
	router.register(sourceId, sourceWebRtcManager)
	router.register(intermediaryId, intermediaryWebRtcManager)
	router.register(destinationId, destinationWebRtcManager)

	routeStates := make(chan P2pRouteState, 32)
	var routeStateOverflow atomic.Bool
	newStreamManager := func(
		client *Client,
		webRtcManager *WebRtcManager,
		observeRoutes bool,
	) *StreamManager {
		settings := DefaultStreamManagerSettings()
		settings.StreamBufferSettings.P2pTransportSettings.DataPlaneMode =
			P2pDataPlaneModeLegacyOnly
		settings.StreamBufferSettings.P2pTransportSettings.EndToEndProbeInterval =
			5 * time.Millisecond
		settings.StreamBufferSettings.P2pTransportSettings.EndToEndProbeTimeout =
			100 * time.Millisecond
		if observeRoutes {
			settings.StreamBufferSettings.P2pTransportSettings.RouteStateObserver =
				func(state P2pRouteState) {
					select {
					case routeStates <- state:
					default:
						routeStateOverflow.Store(true)
					}
				}
		}
		return NewStreamManager(ctx, client, webRtcManager, settings)
	}
	sourceStreamManager := newStreamManager(sourceClient, sourceWebRtcManager, false)
	intermediaryStreamManager := newStreamManager(
		intermediaryClient,
		intermediaryWebRtcManager,
		true,
	)
	destinationStreamManager := newStreamManager(
		destinationClient,
		destinationWebRtcManager,
		false,
	)

	var sourceSequence *StreamSequence
	var intermediarySequence *StreamSequence
	var destinationSequence *StreamSequence
	defer func() {
		if sourceSequence != nil {
			sourceSequence.CloseAndWait()
		}
		if intermediarySequence != nil {
			intermediarySequence.CloseAndWait()
		}
		if destinationSequence != nil {
			destinationSequence.CloseAndWait()
		}
		sourceWebRtcManager.Close()
		intermediaryWebRtcManager.Close()
		destinationWebRtcManager.Close()
		sourceClient.Close()
		intermediaryClient.Close()
		destinationClient.Close()
	}()

	destinationPassiveReady := make(chan struct{})
	var destinationPassiveReadyOnce sync.Once
	destinationWebRtcManager.testingAfterPeerConnRegistered = func(
		path TransferPath,
		active bool,
	) {
		if !active && path == NewTransferPath(destinationId, intermediaryId, streamId) {
			destinationPassiveReadyOnce.Do(func() {
				close(destinationPassiveReady)
			})
		}
	}
	destinationSequence = NewStreamSequence(
		ctx,
		destinationStreamManager,
		&intermediaryId,
		nil,
		streamId,
		destinationStreamManager.streamManagerSettings.StreamBufferSettings,
	)
	go destinationSequence.Run()
	waitForStreamLifecycleSignal(
		t,
		destinationPassiveReady,
		"destination did not register its passive adjacent association",
	)

	privateManagers := &streamIntermediaryRouteManagers{}
	privateManagersReady := make(chan struct{})
	intermediarySourcePassiveReady := make(chan struct{})
	var intermediarySourcePassiveReadyOnce sync.Once
	intermediaryWebRtcManager.testingAfterPeerConnRegistered = func(
		path TransferPath,
		active bool,
	) {
		if !active && path == NewTransferPath(intermediaryId, sourceId, streamId) {
			intermediarySourcePassiveReadyOnce.Do(func() {
				close(intermediarySourcePassiveReady)
			})
		}
	}
	intermediarySequence = NewStreamSequence(
		ctx,
		intermediaryStreamManager,
		&sourceId,
		&destinationId,
		streamId,
		intermediaryStreamManager.streamManagerSettings.StreamBufferSettings,
	)
	intermediarySequence.intermediaryRouteManagersForTest = func(
		toDestination *RouteManager,
		toSource *RouteManager,
	) {
		privateManagers.toDestination = toDestination
		privateManagers.toSource = toSource
		privateManagers.destinationPeerWriter =
			toDestination.OpenMultiRouteWriter(DestinationId(destinationId))
		privateManagers.destinationStreamWriter =
			toDestination.OpenMultiRouteWriter(StreamId(streamId))
		privateManagers.destinationWrongWriter =
			toDestination.OpenMultiRouteWriter(DestinationId(sourceId))
		privateManagers.sourcePeerWriter =
			toSource.OpenMultiRouteWriter(DestinationId(sourceId))
		privateManagers.sourceStreamWriter =
			toSource.OpenMultiRouteWriter(StreamId(streamId))
		privateManagers.sourceWrongWriter =
			toSource.OpenMultiRouteWriter(DestinationId(destinationId))
		close(privateManagersReady)
	}
	go intermediarySequence.Run()
	waitForStreamLifecycleSignal(
		t,
		privateManagersReady,
		"intermediary did not publish its private route managers",
	)
	defer func() {
		privateManagers.toDestination.CloseMultiRouteWriter(
			privateManagers.destinationPeerWriter,
		)
		privateManagers.toDestination.CloseMultiRouteWriter(
			privateManagers.destinationStreamWriter,
		)
		privateManagers.toDestination.CloseMultiRouteWriter(
			privateManagers.destinationWrongWriter,
		)
		privateManagers.toSource.CloseMultiRouteWriter(
			privateManagers.sourcePeerWriter,
		)
		privateManagers.toSource.CloseMultiRouteWriter(
			privateManagers.sourceStreamWriter,
		)
		privateManagers.toSource.CloseMultiRouteWriter(
			privateManagers.sourceWrongWriter,
		)
	}()
	waitForStreamLifecycleSignal(
		t,
		intermediarySourcePassiveReady,
		"intermediary did not register its passive source association",
	)

	sourceSequence = NewStreamSequence(
		ctx,
		sourceStreamManager,
		nil,
		&intermediaryId,
		streamId,
		sourceStreamManager.streamManagerSettings.StreamBufferSettings,
	)
	go sourceSequence.Run()

	toDestinationTag := fmt.Sprintf("->s(%s)", streamId)
	toSourceTag := fmt.Sprintf("<-s(%s)", streamId)
	expectedStates := map[streamIntermediaryRouteStateKey]bool{
		{
			peerId:          destinationId,
			peerType:        PeerTypeDestination,
			routeManagerTag: toDestinationTag,
			send:            true,
		}: true,
		{
			peerId:          destinationId,
			peerType:        PeerTypeDestination,
			routeManagerTag: toSourceTag,
			send:            false,
		}: true,
		{
			peerId:          sourceId,
			peerType:        PeerTypeSource,
			routeManagerTag: toSourceTag,
			send:            true,
		}: true,
		{
			peerId:          sourceId,
			peerType:        PeerTypeSource,
			routeManagerTag: toDestinationTag,
			send:            false,
		}: true,
	}
	connectedStates := map[streamIntermediaryRouteStateKey]bool{}
	allRoutesReady := func() bool {
		for key := range expectedStates {
			if !connectedStates[key] {
				return false
			}
		}
		return len(privateManagers.destinationPeerWriter.GetActiveRoutes()) == 1 &&
			len(privateManagers.destinationStreamWriter.GetActiveRoutes()) == 1 &&
			len(privateManagers.destinationWrongWriter.GetActiveRoutes()) == 0 &&
			len(privateManagers.sourcePeerWriter.GetActiveRoutes()) == 1 &&
			len(privateManagers.sourceStreamWriter.GetActiveRoutes()) == 1 &&
			len(privateManagers.sourceWrongWriter.GetActiveRoutes()) == 0
	}
	routeTimer := time.NewTimer(20 * time.Second)
	defer routeTimer.Stop()
	for !allRoutesReady() {
		select {
		case err := <-router.errors:
			t.Fatalf("in-memory signaling failed: %v", err)
		case state := <-routeStates:
			if state.StreamId != streamId {
				t.Fatalf("intermediary observed the wrong stream: %+v", state)
			}
			key := streamIntermediaryRouteStateKey{
				peerId:          state.PeerId,
				peerType:        state.PeerType,
				routeManagerTag: state.RouteManagerTag,
				send:            state.Send,
			}
			if !expectedStates[key] {
				t.Fatalf("intermediary published an unexpected route direction: %+v", state)
			}
			connectedStates[key] = state.Connected
		case <-routeTimer.C:
			t.Fatalf(
				"intermediary routes did not become simultaneously ready: states=%+v destination=(%d,%d,%d) source=(%d,%d,%d)",
				connectedStates,
				len(privateManagers.destinationPeerWriter.GetActiveRoutes()),
				len(privateManagers.destinationStreamWriter.GetActiveRoutes()),
				len(privateManagers.destinationWrongWriter.GetActiveRoutes()),
				len(privateManagers.sourcePeerWriter.GetActiveRoutes()),
				len(privateManagers.sourceStreamWriter.GetActiveRoutes()),
				len(privateManagers.sourceWrongWriter.GetActiveRoutes()),
			)
		}
	}
	if routeStateOverflow.Load() {
		t.Fatal("intermediary route-state observer overflowed")
	}

	assertSameRoute := func(
		peerWriter MultiRouteWriter,
		streamWriter MultiRouteWriter,
		label string,
	) Route {
		peerRoutes := peerWriter.GetActiveRoutes()
		streamRoutes := streamWriter.GetActiveRoutes()
		if len(peerRoutes) != 1 || len(streamRoutes) != 1 {
			t.Fatalf(
				"%s route counts peer=%d stream=%d, want one each",
				label,
				len(peerRoutes),
				len(streamRoutes),
			)
		}
		if peerRoutes[0] != streamRoutes[0] {
			t.Fatalf("%s peer and stream keys selected different transports", label)
		}
		return peerRoutes[0]
	}
	destinationRoute := assertSameRoute(
		privateManagers.destinationPeerWriter,
		privateManagers.destinationStreamWriter,
		"to-destination",
	)
	sourceRoute := assertSameRoute(
		privateManagers.sourcePeerWriter,
		privateManagers.sourceStreamWriter,
		"to-source",
	)
	if destinationRoute == sourceRoute {
		t.Fatal("opposite intermediary directions shared one physical route")
	}

	assertSendTransport := func(
		routeManager *RouteManager,
		route Route,
		peerId Id,
		label string,
	) {
		var matched *P2pSendTransport
		routeManager.mutex.Lock()
		for transport, routes := range routeManager.writerMatchState.transportRoutes {
			for _, candidate := range routes {
				if candidate == route {
					matched, _ = transport.(*P2pSendTransport)
				}
			}
		}
		routeManager.mutex.Unlock()
		if matched == nil {
			t.Fatalf("%s destination key did not select a P2pSendTransport", label)
		}
		if matched.peerId != peerId || matched.streamId != streamId {
			t.Fatalf(
				"%s transport keys peer=%s stream=%s, want peer=%s stream=%s",
				label,
				matched.peerId,
				matched.streamId,
				peerId,
				streamId,
			)
		}
		if matched.endToEndReadinessRequired {
			t.Fatalf("%s intermediary transport incorrectly requires endpoint readiness", label)
		}
	}
	assertSendTransport(
		privateManagers.toDestination,
		destinationRoute,
		destinationId,
		"to-destination",
	)
	assertSendTransport(
		privateManagers.toSource,
		sourceRoute,
		sourceId,
		"to-source",
	)

	assertExactReceiveTransports := func(routeManager *RouteManager, label string) {
		receiveCount := 0
		reliabilityCounts := map[CarrierReliability]int{}
		routeManager.mutex.Lock()
		for transport, routes := range routeManager.readerMatchState.transportRoutes {
			if len(routes) == 0 || !transport.MatchesReceive(StreamId(streamId)) {
				continue
			}
			var receiveTransport *P2pReceiveTransport
			switch typedTransport := transport.(type) {
			case *P2pReceiveTransport:
				receiveTransport = typedTransport
			case *receiveLaneTransport:
				receiveTransport, _ = typedTransport.base.(*P2pReceiveTransport)
			}
			if receiveTransport == nil || receiveTransport.streamId != streamId {
				routeManager.mutex.Unlock()
				t.Fatalf("%s registered an unexpected receive transport", label)
			}
			receiveCount += 1
			reliability := routeManager.readerMatchState.
				transportProperties[transport].ReceiveReliability
			reliabilityCounts[reliability] += 1
		}
		routeManager.mutex.Unlock()
		if receiveCount != 2 ||
			reliabilityCounts[CarrierReliabilityReliable] != 1 ||
			reliabilityCounts[CarrierReliabilityUnreliable] != 1 {
			t.Fatalf(
				"%s receive routes=%d reliability=%+v, want one SCTP and one native lane",
				label,
				receiveCount,
				reliabilityCounts,
			)
		}
	}
	assertExactReceiveTransports(privateManagers.toDestination, "from-source")
	assertExactReceiveTransports(privateManagers.toSource, "from-destination")

	for {
		select {
		case err := <-router.errors:
			t.Fatalf("in-memory signaling failed after route publication: %v", err)
		default:
			return
		}
	}
}
