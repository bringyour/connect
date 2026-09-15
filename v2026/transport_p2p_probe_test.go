// End-to-end stream readiness tests model opaque intermediary forwarding,
// exact transport generations, reconnect epochs, compatibility, and carrier
// accounting without requiring a public network.
package connect

import (
	"bytes"
	"context"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// The production cadence leaves a measurable idle window after a complete
// challenge on the maximum supported regional round trip.
func TestDefaultP2pStreamProbeCadenceExceedsRegionalRoundTrip(t *testing.T) {
	settings := DefaultP2pTransportSettings()
	maximumRegionalRoundTrip := time.Second
	if settings.EndToEndProbeInterval <= maximumRegionalRoundTrip {
		t.Fatalf(
			"probe interval=%s, want greater than regional round trip %s",
			settings.EndToEndProbeInterval,
			maximumRegionalRoundTrip,
		)
	}
	if settings.EndToEndProbeTimeout < 3*settings.EndToEndProbeInterval {
		t.Fatalf(
			"probe timeout=%s interval=%s, want at least three intervals",
			settings.EndToEndProbeTimeout,
			settings.EndToEndProbeInterval,
		)
	}
}

// One userspace topology owns both endpoint probes and a symmetric chain of
// opaque relays. Link state is shared by its two directions.
type p2pProbeTestTopology struct {
	ctx    context.Context
	cancel context.CancelFunc

	streamId      Id
	sourceId      Id
	destinationId Id
	settings      *P2pTransportSettings

	sourceRouteManager      *RouteManager
	destinationRouteManager *RouteManager
	sourceTransport         *P2pSendTransport
	destinationTransport    *P2pSendTransport
	sourceRoute             Route
	destinationRoute        Route
	sourceProbe             *p2pStreamProbe
	destinationProbe        *p2pStreamProbe
	sourceWriter            MultiRouteWriter
	destinationWriter       MultiRouteWriter
	removeSourceAlias       func()
	removeDestinationAlias  func()

	linkEnableds             []*atomic.Bool
	relayRoutes              []Route
	applicationAtSource      chan []byte
	applicationAtDestination chan []byte
	droppedMessageCount      atomic.Int64
	droppedMessageUpdate     chan struct{}

	challengeLock       sync.Mutex
	lastSourceChallenge Id
	challengeUpdate     chan struct{}
	relayWaitGroup      sync.WaitGroup
}

// Builds a path with one transport generation at each endpoint. A nonnegative
// disabled link starts disconnected before either probe can send.
func newP2pProbeTestTopology(
	t *testing.T,
	hopCount int,
	disabledLinkIndex int,
) *p2pProbeTestTopology {
	t.Helper()
	if hopCount <= 0 {
		t.Fatalf("invalid probe topology hop count %d", hopCount)
	}
	ctx, cancel := context.WithCancel(context.Background())
	settings := DefaultP2pTransportSettings()
	settings.EndToEndProbeInterval = 5 * time.Millisecond
	settings.EndToEndProbeTimeout = 50 * time.Millisecond
	settings.ChannelBufferSize = 32
	topology := &p2pProbeTestTopology{
		ctx:                      ctx,
		cancel:                   cancel,
		streamId:                 NewId(),
		sourceId:                 NewId(),
		destinationId:            NewId(),
		settings:                 settings,
		sourceRouteManager:       NewRouteManager(ctx, "probe-source"),
		destinationRouteManager:  NewRouteManager(ctx, "probe-destination"),
		applicationAtSource:      make(chan []byte, 32),
		applicationAtDestination: make(chan []byte, 32),
		droppedMessageUpdate:     make(chan struct{}, 1),
		challengeUpdate:          make(chan struct{}, 1),
	}
	topology.sourceRoute = make(Route, settings.ChannelBufferSize)
	topology.destinationRoute = make(Route, settings.ChannelBufferSize)
	topology.sourceTransport = newP2pProbeTestSendTransport(
		topology.destinationId,
		topology.streamId,
		topology.sourceRoute,
		settings,
	)
	topology.destinationTransport = newP2pProbeTestSendTransport(
		topology.sourceId,
		topology.streamId,
		topology.destinationRoute,
		settings,
	)
	topology.sourceRouteManager.UpdateTransport(
		topology.sourceTransport,
		[]Route{topology.sourceRoute},
	)
	topology.destinationRouteManager.UpdateTransport(
		topology.destinationTransport,
		[]Route{topology.destinationRoute},
	)
	topology.removeSourceAlias = topology.sourceRouteManager.AddWriterDestinationAlias(
		DestinationId(topology.destinationId),
		StreamId(topology.streamId),
	)
	topology.removeDestinationAlias = topology.destinationRouteManager.AddWriterDestinationAlias(
		DestinationId(topology.sourceId),
		StreamId(topology.streamId),
	)
	topology.sourceWriter = topology.sourceRouteManager.OpenMultiRouteWriter(
		DestinationId(topology.destinationId),
	)
	topology.destinationWriter = topology.destinationRouteManager.OpenMultiRouteWriter(
		DestinationId(topology.sourceId),
	)

	forwardRoutes := make([]Route, hopCount)
	reverseRoutes := make([]Route, hopCount)
	forwardRoutes[0] = topology.sourceRoute
	reverseRoutes[0] = topology.destinationRoute
	for hopIndex := 1; hopIndex < hopCount; hopIndex += 1 {
		forwardRoutes[hopIndex] = make(Route, settings.ChannelBufferSize)
		reverseRoutes[hopIndex] = make(Route, settings.ChannelBufferSize)
	}
	topology.relayRoutes = append(topology.relayRoutes, forwardRoutes...)
	topology.relayRoutes = append(topology.relayRoutes, reverseRoutes...)
	for hopIndex := range hopCount {
		enabled := &atomic.Bool{}
		enabled.Store(hopIndex != disabledLinkIndex)
		topology.linkEnableds = append(topology.linkEnableds, enabled)

		var forwardDestination Route
		if hopIndex+1 < hopCount {
			forwardDestination = forwardRoutes[hopIndex+1]
		}
		var reverseDestination Route
		if hopIndex+1 < hopCount {
			reverseDestination = reverseRoutes[hopIndex+1]
		}
		topology.startRelay(
			forwardRoutes[hopIndex],
			forwardDestination,
			enabled,
			topology.destinationProbeMessage,
		)
		topology.startRelay(
			reverseRoutes[hopIndex],
			reverseDestination,
			enabled,
			topology.sourceProbeMessage,
		)
	}

	topology.sourceProbe = newP2pStreamProbe(
		ctx,
		topology.sourceRouteManager,
		topology.streamId,
		settings,
	)
	topology.destinationProbe = newP2pStreamProbe(
		ctx,
		topology.destinationRouteManager,
		topology.streamId,
		settings,
	)
	topology.sourceProbe.setSendRoute(topology.sourceTransport, topology.sourceRoute)
	topology.destinationProbe.setSendRoute(topology.destinationTransport, topology.destinationRoute)
	return topology
}

// Creates an endpoint-only transport without a competing send worker. The
// relay harness consumes its exact route in the same ownership pattern.
func newP2pProbeTestSendTransport(
	peerId Id,
	streamId Id,
	route Route,
	settings *P2pTransportSettings,
) *P2pSendTransport {
	transport := &P2pSendTransport{
		transportId:               NewId(),
		peerId:                    peerId,
		streamId:                  streamId,
		send:                      route,
		endToEndReadinessRequired: true,
		settings:                  settings,
	}
	transport.probeSendAdmission.open = true
	return transport
}

// Builds probe state without starting its lifecycle, so barrier tests can
// control the first producer exactly.
func newStoppedP2pStreamProbe(
	ctx context.Context,
	routeManager *RouteManager,
	streamId Id,
	settings *P2pTransportSettings,
) *p2pStreamProbe {
	probeCtx, cancel := context.WithCancel(ctx)
	interval := settings.EndToEndProbeInterval
	if interval <= 0 {
		interval = time.Second
	}
	timeout := settings.EndToEndProbeTimeout
	if timeout < 2*interval {
		timeout = 2 * interval
	}
	return &p2pStreamProbe{
		ctx:          probeCtx,
		cancel:       cancel,
		routeManager: routeManager,
		streamId:     streamId,
		interval:     interval,
		timeout:      timeout,
		observer:     settings.EndToEndProbeObserver,
		routeUpdate:  NewMonitor(),
		responses:    make(chan Id, 1),
		done:         make(chan struct{}),
	}
}

// Proves the producer released its share of one exact pooled control buffer
// before the retained test witness is returned.
func assertReturnP2pProbeWitness(t *testing.T, witness []byte) {
	t.Helper()
	if !MessagePoolReturn(witness) {
		t.Error("probe producer retained its share of the exact pooled message")
	}
}

// Waits for an explicit lifecycle edge and fails with the supplied diagnostic
// rather than using state polling.
func waitP2pProbeBarrier(t *testing.T, barrier <-chan struct{}, failure string) {
	t.Helper()
	select {
	case <-barrier:
	case <-time.After(time.Second):
		t.Fatal(failure)
	}
}

// Receives one control message after an explicit producer edge, retaining a
// timeout only as a failed-test guard.
func receiveP2pProbeRouteMessage(t *testing.T, route <-chan []byte, failure string) []byte {
	t.Helper()
	select {
	case message := <-route:
		return message
	case <-time.After(time.Second):
		t.Fatal(failure)
		return nil
	}
}

// Moves ownership across one enabled relay or drops it at a disabled link.
// The endpoint callback receives the final owned message synchronously.
func (self *p2pProbeTestTopology) startRelay(
	source Route,
	destination Route,
	enabled *atomic.Bool,
	endpoint func([]byte),
) {
	self.relayWaitGroup.Add(1)
	go func() {
		defer self.relayWaitGroup.Done()
		for {
			select {
			case <-self.ctx.Done():
				return
			case message := <-source:
				if source == self.sourceRoute {
					self.observeSourceChallenge(message)
				}
				if !enabled.Load() {
					self.droppedMessageCount.Add(1)
					select {
					case self.droppedMessageUpdate <- struct{}{}:
					default:
					}
					MessagePoolReturn(message)
					continue
				}
				if destination == nil {
					endpoint(message)
					continue
				}
				select {
				case <-self.ctx.Done():
					MessagePoolReturn(message)
					return
				case destination <- message:
				}
			}
		}
	}()
}

// Retains only the value identity of the latest source request.
func (self *p2pProbeTestTopology) observeSourceChallenge(message []byte) {
	if recognized, messageType, streamId, nonce := decodeP2pStreamProbe(message); recognized && messageType == p2pStreamProbeRequestType && streamId == self.streamId {
		func() {
			self.challengeLock.Lock()
			defer self.challengeLock.Unlock()
			self.lastSourceChallenge = nonce
		}()
		select {
		case self.challengeUpdate <- struct{}{}:
		default:
		}
	}
}

// Terminates one forward-direction raw envelope or delivers application data.
func (self *p2pProbeTestTopology) destinationProbeMessage(message []byte) {
	if self.destinationProbe.handle(message) {
		MessagePoolReturn(message)
		return
	}
	select {
	case self.applicationAtDestination <- message:
	default:
		MessagePoolReturn(message)
	}
}

// Terminates one reverse-direction raw envelope or delivers application data.
func (self *p2pProbeTestTopology) sourceProbeMessage(message []byte) {
	if self.sourceProbe.handle(message) {
		MessagePoolReturn(message)
		return
	}
	select {
	case self.applicationAtSource <- message:
	default:
		MessagePoolReturn(message)
	}
}

// Returns the most recent challenge emitted by the source generation.
func (self *p2pProbeTestTopology) sourceChallengeNonce() Id {
	self.challengeLock.Lock()
	defer self.challengeLock.Unlock()
	return self.lastSourceChallenge
}

// Waits on challenge edges until a replacement generation emits a nonce that
// differs from the retired generation.
func (self *p2pProbeTestTopology) waitForSourceChallengeChange(
	previousNonce Id,
	timeout time.Duration,
) (Id, bool) {
	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()
	for {
		nonce := self.sourceChallengeNonce()
		if nonce != (Id{}) && nonce != previousNonce {
			return nonce, true
		}
		select {
		case <-self.challengeUpdate:
		case <-timeoutTimer.C:
			return nonce, false
		}
	}
}

// Changes one bidirectional intermediary link.
func (self *p2pProbeTestTopology) setLinkEnabled(linkIndex int, enabled bool) {
	self.linkEnableds[linkIndex].Store(enabled)
}

// Waits for one exact route count on immutable snapshot update edges rather
// than polling selector state or relying on scheduler timing.
func waitForP2pProbeRouteCount(
	writer MultiRouteWriter,
	want int,
	timeout time.Duration,
) (int, bool) {
	selector := writer.(*MultiRouteSelector)
	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()
	for {
		snapshot := selector.activeRoutesSnapshot.Load()
		got := len(snapshot.routes)
		if got == want {
			return got, true
		}
		select {
		case <-snapshot.notify:
		case <-timeoutTimer.C:
			return got, false
		}
	}
}

// Waits on coalesced drop edges until the intentional link-loss count reaches
// its target.
func (self *p2pProbeTestTopology) waitForDropCount(want int64, timeout time.Duration) bool {
	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()
	for self.droppedMessageCount.Load() < want {
		select {
		case <-self.droppedMessageUpdate:
		case <-timeoutTimer.C:
			return false
		}
	}
	return true
}

// Stops probes before relays so readiness withdrawal can finish synchronously.
func (self *p2pProbeTestTopology) close() {
	self.sourceProbe.close()
	self.destinationProbe.close()
	self.sourceRouteManager.CloseMultiRouteWriter(self.sourceWriter)
	self.destinationRouteManager.CloseMultiRouteWriter(self.destinationWriter)
	self.removeSourceAlias()
	self.removeDestinationAlias()
	self.cancel()
	self.relayWaitGroup.Wait()
	drain := func(route <-chan []byte) {
		for {
			select {
			case message := <-route:
				MessagePoolReturn(message)
			default:
				return
			}
		}
	}
	for _, route := range self.relayRoutes {
		drain(route)
	}
	drain(self.applicationAtSource)
	drain(self.applicationAtDestination)
}

// The reserved prefix cannot decode as a TransferFrame and round-trips only
// its exact type, stream, and nonce fields.
func TestP2pStreamProbeEnvelopeIsReservedInvalidProtobuf(t *testing.T) {
	streamId := NewId()
	nonce := NewId()
	message := encodeP2pStreamProbe(p2pStreamProbeRequestType, streamId, nonce)
	defer MessagePoolReturn(message)
	recognized, messageType, decodedStreamId, decodedNonce := decodeP2pStreamProbe(message)
	if !recognized || messageType != p2pStreamProbeRequestType ||
		decodedStreamId != streamId || decodedNonce != nonce {
		t.Fatalf(
			"probe decode recognized=%t type=%d stream=%s nonce=%s",
			recognized,
			messageType,
			decodedStreamId,
			decodedNonce,
		)
	}
	var transferFrame protocol.TransferFrame
	if err := ProtoUnmarshal(message, &transferFrame); err == nil {
		t.Fatal("reserved tag-zero probe decoded as an application TransferFrame")
	}
	if isP2pStreamProbe([]byte("ordinary application payload")) {
		t.Fatal("ordinary payload collided with the probe envelope")
	}
}

// A response producer admitted before route retirement cannot enqueue after
// retirement closes its generation; both sides relinquish their exact buffers.
func TestP2pStreamProbeResponseAdmissionJoinsRouteRetirement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultP2pTransportSettings()
	routeManager := NewRouteManager(ctx, "probe-response-retirement")
	streamId := NewId()
	route := make(Route, 1)
	transport := newP2pProbeTestSendTransport(NewId(), streamId, route, settings)
	probe := newStoppedP2pStreamProbe(ctx, routeManager, streamId, settings)
	probe.setSendRoute(transport, route)

	admittedMessage := make(chan []byte, 1)
	resumeProducer := make(chan struct{})
	var resumeOnce sync.Once
	releaseProducer := func() {
		resumeOnce.Do(func() {
			close(resumeProducer)
		})
	}
	defer releaseProducer()
	probe.testingAfterSendAdmission = func(
		messageType byte,
		routeEpoch uint64,
		message []byte,
	) {
		if messageType == p2pStreamProbeResponseType {
			admittedMessage <- MessagePoolShareReadOnly(message)
			<-resumeProducer
		}
	}
	generationClosed := make(chan struct{})
	probe.testingAfterRouteGenerationClosed = func(uint64) {
		close(generationClosed)
	}

	request := encodeP2pStreamProbe(p2pStreamProbeRequestType, streamId, NewId())
	handleDone := make(chan bool, 1)
	go func() {
		handleDone <- probe.handle(request)
	}()
	var response []byte
	select {
	case response = <-admittedMessage:
	case <-time.After(time.Second):
		t.Fatal("response producer did not enter route admission")
	}
	clearDone := make(chan struct{})
	go func() {
		probe.clearSendRoute(transport, route)
		close(clearDone)
	}()
	waitP2pProbeBarrier(t, generationClosed, "response route generation did not close")
	select {
	case <-clearDone:
		t.Fatal("route retirement bypassed its admitted response producer")
	default:
	}

	releaseProducer()
	select {
	case recognized := <-handleDone:
		if !recognized {
			t.Error("probe request was not recognized")
		}
	case <-time.After(time.Second):
		t.Fatal("response producer did not finish")
	}
	waitP2pProbeBarrier(t, clearDone, "response route retirement did not finish")
	select {
	case lateMessage := <-route:
		if !MessagePoolReturn(lateMessage) {
			t.Error("late response was not pool-owned")
		}
		t.Fatal("response enqueued after its route generation closed")
	default:
	}
	assertReturnP2pProbeWitness(t, response)
	if !MessagePoolReturn(request) {
		t.Fatal("request caller lost ownership of its exact pooled buffer")
	}
}

// The lifecycle's request producer obeys the same generation close/join edge
// as receive-side responses and drops its exact buffer before retirement ends.
func TestP2pStreamProbeRequestAdmissionJoinsRouteRetirement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultP2pTransportSettings()
	settings.EndToEndProbeInterval = time.Hour
	settings.EndToEndProbeTimeout = 2 * time.Hour
	routeManager := NewRouteManager(ctx, "probe-request-retirement")
	streamId := NewId()
	route := make(Route, 1)
	transport := newP2pProbeTestSendTransport(NewId(), streamId, route, settings)
	probe := newStoppedP2pStreamProbe(ctx, routeManager, streamId, settings)
	probe.setSendRoute(transport, route)

	admittedMessage := make(chan []byte, 1)
	resumeProducer := make(chan struct{})
	var resumeOnce sync.Once
	releaseProducer := func() {
		resumeOnce.Do(func() {
			close(resumeProducer)
		})
	}
	defer func() {
		releaseProducer()
		probe.cancel()
		select {
		case <-probe.done:
		case <-time.After(time.Second):
			t.Error("probe lifecycle did not stop during cleanup")
		}
	}()
	probe.testingAfterSendAdmission = func(
		messageType byte,
		routeEpoch uint64,
		message []byte,
	) {
		if messageType == p2pStreamProbeRequestType {
			admittedMessage <- MessagePoolShareReadOnly(message)
			<-resumeProducer
		}
	}
	generationClosed := make(chan struct{})
	probe.testingAfterRouteGenerationClosed = func(uint64) {
		close(generationClosed)
	}
	go HandleError(probe.run, probe.cancel)

	var request []byte
	select {
	case request = <-admittedMessage:
	case <-time.After(time.Second):
		t.Fatal("request producer did not enter route admission")
	}
	clearDone := make(chan struct{})
	go func() {
		probe.clearSendRoute(transport, route)
		close(clearDone)
	}()
	waitP2pProbeBarrier(t, generationClosed, "request route generation did not close")
	select {
	case <-clearDone:
		t.Fatal("route retirement bypassed its admitted request producer")
	default:
	}

	releaseProducer()
	waitP2pProbeBarrier(t, clearDone, "request route retirement did not finish")
	select {
	case lateMessage := <-route:
		if !MessagePoolReturn(lateMessage) {
			t.Error("late request was not pool-owned")
		}
		t.Fatal("request enqueued after its route generation closed")
	default:
	}
	assertReturnP2pProbeWitness(t, request)
}

// Physical send teardown closes probe admission before its final route drain
// and joins a producer paused at that boundary.
func TestP2pSendTransportFinalDrainJoinsAdmittedProbeProducer(t *testing.T) {
	transportCtx, cancelTransport := context.WithCancel(context.Background())
	defer cancelTransport()
	conn := newP2pProbeFastLoopConn()
	defer conn.Close()
	settings := DefaultP2pTransportSettings()
	streamId := NewId()
	transportInterface, route := newP2pSendTransportForPeer(
		transportCtx,
		cancelTransport,
		conn,
		NewId(),
		streamId,
		settings,
		true,
		nil,
	)
	transport := transportInterface.(*P2pSendTransport)
	admissionClosed := make(chan struct{})
	drainDone := make(chan struct{})
	transport.testingAfterProbeSendAdmissionClosed = func() {
		close(admissionClosed)
	}
	transport.testingAfterProbeSendDrain = func() {
		close(drainDone)
	}
	routeManager := NewRouteManager(transportCtx, "probe-physical-drain")
	probe := newStoppedP2pStreamProbe(transportCtx, routeManager, streamId, settings)
	probe.setSendRoute(transport, route)

	admittedMessage := make(chan []byte, 1)
	resumeProducer := make(chan struct{})
	var resumeOnce sync.Once
	releaseProducer := func() {
		resumeOnce.Do(func() {
			close(resumeProducer)
		})
	}
	defer releaseProducer()
	probe.testingAfterSendAdmission = func(
		messageType byte,
		routeEpoch uint64,
		message []byte,
	) {
		if messageType == p2pStreamProbeResponseType {
			admittedMessage <- MessagePoolShareReadOnly(message)
			<-resumeProducer
		}
	}
	request := encodeP2pStreamProbe(p2pStreamProbeRequestType, streamId, NewId())
	handleDone := make(chan bool, 1)
	go func() {
		handleDone <- probe.handle(request)
	}()
	var response []byte
	select {
	case response = <-admittedMessage:
	case <-time.After(time.Second):
		t.Fatal("physical-drain response did not enter admission")
	}
	cancelTransport()
	waitP2pProbeBarrier(t, admissionClosed, "physical probe admission did not close")
	select {
	case <-drainDone:
		t.Fatal("physical route drained before its admitted producer exited")
	default:
	}

	releaseProducer()
	select {
	case recognized := <-handleDone:
		if !recognized {
			t.Error("physical-drain request was not recognized")
		}
	case <-time.After(time.Second):
		t.Fatal("physical-drain response producer did not finish")
	}
	waitP2pProbeBarrier(t, drainDone, "physical route drain did not finish")
	assertReturnP2pProbeWitness(t, response)
	if !MessagePoolReturn(request) {
		t.Fatal("physical-drain caller lost its request buffer")
	}
	probe.clearSendRoute(transport, route)
}

// A readiness grant paused after its local bit flip cannot publish an old
// generation after synchronous route teardown has removed it.
func TestP2pStreamProbeStaleReadyGrantCannotReregisterClearedRoute(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultP2pTransportSettings()
	routeManager := NewRouteManager(ctx, "probe-stale-ready-grant")
	streamId := NewId()
	destination := DestinationId(NewId())
	route := make(Route, 1)
	transport := newP2pProbeTestSendTransport(
		destination.DestinationId,
		streamId,
		route,
		settings,
	)
	routeManager.UpdateTransport(transport, []Route{route})
	writer := routeManager.OpenMultiRouteWriter(destination)
	defer routeManager.CloseMultiRouteWriter(writer)
	probe := newStoppedP2pStreamProbe(ctx, routeManager, streamId, settings)
	probe.setSendRoute(transport, route)
	generation, _ := probe.sendRouteState()

	readyBitSet := make(chan struct{})
	resumeReady := make(chan struct{})
	var resumeOnce sync.Once
	releaseReady := func() {
		resumeOnce.Do(func() {
			close(resumeReady)
		})
	}
	defer releaseReady()
	probe.testingAfterReadyBitSet = func(Transport, Route, uint64) {
		close(readyBitSet)
		<-resumeReady
	}
	generationClosed := make(chan struct{})
	probe.testingAfterRouteGenerationClosed = func(uint64) {
		close(generationClosed)
	}
	readyDone := make(chan bool, 1)
	go func() {
		readyDone <- probe.setReady(transport, route, generation.epoch, true)
	}()
	waitP2pProbeBarrier(t, readyBitSet, "readiness bit did not enter its publication pause")
	clearDone := make(chan struct{})
	go func() {
		probe.clearSendRoute(transport, route)
		close(clearDone)
	}()
	waitP2pProbeBarrier(t, generationClosed, "readiness generation did not close")
	waitP2pProbeBarrier(t, clearDone, "route clear waited for an unenrolled stale grant")
	if transport.endToEndReady.Load() {
		t.Fatal("cleared route retained its local readiness bit")
	}
	if activeRoutes := writer.GetActiveRoutes(); len(activeRoutes) != 0 {
		t.Fatalf("cleared route retained %d writer routes", len(activeRoutes))
	}

	releaseReady()
	select {
	case granted := <-readyDone:
		if granted {
			t.Fatal("stale readiness grant reported publication success")
		}
	case <-time.After(time.Second):
		t.Fatal("stale readiness grant did not finish")
	}
	if transport.endToEndReady.Load() {
		t.Fatal("stale readiness grant restored its local bit")
	}
	if activeRoutes := writer.GetActiveRoutes(); len(activeRoutes) != 0 {
		t.Fatalf("stale readiness grant re-registered %d routes", len(activeRoutes))
	}
}

// A successful endpoint probe rematches the physical send route. That rematch
// must retain native-fast delivery semantics so the SendSequence continues to
// apply its bounded unreliable-carrier flight after readiness is granted.
func TestP2pStreamProbeReadyRematchPreservesFastCarrierProperties(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultP2pTransportSettings()
	settings.DataPlaneMode = P2pDataPlaneModeFastOnly
	routeManager := NewRouteManager(ctx, "probe-fast-properties")
	streamId := NewId()
	destination := DestinationId(NewId())
	route := make(Route, 1)
	transport := newP2pProbeTestSendTransport(
		destination.DestinationId,
		streamId,
		route,
		settings,
	)
	// Model the connected callback's pre-readiness publication. The probe
	// rematch below is the boundary that historically replaced these
	// properties with the zero value.
	routeManager.UpdateTransportWithProperties(
		transport,
		[]Route{route},
		p2pTransferCarrierProperties(transport),
	)
	writer := routeManager.OpenMultiRouteWriter(destination)
	defer routeManager.CloseMultiRouteWriter(writer)
	selector := writer.(*MultiRouteSelector)
	if selector.transferFlightPolicy().limited {
		t.Fatal("not-ready endpoint route entered the active Transfer flight policy")
	}

	probe := newStoppedP2pStreamProbe(ctx, routeManager, streamId, settings)
	probe.setSendRoute(transport, route)
	generation, _ := probe.sendRouteState()
	if !probe.setReady(transport, route, generation.epoch, true) {
		t.Fatal("native-fast route did not become probe-ready")
	}
	if !selector.transferFlightPolicy().limited {
		t.Fatal("probe readiness rematch erased native-fast carrier properties")
	}
	wantLimit := max(1, settings.ReceiveQueueMessageCount-1)
	if limit := selector.transferFlightPolicy().messageLimit; limit != wantLimit {
		t.Fatalf(
			"probe readiness flight limit = %d, want receive data depth %d",
			limit,
			wantLimit,
		)
	}
	probe.clearSendRoute(transport, route)
}

// Reusing one transport pointer for a later route epoch does not let a stale
// withdrawal clear the new generation's readiness or RouteManager entry.
func TestP2pStreamProbeStaleReadyWithdrawalPreservesSameTransportNewEpoch(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultP2pTransportSettings()
	routeManager := NewRouteManager(ctx, "probe-stale-ready-withdrawal")
	streamId := NewId()
	destination := DestinationId(NewId())
	route1 := make(Route, 1)
	route2 := make(Route, 1)
	transport := newP2pProbeTestSendTransport(
		destination.DestinationId,
		streamId,
		route1,
		settings,
	)
	routeManager.UpdateTransport(transport, []Route{route1})
	writer := routeManager.OpenMultiRouteWriter(destination)
	defer routeManager.CloseMultiRouteWriter(writer)
	probe := newStoppedP2pStreamProbe(ctx, routeManager, streamId, settings)
	probe.setSendRoute(transport, route1)
	generation1, _ := probe.sendRouteState()
	if !probe.setReady(transport, route1, generation1.epoch, true) {
		t.Fatal("first route generation did not become ready")
	}

	probe.setSendRoute(transport, route2)
	routeManager.UpdateTransport(transport, []Route{route2})
	generation2, _ := probe.sendRouteState()
	if generation2.epoch == generation1.epoch {
		t.Fatal("same transport route replacement did not advance its epoch")
	}
	if !probe.setReady(transport, route2, generation2.epoch, true) {
		t.Fatal("replacement route generation did not become ready")
	}
	if probe.setReady(transport, route1, generation1.epoch, false) {
		t.Fatal("stale route generation withdrew replacement readiness")
	}
	if !transport.endToEndReady.Load() {
		t.Fatal("stale withdrawal cleared the replacement readiness bit")
	}
	activeRoutes := writer.GetActiveRoutes()
	if len(activeRoutes) != 1 || activeRoutes[0] != route2 {
		t.Fatalf("stale withdrawal active routes=%v, want only replacement", activeRoutes)
	}
	probe.clearSendRoute(transport, route2)
}

// A permanently stalled observer consumes only one fixed dispatcher shard;
// receive handling and saturated diagnostic publication remain nonblocking.
func TestP2pStreamProbeBlockingObserverDoesNotBlockReceiveCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultP2pTransportSettings()
	observerEntered := make(chan struct{})
	releaseObserver := make(chan struct{})
	var observerOnce sync.Once
	settings.EndToEndProbeObserver = func(P2pStreamProbeEvent) {
		observerOnce.Do(func() {
			close(observerEntered)
		})
		<-releaseObserver
	}
	streamId := NewId()
	probe := newStoppedP2pStreamProbe(
		ctx,
		NewRouteManager(ctx, "probe-blocked-observer"),
		streamId,
		settings,
	)
	firstDispatchDone := make(chan struct{})
	go func() {
		probe.observe(P2pStreamProbeEventRouteReady, Id{}, 1)
		close(firstDispatchDone)
	}()
	waitP2pProbeBarrier(t, observerEntered, "observer worker did not enter callback")
	waitP2pProbeBarrier(t, firstDispatchDone, "first observer dispatch blocked its caller")

	saturationDone := make(chan struct{})
	go func() {
		for eventIndex := range 2 * p2pStreamProbeObserverQueueSize {
			probe.observe(P2pStreamProbeEventRequestReceived, Id{}, uint64(eventIndex+2))
		}
		close(saturationDone)
	}()
	waitP2pProbeBarrier(t, saturationDone, "saturated observer queue blocked publication")

	request := encodeP2pStreamProbe(p2pStreamProbeRequestType, streamId, NewId())
	handleDone := make(chan bool, 1)
	go func() {
		handleDone <- probe.handle(request)
	}()
	select {
	case recognized := <-handleDone:
		if !recognized {
			t.Error("request was not recognized while observer stalled")
		}
	case <-time.After(time.Second):
		close(releaseObserver)
		t.Fatal("stalled observer blocked the receive callback")
	}
	if !MessagePoolReturn(request) {
		close(releaseObserver)
		t.Fatal("observer test caller lost its request buffer")
	}
	close(releaseObserver)
}

// Opaque forwarding stays symmetric and promotes both endpoint writers at
// every supported topology depth represented by the regression harness.
func TestP2pStreamProbeReadyAcrossOneThreeFiveAndNineHops(t *testing.T) {
	for _, hopCount := range []int{1, 3, 5, 9} {
		topology := newP2pProbeTestTopology(t, hopCount, -1)
		if got, ok := waitForP2pProbeRouteCount(topology.sourceWriter, 1, time.Second); !ok {
			topology.close()
			t.Fatalf("%d-hop source routes=%d, want 1", hopCount, got)
		}
		if got, ok := waitForP2pProbeRouteCount(topology.destinationWriter, 1, time.Second); !ok {
			topology.close()
			t.Fatalf("%d-hop destination routes=%d, want 1", hopCount, got)
		}
		payload := []byte{byte(hopCount), 2, 3, 4}
		if err := topology.sourceWriter.Write(
			topology.ctx,
			MessagePoolCopy(payload),
			time.Second,
		); err != nil {
			topology.close()
			t.Fatalf("%d-hop application write: %v", hopCount, err)
		}
		select {
		case message := <-topology.applicationAtDestination:
			if !bytes.Equal(message, payload) {
				MessagePoolReturn(message)
				topology.close()
				t.Fatalf("%d-hop application payload changed", hopCount)
			}
			MessagePoolReturn(message)
		case <-time.After(time.Second):
			topology.close()
			t.Fatalf("%d-hop application payload did not cross opaque relays", hopCount)
		}
		reversePayload := []byte{byte(hopCount), 5, 6, 7}
		if err := topology.destinationWriter.Write(
			topology.ctx,
			MessagePoolCopy(reversePayload),
			time.Second,
		); err != nil {
			topology.close()
			t.Fatalf("%d-hop reverse application write: %v", hopCount, err)
		}
		select {
		case message := <-topology.applicationAtSource:
			if !bytes.Equal(message, reversePayload) {
				MessagePoolReturn(message)
				topology.close()
				t.Fatalf("%d-hop reverse application payload changed", hopCount)
			}
			MessagePoolReturn(message)
		case <-time.After(time.Second):
			topology.close()
			t.Fatalf("%d-hop reverse payload did not cross opaque relays", hopCount)
		}
		topology.close()
	}
}

// An incomplete middle link leaves ordinary traffic on the exchange route;
// enabling it later promotes the stream without losing the logical payload.
func TestP2pStreamProbeIncompleteMiddleHopIsNeverApplicationEligible(t *testing.T) {
	topology := newP2pProbeTestTopology(t, 3, 1)
	defer topology.close()
	gatewayTransport := NewSendGatewayTransport()
	gatewayRoute := make(Route, 16)
	topology.sourceRouteManager.UpdateTransport(gatewayTransport, []Route{gatewayRoute})
	defer topology.sourceRouteManager.RemoveTransport(gatewayTransport)

	if !topology.waitForDropCount(4, time.Second) {
		t.Fatal("incomplete middle link did not observe repeated challenges")
	}
	if got := len(topology.sourceWriter.GetActiveRoutes()); got != 1 {
		t.Fatalf("incomplete path source routes=%d, want exchange only", got)
	}
	if got := len(topology.destinationWriter.GetActiveRoutes()); got != 0 {
		t.Fatalf("incomplete path destination routes=%d, want 0", got)
	}
	payload := []byte("logical payload stays on exchange")
	if err := topology.sourceWriter.Write(
		topology.ctx,
		MessagePoolCopy(payload),
		time.Second,
	); err != nil {
		t.Fatal(err)
	}
	select {
	case message := <-gatewayRoute:
		if !bytes.Equal(message, payload) {
			MessagePoolReturn(message)
			t.Fatal("exchange fallback payload changed")
		}
		MessagePoolReturn(message)
	case <-time.After(time.Second):
		t.Fatal("logical payload did not use exchange fallback")
	}
	select {
	case message := <-topology.applicationAtDestination:
		MessagePoolReturn(message)
		t.Fatal("incomplete stream received ordinary application traffic")
	default:
	}

	topology.setLinkEnabled(1, true)
	if got, ok := waitForP2pProbeRouteCount(topology.sourceWriter, 2, time.Second); !ok {
		t.Fatalf("completed source routes=%d, want exchange plus stream", got)
	}
	if got, ok := waitForP2pProbeRouteCount(topology.destinationWriter, 1, time.Second); !ok {
		t.Fatalf("completed destination routes=%d, want stream", got)
	}
}

// A complete route loses both endpoint leases after a middle failure, and the
// same transport generations require new round trips before re-promotion.
func TestP2pStreamProbeEstablishedMiddleLossWithdrawsAndRepromotes(t *testing.T) {
	topology := newP2pProbeTestTopology(t, 3, -1)
	defer topology.close()
	if got, ok := waitForP2pProbeRouteCount(topology.sourceWriter, 1, time.Second); !ok {
		t.Fatalf("initial source routes=%d, want 1", got)
	}
	if got, ok := waitForP2pProbeRouteCount(topology.destinationWriter, 1, time.Second); !ok {
		t.Fatalf("initial destination routes=%d, want 1", got)
	}

	topology.setLinkEnabled(1, false)
	if got, ok := waitForP2pProbeRouteCount(topology.sourceWriter, 0, time.Second); !ok {
		t.Fatalf("expired source routes=%d, want 0", got)
	}
	if got, ok := waitForP2pProbeRouteCount(topology.destinationWriter, 0, time.Second); !ok {
		t.Fatalf("expired destination routes=%d, want 0", got)
	}

	topology.setLinkEnabled(1, true)
	if got, ok := waitForP2pProbeRouteCount(topology.sourceWriter, 1, time.Second); !ok {
		t.Fatalf("re-promoted source routes=%d, want 1", got)
	}
	if got, ok := waitForP2pProbeRouteCount(topology.destinationWriter, 1, time.Second); !ok {
		t.Fatalf("re-promoted destination routes=%d, want 1", got)
	}
}

// Matching responses, including duplicates from an interval-boundary retry,
// restart the quiet interval before the next nonce is sent.
func TestP2pStreamProbeSuccessfulResponsesRearmQuietInterval(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultP2pTransportSettings()
	settings.EndToEndProbeInterval = 5 * time.Millisecond
	settings.EndToEndProbeTimeout = 50 * time.Millisecond
	routeManager := NewRouteManager(ctx, "probe-paced-response")
	streamId := NewId()
	destinationId := NewId()
	route := make(Route, 2)
	transport := newP2pProbeTestSendTransport(destinationId, streamId, route, settings)
	routeManager.UpdateTransport(transport, []Route{route})
	removeAlias := routeManager.AddWriterDestinationAlias(
		DestinationId(destinationId),
		StreamId(streamId),
	)
	defer removeAlias()
	writer := routeManager.OpenMultiRouteWriter(DestinationId(destinationId))
	defer routeManager.CloseMultiRouteWriter(writer)
	probe := newStoppedP2pStreamProbe(ctx, routeManager, streamId, settings)
	timerTicks := make(chan time.Time, 1)
	timerResets := make(chan time.Duration, 4)
	probe.testingProbeTimer = timerTicks
	probe.testingAfterProbeTimerReset = func(interval time.Duration) {
		timerResets <- interval
	}
	probe.setSendRoute(transport, route)
	go HandleError(probe.run, probe.cancel)
	defer func() {
		probe.close()
		probe.clearSendRoute(transport, route)
	}()

	select {
	case interval := <-timerResets:
		if interval != settings.EndToEndProbeInterval {
			t.Fatalf("initial challenge interval=%s", interval)
		}
	case <-time.After(time.Second):
		t.Fatal("initial challenge timer was not armed")
	}
	request := receiveP2pProbeRouteMessage(t, route, "initial challenge was not queued")
	recognized, messageType, _, nonce := decodeP2pStreamProbe(request)
	if !recognized || messageType != p2pStreamProbeRequestType {
		MessagePoolReturn(request)
		t.Fatal("initial challenge was not a request")
	}
	if !MessagePoolReturn(request) {
		t.Fatal("initial challenge ownership was not returned")
	}
	response := encodeP2pStreamProbe(p2pStreamProbeResponseType, streamId, nonce)
	if !probe.handle(response) {
		MessagePoolReturn(response)
		t.Fatal("challenge response was not recognized")
	}
	if !MessagePoolReturn(response) {
		t.Fatal("response caller lost its pooled buffer")
	}
	if got, ok := waitForP2pProbeRouteCount(writer, 1, time.Second); !ok {
		t.Fatalf("responsive route count=%d, want 1", got)
	}
	select {
	case interval := <-timerResets:
		if interval != settings.EndToEndProbeInterval {
			t.Fatalf("response quiet interval=%s", interval)
		}
	case <-time.After(time.Second):
		t.Fatal("response did not rearm the quiet interval")
	}
	select {
	case unexpectedRequest := <-route:
		MessagePoolReturn(unexpectedRequest)
		t.Fatal("response immediately started another challenge")
	default:
	}

	duplicateResponse := encodeP2pStreamProbe(p2pStreamProbeResponseType, streamId, nonce)
	if !probe.handle(duplicateResponse) {
		MessagePoolReturn(duplicateResponse)
		t.Fatal("duplicate challenge response was not recognized")
	}
	if !MessagePoolReturn(duplicateResponse) {
		t.Fatal("duplicate response caller lost its pooled buffer")
	}
	select {
	case interval := <-timerResets:
		if interval != settings.EndToEndProbeInterval {
			t.Fatalf("duplicate response quiet interval=%s", interval)
		}
	case <-time.After(time.Second):
		t.Fatal("duplicate response did not extend the quiet interval")
	}

	timerTicks <- time.Time{}
	select {
	case interval := <-timerResets:
		if interval != settings.EndToEndProbeInterval {
			t.Fatalf("renewed challenge interval=%s", interval)
		}
	case <-time.After(time.Second):
		t.Fatal("interval edge did not arm the next challenge")
	}
	request = receiveP2pProbeRouteMessage(t, route, "renewed challenge was not queued")
	recognized, messageType, _, renewedNonce := decodeP2pStreamProbe(request)
	if !recognized || messageType != p2pStreamProbeRequestType {
		MessagePoolReturn(request)
		t.Fatal("renewed challenge was not a request")
	}
	if renewedNonce == nonce {
		MessagePoolReturn(request)
		t.Fatal("renewed challenge reused the completed nonce")
	}
	if !MessagePoolReturn(request) {
		t.Fatal("renewed challenge ownership was not returned")
	}
}

// A matching response queued while the interval branch is already selected
// wins before that branch can emit a duplicate challenge.
func TestP2pStreamProbeIntervalEdgePrefersQueuedResponse(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultP2pTransportSettings()
	settings.EndToEndProbeInterval = 5 * time.Millisecond
	settings.EndToEndProbeTimeout = 50 * time.Millisecond
	routeManager := NewRouteManager(ctx, "probe-interval-response-race")
	streamId := NewId()
	destinationId := NewId()
	route := make(Route, 2)
	transport := newP2pProbeTestSendTransport(destinationId, streamId, route, settings)
	routeManager.UpdateTransport(transport, []Route{route})
	removeAlias := routeManager.AddWriterDestinationAlias(
		DestinationId(destinationId),
		StreamId(streamId),
	)
	defer removeAlias()
	writer := routeManager.OpenMultiRouteWriter(DestinationId(destinationId))
	defer routeManager.CloseMultiRouteWriter(writer)
	probe := newStoppedP2pStreamProbe(ctx, routeManager, streamId, settings)
	timerTicks := make(chan time.Time, 1)
	timerResets := make(chan time.Duration, 2)
	timerEdgeReached := make(chan struct{})
	releaseTimerEdge := make(chan struct{})
	probe.testingProbeTimer = timerTicks
	probe.testingAfterProbeTimerReset = func(interval time.Duration) {
		timerResets <- interval
	}
	probe.testingAfterProbeTimerEdge = func() {
		close(timerEdgeReached)
		<-releaseTimerEdge
	}
	probe.setSendRoute(transport, route)
	go HandleError(probe.run, probe.cancel)
	defer func() {
		probe.close()
		probe.clearSendRoute(transport, route)
	}()

	select {
	case interval := <-timerResets:
		if interval != settings.EndToEndProbeInterval {
			t.Fatalf("initial challenge interval=%s", interval)
		}
	case <-time.After(time.Second):
		t.Fatal("initial challenge timer was not armed")
	}
	request := receiveP2pProbeRouteMessage(t, route, "initial challenge was not queued")
	recognized, messageType, _, nonce := decodeP2pStreamProbe(request)
	if !recognized || messageType != p2pStreamProbeRequestType {
		MessagePoolReturn(request)
		t.Fatal("initial challenge was not a request")
	}
	if !MessagePoolReturn(request) {
		t.Fatal("initial challenge ownership was not returned")
	}

	timerTicks <- time.Time{}
	select {
	case <-timerEdgeReached:
	case <-time.After(time.Second):
		t.Fatal("interval branch did not reach the response barrier")
	}
	response := encodeP2pStreamProbe(p2pStreamProbeResponseType, streamId, nonce)
	if !probe.handle(response) {
		MessagePoolReturn(response)
		t.Fatal("challenge response was not recognized")
	}
	if !MessagePoolReturn(response) {
		t.Fatal("response caller lost its pooled buffer")
	}
	close(releaseTimerEdge)
	select {
	case interval := <-timerResets:
		if interval != settings.EndToEndProbeInterval {
			t.Fatalf("response quiet interval=%s", interval)
		}
	case <-time.After(time.Second):
		t.Fatal("queued response did not rearm the quiet interval")
	}
	if got, ok := waitForP2pProbeRouteCount(writer, 1, time.Second); !ok {
		t.Fatalf("responsive route count=%d, want 1", got)
	}
	select {
	case duplicateRequest := <-route:
		MessagePoolReturn(duplicateRequest)
		t.Fatal("interval edge emitted a duplicate challenge before its queued response")
	default:
	}
}

// A false edge clears one exact generation synchronously. A true edge and an
// old response cannot restore it; only a challenge from the new epoch can.
func TestP2pStreamProbeSameGenerationReconnectRequiresFreshRoundTrip(t *testing.T) {
	topology := newP2pProbeTestTopology(t, 3, -1)
	defer topology.close()
	if got, ok := waitForP2pProbeRouteCount(topology.sourceWriter, 1, time.Second); !ok {
		t.Fatalf("initial source routes=%d, want 1", got)
	}
	oldNonce := topology.sourceChallengeNonce()
	if oldNonce == (Id{}) {
		t.Fatal("initial source challenge nonce was not observed")
	}

	topology.sourceProbe.clearSendRoute(topology.sourceTransport, topology.sourceRoute)
	if got := len(topology.sourceWriter.GetActiveRoutes()); got != 0 {
		t.Fatalf("synchronous disconnect left %d source routes", got)
	}
	topology.setLinkEnabled(1, false)
	topology.sourceRouteManager.UpdateTransport(
		topology.sourceTransport,
		[]Route{topology.sourceRoute},
	)
	topology.sourceProbe.setSendRoute(topology.sourceTransport, topology.sourceRoute)

	if _, changed := topology.waitForSourceChallengeChange(oldNonce, time.Second); !changed {
		t.Fatal("reconnect did not rotate its challenge nonce")
	}
	staleResponse := encodeP2pStreamProbe(
		p2pStreamProbeResponseType,
		topology.streamId,
		oldNonce,
	)
	if !topology.sourceProbe.handle(staleResponse) {
		MessagePoolReturn(staleResponse)
		t.Fatal("stale response was not recognized as control traffic")
	}
	MessagePoolReturn(staleResponse)
	if !topology.waitForDropCount(1, time.Second) {
		t.Fatal("reconnected generation did not issue a fresh challenge")
	}
	if got := len(topology.sourceWriter.GetActiveRoutes()); got != 0 {
		t.Fatalf("stale response restored %d source routes", got)
	}

	topology.setLinkEnabled(1, true)
	if got, ok := waitForP2pProbeRouteCount(topology.sourceWriter, 1, time.Second); !ok {
		t.Fatalf("fresh round trip source routes=%d, want 1", got)
	}
}

// Readiness is exact to a transport generation even when overlapping
// generations advertise the same stream id and one-hop peer shortcut.
func TestP2pStreamProbeOverlappingSameStreamGenerationsAreIndependent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	routeManager := NewRouteManager(ctx, "probe-generations")
	streamId := NewId()
	destinationId := NewId()
	settings := DefaultP2pTransportSettings()
	route1 := make(Route, 1)
	route2 := make(Route, 1)
	transport1 := newP2pProbeTestSendTransport(destinationId, streamId, route1, settings)
	transport2 := newP2pProbeTestSendTransport(destinationId, streamId, route2, settings)
	routeManager.UpdateTransport(transport1, []Route{route1})
	routeManager.UpdateTransport(transport2, []Route{route2})
	removeAlias := routeManager.AddWriterDestinationAlias(
		DestinationId(destinationId),
		StreamId(streamId),
	)
	defer removeAlias()
	writer := routeManager.OpenMultiRouteWriter(DestinationId(destinationId))
	defer routeManager.CloseMultiRouteWriter(writer)
	if got := len(writer.GetActiveRoutes()); got != 0 {
		t.Fatalf("unprobed generations routes=%d, want 0", got)
	}

	transport1.setEndToEndReady(true)
	routeManager.UpdateTransport(transport1, []Route{route1})
	if got := len(writer.GetActiveRoutes()); got != 1 {
		t.Fatalf("first ready generation routes=%d, want 1", got)
	}
	transport2.setEndToEndReady(true)
	routeManager.UpdateTransport(transport2, []Route{route2})
	if got := len(writer.GetActiveRoutes()); got != 2 {
		t.Fatalf("two ready generations routes=%d, want 2", got)
	}
	transport1.setEndToEndReady(false)
	routeManager.RemoveTransport(transport1)
	if got := len(writer.GetActiveRoutes()); got != 1 {
		t.Fatalf("first withdrawn generation routes=%d, want 1", got)
	}
	transport2.setEndToEndReady(false)
	routeManager.RemoveTransport(transport2)
	if got := len(writer.GetActiveRoutes()); got != 0 {
		t.Fatalf("all withdrawn generations routes=%d, want 0", got)
	}
}

// Two logical destinations may independently share one verified stream. Each
// ref-counted alias can be removed without changing the other writer.
func TestP2pStreamProbeMultipleDestinationAliasesShareReadyTransport(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	routeManager := NewRouteManager(ctx, "probe-aliases")
	streamId := NewId()
	destinationId1 := NewId()
	destinationId2 := NewId()
	settings := DefaultP2pTransportSettings()
	route := make(Route, 2)
	transport := newP2pProbeTestSendTransport(NewId(), streamId, route, settings)
	transport.setEndToEndReady(true)
	routeManager.UpdateTransport(transport, []Route{route})
	removeAlias1 := routeManager.AddWriterDestinationAlias(
		DestinationId(destinationId1),
		StreamId(streamId),
	)
	removeAlias2 := routeManager.AddWriterDestinationAlias(
		DestinationId(destinationId2),
		StreamId(streamId),
	)
	writer1 := routeManager.OpenMultiRouteWriter(DestinationId(destinationId1))
	writer2 := routeManager.OpenMultiRouteWriter(DestinationId(destinationId2))
	defer routeManager.CloseMultiRouteWriter(writer1)
	defer routeManager.CloseMultiRouteWriter(writer2)
	if got1, got2 := len(writer1.GetActiveRoutes()), len(writer2.GetActiveRoutes()); got1 != 1 || got2 != 1 {
		t.Fatalf("shared alias routes=(%d,%d), want (1,1)", got1, got2)
	}

	removeAlias1()
	if got1, got2 := len(writer1.GetActiveRoutes()), len(writer2.GetActiveRoutes()); got1 != 0 || got2 != 1 {
		t.Fatalf("first alias removal routes=(%d,%d), want (0,1)", got1, got2)
	}
	removeAlias2()
	if got1, got2 := len(writer1.GetActiveRoutes()), len(writer2.GetActiveRoutes()); got1 != 0 || got2 != 0 {
		t.Fatalf("all alias removal routes=(%d,%d), want (0,0)", got1, got2)
	}
}

// StreamSequence.Cancel waits through P2pTransport.close, so its endpoint
// readiness route is absent before the synchronous call returns.
func TestP2pStreamProbeStreamSequenceCancelSynchronouslyWithdrawsReadiness(t *testing.T) {
	if runtime.GOOS == "js" {
		t.Skip("native loopback WebRTC lifecycle")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	webRtcSettingsA := DefaultWebRtcSettings()
	webRtcSettingsB := DefaultWebRtcSettings()
	webRtcSettingsA.Log = NewNoopLogger()
	webRtcSettingsB.Log = NewNoopLogger()
	webRtcSettingsA.IceServerUrls = nil
	webRtcSettingsB.IceServerUrls = nil
	webRtcSettingsA.UseLoopbackOnlyIceInterfaces = true
	webRtcSettingsB.UseLoopbackOnlyIceInterfaces = true
	signalPipeA := newP2pProbeSignalPipe()
	signalPipeB := newP2pProbeSignalPipe()
	webRtcManagerA := newTestWebRtcManager(t, ctx, signalPipeA, webRtcSettingsA)
	webRtcManagerB := newTestWebRtcManager(t, ctx, signalPipeB, webRtcSettingsB)
	signalPipeA.SetSignalReceiver(webRtcManagerB)
	signalPipeB.SetSignalReceiver(webRtcManagerA)

	clientSettingsA := DefaultClientSettings()
	clientSettingsB := DefaultClientSettings()
	clientSettingsA.Log = NewNoopLogger()
	clientSettingsB.Log = NewNoopLogger()
	clientA := NewClient(ctx, NewId(), NewNoContractClientOob(), clientSettingsA)
	clientB := NewClient(ctx, NewId(), NewNoContractClientOob(), clientSettingsB)
	defer clientA.Close()
	defer clientB.Close()
	streamSettingsA := DefaultStreamManagerSettings()
	streamSettingsB := DefaultStreamManagerSettings()
	streamSettingsA.StreamBufferSettings.P2pTransportSettings.DataPlaneMode =
		P2pDataPlaneModeLegacyOnly
	streamSettingsB.StreamBufferSettings.P2pTransportSettings.DataPlaneMode =
		P2pDataPlaneModeLegacyOnly
	streamSettingsA.StreamBufferSettings.P2pTransportSettings.EndToEndProbeInterval =
		5 * time.Millisecond
	streamSettingsB.StreamBufferSettings.P2pTransportSettings.EndToEndProbeInterval =
		5 * time.Millisecond
	streamSettingsA.StreamBufferSettings.P2pTransportSettings.EndToEndProbeTimeout =
		100 * time.Millisecond
	streamSettingsB.StreamBufferSettings.P2pTransportSettings.EndToEndProbeTimeout =
		100 * time.Millisecond
	streamManagerA := NewStreamManager(ctx, clientA, webRtcManagerA, streamSettingsA)
	streamManagerB := NewStreamManager(ctx, clientB, webRtcManagerB, streamSettingsB)
	streamId := NewId()
	clientAId := clientA.ClientId()
	clientBId := clientB.ClientId()
	signalPipeA.setSignalSource(clientAId)
	signalPipeB.setSignalSource(clientBId)
	passivePeerRegistered := make(chan struct{})
	var passivePeerRegisteredOnce sync.Once
	webRtcManagerB.testingAfterPeerConnRegistered = func(path TransferPath, active bool) {
		if !active && path.DestinationId == clientAId && path.StreamId == streamId {
			passivePeerRegisteredOnce.Do(func() {
				close(passivePeerRegistered)
			})
		}
	}
	sequenceB := NewStreamSequence(
		ctx,
		streamManagerB,
		&clientAId,
		nil,
		streamId,
		streamSettingsB.StreamBufferSettings,
	)
	go sequenceB.Run()
	select {
	case <-passivePeerRegistered:
	case <-time.After(5 * time.Second):
		sequenceB.CloseAndWait()
		t.Fatal("passive StreamSequence did not register its peer connection")
	}
	sequenceA := NewStreamSequence(
		ctx,
		streamManagerA,
		nil,
		&clientBId,
		streamId,
		streamSettingsA.StreamBufferSettings,
	)
	go sequenceA.Run()
	defer sequenceB.CloseAndWait()

	removeAliasA := clientA.RouteManager().AddWriterDestinationAlias(
		DestinationId(clientBId),
		StreamId(streamId),
	)
	defer removeAliasA()
	removeAliasB := clientB.RouteManager().AddWriterDestinationAlias(
		DestinationId(clientAId),
		StreamId(streamId),
	)
	defer removeAliasB()
	writerA := clientA.RouteManager().OpenMultiRouteWriter(DestinationId(clientBId))
	writerB := clientB.RouteManager().OpenMultiRouteWriter(DestinationId(clientAId))
	defer clientA.RouteManager().CloseMultiRouteWriter(writerA)
	defer clientB.RouteManager().CloseMultiRouteWriter(writerB)
	if got, ok := waitForP2pProbeRouteCount(writerA, 1, 10*time.Second); !ok {
		sequenceA.CloseAndWait()
		t.Fatalf("active StreamSequence routes=%d, want 1", got)
	}
	if got, ok := waitForP2pProbeRouteCount(writerB, 1, 10*time.Second); !ok {
		sequenceA.CloseAndWait()
		t.Fatalf("passive StreamSequence routes=%d, want 1", got)
	}

	sequenceA.CloseAndWait()
	if got := len(writerA.GetActiveRoutes()); got != 0 {
		t.Fatalf("StreamSequence.CloseAndWait returned with %d readiness routes", got)
	}
}

// A synchronous in-memory signaling bridge keeps the lifecycle test isolated
// from the platform control path.
type p2pProbeSignalPipe struct {
	stateLock sync.Mutex
	receiver  SignalReceiver
	sourceId  Id
}

// Allocates one unconnected signaling direction.
func newP2pProbeSignalPipe() *p2pProbeSignalPipe {
	return &p2pProbeSignalPipe{}
}

// Changes the receiver used by subsequent synchronous sends.
func (self *p2pProbeSignalPipe) SetSignalReceiver(receiver SignalReceiver) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.receiver = receiver
}

// Fixes the sender identity before either StreamSequence can emit signaling,
// avoiding a receiver-state lookup that races peer teardown.
func (self *p2pProbeSignalPipe) setSignalSource(sourceId Id) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.sourceId = sourceId
}

// Borrows the fixed sender and current receiver without holding the pipe lock
// through its callback.
func (self *p2pProbeSignalPipe) signalState() (Id, SignalReceiver) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.sourceId, self.receiver
}

// Delivers one signaling frame synchronously from the pipe's fixed sender.
// Source reconstruction must not depend on the receiver's transient peer map.
func (self *p2pProbeSignalPipe) SendSignal(
	_ Id,
	signal *protocol.Frame,
	opts ...any,
) {
	defer MessagePoolReturn(signal.MessageBytes)
	sourceId, receiver := self.signalState()
	if receiver != nil {
		if sourceId == (Id{}) {
			panic("in-memory probe signal source is not configured")
		}
		exchangeSignals := &protocol.ExchangeSignals{}
		if err := ProtoUnmarshal(signal.MessageBytes, exchangeSignals); err != nil {
			panic(err)
		}
		streamId, err := IdFromBytes(exchangeSignals.StreamId)
		if err != nil {
			panic(err)
		}
		receiver.ReceiveSignal(
			TransferPath{
				SourceId: sourceId,
				StreamId: streamId,
			},
			testingSignalTransferKey(opts),
			signal,
		)
	}
}

// An older endpoint that never echoes remains on fallback and switches from
// the discovery interval to the compatibility timeout after a controlled edge.
func TestP2pStreamProbeLegacyPeerStaysUnreadyAndBacksOff(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultP2pTransportSettings()
	settings.EndToEndProbeInterval = 10 * time.Millisecond
	settings.EndToEndProbeTimeout = 40 * time.Millisecond
	routeManager := NewRouteManager(ctx, "probe-legacy")
	streamId := NewId()
	destinationId := NewId()
	route := make(Route, 8)
	transport := newP2pProbeTestSendTransport(destinationId, streamId, route, settings)
	routeManager.UpdateTransport(transport, []Route{route})
	removeAlias := routeManager.AddWriterDestinationAlias(
		DestinationId(destinationId),
		StreamId(streamId),
	)
	defer removeAlias()
	writer := routeManager.OpenMultiRouteWriter(DestinationId(destinationId))
	defer routeManager.CloseMultiRouteWriter(writer)
	probe := newStoppedP2pStreamProbe(ctx, routeManager, streamId, settings)
	clockNanos := atomic.Int64{}
	clockNanos.Store(int64(time.Second))
	timerTicks := make(chan time.Time, 1)
	timerResets := make(chan time.Duration, 2)
	probe.testingNow = func() time.Time {
		return time.Unix(0, clockNanos.Load())
	}
	probe.testingProbeTimer = timerTicks
	probe.testingAfterProbeTimerReset = func(interval time.Duration) {
		timerResets <- interval
	}
	probe.setSendRoute(transport, route)
	go HandleError(probe.run, probe.cancel)
	defer func() {
		probe.close()
		probe.clearSendRoute(transport, route)
	}()

	select {
	case interval := <-timerResets:
		if interval != settings.EndToEndProbeInterval {
			t.Fatalf("legacy discovery interval=%s", interval)
		}
	case <-time.After(time.Second):
		t.Fatal("legacy discovery timer was not armed")
	}
	request := receiveP2pProbeRouteMessage(t, route, "legacy discovery challenge was not queued")
	if !MessagePoolReturn(request) {
		t.Fatal("legacy discovery challenge was not pool-owned")
	}
	clockNanos.Add(int64(settings.EndToEndProbeTimeout))
	timerTicks <- time.Time{}
	select {
	case interval := <-timerResets:
		if interval != settings.EndToEndProbeTimeout {
			t.Fatalf("legacy compatibility interval=%s", interval)
		}
	case <-time.After(time.Second):
		t.Fatal("legacy compatibility timer was not armed")
	}
	request = receiveP2pProbeRouteMessage(t, route, "legacy compatibility challenge was not queued")
	if !MessagePoolReturn(request) {
		t.Fatal("legacy compatibility challenge was not pool-owned")
	}
	if got := len(writer.GetActiveRoutes()); got != 0 {
		t.Fatalf("legacy peer routes=%d, want fallback only", got)
	}
}

// The legacy reader consumes a raw probe before route delivery and counts only
// the ordinary message that follows it.
func TestP2pStreamProbeLegacyReceiveHandlerExcludesDataPlaneStats(t *testing.T) {
	streamId := NewId()
	probeTemplate := encodeP2pStreamProbe(p2pStreamProbeRequestType, streamId, NewId())
	defer MessagePoolReturn(probeTemplate)
	fenceNonce := NewId()
	fenceTemplate := encodeP2pStreamProbe(p2pStreamProbeRequestType, streamId, fenceNonce)
	defer MessagePoolReturn(fenceTemplate)
	applicationMessage := []byte("legacy application frame")
	conn := newQueuedMessageConn(probeTemplate, applicationMessage, fenceTemplate)
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stats := &P2pDataPlaneStats{}
	settings := DefaultP2pTransportSettings()
	settings.DataPlaneMode = P2pDataPlaneModeLegacyOnly
	settings.DataPlaneStats = stats
	var handledProbeCount atomic.Int64
	fenceHandled := make(chan struct{})
	var fenceHandledOnce sync.Once
	_, receiveRoute := newP2pReceiveTransport(
		ctx,
		cancel,
		conn,
		streamId,
		settings,
		nil,
		func(message []byte) bool {
			if recognized, _, _, nonce := decodeP2pStreamProbe(message); recognized {
				handledProbeCount.Add(1)
				if nonce == fenceNonce {
					fenceHandledOnce.Do(func() {
						close(fenceHandled)
					})
				}
				return true
			}
			return false
		},
	)
	select {
	case message := <-receiveRoute:
		if !bytes.Equal(message, applicationMessage) {
			MessagePoolReturn(message)
			t.Fatal("legacy receive handler delivered the probe")
		}
		MessagePoolReturn(message)
	case <-time.After(time.Second):
		t.Fatal("legacy receive handler did not deliver application data")
	}
	waitP2pProbeBarrier(t, fenceHandled, "legacy receive stats fence was not handled")
	snapshot := stats.Snapshot()
	if handledProbeCount.Load() != 2 ||
		snapshot.LegacyReceiveMessageCount != 1 ||
		snapshot.LegacyReceiveByteCount != uint64(len(applicationMessage)) ||
		snapshot.FastReceiveMessageCount != 0 ||
		snapshot.FastDropCount != 0 {
		t.Fatalf(
			"legacy probe handler count=%d stats=%+v",
			handledProbeCount.Load(),
			snapshot,
		)
	}
}

// Prefetched ownership is transferred exactly once: the probe is consumed,
// while the adjacent ordinary buffer is handed unchanged to the route.
func TestP2pStreamProbePrefetchedHandlerPreservesOwnership(t *testing.T) {
	streamId := NewId()
	prefetchedProbe := encodeP2pStreamProbe(p2pStreamProbeRequestType, streamId, NewId())
	prefetchedApplication := MessagePoolCopy([]byte("prefetched application frame"))
	steadyApplication := []byte("steady legacy frame")
	fenceNonce := NewId()
	fenceTemplate := encodeP2pStreamProbe(p2pStreamProbeRequestType, streamId, fenceNonce)
	defer MessagePoolReturn(fenceTemplate)
	conn := newQueuedMessageConn(steadyApplication, fenceTemplate)
	defer conn.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stats := &P2pDataPlaneStats{}
	settings := DefaultP2pTransportSettings()
	settings.DataPlaneMode = P2pDataPlaneModeLegacyOnly
	settings.DataPlaneStats = stats
	var handledProbeCount atomic.Int64
	fenceHandled := make(chan struct{})
	var fenceHandledOnce sync.Once
	_, receiveRoute := newP2pReceiveTransport(
		ctx,
		cancel,
		conn,
		streamId,
		settings,
		[][]byte{prefetchedProbe, prefetchedApplication},
		func(message []byte) bool {
			if recognized, _, _, nonce := decodeP2pStreamProbe(message); recognized {
				handledProbeCount.Add(1)
				if nonce == fenceNonce {
					fenceHandledOnce.Do(func() {
						close(fenceHandled)
					})
				}
				return true
			}
			return false
		},
	)
	select {
	case message := <-receiveRoute:
		if !bytes.Equal(message, prefetchedApplication) ||
			&message[0] != &prefetchedApplication[0] {
			MessagePoolReturn(message)
			t.Fatal("prefetched application ownership was copied or changed")
		}
		MessagePoolReturn(message)
	case <-time.After(time.Second):
		t.Fatal("prefetched application data was not delivered")
	}
	select {
	case message := <-receiveRoute:
		if !bytes.Equal(message, steadyApplication) {
			MessagePoolReturn(message)
			t.Fatal("steady legacy payload changed")
		}
		MessagePoolReturn(message)
	case <-time.After(time.Second):
		t.Fatal("steady legacy application data was not delivered")
	}
	waitP2pProbeBarrier(t, fenceHandled, "prefetched receive stats fence was not handled")
	snapshot := stats.Snapshot()
	if handledProbeCount.Load() != 2 ||
		snapshot.LegacyReceiveMessageCount != 1 ||
		snapshot.LegacyReceiveByteCount != uint64(len(steadyApplication)) {
		t.Fatalf(
			"prefetched probe handler count=%d stats=%+v",
			handledProbeCount.Load(),
			snapshot,
		)
	}
}

// A loopback fast carrier lets one send and receive worker prove that probe
// messages are consumed but excluded from every application traffic counter.
type p2pProbeFastLoopConn struct {
	done         chan struct{}
	closeOnce    sync.Once
	fastMessages chan p2pFastPathReceivedMessage
}

// Allocates a bounded in-memory carrier.
func newP2pProbeFastLoopConn() *p2pProbeFastLoopConn {
	return &p2pProbeFastLoopConn{
		done:         make(chan struct{}),
		fastMessages: make(chan p2pFastPathReceivedMessage, 8),
	}
}

// The legacy lane stays blocked until teardown.
func (self *p2pProbeFastLoopConn) Read([]byte) (int, error) {
	<-self.done
	return 0, net.ErrClosed
}

// The forced-fast test must never use the legacy lane.
func (self *p2pProbeFastLoopConn) Write([]byte) (int, error) {
	return 0, net.ErrClosed
}

// Releases the blocking compatibility reader.
func (self *p2pProbeFastLoopConn) Close() error {
	self.closeOnce.Do(func() {
		close(self.done)
	})
	return nil
}

// Returns a stable synthetic address.
func (self *p2pProbeFastLoopConn) LocalAddr() net.Addr {
	return p2pProbeTestNetAddr("probe-fast-local")
}

// Returns a stable synthetic address.
func (self *p2pProbeFastLoopConn) RemoteAddr() net.Addr {
	return p2pProbeTestNetAddr("probe-fast-remote")
}

// Deadlines are irrelevant to the in-memory carrier.
func (self *p2pProbeFastLoopConn) SetDeadline(time.Time) error {
	return nil
}

// Deadlines are irrelevant to the in-memory carrier.
func (self *p2pProbeFastLoopConn) SetReadDeadline(time.Time) error {
	return nil
}

// Deadlines are irrelevant to the in-memory carrier.
func (self *p2pProbeFastLoopConn) SetWriteDeadline(time.Time) error {
	return nil
}

// The synthetic carrier is ready from construction.
func (self *p2pProbeFastLoopConn) FastPathReady() bool {
	return true
}

// The synthetic carrier is ready from construction.
func (self *p2pProbeFastLoopConn) WaitFastPathReady(context.Context, time.Duration) bool {
	return true
}

// Copies the borrowed send buffer into receiver-owned pooled storage.
func (self *p2pProbeFastLoopConn) WriteFastPathMessage(message []byte) (int, error) {
	select {
	case <-self.done:
		return 0, net.ErrClosed
	case self.fastMessages <- p2pFastPathReceivedMessage{
		message:       MessagePoolCopy(message),
		fragmentCount: 1,
	}:
		return 1, nil
	}
}

// Exposes complete reassembled messages to the production receive worker.
func (self *p2pProbeFastLoopConn) FastPathMessages() <-chan p2pFastPathReceivedMessage {
	return self.fastMessages
}

// A stable string implements net.Addr for the in-memory carrier.
type p2pProbeTestNetAddr string

// Reports the synthetic transport family.
func (self p2pProbeTestNetAddr) Network() string {
	return "probe-test"
}

// Reports the stable endpoint label.
func (self p2pProbeTestNetAddr) String() string {
	return string(self)
}

// Probe traffic crosses the fast handler but contributes zero messages,
// bytes, fragments, fallback, or drop counts.
func TestP2pStreamProbeFastHandlerExcludesDataPlaneStats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := newP2pProbeFastLoopConn()
	defer conn.Close()
	stats := &P2pDataPlaneStats{}
	settings := DefaultP2pTransportSettings()
	settings.DataPlaneMode = P2pDataPlaneModeFastOnly
	settings.DataPlaneStats = stats
	streamId := NewId()
	fenceNonce := NewId()
	fenceHandled := make(chan struct{})
	var fenceHandledOnce sync.Once
	_, sendRoute := NewP2pSendTransport(ctx, cancel, conn, streamId, settings)
	_, receiveRoute := newP2pReceiveTransport(
		ctx,
		cancel,
		conn,
		streamId,
		settings,
		nil,
		func(message []byte) bool {
			recognized, _, _, nonce := decodeP2pStreamProbe(message)
			if recognized && nonce == fenceNonce {
				fenceHandledOnce.Do(func() {
					close(fenceHandled)
				})
			}
			return recognized
		},
	)
	probeMessage := encodeP2pStreamProbe(p2pStreamProbeRequestType, streamId, NewId())
	fenceMessage := encodeP2pStreamProbe(p2pStreamProbeRequestType, streamId, fenceNonce)
	applicationMessage := []byte("counted application frame")
	sendRoute <- probeMessage
	sendRoute <- MessagePoolCopy(applicationMessage)
	sendRoute <- fenceMessage

	select {
	case message := <-receiveRoute:
		if !bytes.Equal(message, applicationMessage) {
			MessagePoolReturn(message)
			t.Fatal("fast handler delivered the probe instead of application data")
		}
		MessagePoolReturn(message)
	case <-time.After(time.Second):
		t.Fatal("fast handler did not deliver application data")
	}
	waitP2pProbeBarrier(t, fenceHandled, "fast receive stats fence was not handled")
	snapshot := stats.Snapshot()
	if snapshot.FastSendMessageCount != 1 || snapshot.FastReceiveMessageCount != 1 ||
		snapshot.FastSendByteCount != uint64(len(applicationMessage)) ||
		snapshot.FastReceiveByteCount != uint64(len(applicationMessage)) ||
		snapshot.FastSendFragmentCount != 1 || snapshot.FastReceiveFragmentCount != 1 ||
		snapshot.LegacySendMessageCount != 0 || snapshot.LegacyReceiveMessageCount != 0 ||
		snapshot.FastFallbackCount != 0 || snapshot.FastDropCount != 0 {
		t.Fatalf("probe changed data-plane stats: %+v", snapshot)
	}
}
