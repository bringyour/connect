package connect

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type blockingP2pRouteManager struct {
	updateStarted chan struct{}
	releaseUpdate chan struct{}
	startOnce     sync.Once
	active        atomic.Bool
	removeCount   atomic.Int32
}

func (self *blockingP2pRouteManager) UpdateTransport(Transport, []Route) {
	self.startOnce.Do(func() { close(self.updateStarted) })
	<-self.releaseUpdate
	self.active.Store(true)
}

func (self *blockingP2pRouteManager) UpdateTransportWithProperties(
	transport Transport,
	routes []Route,
	_ TransferCarrierProperties,
) {
	self.UpdateTransport(transport, routes)
}

func (self *blockingP2pRouteManager) RemoveTransport(Transport) {
	self.active.Store(false)
	self.removeCount.Add(1)
}

type p2pRouteTestTransport struct {
	id Id
}

func (self *p2pRouteTestTransport) TransportId() Id { return self.id }
func (self *p2pRouteTestTransport) Priority() int   { return TransportMaxPriority }
func (self *p2pRouteTestTransport) Weight() float32 { return 0 }
func (self *p2pRouteTestTransport) CanEvalRouteWeight(*RouteStats, map[Transport]*RouteStats) bool {
	return true
}
func (self *p2pRouteTestTransport) RouteWeight(*RouteStats, map[Transport]*RouteStats) float32 {
	return 1
}
func (self *p2pRouteTestTransport) MatchesSend(TransferPath) bool    { return true }
func (self *p2pRouteTestTransport) MatchesReceive(TransferPath) bool { return true }
func (self *p2pRouteTestTransport) Downgrade(TransferPath)           {}

type recordingP2pRouteManager struct {
	properties      TransferCarrierProperties
	propertyUpdates []TransferCarrierProperties
	active          bool
}

func (self *recordingP2pRouteManager) UpdateTransport(Transport, []Route) {
	self.properties = TransferCarrierProperties{}
	self.active = true
}

func (self *recordingP2pRouteManager) UpdateTransportWithProperties(
	_ Transport,
	_ []Route,
	properties TransferCarrierProperties,
) {
	self.properties = properties
	self.propertyUpdates = append(self.propertyUpdates, properties)
	self.active = true
}

func (self *recordingP2pRouteManager) RemoveTransport(Transport) {
	self.active = false
}

type p2pRouteTestFastConn struct {
	net.Conn
	ready atomic.Bool
}

func (self *p2pRouteTestFastConn) FastPathReady() bool {
	return self.ready.Load()
}

func (self *p2pRouteTestFastConn) WaitFastPathReady(context.Context, time.Duration) bool {
	return self.FastPathReady()
}

func (self *p2pRouteTestFastConn) WriteFastPathMessage([]byte) (int, error) {
	return 1, nil
}

func (self *p2pRouteTestFastConn) FastPathMessages() <-chan p2pFastPathReceivedMessage {
	return nil
}

func TestP2pReceiveConstructorPublishesExactPhysicalLanes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	local, remote := net.Pipe()
	defer remote.Close()
	transport, lanes := newP2pReceiveTransportWithLanes(
		ctx,
		cancel,
		local,
		NewId(),
		DefaultP2pTransportSettings(),
		nil,
		nil,
	)
	if len(lanes) != 2 {
		t.Fatalf("P2P receive lanes=%d want=2", len(lanes))
	}
	if lanes[0].reliability != CarrierReliabilityReliable ||
		lanes[1].reliability != CarrierReliabilityUnreliable {
		t.Fatalf("P2P receive lane contracts=%v/%v", lanes[0].reliability, lanes[1].reliability)
	}
	if lanes[0].transport.TransportId() == lanes[1].transport.TransportId() ||
		lanes[0].route == lanes[1].route {
		t.Fatal("P2P physical lanes share route identity")
	}
	manager := &recordingP2pRouteManager{}
	if !updateP2pConnectionReceiveRoutes(ctx, manager, lanes, true) {
		t.Fatal("active P2P receive lanes were not published")
	}
	if len(manager.propertyUpdates) != 2 ||
		manager.propertyUpdates[0].ReceiveReliability != CarrierReliabilityReliable ||
		manager.propertyUpdates[1].ReceiveReliability != CarrierReliabilityUnreliable {
		t.Fatalf("published P2P lane properties=%+v", manager.propertyUpdates)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err := transport.CloseAndWait(closeCtx); err != nil {
		t.Fatal(err)
	}
}

// Auto applies bounded unreliable flight only while the native fast path is
// actually eligible. Its reliable SCTP fallback must keep normal stream flight.
func TestP2pConnectionRoutePublishesFastCarrierProperties(t *testing.T) {
	defaultSettings := DefaultP2pTransportSettings()
	if limit := p2pUnreliableFlightMessageLimit(defaultSettings); limit != 255 {
		t.Fatalf("default P2P route flight limit = %d, want queue-derived 255", limit)
	}
	if limit := p2pUnreliableFlightByteLimit(defaultSettings); limit != kib(240) {
		t.Fatalf("default P2P route byte flight limit = %d, want 240 KiB", limit)
	}
	local, remote := net.Pipe()
	defer local.Close()
	defer remote.Close()
	fastConn := &p2pRouteTestFastConn{Conn: local}
	transport := &P2pSendTransport{
		conn: fastConn,
		settings: &P2pTransportSettings{
			DataPlaneMode:            P2pDataPlaneModeAuto,
			ChannelBufferSize:        4,
			ReceiveQueueMessageCount: 16,
			ReceiveQueueByteCount:    kib(256),
			MaxMessageByteCount:      64 * 1024,
		},
	}
	manager := &recordingP2pRouteManager{}
	if !updateP2pConnectionRoute(
		context.Background(),
		manager,
		transport,
		make(Route),
		true,
	) {
		t.Fatal("active P2P route was not installed")
	}
	if !manager.active || !manager.properties.Unreliable {
		t.Fatalf("Auto fast route properties = %+v, want potentially unreliable", manager.properties)
	}
	if manager.properties.unreliableFlightMessageLimit != 15 {
		t.Fatalf(
			"Auto fast route flight limit = %d, want fifteen data slots plus one control reserve",
			manager.properties.unreliableFlightMessageLimit,
		)
	}
	if manager.properties.unreliableFlightByteLimit != kib(240) {
		t.Fatalf(
			"Auto fast route byte flight limit = %d, want 16 KiB receive reserve",
			manager.properties.unreliableFlightByteLimit,
		)
	}
	if manager.properties.messageUnreliable([]byte("legacy fallback")) {
		t.Fatal("Auto classified its reliable SCTP fallback as unreliable")
	}
	fastConn.ready.Store(true)
	if !manager.properties.messageUnreliable([]byte("native fast")) {
		t.Fatal("Auto did not classify its ready native fast write as unreliable")
	}

	transport.settings.DataPlaneMode = P2pDataPlaneModeLegacyOnly
	if properties := p2pTransferCarrierProperties(transport); properties.Unreliable ||
		properties.messageUnreliable([]byte("legacy only")) ||
		properties.unreliableFlightByteLimit != 0 ||
		properties.unreliableFlightMessageLimit != 0 {
		t.Fatalf("legacy-only P2P route properties = %+v, want reliable", properties)
	}
	transport.settings.DataPlaneMode = P2pDataPlaneModeFastOnly
	if properties := p2pTransferCarrierProperties(transport); !properties.Unreliable ||
		!properties.messageUnreliable([]byte("forced native fast")) {
		t.Fatalf("fast-only P2P route properties = %+v, want unreliable", properties)
	}
	transport.settings.ChannelBufferSize = 1
	transport.settings.ReceiveQueueMessageCount = 1
	if limit := p2pTransferCarrierProperties(transport).unreliableFlightMessageLimit; limit != 1 {
		t.Fatalf("one-slot P2P route flight limit = %d, want progress floor 1", limit)
	}
}

// Concurrent small data/ACK/control/probe sequences use independent count
// headroom while large frames remain constrained by the same 256 KiB payload
// ceiling as the former four-entry 64 KiB route channel.
func TestP2pReceiveQueueSeparatesMessageAndByteBounds(t *testing.T) {
	settings := DefaultP2pTransportSettings()
	settings.DataPlaneStats = &P2pDataPlaneStats{}
	receiver := &P2pReceiveTransport{
		ctx:                        context.Background(),
		pendingReceive:             make(chan []byte, p2pReceiveQueueMessageCount(settings)),
		pendingReceiveMessageLimit: int64(p2pReceiveQueueMessageCount(settings)),
		pendingReceiveByteLimit:    int64(p2pReceiveQueueByteCount(settings)),
		settings:                   settings,
	}
	offer := func(byteCount int) {
		receiver.offerReceive(MessagePoolGet(byteCount), true, 1, false, true)
	}
	drain := func() {
		for 0 < len(receiver.pendingReceive) {
			message := <-receiver.pendingReceive
			receiver.releasePendingReceive(len(message))
			MessagePoolReturn(message)
		}
		if got := receiver.pendingReceiveMessageCount.Load(); got != 0 {
			t.Fatalf("drained receive queue retains %d messages", got)
		}
		if got := receiver.pendingReceiveByteCount.Load(); got != 0 {
			t.Fatalf("drained receive queue retains %d bytes", got)
		}
	}

	for range settings.ReceiveQueueMessageCount {
		offer(154)
	}
	if got := len(receiver.pendingReceive); got != settings.ReceiveQueueMessageCount {
		t.Fatalf("small-message queue count = %d, want %d", got, settings.ReceiveQueueMessageCount)
	}
	if got := receiver.pendingReceiveMessageCount.Load(); got != int64(settings.ReceiveQueueMessageCount) {
		t.Fatalf("small-message retained count = %d, want %d", got, settings.ReceiveQueueMessageCount)
	}
	if got := receiver.pendingReceiveByteCount.Load(); got != int64(154*settings.ReceiveQueueMessageCount) {
		t.Fatalf("small-message queue bytes = %d", got)
	}
	offer(154)
	if drops := settings.DataPlaneStats.Snapshot().FastReceiveQueueDropCount; drops != 1 {
		t.Fatalf("small-message overflow drops = %d, want 1", drops)
	}
	drain()

	for range 5 {
		offer(settings.MaxMessageByteCount)
	}
	if got := len(receiver.pendingReceive); got != 4 {
		t.Fatalf("large-message queue count = %d, want memory-bounded 4", got)
	}
	if got := receiver.pendingReceiveMessageCount.Load(); got != 4 {
		t.Fatalf("large-message retained count = %d, want memory-bounded 4", got)
	}
	if got := receiver.pendingReceiveByteCount.Load(); got != int64(settings.ReceiveQueueByteCount) {
		t.Fatalf(
			"large-message queue bytes = %d, want hard bound %d",
			got,
			settings.ReceiveQueueByteCount,
		)
	}
	if drops := settings.DataPlaneStats.Snapshot().FastReceiveQueueDropCount; drops != 2 {
		t.Fatalf("combined receive queue drops = %d, want 2", drops)
	}
	drain()

	// A message already removed by the forwarding worker remains charged until
	// the RouteManager accepts it. It must consume one of the same sixteen
	// credits instead of becoming an implicit seventeenth retained message.
	forwardingMessage := MessagePoolGet(154)
	forwardingReserved := receiver.reservePendingReceive(len(forwardingMessage))
	defer func() {
		for 0 < len(receiver.pendingReceive) {
			message := <-receiver.pendingReceive
			receiver.releasePendingReceive(len(message))
			MessagePoolReturn(message)
		}
		if forwardingMessage != nil {
			if forwardingReserved {
				receiver.releasePendingReceive(len(forwardingMessage))
			}
			MessagePoolReturn(forwardingMessage)
		}
	}()
	if !forwardingReserved {
		t.Fatal("could not reserve forwarding-worker message")
	}
	for range settings.ReceiveQueueMessageCount - 1 {
		offer(154)
	}
	if got := len(receiver.pendingReceive); got != settings.ReceiveQueueMessageCount-1 {
		t.Fatalf("worker-held queue count = %d, want %d", got, settings.ReceiveQueueMessageCount-1)
	}
	if got := receiver.pendingReceiveMessageCount.Load(); got != int64(settings.ReceiveQueueMessageCount) {
		t.Fatalf("worker-held retained count = %d, want hard bound %d", got, settings.ReceiveQueueMessageCount)
	}
	offer(154)
	if drops := settings.DataPlaneStats.Snapshot().FastReceiveQueueDropCount; drops != 3 {
		t.Fatalf("worker-held overflow drops = %d, want 3 combined", drops)
	}
	for 0 < len(receiver.pendingReceive) {
		message := <-receiver.pendingReceive
		receiver.releasePendingReceive(len(message))
		MessagePoolReturn(message)
	}
	if got := receiver.pendingReceiveMessageCount.Load(); got != 1 {
		t.Fatalf("forwarding worker reservation count = %d, want 1", got)
	}
	receiver.releasePendingReceive(len(forwardingMessage))
	forwardingReserved = false
	MessagePoolReturn(forwardingMessage)
	forwardingMessage = nil
	if got := receiver.pendingReceiveMessageCount.Load(); got != 0 {
		t.Fatalf("released worker reservation retains %d messages", got)
	}
	if got := receiver.pendingReceiveByteCount.Load(); got != 0 {
		t.Fatalf("released worker reservation retains %d bytes", got)
	}
}

func TestP2pLateConnectedCallbackCannotRestoreCanceledRoute(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	manager := &blockingP2pRouteManager{
		updateStarted: make(chan struct{}),
		releaseUpdate: make(chan struct{}),
	}
	transport := &p2pRouteTestTransport{id: NewId()}
	done := make(chan struct{})
	var installed atomic.Bool
	go func() {
		installed.Store(updateP2pConnectionRoute(ctx, manager, transport, make(Route), true))
		close(done)
	}()

	select {
	case <-manager.updateStarted:
	case <-time.After(time.Second):
		t.Fatal("connected callback did not enter route update")
	}
	// Teardown wins while the callback is parked inside the route manager.
	// When the old update finally returns, its post-update check must remove
	// the route again rather than resurrecting the retired connection.
	cancel()
	close(manager.releaseUpdate)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("late connected callback did not finish")
	}
	if manager.active.Load() {
		t.Fatal("late connected callback restored a route after cancellation")
	}
	if manager.removeCount.Load() != 1 {
		t.Fatalf("compensating route removals=%d, want 1", manager.removeCount.Load())
	}
	if installed.Load() {
		t.Fatal("late connected callback reported a canceled route as installed")
	}
}

// Duplicate connection edges leave each active route gauge exact and return
// it to zero after removal.
func TestP2pActiveRouteStatsTrackIdempotentTransitions(t *testing.T) {
	stats := &P2pDataPlaneStats{}
	states := []P2pRouteState{}
	transport := &P2pTransport{
		peerId:   NewId(),
		streamId: NewId(),
		settings: &P2pTransportSettings{
			DataPlaneStats: stats,
			RouteStateObserver: func(state P2pRouteState) {
				states = append(states, state)
			},
		},
	}
	var sendConnected atomic.Bool
	var receiveConnected atomic.Bool
	transport.observeRouteState(&sendConnected, true, true)
	transport.observeRouteState(&sendConnected, true, true)
	transport.observeRouteState(&receiveConnected, false, true)
	transport.observeRouteState(&receiveConnected, false, true)
	snapshot := stats.Snapshot()
	if snapshot.ActiveSendRouteCount != 1 || snapshot.ActiveReceiveRouteCount != 1 {
		t.Fatalf("active route counts after connect=%+v, want send=1 receive=1", snapshot)
	}
	transport.observeRouteState(&sendConnected, true, false)
	transport.observeRouteState(&sendConnected, true, false)
	transport.observeRouteState(&receiveConnected, false, false)
	transport.observeRouteState(&receiveConnected, false, false)
	snapshot = stats.Snapshot()
	if snapshot.ActiveSendRouteCount != 0 || snapshot.ActiveReceiveRouteCount != 0 {
		t.Fatalf("active route counts after disconnect=%+v, want send=0 receive=0", snapshot)
	}
	if len(states) != 4 {
		t.Fatalf("route state observations=%d, want four real edges: %+v", len(states), states)
	}
	if states[0].PeerId != transport.peerId || states[0].StreamId != transport.streamId ||
		!states[0].Send || !states[0].Connected ||
		states[1].Send || !states[1].Connected ||
		!states[2].Send || states[2].Connected ||
		states[3].Send || states[3].Connected {
		t.Fatalf("route state observations=%+v", states)
	}
}
