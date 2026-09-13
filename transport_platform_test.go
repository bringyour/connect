package connect

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	quic "github.com/quic-go/quic-go"
)

// closeInterruptWriteConn models the state observed on iOS after a path had
// gone stale: the WebSocket's next TCP write entered the kernel and remained
// blocked until the connection itself was closed. Context cancellation alone
// cannot interrupt net.Conn.Write.
type closeInterruptWriteConn struct {
	net.Conn

	blockWrite atomic.Bool
	writeOnce  sync.Once
	closeOnce  sync.Once

	writeStarted chan struct{}
	closed       chan struct{}
}

func newCloseInterruptWriteConn(conn net.Conn) *closeInterruptWriteConn {
	return &closeInterruptWriteConn{
		Conn:         conn,
		writeStarted: make(chan struct{}),
		closed:       make(chan struct{}),
	}
}

func (self *closeInterruptWriteConn) Write(buffer []byte) (int, error) {
	if !self.blockWrite.Load() {
		return self.Conn.Write(buffer)
	}
	self.writeOnce.Do(func() {
		close(self.writeStarted)
	})
	<-self.closed
	return 0, net.ErrClosed
}

func (self *closeInterruptWriteConn) Close() error {
	var err error
	self.closeOnce.Do(func() {
		close(self.closed)
		err = self.Conn.Close()
	})
	return err
}

// The platform transport had no test coverage at all: nothing constructed a
// PlatformTransport, so runH1, the mode election loop and the inactive-drain
// watchdog were never exercised. That is how a Monitor shipped with zero
// notifiers — the mode election never ran once in the product's life, and the
// live transport armed a 30s self-kill timer it could never cancel, surviving
// only because server pings happened to arrive faster than the timeout.
//
// These tests drive the real runH1 against a real websocket server. The default
// settings use V2H1Auth, which authenticates with request headers and performs no
// in-band handshake, so an ordinary websocket upgrade is all the platform side
// needs to provide.

// testingPlatformServer is a websocket server standing in for the platform. It
// accepts connections and then stays silent: it never sends a frame. Silence is
// deliberate — an idle inbound connection is exactly the condition the
// inactive-drain watchdog reacts to, and in production it is inbound server pings
// that were accidentally keeping the watchdog at bay.
type testingPlatformServer struct {
	server *httptest.Server
	url    string

	connectCount  atomic.Int64
	emptyMessages atomic.Int64
	dataMessages  atomic.Int64
	rejecting     atomic.Bool

	stateLock sync.Mutex
	conns     []*websocket.Conn
}

func newTestingPlatformServer(t *testing.T) *testingPlatformServer {
	t.Helper()
	return newTestingPlatformServerIpVersion(t, 4)
}

// newTestingPlatformServerIpVersion binds the platform to the loopback of the
// family under test, so the transport dials it over that family.
func newTestingPlatformServerIpVersion(t *testing.T, ipVersion int) *testingPlatformServer {
	t.Helper()
	platform := &testingPlatformServer{}
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	platform.server = newTestingLoopbackHttpServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if platform.rejecting.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		platform.connectCount.Add(1)
		func() {
			platform.stateLock.Lock()
			defer platform.stateLock.Unlock()
			platform.conns = append(platform.conns, ws)
		}()
		// hold the connection open, discarding whatever the client sends
		// (including its keepalive pings, which the client writes directly and
		// never counts). send nothing back
		for {
			_, message, err := ws.ReadMessage()
			if err != nil {
				ws.Close()
				return
			}
			if len(message) == 0 {
				platform.emptyMessages.Add(1)
			} else {
				platform.dataMessages.Add(1)
			}
		}
	}), false)
	platform.url = "ws" + strings.TrimPrefix(platform.server.URL, "http")
	t.Cleanup(func() {
		// close the live connections first so the handlers return; httptest
		// Close blocks until every outstanding request completes
		platform.closeConns()
		platform.server.Close()
	})
	return platform
}

func (self *testingPlatformServer) closeConns() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	for _, ws := range self.conns {
		ws.Close()
	}
	self.conns = nil
}

func (self *testingPlatformServer) sendBinary(message []byte) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	for _, ws := range self.conns {
		_ = ws.WriteMessage(websocket.BinaryMessage, message)
	}
}

// down stops the platform from accepting, and drops the live connections, so the
// transport disconnects and cannot reconnect.
func (self *testingPlatformServer) down() {
	self.rejecting.Store(true)
	self.closeConns()
}

func testingPlatformTransportSettings() *PlatformTransportSettings {
	settings := DefaultPlatformTransportSettings()
	// short enough to exercise the inactive-drain watchdog inside a test, long
	// enough that the election (which is immediate) always precedes it
	settings.InactiveDrainTimeout = 500 * time.Millisecond
	settings.ReconnectTimeout = 50 * time.Millisecond
	// keep the client's own keepalive out of the way; it is written directly to
	// the socket and is not counted as activity either way
	settings.PingTimeout = 30 * time.Second
	return settings
}

func testingPlatformTransport(
	t *testing.T,
	ctx context.Context,
	platformUrl string,
	settings *PlatformTransportSettings,
) *PlatformTransport {
	t.Helper()
	transport := NewPlatformTransportWithTargetMode(
		ctx,
		NewClientStrategyWithDefaults(ctx),
		NewRouteManager(ctx, "test"),
		platformUrl,
		&ClientAuth{
			// the platform side does not verify; ClientId() failing to parse is
			// tolerated by runH1 (it only names log lines)
			ByJwt:      "testing",
			InstanceId: NewId(),
			AppVersion: "testing",
		},
		TransportModeH1,
		settings,
	)
	t.Cleanup(transport.Close)
	return transport
}

func testingWaitForActiveMode(transport *PlatformTransport, want TransportMode, timeout time.Duration) bool {
	return waitForCondition(timeout, func() bool {
		mode, _ := transport.activeMode()
		return mode == want
	})
}

// TestPlatformTransportConnectsAndElects drives the real transport against a real
// websocket platform: it must connect and the election must make the connected
// mode active.
//
// This is the regression test for the mode monitor having no producer.
// setModeAvailable mutated availableModes and notified nobody, so the election
// loop — which subscribes before it reads — parked on its very first iteration
// and the active mode stayed TransportModeNone for the entire process lifetime.
func TestPlatformTransportConnectsAndElects(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingPlatformServerIpVersion(t, ipVersion)
		transport := testingPlatformTransport(t, ctx, platform.url, testingPlatformTransportSettings())

		if !waitForCondition(15*time.Second, func() bool {
			return 0 < platform.connectCount.Load()
		}) {
			t.Fatal("the transport never connected to the platform")
		}

		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			mode, _ := transport.activeMode()
			t.Fatalf("active mode = %q, want h1: the connected transport was never elected", mode)
		}
	})
}

func TestPlatformWebSocketAckAndPacketBurstsCoalesce(t *testing.T) {
	prioritySend := make(chan []byte, platformWebSocketWriteBatchMaxMessages)
	ordinarySend := make(chan []byte, 3)
	for index := range cap(prioritySend) {
		prioritySend <- []byte{byte(index)}
	}
	for index := range cap(ordinarySend) {
		ordinarySend <- []byte{byte(0x80 + index)}
	}

	// The first ACK has already started this physical batch. Prove that two
	// complete subsequent ACK bursts and their ordinary turns remain in it:
	// 7 ACKs, packet, 8 ACKs, packet, 8 ACKs, packet. (The first ACK makes the
	// initial burst eight.)
	priorityMessageCount := 1
	var selection []bool
	for len(selection) < 7+1+8+1+8+1 {
		message, priority, sendOpen, ready :=
			platformWebSocketWriteBatchNextReady(
				prioritySend,
				ordinarySend,
				priorityMessageCount,
			)
		if !ready || !sendOpen || len(message) != 1 {
			t.Fatalf(
				"selection %d = (%v, %t, %t, %t)",
				len(selection),
				message,
				priority,
				sendOpen,
				ready,
			)
		}
		selection = append(selection, priority)
		if priority {
			priorityMessageCount++
		} else {
			priorityMessageCount = 0
		}
	}
	wantPriority := func(index int) bool {
		return index < 7 ||
			(8 <= index && index < 16) ||
			(17 <= index && index < 25)
	}
	for index, priority := range selection {
		if priority != wantPriority(index) {
			t.Fatalf("selection %d priority = %t, want %t; sequence = %v", index, priority, wantPriority(index), selection)
		}
	}
	if len(prioritySend) == 0 {
		t.Fatal("bounded ACK bursts consumed the entire continuously ready lane")
	}

	// With ordinary traffic absent, the ready ACK lane continues draining even
	// after its quantum instead of turning the quantum into a flush boundary.
	message, priority, sendOpen, ready :=
		platformWebSocketWriteBatchNextReady(
			prioritySend,
			ordinarySend,
			platformWebSocketAckPriorityBurstMaxMessages,
		)
	if len(message) != 1 || !priority || !sendOpen || !ready {
		t.Fatalf("priority-only ready drain = (%v, %t, %t, %t)", message, priority, sendOpen, ready)
	}

	// The inverse is also a burst: with no ACK ready, ordinary packets drain
	// consecutively into the same bulk write.
	packetBurst := make(chan []byte, 3)
	for index := range cap(packetBurst) {
		packetBurst <- []byte{byte(index)}
	}
	for index := range cap(packetBurst) {
		message, priority, sendOpen, ready =
			platformWebSocketWriteBatchNextReady(nil, packetBurst, 0)
		if len(message) != 1 || priority || !sendOpen || !ready {
			t.Fatalf("ordinary burst slot %d = (%v, %t, %t, %t)", index, message, priority, sendOpen, ready)
		}
	}
}

func BenchmarkPlatformWebSocketAckPacketReadyDrain(b *testing.B) {
	prioritySend := make(chan []byte, platformWebSocketWriteBatchMaxMessages)
	ordinarySend := make(chan []byte, platformWebSocketWriteBatchMaxMessages)
	priorityMessage := []byte{1}
	ordinaryMessage := []byte{2}

	b.ReportAllocs()
	b.ReportMetric(platformWebSocketWriteBatchMaxMessages, "frames/batch")
	for b.Loop() {
		// Together with the already-selected first ACK, this fills one exact
		// production batch with three ordinary turns and repeated ACK bursts.
		for range platformWebSocketWriteBatchMaxMessages - 4 {
			prioritySend <- priorityMessage
		}
		for range 3 {
			ordinarySend <- ordinaryMessage
		}
		priorityMessageCount := 1
		for range platformWebSocketWriteBatchMaxMessages - 1 {
			_, priority, sendOpen, ready :=
				platformWebSocketWriteBatchNextReady(
					prioritySend,
					ordinarySend,
					priorityMessageCount,
				)
			if !ready || !sendOpen {
				b.Fatal("ready mixed batch ended early")
			}
			if priority {
				priorityMessageCount++
			} else {
				priorityMessageCount = 0
			}
		}
	}
}

func BenchmarkPlatformWebSocketOrdinaryReadyDrain(b *testing.B) {
	ordinarySend := make(chan []byte, platformWebSocketWriteBatchMaxMessages)
	ordinaryMessage := []byte{2}

	b.ReportAllocs()
	b.ReportMetric(platformWebSocketWriteBatchMaxMessages, "frames/batch")
	for b.Loop() {
		for range platformWebSocketWriteBatchMaxMessages - 1 {
			ordinarySend <- ordinaryMessage
		}
		for range platformWebSocketWriteBatchMaxMessages - 1 {
			_, priority, sendOpen, ready :=
				platformWebSocketWriteBatchNextReady(nil, ordinarySend, 0)
			if !ready || !sendOpen || priority {
				b.Fatal("ready ordinary batch ended early")
			}
		}
	}
}

func TestPlatformTransportOptionalH1AckPriorityLaneWritesAndUnregisters(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingPlatformServerIpVersion(t, ipVersion)
		settings := testingPlatformTransportSettings()
		settings.H1AckPriorityBufferSize = 8
		connectedRoute := make(chan Route, 1)
		settings.SendRouteObserver = func(_ Transport, route Route, connected bool) {
			if connected {
				select {
				case connectedRoute <- route:
				default:
				}
			}
		}
		transport := testingPlatformTransport(t, ctx, platform.url, settings)
		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			t.Fatal("the H1 transport was never elected")
		}

		var route Route
		select {
		case route = <-connectedRoute:
		case <-time.After(time.Second):
			t.Fatal("H1 send route was not observed")
		}
		priorityRoute, ok := h1AckPriorityRoute(route)
		if !ok || cap(priorityRoute) != settings.H1AckPriorityBufferSize {
			t.Fatalf("priority route = (%v, %t), want capacity %d", priorityRoute, ok, settings.H1AckPriorityBufferSize)
		}
		message := MessagePoolGet(64)
		priorityRoute <- message
		if !waitForCondition(time.Second, func() bool {
			return 0 < platform.dataMessages.Load()
		}) {
			t.Fatal("H1 writer did not drain the ACK priority lane")
		}

		closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
		defer closeCancel()
		if err := transport.CloseAndWait(closeCtx); err != nil {
			t.Fatalf("close H1 priority transport: %v", err)
		}
		if _, ok := h1AckPriorityRoute(route); ok {
			t.Fatal("H1 ACK priority route remained registered after transport close")
		}
	})
}

func TestPlatformTransportDefaultDoesNotRegisterH1AckPriorityLane(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingPlatformServerIpVersion(t, ipVersion)
		settings := testingPlatformTransportSettings()
		connectedRoute := make(chan Route, 1)
		settings.SendRouteObserver = func(_ Transport, route Route, connected bool) {
			if connected {
				select {
				case connectedRoute <- route:
				default:
				}
			}
		}
		transport := testingPlatformTransport(t, ctx, platform.url, settings)
		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			t.Fatal("the H1 transport was never elected")
		}
		select {
		case route := <-connectedRoute:
			if _, ok := h1AckPriorityRoute(route); ok {
				t.Fatal("default/server transport unexpectedly registered an ACK priority lane")
			}
		case <-time.After(time.Second):
			t.Fatal("H1 send route was not observed")
		}
	})
}

func TestPlatformTransportH1RejectsOversizedWebSocketMessage(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingPlatformServerIpVersion(t, ipVersion)
		settings := testingPlatformTransportSettings()
		settings.H1MaxMessageByteCount = 64
		transport := testingPlatformTransport(t, ctx, platform.url, settings)
		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			t.Fatal("the H1 transport was never elected")
		}
		connectCount := platform.connectCount.Load()
		platform.sendBinary(make([]byte, settings.H1MaxMessageByteCount+1))
		if !waitForCondition(15*time.Second, func() bool {
			return connectCount < platform.connectCount.Load()
		}) {
			t.Fatal("oversized WebSocket message did not close and replace the H1 connection")
		}
	})
}

func TestPlatformTransportInactiveDrainIgnoresH1ControlTraffic(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingPlatformServerIpVersion(t, ipVersion)
		settings := testingPlatformTransportSettings()
		settings.ModePreferences = map[TransportMode]int{
			TransportModeH3: 1,
			TransportModeH1: 2,
		}
		settings.InactiveDrainTimeout = 80 * time.Millisecond
		settings.InactiveDrainMaxTimeout = 2 * time.Second
		settings.PingTimeout = 10 * time.Millisecond
		transport := testingPlatformTransport(t, ctx, platform.url, settings)
		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			t.Fatal("the H1 transport was never elected")
		}
		connectCount := platform.connectCount.Load()

		// Empty inbound messages and the transport's frequent outbound keepalives
		// are control traffic. Neither may extend the payload-only quiet drain once
		// a strictly better mode supersedes H1.
		stopPings := make(chan struct{})
		pingsDone := make(chan struct{})
		defer func() {
			close(stopPings)
			<-pingsDone
		}()
		go func() {
			defer close(pingsDone)
			ticker := time.NewTicker(5 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-stopPings:
					return
				case <-ticker.C:
					platform.sendBinary(nil)
				}
			}
		}()
		transport.setActiveMode(TransportModeH3)
		if !waitForCondition(750*time.Millisecond, func() bool {
			return connectCount < platform.connectCount.Load()
		}) {
			t.Fatal("H1 control traffic prevented the superseded carrier from draining")
		}
	})
}

// TestPlatformTransportActiveModeIsNotDrained pins the watchdog's semantics: the
// transport that IS the active mode must never be drained.
//
// The watchdog tears down a transport that is NOT the active mode after
// InactiveDrainTimeout of no traffic. Because the elected mode was never
// published, the live transport read TransportModeNone forever, concluded it was
// inactive, and armed that kill timer against itself, with no way to cancel it.
//
// Note on real-world impact, so this test is not read as more than it is: under
// the default settings the mis-arming was REDUNDANT, not fatal. InactiveDrainTimeout
// and ReadTimeout are both 30s, and the drain's condition (no inbound AND no
// counted outbound) is a strict subset of the read deadline's (no inbound), which
// the reader resets before every read. So the read deadline always tore down an
// idle connection first, and h1's connection lifecycle was unchanged in practice.
// What the bug really broke is the watchdog's ability to tell active from
// inactive at all — so it could never shed a transport superseded by a better
// mode, which is its actual purpose and which matters the moment h3 is re-enabled.
//
// This test shortens InactiveDrainTimeout well below ReadTimeout precisely to
// isolate the watchdog from the read deadline, so it observes the drain decision
// on its own rather than whichever timer happens to fire first.
func TestPlatformTransportActiveModeIsNotDrained(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingPlatformServerIpVersion(t, ipVersion)
		settings := testingPlatformTransportSettings()
		transport := testingPlatformTransport(t, ctx, platform.url, settings)

		// wait for the connection, deliberately NOT for the election: the drain is a
		// property of the live socket, so this measures the production symptom (a
		// transport tearing its own connection down while idle) rather than depending
		// on how the elected mode is published
		if !waitForCondition(15*time.Second, func() bool {
			return 0 < platform.connectCount.Load()
		}) {
			t.Fatal("the transport never connected to the platform")
		}
		connectCount := platform.connectCount.Load()

		// idle well past the drain timeout. a transport that believes it is inactive
		// cancels its own connection here, and reconnects, over and over
		select {
		case <-time.After(5 * settings.InactiveDrainTimeout):
		}

		if reconnects := platform.connectCount.Load() - connectCount; 0 < reconnects {
			t.Fatalf(
				"the transport reconnected %d times while idle: it drained itself, believing it was not the active mode",
				reconnects,
			)
		}
		// it stayed up because it knows it is the active transport, so the watchdog
		// sits on its benign branch instead of arming the kill timer
		if mode, _ := transport.activeMode(); mode != TransportModeH1 {
			t.Fatalf("active mode = %q after idling, want h1", mode)
		}
	})
}

// TestPlatformTransportSendsRepeatedIdleKeepalives verifies both the initial
// reusable writer timer and its reset after firing.
func TestPlatformTransportSendsRepeatedIdleKeepalives(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingPlatformServerIpVersion(t, ipVersion)
		settings := testingPlatformTransportSettings()
		settings.PingTimeout = 20 * time.Millisecond
		transport := testingPlatformTransport(t, ctx, platform.url, settings)

		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			t.Fatal("the transport was never elected")
		}
		if !waitForCondition(2*time.Second, func() bool {
			return 2 <= platform.emptyMessages.Load()
		}) {
			t.Fatalf("idle keepalive count = %d, want at least 2", platform.emptyMessages.Load())
		}
	})
}

// TestPlatformTransportModeFallsBackOnDisconnect: when the last available mode
// drops, the active mode must return to TransportModeNone.
//
// The election only ever called setActiveMode from inside "some mode is
// available", and its fallback lived in an else on `0 < len(orderedModes)` —
// unreachable, because orderedModes is the key set of a constant map. So a
// disconnected transport left the active mode pinned to its stale value.
func TestPlatformTransportModeFallsBackOnDisconnect(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingPlatformServerIpVersion(t, ipVersion)
		transport := testingPlatformTransport(t, ctx, platform.url, testingPlatformTransportSettings())

		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			t.Fatal("the transport was never elected")
		}

		// the platform goes away: the transport disconnects and cannot reconnect
		platform.down()

		if !testingWaitForActiveMode(transport, TransportModeNone, 15*time.Second) {
			mode, _ := transport.activeMode()
			t.Fatalf("active mode = %q after the transport disconnected, want none", mode)
		}
	})
}

// TestNetworkChangeKicksPlatformTransport pins the network-change path: a
// NetworkChanged broadcast closes the live connection and the transport
// re-dials immediately (the host's path-update signal, not a server drop).
// Adapted from upstream main e05ecee's TestPlatformTransportNetworkChangeKick.
func TestNetworkChangeKicksPlatformTransport(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingPlatformServerIpVersion(t, ipVersion)
		transport := testingPlatformTransport(t, ctx, platform.url, testingPlatformTransportSettings())

		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			t.Fatal("the transport was never elected")
		}
		connectCount := platform.connectCount.Load()

		// the host reports a network path change
		NetworkChanged()

		if !waitForCondition(15*time.Second, func() bool {
			return connectCount < platform.connectCount.Load()
		}) {
			t.Fatal("the transport did not re-dial after a network change")
		}
		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			mode, _ := transport.activeMode()
			t.Fatalf("active mode = %q after network change, want h1", mode)
		}

		// closing the transport unsubscribes it: a later broadcast must not panic
		// or kick a dead transport
		transport.Close()
		NetworkChanged()
	})
}

// TestKickSkipsDialFailureBackoff: a kick that arrives while the transport is
// waiting out a failed-dial backoff re-dials immediately instead of waiting,
// and the re-dial takes the reconnect fast path (hadConnection semantics).
func TestKickSkipsDialFailureBackoff(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingPlatformServerIpVersion(t, ipVersion)
		settings := testingPlatformTransportSettings()
		// make the backoff long enough that only a kick can plausibly beat it
		settings.ReconnectTimeout = 60 * time.Second
		transport := testingPlatformTransport(t, ctx, platform.url, settings)
		defer transport.Close()

		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			t.Fatal("the transport was never elected")
		}

		// reject dials so the run loop lands in the dial-failure backoff, then
		// drop the live connection to force the re-dial that will fail
		platform.rejecting.Store(true)
		platform.closeConns()
		if !waitForCondition(15*time.Second, func() bool {
			return !transport.IsConnected()
		}) {
			t.Fatal("the transport never observed the drop")
		}
		// let the failing re-dial complete and park in the 60s backoff
		time.Sleep(500 * time.Millisecond)

		platform.rejecting.Store(false)
		connectCount := platform.connectCount.Load()
		// kick periodically rather than once: a kick that lands while the failing
		// dial is still in flight is deliberately dropped (the backoff select arms
		// a fresh notify channel), and re-kicking is exactly what a host emitting
		// repeated path updates does. every wait here is still far below the 60s
		// backoff the kick must beat.
		kicked := false
		for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
			transport.Kick()
			if waitForCondition(250*time.Millisecond, func() bool {
				return connectCount < platform.connectCount.Load()
			}) {
				kicked = true
				break
			}
		}
		if !kicked {
			t.Fatal("the kick did not break the transport out of its dial backoff")
		}
		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			mode, _ := transport.activeMode()
			t.Fatalf("active mode = %q after kicked re-dial, want h1", mode)
		}
	})
}

// TestPlatformTransportReconnects: after the platform drops a connection the
// transport reconnects and is elected again, so a transient disconnect does not
// strand the active mode.
// TestPlatformTransportNetworkChangeKick pins the network-change path: a
// NetworkChanged broadcast closes the live connection and the transport
// re-dials immediately (the host's path-update signal, not a server drop).
func TestPlatformTransportNetworkChangeKick(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingPlatformServerIpVersion(t, ipVersion)
		transport := testingPlatformTransport(t, ctx, platform.url, testingPlatformTransportSettings())

		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			t.Fatal("the transport was never elected")
		}
		connectCount := platform.connectCount.Load()

		// the host reports a network path change
		NetworkChanged()

		if !waitForCondition(15*time.Second, func() bool {
			return connectCount < platform.connectCount.Load()
		}) {
			t.Fatal("the transport did not re-dial after a network change")
		}
		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			mode, _ := transport.activeMode()
			t.Fatalf("active mode = %q after network change, want h1", mode)
		}

		// closing the transport unsubscribes it: a later broadcast must not panic
		// or kick a dead transport
		transport.Close()
		NetworkChanged()
	})
}

func TestPlatformTransportReconnects(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingPlatformServerIpVersion(t, ipVersion)
		transport := testingPlatformTransport(t, ctx, platform.url, testingPlatformTransportSettings())

		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			t.Fatal("the transport was never elected")
		}
		connectCount := platform.connectCount.Load()

		// drop the live connection, leaving the platform accepting
		platform.closeConns()

		if !waitForCondition(15*time.Second, func() bool {
			return connectCount < platform.connectCount.Load()
		}) {
			t.Fatal("the transport did not reconnect after the platform dropped it")
		}
		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			mode, _ := transport.activeMode()
			t.Fatalf("active mode = %q after reconnect, want h1", mode)
		}
	})
}

// TestPlatformTransportCloseInterruptsBlockedH1Write is the regression for a
// teardown dependency inversion found in an on-device iOS trace. The logical
// client was removed at 11:42:07, but its old TCP write did not return until its
// deadline at 11:42:16. Teardown removed routes and then joined the writer while
// the deferred socket Close—which was the only operation able to wake that
// writer—could not run until after the join.
func TestPlatformTransportCloseInterruptsBlockedH1Write(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingPlatformServerIpVersion(t, ipVersion)
		wrappedConn := make(chan *closeInterruptWriteConn, 1)
		strategySettings := DefaultClientStrategySettings()
		strategySettings.EnableResilient = false
		strategySettings.ParallelBlockSize = 1
		strategySettings.MinNextConnectDelay = 0
		strategySettings.MaxNextConnectDelay = 0
		netDialer := &net.Dialer{}
		strategySettings.DialContextSettings = &DialContextSettings{
			DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
				conn, err := netDialer.DialContext(ctx, network, address)
				if err != nil {
					return nil, err
				}
				blocking := newCloseInterruptWriteConn(conn)
				select {
				case wrappedConn <- blocking:
				default:
				}
				return blocking, nil
			},
		}
		strategy := NewClientStrategy(ctx, strategySettings)
		routeManager := NewRouteManager(ctx, "test")
		settings := testingPlatformTransportSettings()
		settings.WriteTimeout = 30 * time.Second
		transport := NewPlatformTransportWithTargetMode(
			ctx,
			strategy,
			routeManager,
			platform.url,
			&ClientAuth{
				ByJwt:      "testing",
				InstanceId: NewId(),
				AppVersion: "testing",
			},
			TransportModeH1,
			settings,
		)
		t.Cleanup(transport.Close)

		if !testingWaitForActiveMode(transport, TransportModeH1, 15*time.Second) {
			t.Fatal("the transport was never elected")
		}
		var blocking *closeInterruptWriteConn
		select {
		case blocking = <-wrappedConn:
		case <-time.After(time.Second):
			t.Fatal("the websocket did not expose its underlying connection")
		}
		// Ensure even the pre-fix path can be released after an assertion, rather
		// than leaving the test process parked until the deliberately long deadline.
		defer blocking.Close()

		blocking.blockWrite.Store(true)
		writer := routeManager.OpenMultiRouteWriter(DestinationId(NewId()))
		defer routeManager.CloseMultiRouteWriter(writer)
		message := MessagePoolGet(32)
		if err := writer.Write(ctx, message, time.Second); err != nil {
			MessagePoolReturn(message)
			t.Fatalf("route write failed: %v", err)
		}
		select {
		case <-blocking.writeStarted:
		case <-time.After(time.Second):
			t.Fatal("the transport writer did not enter the blocked socket write")
		}

		transport.Close()
		select {
		case <-blocking.closed:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("transport Close did not close the socket before joining its blocked writer")
		}
		if !waitForCondition(time.Second, func() bool {
			return !transport.IsConnected()
		}) {
			t.Fatal("transport remained registered after Close interrupted the blocked writer")
		}
	})
}

// CloseAndWait must not confuse logical route removal with completed transport
// teardown. Exact barriers hold first the removed-route seam and then receive
// cleanup; completion may publish only after socket close wakes the writer and
// every owned connection worker returns.
func TestPlatformTransportCloseAndWaitJoinsRouteWriterAndReceiveCleanup(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testCtx, testCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer testCancel()

		platform := newTestingPlatformServerIpVersion(t, ipVersion)
		wrappedConn := make(chan *closeInterruptWriteConn, 1)
		strategySettings := DefaultClientStrategySettings()
		strategySettings.EnableResilient = false
		strategySettings.ParallelBlockSize = 1
		strategySettings.MinNextConnectDelay = 0
		strategySettings.MaxNextConnectDelay = 0
		netDialer := &net.Dialer{}
		strategySettings.DialContextSettings = &DialContextSettings{
			DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
				connection, err := netDialer.DialContext(ctx, network, address)
				if err != nil {
					return nil, err
				}
				blocking := newCloseInterruptWriteConn(connection)
				select {
				case wrappedConn <- blocking:
				default:
				}
				return blocking, nil
			},
		}
		strategy := NewClientStrategy(testCtx, strategySettings)
		routeManager := NewRouteManager(testCtx, "close-and-wait")
		settings := testingPlatformTransportSettings()
		settings.WriteTimeout = 30 * time.Second
		teardownArmed := atomic.Bool{}
		teardownEntered := make(chan struct{})
		releaseTeardown := make(chan struct{})
		receiveCleanupEntered := make(chan struct{})
		releaseReceiveCleanup := make(chan struct{})
		var teardownOnce sync.Once
		var receiveCleanupOnce sync.Once
		settings.afterRoutesRemovedForTest = func() {
			if teardownArmed.Load() {
				teardownOnce.Do(func() {
					close(teardownEntered)
					<-releaseTeardown
				})
			}
		}
		settings.beforeReceiveWorkerCleanupForTest = func() {
			if teardownArmed.Load() {
				receiveCleanupOnce.Do(func() {
					close(receiveCleanupEntered)
					<-releaseReceiveCleanup
				})
			}
		}
		transport := NewPlatformTransportWithTargetMode(
			testCtx,
			strategy,
			routeManager,
			platform.url,
			&ClientAuth{
				ByJwt:      "testing",
				InstanceId: NewId(),
				AppVersion: "testing",
			},
			TransportModeH1,
			settings,
		)
		var releaseTeardownOnce sync.Once
		releaseRouteCleanup := func() {
			releaseTeardownOnce.Do(func() {
				close(releaseTeardown)
			})
		}
		var releaseReceiveOnce sync.Once
		releaseReceiverCleanup := func() {
			releaseReceiveOnce.Do(func() {
				close(releaseReceiveCleanup)
			})
		}
		t.Cleanup(func() {
			releaseRouteCleanup()
			releaseReceiverCleanup()
			transport.Close()
		})

		for !transport.IsConnected() {
			notify := transport.ConnectedNotify()
			if transport.IsConnected() {
				break
			}
			select {
			case <-testCtx.Done():
				t.Fatalf("wait for close-and-wait platform route: %v", testCtx.Err())
			case <-notify:
			}
		}
		var blocking *closeInterruptWriteConn
		select {
		case <-testCtx.Done():
			t.Fatalf("wait for close-and-wait socket: %v", testCtx.Err())
		case blocking = <-wrappedConn:
		}
		blocking.blockWrite.Store(true)
		writer := routeManager.OpenMultiRouteWriter(DestinationId(NewId()))
		defer routeManager.CloseMultiRouteWriter(writer)
		message := MessagePoolGet(32)
		if err := writer.Write(testCtx, message, time.Second); err != nil {
			MessagePoolReturn(message)
			t.Fatalf("write close-and-wait message: %v", err)
		}
		select {
		case <-testCtx.Done():
			t.Fatalf("wait for blocked close-and-wait writer: %v", testCtx.Err())
		case <-blocking.writeStarted:
		}

		teardownArmed.Store(true)
		closeResult := make(chan error, 1)
		go func() {
			closeResult <- transport.CloseAndWait(testCtx)
		}()
		select {
		case <-testCtx.Done():
			t.Fatalf("wait for logical route removal: %v", testCtx.Err())
		case <-teardownEntered:
		}
		if transport.IsConnected() {
			t.Fatal("logical platform route remained registered at teardown barrier")
		}
		if activeRoutes := writer.GetActiveRoutes(); len(activeRoutes) != 0 {
			t.Fatalf("logical platform routes=%d at teardown barrier, want zero", len(activeRoutes))
		}
		select {
		case <-blocking.closed:
			t.Fatal("platform socket closed before the post-route teardown barrier")
		default:
		}
		select {
		case err := <-closeResult:
			t.Fatalf("CloseAndWait returned before writer cleanup: %v", err)
		default:
		}
		select {
		case <-transport.Done():
			t.Fatal("transport completion published before writer cleanup")
		default:
		}

		releaseRouteCleanup()
		select {
		case <-testCtx.Done():
			t.Fatalf("wait for receive-worker cleanup: %v", testCtx.Err())
		case <-receiveCleanupEntered:
		}
		select {
		case <-blocking.closed:
		default:
			t.Fatal("receive cleanup began before the blocked socket was closed")
		}
		select {
		case err := <-closeResult:
			t.Fatalf("CloseAndWait returned before receive cleanup: %v", err)
		default:
		}
		select {
		case <-transport.Done():
			t.Fatal("transport completion published before receive cleanup")
		default:
		}

		releaseReceiverCleanup()
		select {
		case <-testCtx.Done():
			t.Fatalf("join close-and-wait transport: %v", testCtx.Err())
		case err := <-closeResult:
			if err != nil {
				t.Fatalf("close and wait: %v", err)
			}
		}
		select {
		case <-transport.Done():
		default:
			t.Fatal("CloseAndWait returned before Done closed")
		}
		select {
		case <-blocking.closed:
		default:
			t.Fatal("CloseAndWait returned before closing the blocked socket")
		}
	})
}

// A canceled connection attempt remains owned until its dial stack returns.
// The exact dial barrier prevents completion from treating context delivery as
// equivalent to joining the mode runner that is still unwinding it.
func TestPlatformTransportCloseAndWaitJoinsPendingDial(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testCtx, testCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer testCancel()

		dialEntered := make(chan struct{})
		dialCanceled := make(chan struct{})
		releaseDial := make(chan struct{})
		var dialEnteredOnce sync.Once
		var dialCanceledOnce sync.Once
		strategySettings := DefaultClientStrategySettings()
		strategySettings.EnableResilient = false
		strategySettings.ParallelBlockSize = 1
		strategySettings.MinNextConnectDelay = 0
		strategySettings.MaxNextConnectDelay = 0
		strategySettings.DialContextSettings = &DialContextSettings{
			DialContext: func(ctx context.Context, _ string, _ string) (net.Conn, error) {
				dialEnteredOnce.Do(func() {
					close(dialEntered)
				})
				<-ctx.Done()
				dialCanceledOnce.Do(func() {
					close(dialCanceled)
				})
				<-releaseDial
				return nil, ctx.Err()
			},
		}
		transport := NewPlatformTransportWithTargetMode(
			testCtx,
			NewClientStrategy(testCtx, strategySettings),
			NewRouteManager(testCtx, "pending-dial"),
			"ws://"+testLoopbackHost(ipVersion)+":1",
			&ClientAuth{
				ByJwt:      "testing",
				InstanceId: NewId(),
				AppVersion: "testing",
			},
			TransportModeH1,
			testingPlatformTransportSettings(),
		)
		var releaseOnce sync.Once
		release := func() {
			releaseOnce.Do(func() {
				close(releaseDial)
			})
		}
		t.Cleanup(func() {
			release()
			transport.Close()
		})

		select {
		case <-testCtx.Done():
			t.Fatalf("wait for pending platform dial: %v", testCtx.Err())
		case <-dialEntered:
		}
		closeResult := make(chan error, 1)
		go func() {
			closeResult <- transport.CloseAndWait(testCtx)
		}()
		select {
		case <-testCtx.Done():
			t.Fatalf("wait for pending dial cancellation: %v", testCtx.Err())
		case <-dialCanceled:
		}
		select {
		case err := <-closeResult:
			t.Fatalf("CloseAndWait returned before the canceled dial unwound: %v", err)
		default:
		}
		select {
		case <-transport.Done():
			t.Fatal("transport completion published before the canceled dial unwound")
		default:
		}

		release()
		select {
		case <-testCtx.Done():
			t.Fatalf("join released pending dial: %v", testCtx.Err())
		case err := <-closeResult:
			if err != nil {
				t.Fatalf("close and join pending dial: %v", err)
			}
		}
	})
}

// TestPlatformTransportCloseInterruptsBlockedH3Write covers the adjacent QUIC
// teardown path. A peer that stops reading exhausts stream flow control and
// parks Framer.Write inside quic.Stream.Write. Canceling handleCtx cannot wake
// that write; connection close must precede a join of both socket workers.
func TestPlatformTransportCloseInterruptsBlockedH3Write(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		certPem, keyPem, err := selfSign(
			[]string{testLoopbackIp(ipVersion)},
			testLoopbackIp(ipVersion),
			24*time.Hour,
			24*time.Hour,
		)
		if err != nil {
			t.Fatal(err)
		}
		cert, err := tls.X509KeyPair(certPem, keyPem)
		if err != nil {
			t.Fatal(err)
		}
		const nextProto = "urnetwork-platform-test"
		listener, err := quic.ListenAddrEarly(
			testLoopbackHostPort(ipVersion, 0),
			&tls.Config{
				Certificates: []tls.Certificate{cert},
				NextProtos:   []string{nextProto},
			},
			&quic.Config{
				MaxIdleTimeout:                 30 * time.Second,
				InitialStreamReceiveWindow:     32 * 1024,
				MaxStreamReceiveWindow:         32 * 1024,
				InitialConnectionReceiveWindow: 64 * 1024,
				MaxConnectionReceiveWindow:     64 * 1024,
			},
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = listener.Close()
		})

		serverCtx, serverCancel := context.WithCancel(context.Background())
		defer serverCancel()
		serverConn := make(chan *quic.Conn, 1)
		serverErr := make(chan error, 1)
		framerSettings := DefaultFramerSettings(int(DefaultClientSettings().MinimumMessageLenLimit()))
		go func() {
			conn, acceptErr := listener.Accept(serverCtx)
			if acceptErr != nil {
				serverErr <- acceptErr
				return
			}
			serverConn <- conn
			stream, acceptErr := conn.AcceptStream(serverCtx)
			if acceptErr != nil {
				serverErr <- acceptErr
				return
			}
			framer := NewFramer(framerSettings)
			authBytes, readErr := framer.Read(stream)
			if readErr != nil {
				serverErr <- readErr
				return
			}
			writeErr := framer.Write(stream, authBytes)
			MessagePoolReturn(authBytes)
			if writeErr != nil {
				serverErr <- writeErr
				return
			}
			// Deliberately never read another byte. The client's next large writes
			// consume the fixed receive credit and then block.
			<-conn.Context().Done()
		}()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		settings := testingPlatformTransportSettings()
		settings.H3Port = listener.Addr().(*net.UDPAddr).Port
		settings.resolveH3AddrsForTest = testingH3LoopbackResolver(ipVersion, settings.H3Port)
		settings.WriteTimeout = 30 * time.Second
		settings.QuicTlsConfig = &tls.Config{
			InsecureSkipVerify: true, // test-only self-signed endpoint
			NextProtos:         []string{nextProto},
		}
		settings.FramerSettings = framerSettings
		receiveCleanupArmed := atomic.Bool{}
		receiveCleanupEntered := make(chan struct{})
		releaseReceiveCleanup := make(chan struct{})
		var receiveCleanupOnce sync.Once
		settings.beforeReceiveWorkerCleanupForTest = func() {
			if receiveCleanupArmed.Load() {
				receiveCleanupOnce.Do(func() {
					close(receiveCleanupEntered)
					<-releaseReceiveCleanup
				})
			}
		}
		routeManager := NewRouteManager(ctx, "test")
		transport := NewPlatformTransportWithTargetMode(
			ctx,
			NewClientStrategyWithDefaults(ctx),
			routeManager,
			"https://"+testLoopbackHost(ipVersion),
			&ClientAuth{
				ByJwt:      "testing",
				InstanceId: NewId(),
				AppVersion: "testing",
			},
			TransportModeH3,
			settings,
		)
		var releaseOnce sync.Once
		releaseReceiverCleanup := func() {
			releaseOnce.Do(func() {
				close(releaseReceiveCleanup)
			})
		}
		t.Cleanup(func() {
			releaseReceiverCleanup()
			transport.Close()
		})

		var accepted *quic.Conn
		select {
		case accepted = <-serverConn:
		case acceptErr := <-serverErr:
			t.Fatal(acceptErr)
		case <-time.After(5 * time.Second):
			t.Fatal("the QUIC server did not accept the platform connection")
		}
		if !testingWaitForActiveMode(transport, TransportModeH3, 5*time.Second) {
			t.Fatal("the H3 transport was never elected")
		}

		writer := routeManager.OpenMultiRouteWriter(DestinationId(NewId()))
		defer routeManager.CloseMultiRouteWriter(writer)
		blocked := false
		for range 128 {
			message := MessagePoolGet(3 * 1024)
			if writeErr := writer.Write(ctx, message, 20*time.Millisecond); writeErr != nil {
				MessagePoolReturn(message)
				blocked = true
				break
			}
		}
		if !blocked {
			t.Fatal("the unread QUIC stream never exhausted its bounded send route")
		}

		receiveCleanupArmed.Store(true)
		closeCtx, closeCancel := context.WithTimeout(ctx, 15*time.Second)
		defer closeCancel()
		closeResult := make(chan error, 1)
		go func() {
			closeResult <- transport.CloseAndWait(closeCtx)
		}()
		select {
		case <-receiveCleanupEntered:
		case <-closeCtx.Done():
			t.Fatalf("H3 receive worker did not reach cleanup: %v", closeCtx.Err())
		}
		select {
		case err := <-closeResult:
			t.Fatalf("CloseAndWait returned before H3 receive cleanup: %v", err)
		default:
		}
		select {
		case <-transport.Done():
			t.Fatal("H3 transport completion published before receive cleanup")
		default:
		}
		releaseReceiverCleanup()
		select {
		case err := <-closeResult:
			if err != nil {
				t.Fatalf("close and join flow-control-blocked H3 transport: %v", err)
			}
		case <-closeCtx.Done():
			t.Fatalf("join flow-control-blocked H3 transport: %v", closeCtx.Err())
		}
		select {
		case <-accepted.Context().Done():
		case <-closeCtx.Done():
			t.Fatalf("peer did not observe flow-control-blocked H3 close: %v", closeCtx.Err())
		}
	})
}

// An H3 Framer.Read transfers its pooled message to the receive channel. Route
// removal makes that channel unreachable to later MultiRouteReader snapshots,
// so transport completion must return any message still queued there.
func TestPlatformTransportH3CloseDrainsQueuedReceiveOwnership(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		certPem, keyPem, err := selfSign(
			[]string{testLoopbackIp(ipVersion)},
			testLoopbackIp(ipVersion),
			24*time.Hour,
			24*time.Hour,
		)
		if err != nil {
			t.Fatal(err)
		}
		cert, err := tls.X509KeyPair(certPem, keyPem)
		if err != nil {
			t.Fatal(err)
		}
		const nextProto = "urnetwork-platform-pool-test"
		listener, err := quic.ListenAddrEarly(
			testLoopbackHostPort(ipVersion, 0),
			&tls.Config{
				Certificates: []tls.Certificate{cert},
				NextProtos:   []string{nextProto},
			},
			&quic.Config{MaxIdleTimeout: 30 * time.Second},
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = listener.Close()
		})

		serverCtx, serverCancel := context.WithCancel(t.Context())
		defer serverCancel()
		serverErrors := make(chan error, 1)
		serverDone := make(chan struct{})
		framerSettings := DefaultFramerSettings(int(DefaultClientSettings().MinimumMessageLenLimit()))
		go func() {
			defer close(serverDone)
			connection, acceptErr := listener.Accept(serverCtx)
			if acceptErr != nil {
				serverErrors <- acceptErr
				return
			}
			stream, acceptErr := connection.AcceptStream(serverCtx)
			if acceptErr != nil {
				serverErrors <- acceptErr
				return
			}
			framer := NewFramer(framerSettings)
			authBytes, readErr := framer.Read(stream)
			if readErr != nil {
				serverErrors <- readErr
				return
			}
			writeErr := framer.Write(stream, authBytes)
			MessagePoolReturn(authBytes)
			if writeErr != nil {
				serverErrors <- writeErr
				return
			}
			if writeErr = framer.Write(stream, make([]byte, 128)); writeErr != nil {
				serverErrors <- writeErr
				return
			}
			<-connection.Context().Done()
		}()

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		settings := testingPlatformTransportSettings()
		settings.H3Port = listener.Addr().(*net.UDPAddr).Port
		settings.resolveH3AddrsForTest = testingH3LoopbackResolver(ipVersion, settings.H3Port)
		settings.QuicTlsConfig = &tls.Config{
			InsecureSkipVerify: true, // test-only self-signed endpoint
			NextProtos:         []string{nextProto},
		}
		settings.FramerSettings = framerSettings
		settings.TransportBufferSize = 1
		receiveWitnesses := make(chan []byte, 1)
		settings.afterH3ReceiveEnqueueForTest = func(message []byte) {
			witness := MessagePoolShareReadOnly(message)
			select {
			case receiveWitnesses <- witness:
			default:
				MessagePoolReturn(witness)
			}
		}
		transport := NewPlatformTransportWithTargetMode(
			ctx,
			NewClientStrategyWithDefaults(ctx),
			NewRouteManager(ctx, "h3-receive-pool"),
			"https://"+testLoopbackHost(ipVersion),
			&ClientAuth{
				ByJwt:      "testing",
				InstanceId: NewId(),
				AppVersion: "testing",
			},
			TransportModeH3,
			settings,
		)
		t.Cleanup(transport.Close)

		var witness []byte
		select {
		case witness = <-receiveWitnesses:
		case serverErr := <-serverErrors:
			t.Fatal(serverErr)
		case <-ctx.Done():
			t.Fatalf("wait for H3 receive enqueue: %v", ctx.Err())
		}
		closeCtx, closeCancel := context.WithTimeout(ctx, 5*time.Second)
		defer closeCancel()
		if err := transport.CloseAndWait(closeCtx); err != nil {
			MessagePoolReturn(witness)
			t.Fatalf("close and join H3 receive owner: %v", err)
		}
		if !MessagePoolReturn(witness) {
			// Reclaim the old queued owner too, so a failure does not contaminate
			// later process-wide pool checks.
			MessagePoolReturn(witness)
			t.Fatal("H3 transport completion retained its queued Framer.Read owner")
		}
		select {
		case <-serverDone:
		case serverErr := <-serverErrors:
			t.Fatal(serverErr)
		case <-closeCtx.Done():
			t.Fatalf("join H3 pool test server: %v", closeCtx.Err())
		}
	})
}

// K1: the extender set of a transport is published through a change counter,
// and an owner may share one counter across transport generations. The counter
// is therefore incremented from whatever it holds: two transports counting
// their own changes would write the same value twice, and the second write
// would notify nobody -- exactly the case where a migration replacement
// connects through a different extender than the transport it replaces.
func TestPlatformTransportExtenderIpsShareOneChangeCounter(t *testing.T) {
	extenderIpsMonitor := NewMonitorValue[uint64](0)
	first := &PlatformTransport{
		extenderIpCounts:   map[netip.Addr]int{},
		extenderIpsMonitor: extenderIpsMonitor,
	}
	second := &PlatformTransport{
		extenderIpCounts:   map[netip.Addr]int{},
		extenderIpsMonitor: extenderIpsMonitor,
	}
	firstIp := netip.MustParseAddr("192.0.2.141")
	secondIp := netip.MustParseAddr("192.0.2.142")

	notified := func(change chan struct{}) bool {
		select {
		case <-change:
			return true
		default:
			return false
		}
	}

	_, change := extenderIpsMonitor.Get()
	first.changeExtenderIp(firstIp, 1)
	if !notified(change) {
		t.Fatal("the first connection through an extender did not notify")
	}
	if ips := first.ExtenderIps(); len(ips) != 1 || ips[0] != firstIp {
		t.Fatalf("extender ips = %v, want [%v]", ips, firstIp)
	}

	// a second connection through the same extender does not change the set
	_, change = extenderIpsMonitor.Get()
	first.changeExtenderIp(firstIp, 1)
	if notified(change) {
		t.Fatal("a second connection through the same extender notified")
	}

	// the replacement generation, sharing the counter
	second.changeExtenderIp(secondIp, 1)
	if !notified(change) {
		t.Fatal("the replacement transport's extender did not notify")
	}
	if ips := second.ExtenderIps(); len(ips) != 1 || ips[0] != secondIp {
		t.Fatalf("replacement extender ips = %v, want [%v]", ips, secondIp)
	}

	// the retired generation releases both of its connections
	_, change = extenderIpsMonitor.Get()
	first.changeExtenderIp(firstIp, -1)
	if notified(change) {
		t.Fatal("releasing one of two connections through an extender notified")
	}
	first.changeExtenderIp(firstIp, -1)
	if !notified(change) {
		t.Fatal("releasing the last connection through an extender did not notify")
	}
	if ips := first.ExtenderIps(); len(ips) != 0 {
		t.Fatalf("extender ips = %v, want none", ips)
	}
}
