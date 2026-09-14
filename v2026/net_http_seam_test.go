package connect

// This file verifies the client-strategy network injection seams without
// depending on the performance harness that consumes them.

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
)

// A direct TLS dial must use ConnectSettings.DialContext when one is supplied,
// even when no proxy is configured. The old direct branch bypassed this seam.
func TestNormalTlsDialUsesInjectedDialContext(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		server := newFamilyHttptestTlsServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("ok"))
		}))
		defer server.Close()

		serverTransport, ok := server.Client().Transport.(*http.Transport)
		if !ok {
			t.Fatalf("unexpected test server transport type %T", server.Client().Transport)
		}
		settings := DefaultClientStrategySettings()
		settings.TlsConfig = serverTransport.TLSClientConfig.Clone()
		var dialCount atomic.Int32
		settings.DialContextSettings = &DialContextSettings{
			DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
				dialCount.Add(1)
				return (&net.Dialer{}).DialContext(ctx, network, address)
			},
		}
		dialer := &clientDialer{
			dialTlsContext:     newNormalDialTlsContext(settings, clientWebSocketNextProtos),
			httpDialTlsContext: newNormalDialTlsContext(settings, clientHttpNextProtos),
			settings:           settings,
		}
		client := dialer.HttpClient()
		defer client.CloseIdleConnections()

		response, err := client.Get(server.URL)
		if err != nil {
			t.Fatal(err)
		}
		_, readErr := io.Copy(io.Discard, response.Body)
		closeErr := response.Body.Close()
		if readErr != nil {
			t.Fatal(readErr)
		}
		if closeErr != nil {
			t.Fatal(closeErr)
		}
		if got := dialCount.Load(); got != 1 {
			t.Fatalf("injected dial context called %d times, expected one", got)
		}
	})
}

// Explicit extender endpoints are copied into persistent dialers. Discovery
// collapse and custom-discovery replacement must not remove or caller-mutate
// these architectural fixtures.
func TestClientStrategyExactExtenderConfigsAreCopiedAndPersistent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	extenderConfig := &ExtenderConfig{
		Profile: ExtenderProfile{
			ConnectMode: ExtenderConnectModeTcpTls,
			ServerName:  "extender.test",
			Port:        8443,
		},
		Ip:     netip.MustParseAddr("192.0.2.10"),
		Secret: "original-secret",
	}
	settings := DefaultClientStrategySettings()
	settings.EnableNormal = false
	settings.EnableResilient = false
	settings.ExposeServerIps = false
	settings.ExposeServerHostNames = false
	settings.ExtenderDropTimeout = 0
	settings.ExtenderConfigs = []*ExtenderConfig{nil, extenderConfig}
	strategy := NewClientStrategy(ctx, settings)

	strategy.mutex.Lock()
	if len(strategy.dialers) != 1 {
		strategy.mutex.Unlock()
		t.Fatalf("configured strategy has %d dialers, expected one", len(strategy.dialers))
	}
	var configuredDialer *clientDialer
	for dialer := range strategy.dialers {
		configuredDialer = dialer
	}
	strategy.mutex.Unlock()
	if !configuredDialer.persistent {
		t.Fatal("configured extender dialer is not persistent")
	}
	if configuredDialer.extenderConfig == extenderConfig {
		t.Fatal("configured extender retained the caller-owned pointer")
	}
	if configuredDialer.extenderConfig.Secret != "original-secret" {
		t.Fatalf("configured secret = %q, expected original value", configuredDialer.extenderConfig.Secret)
	}

	extenderConfig.Secret = "caller-mutated-secret"
	if configuredDialer.extenderConfig.Secret != "original-secret" {
		t.Fatal("caller mutation changed the installed extender config")
	}
	strategy.collapseExtenderDialers()
	strategy.SetCustomExtenders(map[netip.Addr]string{
		netip.MustParseAddr("192.0.2.20"): "discovered-secret",
	})
	strategy.mutex.Lock()
	_, retained := strategy.dialers[configuredDialer]
	strategy.mutex.Unlock()
	if !retained {
		t.Fatal("discovery maintenance removed the configured extender")
	}
}

// Defaults leave both new strategy seams disabled, preserving host TLS dialing
// and dynamic extender discovery.
func TestClientStrategySeamDefaultsAreDisabled(t *testing.T) {
	settings := DefaultClientStrategySettings()
	if settings.DialContextSettings != nil {
		t.Fatal("default strategy unexpectedly injects a dial context")
	}
	if settings.ExtenderConfigs != nil {
		t.Fatal("default strategy unexpectedly installs exact extenders")
	}
	if settings.TlsConfig == nil || settings.TlsConfig.MinVersion < tls.VersionTLS12 {
		t.Fatal("default strategy lost its production TLS configuration")
	}
}

// The MOBILE configuration -- no proxy, no injected dial context -- is the one
// that took the old fast path and bypassed ConnectSettings.DialContext
// entirely. That bypass is why DisableIpv4/DisableIpv6 were dead: the flags
// were honored by the fragment and reorder dialers and ignored by the default
// one, so the strategy raced a forced dialer against an unforced one.
//
// The hook is on ConnectSettings.DialContext -- the seam the family policy
// lives on -- and records the network string AFTER controlDialNetwork has
// resolved it. That placement is what makes this test FAIL on unfixed code:
// with the fast path still present the mobile shape returns a raw tls.Dialer
// and never calls DialContext at all, so the hook never fires and the test
// fails on "the seam is still bypassed".
//
// A hook on the net.Dialer's Control callback could not do this. Control is
// documented to receive an already-family-specific network ("tcp4"/"tcp6"),
// never "tcp", so against an IPv4 literal it records "tcp4" whether or not the
// bypass was ever closed -- a guard that passes on the unfixed code it exists
// to catch.
//
// The target is a NAME, not an address literal. controlDialNetwork
// deliberately leaves a literal's network string alone -- the address already
// fixes the family, so narrowing there can only break a working dial -- and
// the seam this test exists to guard is the one that steers a RESOLUTION.
func TestNormalTlsDialHonorsFamilyPolicyWithNoInjectedDialContext(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		// the forced family is the one under test, so the narrowed network string
		// is asserted for both families rather than for IPv4 alone
		policy := IpFamilyForce4
		if ipVersion == 6 {
			policy = IpFamilyForce6
		}
		SetControlIpFamilyPolicy(policy)
		defer SetControlIpFamilyPolicy(IpFamilyAuto)

		settings := DefaultClientStrategySettings()
		if settings.ProxySettings != nil || settings.DialContextSettings != nil {
			t.Fatal("the default settings are no longer the mobile shape this test pins")
		}

		var mutex sync.Mutex
		var networks []string
		settings.DialNetworkHook = func(network string, addr string) {
			mutex.Lock()
			defer mutex.Unlock()
			networks = append(networks, network)
		}

		dialTls := newNormalDialTlsContext(settings, clientHttpNextProtos)
		// the host is never reached: .invalid is reserved by RFC 2606 and never
		// resolves. What is under test is the NETWORK STRING the seam resolved,
		// which is recorded before the dial is attempted.
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		conn, err := dialTls(ctx, "tcp", "family-seam.invalid:443")
		if err == nil {
			conn.Close()
		}

		mutex.Lock()
		defer mutex.Unlock()
		if len(networks) == 0 {
			t.Fatal("ConnectSettings.DialContext was never called -- the seam is still bypassed")
		}
		if want := testTcpNetwork(ipVersion); len(networks) != 1 || networks[0] != want {
			t.Fatalf("resolved %v, want exactly [%s] under the forced v%d policy", networks, want, ipVersion)
		}
	})
}

// A pooled connection that connected cleanly and later went dark is invisible
// to any dial-time logic: http/2 multiplexes every later request onto it, and
// with no health check each one hangs to the request timeout. Go's default for
// HTTP2Config.SendPingTimeout is zero, which its own doc defines as "no health
// check is performed".
//
// This also pins that the config is built on EVERY platform. It used to be
// built only under the mobile memory guard, so desktop had no HTTP2Config at
// all and therefore no health check either.
func TestHttpClientConfiguresHttp2HealthCheck(t *testing.T) {
	settings := DefaultClientStrategySettings()
	dialer := &clientDialer{
		dialTlsContext:     newNormalDialTlsContext(settings, clientWebSocketNextProtos),
		httpDialTlsContext: newNormalDialTlsContext(settings, clientHttpNextProtos),
		settings:           settings,
	}
	client := dialer.HttpClient()
	defer client.CloseIdleConnections()

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("unexpected transport type %T", client.Transport)
	}
	if transport.HTTP2 == nil {
		t.Fatal("no HTTP2Config: a pooled dead connection is never detected")
	}
	// asserted against the SETTINGS, not against bare literals: the durations
	// are tunable fields, so a test that pinned 10s/5s directly would fail an
	// embedder that legitimately tuned them, and would stop testing that the
	// transport is wired to the settings at all
	if settings.Http2SendPingTimeout <= 0 {
		t.Fatal("the default Http2SendPingTimeout is zero, which disables the health check")
	}
	if settings.Http2PingTimeout <= 0 {
		t.Fatal("the default Http2PingTimeout is zero")
	}
	if transport.HTTP2.SendPingTimeout != settings.Http2SendPingTimeout {
		t.Fatalf("SendPingTimeout is %s, want the settings value %s",
			transport.HTTP2.SendPingTimeout, settings.Http2SendPingTimeout)
	}
	if transport.HTTP2.PingTimeout != settings.Http2PingTimeout {
		t.Fatalf("PingTimeout is %s, want the settings value %s",
			transport.HTTP2.PingTimeout, settings.Http2PingTimeout)
	}
}

// A request deadline cannot unwind HTTP/2 after a connection write owns the
// transport's write mutex: response reads, response closes, and cancellation
// resets can all wait behind that write. The transport therefore needs its own
// progress deadline on the socket write, bounded by the connection/no-progress
// budget.
func TestHttpClientBoundsHttp2WriteProgress(t *testing.T) {
	settings := DefaultClientStrategySettings()
	settings.ConnectTimeout = 37 * time.Second
	settings.RequestTimeout = 91 * time.Second
	dialer := &clientDialer{
		dialTlsContext:     newNormalDialTlsContext(settings, clientWebSocketNextProtos),
		httpDialTlsContext: newNormalDialTlsContext(settings, clientHttpNextProtos),
		settings:           settings,
	}
	client := dialer.HttpClient()
	defer client.CloseIdleConnections()

	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("unexpected transport type %T", client.Transport)
	}
	if transport.HTTP2 == nil {
		t.Fatal("no HTTP2Config: connection writes have no progress deadline")
	}
	if transport.HTTP2.WriteByteTimeout != settings.ConnectTimeout {
		t.Fatalf(
			"WriteByteTimeout is %s, want connection-progress budget %s",
			transport.HTTP2.WriteByteTimeout,
			settings.ConnectTimeout,
		)
	}
}

// assertNativeHttp2WriteProgress checks the shared net/http transport
// invariant without opening a connection.
func assertNativeHttp2WriteProgress(t *testing.T, clientName string, transport *http.Transport, want time.Duration) {
	t.Helper()
	if transport.HTTP2 == nil {
		t.Fatalf("%s has no HTTP2Config", clientName)
	}
	if transport.HTTP2.WriteByteTimeout != want {
		t.Fatalf("%s WriteByteTimeout is %s, want %s", clientName, transport.HTTP2.WriteByteTimeout, want)
	}
}

// Every native HTTP/2 constructor shares the same stalled-write invariant.
// This pins the three adjacent constructors so a later isolated refactor cannot
// silently restore the same unbounded write outside clientDialer.HttpClient.
func TestAdjacentHttp2ConstructorsBoundWriteProgress(t *testing.T) {
	settings := DefaultConnectSettings()
	settings.ConnectTimeout = 37 * time.Second
	settings.RequestTimeout = 91 * time.Second

	extenderClient := NewExtenderHttpClient(settings, &ExtenderConfig{
		Profile: ExtenderProfile{
			ConnectMode: ExtenderConnectModeTcpTls,
			ServerName:  "extender.test",
			Port:        443,
		},
		Ip: netip.MustParseAddr("192.0.2.1"),
	})
	defer extenderClient.CloseIdleConnections()
	extenderTransport, ok := extenderClient.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("unexpected extender transport type %T", extenderClient.Transport)
	}
	assertNativeHttp2WriteProgress(t, "extender", extenderTransport, settings.ConnectTimeout)

	streamTransport := newHttpPostStreamTransport(settings)
	defer streamTransport.CloseIdleConnections()
	assertNativeHttp2WriteProgress(t, "streaming POST", streamTransport, settings.ConnectTimeout)

	dohSettings := DefaultDohSettings()
	dohSettings.ConnectTimeout = settings.ConnectTimeout
	dohSettings.RequestTimeout = settings.RequestTimeout
	dohTransport := &http2.Transport{}
	configureDohHttp2Transport(dohTransport, dohSettings)
	if dohTransport.WriteByteTimeout != settings.ConnectTimeout {
		t.Fatalf(
			"DoH WriteByteTimeout is %s, want %s",
			dohTransport.WriteByteTimeout,
			settings.ConnectTimeout,
		)
	}
}

// writeDeadlineClearErrorConn injects a failure when a write deadline is
// cleared, after forwarding nonzero deadlines to its wrapped connection.
type writeDeadlineClearErrorConn struct {
	net.Conn
	clearErr error
	setCalls atomic.Int32
}

// SetWriteDeadline records each attempt and injects the configured clear
// failure only for a zero deadline.
func (self *writeDeadlineClearErrorConn) SetWriteDeadline(deadline time.Time) error {
	self.setCalls.Add(1)
	if deadline.IsZero() {
		return self.clearErr
	}
	return self.Conn.SetWriteDeadline(deadline)
}

// The extender header is a raw post-TLS write, outside net/http. Its helper
// must install the same phase deadline before entering the socket write.
func TestWriteConnPhaseDeadlineBoundsExtenderHeader(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	gate := newHttp2WriteGateConn(clientConn)
	defer gate.Close()
	gate.arm()
	clearErr := errors.New("clear write deadline")
	deadlineConn := &writeDeadlineClearErrorConn{Conn: gate, clearErr: clearErr}

	writeDone := make(chan error, 1)
	go func() {
		writeDone <- writeConnPhaseWithDeadline(
			context.Background(),
			deadlineConn,
			[]byte("extender header"),
			time.Minute,
		)
	}()
	waitForHttp2Barrier(t, "extender write deadline", gate.deadlineSeen)
	waitForHttp2Barrier(t, "blocked extender header write", gate.writeEntered)
	gate.expire()
	if err := waitForHttp2BodyRead(t, writeDone); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("extender header write error is %v, want deadline exceeded", err)
	}
}

// A successful write still fails when its deadline cannot be cleared, so a
// pooled connection is never returned with stale write state.
func TestWriteConnPhaseDeadlineReportsClearFailure(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	clearErr := errors.New("clear write deadline")
	deadlineConn := &writeDeadlineClearErrorConn{Conn: clientConn, clearErr: clearErr}
	buffer := []byte("extender header")
	readDone := make(chan error, 1)
	go func() {
		readBuffer := make([]byte, len(buffer))
		_, err := io.ReadFull(serverConn, readBuffer)
		readDone <- err
	}()

	err := writeConnPhaseWithDeadline(context.Background(), deadlineConn, buffer, time.Minute)
	if !errors.Is(err, clearErr) {
		t.Fatalf("write result is %v, want clear deadline failure", err)
	}
	select {
	case err := <-readDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the raw write reader")
	}
}

// A context canceled before the phase begins prevents both the deadline
// mutation and the write callback.
func TestConnWritePhaseDeadlineRejectsCanceledContextBeforeWrite(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	deadlineConn := &writeDeadlineClearErrorConn{
		Conn:     clientConn,
		clearErr: errors.New("clear write deadline"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	writeCalled := false
	err := withConnWritePhaseDeadline(ctx, deadlineConn, time.Minute, func() error {
		writeCalled = true
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("write phase result is %v, want context canceled", err)
	}
	if writeCalled {
		t.Fatal("write callback ran for an already canceled context")
	}
	if deadlineConn.setCalls.Load() != 0 {
		t.Fatalf("write deadline changed %d times for an already canceled context", deadlineConn.setCalls.Load())
	}
}

// Off may drain a partial TLS record after HandshakeContext has returned, so
// the expired handshake context cannot interrupt that raw write. The dialer
// wrapper must arm a write deadline before Off enters the drain.
func TestOffResilientTlsConnBoundsPartialRecordDrain(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer serverConn.Close()
	gate := newHttp2WriteGateConn(clientConn)
	defer gate.Close()
	gate.arm()
	rconn := NewResilientTlsConn(gate, true, false)
	rconn.buffer = []byte("partial TLS record")

	offDone := make(chan error, 1)
	go func() {
		offDone <- offResilientTlsConn(context.Background(), rconn, time.Minute)
	}()
	waitForHttp2Barrier(t, "resilient drain write deadline", gate.deadlineSeen)
	waitForHttp2Barrier(t, "blocked resilient drain write", gate.writeEntered)
	gate.expire()
	if err := waitForHttp2BodyRead(t, offDone); !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("resilient drain error is %v, want deadline exceeded", err)
	}
	if rconn.Enabled() {
		t.Fatal("resilient connection remained enabled after failed drain")
	}
	if len(rconn.buffer) != 0 {
		t.Fatalf("resilient connection retained %d drain bytes after failure", len(rconn.buffer))
	}
}

// http2WriteGateConn is transparent until arm. After that, a raw socket write
// makes no progress until the test expires it or the connection closes.
// newNormalDialTlsContext wraps this connection in a real *tls.Conn, so the
// production transport completes TLS and negotiates HTTP/2 normally. The
// explicit channels make the write/deadline order deterministic; wall-clock
// timers are only containment for a broken test.
type http2WriteGateConn struct {
	net.Conn

	stateLock       sync.Mutex
	armed           bool
	writeEntered    chan struct{}
	deadlineSeen    chan struct{}
	expireWrite     chan struct{}
	closed          chan struct{}
	writeEnterOnce  sync.Once
	deadlineSeeOnce sync.Once
	expireOnce      sync.Once
	closeOnce       sync.Once
	closeErr        error
}

// newHttp2WriteGateConn wraps the raw socket beneath the production TLS
// connection with explicit test barriers.
func newHttp2WriteGateConn(conn net.Conn) *http2WriteGateConn {
	return &http2WriteGateConn{
		Conn:         conn,
		writeEntered: make(chan struct{}),
		deadlineSeen: make(chan struct{}),
		expireWrite:  make(chan struct{}),
		closed:       make(chan struct{}),
	}
}

// arm switches subsequent raw writes from pass-through to barrier-controlled.
func (self *http2WriteGateConn) arm() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.armed = true
}

// Write passes through before arm and exposes a deterministic blocked socket
// write afterward.
func (self *http2WriteGateConn) Write(p []byte) (int, error) {
	self.stateLock.Lock()
	armed := self.armed
	self.stateLock.Unlock()
	if !armed {
		return self.Conn.Write(p)
	}
	self.writeEnterOnce.Do(func() {
		close(self.writeEntered)
	})
	select {
	case <-self.expireWrite:
		return 0, os.ErrDeadlineExceeded
	case <-self.closed:
		return 0, net.ErrClosed
	}
}

// SetWriteDeadline records that the HTTP/2 writer installed a nonzero deadline
// after the gate was armed, while preserving the underlying connection call.
func (self *http2WriteGateConn) SetWriteDeadline(deadline time.Time) error {
	if err := self.Conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	self.stateLock.Lock()
	armed := self.armed
	self.stateLock.Unlock()
	if armed && !deadline.IsZero() {
		self.deadlineSeeOnce.Do(func() {
			close(self.deadlineSeen)
		})
	}
	return nil
}

// expire deterministically models the configured write deadline firing.
func (self *http2WriteGateConn) expire() {
	self.expireOnce.Do(func() {
		close(self.expireWrite)
	})
}

// Close releases any blocked write and closes the underlying connection once.
func (self *http2WriteGateConn) Close() error {
	self.closeOnce.Do(func() {
		close(self.closed)
		self.closeErr = self.Conn.Close()
	})
	return self.closeErr
}

// http2BlockedBodyRead owns one real TLS/HTTP2 response whose flow-control
// update is stopped at http2WriteGateConn.Write.
type http2BlockedBodyRead struct {
	gate          *http2WriteGateConn
	cancelRequest context.CancelFunc
	response      *http.Response
	readDone      <-chan error
}

// newHttp2BlockedBodyRead negotiates a real local TLS/HTTP2 connection, then
// arms its raw socket before consuming a response large enough to return flow
// control credit.
func newHttp2BlockedBodyRead(t *testing.T, ipVersion int, disableWriteTimeout bool) *http2BlockedBodyRead {
	t.Helper()
	responseBody := bytes.Repeat([]byte("x"), 128*1024)
	server := newFamilyHttptestUnstartedServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		_, _ = w.Write(responseBody)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	t.Cleanup(server.Close)

	serverTransport, ok := server.Client().Transport.(*http.Transport)
	if !ok {
		t.Fatalf("unexpected test server transport type %T", server.Client().Transport)
	}
	settings := DefaultClientStrategySettings()
	// These are deliberately much longer than the containment timer. The fake
	// connection's expireWrite barrier, not wall time, models deadline expiry.
	settings.ConnectTimeout = time.Minute
	settings.RequestTimeout = 2 * time.Minute
	settings.TlsConfig = serverTransport.TLSClientConfig.Clone()

	gateReady := make(chan *http2WriteGateConn, 1)
	var captureGate sync.Once
	settings.DialContextSettings = &DialContextSettings{
		DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
			conn, err := (&net.Dialer{}).DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			gate := newHttp2WriteGateConn(conn)
			captureGate.Do(func() {
				gateReady <- gate
			})
			return gate, nil
		},
	}
	dialer := &clientDialer{
		dialTlsContext:     newNormalDialTlsContext(settings, clientWebSocketNextProtos),
		httpDialTlsContext: newNormalDialTlsContext(settings, clientHttpNextProtos),
		settings:           settings,
	}
	client := dialer.HttpClient()
	t.Cleanup(client.CloseIdleConnections)
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("unexpected transport type %T", client.Transport)
	}
	if transport.HTTP2 == nil {
		t.Fatal("no HTTP2Config")
	}
	if disableWriteTimeout {
		transport.HTTP2.WriteByteTimeout = 0
	}

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(requestCtx, http.MethodGet, server.URL, nil)
	if err != nil {
		cancelRequest()
		t.Fatal(err)
	}
	response, err := client.Do(request)
	if err != nil {
		cancelRequest()
		t.Fatal(err)
	}
	if response.ProtoMajor != 2 {
		cancelRequest()
		_ = response.Body.Close()
		t.Fatalf("response protocol is %s, want HTTP/2", response.Proto)
	}

	var gate *http2WriteGateConn
	select {
	case gate = <-gateReady:
	case <-time.After(5 * time.Second):
		cancelRequest()
		_ = response.Body.Close()
		t.Fatal("timed out waiting for the captured HTTP/2 connection")
	}
	gate.arm()
	readDone := make(chan error, 1)
	go func() {
		_, readErr := io.Copy(io.Discard, response.Body)
		readDone <- readErr
	}()
	t.Cleanup(func() {
		cancelRequest()
		gate.expire()
		_ = gate.Close()
	})

	return &http2BlockedBodyRead{
		gate:          gate,
		cancelRequest: cancelRequest,
		response:      response,
		readDone:      readDone,
	}
}

// waitForHttp2Barrier waits only as a long containment bound around an explicit
// synchronization point.
func waitForHttp2Barrier(t *testing.T, name string, barrier <-chan struct{}) {
	t.Helper()
	select {
	case <-barrier:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

// waitForHttp2BodyRead contains a broken test after its explicit release
// barrier has fired.
func waitForHttp2BodyRead(t *testing.T, readDone <-chan error) error {
	t.Helper()
	select {
	case err := <-readDone:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the HTTP/2 response-body read")
		return nil
	}
}

// Once a response-body read returns enough HTTP/2 flow-control credit, it owns
// the connection write mutex while flushing WINDOW_UPDATE. This control proves
// that canceling its request cannot interrupt the raw Write: only the explicit
// socket-release barrier lets the caller return.
func TestHttp2CanceledBodyReadDoesNotInterruptBlockedWrite(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		blockedRead := newHttp2BlockedBodyRead(t, ipVersion, true)
		waitForHttp2Barrier(t, "blocked socket write", blockedRead.gate.writeEntered)
		blockedRead.cancelRequest()

		select {
		case readErr := <-blockedRead.readDone:
			t.Fatalf("canceled body read returned before the blocked write was released: %v", readErr)
		default:
		}
		select {
		case <-blockedRead.gate.deadlineSeen:
			t.Fatal("zero WriteByteTimeout unexpectedly installed a socket write deadline")
		default:
		}

		if err := blockedRead.gate.Close(); err != nil {
			t.Fatal(err)
		}
		_ = waitForHttp2BodyRead(t, blockedRead.readDone)
		_ = blockedRead.response.Body.Close()
	})
}

// This is the pre-fix regression. With the production WriteByteTimeout left at
// zero, writeEntered closes but deadlineSeen never does. Once the configured
// deadline is observed, expireWrite deterministically models its firing and the
// canceled caller must unwind.
func TestHttpClientHttp2WriteTimeoutReleasesCanceledBodyRead(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		blockedRead := newHttp2BlockedBodyRead(t, ipVersion, false)
		waitForHttp2Barrier(t, "blocked socket write", blockedRead.gate.writeEntered)
		waitForHttp2Barrier(t, "socket write deadline", blockedRead.gate.deadlineSeen)
		blockedRead.cancelRequest()

		select {
		case readErr := <-blockedRead.readDone:
			t.Fatalf("body read returned before the write deadline expired: %v", readErr)
		default:
		}
		blockedRead.gate.expire()
		_ = waitForHttp2BodyRead(t, blockedRead.readDone)
		_ = blockedRead.response.Body.Close()
	})
}
