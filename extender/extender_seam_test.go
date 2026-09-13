// This file pins the extender's userspace listener, forward dial, error
// attribution, and connection ownership seams outside the performance suite.

package extender

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect"
)

// blockingListener lets ListenAndServe prove listener selection and shutdown
// without binding a host port.
type blockingListener struct {
	acceptOnce    sync.Once
	closeOnce     sync.Once
	acceptStarted chan struct{}
	closed        chan struct{}
}

// singleConnectionListener returns one in-memory connection, then blocks until
// the server releases the listener.
type singleConnectionListener struct {
	connection net.Conn
	acceptOnce sync.Once
	accepted   chan struct{}
	closed     chan struct{}
	closeOnce  sync.Once
}

// Builds a listener around one server-side in-memory connection.
func newSingleConnectionListener(connection net.Conn) *singleConnectionListener {
	return &singleConnectionListener{
		connection: connection,
		accepted:   make(chan struct{}),
		closed:     make(chan struct{}),
	}
}

// Returns the fixture connection once and then waits for listener shutdown.
func (self *singleConnectionListener) Accept() (net.Conn, error) {
	var connection net.Conn
	self.acceptOnce.Do(func() {
		connection = self.connection
		close(self.accepted)
	})
	if connection != nil {
		return connection, nil
	}
	<-self.closed
	return nil, net.ErrClosed
}

// Releases the blocked second Accept.
func (self *singleConnectionListener) Close() error {
	self.closeOnce.Do(func() {
		close(self.closed)
	})
	return nil
}

// Supplies a stable diagnostic address only; no host socket is bound.
func (self *singleConnectionListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 18443}
}

// Builds an unbound listener whose Accept waits for Close.
func newBlockingListener() *blockingListener {
	return &blockingListener{
		acceptStarted: make(chan struct{}),
		closed:        make(chan struct{}),
	}
}

// Waits until the server closes the injected listener.
func (self *blockingListener) Accept() (net.Conn, error) {
	self.acceptOnce.Do(func() {
		close(self.acceptStarted)
	})
	<-self.closed
	return nil, net.ErrClosed
}

// Records listener ownership release.
func (self *blockingListener) Close() error {
	self.closeOnce.Do(func() {
		close(self.closed)
	})
	return nil
}

// Supplies a stable diagnostic address only; no host socket is bound.
func (self *blockingListener) Addr() net.Addr {
	return &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 18443}
}

// closeObservedConn records the extender's release of a successful injected
// forward connection.
type closeObservedConn struct {
	net.Conn
	closeOnce sync.Once
	closed    chan struct{}
}

// Wraps a real stream and records its first close.
func newCloseObservedConn(conn net.Conn) *closeObservedConn {
	return &closeObservedConn{
		Conn:   conn,
		closed: make(chan struct{}),
	}
}

// Closing remains idempotent while notifying the owner test.
func (self *closeObservedConn) Close() error {
	var err error
	self.closeOnce.Do(func() {
		err = self.Conn.Close()
		close(self.closed)
	})
	return err
}

// extenderPipeFixture drives the real outer TLS and signed header over a
// net.Pipe, leaving only the forward dial behavior injectable.
type extenderPipeFixture struct {
	dial       connect.DialTlsContextFunction
	server     *ExtenderServer
	serverDone chan struct{}
	cancel     context.CancelFunc
}

// Connects the production extender client and server without a host listener.
func newExtenderPipeFixture(t *testing.T, settings *ExtenderSettings) *extenderPipeFixture {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	clientConn, serverConn := net.Pipe()
	server := NewExtenderServer(
		ctx,
		[]string{"seam-secret"},
		[]string{"target.test"},
		map[int][]connect.ExtenderConnectMode{},
		&net.Dialer{},
		settings,
	)
	serverDone := make(chan struct{})
	go func() {
		defer close(serverDone)
		server.HandleExtenderConnection(ctx, serverConn)
	}()

	connectSettings := connect.DefaultConnectSettings()
	connectSettings.TlsTimeout = 2 * time.Second
	connectSettings.TlsConfig = &tls.Config{
		InsecureSkipVerify: true, // the forward fixture never completes inner TLS
		MinVersion:         tls.VersionTLS12,
	}
	var dialOnce sync.Once
	connectSettings.DialContextSettings = &connect.DialContextSettings{
		DialContext: func(dialCtx context.Context, network string, address string) (net.Conn, error) {
			var connection net.Conn
			dialOnce.Do(func() {
				connection = clientConn
			})
			if connection == nil {
				return nil, errors.New("outer pipe already used")
			}
			return connection, nil
		},
	}
	dial := connect.NewExtenderDialTlsContext(
		connectSettings,
		&connect.ExtenderConfig{
			Profile: connect.ExtenderProfile{
				ConnectMode: connect.ExtenderConnectModeTcpTls,
				ServerName:  "front.test",
				Port:        18443,
			},
			Ip:     netip.MustParseAddr("192.0.2.44"),
			Secret: "seam-secret",
		},
	)
	fixture := &extenderPipeFixture{
		dial:       dial,
		server:     server,
		serverDone: serverDone,
		cancel:     cancel,
	}
	t.Cleanup(func() {
		fixture.server.Close()
		fixture.cancel()
		clientConn.Close()
		select {
		case <-fixture.serverDone:
		case <-time.After(3 * time.Second):
			t.Error("extender pipe handler did not stop")
		}
	})
	return fixture
}

// ListenAndServe must bind through the callback and close the returned
// listener when the extender is closed.
func TestExtenderListenSeamIsUsedAndOwned(t *testing.T) {
	listener := newBlockingListener()
	settings := DefaultExtenderSettings()
	listenCalls := make(chan string, 1)
	settings.Listen = func(network string, address string) (net.Listener, error) {
		listenCalls <- network + " " + address
		return listener, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := NewExtenderServer(
		ctx,
		nil,
		nil,
		map[int][]connect.ExtenderConnectMode{
			18443: {connect.ExtenderConnectModeTcpTls},
		},
		&net.Dialer{},
		settings,
	)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.ListenAndServe()
	}()

	select {
	case call := <-listenCalls:
		if call != "tcp :18443" {
			t.Fatalf("listen call = %q, expected tcp :18443", call)
		}
	case <-time.After(time.Second):
		t.Fatal("extender did not use the injected listener factory")
	}
	select {
	case <-listener.acceptStarted:
	case <-time.After(time.Second):
		t.Fatal("extender did not accept on the injected listener")
	}
	server.Close()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("extender did not stop after Close")
	}
	select {
	case <-listener.closed:
	default:
		t.Fatal("extender did not close its injected listener")
	}
}

// closeObservedPacketConn records the extender's release of an injected udp
// carrier endpoint.
type closeObservedPacketConn struct {
	net.PacketConn
	closeOnce sync.Once
	closed    chan struct{}
}

// Wraps a real endpoint and records its first close.
func newCloseObservedPacketConn(packetConn net.PacketConn) *closeObservedPacketConn {
	return &closeObservedPacketConn{
		PacketConn: packetConn,
		closed:     make(chan struct{}),
	}
}

// Closing remains idempotent while notifying the owner test.
func (self *closeObservedPacketConn) Close() error {
	var err error
	self.closeOnce.Do(func() {
		err = self.PacketConn.Close()
		close(self.closed)
	})
	return err
}

// ListenAndServe must bind a udp carrier through the callback and close the
// returned endpoint when the extender is closed.
func TestExtenderListenPacketSeamIsUsedAndOwned(t *testing.T) {
	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	observedPacketConn := newCloseObservedPacketConn(packetConn)
	port := packetConn.LocalAddr().(*net.UDPAddr).Port
	settings := DefaultExtenderSettings()
	listenPacketCalls := make(chan string, 1)
	settings.ListenPacket = func(network string, address string) (net.PacketConn, error) {
		listenPacketCalls <- network + " " + address
		return observedPacketConn, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := NewExtenderServer(
		ctx,
		nil,
		nil,
		map[int][]connect.ExtenderConnectMode{
			port: {connect.ExtenderConnectModeQuic},
		},
		&net.Dialer{},
		settings,
	)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.ListenAndServe()
	}()

	select {
	case call := <-listenPacketCalls:
		if call != fmt.Sprintf("udp :%d", port) {
			t.Fatalf("listen packet call = %q, expected udp :%d", call, port)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("extender did not use the injected packet listener factory")
	}
	server.CloseAndWait()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("extender did not stop after Close")
	}
	select {
	case <-observedPacketConn.closed:
	default:
		t.Fatal("extender did not close its injected packet endpoint")
	}
}

// A port that lists both udp carriers cannot be bound twice; the mistake is
// reported rather than silently dropping one carrier.
func TestExtenderRefusesTwoUdpCarriersOnOnePort(t *testing.T) {
	settings := DefaultExtenderSettings()
	settings.ListenPacket = func(network string, address string) (net.PacketConn, error) {
		return nil, errors.New("the port should not have been bound")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := NewExtenderServer(
		ctx,
		nil,
		nil,
		map[int][]connect.ExtenderConnectMode{
			18445: {connect.ExtenderConnectModeQuic, connect.ExtenderConnectModeDns},
		},
		&net.Dialer{},
		settings,
	)
	defer server.Close()

	err := server.ListenAndServe()
	if err == nil {
		t.Fatal("two udp carriers on one port were accepted")
	}
	if !strings.Contains(err.Error(), "more than one udp carrier") {
		t.Fatalf("error = %v, expected the carrier conflict", err)
	}
}

// CloseAndWait interrupts an accepted connection before its TLS handshake and
// joins the listener, handler, and serving operations deterministically.
func TestExtenderCloseAndWaitJoinsAcceptedConnection(t *testing.T) {
	clientConnection, serverConnection := net.Pipe()
	defer clientConnection.Close()
	listener := newSingleConnectionListener(serverConnection)
	settings := DefaultExtenderSettings()
	settings.Listen = func(string, string) (net.Listener, error) {
		return listener, nil
	}
	server := NewExtenderServer(
		context.Background(),
		nil,
		nil,
		map[int][]connect.ExtenderConnectMode{
			18443: {connect.ExtenderConnectModeTcpTls},
		},
		&net.Dialer{},
		settings,
	)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.ListenAndServe()
	}()

	select {
	case <-listener.accepted:
	case <-time.After(time.Second):
		t.Fatal("extender did not accept the fixture connection")
	}
	waitDone := make(chan struct{})
	go func() {
		server.CloseAndWait()
		close(waitDone)
	}()
	select {
	case <-waitDone:
	case <-time.After(time.Second):
		t.Fatal("CloseAndWait did not join every extender operation")
	}
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatal("CloseAndWait returned before ListenAndServe")
	}
	if _, err := clientConnection.Read(make([]byte, 1)); err == nil {
		t.Fatal("accepted connection remained open after CloseAndWait")
	}
}

// A bind failure releases the endpoint returned alongside it, disables only
// that carrier, and leaves the carriers that did bind serving (G2).
func TestExtenderListenSeamOwnsEveryPartialFactoryResult(t *testing.T) {
	firstListener := newBlockingListener()
	rejectedListener := newBlockingListener()
	sentinel := errors.New("injected listen failure")
	settings := DefaultExtenderSettings()
	var listenCallCount int
	settings.Listen = func(network string, address string) (net.Listener, error) {
		listenCallCount += 1
		if address == ":18443" {
			return firstListener, nil
		}
		return rejectedListener, sentinel
	}
	listenErrors := make(chan error, 4)
	settings.ListenErrorHandler = func(carrier string, err error) {
		if carrier != connect.ExtenderCarrierTcp {
			t.Errorf("listen error carrier = %q, expected tcp", carrier)
		}
		listenErrors <- err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := NewExtenderServer(
		ctx,
		nil,
		nil,
		map[int][]connect.ExtenderConnectMode{
			18443: {connect.ExtenderConnectModeTcpTls},
			18444: {connect.ExtenderConnectModeTcpTls},
		},
		&net.Dialer{},
		settings,
	)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.ListenAndServe()
	}()

	select {
	case err := <-listenErrors:
		if !errors.Is(err, sentinel) {
			t.Fatalf("listen error = %v, expected sentinel", err)
		}
	case <-time.After(time.Second):
		t.Fatal("the failed bind was not reported")
	}
	select {
	case <-rejectedListener.closed:
	default:
		t.Fatal("listen error did not release its returned listener")
	}
	<-server.Listening()
	if listenCallCount != 2 {
		t.Fatalf("listen calls = %d, expected two", listenCallCount)
	}
	// the port that did bind is still serving, and the carrier is still offered
	if carriers := server.Carriers(); !slices.Equal(carriers, []string{connect.ExtenderCarrierTcp}) {
		t.Fatalf("carriers = %v, expected tcp", carriers)
	}
	// and a carrier that is serving carries no standing failure, whichever
	// order its ports were bound in (G2)
	if listenErrs := server.ListenErrors(); len(listenErrs) != 0 {
		t.Fatalf("listen errors = %v, expected none for a serving carrier", listenErrs)
	}
	select {
	case <-firstListener.closed:
		t.Fatal("a failed bind released a listener that had bound")
	default:
	}

	server.Close()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("extender did not stop after Close")
	}
	select {
	case <-firstListener.closed:
	default:
		t.Fatal("extender did not close its bound listener")
	}
}

// An occupied tcp port disables only the tcp carrier: the udp carriers still
// serve, the failure is reported once, and Carriers omits tcp (G2).
func TestExtenderBindsEachCarrierIndependently(t *testing.T) {
	occupiedListener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	defer occupiedListener.Close()
	tcpPort := occupiedListener.Addr().(*net.TCPAddr).Port

	quicPacketConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dnsPacketConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	quicPort := quicPacketConn.LocalAddr().(*net.UDPAddr).Port
	dnsPort := dnsPacketConn.LocalAddr().(*net.UDPAddr).Port

	settings := DefaultExtenderSettings()
	settings.ListenPacket = func(network string, address string) (net.PacketConn, error) {
		switch address {
		case fmt.Sprintf(":%d", quicPort):
			return quicPacketConn, nil
		case fmt.Sprintf(":%d", dnsPort):
			return dnsPacketConn, nil
		default:
			return nil, fmt.Errorf("unexpected extender listen packet %s %s", network, address)
		}
	}
	listenErrors := make(chan string, 8)
	settings.ListenErrorHandler = func(carrier string, err error) {
		listenErrors <- carrier
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := NewExtenderServer(
		ctx,
		nil,
		nil,
		map[int][]connect.ExtenderConnectMode{
			tcpPort:  {connect.ExtenderConnectModeTcpTls},
			quicPort: {connect.ExtenderConnectModeQuic},
			dnsPort:  {connect.ExtenderConnectModeDns},
		},
		&net.Dialer{},
		settings,
	)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.ListenAndServe()
	}()

	<-server.Listening()
	expectedCarriers := []string{connect.ExtenderCarrierQuic, connect.ExtenderCarrierDns}
	if carriers := server.Carriers(); !slices.Equal(carriers, expectedCarriers) {
		t.Fatalf("carriers = %v, expected %v", carriers, expectedCarriers)
	}
	if carrier := <-listenErrors; carrier != connect.ExtenderCarrierTcp {
		t.Fatalf("listen error carrier = %q, expected tcp", carrier)
	}
	// the failure is kept beside the handler call, which is what a status read
	// long after the bind sees (G2, F3)
	serverListenErrors := server.ListenErrors()
	if len(serverListenErrors) != 1 {
		t.Fatalf("listen errors = %v, expected the tcp carrier only", serverListenErrors)
	}
	if serverListenErrors[connect.ExtenderCarrierTcp] == nil {
		t.Fatalf("listen errors = %v, expected a tcp entry", serverListenErrors)
	}
	select {
	case carrier := <-listenErrors:
		t.Fatalf("a second bind failure was reported for %q", carrier)
	default:
	}
	select {
	case err := <-serveDone:
		t.Fatalf("serving ended with the udp carriers up: %v", err)
	default:
	}

	server.CloseAndWait()
	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("extender did not stop after Close")
	}
}

// Every carrier failing to bind is the one case that ends serving with an
// error, and the error carries what each carrier reported (G2).
func TestExtenderFailsWhenNoCarrierBinds(t *testing.T) {
	tcpSentinel := errors.New("injected tcp listen failure")
	udpSentinel := errors.New("injected udp listen failure")
	settings := DefaultExtenderSettings()
	settings.Listen = func(network string, address string) (net.Listener, error) {
		return nil, tcpSentinel
	}
	settings.ListenPacket = func(network string, address string) (net.PacketConn, error) {
		return nil, udpSentinel
	}
	carriers := make(chan string, 8)
	settings.ListenErrorHandler = func(carrier string, err error) {
		carriers <- carrier
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := NewExtenderServer(
		ctx,
		nil,
		nil,
		map[int][]connect.ExtenderConnectMode{
			18443: {connect.ExtenderConnectModeTcpTls},
			18444: {connect.ExtenderConnectModeQuic},
			18445: {connect.ExtenderConnectModeDns},
		},
		&net.Dialer{},
		settings,
	)
	defer server.Close()

	err := server.ListenAndServe()
	if !errors.Is(err, tcpSentinel) || !errors.Is(err, udpSentinel) {
		t.Fatalf("listen error = %v, expected both sentinels", err)
	}
	if serverCarriers := server.Carriers(); len(serverCarriers) != 0 {
		t.Fatalf("carriers = %v, expected none", serverCarriers)
	}
	reported := map[string]bool{}
	for range 3 {
		select {
		case carrier := <-carriers:
			reported[carrier] = true
		default:
			t.Fatal("a carrier bind failure was not reported")
		}
	}
	for _, carrier := range []string{
		connect.ExtenderCarrierTcp,
		connect.ExtenderCarrierQuic,
		connect.ExtenderCarrierDns,
	} {
		if !reported[carrier] {
			t.Fatalf("the %s bind failure was not reported", carrier)
		}
	}
}

// A successful injected forward dial is addressed exactly and remains owned
// by the extender until either relay direction ends.
func TestExtenderForwardDialSeamIsUsedAndOwned(t *testing.T) {
	settings := DefaultExtenderSettings()
	forwardAddresses := make(chan string, 1)
	forwardClient, forwardPeer := net.Pipe()
	observedForward := newCloseObservedConn(forwardClient)
	settings.DialContext = func(ctx context.Context, network string, address string) (net.Conn, error) {
		forwardAddresses <- network + " " + address
		return observedForward, nil
	}
	fixture := newExtenderPipeFixture(t, settings)

	dialDone := make(chan error, 1)
	go func() {
		connection, err := fixture.dial(context.Background(), "tcp", "target.test:443")
		if connection != nil {
			connection.Close()
		}
		dialDone <- err
	}()
	select {
	case call := <-forwardAddresses:
		if call != "tcp target.test:443" {
			t.Fatalf("forward dial = %q, expected tcp target.test:443", call)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("extender did not use the injected forward dial")
	}
	if err := forwardPeer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-observedForward.closed:
	case <-time.After(3 * time.Second):
		t.Fatal("extender did not close the injected forward connection")
	}
	select {
	case err := <-dialDone:
		if err == nil {
			t.Fatal("incomplete inner TLS unexpectedly succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("extender client did not observe forward closure")
	}
}

// Connection-stage errors are surfaced only through the optional observer;
// the default remains silent and still returns the client-visible failure.
func TestExtenderErrorHandlerAttributesForwardDialFailure(t *testing.T) {
	sentinel := errors.New("injected forward failure")
	type observedError struct {
		stage string
		err   error
	}
	errorsObserved := make(chan observedError, 1)
	settings := DefaultExtenderSettings()
	settings.DialContext = func(ctx context.Context, network string, address string) (net.Conn, error) {
		return nil, sentinel
	}
	settings.ErrorHandler = func(stage string, err error) {
		errorsObserved <- observedError{stage: stage, err: err}
	}
	fixture := newExtenderPipeFixture(t, settings)

	_, err := fixture.dial(context.Background(), "tcp", "target.test:443")
	if err == nil {
		t.Fatal("failed forward dial unexpectedly established inner TLS")
	}
	select {
	case observed := <-errorsObserved:
		if observed.stage != "forward dial" {
			t.Fatalf("error stage = %q, expected forward dial", observed.stage)
		}
		if !errors.Is(observed.err, sentinel) {
			t.Fatalf("observed error = %v, expected sentinel", observed.err)
		}
	case <-time.After(time.Second):
		t.Fatal("forward dial failure was not attributed")
	}
}

// A failed forward callback can still return a connection. The extender owns
// and releases that connection before surfacing the failure.
func TestExtenderForwardDialSeamOwnsConnectionReturnedWithError(t *testing.T) {
	sentinel := errors.New("injected forward failure")
	forwardClient, forwardPeer := net.Pipe()
	defer forwardPeer.Close()
	observedForward := newCloseObservedConn(forwardClient)
	settings := DefaultExtenderSettings()
	settings.DialContext = func(context.Context, string, string) (net.Conn, error) {
		return observedForward, sentinel
	}
	fixture := newExtenderPipeFixture(t, settings)

	_, err := fixture.dial(context.Background(), "tcp", "target.test:443")
	if err == nil {
		t.Fatal("failed forward dial unexpectedly established inner TLS")
	}
	select {
	case <-observedForward.closed:
	case <-time.After(time.Second):
		t.Fatal("forward dial error did not release its returned connection")
	}
}

// Production defaults leave every test/measurement callback disabled, and the
// service handlers unset, which refuses the reserved services (A8).
func TestExtenderSeamDefaultsAreDisabled(t *testing.T) {
	settings := DefaultExtenderSettings()
	if settings.Listen != nil || settings.ListenPacket != nil ||
		settings.DialContext != nil || settings.ErrorHandler != nil {
		t.Fatal("default extender settings unexpectedly enable a test seam")
	}
	if settings.GossipConnHandler != nil || settings.FeedConnHandler != nil {
		t.Fatal("default extender settings unexpectedly enable a reserved service")
	}
	if settings.IdentityKeySeed != nil {
		t.Fatal("default extender settings unexpectedly carry an identity key")
	}
}
