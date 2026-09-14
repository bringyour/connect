// This file pins the H3 packet-connection factory's selection and ownership
// independently of the userspace performance simulator.

package connect

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
)

// closeObservedPacketConn exposes ownership without changing socket behavior.
type closeObservedPacketConn struct {
	net.PacketConn
	closeOnce sync.Once
	closed    chan struct{}
}

// Wraps a real packet endpoint and records its first close.
func newCloseObservedPacketConn(packetConn net.PacketConn) *closeObservedPacketConn {
	return &closeObservedPacketConn{
		PacketConn: packetConn,
		closed:     make(chan struct{}),
	}
}

// Closing remains idempotent while notifying the test observer.
func (self *closeObservedPacketConn) Close() error {
	var err error
	self.closeOnce.Do(func() {
		err = self.PacketConn.Close()
		close(self.closed)
	})
	return err
}

// A failed injected H3 dial must close the endpoint it received. This covers
// the error path before a ConnStream exists, where ownership is easiest to
// lose during refactoring.
func TestPlatformTransportH3PacketConnFactoryOwnsFailureEndpoint(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		unusedEndpoint, err := net.ListenPacket(testUdpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
		if err != nil {
			t.Fatal(err)
		}
		unusedPort := unusedEndpoint.LocalAddr().(*net.UDPAddr).Port
		if err := unusedEndpoint.Close(); err != nil {
			t.Fatal(err)
		}

		packetConn, err := net.ListenPacket(testUdpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
		if err != nil {
			t.Fatal(err)
		}
		observedPacketConn := newCloseObservedPacketConn(packetConn)
		var factoryCallCount atomic.Int32
		factoryCtxs := make(chan context.Context, 1)
		settings := testingPlatformTransportSettings()
		settings.H3Port = unusedPort
		settings.resolveH3AddrsForTest = testingH3LoopbackResolver(ipVersion, settings.H3Port)
		settings.QuicConnectTimeout = 50 * time.Millisecond
		settings.QuicHandshakeTimeout = 50 * time.Millisecond
		settings.ReconnectTimeout = 5 * time.Second
		settings.H3PacketConnFactory = func(factoryCtx context.Context) (net.PacketConn, error) {
			if factoryCallCount.Add(1) != 1 {
				return nil, errors.New("unexpected repeated packet factory call")
			}
			factoryCtxs <- factoryCtx
			return observedPacketConn, nil
		}

		ctx, cancel := context.WithCancel(context.Background())
		transport := NewPlatformTransportWithTargetMode(
			ctx,
			NewClientStrategyWithDefaults(ctx),
			NewRouteManager(ctx, "test"),
			"https://"+testLoopbackHost(ipVersion),
			&ClientAuth{
				ByJwt:      "testing",
				InstanceId: NewId(),
				AppVersion: "testing",
			},
			TransportModeH3,
			settings,
		)
		defer func() {
			transport.Close()
			cancel()
		}()

		var factoryCtx context.Context
		select {
		case factoryCtx = <-factoryCtxs:
		case <-time.After(time.Second):
			t.Fatal("H3 did not use the packet connection factory")
		}
		select {
		case <-observedPacketConn.closed:
		case <-time.After(3 * time.Second):
			t.Fatal("failed H3 dial did not close its injected packet endpoint")
		}
		transport.Close()
		select {
		case <-factoryCtx.Done():
		case <-time.After(time.Second):
			t.Fatal("factory context outlived the platform transport")
		}
		if got := factoryCallCount.Load(); got != 1 {
			t.Fatalf("packet factory called %d times, expected one", got)
		}
	})
}

// An endpoint returned alongside an error still transfers ownership to the
// platform transport and must be released before the dial attempt is rejected.
func TestPlatformTransportH3PacketConnFactoryOwnsEndpointOnFactoryError(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		packetConn, err := net.ListenPacket(testUdpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
		if err != nil {
			t.Fatal(err)
		}
		observedPacketConn := newCloseObservedPacketConn(packetConn)
		factoryErr := errors.New("injected packet factory error")
		var factoryCallCount atomic.Int32
		settings := testingPlatformTransportSettings()
		settings.ReconnectTimeout = 5 * time.Second
		settings.resolveH3AddrsForTest = testingH3LoopbackResolver(ipVersion, settings.H3Port)
		settings.H3PacketConnFactory = func(context.Context) (net.PacketConn, error) {
			if factoryCallCount.Add(1) != 1 {
				return nil, errors.New("unexpected repeated packet factory call")
			}
			return observedPacketConn, factoryErr
		}

		ctx, cancel := context.WithCancel(context.Background())
		transport := NewPlatformTransportWithTargetMode(
			ctx,
			NewClientStrategyWithDefaults(ctx),
			NewRouteManager(ctx, "test"),
			"https://"+testLoopbackHost(ipVersion),
			&ClientAuth{
				ByJwt:      "testing",
				InstanceId: NewId(),
				AppVersion: "testing",
			},
			TransportModeH3,
			settings,
		)
		defer func() {
			transport.Close()
			cancel()
		}()

		select {
		case <-observedPacketConn.closed:
		case <-time.After(time.Second):
			t.Fatal("factory error did not release its returned packet endpoint")
		}
		if got := factoryCallCount.Load(); got != 1 {
			t.Fatalf("packet factory called %d times, expected one", got)
		}
	})
}

// A successful H3 route retains the injected endpoint for the connection
// lifetime and releases it when PlatformTransport.Close tears the route down.
// Its pooled authentication frame is handshake-scoped and must already be
// returned while the connected route remains active.
func TestPlatformTransportH3PacketConnFactoryOwnsConnectedEndpoint(t *testing.T) {
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
		const nextProto = "urnetwork-h3-seam"
		listener, err := quic.ListenAddrEarly(
			testLoopbackHostPort(ipVersion, 0),
			&tls.Config{
				Certificates: []tls.Certificate{cert},
				NextProtos:   []string{nextProto},
			},
			&quic.Config{},
		)
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		serverCtx, serverCancel := context.WithCancel(context.Background())
		defer serverCancel()
		acceptedConnections := make(chan *quic.Conn, 1)
		serverErrors := make(chan error, 1)
		framerSettings := DefaultFramerSettings(int(DefaultClientSettings().MinimumMessageLenLimit()))
		go func() {
			connection, acceptErr := listener.Accept(serverCtx)
			if acceptErr != nil {
				serverErrors <- acceptErr
				return
			}
			acceptedConnections <- connection
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
			<-connection.Context().Done()
		}()

		packetConn, err := net.ListenPacket(testUdpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
		if err != nil {
			t.Fatal(err)
		}
		observedPacketConn := newCloseObservedPacketConn(packetConn)
		var factoryCallCount atomic.Int32
		settings := testingPlatformTransportSettings()
		settings.H3Port = listener.Addr().(*net.UDPAddr).Port
		settings.resolveH3AddrsForTest = testingH3LoopbackResolver(ipVersion, settings.H3Port)
		settings.QuicTlsConfig = &tls.Config{
			InsecureSkipVerify: true, // test-only self-signed endpoint
			NextProtos:         []string{nextProto},
		}
		settings.FramerSettings = framerSettings
		authWitnesses := make(chan []byte, 1)
		settings.AuthFrameObserver = func(authFrameBytes []byte) {
			witness := MessagePoolShareReadOnly(authFrameBytes)
			select {
			case authWitnesses <- witness:
			default:
				MessagePoolReturn(witness)
			}
		}
		settings.H3PacketConnFactory = func(factoryCtx context.Context) (net.PacketConn, error) {
			factoryCallCount.Add(1)
			return observedPacketConn, nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		transport := NewPlatformTransportWithTargetMode(
			ctx,
			NewClientStrategyWithDefaults(ctx),
			NewRouteManager(ctx, "test"),
			"https://"+testLoopbackHost(ipVersion),
			&ClientAuth{
				ByJwt:      "testing",
				InstanceId: NewId(),
				AppVersion: "testing",
			},
			TransportModeH3,
			settings,
		)
		defer func() {
			transport.Close()
			cancel()
		}()

		var accepted *quic.Conn
		select {
		case accepted = <-acceptedConnections:
		case serverErr := <-serverErrors:
			t.Fatal(serverErr)
		case <-time.After(5 * time.Second):
			t.Fatal("QUIC server did not accept the injected endpoint")
		}
		if !testingWaitForActiveMode(transport, TransportModeH3, 5*time.Second) {
			t.Fatal("injected H3 transport was never elected")
		}
		var authWitness []byte
		select {
		case authWitness = <-authWitnesses:
		case <-time.After(time.Second):
			t.Fatal("connected H3 transport did not expose its auth-frame ownership")
		}
		if !MessagePoolReturn(authWitness) {
			t.Fatal("connected H3 authentication owner outlived route activation")
		}
		select {
		case <-observedPacketConn.closed:
			t.Fatal("connected H3 route released its packet endpoint too early")
		default:
		}
		transport.Close()
		select {
		case <-observedPacketConn.closed:
		case <-time.After(time.Second):
			t.Fatal("connected H3 close did not release its injected packet endpoint")
		}
		select {
		case <-accepted.Context().Done():
		case <-time.After(time.Second):
			t.Fatal("connected H3 close did not close its QUIC connection")
		}
		if got := factoryCallCount.Load(); got != 1 {
			t.Fatalf("packet factory called %d times, expected one", got)
		}
	})
}

// The packet factory belongs only to plain H3. An H1 transport retains its
// existing TCP/WebSocket path even when the optional callback is populated.
func TestPlatformTransportH1IgnoresH3PacketConnFactory(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingPlatformServerIpVersion(t, ipVersion)
		settings := testingPlatformTransportSettings()
		var factoryCallCount atomic.Int32
		settings.H3PacketConnFactory = func(factoryCtx context.Context) (net.PacketConn, error) {
			factoryCallCount.Add(1)
			return nil, errors.New("H1 unexpectedly requested an H3 packet endpoint")
		}
		transport := testingPlatformTransport(t, ctx, platform.url, settings)
		if !testingWaitForActiveMode(transport, TransportModeH1, 5*time.Second) {
			t.Fatal("H1 transport did not connect")
		}
		if got := factoryCallCount.Load(); got != 0 {
			t.Fatalf("H1 called the H3 packet factory %d times", got)
		}
	})
}
