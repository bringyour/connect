// Shared fixtures for the carrier tests: one extender with all three carriers
// bound on one loopback family through the listener seams, and an in-process
// destination whose addresses are resolved by the forward dial seam, so a
// family narrowing (A7) is observable without any name service.

package extender

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"sync"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"github.com/urnetwork/connect"
)

// The caller address the fixture site reports from /hello.
const testHelloClientAddress = "198.51.100.7"

// Synthetic encoding tld of the dns carrier in tests.
const testDnsTld = "x.example."

// Synthetic fronted name the client presents as the outer sni. It is not on
// the whitelist, so a plain request that carries it is refused.
const testServerName = "front.example"

// Synthetic spoof name, installed in place of the bundled list (A10). It is on
// the whitelist and is never a valid extender destination (A5).
const testSpoofName = "spoof.example"

// Secret of the private extender under test.
const testSecret = "fixture-secret"

// destination is one in-process https server reachable on both loopback
// families, each on its own listener, so a test can offer a name only one
// family can reach.
type destination struct {
	certificate     *tls.Certificate
	rootCAs         *x509.CertPool
	familyAddresses map[string]string
	requestCount    atomicCount
	// held, when set, blocks every /hold request until it is closed, which is
	// how a test holds a proxied exchange open or stalls one mid body
	held chan struct{}
}

// atomicCount counts handled destination requests without a lock.
type atomicCount struct {
	stateLock sync.Mutex
	count     int
}

func (self *atomicCount) add() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.count += 1
}

func (self *atomicCount) get() int {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.count
}

// Runs the destination on 127.0.0.1 and on ::1, returning the addresses keyed
// by dial network.
func newDestination(t *testing.T) *destination {
	t.Helper()
	certificate, err := selfSignedCertificate(
		[]string{"dest.example", "dest4.example", "dest6.example", testSpoofName},
		"Extender Test",
		time.Hour,
		24*time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	rootCAs := x509.NewCertPool()
	rootCAs.AddCert(certificate.Leaf)

	dest := &destination{
		certificate:     certificate,
		rootCAs:         rootCAs,
		familyAddresses: map[string]string{},
		held:            make(chan struct{}),
	}
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			dest.requestCount.add()
			switch req.URL.Path {
			case "/hello":
				// the shape the forward probe reads, alongside the host echo
				// every other route answers with
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(
					`{"host":"` + req.Host + `","client_address":"` + testHelloClientAddress + `"}`,
				))
			case "/bytes":
				byteCount, err := strconv.Atoi(req.URL.Query().Get("n"))
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Write(bytes.Repeat([]byte("x"), byteCount))
			case "/hold":
				// headers and a first byte arrive at once; the rest never does
				w.Header().Set("Content-Type", "application/octet-stream")
				w.Write([]byte("x"))
				if flusher, ok := w.(http.Flusher); ok {
					flusher.Flush()
				}
				select {
				case <-dest.held:
				case <-req.Context().Done():
				}
			default:
				w.Header().Set("Content-Type", "application/json")
				w.Write([]byte(`{"host":"` + req.Host + `"}`))
			}
		}),
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{*certificate},
		},
	}
	for _, family := range []struct {
		network string
		address string
	}{
		{network: "tcp4", address: "127.0.0.1:0"},
		{network: "tcp6", address: "[::1]:0"},
	} {
		listener, err := net.Listen(family.network, family.address)
		if err != nil {
			t.Fatalf("destination listen %s: %v", family.network, err)
		}
		dest.familyAddresses[family.network] = listener.Addr().String()
		done := make(chan error, 1)
		go func() {
			done <- server.ServeTLS(listener, "", "")
		}()
		t.Cleanup(func() {
			listener.Close()
			select {
			case err := <-done:
				if err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
					t.Errorf("destination server: %v", err)
				}
			case <-time.After(3 * time.Second):
				t.Error("destination server did not stop")
			}
		})
	}
	t.Cleanup(func() {
		server.Close()
	})
	return dest
}

// The family addresses of one destination name. The names separate what each
// family can reach, which is what the forward dial narrowing is checked
// against.
func (self *destination) addressesForHost(host string) map[string]string {
	switch host {
	case "dest4.example":
		return map[string]string{"tcp4": self.familyAddresses["tcp4"]}
	case "dest6.example":
		return map[string]string{"tcp6": self.familyAddresses["tcp6"]}
	case "dest.example", testSpoofName:
		return self.familyAddresses
	default:
		return map[string]string{}
	}
}

// extenderFixture binds all three carriers of one extender on one loopback
// address and records what the forward dial is asked for.
type extenderFixture struct {
	t               *testing.T
	server          *ExtenderServer
	settings        *ExtenderSettings
	destination     *destination
	ip              netip.Addr
	tcpPort         int
	quicPort        int
	dnsPort         int
	forwardNetworks chan string
	// the outer sni of every handshake the extender terminated, so a test can
	// prove what a dial presented -- an empty entry is a ClientHello with no
	// sni at all (A10)
	serverNames chan string
	errors      chan error
	serveDone   chan error
}

// Builds the private extender of the carrier tests, which requires the fixture
// secret on every header.
func newExtenderFixture(
	t *testing.T,
	loopbackIp string,
	configure func(settings *ExtenderSettings),
) *extenderFixture {
	t.Helper()
	return newExtenderFixtureWithSecrets(t, loopbackIp, []string{testSecret}, configure)
}

// Builds the extender on the given loopback address. An empty secret list is
// an open extender, which is what an operator activated extender is (A4).
// configure runs before the server is constructed, so a test can change
// limits, the identity key or the service handlers.
func newExtenderFixtureWithSecrets(
	t *testing.T,
	loopbackIp string,
	allowedSecrets []string,
	configure func(settings *ExtenderSettings),
) *extenderFixture {
	t.Helper()
	ip, err := netip.ParseAddr(loopbackIp)
	if err != nil {
		t.Fatal(err)
	}
	dest := newDestination(t)

	tcpListener, err := net.Listen("tcp", net.JoinHostPort(loopbackIp, "0"))
	if err != nil {
		t.Fatal(err)
	}
	quicPacketConn, err := net.ListenPacket("udp", net.JoinHostPort(loopbackIp, "0"))
	if err != nil {
		t.Fatal(err)
	}
	dnsPacketConn, err := net.ListenPacket("udp", net.JoinHostPort(loopbackIp, "0"))
	if err != nil {
		t.Fatal(err)
	}
	fixture := &extenderFixture{
		t:               t,
		destination:     dest,
		ip:              ip,
		tcpPort:         tcpListener.Addr().(*net.TCPAddr).Port,
		quicPort:        quicPacketConn.LocalAddr().(*net.UDPAddr).Port,
		dnsPort:         dnsPacketConn.LocalAddr().(*net.UDPAddr).Port,
		forwardNetworks: make(chan string, 64),
		serverNames:     make(chan string, 64),
		errors:          make(chan error, 64),
	}

	settings := DefaultExtenderSettings()
	settings.DnsTlds = []string{testDnsTld}
	settings.HeaderTimeout = 5 * time.Second
	// the whitelist is the synthetic spoof list plus the operator patterns, and
	// the reverse proxy verifies the fixture site normally (A5)
	settings.SpoofDomains = []string{testSpoofName}
	settings.ProxyTlsConfig = &tls.Config{
		RootCAs: dest.rootCAs,
	}
	settings.Listen = func(network string, address string) (net.Listener, error) {
		if address != fmt.Sprintf(":%d", fixture.tcpPort) {
			return nil, fmt.Errorf("unexpected extender listen %s %s", network, address)
		}
		return tcpListener, nil
	}
	settings.ListenPacket = func(network string, address string) (net.PacketConn, error) {
		switch address {
		case fmt.Sprintf(":%d", fixture.quicPort):
			return quicPacketConn, nil
		case fmt.Sprintf(":%d", fixture.dnsPort):
			return dnsPacketConn, nil
		default:
			return nil, fmt.Errorf("unexpected extender listen packet %s %s", network, address)
		}
	}
	settings.DialContext = func(ctx context.Context, network string, address string) (net.Conn, error) {
		select {
		case fixture.forwardNetworks <- network:
		default:
		}
		host, _, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		familyAddress, ok := dest.addressesForHost(host)[network]
		if !ok {
			return nil, fmt.Errorf("no %s address for %s", network, host)
		}
		return (&net.Dialer{}).DialContext(ctx, network, familyAddress)
	}
	settings.ErrorHandler = func(stage string, err error) {
		select {
		case fixture.errors <- fmt.Errorf("%s: %w", stage, err):
		default:
		}
	}
	settings.CertificateHandler = func(serverName string) {
		select {
		case fixture.serverNames <- serverName:
		default:
		}
	}
	if configure != nil {
		configure(settings)
	}
	fixture.settings = settings

	ctx, cancel := context.WithCancel(context.Background())
	fixture.server = NewExtenderServer(
		ctx,
		allowedSecrets,
		[]string{"dest.example", "dest4.example", "dest6.example"},
		map[int][]connect.ExtenderConnectMode{
			fixture.tcpPort:  {connect.ExtenderConnectModeTcpTls},
			fixture.quicPort: {connect.ExtenderConnectModeQuic},
			fixture.dnsPort:  {connect.ExtenderConnectModeDns},
		},
		&net.Dialer{},
		settings,
	)
	fixture.serveDone = make(chan error, 1)
	go func() {
		fixture.serveDone <- fixture.server.ListenAndServe()
	}()
	t.Cleanup(func() {
		fixture.server.CloseAndWait()
		cancel()
		select {
		case err := <-fixture.serveDone:
			if err != nil {
				t.Errorf("extender server: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("extender server did not stop")
		}
	})
	return fixture
}

// The client profile of one carrier on this extender.
func (self *extenderFixture) profile(carrier string) connect.ExtenderProfile {
	switch carrier {
	case connect.ExtenderCarrierQuic:
		return connect.ExtenderProfile{
			ConnectMode: connect.ExtenderConnectModeQuic,
			ServerName:  testServerName,
			Port:        self.quicPort,
		}
	case connect.ExtenderCarrierDns:
		return connect.ExtenderProfile{
			ConnectMode: connect.ExtenderConnectModeDns,
			ServerName:  testServerName,
			Port:        self.dnsPort,
			DnsTld:      testDnsTld,
		}
	default:
		return connect.ExtenderProfile{
			ConnectMode: connect.ExtenderConnectModeTcpTls,
			ServerName:  testServerName,
			Port:        self.tcpPort,
		}
	}
}

func (self *extenderFixture) extenderConfig(carrier string) *connect.ExtenderConfig {
	return &connect.ExtenderConfig{
		Profile: self.profile(carrier),
		Ip:      self.ip,
		Secret:  testSecret,
	}
}

// Client settings that trust the destination certificate and nothing else.
func (self *extenderFixture) connectSettings() *connect.ConnectSettings {
	connectSettings := connect.DefaultConnectSettings()
	connectSettings.TlsConfig = &tls.Config{
		RootCAs: self.destination.rootCAs,
	}
	connectSettings.ConnectTimeout = 10 * time.Second
	connectSettings.TlsTimeout = 10 * time.Second
	connectSettings.RequestTimeout = 20 * time.Second
	return connectSettings
}

// The forward dial network the extender was last asked for.
func (self *extenderFixture) nextForwardNetwork() (string, error) {
	select {
	case network := <-self.forwardNetworks:
		return network, nil
	case <-time.After(10 * time.Second):
		return "", errors.New("the extender did not dial forward")
	}
}

// Every forward dial network the extender has been asked for so far. The
// upstream pools are reused, so a test that makes several requests observes
// only the dials that actually happened.
func (self *extenderFixture) forwardNetworksSeen() []string {
	networks := []string{}
	for {
		select {
		case network := <-self.forwardNetworks:
			networks = append(networks, network)
		default:
			return networks
		}
	}
}

// The outer sni of the next handshake the extender terminated.
func (self *extenderFixture) nextServerName() (string, bool) {
	select {
	case serverName := <-self.serverNames:
		return serverName, true
	case <-time.After(10 * time.Second):
		return "", false
	}
}

// The next attributed connection failure, for tests that assert a refusal.
func (self *extenderFixture) nextError() (error, bool) {
	select {
	case err := <-self.errors:
		return err, true
	case <-time.After(10 * time.Second):
		return nil, false
	}
}

// The extender address as the client dials it for one carrier.
func (self *extenderFixture) authority(port int) string {
	return net.JoinHostPort(self.ip.String(), strconv.Itoa(port))
}

// A raw client on the tcp carrier that presents serverName as the outer sni,
// which is what the whitelist is checked against. nextProtos selects the outer
// alpn, so the same helper drives http/1.1 and h2.
func newRawExtenderHttpClientWithServerName(
	fixture *extenderFixture,
	serverName string,
	nextProtos []string,
) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialTLSContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", fixture.authority(fixture.tcpPort))
				if err != nil {
					return nil, err
				}
				tlsConn := tls.Client(conn, &tls.Config{
					ServerName:         serverName,
					InsecureSkipVerify: true,
					MinVersion:         tls.VersionTLS13,
					NextProtos:         nextProtos,
				})
				if err := tlsConn.HandshakeContext(ctx); err != nil {
					conn.Close()
					return nil, err
				}
				return tlsConn, nil
			},
			ForceAttemptHTTP2: true,
		},
		Timeout: 20 * time.Second,
	}
}

// The same on the quic carrier, where the sni reaches the handler on the
// request's own connection state.
func newRawExtenderH3Transport(fixture *extenderFixture, serverName string) *http3.Transport {
	return &http3.Transport{
		TLSClientConfig: &tls.Config{
			ServerName:         serverName,
			InsecureSkipVerify: true,
			NextProtos:         []string{http3.NextProtoH3},
		},
		Dial: func(
			ctx context.Context,
			addr string,
			tlsConfig *tls.Config,
			quicConfig *quic.Config,
		) (*quic.Conn, error) {
			return quic.DialAddr(ctx, fixture.authority(fixture.quicPort), tlsConfig, quicConfig)
		},
	}
}
