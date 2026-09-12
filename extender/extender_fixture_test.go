// Shared fixtures for the carrier tests: one extender with all three carriers
// bound on one loopback family through the listener seams, and an in-process
// destination whose addresses are resolved by the forward dial seam, so a
// family narrowing (A7) is observable without any name service.

package extender

import (
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

	"github.com/urnetwork/connect"
)

// Synthetic encoding tld of the dns carrier in tests.
const testDnsTld = "x.example."

// Synthetic fronted name the client presents as the outer sni.
const testServerName = "front.example"

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
		[]string{"dest.example", "dest4.example", "dest6.example"},
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
	}
	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			dest.requestCount.add()
			w.Header().Set("Content-Type", "application/json")
			w.Write([]byte(`{"host":"` + req.Host + `"}`))
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
	case "dest.example":
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
	errors          chan error
	serveDone       chan error
}

// Builds the extender on the given loopback address. configure runs before the
// server is constructed, so a test can change limits, the identity key or the
// service handlers.
func newExtenderFixture(
	t *testing.T,
	loopbackIp string,
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
		errors:          make(chan error, 64),
	}

	settings := DefaultExtenderSettings()
	settings.DnsTlds = []string{testDnsTld}
	settings.HeaderTimeout = 5 * time.Second
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
	if configure != nil {
		configure(settings)
	}
	fixture.settings = settings

	ctx, cancel := context.WithCancel(context.Background())
	fixture.server = NewExtenderServer(
		ctx,
		[]string{testSecret},
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
