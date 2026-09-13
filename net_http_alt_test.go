package connect

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"sync"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"
)

// The api's two alt dialers (EXTENDER.md L4). Both reach an in-process alt on
// loopback: the request names the api host, the alt url decides where the
// packets go, and the fixture roots verify the certificate exactly as the
// pinned roots do in production.

const (
	testAltApiHost  = "api.space.example"
	testAltDnsTld   = "alt.example."
	testAltBodyText = "alt-hello"
)

// One in-process alt: an http3 server on loopback, over the decode53
// translation when whodis is set, the roots that verify it, and what every
// client hello it saw offered.
type testAltServer struct {
	altUrl  string
	rootCAs *x509.CertPool

	stateLock   sync.Mutex
	serverNames []string
	alpns       [][]string
}

// Records one client hello, which is where the sni and the offered alpn are
// observable exactly as alt sees them (L1).
func (self *testAltServer) noteClientHello(clientHello *tls.ClientHelloInfo) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.serverNames = append(self.serverNames, clientHello.ServerName)
	self.alpns = append(self.alpns, slices.Clone(clientHello.SupportedProtos))
}

// The sni and the alpn of the one client hello this server saw.
func (self *testAltServer) clientHello(t *testing.T) (string, []string) {
	t.Helper()
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if len(self.serverNames) != 1 {
		t.Fatalf("client hellos = %d, expected one", len(self.serverNames))
	}
	return self.serverNames[0], self.alpns[0]
}

func newTestAltServer(t *testing.T, whodis bool) *testAltServer {
	t.Helper()
	certPem, keyPem, err := selfSign([]string{testAltApiHost}, "alt-test", 1*time.Hour, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(certPem, keyPem)
	if err != nil {
		t.Fatal(err)
	}
	rootCAs := x509.NewCertPool()
	if !rootCAs.AppendCertsFromPEM(certPem) {
		t.Fatal("the fixture certificate is not a usable root")
	}
	altServer := &testAltServer{rootCAs: rootCAs}

	packetConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := packetConn.LocalAddr().(*net.UDPAddr).Port

	ctx, cancel := context.WithCancel(context.Background())
	carrierPacketConn := packetConn
	if whodis {
		ptSettings := DefaultPacketTranslationSettings()
		ptSettings.DnsTlds = [][]byte{[]byte(testAltDnsTld)}
		translation, err := NewPacketTranslation(
			ctx, PacketTranslationModeDecode53, packetConn, ptSettings)
		if err != nil {
			cancel()
			packetConn.Close()
			t.Fatal(err)
		}
		carrierPacketConn = translation
	}

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Host != testAltApiHost {
			w.WriteHeader(http.StatusMisdirectedRequest)
			return
		}
		w.Write([]byte(testAltBodyText))
	})
	h3Server := &http3.Server{
		Handler:     handler,
		IdleTimeout: 30 * time.Second,
	}
	quicTransport := &quic.Transport{Conn: carrierPacketConn}
	listener, err := quicTransport.Listen(
		&tls.Config{
			Certificates: []tls.Certificate{cert},
			// alt advertises only h3 and negotiates per client hello (L1)
			NextProtos: []string{http3.NextProtoH3},
			GetConfigForClient: func(clientHello *tls.ClientHelloInfo) (*tls.Config, error) {
				altServer.noteClientHello(clientHello)
				return nil, nil
			},
		},
		&quic.Config{MaxIdleTimeout: 30 * time.Second},
	)
	if err != nil {
		cancel()
		quicTransport.Close()
		carrierPacketConn.Close()
		t.Fatal(err)
	}
	go func() {
		for {
			quicConn, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			go func() {
				defer quicConn.CloseWithError(0, "")
				h3Server.ServeQUICConn(quicConn)
			}()
		}
	}()
	t.Cleanup(func() {
		cancel()
		listener.Close()
		quicTransport.Close()
		carrierPacketConn.Close()
	})

	altServer.altUrl = fmt.Sprintf("https://%s", net.JoinHostPort("127.0.0.1", fmt.Sprintf("%d", port)))
	return altServer
}

// One strategy with the alt dialers and nothing else, so a request has exactly
// one path to take.
func newTestAltStrategy(t *testing.T, altServer *testAltServer) *ClientStrategy {
	t.Helper()
	settings := DefaultClientStrategySettings()
	settings.EnableNormal = false
	settings.EnableResilient = false
	settings.ExpandExtenderProfileCount = 0
	settings.AltUrl = altServer.altUrl
	settings.DnsTlds = [][]byte{[]byte(testAltDnsTld)}
	settings.ConnectSettings.TlsConfig = &tls.Config{
		RootCAs:    altServer.rootCAs,
		MinVersion: tls.VersionTLS13,
	}
	ctx, cancel := context.WithCancel(context.Background())
	clientStrategy := NewClientStrategy(ctx, settings)
	t.Cleanup(func() {
		clientStrategy.Close()
		cancel()
	})
	return clientStrategy
}

// The dialer of one description, which is how a test addresses one alt carrier
// without racing the other.
func testAltDialer(t *testing.T, clientStrategy *ClientStrategy, description string) *clientDialer {
	t.Helper()
	clientStrategy.mutex.Lock()
	defer clientStrategy.mutex.Unlock()
	for dialer := range clientStrategy.dialers {
		if dialer.description == description {
			return dialer
		}
	}
	t.Fatalf("the strategy has no %q dialer", description)
	return nil
}

// Performs one GET through one dialer and returns the body.
func testAltGet(t *testing.T, dialer *clientDialer) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(
		ctx, http.MethodGet, fmt.Sprintf("https://%s/hello", testAltApiHost), nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := dialer.HttpClient().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d", response.StatusCode)
	}
	bodyBytes, err := io.ReadAll(io.LimitReader(response.Body, 1024))
	if err != nil {
		t.Fatal(err)
	}
	return string(bodyBytes)
}

// The alt h3 dialer completes an api GET against an in-process alt, presenting
// the api host as the sni and h3 as the only alpn, with the fixture roots
// verifying the certificate (L1, L4).
func TestAltH3DialerGet(t *testing.T) {
	altServer := newTestAltServer(t, false)
	clientStrategy := newTestAltStrategy(t, altServer)
	if body := testAltGet(t, testAltDialer(t, clientStrategy, "alt h3")); body != testAltBodyText {
		t.Fatalf("body = %q", body)
	}
	testAltAssertClientHello(t, altServer)
}

// The alt whodis dialer completes the same GET through the dns translation,
// with the same sni and alpn.
func TestAltWhodisDialerGet(t *testing.T) {
	altServer := newTestAltServer(t, true)
	clientStrategy := newTestAltStrategy(t, altServer)
	if body := testAltGet(t, testAltDialer(t, clientStrategy, "alt whodis")); body != testAltBodyText {
		t.Fatalf("body = %q", body)
	}
	testAltAssertClientHello(t, altServer)
}

// An api dial names the api host and offers h3 alone, which is what alt
// dispatches on: an alt name as sni is refused (L1, L3).
func testAltAssertClientHello(t *testing.T, altServer *testAltServer) {
	t.Helper()
	serverName, alpn := altServer.clientHello(t)
	if serverName != testAltApiHost {
		t.Fatalf("sni = %q, expected the api host", serverName)
	}
	if !slices.Equal(alpn, []string{http3.NextProtoH3}) {
		t.Fatalf("alpn = %v, expected h3 alone", alpn)
	}
}

// Both alt dialers are api only: they carry no websocket dialer, so a platform
// dial never selects one, while an api request may (L4).
func TestAltDialersAreApiOnly(t *testing.T) {
	altServer := newTestAltServer(t, false)
	clientStrategy := newTestAltStrategy(t, altServer)

	apiDescriptions := []string{}
	for dialer := range clientStrategy.dialerWeights(false) {
		apiDescriptions = append(apiDescriptions, dialer.description)
		if dialer.supportsWebSocket() {
			t.Fatalf("%q carries a websocket dialer", dialer.description)
		}
	}
	slices.Sort(apiDescriptions)
	if !slices.Equal(apiDescriptions, []string{"alt h3", "alt whodis"}) {
		t.Fatalf("api dialers = %v", apiDescriptions)
	}
	if webSocketDialers := clientStrategy.dialerWeights(true); 0 < len(webSocketDialers) {
		t.Fatalf("websocket dialers = %d, expected none", len(webSocketDialers))
	}

	// a websocket dial with no other dialer has nothing to select, rather than
	// falling through to a direct dial of its own
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, _, _, err := clientStrategy.WsDialContextWithDialer(
		ctx, fmt.Sprintf("wss://%s/ws", testAltApiHost), http.Header{}); err == nil {
		t.Fatal("a websocket dial selected an api-only dialer")
	}
}

// The dialer priorities of L4: the tcp dialers, then alt h3, then the extender
// carriers, then alt whodis.
func TestAltDialerPriorityOrder(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, _ := newTestExtenderStrategy(t, clock, func(settings *ClientStrategySettings) {
		settings.AltUrl = "https://alt.space.example"
	})
	directory.AddBootstrap(netip.MustParseAddr("192.0.2.111"), ExtenderSourceDns)
	if expanded := clientStrategy.expandExtenderDialers(); len(expanded) == 0 {
		t.Fatal("no extender dialers were expanded")
	}

	priorities := map[string]int{}
	for dialer := range clientStrategy.dialerWeights(false) {
		priorities[dialer.description] = dialer.priority
	}
	expected := map[string]int{
		"fragment":         0,
		"normal":           25,
		"fragment+reorder": 50,
		"reorder":          50,
		"alt h3":           60,
		"extender tcptls":  100,
		"extender quic":    110,
		"extender dns":     120,
		"alt whodis":       130,
	}
	for description, priority := range expected {
		if actual, ok := priorities[description]; !ok || actual != priority {
			t.Fatalf("%q priority = %d (present %t), expected %d", description, actual, ok, priority)
		}
	}
}

// The whodis dialer encodes under the same tld the platform dns carrier uses,
// which is the one alt decodes (L1). They are defaulted in different settings
// structs, so a change to one that misses the other is silent until a dial
// stops being answered.
func TestAltWhodisTldMatchesThePlatformDnsTld(t *testing.T) {
	strategyTlds := DefaultClientStrategySettings().DnsTlds
	platformTlds := DefaultPlatformTransportSettings().DnsTlds
	if len(strategyTlds) == 0 {
		t.Fatal("the strategy has no whodis tld")
	}
	if len(strategyTlds) != len(platformTlds) {
		t.Fatalf("whodis tlds = %q, platform dns tlds = %q", strategyTlds, platformTlds)
	}
	for i := range strategyTlds {
		if !bytes.Equal(strategyTlds[i], platformTlds[i]) {
			t.Fatalf("whodis tld %q, platform dns tld %q", strategyTlds[i], platformTlds[i])
		}
	}
}
