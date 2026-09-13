package connect

import (
	"context"
	"net"
	"slices"
	"sync"
	"testing"
	"time"
)

// The platform carriers on alt (EXTENDER.md L3, L4): the H3 modes send their
// packets to the alt host while the sni, the quic authentication and the H1
// websocket all stay on the platform url.

const (
	testAltPlatformUrl = "wss://connect.space.example"
	testAltUrl         = "https://alt.space.example"
	testAltIp          = "192.0.2.53"
)

// One transport with nothing running: h3DialCandidates needs only the
// settings, the strategy resolver and the family pin.
func newTestAltTransport(t *testing.T, settings *PlatformTransportSettings) *PlatformTransport {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	clientStrategy := NewClientStrategyWithDefaults(ctx)
	t.Cleanup(clientStrategy.Close)
	return &PlatformTransport{
		ctx:            ctx,
		log:            loggerOrDefault(nil),
		clientStrategy: clientStrategy,
		platformUrl:    testAltPlatformUrl,
		settings:       settings,
	}
}

func testAltCandidateHostPorts(udpAddrs []*net.UDPAddr) []string {
	hostPorts := []string{}
	for _, udpAddr := range udpAddrs {
		hostPorts = append(hostPorts, udpAddr.String())
	}
	return hostPorts
}

// The plain H3 carrier dials the alt host, and without an alt url it dials the
// platform host exactly as before.
func TestPlatformTransportAltUrlDialsTheAltHostOverH3(t *testing.T) {
	settings := DefaultPlatformTransportSettings()
	settings.AltUrl = "https://" + testAltIp
	transport := newTestAltTransport(t, settings)

	udpAddrs, _, err := transport.h3DialCandidates(
		context.Background(), TransportModeH3, "connect.space.example")
	if err != nil {
		t.Fatal(err)
	}
	hostPorts := testAltCandidateHostPorts(udpAddrs)
	if !slices.Equal(hostPorts, []string{net.JoinHostPort(testAltIp, "443")}) {
		t.Fatalf("candidates = %v, expected the alt host on the h3 port", hostPorts)
	}

	// a port on the alt url pins the carrier, which is what a fixture wants
	settings.AltUrl = "https://" + net.JoinHostPort(testAltIp, "14443")
	udpAddrs, _, err = transport.h3DialCandidates(
		context.Background(), TransportModeH3, "connect.space.example")
	if err != nil {
		t.Fatal(err)
	}
	hostPorts = testAltCandidateHostPorts(udpAddrs)
	if !slices.Equal(hostPorts, []string{net.JoinHostPort(testAltIp, "14443")}) {
		t.Fatalf("candidates = %v, expected the alt url port", hostPorts)
	}

	// with no alt url the platform host is dialed, which is every space with
	// no alt deployment
	settings.AltUrl = ""
	settings.resolveH3AddrsForTest = func(ctx context.Context, address string, ipFamily int) ([]*net.UDPAddr, error) {
		if address != net.JoinHostPort("connect.space.example", "443") {
			t.Errorf("resolved %q, expected the platform host", address)
		}
		return []*net.UDPAddr{{IP: net.ParseIP("198.51.100.7"), Port: 443}}, nil
	}
	if _, _, err := transport.h3DialCandidates(
		context.Background(), TransportModeH3, "connect.space.example"); err != nil {
		t.Fatal(err)
	}
}

// The h3dns carrier dials the alt host on 53 and then 4053, which the race
// launches in that order with the second staggered behind the first (L2).
func TestPlatformTransportAltH3DnsTries53Then4053(t *testing.T) {
	settings := DefaultPlatformTransportSettings()
	settings.AltUrl = "https://" + testAltIp
	transport := newTestAltTransport(t, settings)

	udpAddrs, wrap, err := transport.h3DialCandidates(
		context.Background(), TransportModeH3Dns, "connect.space.example")
	if err != nil {
		t.Fatal(err)
	}
	if wrap == nil {
		t.Fatal("the dns carrier has no packet translation")
	}
	hostPorts := testAltCandidateHostPorts(udpAddrs)
	expected := []string{
		net.JoinHostPort(testAltIp, "53"),
		net.JoinHostPort(testAltIp, "4053"),
	}
	if !slices.Equal(hostPorts, expected) {
		t.Fatalf("candidates = %v, expected %v", hostPorts, expected)
	}

	// with no alt url the carrier keeps its single configured port
	settings.AltUrl = ""
	settings.DnsPort = 15053
	udpAddrs, _, err = transport.h3DialCandidates(
		context.Background(), TransportModeH3Dns, testAltIp)
	if err != nil {
		t.Fatal(err)
	}
	hostPorts = testAltCandidateHostPorts(udpAddrs)
	if !slices.Equal(hostPorts, []string{net.JoinHostPort(testAltIp, "15053")}) {
		t.Fatalf("candidates = %v, expected the configured dns port alone", hostPorts)
	}
}

// The pump host derives from the alt url when nothing names it, and an
// explicit pump host still wins (L3).
func TestPlatformTransportAltH3DnsPumpHost(t *testing.T) {
	settings := DefaultPlatformTransportSettings()
	settings.AltUrl = "https://" + testAltIp
	settings.DnsPumpHost = ""
	transport := newTestAltTransport(t, settings)

	udpAddrs, _, err := transport.h3DialCandidates(
		context.Background(), TransportModeH3DnsPump, "connect.space.example")
	if err != nil {
		t.Fatal(err)
	}
	hostPorts := testAltCandidateHostPorts(udpAddrs)
	expected := []string{
		net.JoinHostPort(testAltIp, "53"),
		net.JoinHostPort(testAltIp, "4053"),
	}
	if !slices.Equal(hostPorts, expected) {
		t.Fatalf("candidates = %v, expected the alt host on both dns ports", hostPorts)
	}

	settings.DnsPumpHost = "198.51.100.9"
	udpAddrs, _, err = transport.h3DialCandidates(
		context.Background(), TransportModeH3DnsPump, "connect.space.example")
	if err != nil {
		t.Fatal(err)
	}
	hostPorts = testAltCandidateHostPorts(udpAddrs)
	if !slices.Equal(hostPorts, []string{net.JoinHostPort("198.51.100.9", "53")}) {
		t.Fatalf("candidates = %v, expected the configured pump host", hostPorts)
	}

	// no pump host and no alt url is a configuration with no destination
	settings.DnsPumpHost = ""
	settings.AltUrl = ""
	if _, _, err := transport.h3DialCandidates(
		context.Background(), TransportModeH3DnsPump, "connect.space.example"); err == nil {
		t.Fatal("a pump carrier with no host was accepted")
	}
}

// The H1 websocket keeps the platform host while the H3 carrier is on alt, and
// the api-only alt dialers never carry a websocket dial (L4).
func TestPlatformTransportAltUrlKeepsH1OnThePlatformHost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var stateLock sync.Mutex
	wsAddrs := []string{}
	h3Addrs := []string{}
	dialed := make(chan struct{}, 8)
	note := func(addrs *[]string, addr string) {
		stateLock.Lock()
		defer stateLock.Unlock()
		if !slices.Contains(*addrs, addr) {
			*addrs = append(*addrs, addr)
		}
		select {
		case dialed <- struct{}{}:
		default:
		}
	}

	strategySettings := DefaultClientStrategySettings()
	strategySettings.AltUrl = testAltUrl
	strategySettings.EnableResilient = false
	strategySettings.ConnectSettings.DialNetworkHook = func(network string, addr string) {
		note(&wsAddrs, addr)
	}
	clientStrategy := NewClientStrategy(ctx, strategySettings)
	defer clientStrategy.Close()

	h1Settings := testingFamilyTransportSettings()
	h1Settings.AltUrl = testAltUrl
	h1Transport := NewPlatformTransportWithTargetMode(
		ctx,
		clientStrategy,
		NewRouteManager(ctx, "alt-h1"),
		testAltPlatformUrl,
		testingFamilyAuth(),
		TransportModeH1,
		h1Settings,
	)
	defer h1Transport.Close()

	h3Settings := testingFamilyTransportSettings()
	h3Settings.AltUrl = testAltUrl
	h3Settings.resolveH3AddrsForTest = func(ctx context.Context, address string, ipFamily int) ([]*net.UDPAddr, error) {
		note(&h3Addrs, address)
		return nil, context.Canceled
	}
	h3Transport := NewPlatformTransportWithTargetMode(
		ctx,
		clientStrategy,
		NewRouteManager(ctx, "alt-h3"),
		testAltPlatformUrl,
		testingFamilyAuth(),
		TransportModeH3,
		h3Settings,
	)
	defer h3Transport.Close()

	deadline := time.After(30 * time.Second)
	for {
		stateLock.Lock()
		haveWs := 0 < len(wsAddrs)
		haveH3 := 0 < len(h3Addrs)
		stateLock.Unlock()
		if haveWs && haveH3 {
			break
		}
		select {
		case <-dialed:
		case <-deadline:
			t.Fatalf("ws dials = %v, h3 dials = %v", wsAddrs, h3Addrs)
		}
	}

	stateLock.Lock()
	defer stateLock.Unlock()
	for _, addr := range wsAddrs {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("ws dialed %q", addr)
		}
		if host != "connect.space.example" {
			t.Fatalf("ws dialed %q, expected the platform host", addr)
		}
	}
	for _, addr := range h3Addrs {
		if addr != net.JoinHostPort("alt.space.example", "443") {
			t.Fatalf("h3 resolved %q, expected the alt host", addr)
		}
	}
}

// The group's pinned transports take the family alt name that pairs with their
// own family platform url, and an explicit alt url is passed through unchanged
// (L3).
func TestFamilyPlatformTransportGroupDerivesFamilyAltUrls(t *testing.T) {
	cases := []struct {
		altUrl   string
		altUrlV4 string
		altUrlV6 string
	}{
		{
			// the derived alt url becomes the derived family alt urls
			altUrl:   "wss://alt.space.example",
			altUrlV4: "wss://alt-v4.space.example",
			altUrlV6: "wss://alt-v6.space.example",
		},
		{
			// an override is not a name the label rule produced, so every
			// transport keeps it
			altUrl:   "https://127.0.0.1:14443",
			altUrlV4: "https://127.0.0.1:14443",
			altUrlV6: "https://127.0.0.1:14443",
		},
		{
			altUrl:   "",
			altUrlV4: "",
			altUrlV6: "",
		},
	}
	for _, c := range cases {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			clientStrategy := NewClientStrategyWithDefaults(ctx)
			defer clientStrategy.Close()
			settings := testingFamilyTransportSettings()
			settings.AltUrl = c.altUrl
			group := NewFamilyPlatformTransportGroup(
				ctx,
				DefaultClientStrategySettings(),
				clientStrategy,
				NewRouteManager(ctx, "alt-group"),
				testAltPlatformUrl,
				"wss://connect-v4.space.example",
				"wss://connect-v6.space.example",
				testingFamilyAuth(),
				TransportModeH1,
				settings,
				&FamilyPlatformTransportGroupSettings{StandbyDelay: time.Hour},
			)
			defer group.Close()

			if altUrl := group.Ipv4Transport().settings.AltUrl; altUrl != c.altUrlV4 {
				t.Errorf("v4 alt url = %q, expected %q", altUrl, c.altUrlV4)
			}
			if altUrl := group.Ipv6Transport().settings.AltUrl; altUrl != c.altUrlV6 {
				t.Errorf("v6 alt url = %q, expected %q", altUrl, c.altUrlV6)
			}
			if altUrl := group.StandbyTransport().settings.AltUrl; altUrl != c.altUrl {
				t.Errorf("standby alt url = %q, expected %q", altUrl, c.altUrl)
			}
		}()
	}
}
