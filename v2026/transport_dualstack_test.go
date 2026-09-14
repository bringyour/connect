package connect

// transport_dualstack_test.go — dual-stack scaffolding for the transport and
// platform tests (IPV6.md D3), on top of test_dualstack_test.go: the benchmark
// fan-out, the url-host and socket-address forms of the loopback literal, an
// httptest server bound to one family, and the H3 address seam that points a
// transport at a test listener.

import (
	"context"
	"fmt"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

// forEachIpVersionBenchmark is forEachIpVersion for benchmarks: one
// sub-benchmark per family, with v6 loopback required rather than skipped.
func forEachIpVersionBenchmark(b *testing.B, body func(b *testing.B, ipVersion int)) {
	b.Helper()
	for _, ipVersion := range testIpVersions {
		b.Run(fmt.Sprintf("v%d", ipVersion), func(b *testing.B) {
			if ipVersion == 6 {
				requireIpv6LoopbackTB(b)
			}
			body(b, ipVersion)
		})
	}
}

// requireIpv6LoopbackTB is requireIpv6Loopback for a test or a benchmark.
func requireIpv6LoopbackTB(tb testing.TB) {
	tb.Helper()
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		tb.Fatalf("ipv6 loopback is required for dual-stack tests: %v", err)
	}
	listener.Close()
}

// testLoopbackHost is the loopback literal in url-host form: v6 is bracketed.
func testLoopbackHost(ipVersion int) string {
	if ipVersion == 6 {
		return "[::1]"
	}
	return "127.0.0.1"
}

// testLoopbackUdpAddr is an unbound (port 0) loopback udp address for the
// version.
func testLoopbackUdpAddr(ipVersion int) *net.UDPAddr {
	return &net.UDPAddr{IP: net.ParseIP(testLoopbackIp(ipVersion)), Port: 0}
}

// testUnspecifiedIp is the wildcard address for the version.
func testUnspecifiedIp(ipVersion int) net.IP {
	if ipVersion == 6 {
		return net.IPv6zero
	}
	return net.IPv4zero
}

// testIndexedIp is the address whose last byte is `index` in the version's
// zero network (0.0.0.index or ::index): a distinct, stable peer key.
func testIndexedIp(ipVersion int, index byte) net.IP {
	ip := testUnspecifiedIp(ipVersion)
	ip = append(net.IP(nil), ip...)
	ip[len(ip)-1] = index
	return ip
}

// testRandomIp draws a random address of the version, for tests that key
// state by peer address.
func testRandomIp(ipVersion int) net.IP {
	ip := make(net.IP, net.IPv4len)
	if ipVersion == 6 {
		ip = make(net.IP, net.IPv6len)
	}
	mathrand.Read(ip)
	return ip
}

// newTestingLoopbackHttpServer starts an httptest server bound to the
// loopback of the family under test; httptest's own listener is always v4.
func newTestingLoopbackHttpServer(tb testing.TB, ipVersion int, handler http.Handler, useTls bool) *httptest.Server {
	tb.Helper()
	server := httptest.NewUnstartedServer(handler)
	server.Listener.Close()
	listener, err := net.Listen(testTcpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
	if err != nil {
		tb.Fatal(err)
	}
	server.Listener = listener
	if useTls {
		server.StartTLS()
	} else {
		server.Start()
	}
	return server
}

// testingH3LoopbackResolver returns the H3 address resolution a v6 subtest
// installs (PlatformTransportSettings.resolveH3AddrsForTest) so the transport
// dials the test's own loopback listener on its ephemeral port, which no
// resolution of the platform host could produce. nil for v4, which keeps
// exercising the real resolution path. h3DialCandidates itself forms v6
// literal addresses correctly -- TestPlatformTransportH3ResolvesLiteralIpv6PlatformHost
// covers that directly, without this seam.
func testingH3LoopbackResolver(ipVersion int, port int) func(context.Context, string, int) ([]*net.UDPAddr, error) {
	if ipVersion != 6 {
		return nil
	}
	udpAddr := &net.UDPAddr{IP: net.ParseIP("::1"), Port: port}
	return func(context.Context, string, int) ([]*net.UDPAddr, error) {
		return []*net.UDPAddr{udpAddr}, nil
	}
}

// A v6 literal platform url ("https://[::1]") must yield a dialable H3
// candidate, as a v4 literal does. It does not: h3DialCandidates joins the
// url host and the port with fmt.Sprintf("%s:%d"), and url.Hostname strips the
// brackets, so the address becomes "::1:<port>" and net.SplitHostPort rejects
// it. Named hosts are unaffected. The v6 subtests of the H3 platform tests
// install testingH3LoopbackResolver to get past this meanwhile.
func TestPlatformTransportH3ResolvesLiteralIpv6PlatformHost(t *testing.T) {
	requireIpv6Loopback(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := testingPlatformTransportSettings()
	settings.StartDisabled = true
	settings.H3Port = 4433
	transport := NewPlatformTransportWithTargetMode(
		ctx,
		NewClientStrategyWithDefaults(ctx),
		NewRouteManager(ctx, "h3-literal-v6"),
		"https://[::1]",
		&ClientAuth{ByJwt: "testing", InstanceId: NewId(), AppVersion: "testing"},
		TransportModeH3,
		settings,
	)
	defer transport.Close()
	serverName, err := connectHost("https://[::1]")
	if err != nil {
		t.Fatal(err)
	}
	udpAddrs, _, err := transport.h3DialCandidates(ctx, TransportModeH3, serverName)
	if err != nil {
		t.Fatalf("h3 dial candidates for a v6 literal platform host: %v", err)
	}
	if len(udpAddrs) != 1 || !udpAddrs[0].IP.Equal(net.ParseIP("::1")) || udpAddrs[0].Port != 4433 {
		t.Fatalf("h3 dial candidates = %v, want [[::1]:4433]", udpAddrs)
	}
}
