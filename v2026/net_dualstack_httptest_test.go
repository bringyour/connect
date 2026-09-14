package connect

// net_dualstack_httptest_test.go — httptest servers and DoH fixtures bound to
// the loopback of one ip family, so the http, DoH and transfer-control tests
// run their assertions over ::1 as well as 127.0.0.1 (IPV6.md D3).
//
// httptest itself always binds 127.0.0.1. The helpers here swap the listener
// before Start so the served URL carries the family literal
// (http://[::1]:port for v6). The httptest certificate is issued for both
// 127.0.0.1 and ::1, so StartTLS and server.Client() work for either family.

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"testing"
)

// newFamilyHttptestUnstartedServer returns an unstarted httptest server whose
// listener is bound to the family's loopback. Configure it (EnableHTTP2,
// Config.ConnState, ...) and then Start or StartTLS it as usual.
func newFamilyHttptestUnstartedServer(t *testing.T, ipVersion int, handler http.Handler) *httptest.Server {
	t.Helper()
	server := httptest.NewUnstartedServer(handler)
	listener, err := net.Listen(testTcpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
	if err != nil {
		server.Listener.Close()
		t.Fatalf("listen on the v%d loopback: %v", ipVersion, err)
	}
	server.Listener.Close()
	server.Listener = listener
	return server
}

// newFamilyHttptestServer is httptest.NewServer on the family's loopback.
func newFamilyHttptestServer(t *testing.T, ipVersion int, handler http.Handler) *httptest.Server {
	t.Helper()
	server := newFamilyHttptestUnstartedServer(t, ipVersion, handler)
	server.Start()
	return server
}

// newFamilyHttptestTlsServer is httptest.NewTLSServer on the family's loopback.
func newFamilyHttptestTlsServer(t *testing.T, ipVersion int, handler http.Handler) *httptest.Server {
	t.Helper()
	server := newFamilyHttptestUnstartedServer(t, ipVersion, handler)
	server.StartTLS()
	return server
}

// testDnsRecordType is the address record type of the family: A or AAAA.
func testDnsRecordType(ipVersion int) string {
	if ipVersion == 6 {
		return "AAAA"
	}
	return "A"
}

// testLoopbackAddr is the loopback address of the family as a netip.Addr.
func testLoopbackAddr(ipVersion int) netip.Addr {
	return netip.MustParseAddr(testLoopbackIp(ipVersion))
}

// testDocAddr is a documentation-range address of the family with the given
// host number: 203.0.113.n (RFC 5737) or 2001:db8::n (RFC 3849). Nothing
// listens on either, so a test can name a server that must never answer.
func testDocAddr(ipVersion int, n int) netip.Addr {
	if ipVersion == 6 {
		return netip.MustParseAddr(fmt.Sprintf("2001:db8::%x", n))
	}
	return netip.MustParseAddr(fmt.Sprintf("203.0.113.%d", n))
}

// testDocHostPort joins testDocAddr with a port, bracketing v6.
func testDocHostPort(ipVersion int, n int, port int) string {
	return net.JoinHostPort(testDocAddr(ipVersion, n).String(), fmt.Sprintf("%d", port))
}

// setRemoteDohUrls installs the remote DoH server list of the family on the
// settings and clears the other family's list. DefaultDnsResolverSettings
// ships real operator endpoints for BOTH families and the cache-wide
// IpVersion defaults to 0 (walk both), so a test that only replaced its own
// family's list would still fan out to the real internet through the other
// list, and over the family it did not replace at that.
func setRemoteDohUrls(settings *DohSettings, ipVersion int, urls ...string) {
	if ipVersion == 6 {
		settings.DnsResolverSettings.RemoteDohUrlsIpv6 = urls
		settings.DnsResolverSettings.RemoteDohUrlsIpv4 = nil
	} else {
		settings.DnsResolverSettings.RemoteDohUrlsIpv4 = urls
		settings.DnsResolverSettings.RemoteDohUrlsIpv6 = nil
	}
}

// setLocalDohUrls is setRemoteDohUrls for the local (host-side) DoH list.
func setLocalDohUrls(settings *DohSettings, ipVersion int, urls ...string) {
	if ipVersion == 6 {
		settings.DnsResolverSettings.LocalDohUrlsIpv6 = urls
		settings.DnsResolverSettings.LocalDohUrlsIpv4 = nil
	} else {
		settings.DnsResolverSettings.LocalDohUrlsIpv4 = urls
		settings.DnsResolverSettings.LocalDohUrlsIpv6 = nil
	}
}

// setRemoteDnsServers installs the remote plain-dns server list of the family
// and clears the other family's list, for the same reason as setRemoteDohUrls.
func setRemoteDnsServers(settings *DohSettings, ipVersion int, servers ...string) {
	if ipVersion == 6 {
		settings.DnsResolverSettings.RemoteDnsIpv6 = servers
		settings.DnsResolverSettings.RemoteDnsIpv4 = nil
	} else {
		settings.DnsResolverSettings.RemoteDnsIpv4 = servers
		settings.DnsResolverSettings.RemoteDnsIpv6 = nil
	}
}

// remoteDohResolverSettings is a remote-DoH-only resolver configuration with
// the urls in the family's list.
func remoteDohResolverSettings(ipVersion int, urls ...string) *DnsResolverSettings {
	settings := &DnsResolverSettings{EnableRemoteDoh: true}
	if ipVersion == 6 {
		settings.RemoteDohUrlsIpv6 = urls
	} else {
		settings.RemoteDohUrlsIpv4 = urls
	}
	return settings
}

// localDohResolverSettings is a local-DoH-only resolver configuration with
// the urls in the family's list.
func localDohResolverSettings(ipVersion int, urls ...string) *DnsResolverSettings {
	settings := &DnsResolverSettings{EnableLocalDoh: true}
	if ipVersion == 6 {
		settings.LocalDohUrlsIpv6 = urls
	} else {
		settings.LocalDohUrlsIpv4 = urls
	}
	return settings
}
