package connect

// tests for the dns lookup modes of DnsResolverSettings — the resolver chain
// remote doh -> local doh -> remote dns -> local dns (see DohCache.resolve and
// DefaultDnsResolverSettings) — each mode in isolation with its correct dialer
// (remote modes egress the tunnel dialer, local modes the host), the fallthrough
// order with every mode enabled, and that names resolved through each mode are
// recorded into the mux's ip→server-name reverse index (UpgradeMux.ServerNames).

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"github.com/urnetwork/connect/v2026/protocol"
)

// answerPlainDns builds the response for one plaintext dns query payload, echoing the
// query id and question: an A answer for a name in answers, NXDOMAIN for an unknown
// name, SERVFAIL when failing. ok is false for an unparseable or question-less query.
func answerPlainDns(queryPayload []byte, answers map[string]netip.Addr, failing bool) ([]byte, bool) {
	var msg dnsmessage.Message
	if err := msg.Unpack(queryPayload); err != nil {
		return nil, false
	}
	if len(msg.Questions) == 0 {
		return nil, false
	}
	q := msg.Questions[0]
	name := strings.TrimSuffix(strings.ToLower(q.Name.String()), ".")
	resp := dnsmessage.Message{
		Header: dnsmessage.Header{
			ID:                 msg.Header.ID,
			Response:           true,
			RecursionAvailable: true,
		},
		Questions: msg.Questions,
	}
	addr, known := answers[name]
	switch {
	case failing:
		resp.Header.RCode = dnsmessage.RCodeServerFailure
	case !known:
		resp.Header.RCode = dnsmessage.RCodeNameError
	case q.Type == dnsmessage.TypeA && addr.Is4():
		resp.Answers = []dnsmessage.Resource{{
			Header: dnsmessage.ResourceHeader{
				Name:  q.Name,
				Type:  dnsmessage.TypeA,
				Class: dnsmessage.ClassINET,
				TTL:   60,
			},
			Body: &dnsmessage.AResource{A: addr.As4()},
		}}
		// a known name with a non-A question answers an empty NOERROR (NODATA)
	}
	respPayload, err := resp.Pack()
	if err != nil {
		return nil, false
	}
	return respPayload, true
}

// fakeDnsServer is a plaintext udp dns server for exercising the plain-dns lookup
// modes (remote dns / local dns): it answers per answerPlainDns from a fixed
// name→address table, with a SERVFAIL toggle for fallthrough tests.
type fakeDnsServer struct {
	pc       net.PacketConn
	answers  map[string]netip.Addr
	failing  atomic.Bool
	requests atomic.Int32
}

func newFakeDnsServer(t *testing.T, answers map[string]netip.Addr) *fakeDnsServer {
	return newFakeDnsServerOnFamily(t, answers, 4)
}

// newFakeDnsServerOnFamily is newFakeDnsServer bound to the loopback of the
// given ip version, so a resolver mode can be exercised over either family.
func newFakeDnsServerOnFamily(t *testing.T, answers map[string]netip.Addr, ipVersion int) *fakeDnsServer {
	t.Helper()
	pc, err := net.ListenPacket(testUdpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
	if err != nil {
		t.Fatal(err)
	}
	self := &fakeDnsServer{
		pc:      pc,
		answers: answers,
	}
	go func() {
		buf := make([]byte, 2048)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			self.requests.Add(1)
			if respPayload, ok := answerPlainDns(buf[:n], self.answers, self.failing.Load()); ok {
				pc.WriteTo(respPayload, from)
			}
		}
	}()
	return self
}

func (self *fakeDnsServer) close() {
	self.pc.Close()
}

// addr is the server's host-reachable udp address, for dial redirects.
func (self *fakeDnsServer) addr() string {
	return self.pc.LocalAddr().String()
}

// resolver returns a go resolver that queries this server, standing in for the
// host-side local resolver — which dials the configured server ip on :53, an
// address a test cannot bind.
func (self *fakeDnsServer) resolver() *net.Resolver {
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network string, addr string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, self.addr())
		},
	}
}

// TestDohCacheRemoteDohMode: the remote doh mode resolves over the tunnel dialer
// (DialContextSettings), and with the mode disabled (and no other mode enabled) the
// server is never queried and the failure is non-authoritative.
func TestDohCacheRemoteDohMode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testIp := netip.MustParseAddr("203.0.113.11")
	var requests atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		writeDohWire(w, r, []netip.Addr{testIp}, 60, false)
	}))
	defer server.Close()
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())

	var mu sync.Mutex
	tunnelDials := []string{}
	newSettings := func(enable bool) *DohSettings {
		settings := DefaultDohSettings()
		settings.RequestTimeout = 5 * time.Second
		settings.DnsResolverSettings = &DnsResolverSettings{
			EnableRemoteDoh:   enable,
			RemoteDohUrlsIpv4: []string{server.URL},
			TlsConfig:         &tls.Config{RootCAs: pool},
		}
		// the "tunnel" dialer: record dials and pass them through
		settings.DialContextSettings = &DialContextSettings{
			DialContext: func(dialCtx context.Context, network string, addr string) (net.Conn, error) {
				mu.Lock()
				tunnelDials = append(tunnelDials, addr)
				mu.Unlock()
				return (&net.Dialer{}).DialContext(dialCtx, network, addr)
			},
		}
		return settings
	}

	addrs, authoritative := NewDohCache(newSettings(true)).QueryResult(ctx, "A", "remote-doh.mode.test")
	if !authoritative || !slices.Contains(addrs, testIp) {
		t.Fatalf("remote doh did not resolve: addrs=%v authoritative=%t", addrs, authoritative)
	}
	mu.Lock()
	if len(tunnelDials) == 0 {
		t.Fatal("the remote doh request must egress the tunnel dialer")
	}
	dialsBefore := len(tunnelDials)
	mu.Unlock()

	// mode disabled: nothing else is enabled, so resolution fails non-authoritatively
	// without touching the server or the tunnel
	requestsBefore := requests.Load()
	addrs, authoritative = NewDohCache(newSettings(false)).QueryResult(ctx, "A", "remote-doh-disabled.mode.test")
	if 0 < len(addrs) || authoritative {
		t.Fatalf("disabled remote doh resolved: addrs=%v authoritative=%t", addrs, authoritative)
	}
	if got := requests.Load(); got != requestsBefore {
		t.Fatalf("disabled remote doh queried the server: %d requests", got-requestsBefore)
	}
	mu.Lock()
	if len(tunnelDials) != dialsBefore {
		t.Fatalf("disabled remote doh dialed the tunnel: %v", tunnelDials[dialsBefore:])
	}
	mu.Unlock()
}

// TestDohCacheLocalDohMode: the local doh mode resolves over the HOST dialer — with
// the tunnel dialer refusing every dial, resolution still succeeds and the tunnel
// carries nothing.
func TestDohCacheLocalDohMode(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testIp := netip.MustParseAddr("203.0.113.12")
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeDohWire(w, r, []netip.Addr{testIp}, 60, false)
	}))
	defer server.Close()
	pool := x509.NewCertPool()
	pool.AddCert(server.Certificate())

	var tunnelDials atomic.Int32
	settings := DefaultDohSettings()
	settings.RequestTimeout = 5 * time.Second
	settings.DnsResolverSettings = &DnsResolverSettings{
		EnableLocalDoh:   true,
		LocalDohUrlsIpv4: []string{server.URL},
		TlsConfig:        &tls.Config{RootCAs: pool},
	}
	settings.DialContextSettings = &DialContextSettings{
		DialContext: func(dialCtx context.Context, network string, addr string) (net.Conn, error) {
			tunnelDials.Add(1)
			return nil, net.ErrClosed
		},
	}

	addrs, authoritative := NewDohCache(settings).QueryResult(ctx, "A", "local-doh.mode.test")
	if !authoritative || !slices.Contains(addrs, testIp) {
		t.Fatalf("local doh did not resolve: addrs=%v authoritative=%t", addrs, authoritative)
	}
	if got := tunnelDials.Load(); got != 0 {
		t.Fatalf("local doh dialed the tunnel %d times; it must use the host dialer", got)
	}
}

// TestDohCacheRemoteDnsMode: the remote plain-dns mode — the shape the regional
// recommendation returns ({EnableRemoteDns, RemoteDnsIpv4}, see
// RegionalDnsResolverSettings) — resolves plaintext :53 through the tunnel dialer to
// the configured server, and the answer is cached (LocalExpiration).
func TestDohCacheRemoteDnsMode(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testDohCacheRemoteDnsMode(t, ipVersion)
	})
}

func testDohCacheRemoteDnsMode(t *testing.T, ipVersion int) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testIp := netip.MustParseAddr("203.0.113.13")
	remoteDns := newFakeDnsServerOnFamily(t, map[string]netip.Addr{
		"remote-dns.mode.test": testIp,
	}, ipVersion)
	defer remoteDns.close()

	// the regional recommendation shape, pointed at the test server of the
	// family under test (the other family's list is emptied so the interleaved
	// resolver list holds exactly this server)
	rs := RegionalDnsResolverSettings("cn")
	if rs == nil || !rs.EnableRemoteDns {
		t.Fatalf("regional settings must enable remote dns: %+v", rs)
	}
	configuredServer := "192.0.2.53"
	if ipVersion == 6 {
		configuredServer = "2001:db8::53"
	}
	rs.RemoteDnsIpv4 = nil
	rs.RemoteDnsIpv6 = nil
	if ipVersion == 4 {
		rs.RemoteDnsIpv4 = []string{configuredServer}
	} else {
		rs.RemoteDnsIpv6 = []string{configuredServer}
	}
	configuredServerPrefix := net.JoinHostPort(configuredServer, "53")

	var mu sync.Mutex
	dns53Dials := []string{}
	settings := DefaultDohSettings()
	settings.RequestTimeout = 5 * time.Second
	settings.DnsResolverSettings = rs
	// the "tunnel" dialer: track plaintext :53 dials and redirect them to the fake
	// server; nothing else should dial
	settings.DialContextSettings = &DialContextSettings{
		DialContext: func(dialCtx context.Context, network string, addr string) (net.Conn, error) {
			if strings.HasSuffix(addr, ":53") {
				mu.Lock()
				dns53Dials = append(dns53Dials, addr)
				mu.Unlock()
				return (&net.Dialer{}).DialContext(dialCtx, network, remoteDns.addr())
			}
			return nil, net.ErrClosed
		},
	}
	cache := NewDohCache(settings)

	addrs, authoritative := cache.QueryResult(ctx, "A", "remote-dns.mode.test")
	if !authoritative || !slices.Contains(addrs, testIp) {
		t.Fatalf("remote dns did not resolve: addrs=%v authoritative=%t", addrs, authoritative)
	}
	mu.Lock()
	if len(dns53Dials) == 0 || dns53Dials[0] != configuredServerPrefix {
		t.Fatalf("the resolution must dial the configured remote dns server through the tunnel, dials=%v", dns53Dials)
	}
	dialsBefore := len(dns53Dials)
	mu.Unlock()

	// the answer is cached: a re-query is served without new resolver traffic
	addrs, authoritative = cache.QueryResult(ctx, "A", "remote-dns.mode.test")
	if !authoritative || !slices.Contains(addrs, testIp) {
		t.Fatalf("cached remote dns query: addrs=%v authoritative=%t", addrs, authoritative)
	}
	mu.Lock()
	if len(dns53Dials) != dialsBefore {
		t.Fatalf("a cached query re-dialed the resolver: %v", dns53Dials[dialsBefore:])
	}
	mu.Unlock()
}

// TestDohCacheLocalDnsMode: the local plain-dns mode resolves via the host-side
// resolver with the tunnel dialer refusing everything, and an NXDOMAIN answer is an
// authoritative miss that is cached. the local resolver dials the configured server
// ip on :53 (unbindable in a test), so the fake server stands in for it.
func TestDohCacheLocalDnsMode(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testDohCacheLocalDnsMode(t, ipVersion)
	})
}

func testDohCacheLocalDnsMode(t *testing.T, ipVersion int) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	testIp := netip.MustParseAddr("203.0.113.14")
	localDns := newFakeDnsServerOnFamily(t, map[string]netip.Addr{
		"local-dns.mode.test": testIp,
	}, ipVersion)
	defer localDns.close()

	var tunnelDials atomic.Int32
	settings := DefaultDohSettings()
	settings.RequestTimeout = 5 * time.Second
	settings.DnsResolverSettings = &DnsResolverSettings{
		EnableLocalDns: true,
	}
	if ipVersion == 4 {
		settings.DnsResolverSettings.LocalDnsIpv4 = []string{"192.0.2.54"}
	} else {
		settings.DnsResolverSettings.LocalDnsIpv6 = []string{"2001:db8::54"}
	}
	settings.DialContextSettings = &DialContextSettings{
		DialContext: func(dialCtx context.Context, network string, addr string) (net.Conn, error) {
			tunnelDials.Add(1)
			return nil, net.ErrClosed
		},
	}
	cache := NewDohCache(settings)
	cache.localResolver = localDns.resolver()

	addrs, authoritative := cache.QueryResult(ctx, "A", "local-dns.mode.test")
	if !authoritative || !slices.Contains(addrs, testIp) {
		t.Fatalf("local dns did not resolve: addrs=%v authoritative=%t", addrs, authoritative)
	}
	if localDns.requests.Load() == 0 {
		t.Fatal("the local dns server was never queried")
	}
	if got := tunnelDials.Load(); got != 0 {
		t.Fatalf("local dns dialed the tunnel %d times; it must stay host-side", got)
	}

	// an NXDOMAIN is an authoritative miss (empty + authoritative), and is cached
	addrs, authoritative = cache.QueryResult(ctx, "A", "missing.mode.test")
	if 0 < len(addrs) || !authoritative {
		t.Fatalf("nxdomain should be an authoritative miss: addrs=%v authoritative=%t", addrs, authoritative)
	}
	requestsBefore := localDns.requests.Load()
	addrs, authoritative = cache.QueryResult(ctx, "A", "missing.mode.test")
	if 0 < len(addrs) || !authoritative {
		t.Fatalf("cached miss: addrs=%v authoritative=%t", addrs, authoritative)
	}
	if got := localDns.requests.Load(); got != requestsBefore {
		t.Fatalf("a cached miss re-queried the resolver: %d requests", got-requestsBefore)
	}
}

// TestDohCacheResolverChainFallthrough drives queries through the full resolver chain
// (remote doh -> local doh -> remote dns -> local dns) with all four modes enabled,
// failing the stages one by one: each stage answers only when every earlier stage has
// failed, the failed stages are actually attempted, and the stages after the winner
// are never touched.
func TestDohCacheResolverChainFallthrough(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	remoteDohIp := netip.MustParseAddr("203.0.113.1")
	localDohIp := netip.MustParseAddr("203.0.113.2")
	remoteDnsIp := netip.MustParseAddr("203.0.113.3")
	localDnsIp := netip.MustParseAddr("203.0.113.4")

	var remoteDohFailing, localDohFailing atomic.Bool
	var remoteDohRequests, localDohRequests atomic.Int32
	remoteDohServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		remoteDohRequests.Add(1)
		if remoteDohFailing.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeDohWire(w, r, []netip.Addr{remoteDohIp}, 60, false)
	}))
	defer remoteDohServer.Close()
	localDohServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		localDohRequests.Add(1)
		if localDohFailing.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeDohWire(w, r, []netip.Addr{localDohIp}, 60, false)
	}))
	defer localDohServer.Close()
	pool := x509.NewCertPool()
	pool.AddCert(remoteDohServer.Certificate())
	pool.AddCert(localDohServer.Certificate())

	chainNames := []string{"one.chain.test", "two.chain.test", "three.chain.test", "four.chain.test"}
	answersTo := func(ip netip.Addr) map[string]netip.Addr {
		answers := map[string]netip.Addr{}
		for _, name := range chainNames {
			answers[name] = ip
		}
		return answers
	}
	remoteDns := newFakeDnsServer(t, answersTo(remoteDnsIp))
	defer remoteDns.close()
	localDns := newFakeDnsServer(t, answersTo(localDnsIp))
	defer localDns.close()

	// a fresh cache per phase, so the phase result is resolved (not served from the
	// previous phase's cache)
	newCache := func() *DohCache {
		settings := DefaultDohSettings()
		settings.RequestTimeout = 5 * time.Second
		settings.DohServerStagger = 0
		settings.DnsResolverSettings = &DnsResolverSettings{
			EnableRemoteDoh:   true,
			EnableLocalDoh:    true,
			EnableRemoteDns:   true,
			EnableLocalDns:    true,
			RemoteDohUrlsIpv4: []string{remoteDohServer.URL},
			LocalDohUrlsIpv4:  []string{localDohServer.URL},
			RemoteDnsIpv4:     []string{"192.0.2.53"},
			LocalDnsIpv4:      []string{"192.0.2.54"},
			TlsConfig:         &tls.Config{RootCAs: pool},
		}
		// the "tunnel" dialer: plaintext :53 goes to the fake remote dns server, the
		// remote doh https dial passes through to its local address
		settings.DialContextSettings = &DialContextSettings{
			DialContext: func(dialCtx context.Context, network string, addr string) (net.Conn, error) {
				if strings.HasSuffix(addr, ":53") {
					return (&net.Dialer{}).DialContext(dialCtx, network, remoteDns.addr())
				}
				return (&net.Dialer{}).DialContext(dialCtx, network, addr)
			},
		}
		cache := NewDohCache(settings)
		// the local plain-dns stage dials the configured server ip on :53 (unbindable
		// in a test); stand in the fake local dns server for the host-side resolver
		cache.localResolver = localDns.resolver()
		return cache
	}

	query := func(label string, domain string, want netip.Addr) {
		t.Helper()
		addrs, authoritative := newCache().QueryResult(ctx, "A", domain)
		if !authoritative || !slices.Contains(addrs, want) {
			t.Fatalf("%s: QueryResult = %v (authoritative %t), want %s", label, addrs, authoritative, want)
		}
	}

	// every stage healthy: remote doh answers, nothing falls through
	query("remote doh first", "one.chain.test", remoteDohIp)
	if got := localDohRequests.Load() + remoteDns.requests.Load() + localDns.requests.Load(); got != 0 {
		t.Fatalf("later stages touched while remote doh answers: %d requests", got)
	}

	// remote doh failing: local doh answers, plain dns untouched
	remoteDohFailing.Store(true)
	remoteDohBefore := remoteDohRequests.Load()
	query("local doh fallback", "two.chain.test", localDohIp)
	if remoteDohRequests.Load() == remoteDohBefore {
		t.Fatal("remote doh was not attempted before falling through")
	}
	if got := remoteDns.requests.Load() + localDns.requests.Load(); got != 0 {
		t.Fatalf("plain dns touched while local doh answers: %d requests", got)
	}

	// both doh stages failing: remote dns answers, local dns untouched
	localDohFailing.Store(true)
	localDohBefore := localDohRequests.Load()
	query("remote dns fallback", "three.chain.test", remoteDnsIp)
	if localDohRequests.Load() == localDohBefore {
		t.Fatal("local doh was not attempted before falling through")
	}
	if got := localDns.requests.Load(); got != 0 {
		t.Fatalf("local dns touched while remote dns answers: %d requests", got)
	}

	// every earlier stage failing: local dns answers
	remoteDns.failing.Store(true)
	remoteDnsBefore := remoteDns.requests.Load()
	query("local dns fallback", "four.chain.test", localDnsIp)
	if remoteDns.requests.Load() == remoteDnsBefore {
		t.Fatal("remote dns was not attempted before falling through")
	}
}

// TestRegionalDnsResolverSettings: the regional recommendation is remote-dns-only
// (plaintext :53 through the tunnel, for regions where the DoH defaults don't work)
// using that region's known-working servers; unknown regions have no recommendation.
func TestRegionalDnsResolverSettings(t *testing.T) {
	rs := RegionalDnsResolverSettings("cn")
	if rs == nil {
		t.Fatal("cn should have a regional recommendation")
	}
	if !rs.EnableRemoteDns || rs.EnableRemoteDoh || rs.EnableLocalDoh || rs.EnableLocalDns {
		t.Fatalf("the regional recommendation must be remote-dns-only: %+v", rs)
	}
	if len(rs.RemoteDnsIpv4) == 0 || !slices.Equal(rs.RemoteDnsIpv4, RegionalDnsServerIps("cn")) {
		t.Fatalf("regional servers mismatch: %v vs %v", rs.RemoteDnsIpv4, RegionalDnsServerIps("cn"))
	}
	if rs.DnsUpgradeMaskAddress != DefaultDnsUpgradeMaskAddress {
		t.Fatalf("regional upgrade mask = %q, expected %q", rs.DnsUpgradeMaskAddress, DefaultDnsUpgradeMaskAddress)
	}

	// country codes are case-insensitive
	if upper := RegionalDnsResolverSettings("CN"); upper == nil || !slices.Equal(upper.RemoteDnsIpv4, rs.RemoteDnsIpv4) {
		t.Fatalf("country code lookup must be case-insensitive: %+v", upper)
	}

	// no recommendation -> nil, so callers fall back to the universal resolver
	if rs := RegionalDnsResolverSettings("zz"); rs != nil {
		t.Fatalf("zz should have no regional recommendation: %+v", rs)
	}

	// every configured regional server address must parse
	for _, server := range RegionalDnsServers() {
		if _, err := netip.ParseAddr(server.Ipv4); err != nil {
			t.Errorf("regional server %s (%s) has a malformed address %q", server.Name, server.CountryCode, server.Ipv4)
		}
	}
}

func TestDefaultDnsUpgradeMaskAddress(t *testing.T) {
	if DefaultDnsUpgradeMaskAddress != "65.49.70.65" {
		t.Fatalf("default upgrade mask = %q", DefaultDnsUpgradeMaskAddress)
	}
	resolver := DefaultDnsResolverSettings()
	if resolver.DnsUpgradeMaskAddress != DefaultDnsUpgradeMaskAddress {
		t.Fatalf("default resolver upgrade mask = %q", resolver.DnsUpgradeMaskAddress)
	}
	muxSettings := DefaultUpgradeMuxSettings()
	if muxSettings == nil || muxSettings.Dns == nil || muxSettings.Dns.Resolver == nil {
		t.Fatalf("default mux resolver missing: %+v", muxSettings)
	}
	if muxSettings.Dns.Resolver.DnsUpgradeMaskAddress != DefaultDnsUpgradeMaskAddress {
		t.Fatalf("default mux upgrade mask = %q", muxSettings.Dns.Resolver.DnsUpgradeMaskAddress)
	}
}

// TestUpgradeMuxRemoteDnsModeServerNames: with the remote plain-dns mode on the mux
// (the regional recommendation shape), a client dns query egresses plaintext :53
// THROUGH the tunnel to the configured server, the client gets the synthesized reply,
// and the resolved ip→name lands in the mux's reverse index (ServerNames).
func TestUpgradeMuxRemoteDnsModeServerNames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queryName = "regional.example.test"
	resolvedIp := netip.MustParseAddr("203.0.113.60")

	var mu sync.Mutex
	answered := false
	var mux *UpgradeMux

	// the fake remote side: answer the plaintext dns egress of the tunnel. unknown
	// names (e.g. resolv.conf search-domain variants) get a fast NXDOMAIN so exotic
	// host resolver configs don't stall the lookup.
	upstream := func(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool {
		ipPath, payload, err := ParseIpPathWithPayload(packet)
		if err != nil {
			return true
		}
		if ipPath.Protocol != IpProtocolUdp || ipPath.DestinationPort != 53 {
			return true
		}
		name, ok := dnsQuestionName(payload)
		if !ok {
			return true
		}
		if name == queryName {
			if ipPath.DestinationIp.String() != "192.0.2.53" {
				t.Errorf("query egressed to %s, want the configured remote dns server", ipPath.DestinationIp)
			}
			mu.Lock()
			answered = true
			mu.Unlock()
		}
		respPayload, ok := answerPlainDns(payload, map[string]netip.Addr{queryName: resolvedIp}, false)
		if !ok {
			return true
		}
		respPath := ipPath.Reverse()
		// deliver the remote answer back through the tunnel
		go mux.Receive(source, provideMode, respPath, ipOosUdpPacket(respPath, respPayload))
		return true
	}

	rec := &ipMuxRecorder{}
	settings := DefaultUpgradeMuxSettings()
	settings.Dns.Resolver = &DnsResolverSettings{
		EnableRemoteDns: true,
		RemoteDnsIpv4:   []string{"192.0.2.53"},
	}
	// no local fallback: resolution must flow through the tunnel only
	settings.Dns.Fallback = nil

	m, err := NewUpgradeMux(ctx, TransferPath{}, protocol.ProvideMode_Network, 0, rec.receive, settings, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	mux = m
	mux.SetUpstream(upstream)

	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, dnsQueryPacket(t, queryName+"."), 0) {
		t.Fatal("SendPacket returned false; the DNS query was not claimed")
	}

	if !waitForCondition(20*time.Second, func() bool {
		_, received := rec.counts()
		return 0 < received && slices.Contains(mux.ServerNames(resolvedIp.String()), queryName)
	}) {
		mu.Lock()
		wasAnswered := answered
		mu.Unlock()
		if !wasAnswered {
			t.Fatal("the query never egressed the tunnel to the remote dns server")
		}
		t.Fatalf("reply or reverse index missing: ServerNames(%s) = %v", resolvedIp, mux.ServerNames(resolvedIp.String()))
	}

	// the synthesized client reply carries the remote-dns answer
	port, id, addrs := parseDnsReply(t, rec.receivedPackets()[0])
	if port != 33333 || id != 0x1234 {
		t.Fatalf("reply port/id = %d/%04x, want the client's 33333/1234", port, id)
	}
	if !slices.Contains(addrs, resolvedIp) {
		t.Fatalf("reply resolves %v, want %s", addrs, resolvedIp)
	}
}

// TestUpgradeMuxLocalDnsModeServerNames: with the local plain-dns mode on the mux, a
// client dns query resolves host-side (nothing egresses the tunnel), the client gets
// the synthesized reply, and the resolved ip→name lands in the mux's reverse index.
func TestUpgradeMuxLocalDnsModeServerNames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queryName = "local-dns.example.test"
	resolvedIp := netip.MustParseAddr("203.0.113.61")
	localDns := newFakeDnsServer(t, map[string]netip.Addr{
		queryName: resolvedIp,
	})
	defer localDns.close()

	rec := &ipMuxRecorder{}
	settings := DefaultUpgradeMuxSettings()
	settings.Dns.Resolver = &DnsResolverSettings{
		EnableLocalDns: true,
		LocalDnsIpv4:   []string{"192.0.2.54"},
	}
	settings.Dns.Fallback = nil

	mux, err := NewUpgradeMux(ctx, TransferPath{}, protocol.ProvideMode_Network, 0, rec.receive, settings, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Close()
	mux.SetUpstream(rec.upstream)
	// the local plain-dns stage dials the configured server ip on :53 (unbindable in a
	// test); stand in the fake server for the tun resolver's host-side resolver
	mux.mux.Tun().DohCache().localResolver = localDns.resolver()

	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, dnsQueryPacket(t, queryName+"."), 0) {
		t.Fatal("SendPacket returned false; the DNS query was not claimed")
	}

	if !waitForCondition(10*time.Second, func() bool {
		_, received := rec.counts()
		return 0 < received
	}) {
		t.Fatal("no downstream reply for the local dns resolution")
	}
	_, _, addrs := parseDnsReply(t, rec.receivedPackets()[0])
	if !slices.Contains(addrs, resolvedIp) {
		t.Fatalf("reply resolves %v, want %s", addrs, resolvedIp)
	}
	if localDns.requests.Load() == 0 {
		t.Fatal("the local dns server was never queried")
	}
	// the resolution is host-side: nothing egresses the tunnel
	if sent, _ := rec.counts(); sent != 0 {
		t.Fatalf("local dns resolution egressed %d packets through the tunnel, want 0", sent)
	}
	// the resolved ip→name reaches the reverse index
	if names := mux.ServerNames(resolvedIp.String()); !slices.Contains(names, queryName) {
		t.Fatalf("ServerNames(%s) = %v, want to contain %s", resolvedIp, names, queryName)
	}
}

// TestUpgradeMuxDnsLocalFallbackServerNames: when the handicapped local fallback
// resolver (the local-egress lookup mode) answers because the tunnel resolver can't,
// its answer also lands in the mux's reverse index, so fallback-resolved flows are
// named like tunnel-resolved ones.
func TestUpgradeMuxDnsLocalFallbackServerNames(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const queryName = "fallback-names.example.test"
	const resolvedIp = "203.0.113.62"

	// the tunnel resolver always fails
	tunnelServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer tunnelServer.Close()
	tunnelPool := x509.NewCertPool()
	tunnelPool.AddCert(tunnelServer.Certificate())

	// the local fallback resolves to a fixed ip
	fallbackServer, fallbackResolver := newDohWireServer(t, resolvedIp)
	defer fallbackServer.Close()

	rec := &ipMuxRecorder{}
	settings := DefaultUpgradeMuxSettings()
	settings.Dns.Resolver = &DnsResolverSettings{
		EnableLocalDoh:   true,
		LocalDohUrlsIpv4: []string{tunnelServer.URL},
		TlsConfig:        &tls.Config{RootCAs: tunnelPool},
	}
	settings.Dns.Fallback = fallbackResolver
	settings.Dns.LocalFallbackTimeout = 200 * time.Millisecond

	mux, err := NewUpgradeMux(ctx, TransferPath{}, protocol.ProvideMode_Network, 0, rec.receive, settings, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Close()
	mux.SetUpstream(rec.upstream)

	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, dnsQueryPacket(t, queryName+"."), 0) {
		t.Fatal("SendPacket returned false; the DNS query was not claimed")
	}

	if !waitForCondition(10*time.Second, func() bool {
		_, received := rec.counts()
		return 0 < received && slices.Contains(mux.ServerNames(resolvedIp), queryName)
	}) {
		_, received := rec.counts()
		t.Fatalf("fallback answer not recorded: received=%d ServerNames(%s)=%v", received, resolvedIp, mux.ServerNames(resolvedIp))
	}
	_, _, addrs := parseDnsReply(t, rec.receivedPackets()[0])
	if !slices.Contains(addrs, netip.MustParseAddr(resolvedIp)) {
		t.Fatalf("reply resolves %v, want %s", addrs, resolvedIp)
	}
}
