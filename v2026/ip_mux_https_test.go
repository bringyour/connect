package connect

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"github.com/urnetwork/connect/v2026/protocol"
)

func httpsQuestion(domain string) dnsmessage.Question {
	return dnsmessage.Question{
		Name:  dnsmessage.MustNewName(domain + "."),
		Type:  dnsTypeHttps,
		Class: dnsmessage.ClassINET,
	}
}

// buildHttpsAnswer builds a DNS response echoing q with a single HTTPS answer carrying the
// given ipv4hint/ipv6hint SvcParams.
func buildHttpsAnswer(t *testing.T, id uint16, q dnsmessage.Question, v4 []string, v6 []string) []byte {
	t.Helper()
	b := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: id, Response: true, RecursionAvailable: true})
	if err := b.StartQuestions(); err != nil {
		t.Fatal(err)
	}
	if err := b.Question(q); err != nil {
		t.Fatal(err)
	}
	if err := b.StartAnswers(); err != nil {
		t.Fatal(err)
	}
	rh := dnsmessage.ResourceHeader{Name: q.Name, Class: dnsmessage.ClassINET, TTL: 300}
	var r dnsmessage.HTTPSResource
	r.Priority = 1
	r.Target = q.Name
	if 0 < len(v4) {
		var v []byte
		for _, s := range v4 {
			a := netip.MustParseAddr(s).As4()
			v = append(v, a[:]...)
		}
		r.SetParam(dnsmessage.SVCParamIPv4Hint, v)
	}
	if 0 < len(v6) {
		var v []byte
		for _, s := range v6 {
			a := netip.MustParseAddr(s).As16()
			v = append(v, a[:]...)
		}
		r.SetParam(dnsmessage.SVCParamIPv6Hint, v)
	}
	if err := b.HTTPSResource(rh, r); err != nil {
		t.Fatal(err)
	}
	resp, err := b.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func TestParseHttpsHints(t *testing.T) {
	// v4 + v6 hints from one HTTPS record (v4 first, then v6, per appendSvcbHints)
	resp := buildHttpsAnswer(t, 0, httpsQuestion("cdn.example"),
		[]string{"104.16.1.1", "104.16.2.2"}, []string{"2606:4700::1"})
	got := parseHttpsHints(resp)
	want := []netip.Addr{
		netip.MustParseAddr("104.16.1.1"),
		netip.MustParseAddr("104.16.2.2"),
		netip.MustParseAddr("2606:4700::1"),
	}
	if !slices.Equal(got, want) {
		t.Fatalf("parseHttpsHints = %v, want %v", got, want)
	}

	// an HTTPS record with no hints yields nothing
	if h := parseHttpsHints(buildHttpsAnswer(t, 0, httpsQuestion("nohints.example"), nil, nil)); len(h) != 0 {
		t.Fatalf("expected no hints, got %v", h)
	}

	// malformed input must not panic and yields no hints
	for _, bad := range [][]byte{nil, {0x00}, {0x00, 0x01, 0x02, 0x03}, make([]byte, 12)} {
		if h := parseHttpsHints(bad); len(h) != 0 {
			t.Fatalf("malformed input produced hints: %v", h)
		}
	}
}

func TestDnsResponseUsable(t *testing.T) {
	mk := func(rcode byte) []byte {
		b := make([]byte, 12)
		b[3] = rcode
		return b
	}
	if !dnsResponseUsable(mk(0)) { // NOERROR
		t.Fatal("NOERROR should be usable")
	}
	if !dnsResponseUsable(mk(3)) { // NXDOMAIN
		t.Fatal("NXDOMAIN should be usable")
	}
	if dnsResponseUsable(mk(2)) { // SERVFAIL
		t.Fatal("SERVFAIL should not be usable")
	}
	if dnsResponseUsable(mk(5)) { // REFUSED
		t.Fatal("REFUSED should not be usable")
	}
	if dnsResponseUsable([]byte{0x00}) {
		t.Fatal("a short response should not be usable")
	}
}

// TestDohCacheForward verifies the raw SVCB/HTTPS forward: the cache POSTs the query to the
// remote DoH server and returns the raw response wire (from which hints parse), and does not
// cache it (each call re-queries).
func TestDohCacheForward(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var requestCount int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requestCount, 1)
		raw, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get("dns"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var p dnsmessage.Parser
		header, err := p.Start(raw)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		q, err := p.Question()
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/dns-message")
		w.Write(buildHttpsAnswer(t, header.ID, q, []string{"104.16.1.1"}, nil))
	}))
	defer server.Close()

	settings := DefaultDohSettings()
	settings.RequestTimeout = 2 * time.Second
	settings.DnsResolverSettings.EnableRemoteDoh = true
	settings.DnsResolverSettings.EnableRemoteDns = false
	settings.DnsResolverSettings.EnableLocalDns = false
	settings.DnsResolverSettings.RemoteDohUrlsIpv4 = []string{server.URL}
	dohCache := NewDohCache(settings)

	resp, ok := dohCache.Forward(ctx, dnsTypeHttps, "cdn.example")
	if !ok {
		t.Fatal("Forward returned ok=false")
	}
	if hints := parseHttpsHints(resp); !slices.Contains(hints, netip.MustParseAddr("104.16.1.1")) {
		t.Fatalf("forwarded response hints = %v, want to contain 104.16.1.1", hints)
	}

	// the forward is best-effort and uncached: a second call re-queries the server
	if _, ok := dohCache.Forward(ctx, dnsTypeHttps, "cdn.example"); !ok {
		t.Fatal("second Forward returned ok=false")
	}
	AssertEqual(t, int32(2), atomic.LoadInt32(&requestCount))
}

func TestDohCacheForwardLocalPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get("dns"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var parser dnsmessage.Parser
		header, err := parser.Start(raw)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		question, err := parser.Question()
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/dns-message")
		w.Write(buildHttpsAnswer(t, header.ID, question, []string{"104.16.3.3"}, nil))
	}))
	defer server.Close()

	settings := DefaultDohSettings()
	settings.RequestTimeout = 2 * time.Second
	settings.DnsResolverSettings.EnableRemoteDoh = false
	settings.DnsResolverSettings.EnableLocalDoh = true
	settings.DnsResolverSettings.LocalDohUrlsIpv4 = []string{server.URL}
	cache := NewDohCache(settings)
	defer cache.Close()

	response, ok := cache.Forward(ctx, dnsTypeHttps, "local.example")
	if !ok {
		t.Fatal("Forward returned ok=false for the configured local DoH path")
	}
	if hints := parseHttpsHints(response); !slices.Contains(hints, netip.MustParseAddr("104.16.3.3")) {
		t.Fatalf("forwarded response hints = %v, want to contain 104.16.3.3", hints)
	}
}

// TestDohCacheForwardHedgesSlowProvider guards the browser-specific stall
// case: Chromium waits on HTTPS/SVCB before opening an origin, so one stuck
// first-choice provider must not serialize the whole public-page fan-out.
func TestDohCacheForwardHedgesSlowProvider(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var requestCount atomic.Int32
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch requestCount.Add(1) {
		case 1:
			close(firstStarted)
			// Model a half-open h2/provider request. The winning hedge must
			// cancel it rather than leaving it alive until RequestTimeout.
			<-r.Context().Done()
			return
		case 2:
			close(secondStarted)
		}
		raw, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get("dns"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var p dnsmessage.Parser
		header, err := p.Start(raw)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		q, err := p.Question()
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/dns-message")
		w.Write(buildHttpsAnswer(t, header.ID, q, []string{"104.16.2.2"}, nil))
	})
	a := httptest.NewServer(handler)
	defer a.Close()
	b := httptest.NewServer(handler)
	defer b.Close()

	settings := DefaultDohSettings()
	settings.RequestTimeout = 3 * time.Second
	settings.DohServerStagger = 2 * time.Second
	settings.DohServerWarmStagger = 40 * time.Millisecond
	settings.DohServerRaceMaxInFlight = 0
	settings.DohServerHedgeReserve = 1
	settings.DohPathWarm = func() bool { return true }
	settings.MaxServersPerQuery = 2
	settings.MaxConcurrentHttpRequests = 2
	settings.DnsResolverSettings.EnableRemoteDoh = true
	settings.DnsResolverSettings.EnableRemoteDns = false
	settings.DnsResolverSettings.EnableLocalDns = false
	settings.DnsResolverSettings.RemoteDohUrlsIpv4 = []string{a.URL, b.URL}
	cache := NewDohCache(settings)
	defer cache.Close()

	start := time.Now()
	response, ok := cache.Forward(ctx, dnsTypeHttps, "hedged.example")
	if !ok {
		t.Fatal("Forward returned ok=false")
	}
	if elapsed := time.Since(start); elapsed >= 500*time.Millisecond {
		t.Fatalf("hedged Forward took %s, want <500ms (serial path waits 2s)", elapsed)
	}
	if hints := parseHttpsHints(response); !slices.Contains(hints, netip.MustParseAddr("104.16.2.2")) {
		t.Fatalf("forwarded response hints = %v, want to contain 104.16.2.2", hints)
	}
	select {
	case <-firstStarted:
	default:
		t.Fatal("first provider was not queried")
	}
	select {
	case <-secondStarted:
	default:
		t.Fatal("slow first provider was not hedged")
	}
	if got := requestCount.Load(); got != 2 {
		t.Fatalf("provider requests=%d, want exactly 2", got)
	}
}

// TestDohCacheForwardQuietRace verifies that an isolated HTTPS/SVCB lookup
// receives the same zero-delay hedge as A/AAAA. This catches a future split
// where opaque forwards silently fall back to serial provider probing.
func TestDohCacheForwardQuietRace(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var requestCount atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requestCount.Add(1) == 1 {
			<-r.Context().Done()
			return
		}
		raw, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get("dns"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var p dnsmessage.Parser
		header, err := p.Start(raw)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		q, err := p.Question()
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/dns-message")
		w.Write(buildHttpsAnswer(t, header.ID, q, []string{"104.16.3.3"}, nil))
	})
	a := httptest.NewServer(handler)
	defer a.Close()
	b := httptest.NewServer(handler)
	defer b.Close()

	settings := DefaultDohSettings()
	settings.RequestTimeout = 3 * time.Second
	settings.DohServerStagger = 2 * time.Second
	settings.DohServerWarmStagger = 2 * time.Second
	settings.DohServerRaceMaxInFlight = 1
	settings.MaxServersPerQuery = 2
	settings.MaxConcurrentHttpRequests = 2
	settings.DnsResolverSettings.EnableRemoteDoh = true
	settings.DnsResolverSettings.EnableRemoteDns = false
	settings.DnsResolverSettings.EnableLocalDns = false
	settings.DnsResolverSettings.RemoteDohUrlsIpv4 = []string{a.URL, b.URL}
	cache := NewDohCache(settings)
	defer cache.Close()

	start := time.Now()
	if _, ok := cache.Forward(ctx, dnsTypeHttps, "quiet-race.example"); !ok {
		t.Fatal("Forward returned ok=false")
	}
	if elapsed := time.Since(start); elapsed >= 500*time.Millisecond {
		t.Fatalf("quiet Forward took %s, want immediate hedge instead of 2s stagger", elapsed)
	}
	if got := requestCount.Load(); got != 2 {
		t.Fatalf("provider requests=%d, want exactly 2", got)
	}
}

// TestUpgradeMuxHttpsLocalFallback verifies that opaque browser records get
// the same bounded startup/stall fallback as A/AAAA. A half-open tunnel query
// must not hold Chromium's origin bundle until its own DNS deadline, and the
// winning local response must cancel that tunnel request.
func TestUpgradeMuxHttpsLocalFallback(t *testing.T) {
	tunnelStarted := make(chan struct{})
	tunnelCanceled := make(chan struct{})
	var tunnelStartedOnce sync.Once
	var tunnelCanceledOnce sync.Once
	tunnelServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tunnelStartedOnce.Do(func() { close(tunnelStarted) })
		<-r.Context().Done()
		tunnelCanceledOnce.Do(func() { close(tunnelCanceled) })
	}))
	defer tunnelServer.Close()
	tunnelPool := x509.NewCertPool()
	tunnelPool.AddCert(tunnelServer.Certificate())

	var fallbackRequests atomic.Int32
	fallbackServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackRequests.Add(1)
		raw, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get("dns"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var parser dnsmessage.Parser
		header, err := parser.Start(raw)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		question, err := parser.Question()
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/dns-message")
		w.Write(buildHttpsAnswer(t, header.ID, question, []string{"203.0.113.65"}, nil))
	}))
	defer fallbackServer.Close()
	fallbackPool := x509.NewCertPool()
	fallbackPool.AddCert(fallbackServer.Certificate())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := &ipMuxRecorder{}
	settings := DefaultUpgradeMuxSettings()
	settings.Dns.Resolver = &DnsResolverSettings{
		EnableLocalDoh:   true,
		LocalDohUrlsIpv4: []string{tunnelServer.URL},
		TlsConfig:        &tls.Config{RootCAs: tunnelPool},
	}
	settings.Dns.Fallback = &DnsResolverSettings{
		EnableLocalDoh:   true,
		LocalDohUrlsIpv4: []string{fallbackServer.URL},
		TlsConfig:        &tls.Config{RootCAs: fallbackPool},
	}
	settings.Dns.LocalFallbackTimeout = 200 * time.Millisecond
	settings.Dns.ColdLocalFallbackTimeout = 0
	settings.Dns.ResolveTimeout = 5 * time.Second
	mux, err := NewUpgradeMux(
		ctx,
		TransferPath{},
		protocol.ProvideMode_Network,
		0,
		rec.receive,
		settings,
		NewNoopLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Close()
	mux.SetUpstream(rec.upstream)

	start := time.Now()
	if !mux.SendPacket(
		TransferPath{},
		protocol.ProvideMode_Network,
		dnsQueryPacketTyped(t, "fallback-https.example.test.", dnsTypeHttps, 0x6501),
		0,
	) {
		t.Fatal("HTTPS query was not claimed")
	}
	select {
	case <-tunnelStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("tunnel HTTPS request did not start")
	}
	if !waitForCondition(2*time.Second, func() bool {
		_, received := rec.counts()
		return received == 1
	}) {
		t.Fatal("local HTTPS fallback did not answer")
	}
	if elapsed := time.Since(start); 2*time.Second <= elapsed {
		t.Fatalf("local HTTPS fallback took %s, want <2s", elapsed)
	}
	select {
	case <-tunnelCanceled:
	case <-time.After(2 * time.Second):
		t.Fatal("local HTTPS fallback did not cancel the losing tunnel request")
	}
	if got := fallbackRequests.Load(); got != 1 {
		t.Fatalf("fallback requests=%d, want 1", got)
	}

	packet := rec.receivedPackets()[0]
	_, payload, err := ParseIpPathWithPayload(packet)
	if err != nil {
		t.Fatal(err)
	}
	header, question, _ := parseDnsBlockedReply(t, packet)
	if header.RCode != dnsmessage.RCodeSuccess || question.Type != dnsTypeHttps {
		t.Fatalf("fallback reply rcode/type = %v/%v, want NOERROR/HTTPS", header.RCode, question.Type)
	}
	if hints := parseHttpsHints(payload); !slices.Contains(hints, netip.MustParseAddr("203.0.113.65")) {
		t.Fatalf("fallback HTTPS hints=%v, want 203.0.113.65", hints)
	}
	if !waitForCondition(time.Second, func() bool {
		mux.inflightLock.Lock()
		defer mux.inflightLock.Unlock()
		return mux.inflight[NewDohKey(dnsTypeHttps.String(), "fallback-https.example.test")] == nil
	}) {
		t.Fatal("fallback HTTPS flight did not retire after the tunnel worker canceled")
	}
}

// TestUpgradeMuxHttpsTunnelWinnerSkipsLocalFallback guards the privacy side
// of the fallback race: a healthy tunnel response proves the path and stops
// the delayed host-egress request before it can leak the question.
func TestUpgradeMuxHttpsTunnelWinnerSkipsLocalFallback(t *testing.T) {
	tunnelServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get("dns"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var parser dnsmessage.Parser
		header, err := parser.Start(raw)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		question, err := parser.Question()
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/dns-message")
		w.Write(buildHttpsAnswer(t, header.ID, question, []string{"203.0.113.66"}, nil))
	}))
	defer tunnelServer.Close()
	tunnelPool := x509.NewCertPool()
	tunnelPool.AddCert(tunnelServer.Certificate())

	var fallbackRequests atomic.Int32
	fallbackServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fallbackRequests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer fallbackServer.Close()
	fallbackPool := x509.NewCertPool()
	fallbackPool.AddCert(fallbackServer.Certificate())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rec := &ipMuxRecorder{}
	settings := DefaultUpgradeMuxSettings()
	settings.Dns.Resolver = &DnsResolverSettings{
		EnableLocalDoh:   true,
		LocalDohUrlsIpv4: []string{tunnelServer.URL},
		TlsConfig:        &tls.Config{RootCAs: tunnelPool},
	}
	settings.Dns.Fallback = &DnsResolverSettings{
		EnableLocalDoh:   true,
		LocalDohUrlsIpv4: []string{fallbackServer.URL},
		TlsConfig:        &tls.Config{RootCAs: fallbackPool},
	}
	settings.Dns.LocalFallbackTimeout = 300 * time.Millisecond
	settings.Dns.ColdLocalFallbackTimeout = 0
	settings.Dns.ResolveTimeout = 3 * time.Second
	mux, err := NewUpgradeMux(
		ctx,
		TransferPath{},
		protocol.ProvideMode_Network,
		0,
		rec.receive,
		settings,
		NewNoopLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Close()
	mux.SetUpstream(rec.upstream)

	if !mux.SendPacket(
		TransferPath{},
		protocol.ProvideMode_Network,
		dnsQueryPacketTyped(t, "tunnel-https.example.test.", dnsTypeHttps, 0x6502),
		0,
	) {
		t.Fatal("HTTPS query was not claimed")
	}
	if !waitForCondition(2*time.Second, func() bool {
		_, received := rec.counts()
		return received == 1
	}) {
		t.Fatal("healthy tunnel HTTPS response was not delivered")
	}
	time.Sleep(500 * time.Millisecond)
	if got := fallbackRequests.Load(); got != 0 {
		t.Fatalf("healthy tunnel leaked %d HTTPS requests to the local fallback", got)
	}
	if mux.tunnelDohCold() {
		t.Fatal("successful tunnel HTTPS response did not prove the DoH path warm")
	}
}

// TestUpgradeMuxHttpsForwardFanOut covers the forward pipeline's delivery half (the
// tunnel-DoH round-trip can't be driven in a connect unit test — TestDohCacheForward
// covers that): a forwarded record is fanned out to every coalesced responder with its
// own transaction id, and its hint addresses are recorded into the reverse index.
func TestUpgradeMuxHttpsForwardFanOut(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rec := &ipMuxRecorder{}
	mux, err := NewUpgradeMux(ctx, TransferPath{}, protocol.ProvideMode_Network, 0, rec.receive, DefaultUpgradeMuxSettings(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Close()
	mux.SetUpstream(rec.upstream)

	const domain = "cdn.example.test"
	const hintIp = "104.16.9.9"

	mkResponder := func(id uint16, srcPort int, questionDomain string) dnsResponder {
		path := &IpPath{
			Version:         4,
			Protocol:        IpProtocolUdp,
			SourceIp:        net.ParseIP("10.0.0.1"),
			SourcePort:      srcPort,
			DestinationIp:   net.ParseIP("10.0.0.53"),
			DestinationPort: 53,
		}
		return dnsResponder{
			id:          id,
			question:    httpsQuestion(questionDomain),
			source:      TransferPath{},
			provideMode: protocol.ProvideMode_Network,
			reverse:     path.Reverse(),
		}
	}
	// two clients coalesced onto one flight for the same HTTPS question
	key := NewDohKey("HTTPS", domain)
	fl := mux.attachDnsResponder(key, mkResponder(0x1111, 40001, "CdN.Example.Test"))
	if fl == nil {
		t.Fatal("expected a new flight for the first responder")
	}
	mux.attachDnsResponder(key, mkResponder(0x2222, 40002, "cDn.eXAMPLE.tEST"))

	response := buildHttpsAnswer(t, 0, httpsQuestion(domain), []string{hintIp}, nil)
	mux.fanOutRawForward(key, fl, domain, response)

	// The responder snapshot and flight removal are atomic: a query attaching
	// after completion starts a new flight instead of joining a responder list
	// that has already been delivered.
	next := mux.attachDnsResponder(key, mkResponder(0x3333, 40003, "CDN.example.test"))
	if next == nil || next == fl {
		t.Fatal("post-completion query did not start a fresh HTTPS flight")
	}

	// both coalesced clients get the record, each stamped with its own transaction id
	if !waitForCondition(2*time.Second, func() bool { _, r := rec.counts(); return r == 2 }) {
		_, r := rec.counts()
		t.Fatalf("delivered %d replies, want 2 (one per coalesced responder)", r)
	}
	ids := map[uint16]bool{}
	questions := map[uint16]string{}
	for _, packet := range rec.receivedPackets() {
		_, payload, err := ParseIpPathWithPayload(packet)
		if err != nil {
			t.Fatalf("reply parse: %v", err)
		}
		var p dnsmessage.Parser
		h, err := p.Start(payload)
		if err != nil {
			t.Fatalf("dns parse: %v", err)
		}
		ids[h.ID] = true
		q, err := p.Question()
		if err != nil {
			t.Fatalf("dns question parse: %v", err)
		}
		questions[h.ID] = q.Name.String()
		// the delivered payload is the forwarded HTTPS record (its hint round-trips)
		if hints := parseHttpsHints(payload); !slices.Contains(hints, netip.MustParseAddr(hintIp)) {
			t.Fatalf("delivered reply missing the HTTPS hint: %v", hints)
		}
	}
	if !ids[0x1111] || !ids[0x2222] {
		t.Fatalf("delivered transaction ids = %v, want both 0x1111 and 0x2222", ids)
	}
	if questions[0x1111] != "CdN.Example.Test." || questions[0x2222] != "cDn.eXAMPLE.tEST." {
		t.Fatalf("delivered question casing = %v", questions)
	}

	// the hint ip -> domain was recorded, so a flow to the hint reports the server name
	if names := mux.ServerNames(hintIp); !slices.Contains(names, domain) {
		t.Fatalf("ServerNames(%s) = %v, want to contain %s", hintIp, names, domain)
	}
}

func TestRawDnsFlightAccountingPrecedesGenerationRetirement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	accounted := make(chan struct{})
	releaseAccounting := make(chan struct{})
	settings := DefaultUpgradeMuxSettings()
	rec := &ipMuxRecorder{}
	mux, err := NewUpgradeMux(
		ctx,
		TransferPath{},
		protocol.ProvideMode_Network,
		0,
		rec.receive,
		settings,
		NewNoopLogger(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Close()
	mux.SetUpstream(rec.upstream)
	mux.firstLoad.lock.Lock()
	mux.firstLoad.dnsDoneHook = func() {
		close(accounted)
		<-releaseAccounting
	}
	mux.firstLoad.lock.Unlock()

	path := &IpPath{
		Version:         4,
		Protocol:        IpProtocolUdp,
		SourceIp:        net.ParseIP("10.0.0.1"),
		SourcePort:      40001,
		DestinationIp:   net.ParseIP("10.0.0.53"),
		DestinationPort: 53,
	}
	responder := dnsResponder{
		id:          0x1111,
		question:    httpsQuestion("generation.example.test"),
		source:      TransferPath{},
		provideMode: protocol.ProvideMode_Network,
		reverse:     path.Reverse(),
	}
	key := NewDohKey("HTTPS", "generation.example.test")
	fl := mux.attachDnsResponder(key, responder)
	if fl == nil {
		t.Fatal("first generation was not admitted")
	}
	mux.firstLoad.dnsStart(key)
	response := buildHttpsAnswer(t, 0, responder.question, []string{"203.0.113.8"}, nil)

	done := make(chan struct{})
	go func() {
		defer close(done)
		mux.fanOutRawForward(
			key,
			fl,
			"generation.example.test",
			response,
		)
	}()
	select {
	case <-accounted:
	case <-time.After(time.Second):
		t.Fatal("first-load accounting did not complete")
	}

	mux.inflightLock.Lock()
	stillOwned := mux.inflight[key] == fl && fl.replied && fl.accounted
	mux.inflightLock.Unlock()
	if !stillOwned {
		t.Fatal("DNS flight generation retired before its accounting completed")
	}
	if next := mux.attachDnsResponder(key, responder); next != nil {
		t.Fatal("a new generation started while old accounting still owned the key")
	}

	close(releaseAccounting)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("raw DNS completion did not retire after accounting resumed")
	}
	next := mux.attachDnsResponder(key, responder)
	if next == nil || next == fl {
		t.Fatal("new DNS generation was not admitted after owned retirement")
	}
	mux.retireDnsFlight(key, next)
}
