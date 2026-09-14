package connect

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

// newWarmTestDohServer starts a local h2 DoH server on the family's loopback
// answering every address query of that family with the loopback address,
// returning its /dns-query url and client tls config.
func newWarmTestDohServer(t *testing.T, ipVersion int) (*httptest.Server, string, *tls.Config) {
	t.Helper()
	server := newFamilyHttptestUnstartedServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		wire, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get("dns"))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var msg dnsmessage.Message
		if err := msg.Unpack(wire); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		msg.Response = true
		msg.Authoritative = true
		if len(msg.Questions) == 1 {
			header := dnsmessage.ResourceHeader{
				Name:  msg.Questions[0].Name,
				Type:  msg.Questions[0].Type,
				Class: dnsmessage.ClassINET,
				TTL:   60,
			}
			switch {
			case msg.Questions[0].Type == dnsmessage.TypeA && ipVersion == 4:
				msg.Answers = []dnsmessage.Resource{{
					Header: header,
					Body:   &dnsmessage.AResource{A: testLoopbackAddr(4).As4()},
				}}
			case msg.Questions[0].Type == dnsmessage.TypeAAAA && ipVersion == 6:
				msg.Answers = []dnsmessage.Resource{{
					Header: header,
					Body:   &dnsmessage.AAAAResource{AAAA: testLoopbackAddr(6).As16()},
				}}
			}
		}
		out, err := msg.Pack()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/dns-message")
		w.Write(out)
	}))
	server.EnableHTTP2 = true
	server.StartTLS()
	tlsConfig := server.Client().Transport.(*http.Transport).TLSClientConfig
	return server, server.URL + "/dns-query", tlsConfig
}

// TestDohClientSessionCache pins the TLS-resumption plumbing: every DoH http
// client carries a ClientSessionCache (TLS 1.3 PSK resumption on re-dial) and
// the long idle timeout, and a caller-provided pinned tls config is cloned,
// not mutated.
func TestDohClientSessionCache(t *testing.T) {
	settings := DefaultDohSettings()
	pinned := &tls.Config{ServerName: "pinned.example"}
	settings.DnsResolverSettings.TlsConfig = pinned

	httpClient := httpClientWithDialer(settings, settings.DialContext, tls.NewLRUClientSessionCache(dohTlsSessionCacheCapacity))
	tr := httpClient.Transport.(*http.Transport)
	if tr.TLSClientConfig.ClientSessionCache == nil {
		t.Fatalf("doh client tls config must carry a session cache")
	}
	if tr.TLSClientConfig.ServerName != "pinned.example" {
		t.Fatalf("cloned tls config must keep the pinned server name")
	}
	if pinned.ClientSessionCache != nil {
		t.Fatalf("caller tls config must not be mutated")
	}
	if tr.IdleConnTimeout != 15*time.Minute {
		t.Fatalf("idle conn timeout must be 15m, got %v", tr.IdleConnTimeout)
	}

	// a config that already has a cache keeps it
	ownCache := tls.NewLRUClientSessionCache(4)
	settings.DnsResolverSettings.TlsConfig = &tls.Config{ClientSessionCache: ownCache}
	httpClient = httpClientWithDialer(settings, settings.DialContext, tls.NewLRUClientSessionCache(dohTlsSessionCacheCapacity))
	tr = httpClient.Transport.(*http.Transport)
	if tr.TLSClientConfig.ClientSessionCache != tls.ClientSessionCache(ownCache) {
		t.Fatalf("existing session cache must be kept")
	}
}

// TestServerStatsSeed pins seed/scores: a seeded server is preferred in the
// fan-out order, the seed decays on the normal windows, and scores round-trip
// (clamped) for persistence.
func TestServerStatsSeed(t *testing.T) {
	now := time.Now()
	urls := []string{"https://a/dns-query", "https://b/dns-query"}

	stats := newServerStats()
	stats.seed(map[string]float64{urls[0]: 100.0})

	// clamped on seed
	scores := stats.scores()
	if scores[urls[0]] < dohSeedMaxScore-0.5 || dohSeedMaxScore+0.5 < scores[urls[0]] {
		t.Fatalf("seed must clamp to dohSeedMaxScore, got %v", scores[urls[0]])
	}
	if _, ok := scores[urls[1]]; ok {
		t.Fatalf("unseeded server must have no score")
	}

	// the seeded server wins the weighted order nearly always (weight ~8 vs floor 0.05)
	firstCount := 0
	trials := 200
	for range trials {
		if stats.orderAt(urls, now)[0] == urls[0] {
			firstCount += 1
		}
	}
	if firstCount < trials*9/10 {
		t.Fatalf("seeded server should lead the order, led %d/%d", firstCount, trials)
	}

	// nil-safe
	var nilStats *serverStats
	nilStats.seed(map[string]float64{urls[0]: 1})
	if nilStats.scores() != nil {
		t.Fatalf("nil stats scores must be nil")
	}
}

// TestDohCacheWarm pins Warm: it opens a connection by answering a real wire
// query, reports success, records the server into the scores (so a warm alone
// seeds the next session), and never pollutes the answer cache.
func TestDohCacheWarm(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		server, dohUrl, tlsConfig := newWarmTestDohServer(t, ipVersion)
		defer server.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 15 * time.Second
		settings.DnsResolverSettings = remoteDohResolverSettings(ipVersion, dohUrl)
		settings.DnsResolverSettings.TlsConfig = tlsConfig
		cache := NewDohCache(settings)
		defer cache.Close()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		if !cache.Warm(ctx, 2) {
			t.Fatalf("warm against a live server must succeed")
		}
		scores := cache.ServerScores()
		if scores[dohUrl] <= 0 {
			t.Fatalf("warm must record the server score, got %v", scores)
		}
		func() {
			cache.stateLock.Lock()
			defer cache.stateLock.Unlock()
			if 0 < len(cache.queryResultExpiration) {
				t.Fatalf("warm must not populate the answer cache")
			}
		}()

		// a local-doh cache (the mux fallback shape) warms over the local client
		localSettings := DefaultDohSettings()
		localSettings.RequestTimeout = 15 * time.Second
		localSettings.DnsResolverSettings = localDohResolverSettings(ipVersion, dohUrl)
		localSettings.DnsResolverSettings.TlsConfig = tlsConfig
		localCache := NewDohCache(localSettings)
		defer localCache.Close()
		if !localCache.Warm(ctx, 2) {
			t.Fatalf("local-doh warm must succeed")
		}

		// warm against nothing fails cleanly
		deadSettings := DefaultDohSettings()
		deadSettings.RequestTimeout = 1 * time.Second
		deadSettings.DnsResolverSettings = &DnsResolverSettings{}
		deadCache := NewDohCache(deadSettings)
		defer deadCache.Close()
		if deadCache.Warm(ctx, 2) {
			t.Fatalf("warm with no servers must fail")
		}
	})
}

func TestDohCacheWarmReturnsAfterFirstHealthyServer(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		slowStarted := make(chan struct{})
		slowCanceled := make(chan struct{})
		var slowStartedOnce sync.Once
		var slowCanceledOnce sync.Once
		slowServer := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			slowStartedOnce.Do(func() { close(slowStarted) })
			<-r.Context().Done()
			slowCanceledOnce.Do(func() { close(slowCanceled) })
		}))
		defer slowServer.Close()

		fastServer := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-slowStarted:
			case <-r.Context().Done():
				return
			}
			writeDohWire(w, r, []netip.Addr{testLoopbackAddr(ipVersion)}, 60, false)
		}))
		defer fastServer.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 3 * time.Second
		settings.MaxConcurrentHttpRequests = 2
		settings.ServerStatsSeed = map[string]float64{slowServer.URL: dohSeedMaxScore}
		settings.DnsResolverSettings = remoteDohResolverSettings(ipVersion, slowServer.URL, fastServer.URL)
		cache := NewDohCache(settings)
		defer cache.Close()

		start := time.Now()
		if !cache.Warm(context.Background(), 2) {
			t.Fatal("warm failed even though one server answered")
		}
		if elapsed := time.Since(start); 500*time.Millisecond <= elapsed {
			t.Fatalf("warm took %s, expected the first healthy server to win promptly", elapsed)
		}
		select {
		case <-slowCanceled:
		case <-time.After(time.Second):
			t.Fatal("healthy warm winner did not cancel the stalled sibling")
		}
	})
}

func TestDohQueryDiagnosticErrorDoesNotExposeQuestion(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		client := &dohClient{httpClient: server.Client()}
		server.Close()

		const secretDomain = "private-question.example"
		_, err := client.queryWireDetailed(
			context.Background(),
			server.URL+"/dns-query",
			testDnsRecordType(ipVersion),
			secretDomain,
		)
		if err == nil {
			t.Fatal("closed DoH server unexpectedly answered")
		}
		message := err.Error()
		if strings.Contains(message, secretDomain) || strings.Contains(message, "dns=") {
			t.Fatalf("diagnostic error exposed the encoded DNS question: %q", message)
		}
		if !strings.Contains(message, server.URL+"/dns-query") {
			t.Fatalf("diagnostic error omitted the failed DoH server: %q", message)
		}
	})
}

func TestDohCacheCloseCancelsInFlightDial(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		dialStarted := make(chan struct{}, 1)
		dialDone := make(chan struct{}, 1)

		settings := DefaultDohSettings()
		settings.RequestTimeout = 10 * time.Second
		settings.DialContextSettings = &DialContextSettings{
			DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
				select {
				case dialStarted <- struct{}{}:
				default:
				}
				<-ctx.Done()
				dialDone <- struct{}{}
				return nil, ctx.Err()
			},
		}
		settings.DnsResolverSettings = remoteDohResolverSettings(ipVersion, "https://"+testDocHostPort(ipVersion, 1, 443)+"/dns-query")

		cache := NewDohCache(settings)
		warmDone := make(chan bool, 1)
		go func() {
			warmDone <- cache.Warm(context.Background(), 1)
		}()

		select {
		case <-dialStarted:
		case <-time.After(time.Second):
			t.Fatal("warm did not start a dial")
		}

		closeDone := make(chan struct{})
		go func() {
			cache.Close()
			close(closeDone)
		}()
		select {
		case <-closeDone:
		case <-time.After(time.Second):
			t.Fatal("cache close remained blocked on an in-flight dial")
		}
		select {
		case <-dialDone:
		case <-time.After(time.Second):
			t.Fatal("cache close did not cancel the dial context")
		}
		select {
		case success := <-warmDone:
			if success {
				t.Fatal("canceled warm unexpectedly succeeded")
			}
		case <-time.After(time.Second):
			t.Fatal("warm remained blocked after cache close")
		}
	})
}

func TestDohCacheCloseCancelsInFlightPlainDnsAndRejectsNewQueries(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		dialStarted := make(chan struct{})
		dialCanceled := make(chan struct{})
		var dialStartedOnce sync.Once
		var dialCanceledOnce sync.Once
		var dialCount atomic.Int32

		settings := DefaultDohSettings()
		settings.DialContextSettings = &DialContextSettings{
			DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
				dialCount.Add(1)
				dialStartedOnce.Do(func() { close(dialStarted) })
				<-ctx.Done()
				dialCanceledOnce.Do(func() { close(dialCanceled) })
				return nil, ctx.Err()
			},
		}
		settings.DnsResolverSettings = &DnsResolverSettings{EnableRemoteDns: true}
		setRemoteDnsServers(settings, ipVersion, testDocAddr(ipVersion, 1).String())

		cache := NewDohCache(settings)
		queryDone := make(chan struct{})
		go func() {
			defer close(queryDone)
			if addrs, authoritative := cache.QueryResult(context.Background(), testDnsRecordType(ipVersion), "retired-path.example"); len(addrs) != 0 || authoritative {
				t.Errorf("canceled plain-DNS query returned %v authoritative=%v", addrs, authoritative)
			}
		}()

		select {
		case <-dialStarted:
		case <-time.After(time.Second):
			t.Fatal("plain-DNS fallback did not start its tunnel dial")
		}

		closeDone := make(chan struct{})
		go func() {
			cache.Close()
			close(closeDone)
		}()
		select {
		case <-dialCanceled:
		case <-time.After(time.Second):
			t.Fatal("cache close did not cancel the plain-DNS resolver dial")
		}
		select {
		case <-queryDone:
		case <-time.After(time.Second):
			t.Fatal("plain-DNS query remained blocked after cache close")
		}
		select {
		case <-closeDone:
		case <-time.After(time.Second):
			t.Fatal("cache close did not join the plain-DNS resolution")
		}

		dialsBefore := dialCount.Load()
		start := time.Now()
		if addrs, authoritative := cache.QueryResult(context.Background(), testDnsRecordType(ipVersion), "post-close.example"); len(addrs) != 0 || authoritative {
			t.Fatalf("retired cache answered a new query: %v authoritative=%v", addrs, authoritative)
		}
		if 100*time.Millisecond < time.Since(start) {
			t.Fatal("retired cache did not reject a new query immediately")
		}
		if dialCount.Load() != dialsBefore {
			t.Fatal("retired cache started a new plain-DNS dial")
		}
	})
}

func TestOneShotDohQueryJoinsCanceledHedgeDial(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		slowDialStarted := make(chan struct{})
		slowDialCanceled := make(chan struct{})
		releaseSlowDial := make(chan struct{})
		var slowDialStartedOnce sync.Once
		var slowDialCanceledOnce sync.Once

		server := newFamilyHttptestUnstartedServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Do not let the winning server answer until the losing hedge is
			// definitely inside DialContext. This makes the late-dial ordering
			// deterministic instead of depending on scheduler timing.
			select {
			case <-slowDialStarted:
			case <-r.Context().Done():
				return
			}
			writeDohWire(
				w,
				r,
				[]netip.Addr{testLoopbackAddr(ipVersion)},
				60,
				false,
			)
		}))
		server.EnableHTTP2 = true
		server.StartTLS()
		defer server.Close()

		fastAddress := server.Listener.Addr().String()
		slowAddress := testDocHostPort(ipVersion, 1, 443)
		settings := DefaultDohSettings()
		settings.RequestTimeout = 5 * time.Second
		settings.MaxConcurrentHttpRequests = 2
		settings.MaxServersPerQuery = 2
		settings.DohServerStagger = 0
		settings.DialContextSettings = &DialContextSettings{
			DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
				if address == slowAddress {
					slowDialStartedOnce.Do(func() { close(slowDialStarted) })
					<-ctx.Done()
					slowDialCanceledOnce.Do(func() { close(slowDialCanceled) })
					// Simulate the small but important interval in which net/http
					// has detached a reusable dial from the request that launched
					// it. The one-shot owner must join this dial before returning.
					<-releaseSlowDial
					return nil, ctx.Err()
				}
				if address != fastAddress {
					return nil, &net.AddrError{Err: "unexpected DoH address", Addr: address}
				}
				return (&net.Dialer{}).DialContext(ctx, network, address)
			},
		}
		settings.DnsResolverSettings = remoteDohResolverSettings(ipVersion, server.URL+"/dns-query", "https://"+slowAddress+"/dns-query")
		settings.DnsResolverSettings.TlsConfig = server.Client().Transport.(*http.Transport).TLSClientConfig

		queryDone := make(chan map[netip.Addr]int, 1)
		go func() {
			queryDone <- DohQuery(context.Background(), ipVersion, testDnsRecordType(ipVersion), settings, "one-shot.example")
		}()

		select {
		case <-slowDialStarted:
		case <-time.After(time.Second):
			t.Fatal("one-shot query did not launch its hedge dial")
		}
		select {
		case <-slowDialCanceled:
		case <-time.After(time.Second):
			t.Fatal("winning DoH answer did not cancel the losing hedge")
		}
		select {
		case <-queryDone:
			close(releaseSlowDial)
			t.Fatal("one-shot query returned before its canceled reusable dial exited")
		case <-time.After(50 * time.Millisecond):
		}

		close(releaseSlowDial)
		select {
		case result := <-queryDone:
			if _, ok := result[testLoopbackAddr(ipVersion)]; !ok {
				t.Fatalf("winning DoH result missing: %v", result)
			}
		case <-time.After(time.Second):
			t.Fatal("one-shot query did not return after its canceled dial exited")
		}
	})
}

// TestDohCacheSeedPersistRoundTrip pins the cross-session flow: scores from a
// used cache seed a fresh cache, which prefers the seeded server immediately.
func TestDohCacheSeedPersistRoundTrip(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		server, dohUrl, tlsConfig := newWarmTestDohServer(t, ipVersion)
		defer server.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 15 * time.Second
		settings.DnsResolverSettings = remoteDohResolverSettings(ipVersion, dohUrl)
		settings.DnsResolverSettings.TlsConfig = tlsConfig
		cache := NewDohCache(settings)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		addrs, authoritative := cache.QueryResult(ctx, testDnsRecordType(ipVersion), "seed.roundtrip.test")
		if len(addrs) == 0 || !authoritative {
			t.Fatalf("query must resolve")
		}
		scores := cache.ServerScores()
		cache.Close()
		if scores[dohUrl] <= 0 {
			t.Fatalf("used cache must have scores")
		}

		seededSettings := DefaultDohSettings()
		seededSettings.ServerStatsSeed = scores
		seededSettings.DnsResolverSettings = settings.DnsResolverSettings
		seeded := NewDohCache(seededSettings)
		defer seeded.Close()
		seededScores := seeded.ServerScores()
		if seededScores[dohUrl] <= 0 {
			t.Fatalf("seeded cache must start with the persisted score, got %v", seededScores)
		}
	})
}
