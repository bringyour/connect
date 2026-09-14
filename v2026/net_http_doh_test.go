package connect

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"testing"
)

// writeDohWire answers the RFC 8484 wire query in r with the given records (A or AAAA matching the
// question type), or NXDOMAIN when nxdomain is set. An unparseable request gets a 400.
func writeDohWire(w http.ResponseWriter, r *http.Request, records []netip.Addr, ttl uint32, nxdomain bool) {
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
	h := dnsmessage.Header{ID: header.ID, Response: true, RecursionAvailable: true}
	if nxdomain {
		h.RCode = dnsmessage.RCodeNameError
	}
	b := dnsmessage.NewBuilder(nil, h)
	b.StartQuestions()
	b.Question(q)
	b.StartAnswers()
	rh := dnsmessage.ResourceHeader{Name: q.Name, Class: dnsmessage.ClassINET, TTL: ttl}
	for _, ip := range records {
		switch {
		case q.Type == dnsmessage.TypeA && ip.Is4():
			b.AResource(rh, dnsmessage.AResource{A: ip.As4()})
		case q.Type == dnsmessage.TypeAAAA && ip.Is6() && !ip.Is4In6():
			b.AAAAResource(rh, dnsmessage.AAAAResource{AAAA: ip.As16()})
		}
	}
	resp, err := b.Finish()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/dns-message")
	w.Write(resp)
}

func TestDohLaunchStaggerClosedStopCancels(t *testing.T) {
	stop := make(chan struct{})
	close(stop)
	if waitDohLaunchStagger(context.Background(), stop, time.Hour) {
		t.Fatal("closed stop channel must cancel the stagger wait")
	}
}

func TestDohLaunchStaggerCanceledContextCancels(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if waitDohLaunchStagger(ctx, make(chan struct{}), time.Hour) {
		t.Fatal("canceled context must cancel the stagger wait")
	}
}

func TestDohLaunchStaggerTimerAdmitsHedge(t *testing.T) {
	if !waitDohLaunchStagger(context.Background(), make(chan struct{}), time.Nanosecond) {
		t.Fatal("timer expiry must admit the next hedge")
	}
}

func TestDohLaunchStaggerCancellationAtTimerBoundary(t *testing.T) {
	// Exercise the expiry/cancellation boundary concurrently. The historical
	// Stop-then-drain pattern could select cancellation, observe Stop == false,
	// and then wait forever for a value from Go 1.23+'s synchronous timer
	// channel. Either timer or cancellation may win; every waiter must return.
	const waiterCount = 1024
	start := make(chan struct{})
	completed := make(chan struct{}, waiterCount)
	for i := range waiterCount {
		go func(i int) {
			<-start
			if i%2 == 0 {
				stop := make(chan struct{})
				close(stop)
				waitDohLaunchStagger(context.Background(), stop, time.Nanosecond)
			} else {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				waitDohLaunchStagger(ctx, make(chan struct{}), time.Nanosecond)
			}
			completed <- struct{}{}
		}(i)
	}
	close(start)

	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for i := 0; i < waiterCount; i++ {
		select {
		case <-completed:
		case <-deadline.C:
			t.Fatalf("only %d/%d boundary waiters returned", i, waiterCount)
		}
	}
}

func TestDohQuery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultDohSettings()

	testIp1, err := netip.ParseAddr("1.1.1.1")
	AssertEqual(t, err, nil)
	testIp2, err := netip.ParseAddr("10.10.10.10")
	AssertEqual(t, err, nil)

	for range 10 {
		ips := DohQuery(ctx, 4, "A", settings, "test1.bringyour.com")
		if len(ips) == 0 {
			// timeout, try again
			fmt.Printf("[doh]timeout. Will wait 1s and try again ...\n")
			select {
			case <-time.After(1 * time.Second):
				continue
			}
		}
		AssertEqual(t, len(ips), 2)
		ttl1 := ips[testIp1]
		AssertNotEqual(t, ttl1, 0)
		ttl2 := ips[testIp2]
		AssertNotEqual(t, ttl2, 0)
	}

}

func TestDohCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultDohSettings()

	dohCache := NewDohCache(settings)

	testIp1, err := netip.ParseAddr("1.1.1.1")
	AssertEqual(t, err, nil)
	testIp2, err := netip.ParseAddr("10.10.10.10")
	AssertEqual(t, err, nil)

	for range 10 {
		ips := dohCache.Query(ctx, "A", "test1.bringyour.com")
		if len(ips) == 0 {
			// timeout, try again
			fmt.Printf("[doh]timeout. Will wait 1s and try again ...\n")
			select {
			case <-time.After(1 * time.Second):
				continue
			}
		}
		AssertEqual(t, len(ips), 2)
		AssertEqual(t, slices.Contains(ips, testIp1), true)
		AssertEqual(t, slices.Contains(ips, testIp2), true)
	}

	for range 10 {
		ips := dohCache.Query(ctx, "A", "test-local.bringyour.com")
		AssertEqual(t, len(ips), 0)
	}

}

func TestDohCacheCachesMiss(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var requestCount int32
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&requestCount, 1)
			writeDohWire(w, r, nil, 0, true) // NXDOMAIN
		}))
		defer server.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 1 * time.Second
		settings.MissExpiration = 1 * time.Minute
		settings.DnsResolverSettings.EnableRemoteDoh = true
		settings.DnsResolverSettings.EnableRemoteDns = false
		settings.DnsResolverSettings.EnableLocalDns = false
		setRemoteDohUrls(settings, ipVersion, server.URL)

		dohCache := NewDohCache(settings)

		for range 3 {
			ips := dohCache.Query(ctx, testDnsRecordType(ipVersion), "missing.example")
			AssertEqual(t, len(ips), 0)
		}
		AssertEqual(t, int32(1), atomic.LoadInt32(&requestCount))
	})
}

func TestDohCacheReportsTunnelRouteForAResult(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		answer := testDocAddr(ipVersion, 65)
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeDohWire(w, r, []netip.Addr{answer}, 300, false)
		}))
		defer server.Close()

		type observedResult struct {
			domain string
			addrs  []netip.Addr
			route  *DohRoute
		}
		observed := make(chan observedResult, 1)
		settings := DefaultDohSettings()
		settings.RequestTimeout = time.Second
		settings.DnsResolverSettings.EnableRemoteDoh = true
		settings.DnsResolverSettings.EnableRemoteDns = false
		settings.DnsResolverSettings.EnableLocalDoh = false
		settings.DnsResolverSettings.EnableLocalDns = false
		setRemoteDohUrls(settings, ipVersion, server.URL)
		// An owner-supplied dialer is the signal that this cache rides a tunnel;
		// use the host dialer in the test while retaining the same observation path.
		settings.DialContextSettings = &DialContextSettings{
			DialContext: (&net.Dialer{}).DialContext,
		}
		settings.DohResultCallback = func(domain string, addrs []netip.Addr, route *DohRoute) {
			observed <- observedResult{domain: domain, addrs: addrs, route: route}
		}

		cache := NewDohCache(settings)
		defer cache.Close()
		addrs, authoritative := cache.QueryResult(context.Background(), testDnsRecordType(ipVersion), "smtp.example.test")
		if !authoritative || !slices.Equal(addrs, []netip.Addr{answer}) {
			t.Fatalf("A result = %v authoritative=%v, want %v true", addrs, authoritative, answer)
		}
		select {
		case result := <-observed:
			if result.domain != "smtp.example.test" || !slices.Equal(result.addrs, []netip.Addr{answer}) {
				t.Fatalf("observed result = %+v", result)
			}
			if result.route == nil || !result.route.Local.IsValid() || !result.route.Remote.IsValid() {
				t.Fatalf("observed route = %+v, want valid tunnel tuple", result.route)
			}
		case <-time.After(time.Second):
			t.Fatal("successful A answer did not report its tunnel route")
		}
	})
}

type dohRouteConn struct {
	net.Conn
	local  net.Addr
	remote net.Addr
}

func (self *dohRouteConn) LocalAddr() net.Addr {
	return self.local
}

func (self *dohRouteConn) RemoteAddr() net.Addr {
	return self.remote
}

// Adapts one deterministic callback into an HTTP transport.
type dohRoundTripperFunc func(*http.Request) (*http.Response, error)

// Delegates each request to the deterministic test callback.
func (self dohRoundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return self(request)
}

type dohCanceledReadBlockingCloseBody struct {
	ctx          context.Context
	readEntered  chan struct{}
	closeEntered chan struct{}
	releaseClose chan struct{}
}

func (self *dohCanceledReadBlockingCloseBody) Read([]byte) (int, error) {
	close(self.readEntered)
	<-self.ctx.Done()
	return 0, self.ctx.Err()
}

func (self *dohCanceledReadBlockingCloseBody) Close() error {
	close(self.closeEntered)
	<-self.releaseClose
	return nil
}

type dohCloseRecordingBody struct {
	read       func([]byte) (int, error)
	closeCount int
}

func (self *dohCloseRecordingBody) Read(buffer []byte) (int, error) {
	return self.read(buffer)
}

func (self *dohCloseRecordingBody) Close() error {
	self.closeCount += 1
	return nil
}

// DoH uses HTTP/2 too, and fan-out cancellation can interrupt a response body
// read after its headers arrived. That request context owns stream cleanup;
// synchronously closing the canceled body can otherwise strand the query
// worker and its concurrency and memory reservations forever.
func TestDohCanceledResponseReadDoesNotBlockOnClose(t *testing.T) {
	testCtx, testCancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer testCancel()

	readEntered := make(chan struct{})
	closeEntered := make(chan struct{})
	releaseClose := make(chan struct{})
	var releaseCloseOnce sync.Once
	releaseBlockedClose := func() {
		releaseCloseOnce.Do(func() { close(releaseClose) })
	}
	t.Cleanup(releaseBlockedClose)

	responseCh := make(chan *http.Response, 1)
	client := &dohClient{httpClient: &http.Client{Transport: dohRoundTripperFunc(func(
		request *http.Request,
	) (*http.Response, error) {
		response := &http.Response{
			Status:     "200 OK",
			StatusCode: http.StatusOK,
			Body: &dohCanceledReadBlockingCloseBody{
				ctx:          request.Context(),
				readEntered:  readEntered,
				closeEntered: closeEntered,
				releaseClose: releaseClose,
			},
			Request: request,
		}
		responseCh <- response
		return response, nil
	})}}
	requestCtx, requestCancel := context.WithCancel(context.Background())
	defer requestCancel()
	resultCh := make(chan error, 1)
	go func() {
		_, _, err := client.queryWireRawDetailedWithRoute(
			requestCtx,
			"https://doh.example.test/dns-query",
			dnsmessage.TypeA,
			"cancel.example.test",
		)
		resultCh <- err
	}()

	var response *http.Response
	select {
	case <-testCtx.Done():
		t.Fatalf("wait for DoH response body read: %v", testCtx.Err())
	case <-readEntered:
		response = <-responseCh
	}
	requestCancel()

	select {
	case err := <-resultCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("DoH canceled read error = %v, expected context cancellation", err)
		}
	case <-closeEntered:
		releaseBlockedClose()
		select {
		case <-resultCh:
		case <-testCtx.Done():
			t.Fatalf("release blocked DoH response close: %v", testCtx.Err())
		}
		t.Fatal("DoH synchronously closed a context-canceled response body")
	case <-testCtx.Done():
		t.Fatalf("DoH response cleanup made no completion decision: %v", testCtx.Err())
	}
	if response.Request.Context().Err() == nil {
		t.Fatal("DoH response request context was not canceled")
	}
	if response.Body != nil {
		t.Fatal("canceled DoH response retained a body owned by the transport")
	}
	select {
	case <-closeEntered:
		t.Fatal("DoH closed the canceled response body")
	default:
	}
}

// Canceled cleanup is the exceptional ownership path. A response read to EOF
// and a read that fails while its request is live both retain the usual
// explicit Body.Close obligation.
func TestDohLiveResponseReadClosesBody(t *testing.T) {
	readErr := errors.New("invalid dns response framing")
	for _, test := range []struct {
		name       string
		bodyReader func([]byte) (int, error)
		wantErr    error
	}{
		{
			name:       "complete",
			bodyReader: bytes.NewReader([]byte{0x01, 0x02}).Read,
		},
		{
			name: "non-context read error",
			bodyReader: func([]byte) (int, error) {
				return 0, readErr
			},
			wantErr: readErr,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := &dohCloseRecordingBody{read: test.bodyReader}
			var response *http.Response
			client := &dohClient{httpClient: &http.Client{Transport: dohRoundTripperFunc(func(
				request *http.Request,
			) (*http.Response, error) {
				response = &http.Response{
					Status:     "200 OK",
					StatusCode: http.StatusOK,
					Body:       body,
					Request:    request,
				}
				return response, nil
			})}}

			_, _, err := client.queryWireRawDetailedWithRoute(
				context.Background(),
				"https://doh.example.test/dns-query",
				dnsmessage.TypeA,
				"ownership.example.test",
			)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("DoH read error = %v, expected %v", err, test.wantErr)
			}
			if body.closeCount != 1 {
				t.Fatalf("DoH response body close count = %d, expected 1", body.closeCount)
			}
			if response.Body != nil {
				t.Fatal("closed DoH response retained its body")
			}
		})
	}
}

// TestDohRouteForConnRejectsMissingEndpoint pins the live proxy panic: an
// HTTP/2 GotConn callback can retain a non-nil connection wrapper after one
// endpoint address has disappeared. Route metadata is optional, so either
// missing endpoint must return no route instead of dereferencing net.Addr.
func TestDohRouteForConnRejectsMissingEndpoint(t *testing.T) {
	local := &net.TCPAddr{IP: net.ParseIP("192.0.2.10"), Port: 41000}
	remote := &net.TCPAddr{IP: net.ParseIP("192.0.2.20"), Port: 443}
	var typedNilAddr *net.TCPAddr
	for _, test := range []struct {
		name string
		conn net.Conn
	}{
		{name: "nil connection"},
		{name: "nil local", conn: &dohRouteConn{remote: remote}},
		{name: "nil remote", conn: &dohRouteConn{local: local}},
		{name: "typed nil local", conn: &dohRouteConn{local: typedNilAddr, remote: remote}},
		{name: "typed nil remote", conn: &dohRouteConn{local: local, remote: typedNilAddr}},
	} {
		if route := dohRouteForConn(test.conn); route != nil {
			t.Errorf("%s: route = %+v, want nil for missing endpoint", test.name, route)
		}
	}
}

// Pins the complete live failure path: HTTP/2 invokes GotConn with a wrapper
// whose endpoint disappeared, but route observation is optional and the valid
// DoH response must still reach the resolver.
func TestDohQueryPreservesResponseWhenRouteEndpointMissing(t *testing.T) {
	responseWire := []byte{0x01, 0x02, 0x03, 0x04}
	remote := &net.TCPAddr{IP: net.ParseIP("192.0.2.20"), Port: 443}
	transport := dohRoundTripperFunc(func(request *http.Request) (*http.Response, error) {
		trace := httptrace.ContextClientTrace(request.Context())
		if trace == nil || trace.GotConn == nil {
			return nil, fmt.Errorf("request has no GotConn trace")
		}
		trace.GotConn(httptrace.GotConnInfo{
			Conn: &dohRouteConn{remote: remote},
		})
		return &http.Response{
			Status:     "200 OK",
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(responseWire)),
			Request:    request,
		}, nil
	})
	client := &dohClient{
		httpClient:   &http.Client{Transport: transport},
		captureRoute: true,
	}

	data, route, err := client.queryWireRawDetailedWithRoute(
		context.Background(),
		"https://doh.example.test/dns-query",
		dnsmessage.TypeA,
		"smtp.example.test",
	)
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if !bytes.Equal(data, responseWire) {
		t.Fatalf("response = %v, want %v", data, responseWire)
	}
	if route != nil {
		t.Fatalf("route = %+v, want no metadata for missing endpoint", route)
	}
}

// TestDohCacheDoesNotCacheHttpError: an HTTP 5xx (transient server failure, not an
// authoritative NXDOMAIN) is not negative-cached — every query re-hits the resolver (a
// cached negative would be a single request). Contrast TestDohCacheCachesMiss.
func TestDohCacheDoesNotCacheHttpError(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var requestCount int32
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&requestCount, 1)
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
		defer server.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 1 * time.Second
		settings.MissExpiration = 1 * time.Minute
		settings.DnsResolverSettings.EnableRemoteDoh = true
		settings.DnsResolverSettings.EnableRemoteDns = false
		settings.DnsResolverSettings.EnableLocalDns = false
		setRemoteDohUrls(settings, ipVersion, server.URL)

		dohCache := NewDohCache(settings)
		for range 3 {
			AssertEqual(t, len(dohCache.Query(ctx, testDnsRecordType(ipVersion), "fail.example")), 0)
		}
		AssertEqual(t, int32(3), atomic.LoadInt32(&requestCount))
	})
}

// TestDohCacheRetriesAfterTimeout: a timed-out query is not cached, so a retry after the
// resolver recovers resolves rather than returning a poisoned empty record.
func TestDohCacheRetriesAfterTimeout(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testIp := testDocAddr(ipVersion, 34)
		var failing atomic.Bool
		failing.Store(true)
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if failing.Load() {
				// stall until the client gives up (its request ctx is canceled on timeout)
				<-r.Context().Done()
				return
			}
			writeDohWire(w, r, []netip.Addr{testIp}, 60, false)
		}))
		defer server.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 500 * time.Millisecond
		settings.MissExpiration = 1 * time.Minute
		settings.DnsResolverSettings.EnableRemoteDoh = true
		settings.DnsResolverSettings.EnableRemoteDns = false
		settings.DnsResolverSettings.EnableLocalDns = false
		setRemoteDohUrls(settings, ipVersion, server.URL)

		dohCache := NewDohCache(settings)
		// first query times out -> empty, and must not be cached
		AssertEqual(t, len(dohCache.Query(ctx, testDnsRecordType(ipVersion), "recover.example")), 0)
		// resolver recovers; the retry must re-query and resolve, not return a cached empty
		failing.Store(false)
		ips := dohCache.Query(ctx, testDnsRecordType(ipVersion), "recover.example")
		AssertEqual(t, len(ips), 1)
		AssertEqual(t, slices.Contains(ips, testIp), true)
	})
}

// TestDohCacheSingleFlight: concurrent identical queries coalesce onto a single upstream
// resolution rather than each firing its own DoH request (retry-storm / dup amplification).
func TestDohCacheSingleFlight(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testIp := testDocAddr(ipVersion, 34)
		var requestCount int32
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&requestCount, 1)
			// hold the request so concurrent callers overlap and coalesce onto this one
			select {
			case <-time.After(200 * time.Millisecond):
			case <-r.Context().Done():
			}
			writeDohWire(w, r, []netip.Addr{testIp}, 60, false)
		}))
		defer server.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 5 * time.Second
		settings.DnsResolverSettings.EnableRemoteDoh = true
		settings.DnsResolverSettings.EnableRemoteDns = false
		settings.DnsResolverSettings.EnableLocalDns = false
		setRemoteDohUrls(settings, ipVersion, server.URL)

		dohCache := NewDohCache(settings)

		const n = 16
		results := make(chan []netip.Addr, n)
		for range n {
			go func() {
				results <- dohCache.Query(ctx, testDnsRecordType(ipVersion), "coalesce.example")
			}()
		}
		for range n {
			addrs := <-results
			AssertEqual(t, len(addrs), 1)
			AssertEqual(t, slices.Contains(addrs, testIp), true)
		}

		// all 16 concurrent callers coalesced onto a single upstream request
		AssertEqual(t, int32(1), atomic.LoadInt32(&requestCount))
	})
}

// TestDohWireFormat: a server is queried via RFC 8484 (?dns=<base64url>, application/dns-message)
// and its wire-format response is parsed.
func TestDohWireFormat(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testIp := testDocAddr(ipVersion, 34)
		var gotWireQuery atomic.Bool
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Query().Get("dns") != "" {
				gotWireQuery.Store(true)
			}
			writeDohWire(w, r, []netip.Addr{testIp}, 60, false)
		}))
		defer server.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 5 * time.Second
		settings.DnsResolverSettings.EnableRemoteDoh = true
		settings.DnsResolverSettings.EnableRemoteDns = false
		settings.DnsResolverSettings.EnableLocalDns = false
		setRemoteDohUrls(settings, ipVersion, server.URL)

		dohCache := NewDohCache(settings)
		addrs := dohCache.Query(ctx, testDnsRecordType(ipVersion), "wire.example")
		AssertEqual(t, gotWireQuery.Load(), true)
		AssertEqual(t, len(addrs), 1)
		AssertEqual(t, slices.Contains(addrs, testIp), true)
	})
}

// TestDohFanoutFastestWins: with multiple resolvers fanned out at once (stagger disabled) a query
// returns as soon as one returns records — a slow/dead server does not delay the lookup.
func TestDohFanoutFastestWins(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testIp := testDocAddr(ipVersion, 34)
		slow := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-time.After(30 * time.Second):
			case <-r.Context().Done():
			}
		}))
		defer slow.Close()
		fast := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeDohWire(w, r, []netip.Addr{testIp}, 60, false)
		}))
		defer fast.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 8 * time.Second
		settings.DohServerStagger = 0 // fan out simultaneously to test fastest-wins
		settings.DnsResolverSettings.EnableRemoteDoh = true
		settings.DnsResolverSettings.EnableRemoteDns = false
		settings.DnsResolverSettings.EnableLocalDns = false
		setRemoteDohUrls(settings, ipVersion, slow.URL, fast.URL)

		dohCache := NewDohCache(settings)
		start := time.Now()
		addrs := dohCache.Query(ctx, testDnsRecordType(ipVersion), "fanout.example")
		elapsed := time.Since(start)

		AssertEqual(t, len(addrs), 1)
		AssertEqual(t, slices.Contains(addrs, testIp), true)
		// must not have waited on the slow server
		if elapsed > 3*time.Second {
			t.Fatalf("fan-out waited %v for the slow server; should return on the fast answer", elapsed)
		}
	})
}

// TestDohServerStagger: with the stagger enabled, a primary that answers within the stagger window
// means the next server is never launched — only one upstream request is made.
func TestDohServerStagger(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testIp := testDocAddr(ipVersion, 34)
		var totalRequests int32
		// responseDelay makes the race assertion deterministic: with instant
		// answers the first server can respond before the launcher fires the
		// second, legitimately short-circuiting the fan-out
		var responseDelayMs int32
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&totalRequests, 1)
			if delay := atomic.LoadInt32(&responseDelayMs); 0 < delay {
				select {
				case <-time.After(time.Duration(delay) * time.Millisecond):
				case <-r.Context().Done():
					return
				}
			}
			writeDohWire(w, r, []netip.Addr{testIp}, 60, false)
		})
		a := newFamilyHttptestServer(t, ipVersion, handler)
		defer a.Close()
		b := newFamilyHttptestServer(t, ipVersion, handler)
		defer b.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 5 * time.Second
		settings.DohServerStagger = 500 * time.Millisecond
		// test the stagger mechanism in isolation: disable the quiet-cache race
		// (which intentionally bypasses the stagger when little is in flight —
		// see DohServerRaceMaxInFlight)
		settings.DohServerRaceMaxInFlight = 0
		settings.DnsResolverSettings.EnableRemoteDoh = true
		settings.DnsResolverSettings.EnableRemoteDns = false
		settings.DnsResolverSettings.EnableLocalDns = false
		setRemoteDohUrls(settings, ipVersion, a.URL, b.URL)

		dohCache := NewDohCache(settings)
		addrs := dohCache.Query(ctx, testDnsRecordType(ipVersion), "stagger.example")
		AssertEqual(t, slices.Contains(addrs, testIp), true)
		// the first-ordered server answers immediately, well within the 500ms stagger, so the second
		// server is never launched
		AssertEqual(t, int32(1), atomic.LoadInt32(&totalRequests))

		// with the quiet-cache race enabled (the default), an isolated query
		// bypasses the stagger and fans out immediately (hedged request). the
		// servers delay so both launches reliably precede either answer.
		atomic.StoreInt32(&totalRequests, 0)
		atomic.StoreInt32(&responseDelayMs, 100)
		settings.DohServerRaceMaxInFlight = DefaultDohSettings().DohServerRaceMaxInFlight
		raceCache := NewDohCache(settings)
		addrs = raceCache.Query(ctx, testDnsRecordType(ipVersion), "race.example")
		AssertEqual(t, slices.Contains(addrs, testIp), true)
		AssertEqual(t, int32(2), atomic.LoadInt32(&totalRequests))
	})
}

func TestDohServerStaggerTracksWarmPathState(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testIp := testDocAddr(ipVersion, 34)
		var totalRequests atomic.Int32
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			totalRequests.Add(1)
			select {
			case <-time.After(200 * time.Millisecond):
			case <-r.Context().Done():
				return
			}
			writeDohWire(w, r, []netip.Addr{testIp}, 60, false)
		})
		a := newFamilyHttptestServer(t, ipVersion, handler)
		defer a.Close()
		b := newFamilyHttptestServer(t, ipVersion, handler)
		defer b.Close()

		var pathWarm atomic.Bool
		settings := DefaultDohSettings()
		settings.RequestTimeout = 2 * time.Second
		settings.DohServerStagger = 300 * time.Millisecond
		settings.DohServerWarmStagger = 50 * time.Millisecond
		settings.DohServerRaceMaxInFlight = 0
		settings.DohPathWarm = pathWarm.Load
		settings.MaxServersPerQuery = 2
		settings.MaxConcurrentHttpRequests = 4
		settings.DohServerHedgeReserve = 1
		settings.DnsResolverSettings.EnableRemoteDoh = true
		settings.DnsResolverSettings.EnableRemoteDns = false
		settings.DnsResolverSettings.EnableLocalDns = false
		setRemoteDohUrls(settings, ipVersion, a.URL, b.URL)

		cache := NewDohCache(settings)
		defer cache.Close()

		// Cold: the first 200ms answer beats the conservative 300ms stagger.
		addrs := cache.Query(ctx, testDnsRecordType(ipVersion), "cold-stagger.example")
		if !slices.Contains(addrs, testIp) {
			t.Fatalf("cold query missing %s: %v", testIp, addrs)
		}
		if got := totalRequests.Load(); got != 1 {
			t.Fatalf("cold path launched %d requests, want 1", got)
		}

		// Warm: the 50ms override launches the hedge before either 200ms answer.
		pathWarm.Store(true)
		totalRequests.Store(0)
		addrs = cache.Query(ctx, testDnsRecordType(ipVersion), "warm-stagger.example")
		if !slices.Contains(addrs, testIp) {
			t.Fatalf("warm query missing %s: %v", testIp, addrs)
		}
		if got := totalRequests.Load(); got != 2 {
			t.Fatalf("warm path launched %d requests, want 2", got)
		}
	})
}

func TestDohWarmPrimaryWaveReservesHedgeCapacity(t *testing.T) {
	settings := DefaultDohSettings()
	settings.MaxConcurrentHttpRequests = 32
	settings.DohServerHedgeReserve = 4
	settings.DohPathWarm = func() bool { return true }
	cache := NewDohCache(settings)
	defer cache.Close()

	if got := cap(cache.remoteClient.httpSem); got != 32 {
		t.Fatalf("http capacity=%d, want 32", got)
	}
	if got := cap(cache.remoteClient.primarySem); got != 28 {
		t.Fatalf("warm primary capacity=%d, want 28", got)
	}
	if cache.remoteClient.primarySem != cache.localClient.primarySem {
		t.Fatal("remote/local clients must share one primary-wave reserve")
	}
}

func TestDohQuietRaceAdmissionIsPredictablyBounded(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		const queryCount = 8
		const raceMax = 2
		// Every query launches its first server immediately; only raceMax queries
		// may bypass the stagger and launch their second server too.
		const expectedImmediateRequests = queryCount + raceMax

		testIp := testDocAddr(ipVersion, 34)
		release := make(chan struct{})
		var releaseOnce sync.Once
		defer releaseOnce.Do(func() { close(release) })
		requestSeen := make(chan struct{}, 2*queryCount)
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requestSeen <- struct{}{}
			select {
			case <-release:
				writeDohWire(w, r, []netip.Addr{testIp}, 60, false)
			case <-r.Context().Done():
			}
		})
		a := newFamilyHttptestServer(t, ipVersion, handler)
		defer a.Close()
		b := newFamilyHttptestServer(t, ipVersion, handler)
		defer b.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 5 * time.Second
		settings.DohServerStagger = 2 * time.Second
		settings.DohServerRaceMaxInFlight = raceMax
		settings.MaxServersPerQuery = 2
		settings.MaxConcurrentHttpRequests = 2 * queryCount
		settings.MaxConcurrentResolutions = queryCount
		settings.DnsResolverSettings.EnableRemoteDoh = true
		settings.DnsResolverSettings.EnableRemoteDns = false
		settings.DnsResolverSettings.EnableLocalDns = false
		setRemoteDohUrls(settings, ipVersion, a.URL, b.URL)

		dohCache := NewDohCache(settings)
		defer dohCache.Close()
		start := make(chan struct{})
		results := make(chan []netip.Addr, queryCount)
		for i := range queryCount {
			go func(i int) {
				<-start
				results <- dohCache.Query(ctx, testDnsRecordType(ipVersion), fmt.Sprintf("race-bound-%d.example", i))
			}(i)
		}
		close(start)

		timer := time.NewTimer(time.Second)
		defer timer.Stop()
		for i := 0; i < expectedImmediateRequests; i++ {
			select {
			case <-requestSeen:
			case <-timer.C:
				t.Fatalf("saw only %d/%d immediate requests", i, expectedImmediateRequests)
			}
		}
		// Well before the two-second stagger, no additional query may have raced.
		select {
		case <-requestSeen:
			t.Fatalf("more than %d requests bypassed the stagger", expectedImmediateRequests)
		case <-time.After(150 * time.Millisecond):
		}

		releaseOnce.Do(func() { close(release) })
		for range queryCount {
			select {
			case addrs := <-results:
				if !slices.Contains(addrs, testIp) {
					t.Fatalf("query result missing %s: %v", testIp, addrs)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("timed out waiting for query results")
			}
		}
	})
}

// TestDohHttpConcurrencyLimit: MaxConcurrentHttpRequests hard-caps concurrent in-flight DoH
// requests across a cache, regardless of how wide the fan-out is.
func TestDohHttpConcurrencyLimit(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testIp := testDocAddr(ipVersion, 34)
		var inFlight, maxInFlight int32
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := atomic.AddInt32(&inFlight, 1)
			for {
				m := atomic.LoadInt32(&maxInFlight)
				if n <= m || atomic.CompareAndSwapInt32(&maxInFlight, m, n) {
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
			atomic.AddInt32(&inFlight, -1)
			writeDohWire(w, r, []netip.Addr{testIp}, 60, false)
		}))
		defer server.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 5 * time.Second
		settings.DohServerStagger = 0 // fan out simultaneously
		settings.MaxConcurrentHttpRequests = 2
		settings.DnsResolverSettings.EnableRemoteDoh = true
		settings.DnsResolverSettings.EnableRemoteDns = false
		settings.DnsResolverSettings.EnableLocalDns = false
		// six servers fanned out at once; only MaxConcurrentHttpRequests may be in flight together
		urls := make([]string, 6)
		for i := range urls {
			urls[i] = server.URL
		}
		setRemoteDohUrls(settings, ipVersion, urls...)

		dohCache := NewDohCache(settings)
		addrs := dohCache.Query(ctx, testDnsRecordType(ipVersion), "concurrency.example")
		AssertEqual(t, slices.Contains(addrs, testIp), true)
		if got := atomic.LoadInt32(&maxInFlight); got > 2 {
			t.Fatalf("max concurrent in-flight requests = %d, want <= 2", got)
		}
	})
}

// TestDohCacheMinTtl: a record with a very low (here zero) DoH TTL is cached for at least
// MinCacheTtl, so it isn't re-resolved (a full fan-out) on nearly every query.
func TestDohCacheMinTtl(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		testIp := testDocAddr(ipVersion, 34)
		var requestCount int32
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&requestCount, 1)
			writeDohWire(w, r, []netip.Addr{testIp}, 0, false)
		}))
		defer server.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 5 * time.Second
		settings.MinCacheTtl = 2 * time.Second
		settings.DnsResolverSettings.EnableRemoteDoh = true
		settings.DnsResolverSettings.EnableRemoteDns = false
		settings.DnsResolverSettings.EnableLocalDns = false
		setRemoteDohUrls(settings, ipVersion, server.URL)

		dohCache := NewDohCache(settings)

		addrs := dohCache.Query(ctx, testDnsRecordType(ipVersion), "lowttl.example")
		AssertEqual(t, len(addrs), 1)
		// past the record's real TTL (0) but within MinCacheTtl — still served from cache
		time.Sleep(500 * time.Millisecond)
		addrs = dohCache.Query(ctx, testDnsRecordType(ipVersion), "lowttl.example")
		AssertEqual(t, len(addrs), 1)
		AssertEqual(t, slices.Contains(addrs, testIp), true)
		// the floor kept it cached: a single upstream request despite the zero TTL
		AssertEqual(t, int32(1), atomic.LoadInt32(&requestCount))
	})
}

// TestServerStatsTokenBucket: a server's score is the summed trailing-window success count, which
// decays as time passes without new successes.
func TestServerStatsTokenBucket(t *testing.T) {
	stats := newServerStats()
	base := time.Unix(1_700_000_000, 0)
	const url = "https://r.example/dns-query"

	// three successes now -> counted in all three windows (5/15/60m): score 3+3+3 = 9
	for range 3 {
		stats.recordAt(url, true, base)
	}
	score := func(now time.Time) float64 {
		stats.lock.Lock()
		defer stats.lock.Unlock()
		return stats.scoreLocked(url, now)
	}
	AssertEqual(t, score(base), float64(9))

	// failures earn nothing
	stats.recordAt(url, false, base)
	AssertEqual(t, score(base), float64(9))

	// some time later the score has decayed but is still positive (longer windows still count)
	mid := score(base.Add(6 * time.Minute))
	if !(0 < mid && mid < 9) {
		t.Fatalf("score after 6m = %v, want in (0,9)", mid)
	}

	// well past the longest window every bucket has aged out -> score 0
	AssertEqual(t, score(base.Add(3*time.Hour)), float64(0))
}

// TestServerStatsOrderBias: the weighted-random order favors the server with the stronger recent
// success history the large majority of the time, while the floor still lets others be tried.
func TestServerStatsOrderBias(t *testing.T) {
	stats := newServerStats()
	now := time.Unix(1_700_000_000, 0)
	good := "https://good.example/dns-query"
	bad := "https://bad.example/dns-query"
	for range 20 {
		stats.recordAt(good, true, now)
	}

	urls := []string{bad, good}
	goodFirst := 0
	for range 1000 {
		if stats.orderAt(urls, now)[0] == good {
			goodFirst++
		}
	}
	// good's weight (~60) dwarfs bad's floor (0.05), so it should lead the vast majority
	if goodFirst < 900 {
		t.Fatalf("good server led %d/1000, want >= 900", goodFirst)
	}
}

// TestDohMaxServersPerQuery: a query's fan-out is capped at MaxServersPerQuery servers (in
// weighted order); 0 fans out to all. On a dead path every launched request hangs holding
// memory until the deadline, so a memory-constrained host caps this.
func TestDohMaxServersPerQuery(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		newFailingServer := func(hits *int32) *httptest.Server {
			return newFamilyHttptestTlsServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				atomic.AddInt32(hits, 1)
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
		}
		var hitsA, hitsB int32
		serverA := newFailingServer(&hitsA)
		defer serverA.Close()
		serverB := newFailingServer(&hitsB)
		defer serverB.Close()

		pool := x509.NewCertPool()
		pool.AddCert(serverA.Certificate())
		pool.AddCert(serverB.Certificate())

		newSettings := func(maxServers int) *DohSettings {
			settings := DefaultDohSettings()
			settings.RequestTimeout = 5 * time.Second
			settings.DohServerStagger = 1 * time.Millisecond
			settings.MaxServersPerQuery = maxServers
			settings.DnsResolverSettings = localDohResolverSettings(ipVersion, serverA.URL, serverB.URL)
			settings.DnsResolverSettings.TlsConfig = &tls.Config{RootCAs: pool}
			return settings
		}

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// capped: exactly one server is tried (both fail, so nothing short-circuits the wave)
		NewDohCache(newSettings(1)).QueryResult(ctx, testDnsRecordType(ipVersion), "cap.example.test")
		if got := atomic.LoadInt32(&hitsA) + atomic.LoadInt32(&hitsB); got != 1 {
			t.Fatalf("with MaxServersPerQuery=1, servers hit = %d, want 1", got)
		}

		// uncapped: the fan-out reaches both servers
		atomic.StoreInt32(&hitsA, 0)
		atomic.StoreInt32(&hitsB, 0)
		NewDohCache(newSettings(0)).QueryResult(ctx, testDnsRecordType(ipVersion), "nocap.example.test")
		if got := atomic.LoadInt32(&hitsA) + atomic.LoadInt32(&hitsB); got != 2 {
			t.Fatalf("with MaxServersPerQuery=0, servers hit = %d, want 2", got)
		}
	})
}

// TestDohCacheShedMemory: shedding drops the query result cache (a later query re-resolves)
// and leaves the cache usable.
func TestDohCacheShedMemory(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		var requests int32
		ip := testDocAddr(ipVersion, 31)
		server := newFamilyHttptestTlsServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&requests, 1)
			writeDohWire(w, r, []netip.Addr{ip}, 300, false)
		}))
		defer server.Close()
		pool := x509.NewCertPool()
		pool.AddCert(server.Certificate())

		settings := DefaultDohSettings()
		settings.RequestTimeout = 5 * time.Second
		settings.DnsResolverSettings = localDohResolverSettings(ipVersion, server.URL)
		settings.DnsResolverSettings.TlsConfig = &tls.Config{RootCAs: pool}
		cache := NewDohCache(settings)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		assertResolves := func(label string) {
			addrs, authoritative := cache.QueryResult(ctx, testDnsRecordType(ipVersion), "shed.example.test")
			if !authoritative || !slices.Contains(addrs, ip) {
				t.Fatalf("%s: QueryResult = %v (authoritative %t), want %s", label, addrs, authoritative, ip)
			}
		}

		assertResolves("first query")
		assertResolves("cached query")
		if got := atomic.LoadInt32(&requests); got != 1 {
			t.Fatalf("requests before shed = %d, want 1 (second query served from cache)", got)
		}

		cache.ShedMemory()

		assertResolves("query after shed")
		if got := atomic.LoadInt32(&requests); got != 2 {
			t.Fatalf("requests after shed = %d, want 2 (the shed cache re-resolves)", got)
		}
	})
}

// expireCachedDohEntry rewinds every address expiration of the cached entry
// for (recordType, domain) to `age` in the past, simulating a record set whose
// TTL ran out that long ago -- the state serve-stale (RFC 8767) operates on.
func expireCachedDohEntry(t *testing.T, cache *DohCache, recordType string, domain string, age time.Duration) {
	t.Helper()
	key := NewDohKey(recordType, domain)
	cache.stateLock.Lock()
	defer cache.stateLock.Unlock()
	r := cache.queryResultExpiration[key]
	if r == nil {
		t.Fatalf("no cached entry for %s %s to expire", recordType, domain)
	}
	expireTime := time.Now().Add(-age)
	for addr := range r.AddrExpirations {
		r.AddrExpirations[addr] = expireTime
	}
}

// TestDohCacheServesStaleOnResolverFailure: an expired-but-retained answer is served when the
// fresh resolution fails (every resolver path errored -- the SERVFAIL shape), which is exactly
// the exit-failover moment DNS must not add to. The stale serve never suppresses the resolution
// attempt, and a later successful resolve replaces the stale answer with the fresh one.
func TestDohCacheServesStaleOnResolverFailure(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		staleIp := testDocAddr(ipVersion, 41)
		freshIp := testDocAddr(ipVersion, 42)
		var failing atomic.Bool
		var requestCount int32
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&requestCount, 1)
			if failing.Load() {
				// a transient resolver failure, NOT an authoritative answer
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			if atomic.LoadInt32(&requestCount) == 1 {
				writeDohWire(w, r, []netip.Addr{staleIp}, 60, false)
			} else {
				writeDohWire(w, r, []netip.Addr{freshIp}, 60, false)
			}
		}))
		defer server.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 1 * time.Second
		settings.DnsResolverSettings.EnableRemoteDoh = true
		settings.DnsResolverSettings.EnableRemoteDns = false
		settings.DnsResolverSettings.EnableLocalDns = false
		setRemoteDohUrls(settings, ipVersion, server.URL)

		dohCache := NewDohCache(settings)

		// resolve and cache
		addrs, authoritative := dohCache.QueryResult(ctx, testDnsRecordType(ipVersion), "stale.example")
		AssertEqual(t, authoritative, true)
		AssertEqual(t, slices.Contains(addrs, staleIp), true)

		// the record's TTL runs out, and then every resolver path fails
		expireCachedDohEntry(t, dohCache, testDnsRecordType(ipVersion), "stale.example", 1*time.Second)
		failing.Store(true)

		addrs, authoritative = dohCache.QueryResult(ctx, testDnsRecordType(ipVersion), "stale.example")
		if !slices.Contains(addrs, staleIp) {
			t.Fatalf("stale answer not served on resolver failure: %v", addrs)
		}
		if !authoritative {
			t.Error("a stale-served answer must not read as SERVFAIL to the caller")
		}
		if got := dohCache.staleServeCount.Load(); got != 1 {
			t.Errorf("staleServeCount = %d, want 1", got)
		}
		// the fresh resolution was attempted (stale never suppresses it)
		if got := atomic.LoadInt32(&requestCount); got < 2 {
			t.Errorf("requests = %d, want >= 2 (the stale serve must still attempt a fresh resolve)", got)
		}

		// the resolver recovers: the next query resolves fresh and replaces the
		// stale answer rather than keeping it
		failing.Store(false)
		addrs, authoritative = dohCache.QueryResult(ctx, testDnsRecordType(ipVersion), "stale.example")
		AssertEqual(t, authoritative, true)
		AssertEqual(t, slices.Contains(addrs, freshIp), true)
		AssertEqual(t, slices.Contains(addrs, staleIp), false)
		if got := dohCache.staleServeCount.Load(); got != 1 {
			t.Errorf("staleServeCount after recovery = %d, want 1 (fresh answers are not stale serves)", got)
		}
	})
}

// TestDohCacheStaleDoesNotOverrideAuthoritative: an authoritative NXDOMAIN wins over retained
// stale data -- serve-stale exists for resolver FAILURE only, and a name the resolver
// re-confirmed absent must not keep resolving to its dead addresses. The authoritative miss
// also replaces the retained entry (later queries hit the cached miss).
func TestDohCacheStaleDoesNotOverrideAuthoritative(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		oldIp := testDocAddr(ipVersion, 43)
		var nxdomain atomic.Bool
		var requestCount int32
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			atomic.AddInt32(&requestCount, 1)
			if nxdomain.Load() {
				writeDohWire(w, r, nil, 0, true)
			} else {
				writeDohWire(w, r, []netip.Addr{oldIp}, 60, false)
			}
		}))
		defer server.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 1 * time.Second
		settings.MissExpiration = 1 * time.Minute
		settings.DnsResolverSettings.EnableRemoteDoh = true
		settings.DnsResolverSettings.EnableRemoteDns = false
		settings.DnsResolverSettings.EnableLocalDns = false
		setRemoteDohUrls(settings, ipVersion, server.URL)

		dohCache := NewDohCache(settings)

		addrs, authoritative := dohCache.QueryResult(ctx, testDnsRecordType(ipVersion), "gone.example")
		AssertEqual(t, authoritative, true)
		AssertEqual(t, slices.Contains(addrs, oldIp), true)

		expireCachedDohEntry(t, dohCache, testDnsRecordType(ipVersion), "gone.example", 1*time.Second)
		nxdomain.Store(true)

		addrs, authoritative = dohCache.QueryResult(ctx, testDnsRecordType(ipVersion), "gone.example")
		if len(addrs) != 0 {
			t.Fatalf("stale data overrode an authoritative NXDOMAIN: %v", addrs)
		}
		AssertEqual(t, authoritative, true)
		if got := dohCache.staleServeCount.Load(); got != 0 {
			t.Errorf("staleServeCount = %d, want 0 (authoritative answers are never stale serves)", got)
		}

		// the authoritative miss replaced the retained entry: a repeat query is a
		// cache hit (no new upstream request) and stays empty
		before := atomic.LoadInt32(&requestCount)
		addrs, authoritative = dohCache.QueryResult(ctx, testDnsRecordType(ipVersion), "gone.example")
		AssertEqual(t, len(addrs), 0)
		AssertEqual(t, authoritative, true)
		AssertEqual(t, atomic.LoadInt32(&requestCount), before)
	})
}

// TestDohCacheStalePastBoundNotServed: the serve-stale bound is a hard limit -- an answer
// expired longer than dohStaleServeBound ago is not served on failure, and its entry leaves
// the cache.
func TestDohCacheStalePastBoundNotServed(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		oldIp := testDocAddr(ipVersion, 44)
		var failing atomic.Bool
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if failing.Load() {
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			writeDohWire(w, r, []netip.Addr{oldIp}, 60, false)
		}))
		defer server.Close()

		settings := DefaultDohSettings()
		settings.RequestTimeout = 1 * time.Second
		settings.DnsResolverSettings.EnableRemoteDoh = true
		settings.DnsResolverSettings.EnableRemoteDns = false
		settings.DnsResolverSettings.EnableLocalDns = false
		setRemoteDohUrls(settings, ipVersion, server.URL)

		dohCache := NewDohCache(settings)

		_, authoritative := dohCache.QueryResult(ctx, testDnsRecordType(ipVersion), "ancient.example")
		AssertEqual(t, authoritative, true)

		expireCachedDohEntry(t, dohCache, testDnsRecordType(ipVersion), "ancient.example", dohStaleServeBound+1*time.Second)
		failing.Store(true)

		addrs, authoritative := dohCache.QueryResult(ctx, testDnsRecordType(ipVersion), "ancient.example")
		AssertEqual(t, len(addrs), 0)
		AssertEqual(t, authoritative, false)
		if got := dohCache.staleServeCount.Load(); got != 0 {
			t.Errorf("staleServeCount = %d, want 0 (past the bound nothing may be served)", got)
		}

		// past-bound entries are dropped rather than retained
		key := NewDohKey(testDnsRecordType(ipVersion), "ancient.example")
		dohCache.stateLock.Lock()
		_, retained := dohCache.queryResultExpiration[key]
		dohCache.stateLock.Unlock()
		AssertEqual(t, retained, false)
	})
}

// TestDohStaleUsableBounds pins the retention predicate itself: records inside the bound are
// stale-usable, records past it are not, and an authoritative miss never is (converting a
// resolver failure into a stale "does not exist" would be the harmful direction).
func TestDohStaleUsableBounds(t *testing.T) {
	now := time.Now()
	addr := netip.MustParseAddr("203.0.113.45")

	inside := &DohResult{
		Time:            now.Add(-1 * time.Minute),
		AddrExpirations: map[netip.Addr]time.Time{addr: now.Add(-1 * time.Second)},
	}
	AssertEqual(t, inside.Valid(now, 5*time.Minute), false)
	AssertEqual(t, inside.staleUsable(now), true)

	past := &DohResult{
		Time:            now.Add(-1 * time.Hour),
		AddrExpirations: map[netip.Addr]time.Time{addr: now.Add(-dohStaleServeBound - time.Second)},
	}
	AssertEqual(t, past.staleUsable(now), false)

	miss := &DohResult{
		Time: now.Add(-1 * time.Hour),
		Miss: true,
	}
	AssertEqual(t, miss.staleUsable(now), false)
}

// TestDohStaleRetentionMemoryBound: retaining expired entries must not unbound the cache --
// pruneCacheLocked's CacheMaxEntries cap evicts oldest-first over stale entries exactly as it
// does over fresh ones, while stale-usable entries under the cap survive the validity sweep.
func TestDohStaleRetentionMemoryBound(t *testing.T) {
	settings := DefaultDohSettings()
	settings.CacheMaxEntries = 8
	cache := NewDohCache(settings)

	now := time.Now()
	addr := netip.MustParseAddr("203.0.113.46")

	cache.stateLock.Lock()
	for i := range 20 {
		key := NewDohKey("A", fmt.Sprintf("stale%d.example", i))
		cache.queryResultExpiration[key] = &DohResult{
			Time: now.Add(-time.Duration(i+1) * time.Second),
			// expired but inside the serve-stale bound: retained by the
			// validity sweep, so only the entry cap bounds them
			AddrExpirations: map[netip.Addr]time.Time{addr: now.Add(-1 * time.Second)},
		}
	}
	cache.pruneCacheLocked(now, 0)
	retained := len(cache.queryResultExpiration)
	cache.stateLock.Unlock()

	if settings.CacheMaxEntries < retained {
		t.Fatalf("stale retention broke the memory bound: %d entries > cap %d", retained, settings.CacheMaxEntries)
	}
	if retained == 0 {
		t.Fatal("the validity sweep dropped stale-usable entries: serve-stale has nothing to serve")
	}
}
