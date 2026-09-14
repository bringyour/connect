// What the reverse proxy relays and what it keeps to itself (EXTENDER.md A5).
// The proxy fetches the real site over its own exchange, so the headers of one
// hop must never cross into the other, the length of the relayed body is this
// server's to decide, and an upstream redirect is an answer to relay rather
// than one to follow.

package extender

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/textproto"
	"net/url"
	"slices"
	"strings"
	"testing"
	"time"
)

// The hop-by-hop request headers a raw http/1.1 client can put on the wire.
// Transfer-Encoding and Trailer never reach the handler: net/http consumes the
// first as the framing of the request it read, and the client never writes the
// second.
var testSendableHopByHopRequestHeaders = []string{
	"Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Upgrade",
}

// The same over h3, where quic-go drops every connection-specific field before
// it is encoded, as h3 requires.
var testSendableH3HopByHopRequestHeaders = []string{
	"Proxy-Authenticate",
	"Proxy-Authorization",
}

// One raw connection on the tcp carrier presenting serverName as the outer sni,
// so a test can read the response head the extender actually wrote rather than
// what net/http made of it.
func dialRawProxyConn(t *testing.T, fixture *extenderFixture, serverName string) *tls.Conn {
	t.Helper()
	conn, err := (&net.Dialer{}).Dial("tcp", fixture.authority(fixture.tcpPort))
	if err != nil {
		t.Fatal(err)
	}
	tlsConn := tls.Client(conn, &tls.Config{
		ServerName:         serverName,
		InsecureSkipVerify: true,
		MinVersion:         tls.VersionTLS13,
	})
	if err := tlsConn.Handshake(); err != nil {
		conn.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		tlsConn.Close()
	})
	if err := tlsConn.SetDeadline(time.Now().Add(20 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return tlsConn
}

// Writes one plain request and reads back the status line and the response
// head exactly as they were written, with none of the transfer processing a
// response reader would do.
func exchangeRawProxyRequest(
	t *testing.T,
	conn *tls.Conn,
	serverName string,
	path string,
) (string, textproto.MIMEHeader) {
	t.Helper()
	request := "GET " + path + " HTTP/1.1\r\nHost: " + serverName + "\r\n\r\n"
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatal(err)
	}
	reader := textproto.NewReader(bufio.NewReader(conn))
	statusLine, err := reader.ReadLine()
	if err != nil {
		t.Fatal(err)
	}
	head, err := reader.ReadMIMEHeader()
	if err != nil {
		t.Fatal(err)
	}
	return statusLine, head
}

// The round tripper of one protocol against this extender. A round trip is
// what the extender answered, with no redirect following of the client's own.
func newProxyRoundTripper(
	t *testing.T,
	fixture *extenderFixture,
	protocol string,
	serverName string,
) http.RoundTripper {
	t.Helper()
	if protocol == "h3" {
		h3Transport := newRawExtenderH3Transport(fixture, serverName)
		t.Cleanup(func() {
			h3Transport.Close()
		})
		return h3Transport
	}
	var nextProtos []string
	if protocol == "h2" {
		nextProtos = []string{"h2"}
	}
	client := newRawExtenderHttpClientWithServerName(fixture, serverName, nextProtos)
	t.Cleanup(client.CloseIdleConnections)
	return client.Transport
}

// The request headers the fixture site saw, lowercased so a protocol's own
// canonicalization is not part of the assertion.
func seenRequestHeaderNames(t *testing.T, body []byte) map[string]bool {
	t.Helper()
	seenHeaders := map[string][]string{}
	if err := json.Unmarshal(body, &seenHeaders); err != nil {
		t.Fatalf("the site did not echo its request headers: %v (%q)", err, body)
	}
	names := map[string]bool{}
	for name := range seenHeaders {
		names[strings.ToLower(name)] = true
	}
	return names
}

// A hop-by-hop request header belongs to the client's exchange with the
// extender and is never relayed upstream; every other header is (A5). The
// upstream leg is h2, which refuses a request carrying a connection-specific
// field at all, so a proxy that relayed them could not even reach the site.
func TestExtenderProxyDropsHopByHopRequestHeaders(t *testing.T) {
	cases := []struct {
		protocol    string
		sentHeaders []string
	}{
		{protocol: "http/1.1", sentHeaders: testSendableHopByHopRequestHeaders},
		{protocol: "h3", sentHeaders: testSendableH3HopByHopRequestHeaders},
	}
	fixture := newExtenderFixture(t, "127.0.0.1", nil)
	for _, c := range cases {
		roundTripper := newProxyRoundTripper(t, fixture, c.protocol, testSpoofName)
		request, err := http.NewRequest(http.MethodGet, "https://"+testSpoofName+"/headers", nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, hopHeader := range c.sentHeaders {
			request.Header.Set(hopHeader, testHopByHopValue)
		}
		request.Header.Set(testOrdinaryResponseHeader, testOrdinaryResponseValue)

		response, err := roundTripper.RoundTrip(request)
		if err != nil {
			t.Fatalf("%s: %v", c.protocol, err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatalf("%s: %v", c.protocol, err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatalf("%s status = %d, expected %d", c.protocol, response.StatusCode, http.StatusOK)
		}

		seenNames := seenRequestHeaderNames(t, body)
		for _, hopHeader := range proxyHopByHopHeaders {
			if seenNames[strings.ToLower(hopHeader)] {
				t.Errorf("%s relayed the hop-by-hop request header %q upstream", c.protocol, hopHeader)
			}
		}
		if !seenNames[strings.ToLower(testOrdinaryResponseHeader)] {
			t.Errorf("%s did not relay an ordinary request header upstream", c.protocol)
		}
	}

	// the names a client leg cannot carry are dropped by the same filter, which
	// the upstream request builds
	proxyRequest := &http.Request{
		Method: http.MethodGet,
		URL:    &url.URL{Path: "/headers"},
		Header: http.Header{},
	}
	for _, hopHeader := range proxyHopByHopHeaders {
		proxyRequest.Header.Set(hopHeader, testHopByHopValue)
	}
	proxyRequest.Header.Set(testOrdinaryResponseHeader, testOrdinaryResponseValue)
	upstreamRequest, err := fixture.server.proxy.newUpstreamRequest(
		context.Background(),
		proxyRequest,
		testSpoofName,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, hopHeader := range proxyHopByHopHeaders {
		if value := upstreamRequest.Header.Get(hopHeader); value != "" {
			t.Errorf("the upstream request carries the hop-by-hop header %q = %q", hopHeader, value)
		}
	}
	if value := upstreamRequest.Header.Get(testOrdinaryResponseHeader); value != testOrdinaryResponseValue {
		t.Errorf("the upstream request dropped an ordinary header: %q", value)
	}
	// the site answers as it would for a client that reached it directly
	if upstreamRequest.Host != testSpoofName {
		t.Errorf("upstream host = %q, expected %q", upstreamRequest.Host, testSpoofName)
	}
	if upstreamRequest.URL.String() != "https://"+testSpoofName+"/headers" {
		t.Errorf("upstream url = %q", upstreamRequest.URL.String())
	}
}

// The headers that belong to one hop are exactly the ones a relay must never
// carry across (A5). The list is the contract the two filters share.
func TestProxyHopByHopHeaderList(t *testing.T) {
	expected := []string{
		"Connection",
		"Keep-Alive",
		"Proxy-Authenticate",
		"Proxy-Authorization",
		"Te",
		"Trailer",
		"Transfer-Encoding",
		"Upgrade",
	}
	if !slices.Equal(proxyHopByHopHeaders, expected) {
		t.Fatalf("hop-by-hop headers = %v, expected %v", proxyHopByHopHeaders, expected)
	}
}

// A hop-by-hop response header belongs to the extender's own exchange with the
// site and is never relayed to the client; every other one is relayed verbatim
// (A5). The upstream h2 leg drops Connection, Transfer-Encoding and Trailer
// before the proxy sees them, so those names are asserted rather than
// exercised; the rest reach the proxy and must be stripped by it.
func TestExtenderProxyDropsHopByHopResponseHeaders(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", nil)

	// the raw carrier read, where the extender's own framing is visible
	conn := dialRawProxyConn(t, fixture, testSpoofName)
	statusLine, head := exchangeRawProxyRequest(t, conn, testSpoofName, "/headers")
	if !strings.Contains(statusLine, "200") {
		t.Fatalf("status line = %q, expected 200", statusLine)
	}
	for _, hopHeader := range proxyHopByHopHeaders {
		// the extender writes its own framing headers, so the relayed value is
		// what identifies a header that crossed the hop
		for _, value := range head.Values(hopHeader) {
			if value == testHopByHopValue {
				t.Errorf("http/1.1 relayed the hop-by-hop response header %q", hopHeader)
			}
		}
	}
	if value := head.Get(testOrdinaryResponseHeader); value != testOrdinaryResponseValue {
		t.Errorf("http/1.1 ordinary response header = %q, expected %q", value, testOrdinaryResponseValue)
	}

	// and over h3, which the udp carrier answers with the same handler
	roundTripper := newProxyRoundTripper(t, fixture, "h3", testSpoofName)
	request, err := http.NewRequest(http.MethodGet, "https://"+testSpoofName+"/headers", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := roundTripper.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	for _, hopHeader := range proxyHopByHopHeaders {
		for _, value := range response.Header.Values(hopHeader) {
			if value == testHopByHopValue {
				t.Errorf("h3 relayed the hop-by-hop response header %q", hopHeader)
			}
		}
	}
	if value := response.Header.Get(testOrdinaryResponseHeader); value != testOrdinaryResponseValue {
		t.Errorf("h3 ordinary response header = %q, expected %q", value, testOrdinaryResponseValue)
	}
}

// The relayed body may be cut at the response bound, so its length is the
// extender's to decide and the upstream Content-Length is never relayed (A5).
// The site answers a fixed 256 bytes with a length of its own, and the client
// is told a chunked body instead.
func TestExtenderProxyDropsTheUpstreamContentLength(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", nil)
	conn := dialRawProxyConn(t, fixture, testSpoofName)
	statusLine, head := exchangeRawProxyRequest(t, conn, testSpoofName, "/bytes?n=256")
	if !strings.Contains(statusLine, "200") {
		t.Fatalf("status line = %q, expected 200", statusLine)
	}
	if contentLength := head.Get("Content-Length"); contentLength != "" {
		t.Errorf("relayed Content-Length = %q, expected the extender to decide the length", contentLength)
	}
	if transferEncoding := head.Get("Transfer-Encoding"); transferEncoding != "chunked" {
		t.Errorf("Transfer-Encoding = %q, expected the extender's own chunked framing", transferEncoding)
	}
	// the content type of the site is an ordinary header and is relayed
	if contentType := head.Get("Content-Type"); contentType != "application/octet-stream" {
		t.Errorf("relayed Content-Type = %q, expected the site's own", contentType)
	}
}

// The reverse proxy relays one exchange: an upstream redirect reaches the
// client with its status and its location, and the extender never fetches what
// the site pointed at (A5).
func TestExtenderProxyRelaysARedirectWithoutFollowingIt(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", nil)
	for _, protocol := range testProxyProtocols {
		roundTripper := newProxyRoundTripper(t, fixture, protocol, testSpoofName)
		request, err := http.NewRequest(http.MethodGet, "https://"+testSpoofName+"/redirect", nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := roundTripper.RoundTrip(request)
		if err != nil {
			t.Fatalf("%s: %v", protocol, err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatalf("%s: %v", protocol, err)
		}
		if response.StatusCode != http.StatusFound {
			t.Fatalf("%s status = %d, expected %d", protocol, response.StatusCode, http.StatusFound)
		}
		if location := response.Header.Get("Location"); location != testRedirectLocation {
			t.Fatalf("%s location = %q, expected %q", protocol, location, testRedirectLocation)
		}
		if 0 < len(body) {
			t.Fatalf("%s relayed a body with the redirect: %q", protocol, body)
		}
	}
}

// The relayed response budget belongs to the client connection, so the streams
// of one h2 connection share it and a prober cannot restore it by opening
// another stream (A5).
func TestExtenderProxyBudgetIsSharedAcrossTheStreamsOfOneConnection(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		settings.ProxyMaxResponseByteCount = 64
	})
	client := newRawExtenderHttpClientWithServerName(fixture, testSpoofName, []string{"h2"})
	defer client.CloseIdleConnections()

	// two streams of one connection, each asking for three quarters of the
	// budget: the first is relayed whole and the second gets what is left
	cases := []struct {
		reused    bool
		byteCount int
	}{
		{reused: false, byteCount: 48},
		{reused: true, byteCount: 16},
	}
	for _, c := range cases {
		request, err := http.NewRequest(http.MethodGet, "https://"+testSpoofName+"/bytes?n=48", nil)
		if err != nil {
			t.Fatal(err)
		}
		connections := make(chan httptrace.GotConnInfo, 1)
		request = request.WithContext(httptrace.WithClientTrace(
			request.Context(),
			&httptrace.ClientTrace{
				GotConn: func(info httptrace.GotConnInfo) {
					select {
					case connections <- info:
					default:
					}
				},
			},
		))
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.ProtoMajor != 2 {
			t.Fatalf("request used HTTP/%d, expected h2", response.ProtoMajor)
		}
		select {
		case info := <-connections:
			if info.Reused != c.reused {
				t.Fatalf("connection reused = %v, expected %v", info.Reused, c.reused)
			}
		default:
			t.Fatal("the request reported no connection")
		}
		if len(body) != c.byteCount {
			t.Fatalf("relayed %d bytes, expected %d of the shared budget", len(body), c.byteCount)
		}
	}
}
