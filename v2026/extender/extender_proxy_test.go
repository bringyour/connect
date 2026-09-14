// The reverse proxy tests of EXTENDER.md section 5 phase 1: a whitelisted name
// gets the real site over every protocol the extender speaks, a name that is
// not on the whitelist gets 403, and each bound of A5 is crossed.

package extender

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026"
)

// One plain request over one protocol, answered by the extender at the given
// outer server name. The h3 case dials the quic carrier, the others the tcp
// carrier with the matching alpn.
func getThroughExtender(
	t *testing.T,
	fixture *extenderFixture,
	protocol string,
	serverName string,
	path string,
) *http.Response {
	t.Helper()
	requestUrl := "https://" + serverName + path
	switch protocol {
	case "h3":
		h3Transport := newRawExtenderH3Transport(fixture, serverName)
		t.Cleanup(func() {
			h3Transport.Close()
		})
		request, err := http.NewRequest(http.MethodGet, requestUrl, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := h3Transport.RoundTrip(request)
		if err != nil {
			t.Fatalf("%s %s: %v", protocol, serverName, err)
		}
		return response
	default:
		var nextProtos []string
		if protocol == "h2" {
			nextProtos = []string{"h2"}
		}
		client := newRawExtenderHttpClientWithServerName(fixture, serverName, nextProtos)
		t.Cleanup(client.CloseIdleConnections)
		response, err := client.Get(requestUrl)
		if err != nil {
			t.Fatalf("%s %s: %v", protocol, serverName, err)
		}
		return response
	}
}

// The protocols a plain prober can reach an extender with (A5).
var testProxyProtocols = []string{"http/1.1", "h2", "h3"}

// A whitelisted name is answered by the real site behind it, over every
// protocol, for a spoof name and for an operator pattern alike. The site
// echoes the host header it saw, which must be the requested name (A5).
func TestExtenderReverseProxiesAWhitelistedName(t *testing.T) {
	serverNames := []string{testSpoofName, "dest.example"}
	for _, family := range testLoopbackFamilies {
		fixture := newExtenderFixture(t, family.loopbackIp, nil)
		for _, protocol := range testProxyProtocols {
			for _, serverName := range serverNames {
				response := getThroughExtender(t, fixture, protocol, serverName, "/")
				body, err := io.ReadAll(response.Body)
				response.Body.Close()
				if err != nil {
					t.Fatalf("%s %s: %v", protocol, serverName, err)
				}
				if response.StatusCode != http.StatusOK {
					t.Fatalf(
						"%s %s status = %d, expected %d",
						protocol,
						serverName,
						response.StatusCode,
						http.StatusOK,
					)
				}
				if string(body) != `{"host":"`+serverName+`"}` {
					t.Fatalf("%s %s body = %q", protocol, serverName, body)
				}
			}
		}
		// the upstream pool is shared across protocols, so only the dials that
		// actually happened are observable; every one is the client's family
		proxyNetworks := fixture.forwardNetworksSeen()
		if len(proxyNetworks) == 0 {
			t.Fatalf("%s: the extender never dialed upstream", family.loopbackIp)
		}
		for _, proxyNetwork := range proxyNetworks {
			if proxyNetwork != family.forwardNetwork {
				t.Fatalf(
					"%s proxy network = %q, expected %q",
					family.loopbackIp,
					proxyNetwork,
					family.forwardNetwork,
				)
			}
		}
	}
}

// A name that is on no whitelist is refused with 403 and no body, over every
// protocol (A5).
func TestExtenderRefusesANameThatIsNotWhitelisted(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", nil)
	for _, protocol := range testProxyProtocols {
		response := getThroughExtender(t, fixture, protocol, "other.example", "/")
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatalf("%s: %v", protocol, err)
		}
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("%s status = %d, expected %d", protocol, response.StatusCode, http.StatusForbidden)
		}
		if 0 < len(body) {
			t.Fatalf("%s refusal carried a body: %q", protocol, body)
		}
	}
}

// The whitelist is the union of the spoof list and the operator patterns, with
// `*.` wildcards on the patterns, and it never admits anything else (A5).
func TestExtenderWhitelistIsTheUnionOfSpoofAndOperatorNames(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultExtenderSettings()
	settings.SpoofDomains = []string{"Spoof.Example"}
	server := NewExtenderServer(
		ctx,
		nil,
		[]string{"api.operator.example", "*.operator.example"},
		nil,
		nil,
		settings,
	)
	defer server.Close()

	cases := []struct {
		serverName  string
		whitelisted bool
	}{
		{serverName: "spoof.example", whitelisted: true},
		{serverName: "SPOOF.example", whitelisted: true},
		{serverName: "spoof.example.", whitelisted: true},
		{serverName: "www.spoof.example", whitelisted: false},
		{serverName: "api.operator.example", whitelisted: true},
		{serverName: "sub.operator.example", whitelisted: true},
		{serverName: "operator.example", whitelisted: false},
		{serverName: "other.example", whitelisted: false},
		{serverName: "", whitelisted: false},
		// tls accepts any bytes as a server name, and the name becomes the
		// upstream authority, so anything that is not a host name is refused
		// before it is matched
		{serverName: "evil.example/x.operator.example", whitelisted: false},
		{serverName: "user@evil.example.operator.example", whitelisted: false},
		{serverName: "api.operator.example:8443", whitelisted: false},
		{serverName: "sub..operator.example", whitelisted: false},
	}
	for _, c := range cases {
		if whitelisted := server.proxy.isWhitelisted(c.serverName); whitelisted != c.whitelisted {
			t.Errorf("%q whitelisted = %v, expected %v", c.serverName, whitelisted, c.whitelisted)
		}
	}
}

// A spoof name is on the whitelist for the proxy and is never a valid extender
// destination: those are the operator patterns alone (A5).
func TestExtenderRefusesASpoofDomainDestination(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", nil)
	if !fixture.server.proxy.isWhitelisted(testSpoofName) {
		t.Fatal("the spoof name is not on the whitelist")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, _, err := connect.DialExtender(
		ctx,
		fixture.connectSettings(),
		fixture.extenderConfig(connect.ExtenderCarrierTcp),
		&connect.ExtenderDial{DestinationHost: testSpoofName, DestinationPort: 443},
	)
	if conn != nil {
		conn.Close()
	}
	if err == nil {
		t.Fatal("a spoof name was accepted as an extender destination")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Fatalf("error = %v, expected a 403 refusal", err)
	}
}

// A request body over the bound is refused with 503 before anything upstream
// is opened (A5).
func TestExtenderProxyRefusesAnOversizedRequestBody(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		settings.ProxyMaxRequestByteCount = 16
	})
	client := newRawExtenderHttpClientWithServerName(fixture, testSpoofName, nil)
	defer client.CloseIdleConnections()

	response, err := client.Post(
		"https://"+testSpoofName+"/",
		"application/octet-stream",
		bytes.NewReader(bytes.Repeat([]byte("x"), 17)),
	)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, expected %d", response.StatusCode, http.StatusServiceUnavailable)
	}
	extenderErr, ok := fixture.nextError()
	if !ok {
		t.Fatal("the oversized request body was not attributed")
	}
	if !strings.Contains(extenderErr.Error(), "proxy request") {
		t.Fatalf("attributed error = %v, expected the proxy request stage", extenderErr)
	}

	// a body within the bound still reaches the site
	response, err = client.Post(
		"https://"+testSpoofName+"/",
		"application/octet-stream",
		bytes.NewReader(bytes.Repeat([]byte("x"), 16)),
	)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, expected %d", response.StatusCode, http.StatusOK)
	}
}

// The relayed body is cut at the per-connection bound, and the bound is per
// connection, so asking again on the same connection does not restore it (A5).
func TestExtenderProxyCutsTheRelayedResponseAtItsBound(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		settings.ProxyMaxResponseByteCount = 64
	})
	client := newRawExtenderHttpClientWithServerName(fixture, testSpoofName, nil)
	defer client.CloseIdleConnections()

	response, err := client.Get("https://" + testSpoofName + "/bytes?n=256")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(body) != 64 {
		t.Fatalf("relayed %d bytes, expected the 64 byte bound", len(body))
	}
	extenderErr, ok := fixture.nextError()
	if !ok {
		t.Fatal("the cut response was not attributed")
	}
	if !strings.Contains(extenderErr.Error(), "proxy relay") {
		t.Fatalf("attributed error = %v, expected the proxy relay stage", extenderErr)
	}

	// the budget belongs to the connection, which the client reuses
	response, err = client.Get("https://" + testSpoofName + "/bytes?n=256")
	if err != nil {
		t.Fatal(err)
	}
	body, err = io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if 0 < len(body) {
		t.Fatalf("a second request on the same connection relayed %d bytes", len(body))
	}
}

// A site that answers and then stops sending releases the proxied exchange at
// the idle bound (A5).
func TestExtenderProxyEndsAnIdleRelay(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		settings.ProxyIdleTimeout = 250 * time.Millisecond
	})
	client := newRawExtenderHttpClientWithServerName(fixture, testSpoofName, nil)
	defer client.CloseIdleConnections()

	response, err := client.Get("https://" + testSpoofName + "/hold")
	if err != nil {
		t.Fatal(err)
	}
	// the site sent its first byte and then stopped; the relay ends on its own
	io.Copy(io.Discard, response.Body)
	response.Body.Close()
	extenderErr, ok := fixture.nextError()
	if !ok {
		t.Fatal("the idle relay was not attributed")
	}
	if !strings.Contains(extenderErr.Error(), "proxy relay") {
		t.Fatalf("attributed error = %v, expected the proxy relay stage", extenderErr)
	}
	close(fixture.destination.held)
}

// A source over its concurrent proxied bound is refused with 503 while it
// holds the bound, and served again once it releases (A5).
func TestExtenderProxyRefusesASourceOverItsConcurrentBound(t *testing.T) {
	assertProxyConcurrencyBound(t, func(settings *ExtenderSettings) {
		settings.ProxyMaxConnectionCountPerSource = 1
		settings.ProxyMaxConnectionCount = 0
	})
}

// The total proxied bound refuses a request from any source (A5).
func TestExtenderProxyRefusesOverTheTotalConcurrentBound(t *testing.T) {
	assertProxyConcurrencyBound(t, func(settings *ExtenderSettings) {
		settings.ProxyMaxConnectionCountPerSource = 0
		settings.ProxyMaxConnectionCount = 1
	})
}

// Holds one proxied exchange open, checks that the next is refused, and checks
// that releasing the first restores the slot.
func assertProxyConcurrencyBound(t *testing.T, configure func(settings *ExtenderSettings)) {
	t.Helper()
	fixture := newExtenderFixture(t, "127.0.0.1", configure)

	heldClient := newRawExtenderHttpClientWithServerName(fixture, testSpoofName, nil)
	defer heldClient.CloseIdleConnections()
	heldResponses := make(chan *http.Response, 1)
	heldErrors := make(chan error, 1)
	go func() {
		response, err := heldClient.Get("https://" + testSpoofName + "/hold")
		if err != nil {
			heldErrors <- err
			return
		}
		heldResponses <- response
	}()
	var heldResponse *http.Response
	select {
	case heldResponse = <-heldResponses:
	case err := <-heldErrors:
		t.Fatal(err)
	case <-time.After(10 * time.Second):
		t.Fatal("the held request never reached the site")
	}
	// the held exchange owns its slot until its body ends

	refusedClient := newRawExtenderHttpClientWithServerName(fixture, testSpoofName, nil)
	defer refusedClient.CloseIdleConnections()
	response, err := refusedClient.Get("https://" + testSpoofName + "/")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, expected %d", response.StatusCode, http.StatusServiceUnavailable)
	}
	extenderErr, ok := fixture.nextError()
	if !ok {
		t.Fatal("the refused request was not attributed")
	}
	if !strings.Contains(extenderErr.Error(), "proxy limit") {
		t.Fatalf("attributed error = %v, expected the proxy limit stage", extenderErr)
	}

	close(fixture.destination.held)
	io.Copy(io.Discard, heldResponse.Body)
	heldResponse.Body.Close()

	response, err = refusedClient.Get("https://" + testSpoofName + "/")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(response.Body)
	response.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status after release = %d, expected %d", response.StatusCode, http.StatusOK)
	}
	if !strings.Contains(string(body), testSpoofName) {
		t.Fatalf("body after release = %q", body)
	}
}

// An upstream site the extender cannot verify is answered with 503, so a
// misissued certificate never reaches the prober as content (A5).
func TestExtenderProxyRequiresUpstreamVerification(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		// the fixture site is self-signed, so the platform roots reject it
		settings.ProxyTlsConfig = nil
	})
	client := newRawExtenderHttpClientWithServerName(fixture, testSpoofName, nil)
	defer client.CloseIdleConnections()

	response, err := client.Get("https://" + testSpoofName + "/")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, expected %d", response.StatusCode, http.StatusServiceUnavailable)
	}
	extenderErr, ok := fixture.nextError()
	if !ok {
		t.Fatal("the unverified upstream was not attributed")
	}
	if !strings.Contains(extenderErr.Error(), "proxy upstream") {
		t.Fatalf("attributed error = %v, expected the proxy upstream stage", extenderErr)
	}
}

// A client that sends request headers and then stalls its body is dropped at
// the header timeout, which is also the http server's read timeout (A9).
func TestExtenderDropsAClientThatStallsItsRequestBody(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		settings.HeaderTimeout = 250 * time.Millisecond
	})

	conn := dialHandshakedExtenderConn(t, fixture)
	// a complete request head that promises a body which never arrives
	request := fmt.Sprintf(
		"POST / HTTP/1.1\r\nHost: %s\r\nContent-Type: %s\r\nContent-Length: 8\r\n\r\n",
		testSpoofName,
		connect.ExtenderContentType,
	)
	if _, err := conn.Write([]byte(request)); err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(conn); err != nil && !strings.Contains(err.Error(), "closed") {
		t.Logf("read after the request deadline: %v", err)
	}
	extenderErr, ok := fixture.nextError()
	if !ok {
		t.Fatal("the stalled request body was not dropped at the read timeout")
	}
	if !strings.Contains(extenderErr.Error(), "header decode") {
		t.Fatalf("attributed error = %v, expected the header stage", extenderErr)
	}
}
