// What the extender discriminates and refuses before anything is relayed
// (EXTENDER.md A3, A4, A5): the exact shape of an extender request, the header
// it will decode, the destinations an operator allowed, and the secrets that
// authorize a header.

package extender

import (
	"bytes"
	"context"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026"
	"github.com/urnetwork/connect/v2026/protocol"
)

// A second secret of the private extender, so a test can prove every allowed
// secret is tried (A4).
const testSecondSecret = "fixture-secret-two"

// Only POST / with the extender content type is the extender protocol;
// everything else is an ordinary request the reverse proxy answers (A3, A5).
// The content type is matched case insensitively, with the surrounding space
// trimmed and any parameter cut, because that is how a content type is written.
func TestIsExtenderRequestDiscriminatesTheExtenderShape(t *testing.T) {
	cases := []struct {
		description     string
		method          string
		url             *url.URL
		contentType     string
		extenderRequest bool
	}{
		{
			description:     "the extender shape",
			method:          http.MethodPost,
			url:             &url.URL{Path: "/"},
			contentType:     connect.ExtenderContentType,
			extenderRequest: true,
		},
		{
			description:     "the content type in another case",
			method:          http.MethodPost,
			url:             &url.URL{Path: "/"},
			contentType:     strings.ToUpper(connect.ExtenderContentType),
			extenderRequest: true,
		},
		{
			description:     "the content type with surrounding space",
			method:          http.MethodPost,
			url:             &url.URL{Path: "/"},
			contentType:     "  " + connect.ExtenderContentType + "  ",
			extenderRequest: true,
		},
		{
			description:     "the content type with a parameter",
			method:          http.MethodPost,
			url:             &url.URL{Path: "/"},
			contentType:     connect.ExtenderContentType + "; charset=utf-8",
			extenderRequest: true,
		},
		{
			description:     "the content type with a spaced parameter",
			method:          http.MethodPost,
			url:             &url.URL{Path: "/"},
			contentType:     connect.ExtenderContentType + " ;v=1",
			extenderRequest: true,
		},
		{
			description:     "another method",
			method:          http.MethodGet,
			url:             &url.URL{Path: "/"},
			contentType:     connect.ExtenderContentType,
			extenderRequest: false,
		},
		{
			description:     "another path",
			method:          http.MethodPost,
			url:             &url.URL{Path: "/extender"},
			contentType:     connect.ExtenderContentType,
			extenderRequest: false,
		},
		{
			description:     "an empty path",
			method:          http.MethodPost,
			url:             &url.URL{Path: ""},
			contentType:     connect.ExtenderContentType,
			extenderRequest: false,
		},
		{
			description:     "another content type",
			method:          http.MethodPost,
			url:             &url.URL{Path: "/"},
			contentType:     "application/json",
			extenderRequest: false,
		},
		{
			description:     "a content type the extender type is a prefix of",
			method:          http.MethodPost,
			url:             &url.URL{Path: "/"},
			contentType:     connect.ExtenderContentType + "-v2",
			extenderRequest: false,
		},
		{
			description:     "no content type",
			method:          http.MethodPost,
			url:             &url.URL{Path: "/"},
			contentType:     "",
			extenderRequest: false,
		},
		{
			description:     "no url",
			method:          http.MethodPost,
			url:             nil,
			contentType:     connect.ExtenderContentType,
			extenderRequest: false,
		},
	}
	for _, c := range cases {
		req := &http.Request{
			Method: c.method,
			URL:    c.url,
			Header: http.Header{},
		}
		if c.contentType != "" {
			req.Header.Set("Content-Type", c.contentType)
		}
		if extenderRequest := isExtenderRequest(req); extenderRequest != c.extenderRequest {
			t.Errorf("%s: extender request = %v, expected %v", c.description, extenderRequest, c.extenderRequest)
		}
	}
}

// A request that carries the extender content type but no header the extender
// can decode is refused with 403 at the header stage, before any destination is
// considered (A3, A4). The content length delimits the header on every carrier,
// so a chunked body has no header at all.
func TestExtenderRefusesAnUndecodableHeader(t *testing.T) {
	cases := []struct {
		description   string
		bodyBytes     []byte
		contentLength int64
		expect        string
	}{
		{
			description: "a chunked body, which carries no length",
			bodyBytes:   testExtenderHeaderBytes(t, "dest.example", 443, testSecret),
			// the http client sends a chunked body for an unknown length
			contentLength: -1,
			expect:        "no content length",
		},
		{
			description:   "a body under the cap that is not a header",
			bodyBytes:     bytes.Repeat([]byte{0xff}, 16),
			contentLength: 0,
			expect:        "cannot parse",
		},
	}
	for _, c := range cases {
		fixture := newExtenderFixture(t, "127.0.0.1", nil)
		client := newRawExtenderHttpClient(fixture, nil)

		request, err := http.NewRequest(
			http.MethodPost,
			"https://"+testServerName+"/",
			bytes.NewReader(c.bodyBytes),
		)
		if err != nil {
			t.Fatal(err)
		}
		request.Header.Set("Content-Type", connect.ExtenderContentType)
		if c.contentLength != 0 {
			request.ContentLength = c.contentLength
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("%s: %v", c.description, err)
		}
		response.Body.Close()
		client.CloseIdleConnections()

		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("%s status = %d, expected %d", c.description, response.StatusCode, http.StatusForbidden)
		}
		extenderErr, ok := fixture.nextError()
		if !ok {
			t.Fatalf("%s was not attributed", c.description)
		}
		if !strings.Contains(extenderErr.Error(), "header decode") {
			t.Fatalf("%s attributed error = %v, expected the header decode stage", c.description, extenderErr)
		}
		if !strings.Contains(extenderErr.Error(), c.expect) {
			t.Fatalf("%s attributed error = %v, expected %q", c.description, extenderErr, c.expect)
		}
	}
}

// An extender forwards only to the operator's patterns: an exact name, or a
// `*.` wildcard, which matches a subdomain of any depth and never the bare
// name (A4, A5).
func TestExtenderIsAllowedHost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := NewExtenderServer(
		ctx,
		nil,
		[]string{"api.operator.example", "*.operator.example"},
		nil,
		nil,
		DefaultExtenderSettings(),
	)
	defer server.Close()

	cases := []struct {
		host    string
		allowed bool
	}{
		{host: "api.operator.example", allowed: true},
		{host: "sub.operator.example", allowed: true},
		// the wildcard is a plain suffix match, so it reaches any depth
		{host: "a.b.operator.example", allowed: true},
		// and never the bare name the wildcard is written against
		{host: "operator.example", allowed: false},
		// the leading dot of the pattern is part of the suffix, so a label that
		// only ends in the name is not a subdomain of it
		{host: "notoperator.example", allowed: false},
		{host: "other.example", allowed: false},
		{host: "", allowed: false},
		// both the exact name and the wildcard suffix are matched literally, so
		// a host that differs only in the case of the pattern part is not the
		// pattern; here only the wildcard's suffix still matches
		{host: "API.operator.example", allowed: true},
		{host: "API.OPERATOR.EXAMPLE", allowed: false},
		// a host idna cannot read is not a host, whatever it matches
		{host: "xn--0.operator.example", allowed: false},
	}
	for _, c := range cases {
		if allowed := server.IsAllowedHost(c.host); allowed != c.allowed {
			t.Errorf("%q allowed = %v, expected %v", c.host, allowed, c.allowed)
		}
	}

	// an extender with no patterns forwards nowhere
	emptyServer := NewExtenderServer(ctx, nil, nil, nil, nil, DefaultExtenderSettings())
	defer emptyServer.Close()
	for _, host := range []string{"api.operator.example", "operator.example", ""} {
		if emptyServer.IsAllowedHost(host) {
			t.Errorf("%q is allowed by an extender with no patterns", host)
		}
	}
}

// Every allowed secret authorizes a header, and the hmac binds both the
// timestamp and the nonce, so a header replayed with either changed is refused
// (A4).
func TestExtenderAllowsEverySecretAndBindsTheSignedFields(t *testing.T) {
	fixture := newExtenderFixtureWithSecrets(
		t,
		"127.0.0.1",
		[]string{testSecret, testSecondSecret},
		nil,
	)

	// a header signed with an allowed secret and then changed no longer
	// carries a signature over what it says
	tamperCases := []struct {
		description string
		tamper      func(header *protocol.ExtenderHeader)
	}{
		{
			description: "the timestamp",
			tamper: func(header *protocol.ExtenderHeader) {
				header.Timestamp += 1
			},
		},
		{
			description: "the nonce",
			tamper: func(header *protocol.ExtenderHeader) {
				nonce := append([]byte(nil), header.Nonce...)
				nonce[0] ^= 0xff
				header.Nonce = nonce
			},
		},
	}
	for _, c := range tamperCases {
		header := newTestExtenderHeader("dest.example", 443, testSecret)
		c.tamper(header)
		headerBytes, err := proto.Marshal(header)
		if err != nil {
			t.Fatal(err)
		}
		client := newRawExtenderHttpClient(fixture, nil)
		response, err := client.Post(
			"https://"+testServerName+"/",
			connect.ExtenderContentType,
			bytes.NewReader(headerBytes),
		)
		if err != nil {
			t.Fatalf("%s: %v", c.description, err)
		}
		response.Body.Close()
		client.CloseIdleConnections()
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf(
				"a header with %s changed after signing got status %d, expected %d",
				c.description,
				response.StatusCode,
				http.StatusForbidden,
			)
		}
		extenderErr, ok := fixture.nextError()
		if !ok {
			t.Fatalf("the header with %s changed was not attributed", c.description)
		}
		if !strings.Contains(extenderErr.Error(), "header authorization") {
			t.Fatalf("attributed error = %v, expected the header authorization stage", extenderErr)
		}
	}

	// either allowed secret authorizes a request
	for _, secret := range []string{testSecret, testSecondSecret} {
		extenderConfig := fixture.extenderConfig(connect.ExtenderCarrierTcp)
		extenderConfig.Secret = secret
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		conn, _, err := connect.DialExtender(
			ctx,
			fixture.connectSettings(),
			extenderConfig,
			&connect.ExtenderDial{DestinationHost: "dest.example", DestinationPort: 443},
		)
		cancel()
		if err != nil {
			t.Fatalf("the header signed with %q was refused: %v", secret, err)
		}
		conn.Close()
	}

	// a secret that is on no list is still refused
	extenderConfig := fixture.extenderConfig(connect.ExtenderCarrierTcp)
	extenderConfig.Secret = "not-an-allowed-secret"
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, _, err := connect.DialExtender(
		ctx,
		fixture.connectSettings(),
		extenderConfig,
		&connect.ExtenderDial{DestinationHost: "dest.example", DestinationPort: 443},
	)
	if conn != nil {
		conn.Close()
	}
	if err == nil {
		t.Fatal("a header signed with an unlisted secret was accepted")
	}
	if !strings.Contains(err.Error(), "403") {
		t.Fatalf("error = %v, expected a 403 refusal", err)
	}
}
