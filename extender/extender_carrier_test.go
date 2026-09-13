// The carrier acceptance tests of EXTENDER.md section 5 phase 1: a client
// reaches an in-process destination through tcp, quic and dns over both
// loopback families, the forward dial uses the client's family, and the v1
// framing still works on tcp.

package extender

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect"
	"github.com/urnetwork/connect/protocol"
)

// The loopback families every carrier test runs on. The hosts running these
// tests are dual-stack, so a missing family is a failure, not a skip.
var testLoopbackFamilies = []struct {
	loopbackIp     string
	forwardNetwork string
}{
	{loopbackIp: "127.0.0.1", forwardNetwork: "tcp4"},
	{loopbackIp: "::1", forwardNetwork: "tcp6"},
}

// Every carrier carries a request to the destination on both families, and the
// forward dial the extender makes is narrowed to the client's family (A1, A7).
func TestExtenderCarriersReachDestination(t *testing.T) {
	carriers := []string{
		connect.ExtenderCarrierTcp,
		connect.ExtenderCarrierQuic,
		connect.ExtenderCarrierDns,
	}
	for _, family := range testLoopbackFamilies {
		for _, carrier := range carriers {
			fixture := newExtenderFixture(t, family.loopbackIp, nil)
			client := connect.NewExtenderHttpClient(
				fixture.connectSettings(),
				fixture.extenderConfig(carrier),
			)

			response, err := client.Get("https://dest.example/hello")
			if err != nil {
				if extenderErr, ok := fixture.nextError(); ok {
					t.Fatalf("%s %s: %v; extender: %v", carrier, family.loopbackIp, err, extenderErr)
				}
				t.Fatalf("%s %s: %v", carrier, family.loopbackIp, err)
			}
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				t.Fatalf("%s %s: %v", carrier, family.loopbackIp, err)
			}
			if response.StatusCode != http.StatusOK {
				t.Fatalf("%s %s status = %d, expected %d", carrier, family.loopbackIp, response.StatusCode, http.StatusOK)
			}
			if !strings.Contains(string(body), "dest.example") {
				t.Fatalf("%s %s body = %q", carrier, family.loopbackIp, body)
			}
			forwardNetwork, err := fixture.nextForwardNetwork()
			if err != nil {
				t.Fatalf("%s %s: %v", carrier, family.loopbackIp, err)
			}
			if forwardNetwork != family.forwardNetwork {
				t.Fatalf(
					"%s %s forward network = %q, expected %q",
					carrier,
					family.loopbackIp,
					forwardNetwork,
					family.forwardNetwork,
				)
			}
			client.CloseIdleConnections()
		}
	}
}

// A destination with an address of the client's family is reached; the same
// destination offered only on the other family is refused by the narrowed dial
// (A7).
func TestExtenderForwardDialRefusesTheOtherFamily(t *testing.T) {
	cases := []struct {
		loopbackIp      string
		reachableHost   string
		unreachableHost string
		forwardNetwork  string
	}{
		{
			loopbackIp:      "127.0.0.1",
			reachableHost:   "dest4.example",
			unreachableHost: "dest6.example",
			forwardNetwork:  "tcp4",
		},
		{
			loopbackIp:      "::1",
			reachableHost:   "dest6.example",
			unreachableHost: "dest4.example",
			forwardNetwork:  "tcp6",
		},
	}
	for _, c := range cases {
		fixture := newExtenderFixture(t, c.loopbackIp, nil)
		client := connect.NewExtenderHttpClient(
			fixture.connectSettings(),
			fixture.extenderConfig(connect.ExtenderCarrierTcp),
		)

		response, err := client.Get(fmt.Sprintf("https://%s/hello", c.reachableHost))
		if err != nil {
			t.Fatalf("%s %s: %v", c.loopbackIp, c.reachableHost, err)
		}
		response.Body.Close()
		forwardNetwork, err := fixture.nextForwardNetwork()
		if err != nil {
			t.Fatal(err)
		}
		if forwardNetwork != c.forwardNetwork {
			t.Fatalf("forward network = %q, expected %q", forwardNetwork, c.forwardNetwork)
		}

		if _, err := client.Get(fmt.Sprintf("https://%s/hello", c.unreachableHost)); err == nil {
			t.Fatalf("%s reached %s, which has no %s address", c.loopbackIp, c.unreachableHost, c.forwardNetwork)
		}
		unreachableNetwork, err := fixture.nextForwardNetwork()
		if err != nil {
			t.Fatal(err)
		}
		if unreachableNetwork != c.forwardNetwork {
			t.Fatalf("forward network = %q, expected %q", unreachableNetwork, c.forwardNetwork)
		}
		client.CloseIdleConnections()
	}
}

// The v1 length-prefixed header still reaches the destination on tcp, with no
// response frame, for one release (A3).
func TestExtenderV1FramedClientReachesDestination(t *testing.T) {
	for _, family := range testLoopbackFamilies {
		fixture := newExtenderFixture(t, family.loopbackIp, nil)
		client := &http.Client{
			Transport: &http.Transport{
				DialTLSContext: newV1ExtenderDialTlsContext(t, fixture),
			},
			Timeout: 20 * time.Second,
		}

		response, err := client.Get("https://dest.example/hello")
		if err != nil {
			if extenderErr, ok := fixture.nextError(); ok {
				t.Fatalf("%s: %v; extender: %v", family.loopbackIp, err, extenderErr)
			}
			t.Fatalf("%s: %v", family.loopbackIp, err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "dest.example") {
			t.Fatalf("v1 body = %q", body)
		}
		forwardNetwork, err := fixture.nextForwardNetwork()
		if err != nil {
			t.Fatal(err)
		}
		if forwardNetwork != family.forwardNetwork {
			t.Fatalf("v1 forward network = %q, expected %q", forwardNetwork, family.forwardNetwork)
		}
		client.CloseIdleConnections()
	}
}

// A dial with no outer name reaches the destination on every carrier, and the
// extender sees a handshake that carried no sni at all, which is what an empty
// spoof list presents so the operator name never appears outside (A10).
func TestExtenderCarriersAcceptAnEmptyServerName(t *testing.T) {
	carriers := []string{
		connect.ExtenderCarrierTcp,
		connect.ExtenderCarrierQuic,
		connect.ExtenderCarrierDns,
	}
	for _, family := range testLoopbackFamilies {
		for _, carrier := range carriers {
			fixture := newExtenderFixture(t, family.loopbackIp, nil)
			extenderConfig := fixture.extenderConfig(carrier)
			extenderConfig.Profile.ServerName = ""
			client := connect.NewExtenderHttpClient(fixture.connectSettings(), extenderConfig)

			response, err := client.Get("https://dest.example/hello")
			if err != nil {
				if extenderErr, ok := fixture.nextError(); ok {
					t.Fatalf("%s %s: %v; extender: %v", carrier, family.loopbackIp, err, extenderErr)
				}
				t.Fatalf("%s %s: %v", carrier, family.loopbackIp, err)
			}
			body, err := io.ReadAll(response.Body)
			response.Body.Close()
			if err != nil {
				t.Fatalf("%s %s: %v", carrier, family.loopbackIp, err)
			}
			if response.StatusCode != http.StatusOK {
				t.Fatalf("%s %s status = %d, expected %d", carrier, family.loopbackIp, response.StatusCode, http.StatusOK)
			}
			if !strings.Contains(string(body), "dest.example") {
				t.Fatalf("%s %s body = %q", carrier, family.loopbackIp, body)
			}
			serverName, ok := fixture.nextServerName()
			if !ok {
				t.Fatalf("%s %s: the extender terminated no handshake", carrier, family.loopbackIp)
			}
			if serverName != "" {
				t.Fatalf(
					"%s %s outer sni = %q, expected the ClientHello to carry none",
					carrier,
					family.loopbackIp,
					serverName,
				)
			}
			client.CloseIdleConnections()
		}
	}
}

// A dialer that carries a spoof name presents exactly that name as the outer
// sni, and the destination name never leaves the inner tls (A10).
func TestExtenderCarriersPresentTheSpoofName(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", nil)
	client := connect.NewExtenderHttpClient(
		fixture.connectSettings(),
		fixture.extenderConfig(connect.ExtenderCarrierTcp),
	)
	defer client.CloseIdleConnections()

	response, err := client.Get("https://dest.example/hello")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	serverName, ok := fixture.nextServerName()
	if !ok {
		t.Fatal("the extender terminated no handshake")
	}
	if serverName != testServerName {
		t.Fatalf("outer sni = %q, expected %q", serverName, testServerName)
	}
}

// An unnamed handshake still gets a usable leaf: a fixed placeholder subject
// and no san, because there is no name to issue for (A10, B3).
func TestExtenderCertificatesIssueForAnEmptyServerName(t *testing.T) {
	certificates, err := newExtenderCertificates(nil, DefaultExtenderSettings())
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := certificates.certificateForServerName("")
	if err != nil {
		t.Fatal(err)
	}
	leaf := certificate.Leaf
	if 0 < len(leaf.DNSNames) || 0 < len(leaf.IPAddresses) {
		t.Fatalf("an unnamed leaf carries a san: dns %v ip %v", leaf.DNSNames, leaf.IPAddresses)
	}
	if len(leaf.Subject.Organization) != 1 || leaf.Subject.Organization[0] != extenderUnnamedOrganization {
		t.Fatalf("unnamed leaf subject = %q, expected the placeholder", leaf.Subject.String())
	}
}

// Builds the v1 client shape: outer tls, a four byte big-endian length, the
// serialized header, then the inner tls straight away.
func newV1ExtenderDialTlsContext(t *testing.T, fixture *extenderFixture) func(context.Context, string, string) (net.Conn, error) {
	t.Helper()
	return func(ctx context.Context, network string, address string) (net.Conn, error) {
		host, portStr, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		port, err := net.LookupPort("tcp", portStr)
		if err != nil {
			return nil, err
		}
		conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", fixture.authority(fixture.tcpPort))
		if err != nil {
			return nil, err
		}
		success := false
		defer func() {
			if !success {
				conn.Close()
			}
		}()
		outerConn := tls.Client(conn, &tls.Config{
			ServerName:         testServerName,
			InsecureSkipVerify: true,
			MinVersion:         tls.VersionTLS13,
		})
		if err := outerConn.HandshakeContext(ctx); err != nil {
			return nil, err
		}

		header := newTestExtenderHeader(host, port, testSecret)
		headerMessageBytes, err := proto.Marshal(header)
		if err != nil {
			return nil, err
		}
		headerBytes := make([]byte, 4+len(headerMessageBytes))
		binary.BigEndian.PutUint32(headerBytes[0:4], uint32(len(headerMessageBytes)))
		copy(headerBytes[4:], headerMessageBytes)
		if _, err := outerConn.Write(headerBytes); err != nil {
			return nil, err
		}

		innerConn := tls.Client(outerConn, &tls.Config{
			ServerName: host,
			RootCAs:    fixture.destination.rootCAs,
		})
		if err := innerConn.HandshakeContext(ctx); err != nil {
			return nil, err
		}
		success = true
		return innerConn, nil
	}
}

// One signed header for a private extender, as both the v1 and v2 clients
// build it.
func newTestExtenderHeader(destinationHost string, destinationPort int, secret string) *protocol.ExtenderHeader {
	header := &protocol.ExtenderHeader{
		DestinationHost: destinationHost,
		DestinationPort: uint32(destinationPort),
		Timestamp:       uint64(time.Now().UnixMilli()),
	}
	if secret != "" {
		nonce := connect.NewId()
		header.Nonce = nonce.Bytes()
		header.Signature = testExtenderHeaderSignature(secret, header.Timestamp, header.Nonce)
	}
	return header
}

// The hmac over timestamp and nonce the extender checks (A4).
func testExtenderHeaderSignature(secret string, timestamp uint64, nonce []byte) []byte {
	mac := hmac.New(sha256.New, []byte(secret))
	timestampBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(timestampBytes[0:8], timestamp)
	mac.Write(timestampBytes)
	mac.Write(nonce)
	return mac.Sum(nil)
}

// The fragment and reorder resilience applies to the tcp carrier only, and a
// request still reaches the destination through it (A10).
func TestExtenderTcpCarrierWithResilientHandshake(t *testing.T) {
	for _, family := range testLoopbackFamilies {
		fixture := newExtenderFixture(t, family.loopbackIp, nil)
		extenderConfig := fixture.extenderConfig(connect.ExtenderCarrierTcp)
		extenderConfig.Profile.Fragment = true
		extenderConfig.Profile.Reorder = true
		client := connect.NewExtenderHttpClient(fixture.connectSettings(), extenderConfig)

		response, err := client.Get("https://dest.example/hello")
		if err != nil {
			if extenderErr, ok := fixture.nextError(); ok {
				t.Fatalf("%s: %v; extender: %v", family.loopbackIp, err, extenderErr)
			}
			t.Fatalf("%s: %v", family.loopbackIp, err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(body), "dest.example") {
			t.Fatalf("resilient body = %q", body)
		}
		forwardNetwork, err := fixture.nextForwardNetwork()
		if err != nil {
			t.Fatal(err)
		}
		if forwardNetwork != family.forwardNetwork {
			t.Fatalf("resilient forward network = %q, expected %q", forwardNetwork, family.forwardNetwork)
		}
		client.CloseIdleConnections()
	}
}
