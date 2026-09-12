// The request acceptance tests of EXTENDER.md section 5 phase 1: what the
// extender refuses (A3, A4, A8), the challenge response and the outer
// certificate a client with a record key checks (B3).

package extender

import (
	"bytes"
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect"
)

// A raw client on the tcp carrier, so a test can send what the production
// client never sends. nextProtos selects the outer alpn, which is what tells
// h2 from http/1.1.
func newRawExtenderHttpClient(fixture *extenderFixture, nextProtos []string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DialTLSContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
				conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", fixture.authority(fixture.tcpPort))
				if err != nil {
					return nil, err
				}
				tlsConn := tls.Client(conn, &tls.Config{
					ServerName:         testServerName,
					InsecureSkipVerify: true,
					MinVersion:         tls.VersionTLS13,
					NextProtos:         nextProtos,
				})
				if err := tlsConn.HandshakeContext(ctx); err != nil {
					conn.Close()
					return nil, err
				}
				return tlsConn, nil
			},
			ForceAttemptHTTP2: true,
		},
		Timeout: 20 * time.Second,
	}
}

// The serialized header a valid request carries.
func testExtenderHeaderBytes(t *testing.T, destinationHost string, destinationPort int, secret string) []byte {
	t.Helper()
	headerBytes, err := proto.Marshal(newTestExtenderHeader(destinationHost, destinationPort, secret))
	if err != nil {
		t.Fatal(err)
	}
	return headerBytes
}

// An extender request over h2 is refused, because an h2 stream cannot be
// hijacked (A3).
func TestExtenderRefusesTheRequestOverH2(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", nil)
	client := newRawExtenderHttpClient(fixture, []string{"h2"})
	defer client.CloseIdleConnections()

	headerBytes := testExtenderHeaderBytes(t, "dest.example", 443, testSecret)
	response, err := client.Post(
		"https://"+testServerName+"/",
		connect.ExtenderContentType,
		bytes.NewReader(headerBytes),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.ProtoMajor != 2 {
		t.Fatalf("request used HTTP/%d, expected h2", response.ProtoMajor)
	}
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, expected %d", response.StatusCode, http.StatusForbidden)
	}
}

// A request that is not an extender request is refused in this phase,
// whatever server name it asks for. Phase 1b answers it with the reverse
// proxy (A5).
func TestExtenderRefusesEveryOtherRequest(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", nil)
	serverNames := []string{testServerName, "other.example", "dest.example"}
	for _, serverName := range serverNames {
		client := newRawExtenderHttpClient(fixture, nil)
		response, err := client.Get("https://" + serverName + "/")
		if err != nil {
			t.Fatalf("%s: %v", serverName, err)
		}
		response.Body.Close()
		if response.ProtoMajor != 1 {
			t.Fatalf("%s used HTTP/%d, expected http/1.1", serverName, response.ProtoMajor)
		}
		if response.StatusCode != http.StatusForbidden {
			t.Fatalf("%s status = %d, expected %d", serverName, response.StatusCode, http.StatusForbidden)
		}
		client.CloseIdleConnections()
	}
}

// A header larger than the request cap is refused before it is decoded (A3).
func TestExtenderRefusesAnOversizedHeader(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", nil)
	client := newRawExtenderHttpClient(fixture, nil)
	defer client.CloseIdleConnections()

	oversizedBytes := make([]byte, connect.ExtenderMaxHeaderByteCount+1)
	response, err := client.Post(
		"https://"+testServerName+"/",
		connect.ExtenderContentType,
		bytes.NewReader(oversizedBytes),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, expected %d", response.StatusCode, http.StatusForbidden)
	}
	extenderErr, ok := fixture.nextError()
	if !ok {
		t.Fatal("the oversized header was not attributed")
	}
	if !strings.Contains(extenderErr.Error(), "header decode") {
		t.Fatalf("attributed error = %v, expected the header stage", extenderErr)
	}
}

// A bad secret, a destination that is not allowed and a reserved service with
// no handler are each refused with 403 on every carrier (A4, A8).
func TestExtenderRefusesUnauthorizedRequests(t *testing.T) {
	carriers := []string{
		connect.ExtenderCarrierTcp,
		connect.ExtenderCarrierQuic,
		connect.ExtenderCarrierDns,
	}
	cases := []struct {
		description  string
		secret       string
		extenderDial *connect.ExtenderDial
	}{
		{
			description:  "bad secret",
			secret:       "not-the-secret",
			extenderDial: &connect.ExtenderDial{DestinationHost: "dest.example", DestinationPort: 443},
		},
		{
			description:  "destination not allowed",
			secret:       testSecret,
			extenderDial: &connect.ExtenderDial{DestinationHost: "blocked.example", DestinationPort: 443},
		},
		{
			description:  "gossip service without a handler",
			secret:       testSecret,
			extenderDial: &connect.ExtenderDial{Service: connect.ExtenderServiceGossip},
		},
		{
			description:  "feed service without a handler",
			secret:       testSecret,
			extenderDial: &connect.ExtenderDial{Service: connect.ExtenderServiceFeed},
		},
		{
			description:  "unknown service",
			secret:       testSecret,
			extenderDial: &connect.ExtenderDial{Service: 9},
		},
	}
	fixture := newExtenderFixture(t, "127.0.0.1", nil)
	for _, carrier := range carriers {
		for _, c := range cases {
			extenderConfig := fixture.extenderConfig(carrier)
			extenderConfig.Secret = c.secret
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			conn, _, err := connect.DialExtender(ctx, fixture.connectSettings(), extenderConfig, c.extenderDial)
			cancel()
			if conn != nil {
				conn.Close()
			}
			if err == nil {
				t.Fatalf("%s %s was accepted", carrier, c.description)
			}
			if !strings.Contains(err.Error(), "403") {
				t.Fatalf("%s %s error = %v, expected a 403 refusal", carrier, c.description, err)
			}
		}
	}
}

// The challenge response verifies under the extender identity key, and only
// that key (A4, B1).
func TestExtenderChallengeResponseVerifies(t *testing.T) {
	seed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := connect.ExtenderPublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	otherSeed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	otherPublicKey, err := connect.ExtenderPublicKeyFromSeed(otherSeed)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		settings.IdentityKeySeed = seed
	})

	carriers := []string{
		connect.ExtenderCarrierTcp,
		connect.ExtenderCarrierQuic,
		connect.ExtenderCarrierDns,
	}
	for _, carrier := range carriers {
		challenge, err := connect.NewExtenderChallenge()
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		conn, response, err := connect.DialExtender(
			ctx,
			fixture.connectSettings(),
			fixture.extenderConfig(carrier),
			&connect.ExtenderDial{
				DestinationHost: "dest.example",
				DestinationPort: 443,
				Challenge:       challenge,
			},
		)
		cancel()
		if err != nil {
			t.Fatalf("%s: %v", carrier, err)
		}
		conn.Close()
		if string(response.PublicKey) != string(publicKey) {
			t.Fatalf("%s published key does not match the identity key", carrier)
		}
		if !connect.VerifyExtenderChallenge(publicKey, challenge, response.ChallengeSignature) {
			t.Fatalf("%s challenge signature does not verify", carrier)
		}
		if connect.VerifyExtenderChallenge(otherPublicKey, challenge, response.ChallengeSignature) {
			t.Fatalf("%s challenge signature verified under the wrong key", carrier)
		}
		if !slices.Contains(response.Carriers, carrier) {
			t.Fatalf("%s carriers = %v", carrier, response.Carriers)
		}
	}
}

// An extender with no identity key publishes no key and signs no challenge.
func TestExtenderWithoutAnIdentityKeyPublishesNothing(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", nil)
	challenge, err := connect.NewExtenderChallenge()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	conn, response, err := connect.DialExtender(
		ctx,
		fixture.connectSettings(),
		fixture.extenderConfig(connect.ExtenderCarrierTcp),
		&connect.ExtenderDial{
			DestinationHost: "dest.example",
			DestinationPort: 443,
			Challenge:       challenge,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if 0 < len(response.PublicKey) {
		t.Fatalf("public key = %x, expected none", response.PublicKey)
	}
	if 0 < len(response.ChallengeSignature) {
		t.Fatalf("challenge signature = %x, expected none", response.ChallengeSignature)
	}
}

// A client that knows the extender key requires the outer leaf to be signed by
// it on every carrier, and an extender with a different identity key fails the
// dial (B3).
func TestExtenderOuterCertificateVerifiesAgainstTheRecordKey(t *testing.T) {
	seed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := connect.ExtenderPublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	otherSeed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	otherPublicKey, err := connect.ExtenderPublicKeyFromSeed(otherSeed)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		settings.IdentityKeySeed = seed
	})

	carriers := []string{
		connect.ExtenderCarrierTcp,
		connect.ExtenderCarrierQuic,
		connect.ExtenderCarrierDns,
	}
	for _, carrier := range carriers {
		verifiedConfig := fixture.extenderConfig(carrier)
		verifiedConfig.PublicKey = []byte(publicKey)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		conn, _, err := connect.DialExtender(
			ctx,
			fixture.connectSettings(),
			verifiedConfig,
			&connect.ExtenderDial{DestinationHost: "dest.example", DestinationPort: 443},
		)
		cancel()
		if err != nil {
			t.Fatalf("%s verified dial: %v", carrier, err)
		}
		conn.Close()

		substitutedConfig := fixture.extenderConfig(carrier)
		substitutedConfig.PublicKey = []byte(otherPublicKey)
		ctx, cancel = context.WithTimeout(context.Background(), 20*time.Second)
		conn, _, err = connect.DialExtender(
			ctx,
			fixture.connectSettings(),
			substitutedConfig,
			&connect.ExtenderDial{DestinationHost: "dest.example", DestinationPort: 443},
		)
		cancel()
		if conn != nil {
			conn.Close()
		}
		if err == nil {
			t.Fatalf("%s accepted a chain signed by another identity key", carrier)
		}
	}
}

// An extender without an identity key presents a leaf no record key can
// verify, so a client that knows a key never accepts it (B3).
func TestExtenderWithoutAnIdentityKeyFailsRecordVerification(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", nil)
	seed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := connect.ExtenderPublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	extenderConfig := fixture.extenderConfig(connect.ExtenderCarrierTcp)
	extenderConfig.PublicKey = []byte(publicKey)
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
		t.Fatal("a self-signed leaf was accepted as a record chain")
	}
}

// Service 1 and 2 reach their handlers with a usable bidirectional stream on
// every carrier (A8).
func TestExtenderServicesReachTheirHandlers(t *testing.T) {
	serve := func(suffix string) func(conn net.Conn) {
		return func(conn net.Conn) {
			request := make([]byte, 4)
			if _, err := io.ReadFull(conn, request); err != nil {
				return
			}
			if _, err := conn.Write([]byte(string(request) + suffix)); err != nil {
				return
			}
			// hold the stream until the client is done, so the extender does
			// not close it under the reply
			io.Copy(io.Discard, conn)
		}
	}
	fixture := newExtenderFixture(t, "127.0.0.1", func(settings *ExtenderSettings) {
		settings.GossipConnHandler = serve("-gossip")
		settings.FeedConnHandler = serve("-feed")
	})

	cases := []struct {
		service uint32
		reply   string
	}{
		{service: connect.ExtenderServiceGossip, reply: "ping-gossip"},
		{service: connect.ExtenderServiceFeed, reply: "ping-feed"},
	}
	carriers := []string{
		connect.ExtenderCarrierTcp,
		connect.ExtenderCarrierQuic,
		connect.ExtenderCarrierDns,
	}
	for _, carrier := range carriers {
		for _, c := range cases {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			conn, _, err := connect.DialExtender(
				ctx,
				fixture.connectSettings(),
				fixture.extenderConfig(carrier),
				&connect.ExtenderDial{Service: c.service},
			)
			if err != nil {
				cancel()
				t.Fatalf("%s service %d: %v", carrier, c.service, err)
			}
			if _, err := conn.Write([]byte("ping")); err != nil {
				conn.Close()
				cancel()
				t.Fatalf("%s service %d: %v", carrier, c.service, err)
			}
			reply := make([]byte, len(c.reply))
			_, err = io.ReadFull(conn, reply)
			conn.Close()
			cancel()
			if err != nil {
				t.Fatalf("%s service %d: %v", carrier, c.service, err)
			}
			if string(reply) != c.reply {
				t.Fatalf("%s service %d reply = %q, expected %q", carrier, c.service, reply, c.reply)
			}
		}
	}
}

// A request that is not an extender request is refused over h3 too, which is
// where phase 1b's reverse proxy will answer it (A3, A5).
func TestExtenderRefusesAPlainRequestOverH3(t *testing.T) {
	fixture := newExtenderFixture(t, "127.0.0.1", nil)
	h3Transport := &http3.Transport{
		TLSClientConfig: &tls.Config{
			ServerName:         testServerName,
			InsecureSkipVerify: true,
			NextProtos:         []string{http3.NextProtoH3},
		},
		Dial: func(
			ctx context.Context,
			addr string,
			tlsConfig *tls.Config,
			quicConfig *quic.Config,
		) (*quic.Conn, error) {
			return quic.DialAddr(ctx, fixture.authority(fixture.quicPort), tlsConfig, quicConfig)
		},
	}
	defer h3Transport.Close()

	request, err := http.NewRequest(http.MethodGet, "https://"+testServerName+"/", nil)
	if err != nil {
		t.Fatal(err)
	}
	response, err := h3Transport.RoundTrip(request)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.ProtoMajor != 3 {
		t.Fatalf("request used HTTP/%d, expected h3", response.ProtoMajor)
	}
	if response.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, expected %d", response.StatusCode, http.StatusForbidden)
	}
}
