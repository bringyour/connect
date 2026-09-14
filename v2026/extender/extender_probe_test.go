// The probe tests of EXTENDER.md section 5 phase 1: the carrier probe the
// operator runs at activation and on the uptime tick (C2, C3), and the forward
// probe that proves the extender actually reaches the api.

package extender

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026"
)

// The connect mode of one carrier name, as a probe is asked for it.
func testProbeConnectMode(carrier string) connect.ExtenderConnectMode {
	switch carrier {
	case connect.ExtenderCarrierQuic:
		return connect.ExtenderConnectModeQuic
	case connect.ExtenderCarrierDns:
		return connect.ExtenderConnectModeDns
	default:
		return connect.ExtenderConnectModeTcpTls
	}
}

// The carrier port and encoding tld of one carrier on a fixture.
func testProbeEndpoint(fixture *extenderFixture, carrier string) (int, string) {
	switch carrier {
	case connect.ExtenderCarrierQuic:
		return fixture.quicPort, ""
	case connect.ExtenderCarrierDns:
		return fixture.dnsPort, testDnsTld
	default:
		return fixture.tcpPort, ""
	}
}

// The carrier probe verifies the challenge on every carrier of an extender
// with an identity key, and reports the carriers that extender serves (C2).
func TestProbeExtenderCarrierVerifiesEveryCarrier(t *testing.T) {
	seed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := connect.ExtenderPublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	// an operator activated extender is open, so a probe carries no secret
	fixture := newExtenderFixtureWithSecrets(t, "127.0.0.1", nil, func(settings *ExtenderSettings) {
		settings.IdentityKeySeed = seed
	})

	carriers := []string{
		connect.ExtenderCarrierTcp,
		connect.ExtenderCarrierQuic,
		connect.ExtenderCarrierDns,
	}
	for _, carrier := range carriers {
		port, dnsTld := testProbeEndpoint(fixture, carrier)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		response, err := connect.ProbeExtenderCarrier(
			ctx,
			fixture.connectSettings(),
			fixture.ip,
			testProbeConnectMode(carrier),
			port,
			dnsTld,
			testServerName,
			publicKey,
			"dest.example",
			443,
		)
		cancel()
		if err != nil {
			t.Fatalf("%s: %v", carrier, err)
		}
		if string(response.PublicKey) != string(publicKey) {
			t.Fatalf("%s published another key", carrier)
		}
		if len(response.Carriers) != 3 {
			t.Fatalf("%s carriers = %v, expected all three", carrier, response.Carriers)
		}
	}
}

// A probe that expects another key fails on every carrier, and so does a probe
// of an extender that has no key at all (C2).
func TestProbeExtenderCarrierRefusesTheWrongKey(t *testing.T) {
	seed, err := connect.NewExtenderKeySeed()
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

	keyedFixture := newExtenderFixtureWithSecrets(t, "127.0.0.1", nil, func(settings *ExtenderSettings) {
		settings.IdentityKeySeed = seed
	})
	keylessFixture := newExtenderFixtureWithSecrets(t, "127.0.0.1", nil, nil)

	carriers := []string{
		connect.ExtenderCarrierTcp,
		connect.ExtenderCarrierQuic,
		connect.ExtenderCarrierDns,
	}
	cases := []struct {
		description     string
		fixture         *extenderFixture
		expectPublicKey []byte
	}{
		{
			description:     "another identity key",
			fixture:         keyedFixture,
			expectPublicKey: otherPublicKey,
		},
		{
			description:     "no identity key",
			fixture:         keylessFixture,
			expectPublicKey: otherPublicKey,
		},
	}
	for _, carrier := range carriers {
		for _, c := range cases {
			port, dnsTld := testProbeEndpoint(c.fixture, carrier)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			_, err := connect.ProbeExtenderCarrier(
				ctx,
				c.fixture.connectSettings(),
				c.fixture.ip,
				testProbeConnectMode(carrier),
				port,
				dnsTld,
				testServerName,
				c.expectPublicKey,
				"dest.example",
				443,
			)
			cancel()
			if err == nil {
				t.Fatalf("%s %s was probed successfully", carrier, c.description)
			}
		}
	}
}

// An extender without an identity key is still probeable when no key is
// expected, which is what a manually configured extender is (C2).
func TestProbeExtenderCarrierWithoutAnExpectedKey(t *testing.T) {
	fixture := newExtenderFixtureWithSecrets(t, "127.0.0.1", nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	response, err := connect.ProbeExtenderCarrier(
		ctx,
		fixture.connectSettings(),
		fixture.ip,
		connect.ExtenderConnectModeTcpTls,
		fixture.tcpPort,
		"",
		testServerName,
		nil,
		"dest.example",
		443,
	)
	if err != nil {
		t.Fatal(err)
	}
	if 0 < len(response.PublicKey) {
		t.Fatalf("public key = %x, expected none", response.PublicKey)
	}
}

// The forward probe performs a verified hello through the tcp carrier and
// returns the caller address the api reported (C2).
func TestProbeExtenderForwardReturnsTheHelloClientAddress(t *testing.T) {
	seed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := connect.ExtenderPublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newExtenderFixtureWithSecrets(t, "127.0.0.1", nil, func(settings *ExtenderSettings) {
		settings.IdentityKeySeed = seed
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	clientAddress, err := connect.ProbeExtenderForward(
		ctx,
		fixture.connectSettings(),
		fixture.ip,
		fixture.tcpPort,
		testServerName,
		publicKey,
		"https://dest.example",
		&tls.Config{RootCAs: fixture.destination.rootCAs},
	)
	if err != nil {
		t.Fatal(err)
	}
	if clientAddress != testHelloClientAddress {
		t.Fatalf("client address = %q, expected %q", clientAddress, testHelloClientAddress)
	}
	forwardNetwork, err := fixture.nextForwardNetwork()
	if err != nil {
		t.Fatal(err)
	}
	if forwardNetwork != "tcp4" {
		t.Fatalf("forward network = %q, expected tcp4", forwardNetwork)
	}
}

// The inner tls of the forward probe is verified, so an extender that
// substitutes the api certificate fails the probe (C2).
func TestProbeExtenderForwardRequiresVerifiedInnerTls(t *testing.T) {
	fixture := newExtenderFixtureWithSecrets(t, "127.0.0.1", nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	_, err := connect.ProbeExtenderForward(
		ctx,
		fixture.connectSettings(),
		fixture.ip,
		fixture.tcpPort,
		testServerName,
		nil,
		"https://dest.example",
		// roots that do not carry the fixture site's certificate
		&tls.Config{RootCAs: x509.NewCertPool()},
	)
	if err == nil {
		t.Fatal("an unverified api certificate passed the forward probe")
	}
}
