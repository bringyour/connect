// The forward probe over ipv6 (EXTENDER.md C2, A7). The probe reaches the api
// through the tcp carrier on either loopback family, and the forward dial it
// causes is narrowed to the family the probe arrived on.

package extender

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026"
)

// The forward probe performs a verified hello through an ipv6 tcp carrier, and
// the extender dials the api over ipv6 because that is the family its client
// reached it on (C2, A7).
func TestProbeExtenderForwardReturnsTheHelloClientAddressOverIpv6(t *testing.T) {
	seed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := connect.ExtenderPublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	fixture := newExtenderFixtureWithSecrets(t, "::1", nil, func(settings *ExtenderSettings) {
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
	if forwardNetwork != "tcp6" {
		t.Fatalf("forward network = %q, expected tcp6", forwardNetwork)
	}
}
