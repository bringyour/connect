// The bounds of the certificate issuer (EXTENDER.md B3): the leaf cache a
// handshake for any name is served from, the san an ip literal gets, and the
// validity window a leaf and the identity ca carry.

package extender

import (
	"crypto/x509"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026"
)

// The leaf cache is bounded and evicts the least recently used name, so a
// prober that asks for a new name on every handshake cannot grow it (B3). A
// name that was used again survives an eviction that takes the name issued
// after it.
func TestExtenderCertificatesEvictTheLeastRecentlyUsedLeaf(t *testing.T) {
	certificates, err := newExtenderCertificates(nil, DefaultExtenderSettings())
	if err != nil {
		t.Fatal(err)
	}

	serverNames := make([]string, extenderLeafCacheCount)
	for i := range serverNames {
		serverNames[i] = fmt.Sprintf("leaf%d.example", i)
		if _, err := certificates.certificateForServerName(serverNames[i]); err != nil {
			t.Fatal(err)
		}
	}
	if count := len(certificates.serverNameEntries); count != extenderLeafCacheCount {
		t.Fatalf("cached leaves = %d, expected the cache to be full at %d", count, extenderLeafCacheCount)
	}

	// asking for the oldest name again makes it the most recent, so the name
	// issued after it becomes the one to evict
	oldest, err := certificates.certificateForServerName(serverNames[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := certificates.certificateForServerName("overflow.example"); err != nil {
		t.Fatal(err)
	}

	if count := len(certificates.serverNameEntries); count != extenderLeafCacheCount {
		t.Fatalf("cached leaves = %d, expected the bound %d", count, extenderLeafCacheCount)
	}
	if count := certificates.recentServerNames.Len(); count != extenderLeafCacheCount {
		t.Fatalf("recent leaves = %d, expected the bound %d", count, extenderLeafCacheCount)
	}
	if _, ok := certificates.serverNameEntries[serverNames[0]]; !ok {
		t.Fatal("the name that was used again was evicted")
	}
	if _, ok := certificates.serverNameEntries[serverNames[1]]; ok {
		t.Fatal("the least recently used name survived the eviction")
	}
	if _, ok := certificates.serverNameEntries["overflow.example"]; !ok {
		t.Fatal("the name that overflowed the cache was not kept")
	}

	// the surviving name keeps its identity, and the evicted one is issued
	// again rather than remembered
	again, err := certificates.certificateForServerName(serverNames[0])
	if err != nil {
		t.Fatal(err)
	}
	if again != oldest {
		t.Fatal("the surviving name was issued a second certificate")
	}
	evicted, err := certificates.certificateForServerName(serverNames[1])
	if err != nil {
		t.Fatal(err)
	}
	if evicted.Leaf.DNSNames[0] != serverNames[1] {
		t.Fatalf("reissued name = %v, expected %q", evicted.Leaf.DNSNames, serverNames[1])
	}
}

// A handshake that asks for an ip literal gets a leaf with that address as its
// san and no dns name, so the leaf still names what the client asked for (B3).
func TestExtenderCertificatesIssueAnIpSan(t *testing.T) {
	certificates, err := newExtenderCertificates(nil, DefaultExtenderSettings())
	if err != nil {
		t.Fatal(err)
	}
	cases := []string{"192.0.2.5", "2001:db8::5"}
	for _, serverName := range cases {
		certificate, err := certificates.certificateForServerName(serverName)
		if err != nil {
			t.Fatalf("%s: %v", serverName, err)
		}
		leaf := certificate.Leaf
		if 0 < len(leaf.DNSNames) {
			t.Errorf("%s leaf carries dns names %v, expected none", serverName, leaf.DNSNames)
		}
		if len(leaf.IPAddresses) != 1 {
			t.Fatalf("%s leaf ip sans = %v, expected exactly one", serverName, leaf.IPAddresses)
		}
		if !leaf.IPAddresses[0].Equal(net.ParseIP(serverName)) {
			t.Errorf("%s leaf ip san = %v", serverName, leaf.IPAddresses[0])
		}
	}
}

// A leaf and the identity ca span the moment they were issued: ValidFrom is
// the tolerated history before it, and the lifetime after it is the leaf
// window for an identity leaf, the ca window for the ca, and the configured
// ValidFor for the self-signed leaf of an extender without an identity (B3).
func TestExtenderCertificateValidityFollowsTheSettings(t *testing.T) {
	settings := DefaultExtenderSettings()
	settings.ValidFrom = 2 * time.Hour
	settings.ValidFor = 3 * time.Hour

	assertAbout := func(description string, actual time.Time, expected time.Time) {
		t.Helper()
		// x509 keeps whole seconds, and issuing takes a moment of its own
		if delta := actual.Sub(expected); delta < -2*time.Second || 2*time.Second < delta {
			t.Errorf("%s = %s, expected about %s", description, actual, expected)
		}
	}

	seed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	issuedAt := time.Now()
	certificates, err := newExtenderCertificates(seed, settings)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := certificates.certificateForServerName("leaf.example")
	if err != nil {
		t.Fatal(err)
	}
	leaf := certificate.Leaf
	assertAbout("identity leaf NotBefore", leaf.NotBefore, issuedAt.Add(-settings.ValidFrom))
	assertAbout("identity leaf NotAfter", leaf.NotAfter, issuedAt.Add(extenderLeafValidFor))
	if leaf.NotBefore.After(issuedAt) || leaf.NotAfter.Before(issuedAt) {
		t.Errorf("leaf validity %s..%s does not span issuance", leaf.NotBefore, leaf.NotAfter)
	}

	caCertificate, err := x509.ParseCertificate(certificates.caCertificate)
	if err != nil {
		t.Fatal(err)
	}
	assertAbout("ca NotBefore", caCertificate.NotBefore, issuedAt.Add(-settings.ValidFrom))
	assertAbout("ca NotAfter", caCertificate.NotAfter, issuedAt.Add(extenderCaValidFor))
	// the ca outlives every leaf it issues, so a leaf is never the shorter of
	// the two by accident
	if !leaf.NotAfter.Before(caCertificate.NotAfter) {
		t.Errorf("leaf NotAfter %s is not before the ca NotAfter %s", leaf.NotAfter, caCertificate.NotAfter)
	}

	// without an identity key the leaf is self-signed for the configured window
	keylessIssuedAt := time.Now()
	keylessCertificates, err := newExtenderCertificates(nil, settings)
	if err != nil {
		t.Fatal(err)
	}
	keylessCertificate, err := keylessCertificates.certificateForServerName("leaf.example")
	if err != nil {
		t.Fatal(err)
	}
	keylessLeaf := keylessCertificate.Leaf
	assertAbout("self-signed leaf NotBefore", keylessLeaf.NotBefore, keylessIssuedAt.Add(-settings.ValidFrom))
	assertAbout("self-signed leaf NotAfter", keylessLeaf.NotAfter, keylessIssuedAt.Add(settings.ValidFor))

	if extenderLeafValidFor != 30*24*time.Hour {
		t.Errorf("leaf window = %s, expected 30 days", extenderLeafValidFor)
	}
	if extenderCaValidFor != 10*365*24*time.Hour {
		t.Errorf("ca window = %s, expected 10 years", extenderCaValidFor)
	}
	if extenderLeafCacheCount != 1024 {
		t.Errorf("leaf cache = %d, expected 1024", extenderLeafCacheCount)
	}
}
