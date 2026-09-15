package connect

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDefaultTlsConfigSharesRootsButNotSessionCaches(t *testing.T) {
	t.Setenv(ExtraRootCAFileEnv, "")
	first, err := DefaultTlsConfig()
	if err != nil {
		t.Fatalf("first DefaultTlsConfig: %v", err)
	}
	second, err := DefaultTlsConfig()
	if err != nil {
		t.Fatalf("second DefaultTlsConfig: %v", err)
	}
	if first.RootCAs == nil || first.RootCAs != second.RootCAs {
		t.Fatal("default TLS configs did not reuse the immutable pinned roots")
	}
	if first.ClientSessionCache == nil || first.ClientSessionCache == second.ClientSessionCache {
		t.Fatal("default TLS configs must retain independent session caches")
	}

	// Preserve PinnedCertPool's public fresh/mutable-pool contract.
	freshFirst, err := PinnedCertPool()
	if err != nil {
		t.Fatalf("first PinnedCertPool: %v", err)
	}
	freshSecond, err := PinnedCertPool()
	if err != nil {
		t.Fatalf("second PinnedCertPool: %v", err)
	}
	if freshFirst == freshSecond || freshFirst == first.RootCAs || freshSecond == first.RootCAs {
		t.Fatal("PinnedCertPool returned a shared default pool")
	}
}

// Creates one private CA and IP leaf so the additional-root path is proved by
// an actual x509 verification rather than by pool shape alone.
func testingPrivatePlatformCertificates(t *testing.T) ([]byte, *x509.Certificate) {
	t.Helper()
	_, caPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "private platform test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}
	caDer, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caPrivateKey.Public(), caPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	caCertificate, err := x509.ParseCertificate(caDer)
	if err != nil {
		t.Fatal(err)
	}
	_, leafPrivateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.1.1"},
		NotBefore:    now.Add(-time.Hour),
		NotAfter:     now.Add(time.Hour),
		IPAddresses:  []net.IP{net.ParseIP("127.0.1.1")},
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	leafDer, err := x509.CreateCertificate(rand.Reader, leafTemplate, caCertificate, leafPrivateKey.Public(), caPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	leafCertificate, err := x509.ParseCertificate(leafDer)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDer}), leafCertificate
}

func TestDefaultTlsConfigAddsExplicitPrivateRootWithoutMutatingDefaults(t *testing.T) {
	caPem, leafCertificate := testingPrivatePlatformCertificates(t)
	caFile := filepath.Join(t.TempDir(), "platform-ca.pem")
	if err := os.WriteFile(caFile, caPem, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(ExtraRootCAFileEnv, caFile)
	first, err := DefaultTlsConfig()
	if err != nil {
		t.Fatalf("first configured DefaultTlsConfig: %v", err)
	}
	second, err := DefaultTlsConfig()
	if err != nil {
		t.Fatalf("second configured DefaultTlsConfig: %v", err)
	}
	if first.RootCAs == nil || first.RootCAs == second.RootCAs {
		t.Fatal("configured TLS roots were not isolated per config")
	}
	if _, err := leafCertificate.Verify(x509.VerifyOptions{Roots: first.RootCAs, DNSName: "127.0.1.1"}); err != nil {
		t.Fatalf("configured private platform certificate was not trusted: %v", err)
	}

	t.Setenv(ExtraRootCAFileEnv, "")
	ordinary, err := DefaultTlsConfig()
	if err != nil {
		t.Fatalf("ordinary DefaultTlsConfig: %v", err)
	}
	if _, err := leafCertificate.Verify(x509.VerifyOptions{Roots: ordinary.RootCAs, DNSName: "127.0.1.1"}); err == nil {
		t.Fatal("private root contaminated the immutable default pool")
	}
}

func TestDefaultTlsConfigRejectsInvalidExplicitPrivateRoot(t *testing.T) {
	directory := t.TempDir()
	malformed := filepath.Join(directory, "malformed.pem")
	if err := os.WriteFile(malformed, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	caPem, leafCertificate := testingPrivatePlatformCertificates(t)
	leafOnly := filepath.Join(directory, "leaf.pem")
	if err := os.WriteFile(leafOnly, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafCertificate.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	trailing := filepath.Join(directory, "trailing.pem")
	if err := os.WriteFile(trailing, append(append([]byte(nil), caPem...), []byte("unreviewed trailing data\n")...), 0o600); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		path   string
		marker string
	}{
		{path: "relative.pem", marker: "absolute path"},
		{path: filepath.Join(directory, "missing.pem"), marker: "no such file"},
		{path: directory, marker: "regular file"},
		{path: malformed, marker: "malformed PEM"},
		{path: leafOnly, marker: "without CA signing authority"},
		{path: trailing, marker: "malformed PEM"},
	}
	for _, test := range cases {
		t.Setenv(ExtraRootCAFileEnv, test.path)
		if _, err := DefaultTlsConfig(); err == nil || !strings.Contains(err.Error(), test.marker) {
			t.Errorf("root %q error=%v, want marker %q", test.path, err, test.marker)
		}
	}
}

func TestStrictPrivateRootsAcceptOnlyCompleteCABundles(t *testing.T) {
	caPem, _ := testingPrivatePlatformCertificates(t)
	pool := x509.NewCertPool()
	if err := appendStrictRootCertificates(pool, append(append([]byte(nil), caPem...), caPem...)); err != nil {
		t.Fatalf("complete two-CA bundle: %v", err)
	}
	if len(pool.Subjects()) == 0 {
		t.Fatal("strict root parser added no CA subjects")
	}
	for _, contents := range [][]byte{
		nil,
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte{1, 2, 3}}),
		append([]byte("comment\n"), caPem...),
	} {
		if err := appendStrictRootCertificates(x509.NewCertPool(), contents); err == nil {
			t.Errorf("strict root parser accepted %q", contents)
		}
	}
}
