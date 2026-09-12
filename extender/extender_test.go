package extender

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"time"

	"testing"

	"github.com/urnetwork/connect"
)

func TestExtender(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}

	settings := DefaultExtenderSettings()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	certificate, err := selfSignedCertificate(
		[]string{"127.0.0.1"},
		"Connect Test",
		settings.ValidFrom,
		settings.ValidFor,
	)
	if err != nil {
		t.Fatal(err)
	}

	contentListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	contentPort := contentListener.Addr().(*net.TCPAddr).Port
	server := &http.Server{
		Handler: &testExtenderServer{},
		TLSConfig: &tls.Config{
			Certificates: []tls.Certificate{*certificate},
		},
	}
	contentDone := make(chan error, 1)
	go func() {
		contentDone <- server.ServeTLS(contentListener, "", "")
	}()
	t.Cleanup(func() {
		server.Close()
		if err := <-contentDone; err != nil && !errors.Is(err, http.ErrServerClosed) && !errors.Is(err, net.ErrClosed) {
			t.Errorf("content server: %v", err)
		}
	})

	extenderListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	extenderPort := extenderListener.Addr().(*net.TCPAddr).Port
	listenerClaimed := false
	settings.Listen = func(network string, address string) (net.Listener, error) {
		if network != "tcp" || address != fmt.Sprintf(":%d", extenderPort) {
			return nil, fmt.Errorf("unexpected extender listen %s %s", network, address)
		}
		if listenerClaimed {
			return nil, fmt.Errorf("extender listener requested more than once")
		}
		listenerClaimed = true
		return extenderListener, nil
	}
	handlerErrors := make(chan error, 1)
	settings.ErrorHandler = func(stage string, err error) {
		select {
		case handlerErrors <- fmt.Errorf("%s: %w", stage, err):
		default:
		}
	}

	extenderServer := NewExtenderServer(
		ctx,
		[]string{"montrose"},
		[]string{"127.0.0.1"},
		map[int][]connect.ExtenderConnectMode{
			extenderPort: {connect.ExtenderConnectModeTcpTls},
		},
		&net.Dialer{},
		settings,
	)
	extenderDone := make(chan error, 1)
	go func() {
		extenderDone <- extenderServer.ListenAndServe()
	}()
	t.Cleanup(func() {
		extenderServer.CloseAndWait()
		if err := <-extenderDone; err != nil {
			t.Errorf("extender server: %v", err)
		}
	})

	localIp, err := netip.ParseAddr("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}

	rootCAs := x509.NewCertPool()
	rootCAs.AddCert(certificate.Leaf)
	connectSettings := connect.DefaultConnectSettings()
	connectSettings.TlsConfig = &tls.Config{
		RootCAs: rootCAs,
	}

	client := connect.NewExtenderHttpClient(
		connectSettings,
		&connect.ExtenderConfig{
			Profile: connect.ExtenderProfile{
				ConnectMode: connect.ExtenderConnectModeTcpTls,
				ServerName:  "front.example",
				Port:        extenderPort,
			},
			Ip:     localIp,
			Secret: "montrose",
		},
	)
	t.Cleanup(client.CloseIdleConnections)

	response, err := client.Get(fmt.Sprintf("https://127.0.0.1:%d/hello", contentPort))
	if err != nil {
		select {
		case handlerErr := <-handlerErrors:
			t.Fatalf("request: %v; extender: %v", err, handlerErr)
		default:
			t.Fatal(err)
		}
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, expected %d", response.StatusCode, http.StatusOK)
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(body) != "{}" {
		t.Fatalf("body = %q, expected %q", body, "{}")
	}

}

// The self-signed leaf spans the moment it was created: ValidFrom is the
// tolerated history before creation and ValidFor the lifetime after it.
func TestSelfSignedCertificateValiditySpansPresent(t *testing.T) {
	before := time.Now()
	certificate, err := selfSignedCertificate(
		[]string{"leaf.example"},
		"Connect Test",
		2*time.Hour,
		3*time.Hour,
	)
	if err != nil {
		t.Fatal(err)
	}
	after := time.Now()
	if delta := certificate.Leaf.NotBefore.Sub(before.Add(-2 * time.Hour)); delta < -time.Second || time.Second < delta {
		t.Fatalf("NotBefore = %s, expected about two hours before creation", certificate.Leaf.NotBefore)
	}
	if delta := certificate.Leaf.NotAfter.Sub(after.Add(3 * time.Hour)); delta < -time.Second || time.Second < delta {
		t.Fatalf("NotAfter = %s, expected about three hours after creation", certificate.Leaf.NotAfter)
	}
	if before.Before(certificate.Leaf.NotBefore) || certificate.Leaf.NotAfter.Before(after) {
		t.Fatalf(
			"certificate validity %s..%s does not span creation",
			certificate.Leaf.NotBefore,
			certificate.Leaf.NotAfter,
		)
	}
}

// A name is issued once and the cached leaf is reused, so a handshake never
// generates a key (B3).
func TestExtenderCertificatesCacheLeavesPerServerName(t *testing.T) {
	settings := DefaultExtenderSettings()
	certificates, err := newExtenderCertificates(nil, settings)
	if err != nil {
		t.Fatal(err)
	}
	first, err := certificates.certificateForServerName("one.example")
	if err != nil {
		t.Fatal(err)
	}
	again, err := certificates.certificateForServerName("one.example")
	if err != nil {
		t.Fatal(err)
	}
	if first != again {
		t.Fatal("the same server name was issued twice")
	}
	other, err := certificates.certificateForServerName("two.example")
	if err != nil {
		t.Fatal(err)
	}
	if first == other {
		t.Fatal("two server names share one certificate")
	}
	if first.Leaf.DNSNames[0] != "one.example" || other.Leaf.DNSNames[0] != "two.example" {
		t.Fatalf("issued names = %v, %v", first.Leaf.DNSNames, other.Leaf.DNSNames)
	}
	if 1 < len(first.Certificate) {
		t.Fatal("an extender without an identity key issued a chain")
	}
}

// With an identity key the leaf is issued under the self-signed ed25519 ca,
// and the leaf signature verifies under the identity key (B3).
func TestExtenderCertificatesIssueUnderTheIdentityKey(t *testing.T) {
	seed, err := connect.NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	settings := DefaultExtenderSettings()
	certificates, err := newExtenderCertificates(seed, settings)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := connect.ExtenderPublicKeyFromSeed(seed)
	if err != nil {
		t.Fatal(err)
	}
	if string(certificates.PublicKey()) != string(publicKey) {
		t.Fatal("the published key is not the identity key")
	}
	certificate, err := certificates.certificateForServerName("leaf.example")
	if err != nil {
		t.Fatal(err)
	}
	if len(certificate.Certificate) != 2 {
		t.Fatalf("certificate chain has %d entries, expected the leaf and the ca", len(certificate.Certificate))
	}
	if certificate.Leaf.SignatureAlgorithm != x509.PureEd25519 {
		t.Fatalf("leaf signature algorithm = %s, expected ed25519", certificate.Leaf.SignatureAlgorithm)
	}
	caCertificate, err := x509.ParseCertificate(certificate.Certificate[1])
	if err != nil {
		t.Fatal(err)
	}
	if !caCertificate.IsCA {
		t.Fatal("the issuing certificate is not a ca")
	}
	if err := certificate.Leaf.CheckSignatureFrom(caCertificate); err != nil {
		t.Fatalf("the leaf is not signed by the ca: %v", err)
	}
}

type testExtenderServer struct {
}

func (self *testExtenderServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {

	w.Header().Add("Content-Type", "application/json")
	w.Write([]byte("{}"))
}
