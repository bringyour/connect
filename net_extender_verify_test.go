package connect

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/netip"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/protocol"
)

// The client side of the extender request and of the outer verification
// (EXTENDER.md A3, A4, B3).
//
// The outer leaf check is the only authentication a client has of the extender
// it dials: `InsecureSkipVerify` stays on and no roots are consulted, so every
// way the check can be skipped is a way a substituted extender passes. Each
// rejection branch is pinned here rather than only through a live handshake,
// where a failure looks the same whichever branch produced it.

// The verifier rejects everything but a leaf that the record key itself
// signed. The branches differ in what an attacker controls, so each is pinned
// separately.
func TestExtenderLeafVerifierRejections(t *testing.T) {
	privateKey, publicKey := newTestRootKey(t)
	_, otherPublicKey := newTestRootKey(t)

	leafBytes := newTestEd25519LeafCertificate(t, privateKey)
	ecdsaLeafBytes := newTestEcdsaLeafCertificate(t)

	// the record key that signed the leaf accepts it
	if err := newExtenderLeafVerifier(publicKey)([][]byte{leafBytes}, nil); err != nil {
		t.Fatalf("a leaf signed by the record key was rejected: %v", err)
	}

	cases := []struct {
		name        string
		publicKey   []byte
		rawCerts    [][]byte
		wantMessage string
	}{
		{
			name:        "short key",
			publicKey:   publicKey[:ed25519.PublicKeySize-1],
			rawCerts:    [][]byte{leafBytes},
			wantMessage: "not an ed25519 key",
		},
		{
			name:        "nil key",
			publicKey:   nil,
			rawCerts:    [][]byte{leafBytes},
			wantMessage: "not an ed25519 key",
		},
		{
			name:        "no certificate",
			publicKey:   publicKey,
			rawCerts:    nil,
			wantMessage: "presented no certificate",
		},
		{
			name:        "empty chain",
			publicKey:   publicKey,
			rawCerts:    [][]byte{},
			wantMessage: "presented no certificate",
		},
		{
			name:      "unparseable leaf",
			publicKey: publicKey,
			rawCerts:  [][]byte{{0x30, 0x00}},
			// the x509 parse error is reported as it stands
			wantMessage: "",
		},
		{
			name:        "leaf signed with another algorithm",
			publicKey:   publicKey,
			rawCerts:    [][]byte{ecdsaLeafBytes},
			wantMessage: "expected ed25519",
		},
		{
			name:        "leaf signed by another key",
			publicKey:   otherPublicKey,
			rawCerts:    [][]byte{leafBytes},
			wantMessage: "not signed by the record key",
		},
	}
	for _, c := range cases {
		err := newExtenderLeafVerifier(c.publicKey)(c.rawCerts, nil)
		if err == nil {
			t.Errorf("%s was accepted", c.name)
			continue
		}
		if c.wantMessage != "" && !strings.Contains(err.Error(), c.wantMessage) {
			t.Errorf("%s failed with %q, expected %q", c.name, err, c.wantMessage)
		}
	}
}

// The verifier copies the key it was built with, so a caller that reuses its
// buffer cannot retarget a live verifier at another identity.
func TestExtenderLeafVerifierCopiesItsKey(t *testing.T) {
	privateKey, publicKey := newTestRootKey(t)
	leafBytes := newTestEd25519LeafCertificate(t, privateKey)

	keyBytes := append([]byte(nil), publicKey...)
	verify := newExtenderLeafVerifier(keyBytes)
	for i := range keyBytes {
		keyBytes[i] = 0
	}
	if err := newExtenderLeafVerifier(keyBytes)([][]byte{leafBytes}, nil); err == nil {
		t.Fatal("the zeroed key still verified, so the case is not meaningful")
	}
	if err := verify([][]byte{leafBytes}, nil); err != nil {
		t.Fatalf("the verifier followed the caller's buffer: %v", err)
	}
}

// The response frame is bounded on both sides at the header cap, so neither
// end can be made to allocate or read past it (A3).
func TestExtenderResponseFrameIsBoundedBothWays(t *testing.T) {
	// a response that serializes over the cap is refused rather than written
	oversized := &protocol.ExtenderResponse{
		PublicKey: make([]byte, ExtenderMaxHeaderByteCount+1),
	}
	if frameBytes, err := ExtenderResponseFrame(oversized); err == nil {
		t.Errorf("an oversized response was framed as %d bytes", len(frameBytes))
	}
	// one that fits is framed with its length prefix
	response := &protocol.ExtenderResponse{
		Carriers: []string{ExtenderCarrierTcp},
	}
	frameBytes, err := ExtenderResponseFrame(response)
	if err != nil {
		t.Fatal(err)
	}
	if len(frameBytes) <= 4 {
		t.Fatalf("the frame is %d bytes", len(frameBytes))
	}

	// a length prefix over the cap is refused before the body is allocated
	overlong := []byte{0xff, 0xff, 0xff, 0xff}
	if _, err := ReadExtenderResponseFrame(strings.NewReader(string(overlong))); err == nil {
		t.Error("an overlong response length was accepted")
	}
	// a truncated body is the read error, not a short response
	truncated := append([]byte(nil), frameBytes...)
	if _, err := ReadExtenderResponseFrame(
		strings.NewReader(string(truncated[:len(truncated)-1])),
	); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Errorf("a truncated frame returned %v", err)
	}
	// a truncated length prefix is likewise a read error
	if _, err := ReadExtenderResponseFrame(strings.NewReader("ab")); err == nil {
		t.Error("a truncated length prefix was accepted")
	}
	// a body that is not a response does not decode
	badBody := []byte{0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff}
	if _, err := ReadExtenderResponseFrame(strings.NewReader(string(badBody))); err == nil {
		t.Error("a body that is not a response decoded")
	}
}

// The client refuses to send a header over the same cap the extender refuses
// to read, so an oversized destination name fails here rather than as an
// opaque 403 (A3, A4).
func TestExtenderRequestHeaderIsBoundedAtTheCap(t *testing.T) {
	extenderConfig := &ExtenderConfig{
		Profile: ExtenderProfile{
			ConnectMode: ExtenderConnectModeTcpTls,
			ServerName:  "spoof.example",
			Port:        443,
		},
	}
	headerBytes, err := extenderRequestHeaderBytes(extenderConfig, &ExtenderDial{
		DestinationHost: "dest.example",
		DestinationPort: 443,
	})
	if err != nil {
		t.Fatal(err)
	}
	if ExtenderMaxHeaderByteCount < len(headerBytes) {
		t.Fatalf("an ordinary header is %d bytes", len(headerBytes))
	}
	header := &protocol.ExtenderHeader{}
	if err := proto.Unmarshal(headerBytes, header); err != nil {
		t.Fatal(err)
	}
	if header.DestinationHost != "dest.example" || header.DestinationPort != 443 {
		t.Fatalf("the header names %s:%d", header.DestinationHost, header.DestinationPort)
	}
	// without a secret there is no nonce and no signature to replay
	if 0 < len(header.Nonce) || 0 < len(header.Signature) {
		t.Fatal("an open extender header carried a signature")
	}

	oversized := strings.Repeat("a", ExtenderMaxHeaderByteCount+1) + ".example"
	if headerBytes, err := extenderRequestHeaderBytes(extenderConfig, &ExtenderDial{
		DestinationHost: oversized,
		DestinationPort: 443,
	}); err == nil {
		t.Errorf("an oversized header was built as %d bytes", len(headerBytes))
	}
}

// The request the client writes is the A3 shape, and its outer host is the
// spoof name when there is one and the address authority when there is not --
// the operator name never appears.
func TestExtenderRequestShapeAndHost(t *testing.T) {
	headerBytes := []byte("header")
	cases := []struct {
		serverName string
		wantHost   string
	}{
		{serverName: "spoof.example", wantHost: "spoof.example"},
		{serverName: "", wantHost: "192.0.2.10:443"},
	}
	for _, c := range cases {
		extenderConfig := &ExtenderConfig{
			Profile: ExtenderProfile{
				ConnectMode: ExtenderConnectModeTcpTls,
				ServerName:  c.serverName,
				Port:        443,
			},
			Ip: netip.MustParseAddr("192.0.2.10"),
		}
		request := newExtenderRequest(extenderConfig, headerBytes, nil)
		if request.Method != http.MethodPost {
			t.Errorf("method = %s", request.Method)
		}
		if request.URL.Path != "/" {
			t.Errorf("path = %s", request.URL.Path)
		}
		if request.URL.Host != "192.0.2.10:443" {
			t.Errorf("url host = %s", request.URL.Host)
		}
		if contentType := request.Header.Get("Content-Type"); contentType != ExtenderContentType {
			t.Errorf("content type = %s", contentType)
		}
		if request.ContentLength != int64(len(headerBytes)) {
			t.Errorf("content length = %d", request.ContentLength)
		}
		if request.Host != c.wantHost {
			t.Errorf("host = %q, expected %q", request.Host, c.wantHost)
		}
	}

	// a v6 extender is reached at its bracketed authority
	extenderConfig := &ExtenderConfig{
		Profile: ExtenderProfile{ConnectMode: ExtenderConnectModeTcpTls, Port: 4053},
		Ip:      netip.MustParseAddr("2001:db8::10"),
	}
	request := newExtenderRequest(extenderConfig, headerBytes, nil)
	if request.URL.Host != "[2001:db8::10]:4053" || request.Host != "[2001:db8::10]:4053" {
		t.Fatalf("v6 authority = %s host = %s", request.URL.Host, request.Host)
	}
}

// The response read phase is bounded by the earlier of the caller deadline and
// its own budget, so an extender that accepts a connection and never answers
// cannot hold the dial open. The write sibling is covered in net_http_seam_test.go.
func TestExtenderReadPhaseDeadlineBoundsTheResponse(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	// the peer never writes, so only the phase budget ends the read
	readErr := withConnReadPhaseDeadline(
		context.Background(),
		clientConn,
		10*time.Millisecond,
		func() error {
			_, err := io.ReadFull(clientConn, make([]byte, 4))
			return err
		},
	)
	if !errors.Is(readErr, os.ErrDeadlineExceeded) {
		t.Fatalf("read error is %v, expected the phase deadline", readErr)
	}
	// the deadline is cleared, so a later read on the same connection is not
	// already expired
	go func() {
		serverConn.Write([]byte("ok"))
	}()
	buffer := make([]byte, 2)
	if _, err := io.ReadFull(clientConn, buffer); err != nil {
		t.Fatalf("the deadline was left standing: %v", err)
	}
}

// A caller deadline earlier than the phase budget wins, and a context that is
// already done never touches the connection at all.
func TestExtenderReadPhaseDeadlineTakesTheEarlierBound(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	readErr := withConnReadPhaseDeadline(ctx, clientConn, time.Hour, func() error {
		_, err := io.ReadFull(clientConn, make([]byte, 4))
		return err
	})
	if !errors.Is(readErr, os.ErrDeadlineExceeded) {
		t.Fatalf("read error is %v, expected the caller deadline", readErr)
	}

	canceledCtx, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	read := false
	if err := withConnReadPhaseDeadline(canceledCtx, clientConn, time.Hour, func() error {
		read = true
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("a canceled context returned %v", err)
	}
	if read {
		t.Fatal("a canceled context still read the connection")
	}
}

// With neither a caller deadline nor a phase budget the read runs unbounded,
// which is what a caller that owns its own timeout asks for.
func TestExtenderReadPhaseDeadlineWithoutABoundReadsDirectly(t *testing.T) {
	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()

	go func() {
		serverConn.Write([]byte("frame"))
	}()
	buffer := make([]byte, 5)
	if err := withConnReadPhaseDeadline(context.Background(), clientConn, 0, func() error {
		_, err := io.ReadFull(clientConn, buffer)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if string(buffer) != "frame" {
		t.Fatalf("read %q", buffer)
	}
}

// A leaf signed by an ed25519 key, which is the shape an extender issues under
// its identity certificate authority (B3).
func newTestEd25519LeafCertificate(t *testing.T, caPrivateKey ed25519.PrivateKey) []byte {
	t.Helper()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"Extender Test"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"spoof.example"},
	}
	leafPrivateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certificateBytes, err := x509.CreateCertificate(
		rand.Reader, template, template, leafPrivateKey.Public(), caPrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	return certificateBytes
}

// A self-signed ecdsa leaf, which is what an extender without an identity key
// issues -- the algorithm check refuses it for a client that expects a key.
func newTestEcdsaLeafCertificate(t *testing.T) []byte {
	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{Organization: []string{"Extender Test"}},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		DNSNames:     []string{"spoof.example"},
	}
	certificateBytes, err := x509.CreateCertificate(
		rand.Reader, template, template, privateKey.Public(), privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return certificateBytes
}
