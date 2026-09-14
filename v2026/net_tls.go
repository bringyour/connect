package connect

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"golang.org/x/crypto/cryptobyte"

	"src.agwa.name/tlshacks"

	_ "embed"
)

const clientTlsSessionCacheCapacity = 16

// Names the optional private-platform CA bundle used by headless deployments.
// The default remains the immutable Let's Encrypt pin set; an explicitly
// configured absolute file adds roots without replacing those public pins.
const ExtraRootCAFileEnv = "URNETWORK_CONNECT_EXTRA_ROOT_CA_FILE"

var (
	clientHttpNextProtos      = []string{"h2", "http/1.1"}
	clientWebSocketNextProtos = []string{"http/1.1"}
)

// newClientTlsConfig isolates TLS session tickets to one dial path and, when
// nextProtos is non-nil, advertises the application protocols that path's
// caller can actually speak. A distinct bounded cache per dialer avoids
// linking direct, resilient, and extender egress addresses while making an
// unavoidable re-dial cheaper.
func newClientTlsConfig(base *tls.Config, nextProtos []string) *tls.Config {
	var config *tls.Config
	if base == nil {
		config = &tls.Config{}
	} else {
		config = base.Clone()
	}
	if nextProtos != nil {
		config.NextProtos = append([]string(nil), nextProtos...)
	}
	// Always replace a caller-supplied cache. The strategy derives separate
	// configs for ordinary HTTP, WebSocket, every resilient mode, and every
	// extender; sharing the base config's cache would let a server correlate
	// those otherwise-independent egress paths through a redeemed ticket.
	config.ClientSessionCache = tls.NewLRUClientSessionCache(clientTlsSessionCacheCapacity)
	return config
}

// the let's encrypt root CAs as defined at https://letsencrypt.org/certificates/
// this includes:
// - ISRG Root X1
// - ISRG Root X2
//
//go:embed net_tls_ca.pem
var tlsCaPem string

func PinnedCertPool() (*x509.CertPool, error) {

	certPool := x509.NewCertPool()

	if !certPool.AppendCertsFromPEM([]byte(tlsCaPem)) {
		return nil, fmt.Errorf("Could not append ca certs")
	}

	return certPool, nil
}

var (
	defaultPinnedCertPoolOnce sync.Once
	defaultPinnedCertPool     *x509.CertPool
	defaultPinnedCertPoolErr  error
)

// sharedDefaultPinnedCertPool parses the embedded immutable roots once for
// the SDK's default client configurations. A connection gets its own TLS
// session cache below, but x509 verification only reads RootCAs, so duplicating
// the same parsed certificates for every Auto exit is allocation and retained
// memory with no isolation benefit. PinnedCertPool intentionally remains the
// public fresh-pool API for callers that plan to mutate their result.
func sharedDefaultPinnedCertPool() (*x509.CertPool, error) {
	defaultPinnedCertPoolOnce.Do(func() {
		defaultPinnedCertPool, defaultPinnedCertPoolErr = PinnedCertPool()
	})
	return defaultPinnedCertPool, defaultPinnedCertPoolErr
}

func appendStrictRootCertificates(certPool *x509.CertPool, contents []byte) error {
	remaining := bytes.TrimSpace(contents)
	count := 0
	for len(remaining) != 0 {
		if !bytes.HasPrefix(remaining, []byte("-----BEGIN CERTIFICATE-----")) {
			return errors.New("root bundle contains non-certificate or malformed PEM data")
		}
		block, rest := pem.Decode(remaining)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return errors.New("root bundle contains non-certificate or malformed PEM data")
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return fmt.Errorf("parse root certificate: %w", err)
		}
		if !certificate.BasicConstraintsValid || !certificate.IsCA || certificate.KeyUsage&x509.KeyUsageCertSign == 0 {
			return errors.New("root bundle contains a certificate without CA signing authority")
		}
		certPool.AddCert(certificate)
		count++
		remaining = bytes.TrimSpace(rest)
	}
	if count == 0 {
		return errors.New("root bundle contains no certificates")
	}
	return nil
}

// Builds the root set for one client configuration. Ordinary clients retain
// the shared immutable pool. A private deployment gets a clone before its
// explicit roots are appended, so one process cannot mutate another config's
// default trust set.
func defaultRootCertPool() (*x509.CertPool, error) {
	certPool, err := sharedDefaultPinnedCertPool()
	if err != nil {
		return nil, err
	}
	extraRootCAFile := strings.TrimSpace(os.Getenv(ExtraRootCAFileEnv))
	if extraRootCAFile == "" {
		return certPool, nil
	}
	if !filepath.IsAbs(extraRootCAFile) {
		return nil, fmt.Errorf("%s must be an absolute path", ExtraRootCAFileEnv)
	}
	info, err := os.Stat(extraRootCAFile)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", ExtraRootCAFileEnv, err)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s is not a regular file", ExtraRootCAFileEnv)
	}
	extraRootCAPem, err := os.ReadFile(extraRootCAFile)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", ExtraRootCAFileEnv, err)
	}
	configuredCertPool := certPool.Clone()
	if err := appendStrictRootCertificates(configuredCertPool, extraRootCAPem); err != nil {
		return nil, fmt.Errorf("%s: %w", ExtraRootCAFileEnv, err)
	}
	return configuredCertPool, nil
}

// tlsClientSessionCacheCapacity sizes the per-config LRU session cache. The
// client talks to a handful of platform hosts (api, connect, extenders resolved
// to the platform), each caching at most a couple of tickets, so 32 is
// generous while bounding memory to a few KB.
const tlsClientSessionCacheCapacity = 32

func DefaultTlsConfig() (*tls.Config, error) {
	certPool, err := defaultRootCertPool()
	if err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS12,
		// Session resumption across re-dials (C4). Without a cache every
		// reconnect pays a full handshake -- an extra round trip plus the
		// certificate exchange -- exactly when latency matters most (a
		// network migration re-dialing every transport at once).
		//
		// One cache per DefaultTlsConfig() result, shared by reference by
		// every Clone() of it: the resilient/fragmenting dialers clone
		// ConnectSettings.TlsConfig per dial (net_resilient.go), and the h3
		// transport clones QuicTlsConfig per dial (transport.go, where
		// quic-go also uses it for 0-RTT via DialEarly) -- all those clones
		// resume against the same tickets, which is the point. Sharing one
		// cache across hosts is safe: entries are keyed by session key
		// (server name / addr), so same-host re-dials hit and distinct hosts
		// never collide. The pinned RootCAs pool composes with resumption --
		// a ticket only exists for a session whose chain already verified
		// against the pinned pool, and Go re-validates a cached session
		// before offering it, falling back to a full (re-verified) handshake
		// whenever the server declines the ticket.
		//
		// Deliberately NOT attached to the extender configs
		// (net_extender.go): those spoof unrelated fronted server names with
		// InsecureSkipVerify, and a shared ticket cache could link separate
		// extender profiles to an observer. They keep full handshakes.
		ClientSessionCache: tls.NewLRUClientSessionCache(tlsClientSessionCacheCapacity),
	}
	return tlsConfig, nil
}

// RFC 5246
type TlsContentType = byte

const (
	TlsContentTypeChangeCipherSpec TlsContentType = 0x14
	TlsContentTypeAlert            TlsContentType = 0x15
	TlsContentTypeHandshake        TlsContentType = 0x16
	TlsContentTypeApplicationData  TlsContentType = 0x17
	TlsContentTypeHeartbeat        TlsContentType = 0x18
)

type TlsVersion = uint16

const (
	// RFC 8446
	TlsVersion1_3 TlsVersion = 0x0304
	// RFC 5246
	TlsVersion1_2 TlsVersion = 0x0303
	// RFC 8996
	TlsVersion1_1 TlsVersion = 0x0302
	TlsVersion1_0 TlsVersion = 0x0301
)

type tlsHeader struct {
	contentType   TlsContentType
	tlsVersion    TlsVersion
	contentLength uint16
}

func parseTlsHeader(b []byte) *tlsHeader {
	return &tlsHeader{
		contentType:   b[0],
		tlsVersion:    binary.BigEndian.Uint16(b[1:3]),
		contentLength: binary.BigEndian.Uint16(b[3:5]),
	}
}

func (self *tlsHeader) reconstruct(content []byte) []byte {
	b := make([]byte, 5+len(content))
	b[0] = self.contentType
	binary.BigEndian.PutUint16(b[1:3], self.tlsVersion)
	binary.BigEndian.PutUint16(b[3:5], uint16(len(content)))
	copy(b[5:5+len(content)], content)
	return b
}

func (self *tlsHeader) valid() bool {
	switch self.contentType {
	case TlsContentTypeChangeCipherSpec, TlsContentTypeAlert, TlsContentTypeHandshake, TlsContentTypeApplicationData, TlsContentTypeHeartbeat:
	default:
		return false
	}
	switch self.tlsVersion {
	case TlsVersion1_3, TlsVersion1_2, TlsVersion1_1, TlsVersion1_0:
	default:
		return false
	}
	return true
}

// https://github.com/AGWA/tlshacks/blob/main/client_hello.go

type clientHelloMeta struct {
	ServerNameValueStart int
	ServerNameValueEnd   int
}

func UnmarshalClientHello(handshakeBytes []byte) (*tlshacks.ClientHelloInfo, *clientHelloMeta) {
	info := &tlshacks.ClientHelloInfo{
		Raw: handshakeBytes,
	}
	meta := &clientHelloMeta{}
	handshakeMessage := cryptobyte.String(handshakeBytes)

	handshakeMessageLength := len(handshakeMessage)

	var messageType uint8
	if !handshakeMessage.ReadUint8(&messageType) || messageType != 1 {
		// fmt.Printf("hello 1\n")
		return nil, nil
	}

	handshakeStart := handshakeMessageLength - len(handshakeMessage)

	var clientHello cryptobyte.String
	if !handshakeMessage.ReadUint24LengthPrefixed(&clientHello) || !handshakeMessage.Empty() {
		// fmt.Printf("hello 2\n")
		return nil, nil
	}

	clientHelloLength := len(clientHello)

	if !clientHello.ReadUint16((*uint16)(&info.Version)) {
		// fmt.Printf("hello 3\n")
		return nil, nil
	}

	if !clientHello.ReadBytes(&info.Random, 32) {
		// fmt.Printf("hello 4\n")
		return nil, nil
	}

	if !clientHello.ReadUint8LengthPrefixed((*cryptobyte.String)(&info.SessionID)) {
		// fmt.Printf("hello 5\n")
		return nil, nil
	}

	var cipherSuites cryptobyte.String
	if !clientHello.ReadUint16LengthPrefixed(&cipherSuites) {
		// fmt.Printf("hello 6\n")
		return nil, nil
	}
	info.CipherSuites = []tlshacks.CipherSuite{}
	for !cipherSuites.Empty() {
		// fmt.Printf("[tls]P1\n")
		var suite uint16
		if !cipherSuites.ReadUint16(&suite) {
			// fmt.Printf("hello 7\n")
			return nil, nil
		}
		info.CipherSuites = append(info.CipherSuites, tlshacks.MakeCipherSuite(suite))
	}

	var compressionMethods cryptobyte.String
	if !clientHello.ReadUint8LengthPrefixed(&compressionMethods) {
		// fmt.Printf("hello 8\n")
		return nil, nil
	}
	info.CompressionMethods = []tlshacks.CompressionMethod{}
	for !compressionMethods.Empty() {
		// fmt.Printf("[tls]P2\n")
		var method uint8
		if !compressionMethods.ReadUint8(&method) {
			// fmt.Printf("hello 9\n")
			return nil, nil
		}
		info.CompressionMethods = append(info.CompressionMethods, tlshacks.CompressionMethod(method))
	}

	info.Extensions = []tlshacks.Extension{}

	if clientHello.Empty() {
		// fmt.Printf("hello 10\n")
		return info, meta
	}

	clientHelloStart := clientHelloLength - len(clientHello)

	var extensions cryptobyte.String
	if !clientHello.ReadUint16LengthPrefixed(&extensions) {
		// fmt.Printf("hello 11\n")
		return nil, nil
	}
	extensionsLength := len(extensions)

	extensionParsers := map[uint16]func([]byte) tlshacks.ExtensionData{
		0:  tlshacks.ParseServerNameData,
		10: tlshacks.ParseSupportedGroupsData,
		11: tlshacks.ParseECPointFormatsData,
		16: tlshacks.ParseALPNData,
		18: tlshacks.ParseEmptyExtensionData,
		22: tlshacks.ParseEmptyExtensionData,
		23: tlshacks.ParseEmptyExtensionData,
		49: tlshacks.ParseEmptyExtensionData,
	}

	for !extensions.Empty() {
		// fmt.Printf("[tls]P3\n")
		var extType uint16
		var extData cryptobyte.String

		start := extensionsLength - len(extensions)
		if !extensions.ReadUint16(&extType) || !extensions.ReadUint16LengthPrefixed(&extData) {
			// fmt.Printf("hello 12\n")
			return nil, nil
		}
		end := extensionsLength - len(extensions)

		parseData := extensionParsers[extType]
		if parseData == nil {
			parseData = tlshacks.ParseUnknownExtensionData
		}
		data := parseData(extData)

		info.Extensions = append(info.Extensions, tlshacks.Extension{
			Type:    extType,
			Name:    tlshacks.Extensions[extType].Name,
			Grease:  tlshacks.Extensions[extType].Grease,
			Private: tlshacks.Extensions[extType].Private,
			Data:    data,
		})

		switch extType {
		case 0:
			info.Info.ServerName = &data.(*tlshacks.ServerNameData).HostName
			meta.ServerNameValueStart = handshakeStart + clientHelloStart + start
			meta.ServerNameValueEnd = handshakeStart + clientHelloStart + end
		case 16:
			info.Info.Protocols = data.(*tlshacks.ALPNData).Protocols
		case 18:
			info.Info.SCTs = true
		}

	}

	if !clientHello.Empty() {
		return nil, nil
	}

	// fmt.Printf("[tls]P4\n")

	info.Info.JA3String = tlshacks.JA3String(info)
	info.Info.JA3Fingerprint = tlshacks.JA3Fingerprint(info.Info.JA3String)

	// fmt.Printf("hello 14\n")
	return info, meta
}

func UnmarshalClientHelloServerName(handshakeBytes []byte) string {
	handshakeMessage := cryptobyte.String(handshakeBytes)

	var messageType uint8
	if !handshakeMessage.ReadUint8(&messageType) || messageType != 1 {
		// fmt.Printf("hello 1\n")
		return ""
	}

	var clientHello cryptobyte.String
	if !handshakeMessage.ReadUint24LengthPrefixed(&clientHello) || !handshakeMessage.Empty() {
		// fmt.Printf("hello 2\n")
		return ""
	}

	var version uint16
	if !clientHello.ReadUint16((*uint16)(&version)) {
		// fmt.Printf("hello 3\n")
		return ""
	}

	var random []byte
	if !clientHello.ReadBytes(&random, 32) {
		// fmt.Printf("hello 4\n")
		return ""
	}

	var sessionId cryptobyte.String
	if !clientHello.ReadUint8LengthPrefixed(&sessionId) {
		// fmt.Printf("hello 5\n")
		return ""
	}

	var cipherSuites cryptobyte.String
	if !clientHello.ReadUint16LengthPrefixed(&cipherSuites) {
		// fmt.Printf("hello 6\n")
		return ""
	}
	// info.CipherSuites = []tlshacks.CipherSuite{}
	// for !cipherSuites.Empty() {
	// 	fmt.Printf("[tls]P1\n")
	// 	var suite uint16
	// 	if !cipherSuites.ReadUint16(&suite) {
	// 		// fmt.Printf("hello 7\n")
	// 		return nil, nil
	// 	}
	// 	info.CipherSuites = append(info.CipherSuites, tlshacks.MakeCipherSuite(suite))
	// }

	var compressionMethods cryptobyte.String
	if !clientHello.ReadUint8LengthPrefixed(&compressionMethods) {
		// fmt.Printf("hello 8\n")
		return ""
	}
	// info.CompressionMethods = []tlshacks.CompressionMethod{}
	// for !compressionMethods.Empty() {
	// 	fmt.Printf("[tls]P2\n")
	// 	var method uint8
	// 	if !compressionMethods.ReadUint8(&method) {
	// 		// fmt.Printf("hello 9\n")
	// 		return nil, nil
	// 	}
	// 	info.CompressionMethods = append(info.CompressionMethods, tlshacks.CompressionMethod(method))
	// }

	// info.Extensions = []tlshacks.Extension{}

	if clientHello.Empty() {
		// fmt.Printf("hello 10\n")
		return ""
	}

	// clientHelloStart := clientHelloLength - len(clientHello)

	var extensions cryptobyte.String
	if !clientHello.ReadUint16LengthPrefixed(&extensions) {
		// fmt.Printf("hello 11\n")
		return ""
	}

	for !extensions.Empty() {
		var extType uint16
		var extData cryptobyte.String

		if !extensions.ReadUint16(&extType) || !extensions.ReadUint16LengthPrefixed(&extData) {
			// fmt.Printf("hello 12\n")
			return ""
		}

		switch extType {
		case 0:
			data := tlshacks.ParseServerNameData(extData)
			return data.(*tlshacks.ServerNameData).HostName
		}
	}

	return ""
}
