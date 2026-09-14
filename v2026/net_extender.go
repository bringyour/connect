package connect

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"sync"
	"time"

	"crypto/ed25519"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"

	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// The extender client (EXTENDER.md A1, A3, A10, B3).
//
// An extender uses an independent url that forwards to the platform. The
// connection to the platform is end-to-end encrypted with TLS using the
// hostname of the destination, so an extender sees only ciphertext.
//
// Three carriers reach one extender, and all three yield one reliable byte
// stream: tcp 443 terminated TLS, udp 443 QUIC with ALPN h3, and udp 53 the
// same QUIC over the dns packet translation. On every carrier the client sends
// one `POST /` with the serialized ExtenderHeader as the body and reads an
// ExtenderResponse; after that the stream carries the inner bytes raw.
//
// The outer TLS is always InsecureSkipVerify: the extender presents a
// certificate for a spoof name it does not own. When the caller knows the
// extender's identity key from a signed record, VerifyPeerCertificate requires
// the presented leaf to be signed by that key (B3), which is the only outer
// authentication there is.

type ExtenderConnectMode string

const (
	ExtenderConnectModeTcpTls ExtenderConnectMode = "tcptls"
	ExtenderConnectModeQuic   ExtenderConnectMode = "quic"
	ExtenderConnectModeDns    ExtenderConnectMode = "dns"
)

// Carrier names as they appear in an ExtenderResponse and a record (A4, B2).
const (
	ExtenderCarrierTcp  = "tcp"
	ExtenderCarrierQuic = "quic"
	ExtenderCarrierDns  = "dns"
)

// Fixed carrier ports (A1, L2). The old multi-port personas are removed. A
// record may name other ports (B2); these are what an address with no record
// is dialed on. The dns carrier moved to the unprivileged 4053, which every
// extender binds; 53 is reached only through a record that lists it, since
// only the platforms that can bind it without privilege offer it.
const (
	ExtenderTcpPort  = 443
	ExtenderQuicPort = 443
	ExtenderDnsPort  = DefaultWhodisPort
)

// The connect mode of a carrier name, and whether the name is one this client
// can dial.
func ExtenderConnectModeForCarrier(carrier string) (ExtenderConnectMode, bool) {
	switch carrier {
	case ExtenderCarrierTcp:
		return ExtenderConnectModeTcpTls, true
	case ExtenderCarrierQuic:
		return ExtenderConnectModeQuic, true
	case ExtenderCarrierDns:
		return ExtenderConnectModeDns, true
	default:
		return "", false
	}
}

// Reserved services of the extender header (A8). 0 forwards to the
// destination; the others hand the taken-over stream to an in-process server.
const (
	ExtenderServiceForward uint32 = 0
	ExtenderServiceGossip  uint32 = 1
	ExtenderServiceFeed    uint32 = 2
)

// Content type of both the extender request and its response (A3).
const ExtenderContentType = "application/x-ur-extender"

// Maximum serialized extender header, request and response alike (A3).
const ExtenderMaxHeaderByteCount = 1024

// Encoding tld of the dns carrier when a record does not carry one (A1).
const DefaultExtenderDnsTld = "ur.xyz."

// The carrier name of a connect mode, as it appears on the wire.
func ExtenderCarrierForConnectMode(connectMode ExtenderConnectMode) string {
	switch connectMode {
	case ExtenderConnectModeQuic:
		return ExtenderCarrierQuic
	case ExtenderConnectModeDns:
		return ExtenderCarrierDns
	default:
		return ExtenderCarrierTcp
	}
}

// One carrier endpoint guess. Fragment and reorder apply to tcp only, and
// DnsTld only to the dns carrier. Comparable, so the strategy can hold a
// visited set of profiles.
type ExtenderProfile struct {
	ConnectMode ExtenderConnectMode
	ServerName  string
	Port        int
	Fragment    bool
	Reorder     bool
	DnsTld      string
}

type ExtenderConfig struct {
	Profile ExtenderProfile
	Ip      netip.Addr
	Secret  string
	// PublicKey, when set, is the extender identity key from a verified
	// record. The outer leaf certificate must be signed by it (B3). Empty
	// keeps the unauthenticated outer TLS of a manually configured extender.
	PublicKey []byte
}

func NewExtenderHttpClient(
	connectSettings *ConnectSettings,
	extenderConfig *ExtenderConfig,
) *http.Client {
	transport := &http.Transport{
		DialTLSContext:    newExtenderDialTlsContext(connectSettings, extenderConfig, clientHttpNextProtos),
		ForceAttemptHTTP2: true,
		HTTP2:             nativeHttp2Config(connectSettings),
	}
	return &http.Client{
		Transport: transport,
		Timeout:   connectSettings.RequestTimeout,
	}
}

// One extender request (A3, A4). The zero value forwards to the destination
// with no challenge, which is what an ordinary dial sends.
type ExtenderDial struct {
	DestinationHost string
	DestinationPort int
	// 32 random bytes; the response carries the signature over it. Probes and
	// activation set it, an ordinary dial leaves it empty.
	Challenge []byte
	// 0 forward, 1 gossip, 2 feed. DestinationHost is ignored when set.
	Service uint32
}

// create a tls connect to (destinationHost, destinationPort) on the connection
// returned by this
// the returned connection is not a tls connection
func NewExtenderDialTlsContext(
	connectSettings *ConnectSettings,
	extenderConfig *ExtenderConfig,
) DialTlsContextFunction {
	return newExtenderDialTlsContext(connectSettings, extenderConfig, nil)
}

func newExtenderDialTlsContext(
	connectSettings *ConnectSettings,
	extenderConfig *ExtenderConfig,
	nextProtos []string,
) DialTlsContextFunction {
	// one outer config per dialer, so its session cache is not shared with
	// any other egress path
	extenderTlsConfig := newExtenderTlsConfig(extenderConfig)
	innerBaseTlsConfig := newClientTlsConfig(connectSettings.TlsConfig, nextProtos)
	return func(
		ctx context.Context,
		network string,
		address string,
	) (net.Conn, error) {
		if network != "tcp" {
			panic(fmt.Errorf("Extender only support tcp network."))
		}

		host, portStr, err := net.SplitHostPort(address)
		if err != nil {
			panic(err)
		}
		port, err := strconv.Atoi(portStr)
		if err != nil {
			panic(err)
		}

		// the outer name is the dialer's spoof name, and nothing at all with
		// an empty spoof list (A10). The destination is deliberately not
		// substituted: it is the operator name this whole layer exists to keep
		// out of the outer ClientHello
		serverConn, _, err := dialExtenderStream(
			ctx,
			connectSettings,
			extenderConfig,
			&ExtenderDial{
				DestinationHost: host,
				DestinationPort: port,
			},
			extenderTlsConfig,
		)
		if err != nil {
			return nil, err
		}

		innerTlsConfig := innerBaseTlsConfig.Clone()
		if innerTlsConfig.ServerName == "" {
			innerTlsConfig.ServerName = host
		}
		tlsServerConn := tls.Client(serverConn, innerTlsConfig)

		// inner handshake; bound the timeout so a slow/malicious extender cannot
		// hold the dial open indefinitely
		innerSuccess := false
		defer func() {
			if !innerSuccess {
				tlsServerConn.Close()
			}
		}()
		func() {
			innerCtx, innerCancel := context.WithTimeout(ctx, connectSettings.TlsTimeout)
			defer innerCancel()
			err = tlsServerConn.HandshakeContext(innerCtx)
		}()
		if err != nil {
			return nil, err
		}
		innerSuccess = true

		return tlsServerConn, nil
	}
}

// DialExtender opens one carrier, performs the extender request and returns
// the raw stream that follows the response. The caller owns the returned
// connection, which for the udp carriers also owns the QUIC connection and its
// socket. Phase 1b probes and the gossip and feed clients use this directly;
// an ordinary dial goes through NewExtenderDialTlsContext, which runs the
// inner TLS on top.
func DialExtender(
	ctx context.Context,
	connectSettings *ConnectSettings,
	extenderConfig *ExtenderConfig,
	extenderDial *ExtenderDial,
) (net.Conn, *protocol.ExtenderResponse, error) {
	return dialExtenderStream(
		ctx,
		connectSettings,
		extenderConfig,
		extenderDial,
		newExtenderTlsConfig(extenderConfig),
	)
}

// The outer TLS configuration of one extender dialer. The spoof name is
// presented as the sni, an empty name presents no sni at all (A10), the
// self-signed leaf is never checked against a root, and 1.3 is required so the
// certificate is encrypted on the wire.
func newExtenderTlsConfig(extenderConfig *ExtenderConfig) *tls.Config {
	tlsConfig := newClientTlsConfig(&tls.Config{
		ServerName:         extenderConfig.Profile.ServerName,
		InsecureSkipVerify: true,
		// require 1.3 to mask self-signed certs
		MinVersion: tls.VersionTLS13,
	}, nil)
	if 0 < len(extenderConfig.PublicKey) {
		tlsConfig.VerifyPeerCertificate = newExtenderLeafVerifier(extenderConfig.PublicKey)
	}
	return tlsConfig
}

// The B3 outer check: the presented leaf must be signed by the extender
// identity key from the record. No chain is built and no root is consulted,
// because the extender certificate is issued for a name it does not own.
func newExtenderLeafVerifier(extenderPublicKey []byte) func([][]byte, [][]*x509.Certificate) error {
	publicKey := ed25519.PublicKey(append([]byte(nil), extenderPublicKey...))
	return func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
		if len(publicKey) != ed25519.PublicKeySize {
			return fmt.Errorf("extender public key is not an ed25519 key")
		}
		if len(rawCerts) == 0 {
			return fmt.Errorf("extender presented no certificate")
		}
		leaf, err := x509.ParseCertificate(rawCerts[0])
		if err != nil {
			return err
		}
		if leaf.SignatureAlgorithm != x509.PureEd25519 {
			return fmt.Errorf("extender leaf is signed with %s, expected ed25519", leaf.SignatureAlgorithm)
		}
		if !ed25519.Verify(publicKey, leaf.RawTBSCertificate, leaf.Signature) {
			return fmt.Errorf("extender leaf is not signed by the record key")
		}
		return nil
	}
}

// Establishes the carrier, sends the A3 request and reads the response. On
// success the returned connection is positioned at the first byte after the
// response.
func dialExtenderStream(
	ctx context.Context,
	connectSettings *ConnectSettings,
	extenderConfig *ExtenderConfig,
	extenderDial *ExtenderDial,
	extenderTlsConfig *tls.Config,
) (net.Conn, *protocol.ExtenderResponse, error) {
	headerBytes, err := extenderRequestHeaderBytes(extenderConfig, extenderDial)
	if err != nil {
		return nil, nil, err
	}

	switch extenderConfig.Profile.ConnectMode {
	case ExtenderConnectModeTcpTls:
		return dialExtenderTcp(ctx, connectSettings, extenderConfig, extenderTlsConfig, headerBytes)
	case ExtenderConnectModeQuic, ExtenderConnectModeDns:
		return dialExtenderQuic(ctx, connectSettings, extenderConfig, extenderTlsConfig, headerBytes)
	default:
		return nil, nil, fmt.Errorf("bad connect mode %s", extenderConfig.Profile.ConnectMode)
	}
}

// The serialized request header, with the hmac over timestamp and nonce when
// the extender is private.
func extenderRequestHeaderBytes(
	extenderConfig *ExtenderConfig,
	extenderDial *ExtenderDial,
) ([]byte, error) {
	header := &protocol.ExtenderHeader{
		DestinationHost: extenderDial.DestinationHost,
		DestinationPort: uint32(extenderDial.DestinationPort),
		Timestamp:       uint64(time.Now().UnixMilli()),
		Challenge:       extenderDial.Challenge,
		Service:         extenderDial.Service,
	}
	if extenderConfig.Secret != "" {
		nonce := NewId()
		header.Nonce = nonce.Bytes()

		mac := hmac.New(sha256.New, []byte(extenderConfig.Secret))
		timestampBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(timestampBytes[0:8], header.Timestamp)
		mac.Write(timestampBytes)
		mac.Write(header.Nonce)
		header.Signature = mac.Sum(nil)
	}

	headerMessageBytes, err := ProtoMarshal(header)
	if err != nil {
		return nil, err
	}
	defer MessagePoolReturn(headerMessageBytes)
	if ExtenderMaxHeaderByteCount < len(headerMessageBytes) {
		return nil, fmt.Errorf("extender header is %d bytes, at most %d", len(headerMessageBytes), ExtenderMaxHeaderByteCount)
	}
	return append([]byte(nil), headerMessageBytes...), nil
}

// The A3 request. The body is written separately on the udp carriers, where
// the request stream carries it as http3 DATA frames, so the body is optional
// here while the content length is not.
func newExtenderRequest(
	extenderConfig *ExtenderConfig,
	headerBytes []byte,
	body io.ReadCloser,
) *http.Request {
	authority := net.JoinHostPort(
		extenderConfig.Ip.String(),
		strconv.Itoa(extenderConfig.Profile.Port),
	)
	requestHost := extenderConfig.Profile.ServerName
	if requestHost == "" {
		requestHost = authority
	}
	return &http.Request{
		Method: http.MethodPost,
		URL: &url.URL{
			Scheme: "https",
			Host:   authority,
			Path:   "/",
		},
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Host:       requestHost,
		Header: http.Header{
			"Content-Type": []string{ExtenderContentType},
		},
		Body:          body,
		ContentLength: int64(len(headerBytes)),
	}
}

// The tcp carrier: terminated outer TLS, then one HTTP/1.1 request. The
// buffered reader that read the response is kept, because it may already hold
// the first bytes the extender sent after it.
func dialExtenderTcp(
	ctx context.Context,
	connectSettings *ConnectSettings,
	extenderConfig *ExtenderConfig,
	extenderTlsConfig *tls.Config,
	headerBytes []byte,
) (net.Conn, *protocol.ExtenderResponse, error) {
	authority := net.JoinHostPort(
		extenderConfig.Ip.String(),
		strconv.Itoa(extenderConfig.Profile.Port),
	)

	// Deliberately NOT routed through dialControlTlsWithFamilyFallback,
	// unlike the normal and resilient dialers. `authority` is built
	// from extenderConfig.Ip, a netip.Addr, so it is always an IP
	// LITERAL: the family is fixed by the address, there is no other
	// family to retry onto -- `dial tcp6 1.1.1.1:443` is "no suitable
	// address found" -- and there is no name resolution whose family
	// choice a strike could inform. controlDialNetwork leaves literal
	// dials unnarrowed for the same reason, which is what keeps this
	// whole fallback layer alive under a demotion.
	conn, err := connectSettings.DialContext(ctx, "tcp", authority)
	if err != nil {
		return nil, nil, err
	}
	// close the underlying conn on any failure path before we return a
	// successful serverConn to the caller
	success := false
	defer func() {
		if !success {
			conn.Close()
		}
	}()

	var serverConn net.Conn
	if extenderConfig.Profile.Fragment || extenderConfig.Profile.Reorder {
		rconn := NewResilientTlsConn(conn, extenderConfig.Profile.Fragment, extenderConfig.Profile.Reorder)
		tlsServerConn := tls.Client(rconn, extenderTlsConfig)

		func() {
			tlsCtx, tlsCancel := context.WithTimeout(ctx, connectSettings.TlsTimeout)
			defer tlsCancel()
			err = tlsServerConn.HandshakeContext(tlsCtx)
		}()
		if err != nil {
			return nil, nil, err
		}
		// once the stream is established, no longer need the resilient features
		if err := offResilientTlsConn(ctx, rconn, connectSettings.ConnectTimeout); err != nil {
			return nil, nil, err
		}

		serverConn = tlsServerConn
	} else {
		tlsServerConn := tls.Client(conn, extenderTlsConfig)

		func() {
			tlsCtx, tlsCancel := context.WithTimeout(ctx, connectSettings.TlsTimeout)
			defer tlsCancel()
			err = tlsServerConn.HandshakeContext(tlsCtx)
		}()
		if err != nil {
			return nil, nil, err
		}

		serverConn = tlsServerConn
	}

	request := newExtenderRequest(
		extenderConfig,
		headerBytes,
		io.NopCloser(bytes.NewReader(headerBytes)),
	)
	if err := withConnWritePhaseDeadline(ctx, serverConn, connectSettings.ConnectTimeout, func() error {
		return request.Write(serverConn)
	}); err != nil {
		return nil, nil, err
	}

	// the reader may buffer past the response; it becomes the read side of
	// the returned connection
	reader := bufio.NewReader(serverConn)
	var response *protocol.ExtenderResponse
	if err := withConnReadPhaseDeadline(ctx, serverConn, connectSettings.ConnectTimeout, func() error {
		httpResponse, err := http.ReadResponse(reader, request)
		if err != nil {
			return err
		}
		defer httpResponse.Body.Close()
		if httpResponse.StatusCode != http.StatusOK {
			return fmt.Errorf("extender refused the request with status %d", httpResponse.StatusCode)
		}
		response, err = ReadExtenderResponseFrame(httpResponse.Body)
		return err
	}); err != nil {
		return nil, nil, err
	}

	success = true
	return newBufferedConn(serverConn, reader), response, nil
}

// The udp carriers: one QUIC connection with ALPN h3 straight to the extender
// ip, the dns carrier over the packet translation first, then one H3 request
// stream that becomes the byte stream.
func dialExtenderQuic(
	ctx context.Context,
	connectSettings *ConnectSettings,
	extenderConfig *ExtenderConfig,
	extenderTlsConfig *tls.Config,
	headerBytes []byte,
) (net.Conn, *protocol.ExtenderResponse, error) {
	if !extenderConfig.Ip.IsValid() {
		return nil, nil, fmt.Errorf("extender address is not valid")
	}
	udpAddr := net.UDPAddrFromAddrPort(netip.AddrPortFrom(
		extenderConfig.Ip,
		uint16(extenderConfig.Profile.Port),
	))

	closers := []func(){}
	success := false
	defer func() {
		if !success {
			for i := len(closers) - 1; 0 <= i; i -= 1 {
				closers[i]()
			}
		}
	}()

	packetConn, err := openExtenderPacketConn(ctx, connectSettings, udpAddr)
	if err != nil {
		// ownership transfers for every non-nil result, including a
		// rejected one
		if packetConn != nil {
			packetConn.Close()
		}
		return nil, nil, err
	}
	if packetConn == nil {
		return nil, nil, fmt.Errorf("extender packet connection factory returned nil")
	}
	closers = append(closers, func() { packetConn.Close() })

	if extenderConfig.Profile.ConnectMode == ExtenderConnectModeDns {
		tld := extenderConfig.Profile.DnsTld
		if tld == "" {
			tld = DefaultExtenderDnsTld
		}
		ptSettings := DefaultPacketTranslationSettings()
		ptSettings.Log = connectSettings.Log
		ptSettings.DnsTlds = [][]byte{[]byte(tld)}
		// The connection cleanup owns the translated PacketConn. Keep its
		// encoder alive while cancellation closes QUIC gracefully, exactly as
		// the platform h3 dns carrier does.
		translation, err := NewPacketTranslation(
			context.WithoutCancel(ctx),
			PacketTranslationModeDns,
			packetConn,
			ptSettings,
		)
		if err != nil {
			return nil, nil, err
		}
		packetConn = translation
		closers = append(closers, func() { translation.Close() })
	}

	quicTransport := &quic.Transport{
		Conn: packetConn,
	}
	closers = append(closers, func() { quicTransport.Close() })

	// with an empty spoof list the outer name is empty (A10). quic-go fills an
	// empty ServerName with the ip literal it is dialing, and crypto/tls omits
	// an ip literal from the sni extension, so the ClientHello still carries no
	// name -- the same shape the tcp carrier sends
	quicTlsConfig := extenderTlsConfig.Clone()
	quicTlsConfig.NextProtos = []string{http3.NextProtoH3}
	quicConfig := &quic.Config{
		HandshakeIdleTimeout: connectSettings.ConnectTimeout + connectSettings.TlsTimeout + connectSettings.HandshakeTimeout,
	}
	quicConn, err := quicTransport.Dial(ctx, udpAddr, quicTlsConfig, quicConfig)
	if err != nil {
		return nil, nil, err
	}
	closers = append(closers, func() { quicConn.CloseWithError(0, "") })

	h3Transport := &http3.Transport{}
	clientConn := h3Transport.NewClientConn(quicConn)
	stream, err := clientConn.OpenRequestStream(ctx)
	if err != nil {
		return nil, nil, err
	}

	// http3 rejects a request stream header that carries a body, so the body
	// is written as DATA frames after it; the content length still describes
	// it
	request := newExtenderRequest(extenderConfig, headerBytes, http.NoBody)
	deadline := time.Now().Add(connectSettings.ConnectTimeout)
	if requestDeadline, ok := ctx.Deadline(); ok && requestDeadline.Before(deadline) {
		deadline = requestDeadline
	}
	if err := stream.SetDeadline(deadline); err != nil {
		return nil, nil, err
	}
	if err := stream.SendRequestHeader(request); err != nil {
		return nil, nil, err
	}
	if _, err := stream.Write(headerBytes); err != nil {
		return nil, nil, err
	}
	httpResponse, err := stream.ReadResponse()
	if err != nil {
		return nil, nil, err
	}
	if httpResponse.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("extender refused the request with status %d", httpResponse.StatusCode)
	}
	response, err := ReadExtenderResponseFrame(stream)
	if err != nil {
		return nil, nil, err
	}
	if err := stream.SetDeadline(time.Time{}); err != nil {
		return nil, nil, err
	}

	success = true
	return newStreamConn(
		stream,
		packetConn.LocalAddr(),
		udpAddr,
		closers,
	), response, nil
}

// One unconnected udp endpoint for a carrier dial, narrowed to the family of
// the extender address. A configured packet endpoint factory wins, so a
// headless host keeps one source identity.
func openExtenderPacketConn(
	ctx context.Context,
	connectSettings *ConnectSettings,
	udpAddr *net.UDPAddr,
) (net.PacketConn, error) {
	if connectSettings.DialContextSettings != nil && connectSettings.DialContextSettings.PacketConnFactory != nil {
		return connectSettings.DialContextSettings.PacketConnFactory(ctx)
	}
	udpNetwork, wildcard := udpWildcardForFamily(udpAddrFamily(udpAddr))
	return net.ListenUDP(udpNetwork, wildcard)
}

// The A3 response body: a 4-byte big-endian length and the serialized
// ExtenderResponse. The length prefix makes the body self-delimiting, which
// the udp carriers need: there the response body and the raw bytes that follow
// it are the same http3 DATA stream, so a content length would bound the
// reader the caller keeps using.
func ExtenderResponseFrame(response *protocol.ExtenderResponse) ([]byte, error) {
	responseBytes, err := proto.Marshal(response)
	if err != nil {
		return nil, err
	}
	if ExtenderMaxHeaderByteCount < len(responseBytes) {
		return nil, fmt.Errorf("extender response is %d bytes, at most %d", len(responseBytes), ExtenderMaxHeaderByteCount)
	}
	frameBytes := make([]byte, 4+len(responseBytes))
	binary.BigEndian.PutUint32(frameBytes[0:4], uint32(len(responseBytes)))
	copy(frameBytes[4:], responseBytes)
	return frameBytes, nil
}

// Reads exactly one response frame, leaving the reader on the first byte that
// follows it.
func ReadExtenderResponseFrame(reader io.Reader) (*protocol.ExtenderResponse, error) {
	lengthBytes := make([]byte, 4)
	if _, err := io.ReadFull(reader, lengthBytes); err != nil {
		return nil, err
	}
	responseByteCount := int(binary.BigEndian.Uint32(lengthBytes))
	if ExtenderMaxHeaderByteCount < responseByteCount {
		return nil, fmt.Errorf("extender response is %d bytes, at most %d", responseByteCount, ExtenderMaxHeaderByteCount)
	}
	responseBytes := make([]byte, responseByteCount)
	if _, err := io.ReadFull(reader, responseBytes); err != nil {
		return nil, err
	}
	response := &protocol.ExtenderResponse{}
	if err := proto.Unmarshal(responseBytes, response); err != nil {
		return nil, err
	}
	return response, nil
}

// Bounds a synchronous connection read phase by the earlier of the caller
// deadline and its phase budget, mirroring the write phase helper.
func withConnReadPhaseDeadline(
	ctx context.Context,
	conn net.Conn,
	phaseTimeout time.Duration,
	read func() error,
) (resultErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, hasDeadline := ctx.Deadline()
	if 0 < phaseTimeout {
		phaseDeadline := time.Now().Add(phaseTimeout)
		if !hasDeadline || phaseDeadline.Before(deadline) {
			deadline = phaseDeadline
			hasDeadline = true
		}
	}
	if !hasDeadline {
		return read()
	}
	if err := conn.SetReadDeadline(deadline); err != nil {
		return err
	}
	defer func() {
		clearErr := conn.SetReadDeadline(time.Time{})
		if resultErr == nil {
			resultErr = clearErr
		}
	}()
	return read()
}

// bufferedConn drains what a buffered reader already took from the connection
// before reading the socket again. The http response parser reads ahead, so
// the first inner bytes can already be in that buffer.
type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func newBufferedConn(conn net.Conn, reader *bufio.Reader) *bufferedConn {
	return &bufferedConn{
		Conn:   conn,
		reader: reader,
	}
}

func (self *bufferedConn) Read(b []byte) (int, error) {
	return self.reader.Read(b)
}

// streamConn adapts one http3 request stream to a connection. It owns the
// QUIC connection, transport and socket underneath, which are released in
// reverse order on Close, so a caller that holds only the stream still frees
// the whole carrier.
type streamConn struct {
	stream     *http3.RequestStream
	localAddr  net.Addr
	remoteAddr net.Addr
	closers    []func()
	closeOnce  sync.Once
}

func newStreamConn(
	stream *http3.RequestStream,
	localAddr net.Addr,
	remoteAddr net.Addr,
	closers []func(),
) *streamConn {
	return &streamConn{
		stream:     stream,
		localAddr:  localAddr,
		remoteAddr: remoteAddr,
		closers:    closers,
	}
}

func (self *streamConn) Read(b []byte) (int, error) {
	return self.stream.Read(b)
}

func (self *streamConn) Write(b []byte) (int, error) {
	return self.stream.Write(b)
}

func (self *streamConn) Close() error {
	self.closeOnce.Do(func() {
		self.stream.Close()
		self.stream.CancelRead(quic.StreamErrorCode(http3.ErrCodeNoError))
		for i := len(self.closers) - 1; 0 <= i; i -= 1 {
			self.closers[i]()
		}
	})
	return nil
}

func (self *streamConn) LocalAddr() net.Addr {
	return self.localAddr
}

func (self *streamConn) RemoteAddr() net.Addr {
	return self.remoteAddr
}

func (self *streamConn) SetDeadline(t time.Time) error {
	return self.stream.SetDeadline(t)
}

func (self *streamConn) SetReadDeadline(t time.Time) error {
	return self.stream.SetReadDeadline(t)
}

func (self *streamConn) SetWriteDeadline(t time.Time) error {
	return self.stream.SetWriteDeadline(t)
}
