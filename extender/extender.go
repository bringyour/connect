package extender

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/idna"

	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect"
	"github.com/urnetwork/connect/protocol"
)

// The extender server (EXTENDER.md A1 to A4, A7 to A9, B3).
//
// Three carriers yield one reliable byte stream each and share one request
// contract. tcp 443 terminates tls and serves http/1.1 or h2 on it; udp 443
// runs quic with alpn h3; udp 53 runs the same quic server over the decode53
// packet translation. Inside every carrier the client sends one
// `POST /` with the serialized ExtenderHeader, and on acceptance the stream is
// taken over and carries the inner bytes: the client's own tls to the
// destination, which the extender never sees inside.
//
// The extender is deliberately indistinguishable from a misconfigured cdn on
// the wire: it terminates tls for every server name with a certificate it
// generates (B3), and the extender protocol lives inside that tls.
//
// One legacy shape is still accepted on tcp: a v1 client sends a four byte
// big-endian length and a header, with no http and no response. No http method
// and no tls record starts with a length of 1024 or less, so the first four
// bytes tell the two apart. v1 acceptance is dropped one release later.
//
// What this phase does not do: the reverse proxy for non-extender requests
// (A5) and the dns forwarder for non-translation queries (A6). Both are
// refused here and land in phase 1b.
//
// The server is safe for concurrent use. Close interrupts every listener and
// connection it owns; CloseAndWait also joins their goroutines.

// https://go.dev/src/crypto/tls/generate_cert.go

func DefaultExtenderSettings() *ExtenderSettings {
	return &ExtenderSettings{
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		ValidFrom:    180 * 24 * time.Hour,
		ValidFor:     180 * 24 * time.Hour,

		HeaderTimeout:               10 * time.Second,
		QuicIdleTimeout:             30 * time.Second,
		MaxConnectionCountPerSource: 64,
		MaxConnectionCount:          4096,

		DnsTlds: []string{connect.DefaultExtenderDnsTld},
	}
}

type ExtenderSettings struct {
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	ValidFrom    time.Duration
	ValidFor     time.Duration

	// Budget for the outer handshake and the extender request of one
	// connection (A9). A client that opens a connection and says nothing is
	// dropped here.
	HeaderTimeout time.Duration
	// Idle quic connections close after this (A9).
	QuicIdleTimeout time.Duration
	// Concurrent connections from one source address (A9). <= 0 disables.
	MaxConnectionCountPerSource int
	// Concurrent connections over every carrier (A9). <= 0 disables.
	MaxConnectionCount int

	// Encoding tlds of the dns carrier. A query that does not use one of them
	// is not part of the translation.
	DnsTlds []string

	// IdentityKeySeed, when set, is the ed25519 seed of the extender identity
	// (B1). It signs the certificate authority the per-name leaves are issued
	// under, and the challenge in an extender response. Empty leaves the
	// extender without an identity, which a client cannot verify.
	IdentityKeySeed []byte

	// GossipConnHandler receives the taken-over stream of a gossip service
	// request (A8). It owns the stream until it returns, and the extender
	// closes the stream afterward, so a listener implementation should hand
	// the stream on and wait for its consumer. Nil refuses the service.
	GossipConnHandler func(conn net.Conn)
	// FeedConnHandler is the same for the feed service (A8).
	FeedConnHandler func(conn net.Conn)

	// Listen, when set, binds the outer TLS listener. Userspace integration
	// tests use it to place the production extender on a simulated TUN. Nil
	// retains net.Listen. The extender owns and closes returned listeners.
	Listen func(network string, address string) (net.Listener, error)
	// ListenPacket, when set, binds the udp carriers. Tests use it to inject
	// a socket without binding a privileged port. Nil retains
	// net.ListenPacket. The extender owns and closes returned endpoints.
	ListenPacket func(network string, address string) (net.PacketConn, error)
	// DialContext, when set, creates the forwarded inner connection. Userspace
	// integration tests use it for the extender-to-connect segment. Nil
	// retains forwardDialer. The extender owns and closes returned connections.
	DialContext connect.DialContextFunction
	// ErrorHandler, when set, receives connection-stage failures. Measurement
	// tests use it to make an otherwise client-visible timeout attributable.
	// It runs synchronously and must not block. Nil retains the silent
	// production behavior.
	ErrorHandler func(stage string, err error)
}

type ExtenderServer struct {
	ctx    context.Context
	cancel context.CancelFunc

	stateLock   sync.Mutex
	closing     bool
	listeners   map[*extenderOwnedListener]bool
	connections map[*extenderOwnedConnection]bool
	closers     map[*extenderOwnedCloser]bool
	workers     sync.WaitGroup

	connectionCount        int
	sourceConnectionCounts map[string]int

	allowedSecrets []string
	// exact (x) or wildcard (*.x)
	// wildcard *.x does not match exact x
	allowedHosts  []string
	ports         map[int][]connect.ExtenderConnectMode
	carriers      []string
	forwardDialer *net.Dialer

	certificates    *extenderCertificates
	certificatesErr error

	httpServer  *http.Server
	http2Server *http2.Server
	h3Server    *http3.Server

	settings *ExtenderSettings
}

// An extenderOwnedListener identifies one listener in the shutdown set.
type extenderOwnedListener struct {
	listener net.Listener
}

// An extenderOwnedConnection identifies one connection in the shutdown set.
type extenderOwnedConnection struct {
	connection net.Conn
}

// An extenderOwnedCloser identifies one endpoint, quic transport, quic
// listener or quic connection in the shutdown set. These have no common
// interface, so the release is carried as a closure.
type extenderOwnedCloser struct {
	close func()
}

func NewExtenderServerWithDefaults(
	ctx context.Context,
	allowedSecrets []string,
	allowedHosts []string,
	ports map[int][]connect.ExtenderConnectMode,
	forwardDialer *net.Dialer,
) *ExtenderServer {
	return NewExtenderServer(
		ctx,
		allowedSecrets,
		allowedHosts,
		ports,
		forwardDialer,
		DefaultExtenderSettings(),
	)
}

func NewExtenderServer(
	ctx context.Context,
	allowedSecrets []string,
	allowedHosts []string,
	ports map[int][]connect.ExtenderConnectMode,
	forwardDialer *net.Dialer,
	settings *ExtenderSettings,
) *ExtenderServer {
	cancelCtx, cancel := context.WithCancel(ctx)

	self := &ExtenderServer{
		ctx:                    cancelCtx,
		cancel:                 cancel,
		listeners:              map[*extenderOwnedListener]bool{},
		connections:            map[*extenderOwnedConnection]bool{},
		closers:                map[*extenderOwnedCloser]bool{},
		sourceConnectionCounts: map[string]int{},
		allowedSecrets:         allowedSecrets,
		allowedHosts:           allowedHosts,
		ports:                  ports,
		carriers:               carriersForPorts(ports),
		forwardDialer:          forwardDialer,
		settings:               settings,
	}

	// A certificate failure is reported when serving starts, so the
	// constructor keeps its shape for callers that cannot handle an error.
	self.certificates, self.certificatesErr = newExtenderCertificates(settings.IdentityKeySeed, settings)

	handler := &extenderHandler{server: self}
	// an h2 connection only ever gets refusals in this phase, so it is
	// reclaimed on the same budget a connection has to make its request
	self.http2Server = &http2.Server{
		IdleTimeout: settings.HeaderTimeout,
	}
	self.httpServer = &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: settings.HeaderTimeout,
		ErrorLog:          log.New(extenderLogWriter{}, "", 0),
	}
	// h2 support is configured through the standard path even though the
	// extender dispatches the protocol itself: it has already read the first
	// bytes of the stream to tell v1 from http, so the connection it serves is
	// no longer the *tls.Conn the alpn hook requires.
	if err := http2.ConfigureServer(self.httpServer, self.http2Server); err != nil && self.certificatesErr == nil {
		self.certificatesErr = err
	}
	self.h3Server = &http3.Server{
		Handler:     handler,
		IdleTimeout: settings.QuicIdleTimeout,
	}

	return self
}

// The carrier names this extender serves, in wire order (A4).
func carriersForPorts(ports map[int][]connect.ExtenderConnectMode) []string {
	carriers := []string{}
	for _, carrier := range []string{
		connect.ExtenderCarrierTcp,
		connect.ExtenderCarrierQuic,
		connect.ExtenderCarrierDns,
	} {
		for _, connectModes := range ports {
			if slices.ContainsFunc(connectModes, func(connectMode connect.ExtenderConnectMode) bool {
				return connect.ExtenderCarrierForConnectMode(connectMode) == carrier
			}) {
				carriers = append(carriers, carrier)
				break
			}
		}
	}
	return carriers
}

// extenderLogWriter keeps the http server's internal errors on the same log as
// the rest of the extender without exposing a writer to callers.
type extenderLogWriter struct{}

func (self extenderLogWriter) Write(b []byte) (int, error) {
	log.Printf("[extender] http: %s", strings.TrimSpace(string(b)))
	return len(b), nil
}

// Begins one owned server operation unless shutdown has already started.
func (self *ExtenderServer) beginWorker() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.closing {
		return false
	}
	self.workers.Add(1)
	return true
}

// Completes one owned server operation.
func (self *ExtenderServer) endWorker() {
	self.workers.Done()
}

// Adds a listener to the resources interrupted by Close.
func (self *ExtenderServer) addListener(listener net.Listener) (*extenderOwnedListener, bool) {
	ownedListener := &extenderOwnedListener{listener: listener}
	self.stateLock.Lock()
	if self.closing {
		self.stateLock.Unlock()
		listener.Close()
		return nil, false
	}
	self.listeners[ownedListener] = true
	self.stateLock.Unlock()
	return ownedListener, true
}

// Removes a listener after its serving operation has released it.
func (self *ExtenderServer) removeListener(ownedListener *extenderOwnedListener) {
	self.stateLock.Lock()
	delete(self.listeners, ownedListener)
	self.stateLock.Unlock()
}

// Adds an endpoint, transport, quic listener or quic connection to the
// resources interrupted by Close. A closer added during shutdown is released
// immediately.
func (self *ExtenderServer) addCloser(closeCloser func()) (*extenderOwnedCloser, bool) {
	ownedCloser := &extenderOwnedCloser{close: closeCloser}
	self.stateLock.Lock()
	if self.closing {
		self.stateLock.Unlock()
		closeCloser()
		return nil, false
	}
	self.closers[ownedCloser] = true
	self.stateLock.Unlock()
	return ownedCloser, true
}

// Removes a closer after its owner has released it.
func (self *ExtenderServer) removeCloser(ownedCloser *extenderOwnedCloser) {
	if ownedCloser == nil {
		return
	}
	self.stateLock.Lock()
	delete(self.closers, ownedCloser)
	self.stateLock.Unlock()
}

// Reserves one connection slot for a source address (A9). A refused
// connection is closed by the caller without a response, so a flood costs the
// extender only an accept.
func (self *ExtenderServer) beginConnection(remoteAddr net.Addr) bool {
	source := connectionSource(remoteAddr)
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.closing {
		return false
	}
	if 0 < self.settings.MaxConnectionCount && self.settings.MaxConnectionCount <= self.connectionCount {
		return false
	}
	if 0 < self.settings.MaxConnectionCountPerSource &&
		self.settings.MaxConnectionCountPerSource <= self.sourceConnectionCounts[source] {
		return false
	}
	self.connectionCount += 1
	self.sourceConnectionCounts[source] += 1
	return true
}

// Releases one connection slot.
func (self *ExtenderServer) endConnection(remoteAddr net.Addr) {
	source := connectionSource(remoteAddr)
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.connectionCount -= 1
	if count := self.sourceConnectionCounts[source] - 1; 0 < count {
		self.sourceConnectionCounts[source] = count
	} else {
		delete(self.sourceConnectionCounts, source)
	}
}

// The address of a peer, or the empty string when there is none.
func remoteAddressString(remoteAddr net.Addr) string {
	if remoteAddr == nil {
		return ""
	}
	return remoteAddr.String()
}

// The limit key of a connection: the address without the port, so a source
// cannot multiply its budget by using more ports.
func connectionSource(remoteAddr net.Addr) string {
	address := remoteAddressString(remoteAddr)
	if host, _, err := net.SplitHostPort(address); err == nil {
		return host
	}
	return address
}

// Runs a connection handler whose socket is interrupted and joined at Close.
func (self *ExtenderServer) startConnection(connection net.Conn) {
	if !self.beginConnection(connection.RemoteAddr()) {
		connection.Close()
		return
	}
	ownedConnection := &extenderOwnedConnection{connection: connection}
	self.stateLock.Lock()
	if self.closing {
		self.stateLock.Unlock()
		self.endConnection(connection.RemoteAddr())
		connection.Close()
		return
	}
	self.connections[ownedConnection] = true
	self.workers.Add(1)
	self.stateLock.Unlock()

	go func() {
		defer func() {
			self.stateLock.Lock()
			delete(self.connections, ownedConnection)
			self.stateLock.Unlock()
			self.endConnection(connection.RemoteAddr())
			self.workers.Done()
		}()
		self.HandleExtenderConnection(self.ctx, connection)
	}()
}

// ListenAndServe owns every accepted listener and connection until shutdown.
func (self *ExtenderServer) ListenAndServe() error {
	if !self.beginWorker() {
		return self.ctx.Err()
	}
	defer self.endWorker()
	defer self.Close()

	if self.certificatesErr != nil {
		return self.certificatesErr
	}

	listeners := map[int]*extenderOwnedListener{}
	defer func() {
		for _, ownedListener := range listeners {
			ownedListener.listener.Close()
			self.removeListener(ownedListener)
		}
	}()

	for port, connectModes := range self.ports {
		if !slices.Contains(connectModes, connect.ExtenderConnectModeTcpTls) {
			continue
		}

		log.Printf("[extender] listen tcp %d", port)
		listen := net.Listen
		if self.settings.Listen != nil {
			listen = self.settings.Listen
		}
		listener, err := listen("tcp", fmt.Sprintf(":%d", port))
		if err != nil {
			// Ownership transfers for every non-nil callback result, even when
			// the callback rejects that result with an error.
			if listener != nil {
				listener.Close()
			}
			log.Printf("[extender] listen error: %s", err)
			return err
		}
		if listener == nil {
			return fmt.Errorf("extender listener factory returned nil")
		}
		ownedListener, ok := self.addListener(listener)
		if !ok {
			return self.ctx.Err()
		}
		listeners[port] = ownedListener
		if !self.beginWorker() {
			return self.ctx.Err()
		}
		go func() {
			defer self.endWorker()
			connect.HandleError(func() {
				defer self.Close()

				for {
					select {
					case <-self.ctx.Done():
						return
					default:
					}

					conn, err := listener.Accept()
					if err != nil {
						log.Printf("[extender] accept error: %s", err)
						return
					}
					self.startConnection(conn)
				}
			}, self.cancel)
		}()
	}

	packetConns := []*extenderOwnedCloser{}
	defer func() {
		for _, ownedCloser := range packetConns {
			ownedCloser.close()
			self.removeCloser(ownedCloser)
		}
	}()

	for port, connectModes := range self.ports {
		udpConnectModes := []connect.ExtenderConnectMode{}
		for _, connectMode := range connectModes {
			switch connectMode {
			case connect.ExtenderConnectModeQuic, connect.ExtenderConnectModeDns:
				udpConnectModes = append(udpConnectModes, connectMode)
			}
		}
		if len(udpConnectModes) == 0 {
			continue
		}
		if 1 < len(udpConnectModes) {
			return fmt.Errorf("port %d lists more than one udp carrier: %v", port, udpConnectModes)
		}
		connectMode := udpConnectModes[0]

		log.Printf("[extender] listen udp %d (%s)", port, connectMode)
		listenPacket := net.ListenPacket
		if self.settings.ListenPacket != nil {
			listenPacket = self.settings.ListenPacket
		}
		packetConn, err := listenPacket("udp", fmt.Sprintf(":%d", port))
		if err != nil {
			if packetConn != nil {
				packetConn.Close()
			}
			log.Printf("[extender] listen packet error: %s", err)
			return err
		}
		if packetConn == nil {
			return fmt.Errorf("extender packet listener factory returned nil")
		}
		ownedCloser, ok := self.addCloser(func() { packetConn.Close() })
		if !ok {
			return self.ctx.Err()
		}
		packetConns = append(packetConns, ownedCloser)

		if !self.beginWorker() {
			return self.ctx.Err()
		}
		go func() {
			defer self.endWorker()
			connect.HandleError(func() {
				defer self.Close()
				if err := self.serveQuicCarrier(connectMode, packetConn); err != nil {
					log.Printf("[extender] udp carrier %s exited: %s", connectMode, err)
				}
			}, self.cancel)
		}()
	}

	select {
	case <-self.ctx.Done():
	}

	return nil
}

// Serves one udp carrier. The dns carrier is the same quic server over the
// decode53 packet translation, so both carriers share this loop (A1).
func (self *ExtenderServer) serveQuicCarrier(
	connectMode connect.ExtenderConnectMode,
	packetConn net.PacketConn,
) error {
	carrierPacketConn := packetConn
	if connectMode == connect.ExtenderConnectModeDns {
		ptSettings := connect.DefaultPacketTranslationSettings()
		dnsTlds := [][]byte{}
		for _, dnsTld := range self.settings.DnsTlds {
			dnsTlds = append(dnsTlds, []byte(dnsTld))
		}
		if len(dnsTlds) == 0 {
			dnsTlds = append(dnsTlds, []byte(connect.DefaultExtenderDnsTld))
		}
		ptSettings.DnsTlds = dnsTlds
		translation, err := connect.NewPacketTranslation(
			self.ctx,
			connect.PacketTranslationModeDecode53,
			packetConn,
			ptSettings,
		)
		if err != nil {
			return err
		}
		ownedCloser, ok := self.addCloser(func() { translation.Close() })
		if !ok {
			translation.Close()
			return self.ctx.Err()
		}
		defer func() {
			translation.Close()
			self.removeCloser(ownedCloser)
		}()
		carrierPacketConn = translation
	}

	quicTransport := &quic.Transport{
		Conn: carrierPacketConn,
	}
	transportCloser, ok := self.addCloser(func() { quicTransport.Close() })
	if !ok {
		quicTransport.Close()
		return self.ctx.Err()
	}
	defer func() {
		quicTransport.Close()
		self.removeCloser(transportCloser)
	}()

	tlsConfig := &tls.Config{
		GetCertificate: self.certificates.GetCertificate,
		NextProtos:     []string{http3.NextProtoH3},
	}
	quicConfig := &quic.Config{
		MaxIdleTimeout: self.settings.QuicIdleTimeout,
	}
	listener, err := quicTransport.Listen(tlsConfig, quicConfig)
	if err != nil {
		return err
	}
	listenerCloser, ok := self.addCloser(func() { listener.Close() })
	if !ok {
		listener.Close()
		return self.ctx.Err()
	}
	defer func() {
		listener.Close()
		self.removeCloser(listenerCloser)
	}()

	for {
		quicConn, err := listener.Accept(self.ctx)
		if err != nil {
			return err
		}
		if !self.beginConnection(quicConn.RemoteAddr()) {
			quicConn.CloseWithError(0, "")
			continue
		}
		connCloser, ok := self.addCloser(func() { quicConn.CloseWithError(0, "") })
		if !ok {
			quicConn.CloseWithError(0, "")
			self.endConnection(quicConn.RemoteAddr())
			return self.ctx.Err()
		}
		if !self.beginWorker() {
			quicConn.CloseWithError(0, "")
			self.removeCloser(connCloser)
			self.endConnection(quicConn.RemoteAddr())
			return self.ctx.Err()
		}
		go func() {
			defer func() {
				quicConn.CloseWithError(0, "")
				self.removeCloser(connCloser)
				self.endConnection(quicConn.RemoteAddr())
				self.endWorker()
			}()
			connect.HandleError(func() {
				if err := self.h3Server.ServeQUICConn(quicConn); err != nil {
					self.reportError("h3 serve", err)
				}
			})
		}()
	}
}

func (self *ExtenderServer) Close() {
	self.stateLock.Lock()
	if self.closing {
		self.stateLock.Unlock()
		return
	}
	self.closing = true
	self.cancel()
	listeners := make([]net.Listener, 0, len(self.listeners))
	for ownedListener := range self.listeners {
		listeners = append(listeners, ownedListener.listener)
	}
	connections := make([]net.Conn, 0, len(self.connections))
	for ownedConnection := range self.connections {
		connections = append(connections, ownedConnection.connection)
	}
	closers := make([]func(), 0, len(self.closers))
	for ownedCloser := range self.closers {
		closers = append(closers, ownedCloser.close)
	}
	self.stateLock.Unlock()

	for _, listener := range listeners {
		listener.Close()
	}
	for _, connection := range connections {
		connection.Close()
	}
	for _, closeCloser := range closers {
		closeCloser()
	}
	self.httpServer.Close()
}

// CloseAndWait interrupts and joins every listener and connection worker.
func (self *ExtenderServer) CloseAndWait() {
	self.Close()
	self.workers.Wait()
}

func (self *ExtenderServer) IsAllowedSecret(header *protocol.ExtenderHeader) bool {
	for _, secret := range self.allowedSecrets {
		mac := hmac.New(sha256.New, []byte(secret))
		timestampBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(timestampBytes[0:8], header.Timestamp)
		mac.Write(timestampBytes)
		mac.Write(header.Nonce)
		signature := mac.Sum(nil)
		if slices.Equal(signature, header.Signature) {
			return true
		}
	}
	return false
}

func (self *ExtenderServer) IsAllowedHost(host string) bool {
	_, err := idna.ToUnicode(host)
	if err != nil {
		// not a valid host
		return false
	}
	for _, allowedHost := range self.allowedHosts {
		if host == allowedHost {
			return true
		}
		if strings.HasPrefix(allowedHost, "*.") {
			if strings.HasSuffix(host, allowedHost[1:]) {
				return true
			}
		}
	}
	return false
}

// The identity key this extender publishes in a response, or nil when it has
// none (B3).
func (self *ExtenderServer) PublicKey() []byte {
	if self.certificates == nil {
		return nil
	}
	return self.certificates.PublicKey()
}

// Signs a probe challenge with the identity key, or returns nil when the
// extender has none (A4).
func (self *ExtenderServer) SignChallenge(challenge []byte) []byte {
	if self.certificates == nil {
		return nil
	}
	return self.certificates.SignChallenge(challenge)
}

// The carrier names this extender serves (A4).
func (self *ExtenderServer) Carriers() []string {
	return slices.Clone(self.carriers)
}

// Connection errors are observable only when a caller installs the test seam.
func (self *ExtenderServer) reportError(stage string, err error) {
	if self.settings.ErrorHandler != nil {
		self.settings.ErrorHandler(stage, err)
	}
}

// HandleExtenderConnection terminates the outer tls of one tcp connection and
// serves whatever is inside: the v1 framing, or an http request that carries
// the extender header. The connection is closed when this returns.
func (self *ExtenderServer) HandleExtenderConnection(ctx context.Context, conn net.Conn) {
	handleCtx, handleCancel := context.WithCancel(ctx)
	defer handleCancel()

	defer conn.Close()

	if self.certificatesErr != nil {
		self.reportError("certificates", self.certificatesErr)
		return
	}

	tlsConfig := &tls.Config{
		GetCertificate: self.certificates.GetCertificate,
		// a prober that asks for h2 gets it (A3); the extender's own client
		// offers no alpn, so it negotiates http/1.1
		NextProtos: []string{"h2", "http/1.1"},
	}
	clientConn := tls.Server(conn, tlsConfig)
	defer clientConn.Close()

	// one budget covers the handshake and the request that follows (A9)
	clientConn.SetDeadline(time.Now().Add(self.settings.HeaderTimeout))
	err := clientConn.HandshakeContext(handleCtx)
	if err != nil {
		self.reportError("outer TLS handshake", err)
		return
	}

	initialBytes := make([]byte, 4)
	for i := 0; i < len(initialBytes); {
		n, err := clientConn.Read(initialBytes[i:])
		i += n
		if err != nil {
			self.reportError("header length", err)
			return
		}
	}

	headerByteCount := int(binary.BigEndian.Uint32(initialBytes))
	if headerByteCount <= connect.ExtenderMaxHeaderByteCount {
		// v1: a length-prefixed header and no response frame. No http method
		// and no tls record begins with such a length.
		self.handleV1Connection(handleCtx, handleCancel, clientConn, headerByteCount)
		return
	}

	if err := clientConn.SetDeadline(time.Time{}); err != nil {
		self.reportError("header length", err)
		return
	}
	requestConn := newConnWithInitialBytes(clientConn, initialBytes)
	self.serveHttpConnection(handleCtx, requestConn, clientConn.ConnectionState().NegotiatedProtocol)
}

// Serves one terminated connection with the http server. h2 is dispatched
// directly because the connection is no longer the *tls.Conn the alpn hook of
// net/http requires: the carrier has already read its first bytes.
func (self *ExtenderServer) serveHttpConnection(
	ctx context.Context,
	requestConn *connWithInitialBytes,
	negotiatedProtocol string,
) {
	if negotiatedProtocol == http2.NextProtoTLS {
		self.http2Server.ServeConn(requestConn, &http2.ServeConnOpts{
			Context:    ctx,
			BaseConfig: self.httpServer,
			Handler:    self.httpServer.Handler,
		})
		return
	}

	listener := newSingleConnListener(requestConn, requestConn.LocalAddr())
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		self.httpServer.Serve(listener)
	}()
	select {
	case <-requestConn.Closed():
	case <-ctx.Done():
	}
	listener.Close()
	<-serveDone
}

// The v1 path: the header length has already been read. There is no response
// frame and no service, exactly as the first release.
func (self *ExtenderServer) handleV1Connection(
	ctx context.Context,
	cancel context.CancelFunc,
	clientConn net.Conn,
	headerByteCount int,
) {
	headerBytes := make([]byte, headerByteCount)
	for i := 0; i < headerByteCount; {
		clientConn.SetReadDeadline(time.Now().Add(self.settings.ReadTimeout))
		n, err := clientConn.Read(headerBytes[i:headerByteCount])
		i += n
		if err != nil {
			self.reportError("header body", err)
			return
		}
	}

	header := &protocol.ExtenderHeader{}
	if err := proto.Unmarshal(headerBytes, header); err != nil {
		self.reportError("header decode", err)
		return
	}

	if !self.IsAllowedSecret(header) {
		self.reportError("header authorization", fmt.Errorf("secret signature is not allowed"))
		return
	}
	if !self.IsAllowedHost(header.DestinationHost) {
		self.reportError("destination authorization", fmt.Errorf("host %q is not allowed", header.DestinationHost))
		return
	}

	forwardConn, err := self.dialForward(ctx, remoteAddressString(clientConn.RemoteAddr()), header)
	if err != nil {
		return
	}
	defer forwardConn.Close()

	if err := clientConn.SetDeadline(time.Time{}); err != nil {
		self.reportError("relay", err)
		return
	}
	self.relay(ctx, cancel, clientConn, forwardConn)
}

// Dials the destination on the family of the client's outer socket (A7), so
// name resolution yields only that family and a destination without an
// address of it fails here rather than crossing families.
func (self *ExtenderServer) dialForward(
	ctx context.Context,
	clientAddress string,
	header *protocol.ExtenderHeader,
) (net.Conn, error) {
	dialContext := self.forwardDialer.DialContext
	if self.settings.DialContext != nil {
		dialContext = self.settings.DialContext
	}
	forwardConn, err := dialContext(ctx, forwardNetwork(clientAddress), net.JoinHostPort(
		header.DestinationHost,
		fmt.Sprintf("%d", header.DestinationPort),
	))
	if err != nil {
		// Ownership transfers for every non-nil callback result, even when
		// the callback also returns an error.
		if forwardConn != nil {
			forwardConn.Close()
		}
		self.reportError("forward dial", err)
		return nil, err
	}
	if forwardConn == nil {
		err = fmt.Errorf("forward dial returned nil connection")
		self.reportError("forward dial", err)
		return nil, err
	}
	return forwardConn, nil
}

// The dial network of the family of the client's outer socket. An address with
// no family, such as an in-memory pipe, keeps the unnarrowed network.
func forwardNetwork(clientAddress string) string {
	host := clientAddress
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return "tcp"
	}
	if addr.Is4() || addr.Is4In6() {
		return "tcp4"
	}
	return "tcp6"
}

// Copies both directions until either ends, then releases both connections.
func (self *ExtenderServer) relay(
	ctx context.Context,
	cancel context.CancelFunc,
	clientConn net.Conn,
	forwardConn net.Conn,
) {
	var relayWorkers sync.WaitGroup
	relayWorkers.Add(2)
	go connect.HandleError(func() {
		defer relayWorkers.Done()
		// read packet from clientConn, write to forwardConn
		defer cancel()

		buffer := make([]byte, 4096)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			clientConn.SetReadDeadline(time.Now().Add(self.settings.ReadTimeout))
			n, err := clientConn.Read(buffer)
			if n > 0 {
				forwardConn.SetWriteDeadline(time.Now().Add(self.settings.WriteTimeout))
				toWrite := buffer[0:n]
				for len(toWrite) > 0 {
					nw, werr := forwardConn.Write(toWrite)
					if nw > 0 {
						toWrite = toWrite[nw:]
					}
					if werr != nil {
						return
					}
				}
			}
			if err != nil {
				return
			}
		}
	}, cancel)

	go connect.HandleError(func() {
		defer relayWorkers.Done()
		// read packet from forwardConn, write to clientConn
		defer cancel()

		buffer := make([]byte, 4096)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			forwardConn.SetReadDeadline(time.Now().Add(self.settings.ReadTimeout))
			n, err := forwardConn.Read(buffer)
			if n > 0 {
				clientConn.SetWriteDeadline(time.Now().Add(self.settings.WriteTimeout))
				toWrite := buffer[0:n]
				for len(toWrite) > 0 {
					nw, werr := clientConn.Write(toWrite)
					if nw > 0 {
						toWrite = toWrite[nw:]
					}
					if werr != nil {
						return
					}
				}
			}
			if err != nil {
				return
			}
		}
	}, cancel)

	select {
	case <-ctx.Done():
	}
	clientConn.Close()
	forwardConn.Close()
	relayWorkers.Wait()
}

func guessOrganizationName(host string) string {

	// FIXME bringyour api for organization name
	/* For the following hostname, tell me your best guess at the organization name. Only list the full organization name and nothing else. The hostname: yandex.ru
	 */

	// FIXME
	return host
}

// https://github.com/AGWA/tlshacks/blob/main/client_hello.go
// https://pkg.go.dev/crypto/tls#ClientHelloInfo
// https://www.agwa.name/blog/post/parsing_tls_client_hello_with_cryptobyte

// client issues tls connect to for a spoof name and ip:port, and does not check the tls cert
// on top of that connection, sends a header (protocol/extender) that lists the upstream host
// and then makes a tls connection through that
