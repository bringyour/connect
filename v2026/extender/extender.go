package extender

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"maps"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/dns/dnsmessage"
	"golang.org/x/net/http2"
	"golang.org/x/net/idna"

	quic "github.com/quic-go/quic-go"
	"github.com/quic-go/quic-go/http3"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026"
	"github.com/urnetwork/connect/v2026/protocol"
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
// Everything that is not the extender protocol is answered as a real host
// would answer it: a request whose server name is on the whitelist is reverse
// proxied to that name (A5), and a query on udp 53 that is not the translation
// is resolved and answered (A6). A prober therefore sees a working site and a
// working resolver, and the extender protocol stays inside the outer tls.
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

		ProxyMaxRequestByteCount:         1024 * 1024,
		ProxyMaxResponseByteCount:        8 * 1024 * 1024,
		ProxyMaxConnectionCountPerSource: 8,
		ProxyMaxConnectionCount:          256,
		ProxyIdleTimeout:                 30 * time.Second,

		DnsMaxQueryRatePerSource:  10,
		DnsMaxQueryBurstPerSource: 20,
		DnsMaxQueryRate:           500,
		DnsMaxQueryBurst:          500,
		DnsMaxResponseByteCount:   4096,
		DnsForwardWorkerCount:     64,
		DnsForwardTimeout:         5 * time.Second,

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

	// Bounds of the reverse proxy that answers everything that is not an
	// extender request (A5). The request body and the concurrency bounds
	// refuse with 503 before anything is relayed; the response bound cuts a
	// body that is already being written, because the status and headers have
	// already left. <= 0 disables each.
	ProxyMaxRequestByteCount         int64
	ProxyMaxResponseByteCount        int64
	ProxyMaxConnectionCountPerSource int
	ProxyMaxConnectionCount          int
	// Budget for one upstream response header and for each further step of the
	// relay, so a site that stops sending releases the proxied slot (A5).
	ProxyIdleTimeout time.Duration
	// SpoofDomains, when set, replaces connect.SpoofDomains() in the whitelist
	// (A5, A10). Tests install synthetic names through it.
	SpoofDomains []string
	// ProxyTlsConfig, when set, is the upstream client configuration of the
	// reverse proxy, which verifies normally. Tests inject the roots of their
	// fixture site. Nil keeps the platform defaults.
	ProxyTlsConfig *tls.Config

	// Bounds of the udp 53 forwarder that answers queries which are not the
	// translation (A6). The per-source rate limit and the one in-flight query
	// per source keep one address from spending the whole budget; a query over
	// any of them is dropped silently. <= 0 disables each.
	DnsMaxQueryRatePerSource  float64
	DnsMaxQueryBurstPerSource int
	DnsMaxQueryRate           float64
	DnsMaxQueryBurst          int
	// Answers larger than this are truncated with the TC bit (A6).
	DnsMaxResponseByteCount int
	// Queries resolved at once. The forwarder hands every query to these
	// workers and drops when they are all busy, so a slow resolver never
	// blocks the translation's read loop (A6).
	DnsForwardWorkerCount int
	// Budget of one resolution, which bounds how long a query holds a worker
	// and its source's in-flight slot. Not named in A6; without it a resolver
	// that never answers retires a worker permanently.
	DnsForwardTimeout time.Duration
	// DnsForward, when set, resolves one question and returns the raw dns
	// response wire (A6). Nil builds a DohCache from DohSettings on first use.
	DnsForward func(ctx context.Context, qType dnsmessage.Type, name string) ([]byte, bool)
	// DohSettings, when set, configures the forwarder's own DoH cache. Nil
	// takes connect.DefaultDohSettings(), which is the connect default server
	// list (A6).
	DohSettings *connect.DohSettings

	// Encoding tlds of the dns carrier. A query that does not use one of them
	// is not part of the translation.
	DnsTlds []string

	// DnsPrivilegedPort also binds the dns carrier on 53, beside the
	// unprivileged port the caller configured (L2). Only the platforms that
	// can take 53 without privilege set it -- the linux daemon and the windows
	// service -- and the bind is never required: a failure is reported through
	// ListenErrorHandler and the carrier keeps serving on its other port.
	DnsPrivilegedPort bool

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
	// ListenErrorHandler, when set, receives the bind failure of one carrier
	// (G2). Each carrier is bound independently: a failure disables that
	// carrier and is reported here once, and only a failure of every carrier
	// ends ListenAndServe with an error. The provider role logs it once and
	// retries on the activation cadence, never as a user-visible error. It
	// runs synchronously and must not block.
	ListenErrorHandler func(carrier string, err error)
	// CertificateHandler, when set, receives the outer sni of every handshake
	// as its certificate is selected, on every carrier. Tests use it to observe
	// what a dial presented; an empty name is a ClientHello that carried no sni
	// at all (A10). It runs synchronously and must not block.
	CertificateHandler func(serverName string)
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

	// the relayed traffic of O1, summed over every carrier and cumulative for
	// the life of the server. They are taken at the relay copy of each
	// direction rather than at a carrier, so a byte is counted exactly once
	// whatever framed it, and the reverse proxy and the dns forwarder, which
	// relay nothing for a client, count nothing.
	ingressByteCount atomic.Int64
	ingressReadCount atomic.Int64
	egressByteCount  atomic.Int64
	egressReadCount  atomic.Int64

	allowedSecrets []string
	// exact (x) or wildcard (*.x)
	// wildcard *.x does not match exact x
	allowedHosts []string
	ports        map[int][]connect.ExtenderConnectMode
	// the carriers whose bind succeeded, in wire order. Empty until
	// ListenAndServe has bound them, because a carrier that did not bind must
	// not be offered to a client or to an activation (G2).
	carriers []string
	// the dns ports whose bind succeeded, ascending, which is the order a
	// client dials them in (L2). What the activation advertises, so a port
	// that did not bind is never probed.
	dnsPorts []int
	// the last bind failure of each carrier that has one, which is what a
	// provider role renders beside the carrier list (G2, F3). A carrier that
	// binds clears its entry.
	carrierListenErrs map[string]error
	forwardDialer     *net.Dialer

	// closed once every carrier bind has been attempted, and at the latest
	// when serving ends. A caller that reports the carriers -- the activation
	// loop of G3 -- waits on it rather than announcing a list still being
	// bound.
	listening     chan struct{}
	listeningOnce sync.Once

	certificates    *extenderCertificates
	certificatesErr error

	proxy *extenderProxy

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
		carriers:               []string{},
		dnsPorts:               []int{},
		carrierListenErrs:      map[string]error{},
		forwardDialer:          forwardDialer,
		listening:              make(chan struct{}),
		settings:               settings,
	}

	// A certificate failure is reported when serving starts, so the
	// constructor keeps its shape for callers that cannot handle an error.
	self.certificates, self.certificatesErr = newExtenderCertificates(settings.IdentityKeySeed, settings)
	self.proxy = newExtenderProxy(self)

	handler := &extenderHandler{server: self}
	// an idle h2 connection is reclaimed on the same budget a connection has to
	// make its request; a proxied exchange on it is bounded by the proxy
	self.http2Server = &http2.Server{
		IdleTimeout: settings.HeaderTimeout,
	}
	self.httpServer = &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: settings.HeaderTimeout,
		// a request whose body never arrives is dropped on the same budget,
		// and net/http clears the deadline on hijack, so the relay that
		// follows an accepted request keeps only its own deadlines (A9)
		ReadTimeout: settings.HeaderTimeout,
		ErrorLog:    log.New(extenderLogWriter{}, "", 0),
		// the sni of a terminated tcp connection is not on the request, which
		// is no longer a *tls.Conn, so it is carried on the connection context
		// along with the per-connection proxy budget (A3, A5)
		ConnContext: func(ctx context.Context, conn net.Conn) context.Context {
			serverName := ""
			if requestConn, ok := conn.(*connWithInitialBytes); ok {
				serverName = requestConn.serverName
			}
			return newExtenderRequestContext(ctx, serverName)
		},
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
		// the udp carriers take the sni from the request, which carries the
		// terminated connection state, so only the proxy budget is per
		// connection here
		ConnContext: func(ctx context.Context, quicConn *quic.Conn) context.Context {
			return newExtenderRequestContext(ctx, "")
		},
	}

	return self
}

// The carrier names in wire order, which is the order Carriers reports (A4).
var extenderCarrierOrder = []string{
	connect.ExtenderCarrierTcp,
	connect.ExtenderCarrierQuic,
	connect.ExtenderCarrierDns,
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
	return connectionSourceAddress(remoteAddressString(remoteAddr))
}

// The limit key of an address in text form, which is how a request carries the
// peer it arrived from.
func connectionSourceAddress(address string) string {
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
//
// Each carrier is bound independently (G2): a bind that fails is reported
// through ListenErrorHandler, disables that carrier, and leaves the others
// serving, because an extender that cannot take udp 443 is still a working
// extender on tcp 443. Only a failure of every carrier is an error. A
// configuration mistake -- two udp carriers on one port -- is not a bind
// failure and is reported before anything is bound.
//
// Nothing is served until every bind has been attempted, so a client that
// reaches the first carrier up can never be told a carrier list that is still
// being assembled. A Close that lands while a carrier is binding ends the call
// with no error, as a Close after the carriers are up does.
func (self *ExtenderServer) ListenAndServe() error {
	if !self.beginWorker() {
		// shutdown started before serving, which is not a bind failure
		self.markListening()
		return nil
	}
	defer self.endWorker()
	defer self.Close()
	// a caller waiting to report the carriers must be released on every exit,
	// including the failures below
	defer self.markListening()

	if self.certificatesErr != nil {
		return self.certificatesErr
	}

	ports := self.listenPorts()

	// one udp carrier per port: the two cannot share a socket, and binding
	// the port once and dropping a carrier silently would hide the mistake
	for port, connectModes := range ports {
		udpCarrierCount := 0
		for _, connectMode := range connectModes {
			switch connectMode {
			case connect.ExtenderConnectModeQuic, connect.ExtenderConnectModeDns:
				udpCarrierCount += 1
			}
		}
		if 1 < udpCarrierCount {
			return fmt.Errorf("port %d lists more than one udp carrier: %v", port, connectModes)
		}
	}

	// one bound tcp listener, accepted only after every carrier has bound
	type boundListener struct {
		listener      net.Listener
		ownedListener *extenderOwnedListener
	}
	// one bound udp endpoint and the carrier it serves
	type boundPacketConn struct {
		connectMode connect.ExtenderConnectMode
		packetConn  net.PacketConn
		ownedCloser *extenderOwnedCloser
	}

	boundListeners := []*boundListener{}
	boundPacketConns := []*boundPacketConn{}
	bindErrs := []error{}
	defer func() {
		for _, bound := range boundListeners {
			bound.listener.Close()
			self.removeListener(bound.ownedListener)
		}
		for _, bound := range boundPacketConns {
			bound.ownedCloser.close()
			self.removeCloser(bound.ownedCloser)
		}
	}()

	for port, connectModes := range ports {
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
			self.reportListenError(connect.ExtenderCarrierTcp, err)
			bindErrs = append(bindErrs, err)
			continue
		}
		if listener == nil {
			err := fmt.Errorf("extender listener factory returned nil")
			self.reportListenError(connect.ExtenderCarrierTcp, err)
			bindErrs = append(bindErrs, err)
			continue
		}
		ownedListener, ok := self.addListener(listener)
		if !ok {
			// shutdown started while binding, which is not a bind failure
			return nil
		}
		boundListeners = append(boundListeners, &boundListener{
			listener:      listener,
			ownedListener: ownedListener,
		})
		self.addCarrier(connect.ExtenderCarrierTcp)
	}

	for port, connectModes := range ports {
		connectMode := connect.ExtenderConnectMode("")
		for _, portConnectMode := range connectModes {
			switch portConnectMode {
			case connect.ExtenderConnectModeQuic, connect.ExtenderConnectModeDns:
				connectMode = portConnectMode
			}
		}
		if connectMode == "" {
			continue
		}
		carrier := connect.ExtenderCarrierForConnectMode(connectMode)

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
			self.reportListenError(carrier, err)
			bindErrs = append(bindErrs, err)
			continue
		}
		if packetConn == nil {
			err := fmt.Errorf("extender packet listener factory returned nil")
			self.reportListenError(carrier, err)
			bindErrs = append(bindErrs, err)
			continue
		}
		ownedCloser, ok := self.addCloser(func() { packetConn.Close() })
		if !ok {
			// shutdown started while binding, which is not a bind failure
			return nil
		}
		boundPacketConns = append(boundPacketConns, &boundPacketConn{
			connectMode: connectMode,
			packetConn:  packetConn,
			ownedCloser: ownedCloser,
		})
		self.addCarrier(carrier)
		if connectMode == connect.ExtenderConnectModeDns {
			self.addDnsPort(port)
		}
	}

	if len(boundListeners) == 0 && len(boundPacketConns) == 0 {
		if 0 < len(bindErrs) {
			return errors.Join(bindErrs...)
		}
		// nothing was configured to bind; there is nothing to serve
		return fmt.Errorf("extender has no carrier to listen on")
	}
	// a carrier that is serving has no standing bind failure, whichever order
	// its ports were bound in
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		for _, carrier := range self.carriers {
			delete(self.carrierListenErrs, carrier)
		}
	}()
	// the carrier list is complete, so nothing served below can report a
	// partial one
	self.markListening()

	for _, bound := range boundListeners {
		listener := bound.listener
		if !self.beginWorker() {
			// shutdown started while binding, which is not a bind failure
			return nil
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

	for _, bound := range boundPacketConns {
		connectMode := bound.connectMode
		packetConn := bound.packetConn
		if !self.beginWorker() {
			// shutdown started while binding, which is not a bind failure
			return nil
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

// Listening closes once every carrier bind has been attempted, and at the
// latest when serving ends. Carriers is complete from that point, so the
// activation loop waits here before it reports what this extender serves (G2,
// G3). A caller must also watch its own cancellation: an extender that is never
// served never binds.
func (self *ExtenderServer) Listening() <-chan struct{} {
	return self.listening
}

// Releases whoever waits on Listening. Idempotent, because both the successful
// bind path and every failure path reach it.
func (self *ExtenderServer) markListening() {
	self.listeningOnce.Do(func() {
		close(self.listening)
	})
}

// The ports to bind: the configured ones, plus 53 for the dns carrier when the
// platform can take it without privilege (L2). The extra bind is additive --
// the configured unprivileged port is bound either way -- and its failure is
// reported like any other carrier bind failure without ending the serve.
func (self *ExtenderServer) listenPorts() map[int][]connect.ExtenderConnectMode {
	ports := maps.Clone(self.ports)
	if ports == nil {
		ports = map[int][]connect.ExtenderConnectMode{}
	}
	if !self.settings.DnsPrivilegedPort {
		return ports
	}
	hasDns := false
	for _, connectModes := range ports {
		if slices.Contains(connectModes, connect.ExtenderConnectModeDns) {
			hasDns = true
			break
		}
	}
	if !hasDns {
		// nothing configured the dns carrier, so there is nothing to widen
		return ports
	}
	if slices.Contains(ports[connect.DefaultDnsPort], connect.ExtenderConnectModeDns) {
		return ports
	}
	ports[connect.DefaultDnsPort] = append(
		slices.Clone(ports[connect.DefaultDnsPort]),
		connect.ExtenderConnectModeDns,
	)
	return ports
}

// Adds one bound dns port, keeping the ascending dial order of L2. Only a bind
// that succeeded reaches this.
func (self *ExtenderServer) addDnsPort(port int) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if slices.Contains(self.dnsPorts, port) {
		return
	}
	self.dnsPorts = append(self.dnsPorts, port)
	slices.Sort(self.dnsPorts)
}

// The dns ports this extender is listening on, ascending, which is the order a
// client dials them in (L2). Complete once Listening has closed, and what the
// activation advertises so the operator never probes a port that did not bind.
func (self *ExtenderServer) DnsPorts() []int {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return slices.Clone(self.dnsPorts)
}

// Adds one carrier to what this extender serves, keeping wire order (A4). Only
// a bind that succeeded reaches this.
func (self *ExtenderServer) addCarrier(carrier string) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if slices.Contains(self.carriers, carrier) {
		return
	}
	self.carriers = append(self.carriers, carrier)
	slices.SortFunc(self.carriers, func(a string, b string) int {
		return slices.Index(extenderCarrierOrder, a) - slices.Index(extenderCarrierOrder, b)
	})
	// a carrier that is serving has no standing bind failure
	delete(self.carrierListenErrs, carrier)
}

// Reports one carrier bind failure (G2). The carrier is disabled; the log line
// is unconditional because a bind that failed is a configuration fact an
// operator needs, and the handler is what the sdk provider role reads.
func (self *ExtenderServer) reportListenError(carrier string, err error) {
	log.Printf("[extender] listen error (%s): %s", carrier, err)
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		self.carrierListenErrs[carrier] = err
	}()
	if self.settings.ListenErrorHandler != nil {
		self.settings.ListenErrorHandler(carrier, err)
	}
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
		// every query that is not the translation is resolved and answered on
		// this same socket, so a prober of udp 53 gets a working resolver (A6)
		forwarder := newExtenderDnsForwarder(self, packetConn)
		defer forwarder.close()
		ptSettings.DnsOtherHandler = forwarder.handleQuery
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
	self.proxy.close()
}

// CloseAndWait interrupts and joins every listener and connection worker.
func (self *ExtenderServer) CloseAndWait() {
	self.Close()
	self.workers.Wait()
}

// An empty secret list is an open extender, which is what an operator
// activated extender is; a non-empty list requires the hmac over timestamp and
// nonce to match one entry, which is the private extender of a network space's
// manual configuration (A4).
func (self *ExtenderServer) IsAllowedSecret(header *protocol.ExtenderHeader) bool {
	if len(self.allowedSecrets) == 0 {
		return true
	}
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

// The carrier names this extender is listening on (A4, G2). Empty until the
// binds have been attempted, which Listening reports.
func (self *ExtenderServer) Carriers() []string {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return slices.Clone(self.carriers)
}

// The last bind failure of each carrier that has one (G2). A carrier that bound
// has no entry, so an empty map with a full carrier list is a fully listening
// extender. The failures are kept rather than only handed to
// ListenErrorHandler because a status is read long after the bind.
func (self *ExtenderServer) ListenErrors() map[string]error {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return maps.Clone(self.carrierListenErrs)
}

// Connections open over every carrier right now, which is what the per-source
// and total caps of A9 are counted against.
func (self *ExtenderServer) ConnectionCount() int {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.connectionCount
}

// ExtenderStats is the traffic one extender has relayed, summed over every
// carrier and cumulative for the life of the server like the packet stats of a
// device (O1). It is operator-centric: ingress is what moves from a client
// toward the forward destination and egress what moves back from the
// destination toward the client. A read is one chunk the relay moved on one
// side, because a byte stream has no packet boundary in userspace.
type ExtenderStats struct {
	IngressByteCount int64
	IngressReadCount int64
	EgressByteCount  int64
	EgressReadCount  int64
}

// A snapshot of the relayed traffic (O1). The four counters are read
// independently, so a snapshot taken while a relay is running can hold a byte
// count of one direction from just after a read whose count it missed; the
// series that samples it reads deltas over a second and never a total that
// must agree across directions.
func (self *ExtenderServer) Stats() ExtenderStats {
	return ExtenderStats{
		IngressByteCount: self.ingressByteCount.Load(),
		IngressReadCount: self.ingressReadCount.Load(),
		EgressByteCount:  self.egressByteCount.Load(),
		EgressReadCount:  self.egressReadCount.Load(),
	}
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
	// the terminated connection is the only place the requested name survives:
	// what the http server serves from here is no longer a *tls.Conn (A3, A5)
	connectionState := clientConn.ConnectionState()
	requestConn := newConnWithInitialBytes(clientConn, initialBytes, connectionState.ServerName)
	self.serveHttpConnection(handleCtx, requestConn, connectionState.NegotiatedProtocol)
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
		// h2 never reaches the ConnContext of the http server, so the request
		// context is built here instead
		self.http2Server.ServeConn(requestConn, &http2.ServeConnOpts{
			Context:    newExtenderRequestContext(ctx, requestConn.serverName),
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
	forwardConn, err := self.dialContext()(ctx, forwardNetwork(clientAddress), net.JoinHostPort(
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

// The egress of this extender: the configured seam, or the forward dialer.
// The forward and the reverse proxy share it, so a test that injects one
// observes both (A5, A7).
func (self *ExtenderServer) dialContext() connect.DialContextFunction {
	if self.settings.DialContext != nil {
		return self.settings.DialContext
	}
	return self.forwardDialer.DialContext
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
				// the ingress of O1, counted before the write so a client that
				// has seen the round trip has seen the count
				self.ingressByteCount.Add(int64(n))
				self.ingressReadCount.Add(1)
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
				// the egress of O1
				self.egressByteCount.Add(int64(n))
				self.egressReadCount.Add(1)
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
