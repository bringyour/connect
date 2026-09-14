package connect

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"sync"
	"time"

	// "golang.org/x/net/proxy"

	"maps"

	"github.com/gorilla/websocket"
)

// censorship-resistant strategies for making https connections
// this uses a random walk of best practices to obfuscate and double-encrypt https connections

// note the net_* files are migrated from net.IP/IPNet to netip.Addr/Prefix
// TODO generally the entire package should migrate over to these newer structs

type HttpPostRawFunction func(ctx context.Context, requestUrl string, requestBodyBytes []byte, byJwt string) ([]byte, error)
type HttpGetRawFunction func(ctx context.Context, requestUrl string, byJwt string) ([]byte, error)

// DefaultMaxHttpResponseBodyBytes bounds API responses that are materialized
// in memory. API calls return JSON/control data rather than streamed payloads;
// larger payloads must use an explicitly streaming transport.
const DefaultMaxHttpResponseBodyBytes int64 = 2 * 1024 * 1024

var ErrHttpResponseBodyTooLarge = errors.New("http response body exceeds memory limit")

func DefaultClientStrategySettings() *ClientStrategySettings {
	settings := &ClientStrategySettings{
		ExposeServerIps:       true,
		ExposeServerHostNames: true,

		EnableNormal:    true,
		EnableResilient: true,

		ParallelBlockSize: 4,

		ExpandExtenderProfileCount:   8,
		ReconnectTimeout:             15 * time.Second,
		MaxExtenderCount:             128,
		ExtenderMinimumWeight:        0.1,
		ExtenderDropTimeout:          5 * time.Minute,
		ExtenderInitialSampleTimeout: 2 * time.Second,

		DohSettings: DefaultDohSettings(),
		DnsTlds:     [][]byte{[]byte(DefaultExtenderDnsTld)},

		HelloRetryTimeout:        5 * time.Second,
		MaxHttpResponseBodyBytes: DefaultMaxHttpResponseBodyBytes,

		GetRetryCount:       1,
		GetRetryStatusCodes: []int{http.StatusBadGateway, http.StatusServiceUnavailable},
		GetRetryMinTimeout:  100 * time.Millisecond,
		GetRetryMaxTimeout:  1000 * time.Millisecond,

		MinNextConnectDelay: 100 * time.Millisecond,
		MaxNextConnectDelay: 1000 * time.Millisecond,

		Http2SendPingTimeout: 10 * time.Second,
		Http2PingTimeout:     5 * time.Second,

		ConnectSettings: *DefaultConnectSettings(),
	}
	// A configured process budget identifies an embedded/mobile client. Bound
	// the connection-resident HTTP/WebSocket working set there; an unset or
	// reference-sized budget leaves Go and gorilla defaults untouched for
	// desktop and server callers.
	if 0 < MemoryBudget() && MemoryBudget() < referenceMemoryBudgetByteCount {
		settings.HttpReadBufferSize = MemoryScaledCount(4*1024, 2*1024)
		settings.HttpWriteBufferSize = MemoryScaledCount(4*1024, 2*1024)
		settings.WebSocketReadBufferSize = MemoryScaledCount(4*1024, 2*1024)
		settings.WebSocketWriteBufferSize = MemoryScaledCount(4*1024, 2*1024)
		settings.Http2MaxDecoderHeaderTableSize = MemoryScaledCount(4*1024, 2*1024)
		settings.Http2MaxEncoderHeaderTableSize = MemoryScaledCount(4*1024, 2*1024)
		settings.Http2MaxReceiveBufferPerConnection = int(
			MemoryScaledByteCount(mib(1), kib(256)),
		)
		settings.Http2MaxReceiveBufferPerStream = int(
			MemoryScaledByteCount(kib(512), kib(128)),
		)
	}
	return settings
}

type ClientStrategySettings struct {
	// Log, when set, is used by the client strategy. nil resolves to
	// `DefaultLogger()`.
	Log Logger

	// expose consistent ips
	// if true, enables ech
	ExposeServerIps bool
	// expose server names
	// if true, enables non-ech
	// TODO set this to default false
	ExposeServerHostNames bool
	// note that extenders and proxy are the only strategies that will be enabled if
	// `ExposeServerIps == false` and `ExposeServerNames == false`

	EnableNormal bool
	// tls frag, retransmit, tls frag + retransmit
	EnableResilient bool

	// for gets and ws connects
	ParallelBlockSize int

	// the number of new extender candidates to draw per expand
	ExpandExtenderProfileCount int
	ReconnectTimeout           time.Duration
	MaxExtenderCount           int
	// extender minimum weight
	ExtenderMinimumWeight float32
	// drop dialers that have not had a successful connect in this timeout
	ExtenderDropTimeout time.Duration
	// ExtenderConfigs installs exact extender endpoints before discovery.
	// Measurement fixtures use it for hermetic production extender paths. Nil
	// retains normal discovery and selection. The strategy copies each entry.
	ExtenderConfigs []*ExtenderConfig
	// ExtenderDirectory is where discovered extenders come from (E1, E2). The
	// strategy draws candidates from it, reports every dial outcome back to
	// it, and drops the dialers of addresses it retires. Nil disables
	// discovery, which is what a direct or url-only strategy wants.
	ExtenderDirectory *ExtenderDirectory
	// How long a cold start waits for the network client's first feed sample
	// before dialing without extenders (E4). The wait happens only while the
	// directory has nothing usable and a network client is still on its first
	// attempt.
	ExtenderInitialSampleTimeout time.Duration

	// ipFamily is the family pin of `NewDirectClientStrategy`, applied to the
	// strategy's own udp dials -- the alt carriers, which do not go through a
	// dial context. 0 is the family-agnostic strategy this has always been.
	// The stream dials are pinned through `DialContextSettings` instead.
	ipFamily int

	// AltUrl enables the two api alt dialers (L4). Only its host and explicit
	// port are read: the alt host decides where the packets go, while the
	// request's own host name stays the sni and the certificate is verified
	// against it as on any direct api dial. Empty disables both dialers, which
	// is every space that has no alt deployment. A port here pins both
	// carriers to it, which is what an in-process fixture wants.
	AltUrl string
	// The encoding tlds of the alt whodis dialer, one picked at random per
	// dial. Empty takes `DefaultExtenderDnsTld`.
	DnsTlds [][]byte

	DohSettings *DohSettings
	// InternalDohDomains are network-space domains whose exact host and
	// subdomains resolve through the strategy's direct DoH cache before a
	// control connection is dialed by raw IP. The request hostname remains
	// unchanged for HTTP Host, TLS SNI, and certificate verification.
	//
	// An explicit ConnectSettings.Resolver takes precedence and disables this
	// rule. This lets embedders and tests retain a resolver they installed.
	InternalDohDomains []string

	HelloRetryTimeout time.Duration

	// MaxHttpResponseBodyBytes is the largest response body the strategy will
	// materialize. Values <= 0 use DefaultMaxHttpResponseBodyBytes so a partial
	// settings struct cannot accidentally restore an unbounded io.ReadAll.
	MaxHttpResponseBodyBytes int64

	// Embedded low-memory transports set explicit connection-resident buffer
	// and HTTP/2 dynamic-table/receive-window bounds. Zero preserves the
	// library defaults, which is the desktop/server behavior.
	HttpReadBufferSize                 int
	HttpWriteBufferSize                int
	WebSocketReadBufferSize            int
	WebSocketWriteBufferSize           int
	Http2MaxDecoderHeaderTableSize     int
	Http2MaxEncoderHeaderTableSize     int
	Http2MaxReceiveBufferPerConnection int
	Http2MaxReceiveBufferPerStream     int

	// HTTP/2 health check for POOLED control-plane connections. Go performs no
	// health check at all when SendPingTimeout is zero (net/http.HTTP2Config:
	// "If zero, no health check is performed"), so a connection that connected
	// cleanly and later went dark stays in the idle pool and every later
	// request multiplexed onto it hangs to the request timeout.
	//
	// Settings fields rather than package constants, per CONSTANTAUDIT.md:
	// they are per-connection tunables and the settings struct that owns the
	// transport reaches the use site directly.
	Http2SendPingTimeout time.Duration
	Http2PingTimeout     time.Duration

	// retry a GET whose RESPONSE status is in `GetRetryStatusCodes` —
	// transient gateway statuses meaning the lb momentarily had no healthy
	// upstream (the edge of a deploy). The strategy already retries
	// transport-level failures internally; this covers the surfaced-status
	// case, which callers otherwise see immediately as an error. GETs only:
	// the api's GET endpoints are idempotent (the parallel dialer racing
	// already re-issues them), and a POST is never replayed by the client.
	GetRetryCount       int
	GetRetryStatusCodes []int
	// the jittered pause before a retry, uniform in
	// [GetRetryMinTimeout, GetRetryMaxTimeout)
	GetRetryMinTimeout time.Duration
	GetRetryMaxTimeout time.Duration

	MinNextConnectDelay time.Duration
	MaxNextConnectDelay time.Duration

	// ExtraHeaders, when set, are applied (Set semantics, overriding same-named
	// headers) to every request this strategy issues: http serial/parallel,
	// the hello ping, and websocket dials. Intended for test/simulation
	// environments, e.g. presenting a forwarded-for address to a local server.
	ExtraHeaders http.Header

	ConnectSettings
}

// stores statistics on client strategies
type ClientStrategy struct {
	ctx                context.Context
	cancel             context.CancelFunc
	closeOnce          sync.Once
	unsubNetworkChange func()
	log                Logger

	settings *ClientStrategySettings
	// internalDohResolver exists only when InternalDohDomains are configured
	// and the caller did not install ConnectSettings.Resolver.
	internalDohResolver *internalDohResolver

	mutex sync.Mutex
	// dialers are only updated inside the mutex
	dialers map[*clientDialer]bool

	// custom extenders
	// these take precedence over other extenders
	extenderIpSecrets map[netip.Addr]string

	nextConnectTime time.Time
	// reconnectFastPathCount is the number of reconnect fast-path slots
	// currently held (see NextReconnectTime). Guarded by mutex. The zero value
	// means all slots free, so a bare test-constructed strategy works
	// unchanged.
	reconnectFastPathCount int
}

func NewClientStrategyWithDefaults(ctx context.Context) *ClientStrategy {
	return NewClientStrategy(ctx, DefaultClientStrategySettings())
}

func newNormalDialTlsContext(
	settings *ClientStrategySettings,
	nextProtos []string,
) DialTlsContextFunction {
	tlsConfig := newClientTlsConfig(settings.TlsConfig, nextProtos)
	// Every dial takes the explicit path below, including the mobile shape
	// (no proxy, no injected dial context) that used to shortcut to a raw
	// tls.Dialer here. That shortcut bypassed ConnectSettings.DialContext, so
	// the address-family policy expressed there was honored by the fragment
	// and reorder dialers and ignored by this one -- a strategy that raced a
	// forced dialer against an unforced one. It also hid the tls handshake,
	// which is where a blackholed path actually fails.
	//
	// The cost is that ConnectTimeout and TlsTimeout are now separate budgets
	// rather than one deadline shared across connect and handshake. That is
	// the intended behavior, and it applies to every dial, not only forced
	// ones.

	return func(ctx context.Context, network string, addr string) (net.Conn, error) {
		// the handshake half of the dial. It does NOT close the connection it
		// was handed on its own error paths -- dialControlTlsWithFamilyFallback
		// owns that connection, because it has to read the family off it before
		// it goes away.
		handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
			netDialer := settings.NetDialer()
			if netDialer.Timeout != 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, netDialer.Timeout)
				defer cancel()
			}
			if !netDialer.Deadline.IsZero() {
				var cancel context.CancelFunc
				ctx, cancel = context.WithDeadline(ctx, netDialer.Deadline)
				defer cancel()
			}

			host, _, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}

			config := tlsConfig.Clone()
			if config.ServerName == "" {
				config.ServerName = host
			}
			tlsConn := tls.Client(conn, config)
			tlsCtx, tlsCancel := context.WithTimeout(ctx, settings.TlsTimeout)
			defer tlsCancel()
			if err := tlsConn.HandshakeContext(tlsCtx); err != nil {
				tlsConn.Close()
				return nil, err
			}
			return tlsConn, nil
		}
		// DialContext preserves injected userspace networks in tests and proxy
		// routing in production before wrapping the resulting connection in TLS.
		return dialControlTlsWithFamilyFallback(
			ctx, &settings.ConnectSettings, network, addr, settings.DialContext, handshake)
	}
}

// extender udp 53 to platform extender
func NewClientStrategy(ctx context.Context, settings *ClientStrategySettings) *ClientStrategy {
	settings, internalDohResolver := clientStrategySettingsWithInternalDoh(settings)

	// propagate so a strategy-level logger covers dial logging. Copy instead
	// of writing through the caller's settings: the caller may share them
	// with concurrent constructions or other readers (see the platform
	// transport framer settings for the same rule).
	if settings.ConnectSettings.Log == nil {
		copied := *settings
		copied.ConnectSettings.Log = settings.Log
		settings = &copied
	}

	// create dialers to match settings
	dialers := map[*clientDialer]bool{}

	if settings.EnableNormal {
		// TODO ECH support
		if settings.ExposeServerHostNames && settings.ExposeServerIps {
			dialer := &clientDialer{
				description:        "normal",
				createTime:         time.Now(),
				minimumWeight:      0.5,
				priority:           25,
				dialTlsContext:     newNormalDialTlsContext(settings, clientWebSocketNextProtos),
				httpDialTlsContext: newNormalDialTlsContext(settings, clientHttpNextProtos),
				settings:           settings,
			}
			dialers[dialer] = true
		}
	}
	if settings.EnableResilient {
		// TODO ECH support
		if settings.ExposeServerHostNames && settings.ExposeServerIps {
			// fragment+reorder
			dialer1 := &clientDialer{
				description:        "fragment+reorder",
				createTime:         time.Now(),
				minimumWeight:      0.25,
				priority:           50,
				dialTlsContext:     newResilientDialTlsContext(&settings.ConnectSettings, true, true, clientWebSocketNextProtos),
				httpDialTlsContext: newResilientDialTlsContext(&settings.ConnectSettings, true, true, clientHttpNextProtos),
				settings:           settings,
			}
			// fragment
			// this is the highest priority because it has no performance impact and additional security benefits
			dialer2 := &clientDialer{
				description:        "fragment",
				createTime:         time.Now(),
				minimumWeight:      0.25,
				priority:           0,
				dialTlsContext:     newResilientDialTlsContext(&settings.ConnectSettings, true, false, clientWebSocketNextProtos),
				httpDialTlsContext: newResilientDialTlsContext(&settings.ConnectSettings, true, false, clientHttpNextProtos),
				settings:           settings,
			}
			// reorder
			dialer3 := &clientDialer{
				description:        "reorder",
				createTime:         time.Now(),
				minimumWeight:      0.25,
				priority:           50,
				dialTlsContext:     newResilientDialTlsContext(&settings.ConnectSettings, false, true, clientWebSocketNextProtos),
				httpDialTlsContext: newResilientDialTlsContext(&settings.ConnectSettings, false, true, clientHttpNextProtos),
				settings:           settings,
			}

			dialers[dialer1] = true
			dialers[dialer2] = true
			dialers[dialer3] = true
		}
	}
	for _, extenderConfig := range settings.ExtenderConfigs {
		if extenderConfig == nil {
			continue
		}
		copiedConfig := *extenderConfig
		dialer := &clientDialer{
			description:        "configured extender",
			createTime:         time.Now(),
			persistent:         true,
			minimumWeight:      settings.ExtenderMinimumWeight,
			priority:           100,
			dialTlsContext:     newExtenderDialTlsContext(&settings.ConnectSettings, &copiedConfig, clientWebSocketNextProtos),
			httpDialTlsContext: newExtenderDialTlsContext(&settings.ConnectSettings, &copiedConfig, clientHttpNextProtos),
			extenderConfig:     &copiedConfig,
			settings:           settings,
		}
		dialers[dialer] = true
	}
	// FIXME
	/*
		if settings.EnablePt {
			// these route the api via the connect server
			// the connect server runs the api listening for pt connections to the api host

			ptDialer1 := &clientDialer{
				description:    "dns",
				minimumWeight:  0.25,
				priority:       100,
				dialTlsContext: NewPtDialTlsContext(
					&settings.ConnectSettings,
					PacketTranslationModeDns,
					&settings.PacketTranslationSettings,
				),
				settings:       settings,
			}
			ptDialer2 := &clientDialer{
				description:    "dnspump",
				minimumWeight:  0.25,
				priority:       125,
				dialTlsContext: NewPtDialTlsContext(
					&settings.ConnectSettings,
					PacketTranslationModeDnsPump,
					&settings.PacketTranslationSettings,
				),
				settings:       settings,
			}

			dialers[ptDialer1] = true
			dialers[ptDialer2] = true
		}
	*/

	strategyCtx, strategyCancel := context.WithCancel(ctx)
	clientStrategy := &ClientStrategy{
		ctx:                 strategyCtx,
		cancel:              strategyCancel,
		log:                 loggerOrDefault(settings.Log),
		settings:            settings,
		internalDohResolver: internalDohResolver,
		dialers:             dialers,
		extenderIpSecrets:   map[netip.Addr]string{},
	}
	// the alt dialers need the strategy itself, for its resolver and its
	// lifetime, so they join the same map after it is built and before
	// anything can read it (L4)
	for _, dialer := range newAltDialers(clientStrategy, settings) {
		dialers[dialer] = true
	}
	// a host network path change drops the dialers' pooled http connections:
	// they are bound to the old path, and the next api call (auth,
	// find-providers) would otherwise stall on a dead socket until its
	// timeout. Clients rebuild lazily on next use. Unsubscribe rides ctx.
	unsubNetworkChange := AddNetworkChangeListener(clientStrategy.networkChanged)
	clientStrategy.unsubNetworkChange = unsubNetworkChange
	go HandleError(func() {
		<-strategyCtx.Done()
		clientStrategy.Close()
	})
	return clientStrategy
}

// Releases every strategy-owned idle HTTP connection without making the
// strategy terminal. Network changes use the same operation before lazy
// redial on the new path.
func (self *ClientStrategy) CloseIdleConnections() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	for dialer := range self.dialers {
		dialer.Close()
	}
	if self.internalDohResolver != nil {
		self.internalDohResolver.CloseIdleConnections()
	}
}

// Ends discovery and releases pooled HTTP connections. APIs and transports
// sharing the strategy must be closed first; repeated calls are safe.
func (self *ClientStrategy) Close() {
	self.closeOnce.Do(func() {
		if self.cancel != nil {
			self.cancel()
		}
		if self.unsubNetworkChange != nil {
			self.unsubNetworkChange()
		}
		self.CloseIdleConnections()
		if self.internalDohResolver != nil {
			self.internalDohResolver.Close()
		}
	})
}

// networkChanged drops every dialer's pooled connections (idle sockets bound
// to the old network path); in-flight requests finish on their own
// connections, and the http clients rebuild lazily on next use.
func (self *ClientStrategy) networkChanged() {
	self.CloseIdleConnections()
}

func (self *ClientStrategy) SetCustomExtenders(extenderIpSecrets map[netip.Addr]string) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.extenderIpSecrets = maps.Clone(extenderIpSecrets)
	for dialer, _ := range self.dialers {
		if dialer.IsExtender() && !dialer.persistent {
			dialer.Close()
			delete(self.dialers, dialer)
		}
	}
}

// ExtenderDirectory is the directory this strategy draws extenders from, nil
// when discovery is disabled. The platform transport reads it to report the
// live connections through each address (K4).
func (self *ClientStrategy) ExtenderDirectory() *ExtenderDirectory {
	if self == nil {
		return nil
	}
	return self.settings.ExtenderDirectory
}

func (self *ClientStrategy) CustomExtenders() map[netip.Addr]string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return maps.Clone(self.extenderIpSecrets)
}

// nextConnectMaxLead caps how far the shared next-connect timestamp may run
// ahead of wall clock. Every cold dial advances the one shared timestamp by
// 100ms-1s, and before the cancel release below existed, a dialer whose pacing
// wait was cancelled never gave its step back — so rapid connect/disconnect
// cycles compounded the lead without bound. In the 2026-08-09 field capture,
// 12 window teardowns of ~10 exits each pushed the staircase 60+ seconds ahead
// of wall clock: every new exit was born 'transport down' (its dial slot was a
// minute in the future), cohorts of ~10 transport-downs expired at the 15s
// evaluation deadline, 0 connections were ever proven, and the replacements
// re-queued at the back of the same staircase — unbounded starvation where
// only the first connect cycle ever worked. The cancel release is the primary
// fix; this clamp is the backstop that bounds the damage of any reservation
// that still leaks: a new dialer never waits more than nextConnectMaxLead.
const nextConnectMaxLead = 10 * time.Second

// new connections should use next connect time to avoid flooding the network
// at once.
//
// The returned release gives this caller's reservation back to the staircase.
// It exists for exactly one situation: the caller's pacing wait was cancelled
// (its context ended) before it ever dialed, so the pacing step it reserved
// paces nobody — without the release the step stays consumed and every later
// caller queues behind a dial that will never happen (see nextConnectMaxLead
// for the field failure this produced). A caller that goes on to dial must NOT
// call release — a consumed step is correct pacing for a dial that happened.
// The release is idempotent and never nil, mirroring NextReconnectTime.
func (self *ClientStrategy) NextConnectTime() (time.Time, func()) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	now := time.Now()
	connectDelayRange := self.settings.MaxNextConnectDelay - self.settings.MinNextConnectDelay
	connectDelay := self.settings.MinNextConnectDelay
	if 0 < connectDelayRange {
		connectDelay += time.Duration(mathrand.Int63n(int64(connectDelayRange)))
	}
	nextConnectTime := self.nextConnectTime.Add(connectDelay)
	if nextConnectTime.Before(now) {
		nextConnectTime = now
	}
	if maxLead := now.Add(nextConnectMaxLead); maxLead.Before(nextConnectTime) {
		nextConnectTime = maxLead
	}
	self.nextConnectTime = nextConnectTime

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			self.mutex.Lock()
			defer self.mutex.Unlock()
			// give back this caller's step. Later callers may have stacked
			// behind it; subtracting shifts them all one step earlier, which
			// is exactly the vacated slot closing up. When the returned time
			// was clamped (to now, or to the lead bound) this can under- or
			// over-release by at most one step; the read path re-clamps, so
			// the shared timestamp stays safe in both directions.
			self.nextConnectTime = self.nextConnectTime.Add(-connectDelay)
		})
	}
	return nextConnectTime, release
}

const (
	// reconnectFastPathLimit caps how many callers may hold the reconnect
	// fast path (NextReconnectTime) at once. A device runs a handful of
	// platform transports (h1 + the h3/pt variants), so 4 covers the common
	// migration re-dial burst while guaranteeing the platform LB never sees
	// more than 4 unpaced dials from one strategy -- callers past the cap fall
	// back to the serialized NextConnectTime staircase.
	reconnectFastPathLimit = 4
	// reconnectFastPathMaxDelay is the independent per-caller jitter for a
	// fast-path reconnect: uniform in [0, 250ms). Enough spread that
	// concurrent reconnects do not hit the LB in the same instant, small
	// enough that it never becomes the dominant term of a reconnect.
	reconnectFastPathMaxDelay = 250 * time.Millisecond
)

// NextReconnectTime is the scoped fast path of NextConnectTime for a caller
// whose transport was connected and just lost its connection.
//
// NextConnectTime advances ONE shared timestamp 100ms-1s per caller, which is
// the right shape for cold connects: a burst of brand-new connections
// staircases instead of stampeding the platform. After a network migration the
// same staircase is wrong -- every transport held a working connection seconds
// ago and every one of them must re-dial now, so ~8 necessary re-dials queue
// behind each other and the last waits multiple seconds for a connection the
// network could carry immediately. A reconnect burst is also not a stampede:
// its size is bounded by how many connections were up, and each caller dials
// once.
//
// So a caller that self-identifies as reconnecting draws a small INDEPENDENT
// jitter (0-250ms) that neither reads nor advances the shared timestamp. The
// fast path is capped at reconnectFastPathLimit concurrent holders (a
// semaphore on the strategy) so the platform LB still never sees an unbounded
// herd; a caller past the cap falls back to the serialized path. The returned
// release func frees the slot and MUST be called once the dial attempt
// completes (success or failure); it is idempotent and never nil. Callers use
// this only for the FIRST dial after a lost connection -- retries after a
// failed reconnect go back through NextConnectTime, restoring the old pacing
// exactly when the platform itself is what is failing.
func (self *ClientStrategy) NextReconnectTime() (time.Time, func()) {
	acquired := false
	func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()
		if self.reconnectFastPathCount < reconnectFastPathLimit {
			self.reconnectFastPathCount += 1
			acquired = true
		}
	}()
	if !acquired {
		// over the cap: this burst is not small after all; serialize like any
		// other connect. NextConnectTime takes the mutex itself, so it must be
		// called with the lock released (done above). The staircase release is
		// deliberately dropped: this method's release contract is "dial
		// attempt completed", and handing back a pacing step after a dial that
		// actually happened would defeat the pacing. A cancelled wait on this
		// rare over-cap path therefore leaks its step, bounded by
		// nextConnectMaxLead.
		next, _ := self.NextConnectTime()
		return next, func() {}
	}

	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() {
			self.mutex.Lock()
			defer self.mutex.Unlock()
			self.reconnectFastPathCount -= 1
		})
	}
	jitter := time.Duration(mathrand.Int63n(int64(reconnectFastPathMaxDelay)))
	return time.Now().Add(jitter), release
}

// The weight of each dialer an eval may use. `webSocketOnly` drops the
// api-only dialers, which is every dialer with no websocket dialer (L4).
func (self *ClientStrategy) dialerWeights(webSocketOnly bool) map[*clientDialer]float32 {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	weights := map[*clientDialer]float32{}

	if len(self.extenderIpSecrets) == 0 {
		for dialer, _ := range self.dialers {
			if webSocketOnly && !dialer.supportsWebSocket() {
				continue
			}
			w := dialer.Weight()
			weights[dialer] = w
		}
	} else {
		for dialer, _ := range self.dialers {
			if dialer.IsExtender() {
				weights[dialer] = 1.0
			}
		}
	}

	return weights
}

type httpResult struct {
	response *http.Response

	// status string
	// statusCode int
	// header http.Header
	// trailer http.Header
	bodyBytes []byte
}

type evalResult struct {
	dialer *clientDialer
	wsConn *websocket.Conn
	err    error
	// materialize is run only for the selected HTTP response. A canceled
	// parallel HTTP response is released by its attempt context instead.
	materialize func() error

	httpResult
}

func readHttpResponseBody(response *http.Response, maxBytes int64) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, fmt.Errorf("http response has no body")
	}
	if maxBytes <= 0 {
		maxBytes = DefaultMaxHttpResponseBodyBytes
	}
	if maxBytes < response.ContentLength {
		return nil, fmt.Errorf(
			"%w: content length %d, limit %d",
			ErrHttpResponseBodyTooLarge,
			response.ContentLength,
			maxBytes,
		)
	}

	bodyBytes, err := io.ReadAll(io.LimitReader(response.Body, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if maxBytes < int64(len(bodyBytes)) {
		return nil, fmt.Errorf("%w: limit %d", ErrHttpResponseBodyTooLarge, maxBytes)
	}
	return bodyBytes, nil
}

func httpResponseContextCanceled(ctx context.Context, response *http.Response) bool {
	return ctx.Err() != nil ||
		(response != nil &&
			response.Request != nil &&
			response.Request.Context().Err() != nil)
}

// releaseHttpResponseBody explicitly closes a live response. Once its request
// context is canceled, net/http owns HTTP/1 connection or HTTP/2 stream cleanup
// and the body reference is dropped without entering a potentially blocking
// HTTP/2 Close.
func releaseHttpResponseBody(ctx context.Context, response *http.Response) {
	if response == nil || response.Body == nil {
		return
	}
	body := response.Body
	response.Body = nil
	if httpResponseContextCanceled(ctx, response) {
		return
	}
	body.Close()
}

func newEvalResultFromHttpResponse(response *http.Response, err error, maxBodyBytes int64) *evalResult {
	result := &evalResult{
		err: err,
		httpResult: httpResult{
			response: response,
		},
	}
	if err == nil && (response == nil || response.Body == nil) {
		result.err = fmt.Errorf("http response has no body")
	} else if err == nil {
		result.materialize = func() error {
			// Ownership stays with evalResult until the read outcome is known.
			// A canceled read must not enter a synchronous HTTP/2 Body.Close.
			bodyBytes, readErr := readHttpResponseBody(response, maxBodyBytes)
			if readErr == nil {
				result.bodyBytes = bodyBytes
			}
			return readErr
		}
	}
	return result
}

func (self *evalResult) Selected() *evalResult {
	if self.materialize != nil {
		self.err = self.materialize()
		self.materialize = nil
	}
	return self
}

// Close synchronously releases a live result. Canceled HTTP results must use
// discardAfterContextCancellation because their transport already owns cleanup.
func (self *evalResult) Close() {
	self.materialize = nil
	if self.wsConn != nil {
		wsConn := self.wsConn
		self.wsConn = nil
		wsConn.Close()
		// if wsConn is set, the response does not need to be closed
		// https://pkg.go.dev/github.com/gorilla/websocket#Dialer.DialContext
	} else if self.response != nil && self.response.Body != nil {
		body := self.response.Body
		self.response.Body = nil
		body.Close()
	}
}

// discardAfterContextCancellation releases a result after the request context
// used to create it has already been canceled. net/http owns
// HTTP stream/connection cancellation through that context. Calling an unread
// HTTP/2 response Body.Close here is unsafe: it may wait indefinitely for the
// connection write mutex. A WebSocket has escaped its handshake context once
// DialContext returns, so it still needs an explicit close.
func (self *evalResult) discardAfterContextCancellation() {
	self.materialize = nil
	if self.wsConn != nil {
		wsConn := self.wsConn
		self.wsConn = nil
		wsConn.Close()
	}
	if self.response != nil {
		self.response.Body = nil
	}
}

// releaseAfterUse closes an ordinary live response, but lets net/http finish
// cleanup when either the evaluation or response request context was canceled.
// The response context also catches a timeout derived internally by http.Client.
func (self *evalResult) releaseAfterUse(ctx context.Context) {
	if httpResponseContextCanceled(ctx, self.response) {
		self.discardAfterContextCancellation()
		return
	}
	self.Close()
}

// materializeHttpResult returns the response body already read when the
// strategy selected this result.
func materializeHttpResult(result *evalResult) (*httpResult, error) {
	defer result.releaseAfterUse(context.Background())
	return &result.httpResult, result.err
}

// Give each synchronous route an equal share of the remaining deadline while
// retaining one share for parallel discovery/fallback. A route remembered as
// successful may have become a black hole; it must not consume the request's
// entire deadline before another route is allowed to run.
func preferredEvalAttemptContext(
	ctx context.Context,
	remainingAttemptCount int,
) (context.Context, context.CancelFunc) {
	deadline, ok := ctx.Deadline()
	if !ok || remainingAttemptCount <= 0 {
		return context.WithCancel(ctx)
	}
	remainingTimeout := time.Until(deadline)
	if remainingTimeout <= 0 {
		attemptCtx, attemptCancel := context.WithCancel(ctx)
		attemptCancel()
		return attemptCtx, func() {}
	}
	attemptTimeout := remainingTimeout / time.Duration(remainingAttemptCount+1)
	return context.WithTimeout(ctx, attemptTimeout)
}

// parallelEval races the dialers of one request. `webSocketOnly` restricts it
// to the dialers that can carry a websocket, which is what a platform dial
// needs and an api request does not (L4).
func (self *ClientStrategy) parallelEval(ctx context.Context, webSocketOnly bool, eval func(ctx context.Context, dialer *clientDialer) *evalResult) *evalResult {
	// in this order:
	// 1. try all dialers that previously worked sequentially
	// 2. try dialers that previously failed in parallel blocks
	// 3. expand the extenders and try new extenders in parallel blocks

	handleCtx, handleCancel := context.WithTimeout(ctx, self.settings.RequestTimeout)
	// Every parallel attempt remains owned by this call until its eval stack
	// returns. Cancellation only asks a dial to stop; it is not completion.
	// Register before launch so cleanup never races Wait with a later Add.
	var workerWaitGroup sync.WaitGroup
	startWorker := func(run func(), handlers ...any) {
		workerWaitGroup.Add(1)
		go func() {
			defer workerWaitGroup.Done()
			HandleError(run, handlers...)
		}()
	}
	defer func() {
		handleCancel()
		workerWaitGroup.Wait()
	}()
	// merge handleCtx with self.ctx
	startWorker(func() {
		defer handleCancel()
		select {
		case <-handleCtx.Done():
			return
		case <-self.ctx.Done():
			return
		}
	}, handleCancel)

	// E4: a cold start gives the network client a bounded moment to land its
	// first feed sample, so the first dial can use the extenders it is about
	// to learn instead of racing them
	self.waitForExtenderInitialSample(handleCtx)

	out := make(chan *evalResult)

	run := func(dialer *clientDialer) {
		success := false
		defer func() {
			if !success {
				select {
				case out <- nil:
				case <-handleCtx.Done():
				}
			}
		}()
		result := eval(handleCtx, dialer)
		if result == nil {
			return
		}

		result.dialer = dialer
		select {
		case out <- result:
			success = true
		case <-handleCtx.Done():
			result.discardAfterContextCancellation()
		}
	}

	// keep trying as long as there is time left
	for {
		select {
		case <-handleCtx.Done():
			return nil
		default:
		}

		reconnect := NewReconnect(self.settings.ReconnectTimeout)

		self.collapseExtenderDialers()

		// the number of runs with pending out
		p := 0

		dialerWeights := self.dialerWeights(webSocketOnly)

		if 0 < len(dialerWeights) {
			serialDialers := []*clientDialer{}
			parallelDialers := []*clientDialer{}

			dialers := slices.Collect(maps.Keys(dialerWeights))
			WeightedShuffle(dialers, dialerWeights)

			for _, dialer := range dialers {
				if dialer.IsLastSuccess() {
					serialDialers = append(serialDialers, dialer)
				} else {
					parallelDialers = append(parallelDialers, dialer)
				}
			}

			// WeightedShuffle(serialDialers, dialerWeights)
			slices.SortStableFunc(serialDialers, func(a *clientDialer, b *clientDialer) int {
				return a.priority - b.priority
			})
			for i, dialer := range serialDialers {
				select {
				case <-handleCtx.Done():
					return nil
				default:
				}

				attemptCtx, attemptCancel := preferredEvalAttemptContext(
					handleCtx,
					len(serialDialers)-i,
				)
				result := eval(attemptCtx, dialer)
				if result != nil {
					result.dialer = dialer
					if result.Selected().err == nil {
						attemptCancel()
						if self.log.V(2).Enabled() {
							self.log.Infof("[net][p]select: %s\n", dialer.String())
						}
						return result
					}
					if self.log.V(2).Enabled() {
						self.log.Infof("[net][p]select: %s = %s\n", dialer.String(), result.err)
					}
					result.releaseAfterUse(attemptCtx)
				}
				attemptErr := attemptCtx.Err()
				attemptCancel()
				if attemptErr != nil && handleCtx.Err() == nil {
					// eval ignores errors caused by its context because parallel
					// losers share that signal. This private attempt deadline is
					// different: it is evidence that this route black-holed.
					dialer.Update(handleCtx, attemptErr)
				}
			}

			// note parallel dialers is in the original weighted order
			// WeightedShuffle(parallelDialers, dialerWeights)
			n := min(len(parallelDialers), self.settings.ParallelBlockSize)
			p += n
			for _, dialer := range parallelDialers[0:n] {
				startWorker(func() {
					run(dialer)
				})
			}
			for _, dialer := range parallelDialers[n:] {
				select {
				case <-handleCtx.Done():
					return nil
				case result := <-out:
					if result != nil {
						if result.Selected().err == nil {
							if self.log.V(2).Enabled() {
								self.log.Infof("[net][p]select: %s\n", result.dialer.String())
							}
							return result
						}
						if self.log.V(2).Enabled() {
							self.log.Infof("[net][p]select: %s = %s\n", result.dialer.String(), result.err)
						}
						result.releaseAfterUse(handleCtx)
					}
					startWorker(func() {
						run(dialer)
					})
				}
			}
		}

		if expandedDialers := self.expandExtenderDialers(); 0 < len(expandedDialers) {
			n := min(len(expandedDialers), self.settings.ParallelBlockSize-p)
			p += n
			for _, dialer := range expandedDialers[0:n] {
				startWorker(func() {
					run(dialer)
				})
			}
			for _, dialer := range expandedDialers[n:] {
				select {
				case <-handleCtx.Done():
					return nil
				case result := <-out:
					if result != nil {
						if result.Selected().err == nil {
							if self.log.V(2).Enabled() {
								self.log.Infof("[net][p]select: %s\n", result.dialer.String())
							}
							return result
						}
						if self.log.V(2).Enabled() {
							self.log.Infof("[net][p]select: %s = %s\n", result.dialer.String(), result.err)
						}
						result.releaseAfterUse(handleCtx)
					}
					startWorker(func() {
						run(dialer)
					})
				}
			}
		}

		for range p {
			select {
			case <-handleCtx.Done():
				return nil
			case result := <-out:
				if result != nil {
					if result.Selected().err == nil {
						return result
					}
					result.releaseAfterUse(handleCtx)
				}
			}
		}

		// the rate limit is important when when the connect timeout is small
		// e.g. local closes due to disconnected network
		select {
		case <-handleCtx.Done():
			return nil
		case <-reconnect.After():
		}
	}

}

func (self *ClientStrategy) serialEval(ctx context.Context, eval func(ctx context.Context, dialer *clientDialer) *evalResult, helloEval func(ctx context.Context, dialer *clientDialer) *evalResult) *evalResult {
	handleCtx, handleCancel := context.WithTimeout(ctx, self.settings.RequestTimeout)
	// The strategy-context bridge is function-owned; join it so even a fast
	// successful serial result leaves no callback racing the caller's cleanup.
	var contextWaitGroup sync.WaitGroup
	defer func() {
		handleCancel()
		contextWaitGroup.Wait()
	}()
	// merge handleCtx with self.ctx
	contextWaitGroup.Add(1)
	go func() {
		defer contextWaitGroup.Done()
		HandleError(func() {
			defer handleCancel()
			select {
			case <-handleCtx.Done():
				return
			case <-self.ctx.Done():
				return
			}
		}, handleCancel)
	}()

	// keep trying as long as there is time left
	for {
		select {
		case <-handleCtx.Done():
			return nil
		default:
		}

		self.collapseExtenderDialers()

		dialerWeights := self.dialerWeights(false)

		serialDialers := []*clientDialer{}

		for dialer, _ := range dialerWeights {
			if dialer.IsLastSuccess() {
				serialDialers = append(serialDialers, dialer)
			}
		}

		slices.SortStableFunc(serialDialers, func(a *clientDialer, b *clientDialer) int {
			return a.priority - b.priority
		})
		for i, dialer := range serialDialers {
			select {
			case <-handleCtx.Done():
				return nil
			default:
			}

			attemptCtx, attemptCancel := preferredEvalAttemptContext(
				handleCtx,
				len(serialDialers)-i,
			)
			result := eval(attemptCtx, dialer)
			if result != nil {
				result.dialer = dialer
				if result.Selected().err == nil {
					attemptCancel()
					if self.log.V(2).Enabled() {
						self.log.Infof("[net][s]select: %s\n", dialer.String())
					}
					return result
				}
				if self.log.V(2).Enabled() {
					self.log.Infof("[net][s]select: %s = %s\n", dialer.String(), result.err)
				}
				result.releaseAfterUse(attemptCtx)
			}
			attemptErr := attemptCtx.Err()
			attemptCancel()
			if attemptErr != nil && handleCtx.Err() == nil {
				// See parallelEval: a private attempt timeout is a route
				// failure, not cancellation of the caller's request.
				dialer.Update(handleCtx, attemptErr)
			}
		}

		// it's more efficient to iterate with a parallel hello
		// keep retrying hello until at least one dialer is success
		for {
			helloStartTime := time.Now()
			result := self.parallelEval(handleCtx, false, helloEval)
			if result != nil {
				// The nested evaluation cancels its selected attempt before
				// returning. Let net/http own that response cleanup rather
				// than synchronously closing an HTTP/2 body after cancellation.
				result.releaseAfterUse(handleCtx)
			}
			helloEndTime := time.Now()

			// check if any dialer succeeded
			successCount := 0
			for dialer, _ := range self.dialerWeights(false) {
				if dialer.IsLastSuccess() {
					successCount += 1
				}
			}
			if 0 < successCount {
				break
			}

			timeout := self.settings.HelloRetryTimeout - helloEndTime.Sub(helloStartTime)
			if 0 < timeout {
				select {
				case <-handleCtx.Done():
					return nil
				case <-time.After(timeout):
				}
			} else {
				select {
				case <-handleCtx.Done():
					return nil
				default:
				}
			}
		}
		// if result.err != nil {
		// 	return &evalResult{
		// 		err: result.err,
		// 	}
		// }
	}

}

// applyExtraHeaders sets the strategy's ExtraHeaders on h (override semantics)
func (self *ClientStrategy) applyExtraHeaders(h http.Header) {
	for name, values := range self.settings.ExtraHeaders {
		h.Del(name)
		for _, value := range values {
			h.Add(name, value)
		}
	}
}

// Multi-route evaluation requires an independent body reader for every
// attempt. Clone copies request metadata; GetBody resets consumed content.
func cloneHttpRequestForAttempt(ctx context.Context, request *http.Request) (*http.Request, error) {
	attemptRequest := request.Clone(ctx)
	if request.Body == nil {
		return attemptRequest, nil
	}
	if request.GetBody == nil {
		return nil, fmt.Errorf("http request body is not replayable")
	}
	attemptBody, err := request.GetBody()
	if err != nil {
		return nil, fmt.Errorf("rebuild http request body: %w", err)
	}
	attemptRequest.Body = attemptBody
	return attemptRequest, nil
}

// Rejects a body that cannot be rebuilt before any route consumes it.
func validateHttpRequestForAttempts(request *http.Request) error {
	if request == nil {
		return fmt.Errorf("http request is nil")
	}
	if request.Body != nil && request.GetBody == nil {
		return fmt.Errorf("http request body is not replayable")
	}
	return nil
}

func (self *ClientStrategy) HttpParallel(request *http.Request) (*httpResult, error) {
	if err := validateHttpRequestForAttempts(request); err != nil {
		return nil, err
	}
	if request.Body != nil {
		defer request.Body.Close()
	}
	self.applyExtraHeaders(request.Header)

	// js/wasm: one fetch, no dialer strategies (net_http_platform_js.go)
	if result, ok := self.httpPlatformDirect(request); ok {
		if result == nil {
			return nil, fmt.Errorf("http request failed")
		}
		return result, nil
	}

	eval := func(handleCtx context.Context, dialer *clientDialer) *evalResult {
		attemptRequest, err := cloneHttpRequestForAttempt(handleCtx, request)
		if err != nil {
			return &evalResult{err: err}
		}
		httpClient := dialer.HttpClient()
		response, err := httpClient.Do(attemptRequest)
		if self.log.V(2).Enabled() {
			if err != nil {
				self.log.Infof("[net]http parallel %s %s = %s\n", request.Method, request.URL, err)
			} else {
				self.log.Infof("[net]http parallel %s %s = %s\n", request.Method, request.URL, response.Status)
			}
		}

		dialer.Update(handleCtx, err)

		return newEvalResultFromHttpResponse(response, err, self.settings.MaxHttpResponseBodyBytes)
	}

	result := self.parallelEval(request.Context(), false, eval)
	if result == nil {
		return nil, fmt.Errorf("Timeout.")
	}
	return materializeHttpResult(result)
}

func (self *ClientStrategy) HttpSerial(request *http.Request, helloRequest *http.Request) (*httpResult, error) {
	// in this order:
	// 1. try all dialers that previously worked sequentially
	// 2. retest and expand dialers using get of the hello request.
	//    This is a basic ping to the server, which is run in parallel.
	// 3. continue from 1 until timeout
	if err := validateHttpRequestForAttempts(request); err != nil {
		return nil, err
	}
	if err := validateHttpRequestForAttempts(helloRequest); err != nil {
		return nil, err
	}
	if request.Body != nil {
		defer request.Body.Close()
	}
	if helloRequest.Body != nil {
		defer helloRequest.Body.Close()
	}

	self.applyExtraHeaders(request.Header)
	self.applyExtraHeaders(helloRequest.Header)

	// js/wasm: one fetch, no dialer strategies (net_http_platform_js.go)
	if result, ok := self.httpPlatformDirect(request); ok {
		if result == nil {
			return nil, fmt.Errorf("http request failed")
		}
		return result, nil
	}

	eval := func(handleCtx context.Context, dialer *clientDialer) *evalResult {
		attemptRequest, err := cloneHttpRequestForAttempt(handleCtx, request)
		if err != nil {
			return &evalResult{err: err}
		}
		httpClient := dialer.HttpClient()
		response, err := httpClient.Do(attemptRequest)
		if self.log.V(2).Enabled() {
			if err != nil {
				self.log.Infof("[net]http serial %s %s = %s\n", request.Method, request.URL, err)
			} else {
				self.log.Infof("[net]http serial %s %s = %s\n", request.Method, request.URL, response.Status)
			}
		}

		dialer.Update(handleCtx, err)

		return newEvalResultFromHttpResponse(response, err, self.settings.MaxHttpResponseBodyBytes)
	}
	helloEval := func(handleCtx context.Context, dialer *clientDialer) *evalResult {
		attemptRequest, err := cloneHttpRequestForAttempt(handleCtx, helloRequest)
		if err != nil {
			return &evalResult{err: err}
		}
		httpClient := dialer.HttpClient()
		response, err := httpClient.Do(attemptRequest)
		if self.log.V(2).Enabled() {
			if err != nil {
				self.log.Infof("[net]http serial hello %s %s = %s\n", helloRequest.Method, helloRequest.URL, err)
			} else {
				self.log.Infof("[net]http serial hello %s %s = %s\n", helloRequest.Method, helloRequest.URL, response.Status)
			}
		}

		dialer.Update(handleCtx, err)

		return newEvalResultFromHttpResponse(response, err, self.settings.MaxHttpResponseBodyBytes)
	}

	result := self.serialEval(request.Context(), eval, helloEval)
	if result == nil {
		return nil, fmt.Errorf("Timeout.")
	}
	return materializeHttpResult(result)
}

// DialerInfo describes the dialer a completed dial was won by (K1). The
// extender ip is the zero value for a direct dial, which is what the platform
// transport reads to decide whether the connection is carried by an extender.
type DialerInfo struct {
	Description string
	ExtenderIp  netip.Addr
}

// WsDialContext dials without reporting the winning dialer, which is every
// caller that does not draw the extender on a connection.
func (self *ClientStrategy) WsDialContext(ctx context.Context, url string, requestHeader http.Header) (*websocket.Conn, *http.Response, error) {
	wsConn, response, _, err := self.WsDialContextWithDialer(ctx, url, requestHeader)
	return wsConn, response, err
}

// WsDialContextWithDialer is the same dial, plus the description and extender
// address of the dialer that won it (K1). The info is nil only when no dialer
// completed at all, which is the same condition that yields the timeout error.
func (self *ClientStrategy) WsDialContextWithDialer(ctx context.Context, url string, requestHeader http.Header) (*websocket.Conn, *http.Response, *DialerInfo, error) {
	if 0 < len(self.settings.ExtraHeaders) {
		// clone so a caller-held header is not mutated across reconnects
		merged := requestHeader.Clone()
		if merged == nil {
			merged = http.Header{}
		}
		self.applyExtraHeaders(merged)
		requestHeader = merged
	}

	eval := func(handleCtx context.Context, dialer *clientDialer) *evalResult {
		wsDialer := dialer.WsDialer(self.settings)
		wsConn, response, err := wsDialer.DialContext(handleCtx, url, requestHeader)
		if self.log.V(2).Enabled() {
			if err != nil {
				self.log.Infof("[net]ws dial %s = %s\n", url, err)
			} else {
				self.log.Infof("[net]ws dial %s = %s\n", url, response.Status)
			}
		}

		dialer.Update(handleCtx, err)

		return &evalResult{
			wsConn: wsConn,
			err:    err,
			httpResult: httpResult{
				// status: response.Status,
				// statusCode: response.StatusCode,
				// header: response.Header.Clone(),
				response: response,
			},
		}
	}

	result := self.parallelEval(ctx, true, eval)
	if result == nil {
		return nil, nil, nil, fmt.Errorf("Timeout.")
	}
	return result.wsConn, result.response, result.dialer.Info(), result.err
}

func (self *ClientStrategy) collapseExtenderDialers() {
	now := time.Now()
	// candidate drops, and the extender dialers the directory still has to
	// judge. The directory is an external object, so it is consulted with no
	// lock held and the result applied in a second pass.
	dropDialers := []*clientDialer{}
	judgeDialers := []*clientDialer{}
	func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		// a manual extender is not in the directory, so its dialers are judged
		// by the drop timeout alone
		judge := self.settings.ExtenderDirectory != nil && len(self.extenderIpSecrets) == 0
		for dialer, _ := range self.dialers {
			if dialer.extenderConfig == nil || dialer.persistent {
				continue
			}
			expired := func() bool {
				dialer.mutex.Lock()
				defer dialer.mutex.Unlock()

				if dialer.isLastSuccessWithLock() {
					return false
				}
				// the timeout runs from the later of creation and the last
				// error (E2). Judging it from the last error alone drops a
				// dialer that was expanded and never dialed on the very next
				// collapse, because its zero last error is older than any
				// timeout
				since := dialer.createTime
				if since.Before(dialer.lastErrorTime) {
					since = dialer.lastErrorTime
				}
				return self.settings.ExtenderDropTimeout <= now.Sub(since)
			}()
			if expired {
				dropDialers = append(dropDialers, dialer)
			} else if judge {
				judgeDialers = append(judgeDialers, dialer)
			}
		}
	}()

	if directory := self.settings.ExtenderDirectory; directory != nil {
		for _, dialer := range judgeDialers {
			// the address was held, revoked, expired or removed (E2)
			if !directory.AddressUsable(dialer.extenderConfig.Ip) {
				dropDialers = append(dropDialers, dialer)
			}
		}
	}

	if len(dropDialers) == 0 {
		return
	}
	func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		for _, dialer := range dropDialers {
			if _, ok := self.dialers[dialer]; !ok {
				continue
			}
			dialer.Close()
			delete(self.dialers, dialer)
		}
	}()
}

// expandExtenderDialers adds one dialer per directory candidate address and
// carrier (E2). A manually configured extender replaces discovery entirely:
// its addresses are dialed on the fixed carrier ports, and `dialerWeights`
// already excludes every non-extender dialer while one is configured.
//
// The outer name is one random spoof domain per dialer (A10). With no bundled
// spoof list the name is left empty and the dial presents no sni at all: the
// operator name the inner TLS is for must never appear in the outer
// ClientHello, and a connection to an ip literal carries no name anyway.
func (self *ClientStrategy) expandExtenderDialers() (expandedDialers []*clientDialer) {
	if self.settings.ExpandExtenderProfileCount <= 0 {
		return []*clientDialer{}
	}

	visitedExtenderIpProfiles := map[extenderIpProfile]bool{}
	visitedExtenderIps := []netip.Addr{}
	extenderIpSecrets := map[netip.Addr]string{}
	maxNewDialerCount := 0
	func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		visitedExtenderProfileCount := 0
		visitedExtenderProfiles := map[ExtenderProfile]bool{}
		for dialer, _ := range self.dialers {
			if dialer.extenderConfig == nil {
				continue
			}
			visitedExtenderProfiles[dialer.extenderConfig.Profile] = true
			visitedExtenderIpProfiles[extenderIpProfile{
				ip:      dialer.extenderConfig.Ip,
				profile: dialer.extenderConfig.Profile,
			}] = true
			visitedExtenderIps = append(visitedExtenderIps, dialer.extenderConfig.Ip)
		}
		visitedExtenderProfileCount = len(visitedExtenderProfiles)
		maxNewDialerCount = min(
			self.settings.ExpandExtenderProfileCount,
			self.settings.MaxExtenderCount-visitedExtenderProfileCount,
		)
		extenderIpSecrets = maps.Clone(self.extenderIpSecrets)
	}()
	if maxNewDialerCount <= 0 {
		// at maximum extenders
		return []*clientDialer{}
	}

	// the directory is an external object; no lock is held here
	extenderConfigs := []*ExtenderConfig{}
	if 0 < len(extenderIpSecrets) {
		ips := slices.Collect(maps.Keys(extenderIpSecrets))
		slices.SortFunc(ips, func(a netip.Addr, b netip.Addr) int {
			return strings.Compare(a.String(), b.String())
		})
		for _, ip := range ips {
			candidate := &ExtenderCandidate{
				Ip:        ip,
				IpVersion: addressIpVersion(ip),
				Carriers:  []string{ExtenderCarrierTcp, ExtenderCarrierQuic, ExtenderCarrierDns},
				TcpPort:   ExtenderTcpPort,
				UdpPort:   ExtenderQuicPort,
				DnsPort:   ExtenderDnsPort,
				DnsPorts:  []int{ExtenderDnsPort},
				DnsTld:    DefaultExtenderDnsTld,
				Source:    ExtenderSourceManual,
			}
			extenderConfigs = append(
				extenderConfigs,
				extenderConfigsForCandidate(candidate, extenderIpSecrets[ip])...,
			)
		}
	} else if directory := self.settings.ExtenderDirectory; directory != nil {
		familyCandidates := [][]*ExtenderCandidate{}
		for _, ipVersion := range []int{4, 6} {
			if !controlFamilyProbe(ipVersion) {
				continue
			}
			familyCandidates = append(
				familyCandidates,
				directory.Candidates(ipVersion, maxNewDialerCount, visitedExtenderIps...),
			)
		}
		// interleave the families, so a dual-stack host does not spend its
		// whole expand budget on v4 before v6 is ever dialed
		for i := 0; ; i += 1 {
			taken := false
			for _, candidates := range familyCandidates {
				if i < len(candidates) {
					taken = true
					extenderConfigs = append(
						extenderConfigs,
						extenderConfigsForCandidate(candidates[i], "")...,
					)
				}
			}
			if !taken {
				break
			}
		}
	}

	func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		for _, extenderConfig := range extenderConfigs {
			if maxNewDialerCount <= len(expandedDialers) {
				break
			}
			ipProfile := extenderIpProfile{
				ip:      extenderConfig.Ip,
				profile: extenderConfig.Profile,
			}
			if visitedExtenderIpProfiles[ipProfile] {
				continue
			}
			visitedExtenderIpProfiles[ipProfile] = true
			dialer := &clientDialer{
				description:        fmt.Sprintf("extender %s", extenderConfig.Profile.ConnectMode),
				createTime:         time.Now(),
				minimumWeight:      self.settings.ExtenderMinimumWeight,
				priority:           extenderDialerPriority(extenderConfig.Profile.ConnectMode),
				dialTlsContext:     newExtenderDialTlsContext(&self.settings.ConnectSettings, extenderConfig, clientWebSocketNextProtos),
				httpDialTlsContext: newExtenderDialTlsContext(&self.settings.ConnectSettings, extenderConfig, clientHttpNextProtos),
				extenderConfig:     extenderConfig,
				settings:           self.settings,
			}
			expandedDialers = append(expandedDialers, dialer)
			self.dialers[dialer] = true
		}
	}()

	return
}

// One address paired with one carrier profile, which is what makes a dialer
// unique: the same profile on two addresses is two dialers.
type extenderIpProfile struct {
	ip      netip.Addr
	profile ExtenderProfile
}

// The dialer priority of a carrier (E2). tcp is tried before quic before dns,
// which is the order of how ordinary the traffic looks.
func extenderDialerPriority(connectMode ExtenderConnectMode) int {
	switch connectMode {
	case ExtenderConnectModeQuic:
		return 110
	case ExtenderConnectModeDns:
		return 120
	default:
		return 100
	}
}

// One extender config per carrier the candidate lists (E2, E5), and one per
// dns port when the candidate offers several (L2): 53 is dialed before 4053.
// The identity key of a verified record is carried into the config so the
// outer leaf is checked against it (B3).
func extenderConfigsForCandidate(candidate *ExtenderCandidate, secret string) []*ExtenderConfig {
	spoofDomains := SpoofDomains()
	extenderConfigs := []*ExtenderConfig{}
	appendConfig := func(profile ExtenderProfile) {
		if 0 < len(spoofDomains) {
			profile.ServerName = spoofDomains[mathrand.Intn(len(spoofDomains))]
		}
		if profile.Port <= 0 {
			return
		}
		extenderConfigs = append(extenderConfigs, &ExtenderConfig{
			Profile:   profile,
			Ip:        candidate.Ip,
			Secret:    secret,
			PublicKey: slices.Clone(candidate.PublicKey),
		})
	}
	for _, carrier := range candidate.Carriers {
		connectMode, ok := ExtenderConnectModeForCarrier(carrier)
		if !ok {
			continue
		}
		profile := ExtenderProfile{
			ConnectMode: connectMode,
		}
		switch connectMode {
		case ExtenderConnectModeQuic:
			profile.Port = candidate.UdpPort
			appendConfig(profile)
		case ExtenderConnectModeDns:
			profile.DnsTld = candidate.DnsTld
			for _, dnsPort := range candidate.dnsCarrierPorts() {
				profile.Port = dnsPort
				appendConfig(profile)
			}
		default:
			profile.Port = candidate.TcpPort
			// fragment and reorder apply to tcp only
			profile.Fragment = mathrand.Intn(2) != 0
			profile.Reorder = mathrand.Intn(2) != 0
			appendConfig(profile)
		}
	}
	return extenderConfigs
}

// waitForExtenderInitialSample is the E4 startup gate. It waits only while the
// directory has nothing usable AND a network client is still on its first
// attempt, for at most ExtenderInitialSampleTimeout. A stored directory, a
// completed first attempt, or no network client at all never waits: the gate
// exists so a cold start does not dial without the extenders it is about to
// learn, not to delay anything else.
func (self *ClientStrategy) waitForExtenderInitialSample(ctx context.Context) {
	directory := self.settings.ExtenderDirectory
	if directory == nil || self.settings.ExtenderInitialSampleTimeout <= 0 {
		return
	}
	if 0 < len(self.CustomExtenders()) {
		return
	}
	timeout := time.After(self.settings.ExtenderInitialSampleTimeout)
	for {
		state, update := directory.InitialSampleMonitor().Get()
		if state != ExtenderInitialSamplePending {
			return
		}
		if 0 < directory.UsableCount(0) {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-self.ctx.Done():
			return
		case <-update:
		case <-timeout:
			return
		}
	}
}

// non-extender dialers are never dropped
type clientDialer struct {
	description string
	// Set at construction and never changed, so it is read without the mutex.
	// The drop timeout is judged from the later of this and lastErrorTime, so a
	// dialer that was expanded but never dialed survives to its first attempt
	// (E2).
	createTime time.Time
	// persistent dialers were supplied explicitly and are not discovery cache.
	persistent    bool
	minimumWeight float32
	// 0 is max
	priority int

	// WebSocket upgrades require HTTP/1.1, while ordinary API requests can
	// negotiate HTTP/2. Keep protocol-specific TLS dialers so enabling h2 for
	// the API cannot make gorilla/websocket receive an h2 connection it cannot
	// speak.
	dialTlsContext     DialTlsContextFunction
	httpDialTlsContext DialTlsContextFunction
	// httpClientFactory, when set, builds the api client of this dialer
	// instead of the default http.Transport over httpDialTlsContext. The alt
	// dialers use it for an http3 round tripper (L4). A dialer that carries
	// one has no dialTlsContext, so it is api only.
	httpClientFactory func() *http.Client

	extenderConfig *ExtenderConfig

	mutex           sync.Mutex
	successCount    uint64
	errorCount      uint64
	lastSuccessTime time.Time
	lastErrorTime   time.Time

	httpClient      *http.Client
	websocketDialer *websocket.Dialer

	settings *ClientStrategySettings
}

// nativeHttp2Config applies the socket-progress invariant shared by every
// native net/http HTTP/2 client. WriteByteTimeout renews whenever bytes move,
// so ConnectTimeout bounds a stalled write without limiting a healthy request.
func nativeHttp2Config(settings *ConnectSettings) *http.HTTP2Config {
	return &http.HTTP2Config{
		WriteByteTimeout: settings.ConnectTimeout,
	}
}

// Info is the dialer as a completed dial reports it (K1). A nil dialer -- no
// dial completed -- reports nothing rather than panicking, so a caller can pass
// the result of a failed eval straight through.
func (self *clientDialer) Info() *DialerInfo {
	if self == nil {
		return nil
	}
	info := &DialerInfo{
		Description: self.description,
	}
	if self.extenderConfig != nil {
		info.ExtenderIp = self.extenderConfig.Ip.Unmap()
	}
	return info
}

func (self *clientDialer) HttpClient() *http.Client {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.httpClient == nil {
		if self.httpClientFactory != nil {
			self.httpClient = self.httpClientFactory()
			return self.httpClient
		}
		dialTlsContext := self.httpDialTlsContext
		if dialTlsContext == nil {
			dialTlsContext = self.dialTlsContext
		}
		// control-dial evidence: while an egress interface is forced (the
		// windows service/app providing a tunnel), each api dial logs its
		// local bind address so tester logs prove the escape path. See
		// egress_dial.go; a no-op everywhere else.
		dialTlsContext = wrapControlDial("api", self.settings.ConnectSettings.Log, true, dialTlsContext)
		transport := &http.Transport{
			DialTLSContext:        dialTlsContext,
			IdleConnTimeout:       self.settings.ConnectSettings.IdleConnTimeout,
			TLSHandshakeTimeout:   self.settings.ConnectSettings.TlsTimeout,
			ResponseHeaderTimeout: self.settings.ConnectTimeout,
			ExpectContinueTimeout: self.settings.ConnectTimeout,
			DisableKeepAlives:     false,
			ReadBufferSize:        self.settings.HttpReadBufferSize,
			WriteBufferSize:       self.settings.HttpWriteBufferSize,
			// A custom DialTLSContext disables net/http's automatic HTTP/2
			// attempt unless this is set. ConnectControl and peer-key requests
			// arrive in parallel while a provider window forms; keeping the
			// implicit HTTP/1.1 fallback opened one TLS connection per request,
			// repeatedly paying the pinned P-384 certificate-chain verification
			// and creating visible CPU/pause bursts on mobile. HTTP/2
			// multiplexes those requests over the established connection.
			ForceAttemptHTTP2: true,
		}
		// A control-plane connection that went dark AFTER connecting is
		// invisible to any dial-time policy: http/2 multiplexes every later
		// request onto it and each one hangs to the request timeout, for as
		// long as the idle pool holds it. Go performs NO health check when
		// SendPingTimeout is zero (net/http.HTTP2Config: "If zero, no health
		// check is performed"), which is what leaves that connection in the
		// pool. The ping is what turns a silent pool poisoning into an
		// eviction.
		//
		// Built unconditionally. The memory-budget fields below are set only
		// when an embedder asked for them -- that guard is about the mobile
		// heap -- but it used to gate the whole config, so a desktop build had
		// no HTTP2Config at all and therefore no health check either.
		transport.HTTP2 = nativeHttp2Config(&self.settings.ConnectSettings)
		transport.HTTP2.SendPingTimeout = self.settings.Http2SendPingTimeout
		transport.HTTP2.PingTimeout = self.settings.Http2PingTimeout
		if 0 < self.settings.Http2MaxDecoderHeaderTableSize ||
			0 < self.settings.Http2MaxEncoderHeaderTableSize ||
			0 < self.settings.Http2MaxReceiveBufferPerConnection ||
			0 < self.settings.Http2MaxReceiveBufferPerStream {
			transport.HTTP2.MaxDecoderHeaderTableSize = self.settings.Http2MaxDecoderHeaderTableSize
			transport.HTTP2.MaxEncoderHeaderTableSize = self.settings.Http2MaxEncoderHeaderTableSize
			transport.HTTP2.MaxReceiveBufferPerConnection = self.settings.Http2MaxReceiveBufferPerConnection
			transport.HTTP2.MaxReceiveBufferPerStream = self.settings.Http2MaxReceiveBufferPerStream
		}
		// Plain http:// has no tls dialer through which to reach ConnectSettings.
		// Always install its DialContext seam so an explicit caller dial remains
		// authoritative and, without one, the configured resolver, family policy,
		// proxy, and address race still apply. https:// uses the dialTlsContext
		// chain above, which reaches the same ConnectSettings.DialContext boundary.
		transport.DialContext = self.settings.ConnectSettings.DialContext
		self.httpClient = &http.Client{
			Transport: transport,
			Timeout:   self.settings.RequestTimeout,
		}
	}
	return self.httpClient
}

func (self *clientDialer) WsDialer(settings *ClientStrategySettings) *websocket.Dialer {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.websocketDialer == nil {
		var netDialTlsContext DialTlsContextFunction
		if self.dialTlsContext != nil {
			// control-dial evidence for the platform transport dials, same as
			// HttpClient's api tag. See egress_dial.go.
			dialTlsContext := wrapControlDial("platform", settings.ConnectSettings.Log, true, self.dialTlsContext)
			netDialTlsContext = func(
				ctx context.Context,
				network string,
				address string,
			) (net.Conn, error) {
				conn, err := dialTlsContext(ctx, network, address)
				if err != nil {
					return nil, err
				}
				return NewWebSocketWriteBatchConn(conn), nil
			}
		}
		// pool, size := MessagePool(2048)
		self.websocketDialer = &websocket.Dialer{
			NetDialTLSContext: netDialTlsContext,
			HandshakeTimeout:  settings.HandshakeTimeout,
			ReadBufferSize:    settings.WebSocketReadBufferSize,
			WriteBufferSize:   settings.WebSocketWriteBufferSize,
			// WriteBufferPool: pool,
			EnableCompression: false,
		}
		// Plain ws:// has no tls dialer through which to reach ConnectSettings.
		// Always install its DialContext seam so an explicit caller dial remains
		// authoritative and, without one, the configured resolver, family policy,
		// proxy, and address race still apply. wss:// uses the dialTlsContext chain
		// above, which reaches the same ConnectSettings.DialContext boundary.
		self.websocketDialer.NetDialContext = func(
			ctx context.Context,
			network string,
			address string,
		) (net.Conn, error) {
			conn, err := settings.ConnectSettings.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			return NewWebSocketWriteBatchConn(conn), nil
		}
	}
	return self.websocketDialer
}

func (self *clientDialer) Weight() float32 {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	c := self.successCount + self.errorCount
	if 0 < c {
		return max(float32(float64(self.successCount)/float64(c)), self.minimumWeight)
	} else {
		return self.minimumWeight
	}
}

// Update records one completed dial outcome. An extender dialer also reports
// the outcome to the directory, which is what drives the hold, warning and
// removal policy of E1. The directory is an external object, so it is called
// with no lock held.
func (self *clientDialer) Update(handleCtx context.Context, err error) {
	recorded := false
	func() {
		self.mutex.Lock()
		defer self.mutex.Unlock()

		if err == nil {
			self.successCount += 1
			self.lastSuccessTime = time.Now()
			recorded = true
		} else {
			select {
			case <-handleCtx.Done():
				// ignore any error is the context is canceled
			default:
				self.errorCount += 1
				self.lastErrorTime = time.Now()
				recorded = true
			}
		}
	}()
	if !recorded || self.extenderConfig == nil || self.settings == nil {
		return
	}
	directory := self.settings.ExtenderDirectory
	if directory == nil {
		return
	}
	if err == nil {
		directory.RecordSuccess(self.extenderConfig.Ip, self.extenderConfig.Profile.ConnectMode)
	} else {
		directory.RecordFailure(self.extenderConfig.Ip, self.extenderConfig.Profile.ConnectMode)
	}
}

// Reports whether this dialer can carry a websocket dial. An api-only dialer
// -- the alt dialers of L4 -- has no tls dial context at all, and dialing
// through gorilla's own default instead would silently bypass the whole
// strategy. Constant after construction.
func (self *clientDialer) supportsWebSocket() bool {
	return self.dialTlsContext != nil
}

func (self *clientDialer) IsExtender() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return self.extenderConfig != nil
}

// Reports whether the latest completed outcome succeeded. The caller holds
// mutex.
func (self *clientDialer) isLastSuccessWithLock() bool {
	return 0 < self.successCount && !self.lastSuccessTime.Before(self.lastErrorTime)
}

func (self *clientDialer) IsLastSuccess() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return self.isLastSuccessWithLock()
}

func (self *clientDialer) String() string {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.extenderConfig != nil {
		return fmt.Sprintf("extender (%v) success=%d error=%d", self.extenderConfig, self.successCount, self.errorCount)
	} else {
		return fmt.Sprintf("%s success=%d error=%d", self.description, self.successCount, self.errorCount)
	}
}

func (self *clientDialer) Close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.httpClient != nil {
		self.httpClient.CloseIdleConnections()
		self.httpClient = nil
	}
}

type ApiCallback[R any] interface {
	Result(result R, err error)
}

// for internal use
type simpleApiCallback[R any] struct {
	callback func(result R, err error)
}

func NewApiCallback[R any](callback func(result R, err error)) ApiCallback[R] {
	return &simpleApiCallback[R]{
		callback: callback,
	}
}

func NewNoopApiCallback[R any]() ApiCallback[R] {
	return &simpleApiCallback[R]{
		callback: func(result R, err error) {},
	}
}

func (self *simpleApiCallback[R]) Result(result R, err error) {
	self.callback(result, err)
}

type ApiCallbackResult[R any] struct {
	Result R
	Error  error
}

func NewBlockingApiCallback[R any](ctx context.Context) (ApiCallback[R], chan ApiCallbackResult[R]) {
	c := make(chan ApiCallbackResult[R])
	apiCallback := NewApiCallback[R](func(result R, err error) {
		r := ApiCallbackResult[R]{
			Result: result,
			Error:  err,
		}
		select {
		case <-ctx.Done():
		case c <- r:
		}
	})
	return apiCallback, c
}

func HttpPostWithStrategyRaw(
	ctx context.Context,
	clientStrategy *ClientStrategy,
	requestUrl string,
	requestBodyBytes []byte,
	byJwt string,
) ([]byte, error) {
	request, err := http.NewRequestWithContext(
		ctx,
		"POST",
		requestUrl,
		bytes.NewReader(requestBodyBytes),
	)
	if err != nil {
		return nil, err
	}

	request.Header.Add("Content-Type", "text/json")

	if byJwt != "" {
		auth := fmt.Sprintf("Bearer %s", byJwt)
		request.Header.Add("Authorization", auth)
	}

	helloRequest, err := HelloRequestFromUrl(ctx, requestUrl, byJwt)
	if err != nil {
		return nil, err
	}

	r, err := clientStrategy.HttpSerial(request, helloRequest)
	if err != nil {
		return nil, err
	}

	if http.StatusOK != r.response.StatusCode {
		// the response body is the error message. Typed so a caller can act on the
		// status -- notably 402, whose body carries x402 payment terms.
		return nil, &HttpStatusError{
			StatusCode: r.response.StatusCode,
			Status:     r.response.Status,
			Body:       r.bodyBytes,
		}
	}

	return r.bodyBytes, nil
}

// HttpStatusError carries a non-200 response so callers can act on the STATUS rather
// than string-matching an error message.
//
// This matters for 402: when a network is over its plan, the server answers 402 and
// the body carries x402 payment terms. An agent needs to see the status and the body
// to sign a payment and retry -- an opaque error string is unusable for that.
//
// Error() renders exactly as the untyped error it replaces, so existing callers that
// only log or string-match are unaffected.
type HttpStatusError struct {
	StatusCode int
	Status     string
	Body       []byte
}

func (self *HttpStatusError) Error() string {
	return fmt.Sprintf("%s: %s", self.Status, strings.TrimSpace(string(self.Body)))
}

// PaymentRequired reports whether the server answered 402 Payment Required. The Body
// holds the x402 payment terms.
func (self *HttpStatusError) PaymentRequired() bool {
	return self.StatusCode == http.StatusPaymentRequired
}

func HttpPostWithStrategy[R any](
	ctx context.Context,
	clientStrategy *ClientStrategy,
	requestUrl string,
	args any,
	byJwt string,
	result R,
	callback ApiCallback[R],
) (R, error) {
	return HttpPostWithRawFunction(
		ctx,
		func(ctx context.Context, requestUrl string, requestBodyBytes []byte, byJwt string) ([]byte, error) {
			return HttpPostWithStrategyRaw(ctx, clientStrategy, requestUrl, requestBodyBytes, byJwt)
		},
		requestUrl,
		args,
		byJwt,
		result,
		callback,
	)
}

func HttpPostWithRawFunction[R any](
	ctx context.Context,
	httpPostRaw HttpPostRawFunction,
	requestUrl string,
	args any,
	byJwt string,
	result R,
	callback ApiCallback[R],
) (R, error) {
	var requestBodyBytes []byte
	if args == nil {
		requestBodyBytes = make([]byte, 0)
	} else {
		var err error
		requestBodyBytes, err = json.Marshal(args)
		if err != nil {
			var empty R
			callback.Result(empty, err)
			return empty, err
		}
	}

	bodyBytes, err := httpPostRaw(ctx, requestUrl, requestBodyBytes, byJwt)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	err = json.Unmarshal(bodyBytes, &result)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	callback.Result(result, nil)
	return result, nil
}

func HelloRequestFromUrl(ctx context.Context, requestUrl string, byJwt string) (*http.Request, error) {
	u, err := url.Parse(requestUrl)
	if err != nil {
		return nil, err
	}
	helloUrl := fmt.Sprintf("%s://%s/hello", u.Scheme, u.Host)

	req, err := http.NewRequestWithContext(ctx, "GET", helloUrl, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("Content-Type", "text/json")

	if byJwt != "" {
		auth := fmt.Sprintf("Bearer %s", byJwt)
		req.Header.Add("Authorization", auth)
	}

	return req, nil
}

func HttpGetWithStrategyRaw(
	ctx context.Context,
	clientStrategy *ClientStrategy,
	requestUrl string,
	byJwt string,
) ([]byte, error) {
	settings := clientStrategy.settings

	newRequest := func() (*http.Request, error) {
		request, err := http.NewRequestWithContext(ctx, "GET", requestUrl, nil)
		if err != nil {
			return nil, err
		}

		request.Header.Add("Content-Type", "text/json")

		if byJwt != "" {
			auth := fmt.Sprintf("Bearer %s", byJwt)
			request.Header.Add("Authorization", auth)
		}
		return request, nil
	}

	var statusError *HttpStatusError
	for attempt := 0; attempt <= max(0, settings.GetRetryCount); attempt += 1 {
		if 0 < attempt {
			// jittered pause before the retry
			retryTimeout := settings.GetRetryMinTimeout
			if settings.GetRetryMinTimeout < settings.GetRetryMaxTimeout {
				retryTimeout += time.Duration(mathrand.Int63n(int64(settings.GetRetryMaxTimeout - settings.GetRetryMinTimeout)))
			}
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(retryTimeout):
			}
		}

		request, err := newRequest()
		if err != nil {
			return nil, err
		}

		r, err := clientStrategy.HttpParallel(request)
		if err != nil {
			return nil, err
		}

		if http.StatusOK == r.response.StatusCode {
			return r.bodyBytes, nil
		}

		// the response body is the error message. Typed so a caller can act on the
		// status -- notably 402, whose body carries x402 payment terms.
		statusError = &HttpStatusError{
			StatusCode: r.response.StatusCode,
			Status:     r.response.Status,
			Body:       r.bodyBytes,
		}
		if !slices.Contains(settings.GetRetryStatusCodes, r.response.StatusCode) {
			return nil, statusError
		}
		// a transient gateway status; pause and retry
	}
	return nil, statusError
}

func HttpGetWithStrategy[R any](
	ctx context.Context,
	clientStrategy *ClientStrategy,
	requestUrl string,
	byJwt string,
	result R,
	callback ApiCallback[R],
) (R, error) {
	return HttpGetWithRawFunction[R](
		ctx,
		func(ctx context.Context, requestUrl string, byJwt string) ([]byte, error) {
			return HttpGetWithStrategyRaw(ctx, clientStrategy, requestUrl, byJwt)
		},
		requestUrl,
		byJwt,
		result,
		callback,
	)
}

func HttpGetWithRawFunction[R any](
	ctx context.Context,
	httpGetRaw HttpGetRawFunction,
	requestUrl string,
	byJwt string,
	result R,
	callback ApiCallback[R],
) (R, error) {
	bodyBytes, err := httpGetRaw(ctx, requestUrl, byJwt)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	err = json.Unmarshal(bodyBytes, &result)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}

	callback.Result(result, nil)
	return result, nil
}

/**
 * Streaming POST
 */
// newHttpPostStreamTransport constructs the native streaming transport with
// the same dial and HTTP/2 progress bounds as other API clients.
func newHttpPostStreamTransport(settings *ConnectSettings) *http.Transport {
	return &http.Transport{
		DialContext:         wrapControlDial("api", settings.Log, true, settings.DialContext),
		TLSClientConfig:     settings.TlsConfig,
		TLSHandshakeTimeout: settings.TlsTimeout,
		ForceAttemptHTTP2:   true,
		HTTP2:               nativeHttp2Config(settings),
	}
}

func HttpPostStreamWithStrategyRaw(
	ctx context.Context,
	requestUrl string,
	body io.Reader,
	byJwt string,
) ([]byte, error) {

	req, err := http.NewRequestWithContext(ctx, "POST", requestUrl, body)
	if err != nil {
		return nil, err
	}

	if byJwt != "" {
		req.Header.Set("Authorization", "Bearer "+byJwt)
	}

	// NOT http.DefaultClient: on the machine that provides a tunnel, the
	// default transport's unbound sockets and OS name resolution both follow
	// the tun default route into this process's own tunnel (R1). Dial through
	// the connect settings so the socket is egress-bound and the hostname
	// resolves in-process. See egress.go / egress_dial.go; identical behavior
	// everywhere else.
	settings := DefaultConnectSettings()
	var transport http.RoundTripper = newHttpPostStreamTransport(settings)
	if direct := platformDirectHttpTransport(); direct != nil {
		// js/wasm: the browser's fetch, which no custom dialer can reach
		transport = direct
	}
	client := &http.Client{Transport: transport}
	defer client.CloseIdleConnections()
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer releaseHttpResponseBody(ctx, res)
	bodyBytes, err := readHttpResponseBody(res, DefaultMaxHttpResponseBodyBytes)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		// typed so a caller can act on the status -- notably 402, whose body carries
		// x402 payment terms
		return nil, &HttpStatusError{
			StatusCode: res.StatusCode,
			Status:     res.Status,
			Body:       bodyBytes,
		}
	}

	return bodyBytes, nil
}

type HttpPostStreamRawFunction func(
	ctx context.Context,
	requestUrl string,
	body io.Reader,
	byJwt string,
) ([]byte, error)

func HttpPostWithStreamFunction[R any](
	ctx context.Context,
	httpPostRaw HttpPostStreamRawFunction,
	requestUrl string,
	body io.Reader,
	byJwt string,
	result R,
	callback ApiCallback[R],
) (R, error) {
	bodyBytes, err := httpPostRaw(ctx, requestUrl, body, byJwt)
	if err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}
	if err := json.Unmarshal(bodyBytes, &result); err != nil {
		var empty R
		callback.Result(empty, err)
		return empty, err
	}
	callback.Result(result, nil)
	return result, nil
}
