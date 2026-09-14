package connect

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	// "runtime/debug"

	"maps"

	"github.com/gorilla/websocket"
	quic "github.com/quic-go/quic-go"

	"github.com/urnetwork/connect/protocol"
)

// note that it is possible to have multiple transports for the same client destination
// e.g. platform, p2p, and a bunch of extenders

// extenders are identified and credited with the platform by ip address
// they forward to a special port, 8443, that whitelists their ip without rate limiting
// when an extender gets an http message from a client, it always connects tcp to connect.bringyour.com:8443
// appends the proxy protocol headers, and then forwards the bytes from the client
// https://docs.nginx.com/nginx/admin-guide/load-balancer/using-proxy-protocol/
// rate limit using $proxy_protocol_addr https://www.nginx.com/blog/rate-limiting-nginx/
// add the source ip as the X-Extender header

// the transport attempts to upgrade from http1 to http3
// versus the h1 transport, h3 is:
// - more cpu efficient.
//   The quic stream does not need to mask/unmask each byte before TLS.
// - better throughput on poor networks.
//   quic optimizes congestion control to better handle poor network conditions.
// However, h3 is not available in all locations due to dpi/filtering.
// When available, it takes precedence over the default transport.

// packet translation mode gives options for how udp packets are formed on the wire
// We include options here that are known to help with availability

// When packet translation is set, the upgrade mode must be h3 only

// 1: initial version
// 2: latency and speed test support
const TransportVersion = 2

// turn this on to be extra careful about returning all messages
// note we don't run this because it's most efficient to let the gc handle some infrequent orphaned messages
const DebugCloseSend = false

// The platform WebSocket writer combines only messages already waiting on its
// bounded route. ACK-sized traffic may drain thirty-two at once; ordinary data
// stops once the batch has reached 12 KiB, so one complete ordinary <=4-KiB H1
// data message fits the existing 16-KiB coalescer. A larger handshake carrier
// may make the bounded wrapper flush its prefix before the batch ends. A
// dedicated ACK lane may consume at most eight ready slots before one ready
// ordinary packet gets a turn. The writer then starts another ACK burst and
// repeats both bursts inside the same nonblocking physical flush. If either
// lane is empty, the other drains without an artificial flush boundary. This
// changes neither storage nor sparse latency and prevents Transfer feedback
// from starving inner TCP ACKs or request data during a sustained download.
const platformWebSocketWriteBatchMaxMessages = 32
const platformWebSocketWriteBatchDrainByteCount = 12 * 1024
const platformWebSocketAckPriorityBurstMaxMessages = 8

func platformWebSocketWriteBatchCanDrain(
	messageCount int,
	messageByteCount int,
) bool {
	return messageCount < platformWebSocketWriteBatchMaxMessages &&
		messageByteCount < platformWebSocketWriteBatchDrainByteCount
}

func platformWebSocketWriteBatchNextReady(
	ackPrioritySend <-chan []byte,
	send <-chan []byte,
	priorityMessageCount int,
) (
	message []byte,
	priority bool,
	sendOpen bool,
	ready bool,
) {
	// Once the ACK quantum is consumed, give already-ready ordinary work the
	// first look. If none exists, keep draining ready ACKs into this same
	// physical write rather than flushing a partial batch. The caller resets
	// priorityMessageCount after an ordinary selection, producing repeated
	// <=8 ACK / 1 ordinary-packet bursts while both sources remain continuously
	// ready. The enclosing writer keeps those bursts in one bulk flush.
	if platformWebSocketAckPriorityBurstMaxMessages <= priorityMessageCount {
		select {
		case message, sendOpen = <-send:
			return message, false, sendOpen, true
		default:
		}
	}
	if ackPrioritySend != nil {
		select {
		case message = <-ackPrioritySend:
			return message, true, true, true
		default:
		}
	}
	select {
	case message, sendOpen = <-send:
		return message, false, sendOpen, true
	default:
		return nil, false, true, false
	}
}

const (
	platformH3WriteBatchMaxMessageCount = 16
	platformH3WriteBatchMaxByteCount    = 64 * 1024
	DefaultDnsPumpHost                  = "whodis.bringyour.com"
)

func transportTypeFromMode(mode TransportMode) TransportType {
	switch mode {
	case TransportModeH3:
		return TransportTypeH3
	case TransportModeH1:
		return TransportTypeH1
	case TransportModeH3Dns:
		return TransportTypeH3Dns
	case TransportModeH3DnsPump:
		return TransportTypeH3DnsPump
	default:
		return TransportTypeUnknown
	}
}

type TransportControl = byte

const (
	TransportControlSpeedStart TransportControl = 1
	TransportControlSpeedStop  TransportControl = 2
)

type TransportMode string

// in order of increasing preference
const (
	// start all modes in skewed parallel and choose the best one
	TransportModeAuto      TransportMode = "auto"
	TransportModeH3DnsPump TransportMode = "h3dnspump"
	TransportModeH3Dns     TransportMode = "h3dns"
	TransportModeH3        TransportMode = "h3"
	TransportModeH1        TransportMode = "h1"
	TransportModeNone      TransportMode = ""
)

// DefaultTransportModePreferences returns the production Auto policy. Lower
// numbers are preferred. H1 is deliberately the primary carrier while H3
// remains a direct fallback for paths where H1 is unavailable. Explicit H3
// selection bypasses this Auto ordering. DNS and DNS pump are progressively
// lower availability fallbacks.
func DefaultTransportModePreferences() map[TransportMode]int {
	return map[TransportMode]int{
		TransportModeH1: 1,
		TransportModeH3: 2,

		TransportModeH3Dns:     3,
		TransportModeH3DnsPump: 4,
	}
}

func normalizeTransportModePreferences(preferences map[TransportMode]int) map[TransportMode]int {
	if preferences == nil {
		return DefaultTransportModePreferences()
	}
	normalized := map[TransportMode]int{}
	for mode, priority := range preferences {
		switch mode {
		case TransportModeH3, TransportModeH1, TransportModeH3Dns, TransportModeH3DnsPump:
			if 0 < priority {
				normalized[mode] = priority
			}
		}
	}
	if len(normalized) == 0 {
		return DefaultTransportModePreferences()
	}
	return normalized
}

// PlatformTransportReceiveModeStatsSnapshot is one lock-free view of complete
// Transfer-frame messages encountering a full platform receive route. Bytes
// are the complete message bytes read from the carrier. QueueDrop is
// application-level loss returned to Transfer recovery; QueueBackpressure is
// reliable-carrier ownership retained while waiting for bounded route space.
type PlatformTransportReceiveModeStatsSnapshot struct {
	QueueDropMessageCount         uint64
	QueueDropByteCount            uint64
	QueueBackpressureMessageCount uint64
	QueueBackpressureByteCount    uint64
}

// PlatformTransportReceiveStatsSnapshot keeps the carrier mode visible: DNS
// translation and direct QUIC can behave very differently on a constrained
// cellular path even though both ultimately use the H3 reader.
type PlatformTransportReceiveStatsSnapshot struct {
	H1                    PlatformTransportReceiveModeStatsSnapshot
	H3                    PlatformTransportReceiveModeStatsSnapshot
	H3Dns                 PlatformTransportReceiveModeStatsSnapshot
	H3DnsPump             PlatformTransportReceiveModeStatsSnapshot
	H1ControlRefusalCount uint64
	H1ControlRefusalBytes uint64
}

type platformTransportReceiveModeStats struct {
	queueDropMessageCount         atomic.Uint64
	queueDropByteCount            atomic.Uint64
	queueBackpressureMessageCount atomic.Uint64
	queueBackpressureByteCount    atomic.Uint64
}

func (self *platformTransportReceiveModeStats) snapshot() PlatformTransportReceiveModeStatsSnapshot {
	return PlatformTransportReceiveModeStatsSnapshot{
		QueueDropMessageCount:         self.queueDropMessageCount.Load(),
		QueueDropByteCount:            self.queueDropByteCount.Load(),
		QueueBackpressureMessageCount: self.queueBackpressureMessageCount.Load(),
		QueueBackpressureByteCount:    self.queueBackpressureByteCount.Load(),
	}
}

// PlatformTransportReceiveStats is shared by every reconnect generation of
// one PlatformTransport. All counters are monotonic and add no receive-path
// lock or observer callback.
type PlatformTransportReceiveStats struct {
	h1                    platformTransportReceiveModeStats
	h3                    platformTransportReceiveModeStats
	h3Dns                 platformTransportReceiveModeStats
	h3DnsPump             platformTransportReceiveModeStats
	h1ControlRefusalCount atomic.Uint64
	h1ControlRefusalBytes atomic.Uint64
}

func (self *PlatformTransportReceiveStats) mode(mode TransportMode) *platformTransportReceiveModeStats {
	if self == nil {
		return nil
	}
	switch mode {
	case TransportModeH1:
		return &self.h1
	case TransportModeH3:
		return &self.h3
	case TransportModeH3Dns:
		return &self.h3Dns
	case TransportModeH3DnsPump:
		return &self.h3DnsPump
	default:
		return nil
	}
}

func (self *PlatformTransportReceiveStats) recordQueueDrop(mode TransportMode, byteCount int) {
	if counters := self.mode(mode); counters != nil {
		counters.queueDropMessageCount.Add(1)
		counters.queueDropByteCount.Add(uint64(max(0, byteCount)))
	}
}

func (self *PlatformTransportReceiveStats) recordQueueBackpressure(
	mode TransportMode,
	byteCount int,
) {
	if counters := self.mode(mode); counters != nil {
		counters.queueBackpressureMessageCount.Add(1)
		counters.queueBackpressureByteCount.Add(uint64(max(0, byteCount)))
	}
}

func (self *PlatformTransportReceiveStats) recordH1ControlRefusal(byteCount int) {
	if self == nil {
		return
	}
	self.h1ControlRefusalCount.Add(1)
	self.h1ControlRefusalBytes.Add(uint64(max(0, byteCount)))
}

func (self *PlatformTransportReceiveStats) Snapshot() PlatformTransportReceiveStatsSnapshot {
	if self == nil {
		return PlatformTransportReceiveStatsSnapshot{}
	}
	return PlatformTransportReceiveStatsSnapshot{
		H1:                    self.h1.snapshot(),
		H3:                    self.h3.snapshot(),
		H3Dns:                 self.h3Dns.snapshot(),
		H3DnsPump:             self.h3DnsPump.snapshot(),
		H1ControlRefusalCount: self.h1ControlRefusalCount.Load(),
		H1ControlRefusalBytes: self.h1ControlRefusalBytes.Load(),
	}
}

type pooledReceiveOfferResult uint8

const (
	pooledReceiveOfferDelivered pooledReceiveOfferResult = iota
	pooledReceiveOfferFull
	pooledReceiveOfferDone
)

// tryOfferPooledReceive is the common carrier-reader handoff. It never waits;
// the caller retains ownership unless delivery succeeds.
func tryOfferPooledReceive(
	done <-chan struct{},
	destination chan<- []byte,
	message []byte,
) pooledReceiveOfferResult {
	select {
	case <-done:
		return pooledReceiveOfferDone
	default:
	}
	select {
	case <-done:
		return pooledReceiveOfferDone
	case destination <- message:
		return pooledReceiveOfferDelivered
	default:
		return pooledReceiveOfferFull
	}
}

type ClientAuth struct {
	ByJwt string
	// ClientId Id
	InstanceId Id
	AppVersion string
}

func (self *ClientAuth) ClientId() (Id, error) {
	byJwt, err := ParseByJwtUnverified(self.ByJwt)
	if err != nil {
		return Id{}, err
	}
	return byJwt.ClientId, nil
}

// Throttles for the log lines that flood during a control-API outage. Each is
// package-level because the flood is across every transport and sequence at
// once, not within one of them — a per-instance limiter would still emit once
// per client per interval, which on a provider with thousands of clients is not
// a limit at all. See logThrottle in log_throttle.go.
var (
	authErrThrottle  = newLogThrottle(time.Minute)
	writeErrThrottle = newLogThrottle(time.Minute)
)

// shouldLogAuthErr reports whether an authentication error should be logged and returns the throttle state.
func shouldLogAuthErr() (bool, int64) { return authErrThrottle.Allow(time.Now()) }

// shouldLogWriteErr reports whether a write error should be logged and returns the associated throttle value.
func shouldLogWriteErr() (bool, int64) { return writeErrThrottle.Allow(time.Now()) }

// lastBackendFailNano is the time of the most recent backend failure (auth or
// contract OOB), in unix nanos. Not throttled — every failure updates it, so
// isBackendDegraded can tell a live outage from a stale count left by an old
// blip on an otherwise idle provider.
var lastBackendFailNano atomic.Int64

// consecutiveBackendFails counts backend failures since the last success. Any
// successful auth or OOB round-trip resets it to 0. A real platform outage
// drives this up quickly because every attempt fails with nothing to reset it;
// isolated transient timeouts never accumulate, because an interleaved success
// clears the count.
//
// This is process-wide rather than per-Client on purpose: "is the control API
// reachable from this host" is a property of the host, not of any one client.
// The known limit of that framing: a process talking to MULTIPLE platform urls
// (separate network spaces) shares one signal across them, so a dead custom
// endpoint can gate a healthy one. Accepted for now -- the fleet and app cases
// run one platform per process -- and keying this state by platform url is the
// upgrade path if that changes.
// A host running many clients gets a stronger signal from sharing the counter,
// because a success on any client clears it — so a single misbehaving client
// cannot trip the threshold on its own, and only a fault broad enough to fail
// every client in a row reads as an outage. That is exactly the distinction
// isBackendDegraded is trying to draw.
var consecutiveBackendFails atomic.Int64

// backendDegradedFailThreshold is how many consecutive backend failures (with
// no intervening success) are required before the backend is treated as
// degraded. Set above the level of normal transient churn so a stray timeout on
// a busy provider is never mistaken for an outage.
const backendDegradedFailThreshold = 3

// backendDegradedWindow is how recent the last failure must be for the counter
// to be trusted. Comfortably larger than the 60s reconnect-backoff cap, so a
// real outage's retries always read as recent.
const backendDegradedWindow = 2 * time.Minute

// isBackendDegraded reports whether backend failures have accumulated past the
// threshold with no intervening success, and the last one is recent. It
// distinguishes a sustained outage (every attempt failing) from the isolated
// single-connection timeouts that are normal churn.
//
// Callers use it to avoid queueing work that cannot complete: with the control
// API unreachable, no client can authorize a contract, so contract creation,
// contract retry pacing, and window expansion are all spending bandwidth on
// data that has nowhere to go.
//
// During an outage where the transport stays connected (the control API down,
// the websocket alive), auth never re-runs and the gated CreateContract is the
// only other success source -- so nothing can SUCCEED to clear the state.
// Recovery then rides the recency window instead: after backendDegradedWindow
// without failures this reads false, the sequences that tick before three
// fresh failures land probe the backend, and either one succeeds (clearing the
// state) or the gate re-trips. The steady state of a long OOB-only outage is
// therefore a bounded probe burst every ~backendDegradedWindow, not a latched
// stop -- which is also what makes recovery need no timer of its own.
func isBackendDegraded() bool {
	if consecutiveBackendFails.Load() < backendDegradedFailThreshold {
		return false
	}
	return time.Now().UnixNano()-lastBackendFailNano.Load() < int64(backendDegradedWindow)
}

// backendFailMu serializes the failure-state transition. Recording a failure is
// a single logical step made of three stores (age out a dead streak, adjust the
// counter, refresh the timestamp); interleaving them could half-clear a stale
// streak and leave it readable as live. Backend failures are rare by definition,
// so serializing them costs nothing, and isBackendDegraded stays lock-free.
var backendFailMu sync.Mutex

// noteBackendFailure records a failed backend round-trip (auth or contract OOB).
//
// A streak older than backendDegradedWindow is discarded rather than extended.
// Without that, an idle provider that saw a few failures long ago and simply
// stopped retrying would carry the old count forward: the next single failure
// would push the total past the threshold with a fresh timestamp, and the
// backend would read as degraded on the strength of one recent failure. The
// threshold means "consecutive failures within the window", so a gap that
// invalidates the streak for isBackendDegraded must also reset it here.
func noteBackendFailure() {
	now := time.Now().UnixNano()

	backendFailMu.Lock()
	defer backendFailMu.Unlock()

	last := lastBackendFailNano.Load()
	if last != 0 && int64(backendDegradedWindow) <= now-last {
		consecutiveBackendFails.Store(1)
	} else {
		consecutiveBackendFails.Add(1)
	}
	lastBackendFailNano.Store(now)
}

// noteBackendSuccess clears the recorded backend failure state after a
// successful auth or OOB round-trip.
// It takes backendFailMu for the same reason noteBackendFailure does: clearing
// the count and the timestamp is one logical transition. Unsynchronized, a
// concurrent failure could land its increment and timestamp between the two
// stores here, leaving a positive count with a zero timestamp — a state
// isBackendDegraded reads as "not degraded" while failures are in fact
// accumulating.
func noteBackendSuccess() {
	backendFailMu.Lock()
	defer backendFailMu.Unlock()

	consecutiveBackendFails.Store(0)
	lastBackendFailNano.Store(0)
}

// (ctx, network, address)
// type DialContextFunc func(ctx context.Context, network string, address string) (net.Conn, error)

type PlatformTransportSettings struct {
	// Log, when set, is used by the platform transport and its framer
	// (used for the framer when `FramerSettings.Log` is nil, via a private
	// copy — the caller's `FramerSettings` is never mutated).
	// nil resolves to `DefaultLogger()`.
	Log Logger

	HttpConnectTimeout   time.Duration
	WsHandshakeTimeout   time.Duration
	QuicConnectTimeout   time.Duration
	QuicHandshakeTimeout time.Duration
	QuicTlsConfig        *tls.Config
	AuthTimeout          time.Duration
	ReconnectTimeout     time.Duration
	PingTimeout          time.Duration
	WriteTimeout         time.Duration
	ReadTimeout          time.Duration
	TransportGenerator   func() (sendTransport Transport, receiveTransport Transport)
	// SendRouteObserver exposes route ownership to deterministic integration
	// harnesses. It must not block. Nil retains normal production behavior.
	SendRouteObserver func(transport Transport, route Route, connected bool)
	// ReceiveStats, when non-nil, aggregates carrier-to-route admission loss and
	// reliable-H1 backpressure across every reconnect generation. The
	// constructor uses a private counter set when it is nil.
	ReceiveStats *PlatformTransportReceiveStats
	// AuthFrameObserver borrows the exact pooled authentication frame before
	// transport I/O. Tests may retain it to prove lifecycle ownership. It must
	// not block; nil retains normal production behavior.
	AuthFrameObserver   func(authFrameBytes []byte)
	TransportBufferSize int
	// H1AckPriorityBufferSize enables a separate bounded writer lane used only
	// by Transfer acknowledgements. Zero (the server/default policy) leaves the
	// lane absent. Mobile embedders can spend a handful of channel slots so ACK
	// feedback cannot sit behind a full bulk route and trigger resend storms.
	H1AckPriorityBufferSize int
	InactiveDrainTimeout    time.Duration
	// InactiveDrainMaxTimeout is the absolute lifetime of a carrier after a
	// strictly better mode supersedes it. Payload activity may extend the quiet
	// drain, but never beyond this bound. A non-positive value uses twice
	// InactiveDrainTimeout.
	InactiveDrainMaxTimeout time.Duration
	// H1MaxMessageByteCount caps each complete WebSocket message before it can
	// grow a pooled buffer. A non-positive value resolves to the framer limit.
	H1MaxMessageByteCount int64
	// PlatformTransportBudget is shared across window transports. Reservations
	// remain held through reconnects so socket churn cannot escape the cap.
	PlatformTransportBudget *PlatformTransportBudget
	// Non-positive carrier/socket values resolve to the memory-scaled defaults.
	H1BudgetByteCount                         ByteCount
	H3BudgetByteCount                         ByteCount
	H3SocketReadBufferByteCount               ByteCount
	H3SocketWriteBufferByteCount              ByteCount
	H3InitialStreamReceiveWindowByteCount     ByteCount
	H3MaxStreamReceiveWindowByteCount         ByteCount
	H3InitialConnectionReceiveWindowByteCount ByteCount
	H3MaxConnectionReceiveWindowByteCount     ByteCount
	// PlatformTransportBudgetPriority orders optional Auto-H3 leases when the
	// aggregate budget cannot hold every caller. Foreground client windows use
	// the zero value; background/provider transports use the background value.
	// Explicit H3 ignores this value and always outranks optional Auto H3.
	PlatformTransportBudgetPriority int
	// it smoothes out the h3 transition to not start/stop h1 if h3 connects in this time
	ModeInitialDelay time.Duration
	// ModePreferences configures the enabled Auto modes and their priorities.
	// Lower values are preferred and every healthy mode tied at the best live
	// priority remains active. Nil selects DefaultTransportModePreferences.
	ModePreferences map[TransportMode]int
	// IpFamily pins the transport to one address family: 4, 6, or 0 for the
	// family-agnostic transport this has always been. A pinned transport
	// dials only its family, declares it to the platform, and never counts
	// its dial failures against the backend. See transport_family.go.
	IpFamily int
	// StartDisabled constructs the transport held (see SetEnabled): it dials
	// nothing until its owner enables it. The provider group's standby uses it.
	StartDisabled bool
	// PinnedReconnectMaxTimeout caps a pinned transport's own exponential
	// dial backoff. Non-positive resolves to five minutes.
	PinnedReconnectMaxTimeout time.Duration

	// MinConnectDelay time.Duration
	// MaxConnectDelay time.Duration

	ProtocolVersion int

	H3Port  int
	DnsPort int
	// DnsPumpHost is only the public UDP/53 destination for H3DnsPump. QUIC
	// authentication and TLS SNI continue to use the platform URL's hostname.
	// Keeping this explicit avoids deriving an infrastructure hostname from the
	// packet codec's canonical TLD representation. Empty derives the pump host
	// from AltUrl instead (L3), since the pump destination is only where the
	// client's own packets go.
	DnsPumpHost string
	// AltUrl, when set, is where the H3, h3dns and h3dnspump carriers send
	// their packets (L4): the alt url's host replaces the platform url's host
	// for the dial only, and a port on it pins the carrier. The sni and the
	// quic authentication stay the platform url's host, which is the connect
	// name alt serves, and H1 keeps the platform url entirely. Empty keeps
	// every carrier on the platform host, which is the behavior of every
	// space with no alt deployment.
	AltUrl string

	// FIXME
	DnsTlds        [][]byte
	V2H1Auth       bool
	FramerSettings *FramerSettings

	PtDnsSlowMultiple int

	// H3PacketConnFactory, when set, creates the UDP endpoint for a plain H3
	// dial. Tests use it to place QUIC below a userspace network model, while
	// headless multi-provider hosts use it to preserve distinct source
	// identities. Nil retains the host UDP socket and physical-egress binding
	// path. The platform transport owns and closes every returned endpoint.
	H3PacketConnFactory func(context.Context) (net.PacketConn, error)
	// Enables the RFC 9221 Transfer carrier only when the server accepts the
	// same version on the authenticated control stream. A legacy peer retains
	// the existing reliable-stream path on this same connection.
	EnableH3Datagrams bool
	// Nil selects conservative bounded defaults. Callers may inject a stats
	// collector to aggregate reconnect generations in a larger measurement.
	H3DatagramSettings *H3DatagramSettings
	H3DatagramStats    *H3DatagramStats
	// H3QuicPacketStats, when non-nil, enables packet-level qlog reduction. The
	// default collector makes PTO and sent-with-zero-response handshakes visible
	// in production and also distinguishes SendDatagram queue admission from
	// actual QUIC DATAGRAM frame emission and application dequeue. Set nil on a
	// custom settings value only when tracing overhead is intentionally disabled.
	H3QuicPacketStats *H3QuicPacketStats
	// Borrows one complete routed Transfer message after its successful H3
	// carrier write. datagram distinguishes the packet lane from the reliable
	// stream lane. The callback must not retain the bytes or block.
	H3SendLaneObserver func(message []byte, datagram bool)

	// ExtenderIpsMonitor, when set, replaces the transport's own change
	// counter for its live extender addresses (K1). An owner that replaces
	// transports across generations -- the window's migration -- supplies one
	// counter so a watcher never has to re-subscribe to a new transport's
	// monitor, and so the departure of the old transport's addresses wakes it.
	// Nil gives the transport a counter of its own.
	ExtenderIpsMonitor *MonitorValue[uint64]

	// Nil outside package tests. A barrier here can hold the exact seam after
	// logical route removal and before connection and writer cleanup.
	afterRoutesRemovedForTest func()
	// Nil outside package tests. A barrier here can hold a receive worker before
	// it releases channel and pooled-message ownership.
	beforeReceiveWorkerCleanupForTest func()
	// Nil outside package tests. A barrier here holds a routed hybrid stream
	// write after lane dispatch so DATAGRAM progress can be verified
	// independently of the reliable writer.
	beforeH3StreamWriteForTest func()
	// Nil outside package tests. The observer borrows one H3 receive message
	// after the channel accepts its ownership.
	afterH3ReceiveEnqueueForTest func([]byte)
	// Nil outside package tests. Replaces one H3 carrier runner so budget lease
	// preemption can hold its exact teardown boundary without opening a socket.
	runH3ModeForTest func(context.Context, TransportMode, time.Duration)
	// Nil outside package tests. Replaces plain-H3 name resolution so the
	// family race can be driven against chosen addresses.
	resolveH3AddrsForTest func(ctx context.Context, address string, ipFamily int) ([]*net.UDPAddr, error)
}

func DefaultPlatformTransportSettings() *PlatformTransportSettings {
	tlsConfig, err := DefaultTlsConfig()
	if err != nil {
		panic(err)
	}
	return &PlatformTransportSettings{
		HttpConnectTimeout:        15 * time.Second,
		WsHandshakeTimeout:        15 * time.Second,
		QuicConnectTimeout:        15 * time.Second,
		QuicHandshakeTimeout:      15 * time.Second,
		QuicTlsConfig:             tlsConfig,
		AuthTimeout:               5 * time.Second,
		ReconnectTimeout:          5 * time.Second,
		PingTimeout:               5 * time.Second,
		WriteTimeout:              10 * time.Second,
		ReadTimeout:               30 * time.Second,
		TransportBufferSize:       32,
		InactiveDrainTimeout:      30 * time.Second,
		InactiveDrainMaxTimeout:   60 * time.Second,
		ModeInitialDelay:          2 * time.Second,
		ModePreferences:           DefaultTransportModePreferences(),
		PinnedReconnectMaxTimeout: 5 * time.Minute,
		// MinConnectDelay:      0,
		// MaxConnectDelay:      1 * time.Second,
		ProtocolVersion: DefaultProtocolVersion,
		H3Port:          443,
		DnsPort:         DefaultDnsPort,
		DnsPumpHost:     DefaultDnsPumpHost,
		// FIXME
		DnsTlds: [][]byte{[]byte("ur.xyz.")},
		// servers are migrated on 2025-06-12. We can remove this and always use true.
		V2H1Auth: true,
		// the platform transport must carry the per-peer encryption handshake,
		// so its framer max is the connect runtime minimum message length
		FramerSettings:                            DefaultFramerSettings(int(DefaultClientSettings().MinimumMessageLenLimit())),
		H1MaxMessageByteCount:                     DefaultClientSettings().MinimumMessageLenLimit(),
		PlatformTransportBudget:                   DefaultPlatformTransportBudget(),
		H1BudgetByteCount:                         MemoryScaledByteCount(kib(512), kib(256)),
		H3BudgetByteCount:                         defaultH3BudgetByteCount(),
		H3SocketReadBufferByteCount:               MemoryScaledByteCount(mib(1), kib(256)),
		H3SocketWriteBufferByteCount:              MemoryScaledByteCount(mib(1), kib(256)),
		H3InitialStreamReceiveWindowByteCount:     kib(256),
		H3MaxStreamReceiveWindowByteCount:         defaultH3MaxStreamReceiveWindowByteCount(),
		H3InitialConnectionReceiveWindowByteCount: kib(512),
		H3MaxConnectionReceiveWindowByteCount:     defaultH3MaxConnectionReceiveWindowByteCount(),
		PtDnsSlowMultiple:                         4,
		EnableH3Datagrams:                         true,
		H3DatagramSettings:                        DefaultH3DatagramSettings(),
		H3QuicPacketStats:                         &H3QuicPacketStats{},
	}
}

// DefaultPlatformTransportSettingsWithMemoryTarget returns platform carrier
// settings whose admission, socket buffers, QUIC receive windows, and
// datagram reassembly state derive from one explicit owner memory target. Each
// call owns a private carrier budget. A nonpositive target retains the legacy
// process-global defaults.
func DefaultPlatformTransportSettingsWithMemoryTarget(
	memoryTargetByteCount ByteCount,
) *PlatformTransportSettings {
	settings := DefaultPlatformTransportSettings()
	if memoryTargetByteCount <= 0 {
		return settings
	}
	settings.PlatformTransportBudget =
		NewPlatformTransportBudgetForMemoryTarget(memoryTargetByteCount)
	settings.H1BudgetByteCount = MemoryTargetScaledByteCount(
		memoryTargetByteCount,
		kib(512),
		kib(256),
	)
	settings.H3BudgetByteCount = h3BudgetByteCountForMemoryTarget(memoryTargetByteCount)
	settings.H3SocketReadBufferByteCount = MemoryTargetScaledByteCount(
		memoryTargetByteCount,
		mib(1),
		kib(256),
	)
	settings.H3SocketWriteBufferByteCount = MemoryTargetScaledByteCount(
		memoryTargetByteCount,
		mib(1),
		kib(256),
	)
	settings.H3MaxStreamReceiveWindowByteCount =
		h3MaxStreamReceiveWindowByteCountForMemoryTarget(memoryTargetByteCount)
	settings.H3MaxConnectionReceiveWindowByteCount =
		h3MaxConnectionReceiveWindowByteCountForMemoryTarget(memoryTargetByteCount)
	if settings.H3DatagramSettings != nil {
		settings.H3DatagramSettings.ProcessReassemblyByteCount = int64(
			MemoryTargetScaledByteCount(
				memoryTargetByteCount,
				mib(8),
				kib(512),
			),
		)
	}
	return settings
}

// The fraction of the memory budget the H3 carrier draws, and the fractions of
// that draw its stream and connection receive windows take.
//
// One eighth, which is 8 MiB at the 64 MiB reference: exactly what the carrier
// reserved there before this was a draw, so no host at or below the reference
// moves. The stream window takes three eighths of the draw and the connection
// window four, the ratio they have always had -- 3 MiB against 4 at the
// reference.
//
// It must never be a memory-scaled constant, and that distinction is the whole
// finding (THROUGHPUTFIX §37.22, §42.1, §44.2 constraint 1). `memoryTargetScale`
// returns one at or above the reference and a fraction below it, so every
// window in this file was sized for a 64 MiB device and could only shrink from
// there: a desktop with a 256 MiB budget ran a 64 MiB device's window, which is
// why no amount of memory has ever made the download path faster.
// `MemoryScaledByteCount` and `MemoryTargetScaledByteCount` share that scale --
// they differ in which budget they read, not in whether they can grow -- so
// neither is a way to write this. Every adjacent line in this file scales a
// constant, so copying one is the natural way to write a share and is the trap;
// `TestTheH3ReceiveWindowsAreADrawOnTheBudget` fails at the 64-to-256 MiB step
// if this is ever rewritten in the local idiom.
const h3BudgetShareDivisor = 8

const (
	h3StreamReceiveWindowShareNumerator     = 3
	h3ConnectionReceiveWindowShareNumerator = 4
	h3ReceiveWindowShareDenominator         = 8
)

// h3BudgetShareByteCount is the carrier's draw on one memory target. It takes
// the target rather than reading the process budget, as
// `transferBudgetShareByteCount` does, because this layer also has a per-owner
// target surface (`DefaultPlatformTransportSettingsWithMemoryTarget`) that
// carries the same defect and needs the same draw.
//
// Zero means there is no budget. That is the absence of the surface rather than
// a small share, and every caller below keeps today's constant in that case:
// falling to a floor would make every unbudgeted process -- which is what a
// provider is (§37.22) -- slower the moment this lands.
func h3BudgetShareByteCount(memoryTargetByteCount ByteCount) ByteCount {
	if memoryTargetByteCount <= 0 {
		return 0
	}
	return memoryTargetByteCount / h3BudgetShareDivisor
}

// h3BudgetByteCountForMemoryTarget is the carrier's reservation against the
// aggregate transport budget: the draw, floored at the working minimum that
// lets one explicitly selected H3 carrier fit the smallest supported host (see
// `newDefaultPlatformTransportBudget`). The draw is an eighth against that
// aggregate's quarter, so one carrier's reservation stays under the aggregate
// at every budget, which is the relationship it has at the reference today.
func h3BudgetByteCountForMemoryTarget(memoryTargetByteCount ByteCount) ByteCount {
	share := h3BudgetShareByteCount(memoryTargetByteCount)
	if share <= 0 {
		return MemoryTargetScaledByteCount(memoryTargetByteCount, mib(8), mib(3))
	}
	return max(mib(3), share)
}

func defaultH3BudgetByteCount() ByteCount {
	return h3BudgetByteCountForMemoryTarget(MemoryBudget())
}

// The receive windows are fractions of the draw itself, each with its own
// working floor, and deliberately not fractions of the floored reservation
// above. The reservation's 3 MiB floor is an admission minimum -- it exists so
// one explicitly selected H3 carrier fits the 8 MiB legacy host's aggregate
// budget -- and inheriting it into the windows would advertise 1.125 MiB of
// stream credit on a host whose whole budget is 8 MiB, and more than the entire
// budget at 1 MiB (§44.2, constraint 3: the floors are the one place the table
// can lie).
//
// Below the reference the fraction of the draw and the scaled constant it
// replaces are the same number by construction, so nothing there moves:
// `MemoryScaledByteCount(mib(3), kib(384))` is `max(384 KiB, 3M/64)`, and three
// eighths of `M/8` is `3M/64`.
func h3MaxStreamReceiveWindowByteCountForMemoryTarget(
	memoryTargetByteCount ByteCount,
) ByteCount {
	share := h3BudgetShareByteCount(memoryTargetByteCount)
	if share <= 0 {
		return MemoryTargetScaledByteCount(memoryTargetByteCount, mib(3), kib(384))
	}
	return max(
		kib(384),
		share*h3StreamReceiveWindowShareNumerator/h3ReceiveWindowShareDenominator,
	)
}

func defaultH3MaxStreamReceiveWindowByteCount() ByteCount {
	return h3MaxStreamReceiveWindowByteCountForMemoryTarget(MemoryBudget())
}

func h3MaxConnectionReceiveWindowByteCountForMemoryTarget(
	memoryTargetByteCount ByteCount,
) ByteCount {
	share := h3BudgetShareByteCount(memoryTargetByteCount)
	if share <= 0 {
		return MemoryTargetScaledByteCount(memoryTargetByteCount, mib(4), kib(512))
	}
	return max(
		kib(512),
		share*h3ConnectionReceiveWindowShareNumerator/h3ReceiveWindowShareDenominator,
	)
}

func defaultH3MaxConnectionReceiveWindowByteCount() ByteCount {
	return h3MaxConnectionReceiveWindowByteCountForMemoryTarget(MemoryBudget())
}

type PlatformTransport struct {
	ctx    context.Context
	cancel context.CancelFunc
	log    Logger
	// done closes after run joins every mode runner and its connection workers.
	done chan struct{}

	// runWaitGroup owns the mode runners started synchronously by run.
	runWaitGroup sync.WaitGroup

	clientStrategy *ClientStrategy
	routeManager   *RouteManager

	platformUrl string
	// auth is an immutable clone guarded by stateLock. Each connection
	// generation takes one value snapshot so its JWT, instance, app version,
	// and JWT-derived client/device identity can never come from mixed updates.
	auth *ClientAuth

	settings *PlatformTransportSettings
	// receiveStats is always non-nil, including when the settings did not expose
	// an aggregate collector.
	receiveStats *PlatformTransportReceiveStats
	// the effective framer settings: `settings.FramerSettings`, or a private
	// copy of it when the transport log is propagated into a nil
	// `FramerSettings.Log`. The caller's settings are never mutated — they
	// may be shared with concurrent framer users (see
	// NewPlatformTransportWithTargetMode).
	framerSettings *FramerSettings
	// One process budget spans every H3 mode and reconnect generation owned by
	// this transport. Each live connection adds its smaller peer-local bound.
	h3DatagramSettings         *H3DatagramSettings
	h3DatagramStats            *H3DatagramStats
	h3DatagramReassemblyBudget *H3DatagramReassemblyBudget
	// Claims register synchronously in the constructor, before carrier
	// goroutines start, so pending H1 demand is visible to every H3 admission.
	h1BudgetReservation *platformTransportBudgetReservation
	h3BudgetReservation *platformTransportBudgetReservation
	h3Gate              *platformH3Gate

	stateLock sync.Mutex
	// notified when availableModes changes. availableModes is a map, so it
	// cannot be a MonitorValue; the notify is issued inside the same locked
	// scope as the mutation (see setModeAvailable)
	availableModeMonitor *Monitor
	availableModes       map[TransportMode]bool
	// immutable after construction; normalized and cloned from settings so a
	// caller can reuse or mutate its settings without racing the run loop
	modePreferences map[TransportMode]int
	targetMode      TransportMode
	// the elected active mode, watched by every transport's mode gate and
	// inactive-drain watchdog. a MonitorValue so the mutation cannot be
	// separated from its notification, and so re-electing the same mode does
	// not wake the election loop's own watchers
	mode *MonitorValue[TransportMode]

	// the number of connections with routes currently registered on the route
	// manager. 0 < count means the transport is carrying (or able to carry)
	// traffic. Used by make-before-break migration to wait for a replacement
	// transport to come up before closing the old one (CONNECTDRAIN2.md §3.3)
	registeredCount  atomic.Int64
	connectedMonitor *Monitor

	// The extender address of each live H1 connection, counted so two
	// connections through one extender (a make-before-break generation) are
	// one entry (K1). A direct connection and every H3 connection contribute
	// nothing. Guarded by stateLock; `extenderIpsMonitor` is a change counter
	// rather than the set itself, because a slice is not comparable, and it is
	// bumped inside the same locked scope as the mutation. The counter is
	// incremented from whatever it holds rather than from a count of this
	// transport's own changes, because an owner may share one counter across
	// transport generations and two transports counting independently would
	// write the same value twice -- a change that notifies nobody.
	extenderIpCounts   map[netip.Addr]int
	extenderIpsMonitor *MonitorValue[uint64]

	// kickMonitor closes the live connection (if any) so the run loop
	// re-dials immediately — fired on a host network path change
	// (NetworkChanged), where the current socket is likely bound to a dead
	// path and would otherwise linger until a ping/write timeout notices.
	// Ported from upstream main e05ecee, merged with our reconnect fast
	// path: the kicked re-dial goes through NextReconnectTime (small
	// independent jitter, capped concurrency) rather than the serialized
	// NextConnectTime staircase, since a network change is a legitimate
	// fresh start.
	kickMonitor *Monitor
	// unsubNetworkChange removes this transport from the process
	// network-change listeners when the run loop exits.
	unsubNetworkChange func()

	// ipFamily is the pin: 4, 6, or 0. Immutable after construction.
	ipFamily int
	// enabled is the owner's switch (SetEnabled). familyHold is a pinned
	// transport's own reason not to dial, a PlatformTransportState, with
	// PlatformTransportStateConnecting meaning none. held derives from both
	// and is what the mode runners wait on. See transport_family.go.
	enabled       atomic.Bool
	familyHold    atomic.Int32
	held          *MonitorValue[bool]
	pinnedBackoff *pinnedDialBackoff
}

// newPlatformQuicConfig keeps H3's memory and path-MTU behavior explicit and
// testable. DPLPMTUD remains enabled so a validated path can grow beyond the
// conservative initial packet and adapt after migration without fragmentation.
func newPlatformQuicConfig(
	settings *PlatformTransportSettings,
	slowMultiple int,
) *quic.Config {
	initialStreamReceiveWindow := settings.H3InitialStreamReceiveWindowByteCount
	if initialStreamReceiveWindow <= 0 {
		initialStreamReceiveWindow = kib(256)
	}
	maxStreamReceiveWindow := settings.H3MaxStreamReceiveWindowByteCount
	if maxStreamReceiveWindow <= 0 {
		maxStreamReceiveWindow = defaultH3MaxStreamReceiveWindowByteCount()
	}
	initialConnectionReceiveWindow := settings.H3InitialConnectionReceiveWindowByteCount
	if initialConnectionReceiveWindow <= 0 {
		initialConnectionReceiveWindow = kib(512)
	}
	maxConnectionReceiveWindow := settings.H3MaxConnectionReceiveWindowByteCount
	if maxConnectionReceiveWindow <= 0 {
		maxConnectionReceiveWindow = defaultH3MaxConnectionReceiveWindowByteCount()
	}
	config := &quic.Config{
		HandshakeIdleTimeout: time.Duration(slowMultiple) *
			(settings.QuicConnectTimeout + settings.QuicHandshakeTimeout),
		MaxIdleTimeout: settings.PingTimeout * 4,
		// QUIC owns hybrid-carrier liveness. Its keepalive loop is independent
		// of the application writer, which can legitimately wait behind
		// quic-go's bounded DATAGRAM queue on a constrained uplink.
		KeepAlivePeriod:   settings.PingTimeout,
		Allow0RTT:         true,
		InitialPacketSize: H3InitialPacketByteCount,
		// Pin the receive windows and stream counts. The platform transport
		// uses one bidirectional stream; the stream counts bound abuse.
		InitialStreamReceiveWindow:     uint64(initialStreamReceiveWindow),
		MaxStreamReceiveWindow:         uint64(maxStreamReceiveWindow),
		InitialConnectionReceiveWindow: uint64(initialConnectionReceiveWindow),
		MaxConnectionReceiveWindow:     uint64(maxConnectionReceiveWindow),
		MaxIncomingStreams:             8,
		MaxIncomingUniStreams:          8,
		EnableDatagrams:                settings.EnableH3Datagrams,
	}
	if settings.H3QuicPacketStats != nil {
		config.Tracer = settings.H3QuicPacketStats.Tracer
	}
	return config
}

// Kick closes the transport's live connection (if any) and skips any pending
// reconnect backoff so the run loop re-dials immediately over the new path.
// The transport itself stays up; an in-flight dial is unaffected. Safe to
// call at any time.
func (self *PlatformTransport) Kick() {
	self.kickMonitor.NotifyAll()
}

// IsConnected reports whether the transport has a connection with routes
// registered on the route manager
func (self *PlatformTransport) IsConnected() bool {
	return 0 < self.registeredCount.Load()
}

// IsWaitingForBudget reports whether the reservation required by the selected
// target mode is currently blocked. Auto's H3 reservation is optional and is
// therefore not reported once its required H1 path can run. It is diagnostic;
// policy migration uses CanMakeBeforeBreakFrom for its terminal decision.
func (self *PlatformTransport) IsWaitingForBudget() bool {
	switch self.targetMode {
	case TransportModeH1:
		return self.h1BudgetReservation.IsWaiting()
	case TransportModeH3, TransportModeH3Dns, TransportModeH3DnsPump:
		return self.h3BudgetReservation.IsWaiting()
	case TransportModeAuto:
		return self.h1BudgetReservation.IsWaiting()
	default:
		return false
	}
}

// AllowBudgetHandoffFrom pairs the reservation needed to activate this
// transport with one acquired reservation on previous. It is deliberately
// limited to transitions with H1 on at least one side: H1 -> H3 is bounded by
// old H1, and H3 -> H1 is bounded by new H1. Two full H3 claims never bypass
// the memory cap. The caller still closes previous only after this transport
// reports connected.
func (self *PlatformTransport) AllowBudgetHandoffFrom(previous *PlatformTransport) bool {
	if self == nil || previous == nil {
		return false
	}
	replacement := self.h1BudgetReservation
	if replacement == nil {
		replacement = self.h3BudgetReservation
	}
	if replacement == nil {
		return false
	}
	// Prefer the previous H1 claim. This keeps Auto's optional H3 lease
	// independently preemptible while its usable H1 route survives the handoff.
	if replacement.AllowHandoffFrom(previous.h1BudgetReservation) {
		return true
	}
	return replacement.AllowHandoffFrom(previous.h3BudgetReservation)
}

// CanMakeBeforeBreakFrom prepares the bounded budget handoff and reports
// whether previous can remain alive until this transport connects. If normal
// capacity already fits, no loan is needed. A budget-blocked H3 -> H3-family
// replacement returns false rather than temporarily allocating a second full
// H3 working set; callers may then release the old H3 to guarantee progress.
func (self *PlatformTransport) CanMakeBeforeBreakFrom(previous *PlatformTransport) bool {
	if self == nil || previous == nil {
		return true
	}
	if self.AllowBudgetHandoffFrom(previous) {
		return true
	}
	return !self.IsWaitingForBudget()
}

// ConnectedNotify returns a channel that closes on the next connect state
// change. Capture the channel before checking `IsConnected`.
func (self *PlatformTransport) ConnectedNotify() <-chan struct{} {
	return self.connectedMonitor.NotifyChannel()
}

// ExtenderIps are the extender addresses carrying this transport's live
// connections right now, deduplicated and sorted (K1). Empty means every live
// connection is direct, which is also what an H3-only transport reports: H3
// never runs through an extender.
func (self *PlatformTransport) ExtenderIps() []netip.Addr {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	ips := make([]netip.Addr, 0, len(self.extenderIpCounts))
	for ip := range self.extenderIpCounts {
		ips = append(ips, ip)
	}
	slices.SortFunc(ips, func(a netip.Addr, b netip.Addr) int {
		return a.Compare(b)
	})
	return ips
}

// ExtenderIpsMonitor counts changes of the set above. A consumer subscribes to
// it and then reads `ExtenderIps`; the counter exists because the set itself is
// a slice and cannot ride a MonitorValue.
func (self *PlatformTransport) ExtenderIpsMonitor() *MonitorValue[uint64] {
	return self.extenderIpsMonitor
}

// holdExtenderIp publishes one live H1 connection through an extender and
// returns the release for when that connection ends (K1, K4). The strategy's
// directory counts the same connection as in use, so the app's active extender
// count is exactly the extenders carrying traffic right now; an api pooled
// connection through the same address is not counted. A direct connection
// (the zero address) holds nothing and releases nothing.
func (self *PlatformTransport) holdExtenderIp(ip netip.Addr) func() {
	if !ip.IsValid() {
		return func() {}
	}
	ip = ip.Unmap()
	directory := self.clientStrategy.ExtenderDirectory()
	self.changeExtenderIp(ip, 1)
	if directory != nil {
		directory.SetInUse(ip, 1)
	}
	return func() {
		self.changeExtenderIp(ip, -1)
		if directory != nil {
			directory.SetInUse(ip, -1)
		}
	}
}

// Adjusts the live connection count of one extender address. The change
// counter is bumped, inside the same locked scope, only when the published set
// actually changed, so a second connection through an address that is already
// carrying one wakes nobody.
func (self *PlatformTransport) changeExtenderIp(ip netip.Addr, delta int) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	count := self.extenderIpCounts[ip] + delta
	changed := false
	if count <= 0 {
		if _, ok := self.extenderIpCounts[ip]; ok {
			delete(self.extenderIpCounts, ip)
			changed = true
		}
	} else {
		if _, ok := self.extenderIpCounts[ip]; !ok {
			changed = true
		}
		self.extenderIpCounts[ip] = count
	}
	if changed {
		self.extenderIpsMonitor.Update(func(count uint64) uint64 {
			return count + 1
		})
	}
}

// ReceiveStats returns a lock-free lifetime snapshot for this transport. It
// includes all reconnect generations and every mode runner owned by Auto.
func (self *PlatformTransport) ReceiveStats() PlatformTransportReceiveStatsSnapshot {
	return self.receiveStats.Snapshot()
}

// DatagramStats returns lifetime candidate-carrier counters across every H3
// mode and reconnect generation owned by this transport.
func (self *PlatformTransport) DatagramStats() H3DatagramStatsSnapshot {
	return self.h3DatagramStats.Snapshot()
}

// Splitting hybrid receive-lane metadata must not duplicate the payload queue.
// DATAGRAM retains the historical bounded queue while the reliable stream uses
// an unbuffered route and may retain only its one already-read frame.
func platformH3ReceiveRouteBufferSizes(
	transportBufferSize int,
	useH3Datagrams bool,
) (reliable int, unreliable int) {
	if useH3Datagrams {
		return 0, transportBufferSize
	}
	return transportBufferSize, 0
}

// offerReceive transfers one complete carrier message to a lane-specific
// Client route. Dropping after a reliable WebSocket, QUIC-stream, or other
// stream read manufactures a Transfer hole and defeats the carrier's own
// backpressure. A reliable reader therefore retains exactly its already-read
// message until route capacity or cancellation. Unreliable DATAGRAM lanes keep
// zero-wait admission and Transfer recovery. False means the connection
// generation ended and its reader should exit.
func (self *PlatformTransport) offerReceive(
	done <-chan struct{},
	mode TransportMode,
	reliability CarrierReliability,
	receive chan<- []byte,
	message []byte,
) (open bool, delivered bool) {
	switch tryOfferPooledReceive(done, receive, message) {
	case pooledReceiveOfferDelivered:
		return true, true
	case pooledReceiveOfferFull:
		if reliability == CarrierReliabilityReliable {
			self.receiveStats.recordQueueBackpressure(mode, len(message))
			select {
			case <-done:
				MessagePoolReturn(message)
				return false, false
			case receive <- message:
				return true, true
			}
		}
		self.receiveStats.recordQueueDrop(mode, len(message))
		MessagePoolReturn(message)
		return true, false
	default:
		MessagePoolReturn(message)
		return false, false
	}
}

// offerH1Control refuses a saturated reliable control queue immediately. A
// speed-test/latency control message has no Transfer acknowledgement above it,
// so skipping it would desynchronize state; the caller closes this websocket
// generation and reconnects instead.
func (self *PlatformTransport) offerH1Control(
	done <-chan struct{},
	controlSend chan<- []byte,
	message []byte,
) bool {
	switch tryOfferPooledReceive(done, controlSend, message) {
	case pooledReceiveOfferDelivered:
		return true
	case pooledReceiveOfferFull:
		self.receiveStats.recordH1ControlRefusal(len(message))
		MessagePoolReturn(message)
		return false
	default:
		MessagePoolReturn(message)
		return false
	}
}

func (self *PlatformTransport) setRegistered(registered bool) {
	if registered {
		self.registeredCount.Add(1)
	} else {
		self.registeredCount.Add(-1)
	}
	self.connectedMonitor.NotifyAll()
}

func NewPlatformTransportWithDefaults(
	ctx context.Context,
	clientStrategy *ClientStrategy,
	routeManager *RouteManager,
	platformUrl string,
	auth *ClientAuth,
) *PlatformTransport {
	return NewPlatformTransport(
		ctx,
		clientStrategy,
		routeManager,
		platformUrl,
		auth,
		DefaultPlatformTransportSettings(),
	)
}

func NewPlatformTransport(
	ctx context.Context,
	clientStrategy *ClientStrategy,
	routeManager *RouteManager,
	platformUrl string,
	auth *ClientAuth,
	settings *PlatformTransportSettings,
) *PlatformTransport {
	return NewPlatformTransportWithTargetMode(
		ctx,
		clientStrategy,
		routeManager,
		platformUrl,
		auth,
		TransportModeAuto,
		settings,
	)
}

func NewPlatformTransportWithTargetMode(
	ctx context.Context,
	clientStrategy *ClientStrategy,
	routeManager *RouteManager,
	platformUrl string,
	auth *ClientAuth,
	targetMode TransportMode,
	settings *PlatformTransportSettings,
) *PlatformTransport {
	cancelCtx, cancel := context.WithCancel(ctx)
	log := loggerOrDefault(settings.Log)
	// propagate so a transport-level logger covers the framer. Copy instead
	// of writing through the caller's settings: the caller may share the
	// framer settings with concurrently running framers (racing this write).
	framerSettings := settings.FramerSettings
	if framerSettings != nil && framerSettings.Log == nil {
		copied := *framerSettings
		copied.Log = log
		framerSettings = &copied
	}
	receiveStats := settings.ReceiveStats
	if receiveStats == nil {
		receiveStats = &PlatformTransportReceiveStats{}
	}
	h3DatagramSettings := settings.H3DatagramSettings
	if h3DatagramSettings == nil {
		h3DatagramSettings = DefaultH3DatagramSettings()
	}
	if err := h3DatagramSettings.Validate(); err != nil {
		panic(err)
	}
	h3DatagramStats := settings.H3DatagramStats
	if h3DatagramStats == nil {
		h3DatagramStats = &H3DatagramStats{}
	}
	transport := &PlatformTransport{
		ctx:    cancelCtx,
		cancel: cancel,
		log:    log,
		done:   make(chan struct{}),
		// cancel: func() {
		// 	select {
		// 	case <- ctx.Done():
		// 	default:
		// 		debug.PrintStack()
		// 		cancel()
		// 	}
		// },
		clientStrategy:     clientStrategy,
		routeManager:       routeManager,
		platformUrl:        platformUrl,
		auth:               cloneClientAuth(auth),
		settings:           settings,
		receiveStats:       receiveStats,
		framerSettings:     framerSettings,
		h3DatagramSettings: h3DatagramSettings,
		h3DatagramStats:    h3DatagramStats,
		h3DatagramReassemblyBudget: NewH3DatagramReassemblyBudget(
			h3DatagramSettings.ProcessReassemblyByteCount,
		),
		availableModeMonitor: NewMonitor(),
		availableModes:       map[TransportMode]bool{},
		modePreferences:      normalizeTransportModePreferences(settings.ModePreferences),
		targetMode:           targetMode,
		mode:                 NewMonitorValue(TransportModeNone),
		connectedMonitor:     NewMonitor(),
		kickMonitor:          NewMonitor(),
		extenderIpCounts:     map[netip.Addr]int{},
		extenderIpsMonitor:   settings.ExtenderIpsMonitor,
	}
	if transport.extenderIpsMonitor == nil {
		transport.extenderIpsMonitor = NewMonitorValue[uint64](0)
	}
	transport.ipFamily = normalizeIpFamily(settings.IpFamily)
	transport.enabled.Store(!settings.StartDisabled)
	transport.held = NewMonitorValue(settings.StartDisabled)
	pinnedReconnectMaxTimeout := settings.PinnedReconnectMaxTimeout
	if pinnedReconnectMaxTimeout <= 0 {
		pinnedReconnectMaxTimeout = 5 * time.Minute
	}
	transport.pinnedBackoff = newPinnedDialBackoff(settings.ReconnectTimeout, pinnedReconnectMaxTimeout)
	if transport.ipFamily == 6 {
		// the dns pump host publishes no AAAA record: the mode cannot work
		// over a v6 pin, so it is not offered rather than left to fail
		delete(transport.modePreferences, TransportModeH3DnsPump)
		if targetMode == TransportModeH3DnsPump {
			log.Infof("[t]h3dnspump is not available over ipv6; using h3dns\n")
			targetMode = TransportModeH3Dns
			transport.targetMode = targetMode
		}
	}
	transport.h3Gate = newPlatformH3Gate(transport)
	h1Enabled := targetMode == TransportModeH1 ||
		(targetMode == TransportModeAuto &&
			transport.modePreference(TransportModeH1) != modePreferenceNone)
	h3Enabled := targetMode == TransportModeH3 || targetMode == TransportModeH3Dns ||
		targetMode == TransportModeH3DnsPump ||
		(targetMode == TransportModeAuto &&
			(transport.modePreference(TransportModeH3) != modePreferenceNone ||
				transport.modePreference(TransportModeH3Dns) != modePreferenceNone ||
				transport.modePreference(TransportModeH3DnsPump) != modePreferenceNone))
	if settings.PlatformTransportBudget != nil {
		if h1Enabled {
			transport.h1BudgetReservation = settings.PlatformTransportBudget.register(
				platformTransportBudgetH1,
				transport.h1BudgetByteCount(),
				true,
			)
		}
		if h3Enabled {
			h3BudgetClass := platformTransportBudgetH3Explicit
			if targetMode == TransportModeAuto {
				h3BudgetClass = platformTransportBudgetH3Auto
			}
			transport.h3BudgetReservation = settings.PlatformTransportBudget.registerWithPriority(
				h3BudgetClass,
				transport.h3BudgetByteCount(),
				!h1Enabled,
				settings.PlatformTransportBudgetPriority,
			)
		}
	}
	// a host network path change kicks the live connection so the re-dial
	// happens now instead of after a ping/write timeout notices the dead path.
	// unsubscribe rides the run loop exit (ctx cancel), not just Close — most
	// owners tear transports down by canceling the client ctx.
	transport.unsubNetworkChange = AddNetworkChangeListener(transport.Kick)
	if transport.pinned() {
		// the initial hold is in place before any runner can dial, so a
		// pinned transport on a device without its family never dials once
		transport.refreshFamilyHold()
		go HandleError(transport.runFamilyHoldWatcher, cancel)
	}
	go HandleError(func() {
		defer close(transport.done)
		defer transport.unsubNetworkChange()
		transport.run()
	}, cancel)
	return transport
}

// cloneClientAuth transfers a caller-owned auth value into transport ownership.
// PlatformTransport historically requires non-nil auth and intentionally keeps
// that fail-fast contract.
func cloneClientAuth(auth *ClientAuth) *ClientAuth {
	cloned := *auth
	return &cloned
}

// authSnapshot returns one coherent auth generation for a connection attempt.
func (self *PlatformTransport) authSnapshot() ClientAuth {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return *self.auth
}

// SetAuth installs an immutable snapshot for future connections. An existing
// authenticated connection keeps its generation until it reconnects.
func (self *PlatformTransport) SetAuth(auth *ClientAuth) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.auth = cloneClientAuth(auth)
}

// setModeAvailable records whether a mode has a live connection, waking the
// election loop (`run`) when that changes. The notify is issued in the same
// locked scope as the mutation, and only on an actual change: an unconditional
// notify would wake the loop on every reconnect churn for no new decision.
func (self *PlatformTransport) setModeAvailable(mode TransportMode, available bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if self.availableModes[mode] == available {
		return
	}
	self.availableModes[mode] = available
	self.availableModeMonitor.NotifyAll()
}

func (self *PlatformTransport) modesAvailable() (map[TransportMode]bool, chan struct{}) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	return maps.Clone(self.availableModes), self.availableModeMonitor.NotifyChannel()
}

// setActiveMode publishes the elected mode. It notifies the mode gates and the
// inactive-drain watchdogs — but deliberately not the election loop, which is
// the caller: watching what it writes would wake it in a cycle.
func (self *PlatformTransport) setActiveMode(mode TransportMode) {
	self.mode.Set(mode)
}

func (self *PlatformTransport) activeMode() (TransportMode, chan struct{}) {
	return self.mode.Get()
}

// transportModePreferences ranks the default real transport modes. LOWER IS BETTER (see
// isBetterMode). TransportModeNone is deliberately NOT a key: modePreference
// ranks it, and any unknown mode, worse than every real mode. Leaving it out of
// the table and reading the map directly scored it 0 — better than everything —
// which is why no mode gate ever parked and the election could not distinguish
// "no transport" from "the best transport".
//
// Four strict production tiers: H1, direct H3, H3-over-DNS, then DNS-pump.
// Custom policies may still tie modes; the election is sticky among equals.
//
// This table previously had the tiers inverted — it made h3dnspump the most
// preferred mode — contradicting the mode constants, which are declared "in
// order of increasing preference". Nothing enforced the ordering then (the mode
// was never elected at all), so the inversion was inert; the gates enforce it now.
var transportModePreferences = DefaultTransportModePreferences()

// modePreferenceNone ranks TransportModeNone — the absence of a transport — and
// any mode missing from the table as worse than every real mode.
const modePreferenceNone = math.MaxInt

func modePreference(mode TransportMode) int {
	if preference, ok := transportModePreferences[mode]; ok {
		return preference
	}
	return modePreferenceNone
}

func (self *PlatformTransport) modePreference(mode TransportMode) int {
	preferences := self.modePreferences
	if preferences == nil {
		preferences = transportModePreferences
	}
	if preference, ok := preferences[mode]; ok {
		return preference
	}
	return modePreferenceNone
}

func (self *PlatformTransport) orderedModes() []TransportMode {
	preferences := self.modePreferences
	if preferences == nil {
		preferences = transportModePreferences
	}
	orderedModes := slices.Collect(maps.Keys(preferences))
	slices.SortFunc(orderedModes, func(a TransportMode, b TransportMode) int {
		preferenceA := self.modePreference(a)
		preferenceB := self.modePreference(b)
		if preferenceA < preferenceB {
			return -1
		} else if preferenceB < preferenceA {
			return 1
		}
		return strings.Compare(string(a), string(b))
	})
	return orderedModes
}

// modeInitialDelay returns one ModeInitialDelay per distinct preference tier,
// not per numeric gap. Custom priorities 10 and 100 therefore remain a one-step
// fallback rather than accidentally waiting 90 intervals.
func (self *PlatformTransport) modeInitialDelay(mode TransportMode) time.Duration {
	priority := self.modePreference(mode)
	if priority == modePreferenceNone {
		return 0
	}
	preferences := self.modePreferences
	if preferences == nil {
		preferences = transportModePreferences
	}
	tiers := []int{}
	for _, candidatePriority := range preferences {
		if !slices.Contains(tiers, candidatePriority) {
			tiers = append(tiers, candidatePriority)
		}
	}
	slices.Sort(tiers)
	return time.Duration(slices.Index(tiers, priority)) * self.settings.ModeInitialDelay
}

func (self *PlatformTransport) runMode(mode TransportMode, initialDelay time.Duration) {
	switch mode {
	case TransportModeH1:
		self.runH1(initialDelay)
	case TransportModeH3:
		self.runH3(self.ctx, TransportModeH3, initialDelay, 1)
	case TransportModeH3Dns:
		self.runH3(self.ctx, TransportModeH3Dns, initialDelay, self.settings.PtDnsSlowMultiple)
	case TransportModeH3DnsPump:
		self.runH3(self.ctx, TransportModeH3DnsPump, initialDelay, self.settings.PtDnsSlowMultiple)
	}
}

func (self *PlatformTransport) runH3Mode(
	ctx context.Context,
	mode TransportMode,
	initialDelay time.Duration,
) {
	if self.settings.runH3ModeForTest != nil {
		self.settings.runH3ModeForTest(ctx, mode, initialDelay)
		return
	}
	switch mode {
	case TransportModeH3:
		self.runH3(ctx, TransportModeH3, initialDelay, 1)
	case TransportModeH3Dns:
		self.runH3(ctx, TransportModeH3Dns, initialDelay, self.settings.PtDnsSlowMultiple)
	case TransportModeH3DnsPump:
		self.runH3(ctx, TransportModeH3DnsPump, initialDelay, self.settings.PtDnsSlowMultiple)
	}
}

func isH3TransportMode(mode TransportMode) bool {
	switch mode {
	case TransportModeH3, TransportModeH3Dns, TransportModeH3DnsPump:
		return true
	default:
		return false
	}
}

func (self *PlatformTransport) startH3ModeGroup(modes []TransportMode, auto bool) {
	if len(modes) == 0 {
		return
	}
	self.startModeRunner(func() {
		reservation := self.h3BudgetReservation
		if reservation != nil {
			defer reservation.Release()
		}
		for self.ctx.Err() == nil {
			// a held transport neither dials nor holds an optional lease
			if !self.waitDialAdmission(self.ctx) {
				return
			}
			if reservation != nil && !reservation.Acquire(self.ctx) {
				return
			}

			groupCtx, cancelGroup := context.WithCancel(self.ctx)
			var waitGroup sync.WaitGroup
			for _, mode := range modes {
				mode := mode
				initialDelay := time.Duration(0)
				if auto {
					initialDelay = self.modeInitialDelay(mode)
				}
				waitGroup.Add(1)
				go HandleError(func() {
					defer waitGroup.Done()
					self.runH3Mode(groupCtx, mode, initialDelay)
					// A carrier runner is normally process-lifetime. Preserve the old
					// fail-closed behavior for an unexpected return, but not for an
					// intentional budget preemption of this H3 group.
					if groupCtx.Err() == nil {
						self.cancel()
					}
				}, self.cancel)
			}
			groupDone := make(chan struct{})
			go func() {
				waitGroup.Wait()
				close(groupDone)
			}()

			var preempt <-chan struct{}
			if reservation != nil {
				preempt = reservation.PreemptNotify()
			}
			// a hold (the owner disabled the transport, or a pinned family
			// went away) stops the runners and yields the optional lease
			// exactly like a preemption, so a parked transport retains no H3
			// working set. The runners restart when the hold lifts.
			held, holdNotify := self.heldNotify()
			if held {
				closed := make(chan struct{})
				close(closed)
				holdNotify = closed
			}
			preempted := false
			heldNow := false
			select {
			case <-self.ctx.Done():
			case <-groupDone:
			case <-preempt:
				preempted = true
			case <-holdNotify:
				heldNow = true
			}
			cancelGroup()
			waitGroup.Wait()
			if self.ctx.Err() != nil || (!preempted && !heldNow) {
				return
			}
			// Routes and sockets are gone before the accounting lease is
			// yielded. The pending reservation will reacquire when the higher-
			// precedence claimant leaves, or when the hold lifts. An explicit
			// H3 claim cannot yield and simply stays acquired across a hold.
			if reservation != nil {
				yielded := reservation.Yield()
				if preempted && !yielded {
					return
				}
			}
		}
	})
}

// electAvailableMode is the pure Auto transition decision. Keeping the
// decision separate from runner startup and notifications gives policy
// transitions one deterministic module: retain an available current mode
// unless a strictly higher-priority mode is available, otherwise fall back to
// the best live mode (or None). Equal custom priorities remain sticky.
func (self *PlatformTransport) electAvailableMode(
	activeMode TransportMode,
	available map[TransportMode]bool,
) TransportMode {
	bestMode := TransportModeNone
	for _, mode := range self.orderedModes() {
		if available[mode] {
			bestMode = mode
			break
		}
	}
	if !available[activeMode] || self.isBetterMode(bestMode, activeMode) {
		return bestMode
	}
	return activeMode
}

// runModeElection owns Auto promotion/fallback. setModeAvailable notifies this
// loop; setActiveMode intentionally notifies only carrier gates so the loop
// cannot wake itself and spin.
func (self *PlatformTransport) runModeElection(ctx context.Context) {
	for {
		available, notify := self.modesAvailable()
		self.setActiveMode(self.electAvailableMode(self.mode.Value(), available))
		select {
		case <-notify:
		case <-ctx.Done():
			return
		}
	}
}

func (self *PlatformTransport) run() {
	defer func() {
		self.cancel()
		self.runWaitGroup.Wait()
	}()
	if self.h1BudgetReservation != nil {
		defer self.h1BudgetReservation.Release()
		if !self.h1BudgetReservation.Acquire(self.ctx) {
			return
		}
	}

	switch self.targetMode {
	case TransportModeAuto:
		h3Modes := []TransportMode{}
		for _, mode := range self.orderedModes() {
			mode := mode
			if isH3TransportMode(mode) && self.h3BudgetReservation != nil {
				h3Modes = append(h3Modes, mode)
				continue
			}
			initialDelay := self.modeInitialDelay(mode)
			self.startModeRunner(func() {
				self.runMode(mode, initialDelay)
			})
		}
		self.startH3ModeGroup(h3Modes, true)
	case TransportModeH3, TransportModeH1, TransportModeH3Dns, TransportModeH3DnsPump:
		if isH3TransportMode(self.targetMode) && self.h3BudgetReservation != nil {
			self.startH3ModeGroup([]TransportMode{self.targetMode}, false)
		} else {
			self.startModeRunner(func() {
				self.runMode(self.targetMode, 0)
			})
		}
	}

	self.runModeElection(self.ctx)
}

// Starts one owned mode runner. All runners are registered before run can
// reach its cancellation wait, so Wait never races a later Add.
func (self *PlatformTransport) startModeRunner(run func()) {
	self.runWaitGroup.Add(1)
	go HandleError(func() {
		defer self.runWaitGroup.Done()
		run()
	}, self.cancel)
}

// returns true is other is better than current
// isBetterMode reports whether mode is strictly preferred over other. Lower
// preference values are better; TransportModeNone is worse than everything.
func isBetterMode(mode TransportMode, other TransportMode) bool {
	return modePreference(mode) < modePreference(other)
}

func (self *PlatformTransport) isBetterMode(mode TransportMode, other TransportMode) bool {
	return self.modePreference(mode) < self.modePreference(other)
}

// standDown reports whether a transport running mode should stand down because a
// strictly better mode is currently active, along with the channel that closes
// when the active mode changes. A transport runs when it is the active mode, or
// when nothing better than it is active — including at startup, where the active
// mode is TransportModeNone (worse than every real mode) precisely so that the
// first transport is admitted and can make itself available.
//
// The gates previously asked isBetterMode(myMode, activeMode) — standing down
// when the transport was BETTER than what was active, exactly backwards. It was
// masked because TransportModeNone scored best, so the predicate was always
// false and no transport ever stood down.
func (self *PlatformTransport) standDown(mode TransportMode) (bool, chan struct{}) {
	activeMode, notify := self.activeMode()
	return self.isBetterMode(activeMode, mode), notify
}

func (self *PlatformTransport) h1MaxMessageByteCount() int64 {
	if 0 < self.settings.H1MaxMessageByteCount {
		return self.settings.H1MaxMessageByteCount
	}
	if self.framerSettings != nil && 0 < self.framerSettings.MaxMessageLen {
		return int64(self.framerSettings.MaxMessageLen)
	}
	return DefaultClientSettings().MinimumMessageLenLimit()
}

func (self *PlatformTransport) h1BudgetByteCount() ByteCount {
	if 0 < self.settings.H1BudgetByteCount {
		return self.settings.H1BudgetByteCount
	}
	return MemoryScaledByteCount(kib(512), kib(256))
}

func (self *PlatformTransport) h3BudgetByteCount() ByteCount {
	if 0 < self.settings.H3BudgetByteCount {
		return self.settings.H3BudgetByteCount
	}
	return defaultH3BudgetByteCount()
}

func (self *PlatformTransport) h3SocketReadBufferByteCount() ByteCount {
	if 0 < self.settings.H3SocketReadBufferByteCount {
		return self.settings.H3SocketReadBufferByteCount
	}
	return MemoryScaledByteCount(mib(1), kib(256))
}

func (self *PlatformTransport) h3SocketWriteBufferByteCount() ByteCount {
	if 0 < self.settings.H3SocketWriteBufferByteCount {
		return self.settings.H3SocketWriteBufferByteCount
	}
	return MemoryScaledByteCount(mib(1), kib(256))
}

// runInactiveDrain bounds a carrier only after a strictly better mode has
// superseded it. Control frames and keepalives never reset the quiet interval;
// callers increment the counters only for routed payload. Even continuous
// payload cannot retain the fallback beyond the absolute deadline.
func (self *PlatformTransport) runInactiveDrain(
	ctx context.Context,
	mode TransportMode,
	slowMultiple int,
	readCounter *atomic.Uint64,
	writeCounter *atomic.Uint64,
	cancel context.CancelFunc,
) {
	quietTimeout := time.Duration(slowMultiple) * self.settings.InactiveDrainTimeout
	if quietTimeout <= 0 {
		quietTimeout = time.Millisecond
	}
	maxTimeout := time.Duration(slowMultiple) * self.settings.InactiveDrainMaxTimeout
	if maxTimeout <= 0 {
		maxTimeout = 2 * quietTimeout
	}

	var supersededSince time.Time
	var startReadCount uint64
	var startWriteCount uint64
	for {
		activeMode, notify := self.activeMode()
		if !self.isBetterMode(activeMode, mode) {
			supersededSince = time.Time{}
			select {
			case <-ctx.Done():
				return
			case <-notify:
			}
			continue
		}

		if supersededSince.IsZero() {
			supersededSince = time.Now()
			startReadCount = readCounter.Load()
			startWriteCount = writeCounter.Load()
		}
		remaining := maxTimeout - time.Since(supersededSince)
		if remaining <= 0 {
			cancel()
			return
		}
		wait := min(quietTimeout, remaining)
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return
		case <-notify:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			continue
		case <-timer.C:
		}

		if maxTimeout <= time.Since(supersededSince) ||
			(readCounter.Load() == startReadCount && writeCounter.Load() == startWriteCount) {
			cancel()
			return
		}
		startReadCount = readCounter.Load()
		startWriteCount = writeCounter.Load()
	}
}

func (self *PlatformTransport) runH1(initialTimeout time.Duration) {
	// connect and update route manager for this transport
	defer self.cancel()

	if 0 < initialTimeout {
		select {
		case <-self.ctx.Done():
			return
		case <-time.After(initialTimeout):
		}
	}

	// hadConnection marks the iteration immediately after a connection ran and
	// died: only that first re-dial takes the reconnect fast path
	// (NextReconnectTime); a failed re-dial clears it, so retries fall back to
	// the serialized NextConnectTime pacing.
	hadConnection := false

	for {
		// stand down while a strictly better mode is active
		func() {
			for {
				standDown, notify := self.standDown(TransportModeH1)
				if !standDown {
					return
				}
				select {
				case <-self.ctx.Done():
					return
				case <-notify:
				}
			}
		}()
		if !self.waitDialAdmission(self.ctx) {
			return
		}
		auth := self.authSnapshot()
		clientId, _ := auth.ClientId()

		reconnect := NewReconnect(self.settings.ReconnectTimeout)
		// the extender the winning dialer used, zero for a direct dial (K1).
		// Written by the single dial below and read by the connection it
		// produced, both on this goroutine.
		var dialExtenderIp netip.Addr
		connect := func() (*websocket.Conn, error) {
			header := http.Header{}
			if self.settings.V2H1Auth {
				header.Add("Authorization", fmt.Sprintf("Bearer %s", auth.ByJwt))
				header.Add("X-UR-AppVersion", auth.AppVersion)
				header.Add("X-UR-InstanceId", auth.InstanceId.String())
				header.Add("X-UR-TransportVersion", fmt.Sprintf("%d", TransportVersion))
				self.applyIntentHeader(header)
			}

			ws, _, dialerInfo, err := self.clientStrategy.WsDialContextWithDialer(
				self.dialContext(self.ctx),
				self.platformUrl,
				header,
			)
			if err != nil {
				return nil, err
			}
			if dialerInfo != nil {
				dialExtenderIp = dialerInfo.ExtenderIp
			}
			ws.SetReadLimit(self.h1MaxMessageByteCount())

			success := false
			defer func() {
				if !success {
					ws.Close()
				}
			}()

			if !self.settings.V2H1Auth {
				authBytes, err := EncodeFrame(&protocol.Auth{
					ByJwt:      auth.ByJwt,
					AppVersion: auth.AppVersion,
					InstanceId: auth.InstanceId.Bytes(),
					IpFamily:   self.authIntent(),
				}, self.settings.ProtocolVersion)
				if err != nil {
					return nil, err
				}
				defer MessagePoolReturn(authBytes)
				if self.settings.AuthFrameObserver != nil {
					self.settings.AuthFrameObserver(authBytes)
				}

				ws.SetWriteDeadline(time.Now().Add(self.settings.AuthTimeout))
				if err := ws.WriteMessage(websocket.BinaryMessage, authBytes); err != nil {
					return nil, err
				}
				ws.SetReadDeadline(time.Now().Add(self.settings.AuthTimeout))
				if messageType, message, err := ws.ReadMessage(); err != nil {
					return nil, err
				} else {
					// verify the auth echo
					switch messageType {
					case websocket.BinaryMessage:
						if !bytes.Equal(authBytes, message) {
							return nil, fmt.Errorf("Auth response error: bad bytes.")
						}
					default:
						return nil, fmt.Errorf("Auth response error.")
					}
				}
			}

			success = true
			return ws, nil
		}

		// a transport that was connected and just died takes the reconnect
		// fast path (small independent jitter, capped concurrency) instead of
		// the shared serializing staircase; see NextReconnectTime. The release
		// frees the fast-path slot as soon as the dial attempt completes, on
		// every exit path.
		// cancelConnect gives the staircase reservation back if this wait is
		// cancelled before the dial happens; it must NOT be called once the
		// dial proceeds — see NextConnectTime. A pinned transport paces
		// itself and returns no-ops for both.
		connectTime, releaseReconnect, cancelConnect := self.nextDialTime(hadConnection)
		hadConnection = false
		if connectDelay := connectTime.Sub(time.Now()); 0 < connectDelay {
			select {
			case <-self.ctx.Done():
				releaseReconnect()
				cancelConnect()
				return
			case <-self.kickMonitor.NotifyChannel():
				// network changed while waiting to dial: any pacing computed
				// for the old path is meaningless — dial now over the new one
			case <-time.After(connectDelay):
			}
		}

		var ws *websocket.Conn
		var err error
		if self.log.V(2).Enabled() {
			ws, err = TraceWithReturnError(fmt.Sprintf("[t]connect %s", clientId), connect)
		} else {
			ws, err = connect()
		}
		releaseReconnect()
		if err != nil {
			// a canceled dial is local teardown -- this transport or its owner
			// shutting down mid-connect -- not a backend signal. Without this
			// carve-out, closing a multi-client window cancels many transports
			// at once and the burst of canceled dials trips the degraded
			// threshold with fresh timestamps, so the NEXT session starts
			// gated. The contract OOB path makes the same carve-out on
			// client.Done.
			if self.ctx.Err() == nil {
				self.noteDialFailure()
			}
			if ok, suppressed := shouldLogAuthErr(); ok {
				if suppressed > 0 {
					self.log.Infof("[t]auth error %s = %s (%d suppressed)\n", clientId, err, suppressed)
				} else {
					self.log.Infof("[t]auth error %s = %s\n", clientId, err)
				}
			} else if v := self.log.V(1); v.Enabled() {
				// throttled at INFO; -v=1 still shows every attempt
				v.Infof("[t]auth error %s = %s\n", clientId, err)
			}
			select {
			case <-self.ctx.Done():
				return
			case <-self.kickMonitor.NotifyChannel():
				// network changed: the failed dial was likely on the dead
				// path — retry now over the new one instead of waiting out
				// the backoff, and take the reconnect fast path (a network
				// change is a legitimate fresh start, not staircase churn)
				self.noteKick()
				hadConnection = true
				continue
			case <-reconnect.After():
				continue
			}
		}

		// auth succeeded: the backend is reachable
		self.noteDialSuccess()

		c := func() {
			defer ws.Close()

			self.setModeAvailable(TransportModeH1, true)
			defer self.setModeAvailable(TransportModeH1, false)

			handleCtx, handleCancel := context.WithCancel(self.ctx)
			defer handleCancel()

			// The connection owns every worker it starts. Registration happens
			// synchronously before cleanup can Wait, and the outer wrapper keeps
			// panic rescue handlers inside the owned lifetime.
			var connectionWaitGroup sync.WaitGroup
			startConnectionWorker := func(run func(), handlers ...any) {
				connectionWaitGroup.Add(1)
				go func() {
					defer connectionWaitGroup.Done()
					HandleError(run, handlers...)
				}()
			}

			// a network-change kick closes this connection so the loop
			// re-dials over the new path immediately (see Kick). the ws.Close
			// is what unblocks a reader/writer parked in a socket call that
			// handleCancel alone cannot wake.
			kick := self.kickMonitor.NotifyChannel()
			startConnectionWorker(func() {
				select {
				case <-handleCtx.Done():
				case <-kick:
					self.log.Infof("[t]kick: closing connection for re-dial\n")
					handleCancel()
					ws.Close()
				}
			})

			var readCounter atomic.Uint64
			var writeCounter atomic.Uint64
			send := make(chan []byte, self.settings.TransportBufferSize)
			receive := make(chan []byte, self.settings.TransportBufferSize)
			controlSend := make(chan []byte, self.settings.TransportBufferSize)
			var ackPrioritySend chan []byte
			ackPriorityBufferSize := min(
				max(0, self.settings.H1AckPriorityBufferSize),
				max(0, self.settings.TransportBufferSize),
			)
			if 0 < ackPriorityBufferSize {
				ackPrioritySend = make(chan []byte, ackPriorityBufferSize)
			}

			drain := func(c chan []byte) {
				for {
					select {
					case message, ok := <-c:
						if !ok {
							return
						}
						MessagePoolReturn(message)
					default:
						return
					}
				}
			}

			var exportedSend chan []byte
			// note: this should be false in production
			//       it seems better to potentially leak messages than to
			//       have an extra inefficiency on the packet path
			if DebugCloseSend {
				// use zero buffer here so that the transport can stop accepting and not drop messages
				exportedSend = make(chan []byte)
				startConnectionWorker(func() {
					defer func() {
						handleCancel()
						close(send)
						drain(send)
					}()
					for {
						select {
						case <-handleCtx.Done():
							return
						case message, ok := <-exportedSend:
							if !ok {
								return
							}
							select {
							case <-handleCtx.Done():
								MessagePoolReturn(message)
								return
							case send <- message:
							}
						}
					}
				}, func() {
					handleCancel()
					close(send)
					drain(send)
				})
			} else {
				exportedSend = send
			}
			if ackPrioritySend != nil {
				registerH1AckPriorityRoute(exportedSend, ackPrioritySend)
			}

			// the platform can route any destination,
			// since every client has a platform transport
			var sendTransport Transport
			var receiveTransport Transport
			if self.settings.TransportGenerator != nil {
				sendTransport, receiveTransport = self.settings.TransportGenerator()
			} else {
				sendTransport = NewSendGatewayTransportWithType(TransportTypeH1)
				receiveTransport = NewReceiveGatewayTransportWithType(TransportTypeH1)
			}

			self.routeManager.UpdateTransport(sendTransport, []Route{exportedSend})
			if self.settings.SendRouteObserver != nil {
				self.settings.SendRouteObserver(sendTransport, exportedSend, true)
			}
			self.routeManager.UpdateTransportWithProperties(
				receiveTransport,
				[]Route{receive},
				TransferCarrierProperties{
					ReceiveReliability: CarrierReliabilityReliable,
				},
			)
			self.setRegistered(true)
			// the extender carrying this connection is published for exactly
			// its lifetime, so the ips and the directory's in-use count follow
			// the connection rather than the dial (K1, K4)
			releaseExtenderIp := self.holdExtenderIp(dialExtenderIp)

			defer func() {
				self.setRegistered(false)
				releaseExtenderIp()
				// Stop new priority admissions before retiring the public route.
				// RemoveTransport then joins any writer that already acquired the
				// old snapshot, so the final drain cannot race an enqueue.
				unregisterH1AckPriorityRoute(exportedSend)
				self.routeManager.RemoveTransport(sendTransport)
				if self.settings.SendRouteObserver != nil {
					self.settings.SendRouteObserver(sendTransport, exportedSend, false)
				}
				self.routeManager.RemoveTransport(receiveTransport)
				if self.settings.afterRoutesRemovedForTest != nil {
					self.settings.afterRoutesRemovedForTest()
				}
				handleCancel()
				// Close the socket before joining workers. Context cancellation
				// cannot interrupt a goroutine already blocked in socket I/O.
				ws.Close()
				// Route removal joins admitted selector writes. Joining every
				// connection worker then closes the remaining reader, watcher,
				// and socket-writer ownership before completion is published.
				connectionWaitGroup.Wait()
				// No producer can enqueue after the join, so one deterministic
				// drain releases every pooled message still sitting in either lane.
				drain(send)
				drain(ackPrioritySend)
			}()
			startConnectionWorker(func() {
				self.runInactiveDrain(
					handleCtx,
					TransportModeH1,
					1,
					&readCounter,
					&writeCounter,
					handleCancel,
				)
			}, handleCancel)

			startConnectionWorker(func() {
				defer handleCancel()

				speedTest := false
				pingTimer := time.NewTimer(0)
				defer pingTimer.Stop()
				resetWakeupTimer(pingTimer, self.settings.PingTimeout, self.settings.PingTimeout)

				writeMessage := func(message []byte) error {
					err := ws.WriteMessage(websocket.BinaryMessage, message)
					MessagePoolReturn(message)
					if err != nil {
						// note that for websocket a dealine timeout cannot be recovered
						if ok, suppressed := shouldLogWriteErr(); ok {
							if suppressed > 0 {
								self.log.Infof("[ts]%s-> error = %s (%d suppressed)\n", clientId, err, suppressed)
							} else {
								self.log.Infof("[ts]%s-> error = %s\n", clientId, err)
							}
						} else if v := self.log.V(1); v.Enabled() {
							v.Infof("[ts]%s-> error = %s\n", clientId, err)
						}
						return err
					}
					if self.log.V(2).Enabled() {
						self.log.Infof("[ts]%s->\n", clientId)
					}
					return nil
				}
				write := func(message []byte) error {
					ws.SetWriteDeadline(time.Now().Add(self.settings.WriteTimeout))
					return writeMessage(message)
				}
				writePayload := func(message []byte) error {
					if err := write(message); err != nil {
						return err
					}
					writeCounter.Add(1)
					return nil
				}
				writeSendMessage := func(message []byte) error {
					if len(message) <= 16 {
						self.log.Infof("[ts]send message must be >16 bytes (%d)\n", len(message))
						MessagePoolReturn(message)
						return nil
					}
					if err := writeMessage(message); err != nil {
						return err
					}
					writeCounter.Add(1)
					return nil
				}

				writeBatchConn, _ :=
					ws.UnderlyingConn().(*WebSocketWriteBatchConn)
				writeReadySendBatch := func(
					firstMessage []byte,
					firstPriority bool,
				) (sendOpen bool, err error) {
					if writeBatchConn == nil {
						ws.SetWriteDeadline(time.Now().Add(self.settings.WriteTimeout))
						return true, writeSendMessage(firstMessage)
					}

					ws.SetWriteDeadline(time.Now().Add(self.settings.WriteTimeout))
					writeBatchConn.BeginWriteBatch()
					if err = writeSendMessage(firstMessage); err != nil {
						writeBatchConn.AbortWriteBatch()
						return true, err
					}

					sendOpen = true
					batchMessageCount := 1
					batchMessageByteCount := len(firstMessage)
					priorityMessageCount := 0
					if firstPriority {
						priorityMessageCount = 1
					}
				drainReady:
					for platformWebSocketWriteBatchCanDrain(
						batchMessageCount,
						batchMessageByteCount,
					) {
						select {
						case <-handleCtx.Done():
							writeBatchConn.AbortWriteBatch()
							return false, nil
						default:
						}
						message, priority, open, ready :=
							platformWebSocketWriteBatchNextReady(
								ackPrioritySend,
								send,
								priorityMessageCount,
							)
						if !ready {
							break drainReady
						}
						if !open {
							sendOpen = false
							break drainReady
						}
						if err = writeSendMessage(message); err != nil {
							writeBatchConn.AbortWriteBatch()
							return true, err
						}
						batchMessageCount += 1
						batchMessageByteCount += len(message)
						if priority {
							priorityMessageCount += 1
						} else {
							priorityMessageCount = 0
						}
					}
					if err = writeBatchConn.FlushWriteBatch(); err != nil {
						// A WebSocket write timeout or partial TLS write cannot
						// be recovered; the transfer sequence retains each
						// item and retries it over the replacement route.
						self.log.Infof("[ts]%s-> batch flush error = %s\n", clientId, err)
					}
					return
				}

				for {
					// A nonblocking pass makes priority deterministic when the
					// ordinary route is continuously readable. The blocking
					// selects below also include the lane so a newly arriving ACK
					// wakes an otherwise idle writer.
					if ackPrioritySend != nil && !speedTest {
						select {
						case message := <-ackPrioritySend:
							if speedTest {
								if len(message) <= 16 {
									self.log.Infof("[ts]send message must be >16 bytes (%d)\n", len(message))
									MessagePoolReturn(message)
								} else if writePayload(message) != nil {
									return
								}
							} else {
								sendOpen, err := writeReadySendBatch(message, true)
								if err != nil || !sendOpen {
									return
								}
							}
							resetWakeupTimer(pingTimer, self.settings.PingTimeout, self.settings.PingTimeout)
							continue
						default:
						}
					}
					if speedTest {
						// during speed test, continue draining user traffic
						// so the route manager does not back up. mixing user
						// traffic with the speed-test echo slightly reduces
						// measurement accuracy but avoids stalling the client.
						select {
						case <-handleCtx.Done():
							return
						case <-pingTimer.C:
							ws.SetWriteDeadline(time.Now().Add(self.settings.WriteTimeout))
							if err := ws.WriteMessage(websocket.BinaryMessage, make([]byte, 0)); err != nil {
								// note that for websocket a dealine timeout cannot be recovered
								return
							}
							resetWakeupTimer(pingTimer, self.settings.PingTimeout, self.settings.PingTimeout)
						case message, ok := <-controlSend:
							if !ok {
								return
							}
							if len(message) == 5 {
								switch message[0] {
								case TransportControlSpeedStop:
									speedTest = false
								}
							}
							if write(message) != nil {
								return
							}
							resetWakeupTimer(pingTimer, self.settings.PingTimeout, self.settings.PingTimeout)
						case message, ok := <-send:
							if !ok {
								return
							}
							if len(message) <= 16 {
								self.log.Infof("[ts]send message must be >16 bytes (%d)\n", len(message))
								MessagePoolReturn(message)
							} else if writePayload(message) != nil {
								return
							}
							resetWakeupTimer(pingTimer, self.settings.PingTimeout, self.settings.PingTimeout)
						case message := <-ackPrioritySend:
							if len(message) <= 16 {
								self.log.Infof("[ts]send message must be >16 bytes (%d)\n", len(message))
								MessagePoolReturn(message)
							} else if writePayload(message) != nil {
								return
							}
							resetWakeupTimer(pingTimer, self.settings.PingTimeout, self.settings.PingTimeout)
						}
					} else {
						select {
						case <-handleCtx.Done():
							return
						case message, ok := <-send:
							if !ok {
								return
							}
							// if !MessagePoolCheckShared(message) {
							// 	panic("[t]shared should be set")
							// }

							sendOpen, err := writeReadySendBatch(message, false)
							if err != nil || !sendOpen {
								return
							}
							resetWakeupTimer(pingTimer, self.settings.PingTimeout, self.settings.PingTimeout)
						case message := <-ackPrioritySend:
							sendOpen, err := writeReadySendBatch(message, true)
							if err != nil || !sendOpen {
								return
							}
							resetWakeupTimer(pingTimer, self.settings.PingTimeout, self.settings.PingTimeout)
						case <-pingTimer.C:
							ws.SetWriteDeadline(time.Now().Add(self.settings.WriteTimeout))
							if err := ws.WriteMessage(websocket.BinaryMessage, make([]byte, 0)); err != nil {
								// note that for websocket a dealine timeout cannot be recovered
								return
							}
							resetWakeupTimer(pingTimer, self.settings.PingTimeout, self.settings.PingTimeout)
						case message, ok := <-controlSend:
							if !ok {
								return
							}
							if len(message) == 5 {
								switch message[0] {
								case TransportControlSpeedStart:
									speedTest = true
								}
							}
							if write(message) != nil {
								return
							}
							resetWakeupTimer(pingTimer, self.settings.PingTimeout, self.settings.PingTimeout)
						}
					}
				}
			}, handleCancel)

			startConnectionWorker(func() {
				defer func() {
					if self.settings.beforeReceiveWorkerCleanupForTest != nil {
						self.settings.beforeReceiveWorkerCleanupForTest()
					}
					handleCancel()
					close(receive)
					close(controlSend)

					drain(receive)
					drain(controlSend)
				}()
				speedTest := false

				for {
					select {
					case <-handleCtx.Done():
						return
					default:
					}

					ws.SetReadDeadline(time.Now().Add(self.settings.ReadTimeout))
					messageType, r, err := ws.NextReader()
					if err != nil {
						if self.log.V(2).Enabled() {
							self.log.Infof("[tr]%s<- error = %s\n", clientId, err)
						}
						return
					}

					switch messageType {
					case websocket.BinaryMessage:

						message, err := MessagePoolReadAllLimit(r, self.h1MaxMessageByteCount())
						if err != nil {
							if self.log.V(2).Enabled() {
								self.log.Infof("[tr]%s<- error = %s\n", clientId, err)
							}
							return
						}

						if len(message) <= 16 {
							if len(message) == 0 {
								// ping
								if self.log.V(2).Enabled() {
									self.log.Infof("[tr]ping %s<-\n", clientId)
								}
								MessagePoolReturn(message)
							} else if len(message) == 5 {
								switch message[0] {
								case TransportControlSpeedStart:
									speedTest = true
									// echo
									if !self.offerH1Control(handleCtx.Done(), controlSend, message) {
										return
									}
								case TransportControlSpeedStop:
									speedTest = false
									// echo
									if !self.offerH1Control(handleCtx.Done(), controlSend, message) {
										return
									}
								default:
									MessagePoolReturn(message)
								}
							} else if len(message) == 16 {
								// latency test echo
								if !self.offerH1Control(handleCtx.Done(), controlSend, message) {
									return
								}
							} else {
								MessagePoolReturn(message)
							}
							continue
						}
						if speedTest {
							// speed test echo
							if !self.offerH1Control(handleCtx.Done(), controlSend, message) {
								return
							}
							continue
						}
						open, delivered := self.offerReceive(
							handleCtx.Done(),
							TransportModeH1,
							CarrierReliabilityReliable,
							receive,
							message,
						)
						if !open {
							return
						}
						if delivered {
							readCounter.Add(1)
						}
						if delivered && self.log.V(2).Enabled() {
							self.log.Infof("[tr]%s<-\n", clientId)
						}
					default:
						if self.log.V(2).Enabled() {
							self.log.Infof("[tr]other=%v %s<-\n", messageType, clientId)
						}
					}

					// messageType, message, err := ws.ReadMessage()
					// if err != nil {
					// 	self.log.Infof("[tr]%s<- error = %s\n", clientId, err)
					// 	return
					// }

				}
			}, func() {
				handleCancel()
				close(receive)
				close(controlSend)

				drain(receive)
				drain(controlSend)
			})

			select {
			case <-handleCtx.Done():
			}
		}

		reconnect = NewReconnect(self.settings.ReconnectTimeout)
		if self.log.V(2).Enabled() {
			Trace(fmt.Sprintf("[t]connect run %s", clientId), c)
		} else {
			c()
		}
		// the connection ran and died: the next dial is a reconnect
		hadConnection = true

		select {
		case <-self.ctx.Done():
			return
		case <-self.kickMonitor.NotifyChannel():
			// a kick arriving after the connection already died skips the
			// residual backoff — the fast-path re-dial starts now. (the kick
			// that killed the connection closed the previous notify channel;
			// this arm arms a fresh one, so one kick fires exactly once here.)
		case <-reconnect.After():
		}
	}
}

func (self *PlatformTransport) runH3(
	ctx context.Context,
	ptMode TransportMode,
	initialTimeout time.Duration,
	slowMultiple int,
) {
	// connect and update route manager for this transport
	if slowMultiple < 1 {
		panic(fmt.Errorf("Bad slow multiple: %d", slowMultiple))
	}

	if 0 < initialTimeout {
		select {
		case <-ctx.Done():
			return
		case <-time.After(initialTimeout):
		}
	}

	// hadConnection marks the iteration immediately after a connection ran and
	// died: only that first re-dial takes the reconnect fast path
	// (NextReconnectTime); a failed re-dial clears it, so retries fall back to
	// the serialized NextConnectTime pacing.
	hadConnection := false

	for {
		// wait until we are back in the specific pt mode or auto mode
		// stand down while a strictly better mode is active
		func() {
			for {
				standDown, notify := self.standDown(ptMode)
				if !standDown {
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-notify:
				}
			}
		}()
		if ctx.Err() != nil {
			return
		}
		if !self.waitDialAdmission(ctx) {
			return
		}
		auth := self.authSnapshot()
		clientId, _ := auth.ClientId()

		reconnect := NewReconnect(self.settings.ReconnectTimeout)

		type ConnStream struct {
			conn           *quic.Conn
			stream         *quic.Stream
			packetConn     net.PacketConn
			quicTransport  *quic.Transport
			useH3Datagrams bool
		}

		connect := func(attemptCtx context.Context) (*ConnStream, error) {
			// quicConfig := &quic.Config{
			// 	HandshakeIdleTimeout: self.settings.QuicConnectTimeout + self.settings.QuicHandshakeTimeout,
			// }
			authMessage := &protocol.Auth{
				ByJwt:      auth.ByJwt,
				AppVersion: auth.AppVersion,
				InstanceId: auth.InstanceId.Bytes(),
				IpFamily:   self.authIntent(),
			}
			SetH3DatagramAuthOffer(authMessage, self.settings.EnableH3Datagrams)
			authBytes, err := EncodeFrame(authMessage, self.settings.ProtocolVersion)
			if err != nil {
				return nil, err
			}
			defer MessagePoolReturn(authBytes)
			if self.settings.AuthFrameObserver != nil {
				self.settings.AuthFrameObserver(authBytes)
			}

			success := false

			quicConfig := newPlatformQuicConfig(self.settings, slowMultiple)
			var tlsConfig *tls.Config
			if self.settings.QuicTlsConfig != nil {
				// copy
				tlsConfig = self.settings.QuicTlsConfig.Clone()
			} else {
				tlsConfig = &tls.Config{}
			}

			serverName, err := connectHost(self.platformUrl)
			if err != nil {
				return nil, err
			}
			tlsConfig.ServerName = serverName

			// the candidate addresses in dial order, and how each socket is
			// wrapped for the mode. One candidate is dialed directly; several
			// race, v6 first with a stagger (see raceH3Dial).
			candidates, wrapPacketConn, err := self.h3DialCandidates(attemptCtx, ptMode, serverName)
			if err != nil {
				return nil, err
			}
			var attempt *h3DialAttempt
			if len(candidates) == 1 {
				attempt, err = self.dialH3(attemptCtx, ptMode, candidates[0], wrapPacketConn, tlsConfig, quicConfig, slowMultiple, false)
			} else {
				attempt, err = raceH3Dial(attemptCtx, candidates, func(dialCtx context.Context, udpAddr *net.UDPAddr) (*h3DialAttempt, error) {
					return self.dialH3(dialCtx, ptMode, udpAddr, wrapPacketConn, tlsConfig, quicConfig, slowMultiple, true)
				})
			}
			if err != nil {
				return nil, err
			}
			conn := attempt.conn
			packetConn := attempt.packetConn
			quicTransport := attempt.quicTransport
			defer func() {
				if !success {
					attempt.close()
				}
			}()

			stream, err := conn.OpenStreamSync(attemptCtx)
			if err != nil {
				self.log.Infof("[c]h3 open stream err = %s\n", err)
				return nil, err
			}

			framer := NewFramer(self.framerSettings)

			stream.SetWriteDeadline(time.Now().Add(time.Duration(slowMultiple) * self.settings.AuthTimeout))
			if err := framer.Write(stream, authBytes); err != nil {
				return nil, err
			}
			stream.SetReadDeadline(time.Now().Add(time.Duration(slowMultiple) * self.settings.AuthTimeout))
			useH3Datagrams := false
			if responseBytes, err := framer.Read(stream); err != nil {
				return nil, err
			} else {
				defer MessagePoolReturn(responseBytes)
				responseMessage, responseErr := DecodeFrame(responseBytes)
				if responseErr != nil {
					return nil, responseErr
				}
				authResponse, ok := responseMessage.(*protocol.Auth)
				if !ok {
					return nil, fmt.Errorf("Auth response error: got %T.", responseMessage)
				}
				connectionState := conn.ConnectionState()
				useH3Datagrams, responseErr = ValidateH3DatagramAuthResponse(
					authMessage,
					authResponse,
					self.settings.EnableH3Datagrams,
					connectionState.SupportsDatagrams.Local,
					connectionState.SupportsDatagrams.Remote,
				)
				if responseErr != nil {
					return nil, responseErr
				}
			}

			success = true
			return &ConnStream{
				conn:           conn,
				stream:         stream,
				packetConn:     packetConn,
				quicTransport:  quicTransport,
				useH3Datagrams: useH3Datagrams,
			}, nil
		}

		// a transport that was connected and just died takes the reconnect
		// fast path (small independent jitter, capped concurrency) instead of
		// the shared serializing staircase; see NextReconnectTime. The release
		// frees the fast-path slot as soon as the dial attempt completes, on
		// every exit path.
		// cancelConnect gives the staircase reservation back if this wait is
		// cancelled before the dial happens; it must NOT be called once the
		// dial proceeds — see NextConnectTime. A pinned transport paces
		// itself and returns no-ops for both.
		connectTime, releaseReconnect, cancelConnect := self.nextDialTime(hadConnection)
		hadConnection = false
		if connectDelay := connectTime.Sub(time.Now()); 0 < connectDelay {
			select {
			case <-ctx.Done():
				releaseReconnect()
				cancelConnect()
				return
			case <-self.kickMonitor.NotifyChannel():
				// network changed while waiting to dial: any pacing computed
				// for the old path is meaningless — dial now over the new one
			case <-time.After(connectDelay):
			}
		}
		attemptCtx, releaseAttempt, acquired := self.h3Gate.Acquire(ctx, ptMode)
		if !acquired {
			releaseReconnect()
			cancelConnect()
			return
		}
		if standDown, _ := self.standDown(ptMode); standDown {
			releaseAttempt()
			releaseReconnect()
			cancelConnect()
			continue
		}

		var connStream *ConnStream
		var err error
		if self.log.V(2).Enabled() {
			connStream, err = TraceWithReturnError(fmt.Sprintf("[t]connect %s", clientId), func() (*ConnStream, error) {
				return connect(attemptCtx)
			})
		} else {
			connStream, err = connect(attemptCtx)
		}
		releaseReconnect()
		if err != nil {
			attemptCanceled := attemptCtx.Err() != nil
			releaseAttempt()
			// a canceled dial is local teardown -- this transport or its owner
			// shutting down mid-connect -- not a backend signal. Without this
			// carve-out, closing a multi-client window cancels many transports
			// at once and the burst of canceled dials trips the degraded
			// threshold with fresh timestamps, so the NEXT session starts
			// gated. The contract OOB path makes the same carve-out on
			// client.Done.
			if ctx.Err() == nil {
				if !attemptCanceled {
					self.noteDialFailure()
				}
			}
			if ok, suppressed := shouldLogAuthErr(); ok {
				if suppressed > 0 {
					self.log.Infof("[t]auth error %s = %s (%d suppressed)\n", clientId, err, suppressed)
				} else {
					self.log.Infof("[t]auth error %s = %s\n", clientId, err)
				}
			} else if v := self.log.V(1); v.Enabled() {
				// throttled at INFO; -v=1 still shows every attempt
				v.Infof("[t]auth error %s = %s\n", clientId, err)
			}
			select {
			case <-ctx.Done():
				return
			case <-self.kickMonitor.NotifyChannel():
				// network changed: retry now over the new path and take the
				// reconnect fast path (see the h1 loop above)
				self.noteKick()
				hadConnection = true
				continue
			case <-reconnect.After():
				continue
			}
		}

		// auth succeeded: the backend is reachable
		self.noteDialSuccess()

		conn := connStream.conn
		stream := connStream.stream

		c := func() {
			defer connStream.packetConn.Close()
			defer connStream.quicTransport.Close()
			defer conn.CloseWithError(0, "")

			self.setModeAvailable(ptMode, true)
			defer self.setModeAvailable(ptMode, false)

			handleCtx, handleCancel := context.WithCancel(attemptCtx)
			defer handleCancel()

			// The connection owns every worker it starts. Registration happens
			// synchronously before cleanup can Wait, and the outer wrapper keeps
			// panic rescue handlers inside the owned lifetime.
			var connectionWaitGroup sync.WaitGroup
			startConnectionWorker := func(run func(), handlers ...any) {
				connectionWaitGroup.Add(1)
				go func() {
					defer connectionWaitGroup.Done()
					HandleError(run, handlers...)
				}()
			}

			// a network-change kick closes this connection so the loop
			// re-dials over the new path immediately (see Kick). closing the
			// QUIC connection is what unblocks a reader/writer parked in a
			// stream call that handleCancel alone cannot wake.
			kick := self.kickMonitor.NotifyChannel()
			startConnectionWorker(func() {
				select {
				case <-handleCtx.Done():
				case <-kick:
					self.log.Infof("[t]kick: closing connection for re-dial\n")
					handleCancel()
					conn.CloseWithError(0, "network change")
				}
			})

			framer := NewFramer(self.framerSettings)
			var datagramFragmenter *H3DatagramFragmenter
			var datagramReassembler *H3DatagramReassembler
			if connStream.useH3Datagrams {
				var datagramErr error
				datagramFragmenter, datagramErr = NewH3DatagramFragmenter(
					self.h3DatagramSettings,
					self.h3DatagramStats,
				)
				if datagramErr != nil {
					self.log.Infof("[t]H3 DATAGRAM sender init error = %s\n", datagramErr)
					return
				}
				datagramReassembler, datagramErr = NewH3DatagramReassembler(
					self.h3DatagramSettings,
					self.h3DatagramReassemblyBudget,
					self.h3DatagramStats,
				)
				if datagramErr != nil {
					self.log.Infof("[t]H3 DATAGRAM receiver init error = %s\n", datagramErr)
					return
				}
				defer datagramReassembler.Close()
			}

			var readCounter atomic.Uint64
			var writeCounter atomic.Uint64
			// The route selector classifies the exact message accepted by this H3
			// generation. Keep the live QUIC DATAGRAM ceiling atomic because Transfer
			// reads it on its sender goroutine while the carrier writer lowers it
			// after synchronous DatagramTooLarge feedback.
			var maxDatagramByteCount atomic.Int64
			maxDatagramByteCount.Store(
				int64(initialH3DatagramPathByteCount(
					self.h3DatagramSettings.TargetDatagramByteCount,
					conn.SendDatagram,
				)),
			)

			send := make(chan []byte, self.settings.TransportBufferSize)
			// Stream-only H3 retains its historical bounded burst queue. Hybrid H3
			// gives the existing bounded queue to DATAGRAM while the reliable stream
			// route is unbuffered: its reader may retain exactly one already-read
			// frame, so splitting lane metadata cannot double payload retention.
			reliableReceiveBufferSize, unreliableReceiveBufferSize :=
				platformH3ReceiveRouteBufferSizes(
					self.settings.TransportBufferSize,
					connStream.useH3Datagrams,
				)
			reliableReceive := make(chan []byte, reliableReceiveBufferSize)
			var unreliableReceive chan []byte
			if connStream.useH3Datagrams {
				unreliableReceive = make(chan []byte, unreliableReceiveBufferSize)
			}

			drain := func(c chan []byte) {
				for {
					select {
					case message, ok := <-c:
						if !ok {
							return
						}
						MessagePoolReturn(message)
					default:
						return
					}
				}
			}

			// the platform can route any destination,
			// since every client has a platform transport
			var sendTransport Transport
			var receiveTransport Transport
			if self.settings.TransportGenerator != nil {
				sendTransport, receiveTransport = self.settings.TransportGenerator()
			} else {
				transportType := transportTypeFromMode(ptMode)
				sendTransport = NewSendGatewayTransportWithType(transportType)
				receiveTransport = NewReceiveGatewayTransportWithType(transportType)
			}

			sendCarrierProperties := TransferCarrierProperties{}
			if connStream.useH3Datagrams {
				sendCarrierProperties.Unreliable = true
				sendCarrierProperties.UnreliableFlowIsolation = true
				sendCarrierProperties.UnreliableFlowReserve = true
				sendCarrierProperties.unreliableForMessageByteCount = func(messageByteCount int) bool {
					return self.h3DatagramSettings.UseDatagramForPath(
						messageByteCount,
						int(maxDatagramByteCount.Load()),
					)
				}
			}
			self.routeManager.UpdateTransportWithProperties(
				sendTransport,
				[]Route{send},
				sendCarrierProperties,
			)
			if self.settings.SendRouteObserver != nil {
				self.settings.SendRouteObserver(sendTransport, send, true)
			}
			self.routeManager.UpdateTransportWithProperties(
				receiveTransport,
				[]Route{reliableReceive},
				TransferCarrierProperties{
					ReceiveReliability: CarrierReliabilityReliable,
				},
			)
			var unreliableReceiveTransport Transport
			if unreliableReceive != nil {
				unreliableReceiveTransport = newReceiveLaneTransport(receiveTransport)
				self.routeManager.UpdateTransportWithProperties(
					unreliableReceiveTransport,
					[]Route{unreliableReceive},
					TransferCarrierProperties{
						ReceiveReliability: CarrierReliabilityUnreliable,
					},
				)
			}
			self.setRegistered(true)

			defer func() {
				self.setRegistered(false)
				self.routeManager.RemoveTransport(sendTransport)
				if self.settings.SendRouteObserver != nil {
					self.settings.SendRouteObserver(sendTransport, send, false)
				}
				self.routeManager.RemoveTransport(receiveTransport)
				if unreliableReceiveTransport != nil {
					self.routeManager.RemoveTransport(unreliableReceiveTransport)
				}
				if self.settings.afterRoutesRemovedForTest != nil {
					self.settings.afterRoutesRemovedForTest()
				}
				handleCancel()
				// Like the websocket path, break blocked socket I/O before
				// joining every owned connection worker.
				conn.CloseWithError(0, "transport teardown")
				connectionWaitGroup.Wait()
				// Route removal and the worker join leave no producer that can
				// enqueue after these deterministic pooled-message drains.
				drain(send)
				drain(reliableReceive)
				if unreliableReceive != nil {
					drain(unreliableReceive)
				}
			}()
			// Hybrid H3 dispatches the two physical lanes before either writer can
			// block. The extra queue transfers pooled-message ownership under both
			// count and retained-backing-byte limits. Legacy stream-only H3 consumes
			// the published route directly.
			var streamSend chan []byte
			var streamSendBudget *H3HybridStreamSendBudget
			streamInput := (<-chan []byte)(send)
			if connStream.useH3Datagrams {
				streamQueueMessageCount := min(
					H3HybridStreamQueueMessageCount,
					max(1, self.settings.TransportBufferSize),
				)
				streamSend = make(chan []byte, streamQueueMessageCount)
				streamSendBudget = NewH3HybridStreamSendBudget(
					streamQueueMessageCount,
					H3HybridStreamQueueByteCount,
					self.h3DatagramStats,
				)
				streamInput = streamSend
			}
			releaseStreamMessage := func(message []byte) {
				if streamSendBudget != nil {
					streamSendBudget.Release(H3HybridStreamRetainedByteCount(message))
				}
				MessagePoolReturn(message)
			}

			// Allocated only if this generation actually selects the reliable data
			// lane. A small-message-only hybrid keeps the former DATAGRAM memory
			// profile instead of pinning the 64 KiB stream batch eagerly.
			var writeBatchStorage []byte
			writeReadySendBatch := func(
				firstMessage []byte,
			) (sendOpen bool, pendingMessage []byte, err error) {
				var messageStorage [platformH3WriteBatchMaxMessageCount][]byte
				messages := messageStorage[:1]
				messages[0] = firstMessage
				batchByteCount := len(firstMessage) + 4
				sendOpen = true
			drainReady:
				for len(messages) < cap(messages) {
					select {
					case <-handleCtx.Done():
						sendOpen = false
						break drainReady
					case message, ok := <-streamInput:
						if !ok {
							sendOpen = false
							break drainReady
						}
						framedByteCount := len(message) + 4
						if platformH3WriteBatchMaxByteCount < batchByteCount+framedByteCount {
							pendingMessage = message
							break drainReady
						}
						messages = append(messages, message)
						batchByteCount += framedByteCount
					default:
						break drainReady
					}
				}

				stream.SetWriteDeadline(
					time.Now().Add(time.Duration(slowMultiple) * self.settings.WriteTimeout),
				)
				if self.settings.beforeH3StreamWriteForTest != nil {
					self.settings.beforeH3StreamWriteForTest()
				}
				if writeBatchStorage == nil {
					writeBatchStorage = make([]byte, platformH3WriteBatchMaxByteCount)
				}
				err = framer.WriteBatchWithStorage(
					stream,
					messages,
					writeBatchStorage,
				)
				if err == nil {
					writeCounter.Add(uint64(len(messages)))
					for _, message := range messages {
						if connStream.useH3Datagrams {
							self.h3DatagramStats.RecordStreamSent(len(message))
						}
						if self.settings.H3SendLaneObserver != nil {
							self.settings.H3SendLaneObserver(message, false)
						}
					}
				}
				for _, message := range messages {
					releaseStreamMessage(message)
				}
				return
			}
			sendDatagramMessage := func(message []byte) (useStream bool, sendErr error) {
				currentMaxDatagramByteCount := int(maxDatagramByteCount.Load())
				var nextMaxDatagramByteCount int
				useStream, nextMaxDatagramByteCount, sendErr = datagramFragmenter.SendHybrid(
					message,
					currentMaxDatagramByteCount,
					conn.SendDatagram,
				)
				if nextMaxDatagramByteCount != currentMaxDatagramByteCount {
					maxDatagramByteCount.Store(int64(nextMaxDatagramByteCount))
				}
				return useStream, sendErr
			}
			logH3WriteError := func(err error) {
				if ok, suppressed := shouldLogWriteErr(); ok {
					if suppressed > 0 {
						self.log.Infof("[ts]%s-> error = %s (%d suppressed)\n", clientId, err, suppressed)
					} else {
						self.log.Infof("[ts]%s-> error = %s\n", clientId, err)
					}
				} else if v := self.log.V(1); v.Enabled() {
					v.Infof("[ts]%s-> error = %s\n", clientId, err)
				}
			}

			startConnectionWorker(func() {
				self.runInactiveDrain(
					handleCtx,
					ptMode,
					slowMultiple,
					&readCounter,
					&writeCounter,
					handleCancel,
				)
			}, handleCancel)

			startConnectionWorker(func() {
				defer handleCancel()
				if streamSend != nil {
					defer func() {
						for message := range streamSend {
							releaseStreamMessage(message)
						}
					}()
				}

				pingTimer := time.NewTimer(0)
				defer pingTimer.Stop()
				resetWakeupTimer(pingTimer, self.settings.PingTimeout, self.settings.PingTimeout)

				var pendingMessage []byte
				defer func() {
					if pendingMessage != nil {
						releaseStreamMessage(pendingMessage)
					}
				}()
				for {
					message := pendingMessage
					pendingMessage = nil
					if message == nil {
						select {
						case <-handleCtx.Done():
							return
						case nextMessage, ok := <-streamInput:
							if !ok {
								return
							}
							message = nextMessage
						case <-pingTimer.C:
							stream.SetWriteDeadline(time.Now().Add(time.Duration(slowMultiple) * self.settings.WriteTimeout))
							if err := framer.Write(stream, make([]byte, 0)); err != nil {
								return
							}
							resetWakeupTimer(pingTimer, self.settings.PingTimeout, self.settings.PingTimeout)
							continue
						}
					}
					sendOpen, nextMessage, err := writeReadySendBatch(message)
					pendingMessage = nextMessage
					if err != nil {
						logH3WriteError(err)
						return
					}
					if !sendOpen {
						return
					}
					if self.log.V(2).Enabled() {
						self.log.Infof("[ts]%s->stream\n", clientId)
					}
					resetWakeupTimer(
						pingTimer,
						self.settings.PingTimeout,
						self.settings.PingTimeout,
					)
				}
			}, handleCancel)

			if connStream.useH3Datagrams {
				startConnectionWorker(func() {
					defer handleCancel()
					defer close(streamSend)

					offerStream := func(message []byte) bool {
						retainedByteCount := H3HybridStreamRetainedByteCount(message)
						if streamSendBudget.MaxByteCount() < retainedByteCount &&
							len(message) <= streamSendBudget.MaxByteCount()-MessagePoolMetaByteCount {
							compactMessage := MessagePoolCopy(message)
							MessagePoolReturn(message)
							message = compactMessage
							retainedByteCount = H3HybridStreamRetainedByteCount(message)
						}
						if !streamSendBudget.Acquire(handleCtx, retainedByteCount) {
							MessagePoolReturn(message)
							if handleCtx.Err() == nil {
								logH3WriteError(fmt.Errorf(
									"H3 hybrid stream message retained bytes %d exceed queue limit %d",
									retainedByteCount,
									streamSendBudget.MaxByteCount(),
								))
							}
							return false
						}
						select {
						case <-handleCtx.Done():
							streamSendBudget.Release(retainedByteCount)
							MessagePoolReturn(message)
							return false
						case streamSend <- message:
							return true
						}
					}
					for {
						select {
						case <-handleCtx.Done():
							return
						case message, ok := <-send:
							if !ok {
								return
							}
							useDatagram := self.h3DatagramSettings.UseDatagramForPath(
								len(message),
								int(maxDatagramByteCount.Load()),
							)
							if useDatagram {
								useStream, err := sendDatagramMessage(message)
								if err != nil {
									MessagePoolReturn(message)
									logH3WriteError(err)
									return
								}
								if !useStream {
									if self.settings.H3SendLaneObserver != nil {
										self.settings.H3SendLaneObserver(message, true)
									}
									MessagePoolReturn(message)
									writeCounter.Add(1)
									if self.log.V(2).Enabled() {
										self.log.Infof("[ts]%s->datagram\n", clientId)
									}
									continue
								}
							}
							if !offerStream(message) {
								return
							}
						}
					}
				}, handleCancel)
			}

			offerRoutedMessage := func(
				message []byte,
				reliability CarrierReliability,
				receive chan<- []byte,
			) bool {
				open, delivered := self.offerReceive(
					handleCtx.Done(),
					ptMode,
					reliability,
					receive,
					message,
				)
				if delivered {
					readCounter.Add(1)
					if self.settings.afterH3ReceiveEnqueueForTest != nil {
						self.settings.afterH3ReceiveEnqueueForTest(message)
					}
					if self.log.V(2).Enabled() {
						self.log.Infof("[tr]%s<-\n", clientId)
					}
				}
				return open
			}

			if connStream.useH3Datagrams {
				// Authentication, liveness, and routed frames above the hybrid
				// threshold share the reliable stream. The single stream reader
				// remains independent from the DATAGRAM receive pump below. Clear
				// the authentication deadline: DATAGRAM activity is invisible to a
				// stream read deadline, so keeping an application deadline here can
				// tear down an otherwise active QUIC connection when the stream is
				// legitimately idle. QUIC's connection-level idle timeout still
				// detects a dead peer, and closing the connection unblocks this read.
				startConnectionWorker(func() {
					defer func() {
						handleCancel()
						close(reliableReceive)
					}()
					if err := stream.SetReadDeadline(time.Time{}); err != nil {
						return
					}
					for {
						message, err := framer.Read(stream)
						if err != nil {
							return
						}
						if len(message) != 0 {
							self.h3DatagramStats.RecordStreamReceived(len(message))
							if !offerRoutedMessage(
								message,
								CarrierReliabilityReliable,
								reliableReceive,
							) {
								return
							}
							continue
						}
						MessagePoolReturn(message)
						datagramReassembler.Expire(time.Now())
					}
				}, handleCancel)
			}

			startConnectionWorker(func() {
				defer func() {
					if self.settings.beforeReceiveWorkerCleanupForTest != nil {
						self.settings.beforeReceiveWorkerCleanupForTest()
					}
					handleCancel()
					if unreliableReceive != nil {
						close(unreliableReceive)
					} else {
						close(reliableReceive)
					}
				}()

				for {
					select {
					case <-handleCtx.Done():
						return
					default:
					}

					var message []byte
					if connStream.useH3Datagrams {
						datagram, err := conn.ReceiveDatagram(handleCtx)
						if err != nil {
							return
						}
						message = datagramReassembler.Accept(datagram, time.Now())
						if message == nil {
							continue
						}
					} else {
						stream.SetReadDeadline(time.Now().Add(time.Duration(slowMultiple) * self.settings.ReadTimeout))
						var err error
						message, err = framer.Read(stream)
						if err != nil {
							self.log.Infof("[tr]%s<- error = %s\n", clientId, err)
							return
						}
						if 0 == len(message) {
							// ping
							if self.log.V(2).Enabled() {
								self.log.Infof("[tr]ping %s<-\n", clientId)
							}
							MessagePoolReturn(message)
							continue
						}
					}
					reliability := CarrierReliabilityReliable
					receive := (chan<- []byte)(reliableReceive)
					if unreliableReceive != nil {
						reliability = CarrierReliabilityUnreliable
						receive = unreliableReceive
					}
					if !offerRoutedMessage(message, reliability, receive) {
						return
					}
				}
			}, func() {
				handleCancel()
				if unreliableReceive != nil {
					close(unreliableReceive)
				} else {
					close(reliableReceive)
				}
			})

			select {
			case <-handleCtx.Done():
			}
		}
		reconnect = NewReconnect(self.settings.ReconnectTimeout)
		if self.log.V(2).Enabled() {
			Trace(fmt.Sprintf("[t]connect run %s", clientId), c)
		} else {
			c()
		}
		releaseAttempt()
		// the connection ran and died: the next dial is a reconnect
		hadConnection = true

		select {
		case <-ctx.Done():
			return
		case <-self.kickMonitor.NotifyChannel():
			// a kick arriving after the connection already died skips the
			// residual backoff — the fast-path re-dial starts now (see the
			// h1 loop above)
		case <-reconnect.After():
		}
	}
}

func (self *PlatformTransport) Close() {
	self.cancel()
	// unsubscribe eagerly (idempotent; the run loop's deferred unsubscribe
	// also fires) so a NetworkChanged broadcast racing Close cannot kick a
	// transport whose owner already considers it dead.
	if self.unsubNetworkChange != nil {
		self.unsubNetworkChange()
	}
}

// Closes after every mode runner and connection worker has released route,
// socket, channel, and pooled-message ownership. Close remains nonblocking.
func (self *PlatformTransport) Done() <-chan struct{} {
	return self.done
}

// CloseAndWait cancels the transport and joins its owned mode runners. The
// caller's context is only a liveness bound; cancellation continues after it
// returns.
func (self *PlatformTransport) CloseAndWait(ctx context.Context) error {
	self.Close()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-self.done:
		return nil
	}
}

func connectHost(platformUrl string) (string, error) {
	u, err := url.Parse(platformUrl)
	if err != nil {
		return "", err
	}
	return u.Hostname(), nil
}
