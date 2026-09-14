package connect

// UpgradeMux is the concrete IpMux that intercepts local DNS (UDP/TCP 53) — resolving it over DoH
// that egresses the tunnel and recording the IP→hostname reverse index for ServerName path
// affinity — and applies the HTTP (TCP/80) policy: pass through to the egress, or drop. It
// wraps the remote UserNat (the exit path) and is held by the SDK device.

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/netip"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"github.com/urnetwork/connect/v2026/protocol"
)

// HttpUpgradeMode selects how intercepted plaintext HTTP (TCP/80) is handled: passed through to
// the egress unchanged, or dropped. String-valued so it is readable in configs and logs; an
// empty/unrecognized value is treated as Unencrypted (pass-through).
type HttpUpgradeMode string

const (
	// HttpUpgradeUnencrypted passes the HTTP request through to the egress unchanged.
	HttpUpgradeUnencrypted HttpUpgradeMode = "unencrypted"
	// HttpUpgradeBlock drops the request.
	HttpUpgradeBlock HttpUpgradeMode = "block"
)

// HttpUpgradeSettings configures HTTP handling.
type HttpUpgradeSettings struct {
	// Mode is HttpUpgradeUnencrypted (pass plaintext HTTP/80 through to the egress) or
	// HttpUpgradeBlock (drop it). Empty is treated as Unencrypted.
	Mode HttpUpgradeMode
}

func DefaultHttpUpgradeSettings() *HttpUpgradeSettings {
	return &HttpUpgradeSettings{Mode: HttpUpgradeUnencrypted}
}

// DnsUpgradeSettings configures how the mux intercepts and resolves DNS (UDP/TCP 53),
// symmetric with HttpUpgradeSettings.
type DnsUpgradeSettings struct {
	// Resolver is how intercepted queries are resolved (DoH etc.). nil disables DNS
	// interception entirely (queries pass through to the egress).
	Resolver *DnsResolverSettings
	// ResolveTimeout is the single timeout for resolving an intercepted query: it bounds the
	// tunnel-DoH retry loop (the per-query context) and the underlying DoH request alike — the
	// tun's DoH request timeout is derived from it, so they don't diverge. Sized to cover a slow
	// first connect + TLS handshake while a tunnel is still establishing. 0 means no bound (rely
	// on the resolver's own timeout).
	ResolveTimeout time.Duration
	// ResponseTtl is the TTL (seconds) set on synthesized DNS replies.
	ResponseTtl uint32
	// ReverseTtl bounds how long an IP→hostname affinity record (used for ServerName path
	// affinity) is retained after its last use. Refreshed whenever the IP is resolved or
	// looked up; an active maintenance goroutine evicts records idle longer than this so the
	// reverse map does not grow unbounded with every resolved IP. 0 disables eviction.
	ReverseTtl time.Duration
	// ReverseMaxEntries hard-caps the IP→hostname affinity map between TTL sweeps: inserting
	// a new IP at the cap evicts the least-recently-active of a sample first, so a resolve
	// burst cannot grow the map without bound on a memory-constrained host. 0 uses a default.
	ReverseMaxEntries int
	// MaxInflightQueries caps the distinct DNS questions the mux resolves concurrently.
	// Identical concurrent queries (client retransmits, duplicate lookups) coalesce onto one
	// resolution pipeline, so this bounds the pipelines — and with them the mux's burst
	// memory — regardless of client behavior. A claimed query beyond the cap is dropped
	// unanswered; the client retries and lands in a freed slot. 0 uses a default.
	MaxInflightQueries int
	// LocalFallbackTimeout handicaps the local fallback: if the tunnel-DoH (Resolver, egressing
	// the tunnel) hasn't answered a query within this delay, the query is also raced against
	// Fallback (a DoH resolver over the LOCAL host egress). The delay is the handicap — the tunnel
	// result is preferred whenever it arrives first, so the fallback only wins while the tunnel is
	// still establishing. 0 (or a nil Fallback) disables the fallback.
	LocalFallbackTimeout time.Duration
	// ColdLocalFallbackTimeout replaces LocalFallbackTimeout while the tunnel-DoH is COLD —
	// before it has answered anything on this mux (a fresh connect), or after consecutive
	// failures (a mid-session stall; see tunnelDohCold). A short cold handicap keeps the
	// first page load responsive while the tunnel establishes, at the cost of widening the
	// accepted startup DNS-leak window (queries race the local host egress that much
	// sooner); the first tunnel success — usually the connect-time warm probe (WarmDns) —
	// restores the full LocalFallbackTimeout handicap. 0 disables the cold phase (always
	// use LocalFallbackTimeout).
	ColdLocalFallbackTimeout time.Duration
	// ServerStatsSeed, when set, seeds the per-server DoH success stats of the tunnel and
	// fallback resolvers at construction, so the first fan-outs pick the server that was
	// fastest last session instead of uniform-random (see DohSettings.ServerStatsSeed).
	// The owner persists UpgradeMux.DnsServerScores across sessions to produce it.
	ServerStatsSeed map[string]float64
	// Fallback resolves over the local host egress (not the tunnel), used as the handicapped
	// fallback above so DNS stays responsive while the tunnel-DoH is still coming up — preventing
	// the OS from tearing down an apparently-unresponsive tunnel, at the cost of a brief DNS leak
	// during startup. nil disables the fallback.
	Fallback *DnsResolverSettings
	// MemoryTarget, when set, is the owner's live dns byte budget, shared by
	// the mux's tunnel and fallback resolver caches: every in-flight DoH
	// request reserves the response read ceiling from it, so the owner's
	// total in-flight resolution memory tracks the target (see
	// DohSettings.MemoryTarget). The concurrency caps also derive from its
	// capacity. The device stamps its per-instance target here. nil keeps
	// the conservative fixed caps.
	MemoryTarget *MemoryTarget
}

// UpgradeMuxSettings holds the DNS and HTTP upgrade policies. Each consumer (apps,
// server/proxy) sets its own; defaults are conservative.
type UpgradeMuxSettings struct {
	Dns  *DnsUpgradeSettings
	Http *HttpUpgradeSettings
}

// DefaultUpgradeMuxSettings is the app/device default: DNS (UDP/TCP 53) is intercepted and resolved
// over DoH that egresses the tunnel, and plaintext HTTP (TCP/80) is passed through to the egress
// unchanged. While the tunnel-DoH is still establishing on a fresh connect (its connect and TLS
// budgets are tens of seconds), a query the tunnel can't answer within LocalFallbackTimeout is
// raced against a handicapped DoH resolver over the LOCAL host egress (Fallback), so DNS stays
// responsive and the OS doesn't tear the tunnel down — at the cost of a brief DNS leak during
// startup. The tunnel result is always preferred when it arrives first.
//
// For pure pass-through to the egress (the server/proxy use case — no DNS interception,
// no HTTP upgrade), do not install a mux at all: pass nil settings, which avoids a
// per-device tun/stack. A mux with nil Dns still passes DNS through but exists for HTTP.
func DefaultUpgradeMuxSettings() *UpgradeMuxSettings {
	resolver := DefaultDnsResolverSettings()
	return &UpgradeMuxSettings{
		Dns: &DnsUpgradeSettings{
			// DoH only, using the DoH client's configured servers, resolved through the
			// tun (egresses via the exit). Deliberately no EnableLocalDns/EnableLocalDoh
			// (host dialer) or EnableRemoteDns (plaintext :53), which would resolve
			// off-tunnel or in the clear. The remote dns servers are carried for the one
			// permitted plaintext use while EnableRemoteDns is off: resolving a
			// hostname-form doh server name through the tunnel (see DohCache.resolve).
			Resolver: &DnsResolverSettings{
				EnableRemoteDoh:       true,
				DnsUpgradeMaskAddress: resolver.DnsUpgradeMaskAddress,
				RemoteDohUrlsIpv4:     resolver.RemoteDohUrlsIpv4,
				RemoteDohUrlsIpv6:     resolver.RemoteDohUrlsIpv6,
				RemoteDnsIpv4:         resolver.RemoteDnsIpv4,
				RemoteDnsIpv6:         resolver.RemoteDnsIpv6,
			},
			// the tunnel/upstream can take tens of seconds to establish on first connect; the
			// budget must cover a full slow connect (tun dial 30s) plus TLS handshake (30s) so a
			// query racing startup waits for the tunnel rather than failing early.
			ResolveTimeout: 60 * time.Second,
			ResponseTtl:    60,
			// server names are stable and the map is capped + memory-shed, so retain
			// affinity records well past the OS resolver's own cache lifetime: a client
			// that dials from its DNS cache (long-TTL records like pbs.com's 24h) emits no
			// query the mux can record, so a short reverse TTL would idle-evict the name
			// while the client keeps using the ip, blanking the block-action host feed.
			ReverseTtl:         1 * time.Hour,
			ReverseMaxEntries:  defaultReverseMaxEntries,
			MaxInflightQueries: defaultMaxInflightDnsQueries,
			// handicapped local fallback: if the tunnel-DoH hasn't answered within 1s, also
			// resolve over the local host egress. A real multi-origin Android page exposed the
			// former 5s value directly as a 5.29s DNS tail when a stale first-choice DoH server
			// filled the bounded tunnel wave. Healthy tunnel answers measured around 0.2s, so
			// 1s retains a clear tunnel preference while bounding the stalled-provider tail.
			// The same DoH servers are used via the host (EnableLocalDoh), not plaintext DNS.
			LocalFallbackTimeout: 1 * time.Second,
			// while the tunnel-DoH is still unproven (fresh connect / stall), race the local
			// fallback after only 250ms so the first page load doesn't wait multiple seconds
			// per lookup — an accepted widening of the startup leak window, closed again by
			// the first tunnel-DoH success (see ColdLocalFallbackTimeout)
			ColdLocalFallbackTimeout: 250 * time.Millisecond,
			Fallback: &DnsResolverSettings{
				EnableLocalDoh:   true,
				LocalDohUrlsIpv4: resolver.RemoteDohUrlsIpv4,
				LocalDohUrlsIpv6: resolver.RemoteDohUrlsIpv6,
			},
		},
		Http: &HttpUpgradeSettings{Mode: HttpUpgradeUnencrypted},
	}
}

const (
	// dnsTypeSvcb / dnsTypeHttps are the SVCB (64) and HTTPS (65) question types
	// (RFC 9460), not defined by x/net/dns/dnsmessage.
	dnsTypeSvcb  = dnsmessage.Type(64)
	dnsTypeHttps = dnsmessage.Type(65)
	// defaultMaxInflightDnsQueries is the DnsUpgradeSettings.MaxInflightQueries default.
	defaultMaxInflightDnsQueries = 96
	// maxDnsRespondersPerQuestion caps the responders attached to one in-flight question, so
	// a flood of same-question queries with distinct transaction ids stays bounded. A dropped
	// responder's client retries and is answered from the resolver cache.
	maxDnsRespondersPerQuestion = 32
	// defaultReverseMaxEntries is the DnsUpgradeSettings.ReverseMaxEntries default.
	defaultReverseMaxEntries = 4096
	// maxServerNamesPerIp caps the hostnames retained per affinity record (a CDN IP fronting
	// many hosts); the oldest name is dropped for a new one.
	maxServerNamesPerIp = 4
	// reverseEvictSampleSize is how many affinity records an over-cap insert samples to evict
	// the least-recently-active from (approximate LRU, O(sample) instead of a full scan).
	reverseEvictSampleSize = 32
	// tunnelDohColdFailureCount is how many consecutive tunnel-DoH resolution failures flip
	// the mux back to the cold fallback handicap (see DnsUpgradeSettings.ColdLocalFallbackTimeout):
	// one lost race can be a hiccup; a run of losses means the tunnel is stalled.
	tunnelDohColdFailureCount = 2
	// tunnelDohWarmLease is the maximum age of the success that lets a query
	// wait the full warm fallback handicap. A pooled h2 connection and its
	// underlying NAT mapping can die while the browser is idle; treating one
	// historical success as permanent made the next public-page lookup wait
	// five seconds before trying the host fallback. Expiry is checked only
	// when DNS work arrives, so it adds no idle ticker or radio wakeup.
	tunnelDohWarmLease = 30 * time.Second
	// The cold-path prober backs off from 2s to 5m between failed attempts.
	// Each attempt already occupies the full bounded DoH request timeout; a
	// fixed 2s pause therefore kept a dead tunnel almost continuously active.
	// A real query can prove recovery immediately, and NetworkChanged wakes the
	// prober, so the long steady-state cap does not sit on an active path.
	dnsColdProbeInitialInterval = 2 * time.Second
	dnsColdProbeMaxInterval     = 5 * time.Minute
	// muxDohHttpWaveSize bounds the number of DoH HTTP/2 streams admitted
	// onto the device's one shared tunnel at once. A browser origin wave is
	// commonly A + AAAA + HTTPS for several names in parallel; admitting 32
	// requests let those records (and their server hedges) compete with the
	// page's TCP handshakes and payload on the same P2P congestion window.
	// Twelve keeps four complete three-record origins moving per wave without
	// turning the resolver into a second bulk workload.
	muxDohHttpWaveSize = 12
	// muxFallbackDohHttpWaveSize lets one complete browser origin bundle wave
	// use the local rescue path. Unlike tunnel DoH, these requests do not
	// share the selected peer's congestion window; the shared MemoryTarget
	// remains the byte governor. Eight slots made a measured post-connect
	// fallback cohort queue for another 1.4s after its 1s handicap expired.
	muxFallbackDohHttpWaveSize = 12
	// muxDohWarmServerStagger is deliberately above a healthy selected-peer
	// DNS RTT. The former 100 ms delay fired a redundant second-server request
	// for nearly every real-device query once a multi-origin page put modest
	// load on the shared pipe. The first interactive lookup still races
	// immediately through DohServerRaceMaxInFlight below.
	muxDohWarmServerStagger = 350 * time.Millisecond
	// maxDnsTcpConnections bounds locally terminated TCP/53 connections. DNS
	// TCP is a truncation fallback, so a small cap is enough even for a browser
	// fan-out and keeps gVisor sockets, goroutines, and query buffers bounded.
	maxDnsTcpConnections = 8
	// maxDnsTcpFlows bounds the DNAT/SNAT identity map independently of the
	// accepted-connection cap; stale half-open SYNs therefore stay bounded.
	maxDnsTcpFlows = 32
	// maxDnsTcpQueryBytes admits normal DNS + EDNS queries without permitting
	// a 64 KiB allocation per accepted client. DoH responses remain separately
	// bounded by maxDohResponseBytes.
	maxDnsTcpQueryBytes = 4 * 1024
	dnsTcpFlowTtl       = 2 * time.Minute
	dnsTcpIoTimeout     = 15 * time.Second
)

type dnsTcpFlowKey struct {
	clientAddr netip.Addr
	clientPort uint16
}

type dnsTcpFlow struct {
	serverAddr netip.Addr
	lastActive time.Time
}

type UpgradeMux struct {
	ctx    context.Context
	cancel context.CancelFunc

	mux      *IpMux
	settings atomic.Pointer[UpgradeMuxSettings]

	// blocker, when set and enabled, blocks resolution of ad/tracking
	// hostnames in handleDns. installed by the device via SetBlocker —
	// deliberately not part of the swappable settings, so SetSettings
	// cannot clear it.
	blocker atomic.Pointer[Blocker]

	// ipv6Unroutable, when set and true, answers every AAAA query with an
	// empty NOERROR without an upstream lookup (IPV6.md B6): the windows have
	// formed and no exit can carry v6, so a v6 address would only send the
	// app into a blackholed connect until its own fallback fires, which many
	// apps lack. The multi client owns the answer; the device installs it
	// through SetIpv6Unroutable, outside the swappable settings like the
	// blocker. A NOERROR with no records (not NXDOMAIN) keeps the A answer
	// for the same name valid.
	ipv6Unroutable atomic.Pointer[func() bool]

	// fallbackDohCache resolves over the local host egress (not the tun); the handicapped local
	// fallback used when the tunnel-DoH is slow to come up. nil when no Fallback is configured.
	fallbackDohCache atomic.Pointer[DohCache]

	// tunnelDohGeneration changes whenever the underlying path or resolver
	// configuration changes. Success/failure completions carry the generation
	// they started on, so a late callback from a retired path cannot prove or
	// poison the replacement. Readers remain lock-free; the small state lock
	// serializes only DNS completion/path-change writes.
	tunnelDohStateLock         sync.Mutex
	tunnelDohGeneration        atomic.Uint64
	tunnelDohProvenGeneration  atomic.Uint64
	tunnelDohFailureGeneration atomic.Uint64
	tunnelDohLastSuccessNanos  atomic.Int64
	tunnelDohFailures          atomic.Int32

	// dnsProberRunning guards the single cold-phase warm-probe goroutine.
	// Explicit warm/network-change requests are coalesced through one buffered
	// edge; retries use bounded exponential backoff.
	dnsProberRunning      atomic.Bool
	dnsProbeRequested     atomic.Bool
	dnsProberWake         chan struct{}
	dnsProbeInitialDelay  time.Duration
	dnsProbeMaxDelay      time.Duration
	tunnelDohWarmFunction func(context.Context, int) bool

	// The local fallback has the same single-worker/coalescing requirement:
	// repeated NetworkChanged/SetSettings calls can request one follow-up warm,
	// but cannot accumulate one goroutine and request budget apiece.
	fallbackDohWarmerRunning atomic.Bool
	fallbackDohWarmPending   atomic.Bool
	fallbackDohWarmFunction  func(context.Context, *DohCache, int) bool

	// firstLoad measures the first flows after this mux's construction (one mux per
	// connect): dns query→answer, tcp syn→synack, first payload byte. Self-deactivating —
	// after the first-load window the per-packet cost is one atomic load (see
	// firstLoadTimeline).
	firstLoad *firstLoadTimeline

	// source/provideMode stamp packets the mux injects downstream (DNS replies).
	source      TransferPath
	provideMode protocol.ProvideMode

	// unregisterShed removes this mux from the memory-shedder registry on Close.
	unregisterShed func()

	// inflight coalesces claimed DNS queries by question: one resolution pipeline per
	// distinct in-flight question, fanning the answer out to the attached responders. Burst
	// memory then scales with distinct questions (capped by MaxInflightQueries), not with
	// claimed packets (client stub resolvers retransmit unanswered queries every ~1s).
	inflightLock sync.Mutex
	inflight     map[DohKey]*dnsFlight

	// dnsTcp locally terminates client TCP/53 after DNAT to the internal Tun.
	// The flow table remembers the advertised destination so stack replies can
	// be SNATed back before downstream delivery. Both accepted connections and
	// remembered flows are hard-capped; DNS-over-TCP is a rare truncation
	// fallback and must not create an unbounded per-client surface.
	// one listener per family the internal stack has a local address for;
	// the v6 listener is optional (a stack with no v6 address fails v6
	// DNS-over-TCP closed rather than leaking it)
	dnsTcpListeners []net.Listener
	dnsTcpLocalAddr netip.Addr
	// dnsTcpLocalAddr6 is the zero Addr when the stack has no v6 address
	dnsTcpLocalAddr6 netip.Addr
	dnsTcpSem        chan struct{}
	dnsTcpLock       sync.Mutex
	dnsTcpFlows      map[dnsTcpFlowKey]dnsTcpFlow

	// reverse maps a resolved IP to the hostname(s) the mux served for it, for the
	// multi-client's ServerName path affinity (point 4) and block-action server-name
	// reporting. Self-contained + independently testable (see reverseIndex); the mux
	// records DoH resolutions into it, drives its idle eviction from run(), and its
	// ServerNameLookup / ServerNamesLearnedNotifier methods delegate to it.
	reverse *reverseIndex

	// sni passively captures the TLS SNI of egress TCP/443 ClientHellos into the reverse
	// index, naming flows the DNS path can't (a client dialing an ip from its own DNS
	// cache emits no query the mux sees). Observation only — the flow always passes
	// through. Self-contained + independently testable (see sniSniffer).
	sni *sniSniffer

	// upstreamMultiClient resolves a successful DoH connection's exact socket
	// tuple to the provider that carried it. Installed with the batch upstream;
	// nil for generic/server uses where provider affinity is not meaningful.
	upstreamMultiClient atomic.Pointer[RemoteUserNatMultiClient]
}

// dnsResolverSettings extracts the resolver config from the mux settings (nil = no DNS
// interception).
func dnsResolverSettings(settings *UpgradeMuxSettings) *DnsResolverSettings {
	if settings != nil && settings.Dns != nil {
		return settings.Dns.Resolver
	}
	return nil
}

// fallbackResolverSettings extracts the local-egress fallback resolver config from the mux
// settings (nil = no fallback).
func fallbackResolverSettings(settings *UpgradeMuxSettings) *DnsResolverSettings {
	if settings != nil && settings.Dns != nil {
		return settings.Dns.Fallback
	}
	return nil
}

// dnsUpgradeMemoryTarget extracts the owner's dns byte budget from the mux
// settings (nil = no byte bound).
func dnsUpgradeMemoryTarget(settings *UpgradeMuxSettings) *MemoryTarget {
	if settings != nil && settings.Dns != nil {
		return settings.Dns.MemoryTarget
	}
	return nil
}

// dnsUpgradeServerStatsSeed extracts the persisted per-server score seed from the mux
// settings (nil = no seed).
func dnsUpgradeServerStatsSeed(settings *UpgradeMuxSettings) map[string]float64 {
	if settings != nil && settings.Dns != nil {
		return settings.Dns.ServerStatsSeed
	}
	return nil
}

// dohRequestTimeout is the per-DoH-request timeout derived from the mux's resolve budget: a single
// ResolveTimeout bounds both the resolve retry loop and each underlying DoH request. 0 (no Dns, or
// no bound) lets the tun fall back to its default.
func dohRequestTimeout(settings *UpgradeMuxSettings) time.Duration {
	if settings != nil && settings.Dns != nil {
		return settings.Dns.ResolveTimeout
	}
	return 0
}

// buildFallbackDohCache builds a DoH resolver that egresses the LOCAL host network (not the tun),
// used as the handicapped local fallback when the tunnel-DoH is slow to come up. A nil rs (no
// Fallback configured) disables it. The resolver dials the host net dialer (DefaultDohSettings
// leaves DialContextSettings nil) and queries rs's local DoH servers (EnableLocalDoh).
func buildFallbackDohCache(rs *DnsResolverSettings, memoryTarget *MemoryTarget, serverStatsSeed map[string]float64) *DohCache {
	if rs == nil {
		return nil
	}
	dohSettings := DefaultDohSettings()
	dohSettings.DnsResolverSettings = rs
	// Seed the fallback from the tunnel ranking as the only persisted
	// baseline available at construction. The fallback learns independently
	// after that: DNS anycast routing and RTT can differ substantially between
	// the host egress and the selected tunnel provider. DnsServerScores
	// deliberately does not feed those local-path results back into the next
	// tunnel ranking.
	dohSettings.ServerStatsSeed = serverStatsSeed
	// the fallback only bridges tunnel startup; keep its in-flight footprint and cache small
	// so it adds little to the (memory-constrained) extension on top of the primary
	// tunnel-DoH cache. with a dns memory target set the count caps open up — the fallback
	// then draws from the same owner byte target as the primary (see DohSettings.MemoryTarget)
	// — but stay wave-capped like the primary (the shared-pipe bound; see NewUpgradeMux).
	dohSettings.MemoryTarget = memoryTarget
	fallbackHttpConcurrency := min(muxFallbackDohHttpWaveSize, dnsTargetHttpConcurrency(memoryTarget.Capacity(), 4))
	dohSettings.MaxConcurrentHttpRequests = fallbackHttpConcurrency
	dohSettings.MaxConcurrentResolutions = 2 * fallbackHttpConcurrency
	dohSettings.MaxServersPerQuery = 2
	dohSettings.CacheMaxEntries = dnsTargetCacheEntries(memoryTarget.Capacity(), 1024) / 4
	return NewDohCache(dohSettings)
}

func NewUpgradeMux(
	ctx context.Context,
	source TransferPath,
	provideMode protocol.ProvideMode,
	sendTimeout time.Duration,
	initialReceiver ReceivePacketFunction,
	settings *UpgradeMuxSettings,
	log Logger,
) (*UpgradeMux, error) {
	cancelCtx, cancel := context.WithCancel(ctx)
	tunSettings := DefaultTunSettings()
	tunSettings.Log = log
	// the DoH connections run inside a memory-constrained host (notably the iOS network
	// extension). The gVisor TCP buffers size the shared h2 pipe that ALL concurrent DoH
	// streams multiplex over — 64KB (up from 16KB) carries a page-load wave of responses
	// without head-of-line queueing on the connection, for ~100KB per (few) connections,
	// covered by the dns share of the device memory target.
	tunSettings.TcpReceiveBuffer = TcpBufferRange{Min: 4 * 1024, Default: 64 * 1024, Max: 64 * 1024}
	tunSettings.TcpSendBuffer = TcpBufferRange{Min: 4 * 1024, Default: 64 * 1024, Max: 64 * 1024}
	tunSettings.UdpReceiveBufferByteCount = 32 * 1024
	tunSettings.UdpSendBufferByteCount = 32 * 1024
	// this stack only carries DoH resolution traffic, so the endpoint queue can be much
	// smaller than the data-plane default (1024), which bounds a stack-emit burst
	tunSettings.ChannelSize = 128
	// a single dial per attempt: the DoH servers are anycast and reliable, and each raced
	// dial holds an extra gVisor endpoint (and its goroutines) for up to the dial timeout
	// while the tunnel is still establishing
	tunSettings.DialRace = 1
	self := &UpgradeMux{
		ctx:                  cancelCtx,
		cancel:               cancel,
		source:               source,
		provideMode:          provideMode,
		inflight:             map[DohKey]*dnsFlight{},
		dnsTcpSem:            make(chan struct{}, maxDnsTcpConnections),
		dnsTcpFlows:          map[dnsTcpFlowKey]dnsTcpFlow{},
		dnsProberWake:        make(chan struct{}, 1),
		dnsProbeInitialDelay: dnsColdProbeInitialInterval,
		dnsProbeMaxDelay:     dnsColdProbeMaxInterval,
	}
	self.markTunnelDohUnproven()
	self.settings.Store(settings)
	// the reverse index reads its cap from the live settings (reverseMaxEntries), so a
	// SetSettings change to ReverseMaxEntries applies without rebuilding the index
	self.reverse = newReverseIndex(self.reverseMaxEntries)
	// captured TLS SNIs feed the same reverse index as DNS resolutions (firing the
	// learned callbacks that invalidate stale block-action decisions)
	self.sni = newSniSniffer(func(dstAddr netip.Addr, serverName string) {
		self.reverse.record([]netip.Addr{dstAddr}, serverName)
	})
	// bound the resolver cache and fan-out below the server/proxy defaults (see
	// DefaultDohSettings); the mux resolves a device's queries, not a data plane's.
	// with a dns memory target set (the device's per-instance target) the count
	// caps derive from it, BUT http concurrency is additionally capped at a wave
	// size the shared h2 pipe can actually carry: all concurrent streams multiplex
	// over one tls connection per server through the (possibly cold) tunnel, and
	// an uncapped burst (a first page load) queues on that pipe instead of
	// completing in waves — measured as real first-load slowness on device. The
	// byte budget remains the memory governor. (see DohSettings.MemoryTarget)
	var dnsMemoryTarget *MemoryTarget
	var serverStatsSeed map[string]float64
	if settings != nil && settings.Dns != nil {
		dnsMemoryTarget = settings.Dns.MemoryTarget
		serverStatsSeed = settings.Dns.ServerStatsSeed
	}
	dohSettings := DefaultDohSettings()
	dohSettings.MemoryTarget = dnsMemoryTarget
	// Keep the conservative 750 ms server stagger while the tunnel is
	// forming or has gone stale. Once a real query/warm probe proves the
	// shared path, the resolver uses the shorter warm stagger and reserves a
	// few HTTP slots for those hedges (see DohSettings).
	dohSettings.DohPathWarm = func() bool {
		return !self.tunnelDohCold()
	}
	dohSettings.DohServerWarmStagger = muxDohWarmServerStagger
	// Only the leading interactive lookup bypasses the stagger. A synchronized
	// browser wave must not make its first four questions each fan out to two
	// servers before the shared active-query counter becomes visible.
	dohSettings.DohServerRaceMaxInFlight = 1
	dohSettings.DohServerHedgeReserve = 2
	// carry the last session's per-server ordering into the first fan-outs
	dohSettings.ServerStatsSeed = serverStatsSeed
	dohSettings.CacheMaxEntries = dnsTargetCacheEntries(dnsMemoryTarget.Capacity(), 1024)
	muxHttpConcurrency := min(muxDohHttpWaveSize, dnsTargetHttpConcurrency(dnsMemoryTarget.Capacity(), 8))
	dohSettings.MaxConcurrentResolutions = 2 * muxHttpConcurrency
	dohSettings.MaxConcurrentHttpRequests = muxHttpConcurrency
	dohSettings.MaxServersPerQuery = 2
	// record doh server name resolutions into the ip→hostname reverse index,
	// so the block action ignore matcher (which lists the server names)
	// also matches the resolved server addresses
	dohSettings.DohServerResolvedCallback = func(domain string, addrs []netip.Addr) {
		self.reverse.record(addrs, domain)
	}
	dohSettings.DohResultCallback = func(domain string, addrs []netip.Addr, route *DohRoute) {
		self.bindDnsResultToExit(domain, addrs, route)
	}
	tunSettings.DohSettings = dohSettings
	// ResolveTimeout is the single DNS-resolution timeout: it bounds each handleDns attempt (the
	// query context) and the underlying DoH request through the tun alike. SetSettings re-derives
	// it too, so a runtime settings change updates both.
	tunSettings.DohRequestTimeout = dohRequestTimeout(settings)
	tun, err := CreateTunWithResolver(cancelCtx, tunSettings, dnsResolverSettings(settings))
	if err != nil {
		cancel()
		return nil, err
	}
	self.fallbackDohCache.Store(buildFallbackDohCache(fallbackResolverSettings(settings), dnsMemoryTarget, serverStatsSeed))
	// one mux per connect, so the first-load timeline's activation is the connect start
	self.firstLoad = newFirstLoadTimeline(log)
	self.mux = NewIpMux(cancelCtx, tun, source, provideMode, sendTimeout, self.onSend, self.onPump, initialReceiver, log)
	self.mux.setOnSendGroup(self.onSendGroup)
	self.tunnelDohWarmFunction = func(ctx context.Context, serverCount int) bool {
		return self.mux.Tun().DohCache().Warm(ctx, serverCount)
	}
	self.fallbackDohWarmFunction = func(ctx context.Context, cache *DohCache, serverCount int) bool {
		return cache.Warm(ctx, serverCount)
	}
	if err := self.startDnsTcpServer(tun); err != nil {
		self.mux.Close()
		cancel()
		return nil, err
	}
	// drop recoverable caches when the host signals memory pressure
	self.unregisterShed = AddMemoryShedder(self.ShedMemory)
	// active maintenance: TTL-evict the IP→hostname affinity map so it doesn't grow unbounded
	go HandleError(self.run)
	return self, nil
}

// startDnsTcpServer listens on port 53 of the internal stack's first v4
// address (required) and first v6 address (when the stack has one), so a
// truncated query over either family is redirected to a server of the same
// family. Redirected flows keep their original family end to end.
func (self *UpgradeMux) startDnsTcpServer(tun *Tun) error {
	for _, addr := range tun.LocalAddresses() {
		if addr.Is4() && self.dnsTcpLocalAddr.IsValid() ||
			addr.Is6() && self.dnsTcpLocalAddr6.IsValid() {
			continue
		}
		listener, err := tun.ListenTCP(&net.TCPAddr{
			IP:   net.IP(addr.AsSlice()),
			Port: 53,
		})
		if err != nil {
			for _, listener := range self.dnsTcpListeners {
				listener.Close()
			}
			self.dnsTcpListeners = nil
			return fmt.Errorf("listen on internal dns tcp address %s: %w", addr, err)
		}
		if addr.Is4() {
			self.dnsTcpLocalAddr = addr
		} else {
			self.dnsTcpLocalAddr6 = addr
		}
		self.dnsTcpListeners = append(self.dnsTcpListeners, listener)
		go HandleError(func() {
			self.serveDnsTcp(listener)
		})
	}
	if !self.dnsTcpLocalAddr.IsValid() {
		for _, listener := range self.dnsTcpListeners {
			listener.Close()
		}
		self.dnsTcpListeners = nil
		return fmt.Errorf("internal dns tcp server has no IPv4 address")
	}
	return nil
}

func (self *UpgradeMux) serveDnsTcp(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		select {
		case self.dnsTcpSem <- struct{}{}:
			go HandleError(func() {
				defer func() { <-self.dnsTcpSem }()
				self.handleDnsTcpConnection(conn)
			})
		default:
			// A bounded refusal is preferable to queueing arbitrary accepted
			// sockets and their gVisor buffers. The client can retry after an
			// existing truncation fallback completes.
			conn.Close()
		}
	}
}

func (self *UpgradeMux) handleDnsTcpConnection(conn net.Conn) {
	defer conn.Close()
	var lengthBytes [2]byte
	for {
		if err := conn.SetDeadline(time.Now().Add(dnsTcpIoTimeout)); err != nil {
			return
		}
		if _, err := io.ReadFull(conn, lengthBytes[:]); err != nil {
			return
		}
		queryByteCount := int(binary.BigEndian.Uint16(lengthBytes[:]))
		if queryByteCount < 12 || maxDnsTcpQueryBytes < queryByteCount {
			return
		}
		query := make([]byte, queryByteCount)
		if _, err := io.ReadFull(conn, query); err != nil {
			return
		}
		response := self.resolveDnsTcpQuery(query)
		if len(response) == 0 || 0xffff < len(response) {
			return
		}
		frame := make([]byte, 2+len(response))
		binary.BigEndian.PutUint16(frame[:2], uint16(len(response)))
		copy(frame[2:], response)
		for 0 < len(frame) {
			n, err := conn.Write(frame)
			if err != nil || n <= 0 {
				return
			}
			frame = frame[n:]
		}
	}
}

func (self *UpgradeMux) resolveDnsTcpQuery(query []byte) []byte {
	var parser dnsmessage.Parser
	header, err := parser.Start(query)
	if err != nil || header.Response {
		return nil
	}
	question, err := parser.Question()
	if err != nil {
		return nil
	}
	domain := strings.ToLower(strings.TrimSuffix(question.Name.String(), "."))
	responder := &dnsResponder{id: header.ID, question: question}

	// resolver.arpa is a locally served special-use zone (RFC 9462 §6.1).
	// A tunnel DNS forwarder must not ask an unrelated upstream resolver to
	// designate encrypted DNS on its behalf. We expose no OS-level designated
	// resolver—the mux already upgrades ordinary DNS internally—so answer every
	// name and type in the zone with prompt NODATA.
	if isResolverArpaDomain(domain) {
		response, _ := buildDnsResponse(header.ID, question, nil, 0)
		return response
	}

	if blocker := self.getBlocker(); blocker != nil && blocker.BlockHost(domain) {
		var responseTtl uint32
		if settings := self.settings.Load(); settings != nil && settings.Dns != nil {
			responseTtl = settings.Dns.ResponseTtl
		}
		var addrs []netip.Addr
		switch question.Type {
		case dnsmessage.TypeA:
			addrs = []netip.Addr{netip.IPv4Unspecified()}
		case dnsmessage.TypeAAAA:
			addrs = []netip.Addr{netip.IPv6Unspecified()}
		}
		response, _ := buildDnsResponse(header.ID, question, addrs, responseTtl)
		return response
	}

	timeout := dnsTcpIoTimeout
	if settings := self.settings.Load(); settings != nil && settings.Dns != nil &&
		0 < settings.Dns.ResolveTimeout {
		timeout = min(timeout, settings.Dns.ResolveTimeout)
	}
	queryCtx, cancel := context.WithTimeout(self.ctx, timeout)
	defer cancel()

	key := NewDohKey(question.Type.String(), domain)
	self.firstLoad.dnsStart(key)
	tunnelDohGeneration := self.tunnelDohGeneration.Load()
	response, ok := self.mux.Tun().DohCache().Forward(queryCtx, question.Type, domain)
	self.firstLoad.dnsDone(key, ok)
	if !ok {
		if tunnelDohColdFailureCount <= self.recordTunnelDohFailureForGeneration(tunnelDohGeneration) {
			self.ensureColdProber()
		}
		failure, _ := buildDnsStatusResponse(header.ID, question, dnsmessage.RCodeServerFailure, false)
		return failure
	}
	self.markTunnelDohProvenForGeneration(tunnelDohGeneration)

	switch question.Type {
	case dnsmessage.TypeA, dnsmessage.TypeAAAA:
		result := parseDohWire(response, question.Type)
		addrs := make([]netip.Addr, 0, len(result.AddrTtls))
		for addr := range result.AddrTtls {
			addrs = append(addrs, addr)
		}
		if 0 < len(addrs) {
			self.reverse.record(addrs, domain)
		}
	case dnsTypeSvcb, dnsTypeHttps:
		if hints := parseHttpsHints(response); 0 < len(hints) {
			self.reverse.record(hints, domain)
		}
	}

	patched, ok := dnsResponseForResponder(response, responder)
	if !ok {
		failure, _ := buildDnsStatusResponse(header.ID, question, dnsmessage.RCodeServerFailure, false)
		return failure
	}
	return patched
}

func dnsTcpFlowKeyFrom(ip net.IP, port int) (dnsTcpFlowKey, bool) {
	addr, ok := netIPAddr(ip)
	if !ok || port < 0 || 0xffff < port {
		return dnsTcpFlowKey{}, false
	}
	return dnsTcpFlowKey{
		clientAddr: addr,
		clientPort: uint16(port),
	}, true
}

// dnsTcpLocalAddrFor is the internal server address for a client's family,
// or the zero Addr when the stack has none for that family.
func (self *UpgradeMux) dnsTcpLocalAddrFor(ipVersion int) netip.Addr {
	switch ipVersion {
	case 4:
		return self.dnsTcpLocalAddr
	case 6:
		return self.dnsTcpLocalAddr6
	default:
		return netip.Addr{}
	}
}

func (self *UpgradeMux) rememberDnsTcpFlow(ipPath *IpPath) bool {
	key, ok := dnsTcpFlowKeyFrom(ipPath.SourceIp, ipPath.SourcePort)
	serverAddr, serverOk := netIPAddr(ipPath.DestinationIp)
	if !ok || !serverOk || key.clientAddr.Is4() != serverAddr.Is4() {
		return false
	}
	now := time.Now()
	self.dnsTcpLock.Lock()
	defer self.dnsTcpLock.Unlock()

	if _, exists := self.dnsTcpFlows[key]; !exists && maxDnsTcpFlows <= len(self.dnsTcpFlows) {
		var oldestKey dnsTcpFlowKey
		var oldestTime time.Time
		foundOldest := false
		for candidate, flow := range self.dnsTcpFlows {
			if dnsTcpFlowTtl < now.Sub(flow.lastActive) {
				delete(self.dnsTcpFlows, candidate)
				continue
			}
			if !foundOldest || flow.lastActive.Before(oldestTime) {
				oldestKey = candidate
				oldestTime = flow.lastActive
				foundOldest = true
			}
		}
		if maxDnsTcpFlows <= len(self.dnsTcpFlows) && foundOldest {
			delete(self.dnsTcpFlows, oldestKey)
		}
	}
	self.dnsTcpFlows[key] = dnsTcpFlow{
		serverAddr: serverAddr,
		lastActive: now,
	}
	return true
}

func (self *UpgradeMux) dnsTcpServerForClient(ipPath *IpPath) (netip.Addr, bool) {
	key, ok := dnsTcpFlowKeyFrom(ipPath.DestinationIp, ipPath.DestinationPort)
	if !ok {
		return netip.Addr{}, false
	}
	now := time.Now()
	self.dnsTcpLock.Lock()
	defer self.dnsTcpLock.Unlock()
	flow, ok := self.dnsTcpFlows[key]
	if !ok {
		return netip.Addr{}, false
	}
	if dnsTcpFlowTtl < now.Sub(flow.lastActive) {
		delete(self.dnsTcpFlows, key)
		return netip.Addr{}, false
	}
	flow.lastActive = now
	self.dnsTcpFlows[key] = flow
	if ipPath.Rst {
		delete(self.dnsTcpFlows, key)
	}
	return flow.serverAddr, true
}

// rewriteDnsTcpAddress changes one address of a non-fragmented TCP packet
// to `address`, which must be of the packet's own family, and recomputes the
// checksums (the v4 header checksum and the transport checksum with the
// family's pseudo header). A v6 packet may carry extension headers; a v6
// fragment cannot be rewritten. Callers own and may mutate packet.
func rewriteDnsTcpAddress(packet []byte, address netip.Addr, source bool) bool {
	if len(packet) == 0 {
		return false
	}
	switch packet[0] >> 4 {
	case 4:
		if !address.Is4() || len(packet) < Ipv4HeaderSizeWithoutExtensions {
			return false
		}
		headerByteCount := int(packet[0]&0x0f) * 4
		totalByteCount := int(binary.BigEndian.Uint16(packet[2:4]))
		if headerByteCount < Ipv4HeaderSizeWithoutExtensions ||
			totalByteCount < headerByteCount+TcpHeaderSizeWithoutExtensions ||
			len(packet) < totalByteCount ||
			packet[9] != byte(ipProtocolNumberTcp) ||
			binary.BigEndian.Uint16(packet[6:8])&0x3fff != 0 {
			return false
		}
		address4 := address.As4()
		if source {
			copy(packet[12:16], address4[:])
		} else {
			copy(packet[16:20], address4[:])
		}
		packet[10], packet[11] = 0, 0
		binary.BigEndian.PutUint16(
			packet[10:12],
			checksumFinish(checksumAdd(0, packet[:headerByteCount])),
		)
		transport := packet[headerByteCount:totalByteCount]
		transport[16], transport[17] = 0, 0
		binary.BigEndian.PutUint16(
			transport[16:18],
			transportChecksum(ipProtocolNumberTcp, packet[12:16], packet[16:20], transport),
		)
		return true
	case 6:
		if !address.Is6() || address.Is4In6() {
			return false
		}
		nextHeader, transportOffset, payloadEnd, ok := ipv6TransportOffset(packet)
		if !ok || nextHeader != ipProtocolNumberTcp ||
			payloadEnd < transportOffset+TcpHeaderSizeWithoutExtensions {
			return false
		}
		address16 := address.As16()
		if source {
			copy(packet[8:24], address16[:])
		} else {
			copy(packet[24:40], address16[:])
		}
		transport := packet[transportOffset:payloadEnd]
		transport[16], transport[17] = 0, 0
		binary.BigEndian.PutUint16(
			transport[16:18],
			transportChecksum(ipProtocolNumberTcp, packet[8:24], packet[24:40], transport),
		)
		return true
	default:
		return false
	}
}

func (self *UpgradeMux) handleDnsTcpPacket(ipPath *IpPath, packet []byte) bool {
	localAddr := self.dnsTcpLocalAddrFor(ipPath.Version)
	if !localAddr.IsValid() || !self.rememberDnsTcpFlow(ipPath) {
		return true // claimed and fail-closed; never leak to the advertised identity
	}
	redirected := append([]byte(nil), packet...)
	if !rewriteDnsTcpAddress(redirected, localAddr, false) {
		return true
	}
	if _, err := self.mux.Tun().Write(redirected); err != nil {
		if log := self.mux.log.V(2); log.Enabled() {
			log.Infof("[dns]tcp redirect failed: %s\n", err)
		}
	}
	return true
}

// onPump recognizes replies emitted by the internal TCP/53 server, restores
// the advertised server address, and sends them downstream instead of out the
// tunnel as ordinary stack-originated traffic.
func (self *UpgradeMux) onPump(packet []byte) bool {
	var ipPath IpPath
	if _, err := parseIpPathWithPayloadBorrowed(packet, &ipPath); err != nil ||
		ipPath.Protocol != IpProtocolTcp ||
		ipPath.SourcePort != 53 {
		return false
	}
	localAddr := self.dnsTcpLocalAddrFor(ipPath.Version)
	source, ok := netIPAddr(ipPath.SourceIp)
	if !ok || !localAddr.IsValid() || source != localAddr {
		return false
	}
	serverAddr, ok := self.dnsTcpServerForClient(&ipPath)
	if !ok {
		return false
	}
	defer MessagePoolReturn(packet)
	if !rewriteDnsTcpAddress(packet, serverAddr, true) {
		return true
	}
	self.mux.deliverDownstream(self.source, self.provideMode, &ipPath, packet)
	return true
}

// onSend claims and terminates intercepted DNS (UDP/TCP 53) and HTTP (TCP/80); everything else
// passes through to the upstream. TCP/443 passes through too, but is first observed for its
// TLS SNI (never claimed). The claim decision is a pure function of (protocol, dst port), so
// it is read from a cheap, allocation-free header peek — only a claimed flow (or a header the
// peek can't classify, e.g. IPv6 extension headers) needs the allocating full parse. This
// keeps the pass-through bulk off the parse/allocation path entirely.
func (self *UpgradeMux) onSend(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool {
	// first-load timeline: one atomic load once deactivated (see firstLoadTimeline)
	self.firstLoad.observeSend(packet)
	var tls tlsSegment
	switch peekClaim(packet, &tls) {
	case peekOther:
		return false // not a claimable flow — pass through without parsing
	case peekHttp:
		// block drops claimed plaintext HTTP; otherwise it passes through unchanged. neither
		// needs the full parse, so the pass-through :80 bulk stays off the allocating parse path.
		return self.httpBlocked()
	case peekTls:
		// observe the ClientHello for its SNI, then always pass through — TLS is never
		// claimed, only named. peekClaim already extracted the flow/payload while classifying,
		// so the sniffer reassembles from that segment without re-walking the L4 headers.
		self.sni.observeSegment(tls)
		return false
	}
	// peekDns or peekUndecided — classify with the authoritative full parse
	ipPath, payload, err := ParseIpPathWithPayload(packet)
	if err != nil {
		return false
	}
	switch {
	case IpProtocolUdp == ipPath.Protocol && 53 == ipPath.DestinationPort:
		if settings := self.settings.Load(); settings != nil &&
			settings.Dns != nil && settings.Dns.Resolver != nil {
			return self.handleDns(source, provideMode, ipPath, payload)
		}
		return false // DNS interception disabled — pass through to the egress
	case IpProtocolTcp == ipPath.Protocol && 53 == ipPath.DestinationPort:
		if settings := self.settings.Load(); settings != nil &&
			settings.Dns != nil && settings.Dns.Resolver != nil {
			return self.handleDnsTcpPacket(ipPath, packet)
		}
		return false
	case IpProtocolTcp == ipPath.Protocol && 80 == ipPath.DestinationPort:
		return self.httpBlocked() // reached via peekUndecided (e.g. IPv6 extension headers)
	}
	return false
}

// Classifies one exact directional flow once while preserving content state
// that inherently advances for each ordered packet. A claimed group is
// consumed in full; malformed content inside an intercepted flow fails closed
// instead of decomposing the group into independently routed packets.
func (self *UpgradeMux) onSendGroup(
	source TransferPath,
	provideMode protocol.ProvideMode,
	group *ipPacketGroup,
	timeout time.Duration,
) bool {
	if group == nil || group.ipPath == nil || len(group.packets) == 0 {
		return true
	}
	for _, packet := range group.packets {
		self.firstLoad.observeSend(packet)
	}

	ipPath := group.ipPath
	switch {
	case ipPath.Protocol == IpProtocolTcp && ipPath.DestinationPort == 443:
		for _, packet := range group.packets {
			var segment tlsSegment
			if peekClaim(packet, &segment) == peekTls {
				self.sni.observeSegment(segment)
			}
		}
		return false
	case ipPath.Protocol == IpProtocolTcp && ipPath.DestinationPort == 80:
		return self.httpBlocked()
	case ipPath.Protocol == IpProtocolUdp && ipPath.DestinationPort == 53:
		settings := self.settings.Load()
		if settings == nil || settings.Dns == nil || settings.Dns.Resolver == nil {
			return false
		}
		for _, packet := range group.packets {
			packetPath, payload, err := ParseIpPathWithPayload(packet)
			if err == nil {
				self.handleDns(source, provideMode, packetPath, payload)
			}
		}
		return true
	case ipPath.Protocol == IpProtocolTcp && ipPath.DestinationPort == 53:
		settings := self.settings.Load()
		if settings == nil || settings.Dns == nil || settings.Dns.Resolver == nil {
			return false
		}
		for _, packet := range group.packets {
			packetPath, err := ParseIpPath(packet)
			if err == nil {
				self.handleDnsTcpPacket(packetPath, packet)
			}
		}
		return true
	default:
		return false
	}
}

// httpBlocked reports whether claimed plaintext HTTP (TCP/80) should be dropped (block mode);
// otherwise it passes through to the egress unchanged.
func (self *UpgradeMux) httpBlocked() bool {
	s := self.settings.Load()
	return s.Http != nil && HttpUpgradeBlock == s.Http.Mode
}

// SetBlocker installs (or, with nil, removes) the ad/tracker Blocker
// consulted by handleDns for every claimed dns query. the blocker is shared
// with the multi client and owned by the device; enabling/disabling happens
// on the blocker itself.
func (self *UpgradeMux) SetBlocker(blocker Blocker) {
	if blocker == nil {
		self.blocker.Store(nil)
	} else {
		self.blocker.Store(&blocker)
	}
}

func (self *UpgradeMux) getBlocker() Blocker {
	if b := self.blocker.Load(); b != nil {
		return *b
	}
	return nil
}

// SetIpv6Unroutable installs (or, with nil, removes) the predicate that
// reports whether the tunnel currently has no exit able to carry IPv6. While
// it reports true, AAAA queries are answered empty; see the field comment.
func (self *UpgradeMux) SetIpv6Unroutable(ipv6Unroutable func() bool) {
	if ipv6Unroutable == nil {
		self.ipv6Unroutable.Store(nil)
	} else {
		self.ipv6Unroutable.Store(&ipv6Unroutable)
	}
}

func (self *UpgradeMux) isIpv6Unroutable() bool {
	if f := self.ipv6Unroutable.Load(); f != nil {
		return (*f)()
	}
	return false
}

// tunnelDohCold reports whether the tunnel-DoH path is cold: it has never
// answered on this mux, its last success lease expired while idle, or it has
// failed tunnelDohColdFailureCount consecutive resolutions. While cold, dns
// pipelines use the short ColdLocalFallbackTimeout handicap and the warm probe
// runs (see ensureColdProber).
func (self *UpgradeMux) tunnelDohCold() bool {
	generation := self.tunnelDohGeneration.Load()
	if self.tunnelDohProvenGeneration.Load() != generation {
		return true
	}
	lastSuccessNanos := self.tunnelDohLastSuccessNanos.Load()
	return lastSuccessNanos == 0 ||
		tunnelDohWarmLease < time.Since(time.Unix(0, lastSuccessNanos)) ||
		(self.tunnelDohFailureGeneration.Load() == generation &&
			tunnelDohColdFailureCount <= self.tunnelDohFailures.Load())
}

// markTunnelDohProven records a tunnel-DoH success: the full fallback handicap
// applies until the bounded warm lease expires.
func (self *UpgradeMux) markTunnelDohProven() {
	self.markTunnelDohProvenForGeneration(self.tunnelDohGeneration.Load())
}

// markTunnelDohProvenForGeneration publishes a success only when its resolver
// generation is still current. NetworkChanged and SetSettings invalidate the
// generation before touching connections, so a late completion from an old
// socket cannot make the replacement path appear warm.
func (self *UpgradeMux) markTunnelDohProvenForGeneration(generation uint64) bool {
	self.tunnelDohStateLock.Lock()
	defer self.tunnelDohStateLock.Unlock()
	if self.tunnelDohGeneration.Load() != generation {
		return false
	}
	self.tunnelDohLastSuccessNanos.Store(time.Now().UnixNano())
	self.tunnelDohFailures.Store(0)
	self.tunnelDohFailureGeneration.Store(generation)
	// Publish the matching generation last. A lock-free reader that observes
	// it also observes the success time and cleared failure count above.
	self.tunnelDohProvenGeneration.Store(generation)
	return true
}

// markTunnelDohUnproven advances the resolver generation and clears failures.
// A success from the prior generation may still complete, but its tagged
// publication is rejected by markTunnelDohProvenForGeneration.
func (self *UpgradeMux) markTunnelDohUnproven() uint64 {
	self.tunnelDohStateLock.Lock()
	defer self.tunnelDohStateLock.Unlock()
	generation := self.tunnelDohGeneration.Add(1)
	self.tunnelDohLastSuccessNanos.Store(0)
	self.tunnelDohFailures.Store(0)
	self.tunnelDohFailureGeneration.Store(generation)
	return generation
}

// recordTunnelDohFailureForGeneration counts a failure only for its current
// resolver generation. It returns the current consecutive count, or zero for a
// stale completion.
func (self *UpgradeMux) recordTunnelDohFailureForGeneration(generation uint64) int32 {
	self.tunnelDohStateLock.Lock()
	defer self.tunnelDohStateLock.Unlock()
	if self.tunnelDohGeneration.Load() != generation {
		return 0
	}
	if self.tunnelDohFailureGeneration.Load() != generation {
		self.tunnelDohFailureGeneration.Store(generation)
		self.tunnelDohFailures.Store(0)
	}
	failureCount := self.tunnelDohFailures.Load() + 1
	self.tunnelDohFailures.Store(failureCount)
	return failureCount
}

// dnsColdProbeDelay returns the bounded exponential delay after failureCount
// failed probes. The first failed attempt waits initialDelay, each subsequent
// one doubles it, and arithmetic saturates at maxDelay.
func dnsColdProbeDelay(failureCount int, initialDelay time.Duration, maxDelay time.Duration) time.Duration {
	if initialDelay <= 0 {
		return time.Millisecond
	}
	if maxDelay <= 0 || maxDelay < initialDelay {
		maxDelay = initialDelay
	}
	delay := initialDelay
	for i := 1; i < failureCount && delay < maxDelay; i++ {
		if maxDelay/2 < delay {
			return maxDelay
		}
		delay *= 2
	}
	return min(delay, maxDelay)
}

func (self *UpgradeMux) coldDohProbeNeeded() bool {
	return self.dnsUpgradeEnabled() && (self.dnsProbeRequested.Load() ||
		(self.fallbackDohCache.Load() != nil && self.tunnelDohCold()))
}

// dnsUpgradeEnabled reports whether this mux owns DNS interception. An
// UpgradeMux may exist only for HTTP/SNI policy with Dns nil; in that mode its
// internal Tun still has a DohCache for implementation uniformity, but it must
// not open speculative DoH connections or retain their retry workers.
func (self *UpgradeMux) dnsUpgradeEnabled() bool {
	return dnsResolverSettings(self.settings.Load()) != nil
}

// ensureColdProber starts at most one background warm-probe worker. A cold mux
// with a local fallback keeps retrying because otherwise a 250ms fallback win
// can cancel every tunnel query and pin the privacy-leaking cold state forever.
// WarmDns can also request one attempt when no fallback is configured.
func (self *UpgradeMux) ensureColdProber() bool {
	if self.ctx.Err() != nil || !self.coldDohProbeNeeded() {
		return false
	}
	if !self.dnsProberRunning.CompareAndSwap(false, true) {
		return false
	}
	go HandleError(self.runColdDohProber)
	return true
}

func (self *UpgradeMux) runColdDohProber() {
	defer func() {
		self.dnsProberRunning.Store(false)
		// Close the CAS handoff race: a path change can request a probe while
		// this worker is returning but before running becomes false.
		if self.ctx.Err() == nil && self.coldDohProbeNeeded() {
			self.ensureColdProber()
		}
	}()

	// A wake queued for the worker that just retired is already represented by
	// dnsProbeRequested/cold state; consume it before the first immediate probe
	// so it cannot cause a duplicate immediate retry.
	select {
	case <-self.dnsProberWake:
	default:
	}

	failureCount := 0
	serverCount := 2
	for self.coldDohProbeNeeded() {
		self.dnsProbeRequested.Store(false)
		generation := self.tunnelDohGeneration.Load()
		warmFunction := self.tunnelDohWarmFunction
		if warmFunction == nil {
			return
		}
		if warmFunction(self.ctx, serverCount) {
			if self.markTunnelDohProvenForGeneration(generation) {
				return
			}
			// The path changed while the request was in flight. Its result is
			// valid DNS data but cannot prove the replacement generation.
			self.dnsProbeRequested.Store(true)
			serverCount = 2
			continue
		}
		if self.fallbackDohCache.Load() == nil {
			// With no local fallback there is no leak/handicap to recover from;
			// an explicit WarmDns request is one-shot.
			return
		}

		failureCount++
		serverCount = 1
		delay := dnsColdProbeDelay(
			failureCount,
			self.dnsProbeInitialDelay,
			self.dnsProbeMaxDelay,
		)
		select {
		case <-self.ctx.Done():
			return
		case <-self.dnsProberWake:
			// A network/resolver change deserves an immediate two-server probe
			// and a fresh backoff sequence.
			failureCount = 0
			serverCount = 2
		case <-time.After(delay):
		}
	}
}

func (self *UpgradeMux) wakeColdProber() {
	if !self.dnsUpgradeEnabled() {
		self.dnsProbeRequested.Store(false)
		return
	}
	self.dnsProbeRequested.Store(true)
	if self.ensureColdProber() {
		return
	}
	select {
	case self.dnsProberWake <- struct{}{}:
	default:
	}
}

func (self *UpgradeMux) warmFallbackDns() {
	if !self.dnsUpgradeEnabled() || self.ctx.Err() != nil || self.fallbackDohCache.Load() == nil {
		self.fallbackDohWarmPending.Store(false)
		return
	}
	self.fallbackDohWarmPending.Store(true)
	if !self.fallbackDohWarmerRunning.CompareAndSwap(false, true) {
		return
	}
	go HandleError(func() {
		defer func() {
			self.fallbackDohWarmerRunning.Store(false)
			// Preserve a request that raced the final pending check.
			if self.ctx.Err() == nil && self.fallbackDohWarmPending.Load() {
				self.warmFallbackDns()
			}
		}()
		for self.fallbackDohWarmPending.Swap(false) {
			if self.ctx.Err() != nil {
				return
			}
			fallback := self.fallbackDohCache.Load()
			warmFunction := self.fallbackDohWarmFunction
			if fallback == nil || warmFunction == nil {
				return
			}
			warmFunction(self.ctx, fallback, 2)
		}
	})
}

// WarmDns opens the DoH server connections in the background, ahead of the first user query:
// the tunnel resolver's connections (TCP+TLS+h2 through the tunnel — the dials park until the
// window can carry traffic and complete at the earliest usable moment, so calling this right
// after the mux is wired to the egress self-times to tunnel-up) and the local fallback's
// (which answer the first cold-phase queries). A successful tunnel warm also proves the
// tunnel-DoH path (see tunnelDohCold); a failed one starts the cold probe loop.
func (self *UpgradeMux) WarmDns() {
	self.wakeColdProber()
	self.warmFallbackDns()
}

// NetworkChanged reacts to a host network path change: the pooled DoH
// connections ride sockets that may be bound to the dead path, so drop them
// (the answer caches are kept — resolved records stay valid across a path
// change) and re-warm in the background; the tunnel-DoH is treated as
// unproven again so lookups race the short cold fallback until it re-proves
// over the new path (see tunnelDohCold).
func (self *UpgradeMux) NetworkChanged() {
	self.markTunnelDohUnproven()
	self.mux.Tun().DohCache().CloseIdleConnections()
	if fallback := self.fallbackDohCache.Load(); fallback != nil {
		fallback.CloseIdleConnections()
	}
	self.wakeColdProber()
	self.warmFallbackDns()
}

// DnsServerScores returns the tunnel path's per-server DoH success scores for
// the owner to persist and pass back as DnsUpgradeSettings.ServerStatsSeed.
// Do not merge the local fallback scores: public DNS services are anycast, so
// a server that is fastest from the phone's host egress can map to a different
// site and be slow through the selected tunnel provider. Mixing the paths made
// the next public-page load prefer an unrelated local-path winner.
func (self *UpgradeMux) DnsServerScores() map[string]float64 {
	return self.mux.Tun().DohCache().ServerScores()
}

// FirstLoadSamples returns the first-load timeline measurements recorded since this mux's
// construction (see firstLoadTimeline): dns query→answer and tcp/443 syn→synack / first byte
// for the first flows after connect.
func (self *UpgradeMux) FirstLoadSamples() []*FirstLoadSample {
	return self.firstLoad.Samples()
}

// peekResult classifies a send packet by the flow the mux may claim, so the common pass-through
// bulk (peekOther) and pass-through TCP/80 (peekHttp, not blocking) are decided without the
// allocating full parse. Only DNS and unclassifiable packets parse.
type peekResult int

const (
	peekOther     peekResult = iota // not a claimable flow → pass through
	peekDns                         // UDP/TCP 53 (DNS)
	peekHttp                        // TCP/80 (HTTP)
	peekTls                         // TCP/443 (observed for SNI, never claimed)
	peekUndecided                   // header can't be classified cheaply → full parse
)

// peekClaim classifies a packet from the fixed IP/L4 header offsets without allocating
// (ParseIpPathWithPayload allocates the IpPath and an address backing per call). peekUndecided
// means the header can't be classified cheaply — IPv6 with extension headers, or a
// short/unsupported header — and the caller must fall back to the full parse.
//
// For a TCP/443 packet (peekTls) it also fills *seg with the flow 4-tuple and TCP payload, so
// the SNI sniffer can reassemble without re-walking the L4 headers; seg is left untouched for
// every other result. Pass a throwaway *tlsSegment when only the classification is wanted.
func peekClaim(packet []byte, seg *tlsSegment) peekResult {
	if len(packet) < 20 {
		return peekUndecided
	}
	switch packet[0] >> 4 {
	case 4:
		ihl := int(packet[0]&0x0f) * 4
		if ihl < 20 || len(packet) < ihl+4 {
			return peekUndecided
		}
		switch packet[9] { // protocol
		case 6: // tcp
			switch int(packet[ihl+2])<<8 | int(packet[ihl+3]) {
			case 53:
				return peekDns
			case 80:
				return peekHttp
			case 443:
				totalLen := int(packet[2])<<8 | int(packet[3])
				if totalLen < ihl || len(packet) < totalLen {
					totalLen = len(packet)
				}
				src, _ := netip.AddrFromSlice(packet[12:16])
				dst, _ := netip.AddrFromSlice(packet[16:20])
				if s, ok := tcpSegment443(packet, ihl, totalLen, src, dst); ok {
					*seg = s
					return peekTls
				}
				return peekOther
			}
			return peekOther
		case 17: // udp
			if 53 == int(packet[ihl+2])<<8|int(packet[ihl+3]) {
				return peekDns
			}
			return peekOther
		case 1: // icmp: passthrough, never claimed
			return peekOther
		default:
			return peekOther // not tcp/udp: never claimed
		}
	case 6:
		if len(packet) < 40 {
			return peekUndecided
		}
		// walk any extension chain to the transport (ip_ipv6_ext.go). a v6
		// fragment has no transport header to classify and is left to the
		// full parse, which rejects it until reassembly
		nextHeader, transportOffset, end, ok := ipv6TransportOffset(packet)
		if !ok {
			return peekUndecided
		}
		if len(packet) < transportOffset+4 {
			return peekUndecided
		}
		switch nextHeader {
		case ipProtocolNumberTcp:
			switch int(packet[transportOffset+2])<<8 | int(packet[transportOffset+3]) {
			case 53:
				return peekDns
			case 80:
				return peekHttp
			case 443:
				src, _ := netip.AddrFromSlice(packet[8:24])
				dst, _ := netip.AddrFromSlice(packet[24:40])
				if s, ok := tcpSegment443(packet, transportOffset, end, src, dst); ok {
					*seg = s
					return peekTls
				}
				return peekOther
			}
			return peekOther
		case ipProtocolNumberUdp:
			if 53 == int(packet[transportOffset+2])<<8|int(packet[transportOffset+3]) {
				return peekDns
			}
			return peekOther
		case ipProtocolNumberIcmp6: // passthrough, never claimed
			return peekOther
		default:
			return peekOther // not tcp/udp: never claimed
		}
	default:
		return peekUndecided
	}
}

// dnsResponder is one claimed client query awaiting the shared resolution of its
// question: enough to synthesize that client's exact reply — its transaction id and
// question (casing preserved, for clients that randomize it) — on its reversed path.
type dnsResponder struct {
	id          uint16
	question    dnsmessage.Question
	source      TransferPath
	provideMode protocol.ProvideMode
	reverse     *IpPath
}

// dnsFlight is the shared resolution pipeline for one in-flight question (see
// UpgradeMux.inflight). All fields are guarded by inflightLock.
type dnsFlight struct {
	responders []dnsResponder
	// replied is set when responders have been snapshotted; the generation
	// remains installed until its workers/accounting finish, so late arrivals
	// cannot join a snapshot that was already delivered.
	replied bool
	// accounted makes raw-forward completion idempotent across the normal
	// fan-out and its panic-safe outer defer.
	accounted bool
	// workers is the outstanding pipeline goroutine count; the flight is removed when it
	// reaches 0 without a reply (resolution failed: send nothing, the clients retry)
	workers int
	// cancel is shared by every worker in an A/AAAA race. The first
	// authoritative answer cancels its losing sibling, and the flight remains
	// in inflight until all workers have observed cancellation and exited.
	cancel context.CancelFunc
}

// handleDns claims a single DNS query and attaches it to the resolution
// pipeline for its question — joining the in-flight pipeline when one exists, else
// starting one (resolution can block on the network, so it runs asynchronously via
// the Tun's DohCache). The pipeline writes each attached client its own response and
// records the IP→hostname mapping. A/AAAA are synthesized from the address cache;
// every other record type is forwarded opaquely over DoH so the advertised DNS
// identity never needs to be a real resolver. SVCB/HTTPS ipv4hint/ipv6hint addresses
// are additionally recorded into the reverse index. When a raw forward cannot answer
// the client gets a prompt SERVFAIL, and an over-UDP-size record gets a truncated
// (TC) reply instead of hanging on a claimed query.
//
// The parsed question is a value (dnsmessage copies the name into a fixed array), and
// the reversed path owns its address bytes, so nothing aliases the recycled packet
// buffer across the pipeline goroutines.
func (self *UpgradeMux) handleDns(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, payload []byte) bool {
	var parser dnsmessage.Parser
	header, err := parser.Start(payload)
	if err != nil {
		return false
	}
	question, err := parser.Question()
	if err != nil {
		return false
	}
	// lower cased: clients that randomize query casing (dns 0x20) must coalesce,
	// cache, and record as one name — the reverse index and its learned callbacks
	// key on this. The client's reply echoes the original-cased question, which is
	// carried separately on the responder.
	domain := strings.ToLower(strings.TrimSuffix(question.Name.String(), "."))

	// resolver.arpa is a locally served special-use zone (RFC 9462 §6.1).
	// Android performs `_dns.resolver.arpa` SVCB discovery before ordinary
	// lookups. Forwarding that query over a cold tunnel both gives the wrong
	// resolver's designation and can hold the first load until the raw-DoH
	// timeout. The mux already provides encrypted resolution internally, so it
	// advertises no additional OS-level resolver and returns NODATA locally.
	if isResolverArpaDomain(domain) {
		respPayload, err := buildDnsResponse(header.ID, question, nil, 0)
		if err != nil {
			return false
		}
		reverse := ipPath.Reverse()
		self.mux.deliverDownstream(source, provideMode, reverse, ipOosPacket(reverse, respPayload))
		return true
	}

	// the blocker consults every query type, ahead of any resolution or
	// caching: a blocked name answers A/AAAA with the unspecified address
	// (the OS fails the connect instantly and locally) and every other type
	// — notably HTTPS/SVCB (65), whose ipv4hint/ipv6hint would bypass the
	// null A/AAAA — with an empty NOERROR. blocked replies never populate
	// the reverse index or the resolver cache, so toggling the blocker
	// takes effect immediately.
	if blocker := self.getBlocker(); blocker != nil && blocker.BlockHost(domain) {
		var responseTtl uint32
		if dns := self.settings.Load().Dns; dns != nil {
			responseTtl = dns.ResponseTtl
		}
		var addrs []netip.Addr
		switch question.Type {
		case dnsmessage.TypeA:
			addrs = []netip.Addr{netip.IPv4Unspecified()}
		case dnsmessage.TypeAAAA:
			addrs = []netip.Addr{netip.IPv6Unspecified()}
		}
		respPayload, err := buildDnsResponse(header.ID, question, addrs, responseTtl)
		if err != nil {
			return false
		}
		reverse := ipPath.Reverse()
		self.mux.deliverDownstream(source, provideMode, reverse, ipOosPacket(reverse, respPayload))
		return true
	}

	// while no exit can carry v6, an AAAA answers empty NOERROR locally so
	// the app connects over v4 at once instead of timing out on a v6 address
	// the tunnel cannot route (IPV6.md B6). Never cached and never recorded
	// in the reverse index: the answer describes the tunnel, not the name.
	if question.Type == dnsmessage.TypeAAAA && self.isIpv6Unroutable() {
		var responseTtl uint32
		if dns := self.settings.Load().Dns; dns != nil {
			responseTtl = dns.ResponseTtl
		}
		respPayload, err := buildDnsResponse(header.ID, question, nil, responseTtl)
		if err != nil {
			return false
		}
		reverse := ipPath.Reverse()
		self.mux.deliverDownstream(source, provideMode, reverse, ipOosPacket(reverse, respPayload))
		return true
	}

	// A/AAAA resolve-and-synthesize; every other record type is forwarded
	// opaquely. Both coalesce on the same bounded in-flight machinery.
	var recordType string
	forward := false
	switch question.Type {
	case dnsmessage.TypeA:
		recordType = "A"
	case dnsmessage.TypeAAAA:
		recordType = "AAAA"
	default:
		recordType = question.Type.String()
		forward = true
	}

	responder := dnsResponder{
		id:          header.ID,
		question:    question,
		source:      source,
		provideMode: provideMode,
		// the response flows from the queried resolver back to the client
		reverse: ipPath.Reverse(),
	}
	key := NewDohKey(recordType, domain)
	if fl := self.attachDnsResponder(key, responder); fl != nil {
		if forward {
			self.startRawForwardPipeline(key, fl, question.Type, domain)
		} else {
			self.startDnsPipeline(key, fl, recordType, domain)
		}
	}
	return true
}

// isResolverArpaDomain reports whether domain belongs to the locally served
// resolver.arpa zone. domain is normally normalized by handleDns or
// resolveDnsTcpQuery; trimming here keeps the helper safe for direct callers.
func isResolverArpaDomain(domain string) bool {
	domain = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(domain), "."))
	return domain == "resolver.arpa" || strings.HasSuffix(domain, ".resolver.arpa")
}

// attachDnsResponder attaches a claimed query to the resolution pipeline for its
// question: it joins the in-flight pipeline when one exists (client retransmits and
// a burst's duplicate lookups attach as responders, ~100 bytes, instead of spawning
// their own pipelines), starts a new flight otherwise, or drops the query at the
// caps — the client retries into a freed slot, which bounds burst memory regardless
// of client behavior. It returns the new flight when the caller must start the
// pipeline, else nil.
func (self *UpgradeMux) attachDnsResponder(key DohKey, responder dnsResponder) *dnsFlight {
	self.inflightLock.Lock()
	defer self.inflightLock.Unlock()
	if fl, ok := self.inflight[key]; ok {
		// An answer has already snapshotted this flight's responders. Keep the
		// flight counted until its losing workers exit, but do not attach a
		// responder that could no longer receive that answer.
		if fl.replied {
			return nil
		}
		for _, r := range fl.responders {
			if r.id == responder.id && r.reverse.DestinationPort == responder.reverse.DestinationPort && r.reverse.DestinationIp.Equal(responder.reverse.DestinationIp) {
				// a retransmit of an attached query; it is answered when the pipeline replies
				return nil
			}
		}
		if len(fl.responders) < maxDnsRespondersPerQuestion {
			fl.responders = append(fl.responders, responder)
		}
		// else over the responder cap: drop (the client retries into the answer cache)
		return nil
	}
	maxInflight := defaultMaxInflightDnsQueries
	if dns := self.settings.Load().Dns; dns != nil && 0 < dns.MaxInflightQueries {
		maxInflight = dns.MaxInflightQueries
	}
	if maxInflight <= len(self.inflight) {
		// at the question cap: drop (the client retries; slots free as pipelines finish)
		return nil
	}
	fl := &dnsFlight{
		responders: []dnsResponder{responder},
	}
	self.inflight[key] = fl
	return fl
}

// startDnsPipeline resolves one question and fans the first successful answer out to
// the flight's responders, exactly once, retiring the flight. A later identical query
// starts a fresh pipeline, answered immediately from the resolver cache.
func (self *UpgradeMux) startDnsPipeline(key DohKey, fl *dnsFlight, recordType string, domain string) {
	var resolveTimeout time.Duration
	var responseTtl uint32
	var localFallbackTimeout time.Duration
	var coldLocalFallbackTimeout time.Duration
	if dns := self.settings.Load().Dns; dns != nil {
		resolveTimeout = dns.ResolveTimeout
		responseTtl = dns.ResponseTtl
		localFallbackTimeout = dns.LocalFallbackTimeout
		coldLocalFallbackTimeout = dns.ColdLocalFallbackTimeout
	}
	fallback := self.fallbackDohCache.Load()
	// while the tunnel-DoH is cold (unproven, or stalled after consecutive failures),
	// shorten the fallback handicap so lookups stay responsive through connect — the
	// accepted startup-leak window. The unraced warm probe is what re-proves the tunnel
	// and restores the full handicap (cold-phase query workers are canceled when the
	// fallback wins, so they can lose every race). Only when the fallback is enabled at
	// all (configured AND a base handicap set) — the cold phase must never re-enable a
	// fallback the settings disabled.
	if fallback != nil && 0 < localFallbackTimeout && 0 < coldLocalFallbackTimeout && self.tunnelDohCold() {
		// min: the cold phase only ever shortens the handicap
		localFallbackTimeout = min(localFallbackTimeout, coldLocalFallbackTimeout)
		self.ensureColdProber()
	}
	self.firstLoad.dnsStart(key)

	var queryCtx context.Context
	var queryCancel context.CancelFunc
	if 0 < resolveTimeout {
		queryCtx, queryCancel = context.WithTimeout(self.ctx, resolveTimeout)
	} else {
		queryCtx, queryCancel = context.WithCancel(self.ctx)
	}

	// reply delivers the first successful resolution to every attached responder, exactly
	// once. A failure — no records and no authoritative no-record answer, from both the
	// tunnel and the fallback — sends nothing: the clients time out and retry, which the OS
	// tolerates far better than a SERVFAIL or empty NOERROR reply it would surface as
	// "can't resolve address".
	reply := func(addrs []netip.Addr, authoritative bool) {
		if len(addrs) == 0 && !authoritative {
			return
		}
		var responders []dnsResponder
		var cancel context.CancelFunc
		func() {
			self.inflightLock.Lock()
			defer self.inflightLock.Unlock()
			if fl.replied {
				return
			}
			fl.replied = true
			responders = fl.responders
			cancel = fl.cancel
		}()
		if responders == nil {
			return
		}
		// Cancel the losing resolver before doing response construction and
		// delivery. The flight remains in the map until workerDone observes
		// every worker exit, so MaxInflightQueries also bounds queued losers.
		if cancel != nil {
			cancel()
		}
		if 0 < len(addrs) {
			self.reverse.record(addrs, domain)
		}
		for i := range responders {
			r := &responders[i]
			respPayload, err := buildDnsResponse(r.id, r.question, addrs, responseTtl)
			if err != nil {
				continue
			}
			self.mux.deliverDownstream(r.source, r.provideMode, r.reverse, ipOosPacket(r.reverse, respPayload))
		}
		self.firstLoad.dnsDone(key, true)
	}

	// workerDone retires the flight once every worker has exited without an answer, so a
	// failed question frees its slot for the clients' retries to start a fresh pipeline.
	workerDone := func() {
		var cancel context.CancelFunc
		failed := false
		finished := false
		self.inflightLock.Lock()
		fl.workers -= 1
		if fl.workers == 0 {
			finished = true
			failed = !fl.replied
			// Keep this generation in the map, but closed to new responders,
			// until its first-load accounting is complete. Deleting first
			// creates a gap where a new same-key generation can start and the
			// old dnsDone then consumes the new measurement.
			fl.replied = true
			cancel = fl.cancel
			fl.cancel = nil
		}
		self.inflightLock.Unlock()
		if cancel != nil {
			cancel()
		}
		if failed {
			self.firstLoad.dnsDone(key, false)
		}
		if finished {
			self.retireDnsFlight(key, fl)
		}
	}

	workers := 1
	if fallback != nil && 0 < localFallbackTimeout {
		workers = 2
	}
	func() {
		self.inflightLock.Lock()
		defer self.inflightLock.Unlock()
		fl.workers = workers
		fl.cancel = queryCancel
	}()

	// primary: resolve over the tunnel-DoH (preferred — egresses the tunnel, no DNS leak), retrying
	// on the dnsRetryBackoff schedule. The first connect/TLS over a freshly-connecting tunnel can
	// take tens of seconds (the tun dial and TLS handshake budgets are 30s each, within the 60s
	// ResolveTimeout), so a slow attempt waits for the tunnel rather than failing fast.
	tunnelOk := make(chan struct{})
	go HandleError(func() {
		defer workerDone()
		tunnelDohGeneration := self.tunnelDohGeneration.Load()
		addrs, authoritative := self.resolveTunnelDoh(queryCtx, recordType, domain)
		if 0 < len(addrs) || authoritative {
			// the tunnel path works: restore the full fallback handicap (see tunnelDohCold)
			self.markTunnelDohProvenForGeneration(tunnelDohGeneration)
			close(tunnelOk) // the tunnel won — signal the fallback to skip its local query (no leak)
		} else {
			// a failure here includes losing the fallback race (reply cancels the shared
			// ctx): one loss can be a hiccup, a run of losses means the tunnel is stalled
			// — flip cold and start the unraced probe that will re-prove recovery
			if tunnelDohColdFailureCount <= self.recordTunnelDohFailureForGeneration(tunnelDohGeneration) {
				self.ensureColdProber()
			}
		}
		reply(addrs, authoritative)
	})

	// handicapped local fallback: if the tunnel-DoH hasn't produced an answer within
	// LocalFallbackTimeout, also resolve over the LOCAL host egress (bypassing the tunnel). The
	// delay handicaps the local resolver so the tunnel wins whenever it can; the fallback only
	// answers while the tunnel is still establishing, keeping DNS responsive so the OS doesn't tear
	// down the tunnel (at the cost of a brief DNS leak during startup).
	if workers == 2 {
		go HandleError(func() {
			defer workerDone()
			timer := time.NewTimer(localFallbackTimeout)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-tunnelOk:
				return
			case <-queryCtx.Done():
				return
			}
			addrs, authoritative := fallback.QueryResult(queryCtx, recordType, domain)
			reply(addrs, authoritative)
		})
	}
}

// maxForwardedDnsResponse bounds a raw response delivered to the client over
// UDP/53 to the conservative DNS Flag Day EDNS size. A larger record is answered
// with TC so the client retries through the mux's TCP/53 terminator.
const maxForwardedDnsResponse = 1232

// startRawForwardPipeline forwards one non-address question over the tunnel
// DoH and, after the same cold/warm handicap as A/AAAA, races the configured
// local-egress fallback. Chromium commonly waits for HTTPS/SVCB alongside the
// address records before opening an origin; omitting the fallback from only
// this branch let a half-open tunnel request stall an otherwise-successful
// public-page fan-out for the caller's full DNS deadline.
func (self *UpgradeMux) startRawForwardPipeline(key DohKey, fl *dnsFlight, qType dnsmessage.Type, domain string) {
	var resolveTimeout time.Duration
	var localFallbackTimeout time.Duration
	var coldLocalFallbackTimeout time.Duration
	if dns := self.settings.Load().Dns; dns != nil {
		resolveTimeout = dns.ResolveTimeout
		localFallbackTimeout = dns.LocalFallbackTimeout
		coldLocalFallbackTimeout = dns.ColdLocalFallbackTimeout
	}
	fallback := self.fallbackDohCache.Load()
	if fallback != nil && 0 < localFallbackTimeout && 0 < coldLocalFallbackTimeout && self.tunnelDohCold() {
		localFallbackTimeout = min(localFallbackTimeout, coldLocalFallbackTimeout)
		self.ensureColdProber()
	}
	self.firstLoad.dnsStart(key)
	go HandleError(func() {
		succeeded := false
		// Also covers an unexpected resolver/delivery panic: the owned flight
		// and its measurement cannot remain stranded. fanOutRawForward may
		// complete the success path first; both operations are identity-safe
		// and idempotent for the retired generation.
		defer func() { self.completeRawDnsFlight(key, fl, succeeded) }()

		queryCtx := self.ctx
		var queryCancel context.CancelFunc
		if 0 < resolveTimeout {
			queryCtx, queryCancel = context.WithTimeout(self.ctx, resolveTimeout)
		} else {
			queryCtx, queryCancel = context.WithCancel(self.ctx)
		}
		defer queryCancel()

		type rawForwardResult struct {
			response []byte
			ok       bool
		}
		results := make(chan rawForwardResult, 2)
		tunnelOk := make(chan struct{})
		var workers sync.WaitGroup

		workers.Add(1)
		go HandleError(func() {
			defer workers.Done()
			tunnelDohGeneration := self.tunnelDohGeneration.Load()
			response, ok := self.mux.Tun().DohCache().Forward(queryCtx, qType, domain)
			if ok {
				self.markTunnelDohProvenForGeneration(tunnelDohGeneration)
				close(tunnelOk)
			} else if tunnelDohColdFailureCount <= self.recordTunnelDohFailureForGeneration(tunnelDohGeneration) {
				self.ensureColdProber()
			}
			results <- rawForwardResult{response: response, ok: ok}
		})

		if fallback != nil && 0 < localFallbackTimeout {
			workers.Add(1)
			go HandleError(func() {
				defer workers.Done()
				timer := time.NewTimer(localFallbackTimeout)
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-tunnelOk:
					return
				case <-queryCtx.Done():
					return
				}
				response, ok := fallback.Forward(queryCtx, qType, domain)
				results <- rawForwardResult{response: response, ok: ok}
			})
		}

		go func() {
			workers.Wait()
			close(results)
		}()

		for result := range results {
			if succeeded || !result.ok {
				continue
			}
			succeeded = true
			queryCancel()
			self.deliverRawForwardResponse(
				self.takeRawForwardResponders(key, fl),
				domain,
				result.response,
			)
			// Keep the replied generation installed until the canceled loser
			// releases its resolver admission. This preserves the mux's hard
			// in-flight bound under client retransmits.
		}
		if !succeeded {
			// Both paths failed. Every raw type is claimed, so answer SERVFAIL
			// rather than leaving the advertised DNS identity silent until the
			// client's own timeout.
			self.deliverDnsStatus(
				self.takeRawForwardResponders(key, fl),
				dnsmessage.RCodeServerFailure,
				false,
			)
		}
	})
}

// takeRawForwardResponders snapshots a raw-forward flight exactly once,
// returning nil when the flight has already replied. The replied generation
// deliberately remains installed until its first-load accounting completes;
// attachDnsResponder sees replied and cannot attach a query that missed the
// snapshot. retireDnsFlight then identity-checks the final removal.
func (self *UpgradeMux) takeRawForwardResponders(_ DohKey, fl *dnsFlight) []dnsResponder {
	var responders []dnsResponder
	func() {
		self.inflightLock.Lock()
		defer self.inflightLock.Unlock()
		if fl.replied {
			return
		}
		fl.replied = true
		responders = fl.responders
	}()
	return responders
}

func (self *UpgradeMux) retireDnsFlight(key DohKey, fl *dnsFlight) {
	self.inflightLock.Lock()
	defer self.inflightLock.Unlock()
	if self.inflight[key] == fl {
		delete(self.inflight, key)
	}
}

func (self *UpgradeMux) completeRawDnsFlight(key DohKey, fl *dnsFlight, succeeded bool) {
	self.inflightLock.Lock()
	if fl.accounted {
		self.inflightLock.Unlock()
		return
	}
	fl.accounted = true
	// A panic may complete before takeRawForwardResponders marks the flight.
	// Close it to late attachments while the owned timeline entry is retired.
	fl.replied = true
	self.inflightLock.Unlock()

	// Account first, retire second. A new same-key generation cannot become
	// visible between these operations and be mistaken for the old sample.
	self.firstLoad.dnsDone(key, succeeded)
	self.retireDnsFlight(key, fl)
}

// deliverDnsStatus answers each responder with a header+question-only response carrying
// `rcode` and, when `truncated`, the TC bit — the fail-fast replies (SERVFAIL for a dead
// forward, TC for an answer that exceeds UDP/53).
func (self *UpgradeMux) deliverDnsStatus(responders []dnsResponder, rcode dnsmessage.RCode, truncated bool) {
	for i := range responders {
		r := &responders[i]
		respPayload, err := buildDnsStatusResponse(r.id, r.question, rcode, truncated)
		if err != nil {
			continue
		}
		self.mux.deliverDownstream(r.source, r.provideMode, r.reverse, ipOosPacket(r.reverse, respPayload))
	}
}

// fanOutRawForward delivers a forwarded record to the flight's responders
// (each with its own DNS transaction id stamped in). For SVCB/HTTPS, it records
// ipv4hint/ipv6hint addresses into the reverse index. An oversized response is
// answered with TC for TCP retry; malformed input is answered SERVFAIL.
func (self *UpgradeMux) fanOutRawForward(key DohKey, fl *dnsFlight, domain string, response []byte) {
	defer self.completeRawDnsFlight(key, fl, true)
	responders := self.takeRawForwardResponders(key, fl)
	self.deliverRawForwardResponse(responders, domain, response)
}

// deliverRawForwardResponse performs the delivery half after a pipeline has
// atomically snapshotted its responders. Keeping it separate lets a winning
// local fallback answer immediately while the canceled tunnel worker releases
// its bounded HTTP/memory admission before the flight generation is retired.
func (self *UpgradeMux) deliverRawForwardResponse(responders []dnsResponder, domain string, response []byte) {
	if responders == nil {
		return
	}

	// record the hint addresses -> domain (fires the learned callbacks that invalidate stale
	// block-action decisions), so a flow to a hint ip reports the server name
	if hints := parseHttpsHints(response); 0 < len(hints) {
		self.reverse.record(hints, domain)
	}

	if maxForwardedDnsResponse < len(response) {
		// oversized for UDP/53: truncation is the accurate signal — the client
		// retries over its own TCP path (unclaimed pass-through) or falls back
		// to A/AAAA, instead of timing out on silence
		self.deliverDnsStatus(responders, dnsmessage.RCodeSuccess, true)
		return
	}
	if len(response) < 2 {
		// malformed beyond repair (no transaction id to patch): a failure
		self.deliverDnsStatus(responders, dnsmessage.RCodeServerFailure, false)
		return
	}
	for i := range responders {
		r := &responders[i]
		// The DoH lookup uses the normalized lowercase domain, but DNS 0x20
		// clients validate that the response echoes their exact query casing.
		// Patch both the transaction id and the (same-length) question name
		// without changing answer offsets or compression pointers.
		resp, ok := dnsResponseForResponder(response, r)
		if !ok {
			continue
		}
		self.mux.deliverDownstream(r.source, r.provideMode, r.reverse, ipOosPacket(r.reverse, resp))
	}
}

// dnsResponseForResponder copies a one-question DNS response and restores the
// responder's original question casing. The normalized DoH query and original
// query differ only by ASCII case, so the wire name has identical length and
// answer compression offsets remain valid. A compressed/mismatched question is
// rejected rather than returning a response that can fail DNS 0x20 validation.
func dnsResponseForResponder(response []byte, responder *dnsResponder) ([]byte, bool) {
	if len(response) < 12 || binary.BigEndian.Uint16(response[4:6]) != 1 {
		return nil, false
	}
	name := responder.question.Name.String()
	if name == "" || name[len(name)-1] != '.' {
		return nil, false
	}

	wireName := make([]byte, 0, len(name)+1)
	for labelStart := 0; labelStart < len(name)-1; {
		labelEnd := strings.IndexByte(name[labelStart:], '.')
		if labelEnd < 0 || 63 < labelEnd {
			return nil, false
		}
		labelEnd += labelStart
		wireName = append(wireName, byte(labelEnd-labelStart))
		wireName = append(wireName, name[labelStart:labelEnd]...)
		labelStart = labelEnd + 1
	}
	wireName = append(wireName, 0)

	questionEnd := 12 + len(wireName)
	if len(response) < questionEnd+4 {
		return nil, false
	}
	responseName := response[12:questionEnd]
	for i := range wireName {
		a := responseName[i]
		b := wireName[i]
		if 'A' <= a && a <= 'Z' {
			a += 'a' - 'A'
		}
		if 'A' <= b && b <= 'Z' {
			b += 'a' - 'A'
		}
		if a != b {
			return nil, false
		}
	}
	if dnsmessage.Type(binary.BigEndian.Uint16(response[questionEnd:questionEnd+2])) != responder.question.Type ||
		dnsmessage.Class(binary.BigEndian.Uint16(response[questionEnd+2:questionEnd+4])) != responder.question.Class {
		return nil, false
	}

	result := make([]byte, len(response))
	copy(result, response)
	binary.BigEndian.PutUint16(result[0:2], responder.id)
	copy(result[12:questionEnd], wireName)
	return result, true
}

// parseHttpsHints extracts the ipv4hint (SvcParamKey 4) and ipv6hint (key 6) addresses from the
// SVCB/HTTPS answers of a DNS response wire, for recording into the reverse index. Malformed input
// yields the hints parsed so far.
func parseHttpsHints(response []byte) []netip.Addr {
	var parser dnsmessage.Parser
	if _, err := parser.Start(response); err != nil {
		return nil
	}
	if err := parser.SkipAllQuestions(); err != nil {
		return nil
	}
	var hints []netip.Addr
	for {
		h, err := parser.AnswerHeader()
		if err != nil { // ErrSectionDone or malformed
			break
		}
		switch h.Type {
		case dnsTypeHttps:
			r, err := parser.HTTPSResource()
			if err != nil {
				return hints
			}
			hints = appendSvcbHints(hints, &r.SVCBResource)
		case dnsTypeSvcb:
			r, err := parser.SVCBResource()
			if err != nil {
				return hints
			}
			hints = appendSvcbHints(hints, &r)
		default:
			if err := parser.SkipAnswer(); err != nil {
				return hints
			}
		}
	}
	return hints
}

func appendSvcbHints(hints []netip.Addr, r *dnsmessage.SVCBResource) []netip.Addr {
	if v, ok := r.GetParam(dnsmessage.SVCParamIPv4Hint); ok {
		for i := 0; i+4 <= len(v); i += 4 {
			hints = append(hints, netip.AddrFrom4([4]byte(v[i:i+4])))
		}
	}
	if v, ok := r.GetParam(dnsmessage.SVCParamIPv6Hint); ok {
		for i := 0; i+16 <= len(v); i += 16 {
			hints = append(hints, netip.AddrFrom16([16]byte(v[i:i+16])))
		}
	}
	return hints
}

// dnsRetryBackoff is the wait between tunnel-DoH resolution attempts, for attempts that fail
// fast (a hanging attempt already waits inside its request until ResolveTimeout). The schedule
// is short: each retry rebuilds a full server fan-out, and once the pipeline retires, the
// clients' own retransmits start a fresh one anyway — so long mux-side backoff only holds
// pipeline memory without improving resolution.
var dnsRetryBackoff = []time.Duration{
	1 * time.Second,
	2 * time.Second,
}

// resolveTunnelDoh resolves over the tun's DoH cache, retrying on dnsRetryBackoff until it gets a
// real answer (records, or an authoritative no-record), the backoff is exhausted, or ctx is done.
func (self *UpgradeMux) resolveTunnelDoh(ctx context.Context, recordType string, domain string) ([]netip.Addr, bool) {
	doh := self.mux.Tun().DohCache()
	var addrs []netip.Addr
	var authoritative bool
	for i := 0; ; i++ {
		addrs, authoritative = doh.QueryResult(ctx, recordType, domain)
		if 0 < len(addrs) || authoritative {
			return addrs, authoritative
		}
		if len(dnsRetryBackoff) <= i {
			return addrs, authoritative
		}
		select {
		case <-time.After(dnsRetryBackoff[i]):
		case <-ctx.Done():
			return addrs, authoritative
		}
	}
}

// buildDnsStatusResponse builds a header+question-only response: `rcode` (SERVFAIL for a
// forward that can never answer) and/or the TC bit (an answer that exceeds UDP/53). Used by
// the SVCB/HTTPS forward path to fail fast — the claimed type must never be a black hole.
func buildDnsStatusResponse(id uint16, question dnsmessage.Question, rcode dnsmessage.RCode, truncated bool) ([]byte, error) {
	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{
		ID:                 id,
		Response:           true,
		RecursionAvailable: true,
		Truncated:          truncated,
		RCode:              rcode,
	})
	builder.EnableCompression()
	if err := builder.StartQuestions(); err != nil {
		return nil, err
	}
	if err := builder.Question(question); err != nil {
		return nil, err
	}
	return builder.Finish()
}

func buildDnsResponse(id uint16, question dnsmessage.Question, addrs []netip.Addr, ttl uint32) ([]byte, error) {
	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{
		ID:                 id,
		Response:           true,
		RecursionAvailable: true,
	})
	builder.EnableCompression()
	if err := builder.StartQuestions(); err != nil {
		return nil, err
	}
	if err := builder.Question(question); err != nil {
		return nil, err
	}
	if err := builder.StartAnswers(); err != nil {
		return nil, err
	}
	rh := dnsmessage.ResourceHeader{
		Name:  question.Name,
		Class: dnsmessage.ClassINET,
		TTL:   ttl,
	}
	for _, addr := range addrs {
		switch question.Type {
		case dnsmessage.TypeA:
			if addr.Is4() {
				if err := builder.AResource(rh, dnsmessage.AResource{A: addr.As4()}); err != nil {
					return nil, err
				}
			}
		case dnsmessage.TypeAAAA:
			if addr.Is6() && !addr.Is4In6() {
				if err := builder.AAAAResource(rh, dnsmessage.AAAAResource{AAAA: addr.As16()}); err != nil {
					return nil, err
				}
			}
		}
	}
	return builder.Finish()
}

// reverseEntry is the IP→hostname affinity record: the server names resolved to an IP and
// the last time the IP saw activity — resolved, looked up for affinity, or seen as the
// source of a return packet — for idle TTL eviction (see ReverseTtl).
type reverseEntry struct {
	serverNames       []string
	lastActivityNanos int64
}

// reverseIndex maps a resolved ip to the server name(s) observed for it (from DNS
// resolution), for the multi-client's ServerName path affinity and block-action
// server-name reporting. It is self-contained — it holds no tun/mux and runs no
// background loop — so it can be constructed and driven directly in a test; the
// owning UpgradeMux records DoH resolutions into it (record), drives its idle
// eviction from its maintenance loop (evictIdle), and delegates its ServerNameLookup
// / ServerNamesLearnedNotifier methods to it.
type reverseIndex struct {
	// maxEntries returns the live hard cap on the map between TTL sweeps. Read per
	// insert (not snapshotted) so a settings change to ReverseMaxEntries applies
	// without rebuilding the index.
	maxEntries func() int

	lock             sync.Mutex
	entries          map[netip.Addr]reverseEntry
	learnedCallbacks *CallbackList[ServerNamesLearnedFunction]
}

func newReverseIndex(maxEntries func() int) *reverseIndex {
	return &reverseIndex{
		maxEntries:       maxEntries,
		entries:          map[netip.Addr]reverseEntry{},
		learnedCallbacks: NewCallbackList[ServerNamesLearnedFunction](),
	}
}

// record associates domain with each of addrs (a DoH resolution), refreshing their
// activity, and fires the learned callbacks for the ips that newly gained the name.
func (self *reverseIndex) record(addrs []netip.Addr, domain string) {
	maxEntries := self.maxEntries()
	now := time.Now().UnixNano()
	// the ips for which this domain was newly recorded (took the !found branch)
	var learned []netip.Addr
	func() {
		self.lock.Lock()
		defer self.lock.Unlock()
		for _, addr := range addrs {
			e, ok := self.entries[addr]
			if !ok && maxEntries <= len(self.entries) {
				// at the cap: make room by evicting the least-recently-active of a sample,
				// so a resolve burst cannot grow the map without bound between TTL sweeps
				self.evictOldestSampleLocked()
			}
			found := false
			for _, name := range e.serverNames {
				if name == domain {
					found = true
					break
				}
			}
			if !found {
				if maxServerNamesPerIp <= len(e.serverNames) {
					// keep the most recent names for a shared IP (CDN fronting many hosts):
					// drop the oldest
					copy(e.serverNames, e.serverNames[1:])
					e.serverNames[len(e.serverNames)-1] = domain
				} else {
					e.serverNames = append(e.serverNames, domain)
				}
				learned = append(learned, addr)
			}
			e.lastActivityNanos = now
			self.entries[addr] = e
		}
	}()
	// notify downstream (e.g. the multi-client's block-action decision caches) of
	// the newly-learned names, OUTSIDE the lock, so subsequent block actions for
	// these ips report the server name instead of the ip going forward
	if 0 < len(learned) {
		for _, callback := range self.learnedCallbacks.Get() {
			HandleError(func() {
				callback(learned)
			})
		}
	}
}

// addLearnedCallback registers a callback fired with the ips for which a new server
// name was just learned.
func (self *reverseIndex) addLearnedCallback(callback ServerNamesLearnedFunction) func() {
	callbackId := self.learnedCallbacks.Add(callback)
	return func() {
		self.learnedCallbacks.Remove(callbackId)
	}
}

// evictOldestSampleLocked deletes the least-recently-active record of a
// reverseEvictSampleSize sample (map iteration starts at a random bucket, so the
// sample is effectively random): approximate LRU at O(sample) per over-cap insert.
func (self *reverseIndex) evictOldestSampleLocked() {
	var oldestAddr netip.Addr
	var oldestNanos int64
	found := false
	i := 0
	for addr, e := range self.entries {
		if !found || e.lastActivityNanos < oldestNanos {
			oldestAddr = addr
			oldestNanos = e.lastActivityNanos
			found = true
		}
		i += 1
		if reverseEvictSampleSize <= i {
			break
		}
	}
	if found {
		delete(self.entries, oldestAddr)
	}
}

// serverNames returns the hostname(s) recorded for the given ip, for ServerName-based
// path affinity. Empty if none seen. Refreshes the record's activity.
func (self *reverseIndex) serverNames(ip string) []string {
	addr, err := netip.ParseAddr(ip)
	if err != nil {
		return nil
	}
	self.lock.Lock()
	defer self.lock.Unlock()
	e, ok := self.entries[addr]
	if !ok {
		return nil
	}
	// refresh activity so an IP that is actively routed keeps its affinity record
	e.lastActivityNanos = time.Now().UnixNano()
	self.entries[addr] = e
	return append([]string{}, e.serverNames...)
}

// touch refreshes the affinity record for addr — a return packet's source — so an IP with
// live (return) traffic keeps its IP→hostname affinity and is not idle-evicted. It is a
// no-op for an IP with no record (e.g. a direct-IP flow that was never resolved here).
func (self *reverseIndex) touch(addr netip.Addr) {
	if !addr.IsValid() {
		return
	}
	self.lock.Lock()
	defer self.lock.Unlock()
	if e, ok := self.entries[addr]; ok {
		e.lastActivityNanos = time.Now().UnixNano()
		self.entries[addr] = e
	}
}

// evictIdle drops affinity records idle at least ttl (none when ttl <= 0).
func (self *reverseIndex) evictIdle(ttl time.Duration) {
	if 0 < ttl {
		cutoff := time.Now().UnixNano() - int64(ttl)
		self.lock.Lock()
		defer self.lock.Unlock()
		for addr, e := range self.entries {
			if e.lastActivityNanos <= cutoff {
				delete(self.entries, addr)
			}
		}
	}
}

// shed drops the least-recently-active records under host memory pressure, keeping the
// most-recently-active half. A full clear would strip the names for every live flow
// (which re-look-up affinity only on a new flow, not per packet), flipping active
// downloads from by-name to by-IP routing and blanking the block-action host feed; the
// active records are the small, useful part of the map, so retaining them costs little
// while still releasing the bulk (idle, never-to-be-seen-again resolutions).
func (self *reverseIndex) shed() {
	self.lock.Lock()
	defer self.lock.Unlock()
	if len(self.entries) <= 1 {
		return
	}
	// keep the most-recently-active half: collect activity times, find the median, and
	// drop records at or below it (approximate — ties around the median may skew the
	// kept count slightly, which is fine for a memory-pressure shed).
	times := make([]int64, 0, len(self.entries))
	for _, e := range self.entries {
		times = append(times, e.lastActivityNanos)
	}
	slices.Sort(times)
	cutoff := times[len(times)/2]
	for addr, e := range self.entries {
		if e.lastActivityNanos < cutoff {
			delete(self.entries, addr)
		}
	}
}

// adoptFrom copies src's records into this index (keeping the more-recently-active on a
// key collision), so a freshly built index inherits the names a prior instance learned —
// e.g. across a mux rebuild on reconnect. Server names outlive the physical connection,
// so carrying them avoids blanking the host feed for every already-open flow after a
// reconnect. Callbacks are NOT copied (the new index has its own subscribers). No-op on a
// nil src.
func (self *reverseIndex) adoptFrom(src *reverseIndex) {
	if src == nil {
		return
	}
	// snapshot src under its lock, then merge under ours, so the two locks never nest.
	// serverNames is deep-copied in the snapshot: record mutates a capped entry's slice
	// in place, and src may still be recording (a draining prior mux) after the merge —
	// the adopted entries must not share backing arrays with it
	var srcEntries map[netip.Addr]reverseEntry
	func() {
		src.lock.Lock()
		defer src.lock.Unlock()
		srcEntries = make(map[netip.Addr]reverseEntry, len(src.entries))
		for addr, e := range src.entries {
			e.serverNames = append([]string{}, e.serverNames...)
			srcEntries[addr] = e
		}
	}()
	maxEntries := self.maxEntries()
	self.lock.Lock()
	defer self.lock.Unlock()
	for addr, e := range srcEntries {
		existing, ok := self.entries[addr]
		if ok && existing.lastActivityNanos >= e.lastActivityNanos {
			continue
		}
		if !ok && maxEntries <= len(self.entries) {
			self.evictOldestSampleLocked()
		}
		self.entries[addr] = e
	}
}

// count returns the number of records held (observability / tests).
func (self *reverseIndex) count() int {
	self.lock.Lock()
	defer self.lock.Unlock()
	return len(self.entries)
}

// reverseMaxEntries is the live hard cap on the reverse index between TTL sweeps, from
// the mux's DNS settings (ReverseMaxEntries), falling back to the default.
func (self *UpgradeMux) reverseMaxEntries() int {
	if dns := self.settings.Load().Dns; dns != nil && 0 < dns.ReverseMaxEntries {
		return dns.ReverseMaxEntries
	}
	return defaultReverseMaxEntries
}

// ServerNames returns the hostname(s) the mux has resolved to the given IP, for
// ServerName-based path affinity. Empty if none seen. Implements ServerNameLookup.
func (self *UpgradeMux) ServerNames(ip string) []string {
	return self.reverse.serverNames(ip)
}

// AddServerNamesLearnedCallback registers a callback fired with the ips for which
// a new server name was just learned (implements ServerNamesLearnedNotifier).
func (self *UpgradeMux) AddServerNamesLearnedCallback(callback ServerNamesLearnedFunction) func() {
	return self.reverse.addLearnedCallback(callback)
}

// reverseTtl is the configured IP→hostname affinity TTL (0 if unset/disabled).
func (self *UpgradeMux) reverseTtl() time.Duration {
	if dns := self.settings.Load().Dns; dns != nil {
		return dns.ReverseTtl
	}
	return 0
}

// run is the mux's lifecycle loop: it TTL-evicts the IP→hostname affinity map so it doesn't grow
// unbounded with every resolved IP. It ticks on the reverse TTL (a default cadence when disabled)
// and runs until the mux ctx is done.
func (self *UpgradeMux) run() {
	for {
		ttl := self.reverseTtl()
		interval := ttl
		if interval <= 0 {
			interval = 5 * time.Minute
		}
		select {
		case <-self.ctx.Done():
			return
		case <-time.After(interval):
		}
		self.reverse.evictIdle(ttl)
	}
}

// SendPacket conforms to the UserNat send signature (the device's send route calls it).
func (self *UpgradeMux) SendPacket(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool {
	return self.mux.SendPacket(source, provideMode, packet, timeout)
}

// Consumes a native packet burst after grouping it by exact directional flow.
// Each group is classified once at the routing boundary while content-aware
// observers still inspect its packets in order.
func (self *UpgradeMux) SendPacketBatch(
	source TransferPath,
	provideMode protocol.ProvideMode,
	packets [][]byte,
	timeout time.Duration,
) int {
	return self.mux.SendPacketBatch(source, provideMode, packets, timeout)
}

// Receive is installed as the wrapped upstream's receive callback. The
// callback IpPath is the canonical outbound path, not the return packet's
// direction. Read the actual packet source (the server IP) to refresh that
// affinity record. The multi-client does not re-look-up affinity per packet
// (only on a new flow), so without this an active download's record would
// expire at the idle TTL and its routing would flip from base-domain to by-IP,
// breaking the flow.
func (self *UpgradeMux) Receive(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
	// first-load timeline: one atomic load once deactivated (see firstLoadTimeline)
	self.firstLoad.observeReceive(packet)
	if packetSource, _, ok := ipPacketSourceDestinationAddrs(packet); ok {
		self.reverse.touch(packetSource)
	}
	self.mux.Receive(source, provideMode, ipPath, packet)
}

// Batch receive preserves first-load and reverse-affinity observation for
// every packet, then retains the batch through the generic mux boundary.
func (self *UpgradeMux) ReceivePackets(
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	packets [][]byte,
) {
	for _, packet := range packets {
		self.firstLoad.observeReceive(packet)
		if packetSource, _, ok := ipPacketSourceDestinationAddrs(packet); ok {
			self.reverse.touch(packetSource)
		}
	}
	self.mux.ReceivePackets(source, provideMode, ipPath, packets)
}

// A batch receiver is used by device adapters while the singular receiver
// remains available for synthesized and mixed traffic.
func (self *UpgradeMux) AddPacketsReceiver(receiver ReceivePacketsFunction) func() {
	return self.mux.AddPacketsReceiver(receiver)
}

// SetUpstream wires the wrapped upstream send (the remote UserNat).
func (self *UpgradeMux) SetUpstream(upstream IpMuxSend) {
	self.upstreamMultiClient.Store(nil)
	self.mux.SetUpstream(upstream)
}

func (self *UpgradeMux) bindDnsResultToExit(domain string, addrs []netip.Addr, route *DohRoute) {
	upstream := self.upstreamMultiClient.Load()
	if upstream == nil || route == nil || !route.Local.IsValid() || !route.Remote.IsValid() {
		return
	}
	localAddr := route.Local.Addr().Unmap()
	remoteAddr := route.Remote.Addr().Unmap()
	if localAddr.Is4() != remoteAddr.Is4() {
		return
	}
	version := 6
	if localAddr.Is4() {
		version = 4
	}
	upstream.bindDnsResultToExit(&IpPath{
		Version:         version,
		Protocol:        IpProtocolTcp,
		SourceIp:        net.IP(localAddr.AsSlice()),
		SourcePort:      int(route.Local.Port()),
		DestinationIp:   net.IP(remoteAddr.AsSlice()),
		DestinationPort: int(route.Remote.Port()),
	}, domain, addrs)
}

// Wires an exact-flow group path when the upstream supports it.
func (self *UpgradeMux) SetUpstreamBatchClient(upstream *RemoteUserNatMultiClient) {
	self.upstreamMultiClient.Store(upstream)
	self.mux.SetUpstream(upstream.SendPacket)
	self.mux.setUpstreamGroupSend(func(
		source TransferPath,
		provideMode protocol.ProvideMode,
		group *ipPacketGroup,
		timeout time.Duration,
	) bool {
		return upstream.sendPacketGroup(source, provideMode, group, timeout)
	})
}

// SetSettings updates the mux's DNS and HTTP policy at runtime. The tun's DohCache is
// rebuilt so a changed DNS resolution path takes effect immediately; HTTP mode changes
// apply to subsequent connections (the terminator binds lazily on the first HTTPS
// claim). settings must be non-nil — disabling the mux entirely is a teardown, not a
// settings change.
func (self *UpgradeMux) SetSettings(settings *UpgradeMuxSettings) {
	self.settings.Store(settings)
	// The replacement resolver has no relationship to a success/failure still
	// completing on the old cache. Invalidate first; SetDnsResolverSettings
	// then closes and joins that retired cache.
	self.markTunnelDohUnproven()
	self.mux.Tun().SetDnsResolverSettings(dnsResolverSettings(settings), dohRequestTimeout(settings))
	if replaced := self.fallbackDohCache.Swap(buildFallbackDohCache(fallbackResolverSettings(settings), dnsUpgradeMemoryTarget(settings), dnsUpgradeServerStatsSeed(settings))); replaced != nil {
		// release the replaced cache's pooled connections now instead of holding them
		// (and their keepalive pings) until the idle timeout
		replaced.Close()
	}
	self.wakeColdProber()
	self.warmFallbackDns()
}

// ShedMemory drops the mux's recoverable caches under host memory pressure: the resolver
// caches (with their pooled connections) fully, and the IP→hostname affinity map down to
// its most-recently-active half. The active affinity records are the small, useful part
// (live flows re-look-up affinity only on a new flow), so they are retained — a full drop
// would flip active flows to by-IP routing and blank the block-action host feed — while
// the idle bulk is released.
func (self *UpgradeMux) ShedMemory() {
	self.mux.Tun().DohCache().ShedMemory()
	if fallback := self.fallbackDohCache.Load(); fallback != nil {
		fallback.ShedMemory()
	}
	self.reverse.shed()
	self.sni.shed()
}

// AdoptServerNames seeds this mux's reverse index with the names a prior mux learned,
// so a rebuild (e.g. on reconnect / location change) doesn't blank the IP→hostname
// affinity — and the block-action host feed — for flows the OS keeps open across the
// reconnect (server names outlive the physical connection). No-op on a nil prior.
func (self *UpgradeMux) AdoptServerNames(prior *UpgradeMux) {
	if prior == nil {
		return
	}
	self.reverse.adoptFrom(prior.reverse)
}

func (self *UpgradeMux) Close() {
	self.cancel()
	if self.firstLoad != nil {
		self.firstLoad.Close()
	}
	for _, listener := range self.dnsTcpListeners {
		listener.Close()
	}
	self.unregisterShed()
	if fallback := self.fallbackDohCache.Load(); fallback != nil {
		fallback.Close()
	}
	self.mux.Close()
}
