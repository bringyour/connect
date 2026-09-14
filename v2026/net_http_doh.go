package connect

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"math"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/netip"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/dns/dnsmessage"
	"golang.org/x/net/idna"
	"maps"

	"golang.org/x/net/http2"
	// "github.com/urnetwork/glog/v2026"
)

// FIXME DoH certs need to be included in the pinned certs

const (
	// dohServerWeightFloor keeps every server a small chance of being tried first (exploration), so
	// a server that recovers can climb back even after a streak of failures.
	dohServerWeightFloor = 0.05
	// maxDohResponseBytes caps a DoH response body read (a memory guard); a real DNS answer is tiny
	// (typically <1 KiB, a few KiB for dnssec-heavy or ech/https records), so this only bounds a
	// hostile or broken server. It doubles as the per-request reservation unit against the dns
	// memory target, so the target divided by this is the guaranteed-parallelism floor.
	maxDohResponseBytes = 16 * 1024
	// dohStaleServeBound is how long past its expiration a resolved answer may still be served
	// when a FRESH resolution fails (RFC 8767 serve-stale). The failure this exists for is exit
	// failover: DNS is the one dependency every new connection shares, and the moment every
	// resolver path is briefly unreachable (the tunnel re-racing onto a new exit) is exactly the
	// moment a burst of resolutions arrives — answering SERVFAIL then makes DNS the reason the
	// failover looks broken. Failover completes in seconds-to-a-minute, so 5 minutes covers it
	// with margin while keeping the worst-case staleness far below the RFC's permitted days —
	// an address that was correct 5 minutes ago is overwhelmingly still correct, and a wrong one
	// costs one failed connect followed by a re-resolve. Stale answers are only served when the
	// fresh resolve FAILS (resolver failure / non-authoritative empty), never over an
	// authoritative answer (records or NXDOMAIN/NODATA), and retained entries stay subject to
	// the CacheMaxEntries cap in pruneCacheLocked, so the bound adds no unmetered memory.
	dohStaleServeBound = 5 * time.Minute
	// dohQueryReserveByteCount is the dns memory target reservation held for the lifetime of one
	// in-flight DoH HTTP request (the response read cap; the request wire and goroutine are noise
	// next to it)
	dohQueryReserveByteCount = maxDohResponseBytes
	// dnsLookupReserveByteCount is the dns memory target reservation for one plain-dns LookupIP
	// (small udp/tcp exchanges via net.Resolver, no doh response body)
	dnsLookupReserveByteCount = 4 * 1024
	// dohTlsSessionCacheCapacity bounds a DoH client's TLS session ticket cache; a ticket or
	// two per configured server is plenty (see httpClientWithDialer).
	dohTlsSessionCacheCapacity = 16
	// dohSeedMaxScore clamps a persisted per-server score on seed (see serverStats.seed): high
	// enough to make the last session's fastest server the clear first pick, low enough that a
	// few live successes on another server can overturn a stale ordering.
	dohSeedMaxScore = 8.0
	// dohWarmDomain is the benign, universally-answered name Warm queries to open a server
	// connection (TCP+TLS+h2) ahead of the first real lookup. The answer is never cached.
	dohWarmDomain = "example.com"
)

// dohServerWindows are the trailing time spans over which each server's successful resolutions are
// counted (per-window token buckets). A success falls inside every window whose span covers it, so a
// recent success is counted in more windows and weighted more — the fan-out order then favors
// servers that have resolved most recently and most often. See serverStats.
var dohServerWindows = []time.Duration{
	5 * time.Minute,
	15 * time.Minute,
	60 * time.Minute,
}

func DefaultDohSettings() *DohSettings {
	return &DohSettings{
		ConnectSettings: *DefaultConnectSettings(),
		// both families: every resolver list below is consulted and, for DoH,
		// each operator's v4 and v6 endpoint is raced as a pair (see
		// dohLaunchWaves). A caller that knows its path carries one family
		// pins 4 or 6.
		IpVersion:       0,
		MissExpiration:  300 * time.Second,
		LocalExpiration: 300 * time.Second,
		MinCacheTtl:     30 * time.Second,
		// per doh cache, so scaled by the memory budget
		CacheMaxEntries:           MemoryScaledCount(4096, 512),
		MaxConcurrentResolutions:  64,
		MaxConcurrentHttpRequests: 16,
		// hedged requests, conditioned on load: an ISOLATED query (in-flight
		// below DohServerRaceMaxInFlight) races its fanned-out servers
		// immediately (stagger 0) — completing at the min of the raced rtts,
		// so a just-died first pick costs the surviving server's rtt
		// (measured 812ms -> 67ms worst case). A BURST (page load, first
		// load) keeps the stagger: racing a burst doubles the stream volume
		// on the single shared h2 connection through the (cold) tunnel at
		// exactly the wrong moment — measured as a real first-load
		// regression on device. See PACKETRESEARCH1 §11.
		DohServerStagger:         750 * time.Millisecond,
		DohServerWarmStagger:     100 * time.Millisecond,
		DohServerRaceMaxInFlight: 4,
		DohServerHedgeReserve:    4,
		DnsResolverSettings:      DefaultDnsResolverSettings(),
	}
}

// dnsTargetHttpConcurrency returns the in-flight http request cap implied by
// a dns memory target's byte capacity (the capacity divided by the
// per-request reservation), or `fallback` when there is no target. With a
// target, the count cap is a generous upper bound and the owner's byte
// target is the real limiter (see DohSettings.MemoryTarget).
func dnsTargetHttpConcurrency(targetByteCount ByteCount, fallback int) int {
	if 0 < targetByteCount {
		return max(fallback, int(targetByteCount/dohQueryReserveByteCount))
	}
	return fallback
}

// dnsTargetCacheEntries derives a resolver cache entry cap from a dns memory
// target's byte capacity: one entry per 4 KiB of target (roughly a tenth of
// the target in actual entry bytes), or `fallback` when there is no target
func dnsTargetCacheEntries(targetByteCount ByteCount, fallback int) int {
	if 0 < targetByteCount {
		return max(fallback, int(targetByteCount/kib(4)))
	}
	return fallback
}

// the resolver tries the following sequence until there is a found record:
// 1. if enable remote doh, remote doh
// 2. if enable local doh, local doh (host-dialed, e.g. a sidecar resolver)
// 3. if enable remote dns, remote dns
// 4. if enable local dns, local dns
//
// the remote doh servers are queried as RFC 8484 wire-format (application/dns-message) in a
// staggered, weighted-random order (see dohClient.queryResult); the first server to return records
// wins. each must present an IP-SAN cert when addressed by IP (these do). Cloudflare, Google, Quad9,
// and OpenDNS all serve wire-format on :443 /dns-query.
// https://developers.cloudflare.com/1.1.1.1/encryption/dns-over-https/
// https://developers.google.com/speed/public-dns/docs/doh
func DefaultDnsResolverSettings() *DnsResolverSettings {
	return &DnsResolverSettings{
		EnableRemoteDoh:       true,
		EnableLocalDns:        true,
		DnsUpgradeMaskAddress: DefaultDnsUpgradeMaskAddress,
		// the v4 and v6 lists pair by operator (knownDohUrlPairs) and a
		// query races each pair as one wave (see dohLaunchWaves)
		RemoteDohUrlsIpv4: []string{
			"https://1.1.1.1/dns-query",        // Cloudflare
			"https://8.8.8.8/dns-query",        // Google
			"https://9.9.9.9/dns-query",        // Quad9
			"https://208.67.222.222/dns-query", // OpenDNS
		},
		RemoteDohUrlsIpv6: []string{
			"https://[2606:4700:4700::1111]/dns-query", // Cloudflare
			"https://[2001:4860:4860::8888]/dns-query", // Google
			"https://[2620:fe::fe]/dns-query",          // Quad9
			"https://[2620:119:35::35]/dns-query",      // OpenDNS
		},
		// remote plain-dns servers, dialed through the tunnel. remote dns
		// stays disabled by default for general resolution — while disabled,
		// the doh server names are the only names permitted to resolve over
		// it (see DohCache.resolve), so a hostname-form doh server never
		// leaks to the local resolver
		RemoteDnsIpv4: []string{
			"1.1.1.1",        // Cloudflare
			"9.9.9.9",        // Quad9
			"8.8.8.8",        // Google
			"208.67.222.222", // OpenDNS
		},
		RemoteDnsIpv6: []string{
			"2606:4700:4700::1111", // Cloudflare
			"2620:fe::fe",          // Quad9
			"2001:4860:4860::8888", // Google
			"2620:119:35::35",      // OpenDNS
		},
		// local plain-dns servers: host-side resolution, and the actual tunnel
		// resolver targets when the local-dns toggle is explicitly enabled. These
		// are independent of DnsUpgradeMaskAddress, which is only the destination
		// advertised to the OS while UpgradeMux owns plain DNS interception.
		LocalDnsIpv4: []string{
			"9.9.9.9", // Quad9
			"1.1.1.1", // Cloudflare
		},
		LocalDnsIpv6: []string{
			"2620:fe::fe",          // Quad9
			"2606:4700:4700::1111", // Cloudflare
		},
	}
}

type DohSettings struct {
	ConnectSettings
	IpVersion       int
	MissExpiration  time.Duration
	LocalExpiration time.Duration
	// MinCacheTtl floors the cache lifetime of a resolved record so very-low / zero-TTL records
	// don't re-resolve (a full fan-out) on nearly every query. 0 disables the floor.
	MinCacheTtl     time.Duration
	CacheMaxEntries int
	// MaxConcurrentResolutions bounds in-flight resolutions (DohCache.resolveSem) so a burst
	// or flood of distinct names cannot fan out unbounded. 0 uses a sane default.
	MaxConcurrentResolutions int
	// MaxConcurrentHttpRequests hard-caps concurrent in-flight DoH HTTP requests (DohCache.httpSem),
	// the dominant memory cost under load on a constrained host (the iOS network extension). It
	// bounds the actual requests regardless of resolution count or per-server fan-out. 0 uses a sane
	// default.
	MaxConcurrentHttpRequests int
	// DohServerStagger delays launching each additional DoH server within a fan-out: the first
	// server is queried immediately and each next one only if no answer has arrived within this
	// interval, so a healthy primary answers before the redundant servers fire. 0 fans out to all
	// servers at once.
	DohServerStagger time.Duration
	// DohServerWarmStagger is the shorter post-formation hedge delay. It is
	// used only while DohPathWarm reports true, and never lengthens
	// DohServerStagger. Zero disables the state-aware override.
	DohServerWarmStagger time.Duration
	// MaxServersPerQuery caps how many DoH servers a single query fans out to (in weighted
	// order, so the best recent performers are the ones tried). On a dead path every launched
	// request hangs until the deadline holding memory, so a memory-constrained host caps the
	// fan-out and relies on the weighted rotation across queries to explore the other servers.
	// 0 fans out to all servers.
	MaxServersPerQuery int
	// DohServerRaceMaxInFlight conditions the hedge: when fewer than this
	// many resolutions are active on the cache, a query races its servers
	// immediately (effective stagger 0 — the interactive/tail-latency win).
	// Admission uses one shared atomic counter, so at most this many
	// concurrent resolutions bypass the stagger even when a burst starts at
	// once. 0 disables the race (always stagger).
	DohServerRaceMaxInFlight int
	// DohServerHedgeReserve keeps this many MaxConcurrentHttpRequests slots
	// unavailable to first-wave requests while DohPathWarm is true. Timed
	// second-server hedges can therefore run even when a stale/dead primary
	// has filled the ordinary wave. Zero disables the reserve.
	DohServerHedgeReserve int
	// DohPathWarm reports whether the shared tunnel path is proven/warm.
	// nil means state-aware staggering and hedge reservation are disabled.
	// UpgradeMux supplies an atomic, allocation-free callback.
	DohPathWarm         func() bool
	DnsResolverSettings *DnsResolverSettings
	// MemoryTarget, when set, is the owner's live dns byte budget: every
	// in-flight DoH request reserves the response read ceiling
	// (`dohQueryReserveByteCount`) from it for the request's lifetime, and
	// plain-dns lookups reserve a smaller unit — so the owner's in-flight
	// resolution memory tracks the target, waiting (not failing) when it is
	// exhausted. The owner shares one target across its caches (e.g. the
	// mux's tunnel + fallback resolvers). nil disables the byte bound.
	MemoryTarget *MemoryTarget
	// ServerStatsSeed, when set, pre-loads the per-server success stats with the given
	// scores (url -> score, clamped to dohSeedMaxScore) at construction, so the weighted
	// fan-out order starts from the last session's experience — the first queries go to the
	// server that was fastest then — instead of uniform-random. Live results take over as
	// they accrue (seeds decay on the same trailing windows). See DohCache.ServerScores.
	ServerStatsSeed map[string]float64
	// DohServerResolvedCallback, when set, is called after a doh server name
	// (the hostname of a remote doh url) resolves, with the resolved
	// addresses. the upgrade mux records these into its ip→hostname reverse
	// index, so the server addresses are excluded from the override and
	// association logic along with the server names
	// (see `reverseIndex.record` and `SetBlockActionIgnoreHosts`)
	DohServerResolvedCallback func(domain string, addrs []netip.Addr)
	// DohResultCallback observes a successful remote-DoH address answer and
	// the exact tunnel TCP flow that delivered it. UpgradeMux uses this to pin
	// the resulting destination names to the same provider exit, preserving
	// topology-sensitive DNS locality without exposing the queried name in
	// telemetry. nil and host-dialed DoH remain inert.
	DohResultCallback func(domain string, addrs []netip.Addr, route *DohRoute)
}

// DohRoute identifies the tunnel TCP connection that delivered a DoH answer.
// It intentionally contains only the socket tuple; the owning multi-client
// resolves that tuple to its current provider channel.
type DohRoute struct {
	Local  netip.AddrPort
	Remote netip.AddrPort
}

func (self *DohSettings) ResolverIp() string {
	switch self.IpVersion {
	case 4:
		return "ip4"
	case 6:
		return "ip6"
	default:
		return "ip"
	}
}

// DefaultDnsUpgradeMaskAddress is the plain-DNS destination advertised to a
// tunnel's OS resolver while UpgradeMux owns UDP/TCP :53. It is deliberately
// separate from the upstream resolver lists: no DNS service is expected at
// this address, because UpgradeMux claims the packet and upgrades it to DoH
// before the destination is reached.
const DefaultDnsUpgradeMaskAddress = "65.49.70.65"

type DnsResolverSettings struct {
	EnableRemoteDoh bool `json:"enable_remote_doh,omitempty"`
	EnableLocalDoh  bool `json:"enable_local_doh,omitempty"`
	EnableRemoteDns bool `json:"enable_remote_dns,omitempty"`
	EnableLocalDns  bool `json:"enable_local_dns,omitempty"`
	// DnsUpgradeMaskAddress is a stand-in destination for the platform's plain
	// DNS configuration while UpgradeMux intercepts UDP/TCP :53. It is not an
	// upstream resolver and must not be dialed by DohCache.
	DnsUpgradeMaskAddress string `json:"dns_upgrade_mask_address,omitempty"`
	// DoH server URLs, queried as RFC 8484 wire-format (GET ?dns=<base64url DNS message>,
	// Accept application/dns-message). Each must present an IP-SAN cert when addressed by IP.
	RemoteDohUrlsIpv4 []string `json:"remote_doh_urls_ipv4,omitempty"`
	RemoteDohUrlsIpv6 []string `json:"remote_doh_urls_ipv6,omitempty"`
	LocalDohUrlsIpv4  []string `json:"local_doh_urls_ipv4,omitempty"`
	LocalDohUrlsIpv6  []string `json:"local_doh_urls_ipv6,omitempty"`
	RemoteDnsIpv4     []string `json:"remote_dns_ipv4,omitempty"`
	RemoteDnsIpv6     []string `json:"remote_dns_ipv6,omitempty"`
	LocalDnsIpv4      []string `json:"local_dns_ipv4,omitempty"`
	LocalDnsIpv6      []string `json:"local_dns_ipv6,omitempty"`

	// TlsConfig, if set, is used by the DoH HTTP clients — production cert pinning,
	// or trusting a local server's cert in tests. Not serialized.
	TlsConfig *tls.Config `json:"-"`
}

// configureDohHttp2Transport applies DoH's keepalive policy and the same
// socket-progress invariant as the native net/http HTTP/2 clients.
func configureDohHttp2Transport(h2tr *http2.Transport, settings *DohSettings) {
	h2tr.ReadIdleTimeout = 30 * time.Second
	h2tr.PingTimeout = 15 * time.Second
	// Context cancellation cannot interrupt an HTTP/2 flow-control or reset
	// write already holding the connection write mutex. Give the socket write
	// its own progress bound, as the ordinary API transports do.
	h2tr.WriteByteTimeout = settings.ConnectTimeout
}

// httpClientWithDialer builds a DoH HTTP client over the given dialer. Remote DoH
// uses the tun dialer (settings.DialContext); local DoH uses the host dialer.
// sessionCache holds TLS session tickets so a re-dial resumes instead of paying a
// full handshake; the owner passes a distinct cache per dial path (see NewDohCache).
func httpClientWithDialer(settings *DohSettings, dialContext DialContextFunction, sessionCache tls.ClientSessionCache) *http.Client {
	tr := &http.Transport{
		DialContext:         dialContext,
		TLSHandshakeTimeout: settings.TlsTimeout,
		// keep the (typically single) DoH connection pooled across bursts so lookups don't
		// re-pay a TCP+TLS handshake over the tunnel. Long: with session resumption the
		// re-dial is cheap, but not re-dialing at all is cheaper still, and an idle h2
		// connection is small (the keepalive pings below hold NAT state open).
		IdleConnTimeout: 15 * time.Minute,
	}
	// TLS session resumption: cache the server's session tickets so a re-dial — after an
	// idle close, a memory shed, or a mux rebuild sharing this cache — resumes via TLS 1.3
	// PSK (one round trip) instead of a full handshake. Through a cold tunnel each saved
	// round trip is user-visible first-load time.
	var tlsConfig *tls.Config
	if settings.DnsResolverSettings != nil && settings.DnsResolverSettings.TlsConfig != nil {
		tlsConfig = settings.DnsResolverSettings.TlsConfig.Clone()
	} else {
		tlsConfig = &tls.Config{}
	}
	if tlsConfig.ClientSessionCache == nil {
		tlsConfig.ClientSessionCache = sessionCache
	}
	tr.TLSClientConfig = tlsConfig
	// most doh providers discontinued http1.1 late 2025; force h2 instead of the default
	// h1->h2 autonegotiate, since that no longer works.
	// see https://quad9.net/news/blog/doh-http-1-1-retirement/
	// ConfigureTransports (plural) returns the h2 transport so we can keep the connection
	// warm: ReadIdleTimeout sends keepalive PINGs while idle, which both holds the pooled
	// connection open across bursts and detects a dead tunnel so the next query re-dials
	// rather than stalling on a half-open connection.
	h2tr, err := http2.ConfigureTransports(tr)
	if err != nil {
		panic(err)
	}
	configureDohHttp2Transport(h2tr, settings)
	httpClient := &http.Client{
		Timeout:   settings.RequestTimeout,
		Transport: tr,
	}
	return httpClient
}

type DohCache struct {
	// remoteClient resolves over the tun (settings.DialContext); localClient over the host. Both
	// share httpSem (the global in-flight cap) and the per-server success stats.
	remoteClient   *dohClient
	localClient    *dohClient
	remoteResolver *net.Resolver
	localResolver  *net.Resolver
	settings       *DohSettings
	log            Logger

	// the hostname-form remote doh server names (lowercase). these can not
	// resolve through remote doh (circular), so they resolve over remote
	// plain dns through the tunnel even when EnableRemoteDns is false — the
	// one permitted consumer while it is disabled (see resolve)
	dohServerNames map[string]bool

	stateLock             sync.Mutex
	queryResultExpiration map[DohKey]*DohResult
	// in-flight resolutions keyed by query: concurrent identical queries (retry storms, the
	// A/AAAA split, multi-client dups) coalesce onto one resolution (single-flight). guarded
	// by stateLock.
	inflight map[DohKey]*dohFlight

	// bounds concurrent resolutions so a flood of distinct names can't fan out unbounded
	resolveSem chan struct{}

	// lifecycle cancels and joins every HTTP request and transport dial when
	// Close permanently retires this cache. net/http may deliberately detach a
	// connection attempt from the request that initiated it so another request
	// can reuse the result; tracking both layers prevents such a late dial from
	// installing an h2 connection after CloseIdleConnections already ran.
	lifecycle *dohCacheLifecycle

	// staleServeCount counts stale answers served under dohStaleServeBound (RFC 8767): each
	// increment pairs with the per-serve log line in QueryResult. An atomic (not stateLock) so
	// tests can read it without reaching into the lock, and so the zero value works on any
	// DohCache.
	staleServeCount atomic.Uint64
}

type dohCacheLifecycle struct {
	ctx    context.Context
	cancel context.CancelFunc

	stateLock sync.Mutex
	closing   bool
	retired   atomic.Bool
	workers   sync.WaitGroup
}

func newDohCacheLifecycle() *dohCacheLifecycle {
	ctx, cancel := context.WithCancel(context.Background())
	return &dohCacheLifecycle{
		ctx:    ctx,
		cancel: cancel,
	}
}

// context admits one request/dial before shutdown and links its caller context
// to the cache lifetime. Add and shutdown's Wait are serialized by stateLock,
// so a detached net/http dial cannot appear after shutdown begins.
func (self *dohCacheLifecycle) context(ctx context.Context) (context.Context, func(), bool) {
	self.stateLock.Lock()
	if self.closing {
		self.stateLock.Unlock()
		return nil, nil, false
	}
	self.workers.Add(1)
	lifetimeCtx := self.ctx
	self.stateLock.Unlock()

	linkedCtx, linkedCancel := context.WithCancel(ctx)
	stopLifetimeCancel := context.AfterFunc(lifetimeCtx, linkedCancel)
	return linkedCtx, func() {
		stopLifetimeCancel()
		linkedCancel()
		self.workers.Done()
	}, true
}

func (self *dohCacheLifecycle) dialContext(dialContext DialContextFunction) DialContextFunction {
	return func(ctx context.Context, network string, address string) (net.Conn, error) {
		linkedCtx, done, ok := self.context(ctx)
		if !ok {
			return nil, context.Canceled
		}
		defer done()
		return dialContext(linkedCtx, network, address)
	}
}

func (self *dohCacheLifecycle) shutdown() {
	// Fast-path gate for public cache operations. Store before taking the
	// admission lock: an operation already inside context() is still joined,
	// while every operation beginning after retirement is rejected without
	// allocating a linked context.
	self.retired.Store(true)
	self.stateLock.Lock()
	if !self.closing {
		self.closing = true
		self.cancel()
	}
	self.stateLock.Unlock()
	self.workers.Wait()
}

// dohFlight is one in-flight resolution shared by every caller waiting on the same query. the
// leader resolves, sets addrs/authoritative, then closes done to release the waiters.
type dohFlight struct {
	done          chan struct{}
	addrs         []netip.Addr
	authoritative bool
}

func dnsResolverAddrs(settings *DohSettings, remote bool, network string) []string {
	var ipv4 []string
	var ipv6 []string
	if remote {
		ipv4 = settings.DnsResolverSettings.RemoteDnsIpv4
		ipv6 = settings.DnsResolverSettings.RemoteDnsIpv6
	} else {
		ipv4 = settings.DnsResolverSettings.LocalDnsIpv4
		ipv6 = settings.DnsResolverSettings.LocalDnsIpv6
	}

	switch {
	case strings.HasSuffix(network, "6") || settings.IpVersion == 6:
		if 0 < len(ipv6) {
			return ipv6
		}
		return ipv4
	case strings.HasSuffix(network, "4") || settings.IpVersion == 4:
		if 0 < len(ipv4) {
			return ipv4
		}
		return ipv6
	default:
		// both families, operator-paired and so interleaved v4 first: the
		// plain-dns dial hook walks this list one entry per resolver attempt
		// (see NewDohCache), so two attempts cover both families whichever
		// one the path lacks
		return dualStackList(ipv4, ipv6, knownDnsServerPairs)
	}
}

// dnsLookupNetwork is the net.Resolver network for a plain-dns lookup of one
// record type: an A query must look up ip4 and an AAAA query ip6, whatever the
// cache-wide IpVersion says, or the answer of one family would be cached under
// the other's key. The second result is false when IpVersion excludes the
// record type's family, in which case the lookup is skipped.
func dnsLookupNetwork(recordType string, ipVersion int) (string, bool) {
	switch recordType {
	case "A":
		return "ip4", ipVersion != 6
	case "AAAA":
		return "ip6", ipVersion != 4
	default:
		return "", false
	}
}

func netIPAddr(ip net.IP) (netip.Addr, bool) {
	if ip4 := ip.To4(); ip4 != nil {
		addr, ok := netip.AddrFromSlice(ip4)
		return addr, ok
	}
	if ip16 := ip.To16(); ip16 != nil {
		addr, ok := netip.AddrFromSlice(ip16)
		return addr, ok
	}
	return netip.Addr{}, false
}

func authoritativeDnsMiss(err error) bool {
	var dnsErr *net.DNSError
	return errors.As(err, &dnsErr) && dnsErr.IsNotFound
}

func NewDohCache(settings *DohSettings) *DohCache {
	lifecycle := newDohCacheLifecycle()
	// whether this cache's remote path rides the egress-bound host dialer
	// (DefaultDohSettings) or an owner-supplied dial context (the mux's
	// in-tunnel path) — carried into the control-dial evidence lines
	remoteBound := settings.DialContextSettings == nil
	// each resolver attempt takes the next server in the interleaved list
	// (dnsResolverAddrs), so a family the path lacks costs one attempt, not
	// a coin flip per attempt
	var remoteDnsNext atomic.Uint64
	var localDnsNext atomic.Uint64
	remoteResolver := &net.Resolver{
		PreferGo: true,
		Dial: lifecycle.dialContext(func(ctx context.Context, network string, addr string) (net.Conn, error) {
			_, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			localAddrs := dnsResolverAddrs(settings, true, network)
			if len(localAddrs) == 0 {
				return nil, fmt.Errorf("no remote DNS resolvers configured")
			}
			localAddr := localAddrs[int((remoteDnsNext.Add(1)-1)%uint64(len(localAddrs)))]
			addr = net.JoinHostPort(localAddr, port)
			conn, err := settings.DialContext(ctx, network, addr)
			logControlDialResult(settings.Log, "dns", remoteBound, network, addr, conn, err)
			return conn, err
		}),
	}

	netDialer := settings.NetDialer()
	localResolver := &net.Resolver{
		PreferGo: true,
		Dial: lifecycle.dialContext(func(ctx context.Context, network string, addr string) (net.Conn, error) {
			_, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			localAddrs := dnsResolverAddrs(settings, false, network)
			if len(localAddrs) == 0 {
				return nil, fmt.Errorf("no local DNS resolvers configured")
			}
			localAddr := localAddrs[int((localDnsNext.Add(1)-1)%uint64(len(localAddrs)))]
			addr = net.JoinHostPort(localAddr, port)
			conn, err := netDialer.DialContext(ctx, network, addr)
			logControlDialResult(settings.Log, "dns", true, network, addr, conn, err)
			return conn, err
		}),
	}

	maxResolutions := settings.MaxConcurrentResolutions
	if maxResolutions <= 0 {
		maxResolutions = 4 * dnsTargetHttpConcurrency(settings.MemoryTarget.Capacity(), 16)
	}

	// distinct TLS session caches per dial path: a ticket obtained via the host egress
	// (localClient) must never be redeemed through the tunnel (remoteClient) — ticket reuse
	// across paths would let the DoH server link the host address with the tunnel egress.
	// Within a path, resumption saves a handshake round trip on every re-dial.
	httpClient := httpClientWithDialer(settings, lifecycle.dialContext(wrapControlDial("doh", settings.Log, remoteBound, settings.DialContext)), tls.NewLRUClientSessionCache(dohTlsSessionCacheCapacity))
	localHttpClient := httpClientWithDialer(settings, lifecycle.dialContext(wrapControlDial("doh", settings.Log, true, netDialer.DialContext)), tls.NewLRUClientSessionCache(dohTlsSessionCacheCapacity))
	// one in-flight-request semaphore and one stats table shared across the remote + local clients,
	// so the cap bounds the cache's total concurrent DoH requests
	httpConcurrency := maxConcurrentHttpRequests(settings)
	httpSem := make(chan struct{}, httpConcurrency)
	primarySem := newDohPrimarySem(httpConcurrency, settings.DohServerHedgeReserve)
	activeQueries := &atomic.Int64{}
	stats := newServerStats()
	// seed the fan-out order from the last session's per-server scores (if the owner
	// persisted any), so the first queries pick the known-fastest server instead of
	// spending the first minutes re-learning the ordering
	stats.seed(settings.ServerStatsSeed)

	// the hostname-form remote doh server names (see the field doc)
	dohServerNames := map[string]bool{}
	if settings.DnsResolverSettings != nil {
		dohUrlLists := [][]string{
			settings.DnsResolverSettings.RemoteDohUrlsIpv4,
			settings.DnsResolverSettings.RemoteDohUrlsIpv6,
		}
		for _, dohUrls := range dohUrlLists {
			for _, dohUrl := range dohUrls {
				u, err := url.Parse(strings.TrimSpace(dohUrl))
				if err != nil {
					continue
				}
				host := u.Hostname()
				if host == "" {
					continue
				}
				if _, err := netip.ParseAddr(host); err != nil {
					dohServerNames[strings.ToLower(host)] = true
				}
			}
		}
	}

	siblings := dohServerSiblings(settings.DnsResolverSettings)

	return &DohCache{
		remoteClient:          &dohClient{httpClient: httpClient, httpSem: httpSem, primarySem: primarySem, activeQueries: activeQueries, stats: stats, memoryTarget: settings.MemoryTarget, lifecycle: lifecycle, captureRoute: settings.DialContextSettings != nil, siblings: siblings},
		localClient:           &dohClient{httpClient: localHttpClient, httpSem: httpSem, primarySem: primarySem, activeQueries: activeQueries, stats: stats, memoryTarget: settings.MemoryTarget, lifecycle: lifecycle, siblings: siblings},
		remoteResolver:        remoteResolver,
		localResolver:         localResolver,
		settings:              settings,
		log:                   loggerOrDefault(settings.Log),
		dohServerNames:        dohServerNames,
		queryResultExpiration: map[DohKey]*DohResult{},
		inflight:              map[DohKey]*dohFlight{},
		resolveSem:            make(chan struct{}, maxResolutions),
		lifecycle:             lifecycle,
	}
}

func maxConcurrentHttpRequests(settings *DohSettings) int {
	if 0 < settings.MaxConcurrentHttpRequests {
		return settings.MaxConcurrentHttpRequests
	}
	return dnsTargetHttpConcurrency(settings.MemoryTarget.Capacity(), 16)
}

func newDohPrimarySem(httpConcurrency int, hedgeReserve int) chan struct{} {
	reserve := min(max(0, hedgeReserve), max(0, httpConcurrency-1))
	if reserve == 0 {
		return nil
	}
	return make(chan struct{}, httpConcurrency-reserve)
}

// CloseIdleConnections releases the cache's currently idle pooled DoH
// connections while keeping the cache usable. Network changes and memory
// pressure use this operation before a later query or warm re-dials.
func (self *DohCache) CloseIdleConnections() {
	self.remoteClient.httpClient.CloseIdleConnections()
	self.localClient.httpClient.CloseIdleConnections()
}

// Close permanently retires the cache. It cancels and joins both HTTP
// requests and transport dials before closing the idle pools, including dials
// net/http detached from their initiating request for possible reuse.
func (self *DohCache) Close() {
	self.lifecycle.shutdown()
	self.CloseIdleConnections()
}

// ServerScores returns the per-server success scores driving the fan-out order, for the owner
// to persist and pass back as ServerStatsSeed on the next construction (the remote and local
// clients share one stats table, so this is the cache's full view).
func (self *DohCache) ServerScores() map[string]float64 {
	return self.remoteClient.stats.scores()
}

// Warm opens the cache's DoH server connections ahead of the first real query: it issues one
// minimal query (dohWarmDomain) to each of the top serverCount servers in the current weighted
// order — with a seeded ordering (ServerStatsSeed), the servers the next real queries will
// actually pick — paying the TCP+TLS+h2 handshake off the user's critical path. A remote-DoH
// cache warms through the tun dialer: a dial parked on a still-establishing tunnel completes
// the handshake the moment the tunnel can carry traffic, so calling this at connect start
// self-times to the earliest useful moment. A local-DoH cache (the mux's host-egress fallback)
// warms over the host dialer. Results are recorded into the server stats but never the answer
// cache. Blocking (bounded by RequestTimeout) and reports whether any server answered; run it
// in the background.
func (self *DohCache) Warm(ctx context.Context, serverCount int) bool {
	if self.lifecycle.retired.Load() {
		return false
	}
	queryLifetimeCtx, queryLifetimeDone, ok := self.lifecycle.context(ctx)
	if !ok {
		return false
	}
	defer queryLifetimeDone()
	ctx = queryLifetimeCtx

	settings := self.settings
	rs := settings.DnsResolverSettings
	if rs == nil || settings.RequestTimeout <= 0 {
		return false
	}
	var client *dohClient
	var dohUrls []string
	switch {
	case rs.EnableRemoteDoh:
		client = self.remoteClient
		dohUrls = remoteDohUrls(settings, settings.IpVersion)
	case rs.EnableLocalDoh:
		client = self.localClient
		dohUrls = localDohUrls(settings, settings.IpVersion)
	default:
		return false
	}
	ordered := client.stats.order(dohUrls)
	if 0 < serverCount && serverCount < len(ordered) {
		ordered = ordered[:serverCount]
	}
	if len(ordered) == 0 {
		return false
	}

	queryCtx, queryCancel := context.WithTimeout(ctx, settings.RequestTimeout)
	defer queryCancel()

	var successCount atomic.Int32
	var successCancelOnce sync.Once
	var failureLock sync.Mutex
	var firstFailure string
	var warmWg sync.WaitGroup
warmLoop:
	for _, dohUrl := range ordered {
		// respect the shared in-flight cap and byte budget like any real request, so a warm
		// can never crowd out a user query
		if client.httpSem != nil {
			select {
			case client.httpSem <- struct{}{}:
			case <-queryCtx.Done():
				break warmLoop
			}
		}
		if !settings.MemoryTarget.Acquire(queryCtx, dohQueryReserveByteCount) {
			if client.httpSem != nil {
				<-client.httpSem
			}
			break warmLoop
		}
		warmWg.Add(1)
		go HandleError(func() {
			defer warmWg.Done()
			defer settings.MemoryTarget.Release(dohQueryReserveByteCount)
			if client.httpSem != nil {
				defer func() { <-client.httpSem }()
			}
			result, queryErr := client.queryWireDetailed(queryCtx, dohUrl, "A", dohWarmDomain)
			ok := 0 < len(result.AddrTtls) || result.Miss
			if ok {
				client.stats.record(dohUrl, true)
				successCount.Add(1)
				// Warm needs one usable connection, not every configured
				// provider. Cancel a dead/slow sibling immediately; waiting for
				// all probes made one broken server consume the full timeout
				// even after another had already proved the tunnel path.
				successCancelOnce.Do(queryCancel)
			} else if successCount.Load() == 0 {
				client.stats.record(dohUrl, false)
				var failure string
				if queryErr != nil {
					failure = queryErr.Error()
				} else {
					failure = fmt.Sprintf("%s returned no usable A answer", dohUrl)
				}
				failureLock.Lock()
				if firstFailure == "" {
					firstFailure = failure
				}
				failureLock.Unlock()
			}
		})
	}
	warmWg.Wait()
	if 0 < successCount.Load() {
		return true
	}
	if firstFailure != "" {
		self.log.Infof("[dns]warm failed: %s\n", firstFailure)
	}
	return false
}

// ShedMemory drops the query result cache and releases the pooled connections, for the host's
// memory pressure signal. Subsequent queries re-resolve and re-dial.
func (self *DohCache) ShedMemory() {
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		clear(self.queryResultExpiration)
	}()
	self.CloseIdleConnections()
}

func (self *DohCache) pruneCacheLocked(now time.Time, reserve int) {
	for key, result := range self.queryResultExpiration {
		// an expired entry is retained while it is still stale-servable (RFC 8767, see
		// dohStaleServeBound) so a resolution failure during that window can fall back to it.
		// memory stays bounded regardless: the CacheMaxEntries cap below evicts oldest-first
		// over ALL entries, fresh and stale alike.
		if !result.Valid(now, self.settings.MissExpiration) && !result.staleUsable(now) {
			delete(self.queryResultExpiration, key)
		}
	}

	maxEntries := self.settings.CacheMaxEntries
	for maxEntries < len(self.queryResultExpiration)+reserve {
		var oldestKey DohKey
		var oldestTime time.Time
		found := false
		for key, result := range self.queryResultExpiration {
			if !found || result.Time.Before(oldestTime) {
				oldestKey = key
				oldestTime = result.Time
				found = true
			}
		}
		if !found {
			return
		}
		delete(self.queryResultExpiration, oldestKey)
	}
}

// Query resolves a record to addresses, returning an empty slice both on an authoritative
// no-record answer and on a resolution failure. Use QueryResult to tell the two apart.
func (self *DohCache) Query(ctx context.Context, recordType string, domain string) []netip.Addr {
	addrs, _ := self.QueryResult(ctx, recordType, domain)
	return addrs
}

// QueryResult resolves a record and reports whether the answer was authoritative. authoritative
// is true when the resolver returned records or an authoritative no-record answer (NXDOMAIN /
// NODATA), and false when the resolution failed (timeout, ctx canceled, all resolvers errored)
// — a caller can map false+empty to SERVFAIL so a client retries instead of treating it as an
// authoritative "no address". Concurrent identical queries are coalesced onto one resolution
// (single-flight), and concurrent resolutions are bounded (MaxConcurrentResolutions).
//
// Serve-stale (RFC 8767): when a fresh resolution would return non-authoritative empty (the
// SERVFAIL shape) and an expired-but-retained answer for the key exists inside
// dohStaleServeBound, the stale answer is served instead — reported as authoritative, because
// this method's callers use the flag only to decide answer-vs-SERVFAIL and the stale answer
// exists precisely to avoid the SERVFAIL. An authoritative fresh answer (records or
// NXDOMAIN/NODATA) is never overridden by stale data; it also overwrites the retained entry
// through the normal resolve() caching. The stale entry never suppresses the resolution attempt
// itself — every expired-entry query still resolves (or joins the in-flight resolution) first.
func (self *DohCache) QueryResult(ctx context.Context, recordType string, domain string) ([]netip.Addr, bool) {
	if self.lifecycle.retired.Load() {
		return nil, false
	}

	q := NewDohKey(recordType, domain)
	now := time.Now()

	var fl *dohFlight
	var leader bool
	var hit bool
	var hitAddrs []netip.Addr
	var staleAddrs []netip.Addr
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		if r := self.queryResultExpiration[q]; r != nil {
			if r.Valid(now, self.settings.MissExpiration) {
				hit = true
				hitAddrs = r.Addrs()
				return
			}
			if r.staleUsable(now) {
				// expired but inside the serve-stale bound: keep the entry (a later query may
				// need it too) and remember its answer as the fallback for a failed resolve
				staleAddrs = r.Addrs()
			} else {
				delete(self.queryResultExpiration, q)
			}
		}
		// single-flight: lead a new resolution for this key, or join the one already running
		if existing, ok := self.inflight[q]; ok {
			fl = existing
		} else {
			fl = &dohFlight{done: make(chan struct{})}
			self.inflight[q] = fl
			leader = true
		}
	}()
	if hit {
		// a cached entry (records or an authoritative miss) is itself authoritative
		return hitAddrs, true
	}

	// serveStale is the one place a stale answer leaves this method: it logs (one line per
	// stale serve, naming the domain — the field signal that failover leaned on the cache) and
	// counts, so neither can drift from the other.
	serveStale := func() ([]netip.Addr, bool) {
		self.staleServeCount.Add(1)
		// loggerOrDefault: nil-safe against a literally-constructed cache (NewDohCache always
		// sets log, but a panic in the DNS fallback path is never acceptable)
		loggerOrDefault(self.log).Infof("[doh]serve stale %s %s (%d addrs)\n", q.RecordType, q.Domain, len(staleAddrs))
		return staleAddrs, true
	}

	if !leader {
		// a resolution for this key is already in flight; wait for it rather than firing a
		// duplicate, bounded by this caller's own ctx and cache lifetime.
		select {
		case <-fl.done:
			return fl.addrs, fl.authoritative
		case <-ctx.Done():
			if 0 < len(staleAddrs) {
				return serveStale()
			}
			return nil, false
		case <-self.lifecycle.ctx.Done():
			return nil, false
		}
	}

	// leader: resolve once, publish to any waiters, and drop the in-flight entry
	defer func() {
		self.stateLock.Lock()
		delete(self.inflight, q)
		self.stateLock.Unlock()
		close(fl.done)
	}()
	// Admit the whole resolver chain, not only its individual DoH requests.
	// Plain-DNS fallback uses net.Resolver directly; without this outer linked
	// context, a fallback dial loaded just before a cache swap could survive
	// Close and keep using the retired tunnel generation.
	resolveCtx, resolveDone, ok := self.lifecycle.context(ctx)
	if !ok {
		return nil, false
	}
	defer resolveDone()
	ctx = resolveCtx

	// bound concurrent resolutions; shed if a slot is not free before this caller's ctx
	// expires — with a retained stale answer that shed serves stale, otherwise it surfaces
	// as empty + non-authoritative (SERVFAIL)
	select {
	case self.resolveSem <- struct{}{}:
		defer func() { <-self.resolveSem }()
	case <-ctx.Done():
		if 0 < len(staleAddrs) {
			fl.addrs, fl.authoritative = serveStale()
			return fl.addrs, fl.authoritative
		}
		return nil, false
	}
	fl.addrs, fl.authoritative = self.resolve(ctx, q, now)
	if !fl.authoritative && len(fl.addrs) == 0 && 0 < len(staleAddrs) {
		// the exact SERVFAIL shape (resolve returns authoritative=false only with no
		// addresses: every resolver path failed or answered non-authoritatively empty):
		// serve the retained stale answer instead. An authoritative NXDOMAIN/NODATA came
		// back authoritative=true and is deliberately NOT overridden.
		fl.addrs, fl.authoritative = serveStale()
	}
	return fl.addrs, fl.authoritative
}

// Forward resolves qType for domain and returns the raw RFC 8484 response wire
// for record types the cache forwards opaquely rather than parsing into
// addresses. It follows the cache's configured path order (remote/tunnel DoH,
// then local DoH) and is not cached—the client stub caches by record TTL.
func (self *DohCache) Forward(ctx context.Context, qType dnsmessage.Type, domain string) ([]byte, bool) {
	if self.lifecycle.retired.Load() {
		return nil, false
	}
	forwardCtx, forwardDone, ok := self.lifecycle.context(ctx)
	if !ok {
		return nil, false
	}
	defer forwardDone()
	ctx = forwardCtx

	rs := self.settings.DnsResolverSettings
	if self.dohServerNames[domain] {
		// a doh server name must not resolve through doh (circular)
		return nil, false
	}
	if rs.EnableRemoteDoh {
		if response, ok := self.remoteClient.forwardRaw(
			ctx,
			remoteDohUrls(self.settings, self.settings.IpVersion),
			qType,
			self.settings,
			domain,
		); ok {
			return response, true
		}
	}
	if rs.EnableLocalDoh {
		return self.localClient.forwardRaw(
			ctx,
			localDohUrls(self.settings, self.settings.IpVersion),
			qType,
			self.settings,
			domain,
		)
	}
	return nil, false
}

// resolve runs the resolver chain (remote DoH -> local DoH -> remote DNS -> local DNS) for one
// query, caches an authoritative result, and returns the addresses plus whether the answer was
// authoritative. it is not single-flighted itself; QueryResult coalesces concurrent callers.
func (self *DohCache) resolve(ctx context.Context, q DohKey, now time.Time) ([]netip.Addr, bool) {
	addrExpirations := map[netip.Addr]time.Time{}
	cacheMiss := false
	minCacheTtl := self.settings.MinCacheTtl

	// a doh server name can not resolve through doh (circular). it resolves
	// over remote plain dns through the tunnel instead — permitted even when
	// EnableRemoteDns is false, so a hostname-form doh server resolves
	// remotely rather than falling through to the local resolver. remote dns
	// remains disabled for every other name.
	dohServerName := self.dohServerNames[q.Domain]

	if !dohServerName && self.settings.DnsResolverSettings.EnableRemoteDoh {
		queryResult := self.remoteClient.queryResult(ctx, remoteDohUrls(self.settings, self.settings.IpVersion), q.RecordType, self.settings, q.Domain)

		for addr, ttlSeconds := range queryResult.AddrTtls {
			addrExpirations[addr] = now.Add(max(time.Duration(ttlSeconds)*time.Second, minCacheTtl))
		}
		if 0 < len(queryResult.AddrTtls) && queryResult.Route != nil && self.settings.DohResultCallback != nil {
			addrs := make([]netip.Addr, 0, len(queryResult.AddrTtls))
			for addr := range queryResult.AddrTtls {
				addrs = append(addrs, addr)
			}
			HandleError(func() {
				self.settings.DohResultCallback(q.Domain, addrs, queryResult.Route)
			})
		}
		if len(addrExpirations) == 0 && queryResult.Miss {
			cacheMiss = true
		}
	}

	if len(addrExpirations) == 0 && !dohServerName && self.settings.DnsResolverSettings.EnableLocalDoh {
		queryResult := self.localClient.queryResult(ctx, localDohUrls(self.settings, self.settings.IpVersion), q.RecordType, self.settings, q.Domain)

		for addr, ttlSeconds := range queryResult.AddrTtls {
			addrExpirations[addr] = now.Add(max(time.Duration(ttlSeconds)*time.Second, minCacheTtl))
		}
		if len(addrExpirations) == 0 && queryResult.Miss {
			cacheMiss = true
		}
	}

	lookupNetwork, lookupFamilyEnabled := dnsLookupNetwork(q.RecordType, self.settings.IpVersion)

	if len(addrExpirations) == 0 && lookupFamilyEnabled && (dohServerName || self.settings.DnsResolverSettings.EnableRemoteDns) &&
		self.settings.MemoryTarget.Acquire(ctx, dnsLookupReserveByteCount) {
		// try the remote resolver
		resolvedIps, err := self.remoteResolver.LookupIP(ctx, lookupNetwork, q.Domain)
		self.settings.MemoryTarget.Release(dnsLookupReserveByteCount)
		if err == nil {
			found := false
			for _, ip := range resolvedIps {
				if addr, ok := netIPAddr(ip); ok {
					addrExpirations[addr] = now.Add(self.settings.LocalExpiration)
					found = true
				}
			}
			if !found {
				cacheMiss = true
			}
		} else if authoritativeDnsMiss(err) {
			cacheMiss = true
		} else if log := self.log.V(2); log.Enabled() {
			log.Infof("[doh]remote (%s) err = %s\n", q.Domain, err)
		}
	}

	if len(addrExpirations) == 0 && lookupFamilyEnabled && self.settings.DnsResolverSettings.EnableLocalDns &&
		self.settings.MemoryTarget.Acquire(ctx, dnsLookupReserveByteCount) {
		// try the local resolver
		resolvedIps, err := self.localResolver.LookupIP(ctx, lookupNetwork, q.Domain)
		self.settings.MemoryTarget.Release(dnsLookupReserveByteCount)
		if err == nil {
			found := false
			for _, ip := range resolvedIps {
				if addr, ok := netIPAddr(ip); ok {
					addrExpirations[addr] = now.Add(self.settings.LocalExpiration)
					found = true
				}
			}
			if !found {
				cacheMiss = true
			}
		} else if authoritativeDnsMiss(err) {
			cacheMiss = true
		} else if log := self.log.V(2); log.Enabled() {
			log.Infof("[doh]local (%s) err = %s\n", q.Domain, err)
		}
	}

	if dohServerName && 0 < len(addrExpirations) && self.settings.DohServerResolvedCallback != nil {
		// surface the server addresses (e.g. into the mux reverse index, so
		// the ignore matcher covers them alongside the server name)
		addrs := []netip.Addr{}
		for addr := range addrExpirations {
			addrs = append(addrs, addr)
		}
		HandleError(func() {
			self.settings.DohServerResolvedCallback(q.Domain, addrs)
		})
	}

	authoritative := 0 < len(addrExpirations) || cacheMiss
	if ctx.Err() == nil && authoritative {
		r := &DohResult{
			Time:            now,
			AddrExpirations: addrExpirations,
			Miss:            cacheMiss && len(addrExpirations) == 0,
		}
		func() {
			self.stateLock.Lock()
			defer self.stateLock.Unlock()

			self.pruneCacheLocked(now, 1)
			self.queryResultExpiration[q] = r
		}()
	}

	return (&DohResult{
		Time:            now,
		AddrExpirations: addrExpirations,
	}).Addrs(), authoritative
}

func DohQueryWithDefaults(ctx context.Context, recordType string, domains ...string) map[netip.Addr]int {
	return DohQuery(ctx, 0, recordType, DefaultDohSettings(), domains...)
}

// return ip -> ttl (seconds)
// use `ipVersion=0` to try all versions
func DohQuery(ctx context.Context, ipVersion int, recordType string, settings *DohSettings, domains ...string) map[netip.Addr]int {
	// A one-shot query can return as soon as its fastest server answers while
	// slower hedge requests and net/http's reusable dials are still winding
	// down. Give it the same request+dial join used by a permanent DohCache;
	// CloseIdleConnections alone can run before a detached dial installs its
	// resulting h2 connection, leaking that connection until the 15-minute
	// idle timeout.
	lifecycle := newDohCacheLifecycle()
	httpClient := httpClientWithDialer(
		settings,
		lifecycle.dialContext(wrapControlDial("doh", settings.Log, settings.DialContextSettings == nil, settings.DialContext)),
		tls.NewLRUClientSessionCache(dohTlsSessionCacheCapacity),
	)
	result := dohQueryWithClient(
		ctx,
		httpClient,
		ipVersion,
		recordType,
		settings,
		lifecycle,
		domains...,
	)
	lifecycle.shutdown()
	httpClient.CloseIdleConnections()
	return result
}

func DohQueryWithClient(
	ctx context.Context,
	httpClient *http.Client,
	ipVersion int,
	recordType string,
	settings *DohSettings,
	domains ...string,
) map[netip.Addr]int {
	return dohQueryWithClient(
		ctx,
		httpClient,
		ipVersion,
		recordType,
		settings,
		nil,
		domains...,
	)
}

func dohQueryWithClient(
	ctx context.Context,
	httpClient *http.Client,
	ipVersion int,
	recordType string,
	settings *DohSettings,
	lifecycle *dohCacheLifecycle,
	domains ...string,
) map[netip.Addr]int {
	// a one-shot client: bound its in-flight requests, but keep no persistent per-server stats
	// (nil stats -> uniform-random fan-out order)
	c := &dohClient{
		httpClient:    httpClient,
		httpSem:       make(chan struct{}, maxConcurrentHttpRequests(settings)),
		primarySem:    newDohPrimarySem(maxConcurrentHttpRequests(settings), settings.DohServerHedgeReserve),
		activeQueries: &atomic.Int64{},
		stats:         nil,
		memoryTarget:  settings.MemoryTarget,
		lifecycle:     lifecycle,
		siblings:      dohServerSiblings(settings.DnsResolverSettings),
	}
	return c.queryResult(ctx, remoteDohUrls(settings, ipVersion), recordType, settings, domains...).AddrTtls
}

func dohUrlsFor(ipv4 []string, ipv6 []string, ipVersion int) []string {
	switch ipVersion {
	case 4:
		return ipv4
	case 6:
		return ipv6
	default:
		// both families, operator-paired (see dualStackList)
		return dualStackList(ipv4, ipv6, knownDohUrlPairs)
	}
}

// remoteDohUrls/localDohUrls return the configured wire-format DoH server URLs for the ip version.
func remoteDohUrls(settings *DohSettings, ipVersion int) []string {
	rs := settings.DnsResolverSettings
	return dohUrlsFor(rs.RemoteDohUrlsIpv4, rs.RemoteDohUrlsIpv6, ipVersion)
}

func localDohUrls(settings *DohSettings, ipVersion int) []string {
	rs := settings.DnsResolverSettings
	return dohUrlsFor(rs.LocalDohUrlsIpv4, rs.LocalDohUrlsIpv6, ipVersion)
}

type dohQueryResult struct {
	AddrTtls map[netip.Addr]int
	Miss     bool
	Route    *DohRoute
}

func newDohQueryResult() *dohQueryResult {
	return &dohQueryResult{
		AddrTtls: map[netip.Addr]int{},
	}
}

// dohClient issues RFC 8484 wire-format queries over one HTTP client. httpSem hard-caps concurrent
// in-flight requests (shared across a DohCache's remote + local clients); stats biases the fan-out
// order toward recently-successful servers. stats may be nil (one-shot queries: uniform-random
// order, no recording).
type dohClient struct {
	httpClient    *http.Client
	httpSem       chan struct{}
	primarySem    chan struct{}
	activeQueries *atomic.Int64
	stats         *serverStats
	// the owner's live dns byte budget (see DohSettings.MemoryTarget).
	// nil disables the byte bound.
	memoryTarget *MemoryTarget
	// nil only for caller-owned HTTP clients supplied to DohQueryWithClient.
	lifecycle *dohCacheLifecycle
	// captureRoute is true only when the remote DoH client dials through an
	// owner-supplied tunnel stack. Host/local clients must never manufacture a
	// provider-affinity hint from their physical socket.
	captureRoute bool
	// siblings pairs each server url with the same operator's other-family
	// url (dohServerSiblings), so a fan-out launches the pair as one wave.
	siblings map[string]string
}

// knownDohUrlPairs are the v4/v6 endpoint pairs of the default DoH
// operators. A pair is what "race each operator's v4 and v6 endpoint" means
// for a url the defaults ship; a url outside this table pairs by index only
// with another url outside it (see familySiblings).
var knownDohUrlPairs = [][2]string{
	{"https://1.1.1.1/dns-query", "https://[2606:4700:4700::1111]/dns-query"},
	{"https://1.0.0.1/dns-query", "https://[2606:4700:4700::1001]/dns-query"},
	{"https://8.8.8.8/dns-query", "https://[2001:4860:4860::8888]/dns-query"},
	{"https://8.8.4.4/dns-query", "https://[2001:4860:4860::8844]/dns-query"},
	{"https://9.9.9.9/dns-query", "https://[2620:fe::fe]/dns-query"},
	{"https://149.112.112.112/dns-query", "https://[2620:fe::9]/dns-query"},
	{"https://208.67.222.222/dns-query", "https://[2620:119:35::35]/dns-query"},
	{"https://208.67.220.220/dns-query", "https://[2620:119:53::53]/dns-query"},
}

// knownDnsServerPairs are the v4/v6 pairs of the default and regional plain
// dns operators, the same rule for the plain-dns lists.
var knownDnsServerPairs = [][2]string{
	{"1.1.1.1", "2606:4700:4700::1111"},
	{"1.0.0.1", "2606:4700:4700::1001"},
	{"8.8.8.8", "2001:4860:4860::8888"},
	{"8.8.4.4", "2001:4860:4860::8844"},
	{"9.9.9.9", "2620:fe::fe"},
	{"149.112.112.112", "2620:fe::9"},
	{"208.67.222.222", "2620:119:35::35"},
	{"208.67.220.220", "2620:119:53::53"},
	{"223.5.5.5", "2400:3200::1"},
	{"119.29.29.29", "2402:4e00::"},
	{"77.88.8.8", "2a02:6b8::feed:0ff"},
}

// familySiblings pairs the entries of a v4 list and a v6 list by operator:
// a known pair from the table when both members are configured, else by
// list index for entries that are outside the table on both sides. The
// result maps each paired entry to its sibling in both directions.
//
// The table rule is what lets "start from the defaults and replace the v4
// list" keep meaning "use only these servers": the default v6 entries are
// known operators whose v4 partners are then absent, so they pair with
// nothing and dualStackList leaves them out, instead of index-pairing a
// caller's private server with Cloudflare's v6 endpoint.
func familySiblings(ipv4 []string, ipv6 []string, known [][2]string) map[string]string {
	siblings := map[string]string{}
	knownOf := map[string]string{}
	for _, pair := range known {
		knownOf[pair[0]] = pair[1]
		knownOf[pair[1]] = pair[0]
	}
	present6 := map[string]bool{}
	for _, entry := range ipv6 {
		present6[entry] = true
	}
	unknown4 := []string{}
	for _, entry := range ipv4 {
		if partner, ok := knownOf[entry]; ok {
			if present6[partner] && partner != entry {
				siblings[entry] = partner
				siblings[partner] = entry
			}
			continue
		}
		unknown4 = append(unknown4, entry)
	}
	unknown6 := []string{}
	for _, entry := range ipv6 {
		if _, ok := knownOf[entry]; !ok {
			unknown6 = append(unknown6, entry)
		}
	}
	for i := 0; i < min(len(unknown4), len(unknown6)); i++ {
		if unknown4[i] == "" || unknown6[i] == "" || unknown4[i] == unknown6[i] {
			continue
		}
		siblings[unknown4[i]] = unknown6[i]
		siblings[unknown6[i]] = unknown4[i]
	}
	return siblings
}

// dualStackList is the list a family-agnostic (IpVersion 0) consumer walks:
// every v4 entry in order, each followed by its v6 sibling, so the families
// interleave by operator.
//
// Two kinds of leftover v6 entry are treated differently, and the difference
// is what makes "start from the defaults and replace the v4 list" behave:
//
//   - A KNOWN-TABLE entry whose v4 partner is absent is left out. Those are
//     the shipped defaults; a caller who replaced the v4 list means "use my
//     servers", and pulling Cloudflare's v6 endpoint back in would send real
//     queries to an operator they just removed.
//   - An entry OUTSIDE the table is kept, at the end. Those can only have come
//     from the caller, so dropping one would ignore a server they explicitly
//     configured -- which is what happened to every custom v6 endpoint past
//     the shorter list's length, since familySiblings can only index-pair
//     min(len4, len6) of them.
func dualStackList(ipv4 []string, ipv6 []string, known [][2]string) []string {
	if len(ipv4) == 0 {
		return append([]string{}, ipv6...)
	}
	inTable := make(map[string]bool, 2*len(known))
	for _, pair := range known {
		inTable[pair[0]] = true
		inTable[pair[1]] = true
	}
	siblings := familySiblings(ipv4, ipv6, known)
	list := make([]string, 0, len(ipv4)+len(ipv6))
	emitted := make(map[string]bool, len(ipv4)+len(ipv6))
	emit := func(entry string) {
		if emitted[entry] {
			return
		}
		emitted[entry] = true
		list = append(list, entry)
	}
	for _, entry := range ipv4 {
		emit(entry)
		if sibling, ok := siblings[entry]; ok {
			emit(sibling)
		}
	}
	for _, entry := range ipv6 {
		if !inTable[entry] {
			emit(entry)
		}
	}
	return list
}

// dohServerSiblings is the sibling map over both DoH url lists, for the
// launch waves.
func dohServerSiblings(rs *DnsResolverSettings) map[string]string {
	siblings := map[string]string{}
	if rs == nil {
		return siblings
	}
	maps.Copy(siblings, familySiblings(rs.RemoteDohUrlsIpv4, rs.RemoteDohUrlsIpv6, knownDohUrlPairs))
	maps.Copy(siblings, familySiblings(rs.LocalDohUrlsIpv4, rs.LocalDohUrlsIpv6, knownDohUrlPairs))
	return siblings
}

// dohLaunchWaves groups an ordered server list into launch waves: a server
// and its operator sibling (when both are in the list) form one wave, so the
// operator's v4 and v6 endpoints race each other inside the wave instead of
// the v6 endpoint waiting a whole stagger behind the v4 one. Wave order
// follows the first appearance of either member in the weighted order, so a
// well-scoring operator still fires first. Every url appears exactly once.
func dohLaunchWaves(ordered []string, siblings map[string]string) [][]string {
	present := make(map[string]bool, len(ordered))
	for _, dohUrl := range ordered {
		present[dohUrl] = true
	}
	placed := make(map[string]bool, len(ordered))
	waves := make([][]string, 0, len(ordered))
	for _, dohUrl := range ordered {
		if placed[dohUrl] {
			continue
		}
		wave := []string{dohUrl}
		placed[dohUrl] = true
		if sibling, ok := siblings[dohUrl]; ok && present[sibling] && !placed[sibling] {
			wave = append(wave, sibling)
			placed[sibling] = true
		}
		waves = append(waves, wave)
	}
	return waves
}

// beginQuery admits one logical lookup into the shared quiet-query counter.
// The counter is shared by parsed A/AAAA and opaque SVCB/HTTPS lookups so a
// browser's three-record origin burst gets one predictable hedge allowance,
// rather than one allowance per implementation path.
func (self *dohClient) beginQuery() (int64, func()) {
	if self.activeQueries == nil {
		return 1, func() {}
	}
	activeQueryCount := self.activeQueries.Add(1)
	return activeQueryCount, func() {
		self.activeQueries.Add(-1)
	}
}

// serverStagger returns the launch delay for additional servers for this
// logical lookup. A proven path uses the shorter warm delay; only the first
// bounded set of otherwise-idle lookups races immediately.
func dohServerStagger(settings *DohSettings, pathWarm bool, activeQueryCount int64) time.Duration {
	stagger := settings.DohServerStagger
	if pathWarm && 0 < settings.DohServerWarmStagger {
		stagger = min(stagger, settings.DohServerWarmStagger)
	}
	if 0 < stagger && 0 < settings.DohServerRaceMaxInFlight &&
		activeQueryCount <= int64(settings.DohServerRaceMaxInFlight) {
		return 0
	}
	return stagger
}

// waitDohLaunchStagger waits for the next hedge wave without the historical
// Stop-then-drain timer pattern. Go 1.23+ timer channels are synchronous:
// Stop can report false while no value is available to drain, so a cancellation
// racing expiry could otherwise park the only launcher forever.
func waitDohLaunchStagger(
	ctx context.Context,
	stop <-chan struct{},
	stagger time.Duration,
) bool {
	if stagger <= 0 {
		return true
	}
	timer := time.NewTimer(stagger)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-stop:
		return false
	case <-ctx.Done():
		return false
	}
}

// queryResult resolves recordType for the given domains across dohUrls (RFC 8484 wire), returning
// as soon as any server returns records. Servers are tried in a weighted-random order biased toward
// recent success, and launched one wave per DohServerStagger so a fast primary answers before the
// rest fire — which bounds concurrent in-flight requests and skips the redundant fan-out entirely
// when an early server wins.
func (self *dohClient) queryResult(
	ctx context.Context,
	dohUrls []string,
	recordType string,
	settings *DohSettings,
	domains ...string,
) *dohQueryResult {
	switch recordType {
	case "A", "AAAA":
	default:
		return newDohQueryResult()
	}
	if len(dohUrls) == 0 || settings.RequestTimeout <= 0 {
		return newDohQueryResult()
	}

	names := make([]string, 0, len(domains))
	for _, domain := range domains {
		name, err := Punycode(domain)
		if err != nil {
			continue
		}
		names = append(names, name)
	}
	if len(names) == 0 {
		return newDohQueryResult()
	}

	activeQueryCount, endQuery := self.beginQuery()
	defer endQuery()

	queryCtx, queryCancel := context.WithTimeout(ctx, settings.RequestTimeout)
	defer queryCancel()

	// weighted-random order: the best recent performers tend to fire first (and the rest are
	// skipped when an early server wins)
	ordered := self.stats.order(dohUrls)
	if 0 < settings.MaxServersPerQuery && settings.MaxServersPerQuery < len(ordered) {
		ordered = ordered[:settings.MaxServersPerQuery]
	}
	// an operator's v4 and v6 endpoints race inside one wave
	waves := dohLaunchWaves(ordered, self.siblings)

	queryCount := len(ordered) * len(names)
	receiveResults := make(chan *dohQueryResult, queryCount)

	// launchCtx additionally ends when an early server wins (stop), so a
	// launcher parked on the dns memory target does not outlive its query
	launchCtx, launchCancel := context.WithCancel(queryCtx)
	defer launchCancel()

	stop := make(chan struct{})
	var stopOnce sync.Once
	stopLaunching := func() { stopOnce.Do(func() { close(stop); launchCancel() }) }
	defer stopLaunching()

	pathWarm := settings.DohPathWarm != nil && settings.DohPathWarm()
	stagger := dohServerStagger(settings, pathWarm, activeQueryCount)
	// Hedge-on-quiet: the first few concurrent resolutions race their servers;
	// later members of a burst keep the stagger. The shared atomic admission
	// counter makes this bound exact. Reading len(httpSem) here was only a
	// snapshot before launchers acquired their slots, so a synchronized burst
	// could nondeterministically admit many more hedges than intended.

	// launcher: start one server-wave per stagger interval (in weighted order) until an early
	// server wins (stop), the deadline passes, or every server has been launched.
	go HandleError(func() {
		for waveIndex, wave := range waves {
			if 0 < waveIndex && !waitDohLaunchStagger(queryCtx, stop, stagger) {
				return
			}
			for _, dohUrl := range wave {
				for _, name := range names {
					primaryAcquired := false
					if pathWarm && waveIndex == 0 && self.primarySem != nil {
						select {
						case self.primarySem <- struct{}{}:
							primaryAcquired = true
						case <-stop:
							return
						case <-queryCtx.Done():
							return
						}
					}
					// acquire the in-flight slot and byte reservation here so work
					// waiting on the caps parks in this one launcher instead of one
					// parked goroutine per (server, name); the request goroutine owns
					// both and releases them when done
					if self.httpSem != nil {
						select {
						case self.httpSem <- struct{}{}:
						case <-stop:
							if primaryAcquired {
								<-self.primarySem
							}
							return
						case <-queryCtx.Done():
							if primaryAcquired {
								<-self.primarySem
							}
							return
						}
					}
					// the request pins up to a full response read; the owner's dns
					// memory target bounds its total in-flight bytes across all of
					// its resolver caches
					if !self.memoryTarget.Acquire(launchCtx, dohQueryReserveByteCount) {
						if self.httpSem != nil {
							<-self.httpSem
						}
						if primaryAcquired {
							<-self.primarySem
						}
						return
					}
					go HandleError(func() {
						defer self.memoryTarget.Release(dohQueryReserveByteCount)
						if self.httpSem != nil {
							defer func() { <-self.httpSem }()
						}
						if primaryAcquired {
							defer func() { <-self.primarySem }()
						}
						result := self.queryWire(queryCtx, dohUrl, recordType, name)
						// a server that returns records or an authoritative no-record answer is healthy;
						// anything else (error, non-200, or no answer before it was beaten) counts
						// against it. The large stagger means the first wave is the usual winner, so a
						// later wave is only launched — and only judged — when an earlier server was
						// slow or failed.
						self.stats.record(dohUrl, 0 < len(result.AddrTtls) || result.Miss)
						select {
						case receiveResults <- result:
						case <-queryCtx.Done():
						}
					})
				}
			}
		}
	})

	mergedResult := newDohQueryResult()
	for range queryCount {
		select {
		case <-queryCtx.Done():
			return &dohQueryResult{
				AddrTtls: mergedResult.AddrTtls,
			}
		case result := <-receiveResults:
			maps.Copy(mergedResult.AddrTtls, result.AddrTtls)
			if 0 < len(result.AddrTtls) && result.Route != nil {
				mergedResult.Route = result.Route
			}
			if result.Miss {
				mergedResult.Miss = true
			}
			// fastest-record-wins: return as soon as any server returns records rather than
			// waiting for the rest, so a slow or dead server can't delay a successful lookup. an
			// authoritative miss is not short-circuited — keep collecting so a filtering
			// resolver's NXDOMAIN can't override a server that resolves the name.
			if 0 < len(mergedResult.AddrTtls) {
				stopLaunching()
				return &dohQueryResult{
					AddrTtls: mergedResult.AddrTtls,
					Route:    mergedResult.Route,
				}
			}
		}
	}
	mergedResult.Miss = len(mergedResult.AddrTtls) == 0 && mergedResult.Miss
	return mergedResult
}

// queryWire runs an RFC 8484 wire-format DoH query (Accept application/dns-message,
// GET ?dns=<base64url DNS message>). name must already be punycoded ascii.
func (self *dohClient) queryWire(ctx context.Context, dohUrl string, recordType string, name string) *dohQueryResult {
	result, _ := self.queryWireDetailed(ctx, dohUrl, recordType, name)
	return result
}

// queryWireDetailed is queryWire with a diagnostic error for maintenance
// probes. Ordinary user queries intentionally use the quiet wrapper above;
// one failed provider in a successful hedge is expected and must not log.
func (self *dohClient) queryWireDetailed(ctx context.Context, dohUrl string, recordType string, name string) (*dohQueryResult, error) {
	result := newDohQueryResult()
	var qType dnsmessage.Type
	switch recordType {
	case "A":
		qType = dnsmessage.TypeA
	case "AAAA":
		qType = dnsmessage.TypeAAAA
	default:
		return result, fmt.Errorf("unsupported record type %q", recordType)
	}
	data, route, err := self.queryWireRawDetailedWithRoute(ctx, dohUrl, qType, name)
	if err != nil {
		return result, err
	}
	result = parseDohWire(data, qType)
	result.Route = route
	return result, nil
}

// queryWireRaw issues one RFC 8484 query for (qType, name) to dohUrl and returns the raw
// response wire, without parsing it. Used to forward record types the cache handles
// opaquely (SVCB/HTTPS) — preserving the exact record for the client — where queryWire
// parses A/AAAA into addresses. Like queryWire, the caller holds an httpSem slot for the
// lifetime of this request.
func (self *dohClient) queryWireRaw(ctx context.Context, dohUrl string, qType dnsmessage.Type, name string) ([]byte, bool) {
	data, err := self.queryWireRawDetailed(ctx, dohUrl, qType, name)
	return data, err == nil
}

// queryWireRawDetailed returns stage-specific errors without exposing the
// encoded DNS question. Maintenance logs therefore identify the failed DoH
// server and transport stage while preserving the queried hostname.
func (self *dohClient) queryWireRawDetailed(ctx context.Context, dohUrl string, qType dnsmessage.Type, name string) ([]byte, error) {
	data, _, err := self.queryWireRawDetailedWithRoute(ctx, dohUrl, qType, name)
	return data, err
}

func dohRouteForConn(conn net.Conn) *DohRoute {
	if conn == nil {
		return nil
	}
	addrPort := func(addr net.Addr) (netip.AddrPort, bool) {
		// httptrace may report a live wrapper whose address is temporarily nil
		// while an HTTP/2 connection is being retired. Route observation is
		// diagnostic only; an absent endpoint must not panic and abort the DoH
		// result path.
		if addr == nil {
			return netip.AddrPort{}, false
		}
		addrValue := reflect.ValueOf(addr)
		switch addrValue.Kind() {
		case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
			if addrValue.IsNil() {
				return netip.AddrPort{}, false
			}
		}
		if tcpAddr, ok := addr.(*net.TCPAddr); ok {
			ip, found := netip.AddrFromSlice(tcpAddr.IP)
			if !found || tcpAddr.Port < 0 || 65535 < tcpAddr.Port {
				return netip.AddrPort{}, false
			}
			ip = ip.Unmap()
			if tcpAddr.Zone != "" && ip.Is6() {
				ip = ip.WithZone(tcpAddr.Zone)
			}
			return netip.AddrPortFrom(ip, uint16(tcpAddr.Port)), true
		}
		parsed, err := netip.ParseAddrPort(addr.String())
		return parsed, err == nil
	}
	local, localOk := addrPort(conn.LocalAddr())
	remote, remoteOk := addrPort(conn.RemoteAddr())
	if !localOk || !remoteOk {
		return nil
	}
	return &DohRoute{Local: local, Remote: remote}
}

// queryWireRawDetailedWithRoute is the route-observing form used for parsed
// A/AAAA answers. The compatibility wrapper above keeps raw forwarders and
// maintenance probes on their existing signature.
func (self *dohClient) queryWireRawDetailedWithRoute(ctx context.Context, dohUrl string, qType dnsmessage.Type, name string) ([]byte, *DohRoute, error) {
	if self.lifecycle != nil {
		linkedCtx, done, ok := self.lifecycle.context(ctx)
		if !ok {
			return nil, nil, context.Canceled
		}
		defer done()
		ctx = linkedCtx
	}

	dnsName, err := dnsmessage.NewName(name + ".")
	if err != nil {
		return nil, nil, fmt.Errorf("build name: %w", err)
	}
	// id 0 is recommended for DoH (RFC 8484 §4.1); recursion desired
	msg := dnsmessage.Message{
		Header:    dnsmessage.Header{RecursionDesired: true},
		Questions: []dnsmessage.Question{{Name: dnsName, Type: qType, Class: dnsmessage.ClassINET}},
	}
	wire, err := msg.Pack()
	if err != nil {
		return nil, nil, fmt.Errorf("pack query: %w", err)
	}
	requestUrl := fmt.Sprintf("%s?dns=%s", dohUrl, base64.RawURLEncoding.EncodeToString(wire))

	request, err := http.NewRequestWithContext(ctx, "GET", requestUrl, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("build request for %s: %w", dohUrl, err)
	}
	var route atomic.Pointer[DohRoute]
	if self.captureRoute {
		request = request.WithContext(httptrace.WithClientTrace(request.Context(), &httptrace.ClientTrace{
			GotConn: func(info httptrace.GotConnInfo) {
				if observed := dohRouteForConn(info.Conn); observed != nil {
					route.Store(observed)
				}
			},
		}))
	}
	request.Header.Set("Accept", "application/dns-message")

	response, err := self.httpClient.Do(request)
	if err != nil {
		// url.Error includes the full request URL, whose dns query parameter
		// encodes the hostname. Retain only its underlying transport error.
		var urlError *url.Error
		if errors.As(err, &urlError) {
			err = urlError.Err
		}
		return nil, nil, fmt.Errorf("request %s: %w", dohUrl, err)
	}
	defer releaseHttpResponseBody(ctx, response)
	if response.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("request %s: HTTP status %s", dohUrl, response.Status)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, maxDohResponseBytes))
	if err != nil {
		return nil, nil, fmt.Errorf("read %s: %w", dohUrl, err)
	}
	return data, route.Load(), nil
}

// forwardRaw resolves qType for domain across dohUrls (best recent performers
// first) and returns the fastest usable raw response wire (NOERROR/NXDOMAIN).
// It uses the same load-aware stagger, quiet-query admission, hedge reserve,
// shared HTTP cap, and live memory target as parsed A/AAAA queries. This is
// important for browsers: Chromium commonly waits for HTTPS before opening an
// origin, so serially waiting for one slow DoH server multiplied its delay
// across an entire parallel first-load fan-out.
func (self *dohClient) forwardRaw(ctx context.Context, dohUrls []string, qType dnsmessage.Type, settings *DohSettings, domain string) ([]byte, bool) {
	if len(dohUrls) == 0 || settings.RequestTimeout <= 0 {
		return nil, false
	}
	name, err := Punycode(domain)
	if err != nil {
		return nil, false
	}

	activeQueryCount, endQuery := self.beginQuery()
	defer endQuery()

	queryCtx, queryCancel := context.WithTimeout(ctx, settings.RequestTimeout)
	defer queryCancel()

	ordered := self.stats.order(dohUrls)
	if 0 < settings.MaxServersPerQuery && settings.MaxServersPerQuery < len(ordered) {
		ordered = ordered[:settings.MaxServersPerQuery]
	}
	if len(ordered) == 0 {
		return nil, false
	}
	waves := dohLaunchWaves(ordered, self.siblings)

	type rawResult struct {
		data   []byte
		usable bool
	}
	results := make(chan rawResult, len(ordered))

	// A winner cancels both launch admission and every losing HTTP request.
	// The buffered result channel lets a loser finish without depending on
	// this function remaining on the receive side.
	launchCtx, launchCancel := context.WithCancel(queryCtx)
	defer launchCancel()
	stop := make(chan struct{})
	var stopOnce sync.Once
	stopLaunching := func() {
		stopOnce.Do(func() {
			close(stop)
			launchCancel()
		})
	}
	defer stopLaunching()

	pathWarm := settings.DohPathWarm != nil && settings.DohPathWarm()
	stagger := dohServerStagger(settings, pathWarm, activeQueryCount)

	go HandleError(func() {
		for waveIndex, wave := range waves {
			if 0 < waveIndex && !waitDohLaunchStagger(queryCtx, stop, stagger) {
				return
			}
			for _, dohUrl := range wave {

				primaryAcquired := false
				if pathWarm && waveIndex == 0 && self.primarySem != nil {
					select {
					case self.primarySem <- struct{}{}:
						primaryAcquired = true
					case <-stop:
						return
					case <-queryCtx.Done():
						return
					}
				}
				if self.httpSem != nil {
					select {
					case self.httpSem <- struct{}{}:
					case <-stop:
						if primaryAcquired {
							<-self.primarySem
						}
						return
					case <-queryCtx.Done():
						if primaryAcquired {
							<-self.primarySem
						}
						return
					}
				}
				if !self.memoryTarget.Acquire(launchCtx, dohQueryReserveByteCount) {
					if self.httpSem != nil {
						<-self.httpSem
					}
					if primaryAcquired {
						<-self.primarySem
					}
					return
				}

				go HandleError(func() {
					defer self.memoryTarget.Release(dohQueryReserveByteCount)
					if self.httpSem != nil {
						defer func() { <-self.httpSem }()
					}
					if primaryAcquired {
						defer func() { <-self.primarySem }()
					}
					data, ok := self.queryWireRaw(queryCtx, dohUrl, qType, name)
					usable := ok && dnsResponseUsable(data)
					self.stats.record(dohUrl, usable)
					select {
					case results <- rawResult{data: data, usable: usable}:
					case <-queryCtx.Done():
					}
				})
			}
		}
	})

	for range ordered {
		select {
		case <-queryCtx.Done():
			return nil, false
		case result := <-results:
			if result.usable {
				stopLaunching()
				queryCancel()
				return result.data, true
			}
		}
	}
	return nil, false
}

// dnsResponseUsable reports whether a raw DNS response is a well-formed answer worth forwarding:
// a NOERROR (records or NODATA) or NXDOMAIN. A SERVFAIL/REFUSED/other is not (the caller tries the
// next server).
func dnsResponseUsable(response []byte) bool {
	if len(response) < 12 {
		return false
	}
	// header flags are bytes 2-3; RCODE is the low 4 bits of byte 3
	switch response[3] & 0x0f {
	case 0, 3: // NOERROR, NXDOMAIN
		return true
	default:
		return false
	}
}

// parseDohWire parses an RFC 8484 wire-format DNS response, extracting the A or AAAA answers
// matching qType. NXDOMAIN -> Miss; other non-success RCODEs -> failure (empty, not Miss).
func parseDohWire(data []byte, qType dnsmessage.Type) *dohQueryResult {
	result := newDohQueryResult()

	var p dnsmessage.Parser
	header, err := p.Start(data)
	if err != nil {
		return result
	}
	switch header.RCode {
	case dnsmessage.RCodeSuccess:
	case dnsmessage.RCodeNameError: // NXDOMAIN
		result.Miss = true
		return result
	default:
		return result
	}
	if err := p.SkipAllQuestions(); err != nil {
		return result
	}
	for {
		ah, err := p.AnswerHeader()
		if err == dnsmessage.ErrSectionDone {
			break
		}
		if err != nil {
			return result
		}
		switch {
		case ah.Type == dnsmessage.TypeA && qType == dnsmessage.TypeA:
			r, err := p.AResource()
			if err != nil {
				return result
			}
			ip := netip.AddrFrom4(r.A)
			result.AddrTtls[ip] = max(result.AddrTtls[ip], int(ah.TTL))
		case ah.Type == dnsmessage.TypeAAAA && qType == dnsmessage.TypeAAAA:
			r, err := p.AAAAResource()
			if err != nil {
				return result
			}
			ip := netip.AddrFrom16(r.AAAA)
			result.AddrTtls[ip] = max(result.AddrTtls[ip], int(ah.TTL))
		default:
			if err := p.SkipAnswer(); err != nil {
				return result
			}
		}
	}
	if len(result.AddrTtls) == 0 {
		result.Miss = true
	}
	return result
}

// serverStats tracks each DoH server's recent successful resolutions in per-window token buckets — a
// current and a previous bucket per dohServerWindows span — and scores a server by summing the
// trailing-window estimates, so the fan-out order favors servers that have resolved most recently
// and most often. All methods are safe for concurrent use and safe to call on a nil *serverStats (a
// no-op / uniform-random order).
type serverStats struct {
	lock  sync.Mutex
	byUrl map[string]*serverStat
}

func newServerStats() *serverStats {
	return &serverStats{byUrl: map[string]*serverStat{}}
}

// serverStat holds one tokenBucket per dohServerWindows span (parallel index), counting the
// server's successful resolutions.
type serverStat struct {
	windows []tokenBucket
}

// tokenBucket is a sliding-window-counter approximation over a fixed span: current counts events in
// the current interval [epoch*span, (epoch+1)*span) and previous the interval before it. The
// trailing-window estimate prorates previous by how much of it still falls within the last span.
type tokenBucket struct {
	epoch    int64
	current  float64
	previous float64
}

// roll advances the bucket to the interval containing now: a single-interval step shifts
// current->previous; a longer gap clears both (the events fell out of the trailing window).
func (self *tokenBucket) roll(span time.Duration, now time.Time) {
	epoch := now.UnixNano() / int64(span)
	switch {
	case epoch == self.epoch:
	case epoch == self.epoch+1:
		self.previous = self.current
		self.current = 0
	default:
		self.previous = 0
		self.current = 0
	}
	self.epoch = epoch
}

func (self *tokenBucket) add(span time.Duration, now time.Time, n float64) {
	self.roll(span, now)
	self.current += n
}

// estimate returns the prorated event count over the trailing span ending at now.
func (self *tokenBucket) estimate(span time.Duration, now time.Time) float64 {
	self.roll(span, now)
	elapsed := now.UnixNano() - self.epoch*int64(span)
	frac := float64(int64(span)-elapsed) / float64(span)
	return self.current + self.previous*frac
}

// record credits a server with a successful resolution (ok == it returned records or an
// authoritative no-record answer); failures earn nothing and simply let the buckets decay.
func (self *serverStats) record(url string, ok bool) {
	self.recordAt(url, ok, time.Now())
}

func (self *serverStats) recordAt(url string, ok bool, now time.Time) {
	if self == nil || !ok {
		return
	}
	self.lock.Lock()
	defer self.lock.Unlock()

	st := self.byUrl[url]
	if st == nil {
		st = &serverStat{windows: make([]tokenBucket, len(dohServerWindows))}
		self.byUrl[url] = st
	}
	for k, span := range dohServerWindows {
		st.windows[k].add(span, now, 1)
	}
}

// seed pre-loads each server's windows with a persisted score (clamped to dohSeedMaxScore),
// spread evenly across the windows so the summed score matches and decays on the normal
// trailing-window schedule. Used at construction to carry the fan-out ordering across a
// restart; live results then dominate as they accrue.
func (self *serverStats) seed(scores map[string]float64) {
	if self == nil || len(scores) == 0 {
		return
	}
	now := time.Now()
	self.lock.Lock()
	defer self.lock.Unlock()
	for url, score := range scores {
		if score <= 0 {
			continue
		}
		score = min(score, dohSeedMaxScore)
		st := self.byUrl[url]
		if st == nil {
			st = &serverStat{windows: make([]tokenBucket, len(dohServerWindows))}
			self.byUrl[url] = st
		}
		for k, span := range dohServerWindows {
			st.windows[k].add(span, now, score/float64(len(dohServerWindows)))
		}
	}
}

// scores returns each known server's current summed trailing-window success estimate (the
// fan-out order weights), for the owner to persist and pass back as ServerStatsSeed on the
// next construction. Zero-score servers are omitted.
func (self *serverStats) scores() map[string]float64 {
	if self == nil {
		return nil
	}
	now := time.Now()
	self.lock.Lock()
	defer self.lock.Unlock()
	scores := map[string]float64{}
	for url := range self.byUrl {
		if score := self.scoreLocked(url, now); 0 < score {
			scores[url] = score
		}
	}
	return scores
}

// scoreLocked sums a server's trailing-window success estimates; an untried server scores 0.
func (self *serverStats) scoreLocked(url string, now time.Time) float64 {
	st := self.byUrl[url]
	if st == nil {
		return 0
	}
	var score float64
	for k, span := range dohServerWindows {
		score += st.windows[k].estimate(span, now)
	}
	return score
}

// order returns urls in a weighted-random permutation: a server's weight is its summed recent
// success score plus an exploration floor. Uses the Efraimidis–Spirakis weighted-permutation method
// (key = u^(1/w); higher weight -> earlier). A nil *serverStats yields a uniform-random shuffle.
func (self *serverStats) order(urls []string) []string {
	return self.orderAt(urls, time.Now())
}

func (self *serverStats) orderAt(urls []string, now time.Time) []string {
	ordered := append([]string{}, urls...)
	if len(ordered) <= 1 {
		return ordered
	}
	if self == nil {
		mathrand.Shuffle(len(ordered), func(i, j int) {
			ordered[i], ordered[j] = ordered[j], ordered[i]
		})
		return ordered
	}

	type weighted struct {
		url string
		key float64
	}
	ws := make([]weighted, len(ordered))
	self.lock.Lock()
	for i, url := range ordered {
		weight := dohServerWeightFloor + self.scoreLocked(url, now)
		u := mathrand.Float64()
		if u <= 0 {
			u = math.SmallestNonzeroFloat64
		}
		ws[i] = weighted{url: url, key: math.Pow(u, 1/weight)}
	}
	self.lock.Unlock()

	sort.Slice(ws, func(i, j int) bool {
		return ws[i].key > ws[j].key
	})
	for i := range ws {
		ordered[i] = ws[i].url
	}
	return ordered
}

type DohKey struct {
	RecordType string
	Domain     string
}

func NewDohKey(recordType string, domain string) DohKey {
	return DohKey{
		RecordType: strings.ToUpper(recordType),
		Domain:     strings.ToLower(domain),
	}
}

type DohResult struct {
	Time            time.Time
	AddrExpirations map[netip.Addr]time.Time
	Miss            bool
}

func (self *DohResult) Valid(now time.Time, missExpiration time.Duration) bool {
	if len(self.AddrExpirations) == 0 {
		return self.Miss && !self.Time.Add(missExpiration).Before(now)
	}
	for _, expireTime := range self.AddrExpirations {
		if expireTime.Before(now) {
			return false
		}
	}
	return true
}

// staleUsable reports whether an entry that is no longer Valid may still be served as a stale
// answer under dohStaleServeBound (RFC 8767): it holds addresses, and less than the bound has
// passed since the answer's last moment of freshness (its latest address expiration — the
// point the whole record set stopped being fresh, so the served data is never older than
// bound past what its TTL promised). A miss entry (authoritative NXDOMAIN/NODATA) is never
// stale-servable: converting a resolution failure into a stale "does not exist" would deny a
// name the resolver never re-confirmed absent, the harmful direction — RFC 8767's use case is
// keeping known-good addresses reachable, and an expired miss carries none.
func (self *DohResult) staleUsable(now time.Time) bool {
	if len(self.AddrExpirations) == 0 {
		return false
	}
	var latest time.Time
	for _, expireTime := range self.AddrExpirations {
		if latest.Before(expireTime) {
			latest = expireTime
		}
	}
	return now.Before(latest.Add(dohStaleServeBound))
}

func (self *DohResult) Addrs() []netip.Addr {
	ips := []netip.Addr{}
	for ip := range self.AddrExpirations {
		ips = append(ips, ip)
	}
	return ips
}

func Punycode(domain string) (string, error) {
	name := strings.TrimSpace(domain)

	return idna.New(
		idna.MapForLookup(),
		idna.Transitional(true),
		idna.StrictDomainName(false),
	).ToASCII(name)
}
