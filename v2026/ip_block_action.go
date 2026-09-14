package connect

import (
	"net/netip"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// block action overrides let the user override the multi client's default egress
// decisions - both the security decision (block) and the local routing decision.
// an override matches a destination by host, and the match extends to the
// destination's `IpAssoc` cluster, so blocking a single host blocks the ips that
// activity-associate with it (cdns, apis, etc).
//
// every egress decision is surfaced as a `BlockAction`, deduplicated per
// destination per event epoch, so listeners see all routing decisions without
// per-packet dispatch.

// blockActionEvictSampleSize is how many entries an over-cap insert into the
// decision or ignore cache samples to evict the soonest-to-expire from
// (approximate lru, O(sample) instead of a full scan).
const blockActionEvictSampleSize = 32

type BlockOverride struct {
	Block bool
}

type RouteOverride struct {
	Local bool
	// Pin holds the matching traffic to one stable egress.
	//
	// An APP rule is the strong form: every flow the app owns joins a single
	// app-scoped affinity group INSTEAD of its per-domain groups, so the
	// app's api session and all of its cdns converge on one exit, across
	// both ip versions.
	//
	// A HOST rule is the weak form: the matched flows keep their ordinary
	// domain groups (which already pin a site to one exit) and gain only the
	// longer bench-follow below. It exists for completeness; the app rule is
	// what fixes an app whose content fails to load.
	//
	// Either way a pinned flow's affinity inheritance follows a quarantined
	// donor for a MULTIPLE of the ordinary follow window (not indefinitely --
	// see pinnedFollowWindow), and a warned donor still refuses.
	//
	// Pin never changes block/local routing -- a pinned flow egresses
	// remotely as normal; only WHERE it egresses is held. Zero-value off, so
	// stored rules from before this field are unchanged (the store is json).
	Pin bool
}

type BlockActionOverride struct {
	// set by the caller. must be unique per override
	OverrideId Id
	// traffic matches when it overlaps any of the hosts.
	// a host can be
	// 1. an exact hostname
	// 2. a wildcard hostname: *.H matches subdomains of H, **.H matches H and subdomains
	// 3. an ipv4 or ipv6 subnet
	// 4. an ipv4 or ipv6
	Hosts         []string
	BlockOverride *BlockOverride
	RouteOverride *RouteOverride
}

// a routing decision surfaced to listeners, aggregated per cluster per epoch.
// decisions are made on the cluster level (see `IpAssoc`), so one action carries
// all the ips and hosts in the destination's cluster
type BlockAction struct {
	// time of the first decision in the epoch
	Time time.Time
	// cluster ips that did NOT match an override (disjoint from MatchedIps)
	Ips []netip.Addr
	// cluster server names that did NOT match an override (disjoint from MatchedHosts)
	Hosts []string
	// the exact ips and server names that matched an override rule (the UI renders
	// these distinctly, e.g. green). Disjoint from Ips/Hosts so nothing is shown or
	// counted twice; MatchedIps ∪ Ips is the full cluster ip set, MatchedHosts ∪
	// Hosts the full observed-name set. Empty when no override matched.
	MatchedIps   []netip.Addr
	MatchedHosts []string
	Block        bool
	Local        bool
	// set when the ad/tracker blocker matched the destination (the final
	// decision may still be un-blocked by an override; see Block)
	Blocker bool
	// set when an override determined the decision
	BlockOverrideId *Id
	RouteOverrideId *Id
	PacketCount     int
	ByteCount       ByteCount
}

type BlockActionFunction func(blockActions []*BlockAction)

// cumulative packet counts by route. read with `RemoteUserNatMultiClient.PacketStats`.
// block counts split by direction: egress is traffic blocked on the way out
// (the send path, or the provider's return into the tunnel), ingress is
// traffic blocked on the way in (the provider's receive from the tunnel)
type PacketStats struct {
	RemoteEgressPacketCount  int64
	RemoteEgressByteCount    ByteCount
	RemoteIngressPacketCount int64
	RemoteIngressByteCount   ByteCount
	LocalEgressPacketCount   int64
	LocalEgressByteCount     ByteCount
	LocalIngressPacketCount  int64
	LocalIngressByteCount    ByteCount
	BlockEgressPacketCount   int64
	BlockEgressByteCount     ByteCount
	BlockIngressPacketCount  int64
	BlockIngressByteCount    ByteCount
	// TransportStats partitions only the remote ingress/egress totals by the
	// physical carrier. Local and blocked traffic never entered a carrier.
	// Every TransportTypes entry is present in snapshots, including unknown.
	TransportStats map[TransportType]*PacketStats
}

type PacketStatsFunction func(packetStats *PacketStats)

func packetStatsEqual(a *PacketStats, b *PacketStats) bool {
	if a == nil || b == nil {
		return a == b
	}
	if a.RemoteEgressPacketCount != b.RemoteEgressPacketCount ||
		a.RemoteEgressByteCount != b.RemoteEgressByteCount ||
		a.RemoteIngressPacketCount != b.RemoteIngressPacketCount ||
		a.RemoteIngressByteCount != b.RemoteIngressByteCount ||
		a.LocalEgressPacketCount != b.LocalEgressPacketCount ||
		a.LocalEgressByteCount != b.LocalEgressByteCount ||
		a.LocalIngressPacketCount != b.LocalIngressPacketCount ||
		a.LocalIngressByteCount != b.LocalIngressByteCount ||
		a.BlockEgressPacketCount != b.BlockEgressPacketCount ||
		a.BlockEgressByteCount != b.BlockEgressByteCount ||
		a.BlockIngressPacketCount != b.BlockIngressPacketCount ||
		a.BlockIngressByteCount != b.BlockIngressByteCount {
		return false
	}
	for _, transportType := range TransportTypes() {
		aStats := a.TransportStats[transportType]
		bStats := b.TransportStats[transportType]
		if aStats == nil || bStats == nil {
			if aStats != bStats {
				return false
			}
			continue
		}
		if aStats.RemoteEgressPacketCount != bStats.RemoteEgressPacketCount ||
			aStats.RemoteEgressByteCount != bStats.RemoteEgressByteCount ||
			aStats.RemoteIngressPacketCount != bStats.RemoteIngressPacketCount ||
			aStats.RemoteIngressByteCount != bStats.RemoteIngressByteCount {
			return false
		}
	}
	return true
}

type packetStatsCounters struct {
	remoteEgressPacketCount  atomic.Int64
	remoteEgressByteCount    atomic.Int64
	remoteIngressPacketCount atomic.Int64
	remoteIngressByteCount   atomic.Int64
	localEgressPacketCount   atomic.Int64
	localEgressByteCount     atomic.Int64
	localIngressPacketCount  atomic.Int64
	localIngressByteCount    atomic.Int64
	blockEgressPacketCount   atomic.Int64
	blockEgressByteCount     atomic.Int64
	blockIngressPacketCount  atomic.Int64
	blockIngressByteCount    atomic.Int64
	transportStatsLock       sync.Mutex
	transportStats           map[TransportType]*PacketStats
}

func (self *packetStatsCounters) snapshot() *PacketStats {
	self.transportStatsLock.Lock()
	defer self.transportStatsLock.Unlock()
	packetStats := &PacketStats{
		RemoteEgressPacketCount:  self.remoteEgressPacketCount.Load(),
		RemoteEgressByteCount:    ByteCount(self.remoteEgressByteCount.Load()),
		RemoteIngressPacketCount: self.remoteIngressPacketCount.Load(),
		RemoteIngressByteCount:   ByteCount(self.remoteIngressByteCount.Load()),
		LocalEgressPacketCount:   self.localEgressPacketCount.Load(),
		LocalEgressByteCount:     ByteCount(self.localEgressByteCount.Load()),
		LocalIngressPacketCount:  self.localIngressPacketCount.Load(),
		LocalIngressByteCount:    ByteCount(self.localIngressByteCount.Load()),
		BlockEgressPacketCount:   self.blockEgressPacketCount.Load(),
		BlockEgressByteCount:     ByteCount(self.blockEgressByteCount.Load()),
		BlockIngressPacketCount:  self.blockIngressPacketCount.Load(),
		BlockIngressByteCount:    ByteCount(self.blockIngressByteCount.Load()),
		TransportStats:           map[TransportType]*PacketStats{},
	}
	for _, transportType := range TransportTypes() {
		transportStats := &PacketStats{}
		if current := self.transportStats[transportType]; current != nil {
			transportStats.RemoteEgressPacketCount = current.RemoteEgressPacketCount
			transportStats.RemoteEgressByteCount = current.RemoteEgressByteCount
			transportStats.RemoteIngressPacketCount = current.RemoteIngressPacketCount
			transportStats.RemoteIngressByteCount = current.RemoteIngressByteCount
		}
		packetStats.TransportStats[transportType] = transportStats
	}
	return packetStats
}

func normalizedTransportType(transportType TransportType) TransportType {
	switch transportType {
	case TransportTypeH3,
		TransportTypeH1,
		TransportTypeH3Dns,
		TransportTypeH3DnsPump,
		TransportTypeP2p,
		TransportTypeUnknown:
		return transportType
	default:
		return TransportTypeUnknown
	}
}

func (self *packetStatsCounters) transportStatsWithLock(transportType TransportType) *PacketStats {
	if self.transportStats == nil {
		self.transportStats = map[TransportType]*PacketStats{}
	}
	transportType = normalizedTransportType(transportType)
	stats := self.transportStats[transportType]
	if stats == nil {
		stats = &PacketStats{}
		self.transportStats[transportType] = stats
	}
	return stats
}

func (self *packetStatsCounters) recordRemoteEgress(
	transportType TransportType,
	packetCount int64,
	byteCount ByteCount,
) {
	self.transportStatsLock.Lock()
	defer self.transportStatsLock.Unlock()
	self.remoteEgressPacketCount.Add(packetCount)
	self.remoteEgressByteCount.Add(int64(byteCount))
	stats := self.transportStatsWithLock(transportType)
	stats.RemoteEgressPacketCount += packetCount
	stats.RemoteEgressByteCount += byteCount
}

func (self *packetStatsCounters) moveRemoteEgress(
	from TransportType,
	to TransportType,
	packetCount int64,
	byteCount ByteCount,
) {
	from = normalizedTransportType(from)
	to = normalizedTransportType(to)
	if from == to {
		return
	}
	self.transportStatsLock.Lock()
	defer self.transportStatsLock.Unlock()
	fromStats := self.transportStatsWithLock(from)
	toStats := self.transportStatsWithLock(to)
	fromStats.RemoteEgressPacketCount -= packetCount
	fromStats.RemoteEgressByteCount -= byteCount
	toStats.RemoteEgressPacketCount += packetCount
	toStats.RemoteEgressByteCount += byteCount
}

func (self *packetStatsCounters) recordRemoteIngress(
	transportType TransportType,
	packetCount int64,
	byteCount ByteCount,
) {
	self.transportStatsLock.Lock()
	defer self.transportStatsLock.Unlock()
	self.remoteIngressPacketCount.Add(packetCount)
	self.remoteIngressByteCount.Add(int64(byteCount))
	stats := self.transportStatsWithLock(transportType)
	stats.RemoteIngressPacketCount += packetCount
	stats.RemoteIngressByteCount += byteCount
}

// transportPacketAttribution joins asynchronous route selection to the
// synchronous logical-packet admission. Before a physical route is known the
// admitted packet lives in unknown; the first known route moves it atomically.
// Race attempts share one tracker, so duplicate physical sends cannot inflate
// logical packet totals.
type transportPacketAttribution struct {
	stateLock     sync.Mutex
	counters      *packetStatsCounters
	packetCount   int64
	byteCount     ByteCount
	admitted      bool
	observed      bool
	transportType TransportType
}

func newTransportPacketAttribution(
	counters *packetStatsCounters,
	packetCount int,
	byteCount ByteCount,
) *transportPacketAttribution {
	return &transportPacketAttribution{
		counters:      counters,
		packetCount:   int64(packetCount),
		byteCount:     byteCount,
		transportType: TransportTypeUnknown,
	}
}

func (self *transportPacketAttribution) observe(transportType TransportType) {
	transportType = normalizedTransportType(transportType)
	if transportType == TransportTypeUnknown {
		return
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.observed {
		return
	}
	self.observed = true
	self.transportType = transportType
	if self.admitted {
		self.counters.moveRemoteEgress(
			TransportTypeUnknown,
			transportType,
			self.packetCount,
			self.byteCount,
		)
	}
}

func (self *transportPacketAttribution) admit() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.admitted {
		return
	}
	self.admitted = true
	self.counters.recordRemoteEgress(
		self.transportType,
		self.packetCount,
		self.byteCount,
	)
}

// compiled override rules
type blockActionMatcher struct {
	exactHosts    map[string][]*BlockActionOverride
	wildcardHosts []*blockActionWildcardHost
	addrs         map[netip.Addr][]*BlockActionOverride
	prefixes      []*blockActionPrefix
}

type blockActionWildcardHost struct {
	// ".h" suffix match
	dotSuffix string
	// **.h also matches h itself
	base     string
	override *BlockActionOverride
}

type blockActionPrefix struct {
	prefix   netip.Prefix
	override *BlockActionOverride
}

// returns nil when there are no overrides
func newBlockActionMatcher(overrides []*BlockActionOverride) *blockActionMatcher {
	if len(overrides) == 0 {
		return nil
	}
	matcher := &blockActionMatcher{
		exactHosts: map[string][]*BlockActionOverride{},
		addrs:      map[netip.Addr][]*BlockActionOverride{},
	}
	for _, override := range overrides {
		for _, host := range override.Hosts {
			host = strings.ToLower(strings.TrimSpace(host))
			if host == "" {
				continue
			}
			if after, ok := strings.CutPrefix(host, "**."); ok {
				matcher.wildcardHosts = append(matcher.wildcardHosts, &blockActionWildcardHost{
					dotSuffix: "." + after,
					base:      after,
					override:  override,
				})
			} else if after, ok := strings.CutPrefix(host, "*."); ok {
				matcher.wildcardHosts = append(matcher.wildcardHosts, &blockActionWildcardHost{
					dotSuffix: "." + after,
					override:  override,
				})
			} else if prefix, err := netip.ParsePrefix(host); err == nil {
				matcher.prefixes = append(matcher.prefixes, &blockActionPrefix{
					prefix:   prefix.Masked(),
					override: override,
				})
			} else if addr, err := netip.ParseAddr(host); err == nil {
				addr = addr.Unmap()
				matcher.addrs[addr] = append(matcher.addrs[addr], override)
			} else {
				matcher.exactHosts[host] = append(matcher.exactHosts[host], override)
			}
		}
	}
	return matcher
}

// the merged effect of the matched overrides
type blockActionMatch struct {
	blockOverride   *BlockOverride
	blockOverrideId Id
	routeOverride   *RouteOverride
	routeOverrideId Id
	// the exact server names and ips that actually triggered a match, so the UI can
	// show which cluster members an override rule hit (disjoint from the rest). nil
	// until something matches; keys are the original-case server name / the addr.
	matchedHosts map[string]bool
	matchedIps   map[netip.Addr]bool
}

func (self *blockActionMatch) any() bool {
	return self.blockOverride != nil || self.routeOverride != nil
}

func (self *blockActionMatch) recordMatchedHost(serverName string) {
	if self.matchedHosts == nil {
		self.matchedHosts = map[string]bool{}
	}
	self.matchedHosts[serverName] = true
}

func (self *blockActionMatch) recordMatchedIp(addr netip.Addr) {
	if self.matchedIps == nil {
		self.matchedIps = map[netip.Addr]bool{}
	}
	self.matchedIps[addr] = true
}

// merge is order-independent: block=true wins over block=false,
// and local=true wins over local=false
func (self *blockActionMatch) merge(override *BlockActionOverride) {
	if override.BlockOverride != nil {
		if self.blockOverride == nil || !self.blockOverride.Block && override.BlockOverride.Block {
			self.blockOverride = override.BlockOverride
			self.blockOverrideId = override.OverrideId
		}
	}
	if override.RouteOverride != nil {
		// preference order: Local wins (local traffic never reaches a
		// provider, so a pin on the same cluster is moot), then Pin, then
		// any. Order-independent like the block half.
		replace := self.routeOverride == nil ||
			(!self.routeOverride.Local && override.RouteOverride.Local) ||
			(!self.routeOverride.Local && !self.routeOverride.Pin && override.RouteOverride.Pin)
		if replace {
			self.routeOverride = override.RouteOverride
			self.routeOverrideId = override.OverrideId
		}
	}
}

// blockActionApply combines the security policy result, the ad/tracker
// blocker, and the matched overrides into the final egress decision. the
// rules are
//  1. an incident (martian/malformed) is always blocked and not overridable
//  2. a blocker match blocks by default, like a security drop
//  3. the block override takes precedence over both default decisions,
//     and the route override over the default route (local when the security
//     result is drop and the local security bypass is on, else egress)
//  4. drop-classified traffic never egresses to a provider: un-blocking it
//     routes it locally. an un-blocked blocker match (allow-classified)
//     egresses remotely as normal
func blockActionApply(r SecurityPolicyResult, localSecurityBypass bool, blockerBlock bool, match *blockActionMatch) (block bool, local bool) {
	switch r {
	case SecurityPolicyResultAllow:
	case SecurityPolicyResultDrop:
		if localSecurityBypass {
			local = true
		} else {
			block = true
		}
	default:
		// incident. always blocked, not overridable
		block = true
		return
	}

	if blockerBlock {
		block = true
		local = false
	}

	if match != nil {
		if match.blockOverride != nil {
			block = match.blockOverride.Block
		}
		if match.routeOverride != nil {
			local = match.routeOverride.Local
		}
		if r == SecurityPolicyResultDrop && !block {
			// drop-classified traffic never egresses to a provider
			local = true
		}
		if block {
			local = false
		}
	}
	return
}

func (self *blockActionMatcher) matchAddr(match *blockActionMatch, addr netip.Addr, serverNames []string) {
	addrMatched := false
	for _, override := range self.addrs[addr] {
		match.merge(override)
		addrMatched = true
	}
	for _, blockActionPrefix := range self.prefixes {
		if blockActionPrefix.prefix.Contains(addr) {
			match.merge(blockActionPrefix.override)
			addrMatched = true
		}
	}
	if addrMatched {
		match.recordMatchedIp(addr)
	}
	for _, serverName := range serverNames {
		lower := strings.ToLower(serverName)
		hostMatched := false
		for _, override := range self.exactHosts[lower] {
			match.merge(override)
			hostMatched = true
		}
		for _, wildcardHost := range self.wildcardHosts {
			if strings.HasSuffix(lower, wildcardHost.dotSuffix) || lower == wildcardHost.base {
				match.merge(wildcardHost.override)
				hostMatched = true
			}
		}
		if hostMatched {
			// record the original-case name so it lines up with clusterHosts for the
			// disjoint partition in the collector
			match.recordMatchedHost(serverName)
		}
	}
}

// immutable snapshot swapped on `SetBlockActionOverrides`
type blockActionState struct {
	version uint64
	// nil when no overrides are set
	matcher *blockActionMatcher
}

// a cached decision for a destination.
// valid while the overrides and cluster versions and the blocker-active
// state are unchanged, up to a ttl (server names for the destination can
// drift as dns is observed)
type blockActionDecision struct {
	match *blockActionMatch
	// the ad/tracker blocker matched the destination (its ip or its own
	// observed server names — destination scoped, not the cluster)
	blockerBlock bool
	// the blocker-active state the decision was computed under
	blockerEnabled   bool
	overridesVersion uint64
	clusterVersion   uint64
	expireTime       time.Time

	// the destination's cluster. a destination with no cluster is
	// a cluster of itself
	clusterKey   netip.Addr
	clusterIps   []netip.Addr
	clusterHosts []string
}

type blockActionCache struct {
	ttl      time.Duration
	maxCount int

	stateLock sync.Mutex
	decisions map[netip.Addr]*blockActionDecision
}

func newBlockActionCache(ttl time.Duration, maxCount int) *blockActionCache {
	return &blockActionCache{
		ttl:       ttl,
		maxCount:  maxCount,
		decisions: map[netip.Addr]*blockActionDecision{},
	}
}

func (self *blockActionCache) get(addr netip.Addr, overridesVersion uint64, clusterVersion uint64, blockerActive bool, now time.Time) *blockActionDecision {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	decision, ok := self.decisions[addr]
	if !ok {
		return nil
	}
	if decision.overridesVersion != overridesVersion || decision.clusterVersion != clusterVersion || decision.blockerEnabled != blockerActive || decision.expireTime.Before(now) {
		delete(self.decisions, addr)
		return nil
	}
	return decision
}

func (self *blockActionCache) put(addr netip.Addr, decision *blockActionDecision) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.maxCount <= len(self.decisions) {
		self.evictSoonestSampleLocked()
	}
	self.decisions[addr] = decision
}

// evictSoonestSampleLocked deletes the soonest-to-expire decision of a
// blockActionEvictSampleSize sample (map iteration starts at a random bucket, so
// the sample is unbiased): approximate lru, O(sample) instead of a full scan.
// clearing the whole map at capacity instead would make every active destination
// recompute its decision at once — a server names lookup, a cluster walk, the
// override matcher and the blocker, per flow — and a device that stays over the
// cap would repeat that storm on every insert.
func (self *blockActionCache) evictSoonestSampleLocked() {
	var evictAddr netip.Addr
	var evictTime time.Time
	found := false
	i := 0
	for addr, decision := range self.decisions {
		if !found || decision.expireTime.Before(evictTime) {
			evictAddr = addr
			evictTime = decision.expireTime
			found = true
		}
		i += 1
		if blockActionEvictSampleSize <= i {
			break
		}
	}
	if found {
		delete(self.decisions, evictAddr)
	}
}

func (self *blockActionCache) clear() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	clear(self.decisions)
}

// delete drops the cached decision for one destination so its next flow rebuilds
// it (re-resolving server names). Used when a server name is learned for the ip,
// so block actions report the server name instead of the ip going forward. This
// is the incremental (per-ip) counterpart to clear(), avoiding a full recompute.
func (self *blockActionCache) delete(addr netip.Addr) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	delete(self.decisions, addr)
}

// defaultRemoteDohIgnoreAddrs is the built-in ignore baseline: the hard coded
// remote doh resolver ips from `DefaultDnsResolverSettings`. these are always
// excluded from the override and association logic, independent of the host's
// `SetBlockActionIgnoreHosts` wiring — user override rules must never capture
// dns to the well known resolvers, which the os or browser may query directly.
var defaultRemoteDohIgnoreAddrs = sync.OnceValue(func() map[netip.Addr]bool {
	addrs := map[netip.Addr]bool{}
	resolver := DefaultDnsResolverSettings()
	dohUrlLists := [][]string{
		resolver.RemoteDohUrlsIpv4,
		resolver.RemoteDohUrlsIpv6,
	}
	for _, dohUrls := range dohUrlLists {
		for _, dohUrl := range dohUrls {
			u, err := url.Parse(strings.TrimSpace(dohUrl))
			if err != nil {
				continue
			}
			if addr, err := netip.ParseAddr(u.Hostname()); err == nil {
				// unmapped, matching the `ipAssocAddr` normalization
				addrs[addr.Unmap()] = true
			}
		}
	}
	return addrs
})

// destinations excluded from the override and association logic,
// e.g. the dns resolver endpoints. host values use the same forms as
// `BlockActionOverride.Hosts`. matching is on the destination itself
// (ip and observed server names), not its cluster, since ignored
// destinations do not associate.
// immutable snapshot swapped on `SetBlockActionIgnoreHosts`
type blockActionIgnoreState struct {
	version uint64
	// nil when no ignore host values are set
	matcher *blockActionMatcher
}

// caches the per-destination ignore result.
// valid while the ignore version is unchanged, up to a ttl
// (server names for the destination can drift as dns is observed)
type blockActionIgnoreCache struct {
	ttl      time.Duration
	maxCount int

	stateLock sync.Mutex
	entries   map[netip.Addr]*blockActionIgnoreEntry
}

type blockActionIgnoreEntry struct {
	ignored    bool
	version    uint64
	expireTime time.Time
}

func newBlockActionIgnoreCache(ttl time.Duration, maxCount int) *blockActionIgnoreCache {
	return &blockActionIgnoreCache{
		ttl:      ttl,
		maxCount: maxCount,
		entries:  map[netip.Addr]*blockActionIgnoreEntry{},
	}
}

func (self *blockActionIgnoreCache) get(addr netip.Addr, version uint64, now time.Time) *blockActionIgnoreEntry {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	entry, ok := self.entries[addr]
	if !ok {
		return nil
	}
	if entry.version != version || entry.expireTime.Before(now) {
		delete(self.entries, addr)
		return nil
	}
	return entry
}

func (self *blockActionIgnoreCache) put(addr netip.Addr, entry *blockActionIgnoreEntry) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.maxCount <= len(self.entries) {
		self.evictSoonestSampleLocked()
	}
	self.entries[addr] = entry
}

// evictSoonestSampleLocked is the `blockActionCache` eviction for the ignore
// entries: drop the soonest-to-expire of a bounded sample rather than clearing
// the whole map at capacity.
func (self *blockActionIgnoreCache) evictSoonestSampleLocked() {
	var evictAddr netip.Addr
	var evictTime time.Time
	found := false
	i := 0
	for addr, entry := range self.entries {
		if !found || entry.expireTime.Before(evictTime) {
			evictAddr = addr
			evictTime = entry.expireTime
			found = true
		}
		i += 1
		if blockActionEvictSampleSize <= i {
			break
		}
	}
	if found {
		delete(self.entries, evictAddr)
	}
}

func (self *blockActionIgnoreCache) clear() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	clear(self.entries)
}

// delete drops the cached ignore decision for one destination (per-ip
// counterpart to clear()); see blockActionCache.delete.
func (self *blockActionIgnoreCache) delete(addr netip.Addr) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	delete(self.entries, addr)
}

// aggregates decisions per destination per epoch
type blockActionCollector struct {
	maxCount int
	log      Logger

	callbacks     *CallbackList[BlockActionFunction]
	callbackCount atomic.Int64

	stateLock sync.Mutex
	agg       map[blockActionKey]*blockActionAgg
	// aggregations dropped at the cap this epoch. surfaced at flush so the
	// under-reporting is visible rather than silent
	droppedCount int
}

type blockActionKey struct {
	// the canonical cluster member (the lowest ip)
	clusterKey netip.Addr
	block      bool
	local      bool
	// the ad/tracker blocker matched the destination
	blocker bool
	// zero when no override applied
	blockOverrideId Id
	routeOverrideId Id
}

type blockActionAgg struct {
	firstTime time.Time
	ips       []netip.Addr
	hosts     []string
	// the exact ips / server names that matched an override, unioned across the
	// epoch's decisions for this key (a cluster can match on different members over
	// the epoch). nil until an override matches.
	matchedIps   map[netip.Addr]bool
	matchedHosts map[string]bool
	packetCount  int
	byteCount    ByteCount
}

func newBlockActionCollector(maxCount int, log Logger) *blockActionCollector {
	return &blockActionCollector{
		maxCount:  maxCount,
		log:       loggerOrDefault(log),
		callbacks: NewCallbackList[BlockActionFunction](),
		agg:       map[blockActionKey]*blockActionAgg{},
	}
}

func (self *blockActionCollector) hasCallbacks() bool {
	return 0 < self.callbackCount.Load()
}

func (self *blockActionCollector) addCallback(callback BlockActionFunction) func() {
	callbackId := self.callbacks.Add(callback)
	self.callbackCount.Add(1)
	return func() {
		self.callbacks.Remove(callbackId)
		self.callbackCount.Add(-1)
	}
}

func (self *blockActionCollector) add(
	decision *blockActionDecision,
	block bool,
	local bool,
	match *blockActionMatch,
	byteCount ByteCount,
) {
	key := blockActionKey{
		clusterKey: decision.clusterKey,
		block:      block,
		local:      local,
		blocker:    decision.blockerBlock,
	}
	if match != nil {
		if match.blockOverride != nil {
			key.blockOverrideId = match.blockOverrideId
		}
		if match.routeOverride != nil {
			key.routeOverrideId = match.routeOverrideId
		}
	}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	agg, ok := self.agg[key]
	if !ok {
		if self.maxCount <= len(self.agg) {
			// at capacity. drop the aggregation for this epoch, and count the
			// drop: the itemized block action list then under-reports exactly
			// during the destination bursts a user would most want to inspect,
			// so the loss must not be silent. the packet and block stats
			// counters are unaffected (separate atomics)
			self.droppedCount += 1
			return
		}
		agg = &blockActionAgg{
			firstTime: time.Now(),
			ips:       decision.clusterIps,
			hosts:     decision.clusterHosts,
		}
		self.agg[key] = agg
	} else if len(agg.hosts) == 0 && 0 < len(decision.clusterHosts) {
		// the aggregate was seeded by a decision made before the destination's
		// server name was learned (e.g. the flow's first packets raced the DNS
		// record, or a stale cached decision was since invalidated). Adopt the
		// named decision's hosts so the flushed action reports the server name
		// instead of the ip.
		agg.hosts = decision.clusterHosts
	}
	// carry which cluster members an override actually hit, unioned across the
	// epoch, so the flushed action can render them distinctly (and disjoint from
	// the rest). Same key => same override decision, so the sets accumulate cleanly.
	if match != nil {
		for host := range match.matchedHosts {
			if agg.matchedHosts == nil {
				agg.matchedHosts = map[string]bool{}
			}
			agg.matchedHosts[host] = true
		}
		for ip := range match.matchedIps {
			if agg.matchedIps == nil {
				agg.matchedIps = map[netip.Addr]bool{}
			}
			agg.matchedIps[ip] = true
		}
	}
	agg.packetCount += 1
	agg.byteCount += byteCount
}

func (self *blockActionCollector) flush() {
	var agg map[blockActionKey]*blockActionAgg
	dropped := 0
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		dropped = self.droppedCount
		self.droppedCount = 0
		if len(self.agg) == 0 {
			return
		}
		agg = self.agg
		self.agg = map[blockActionKey]*blockActionAgg{}
	}()
	if 0 < dropped {
		self.log.Warningf(
			"[block]dropped %d block action aggregations at the %d per-epoch cap; the block action list under-reports this epoch\n",
			dropped,
			self.maxCount,
		)
	}
	if len(agg) == 0 {
		return
	}

	blockActions := make([]*BlockAction, 0, len(agg))
	for key, keyAgg := range agg {
		// partition into matched (an override hit these) and the rest, disjoint:
		// Hosts/Ips exclude everything in the matched sets, MatchedHosts/MatchedIps
		// are the full matched sets (which may include a name learned after the agg
		// was seeded, so we don't derive them by filtering the cluster list).
		blockAction := &BlockAction{
			Time:         keyAgg.firstTime,
			Ips:          excludeMatchedIps(keyAgg.ips, keyAgg.matchedIps),
			Hosts:        excludeMatchedHosts(keyAgg.hosts, keyAgg.matchedHosts),
			MatchedIps:   sortedMatchedIps(keyAgg.matchedIps),
			MatchedHosts: sortedMatchedHosts(keyAgg.matchedHosts),
			Block:        key.block,
			Local:        key.local,
			Blocker:      key.blocker,
			PacketCount:  keyAgg.packetCount,
			ByteCount:    keyAgg.byteCount,
		}
		if key.blockOverrideId != (Id{}) {
			blockOverrideId := key.blockOverrideId
			blockAction.BlockOverrideId = &blockOverrideId
		}
		if key.routeOverrideId != (Id{}) {
			routeOverrideId := key.routeOverrideId
			blockAction.RouteOverrideId = &routeOverrideId
		}
		blockActions = append(blockActions, blockAction)
	}
	sort.Slice(blockActions, func(i int, j int) bool {
		return blockActions[i].Time.Before(blockActions[j].Time)
	})

	for _, callback := range self.callbacks.Get() {
		HandleError(func() {
			callback(blockActions)
		})
	}
}

// excludeMatchedIps returns the ips not in the matched set, preserving order, so
// BlockAction.Ips stays disjoint from BlockAction.MatchedIps.
func excludeMatchedIps(ips []netip.Addr, matched map[netip.Addr]bool) []netip.Addr {
	if len(matched) == 0 {
		return ips
	}
	out := make([]netip.Addr, 0, len(ips))
	for _, ip := range ips {
		if !matched[ip] {
			out = append(out, ip)
		}
	}
	return out
}

// excludeMatchedHosts returns the hosts not in the matched set, preserving order,
// so BlockAction.Hosts stays disjoint from BlockAction.MatchedHosts.
func excludeMatchedHosts(hosts []string, matched map[string]bool) []string {
	if len(matched) == 0 {
		return hosts
	}
	out := make([]string, 0, len(hosts))
	for _, host := range hosts {
		if !matched[host] {
			out = append(out, host)
		}
	}
	return out
}

// sortedMatchedIps is the matched ip set as a stable-ordered slice (nil if empty).
func sortedMatchedIps(matched map[netip.Addr]bool) []netip.Addr {
	if len(matched) == 0 {
		return nil
	}
	out := make([]netip.Addr, 0, len(matched))
	for ip := range matched {
		out = append(out, ip)
	}
	sort.Slice(out, func(i int, j int) bool {
		return out[i].Compare(out[j]) < 0
	})
	return out
}

// sortedMatchedHosts is the matched server-name set as a stable-ordered slice
// (nil if empty).
func sortedMatchedHosts(matched map[string]bool) []string {
	if len(matched) == 0 {
		return nil
	}
	out := make([]string, 0, len(matched))
	for host := range matched {
		out = append(out, host)
	}
	sort.Strings(out)
	return out
}
