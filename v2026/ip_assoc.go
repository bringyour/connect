package connect

import (
	"context"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/publicsuffix"
)

// IpAssoc is a time-blocked co-association matrix that tracks clusters
// of IpPaths by ip address and server base names.
// The core association is packet activity that occurs within `PacketAssociationTime`
// of each other on each path. The association probability in a time block is
// `P(a|b) = (co-occuring packets of a with b)/min(total packets of a, total packets of b)`
// The clustering uses an agglomative clustering with a mean inter-entity co-association threshold,
// which means each member has a mean of at least the inter-entity co-association threshold.
// The algorithm is greedy to build the largest clusters that satisfy the mean inter-entity co-association threshold,
// starting with all paths in one cluster and recursively splitting off paths to keep the largest amount that satisfy the mean inter-entity co-association threshold.
// the association clustering should use data from all available time blocks, not just the latest
//
// implementation notes:
// - an entity is a remote endpoint ip (egress destination, ingress source).
//   entities that share a server base name (eTLD+1, resolved via the optional
//   `ServerNameLookup`) are merged before clustering, since they are the same site.
// - packet activity is coalesced per `ActivityGranule` so the per-packet hot path is
//   one lock + map probe in the steady state. co-occurrence therefore counts active
//   granules, not raw packets, which bounds the pair-update cost under load and is
//   robust to packet-rate skew between paths.
// - each activity pairs with at most `MaxPairPartnersPerActivity` co-active entities
//   (a uniform sample via randomized map iteration), so a burst of n concurrent
//   endpoints cannot mint n^2 pairs; persistent co-activity still accumulates the
//   highest counts over a block.
// - a block stores entities on a dense uint16 index (addresses interned once per
//   block) and pairs on a packed uint32 index-pair key, so the matrix costs a few
//   bytes per pair instead of two addresses per pair.
// - clustering runs on the `ClusterEpoch` only when new activity was recorded, over
//   reusable scratch (the aggregate is refilled, not reallocated), and only over
//   connected components of the co-association graph with at most
//   `MaxComponentNodeCount` nodes — a larger component is a degenerate burst clique
//   with quadratic split cost and little affinity value, and is skipped.
// - a clustering pass checks the assoc ctx between phases, so teardown aborts an
//   in-flight pass instead of letting it run out over a large matrix. the run loop
//   drops the matrix and unregisters the memory shedder on exit (Close or parent
//   ctx cancel), and `ShedMemory` (registered with `AddMemoryShedder`) drops it
//   under host memory pressure; both publish empty clusters so consumers do not act
//   on stale members.

type IpAssocSettings struct {
	AssociationBlockDuration time.Duration
	AssociationBlockCount    int

	// activity on two paths within this time counts as co-occurring
	PacketAssociationTime time.Duration
	// per-entity activity is coalesced to at most one count per granule
	ActivityGranule time.Duration

	MinMeanAssociation float64

	ClusterEpoch time.Duration

	// bounds per block. entities/pairs beyond these are not counted.
	// MaxEntityCount is clamped to 65535 (entities are block-indexed on uint16).
	MaxEntityCount      int
	MaxAssociationCount int

	// MaxPairPartnersPerActivity caps how many co-active entities one activity pairs
	// with — a uniform sample of the active set (randomized map iteration order) — so
	// a burst of concurrent endpoints grows pairs linearly, not quadratically.
	// 0 uses a default.
	MaxPairPartnersPerActivity int

	// MaxComponentNodeCount skips clustering a connected component larger than this
	// many nodes: a degenerate burst clique has quadratic split cost and little
	// affinity value. 0 uses a default.
	MaxComponentNodeCount int
}

func DefaultIpAssocSettings() *IpAssocSettings {
	return &IpAssocSettings{
		AssociationBlockDuration:   300 * time.Second,
		AssociationBlockCount:      8,
		PacketAssociationTime:      1 * time.Second,
		ActivityGranule:            1 * time.Second,
		MinMeanAssociation:         0.9,
		ClusterEpoch:               1 * time.Second,
		MaxEntityCount:             2048,
		MaxAssociationCount:        16384,
		MaxPairPartnersPerActivity: defaultMaxPairPartnersPerActivity,
		MaxComponentNodeCount:      defaultMaxComponentNodeCount,
	}
}

const (
	// defaultMaxPairPartnersPerActivity is the MaxPairPartnersPerActivity default.
	defaultMaxPairPartnersPerActivity = 16
	// defaultMaxComponentNodeCount is the MaxComponentNodeCount default.
	defaultMaxComponentNodeCount = 256
	// maxIpAssocEntityCount is the hard entity-per-block bound (dense uint16 index).
	maxIpAssocEntityCount = 65535
	// ipAssocPruneScanMin is the minimum lastActive entries an activity's pair scan
	// visits before it may stop early, so pruning of expired activity stays amortized
	// even when the pair-partner cap is reached immediately.
	ipAssocPruneScanMin = 64
	// ipAssocScratchShrinkMin is the scratch map fill above which a large-then-idle
	// pass replaces the map (clear keeps buckets, which would pin a burst's footprint).
	ipAssocScratchShrinkMin = 4096
)

// canonical pair key with a < b. used by the map-input convenience form of
// clustering (see clusterIpAssoc); blocks store pairs on packed dense indexes.
type ipAssocPair struct {
	a netip.Addr
	b netip.Addr
}

func newIpAssocPair(a netip.Addr, b netip.Addr) ipAssocPair {
	if b.Less(a) {
		a, b = b, a
	}
	return ipAssocPair{a: a, b: b}
}

// ipAssocPackIndexPair packs two block entity indexes into the canonical (low, high)
// pair key.
func ipAssocPackIndexPair(i uint16, j uint16) uint32 {
	if j < i {
		i, j = j, i
	}
	return uint32(i)<<16 | uint32(j)
}

// ipAssocPackNodePair packs two dense ids into the canonical (low, high) pair key.
func ipAssocPackNodePair(a uint32, b uint32) uint64 {
	if b < a {
		a, b = b, a
	}
	return uint64(a)<<32 | uint64(b)
}

// ipAssocBlock stores one time block of the matrix on a dense entity index:
// an address is interned once (indexes/addrs), activity is a plain slice, and a
// pair is a packed index pair — a few bytes per pair instead of two addresses.
// An entity holds an index (and counts toward `MaxEntityCount`) once it records
// activity or appears as a pair partner.
type ipAssocBlock struct {
	endTime time.Time
	// entity -> dense index into addrs/counts
	indexes map[netip.Addr]uint16
	// index -> entity
	addrs []netip.Addr
	// index -> active granule count (0 for a pure pair partner)
	counts []uint32
	// packed index pair -> co-occurring granule count
	coCounts map[uint32]uint32
}

func newIpAssocBlock(endTime time.Time) *ipAssocBlock {
	return &ipAssocBlock{
		endTime:  endTime,
		indexes:  map[netip.Addr]uint16{},
		coCounts: map[uint32]uint32{},
	}
}

// index returns the dense index for addr, interning it if there is entity capacity.
func (self *ipAssocBlock) index(addr netip.Addr, maxEntityCount int) (uint16, bool) {
	if i, ok := self.indexes[addr]; ok {
		return i, true
	}
	if maxEntityCount <= len(self.addrs) {
		return 0, false
	}
	i := uint16(len(self.addrs))
	self.indexes[addr] = i
	self.addrs = append(self.addrs, addr)
	self.counts = append(self.counts, 0)
	return i, true
}

// countFor returns the entity's active granule count in this block (0 if absent).
func (self *ipAssocBlock) countFor(addr netip.Addr) uint32 {
	if i, ok := self.indexes[addr]; ok {
		return self.counts[i]
	}
	return 0
}

// immutable clustering snapshot, read lock-free on the packet path
type ipAssocClusters struct {
	version uint64
	// entity -> cluster member entities (including the entity), for clusters with 2+ members
	members map[netip.Addr][]netip.Addr
}

type IpAssoc struct {
	ctx    context.Context
	cancel context.CancelFunc

	settings *IpAssocSettings

	serverNameLookup atomic.Pointer[serverNameLookupHolder]

	stateLock sync.Mutex
	// newest last
	blocks []*ipAssocBlock
	// entities active within `PacketAssociationTime`, entity -> last activity
	lastActive map[netip.Addr]time.Time
	// entity -> server base names (eTLD+1), bounded small
	baseNames map[netip.Addr][]string
	dirty     bool
	// generation increments when the matrix is dropped (shed or teardown), so an
	// in-flight clustering pass does not publish a stale result over the freshly
	// emptied clusters
	generation uint64

	unregisterShed func()

	clusters atomic.Pointer[ipAssocClusters]

	// scratch is the reusable aggregation/clustering workspace, owned exclusively by
	// the clustering pass (the run goroutine; tests with a long ClusterEpoch drive
	// updateClusters directly instead)
	scratch ipAssocScratch
}

// indirection so a nil interface can be stored/cleared atomically
type serverNameLookupHolder struct {
	lookup ServerNameLookup
}

func NewIpAssoc(ctx context.Context, settings *IpAssocSettings) *IpAssoc {
	cancelCtx, cancel := context.WithCancel(ctx)

	ipAssoc := &IpAssoc{
		ctx:        cancelCtx,
		cancel:     cancel,
		settings:   settings,
		lastActive: map[netip.Addr]time.Time{},
		baseNames:  map[netip.Addr][]string{},
	}
	ipAssoc.clusters.Store(&ipAssocClusters{
		version: 0,
		members: map[netip.Addr][]netip.Addr{},
	})
	// drop the matrix under host memory pressure; the signal rebuilds from live traffic
	ipAssoc.unregisterShed = AddMemoryShedder(ipAssoc.ShedMemory)

	go HandleError(ipAssoc.run, cancel)

	return ipAssoc
}

// maxEntityCount is the per-block entity bound, clamped to the dense index range.
func (self *IpAssoc) maxEntityCount() int {
	return min(self.settings.MaxEntityCount, maxIpAssocEntityCount)
}

// maxPairPartners is the per-activity pair fan-out bound (0 settings value = default).
func (self *IpAssoc) maxPairPartners() int {
	if 0 < self.settings.MaxPairPartnersPerActivity {
		return self.settings.MaxPairPartnersPerActivity
	}
	return defaultMaxPairPartnersPerActivity
}

// maxComponentNodes is the clustering component bound (0 settings value = default).
func (self *IpAssoc) maxComponentNodes() int {
	if 0 < self.settings.MaxComponentNodeCount {
		return self.settings.MaxComponentNodeCount
	}
	return defaultMaxComponentNodeCount
}

// SetServerNameLookup installs (or clears, with nil) the lookup used to resolve
// entity ips to server base names. Safe to call at runtime.
func (self *IpAssoc) SetServerNameLookup(serverNameLookup ServerNameLookup) {
	self.serverNameLookup.Store(&serverNameLookupHolder{
		lookup: serverNameLookup,
	})
}

func (self *IpAssoc) AddEgressPacket(path *IpPath) {
	if addr, ok := ipAssocAddr(path.DestinationIp); ok {
		self.addActivity(addr)
	}
}

// the ingress path is the return packet as received, before reverse
// (the remote endpoint is the source)
func (self *IpAssoc) AddIngressPacket(path *IpPath) {
	if addr, ok := ipAssocAddr(path.SourceIp); ok {
		self.addActivity(addr)
	}
}

func ipAssocAddr(ip []byte) (netip.Addr, bool) {
	addr, ok := netip.AddrFromSlice(ip)
	if !ok {
		return netip.Addr{}, false
	}
	return addr.Unmap(), true
}

func (self *IpAssoc) addActivity(addr netip.Addr) {
	now := time.Now()

	needBaseNames := false
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		if last, ok := self.lastActive[addr]; ok && now.Sub(last) < self.settings.ActivityGranule {
			// coalesced into the current granule. steady-state fast path
			return
		}

		block := self.blockWithLock(now)

		idx, ok := block.index(addr, self.maxEntityCount())
		if !ok {
			// the block is at entity capacity
			return
		}
		block.counts[idx] += 1

		// pair with a bounded sample of the entities active within the association
		// time, pruning expired entries during the same scan. map iteration starts
		// at a random position, so the sample is uniform and the amortized pruning
		// (at least ipAssocPruneScanMin entries per scan) reaches every entry.
		maxPartners := self.maxPairPartners()
		partners := 0
		visited := 0
		for other, otherLast := range self.lastActive {
			visited += 1
			if other != addr {
				if self.settings.PacketAssociationTime < now.Sub(otherLast) {
					delete(self.lastActive, other)
				} else if partners < maxPartners {
					if otherIdx, ok := block.index(other, self.maxEntityCount()); ok {
						key := ipAssocPackIndexPair(idx, otherIdx)
						if _, ok := block.coCounts[key]; ok || len(block.coCounts) < self.settings.MaxAssociationCount {
							block.coCounts[key] += 1
						}
						partners += 1
					}
				}
			}
			if maxPartners <= partners && ipAssocPruneScanMin <= visited {
				break
			}
		}
		self.lastActive[addr] = now

		if _, ok := self.baseNames[addr]; !ok {
			// mark looked-up so nameless entities do not repeat the lookup.
			// names observed later refresh after the entity ages out of all blocks
			self.baseNames[addr] = nil
			needBaseNames = true
		}

		self.dirty = true
	}()

	if needBaseNames {
		// the lookup calls an external object, so it must not hold the state lock
		if baseNames := self.lookupBaseNames(addr); baseNames != nil {
			self.stateLock.Lock()
			defer self.stateLock.Unlock()
			self.baseNames[addr] = baseNames
		}
	}
}

func (self *IpAssoc) lookupBaseNames(addr netip.Addr) []string {
	holder := self.serverNameLookup.Load()
	if holder == nil || holder.lookup == nil {
		return nil
	}
	serverNames := holder.lookup.ServerNames(addr.String())
	if len(serverNames) == 0 {
		return nil
	}
	var baseNames []string
	seen := map[string]bool{}
	for _, serverName := range serverNames {
		baseName := serverName
		if rootDomain, err := publicsuffix.EffectiveTLDPlusOne(serverName); err == nil {
			baseName = rootDomain
		}
		if !seen[baseName] {
			seen[baseName] = true
			baseNames = append(baseNames, baseName)
		}
	}
	return baseNames
}

// called with stateLock
func (self *IpAssoc) blockWithLock(now time.Time) *ipAssocBlock {
	if n := len(self.blocks); 0 < n && now.Before(self.blocks[n-1].endTime) {
		return self.blocks[n-1]
	}
	block := newIpAssocBlock(now.Add(self.settings.AssociationBlockDuration))
	self.blocks = append(self.blocks, block)
	if self.settings.AssociationBlockCount < len(self.blocks) {
		self.blocks = self.blocks[len(self.blocks)-self.settings.AssociationBlockCount:]
		// prune base names for entities no longer in any block
		liveAddrs := map[netip.Addr]bool{}
		for _, block := range self.blocks {
			for addr := range block.indexes {
				liveAddrs[addr] = true
			}
		}
		for addr := range self.baseNames {
			if !liveAddrs[addr] {
				delete(self.baseNames, addr)
			}
		}
	}
	return block
}

func (self *IpAssoc) run() {
	defer self.cancel()
	// teardown (Close or parent ctx cancel): a stopped assoc must not hold the
	// matrix or stay registered for memory shedding. the scratch is owned by this
	// goroutine, so it is released here too.
	defer func() {
		self.unregisterShed()
		self.ShedMemory()
		self.scratch = ipAssocScratch{}
	}()

	for {
		select {
		case <-self.ctx.Done():
			return
		case <-time.After(self.settings.ClusterEpoch):
		}

		self.updateClusters()
	}
}

// ShedMemory drops the association matrix — blocks, activity, the name cache, and the
// published clusters — under host memory pressure (or at teardown). The signal rebuilds
// from live traffic; consumers see empty clusters rather than stale members.
func (self *IpAssoc) ShedMemory() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.blocks = nil
	clear(self.lastActive)
	clear(self.baseNames)
	self.dirty = false
	self.generation += 1
	previous := self.clusters.Load()
	if 0 < len(previous.members) {
		self.clusters.Store(&ipAssocClusters{
			version: previous.version + 1,
			members: map[netip.Addr][]netip.Addr{},
		})
	}
}

func (self *IpAssoc) updateClusters() {
	scratch := &self.scratch
	var generation uint64
	dirty := func() bool {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		if !self.dirty {
			return false
		}
		self.dirty = false
		generation = self.generation

		// aggregate all available time blocks into the reusable scratch: the matrix
		// summed on dense aggregate indexes, base names carried by reference. pairs
		// collect as a flat slice here (cheap under the lock, and several times
		// smaller than a map); the clustering pass sorts and coalesces them outside
		// the lock.
		scratch.resetAggregate()
		for _, block := range self.blocks {
			remap := scratch.remap[:0]
			for i, addr := range block.addrs {
				aggIdx := scratch.index(addr)
				scratch.counts[aggIdx] += uint64(block.counts[i])
				scratch.baseNames[aggIdx] = self.baseNames[addr]
				remap = append(remap, aggIdx)
			}
			scratch.remap = remap
			for packed, coCount := range block.coCounts {
				i := remap[uint16(packed>>16)]
				j := remap[uint16(packed)]
				scratch.pairs = append(scratch.pairs, ipAssocAggPair{
					key:   ipAssocPackNodePair(i, j),
					count: coCount,
				})
			}
		}
		return true
	}()
	if !dirty {
		return
	}

	members := clusterIpAssocScratch(self.ctx, scratch, self.settings.MinMeanAssociation, self.maxComponentNodes())
	scratch.finishPass()
	if members == nil {
		// aborted by teardown
		return
	}

	previous := self.clusters.Load()
	if ipAssocClustersEqual(previous.members, members) {
		return
	}
	// do not publish over a shed/teardown that raced this pass
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if generation != self.generation {
		return
	}
	self.clusters.Store(&ipAssocClusters{
		version: previous.version + 1,
		members: members,
	})
}

// ipAssocAggPair is one aggregated pair observation: a packed pair key and its
// summed co-occurrence count. The aggregate keeps pairs as a flat slice, sorted and
// coalesced outside the state lock — the clustering only ever iterates pairs, and a
// slice is several times smaller than a map at the same fill.
type ipAssocAggPair struct {
	key   uint64
	count uint32
}

// ipAssocScratch is the reusable aggregation and clustering workspace: the map is
// cleared and refilled (buckets are kept), slices are truncated and regrown, so a
// steady-state clustering pass allocates almost nothing. finishPass shrinks storage
// whose fill collapsed well below its high water, so one burst does not pin its
// peak footprint.
type ipAssocScratch struct {
	// aggregate matrix: entity -> dense aggregate index
	indexes map[netip.Addr]uint32
	// aggregate index -> entity / summed count / base names (shared, read-only)
	addrs     []netip.Addr
	counts    []uint64
	baseNames [][]string
	// pair observations on packed aggregate index pairs; sorted and coalesced by the
	// clustering pass, and remapped in place onto node pairs after the name merge
	pairs []ipAssocAggPair
	// block index -> aggregate index, reused per block
	remap []uint32

	// clustering workspace, sized to the aggregate per pass
	parent         []uint32
	nodeIds        []int32
	nodeCounts     []uint64
	nodeMemberInit []int32
	nodeMembers    []uint32
	memberFill     []int32
	compParent     []uint32
	compSizes      []int32
	compIds        []int32
	compInit       []int32
	compNodes      []uint32
	degrees        []int32
	adjInit        []int32
	adjNodes       []uint32
	adjPs          []float64
	splitInCluster []bool
	splitSums      []float64

	entityHighWater int
}

func (self *ipAssocScratch) resetAggregate() {
	if self.indexes == nil {
		self.indexes = map[netip.Addr]uint32{}
	}
	clear(self.indexes)
	self.addrs = self.addrs[:0]
	self.counts = self.counts[:0]
	self.baseNames = self.baseNames[:0]
	self.pairs = self.pairs[:0]
}

// index returns the dense aggregate index for addr, interning it on first use.
func (self *ipAssocScratch) index(addr netip.Addr) uint32 {
	if i, ok := self.indexes[addr]; ok {
		return i
	}
	i := uint32(len(self.addrs))
	self.indexes[addr] = i
	self.addrs = append(self.addrs, addr)
	self.counts = append(self.counts, 0)
	self.baseNames = append(self.baseNames, nil)
	return i
}

// finishPass tracks fill high water and shrinks storage whose fill collapsed well
// below it (a cleared map keeps its buckets and a truncated slice its backing array,
// which would otherwise pin a burst's peak footprint).
func (self *ipAssocScratch) finishPass() {
	if self.entityHighWater < len(self.indexes) {
		self.entityHighWater = len(self.indexes)
	}
	if ipAssocScratchShrinkMin <= self.entityHighWater && len(self.indexes)*4 < self.entityHighWater {
		fresh := make(map[netip.Addr]uint32, len(self.indexes))
		for addr, i := range self.indexes {
			fresh[addr] = i
		}
		self.indexes = fresh
		self.entityHighWater = len(self.indexes)
	}
	if ipAssocScratchShrinkMin <= cap(self.pairs) && len(self.pairs)*4 < cap(self.pairs) {
		self.pairs = make([]ipAssocAggPair, 0, len(self.pairs))
	}
}

// coalesceIpAssocPairs sorts the pair observations by key and sums duplicates in
// place, returning the compacted slice.
func coalesceIpAssocPairs(pairs []ipAssocAggPair) []ipAssocAggPair {
	slices.SortFunc(pairs, func(x ipAssocAggPair, y ipAssocAggPair) int {
		switch {
		case x.key < y.key:
			return -1
		case y.key < x.key:
			return 1
		default:
			return 0
		}
	})
	coalesced := pairs[:0]
	for _, pair := range pairs {
		if n := len(coalesced); 0 < n && coalesced[n-1].key == pair.key {
			coalesced[n-1].count += pair.count
		} else {
			coalesced = append(coalesced, pair)
		}
	}
	return coalesced
}

// find is path-compressing union-find over the aggregate indexes.
func (self *ipAssocScratch) find(i uint32) uint32 {
	for self.parent[i] != i {
		self.parent[i] = self.parent[self.parent[i]]
		i = self.parent[i]
	}
	return i
}

func (self *ipAssocScratch) union(a uint32, b uint32) {
	ra := self.find(a)
	rb := self.find(b)
	if ra != rb {
		self.parent[rb] = ra
	}
}

// growUint32 / growInt32 / growUint64 / growBool size a reusable slice to n, reusing
// the backing array when it is large enough.
func growUint32(s []uint32, n int) []uint32 {
	if cap(s) < n {
		return make([]uint32, n)
	}
	return s[:n]
}

func growInt32(s []int32, n int) []int32 {
	if cap(s) < n {
		return make([]int32, n)
	}
	return s[:n]
}

func growUint64(s []uint64, n int) []uint64 {
	if cap(s) < n {
		return make([]uint64, n)
	}
	return s[:n]
}

func growFloat64(s []float64, n int) []float64 {
	if cap(s) < n {
		return make([]float64, n)
	}
	return s[:n]
}

func growBool(s []bool, n int) []bool {
	if cap(s) < n {
		return make([]bool, n)
	}
	return s[:n]
}

// clusterIpAssoc merges entities by shared base name into nodes, then greedily
// clusters the nodes: starting from each connected component of the co-association
// graph, the node with the lowest mean association is split off until every member's
// mean association with the rest of the cluster is at least `minMeanAssociation`.
// split-off nodes re-cluster among themselves the same way.
// returns entity -> cluster member entities, for clusters with 2+ entities.
//
// this map-input form is a convenience over the dense scratch pass that
// updateClusters uses directly (it builds a one-off scratch from the maps).
func clusterIpAssoc(
	counts map[netip.Addr]uint32,
	coCounts map[ipAssocPair]uint32,
	baseNames map[netip.Addr][]string,
	minMeanAssociation float64,
) map[netip.Addr][]netip.Addr {
	scratch := &ipAssocScratch{}
	scratch.resetAggregate()
	for addr, count := range counts {
		i := scratch.index(addr)
		scratch.counts[i] += uint64(count)
		scratch.baseNames[i] = baseNames[addr]
	}
	for pair, coCount := range coCounts {
		// pairs for entities without counts are dropped (they aged out of all blocks)
		i, iOk := scratch.indexes[pair.a]
		j, jOk := scratch.indexes[pair.b]
		if !iOk || !jOk {
			continue
		}
		scratch.pairs = append(scratch.pairs, ipAssocAggPair{
			key:   ipAssocPackNodePair(i, j),
			count: coCount,
		})
	}
	return clusterIpAssocScratch(context.Background(), scratch, minMeanAssociation, defaultMaxComponentNodeCount)
}

// clusterIpAssocScratch runs the clustering pass over the filled aggregate scratch.
// It checks ctx between phases and returns nil when aborted (teardown), so a pass
// over a large matrix stops promptly instead of running out.
func clusterIpAssocScratch(
	ctx context.Context,
	scratch *ipAssocScratch,
	minMeanAssociation float64,
	maxComponentNodes int,
) map[netip.Addr][]netip.Addr {
	entityCount := len(scratch.addrs)
	members := map[netip.Addr][]netip.Addr{}
	if entityCount == 0 {
		return members
	}

	// aggregate duplicate pair observations (the same pair seen in several blocks)
	scratch.pairs = coalesceIpAssocPairs(scratch.pairs)
	if ctx.Err() != nil {
		return nil
	}

	// merge entities that share a base name (union-find; transitive across names)
	scratch.parent = growUint32(scratch.parent, entityCount)
	for i := range entityCount {
		scratch.parent[i] = uint32(i)
	}
	merged := false
	nameFirst := map[string]uint32{}
	for i := range entityCount {
		for _, baseName := range scratch.baseNames[i] {
			if first, ok := nameFirst[baseName]; ok {
				scratch.union(first, uint32(i))
				merged = true
			} else {
				nameFirst[baseName] = uint32(i)
			}
		}
	}
	if ctx.Err() != nil {
		return nil
	}

	// dense node ids per union root, node counts, and node -> member entities (csr)
	scratch.nodeIds = growInt32(scratch.nodeIds, entityCount)
	for i := range entityCount {
		scratch.nodeIds[i] = -1
	}
	nodeCount := 0
	for i := range entityCount {
		root := scratch.find(uint32(i))
		if scratch.nodeIds[root] < 0 {
			scratch.nodeIds[root] = int32(nodeCount)
			nodeCount += 1
		}
		scratch.nodeIds[i] = scratch.nodeIds[root]
	}
	scratch.nodeCounts = growUint64(scratch.nodeCounts, nodeCount)
	clear(scratch.nodeCounts)
	// nodeMemberInit doubles as the running fill cursor while nodeMembers is built
	scratch.nodeMemberInit = growInt32(scratch.nodeMemberInit, nodeCount+1)
	clear(scratch.nodeMemberInit)
	for i := range entityCount {
		node := scratch.nodeIds[i]
		scratch.nodeCounts[node] += scratch.counts[i]
		scratch.nodeMemberInit[node+1] += 1
	}
	for node := range nodeCount {
		scratch.nodeMemberInit[node+1] += scratch.nodeMemberInit[node]
	}
	scratch.nodeMembers = growUint32(scratch.nodeMembers, entityCount)
	scratch.memberFill = growInt32(scratch.memberFill, nodeCount)
	copy(scratch.memberFill, scratch.nodeMemberInit[:nodeCount])
	for i := range entityCount {
		node := scratch.nodeIds[i]
		scratch.nodeMembers[scratch.memberFill[node]] = uint32(i)
		scratch.memberFill[node] += 1
	}
	if ctx.Err() != nil {
		return nil
	}

	// remap the pairs onto node pairs. without a name merge the node ids are the
	// identity, so the coalesced entity pairs already are the node pairs
	if merged {
		remapped := scratch.pairs[:0]
		for _, pair := range scratch.pairs {
			a := scratch.nodeIds[uint32(pair.key>>32)]
			b := scratch.nodeIds[uint32(pair.key)]
			if a == b {
				continue
			}
			remapped = append(remapped, ipAssocAggPair{
				key:   ipAssocPackNodePair(uint32(a), uint32(b)),
				count: pair.count,
			})
		}
		scratch.pairs = coalesceIpAssocPairs(remapped)
	}
	if ctx.Err() != nil {
		return nil
	}

	// connected components via union-find over the node pairs, discovered before any
	// adjacency is built: a degenerate component (a burst clique beyond
	// maxComponentNodes) is then skipped without paying for its edges at all
	scratch.compParent = growUint32(scratch.compParent, nodeCount)
	for node := range nodeCount {
		scratch.compParent[node] = uint32(node)
	}
	compFind := func(i uint32) uint32 {
		for scratch.compParent[i] != i {
			scratch.compParent[i] = scratch.compParent[scratch.compParent[i]]
			i = scratch.compParent[i]
		}
		return i
	}
	pairNodes := func(pair ipAssocAggPair) (uint32, uint32, bool) {
		a := uint32(pair.key >> 32)
		b := uint32(pair.key)
		return a, b, 0 < pair.count && 0 < min(scratch.nodeCounts[a], scratch.nodeCounts[b])
	}
	for _, pair := range scratch.pairs {
		if a, b, ok := pairNodes(pair); ok {
			ra := compFind(a)
			rb := compFind(b)
			if ra != rb {
				scratch.compParent[rb] = ra
			}
		}
	}
	scratch.compSizes = growInt32(scratch.compSizes, nodeCount)
	clear(scratch.compSizes)
	for node := range nodeCount {
		scratch.compSizes[compFind(uint32(node))] += 1
	}
	keepNode := func(node uint32) bool {
		size := int(scratch.compSizes[compFind(node)])
		return 2 <= size && size <= maxComponentNodes
	}
	if ctx.Err() != nil {
		return nil
	}

	// association edges as csr adjacency (node -> (neighbor, p)), kept components only
	scratch.degrees = growInt32(scratch.degrees, nodeCount)
	clear(scratch.degrees)
	edgeCount := 0
	for _, pair := range scratch.pairs {
		if a, b, ok := pairNodes(pair); ok && keepNode(a) {
			scratch.degrees[a] += 1
			scratch.degrees[b] += 1
			edgeCount += 1
		}
	}
	scratch.adjInit = growInt32(scratch.adjInit, nodeCount+1)
	scratch.adjInit[0] = 0
	for node := range nodeCount {
		scratch.adjInit[node+1] = scratch.adjInit[node] + scratch.degrees[node]
	}
	scratch.adjNodes = growUint32(scratch.adjNodes, 2*edgeCount)
	scratch.adjPs = growFloat64(scratch.adjPs, 2*edgeCount)
	adjFill := scratch.degrees
	clear(adjFill)
	for _, pair := range scratch.pairs {
		a, b, ok := pairNodes(pair)
		if !ok || !keepNode(a) {
			continue
		}
		// coalesced counts can overlap; cap at 1
		p := min(float64(pair.count)/float64(min(scratch.nodeCounts[a], scratch.nodeCounts[b])), 1.0)
		ai := scratch.adjInit[a] + adjFill[a]
		scratch.adjNodes[ai] = b
		scratch.adjPs[ai] = p
		adjFill[a] += 1
		bi := scratch.adjInit[b] + adjFill[b]
		scratch.adjNodes[bi] = a
		scratch.adjPs[bi] = p
		adjFill[b] += 1
	}
	if ctx.Err() != nil {
		return nil
	}

	emit := func(clusterNodes []uint32) {
		clusterAddrs := []netip.Addr{}
		for _, node := range clusterNodes {
			for m := scratch.nodeMemberInit[node]; m < scratch.nodeMemberInit[node+1]; m += 1 {
				clusterAddrs = append(clusterAddrs, scratch.addrs[scratch.nodeMembers[m]])
			}
		}
		for _, addr := range clusterAddrs {
			members[addr] = clusterAddrs
		}
	}

	// gather the kept components (csr by dense component id)
	scratch.compIds = growInt32(scratch.compIds, nodeCount)
	for node := range nodeCount {
		scratch.compIds[node] = -1
	}
	compCount := 0
	for node := range nodeCount {
		if !keepNode(uint32(node)) {
			continue
		}
		root := compFind(uint32(node))
		if scratch.compIds[root] < 0 {
			scratch.compIds[root] = int32(compCount)
			compCount += 1
		}
		scratch.compIds[node] = scratch.compIds[root]
	}
	scratch.compInit = growInt32(scratch.compInit, compCount+1)
	clear(scratch.compInit)
	for node := range nodeCount {
		if 0 <= scratch.compIds[node] {
			scratch.compInit[scratch.compIds[node]+1] += 1
		}
	}
	for comp := range compCount {
		scratch.compInit[comp+1] += scratch.compInit[comp]
	}
	scratch.compNodes = growUint32(scratch.compNodes, int(scratch.compInit[compCount]))
	scratch.memberFill = growInt32(scratch.memberFill, compCount)
	copy(scratch.memberFill, scratch.compInit[:compCount])
	for node := range nodeCount {
		if comp := scratch.compIds[node]; 0 <= comp {
			scratch.compNodes[scratch.memberFill[comp]] = uint32(node)
			scratch.memberFill[comp] += 1
		}
	}

	// greedy split within each component, re-clustering the split-off pool
	for comp := range compCount {
		if ctx.Err() != nil {
			return nil
		}
		pool := scratch.compNodes[scratch.compInit[comp]:scratch.compInit[comp+1]]
		for 2 <= len(pool) {
			cluster, rest := splitIpAssocCluster(scratch, pool, minMeanAssociation)
			if 2 <= len(cluster) {
				emit(cluster)
			}
			if len(rest) == len(pool) {
				// no progress
				break
			}
			pool = rest
		}
	}

	// base-name merged nodes are a cluster on their own if not already clustered
	for node := range nodeCount {
		first := scratch.nodeMemberInit[node]
		end := scratch.nodeMemberInit[node+1]
		if 2 <= end-first {
			if _, ok := members[scratch.addrs[scratch.nodeMembers[first]]]; !ok {
				emit([]uint32{uint32(node)})
			}
		}
	}

	return members
}

// splitIpAssocCluster removes the member with the lowest mean association until all
// members have mean association >= minMeanAssociation, returning the surviving
// cluster and removed members in removal order. It partitions pool in place and
// uses node-indexed reusable scratch so the bounded component split does not
// allocate or add map work to every clustering epoch.
func splitIpAssocCluster(
	scratch *ipAssocScratch,
	pool []uint32,
	minMeanAssociation float64,
) (cluster []uint32, rest []uint32) {
	scratch.splitInCluster = growBool(scratch.splitInCluster, len(scratch.nodeCounts))
	scratch.splitSums = growFloat64(scratch.splitSums, len(scratch.nodeCounts))
	for _, node := range pool {
		scratch.splitInCluster[node] = true
		scratch.splitSums[node] = 0
	}
	// sum of associations to other members
	for _, node := range pool {
		for ai := scratch.adjInit[node]; ai < scratch.adjInit[node+1]; ai += 1 {
			if scratch.splitInCluster[scratch.adjNodes[ai]] {
				scratch.splitSums[node] += scratch.adjPs[ai]
			}
		}
	}

	activeCount := len(pool)
	for 2 <= activeCount {
		minIndex := 0
		minMean := 0.0
		for i, node := range pool[:activeCount] {
			mean := scratch.splitSums[node] / float64(activeCount-1)
			if i == 0 || mean < minMean {
				minIndex = i
				minMean = mean
			}
		}
		if minMeanAssociation <= minMean {
			// all members satisfy the threshold
			slices.Reverse(pool[activeCount:])
			for _, node := range pool[:activeCount] {
				scratch.splitInCluster[node] = false
			}
			return pool[:activeCount], pool[activeCount:]
		}
		// split off the weakest member
		minNode := pool[minIndex]
		scratch.splitInCluster[minNode] = false
		for ai := scratch.adjInit[minNode]; ai < scratch.adjInit[minNode+1]; ai += 1 {
			neighbor := scratch.adjNodes[ai]
			if scratch.splitInCluster[neighbor] {
				scratch.splitSums[neighbor] -= scratch.adjPs[ai]
			}
		}
		copy(pool[minIndex:activeCount-1], pool[minIndex+1:activeCount])
		activeCount -= 1
		pool[activeCount] = minNode
	}
	scratch.splitInCluster[pool[0]] = false
	slices.Reverse(pool)
	return nil, pool
}

// ipAssocClustersEqual compares two cluster snapshots. cluster member slices are
// shared among their members, so each distinct cluster pairing is verified once
// (set compare) and every further member key checks by backing-array identity —
// O(keys + total members) instead of a set per key.
func ipAssocClustersEqual(a map[netip.Addr][]netip.Addr, b map[netip.Addr][]netip.Addr) bool {
	if len(a) != len(b) {
		return false
	}
	matched := map[*netip.Addr]*netip.Addr{}
	for addr, aMembers := range a {
		bMembers, ok := b[addr]
		if !ok || len(aMembers) != len(bMembers) {
			return false
		}
		aFirst := &aMembers[0]
		bFirst := &bMembers[0]
		if prev, ok := matched[aFirst]; ok {
			if prev != bFirst {
				return false
			}
			continue
		}
		aSet := make(map[netip.Addr]bool, len(aMembers))
		for _, member := range aMembers {
			aSet[member] = true
		}
		for _, member := range bMembers {
			if !aSet[member] {
				return false
			}
		}
		matched[aFirst] = bFirst
	}
	return true
}

// ClusterVersion changes whenever the clustering output changes,
// so callers can cache cluster-derived decisions
func (self *IpAssoc) ClusterVersion() uint64 {
	return self.clusters.Load().version
}

// this is consulted when applying a routing rule that needs to match against an entire cluster not just a single path
func (self *IpAssoc) GetCluster(path *IpPath) []*IpPath {
	addr, ok := ipAssocAddr(path.DestinationIp)
	if !ok {
		return nil
	}
	members := self.clusters.Load().members[addr]
	if len(members) == 0 {
		return nil
	}
	memberPaths := make([]*IpPath, 0, len(members))
	for _, member := range members {
		version := 4
		if member.Is6() {
			version = 6
		}
		memberPaths = append(memberPaths, &IpPath{
			Version:       version,
			DestinationIp: member.AsSlice(),
		})
	}
	return memberPaths
}

// GetClusterAddrs returns the cluster member ips for an entity ip,
// or nil if the entity is not in a multi-member cluster
func (self *IpAssoc) GetClusterAddrs(addr netip.Addr) []netip.Addr {
	return self.clusters.Load().members[addr.Unmap()]
}

func (self *IpAssoc) Close() {
	self.cancel()
}
