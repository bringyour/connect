package connect

// ip_remote_multi_client_family.go — the address-family half of the window
// (IPV6.md B1–B6): each exit's effective category with the local v6
// downgrade, the versioned dial-strike record the downgrade is judged on, the
// soft v6-capable minimum's shortfall and starvation state, the family-aware
// discovery order, the admission preference, the capacity swap, and the v6
// no-route reply.
//
// Threading: an exit's category is read on the send hot path, so the two
// per-exit flags are atomics. The window's shortfall state is guarded by the
// window stateLock. The versioned strike record extends the channel's
// stateLock-guarded strike slices entry for entry. Nothing here takes the
// parent stateLock except through the injected flow-count seam, which is
// only reached from the resize pass with no lock held.

import (
	"slices"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// defaultIpFamilyStarvedRetryTimeout is how long the enumerator waits before
// asking for a family again after the platform returned nothing for it. Long
// enough that a region with no v6-capable provider is not polled on every
// resize pass, short enough that a provider coming online is picked up
// within a minute.
const defaultIpFamilyStarvedRetryTimeout = 60 * time.Second

// enumeratedDestination is one discovery result in the order the enumerator
// hands candidates to expand: v6-capable results first while a v6 shortfall
// is open, so the expand pass evaluates them before the main fill.
type enumeratedDestination struct {
	destination MultiHopId
	stats       DestinationStats
}

// --- exit category ---

// IpFamily is this exit's effective category: the discovered category, or
// v4-only once the local v6 downgrade fired (maybeDowngradeIpv6). A bare
// fixture without args reads as legacy, which carries v4 only.
func (self *multiClientChannel) IpFamily() IpFamily {
	if self.ipFamilyDowngraded.Load() {
		return IpFamilyV4Only
	}
	if self.args == nil {
		return IpFamilyLegacy
	}
	return self.args.DestinationStats.IpFamily.Normalize()
}

// supportsIpVersion is the placement predicate: whether this exit can carry
// a flow of the packet's ip version. 0 means "any", for callers that gather
// exits without a flow in hand. A fixed-destination exit carries every
// version: it is the user's choice, with no alternative to prefer (see
// multiClientChannelArgs.FixedDestination).
func (self *multiClientChannel) supportsIpVersion(ipVersion int) bool {
	if ipVersion == 0 {
		return true
	}
	if self.args != nil && self.args.FixedDestination {
		return true
	}
	return self.IpFamily().SupportsIpVersion(ipVersion)
}

// maybeDowngradeIpv6 is B5: when this exit's v6 dials are starved while its
// v4 stays healthy, its category drops to v4-only for the life of the
// window. Returns true exactly once, on the transition, so the caller can
// publish it. An exit whose category never carried v6 has nothing to lose.
func (self *multiClientChannel) maybeDowngradeIpv6() bool {
	// only a dualstack exit has a v4 side to fall back on: a v6-only exit
	// whose v6 is starved is simply starved, and the ordinary warning is the
	// right answer
	if self.IpFamily() != IpFamilyDualstack {
		return false
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if !self.ipv6DialStarvedWithLock(time.Now()) {
		return false
	}
	return self.ipFamilyDowngraded.CompareAndSwap(false, true)
}

// dialStrikeVersion is the recorded ip version of strike i. A legacy
// fixture that injects dialFailureTimes alone has no versions; its strikes
// read as v4, which keeps every pre-existing starvation test exact.
func dialStrikeVersion(versions []int, i int) int {
	if i < len(versions) && versions[i] == 6 {
		return 6
	}
	return 4
}

// ipv6DialStarvedWithLock is dialStarvedWithLock restricted to v6, plus the
// "v4 stays healthy" half of B5: v6 has at least dialStarvedFailureThreshold
// strikes across dialStarvedMinDestinations distinct destinations and no
// proven v6 connect, while v4 has fewer strikes than the threshold or a
// proven connect. Without the v4 half a provider whose whole upstream is
// failing would be misread as v4-only instead of starved, and the ordinary
// starvation warning is the right answer there.
//
// must be called with stateLock
func (self *multiClientChannel) ipv6DialStarvedWithLock(now time.Time) bool {
	horizon := now.Add(-dialStrikeWindow)
	self.pruneDialStrikesWithLock(horizon)
	self.pruneConnectSuccessesWithLock(horizon)

	v4Fails, v6Fails := 0, 0
	v6Destinations := map[string]bool{}
	for i := range self.dialFailureTimes {
		if dialStrikeVersion(self.dialFailureVersions, i) == 6 {
			v6Fails += 1
			if i < len(self.dialFailureDestinations) {
				v6Destinations[self.dialFailureDestinations[i]] = true
			}
		} else {
			v4Fails += 1
		}
	}
	if v6Fails < dialStarvedFailureThreshold || len(v6Destinations) < dialStarvedMinDestinations {
		return false
	}
	v4Success, v6Success := 0, 0
	for i := range self.connectSuccessTimes {
		if dialStrikeVersion(self.connectSuccessVersions, i) == 6 {
			v6Success += 1
		} else {
			v4Success += 1
		}
	}
	if 0 < v6Success {
		return false
	}
	return v4Fails < dialStarvedFailureThreshold || 0 < v4Success
}

// pruneConnectSuccessesWithLock prunes the proven-connect record to the
// strike window, cutting the same prefix from the parallel versions. The
// version cut is clamped, like pruneDialStrikesWithLock, for fixtures that
// inject the times alone.
//
// must be called with stateLock
func (self *multiClientChannel) pruneConnectSuccessesWithLock(horizon time.Time) {
	i := 0
	for i < len(self.connectSuccessTimes) && self.connectSuccessTimes[i].Before(horizon) {
		i += 1
	}
	if 0 < i {
		self.connectSuccessTimes = self.connectSuccessTimes[i:]
		self.connectSuccessVersions = self.connectSuccessVersions[min(i, len(self.connectSuccessVersions)):]
	}
}

// isFamilySwapVictim reports whether the resize pass chose this exit to give
// up its slot to a v6-capable candidate (see selectFamilySwapVictim). The
// mark persists across passes so the warning it causes is not undone by the
// next classification; it is cleared when the v6 fill starves.
func (self *multiClientChannel) isFamilySwapVictim() bool {
	return self.familySwapVictim.Load()
}

func (self *multiClientChannel) markFamilySwapVictim() {
	self.familySwapVictim.Store(true)
}

func (self *multiClientChannel) clearFamilySwapVictim() {
	self.familySwapVictim.Store(false)
}

// --- window shortfall state ---

// ipv6CapableShortfall is the soft v6-capable minimum's open shortfall for
// one resize pass (IPV6.md B1): how many v6-capable exits the window wants
// and holds none of. Zero when the minimum does not apply -- a fixed
// performance profile, a fixed window size or a fixed destination pins the
// membership and a soft minimum has no slot to claim -- and zero while the
// window is still forming (no healthy exit yet): during formation the
// dualstack-first main fill is the way to get a v6-capable exit, so nothing
// extra is asked for. The shortfall is the whole minimum, not the
// difference, because it is only open while the count is zero: one
// v6-capable exit satisfies the minimum however large it is set, which is
// what keeps the extra request to one candidate.
func ipv6CapableShortfall(
	windowSize WindowSizeSettings,
	fixedProfile bool,
	fixedDestination bool,
	clientCount int,
	ipv6CapableCount int,
) int {
	if fixedProfile || 0 < windowSize.FixedWindowSize || fixedDestination {
		return 0
	}
	if windowSize.WindowSizeMinIpv6Capable <= 0 || clientCount <= 0 || 0 < ipv6CapableCount {
		return 0
	}
	return windowSize.WindowSizeMinIpv6Capable
}

// setIpv6Shortfall records the v6-capable shortfall the resize pass computed:
// how many v6-capable exits the window wants and has none of. 0 closes it.
func (self *multiClientWindow) setIpv6Shortfall(shortfall int) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.ipv6Shortfall = max(0, shortfall)
}

// ipv6FillWanted is how many v6-capable candidates the enumerator should ask
// for ahead of the main fill: the open shortfall, unless the family is in
// its starvation backoff.
func (self *multiClientWindow) ipv6FillWanted(now time.Time) int {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.ipv6Shortfall <= 0 || now.Before(self.ipv6StarvedUntil) {
		return 0
	}
	return self.ipv6Shortfall
}

// ipv6Starved reports whether the v6-capable fill is in its retry backoff.
func (self *multiClientWindow) ipv6Starved(now time.Time) bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return now.Before(self.ipv6StarvedUntil)
}

// noteIpv6Starved records that the platform returned nothing v6-capable:
// the family is re-polled only after IpFamilyStarvedRetryTimeout, and any
// exit warned as a swap victim on the strength of a fill that is not coming
// is released back to selection.
func (self *multiClientWindow) noteIpv6Starved(now time.Time) {
	timeout := self.settings.IpFamilyStarvedRetryTimeout
	if timeout <= 0 {
		timeout = defaultIpFamilyStarvedRetryTimeout
	}
	released := 0
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		self.ipv6StarvedUntil = now.Add(timeout)
		for _, client := range self.clients {
			if client.isFamilySwapVictim() {
				client.clearFamilySwapVictim()
				released += 1
			}
		}
	}()
	loggerOrDefault(self.log).Infof("%s\n", relEvent(
		"family_starved",
		"window", self.windowType.RankMode(),
		"family", string(IpFamilyFilterV6Capable),
		"retry", timeout,
		"released", released,
	))
}

// requestIpv6Probe asks the enumerator for one probe-only round: the
// v6-capable request alone, so the platform is consulted BEFORE the window
// gives an exit up for the candidate (see selectFamilySwapVictim). Wakes the
// enumerator.
func (self *multiClientWindow) requestIpv6Probe() {
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		self.ipv6ProbeRequested = true
	}()
	self.generatorMonitor.NotifyAll()
}

// takeIpv6ProbeRequest consumes a pending probe request.
func (self *multiClientWindow) takeIpv6ProbeRequest() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	requested := self.ipv6ProbeRequested
	self.ipv6ProbeRequested = false
	return requested
}

// setIpv6CandidateReady marks whether a probe's v6-capable candidate is
// minted and waiting for expand to consume it.
func (self *multiClientWindow) setIpv6CandidateReady(ready bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.ipv6CandidateReady = ready
}

func (self *multiClientWindow) hasIpv6CandidateReady() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.ipv6CandidateReady
}

// ipv6FillFailureStarveCount is how many consecutive failed v6-capable
// evaluations starve the family for a retry period.
const ipv6FillFailureStarveCount = 2

// noteIpv6CandidateFailed counts a v6-capable candidate that failed its
// evaluation while the shortfall was open, and starves the family once the
// count reaches ipv6FillFailureStarveCount: a candidate the platform keeps
// offering and that keeps failing must not cost an exit per pass.
func (self *multiClientWindow) noteIpv6CandidateFailed() {
	starve := false
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		self.ipv6FillFailures += 1
		if ipv6FillFailureStarveCount <= self.ipv6FillFailures {
			self.ipv6FillFailures = 0
			starve = true
		}
	}()
	if starve {
		self.noteIpv6Starved(time.Now())
	}
}

// noteIpv6CandidateAdmitted resets the failure count on a successful fill.
func (self *multiClientWindow) noteIpv6CandidateAdmitted() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.ipv6FillFailures = 0
}

// ipv6Available reports whether this window holds an added, unwarned exit
// that can carry v6 (B6). Read live from the client set rather than from the
// last resize snapshot, so the flag follows an admission or a downgrade
// without waiting for the next pass.
func (self *multiClientWindow) ipv6Available() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	for _, client := range self.clients {
		if client == nil || client.IsDone() || client.isWarning() {
			continue
		}
		if client.supportsIpVersion(6) {
			return true
		}
	}
	return false
}

// hasAddedExit reports whether this window holds any added, unwarned exit
// of any family: the window has formed.
func (self *multiClientWindow) hasAddedExit() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	for _, client := range self.clients {
		if client != nil && !client.IsDone() && !client.isWarning() {
			return true
		}
	}
	return false
}

// hardMinimumClientCount is the live count the hard minimum is judged
// against (see hardMinimumCount for the rule): every unwarned exit, as long
// as at least one of them can carry v4 or the destination is fixed.
func (self *multiClientWindow) hardMinimumClientCount(fixedDestination bool) int {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	clients := []*multiClientChannel{}
	for _, client := range self.clients {
		if client == nil || client.IsDone() || client.isWarning() {
			continue
		}
		clients = append(clients, client)
	}
	return hardMinimumCount(clients, fixedDestination)
}

// hardMinimumCount is the rule the hard minimum is judged by (IPV6.md B1):
// every unwarned exit counts, as long as at least one of them can carry v4
// -- a window whose only exits are v6-only is not connected for the traffic
// that matters, so it counts as empty. A fixed destination counts whatever
// it holds: the user chose that peer whatever its family. A v6-only exit
// beside v4-capable ones counts, since it occupies a slot the soft minimum
// deliberately gave it.
func hardMinimumCount(clients []*multiClientChannel, fixedDestination bool) int {
	if fixedDestination {
		return len(clients)
	}
	for _, client := range clients {
		if client.IpFamily().SupportsIpv4() {
			return len(clients)
		}
	}
	return 0
}

// hasFamilySwapVictim reports whether an exit is already marked to give up
// its slot, so a pass never marks a second one while the first is draining.
func (self *multiClientWindow) hasFamilySwapVictim() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	for _, client := range self.clients {
		if client != nil && client.isFamilySwapVictim() {
			return true
		}
	}
	return false
}

// selectFamilySwapVictim picks the exit that gives up its slot to a
// v6-capable candidate when the window is at capacity (B2's "v4-only exits
// only top up when dualstack runs out", applied after the fact): among the
// healthy exits that cannot carry v6, a flowless one first -- it can be
// removed at once -- else the one carrying the least traffic, which is
// drain-warned. Lowest weight wins within each class. nil when none
// qualifies. Must be called with no lock held: the flow count takes the
// parent stateLock inside.
func (self *multiClientWindow) selectFamilySwapVictim(
	clients []*multiClientChannel,
	weights map[*multiClientChannel]float32,
) (victim *multiClientChannel, flowless bool) {
	var loaded *multiClientChannel
	for _, client := range clients {
		if client.supportsIpVersion(6) {
			continue
		}
		if self.flowCount(client) == 0 {
			if victim == nil || weights[client] < weights[victim] {
				victim = client
			}
			continue
		}
		if loaded == nil || weights[client] < weights[loaded] {
			loaded = client
		}
	}
	if victim != nil {
		return victim, true
	}
	return loaded, false
}

// --- discovery order ---

// enumerateDestinations is one discovery round in push order. With a
// family-aware generator the round is: the v6-capable request first while a
// v6 shortfall is open and not starved (sized to the shortfall), then the
// main v4-capable fill, which the platform draws dualstack-first. Duplicates
// across the two are dropped. probeOnly is the resize pass's probe
// (requestIpv6Probe): the v6-capable request alone, nothing when no shortfall
// is open. A generator without the capability is called exactly as before
// and its destinations read as legacy. Every call carries the generator
// deadline, so a hung platform surfaces as an error on the caller's ordinary
// retry cadence.
func (self *multiClientWindow) enumerateDestinations(excludeDestinations []MultiHopId, probeOnly bool) ([]enumeratedDestination, error) {
	call := func(f func() (map[MultiHopId]DestinationStats, error)) (map[MultiHopId]DestinationStats, error) {
		return windowGeneratorCall(
			self.ctx,
			self.settings.WindowGeneratorTimeout,
			f,
			nil,
		)
	}
	ordered := []enumeratedDestination{}
	seen := map[MultiHopId]bool{}
	appendAll := func(destinations map[MultiHopId]DestinationStats) {
		for destination, stats := range destinations {
			if seen[destination] {
				continue
			}
			seen[destination] = true
			ordered = append(ordered, enumeratedDestination{
				destination: destination,
				stats:       stats,
			})
		}
	}
	rankMode := self.windowType.RankMode()

	familyGenerator, familyOk := self.generator.(MultiClientGeneratorWithIpFamily)
	if !familyOk {
		if probeOnly {
			// a legacy generator has no family to ask for
			return ordered, nil
		}
		destinations, err := call(func() (map[MultiHopId]DestinationStats, error) {
			return self.generator.NextDestinations(
				self.settings.WindowExpandBlockCount,
				excludeDestinations,
				rankMode,
			)
		})
		if err != nil {
			return nil, err
		}
		appendAll(destinations)
		return ordered, nil
	}

	now := time.Now()
	if wanted := self.ipv6FillWanted(now); 0 < wanted {
		destinations, err := call(func() (map[MultiHopId]DestinationStats, error) {
			return familyGenerator.NextDestinationsWithIpFamily(
				wanted,
				excludeDestinations,
				rankMode,
				IpFamilyFilterV6Capable,
			)
		})
		if err != nil {
			return nil, err
		}
		if len(destinations) == 0 {
			self.noteIpv6Starved(now)
		} else {
			appendAll(destinations)
		}
	}
	if probeOnly {
		return ordered, nil
	}
	destinations, err := call(func() (map[MultiHopId]DestinationStats, error) {
		return familyGenerator.NextDestinationsWithIpFamily(
			self.settings.WindowExpandBlockCount,
			excludeDestinations,
			rankMode,
			IpFamilyFilterV4Capable,
		)
	})
	if err != nil {
		return nil, err
	}
	appendAll(destinations)
	return ordered, nil
}

// --- admission ---

// poolAdmitOrderWithShortfall is poolAdmitOrder with one more rank above
// qualification: candidates that close the window's open family shortfall
// come first, then qualified candidates, then the rest in arrival order.
// With no shortfall it is exactly poolAdmitOrder.
func poolAdmitOrderWithShortfall(closesShortfall []bool, qualified []bool, count int) []int {
	if count <= 0 || len(qualified) == 0 {
		return nil
	}
	order := []int{}
	taken := make([]bool, len(qualified))
	pass := func(want func(i int) bool) {
		for i := range qualified {
			if !taken[i] && want(i) {
				taken[i] = true
				order = append(order, i)
			}
		}
	}
	pass(func(i int) bool { return i < len(closesShortfall) && closesShortfall[i] })
	pass(func(i int) bool { return qualified[i] })
	pass(func(i int) bool { return true })
	if count < len(order) {
		order = order[:count]
	}
	return order
}

// collapseFamilyOrder is the family key of the capacity collapse: dualstack
// exits sort first (kept), single-family exits last (shed first). 0 when the
// two are the same class.
func collapseFamilyOrder(a *multiClientChannel, b *multiClientChannel) int {
	aDualstack := a.IpFamily() == IpFamilyDualstack
	bDualstack := b.IpFamily() == IpFamilyDualstack
	switch {
	case aDualstack == bDualstack:
		return 0
	case aDualstack:
		return -1
	default:
		return 1
	}
}

// --- parent ---

// Ipv6Available reports whether any window holds an added, unwarned exit
// that can carry v6 (B6). This is the raw flag the window status carries.
func (self *RemoteUserNatMultiClient) Ipv6Available() bool {
	for _, window := range self.windows {
		if window.ipv6Available() {
			return true
		}
	}
	return false
}

// Ipv6Unroutable is the B6 verdict: the windows have formed -- some added,
// unwarned exit exists -- and none of them can carry v6. While true a v6
// flow with no candidate is answered with no-route and the in-tunnel
// resolver should answer AAAA empty, so applications fall back to v4
// instead of blackholing. It is deliberately false while the windows are
// still forming, when the dualstack-first fill may land a v6-capable exit
// at any moment.
func (self *RemoteUserNatMultiClient) Ipv6Unroutable() bool {
	formed := false
	for _, window := range self.windows {
		if window.hasAddedExit() {
			formed = true
			break
		}
	}
	return formed && !self.Ipv6Available()
}

// replyIpv6NoRoute answers a v6 flow that has no v6-capable exit anywhere
// with an icmpv6 destination-unreachable as if from the destination, so the
// application fails fast (and its own happy-eyeballs falls back to v4)
// instead of hanging until its connect timeout. Delivered on the receive path
// the provider dial-failure signal uses. Returns whether a reply went out.
func (self *RemoteUserNatMultiClient) replyIpv6NoRoute(ipPath *IpPath) bool {
	if ipPath == nil || ipPath.Version != 6 {
		return false
	}
	packet, ok := ipOosUnreachable(ipPath)
	if !ok {
		return false
	}
	self.ipv6NoRouteCount.Add(1)
	loggerOrDefault(self.log).V(1).Infof("[multi]ipv6 no route %s: no v6-capable exit\n", ipPath.DestinationHostPort())
	self.deliverReceivePacket(TransferPath{}, protocol.ProvideMode_Network, ipPath, packet)
	return true
}

// Ipv6NoRouteCount is how many v6 flows were answered with no-route because
// no v6-capable exit existed, for diagnostics.
func (self *RemoteUserNatMultiClient) Ipv6NoRouteCount() uint64 {
	return self.ipv6NoRouteCount.Load()
}

// noteIpFamilyDowngrade publishes an exit's local v6 downgrade (B5): the
// monitor event's category changes in place -- the exit stays Added, its
// connected-since time untouched -- and the owning window's resize pass is
// woken so the v6 shortfall the downgrade may have opened is filled now.
func (self *RemoteUserNatMultiClient) noteIpFamilyDowngrade(client *multiClientChannel) {
	loggerOrDefault(self.log).Infof("%s\n", relEvent(
		"family_downgrade",
		"exit", client.ClientId(),
		"to", string(IpFamilyV4Only),
		"dialfails", client.dialFailureCount(),
	))
	for _, window := range self.windows {
		if window.noteClientIpFamily(client) {
			return
		}
	}
}

// noteClientIpFamily republishes the client's category on this window's
// monitor when the client is one of this window's exits, and wakes resize.
// Returns whether the client belonged to this window.
func (self *multiClientWindow) noteClientIpFamily(client *multiClientChannel) bool {
	owned := false
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		owned = self.clients[client.ClientId()] == client
	}()
	if !owned {
		return false
	}
	self.monitor.SetProviderIpFamily(client.ClientId(), client.IpFamily())
	self.resizeMonitor.NotifyAll()
	return true
}

// candidatesForIpVersion narrows a candidate list to exits that can carry
// the version, preserving order. 0 returns the list unchanged.
func candidatesForIpVersion(clients []*multiClientChannel, ipVersion int) []*multiClientChannel {
	if ipVersion == 0 {
		return clients
	}
	return slices.DeleteFunc(slices.Clone(clients), func(client *multiClientChannel) bool {
		return !client.supportsIpVersion(ipVersion)
	})
}
