package connect

import (
	"net/netip"
	"strings"
	"time"
)

const (
	// A one-second bucket is long enough to reject ACK-compression spikes while
	// still learning from a short rejected TLS connection. The final partial
	// bucket uses a 250 ms floor for the same reason.
	tcpAckPerformanceWindow        = time.Second
	tcpAckPerformancePartialFloor  = 250 * time.Millisecond
	affinityPerformancePriorWeight = 1.0 // seconds of advertised-rate evidence

	// Keep this learner materially smaller than the live-flow tables. Entries
	// are session-local and short-lived: anti-bias is a fresh retry hint, not a
	// durable provider verdict.
	affinityPerformanceMaxEntries = 128
	affinityPerformanceTTL        = 10 * time.Minute
	affinityPerformanceMaxWeight  = 30.0 // seconds of combined evidence
	affinityPerformanceMaxPart    = 10.0 // seconds per time/byte component
	affinityPerformanceTimeGrain  = 100 * time.Millisecond

	// DNS association normally yields one canonical name plus one exact IP.
	// Keep ordinary placement entirely on the stack; an unusually shared
	// address can still grow safely through append.
	affinityPerformanceDestinationStack = 8
	// Production quality windows are capped at six exits. Keep ample room for
	// overrides while retaining all candidate scores on the placement stack;
	// an unexpectedly wider field fails open instead of allocating.
	affinityPerformanceCandidateStack = 16
	// A posterior is a routing hint, not proof that one public exit will keep
	// working for the next origin connection. Preserve a small simultaneous
	// escape set when measurements would otherwise collapse a full quality race
	// to one candidate. Three alternatives retain rough-pool tail insurance
	// without returning to the six-way fresh-flow fan-out that exceeded the
	// mobile memory budget.
	affinityPerformanceMinRaceCandidates = 4
)

// tcpAckPerformance measures useful return progress without looking inside
// TLS. The source's cumulative TCP ACK number advances only after bytes from
// the origin have reached the browser. It is therefore the closest available
// TLS-blind signal for whether a media connection actually moved data.
//
// All fields are guarded by multiClientChannelUpdate.stateLock.
type tcpAckPerformance struct {
	sequenceSeen bool
	sequence     uint32

	totalBytes uint64
	peakRate   float64

	bucketStart  time.Time
	bucketBytes  uint64
	lastProgress time.Time
}

func (self *tcpAckPerformance) reset() {
	*self = tcpAckPerformance{}
}

func bytesPerSecond(byteCount uint64, duration time.Duration) float64 {
	if byteCount == 0 || duration <= 0 {
		return 0
	}
	return float64(byteCount) / duration.Seconds()
}

func (self *tcpAckPerformance) finishBucket(duration time.Duration) {
	if duration < tcpAckPerformancePartialFloor {
		duration = tcpAckPerformancePartialFloor
	}
	self.peakRate = max(self.peakRate, bytesPerSecond(self.bucketBytes, duration))
}

func (self *tcpAckPerformance) needsTimestamp(ack bool, sequence uint32) bool {
	return ack && self.sequenceSeen && 0 < int32(sequence-self.sequence)
}

func (self *tcpAckPerformance) observe(now time.Time, ack bool, sequence uint32) {
	if !ack {
		return
	}
	if !self.sequenceSeen {
		// The first ACK contains the origin's random initial sequence number.
		// It is a baseline, never a byte count.
		self.sequenceSeen = true
		self.sequence = sequence
		return
	}

	delta := int32(sequence - self.sequence)
	if delta <= 0 {
		// Duplicate/reordered ACK, or a regression. Do not move the baseline
		// backward and count the same bytes twice.
		return
	}
	self.sequence = sequence

	if self.bucketStart.IsZero() {
		self.bucketStart = now
	} else if elapsed := now.Sub(self.bucketStart); tcpAckPerformanceWindow <= elapsed {
		self.finishBucket(elapsed)
		self.bucketStart = now
		self.bucketBytes = 0
	}
	self.bucketBytes += uint64(delta)
	self.totalBytes += uint64(delta)
	self.lastProgress = now
}

func (self *tcpAckPerformance) snapshot() (peakRate float64, totalBytes uint64) {
	peakRate = self.peakRate
	if self.bucketBytes != 0 {
		duration := self.lastProgress.Sub(self.bucketStart)
		if duration < tcpAckPerformancePartialFloor {
			duration = tcpAckPerformancePartialFloor
		}
		peakRate = max(peakRate, bytesPerSecond(self.bucketBytes, duration))
	}
	return peakRate, self.totalBytes
}

type affinityPerformanceDestination struct {
	name string
	addr netip.Addr
}

type affinityPerformanceDestinationSet struct {
	values [affinityPerformanceDestinationStack]affinityPerformanceDestination
	count  int
}

func (self *affinityPerformanceDestinationSet) add(destination affinityPerformanceDestination) {
	if destination.name == "" && !destination.addr.IsValid() {
		return
	}
	for i := 0; i < self.count; i++ {
		if self.values[i] == destination {
			return
		}
	}
	if self.count < len(self.values) {
		self.values[self.count] = destination
		self.count++
	}
}

func (self *affinityPerformanceDestinationSet) slice() []affinityPerformanceDestination {
	return self.values[:self.count]
}

type affinityPerformanceKey struct {
	provider    Id
	destination affinityPerformanceDestination
}

type affinityPerformanceEntry struct {
	weightedPeakRate float64
	evidenceWeight   float64
	lastUpdate       time.Time
}

type affinityPerformanceCandidateScore struct {
	client   *multiClientChannel
	score    float64
	measured bool
}

func canonicalAffinityPerformanceName(name string) string {
	name = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(name), "."))
	if name == "" || strings.HasPrefix(name, "app:") {
		return ""
	}
	return affinityNameForServerName(name)
}

func canonicalAffinityPerformanceGroupName(name string) string {
	name = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(name), "."))
	if name == "" || strings.HasPrefix(name, "app:") {
		return ""
	}
	// update affinity keys have already passed affinityNameForServerName at
	// flow creation. Avoid repeating public-suffix parsing on fresh placement.
	return name
}

func (self *RemoteUserNatMultiClient) affinityPerformanceDestinationSet(
	ipPath *IpPath,
) (destinations affinityPerformanceDestinationSet) {
	if ipPath == nil || ipPath.Protocol != IpProtocolTcp || ipPath.DestinationPort != 443 {
		return
	}
	addr, ok := netIPAddr(ipPath.DestinationIp)
	if !ok {
		return
	}
	// Reserve the first slot for the exact endpoint. Up to seven canonical
	// names can supplement it without making unbounded DNS association state a
	// placement allocation vector.
	destinations.add(affinityPerformanceDestination{addr: addr.Unmap()})
	for _, serverName := range self.serverNames(addr) {
		if name := canonicalAffinityPerformanceName(serverName); name != "" {
			destinations.add(affinityPerformanceDestination{name: name})
		}
	}
	// Keep the exact endpoint as well as the canonical domain. The domain
	// generalizes evidence to a newly selected video; the address prevents a
	// fast page endpoint from hiding a repeatedly weak media endpoint in the
	// same constellation. Scoring deliberately takes the conservative minimum.
	return
}

func affinityPerformanceDestinationSetForUpdate(
	update *multiClientChannelUpdate,
) (destinations affinityPerformanceDestinationSet) {
	if update == nil || update.ipPath == nil || update.ipPath.Protocol != IpProtocolTcp ||
		update.ipPath.DestinationPort != 443 {
		return
	}
	if addr, ok := netIPAddr(update.ipPath.DestinationIp); ok {
		destinations.add(affinityPerformanceDestination{addr: addr.Unmap()})
	}
	for affinityPath := range update.affinityIp4Paths {
		if name := canonicalAffinityPerformanceGroupName(affinityPath.ServerName); name != "" {
			destinations.add(affinityPerformanceDestination{name: name})
		}
	}
	for affinityPath := range update.affinityIp6Paths {
		if name := canonicalAffinityPerformanceGroupName(affinityPath.ServerName); name != "" {
			destinations.add(affinityPerformanceDestination{name: name})
		}
	}
	return
}

func affinityPerformanceEvidenceWeight(
	priorRate float64,
	activeDuration time.Duration,
	ackedBytes uint64,
) float64 {
	// Scheduler-nanosecond differences are not routing evidence. Quantizing
	// lifetime keeps otherwise equal short connections at exactly equal weight.
	durationPart := min(
		max(activeDuration.Round(affinityPerformanceTimeGrain).Seconds(), 0.0),
		affinityPerformanceMaxPart,
	)
	bytePart := 0.0
	if 0 < priorRate {
		// Express acknowledged bytes as the time the advertised provider rate
		// would need to move them. Time and bytes therefore contribute in the
		// same unit and equal short histories receive equal evidence weight.
		bytePart = min(float64(ackedBytes)/priorRate, affinityPerformanceMaxPart)
	}
	return durationPart + bytePart
}

func affinityPerformancePosterior(priorRate float64, entry *affinityPerformanceEntry) float64 {
	if entry == nil || entry.evidenceWeight <= 0 {
		return priorRate
	}
	return (priorRate*affinityPerformancePriorWeight + entry.weightedPeakRate) /
		(affinityPerformancePriorWeight + entry.evidenceWeight)
}

// affinityPerformanceRecordEligible reports whether evidence still belongs to
// a route that could carry a fresh flow now. Flow retirement is asynchronous:
// by the time it reaches recordAffinityPerformance, the winning channel may
// already have lost every transport, entered warning/quarantine, or left its
// window. Recording that teardown interval as provider throughput poisons the
// next placement decision with a carrier failure that is no longer current.
//
// This check is deliberately on the cold flow-retirement path. It adds no work
// to ACK observation. A route outside every live window is ineligible even if
// its channel-local transport has not finished closing yet.
func (self *RemoteUserNatMultiClient) affinityPerformanceRecordEligible(
	client *multiClientChannel,
) bool {
	if client == nil || (client.ctx != nil && client.IsDone()) ||
		!client.hasActiveTransport() || client.isWarning() {
		return false
	}
	active := false
	for _, window := range self.windows {
		if window == nil {
			continue
		}
		window.stateLock.Lock()
		for _, candidate := range window.clients {
			if candidate == client {
				active = true
				break
			}
		}
		window.stateLock.Unlock()
		if active {
			break
		}
	}
	// Recheck channel-local state after the window scan. A concurrent removal
	// cannot make a stale route look eligible merely because it occupied a
	// window slot at the beginning of this cold-path check.
	return active && (client.ctx == nil || !client.IsDone()) &&
		client.hasActiveTransport() && !client.isWarning()
}

func (self *RemoteUserNatMultiClient) recordAffinityPerformance(
	update *multiClientChannelUpdate,
	client *multiClientChannel,
) {
	if update == nil || client == nil || !self.reliabilitySettings().PerformanceAwareAffinity ||
		!update.receivedInbound.Load() || !self.affinityPerformanceRecordEligible(client) {
		return
	}
	destinationSet := affinityPerformanceDestinationSetForUpdate(update)
	destinations := destinationSet.slice()
	if len(destinations) == 0 {
		return
	}

	update.stateLock.Lock()
	peakRate, ackedBytes := update.ackPerformance.snapshot()
	openTime := update.openTime
	update.stateLock.Unlock()
	activeDuration := update.activityTime.Sub(openTime)
	priorRate := float64(max(client.EstimatedByteCountPerSecond(), 0))
	evidenceWeight := affinityPerformanceEvidenceWeight(priorRate, activeDuration, ackedBytes)
	if evidenceWeight <= 0 {
		return
	}

	provider, ok := providerIdentity(client)
	if !ok {
		// Never retain a whole channel graph merely to key a small learner.
		// Production discovery channels have a stable provider identity; bare
		// fixtures and malformed channels simply remain at the null hypothesis.
		return
	}
	now := time.Now()
	self.affinityPerformanceLock.Lock()
	defer self.affinityPerformanceLock.Unlock()
	if self.affinityPerformance == nil {
		self.affinityPerformance = map[affinityPerformanceKey]*affinityPerformanceEntry{}
	}
	for key, entry := range self.affinityPerformance {
		if affinityPerformanceTTL <= now.Sub(entry.lastUpdate) {
			delete(self.affinityPerformance, key)
		}
	}
	for _, destination := range destinations {
		key := affinityPerformanceKey{provider: provider, destination: destination}
		entry := self.affinityPerformance[key]
		if entry == nil {
			if affinityPerformanceMaxEntries <= len(self.affinityPerformance) {
				var oldestKey affinityPerformanceKey
				var oldestTime time.Time
				for candidateKey, candidate := range self.affinityPerformance {
					if oldestTime.IsZero() || candidate.lastUpdate.Before(oldestTime) {
						oldestKey, oldestTime = candidateKey, candidate.lastUpdate
					}
				}
				delete(self.affinityPerformance, oldestKey)
			}
			entry = &affinityPerformanceEntry{}
			self.affinityPerformance[key] = entry
		}
		entry.weightedPeakRate += peakRate * evidenceWeight
		entry.evidenceWeight += evidenceWeight
		if affinityPerformanceMaxWeight < entry.evidenceWeight {
			scale := affinityPerformanceMaxWeight / entry.evidenceWeight
			entry.weightedPeakRate *= scale
			entry.evidenceWeight = affinityPerformanceMaxWeight
		}
		entry.lastUpdate = now
	}
	self.reliabilityMetrics.affinityPerformanceSample()
}

func (self *RemoteUserNatMultiClient) affinityPerformanceScore(
	client *multiClientChannel,
	ipPath *IpPath,
	now time.Time,
) (score float64, prior float64, measured bool) {
	destinationSet := self.affinityPerformanceDestinationSet(ipPath)
	destinations := destinationSet.slice()
	if len(destinations) == 0 {
		if client != nil && client.args != nil {
			prior = float64(max(client.args.EstimatedBytesPerSecond, 0))
		}
		return prior, prior, false
	}
	self.affinityPerformanceLock.Lock()
	defer self.affinityPerformanceLock.Unlock()
	return self.affinityPerformanceScoreWithLock(client, destinations, now)
}

// called with affinityPerformanceLock
func (self *RemoteUserNatMultiClient) affinityPerformanceScoreWithLock(
	client *multiClientChannel,
	destinations []affinityPerformanceDestination,
	now time.Time,
) (score float64, prior float64, measured bool) {
	if client == nil || client.args == nil {
		return 0, 0, false
	}
	prior = float64(max(client.args.EstimatedBytesPerSecond, 0))
	provider, ok := providerIdentity(client)
	if !ok {
		return prior, prior, false
	}
	score = prior
	for _, destination := range destinations {
		key := affinityPerformanceKey{provider: provider, destination: destination}
		entry := self.affinityPerformance[key]
		if entry == nil {
			continue
		}
		if affinityPerformanceTTL <= now.Sub(entry.lastUpdate) {
			delete(self.affinityPerformance, key)
			continue
		}
		entryScore := affinityPerformancePosterior(prior, entry)
		if !measured || entryScore < score {
			score = entryScore
		}
		measured = true
	}
	return score, prior, measured
}

// affinityPerformanceUpdateMatches reports whether a live flow supplies
// evidence for any conservative destination key of the new flow. The exact IP
// catches one weak media endpoint; the canonical group lets one low-rate CDN
// connection inform the next selected video. Affinity keys are canonicalized
// when the update is created, so this is only fixed-size/map comparison and
// never repeats public-suffix parsing on placement.
//
// Called with the parent stateLock, which guards affinityIp{4,6}Paths.
func affinityPerformanceUpdateMatches(
	update *multiClientChannelUpdate,
	destinations []affinityPerformanceDestination,
) bool {
	if update == nil || update.ipPath == nil || update.ipPath.Protocol != IpProtocolTcp ||
		update.ipPath.DestinationPort != 443 {
		return false
	}
	var address netip.Addr
	if parsed, ok := netIPAddr(update.ipPath.DestinationIp); ok {
		address = parsed.Unmap()
	}
	for _, destination := range destinations {
		if destination.addr.IsValid() && destination.addr == address {
			return true
		}
		if destination.name == "" {
			continue
		}
		for path := range update.affinityIp4Paths {
			if path.ServerName == destination.name {
				return true
			}
		}
		for path := range update.affinityIp6Paths {
			if path.ServerName == destination.name {
				return true
			}
		}
	}
	return false
}

// affinityPerformanceLiveScoreWithLock folds still-open flows into one
// candidate posterior. This closes the important H2 lifetime gap: a short
// response may leave Chrome's transport open, so no completed-flow record
// exists yet when a newly selected video creates a fresh connection.
//
// clientUpdates already bounds the scan to flows carried by this provider
// (normally <= MaxFlowsPerExit), avoiding a whole-session flow walk or a new
// reverse index. Called with the parent stateLock. Per-flow locks are leaves.
func (self *RemoteUserNatMultiClient) affinityPerformanceLiveScoreWithLock(
	client *multiClientChannel,
	destinations []affinityPerformanceDestination,
	now time.Time,
) (score float64, prior float64, measured bool) {
	if client == nil || client.args == nil {
		return 0, 0, false
	}
	prior = float64(max(client.args.EstimatedBytesPerSecond, 0))
	if prior <= 0 {
		return prior, prior, false
	}
	entry := affinityPerformanceEntry{}
	for update := range self.clientUpdates[client] {
		if update == nil || update.IsDone() || !update.receivedInbound.Load() ||
			!affinityPerformanceUpdateMatches(update, destinations) {
			continue
		}
		update.stateLock.Lock()
		peakRate, ackedBytes := update.ackPerformance.snapshot()
		openTime := update.openTime
		update.stateLock.Unlock()
		if ackedBytes == 0 {
			continue
		}
		weight := affinityPerformanceEvidenceWeight(prior, now.Sub(openTime), ackedBytes)
		if weight <= 0 {
			continue
		}
		entry.weightedPeakRate += peakRate * weight
		entry.evidenceWeight += weight
		measured = true
	}
	if !measured {
		return prior, prior, false
	}
	if affinityPerformanceMaxWeight < entry.evidenceWeight {
		scale := affinityPerformanceMaxWeight / entry.evidenceWeight
		entry.weightedPeakRate *= scale
		entry.evidenceWeight = affinityPerformanceMaxWeight
	}
	return affinityPerformancePosterior(prior, &entry), prior, true
}

func affinityPerformanceHysteresis(settings *ReliabilitySettings) float64 {
	if settings == nil {
		return 0
	}
	return min(max(settings.PlacementHysteresisPct, 0), 50) / 100
}

// affinityPerformanceAllowsDonor is consulted only while creating a fresh
// flow. Returning false releases that flow to the race; it never alters the
// donor flow or any other established connection.
func (self *RemoteUserNatMultiClient) affinityPerformanceAllowsDonor(
	client *multiClientChannel,
	ipPath *IpPath,
) bool {
	settings := self.reliabilitySettings()
	if !settings.PerformanceAwareAffinity {
		return true
	}
	score, prior, measured := self.affinityPerformanceScore(client, ipPath, time.Now())
	if !measured || prior <= 0 {
		return true
	}
	allowed := prior*(1-affinityPerformanceHysteresis(settings)) <= score
	if !allowed {
		self.reliabilityMetrics.affinityPerformanceDonorBypass()
	}
	return allowed
}

// affinityPerformanceAllowsFlowDonor checks the live flow that is about to
// donate affinity before falling back to completed-flow history. This is the
// load-bearing case for "select another video": the page's H2 connection can
// still be open when Chrome creates a fresh media connection. Its current peak
// ACK rate is available even though no flow-close record exists yet.
//
// Called with the parent stateLock. The update lock is a leaf and is released
// before the history lookup can take affinityPerformanceLock.
func (self *RemoteUserNatMultiClient) affinityPerformanceAllowsFlowDonor(
	client *multiClientChannel,
	donor *multiClientChannelUpdate,
	ipPath *IpPath,
) bool {
	settings := self.reliabilitySettings()
	if !settings.PerformanceAwareAffinity || donor == nil || client == nil ||
		ipPath == nil || ipPath.Protocol != IpProtocolTcp || ipPath.DestinationPort != 443 ||
		!donor.receivedInbound.Load() {
		return true
	}

	donor.stateLock.Lock()
	peakRate, ackedBytes := donor.ackPerformance.snapshot()
	openTime := donor.openTime
	donor.stateLock.Unlock()
	if ackedBytes == 0 {
		// A SYN-ACK alone says the connection exists, not how fast it can move
		// content. Preserve the advertised null until useful bytes are ACKed.
		return self.affinityPerformanceAllowsDonor(client, ipPath)
	}
	prior := float64(max(client.EstimatedByteCountPerSecond(), 0))
	if prior <= 0 {
		return true
	}
	weight := affinityPerformanceEvidenceWeight(prior, time.Since(openTime), ackedBytes)
	entry := &affinityPerformanceEntry{
		weightedPeakRate: peakRate * weight,
		evidenceWeight:   weight,
	}
	score := affinityPerformancePosterior(prior, entry)
	allowed := prior*(1-affinityPerformanceHysteresis(settings)) <= score
	if !allowed {
		self.reliabilityMetrics.affinityPerformanceDonorBypass()
	}
	return allowed
}

// preferAffinityPerformanceCandidates makes the posterior load-bearing on the
// full-field race. Candidates within the hysteresis band remain tied; a low
// measured provider is removed only when another candidate has materially
// more weight. If every outcome is equally short/slow, every candidate stays.
func (self *RemoteUserNatMultiClient) preferAffinityPerformanceCandidates(
	candidates []*multiClientChannel,
	update *multiClientChannelUpdate,
) []*multiClientChannel {
	settings := self.reliabilitySettings()
	var ipPath *IpPath
	if update != nil {
		ipPath = update.ipPath
	}
	if !settings.PerformanceAwareAffinity || len(candidates) < 2 || ipPath == nil ||
		ipPath.Protocol != IpProtocolTcp || ipPath.DestinationPort != 443 {
		return candidates
	}
	if affinityPerformanceCandidateStack < len(candidates) {
		// A pathological runtime override must not turn provider count into a
		// placement allocation vector. Normal quality windows have at most six.
		return candidates
	}

	now := time.Now()
	destinationSet := affinityPerformanceDestinationSetForUpdate(update)
	destinations := destinationSet.slice()
	if len(destinations) == 0 {
		return candidates
	}
	var candidateScores [affinityPerformanceCandidateStack]affinityPerformanceCandidateScore
	self.affinityPerformanceLock.Lock()
	for i, candidate := range candidates {
		score, _, measured := self.affinityPerformanceScoreWithLock(candidate, destinations, now)
		candidateScores[i] = affinityPerformanceCandidateScore{
			client:   candidate,
			score:    score,
			measured: measured,
		}
	}
	self.affinityPerformanceLock.Unlock()

	// Completed history alone misses a still-open H2 page connection. Fold in
	// live per-provider flows without retaining another index or touching the
	// ACK hot path. Never nest this parent lock with affinityPerformanceLock.
	self.stateLock.Lock()
	for i := range len(candidates) {
		liveScore, _, liveMeasured := self.affinityPerformanceLiveScoreWithLock(
			candidateScores[i].client,
			destinations,
			now,
		)
		if liveMeasured && (!candidateScores[i].measured || liveScore < candidateScores[i].score) {
			candidateScores[i].score = liveScore
		}
		candidateScores[i].measured = candidateScores[i].measured || liveMeasured
	}
	self.stateLock.Unlock()

	bestScore := 0.0
	anyMeasured := false
	for i := range len(candidates) {
		bestScore = max(bestScore, candidateScores[i].score)
		anyMeasured = anyMeasured || candidateScores[i].measured
	}
	if !anyMeasured || bestScore <= 0 {
		return candidates
	}

	threshold := bestScore * (1 - affinityPerformanceHysteresis(settings))
	keptCount := 0
	for i := range len(candidates) {
		if threshold <= candidateScores[i].score {
			keptCount++
		}
	}
	if keptCount == 0 || keptCount == len(candidates) {
		return candidates
	}

	// raceCandidates returns a placement-local slice. Order it in place so
	// measured fresh flows do not pay a heap allocation precisely when the
	// session is already carrying enough traffic to have useful evidence. Sort
	// the whole tiny field, then retain the threshold set or the exploration
	// floor, whichever is wider.
	preferred := candidates[:0]
	var preferredScores [affinityPerformanceCandidateStack]float64
	for i := range len(candidates) {
		candidate := candidateScores[i].client
		score := candidateScores[i].score
		// Stable insertion keeps the highest posterior first for a truncated
		// rescoring the preceding kept item costs less memory than retaining a
		// heap score buffer on every measured flow.
		insertAt := len(preferred)
		for 0 < insertAt {
			if score <= preferredScores[insertAt-1] {
				break
			}
			insertAt--
		}
		preferred = preferred[:len(preferred)+1]
		copy(preferred[insertAt+1:], preferred[insertAt:])
		copy(preferredScores[insertAt+1:], preferredScores[insertAt:len(preferred)-1])
		preferred[insertAt] = candidate
		preferredScores[insertAt] = score
	}
	selectedCount := max(
		keptCount,
		min(affinityPerformanceMinRaceCandidates, len(preferred)),
	)
	self.reliabilityMetrics.affinityPerformanceCandidatesRemoved(len(candidates) - selectedCount)
	return preferred[:selectedCount]
}
