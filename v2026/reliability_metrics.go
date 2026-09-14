package connect

import (
	"sync"
	"sync/atomic"
	"time"
)

// Reliability work on this client has repeatedly shipped fixes that were
// correct in isolation and changed nothing on a real device, because the only
// available measurement was how long a freeze felt. These counters exist to
// replace that with a number.
//
// The headline number is blast radius: how many live flows one provider
// failure destroys. Providers are split-tcp, so an exit *is* the tcp endpoint
// for every flow pinned to it -- when it dies those connections cannot be
// migrated, on any platform. The client cannot make that cost zero, so the
// goal is to make it small (spread flows over more exits) and to make the
// recovery immediate (tell the peer at once rather than letting it time out).
// Both are measured here, so a candidate fix can be judged instead of assumed.

const (
	// a destination is only a recovery candidate for as long as a user would
	// plausibly still be waiting on it. beyond this the reconnect is a new
	// request, not a recovery, and counting it would flatter the numbers.
	recoveryTrackerMaxAge = 60 * time.Second
	// bounds memory when many flows die at once and nothing comes back --
	// entries are dropped oldest-first rather than allowed to accumulate.
	recoveryTrackerMaxEntries = 4096
)

// blackholeAgeString renders how long a stat clock has been running, for the
// blackhole verdict line. A zero time means the clock never started, which is
// materially different from "started just now" -- it is the difference between
// a provider that has unacked sends outstanding and one that has none.
func blackholeAgeString(t time.Time) string {
	if t.IsZero() {
		return "none"
	}
	return time.Since(t).Round(time.Millisecond).String()
}

// recoveryKey identifies a remote endpoint across the death and rebirth of the
// flows that talk to it. Port is included because a browser reconnecting to
// the same host on a new source port is the recovery being measured, but a
// different service on that host is not.
type recoveryKey struct {
	// net.IP is a byte slice and not comparable; the string conversion is the
	// standard way to key on one.
	ip   string
	port int
}

func newRecoveryKey(ip []byte, port int) recoveryKey {
	return recoveryKey{ip: string(ip), port: port}
}

// pendingRecovery is one armed recovery measurement: a destination whose
// flows died (or moved) with an exit, waiting for its first packet back.
type pendingRecovery struct {
	lostTime time.Time
	// reboundLocalPort is the local source port of the proactively rebound
	// flow that armed this entry, or -1 when the entry was armed by a plain
	// teardown. When the destination answers, comparing the answering flow's
	// local port against this classifies the recovery: the same port means
	// the rebound socket itself is receiving again, i.e. the server accepted
	// the quic path migration; a different port means the app abandoned the
	// old connection and re-dialed. -1 entries close unclassified.
	reboundLocalPort int
}

// reboundFlow identifies one proactively rebound flow for the recovery
// tracker: the destination it serves plus the local source port it was using.
// The port is what later classifies the recovery as a server-accepted
// migration vs an app re-dial.
type reboundFlow struct {
	key       recoveryKey
	localPort int
}

// reliabilityMetrics is written from the packet hot path, so every counter is
// an atomic and the only lock guards the recovery tracker -- which is touched
// once per exit loss and once per first-packet-from-a-pending-destination,
// never per packet.
type reliabilityMetrics struct {
	flowsOpened atomic.Uint64

	// exitLossEvents counts provider failures; flowsLostToExit counts the
	// connections they cost. The ratio is the blast radius.
	exitLossEvents         atomic.Uint64
	flowsLostToExit        atomic.Uint64
	maxFlowsLostInOneEvent atomic.Uint64

	// recovery spans from the moment a flow's exit died to the first packet
	// back from that same destination over a replacement exit. This is the
	// interval the user experiences as the freeze.
	recoveryCount    atomic.Uint64
	recoveryNanos    atomic.Uint64
	recoveryMaxNanos atomic.Uint64
	// recoveryMissed counts destinations that died and never came back inside
	// recoveryTrackerMaxAge. Without it a fix that abandons flows entirely
	// would look better than one that recovers them slowly.
	recoveryMissed atomic.Uint64

	// dialFailures counts provider dial-failure signals intercepted at the
	// channel; flowsReraced counts the flows silently unbound so the
	// application's own retransmit races them onto another exit. The gap
	// between them is failures that matched no live flow (late or stale).
	dialFailures atomic.Uint64
	flowsReraced atomic.Uint64

	// flowsRebound counts established quic flows re-pinned to a replacement
	// exit inside a removal (the proactive rebind) instead of being torn
	// down. rebindsAccepted / rebindsRedialed classify how those recoveries
	// completed: the destination answering the same local source port means
	// the server accepted the quic path migration; a new port means the app
	// re-dialed around it. The pair is the field answer to how well servers
	// actually accept path changes. Their sum can lag flowsRebound: a rebind
	// whose destination never answers inside the tracking window classifies
	// as neither.
	flowsRebound    atomic.Uint64
	rebindsAccepted atomic.Uint64
	rebindsRedialed atomic.Uint64

	// A sustained quarantine fails established split-TCP flows fast so the
	// application can reconnect, and removes every placement reference that
	// could board another handshake onto that exit. Sticky-flow retirement is
	// the independent steady-state cap that preserves the same provider/IP.
	quarantineTcpResets             atomic.Uint64
	quarantineAffinityInvalidations atomic.Uint64
	stickyFlowsRetired              atomic.Uint64
	// Performance-aware affinity learns from TLS-blind inner-TCP ACK
	// progress. Samples counts completed measurements, donorBypasses counts
	// fresh-flow affinity decisions released to the race, and candidatesFiltered
	// counts low-posterior exits removed from those fresh races.
	affinityPerformanceSamples            atomic.Uint64
	affinityPerformanceDonorBypasses      atomic.Uint64
	affinityPerformanceCandidatesFiltered atomic.Uint64

	// a blackhole verdict is only as good as the evidence it is built on, and
	// two conditions make the evidence inadmissible: the local uplink went
	// stale (nothing was deliverable, so silence convicts the network, not
	// the provider), and the transport under the channel is known down.
	// verdictsHeldUplinkStale and verdictsHeldTransportDown count verdicts
	// suppressed for each reason; removalsDeferred counts provider removals
	// postponed while a hold was in effect. This package only defines them --
	// the verdict-gating work that increments them lands separately, and
	// defining the counters first means that work ships already measured.
	verdictsHeldUplinkStale atomic.Uint64
	// verdictsHeldSharedFate counts destructive verdicts held because enough
	// DISTINCT exits developed silence/stall evidence inside one short window
	// that the common cause is overwhelmingly the shared path (the phone's
	// access network), not that many independent providers died at once.
	verdictsHeldSharedFate    atomic.Uint64
	verdictsHeldTransportDown atomic.Uint64
	removalsDeferred          atomic.Uint64

	// provider qualification. probesSent and probesAnswered are the raw
	// question-and-answer counts; providersQualified counts providers that
	// crossed into the qualified state (transitions, not re-proofs).
	//
	// Note what is NOT here: any count of probe failures. That is deliberate --
	// there is no failure metric because a probe failure is not an event about
	// the provider, and a counter for it would be the first thing a future
	// change reached for when it wanted to act on one. The gap between
	// probesSent and probesAnswered is available to anyone who wants the drop
	// rate for tuning; nothing attributes it.
	probesSent     atomic.Uint64
	probesAnswered atomic.Uint64

	// the shared-fate evidence ring: which exits have CURRENT silence/stall
	// evidence, deduplicated by exit and pruned to the configured window on
	// access. Written on verdict passes (a few suspicious exits at a time),
	// never on a packet path.
	sharedFateLock      sync.Mutex
	sharedFateEvidences map[Id]time.Time
	providersQualified  atomic.Uint64

	// the busy-flow liveness probe (see busyLivenessProbe). busyProbesSent
	// counts probes that reached the wire against a stalled exit;
	// busyProbesAcquitted counts the ones answered inside the budget -- exits
	// that would have been removed before this port existed and were not. The
	// gap is convictions, which the ordinary exit-loss counters already record,
	// so nothing here double-counts a removal.
	//
	// schedulerPausesDetected counts host suspends the detector caught. It is
	// the only evidence a doze happened at all: from inside the process the
	// suspend is invisible except as verdicts that would otherwise have fired
	// on wake.
	busyProbesSent          atomic.Uint64
	busyProbesAcquitted     atomic.Uint64
	schedulerPausesDetected atomic.Uint64

	// the G-1 group-follow ledger. groupsFollowed counts inheritances a
	// quarantined donor was allowed to keep (group-follow on, receive
	// fresh); groupsScattered counts inheritances refused ONLY because of
	// quarantine -- each one is a site whose egress ip split on suspicion,
	// which is exactly the event the follow exists to eliminate. Refusals
	// for resize warnings are not counted here; those exits shed new flows
	// on purpose.
	groupsFollowed  atomic.Uint64
	groupsScattered atomic.Uint64

	pendingLock sync.Mutex
	pending     map[recoveryKey]pendingRecovery
}

func newReliabilityMetrics() *reliabilityMetrics {
	return &reliabilityMetrics{
		pending: map[recoveryKey]pendingRecovery{},
	}
}

// Every method below tolerates a nil receiver. Measurement sits directly on
// the packet path, and a client assembled without one -- tests build the
// struct literally -- must lose its counters, not its traffic.

func (self *reliabilityMetrics) flowOpened() {
	if self == nil {
		return
	}
	self.flowsOpened.Add(1)
}

func (self *reliabilityMetrics) dialFailureIntercepted() {
	if self == nil {
		return
	}
	self.dialFailures.Add(1)
}

func (self *reliabilityMetrics) flowReraced() {
	if self == nil {
		return
	}
	self.flowsReraced.Add(1)
}

// recordSharedFate stamps CURRENT silence/stall evidence against an exit.
// Re-recording refreshes the stamp: the question the window answers is "how
// many exits look silent right now", not "how many ever did".
func (self *reliabilityMetrics) recordSharedFate(exitId Id, now time.Time) {
	if self == nil {
		return
	}
	self.sharedFateLock.Lock()
	defer self.sharedFateLock.Unlock()
	if self.sharedFateEvidences == nil {
		self.sharedFateEvidences = map[Id]time.Time{}
	}
	self.sharedFateEvidences[exitId] = now
}

// sharedFatePeers counts DISTINCT exits other than exitId with evidence
// inside the window, pruning stale entries as it goes.
func (self *reliabilityMetrics) sharedFatePeers(exitId Id, now time.Time, window time.Duration) int {
	if self == nil || window <= 0 {
		return 0
	}
	self.sharedFateLock.Lock()
	defer self.sharedFateLock.Unlock()
	peers := 0
	for id, at := range self.sharedFateEvidences {
		if window <= now.Sub(at) {
			delete(self.sharedFateEvidences, id)
			continue
		}
		if id != exitId {
			peers += 1
		}
	}
	return peers
}

func (self *reliabilityMetrics) verdictHeldSharedFate() {
	if self == nil {
		return
	}
	self.verdictsHeldSharedFate.Add(1)
}

func (self *reliabilityMetrics) verdictHeldUplinkStale() {
	if self == nil {
		return
	}
	self.verdictsHeldUplinkStale.Add(1)
}

func (self *reliabilityMetrics) verdictHeldTransportDown() {
	if self == nil {
		return
	}
	self.verdictsHeldTransportDown.Add(1)
}

func (self *reliabilityMetrics) removalDeferred() {
	if self == nil {
		return
	}
	self.removalsDeferred.Add(1)
}

func (self *reliabilityMetrics) probeSent() {
	if self == nil {
		return
	}
	self.probesSent.Add(1)
}

func (self *reliabilityMetrics) groupFollowed() {
	if self == nil {
		return
	}
	self.groupsFollowed.Add(1)
}

func (self *reliabilityMetrics) groupScattered() {
	if self == nil {
		return
	}
	self.groupsScattered.Add(1)
}

func (self *reliabilityMetrics) quarantineTcpReset(count int) {
	if self == nil || count <= 0 {
		return
	}
	self.quarantineTcpResets.Add(uint64(count))
}

func (self *reliabilityMetrics) quarantineAffinityInvalidated(count int) {
	if self == nil || count <= 0 {
		return
	}
	self.quarantineAffinityInvalidations.Add(uint64(count))
}

func (self *reliabilityMetrics) stickyFlowRetired() {
	if self == nil {
		return
	}
	self.stickyFlowsRetired.Add(1)
}

func (self *reliabilityMetrics) affinityPerformanceSample() {
	if self == nil {
		return
	}
	self.affinityPerformanceSamples.Add(1)
}

func (self *reliabilityMetrics) affinityPerformanceDonorBypass() {
	if self == nil {
		return
	}
	self.affinityPerformanceDonorBypasses.Add(1)
}

func (self *reliabilityMetrics) affinityPerformanceCandidatesRemoved(count int) {
	if self == nil || count <= 0 {
		return
	}
	self.affinityPerformanceCandidatesFiltered.Add(uint64(count))
}

func (self *reliabilityMetrics) probeAnswered() {
	if self == nil {
		return
	}
	self.probesAnswered.Add(1)
}

func (self *reliabilityMetrics) providerQualified() {
	if self == nil {
		return
	}
	self.providersQualified.Add(1)
}

func (self *reliabilityMetrics) busyProbeSent() {
	if self == nil {
		return
	}
	self.busyProbesSent.Add(1)
}

func (self *reliabilityMetrics) busyProbeAcquitted() {
	if self == nil {
		return
	}
	self.busyProbesAcquitted.Add(1)
}

func (self *reliabilityMetrics) schedulerPauseDetected() {
	if self == nil {
		return
	}
	self.schedulerPausesDetected.Add(1)
}

// exitLost records one provider failure and the flows it destroyed, and arms
// each of their destinations for a recovery measurement.
func (self *reliabilityMetrics) exitLost(destinations []recoveryKey) {
	if self == nil {
		return
	}
	self.exitLossEvents.Add(1)
	self.flowsLostToExit.Add(uint64(len(destinations)))

	n := uint64(len(destinations))
	for {
		observed := self.maxFlowsLostInOneEvent.Load()
		if n <= observed || self.maxFlowsLostInOneEvent.CompareAndSwap(observed, n) {
			break
		}
	}

	if len(destinations) == 0 {
		return
	}

	now := time.Now()

	self.pendingLock.Lock()
	defer self.pendingLock.Unlock()

	for _, key := range destinations {
		// keep the earliest death for a destination. several flows to one host
		// die together, and the user is waiting from the first of them.
		if _, exists := self.pending[key]; !exists {
			self.pending[key] = pendingRecovery{
				lostTime:         now,
				reboundLocalPort: -1,
			}
		}
	}
	// evict after inserting, not before: a single exit can be carrying more
	// flows than the tracker holds, and evicting first would leave the whole
	// batch resident.
	self.evictPendingWithLock(now)
}

// exitLostRebound records the flows a dying exit handed to live replacements,
// and arms each of their destinations for the same recovery measurement
// exitLost arms -- with the local source port attached so the recovery can be
// classified as a server-accepted migration vs an app re-dial. Deliberately
// distinct from exitLost: a rebound flow is not lost, so it stays out of the
// blast-radius counters, and the exit-loss event itself is already counted by
// the exitLost call every removal makes.
func (self *reliabilityMetrics) exitLostRebound(rebounds []reboundFlow) {
	if self == nil {
		return
	}
	self.flowsRebound.Add(uint64(len(rebounds)))
	if len(rebounds) == 0 {
		return
	}

	now := time.Now()

	self.pendingLock.Lock()
	defer self.pendingLock.Unlock()

	for _, rebound := range rebounds {
		// keep the earliest death for a destination, the same rule exitLost
		// applies: when a torn-down flow to this destination already armed
		// the entry, the user has been waiting since that teardown, and the
		// entry keeps its unclassified identity. One entry per destination is
		// the tracker's grain, so the migration split is sampled per
		// destination rather than per flow -- several rebound flows to one
		// host contribute the port of whichever armed first.
		if _, exists := self.pending[rebound.key]; !exists {
			self.pending[rebound.key] = pendingRecovery{
				lostTime:         now,
				reboundLocalPort: rebound.localPort,
			}
		}
	}
	self.evictPendingWithLock(now)
}

// destinationReachable reports the first packet back from a remote endpoint. It
// closes out a pending recovery if one is armed, and is a no-op otherwise --
// which is the overwhelmingly common case, so it takes the lock only after a
// racy empty check. localPort is the local (app-side) port that packet arrived
// for -- the ingress path reads it off the packet's destination before the
// path is reversed -- and it is what classifies a rebound flow's recovery.
func (self *reliabilityMetrics) destinationReachable(ip []byte, port int, localPort int) {
	if self == nil || len(ip) == 0 {
		return
	}

	self.pendingLock.Lock()
	defer self.pendingLock.Unlock()

	if len(self.pending) == 0 {
		return
	}

	key := newRecoveryKey(ip, port)
	entry, ok := self.pending[key]
	if !ok {
		return
	}
	delete(self.pending, key)

	// Past the window this is not a recovery, it is the user happening to
	// visit the site again. Eviction is lazy -- it only runs when another exit
	// dies -- so a stale entry can survive well beyond the age bound and, left
	// unchecked here, is credited as an enormously slow recovery. On device
	// that turned a 14s average into "avg 2m, worst 9m" and made the number
	// measure browsing habits rather than the tunnel.
	if recoveryTrackerMaxAge <= time.Since(entry.lostTime) {
		self.recoveryMissed.Add(1)
		return
	}

	// classify a rebound flow's recovery: the destination answering the very
	// local port the rebound flow was using means the rebound socket itself
	// is receiving again -- the server accepted the quic path migration. A
	// different port means the app abandoned the moved connection and
	// re-dialed. Teardown-armed entries carry -1 and close unclassified.
	if 0 <= entry.reboundLocalPort {
		if entry.reboundLocalPort == localPort {
			self.rebindsAccepted.Add(1)
		} else {
			self.rebindsRedialed.Add(1)
		}
	}

	nanos := time.Since(entry.lostTime).Nanoseconds()
	if nanos < 0 {
		nanos = 0
	}
	self.recoveryCount.Add(1)
	self.recoveryNanos.Add(uint64(nanos))
	for {
		observed := self.recoveryMaxNanos.Load()
		if uint64(nanos) <= observed || self.recoveryMaxNanos.CompareAndSwap(observed, uint64(nanos)) {
			break
		}
	}
}

// evictPendingWithLock retires destinations that never came back. Callers hold
// pendingLock.
func (self *reliabilityMetrics) evictPendingWithLock(now time.Time) {
	for key, entry := range self.pending {
		if recoveryTrackerMaxAge <= now.Sub(entry.lostTime) {
			delete(self.pending, key)
			self.recoveryMissed.Add(1)
		}
	}

	// age alone does not bound the map when losses arrive faster than they
	// expire, so drop the oldest until it fits.
	for recoveryTrackerMaxEntries < len(self.pending) {
		var oldestKey recoveryKey
		var oldestTime time.Time
		first := true
		for key, entry := range self.pending {
			if first || entry.lostTime.Before(oldestTime) {
				oldestKey, oldestTime, first = key, entry.lostTime, false
			}
		}
		if first {
			break
		}
		delete(self.pending, oldestKey)
		self.recoveryMissed.Add(1)
	}
}

func (self *reliabilityMetrics) reset() {
	if self == nil {
		return
	}
	self.flowsOpened.Store(0)
	self.exitLossEvents.Store(0)
	self.flowsLostToExit.Store(0)
	self.maxFlowsLostInOneEvent.Store(0)
	self.recoveryCount.Store(0)
	self.recoveryNanos.Store(0)
	self.recoveryMaxNanos.Store(0)
	self.recoveryMissed.Store(0)
	self.dialFailures.Store(0)
	self.flowsReraced.Store(0)
	self.flowsRebound.Store(0)
	self.rebindsAccepted.Store(0)
	self.rebindsRedialed.Store(0)
	self.quarantineTcpResets.Store(0)
	self.quarantineAffinityInvalidations.Store(0)
	self.stickyFlowsRetired.Store(0)
	self.affinityPerformanceSamples.Store(0)
	self.affinityPerformanceDonorBypasses.Store(0)
	self.affinityPerformanceCandidatesFiltered.Store(0)
	self.verdictsHeldUplinkStale.Store(0)
	self.verdictsHeldSharedFate.Store(0)
	self.verdictsHeldTransportDown.Store(0)
	self.removalsDeferred.Store(0)
	self.probesSent.Store(0)
	self.probesAnswered.Store(0)
	self.providersQualified.Store(0)
	self.busyProbesSent.Store(0)
	self.busyProbesAcquitted.Store(0)
	self.schedulerPausesDetected.Store(0)
	self.groupsFollowed.Store(0)
	self.groupsScattered.Store(0)

	self.pendingLock.Lock()
	defer self.pendingLock.Unlock()
	self.pending = map[recoveryKey]pendingRecovery{}
}

// ReliabilityMetricsSnapshot is a consistent-enough read of the counters for
// reporting. The counters are sampled independently, so a snapshot taken while
// traffic is flowing can straddle an update; that is fine for A/B comparison
// over a run and avoids taking a lock on the packet path.
type ReliabilityMetricsSnapshot struct {
	FlowsOpened uint64

	ExitLossEvents         uint64
	FlowsLostToExit        uint64
	MaxFlowsLostInOneEvent uint64
	// MeanFlowsLostPerExitLoss is the blast radius: the average number of
	// connections one provider failure costs. Lower is the goal.
	MeanFlowsLostPerExitLoss float64

	RecoveryCount     uint64
	RecoveryMissed    uint64
	RecoveryMeanNanos int64
	RecoveryMaxNanos  int64
	// RecoveryPending is how many destinations are still waiting to come back
	// at the moment of the snapshot.
	RecoveryPending int

	// DialFailuresIntercepted counts provider could-not-connect signals;
	// FlowsReraced counts the flows quietly moved to another exit in
	// response. These are the events the user never sees -- each one would
	// previously have been a 3-63s syn-backoff hang.
	DialFailuresIntercepted uint64
	FlowsReraced            uint64

	// FlowsRebound counts established quic flows proactively re-pinned to a
	// replacement exit inside a removal instead of being torn down.
	// RebindsAccepted counts those whose destination answered the same local
	// source port -- the server accepted the quic path migration;
	// RebindsRedialed counts destinations that answered a new port -- the
	// app re-dialed. The sum can lag FlowsRebound: a rebind whose
	// destination never answers inside the tracking window is neither.
	FlowsRebound    uint64
	RebindsAccepted uint64
	RebindsRedialed uint64

	QuarantineTcpResets                   uint64
	QuarantineAffinityInvalidations       uint64
	StickyFlowsRetired                    uint64
	AffinityPerformanceSamples            uint64
	AffinityPerformanceDonorBypasses      uint64
	AffinityPerformanceCandidatesFiltered uint64

	// VerdictsHeldUplinkStale and VerdictsHeldTransportDown count blackhole
	// verdicts suppressed because the evidence was inadmissible (the local
	// uplink was stale, the transport was known down); RemovalsDeferred
	// counts provider removals postponed while such a hold was in effect.
	VerdictsHeldUplinkStale   uint64
	VerdictsHeldSharedFate    uint64
	VerdictsHeldTransportDown uint64
	RemovalsDeferred          uint64

	// ProbesSent and ProbesAnswered are the provider-qualification probes this
	// session asked and got back; ProvidersQualified is how many providers a
	// pass proved (transitions into the qualified state). There is deliberately
	// no failure counter -- see the fields on reliabilityMetrics.
	ProbesSent         uint64
	ProbesAnswered     uint64
	ProvidersQualified uint64

	// BusyProbesSent and BusyProbesAcquitted are the busy-flow liveness probes
	// fired at stalled exits and the ones answered inside the budget -- the
	// removals the probe prevented. SchedulerPausesDetected counts host
	// suspends (doze, freezer, thermal) the pause detector caught.
	BusyProbesSent          uint64
	BusyProbesAcquitted     uint64
	SchedulerPausesDetected uint64

	// GroupsFollowed counts new flows a quarantined donor kept under
	// group-follow; GroupsScattered counts the ones quarantine still turned
	// away (follow off, or the donor receive-stale) -- each a site whose
	// egress ip split on suspicion.
	GroupsFollowed  uint64
	GroupsScattered uint64
}

func (self *reliabilityMetrics) snapshot() *ReliabilityMetricsSnapshot {
	if self == nil {
		return &ReliabilityMetricsSnapshot{}
	}
	exitLossEvents := self.exitLossEvents.Load()
	flowsLost := self.flowsLostToExit.Load()
	recoveryCount := self.recoveryCount.Load()
	recoveryNanos := self.recoveryNanos.Load()

	snapshot := &ReliabilityMetricsSnapshot{
		FlowsOpened:            self.flowsOpened.Load(),
		ExitLossEvents:         exitLossEvents,
		FlowsLostToExit:        flowsLost,
		MaxFlowsLostInOneEvent: self.maxFlowsLostInOneEvent.Load(),
		RecoveryCount:          recoveryCount,
		RecoveryMissed:         self.recoveryMissed.Load(),
		RecoveryMaxNanos:       int64(self.recoveryMaxNanos.Load()),

		DialFailuresIntercepted: self.dialFailures.Load(),
		FlowsReraced:            self.flowsReraced.Load(),

		FlowsRebound:    self.flowsRebound.Load(),
		RebindsAccepted: self.rebindsAccepted.Load(),
		RebindsRedialed: self.rebindsRedialed.Load(),

		QuarantineTcpResets:                   self.quarantineTcpResets.Load(),
		QuarantineAffinityInvalidations:       self.quarantineAffinityInvalidations.Load(),
		StickyFlowsRetired:                    self.stickyFlowsRetired.Load(),
		AffinityPerformanceSamples:            self.affinityPerformanceSamples.Load(),
		AffinityPerformanceDonorBypasses:      self.affinityPerformanceDonorBypasses.Load(),
		AffinityPerformanceCandidatesFiltered: self.affinityPerformanceCandidatesFiltered.Load(),

		VerdictsHeldUplinkStale:   self.verdictsHeldUplinkStale.Load(),
		VerdictsHeldSharedFate:    self.verdictsHeldSharedFate.Load(),
		VerdictsHeldTransportDown: self.verdictsHeldTransportDown.Load(),
		RemovalsDeferred:          self.removalsDeferred.Load(),

		ProbesSent:         self.probesSent.Load(),
		ProbesAnswered:     self.probesAnswered.Load(),
		ProvidersQualified: self.providersQualified.Load(),

		BusyProbesSent:          self.busyProbesSent.Load(),
		BusyProbesAcquitted:     self.busyProbesAcquitted.Load(),
		SchedulerPausesDetected: self.schedulerPausesDetected.Load(),

		GroupsFollowed:  self.groupsFollowed.Load(),
		GroupsScattered: self.groupsScattered.Load(),
	}

	if 0 < exitLossEvents {
		snapshot.MeanFlowsLostPerExitLoss = float64(flowsLost) / float64(exitLossEvents)
	}
	if 0 < recoveryCount {
		snapshot.RecoveryMeanNanos = int64(recoveryNanos / recoveryCount)
	}

	self.pendingLock.Lock()
	snapshot.RecoveryPending = len(self.pending)
	self.pendingLock.Unlock()

	return snapshot
}

// ReliabilityMetrics reports what provider failures have cost this session.
func (self *RemoteUserNatMultiClient) ReliabilityMetrics() *ReliabilityMetricsSnapshot {
	return self.reliabilityMetrics.snapshot()
}

// ResetReliabilityMetrics zeroes the counters so an A/B run starts clean.
// Pending recoveries are dropped rather than carried across the boundary,
// since a recovery that began under the previous config would otherwise be
// credited to the next one.
func (self *RemoteUserNatMultiClient) ResetReliabilityMetrics() {
	// logged for the same reason every other dev action is: the counters in a
	// later heartbeat jumping backwards is otherwise an unexplained anomaly,
	// and this line is the explanation
	self.logAction("reset_metrics")
	self.reliabilityMetrics.reset()
}
