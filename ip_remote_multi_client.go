package connect

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	// "reflect"
	"errors"
	"fmt"
	"math"
	"math/bits"
	mathrand "math/rand"
	"net"
	"net/netip"
	"slices"
	"strings"

	"golang.org/x/net/publicsuffix"
	"maps"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/protocol"
)

// multi client is a sender approach to mitigate bad destinations
// it maintains a window of compatible clients chosen using specs
// (e.g. from a desription of the intent of use)
// - the clients are rate limited by the number of outstanding acks (nacks)
// - the size of allowed outstanding nacks increases with each ack,
// scaling up successful destinations to use the full transfer buffer
// - the clients are chosen with probability weighted by their
// net frame count statistics (acks - nacks)

// the following functions handle moving clients in and out of the window:
// - `resize`
//   The goal of the resize is to meet a target window size based on the number
//   of different source ip:port per destination ip:port.
//   Two statistics are used: effective bytes per second ([ack used])
//     and expected bytes per second ([capacity]-[ack used]-[unacked used]).
//   Unhealthy clients are removed from the window based on low effective stats,
//   unless the window is fixed size.
//   Fundamentally this approach can't tell the difference between an
//   unhealthy client and an idle client, so the norm is to continually change clients
//   in the lull after a burst of usage.
// - `detectBlackhole`
//   When a client acks traffic but does not return traffic,
//   it gets labeled a black hole. Black hole clients may be malicious
//   or have network filtering. Black hole clients are removed.
// - `ping`
//   When a client is idle it must continually ack ping requests.
//   Clients that fail to ack are removed.

// TODO surface window stats to show to users

type clientReceivePacketFunction func(client *multiClientChannel, source TransferPath, provideMode protocol.ProvideMode, transportType TransportType, ipPath *IpPath, packet []byte)

// dialFailureFunction carries a provider's intercepted dial-failure signal up
// from the channel that received it to the parent, which owns the flow maps.
// The signal is an icmp destination-unreachable that ParseIpPath rejects;
// egressIpPath is the failed flow's original source->destination key recovered
// from the icmp embed by ipParseIcmpUnreachable. Mirrors
// clientReceivePacketFunction, but the parent rebuilds the packet from the path
// (the removeClient teardown convention) rather than carrying raw bytes.
// The bool reports whether the flow was actually unbound for a re-race, so a
// caller holding its own client snapshot knows whether to discard it.
type dialFailureFunction func(sourceClient *multiClientChannel, egressIpPath *IpPath) (reraced bool)

type DestinationStats struct {
	EstimatedBytesPerSecond ByteCount
	Tier                    int
	// NetworkOnly identifies a provider that the server admitted because it
	// belongs to the caller's own network. Auto routing may prefer one for a
	// fresh web/media session only when its measured quality is fast enough;
	// it never changes an established flow's exit.
	NetworkOnly bool
	// ReputationFailures is the normalized, bounded set of domains/vendor
	// labels rejected by the provider's external reputation probes. It is
	// discovery metadata only: Connect never inspects encrypted HTTP status.
	ReputationFailures []string
	// Location is the destination (egress) provider's location from
	// find-providers2. nil for fixed client-id and restored-identity
	// destinations, which bypass discovery.
	Location *ProviderLocation
	// IpFamily is the destination provider's proven address-family
	// category from find-providers2. Legacy (empty) for fixed and restored
	// destinations and for older servers; legacy carries v4 only.
	IpFamily IpFamily
}

type WindowType int

func (self WindowType) RankMode() string {
	switch self {
	case WindowTypeQuality:
		return "quality"
	case WindowTypeSpeed:
		return "speed"
	default:
		return ""
	}
}

const (
	// WindowTypeAuto is the zero value: no fixed window type. A nil
	// performance profile and a profile with `WindowTypeAuto` mean the same
	// thing — traffic balances across the window types and each window uses
	// its own default size settings (the profile `WindowSize` is ignored).
	WindowTypeAuto    WindowType = 0
	WindowTypeQuality WindowType = 1
	WindowTypeSpeed   WindowType = 2
)

// for each `NewClientArgs`,
//
//	`RemoveClientWithArgs` will be called if a client was created for the args,
//	else `RemoveClientArgs`
type MultiClientGenerator interface {
	// path -> estimated byte count per second
	// the enumeration should typically
	// 1. not repeat final destination ids from any path
	// 2. not repeat intermediary elements from any path
	NextDestinations(count int, excludeDestinations []MultiHopId, rankMode string) (map[MultiHopId]DestinationStats, error)
	// client id, client auth
	NewClientArgs() (*MultiClientGeneratorClientArgs, error)
	RemoveClientArgs(args *MultiClientGeneratorClientArgs)
	RemoveClientWithArgs(client *Client, args *MultiClientGeneratorClientArgs)
	NewClientSettings() *ClientSettings
	NewClient(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error)
	FixedDestinationSize() (int, bool)
}

// MultiClientGeneratorExcluder is an optional generator capability: exclude a
// provider from near-term discovery. Implementations own the retention policy;
// the API generator uses a bounded oldest-evicted runtime history. Used by
// RemoteUserNatMultiClient.RemoveProvider so a removed provider is not handed
// straight back by the next discovery call.
type MultiClientGeneratorExcluder interface {
	ExcludeClientId(clientId Id)
}

// MultiClientGeneratorWithIpFamily is an optional generator capability:
// discover destinations that satisfy an address-family filter. A window uses
// it to fill a family shortfall (see WindowSizeSettings.WindowSizeMinIpv6Capable).
// A generator without it is called through `NextDestinations` and its
// destinations are treated as legacy, which carries v4 only.
type MultiClientGeneratorWithIpFamily interface {
	NextDestinationsWithIpFamily(count int, excludeDestinations []MultiHopId, rankMode string, ipFamily IpFamilyFilter) (map[MultiHopId]DestinationStats, error)
}

func DefaultMultiClientSettings() *MultiClientSettings {
	return &MultiClientSettings{
		SequenceBufferSize:  defaultTransferBufferSize,
		SequenceIdleTimeout: 120 * time.Second,
		// tcp connections are routinely idle between requests and users still
		// consider them open; traditional vpns hold nat state for 5-30 minutes
		TcpSequenceIdleTimeout: 600 * time.Second,

		WindowSizes: map[WindowType]WindowSizeSettings{
			// TODO increase `WindowSizeMinP2pOnly` when p2p is deployed
			WindowTypeQuality: WindowSizeSettings{
				// Six providers preserve quality-window redundancy without the
				// memory cost of growing the target to twelve. Keep the hard max
				// at six too: StandingReserve normally adds one provider beyond
				// the target, but that seventh client still owns a full client and
				// transport graph.
				//
				// WindowSizeMin is held at idle, so the quality window remains
				// ready rather than rebuilding from a smaller floor.
				WindowSizeMin:     6,
				WindowSizeMax:     6,
				WindowSizeHardMax: 6,
				// one v6-capable exit, softly: see WindowSizeMinIpv6Capable
				WindowSizeMinIpv6Capable: 1,
				// reconnects per source
				WindowSizeReconnectScale: 1.2,
				KeepHealthiestCount:      0,
			},
			WindowTypeSpeed: WindowSizeSettings{
				WindowSizeMin:     1,
				WindowSizeMax:     2,
				WindowSizeHardMax: 4,
				FixedWindowSize:   1,
				// WindowSizeUseMax:     1,
				// reconnects per source
				WindowSizeReconnectScale: 1.0,
				KeepHealthiestCount:      0,
			},
		},
		SendRetryTimeout: 2000 * time.Millisecond,
		// while the window has no clients at all, poll for the first one
		// quickly so the first packets leave moments after it lands
		FormationPollTimeout:          200 * time.Millisecond,
		EncryptionCapabilityPrefilter: true,
		PingWriteTimeout:              5 * time.Second,
		CPingWriteTimeout:             15 * time.Second,
		CPingMaxByteCountPerSecond:    kib(32),
		// the initial ping includes creating the transports and contract
		// ease up the timeout until perf issues are fully resolved
		PingTimeout:  30 * time.Second,
		CPingTimeout: 30 * time.Second,
		// the rest between continuous pings. decoupled from `CPingTimeout` (the
		// ack wait) so a dead idle client is detected within
		// ~CPingRestTimeout+CPingTimeout instead of ~2x CPingTimeout
		CPingRestTimeout: 10 * time.Second,
		// a lower ack timeout helps cycle through bad providers faster
		AckTimeout:              30 * time.Second,
		BlackholeTimeout:        5 * time.Second,
		BlackholeReceiveTimeout: 20 * time.Second,
		MaxFlowsPerExit:         16,
		// Legacy/explicit affinity keeps its exit as it grows; ordinary fresh
		// flows race by default (FreshFlowAffinity is false).
		AffinityStickyPastCap: true,
		// Ordinary fresh flows do not inherit a site/IP donor. Affinity state is
		// still collected as a bounded measurement key, and explicit app/host
		// pins remain strict opt-ins to one-egress behavior.
		FreshFlowAffinity: false,
		// Fresh TLS races learn from peak inner-TCP ACK progress relative to
		// each provider's advertised bandwidth prior.
		PerformanceAwareAffinity: true,
		// Sticky sites retain one egress IP, but idle H1 state is trimmed once
		// the site has grown well beyond an ordinary browser connection set.
		MaxStickyFlowsPerExit: 64,
		StickyFlowIdleTimeout: 30 * time.Second,
		// a benched site keeps its exit through the early bench;
		// see the field comments
		QuarantineGroupFollow:   false,
		GroupFollowWindow:       45 * time.Second,
		DialFailureRerace:       true,
		BlackholeConnectTimeout: 30 * time.Second,
		// a third of the full connect bar: long enough that a slow-but-working
		// first connect is not cut short, short enough that an exit which has
		// established nothing while two siblings are visibly receiving stops
		// holding its flows hostage for the full 30s
		BlackholeConnectComparativeTimeout: 10 * time.Second,
		// long enough that an ordinary quiet moment (all flows idle between
		// requests) does not read as an outage, short enough that the gate is
		// engaged well before the shortest receive verdict (20s) can mature
		// on evidence collected during the silence
		UplinkStalenessGate:                       5 * time.Second,
		WindowResizeTimeout:                       15 * time.Second,
		StatsWindowGraceperiod:                    30 * time.Second,
		StatsWindowMaxEstimatedByteCountPerSecond: mib(16),
		// StatsWindowMaxEffectiveByteCountPerSecondScale: 0.8,
		StatsWindowEntropy:  0.0,
		WindowExpandTimeout: 15 * time.Second,
		// WindowExpandBlockTimeout: 5 * time.Second,
		WindowExpandBlockCount: 4,
		// wait this time before enumerating potential clients again
		// WindowEnumerateEmptyTimeout: 5 * time.Second,
		WindowEnumerateErrorTimeout: 1 * time.Second,
		WindowRateLimitBackoffMin:   5 * time.Second,
		WindowRateLimitBackoffMax:   2 * time.Minute,
		// long enough for a slow-but-alive platform API round trip; a hung
		// API must never wedge the enumerate/expand machinery
		WindowGeneratorTimeout:      20 * time.Second,
		IpFamilyStarvedRetryTimeout: defaultIpFamilyStarvedRetryTimeout,
		// WindowMaxScale:              4.0,
		// WindowExpandMaxOvershotScale: 2.0,
		// WindowRevisitTimeout:      2 * time.Minute,
		WindowExpandArgsTimeout:   2 * time.Minute,
		StatsWindowDuration:       30 * time.Second,
		StatsWindowBucketDuration: 1 * time.Second,
		StatsSampleWeightsCount:   8,
		// percentile
		StatsSourceCountSelection: 0.9,
		// ClientAffinityTimeout:        0 * time.Second,

		MultiRaceSetOnNoResponseTimeout:      5 * time.Second,
		MultiRaceSetOnResponseTimeout:        2 * time.Second,
		MultiRaceClientSentPacketMaxCount:    16,
		MultiRaceClientPacketMaxCount:        8,
		MultiRacePacketMaxCount:              32,
		MultiRaceClientEarlyCompleteFraction: 0.25,
		// MultiRaceClientCount bounds how many exits one cold-start race dials
		// in parallel. 0 (the previous default) races the entire ordered field,
		// which at a full window means every new flow's first packet is
		// duplicated to up to window-size exits. Most flows never reach the
		// race at all -- destination affinity hands them an exit directly --
		// so the bound only touches genuinely cold starts, where the first two
		// candidates are already the weighted-shuffle's best guesses and a
		// two-way race preserves the latency benefit of racing at all
		// (a losing first pick is covered by the second in the same round
		// trip, and the retry loop re-races with a fresh field anyway).
		//
		// 0 = race everything, and that is deliberate. A bound of 2 was tried
		// (the dial-strike-noise rationale: window-size duplicate origin dials
		// per cold start manufactured starvation evidence against exits that
		// were merely slower) and reverted after one field session: on a pool
		// whose providers stall intermittently, two picks are a coin flip, and
		// a cold-start-heavy workload -- short-video feeds open new quic
		// connections continuously -- read as constant spinners. The wide race
		// is the tail-latency insurance exactly when the pool is rough. The
		// strike-noise concern is carried instead by the per-destination
		// gating on dialStarved, which requires strikes spanning distinct
		// destinations and so already discounts race-loser noise.
		//
		// The truncation is enforced at raceClients in sendPacket
		// (`raceOrderedClients = orderedClients[:self.settings.MultiRaceClientCount]`).
		MultiRaceClientCount: 0,

		StatsWindowMaxUnhealthyDuration:  15 * time.Second,
		StatsWindowWarnUnhealthyDuration: 5 * time.Second,
		// how long a rank-kept client (FixedWindowSize/KeepHealthiestCount) may
		// remain continuously unhealthy before it is removed anyway. The same
		// bound is the restoration grace for an empty transport set: a network
		// migration may temporarily withdraw every route, but a route set that
		// never returns must not pin a dead client (and its UI dot) forever.
		StatsWindowKeepUnhealthyDuration: 60 * time.Second,
		// StatsWindowKeepHealthiestCount:   2,
		// the effective byte count is per stats window `StatsWindowDuration`
		// StatsWindowMinHealthyEffectiveSendByteCount:    kib(1),
		// StatsWindowMinHealthyEffectiveReceiveByteCount: kib(32),

		MaxClientLifetime: 60 * time.Minute,

		ProtocolVersion:     DefaultProtocolVersion,
		DestinationAffinity: true,

		DefaultReconnectScale: 1.0,
		DefaultUlimit:         0,

		TcpCollapsePrevention:   true,
		UdpCollapsePrevention:   false,
		EnableIcmp:              false,
		DegradedMode:            &atomic.Bool{},
		DegradedLivenessScale:   3.0,
		RemovalReceiveQueueSize: 256,
		// One native bridge batch is at most 64 packets / 96 KiB. Keep the
		// default ownership transaction no larger than that; mobile profiles can
		// select smaller chunks without changing bridge acceptance.
		PacketGroupMaxPacketCount: 64,
		PacketGroupMaxByteCount:   96 * 1024,

		UdpTeardownSignal:        true,
		QuicRebindOnExitLoss:     true,
		StandingReserve:          true,
		ClusterAffinityFallback:  true,
		ServerNameAffinityBridge: true,
		// well inside AckTimeout (30s), which is what otherwise bounds recovery
		// from a client that accepts packets and never delivers them, and well
		// outside any normal ack round trip so ordinary latency is not mistaken
		// for a stall
		SendStallTimeout: 3 * time.Second,
		// ask before convicting: a congested exit answers, a dead one does not.
		// false is the convict-immediately A/B comparison point
		BusyProbe: true,
		// 0 derives max(1s, SendStallTimeout/2) = 1.5s at the bar above
		BusyProbeBudget: 0,
		// 2s of excess timer delay is far outside any scheduling jitter a
		// running process sees and far inside the shortest doze window, so the
		// detector fires on real suspends and on nothing else
		SchedulerPauseTolerance: 2 * time.Second,
		// one full uplink-gate window (5s) of grace after wake: long enough for
		// the transports to re-register and the first return packets to land,
		// short enough that a genuinely dead exit is still convicted promptly
		SchedulerPauseRecoveryTimeout: 5 * time.Second,
		// well under the 30s AckTimeout that previously bounded a stalled
		// flow, and past the ~200ms-1s range of a first tcp rto so a healthy
		// flow's retransmits are still collapsed
		TcpCollapseMaxHold: 1500 * time.Millisecond,
		SoftVerdictDemote:  true,
		// two removals per half minute lets the ordinary single-provider
		// failure execute immediately (and its replacement fail once too)
		// while stopping the storm shape observed in the field: one
		// migration-flavored event convicting exit after exit within seconds
		RemovalBudgetCount:  2,
		RemovalBudgetWindow: 30 * time.Second,
		// rank on live health, not just the platform's static tier; see the
		// field comment. false is the static-Tier A/B comparison point
		EffectiveTierSelection: true,
		// one dead website must not convict an exit; see the field comment.
		// 2 is the smallest value that makes the no-receive-ack verdict
		// corroborated evidence rather than a single destination's silence
		MinBlackholeDestinations: 2,
		// and the busier the exit, the broader the silence must be; see the
		// field comment
		BlackholeLoadCorroboration: 8,
		// on by default on this fork. With F-2 this is no longer inert: the
		// constructor starts the prober loop (startup sweep, joiner probes,
		// staleness re-probes -- see runProber), effectiveTier reads the
		// qualification as a +1 demerit for unproven providers, and expand's
		// admit selection prefers qualified candidates.
		ProviderProbe: true,
		// a tcp handshake through a provider to a live site completes well
		// inside this on any network worth keeping; past it the answer is not
		// coming, and a probe that waits longer only delays a sweep
		ProbeTimeout: 4 * time.Second,
		// 0 = the entire table each pass; see the field comment
		ProbeSampleHostCount: 0,
		// two consecutive all-silent passes before the placement warning: one
		// could be unlucky timing against a dozing device's wake cycle, two
		// spans ~10 minutes of retest cadence -- and the streak self-clears on
		// any evidence of life, so the cost of warning a provider that was
		// merely asleep is a few minutes out of new-flow selection
		ProbeSilenceWarnStreak: 2,
		// three exits going silent inside ten seconds is not three provider
		// deaths; see the field comment
		SharedFateMinExits: 3,
		SharedFateWindow:   10 * time.Second,
		// mainnet-aggressive: evaluate up to twice the candidates a window
		// expansion needs and keep the best. The four-client expansion-step cap
		// still bounds complete client constructions in one pass.
		EvaluationPoolMultiple: MemoryScaledCount(2, 1),

		// the outcome deadline (window honesty): zero Added this long after
		// the window first tries to expand triggers ONE silent window rebuild
		// (the programmatic form of the manual disconnect+reconnect that
		// reliably recovered the field hangs), and zero Added this long after
		// the rebuild latches the terminal failed state with the stall reason.
		// Long enough that a slow-but-working first connect (the 9.3s green
		// baseline, with margin for a rough pool) is never cut short; short
		// enough that nobody watches yellow dots for minutes. 0 disables.
		WindowOutcomeDeadline:        45 * time.Second,
		WindowOutcomeRebuildDeadline: 45 * time.Second,

		// one state line per minute. Frequent enough that any window of a
		// capture reconstructs the session shape, rare enough that a full day of
		// logs costs a few hundred lines -- and only when something is actually
		// changing, since an unchanged beat is skipped
		HeartbeatInterval: 60 * time.Second,

		SecurityPolicyGenerator: DefaultSecurityPolicyWithStats,

		// the epoch for flushing block action and packet stats events to listeners
		EventEpoch:                  1 * time.Second,
		BlockActionDecisionTtl:      30 * time.Second,
		BlockActionDecisionMaxCount: 4096,
		BlockActionAggMaxCount:      1024,
		IpAssocSettings:             DefaultIpAssocSettings(),

		RemoteUserNatMultiClientMonitorSettings: *DefaultRemoteUserNatMultiClientMonitorSettings(),

		// Smart routing (Phase 2), turned on by default -- see the
		// ReliabilitySettings field comments above for what each knob does.
		// This is the one place in the smart-routing phase permitted to
		// change a default: the classifier, telemetry, and reward tap this
		// phase built are otherwise inert (scoredplacement=1 with no
		// classifier installed is a no-op), so the owner could never observe
		// them working without a hand-flipped developer-menu override.
		// QuarantineReentryRamp is deliberately left at its zero value here
		// -- its decay story is separate and not part of this change.
		LightClassifier:            true,
		ScoredPlacement:            true,
		RewardInstrumentation:      true,
		PlacementHysteresisPct:     10,
		PlacementDemoteConsecutive: 3,
		QuarantineDampening:        true,
	}
}

type MultiClientSettings struct {
	// Log, when set, is used by the multi client, its windows, channels, and
	// internal local user nat. nil resolves to `DefaultLogger()`.
	Log Logger

	SequenceBufferSize  int
	SequenceIdleTimeout time.Duration
	// TcpSequenceIdleTimeout is the idle timeout for tcp flows specifically.
	// 0 falls back to SequenceIdleTimeout, restoring the previous single-value
	// behavior. See `sequenceIdleTimeout`.
	TcpSequenceIdleTimeout time.Duration
	WindowSizes            map[WindowType]WindowSizeSettings
	// StrictWindowSizeHardMax makes WindowSizeHardMax an admission ceiling as
	// well as a resize target. When a flow-carrying warned client cannot yet be
	// collapsed, a new identity is declined instead of temporarily growing the
	// live client/transport graph beyond the hard max. Replacements of an
	// existing identity remain allowed and existing flows are never torn down.
	//
	// The zero value preserves the server/default behavior, where availability
	// takes precedence over a transient ownership overshoot. Mobile low-memory
	// profiles opt in because their process has a strict memory envelope.
	StrictWindowSizeHardMax bool
	// ClientNackInitialLimit int
	// ClientNackMaxLimit int
	// ClientNackScale float64
	// ClientWriteTimeout time.Duration
	// SendTimeout time.Duration
	// WriteTimeout time.Duration
	SendRetryTimeout time.Duration
	// FormationPollTimeout is how often a flow with NO candidate clients at
	// all re-checks the window while it forms. Distinct from the ordinary
	// SendRetryTimeout retry (which paces re-races against candidates that
	// exist, including our benched fallback): while the window is empty there
	// is nothing to race and nothing to pace — only the wait for the first
	// client to land. Polling that wait at SendRetryTimeout (2s) meant the
	// first DNS+SYN of a fresh connect could sit up to 2s AFTER the first
	// client was already usable. 0 falls back to SendRetryTimeout, the
	// pre-change behavior. Ported as a concept from upstream main e05ecee's
	// formation fast-poll.
	FormationPollTimeout time.Duration
	// EncryptionCapabilityPrefilter, when true (default), fails a window
	// candidate immediately when the local client requires encryption
	// (`EncryptionModeRequired`) and the platform's out-of-band key API
	// reports the candidate has never published a client identity key — such
	// a peer can never complete the identity-verified handshake, so the ping
	// would only wait out `PingTimeout` against it. Fetch errors leave the
	// candidate to the ordinary ping evaluation: the prefilter only
	// accelerates certain failure, it never admits a candidate.
	EncryptionCapabilityPrefilter bool
	PingWriteTimeout              time.Duration
	CPingWriteTimeout             time.Duration
	CPingMaxByteCountPerSecond    ByteCount
	PingTimeout                   time.Duration
	CPingTimeout                  time.Duration
	CPingRestTimeout              time.Duration
	AckTimeout                    time.Duration
	BlackholeTimeout              time.Duration
	// BlackholeReceiveTimeout bounds the weaker of the two blackhole signals:
	// the provider is acknowledging our sends, so it is demonstrably alive,
	// but nothing has come back from the destination. That is ambiguous -- a
	// flow waiting on a slow origin looks identical to a provider whose
	// upstream is broken -- and removing an exit is destructive, killing every
	// flow pinned to it rather than just the quiet one. So it gets a longer
	// bar than BlackholeTimeout, which covers the unambiguous case of a
	// provider that has stopped acknowledging anything at all.
	//
	// On mainnet at 5s this fired 44 times out of 44 removals, roughly one
	// every 18s under load, against providers that were acking as much as 602
	// sends / 222KB. 0 disables the check.
	//
	// Bounded above by roughly StatsWindowDuration + StatsWindowBucketDuration
	// (~31s at production constants): the age this is compared against comes
	// from surviving stat buckets, and coalesceEventBuckets drops every bucket
	// older than StatsWindowDuration. A value at or above that ceiling never
	// fires and is silently equivalent to 0 -- no error, no log. The default
	// keeps headroom so a later reduction of StatsWindowDuration does not
	// quietly disable it; TestBlackholeReceiveTimeoutIsReachable fails loudly
	// if that margin is lost.
	BlackholeReceiveTimeout time.Duration
	BlackholeConnectTimeout time.Duration
	// BlackholeConnectComparativeTimeout is the shorter bar the no-receive-syn
	// branch fires at while the rest of the pool is demonstrably working.
	// Concept ported from upstream main e05ecee's
	// BlackholeConnectComparativeTimeout (theirs 10s against a 30s full bar,
	// keyed on any sibling with recent receive; ours takes the same idea onto
	// our gate machinery and requires TWO receiving siblings).
	//
	// The full BlackholeConnectTimeout (30s) is deliberately patient because
	// "syns out, none back" conflates a provider that cannot dial with a
	// destination that drops connections and with a merely slow one -- and in
	// two of those the exit is innocent. That patience is only warranted while
	// the evidence is ambiguous. When two OTHER exits in the pool are receiving
	// return traffic right now, the ambiguity is gone: the phone's uplink
	// delivers, the tunnel works, and this exit alone has established nothing.
	// Cutting it at the comparative bar recovers the flows waiting on it 20s
	// sooner.
	//
	// Every existing gate still applies unchanged -- the uplink gate, the
	// transport gate, the receive-clock rebase, and the flow-count quarantine
	// decision -- so this only moves WHEN the branch matures, never whether it
	// is admissible or what it costs. 0 disables the cut, restoring the single
	// BlackholeConnectTimeout bar. A value at or above BlackholeConnectTimeout
	// is a no-op for the same reason.
	BlackholeConnectComparativeTimeout time.Duration
	// UplinkStalenessGate is how long the whole tunnel may go without a single
	// provider-originated ingress packet before the receive-branch blackhole
	// verdicts are held as inadmissible. Those verdicts convict a provider on
	// silence, and silence is only evidence while the local uplink is known to
	// deliver: during a wifi/cellular migration nothing from any provider can
	// arrive, so every exit looks identically guilty. Measured on device, one
	// network migration executed 7 exits in 79s, every verdict
	// `no-receive-ack recv 0/0B` -- the providers were fine, the phone was
	// between networks. The gate also holds no-send-ack when the active carrier
	// is unreliable: transport-up then proves only write admission, not Pack or
	// Ack delivery. A reliable carrier, or a fresh uplink, keeps that verdict on
	// its own fast signal. The gate only applies while at least two channels are
	// actively talking (see sendingChannelCount), and stops applying after
	// uplinkStalenessMaxHold of continuous staleness so a genuinely dead
	// window can still be recycled. 0 disables the gate entirely.
	UplinkStalenessGate time.Duration
	// MaxFlowsPerExit bounds how many live flows may be pinned to one exit.
	//
	// Providers are split-tcp, so removing an exit destroys every flow on it.
	// Measured on device: of 14 removals in 28 minutes, 10 cost nothing and
	// one cost 157 connections at once -- a visible 15s stall. Blast radius,
	// not removal rate, is what a user actually feels.
	//
	// The cost is destination affinity: a site whose flows would have shared
	// an exit gets split across two once the first is full, so it sees more
	// than one egress ip. A real trade-off rather than a free win, which is
	// why it is tunable at runtime; 0 restores the previous unbounded
	// behavior.
	//
	// The default comes from observed recovery times, not from the flow counts
	// alone. Over two hours of real use, 21 teardowns carried
	//
	//	1 1 2 4 4 6 7 20 26 35 36 36 43 44 46 53 62 71 101 157 484
	//
	// flows, and the stalls reported against them were: 4-6 flows about 3-5s,
	// 44 about 15s, 157 about 15s, 484 about 35s. Recovery grows with the
	// count sublinearly, with a long plateau through the middle -- so the
	// useful target is keeping events down near the small end, where a hiccup
	// is a few seconds, rather than shaving the tail.
	//
	// A permissive bound misses this entirely. The median teardown here was 36
	// flows, so a cap of 64 would not have touched a single one of the 15s
	// stalls. 16 is chosen to land most events in the range that recovers in
	// a few seconds, accepting more affinity splitting as the price.
	//
	// The cap must never make a flow unroutable -- it bounds blast radius, it
	// is not admission control. When every candidate is full the flow is
	// placed anyway.
	MaxFlowsPerExit int

	// AffinityStickyPastCap exempts hard affinity-group inheritance from the
	// flow cap. It applies to explicit pins and legacy A/B runs with
	// FreshFlowAffinity enabled; ordinary production flows race and remain
	// capped normally.
	//
	// This exists because the cap veto was observed splitting a busy site's
	// egress ip exactly when the site was busiest: flow 17 of a video session
	// was refused its donor and raced onto a different exit, and services
	// that bind sessions or signed media urls to the client ip (video cdns
	// do) then rejected the strays. A site changing egress ip mid-session is
	// strictly worse for the user than an exit running long -- one is a
	// player error, the other is a number on the developer screen.
	//
	// false restores the veto, the A/B comparison point.
	AffinityStickyPastCap bool

	// FreshFlowAffinity lets an ordinary NEW flow directly inherit the exit of
	// another flow in the same IP/domain affinity group. Existing flows never
	// use this switch: their exact TCP/UDP tuple remains on its committed exit.
	// Explicit app/host pins also bypass it because the user selected stable
	// egress semantics for those flows.
	//
	// The production default is false. With hard inheritance disabled, each
	// fresh flow reaches the health- and performance-weighted provider race,
	// preventing one long-lived page connection from pinning every later media
	// connection to the same mediocre or challenged exit. DestinationAffinity
	// may remain enabled: its bounded groups are still useful measurement keys
	// and do not themselves assign an exit. True restores legacy hard site/IP
	// affinity for controlled A/B tests.
	FreshFlowAffinity bool

	// PerformanceAwareAffinity weights fresh TCP/443 placement using a bounded,
	// session-local peak rate from cumulative inner-TCP ACK progress. An
	// unmeasured provider starts at its advertised EstimatedBytesPerSecond (the
	// null hypothesis). The score removes materially slower candidates from a
	// fresh full-field race while preserving equal candidates and failing open.
	// When FreshFlowAffinity is explicitly enabled for an A/B run, the same
	// score also vetoes a materially slow live/history donor.
	//
	// This never moves an established flow, never inspects TLS plaintext, and
	// does not apply to explicit app pins. The measurement table is bounded and
	// expires quickly; false or stale evidence therefore cannot become a
	// permanent provider ban. False restores unconditional affinity.
	PerformanceAwareAffinity bool

	// MaxStickyFlowsPerExit is the steady-state bound for an exit that grows
	// past MaxFlowsPerExit through explicit or legacy hard affinity. It does not
	// split that affinity group: when the bound is exceeded, the shared flow
	// reaper retires only the oldest TCP flows that have been idle for at least
	// StickyFlowIdleTimeout. Active bursts may exceed the bound temporarily;
	// preserving an in-flight request is preferable to enforcing a numeric cap
	// by resetting useful work. 0 disables sticky-flow trimming.
	//
	// The separate bound is intentional. MaxFlowsPerExit=16 prevents an exit
	// from collecting new sites, while 64 leaves headroom for one H1-heavy
	// video constellation without retaining the 186-195 flows observed in the
	// CNN failure. Every replacement connection remains on the same provider,
	// so signed URLs and session IP binding remain valid.
	MaxStickyFlowsPerExit int

	// StickyFlowIdleTimeout is the minimum idle age before an excess sticky TCP
	// flow may be reset. 0 disables sticky-flow trimming even when the count is
	// configured.
	StickyFlowIdleTimeout time.Duration

	// ScoredAffinityDonor makes the learner load-bearing on the legacy hard
	// affinity path. It is inert for ordinary production flows while
	// FreshFlowAffinity is false, but remains available for controlled A/B runs
	// and still applies to explicit affinity groups.
	//
	// READ THIS WITH scoredPlacementReorder's header. That function scores
	// the RACE field, and the race is the LAST of four placement steps in
	// sendUpdate -- reached only when the flow's own hard affinity groups, its
	// app pin, and the destination bridge have all declined to donate. With
	// legacy FreshFlowAffinity enabled, most flows never get there. The step
	// that carries them in that mode,
	// inheritAffinityClient{4,6}WithLock, picked its donor by
	// `createTime.After(mostRecentCreateTime)`: pure recency, with no
	// quality signal of any kind. So the learner could observe everything
	// and steer nothing -- ProviderPriors.Bias had zero production callers
	// before this knob.
	//
	// On, the donor scan ranks eligible donors by Bias (the persisted
	// per-provider reward EWMA, less a conviction penalty) and falls back to
	// the recency rule on a tie. This is the FIRST production consumer of
	// the priors the reward tap has been accumulating.
	//
	// Why this is the safe place to put the learner, and raising
	// MultiRaceClientCount is not:
	//
	//   - This tiebreak only decides anything when a group ALREADY spans
	//     more than one exit, i.e. the group is already scattered. It
	//     chooses which exit a scattered group reconverges onto. It cannot
	//     break the one-egress-ip-per-site invariant, because the invariant
	//     is already broken in every case that reaches the comparison.
	//   - It removes no starvation guard. MaxFlowsPerExit (16, on by
	//     default) gates this path through clientAtFlowCapWithLock, so a
	//     well-scoring exit still stops collecting new sites at the cap.
	//     The wide race is the starvation guard on the RACE path, which is
	//     why narrowing that race to make the scorer matter would have
	//     traded a guard away. This trades nothing.
	//   - It costs no lock and no I/O at placement time. Bias is a map read
	//     plus arithmetic under ProviderPriors' own mutex; nothing here
	//     calls windowStatsWithCoalesce, whose "already called on this path"
	//     safety argument holds for the race and NOT for affinity.
	//
	// One behavior difference beyond ordering, on only: the legacy loop
	// checked recency BEFORE eligibility, so a donor older than the current
	// best never had affinityDonorEligible called on it and could not report
	// donorQuarantineScattered. Ranking requires evaluating every candidate,
	// so scattered is now observed on quarantined donors the legacy scan
	// skipped. That makes the G-1 scatter ledger MORE complete, never less;
	// with the knob off the legacy short-circuit is preserved exactly.
	ScoredAffinityDonor bool

	// QuarantineGroupFollow is retained for settings compatibility but no
	// longer admits a fresh flow to a quarantined donor. The CNN DASH failure
	// demonstrated that preserving the egress IP of a path already proven
	// receive-silent strands every new H1 handshake on that path. Existing
	// flows remain protected until the parent handles the sustained verdict;
	// new flows always scatter immediately.
	QuarantineGroupFollow bool

	// GroupFollowWindow is retained for settings compatibility with older
	// clients. Fresh flows no longer follow a quarantined exit, regardless of
	// this value.
	GroupFollowWindow time.Duration
	// DialFailureRerace, when a provider reports it could not open the
	// upstream for a new flow (see ipOosUnreachable's dial-failure use),
	// silently unbinds the flow and lets the application's own retransmit
	// race it onto another exit -- turning a 3-63s syn-backoff hang into
	// about one second. Off forwards the failure signal to the application
	// instead, which is visible but still fast.
	DialFailureRerace                         bool
	WindowResizeTimeout                       time.Duration
	StatsWindowGraceperiod                    time.Duration
	StatsWindowMaxEstimatedByteCountPerSecond ByteCount
	// StatsWindowMaxEffectiveByteCountPerSecondScale float32
	StatsWindowEntropy  float32
	WindowExpandTimeout time.Duration
	// WindowExpandBlockTimeout     time.Duration
	WindowExpandBlockCount int
	// WindowEnumerateEmptyTimeout time.Duration
	WindowEnumerateErrorTimeout time.Duration
	// WindowRateLimitBackoffMin / Max bound the retry cadence when the platform
	// answers a window generator call with a RATE LIMIT (429).
	//
	// WindowEnumerateErrorTimeout alone is a FIXED 1s retry with no jitter, and
	// it is applied per expander goroutine. That is a thundering herd by
	// construction: the window runs one expander per slot it wants to fill, so
	// the aggregate request rate scales linearly with WindowSizeMin, and once
	// the platform starts refusing, every expander re-asks every second
	// forever. Measured on a real client 2026-08-17: 3,726 429s over 39
	// minutes, climbing 327 -> 672 per minute as more slots entered the failing
	// loop, against a platform budget of 1000/min. Nothing in the loop backed
	// off, because classifyWindowFailure already recognised the 429 and the
	// verdict was recorded for the stall label and then discarded.
	//
	// Backoff is exponential from Min to Max, doubling per consecutive rate
	// limit, and JITTERED — jitter is not decoration here, since dozens of
	// expanders that all failed at the same instant would otherwise retry in
	// lockstep no matter how long the delay. Any success resets it.
	WindowRateLimitBackoffMin time.Duration
	WindowRateLimitBackoffMax time.Duration
	// WindowGeneratorTimeout bounds each generator call the window's
	// enumerate/expand machinery makes (NextDestinations, NewClientArgs,
	// NewClientArgsForDestination). The generator wraps a platform API that
	// is trusted to return; when it hangs instead, the enumerate goroutine
	// wedges and the window can never grow again. Past the deadline the call
	// is ABANDONED (the generator interface takes no context, so it cannot be
	// canceled) and treated as an error on the WindowEnumerateErrorTimeout
	// retry cadence; see windowGeneratorCall for the late-result contract.
	// 0 = no deadline, the pre-change trust-the-API behavior.
	// Ported as a concept from upstream main e05ecee's generator deadlines.
	WindowGeneratorTimeout time.Duration
	// IpFamilyStarvedRetryTimeout is how long a window waits before asking
	// the platform for a family again after a family-filtered discovery
	// returned nothing (IPV6.md B2). 0 uses the default of one minute.
	IpFamilyStarvedRetryTimeout time.Duration
	// WindowMaxScale              float64
	// WindowExpandMaxOvershotScale float64
	// WindowRevisitTimeout      time.Duration
	WindowExpandArgsTimeout   time.Duration
	StatsWindowDuration       time.Duration
	StatsWindowBucketDuration time.Duration
	StatsSampleWeightsCount   int
	// percentile
	StatsSourceCountSelection float64
	// lower affinity is more private
	// however, there may be some applications that assume the same ip across multiple connections
	// in those cases, we would need some small affinity
	// ClientAffinityTimeout time.Duration

	// time since first send to end the race, if no response
	MultiRaceSetOnNoResponseTimeout time.Duration
	// time after the first response to end the race
	MultiRaceSetOnResponseTimeout        time.Duration
	MultiRaceClientSentPacketMaxCount    int
	MultiRaceClientPacketMaxCount        int
	MultiRacePacketMaxCount              int
	MultiRaceClientEarlyCompleteFraction float32
	MultiRaceClientCount                 int

	StatsWindowMaxUnhealthyDuration  time.Duration
	StatsWindowWarnUnhealthyDuration time.Duration
	StatsWindowKeepUnhealthyDuration time.Duration
	// StatsWindowKeepHealthiestCount                 int
	// StatsWindowMinHealthyEffectiveSendByteCount    ByteCount
	// StatsWindowMinHealthyEffectiveReceiveByteCount ByteCount

	// active clients longer than this lifetime will not be forced closed
	// new connections will be routed to new clients
	// each channel actually uses a per-channel effective lifetime of
	// MaxClientLifetime x uniform(0.75, 1.0), drawn once at construction, so
	// channels created together do not all rotate in the same resize pass;
	// see jitterClientLifetime. 0 disables rotation entirely.
	MaxClientLifetime time.Duration

	ProtocolVersion int

	// DestinationAffinity creates bounded IP/domain grouping state. With the
	// default FreshFlowAffinity=false those groups are measurement keys only;
	// they do not assign ordinary new flows. Explicit pins and controlled
	// legacy A/B runs may still consume them as hard affinity.
	DestinationAffinity bool

	DefaultPerformanceProfile *PerformanceProfile

	// OverrideAllowDirect, when set, hard-overrides direct mode
	// (`AllowDirect`) no matter what performance profile is set, in either
	// direction, superseding the same-network force. Cloud hosted clients
	// set false because a direct connection would leak that the client is
	// hosted and where it is hosted: the host addresses appear in the
	// direct connection setup. true forces direct mode on regardless of
	// the profile. When unset, a trusted same-network peer connection
	// (provide mode Network) forces direct mode on; otherwise the
	// profile's own `AllowDirect` applies.
	OverrideAllowDirect *bool

	// used when reconnect scale is not set in a custom performance profile
	DefaultReconnectScale float64
	// used when ulimit is not set in a custom performance profile
	DefaultUlimit int

	TcpCollapsePrevention bool
	UdpCollapsePrevention bool
	// icmp echo egress. off by default until the provider fleet broadly
	// parses icmp: a not-yet-upgraded provider silently blackholes icmp
	// flows, and flow stickiness pins a ping run to its client (see ICMP.md)
	EnableIcmp bool
	// DegradedMode, when its value is true, reports that the HOST is in a
	// degraded-performance state (low power mode, thermal throttling, a weak
	// or constrained network). While set, the idle continuous-ping rest
	// scales by DegradedLivenessScale so an idle tunnel wakes the radio less
	// often. The conviction-path timings deliberately do NOT scale here: the
	// stall gate already requires sibling corroboration or a probe ack, which
	// self-calibrates to a slow host.
	DegradedMode *atomic.Bool
	// DegradedLivenessScale multiplies the idle continuous-ping rest while
	// DegradedMode is set.
	DegradedLivenessScale float64
	// RemovalReceiveQueueSize bounds best-effort packets synthesized while a
	// client is being removed (flow teardown resets). Delivery rides an
	// isolated worker so a suspended packet-flow/TUN consumer must not stop
	// window maintenance, and repeated removals must not grow memory without
	// bound.
	RemovalReceiveQueueSize int
	// PacketGroupMax* bounds one exact-flow ownership transaction after a
	// native packet batch is parsed. A nonpositive value preserves the legacy
	// unbounded-per-input-batch behavior. An individually oversized packet is
	// still admitted as a singleton.
	PacketGroupMaxPacketCount int
	PacketGroupMaxByteCount   ByteCount

	// TcpCollapseMaxHold bounds how long TcpCollapsePrevention may keep
	// discarding a sender's retransmits while the committed packet makes no
	// progress. After this long at the same sequence state, one retransmit is
	// admitted per window. 0 disables the bound, restoring the previous
	// behavior where retransmits were dropped until the client was declared
	// dead (up to AckTimeout). Ignored when TcpCollapsePrevention is off.
	TcpCollapseMaxHold time.Duration

	// SendStallTimeout is how long a client may hold outstanding sends without
	// acknowledging any of them before it is treated as failed and removed from
	// the window. A client in that state looks busy rather than broken, so
	// without this it survives until AckTimeout (30s) while every flow pinned to
	// it is frozen. 0 disables the check. See `sendStalled`.
	SendStallTimeout time.Duration

	// BusyProbe interposes an active liveness probe between the send-stall bar
	// tripping and the conviction it used to execute immediately. Concept
	// ported from upstream main e05ecee (their CPingBusyStaleTimeout /
	// busyStale / addBusyProbeAck), landed on OUR machinery: the trigger stays
	// our sendStalled bar and the verdict stays convictSendStalls, the probe
	// only decides whether that verdict is deserved.
	//
	// What it buys: a stalled exit and a merely CONGESTED exit are the same
	// observation -- bytes committed, no acks -- and today both are convicted
	// at the 3s bar. An exit whose upstream is saturated (or whose send window
	// is full of the very data the stall is about) answers a control ping in a
	// couple of rtts, because the ping rides the transport rather than the
	// blocked flow. Asking is cheap and the answer is decisive, so the fast
	// 3-4s rescue is kept without paying for it in false removals.
	//
	// The interposition is confined to the send-stall path. The no-send-ack
	// verdict, the transport-down hold, and the cping loop keep their exact
	// behavior -- see the busyLivenessProbe comment for why each of them must.
	//
	// false (the zero value) is today's behavior: the bar trips, the exit is
	// convicted, nothing is asked.
	BusyProbe bool
	// BusyProbeBudget is how long a busy probe waits for its ack before the
	// stall verdict stands. 0 derives max(1s, SendStallTimeout/2) -- 1.5s at
	// the shipped 3s bar -- which keeps total detection inside ~4.5s while
	// leaving a congested-but-alive exit several rtts to answer. Only consulted
	// when BusyProbe is on.
	BusyProbeBudget time.Duration

	// SchedulerPauseTolerance is how much later than armed a timer may fire
	// before the host is judged to have been suspended (doze, the app freezer,
	// thermal throttling, a laptop lid). Concept ported from upstream main
	// e05ecee's SchedulerPauseTolerance.
	//
	// A suspended host looks exactly like a dead network from inside this
	// process: no packets arrived, no acks landed, every clock aged. The uplink
	// gate already covers the network-migration case; this covers the other one
	// the gate cannot see, because during a suspend the gate's own evaluation
	// passes did not run either. 0 disables the detector entirely, which is the
	// pre-change behavior.
	SchedulerPauseTolerance time.Duration
	// SchedulerPauseRecoveryTimeout is how long after a detected suspend the
	// receive-branch verdicts stay held. The clocks are rebased to the resume
	// instant regardless; this is the additional grace during which evidence
	// collected across the suspend boundary is treated as inadmissible, so the
	// first verdict pass after wake cannot convict on a window whose history
	// was accumulated before the host stopped. 0 rebases without holding.
	SchedulerPauseRecoveryTimeout time.Duration

	// SoftVerdictDemote changes what a soft classification against a loaded
	// exit does. The core invariant it enforces: an exit carrying live flows
	// may only be closed on hard evidence -- transport dead, no-send-ack,
	// an honest send stall, a dead continuous ping, or explicit user action.
	// The soft signals (no-receive-ack, no-receive-syn, the stats-unhealthy
	// resize removal) convict on silence, and silence from a loaded exit is
	// ambiguous; executing on it destroys the exit's established flows, which
	// are its only working asset. With this on, those signals demote instead:
	// the exit is warned out of selection (no new flows), its established
	// flows keep running, and removal is deferred until it is flowless or the
	// same evidence has held continuously past
	// StatsWindowKeepUnhealthyDuration. false restores the pre-change
	// execute-immediately behavior, which is the A/B comparison point. See
	// `verdictAction` and the quarantine state on `multiClientChannel`.
	SoftVerdictDemote bool

	// RemovalBudgetCount and RemovalBudgetWindow are the verdict-removal
	// storm breaker: at most RemovalBudgetCount verdict-driven removals per
	// window per RemovalBudgetWindow, and past the budget a removal is
	// deferred (the client is warned and kept) instead of executed. A storm
	// of verdicts is far more likely to be one shared cause -- a network
	// migration convicting every exit on the same silence -- than that many
	// independent provider failures, and the deferral costs seconds while a
	// wrong mass execution costs every flow in the window. Exempt from the
	// budget: user action (DropExit), context-done/transport-dead cleanup,
	// lifetime drains, and capacity collapse -- the breaker only meters the
	// removals that a verdict argued for. RemovalBudgetCount 0 turns the
	// breaker off. See `verdictRemovalAllowed`.
	RemovalBudgetCount  int
	RemovalBudgetWindow time.Duration

	// EffectiveTierSelection makes new-flow selection rank exits by
	// effectiveTier() -- the platform's static Tier plus live demerits for
	// dial starvation (+2), an active or recently survived quarantine (+2),
	// and a currently-unhealthy stats window (+1) -- instead of the static
	// Tier alone. The static rank is the platform's measurement of latency
	// and speed, which is the right prior but a slow one: a tier-0 provider
	// whose upstream starts refusing dials keeps winning every race for the
	// full length of its rank advantage, and every new flow placed on it
	// burns a syn-backoff round before the re-race machinery rescues it.
	// Demerits are computed from the channel's own strike/quarantine/health
	// state, so a failing provider falls in the ranking within about one
	// selection pass (~1s) of the evidence landing, while promotion back is
	// deliberately slow: dial-strike demerits decay only with the 60s strike
	// window, and quarantine memory requires both a clean interval and a
	// proven connect success (see quarantineMemoryDuration). false restores
	// selection on the static Tier, the A/B comparison point.
	EffectiveTierSelection bool

	// MinBlackholeDestinations is how many distinct send destinations the
	// stats window must contain before the no-receive-ack blackhole verdict
	// is admissible. That verdict convicts an exit because sends are
	// acknowledged and nothing comes back -- but with traffic to a single
	// destination, "nothing comes back" is precisely what one dead or
	// silently-dropping website looks like, and removing the exit destroys
	// every flow pinned to it to punish a destination's silence. Requiring
	// at least 2 distinct destinations makes the verdict corroborated: two
	// unrelated destinations both silent through the same exit is evidence
	// about the exit. 0 or 1 restores the single-destination behavior, the
	// A/B comparison point. Only the no-receive-ack branch is gated; the
	// no-send-ack verdict is about the provider itself (nothing is
	// acknowledged at all) and stays as fast as it was.
	MinBlackholeDestinations int

	// BlackholeLoadCorroboration widens that corroboration with load: the
	// effective distinct-destination requirement is
	// max(MinBlackholeDestinations, flowCount/BlackholeLoadCorroboration).
	// A loaded exit has more in-flight questions and more ways to look
	// briefly silent -- the 2026-08-03 session benched five exits carrying
	// 22-24 flows each on soft receive evidence and acquitted every one --
	// so the busier the exit, the broader the silence must be before the
	// soft verdict is admissible. At the default 8, an exit with 24 flows
	// needs 3 silent destinations instead of 2; hard evidence paths are
	// untouched. 0 disables the scaling (the flat pre-change behavior), the
	// A/B comparison point.
	BlackholeLoadCorroboration int

	// ProviderProbe enables client-side provider qualification: a crafted tcp
	// syn (and one dns query) sent through an exit to real destinations, where
	// an ANSWER proves the provider completes real upstream dials and a
	// non-answer proves nothing at all. The asymmetry is the whole design --
	// probes qualify, they never convict -- and it is enforced structurally:
	// the probe send bypasses the window accounting entirely, probe flows never
	// enter the flow bookkeeping, and a probe failure touches no strike, no
	// verdict input and no metric. See ip_remote_multi_client_probe.go.
	//
	// false is the A/B comparison point and makes the mechanism inexistent
	// (probeExit returns an empty result without sending anything). true is the
	// default on this fork, and is inert on its own: something has to call
	// probeExit, and the sweep that does lands in the next package.
	ProviderProbe bool

	// ProbeTimeout bounds one probe pass. 0 falls back to the built-in 4s. It
	// bounds how long positive evidence is waited for; it is never a timer that
	// produces a verdict, because a pass that ends with nothing back leaves
	// every provider exactly where it was.
	ProbeTimeout time.Duration

	// ProbeSampleHostCount is how many health hosts one qualification pass asks
	// about. 0 or negative means the ENTIRE table every pass -- the
	// mainnet-aggressive default on this fork: a pass then answers "how much of
	// the internet does this provider reach" instead of "does it reach four
	// sites", at a cost of a few kilobytes of syns and resolution queries per
	// pass (all in flight together against the one ProbeTimeout, so a wide pass
	// costs no more wall time than a narrow one). A positive value narrows the
	// pass back to a rotating block of that many hosts; the rotation then walks
	// the whole table across passes (4 was the pre-change compact width).
	ProbeSampleHostCount int

	// ProbeSilenceWarnStreak is the placement compensation for provider-device
	// churn: after this many consecutive probe passes answered with TOTAL
	// silence -- zero stage-B answers and zero dns resolutions, so zero
	// evidence of life across ~all of the target table -- the exit is warned
	// out of new-flow selection (and the size math backfills a replacement),
	// exactly like a dial-starved exit. This is a PLACEMENT input only:
	// probes remain non-punitive for removal, which stays traffic-based, and
	// any evidence of life -- one probe answer, one dns resolution, one
	// received byte newer than the silence -- clears the streak and the
	// warning on the next pass.
	//
	// The field capture that motivates it (2026-08-04): providers on consumer
	// devices go completely silent mid-session (0 of 127 answers, repeated
	// for 25+ minutes) and sat in the window classified healthy -- idle stats
	// look fine -- until real app traffic bound to them and suffered the
	// ~10-30s of dead syns that convicts. This warns the corpse out of
	// placement between the first silent retests and that conviction.
	//
	// 0 is off (the pre-change behavior and the A/B comparison point).
	ProbeSilenceWarnStreak int

	// SharedFateMinExits and SharedFateWindow are the shared-fate verdict
	// gate: when at least SharedFateMinExits DISTINCT exits develop
	// silence/stall evidence inside one SharedFateWindow, the common cause is
	// overwhelmingly the shared path (the phone's access network wobbling,
	// bufferbloat, a handoff), not that many independent providers died in
	// the same seconds -- so DESTRUCTIVE verdicts hold while the correlation
	// stands. Benching (non-destructive) continues, receive progress still
	// acquits, and a genuinely dead exit still executes the first pass after
	// the correlation clears. The field captures that motivate it: mainnet
	// congestion waves benching 6 exits at once (2026-08-04), and a stable
	// pool where every stall conviction clustered with siblings' silence
	// (2026-08-05). SharedFateMinExits 0 or SharedFateWindow 0 is off.
	SharedFateMinExits int
	SharedFateWindow   time.Duration

	// EvaluationPoolMultiple is the aggressive-pooling knob: within the
	// four-client expansion-step cap, window expansion requests and
	// ping-evaluates this multiple of the candidates it needs, then admits only
	// the needed count -- preferring qualified providers (see poolAdmitOrder) --
	// and politely cancels the evaluated surplus, which carries no flows by
	// construction (an unadmitted candidate never enters the window, so
	// selection can never have placed a flow on it).
	//
	// The multiple applies to the candidate-request count, while both requests
	// and admissions are capped at four per pass. The window's size math -- the
	// demand target, standing reserve, and WindowSizeHardMax collapse -- remains
	// unchanged; a large deficit is filled over several resize passes instead
	// of one allocation spike. Pooling is skipped for fixed-destination
	// generators, whose destination set cannot produce useful surplus.
	//
	// 1 (and, via the zero-value ReliabilitySettings, 0) requests up to four
	// needed candidates. 2 is the mainnet-aggressive default on this fork.
	EvaluationPoolMultiple int

	// WindowOutcomeDeadline is the outcome deadline (window honesty, see
	// ip_remote_multi_client_outcome.go): a window that has Added ZERO
	// providers this long after it first tried to expand gets one automatic
	// silent rebuild — the programmatic form of the manual
	// disconnect+reconnect that reliably recovered the 2026-08-11 field hangs.
	// 0 disables both the rebuild and the failed latch below.
	WindowOutcomeDeadline time.Duration
	// WindowOutcomeRebuildDeadline is the second half: zero Added this long
	// after the automatic rebuild latches the terminal failed state (surfaced
	// to the app with the stall reason, rendered as a failure + Retry). The
	// machinery keeps running underneath, and a provider that lands later
	// clears the latch. 0 disables the failed latch only.
	WindowOutcomeRebuildDeadline time.Duration

	// ServerNameAffinityBridge lets a new flow whose own affinity group has no
	// donor inherit the client from the destination-scoped group an earlier
	// nameless flow to the same destination joined. Those groups are read, never
	// joined. off restores the previous behavior, where the first connection to
	// a site the mux resolved late stays stranded on a different exit from the
	// rest of the session. See `affinityFallbackIpPathsWithLock`.
	ServerNameAffinityBridge bool

	// ClusterAffinityFallback groups destination ips by their IpAssoc cluster
	// when no server name is known for them, so a site whose flows the dns mux
	// never saw resolved still pins to one client instead of splitting across
	// the window per ip. off restores plain per-ip affinity. Only consulted on
	// the no-server-name path; a known server name always wins.
	ClusterAffinityFallback bool

	// UdpTeardownSignal sends an icmp destination-unreachable to the source
	// when a udp flow is torn down, the way tcp flows already get a rst. off
	// restores the previous behavior, where a udp flow whose exit is removed
	// goes silent and stalls until the application times out. see
	// `ipOosUnreachable`.
	UdpTeardownSignal bool

	// QuicRebindOnExitLoss re-pins an established quic flow (udp/443 with
	// inbound data seen) to a live replacement exit inside the removal of its
	// dying exit, instead of tearing it down. Quic keys the connection on the
	// connection id rather than the 4-tuple (RFC 9000), so the server sees
	// the same connection arrive from the replacement's egress address and
	// path-validates it -- the flow survives the exit death, and the app's
	// very next packet already egresses through a warm exit. Without this an
	// established quic flow waits for its next app packet plus a full race
	// (~3.6s measured); quic is 56% of traffic, so this is most of what an
	// exit death costs. Tcp is excluded on purpose: providers are split-tcp,
	// the exit held the remote end of the connection, and there is nothing to
	// migrate -- fail-fast teardown remains correct there. false restores
	// teardown-for-everything, the A/B comparison point. See
	// `removeClient` and `rebindCandidates`.
	QuicRebindOnExitLoss bool

	// StandingReserve sizes each window one spare exit beyond its computed
	// target (bounded by WindowSizeHardMax), so a replacement for a failed or
	// draining exit is already connected when it is needed. Measured on
	// device, failover backfill took ~45s because replacement only started
	// after a loss was noticed: a resize tick to see the hole, a generator
	// round trip, a dial, and an evaluation ping all sat between the failure
	// and the first packet over the replacement. With a standing spare the
	// flows re-race onto an exit that already passed evaluation. The cost is
	// one idle provider connection per window. Not applied to
	// fixed-destination generators (their destination set cannot produce a
	// spare) or to a window whose resize is disabled (target 0). false
	// restores the previous exact-target sizing, the A/B comparison point.
	// See `standingReserveTarget`.
	StandingReserve bool

	// HeartbeatInterval is how often the multi-client logs one line summarizing
	// its live state (see runHeartbeat). 0 turns the heartbeat off entirely --
	// the goroutine is never started -- which is the pre-change behavior and,
	// via the zero-value ReliabilitySettings, what a developer menu writing an
	// override built from an older struct produces.
	//
	// It exists for post-hoc logcat forensics: without it, the shape of a
	// session (how many exits, how many proven, how many flows, what the
	// recovery machinery had done) lives only in the developer screen, which
	// nobody is looking at when the symptom happens and which is gone by the
	// time the capture is read. The beat is suppressed when nothing changed
	// since the previous one, so an idle session costs nothing.
	HeartbeatInterval time.Duration

	// Smart routing (Phase 1), all zero-value-off so an override from an older
	// struct keeps today's placement. ScoredPlacement is the master gate;
	// PlacementHysteresisPct==0 is plain greater-than; PlacementDemoteConsecutive<=1
	// acts on every sample; RewardInstrumentation==false emits no reward lines.
	ScoredPlacement            bool
	PlacementHysteresisPct     float64
	PlacementDemoteConsecutive int
	RewardInstrumentation      bool
	// LightClassifier installs the pure-Go light-tier FlowClassifier
	// (routing_classifier_light.go) at session construction, resolving
	// server names through the existing ServerNameLookup seam
	// (SetServerNameLookup / the mux's IP->hostname reverse index) -- see
	// maybeInstallLightClassifier. false (the zero value) leaves
	// flowClassifier nil exactly as before this knob existed:
	// classifyOrUnknown then always names ClassUnknown, so
	// scoredPlacementReorder stays a no-op even with ScoredPlacement on. On
	// its own (ScoredPlacement still off) this changes nothing observable
	// except the session banner and the sampled classify line.
	LightClassifier bool

	// Quarantine flap damping (Phase 1), zero-value-off like the rest of this
	// block. QuarantineDampening false leaves benchDuration returning the
	// constant StatsWindowKeepUnhealthyDuration it always has -- today's
	// behavior. QuarantineReentryRamp==0 disables the released-exit score
	// ramp entirely, so a lifted quarantine returns to full scored-placement
	// standing immediately, exactly as before this task. See benchDuration
	// and reentryScorePenalty.
	QuarantineDampening   bool
	QuarantineReentryRamp time.Duration

	SecurityPolicyGenerator func(context.Context, *SecurityPolicyStatsCollector) SecurityPolicy

	// the epoch for flushing block action and packet stats events to listeners
	EventEpoch time.Duration
	// how long a cached block action decision stays valid while the overrides
	// and cluster versions are unchanged (server names for a destination can drift)
	BlockActionDecisionTtl      time.Duration
	BlockActionDecisionMaxCount int
	// max distinct block actions aggregated per epoch
	BlockActionAggMaxCount int
	// nil disables activity association (`IpAssoc`)
	IpAssocSettings *IpAssocSettings

	RemoteUserNatMultiClientMonitorSettings
}

type WindowSizeSettings struct {
	WindowSizeMin int
	// the minimumum number of items in the windows that must be connected via p2p only
	WindowSizeMinP2pOnly int
	// WindowSizeMinIpv6Capable is the SOFT minimum of v6-capable exits
	// (dualstack or v6-only) an auto-sized window tries to hold (IPV6.md
	// B1). Like WindowSizeMinP2pOnly it raises the resize target by its
	// shortfall and steers admission, but it never changes the hard minimum,
	// so a window with no v6-capable provider in reach still reads as
	// connected. Fixed-size, fixed-profile and fixed-destination windows
	// ignore it.
	WindowSizeMinIpv6Capable int
	// inclusive
	WindowSizeMax     int
	WindowSizeHardMax int
	// leave 0 to automatically size between `WindowSizeMin` and `WindowSizeMax`
	FixedWindowSize int
	// WindowSizeUseMax     int
	// clients per source (leave 0 for default)
	WindowSizeReconnectScale float64
	KeepHealthiestCount      int
	// (leave 0 for default)
	Ulimit int
}

func (self *WindowSizeSettings) Validate() error {
	if self.WindowSizeMinIpv6Capable < 0 {
		return fmt.Errorf(
			"Window size min ipv6 capable =%d must be >= 0",
			self.WindowSizeMinIpv6Capable,
		)
	}
	if self.WindowSizeMax < self.WindowSizeMin {
		return fmt.Errorf(
			"Window size [%d, %d] invalid. Max must be >= min",
			self.WindowSizeMin,
			self.WindowSizeMax,
		)
	}

	if 0 < self.FixedWindowSize {
		if self.FixedWindowSize < self.WindowSizeMin || self.WindowSizeMax < self.FixedWindowSize {
			return fmt.Errorf(
				"Window size [%d, %d] must include the fixed size =%d",
				self.WindowSizeMin,
				self.WindowSizeMax,
				self.FixedWindowSize,
			)
		}
	}

	return nil
}

// not setting a performance profile, or setting one with `WindowTypeAuto`,
// uses the default "auto" mode which balances traffic across multiple window
// types with an internal set of profiles. In auto mode `WindowSize` is
// ignored; the orthogonal settings (`AllowDirect`,
// `PostQuantumEncryption`) still apply.
type PerformanceProfile struct {
	WindowType  WindowType
	WindowSize  WindowSizeSettings
	AllowDirect bool
	// enable the per-peer e2e encryption sessions (post-quantum key
	// exchange) on the window clients. Opportunistic: a provider that does
	// not support the sessions falls back to plaintext at this layer.
	PostQuantumEncryption bool
}

// FixedWindow returns the fixed window type and size when the profile fixes
// one. ok is false when the profile is nil or auto — the equivalent cases
// where each window uses its own default size settings and the profile
// `WindowSize` is ignored.
func (self *PerformanceProfile) FixedWindow() (windowType WindowType, windowSize WindowSizeSettings, ok bool) {
	if self == nil || self.WindowType == WindowTypeAuto {
		return WindowTypeAuto, WindowSizeSettings{}, false
	}
	return self.WindowType, self.WindowSize, true
}

func (self *PerformanceProfile) Validate() error {
	err := self.WindowSize.Validate()
	if err != nil {
		return err
	}

	return nil
}

func DefaultWindowSizeSettings() WindowSizeSettings {
	return WindowSizeSettings{
		WindowSizeMin:            1,
		WindowSizeMax:            1,
		WindowSizeHardMax:        4,
		WindowSizeReconnectScale: 1.0,
		KeepHealthiestCount:      1,
	}
}

type receivePacket struct {
	Source      TransferPath
	ProvideMode protocol.ProvideMode
	IpPath      *IpPath
	Packet      []byte
	Pooled      bool
	// tcpControl preserves the wire-direction flags and sequence numbers that
	// IpPath.Reverse intentionally drops before application delivery.
	tcpControl tcpControlObservation
}

type tcpControlObservation struct {
	sequenceNumber    uint32
	ackSequenceNumber uint32
	payloadByteCount  uint32
	ack               bool
	fin               bool
	rst               bool
	valid             bool
}

type pendingIngressTcpControl struct {
	update  *multiClientChannelUpdate
	control tcpControlObservation
}

func tcpControlFromIpPath(ipPath *IpPath) tcpControlObservation {
	if ipPath == nil || ipPath.Protocol != IpProtocolTcp {
		return tcpControlObservation{}
	}
	return tcpControlObservation{
		sequenceNumber:    ipPath.SequenceNumber,
		ackSequenceNumber: ipPath.AckSequenceNumber,
		payloadByteCount:  uint32(max(0, ipPath.TcpPayloadByteCount)),
		ack:               ipPath.Ack,
		fin:               ipPath.Fin,
		rst:               ipPath.Rst,
		valid:             true,
	}
}

type RemoteUserNatMultiClient struct {
	ctx    context.Context
	cancel context.CancelFunc

	generator MultiClientGenerator

	// ipv6NoRouteCount counts v6 flows answered with no-route because no
	// v6-capable exit existed (IPV6.md B6); see replyIpv6NoRoute.
	ipv6NoRouteCount atomic.Uint64

	// receivePacketCallback delivers to the app through an atomic holder so
	// the owner can be swapped or retired at runtime: later deliveries
	// observe nil and stop retaining/calling the retired owner.
	receivePacketCallback atomic.Pointer[receivePacketCallbackHolder]
	// receivePacketsCallback, when set, takes the committed-flow fast path's
	// packets as one batch per client receive dispatch (see
	// clientReceivePackets). Rare paths (uncommitted flows, races, probes)
	// continue through the per-packet path.
	receivePacketsCallback atomic.Pointer[receivePacketsCallbackHolder]

	// Malformed/unsupported packets and policy failures are untrusted,
	// potentially high-rate input. Keep separate lifetime counters and emit
	// only power-of-two summaries so the drop path has fixed memory and
	// bounded logging work while retaining first-error and total visibility.
	sendParseDropCount        atomic.Uint64
	sendPolicyDropCount       atomic.Uint64
	sendSmtpDropCount         atomic.Uint64
	sendIcmpDisabledDropCount atomic.Uint64
	// Best-effort removal-generated packets are delivered by one isolated
	// worker. A permanently blocked downstream therefore cannot wedge the
	// maintenance paths; the fixed queue caps retained packets and memory.
	removalReceiveQueue     chan receivePacket
	removalReceiveDropCount atomic.Uint64
	// flowReaperWake drives one parent-level idle-flow reaper. A buffered edge
	// is sufficient: activity can only move an existing deadline later, while
	// creating a flow is the only operation that can introduce an earlier one.
	// Keeping this at the parent replaces one sleeping goroutine and timer per
	// flow, which otherwise makes short-lived web traffic grow the tunnel's
	// runtime footprint for the full (intentionally long) TCP idle timeout.
	flowReaperWake chan struct{}

	settings *MultiClientSettings
	log      Logger
	// defaultReliabilitySettings is the immutable projection of settings used
	// when no live override is installed. The packet and watchdog hot paths ask
	// for this frequently; rebuilding the projection on every read creates a
	// steady stream of short-lived configuration objects.
	defaultReliabilitySettings *ReliabilitySettings

	windows map[WindowType]*multiClientWindow
	monitor MultiClientMonitor

	securityPolicyStats *SecurityPolicyStatsCollector
	securityPolicy      SecurityPolicy
	smtpEgressGuard     smtpEgressGuard
	egressIpv4Fragments ipv4FragmentGate

	// the provide mode of the source packets
	// for locally generated packets this is `ProvideMode_Network`
	provideMode protocol.ProvideMode

	stateLock      sync.Mutex
	ip4PathUpdates map[Ip4Path]*multiClientChannelUpdate
	ip6PathUpdates map[Ip6Path]*multiClientChannelUpdate
	// flowUpdates owns every ordinary flow generation until the shared reaper
	// finishes it. It is separate from the path maps because a completed
	// generation can be replaced at the same tuple before the reaper runs; the
	// old generation still needs its client/affinity cleanup without deleting
	// the replacement. Provider probes deliberately stay out and retain their
	// explicit unregister lifecycle.
	flowUpdates      map[*multiClientChannelUpdate]bool
	affinityIp4Paths map[Ip4Path]map[Ip4Path]time.Time
	affinityIp6Paths map[Ip6Path]map[Ip6Path]time.Time
	clientUpdates    map[*multiClientChannel]map[*multiClientChannelUpdate]bool
	// qualification is the provider-qualification table: what each provider's
	// probes have proven, keyed by destination so it survives the channel
	// incarnations that come and go for one provider. Guarded by stateLock
	// (created lazily, so a fixture-assembled parent works), bounded by
	// qualificationMaxEntries. See ip_remote_multi_client_probe.go.
	qualification map[MultiHopId]*providerQualification
	// destinationServiceFailures is passive, destination+port-scoped negative
	// memory. It makes an unanswered/rejected connect re-race onto a different
	// exit without treating one site's egress policy as a provider-wide fault.
	// Guarded by stateLock; see ip_remote_multi_client_service_failure.go.
	destinationServiceFailures map[destinationServiceFailureKey]destinationServiceFailure
	// dnsExitHints maps a canonical server-name affinity group to the exit
	// that delivered its tunneled DoH answer. Guarded by stateLock; see
	// ip_remote_multi_client_dns_affinity.go.
	dnsExitHints        map[string]dnsExitHint
	dnsAddressExitHints map[netip.Addr]dnsExitHint
	// demotionStates is scoredPlacementReorder's owned N-of-M demotion
	// bookkeeping (routing_score.go's demotionState), keyed per (exit,
	// class) by demotionKey. Guarded by stateLock, created lazily like
	// qualification above. Unlike qualification, an entry does NOT survive a
	// channel's teardown -- removeClient evicts every entry for a departing
	// ClientId, so this cannot grow unbounded across a long session's churn.
	// See demotionObserve and demotionKey's own doc for the keying rationale.
	demotionStates map[demotionKey]*demotionState

	// affinityPerformance is the bounded session-local per-(site/IP,provider)
	// completed TCP ACK-rate evidence used to weight a fresh provider race (and
	// to veto a legacy hard donor in A/B mode). Its leaf lock is never held
	// across parent/window locks or I/O. The map stays nil until the first
	// completed measured flow; still-open evidence uses clientUpdates directly
	// and retains no second index.
	affinityPerformanceLock sync.Mutex
	affinityPerformance     map[affinityPerformanceKey]*affinityPerformanceEntry

	// config is an immutable snapshot of the rarely-changed routing config
	// (performance profile + local security bypass). it is rebuilt under
	// stateLock by the setters and read lock-free by selectWindowTypes, the
	// affinity selection, and the SendPacket drop path.
	config atomic.Pointer[multiClientConfig]

	// reliability holds runtime overrides for the reliability knobs, so the
	// developer menu can A/B a live freeze without a rebuild. Unset means "use
	// the values in settings", which is what every non-overridden client does.
	// Read on the packet hot path, so an atomic rather than a lock.
	reliability atomic.Pointer[ReliabilitySettings]
	// reliabilitySettingsLock serializes SetReliabilitySettings callers against
	// each other -- NOT against readers, and NEVER taken on the placement path
	// (reliabilitySettings() stays a bare atomic load, exactly as before). It
	// exists solely because LightClassifier's edge-trigger reads `before`,
	// stores, and reads `after` as three separate steps around the single
	// atomic `reliability` swap: two concurrent SetReliabilitySettings callers
	// can otherwise each compute their edge decision from their OWN
	// before/after pair, so the caller whose store lands SECOND in
	// `reliability` is not guaranteed to be the caller whose flowClassifier
	// install/clear runs last -- the settings and the classifier can disagree
	// about which caller "won". A leaf lock: only ever held across a settings
	// store, a classifier install/clear (SetFlowClassifier, itself lock-free),
	// and a handful of log lines, all inside SetReliabilitySettings; a
	// developer-menu action is rare enough that serializing it costs nothing.
	reliabilitySettingsLock sync.Mutex

	localUserNat      *LocalUserNat
	localUserNatUnsub func()

	// nil when `IpAssocSettings` is not set
	ipAssoc *IpAssoc
	// immutable snapshot of the compiled overrides, swapped by `SetBlockActionOverrides`
	blockActionState atomic.Pointer[blockActionState]
	blockActionCache *blockActionCache

	// the G-4b flow-owner seam: the platform's resolver for "which pinned
	// app owns this flow", with its per-flow-key answer cache. Zero values
	// are fully inert (nil func = no app pinning), so no constructor wiring
	// is required and bare fixtures are unaffected. flowOwnerLock guards the
	// two maps only -- it is NEVER held across the resolver call, and it is
	// a leaf: nothing else is taken under it.
	flowOwnerFunc       atomic.Pointer[FlowOwnerLookupFunc]
	flowOwnerGeneration atomic.Uint64
	flowOwnerLock       sync.Mutex
	flowOwner4          map[Ip4Path]flowOwnerEntry
	flowOwner6          map[Ip6Path]flowOwnerEntry
	// the smart-routing classifier seam (Phase 1): the platform's traffic
	// classifier, installed by SetFlowClassifier. Mirrors flowOwnerFunc
	// exactly -- atomic pointer, nil clears, one-branch nil cost paid via
	// classifyOrUnknown (routing_class.go). Zero value is fully inert: no
	// constructor wiring is required and bare fixtures are unaffected.
	// Consulted ONLY from the guarded scored-placement path (see
	// scoredPlacementEnabled); with that gate off, or with no classifier
	// installed, nothing in this file's new routing code runs.
	flowClassifier atomic.Pointer[FlowClassifier]
	// rewardLock is the leaf lock guarding reward and providerPriors below --
	// never the parent stateLock, on the same reasoning as
	// reliabilitySettingsLock: the reward tap runs on the flow-close path
	// (recordFlowReward) at flow-completion frequency, and giving it its own
	// lock keeps that path off the parent lock entirely rather than adding
	// contention to the hottest lock in the file. Only ever held across a map
	// insert, an EWMA update, or a Snapshot copy -- never across I/O or
	// another lock acquisition.
	rewardLock sync.Mutex
	// reward is the Phase 0 per-(class,exit) accumulator (routing_reward.go),
	// created lazily on the first recorded sample. RewardInstrumentation's
	// zero value must be fully inert -- see recordFlowReward's gate -- so
	// this stays nil, not merely empty, until a sample is actually recorded.
	reward *rewardAccumulator
	// providerPriors is the in-memory EWMA/conviction store
	// (routing_priors.go) that recordFlowReward's interval fold
	// (foldRewardAndPersist) writes into and scoredPlacementReorder's bias
	// consult (a later task) would read from. Created lazily, same
	// zero-value-off discipline as reward above.
	providerPriors *ProviderPriors
	// priorsStore is the persistence seam for providerPriors: the
	// platform-supplied dot-file-backed implementation, installed via
	// SetPriorsStore. Mirrors flowClassifier exactly -- atomic pointer, nil
	// clears, nil-safe throughout. nil (a bare client, or one never wired to
	// a platform store) means in-memory-only priors, matching PriorsStore's
	// own doc.
	priorsStore atomic.Pointer[PriorsStore]
	// Nil test seams make whole-group candidate admission deterministic without
	// changing production selection or adding synchronization to the send path.
	groupRaceCandidatesForTest func(*parsedPacketGroup) []*multiClientChannel
	sendClientPathForTest      func(*IpPath, flowPin, func(*multiClientChannelUpdate, *multiClientChannel))
	// appPinClients is the cross-version half of an app pin: the exit an
	// app's flows are currently placed on, keyed by app id. The affinity
	// groups are per-ip-version by construction (separate path maps), so a
	// dual-stack app would otherwise land one exit for v4 and another for
	// v6 -- two egress ips, the exact failure pinning exists to prevent.
	// Consulted when the version's own app group has no donor. Guarded by
	// the parent stateLock.
	appPinClients        map[string]*multiClientChannel
	blockActionCollector *blockActionCollector
	// immutable snapshot of the compiled ignore host values,
	// swapped by `SetBlockActionIgnoreHosts`
	blockActionIgnoreState atomic.Pointer[blockActionIgnoreState]
	blockActionIgnoreCache *blockActionIgnoreCache
	// unsubscribe from the current server-name-lookup's learned notifications
	// (guarded by stateLock); see SetServerNameLookup
	serverNamesLearnedUnsub func()
	packetStatsCounters     *packetStatsCounters
	packetStatsCallbacks    *CallbackList[PacketStatsFunction]

	// reliabilityMetrics measures what a provider failure actually costs the
	// user -- how many flows die with an exit, and how long the destinations
	// they served stay unreachable. The reliability knobs above are only
	// A/B-testable against a number, and this is the number.
	reliabilityMetrics *reliabilityMetrics

	// rebindCandidatesFunc, when non-nil, overrides how removeClient gathers
	// the replacement candidates for the proactive quic rebind (see
	// rebindCandidates). This is a test seam: the default gather reads the
	// live windows, which a fixture-built parent does not have, and the
	// rebind choreography -- candidate re-validation, cap headroom, affinity
	// cohesion -- is exactly what needs testing without transports. The
	// returned list must be ordered most-preferred first and must not be
	// produced while holding the parent stateLock. Production leaves it nil.
	rebindCandidatesFunc func(dying *multiClientChannel) []*multiClientChannel

	// probePassFunc, when non-nil, overrides how ProbeAllExits runs one
	// scheduled pass. A test seam in the same spirit as rebindCandidatesFunc:
	// the property that matters about ProbeAllExits is that it SCHEDULES rather
	// than runs -- a pass waits on the network for seconds and the caller is a
	// ui thread -- and proving that needs a pass that blocks on command, which
	// a real one cannot be made to do without transports. Production leaves it
	// nil, and the real pass (probeProviderPass) is used.
	probePassFunc func(client *multiClientChannel)

	// uplinkLastIngressNanos is the unix-nano time the local uplink last
	// proved it delivers: the last provider-originated packet to arrive, or
	// the last intercepted dial-failure icmp (not a receive-ack, but it did
	// arrive). Stamped on the packet hot path, so an atomic, and coarsened to
	// uplinkStampCoarseness so a download does not rewrite it per packet. See
	// stampUplinkIngress and uplinkGate.
	uplinkLastIngressNanos atomic.Int64

	// the uplink staleness epoch, maintained lazily by uplinkGate as the
	// channels' verdict passes call it. uplinkStaleSince is set while the
	// tunnel is observed stale (zero while fresh); uplinkFreshSince is when
	// the last stale epoch ended (zero if never stale), which is what the
	// receive-verdict clocks rebase from. Guarded by uplinkStateLock, a leaf
	// lock: uplinkGate takes it with no other lock held.
	uplinkStateLock  sync.Mutex
	uplinkStaleSince time.Time
	uplinkFreshSince time.Time
	// one log line per epoch when the hold cap expires, not one per pass
	uplinkCapLogged bool
	// schedulerPauseHoldUntil is when the current scheduler-pause recovery
	// window ends (zero when none is open). Set by notifySchedulerPause and
	// read by uplinkGate, which reports it as ordinary uplink staleness so the
	// suspend case reuses the migration case's whole hold-and-rebase path
	// rather than growing a second one. Guarded by uplinkStateLock alongside
	// the epoch fields it rides with.
	schedulerPauseHoldUntil time.Time
}

// MultiClientMemorySnapshot is an allocation-free primitive view for a
// low-frequency mobile memory sampler. It deliberately contains counts only:
// callers that need identities or status objects use Exits/Monitor instead.
type MultiClientMemorySnapshot struct {
	QualityClientCount int
	SpeedClientCount   int
	FlowCount          int

	// Aggregate existing per-Client Transfer counters without installing a
	// packet observer. These distinguish a full receive/ACK handoff from
	// upstream route variance in physical low-memory measurements.
	PackHandoffDropCount            uint64
	PackHandoffDropByteCount        uint64
	PackHandoffWaitCount            uint64
	PackHandoffWaitSuccess          uint64
	PackHandoffMaxCount             uint64
	PackHandoffMaxByteCount         uint64
	PackHandoffSaturationCount      uint64
	PackHandoffDepthGrowCount       uint64
	PackHandoffDeepenedFlows        uint64
	PackHandoffAdaptiveMaxDepth     uint64
	PackHandoffAdaptiveMaxByteCount uint64
	AckHandoffDropCount             uint64
	AckHandoffQueueFullCount        uint64
	AckHandoffMissCount             uint64
	AckHandoffWaitCount             uint64
	AckHandoffWaitSuccess           uint64
	AckRouteWriteCount              uint64
	AckRoutePriorityWriteCount      uint64
	AckRouteWriteBlockedCount       uint64
	AckRouteWriteErrorCount         uint64
	AckRouteWriteWaitNanos          uint64
	AckRouteWriteMaxWaitNanos       uint64
	InitialWriteCount               uint64
	InitialFrameCount               uint64
	InitialMessageByteCount         uint64
	TimeoutResendWriteCount         uint64
	AckPendingResendPreemptCount    uint64
	CarrierChangeWriteCount         uint64
	SelectiveGapWriteCount          uint64
	AckTailProbeWriteCount          uint64
	CumulativeProbeWriteCount       uint64
	RecoveryWriteErrorCount         uint64
}

func addMultiClientTransferMemorySnapshot(
	snapshot *MultiClientMemorySnapshot,
	window *multiClientWindow,
	clientCount *int,
) {
	if window == nil {
		return
	}
	window.stateLock.Lock()
	*clientCount = len(window.clients)
	for _, channel := range window.clients {
		if channel == nil || channel.client == nil {
			continue
		}
		receive := channel.client.ReceiveStats()
		snapshot.PackHandoffDropCount += receive.PackHandoffDropCount
		snapshot.PackHandoffDropByteCount += receive.PackHandoffDropByteCount
		snapshot.PackHandoffWaitCount += receive.PackHandoffWaitCount
		snapshot.PackHandoffWaitSuccess += receive.PackHandoffWaitSuccess
		snapshot.PackHandoffMaxCount = max(
			snapshot.PackHandoffMaxCount,
			receive.PackHandoffMaxCount,
		)
		snapshot.PackHandoffMaxByteCount = max(
			snapshot.PackHandoffMaxByteCount,
			receive.PackHandoffMaxByteCount,
		)
		snapshot.PackHandoffSaturationCount += receive.PackHandoffSaturationCount
		snapshot.PackHandoffDepthGrowCount += receive.PackHandoffDepthGrowCount
		snapshot.PackHandoffDeepenedFlows += receive.PackHandoffDeepenedFlows
		snapshot.PackHandoffAdaptiveMaxDepth = max(
			snapshot.PackHandoffAdaptiveMaxDepth,
			receive.PackHandoffAdaptiveMaxDepth,
		)
		snapshot.PackHandoffAdaptiveMaxByteCount = max(
			snapshot.PackHandoffAdaptiveMaxByteCount,
			receive.PackHandoffAdaptiveMaxByteCount,
		)
		snapshot.AckHandoffDropCount += receive.AckHandoffDropCount
		snapshot.AckHandoffQueueFullCount += receive.AckHandoffQueueFullCount
		snapshot.AckHandoffMissCount += receive.AckHandoffMissCount
		snapshot.AckHandoffWaitCount += receive.AckHandoffWaitCount
		snapshot.AckHandoffWaitSuccess += receive.AckHandoffWaitSuccess
		snapshot.AckRouteWriteCount += receive.AckRouteWriteCount
		snapshot.AckRoutePriorityWriteCount += receive.AckRoutePriorityWriteCount
		snapshot.AckRouteWriteBlockedCount += receive.AckRouteWriteBlockedCount
		snapshot.AckRouteWriteErrorCount += receive.AckRouteWriteErrorCount
		snapshot.AckRouteWriteWaitNanos += uint64(receive.AckRouteWriteWaitDuration)
		snapshot.AckRouteWriteMaxWaitNanos = max(
			snapshot.AckRouteWriteMaxWaitNanos,
			uint64(receive.AckRouteWriteMaxWait),
		)
		recovery := channel.client.SendRecoveryStats()
		snapshot.InitialWriteCount += recovery.InitialWriteCount
		snapshot.InitialFrameCount += recovery.InitialFrameCount
		snapshot.InitialMessageByteCount += recovery.InitialMessageByteCount
		snapshot.TimeoutResendWriteCount += recovery.TimeoutResendWriteCount
		snapshot.AckPendingResendPreemptCount += recovery.AckPendingResendPreemptCount
		snapshot.CarrierChangeWriteCount += recovery.CarrierChangeWriteCount
		snapshot.SelectiveGapWriteCount += recovery.SelectiveGapWriteCount
		snapshot.AckTailProbeWriteCount += recovery.AckTailProbeWriteCount
		snapshot.CumulativeProbeWriteCount += recovery.CumulativeProbeWriteCount
		snapshot.RecoveryWriteErrorCount += recovery.RecoveryWriteErrorCount
	}
	window.stateLock.Unlock()
}

// MemorySnapshot samples the current connected topology without constructing
// ExitInfo, ProviderEvent, or monitor maps. The windows map is immutable after
// construction; each mutable count is read under its owning lock and no two
// locks are held together.
func (self *RemoteUserNatMultiClient) MemorySnapshot() MultiClientMemorySnapshot {
	if self == nil {
		return MultiClientMemorySnapshot{}
	}
	var snapshot MultiClientMemorySnapshot
	addMultiClientTransferMemorySnapshot(
		&snapshot,
		self.windows[WindowTypeQuality],
		&snapshot.QualityClientCount,
	)
	addMultiClientTransferMemorySnapshot(
		&snapshot,
		self.windows[WindowTypeSpeed],
		&snapshot.SpeedClientCount,
	)
	self.stateLock.Lock()
	snapshot.FlowCount = len(self.flowUpdates)
	self.stateLock.Unlock()
	return snapshot
}

// ServerNameLookup resolves a destination IP to the server name(s) previously observed
// for it — e.g. a DNS upgrade mux that recorded which hostnames resolved to the IP. The
// multi-client uses it for ServerName-based path affinity, so flows to the same site
// share a client channel even when the SNI is not visible on the wire (point 4).
type ServerNameLookup interface {
	ServerNames(ip string) []string
}

// ServerNamesLearnedFunction is called with the ips for which a new server name
// was just learned.
type ServerNamesLearnedFunction func(addrs []netip.Addr)

// ServerNamesLearnedNotifier is an optional capability of a ServerNameLookup: it
// notifies when a new server name is learned for an ip (e.g. an out-of-band DNS
// resolution after a flow already started). The multi-client uses it to
// invalidate that ip's cached block-action decision so subsequent block actions
// report the server name instead of the ip — we prefer the server name wherever
// possible. A lookup that doesn't implement it simply reconciles on the ttl.
type ServerNamesLearnedNotifier interface {
	AddServerNamesLearnedCallback(callback ServerNamesLearnedFunction) func()
}

type multiClientConfig struct {
	performanceProfile  *PerformanceProfile
	localSecurityBypass bool
	serverNameLookup    ServerNameLookup
	// the ad/tracker blocker consulted in the egress decision (nil = none)
	blocker Blocker
}

// FlowOwnerLookupFunc resolves the PINNED app that owns a flow, or "" for
// none -- "none" covers both "no app pin rules exist" and "the owning app is
// not pinned", which the caller cannot and need not distinguish. Called once
// per new flow on the egress path and cached per flow key, so an
// implementation may cross into platform code (on android, one
// ConnectivityManager.getConnectionOwnerUid binder call).
type FlowOwnerLookupFunc func(ipPath *IpPath) string

// flowOwnerEntry is one cached lookup answer, tagged with the lookup
// generation that produced it so an answer landing after a rule change (the
// resolver runs unlocked) cannot resurrect a removed pin.
type flowOwnerEntry struct {
	appId      string
	generation uint64
}

// flowOwnerCacheMaxCount bounds the cache. Entries are never refreshed (see
// flowOwnerAppId), so this counts distinct flow keys seen since the last
// reset, and the reset is wholesale -- the cache must never become the
// memory story of a long session, and a precise LRU would cost more than the
// lookup it protects.
const flowOwnerCacheMaxCount = 8192

// SetFlowOwnerLookup installs (or, with nil, removes) the platform's
// flow-owner resolver. Safe at runtime: the generation bump invalidates every
// cached answer, including any in flight, so a changed pin-rule set takes
// effect on the next new flow.
func (self *RemoteUserNatMultiClient) SetFlowOwnerLookup(lookup FlowOwnerLookupFunc) {
	// the wiring's proof of life. Per-app pinning crosses three languages
	// and a platform api before it can do anything, and without this line a
	// field capture cannot distinguish "no apps pinned" from "the lookup
	// never reached the go side" -- the standing rule of this work is that a
	// mechanism with no observable signal does not exist.
	loggerOrDefault(self.log).Infof("%s\n", relEvent(
		"pin_lookup",
		"installed", lookup != nil,
	))

	// the two locks are taken in SEPARATE sections, never nested:
	// flowOwnerLock is documented as a leaf, and nesting the parent lock
	// under it here would be the only place in this file that inverts the
	// hierarchy. Neither section depends on the other's result.
	func() {
		self.flowOwnerLock.Lock()
		defer self.flowOwnerLock.Unlock()

		if lookup == nil {
			self.flowOwnerFunc.Store(nil)
		} else {
			self.flowOwnerFunc.Store(&lookup)
		}
		self.flowOwnerGeneration.Add(1)
		self.flowOwner4 = map[Ip4Path]flowOwnerEntry{}
		self.flowOwner6 = map[Ip6Path]flowOwnerEntry{}
	}()

	// the recorded app placements describe the pin set being replaced
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.clearAppPinsWithLock()
}

// SetFlowClassifier installs (or, with nil, removes) the smart-routing
// traffic classifier. Mirrors SetFlowOwnerLookup: a bare atomic store, safe
// at runtime and on a bare client. Installing a classifier does not, by
// itself, change placement -- scoredPlacementEnabled must also be true (see
// the guarded branch in sendPacket's placement helper), so this is inert
// until both are true, matching every other nil-func convention in this
// file.
func (self *RemoteUserNatMultiClient) SetFlowClassifier(c FlowClassifier) {
	loggerOrDefault(self.log).Infof("%s\n", relEvent(
		"flow_classifier",
		"installed", c != nil,
	))
	self.setFlowClassifierUnlogged(c)
}

// setFlowClassifierUnlogged performs the raw flowClassifier swap without
// SetFlowClassifier's own log line. It exists for SetReliabilitySettings,
// which must perform this swap INSIDE reliabilitySettingsLock (see that
// function) so the classifier install/clear lands atomically-as-a-unit with
// the settings store, but must NOT hold that lock across a logging call --
// logging is I/O-adjacent and this lock's whole contract is that nothing
// blocking runs under it. SetReliabilitySettings logs the same
// "flow_classifier" event itself, after releasing the lock.
func (self *RemoteUserNatMultiClient) setFlowClassifierUnlogged(c FlowClassifier) {
	if c == nil {
		self.flowClassifier.Store(nil)
	} else {
		self.flowClassifier.Store(&c)
	}
}

// SetPriorsStore installs (or, with nil, removes) the provider-priors
// persistence seam (the sdk's dot-file-backed implementation in production).
// Mirrors SetFlowClassifier: a bare atomic store, safe at runtime and on a
// bare client.
//
// Installing a non-nil store immediately calls Load() and seeds
// providerPriors from whatever was last persisted, so a restart resumes with
// history instead of starting every provider neutral. This is the one place
// in the reward path that may do real I/O (a dot-file read) -- deliberately
// an explicit, one-time setup call, like SetServerNameLookup, and never
// reachable from the flow-close or placement path. An empty or absent
// Load() leaves providerPriors exactly as it was (nil until the first real
// sample), so calling this before any reward has ever been recorded costs no
// allocation.
func (self *RemoteUserNatMultiClient) SetPriorsStore(store PriorsStore) {
	loggerOrDefault(self.log).Infof("%s\n", relEvent(
		"priors_store",
		"installed", store != nil,
	))

	if store == nil {
		self.priorsStore.Store(nil)
		return
	}
	self.priorsStore.Store(&store)

	loaded := store.Load()
	if len(loaded) == 0 {
		return
	}
	self.rewardLock.Lock()
	defer self.rewardLock.Unlock()
	if self.providerPriors == nil {
		self.providerPriors = NewProviderPriors()
	}
	self.providerPriors.Load(loaded)
}

// providerIdentity returns the exit's STABLE provider identity -- the
// destination tail, exactly what removeProvider/RemoveProvider already key
// on under the name egressClientId -- NEVER the channel's own ClientId.
//
// ClientId is an ephemeral, locally-minted per-window-slot id: the api
// generator mints a fresh one (with a fresh instance id) on essentially
// every channel (re)connect (see ip_remote_multi_client_identity.go's own
// doc), and the default generator installs no identity store to make it
// stable even across a within-process reconnect. Keying persisted priors on
// it would silently orphan a provider's whole history on the next demotion,
// blackhole replacement, or migration BACK to the very same provider -- not
// merely across a process restart, which is the failure routing_priors.go's
// own doc warns about ("one provider IDENTITY, never an exit instance").
//
// false when the channel carries no destination yet (a bare fixture, or a
// channel torn down before it ever dialed) -- recording under a zero
// identity would silently pool every such flow's reward into one bogus
// entry, which is worse than dropping the sample.
func providerIdentity(client *multiClientChannel) (Id, bool) {
	if client == nil || client.args == nil {
		return Id{}, false
	}
	destination := client.args.Destination
	if destination.Len() == 0 {
		return Id{}, false
	}
	// The length check above already proves Tail's precondition. Read the
	// comparable value directly so the compiler does not materialize Tail's
	// defensive panic error on this placement-hot path.
	return destination.ids[destination.len-1], true
}

// recordFlowReward is the reward tap's per-flow half: called once a flow has
// actually finished (sendUpdate's per-flow idle-timeout teardown goroutine,
// both ip versions -- see the flow-close path there), with no lock held. It
// stays as cheap as a placement decision -- a classify lookup, a metrics
// snapshot taken through the non-coalescing accessor (see exitMetricsSnapshot;
// it is NOT side-effect-free, only a weaker duplicate of a call already made
// on this path) via its own
// contract, and a map insert under a leaf lock -- never I/O, never a
// blocking call, matching the same discipline scoredPlacementReorder's
// demotionObserve call already established.
//
// RewardInstrumentation's zero value is fully inert: off, this records
// nothing and allocates nothing -- the accumulator is created lazily, only
// once a sample is actually about to be recorded, so a build that never
// turns the knob on never pays for it. This is also the ONLY place that
// reads RewardInstrumentation for the tap (the sendUpdate call sites below
// no longer pre-check it -- one read, not three). class is derived exactly
// the way scoredPlacementReorder derives it (classifyOrUnknown over the same
// flowClassifier seam), so a flow's reward is filed under the same class its
// placement decision used. flows is the caller's own flow-count read (the
// same contract exitMetricsSnapshot documents), never re-read here.
func (self *RemoteUserNatMultiClient) recordFlowReward(ipPath *IpPath, appId string, client *multiClientChannel, flows int) {
	if !self.reliabilitySettings().RewardInstrumentation {
		return
	}
	providerId, ok := providerIdentity(client)
	if !ok {
		return
	}

	var classifier FlowClassifier
	if p := self.flowClassifier.Load(); p != nil {
		classifier = *p
	}
	class := classifyOrUnknown(classifier, ipPath, appId).Class

	metrics := exitMetricsSnapshot(client, flows)
	stallFree := metrics.StallEvents == 0

	self.rewardLock.Lock()
	defer self.rewardLock.Unlock()
	if self.reward == nil {
		self.reward = newRewardAccumulator()
	}
	self.reward.add(class, providerId.String(), metrics.GoodputBytesPerSec, stallFree)
}

// foldRewardAndPersist is the reward tap's interval-triggered half: it folds
// whatever recordFlowReward has accumulated since the last tick into
// providerPriors, logs the same interval as [rel] event=reward lines, and --
// only if a store is installed -- persists the coarse ProviderPrior snapshot.
// Called from runHeartbeat's own wake cadence rather than a new goroutine, so
// persistence runs on a timer, never at flow-close frequency: draining the
// accumulator every beat is what keeps a busy session from writing the
// dot-file on every flow teardown.
//
// An empty interval (nothing recorded since the last tick, the common case
// with RewardInstrumentation off, or an idle session with it on) costs one
// lock acquisition and nothing else -- no log line, no Snapshot copy, no
// Save call -- the same "idle session contributes nothing" discipline the
// heartbeat line itself uses.
//
// No lock is held across the log calls or the Save call: the fold, the
// drain, and the Snapshot copy all happen inside one short rewardLock
// section, and the store's Save (the only I/O here) runs after that section
// returns.
func (self *RemoteUserNatMultiClient) foldRewardAndPersist() {
	if !self.reliabilitySettings().RewardInstrumentation {
		return
	}

	var lines []string
	var snapshot map[string]ProviderPrior
	haveSnapshot := false

	func() {
		self.rewardLock.Lock()
		defer self.rewardLock.Unlock()

		if self.reward == nil || len(self.reward.m) == 0 {
			return
		}
		if self.providerPriors == nil {
			self.providerPriors = NewProviderPriors()
		}
		self.reward.foldInto(self.providerPriors, time.Now().Unix())
		lines = self.reward.drainLines()
		snapshot = self.providerPriors.Snapshot()
		haveSnapshot = true
	}()

	for _, line := range lines {
		loggerOrDefault(self.log).Infof("%s\n", line)
	}

	if !haveSnapshot {
		return
	}
	storePtr := self.priorsStore.Load()
	if storePtr == nil {
		return
	}
	if err := (*storePtr).Save(snapshot); err != nil {
		loggerOrDefault(self.log).Infof("%s\n", relEvent(
			"priors_persist",
			"ok", false,
			"err", err,
		))
	}
}

// serverNameResolver adapts the multi-client's live ServerNameLookup (see
// SetServerNameLookup and the config's serverNameLookup field) to the
// LightClassifier's ServerNameResolver shape, for the server-name tier of
// the light classifier.
//
// It reads self.config on every call rather than closing over a snapshot
// taken at install time. That matters because of construction order: at
// session construction the config's ServerNameLookup is nil (see
// NewRemoteUserNatMultiClient) -- the real lookup, an *UpgradeMux, is wired
// afterward by SetServerNameLookup. A resolver that captured the lookup once
// at install time would stay forever blind to it; reading self.config here
// means the classifier immediately sees a lookup installed or swapped in
// later, with no reinstall required.
//
// Table lookup only, no I/O: config.Load() is an atomic read and
// ServerNameLookup.ServerNames (the UpgradeMux's reverseIndex.serverNames)
// is a map lookup under its own leaf lock. Safe to call inline on the
// placement path at new-flow frequency, per LightClassifier's contract.
func (self *RemoteUserNatMultiClient) serverNameResolver(ip netip.Addr) (string, bool) {
	config := self.config.Load()
	if config == nil || config.serverNameLookup == nil {
		return "", false
	}
	names := config.serverNameLookup.ServerNames(ip.String())
	if len(names) == 0 {
		return "", false
	}
	// the reverse index keeps the names it has seen for an ip in no
	// particular scoring order; the first is as good a signal as any -- the
	// classifier only needs one name to match a suffix table entry.
	return names[0], true
}

// maybeInstallLightClassifier installs the pure-Go light-tier classifier
// (routing_classifier_light.go, Task 1) when settings.LightClassifier is on,
// through the existing SetFlowClassifier seam. false (the zero value) is a
// no-op: flowClassifier stays nil, exactly as before this knob existed.
//
// Extracted from NewRemoteUserNatMultiClient as its own method so the
// install site is directly unit-testable (construct a bare client, flip the
// knob, call this, assert on flowClassifier) without needing a full session
// -- windows, goroutines, a live generator -- that the constructor also
// stands up.
//
// This is only the CONSTRUCTION-time install: it runs once, from
// NewRemoteUserNatMultiClient, for a session that starts with the knob
// already on. A later runtime toggle -- SetReliabilitySettings flipping
// LightClassifier at 0->1 or 1->0 on a live session -- does NOT come back
// through here; SetReliabilitySettings installs or clears the classifier
// itself (via setFlowClassifierUnlogged, under reliabilitySettingsLock) so
// the toggle is real rather than merely changing what the banner reports.
// See SetReliabilitySettings for that path and why it needs its own lock.
func (self *RemoteUserNatMultiClient) maybeInstallLightClassifier() {
	if self.settings == nil || !self.settings.LightClassifier {
		return
	}
	self.SetFlowClassifier(NewLightClassifier(self.serverNameResolver))
}

// flowOwnerAppId answers "which pinned app owns this flow" -- from the cache
// when the key has been seen, else from the platform lookup.
//
// Runs on the egress path OUTSIDE every lock (the resolver crosses into
// platform code; on android that is a binder round trip), and the answer is
// consumed only at flow CREATION. Two consequences shape this:
//
//   - a cached answer is never refreshed. The answer's only consumer is the
//     flow that does not exist yet, so re-asking for a long-lived flow would
//     burn a platform call per key per ttl and discard every result. Entries
//     live until the wholesale reset.
//   - the lookup is only reached when a resolver is installed, which the
//     platform does only while at least one app is pinned. A user who pins
//     nothing pays nothing.
//
// Typed maps rather than sync.Map: the keys are comparable structs, and
// boxing one into `any` would allocate on the per-packet path this file
// otherwise keeps allocation-free.
func (self *RemoteUserNatMultiClient) flowOwnerAppId(ipPath *IpPath, lookup FlowOwnerLookupFunc) string {
	generation := self.flowOwnerGeneration.Load()

	var ip4Key Ip4Path
	var ip6Key Ip6Path
	switch ipPath.Version {
	case 4:
		ip4Key = ipPath.ToIp4Path()
	case 6:
		ip6Key = ipPath.ToIp6Path()
	default:
		return ""
	}

	if appId, ok := func() (string, bool) {
		self.flowOwnerLock.Lock()
		defer self.flowOwnerLock.Unlock()
		var entry flowOwnerEntry
		var ok bool
		if ipPath.Version == 4 {
			entry, ok = self.flowOwner4[ip4Key]
		} else {
			entry, ok = self.flowOwner6[ip6Key]
		}
		return entry.appId, ok && entry.generation == generation
	}(); ok {
		return appId
	}

	// unlocked: the resolver may block (binder), and holding a lock across it
	// would put every other flow behind it
	appId := lookup(ipPath)

	self.flowOwnerLock.Lock()
	defer self.flowOwnerLock.Unlock()
	if generation != self.flowOwnerGeneration.Load() {
		// the rules changed while this answer was in flight: it describes a
		// pin set that no longer exists, so it is dropped rather than cached
		return ""
	}
	if flowOwnerCacheMaxCount <= len(self.flowOwner4)+len(self.flowOwner6) {
		self.flowOwner4 = map[Ip4Path]flowOwnerEntry{}
		self.flowOwner6 = map[Ip6Path]flowOwnerEntry{}
	}
	entry := flowOwnerEntry{appId: appId, generation: generation}
	if ipPath.Version == 4 {
		self.flowOwner4[ip4Key] = entry
	} else {
		self.flowOwner6[ip6Key] = entry
	}
	return appId
}

// pinnedFollowWindowMultiple scales the ordinary GroupFollowWindow for a
// pinned flow: a pin asks for stability through a bench episode, not through
// an exit's whole decline. Unbounded following would be self-defeating --
// verdictAction only executes a soft verdict against a FLOWLESS exit and
// quarantineVacated only releases one, so an endless stream of pinned flows
// keeps a benched exit both un-executed and un-released (review finding,
// 2026-08-03). 3x outlives the false-positive benches the follow exists for
// while still letting a genuinely failing exit drain.
const pinnedFollowWindowMultiple = 3

// appAffinityName is the affinity group name for a pinned app. The "app:"
// prefix cannot collide with an eTLD+1 -- ':' is not legal in a domain.
func appAffinityName(appId string) string {
	return "app:" + appId
}

// pinnedFollowWindow scales the configured window for pinned flows; a
// non-positive configured window still means "follow disabled".
func pinnedFollowWindow(window time.Duration) time.Duration {
	if window <= 0 {
		return 0
	}
	return pinnedFollowWindowMultiple * window
}

// appPinDonorWithLock is the cross-version half of an app pin: the exit the
// app's flows are currently on, whichever ip version placed them. Returns nil
// when the app has no placement or the placement is no longer donatable.
//
// called with stateLock
func (self *RemoteUserNatMultiClient) appPinDonorWithLock(
	appId string,
	follow bool,
	window time.Duration,
	sticky bool,
) *multiClientChannel {
	if appId == "" || self.appPinClients == nil {
		return nil
	}
	client := self.appPinClients[appId]
	if client == nil {
		return nil
	}
	if client.IsDone() {
		delete(self.appPinClients, appId)
		return nil
	}
	// the same cap rule the in-version inherit applies: with sticky affinity
	// off, a full exit stops taking the app's growth here too
	if !sticky && self.clientAtFlowCapWithLock(client) {
		return nil
	}
	switch client.affinityDonorEligible(follow, window) {
	case donorEligible, donorQuarantineFollowed:
		return client
	}
	return nil
}

// recordAppPinWithLock remembers where an app's flows are placed, so the
// other ip version converges on the same exit. Called from every placement
// path -- the in-version group, the cross-version donor, and (through
// bindClientFlow) the async race, which is the one that places an app's
// FIRST flow of each ip version and therefore the one that makes dual-stack
// convergence work at all.
//
// called with stateLock
func (self *RemoteUserNatMultiClient) recordAppPinWithLock(appId string, client *multiClientChannel) {
	if appId == "" || client == nil || client.IsDone() {
		return
	}
	if self.appPinClients == nil {
		self.appPinClients = map[string]*multiClientChannel{}
	}
	self.appPinClients[appId] = client
}

// clearAppPinsWithLock drops every recorded app placement. Called when the
// pin rules change: the records describe a pin set that no longer exists,
// and holding them would both mis-converge a re-pinned app and keep a strong
// reference to a dead channel for the process lifetime.
//
// called with stateLock
func (self *RemoteUserNatMultiClient) clearAppPinsWithLock() {
	self.appPinClients = nil
}

// AppPin is one pinned app's current placement, for the readout.
type AppPin struct {
	AppId    string
	ClientId Id
}

// AppPins reports where each pinned app's flows are currently placed -- the
// field answer to "did the pin engage, and is the app on one exit". Empty
// while nothing is pinned or before a pinned app has opened a flow. Also
// folded into the heartbeat as the pins= count.
func (self *RemoteUserNatMultiClient) AppPins() []*AppPin {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	appPins := make([]*AppPin, 0, len(self.appPinClients))
	for appId, client := range self.appPinClients {
		if client == nil {
			continue
		}
		appPins = append(appPins, &AppPin{AppId: appId, ClientId: client.ClientId()})
	}
	slices.SortFunc(appPins, func(a, b *AppPin) int {
		return strings.Compare(a.AppId, b.AppId)
	})
	return appPins
}

func NewRemoteUserNatMultiClientWithDefaults(
	ctx context.Context,
	generator MultiClientGenerator,
	receivePacketCallback ReceivePacketFunction,
	provideMode protocol.ProvideMode,
) *RemoteUserNatMultiClient {
	return NewRemoteUserNatMultiClient(
		ctx,
		generator,
		receivePacketCallback,
		provideMode,
		DefaultMultiClientSettings(),
	)
}

func NewRemoteUserNatMultiClient(
	ctx context.Context,
	generator MultiClientGenerator,
	receivePacketCallback ReceivePacketFunction,
	provideMode protocol.ProvideMode,
	settings *MultiClientSettings,
) *RemoteUserNatMultiClient {
	cancelCtx, cancel := context.WithCancel(ctx)

	log := loggerOrDefault(settings.Log)

	securityPolicyStats := DefaultSecurityPolicyStatsCollector()

	localUserNatSettings := DefaultLocalUserNatSettings()
	// no ulimit for local traffic
	localUserNatSettings.UdpBufferSettings.UserLimit = 0
	localUserNatSettings.TcpBufferSettings.UserLimit = 0
	localUserNatSettings.Log = log
	localUserNat := NewLocalUserNat(cancelCtx, "multi local", localUserNatSettings)

	multiClient := &RemoteUserNatMultiClient{
		ctx:                        cancelCtx,
		cancel:                     cancel,
		log:                        log,
		generator:                  generator,
		settings:                   settings,
		defaultReliabilitySettings: ReliabilitySettingsFrom(settings),
		windows:                    map[WindowType]*multiClientWindow{},
		securityPolicyStats:        securityPolicyStats,
		securityPolicy:             settings.SecurityPolicyGenerator(cancelCtx, securityPolicyStats),
		provideMode:                provideMode,
		ip4PathUpdates:             map[Ip4Path]*multiClientChannelUpdate{},
		ip6PathUpdates:             map[Ip6Path]*multiClientChannelUpdate{},
		flowUpdates:                map[*multiClientChannelUpdate]bool{},
		affinityIp4Paths:           map[Ip4Path]map[Ip4Path]time.Time{},
		affinityIp6Paths:           map[Ip6Path]map[Ip6Path]time.Time{},
		clientUpdates:              map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
		qualification:              map[MultiHopId]*providerQualification{},
		destinationServiceFailures: map[destinationServiceFailureKey]destinationServiceFailure{},
		localUserNat:               localUserNat,
		blockActionCache:           newBlockActionCache(settings.BlockActionDecisionTtl, settings.BlockActionDecisionMaxCount),
		blockActionCollector:       newBlockActionCollector(settings.BlockActionAggMaxCount, log),
		blockActionIgnoreCache:     newBlockActionIgnoreCache(settings.BlockActionDecisionTtl, settings.BlockActionDecisionMaxCount),
		packetStatsCounters:        &packetStatsCounters{},
		packetStatsCallbacks:       NewCallbackList[PacketStatsFunction](),
		reliabilityMetrics:         newReliabilityMetrics(),
		flowReaperWake:             make(chan struct{}, 1),
	}
	if settings.IpAssocSettings != nil {
		multiClient.ipAssoc = NewIpAssoc(cancelCtx, settings.IpAssocSettings)
	}
	effectivePerformanceProfile := multiClient.overrideAllowDirect(settings.DefaultPerformanceProfile)
	multiClient.config.Store(&multiClientConfig{
		performanceProfile:  effectivePerformanceProfile,
		localSecurityBypass: false,
		serverNameLookup:    nil,
		blocker:             nil,
	})
	multiClient.blockActionState.Store(&blockActionState{
		version: 0,
		matcher: nil,
	})
	multiClient.blockActionIgnoreState.Store(&blockActionIgnoreState{
		version: 0,
		matcher: nil,
	})

	// the smart-routing classifier install site (Phase 2 Task 2): a no-op
	// when settings.LightClassifier is off, the zero value and every current
	// build's default.
	multiClient.maybeInstallLightClassifier()

	multiClient.SetReceivePacketCallback(receivePacketCallback)

	multiClient.windows[WindowTypeQuality] = newMultiClientWindow(
		cancelCtx,
		cancel,
		generator,
		multiClient.clientReceivePacket,
		multiClient.clientReceivePackets,
		provideMode == protocol.ProvideMode_Network,
		multiClient.clientDialFailure,
		multiClient.securityPolicy,
		multiClient.removeClient,
		WindowTypeQuality,
		effectivePerformanceProfile,
		settings,
		multiClient.reliabilitySettings,
		multiClient.uplinkGate,
		multiClient.reliabilityMetricsRef,
		multiClient.clientFlowCount,
		multiClient.providerQualified,
		multiClient.receivingChannelCount,
		multiClient.recordProbePass,
	)
	multiClient.windows[WindowTypeQuality].clientMigrateFunc = multiClient.migrateClientFlows
	if _, fixed := generator.FixedDestinationSize(); !fixed {
		multiClient.windows[WindowTypeSpeed] = newMultiClientWindow(
			cancelCtx,
			cancel,
			generator,
			multiClient.clientReceivePacket,
			multiClient.clientReceivePackets,
			provideMode == protocol.ProvideMode_Network,
			multiClient.clientDialFailure,
			multiClient.securityPolicy,
			multiClient.removeClient,
			WindowTypeSpeed,
			effectivePerformanceProfile,
			settings,
			multiClient.reliabilitySettings,
			multiClient.uplinkGate,
			multiClient.reliabilityMetricsRef,
			multiClient.clientFlowCount,
			multiClient.providerQualified,
			multiClient.receivingChannelCount,
			multiClient.recordProbePass,
		)
		multiClient.windows[WindowTypeSpeed].clientMigrateFunc = multiClient.migrateClientFlows
	}
	// else only keep the quality window for fixed destination

	multiClient.localUserNatUnsub = localUserNat.AddReceivePacketCallback(multiClient.localReceivePacket)

	monitors := []MultiClientMonitor{}
	for _, window := range multiClient.windows {
		monitors = append(monitors, window.monitor)
	}
	multiClient.monitor = NewMergedMultiClientMonitor(monitors)

	// the session banner (P3): one default-level line naming the build and the
	// complete effective reliability configuration, emitted before any other
	// line this client will produce. Every capture is then self-describing --
	// which A/B arm was running is answered by the log itself instead of by
	// asking the owner what was toggled that day.
	multiClient.logSessionBanner()

	removalReceiveQueueSize := settings.RemovalReceiveQueueSize
	if removalReceiveQueueSize <= 0 {
		removalReceiveQueueSize = 256
	}
	multiClient.removalReceiveQueue = make(chan receivePacket, removalReceiveQueueSize)
	go HandleError(multiClient.runRemovalReceive)
	go HandleError(multiClient.runFlowReaper)
	go HandleError(multiClient.runEventEpoch, cancel)

	// the state heartbeat (P3). Gated on the constructed interval -- a client
	// built with 0 never runs the goroutine -- while the runtime toggle is
	// honored per beat inside. Not wired to `cancel`, for the same reason as
	// the prober and the pause detector below: a goroutine that only describes
	// the tunnel must never be able to tear it down.
	if 0 < settings.HeartbeatInterval {
		go HandleError(multiClient.runHeartbeat)
	}

	// the provider-qualification prober (F-2): startup sweep, joiner probes,
	// staleness re-probes. Gated on the constructed setting -- a client built
	// with ProviderProbe off never runs the goroutine at all -- while the
	// runtime toggle is honored per scan inside. Not wired to `cancel`: the
	// prober is an optional aide, and its failure must never tear down the
	// tunnel (HandleError still logs the panic).
	if settings.ProviderProbe {
		go HandleError(multiClient.runProber)
	}

	// the scheduler-pause detector (P2). Gated on the constructed setting -- a
	// client built with a zero tolerance never runs the goroutine -- while the
	// runtime toggle is honored per pass inside. Not wired to `cancel` for the
	// same reason as the prober: an exculpatory aide must never be able to tear
	// down the tunnel it exists to protect.
	if 0 < settings.SchedulerPauseTolerance {
		go HandleError(multiClient.runSchedulerPauseDetector)
	}

	return multiClient
}

// flushes block action and packet stats events to listeners on the event epoch
func (self *RemoteUserNatMultiClient) runEventEpoch() {
	defer self.cancel()

	var lastPacketStats *PacketStats
	for {
		select {
		case <-self.ctx.Done():
			return
		case <-time.After(self.settings.EventEpoch):
		}

		self.blockActionCollector.flush()

		if callbacks := self.packetStatsCallbacks.Get(); 0 < len(callbacks) {
			packetStats := self.packetStatsCounters.snapshot()
			if !packetStatsEqual(packetStats, lastPacketStats) {
				lastPacketStats = packetStats
				for _, callback := range callbacks {
					HandleError(func() {
						callback(packetStats)
					})
				}
			}
		}
	}
}

// the local user nat receive callback. return traffic for locally routed flows
func (self *RemoteUserNatMultiClient) localReceivePacket(
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	packet []byte,
) {
	self.packetStatsCounters.localIngressPacketCount.Add(1)
	self.packetStatsCounters.localIngressByteCount.Add(int64(len(packet)))
	if self.ipAssoc != nil && !self.blockActionIgnored(ipPath) {
		// the local user nat delivers the flow's egress-oriented path
		// (the remote endpoint is the destination)
		self.ipAssoc.AddEgressPacket(ipPath)
	}
	self.deliverReceivePacket(source, provideMode, ipPath, packet)
}

func (self *RemoteUserNatMultiClient) SecurityPolicyStats(reset bool) SecurityPolicyStats {
	return self.securityPolicyStats.Stats(reset)
}

func (self *RemoteUserNatMultiClient) Monitor() MultiClientMonitor {
	return self.monitor
}

func (self *RemoteUserNatMultiClient) AddContractStatusCallback(contractStatusCallback ContractStatusFunction) func() {
	subs := []func(){}
	for _, window := range self.windows {
		sub := window.AddContractStatusCallback(contractStatusCallback)
		subs = append(subs, sub)
	}
	return func() {
		for _, sub := range subs {
			sub()
		}
	}
}

// AddContractStatsCallback registers a listener for the epoch contract stats
// events of all the window clients (see `ContractManager.AddContractStatsCallback`)
func (self *RemoteUserNatMultiClient) AddContractStatsCallback(contractStatsCallback ContractStatsFunction) func() {
	subs := []func(){}
	for _, window := range self.windows {
		sub := window.AddContractStatsCallback(contractStatsCallback)
		subs = append(subs, sub)
	}
	return func() {
		for _, sub := range subs {
			sub()
		}
	}
}

// AddPeerIdentityChangeCallback registers a listener fired whenever any
// window client's established + identity-verified peer set may have changed
// (see `EncryptionSessionManager.AddPeerIdentityChangeCallback`). Consumers
// re-read `PeerIdentities`.
func (self *RemoteUserNatMultiClient) AddPeerIdentityChangeCallback(callback func()) func() {
	subs := []func(){}
	for _, window := range self.windows {
		sub := window.AddPeerIdentityChangeCallback(callback)
		subs = append(subs, sub)
	}
	return func() {
		for _, sub := range subs {
			sub()
		}
	}
}

// PeerIdentities returns the peers with an established, identity-verified
// e2e session across all window clients, deduplicated by peer id.
func (self *RemoteUserNatMultiClient) PeerIdentities() []*PeerIdentity {
	byPeer := map[Id]*PeerIdentity{}
	for _, window := range self.windows {
		for _, clientChannel := range window.unorderedClients() {
			for _, peerIdentity := range clientChannel.client.EncryptionSessionManager().PeerIdentities() {
				if _, ok := byPeer[peerIdentity.PeerId]; !ok {
					byPeer[peerIdentity.PeerId] = peerIdentity
				}
			}
		}
	}
	out := make([]*PeerIdentity, 0, len(byPeer))
	for _, peerIdentity := range byPeer {
		out = append(out, peerIdentity)
	}
	return out
}

// overrideAllowDirect applies the allow-direct override chain to the
// profile. A trusted same-network peer connection (provide mode Network)
// always enables direct mode, superseding the profile, so the connection can
// upgrade to a direct p2p stream. An explicit `settings.OverrideAllowDirect`
// then supersedes everything — false is the cloud-hosted hard limit (a
// direct connection would leak that the client is hosted and where), true
// forces direct mode on regardless of the profile.
func (self *RemoteUserNatMultiClient) overrideAllowDirect(performanceProfile *PerformanceProfile) *PerformanceProfile {
	if self.provideMode == protocol.ProvideMode_Network {
		performanceProfile = forceAllowDirect(performanceProfile, true)
	}
	if self.settings.OverrideAllowDirect != nil {
		performanceProfile = forceAllowDirect(performanceProfile, *self.settings.OverrideAllowDirect)
	}
	return performanceProfile
}

// forceAllowDirect returns a profile with `AllowDirect` forced to the value,
// fabricating a profile when forcing on with none set. The input profile is
// never mutated in place.
func forceAllowDirect(performanceProfile *PerformanceProfile, allowDirect bool) *PerformanceProfile {
	if performanceProfile == nil {
		if !allowDirect {
			// direct mode is already off with no profile
			return nil
		}
		return &PerformanceProfile{
			WindowType:  WindowTypeAuto,
			AllowDirect: true,
		}
	}
	if performanceProfile.AllowDirect == allowDirect {
		return performanceProfile
	}
	overridden := *performanceProfile
	overridden.AllowDirect = allowDirect
	return &overridden
}

// performanceProfilesEqual reports whether two profiles produce the same
// window behavior: nil and an auto profile are equivalent when their
// orthogonal flags are false, and auto ignores WindowSize. This distinction
// matters because presentation code commonly reconstructs an equivalent
// value when it resumes; treating the fresh pointer as a settings change
// unnecessarily retires every healthy window client.
func performanceProfilesEqual(a *PerformanceProfile, b *PerformanceProfile) bool {
	allowDirect := func(profile *PerformanceProfile) bool {
		return profile != nil && profile.AllowDirect
	}
	postQuantumEncryption := func(profile *PerformanceProfile) bool {
		return profile != nil && profile.PostQuantumEncryption
	}
	if allowDirect(a) != allowDirect(b) ||
		postQuantumEncryption(a) != postQuantumEncryption(b) {
		return false
	}

	windowType := func(profile *PerformanceProfile) WindowType {
		if profile == nil {
			return WindowTypeAuto
		}
		return profile.WindowType
	}
	aWindowType := windowType(a)
	bWindowType := windowType(b)
	if aWindowType != bWindowType {
		return false
	}
	if aWindowType == WindowTypeAuto {
		return true
	}
	return a.WindowSize == b.WindowSize
}

func (self *RemoteUserNatMultiClient) SetPerformanceProfile(performanceProfile *PerformanceProfile) {
	performanceProfile = self.overrideAllowDirect(performanceProfile)
	if performanceProfile != nil {
		err := performanceProfile.Validate()
		if err != nil {
			panic(err)
		}
	}

	// an equivalent profile is a no-op: shuffle() below retires every window
	// client, and presentation code commonly re-applies an equal profile on
	// resume -- that must not tear the window down
	if performanceProfilesEqual(self.config.Load().performanceProfile, performanceProfile) {
		return
	}

	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		// rebuild the immutable config snapshot under the lock so concurrent
		// setters do not lose each other's field
		prev := self.config.Load()
		self.config.Store(&multiClientConfig{
			performanceProfile:  performanceProfile,
			localSecurityBypass: prev.localSecurityBypass,
			serverNameLookup:    prev.serverNameLookup,
			blocker:             prev.blocker,
		})
	}()
	for _, window := range self.windows {
		window.SetPerformanceProfile(performanceProfile)
		// reset the window
		window.shuffle()
	}
}

func (self *RemoteUserNatMultiClient) SetLocalSecurityBypass(localSecurityBypass bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	prev := self.config.Load()
	self.config.Store(&multiClientConfig{
		performanceProfile:  prev.performanceProfile,
		localSecurityBypass: localSecurityBypass,
		serverNameLookup:    prev.serverNameLookup,
		blocker:             prev.blocker,
	})
}

// SetServerNameLookup installs (or clears, with nil) the ServerNameLookup used for
// ServerName-based path affinity. Safe to call at runtime.
func (self *RemoteUserNatMultiClient) SetServerNameLookup(serverNameLookup ServerNameLookup) {
	var prevUnsub func()
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		prev := self.config.Load()
		self.config.Store(&multiClientConfig{
			performanceProfile:  prev.performanceProfile,
			localSecurityBypass: prev.localSecurityBypass,
			serverNameLookup:    serverNameLookup,
			blocker:             prev.blocker,
		})
		// re-subscribe to server-name-learned notifications: a name learned for an
		// ip invalidates that ip's cached block-action decisions so they re-resolve
		// with the server name. A lookup that doesn't notify just reconciles on the
		// ttl.
		prevUnsub = self.serverNamesLearnedUnsub
		self.serverNamesLearnedUnsub = nil
		if notifier, ok := serverNameLookup.(ServerNamesLearnedNotifier); ok {
			self.serverNamesLearnedUnsub = notifier.AddServerNamesLearnedCallback(self.invalidateServerNames)
		}
	}()
	if prevUnsub != nil {
		prevUnsub()
	}
	if self.ipAssoc != nil {
		self.ipAssoc.SetServerNameLookup(serverNameLookup)
	}
	// full reset on install: the new lookup may report different names. (Ongoing
	// single-name learns are handled incrementally by invalidateServerNames.)
	self.blockActionCache.clear()
	self.blockActionIgnoreCache.clear()
}

// invalidateServerNames drops the cached block-action decisions for the given
// ips, so the next flow to each rebuilds its decision and re-resolves the server
// name(s) — reporting the server name instead of the ip going forward. A cached
// decision unions server names over the destination's entire cluster, so the
// learned ip's cluster siblings are invalidated too: their cached decisions
// would otherwise keep reporting without the newly learned name until the ttl.
// Wired to the server-name lookup's learned notifications in SetServerNameLookup.
func (self *RemoteUserNatMultiClient) invalidateServerNames(addrs []netip.Addr) {
	for _, addr := range addrs {
		// the caches key on the unmapped addr (ipAssocAddr)
		key := addr.Unmap()
		self.blockActionCache.delete(key)
		self.blockActionIgnoreCache.delete(key)
		if self.ipAssoc != nil {
			for _, member := range self.ipAssoc.GetClusterAddrs(key) {
				memberKey := member.Unmap()
				if memberKey != key {
					self.blockActionCache.delete(memberKey)
					self.blockActionIgnoreCache.delete(memberKey)
				}
			}
		}
	}
}

// SetBlocker installs (or clears, with nil) the ad/tracker Blocker consulted
// in the egress decision: a destination is blocked when its ip falls in the
// blocker's ranges or any of its own observed server names is a blocked
// hostname. the check is destination scoped — deliberately not extended over
// the IpAssoc cluster, which would over-block co-clustered infrastructure.
// user overrides take precedence: an un-blocked blocker match egresses
// remotely as normal. enabling/disabling happens on the blocker itself and
// takes effect immediately (cached decisions revalidate against the enabled
// state). safe to call at runtime.
func (self *RemoteUserNatMultiClient) SetBlocker(blocker Blocker) {
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		prev := self.config.Load()
		self.config.Store(&multiClientConfig{
			performanceProfile:  prev.performanceProfile,
			localSecurityBypass: prev.localSecurityBypass,
			serverNameLookup:    prev.serverNameLookup,
			blocker:             blocker,
		})
	}()
	self.blockActionCache.clear()
}

func (self *RemoteUserNatMultiClient) LocalSecurityBypass() bool {
	return self.config.Load().localSecurityBypass
}

// ordered by choice descending
func (self *RemoteUserNatMultiClient) selectWindowTypes(sendPacket *parsedPacket, appId string) []WindowType {
	// - web traffic is routed to quality providers
	// - all other traffic is routed to speed providers

	if _, fixed := self.generator.FixedDestinationSize(); fixed {
		return []WindowType{WindowTypeQuality}
	}

	if windowType, _, ok := self.config.Load().performanceProfile.FixedWindow(); ok {
		return []WindowType{windowType}
	} else {
		// HTTPS remains quality-first even with no classifier. A classifier can
		// additionally identify streaming carried on another port (or by a
		// streaming app), which needs the same low-stall quality window rather
		// than the bulk-oriented speed window.
		flowClass := ClassUnknown
		if p := self.flowClassifier.Load(); p != nil {
			flowClass = classifyOrUnknown(*p, sendPacket.ipPath, appId).Class
		}
		if sendPacket.ipPath.DestinationPort == 443 || flowClass == ClassStreaming {
			return []WindowType{WindowTypeQuality, WindowTypeSpeed}
		}
		return []WindowType{WindowTypeSpeed, WindowTypeQuality}
	}
}

// ReliabilitySettings is the runtime-overridable subset of MultiClientSettings:
// the knobs that change how a flow reacts when its exit misbehaves. Each one
// exists so the freeze it addresses can be turned off and back on against a
// live connection, since which cause a given user is hitting is not something
// the code can tell from the outside.
type ReliabilitySettings struct {
	// see the matching MultiClientSettings fields for what each one does
	UdpTeardownSignal        bool
	QuicRebindOnExitLoss     bool
	TcpCollapseMaxHold       time.Duration
	SendStallTimeout         time.Duration
	ClusterAffinityFallback  bool
	ServerNameAffinityBridge bool
	SequenceIdleTimeout      time.Duration
	TcpSequenceIdleTimeout   time.Duration
	BlackholeReceiveTimeout  time.Duration
	MaxFlowsPerExit          int
	AffinityStickyPastCap    bool
	FreshFlowAffinity        bool
	PerformanceAwareAffinity bool
	MaxStickyFlowsPerExit    int
	StickyFlowIdleTimeout    time.Duration
	ScoredAffinityDonor      bool
	// the G-1 group-follow pair; see the MultiClientSettings fields
	QuarantineGroupFollow      bool
	GroupFollowWindow          time.Duration
	DialFailureRerace          bool
	UplinkStalenessGate        time.Duration
	SoftVerdictDemote          bool
	RemovalBudgetCount         int
	RemovalBudgetWindow        time.Duration
	StandingReserve            bool
	EffectiveTierSelection     bool
	MinBlackholeDestinations   int
	BlackholeLoadCorroboration int
	ProviderProbe              bool
	ProbeTimeout               time.Duration
	ProbeSampleHostCount       int
	ProbeSilenceWarnStreak     int
	SharedFateMinExits         int
	SharedFateWindow           time.Duration
	EvaluationPoolMultiple     int
	// FormationPollTimeout: 0 falls back to SendRetryTimeout (the pre-change
	// behavior), unlike the other zero-value-off knobs here
	FormationPollTimeout time.Duration
	// the outcome-deadline pair (window honesty); zero-value-off like the
	// rest, so an override built from an older struct turns the deadline off
	// rather than changing its semantics
	WindowOutcomeDeadline        time.Duration
	WindowOutcomeRebuildDeadline time.Duration
	// the P2 upstream-port knobs. Every one of them is zero-value-off, so a
	// developer menu that writes an override built from an older struct turns
	// the ports off rather than changing their semantics.
	BusyProbe                          bool
	BusyProbeBudget                    time.Duration
	SchedulerPauseTolerance            time.Duration
	SchedulerPauseRecoveryTimeout      time.Duration
	BlackholeConnectComparativeTimeout time.Duration
	// HeartbeatInterval is the P3 observability knob, zero-value-off like the
	// rest. It is mirrored here so the heartbeat can be silenced (or sped up
	// for a drill) from the developer menu without a reconnect -- a field
	// capture is sometimes taken with the beat turned up to spot a transition,
	// and sometimes with it off to keep an hour of buffer for something else.
	HeartbeatInterval time.Duration

	// Smart routing (Phase 1), all zero-value-off so an override from an older
	// struct keeps today's placement. ScoredPlacement is the master gate;
	// PlacementHysteresisPct==0 is plain greater-than; PlacementDemoteConsecutive<=1
	// acts on every sample; RewardInstrumentation==false emits no reward lines.
	ScoredPlacement            bool
	PlacementHysteresisPct     float64
	PlacementDemoteConsecutive int
	RewardInstrumentation      bool
	// LightClassifier; see the matching MultiClientSettings field for what it
	// does. Unlike the rest of this block, a runtime override DOES take
	// effect immediately: SetReliabilitySettings installs or clears the
	// classifier itself on a 0->1 / 1->0 edge, so toggling this through a
	// developer menu changes placement on the next flow, not just the
	// banner's report. See SetReliabilitySettings.
	LightClassifier bool

	// the quarantine flap-damping pair; see the matching MultiClientSettings
	// fields for what each one does
	QuarantineDampening   bool
	QuarantineReentryRamp time.Duration
}

// ReliabilitySettingsFrom reads the effective values out of a settings struct.
// nil yields the zero value, which is every reliability behavior off -- the
// state before any of this work, and what the bare test fixtures get.
func ReliabilitySettingsFrom(settings *MultiClientSettings) *ReliabilitySettings {
	if settings == nil {
		return &ReliabilitySettings{}
	}
	return &ReliabilitySettings{
		UdpTeardownSignal:          settings.UdpTeardownSignal,
		QuicRebindOnExitLoss:       settings.QuicRebindOnExitLoss,
		TcpCollapseMaxHold:         settings.TcpCollapseMaxHold,
		SendStallTimeout:           settings.SendStallTimeout,
		ClusterAffinityFallback:    settings.ClusterAffinityFallback,
		ServerNameAffinityBridge:   settings.ServerNameAffinityBridge,
		SequenceIdleTimeout:        settings.SequenceIdleTimeout,
		TcpSequenceIdleTimeout:     settings.TcpSequenceIdleTimeout,
		BlackholeReceiveTimeout:    settings.BlackholeReceiveTimeout,
		MaxFlowsPerExit:            settings.MaxFlowsPerExit,
		AffinityStickyPastCap:      settings.AffinityStickyPastCap,
		FreshFlowAffinity:          settings.FreshFlowAffinity,
		PerformanceAwareAffinity:   settings.PerformanceAwareAffinity,
		MaxStickyFlowsPerExit:      settings.MaxStickyFlowsPerExit,
		StickyFlowIdleTimeout:      settings.StickyFlowIdleTimeout,
		ScoredAffinityDonor:        settings.ScoredAffinityDonor,
		QuarantineGroupFollow:      settings.QuarantineGroupFollow,
		GroupFollowWindow:          settings.GroupFollowWindow,
		DialFailureRerace:          settings.DialFailureRerace,
		UplinkStalenessGate:        settings.UplinkStalenessGate,
		SoftVerdictDemote:          settings.SoftVerdictDemote,
		RemovalBudgetCount:         settings.RemovalBudgetCount,
		RemovalBudgetWindow:        settings.RemovalBudgetWindow,
		StandingReserve:            settings.StandingReserve,
		EffectiveTierSelection:     settings.EffectiveTierSelection,
		MinBlackholeDestinations:   settings.MinBlackholeDestinations,
		BlackholeLoadCorroboration: settings.BlackholeLoadCorroboration,
		ProviderProbe:              settings.ProviderProbe,
		ProbeTimeout:               settings.ProbeTimeout,
		ProbeSampleHostCount:       settings.ProbeSampleHostCount,
		ProbeSilenceWarnStreak:     settings.ProbeSilenceWarnStreak,
		SharedFateMinExits:         settings.SharedFateMinExits,
		SharedFateWindow:           settings.SharedFateWindow,
		EvaluationPoolMultiple:     settings.EvaluationPoolMultiple,
		FormationPollTimeout:       settings.FormationPollTimeout,

		WindowOutcomeDeadline:        settings.WindowOutcomeDeadline,
		WindowOutcomeRebuildDeadline: settings.WindowOutcomeRebuildDeadline,

		BusyProbe:                          settings.BusyProbe,
		BusyProbeBudget:                    settings.BusyProbeBudget,
		SchedulerPauseTolerance:            settings.SchedulerPauseTolerance,
		SchedulerPauseRecoveryTimeout:      settings.SchedulerPauseRecoveryTimeout,
		BlackholeConnectComparativeTimeout: settings.BlackholeConnectComparativeTimeout,
		HeartbeatInterval:                  settings.HeartbeatInterval,

		ScoredPlacement:            settings.ScoredPlacement,
		PlacementHysteresisPct:     settings.PlacementHysteresisPct,
		PlacementDemoteConsecutive: settings.PlacementDemoteConsecutive,
		RewardInstrumentation:      settings.RewardInstrumentation,
		LightClassifier:            settings.LightClassifier,

		QuarantineDampening:   settings.QuarantineDampening,
		QuarantineReentryRamp: settings.QuarantineReentryRamp,
	}
}

// reliabilitySettings is the effective reliability config: a runtime override
// when one has been set, else whatever the client was constructed with. Safe on
// a bare client, which several test fixtures rely on.
func (self *RemoteUserNatMultiClient) reliabilitySettings() *ReliabilitySettings {
	if overrides := self.reliability.Load(); overrides != nil {
		return overrides
	}
	if self.defaultReliabilitySettings != nil {
		return self.defaultReliabilitySettings
	}
	// Preserve the mutable-settings behavior of literal test fixtures. Real
	// clients always install the immutable projection in the constructor.
	return ReliabilitySettingsFrom(self.settings)
}

// rebuildAffinityDonorIndexesWithLock changes only the reverse donor indexes.
// Every update keeps its small local group keys for performance measurement,
// so a rare developer A/B toggle can make the legacy mode immediately exact
// without retaining ordinary reverse entries while that mode is off.
//
// called with stateLock
func (self *RemoteUserNatMultiClient) rebuildAffinityDonorIndexesWithLock(includeOrdinary bool) {
	ip4 := map[Ip4Path]map[Ip4Path]time.Time{}
	for path, update := range self.ip4PathUpdates {
		if update == nil || update.IsDone() || (!includeOrdinary && !update.pinned) {
			continue
		}
		createTime := update.activityTime
		if createTime.IsZero() {
			createTime = update.openTime
		}
		for affinityPath := range update.affinityIp4Paths {
			paths := ip4[affinityPath]
			if paths == nil {
				paths = map[Ip4Path]time.Time{}
				ip4[affinityPath] = paths
			}
			paths[path] = createTime
		}
	}
	ip6 := map[Ip6Path]map[Ip6Path]time.Time{}
	for path, update := range self.ip6PathUpdates {
		if update == nil || update.IsDone() || (!includeOrdinary && !update.pinned) {
			continue
		}
		createTime := update.activityTime
		if createTime.IsZero() {
			createTime = update.openTime
		}
		for affinityPath := range update.affinityIp6Paths {
			paths := ip6[affinityPath]
			if paths == nil {
				paths = map[Ip6Path]time.Time{}
				ip6[affinityPath] = paths
			}
			paths[path] = createTime
		}
	}
	self.affinityIp4Paths = ip4
	self.affinityIp6Paths = ip6
}

// scoredPlacementEnabled is the single gate the placement path checks. When
// false (the zero value, and every current build's default) selection is
// exactly today's; nothing in this file's new routing code runs. See the
// guarded branch in sendPacket's placement helper (coalesceOrderedClients)
// and scoredPlacementReorder.
func scoredPlacementEnabled(r *ReliabilitySettings) bool {
	return r != nil && r.ScoredPlacement
}

// SetReliabilitySettings installs runtime overrides for the reliability knobs.
// nil clears them, restoring the constructed settings. Takes effect on the next
// packet -- no reconnect needed, which is the point: a freeze can be A/B'd
// while it is happening.
//
// Every field that actually changed is logged, one line each. This is what
// makes an A/B arm self-documenting in a field capture: the session banner says
// what the run started with, and these lines say what the owner toggled and
// when, so a symptom can be read against the configuration that was in force at
// that moment instead of against a guess. The diff is taken between EFFECTIVE
// configurations (before and after the store), so clearing an override logs the
// restoration rather than a misleading "everything went to zero".
//
// LightClassifier and FreshFlowAffinity are the two knobs this function must
// do more than report on. Every other field here is read fresh from
// reliabilitySettings() at the point it is consulted (e.g.
// scoredPlacementEnabled, benchDuration), so swapping the override is the
// whole story for them. LightClassifier instead gates whether an *object* --
// the classifier -- sits behind the separate SetFlowClassifier/flowClassifier
// seam, and nothing about storing a new ReliabilitySettings touches that seam
// on its own. Left alone, a runtime toggle would log
// "field=lightclassifier from=0 to=1" and the banner would agree, while
// flowClassifier stayed nil and placement never changed -- a confirming log
// line for a mechanism that never engaged. So an edge (before != after) on
// this one field additionally installs or clears the classifier, which is
// what makes the toggle real rather than decorative. The initial install at
// construction (maybeInstallLightClassifier, called once from
// NewRemoteUserNatMultiClient) is unaffected by this and stays as the first
// install for a session that starts with the knob already on.
// FreshFlowAffinity is still read directly on placement, but its enabled mode
// retains DNS-exit channel hints. A true->false edge clears those references
// after releasing the settings leaf lock; otherwise disabling hard affinity
// could retain an hour of state that no production decision will consume.
//
// Lock discipline: reliabilitySettingsLock, a dedicated leaf mutex, is held
// ONLY across the before/store/after/classifier-swap sequence below -- two
// atomic operations and nothing else, never a log call, never another lock,
// never anything that can block. It is NEVER taken on the placement path
// (nothing else in this file takes it; reliabilitySettings() itself stays a
// bare atomic load, so ordinary reads of the effective settings are
// unaffected). It exists because the edge decision spans two separate atomic
// operations (the `reliability` swap and the `flowClassifier` swap) that must
// land as one unit: without it, two concurrent callers can each compute
// before/after from their own interleaved snapshot, so the caller whose
// settings store lands last in `reliability` is not guaranteed to be the
// caller whose classifier install/clear runs last either -- the settings and
// the classifier can end up disagreeing about which caller "won" (see
// TestSetReliabilitySettingsConcurrentTogglesConverge). Serializing this
// rare, developer-menu-triggered call against itself costs nothing.
//
// The classifier's own "flow_classifier" log line is deliberately emitted
// AFTER the lock is released (via setFlowClassifierUnlogged inside the lock,
// then a manual log line below), rather than by calling SetFlowClassifier
// directly from the locked section -- SetFlowClassifier logs before it
// stores, and this function's lock must never wrap a logging call.
func (self *RemoteUserNatMultiClient) SetReliabilitySettings(reliabilitySettings *ReliabilitySettings) {
	self.reliabilitySettingsLock.Lock()
	before := self.reliabilitySettings()
	self.reliability.Store(reliabilitySettings)
	after := self.reliabilitySettings()

	classifierChanged := before.LightClassifier != after.LightClassifier
	hardAffinityChanged := before.FreshFlowAffinity != after.FreshFlowAffinity
	if classifierChanged {
		if after.LightClassifier {
			self.setFlowClassifierUnlogged(NewLightClassifier(self.serverNameResolver))
		} else {
			self.setFlowClassifierUnlogged(nil)
		}
	}
	self.reliabilitySettingsLock.Unlock()
	if hardAffinityChanged {
		self.stateLock.Lock()
		if !after.FreshFlowAffinity {
			clear(self.dnsExitHints)
			clear(self.dnsAddressExitHints)
		}
		self.rebuildAffinityDonorIndexesWithLock(after.FreshFlowAffinity)
		self.stateLock.Unlock()
	}
	// SequenceIdleTimeout and TcpSequenceIdleTimeout are runtime reliability
	// knobs. Wake the shared reaper so a shorter override applies immediately;
	// a longer one is harmlessly rechecked before any flow is detached.
	self.notifyFlowReaper()

	log := loggerOrDefault(self.log)
	if classifierChanged {
		log.Infof("%s\n", relEvent(
			"flow_classifier",
			"installed", after.LightClassifier,
		))
	}
	for _, line := range relSettingsDiffLines(before, after) {
		log.Infof("%s\n", line)
	}
}

// ReliabilitySettings returns the effective reliability config, for reporting
// the live state back to a developer menu.
func (self *RemoteUserNatMultiClient) ReliabilitySettings() *ReliabilitySettings {
	return self.reliabilitySettings()
}

// clusterAffinityRepresentative picks one stable member of a cluster so every
// ip in it resolves to the same affinity key. The minimum orders
// deterministically, where map iteration would hand back a different member per
// call and defeat the grouping entirely. Empty (an ip in no multi-member
// cluster) yields false, leaving the caller on per-ip affinity.
func clusterAffinityRepresentative(members []netip.Addr) (netip.Addr, bool) {
	var representative netip.Addr
	found := false
	for _, member := range members {
		member = member.Unmap()
		if !found || member.Less(representative) {
			representative = member
			found = true
		}
	}
	return representative, found
}

// destinationAffinityIpPathWithLock builds the destination-scoped affinity key
// for the web ports, or nil when the port has no such key.
//
// No server name means the dns mux never observed a query for this ip: the app
// resolved over its own doh (chrome secure dns, android private dns), or the os
// answered from its cache -- the long-ttl case ReverseTtl's comment calls out.
// Per-ip affinity then splits one cdn-hosted site across the window, since its
// many ips each key separately, so this falls back to the ip association
// cluster, which already groups co-active ips.
//
// Shared by the registration path and the late-name bridge so the two can never
// build the key differently -- if they diverged the bridge would silently stop
// matching and nothing would fail.
//
// called with stateLock
func (self *RemoteUserNatMultiClient) destinationAffinityIpPathWithLock(ipPath *IpPath) *IpPath {
	switch ipPath.DestinationPort {
	case 80, 53, 443:
	default:
		return nil
	}

	destinationIp := ipPath.DestinationIp
	// this function is otherwise a pure function of the config and the path,
	// and is exercised that way from bare clients, so an unset settings falls
	// back to plain per-ip affinity rather than panicking
	if self.reliabilitySettings().ClusterAffinityFallback && self.ipAssoc != nil {
		if addr, ok := ipAssocAddr(ipPath.DestinationIp); ok {
			// GetClusterAddrs is a lock-free atomic load, safe to call with the
			// parent stateLock held
			if rep, ok := clusterAffinityRepresentative(self.ipAssoc.GetClusterAddrs(addr)); ok {
				destinationIp = rep.AsSlice()
			}
		}
	}

	return &IpPath{
		Version:         ipPath.Version,
		DestinationIp:   destinationIp,
		DestinationPort: ipPath.DestinationPort,
	}
}

// affinityFallbackIpPathsWithLock returns the destination-scoped groups a NEW
// flow may inherit a client from when its own affinity group has no donor.
// Consulted, never joined.
//
// The mux learns a server name from the tls ClientHello as well as from dns, and
// the ClientHello is the fourth packet of a connection whose syn already created
// the flow. So for any site the mux did not resolve itself, the ordering is
// always: the first flow is created nameless and keys on the destination, the
// name is learned an rtt later, and every later flow keys on the base domain
// with an empty group -- stranding the first connection, usually the main page
// load, on a different exit from the rest of the session.
//
// Reading the destination group lets those later flows converge onto the exit
// the established flow already uses. Convergence is toward the established
// exit, which is the only direction available: providers terminate tcp, so
// moving a live flow would break it, which is the failure this whole change
// exists to prevent.
//
// Ordering is most specific first -- the exact destination ip, then the cluster
// representative when it differs. Ports outside the web set deliberately return
// nil: their no-name keys are port-only or global, so bridging into them would
// pin every named site to one exit.
//
// called with stateLock
func (self *RemoteUserNatMultiClient) affinityFallbackIpPathsWithLock(ipPath *IpPath) []*IpPath {
	if !self.reliabilitySettings().ServerNameAffinityBridge {
		return nil
	}
	switch ipPath.DestinationPort {
	case 80, 53, 443:
	default:
		return nil
	}
	if ipPath.DestinationIp == nil {
		return nil
	}

	// a window fixed to one client has a single global affinity path already.
	// nil-tolerant: bare test fixtures assemble the struct without a config
	if config := self.config.Load(); config != nil {
		if _, windowSize, ok := config.performanceProfile.FixedWindow(); ok && windowSize.FixedWindowSize == 1 {
			return nil
		}
	}

	fallbackPaths := []*IpPath{
		{
			Version:         ipPath.Version,
			DestinationIp:   ipPath.DestinationIp,
			DestinationPort: ipPath.DestinationPort,
		},
	}
	// the cluster representative is the broader guess, consulted only if the
	// exact destination group has no usable donor
	if destinationPath := self.destinationAffinityIpPathWithLock(ipPath); destinationPath != nil &&
		!destinationPath.DestinationIp.Equal(ipPath.DestinationIp) {
		fallbackPaths = append(fallbackPaths, destinationPath)
	}
	return fallbackPaths
}

// underFlowCap drops candidates that are already carrying their share of
// flows, preserving the caller's ordering. May return an empty slice when
// every candidate is full: the caller decides the fallback, because only the
// caller knows whether a wider field (another tier) is available to try
// first. See raceCandidates for the fallback order.
//
// NOTE this filter alone does not bound anything. It runs at *selection*, and
// the cap is never re-checked at *assignment*: `sendUpdate` holds stateLock to
// pick a client and releases it, while the matching
// `clientUpdates[client][update] = true` happens in a separate acquisition in
// `sendClientPath`. N concurrent flows in one affinity group all observe the
// same count and all commit, taking an exit to count+N. On device an exit
// reached 79 flows against a cap of 16 with this filter in place.
//
// The real fix is to make check-and-assign atomic at each of the three
// assignment sites. Not attempted here.
func (self *RemoteUserNatMultiClient) underFlowCap(clients []*multiClientChannel) []*multiClientChannel {
	if len(clients) == 0 || self.reliabilitySettings().MaxFlowsPerExit <= 0 {
		return clients
	}

	under := make([]*multiClientChannel, 0, len(clients))
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		for _, client := range clients {
			if !self.clientAtFlowCapWithLock(client) {
				under = append(under, client)
			}
		}
	}()
	return under
}

// raceCandidates assembles the field a new flow is placed over.
//
// The window offers only its best rank (OrderedClients keeps the min tier) so
// traffic does not cross rank until necessary. With one or two exits in the
// top rank -- the normal state on a small provider pool, since the platform
// tiers on measured latency and speed -- that gate used to defeat the flow
// cap outright: the under-cap filter could never keep two candidates, fell
// back to the unfiltered list, and the lowest-rtt winner re-picked the same
// saturated exit for every flow. On device that read as 86 flows on one exit
// with twelve idle spares, which is the exact blast radius MaxFlowsPerExit
// exists to bound.
//
// A min tier with every exit at the cap is the "necessary" the rank gate was
// waiting for. So, in order: the min tier's exits with capacity; only when
// the min tier has none, any tier's exits with capacity; and when everything
// everywhere is full, the least-loaded exits of any tier -- the cap bounds
// blast radius, it is not admission control, and a flow with no exit under
// the cap is still placed, spread toward an even share rather than piled on
// the best rank.
//
// A single under-cap candidate is returned alone, taking the send path's
// no-race branch rather than widening the field: crossing rank while the top
// rank still has capacity would let a nearby lower-rank exit win on rtt and
// split traffic off the rank the platform chose. A no-race placement still
// recovers through the dial-failure and send-error re-race paths.
// ipVersion is the flow's ip version: every list source is narrowed to the
// exits that can carry it (IPV6.md B3).
func (self *RemoteUserNatMultiClient) raceCandidates(window *multiClientWindow, ipVersion int) []*multiClientChannel {
	return self.raceCandidatesFrom(
		func() []*multiClientChannel { return window.OrderedClientsForIpVersion(ipVersion) },
		func() []*multiClientChannel { return window.orderedClientsCrossTierForIpVersion(ipVersion) },
		func() []*multiClientChannel { return window.lastResortClientsForIpVersion(ipVersion) },
	)
}

// raceCandidatesFrom is raceCandidates over explicit list sources, the seam
// the tests drive. All are pulled lazily: the cross-tier walk only happens
// when the min tier is saturated, the last-resort walk only when the window's
// whole offer is empty, and with the cap off the window's rank gate is left
// exactly as it was.
func (self *RemoteUserNatMultiClient) raceCandidatesFrom(
	minTier func() []*multiClientChannel,
	crossTier func() []*multiClientChannel,
	lastResort func() []*multiClientChannel,
) []*multiClientChannel {
	minTierClients := minTier()
	if len(minTierClients) == 0 {
		// the window offered nothing: every exit is warned or quarantined at
		// once -- the steady state of a pool whose providers stall
		// intermittently under load, since a quarantined exit is excluded
		// from the offer for exactly as long as its stall lasts. Refusing to
		// place leaves the flow in the send retry loop with nothing to try,
		// which the user experiences as a spinner that ends only when some
		// exit happens to unbench. A benched exit is soft-suspect but alive:
		// hand the whole benched field to the race and let the first
		// responder win -- the race is itself the health probe that finds
		// whichever stalled exit is currently moving bytes.
		if benched := lastResort(); 0 < len(benched) {
			return benched
		}
		return minTierClients
	}
	if self.reliabilitySettings().MaxFlowsPerExit <= 0 {
		return minTierClients
	}
	if under := self.underFlowCap(minTierClients); 0 < len(under) {
		return under
	}
	crossed := crossTier()
	if under := self.underFlowCap(crossed); 0 < len(under) {
		return under
	}
	// every exit of every rank is at the cap: demand exceeds the pool's
	// capacity, and something must exceed the cap. The overflow goes to
	// whoever carries least, not to the min tier -- placing it by rank
	// re-created the single-exit pileup the cap exists to prevent (on device:
	// five exits pinned at 16 while the lone tier-1 exit absorbed 267 flows,
	// a 267-flow teardown waiting to happen). An even share is the best
	// remaining blast-radius bound once capacity is spent.
	if least := self.leastLoadedClients(crossed); 0 < len(least) {
		return least
	}
	return minTierClients
}

// leastLoadedClients keeps the clients carrying the fewest recorded flows --
// the placement of last resort when every exit of every rank is at the flow
// cap. Ties all stay in (the common case at the even-share equilibrium,
// where whole groups sit at the same count), so the multi-exit race is
// preserved exactly when it matters most. Caller ordering is preserved.
func (self *RemoteUserNatMultiClient) leastLoadedClients(clients []*multiClientChannel) []*multiClientChannel {
	if len(clients) < 2 {
		return clients
	}

	least := []*multiClientChannel{}
	minCount := -1
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	for _, client := range clients {
		count := len(self.clientUpdates[client])
		if minCount < 0 || count < minCount {
			minCount = count
			least = append(least[:0], client)
		} else if count == minCount {
			least = append(least, client)
		}
	}
	return least
}

// classifyLogInterval bounds the `[rel] event=classify` emission rate (see
// classifyLogThrottle). scoredPlacementReorder runs at new-flow frequency, so
// an un-throttled line would flood the log the moment a page load opens a
// burst of connections at once -- the exact "flow-storm" case the sampling
// requirement exists for. Short enough that a field capture still narrates
// what the classifier is doing within a few seconds of a session starting;
// long enough that a storm of hundreds of new flows costs one line, not
// hundreds. Package-level (not per-client) on the same pattern as
// dropErrThrottle/oobErrThrottle/authErrThrottle: this process runs at most
// one multi-client session, and a shared throttle is lock-free and needs no
// per-instance init, so bare test fixtures (a literal &RemoteUserNatMultiClient{})
// never see a nil-throttle panic.
const classifyLogInterval = 5 * time.Second

var classifyLogThrottle = newLogThrottle(classifyLogInterval)

// demotionKey identifies one (exit, class) demotion streak -- an exit's own
// TrafficClass-scoped N-of-M bad-interval count (demotionState,
// routing_score.go).
//
// Keyed by the exit's stable ClientId, NOT the *multiClientChannel pointer.
// The reason is not "an exit reconnecting should keep its old streak" --
// removeClient (below) evicts every entry for a ClientId the moment its
// channel goes away, so a genuine reconnect (same ClientId, new channel
// object) starts with a clean streak exactly as a pointer-keyed map would
// give it too. The reason is what happens when eviction is EVER missed (a
// bug, a narrow race): once a *multiClientChannel is unreachable, Go's
// allocator is free to reuse its freed memory for a later, wholly unrelated
// object. A pointer-keyed map has no way to tell "the same exit came back"
// apart from "an unrelated exit's channel happened to land at the address a
// stale entry was still keyed by" -- a real, if rare, Go footgun for any map
// that outlives the pointers it was built from. ClientId is a logical value
// that cannot collide between two different exits, so a missed eviction can
// only ever leak memory (the entry sits unread forever), never hand a new
// exit an old one's bad reputation.
type demotionKey struct {
	clientId Id
	class    TrafficClass
}

// demotionObserve records one good/bad observation for (clientId, class) and
// reports whether PlacementDemoteConsecutive consecutive bad observations
// have now accumulated -- demotionState.observe's N-of-M rule, applied to
// the map/state entry this method owns. The map and the per-key state are
// both created lazily under stateLock, the same nil-safe convention as
// qualification above: a bare test fixture (a literal
// &RemoteUserNatMultiClient{}) leaves demotionStates nil until first use.
// demotionState.observe is a couple of int comparisons -- no lock, I/O, or
// blocking of its own -- so holding stateLock across the call (rather than
// releasing it first) adds no risk and rules out a data race on the shared
// *demotionState's bad counter when two flows for the same exit and class
// are placed concurrently.
func (self *RemoteUserNatMultiClient) demotionObserve(clientId Id, class TrafficClass, good bool, needBad int) bool {
	key := demotionKey{clientId: clientId, class: class}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if self.demotionStates == nil {
		self.demotionStates = map[demotionKey]*demotionState{}
	}
	d, ok := self.demotionStates[key]
	if !ok {
		d = &demotionState{}
		self.demotionStates[key] = d
	}
	return d.observe(good, needBad)
}

// demotionLogInterval bounds the `[rel] event=demote` emission rate. Once an
// exit crosses its bad-interval threshold, demotionState.observe reports
// "demoted" on every subsequent bad interval, not just the transition -- at
// new-flow call frequency an un-throttled line here would flood the log
// exactly like an un-throttled classify line would, so this reuses that same
// sampling discipline rather than inventing a new one.
var demotionLogThrottle = newLogThrottle(classifyLogInterval)

// scoredPlacementReorder is the Phase 1 scored-placement path, called ONLY
// when scoredPlacementEnabled is true (see the guarded branch in sendPacket's
// coalesceOrderedClients). It NEVER changes which exits are eligible --
// candidates has already been through raceCandidates' verdict/quarantine/
// tier/flow-cap gates -- it only ever re-orders that already-healthy field,
// promoting the scorer's pick to the front. The learner never overrides the
// safety layer: membership is untouched, only order.
//
// READ THIS BEFORE ASSUMING THIS FUNCTION STEERS TRAFFIC. It does not, in the
// shipped configuration. MultiRaceClientCount is 0 (see its default), so
// raceOrderedClients is the WHOLE candidate list: every exit is dialed in
// parallel and completeRace binds whichever responds with the lowest
// MEASURED rtt, ignoring list order entirely. Most flows never race at all
// (destination affinity short-circuits first). So promoting an exit to index
// 0 changes goroutine launch order and the degenerate no-response lock-in,
// and nothing else.
//
// An earlier revision of this comment said the way to make the scorer
// load-bearing was to raise MultiRaceClientCount above 0. DO NOT DO THAT
// without reading the rest of this paragraph: the wide race is the only
// thing preventing rich-get-richer starvation ON THIS PATH, because a
// measured exit outscores an unmeasured one badly enough to defeat
// lessLoadedTieBreak. Narrowing the race to give the scorer influence buys
// that influence by spending a guard, over the minority of flows that race
// at all.
//
// In legacy FreshFlowAffinity A/B mode, the learner is instead made
// load-bearing at ScoredAffinityDonor, on
// inheritAffinityClient{4,6}WithLock. With the production default off,
// ordinary fresh flows skip that hard-donor step and reach this full race;
// explicit pins retain their requested stable-exit semantics. See the two
// settings' field comments for the full argument.
//
// classifyOrUnknown is nil-safe, so an unset flowClassifier (every build by
// default -- LightClassifier, Task 2's install knob, is zero-value-off)
// always classifies ClassUnknown, which returns candidates completely
// untouched: there is nothing to score against. This is what makes turning
// ScoredPlacement on, by itself, a no-op with no classifier installed --
// behavior only ever *refines*, never regresses, once a real classifier is
// installed.
func (self *RemoteUserNatMultiClient) scoredPlacementReorder(candidates []*multiClientChannel, ipPath *IpPath, appId string) []*multiClientChannel {
	if len(candidates) < 2 {
		// nothing to reorder
		return candidates
	}

	var classifier FlowClassifier
	if p := self.flowClassifier.Load(); p != nil {
		classifier = *p
	}
	flowClass := classifyOrUnknown(classifier, ipPath, appId)
	class := flowClass.Class

	// [rel] event=classify, SAMPLED via classifyLogThrottle rather than
	// per-flow: this runs at new-flow frequency on the placement path, and a
	// per-flow line would flood the log at flow-storm rates (a page load
	// opening dozens of connections at once). One line per
	// classifyLogInterval is enough to prove the classifier is alive in a
	// field capture without paying flood cost; the throttle's own suppressed
	// counter is folded in so the capture also shows how much was elided.
	// Emitted for every call (including ClassUnknown) so a capture can also
	// show the classifier declining to name a class, not just its hits.
	if ok, suppressed := classifyLogThrottle.Allow(time.Now()); ok {
		port := 0
		if ipPath != nil {
			port = ipPath.DestinationPort
		}
		loggerOrDefault(self.log).Infof("%s\n", relEvent(
			"classify",
			"class", class,
			"app", flowClass.AppId,
			"port", port,
			"suppressed", suppressed,
		))
	}

	if class == ClassUnknown {
		// no classifier installed, or it declined to name a class: nothing to
		// score against, so the field stands in the legacy order raceCandidates
		// already computed.
		return candidates
	}

	// flow counts are parent-lock state (the same bookkeeping
	// leastLoadedClients reads above), gathered in one locked pass and then
	// scored with no lock held. The rest of exitMetricsSnapshot's inputs
	// (windowStatsWithCoalesce(false), quarantineReconvictionCount,
	// rttEwmaSnapshot) each take only their OWN channel's stateLock, in their
	// own short critical section -- never the parent lock, never held across
	// another call -- so gathering them per-candidate below is the same
	// no-lock-across-a-call discipline as this flow-count pass, just without
	// the need to pre-gather into a map first.
	flowCounts := make(map[*multiClientChannel]int, len(candidates))
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		for _, c := range candidates {
			flowCounts[c] = len(self.clientUpdates[c])
		}
	}()

	weights := classWeights(class)
	hysteresisPct := self.reliabilitySettings().PlacementHysteresisPct
	// the re-entry ramp: 0 (QuarantineReentryRamp's zero value) makes every
	// reentryPenalty call below return 0 unconditionally, so this loop's
	// scores are byte-for-byte exitScore(...) with no penalty applied --
	// exactly as before this task.
	reentryRamp := self.reliabilitySettings().QuarantineReentryRamp
	// PlacementDemoteConsecutive's zero value must be inert -- today's
	// behavior (before this task, and with the knob left unset) is no
	// demotion at all. demotionState.observe treats needBad<=1 as
	// act-on-every-sample, NOT as off, so needBad<=0 is guarded at the call
	// site below rather than passed through into observe.
	needBad := self.reliabilitySettings().PlacementDemoteConsecutive

	incumbentScore := exitScore(exitMetricsSnapshot(candidates[0], flowCounts[candidates[0]]), weights) - candidates[0].reentryPenalty(reentryRamp)

	bestIndex := 0
	bestFlows := flowCounts[candidates[0]]
	bestScore := incumbentScore
	// plainBest tracks the highest raw score with NO hysteresis or load
	// tie-break applied. demotionState ownership (below) needs to know
	// whether a challenger has genuinely outscored the incumbent this round,
	// independent of whether hysteresis was sticky enough to keep bestIndex
	// at 0 -- that stickiness is exactly what N-of-M demotion exists to
	// eventually override.
	plainBestIndex := 0
	plainBestScore := incumbentScore
	for i := 1; i < len(candidates); i++ {
		flows := flowCounts[candidates[i]]
		score := exitScore(exitMetricsSnapshot(candidates[i], flows), weights) - candidates[i].reentryPenalty(reentryRamp)
		if lessLoadedTieBreak(bestScore, score, bestFlows, flows, hysteresisPct) {
			bestIndex, bestScore, bestFlows = i, score, flows
		}
		if score > plainBestScore {
			plainBestIndex, plainBestScore = i, score
		}
	}

	// demotionState ownership: an incumbent hysteresis is still protecting
	// (bestIndex still 0) but that a challenger has genuinely, if mildly,
	// outscored for needBad consecutive rounds gets re-ranked anyway -- a
	// single noisy round never does this (demotionState's N-of-M smoothing
	// absorbs it), but persistent quiet underperformance is not noise. This
	// only ever moves bestIndex to plainBestIndex, one candidate already in
	// this call's field -- never adds or removes a candidate, so membership
	// stays exactly what raceCandidates decided.
	if needBad > 0 {
		bad := plainBestScore > incumbentScore
		demoted := self.demotionObserve(candidates[0].ClientId(), class, !bad, needBad)
		if demoted && bestIndex == 0 {
			if ok, suppressed := demotionLogThrottle.Allow(time.Now()); ok {
				loggerOrDefault(self.log).Infof("%s\n", relEvent(
					"demote",
					"exit", candidates[0].ClientId(),
					"class", class,
					"suppressed", suppressed,
				))
			}
			bestIndex = plainBestIndex
		}
	}

	if bestIndex == 0 {
		return candidates
	}

	reordered := make([]*multiClientChannel, 0, len(candidates))
	reordered = append(reordered, candidates[bestIndex])
	for i, c := range candidates {
		if i != bestIndex {
			reordered = append(reordered, c)
		}
	}
	return reordered
}

// exitMetricsSnapshot builds the scorer's read of one candidate, entirely
// from non-coalescing accessors -- no lock held across a call, no I/O, no
// blocking, safe at new-flow frequency:
//
//   - Flows is the caller's own flow-count read (leastLoadedClients' same
//     bookkeeping), passed in rather than re-read here.
//   - GoodputBytesPerSec comes from windowStatsWithCoalesce(false), the
//     NON-coalescing twin of WindowStats(). It is NOT side-effect-free: it
//     still writes maxEffectiveByteCountPerSecond, healthy, lastHealthy/
//     UnhealthyTime and the healthy/unhealthy durations. It is safe here only
//     because orderedClients() already calls the COALESCING WindowStats() on
//     every client on this same path at this same frequency, so this is a
//     strictly weaker duplicate rather than new perturbation. WindowStats()
//     itself must
//     NEVER be called from this path: it coalesces event buckets and
//     perturbs the resize pass's ~15s cadence bookkeeping.
//   - StallEvents is quarantineReconvictionCount(): this channel's completed
//     bench-then-lift cycle count. A hard send-stall conviction removes the
//     channel outright (convictSendStalls), so a convicted exit is never a
//     candidate here at all -- reconvictions are the chronic-misbehavior
//     signal that survives while still in the window.
//   - RttMillis/Jitter come from rttEwmaSnapshot, this channel's own
//     send-to-ack EWMA (see addSendRttSample). Nothing else in this file
//     timed a round trip before this task (see the field comment beside
//     rttEwmaMillis).
//
// THE ZERO-RTT TRAP: exitScore sanitizes a bare 0 RTT/Jitter into the BEST
// possible sub-score (routing_score.go), the same treatment it gives a
// genuinely excellent measurement. A zero is therefore not a neutral
// "unknown" -- it is a maximal bonus, and an unmeasured exit would outrank
// every measured one. So an exit with no completed round trip yet (rttOk
// false: every bare test fixture, and a real channel before its first ack)
// reports RttMillis/Jitter as math.NaN(), NOT 0 -- exitScore's existing
// hostile-input guard already sanitizes NaN to the WORST sub-score (0.0),
// exactly the intended "unmeasured must not beat measured" reading, reusing
// tested code instead of adding a second convention. jitterOk is gated one
// sample stricter than rttOk (a single round trip has no deviation to
// report yet), so a channel with exactly one sample reports a real RttMillis
// but NaN Jitter -- consistent with the same rule.
func exitMetricsSnapshot(client *multiClientChannel, flows int) ExitMetrics {
	m := ExitMetrics{
		Flows:     flows,
		RttMillis: math.NaN(),
		Jitter:    math.NaN(),
	}
	if client == nil {
		return m
	}

	if stats, _ := client.windowStatsWithCoalesce(false); stats != nil {
		m.GoodputBytesPerSec = float64(stats.EffectiveByteCountPerSecond())
	}

	m.StallEvents = client.quarantineReconvictionCount()

	if rttMillis, jitterMillis, rttOk, jitterOk := client.rttEwmaSnapshot(); rttOk {
		m.RttMillis = rttMillis
		if jitterOk {
			m.Jitter = jitterMillis
		}
	}

	return m
}

// bindClientFlow records that a flow is now committed to client, which is what
// clientUpdates exists to track.
//
// The send path maintains this bookkeeping only when it notices the committed
// client changed across its callback. A flow that wins its exit from an async
// race never trips that: the first packet leaves with the client still nil, the
// race stores the winner later from the receive path or the race completion,
// and every subsequent send sees no change. Such a flow was therefore never
// entered here at all. (The exit readout no longer relies on this map, so the
// visible "14 exits, flows on one" symptom is fixed independently -- what this
// buys is the three consumers below.)
//
// That map is not cosmetic. clientAtFlowCapWithLock counts it, so uncounted
// flows are also uncapped, and removeClient iterates it to tear flows down, so
// uncounted flows get no teardown and recover only via the slower send-error
// path. Calling this at every assignment site makes all three correct.
//
// Must NOT be called while holding the update's stateLock: that lock is a leaf
// and its holders never take the parent lock. Both race sites call this after
// their locked section returns.
func (self *RemoteUserNatMultiClient) bindClientFlow(update *multiClientChannelUpdate, client *multiClientChannel) {
	if update == nil || client == nil || update.IsDone() {
		return
	}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	// re-read under the parent lock: the flow may have moved or been torn down
	// between the race storing its winner and this call
	if update.IsDone() || update.client.Load() != client || client.IsDone() {
		return
	}

	// drop it from any other client's set first -- a re-raced flow moves
	// between exits, and a stale entry would inflate the old exit's count and
	// hand it a teardown for a flow it no longer carries
	for otherClient, updates := range self.clientUpdates {
		if otherClient == client {
			continue
		}
		if _, ok := updates[update]; ok {
			delete(updates, update)
			if len(updates) == 0 {
				delete(self.clientUpdates, otherClient)
			}
		}
	}

	updates, ok := self.clientUpdates[client]
	if !ok {
		updates = map[*multiClientChannelUpdate]bool{}
		self.clientUpdates[client] = updates
	}
	updates[update] = true

	// an app-pinned flow that got here was placed by the RACE, not by
	// affinity -- which is the case for the app's first flow of each ip
	// version, and therefore the placement the cross-version convergence
	// most needs to know about. Recording only at creation missed it
	// entirely, so a dual-stack app still took two exits (review finding,
	// 2026-08-03).
	self.recordAppPinWithLock(update.pinAppId, client)
}

// clientAtFlowCapWithLock reports whether an exit is already carrying its
// share of flows.
//
// Applied on both assignment paths -- affinity inheritance and the race -- and
// that is the whole point. Affinity assigns update.client directly and never
// reaches the race, so a cap enforced only at client selection would not fire
// for the flows that actually concentrate: a feed opens many connections to
// the same handful of domains, which is exactly what affinity pins together.
//
// called with stateLock
func (self *RemoteUserNatMultiClient) clientAtFlowCapWithLock(client *multiClientChannel) bool {
	maxFlows := self.reliabilitySettings().MaxFlowsPerExit
	if maxFlows <= 0 {
		return false
	}
	return maxFlows <= len(self.clientUpdates[client])
}

// affinityDonorBias is the learner's read at the affinity placement point:
// this exit's provider identity, looked up in the priors the reward tap has
// been accumulating (and that PriorsStore persists across restarts).
//
// Answers priorsNeutralBias whenever there is nothing learned to say -- no
// priors created yet, or a channel with no resolvable provider identity -- so
// an unrankable donor neither wins nor loses against a known-neutral one.
// Deliberately NOT the zero-rtt trap's shape: 0.5 is the middle of Bias's
// range, not an extreme, so "unknown" cannot outrank a good measured provider
// the way a bare 0 rtt outranks a real measurement in exitScore.
//
// Locking: called with stateLock held. rewardLock is a leaf (see its field
// comment) and is taken here only to read the lazily-created providerPriors
// POINTER. Bias synchronizes on ProviderPriors' OWN mutex and is called with
// rewardLock already released, so this adds one uncontended leaf acquisition
// to the placement path and never nests two hot locks. Nothing here calls
// windowStatsWithCoalesce: that accessor is safe on the race path only
// because orderedClients has already called its coalescing twin at the same
// frequency, and affinity placement never calls orderedClients at all.
//
// called with stateLock
func (self *RemoteUserNatMultiClient) affinityDonorBias(client *multiClientChannel) float64 {
	providerId, ok := providerIdentity(client)
	if !ok {
		return priorsNeutralBias
	}
	self.rewardLock.Lock()
	priors := self.providerPriors
	self.rewardLock.Unlock()
	if priors == nil {
		return priorsNeutralBias
	}
	return priors.Bias(providerId.String())
}

// betterAffinityDonor ranks two eligible donors under ScoredAffinityDonor:
// higher bias wins, then the legacy recency rule, then provider id ascending
// so an otherwise exact tie resolves identically on every pass instead of
// following Go's randomized map iteration order. Donors equal on all three are
// interchangeable and the scan keeps whichever it saw first.
func betterAffinityDonor(
	bias float64,
	createTime time.Time,
	providerId string,
	bestBias float64,
	bestCreateTime time.Time,
	bestProviderId string,
) bool {
	if bias != bestBias {
		return bias > bestBias
	}
	if !createTime.Equal(bestCreateTime) {
		return createTime.After(bestCreateTime)
	}
	return providerId < bestProviderId
}

// inheritAffinityClient4WithLock adopts a healthy client in an affinity group:
// the most recently joined one, or -- under ScoredAffinityDonor -- the one the
// learner rates highest, falling back to recency on a tie. Only ever called with `update.client` nil -- it must
// never repoint a flow that already has a client, since providers terminate tcp
// and a moved flow is a broken flow.
//
// Returns the winning donor's verdict (donorRefused when nothing was adopted)
// and whether any candidate was refused ONLY for quarantine. The G-1 ledger is
// NOT written here: one flow placement makes several of these calls (one per
// affinity group, then the fallback groups), so per-call counting overcounts
// -- a flow whose first group scattered but whose second group donated was
// placed WITH its group, not scattered (review finding, 2026-08-03). The
// caller aggregates across all its calls and books one event per flow.
//
// called with stateLock
func (self *RemoteUserNatMultiClient) inheritAffinityClient4WithLock(update *multiClientChannelUpdate, paths map[Ip4Path]time.Time) (donorVerdict, bool) {
	// with AffinityStickyPastCap a donor at the flow cap still donates: the
	// group's egress ip is worth more than the cap, which continues to gate
	// every placement that would put a NEW site on the exit
	reliabilitySettings := self.reliabilitySettings()
	if update == nil || (!update.pinned && !reliabilitySettings.FreshFlowAffinity) {
		// Ordinary fresh flows race. The group remains registered so completed
		// flows can contribute bounded IP/domain performance evidence, but a
		// live page connection is no longer a hard placement shortcut for a
		// later media connection.
		return donorRefused, false
	}
	sticky := reliabilitySettings.AffinityStickyPastCap
	follow := reliabilitySettings.QuarantineGroupFollow
	window := reliabilitySettings.GroupFollowWindow
	if update.pinned {
		// a pinned flow chose stability: its group follows a benched donor
		// for a multiple of the ordinary window (see pinnedFollowWindow --
		// bounded on purpose, so a pinned app cannot keep a failing exit
		// both un-executed and un-released). Warned donors still refuse -- a
		// pin is not a license to board a retiring or unhealthy exit.
		follow = true
		window = pinnedFollowWindow(window)
	}
	scored := reliabilitySettings.ScoredAffinityDonor
	var mostRecentCreateTime time.Time
	var bestBias float64
	var bestProviderId string
	winnerVerdict, scattered := donorRefused, false
	for copyIp4Path, createTime := range paths {
		copyUpdate, ok := self.ip4PathUpdates[copyIp4Path]
		if !ok {
			continue
		}
		c := copyUpdate.client.Load()
		// a donor downgraded to v4-only after this group formed still
		// carries its established flows, but must not receive new ones of
		// a version it no longer carries (IPV6.md B3, B5)
		if c == nil || c.IsDone() || !c.supportsIpVersion(4) || (!sticky && self.clientAtFlowCapWithLock(c)) {
			continue
		}
		// off: the legacy short-circuit, preserved exactly -- a donor no more
		// recent than the current best is never even asked for its verdict.
		// On, every candidate must be evaluated to be ranked, which is the one
		// documented behavior difference (see ScoredAffinityDonor): quarantined
		// donors the legacy scan skipped now report their scatter.
		if !scored && !createTime.After(mostRecentCreateTime) {
			continue
		}
		switch verdict := c.affinityDonorEligible(follow, window); verdict {
		case donorEligible, donorQuarantineFollowed:
			if !update.pinned && !self.affinityPerformanceAllowsFlowDonor(c, copyUpdate, update.ipPath) {
				continue
			}
			// donorQuarantineFollowed is the G-1 follow: a benched
			// donor inside the follow window keeps its own site
			bias, providerId := 0.0, ""
			if scored {
				bias = self.affinityDonorBias(c)
				if id, idOk := providerIdentity(c); idOk {
					providerId = id.String()
				}
				if winnerVerdict != donorRefused && !betterAffinityDonor(
					bias, createTime, providerId,
					bestBias, mostRecentCreateTime, bestProviderId,
				) {
					continue
				}
			}
			bestBias, bestProviderId = bias, providerId
			mostRecentCreateTime = createTime
			update.client.Store(c)
			winnerVerdict = verdict
		case donorQuarantineScattered:
			scattered = true
		}
	}
	return winnerVerdict, scattered
}

// inheritAffinityClient6WithLock is the v6 twin of inheritAffinityClient4WithLock
//
// called with stateLock
func (self *RemoteUserNatMultiClient) inheritAffinityClient6WithLock(update *multiClientChannelUpdate, paths map[Ip6Path]time.Time) (donorVerdict, bool) {
	// see inheritAffinityClient4WithLock for the sticky, follow, pin, and
	// caller-side-counting rationale
	reliabilitySettings := self.reliabilitySettings()
	if update == nil || (!update.pinned && !reliabilitySettings.FreshFlowAffinity) {
		return donorRefused, false
	}
	sticky := reliabilitySettings.AffinityStickyPastCap
	follow := reliabilitySettings.QuarantineGroupFollow
	window := reliabilitySettings.GroupFollowWindow
	if update.pinned {
		follow = true
		window = pinnedFollowWindow(window)
	}
	scored := reliabilitySettings.ScoredAffinityDonor
	var mostRecentCreateTime time.Time
	var bestBias float64
	var bestProviderId string
	winnerVerdict, scattered := donorRefused, false
	for copyIp6Path, createTime := range paths {
		copyUpdate, ok := self.ip6PathUpdates[copyIp6Path]
		if !ok {
			continue
		}
		c := copyUpdate.client.Load()
		// see the v4 twin: a downgraded donor takes no new v6 flows
		if c == nil || c.IsDone() || !c.supportsIpVersion(6) || (!sticky && self.clientAtFlowCapWithLock(c)) {
			continue
		}
		if !scored && !createTime.After(mostRecentCreateTime) {
			continue
		}
		switch verdict := c.affinityDonorEligible(follow, window); verdict {
		case donorEligible, donorQuarantineFollowed:
			if !update.pinned && !self.affinityPerformanceAllowsFlowDonor(c, copyUpdate, update.ipPath) {
				continue
			}
			bias, providerId := 0.0, ""
			if scored {
				bias = self.affinityDonorBias(c)
				if id, idOk := providerIdentity(c); idOk {
					providerId = id.String()
				}
				if winnerVerdict != donorRefused && !betterAffinityDonor(
					bias, createTime, providerId,
					bestBias, mostRecentCreateTime, bestProviderId,
				) {
					continue
				}
			}
			bestBias, bestProviderId = bias, providerId
			mostRecentCreateTime = createTime
			update.client.Store(c)
			winnerVerdict = verdict
		case donorQuarantineScattered:
			scattered = true
		}
	}
	return winnerVerdict, scattered
}

// bookGroupLedger books ONE G-1 ledger event for one flow placement, from the
// aggregate of every inherit call the placement made: a follow only if the
// flow ended up with a donor that was a followed quarantine, a scatter only if
// quarantine refusals contributed and the flow found no donor at all.
func (self *RemoteUserNatMultiClient) bookGroupLedger(placed bool, followWinner bool, sawScatter bool) {
	if followWinner && placed {
		self.reliabilityMetrics.groupFollowed()
	} else if sawScatter && !placed {
		self.reliabilityMetrics.groupScattered()
	}
}

// domainAffinityAliases collapses the CDN constellations one service operates
// onto one bounded measurement group. With ordinary hard affinity disabled by
// default, this lets a weak media endpoint inform the next fresh connection
// without pinning the constellation to its page connection. Explicit pins and
// legacy FreshFlowAffinity A/B runs also use the canonical key when stable
// one-egress semantics are intentionally requested.
//
// Values must be canonical (never themselves keys); the anchor test walks the
// table.
var domainAffinityAliases = map[string]string{
	// youtube: manifest on the site domain, media on googlevideo, thumbs on
	// ytimg/ggpht -- the constellation whose split motivated this table
	"googlevideo.com": "youtube.com",
	"ytimg.com":       "youtube.com",
	"ggpht.com":       "youtube.com",
	"youtu.be":        "youtube.com",
	// twitter/x
	"twimg.com":   "x.com",
	"twitter.com": "x.com",
	// meta
	"fbcdn.net":        "facebook.com",
	"cdninstagram.com": "instagram.com",
	// tiktok
	"tiktokcdn.com":    "tiktok.com",
	"tiktokcdn-us.com": "tiktok.com",
	"tiktokv.com":      "tiktok.com",
	// netflix
	"nflxvideo.net": "netflix.com",
	"nflximg.net":   "netflix.com",
	"nflxso.net":    "netflix.com",
	// bloomberg: player assets and manifests are served across the publisher
	// domain and its bwbx media/static constellation. Share bounded performance
	// evidence by default; only an explicit pin or legacy A/B mode hard-binds
	// the group to one egress IP.
	"bwbx.io": "bloomberg.com",
	// twitch
	"ttvnw.net": "twitch.tv",
	"jtvnw.net": "twitch.tv",
	// reddit
	"redd.it":          "reddit.com",
	"redditmedia.com":  "reddit.com",
	"redditstatic.com": "reddit.com",
	// doordash: the app's api session and its image cdn -- split egress ips
	// were observed as images failing to load in the app
	"cdn4dd.com":      "doordash.com",
	"doordash.com.au": "doordash.com",
}

// affinityNameForServerName is the one place a server name becomes an affinity
// group name: base domain (a.foo.com, b.c.foo.com and foo.com all collapse to
// foo.com), then the constellation alias. It is a measurement key by default
// and a hard-placement key only for explicit or legacy affinity.
func affinityNameForServerName(serverName string) string {
	affinityName := serverName
	if rootDomain, err := publicsuffix.EffectiveTLDPlusOne(serverName); err == nil {
		affinityName = rootDomain
	}
	if alias, ok := domainAffinityAliases[affinityName]; ok {
		affinityName = alias
	}
	return affinityName
}

// called with stateLock
func (self *RemoteUserNatMultiClient) affinityIpPathsWithLock(ipPath *IpPath) (affinityPaths []*IpPath) {
	config := self.config.Load()

	singleIp := false
	if _, windowSize, ok := config.performanceProfile.FixedWindow(); ok {
		singleIp = (windowSize.FixedWindowSize == 1)
	}

	if singleIp {
		singlePath := &IpPath{
			Version: ipPath.Version,
		}
		affinityPaths = append(affinityPaths, singlePath)
	} else {
		var serverNames []string
		// resolve the destination IP to the server name(s) observed for it (e.g. by a
		// DNS upgrade mux), giving ServerName path affinity without parsing the SNI off
		// the wire. affinity is by the base domain — a.foo.com, b.c.foo.com and foo.com
		// all collapse to foo.com — so a site's flows pin to one client channel.
		if config.serverNameLookup != nil && ipPath.DestinationIp != nil {
			serverNames = config.serverNameLookup.ServerNames(ipPath.DestinationIp.String())
		}

		if 0 < len(serverNames) {
			seen := map[string]bool{}
			for _, serverName := range serverNames {
				affinityName := affinityNameForServerName(serverName)
				if seen[affinityName] {
					continue
				}
				seen[affinityName] = true
				affinityPaths = append(affinityPaths, &IpPath{
					ServerName: affinityName,
				})
			}
		} else if ipPath.Protocol == IpProtocolIcmp {
			// cycle per destination ip: pings to one host share a client with
			// each other, and with the host's other flows when the
			// destination resolves via the server name branch above
			destinationPath := &IpPath{
				Version:       ipPath.Version,
				DestinationIp: ipPath.DestinationIp,
			}
			affinityPaths = append(affinityPaths, destinationPath)
		} else if destinationPath := self.destinationAffinityIpPathWithLock(ipPath); destinationPath != nil {
			// for these ports, cycle the path per destination ip/port, regardless of protocol
			affinityPaths = append(affinityPaths, destinationPath)
		} else if ipPath.DestinationPort < 1024 {
			// for these ports, cycle the path per destination port, regardless of protocol or ip
			destinationPortPath := &IpPath{
				Version:         ipPath.Version,
				DestinationPort: ipPath.DestinationPort,
			}
			affinityPaths = append(affinityPaths, destinationPortPath)
		} else {
			// for user space ports, use a single path, regardless of protocol, ip, or port
			singlePath := &IpPath{
				Version: ipPath.Version,
			}
			affinityPaths = append(affinityPaths, singlePath)
		}
	}

	return
}

func (self *RemoteUserNatMultiClient) sendClientPath(ipPath *IpPath, pin flowPin, callback func(*multiClientChannelUpdate, *multiClientChannel)) {
	if self.sendClientPathForTest != nil {
		self.sendClientPathForTest(ipPath, pin, callback)
		return
	}
	update, previousClient, currentClient := self.sendUpdate(ipPath, pin)
	if update == nil {
		// closed multi-client (see sendUpdate); the packet is dropped
		return
	}
	callback(update, currentClient)

	// fast path: if the flow's client did not change during the callback, no
	// clientUpdates bookkeeping is needed, so skip the parent lock entirely
	// (client is atomic). this is the steady-state egress path.
	if previousClient == update.client.Load() {
		return
	}

	// re-read (the lock-free check above can race a concurrent client change)
	client := update.client.Load()

	if client != nil {
		// bindClientFlow, not a local add: it clears the flow from every other
		// client's set, which `previousClient` alone cannot do. previousClient
		// is this path's own stale snapshot, and an async race can commit a
		// different client between the snapshot and here -- the single-client
		// send below then stores over it, leaving the flow recorded under the
		// race's winner forever. Nothing ever cleans that: the reaper and
		// removeClient only look under update.client.Load(). The stale entry
		// inflates the abandoned exit's cap count, which makes it look full
		// sooner, which makes the single-client path more likely -- a loop
		// that only tightens.
		self.bindClientFlow(update, client)
		return
	}

	// the flow committed to nothing: drop any entry the previous client held
	if previousClient == nil {
		return
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if updates, ok := self.clientUpdates[previousClient]; ok {
		delete(updates, update)
		if len(updates) == 0 {
			delete(self.clientUpdates, previousClient)
		}
	}
}

// sequenceIdleTimeout is how long a flow may sit idle before it is torn down.
//
// A tcp connection is routinely idle between requests -- ssh sessions,
// websockets, push channels -- and the application still considers it open.
// Traditional vpns hold tcp nat state for 5-30 minutes, so a 2 minute bound
// resets connections users have every reason to think are alive. Udp has no
// equivalent notion of an open connection and its mappings are conventionally
// short lived, so it keeps the tighter bound.
func (self *RemoteUserNatMultiClient) sequenceIdleTimeout(ipPath *IpPath) time.Duration {
	return sequenceIdleTimeoutWithSettings(ipPath, self.reliabilitySettings())
}

func sequenceIdleTimeoutWithSettings(ipPath *IpPath, reliabilitySettings *ReliabilitySettings) time.Duration {
	if reliabilitySettings == nil {
		return 0
	}
	if ipPath != nil && ipPath.Protocol == IpProtocolTcp && 0 < reliabilitySettings.TcpSequenceIdleTimeout {
		return reliabilitySettings.TcpSequenceIdleTimeout
	}
	return reliabilitySettings.SequenceIdleTimeout
}

// retiredMultiClientFlow carries the leaf-lock work out of stateLock. The
// reaper removes routing ownership atomically under the parent lock, then
// records rewards, constructs any teardown packet, and closes race state with
// no parent lock held.
type retiredMultiClientFlow struct {
	update       *multiClientChannelUpdate
	client       *multiClientChannel
	shouldSignal bool
	stickyTrim   bool
}

// notifyFlowReaper is an edge-triggered wake. It is nil-safe for the small
// fixture-built clients that call sendUpdate directly without running the
// production background workers.
func (self *RemoteUserNatMultiClient) notifyFlowReaper() {
	if self.flowReaperWake == nil {
		return
	}
	select {
	case self.flowReaperWake <- struct{}{}:
	default:
	}
}

func (self *RemoteUserNatMultiClient) retireCompletedTcpFlow(update *multiClientChannelUpdate) {
	if update == nil || update.IsDone() || update.cancel == nil {
		return
	}
	update.cancel()
	self.notifyFlowReaper()
}

// detachIdleFlows removes every expired/cancelled flow and reports the next
// live deadline. There is deliberately no goroutine or timer per update: one
// scan is cheap compared with retaining a stack and a runtime timer for every
// short-lived connection over the five-to-thirty-minute TCP NAT lifetime.
func (self *RemoteUserNatMultiClient) detachIdleFlows(now time.Time) (
	retired []retiredMultiClientFlow,
	nextDelay time.Duration,
	hasNext bool,
) {
	reliabilitySettings := self.reliabilitySettings()
	var nextDeadline time.Time

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	// Affinity may deliberately grow one site's exit past the ordinary flow
	// cap to preserve a stable egress IP. Bound the retained H1 state without
	// changing that IP: select only the oldest idle excess TCP flows on each
	// overloaded exit. Active flows are never selected, so a burst can exceed
	// the steady bound temporarily and settle as connections go idle.
	stickyTrim := map[*multiClientChannelUpdate]bool{}
	stickyLimit := reliabilitySettings.MaxStickyFlowsPerExit
	stickyIdle := reliabilitySettings.StickyFlowIdleTimeout
	if 0 < stickyLimit && 0 < stickyIdle {
		for _, updates := range self.clientUpdates {
			excess := len(updates) - stickyLimit
			if excess <= 0 {
				continue
			}
			candidates := make([]*multiClientChannelUpdate, 0, excess)
			for update := range updates {
				if update == nil || update.IsDone() || update.ipPath == nil ||
					update.ipPath.Protocol != IpProtocolTcp ||
					now.Sub(update.activityTime) < stickyIdle {
					continue
				}
				candidates = append(candidates, update)
			}
			slices.SortFunc(candidates, func(a, b *multiClientChannelUpdate) int {
				return a.activityTime.Compare(b.activityTime)
			})
			for _, update := range candidates[:min(excess, len(candidates))] {
				stickyTrim[update] = true
			}
		}
	}

	rememberDeadline := func(deadline time.Time) {
		if nextDeadline.IsZero() || deadline.Before(nextDeadline) {
			nextDeadline = deadline
		}
	}
	retire := func(update *multiClientChannelUpdate) {
		shouldSignal := !update.IsDone()
		// Mark the generation done before releasing the routing lock. An
		// in-flight send that acquired the update just before this scan can then
		// neither start another race nor re-add the detached generation to
		// clientUpdates after the reaper removed it.
		update.cancel()
		client := update.client.Load()
		update.client.Store(nil)
		if client != nil {
			if updates, ok := self.clientUpdates[client]; ok {
				delete(updates, update)
				if len(updates) == 0 {
					delete(self.clientUpdates, client)
				}
			}
		}
		retired = append(retired, retiredMultiClientFlow{
			update:       update,
			client:       client,
			shouldSignal: shouldSignal,
			stickyTrim:   stickyTrim[update],
		})
	}

	for update := range self.flowUpdates {
		if update == nil {
			delete(self.flowUpdates, update)
			continue
		}
		deadline := update.activityTime.Add(sequenceIdleTimeoutWithSettings(update.ipPath, reliabilitySettings))
		if !stickyTrim[update] && !update.IsDone() && now.Before(deadline) {
			rememberDeadline(deadline)
			continue
		}

		delete(self.flowUpdates, update)
		if update.ipPath != nil {
			switch update.ipPath.Version {
			case 4:
				ip4Path := update.ipPath.ToIp4Path()
				// A completed generation can already have been replaced at this
				// tuple. Remove routing and affinity state only when it is still
				// ours; client bookkeeping below remains generation-specific.
				if current := self.ip4PathUpdates[ip4Path]; current == update {
					delete(self.ip4PathUpdates, ip4Path)
					for affinityIp4Path := range update.affinityIp4Paths {
						if paths, ok := self.affinityIp4Paths[affinityIp4Path]; ok {
							delete(paths, ip4Path)
							if len(paths) == 0 {
								delete(self.affinityIp4Paths, affinityIp4Path)
							}
						}
					}
				}
			case 6:
				ip6Path := update.ipPath.ToIp6Path()
				if current := self.ip6PathUpdates[ip6Path]; current == update {
					delete(self.ip6PathUpdates, ip6Path)
					for affinityIp6Path := range update.affinityIp6Paths {
						if paths, ok := self.affinityIp6Paths[affinityIp6Path]; ok {
							delete(paths, ip6Path)
							if len(paths) == 0 {
								delete(self.affinityIp6Paths, affinityIp6Path)
							}
						}
					}
				}
			}
		}
		retire(update)
	}

	if !nextDeadline.IsZero() {
		return retired, max(time.Duration(0), nextDeadline.Sub(now)), true
	}
	return retired, 0, false
}

func (self *RemoteUserNatMultiClient) finishRetiredFlows(retired []retiredMultiClientFlow) {
	for _, flow := range retired {
		if flow.stickyTrim {
			self.reliabilityMetrics.stickyFlowRetired()
		}
		if flow.client != nil {
			self.recordAffinityPerformance(flow.update, flow.client)
			self.recordFlowReward(flow.update.ipPath, flow.update.pinAppId, flow.client, flow.client.flowCount())
		}
		if flow.shouldSignal && self.ctx.Err() == nil {
			self.rstFlow(flow.update.ipPath, flow.client, flow.update.sourceRstSequence())
		}
		// The update was cancelled while it was detached so in-flight work cannot
		// resurrect it. Close now clears race state and returns pooled packets;
		// shouldSignal preserves the historical idle teardown despite that early
		// cancellation.
		flow.update.Close()
	}
}

// finishQuarantinedTcpFlows signals only the local/source endpoint. The
// provider was quarantined because its receive path is silent, so sending a
// destination RST through that same path cannot make browser recovery faster
// and can itself wait behind the congestion being diagnosed. The source RST
// is the useful half: it makes H1 retry immediately on a fresh flow.
func (self *RemoteUserNatMultiClient) finishQuarantinedTcpFlows(retired []retiredMultiClientFlow) {
	for _, flow := range retired {
		if flow.client != nil {
			self.recordAffinityPerformance(flow.update, flow.client)
		}
		if self.ctx != nil && self.ctx.Err() == nil {
			if packet, ok := self.teardownSourcePacket(flow.update.ipPath, flow.update.sourceRstSequence()); ok {
				self.enqueueRemovalReceive(&receivePacket{
					Source:      TransferPath{},
					ProvideMode: protocol.ProvideMode_Network,
					IpPath:      flow.update.ipPath,
					Packet:      packet,
				})
			}
		}
		flow.update.Close()
	}
}

func stopFlowReaperTimer(timer *time.Timer) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
}

func (self *RemoteUserNatMultiClient) runFlowReaper() {
	timer := time.NewTimer(time.Hour)
	stopFlowReaperTimer(timer)
	defer timer.Stop()

	for {
		retired, nextDelay, hasNext := self.detachIdleFlows(time.Now())
		self.finishRetiredFlows(retired)
		if 0 < len(retired) {
			// Finishing can take long enough for another deadline to mature.
			// Re-scan before arming rather than sleeping on a stale minimum.
			continue
		}

		if !hasNext {
			select {
			case <-self.ctx.Done():
				return
			case <-self.flowReaperWake:
				continue
			}
		}

		timer.Reset(nextDelay)
		select {
		case <-self.ctx.Done():
			stopFlowReaperTimer(timer)
			return
		case <-self.flowReaperWake:
			stopFlowReaperTimer(timer)
		case <-timer.C:
		}
	}
}

// rstFlow sends a reset to both ends of a flow being torn down. like
// the shared idle reaper it runs outside the parent state lock and is a method,
// not an inline closure, to avoid a per-packet allocation in sendUpdate.
// teardownSourcePacket builds the packet that tells the source its flow is
// gone: a rst for tcp, and for udp an icmp unreachable when
// `UdpTeardownSignal` is set. `ipPath` is the flow's own direction (source to
// destination); the returned packet is already addressed back toward the
// source. false means there is nothing to send, which is the pre-existing
// behavior for udp and for every non-tcp, non-udp protocol.
func (self *RemoteUserNatMultiClient) teardownSourcePacket(ipPath *IpPath, sourceRstSequence uint32) ([]byte, bool) {
	if packet, ok := ipOosRstSequence(ipPath.Reverse(), sourceRstSequence); ok {
		return packet, true
	}
	if self.reliabilitySettings().UdpTeardownSignal {
		return ipOosUnreachable(ipPath)
	}
	return nil, false
}

func (self *RemoteUserNatMultiClient) rstFlow(ipPath *IpPath, client *multiClientChannel, sourceRstSequence uint32) {
	// Bare fixture channels have no underlying transfer client. Production
	// channels always do; keeping the source reset below independent also
	// makes local recovery robust if a channel loses its transport first.
	if client != nil && client.client != nil {
		// rst to destination
		if packet, ok := ipOosRst(ipPath); ok {
			client.Send(&parsedPacket{
				packet: packet,
				ipPath: ipPath,
			}, 0)
		}
	}
	// teardown to source
	if packet, ok := self.teardownSourcePacket(ipPath, sourceRstSequence); ok {
		self.enqueueRemovalReceive(&receivePacket{
			Source:      TransferPath{},
			ProvideMode: protocol.ProvideMode_Network,
			IpPath:      ipPath,
			Packet:      packet,
		})
	}
}

// returns the flow's update, the client it was previously associated with (for
// the caller's `clientUpdates` bookkeeping), and the current client to send to.
// the current client is read here under the parent lock that is already held,
// so the egress hot path does not reacquire the parent lock to read it.
func (self *RemoteUserNatMultiClient) sendUpdate(ipPath *IpPath, pin flowPin) (
	*multiClientChannelUpdate,
	*multiClientChannel,
	*multiClientChannel,
) {
	// a closed multi-client accepts no new flow generations: Close has
	// already cleared the tables, and an entry created after that clear would
	// leave a map slot outside the shared reaper's lifetime
	if self.ctx.Err() != nil {
		return nil, nil, nil
	}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	switch ipPath.Version {
	case 4:
		ip4Path := ipPath.ToIp4Path()
		var previousClient *multiClientChannel
		update, ok := self.ip4PathUpdates[ip4Path]
		if !ok || update.IsDone() {
			update = newMultiClientChannelUpdate(self.ctx, ipPath)
			self.reliabilityMetrics.flowOpened()
			self.ip4PathUpdates[ip4Path] = update
			if self.flowUpdates == nil {
				self.flowUpdates = map[*multiClientChannelUpdate]bool{}
			}
			self.flowUpdates[update] = true
			self.notifyFlowReaper()

			var affinityIpPaths []*IpPath
			if self.settings.DestinationAffinity {
				affinityIpPaths = self.affinityIpPathsWithLock(ipPath)
			}
			// the G-4b pin. A pinned app's flows join ONE app-scoped
			// affinity group INSTEAD of their domain groups, so the app's
			// api session and every one of its cdn destinations converge on
			// one exit -- one egress ip for the whole app, with no domain
			// knowledge required. Replacing rather than adding is
			// load-bearing: a pinned flow that also joined youtube.com
			// would donate its app-chosen exit to unrelated youtube flows,
			// dragging traffic that has nothing to do with the pin onto the
			// pinned app's exit (review finding, 2026-08-03). The "app:"
			// prefix cannot collide with an eTLD+1. update.pinned
			// additionally exempts the flow's inheritance from the bench
			// follow window (see inheritAffinityClient4WithLock).
			update.pinned = pin.pinned()
			update.pinAppId = pin.appId
			hardAffinity := update.pinned || self.reliabilitySettings().FreshFlowAffinity
			if pin.appId != "" {
				affinityIpPaths = []*IpPath{{ServerName: appAffinityName(pin.appId)}}
			}
			// DNS topology is a hard placement input only for explicit pins or
			// the legacy A/B mode. Ordinary fresh flows skip the lookup and
			// reach the provider race.
			if pin.appId == "" && hardAffinity {
				self.inheritDnsExitHintWithLock(update, ipPath, affinityIpPaths)
			}

			followWinner, sawScatter := false, false
			for _, affinityIpPath := range affinityIpPaths {
				affinityIp4Path := affinityIpPath.ToIp4Path()
				update.affinityIp4Paths[affinityIp4Path] = true
				if !hardAffinity {
					// Retain only the update-local measurement key. The
					// reverse donor index would consume map entries and a
					// timestamp while no production decision can read it.
					continue
				}
				paths, ok := self.affinityIp4Paths[affinityIp4Path]
				if !ok {
					paths = map[Ip4Path]time.Time{}
					self.affinityIp4Paths[affinityIp4Path] = paths
				}
				paths[ip4Path] = time.Now()

				if update.client.Load() == nil {
					verdict, scattered := self.inheritAffinityClient4WithLock(update, paths)
					followWinner = followWinner || verdict == donorQuarantineFollowed
					sawScatter = sawScatter || scattered
				}
			}
			// the app pin's cross-version convergence, consulted BEFORE the
			// destination bridge below: the ip4 and ip6 affinity groups are
			// separate maps, so a dual-stack pinned app would otherwise take
			// one exit per version -- two egress ips, the failure pinning
			// exists to prevent. Ordering is load-bearing: the bridge places
			// by destination ip, which on a shared cdn address is a
			// STRANGER's exit, and letting it outrank the pin both scattered
			// the app and (through the record below) rewrote the app's
			// canonical exit to the stranger's (review finding, 2026-08-03).
			if pin.appId != "" && update.client.Load() == nil {
				reliabilitySettings := self.reliabilitySettings()
				if donor := self.appPinDonorWithLock(
					pin.appId,
					true,
					pinnedFollowWindow(reliabilitySettings.GroupFollowWindow),
					reliabilitySettings.AffinityStickyPastCap,
				); donor != nil && donor.supportsIpVersion(4) {
					update.client.Store(donor)
				}
			}

			// the flow's own groups had no donor. an established flow to this
			// destination may still exist under the key it was created with
			// before the server name was learned -- read those groups without
			// joining them, so this flow converges onto the exit already in
			// use. NOT for an app-pinned flow: its placement is the app's,
			// and a destination bridge would hand it a stranger's exit.
			if hardAffinity && update.client.Load() == nil && pin.appId == "" {
				for _, fallbackIpPath := range self.affinityFallbackIpPathsWithLock(ipPath) {
					fallbackIp4Path := fallbackIpPath.ToIp4Path()
					if update.affinityIp4Paths[fallbackIp4Path] {
						// already joined and scanned above
						continue
					}
					if paths, ok := self.affinityIp4Paths[fallbackIp4Path]; ok {
						verdict, scattered := self.inheritAffinityClient4WithLock(update, paths)
						followWinner = followWinner || verdict == donorQuarantineFollowed
						sawScatter = sawScatter || scattered
						if update.client.Load() != nil {
							break
						}
					}
				}
			}
			// record only a placement the app itself produced (its group or
			// its cross-version donor). A flow still unplaced here goes to
			// the race, which records through bindClientFlow when it commits.
			self.recordAppPinWithLock(pin.appId, update.client.Load())
			// one ledger event per flow, from the aggregate of every inherit
			// attempt above
			self.bookGroupLedger(update.client.Load() != nil, followWinner, sawScatter)
		} else {
			previousClient = update.client.Load()
		}

		update.activityTime = time.Now()
		return update, previousClient, update.client.Load()
	case 6:
		ip6Path := ipPath.ToIp6Path()
		var previousClient *multiClientChannel
		update, ok := self.ip6PathUpdates[ip6Path]
		if !ok || update.IsDone() {
			update = newMultiClientChannelUpdate(self.ctx, ipPath)
			self.reliabilityMetrics.flowOpened()
			self.ip6PathUpdates[ip6Path] = update
			if self.flowUpdates == nil {
				self.flowUpdates = map[*multiClientChannelUpdate]bool{}
			}
			self.flowUpdates[update] = true
			self.notifyFlowReaper()

			var affinityIpPaths []*IpPath
			if self.settings.DestinationAffinity {
				affinityIpPaths = self.affinityIpPathsWithLock(ipPath)
			}
			// see the v4 twin for the pin rationale
			update.pinned = pin.pinned()
			update.pinAppId = pin.appId
			hardAffinity := update.pinned || self.reliabilitySettings().FreshFlowAffinity
			if pin.appId != "" {
				affinityIpPaths = []*IpPath{{ServerName: appAffinityName(pin.appId)}}
			}
			// See the v4 twin: ordinary flows skip hard DNS placement.
			if pin.appId == "" && hardAffinity {
				self.inheritDnsExitHintWithLock(update, ipPath, affinityIpPaths)
			}

			followWinner, sawScatter := false, false
			for _, affinityIpPath := range affinityIpPaths {
				affinityIp6Path := affinityIpPath.ToIp6Path()
				update.affinityIp6Paths[affinityIp6Path] = true
				if !hardAffinity {
					continue
				}
				paths, ok := self.affinityIp6Paths[affinityIp6Path]
				if !ok {
					paths = map[Ip6Path]time.Time{}
					self.affinityIp6Paths[affinityIp6Path] = paths
				}
				paths[ip6Path] = time.Now()

				if update.client.Load() == nil {
					verdict, scattered := self.inheritAffinityClient6WithLock(update, paths)
					followWinner = followWinner || verdict == donorQuarantineFollowed
					sawScatter = sawScatter || scattered
				}
			}
			// see the v4 twin: the app pin converges across ip versions, and
			// outranks the destination bridge below
			if pin.appId != "" && update.client.Load() == nil {
				reliabilitySettings := self.reliabilitySettings()
				if donor := self.appPinDonorWithLock(
					pin.appId,
					true,
					pinnedFollowWindow(reliabilitySettings.GroupFollowWindow),
					reliabilitySettings.AffinityStickyPastCap,
				); donor != nil && donor.supportsIpVersion(6) {
					update.client.Store(donor)
				}
			}

			// the flow's own groups had no donor. an established flow to this
			// destination may still exist under the key it was created with
			// before the server name was learned -- read those groups without
			// joining them, so this flow converges onto the exit already in
			// use. NOT for an app-pinned flow; see the v4 twin.
			if hardAffinity && update.client.Load() == nil && pin.appId == "" {
				for _, fallbackIpPath := range self.affinityFallbackIpPathsWithLock(ipPath) {
					fallbackIp6Path := fallbackIpPath.ToIp6Path()
					if update.affinityIp6Paths[fallbackIp6Path] {
						// already joined and scanned above
						continue
					}
					if paths, ok := self.affinityIp6Paths[fallbackIp6Path]; ok {
						verdict, scattered := self.inheritAffinityClient6WithLock(update, paths)
						followWinner = followWinner || verdict == donorQuarantineFollowed
						sawScatter = sawScatter || scattered
						if update.client.Load() != nil {
							break
						}
					}
				}
			}
			self.recordAppPinWithLock(pin.appId, update.client.Load())
			// one ledger event per flow, from the aggregate of every inherit
			// attempt above
			self.bookGroupLedger(update.client.Load() != nil, followWinner, sawScatter)
		} else {
			previousClient = update.client.Load()
		}

		update.activityTime = time.Now()
		return update, previousClient, update.client.Load()
	default:
		panic(fmt.Errorf("Bad protocol version %d", ipPath.Version))
	}
}

func (self *RemoteUserNatMultiClient) receiveClientPath(ipPath *IpPath, callback func(*multiClientChannelUpdate)) bool {
	update := self.receiveUpdate(ipPath)
	if update == nil {
		return false
	}
	callback(update)
	return true
}

func (self *RemoteUserNatMultiClient) receiveUpdate(ipPath *IpPath) *multiClientChannelUpdate {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	switch ipPath.Version {
	case 4:
		ip4Path := ipPath.ToIp4Path()
		update := self.ip4PathUpdates[ip4Path]
		if update != nil {
			update.activityTime = time.Now()
			return update
		}
	case 6:
		ip6Path := ipPath.ToIp6Path()
		update := self.ip6PathUpdates[ip6Path]
		if update != nil {
			update.activityTime = time.Now()
			return update
		}
	default:
		panic(fmt.Errorf("Bad protocol version %d", ipPath.Version))
	}

	return nil
}

/*
func (self *RemoteUserNatMultiClient) updateClient(update *multiClientChannelUpdate, previousClient *multiClientChannel) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	client := update.client

	if previousClient != client {
		if previousClient != nil {
			if updates, ok := self.clientUpdates[previousClient]; ok {
				delete(updates, update)
				if len(updates) == 0 {
					delete(self.clientUpdates, previousClient)
				}
			}
		}
		if client != nil && !client.IsDone() {
			updates, ok := self.clientUpdates[client]
			if !ok {
				updates = map[*multiClientChannelUpdate]bool{}
				self.clientUpdates[client] = updates
			}
			updates[update] = true
		}
	}
}
*/

// remove a client from all updates.
//
// Removal is no longer pure teardown. An established quic flow (udp/443 with
// inbound data seen) does not have to die with its exit: quic keys the
// connection on the connection id, not the 4-tuple (RFC 9000 §5.1), and udp
// has no split-tcp terminated state to lose -- so the same connection
// arriving from a replacement's egress address is a path migration the server
// answers with path validation (§9), not a broken connection. Those flows are
// re-pinned to a live replacement inside the removal itself, so the app's
// very next packet (at worst a quic PTO probe) egresses through a warm exit:
// recovery in roughly one packet interval against the measured ~3.6s of
// waiting for the next app packet plus a full race. Quic is 56% of traffic,
// so this is most of what an exit death used to cost. Everything else --
// tcp (split-tcp: the exit held the remote end, nothing to migrate,
// fail-fast rst is correct), unestablished flows (no proven connection to
// migrate; their own re-race already covers them), and rebindable flows with
// no live under-cap replacement -- keeps the teardown-with-signal behavior.
func (self *RemoteUserNatMultiClient) removeClient(client *multiClientChannel) {
	// Replacement candidates are gathered BEFORE the parent-locked section.
	// The gather reads each window's ordered offer (the window's own lock)
	// and each candidate's stats clock (the channel's lock, heavyweight
	// bucket coalescing) -- work that must not run under the parent
	// stateLock, which every flow lookup contends for, and the window lock
	// and parent lock deliberately have no order between them (they are
	// never held together anywhere in this file). The list may be slightly
	// stale by assignment time, which is fine: every candidate is
	// re-validated under the parent lock before a flow is stored onto it.
	var candidates []*multiClientChannel
	if self.reliabilitySettings().QuicRebindOnExitLoss {
		candidates = self.rebindCandidates(client)
	}

	rstPackets := []*receivePacket{}
	// the flows that could not be rebound die with the exit -- split-tcp
	// means the exit holds the remote end of a tcp connection, so there is
	// nothing to migrate, and a udp flow with no live replacement has
	// nowhere to go. the count is the blast radius of one provider failure.
	lostDestinations := []recoveryKey{}
	// the flows re-pinned to a replacement, each with the local source port
	// it was using -- the recovery tracker classifies the recovery by
	// whether the destination answers that same port (migration accepted)
	// or a new one (the app re-dialed).
	reboundFlows := []reboundFlow{}
	reboundReplacementCount := 0

	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		// note client must be marked as done, otherwise it may be re-added by updates in flight
		if !client.IsDone() {
			self.log.Errorf("[multi]removed client that is not marked as done. This might lead to memory leak.")
		}

		// evict this exit's demotion streaks (every class) now that its
		// channel is gone -- see demotionKey's doc for why this, and not a
		// size-bound LRU like qualification's, is the right lifecycle for
		// this particular map: an unbounded map keyed by exit identity would
		// otherwise leak across a long session's churn.
		//
		// THIS MUST STAY ABOVE the clientUpdates lookup below. That lookup
		// returns early when the exit never had a flow bound to it, and
		// demotionObserve creates an entry for whichever exit is at the head
		// of the candidate list whether or not it ever wins a race -- so an
		// exit that is repeatedly ranked first and never chosen is exactly
		// the case that leaks. Evicting after the early return covered only
		// the exits that could never leak.
		clientId := client.ClientId()
		for key := range self.demotionStates {
			if key.clientId == clientId {
				delete(self.demotionStates, key)
			}
		}

		updates, ok := self.clientUpdates[client]
		if !ok {
			return
		}
		delete(self.clientUpdates, client)

		// partition the dying exit's flows: established quic moves, the rest
		// gets the teardown signal
		rebindable := []*multiClientChannelUpdate{}
		teardownUpdates := []*multiClientChannelUpdate{}
		for update, _ := range updates {
			if update.client.Load() != client {
				self.log.Errorf("[multi]update associated with incorrect client")
				continue
			}
			// established (receivedInbound) is load-bearing twice over: it
			// is the proof there is a connection to migrate, and it is the
			// guard that exempts the flow from the dial-failure inference --
			// so a flow rebound onto a dead replacement has no fast escape,
			// which is why the candidate gather prefers demonstrably-alive
			// exits and the blackhole detector remains the backstop.
			if 0 < len(candidates) &&
				update.ipPath.Protocol == IpProtocolUdp &&
				update.ipPath.DestinationPort == 443 &&
				update.receivedInbound.Load() {
				rebindable = append(rebindable, update)
			} else {
				teardownUpdates = append(teardownUpdates, update)
			}
		}

		var unplaced []*multiClientChannelUpdate
		reboundFlows, reboundReplacementCount, unplaced = self.rebindFlowsWithLock(client, rebindable, candidates)
		// a rebindable flow that found no under-cap live replacement falls
		// back to exactly the old behavior -- a flow is never left silently
		// unpinned without its teardown signal
		teardownUpdates = append(teardownUpdates, unplaced...)

		for _, update := range teardownUpdates {
			update.client.Store(nil)

			// the update's ipPath is egress-oriented, so the remote
			// endpoint the user is waiting on is the destination.
			lostDestinations = append(lostDestinations, newRecoveryKey(
				update.ipPath.DestinationIp,
				update.ipPath.DestinationPort,
			))

			// rebound flows deliberately do not pass through here: the
			// teardown signal for udp is an icmp unreachable, which cannot
			// close a quic connection anyway -- RFC 9000 requires endpoints
			// to treat unauthenticated network signals as at most advisory,
			// so it is inert -- and the flow is not dead, it moved.
			if packet, ok := self.teardownSourcePacket(update.ipPath, update.sourceRstSequence()); ok {
				rstPacket := &receivePacket{
					Source:      TransferPath{},
					ProvideMode: protocol.ProvideMode_Network,
					IpPath:      update.ipPath,
					Packet:      packet,
				}
				rstPackets = append(rstPackets, rstPacket)
			}
		}
	}()

	// recorded outside stateLock -- the metrics take their own lock, and
	// nesting them under the parent lock would put the recovery tracker on the
	// path every flow lookup contends for.
	self.reliabilityMetrics.exitLost(lostDestinations)
	self.reliabilityMetrics.exitLostRebound(reboundFlows)

	// the removal summary: what this exit death cost and how much of it was
	// recovered in place. one line per removal that affected any flow -- a
	// flowless removal is routine window churn and stays quiet, matching the
	// teardown logging below. this line is also the on-device proof the
	// rebind path ran at all, per the measurement protocol.
	if 0 < len(reboundFlows) || 0 < len(lostDestinations) {
		self.log.Infof("%s\n", relEvent(
			"rebind",
			"exit", client.ClientId(),
			"rebound", len(reboundFlows),
			"replacements", reboundReplacementCount,
			"torndown", len(lostDestinations),
		))
	}

	// a client removed while carrying nothing is routine window churn --
	// collapsing the lowest-weighted client, rank-based removal -- and logging
	// those buries the removals that actually cost a user something
	teardownWorthLogging := 0 < len(rstPackets) || 0 < len(lostDestinations)

	select {
	case <-self.ctx.Done():
		// the teardown is dropped when the client is shutting down. worth
		// logging: it is the one case where flows die with no signal at all,
		// and it is otherwise indistinguishable from a teardown that was sent
		// and ignored.
		if teardownWorthLogging {
			self.log.Infof(
				"[multi]teardown skipped, context done: %d packet(s) for %d flow(s) of client %s\n",
				len(rstPackets), len(lostDestinations), client.ClientId(),
			)
		}
	default:
		// whether the peer is told is the difference between a flow that fails
		// fast and one that hangs until the app's own timeout. on device a
		// dropped exit left a download at 0bps rather than erroring, and there
		// was no way to tell whether the teardown was never built, never sent,
		// or sent and ignored. log the emission so those are distinguishable.
		if teardownWorthLogging {
			// the historical format is preserved EXACTLY up to the separator:
			// the owner's workflow and this session's greps read this line, and
			// a log line external tooling parses is an interface. The [rel] twin
			// rides on the same line rather than a second one so the two can
			// never be separated by interleaving from another goroutine.
			self.log.Infof(
				"[multi]teardown sending %d packet(s) for %d flow(s) of client %s | %s\n",
				len(rstPackets), len(lostDestinations), client.ClientId(),
				relEvent(
					"teardown",
					"exit", client.ClientId(),
					"packets", len(rstPackets),
					"flows", len(lostDestinations),
				),
			)
		}
		for _, p := range rstPackets {
			if self.log.V(1).Enabled() {
				// IpPath has no String(), so expand the fields -- %s on the
				// struct prints an unreadable blob with the ports and flags
				// mangled, which is useless for the one job this line has
				self.log.Infof(
					"[multi]teardown -> ipv%d p%v %s:%d->%s:%d\n",
					p.IpPath.Version, p.IpPath.Protocol,
					p.IpPath.SourceIp, p.IpPath.SourcePort,
					p.IpPath.DestinationIp, p.IpPath.DestinationPort,
				)
			}
			self.enqueueRemovalReceive(p)
		}
	}
}

// migrateClientFlows is the common proactive hand-off for a planned drain and
// a sustained quarantine verdict. Established QUIC flows are re-pinned to
// live replacements in both cases. A planned drain keeps split-TCP flows alive
// so they can finish naturally. A quarantine is materially different: the
// exit has already gone receive-silent, so preserving established H1 flows is
// preserving the user's freeze. Those TCP flows are detached and reset toward
// the source immediately, which makes the browser open a new connection on a
// healthy exit.
//
// The quarantine pass also invalidates every DNS, site, and app-affinity
// reference still naming the failed exit. This happens under the same parent
// lock as the flow detach; affinityDonorEligible already refuses quarantine,
// closing the smaller channel-state -> parent-callback race before this pass
// begins.
//
// Returns flows moved, replacement exits used, and flows that remain. During a
// planned drain, TCP remains deliberately; during quarantine, remaining flows
// are non-TCP flows that could not safely move or lacked replacement headroom.
func (self *RemoteUserNatMultiClient) migrateClientFlows(client *multiClientChannel, cause string) (rebound int, replacements int, remaining int) {
	if client == nil {
		return
	}
	// the same gate as the removal-time rebind: with the mechanism off, a
	// drain behaves exactly as it did before this existed
	var candidates []*multiClientChannel
	if self.reliabilitySettings().QuicRebindOnExitLoss {
		candidates = self.rebindCandidates(client)
	}

	var reboundFlows []reboundFlow
	var retiredTcp []retiredMultiClientFlow
	movable := 0
	affinityInvalidations := 0
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		quarantine := cause == "bench"

		if quarantine {
			for name, hint := range self.dnsExitHints {
				if hint.client == client {
					delete(self.dnsExitHints, name)
					affinityInvalidations += 1
				}
			}
			for addr, hint := range self.dnsAddressExitHints {
				if hint.client == client {
					delete(self.dnsAddressExitHints, addr)
					affinityInvalidations += 1
				}
			}
			for appId, pinned := range self.appPinClients {
				if pinned == client {
					delete(self.appPinClients, appId)
					affinityInvalidations += 1
				}
			}
		}

		updates, ok := self.clientUpdates[client]
		if !ok {
			return
		}

		rebindable := []*multiClientChannelUpdate{}
		for update := range updates {
			if update.client.Load() != client {
				continue
			}
			// removeClient's partition: established quic moves; everything
			// else stays on a planned drain. A quarantine's TCP partition is
			// detached below after QUIC has had first use of its affinity groups.
			if 0 < len(candidates) &&
				update.ipPath.Protocol == IpProtocolUdp &&
				update.ipPath.DestinationPort == 443 &&
				update.receivedInbound.Load() {
				rebindable = append(rebindable, update)
			}
		}
		if 0 < len(rebindable) && 0 < len(candidates) {
			movable = len(rebindable)
			reboundFlows, replacements, _ = self.rebindFlowsWithLock(client, rebindable, candidates)
		}
		// unlike the removal, unplaced flows are NOT torn down: they stay
		// registered on the alive exit and keep working

		// the moved flows leave the draining exit's book, so the cap, the
		// flowless check, and the eventual close's teardown all see the
		// truth. rebindFlowsWithLock stored the replacement into each moved
		// update's client, so "moved" is exactly "no longer points here" --
		// the same catch-up bindClientFlow performs on the send path,
		// inlined because bindClientFlow takes this lock. After the sweep,
		// the book holds exactly the flows still riding this exit, which is
		// the honest remaining count (the pre-sweep subtraction counted
		// stale entries as stayers; review finding, 2026-08-03).
		for update := range updates {
			if update.client.Load() != client {
				delete(updates, update)
			}
		}

		if quarantine {
			// Remove one flow's membership from every site-affinity index. The
			// flow tuple itself is removed only for TCP below; UDP can remain on
			// the benched exit without donating that exit to another handshake.
			invalidateFlowAffinity := func(update *multiClientChannelUpdate) {
				if update == nil || update.ipPath == nil {
					return
				}
				switch update.ipPath.Version {
				case 4:
					path := update.ipPath.ToIp4Path()
					for affinityPath := range update.affinityIp4Paths {
						if paths := self.affinityIp4Paths[affinityPath]; paths != nil {
							if _, found := paths[path]; found {
								delete(paths, path)
								affinityInvalidations += 1
							}
							if len(paths) == 0 {
								delete(self.affinityIp4Paths, affinityPath)
							}
						}
						delete(update.affinityIp4Paths, affinityPath)
					}
				case 6:
					path := update.ipPath.ToIp6Path()
					for affinityPath := range update.affinityIp6Paths {
						if paths := self.affinityIp6Paths[affinityPath]; paths != nil {
							if _, found := paths[path]; found {
								delete(paths, path)
								affinityInvalidations += 1
							}
							if len(paths) == 0 {
								delete(self.affinityIp6Paths, affinityPath)
							}
						}
						delete(update.affinityIp6Paths, affinityPath)
					}
				}
			}

			for update := range updates {
				if update.client.Load() != client {
					delete(updates, update)
					continue
				}
				invalidateFlowAffinity(update)
				if update.ipPath == nil || update.ipPath.Protocol != IpProtocolTcp {
					continue
				}

				// Detach the exact generation before signaling the source. A SYN
				// that follows the RST then creates a clean generation and races a
				// healthy exit; no stale nil-client update can resurrect the old
				// provider binding.
				switch update.ipPath.Version {
				case 4:
					path := update.ipPath.ToIp4Path()
					if self.ip4PathUpdates[path] == update {
						delete(self.ip4PathUpdates, path)
					}
				case 6:
					path := update.ipPath.ToIp6Path()
					if self.ip6PathUpdates[path] == update {
						delete(self.ip6PathUpdates, path)
					}
				}
				delete(self.flowUpdates, update)
				delete(updates, update)
				update.client.Store(nil)
				if update.cancel != nil {
					update.cancel()
				}
				retiredTcp = append(retiredTcp, retiredMultiClientFlow{
					update:       update,
					client:       client,
					shouldSignal: true,
				})
			}
		}
		remaining = len(updates)
		if len(updates) == 0 {
			delete(self.clientUpdates, client)
		}
	}()
	rebound = len(reboundFlows)
	self.finishQuarantinedTcpFlows(retiredTcp)
	self.reliabilityMetrics.quarantineTcpReset(len(retiredTcp))
	self.reliabilityMetrics.quarantineAffinityInvalidated(affinityInvalidations)

	// The recovery tracker is deliberately NOT armed here, unlike the
	// removal path. Its entries close on the next provider-originated
	// ingress for the destination -- and after a migration the OLD exit is
	// still alive and still delivering until the server validates the new
	// path, so every entry would close milliseconds later as a fake ~0s
	// "recovery" and a fake rebindsAccepted, systematically corrupting the
	// two headline metrics this program is judged by (review finding,
	// 2026-08-03 -- the shipped-metric-measures-the-wrong-thing class). The
	// [rel] migrate line below is this mechanism's field signal.

	// UNCONDITIONAL, and it names its cause. rebound=0 is the interesting
	// case, not the boring one: it means the hand-off ran and moved nothing
	// (all-tcp flows, no under-cap candidate, or QuicRebindOnExitLoss off),
	// which is exactly the state behind "the exit was benched and my app
	// still hung". Logging only successes made the mechanism unfalsifiable
	// -- the shape of failure this project has shipped repeatedly (review
	// finding, 2026-08-03).
	loggerOrDefault(self.log).Infof("%s\n", relEvent(
		"migrate",
		"exit", client.ClientId(),
		"cause", cause,
		"rebound", rebound,
		"replacements", replacements,
		"remaining", remaining,
		"tcpresets", len(retiredTcp),
		"affinityinvalidated", affinityInvalidations,
		// why nothing moved, when nothing moved
		"movable", movable,
		"candidates", len(candidates),
	))
	return
}

// MigrateExit runs the drain-time flow migration against one exit on demand:
// the developer-menu drill for G-3, and useful ahead of a deliberate drop.
// Unlike DropExit nothing is killed -- established quic moves, the rest
// keeps working where it is. Returns the number of flows moved, -1 when no
// such exit is in the window.
func (self *RemoteUserNatMultiClient) MigrateExit(clientId Id) int {
	self.logAction("migrate_exit", "exit", clientId)
	for _, window := range self.windows {
		for _, client := range window.unorderedClients() {
			if client.ClientId() == clientId {
				rebound, _, _ := self.migrateClientFlows(client, "action")
				return rebound
			}
		}
	}
	return -1
}

// rebindCandidates gathers the live exits a dying exit's established quic
// flows may be re-pinned onto, ordered most-preferred first. Preference is
// recent activity (the stats lastEventTime), and that preference is not
// cosmetic: a rebound flow is established (receivedInbound), which exempts it
// from the dial-failure inference by design, so a flow re-pinned onto a dead
// replacement has no fast escape -- the blackhole detector, tens of seconds
// out, is the only backstop. An exit that demonstrably moved bytes recently
// is the best cheaply-available proof of life. A zero lastEventTime (no
// recorded events yet -- e.g. a fresh standing-reserve spare) sorts last:
// still usable, but only after every exit with actual evidence. The sort is
// stable so ties keep the window's weighted-shuffle order.
//
// Locking: takes NO parent lock. OrderedClients holds only the window's own
// lock plus each channel's stats lock (verified: orderedClients ->
// unorderedClients/WindowStats), and the extra WindowStats read here takes
// the channel lock again. That is why removeClient runs this before entering
// its parent-locked section -- the gather is too heavy for the parent lock,
// and the window lock must never order against it.
func (self *RemoteUserNatMultiClient) rebindCandidates(dying *multiClientChannel) []*multiClientChannel {
	if self.rebindCandidatesFunc != nil {
		return self.rebindCandidatesFunc(dying)
	}

	candidates := []*multiClientChannel{}
	lastEventTimes := map[*multiClientChannel]time.Time{}
	gather := func(clients []*multiClientChannel) {
		for _, c := range clients {
			if c == dying {
				continue
			}
			if _, seen := lastEventTimes[c]; seen {
				continue
			}
			// re-read the stats rather than threading times out of
			// OrderedClients, so the window's offer stays a plain client
			// list. a candidate whose stats now error is mid-removal itself
			// and is dropped here.
			stats, err := c.WindowStats()
			if err != nil {
				continue
			}
			candidates = append(candidates, c)
			lastEventTimes[c] = stats.lastEventTime
		}
	}
	for _, window := range self.windows {
		gather(window.OrderedClients())
	}
	if len(candidates) == 0 {
		// the windows' offer is empty -- every exit is warned or quarantined
		// at once, which is the normal state of a pool whose providers stall
		// intermittently under load. A benched exit is soft-suspect but
		// alive; a rebind onto it beats the alternative, which is tearing
		// down every one of the dying exit's established flows for want of a
		// clean candidate. On device this exact gap read as "0 flows rebound
		// to 0 replacements, 33 torn down" in the middle of a video.
		for _, window := range self.windows {
			gather(window.unorderedClients())
		}
	}
	// a candidate that has never delivered a byte sorts LAST, whatever its
	// event recency: a fresh dial to a qualified destination reads as proven
	// (qualification is destination-keyed) while its own transport is
	// unproven, and the field capture that motivates this (2026-08-05)
	// showed a dead-on-arrival replacement inherit 24 flows and execute 90s
	// later -- one teardown amplified into two. Proven candidates take the
	// flows while they have headroom; the unproven tier still exists so an
	// all-fresh pool can place flows at all.
	slices.SortStableFunc(candidates, func(a *multiClientChannel, b *multiClientChannel) int {
		provenA := a.hasEverReceived()
		provenB := b.hasEverReceived()
		if provenA != provenB {
			if provenA {
				return -1
			}
			return 1
		}
		// descending: most recently active first
		timeA := lastEventTimes[a]
		timeB := lastEventTimes[b]
		if timeA.After(timeB) {
			return -1
		} else if timeB.After(timeA) {
			return 1
		}
		return 0
	})
	return candidates
}

// rebindFlowsWithLock re-pins a dying exit's rebindable flows onto live
// candidates and reports what it did: the flows successfully rebound (with
// the local port the recovery tracker classifies by), how many distinct
// replacements were used, and the flows it could NOT place -- the caller owes
// those the normal teardown, because a flow must never be left silently
// unpinned.
//
// Affinity cohesion: flows sharing any affinity key are one site's
// connections, and a whole group is placed on ONE replacement wherever cap
// headroom allows, so the site sees a single coordinated egress-ip change
// instead of its connections scattering across the window. A group is split
// across candidates only when it exceeds every single candidate's remaining
// headroom. The grouping is a representative-key union: a later flow that
// bridges two groups re-registers its keys onto the earlier group rather
// than merging them -- the property that matters (a site's flows
// overwhelmingly land together) survives, and the exact partition does not
// justify a union-find here. Ungrouped flows spread least-loaded so the
// rebind itself cannot re-create the single-exit pileup the flow cap exists
// to prevent.
//
// called with stateLock. The assignment is bindClientFlow's map maintenance
// inlined: bindClientFlow takes the parent lock and sync.Mutex is not
// reentrant, so calling it from here would deadlock. Its scan-other-clients
// step is unnecessary in this context -- every update here was registered
// under exactly one client, the dying one, whose whole set the caller just
// detached. Candidate re-validation uses only what is safe under the parent
// lock: IsDone (lock-free ctx read), isWarning (the brief channel-lock
// nesting the affinity inherit path already performs -- the reverse order,
// channel lock then parent lock, is what never happens, see clientFlowCount),
// and clientAtFlowCapWithLock (parent state, NOT underFlowCap, which
// re-takes the parent lock).
func (self *RemoteUserNatMultiClient) rebindFlowsWithLock(
	dying *multiClientChannel,
	rebindable []*multiClientChannelUpdate,
	candidates []*multiClientChannel,
) (rebounds []reboundFlow, replacementCount int, unplaced []*multiClientChannelUpdate) {
	if len(rebindable) == 0 {
		return
	}

	maxFlows := self.reliabilitySettings().MaxFlowsPerExit

	// A benched exit is better than no exit, and an over-cap exit is better
	// than a teardown -- the same ladder raceCandidatesFrom already walks for
	// placement, applied here.
	//
	// Both relaxations exist because the strict predicates below are
	// self-defeating in exactly the states that produce mass teardowns
	// (review finding, 2026-08-03):
	//
	//   - warned: rebindCandidates falls back to unorderedClients when every
	//     window's ordered offer is empty -- but the offer is empty PRECISELY
	//     because every live exit is warned, so every candidate the fallback
	//     can add is warned, and a `!isWarning()` test rejects all of them.
	//     The fallback could never place a single flow.
	//   - capped: sticky affinity deliberately lets a heavy constellation
	//     grow past MaxFlowsPerExit on one exit, so the LARGEST groups are
	//     the ones no candidate has headroom for -- they were split across
	//     exits (several egress ips for one site, which is the failure the
	//     affinity work exists to prevent) or torn down entirely.
	//
	// The cap keeps its meaning for the ordinary case: it is consulted
	// first, and only a group that cannot be placed under it falls through.
	lastResort := false
	usable := func(c *multiClientChannel) bool {
		if c == dying || c.IsDone() {
			return false
		}
		if lastResort {
			// still never a DRAINING or unhealthy-warned exit's own
			// death-in-progress: IsDone above covers that. Quarantine and
			// warning are soft, and a soft-suspect live exit beats a
			// guaranteed teardown.
			return true
		}
		return !c.isWarning() && !self.clientAtFlowCapWithLock(c)
	}
	// headroom is how many more flows a candidate may take; negative means
	// unbounded (the cap is off, or this is the last-resort pass where the
	// alternative to over-filling is destroying the flows). recomputed per
	// read because assignments below grow clientUpdates as they go, which
	// keeps the cap honest across groups.
	headroom := func(c *multiClientChannel) int {
		if maxFlows <= 0 || lastResort {
			return -1
		}
		return maxFlows - len(self.clientUpdates[c])
	}

	usedReplacements := map[*multiClientChannel]bool{}
	// which pinned apps have already had their canonical exit recorded by
	// this rebind; see the record in assign
	rebindRecordedPins := map[string]bool{}
	assign := func(update *multiClientChannelUpdate, replacement *multiClientChannel) {
		update.client.Store(replacement)
		updates, ok := self.clientUpdates[replacement]
		if !ok {
			updates = map[*multiClientChannelUpdate]bool{}
			self.clientUpdates[replacement] = updates
		}
		updates[update] = true
		if !usedReplacements[replacement] {
			usedReplacements[replacement] = true
			replacementCount += 1
		}
		// a rebound pinned flow moves the app's canonical exit with it, so
		// the other ip version converges on the replacement rather than
		// chasing the dying exit. FIRST writer wins per app within one
		// rebind: candidates are consumed in preference order, so a group
		// split across several replacements would otherwise leave the app
		// recorded on the LAST, least-preferred fragment (the one that took
		// the smallest share) and pull every later flow of the app onto it
		// -- splitting the app further, which is what the pin exists to
		// prevent (review finding, 2026-08-03).
		if update.pinAppId != "" && !rebindRecordedPins[update.pinAppId] {
			rebindRecordedPins[update.pinAppId] = true
			self.recordAppPinWithLock(update.pinAppId, replacement)
		}
		// the update's ipPath is egress-oriented: destination is the remote
		// endpoint the tracker keys on, source port is the local port whose
		// answer classifies the recovery
		rebounds = append(rebounds, reboundFlow{
			key: newRecoveryKey(
				update.ipPath.DestinationIp,
				update.ipPath.DestinationPort,
			),
			localPort: update.ipPath.SourcePort,
		})
	}

	// group by affinity membership: flows sharing any key belong together
	type rebindGroup struct {
		updates []*multiClientChannelUpdate
	}
	groups := []*rebindGroup{}
	groupByIp4 := map[Ip4Path]*rebindGroup{}
	groupByIp6 := map[Ip6Path]*rebindGroup{}
	ungrouped := []*multiClientChannelUpdate{}
	for _, update := range rebindable {
		if len(update.affinityIp4Paths) == 0 && len(update.affinityIp6Paths) == 0 {
			ungrouped = append(ungrouped, update)
			continue
		}
		var group *rebindGroup
		for affinityIp4Path, _ := range update.affinityIp4Paths {
			if g, ok := groupByIp4[affinityIp4Path]; ok {
				group = g
				break
			}
		}
		if group == nil {
			for affinityIp6Path, _ := range update.affinityIp6Paths {
				if g, ok := groupByIp6[affinityIp6Path]; ok {
					group = g
					break
				}
			}
		}
		if group == nil {
			group = &rebindGroup{}
			groups = append(groups, group)
		}
		group.updates = append(group.updates, update)
		for affinityIp4Path, _ := range update.affinityIp4Paths {
			groupByIp4[affinityIp4Path] = group
		}
		for affinityIp6Path, _ := range update.affinityIp6Paths {
			groupByIp6[affinityIp6Path] = group
		}
	}

	// a replacement must carry every flow it is handed (IPV6.md B3): a group
	// is narrowed to the candidates that can carry all of its versions, and
	// a single flow to the candidates that can carry its own
	carriesUpdates := func(c *multiClientChannel, updates []*multiClientChannelUpdate) bool {
		for _, update := range updates {
			if update.ipPath != nil && !c.supportsIpVersion(update.ipPath.Version) {
				return false
			}
		}
		return true
	}
	for _, group := range groups {
		remaining := group.updates
		groupCandidates := []*multiClientChannel{}
		for _, c := range candidates {
			if carriesUpdates(c, remaining) {
				groupCandidates = append(groupCandidates, c)
			}
		}
		// first choice: the most-preferred candidate that holds the WHOLE
		// group, so the site sees one egress ip
		for _, c := range groupCandidates {
			if !usable(c) {
				continue
			}
			if h := headroom(c); h < 0 || len(remaining) <= h {
				for _, update := range remaining {
					assign(update, c)
				}
				remaining = nil
				break
			}
		}
		// no single candidate fits: split, filling in preference order --
		// two egress ips beat losing the site's flows outright
		for _, c := range groupCandidates {
			if len(remaining) == 0 {
				break
			}
			if !usable(c) {
				continue
			}
			n := len(remaining)
			if h := headroom(c); 0 <= h && h < n {
				n = h
			}
			for _, update := range remaining[:n] {
				assign(update, c)
			}
			remaining = remaining[n:]
		}
		unplaced = append(unplaced, remaining...)
	}

	// ungrouped flows go least-loaded among the usable candidates, ties
	// broken by candidate preference order
	for _, update := range ungrouped {
		var best *multiClientChannel
		bestCount := 0
		for _, c := range candidates {
			if !usable(c) || !carriesUpdates(c, []*multiClientChannelUpdate{update}) {
				continue
			}
			if count := len(self.clientUpdates[c]); best == nil || count < bestCount {
				best = c
				bestCount = count
			}
		}
		if best == nil {
			unplaced = append(unplaced, update)
			continue
		}
		assign(update, best)
	}

	// the last-resort pass. Everything above ran under the strict
	// predicates; whatever is still unplaced was about to be torn down (in
	// removeClient) or abandoned on a dying exit. Re-run those flows with
	// warned and over-cap candidates admitted, least-loaded first, because a
	// soft-suspect or busy exit beats a destroyed connection. Affinity
	// cohesion is deliberately not attempted here -- by this point the
	// choice is placement or nothing.
	if 0 < len(unplaced) {
		lastResort = true
		stillUnplaced := []*multiClientChannelUpdate{}
		for _, update := range unplaced {
			var best *multiClientChannel
			bestCount := 0
			for _, c := range candidates {
				if !usable(c) || !carriesUpdates(c, []*multiClientChannelUpdate{update}) {
					continue
				}
				if count := len(self.clientUpdates[c]); best == nil || count < bestCount {
					best = c
					bestCount = count
				}
			}
			if best == nil {
				stillUnplaced = append(stillUnplaced, update)
				continue
			}
			assign(update, best)
		}
		unplaced = stillUnplaced
		lastResort = false
	}

	return
}

// MultiClientGeneratorTransportMigrator is an optional generator capability
// for make-before-break drain migration. Window clients own platform
// transports through their generator, so the generator is the only layer
// with enough information to construct a replacement using the same auth and
// route manager. Implementations must return promptly and deduplicate
// overlapping requests; the existing window/client lifecycle remains the
// fallback for generators that do not implement it.
type MultiClientGeneratorTransportMigrator interface {
	MigrateClientTransport(client *Client, args *MultiClientGeneratorClientArgs, migrateTime time.Time)
}

// MultiClientGeneratorWithExtenderIps is an optional generator capability: the
// extender addresses carrying one window client's live platform transport
// (K1). The generator owns the transports, so it is the only layer that can
// answer, and it answers across transport generations -- a migration
// replacement is a change like any other. A generator without it publishes no
// extenders, which is what a P2P-only or fixture window does.
type MultiClientGeneratorWithExtenderIps interface {
	// The addresses and a channel that closes when they may have changed. The
	// pair must be taken together, with the subscribe immediately before the
	// read. A nil channel means there is nothing to watch.
	ClientExtenderIps(client *Client) ([]netip.Addr, <-chan struct{})
}

// the icmp send gate is not part of a normal handshake; it flips to a
// default-on release once the provider fleet broadly parses icmp (see ICMP.md)
var errIcmpDisabled = errors.New("icmp send is disabled")

func (self *RemoteUserNatMultiClient) logSparseSendDrop(
	category string,
	counter *atomic.Uint64,
	err error,
) {
	count := counter.Add(1)
	if count == 0 || count&(count-1) != 0 {
		return
	}
	self.log.Infof(
		"[multi]send bad packet (%s; count=%d) = %s\n",
		category,
		count,
		err,
	)
}

// smtpBlockActionParts performs the ordinary association/override lookup for
// the two SMTP decisions that occur before the general security policy. This
// keeps the explicit local route and an encryption rejection visible through
// the same BlockAction stream as every other egress routing decision.
func (self *RemoteUserNatMultiClient) smtpBlockActionParts(
	ipPath *IpPath,
) (decision *blockActionDecision, match *blockActionMatch, blockerBlock bool) {
	ignored := self.blockActionIgnored(ipPath)
	if !ignored && self.ipAssoc != nil {
		self.ipAssoc.AddEgressPacket(ipPath)
	}

	blockActionState := self.blockActionState.Load()
	config := self.config.Load()
	blockerActive := config.blocker != nil && config.blocker.Enabled()
	if !ignored && (blockActionState.matcher != nil || self.blockActionCollector.hasCallbacks() || blockerActive) {
		decision = self.blockActionDecision(blockActionState, config.blocker, blockerActive, ipPath)
	}
	if decision != nil {
		match = decision.match
		blockerBlock = decision.blockerBlock
	}
	return
}

// sendSmtpLocal routes TCP/25 directly regardless of localSecurityBypass. The
// forced Drop+local combination is deliberate: block and blocker overrides
// still work, but a route override cannot move port 25 onto a provider.
func (self *RemoteUserNatMultiClient) sendSmtpLocal(
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	packet []byte,
) bool {
	decision, match, blockerBlock := self.smtpBlockActionParts(ipPath)
	block, local := blockActionApply(
		SecurityPolicyResultDrop,
		true,
		blockerBlock,
		match,
	)
	byteCount := ByteCount(len(packet))
	if decision != nil && self.blockActionCollector.hasCallbacks() {
		self.blockActionCollector.add(decision, block, local, match, byteCount)
	}
	if block {
		self.packetStatsCounters.blockEgressPacketCount.Add(1)
		self.packetStatsCounters.blockEgressByteCount.Add(int64(byteCount))
		return false
	}
	if !local || self.localUserNat == nil {
		return false
	}
	success := self.localUserNat.SendPacket(source, provideMode, packet, 0)
	if success {
		self.packetStatsCounters.localEgressPacketCount.Add(1)
		self.packetStatsCounters.localEgressByteCount.Add(int64(byteCount))
	}
	return success
}

func (self *RemoteUserNatMultiClient) rejectSmtpPacket(
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	packet []byte,
) {
	decision, _, _ := self.smtpBlockActionParts(ipPath)
	byteCount := ByteCount(len(packet))
	if decision != nil && self.blockActionCollector.hasCallbacks() {
		// Encryption enforcement is not overridable. Pass no match so a user
		// override is not reported as the cause of this mandatory rejection.
		self.blockActionCollector.add(decision, true, false, nil, byteCount)
	}
	self.packetStatsCounters.blockEgressPacketCount.Add(1)
	self.packetStatsCounters.blockEgressByteCount.Add(int64(byteCount))
	self.logSparseSendDrop("smtp encryption", &self.sendSmtpDropCount, errSmtpEncryptionRequired)
	deliverTcpPolicyReset(self.deliverReceivePacket, source, provideMode, ipPath, packet)
}

// `SendPacketFunction`
func (self *RemoteUserNatMultiClient) SendPacket(
	source TransferPath,
	provideMode protocol.ProvideMode,
	packet []byte,
	timeout time.Duration,
) bool {
	if isIpFragmentPacket(packet) {
		result := self.egressIpv4Fragments.processOwned(
			source,
			TransferKey{},
			provideMode,
			packet,
		)
		if result.packet != nil {
			if self.sendReassembledUdpFragments(
				source,
				provideMode,
				result.packet,
				result.fragments,
				timeout,
			) {
				result.fragments = nil
			}
		}
		returnIpFragmentProcessResult(result)
		// The fragment owner has been consumed by the bounded gate even when
		// the completed datagram is later rejected by security policy.
		return true
	}

	relationship := egressRelationship(provideMode, self.provideMode)

	ipPath, payload, err := ParseIpPathWithPayload(packet)
	if err != nil {
		self.logSparseSendDrop("parse", &self.sendParseDropCount, err)
		return false
	}
	if ipPath.Protocol == IpProtocolIcmp && !self.settings.EnableIcmp {
		// the client send gate (see ICMP.md): default off until the provider
		// fleet broadly parses icmp, since a not-yet-upgraded provider
		// silently blackholes icmp flows
		self.logSparseSendDrop("icmp disabled", &self.sendIcmpDisabledDropCount, errIcmpDisabled)
		return false
	}
	// Port 25 is a deliberate local-only route and must be selected before
	// CFAA inspection (which classifies privileged SMTP as a drop). Ports
	// 465/587 remain provider-routed only while their SMTP/TLS stream validates.
	if smtpRoutesLocally(ipPath) {
		if ipPath.Syn && !ipPath.Ack {
			self.logSmtpPolicyOutcome(ipPath, "local")
		}
		return self.sendSmtpLocal(source, provideMode, ipPath, packet)
	}
	smtpVerdict, smtpMilestone := self.smtpEgressGuard.inspectDetailed(ipPath, payload)
	switch smtpMilestone {
	case smtpEgressMilestoneSecure:
		self.logSmtpPolicyOutcome(ipPath, "encrypted")
	case smtpEgressMilestoneRejected:
		self.logSmtpPolicyOutcome(ipPath, "rejected")
	}
	if smtpVerdict == smtpEgressReject {
		self.rejectSmtpPacket(source, provideMode, ipPath, packet)
		return false
	}
	r, err := self.securityPolicy.InspectEgress(relationship, ipPath, payload)
	if err != nil {
		self.logSparseSendDrop("policy", &self.sendPolicyDropCount, err)
		return false
	}
	// refresh the flow's activity on the send direction (keeps a download-heavy flow alive)
	self.securityPolicy.RefreshEgress(ipPath)

	// infrastructure destinations (the resolver endpoints) are excluded
	// from the association and override logic
	ignored := self.blockActionIgnored(ipPath)

	if !ignored && self.ipAssoc != nil {
		self.ipAssoc.AddEgressPacket(ipPath)
	}

	if r != SecurityPolicyResultAllow && r != SecurityPolicyResultDrop {
		// incident (martian/malformed). always blocked, not overridable
		if self.log.V(1).Enabled() {
			self.log.Infof("[multi]drop packet ipv%d p%v -> %s:%d\n", ipPath.Version, ipPath.Protocol, ipPath.DestinationIp, ipPath.DestinationPort)
		}
	}

	// the overrides take precedence over the default decisions (the security
	// result and the ad/tracker blocker)
	blockActionState := self.blockActionState.Load()
	config := self.config.Load()
	blockerActive := config.blocker != nil && config.blocker.Enabled()
	var decision *blockActionDecision
	if !ignored && (blockActionState.matcher != nil || self.blockActionCollector.hasCallbacks() || blockerActive) {
		decision = self.blockActionDecision(blockActionState, config.blocker, blockerActive, ipPath)
	}
	var match *blockActionMatch
	blockerBlock := false
	if decision != nil {
		match = decision.match
		blockerBlock = decision.blockerBlock
	}
	block, local := blockActionApply(r, config.localSecurityBypass, blockerBlock, match)

	byteCount := ByteCount(len(packet))
	if decision != nil && self.blockActionCollector.hasCallbacks() {
		self.blockActionCollector.add(decision, block, local, match, byteCount)
	}

	if block {
		self.packetStatsCounters.blockEgressPacketCount.Add(1)
		self.packetStatsCounters.blockEgressByteCount.Add(int64(byteCount))
		return false
	}
	if local {
		success := self.localUserNat.SendPacket(source, provideMode, packet, 0)
		if success {
			self.packetStatsCounters.localEgressPacketCount.Add(1)
			self.packetStatsCounters.localEgressByteCount.Add(int64(byteCount))
		}
		return success
	}
	// the G-4b pin, resolved here in the lock-free zone: a host pin from the
	// override match this packet already computed, an app pin from the
	// platform's flow-owner lookup (one cached call per flow key)
	pin := flowPin{
		site: match != nil && match.routeOverride != nil && match.routeOverride.Pin,
	}
	if lookupPtr := self.flowOwnerFunc.Load(); lookupPtr != nil {
		pin.appId = self.flowOwnerAppId(ipPath, *lookupPtr)
	}

	parsedPacket := &parsedPacket{
		packet:  packet,
		ipPath:  ipPath,
		payload: payload,
		pin:     pin,
		transportAttribution: newTransportPacketAttribution(
			self.packetStatsCounters,
			1,
			byteCount,
		),
	}
	success := self.sendPacket(source, provideMode, parsedPacket, timeout)
	if success {
		parsedPacket.transportAttribution.admit()
	}
	return success
}

// Applies SMTP/CFAA/block routing policy to one complete device-originated
// fragmented UDP datagram, then sends the original MTU-sized fragments as one
// exact-flow group. Fragmented TCP is deliberately unsupported: normal TCP
// uses MSS, while accepting a partial transport header could evade the SMTP
// state machine or another flow policy.
func (self *RemoteUserNatMultiClient) sendReassembledUdpFragments(
	source TransferPath,
	provideMode protocol.ProvideMode,
	reassembled []byte,
	fragments [][]byte,
	timeout time.Duration,
) bool {
	if len(fragments) == 0 {
		return false
	}
	ipPath, payload, err := ParseIpPathWithPayload(reassembled)
	if err != nil || ipPath.Protocol != IpProtocolUdp {
		return false
	}
	relationship := egressRelationship(provideMode, self.provideMode)
	r, err := self.securityPolicy.InspectEgress(relationship, ipPath, payload)
	if err != nil {
		self.logSparseSendDrop("fragment policy", &self.sendPolicyDropCount, err)
		return false
	}
	self.securityPolicy.RefreshEgress(ipPath)

	ignored := self.blockActionIgnored(ipPath)
	if !ignored && self.ipAssoc != nil {
		self.ipAssoc.AddEgressPacket(ipPath)
	}
	blockActionState := self.blockActionState.Load()
	config := self.config.Load()
	blockerActive := config.blocker != nil && config.blocker.Enabled()
	var decision *blockActionDecision
	if !ignored && (blockActionState.matcher != nil || self.blockActionCollector.hasCallbacks() || blockerActive) {
		decision = self.blockActionDecision(blockActionState, config.blocker, blockerActive, ipPath)
	}
	var match *blockActionMatch
	blockerBlock := false
	if decision != nil {
		match = decision.match
		blockerBlock = decision.blockerBlock
	}
	block, local := blockActionApply(r, config.localSecurityBypass, blockerBlock, match)
	byteCount := ByteCount(0)
	for _, fragment := range fragments {
		fragmentByteCount := ByteCount(len(fragment))
		byteCount += fragmentByteCount
		if decision != nil && self.blockActionCollector.hasCallbacks() {
			self.blockActionCollector.add(decision, block, local, match, fragmentByteCount)
		}
	}
	if block {
		self.packetStatsCounters.blockEgressPacketCount.Add(int64(len(fragments)))
		self.packetStatsCounters.blockEgressByteCount.Add(int64(byteCount))
		return false
	}
	if local {
		if self.localUserNat == nil || !self.localUserNat.SendPackets(
			source,
			provideMode,
			fragments,
			timeout,
		) {
			return false
		}
		self.packetStatsCounters.localEgressPacketCount.Add(int64(len(fragments)))
		self.packetStatsCounters.localEgressByteCount.Add(int64(byteCount))
		return true
	}

	pin := flowPin{
		site: match != nil && match.routeOverride != nil && match.routeOverride.Pin,
	}
	if lookupPtr := self.flowOwnerFunc.Load(); lookupPtr != nil {
		pin.appId = self.flowOwnerAppId(ipPath, *lookupPtr)
	}
	parsedPackets := make([]parsedPacket, len(fragments))
	for i, fragment := range fragments {
		parsedPackets[i] = parsedPacket{
			packet: fragment,
			ipPath: ipPath,
		}
	}
	parsedGroup := &parsedPacketGroup{
		packets:   parsedPackets,
		ipPath:    ipPath,
		pin:       pin,
		byteCount: byteCount,
		transportAttribution: newTransportPacketAttribution(
			self.packetStatsCounters,
			len(fragments),
			byteCount,
		),
	}
	if !self.sendParsedPacketGroup(source, provideMode, parsedGroup, timeout) {
		return false
	}
	parsedGroup.transportAttribution.admit()
	return true
}

// Consumes a packet burst after grouping it by exact directional flow. One
// group is accepted or rejected as a unit; malformed packets are returned.
func (self *RemoteUserNatMultiClient) SendPacketBatch(
	source TransferPath,
	provideMode protocol.ProvideMode,
	packets [][]byte,
	timeout time.Duration,
) int {
	containsFragments := false
	for _, packet := range packets {
		if isIpFragmentPacket(packet) {
			containsFragments = true
			break
		}
	}
	if !containsFragments {
		return self.sendCompletePacketBatch(source, provideMode, packets, timeout)
	}

	// Preserve wire order around a fragmented datagram. Ordinary contiguous
	// runs retain the existing exact-flow batch path; fragments enter their
	// bounded gate one at a time and complete atomically when the final member
	// arrives (possibly in a later native read batch).
	acceptedCount := 0
	completeStart := 0
	flushComplete := func(end int) {
		if completeStart < end {
			acceptedCount += self.sendCompletePacketBatch(
				source,
				provideMode,
				packets[completeStart:end],
				timeout,
			)
		}
	}
	for i, packet := range packets {
		if !isIpFragmentPacket(packet) {
			continue
		}
		flushComplete(i)
		if self.SendPacket(source, provideMode, packet, timeout) {
			acceptedCount++
		}
		completeStart = i + 1
	}
	flushComplete(len(packets))
	return acceptedCount
}

func (self *RemoteUserNatMultiClient) sendCompletePacketBatch(
	source TransferPath,
	provideMode protocol.ProvideMode,
	packets [][]byte,
	timeout time.Duration,
) int {
	groups, rejectedPackets := groupIpPacketsBounded(
		packets,
		self.settings.PacketGroupMaxPacketCount,
		self.settings.PacketGroupMaxByteCount,
	)
	for _, packet := range rejectedPackets {
		MessagePoolReturn(packet)
	}

	sentPacketCount := 0
	for _, group := range groups {
		if smtpNeedsOrderedSend(group.ipPath) {
			// SMTP validation is stream-ordered and may accept an earlier
			// negotiation segment while rejecting a later plaintext segment.
			// Preserve that per-packet result instead of applying the ordinary
			// all-or-nothing flow-group transaction.
			for _, packet := range group.packets {
				if self.SendPacket(source, provideMode, packet, timeout) {
					sentPacketCount += 1
				} else {
					MessagePoolReturn(packet)
				}
			}
			continue
		}
		if self.sendPacketGroup(source, provideMode, group, timeout) {
			sentPacketCount += len(group.packets)
			continue
		}
		for _, packet := range group.packets {
			MessagePoolReturn(packet)
		}
	}
	return sentPacketCount
}

// Conditionally transfers one homogeneous flow group. Success transfers every
// packet; failure leaves every packet with the caller.
func (self *RemoteUserNatMultiClient) sendPacketGroup(
	source TransferPath,
	provideMode protocol.ProvideMode,
	group *ipPacketGroup,
	timeout time.Duration,
) bool {
	if group == nil || group.ipPath == nil || len(group.packets) == 0 ||
		len(group.ipPaths) != len(group.packets) ||
		len(group.payloads) != len(group.packets) {
		return false
	}

	ipPath := group.ipPath
	if ipPath.Protocol == IpProtocolIcmp && !self.settings.EnableIcmp {
		self.logSparseSendDrop("icmp disabled", &self.sendIcmpDisabledDropCount, errIcmpDisabled)
		return false
	}

	ignored := self.blockActionIgnored(ipPath)
	if !ignored && self.ipAssoc != nil {
		self.ipAssoc.AddEgressPacket(ipPath)
	}

	blockActionState := self.blockActionState.Load()
	config := self.config.Load()
	blockerActive := config.blocker != nil && config.blocker.Enabled()
	var decision *blockActionDecision
	if !ignored && (blockActionState.matcher != nil || self.blockActionCollector.hasCallbacks() || blockerActive) {
		decision = self.blockActionDecision(blockActionState, config.blocker, blockerActive, ipPath)
	}
	var match *blockActionMatch
	blockerBlock := false
	if decision != nil {
		match = decision.match
		blockerBlock = decision.blockerBlock
	}

	parsedPackets := make([]parsedPacket, len(group.packets))
	memberIpPaths := make([]IpPath, len(group.packets))
	for packetIndex, packet := range group.packets {
		memberIpPaths[packetIndex] = group.ipPaths[packetIndex]
		memberIpPath := &memberIpPaths[packetIndex]
		// groupIpPackets already proved a homogeneous exact tuple. Retained path
		// values use its canonical owned addresses rather than packet aliases.
		memberIpPath.SourceIp = ipPath.SourceIp
		memberIpPath.DestinationIp = ipPath.DestinationIp
		payload := group.payloads[packetIndex]
		parsedPackets[packetIndex] = parsedPacket{
			packet:  packet,
			ipPath:  memberIpPath,
			payload: payload,
		}
	}
	result, err := inspectAndRefreshEgressGroupBorrowed(
		self.securityPolicy,
		egressRelationship(provideMode, self.provideMode),
		memberIpPaths,
		group.payloads,
	)
	if err != nil {
		self.logSparseSendDrop("policy", &self.sendPolicyDropCount, err)
		return false
	}
	block, local := blockActionApply(
		result,
		config.localSecurityBypass,
		blockerBlock,
		match,
	)

	if decision != nil && self.blockActionCollector.hasCallbacks() {
		for _, packet := range group.packets {
			self.blockActionCollector.add(
				decision,
				block,
				local,
				match,
				ByteCount(len(packet)),
			)
		}
	}
	if block {
		self.packetStatsCounters.blockEgressPacketCount.Add(int64(len(group.packets)))
		self.packetStatsCounters.blockEgressByteCount.Add(int64(group.byteCount))
		return false
	}
	if local {
		if self.localUserNat == nil || !self.localUserNat.SendPackets(
			source,
			provideMode,
			group.packets,
			timeout,
		) {
			return false
		}
		self.packetStatsCounters.localEgressPacketCount.Add(int64(len(group.packets)))
		self.packetStatsCounters.localEgressByteCount.Add(int64(group.byteCount))
		return true
	}

	pin := flowPin{
		site: match != nil && match.routeOverride != nil && match.routeOverride.Pin,
	}
	if lookupPtr := self.flowOwnerFunc.Load(); lookupPtr != nil {
		pin.appId = self.flowOwnerAppId(ipPath, *lookupPtr)
	}
	parsedGroup := &parsedPacketGroup{
		packets:   parsedPackets,
		ipPath:    ipPath,
		pin:       pin,
		byteCount: group.byteCount,
		transportAttribution: newTransportPacketAttribution(
			self.packetStatsCounters,
			len(group.packets),
			group.byteCount,
		),
	}
	if !self.sendParsedPacketGroup(source, provideMode, parsedGroup, timeout) {
		return false
	}
	parsedGroup.transportAttribution.admit()
	return true
}

// the cached override match, blocker match, and server names for a
// destination. the external lookups (server names, cluster) run outside the
// cache lock
func (self *RemoteUserNatMultiClient) blockActionDecision(state *blockActionState, blocker Blocker, blockerActive bool, ipPath *IpPath) *blockActionDecision {
	addr, ok := ipAssocAddr(ipPath.DestinationIp)
	if !ok {
		return nil
	}

	var clusterVersion uint64
	if self.ipAssoc != nil {
		clusterVersion = self.ipAssoc.ClusterVersion()
	}
	now := time.Now()

	if decision := self.blockActionCache.get(addr, state.version, clusterVersion, blockerActive, now); decision != nil {
		return decision
	}

	// decisions are made on the cluster level.
	// a destination with no cluster is a cluster of itself
	clusterIps := []netip.Addr{addr}
	if self.ipAssoc != nil {
		if members := self.ipAssoc.GetClusterAddrs(addr); 0 < len(members) {
			clusterIps = members
		}
	}
	slices.SortFunc(clusterIps, func(a netip.Addr, b netip.Addr) int {
		return a.Compare(b)
	})

	decision := &blockActionDecision{
		overridesVersion: state.version,
		clusterVersion:   clusterVersion,
		blockerEnabled:   blockerActive,
		expireTime:       now.Add(self.blockActionCache.ttl),
		clusterKey:       clusterIps[0],
		clusterIps:       clusterIps,
	}
	var match *blockActionMatch
	if state.matcher != nil {
		match = &blockActionMatch{}
	}
	destSeen := false
	seenHosts := map[string]bool{}
	for _, member := range clusterIps {
		memberServerNames := self.serverNames(member)
		for _, serverName := range memberServerNames {
			if !seenHosts[serverName] {
				seenHosts[serverName] = true
				decision.clusterHosts = append(decision.clusterHosts, serverName)
			}
		}
		if match != nil {
			// the match extends over the destination's entire cluster
			state.matcher.matchAddr(match, member, memberServerNames)
		}
		if blockerActive && member == addr {
			// the blocker matches the destination itself: its ip and its own
			// observed server names, never the cluster's (over-blocking
			// co-clustered infrastructure would break sites)
			destSeen = true
			decision.blockerBlock = blockerCheck(blocker, addr, memberServerNames)
		}
	}
	if blockerActive && !destSeen {
		decision.blockerBlock = blockerCheck(blocker, addr, self.serverNames(addr))
	}
	if match != nil && match.any() {
		decision.match = match
	}
	self.blockActionCache.put(addr, decision)
	return decision
}

// blockerCheck is the destination-scoped blocker match: the destination ip
// against the blocked ranges, and each of the destination's own observed
// server names against the blocked host set.
func blockerCheck(blocker Blocker, addr netip.Addr, serverNames []string) bool {
	if blocker.BlockIp(addr) {
		return true
	}
	for _, serverName := range serverNames {
		if blocker.BlockHost(serverName) {
			return true
		}
	}
	return false
}

func (self *RemoteUserNatMultiClient) serverNames(addr netip.Addr) []string {
	config := self.config.Load()
	if config == nil || config.serverNameLookup == nil {
		return nil
	}
	return config.serverNameLookup.ServerNames(addr.String())
}

// SetBlockActionOverrides replaces the override rules.
// overrides take precedence over the default security and local routing decisions.
// safe to call at runtime
func (self *RemoteUserNatMultiClient) SetBlockActionOverrides(overrides []*BlockActionOverride) {
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		prev := self.blockActionState.Load()
		self.blockActionState.Store(&blockActionState{
			version: prev.version + 1,
			matcher: newBlockActionMatcher(overrides),
		})
	}()
	self.blockActionCache.clear()
}

// SetBlockActionIgnoreHosts replaces the host values (hostnames and ips, in
// the same forms as `BlockActionOverride.Hosts`) excluded from the override
// and association logic. used for infrastructure destinations like the dns
// resolver endpoints, which must never be captured by user override rules or
// cluster with user traffic. the default security and routing decisions and
// the packet stats still apply to ignored destinations, but no block actions
// are surfaced for them. safe to call at runtime.
// the hard coded remote doh resolver ips are always excluded in addition to
// these host values (see `defaultRemoteDohIgnoreAddrs`).
func (self *RemoteUserNatMultiClient) SetBlockActionIgnoreHosts(hostValues []string) {
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		prev := self.blockActionIgnoreState.Load()
		var matcher *blockActionMatcher
		if 0 < len(hostValues) {
			matcher = newBlockActionMatcher([]*BlockActionOverride{{
				Hosts:         hostValues,
				BlockOverride: &BlockOverride{},
			}})
		}
		self.blockActionIgnoreState.Store(&blockActionIgnoreState{
			version: prev.version + 1,
			matcher: matcher,
		})
	}()
	self.blockActionIgnoreCache.clear()
}

// blockActionIgnored returns whether the destination is excluded from the
// override and association logic. the result is cached per destination.
// the hard coded remote doh resolver ips are always excluded (see
// `defaultRemoteDohIgnoreAddrs`), independent of `SetBlockActionIgnoreHosts`.
func (self *RemoteUserNatMultiClient) blockActionIgnored(ipPath *IpPath) bool {
	addr, ok := ipAssocAddr(ipPath.DestinationIp)
	if !ok {
		return false
	}
	if defaultRemoteDohIgnoreAddrs()[addr] {
		return true
	}
	state := self.blockActionIgnoreState.Load()
	if state.matcher == nil {
		return false
	}
	now := time.Now()
	if entry := self.blockActionIgnoreCache.get(addr, state.version, now); entry != nil {
		return entry.ignored
	}
	match := &blockActionMatch{}
	state.matcher.matchAddr(match, addr, self.serverNames(addr))
	entry := &blockActionIgnoreEntry{
		ignored:    match.any(),
		version:    state.version,
		expireTime: now.Add(self.blockActionIgnoreCache.ttl),
	}
	self.blockActionIgnoreCache.put(addr, entry)
	return entry.ignored
}

// AddBlockActionCallback registers a listener for the epoch block action events.
// all egress routing decisions are surfaced, deduplicated per destination per epoch
func (self *RemoteUserNatMultiClient) AddBlockActionCallback(blockActionCallback BlockActionFunction) func() {
	return self.blockActionCollector.addCallback(blockActionCallback)
}

// PacketStats returns the cumulative packet counts by route
func (self *RemoteUserNatMultiClient) PacketStats() *PacketStats {
	return self.packetStatsCounters.snapshot()
}

// AddPacketStatsCallback registers a listener fired on the event epoch when the
// packet stats change
func (self *RemoteUserNatMultiClient) AddPacketStatsCallback(packetStatsCallback PacketStatsFunction) func() {
	callbackId := self.packetStatsCallbacks.Add(packetStatsCallback)
	return func() {
		self.packetStatsCallbacks.Remove(callbackId)
	}
}

func (self *RemoteUserNatMultiClient) canSendPacket(
	sendPacket *parsedPacket,
	update *multiClientChannelUpdate,
	currentClient *multiClientChannel,
) (allow bool) {
	ipPath := sendPacket.ipPath
	switch ipPath.Protocol {
	case IpProtocolTcp:
		// Collapse prevention is valid only when the selected client's exact
		// send policy gives Transfer end-to-end recovery ownership. Tunneled TCP
		// always requests Transfer ACKs, including on a direct or otherwise
		// reliable carrier, because carrier delivery cannot survive a disconnect
		// or route replacement.
		transferAckRequired := currentClient == nil ||
			currentClient.ipPacketTransferAckRequired(ipPath)
		if self.settings.TcpCollapsePrevention && transferAckRequired {
			// limit sender tcp collapse
			// As soon as a packet is sent to a client, Transfer either commits it
			// end to end or the client is dropped. Inner retransmits do not need to
			// be sent again while that exact Transfer item is still recovering.
			if ipPath.Syn || ipPath.Rst {
				allow = true
			} else if update.canUpdateSequence(sendPacket) {
				// sequence state is guarded by the per-flow `stateLock`, not
				// the parent `stateLock`
				allow = true
			} else if tcpCollapseMaxHold := self.reliabilitySettings().TcpCollapseMaxHold; 0 < tcpCollapseMaxHold &&
				update.releaseSequenceHold(tcpCollapseMaxHold) {
				// the flow has been pinned at the same sequence state past the
				// hold, so the committed packet is not making progress. let a
				// retransmit through rather than discarding the sender's only
				// recovery mechanism until failure detection catches up
				allow = true
			}
		} else {
			allow = true
		}
	default:
		allow = true
	}
	return
}

// Applies collapse prevention once to a group. If any member can advance the
// flow, the whole ordered group stays intact; a fully redundant group drops.
func (self *RemoteUserNatMultiClient) canSendPacketGroup(
	sendPacketGroup *parsedPacketGroup,
	update *multiClientChannelUpdate,
	currentClient *multiClientChannel,
) bool {
	for packetIndex := range sendPacketGroup.packets {
		if self.canSendPacket(&sendPacketGroup.packets[packetIndex], update, currentClient) {
			return true
		}
	}
	return false
}

func (self *RemoteUserNatMultiClient) sendPacket(
	source TransferPath,
	provideMode protocol.ProvideMode,
	sendPacket *parsedPacket,
	timeout time.Duration,
) (success bool) {
	return self.sendParsedPacketGroup(
		source,
		provideMode,
		&parsedPacketGroup{
			packets:              []parsedPacket{*sendPacket},
			ipPath:               sendPacket.ipPath,
			pin:                  sendPacket.pin,
			byteCount:            ByteCount(len(sendPacket.packet)),
			transportAttribution: sendPacket.transportAttribution,
		},
		timeout,
	)
}

// Routes and admits one already-parsed exact-flow group as a unit.
func (self *RemoteUserNatMultiClient) sendParsedPacketGroup(
	source TransferPath,
	provideMode protocol.ProvideMode,
	sendPacketGroup *parsedPacketGroup,
	timeout time.Duration,
) (success bool) {
	firstPacket := &sendPacketGroup.packets[0]
	ipPath := sendPacketGroup.ipPath
	var sentUpdate *multiClientChannelUpdate
	self.sendClientPath(ipPath, sendPacketGroup.pin, func(update *multiClientChannelUpdate, currentClient *multiClientChannel) {
		sentUpdate = update
		if !self.canSendPacketGroup(sendPacketGroup, update, currentClient) {
			return
		}
		// Preserve the singular path's pre-admission control semantics. In
		// particular, a newly bound or racing SYN/RST must clear stale collapse
		// state even though no current client exists to run the success commit.
		update.resetSequenceGroup(sendPacketGroup)

		enterTime := time.Now()

		// Client-side dial-failure inference. A connection attempt
		// retransmitting on an exit that has answered nothing is the silent
		// form of the failure the provider signal covers: the provider cannot
		// reach the destination, or the destination drops the connection
		// without a word, as anti-bot infrastructure commonly does to
		// datacenter ip ranges. No signal ever arrives for those, so the
		// wait is the only evidence. Feeding the same clientDialFailure path
		// as the explicit signal gets the same guards, the same unbind, and
		// the same dial-strike accounting that warns the exit off new flows --
		// while its established traffic keeps running, which is the entire
		// point: an exit is never torn down for a destination's silence.
		// Covers tcp syns and the request-response udp handshakes (quic,
		// dns) -- see dialProbePacket for why the udp side is port-gated.
		// guard order matters on this path: every egress packet passes here,
		// so the plain field checks go first and the atomic load, settings
		// read, and clock only run for a probe on an unestablished flow.
		if dialProbePacket(ipPath) &&
			currentClient != nil && !update.receivedInbound.Load() &&
			self.reliabilitySettings().DialFailureRerace &&
			update.synWaitExceeded(currentClient, ipPath, inferredDialFailureTimeout) {
			// treat the flow as unbound locally only if it really was: the
			// guards inside can decline after our check passed (a syn-ack can
			// land in between), and racing while the flow is still committed
			// aborts every attempt until the send times out. when it was
			// unbound, this very retransmit races a fresh exit instead of
			// following the old one into the same silence for another backoff
			// round.
			if self.clientDialFailure(currentClient, ipPath) {
				currentClient = nil
			}
		}

		// Send through a committed client and preserve the selected-client error
		// transition. Provider selection also calls this helper when its original
		// nil snapshot becomes stale because another sender committed the flow.
		sendBoundClient := func(client *multiClientChannel, sendTimeout time.Duration) bool {
			if client == nil {
				return false
			}
			var err error
			success, err = client.SendGroupDetailed(sendPacketGroup, sendTimeout)
			if success {
				// sequence state is guarded by the per-flow `stateLock`
				update.commitSequenceGroup(sendPacketGroup)
			} else if err != nil {
				// reset the path.
				//
				// COMPARE-AND-SWAP, not a bare Store: `client` here is a
				// snapshot taken under the parent lock inside sendUpdate,
				// and that lock was released before this send ran. A
				// migration or rebind can commit the flow to a REPLACEMENT
				// in that window -- which is the ordinary case now that a
				// bench migrates flows, since a wedged exit is exactly what
				// benches -- and a bare Store(nil) would then undo a
				// successful move, leave the flow booked under the
				// replacement with a nil client (invisible to the idle
				// reaper, which keys on update.client), and hand the app an
				// RST for a flow that is alive somewhere else. The swap
				// failing means someone already moved this flow: there is
				// nothing to reset and nothing to tell the app.
				if !update.client.CompareAndSwap(client, nil) {
					return true
				}

				self.log.Infof("[multi]reset error = %s\n", err)

				rstPackets := []*receivePacket{}

				if packet, ok := self.teardownSourcePacket(update.ipPath, update.sourceRstSequence()); ok {
					rstPacket := &receivePacket{
						Source:      TransferPath{},
						ProvideMode: protocol.ProvideMode_Network,
						IpPath:      update.ipPath,
						Packet:      packet,
					}
					rstPackets = append(rstPackets, rstPacket)
				}

				select {
				case <-self.ctx.Done():
				default:
					for _, p := range rstPackets {
						self.enqueueRemovalReceive(p)
					}
				}
			}
			return true
		}

		// `currentClient` is the client snapshot read by `sendClientPath` under
		// the parent lock it already held, so the steady-state send no longer
		// takes the parent lock again just to read `update.client`.
		if sendBoundClient(currentClient, timeout) {
			// A selected client owns this attempt even when bounded admission
			// reports backpressure. The ordinary caller decides whether to retry.
			return
		}

		// find a new client
		// the race is between as many clients as can send in parallel

		// if _, fixed := self.generator.FixedDestinationSize(); fixed {
		// 	window := self.windows[WindowTypeQuality]
		// 	orderedClients := window.OrderedClients()

		// 	for _, client := range orderedClients {
		// 		if client.Send(sendPacket, timeout) {
		// 			success = true

		// 			func() {
		// 				self.stateLock.Lock()
		// 				defer self.stateLock.Unlock()

		// 				update.client = client
		// 			}()
		// 		}
		// 	}

		// 	return
		// }

		raceClients := func(orderedClients []*multiClientChannel, sendTimeout time.Duration) {
			// sendClientPath deliberately snapshots without holding the parent
			// lock through admission. A concurrent sender can therefore bind this
			// flow after our nil snapshot but before provider selection. Refresh
			// here and use that commitment; repeatedly selecting with the stale nil
			// snapshot would otherwise wait forever without sending the packet.
			if sendBoundClient(update.client.Load(), sendTimeout) {
				return
			}
			switch len(orderedClients) {
			case 0:
				return
			case 1:
				client := orderedClients[0]
				// Even one candidate needs a published race state before queue
				// admission: its response can arrive before SendGroup returns.
				// Unlike a wide race, success still commits immediately because
				// there is no winner decision to wait for.
				var race *multiClientChannelUpdateRace
				var committedClient *multiClientChannel
				func() {
					update.stateLock.Lock()
					defer update.stateLock.Unlock()
					if client := update.client.Load(); client != nil {
						committedClient = client
						return
					}
					if update.race == nil {
						update.initRaceWithLock()
					}
					race = update.race
					state := race.clientStates[client]
					if state == nil {
						state = &multiClientChannelRaceClientState{sendTime: time.Now()}
						race.clientStates[client] = state
					}
					state.pendingSendCount += 1
				}()
				if committedClient != nil {
					sendBoundClient(committedClient, sendTimeout)
					return
				}
				if race == nil {
					return
				}

				sent := client.SendGroup(sendPacketGroup, sendTimeout)
				var abandonedClients []*multiClientChannel
				var receivePackets []*receivePacket
				var returnPackets []*receivePacket
				connectSucceeded := false
				func() {
					update.stateLock.Lock()
					defer update.stateLock.Unlock()
					if update.race != race {
						return
					}
					state := race.clientStates[client]
					if state == nil {
						return
					}
					state.pendingSendCount -= 1
					if !sent {
						if state.pendingSendCount == 0 &&
							state.sentPacketCount == 0 && len(state.packets) == 0 {
							delete(race.clientStates, client)
						}
						if len(race.clientStates) == 0 {
							update.clearRaceWithLock()
						}
						return
					}
					state.sentPacketCount += 1
					race.sentPacketCount += 1
					receivePackets, abandonedClients, returnPackets, connectSucceeded =
						update.commitRaceClientWithLock(client)
				}()
				if !sent {
					return
				}

				success = true
				if connectSucceeded {
					client.addConnectSuccess(update.ipPath.Version)
					self.clearDestinationServiceFailure(client, update.ipPath)
					self.logSmtpProviderOutcome(update.ipPath, client, "connected")
				}
				self.bindClientFlow(update, client)
				if 0 < len(abandonedClients) {
					if rstPacket, ok := ipOosRst(update.ipPath); ok {
						for _, abandonedClient := range abandonedClients {
							abandonedClient.Send(&parsedPacket{
								packet: rstPacket,
								ipPath: update.ipPath,
							}, 0)
						}
					}
				}
				completed := false
				for _, packet := range receivePackets {
					self.deliverReceivePacket(packet.Source, packet.ProvideMode, packet.IpPath, packet.Packet)
					if update.observeTcpControl(packet.tcpControl, true) {
						completed = true
					}
				}
				if completed {
					self.retireCompletedTcpFlow(update)
				}
				for _, packet := range returnPackets {
					MessagePoolReturn(packet.Packet)
				}
				// A successful no-response commitment still starts its silence
				// clock on the first probe. A synchronous response sets
				// receivedInbound above and makes startSynWait a no-op.
				if dialProbePacket(ipPath) && update.client.Load() == client {
					update.startSynWait(client)
				}
				return

			default:
				var raceOrderedClients []*multiClientChannel
				if 0 < self.settings.MultiRaceClientCount && self.settings.MultiRaceClientCount < len(orderedClients) {
					raceOrderedClients = orderedClients[:self.settings.MultiRaceClientCount]
				} else {
					raceOrderedClients = orderedClients
				}

				// Publish the race and every candidate before any SendGroup call.
				// Queue admission and provider return run on independent goroutines;
				// a fast response can therefore arrive before SendGroup returns.
				// Registering afterwards drops that real response as unknown and
				// leaves an unanswered flow even though an exit answered it.
				var race *multiClientChannelUpdateRace
				var committedClient *multiClientChannel
				var initRace *multiClientChannelUpdateRace
				var initRaceEarlyComplete <-chan struct{}
				func() {
					update.stateLock.Lock()
					defer update.stateLock.Unlock()
					if client := update.client.Load(); client != nil {
						committedClient = client
						return
					}
					if update.race == nil {
						update.initRaceWithLock()
						initRace = update.race
						initRaceEarlyComplete = initRace.completeMonitor.NotifyChannel()
					}
					race = update.race
					now := time.Now()
					for _, client := range raceOrderedClients {
						state := race.clientStates[client]
						if state == nil {
							state = &multiClientChannelRaceClientState{sendTime: now}
							race.clientStates[client] = state
						}
						state.pendingSendCount += 1
					}
				}()
				if committedClient != nil {
					sendBoundClient(committedClient, sendTimeout)
					return
				}
				if race == nil {
					return
				}

				var scheduleOnce sync.Once
				scheduleRace := func() {
					if initRace != nil {
						scheduleOnce.Do(func() {
							self.scheduleCompleteRace(update.ipPath, initRace, initRaceEarlyComplete)
						})
					}
				}

				var successCount atomic.Int32
				send := func(client *multiClientChannel) {
					sent := false
					select {
					case <-update.ctx.Done():
					default:
						if update.client.Load() == nil {
							sent = sendMultiClientGroupRaceAttempt(
								client,
								sendPacketGroup,
								sendTimeout,
							)
						}
					}
					if sent {
						successCount.Add(1)
					}

					var abandonedClients []*multiClientChannel
					func() {
						update.stateLock.Lock()
						defer update.stateLock.Unlock()
						if update.race != race {
							return
						}
						state := race.clientStates[client]
						if state == nil {
							return
						}
						state.pendingSendCount -= 1
						if !sent {
							if state.pendingSendCount == 0 &&
								state.sentPacketCount == 0 && len(state.packets) == 0 {
								delete(race.clientStates, client)
							}
							return
						}
						state.sentPacketCount += 1
						race.sentPacketCount += 1
						bufferExceeded := self.settings.MultiRaceSetOnNoResponseTimeout <= time.Since(state.sendTime) ||
							self.settings.MultiRaceClientSentPacketMaxCount < state.sentPacketCount
						if race.packetCount == 0 && bufferExceeded &&
							!dialProbeAwaitsResponse(ipPath, state.sentPacketCount) {
							// Silence can select a client only for traffic which may
							// truly be send-only. TCP SYN and the bounded initial
							// QUIC/DNS window still require response evidence.
							for abandonedClient := range race.clientStates {
								if abandonedClient != client {
									abandonedClients = append(abandonedClients, abandonedClient)
								}
							}
							update.clearRaceWithLock()
							update.client.Store(client)
						}
					}()

					// Start the response deadline only after an admission call
					// returns. Starting it at registration can expire the race
					// while a bounded send is still waiting for queue capacity.
					scheduleRace()
					if 0 < len(abandonedClients) {
						if rstPacket, ok := ipOosRst(update.ipPath); ok {
							for _, abandonedClient := range abandonedClients {
								abandonedClient.Send(&parsedPacket{
									packet: rstPacket,
									ipPath: update.ipPath,
								}, 0)
							}
						}
					}
				}

				// if 0 < timeout {
				var wg sync.WaitGroup

				for _, client := range raceOrderedClients {
					wg.Add(1)
					go HandleError(func() {
						defer wg.Done()

						send(client)
					})
				}

				wg.Wait()
				// } else {
				// 	for _, client := range raceOrderedClients {
				// 		send(client)
				// 	}
				// }

				if 0 < successCount.Load() {
					success = true
					for packetIndex := range sendPacketGroup.packets {
						MessagePoolReturn(sendPacketGroup.packets[packetIndex].packet)
					}
				}
				return
			}
		}

		coalesceOrderedClients := func() []*multiClientChannel {
			if self.groupRaceCandidatesForTest != nil {
				return self.groupRaceCandidatesForTest(sendPacketGroup)
			}
			for _, windowType := range self.selectWindowTypes(firstPacket, sendPacketGroup.pin.appId) {
				if window, ok := self.windows[windowType]; ok {
					orderedClients := self.raceCandidates(window, ipPath.Version)
					// A destination+port failure is narrower than the
					// window's rank. Widen across healthy tiers before
					// filtering so a rejected SMTP endpoint actually gets a
					// different egress address on its retransmitted SYN.
					if self.destinationServiceFailurePresent(orderedClients, ipPath) {
						if crossed := window.orderedClientsCrossTierForIpVersion(ipPath.Version); 0 < len(crossed) {
							orderedClients = crossed
						} else if fallback := window.lastResortClientsForIpVersion(ipPath.Version); 0 < len(fallback) {
							orderedClients = fallback
						}
					}
					orderedClients = self.filterDestinationServiceFailures(orderedClients, ipPath)
					// External probe reputation is used only here, after affinity
					// declined to bind this fresh flow. It cannot move an established
					// media session and it has no access to TLS plaintext.
					orderedClients = self.filterReputationFailedCandidates(orderedClients, update)
					// A completed short TLS flow is weaker evidence than an
					// unmeasured provider's advertised-rate prior. Weight the fresh
					// race by peak inner-TCP ACK progress before affinity can keep
					// selecting the same low-throughput exit.
					orderedClients = self.preferAffinityPerformanceCandidates(orderedClients, update)
					if windowType == WindowTypeQuality {
						orderedClients = self.preferFastSameNetworkCandidates(
							orderedClients,
							ipPath,
							sendPacketGroup.pin.appId,
						)
					}
					if scoredPlacementEnabled(self.reliabilitySettings()) {
						// guarded scored-placement path (Phase 1): re-orders the
						// already health-filtered field above, never widens or
						// narrows it. See scoredPlacementReorder.
						orderedClients = self.scoredPlacementReorder(orderedClients, ipPath, sendPacketGroup.pin.appId)
					}
					// legacy selection unchanged: with the gate off (every
					// current build's default), orderedClients is exactly what
					// raceCandidates returned, untouched.
					if 0 < len(orderedClients) {
						return orderedClients
					}
				}
			}
			return []*multiClientChannel{}
		}

		raceClients(coalesceOrderedClients(), 0)
		if success {
			return
		}

		for {
			var retryTimeout time.Duration
			if 0 <= timeout {
				remainingTimeout := enterTime.Add(timeout).Sub(time.Now())

				if remainingTimeout <= 0 {
					// drop
					return
				}

				retryTimeout = min(remainingTimeout, self.settings.SendRetryTimeout)
			} else {
				retryTimeout = self.settings.SendRetryTimeout
			}

			orderedClients := coalesceOrderedClients()
			if len(orderedClients) == 0 {
				// IPV6.md B6: once the windows hold exits and none can carry
				// v6, a v6 flow is answered with no-route and dropped, so the
				// application fails fast (and falls back to v4) instead of
				// retrying into a blackhole. While the windows are still
				// forming a v6 flow waits on the formation poll like a v4 one:
				// the dualstack-first fill may land a v6-capable exit any
				// moment, and answering no-route then would push every
				// dual-stack application onto v4 for the session.
				if ipPath.Version == 6 && self.Ipv6Unroutable() {
					self.replyIpv6NoRoute(ipPath)
					return
				}
				// formation fast-poll (ported as a concept from upstream main
				// e05ecee): the window has no offer AT ALL — distinct from the
				// benched fallback, which still returns candidates to race.
				// There is nothing to send to and nothing to pace, so poll for
				// the first client at FormationPollTimeout instead of sitting
				// out SendRetryTimeout: on a fresh connect the first DNS+SYN
				// then leaves moments after the first client lands. Bounded to
				// while-empty only; once candidates exist the ordinary retry
				// pacing below applies. 0 keeps the pre-change SendRetryTimeout
				// pacing (retryTimeout is already remaining-bounded above).
				if formationPoll := self.reliabilitySettings().FormationPollTimeout; 0 < formationPoll {
					retryTimeout = min(retryTimeout, formationPoll)
				}
				select {
				case <-update.ctx.Done():
					return
				case <-time.After(retryTimeout):
				}
				continue
			}

			startTime := time.Now()
			raceClients(orderedClients, retryTimeout)
			if success {
				return
			}
			endTime := time.Now()
			retryTimeout -= endTime.Sub(startTime)

			if 0 < retryTimeout {
				select {
				case <-update.ctx.Done():
					return
				case <-time.After(retryTimeout):
				}
			}
		}
	})
	// Commit close state only after the group was accepted by an exit and
	// sendClientPath finished its client bookkeeping. A failed/backpressured
	// FIN or RST must not retire a flow that is still live upstream.
	if success && sentUpdate != nil && sentUpdate.observeEgressTcpGroup(sendPacketGroup) {
		self.retireCompletedTcpFlow(sentUpdate)
	}
	return
}

// The uplink gate.
//
// A blackhole verdict convicts a provider on silence, and silence is only
// evidence while the local uplink is known to deliver. When the phone itself
// is between networks -- a wifi/cellular migration, a captive portal, an
// elevator -- nothing from any provider can arrive, so every exit looks
// identically guilty and the verdict loop executes them one after another.
// Measured on device: one wifi network migration executed 7 exits in 79s,
// every verdict `no-receive-ack recv 0/0B`. The gate makes that evidence
// inadmissible: while the whole tunnel has been silent past
// UplinkStalenessGate, the receive-branch verdicts are held, and when
// receiving resumes their clocks restart from the end of the silence so the
// held verdicts do not all fire at once on unfreeze.

const (
	// uplinkStampCoarseness bounds how often the ingress stamp is rewritten.
	// The gate compares against seconds, so per-packet nanosecond precision
	// buys nothing and costs a contended cache line on the download hot path;
	// a load-and-compare skips the store while the stamp is fresh. The
	// unsynchronized read-then-write race is benign: losing a store leaves a
	// stamp at most this much older than the truth.
	uplinkStampCoarseness = 100 * time.Millisecond

	// uplinkStalenessMaxHold caps how long continuous staleness may hold
	// verdicts. The gate cannot tell a long network outage from a window
	// whose every provider is dead -- both are tunnel-wide silence -- so past
	// this bound it stops applying and the ordinary verdicts recycle the
	// window. The epoch stays open: the clocks still rebase when receiving
	// actually resumes, so the recycled channels' replacements start clean.
	uplinkStalenessMaxHold = 60 * time.Second
)

// stampUplinkIngress records proof that the local uplink delivers. Called
// wherever a provider-originated packet is known to have arrived; see the
// uplinkStampCoarseness comment for why most calls store nothing.
func (self *RemoteUserNatMultiClient) stampUplinkIngress() {
	nowNanos := time.Now().UnixNano()
	if lastNanos := self.uplinkLastIngressNanos.Load(); nowNanos-lastNanos < int64(uplinkStampCoarseness) {
		return
	}
	self.uplinkLastIngressNanos.Store(nowNanos)
}

// sendingChannelCount reports how many channels across all windows currently
// hold outstanding sends. The gate carries zero exculpatory information when
// only one channel is talking -- tunnel-wide silence is then indistinguishable
// from that one provider being dead -- so uplinkGate only engages at two or
// more.
//
// Locking: the parent stateLock is taken briefly to snapshot the channels
// with bound flows, released, and then each channel's own stateLock is taken
// one at a time with nothing else held. The channel stateLock must never nest
// under the parent stateLock (see clientDialFailure), and consequently this
// must never be called while holding a channel or per-flow leaf lock.
func (self *RemoteUserNatMultiClient) sendingChannelCount() int {
	var clients []*multiClientChannel
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		clients = make([]*multiClientChannel, 0, len(self.clientUpdates))
		for client := range self.clientUpdates {
			clients = append(clients, client)
		}
	}()

	sendingCount := 0
	for _, client := range clients {
		if client.hasOutstandingSends() {
			sendingCount += 1
		}
	}
	return sendingCount
}

// comparativeReceiveWindow is how recent a sibling's return traffic must be to
// count as "the pool is demonstrably fine" for the comparative connect cut. The
// uplink gate's own window when it is configured, so one exit's silence is
// judged against exactly the freshness bar the tunnel-wide gate uses; otherwise
// the blackhole poll bound, and a last-resort constant so a bare fixture still
// gets a sane window instead of zero (which would disable the count).
func (self *RemoteUserNatMultiClient) comparativeReceiveWindow() time.Duration {
	if window := self.reliabilitySettings().UplinkStalenessGate; 0 < window {
		return window
	}
	if self.settings != nil && 0 < self.settings.BlackholeTimeout {
		return self.settings.BlackholeTimeout
	}
	return 5 * time.Second
}

// receivingChannelCount reports how many channels OTHER than exclude have
// received return traffic inside comparativeReceiveWindow. The receive-side
// sibling of sendingChannelCount, and the evidence behind the comparative
// connect cut: two exits visibly receiving means the uplink delivers and the
// pool works, so a third exit that has established nothing is alone in its
// trouble.
//
// Locking: identical discipline to sendingChannelCount, and for the same
// reason. The parent stateLock is taken briefly to snapshot the channels with
// bound flows, released, and then each channel's own stateLock is taken one at
// a time with nothing else held -- the channel lock must never nest under the
// parent lock (see clientDialFailure), so this must never be called while
// holding a channel or per-flow leaf lock. detectBlackhole calls it with
// nothing held.
//
// Reading clientUpdates (rather than walking the windows) matches
// sendingChannelCount and keeps this to one map iteration: a channel carrying
// return traffic is a channel with flows bound to it, so the approximation errs
// only toward undercounting -- which errs toward the patient full timeout,
// the safe direction for a bar that shortens a removal.
func (self *RemoteUserNatMultiClient) receivingChannelCount(exclude *multiClientChannel) int {
	window := self.comparativeReceiveWindow()

	var clients []*multiClientChannel
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		clients = make([]*multiClientChannel, 0, len(self.clientUpdates))
		for client := range self.clientUpdates {
			if client == exclude {
				continue
			}
			clients = append(clients, client)
		}
	}()

	receivingCount := 0
	for _, client := range clients {
		if client.hasRecentReceive(window) {
			receivingCount += 1
		}
	}
	return receivingCount
}

// uplinkGate evaluates the tunnel-wide uplink gate for one verdict pass and
// maintains the staleness epoch as a side effect. stale means the receive
// verdicts must be held this pass; freshSince is when the last stale epoch
// ended (zero if never stale) and is what the receive-verdict clocks rebase
// from. Safe on a bare client; called from every channel's detectBlackhole
// cadence with no locks held.
func (self *RemoteUserNatMultiClient) uplinkGate(now time.Time) (stale bool, freshSince time.Time) {
	gate := self.reliabilitySettings().UplinkStalenessGate

	timeStale := false
	if 0 < gate {
		lastIngressNanos := self.uplinkLastIngressNanos.Load()
		// no stamp yet means no baseline: the uplink never proved it delivers,
		// so there is nothing for it to have stopped doing. a dead-on-arrival
		// window is left to the ordinary verdicts rather than held for the cap
		// first.
		timeStale = lastIngressNanos != 0 && gate < now.Sub(time.Unix(0, lastIngressNanos))

		// the degenerate-case guard. checked only once the clock already says
		// stale, so the channel sweep runs at most once per verdict pass.
		if timeStale && self.sendingChannelCount() < 2 {
			timeStale = false
		}
	}
	// gate <= 0 leaves timeStale false and falls through: the ingress-staleness
	// half is off, but the scheduler-pause hold below is a separate mechanism
	// with its own setting and still applies. With neither engaged this returns
	// (false, uplinkFreshSince), and uplinkFreshSince is zero on a client that
	// has never been gated or rebased -- the disabled-gate behavior unchanged.

	self.uplinkStateLock.Lock()
	defer self.uplinkStateLock.Unlock()

	// the scheduler-pause recovery hold rides out on the same two values the
	// migration case uses (see notifySchedulerPause): a suspend and a network
	// migration are the same problem for the verdict layer -- evidence
	// collected while nothing could arrive -- so they share one hold path
	// instead of the channels learning about a second one.
	pauseHold := !self.schedulerPauseHoldUntil.IsZero() && now.Before(self.schedulerPauseHoldUntil)

	if timeStale {
		if self.uplinkStaleSince.IsZero() {
			self.uplinkStaleSince = now
			self.uplinkCapLogged = false
			// transitions only -- the passes in between are silent
			loggerOrDefault(self.log).Infof("%s\n", relEvent("uplink", "state", "stale"))
		}
		if uplinkStalenessMaxHold <= now.Sub(self.uplinkStaleSince) {
			// past the cap the gate stops applying so a genuinely dead window
			// can be recycled. the epoch deliberately stays open: freshSince
			// must not advance to here, or the recycle the cap exists to
			// allow would be rebased away.
			if !self.uplinkCapLogged {
				self.uplinkCapLogged = true
				loggerOrDefault(self.log).Infof("%s\n", relEvent(
					"uplink",
					"state", "cap",
					"hold", uplinkStalenessMaxHold,
				))
			}
			// the cap releases the ingress-staleness hold only. an open
			// scheduler-pause window is a different, bounded piece of evidence
			// and keeps holding on its own timer.
			return pauseHold, self.uplinkFreshSince
		}
		return true, self.uplinkFreshSince
	}

	if !self.uplinkStaleSince.IsZero() {
		// the stale epoch just ended. the receive-verdict clocks rebase from
		// here, so verdicts held across the epoch restart instead of all
		// firing at once on unfreeze.
		self.uplinkStaleSince = time.Time{}
		self.uplinkFreshSince = now
		loggerOrDefault(self.log).Infof("%s\n", relEvent("uplink", "state", "fresh"))
	}
	return pauseHold, self.uplinkFreshSince
}

// schedulerPauseProbeInterval is how long the pause detector arms for on each
// iteration. Short enough that a suspend is noticed on the first wakeup after
// resume (the hold has to be in place before the channels' 1.25s verdict passes
// run), long enough that the goroutine costs one wakeup per second.
const schedulerPauseProbeInterval = 1 * time.Second

// schedulerPauseDetected is the whole detection rule: a timer armed for
// `expected` that took `elapsed` to come back was not merely late, it was not
// running. Concept ported from upstream main e05ecee's schedulerPauseDetected.
//
// Pure on purpose -- the interesting cases (a 30s doze, a 2.001s jitter) cannot
// be produced on demand from a test, and the rule is the part worth pinning.
// A zero tolerance disables detection, which is the pre-change behavior.
func schedulerPauseDetected(elapsed time.Duration, expected time.Duration, tolerance time.Duration) bool {
	if expected <= 0 || tolerance <= 0 {
		return false
	}
	return expected+tolerance < elapsed
}

// runSchedulerPauseDetector watches for the host stopping underneath us.
//
// The instrument is deliberately the crudest one available: arm a timer, see
// how long it actually took. Everything else this process could measure went
// away with the cpu -- no packets arrived, no acks landed, no verdict pass ran
// -- so the only observable left is that wall-clock time passed while we were
// not running. That is exactly what doze, the app freezer, thermal throttling
// and a closed lid look like from in here.
//
// Plain time.After, NOT WakeupAfter: the wakeup scheduler intentionally
// coalesces timers to save radio wakeups, and a coalesced fire is precisely the
// "late" this loop would misread as a suspend.
//
// Tied to the multi-client ctx and not to `cancel`: like the prober, a detector
// failure must never tear down the tunnel.
func (self *RemoteUserNatMultiClient) runSchedulerPauseDetector() {
	for {
		armed := time.Now()
		select {
		case <-self.ctx.Done():
			return
		case <-time.After(schedulerPauseProbeInterval):
		}

		// read the tolerance AFTER the wait so the runtime toggle takes effect
		// without a reconnect, the same discipline the other loops here use
		tolerance := self.reliabilitySettings().SchedulerPauseTolerance
		elapsed := time.Since(armed)
		if schedulerPauseDetected(elapsed, schedulerPauseProbeInterval, tolerance) {
			self.notifySchedulerPause(elapsed)
		}
	}
}

// notifySchedulerPause records that the host was suspended and just resumed.
//
// It feeds the SAME hold path the uplink gate uses rather than a parallel one:
// the epoch is closed and uplinkFreshSince rebased to the resume instant (so
// every channel's receive-verdict clock restarts from now instead of counting
// the suspend as silence), and a recovery window is opened during which
// uplinkGate reports stale -- holding the receive-branch verdicts while the
// transports re-register and the first return packets land.
//
// Deliberately NOT a network-change broadcast: a resume is not necessarily a
// path change, and kicking every transport to re-dial on every doze exit would
// manufacture the churn this whole layer exists to avoid. If the path really
// did change, the platform's own callback fires NotifyNetworkChanged for it.
//
// Lock discipline: only uplinkStateLock (a leaf) is taken, with nothing else
// held. Safe on a bare client.
func (self *RemoteUserNatMultiClient) notifySchedulerPause(elapsed time.Duration) {
	recoveryTimeout := self.reliabilitySettings().SchedulerPauseRecoveryTimeout
	now := time.Now()
	func() {
		self.uplinkStateLock.Lock()
		defer self.uplinkStateLock.Unlock()
		self.uplinkStaleSince = time.Time{}
		self.uplinkCapLogged = false
		self.uplinkFreshSince = now
		if 0 < recoveryTimeout {
			self.schedulerPauseHoldUntil = now.Add(recoveryTimeout)
		}
	}()
	self.reliabilityMetrics.schedulerPauseDetected()
	// one line per detected pause -- a suspend is a rare, decisive event and
	// the reconstruction of any wake-up incident starts here
	loggerOrDefault(self.log).Infof("%s\n", relEvent(
		"scheduler_pause",
		"elapsed", elapsed.Round(time.Millisecond),
		"hold", recoveryTimeout,
	))
}

// NotifyNetworkChanged is the parent seam for the host's network path change
// signal (NWPathMonitor / ConnectivityManager; the sdk device forwards it
// here). Ported as a concept from upstream main e05ecee's network-change kick,
// wired into our uplink-gate machinery.
//
// A network change is a legitimate fresh start for the staleness clocks:
// silence accumulated on the old path says nothing about the new one. So this
// (1) stamps any open uplink-stale epoch closed and rebases uplinkFreshSince
// to now — every channel's receive-verdict clock restarts, and if the tunnel
// is still silent the gate re-engages with a fresh hold-cap window instead of
// inheriting an epoch that is about to hit the cap — and (2) fires the
// process-wide NetworkChanged broadcast, kicking every registered platform
// transport to drop its live connection and re-dial immediately over the new
// path (see PlatformTransport.Kick).
//
// Lock discipline: only the parent uplinkStateLock (a leaf lock) is taken,
// with nothing else held; the broadcast runs outside it. Safe on a bare
// client and safe to call on every OS path update.
func (self *RemoteUserNatMultiClient) NotifyNetworkChanged() {
	now := time.Now()
	func() {
		self.uplinkStateLock.Lock()
		defer self.uplinkStateLock.Unlock()
		self.uplinkStaleSince = time.Time{}
		self.uplinkFreshSince = now
	}()
	// note this is the EVENT line, not the action line: a real OS path change
	// and the developer menu's drill (SimulateNetworkChange) both land here,
	// and only the drill is preceded by an event=action line -- which is
	// exactly how a capture tells them apart
	loggerOrDefault(self.log).Infof("%s\n", relEvent("network_change", "rebased", true, "kick", true))
	NetworkChanged()
}

// reliabilityMetricsRef hands the shared counters to the window channels via
// injection, mirroring how reliabilitySettings reaches them. Unexported on
// purpose: the public ReliabilityMetrics() returns a snapshot, this returns
// the live counters (possibly nil on a bare client, which every counter
// method tolerates).
func (self *RemoteUserNatMultiClient) reliabilityMetricsRef() *reliabilityMetrics {
	return self.reliabilityMetrics
}

// clientFlowCount reports how many live flows are currently bound to a window
// client, from the clientUpdates bookkeeping that bindClientFlow single-sources
// (the same count the flow cap and the teardown read). This is the number that
// decides whether closing an exit costs anything: a verdict against a flowless
// exit is free to execute, one against a loaded exit destroys established
// connections and must clear a higher bar.
//
// Locking: takes only the parent stateLock, which sits at the top of the lock
// hierarchy here. It must therefore never be called while a channel stateLock
// or a per-flow leaf lock is held (the same rule sendingChannelCount documents)
// -- callers are the resize pass and detectBlackhole, both of which hold
// nothing when they read it.
func (self *RemoteUserNatMultiClient) clientFlowCount(client *multiClientChannel) int {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return len(self.clientUpdates[client])
}

// clientReceivePacketFunction
// runRemovalReceive isolates best-effort packets produced by client teardown
// from the maintenance paths. Normal ingress keeps its direct low-latency
// path; only synthetic teardown traffic pays this queue hop.
func (self *RemoteUserNatMultiClient) runRemovalReceive() {
	for {
		select {
		case <-self.ctx.Done():
			return
		case packet := <-self.removalReceiveQueue:
			if self.ctx.Err() != nil {
				return
			}
			HandleError(func() {
				self.deliverReceivePacket(
					packet.Source,
					packet.ProvideMode,
					packet.IpPath,
					packet.Packet,
				)
			})
		}
	}
}

func (self *RemoteUserNatMultiClient) enqueueRemovalReceive(packet *receivePacket) {
	if packet == nil {
		return
	}
	if self.removalReceiveQueue == nil {
		// bare fixtures assemble the struct without a queue; deliver inline,
		// which is the pre-queue behavior
		self.deliverReceivePacket(packet.Source, packet.ProvideMode, packet.IpPath, packet.Packet)
		return
	}
	select {
	case <-self.ctx.Done():
	case self.removalReceiveQueue <- *packet:
	default:
		// A reset is advisory; bounded loss is preferable to either blocking
		// maintenance or retaining every dead flow while the receiver is stuck.
		// Log at powers of two so a persistent stall is visible without spam.
		self.addRemovalReceiveDrops(1)
	}
}

func (self *RemoteUserNatMultiClient) addRemovalReceiveDrops(count uint64) {
	if count == 0 {
		return
	}
	previous := self.removalReceiveDropCount.Add(count) - count
	current := previous + count
	// Log when this batch crosses a power-of-two threshold. This remains
	// sparse for a large flow-table teardown while preserving visibility.
	nextPower := uint64(1)
	if 0 < previous {
		nextPower = 1 << uint64(bits.Len64(previous))
	}
	if nextPower <= current {
		self.log.Infof("[multi]removal receive queue full; dropped %d best-effort packets\n", current)
	}
}

// receivePacketCallbackHolder holds the app's per-packet receive callback.
type receivePacketCallbackHolder struct {
	callback ReceivePacketFunction
}

// SetPerformanceDegraded reports the host's degraded-performance state (low
// power mode, thermal throttling, a weak or constrained network): while set,
// the idle continuous-ping rest scales by DegradedLivenessScale so an idle
// tunnel wakes the radio less often. Cheap and safe to call whenever the OS
// signals change.
func (self *RemoteUserNatMultiClient) SetPerformanceDegraded(degraded bool) {
	if degradedMode := self.settings.DegradedMode; degradedMode != nil {
		degradedMode.Store(degraded)
	}
}

// SetReceivePacketCallback swaps (or with nil retires) the app's per-packet
// receive callback.
func (self *RemoteUserNatMultiClient) SetReceivePacketCallback(receivePacketCallback ReceivePacketFunction) {
	if receivePacketCallback == nil {
		self.receivePacketCallback.Store(nil)
		return
	}
	self.receivePacketCallback.Store(&receivePacketCallbackHolder{callback: receivePacketCallback})
}

func (self *RemoteUserNatMultiClient) deliverReceivePacket(
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	packet []byte,
) {
	if holder := self.receivePacketCallback.Load(); holder != nil {
		// Deliberate device-TUN exception from CODESTYLE.md. This is the final
		// synchronous injection boundary; Transfer does not ACK the borrowed
		// packet until the callback returns. It must not grow into a general
		// blocking observer or a send back into the shared receive pump.
		holder.callback(source, provideMode, ipPath, packet)
	}
}

// receivePacketsCallbackHolder holds the app's batch receive callback (the
// shared ReceivePacketsFunction type from ip.go). The multi client's batches
// may span flows, so the callback's advisory ipPath is nil.
type receivePacketsCallbackHolder struct {
	callback ReceivePacketsFunction
}

// SetReceivePacketsCallback registers a batch receive callback. When set, the
// committed-flow fast path delivers each client receive dispatch's packets as
// one batch through it (amortizing the app's per-packet inject cost, e.g.
// tun.WriteBatch); the per-packet callback still receives the rare paths.
func (self *RemoteUserNatMultiClient) SetReceivePacketsCallback(receivePacketsCallback ReceivePacketsFunction) {
	if receivePacketsCallback == nil {
		self.receivePacketsCallback.Store(nil)
		return
	}
	self.receivePacketsCallback.Store(&receivePacketsCallbackHolder{callback: receivePacketsCallback})
}

// clientReceivePackets is the batch form of clientReceivePacket: one call per
// client receive dispatch with every parsed return packet. Packets on the
// committed-flow fast path (the common download case) collect into one batch
// delivery; everything else falls back to the per-packet path unchanged. The
// probe intercept holds on this path too: a probe answer is consumed by the
// qualification mechanism and never reaches the application, batched or not.
func (self *RemoteUserNatMultiClient) clientReceivePackets(
	sourceClient *multiClientChannel,
	source TransferPath,
	provideMode protocol.ProvideMode,
	transportType TransportType,
	ipPaths []*IpPath,
	packets [][]byte,
) {
	holder := self.receivePacketsCallback.Load()
	if holder == nil {
		for i := range packets {
			self.clientReceivePacket(
				sourceClient,
				source,
				provideMode,
				transportType,
				ipPaths[i],
				packets[i],
			)
		}
		return
	}

	// Both slices are bounded by the dispatch batch (one clientReceive burst).
	// Controls are committed only after their packets have been delivered.
	var batchPackets [][]byte
	var batchControls []pendingIngressTcpControl
	var completedUpdates []*multiClientChannelUpdate
	completeLater := func(update *multiClientChannelUpdate) {
		for _, completed := range completedUpdates {
			if completed == update {
				return
			}
		}
		completedUpdates = append(completedUpdates, update)
	}
	flush := func() {
		if len(batchPackets) == 0 {
			return
		}
		// The batch may span flows: the advisory per-flow ipPath is nil. This
		// is the batched form of the documented synchronous device-TUN
		// exception in deliverReceivePacket.
		holder.callback(source, provideMode, nil, batchPackets)
		for _, pending := range batchControls {
			if pending.update.observeTcpControl(pending.control, true) {
				completeLater(pending.update)
			}
		}
		batchPackets = batchPackets[:0]
		batchControls = batchControls[:0]
	}

	for i := range packets {
		ipPath := ipPaths[i]
		packet := packets[i]

		// mirror clientReceivePacket's pre-delivery pipeline exactly
		self.smtpEgressGuard.retireReturn(ipPath)
		r, err := self.securityPolicy.InspectIngress(provideMode, ipPath, nil)
		if err != nil {
			continue
		}
		self.securityPolicy.RefreshIngress(ipPath)
		if r != SecurityPolicyResultAllow {
			continue
		}
		self.packetStatsCounters.recordRemoteIngress(
			transportType,
			1,
			ByteCount(len(packet)),
		)
		self.stampUplinkIngress()
		if probeIngressPath(ipPath) {
			self.clientReceiveProbePacket(sourceClient, ipPath, packet)
			continue
		}
		self.reliabilityMetrics.destinationReachable(ipPath.SourceIp, ipPath.SourcePort, ipPath.DestinationPort)
		if self.ipAssoc != nil {
			self.ipAssoc.AddIngressPacket(ipPath)
		}
		tcpControl := tcpControlFromIpPath(ipPath)
		ipPath = ipPath.Reverse()

		if update := self.receiveUpdate(ipPath); update != nil && update.client.Load() == sourceClient {
			// committed-flow fast path: batch. the first-inbound mark still
			// runs per packet -- it gates the dial-failure re-race and resets
			// the dial-strike window, exactly as on the per-packet path.
			if update.receivedInbound.CompareAndSwap(false, true) {
				sourceClient.addConnectSuccess(update.ipPath.Version)
				self.clearDestinationServiceFailure(sourceClient, update.ipPath)
				self.logSmtpProviderOutcome(update.ipPath, sourceClient, "connected")
			}
			batchPackets = append(batchPackets, packet)
			if tcpControl.valid {
				batchControls = append(batchControls, pendingIngressTcpControl{update: update, control: tcpControl})
			}
			continue
		}
		// rare path (unknown flow / race / migration): keep delivery ORDER
		// relative to the batch by flushing first, then reuse the resolve
		// tail from its post-accounting point
		flush()
		self.clientReceivePacketResolve(sourceClient, source, provideMode, ipPath, packet, tcpControl)
	}
	flush()
	for _, update := range completedUpdates {
		self.retireCompletedTcpFlow(update)
	}
}

func (self *RemoteUserNatMultiClient) clientReceivePacket(
	sourceClient *multiClientChannel,
	source TransferPath,
	provideMode protocol.ProvideMode,
	transportType TransportType,
	ipPath *IpPath,
	packet []byte,
) {
	self.smtpEgressGuard.retireReturn(ipPath)
	r, err := self.securityPolicy.InspectIngress(provideMode, ipPath, nil)
	if err != nil {
		return
	}
	// refresh on the return direction before ipPath is reversed for downstream delivery
	self.securityPolicy.RefreshIngress(ipPath)
	if r != SecurityPolicyResultAllow {
		return
	}

	self.packetStatsCounters.recordRemoteIngress(
		transportType,
		1,
		ByteCount(len(packet)),
	)

	// every packet here is provider-originated by construction, so it is
	// proof the local uplink delivers -- the freshness the uplink gate
	// measures verdict silence against
	self.stampUplinkIngress()

	// The probe intercept. A packet addressed to a reserved probe port at the
	// benchmarking source address belongs to the qualification mechanism, not
	// to any application: it is consumed here and NEVER forwarded to
	// receivePacketCallback, matched or not (see clientReceiveProbePacket for
	// why the unmatched case must be consumed too). The gate is two integer
	// comparisons and an address check on a range no tun is ever assigned, so
	// the download hot path pays nothing measurable, and it sits before the
	// recovery tracker and the ip association deliberately -- a probe is not a
	// user flow recovering and must not be recorded as one.
	//
	// The receive-side accounting a probe DOES feed already happened upstream
	// of here, at the channel (addReceiveAck / addReceiveSyn in clientReceive):
	// an answer through this exit is real delivery, and the positive half of
	// the probe asymmetry is exactly that it counts.
	if probeIngressPath(ipPath) {
		self.clientReceiveProbePacket(sourceClient, ipPath, packet)
		return
	}

	// traffic from a destination whose flows died with an exit closes out the
	// recovery measurement. also before reverse, so the remote endpoint is
	// still the source -- and the local endpoint is still the destination,
	// whose port is the third argument: the local source port this answer
	// arrived for, which is what classifies a rebound flow's recovery (the
	// rebound flow's own port answering = the server accepted the quic path
	// migration; a new port = the app re-dialed). a no-op unless that
	// destination is actually pending.
	self.reliabilityMetrics.destinationReachable(ipPath.SourceIp, ipPath.SourcePort, ipPath.DestinationPort)

	if self.ipAssoc != nil {
		// before reverse, the remote endpoint is the source
		self.ipAssoc.AddIngressPacket(ipPath)
	}

	tcpControl := tcpControlFromIpPath(ipPath)
	ipPath = ipPath.Reverse()

	self.clientReceivePacketResolve(sourceClient, source, provideMode, ipPath, packet, tcpControl)
}

// clientReceivePacketResolve is the post-accounting tail of the receive path
// -- flow resolution, races, and delivery -- shared by the per-packet path
// and the batch dispatch's rare cases (see clientReceivePackets). ipPath is
// already reversed to the from-destination orientation.
func (self *RemoteUserNatMultiClient) clientReceivePacketResolve(
	sourceClient *multiClientChannel,
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	packet []byte,
	tcpControl tcpControlObservation,
) {
	var abandonedClients []*multiClientChannel
	var receivePackets []*receivePacket
	var returnPackets []*receivePacket
	// connectSucceeded is set when this inbound packet is the first to resolve
	// its flow to sourceClient -- a proven upstream connect. Recorded on the
	// channel after receiveClientPath returns, outside every lock, so the
	// channel stateLock never nests under the parent or per-flow stateLock.
	connectSucceeded := false
	var connectPath *IpPath
	// boundUpdate is set when this path commits a flow to sourceClient, so the
	// clientUpdates bookkeeping can be recorded after receiveClientPath
	// returns. Same reason as connectSucceeded above: bindClientFlow takes the
	// parent stateLock, and this closure runs under the per-flow leaf lock.
	var boundUpdate *multiClientChannelUpdate
	var resolvedUpdate *multiClientChannelUpdate
	success := self.receiveClientPath(ipPath, func(update *multiClientChannelUpdate) {
		resolvedUpdate = update
		// steady-state fast path: the flow is already committed to this client,
		// so deliver without taking any lock (client is atomic). this is the
		// common download path and no longer contends the parent stateLock.
		if update.client.Load() == sourceClient {
			// first inbound packet for this flow marks it established, which
			// gates the dial-failure re-race (a stale signal must not unbind a
			// flow already carrying data).
			if update.receivedInbound.CompareAndSwap(false, true) {
				connectSucceeded = true
				connectPath = update.ipPath
			}
			p := &receivePacket{
				Source:      source,
				ProvideMode: provideMode,
				IpPath:      ipPath,
				Packet:      packet,
				tcpControl:  tcpControl,
			}
			receivePackets = []*receivePacket{p}
			return
		}

		// race / not-yet-committed paths are guarded by the per-flow stateLock
		update.stateLock.Lock()
		defer update.stateLock.Unlock()

		client := update.client.Load()

		if client == sourceClient {
			// committed between the lock-free check and acquiring the lock
			if update.receivedInbound.CompareAndSwap(false, true) {
				connectSucceeded = true
				connectPath = update.ipPath
			}
			p := &receivePacket{
				Source:      source,
				ProvideMode: provideMode,
				IpPath:      ipPath,
				Packet:      packet,
				tcpControl:  tcpControl,
			}
			receivePackets = []*receivePacket{p}
		} else if client != nil {
			// another client already chosen, drop
		} else if race := update.race; race == nil {
			// no race, no client, drop
			self.log.Infof("[multi]receive no race and no client")
		} else if state, ok := race.clientStates[sourceClient]; !ok {
			// this client is not part of the race, drop
			self.log.Infof("[multi]receive client not part of race")
		} else if len(state.packets) < self.settings.MultiRaceClientPacketMaxCount && race.packetCount < self.settings.MultiRacePacketMaxCount {
			packetCopy, pooled := MessagePoolCopyDetailed(packet)
			receivePacket := &receivePacket{
				Source:      source,
				ProvideMode: provideMode,
				IpPath:      ipPath,
				Packet:      packetCopy,
				Pooled:      pooled,
				tcpControl:  tcpControl,
			}
			state.packets = append(state.packets, receivePacket)
			if 1 == len(state.packets) {
				state.receiveTime = time.Now()
			}
			race.packetCount += 1
			if len(state.packets) == 1 {
				race.clientsWithPacketCount += 1
				if int(float32(len(race.clientStates))*self.settings.MultiRaceClientEarlyCompleteFraction) <= race.clientsWithPacketCount {
					race.completeMonitor.NotifyAll()
				}
			}
		} else {
			// race buffer limits exceeded, end the race immediately
			self.log.Infof("[multi]receive race buffer limit reached")

			for abandonedClient, abandonedState := range race.clientStates {
				if abandonedClient != sourceClient {
					abandonedClients = append(abandonedClients, abandonedClient)
					for _, p := range abandonedState.packets {
						if p.Pooled {
							p.Pooled = false
							returnPackets = append(returnPackets, p)
						}
					}
				}
			}

			update.clearRaceWithLock()
			update.client.Store(sourceClient)
			boundUpdate = update
			if update.receivedInbound.CompareAndSwap(false, true) {
				connectSucceeded = true
				connectPath = update.ipPath
			}
			receivePacket := &receivePacket{
				Source:      source,
				ProvideMode: provideMode,
				IpPath:      ipPath,
				Packet:      packet,
				tcpControl:  tcpControl,
			}
			receivePackets = append(state.packets, receivePacket)
			for _, p := range receivePackets {
				if p.Pooled {
					p.Pooled = false
					returnPackets = append(returnPackets, p)
				}
			}
		}
	})
	// a flow's first inbound packet is a proven upstream connect on this
	// channel; it resets the dial-strike window (dialStarved requires zero
	// successes). recorded outside every lock held above.
	if connectSucceeded {
		sourceClient.addConnectSuccess(connectPath.Version)
		self.clearDestinationServiceFailure(sourceClient, connectPath)
		self.logSmtpProviderOutcome(connectPath, sourceClient, "connected")
	}
	// a race won from the receive path commits the flow without the send path
	// ever noticing a change, so this is the only place the bookkeeping can be
	// recorded for it. outside every lock held above, per bindClientFlow.
	self.bindClientFlow(boundUpdate, sourceClient)
	if success {
		if 0 < len(abandonedClients) {
			if rstPacket, ok := ipOosRst(ipPath); ok {
				for _, abandonedClient := range abandonedClients {
					abandonedClient.Send(&parsedPacket{
						packet: rstPacket,
						ipPath: ipPath,
					}, 0)
				}
			}
		}
		completed := false
		for _, p := range receivePackets {
			self.deliverReceivePacket(p.Source, p.ProvideMode, p.IpPath, p.Packet)
			if resolvedUpdate != nil && resolvedUpdate.observeTcpControl(p.tcpControl, true) {
				completed = true
			}
		}
		if completed {
			self.retireCompletedTcpFlow(resolvedUpdate)
		}
		for _, p := range returnPackets {
			MessagePoolReturn(p.Packet)
		}
	} else {
		// incoming packets not in response to outgoing packets
		self.deliverReceivePacket(source, provideMode, ipPath, packet)
	}
}

// clientDialFailure handles a provider dial-failure signal intercepted on
// sourceClient's channel: an icmp destination-unreachable whose embed named the
// egress flow egressIpPath. The provider could not open the upstream for that
// flow and answered instead of going silent, so the source is not left in
// syn-retransmit backoff.
//
// It acts only on a flow that is (a) still known, (b) still pinned to the very
// client that reported the failure, and (c) has never received inbound data --
// a flow still waiting on its first upstream connect, not an established one a
// late or stale signal happens to name. When those hold and DialFailureRerace
// is on, it unbinds the flow (the removeClient per-update idiom, minus teardown
// and minus cancelling the update) so the application's own retransmit re-races
// it onto another exit within ~1s. With the knob off it rebuilds the icmp and
// forwards it to the app (removeClient's teardown convention) for a
// visible-but-fast failure. Every intercepted signal, matched or not, is
// counted; an unmatched one is dropped.
// It reports whether the flow was actually unbound, so a caller holding its
// own client snapshot knows whether to discard it. The guards here can decline
// after a caller's own check passed -- a syn-ack can land in between -- and
// treating a still-bound flow as unbound leaves that caller racing against a
// commitment that never clears.
func (self *RemoteUserNatMultiClient) clientDialFailure(sourceClient *multiClientChannel, egressIpPath *IpPath) (reraced bool) {
	// The probe intercept comes first, ahead of every effect below including
	// the counters and the uplink stamp. A dial failure naming a probe flow is
	// the probe's answer -- "no" -- and a probe's failure must feed nothing:
	// no strike, no re-race, no metric, no forwarded packet. Returning false
	// here is honest twice over: nothing was re-raced, and the caller (the
	// channel intercept, or the send-path inference) has nothing to reconsider.
	// See probeDialFailure for the reasoning on each omission.
	if self.probeDialFailure(sourceClient, egressIpPath) {
		return false
	}

	// counted for every intercepted signal, matched or not: the gap between
	// this and flowsReraced is failures that named no live flow.
	self.reliabilityMetrics.dialFailureIntercepted()

	// an intercepted icmp is deliberately not a receive-ack -- it must not
	// make a provider look like it is delivering data -- but it did arrive,
	// which is proof the local uplink delivers. without this stamp a burst of
	// dial failures during real provider trouble could read as uplink
	// staleness and hold the very verdicts that should fire.
	self.stampUplinkIngress()

	rerace := self.reliabilitySettings().DialFailureRerace

	matched := false
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		var update *multiClientChannelUpdate
		switch egressIpPath.Version {
		case 4:
			update = self.ip4PathUpdates[egressIpPath.ToIp4Path()]
		case 6:
			update = self.ip6PathUpdates[egressIpPath.ToIp6Path()]
		default:
			return
		}

		// act only on the flow this very client owns that has not yet received
		// inbound data. a nil update (gone), a different client (already
		// re-raced onto another exit), or an established flow (receivedInbound)
		// all mean this signal is stale and must not disturb a working flow.
		if update == nil || update.client.Load() != sourceClient || update.receivedInbound.Load() {
			return
		}

		matched = true
		self.recordDestinationServiceFailureWithLock(sourceClient, egressIpPath)

		if rerace {
			// unbind exactly like removeClient does per-update, minus teardown
			// and minus cancelling the update: drop it from the client's flow
			// set and clear its client pointer. The flow's own next retransmit
			// (SYN ~1s, QUIC PTO, DNS retry) then sees a nil client in
			// sendPacket and races a fresh exit.
			if updates, ok := self.clientUpdates[sourceClient]; ok {
				delete(updates, update)
				if len(updates) == 0 {
					delete(self.clientUpdates, sourceClient)
				}
			}
			update.client.Store(nil)
		}
	}()

	if !matched {
		// no live flow for this signal: dropped here, already counted above.
		return false
	}

	// strike accounting is per-channel and takes the channel's own stateLock;
	// recorded outside the parent stateLock so the two never nest. The
	// destination ip rides along so dialStarved can require strikes to span
	// distinct destinations -- one polled-dead site retransmitting into this
	// path must not starve-warn the exit by itself. net.IP.String() on a nil
	// ip yields a stable "<nil>" key, so a pathological signal still records
	// safely rather than panicking.
	sourceClient.addDialFailure(egressIpPath.DestinationIp.String(), egressIpPath.Version)
	// B5: v6 strikes with healthy v4 downgrade the exit to v4-only; the
	// window republishes the category and fills the v6 shortfall it opened
	if egressIpPath.Version == 6 && sourceClient.maybeDowngradeIpv6() {
		self.noteIpFamilyDowngrade(sourceClient)
	}

	if rerace {
		self.reliabilityMetrics.flowReraced()
		self.logSmtpProviderOutcome(egressIpPath, sourceClient, "reraced")
		// swallowed: the icmp is never forwarded. the app's retransmit drives
		// recovery. this is the whole point -- ~1s instead of 3-63s.
		return true
	}

	// rerace disabled: hand the icmp to the app so it fails fast instead of
	// hanging. rebuilt from the egress path with the same builder the provider
	// used, and delivered with the egress ipPath -- exactly the convention
	// removeClient uses for its teardown packets.
	if packet, ok := ipOosUnreachable(egressIpPath); ok {
		self.deliverReceivePacket(TransferPath{}, protocol.ProvideMode_Network, egressIpPath, packet)
	}
	return false
}

// spawns a goroutine that completes the race after a timeout. it acquires the
// per-flow stateLock (not the parent lock) when it evaluates the race.
func (self *RemoteUserNatMultiClient) scheduleCompleteRace(
	ipPath *IpPath,
	race *multiClientChannelUpdateRace,
	earlyComplete <-chan struct{},
) {
	go HandleError(func() {
		// wait for the race to finish, then choose

		select {
		case <-race.ctx.Done():
			return
		case <-earlyComplete:
		case <-time.After(self.settings.MultiRaceSetOnResponseTimeout):
		}

		var abandonedClients []*multiClientChannel
		var receivePackets []*receivePacket
		var returnPackets []*receivePacket
		// the race winner, recorded for the clientUpdates bookkeeping once the
		// per-flow leaf lock below is released -- bindClientFlow takes the
		// parent lock and must never nest under it
		var boundUpdate *multiClientChannelUpdate
		var boundClient *multiClientChannel
		connectSucceeded := false
		var connectClient *multiClientChannel
		var connectPath *IpPath
		self.receiveClientPath(ipPath, func(update *multiClientChannelUpdate) {
			// race state is guarded by the per-flow stateLock (a leaf); client
			// is atomic
			update.stateLock.Lock()
			defer update.stateLock.Unlock()

			if update.race != race {
				return
			}

			winner := update.client.Load()
			if winner == nil {
				// weighted shuffle clients by rtt
				orderedClients := []*multiClientChannel{}
				weights := map[*multiClientChannel]float32{}
				for client, state := range race.clientStates {
					if 0 < len(state.packets) {
						orderedClients = append(orderedClients, client)
						rtt := state.receiveTime.Sub(state.sendTime)
						weights[client] = float32(rtt / time.Millisecond)
					}
				}
				WeightedShuffleWithEntropy(orderedClients, weights, self.settings.StatsWindowEntropy)
				if 0 < len(orderedClients) {
					// the last is the lowest rtt
					winner = orderedClients[len(orderedClients)-1]
				}
			}
			if winner != nil {
				receivePackets, abandonedClients, returnPackets, connectSucceeded =
					update.commitRaceClientWithLock(winner)
				if connectSucceeded {
					connectClient = winner
					connectPath = update.ipPath
				}
				if update.client.Load() == winner {
					boundUpdate, boundClient = update, winner
				}
			}
			// No response, or a committed client which was not in this stale
			// race: release the race's buffered owners without changing the
			// current binding.
			if update.race == race {
				update.clearRaceWithLock()
			}
		})
		if connectSucceeded {
			connectClient.addConnectSuccess(connectPath.Version)
			self.clearDestinationServiceFailure(connectClient, connectPath)
			self.logSmtpProviderOutcome(connectPath, connectClient, "connected")
		}
		// the race completion commits the flow with no send-path transition to
		// notice it, so this is the only place its bookkeeping can be recorded
		self.bindClientFlow(boundUpdate, boundClient)
		if 0 < len(abandonedClients) {
			if rstPacket, ok := ipOosRst(ipPath); ok {
				for _, abandonedClient := range abandonedClients {
					abandonedClient.Send(&parsedPacket{
						packet: rstPacket,
						ipPath: ipPath,
					}, 0)
				}
			}
		}
		completed := false
		for _, p := range receivePackets {
			self.deliverReceivePacket(p.Source, p.ProvideMode, p.IpPath, p.Packet)
			if boundUpdate != nil && boundUpdate.observeTcpControl(p.tcpControl, true) {
				completed = true
			}
		}
		if completed {
			self.retireCompletedTcpFlow(boundUpdate)
		}
		for _, p := range returnPackets {
			MessagePoolReturn(p.Packet)
		}
	})
}

// ExitInfo is one provider channel in the window, as reported to a developer
// menu: enough to see which exits exist, which the flows are pinned to, and
// which are on their way out.
type ExitInfo struct {
	ClientId   Id
	WindowType WindowType
	// IpFamily is the exit's address-family category as discovered, after any
	// local downgrade. Legacy reads as v4-only.
	IpFamily IpFamily
	// Warning marks a client new flows already avoid -- either unhealthy or
	// past MaxClientLifetime and draining. Note this is isWarning(), which is
	// true for a quarantined exit as well: quarantine's whole mechanism is
	// exclusion from selection.
	Warning bool
	// Quarantined is the narrower state: a blackhole verdict matured against
	// this exit and was demoted rather than executed, because the exit is
	// carrying flows. Reported separately from Warning because "out of
	// selection" and "out of selection because a verdict was held" are
	// different facts to a reconstruction -- and because the heartbeat counts
	// them separately.
	Quarantined bool
	// WarningCause names WHY the resize pass warned this exit: "draining"
	// (past lifetime, healthy, retiring), "starved" (its upstream failing
	// dials), "unhealthy" (a verdict demoted or deferred against it), or ""
	// when the warning is off (a quarantine alone reports "" here -- the
	// Quarantined field is its name).
	WarningCause string
	Done         bool
	P2pOnly      bool
	// FlowCount is how many live flows are currently pinned to this exit
	FlowCount int
	// DialFailureCount is how many upstream dials this exit has reported
	// failing in the recent window -- the signature of a provider whose own
	// upstream (a resold proxy, an exhausted socket table) is refusing work
	// while the provider itself stays reachable
	DialFailureCount int
	// Tier is the platform's rank for this provider (0 is best). The window
	// races only the best rank present until it is at the flow cap, so this is
	// the field that explains why an exit carries no flows: it is a spare on a
	// higher tier, not a failure
	Tier int
	// EffectiveTier is the rank selection actually uses: Tier plus live
	// demerits (dial starvation, active or survived quarantine, unhealthy
	// window, unproven qualification). EffectiveTier > Tier is a demoted exit
	// -- new flows avoid it even though the platform ranked it well. Equal
	// when clean or when EffectiveTierSelection is off.
	EffectiveTier int
	// Proven reports a current qualification: a probe pass (or live receive
	// traffic, which refreshes for free) proved this provider dials real
	// destinations inside QualificationMaxAge. False is "not yet proven",
	// never "bad" -- the probe design records no negative state to report.
	Proven bool
	// ProbeAge is how long ago the provider was last proven; -1 when never.
	// Can exceed QualificationMaxAge (then Proven is false): a stale age is
	// still information a dev screen can show.
	ProbeAge time.Duration
	// Provider identity and source-scoped block counters are populated after
	// the first diagnostics message. They make stale provider policy directly
	// visible instead of inferring it from client-side timeouts.
	ProviderDiagnosticsAvailable bool
	ProviderBuildVersion         string
	ProviderSecurityPolicyHash   string
	ProviderBlockIngressPackets  int64
	ProviderBlockIngressBytes    int64
	ProviderBlockEgressPackets   int64
	ProviderBlockEgressBytes     int64
	ProviderDiagnosticsSequence  int64
}

// Exits reports the provider channels across every window, with the number of
// flows pinned to each. This is the readout that makes the affinity behavior
// observable: a site split across exits shows up here as flows spread over
// several entries instead of collected on one.
func (self *RemoteUserNatMultiClient) Exits() []*ExitInfo {
	flowCounts := map[Id]int{}
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		// count from the flows themselves, not from the clientUpdates
		// bookkeeping. The bookkeeping is only maintained on the send path
		// when it notices the committed client changed, so a race-won flow --
		// winner stored from the receive path or the race completion, then
		// every later send seeing no change -- is frequently never entered.
		// On device that read as "13 exits, flows on only 2" while traffic
		// plainly moved through others: the two visible were affinity
		// clusters (counted at assignment under this lock), the rest carried
		// flows this map had never heard of. update.client is the ground
		// truth: it is what the egress path actually sends on.
		countClient := func(update *multiClientChannelUpdate) {
			if update == nil || update.IsDone() {
				return
			}
			if c := update.client.Load(); c != nil {
				flowCounts[c.ClientId()] += 1
			}
		}
		for _, update := range self.ip4PathUpdates {
			countClient(update)
		}
		for _, update := range self.ip6PathUpdates {
			countClient(update)
		}
	}()

	exits := []*ExitInfo{}
	for windowType, window := range self.windows {
		for _, client := range window.unorderedClients() {
			clientId := client.ClientId()
			// the qualification readout takes the parent stateLock per exit
			// (released again before the next); fine for a dev readout, and it
			// must not be folded into the flowCounts section above because
			// effectiveTier below also reaches the parent lock through the
			// injected lookup -- nothing here may hold it across these calls
			proven, probeAge := self.qualificationExitInfo(client.probeDestination())
			exitInfo := &ExitInfo{
				ClientId:   clientId,
				WindowType: windowType,
				IpFamily:   client.IpFamily(),
				Warning:    client.isWarning(),
				// each of these takes only the channel's own stateLock, and the
				// parent lock is released above -- the same discipline the rest
				// of this walk follows
				Quarantined:      client.isQuarantined(),
				WarningCause:     client.warningCause().String(),
				Done:             client.IsDone(),
				P2pOnly:          client.IsP2pOnly(),
				FlowCount:        flowCounts[clientId],
				DialFailureCount: client.dialFailureCount(),
				Tier:             client.Tier(),
				EffectiveTier:    client.effectiveTier(),
				Proven:           proven,
				ProbeAge:         probeAge,
			}
			if diagnostics := client.providerDiagnosticsSnapshot(); diagnostics != nil {
				exitInfo.ProviderDiagnosticsAvailable = true
				exitInfo.ProviderBuildVersion = diagnostics.BuildVersion
				exitInfo.ProviderSecurityPolicyHash = diagnostics.SecurityPolicyHash
				exitInfo.ProviderBlockIngressPackets = diagnostics.BlockIngressPacketCount
				exitInfo.ProviderBlockIngressBytes = diagnostics.BlockIngressByteCount
				exitInfo.ProviderBlockEgressPackets = diagnostics.BlockEgressPacketCount
				exitInfo.ProviderBlockEgressBytes = diagnostics.BlockEgressByteCount
				exitInfo.ProviderDiagnosticsSequence = diagnostics.Sequence
			}
			exits = append(exits, exitInfo)
		}
	}
	// deterministic order: the walk above ranges two maps (windows, then each
	// window's client set), so without this every readout shuffles the rows --
	// on the developer screen the exits visibly jumped positions each refresh.
	// Window type then client id: stable while membership is stable, and a
	// membership change moves only the rows it must.
	slices.SortFunc(exits, func(a, b *ExitInfo) int {
		if a.WindowType != b.WindowType {
			return int(a.WindowType) - int(b.WindowType)
		}
		return a.ClientId.Cmp(b.ClientId)
	})
	return exits
}

// exitTelemetrySnapshot gathers this session's live per-exit ExitMetrics for
// the heartbeat line, using exactly the same non-coalescing accessors
// exitMetricsSnapshot uses for scoring (windowStatsWithCoalesce(false),
// quarantineReconvictionCount, rttEwmaSnapshot) -- so what the heartbeat
// reports is what the scorer actually saw, not a second, drifting readout.
// Same walk shape as Exits() (every window, every client, no lock held
// across a channel's own accessors), and the same nil-safe reading of a
// window with nothing in it. Flows is left at 0: the heartbeat already
// reports total flow count from Exits()' own walk, and this readout only
// exists for the goodput/rtt aggregate beside it.
func (self *RemoteUserNatMultiClient) exitTelemetrySnapshot() []ExitMetrics {
	if self == nil {
		return nil
	}
	var telemetry []ExitMetrics
	for _, window := range self.windows {
		if window == nil {
			continue
		}
		for _, client := range window.unorderedClients() {
			telemetry = append(telemetry, exitMetricsSnapshot(client, 0))
		}
	}
	return telemetry
}

// DestinationExit is one (destination ip, exit) pairing in the live flow
// table: FlowCount flows to DestinationIp currently ride the exit ClientId.
type DestinationExit struct {
	DestinationIp netip.Addr
	ClientId      Id
	FlowCount     int
}

// DestinationExits reports which exit currently carries each destination ip,
// aggregated over the live flows. This is the join the Local statistics
// screen renders: the block-action window already names the sites (observed
// hosts + cluster ips), and this readout says which egress each of those ips
// is riding RIGHT NOW -- a pull-model answer on purpose, so a re-raced or
// rebound flow reads as its current exit, not the one it was opened on. One
// walk of the flow maps under the parent lock, the same cost class as the
// Exits() flow count. Order is deterministic (ip, then client id) for the
// same reason the exit readout sorts: consumers render it.
func (self *RemoteUserNatMultiClient) DestinationExits() []*DestinationExit {
	type key struct {
		ip       netip.Addr
		clientId Id
	}
	counts := map[key]int{}
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		count := func(destinationIp []byte, update *multiClientChannelUpdate) {
			if update == nil || update.IsDone() {
				return
			}
			c := update.client.Load()
			if c == nil {
				return
			}
			addr, ok := netip.AddrFromSlice(destinationIp)
			if !ok {
				return
			}
			counts[key{ip: addr.Unmap(), clientId: c.ClientId()}] += 1
		}
		for ip4Path, update := range self.ip4PathUpdates {
			count(ip4Path.DestinationIp[:], update)
		}
		for ip6Path, update := range self.ip6PathUpdates {
			count(ip6Path.DestinationIp[:], update)
		}
	}()

	destinationExits := make([]*DestinationExit, 0, len(counts))
	for k, flowCount := range counts {
		destinationExits = append(destinationExits, &DestinationExit{
			DestinationIp: k.ip,
			ClientId:      k.clientId,
			FlowCount:     flowCount,
		})
	}
	slices.SortFunc(destinationExits, func(a, b *DestinationExit) int {
		if c := a.DestinationIp.Compare(b.DestinationIp); c != 0 {
			return c
		}
		return a.ClientId.Cmp(b.ClientId)
	})
	return destinationExits
}

// DropExit cancels a single provider channel, as if that one exit had died.
//
// Shuffle() replaces every exit at once, which is not what a real failure looks
// like -- the interesting case, and the one all of the teardown work addresses,
// is one exit vanishing while the others keep working and flows have to
// discover it. Returns false if no such client is in the window.
func (self *RemoteUserNatMultiClient) DropExit(clientId Id) bool {
	// logged before the act, not after: the exit death this causes is
	// indistinguishable in a capture from a real one, and the difference
	// between "a provider failed" and "the owner pressed the button" is the
	// whole meaning of the next twenty lines
	self.logAction("drop_exit", "exit", clientId)
	for _, window := range self.windows {
		for _, client := range window.unorderedClients() {
			if client.ClientId() == clientId {
				client.Cancel()
				window.resizeMonitor.NotifyAll()
				return true
			}
		}
	}
	return false
}

// StallExit makes a provider channel stop acknowledging without being cancelled,
// reproducing the state the collapse-prevention bound exists for: a client that
// is neither healthy nor detectably dead, holding its flows' sequence state.
//
// Note the stall is not held indefinitely. The swallowed packet is accounted
// for as an outstanding send, so sendStalled trips after SendStallTimeout (3s
// by default) and the resize pass removes and replaces the exit -- which is
// the behavior being exercised. Expect the stalled exit to disappear from the
// window a few seconds after the button, rather than sit there stalled.
// Before that accounting was in place the exit was invisible to the very
// detector this reproduces the input for, and a stall went unnoticed for 34s
// on device.
//
// This is the only way to exercise that path deliberately -- a real stall
// depends on a provider misbehaving at the right moment. Returns false if no
// such client is in the window.
func (self *RemoteUserNatMultiClient) StallExit(clientId Id, stalled bool) bool {
	self.logAction("stall_exit", "exit", clientId, "stalled", stalled)
	for _, window := range self.windows {
		for _, client := range window.unorderedClients() {
			if client.ClientId() == clientId {
				client.setStalled(stalled)
				return true
			}
		}
	}
	return false
}

func (self *RemoteUserNatMultiClient) Shuffle() {
	// no exit= key: the action is against the whole window, and inventing a
	// value for a key that has no meaning here would be worse than omitting it
	self.logAction("shuffle")
	for _, window := range self.windows {
		window.shuffle()
	}
}

// RemoveProvider drops the provider with this egress (destination tail) client
// id from every window and, for a discovery-based connection, excludes it from
// near-term discovery under the generator's retention policy. Both halves are
// required: the resize loop wakes as soon as a client dies, so a removal
// without the exclusion is immediately undone by re-discovering the same
// provider. Reports whether a window client was actually removed.
//
// A fixed-destination connection (every spec is an explicit client id, e.g. a
// chosen network peer) is NOT excluded — there is nothing to replace it with,
// so excluding would leave the tunnel with no destination at all. There the
// call just drops the client and lets the window redial it.
func (self *RemoteUserNatMultiClient) RemoveProvider(egressClientId Id) bool {
	if _, fixed := self.generator.FixedDestinationSize(); !fixed {
		if excluder, ok := self.generator.(MultiClientGeneratorExcluder); ok {
			excluder.ExcludeClientId(egressClientId)
		}
	}
	removed := false
	for _, window := range self.windows {
		if window.removeProvider(egressClientId) {
			removed = true
		}
	}
	return removed
}

func (self *RemoteUserNatMultiClient) Close() {
	self.cancel()
	self.egressIpv4Fragments.close()

	// detach retired owner references: later deliveries observe nil and stop
	// retaining/calling the retired owner (typically an UpgradeMux), and the
	// routing snapshot drops the app's lookup so Close releases everything it
	// borrowed
	self.SetReceivePacketCallback(nil)
	self.SetReceivePacketsCallback(nil)
	self.SetServerNameLookup(nil)

	// release the server-name-learned subscription so the lookup does not retain
	// this (now-closing) multi-client
	var serverNamesLearnedUnsub func()
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		serverNamesLearnedUnsub = self.serverNamesLearnedUnsub
		self.serverNamesLearnedUnsub = nil
	}()
	if serverNamesLearnedUnsub != nil {
		serverNamesLearnedUnsub()
	}

	for _, window := range self.windows {
		window.Close()
	}

	var removedUpdates []*multiClientChannelUpdate
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		seen := map[*multiClientChannelUpdate]bool{}
		appendUpdate := func(update *multiClientChannelUpdate) {
			if update != nil && !seen[update] {
				seen[update] = true
				removedUpdates = append(removedUpdates, update)
			}
		}
		for update := range self.flowUpdates {
			appendUpdate(update)
		}
		for _, update := range self.ip4PathUpdates {
			appendUpdate(update)
		}
		for _, update := range self.ip6PathUpdates {
			appendUpdate(update)
		}
		clear(self.flowUpdates)
		clear(self.ip4PathUpdates)
		clear(self.ip6PathUpdates)
		clear(self.affinityIp4Paths)
		clear(self.affinityIp6Paths)
		clear(self.destinationServiceFailures)
		clear(self.dnsExitHints)
		clear(self.dnsAddressExitHints)
	}()

	// close updates outside the parent lock: update.Close() takes the per-flow
	// stateLock (clearRaceWithLock), and keeping it off the parent lock means
	// the per-flow stateLock never nests under the parent lock anywhere.
	for _, update := range removedUpdates {
		update.Close()
	}

	self.localUserNat.Close()
	self.localUserNatUnsub()
}

// A successful join makes the embedded local packet dispatcher's ownership
// terminal. Generated network clients remain owned by their generator.
func (self *RemoteUserNatMultiClient) CloseAndWait(ctx context.Context) error {
	self.Close()
	if self.localUserNat == nil {
		return nil
	}
	return self.localUserNat.CloseAndWait(ctx)
}

type multiClientChannelUpdate struct {
	ctx    context.Context
	cancel context.CancelFunc
	// openTime and activityTime bound one TCP generation's active lifetime for
	// the affinity-performance learner; no timer or goroutine is retained per
	// flow. Construction initializes it, and a same-tuple fresh SYN resets it
	// under stateLock with the other generation state.
	openTime time.Time

	// client is the channel this flow is committed to. it is read on the
	// per-packet hot path (egress send target, ingress steady-state match) and
	// across flows by affinity selection, so it is an atomic for lock-free
	// reads. writes happen under the parent stateLock (which serializes them
	// against the path maps) or a context where the writer has exclusive access.
	client atomic.Pointer[multiClientChannel]

	// pinned marks a flow a pin rule claimed (host pin or app pin): its
	// affinity inheritance follows a benched donor for a multiple of the
	// ordinary follow window. Written once at creation under the parent
	// stateLock, read by the inherit path under the same lock.
	pinned bool
	// pinAppId is the pinned app that owns this flow (empty for unpinned
	// flows and host pins). Carried on the update so EVERY placement path --
	// including the async race, which commits through bindClientFlow long
	// after creation -- can record where the app landed, which is what makes
	// the cross-version convergence work for an app's FIRST flow of each ip
	// version (review finding, 2026-08-03).
	pinAppId string

	// receivedInbound is set true the first time an inbound (non-icmp) packet
	// resolves this flow to its committed client (the receiveClientPath path).
	// It marks the flow established, which gates the dial-failure re-race: a
	// late or stale dial-failure signal must never unbind a flow that is
	// already carrying data. Written lock-free (atomic) from the ingress path.
	receivedInbound atomic.Bool

	// synWaitStart is when synWaitClient was first asked to open this flow's
	// upstream connection -- the first dial probe (tcp syn, or quic/dns udp:
	// see dialProbePacket) sent while receivedInbound is still false. It backs
	// the client-side dial-failure inference: a provider that silently cannot
	// reach the destination (or a destination that silently drops the
	// connection) produces no failure signal at all, so the wait itself is
	// the only evidence. Keyed to synWaitClient so a re-raced flow judges
	// each exit on its own silence; the pointer is only ever compared, never
	// dereferenced, so a closed channel is harmless here.
	// Both guarded by stateLock.
	synWaitStart  time.Time
	synWaitClient *multiClientChannel
	// synWaitSendCount counts probes sent to synWaitClient since the clock
	// started. Past dialProbeMaxSends the flow is a one-way stream rather
	// than a handshake and the inference leaves it alone. Guarded by
	// stateLock alongside the clock it qualifies.
	synWaitSendCount int

	// stateLock guards the per-flow mutable state below: the active race and the
	// tcp collapse-prevention sequence counters. it is a leaf — its holders
	// never take the parent `RemoteUserNatMultiClient.stateLock` — so
	// independent flows never serialize on the parent lock for race or sequence
	// work, and there is no ordering constraint with the parent lock.
	stateLock sync.Mutex
	// race is guarded by stateLock.
	race *multiClientChannelUpdateRace

	// activityTime is guarded by the parent stateLock (written during the path
	// map lookup in send/receiveUpdate).
	activityTime time.Time
	ipPath       *IpPath

	sequencePacketCount int // guarded by stateLock
	// ackPerformance observes cumulative inner-TCP ACK sequence progress on
	// successfully admitted source packets. It contains only counters and
	// timestamps, never payload or TLS metadata, and is guarded by stateLock.
	ackPerformance tcpAckPerformance
	// sequenceTime is when the sequence state last advanced. it bounds how long
	// TcpCollapsePrevention may keep discarding a sender's retransmits while
	// the committed packet makes no progress. guarded by stateLock.
	sequenceTime      time.Time
	ackSequenceNumber uint32 // guarded by stateLock
	// tcpWindowSize is the last successfully committed raw advertised receive
	// window. A pure ACK can keep both sequence numbers unchanged while
	// reopening a zero window after the application drains its socket. Treating
	// that packet as a duplicate deadlocks the provider's split-TCP return path.
	tcpWindowSize uint16 // guarded by stateLock
	// sequenceNumber wraps at 2^32. ordering is determined via `int32(a - b)`
	// signed-delta arithmetic (per RFC 1323 PAWS / RFC 7323), wraparound-tolerant
	// across the 32-bit boundary.
	sequenceNumber uint32 // guarded by stateLock

	// TCP close observations let the shared reaper retire a completed flow
	// immediately instead of retaining its route, affinity entries, context,
	// and client bookkeeping for the deliberately long TCP idle allowance. A
	// half-close remains live: both FINs must be observed and each must be
	// acknowledged. Latest ACKs are retained even before their FIN is recorded
	// because a synchronous provider return can race ahead of the successful
	// egress send callback that commits that FIN.
	egressFinSequence  uint32 // guarded by stateLock
	ingressFinSequence uint32 // guarded by stateLock
	egressAckSequence  uint32 // guarded by stateLock
	ingressAckSequence uint32 // guarded by stateLock
	egressFinSeen      bool   // guarded by stateLock
	ingressFinSeen     bool   // guarded by stateLock
	egressAckSeen      bool   // guarded by stateLock
	ingressAckSeen     bool   // guarded by stateLock

	// affinityIp{4,6}Paths are guarded by the parent stateLock (creation only).
	affinityIp4Paths map[Ip4Path]bool
	affinityIp6Paths map[Ip6Path]bool

	// probe marks this update as a provider-qualification probe rather than an
	// application flow, and carries the probe's completion state. Non-nil only
	// for updates built by registerProbeFlow; written once at construction,
	// before the update is published to the path maps, and read-only after --
	// so it needs no lock, and the isProbe() predicate over it is safe to ask
	// from any of the paths that must treat a probe differently (the ingress
	// consume branch, the dial-failure intercept). See
	// ip_remote_multi_client_probe.go for the asymmetry it enforces: a probe's
	// success is evidence, a probe's failure is nothing at all.
	probe *probeFlow
}

func newMultiClientChannelUpdate(ctx context.Context, ipPath *IpPath) *multiClientChannelUpdate {
	cancelCtx, cancel := context.WithCancel(ctx)
	// own the flow path: a caller's path may borrow (alias) packet storage
	// (parseIpPathWithPayloadBorrowed), and the update outlives the packet
	if ipPath != nil {
		owned := *ipPath
		owned.SourceIp = append(net.IP(nil), ipPath.SourceIp...)
		owned.DestinationIp = append(net.IP(nil), ipPath.DestinationIp...)
		ipPath = &owned
	}
	return &multiClientChannelUpdate{
		ctx:              cancelCtx,
		cancel:           cancel,
		openTime:         time.Now(),
		ipPath:           ipPath,
		affinityIp4Paths: map[Ip4Path]bool{},
		affinityIp6Paths: map[Ip6Path]bool{},
	}
}

// inferredDialFailureTimeout is how long a flow's initial connect may go
// unanswered on one exit before the wait itself is treated as a dial failure.
//
// TCP retransmits its syn at roughly 1s, 2s, 4s -- so 3s means the second or
// third retransmit re-races instead of following the same path into the same
// silence. Genuine slow dials overwhelmingly complete inside 3s, and the cost
// of a wrong inference is one syn racing a different exit, not a teardown: if
// the destination is merely slow, the connect still completes wherever the
// race lands it.
const inferredDialFailureTimeout = 3 * time.Second

// dialProbeMaxSends bounds how many sends into silence still look like a
// handshake. No sane transport sends more than its initial window before the
// first response -- quic's initial cwnd is 10 packets, a tcp syn retransmits
// at most ~7 times in a minute -- so a flow past this count with nothing back
// is not dialing, it is a one-way stream (a udp pump, send-heavy telemetry on
// a watched port), and re-racing it would drop its in-flight responses for no
// diagnostic gain. Streams into silence are the blackhole detector's job.
// Caught by TestMultiClientUdp6, whose long-lived udp/53 stream was churned
// by the inference whenever the first echo ran past the wait under load.
const dialProbeMaxSends = 16

// dialProbePacket reports whether this egress packet is a connection attempt
// whose continued silence is diagnostic of a dead path: a pure tcp syn, or a
// packet of a request-response udp protocol -- quic on 443, dns on 53 --
// on a flow that has never received a byte.
//
// The port gate is what keeps the inference honest for udp. Tcp declares its
// intent in the syn flag, but a udp datagram carries no handshake marker, and
// send-only protocols (telemetry, some game and logging traffic) legitimately
// never hear back -- re-racing those on silence would bounce them between
// exits forever and strike healthy exits for a silence that is normal. Quic
// and dns always expect an answer, are the overwhelming mass of udp here
// (56% of traffic is quic), and are exactly the flows observed pinned to a
// dead exit for the full blackhole bound: on device, three exits held 63
// flows in syn-and-quic silence for 29s -- 22-28 unanswered syns each, 0
// bytes received -- because only their tcp minority could escape early.
//
// The caller layers the remaining guards: flow unestablished, wait exceeded
// per exit, and the same clientDialFailure gate the provider's explicit
// signal uses, so quic's established-flow protections are identical to tcp's.
// A moved quic flow survives the exit change by design: quic keys the
// connection on its connection id, not the 4-tuple.
func dialProbePacket(ipPath *IpPath) bool {
	switch ipPath.Protocol {
	case IpProtocolTcp:
		return ipPath.Syn && !ipPath.Ack
	case IpProtocolUdp:
		switch ipPath.DestinationPort {
		case 443, 53:
			return true
		}
	}
	return false
}

// dialProbeAwaitsResponse reports whether an unanswered dial-shaped send must
// keep all race candidates eligible. TCP SYNs always require a response, so
// silence can never choose a winner. UDP has no handshake bit; the bounded
// probe window lets a send-only flow on a watched port eventually fall back to
// the historical no-response commitment instead of racing forever.
func dialProbeAwaitsResponse(ipPath *IpPath, sendCount int) bool {
	if !dialProbePacket(ipPath) {
		return false
	}
	return ipPath.Protocol == IpProtocolTcp || sendCount <= dialProbeMaxSends
}

// startSynWait records the first unanswered dial probe carried by a committed
// client. It is used by the one-candidate path, where there is no response race
// to perform this transition. An inbound packet racing the send wins and keeps
// the flow established without arming stale failure state.
func (self *multiClientChannelUpdate) startSynWait(client *multiClientChannel) {
	if client == nil || self.receivedInbound.Load() {
		return
	}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.receivedInbound.Load() {
		return
	}
	if self.synWaitClient != client || self.synWaitStart.IsZero() {
		self.synWaitClient = client
		self.synWaitStart = time.Now()
		self.synWaitSendCount = 1
	}
}

// synWaitExceeded starts the connect-wait clock on the first dial probe (a
// tcp syn or a udp handshake packet, see dialProbePacket) a given exit
// carries for this flow, and reports whether it has run past timeout on later
// probes through that same exit. The clock is keyed to the exit: a flow that
// re-races restarts the wait, so each exit is judged on its own silence rather
// than inheriting the previous one's -- otherwise the first probe through a
// fresh exit would strike it immediately. When it trips, the clock restarts
// for the same reason.
func (self *multiClientChannelUpdate) synWaitExceeded(client *multiClientChannel, ipPath *IpPath, timeout time.Duration) bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	now := time.Now()
	if self.synWaitClient != client || self.synWaitStart.IsZero() {
		self.synWaitClient = client
		self.synWaitStart = now
		self.synWaitSendCount = 1
		return false
	}
	self.synWaitSendCount += 1
	// Past an initial UDP window with nothing back this may be a one-way
	// stream rather than a handshake -- see dialProbeMaxSends. A pure TCP SYN
	// is unambiguously waiting for a response and must never age into that
	// exemption.
	if !dialProbeAwaitsResponse(ipPath, self.synWaitSendCount) {
		return false
	}
	if timeout <= now.Sub(self.synWaitStart) {
		self.synWaitStart = now
		return true
	}
	return false
}

func (self *multiClientChannelUpdate) resetSequence(sendPacket *parsedPacket) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.resetSequenceWithLock(sendPacket)
}

// Resets every control boundary in source order under one existing flow lock.
func (self *multiClientChannelUpdate) resetSequenceGroup(sendPacketGroup *parsedPacketGroup) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	for packetIndex := range sendPacketGroup.packets {
		sendPacket := &sendPacketGroup.packets[packetIndex]
		if sendPacket.ipPath.Syn || sendPacket.ipPath.Rst {
			self.resetSequenceWithLock(sendPacket)
		}
	}
}

// Must be called with stateLock.
func (self *multiClientChannelUpdate) resetSequenceWithLock(sendPacket *parsedPacket) {
	ipPath := sendPacket.ipPath
	if ipPath.Syn {
		// A source port can be reused before the old tuple's idle deadline.
		// A fresh SYN is a new TCP generation and must not inherit either FIN or
		// ACK edge from the previous connection.
		self.egressFinSequence = 0
		self.ingressFinSequence = 0
		self.egressAckSequence = 0
		self.ingressAckSequence = 0
		self.egressFinSeen = false
		self.ingressFinSeen = false
		self.egressAckSeen = false
		self.ingressAckSeen = false
		self.openTime = time.Now()
		self.ackPerformance.reset()
	}

	self.ackSequenceNumber = ipPath.AckSequenceNumber
	self.sequenceNumber = ipPath.SequenceNumber
	self.tcpWindowSize = ipPath.TcpWindowSize
	self.sequencePacketCount = 0
	self.sequenceTime = time.Now()
}

func (self *multiClientChannelUpdate) updateSequence(sendPacket *parsedPacket) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.updateSequenceWithLock(sendPacket)
}

// Applies a successful group in order under one existing flow lock. A control
// packet resets state at its exact position, matching sequential sends.
func (self *multiClientChannelUpdate) commitSequenceGroup(sendPacketGroup *parsedPacketGroup) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	for packetIndex := range sendPacketGroup.packets {
		sendPacket := &sendPacketGroup.packets[packetIndex]
		if sendPacket.ipPath.Syn || sendPacket.ipPath.Rst {
			self.resetSequenceWithLock(sendPacket)
		}
		self.updateSequenceWithLock(sendPacket)
	}
}

// Must be called with stateLock.
func (self *multiClientChannelUpdate) updateSequenceWithLock(sendPacket *parsedPacket) {

	ipPath := sendPacket.ipPath
	update := false
	var now time.Time
	if ipPath.Protocol == IpProtocolTcp {
		if self.ackPerformance.needsTimestamp(ipPath.Ack, ipPath.AckSequenceNumber) {
			now = time.Now()
		}
		self.ackPerformance.observe(now, ipPath.Ack, ipPath.AckSequenceNumber)
	}

	nextAckSequenceNumber := ipPath.AckSequenceNumber
	if self.ackSequenceNumber != nextAckSequenceNumber {
		self.ackSequenceNumber = nextAckSequenceNumber
		update = true
	}
	if self.tcpWindowSize != ipPath.TcpWindowSize {
		self.tcpWindowSize = ipPath.TcpWindowSize
		update = true
	}

	nextSequenceNumber := tcpPacketNextSequenceNumber(ipPath, sendPacket.payload)
	// signed-delta comparison is wraparound-tolerant: > 0 means nextSequenceNumber
	// is later in TCP sequence space than self.sequenceNumber
	if 0 < int32(nextSequenceNumber-self.sequenceNumber) {
		self.sequenceNumber = nextSequenceNumber
		update = true
	}

	if update {
		self.sequencePacketCount += 1
		if now.IsZero() {
			now = time.Now()
		}
		self.sequenceTime = now
	}
}

// tcpPacketNextSequenceNumber returns the right edge occupied by this segment.
// SYN and FIN each consume one sequence number even when the payload is empty;
// omitting FIN here lets collapse prevention discard the only close signal as
// an apparent retransmission of the preceding data edge.
func tcpPacketNextSequenceNumber(ipPath *IpPath, payload []byte) uint32 {
	next := ipPath.SequenceNumber + uint32(len(payload))
	if ipPath.Syn {
		next += 1
	}
	if ipPath.Fin {
		next += 1
	}
	return next
}

// tcpSequenceAtOrAfter compares sequence-space positions across uint32 wrap.
// A live TCP window cannot span half the sequence space, so signed-delta
// ordering is unambiguous for every ACK relevant to one flow close.
func tcpSequenceAtOrAfter(sequenceNumber uint32, target uint32) bool {
	return 0 <= int32(sequenceNumber-target)
}

func updateLatestTcpSequence(seen *bool, current *uint32, next uint32) {
	if !*seen || 0 < int32(next-*current) {
		*seen = true
		*current = next
	}
}

// observeTcpControl records one successfully-sent egress control or one
// already-delivered ingress control. RST completes immediately. FIN preserves
// half-close semantics and completes only after both FIN sequence edges have
// been acknowledged by the opposite side.
func (self *multiClientChannelUpdate) observeTcpControl(
	control tcpControlObservation,
	ingress bool,
) bool {
	if !control.valid {
		return false
	}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if control.rst {
		return true
	}
	if ingress {
		if control.ack {
			updateLatestTcpSequence(&self.ingressAckSeen, &self.ingressAckSequence, control.ackSequenceNumber)
		}
		if control.fin {
			finSequence := control.sequenceNumber + control.payloadByteCount + 1
			updateLatestTcpSequence(&self.ingressFinSeen, &self.ingressFinSequence, finSequence)
		}
	} else {
		if control.ack {
			updateLatestTcpSequence(&self.egressAckSeen, &self.egressAckSequence, control.ackSequenceNumber)
		}
		if control.fin {
			finSequence := control.sequenceNumber + control.payloadByteCount + 1
			updateLatestTcpSequence(&self.egressFinSeen, &self.egressFinSequence, finSequence)
		}
	}

	return self.egressFinSeen && self.ingressFinSeen &&
		self.ingressAckSeen && tcpSequenceAtOrAfter(self.ingressAckSequence, self.egressFinSequence) &&
		self.egressAckSeen && tcpSequenceAtOrAfter(self.egressAckSequence, self.ingressFinSequence)
}

func (self *multiClientChannelUpdate) observeEgressTcpGroup(sendPacketGroup *parsedPacketGroup) bool {
	for packetIndex := range sendPacketGroup.packets {
		if self.observeTcpControl(tcpControlFromIpPath(sendPacketGroup.packets[packetIndex].ipPath), false) {
			return true
		}
	}
	return false
}

// releaseSequenceHold reports whether the flow has sat at the same sequence
// state for at least maxHold, and when it has, restarts the window.
//
// TcpCollapsePrevention discards a sender's retransmits on the premise that the
// packet already committed to a client will either be delivered reliably or the
// client will be dropped. When a client stalls without yet being declared dead,
// that premise fails: retransmits -- the sender's only recovery mechanism --
// are discarded for as long as failure detection takes (up to AckTimeout, 30s),
// and the flow is frozen the whole time.
//
// Restarting the window on release means at most one retransmit is admitted per
// maxHold rather than the whole backlog, so collapse prevention still holds
// during a normal stall while a genuinely stuck flow keeps a way to recover.
func (self *multiClientChannelUpdate) releaseSequenceHold(maxHold time.Duration) bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	// nothing committed yet: canUpdateSequence already allows these
	if self.sequencePacketCount == 0 || self.sequenceTime.IsZero() {
		return false
	}

	now := time.Now()
	if now.Sub(self.sequenceTime) < maxHold {
		return false
	}
	self.sequenceTime = now
	return true
}

// sourceRstSequence is the sequence a reset toward the source must carry to be
// accepted: the sequence the source expects next from the destination, which is
// the ack number it has been sending. See `ipOosRstSequence`.
func (self *multiClientChannelUpdate) sourceRstSequence() uint32 {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.ackSequenceNumber
}

func (self *multiClientChannelUpdate) canUpdateSequence(sendPacket *parsedPacket) bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if self.sequencePacketCount == 0 {
		return true
	}

	ipPath := sendPacket.ipPath

	if self.ackSequenceNumber != ipPath.AckSequenceNumber {
		return true
	}
	if self.tcpWindowSize != ipPath.TcpWindowSize {
		return true
	}

	nextSequenceNumber := tcpPacketNextSequenceNumber(ipPath, sendPacket.payload)
	// strict signed-delta > 0 mirrors updateSequence; treating equality as
	// "can update" would let identical retransmits pass the gate even though
	// updateSequence won't advance state, defeating TcpCollapsePrevention.
	if 0 < int32(nextSequenceNumber-self.sequenceNumber) {
		return true
	}

	return false
}

// commitRaceClientWithLock makes client the race winner and transfers every
// buffered response owner to the caller. The caller holds stateLock. Marking
// pooled packets before clearRaceWithLock is the ownership handoff: race.Close
// then releases only packets this result did not claim.
func (self *multiClientChannelUpdate) commitRaceClientWithLock(
	client *multiClientChannel,
) (
	receivePackets []*receivePacket,
	abandonedClients []*multiClientChannel,
	returnPackets []*receivePacket,
	connectSucceeded bool,
) {
	race := self.race
	if race == nil || client == nil {
		return
	}
	state := race.clientStates[client]
	if state == nil {
		return
	}

	for abandonedClient, abandonedState := range race.clientStates {
		if abandonedClient == client {
			continue
		}
		abandonedClients = append(abandonedClients, abandonedClient)
		for _, packet := range abandonedState.packets {
			if packet.Pooled {
				packet.Pooled = false
				returnPackets = append(returnPackets, packet)
			}
		}
	}
	receivePackets = append(receivePackets, state.packets...)
	for _, packet := range receivePackets {
		if packet.Pooled {
			packet.Pooled = false
			returnPackets = append(returnPackets, packet)
		}
	}

	self.clearRaceWithLock()
	self.client.Store(client)
	if 0 < len(receivePackets) && self.receivedInbound.CompareAndSwap(false, true) {
		connectSucceeded = true
	}
	return
}

// must be called with `stateLock`
func (self *multiClientChannelUpdate) initRaceWithLock() {
	if self.race == nil {
		self.race = newMultiClientChannelUpdateRace(self.ctx)
	}
}

// must be called with `stateLock`
func (self *multiClientChannelUpdate) clearRaceWithLock() {
	if self.race != nil {
		self.race.Close()
		self.race = nil
	}
}

func (self *multiClientChannelUpdate) IsDone() bool {
	// A few routing tests build zero-value updates to exercise bookkeeping in
	// isolation. Treat an update with no context as live; production updates
	// always receive their child context from newMultiClientChannelUpdate.
	if self == nil || self.ctx == nil {
		return false
	}
	select {
	case <-self.ctx.Done():
		return true
	default:
		return false
	}
}

func (self *multiClientChannelUpdate) Close() {
	self.cancel()
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.clearRaceWithLock()
}

type multiClientChannelUpdateRace struct {
	ctx                    context.Context
	cancel                 context.CancelFunc
	clientStates           map[*multiClientChannel]*multiClientChannelRaceClientState
	sentPacketCount        int
	packetCount            int
	clientsWithPacketCount int
	completeMonitor        *Monitor
}

func newMultiClientChannelUpdateRace(ctx context.Context) *multiClientChannelUpdateRace {
	cancelCtx, cancel := context.WithCancel(ctx)
	return &multiClientChannelUpdateRace{
		ctx:          cancelCtx,
		cancel:       cancel,
		clientStates: map[*multiClientChannel]*multiClientChannelRaceClientState{},
		// sentPacketCount:        0,
		// packetCount:            0,
		// clientsWithPacketCount: 0,
		completeMonitor: NewMonitor(),
	}
}

func (self *multiClientChannelUpdateRace) Close() {
	self.cancel()
	// return any still-pooled packets that no other path has claimed.
	// the Pooled flag is the single-owner marker: paths that consume a
	// receivePacket (race-buffer-exceeded in clientReceivePacket, the
	// scheduleCompleteRace winner/abandoned drains) set Pooled=false
	// before appending to their own return list. anything still
	// Pooled=true here was abandoned without a cleanup pass.
	for _, state := range self.clientStates {
		for _, p := range state.packets {
			if p.Pooled {
				p.Pooled = false
				MessagePoolReturn(p.Packet)
			}
		}
		state.packets = nil
	}
	clear(self.clientStates)
}

type multiClientChannelRaceClientState struct {
	sendTime        time.Time
	receiveTime     time.Time
	packets         []*receivePacket
	sentPacketCount int
	// pendingSendCount keeps a pre-registered candidate present until every
	// concurrent admission attempt returns. Without the count, one refused
	// attempt can delete the response target of another attempt still in
	// flight on the same flow.
	pendingSendCount int
}

type parsedPacket struct {
	packet  []byte
	ipPath  *IpPath
	payload []byte
	// pin, when set, is the G-4b flow pin computed on the egress path (the
	// lock-free zone) and carried to the flow-open under the parent lock:
	// signature changes stop at this struct, and every other construction
	// site (probes, tests) gets the zero value = unpinned.
	pin                  flowPin
	transportAttribution *transportPacketAttribution
}

// One exact directional flow carried through policy, provider selection, and
// Transfer admission without decomposing back into independently routed sends.
type parsedPacketGroup struct {
	packets              []parsedPacket
	ipPath               *IpPath
	pin                  flowPin
	byteCount            ByteCount
	transportAttribution *transportPacketAttribution
}

// flowPin is what a pin rule resolved to for one flow: the owning pinned
// app's id (empty when none), and whether a host rule pinned the destination.
// Either one marks the flow pinned; the app id additionally names the app
// affinity group every flow of that app joins.
type flowPin struct {
	appId string
	site  bool
}

func (self flowPin) pinned() bool {
	return self.site || self.appId != ""
}

func newParsedPacket(packet []byte) (*parsedPacket, error) {
	ipPath, err := ParseIpPath(packet)
	if err != nil {
		return nil, err
	}
	return &parsedPacket{
		packet: packet,
		ipPath: ipPath,
	}, nil
}

const (
	// clientLifetimeJitterMinFraction is the low end of the per-channel
	// lifetime jitter: each channel's effective lifetime is MaxClientLifetime
	// scaled by a uniform draw from [this, 1.0), taken once at construction.
	// Without it, every channel created at connect time reaches its
	// removeTime in the same resize pass, so rotation is a synchronized
	// hourly event -- every exit drains at once, every flow re-races in the
	// same window, and the reconnect burst lands on the platform as one
	// spike. 0.75 spreads a 60m lifetime over a 15m band, which is dozens of
	// resize ticks, while never shortening a lifetime below three quarters of
	// what was configured. A constant rather than a setting: the exact
	// fraction is not worth a knob, only "spread, never synchronized" is the
	// requirement.
	clientLifetimeJitterMinFraction = 0.75

	// collapseDeadlineLifetimes sets the capacity-collapse hard deadline at
	// this many MaxClientLifetimes past a client's first event. The collapse
	// gate keeps a flow-carrying client alive through capacity collapse --
	// flowless is the normal removal path -- but without an upper bound one
	// immortal flow (a weeks-long idle ssh session that never goes flowless)
	// would pin its exit in the window forever, and a window over hard max
	// could never converge. 2x is deliberately generous: it is a whole extra
	// lifetime beyond the point where the drain warning stopped new flows
	// from landing on the client, so anything still bound to it has had an
	// entire rotation period to finish on its own. Measured against the raw
	// MaxClientLifetime, not the jittered per-channel lifetime, so the
	// deadline is a stable bound that is always at least twice any effective
	// lifetime.
	collapseDeadlineLifetimes = 2

	// standingReserveSpares is how many spare exits StandingReserve holds
	// beyond the computed target window size. One is the measured need: the
	// ~45s failover backfill gap is closed by any already-evaluated spare,
	// and each additional spare is another idle provider connection paying
	// for a second simultaneous failure, which the resize tick already
	// covers within ~15s.
	standingReserveSpares = 1
)

// jitterClientLifetime returns the effective lifetime for one channel:
// maxClientLifetime scaled by uniform [clientLifetimeJitterMinFraction, 1.0),
// drawn once at construction and fixed for the channel's life. Drawing per
// channel (not per read) is what desynchronizes rotation: channels created in
// the same expand burst land their removeTimes across the jitter band instead
// of in one resize pass. A non-positive lifetime is returned unchanged --
// 0 (or negative) MaxClientLifetime means rotation is disabled, and jitter
// must not invent a lifetime where none was configured.
func jitterClientLifetime(maxClientLifetime time.Duration) time.Duration {
	if maxClientLifetime <= 0 {
		return maxClientLifetime
	}
	fraction := clientLifetimeJitterMinFraction + mathrand.Float64()*(1.0-clientLifetimeJitterMinFraction)
	return time.Duration(float64(maxClientLifetime) * fraction)
}

// standingReserveTarget applies the standing reserve to a window's computed
// target size: +standingReserveSpares beyond the target, bounded by
// windowSizeHardMax (0 hard max is unbounded, as everywhere else). The spare
// exists so that when an exit fails or drains, its replacement has already
// been dialed and evaluated -- the measured motivation is a ~45s failover
// backfill that only started after a loss was noticed. Deliberately allowed
// to exceed WindowSizeMax: the max bounds demand-driven growth, while the
// spare is capacity insurance on top of whatever target that math produced.
//
// The reserve is skipped when:
//   - standingReserve is off (the A/B comparison point, and what a zero
//     ReliabilitySettings -- every bare fixture -- reads),
//   - the generator has a fixed destination set (fixedDestination): there is
//     nothing beyond the set to hold in reserve, and asking for it would
//     leave every expand pass waiting out its timeout on args that cannot
//     arrive,
//   - the computed target is 0, which is how resize disables a non-active
//     fixed-profile window -- a spare there would silently re-enable it.
func standingReserveTarget(
	targetWindowSize int,
	windowSizeHardMax int,
	standingReserve bool,
	fixedDestination bool,
) int {
	if !standingReserve || fixedDestination || targetWindowSize <= 0 {
		return targetWindowSize
	}
	reserveTargetWindowSize := targetWindowSize + standingReserveSpares
	if 0 < windowSizeHardMax {
		reserveTargetWindowSize = min(reserveTargetWindowSize, windowSizeHardMax)
	}
	return reserveTargetWindowSize
}

type multiClientWindow struct {
	ctx    context.Context
	cancel context.CancelFunc
	log    Logger

	// Consecutive platform rate limits across ALL expanders in this window.
	// Shared deliberately: the herd is the problem, so the backoff has to be a
	// property of the window rather than of each goroutine. See
	// windowRetryDelay.
	rateLimitStreak atomic.Uint64

	generator                    MultiClientGenerator
	clientReceivePacketCallback  clientReceivePacketFunction
	clientReceivePacketsCallback clientReceivePacketsFunction
	dialFailureCallback          dialFailureFunction
	// networkPeerDestination is true only when the embedding app explicitly
	// selected a trusted same-network peer and the entire multi-client uses
	// the Network relationship.
	networkPeerDestination bool
	ingressSecurityPolicy  SecurityPolicy
	clientRemoveCallback   func(client *multiClientChannel)
	windowType             WindowType

	settings *MultiClientSettings
	// reliabilitySettingsFunc reads the parent's effective reliability config,
	// which the developer menu can override at runtime. A callback rather than a
	// back-reference so the window keeps its existing lifetime and the bare
	// window fixtures in the suite stay constructible. nil falls back to
	// `settings`.
	reliabilitySettingsFunc func() *ReliabilitySettings
	// uplinkGateFunc reads the parent's tunnel-wide uplink gate for the
	// channels' verdict passes; see RemoteUserNatMultiClient.uplinkGate. Same
	// callback convention as reliabilitySettingsFunc; nil (bare fixtures)
	// reads as gate off.
	uplinkGateFunc func(now time.Time) (stale bool, freshSince time.Time)
	// reliabilityMetricsFunc hands the parent's shared counters to the
	// channels. nil (bare fixtures) loses the counts, never the traffic --
	// every counter method tolerates a nil receiver.
	reliabilityMetricsFunc func() *reliabilityMetrics
	// flowCountFunc reads how many live flows the parent currently has bound
	// to a window client (the clientUpdates bookkeeping, read under the parent
	// stateLock). Same callback convention as reliabilitySettingsFunc. nil --
	// bare test windows -- reads as 0 for every client, which means the
	// capacity/removal gates treat every client as flowless: exactly the
	// pre-change execute-immediately behavior those fixtures assert.
	//
	// Locking: the func takes the parent stateLock inside, so it must only be
	// called with no window or channel lock held. Every resize call site reads
	// it from the classification loop, which holds nothing.
	flowCountFunc func(*multiClientChannel) int
	// providerQualifiedFunc reads the parent's qualification table by
	// destination. The window itself consults it in expand's admit selection
	// (prefer qualified candidates) and hands it to every channel it builds
	// (the effectiveTier demerit). Same callback convention and same
	// parent-stateLock contract as flowCountFunc; nil -- bare test windows --
	// reads as nothing qualified, which makes admit selection plain arrival
	// order and the channel demerit inert.
	providerQualifiedFunc func(MultiHopId) bool
	// receivingSiblingsFunc is handed to every channel for the comparative
	// connect cut's evidence (RemoteUserNatMultiClient.receivingChannelCount);
	// see the channel field. nil on bare test windows, which reads as zero
	// receiving siblings and leaves the full connect bar in place.
	receivingSiblingsFunc func(exclude *multiClientChannel) int
	// qualificationRefreshFunc is handed to every channel for the receive-ack
	// qualification refresh; see the channel field. nil on bare test windows.
	qualificationRefreshFunc func(MultiHopId)
	// clientMigrateFunc is G-3's drain-time seam: the parent's
	// migrateClientFlows, called once when the resize pass starts draining an
	// exit so its movable flows leave while everything else finishes
	// naturally. Set by the parent after construction rather than through the
	// (already long) constructor; nil -- bare test windows -- makes a drain
	// behave exactly as it did before migration existed. The func takes the
	// parent stateLock inside, so the same contract as flowCountFunc applies:
	// call it with no window or channel lock held.
	clientMigrateFunc func(client *multiClientChannel, cause string) (rebound int, replacements int, remaining int)

	clientChannelArgs chan *multiClientChannelArgs

	monitor *RemoteUserNatMultiClientMonitor

	contractStatusCallbacks *CallbackList[*contractStatusCallbackWorker]
	contractStatsCallbacks  *CallbackList[ContractStatsFunction]
	// relayed from every window client's encryption session manager
	peerIdentityChangeCallbacks *CallbackList[func()]

	stateLock          sync.Mutex
	clients            map[Id]*multiClientChannel
	performanceProfile *PerformanceProfile
	// Nil test seams expose one initial-evaluation callback across the exact
	// expand-pass terminal boundary. Production leaves all four unset.
	beforeExpandPingResultForTest func()
	afterExpandPingResultForTest  func()
	finishExpandPassForTest       <-chan struct{}
	expireExpandPassForTest       <-chan struct{}
	// verdictRemovalTimes is the storm breaker's record of recent
	// verdict-driven removals, pruned to RemovalBudgetWindow on each check.
	// Guarded by stateLock. Only removals a verdict argued for are recorded
	// -- user action, cleanup of already-dead clients, lifetime drains, and
	// capacity collapse never touch it. See verdictRemovalAllowed.
	verdictRemovalTimes []time.Time

	generatorMonitor *Monitor
	resizeMonitor    *Monitor

	// --- window honesty (see ip_remote_multi_client_outcome.go) ---

	// failures counts recent classified evaluation failures for the stall
	// reason. Its own lock inside; safe from any goroutine.
	failures *windowFailureRecorder
	// outcomeLock guards the outcome state machine below. A dedicated small
	// lock — never held while logging, dispatching, or touching stateLock.
	outcomeLock sync.Mutex
	// outcomeArmTime is when the window first tried to expand (zero = never);
	// reset by the automatic rebuild so the second deadline measures from it.
	outcomeArmTime time.Time
	// outcomeRebuilt: the ONE automatic rebuild has been spent.
	outcomeRebuilt bool
	// outcomeFailed is a UI/status latch cleared by noteClientAdded. It never
	// terminates enumeration, resize, evaluation, or carrier retries.
	outcomeFailed bool
	// everAdded: a provider has been installed; permanently disarms the
	// outcome watchdog for this window.
	everAdded bool
	// evalEpochCtx is the context evaluation channels are built under; the
	// rebuild cancels and replaces it. See evalEpochContext.
	evalEpochCtx    context.Context
	evalEpochCancel context.CancelFunc

	// throttles for the unconditional evaluation-failure lines, one per line
	// shape, on the "(N suppressed)" pattern the egress-dial evidence uses
	createFailThrottle    *logThrottle
	pingFailThrottle      *logThrottle
	budgetFailThrottle    *logThrottle
	enumerateZeroThrottle *logThrottle

	// --- address family (see ip_remote_multi_client_family.go) ---

	// ipv6Shortfall is the v6-capable shortfall the resize pass last
	// computed: how many v6-capable exits the window wants and has none of
	// (0 when none, or when the soft minimum does not apply). The enumerator
	// asks for that many v6-capable candidates ahead of the main fill.
	// ipv6StarvedUntil is when it may ask again after a request returned
	// nothing. Both guarded by stateLock.
	ipv6Shortfall    int
	ipv6StarvedUntil time.Time
	// ipv6ProbeRequested asks the enumerator for one probe-only round (the
	// v6-capable request alone) before the resize pass gives up an exit for
	// the candidate; ipv6CandidateReady is set while a probe's candidate is
	// minted and waiting to be consumed, which is what licenses the swap.
	// ipv6FillFailures counts v6-capable candidates that failed evaluation
	// while the shortfall stayed open; two in a row starve the family so a
	// dud candidate cannot churn an exit per pass. All guarded by stateLock.
	ipv6ProbeRequested bool
	ipv6CandidateReady bool
	ipv6FillFailures   int
}

func newMultiClientWindow(
	ctx context.Context,
	cancel context.CancelFunc,
	generator MultiClientGenerator,
	clientReceivePacketCallback clientReceivePacketFunction,
	clientReceivePacketsCallback clientReceivePacketsFunction,
	networkPeerDestination bool,
	dialFailureCallback dialFailureFunction,
	ingressSecurityPolicy SecurityPolicy,
	clientRemoveCallback func(client *multiClientChannel),
	windowType WindowType,
	initialPerformanceProfile *PerformanceProfile,
	settings *MultiClientSettings,
	reliabilitySettingsFunc func() *ReliabilitySettings,
	uplinkGateFunc func(now time.Time) (stale bool, freshSince time.Time),
	reliabilityMetricsFunc func() *reliabilityMetrics,
	flowCountFunc func(*multiClientChannel) int,
	providerQualifiedFunc func(MultiHopId) bool,
	receivingSiblingsFunc func(exclude *multiClientChannel) int,
	qualificationRefreshFunc func(MultiHopId),
) *multiClientWindow {
	window := &multiClientWindow{
		ctx:                          ctx,
		cancel:                       cancel,
		log:                          loggerOrDefault(settings.Log),
		generator:                    generator,
		clientReceivePacketCallback:  clientReceivePacketCallback,
		clientReceivePacketsCallback: clientReceivePacketsCallback,
		networkPeerDestination:       networkPeerDestination,
		dialFailureCallback:          dialFailureCallback,
		ingressSecurityPolicy:        ingressSecurityPolicy,
		clientRemoveCallback:         clientRemoveCallback,
		windowType:                   windowType,
		performanceProfile:           initialPerformanceProfile,
		settings:                     settings,
		reliabilitySettingsFunc:      reliabilitySettingsFunc,
		uplinkGateFunc:               uplinkGateFunc,
		reliabilityMetricsFunc:       reliabilityMetricsFunc,
		flowCountFunc:                flowCountFunc,
		providerQualifiedFunc:        providerQualifiedFunc,
		receivingSiblingsFunc:        receivingSiblingsFunc,
		qualificationRefreshFunc:     qualificationRefreshFunc,
		clientChannelArgs:            make(chan *multiClientChannelArgs),
		monitor:                      NewRemoteUserNatMultiClientMonitor(&settings.RemoteUserNatMultiClientMonitorSettings),
		contractStatusCallbacks:      NewCallbackList[*contractStatusCallbackWorker](),
		contractStatsCallbacks:       NewCallbackList[ContractStatsFunction](),
		peerIdentityChangeCallbacks:  NewCallbackList[func()](),
		clients:                      map[Id]*multiClientChannel{},
		generatorMonitor:             NewMonitor(),
		resizeMonitor:                NewMonitor(),
		failures:                     &windowFailureRecorder{},
		createFailThrottle:           newLogThrottle(evaluationFailureLogInterval),
		pingFailThrottle:             newLogThrottle(evaluationFailureLogInterval),
		budgetFailThrottle:           newLogThrottle(evaluationFailureLogInterval),
		enumerateZeroThrottle:        newLogThrottle(evaluationFailureLogInterval),
	}
	window.evalEpochCtx, window.evalEpochCancel = context.WithCancel(ctx)

	go HandleError(window.randomEnumerateClientArgs, cancel)
	go HandleError(window.resize, cancel)
	go HandleError(window.watchSendStalls, cancel)
	// deliberately NOT wired to `cancel`, like the heartbeat and the prober: a
	// watchdog that exists to describe and rescue the window must never be
	// able to tear down the tunnel
	go HandleError(window.watchOutcome)

	return window
}

// sendStallPollTimeout is how often the stall watchdog looks.
//
// A fraction of the stall timeout, so a stall is noticed within roughly its own
// timeout rather than a multiple of it, with a floor so a very small timeout
// cannot turn the watchdog into a busy loop. When stall detection is disabled
// the watchdog idles at the resize cadence instead of exiting, so enabling it
// at runtime from the developer menu is picked up without a reconnect.
func sendStallPollTimeout(stallTimeout time.Duration, resizeTimeout time.Duration) time.Duration {
	if stallTimeout <= 0 {
		return resizeTimeout
	}
	return max(stallTimeout/3, 250*time.Millisecond)
}

// watchSendStalls evaluates a client once its acknowledged-send progress stops.
//
// The watchdog polls on a fraction of the stall timeout (resize otherwise runs
// on WindowResizeTimeout -- 15s, and detecting a stall at 3s is worth nothing
// if it is only consulted every 15s). Earlier this loop only NOTIFIED resize
// and left the verdict to the resize pass's own sendStalled check, preserving
// a single classifier. On device that split the detection-to-removal latency
// across whatever else the resize pass was doing: 3-18s depending on where the
// sweep was when the notify landed, for a verdict whose whole reason to exist
// is a consistent 3-4s rescue.
//
// The complete verdict executes HERE (convictSendStalls): raw sendStalled is
// only the trigger because a dead exit, a congested uplink, and a shared path
// outage can all present as outstanding bytes with no ACK progress. The active
// probe and uplink/shared-fate/quarantine gates decide whether that trigger is
// admissible. An admitted verdict errors and cancels the channel immediately;
// resize is woken only to reap it and backfill the window. Soft classifications
// (stats-unhealthy, rank, drain) remain exclusively in resize.
func (self *multiClientWindow) watchSendStalls() {
	for {
		stallTimeout := self.reliabilitySettings().SendStallTimeout

		pollTimeout := sendStallPollTimeout(stallTimeout, self.settings.WindowResizeTimeout)

		select {
		case <-self.ctx.Done():
			return
		case <-time.After(pollTimeout):
		}

		if stallTimeout <= 0 {
			continue
		}
		if self.convictSendStalls(stallTimeout) {
			// wake resize for the reap and the backfill only; the verdict has
			// already been executed above
			self.resizeMonitor.NotifyAll()
		}
	}
}

// convictSendStalls is one watchdog pass: every client currently past the
// stall bound is errored and cancelled here, and the pass reports whether it
// convicted anything so the caller can wake resize for cleanup and backfill.
//
// The error is added BEFORE Cancel so it wins the endErr slot (addError keeps
// the first error; Cancel writes "Done.") -- the field log's removal line then
// names the actual reason instead of a bare "Done.".
//
// The reason string must NEVER gain a "Blackhole " prefix: the storm breaker
// (blackholeVerdictErr / verdictRemovalAllowed) budgets removals keyed on that
// prefix because verdict evidence is correlated and soft. A send-stall verdict
// that survives the probe and cross-exit gates is hard evidence; budgeting that
// admitted verdict would hold a dead exit in the window, freezing every flow
// pinned to it. The resize pass reaps this channel through its ordinary
// WindowStats error branch, exactly the cancel-then-reap path DropExit already
// exercises.
// The busy-flow liveness probe (BusyProbe, concept ported from upstream main
// e05ecee) interposes here and ONLY here. The bar tripping is the trigger it
// always was; what changed is that between the trigger and the verdict the exit
// gets asked one question. An acquitted exit is left alone with its stall bar
// refreshed (see addBusyProbeAck); a convicted one is errored and cancelled
// exactly as before, with the reason extended to name the probe outcome. With
// BusyProbe off -- the zero value, and every bare fixture -- no question is
// asked and this is the previous function.
//
// The probes run concurrently because they all wait out the same budget: a
// window with four stalled exits must not take four budgets to judge them, and
// the wait is idle time, not work. Each probe touches only its own channel's
// state; the verdicts are applied serially afterwards so the addError-before-
// Cancel ordering above is preserved per channel.
func (self *multiClientWindow) convictSendStalls(stallTimeout time.Duration) bool {
	var stalled []*multiClientChannel
	for _, client := range self.unorderedClients() {
		if client.sendStalled(stallTimeout) {
			stalled = append(stalled, client)
		}
	}
	if len(stalled) == 0 {
		return false
	}

	// verdicts[i] is what to do about stalled[i]. Pre-filled to convict with
	// today's exact reason, which is what the probe-disabled path wants and
	// what a channel with no probe plumbing under it falls back to.
	verdicts := make([]busyProbeVerdict, len(stalled))
	for i := range verdicts {
		verdicts[i] = busyProbeVerdict{convict: true}
	}

	reliabilitySettings := self.reliabilitySettings()
	if reliabilitySettings.BusyProbe {
		budget := busyProbeBudget(stallTimeout, reliabilitySettings.BusyProbeBudget)
		probeWait := sync.WaitGroup{}
		for i, client := range stalled {
			probeWait.Add(1)
			go HandleError(func() {
				defer probeWait.Done()
				verdicts[i] = client.busyLivenessProbe(budget)
			})
		}
		probeWait.Wait()
	}

	// the sibling-corroboration gate. The probe rides the same possibly-dead
	// uplink it is investigating, so its timeout can never distinguish "this
	// exit is dead" from "the phone is": one cellular blip shorter than the
	// uplink gate's 5s executed three loaded exits in three minutes in the
	// field (2026-08-03), because 3s stall + 1.5s probe outruns that gate --
	// the same shape the receive verdicts were stormed by before THEY were
	// gated. A stall conviction is admissible only while some OTHER window
	// client shows receive progress inside the judged interval: return
	// traffic arriving anywhere proves the tunnel carries packets, which
	// makes this exit's total silence evidence about the exit. When nothing
	// anywhere is receiving, every verdict is held un-refreshed -- the next
	// pass re-judges, and a real stall convicts on the first pass after the
	// uplink proves out. A window with no sibling to consult (fixed single
	// exit) never convicts here; the ordinary blackhole and cping paths
	// still cover it.
	receiveWindow := stallTimeout + busyProbeBudget(stallTimeout, reliabilitySettings.BusyProbeBudget)
	receivingElsewhere := func(exclude *multiClientChannel) bool {
		for _, client := range self.unorderedClients() {
			if client == exclude {
				continue
			}
			// data receive, or a sibling's own liveness-probe ack: both are
			// return packets that crossed the uplink. The probe-ack half is
			// what lets a window with several simultaneously stalled exits
			// resolve -- one acquittal proves the uplink for judging the
			// rest, where data-receive alone would hold everything until an
			// unrelated site happened to answer.
			if client.hasRecentReceive(receiveWindow) || client.hasRecentBusyProbeAck(receiveWindow) {
				return true
			}
		}
		return false
	}

	// every stalled exit is CURRENT evidence for the shared-fate window,
	// recorded for ALL of them before ANY is judged -- recording inside the
	// judgment loop would let the first-judged exit convict before its
	// co-sufferers were visible
	reliabilitySettings = self.reliabilitySettings()
	sharedFateOn := reliabilitySettings != nil &&
		0 < reliabilitySettings.SharedFateMinExits &&
		0 < reliabilitySettings.SharedFateWindow
	if sharedFateOn {
		now := time.Now()
		for _, client := range stalled {
			client.metrics().recordSharedFate(client.ClientId(), now)
		}
	}

	convicted := false
	for i, client := range stalled {
		verdict := verdicts[i]
		if !verdict.convict {
			// acquitted by the probe, or deferred to the next pass (a single
			// unsendable probe). Nothing is written here: the acquittal already
			// refreshed the stall bar through addBusyProbeAck, and there is no
			// un-conviction to undo.
			//
			// At default verbosity because each of these lines is a removal
			// that did NOT happen -- the single most useful thing a field
			// capture can say about this port. It is bounded, not spam: an
			// acquittal refreshes the stall bar, so the same exit cannot
			// produce another line for a full SendStallTimeout, and only an
			// exit making literally zero ack progress produces them at all.
			loggerOrDefault(self.log).Infof("%s\n", relEvent(
				"busy_probe",
				"exit", client.ClientId(),
				"outcome", "acquitted",
				"detail", verdict.detail,
				"bar", stallTimeout,
			))
			continue
		}
		if client.isQuarantined() {
			// held: the bench already owns this exit's lifecycle. New flows
			// are stopped, the movable flows were handed off at bench time,
			// and the quarantine acquits on receive progress or executes on
			// sustained silence -- a second judge executing mid-bench
			// destroys exactly the flows the bench is protecting. The field
			// capture that motivates this (2026-08-05, stable providers):
			// every stall conviction in the window landed 6-41s after a
			// bench, ahead of the acquittal that receive-silence benches on
			// that pool overwhelmingly earn. The stall clock is deliberately
			// NOT refreshed, so a real stall still convicts on carried
			// evidence the first pass after the bench lifts.
			client.resetBusyProbeSendFailures()
			if client.markStallHoldOnce() {
				loggerOrDefault(self.log).Infof("%s\n", relEvent(
					"busy_probe",
					"exit", client.ClientId(),
					"outcome", "held",
					"detail", "benched: quarantine owns the verdict",
					"bar", stallTimeout,
				))
			}
			continue
		}
		if !receivingElsewhere(client) {
			// held, not acquitted: the stall clock is deliberately NOT
			// refreshed, so the evidence carries into the next pass and a
			// real stall still convicts the moment a sibling proves the
			// uplink. Two things ARE reset here:
			//   - the unsendable-probe run: probes fired while the verdict
			//     was inadmissible are evidence about the phone, and letting
			//     them accumulate would convict on "unsendable 2x" one pass
			//     after the gate opens -- executing the exact exits the hold
			//     protects (review finding, 2026-08-03). A genuinely dead
			//     exit still convicts via probe timeout on the first open
			//     pass.
			//   - nothing else.
			// The hold is booked and logged once per stall episode (the
			// latch), matching the per-episode semantics the uplink-hold
			// counter has in detectBlackhole -- per-pass counting there would
			// book ~24 holds/exit/minute and make the counter unreadable.
			client.resetBusyProbeSendFailures()
			if client.markStallHoldOnce() {
				client.metrics().verdictHeldUplinkStale()
				loggerOrDefault(self.log).Infof("%s\n", relEvent(
					"busy_probe",
					"exit", client.ClientId(),
					"outcome", "held",
					"detail", "no receiving sibling: uplink unproven",
					"bar", stallTimeout,
				))
			}
			continue
		}
		if sharedFateOn {
			peers := client.metrics().sharedFatePeers(client.ClientId(), time.Now(), reliabilitySettings.SharedFateWindow)
			if reliabilitySettings.SharedFateMinExits <= peers+1 {
				// held: this many exits stalling in the same seconds is one
				// fact about the shared path, not peers+1 facts about
				// providers. The stall clock is NOT refreshed -- the first
				// pass after the correlation clears convicts a real stall on
				// carried evidence.
				client.resetBusyProbeSendFailures()
				if client.markStallHoldOnce() {
					client.metrics().verdictHeldSharedFate()
					loggerOrDefault(self.log).Infof("%s\n", relEvent(
						"busy_probe",
						"exit", client.ClientId(),
						"outcome", "held",
						"detail", "shared fate",
						"peers", peers,
						"bar", stallTimeout,
					))
				}
				continue
			}
		}
		reason := fmt.Sprintf("no ack progress for %s", stallTimeout)
		if verdict.detail != "" {
			reason = fmt.Sprintf("%s, %s", reason, verdict.detail)
		}
		client.addError(fmt.Errorf("send stalled: %s", reason))
		client.Cancel()
		loggerOrDefault(self.log).Infof("%s\n", relEvent(
			"busy_probe",
			"exit", client.ClientId(),
			"outcome", "convicted",
			"detail", reason,
			"bar", stallTimeout,
		))
		convicted = true
	}
	return convicted
}

func (self *multiClientWindow) AddContractStatusCallback(contractStatusCallback ContractStatusFunction) func() {
	worker := newContractStatusCallbackWorker(
		self.ctx,
		contractStatusCallback,
		self.contractStatusPendingMaxCount(),
	)
	callbackId := self.contractStatusCallbacks.Add(worker)
	return func() {
		self.contractStatusCallbacks.Remove(callbackId)
		worker.Close()
	}
}

// contractStatusPendingMaxCount sizes observability to the contracts that can
// actually be live in this window, not to Transfer's packet sequence channel.
// A window can place at most MaxFlowsPerExit flows on each admitted exit, and
// each live flow needs at most one latest status entry. The fixed ring remains
// bounded when settings are partial in tests.
func (self *multiClientWindow) contractStatusPendingMaxCount() int {
	windowHardMax := 1
	if self.settings != nil {
		if windowSize, ok := self.settings.WindowSizes[self.windowType]; ok &&
			0 < windowSize.WindowSizeHardMax {
			windowHardMax = windowSize.WindowSizeHardMax
		}
		return max(1, windowHardMax*max(1, self.settings.MaxFlowsPerExit))
	}
	return 1
}

func (self *multiClientWindow) contractStatus(contractStatus *ContractStatus) {
	for _, contractStatusCallback := range self.contractStatusCallbacks.Get() {
		contractStatusCallback.Dispatch(contractStatus)
	}
}

var errContractReliability = errors.New("Contract reliability failure.")

// contractStatusMatchesClient keeps a reliability result scoped to the exact
// window client whose destination requested the contract. A ContractManager is
// owned by one channel, but the explicit key check makes that ownership
// invariant fail closed if the manager ever starts carrying other routes.
func contractStatusMatchesClient(client *multiClientChannel, status *ContractStatus) bool {
	if client == nil || client.args == nil || status == nil || client.args.Destination.Len() == 0 {
		return false
	}
	return status.Key.Destination.DestinationId == client.args.Destination.Tail()
}

// contractStatusFromClient receives the status together with the channel that
// emitted it. Reliability is the platform's authoritative statement that the
// selected destination is no longer a usable route: exclude that channel from
// new-flow selection immediately, make its WindowStats terminal, and wake the
// ordinary resize path to remove it, migrate eligible flows, and backfill the
// window. Other contract errors are account, policy, setup, or trust results;
// they remain observable but cannot poison provider selection.
func (self *multiClientWindow) contractStatusFromClient(client *multiClientChannel, status *ContractStatus) {
	if contractStatusMatchesClient(client, status) &&
		status.Error != nil &&
		*status.Error == protocol.ContractError_Reliability {
		client.contractReliabilityOnce.Do(func() {
			// The exclusion must become visible to discovery before resize is
			// woken. Otherwise the first replacement pass can immediately hand
			// the terminal destination back to this window.
			if self.generator != nil {
				if _, fixed := self.generator.FixedDestinationSize(); !fixed {
					if excluder, ok := self.generator.(MultiClientGeneratorExcluder); ok {
						excluder.ExcludeClientId(client.args.Destination.Tail())
					}
				}
			}
			client.setWarning(true, warnUnhealthy)
			client.addError(errContractReliability)
			if self.resizeMonitor != nil {
				self.resizeMonitor.NotifyAll()
			}
		})
	}

	self.contractStatus(status)
}

func (self *multiClientWindow) AddContractStatsCallback(contractStatsCallback ContractStatsFunction) func() {
	callbackId := self.contractStatsCallbacks.Add(contractStatsCallback)
	return func() {
		self.contractStatsCallbacks.Remove(callbackId)
	}
}

// registered on every window client's contract manager.
// the manager's epoch worker calls this off the packet paths
func (self *multiClientWindow) contractStats(contractStatsEvents []*ContractStatsEvent) {
	for _, contractStatsCallback := range self.contractStatsCallbacks.Get() {
		HandleError(func() {
			contractStatsCallback(contractStatsEvents)
		})
	}
}

func (self *multiClientWindow) AddPeerIdentityChangeCallback(callback func()) func() {
	callbackId := self.peerIdentityChangeCallbacks.Add(callback)
	return func() {
		self.peerIdentityChangeCallbacks.Remove(callbackId)
	}
}

// registered on every window client's encryption session manager
// (and fired once more when a window client is removed)
func (self *multiClientWindow) peerIdentityChanged() {
	for _, callback := range self.peerIdentityChangeCallbacks.Get() {
		HandleError(callback)
	}
}

// the performance profile will take effect at the next `resize` iteration
func (self *multiClientWindow) SetPerformanceProfile(performanceProfile *PerformanceProfile) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.performanceProfile = performanceProfile
}

// generatorCallResult carries one generator call's result across the deadline
// boundary in windowGeneratorCall.
type generatorCallResult[T any] struct {
	value T
	err   error
}

// windowGeneratorCall runs one generator call with a deadline so a hung
// platform API can never wedge the window's enumerate/expand machinery.
// Ported as a concept from upstream main e05ecee's generator deadlines.
//
// The MultiClientGenerator interface does not accept a context, so the
// deadline cannot cancel the call: past the deadline the call is ABANDONED —
// the wrapper returns an error while the underlying call keeps running and
// may complete late. Exactly one result is produced per call (a panic inside
// the call is converted to an error result), the handoff channel is buffered,
// and a drain goroutine consumes the late result, so neither the producer nor
// the drain can park forever on the handoff — the only goroutine that can
// linger is the hung call itself, which is irreducible without context
// support in the generator API.
//
// The late result must therefore be one of:
//   - side-effect-safe to discard (lateResult nil): NextDestinations returns
//     a listing that allocates no per-call platform state, or
//   - routed to cleanup (lateResult non-nil): NewClientArgs creates a
//     platform-side network client, so an abandoned call's late value goes to
//     the same RemoveClientArgs cleanup the decline paths use.
//
// timeout <= 0 calls directly with no deadline — the pre-change
// trust-the-API behavior.
func windowGeneratorCall[T any](
	ctx context.Context,
	timeout time.Duration,
	call func() (T, error),
	lateResult func(value T, err error),
) (T, error) {
	if timeout <= 0 {
		return call()
	}
	startTime := time.Now()
	// cap 1: the producer's single send never blocks, so an abandoned call's
	// goroutine exits as soon as the call itself returns
	out := make(chan generatorCallResult[T], 1)
	go HandleError(func() {
		value, err := call()
		out <- generatorCallResult[T]{value: value, err: err}
	}, func(err error) {
		// the call panicked before producing a result: convert it to an error
		// result so the waiter (or the late drain below) is never parked
		out <- generatorCallResult[T]{err: err}
	})
	canceled := false
	select {
	case result := <-out:
		return result.value, result.err
	case <-ctx.Done():
		canceled = true
	case <-time.After(timeout):
	}
	// abandoned: drain the eventual result off-path
	go HandleError(func() {
		result := <-out
		if lateResult != nil {
			lateResult(result.value, result.err)
		}
	})
	var zero T
	if canceled {
		// ctx cancel is local teardown, not a hung generator, and it fires
		// however little time has passed — stamping this path with the
		// configured timeout produced logs claiming a 20s abandonment for a
		// wait that lasted milliseconds
		return zero, fmt.Errorf("generator call canceled")
	}
	return zero, fmt.Errorf("generator call abandoned after %s", time.Since(startTime))
}

// removeLateClientArgs is the lateResult route for abandoned NewClientArgs /
// NewClientArgsForDestination calls: the call completed after its deadline,
// so its platform-side network client exists but nothing will ever use it.
// RemoveClientArgs is the same cleanup every decline path uses (expired args,
// ctx teardown), so the late client is deleted (and dropped from the identity
// store) instead of leaking until server-side idle reap.
func (self *multiClientWindow) removeLateClientArgs(clientArgs *MultiClientGeneratorClientArgs, err error) {
	if err == nil && clientArgs != nil {
		loggerOrDefault(self.log).Infof("[multi]abandoned client args completed late; removing\n")
		self.generator.RemoveClientArgs(clientArgs)
	}
}

func (self *multiClientWindow) randomEnumerateClientArgs() {
	defer func() {
		close(self.clientChannelArgs)

		// drain the channel
		func() {
			for {
				select {
				case args, ok := <-self.clientChannelArgs:
					if !ok {
						return
					}
					self.generator.RemoveClientArgs(&args.MultiClientGeneratorClientArgs)
				}
			}
		}()
	}()

	for {
		generatorNotify := self.generatorMonitor.NotifyChannel()

		// exclude healthy destinations that are already in the window
		windowDestinations := func() map[MultiHopId]bool {
			self.stateLock.Lock()
			defer self.stateLock.Unlock()
			windowDestinations := map[MultiHopId]bool{}
			for _, client := range self.clients {
				if !client.isWarning() {
					windowDestinations[client.Destination()] = true
				}
			}
			return windowDestinations
		}

		// deadline-wrapped: a hung platform API surfaces here as an error on
		// the ordinary retry cadence instead of wedging this goroutine (and
		// with it every future window expand). A late listing carries no
		// per-call platform state, so its discard route is nil.
		// family-aware discovery order: v6-capable first while a shortfall
		// is open, then the main fill -- or, for a resize-pass probe, the
		// v6-capable request alone; see enumerateDestinations
		probeOnly := self.takeIpv6ProbeRequest()
		destinations, err := self.enumerateDestinations(
			slices.Collect(maps.Keys(windowDestinations())),
			probeOnly,
		)
		if err != nil {
			select {
			case <-self.ctx.Done():
				return
			default:
			}
			self.log.Infof("[multi]window enumerate error timeout = %s\n", err)
			// a hung/erroring platform api is the platform-unreachable class
			// unless the message names auth or rate limiting
			class := self.recordEvaluationFailure(windowFailurePlatform, err)
			select {
			case <-self.ctx.Done():
				return
			case <-time.After(self.windowRetryDelay(class)):
			}
			continue
		}
		// The platform answered. Any success clears the rate-limit backoff so
		// the window returns to its normal cadence once the limit lifts.
		self.windowRetryReset()

		// unconditional (V0): the platform answered with ZERO candidates. Only
		// a failure while the window is empty — a full window legitimately
		// enumerates nothing because its destinations are all excluded.
		if len(destinations) == 0 && len(self.unorderedClients()) == 0 {
			if ok, suppressed := self.enumerateZeroThrottle.Allow(time.Now()); ok {
				self.log.Infof("[multi]window enumerate returned zero providers%s\n",
					suppressedSuffix(suppressed))
			}
			self.recordEvaluationFailure(windowFailureProvider, nil)
		}

		if probeOnly && 0 < len(destinations) {
			// the probe found a v6-capable candidate: the resize pass may now
			// give an exit up for it. The candidate stays "ready" for as long
			// as the push below is waiting to be consumed.
			self.setIpv6CandidateReady(true)
			self.resizeMonitor.NotifyAll()
		}
		func() {
			// destinations must be used by `expirationTime`
			expirationTime := time.Now().Add(self.settings.WindowExpandArgsTimeout)
			for _, enumerated := range destinations {
				destination, stats := enumerated.destination, enumerated.stats

				for {
					timeout := expirationTime.Sub(time.Now())
					if timeout <= 0 {
						return
					}

					// a destination-aware generator can reuse a persisted
					// identity for this destination (PROXYDRAIN1.md §3.5).
					// deadline-wrapped like NextDestinations above — but this
					// call creates a platform-side network client, so an
					// abandoned call's late success is routed to
					// removeLateClientArgs rather than discarded.
					var clientArgs *MultiClientGeneratorClientArgs
					var err error
					if destinationGenerator, ok := self.generator.(MultiClientGeneratorWithDestination); ok {
						clientArgs, err = windowGeneratorCall(
							self.ctx,
							self.settings.WindowGeneratorTimeout,
							func() (*MultiClientGeneratorClientArgs, error) {
								return destinationGenerator.NewClientArgsForDestination(destination)
							},
							self.removeLateClientArgs,
						)
					} else {
						clientArgs, err = windowGeneratorCall(
							self.ctx,
							self.settings.WindowGeneratorTimeout,
							self.generator.NewClientArgs,
							self.removeLateClientArgs,
						)
					}
					if err != nil {
						select {
						case <-self.ctx.Done():
							return
						default:
						}
						self.log.Infof("[multi]create client args error = %s\n", err)
						// platform api again: the client mint is a platform
						// round trip, so its timeout is platform-unreachable
						// (auth/rate refine via the message)
						class := self.recordEvaluationFailure(windowFailurePlatform, err)
						select {
						case <-self.ctx.Done():
							return
						case <-time.After(self.windowRetryDelay(class)):
						}
						continue
					}
					self.windowRetryReset()

					_, fixedDestination := self.generator.FixedDestinationSize()
					args := &multiClientChannelArgs{
						Destination:                    destination,
						DestinationStats:               stats,
						MultiClientGeneratorClientArgs: *clientArgs,
						FixedDestination:               fixedDestination,
					}
					select {
					case <-self.ctx.Done():
						self.generator.RemoveClientArgs(clientArgs)
						return
					case self.clientChannelArgs <- args:
					case <-time.After(timeout):
						// destination expired
						self.log.Infof("[multi]create client args expired\n")
						self.generator.RemoveClientArgs(clientArgs)
						return
					}
					break
				}
			}
		}()
		if probeOnly {
			self.setIpv6CandidateReady(false)
		}

		select {
		case <-self.ctx.Done():
			return
		case <-generatorNotify:
		}
	}
}

func (self *multiClientWindow) resize() {
	for {
		update := self.resizeMonitor.NotifyChannel()

		var windowSize WindowSizeSettings
		var fixedWindowType *WindowType
		func() {
			self.stateLock.Lock()
			defer self.stateLock.Unlock()
			if profileWindowType, profileWindowSize, ok := self.performanceProfile.FixedWindow(); ok {
				fixedWindowType = &profileWindowType
				windowSize = profileWindowSize
			} else {
				windowSize = self.settings.WindowSizes[self.windowType]
			}
		}()

		startTime := time.Now()

		warnedClients := []*multiClientChannel{}
		clients := []*multiClientChannel{}
		maxSourceCount := 0
		weights := map[*multiClientChannel]float32{}
		durations := map[*multiClientChannel]time.Duration{}

		removeClient := func(client *multiClientChannel) {
			client.Close()
			removed := false
			func() {
				self.stateLock.Lock()
				defer self.stateLock.Unlock()
				// guard the slot: the client may have been replaced under the same
				// client id by a concurrent expand (see the replace on ping success).
				// in that case do not delete the new client's entry and do not emit
				// Removed for the shared id — the monitor dot now belongs to the
				// new client.
				if self.clients[client.ClientId()] == client {
					delete(self.clients, client.ClientId())
					removed = true
				}
			}()
			if removed {
				self.removeClients(client)
			}
		}
		keepClient := func(client *multiClientChannel, stats *clientWindowStats) {
			clients = append(clients, client)
			maxSourceCount = max(maxSourceCount, stats.sourceCount)
			weights[client] = float32(stats.EffectiveByteCountPerSecond())
			durations[client] = stats.clientDuration
		}
		warnClient := func(client *multiClientChannel, stats *clientWindowStats) {
			warnedClients = append(warnedClients, client)
			maxSourceCount = max(maxSourceCount, stats.sourceCount)
			weights[client] = float32(stats.EffectiveByteCountPerSecond())
			durations[client] = stats.clientDuration
		}

		clientStats := map[*multiClientChannel]*clientWindowStats{}
		for _, client := range self.unorderedClients() {
			if stats, err := client.WindowStats(); err == nil {
				clientStats[client] = stats
			} else if blackholeVerdictErr(err) && !self.verdictRemovalAllowed(startTime) {
				// the storm breaker: this error is a blackhole verdict, and
				// the budget of verdict-driven removals for this window is
				// already spent. Defer -- warn the client out of selection and
				// keep it, so its slot is replaced (it drops out of the size
				// math below) but its teardown does not join the storm. It is
				// re-judged on the next pass, when budget has aged back in.
				// Cancellations and cping failures never reach this branch
				// (blackholeVerdictErr keys on the verdict prefix), so user
				// action and dead-transport cleanup stay immediate.
				previousCause, causeChanged := client.setWarning(true, warnUnhealthy)
				self.logWarnTransition(client, previousCause, causeChanged, nil)
				self.metrics().removalDeferred()
				// the full verdict error rides along as `detail`: a deferred
				// removal may never be executed (the exit recovers, or stays
				// warned), so this is the ONLY place its evidence -- the send
				// and receive counts the verdict was built on -- ever appears
				self.log.Infof("%s\n", relEvent(
					"deferral",
					"exit", client.ClientId(),
					"kind", "verdict_budget",
					"reason", relRemovalReason(err),
					"flows", self.flowCount(client),
					"detail", err,
				))
			} else {
				// the historical format is preserved EXACTLY up to the
				// separator -- this is THE verdict line, the one the owner
				// greps and the one every removal postmortem starts from. The
				// twin adds the compact reason token and the blast radius on
				// the same line, so `grep 'event=removal'` yields the removal
				// census without a regex over the free-text error.
				self.log.Infof(
					"[multi]remove error client [%s] = %s | %s\n",
					client.ClientId(), err,
					relEvent(
						"removal",
						"exit", client.ClientId(),
						"reason", relRemovalReason(err),
						"flows", self.flowCount(client),
					),
				)
				removeClient(client)
			}
		}

		netHealthRanks := map[*multiClientChannel]int{}
		func() {
			orderedClients := slices.Collect(maps.Keys(clientStats))
			slices.SortFunc(orderedClients, func(a *multiClientChannel, b *multiClientChannel) int {
				// descending healthy duration, net healthy duration
				statsA := clientStats[a]
				statsB := clientStats[b]

				if c := statsB.healthyDuration - statsA.healthyDuration; c < 0 {
					return -1
				} else if 0 < c {
					return 1
				}

				if c := statsB.netHealthyDuration - statsA.netHealthyDuration; c < 0 {
					return -1
				} else if 0 < c {
					return 1
				}

				return 0
			})
			for i, client := range orderedClients {
				netHealthRanks[client] = i
			}
		}()

		for client, stats := range clientStats {
			// note for fixed destination size, the destination might still be aliased with multiple clients
			// TODO it's still not clear why one client might stop working occasionally

			// The dedicated watchdog is the sole owner of send-stall conviction.
			// It runs the busy probe plus uplink, shared-fate, and quarantine
			// gates before canceling a client. Re-reading raw sendStalled here
			// used to bypass every one of those acquittals and remove a slow
			// one-bar route on the next resize pass.
			healthy := stats.unhealthyDuration < self.settings.StatsWindowMaxUnhealthyDuration

			printStats := func(status string) {
				effectiveByteCountPerSecond := stats.EffectiveByteCountPerSecond()
				expectedByteCountPerSecond := stats.ExpectedByteCountPerSecond()

				if self.log.V(1).Enabled() {
					self.log.Infof(
						"[multi]%s [%s]: h=%d+%dms/u=%d+%dms effective=%db/s expected=%db/s send=%db sendNack=%db receive=%db\n",
						status,
						client.ClientId(),
						stats.netHealthyDuration/time.Millisecond,
						stats.healthyDuration/time.Millisecond,
						stats.netUnhealthyDuration/time.Millisecond,
						stats.unhealthyDuration/time.Millisecond,
						effectiveByteCountPerSecond,
						expectedByteCountPerSecond,
						stats.sendAckByteCount,
						stats.sendNackByteCount,
						stats.receiveAckByteCount,
					)
				}
			}
			// the top `StatsWindowKeepHealthiestCount` won't be marked as warning or removed
			netHealthRank := netHealthRanks[client]
			remove := max(windowSize.FixedWindowSize, windowSize.KeepHealthiestCount) <= netHealthRank
			// a rank-kept client is still removed once it has been continuously
			// unhealthy past the keep cap: keeping the healthiest rides out
			// transient badness, but a client that never recovers must be
			// replaced so the window re-expands with fresh candidates (and its
			// grid dot is reclaimed) instead of pinning a dead client forever.
			// sustainedUnhealthy is also the escape hatch for the loaded-exit
			// demote below: evidence held continuously this long is no longer
			// a transient classification, so it may remove regardless of flows
			sustainedUnhealthy := 0 < self.settings.StatsWindowKeepUnhealthyDuration &&
				self.settings.StatsWindowKeepUnhealthyDuration <= stats.unhealthyDuration
			if sustainedUnhealthy {
				remove = true
			}
			if healthy {
				// a client after its `removeTime` will be in a permananent warning state as long as it continues to route traffic
				// this prevents new connections from using the client
				if stats.unhealthyDuration < self.settings.StatsWindowWarnUnhealthyDuration {
					if !stats.removeTime.IsZero() && stats.removeTime.Before(startTime) {
						printStats("client drain")
						// a past-lifetime client always warns, regardless of
						// the remove-rank math above. `remove` is a health
						// protection -- the top max(FixedWindowSize,
						// KeepHealthiestCount) ranks are shielded from
						// warn/remove so transient badness in the best
						// clients is ridden out -- and for a
						// FixedWindowSize=1 (speed) window the rank-0 client
						// computes remove=false, so warning with the
						// rank-derived value here left the best speed exit
						// selectable forever:
						// rotation was silently a no-op for exactly the exit
						// it matters most for. Draining is rotation policy,
						// not a health verdict, so the rank shield does not
						// apply. The warning only stops NEW flows from
						// choosing this client (established flows keep
						// running until they finish or the collapse deadline
						// passes), and warnClient counts it in
						// warnedClients, which is what makes the size math
						// below see the hole and expand the replacement the
						// drain hands over to. The health branches below
						// keep using `remove` untouched -- only the lifetime
						// drain is exempt from the rank shield.
						previousCause, causeChanged := client.setWarning(true, warnDraining)
						self.logWarnTransition(client, previousCause, causeChanged, stats)
						warnClient(client, stats)
						// G-3: retirement is a hand-off, not a deadline. The
						// drain's movable (established quic) flows are
						// re-pinned to live replacements NOW -- with their
						// affinity groups kept together by the rebind
						// machinery -- while tcp and everything unplaceable
						// keeps working here until it finishes. Once per
						// drain: the latch, not the pass cadence, decides.
						// Held locks: none at this point in the
						// classification loop, the same contract the
						// removeClient call below relies on.
						if self.clientMigrateFunc != nil && client.markDrainMigrateOnce() {
							self.clientMigrateFunc(client, "drain")
						}
					} else if client.isFamilySwapVictim() {
						// chosen to give up its slot to a v6-capable
						// candidate (see selectFamilySwapVictim): the mark
						// persists so the warning survives every pass until
						// the collapse removes it or the v6 fill starves
						printStats("client family swap")
						previousCause, causeChanged := client.setWarning(true, warnFamilySwap)
						self.logWarnTransition(client, previousCause, causeChanged, stats)
						warnClient(client, stats)
					} else {
						printStats("client ok")
						windowSizeUlimit := self.settings.DefaultUlimit
						if 0 < windowSize.Ulimit {
							windowSizeUlimit = windowSize.Ulimit
						}
						ulimit := 0 < windowSizeUlimit && windowSizeUlimit <= stats.netSourceCount
						// a dial-starved provider -- its own upstream refusing
						// connects -- warns out of new-flow selection so fresh
						// flows avoid it, while its established flows keep
						// working. This is the warning site ONLY: it must never
						// reach the remove decision above, because those flows
						// are the provider's only working asset.
						//
						// only when there is somewhere else to go. warning the
						// sole exit of a fixed window blocks every new flow
						// while helping none of them, and with the client-side
						// inference feeding strikes, one silently-dead polled
						// destination could oscillate a single-exit window
						// between warned and not indefinitely.
						if ulimit {
							previousCause, causeChanged := client.setWarning(true, warnCapacity)
							self.logWarnTransition(client, previousCause, causeChanged, stats)
							warnClient(client, stats)
						} else if 1 < len(clientStats) && client.dialStarved() {
							previousCause, causeChanged := client.setWarning(true, warnStarved)
							self.logWarnTransition(client, previousCause, causeChanged, stats)
							warnClient(client, stats)
						} else if client.isQuarantined() {
							// a quarantined exit (detectBlackhole demoted a
							// soft verdict against it; see setQuarantined) is
							// already out of selection through isWarning(),
							// and the quarantine flag survives the
							// setWarning(false) here by design -- that is the
							// whole reason it is not the shared warning bool.
							// Count it as warned rather than kept so the size
							// math below sees the hole and expands a
							// replacement for the flows it can no longer
							// accept, which is the other half of the demote.
							previousCause, causeChanged := client.setWarning(false, warnNone)
							self.logWarnTransition(client, previousCause, causeChanged, stats)
							warnClient(client, stats)
							// and hand its MOVABLE flows off now, once per
							// episode. A quarantine is receive-silence by
							// construction, so every flow on this exit is
							// already getting nothing -- and holding them
							// here until the sustained-evidence bar matures
							// is the freeze the user actually experiences:
							// on 2026-08-03 a pinned app's 46 flows sat on a
							// benched exit for 55s before it was executed.
							// Established quic moves in about a packet
							// interval; tcp cannot move (split-tcp) and stays
							// to finish or die with the exit, exactly as
							// before. If the bench was a false positive the
							// exit is acquitted and keeps taking new flows --
							// a rebound quic flow is not harmed by having
							// moved. Gated with the drain migration on
							// QuicRebindOnExitLoss.
							if self.clientMigrateFunc != nil && client.markQuarantineMigrateOnce() {
								self.clientMigrateFunc(client, "bench")
							}
						} else if silenceStreak := self.reliabilitySettings().ProbeSilenceWarnStreak; 0 < silenceStreak &&
							silenceStreak <= client.probeSilentStreak() &&
							1 < len(clientStats) {
							// the provider-churn compensation
							// (ProbeSilenceWarnStreak): repeated all-silent
							// probe passes mean the device has very likely left
							// the network, but a flowless corpse's idle stats
							// reach this branch looking healthy, and before this
							// warning it stayed SELECTABLE until real app
							// traffic bound to it and ate the ~10-30s of dead
							// syns that convicts (field capture 2026-08-04).
							// Warn it out of new-flow placement and let the size
							// math backfill a replacement; removal stays
							// traffic-based, so a device that was merely asleep
							// is acquitted by its next answered probe or
							// received byte rather than executed. Gated like
							// starvation on having somewhere else to go: warning
							// the sole exit blocks every new flow while helping
							// none of them.
							previousCause, causeChanged := client.setWarning(true, warnSilent)
							self.logWarnTransition(client, previousCause, causeChanged, stats)
							warnClient(client, stats)
						} else {
							previousCause, causeChanged := client.setWarning(false, warnNone)
							self.logWarnTransition(client, previousCause, causeChanged, stats)
							keepClient(client, stats)
						}
					}
				} else {
					printStats("client health warning")
					previousCause, causeChanged := client.setWarning(remove, warnUnhealthy)
					self.logWarnTransition(client, previousCause, causeChanged, stats)
					warnClient(client, stats)
				}
			} else {
				printStats(fmt.Sprintf("unhealthy client (#%d remove=%t)", netHealthRank, remove))

				if remove {
					// "stats-unhealthy" is a soft classification -- it cannot
					// tell a broken client from an idle one (see the resize doc
					// comment at the top of this file). Executing on it against
					// an exit carrying live flows destroys established
					// connections on ambiguous evidence, so with SoftVerdictDemote
					// on such an exit is demoted instead: warned out of new-flow
					// selection and kept, its flows running, until it is either
					// flowless or has been continuously unhealthy past the
					// sustained bound (sustainedUnhealthy above). The two hard
					// sustainedUnhealthy is the same evidence held for a full
					// minute. A send-stall conviction never reaches this branch:
					// the watchdog cancels it after all hard-verdict gates. The demote
					// deliberately leaves the rank math above untouched -- what
					// changes is only what a remove verdict against a loaded
					// exit does.
					if self.reliabilitySettings().SoftVerdictDemote &&
						!sustainedUnhealthy &&
						0 < self.flowCount(client) {
						previousCause, causeChanged := client.setWarning(true, warnUnhealthy)
						self.logWarnTransition(client, previousCause, causeChanged, stats)
						warnClient(client, stats)
						self.log.Infof(
							"[multi]unhealthy removal demoted to warning [%s]: carrying flows, evidence not sustained\n",
							client.ClientId(),
						)
					} else if !self.verdictRemovalAllowed(startTime) {
						// the storm breaker (see verdictRemovalAllowed): the
						// verdict-removal budget is spent, so defer -- warn and
						// keep, re-judge next pass
						previousCause, causeChanged := client.setWarning(true, warnUnhealthy)
						self.logWarnTransition(client, previousCause, causeChanged, stats)
						warnClient(client, stats)
						self.metrics().removalDeferred()
						self.log.Infof("%s\n", relEvent(
							"deferral",
							"exit", client.ClientId(),
							"kind", "verdict_budget",
							"reason", "unhealthy",
							"flows", self.flowCount(client),
						))
					} else {
						previousCause, causeChanged := client.setWarning(true, warnUnhealthy)
						self.logWarnTransition(client, previousCause, causeChanged, stats)
						removeClient(client)
					}
				} else {
					previousCause, causeChanged := client.setWarning(false, warnNone)
					self.logWarnTransition(client, previousCause, causeChanged, stats)
					warnClient(client, stats)
				}
			}
		}

		collapseLowestWeighted := func(targetWindowSize int) {

			n := (len(warnedClients) + len(clients)) - targetWindowSize

			collapse := func(cs []*multiClientChannel) {
				if 0 < n && 0 < len(cs) {
					m := min(len(cs), n)
					if 0 < m {
						slices.SortFunc(cs, func(a *multiClientChannel, b *multiClientChannel) int {
							// dualstack exits are kept ahead of single-family
							// ones (IPV6.md B2); the tail is what is shed
							if c := collapseFamilyOrder(a, b); c != 0 {
								return c
							}
							// descending weight
							aWeight := weights[a]
							bWeight := weights[b]
							if aWeight < bWeight {
								return 1
							} else if bWeight < aWeight {
								return -1
							}
							aDuration := durations[a]
							bDuration := durations[b]
							if aDuration < bDuration {
								return 1
							} else if bDuration < aDuration {
								return -1
							}
							return 0
						})

						for _, client := range cs[len(cs)-m : len(cs)] {
							if self.settings.StatsWindowGraceperiod <= durations[client] && weights[client] <= 0 {
								// capacity collapse must not destroy live
								// flows: a zero 30s effective weight cannot
								// tell an idle-but-open session (an ssh
								// window between keystrokes) from a dead
								// client, so the weight alone is not license
								// to remove. The gate (see
								// collapseRemovalAllowed) admits the removal
								// when the client is flowless -- the normal
								// path -- or past the hard collapse deadline,
								// the escape that keeps one immortal flow
								// from pinning an exit forever. Called here
								// with no lock held, matching removeClient's
								// own idiom: the flow count read takes the
								// parent stateLock inside.
								if self.collapseRemovalAllowed(client, durations[client]) {
									removeClient(client)
								} else {
									// collapse deferred: the capacity collapse
									// would have destroyed live flows
									self.log.Infof("%s\n", relEvent(
										"collapse_defer",
										"exit", client.ClientId(),
										"reason", "carrying_flows",
										"flows", self.flowCount(client),
									))
								}
							}
						}
						n -= m
					}
				}
			}
			collapse(warnedClients)
			collapse(clients)
		}

		p2pOnlyWindowSize := 0
		ipv4CapableCount := 0
		ipv6CapableCount := 0
		for _, client := range clients {
			if client.IsP2pOnly() {
				p2pOnlyWindowSize += 1
			}
			family := client.IpFamily()
			if family.SupportsIpv4() {
				ipv4CapableCount += 1
			}
			if family.SupportsIpv6() {
				ipv6CapableCount += 1
			}
		}
		// the soft v6-capable minimum (IPV6.md B1) applies to auto-sized
		// windows only: a fixed profile, a fixed size or a fixed destination
		// pins the membership, and a soft minimum has no slot to claim there.
		// The shortfall is CONFIRMED only once the main fill has produced
		// exits and none carries v6 -- during formation the dualstack-first
		// fill is still the way to get one, so nothing extra is asked for.
		_, familyFixedDestination := self.generator.FixedDestinationSize()
		ipv6Shortfall := ipv6CapableShortfall(
			windowSize,
			fixedWindowType != nil,
			familyFixedDestination,
			len(clients),
			ipv6CapableCount,
		)

		var windowSizeMin int
		var targetWindowSize int
		fixedDestinationSize, fixedDestination := self.generator.FixedDestinationSize()
		if fixedDestination {
			targetWindowSize = fixedDestinationSize
			windowSizeMin = targetWindowSize
		} else if 0 < windowSize.FixedWindowSize {
			if fixedWindowType == nil || self.windowType == *fixedWindowType {
				targetWindowSize = windowSize.FixedWindowSize
			} else {
				// not the active window, disable resize
				targetWindowSize = 0
			}
			windowSizeMin = targetWindowSize
		} else {
			// scale the number of reconnects
			reconnectScale := self.settings.DefaultReconnectScale
			if 0 < windowSize.WindowSizeReconnectScale {
				reconnectScale = windowSize.WindowSizeReconnectScale
			}
			targetWindowSize = int(math.Ceil(float64(maxSourceCount) * reconnectScale))

			if n := windowSize.WindowSizeMinP2pOnly - p2pOnlyWindowSize; 0 < n {
				targetWindowSize += n
			}
			// the soft v6-capable minimum raises the target by its
			// shortfall exactly like the p2p-only minimum above, and like
			// it is bounded by WindowSizeMax below; windowSizeMin is untouched
			if 0 < ipv6Shortfall {
				targetWindowSize += ipv6Shortfall
			}

			targetWindowSize = min(
				windowSize.WindowSizeMax,
				max(
					windowSize.WindowSizeMin,
					targetWindowSize,
				),
			)
			windowSizeMin = windowSize.WindowSizeMin
		}

		// the standing reserve: hold one spare exit beyond the computed
		// target, bounded by WindowSizeHardMax, so a failed or draining
		// exit's replacement is already dialed and evaluated before it is
		// needed. Measured motivation: failover backfill took ~45s because
		// replacement only started after a loss -- a resize tick to notice
		// the hole plus a generator round trip, dial, and evaluation ping all
		// sat between the failure and the first packet over the replacement.
		// A distinct step from the demand math above on purpose: the target
		// answers "how many exits does the traffic need", the reserve answers
		// "how many failures can be absorbed without a connect in the
		// recovery path". windowSizeMin is deliberately untouched -- the
		// spare must never make the window read as unsatisfied.
		targetWindowSize = standingReserveTarget(
			targetWindowSize,
			windowSize.WindowSizeHardMax,
			self.reliabilitySettings().StandingReserve,
			fixedDestination,
		)

		// publish the shortfall for the enumerator (it asks for v6-capable
		// candidates first while one is open) and, at capacity, make room:
		// with WindowSizeMax == WindowSizeMin the raise above cannot grow the
		// window, so a v6-capable candidate can only enter by taking the slot
		// of an exit that cannot carry v6. See selectFamilySwapVictim.
		self.setIpv6Shortfall(ipv6Shortfall)
		withinCeiling := windowSize.WindowSizeHardMax <= 0 ||
			len(clients)+len(warnedClients) <= windowSize.WindowSizeHardMax
		if 0 < ipv6Shortfall && targetWindowSize <= len(clients) &&
			!self.ipv6Starved(startTime) && !self.hasFamilySwapVictim() {
			victim, flowless := self.selectFamilySwapVictim(clients, weights)
			// a strict window cannot admit past its ceiling while a drained
			// victim is retained, so it only ever swaps a flowless one
			swappable := victim != nil && (flowless || !self.settings.StrictWindowSizeHardMax)
			if swappable && !self.hasIpv6CandidateReady() {
				// ask before giving anything up: the enumerator probes for
				// one v6-capable candidate and wakes this pass when it has
				// one; a region with none costs nothing but the request
				self.requestIpv6Probe()
			} else if swappable {
				if self.settings.StrictWindowSizeHardMax {
					// strict: the ceiling is ownership, so the flowless
					// victim leaves first and the candidate takes its slot
					// (the hard minimum dips for one evaluation)
					self.log.Infof("%s\n", relEvent(
						"family_swap",
						"exit", victim.ClientId(),
						"action", "remove_idle",
						"family", string(victim.IpFamily()),
					))
					removeClient(victim)
					clients = slices.DeleteFunc(clients, func(c *multiClientChannel) bool { return c == victim })
				} else {
					// admit one over target and let the capacity collapse
					// below shed a single-family exit once the candidate is
					// in (collapseFamilyOrder), so the hard minimum never
					// dips during the swap
					targetWindowSize = len(clients) + 1
				}
				if !flowless {
					// a loaded victim is drain-warned: new flows avoid it,
					// its movable flows are handed off now, and the collapse
					// removes it once idle
					victim.markFamilySwapVictim()
					previousCause, causeChanged := victim.setWarning(true, warnFamilySwap)
					self.logWarnTransition(victim, previousCause, causeChanged, clientStats[victim])
					self.log.Infof("%s\n", relEvent(
						"family_swap",
						"exit", victim.ClientId(),
						"action", "drain",
						"family", string(victim.IpFamily()),
						"flows", self.flowCount(victim),
					))
					clients = slices.DeleteFunc(clients, func(c *multiClientChannel) bool { return c == victim })
					warnedClients = append(warnedClients, victim)
					if self.clientMigrateFunc != nil {
						self.clientMigrateFunc(victim, "family-swap")
					}
				}
			}
		} else if ipv6Shortfall == 0 && withinCeiling && self.hasFamilySwapVictim() {
			// the shortfall closed and the window is within its ceiling: a
			// victim still draining returns to service on the next pass
			for _, client := range warnedClients {
				client.clearFamilySwapVictim()
			}
		}
		// the hard minimum ignores a window whose only exits are v6-only;
		// see hardMinimumCount
		hardMinimumCount := hardMinimumCount(clients, fixedDestination)

		// the outcome clock arms the first time this window actually tries to
		// form (see watchOutcome); a disabled window (target 0) never arms
		if 0 < targetWindowSize {
			self.armOutcome()
		}
		minSatisfied := func(clientCount int) bool {
			return windowMinSatisfied(
				windowSizeMin,
				clientCount,
				len(warnedClients),
				fixedDestination,
				self.settings.StrictWindowSizeHardMax,
				windowSize.WindowSizeHardMax,
			)
		}

		addedCount := 0
		if len(clients) < targetWindowSize {
			// expand
			neededCount := targetWindowSize - len(clients)
			plannedAdmitCount, _ := multiClientExpandPlan(
				neededCount,
				self.reliabilitySettings().EvaluationPoolMultiple,
				fixedDestination,
			)
			self.monitor.AddWindowExpandEvent(
				minSatisfied(hardMinimumCount),
				targetWindowSize+len(warnedClients),
				self.ipv6Available(),
			)
			addedCount = self.expand(
				windowSize,
				len(clients),
				p2pOnlyWindowSize,
				targetWindowSize,
				windowSizeMin,
				neededCount,
				ipv6Shortfall,
			)
			if self.log.V(1).Enabled() {
				self.log.Infof(
					"[multi]window expand +%d needed=%d %d->%d (+%d)\n",
					plannedAdmitCount,
					neededCount,
					len(clients),
					targetWindowSize,
					addedCount,
				)
			}
		}
		if 0 < windowSize.WindowSizeHardMax && windowSize.WindowSizeHardMax < len(clients)+len(warnedClients)+addedCount {
			self.monitor.AddWindowExpandEvent(
				minSatisfied(self.hardMinimumClientCount(fixedDestination)),
				windowSize.WindowSizeHardMax,
				self.ipv6Available(),
			)
			collapseLowestWeighted(max(0, windowSize.WindowSizeHardMax-addedCount))
			if self.log.V(1).Enabled() {
				self.log.Infof("[multi]window collapse -%d ->%d\n", (len(clients)+len(warnedClients)+addedCount)-windowSize.WindowSizeHardMax, windowSize.WindowSizeHardMax)
			}
		} else {
			self.monitor.AddWindowExpandEvent(
				minSatisfied(self.hardMinimumClientCount(fixedDestination)),
				len(clients)+len(warnedClients)+addedCount,
				self.ipv6Available(),
			)
		}

		timeout := self.settings.WindowResizeTimeout - time.Now().Sub(startTime)
		if timeout <= 0 {
			select {
			case <-self.ctx.Done():
				return
			default:
			}
		} else {
			select {
			case <-self.ctx.Done():
				return
			case <-update:
			case <-time.After(timeout):
			}
		}
	}
}

// expandEvaluatedCandidate is one expand candidate that passed its evaluation
// ping and awaits admission (or polite cancellation): the pooling state
// between "evaluated" and "in the window". By construction it carries no
// flows -- selection only ever sees installed window clients.
type expandEvaluatedCandidate struct {
	client *multiClientChannel
	args   *multiClientChannelArgs
}

// multiClientExpandStepMax bounds every object-producing expansion pass, not
// just the number ultimately admitted. Evaluated surplus candidates own full
// Client / PlatformTransport graphs too, so capping only admissions would
// still permit the sharp memory jump this limit is meant to prevent.
const multiClientExpandStepMax = 4

func multiClientExpandPlan(
	neededCount int,
	evaluationPoolMultiple int,
	fixedDestination bool,
) (admitCount int, requestCount int) {
	if neededCount <= 0 {
		return 0, 0
	}
	evaluationPoolMultiple = min(
		multiClientExpandStepMax,
		max(1, evaluationPoolMultiple),
	)
	if fixedDestination {
		evaluationPoolMultiple = 1
	}
	requestCount = min(
		multiClientExpandStepMax,
		min(neededCount, multiClientExpandStepMax)*evaluationPoolMultiple,
	)
	admitCount = min(neededCount, requestCount)
	return
}

func (self *multiClientWindow) expand(
	windowSize WindowSizeSettings,
	currentWindowSize int,
	currentP2pOnlyWindowSize int,
	targetWindowSize int,
	windowSizeMin int,
	n int,
	// ipv6Shortfall is the window's open v6-capable shortfall (IPV6.md B2):
	// while positive and unfilled, an evaluated candidate that can carry v6
	// is admitted ahead of every other, as p2p-only candidates are preferred
	ipv6Shortfall int,
) (returnPingSuccess int) {
	mutex := sync.Mutex{}
	pendingPingDones := []context.Context{}
	added := 0
	addedP2pOnly := 0
	admittedIpv6Capable := 0
	pingSuccess := 0

	defer func() {
		mutex.Lock()
		defer mutex.Unlock()

		returnPingSuccess = pingSuccess
	}()

	// Aggressive pooling (EvaluationPoolMultiple): request and ping-evaluate a
	// multiple of the candidates this pass actually needs, admit only the
	// needed count -- preferring qualified providers -- and politely cancel
	// the evaluated surplus.
	//
	// The multiple applies HERE, to the candidate-request count. Both requests
	// and admissions are capped by multiClientExpandStepMax: an evaluated
	// candidate already owns the same memory-heavy client and transport graph,
	// even if it is later discarded as surplus. The terminal cleanup below
	// cancels unresolved pings before the pass returns, so this private budget
	// cannot overlap the next resize pass.
	//
	// Fixed-destination generators skip the multiple for the same reason they
	// skip the standing reserve: their destination set cannot produce surplus
	// candidates, and asking would only stall each pass against its args
	// timeout.
	_, fixedDestination := self.generator.FixedDestinationSize()
	admitBudget, requestCount := multiClientExpandPlan(
		n,
		self.reliabilitySettings().EvaluationPoolMultiple,
		fixedDestination,
	)

	admitted := 0
	pending := []*expandEvaluatedCandidate{}
	type pendingPingFailure struct {
		fail            func() bool
		evaluationCtx   context.Context
		startedAt       time.Time
		effectiveBudget time.Duration
	}
	pendingPingFailures := []pendingPingFailure{}
	expandEnded := false
	passDeadlineExpired := false

	// admitCandidate installs one evaluated candidate into the window, running
	// the same-clientId replacement gate exactly as the pre-pooling install
	// did. Returns whether it installed; a declined replacement consumes the
	// candidate (cancelled, args returned, dot restored to the live old
	// client) without consuming admit budget, so the budget slot falls to the
	// next evaluated candidate.
	//
	// must be called with mutex (replacementAllowed reads the flow count,
	// which takes the parent stateLock -- the same nesting the pre-pooling
	// callback already had)
	admitCandidate := func(candidate *expandEvaluatedCandidate) bool {
		client := candidate.client
		args := candidate.args
		clientId := client.ClientId()

		// same-clientId replacement gate: the generator can re-hand an
		// identity already in the window (a destination-aware generator reuses
		// a persisted identity per destination). Replacing used to
		// unconditionally cancel the old channel, which destroyed its live
		// flows for no failure at all. The existing channel is read under the
		// window stateLock, but the decision (replacementAllowed) runs with
		// the lock released: the flow count read inside takes the parent
		// stateLock, which must never nest under a window or channel lock.
		var existingClient *multiClientChannel
		func() {
			self.stateLock.Lock()
			defer self.stateLock.Unlock()
			existingClient = self.clients[clientId]
		}()
		if !self.replacementAllowed(existingClient) {
			// decline: keep the old, flow-carrying channel and discard the new
			// one. Deliberately NOT fail(): fail emits EvaluationFailed, a
			// terminal monitor state that would delete the dot of the LIVE old
			// client sharing this id. Re-emitting Added restores the dot to
			// the truth: the id remains in the window, routing, via the old
			// channel.
			self.log.Infof("%s\n", relEvent(
				"expand_decline",
				"exit", clientId,
				"reason", "carrying_flows",
			))
			// The constructed channel owns both the Client and its generator
			// args. Canceling it runs the newMultiClientChannel cleanup, which
			// calls RemoveClientWithArgs only after Client/OOB work is joined.
			// Calling RemoveClientArgs here would revoke the derived JWT first
			// and turn the channel's final contract closes into 401s.
			client.Cancel()
			self.monitor.AddProviderEventWithExtenderIps(
				args.ClientId,
				ProviderStateAdded,
				args.Destination.Tail(),
				args.Location,
				args.IpFamily,
				// the dot belongs to the live old client, so its extenders are
				// what the re-emitted event must carry
				self.clientExtenderIps(existingClient),
			)
			return false
		}
		if !self.strictWindowAdmissionAllowed(clientId, windowSize) {
			self.log.Infof("%s\n", relEvent(
				"expand_decline",
				"exit", clientId,
				"reason", "strict_hard_max",
			))
			// The candidate was fully constructed but never made visible to
			// selection, so cancellation can return all client/transport
			// ownership without disturbing an existing flow. The channel owns
			// its generator args at this point; do not return them separately.
			client.Cancel()
			self.monitor.AddProviderEvent(args.ClientId, ProviderStateNotAdded, args.Destination.Tail(), args.Location, args.IpFamily)
			return false
		}

		self.log.V(1).Infof("[multi]expand new client\n")

		var replacedClient *multiClientChannel
		func() {
			self.stateLock.Lock()
			defer self.stateLock.Unlock()
			// re-read under the lock: the slot is installed against whatever
			// is there NOW, so a concurrent remove/replace between the gate
			// above and here can never leak an uncancelled channel
			replacedClient = self.clients[clientId]
			self.clients[clientId] = client
		}()
		if client.IpFamily().SupportsIpv6() {
			admittedIpv6Capable += 1
			self.noteIpv6CandidateAdmitted()
		}
		if replacedClient != nil {
			// the replaced client is stored under the same client id as the
			// new client, so they share one monitor dot. Cancel it without
			// emitting Removed — a Removed here would terminal-arm the dot of
			// the NEW live client (Added below), and the ui would reap it
			// while the client is still routing.
			replacedClient.Cancel()
		}
		self.monitor.AddProviderEventWithExtenderIps(
			args.ClientId,
			ProviderStateAdded,
			args.Destination.Tail(),
			args.Location,
			args.IpFamily,
			self.clientExtenderIps(client),
		)
		// the outcome watchdog stands down: this window has proven it can
		// install a provider (and a latched failed state is cleared)
		self.noteClientAdded(client)
		// K1: the extenders carrying this exit ride its dot from here on
		go HandleError(func() {
			self.watchExtenderIps(client)
		})
		// reap promptly when the client dies (the continuous ping or blackhole
		// detection cancels the channel): wake the resize loop instead of
		// waiting for its next tick
		go HandleError(func() {
			select {
			case <-self.ctx.Done():
			case <-client.Done():
				self.resizeMonitor.NotifyAll()
			}
		})
		return true
	}

	// cancelCandidate politely discards an evaluated-but-unadmitted candidate:
	// it passed its ping, but the pass needed fewer exits than it evaluated.
	// The channel carries no flows by construction -- it was never installed
	// in the window, so selection never saw it and nothing was ever pinned to
	// it -- which is what makes the cancel free. NotAdded is the monitor's
	// terminal state for exactly this outcome (evaluated, healthy, not
	// chosen), distinct from EvaluationFailed.
	//
	// must be called with mutex
	cancelCandidate := func(candidate *expandEvaluatedCandidate) {
		// A successfully constructed channel owns its generator args. Its
		// cancellation cleanup returns them through RemoveClientWithArgs after
		// the Client/OOB join; do not also revoke them directly here.
		candidate.client.Cancel()
		self.monitor.AddProviderEvent(candidate.args.ClientId, ProviderStateNotAdded, candidate.args.Destination.Tail(), candidate.args.Location, candidate.args.IpFamily)
	}

	// admitPending admits from the evaluated pool while budget remains. Every
	// admission is routed through poolAdmitOrder -- the single chooser -- so
	// "prefer qualified" is a property of one pure function rather than of
	// call-site discipline. In the common case a ping success finds spare
	// budget and an otherwise-empty pool and is admitted immediately, which
	// keeps first-connect latency identical to the pre-pooling path; the
	// preference decides whenever more than one candidate is pending at an
	// admit moment, and the qualification lookup is best-effort by design (a
	// cold start has nothing qualified yet -- the effectiveTier demerit does
	// the ongoing steering after admission).
	//
	// must be called with mutex
	admitPending := func() {
		for admitted < admitBudget && 0 < len(pending) {
			qualified := make([]bool, len(pending))
			for i, candidate := range pending {
				// the lookup takes the parent stateLock inside; nil (bare
				// windows) reads as nothing qualified -> plain arrival order
				qualified[i] = self.providerQualifiedFunc != nil &&
					self.providerQualifiedFunc(candidate.args.Destination)
			}
			// the open family shortfall outranks qualification: the one
			// v6-capable candidate this pass asked for must land before
			// the fill closes the slot with another single-family exit
			var pick int
			if 0 < ipv6Shortfall && admittedIpv6Capable == 0 {
				closesShortfall := make([]bool, len(pending))
				for i, candidate := range pending {
					closesShortfall[i] = candidate.client.IpFamily().SupportsIpv6()
				}
				pick = poolAdmitOrderWithShortfall(closesShortfall, qualified, 1)[0]
			} else {
				pick = poolAdmitOrder(qualified, 1)[0]
			}
			candidate := pending[pick]
			pending = append(pending[:pick], pending[pick+1:]...)
			if admitCandidate(candidate) {
				admitted += 1
				pingSuccess += 1
			}
		}
	}

	// the surplus MUST be released on every exit path -- including the expand
	// timeout returns mid-loop -- so the cleanup is a defer, not a tail.
	// Registered after the returnPingSuccess defer above (LIFO), so an
	// admission completed here still counts in the returned total.
	defer func() {
		mutex.Lock()
		expandEnded = true
		admitPending()
		for _, candidate := range pending {
			cancelCandidate(candidate)
		}
		pending = nil
		// The pass deadline is an ownership boundary, not merely a return to
		// resize. A ping callback left alive here used to retain this pass's
		// private admit budget; overlapping timed-out passes could therefore
		// each install one candidate and grow a fixed-size-one window to six.
		// Failing every unresolved evaluation cancels its Client and returns its
		// generator args before a later resize pass can begin.
		deadlineFailureCount := 0
		var effectiveBudgetMin time.Duration
		var observedBudgetMax time.Duration
		for _, pendingFailure := range pendingPingFailures {
			deadlineOwned := evaluationBudgetDeadlineOwned(
				passDeadlineExpired,
				self.ctx,
				pendingFailure.evaluationCtx,
			)
			if !pendingFailure.fail() || !deadlineOwned {
				continue
			}
			deadlineFailureCount += 1
			if effectiveBudgetMin == 0 || pendingFailure.effectiveBudget < effectiveBudgetMin {
				effectiveBudgetMin = pendingFailure.effectiveBudget
			}
			observedBudget := time.Since(pendingFailure.startedAt)
			if observedBudgetMax < observedBudget {
				observedBudgetMax = observedBudget
			}
		}
		mutex.Unlock()

		self.recordEvaluationBudgetExhausted(
			deadlineFailureCount,
			effectiveBudgetMin,
			observedBudgetMax,
		)
	}()

	endTime := time.Now().Add(self.settings.WindowExpandTimeout)

	for i := 0; i < requestCount; i += 1 {
		timeout := endTime.Sub(time.Now())
		if timeout < 0 {
			passDeadlineExpired = true
			self.log.V(1).Infof("[multi]expand window timeout\n")
			return
		}

		self.generatorMonitor.NotifyAll()
		select {
		case <-self.ctx.Done():
			return
		case <-self.finishExpandPassForTest:
			return
		case <-self.expireExpandPassForTest:
			passDeadlineExpired = true
			return
		// case <- update:
		//     // continue
		case args, ok := <-self.clientChannelArgs:
			if !ok {
				return
			}
			// randomly set to p2p only to meet the minimum requirement
			if !args.MultiClientGeneratorClientArgs.P2pOnly {
				var a int
				var b int
				func() {
					mutex.Lock()
					defer mutex.Unlock()

					a = max(windowSize.WindowSizeMin-(currentWindowSize+added), 0)
					b = max(windowSize.WindowSizeMinP2pOnly-(currentP2pOnlyWindowSize+addedP2pOnly), 0)
				}()
				var p2pOnlyP float32
				if a+b == 0 {
					p2pOnlyP = 0
				} else {
					p2pOnlyP = float32(b) / float32(a+b)
				}
				args.MultiClientGeneratorClientArgs.P2pOnly = mathrand.Float32() < p2pOnlyP
			}

			// the batch delivery seam rides the args so the channel
			// constructor signature stays put (nil falls back per-packet)
			args.ReceivePackets = self.clientReceivePacketsCallback
			args.NetworkPeerDestination = self.networkPeerDestination
			args.contractStatus = self.contractStatusFromClient
			// the evaluation epoch, not the window ctx: identical between
			// rebuilds, and what lets the outcome rebuild fail every
			// in-flight candidate fast (see evalEpochContext)
			evaluationCtx := self.evalEpochContext()
			client, err := newMultiClientChannel(
				evaluationCtx,
				args,
				self.generator,
				self.clientReceivePacketCallback,
				self.dialFailureCallback,
				self.ingressSecurityPolicy,
				self.contractStatus,
				self.contractStats,
				self.peerIdentityChanged,
				self.performanceProfile,
				self.settings,
				self.reliabilitySettings,
				self.uplinkGateFunc,
				self.reliabilityMetricsFunc,
				self.flowCountFunc,
				func() { self.resizeMonitor.NotifyAll() },
				// the bench hand-off routes through the window's own
				// migrate seam, so a window with no parent wiring (bare
				// fixtures) simply never migrates
				func(client *multiClientChannel) {
					if self.clientMigrateFunc != nil {
						self.clientMigrateFunc(client, "bench")
					}
				},
				self.providerQualifiedFunc,
				self.receivingSiblingsFunc,
				self.qualificationRefreshFunc,
			)
			if err != nil {
				providerFailure := self.recordChannelCreationFailure(evaluationCtx, args, err)
				self.generator.RemoveClientArgs(&args.MultiClientGeneratorClientArgs)
				if providerFailure {
					self.monitor.AddProviderEvent(args.ClientId, ProviderStateEvaluationFailed, args.Destination.Tail(), args.Location, args.IpFamily)
				}
			} else {

				// send an initial ping on the client and let the ack timeout close it
				pingDone, pingCancel := context.WithCancel(self.ctx)
				pendingPingDones = append(pendingPingDones, pingDone)

				// must be called with mutex
				fail := func() bool {
					select {
					case <-pingDone.Done():
						// already done
						return false
					default:
					}

					pingCancel()
					// The channel cleanup owns the generator args and preserves the
					// derived identity through its final contract-close controls.
					client.Cancel()
					self.monitor.AddProviderEvent(args.ClientId, ProviderStateEvaluationFailed, args.Destination.Tail(), args.Location, args.IpFamily)
					if 0 < ipv6Shortfall && args.IpFamily.SupportsIpv6() {
						self.noteIpv6CandidateFailed()
					}
					return true
				}
				pingStartedAt := time.Now()
				effectiveBudget := min(self.settings.PingTimeout, max(time.Duration(0), endTime.Sub(pingStartedAt)))
				pendingPingFailures = append(pendingPingFailures, pendingPingFailure{
					fail:            fail,
					evaluationCtx:   evaluationCtx,
					startedAt:       pingStartedAt,
					effectiveBudget: effectiveBudget,
				})

				// EncryptionCapabilityPrefilter: under EncryptionModeRequired
				// a candidate that has never published a client identity key
				// can never complete the identity-verified handshake — fail it
				// as soon as the platform says so instead of letting the ping
				// wait out PingTimeout against it (the ping itself is
				// entry-gated on the cipher under Required, so against such a
				// peer it can only time out). Runs concurrently with the ping,
				// parented on pingDone so a resolved ping moots the fetch.
				if self.settings.EncryptionCapabilityPrefilter {
					if fetch, mode := client.EncryptionCapabilityFetcher(); fetch != nil && mode == EncryptionModeRequired {
						go HandleError(func() {
							fetchCtx, fetchCancel := context.WithTimeout(pingDone, self.settings.PingTimeout)
							defer fetchCancel()
							publicKey, fetchErr := fetch(fetchCtx)
							if rejectCandidateMissingEncryptionKey(mode, publicKey, fetchErr) {
								if self.log.V(1).Enabled() {
									self.log.Infof(
										"[multi]expand prefilter: %s has no published identity key — cannot seal under required encryption, failing candidate\n",
										args.Destination.Tail(),
									)
								}
								mutex.Lock()
								defer mutex.Unlock()
								fail()
							}
						}, client.Cancel)
					}
				}

				go HandleError(func() {
					mutex.Lock()
					defer mutex.Unlock()

					select {
					case <-pingDone.Done():
						// already done
						return
					default:
					}

					added += 1
					if client.IsP2pOnly() {
						addedP2pOnly += 1
					}

					self.monitor.AddProviderEventWithExtenderIps(
						args.ClientId,
						ProviderStateInEvaluation,
						args.Destination.Tail(),
						args.Location,
						args.IpFamily,
						self.clientExtenderIps(client),
					)

					success, err := client.SendDetailedMessage(
						&protocol.IpPing{},
						self.settings.PingWriteTimeout,
						func(err error) {
							if self.beforeExpandPingResultForTest != nil {
								self.beforeExpandPingResultForTest()
							}
							if self.afterExpandPingResultForTest != nil {
								defer self.afterExpandPingResultForTest()
							}
							mutex.Lock()
							defer mutex.Unlock()

							select {
							case <-pingDone.Done():
								// already done
								return
							default:
							}

							if err == nil {
								// evaluated: the candidate answered its ping.
								// Admission (with the same-clientId
								// replacement gate) now runs through the
								// pooling admit path -- see admitCandidate /
								// admitPending above. Cancelling pingDone
								// below is what releases the pending-ping
								// wait, so nothing wedges on either branch.
								candidate := &expandEvaluatedCandidate{
									client: client,
									args:   args,
								}
								if expandEnded {
									// A returned pass owns no admission budget. This
									// branch is reachable only when its callback crossed
									// the terminal mutex boundary before cleanup canceled
									// the unresolved ping.
									cancelCandidate(candidate)
								} else {
									pending = append(pending, candidate)
									admitPending()
								}
								pingCancel()
							} else {
								self.recordEvaluationPingFailure(evaluationCtx, args, err)
								fail()
							}
						},
					)
					if err != nil {
						if !isEvaluationContextCancellation(evaluationCtx, err) {
							self.log.Infof("[multi]create client ping error = %s\n", err)
							self.recordEvaluationFailure(windowFailureProvider, err)
						}
						fail()
					} else if !success {
						fail()
					} else {
						// async wait for the ping
						go HandleError(func() {
							select {
							case <-pingDone.Done():
							case <-evaluationCtx.Done():
								func() {
									mutex.Lock()
									defer mutex.Unlock()
									fail()
								}()
							case <-time.After(self.settings.PingTimeout):
								func() {
									mutex.Lock()
									defer mutex.Unlock()
									if evaluationCtx.Err() != nil {
										fail()
										return
									}
									// unconditional (V0), was V(2): the unanswered
									// evaluation ping is THE dominant transition of
									// the field hang, and it logged nothing
									if ok, suppressed := self.pingFailThrottle.Allow(time.Now()); ok {
										self.log.Infof("[multi]evaluation ping timeout [%s]%s\n",
											args.ClientId, suppressedSuffix(suppressed))
									}
									self.recordEvaluationFailure(windowFailureProvider, nil)
									fail()
								}()
							}
						}, client.Cancel)
					}
				})
			}
		case <-time.After(timeout):
			passDeadlineExpired = true
			self.log.V(2).Infof("[multi]expand window timeout waiting for args\n")
			return
		}
	}

	// wait for pending pings
	for _, pingDone := range pendingPingDones {
		timeout := endTime.Sub(time.Now())
		if timeout <= 0 {
			passDeadlineExpired = true
			return
		}

		select {
		case <-self.ctx.Done():
			return
		case <-self.finishExpandPassForTest:
			return
		case <-self.expireExpandPassForTest:
			passDeadlineExpired = true
			return
		case <-pingDone.Done():
		case <-time.After(timeout):
			passDeadlineExpired = true
			return
		}
	}

	return
}

func evaluationBudgetDeadlineOwned(
	passDeadlineExpired bool,
	windowCtx context.Context,
	evaluationCtx context.Context,
) bool {
	return passDeadlineExpired &&
		windowCtx.Err() == nil &&
		evaluationCtx.Err() == nil
}

// recordEvaluationBudgetExhausted owns only candidates still unresolved when
// the expansion pass reaches its natural deadline. Pass cleanup remains the
// terminal owner, so a delayed ping callback cannot admit into a later pass;
// this method restores the provider-failure evidence that cleanup previously
// erased when WindowExpandTimeout was shorter than PingTimeout. Local epoch or
// window cancellation is filtered by the caller before a candidate is counted.
func (self *multiClientWindow) recordEvaluationBudgetExhausted(
	candidateCount int,
	effectiveBudgetMin time.Duration,
	observedBudgetMax time.Duration,
) {
	if candidateCount <= 0 {
		return
	}

	allowLog := true
	var suppressed int64
	if self.budgetFailThrottle != nil {
		allowLog, suppressed = self.budgetFailThrottle.Allow(time.Now())
	}
	if allowLog {
		loggerOrDefault(self.log).Infof("%s\n", relEvent(
			"evaluation_budget_exhausted",
			"window", self.windowName(),
			"candidates", candidateCount,
			"effective_min", effectiveBudgetMin,
			"observed_max", observedBudgetMax,
			"ping_timeout", self.settings.PingTimeout,
			"expand_timeout", self.settings.WindowExpandTimeout,
			"suppressed", suppressed,
		))
	}
	for range candidateCount {
		self.recordEvaluationFailure(windowFailureProvider, nil)
	}
}

// isEvaluationContextCancellation distinguishes an owning evaluation-epoch
// rebuild or window retirement from an identically worded provider error. The
// context must be done and the returned error must match that exact context
// outcome; a live-context context.Canceled remains provider evidence.
func isEvaluationContextCancellation(evaluationCtx context.Context, err error) bool {
	contextErr := evaluationCtx.Err()
	return contextErr != nil && errors.Is(err, contextErr)
}

// recordEvaluationPingFailure keeps the provider-failure reason free of
// cancellations caused by rebuildWindow replacing the evaluation epoch. The
// caller still performs ordinary candidate cleanup on both branches.
func (self *multiClientWindow) recordEvaluationPingFailure(
	evaluationCtx context.Context,
	args *multiClientChannelArgs,
	err error,
) bool {
	if isEvaluationContextCancellation(evaluationCtx, err) {
		return false
	}
	// unconditional (V0), was V(1): a ping-ack error is an
	// evaluation-failure transition, and those were invisible in the field.
	if ok, suppressed := self.pingFailThrottle.Allow(time.Now()); ok {
		self.log.Infof("[multi]evaluation ping error [%s] = %s%s\n",
			args.ClientId, err, suppressedSuffix(suppressed))
	}
	self.recordEvaluationFailure(windowFailureProvider, err)
	return true
}

// recordChannelCreationFailure reports whether a failed candidate needs a
// terminal provider event. A failure matching the construction context after
// that context ended is caller-owned epoch rebuild or window retirement, not
// a provider event or failure. A live-context error remains a genuine provider
// failure even when the remote error itself is Canceled or DeadlineExceeded.
// The caller retains generator ownership until this classification completes.
func (self *multiClientWindow) recordChannelCreationFailure(
	evaluationCtx context.Context,
	args *multiClientChannelArgs,
	err error,
) bool {
	if isEvaluationContextCancellation(evaluationCtx, err) {
		return false
	}
	// unconditional (V0): this transition was invisible in the field —
	// EvaluationFailed is terminal, the dot is deleted from the monitor map,
	// and nothing said why
	if ok, suppressed := self.createFailThrottle.Allow(time.Now()); ok {
		self.log.Infof("[multi]create channel error [%s] = %s%s\n",
			args.ClientId, err, suppressedSuffix(suppressed))
	}
	self.recordEvaluationFailure(windowFailureProvider, err)
	return true
}

// reliabilitySettings is the effective reliability config for this window: the
// parent's runtime override when one is installed, else what the window was
// constructed with. Safe on a bare window, which the suite's fixtures rely on.
func (self *multiClientWindow) reliabilitySettings() *ReliabilitySettings {
	if self.reliabilitySettingsFunc != nil {
		return self.reliabilitySettingsFunc()
	}
	return ReliabilitySettingsFrom(self.settings)
}

// flowCount reads the parent's live flow count for a window client. nil func
// (a bare test window) reads as 0 -- every client looks flowless, so the
// capacity gates below behave exactly as they did before this count existed,
// which is what the bare fixtures assert. Must not be called with the window
// stateLock or any channel lock held; see the flowCountFunc field.
func (self *multiClientWindow) flowCount(client *multiClientChannel) int {
	if self.flowCountFunc == nil {
		return 0
	}
	return self.flowCountFunc(client)
}

// strictWindowAdmissionAllowed applies the opt-in ownership ceiling at the
// last responsible moment before a candidate is installed. Resize already
// targets WindowSizeHardMax, but it deliberately retains flow-carrying warned
// clients; without this gate, backfill can temporarily add a fourth mobile
// quality client while three remain live. A same-identity replacement does
// not increase ownership and therefore remains admissible at the ceiling.
func (self *multiClientWindow) strictWindowAdmissionAllowed(
	clientId Id,
	windowSize WindowSizeSettings,
) bool {
	if self == nil || self.settings == nil ||
		!self.settings.StrictWindowSizeHardMax || windowSize.WindowSizeHardMax <= 0 {
		return true
	}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	_, replacing := self.clients[clientId]
	return replacing || len(self.clients) < windowSize.WindowSizeHardMax
}

// collapseRemovalAllowed is the capacity-collapse gate: it admits removing a
// zero-weight client when the client is flowless, or -- the escape hatch --
// when the client is past the hard collapse deadline of
// collapseDeadlineLifetimes x MaxClientLifetime measured from its first event
// time. clientDuration is the caller's stats.clientDuration, which
// windowStatsWithCoalesce computes as now minus firstEventTime -- the same
// anchor removeTime derives from -- so "deadline <= clientDuration" is
// exactly "now is past firstEventTime + 2 lifetimes" without re-reading
// channel state here.
//
// Flowless is the normal path: a drained client removes exactly as it did
// before this gate existed, and a nil flowCountFunc (a bare test window)
// reads every client as flowless, keeping pre-change behavior for the bare
// fixtures. The deadline is only the escape: by then the client has been
// drain-warned for at least a whole extra lifetime, so anything still bound
// to it is effectively immortal and may no longer pin the exit against the
// hard max.
//
// With MaxClientLifetime 0 (rotation disabled) there is no deadline: the
// operator opted out of forced rotation, so a flow-carrying client is simply
// never collapsible. Must be called with no window or channel lock held --
// the flow count read takes the parent stateLock inside (see flowCount).
func (self *multiClientWindow) collapseRemovalAllowed(client *multiClientChannel, clientDuration time.Duration) bool {
	if self.flowCount(client) == 0 {
		return true
	}
	maxClientLifetime := self.settings.MaxClientLifetime
	if 0 < maxClientLifetime && time.Duration(collapseDeadlineLifetimes)*maxClientLifetime <= clientDuration {
		return true
	}
	return false
}

// replacementAllowed is expand's same-clientId gate: when the generator
// re-hands an identity already in the window, the freshly pinged channel may
// only replace (cancel) the existing one when the existing channel is done or
// flowless. Cancelling a live flow-carrying channel just because the
// generator re-issued its identity destroys established connections for no
// failure at all -- the old channel was routing fine. When this returns
// false the caller declines the replacement: the new channel and its args
// are discarded and the old channel keeps its flows.
//
// nil (no channel under the id) and done are the ordinary replace cases: the
// slot is empty or the occupant is already dead, so installing the new
// channel is pure gain. A nil flowCountFunc (bare fixtures, and the window
// before the parent injects the count) reads as flowless, which is the
// pre-change always-replace behavior. Must be called with no window or
// channel lock held -- the flow count read takes the parent stateLock inside
// (see flowCount).
func (self *multiClientWindow) replacementAllowed(existingClient *multiClientChannel) bool {
	if existingClient == nil {
		return true
	}
	if existingClient.IsDone() {
		return true
	}
	return self.flowCount(existingClient) == 0
}

// metrics reaches the parent's shared reliability counters, mirroring the
// channel-side accessor. nil func or nil counters (a bare window) is fine:
// every counter method tolerates a nil receiver.
func (self *multiClientWindow) metrics() *reliabilityMetrics {
	if self.reliabilityMetricsFunc != nil {
		return self.reliabilityMetricsFunc()
	}
	return nil
}

// The extenders carrying one exit's platform transport right now (K1). None
// when the generator owns no transports, which is every fixture window and
// every P2P-only client.
func (self *multiClientWindow) clientExtenderIps(client *multiClientChannel) []netip.Addr {
	source, ok := self.generator.(MultiClientGeneratorWithExtenderIps)
	if !ok || client == nil || client.client == nil {
		return nil
	}
	ips, _ := source.ClientExtenderIps(client.client)
	return ips
}

// watchExtenderIps republishes one exit's extender addresses on this window's
// monitor whenever they change (K1), so a provider dot follows the transport
// it is actually carried by. It ends with the window or with the client. The
// generator arms the change channel immediately before reading the addresses,
// so nothing can slip between the two; a generator with no transport for this
// client returns no channel and the watcher simply waits for the client to
// end.
func (self *multiClientWindow) watchExtenderIps(client *multiClientChannel) {
	source, ok := self.generator.(MultiClientGeneratorWithExtenderIps)
	if !ok || client == nil || client.client == nil {
		return
	}
	clientId := client.ClientId()
	for {
		extenderIps, change := source.ClientExtenderIps(client.client)
		// the dot belongs to whichever channel currently holds this id: a
		// same-id replacement installs its own watcher, and this one must not
		// publish over it
		owned := func() bool {
			self.stateLock.Lock()
			defer self.stateLock.Unlock()
			return self.clients[clientId] == client
		}()
		if !owned {
			return
		}
		self.monitor.SetProviderExtenderIps(clientId, extenderIps)
		select {
		case <-self.ctx.Done():
			return
		case <-client.Done():
			return
		case <-change:
		}
	}
}

// blackholeVerdictErr reports whether a channel's end error is a blackhole
// verdict, which is what makes its removal subject to the storm breaker's
// budget. Keyed on the "Blackhole " prefix that every verdict line
// detectBlackhole writes shares (including the quarantine-expired variant) --
// the reason strings were made distinctive exactly so a later reader could
// key on them. Everything else that lands a client in the WindowStats error
// branch is exempt by construction: user action and shutdown cleanup write
// "Done." (Cancel/Close), a dead continuous ping surfaces its transport
// error verbatim (an unanswered ping ends only the ping loop, never the
// channel), and the stall watchdog writes "send stalled: ..."
// (convictSendStalls). Those are hard evidence or explicit intent, and
// metering them would delay cleanup that costs nothing to run -- which is
// why those reason strings must never gain a "Blackhole " prefix.
func blackholeVerdictErr(err error) bool {
	return err != nil && strings.HasPrefix(err.Error(), "Blackhole ")
}

// verdictRemovalAllowed is the storm breaker: it admits a verdict-driven
// removal only while fewer than RemovalBudgetCount such removals happened in
// the last RemovalBudgetWindow, and records the removal when it admits one.
//
// The reasoning: verdicts convict independently, but their evidence is not
// independent -- one local cause (a network migration, an uplink dying) makes
// every exit silent at once, and the field capture that motivated this showed
// exactly that shape, 7 exits executed in 79s on identical no-receive
// verdicts. Two removals in half a minute is a provider failing and its
// replacement failing too; a third is a pattern, and the budget says the
// pattern is more likely us than them. A deferred removal costs seconds (the
// client is warned out of selection and re-judged next pass); a wrong mass
// removal costs every flow in the window.
//
// RemovalBudgetCount 0 (or a non-positive window, which could never
// accumulate a budget) turns the breaker off. Takes the window stateLock;
// callers hold no lock, per the resize idiom of classifying under no lock and
// acting through the closures.
func (self *multiClientWindow) verdictRemovalAllowed(now time.Time) bool {
	reliabilitySettings := self.reliabilitySettings()
	budgetCount := reliabilitySettings.RemovalBudgetCount
	budgetWindow := reliabilitySettings.RemovalBudgetWindow
	if budgetCount <= 0 || budgetWindow <= 0 {
		return true
	}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	horizon := now.Add(-budgetWindow)
	// timestamps are appended in order, so pruning is a prefix scan
	i := 0
	for i < len(self.verdictRemovalTimes) && self.verdictRemovalTimes[i].Before(horizon) {
		i += 1
	}
	if 0 < i {
		self.verdictRemovalTimes = self.verdictRemovalTimes[i:]
	}

	if budgetCount <= len(self.verdictRemovalTimes) {
		return false
	}
	self.verdictRemovalTimes = append(self.verdictRemovalTimes, now)
	return true
}

func (self *multiClientWindow) shuffle() {
	for _, client := range self.unorderedClients() {
		client.Cancel()
	}
	self.resizeMonitor.NotifyAll()
}

// removeProvider cancels every client routing to this egress (destination
// tail) client id. Cancellation wakes the resize loop, which reaps the client
// (emitting ProviderStateRemoved) and refills the window.
func (self *multiClientWindow) removeProvider(egressClientId Id) bool {
	removed := false
	for _, client := range self.unorderedClients() {
		if client.Destination().Tail() == egressClientId {
			client.Cancel()
			removed = true
		}
	}
	if removed {
		self.resizeMonitor.NotifyAll()
	}
	return removed
}

func (self *multiClientWindow) unorderedClients() []*multiClientChannel {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return slices.Collect(maps.Values(self.clients))
}

// lastResortClients is the window's offer when its ordinary offer is empty:
// every client that is still alive, warned and quarantined ones included. A
// benched exit is soft-suspect, not dead -- on a pool whose providers stall
// intermittently, the whole window benches at once routinely, and refusing
// to place new flows then turns a rough pool into no service at all. The
// list is deliberately unordered and unfiltered beyond liveness: the caller
// races it, and the first responder is by construction whichever benched
// exit is currently moving bytes.
func (self *multiClientWindow) lastResortClients() []*multiClientChannel {
	return self.lastResortClientsForIpVersion(0)
}

// lastResortClientsForIpVersion is lastResortClients narrowed to the exits
// that can carry the version -- a benched exit that cannot carry v6 is no
// exit at all for a v6 flow. 0 is any.
func (self *multiClientWindow) lastResortClientsForIpVersion(ipVersion int) []*multiClientChannel {
	clients := []*multiClientChannel{}
	for _, client := range self.unorderedClients() {
		if client.IsDone() || !client.supportsIpVersion(ipVersion) {
			continue
		}
		clients = append(clients, client)
	}
	return clients
}

// OrderedClients is the window's offer to the race: its healthy clients,
// weighted-shuffled, narrowed to the best rank present ("min tier") so
// traffic does not cross rank until necessary.
// windowMinSatisfied is the monitor's "connected" gate. A fixed destination
// counts warned clients toward the minimum: its only replacement is another
// client to the same endpoint, so a warned sole selected peer must not report
// "connecting" for its whole session while it is still routing (the same
// judgment as the OrderedClients fixed-destination fallback).
//
// An expanding window normally excludes warned clients because it can replace
// them. A strict window at its ownership ceiling cannot: the admission gate
// counts retained warned clients and rejects a new identity. Count those
// clients toward the presentation minimum only when the ceiling is saturated
// and at least one unwarned client remains selectable. This keeps the monitor
// consistent with the admission policy without declaring an all-warned window
// connected or concealing a replacement slot that can still be filled.
func windowMinSatisfied(
	windowSizeMin int,
	clientCount int,
	warnedCount int,
	fixedDestination bool,
	strictWindowSizeHardMax bool,
	windowSizeHardMax int,
) bool {
	if fixedDestination {
		return windowSizeMin <= clientCount+warnedCount
	}
	if windowSizeMin <= clientCount {
		return true
	}
	return strictWindowSizeHardMax &&
		0 < clientCount &&
		0 < windowSizeHardMax &&
		windowSizeHardMax <= clientCount+warnedCount &&
		windowSizeMin <= clientCount+warnedCount
}

func (self *multiClientWindow) OrderedClients() []*multiClientChannel {
	return self.orderedClients(false, 0)
}

// OrderedClientsForIpVersion is OrderedClients narrowed to the exits that
// can carry a flow of the packet's ip version (IPV6.md B3). 0 is any.
func (self *multiClientWindow) OrderedClientsForIpVersion(ipVersion int) []*multiClientChannel {
	return self.orderedClients(false, ipVersion)
}

// orderedClientsCrossTier is OrderedClients without the min-tier gate, for
// the caller that has decided crossing rank IS now necessary -- every min-tier
// exit sitting at the flow cap. See RemoteUserNatMultiClient.raceCandidates.
func (self *multiClientWindow) orderedClientsCrossTier() []*multiClientChannel {
	return self.orderedClients(true, 0)
}

// orderedClientsCrossTierForIpVersion is orderedClientsCrossTier narrowed
// to the exits that can carry the version. 0 is any.
func (self *multiClientWindow) orderedClientsCrossTierForIpVersion(ipVersion int) []*multiClientChannel {
	return self.orderedClients(true, ipVersion)
}

// ipVersion narrows the offer to exits whose category can carry a flow of
// that version (IPV6.md B3); 0 keeps every exit. The narrowing runs before
// the weighting and the rank gate, so a v6 flow sees the best rank among
// the exits that can actually carry it.
func (self *multiClientWindow) orderedClients(crossTier bool, ipVersion int) []*multiClientChannel {
	var windowSize WindowSizeSettings
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		if _, profileWindowSize, ok := self.performanceProfile.FixedWindow(); ok {
			windowSize = profileWindowSize
		} else {
			windowSize = self.settings.WindowSizes[self.windowType]
		}
	}()

	clients := []*multiClientChannel{}
	lruTimes := map[*multiClientChannel]time.Time{}
	weights := map[*multiClientChannel]float32{}

	for _, client := range self.unorderedClients() {
		if !client.supportsIpVersion(ipVersion) {
			continue
		}
		if stats, err := client.WindowStats(); err == nil && !client.isWarning() {
			clients = append(clients, client)
			if !stats.lastEventTime.IsZero() {
				lruTimes[client] = stats.lastEventTime
			}
			weights[client] = float32(1 + stats.ExpectedByteCountPerSecond())
		}
	}

	if 0 == len(clients) {
		// a fixed-destination window has no replacement to prefer: its only
		// "alternative" is another client to the same endpoint, so excluding
		// every warned client leaves new flows with no candidate at all --
		// stalled on the send retry cadence while the peer still routes.
		// Offer the warned field as-is instead, the same judgment as the
		// race-level benched fallback ("a benched exit is better than no
		// exit").
		if self.generator != nil {
			if _, fixed := self.generator.FixedDestinationSize(); fixed {
				for _, client := range self.unorderedClients() {
					if !client.supportsIpVersion(ipVersion) {
						continue
					}
					if stats, err := client.WindowStats(); err == nil {
						clients = append(clients, client)
						if !stats.lastEventTime.IsZero() {
							lruTimes[client] = stats.lastEventTime
						}
						weights[client] = float32(1 + stats.ExpectedByteCountPerSecond())
					}
				}
			}
		}
		if 0 == len(clients) {
			return clients
		}
	}

	if self.log.V(1).Enabled() {
		self.statsSampleWeights(weights)
	}

	if 0 < windowSize.FixedWindowSize && windowSize.FixedWindowSize < len(clients) {
		slices.SortFunc(clients, func(a *multiClientChannel, b *multiClientChannel) int {
			lruTimeA := lruTimes[a]
			lruTimeB := lruTimes[b]
			// descending
			if lruTimeA.Before(lruTimeB) {
				return 1
			} else if lruTimeB.Before(lruTimeA) {
				return -1
			}
			weightA := weights[a]
			weightB := weights[b]
			// descending
			if weightA < weightB {
				return 1
			} else if weightB < weightA {
				return -1
			}
			return 0
		})
		clients = clients[:windowSize.FixedWindowSize]
	}

	WeightedShuffleWithEntropy(clients, weights, self.settings.StatsWindowEntropy)

	if crossTier {
		return clients
	}

	// use only the top n items from the window
	// if 0 < windowSize.WindowSizeUseMax {
	// 	clients = clients[:min(len(clients), windowSize.WindowSizeUseMax)]
	// }

	// use only clients in the min tier
	// this prevents the window from crossing rank until necessary
	return minTierClients(clients)
}

// minTierClients keeps only the clients of the best (lowest) rank present,
// preserving order. Empty input yields empty output.
//
// Rank is effectiveTier -- the static platform tier plus live demerits --
// so a provider failing dials falls out of the kept set on the next pass and
// the race field reorders onto the healthy without this function changing
// shape (raceCandidates' tier-crossing and least-loaded overflow consume the
// result as before). With EffectiveTierSelection off, effectiveTier IS
// Tier() and this is exactly the pre-change rank gate. Each client's rank is
// read once into a parallel slice: effectiveTier takes the channel lock and
// prunes state, so reading it twice would double that work and could tear --
// a demerit landing between the min scan and the filter would keep a
// different set than the min was computed over.
func minTierClients(clients []*multiClientChannel) []*multiClientChannel {
	if len(clients) == 0 {
		return clients
	}
	tiers := make([]int, len(clients))
	for i, client := range clients {
		tiers[i] = client.effectiveTier()
	}
	minTier := tiers[0]
	for _, tier := range tiers[1:] {
		minTier = min(minTier, tier)
	}
	kept := []*multiClientChannel{}
	for i, client := range clients {
		if tiers[i] == minTier {
			kept = append(kept, client)
		}
	}
	return kept
}

func (self *multiClientWindow) statsSampleWeights(weights map[*multiClientChannel]float32) {
	// randonly sample log statistics for weights
	if mathrand.Intn(self.settings.StatsSampleWeightsCount) == 0 {
		// sample the weights
		weightValues := slices.Collect(maps.Values(weights))
		slices.SortFunc(weightValues, func(a float32, b float32) int {
			// descending
			if a < b {
				return 1
			} else if b < a {
				return -1
			} else {
				return 0
			}
		})
		net := float32(0)
		for _, weight := range weightValues {
			net += weight
		}
		if 0 < net {
			var sb strings.Builder
			netThresh := float32(0.99)
			netp := float32(0)
			netCount := 0
			for i, weight := range weightValues {
				p := 100 * weight / net
				netp += p
				netCount += 1
				if 0 < i {
					sb.WriteString(" ")
				}
				sb.WriteString(fmt.Sprintf("[%d]%.2f", i, p))
				if netThresh*100 <= netp {
					break
				}
			}

			self.log.Infof("[multi]sample weights: %s (+%d more in window <%.0f%%)\n", sb.String(), len(weights)-netCount, 100*(1-netThresh))
		} else {
			self.log.Infof("[multi]sample weights: zero (%d in window)\n", len(weights))
		}
	}
}

func (self *multiClientWindow) Close() {
	var removedClients []*multiClientChannel
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		for _, client := range self.clients {
			// client.Close()
			removedClients = append(removedClients, client)
		}
		clear(self.clients)
	}()
	for _, client := range removedClients {
		client.Close()
	}
	// DELIBERATELY NOT self.removeClients(removedClients) -- but the reason is
	// worth stating, because the bare commented-out call above invited exactly
	// the wrong fix.
	//
	// removeClients does TWO things: it emits ProviderStateRemoved to the
	// monitor, and it invokes clientRemoveCallback (multiClient.removeClient),
	// which migrates the dying exit's flows onto replacement exits. The second
	// is meaningless here -- this is session teardown, every replacement was
	// just closed in the loop above -- so calling removeClients wholesale would
	// have every exit try to rebind its flows onto corpses.
	//
	// The cost of skipping it is that providers leave no removal record at
	// session close. That is a real observability gap (issue #51), and the fix
	// is to emit the monitor event WITHOUT the migration callback -- not to
	// uncomment this line. Note #51's reported symptom is a MID-SESSION
	// self-close, which is a different path than this one; this is a second,
	// confirmed instance of the same class, not necessarily its cause.
}

func (self *multiClientWindow) removeClients(removedClients ...*multiClientChannel) {
	for _, client := range removedClients {
		self.monitor.AddProviderEvent(client.ClientId(), ProviderStateRemoved, client.args.Destination.Tail(), client.args.Location, client.IpFamily())
	}
	for _, client := range removedClients {
		self.clientRemoveCallback(client)
	}
}

type multiClientChannelArgs struct {
	MultiClientGeneratorClientArgs

	Destination MultiHopId
	DestinationStats

	// ReceivePackets, when set, takes each client receive dispatch's parsed
	// return packets as ONE call instead of one per packet (see
	// clientReceive), so the owner can amortize per-packet delivery costs.
	// nil falls back to the per-packet callback for every packet.
	ReceivePackets clientReceivePacketsFunction

	// NetworkPeerDestination is true only when the embedding app explicitly
	// selected a trusted same-network peer and the entire multi-client uses
	// the Network relationship.
	NetworkPeerDestination bool

	// FixedDestination marks an exit of a fixed-destination window (the
	// generator reports a fixed destination set: an explicit client id, a
	// network peer). Discovery is bypassed for these, so their category is
	// always legacy, and the user chose the peer whatever its family: the
	// placement predicate does not narrow a fixed exit by ip version, it
	// reads as v6 available, and it is never a family-swap victim
	// (IPV6.md B1 exempts fixed windows from the soft minimum).
	FixedDestination bool

	// contractStatus preserves the identity of the channel whose contract
	// manager emitted a result. The public constructor callback does not carry
	// that identity; the owning window needs it to retire only the failed route.
	// nil keeps directly constructed test channels on the legacy relay path.
	contractStatus func(client *multiClientChannel, status *ContractStatus)
}

// clientReceivePacketsFunction is the batch form of
// clientReceivePacketFunction: all parsed return packets of one dispatch, in
// delivery order, borrowed for the call.
type clientReceivePacketsFunction func(client *multiClientChannel, source TransferPath, provideMode protocol.ProvideMode, transportType TransportType, ipPaths []*IpPath, packets [][]byte)

type multiClientEventType int

const (
	multiClientEventTypeAck    multiClientEventType = 1
	multiClientEventTypeNack   multiClientEventType = 2
	multiClientEventTypeError  multiClientEventType = 3
	multiClientEventTypeSource multiClientEventType = 4
)

type multiClientEventBucket struct {
	createTime time.Time
	eventTime  time.Time

	sendAckCount        int
	sendAckByteCount    ByteCount
	sendNackCount       int
	sendNackByteCount   ByteCount
	sendSynCount        int
	receiveAckCount     int
	receiveAckByteCount ByteCount
	receiveSynCount     int
	sendAckTime         time.Time
	sendNackTime        time.Time
	sendSynTime         time.Time
	errs                []error
	ip4Paths            map[Ip4Path]bool
	ip6Paths            map[Ip6Path]bool
}

func newMultiClientEventBucket() *multiClientEventBucket {
	now := time.Now()
	return &multiClientEventBucket{
		createTime: now,
		eventTime:  now,
	}
}

type clientWindowStats struct {
	log Logger

	sourceCount    int
	netSourceCount int
	// sendDestinationCount is how many distinct destination paths (protocol,
	// destination ip, destination port) this channel has sent toward inside
	// the surviving stats window. Computed as the key count of the
	// ip{4,6}DestinationSourceCount maps, which addSend maintains per
	// surviving bucket and coalesceEventBuckets releases with the buckets --
	// so the count covers exactly the same buckets the packet counters
	// aggregate over (all surviving buckets, including the newest partial
	// ones). This is the corroboration input to the no-receive-ack blackhole
	// gate: with only one destination in the window, tunnel silence is
	// indistinguishable from that one destination being dead. See
	// MinBlackholeDestinations.
	sendDestinationCount int
	sendAckCount         int
	sendAckByteCount     ByteCount
	sendNackCount        int
	sendNackByteCount    ByteCount
	sendSynCount         int
	receiveAckCount      int
	receiveAckByteCount  ByteCount
	receiveSynCount      int
	ackByteCount         ByteCount
	windowDuration       time.Duration
	firstSendAckTime     time.Time
	firstSendNackTime    time.Time
	firstSendSynTime     time.Time
	// firstUnansweredSendSynTime is the start of the CURRENT unanswered
	// connect attempt: set when a SYN goes out and no attempt is pending,
	// left alone by SYN retransmits (the device stack retransmits at 1s, 2s,
	// 4s, 8s... — each one resets a "latest SYN" clock, which is why the
	// no-receive-syn clock cannot use one), and cleared by any received SYN,
	// since an answered connect proves the route forwards connects right now.
	// LIFETIME, deliberately not windowed like the counters beside it: the
	// windowed firstSendSynTime is derived from surviving buckets and so has
	// the same ~StatsWindowDuration ceiling receiveTimeout has (see the
	// blackholeReasonFromStats doc) — with BlackholeConnectTimeout at that
	// same scale, the windowed clock ages out at the very moment the
	// no-receive-syn bar matures and the verdict can barely ever fire.
	firstUnansweredSendSynTime  time.Time
	estimatedByteCountPerSecond ByteCount
	// FIXME firstStatDuration
	clientDuration       time.Duration
	healthyDuration      time.Duration
	unhealthyDuration    time.Duration
	netHealthyDuration   time.Duration
	netUnhealthyDuration time.Duration
	healthy              bool
	removeTime           time.Time
	lastEventTime        time.Time

	// internal
	bucketCount int
}

func (self *clientWindowStats) EffectiveByteCountPerSecond() ByteCount {
	millis := int64(self.windowDuration / time.Millisecond)
	if millis <= 0 {
		return ByteCount(0)
	}
	netByteCount := int64(self.sendAckByteCount + self.receiveAckByteCount)
	return ByteCount((1000*netByteCount + millis/2) / millis)
}

func (self *clientWindowStats) EffectiveByteCount() (send ByteCount, receive ByteCount) {
	millis := int64(self.windowDuration / time.Millisecond)
	if millis <= 0 {
		return
	}
	send = self.sendAckByteCount
	receive = self.receiveAckByteCount
	return
}

func (self *clientWindowStats) ExpectedByteCountPerSecond() ByteCount {
	millis := int64(self.windowDuration / time.Millisecond)
	if millis <= 0 {
		return self.estimatedByteCountPerSecond
	}
	netByteCount := int64(self.sendAckByteCount + self.sendNackByteCount + self.receiveAckByteCount)
	if self.log.V(2).Enabled() {
		self.log.Infof("[multi]expected use estimated = %dbps (net = %db/%dms)\n", self.estimatedByteCountPerSecond, netByteCount, millis)
	}
	return max(
		self.estimatedByteCountPerSecond-ByteCount((1000*netByteCount+millis/2)/millis),
		0,
	)
}

type multiClientChannel struct {
	ctx    context.Context
	cancel context.CancelFunc
	log    Logger

	args *multiClientChannelArgs

	api *BringYourApi

	clientReceivePacketCallback clientReceivePacketFunction
	dialFailureCallback         dialFailureFunction
	ingressSecurityPolicy       SecurityPolicy
	ingressIpv4Fragments        ipv4FragmentGate
	// optional owner of this client's platform transport. Captured once at
	// construction so ordinary receive frames do not pay a type assertion.
	transportMigrator  MultiClientGeneratorTransportMigrator
	performanceProfile *PerformanceProfile
	createTime         time.Time

	settings *MultiClientSettings
	// effectiveLifetime is this channel's jittered rotation lifetime:
	// settings.MaxClientLifetime x uniform(0.75, 1.0), drawn once by the
	// constructor (see jitterClientLifetime) and immutable afterwards, so it
	// needs no lock. Every place a removeTime is derived from the lifetime
	// must read this field, not settings.MaxClientLifetime -- reading the
	// setting directly would silently re-synchronize rotation. Zero on
	// channels built directly by tests (and whenever MaxClientLifetime is 0),
	// which reproduces the pre-jitter behavior for a zero lifetime exactly.
	effectiveLifetime time.Duration
	// reliabilitySettingsFunc reads the parent's effective reliability config,
	// so the blackhole bound can be retuned on a live connection. nil on
	// channels built directly by tests; see reliabilitySettings().
	reliabilitySettingsFunc func() *ReliabilitySettings
	// uplinkGateFunc reads the parent's tunnel-wide uplink gate; see
	// RemoteUserNatMultiClient.uplinkGate. nil on channels built directly by
	// tests, which reads as gate off; see uplinkGate().
	uplinkGateFunc func(now time.Time) (stale bool, freshSince time.Time)
	// reliabilityMetricsFunc reaches the parent's shared counters. nil on
	// channels built directly by tests, which loses the counts, never the
	// behavior; see metrics().
	reliabilityMetricsFunc func() *reliabilityMetrics
	// flowCountFunc reads the parent's live flow count for this channel (the
	// parent's clientFlowCount, same func the window carries). nil on channels
	// built directly by tests, which reads as 0 flows -- and a flowless
	// verdict executes, so bare fixtures keep the pre-change
	// execute-immediately behavior they assert; see flowCount(). The func
	// takes the parent stateLock inside, so it must never be called with this
	// channel's stateLock (or any leaf lock) held.
	flowCountFunc func(*multiClientChannel) int
	// resizeWakeFunc wakes the owning window's resize pass, so a demoted
	// (quarantined) exit gets its replacement expanded now rather than on the
	// next 15s tick. nil on channels built directly by tests -- the wake is
	// an optimization, never a correctness requirement, because the periodic
	// pass picks the quarantine up through isWarning anyway; see resizeWake().
	resizeWakeFunc func()
	// migrateFlowsFunc is the bench-time hand-off seam: the parent's
	// migrateClientFlows, invoked from the verdict itself when a soft
	// verdict benches this exit. nil on channels built directly by tests --
	// the hand-off is an optimization, never a correctness requirement.
	// Takes the parent stateLock inside, so it must never be called with
	// this channel's stateLock held (migrateFlows releases it first).
	migrateFlowsFunc func(*multiClientChannel)
	// providerQualifiedFunc reads the parent's qualification table for this
	// channel's destination (RemoteUserNatMultiClient.providerQualified).
	// Consumed by effectiveTier as a +1 demerit for unproven/stale providers
	// when ProviderProbe is on. nil on channels built directly by tests, which
	// reads as NO demerit -- absence of the probe machinery must never demote
	// anyone, matching every other nil-func convention here. The func takes
	// the parent stateLock inside, so it must never be called with this
	// channel's stateLock (or any leaf lock) held -- effectiveTier reads it
	// before taking its own lock.
	providerQualifiedFunc func(MultiHopId) bool
	// receivingSiblingsFunc counts how many OTHER channels currently show
	// recent receive progress (RemoteUserNatMultiClient.receivingChannelCount).
	// Consumed by detectBlackhole as the comparative connect cut's evidence.
	// nil on channels built directly by tests, which reads as ZERO receiving
	// siblings -- absence of the machinery must leave the patient full
	// BlackholeConnectTimeout in place, matching every other nil-func
	// convention here. The func takes the parent stateLock inside, so it must
	// never be called with this channel's stateLock (or any leaf lock) held.
	receivingSiblingsFunc func(exclude *multiClientChannel) int
	// busyProbeSendFunc, when non-nil, overrides how the busy-flow liveness
	// probe puts its control ping on the wire. Test seam only: the real path
	// needs a live Client under the channel and a provider to answer, and what
	// needs testing is the probe's DECISION (acquit, defer, convict) against
	// each outcome. Production leaves it nil, which routes through the same
	// SendDetailedMessage(&protocol.IpPing{}) plumbing the cping loop uses --
	// pinned by TestBusyProbeUsesTheControlPingPlumbing.
	busyProbeSendFunc func(timeout time.Duration, ackCallback func(error)) (bool, error)
	// Nil outside focused tests. The callback assumes the same conditional
	// ownership as Transfer: success consumes every group packet.
	sendGroupForTest func(*parsedPacketGroup, time.Duration, bool) (bool, error)
	// Nil outside focused tests. Replaces only concrete Client admission after
	// the real packet/group callback is built, since Client is not an interface.
	sendTransferForTest func(AckFunction) (bool, error)
	// Nil outside focused tests. Makes the send-to-completion interval exact
	// without replacing either side of the production callback path.
	packetTransferNowForTest func() time.Time
	// Nil outside focused tests. Overrides only the idle cping send so its
	// timeout lifecycle can be tested without constructing a full Transfer
	// route. Production uses SendDetailedMessage with the same signature.
	pingSendForTest func(time.Duration, func(error)) (bool, error)
	// qualificationRefreshFunc re-stamps the parent's qualification for this
	// channel's destination (RemoteUserNatMultiClient.recordProbePass), called
	// from the receive-ack path at most once per
	// qualificationReceiveRefreshInterval -- real receive progress is better
	// qualification evidence than any probe, and refreshing on it keeps loaded
	// exits from ever going stale or wasting a re-probe. nil on channels built
	// directly by tests. Same parent-stateLock contract as
	// providerQualifiedFunc; see touchQualificationOnReceive.
	qualificationRefreshFunc func(MultiHopId)
	// qualificationRefreshedNanos is the last receive-refresh stamp (unix
	// nanos), the atomic gate that keeps touchQualificationOnReceive near-free
	// on the per-packet path.
	qualificationRefreshedNanos atomic.Int64
	// Latest immutable provider identity/block snapshot. Atomic publication
	// keeps the receive callback off stateLock and Exits reads contention-free.
	providerDiagnostics atomic.Pointer[ProviderDiagnostics]

	// sourceFilter map[TransferPath]bool

	client *Client
	// contractReliabilityOnce makes the platform's terminal destination
	// verdict one lifecycle transition. Duplicate status frames may be
	// dispatched concurrently, but they must not schedule unbounded window
	// replacements or repeat generator mutations.
	contractReliabilityOnce sync.Once

	stateLock    sync.Mutex
	eventBuckets []*multiClientEventBucket
	// destination -> source -> count
	ip4DestinationSourceCount          map[Ip4Path]map[Ip4Path]int
	ip6DestinationSourceCount          map[Ip6Path]map[Ip6Path]int
	packetStats                        *clientWindowStats
	endErr                             error
	maxEffectiveByteCountPerSecond     ByteCount
	maxEffectiveByteCountPerSecondTime time.Time
	firstEventTime                     time.Time

	healthy              bool
	lastHealthyTime      time.Time
	lastUnhealthyTime    time.Time
	netHealthyDuration   time.Duration
	netUnhealthyDuration time.Duration

	// affinityCount int
	// affinityTime  time.Time

	clientReceiveUnsub func()

	// warning + warnCause: the resize pass's exclusion from new-flow
	// selection, and WHY. The bool is the compatibility surface every
	// existing consumer reads (isWarning ORs it with quarantined); the cause
	// is for consumers that must treat retirement differently from evidence
	// -- a draining exit is healthy and its groups deserve a coordinated
	// move, an unhealthy or starved one is suspect and its groups scatter on
	// purpose. Written together under stateLock by setWarning.
	warning   bool
	warnCause warnCause

	// the quarantine state: a soft blackhole verdict fired against this
	// channel while it carried live flows, so instead of executing it the
	// channel was demoted -- warned out of selection with its established
	// flows left running -- until the flows drain, the evidence expires the
	// quarantine, or receive progress clears it. Deliberately NOT folded into
	// the shared `warning` bool: the resize pass recomputes that bool every
	// pass and its healthy path writes setWarning(false), which would
	// silently clear a quarantine the verdict pass just set. Guarded by
	// stateLock like the rest of the channel state. quarantineStart is when
	// the current reason began holding continuously; setQuarantined restarts
	// it when the reason changes, so the expiry bound always measures one
	// unbroken run of the same evidence.
	quarantined      bool
	quarantineReason blackholeReason
	quarantineStart  time.Time

	// survived-quarantine memory: set on every quarantine lift (receive
	// progress, or the bench-leak release), it keeps the effectiveTier
	// demerit applied after the episode itself ends. Demotion is instant
	// (the demerit applies on the next selection pass) but promotion back
	// must be slow and earned, or a flapping exit oscillates in and out of
	// the top rank on every lift. The memory expires only when BOTH hold:
	// quarantineMemoryDuration has passed since the lift (a clean interval
	// with no re-quararantine -- a new episode's lift restarts the clock),
	// AND at least one proven upstream connect landed since the lift
	// (quarantineLiftConnectSeen, stamped by addConnectSuccess) -- positive
	// evidence, not just the absence of new suspicion. Guarded by stateLock;
	// the expiry check decays the flag on read in effectiveTier.
	survivedQuarantine        bool
	quarantineLiftTime        time.Time
	quarantineLiftConnectSeen bool

	// quarantineReconvictions counts this channel's completed bench-then-lift
	// cycles: 0 through its first-ever bench, incremented on every lift (see
	// clearQuarantineWithLock). Feeds benchDuration so a channel that keeps
	// re-earning a bench holds it longer each time (RFC 2439-style flap
	// damping) instead of the constant hold every episode got before this
	// task. Deliberately session-scoped and never persisted -- same lifetime
	// as quarantineMigrated and the rest of the episode bookkeeping beside
	// it, not new durable state. Guarded by stateLock.
	//
	// With QuarantineDampening on, this field is a decaying (leaky-bucket
	// style) count kept up to date at TWO points, both sharing the same pure
	// quarantineReconvictionDecay math and the same quarantineLiftTime
	// anchor (so a fresh conviction resets the decay clock for free -- no
	// separate field to keep in sync):
	//   - on WRITE (clearQuarantineWithLock): decay-then-increment. The OLD
	//     quarantineLiftTime is read and folded into this field via
	//     quarantineReconvictionDecay BEFORE it is overwritten and this
	//     lift's +1 is added -- so a count that decayed to a read of 0
	//     during a long quiet interval is stored back as 1 at the next
	//     conviction, not the historical peak + 1. This is what makes the
	//     decay durable across the count's next climb rather than a view
	//     that evaporates the instant this channel reconvicts.
	//   - on READ (quarantineReconvictionCount): the same decay is applied
	//     again, non-destructively, against elapsed time since that same
	//     quarantineLiftTime -- this is what lets a STILL-OPEN episode's
	//     benchDuration/StallEvents reading shrink as it sits quiet, before
	//     its own eventual lift ever runs the write-side fold.
	// QuarantineDampening off keeps both sites exactly what they were before
	// decay existed: the write site is exactly self.quarantineReconvictions++,
	// and the read site returns this field verbatim with no clock read at
	// all -- so a default build's benchDuration, and the StallEvents
	// telemetry this field also feeds (see exitMetricsSnapshot), stay
	// byte-for-byte what they were before decay existed.
	quarantineReconvictions int

	// pendingSendTime is when the current run of unacked sends began, reset on
	// every ack. With sendNackCount > 0 it is the age of the oldest unmade
	// progress, which is what sendStalled tests. Guarded by stateLock.
	pendingSendTime time.Time

	// the busy-flow liveness probe's state (see busyLivenessProbe). All three
	// are guarded by stateLock.
	//
	// busyProbeAckTime is the last time a probe was answered: proof the exit is
	// alive, taken on its own terms and recorded on its own field. It is
	// deliberately NOT written into pendingSendTime -- a probe ack is not a
	// send ack, and forging one would erase the true age of the outstanding
	// run from every other reader of that field (the abandoned-send undo, the
	// stats, a future reader that has not been written yet). sendStalled
	// instead measures its bar from the LATER of the two, so the acquittal
	// refreshes the stall clock exactly as far as the evidence supports and no
	// further: the exit gets another full SendStallTimeout to either deliver or
	// be asked again, and the record of what actually happened stays honest.
	//
	// busyProbeOutstanding is set while a probe is in flight and unanswered,
	// which is the effectiveTier suspect demerit's whole input. Cleared on the
	// ack and on the conviction.
	//
	// busyProbeSendFailures counts probes that could not even be queued within
	// ONE stale episode -- the send path being wedged full of the same unacked
	// data the probe is investigating is itself weak evidence, so it takes two
	// in a row to convict. Reset whenever the channel reads not-stalled, which
	// is what ends an episode.
	busyProbeAckTime      time.Time
	busyProbeOutstanding  bool
	busyProbeSendFailures int
	// stallHoldCounted latches the sibling-corroboration hold to one counter
	// increment and one log line per stall episode. Cleared on every
	// sendStalled reset path and on the probe ack, the same places the
	// unsendable run resets -- those are what end an episode.
	stallHoldCounted bool

	// quarantineMigrated latches the bench-time migration to once per
	// quarantine EPISODE (unlike drainMigrated, which is once per channel):
	// an exit can be benched, acquitted, and benched again, and each new
	// episode's flows deserve the same hand-off. Cleared wherever the
	// episode ends, beside the rest of the quarantine state.
	quarantineMigrated bool

	// drainMigrated latches G-3's drain-time migration to once per channel:
	// a drain lasts many resize passes, and the movable flows only need
	// moving the first time. Never cleared -- a channel drains once, and a
	// second migration pass would find nothing movable anyway; the latch
	// exists so the resize pass does not pay the candidate gather every 15s
	// for the rest of the drain.
	drainMigrated bool

	// lastReceiveAckTime is when return traffic last arrived on this channel,
	// stamped by addReceiveAck under the lock it already takes. Read by
	// hasRecentReceive for the parent's receive-side sibling count (the
	// comparative connect cut's evidence). Guarded by stateLock.
	lastReceiveAckTime time.Time
	// lastSendAckTime is when the provider last acknowledged one of this
	// channel's sends at the transfer layer. A send ack proves the local
	// path, the platform route, and the peer PROCESS are all alive — which is
	// exactly the evidence that makes an unanswered connect the peer's own
	// fault rather than an outage (see comparativeConnectTimeout's
	// recentOwnSendAck arm). Guarded by stateLock, like lastReceiveAckTime.
	lastSendAckTime time.Time

	// rttEwmaMillis/rttJitterMillis/rttSamples are the placement scorer's only
	// source of per-exit round-trip time (see exitMetricsSnapshot). Nothing
	// on the transfer layer timed a send-ack round trip before this:
	// SendDetailedMessage just relays an opaque ackCallback, and the busy
	// probe's own wait (busyLivenessProbe) only measures elapsed time to
	// detect a suspended scheduler and throws the duration away. So this
	// channel times it itself -- SendDetailedWithAck's Ack-required callback
	// captures the send instant in its closure and folds the elapsed duration
	// in here only after a successful peer Ack, via addSendRttSample.
	//
	// rttEwmaMillis smooths with the classic 1/8 TCP SRTT weight (RFC 6298);
	// rttJitterMillis is the matching RTTVAR-style mean absolute deviation,
	// seeded (not smoothed) from the first gap since a single sample has no
	// deviation to report. rttSamples is the fold count: 0 means "never
	// completed a timed send-ack round trip", which rttEwmaSnapshot reports
	// as rttOk=false rather than as a fabricated 0ms. This matters because
	// exitScore sanitizes a bare 0 RTT into the BEST possible sub-score (see
	// routing_score.go) -- a channel with no measurement must read as
	// unmeasured, never as a perfect one. Guarded by stateLock like every
	// other field in this block.
	rttEwmaMillis   float64
	rttJitterMillis float64
	rttSamples      int

	// stalled is a test/diagnostic hook, set only by StallExit. Read on the
	// send hot path, so an atomic rather than the state lock.
	stalled atomic.Bool

	// silentProbeStreak counts consecutive probe passes this channel answered
	// with TOTAL silence: zero stage-B answers and zero dns resolutions, so
	// zero evidence of life across the whole pass. silentProbeTime is when the
	// latest such pass completed (unix nanos); any receive newer than it is
	// proof of life and acquits the streak (see probeSilentStreak). Atomics
	// because the writers are probe passes and the reader is the resize pass,
	// and neither should contend on stateLock for a counter.
	silentProbeStreak atomic.Int32
	silentProbeTime   atomic.Int64

	// dialFailureTimes and connectSuccessTimes are this channel's sliding-window
	// strike record: a timestamp appended on each intercepted dial failure, and
	// on each flow that first receives inbound data (a proven upstream connect).
	// Both are pruned to dialStrikeWindow on access. Guarded by stateLock. A
	// provider whose own upstream is refusing work -- a resold proxy over its
	// concurrency cap, an exhausted socket table -- shows failures with no
	// successes here, which dialStarved reports so the resize pass warns it out
	// of new-flow selection without destroying its established flows.
	//
	// dialFailureDestinations parallels dialFailureTimes entry for entry: the
	// destination ip each strike was dialing, pruned with the same prefix cut
	// (see pruneDialStrikesWithLock). dialStarved requires the surviving
	// strikes to span dialStarvedMinDestinations distinct destinations, so a
	// single polled-dead site retransmitting its dials cannot starve-warn an
	// exit by itself, while a real dud -- strikes across many destinations --
	// still convicts at full speed.
	dialFailureTimes        []time.Time
	dialFailureDestinations []string
	// dialFailureVersions parallels dialFailureTimes entry for entry with the
	// ip version of the failed dial, pruned with the same prefix cut, so the
	// v6 downgrade (ipv6DialStarvedWithLock) can judge v6 strikes apart from
	// v4 ones. connectSuccessVersions does the same for connectSuccessTimes.
	// Entries a legacy fixture never wrote read as v4 (dialStrikeVersion).
	dialFailureVersions    []int
	connectSuccessTimes    []time.Time
	connectSuccessVersions []int

	// ipFamilyDowngraded latches the local v6 downgrade (IPV6.md B5): set
	// once by maybeDowngradeIpv6, never cleared -- for the life of the
	// window this exit is v4-only whatever discovery said. familySwapVictim
	// marks an exit the resize pass chose to give up its slot to a
	// v6-capable candidate (selectFamilySwapVictim). Both atomics: read on
	// the placement path, written from the strike record and the resize pass.
	ipFamilyDowngraded atomic.Bool
	familySwapVictim   atomic.Bool
}

func newMultiClientChannel(
	ctx context.Context,
	args *multiClientChannelArgs,
	generator MultiClientGenerator,
	clientReceivePacketCallback clientReceivePacketFunction,
	dialFailureCallback dialFailureFunction,
	ingressSecurityPolicy SecurityPolicy,
	contractStatusCallback ContractStatusFunction,
	contractStatsCallback ContractStatsFunction,
	peerIdentityChangeCallback func(),
	performanceProfile *PerformanceProfile,
	settings *MultiClientSettings,
	// reliabilitySettingsFunc reads the parent's effective reliability config
	// so the blackhole bound can be retuned on a live connection. nil falls
	// back to settings, which the suite's bare-channel fixtures rely on.
	reliabilitySettingsFunc func() *ReliabilitySettings,
	// uplinkGateFunc, reliabilityMetricsFunc, flowCountFunc, resizeWakeFunc,
	// providerQualifiedFunc, receivingSiblingsFunc, and qualificationRefreshFunc
	// follow the same convention: injected parent/window state, nil-safe so bare
	// fixtures keep working
	uplinkGateFunc func(now time.Time) (stale bool, freshSince time.Time),
	reliabilityMetricsFunc func() *reliabilityMetrics,
	flowCountFunc func(*multiClientChannel) int,
	resizeWakeFunc func(),
	migrateFlowsFunc func(*multiClientChannel),
	providerQualifiedFunc func(MultiHopId) bool,
	receivingSiblingsFunc func(exclude *multiClientChannel) int,
	qualificationRefreshFunc func(MultiHopId),
) (*multiClientChannel, error) {
	cancelCtx, cancel := context.WithCancel(ctx)

	clientSettings := generator.NewClientSettings()
	clientSettings.SendBufferSettings.AckTimeout = settings.AckTimeout
	// This client is dedicated to the selected destination. Stamp every send
	// with the authenticated relationship before NewClient can emit its
	// initial ping; the platform's NetworkPeers batch may arrive later.
	clientSettings.DefaultTransferOpts.NetworkPeer = args.NetworkPeerDestination
	if performanceProfile != nil && performanceProfile.PostQuantumEncryption {
		// pqe: the user asked for post-quantum e2e, so this consumer runs
		// fail-closed (EncryptionModeRequired) — application traffic to a
		// destination that cannot establish a session is held and retried, never
		// sent in the clear. A provider that lacks session support therefore
		// carries no application data for this client rather than downgrading it
		// to plaintext the operator could read. (The provider side runs
		// Opportunistic so it keeps serving non-pqe consumers.)
		if clientSettings.EncryptionSettings == nil {
			clientSettings.EncryptionSettings = DefaultEncryptionSettings()
		}
		clientSettings.EncryptionSettings.Mode = EncryptionModeRequired
	}

	client, err := generator.NewClient(
		cancelCtx,
		&args.MultiClientGeneratorClientArgs,
		clientSettings,
	)
	if err != nil {
		cancel()
		return nil, err
	}
	// The selected destination is authenticated by the app's explicit Network
	// relationship before this client exists. Mark it before the initial ping
	// can open a P2P stream. Waiting for the first incoming Network signal is
	// too late on the active side: peer-connection admission happens while
	// constructing the outbound offer, so a full public pool can refuse the
	// selected peer before any trusted signal has a chance to return.
	if args.NetworkPeerDestination && 0 < args.Destination.Len() {
		client.webRtcManager.PrioritizePeer(args.Destination.Tail())
	}

	// sourceFilter := map[TransferPath]bool{
	//     Path{ClientId:args.DestinationId}: true,
	// }

	clientChannel := &multiClientChannel{
		ctx:    cancelCtx,
		cancel: cancel,
		transportMigrator: func() MultiClientGeneratorTransportMigrator {
			migrator, _ := generator.(MultiClientGeneratorTransportMigrator)
			return migrator
		}(),
		log:                         loggerOrDefault(settings.Log),
		args:                        args,
		clientReceivePacketCallback: clientReceivePacketCallback,
		dialFailureCallback:         dialFailureCallback,
		ingressSecurityPolicy:       ingressSecurityPolicy,
		performanceProfile:          performanceProfile,
		createTime:                  time.Now(),
		settings:                    settings,
		// the rotation lifetime is jittered per channel so channels created
		// in the same connect burst do not all drain in the same resize pass
		effectiveLifetime: jitterClientLifetime(settings.MaxClientLifetime),
		// sourceFilter: sourceFilter,
		client:                    client,
		eventBuckets:              []*multiClientEventBucket{},
		ip4DestinationSourceCount: map[Ip4Path]map[Ip4Path]int{},
		ip6DestinationSourceCount: map[Ip6Path]map[Ip6Path]int{},
		packetStats:               &clientWindowStats{log: loggerOrDefault(settings.Log)},
		reliabilitySettingsFunc:   reliabilitySettingsFunc,
		uplinkGateFunc:            uplinkGateFunc,
		reliabilityMetricsFunc:    reliabilityMetricsFunc,
		flowCountFunc:             flowCountFunc,
		resizeWakeFunc:            resizeWakeFunc,
		migrateFlowsFunc:          migrateFlowsFunc,
		providerQualifiedFunc:     providerQualifiedFunc,
		receivingSiblingsFunc:     receivingSiblingsFunc,
		qualificationRefreshFunc:  qualificationRefreshFunc,
		// affinityCount:             0,
		// affinityTime:              time.Time{},
	}

	// The window-owned callback includes this channel's identity so a typed
	// reliability failure can retire only its source. Direct constructor users
	// leave it nil and retain the original public status relay.
	effectiveContractStatusCallback := contractStatusCallback
	if args.contractStatus != nil {
		effectiveContractStatusCallback = func(status *ContractStatus) {
			args.contractStatus(clientChannel, status)
		}
	}
	// The callback performs only bounded Dispatch into the window-owned
	// coalescer plus constant-time reliability classification. Register it
	// directly so every exit does not allocate its own goroutine and
	// packet-sequence-sized status ring.
	contractStatusSub := client.ContractManager().addContractStatusDispatchCallback(effectiveContractStatusCallback)
	contractStatsSub := client.ContractManager().AddContractStatsCallback(contractStatsCallback)
	peerIdentitySub := client.EncryptionSessionManager().AddPeerIdentityChangeCallback(peerIdentityChangeCallback)
	go HandleError(func() {
		select {
		case <-cancelCtx.Done():
		case <-client.Done():
		}
		// Essential cleanup first, observer-facing events after. A stats
		// observer can remain parked (an app suspended mid-callback), and
		// CloseContractStats dispatches to it SYNCHRONOUSLY — ordering it
		// first let a parked observer block RemoveClientWithArgs, retaining
		// the platform identity and another client/transport record on every
		// peer churn. See TestMultiClientCleanupPrecedesBlockedObservers.
		contractStatusSub()
		peerIdentitySub()
		// Detach the platform transport before the synchronous local teardown.
		// The API generator retires the derived identity asynchronously after
		// Client and OOB cleanup have joined, so a slow Pion close never blocks
		// this path and the final contract-close controls retain valid auth.
		generator.RemoveClientWithArgs(client, &args.MultiClientGeneratorClientArgs)
		client.Cancel()
		// Fire the contract-close events for this client's still-open
		// contracts while the stats listener is still attached, or a removed
		// peer's contracts linger open forever in the contract-details UI.
		// The client is already cancelled — that is what woke this cleanup —
		// and CloseAllContractStats is the deterministic synchronous backstop
		// that emits regardless of the stopped epoch worker.
		client.CloseContractStats()
		contractStatsSub()
		// the removed client's established peers leave the aggregate set
		peerIdentityChangeCallback()
	}, cancel)

	go HandleError(clientChannel.detectBlackhole, cancel)
	go HandleError(clientChannel.ping, cancel)

	clientReceiveUnsub := client.AddReceiveCallback(clientChannel.clientReceive)
	clientChannel.clientReceiveUnsub = clientReceiveUnsub

	return clientChannel, nil
}

func (self *multiClientChannel) ClientId() Id {
	// bare fixture channels have no underlying client; identify by the args
	// id (which is the same id the real construction hands the client), or
	// as the zero id for the barest fixtures, rather than panic on a log
	// line -- same reasoning as Tier below
	if self.client == nil {
		if self.args != nil {
			return self.args.ClientId
		}
		return Id{}
	}
	return self.client.ClientId()
}

func (self *multiClientChannel) IsP2pOnly() bool {
	return self.args.MultiClientGeneratorClientArgs.P2pOnly
}

// rejectCandidateMissingEncryptionKey decides the
// EncryptionCapabilityPrefilter outcome for one out-of-band key fetch: reject
// only on the definitive "peer has published no key" answer under
// `EncryptionModeRequired`. Fetch errors (platform unreachable) and
// non-Required modes never reject — the prefilter only accelerates a failure
// the ping evaluation would reach anyway, it never admits a candidate.
func rejectCandidateMissingEncryptionKey(mode EncryptionMode, publicKey []byte, fetchErr error) bool {
	return mode == EncryptionModeRequired && fetchErr == nil && len(publicKey) == 0
}

// EncryptionCapabilityFetcher returns a one-shot fetcher for the channel
// destination's published client identity key, plus the channel client's
// encryption mode, for the window's EncryptionCapabilityPrefilter. The
// fetcher is minted from the client's configured out-of-band key fetcher
// factory (`EncryptionSettings.NewPeerClientPublicKeyFetcher`); nil when no
// factory is configured, the channel has no destination, or the channel is a
// bare fixture without an underlying client.
func (self *multiClientChannel) EncryptionCapabilityFetcher() (func(ctx context.Context) ([]byte, error), EncryptionMode) {
	if self.client == nil || self.args == nil {
		return nil, EncryptionModeOff
	}
	settings := self.client.EncryptionSessionManager().Settings()
	if settings == nil {
		return nil, EncryptionModeOff
	}
	mode := settings.Mode
	if settings.NewPeerClientPublicKeyFetcher == nil {
		return nil, mode
	}
	destinationId := self.args.Destination.Tail()
	if destinationId == (Id{}) {
		return nil, mode
	}
	return settings.NewPeerClientPublicKeyFetcher(destinationId), mode
}

func (self *multiClientChannel) Tier() int {
	// bare fixture channels have no args; rank them best rather than panic on
	// the selection path
	if self.args == nil {
		return 0
	}
	return self.args.DestinationStats.Tier
}

// quarantineMemoryDuration is the clean interval a survived quarantine's
// effectiveTier demerit outlives the episode. ~5 minutes: long enough that an
// exit which flaps in and out of quarantine cannot re-enter the top rank
// between episodes (episodes recur on the 20-30s verdict bounds, so a minute
// or two of memory would still let it win races in the gaps), short enough
// that a provider that genuinely recovered is not exiled for the rest of its
// up-to-an-hour lifetime. The interval alone is not sufficient -- expiry also
// requires a proven connect success since the lift (see the memory fields on
// multiClientChannel) -- so an idle benched exit does not drift back into the
// top rank on the clock alone with zero evidence it works now.
const quarantineMemoryDuration = 5 * time.Minute

// effectiveTier is the rank selection actually uses when
// EffectiveTierSelection is on: the platform's static Tier plus quantized
// demerits for what this channel is doing right now. The static tier is the
// platform's latency/speed prior; the demerits are the live evidence the
// prior cannot know:
//
//   - dial-starved (+2): the upstream is refusing work for new flows, so the
//     channel must fall behind every clean channel of the next tier. Expiry
//     is the strike record's own decay -- dialStarved self-clears as the
//     strikes age past the 60s dialStrikeWindow or a proven connect lands.
//
//   - quarantined, or survived-quarantine memory (+2): a soft blackhole
//     verdict fired against it (or recently had); see the memory fields for
//     the slow, evidence-gated expiry. One +2 covers both states -- the
//     memory IS the episode's demerit outliving the episode, not a second
//     offense.
//
//   - unproven qualification (+1): no probe pass (and no live receive
//     traffic) has proven this provider inside QualificationMaxAge. This is
//     the one POSITIVE-evidence demerit: it is the starting state of every
//     provider, decays the moment a pass or real traffic proves the dial
//     path, and can never be (re)imposed by any failure -- a failed probe
//     leaves qualification exactly as it was. Only applied when
//     ProviderProbe is on and the parent's lookup is wired
//     (providerQualifiedFunc), so bare fixtures and the kill switch both
//     read zero.
//
//   - unhealthy stats window (+1): the send/receive balance check
//     (windowStatsWithCoalesce) currently classifies the window unhealthy.
//     This reads the healthy flag the last stats coalesce computed -- one
//     bool under the already-held lock, at most one detectBlackhole poll
//     (~1.25s) stale -- because recomputing health here would need the full
//     bucket coalesce, and sendStalled would need a RouteManager read under
//     a foreign lock. A genuinely stalled channel is convicted and removed
//     by the stall watchdog within SendStallTimeout anyway, so a ranking
//     demerit for it would never be observable; the coalesced flag is the
//     honest cheap signal. The lastUnhealthyTime guard keeps a bare channel
//     that has never coalesced stats (healthy's zero value is false) at its
//     static tier.
//
// Demerits apply immediately -- the next selection pass reads them -- which
// is the ~1s demotion the design asks for; every one of them decays toward
// the static tier on its own slow, documented schedule. The +2 steps mean a
// single demerit pushes a channel behind the next full tier, while the +1
// health wobble only reorders within reach of adjacent tiers.
//
// Computed under this channel's own stateLock, a leaf: nothing else is
// locked inside. The settings read happens before the lock -- it is
// lock-free (an atomic load on the parent override, else a pure read of the
// constructed settings) -- so this method adds no lock-order edge. With
// EffectiveTierSelection off (including every fixture built with nil
// settings, whose zero-value ReliabilitySettings reads false) this is
// exactly Tier(), the A/B comparison point, and minTierClients stays
// signature-stable because the toggle is honored here rather than passed in.
func (self *multiClientChannel) effectiveTier() int {
	tier := self.Tier()
	reliabilitySettings := self.reliabilitySettings()
	if !reliabilitySettings.EffectiveTierSelection {
		return tier
	}

	// The qualification demerit (+1): an unproven or stale-qualified provider
	// ranks one step behind a proven peer of the same tier. POSITIVE-only, per
	// the whole probe design: unqualified is the state every provider starts
	// in, never a conviction -- which is also why it is +1 (reorders within
	// reach of adjacent tiers) rather than the +2 the evidence-of-failure
	// demerits carry. Gated on ProviderProbe so the A/B kill switch removes
	// the mechanism's every effect, and on the injected lookup so bare
	// fixtures (nil func) see no demerit at all.
	//
	// Read BEFORE this channel's stateLock: the lookup takes the parent
	// stateLock inside, and the parent lock must never nest under a leaf --
	// the same ordering contract flowCountFunc documents.
	unproven := false
	if reliabilitySettings.ProviderProbe && self.providerQualifiedFunc != nil {
		unproven = !self.providerQualifiedFunc(self.probeDestination())
	}

	now := time.Now()
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if unproven {
		tier += 1
	}

	if self.dialStarvedWithLock(now) {
		tier += 2
	}

	// decay the survived-quarantine memory on read: expiry requires the
	// clean interval AND the post-lift connect success together
	if self.survivedQuarantine && self.quarantineLiftConnectSeen &&
		quarantineMemoryDuration <= now.Sub(self.quarantineLiftTime) {
		self.survivedQuarantine = false
	}
	if self.quarantined || self.survivedQuarantine {
		tier += 2
	}

	if !self.healthy && !self.lastUnhealthyTime.IsZero() {
		tier += 1
	}

	// the suspect demerit (+1): a busy-flow liveness probe is in flight against
	// this channel and has not been answered. Concept ported from upstream main
	// e05ecee's suspect bit + orderClientsSuspectLast, absorbed into the rank we
	// already compute rather than a second ordering pass.
	//
	// +1 rather than +2 because the evidence is a QUESTION, not a finding: the
	// channel's flow acks are stalled and we are in the middle of asking whether
	// that means anything. One step is enough to steer a new flow to a clean
	// peer of the same tier for the ~1.5s the probe runs, without exiling the
	// channel behind the next full tier for a suspicion that acquits more often
	// than it convicts. Self-clearing by construction: the flag is cleared on
	// the probe ack and on the conviction (which removes the channel anyway), so
	// nothing here can outlive the probe.
	if self.busyProbeOutstanding {
		tier += 1
	}

	return tier
}

func (self *multiClientChannel) EstimatedByteCountPerSecond() ByteCount {
	// nil-safe like the other self.args readers in this file (see
	// clientReceive's `self.args != nil` guard): a channel built without args
	// -- every routing-scorer test fixture that only cares about flow counts
	// -- has nothing estimated yet, which is the same fact this returns for a
	// channel with args but no traffic.
	if self.args == nil {
		return ByteCount(0)
	}
	return self.args.EstimatedBytesPerSecond
}

// reliabilitySettings is the effective reliability config for this channel:
// the parent's runtime override when one is installed, else what the channel
// was constructed with. Safe on a channel built without the parent, which the
// suite's fixtures rely on.
func (self *multiClientChannel) reliabilitySettings() *ReliabilitySettings {
	if self.reliabilitySettingsFunc != nil {
		return self.reliabilitySettingsFunc()
	}
	return ReliabilitySettingsFrom(self.settings)
}

// uplinkGate reads the parent's tunnel-wide uplink gate for a verdict pass.
// nil func (a channel built without the parent) reads as gate off, matching
// the reliabilitySettingsFunc convention.
func (self *multiClientChannel) uplinkGate(now time.Time) (stale bool, freshSince time.Time) {
	if self.uplinkGateFunc != nil {
		return self.uplinkGateFunc(now)
	}
	return false, time.Time{}
}

// metrics reaches the parent's shared reliability counters. nil func or nil
// counters (a channel built without the parent) is fine: every counter method
// tolerates a nil receiver, so the counts are lost but nothing else changes.
func (self *multiClientChannel) metrics() *reliabilityMetrics {
	if self.reliabilityMetricsFunc != nil {
		return self.reliabilityMetricsFunc()
	}
	return nil
}

// flowCount reads the parent's live flow count for this channel. nil func (a
// channel built without the parent) reads as 0, and 0 flows means a verdict
// executes -- so bare fixtures keep the pre-change execute path their
// assertions were written against. Must be called with no channel or leaf
// lock held: the injected func takes the parent stateLock inside.
func (self *multiClientChannel) flowCount() int {
	if self.flowCountFunc == nil {
		return 0
	}
	return self.flowCountFunc(self)
}

// resizeWake nudges the owning window's resize pass, nil-safe for bare
// fixtures. Only ever an optimization -- the periodic pass reaches the same
// state through isWarning.
func (self *multiClientChannel) resizeWake() {
	if self.resizeWakeFunc != nil {
		self.resizeWakeFunc()
	}
}

// setQuarantined demotes this channel: a soft verdict fired while it carried
// live flows, so it leaves selection (via isWarning) with its established
// flows intact instead of being executed. Returns whether this call
// started (or, on a reason change, restarted) the episode, so the caller can
// log and wake the resize exactly once rather than on every verdict pass that
// re-confirms it. Re-asserting the same reason keeps the original start time
// -- quarantineStart must measure one continuous run of the same evidence,
// because that run's age is what eventually justifies executing after all.
func (self *multiClientChannel) setQuarantined(reason blackholeReason) bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if self.quarantined && self.quarantineReason == reason {
		return false
	}
	self.quarantined = true
	self.quarantineReason = reason
	self.quarantineStart = time.Now()
	return true
}

// clearQuarantine lifts the demotion. Called on receive-ack progress (see
// addReceiveAck, which applies the same clear under its already-held lock),
// by the bench-leak release in detectBlackhole, and available to tests.
func (self *multiClientChannel) clearQuarantine() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.clearQuarantineWithLock()
}

// clearQuarantineWithLock is the single lift implementation, shared by every
// clear site so none of them can forget the survived-quarantine memory. A lift
// from an actually-quarantined state records the memory: the flag that keeps
// the effectiveTier demerit applied, the lift time the clean-interval clock
// measures from, and a reset of the connect-seen stamp so promotion requires
// fresh positive evidence after THIS episode, not one left over from before
// it. Clearing an already-clear channel records nothing -- a no-op lift must
// not manufacture a demerit.
//
// must be called with stateLock
func (self *multiClientChannel) clearQuarantineWithLock() {
	if self.quarantined {
		self.survivedQuarantine = true
		now := time.Now()
		// this episode is now a completed cycle, so the NEXT bench (if any)
		// escalates one step; see quarantineReconvictions and benchDuration.
		// A no-op lift (already clear, the branch above did not run) must not
		// count -- there was no episode to have completed.
		//
		// QuarantineDampening on: decay-then-increment, not increment then
		// decay-on-read. quarantineReconvictionCount()'s read-side decay is a
		// pure lens over this field and quarantineLiftTime -- it can shrink
		// what a still-open episode reads, but it never writes anything
		// back, so on its own a count that decayed to a read of 0 during a
		// long quiet interval would resume climbing from the historical raw
		// peak the instant this channel reconvicts (raw+1, not 0+1) --
		// resurrecting exactly the convictions the decay exists to forgive.
		// Folding the SAME decay math into the stored value here, using the
		// OLD quarantineLiftTime read below before this line overwrites it,
		// makes the forgiveness durable across the count's next climb
		// instead of a view that evaporates on the next event. No second
		// timestamp field: the existing anchor need only be read once, right
		// before it moves.
		//
		// QuarantineDampening off: exactly self.quarantineReconvictions++,
		// byte-for-byte -- the dampening check keeps the stored counter from
		// ever depending on the clock, matching quarantineReconvictionCount's
		// own off-path guarantee.
		if self.reliabilitySettings().QuarantineDampening {
			self.quarantineReconvictions = quarantineReconvictionDecay(self.quarantineReconvictions, now.Sub(self.quarantineLiftTime)) + 1
		} else {
			self.quarantineReconvictions++
		}
		self.quarantineLiftTime = now
		self.quarantineLiftConnectSeen = false
	}
	self.quarantined = false
	self.quarantineReason = blackholeNone
	self.quarantineStart = time.Time{}
	// the episode is over, so the next one gets its own hand-off
	self.quarantineMigrated = false
}

// markQuarantineMigrateOnce latches the bench-time migration: true exactly
// once per quarantine episode.
func (self *multiClientChannel) markQuarantineMigrateOnce() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if !self.quarantined || self.quarantineMigrated {
		return false
	}
	self.quarantineMigrated = true
	return true
}

// emptiedByMigration reports whether this exit's flows left because the
// bench hand-off moved them. Consumed by verdictAction so flowlessness we
// caused cannot become the evidence that convicts.
func (self *multiClientChannel) emptiedByMigration() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.quarantined && self.quarantineMigrated
}

// migrateFlows runs the bench-time hand-off through the injected seam,
// latched to once per quarantine episode. nil func (bare fixtures, and any
// channel built without a parent) is a no-op, matching every other injected
// seam here.
func (self *multiClientChannel) migrateFlows() {
	if self.migrateFlowsFunc == nil {
		return
	}
	if !self.markQuarantineMigrateOnce() {
		return
	}
	self.migrateFlowsFunc(self)
}

func (self *multiClientChannel) isQuarantined() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.quarantined
}

// quarantineState reads the current episode's reason and start time
// (blackholeNone and zero when not quarantined), for the verdict pass to
// judge whether the same evidence has held continuously past the expiry
// bound.
func (self *multiClientChannel) quarantineState() (blackholeReason, time.Time) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if !self.quarantined {
		return blackholeNone, time.Time{}
	}
	return self.quarantineReason, self.quarantineStart
}

// quarantineReconvictionDecayInterval is the clean interval one reconviction
// step decays over: quarantineReconvictionCount() removes one step from the
// raw count for every whole interval elapsed since quarantineLiftTime, the
// stamp of the LAST completed bench-then-lift cycle (the same instant
// quarantineReconvictions itself advances on -- see clearQuarantineWithLock
// -- so a fresh conviction resets this clock for free). Set equal to
// quarantineMemoryDuration: the existing answer this file already gives for
// "how long counts as a clean interval" for the sibling survived-quarantine
// memory, reused rather than inventing a second opinion on the same
// question -- though the two remain independent constants should they ever
// need to diverge.
const quarantineReconvictionDecayInterval = quarantineMemoryDuration

// quarantineReconvictionDecay returns reconvictions with one step removed
// for every whole quarantineReconvictionDecayInterval elapsed is worth,
// floored at 0. A non-positive reconvictions or a non-positive elapsed (a
// clock problem, not evidence of a longer clean interval -- the same
// convention reentryScorePenalty's elapsed<0 clamp uses) both read as "no
// decay to apply" rather than propagating toward a negative result: a
// negative count feeding benchDuration or exitScore's StallEvents term is
// exactly the defect class this branch has already shipped twice (negative
// StallEvents scoring as a bonus, a zero RTT scoring best), so this must
// never produce one. Pure: no clock reads, no locks -- mirrors benchDuration
// and reentryScorePenalty.
func quarantineReconvictionDecay(reconvictions int, elapsed time.Duration) int {
	if reconvictions <= 0 {
		return 0
	}
	if elapsed <= 0 {
		return reconvictions
	}
	decayed := reconvictions - int(elapsed/quarantineReconvictionDecayInterval)
	if decayed < 0 {
		return 0
	}
	return decayed
}

// quarantineReconvictionCount reads the completed bench-then-lift cycle
// count benchDuration escalates on; see the field comment. With
// QuarantineDampening on, the raw count is decayed by elapsed quiet time
// since the last completed cycle (quarantineReconvictionDecay, above) before
// being returned, so a channel that reconvicted long ago and has been clean
// since reads a low count again rather than the historical peak. With the
// knob at its zero value (off) this returns the raw field verbatim and does
// not even read the clock -- so a default build's reading, and everything
// downstream of it (benchDuration, and exitMetricsSnapshot's StallEvents),
// stays byte-for-byte what it was before this decay existed.
func (self *multiClientChannel) quarantineReconvictionCount() int {
	// reliabilitySettings() is a bare atomic load (see its own comment),
	// never blocking, so reading it before the lock adds no new lock
	// ordering and keeps the locked section to the two fields below.
	dampening := self.reliabilitySettings().QuarantineDampening

	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if !dampening || self.quarantineLiftTime.IsZero() {
		return self.quarantineReconvictions
	}
	return quarantineReconvictionDecay(self.quarantineReconvictions, time.Now().Sub(self.quarantineLiftTime))
}

// quarantineReentryElapsed reports how long it has been since this channel's
// most recent quarantine lift, and whether a lift has ever been recorded at
// all -- ok is false for a channel that has never been quarantined, which
// must read as "no ramp applies" rather than as a zero (== just-released)
// elapsed. Reuses quarantineLiftTime, the same stamp the survived-quarantine
// effectiveTier memory already reads, so the re-entry ramp adds no new
// persistent state of its own.
func (self *multiClientChannel) quarantineReentryElapsed(now time.Time) (time.Duration, bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.quarantineLiftTime.IsZero() {
		return 0, false
	}
	return now.Sub(self.quarantineLiftTime), true
}

// reentryPenalty is the scored-placement convenience wrapper around
// reentryScorePenalty: 0 when ramp is disabled (QuarantineReentryRamp's
// zero-value-off) or this channel has never been quarantined, else the
// current point on the decay curve since its last lift.
func (self *multiClientChannel) reentryPenalty(ramp time.Duration) float64 {
	if ramp <= 0 {
		return 0
	}
	elapsed, ok := self.quarantineReentryElapsed(time.Now())
	if !ok {
		return 0
	}
	return reentryScorePenalty(elapsed, ramp)
}

// hasOutstandingSends reports whether this channel currently holds sends that
// were committed and not yet acknowledged -- the "talking" the uplink gate's
// degenerate-case guard counts across the windows. Takes only this channel's
// own stateLock.
func (self *multiClientChannel) hasOutstandingSends() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return 0 < self.packetStats.sendNackCount
}

// hasActiveTransport reports whether the transport set under this channel's
// client is non-empty. An empty set means the channel's own carrier is down:
// nothing it sends can leave the device and nothing can arrive, so its
// silence proves nothing about the provider. A bare fixture channel has no
// client, which reads as up -- absence of a client is not evidence of a down
// carrier, and the pre-gate behavior is what those fixtures assert.
func (self *multiClientChannel) hasActiveTransport() bool {
	if self.client == nil {
		return true
	}
	return self.client.RouteManager().HasActiveTransport()
}

// hasActiveUnreliableSendTransport distinguishes a registered carrier from
// one whose successful write proves delivery. Bare fixtures retain reliable
// semantics so existing classifier tests opt into the weaker evidence
// explicitly.
func (self *multiClientChannel) hasActiveUnreliableSendTransport() bool {
	if self.client == nil {
		return false
	}
	return self.client.RouteManager().HasActiveUnreliableSendTransport()
}

// setStalled makes the channel swallow packets without acknowledging or
// erroring. See RemoteUserNatMultiClient.StallExit.
func (self *multiClientChannel) setStalled(stalled bool) {
	self.stalled.Store(stalled)
}

// sendStalled reports whether this client has outstanding sends that have made
// no progress for at least stallTimeout: bytes committed, nothing acknowledged.
//
// This is the state a client is in when it accepts packets and never delivers
// them. Failure detection otherwise waits for AckTimeout (30s), because the
// client is neither erroring nor going quiet -- it looks busy. Every flow
// pinned to it is frozen for that whole window, and releasing the sender's
// retransmits does not help: they go to the same black hole. The flow can only
// recover once this client is out of the window, so the useful signal is "in
// flight, no acks", and it is available within seconds.
//
// 0 disables the check, restoring the previous AckTimeout-bounded behavior.
//
// A channel whose transport set is empty is never stalled: with no carrier
// registered the outstanding sends cannot make progress, so their age is
// evidence against the carrier, not the exit. Holding the verdict alone would
// not be enough -- a clock that kept aging through the outage convicts on the
// first poll after the transport re-registers, before the send sequences have
// even re-sent -- so while the carrier is down the clock is restarted on each
// observation, the stall-clock equivalent of the receive-verdict rebase in
// detectBlackhole. On re-registration the outstanding sends are re-sent by
// their sequences; either acks arrive (addSendAck restarts the clock, the
// normal path) or a genuinely dead exit earns a fresh, fully-aged verdict.
//
// A busy-probe ack refreshes the bar the same way an ack of the outstanding
// sends would, WITHOUT pretending one arrived: the clock is measured from the
// later of pendingSendTime and busyProbeAckTime, so an exit that answered a
// liveness probe gets another full stallTimeout to either deliver or be asked
// again, while pendingSendTime keeps its true meaning (when the current unacked
// run began) for every other reader. See the busyProbe* fields and
// busyLivenessProbe. With BusyProbe off nothing ever writes busyProbeAckTime
// and this is exactly the previous computation.
func (self *multiClientChannel) sendStalled(stallTimeout time.Duration) bool {
	if stallTimeout <= 0 {
		return false
	}

	// read the transport before taking stateLock: RouteManager has its own
	// mutex, and taking it under the channel lock would add a lock order this
	// file otherwise does not have
	transportDown := !self.hasActiveTransport()

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	// nothing outstanding means nothing to be stalled on -- an idle client is
	// not a broken one
	if self.packetStats.sendNackCount <= 0 || self.pendingSendTime.IsZero() {
		self.busyProbeSendFailures = 0
		self.stallHoldCounted = false
		return false
	}
	if transportDown {
		// see the doc comment: no progress is possible, so no verdict, and
		// the clock must not age while the carrier is out. This return is also
		// what keeps the busy probe off a channel with no carrier: the probe
		// only ever runs against a channel this method reported stalled, so
		// "no probe, no conviction while the transport is down" needs no
		// separate guard downstream.
		self.pendingSendTime = time.Now()
		self.busyProbeSendFailures = 0
		self.stallHoldCounted = false
		return false
	}

	stallStart := self.pendingSendTime
	if stallStart.Before(self.busyProbeAckTime) {
		stallStart = self.busyProbeAckTime
	}
	if time.Since(stallStart) < stallTimeout {
		// the stale episode is over (or has not matured), so the consecutive
		// unsendable-probe count starts fresh (without this, one transient
		// queue-full result could survive a healthy interval and make the next
		// episode convict on its first unsendable probe), and so does the
		// per-episode hold latch
		self.busyProbeSendFailures = 0
		self.stallHoldCounted = false
		return false
	}
	return true
}

// resetBusyProbeSendFailures clears the consecutive-unsendable-probe run.
// Called by the held branch of convictSendStalls: probes fired while the
// sibling-corroboration gate held the verdict are evidence about the phone,
// not the exit, and letting them accumulate would convict on "unsendable 2x"
// one pass after the gate opens.
func (self *multiClientChannel) resetBusyProbeSendFailures() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.busyProbeSendFailures = 0
}

// markStallHoldOnce latches the per-episode hold: true exactly once per stall
// episode, so the uplink-hold counter and log line keep per-episode semantics
// (matching detectBlackhole's heldCounted) instead of booking one per pass.
// The latch clears wherever the episode ends -- every sendStalled reset path.
func (self *multiClientChannel) markStallHoldOnce() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.stallHoldCounted {
		return false
	}
	self.stallHoldCounted = true
	return true
}

// markDrainMigrateOnce latches G-3's drain-time migration: true exactly once
// per channel, on the first drain pass.
func (self *multiClientChannel) markDrainMigrateOnce() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.drainMigrated {
		return false
	}
	self.drainMigrated = true
	return true
}

// busyProbeUnsendableConvictions is how many consecutive probes must fail to
// even reach the wire, inside one stale episode, before the stall verdict
// stands on that evidence alone. One is not enough: the send path being wedged
// full of the same unacked data the probe is investigating is what a congested
// exit looks like too, and a live one drains enough between polls for the next
// probe to queue.
const busyProbeUnsendableConvictions = 2

// errBusyProbeUnavailable marks a channel with no probe plumbing under it (a
// bare fixture, a channel whose client is gone). Distinct from a send failure
// on purpose: absence of the mechanism must fall back to the pre-probe verdict,
// never manufacture evidence.
var errBusyProbeUnavailable = errors.New("busy probe unavailable")

// busyProbeBudget is how long a probe waits for its ack. 0 derives
// max(1s, stallTimeout/2): half the bar that just tripped, floored so a very
// small stall bar cannot ask a question it does not wait for an answer to.
func busyProbeBudget(stallTimeout time.Duration, configured time.Duration) time.Duration {
	if 0 < configured {
		return configured
	}
	return max(time.Second, stallTimeout/2)
}

// busyProbeWriteTimeout is the snappy write bound for the probe's control ping.
// The whole point is to fail fast when the send queue is wedged full of the
// unacked data the probe is investigating -- blocking for the idle
// CPingWriteTimeout (15s) would push detection out past every path this exists
// to beat. A quarter of the ack budget, floored, and never longer than the idle
// write timeout the settings already carry.
func busyProbeWriteTimeout(budget time.Duration, cpingWriteTimeout time.Duration) time.Duration {
	writeTimeout := max(250*time.Millisecond, budget/4)
	if 0 < cpingWriteTimeout && cpingWriteTimeout < writeTimeout {
		writeTimeout = cpingWriteTimeout
	}
	return writeTimeout
}

// busyProbeVerdict is one liveness probe's conclusion about a stalled channel.
type busyProbeVerdict struct {
	// convict: the send-stall verdict stands this pass.
	convict bool
	// detail extends the conviction reason so a field capture can tell WHICH
	// probe outcome convicted. "" convicts with today's exact reason string,
	// which is what a channel with no probe plumbing gets.
	detail string
}

// addBusyProbeAck records positive liveness: the exit answered a control ping
// while its flow acks were stalled. Concept ported from upstream main
// e05ecee's addBusyProbeAck.
//
// This is the acquittal, and it is deliberately narrow. It does NOT touch
// pendingSendTime, the outstanding counts, or the stats window -- nothing about
// the stalled sends changed, and writing a send ack that never happened would
// corrupt the very accounting the next verdict is built on. It records the one
// new fact (the exit is reachable, at this instant) on its own field, and
// sendStalled reads that field as a second, equally valid restart point for its
// bar. The stale episode ends here, so the unsendable-probe run resets too.
func (self *multiClientChannel) addBusyProbeAck() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.busyProbeAckTime = time.Now()
	self.busyProbeOutstanding = false
	self.busyProbeSendFailures = 0
	self.stallHoldCounted = false
}

// setBusyProbeOutstanding arms/disarms the suspect demerit (see effectiveTier).
func (self *multiClientChannel) setBusyProbeOutstanding(outstanding bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.busyProbeOutstanding = outstanding
}

// busyProbeOutstandingNow reports whether a probe is in flight and unanswered.
func (self *multiClientChannel) busyProbeOutstandingNow() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.busyProbeOutstanding
}

// probeReceiveAge reports how many whole seconds since return traffic last
// arrived on this channel, -1 for never. The probe verdict line carries it so
// a field capture can tell a provider that is GONE from one that is
// demonstrably alive and simply cannot get the probe targets to answer.
//
// Only the RECEIVE clock: there is no send-ack timestamp on this channel
// (packetStats counts send acks but does not stamp them), and inventing a
// second age from a field that does not exist is how a diagnostic ends up
// reporting a number nobody produced.
// hasEverReceived reports whether this channel has ever delivered return
// traffic. Provider qualification is DESTINATION-keyed and survives channel
// incarnations, so a fresh dial to a qualified destination reads as proven
// while its own transport has delivered nothing -- this is the channel-level
// proof-of-delivery those consumers gate on.
func (self *multiClientChannel) hasEverReceived() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return !self.lastReceiveAckTime.IsZero()
}

func (self *multiClientChannel) probeReceiveAge() int64 {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if self.lastReceiveAckTime.IsZero() {
		return -1
	}
	return int64(time.Since(self.lastReceiveAckTime) / time.Second)
}

// recordProbeSilence counts one totally-silent probe pass: questions were
// asked (stage-B probes sent, usually a resolution stage too) and nothing at
// all came back. Called only by probeExit, which never runs two passes at
// once against the same channel.
func (self *multiClientChannel) recordProbeSilence() {
	self.silentProbeTime.Store(time.Now().UnixNano())
	self.silentProbeStreak.Add(1)
}

// recordProbeLife resets the silence streak: the pass carried at least one
// answer back -- a stage-B answer or a dns resolution -- which is proof the
// provider is on the network regardless of whether the pass PASSED. The
// reset is unconditional; a concurrent silent pass losing its increment to
// this store costs one retest interval of warning delay, nothing more.
func (self *multiClientChannel) recordProbeLife() {
	self.silentProbeStreak.Store(0)
}

// probeSilentStreak reads the current silence streak, first acquitting it if
// any receive landed after the latest silent pass: real return traffic is
// better evidence of life than a probe answer, and without this check an
// exit that revived between retests would stay warned until the prober got
// back around to it. The acquittal CAS deliberately loses to a concurrent
// recordProbeSilence -- newer silence outranks the older receive it raced.
func (self *multiClientChannel) probeSilentStreak() int {
	streak := self.silentProbeStreak.Load()
	if streak == 0 {
		return 0
	}
	self.stateLock.Lock()
	lastReceive := self.lastReceiveAckTime
	self.stateLock.Unlock()
	if !lastReceive.IsZero() && self.silentProbeTime.Load() < lastReceive.UnixNano() {
		if self.silentProbeStreak.CompareAndSwap(streak, 0) {
			return 0
		}
		streak = self.silentProbeStreak.Load()
	}
	return int(streak)
}

// hasRecentBusyProbeAck reports whether this channel answered a liveness
// probe inside window -- a return packet that crossed the uplink, which is
// what the stall-conviction sibling gate counts as proof the tunnel works.
func (self *multiClientChannel) hasRecentBusyProbeAck(window time.Duration) bool {
	if window <= 0 {
		return false
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return !self.busyProbeAckTime.IsZero() && time.Since(self.busyProbeAckTime) < window
}

// addBusyProbeSendFailure counts one probe that could not be queued and reports
// whether the run inside this stale episode is now long enough to convict on.
func (self *multiClientChannel) addBusyProbeSendFailure() int {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.busyProbeSendFailures += 1
	return self.busyProbeSendFailures
}

// sendBusyProbe puts the probe's control ping on the wire through the same
// plumbing the idle cping loop uses. The seam (busyProbeSendFunc) exists only
// so the probe's decisions can be driven from a test; production is always the
// SendDetailedMessage path below.
func (self *multiClientChannel) sendBusyProbe(timeout time.Duration, ackCallback func(error)) (bool, error) {
	if self.busyProbeSendFunc != nil {
		return self.busyProbeSendFunc(timeout, ackCallback)
	}
	if self.client == nil || self.args == nil {
		return false, errBusyProbeUnavailable
	}
	return self.SendDetailedMessage(&protocol.IpPing{}, timeout, ackCallback)
}

// busyLivenessProbe asks a stalled exit whether it is still there, and reports
// whether the send-stall verdict should stand.
//
// Concept ported from upstream main e05ecee: their CPingBusyStaleTimeout /
// busyStale / addBusyProbeAck run this idea inside their ping loop against
// their own staleness classifier. Here the trigger stays OURS -- the sendStalled
// bar, evaluated by the window's stall watchdog -- and only the question is
// borrowed. What it buys: "bytes committed, nothing acknowledged" is the same
// observation for an exit that died and for an exit whose upstream is saturated
// or whose send window is full of the very data the stall is about. The control
// ping rides the transport rather than the blocked flow, so a live exit answers
// it in a couple of rtts. Asking costs one packet; the answer separates the two
// cases the passive signal cannot.
//
// WHERE THIS MAY INTERPOSE, and nowhere else:
//   - the sendStalled path only. The no-send-ack blackhole verdict has its own
//     carrier-aware evidence gate: it stays fast on a reliable carrier, while
//     an unreliable carrier defers to Transfer's bounded Ack retry lifetime.
//     Transport-up does not prove DATAGRAM delivery, even when some other
//     datagrams are getting through. The transport-down hold is an exculpation,
//     not a conviction, and delaying it would be meaningless;
//     the cping loop must keep its exact behavior, where an unanswered ping ends
//     the ping loop and convicts NOTHING (see the comment in ping() and
//     TestCpingTimeoutSourceAnchors).
//   - and only after sendStalled already returned true, which is what carries
//     the transport gate: a channel with no active carrier never reads stalled,
//     so it is never probed and never convicted here.
//
// The outcomes:
//   - ack inside the budget: the exit is alive despite the stalled flow acks.
//     Record the liveness (addBusyProbeAck, which refreshes the stall bar
//     without forging a send ack) and do not convict.
//   - budget expires: convict, with the reason naming the probe. One fresh
//     budget is granted first if the wait itself was suspended (see
//     schedulerPauseDetected) -- a probe armed before a doze must not convict on
//     wake, when neither the exit's answer nor this waiter had a cpu.
//   - the probe cannot be queued twice in one stale episode: convict. Once is
//     not evidence (a congested exit drains between polls); twice, while the
//     same data sits unacked, is.
//   - no probe plumbing at all: convict exactly as before the probe existed.
//
// Locking: takes only this channel's stateLock, in short sections, never across
// the send or the wait.
func (self *multiClientChannel) busyLivenessProbe(budget time.Duration) busyProbeVerdict {
	if budget <= 0 {
		return busyProbeVerdict{convict: true}
	}

	probeDone := make(chan error, 1)
	cpingWriteTimeout := time.Duration(0)
	if self.settings != nil {
		cpingWriteTimeout = self.settings.CPingWriteTimeout
	}
	writeTimeout := busyProbeWriteTimeout(budget, cpingWriteTimeout)
	success, err := self.sendBusyProbe(writeTimeout, func(err error) {
		// buffered by one and non-blocking: the waiter below may already have
		// given up, and the transport's ack callback must never block on us
		select {
		case probeDone <- err:
		default:
		}
	})
	if errors.Is(err, errBusyProbeUnavailable) {
		return busyProbeVerdict{convict: true}
	}
	if err != nil || !success {
		// could not even be queued. see busyProbeUnsendableConvictions
		if failures := self.addBusyProbeSendFailure(); busyProbeUnsendableConvictions <= failures {
			return busyProbeVerdict{
				convict: true,
				detail: fmt.Sprintf(
					"liveness probe unsendable %dx",
					busyProbeUnsendableConvictions,
				),
			}
		} else {
			return busyProbeVerdict{
				detail: fmt.Sprintf("liveness probe unsendable %dx, deferred", failures),
			}
		}
	}

	self.setBusyProbeOutstanding(true)
	// every return below either acks (addBusyProbeAck clears it) or convicts;
	// the disarm covers both plus the ctx-done escape, so the suspect demerit
	// can never outlive the probe that armed it
	defer self.setBusyProbeOutstanding(false)
	self.metrics().busyProbeSent()

	tolerance := self.reliabilitySettings().SchedulerPauseTolerance
	waitStart := time.Now()
	budgetRefreshed := false
	timer := time.NewTimer(budget)
	defer timer.Stop()

	// a bare fixture may carry no context. A nil channel never fires in a
	// select, which is the right reading of "nothing has cancelled this".
	var done <-chan struct{}
	if self.ctx != nil {
		done = self.ctx.Done()
	}

	for {
		select {
		case <-done:
			// the channel is going away for its own reasons; no verdict to add
			return busyProbeVerdict{detail: "liveness probe abandoned: channel closing"}
		case err := <-probeDone:
			if err != nil {
				// the send sequence gave up on the ping itself. that is a
				// failed question, not an answer, so it convicts -- but it is
				// named distinctly from a plain timeout so a capture can tell
				// "no reply" from "the transport refused to carry the reply"
				return busyProbeVerdict{
					convict: true,
					detail:  fmt.Sprintf("liveness probe failed: %s", err),
				}
			}
			self.addBusyProbeAck()
			self.metrics().busyProbeAcquitted()
			return busyProbeVerdict{detail: "liveness probe answered"}
		case <-timer.C:
			if !budgetRefreshed && schedulerPauseDetected(time.Since(waitStart), budget, tolerance) {
				// the host was suspended while this probe was in flight: the
				// exit's answer and this waiter were both off the cpu, so the
				// expiry says nothing about the exit. Grant the SAME probe one
				// fresh budget. Once only -- a second expiry past a full budget
				// of real running time is a real timeout, and an unbounded
				// refresh would let a flapping scheduler suspend the verdict
				// forever.
				budgetRefreshed = true
				waitStart = time.Now()
				timer.Reset(budget)
				loggerOrDefault(self.log).Infof("%s\n", relEvent(
					"busy_probe",
					"exit", self.ClientId(),
					"outcome", "refreshed",
					"reason", "scheduler_pause",
					"budget", budget,
				))
				continue
			}
			return busyProbeVerdict{
				convict: true,
				detail:  fmt.Sprintf("liveness probe timed out after %s", budget),
			}
		}
	}
}

// hasRecentSendAck reports whether the provider acknowledged one of this
// channel's sends inside window — the own-liveness half of the comparative
// connect cut's evidence. A single-destination window (a user-selected
// network peer) has no sibling to prove the pool works, so its own send acks
// are the only proof available that the peer process is alive while a
// connect goes unanswered. Same lock discipline as hasRecentReceive.
func (self *multiClientChannel) hasRecentSendAck(window time.Duration) bool {
	if window <= 0 {
		return false
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return !self.lastSendAckTime.IsZero() && time.Since(self.lastSendAckTime) < window
}

// rttEwmaAlpha weights each new round-trip sample against the running
// average. 1/8 is the classic TCP SRTT smoothing constant (RFC 6298) --
// fast enough to track a real path change within a handful of samples,
// slow enough that one spiky packet cannot swing the estimate into
// implausible territory.
const rttEwmaAlpha = 0.125

// addSendRttSample folds one measured send-to-ack round trip into this
// channel's smoothed RTT/jitter estimate. Called only from
// SendDetailedWithAck's ackCallback, on a successful ack, with the elapsed
// time since that specific packet's own send call -- never from a shared
// clock like pendingSendTime, which measures the OLDEST outstanding send and
// would misattribute the round trip under concurrent in-flight packets.
//
// A non-positive duration is not a real measurement (clock skew, or a test
// driving the callback with a synthetic time) and is dropped rather than
// folded in: exitScore reads a 0 RTT as the BEST possible sub-score, so
// recording one here would be the same trap exitMetricsSnapshot's ok gating
// exists to avoid, just moved one layer down.
func (self *multiClientChannel) addSendRttSample(rtt time.Duration) {
	if rtt <= 0 {
		return
	}
	millis := float64(rtt) / float64(time.Millisecond)

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if self.rttSamples == 0 {
		self.rttEwmaMillis = millis
	} else {
		diff := millis - self.rttEwmaMillis
		self.rttEwmaMillis += rttEwmaAlpha * diff
		absDiff := math.Abs(diff)
		if self.rttSamples == 1 {
			// the first-ever deviation: seeded, not smoothed, the same
			// convention RFC 6298 uses to initialize RTTVAR
			self.rttJitterMillis = absDiff
		} else {
			self.rttJitterMillis += rttEwmaAlpha * (absDiff - self.rttJitterMillis)
		}
	}
	self.rttSamples += 1
}

// rttEwmaSnapshot reads the current smoothed RTT/jitter estimate.
// rttOk is false for a channel that has never completed a timed send-ack
// round trip (every bare test fixture, and every real channel before its
// first ack) -- callers MUST treat that as "no data", never as a 0ms
// measurement (see exitMetricsSnapshot). jitterOk additionally requires a
// SECOND sample: one round trip alone has no deviation to report, and
// reporting 0 there would trip the same zero-is-best trap RTT itself is
// guarded against.
func (self *multiClientChannel) rttEwmaSnapshot() (rttMillis float64, jitterMillis float64, rttOk bool, jitterOk bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.rttSamples == 0 {
		return 0, 0, false, false
	}
	if self.rttSamples == 1 {
		return self.rttEwmaMillis, 0, true, false
	}
	return self.rttEwmaMillis, self.rttJitterMillis, true, true
}

// hasRecentReceive reports whether return traffic arrived on this channel
// inside window -- the per-sibling half of the comparative connect cut's
// evidence (see RemoteUserNatMultiClient.receivingChannelCount). Takes only
// this channel's own stateLock, the same discipline hasOutstandingSends
// follows, so the parent's sweep never nests locks.
func (self *multiClientChannel) hasRecentReceive(window time.Duration) bool {
	if window <= 0 {
		return false
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return !self.lastReceiveAckTime.IsZero() && time.Since(self.lastReceiveAckTime) < window
}

// receivingSiblings reads the parent's receive-side sibling count. nil func (a
// channel built without the parent) reads as zero, which leaves the full
// BlackholeConnectTimeout in place -- matching the reliabilitySettingsFunc
// convention and the safe direction for a bar that shortens a removal.
func (self *multiClientChannel) receivingSiblings() int {
	if self.receivingSiblingsFunc != nil {
		return self.receivingSiblingsFunc(self)
	}
	return 0
}

// warnCause is WHY a channel is warned -- see the warning/warnCause fields.
type warnCause int

const (
	warnNone warnCause = iota
	// warnDraining: past its lifetime, healthy, retiring. Rotation policy,
	// not a health verdict.
	warnDraining
	// warnStarved: the provider's own upstream is failing dials.
	warnStarved
	// warnCapacity: the window's source-count ulimit -- the exit is full by
	// policy, not failing. Distinct from starved because a full exit and a
	// dial-failing one are different facts to the developer screen (review
	// finding, 2026-08-03: the ulimit branch displayed as "starved").
	warnCapacity
	// warnUnhealthy: a stats or blackhole verdict was demoted or deferred
	// against it, or the rank-derived health warning fired.
	warnUnhealthy
	// warnSilent: ProbeSilenceWarnStreak consecutive probe passes answered
	// with total silence -- the provider device has very likely left the
	// network (dozed, switched networks, app killed). Placement only; removal
	// stays traffic-based.
	warnSilent
	// warnFamilySwap: chosen to give up its slot to a v6-capable candidate
	// while the window is at capacity with no exit that can carry v6
	// (IPV6.md B2, selectFamilySwapVictim). Rotation policy, not a health
	// verdict, exactly like draining.
	warnFamilySwap
)

func (self warnCause) String() string {
	switch self {
	case warnDraining:
		return "draining"
	case warnStarved:
		return "starved"
	case warnCapacity:
		return "capacity"
	case warnUnhealthy:
		return "unhealthy"
	case warnSilent:
		return "silent"
	case warnFamilySwap:
		return "family-swap"
	default:
		return ""
	}
}

// setWarning sets or clears the resize pass's warning together with its
// cause. The cause is stored only while warned -- clearing the warning
// clears it, so a stale cause can never describe a healthy channel.
//
// Returns the PREVIOUS cause and whether this call changed it, so the caller
// can log the transition. The resize pass rewrites this every pass for every
// client, so only transitions may be logged: the state itself would be a few
// hundred identical lines a minute.
func (self *multiClientChannel) setWarning(warning bool, cause warnCause) (warnCause, bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	previous := self.warnCause
	if !self.warning {
		previous = warnNone
	}

	self.warning = warning
	if warning {
		self.warnCause = cause
	} else {
		self.warnCause = warnNone
	}

	current := self.warnCause
	if !self.warning {
		current = warnNone
	}
	return previous, previous != current
}

// logWarnTransition emits one line when a client's warning cause changes,
// including to and from none. This is the answer to "why did new flows stop
// choosing this exit", which before it existed lived only behind V(1) --
// compiled off in the field -- so a capture showing 6 of 15 exits warned
// could not say whether they were retiring, out of capacity, failing dials,
// or suspected. Bounded by construction: transitions only, and the resize
// pass reaches a steady state within a pass or two.
func (self *multiClientWindow) logWarnTransition(
	client *multiClientChannel,
	previous warnCause,
	changed bool,
	stats *clientWindowStats,
) {
	if !changed {
		return
	}
	from, to := previous.String(), client.warningCause().String()
	if from == "" {
		from = "none"
	}
	if to == "" {
		to = "none"
	}
	sourceCount := 0
	if stats != nil {
		sourceCount = stats.sourceCount
	}
	loggerOrDefault(self.log).Infof("%s\n", relEvent(
		"warn",
		"exit", client.ClientId(),
		"from", from,
		"to", to,
		"flows", self.flowCount(client),
		// the two inputs behind the two causes a reader most often wants to
		// separate: capacity is about source count, starvation about dials
		"sources", sourceCount,
		"dialfails", client.dialFailureCount(),
	))
}

// warningCause reads the current cause; warnNone when not warned.
func (self *multiClientChannel) warningCause() warnCause {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if !self.warning {
		return warnNone
	}
	return self.warnCause
}

// isWarning reports whether new flows should avoid this channel: the resize
// pass's recomputed warning, OR an active quarantine. The OR is what makes
// quarantine exclusion automatic for both consumers of this method -- the
// parent's affinity/selection reads and the window's ordered-clients offer --
// without either needing to know quarantine exists.
func (self *multiClientChannel) isWarning() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.warning || self.quarantined
}

// donorVerdict is affinityDonorEligible's answer, kept three-valued so the
// caller can count the interesting refusal (a group scattered by its donor's
// quarantine) separately from the ordinary ones.
type donorVerdict int

const (
	// donorEligible: inherit
	donorEligible donorVerdict = iota
	// donorRefused: warned (any cause) -- the pre-G-1 behavior for every
	// exclusion, and still the behavior for resize warnings
	donorRefused
	// donorQuarantineScattered: refused ONLY because of quarantine -- with
	// group-follow on this would have been (or was, before staleness) a
	// follow, so the refusal is the scatter G-1 exists to count and prevent
	donorQuarantineScattered
	// donorQuarantineFollowed: quarantined, and inherited anyway under
	// group-follow with fresh receive evidence
	donorQuarantineFollowed
)

// affinityDonorEligible decides whether this channel may donate to a new flow
// of an affinity group it already hosts. Warnings and quarantine both refuse.
// In particular, a sustained no-receive verdict must close admission before
// the parent callback invalidates the affinity tables; otherwise a fresh H1
// handshake can land in that small transition window and inherit the exact
// exit the verdict proved receive-silent. The follow arguments remain in the
// signature for source/settings compatibility but no longer override a
// quarantine for a newly-created flow.
func (self *multiClientChannel) affinityDonorEligible(followQuarantine bool, window time.Duration) donorVerdict {
	_ = followQuarantine
	_ = window
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.warning {
		return donorRefused
	}
	if self.quarantined {
		return donorQuarantineScattered
	}
	return donorEligible
}

// dialStrikeWindow is how long a dial failure or a connect success counts
// toward the starvation decision. Matches the recovery tracker's horizon: long
// enough that a burst of failures is not forgotten between resize passes, short
// enough that a provider that recovers stops being warned within a minute.
const dialStrikeWindow = 60 * time.Second

// dialStarvedFailureThreshold is how many intercepted dial failures (with zero
// successes) mark a channel starved. A couple of failures are noise -- a site
// that is genuinely down, a transient blip; a sustained run with nothing
// connecting is the resold-proxy-over-cap signature this warns on.
const dialStarvedFailureThreshold = 3

// dialStarvedMinDestinations is how many distinct destination ips the
// surviving strikes must span before they count as starvation. A single
// destination retransmitting failed dials -- a polled-dead site, a tracker
// the network blackholes -- can supply any number of strikes by itself, and
// before this requirement it could starve-warn a healthy exit (and, through
// the effectiveTier demerit, demote it in rank) for one website's death. Two
// distinct destinations is the smallest span that makes the strikes evidence
// about the exit rather than about a destination, and a genuinely starved
// upstream fails dials for everything, so the real dud still convicts at
// full speed.
const dialStarvedMinDestinations = 2

// pruneStrikeTimes drops timestamps strictly before horizon. The slices are
// append-only and time-ordered, so a prefix scan is sufficient.
func pruneStrikeTimes(times []time.Time, horizon time.Time) []time.Time {
	i := 0
	for i < len(times) && times[i].Before(horizon) {
		i++
	}
	return times[i:]
}

// pruneDialStrikesWithLock prunes the dial-failure record to the strike
// window, cutting the same prefix from the times and the parallel
// destinations so the two stay entry-aligned. The destination cut is clamped:
// a fixture that hand-injects dialFailureTimes without destinations (the
// pre-existing test idiom) must prune cleanly rather than slice out of range
// -- production always appends the two together via addDialFailure.
//
// must be called with stateLock
func (self *multiClientChannel) pruneDialStrikesWithLock(horizon time.Time) {
	i := 0
	for i < len(self.dialFailureTimes) && self.dialFailureTimes[i].Before(horizon) {
		i += 1
	}
	if 0 < i {
		self.dialFailureTimes = self.dialFailureTimes[i:]
		self.dialFailureDestinations = self.dialFailureDestinations[min(i, len(self.dialFailureDestinations)):]
		self.dialFailureVersions = self.dialFailureVersions[min(i, len(self.dialFailureVersions)):]
	}
}

// addDialFailure records one intercepted dial failure for this channel,
// alongside the destination ip the failed dial was for (see
// dialFailureDestinations for why the destination matters) and the ip
// version of the dial (see dialFailureVersions).
func (self *multiClientChannel) addDialFailure(destination string, ipVersion int) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	now := time.Now()
	self.pruneDialStrikesWithLock(now.Add(-dialStrikeWindow))
	self.dialFailureTimes = append(self.dialFailureTimes, now)
	self.dialFailureDestinations = append(self.dialFailureDestinations, destination)
	self.dialFailureVersions = append(self.dialFailureVersions, ipVersion)
}

// addConnectSuccess records one proven upstream connect for this channel (a
// flow that received its first inbound data). A single success in the window
// clears dialStarved. It also stamps the survived-quarantine memory: a proven
// connect is the positive evidence promotion back to the static tier
// requires (see the memory fields on multiClientChannel).
func (self *multiClientChannel) addConnectSuccess(ipVersion int) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	now := time.Now()
	self.pruneConnectSuccessesWithLock(now.Add(-dialStrikeWindow))
	self.connectSuccessTimes = append(self.connectSuccessTimes, now)
	self.connectSuccessVersions = append(self.connectSuccessVersions, ipVersion)
	if self.survivedQuarantine {
		self.quarantineLiftConnectSeen = true
	}
}

// dialStarved reports whether this channel's upstream is refusing work: at
// least dialStarvedFailureThreshold intercepted dial failures spanning at
// least dialStarvedMinDestinations distinct destinations, and zero proven
// connects, inside the sliding window. It gates new-flow selection only (the
// resize pass warning and the effectiveTier demerit); it must never feed the
// removal decision, because a dial-starved provider's established flows are
// its only working asset and destroying them is the bug this whole design
// exists to avoid.
func (self *multiClientChannel) dialStarved() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	return self.dialStarvedWithLock(time.Now())
}

// dialStarvedWithLock is dialStarved under an already-held stateLock, for
// effectiveTier, which computes every demerit in one locked section.
//
// must be called with stateLock
func (self *multiClientChannel) dialStarvedWithLock(now time.Time) bool {
	horizon := now.Add(-dialStrikeWindow)
	self.pruneDialStrikesWithLock(horizon)
	self.pruneConnectSuccessesWithLock(horizon)
	if len(self.dialFailureTimes) < dialStarvedFailureThreshold || 0 < len(self.connectSuccessTimes) {
		return false
	}
	// the distinct-destination span. Fewer recorded destinations than the
	// required span can never satisfy it (this is also the nil-safe path for
	// fixtures that inject dialFailureTimes without destinations). The
	// surviving strike count is small -- bounded by the failure rate over a
	// 60s window -- so a linear first-differs scan finds "at least 2
	// distinct" without allocating.
	if len(self.dialFailureDestinations) < dialStarvedMinDestinations {
		return false
	}
	distinct := false
	for _, destination := range self.dialFailureDestinations[1:] {
		if destination != self.dialFailureDestinations[0] {
			distinct = true
			break
		}
	}
	return distinct
}

// dialFailureCount is the number of intercepted dial failures in the current
// window, for ExitInfo.DialFailureCount. Prunes on access like dialStarved.
func (self *multiClientChannel) dialFailureCount() int {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.pruneDialStrikesWithLock(time.Now().Add(-dialStrikeWindow))
	return len(self.dialFailureTimes)
}

// func (self *multiClientChannel) UpdateAffinity() {
// 	self.stateLock.Lock()
// 	defer self.stateLock.Unlock()

// 	self.affinityCount += 1
// 	self.affinityTime = time.Now()
// }

// func (self *multiClientChannel) ClearAffinity() {
// 	self.stateLock.Lock()
// 	defer self.stateLock.Unlock()

// 	self.affinityCount = 0
// 	self.affinityTime = time.Time{}
// }

// func (self *multiClientChannel) MostRecentAffinity() (int, time.Time) {
// 	self.stateLock.Lock()
// 	defer self.stateLock.Unlock()

// 	return self.affinityCount, self.affinityTime
// }

func (self *multiClientChannel) Send(parsedPacket *parsedPacket, timeout time.Duration) bool {
	success, err := self.SendDetailed(parsedPacket, timeout)
	return success && err == nil
}

func (self *multiClientChannel) SendDetailed(parsedPacket *parsedPacket, timeout time.Duration) (bool, error) {
	return self.SendDetailedWithAck(
		parsedPacket,
		timeout,
		self.ipPacketTransferAckRequired(parsedPacket.ipPath),
	)
}

// Returns the Transfer recovery policy used for this client's IP packets.
// Routing gates that rely on reliable commit must consult this same decision.
func (self *multiClientChannel) ipPacketTransferAckRequired(ipPath *IpPath) bool {
	allowDirect := self.performanceProfile != nil &&
		self.performanceProfile.AllowDirect
	return ipPacketTransferAckRequired(
		ipPath,
		allowDirect,
		self.settings.UdpCollapsePrevention,
	)
}

// TCP always retains end-to-end Transfer acknowledgement. A reliable carrier
// only guarantees delivery for the lifetime of that carrier; it cannot prove
// delivery across a disconnect or route replacement. UDP/ICMP retain datagram
// semantics on direct paths and when collapse prevention explicitly selects
// them on a non-direct path.
func ipPacketTransferAckRequired(
	ipPath *IpPath,
	allowDirect bool,
	udpCollapsePrevention bool,
) bool {
	if ipPacketTransferAckForRequest(ipPath, false) {
		return true
	}
	if allowDirect {
		return false
	}
	switch ipPath.Protocol {
	case IpProtocolUdp, IpProtocolIcmp:
		return !udpCollapsePrevention
	default:
		return true
	}
}

// Applies the non-negotiable part of IP recovery policy at the final
// packet-to-Transfer boundary. TCP and unclassified packets cannot be demoted
// to NoAck by a caller; an affirmative request for any protocol is preserved.
func ipPacketTransferAckForRequest(ipPath *IpPath, requested bool) bool {
	return requested || ipPath == nil || ipPath.Protocol == IpProtocolTcp
}

// sendMultiClientRaceAttempt shares the packet read-only into a race
// attempt: on send the sequence owns the share; on refusal the share is
// returned here so the racing caller keeps sole ownership of the original.
func sendMultiClientRaceAttempt(
	client *multiClientChannel,
	packet []byte,
	ipPath *IpPath,
	timeout time.Duration,
) bool {
	sharedPacket := &parsedPacket{
		packet: MessagePoolShareReadOnly(packet),
		ipPath: ipPath,
	}
	if client.SendWithAck(sharedPacket, timeout, true) {
		return true
	}
	MessagePoolReturn(sharedPacket.packet)
	return false
}

// Shares every group member into one candidate attempt. Admission transfers
// all shares; refusal returns all shares while originals stay with the race.
func sendMultiClientGroupRaceAttempt(
	client *multiClientChannel,
	sendPacketGroup *parsedPacketGroup,
	timeout time.Duration,
) bool {
	sharedGroup := &parsedPacketGroup{
		packets:              make([]parsedPacket, len(sendPacketGroup.packets)),
		ipPath:               sendPacketGroup.ipPath,
		pin:                  sendPacketGroup.pin,
		byteCount:            sendPacketGroup.byteCount,
		transportAttribution: sendPacketGroup.transportAttribution,
	}
	for packetIndex := range sendPacketGroup.packets {
		sharedGroup.packets[packetIndex] = sendPacketGroup.packets[packetIndex]
		sharedGroup.packets[packetIndex].packet = MessagePoolShareReadOnly(
			sendPacketGroup.packets[packetIndex].packet,
		)
	}
	if client.SendGroupWithAck(sharedGroup, timeout, true) {
		return true
	}
	for packetIndex := range sharedGroup.packets {
		MessagePoolReturn(sharedGroup.packets[packetIndex].packet)
	}
	return false
}

func (self *multiClientChannel) SendWithAck(parsedPacket *parsedPacket, timeout time.Duration, ack bool) bool {
	success, err := self.SendDetailedWithAck(parsedPacket, timeout, ack)
	return success && err == nil
}

// Admits one exact-flow group using the channel's normal recovery policy.
func (self *multiClientChannel) SendGroup(sendPacketGroup *parsedPacketGroup, timeout time.Duration) bool {
	success, err := self.SendGroupDetailed(sendPacketGroup, timeout)
	return success && err == nil
}

// Returns the group admission error beside the bounded-send result.
func (self *multiClientChannel) SendGroupDetailed(
	sendPacketGroup *parsedPacketGroup,
	timeout time.Duration,
) (bool, error) {
	return self.SendGroupDetailedWithAck(
		sendPacketGroup,
		timeout,
		self.ipPacketTransferAckRequired(sendPacketGroup.ipPath),
	)
}

// Admits a whole group with one Transfer callback and exact ownership.
func (self *multiClientChannel) SendGroupWithAck(
	sendPacketGroup *parsedPacketGroup,
	timeout time.Duration,
	ack bool,
) bool {
	success, err := self.SendGroupDetailedWithAck(sendPacketGroup, timeout, ack)
	return success && err == nil
}

// Builds every frame before the one atomic logical-group admission. Failure
// leaves all caller packet owners untouched.
func (self *multiClientChannel) SendGroupDetailedWithAck(
	sendPacketGroup *parsedPacketGroup,
	timeout time.Duration,
	ack bool,
) (bool, error) {
	// This is the final packet-to-Transfer boundary. Keep TCP recovery
	// end-to-end even if a lower-level or future caller explicitly requests
	// NoAck; carrier reliability cannot commit an item across disconnect.
	ack = ipPacketTransferAckForRequest(sendPacketGroup.ipPath, ack)
	if self.sendGroupForTest != nil {
		return self.sendGroupForTest(sendPacketGroup, timeout, ack)
	}
	frames := make([]*protocol.Frame, len(sendPacketGroup.packets))
	for packetIndex := range sendPacketGroup.packets {
		frame, err := ipPacketToProviderFrame(
			sendPacketGroup.packets[packetIndex].packet,
			self.settings.ProtocolVersion,
		)
		if err != nil {
			for _, builtFrame := range frames[:packetIndex] {
				if !builtFrame.Raw {
					MessagePoolReturn(builtFrame.MessageBytes)
				}
			}
			self.addError(err)
			return false, err
		}
		frames[packetIndex] = frame
	}

	self.addSendGroup(sendPacketGroup)
	if self.stalled.Load() {
		for packetIndex, frame := range frames {
			if !frame.Raw {
				MessagePoolReturn(frame.MessageBytes)
			}
			MessagePoolReturn(sendPacketGroup.packets[packetIndex].packet)
		}
		return true, nil
	}

	var completionOnce sync.Once
	ackCallback := func(err error) {
		completionOnce.Do(func() {
			self.observePacketGroupTransferCompletion(sendPacketGroup, ack, err)
		})
	}
	opts := []any{scheduleIpFlow(sendPacketGroup.ipPath)}
	if self.performanceProfile != nil && self.performanceProfile.AllowDirect {
		opts = append(opts, ForceStream())
	}
	if !ack {
		opts = append(opts, NoAck())
	}
	if sendPacketGroup.transportAttribution != nil {
		opts = append(
			opts,
			observeTransportWrite(sendPacketGroup.transportAttribution.observe),
		)
	}
	var success bool
	var err error
	if self.sendTransferForTest != nil {
		success, err = self.sendTransferForTest(ackCallback)
	} else {
		success, err = self.client.sendMultiHopGroupWithTimeoutDetailed(
			frames,
			self.args.Destination,
			ackCallback,
			timeout,
			opts...,
		)
	}
	if err != nil || !success {
		self.addSendAbandonedGroup(sendPacketGroup)
		for _, frame := range frames {
			if !frame.Raw {
				MessagePoolReturn(frame.MessageBytes)
			}
		}
		return success, err
	}
	for packetIndex, frame := range frames {
		if !frame.Raw {
			MessagePoolReturn(sendPacketGroup.packets[packetIndex].packet)
		}
	}
	return true, nil
}

func (self *multiClientChannel) SendDetailedWithAck(parsedPacket *parsedPacket, timeout time.Duration, ack bool) (bool, error) {
	// Match the grouped boundary above. TCP is never converted to Transfer
	// NoAck, regardless of the selected carrier or the caller's hint.
	ack = ipPacketTransferAckForRequest(parsedPacket.ipPath, ack)
	if frame, err := ipPacketToProviderFrame(parsedPacket.packet, self.settings.ProtocolVersion); err != nil {
		self.addError(err)
		return false, err
	} else {
		packetByteCount := ByteCount(len(parsedPacket.packet))
		self.addSend(packetByteCount, parsedPacket.ipPath)
		// The packet-local clock is needed only when a peer Ack will complete a
		// real round trip. A NoAck callback observes the initial route write.
		var sendTime time.Time
		if ack {
			sendTime = self.packetTransferNow()
		}

		// a stalled exit swallows the packet: reported sent, never acknowledged,
		// and crucially no error -- an error would reset the flow immediately,
		// which is the opposite of the state being reproduced. see StallExit.
		//
		// this must come *after* addSend. addSend is what starts the stall
		// clock (pendingSendTime) and counts the send as outstanding, and
		// sendStalled treats a client with nothing outstanding as idle rather
		// than broken. returning before it made a stalled exit invisible to
		// the detector built to catch stalled exits: on device, a stall went
		// unnoticed for 34s while the flows on it were dead. a provider that
		// really blackholes takes this path and is accounted for, so swallowing
		// here is also the faithful simulation -- the packet is committed and
		// simply never acknowledged.
		if self.stalled.Load() {
			if !frame.Raw {
				MessagePoolReturn(frame.MessageBytes)
			}
			MessagePoolReturn(parsedPacket.packet)
			return true, nil
		}

		var completionOnce sync.Once
		ackCallback := func(err error) {
			completionOnce.Do(func() {
				self.observePacketTransferCompletion(packetByteCount, sendTime, ack, err)
			})
		}

		opts := []any{scheduleIpFlow(parsedPacket.ipPath)}
		if self.performanceProfile != nil && self.performanceProfile.AllowDirect {
			opts = append(opts, ForceStream())
		}
		if !ack {
			opts = append(opts, NoAck())
		}
		var success bool
		if self.sendTransferForTest != nil {
			success, err = self.sendTransferForTest(ackCallback)
		} else {
			success, err = self.client.SendMultiHopWithTimeoutDetailed(
				frame,
				self.args.Destination,
				ackCallback,
				timeout,
				opts...,
			)
		}
		// ownership: `parsedPacket.packet` is consumed on success and stays with the
		// caller on any failure. The wrapped (!raw) marshal buffer is internal and
		// must be freed on any failure; for raw frames the frame bytes ARE the
		// caller's packet, so they are never freed here on failure.
		//
		// both failure returns below mean the transport refused the send before
		// accepting it into a sequence -- an error, or backpressure (success ==
		// false) -- so the ackCallback above is unreachable for it: every
		// acceptance path in SendSequence.Pack (and the loopback fast path)
		// returns success, and the callback is only ever invoked for packs that
		// entered a sequence. the accounting armed by addSend must therefore be
		// unwound here, or the refusal is booked as an outstanding send that
		// can never be acked, aging pendingSendTime toward a sendStalled
		// verdict on an innocent exit. no double-undo is possible: an accepted
		// send never takes these returns, and a refused send never reaches the
		// ack callback.
		if err != nil {
			self.addSendAbandoned(packetByteCount)
			if !frame.Raw {
				MessagePoolReturn(frame.MessageBytes)
			}
			return success, err
		}
		if success {
			if !frame.Raw {
				MessagePoolReturn(parsedPacket.packet)
			}
		} else {
			self.addSendAbandoned(packetByteCount)
			if !frame.Raw {
				MessagePoolReturn(frame.MessageBytes)
			}
		}
		return success, err
	}
}

// Records the terminal disposition of one packet admitted to Transfer.
//
// For an Ack-required packet, a terminal error means the reliable Transfer
// sequence failed and remains hard evidence against the provider. For a NoAck
// packet, the callback is only the initial route-write disposition: a timeout
// means this datagram was dropped under transient carrier backpressure, not
// that the provider failed. Poisoning endErr in that case removes the whole
// provider channel and resets unrelated TCP flows that share it. A successful
// NoAck write retires its outstanding accounting but cannot contribute RTT:
// no peer acknowledgement completed a round trip.
func (self *multiClientChannel) observePacketTransferCompletion(
	packetByteCount ByteCount,
	sendTime time.Time,
	ack bool,
	err error,
) {
	if err == nil {
		self.addSendAck(packetByteCount)
		if ack {
			self.addSendRttSample(self.packetTransferNow().Sub(sendTime))
		}
		return
	}
	if ack || !errors.Is(err, errTransferRouteWriteTimeout) {
		self.addError(err)
		return
	}
	self.addSendAbandoned(packetByteCount)
}

// Returns the packet completion clock. Production uses the monotonic wall
// clock; focused tests can provide exact send and completion instants.
func (self *multiClientChannel) packetTransferNow() time.Time {
	if self.packetTransferNowForTest != nil {
		return self.packetTransferNowForTest()
	}
	return time.Now()
}

// Applies the same reliable-vs-datagram failure boundary to a logical packet
// group. One failed NoAck group is retired packet-accurately without giving a
// transient route-write timeout provider-wide shared fate.
func (self *multiClientChannel) observePacketGroupTransferCompletion(
	sendPacketGroup *parsedPacketGroup,
	ack bool,
	err error,
) {
	if err == nil {
		self.addSendAckGroup(sendPacketGroup)
		return
	}
	if ack || !errors.Is(err, errTransferRouteWriteTimeout) {
		self.addError(err)
		return
	}
	self.addSendAbandonedGroup(sendPacketGroup)
}

func (self *multiClientChannel) SendDetailedMessage(message proto.Message, timeout time.Duration, ackCallback func(error)) (bool, error) {
	if frame, err := ToFrame(message, self.settings.ProtocolVersion); err != nil {
		return false, err
	} else {
		var opts []any
		if self.performanceProfile != nil && self.performanceProfile.AllowDirect {
			opts = append(opts, ForceStream())
		}
		return self.client.SendMultiHopWithTimeoutDetailed(
			frame,
			self.args.Destination,
			ackCallback,
			timeout,
			opts...,
		)
	}
}

func (self *multiClientChannel) Done() <-chan struct{} {
	return self.ctx.Done()
}

func (self *multiClientChannel) Destination() MultiHopId {
	return self.args.Destination
}

// blackholeReason names which signal removed a provider. The three used to
// report an identical error string, so a field capture could not tell them
// apart -- the discriminating counts live only on a V(1) line that is off in
// the field. Naming them makes the next capture decisive.
type blackholeReason string

const (
	blackholeNone blackholeReason = ""
	// the provider acknowledges nothing: it is not there
	blackholeNoSendAck blackholeReason = "no-send-ack"
	// the provider acknowledges our sends but no destination data comes back
	blackholeNoReceiveAck blackholeReason = "no-receive-ack"
	// no connection was ever established back
	blackholeNoReceiveSyn blackholeReason = "no-receive-syn"
)

// blackholeReasonFromStats decides whether a provider looks like a blackhole,
// and by which signal.
//
// The signals differ in strength, so they get different bars:
//
//   - The provider acknowledges nothing. It is not there. Unambiguous, acted
//     on at blackholeTimeout.
//   - The provider acknowledges our sends but no destination data comes back.
//     It is demonstrably alive, and may simply be carrying a flow waiting on a
//     slow origin. Removing an exit destroys every flow pinned to it, not just
//     the quiet one, so this needs receiveTimeout -- a longer bar.
//
// Sharing one 5s bound removed 44 providers out of 44 on mainnet, about one
// every 18s under load, every one still acknowledging sends (up to 602 sends /
// 222KB). receiveTimeout of 0 disables the weaker check, leaving only the
// unambiguous one.
//
// IMPORTANT: receiveTimeout has a hard ceiling of roughly
// StatsWindowDuration + StatsWindowBucketDuration (~31s at production
// constants). firstSendNackTime is derived from surviving buckets, and
// coalesceEventBuckets drops every bucket older than StatsWindowDuration, so
// sendNackAge can never exceed that. A receiveTimeout above the ceiling is
// silently equivalent to 0 -- it does not error, it just never fires. See
// TestBlackholeReceiveTimeoutIsReachable.
//
// blackholeGates carries the exculpatory evidence that can make a verdict
// inadmissible for one evaluation pass. The zero value is "no gating", which
// is the pre-gate behavior and what bare fixtures exercise.
type blackholeGates struct {
	// transportDown: this channel's own transport set is empty. Its silence
	// proves nothing -- nothing it sends can leave the device -- so every
	// verdict is held, including no-send-ack.
	transportDown bool
	// unreliableSendTransport means an active writer can accept and then lose
	// a complete Transfer frame. On that carrier, no-send-ack is never
	// independent proof against the provider: the missing Pack or its Ack may be
	// the carrier loss Transfer is recovering. Transfer's Ack timeout is the
	// bounded hard verdict after its retransmit policy has been exhausted.
	unreliableSendTransport bool
	// uplinkStale: tunnel-wide ingress silence past the uplink gate. The
	// phone's own uplink is the prime suspect, so the receive verdicts --
	// which convict purely on silence -- are held. No-send-ack remains
	// independent proof on a reliable carrier; the carrier-aware gate above
	// handles it separately on an unreliable carrier.
	uplinkStale bool
	// receiveFreshSince is when the last inadmissible-evidence epoch ended:
	// the end of the last uplink-stale epoch, or this channel's last
	// transport re-registration, whichever is later. The receive-branch ages
	// count from max(firstSend*Time, receiveFreshSince), so verdicts held
	// across an epoch restart their clocks instead of all firing at once on
	// unfreeze -- the 7-exits-in-79s incident was exactly the no-rebase
	// failure mode, one held verdict maturing after another during one
	// continuous silence. Zero means never gated and changes nothing.
	receiveFreshSince time.Time
	// minReceiveAckDestinations is the corroboration bar for the
	// no-receive-ack verdict: the window must contain sends toward at least
	// this many distinct destination paths (windowStats.sendDestinationCount)
	// or the verdict does not fire at all. One destination silent through an
	// alive, acking exit is what a single dead website looks like, not what
	// a broken exit looks like. Strictly speaking this is a config threshold
	// rather than exculpatory evidence, but it lives here so the pure
	// decision keeps its signature and the zero value keeps reproducing the
	// pre-change single-destination behavior (0 or 1 = no bar), which every
	// existing fixture relies on. Below the bar the verdict is not "held"
	// (nothing is reported to the held counters) -- the evidence is
	// insufficient, not inadmissible, exactly like a disabled receiveTimeout.
	// Fed from MinBlackholeDestinations by detectBlackhole.
	minReceiveAckDestinations int
}

// unansweredConnectSince is the clock the no-receive-syn branch matures on:
// the start of the CURRENT unanswered connect attempt
// (firstUnansweredSendSynTime — lifetime, armed by the first SYN of an
// attempt, held through retransmits, cleared by any received SYN), with the
// windowed firstSendSynTime as a fallback for stats built before the marker
// was recorded.
//
// The windowed clock alone cannot carry this branch: buckets are trimmed at
// StatsWindowDuration, so like receiveTimeout (see the doc above) the syn age
// has a hard ceiling of ~StatsWindowDuration + StatsWindowBucketDuration — and
// with BlackholeConnectTimeout at the same scale, the evidence ages out at the
// very moment the bar matures. The verdict then fires only in the sliver where
// a surviving retransmit bucket happens to be old enough, which at production
// constants is almost never: a silent route holds its flows for the whole bar
// and usually past it. The fallback under-reports (that is the defect) but
// never over-reports, so fixtures that predate the marker keep their meaning.
//
// The fallback is skipped once a SYN was answered in the window
// (receiveSynCount > 0): the marker being zero then means "answered", not
// "unknown", and the windowed clock must not resurrect a dead attempt.
func unansweredConnectSince(windowStats *clientWindowStats) time.Time {
	if !windowStats.firstUnansweredSendSynTime.IsZero() {
		return windowStats.firstUnansweredSendSynTime
	}
	if windowStats.receiveSynCount <= 0 {
		return windowStats.firstSendSynTime
	}
	return time.Time{}
}

// rebaseReceiveClock moves a receive-branch clock forward onto the end of the
// last inadmissible-evidence epoch, so a verdict held across an outage restarts
// its clock instead of maturing the instant the hold lifts. Zero freshSince
// (never gated) changes nothing. Shared by blackholeReasonFromStats and
// comparativeConnectTimeout so the two can never disagree about when a clock
// starts.
func rebaseReceiveClock(clockStart time.Time, receiveFreshSince time.Time) time.Time {
	if !receiveFreshSince.IsZero() && clockStart.Before(receiveFreshSince) {
		return receiveFreshSince
	}
	return clockStart
}

// comparativeReceivingSiblings is how many OTHER exits must be receiving before
// this exit's silence is judged against the short connect bar. Two, not one: a
// single receiving sibling is one data point, and the whole claim being made is
// "the pool is demonstrably fine, so the fault is local to this exit". Two
// independent exits carrying return traffic is the smallest sample that makes
// that a statement about the pool rather than about one lucky peer -- the same
// reasoning MinBlackholeDestinations applies to destinations, and the same
// reasoning the uplink gate's own degenerate-case guard applies at two.
const comparativeReceivingSiblings = 2

// comparativeConnectTimeout picks the bar the no-receive-syn branch matures at.
//
// Concept ported from upstream main e05ecee's
// BlackholeConnectComparativeTimeout. The full BlackholeConnectTimeout is
// patient because unanswered syns are ambiguous; when two OTHER exits are
// visibly receiving right now, they resolve the ambiguity -- the uplink
// delivers and the pool works, so an exit that has established nothing is alone
// in its trouble and its flows should not wait out the full bar.
//
// Pure but for the injected count, so the decision is testable and so the
// SWEEP IS BOUNDED: the sibling count is only taken once the syn age is already
// past the comparative bar and still short of the full one, which is the only
// interval where the answer can change anything. Outside that window this is a
// few comparisons, so the ~1.25s verdict cadence does not pay for a channel
// walk it cannot use.
//
// Every gate still applies downstream: this only moves WHEN the branch matures.
// The verdict still passes through blackholeReasonFromStats (uplink gate,
// transport gate, clock rebase) and still costs whatever verdictAction says
// (quarantine against live flows, execute against none).
// recentOwnSendAck is the second, sibling-free proof (nil = disabled): the
// exit is acknowledging THIS channel's sends at the transfer layer while the
// connect goes unanswered. A send ack is produced by the peer's receive
// sequence, so it proves the local path, the platform route, and the peer
// process are all alive — the ambiguity the full bar exists for is resolved by
// the exit's own signal, no pool required. This is what a single-destination
// window has: a user-selected network peer has exactly one candidate by
// construction and can never produce comparativeReceivingSiblings, so without
// this arm it could only ever reach the full bar — 30s to reclaim a dead
// route on a first-class connection, far past any TCP connect budget. The
// observed shape it cuts: a fresh window client whose sends are acked while
// its return path is dead (e.g. return-contract starvation) — every flow
// pinned to it stranded for the full bar, the tunnel dead on arrival.
func comparativeConnectTimeout(
	now time.Time,
	windowStats *clientWindowStats,
	connectTimeout time.Duration,
	comparativeTimeout time.Duration,
	receiveFreshSince time.Time,
	receivingSiblings func() int,
	recentOwnSendAck func() bool,
) time.Duration {
	if comparativeTimeout <= 0 || connectTimeout <= comparativeTimeout {
		// off, or configured above the bar it exists to shorten
		return connectTimeout
	}
	if windowStats == nil {
		return connectTimeout
	}
	unansweredSince := unansweredConnectSince(windowStats)
	if unansweredSince.IsZero() {
		return connectTimeout
	}
	// the branch this shortens also requires nothing received at all; an exit
	// with receive progress is not the case the cut exists for, and counting
	// siblings for it would be wasted work
	if 0 < windowStats.receiveSynCount || 0 < windowStats.receiveAckCount {
		return connectTimeout
	}
	synAge := now.Sub(rebaseReceiveClock(unansweredSince, receiveFreshSince))
	if synAge < comparativeTimeout || connectTimeout <= synAge {
		// below the short bar there is nothing to cut short yet; past the full
		// bar the ordinary verdict is already firing. Either way the answer
		// cannot change the outcome, so the sweep is skipped.
		return connectTimeout
	}
	if receivingSiblings != nil && comparativeReceivingSiblings <= receivingSiblings() {
		return comparativeTimeout
	}
	if recentOwnSendAck != nil && recentOwnSendAck() {
		return comparativeTimeout
	}
	return connectTimeout
}

// loadScaledMinDestinations is the G-6 corroboration scaling: the effective
// distinct-destination requirement for the soft no-receive-ack verdict is
// max(configuredMin, flowCount/perFlows). A loaded exit has more in-flight
// questions and more ways to look briefly silent, so the busier it is, the
// broader the silence must be before suspicion is admissible -- the
// 2026-08-03 field session benched five 22-24-flow exits on soft evidence
// and acquitted every one. perFlows <= 0 disables the scaling (the flat
// pre-change behavior). Pure, so the table is testable.
func loadScaledMinDestinations(configuredMin int, flowCount int, perFlows int) int {
	if perFlows <= 0 {
		return configuredMin
	}
	if scaled := flowCount / perFlows; configuredMin < scaled {
		return scaled
	}
	return configuredMin
}

// Split out from detectBlackhole so the decision can be tested against real
// window stats rather than restated in a test, where it could drift from what
// actually ships.
//
// Returns the verdict to act on, and separately the verdict that would have
// fired this pass but was held by a gate (blackholeNone for either when there
// is none). Held verdicts are reported rather than swallowed so the caller
// can count them -- a gate that silently ate verdicts would be untunable.
func blackholeReasonFromStats(
	now time.Time,
	windowStats *clientWindowStats,
	blackholeTimeout time.Duration,
	receiveTimeout time.Duration,
	connectTimeout time.Duration,
	gates blackholeGates,
) (reason blackholeReason, held blackholeReason) {
	// verdict applies the gates to a reason that is otherwise firing.
	// receiveBranch marks the verdicts that always convict purely on silence.
	// They are subject to the uplink gate. No-send-ack is instead held whenever
	// an unreliable send carrier makes a missing Transfer Ack ambiguous; the
	// Transfer send sequence owns the bounded retry and terminal timeout. The
	// transport gate covers everything -- see blackholeGates.
	verdict := func(firing blackholeReason, receiveBranch bool) (blackholeReason, blackholeReason) {
		if gates.transportDown {
			return blackholeNone, firing
		}
		if firing == blackholeNoSendAck && gates.unreliableSendTransport {
			return blackholeNone, firing
		}
		if gates.uplinkStale && receiveBranch {
			return blackholeNone, firing
		}
		return firing, blackholeNone
	}

	// receiveClockStart rebases a receive-branch clock onto the end of the
	// last gated epoch; see blackholeGates.receiveFreshSince. the no-send-ack
	// age below deliberately does not pass through this.
	receiveClockStart := func(clockStart time.Time) time.Time {
		return rebaseReceiveClock(clockStart, gates.receiveFreshSince)
	}

	if !windowStats.firstSendNackTime.IsZero() {
		sendNackAge := now.Sub(windowStats.firstSendNackTime)

		if blackholeTimeout <= sendNackAge && windowStats.sendAckCount <= 0 {
			return verdict(blackholeNoSendAck, false)
		}
		receiveNackAge := now.Sub(receiveClockStart(windowStats.firstSendNackTime))
		if 0 < receiveTimeout && receiveTimeout <= receiveNackAge && windowStats.receiveAckCount <= 0 &&
			// the corroboration bar: one destination's silence must not
			// convict an exit that is demonstrably alive (it is acking the
			// sends). 0/1 keeps the pre-change behavior. deliberately not a
			// "held" verdict -- insufficient evidence, not inadmissible; see
			// blackholeGates.minReceiveAckDestinations
			(gates.minReceiveAckDestinations <= 1 ||
				gates.minReceiveAckDestinations <= windowStats.sendDestinationCount) {
			return verdict(blackholeNoReceiveAck, true)
		}
	}

	if unansweredSince := unansweredConnectSince(windowStats); !unansweredSince.IsZero() {
		// unanswered syns alone must not remove an exit whose established
		// traffic is flowing. the syn-acks here are built by the provider only
		// after its upstream dial succeeds, so "syns out, none back" conflates
		// three cases the counter cannot tell apart: the provider cannot dial,
		// the destination silently drops connections (datacenter ip ranges are
		// widely dropped by anti-bot infrastructure), or the destination is
		// merely slow. in two of those the exit is innocent -- and in all
		// three, its established flows are its only working asset. on device
		// this branch removed an exit moving 48 packets / 8.7KB of return
		// traffic because ~18 syns (a handful of destinations, retransmitting)
		// went unanswered, destroying 276 working connections.
		//
		// connect trouble on a live exit is instead handled per-flow: the
		// unanswered flow re-races onto another exit (dial-failure signalling
		// and the client-side inference that feeds the same path), and the
		// dial-strike warning stops new flows choosing the exit. removal here
		// is reserved for an exit that has established nothing at all.
		if connectTimeout <= now.Sub(receiveClockStart(unansweredSince)) &&
			windowStats.receiveSynCount <= 0 &&
			windowStats.receiveAckCount <= 0 {
			return verdict(blackholeNoReceiveSyn, true)
		}
	}

	return blackholeNone, blackholeNone
}

// verdictActionKind is what detectBlackhole does with a verdict that survived
// the evidence gates. Split from the gate decision (blackholeReasonFromStats)
// on purpose: the gates judge whether the evidence is admissible at all, this
// judges what an admissible verdict is allowed to cost -- and both are pure
// so the field behavior is testable without restating it.
type verdictActionKind int

const (
	// no verdict is firing this pass
	verdictActionNone verdictActionKind = 0
	// remove the exit now, exactly as before this work: hard evidence
	// (no-send-ack), soft-demote disabled, or a soft verdict against a
	// flowless exit, where execution costs nothing
	verdictActionExecute verdictActionKind = 1
	// a soft verdict against a loaded exit: quarantine it -- out of selection
	// via isWarning, established flows kept running, removal deferred
	verdictActionQuarantine verdictActionKind = 2
	// the same soft evidence has held continuously since the quarantine began,
	// past the sustained bound, with zero receive progress (any receive ack
	// clears the quarantine, so an old quarantineStart proves the silence was
	// unbroken): remove, tagged distinctly so field logs can tell an expired
	// quarantine from an immediate execution
	verdictActionExecuteExpired verdictActionKind = 3
)

// benchDuration is the RFC 2439-style flap-damping schedule for how long a
// quarantine holds before the expiry escape (verdictActionExecuteExpired,
// below) may act on it. dampening off returns base unconditionally -- the
// constant StatsWindowKeepUnhealthyDuration hold every episode got before
// this task, and what QuarantineDampening's zero value preserves exactly.
// dampening on escalates 60s -> 120s -> 240s by reconvictions (this
// channel's count of prior completed bench-then-lift cycles, see
// quarantineReconvictionCount), capped at the third step: real field
// evidence showed the SAME exits bench/lift 4-5 times in 10-20 minutes with
// zero effect from bench migration (movable=0 on all 20 attempts) -- the
// escalation exists to make repeat churn cost more without holding a
// genuinely-recovered exit's bench open indefinitely. base is accepted but
// ignored when dampening is on: the schedule is fixed by the literature, not
// tunable per-deployment through the pre-existing knob. Pure: no clock
// reads, no locks.
func benchDuration(reconvictions int, base time.Duration, dampening bool) time.Duration {
	if !dampening {
		return base
	}
	steps := []time.Duration{60 * time.Second, 120 * time.Second, 240 * time.Second}
	i := reconvictions
	if i < 0 {
		i = 0
	}
	if i >= len(steps) {
		i = len(steps) - 1
	}
	return steps[i]
}

// reentryPenaltyWeight is the ceiling of the re-entry ramp's score
// subtraction, at the instant a quarantine lifts (elapsed==0). exitScore's
// own stall term alone can already cost up to 3.0 (see exitScore's
// stallPenalty cap), so 1.0 is a meaningful push against a freshly released
// exit without being able to outweigh a genuinely large health gap on its
// own -- a just-lifted exit can still win if it is clearly the best
// candidate, it just does not win a close tie-break on the strength of a
// lift that happened a second ago.
const reentryPenaltyWeight = 1.0

// reentryScorePenalty is the asymmetric-re-entry half of the flap-damping
// pair: a released exit does not return to full scored-placement standing
// the instant its quarantine lifts (fast to leave selection, slow to return
// to it in full), it returns at reduced weight that decays LINEARLY to zero
// once `ramp` has elapsed since the lift. ramp<=0 disables the ramp entirely
// (returns 0 for any input), which is QuarantineReentryRamp's zero-value-off
// contract -- the legacy, pre-this-task behavior, where a lifted quarantine
// carries no score penalty at all. elapsed<0 is clamped to 0 (full penalty)
// rather than read as "already decayed" -- a caller passing a negative
// elapsed has a clock problem, not evidence of a longer-than-possible clean
// interval. This is a temporary SCORE adjustment only: it never touches
// quarantined/warning state, membership, or which exits are convicted --
// see reentryPenalty, its only caller, and scoredPlacementReorder, its
// only consumer. Pure: no clock reads, no locks.
func reentryScorePenalty(elapsed time.Duration, ramp time.Duration) float64 {
	if ramp <= 0 || elapsed >= ramp {
		return 0
	}
	if elapsed < 0 {
		elapsed = 0
	}
	return reentryPenaltyWeight * float64(ramp-elapsed) / float64(ramp)
}

// verdictAction decides between executing, quarantining, and expiring a
// blackhole verdict. The core invariant it encodes: an exit carrying live
// flows may only be closed on hard evidence. Of the verdicts that reach here,
// only no-send-ack is hard -- the provider acknowledges nothing while its
// transport is provably up. The receive-branch verdicts (no-receive-ack,
// no-receive-syn) convict on silence, which a loaded exit can be innocent of,
// so against flows they demote; no-receive-syn takes the same path because
// even though its blast radius is nominally unestablished flows, an
// established flow can sit window-idle long enough for its history to age out
// of the stats -- the flow-count gate is free safety there.
//
// quarantinedSince must be the start of the current quarantine episode only
// when its reason matches this verdict (zero otherwise): the expiry bound
// measures one continuous run of the same evidence, not the sum of different
// suspicions. quarantineExpiry 0 disables the expiry escape entirely,
// deferring removal until the exit is flowless.
func verdictAction(
	reason blackholeReason,
	softDemote bool,
	flowCount int,
	quarantinedSince time.Time,
	now time.Time,
	quarantineExpiry time.Duration,
	// emptiedByMigration: this exit's flows left because WE moved them at
	// bench time, not because the exit drained naturally
	emptiedByMigration bool,
) verdictActionKind {
	if reason == blackholeNone {
		return verdictActionNone
	}
	// no-send-ack is the one hard verdict here and is untouched by the
	// demote: it must stay as fast as it was, because it covers the provider
	// that is simply gone
	if reason == blackholeNoSendAck {
		return verdictActionExecute
	}
	if !softDemote {
		// the pre-change behavior, kept reachable for A/B: soft verdicts
		// execute immediately
		return verdictActionExecute
	}
	if flowCount <= 0 && !emptiedByMigration {
		// a flowless exit has no blast radius, so the soft verdict may
		// execute exactly as before
		return verdictActionExecute
	}
	if flowCount <= 0 && emptiedByMigration {
		// OUR OWN hand-off emptied it, and flowlessness must not become the
		// evidence: the bench moved the flows, the flows were the only
		// traffic that could produce a receive ack, and a receive ack is the
		// only thing that acquits. Executing here would make every all-quic
		// bench a conviction inside one poll interval and remove the exit's
		// path to acquittal at the same time (review finding, 2026-08-03).
		// The episode still matures on the expiry bound below, and a genuine
		// recovery still releases it through quarantineVacated -- the
		// difference is that the exit gets to serve its sentence instead of
		// being convicted for having been rescued.
		if !quarantinedSince.IsZero() && 0 < quarantineExpiry && quarantineExpiry <= now.Sub(quarantinedSince) {
			return verdictActionExecuteExpired
		}
		return verdictActionQuarantine
	}
	if !quarantinedSince.IsZero() && 0 < quarantineExpiry && quarantineExpiry <= now.Sub(quarantinedSince) {
		return verdictActionExecuteExpired
	}
	return verdictActionQuarantine
}

// quarantineVacated is the bench-leak escape: it reports whether a quarantined
// exit should be released back into selection because its case has evaporated.
//
// The leak it closes, from the A+B build's known limitations: a quarantined
// exit's verdict evidence lives in the ~30s stats window, and a demoted exit
// gets no new flows -- so if its remaining flows go quiet and then drain, the
// evidence ages out, no verdict fires ever again, and nothing else touches the
// quarantine. The exit sits benched until rotation (up to an hour), a spare
// the window pays for and can never use.
//
// The conditions are deliberately all three:
//   - quarantined: there is something to release.
//   - reason and held both none: no verdict is firing AND none would have
//     fired but for a gate. A held verdict means the evidence still exists and
//     is merely inadmissible this pass -- releasing on it would acquit on a
//     technicality that the next admissible pass immediately re-convicts.
//   - flowless: a loaded quarantined exit keeps waiting, unchanged, because
//     its flows are the receive source that can genuinely acquit it (receive
//     progress lifts the quarantine) and their continued silence is the very
//     evidence the expiry bound is aging.
//
// The release is safe because it is not an acquittal: clearQuarantine records
// survived-quarantine memory, so the exit returns to selection carrying the
// effectiveTier demerit and is only raced again when the healthier field is
// exhausted -- until it earns promotion with a clean interval and a proven
// connect (see quarantineMemoryDuration).
func quarantineVacated(
	quarantined bool,
	reason blackholeReason,
	held blackholeReason,
	flowCount int,
) bool {
	return quarantined &&
		reason == blackholeNone &&
		held == blackholeNone &&
		flowCount <= 0
}

var errTransportDownTimeout = errors.New("transport down timeout")

// updateTransportDownEpoch owns the transition semantics for the structural
// transport-loss clock. Returning entered/restored separately keeps transition
// logs and receive-clock rebasing edge-triggered while the timestamp itself
// remains a pure, deterministically testable state machine.
func updateTransportDownEpoch(
	now time.Time,
	transportDown bool,
	transportDownSince time.Time,
) (nextTransportDownSince time.Time, entered bool, restored bool) {
	if transportDown {
		if transportDownSince.IsZero() {
			return now, true, false
		}
		return transportDownSince, false, false
	}
	if !transportDownSince.IsZero() {
		return time.Time{}, false, true
	}
	return time.Time{}, false, false
}

// transportDownExpired bounds the structural transport-loss hold separately
// from blackhole evidence. While a channel has no route, its traffic silence
// cannot convict the provider; once the route-restoration grace itself expires,
// however, the local fact that this channel still cannot carry any packet is
// sufficient to retire it and let the window build a fresh client. A zero or
// negative bound preserves the disabled/unbounded behavior.
func transportDownExpired(
	now time.Time,
	transportDownSince time.Time,
	maxHold time.Duration,
) bool {
	return 0 < maxHold &&
		!transportDownSince.IsZero() &&
		maxHold <= now.Sub(transportDownSince)
}

func (self *multiClientChannel) detectBlackhole() {
	// within a timeout window, if there are sent data but none received,
	// error out. This is similar to an ack timeout.
	defer self.cancel()

	// the transport-liveness epoch for this channel. detectBlackhole is the
	// only goroutine that reads the transport for verdicts, so the epoch
	// lives in loop locals rather than on the struct -- there is nothing to
	// lock. transportDownSince is set while the transport set is empty;
	// transportFreshSince is when the last down epoch ended (zero if never
	// down) and rebases this channel's receive-verdict clocks exactly like
	// the tunnel-wide uplink freshSince does.
	var transportDownSince time.Time
	var transportFreshSince time.Time

	// a held verdict is counted once per hold episode, not once per
	// evaluation pass: the counters answer "how many verdicts did the gates
	// suppress", and one suppressed verdict re-evaluated every 1.25s for a
	// minute is still one suppressed verdict.
	heldCounted := false
	sharedFateCounted := false

	for {
		if windowStats, err := self.WindowStats(); err != nil {
			return
		} else {
			now := time.Now()

			// evidence gates, computed fresh each pass with no lock held:
			// WindowStats released the channel stateLock before returning,
			// and uplinkGate takes the parent stateLock inside -- the parent
			// helper must never be called with a channel or leaf lock held.
			uplinkStale, uplinkFreshSince := self.uplinkGate(now)

			transportDown := !self.hasActiveTransport()
			unreliableSendTransport := self.hasActiveUnreliableSendTransport()
			var enteredTransportDown bool
			var restoredTransport bool
			transportDownSince, enteredTransportDown, restoredTransport = updateTransportDownEpoch(
				now,
				transportDown,
				transportDownSince,
			)
			if transportDown {
				if enteredTransportDown {
					// transitions only, never per-pass
					self.log.Infof("[multi]routing %s transport down: verdicts held\n", self.args.Destination)
				}
				if transportDownExpired(
					now,
					transportDownSince,
					self.settings.StatsWindowKeepUnhealthyDuration,
				) {
					// This is not a silence verdict and does not enter the verdict
					// storm budget. The channel's own route set has remained empty
					// for the complete migration grace, so no established flow can
					// make progress here and a fresh client is the only recovery.
					self.addError(errTransportDownTimeout)
					self.log.Infof("%s\n", relEvent(
						"transport",
						"exit", self.ClientId(),
						"state", "expired",
						"hold", self.settings.StatsWindowKeepUnhealthyDuration,
					))
					return
				}
			} else if restoredTransport {
				transportFreshSince = now
				self.log.Infof("[multi]routing %s transport restored: verdict clocks rebased\n", self.args.Destination)
			}

			// this channel's receive clocks restart at whichever exculpatory
			// epoch ended last: tunnel-wide uplink freshness, or this
			// channel's own transport re-registration
			receiveFreshSince := uplinkFreshSince
			if receiveFreshSince.Before(transportFreshSince) {
				receiveFreshSince = transportFreshSince
			}

			// the comparative connect cut: while two OTHER exits are visibly
			// receiving — or this exit's own sends are being acknowledged
			// (the sibling-free proof a single-destination window relies on)
			// — the no-receive-syn branch matures at the short bar instead of
			// the full one. Read with no lock held -- the sibling count
			// reaches the parent stateLock inside, and the own-ack read takes
			// this channel's own stateLock, the same contract flowCount and
			// uplinkGate follow.
			connectTimeout := comparativeConnectTimeout(
				now,
				windowStats,
				self.settings.BlackholeConnectTimeout,
				self.reliabilitySettings().BlackholeConnectComparativeTimeout,
				receiveFreshSince,
				self.receivingSiblings,
				func() bool {
					return self.hasRecentSendAck(self.settings.BlackholeTimeout)
				},
			)

			// read once per pass: the corroboration gate below scales with it,
			// and the verdict handling further down reuses the same reading so
			// one pass judges one consistent count
			flowCount := self.flowCount()

			reason, held := blackholeReasonFromStats(
				now,
				windowStats,
				self.settings.BlackholeTimeout,
				self.reliabilitySettings().BlackholeReceiveTimeout,
				connectTimeout,
				blackholeGates{
					transportDown:           transportDown,
					unreliableSendTransport: unreliableSendTransport,
					uplinkStale:             uplinkStale,
					receiveFreshSince:       receiveFreshSince,
					// the G-6 load scaling: the busier the exit, the broader
					// the silence must be before the soft verdict is
					// admissible
					minReceiveAckDestinations: loadScaledMinDestinations(
						self.reliabilitySettings().MinBlackholeDestinations,
						flowCount,
						self.reliabilitySettings().BlackholeLoadCorroboration,
					),
				},
			)
			blackhole := reason != blackholeNone

			// admissible receive-silence is CURRENT evidence for the
			// shared-fate window, recorded before any gate so concurrent
			// sufferers see each other
			sharedFateMinExits := self.reliabilitySettings().SharedFateMinExits
			sharedFateWindow := self.reliabilitySettings().SharedFateWindow
			sharedFateOn := 0 < sharedFateMinExits && 0 < sharedFateWindow
			if blackhole && sharedFateOn {
				self.metrics().recordSharedFate(self.ClientId(), now)
			}

			if held != blackholeNone {
				if !heldCounted {
					heldCounted = true
					// attribution follows the gate: the transport gate is the
					// channel-specific, stronger evidence, so a pass where
					// both are engaged books against it
					if transportDown {
						self.metrics().verdictHeldTransportDown()
					} else {
						self.metrics().verdictHeldUplinkStale()
					}
				}
			} else {
				heldCounted = false
			}

			if blackhole {
				// the client has sent data but received nothing back
				// this looks like a blackhole
				if self.log.V(1).Enabled() {
					self.log.Infof("[multi]routing %s blackhole: %d %dB <> %d %dB (%d <> %d)\n",
						self.args.Destination,
						windowStats.sendAckCount,
						windowStats.sendAckByteCount,
						windowStats.receiveAckCount,
						windowStats.receiveAckByteCount,
						windowStats.sendSynCount,
						windowStats.receiveSynCount,
					)
				}

				// what this admissible verdict is allowed to cost. every read
				// here runs with no lock held: WindowStats released the
				// channel stateLock before returning, quarantineState takes
				// and releases it, and flowCount reaches into the parent
				// stateLock -- which must never happen under a channel lock.
				// quarantinedSince only carries when the current episode is
				// for this same reason, so the expiry bound measures one
				// unbroken run of the same evidence. flowCount was read once
				// at the top of this pass, before the corroboration gate.
				quarantinedSince := time.Time{}
				if quarantineReason, quarantineStart := self.quarantineState(); quarantineReason == reason {
					quarantinedSince = quarantineStart
				}
				// the bench hold time: short-circuited on the knob BEFORE
				// quarantineReconvictionCount() (which takes stateLock) is
				// ever called, so a default build with QuarantineDampening
				// off does not acquire a new lock on this live path -- the
				// off-path value stays byte-for-byte
				// self.settings.StatsWindowKeepUnhealthyDuration, exactly as
				// it was passed directly before this task. benchDuration's
				// own `if !dampening { return base }` guard is kept as
				// defence in depth, not relied on here for the lock-skip.
				// dampening is always true inside this branch (that is the
				// condition), so it is passed to benchDuration as a literal
				// rather than re-read.
				quarantineExpiry := self.settings.StatsWindowKeepUnhealthyDuration
				if self.reliabilitySettings().QuarantineDampening {
					quarantineExpiry = benchDuration(self.quarantineReconvictionCount(), quarantineExpiry, true)
				}
				action := verdictAction(
					reason,
					self.reliabilitySettings().SoftVerdictDemote,
					flowCount,
					quarantinedSince,
					now,
					quarantineExpiry,
					self.emptiedByMigration(),
				)

				if action == verdictActionQuarantine {
					// demote instead of execute: the exit leaves selection
					// (isWarning) with its established flows intact, and the
					// window is woken so a replacement expands now. No
					// addError here and no return -- endErr is
					// first-write-wins and there is no un-conviction, so
					// nothing may be written until the decision really is to
					// execute. The loop keeps running and re-judges every
					// pass: receive progress lifts the quarantine, a drained
					// flow count or the expiry bound executes it.
					if self.setQuarantined(reason) {
						self.log.Infof("%s\n", relEvent(
							"quarantine",
							"exit", self.ClientId(),
							"reason", string(reason),
							"flows", flowCount,
						))
						self.resizeWake()
						// hand the movable flows off HERE, at the verdict,
						// rather than only from the resize classification
						// tree. That call site sits inside `healthy &&
						// unhealthyDuration < 5s` and behind the dialStarved
						// branch, so it loses silently whenever the resize
						// goroutine is blocked in expand() -- which the
						// field logs show is the state during the incident
						// this was built to fix (its demote lines are the
						// UNHEALTHY branch, so the quarantine branch was
						// never reached and no hand-off could have run).
						// Driven by the verdict, it cannot be preempted by a
						// classification. The per-episode latch is shared
						// with the resize site, which stays as a backstop.
						self.migrateFlows()
					}
				} else {
					// Everything needed to judge the verdict goes in the error,
					// because this is the only line that survives into a field log.
					// The V(1) diagnostic above carries the same counts but glog
					// verbosity is pinned to 0 in sdk.go with no runtime control,
					// so in practice it never prints.
					//
					// This matters for telling a real blackhole from a false
					// positive. On a small network of known-good providers a
					// blackhole verdict is far more likely to be our accounting
					// than a broken exit, and the counts say which: sends
					// acknowledged with nothing received can simply be a quiet
					// destination, while an unacked-send age far past the bound
					// with no acks at all is a provider that really went away.
					// Once per removal, so the cost is nothing.
					//
					// "quarantine expired" marks the execution of a verdict
					// that was first demoted and then held for the whole
					// sustained bound with zero receive progress -- a field
					// capture must be able to tell that slow-path conviction
					// from an immediate one. The "Blackhole " prefix is shared
					// by both forms on purpose: the window's storm breaker
					// keys verdict-driven removals on it.
					sharedFateHeld := false
					if sharedFateOn {
						peers := self.metrics().sharedFatePeers(self.ClientId(), now, sharedFateWindow)
						if sharedFateMinExits <= peers+1 {
							// held: this many exits silent in the same seconds
							// is one fact about the shared path. No addError --
							// endErr is first-write-wins and there is no
							// un-conviction. The loop keeps judging (falling
							// through to the pass wait, never skipping it):
							// receive progress acquits, and a genuinely dead
							// exit executes the first pass after the
							// correlation clears, on its own carried evidence.
							sharedFateHeld = true
							if !sharedFateCounted {
								sharedFateCounted = true
								self.metrics().verdictHeldSharedFate()
								self.log.Infof("%s\n", relEvent(
									"verdict_hold",
									"exit", self.ClientId(),
									"reason", string(reason),
									"cause", "shared_fate",
									"peers", peers,
									"flows", flowCount,
								))
							}
						}
					}
					if !sharedFateHeld {
						sharedFateCounted = false
						expired := ""
						if action == verdictActionExecuteExpired {
							expired = " quarantine expired"
						}
						// dsts is the distinct-send-destination count behind the
						// MinBlackholeDestinations gate: a field capture must be
						// able to tell "many destinations silent" (a real
						// blackhole) from "one destination silent" (a dead
						// website that squeaked past a lowered gate) on the one
						// line that survives into a field log.
						self.addError(fmt.Errorf(
							"Blackhole %s%s (send %d/%dB recv %d/%dB syn %d/%d nackAge %s synAge %s dsts=%d)",
							reason,
							expired,
							windowStats.sendAckCount,
							windowStats.sendAckByteCount,
							windowStats.receiveAckCount,
							windowStats.receiveAckByteCount,
							windowStats.sendSynCount,
							windowStats.receiveSynCount,
							blackholeAgeString(windowStats.firstSendNackTime),
							blackholeAgeString(windowStats.firstSendSynTime),
							windowStats.sendDestinationCount,
						))
						return
					}
				}
			} else {
				// the bench-leak escape (see quarantineVacated): no verdict is
				// firing, none is held, and the exit is flowless -- its case
				// has evaporated, so release it to selection where the
				// survived-quarantine demerit keeps it deprioritized until it
				// earns promotion. Ordering: isQuarantined takes only this
				// channel's stateLock, flowCount reaches the parent stateLock,
				// and nothing is held across either read -- same discipline as
				// the verdict branch above. The cheap channel-local reads run
				// first so the parent lock is only touched for an actually
				// quarantined channel.
				if self.isQuarantined() &&
					quarantineVacated(true, reason, held, self.flowCount()) {
					self.clearQuarantine()
					self.log.Infof("%s\n", relEvent(
						"quarantine_clear",
						"exit", self.ClientId(),
						"reason", "vacated",
					))
				}
				if self.log.V(1).Enabled() {
					self.log.Infof(
						"[multi]routing ok %s: %d %dB <> %d %dB (%d <> %d)\n",
						self.args.Destination,
						windowStats.sendAckCount,
						windowStats.sendAckByteCount,
						windowStats.receiveAckCount,
						windowStats.receiveAckByteCount,
						windowStats.sendSynCount,
						windowStats.receiveSynCount,
					)
				}
			}

			select {
			case <-self.ctx.Done():
				return
			case <-self.client.Done():
				return
			case <-time.After(self.settings.BlackholeTimeout / 4):
			}
		}
	}
}

func (self *multiClientChannel) ping() {
	for {
		if windowStats, err := self.WindowStats(); err != nil {
			return
		} else if self.settings.CPingMaxByteCountPerSecond == 0 || windowStats.EffectiveByteCountPerSecond() <= self.settings.CPingMaxByteCountPerSecond {
			// One callback, one buffered result. The ping loop deliberately may
			// stop before a late Ack arrives; buffering lets that callback finish
			// without retaining a goroutine until the channel itself closes.
			pingDone := make(chan error, 1)
			ackCallback := func(err error) {
				select {
				case <-self.ctx.Done():
					return
				case pingDone <- err:
				default:
				}
			}
			var success bool
			var err error
			if self.pingSendForTest != nil {
				success, err = self.pingSendForTest(
					self.settings.CPingWriteTimeout,
					ackCallback,
				)
			} else {
				success, err = self.SendDetailedMessage(
					&protocol.IpPing{},
					self.settings.CPingWriteTimeout,
					ackCallback,
				)
			}
			if err != nil {
				self.addError(err)
				self.cancel()
				return
			} else if !success {
				self.log.Infof("[multi]cping %s not admitted: ping loop ended, channel remains\n", self.args.Destination)
				return
			} else {
				// wait for the ping ack. The timeout branch deliberately does
				// NOT convict: a single unanswered ping ends this monitoring
				// loop and nothing else -- the channel lives on, judged by the
				// same evidence machinery as every other exit. In particular,
				// ping has no deferred cancel: that stale defer contradicted this
				// branch and reset a lossy H3 flow at CPingTimeout with reason
				// "Done.".
				select {
				case <-self.ctx.Done():
					return
				case <-self.clientDone():
					return
				case err := <-pingDone:
					if err != nil {
						self.addError(err)
						self.cancel()
						return
					}
				case <-time.After(self.settings.CPingTimeout):
					self.log.Infof("[multi]cping %s unanswered: ping loop ended, channel remains\n", self.args.Destination)
					return
				}
			}
		}

		// rest between pings. `CPingRestTimeout` is decoupled from the ack wait
		// so a dead idle client is detected promptly (rest + ack wait), with a
		// fallback to `CPingTimeout` for settings that predate the split
		restTimeout := self.settings.CPingRestTimeout
		if restTimeout <= 0 {
			restTimeout = self.settings.CPingTimeout
		}
		// a degraded host rests longer between idle pings (radio wakeups),
		// which delays only idle-death detection, never a conviction
		if degradedMode := self.settings.DegradedMode; degradedMode != nil && degradedMode.Load() {
			if scale := self.settings.DegradedLivenessScale; 1 < scale {
				restTimeout = time.Duration(float64(restTimeout) * scale)
			}
		}
		select {
		case <-self.ctx.Done():
			return
		case <-self.clientDone():
			return
		case <-WakeupAfter(restTimeout, restTimeout):
		}
	}
}

// addSend records the per-packet send stats (nack, optional syn, and source)
// in a single locked section, so the hot send path takes the channel lock and
// resolves the event bucket once per packet instead of two-three times.
func (self *multiClientChannel) addSend(packetByteCount ByteCount, ipPath *IpPath) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	eventBucket := self.eventBucket()

	if self.packetStats.sendNackCount == 0 {
		// first outstanding send, so start the stall clock. see sendStalled
		self.pendingSendTime = time.Now()
	}
	self.packetStats.sendNackCount += 1
	self.packetStats.sendNackByteCount += packetByteCount
	if eventBucket.sendNackCount == 0 {
		eventBucket.sendNackTime = time.Now()
	}
	eventBucket.sendNackCount += 1
	eventBucket.sendNackByteCount += packetByteCount

	if ipPath.Syn {
		if self.packetStats.firstUnansweredSendSynTime.IsZero() {
			self.packetStats.firstUnansweredSendSynTime = time.Now()
		}
		self.packetStats.sendSynCount += 1
		if eventBucket.sendSynCount == 0 {
			eventBucket.sendSynTime = time.Now()
		}
		eventBucket.sendSynCount += 1
	}

	self.addSourceToEventBucketWithLock(eventBucket, ipPath)
}

// Records an admitted group in one existing channel-lock section while
// retaining packet-accurate health and event accounting.
func (self *multiClientChannel) addSendGroup(sendPacketGroup *parsedPacketGroup) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	eventBucket := self.eventBucket()
	packetCount := len(sendPacketGroup.packets)
	if self.packetStats.sendNackCount == 0 {
		self.pendingSendTime = time.Now()
	}
	self.packetStats.sendNackCount += packetCount
	self.packetStats.sendNackByteCount += sendPacketGroup.byteCount
	if eventBucket.sendNackCount == 0 {
		eventBucket.sendNackTime = time.Now()
	}
	eventBucket.sendNackCount += packetCount
	eventBucket.sendNackByteCount += sendPacketGroup.byteCount

	synCount := 0
	for packetIndex := range sendPacketGroup.packets {
		if sendPacketGroup.packets[packetIndex].ipPath.Syn {
			synCount += 1
		}
	}
	if 0 < synCount {
		if self.packetStats.firstUnansweredSendSynTime.IsZero() {
			self.packetStats.firstUnansweredSendSynTime = time.Now()
		}
		self.packetStats.sendSynCount += synCount
		if eventBucket.sendSynCount == 0 {
			eventBucket.sendSynTime = time.Now()
		}
		eventBucket.sendSynCount += synCount
	}
	self.addSourceToEventBucketWithLock(eventBucket, sendPacketGroup.ipPath)
}

// addSendAbandoned is the symmetric undo of addSend for a send that cannot
// earn acknowledgement credit. This includes a hard error/false result before
// Transfer admission, and an admitted NoAck datagram whose initial route write
// failed. The first case has no callback; the second callback describes only
// that disposable write and must not poison the provider. Without the undo,
// either send stays booked as outstanding forever: the nack count stays up and
// pendingSendTime keeps aging, so a burst of backpressure is enough for
// sendStalled to convict an exit that then goes innocently idle.
//
// The aggregate undo is exact. The refused send is by construction still
// counted in packetStats -- only its own ack could have removed it, and no
// ack exists for it -- so the decrement mirrors what addSend recorded, with
// no ack credit. When the outstanding count reaches zero the stall clock is
// cleared outright: sendStalled already treats a zero count as idle, but
// addSend keys its restart off the count alone, and a stale nonzero
// pendingSendTime is a lie waiting for a reader that forgets to check the
// count first.
//
// The bucket undo cannot always be exact. addSend recorded into whichever
// bucket was current at send time, and by the time the transport reports the
// refusal that bucket may have rotated out or been coalesced away; holding a
// reference to it across the transport call would keep dead buckets alive
// for a case that should be rare. The choice: decrement the newest existing
// bucket, clamped at zero. Under rotation this can under-count a younger
// bucket's real nacks by at most the abandoned send, which errs toward "the
// exit is fine" -- the honest direction for an undo whose purpose is to stop
// manufacturing guilt. No new bucket is created and eventTime is not
// advanced: a retraction is not activity, and extending the bucket's life or
// the stats window on it would fabricate both.
//
// The syn count addSend may also have recorded is deliberately left alone:
// it is windowed, so it ages out with its bucket, and unwinding it here could
// just as easily erase a different live syn's record from the newest bucket.
func (self *multiClientChannel) addSendAbandoned(packetByteCount ByteCount) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.packetStats.sendNackCount -= 1
	self.packetStats.sendNackByteCount -= packetByteCount
	if self.packetStats.sendNackCount <= 0 {
		self.pendingSendTime = time.Time{}
	}

	if n := len(self.eventBuckets); 0 < n {
		eventBucket := self.eventBuckets[n-1]
		if 0 < eventBucket.sendNackCount {
			eventBucket.sendNackCount -= 1
		}
		eventBucket.sendNackByteCount = max(eventBucket.sendNackByteCount-packetByteCount, 0)
	}
}

// Retracts one refused logical group's exact packet and byte accounting.
func (self *multiClientChannel) addSendAbandonedGroup(sendPacketGroup *parsedPacketGroup) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	packetCount := len(sendPacketGroup.packets)
	self.packetStats.sendNackCount -= packetCount
	self.packetStats.sendNackByteCount -= sendPacketGroup.byteCount
	if self.packetStats.sendNackCount <= 0 {
		self.pendingSendTime = time.Time{}
	}
	if eventBucketCount := len(self.eventBuckets); 0 < eventBucketCount {
		eventBucket := self.eventBuckets[eventBucketCount-1]
		eventBucket.sendNackCount = max(eventBucket.sendNackCount-packetCount, 0)
		eventBucket.sendNackByteCount = max(
			eventBucket.sendNackByteCount-sendPacketGroup.byteCount,
			0,
		)
	}
}

func (self *multiClientChannel) addSendNack(ackByteCount ByteCount) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.packetStats.sendNackCount += 1
	self.packetStats.sendNackByteCount += ackByteCount

	eventBucket := self.eventBucket()
	if eventBucket.sendNackCount == 0 {
		eventBucket.sendNackTime = time.Now()
	}
	eventBucket.sendNackCount += 1
	eventBucket.sendNackByteCount += ackByteCount
}

func (self *multiClientChannel) addSendAck(ackByteCount ByteCount) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.packetStats.sendNackCount -= 1
	self.packetStats.sendNackByteCount -= ackByteCount
	self.packetStats.sendAckCount += 1
	self.packetStats.sendAckByteCount += ackByteCount
	self.lastSendAckTime = time.Now()
	// an ack is progress, so the stall clock restarts here
	self.pendingSendTime = time.Now()

	eventBucket := self.eventBucket()
	if eventBucket.sendAckCount == 0 {
		eventBucket.sendAckTime = time.Now()
	}
	eventBucket.sendAckCount += 1
	eventBucket.sendAckByteCount += ackByteCount
}

// Retires one logical group's packet-accurate outstanding accounting.
func (self *multiClientChannel) addSendAckGroup(sendPacketGroup *parsedPacketGroup) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	packetCount := len(sendPacketGroup.packets)
	self.packetStats.sendNackCount -= packetCount
	self.packetStats.sendNackByteCount -= sendPacketGroup.byteCount
	self.packetStats.sendAckCount += packetCount
	self.packetStats.sendAckByteCount += sendPacketGroup.byteCount
	self.lastSendAckTime = time.Now()
	self.pendingSendTime = time.Now()

	eventBucket := self.eventBucket()
	if eventBucket.sendAckCount == 0 {
		eventBucket.sendAckTime = time.Now()
	}
	eventBucket.sendAckCount += packetCount
	eventBucket.sendAckByteCount += sendPacketGroup.byteCount
}

func (self *multiClientChannel) addSendSyn(synCount int) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if 0 < synCount && self.packetStats.firstUnansweredSendSynTime.IsZero() {
		self.packetStats.firstUnansweredSendSynTime = time.Now()
	}
	self.packetStats.sendSynCount += synCount

	eventBucket := self.eventBucket()
	if eventBucket.sendSynCount == 0 {
		eventBucket.sendSynTime = time.Now()
	}
	eventBucket.sendSynCount += synCount
}

func (self *multiClientChannel) addReceiveAck(ackByteCount ByteCount) {
	// the lift is decided under the lock and LOGGED outside it: the house rule
	// here is that no lock is ever held across logging, and a logger is host
	// code that can do anything (a file write, an ipc hop) while this channel's
	// stateLock is on the receive path of every packet
	lifted := false
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		self.packetStats.receiveAckCount += 1
		self.packetStats.receiveAckByteCount += ackByteCount

		// the receive freshness stamp read by hasRecentReceive for the
		// parent's receive-side sibling count (the comparative connect cut's
		// evidence). Written under the lock this path already holds, so it
		// costs one store on the receive path and no new contention.
		self.lastReceiveAckTime = time.Now()

		// receive progress is exactly the evidence the quarantining verdicts said
		// was missing, so it acquits: lift the quarantine here, where the count
		// advances. The cost on the hot path is one bool read under the
		// already-held lock; the log fires once per lifted episode, never per
		// packet. The lift goes through the shared clear so it records the
		// survived-quarantine memory like every other lift -- acquitted of the
		// episode, still demoted in rank until promotion is earned.
		if self.quarantined {
			self.clearQuarantineWithLock()
			lifted = true
		}

		eventBucket := self.eventBucket()
		eventBucket.receiveAckCount += 1
		eventBucket.receiveAckByteCount += ackByteCount
	}()

	// once per lifted episode, never per packet
	if lifted {
		loggerOrDefault(self.log).Infof("%s\n", relEvent(
			"quarantine_lift",
			"exit", self.ClientId(),
			"reason", "receive_progress",
		))
	}

	// the qualification refresh: receive progress is delivery proof, the same
	// fact a probe answer records, so it keeps this exit's qualification fresh
	// for free. OUTSIDE the locked section above -- the refresh takes the
	// parent stateLock inside, which must never nest under this channel's --
	// and gated to at most once per qualificationReceiveRefreshInterval by an
	// atomic stamp, so the per-packet cost is one atomic load and a compare.
	self.touchQualificationOnReceive()
}

func (self *multiClientChannel) addReceiveSyn(synCount int) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if 0 < synCount {
		// an answered connect: the route forwards connect traffic right now,
		// so any pending attempt state is stale
		self.packetStats.firstUnansweredSendSynTime = time.Time{}
	}
	self.packetStats.receiveSynCount += synCount

	eventBucket := self.eventBucket()
	eventBucket.receiveSynCount += synCount
}

func (self *multiClientChannel) addSource(ipPath *IpPath) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.addSourceToEventBucketWithLock(self.eventBucket(), ipPath)
}

// must be called with `stateLock`
func (self *multiClientChannel) addSourceToEventBucketWithLock(eventBucket *multiClientEventBucket, ipPath *IpPath) {
	// `ip{4,6}DestinationSourceCount[destination][source]` is a reference count
	// of how many event buckets currently hold the path. `removeEventBucket`
	// decrements it once per path in the bucket's set, so the increment here
	// must also happen exactly once per (bucket, path) — i.e. only when the path
	// is newly added to this bucket's set. doing it on every packet (the prior
	// behavior) both over-counted (the count never returned to zero, so sources
	// were never released — unbounded growth) and did redundant per-packet map
	// writes under the lock. after the first packet of a flow in a bucket, this
	// is a single map read and no writes.
	switch ipPath.Version {
	case 4:
		ip4Path := ipPath.ToIp4Path()

		if eventBucket.ip4Paths == nil {
			eventBucket.ip4Paths = map[Ip4Path]bool{}
		}
		if eventBucket.ip4Paths[ip4Path] {
			return
		}
		eventBucket.ip4Paths[ip4Path] = true

		source := ip4Path.Source()
		destination := ip4Path.Destination()

		sourceCount, ok := self.ip4DestinationSourceCount[destination]
		if !ok {
			sourceCount = map[Ip4Path]int{}
			self.ip4DestinationSourceCount[destination] = sourceCount
		}
		sourceCount[source] += 1
	case 6:
		ip6Path := ipPath.ToIp6Path()

		if eventBucket.ip6Paths == nil {
			eventBucket.ip6Paths = map[Ip6Path]bool{}
		}
		if eventBucket.ip6Paths[ip6Path] {
			return
		}
		eventBucket.ip6Paths[ip6Path] = true

		source := ip6Path.Source()
		destination := ip6Path.Destination()

		sourceCount, ok := self.ip6DestinationSourceCount[destination]
		if !ok {
			sourceCount = map[Ip6Path]int{}
			self.ip6DestinationSourceCount[destination] = sourceCount
		}
		sourceCount[source] += 1
	default:
		panic(fmt.Errorf("Bad protocol version %d", ipPath.Version))
	}
}

func (self *multiClientChannel) addError(err error) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if self.endErr == nil {
		self.endErr = err
	}

	eventBucket := self.eventBucket()
	eventBucket.errs = append(eventBucket.errs, err)
}

// must be called with `stateLock`
func (self *multiClientChannel) eventBucket() *multiClientEventBucket {
	now := time.Now()

	var eventBucket *multiClientEventBucket
	if n := len(self.eventBuckets); 0 < n {
		eventBucket = self.eventBuckets[n-1]
	}

	if eventBucket == nil || eventBucket.createTime.Add(self.settings.StatsWindowBucketDuration).Before(now) {
		eventBucket = newMultiClientEventBucket()
		self.eventBuckets = append(self.eventBuckets, eventBucket)
	}

	eventBucket.eventTime = now

	self.coalesceEventBuckets()

	return eventBucket
}

// must be called with `stateLock`
func (self *multiClientChannel) coalesceEventBuckets() {
	// if there is no activity (no new buckets), keep historical buckets around
	minBucketCount := 1 + int(self.settings.StatsWindowDuration/self.settings.StatsWindowBucketDuration)

	windowStart := time.Now().Add(-self.settings.StatsWindowDuration)

	removeEventBucket := func(eventBucket *multiClientEventBucket) {
		self.packetStats.sendAckCount -= eventBucket.sendAckCount
		self.packetStats.sendAckByteCount -= eventBucket.sendAckByteCount
		self.packetStats.sendSynCount -= eventBucket.sendSynCount
		self.packetStats.receiveAckCount -= eventBucket.receiveAckCount
		self.packetStats.receiveAckByteCount -= eventBucket.receiveAckByteCount
		self.packetStats.receiveSynCount -= eventBucket.receiveSynCount

		for ip4Path, _ := range eventBucket.ip4Paths {
			source := ip4Path.Source()
			destination := ip4Path.Destination()

			sourceCount, ok := self.ip4DestinationSourceCount[destination]
			if ok {
				count := sourceCount[source]
				if count-1 <= 0 {
					delete(sourceCount, source)
				} else {
					sourceCount[source] = count - 1
				}
				if len(sourceCount) == 0 {
					delete(self.ip4DestinationSourceCount, destination)
				}
			}
		}

		for ip6Path, _ := range eventBucket.ip6Paths {
			source := ip6Path.Source()
			destination := ip6Path.Destination()

			sourceCount, ok := self.ip6DestinationSourceCount[destination]
			if ok {
				count := sourceCount[source]
				if count-1 <= 0 {
					delete(sourceCount, source)
				} else {
					sourceCount[source] = count - 1
				}
				if len(sourceCount) == 0 {
					delete(self.ip6DestinationSourceCount, destination)
				}
			}
		}
	}

	// remove all events before the window start
	i := 0
	for i < len(self.eventBuckets) && self.eventBuckets[i].eventTime.Before(windowStart) {
		removeEventBucket(self.eventBuckets[i])
		self.eventBuckets[i] = nil
		i += 1
	}
	for i < len(self.eventBuckets) && minBucketCount < len(self.eventBuckets) {
		removeEventBucket(self.eventBuckets[i])
		self.eventBuckets[i] = nil
		i += 1
	}
	if 0 < i {
		self.eventBuckets = self.eventBuckets[i:]
	}
}

func (self *multiClientChannel) WindowStats() (*clientWindowStats, error) {
	return self.windowStatsWithCoalesce(true)
}

func (self *multiClientChannel) windowStatsWithCoalesce(coalesce bool) (*clientWindowStats, error) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if coalesce {
		self.coalesceEventBuckets()
	}

	// omit the latest two event buckets since they may be partial
	var eventBuckets []*multiClientEventBucket
	if 2 <= len(self.eventBuckets) {
		eventBuckets = self.eventBuckets[0 : len(self.eventBuckets)-2]
	}

	windowDuration := time.Duration(0)
	if 0 < len(eventBuckets) {
		endTime := eventBuckets[len(eventBuckets)-1].eventTime
		windowDuration = endTime.Sub(eventBuckets[0].createTime)
	}
	var firstSendAckTime time.Time
	for _, eventBucket := range eventBuckets {
		if 0 < eventBucket.sendAckCount {
			firstSendAckTime = eventBucket.sendAckTime
			break
		}
	}
	var firstSendNackTime time.Time
	for _, eventBucket := range eventBuckets {
		if 0 < eventBucket.sendNackCount {
			firstSendNackTime = eventBucket.sendNackTime
			break
		}
	}
	var firstSendSynTime time.Time
	for _, eventBucket := range eventBuckets {
		if 0 < eventBucket.sendSynCount {
			firstSendSynTime = eventBucket.sendSynTime
			break
		}
	}

	// public internet resource ports
	isPublicPort := func(port int) bool {
		switch port {
		case 443:
			return true
		default:
			return false
		}
	}

	netSourceCounts := []int{}
	for ip4Path, sourceCounts := range self.ip4DestinationSourceCount {
		if isPublicPort(ip4Path.DestinationPort) {
			netSourceCounts = append(netSourceCounts, len(sourceCounts))
		}
	}
	for ip6Path, sourceCounts := range self.ip6DestinationSourceCount {
		if isPublicPort(ip6Path.DestinationPort) {
			netSourceCounts = append(netSourceCounts, len(sourceCounts))
		}
	}
	slices.Sort(netSourceCounts)
	maxSourceCount := 0
	selectionIndex := int(math.Ceil(
		self.settings.StatsSourceCountSelection * float64(len(netSourceCounts)-1),
	))
	if selectionIndex < len(netSourceCounts) {
		maxSourceCount = netSourceCounts[selectionIndex]
	}
	netSourceCount := 0
	for _, sourceCounts := range self.ip4DestinationSourceCount {
		netSourceCount += len(sourceCounts)
	}
	for _, sourceCounts := range self.ip6DestinationSourceCount {
		netSourceCount += len(sourceCounts)
	}
	// loggerOrDefault, not raw self.log: this accessor is now also called
	// from the scored-placement path (exitMetricsSnapshot), which runs
	// against bare test fixtures that carry no logger the same way several
	// of this file's other placement-path helpers already tolerate a nil
	// self.log would panic on a nil interface's method set.
	log := loggerOrDefault(self.log)
	if log.V(2).Enabled() {
		for ip4Path, sourceCounts := range self.ip4DestinationSourceCount {
			if isPublicPort(ip4Path.DestinationPort) {
				if len(sourceCounts) == maxSourceCount {
					log.Infof("[multi]max source count %d = %v\n", maxSourceCount, ip4Path)
				}
			}
		}
		for ip6Path, sourceCounts := range self.ip6DestinationSourceCount {
			if isPublicPort(ip6Path.DestinationPort) {
				if len(sourceCounts) == maxSourceCount {
					log.Infof("[multi]max source count %d = %v\n", maxSourceCount, ip6Path)
				}
			}
		}
	}

	stats := &clientWindowStats{
		log:            log,
		sourceCount:    maxSourceCount,
		netSourceCount: netSourceCount,
		// distinct destination paths with sends in the window: the maps are
		// keyed by destination path and pruned to zero with their buckets,
		// so the key count is exactly the window's distinct-destination set
		sendDestinationCount:       len(self.ip4DestinationSourceCount) + len(self.ip6DestinationSourceCount),
		sendAckCount:               self.packetStats.sendAckCount,
		sendNackCount:              self.packetStats.sendNackCount,
		sendAckByteCount:           self.packetStats.sendAckByteCount,
		sendSynCount:               self.packetStats.sendSynCount,
		sendNackByteCount:          self.packetStats.sendNackByteCount,
		receiveAckCount:            self.packetStats.receiveAckCount,
		receiveAckByteCount:        self.packetStats.receiveAckByteCount,
		receiveSynCount:            self.packetStats.receiveSynCount,
		windowDuration:             windowDuration,
		firstSendAckTime:           firstSendAckTime,
		firstSendNackTime:          firstSendNackTime,
		firstSendSynTime:           firstSendSynTime,
		firstUnansweredSendSynTime: self.packetStats.firstUnansweredSendSynTime,
		bucketCount:                len(eventBuckets),
	}
	if 0 < len(eventBuckets) || !self.firstEventTime.IsZero() {
		// var eventTime time.Time
		// if 0 < len(eventBuckets) {
		// 	eventTime = eventBuckets[len(eventBuckets)-1].eventTime
		// } else {
		// 	eventTime = time.Now()
		// }
		eventTime := time.Now()

		if 0 < len(eventBuckets) {
			stats.lastEventTime = eventBuckets[len(eventBuckets)-1].eventTime
		}

		effectiveByteCountPerSecond := stats.EffectiveByteCountPerSecond()
		// scaledEffectiveByteCountPerSecond := ByteCount(self.settings.StatsWindowMaxEffectiveByteCountPerSecondScale * float32(stats.EffectiveByteCountPerSecond()))
		if self.maxEffectiveByteCountPerSecond < effectiveByteCountPerSecond {
			self.maxEffectiveByteCountPerSecond = effectiveByteCountPerSecond
			self.maxEffectiveByteCountPerSecondTime = eventTime
		}

		effectiveSendByteCount, effectiveReceiveByteCount := stats.EffectiveByteCount()
		healthy := (0 < effectiveSendByteCount) == (0 < effectiveReceiveByteCount)
		if healthy {
			if self.lastUnhealthyTime.IsZero() {
				self.lastUnhealthyTime = eventTime
			}

			if !self.healthy {
				self.healthy = true
				if !self.lastHealthyTime.IsZero() {
					self.netUnhealthyDuration += self.lastUnhealthyTime.Sub(self.lastHealthyTime)
				}
			}

			self.lastHealthyTime = eventTime

			stats.healthyDuration = eventTime.Sub(self.lastUnhealthyTime)
		} else {
			if self.lastHealthyTime.IsZero() {
				self.lastHealthyTime = eventTime
			}

			if self.healthy {
				self.healthy = false
				if !self.lastUnhealthyTime.IsZero() {
					self.netHealthyDuration += self.lastHealthyTime.Sub(self.lastUnhealthyTime)
				}
			}

			self.lastUnhealthyTime = eventTime

			stats.unhealthyDuration = eventTime.Sub(self.lastHealthyTime)
		}
		stats.healthy = healthy
		stats.netHealthyDuration = self.netHealthyDuration + stats.healthyDuration
		stats.netUnhealthyDuration = self.netUnhealthyDuration + stats.unhealthyDuration
		if self.firstEventTime.IsZero() {
			self.firstEventTime = eventBuckets[0].createTime
		}
		stats.clientDuration = eventTime.Sub(self.firstEventTime)
		// the jittered per-channel lifetime, never the raw configured
		// lifetime: reading the setting here would re-synchronize every
		// channel's removeTime onto the same resize pass, which is exactly
		// what the jitter exists to prevent (and the anchor test pins this
		// function to the jittered field). A zero effectiveLifetime (rotation
		// disabled, or a bare test channel) behaves exactly as a zero
		// configured lifetime did before the jitter existed.
		stats.removeTime = self.firstEventTime.Add(self.effectiveLifetime)
	}
	if self.settings.StatsWindowGraceperiod < stats.clientDuration {
		stats.estimatedByteCountPerSecond = self.maxEffectiveByteCountPerSecond
	} else {
		stats.estimatedByteCountPerSecond = max(
			min(self.EstimatedByteCountPerSecond(), self.settings.StatsWindowMaxEstimatedByteCountPerSecond),
			self.maxEffectiveByteCountPerSecond,
		)
	}

	err := self.endErr
	if err == nil {
		// a bare fixture may carry no context -- same pattern
		// busyLivenessProbe uses. A nil channel never fires in a select, which
		// is the right reading of "nothing has cancelled this".
		var ctxDone <-chan struct{}
		if self.ctx != nil {
			ctxDone = self.ctx.Done()
		}
		select {
		case <-ctxDone:
			err = errors.New("Done.")
		case <-self.clientDone():
			err = errors.New("Done.")
		default:
		}
	}

	return stats, err
}

// clientDone is the underlying transport client's done channel, nil-safe for
// the bare fixture channels built literally by tests (the same convention
// ClientId, Tier and Cancel follow). A nil channel blocks forever in a select,
// which is the correct reading of "this channel has no transport that could be
// done" -- and it lets the stats derivation that ships be asserted against
// directly, instead of restated in a test where it could drift.
func (self *multiClientChannel) clientDone() <-chan struct{} {
	if self.client == nil {
		return nil
	}
	return self.client.Done()
}

func (self *multiClientChannel) providerDiagnosticsSnapshot() *ProviderDiagnostics {
	if diagnostics := self.providerDiagnostics.Load(); diagnostics != nil {
		copy := *diagnostics
		return &copy
	}
	return nil
}

// `connect.ReceiveFunction`
func (self *multiClientChannel) clientReceive(source TransferPath, frames []*protocol.Frame, peer Peer) {
	select {
	case <-self.ctx.Done():
		return
	default:
	}

	// only process frames from the destinations
	// if allow := self.sourceFilter[source]; !allow {
	//     self.log.V(2).Infof("[multi]receive drop %d %s<-\n", len(frames), self.args.DestinationId)
	//     return
	// }

	// with a batch callback, parsed packets collect and dispatch as ONE call
	// after the frame loop (the frames slice is one in-order dispatch burst),
	// amortizing the per-packet delivery pipeline. Packets are borrowed for
	// the dispatch, same as frames. The per-packet receive accounting
	// (addReceiveAck / addReceiveSyn) stays exactly where it is -- only the
	// delivery is batched.
	var batchIpPaths []*IpPath
	var batchPackets [][]byte
	var ownedBatchPackets [][]byte
	defer func() {
		for _, packet := range ownedBatchPackets {
			MessagePoolReturn(packet)
		}
	}()
	batch := self.args != nil && self.args.ReceivePackets != nil

	for _, frame := range frames {
		switch frame.MessageType {
		case protocol.MessageType_IpIpProviderDiagnostics:
			message, err := FromFrame(frame)
			if err != nil {
				continue
			}
			diagnostics, ok := message.(*protocol.IpProviderDiagnostics)
			if !ok {
				continue
			}
			next := providerDiagnosticsFromProtocol(diagnostics)
			for {
				current := self.providerDiagnostics.Load()
				if current != nil && next.Sequence <= current.Sequence {
					break
				}
				if self.providerDiagnostics.CompareAndSwap(current, next) {
					break
				}
			}
		case protocol.MessageType_TransferResidentMigrate:
			// Drain migration is a platform control instruction. A provider
			// must not be able to make us churn transports by injecting the
			// same protobuf on an ordinary data path.
			if self.transportMigrator == nil || !source.IsControlSource() {
				continue
			}
			message, err := FromFrame(frame)
			if err != nil {
				continue
			}
			residentMigrate, ok := message.(*protocol.ResidentMigrate)
			if !ok {
				continue
			}
			self.transportMigrator.MigrateClientTransport(
				self.client,
				&self.args.MultiClientGeneratorClientArgs,
				time.UnixMilli(int64(residentMigrate.MigrateTime)),
			)
		case protocol.MessageType_IpIpPacketFromProvider:
			if ipPacketFromProvider_, err := FromFrame(frame); err == nil {
				ipPacketFromProvider := ipPacketFromProvider_.(*protocol.IpPacketFromProvider)

				packet := ipPacketFromProvider.IpPacket.PacketBytes
				if isIpFragmentPacket(packet) {
					result := self.ingressIpv4Fragments.processOwned(
						source,
						peer.TransferKey,
						peer.ProvideMode,
						MessagePoolCopy(packet),
					)
					if result.packet != nil {
						ipPath, parseErr := ParseIpPath(result.packet)
						if parseErr == nil && ipPath.Protocol == IpProtocolUdp {
							for _, fragment := range result.fragments {
								self.addReceiveAck(ByteCount(len(fragment)))
								if batch {
									batchIpPaths = append(batchIpPaths, ipPath)
									batchPackets = append(batchPackets, fragment)
									ownedBatchPackets = append(ownedBatchPackets, fragment)
								} else {
									self.clientReceivePacketCallback(
										self,
										source,
										peer.ProvideMode,
										peer.TransportType,
										ipPath,
										fragment,
									)
									MessagePoolReturn(fragment)
								}
							}
							result.fragments = nil
						}
					}
					returnIpFragmentProcessResult(result)
					continue
				}

				ipPath, err := ParseIpPath(packet)
				if err == nil {
					self.addReceiveAck(ByteCount(len(packet)))
					if ipPath.Syn {
						self.addReceiveSyn(1)
					}
					if batch {
						batchIpPaths = append(batchIpPaths, ipPath)
						batchPackets = append(batchPackets, packet)
					} else {
						self.clientReceivePacketCallback(
							self,
							source,
							peer.ProvideMode,
							peer.TransportType,
							ipPath,
							packet,
						)
					}
				} else if egressIpPath, ok := ipParseIcmpUnreachable(packet); ok {
					// a provider dial-failure signal: an icmp
					// destination-unreachable that ParseIpPath rejects ("No
					// support for protocol 1"), carrying the failed flow's
					// egress key in its embed. Hand it to the parent to re-race.
					//
					// Deliberately NOT counted as received data: the
					// addReceiveAck / addReceiveSyn above are in the err == nil
					// branch only, and this is the parse-failure branch, so an
					// intercepted icmp never touches the receive counters. A
					// provider that answers every dial with a failure must not
					// look healthy to detectBlackhole.
					if self.dialFailureCallback != nil {
						self.dialFailureCallback(self, egressIpPath)
					}
				}
				// else not an ip packet, drop
			} else {
				if self.log.V(2).Enabled() {
					self.log.Infof("[multi]receive drop %s<- = %s\n", self.args.Destination, err)
				}
			}
		default:
			// unknown message, drop
		}
	}

	if batch && 0 < len(batchPackets) {
		self.args.ReceivePackets(
			self,
			source,
			peer.ProvideMode,
			peer.TransportType,
			batchIpPaths,
			batchPackets,
		)
	}
}

func (self *multiClientChannel) Cancel() {
	self.addError(errors.New("Done."))
	self.ingressIpv4Fragments.close()
	// nil-guard each teardown hook: bare fixture channels (built literally by
	// tests, per the ClientId/Tier convention above) have none of them, and
	// the stall watchdog now calls Cancel directly against the same fixtures
	// the stall detector is tested with. Production channels always have all
	// three set by the constructor, so this changes nothing in the field.
	if self.cancel != nil {
		self.cancel()
	}
	if self.client != nil {
		self.client.Cancel()
	}
	// unsubscribe even on Cancel so the underlying Client's callback list
	// doesn't retain a dangling reference for channels that are shuffled out
	// without ever going through Close. unsub is idempotent.
	if self.clientReceiveUnsub != nil {
		self.clientReceiveUnsub()
	}
}

func (self *multiClientChannel) Close() {
	self.addError(errors.New("Done."))
	self.ingressIpv4Fragments.close()
	self.cancel()
	// the local client teardown (which can wait on a slow Pion close) is
	// handled by the channel's own teardown goroutine, after the platform
	// deregistration -- Close must not wait behind it
	self.clientReceiveUnsub()
}

func (self *multiClientChannel) IsDone() bool {
	select {
	case <-self.ctx.Done():
		return true
	default:
		return false
	}
}
