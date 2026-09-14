package connect

import (
	"context"
	"net"
	"testing"
	"time"
)

// ScoredAffinityDonor is the knob that finally makes the learner load-bearing.
// Everything Phase 2 built observed the network and steered nothing: the
// scorer ran only on the RACE field, and the race is the last of sendUpdate's
// four placement steps -- reached only when the flow's affinity groups, its
// app pin, and the destination bridge have all declined to donate. The step
// that carries most flows, inheritAffinityClient{4,6}WithLock, chose its donor
// by recency alone.
//
// These tests pin the two halves of that claim: with the knob off the recency
// rule is preserved byte-for-byte, and with it on the SAME fixture picks the
// other exit. One fixture, opposite answers, one boolean between them -- that
// is the whole proof that the knob is what steers.

func scoredDonorPath(sourcePort int) *IpPath {
	return &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        net.ParseIP("10.20.30.40"),
		SourcePort:      sourcePort,
		DestinationIp:   net.ParseIP("198.51.100.9"),
		DestinationPort: 443,
	}
}

// scoredDonorClient mirrors donorAffinityClient (see
// ip_remote_multi_client_donor_affinity_test.go) with the knob under test
// parameterized. MaxFlowsPerExit stays 0 so clientAtFlowCapWithLock never
// vetoes -- the cap is a real gate on this path, but it is not what these
// tests are about.
func scoredDonorClient(ctx context.Context, scored bool) *RemoteUserNatMultiClient {
	mc := &RemoteUserNatMultiClient{
		ctx: ctx,
		settings: &MultiClientSettings{
			DestinationAffinity:    true,
			FreshFlowAffinity:      true,
			SequenceIdleTimeout:    10 * time.Minute,
			TcpSequenceIdleTimeout: 10 * time.Minute,
			ScoredAffinityDonor:    scored,
		},
		ip4PathUpdates:   map[Ip4Path]*multiClientChannelUpdate{},
		ip6PathUpdates:   map[Ip6Path]*multiClientChannelUpdate{},
		affinityIp4Paths: map[Ip4Path]map[Ip4Path]time.Time{},
		affinityIp6Paths: map[Ip6Path]map[Ip6Path]time.Time{},
		clientUpdates:    map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
	}
	mc.config.Store(&multiClientConfig{})
	return mc
}

// scoredDonorExit builds a channel with a resolvable provider identity --
// without one, providerIdentity declines and affinityDonorBias reports neutral
// for every candidate, which would collapse the ranking into a recency tie and
// make a scoring test silently vacuous.
func scoredDonorExit(ctx context.Context) (*multiClientChannel, string) {
	exit := &multiClientChannel{
		ctx:  ctx,
		args: &multiClientChannelArgs{Destination: RequireMultiHopId(NewId())},
	}
	providerId, ok := providerIdentity(exit)
	if !ok {
		panic("fixture exit must have a resolvable provider identity")
	}
	return exit, providerId.String()
}

// twoDonorGroup registers an older flow on `older` and a newer flow on
// `newer`, both in the same affinity group, and returns the port a third flow
// should use to inherit from that group. Registration order sets createTime
// order, which is exactly the signal the legacy rule reads.
func twoDonorGroup(mc *RemoteUserNatMultiClient, older, newer *multiClientChannel) int {
	registerScoredDonorFlow(mc, scoredDonorPath(41001), older)
	// distinct wall-clock stamps: the legacy rule compares createTime with
	// After, so two registrations inside the same clock tick would tie and the
	// "most recent" half of these tests would not be testing anything
	time.Sleep(2 * time.Millisecond)
	registerScoredDonorFlow(mc, scoredDonorPath(41002), newer)
	return 41003
}

// registerScoredDonorFlow is registerDonorFlow's local twin, kept separate so
// this file's fixture cannot be broken by a change to the donor-affinity
// suite's own fixture.
func registerScoredDonorFlow(mc *RemoteUserNatMultiClient, ipPath *IpPath, exit *multiClientChannel) *multiClientChannelUpdate {
	update := newMultiClientChannelUpdate(mc.ctx, ipPath)
	update.client.Store(exit)
	ip4Path := ipPath.ToIp4Path()
	mc.stateLock.Lock()
	defer mc.stateLock.Unlock()
	mc.ip4PathUpdates[ip4Path] = update
	for _, affinityIpPath := range mc.affinityIpPathsWithLock(ipPath) {
		affinityIp4Path := affinityIpPath.ToIp4Path()
		update.affinityIp4Paths[affinityIp4Path] = true
		paths, ok := mc.affinityIp4Paths[affinityIp4Path]
		if !ok {
			paths = map[Ip4Path]time.Time{}
			mc.affinityIp4Paths[affinityIp4Path] = paths
		}
		paths[ip4Path] = time.Now()
	}
	if updates, ok := mc.clientUpdates[exit]; ok {
		updates[update] = true
	} else {
		mc.clientUpdates[exit] = map[*multiClientChannelUpdate]bool{update: true}
	}
	return update
}

// TestScoredAffinityDonorDefaultOff: the knob must ship inert. Every other
// routing setting in this file follows the same zero-value-off discipline, and
// a default-on learner would change flow placement for every existing build on
// upgrade.
func TestScoredAffinityDonorDefaultOff(t *testing.T) {
	settings := DefaultMultiClientSettings()
	if settings.ScoredAffinityDonor {
		t.Error("ScoredAffinityDonor must default off: it changes which exit carries a site's flows")
	}
	if ReliabilitySettingsFrom(settings).ScoredAffinityDonor {
		t.Error("ReliabilitySettingsFrom must carry ScoredAffinityDonor through; an unmapped field is silently unreachable from the runtime override path")
	}
	// the mapping must also carry a set value -- a field that reads false
	// under both settings would pass the check above while being permanently
	// stuck off
	settings.ScoredAffinityDonor = true
	if !ReliabilitySettingsFrom(settings).ScoredAffinityDonor {
		t.Error("ReliabilitySettingsFrom dropped a set ScoredAffinityDonor")
	}
}

// TestAffinityDonorOffPicksMostRecent pins the legacy rule. The better-scoring
// exit is deliberately the OLDER one, so a scorer leaking into the off path
// fails here rather than passing by coincidence.
func TestAffinityDonorOffPicksMostRecent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc := scoredDonorClient(ctx, false)
	betterOlder, betterId := scoredDonorExit(ctx)
	worseNewer, worseId := scoredDonorExit(ctx)

	priors := NewProviderPriors()
	priors.Observe(betterId, 0.9, time.Now().Unix())
	priors.Observe(worseId, 0.1, time.Now().Unix())
	mc.providerPriors = priors

	port := twoDonorGroup(mc, betterOlder, worseNewer)

	_, _, current := mc.sendUpdate(scoredDonorPath(port), flowPin{})
	if current != worseNewer {
		t.Fatalf("with ScoredAffinityDonor off the most recently joined donor must win regardless of its score (got %p want %p)", current, worseNewer)
	}
}

// TestScoredAffinityDonorPicksHigherBiasOverMoreRecent is the point of the
// whole knob: the same fixture as the test above, one boolean different, and
// the placement goes the other way. This is also ProviderPriors.Bias's first
// production caller -- before this it had none, so the learner's output was
// unreachable from the data path.
func TestScoredAffinityDonorPicksHigherBiasOverMoreRecent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc := scoredDonorClient(ctx, true)
	betterOlder, betterId := scoredDonorExit(ctx)
	worseNewer, worseId := scoredDonorExit(ctx)

	priors := NewProviderPriors()
	priors.Observe(betterId, 0.9, time.Now().Unix())
	priors.Observe(worseId, 0.1, time.Now().Unix())
	mc.providerPriors = priors

	port := twoDonorGroup(mc, betterOlder, worseNewer)

	_, _, current := mc.sendUpdate(scoredDonorPath(port), flowPin{})
	if current != betterOlder {
		t.Fatalf("with ScoredAffinityDonor on the better-scoring donor must win over the more recent one (got %p want %p); the learner is observing but not steering", current, betterOlder)
	}
}

// TestScoredAffinityDonorTieFallsBackToRecency: equal bias must not discard
// the legacy signal. Two providers the learner rates identically -- including
// the common cold-start case where neither has been observed at all -- keep
// the recency rule rather than resolving on Go's randomized map order.
func TestScoredAffinityDonorTieFallsBackToRecency(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc := scoredDonorClient(ctx, true)
	older, _ := scoredDonorExit(ctx)
	newer, _ := scoredDonorExit(ctx)
	// no priors recorded at all: both resolve to priorsNeutralBias
	port := twoDonorGroup(mc, older, newer)

	_, _, current := mc.sendUpdate(scoredDonorPath(port), flowPin{})
	if current != newer {
		t.Fatalf("on a bias tie the most recent donor must still win (got %p want %p)", current, newer)
	}
}

// TestAffinityDonorBiasNeutralWithoutPriors guards the zero-rtt trap's shape.
// exitScore sanitizes a bare 0 into the BEST sub-score, so an unmeasured exit
// outranks every measured one there. Bias must not repeat that: an
// unresolvable or never-seen provider has to land mid-range, where it loses to
// a good provider and beats a bad one.
func TestAffinityDonorBiasNeutralWithoutPriors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc := scoredDonorClient(ctx, true)

	// no priors store at all
	exit, _ := scoredDonorExit(ctx)
	if got := mc.affinityDonorBias(exit); got != priorsNeutralBias {
		t.Errorf("a client with no priors must report neutral bias, got %v want %v", got, priorsNeutralBias)
	}

	// a channel with no resolvable provider identity
	anonymous := &multiClientChannel{ctx: ctx}
	if got := mc.affinityDonorBias(anonymous); got != priorsNeutralBias {
		t.Errorf("an exit with no provider identity must report neutral bias, got %v want %v", got, priorsNeutralBias)
	}

	// and neutral must genuinely sit between a good and a bad provider,
	// otherwise "neutral" is just a differently-spelled extreme
	priors := NewProviderPriors()
	good, goodId := scoredDonorExit(ctx)
	bad, badId := scoredDonorExit(ctx)
	priors.Observe(goodId, 0.9, time.Now().Unix())
	priors.Observe(badId, 0.1, time.Now().Unix())
	mc.providerPriors = priors
	if !(mc.affinityDonorBias(bad) < priorsNeutralBias && priorsNeutralBias < mc.affinityDonorBias(good)) {
		t.Errorf(
			"neutral must sit strictly between bad and good (bad=%v neutral=%v good=%v)",
			mc.affinityDonorBias(bad), priorsNeutralBias, mc.affinityDonorBias(good),
		)
	}
}

// TestBetterAffinityDonorIsDeterministic: the final tiebreak exists so that
// two donors identical on bias AND createTime resolve the same way on every
// pass. Without it the winner follows Go's randomized map iteration, and a
// site's egress ip could flap between two equally-rated exits on consecutive
// placements for no reason the user could ever observe or report.
func TestBetterAffinityDonorIsDeterministic(t *testing.T) {
	now := time.Now()

	if !betterAffinityDonor(0.9, now, "b", 0.5, now, "a") {
		t.Error("higher bias must win regardless of the id order")
	}
	if betterAffinityDonor(0.5, now, "a", 0.9, now, "b") {
		t.Error("lower bias must lose regardless of the id order")
	}
	if !betterAffinityDonor(0.5, now.Add(time.Second), "z", 0.5, now, "a") {
		t.Error("on equal bias the more recent donor must win")
	}
	if betterAffinityDonor(0.5, now, "z", 0.5, now.Add(time.Second), "a") {
		t.Error("on equal bias the older donor must lose")
	}
	if !betterAffinityDonor(0.5, now, "a", 0.5, now, "b") {
		t.Error("on a total tie the lower provider id must win, so the result is stable across passes")
	}
	if betterAffinityDonor(0.5, now, "b", 0.5, now, "a") {
		t.Error("the id tiebreak must be antisymmetric")
	}
}
