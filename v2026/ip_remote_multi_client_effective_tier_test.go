package connect

import (
	"context"
	"strings"
	"testing"
	"time"
)

// effectiveTierTestChannel builds a bare channel at a static platform tier
// with the shipped defaults, so EffectiveTierSelection is on (the default) and
// every demerit input starts clean.
func effectiveTierTestChannel(tier int) *multiClientChannel {
	return &multiClientChannel{
		args:     &multiClientChannelArgs{DestinationStats: DestinationStats{Tier: tier}},
		settings: DefaultMultiClientSettings(),
	}
}

// starveChannel lands three dial strikes spanning two destinations -- the
// minimum that satisfies both the failure threshold and the distinct-
// destination span, so dialStarved reports true.
func starveChannel(c *multiClientChannel) {
	c.addDialFailure("93.184.216.34", 4)
	c.addDialFailure("142.250.74.100", 4)
	c.addDialFailure("93.184.216.34", 4)
}

// The demerit table: each live signal adds its quantum on top of the static
// tier, immediately -- the next selection pass reads the demoted rank. This is
// the decision effectiveTier() ships, driven directly on bare channels.
func TestEffectiveTierDecisionTable(t *testing.T) {
	// clean channel: effective == static
	clean := effectiveTierTestChannel(0)
	AssertEqual(t, clean.effectiveTier(), 0)

	// dial starvation: +2, and it decays with the strike window (see
	// TestDialStarvedPrunesOldFailures for the decay itself)
	starved := effectiveTierTestChannel(0)
	starveChannel(starved)
	AssertEqual(t, starved.effectiveTier(), 2)

	// an active quarantine: +2
	quarantined := effectiveTierTestChannel(0)
	quarantined.setQuarantined(blackholeNoReceiveAck)
	AssertEqual(t, quarantined.effectiveTier(), 2)

	// survived-quarantine memory: the demerit outlives the lift
	survived := effectiveTierTestChannel(0)
	survived.setQuarantined(blackholeNoReceiveAck)
	survived.clearQuarantine()
	AssertEqual(t, survived.effectiveTier(), 2)

	// active + memory is one demerit, not two: the memory IS the episode's
	// demerit outliving it, not a second offense
	requarantined := effectiveTierTestChannel(0)
	requarantined.setQuarantined(blackholeNoReceiveAck)
	requarantined.clearQuarantine()
	requarantined.setQuarantined(blackholeNoReceiveSyn)
	AssertEqual(t, requarantined.effectiveTier(), 2)

	// demerits stack across signals: starved + survived = +4
	combined := effectiveTierTestChannel(0)
	starveChannel(combined)
	combined.setQuarantined(blackholeNoReceiveAck)
	combined.clearQuarantine()
	AssertEqual(t, combined.effectiveTier(), 4)

	// the unhealthy stats window: +1, read from the flag the last coalesce
	// computed. The lastUnhealthyTime guard means a channel that has never
	// coalesced stats (healthy's zero value is false) stays at its static
	// tier -- asserted by `clean` above -- while an observed-unhealthy
	// channel is demoted one step
	unhealthy := effectiveTierTestChannel(0)
	unhealthy.lastUnhealthyTime = time.Now()
	AssertEqual(t, unhealthy.effectiveTier(), 1)

	// an outstanding busy-flow liveness probe (the suspect demerit): +1. The
	// evidence is a QUESTION -- the channel's flow acks are stalled and the
	// probe is mid-flight -- so it is one step, enough to steer a new flow to a
	// clean peer of the same tier for the ~1.5s the probe runs, not the +2 the
	// evidence-of-failure demerits carry.
	suspect := effectiveTierTestChannel(0)
	suspect.setBusyProbeOutstanding(true)
	AssertEqual(t, suspect.effectiveTier(), 1)

	// it clears on the probe ack, so an acquitted exit is instantly back at its
	// static rank -- the probe answered, there is nothing left to suspect
	acquitted := effectiveTierTestChannel(0)
	acquitted.setBusyProbeOutstanding(true)
	acquitted.addBusyProbeAck()
	AssertEqual(t, acquitted.effectiveTier(), 0)

	// and it stacks with the rest: starved + survived + suspect = +5
	suspectAndWorse := effectiveTierTestChannel(0)
	starveChannel(suspectAndWorse)
	suspectAndWorse.setQuarantined(blackholeNoReceiveAck)
	suspectAndWorse.clearQuarantine()
	suspectAndWorse.setBusyProbeOutstanding(true)
	AssertEqual(t, suspectAndWorse.effectiveTier(), 5)

	// everything at once, on a nonzero static tier
	worst := effectiveTierTestChannel(1)
	starveChannel(worst)
	worst.setQuarantined(blackholeNoReceiveAck)
	worst.clearQuarantine()
	worst.lastUnhealthyTime = time.Now()
	worst.setBusyProbeOutstanding(true)
	AssertEqual(t, worst.effectiveTier(), 7)

	// the toggle off is the static-Tier A/B comparison point: the same
	// demerits change nothing
	off := effectiveTierTestChannel(0)
	off.settings.EffectiveTierSelection = false
	starveChannel(off)
	off.setQuarantined(blackholeNoReceiveAck)
	off.clearQuarantine()
	off.lastUnhealthyTime = time.Now()
	off.setBusyProbeOutstanding(true)
	AssertEqual(t, off.effectiveTier(), 0)

	// a channel built with nil settings (the oldest fixture idiom) reads the
	// zero-value ReliabilitySettings -- toggle off -- and must be static and
	// panic-free, per the Tier() nil-safety precedent
	bare := &multiClientChannel{}
	starveChannel(bare)
	AssertEqual(t, bare.effectiveTier(), 0)
}

// Promotion back is slow and must be earned: the survived-quarantine demerit
// expires only when the clean interval AND a proven post-lift connect success
// hold together. Either alone keeps the exit demoted.
func TestEffectiveTierPromoteSlow(t *testing.T) {
	// the clean interval alone does not promote: no positive evidence
	idle := effectiveTierTestChannel(0)
	idle.setQuarantined(blackholeNoReceiveAck)
	idle.clearQuarantine()
	idle.quarantineLiftTime = time.Now().Add(-quarantineMemoryDuration - time.Minute)
	AssertEqual(t, idle.effectiveTier(), 2)

	// positive evidence alone does not promote: the clean interval has not
	// elapsed since the lift
	fresh := effectiveTierTestChannel(0)
	fresh.setQuarantined(blackholeNoReceiveAck)
	fresh.clearQuarantine()
	fresh.addConnectSuccess(4)
	AssertEqual(t, fresh.effectiveTier(), 2)

	// both together promote, and the memory decays rather than being masked
	earned := effectiveTierTestChannel(0)
	earned.setQuarantined(blackholeNoReceiveAck)
	earned.clearQuarantine()
	earned.quarantineLiftTime = time.Now().Add(-quarantineMemoryDuration - time.Minute)
	earned.addConnectSuccess(4)
	AssertEqual(t, earned.effectiveTier(), 0)
	AssertEqual(t, earned.survivedQuarantine, false)

	// a new episode's lift resets the evidence requirement: a connect success
	// from before the re-quarantine does not count toward the new memory
	relifted := effectiveTierTestChannel(0)
	relifted.setQuarantined(blackholeNoReceiveAck)
	relifted.clearQuarantine()
	relifted.addConnectSuccess(4)
	relifted.setQuarantined(blackholeNoReceiveAck)
	relifted.clearQuarantine()
	relifted.quarantineLiftTime = time.Now().Add(-quarantineMemoryDuration - time.Minute)
	AssertEqual(t, relifted.effectiveTier(), 2)
}

// The consumption point: minTierClients ranks on effectiveTier, so demerits
// reorder the race field with no other selection code changing -- a demoted
// tier-0 exit loses the field to a clean tier-1, and raceCandidates'
// tier-crossing / least-loaded overflow consume the result exactly as before.
func TestMinTierClientsReordersOnDemerits(t *testing.T) {
	settings := DefaultMultiClientSettings()
	tiered := func(tier int) *multiClientChannel {
		return &multiClientChannel{
			args:     &multiClientChannelArgs{DestinationStats: DestinationStats{Tier: tier}},
			settings: settings,
		}
	}

	// a starved tier-0 (effective 2) falls behind a clean tier-1 (effective 1)
	demoted := tiered(0)
	starveChannel(demoted)
	cleanNext := tiered(1)
	kept := minTierClients([]*multiClientChannel{demoted, cleanNext})
	if len(kept) != 1 || kept[0] != cleanNext {
		t.Fatalf("got %d clients, want only the clean tier-1: the demoted tier-0 must lose the field", len(kept))
	}

	// quantization: the +2 starvation demerit lands the tier-0 exactly on a
	// clean tier-2, so the two tie and both stay in the race
	cleanFar := tiered(2)
	kept = minTierClients([]*multiClientChannel{demoted, cleanFar})
	if len(kept) != 2 {
		t.Fatalf("got %d clients, want both: a starved tier-0 ranks equal to a clean tier-2", len(kept))
	}

	// the A/B toggle: off, the same channels rank statically and the tier-0
	// wins despite its strikes
	offSettings := DefaultMultiClientSettings()
	offSettings.EffectiveTierSelection = false
	staticDemoted := &multiClientChannel{
		args:     &multiClientChannelArgs{DestinationStats: DestinationStats{Tier: 0}},
		settings: offSettings,
	}
	starveChannel(staticDemoted)
	staticClean := &multiClientChannel{
		args:     &multiClientChannelArgs{DestinationStats: DestinationStats{Tier: 1}},
		settings: offSettings,
	}
	kept = minTierClients([]*multiClientChannel{staticDemoted, staticClean})
	if len(kept) != 1 || kept[0] != staticDemoted {
		t.Fatalf("toggle off: got %d clients, want the static tier-0 alone", len(kept))
	}
}

// The rank gate must actually consume effectiveTier -- a demerit function that
// is correct but unconsulted reorders nothing, which is the failure shape this
// codebase has shipped more than once.
func TestMinTierClientsConsumesEffectiveTier(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func minTierClients(")
	if !ok {
		t.Fatal("could not find minTierClients")
	}
	if !strings.Contains(body, "effectiveTier()") {
		t.Error("minTierClients does not rank on effectiveTier: demerits never reorder selection")
	}
}

// The exit readout carries both ranks, so the developer screen can show
// "tier N→M" for a demoted exit.
func TestExitsReportEffectiveTier(t *testing.T) {
	settings := DefaultMultiClientSettings()
	client := &multiClientChannel{
		ctx:      context.Background(),
		args:     &multiClientChannelArgs{DestinationStats: DestinationStats{Tier: 1}},
		settings: settings,
	}
	starveChannel(client)

	mc := &RemoteUserNatMultiClient{
		settings: settings,
		windows: map[WindowType]*multiClientWindow{
			WindowTypeQuality: &multiClientWindow{
				settings: settings,
				clients:  map[Id]*multiClientChannel{{}: client},
			},
		},
	}

	exits := mc.Exits()
	AssertEqual(t, len(exits), 1)
	AssertEqual(t, exits[0].Tier, 1)
	AssertEqual(t, exits[0].EffectiveTier, 3)
}

// The destination gate on the no-receive-ack verdict, driven through the pure
// decision: one silent destination is not evidence about the exit, two are,
// and the 0/1 settings reproduce the pre-change behavior exactly.
func TestBlackholeDestinationGate(t *testing.T) {
	receiveSilent := func(destinations int) *clientWindowStats {
		return &clientWindowStats{
			log:                  DefaultLogger(),
			firstSendNackTime:    time.Now().Add(-21 * time.Second),
			sendAckCount:         7,
			receiveAckCount:      0,
			sendDestinationCount: destinations,
		}
	}

	// one destination silent: no verdict -- and not "held" either, the
	// evidence is insufficient rather than inadmissible
	reason, held := blackholeReasonFromStats(
		time.Now(), receiveSilent(1),
		5*time.Second, 20*time.Second, 30*time.Second,
		blackholeGates{minReceiveAckDestinations: 2},
	)
	if reason != blackholeNone {
		t.Errorf("a single silent destination convicted the exit: %s", reason)
	}
	if held != blackholeNone {
		t.Errorf("insufficient evidence was reported held: %q", held)
	}

	// two destinations silent: corroborated, the verdict fires
	reason, _ = blackholeReasonFromStats(
		time.Now(), receiveSilent(2),
		5*time.Second, 20*time.Second, 30*time.Second,
		blackholeGates{minReceiveAckDestinations: 2},
	)
	if reason != blackholeNoReceiveAck {
		t.Errorf("two silent destinations: reason = %q, want %q", reason, blackholeNoReceiveAck)
	}

	// 0 and 1 are today's behavior: a single destination convicts
	for _, minDestinations := range []int{0, 1} {
		reason, _ = blackholeReasonFromStats(
			time.Now(), receiveSilent(1),
			5*time.Second, 20*time.Second, 30*time.Second,
			blackholeGates{minReceiveAckDestinations: minDestinations},
		)
		if reason != blackholeNoReceiveAck {
			t.Errorf("min=%d: reason = %q, want %q (pre-change behavior)", minDestinations, reason, blackholeNoReceiveAck)
		}
	}

	// the hard no-send-ack verdict is untouched by the gate: a provider
	// acknowledging nothing is gone, however few destinations were tried
	silent := receiveSilent(1)
	silent.sendAckCount = 0
	silent.firstSendNackTime = time.Now().Add(-10 * time.Second)
	reason, _ = blackholeReasonFromStats(
		time.Now(), silent,
		5*time.Second, 20*time.Second, 30*time.Second,
		blackholeGates{minReceiveAckDestinations: 2},
	)
	if reason != blackholeNoSendAck {
		t.Errorf("the destination gate touched the send verdict: reason = %q, want %q", reason, blackholeNoSendAck)
	}

	// the syn branch is deliberately ungated too: it already convicts only an
	// exit that has established nothing at all
	synStats := &clientWindowStats{
		log:                  DefaultLogger(),
		firstSendSynTime:     time.Now().Add(-31 * time.Second),
		sendSynCount:         5,
		sendDestinationCount: 1,
	}
	reason, _ = blackholeReasonFromStats(
		time.Now(), synStats,
		5*time.Second, 20*time.Second, 30*time.Second,
		blackholeGates{minReceiveAckDestinations: 2},
	)
	if reason != blackholeNoReceiveSyn {
		t.Errorf("the destination gate touched the syn verdict: reason = %q, want %q", reason, blackholeNoReceiveSyn)
	}
}

// detectBlackhole must feed the gate from the runtime-overridable setting and
// carry the destination count in the one line that survives into a field log.
func TestBlackholeVerdictLogsDestinationCount(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *multiClientChannel) detectBlackhole()")
	if !ok {
		t.Fatal("could not find detectBlackhole")
	}
	if !strings.Contains(body, "MinBlackholeDestinations") {
		t.Error("detectBlackhole does not feed the destination gate from the setting, so the control has no effect")
	}
	if !strings.Contains(body, "dsts=%d") {
		t.Error("the verdict error does not carry dsts=, so a field capture cannot tell many-destination silence from one dead website")
	}
}

// The bench-leak release decision, driven directly: a quarantined exit whose
// evidence has evaporated (no verdict firing, none held) and whose flows have
// drained is released; a loaded exit, or one whose evidence is merely held by
// a gate, keeps waiting.
func TestQuarantineVacatedTable(t *testing.T) {
	cases := []struct {
		name        string
		quarantined bool
		reason      blackholeReason
		held        blackholeReason
		flowCount   int
		want        bool
	}{
		{"flowless, evidence gone", true, blackholeNone, blackholeNone, 0, true},
		{"loaded keeps waiting", true, blackholeNone, blackholeNone, 3, false},
		{"verdict still firing", true, blackholeNoReceiveAck, blackholeNone, 0, false},
		{"verdict held by a gate is not gone", true, blackholeNone, blackholeNoReceiveAck, 0, false},
		{"not quarantined", false, blackholeNone, blackholeNone, 0, false},
	}
	for _, c := range cases {
		if got := quarantineVacated(c.quarantined, c.reason, c.held, c.flowCount); got != c.want {
			t.Errorf("%s: quarantineVacated = %v, want %v", c.name, got, c.want)
		}
	}
}

// The release transition end to end at the channel: a flowless quarantined
// channel whose evidence is gone clears -- and returns to selection demoted,
// carrying the survived-quarantine memory -- while a loaded one stays
// benched. The nil flowCountFunc convention (bare fixture reads 0 flows)
// matches the production drain: the release happens exactly when the count
// reaches zero.
func TestQuarantineBenchLeakRelease(t *testing.T) {
	// flowless: released, and demoted rather than acquitted
	drained := effectiveTierTestChannel(0)
	drained.setQuarantined(blackholeNoReceiveAck)
	if quarantineVacated(drained.isQuarantined(), blackholeNone, blackholeNone, drained.flowCount()) {
		drained.clearQuarantine()
	}
	AssertEqual(t, drained.isQuarantined(), false)
	AssertEqual(t, drained.effectiveTier(), 2)

	// loaded: the flows are the receive source that can genuinely acquit it,
	// so it keeps waiting for receive progress or the expiry bound
	loaded := effectiveTierTestChannel(0)
	loaded.flowCountFunc = func(*multiClientChannel) int { return 3 }
	loaded.setQuarantined(blackholeNoReceiveAck)
	if quarantineVacated(loaded.isQuarantined(), blackholeNone, blackholeNone, loaded.flowCount()) {
		loaded.clearQuarantine()
	}
	AssertEqual(t, loaded.isQuarantined(), true)
}

// The release must actually be wired into the verdict loop's no-verdict
// branch, or the leak stays leaked.
func TestQuarantineBenchLeakSiteAnchor(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *multiClientChannel) detectBlackhole()")
	if !ok {
		t.Fatal("could not find detectBlackhole")
	}
	if !strings.Contains(body, "quarantineVacated(") {
		t.Error("detectBlackhole does not consult quarantineVacated: a drained quarantined exit stays benched until rotation")
	}
	if !strings.Contains(body, "clearQuarantine()") {
		t.Error("detectBlackhole cannot clear a vacated quarantine, so the release decision has no effect")
	}
}

// Every lift site must record the survived-quarantine memory, or an exit
// acquitted by one path returns to the top rank while the same exit acquitted
// by another stays demoted. Both lifts route through the shared clear.
func TestQuarantineLiftRecordsMemory(t *testing.T) {
	// the explicit clear (bench-leak release, tests)
	cleared := effectiveTierTestChannel(0)
	cleared.setQuarantined(blackholeNoReceiveAck)
	cleared.clearQuarantine()
	AssertEqual(t, cleared.survivedQuarantine, true)

	// the receive-progress lift in addReceiveAck
	acquitted := effectiveTierTestChannel(0)
	acquitted.packetStats = &clientWindowStats{log: DefaultLogger()}
	acquitted.setQuarantined(blackholeNoReceiveAck)
	acquitted.addReceiveAck(1440)
	AssertEqual(t, acquitted.isQuarantined(), false)
	AssertEqual(t, acquitted.survivedQuarantine, true)

	// a clear on a channel that was never quarantined must not manufacture
	// a demerit
	innocent := effectiveTierTestChannel(0)
	innocent.clearQuarantine()
	AssertEqual(t, innocent.survivedQuarantine, false)
}

// The two new knobs ship with the intended defaults and survive the round
// trip through the override type -- a missed field zeroes on every settings
// write, silently turning the behavior off.
func TestReliabilitySettingsPreferHealthyDefaults(t *testing.T) {
	settings := DefaultMultiClientSettings()
	AssertEqual(t, settings.EffectiveTierSelection, true)
	AssertEqual(t, settings.MinBlackholeDestinations, 2)

	reliabilitySettings := ReliabilitySettingsFrom(settings)
	AssertEqual(t, reliabilitySettings.EffectiveTierSelection, settings.EffectiveTierSelection)
	AssertEqual(t, reliabilitySettings.MinBlackholeDestinations, settings.MinBlackholeDestinations)

	// nil is every reliability behavior off -- the bare-fixture state, which
	// is what makes effectiveTier static on nil-settings channels
	bare := ReliabilitySettingsFrom(nil)
	AssertEqual(t, bare.EffectiveTierSelection, false)
	AssertEqual(t, bare.MinBlackholeDestinations, 0)
}

// The race field ships UNBOUNDED. A bound of 2 was tried and reverted after
// one field session: on a pool whose providers stall intermittently, two
// picks are a coin flip, and a cold-start-heavy workload (short-video feeds)
// read as constant spinners. The wide race is the tail-latency insurance
// exactly when the pool is rough; per-destination strike gating carries the
// dial-noise concern instead. The truncation machinery stays present so the
// bound remains one settings write away for pools where it fits.
func TestReliabilitySettingsMultiRaceClientCountDefault(t *testing.T) {
	settings := DefaultMultiClientSettings()
	AssertEqual(t, settings.MultiRaceClientCount, 0)

	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *RemoteUserNatMultiClient) sendParsedPacketGroup(")
	if !ok {
		t.Fatal("could not find sendParsedPacketGroup")
	}
	if !strings.Contains(body, "orderedClients[:self.settings.MultiRaceClientCount]") {
		t.Error("sendParsedPacketGroup no longer truncates the race field to MultiRaceClientCount, so the default bounds nothing")
	}
}
