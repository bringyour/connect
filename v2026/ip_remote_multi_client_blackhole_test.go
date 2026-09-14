package connect

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"
)

// The two blackhole signals are not equally strong and must not share a bound.
//
// A provider that acknowledges nothing is gone. A provider that acknowledges
// our sends but returns no destination data is demonstrably alive, and may
// simply be carrying a flow that is waiting on a slow origin. Removing an exit
// destroys every flow pinned to it, not just the quiet one, so the ambiguous
// case has to clear a higher bar.
//
// On mainnet the shared 5s bound removed 44 providers out of 44 -- roughly one
// every 18s under load -- and every one of those providers was still
// acknowledging sends, up to 602 of them / 222KB.
func TestBlackholeReceiveTimeoutIsSeparateFromSendTimeout(t *testing.T) {
	settings := DefaultMultiClientSettings()

	if settings.BlackholeReceiveTimeout <= settings.BlackholeTimeout {
		t.Errorf(
			"receive bound %v must be longer than the send bound %v: it is the weaker signal",
			settings.BlackholeReceiveTimeout, settings.BlackholeTimeout,
		)
	}
}

// The receive bound is compared against an age derived from surviving stat
// buckets, and coalesceEventBuckets drops every bucket older than
// StatsWindowDuration. So the age can never exceed roughly
// StatsWindowDuration + StatsWindowBucketDuration, and a receive bound at or
// above that ceiling never fires -- silently, with no error and no log.
//
// This guards the margin. Without it, reducing StatsWindowDuration to make
// stats more responsive would permanently disable the check while every other
// test still passed.
func TestBlackholeReceiveTimeoutIsReachable(t *testing.T) {
	settings := DefaultMultiClientSettings()

	ceiling := settings.StatsWindowDuration + settings.StatsWindowBucketDuration
	if ceiling <= settings.BlackholeReceiveTimeout {
		t.Fatalf(
			"receive bound %v is at or above the reachable ceiling %v (StatsWindowDuration %v + bucket %v): it can never fire",
			settings.BlackholeReceiveTimeout, ceiling,
			settings.StatsWindowDuration, settings.StatsWindowBucketDuration,
		)
	}

	// require real headroom, not a single bucket of luck
	if margin := ceiling - settings.BlackholeReceiveTimeout; margin < 2*settings.StatsWindowBucketDuration {
		t.Errorf(
			"receive bound %v leaves only %v under the ceiling %v; want at least %v",
			settings.BlackholeReceiveTimeout, margin, ceiling, 2*settings.StatsWindowBucketDuration,
		)
	}
}

// A provider acknowledging sends with nothing back must survive the shorter
// send bound -- this is the case that was killing healthy exits.
func TestAckingProviderSurvivesShortWindow(t *testing.T) {
	stats := &clientWindowStats{
		log:               DefaultLogger(),
		firstSendNackTime: time.Now().Add(-10 * time.Second),
		sendAckCount:      7,
		sendAckByteCount:  360,
		receiveAckCount:   0,
	}

	reason, _ := blackholeReasonFromStats(time.Now(), stats, 5*time.Second, 20*time.Second, 30*time.Second, blackholeGates{})
	if reason != blackholeNone {
		t.Errorf("a provider still acknowledging sends was removed on the send bound: %s", reason)
	}

	// once the longer receive bound elapses it is removed, and the reason says
	// which signal did it
	stats.firstSendNackTime = time.Now().Add(-21 * time.Second)
	reason, _ = blackholeReasonFromStats(time.Now(), stats, 5*time.Second, 20*time.Second, 30*time.Second, blackholeGates{})
	if reason != blackholeNoReceiveAck {
		t.Errorf("past the receive bound: reason = %q, want %q", reason, blackholeNoReceiveAck)
	}
}

// A provider acknowledging nothing is unambiguously gone and must still be
// removed quickly -- the fix must not slow down the case that works.
func TestSilentProviderStillRemovedOnSendBound(t *testing.T) {
	stats := &clientWindowStats{
		log:               DefaultLogger(),
		firstSendNackTime: time.Now().Add(-10 * time.Second),
		sendAckCount:      0,
		receiveAckCount:   0,
	}

	reason, _ := blackholeReasonFromStats(time.Now(), stats, 5*time.Second, 20*time.Second, 30*time.Second, blackholeGates{})
	if reason != blackholeNoSendAck {
		t.Errorf("silent provider: reason = %q, want %q", reason, blackholeNoSendAck)
	}
}

// 0 disables the receive check, leaving only the unambiguous signal. This is
// the setting to compare against when measuring how much churn the receive
// branch is responsible for.
func TestBlackholeReceiveTimeoutZeroDisables(t *testing.T) {
	stats := &clientWindowStats{
		log:               DefaultLogger(),
		firstSendNackTime: time.Now().Add(-10 * time.Minute),
		sendAckCount:      7,
		receiveAckCount:   0,
	}

	reason, _ := blackholeReasonFromStats(time.Now(), stats, 5*time.Second, 0, 30*time.Second, blackholeGates{})
	if reason != blackholeNone {
		t.Errorf("receive check ran with the bound disabled: %s", reason)
	}
}

// Unanswered syns alone must not remove an exit whose established traffic is
// flowing. The syn-ack only exists after the provider's upstream dial
// succeeds, so "syns out, none back" cannot distinguish a broken provider from
// a destination that silently drops connections -- and on device this branch
// removed an exit moving 48 packets / 8.7KB of return traffic because ~18 syns
// to a handful of unresponsive destinations went unanswered, destroying 276
// working connections. Removal is reserved for an exit that has established
// nothing at all; a live exit's connect trouble is handled per-flow by the
// dial-failure re-race and the dial-strike warning instead.
func TestBlackholeSynBranchSparesEstablishedTraffic(t *testing.T) {
	// the field case: syns unanswered past the bound, established flows moving
	stats := &clientWindowStats{
		log:                 DefaultLogger(),
		firstSendSynTime:    time.Now().Add(-31 * time.Second),
		sendSynCount:        18,
		receiveSynCount:     0,
		sendAckCount:        78,
		sendAckByteCount:    11071,
		receiveAckCount:     48,
		receiveAckByteCount: 8712,
	}

	reason, _ := blackholeReasonFromStats(time.Now(), stats, 5*time.Second, 20*time.Second, 30*time.Second, blackholeGates{})
	if reason != blackholeNone {
		t.Errorf("an exit with flowing established traffic was removed for unanswered syns: %s", reason)
	}

	// with nothing established the same syn silence still removes -- the fix
	// must not blind the branch to an exit that never worked at all
	stats.receiveAckCount = 0
	stats.receiveAckByteCount = 0
	reason, _ = blackholeReasonFromStats(time.Now(), stats, 5*time.Second, 20*time.Second, 30*time.Second, blackholeGates{})
	if reason != blackholeNoReceiveSyn {
		t.Errorf("an exit that established nothing was kept: reason = %q, want %q", reason, blackholeNoReceiveSyn)
	}
}

// The connect-wait clock behind the client-side dial-failure inference: it
// arms on the first syn an exit carries for the flow, trips only after the
// timeout on that same exit, and restarts when the flow moves -- otherwise the
// first syn through a fresh exit would inherit the old exit's wait and strike
// it immediately.
func TestSynWaitExceededIsPerExit(t *testing.T) {
	update := &multiClientChannelUpdate{}
	exitA := &multiClientChannel{}
	exitB := &multiClientChannel{}
	tcpPath := udpTestPath(4)
	tcpPath.Protocol = IpProtocolTcp
	tcpPath.Syn = true
	// wide enough that a scheduler or GC stall between two adjacent calls
	// cannot age the clock past the bound and fail the "does not trip"
	// assertions spuriously on a loaded runner
	timeout := 250 * time.Millisecond

	// first syn arms the clock, nothing trips
	if update.synWaitExceeded(exitA, tcpPath, timeout) {
		t.Fatal("first syn tripped the clock")
	}
	// a retransmit inside the window does not trip
	if update.synWaitExceeded(exitA, tcpPath, timeout) {
		t.Fatal("retransmit inside the window tripped the clock")
	}

	time.Sleep(timeout + 50*time.Millisecond)

	// moving to a new exit must restart the wait, not inherit the old one
	if update.synWaitExceeded(exitB, tcpPath, timeout) {
		t.Fatal("a fresh exit inherited the previous exit's wait")
	}

	time.Sleep(timeout + 50*time.Millisecond)

	// the same exit past the timeout trips
	if !update.synWaitExceeded(exitB, tcpPath, timeout) {
		t.Fatal("an aged wait on the same exit did not trip")
	}
	// and tripping restarts the clock rather than firing on every retransmit
	if update.synWaitExceeded(exitB, tcpPath, timeout) {
		t.Fatal("the clock did not restart after tripping")
	}
}

// The connect signal is independent of the send/receive bounds and keeps its
// own reason, so a capture can tell the branches apart. They previously all
// reported an identical error string, which is why 44 field removals could not
// be attributed to a branch.
func TestBlackholeReasonsAreDistinct(t *testing.T) {
	noSyn := &clientWindowStats{
		log:               DefaultLogger(),
		firstSendSynTime:  time.Now().Add(-31 * time.Second),
		firstSendNackTime: time.Time{},
		receiveSynCount:   0,
	}

	reason, _ := blackholeReasonFromStats(time.Now(), noSyn, 5*time.Second, 20*time.Second, 30*time.Second, blackholeGates{})
	if reason != blackholeNoReceiveSyn {
		t.Errorf("no syn back: reason = %q, want %q", reason, blackholeNoReceiveSyn)
	}

	seen := map[blackholeReason]bool{}
	for _, r := range []blackholeReason{
		blackholeNone, blackholeNoSendAck, blackholeNoReceiveAck, blackholeNoReceiveSyn,
	} {
		if seen[r] {
			t.Errorf("duplicate blackhole reason %q -- a capture could not tell them apart", r)
		}
		seen[r] = true
	}
}

// A clean window is not a blackhole by any signal.
func TestHealthyProviderIsNotBlackholed(t *testing.T) {
	stats := &clientWindowStats{
		log:               DefaultLogger(),
		firstSendNackTime: time.Now().Add(-60 * time.Second),
		firstSendSynTime:  time.Now().Add(-60 * time.Second),
		sendAckCount:      100,
		receiveAckCount:   100,
		receiveSynCount:   3,
	}

	reason, _ := blackholeReasonFromStats(time.Now(), stats, 5*time.Second, 20*time.Second, 30*time.Second, blackholeGates{})
	if reason != blackholeNone {
		t.Errorf("healthy provider removed: %s", reason)
	}
}

// detectBlackhole must actually consult the decision, and must pass the
// runtime-override receive bound rather than the static setting. Without this
// the decision function could be correct and simply not used -- which is the
// shape of bug that has already shipped here more than once.
func TestDetectBlackholeUsesTheReasonAndOverride(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	body, ok := functionBody(source, "func (self *multiClientChannel) detectBlackhole()")
	if !ok {
		t.Fatal("could not find detectBlackhole")
	}

	if !strings.Contains(body, "blackholeReasonFromStats(") {
		t.Error("detectBlackhole does not call blackholeReasonFromStats: the decision is not reached")
	}
	if !strings.Contains(body, "self.reliabilitySettings().BlackholeReceiveTimeout") {
		t.Error("detectBlackhole does not read the receive bound from the runtime override, so the developer control has no effect")
	}
	if !strings.Contains(body, "reason") {
		t.Error("detectBlackhole does not report the reason, so a capture cannot attribute a removal to a branch")
	}
}

// The receive verdicts convict on silence, and during a network migration
// silence is tunnel-wide: nothing from any provider can arrive, so every exit
// looks identically guilty. On device one wifi migration executed 7 exits in
// 79s, every verdict `no-receive-ack recv 0/0B`. While the uplink is stale
// those verdicts must be held -- and reported as held, so the gate is
// countable rather than silently eating verdicts.
func TestBlackholeUplinkStaleHoldsReceiveVerdicts(t *testing.T) {
	// the field case: acking provider, nothing received, past the receive bound
	stats := &clientWindowStats{
		log:               DefaultLogger(),
		firstSendNackTime: time.Now().Add(-21 * time.Second),
		sendAckCount:      7,
		receiveAckCount:   0,
	}

	reason, held := blackholeReasonFromStats(time.Now(), stats, 5*time.Second, 20*time.Second, 30*time.Second, blackholeGates{uplinkStale: true})
	if reason != blackholeNone {
		t.Errorf("a receive verdict fired through a stale uplink: %s", reason)
	}
	if held != blackholeNoReceiveAck {
		t.Errorf("held = %q, want %q -- a gate that hides what it held cannot be measured", held, blackholeNoReceiveAck)
	}

	// the syn branch is a receive verdict too: nothing established, silence
	// past the connect bound
	synStats := &clientWindowStats{
		log:              DefaultLogger(),
		firstSendSynTime: time.Now().Add(-31 * time.Second),
		sendSynCount:     18,
	}
	reason, held = blackholeReasonFromStats(time.Now(), synStats, 5*time.Second, 20*time.Second, 30*time.Second, blackholeGates{uplinkStale: true})
	if reason != blackholeNone {
		t.Errorf("the syn verdict fired through a stale uplink: %s", reason)
	}
	if held != blackholeNoReceiveSyn {
		t.Errorf("held = %q, want %q", held, blackholeNoReceiveSyn)
	}
}

// On a reliable carrier, no-send-ack is an unambiguous signal and must never be
// gated or rebased by uplink staleness: successful carrier delivery leaves the
// provider as the missing acknowledgement owner.
func TestBlackholeUplinkGateNeverHoldsReliableNoSendAck(t *testing.T) {
	stats := &clientWindowStats{
		log:               DefaultLogger(),
		firstSendNackTime: time.Now().Add(-10 * time.Second),
		sendAckCount:      0,
		receiveAckCount:   0,
	}

	// a stale uplink AND a rebase point younger than the nack clock: neither
	// may touch the send verdict
	gates := blackholeGates{
		uplinkStale:       true,
		receiveFreshSince: time.Now().Add(-1 * time.Second),
	}
	reason, held := blackholeReasonFromStats(time.Now(), stats, 5*time.Second, 20*time.Second, 30*time.Second, gates)
	if reason != blackholeNoSendAck {
		t.Errorf("the uplink gate touched the send verdict: reason = %q, want %q", reason, blackholeNoSendAck)
	}
	if held != blackholeNone {
		t.Errorf("the send verdict was reported held: %q", held)
	}
}

// QUIC DATAGRAM transport-up proves only that the carrier accepts writes. A
// Pack or its Ack can still be lost while Transfer is recovering it, including
// when unrelated datagrams are getting through. Transfer's bounded Ack retry
// lifetime is therefore the hard verdict for an unreliable carrier; the short
// blackhole window must not race it in either uplink state.
func TestBlackholeDefersUnreliableNoSendAckToTransfer(t *testing.T) {
	now := time.Now()
	stats := &clientWindowStats{
		log:               DefaultLogger(),
		firstSendNackTime: now.Add(-10 * time.Second),
	}

	for _, uplinkStale := range []bool{false, true} {
		gates := blackholeGates{
			uplinkStale:             uplinkStale,
			unreliableSendTransport: true,
		}
		reason, held := blackholeReasonFromStats(
			now,
			stats,
			5*time.Second,
			20*time.Second,
			30*time.Second,
			gates,
		)
		if reason != blackholeNone || held != blackholeNoSendAck {
			t.Fatalf("uplinkStale=%t unreliable carrier reason=%q held=%q, want none/%q", uplinkStale, reason, held, blackholeNoSendAck)
		}
	}
}

// When a gated epoch ends, the receive-branch clocks count from the epoch end
// rather than the original first-send time -- otherwise every verdict held
// across the silence matures at once on unfreeze and the executions merely
// arrive in a burst instead of a drip.
func TestBlackholeRebaseRestartsReceiveClocks(t *testing.T) {
	now := time.Now()
	stats := &clientWindowStats{
		log:               DefaultLogger(),
		firstSendNackTime: now.Add(-25 * time.Second),
		sendAckCount:      7,
		receiveAckCount:   0,
	}

	// ungated, the verdict is mature and fires
	reason, _ := blackholeReasonFromStats(now, stats, 5*time.Second, 20*time.Second, 30*time.Second, blackholeGates{})
	if reason != blackholeNoReceiveAck {
		t.Fatalf("baseline: reason = %q, want %q", reason, blackholeNoReceiveAck)
	}

	// a stale epoch ended 3s ago: the clock restarts there, so no verdict --
	// and nothing held either, because nothing would have fired
	reason, held := blackholeReasonFromStats(now, stats, 5*time.Second, 20*time.Second, 30*time.Second, blackholeGates{receiveFreshSince: now.Add(-3 * time.Second)})
	if reason != blackholeNone {
		t.Errorf("a rebased clock still fired: %s", reason)
	}
	if held != blackholeNone {
		t.Errorf("a verdict that was not firing was reported held: %q", held)
	}

	// once a fresh full receive window elapses after the epoch end, silence
	// convicts again -- the rebase is a restart, not immunity
	reason, _ = blackholeReasonFromStats(now, stats, 5*time.Second, 20*time.Second, 30*time.Second, blackholeGates{receiveFreshSince: now.Add(-21 * time.Second)})
	if reason != blackholeNoReceiveAck {
		t.Errorf("a fully re-aged clock did not fire: reason = %q, want %q", reason, blackholeNoReceiveAck)
	}

	// the syn clock rebases the same way
	synStats := &clientWindowStats{
		log:              DefaultLogger(),
		firstSendSynTime: now.Add(-40 * time.Second),
		sendSynCount:     5,
	}
	reason, _ = blackholeReasonFromStats(now, synStats, 5*time.Second, 20*time.Second, 30*time.Second, blackholeGates{receiveFreshSince: now.Add(-5 * time.Second)})
	if reason != blackholeNone {
		t.Errorf("a rebased syn clock still fired: %s", reason)
	}
}

// A channel whose transport set is empty cannot deliver or receive anything,
// so its silence proves nothing about the provider: every verdict is held,
// including the otherwise-ungated no-send-ack.
func TestBlackholeTransportDownHoldsAllVerdicts(t *testing.T) {
	now := time.Now()
	cases := []struct {
		stats *clientWindowStats
		want  blackholeReason
	}{
		// silent provider that would be convicted on the send bound
		{&clientWindowStats{
			log:               DefaultLogger(),
			firstSendNackTime: now.Add(-10 * time.Second),
		}, blackholeNoSendAck},
		// acking-then-quiet provider that would be convicted on the receive bound
		{&clientWindowStats{
			log:               DefaultLogger(),
			firstSendNackTime: now.Add(-21 * time.Second),
			sendAckCount:      7,
		}, blackholeNoReceiveAck},
		// nothing-established provider that would be convicted on the syn bound
		{&clientWindowStats{
			log:              DefaultLogger(),
			firstSendSynTime: now.Add(-31 * time.Second),
			sendSynCount:     3,
		}, blackholeNoReceiveSyn},
	}
	for _, c := range cases {
		reason, held := blackholeReasonFromStats(now, c.stats, 5*time.Second, 20*time.Second, 30*time.Second, blackholeGates{transportDown: true})
		if reason != blackholeNone {
			t.Errorf("a verdict fired with the transport down: %s", reason)
		}
		if held != c.want {
			t.Errorf("held = %q, want %q", held, c.want)
		}
	}

	// a window with nothing firing reports nothing held: held is a suppressed
	// verdict, not a gate-engaged flag
	healthy := &clientWindowStats{
		log:               DefaultLogger(),
		firstSendNackTime: now.Add(-60 * time.Second),
		sendAckCount:      100,
		receiveAckCount:   100,
	}
	reason, held := blackholeReasonFromStats(now, healthy, 5*time.Second, 20*time.Second, 30*time.Second, blackholeGates{transportDown: true})
	if reason != blackholeNone || held != blackholeNone {
		t.Errorf("a healthy window reported reason %q held %q with the transport down", reason, held)
	}
}

// An empty route set is exculpatory only for a bounded restoration grace. The
// clock is separate from provider-traffic stats because those stats eventually
// age to zero/zero (healthy) while a permanently route-less channel still
// cannot carry a single packet.
func TestTransportDownExpiryBoundsTheVerdictHold(t *testing.T) {
	now := time.Now()
	maxHold := 15 * time.Second

	if transportDownExpired(now, time.Time{}, maxHold) {
		t.Fatal("a transport that never entered a down epoch expired")
	}
	if transportDownExpired(now, now.Add(-maxHold), 0) {
		t.Fatal("a disabled transport-down bound expired")
	}
	if transportDownExpired(now, now.Add(-maxHold+time.Nanosecond), maxHold) {
		t.Fatal("transport-down grace expired early")
	}
	if !transportDownExpired(now, now.Add(-maxHold), maxHold) {
		t.Fatal("transport-down grace did not expire at its bound")
	}
}

// Exercise the same route-set predicate and epoch state machine used by the
// live detector. A migration that restores a route inside the grace must erase
// the old epoch, so elapsed time from the withdrawal cannot later retire the
// channel. A second withdrawal starts a fresh epoch rather than inheriting the
// first one's age.
func TestTransportDownEpochResetsWhenRouteReturns(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	routeManager := NewRouteManager(ctx, "transport-down-epoch-test")
	transport := NewSendGatewayTransport()
	route := make(chan []byte)
	t0 := time.Now()
	maxHold := 15 * time.Second

	transportDownSince, entered, restored := updateTransportDownEpoch(
		t0,
		!routeManager.HasActiveTransport(),
		time.Time{},
	)
	if !entered || restored || !transportDownSince.Equal(t0) {
		t.Fatalf("initial withdrawal = (%v, entered=%v, restored=%v), want (%v, true, false)", transportDownSince, entered, restored, t0)
	}

	routeManager.UpdateTransport(transport, []Route{route})
	transportDownSince, entered, restored = updateTransportDownEpoch(
		t0.Add(maxHold/2),
		!routeManager.HasActiveTransport(),
		transportDownSince,
	)
	if entered || !restored || !transportDownSince.IsZero() {
		t.Fatalf("route restoration = (%v, entered=%v, restored=%v), want (zero, false, true)", transportDownSince, entered, restored)
	}
	if transportDownExpired(t0.Add(2*maxHold), transportDownSince, maxHold) {
		t.Fatal("the erased withdrawal epoch expired after its route returned")
	}

	routeManager.RemoveTransport(transport)
	secondDown := t0.Add(2 * maxHold)
	transportDownSince, entered, restored = updateTransportDownEpoch(
		secondDown,
		!routeManager.HasActiveTransport(),
		transportDownSince,
	)
	if !entered || restored || !transportDownSince.Equal(secondDown) {
		t.Fatalf("second withdrawal = (%v, entered=%v, restored=%v), want (%v, true, false)", transportDownSince, entered, restored, secondDown)
	}
}

// uplinkGateTestParent builds a bare parent whose window carries
// sendingChannels channels that each hold one outstanding send -- the state
// the gate's degenerate-case guard counts.
func uplinkGateTestParent(sendingChannels int) *RemoteUserNatMultiClient {
	mc := &RemoteUserNatMultiClient{
		settings:      DefaultMultiClientSettings(),
		clientUpdates: map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
	}
	for range sendingChannels {
		client := stallTestChannel()
		client.addSend(1440, udpTestPath(4))
		mc.clientUpdates[client] = map[*multiClientChannelUpdate]bool{
			new(multiClientChannelUpdate): true,
		}
	}
	return mc
}

// The gate's lifecycle against one continuous silence: engage past the gate
// bound, disengage past the hard cap so a genuinely dead window can still be
// recycled, and rebase the verdict clocks only when receiving actually
// resumes.
func TestBlackholeUplinkGateStaleEpochAndCap(t *testing.T) {
	mc := uplinkGateTestParent(2)
	now := time.Now()

	// ingress within the gate: fresh, nothing to rebase
	mc.uplinkLastIngressNanos.Store(now.Add(-1 * time.Second).UnixNano())
	stale, freshSince := mc.uplinkGate(now)
	if stale {
		t.Fatal("a fresh uplink read as stale")
	}
	if !freshSince.IsZero() {
		t.Errorf("never-stale must rebase nothing: freshSince = %v", freshSince)
	}

	// silence past the gate opens a stale epoch
	mc.uplinkLastIngressNanos.Store(now.Add(-6 * time.Second).UnixNano())
	stale, freshSince = mc.uplinkGate(now)
	if !stale {
		t.Fatal("tunnel-wide silence past the gate did not read as stale")
	}
	if !freshSince.IsZero() {
		t.Errorf("no earlier epoch to rebase from: freshSince = %v", freshSince)
	}

	// past the hard cap the gate stops applying -- a window whose every
	// provider is dead is also tunnel-wide silence, and it must still recycle
	capped := now.Add(uplinkStalenessMaxHold + time.Second)
	stale, freshSince = mc.uplinkGate(capped)
	if stale {
		t.Error("the gate still held past the hard cap")
	}
	// the epoch stays open at the cap: advancing the rebase point here would
	// rebase away the very recycle the cap exists to allow
	if !freshSince.IsZero() {
		t.Errorf("the cap advanced the rebase point: freshSince = %v", freshSince)
	}

	// receiving resumes: the epoch closes and the rebase point records when
	resumed := capped.Add(2 * time.Second)
	mc.uplinkLastIngressNanos.Store(resumed.Add(-100 * time.Millisecond).UnixNano())
	stale, freshSince = mc.uplinkGate(resumed)
	if stale {
		t.Error("a resumed uplink read as stale")
	}
	if !freshSince.Equal(resumed) {
		t.Errorf("freshSince = %v, want the epoch end %v", freshSince, resumed)
	}
}

// TestNetworkChangeStampsUplinkEpoch: NotifyNetworkChanged is a legitimate
// fresh start for the staleness clocks. It must close any open stale epoch and
// rebase uplinkFreshSince to the change instant, and it must fire the
// process-wide NetworkChanged broadcast (the transport kick). If the tunnel
// stays silent afterwards, the gate re-engages as a NEW epoch — fresh hold-cap
// window — rather than inheriting the old one.
func TestNetworkChangeStampsUplinkEpoch(t *testing.T) {
	mc := uplinkGateTestParent(2)
	now := time.Now()

	// open a stale epoch
	mc.uplinkLastIngressNanos.Store(now.Add(-6 * time.Second).UnixNano())
	if stale, _ := mc.uplinkGate(now); !stale {
		t.Fatal("tunnel-wide silence past the gate did not read as stale")
	}

	// the broadcast half: the registered listeners (in production, the
	// platform transports' Kick) must fire exactly once per change
	kicks := 0
	unsub := AddNetworkChangeListener(func() {
		kicks += 1
	})
	defer unsub()

	before := time.Now()
	mc.NotifyNetworkChanged()

	if kicks != 1 {
		t.Fatalf("network change fired %d kicks, want 1", kicks)
	}
	var staleSince, freshSince time.Time
	func() {
		mc.uplinkStateLock.Lock()
		defer mc.uplinkStateLock.Unlock()
		staleSince = mc.uplinkStaleSince
		freshSince = mc.uplinkFreshSince
	}()
	if !staleSince.IsZero() {
		t.Errorf("the network change left the stale epoch open: staleSince = %v", staleSince)
	}
	if freshSince.Before(before) {
		t.Errorf("freshSince = %v, want at or after the change instant %v", freshSince, before)
	}

	// still silent after the change: the gate re-engages as a new epoch, and
	// the rebase point stays at the change stamp
	later := time.Now().Add(time.Second)
	stale, gateFreshSince := mc.uplinkGate(later)
	if !stale {
		t.Fatal("continued silence after the change did not re-engage the gate")
	}
	if !gateFreshSince.Equal(freshSince) {
		t.Errorf("re-engaged epoch rebase point = %v, want the change stamp %v", gateFreshSince, freshSince)
	}
	var newStaleSince time.Time
	func() {
		mc.uplinkStateLock.Lock()
		defer mc.uplinkStateLock.Unlock()
		newStaleSince = mc.uplinkStaleSince
	}()
	if newStaleSince.Before(before) {
		t.Errorf("the re-engaged epoch inherited the old start %v; its hold cap must clock from after the change", newStaleSince)
	}
}

// With fewer than two channels talking, tunnel-wide silence is
// indistinguishable from that one provider being dead, so the gate carries
// zero exculpatory information and must not engage.
func TestBlackholeUplinkGateRequiresTwoSendingChannels(t *testing.T) {
	mc := uplinkGateTestParent(1)
	now := time.Now()
	mc.uplinkLastIngressNanos.Store(now.Add(-10 * time.Second).UnixNano())

	if stale, _ := mc.uplinkGate(now); stale {
		t.Error("the gate engaged with a single talking channel")
	}

	// a channel with nothing outstanding is not talking and must not count
	idle := stallTestChannel()
	mc.clientUpdates[idle] = map[*multiClientChannelUpdate]bool{
		new(multiClientChannelUpdate): true,
	}
	if stale, _ := mc.uplinkGate(now); stale {
		t.Error("an idle channel counted toward the degenerate guard")
	}

	// a second channel with outstanding sends makes the silence meaningful
	talking := stallTestChannel()
	talking.addSend(1440, udpTestPath(4))
	mc.clientUpdates[talking] = map[*multiClientChannelUpdate]bool{
		new(multiClientChannelUpdate): true,
	}
	if stale, _ := mc.uplinkGate(now); !stale {
		t.Error("the gate did not engage with two talking channels silent")
	}
}

// 0 disables the gate entirely, and a client that has never received anything
// has no baseline to have gone stale from -- a dead-on-arrival window is left
// to the ordinary verdicts rather than held for the cap first.
func TestBlackholeUplinkGateOffAndNoBaseline(t *testing.T) {
	mc := uplinkGateTestParent(2)
	mc.settings.UplinkStalenessGate = 0
	now := time.Now()
	mc.uplinkLastIngressNanos.Store(now.Add(-10 * time.Minute).UnixNano())

	stale, freshSince := mc.uplinkGate(now)
	if stale {
		t.Error("the gate engaged while disabled")
	}
	if !freshSince.IsZero() {
		t.Errorf("a disabled gate must rebase nothing: freshSince = %v", freshSince)
	}

	// no stamp ever
	fresh := uplinkGateTestParent(2)
	if stale, _ := fresh.uplinkGate(now); stale {
		t.Error("the gate engaged with no ingress baseline")
	}
}

// The stamp is on the download hot path, so it is coarsened: a fresh stamp is
// left alone and only a stale one is rewritten.
func TestBlackholeUplinkStampCoarsens(t *testing.T) {
	mc := &RemoteUserNatMultiClient{}

	mc.stampUplinkIngress()
	first := mc.uplinkLastIngressNanos.Load()
	if first == 0 {
		t.Fatal("the first stamp did not store")
	}

	// immediately again: within the coarseness window, skipped
	mc.stampUplinkIngress()
	if got := mc.uplinkLastIngressNanos.Load(); got != first {
		t.Errorf("a fresh stamp was rewritten: %d -> %d", first, got)
	}

	// an aged stamp is refreshed
	aged := time.Now().Add(-1 * time.Second).UnixNano()
	mc.uplinkLastIngressNanos.Store(aged)
	mc.stampUplinkIngress()
	if got := mc.uplinkLastIngressNanos.Load(); got <= aged {
		t.Errorf("a stale stamp was not refreshed: %d", got)
	}
}

// The default gate must exist and sit well under the receive bound it
// protects, and it must survive the round trip through the override type -- a
// missed field zeroes (gate off) on every settings write.
func TestBlackholeUplinkGateDefault(t *testing.T) {
	settings := DefaultMultiClientSettings()

	if settings.UplinkStalenessGate != 5*time.Second {
		t.Errorf("UplinkStalenessGate = %v, want 5s", settings.UplinkStalenessGate)
	}
	if settings.BlackholeReceiveTimeout <= settings.UplinkStalenessGate {
		t.Errorf(
			"gate %v must engage before the receive bound %v can mature on silent evidence",
			settings.UplinkStalenessGate, settings.BlackholeReceiveTimeout,
		)
	}
	if ReliabilitySettingsFrom(settings).UplinkStalenessGate != settings.UplinkStalenessGate {
		t.Error("UplinkStalenessGate is dropped by ReliabilitySettingsFrom, so every override write turns the gate off")
	}
}

// The stamp is only worth anything if the ingress sites actually call it --
// the correct-but-uncalled helper is the failure mode this codebase has
// shipped more than once. Both sites are pinned: the provider-originated
// receive path, and the intercepted dial failure (deliberately not a
// receive-ack, but proof the uplink delivers).
func TestBlackholeUplinkStampSites(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	for _, site := range []struct{ fn, desc string }{
		{"func (self *RemoteUserNatMultiClient) clientReceivePacket(", "provider-originated ingress"},
		{"func (self *RemoteUserNatMultiClient) clientDialFailure(", "intercepted dial failure"},
	} {
		body, ok := functionBody(source, site.fn)
		if !ok {
			t.Fatalf("could not find %s", site.fn)
		}
		if !strings.Contains(body, "stampUplinkIngress(") {
			t.Errorf("%s does not stamp the uplink: the gate would go stale under working ingress", site.desc)
		}
	}
}

// detectBlackhole must actually consult both gates and count what they hold.
// The decision function carrying gate parameters is worthless if the caller
// passes zero values forever.
func TestBlackholeDetectConsultsGates(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	body, ok := functionBody(source, "func (self *multiClientChannel) detectBlackhole()")
	if !ok {
		t.Fatal("could not find detectBlackhole")
	}

	if !strings.Contains(body, "self.uplinkGate(") {
		t.Error("detectBlackhole does not read the tunnel-wide uplink gate")
	}
	if !strings.Contains(body, "hasActiveTransport(") {
		t.Error("detectBlackhole does not cross-check the channel's transport liveness")
	}
	if !strings.Contains(body, "hasActiveUnreliableSendTransport(") {
		t.Error("detectBlackhole does not distinguish unreliable send-carrier silence")
	}
	if !strings.Contains(body, "blackholeGates{") {
		t.Error("detectBlackhole does not pass the gates into the decision")
	}
	if !strings.Contains(body, "verdictHeldUplinkStale(") || !strings.Contains(body, "verdictHeldTransportDown(") {
		t.Error("detectBlackhole does not count held verdicts, so the gates cannot be measured")
	}
}

// TestFormationPollTimeoutDefaults pins the fast-poll knob's contract: the
// default is 200ms, the runtime override plumbing carries it, and — unlike the
// other reliability knobs — zero means "fall back to SendRetryTimeout" (the
// pre-change pacing), not "off".
func TestFormationPollTimeoutDefaults(t *testing.T) {
	settings := DefaultMultiClientSettings()
	if settings.FormationPollTimeout != 200*time.Millisecond {
		t.Errorf("FormationPollTimeout default = %v, want 200ms", settings.FormationPollTimeout)
	}
	if got := ReliabilitySettingsFrom(settings).FormationPollTimeout; got != settings.FormationPollTimeout {
		t.Errorf("ReliabilitySettingsFrom dropped FormationPollTimeout: %v", got)
	}
	// the bare-fixture zero value must read as zero, which the send loop
	// treats as SendRetryTimeout pacing
	if got := ReliabilitySettingsFrom(nil).FormationPollTimeout; got != 0 {
		t.Errorf("nil settings FormationPollTimeout = %v, want 0", got)
	}
}

// TestFormationFastPollGuardsEmptyWindowOnly pins where the fast poll applies:
// sendParsedPacketGroup's retry loop consults FormationPollTimeout only on the
// zero-candidates branch (the window has no offer at all), never as a general
// retry accelerant — candidates that exist, including the benched fallback,
// keep the SendRetryTimeout pacing.
func TestFormationFastPollGuardsEmptyWindowOnly(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	body, ok := functionBody(source, "func (self *RemoteUserNatMultiClient) sendParsedPacketGroup(")
	if !ok {
		t.Fatal("could not find sendParsedPacketGroup")
	}

	emptyBranch := "if len(orderedClients) == 0 {"
	branchStart := strings.Index(body, emptyBranch)
	if branchStart < 0 {
		t.Fatal("sendParsedPacketGroup's retry loop has no empty-candidates branch")
	}
	if !strings.Contains(body, "FormationPollTimeout") {
		t.Fatal("sendParsedPacketGroup does not consult FormationPollTimeout")
	}
	// the consult must live inside the empty branch: after its start, before
	// the race call that only runs with candidates
	raceAfterBranch := strings.Index(body[branchStart:], "raceClients(orderedClients")
	pollInBranch := strings.Index(body[branchStart:], "FormationPollTimeout")
	if pollInBranch < 0 || (0 <= raceAfterBranch && raceAfterBranch < pollInBranch) {
		t.Error("FormationPollTimeout is not confined to the empty-candidates branch")
	}
	// zero falls back to the pre-change pacing: the consult must be gated on
	// a positive value
	if !strings.Contains(body, "0 < formationPoll") {
		t.Error("the empty-candidates branch does not gate on a positive FormationPollTimeout (zero must keep SendRetryTimeout pacing)")
	}
}

// --- comparative connect blackhole ---
//
// Concept ported from upstream main e05ecee's
// BlackholeConnectComparativeTimeout: the no-receive-syn branch's patience is
// only warranted while the evidence is ambiguous, and two OTHER exits carrying
// return traffic right now resolve the ambiguity.

// comparativeSynStats is a window that has sent syns and received nothing --
// the exact shape the no-receive-syn branch judges -- with the first syn synAge
// old.
func comparativeSynStats(synAge time.Duration) *clientWindowStats {
	return &clientWindowStats{
		log:              DefaultLogger(),
		firstSendSynTime: time.Now().Add(-synAge),
		sendSynCount:     18,
	}
}

// The decision table for which bar the syn branch matures at, and -- just as
// important -- when the sibling sweep is allowed to run at all. The count
// reaches the parent stateLock, so a version of this that swept on every
// verdict pass would put a channel walk on the 1.25s cadence for nothing.
func TestComparativeConnectTimeoutDecision(t *testing.T) {
	now := time.Now()
	full := 30 * time.Second
	comparative := 10 * time.Second

	sweeps := 0
	receiving := func(count int) func() int {
		return func() int {
			sweeps += 1
			return count
		}
	}

	// off: the zero value is the single-bar behavior
	AssertEqual(t, comparativeConnectTimeout(now, comparativeSynStats(15*time.Second), full, 0, time.Time{}, receiving(4),
		nil,
	), full)
	// configured at or above the bar it exists to shorten: a no-op
	AssertEqual(t, comparativeConnectTimeout(now, comparativeSynStats(15*time.Second), full, full, time.Time{}, receiving(4),
		nil,
	), full)

	sweeps = 0
	// nothing has been sent: nothing to judge
	AssertEqual(t, comparativeConnectTimeout(now, &clientWindowStats{log: DefaultLogger()}, full, comparative, time.Time{}, receiving(4),
		nil,
	), full)
	AssertEqual(t, comparativeConnectTimeout(now, nil, full, comparative, time.Time{}, receiving(4),
		nil,
	), full)

	// below the short bar there is nothing to cut short yet...
	AssertEqual(t, comparativeConnectTimeout(now, comparativeSynStats(5*time.Second), full, comparative, time.Time{}, receiving(4),
		nil,
	), full)
	// ...and past the full bar the ordinary verdict is already firing
	AssertEqual(t, comparativeConnectTimeout(now, comparativeSynStats(35*time.Second), full, comparative, time.Time{}, receiving(4),
		nil,
	), full)
	if sweeps != 0 {
		t.Errorf("the sibling sweep ran %d times outside the interval where it can change the outcome", sweeps)
	}

	// inside the interval, but the pool is not demonstrably fine: one receiving
	// sibling is one data point, not a statement about the pool
	sweeps = 0
	AssertEqual(t, comparativeConnectTimeout(now, comparativeSynStats(15*time.Second), full, comparative, time.Time{}, receiving(1),
		nil,
	), full)
	AssertEqual(t, sweeps, 1)

	// two receiving siblings: the uplink delivers, the pool works, and this
	// exit alone has established nothing
	AssertEqual(t, comparativeConnectTimeout(now, comparativeSynStats(15*time.Second), full, comparative, time.Time{}, receiving(2),
		nil,
	), comparative)

	// an exit with any receive progress is not the case the cut exists for
	withSyn := comparativeSynStats(15 * time.Second)
	withSyn.receiveSynCount = 1
	AssertEqual(t, comparativeConnectTimeout(now, withSyn, full, comparative, time.Time{}, receiving(4),
		nil,
	), full)
	withAck := comparativeSynStats(15 * time.Second)
	withAck.receiveAckCount = 1
	AssertEqual(t, comparativeConnectTimeout(now, withAck, full, comparative, time.Time{}, receiving(4),
		nil,
	), full)

	// a bare channel (nil count func) keeps the patient bar: absence of the
	// machinery must never shorten a removal
	AssertEqual(t, comparativeConnectTimeout(now, comparativeSynStats(15*time.Second), full, comparative, time.Time{}, nil,
		nil,
	), full)

	// the clock rebase applies here too: a syn whose age predates the end of an
	// inadmissible-evidence epoch counts from the epoch's end, so a held
	// verdict does not mature the instant the hold lifts
	AssertEqual(t, comparativeConnectTimeout(
		now,
		comparativeSynStats(15*time.Second),
		full,
		comparative,
		now.Add(-2*time.Second),
		receiving(4),
		nil,
	), full)
}

// End to end through the decision the channel actually ships: with two
// receiving siblings the syn branch fires at 10s; without them the same window
// is innocent until 30s.
func TestComparativeConnectFiresAtTheShortBarWithSiblings(t *testing.T) {
	now := time.Now()
	full := 30 * time.Second
	comparative := 10 * time.Second

	fire := func(synAge time.Duration, siblings int) blackholeReason {
		stats := comparativeSynStats(synAge)
		connectTimeout := comparativeConnectTimeout(
			now, stats, full, comparative, time.Time{},
			func() int { return siblings },
			nil,
		)
		reason, _ := blackholeReasonFromStats(now, stats, 5*time.Second, 0, connectTimeout, blackholeGates{})
		return reason
	}

	// the pool is fine and this exit has established nothing: cut at 10s
	AssertEqual(t, fire(11*time.Second, 2), blackholeNoReceiveSyn)
	// still inside the short bar
	AssertEqual(t, fire(9*time.Second, 2), blackholeNone)

	// nothing else is receiving -- the silence could be the whole tunnel, so
	// the patient bar stands
	AssertEqual(t, fire(11*time.Second, 0), blackholeNone)
	AssertEqual(t, fire(29*time.Second, 0), blackholeNone)
	AssertEqual(t, fire(31*time.Second, 0), blackholeNoReceiveSyn)
}

// Every existing gate still applies: the cut moves WHEN the branch matures,
// never whether the evidence is admissible.
func TestComparativeConnectStillPassesTheGates(t *testing.T) {
	now := time.Now()
	stats := comparativeSynStats(11 * time.Second)
	connectTimeout := comparativeConnectTimeout(
		now, stats, 30*time.Second, 10*time.Second, time.Time{},
		func() int { return 4 },
		nil,
	)
	AssertEqual(t, connectTimeout, 10*time.Second)

	// the transport gate holds everything
	reason, held := blackholeReasonFromStats(now, stats, 5*time.Second, 0, connectTimeout, blackholeGates{transportDown: true})
	AssertEqual(t, reason, blackholeNone)
	AssertEqual(t, held, blackholeNoReceiveSyn)

	// the uplink gate holds the receive branch, which this is
	reason, held = blackholeReasonFromStats(now, stats, 5*time.Second, 0, connectTimeout, blackholeGates{uplinkStale: true})
	AssertEqual(t, reason, blackholeNone)
	AssertEqual(t, held, blackholeNoReceiveSyn)
}

// The receive-side sibling count: the mirror of sendingChannelCount, excluding
// the channel being judged, windowed on the uplink gate's own freshness bar.
func TestComparativeReceivingChannelCount(t *testing.T) {
	mc := &RemoteUserNatMultiClient{
		settings:      DefaultMultiClientSettings(),
		clientUpdates: map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
	}
	add := func() *multiClientChannel {
		client := stallTestChannel()
		mc.clientUpdates[client] = map[*multiClientChannelUpdate]bool{
			new(multiClientChannelUpdate): true,
		}
		return client
	}

	subject := add()
	silent := add()
	receiving1 := add()
	receiving2 := add()

	// nothing has received: no comparative evidence at all
	AssertEqual(t, mc.receivingChannelCount(subject), 0)

	receiving1.addReceiveAck(1440)
	AssertEqual(t, mc.receivingChannelCount(subject), 1)

	receiving2.addReceiveAck(1440)
	AssertEqual(t, mc.receivingChannelCount(subject), 2)

	// the subject's own receive progress is never its own alibi
	subject.addReceiveAck(1440)
	AssertEqual(t, mc.receivingChannelCount(subject), 2)

	// stale receive progress does not count: the window is the uplink gate's
	silent.stateLock.Lock()
	silent.lastReceiveAckTime = time.Now().Add(-time.Minute)
	silent.stateLock.Unlock()
	AssertEqual(t, mc.receivingChannelCount(subject), 2)

	// and a bare parent does not panic
	bare := &RemoteUserNatMultiClient{}
	AssertEqual(t, bare.receivingChannelCount(nil), 0)
}

// detectBlackhole must actually consult the cut, and must read the comparative
// bound from the runtime override so the developer control has an effect.
func TestComparativeConnectDetectSourceAnchors(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *multiClientChannel) detectBlackhole()")
	if !ok {
		t.Fatal("could not find detectBlackhole")
	}
	if !strings.Contains(body, "comparativeConnectTimeout(") {
		t.Error("detectBlackhole does not consult the comparative cut, so the setting is inert")
	}
	if !strings.Contains(body, "self.reliabilitySettings().BlackholeConnectComparativeTimeout") {
		t.Error("detectBlackhole does not read the comparative bound from the runtime override")
	}
	if !strings.Contains(body, "self.receivingSiblings") {
		t.Error("detectBlackhole does not pass the receive-side sibling count into the cut")
	}

	// the sweep must follow sendingChannelCount's discipline: snapshot under
	// the parent lock, then take each channel's own lock alone
	body, ok = functionBody(source, "func (self *RemoteUserNatMultiClient) receivingChannelCount(")
	if !ok {
		t.Fatal("could not find receivingChannelCount")
	}
	if !strings.Contains(body, "self.stateLock.Lock()") || !strings.Contains(body, "client.hasRecentReceive(") {
		t.Error("receivingChannelCount does not snapshot-then-read-individually")
	}
	if strings.Count(body, "self.stateLock.Lock()") != 1 {
		t.Error("receivingChannelCount takes the parent lock more than once per sweep")
	}
	// the per-channel read must be OUTSIDE the parent's locked section
	snapshotEnd := strings.Index(body, "}()")
	perChannel := strings.Index(body, "client.hasRecentReceive(")
	if snapshotEnd < 0 || perChannel < snapshotEnd {
		t.Error("the per-channel lock is taken under the parent stateLock: the channel lock must never nest under it")
	}
}

// The shipped default and the override round trip.
func TestComparativeConnectDefaults(t *testing.T) {
	settings := DefaultMultiClientSettings()

	if settings.BlackholeConnectComparativeTimeout != 10*time.Second {
		t.Errorf("BlackholeConnectComparativeTimeout = %v, want 10s", settings.BlackholeConnectComparativeTimeout)
	}
	// above the full bar it would be silently inert
	if settings.BlackholeConnectTimeout <= settings.BlackholeConnectComparativeTimeout {
		t.Errorf(
			"the comparative bar (%v) is not shorter than the full bar (%v), so the cut can never fire",
			settings.BlackholeConnectComparativeTimeout, settings.BlackholeConnectTimeout,
		)
	}
	if got := ReliabilitySettingsFrom(settings).BlackholeConnectComparativeTimeout; got != settings.BlackholeConnectComparativeTimeout {
		t.Errorf("ReliabilitySettingsFrom dropped BlackholeConnectComparativeTimeout: %v", got)
	}
	// the zero value is the single-bar pre-change behavior
	if got := ReliabilitySettingsFrom(nil).BlackholeConnectComparativeTimeout; got != 0 {
		t.Errorf("nil settings BlackholeConnectComparativeTimeout = %v, want 0", got)
	}
}

// The G-6 corroboration table: the soft no-receive-ack verdict needs more
// distinct silent destinations the busier the exit is, and the scaling can
// only ever raise the bar, never lower it below the configured minimum.
func TestLoadScaledMinDestinations(t *testing.T) {
	for _, row := range []struct {
		configured, flows, perFlows, want int
	}{
		// flat behavior: scaling off
		{2, 24, 0, 2},
		{2, 24, -1, 2},
		// under the threshold the configured minimum holds
		{2, 0, 8, 2},
		{2, 15, 8, 2},
		// the 2026-08-03 shape: a 24-flow exit needs 3, not 2
		{2, 24, 8, 3},
		{2, 80, 8, 10},
		// the scaling never lowers the configured bar
		{4, 8, 8, 4},
		// a zero configured minimum still scales up under load
		{0, 24, 8, 3},
	} {
		got := loadScaledMinDestinations(row.configured, row.flows, row.perFlows)
		if got != row.want {
			t.Errorf("loadScaledMinDestinations(%d, %d, %d) = %d, want %d",
				row.configured, row.flows, row.perFlows, got, row.want)
		}
	}
}

// And the gate must actually be fed the scaled value: the detectBlackhole
// call site has to route MinBlackholeDestinations through the scaler, or the
// table above tests a function nothing reaches.
func TestBlackholeGateUsesLoadScaledDestinations(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *multiClientChannel) detectBlackhole()")
	if !ok {
		t.Fatal("could not find detectBlackhole")
	}
	if !strings.Contains(body, "loadScaledMinDestinations(") {
		t.Error("detectBlackhole does not call loadScaledMinDestinations: the corroboration gate ignores load")
	}
}

// readSource normalizes line endings, because functionBody delimits a body with
// a bare "\n}\n" and git checks these files out with CRLF wherever core.autocrlf
// is on, which is the default on Windows. Without this the delimiter matches
// nothing, functionBody hands back the whole rest of the file, and every anchor
// built on strings.Contains passes against text from somewhere else entirely.
// The six anchors written as strings.Count fail loudly in that state; the
// eighty-odd written as strings.Contains do not, so the failure is silent
// exactly where the protection matters most.
func readSource(name string) (string, error) {
	b, err := os.ReadFile(name)
	if err != nil {
		return "", err
	}
	return strings.ReplaceAll(string(b), "\r\n", "\n"), nil
}

// functionBody returns the source between a function's signature and the
// closing brace at column 0. Good enough for asserting a call site exists,
// which is all it is used for. A missing terminator reports false rather than
// returning the rest of the file: an anchor that silently widens to the whole
// file still passes, and a passing anchor that proves nothing is worse than no
// anchor, because it is indistinguishable from a real one.
func functionBody(source string, signature string) (string, bool) {
	start := strings.Index(source, signature)
	if start < 0 {
		return "", false
	}
	rest := source[start:]
	if end := strings.Index(rest, "\n}\n"); 0 <= end {
		return rest[:end], true
	}
	return "", false
}
