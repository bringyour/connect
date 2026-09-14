package connect

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// The quarantine state machine on a bare channel: set marks the channel, the
// episode start is stable while the same reason re-asserts, a reason change
// restarts it, and clear removes everything. These transitions are what the
// verdict pass's expiry math stands on.
func TestQuarantineSetClearAndState(t *testing.T) {
	client := stallTestChannel()

	AssertEqual(t, client.isQuarantined(), false)
	reason, since := client.quarantineState()
	AssertEqual(t, reason, blackholeNone)
	AssertEqual(t, since.IsZero(), true)

	AssertEqual(t, client.setQuarantined(blackholeNoReceiveAck), true)
	AssertEqual(t, client.isQuarantined(), true)
	reason, since = client.quarantineState()
	AssertEqual(t, reason, blackholeNoReceiveAck)
	AssertEqual(t, since.IsZero(), false)

	// re-asserting the same reason must keep the original start: the expiry
	// bound measures one continuous run of the same evidence, and a start
	// that slid forward on every verdict pass could never expire
	AssertEqual(t, client.setQuarantined(blackholeNoReceiveAck), false)
	_, sinceAgain := client.quarantineState()
	AssertEqual(t, sinceAgain.Equal(since), true)

	// a different reason is a different suspicion and restarts the episode
	AssertEqual(t, client.setQuarantined(blackholeNoReceiveSyn), true)
	reason, sinceRestarted := client.quarantineState()
	AssertEqual(t, reason, blackholeNoReceiveSyn)
	AssertEqual(t, sinceRestarted.Before(since), false)

	client.clearQuarantine()
	AssertEqual(t, client.isQuarantined(), false)
	reason, since = client.quarantineState()
	AssertEqual(t, reason, blackholeNone)
	AssertEqual(t, since.IsZero(), true)
}

// isWarning is warning OR quarantined, which is what makes quarantine
// exclusion automatic for both existing consumers -- the parent's
// affinity/selection reads and the window's ordered-clients offer -- without
// either knowing quarantine exists.
func TestQuarantineIsWarningOr(t *testing.T) {
	client := stallTestChannel()
	AssertEqual(t, client.isWarning(), false)

	client.setQuarantined(blackholeNoReceiveAck)
	AssertEqual(t, client.isWarning(), true)

	client.clearQuarantine()
	AssertEqual(t, client.isWarning(), false)

	client.setWarning(true, warnUnhealthy)
	AssertEqual(t, client.isWarning(), true)
}

// The clobber test, and the reason quarantine is its own flag: the resize
// pass recomputes the shared warning bool every pass and its healthy path
// writes setWarning(false) for a client it keeps. If quarantine lived in that
// bool, every healthy-looking pass would silently release a demoted exit back
// into selection.
func TestQuarantineSurvivesResizeWarningClear(t *testing.T) {
	client := stallTestChannel()
	client.setQuarantined(blackholeNoReceiveAck)

	// the resize healthy path's write
	client.setWarning(false, warnNone)

	AssertEqual(t, client.isQuarantined(), true)
	AssertEqual(t, client.isWarning(), true)

	// and the reverse: lifting a quarantine must not erase an independent
	// resize warning
	client.setWarning(true, warnUnhealthy)
	client.clearQuarantine()
	AssertEqual(t, client.isQuarantined(), false)
	AssertEqual(t, client.isWarning(), true)
}

// The cause names WHY a channel is warned, and clearing the warning clears
// it: a stale cause must never describe a healthy channel, and a quarantine
// alone (no resize warning) reports no cause -- Quarantined is its name.
func TestWarningCauseClearsWithWarning(t *testing.T) {
	client := stallTestChannel()
	AssertEqual(t, client.warningCause(), warnNone)

	client.setWarning(true, warnDraining)
	AssertEqual(t, client.warningCause(), warnDraining)

	// the cause argument of a clear is ignored, and the stored cause resets
	client.setWarning(false, warnUnhealthy)
	AssertEqual(t, client.warningCause(), warnNone)

	// quarantine does not invent a warning cause
	client.setQuarantined(blackholeNoReceiveAck)
	AssertEqual(t, client.warningCause(), warnNone)
	client.clearQuarantine()
}

// Receive progress is exactly the evidence the quarantining verdicts said was
// missing, so it acquits -- and only receive progress does. Send acks prove
// the provider is alive, which was never in question for the receive-branch
// verdicts.
func TestQuarantineClearsOnReceiveProgress(t *testing.T) {
	client := stallTestChannel()

	client.setQuarantined(blackholeNoReceiveAck)
	client.addReceiveAck(1440)
	AssertEqual(t, client.isQuarantined(), false)

	client.setQuarantined(blackholeNoReceiveSyn)
	client.addSend(1440, udpTestPath(4))
	client.addSendAck(1440)
	client.addReceiveSyn(1)
	AssertEqual(t, client.isQuarantined(), true)
}

// The clear must live where the receive-ack count advances, or a quarantined
// exit whose destination finally answered would stay demoted forever.
func TestQuarantineClearSiteAnchor(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	body, ok := functionBody(source, "func (self *multiClientChannel) addReceiveAck(")
	if !ok {
		t.Fatal("could not find addReceiveAck")
	}
	if !strings.Contains(body, "quarantined") {
		t.Error("addReceiveAck does not clear the quarantine, so receive progress cannot acquit a demoted exit")
	}
}

// Both selection consumers must go through isWarning, or the quarantine OR
// would exclude an exit from one path and not the other.
func TestQuarantineExclusionConsumersUseIsWarning(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	// the window's offer to the race
	body, ok := functionBody(source, "func (self *multiClientWindow) orderedClients(")
	if !ok {
		t.Fatal("could not find orderedClients")
	}
	if !strings.Contains(body, "isWarning()") {
		t.Error("orderedClients does not consult isWarning, so a quarantined exit stays raceable")
	}

	// the parent's affinity inheritance judges donors through the G-1 verdict
	// (which refuses every resize warning and admits a quarantined donor only
	// under group-follow with fresh receive evidence -- see
	// affinityDonorEligible and its verdict-table test), for both ip versions
	if strings.Count(source, "c.affinityDonorEligible(") < 2 {
		t.Error("the affinity selection no longer judges donors through affinityDonorEligible for both ip versions, so either a warned exit can be inherited or a benched site is scattered unconditionally")
	}
}

// The channel-side flow count is injected parent state: nil (a bare fixture)
// reads as 0, and a 0-flow verdict executes -- so channels built without the
// parent keep the pre-change execute-immediately behavior every older test
// asserts.
func TestQuarantineChannelFlowCountInjection(t *testing.T) {
	client := stallTestChannel()
	AssertEqual(t, client.flowCount(), 0)

	client.flowCountFunc = func(c *multiClientChannel) int {
		AssertEqual(t, c == client, true)
		return 7
	}
	AssertEqual(t, client.flowCount(), 7)
}

// The window-side count follows the same convention, and nil means every
// client reads as flowless -- the capacity gates then behave exactly as they
// did before the count existed, which is what the bare window fixtures assert.
func TestQuarantineWindowFlowCountNilIsZero(t *testing.T) {
	window := &multiClientWindow{settings: DefaultMultiClientSettings()}
	AssertEqual(t, window.flowCount(stallTestChannel()), 0)

	window.flowCountFunc = func(c *multiClientChannel) int { return 3 }
	AssertEqual(t, window.flowCount(stallTestChannel()), 3)
}

// The parent's count reads the clientUpdates bookkeeping that bindClientFlow
// single-sources -- the same map the flow cap and the teardown read, so the
// demote decision and the blast radius agree about what an exit carries.
func TestQuarantineParentFlowCountReadsBindings(t *testing.T) {
	parent := bindFlowTestParent()
	client := bindFlowTestChannel(parent)

	AssertEqual(t, parent.clientFlowCount(client), 0)

	update := &multiClientChannelUpdate{}
	update.client.Store(client)
	parent.bindClientFlow(update, client)

	AssertEqual(t, parent.clientFlowCount(client), 1)
}

// detectBlackhole must route its conviction through the action decision, read
// the live flow count, and be able to quarantine -- a decision helper that is
// correct but unconsulted is the failure shape this codebase has shipped more
// than once.
func TestQuarantineDetectBlackholeConsultsVerdictAction(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	body, ok := functionBody(source, "func (self *multiClientChannel) detectBlackhole()")
	if !ok {
		t.Fatal("could not find detectBlackhole")
	}
	if !strings.Contains(body, "verdictAction(") {
		t.Error("detectBlackhole does not consult verdictAction: soft verdicts still execute unconditionally")
	}
	if !strings.Contains(body, "self.flowCount()") {
		t.Error("detectBlackhole does not read the live flow count, so the demote cannot see blast radius")
	}
	if !strings.Contains(body, "setQuarantined(") {
		t.Error("detectBlackhole cannot quarantine, so a demote decision has no effect")
	}
	if !strings.Contains(body, "quarantine expired") {
		t.Error("the expired execution is not tagged, so field logs cannot tell it from an immediate one")
	}
}

// The resize's unhealthy branch must consult the flow count before removing,
// and the window accessor must actually read the injected func.
func TestQuarantineResizeConsultsFlowCount(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	body, ok := functionBody(source, "func (self *multiClientWindow) resize()")
	if !ok {
		t.Fatal("could not find resize")
	}
	if !strings.Contains(body, "self.flowCount(client)") {
		t.Error("resize's unhealthy branch does not consult the flow count, so stats-unhealthy still removes loaded exits")
	}
	if !strings.Contains(body, "SoftVerdictDemote") {
		t.Error("resize's demote is not gated on SoftVerdictDemote, so the pre-change behavior is not reachable for A/B")
	}

	accessor, ok := functionBody(source, "func (self *multiClientWindow) flowCount(")
	if !ok {
		t.Fatal("could not find the window flowCount accessor")
	}
	if !strings.Contains(accessor, "flowCountFunc") {
		t.Error("the window flowCount accessor does not read the injected func")
	}
}

// The action decision itself, driven directly. This is the demote/execute
// table the invariant promises: hard evidence executes, soft evidence against
// flows demotes, sustained soft evidence executes after all.
func TestVerdictActionTable(t *testing.T) {
	now := time.Now()
	expiry := 60 * time.Second

	cases := []struct {
		name             string
		reason           blackholeReason
		softDemote       bool
		flowCount        int
		quarantinedSince time.Time
		expiry           time.Duration
		want             verdictActionKind
	}{
		{"no verdict", blackholeNone, true, 5, time.Time{}, expiry, verdictActionNone},
		// no-send-ack is hard evidence and is untouched by the demote,
		// whatever the flow count or quarantine age
		{"no-send-ack loaded", blackholeNoSendAck, true, 5, time.Time{}, expiry, verdictActionExecute},
		{"no-send-ack long-quarantined", blackholeNoSendAck, true, 5, now.Add(-10 * time.Minute), expiry, verdictActionExecute},
		// a flowless exit has no blast radius: soft verdicts execute as before
		{"no-receive-ack flowless", blackholeNoReceiveAck, true, 0, time.Time{}, expiry, verdictActionExecute},
		{"no-receive-syn flowless", blackholeNoReceiveSyn, true, 0, time.Time{}, expiry, verdictActionExecute},
		// a loaded exit demotes
		{"no-receive-ack loaded", blackholeNoReceiveAck, true, 3, time.Time{}, expiry, verdictActionQuarantine},
		{"no-receive-syn loaded", blackholeNoReceiveSyn, true, 2, time.Time{}, expiry, verdictActionQuarantine},
		// still inside the sustained bound: stay quarantined
		{"no-receive-ack quarantined 10s", blackholeNoReceiveAck, true, 3, now.Add(-10 * time.Second), expiry, verdictActionQuarantine},
		// the same evidence held continuously past the bound executes, tagged
		{"no-receive-ack quarantine expired", blackholeNoReceiveAck, true, 3, now.Add(-61 * time.Second), expiry, verdictActionExecuteExpired},
		{"no-receive-syn quarantine expired", blackholeNoReceiveSyn, true, 2, now.Add(-61 * time.Second), expiry, verdictActionExecuteExpired},
		// expiry 0 disables the escape: removal waits for flowless
		{"expiry disabled", blackholeNoReceiveAck, true, 3, now.Add(-10 * time.Minute), 0, verdictActionQuarantine},
		// soft demote off is the pre-change behavior for A/B
		{"demote off", blackholeNoReceiveAck, false, 3, time.Time{}, expiry, verdictActionExecute},
		{"demote off syn", blackholeNoReceiveSyn, false, 2, time.Time{}, expiry, verdictActionExecute},
	}

	for _, c := range cases {
		got := verdictAction(c.reason, c.softDemote, c.flowCount, c.quarantinedSince, now, c.expiry, false)
		if got != c.want {
			t.Errorf("%s: verdictAction = %d, want %d", c.name, got, c.want)
		}
	}
}

// The default configuration ships the demote on with the shipped budget, and
// every new knob survives the round trip through the override type -- a
// missed field zeroes on every settings write, silently turning the behavior
// off.
func TestVerdictActionDefaultsAndOverrideRoundTrip(t *testing.T) {
	settings := DefaultMultiClientSettings()

	AssertEqual(t, settings.SoftVerdictDemote, true)
	AssertEqual(t, settings.RemovalBudgetCount, 2)
	AssertEqual(t, settings.RemovalBudgetWindow, 30*time.Second)

	reliabilitySettings := ReliabilitySettingsFrom(settings)
	AssertEqual(t, reliabilitySettings.SoftVerdictDemote, settings.SoftVerdictDemote)
	AssertEqual(t, reliabilitySettings.RemovalBudgetCount, settings.RemovalBudgetCount)
	AssertEqual(t, reliabilitySettings.RemovalBudgetWindow, settings.RemovalBudgetWindow)
}

// The breaker's budget on a bare window: the shipped budget admits two
// verdict removals per window, defers the third, and lets budget age back in.
func TestRemovalBudgetAdmitsThenDefers(t *testing.T) {
	window := &multiClientWindow{settings: DefaultMultiClientSettings()}
	now := time.Now()

	AssertEqual(t, window.verdictRemovalAllowed(now), true)
	AssertEqual(t, window.verdictRemovalAllowed(now), true)
	AssertEqual(t, window.verdictRemovalAllowed(now), false)
	// still spent inside the window
	AssertEqual(t, window.verdictRemovalAllowed(now.Add(10*time.Second)), false)

	// past the window the old removals age out and budget returns
	later := now.Add(window.settings.RemovalBudgetWindow + time.Second)
	AssertEqual(t, window.verdictRemovalAllowed(later), true)
}

// A deferral must not consume budget: only admitted removals are recorded, so
// a storm of denied attempts cannot lock the breaker shut forever.
func TestRemovalBudgetDeniedAttemptsDoNotAccumulate(t *testing.T) {
	window := &multiClientWindow{settings: DefaultMultiClientSettings()}
	now := time.Now()

	AssertEqual(t, window.verdictRemovalAllowed(now), true)
	AssertEqual(t, window.verdictRemovalAllowed(now), true)
	for i := 0; i < 5; i += 1 {
		AssertEqual(t, window.verdictRemovalAllowed(now.Add(time.Duration(i)*time.Second)), false)
	}

	// the two admitted removals age out on their own schedule, unaffected by
	// the denied attempts in between
	later := now.Add(window.settings.RemovalBudgetWindow + time.Second)
	AssertEqual(t, window.verdictRemovalAllowed(later), true)
}

// 0 count turns the breaker off, and a non-positive window -- which could
// never accumulate budget -- reads as off too rather than as always-deny.
func TestRemovalBudgetZeroDisables(t *testing.T) {
	countOff := DefaultMultiClientSettings()
	countOff.RemovalBudgetCount = 0
	window := &multiClientWindow{settings: countOff}
	for i := 0; i < 10; i += 1 {
		AssertEqual(t, window.verdictRemovalAllowed(time.Now()), true)
	}

	windowOff := DefaultMultiClientSettings()
	windowOff.RemovalBudgetWindow = 0
	window = &multiClientWindow{settings: windowOff}
	for i := 0; i < 10; i += 1 {
		AssertEqual(t, window.verdictRemovalAllowed(time.Now()), true)
	}
}

// The runtime override wins over the constructed settings, so the breaker can
// be retuned (or turned off) against a live connection like every other
// reliability knob.
func TestRemovalBudgetUsesTheOverride(t *testing.T) {
	window := &multiClientWindow{
		settings: DefaultMultiClientSettings(),
		reliabilitySettingsFunc: func() *ReliabilitySettings {
			return &ReliabilitySettings{
				RemovalBudgetCount:  1,
				RemovalBudgetWindow: 30 * time.Second,
			}
		},
	}
	now := time.Now()
	AssertEqual(t, window.verdictRemovalAllowed(now), true)
	AssertEqual(t, window.verdictRemovalAllowed(now), false)
}

// What the breaker meters and what it exempts, by the error classification it
// keys on. Verdict-driven removals carry detectBlackhole's "Blackhole "
// prefix -- including the quarantine-expired variant. User action and
// shutdown cleanup write "Done." (Cancel/Close), and a dead continuous ping
// surfaces its transport error verbatim; none of those may burn budget,
// because they are hard evidence or explicit intent.
func TestStormBreakerErrorClassification(t *testing.T) {
	AssertEqual(t, blackholeVerdictErr(nil), false)
	// what Cancel and Close write -- DropExit, shuffle, shutdown
	AssertEqual(t, blackholeVerdictErr(errors.New("Done.")), false)
	// a cping transport error, surfaced verbatim
	AssertEqual(t, blackholeVerdictErr(errors.New("write timeout")), false)
	// the immediate verdict form
	AssertEqual(t, blackholeVerdictErr(errors.New("Blackhole no-receive-ack (send 7/360B recv 0/0B syn 0/0 nackAge 21s synAge none)")), true)
	// the quarantine-expired form keeps the prefix on purpose
	AssertEqual(t, blackholeVerdictErr(errors.New("Blackhole no-receive-ack quarantine expired (send 7/360B recv 0/0B syn 0/0 nackAge 61s synAge none)")), true)
}

// The breaker must wrap both verdict-driven removal sites in resize -- the
// WindowStats-error removal and the unhealthy-stats removal -- and only
// those. The capacity collapse and the drain paths keep calling the plain
// removal.
func TestStormBreakerWrapsBothRemovalSites(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	body, ok := functionBody(source, "func (self *multiClientWindow) resize()")
	if !ok {
		t.Fatal("could not find resize")
	}

	if got := strings.Count(body, "self.verdictRemovalAllowed("); got != 2 {
		t.Errorf("resize consults the removal budget at %d sites, want exactly 2 (the WindowStats-err verdict removal and the unhealthy-stats removal)", got)
	}
	if !strings.Contains(body, "blackholeVerdictErr(") {
		t.Error("resize does not classify WindowStats errors, so cancellations would burn verdict budget")
	}
	if !strings.Contains(body, "removalDeferred()") {
		t.Error("resize does not count deferred removals, so the breaker cannot be measured")
	}
}

// User action must bypass the budget entirely: DropExit cancels the channel
// directly, and the cancellation reaches resize as a "Done." error, which the
// classification above exempts. Pin both halves.
func TestStormBreakerDropExitIsExempt(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	dropExit, ok := functionBody(source, "func (self *RemoteUserNatMultiClient) DropExit(")
	if !ok {
		t.Fatal("could not find DropExit")
	}
	if !strings.Contains(dropExit, "client.Cancel()") {
		t.Error("DropExit no longer cancels the channel directly")
	}
	if strings.Contains(dropExit, "verdictRemovalAllowed") {
		t.Error("DropExit consults the removal budget: user action must never be deferred")
	}

	cancel, ok := functionBody(source, "func (self *multiClientChannel) Cancel()")
	if !ok {
		t.Fatal("could not find Cancel")
	}
	if !strings.Contains(cancel, `"Done."`) {
		t.Error(`Cancel no longer writes the "Done." error the breaker's exemption keys on`)
	}
}

// A deferred removal increments the shared removalsDeferred counter through
// the window's injected metrics, so the field numbers show the breaker
// working.
func TestStormBreakerDeferralIsCounted(t *testing.T) {
	metrics := newReliabilityMetrics()
	window := &multiClientWindow{
		settings:               DefaultMultiClientSettings(),
		reliabilityMetricsFunc: func() *reliabilityMetrics { return metrics },
	}

	// the deferral path in resize is: budget denied -> metrics().removalDeferred()
	now := time.Now()
	window.verdictRemovalAllowed(now)
	window.verdictRemovalAllowed(now)
	if !window.verdictRemovalAllowed(now) {
		window.metrics().removalDeferred()
	}

	AssertEqual(t, metrics.snapshot().RemovalsDeferred, uint64(1))

	// and a bare window without metrics stays nil-safe
	bare := &multiClientWindow{settings: DefaultMultiClientSettings()}
	bare.metrics().removalDeferred()
}
