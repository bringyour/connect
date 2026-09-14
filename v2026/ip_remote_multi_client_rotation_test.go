package connect

import (
	"context"
	"strings"
	"testing"
	"time"
)

// The rotation/capacity gates share one fixture convention with the
// quarantine suite: a bare window with an INJECTED flowCountFunc. The nil
// default reads every client as flowless, which deliberately reproduces the
// pre-change behavior -- so a test that wants to see the new gates hold a
// flow-carrying client MUST inject a real count, or it is asserting nothing.
func rotationTestWindow(settings *MultiClientSettings, flowCount int) *multiClientWindow {
	return &multiClientWindow{
		settings: settings,
		flowCountFunc: func(c *multiClientChannel) int {
			return flowCount
		},
	}
}

// --- item 1: the capacity-collapse gate ---

// A drained (zero 30s weight, past grace) client that still carries flows
// must survive capacity collapse: zero effective weight cannot tell an
// idle-but-open session (an ssh window between keystrokes) from a dead
// client, and collapse used to execute on the weight alone.
func TestCollapseGateFlowCarryingClientSurvives(t *testing.T) {
	settings := DefaultMultiClientSettings() // MaxClientLifetime 60m
	window := rotationTestWindow(settings, 1)
	client := stallTestChannel()

	// well past the grace period, well inside the hard deadline
	AssertEqual(t, window.collapseRemovalAllowed(client, 40*time.Minute), false)
}

// Flowless is the normal removal path and behaves exactly as before the gate
// existed -- including on a bare window, where the nil flowCountFunc reads
// every client as flowless.
func TestCollapseGateFlowlessCollapses(t *testing.T) {
	settings := DefaultMultiClientSettings()
	window := rotationTestWindow(settings, 0)
	client := stallTestChannel()

	AssertEqual(t, window.collapseRemovalAllowed(client, 40*time.Minute), true)

	// the bare-window default: nil func = flowless = pre-change collapse
	bare := &multiClientWindow{settings: settings}
	AssertEqual(t, bare.collapseRemovalAllowed(client, 40*time.Minute), true)
}

// The hard deadline is the escape hatch: past collapseDeadlineLifetimes x
// MaxClientLifetime from the client's first event, even a flow-carrying
// client collapses, so one immortal flow cannot pin an exit forever.
func TestCollapseGatePastDeadlineCollapsesDespiteFlows(t *testing.T) {
	settings := DefaultMultiClientSettings()
	window := rotationTestWindow(settings, 5)

	deadline := time.Duration(collapseDeadlineLifetimes) * settings.MaxClientLifetime

	// just inside the deadline: held
	AssertEqual(t, window.collapseRemovalAllowed(stallTestChannel(), deadline-time.Minute), false)
	// at and past the deadline: collapsible despite the flows
	AssertEqual(t, window.collapseRemovalAllowed(stallTestChannel(), deadline), true)
	AssertEqual(t, window.collapseRemovalAllowed(stallTestChannel(), deadline+time.Hour), true)
}

// MaxClientLifetime 0 means rotation is disabled, and with it the deadline:
// the operator opted out of forced rotation, so a flow-carrying client is
// simply never collapsible, however old.
func TestCollapseGateDisabledLifetimeHasNoDeadline(t *testing.T) {
	settings := DefaultMultiClientSettings()
	settings.MaxClientLifetime = 0
	window := rotationTestWindow(settings, 1)

	AssertEqual(t, window.collapseRemovalAllowed(stallTestChannel(), 1000*time.Hour), false)
}

// The gate is only worth anything if the collapse actually consults it -- the
// correct-but-uncalled helper is the failure shape this codebase has shipped
// more than once. Pin the call site inside resize's collapse closure, and the
// deferral log that makes a held collapse observable in the field.
func TestCollapseGateSourceAnchor(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	body, ok := functionBody(source, "func (self *multiClientWindow) resize()")
	if !ok {
		t.Fatal("could not find resize")
	}
	if !strings.Contains(body, "self.collapseRemovalAllowed(") {
		t.Error("resize's collapse does not consult collapseRemovalAllowed, so capacity collapse still destroys live flows")
	}
	// the deferral log that makes a held collapse observable in the field. It
	// moved to the [rel] grammar in P3 (event=collapse_defer); the property
	// being anchored is unchanged -- a held collapse must produce a line.
	if !strings.Contains(body, `"collapse_defer"`) {
		t.Error("a deferred collapse is not logged, so a held collapse is invisible in field logs")
	}

	gate, ok := functionBody(source, "func (self *multiClientWindow) collapseRemovalAllowed(")
	if !ok {
		t.Fatal("could not find collapseRemovalAllowed")
	}
	if !strings.Contains(gate, "self.flowCount(") {
		t.Error("collapseRemovalAllowed does not read the live flow count")
	}
}

// --- item 2: the same-clientId replacement decline ---

// expand may only cancel an in-window channel for a re-handed identity when
// that channel is done or flowless. A live, flow-carrying channel keeps its
// flows: the new channel is declined instead.
func TestExpandReplacementAllowedTable(t *testing.T) {
	settings := DefaultMultiClientSettings()

	liveChannel := func() (*multiClientChannel, context.CancelFunc) {
		ctx, cancel := context.WithCancel(context.Background())
		client := stallTestChannel()
		client.ctx = ctx
		return client, cancel
	}

	// an empty slot always installs
	window := rotationTestWindow(settings, 3)
	AssertEqual(t, window.replacementAllowed(nil), true)

	// live and carrying flows: declined
	client, cancel := liveChannel()
	AssertEqual(t, window.replacementAllowed(client), false)

	// the same channel done: replaceable, whatever its stale flow count reads
	cancel()
	AssertEqual(t, window.replacementAllowed(client), true)

	// live but flowless: replaceable, nothing is destroyed
	window = rotationTestWindow(settings, 0)
	client, cancel = liveChannel()
	defer cancel()
	AssertEqual(t, window.replacementAllowed(client), true)

	// and the bare-window default (nil flowCountFunc) reads flowless, which
	// is the pre-change always-replace behavior the older fixtures assert
	bare := &multiClientWindow{settings: settings}
	AssertEqual(t, bare.replacementAllowed(client), true)
}

// expand's body must consult the gate before installing over an existing
// channel, and the decline must be logged. Driving the real expand needs a
// generator plus a completed evaluation ping, so the call sites are pinned at
// the source level, the same convention as the quarantine and storm-breaker
// anchors.
func TestExpandReplacementDeclineSourceAnchor(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	body, ok := functionBody(source, "func (self *multiClientWindow) expand(")
	if !ok {
		t.Fatal("could not find expand")
	}
	if !strings.Contains(body, "self.replacementAllowed(") {
		t.Error("expand does not consult replacementAllowed: a re-handed identity still cancels a live flow-carrying channel")
	}
	// the decline log moved to the [rel] grammar in P3 (event=expand_decline);
	// the property being anchored is unchanged -- a decline must produce a line
	if !strings.Contains(body, `"expand_decline"`) {
		t.Error("a declined replacement is not logged, so the decline is invisible in field logs")
	}
	// Once newMultiClientChannel succeeds, its cancellation goroutine owns both
	// the Client and its generator args and retires them through
	// RemoveClientWithArgs. A second direct RemoveClientArgs call revokes the
	// derived JWT before final contract-close controls finish. The two remaining
	// calls are both pre-ownership: an args delivery observed after acquisition
	// closed, and the construction-error path.
	if count := strings.Count(body, "RemoveClientArgs("); count != 2 {
		t.Errorf("expand has %d direct RemoveClientArgs calls, want only the late-args and construction-error cleanups", count)
	}
	deadlineCheck := strings.Index(body, "expandCandidateWithinAcquisitionDeadline(")
	channelConstruction := strings.Index(body, "newMultiClientChannel(")
	if deadlineCheck < 0 || channelConstruction < 0 || channelConstruction < deadlineCheck {
		t.Error("expand checks the acquisition deadline after channel construction, so direct args cleanup could revoke an owned channel")
	}

	gate, ok := functionBody(source, "func (self *multiClientWindow) replacementAllowed(")
	if !ok {
		t.Fatal("could not find replacementAllowed")
	}
	if !strings.Contains(gate, "self.flowCount(") {
		t.Error("replacementAllowed does not read the live flow count")
	}
	if !strings.Contains(gate, "IsDone()") {
		t.Error("replacementAllowed does not check for a done channel, so dead channels could be kept over fresh ones")
	}
}

func TestStrictWindowAdmissionHardMax(t *testing.T) {
	firstId := NewId()
	secondId := NewId()
	newId := NewId()
	windowSize := WindowSizeSettings{WindowSizeHardMax: 2}
	window := &multiClientWindow{
		settings: &MultiClientSettings{StrictWindowSizeHardMax: true},
		clients: map[Id]*multiClientChannel{
			firstId:  nil,
			secondId: nil,
		},
	}

	if window.strictWindowAdmissionAllowed(newId, windowSize) {
		t.Fatal("strict hard max admitted a new identity at the ownership ceiling")
	}
	if !window.strictWindowAdmissionAllowed(firstId, windowSize) {
		t.Fatal("strict hard max rejected a same-identity replacement")
	}

	delete(window.clients, secondId)
	if !window.strictWindowAdmissionAllowed(newId, windowSize) {
		t.Fatal("strict hard max rejected a new identity below the ceiling")
	}
}

func TestStrictWindowAdmissionIsOptInAndZeroMaxIsUnbounded(t *testing.T) {
	newId := NewId()
	var nilWindow *multiClientWindow
	if !nilWindow.strictWindowAdmissionAllowed(newId, WindowSizeSettings{WindowSizeHardMax: 1}) {
		t.Fatal("nil window unexpectedly enabled strict admission")
	}
	bareWindow := &multiClientWindow{}
	if !bareWindow.strictWindowAdmissionAllowed(newId, WindowSizeSettings{WindowSizeHardMax: 1}) {
		t.Fatal("partial settings unexpectedly enabled strict admission")
	}
	window := &multiClientWindow{
		settings: DefaultMultiClientSettings(),
		clients: map[Id]*multiClientChannel{
			NewId(): nil,
		},
	}
	if !window.strictWindowAdmissionAllowed(newId, WindowSizeSettings{WindowSizeHardMax: 1}) {
		t.Fatal("default settings unexpectedly enabled strict admission")
	}

	window.settings.StrictWindowSizeHardMax = true
	if !window.strictWindowAdmissionAllowed(newId, WindowSizeSettings{}) {
		t.Fatal("zero hard max did not retain its unbounded meaning")
	}
}

func TestExpandConsultsStrictWindowAdmissionGate(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *multiClientWindow) expand(")
	if !ok {
		t.Fatal("could not find expand")
	}
	if !strings.Contains(body, "self.strictWindowAdmissionAllowed(") {
		t.Fatal("expand bypasses the strict hard-max admission gate")
	}
	if !strings.Contains(body, `"strict_hard_max"`) {
		t.Fatal("strict hard-max declines are not observable in field logs")
	}
}

// --- item 3: the per-channel lifetime jitter ---

// The effective lifetime must stay within [0.75, 1.0) x the configured
// lifetime across many draws, and must actually vary -- a constant fraction
// would keep rotation synchronized, which is the whole failure being fixed.
func TestLifetimeJitterBounds(t *testing.T) {
	maxClientLifetime := 60 * time.Minute
	low := time.Duration(float64(maxClientLifetime) * clientLifetimeJitterMinFraction)

	seen := map[time.Duration]bool{}
	for i := 0; i < 2000; i += 1 {
		effective := jitterClientLifetime(maxClientLifetime)
		if effective < low || maxClientLifetime < effective {
			t.Fatalf("effective lifetime %v outside [%v, %v]", effective, low, maxClientLifetime)
		}
		seen[effective] = true
	}
	// uniform draws must spread; 2000 identical samples means the jitter is
	// not being applied at all
	if len(seen) < 2 {
		t.Error("effective lifetime never varies: rotation stays synchronized")
	}
}

// 0 (and negative) MaxClientLifetime means rotation disabled, and jitter must
// not invent a lifetime where none was configured.
func TestLifetimeJitterDisabledStaysDisabled(t *testing.T) {
	AssertEqual(t, jitterClientLifetime(0), time.Duration(0))
	AssertEqual(t, jitterClientLifetime(-time.Minute), -time.Minute)
}

// The jitter must be drawn once at construction and used wherever removeTime
// derives from the lifetime. windowStatsWithCoalesce is the derivation site:
// it must read the stored effectiveLifetime, and must no longer read
// settings.MaxClientLifetime, which would silently re-synchronize rotation.
func TestLifetimeJitterSourceAnchor(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	constructor, ok := functionBody(source, "func newMultiClientChannel(")
	if !ok {
		t.Fatal("could not find newMultiClientChannel")
	}
	if !strings.Contains(constructor, "jitterClientLifetime(") {
		t.Error("newMultiClientChannel does not draw the lifetime jitter, so every channel rotates on the raw lifetime")
	}

	statsBody, ok := functionBody(source, "func (self *multiClientChannel) windowStatsWithCoalesce(")
	if !ok {
		t.Fatal("could not find windowStatsWithCoalesce")
	}
	if !strings.Contains(statsBody, "self.effectiveLifetime") {
		t.Error("windowStatsWithCoalesce does not derive removeTime from the jittered effective lifetime")
	}
	if strings.Contains(statsBody, "MaxClientLifetime") {
		t.Error("windowStatsWithCoalesce still reads MaxClientLifetime directly, re-synchronizing rotation")
	}
}

// removeTime on the stats a resize pass sees is first event + the stored
// effective lifetime, driven through the real stats path on a bare channel.
func TestLifetimeJitterRemoveTimeUsesEffectiveLifetime(t *testing.T) {
	client := stallTestChannel()
	client.ctx = context.Background()
	client.log = DefaultLogger()
	client.client = NewClientWithDefaults(context.Background(), NewId(), NewNoContractClientOob())
	defer client.client.Cancel()

	effectiveLifetime := 42 * time.Minute
	firstEventTime := time.Now().Add(-time.Minute)
	client.stateLock.Lock()
	client.effectiveLifetime = effectiveLifetime
	client.firstEventTime = firstEventTime
	client.stateLock.Unlock()

	stats, err := client.windowStatsWithCoalesce(false)
	AssertEqual(t, err == nil, true)
	AssertEqual(t, stats.removeTime.Equal(firstEventTime.Add(effectiveLifetime)), true)
}

// --- item 4: the speed-window drain quirk ---

// The drain branch must warn unconditionally. `remove` there comes from the
// rank math, and for a FixedWindowSize=1 (speed) window the rank-0 client
// computes remove=false -- so the best speed exit never actually drained and
// rotation was silently a no-op for it. The health-warning branch must keep
// using the rank-derived value: the rank shield exists to ride out transient
// badness in healthy clients, and only the lifetime drain is exempt.
func TestDrainBranchWarnsUnconditionally(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	body, ok := functionBody(source, "func (self *multiClientWindow) resize()")
	if !ok {
		t.Fatal("could not find resize")
	}

	nextWarningCall := func(marker string) string {
		at := strings.Index(body, marker)
		if at < 0 {
			t.Fatalf("could not find %q in resize", marker)
		}
		rest := body[at:]
		warnAt := strings.Index(rest, "setWarning(")
		if warnAt < 0 {
			t.Fatalf("no setWarning call after %q", marker)
		}
		end := strings.Index(rest[warnAt:], ")")
		return rest[warnAt : warnAt+end+1]
	}

	if got := nextWarningCall(`printStats("client drain")`); got != "setWarning(true, warnDraining)" {
		t.Errorf("the drain branch warns with %q, want setWarning(true, warnDraining): a rank-kept speed exit never drains, and the cause must say retirement, not evidence", got)
	}
	// and the drain branch hands its movable flows off exactly once (G-3):
	// the migration must be latched AND routed through the parent seam, or
	// retirement stays a deadline teardown
	drainAt := strings.Index(body, `printStats("client drain")`)
	rest := body[drainAt:]
	drainBranch := rest[:strings.Index(rest, "} else")]
	if !strings.Contains(drainBranch, "markDrainMigrateOnce()") {
		t.Error("the drain branch does not latch the migration: retirement would migrate every resize pass, or never")
	}
	if !strings.Contains(drainBranch, "self.clientMigrateFunc(client, \"drain\")") {
		t.Error("the drain branch does not call the migration seam: retirement tears down movable flows at the deadline instead of handing them off")
	}
	// and the seam is actually WIRED by the parent for both windows -- a
	// correct-but-unwired seam is the failure mode this suite pins against,
	// and deleting the two wiring lines would otherwise fail no test
	if got := strings.Count(source, "clientMigrateFunc = multiClient.migrateClientFlows"); got < 2 {
		t.Errorf("the migration seam is wired %d time(s), want 2 (quality and speed windows): an unwired seam makes every drain a deadline teardown again", got)
	}
	if got := nextWarningCall(`printStats("client health warning")`); got != "setWarning(remove, warnUnhealthy)" {
		t.Errorf("the health-warning branch warns with %q, want the rank-derived setWarning(remove, warnUnhealthy)", got)
	}
}

// --- item 5: the standing reserve ---

// One spare beyond the computed target, bounded by the hard max, skipped for
// the cases where a spare is meaningless or harmful.
func TestStandingReserveTargetTable(t *testing.T) {
	cases := []struct {
		name             string
		target           int
		hardMax          int
		standingReserve  bool
		fixedDestination bool
		want             int
	}{
		// the speed window's shape: fixed size 1, hard max 4
		{"speed window", 1, 4, true, false, 2},
		// the quality window's default shape: its memory ceiling prevents
		// the reserve from adding a seventh full client graph
		{"quality window at memory ceiling", 6, 6, true, false, 6},
		// the hard max is a hard bound: the spare never breaches it
		{"at hard max", 4, 4, true, false, 4},
		// 0 hard max is unbounded, as everywhere else
		{"no hard max", 2, 0, true, false, 3},
		// target 0 is a disabled window (a non-active fixed-profile window);
		// a spare would silently re-enable it
		{"disabled window", 0, 12, true, false, 0},
		// the A/B off switch restores exact-target sizing
		{"reserve off", 3, 12, false, false, 3},
		// a fixed destination set cannot produce a spare; asking would leave
		// every expand pass waiting out its timeout on args that cannot come
		{"fixed destination", 3, 12, true, true, 3},
	}

	for _, c := range cases {
		got := standingReserveTarget(c.target, c.hardMax, c.standingReserve, c.fixedDestination)
		if got != c.want {
			t.Errorf("%s: standingReserveTarget = %d, want %d", c.name, got, c.want)
		}
	}
}

// The reserve ships on by default, survives the settings -> override round
// trip (a missed field zeroes on every settings write, silently turning the
// behavior off), and a zero ReliabilitySettings -- every bare fixture --
// reads as off, the pre-change sizing.
func TestStandingReserveDefaultsAndOverrideRoundTrip(t *testing.T) {
	settings := DefaultMultiClientSettings()
	AssertEqual(t, settings.StandingReserve, true)

	qualityWindow := settings.WindowSizes[WindowTypeQuality]
	AssertEqual(t, qualityWindow.WindowSizeMin, 6)
	AssertEqual(t, qualityWindow.WindowSizeMax, 6)
	AssertEqual(t, qualityWindow.WindowSizeHardMax, 6)
	AssertEqual(t, standingReserveTarget(
		qualityWindow.WindowSizeMax,
		qualityWindow.WindowSizeHardMax,
		settings.StandingReserve,
		false,
	), 6)

	reliabilitySettings := ReliabilitySettingsFrom(settings)
	AssertEqual(t, reliabilitySettings.StandingReserve, true)

	AssertEqual(t, ReliabilitySettingsFrom(nil).StandingReserve, false)
}

// resize must actually apply the reserve to its computed target.
func TestStandingReserveSourceAnchor(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	body, ok := functionBody(source, "func (self *multiClientWindow) resize()")
	if !ok {
		t.Fatal("could not find resize")
	}
	if !strings.Contains(body, "standingReserveTarget(") {
		t.Error("resize does not apply the standing reserve, so failover still starts with a cold connect")
	}
}
