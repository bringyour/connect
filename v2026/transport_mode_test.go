package connect

import (
	"context"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"maps"
)

func TestInactiveDrainIgnoresEqualPriorityMode(t *testing.T) {
	transport := testingPlatformTransportModes()
	transport.settings = DefaultPlatformTransportSettings()
	transport.settings.InactiveDrainTimeout = 10 * time.Millisecond
	transport.settings.InactiveDrainMaxTimeout = 20 * time.Millisecond
	transport.modePreferences = normalizeTransportModePreferences(map[TransportMode]int{
		TransportModeH1: 7,
		TransportModeH3: 7,
	})
	transport.setActiveMode(TransportModeH3)
	ctx, cancelCtx := context.WithCancel(t.Context())
	defer cancelCtx()
	canceled := make(chan struct{})
	var once atomic.Bool
	go transport.runInactiveDrain(
		ctx,
		TransportModeH1,
		1,
		&atomic.Uint64{},
		&atomic.Uint64{},
		func() {
			if once.CompareAndSwap(false, true) {
				close(canceled)
			}
		},
	)
	select {
	case <-canceled:
		t.Fatal("equal-priority H3 incorrectly drained H1")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestInactiveDrainHasAbsoluteDeadlineDespitePayload(t *testing.T) {
	transport := testingPlatformTransportModes()
	transport.settings = DefaultPlatformTransportSettings()
	transport.settings.InactiveDrainTimeout = 20 * time.Millisecond
	transport.settings.InactiveDrainMaxTimeout = 60 * time.Millisecond
	transport.modePreferences = normalizeTransportModePreferences(nil)
	transport.setActiveMode(TransportModeH1)
	ctx, cancelCtx := context.WithCancel(t.Context())
	defer cancelCtx()
	var reads atomic.Uint64
	var writes atomic.Uint64
	canceled := make(chan struct{})
	go transport.runInactiveDrain(
		ctx,
		TransportModeH3Dns,
		1,
		&reads,
		&writes,
		func() { close(canceled) },
	)
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.After(500 * time.Millisecond)
	for {
		select {
		case <-ticker.C:
			reads.Add(1)
		case <-canceled:
			return
		case <-deadline:
			t.Fatal("payload activity defeated the absolute superseded-carrier deadline")
		}
	}
}

// testingPlatformTransportModes builds just the mode state of a
// PlatformTransport, without the network side, so the election plumbing can be
// exercised directly.
func testingPlatformTransportModes() *PlatformTransport {
	return &PlatformTransport{
		availableModeMonitor: NewMonitor(),
		availableModes:       map[TransportMode]bool{},
		mode:                 NewMonitorValue(TransportModeNone),
	}
}

// TestPlatformTransportModeAvailableNotifies pins the half of the monitor
// contract that was missing: a mutation must notify. setModeAvailable
// previously wrote availableModes and notified nobody, so the election loop —
// which subscribes before it reads — parked forever on the very first
// iteration and the active mode stayed TransportModeNone for the process life.
func TestPlatformTransportModeAvailableNotifies(t *testing.T) {
	transport := testingPlatformTransportModes()

	available, notify := transport.modesAvailable()
	if 0 != len(available) {
		t.Fatalf("available = %v, want empty", available)
	}
	select {
	case <-notify:
		t.Fatalf("notified with no change")
	default:
	}

	transport.setModeAvailable(TransportModeH1, true)
	select {
	case <-notify:
	case <-time.After(5 * time.Second):
		t.Fatalf("setModeAvailable did not notify: the election loop would never wake")
	}

	available, _ = transport.modesAvailable()
	if !available[TransportModeH1] {
		t.Fatalf("available = %v, want h1", available)
	}
}

// TestPlatformTransportModeAvailableChangeGated: re-asserting the same
// availability must not notify, so reconnect churn does not wake the election
// loop for a decision it already made.
func TestPlatformTransportModeAvailableChangeGated(t *testing.T) {
	transport := testingPlatformTransportModes()
	transport.setModeAvailable(TransportModeH1, true)

	_, notify := transport.modesAvailable()
	transport.setModeAvailable(TransportModeH1, true)
	select {
	case <-notify:
		t.Fatalf("re-asserting the same availability notified")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestPlatformTransportActiveModeNotifies: the elected mode must wake the mode
// gates and the inactive-drain watchdogs. Without it the live transport reads
// TransportModeNone forever, believes it is NOT the active transport, and arms
// a 30s inactivity kill timer against itself that nothing can cancel.
func TestPlatformTransportActiveModeNotifies(t *testing.T) {
	transport := testingPlatformTransportModes()

	mode, notify := transport.activeMode()
	if mode != TransportModeNone {
		t.Fatalf("mode = %v, want none", mode)
	}

	transport.setActiveMode(TransportModeH1)
	select {
	case <-notify:
	case <-time.After(5 * time.Second):
		t.Fatalf("setActiveMode did not notify: the drain watchdog could never re-evaluate")
	}

	mode, _ = transport.activeMode()
	if mode != TransportModeH1 {
		t.Fatalf("mode = %v, want h1", mode)
	}

	// the live transport now reads itself as active, so the drain watchdog takes
	// the benign branch instead of arming the inactivity kill timer
	if mode != TransportModeH1 {
		t.Fatalf("the h1 watchdog would still arm its kill timer against a live transport")
	}
}

// TestPlatformTransportActiveModeDoesNotWakeElection is the property that keeps
// the election loop from spinning: the loop subscribes to availableModes and
// then writes the active mode. If the active mode shared that notification, the
// loop's own write would wake it, forever.
func TestPlatformTransportActiveModeDoesNotWakeElection(t *testing.T) {
	transport := testingPlatformTransportModes()

	_, availableNotify := transport.modesAvailable()
	transport.setActiveMode(TransportModeH1)
	select {
	case <-availableNotify:
		t.Fatalf("electing a mode woke the election loop's own subscription: it would spin")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestPlatformTransportElectPreference walks the default election decision the
// run loop makes: H1 preempts H3, a worse fallback does not preempt, and losing
// the active mode falls back — to TransportModeNone when nothing remains,
// which was previously unreachable so a dropped mode left the active mode
// pinned stale.
func TestPlatformTransportElectPreference(t *testing.T) {
	transport := testingPlatformTransportModes()

	// the election exactly as `run` performs it
	elect := func() TransportMode {
		available, _ := transport.modesAvailable()
		activeMode := transport.electAvailableMode(transport.mode.Value(), available)
		transport.setActiveMode(activeMode)
		return activeMode
	}

	// nothing available
	if mode := elect(); mode != TransportModeNone {
		t.Fatalf("mode = %q, want none", mode)
	}

	// h3 connects first and becomes active
	transport.setModeAvailable(TransportModeH3, true)
	if mode := elect(); mode != TransportModeH3 {
		t.Fatalf("mode = %q, want h3", mode)
	}

	// h1 connects second. The production Auto policy strictly prefers it, so it
	// must preempt H3 even though H3 connected first.
	transport.setModeAvailable(TransportModeH1, true)
	if mode := elect(); mode != TransportModeH1 {
		t.Fatalf("mode = %q, want h1: H3 remained active after H1 became available", mode)
	}

	// h3 drops: the preferred H1 remains active
	transport.setModeAvailable(TransportModeH3, false)
	if mode := elect(); mode != TransportModeH1 {
		t.Fatalf("mode = %q, want h1", mode)
	}

	// a translation fallback connects. it is strictly worse, so it must not take over
	transport.setModeAvailable(TransportModeH3DnsPump, true)
	if mode := elect(); mode != TransportModeH1 {
		t.Fatalf("mode = %q, want h1: a worse mode took over", mode)
	}

	// h1 drops, leaving only the translation fallback
	transport.setModeAvailable(TransportModeH1, false)
	if mode := elect(); mode != TransportModeH3DnsPump {
		t.Fatalf("mode = %q, want h3dnspump", mode)
	}

	// a direct mode returns: strictly better, so it preempts the fallback
	transport.setModeAvailable(TransportModeH3, true)
	if mode := elect(); mode != TransportModeH3 {
		t.Fatalf("mode = %q, want h3: a strictly better mode did not preempt", mode)
	}

	// everything drops: fall back to none (previously unreachable)
	transport.setModeAvailable(TransportModeH3, false)
	transport.setModeAvailable(TransportModeH3DnsPump, false)
	if mode := elect(); mode != TransportModeNone {
		t.Fatalf("mode = %q, want none — a dropped mode left the active mode stale", mode)
	}
}

// The production election loop must promote and fall back through every Auto
// tier without reconstructing the PlatformTransport. This exercises the real
// notification loop rather than duplicating its decision in the test.
func TestPlatformTransportAutoPromotionAndFallbackLive(t *testing.T) {
	transport := testingPlatformTransportModes()
	transport.modePreferences = DefaultTransportModePreferences()
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan struct{})
	go func() {
		transport.runModeElection(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Error("Auto election loop did not stop")
		}
	})

	waitMode := func(want TransportMode) {
		t.Helper()
		deadline := time.NewTimer(time.Second)
		defer deadline.Stop()
		for {
			mode, notify := transport.activeMode()
			if mode == want {
				return
			}
			select {
			case <-notify:
			case <-deadline.C:
				t.Fatalf("active mode = %q, want %q", mode, want)
			}
		}
	}

	waitMode(TransportModeNone)
	for _, step := range []struct {
		mode TransportMode
		want TransportMode
	}{
		{TransportModeH3DnsPump, TransportModeH3DnsPump},
		{TransportModeH3Dns, TransportModeH3Dns},
		{TransportModeH3, TransportModeH3},
		{TransportModeH1, TransportModeH1},
	} {
		transport.setModeAvailable(step.mode, true)
		waitMode(step.want)
	}

	// Remove each winner in strict priority order. The next-best live carrier
	// must become active immediately, ending at None when all are gone.
	for _, step := range []struct {
		mode TransportMode
		want TransportMode
	}{
		{TransportModeH1, TransportModeH3},
		{TransportModeH3, TransportModeH3Dns},
		{TransportModeH3Dns, TransportModeH3DnsPump},
		{TransportModeH3DnsPump, TransportModeNone},
	} {
		transport.setModeAvailable(step.mode, false)
		waitMode(step.want)
	}

	// Recovery may announce modes in the worst possible order. Every higher
	// tier must still preempt the lower one, ending at H1.
	for _, mode := range []TransportMode{
		TransportModeH3DnsPump,
		TransportModeH3Dns,
		TransportModeH3,
		TransportModeH1,
	} {
		transport.setModeAvailable(mode, true)
		waitMode(mode)
	}
}

// Every combination of live Auto carriers must elect the strict production
// winner, independent of the previously active mode. This covers all 16
// availability sets times all five possible prior states.
func TestPlatformTransportAutoElectionExhaustiveAvailabilityMatrix(t *testing.T) {
	transport := testingPlatformTransportModes()
	transport.modePreferences = DefaultTransportModePreferences()
	modes := []TransportMode{
		TransportModeH1,
		TransportModeH3,
		TransportModeH3Dns,
		TransportModeH3DnsPump,
	}
	priorModes := append([]TransportMode{TransportModeNone}, modes...)
	for mask := 0; mask < 1<<len(modes); mask++ {
		available := map[TransportMode]bool{}
		want := TransportModeNone
		for i, mode := range modes {
			if mask&(1<<i) != 0 {
				available[mode] = true
				if want == TransportModeNone {
					want = mode
				}
			}
		}
		for _, prior := range priorModes {
			if got := transport.electAvailableMode(prior, available); got != want {
				t.Errorf("mask=%04b prior=%q elected=%q want=%q", mask, prior, got, want)
			}
		}
	}
}

// TestTransportModeNoneIsWorst: TransportModeNone is the absence of a transport
// and must rank below every real mode. It is absent from the preference table,
// so reading the map directly scored it 0 — better than everything — which made
// every mode gate's predicate false and meant no transport ever stood down.
func TestTransportModeNoneIsWorst(t *testing.T) {
	realModes := []TransportMode{
		TransportModeH3DnsPump,
		TransportModeH3Dns,
		TransportModeH3,
		TransportModeH1,
	}
	for _, mode := range realModes {
		if !isBetterMode(mode, TransportModeNone) {
			t.Errorf("%v is not better than none", mode)
		}
		if isBetterMode(TransportModeNone, mode) {
			t.Errorf("none is better than %v", mode)
		}
	}
	// an unknown mode ranks with none, not above everything
	if isBetterMode(TransportMode("nonsense"), TransportModeH1) {
		t.Errorf("an unknown mode outranks h1")
	}
}

// TestTransportModeTiers pins the four-tier production preference: H1 is the
// primary carrier, direct H3 is its first fallback, H3-over-DNS follows, and
// H3-over-DNS-pump is the final fallback.
func TestTransportModeTiers(t *testing.T) {
	want := []TransportMode{
		TransportModeH1,
		TransportModeH3,
		TransportModeH3Dns,
		TransportModeH3DnsPump,
	}
	for betterIndex, better := range want {
		for _, worse := range want[betterIndex+1:] {
			if !isBetterMode(better, worse) {
				t.Errorf("%q is not better than %q", better, worse)
			}
			if isBetterMode(worse, better) {
				t.Errorf("%q incorrectly outranks %q", worse, better)
			}
		}
	}
}

// TestPlatformTransportCustomModePreferences verifies that Auto can enable a
// subset and that equal custom priorities retain the same coexistence rule.
func TestPlatformTransportCustomModePreferences(t *testing.T) {
	transport := testingPlatformTransportModes()
	transport.settings = DefaultPlatformTransportSettings()
	transport.settings.ModeInitialDelay = 250 * time.Millisecond
	transport.modePreferences = normalizeTransportModePreferences(map[TransportMode]int{
		TransportModeH3:        10,
		TransportModeH1:        10,
		TransportModeH3DnsPump: 100,
	})

	wantOrder := []TransportMode{TransportModeH1, TransportModeH3, TransportModeH3DnsPump}
	if got := transport.orderedModes(); !slices.Equal(got, wantOrder) {
		t.Fatalf("ordered modes = %v, want %v", got, wantOrder)
	}
	if standDown, _ := transport.standDown(TransportModeH1); standDown {
		t.Fatal("h1 stood down at startup")
	}
	transport.setActiveMode(TransportModeH3)
	if standDown, _ := transport.standDown(TransportModeH1); standDown {
		t.Fatal("equal-priority h1 stood down for h3")
	}
	if delay := transport.modeInitialDelay(TransportModeH3); delay != 0 {
		t.Fatalf("h3 delay = %s, want 0", delay)
	}
	if delay := transport.modeInitialDelay(TransportModeH3DnsPump); delay != 250*time.Millisecond {
		t.Fatalf("h3dnspump delay = %s, want one tier interval", delay)
	}
}

// TestPlatformTransportStandDown covers the gate predicate. A transport runs
// when it is active, when only an equal custom mode is active (ties coexist),
// when a worse mode is active, and — critically — at startup when the active
// mode is none. It stands down only while a STRICTLY better mode is active. The
// arguments were previously reversed, so a transport stood down when it was
// BETTER than the active mode.
func TestPlatformTransportStandDown(t *testing.T) {
	transport := testingPlatformTransportModes()

	// startup: the active mode is none, so the first transport must be admitted
	// — otherwise it never connects, never becomes available, and the election
	// never has a mode to elect (a deadlock)
	if standDown, _ := transport.standDown(TransportModeH1); standDown {
		t.Fatalf("h1 stood down at startup: it would never connect")
	}

	// the active mode runs
	transport.setActiveMode(TransportModeH1)
	if standDown, _ := transport.standDown(TransportModeH1); standDown {
		t.Fatalf("h1 stood down while it was the active mode")
	}

	// H3 is active, but H1 is strictly better and must be allowed to start so it
	// can replace H3.
	transport.setActiveMode(TransportModeH3)
	if standDown, _ := transport.standDown(TransportModeH1); standDown {
		t.Fatalf("h1 stood down for lower-priority h3")
	}

	// a strictly better mode is active: H3 and the translation fallback stand down
	transport.setActiveMode(TransportModeH1)
	if standDown, _ := transport.standDown(TransportModeH3); !standDown {
		t.Fatalf("h3 kept running while preferred h1 was active")
	}
	if standDown, _ := transport.standDown(TransportModeH3DnsPump); !standDown {
		t.Fatalf("the translation fallback kept running while a direct mode was active")
	}

	// a strictly WORSE mode is active: the better transport must NOT stand down
	// (the old predicate parked it here — the best mode yielding to a worse one)
	transport.setActiveMode(TransportModeH3DnsPump)
	if standDown, _ := transport.standDown(TransportModeH1); standDown {
		t.Fatalf("h1 stood down for the translation fallback")
	}

	// standing down wakes when the active mode changes
	transport.setActiveMode(TransportModeH1)
	standDown, notify := transport.standDown(TransportModeH3DnsPump)
	if !standDown {
		t.Fatalf("expected the translation fallback to stand down")
	}
	transport.setActiveMode(TransportModeH3DnsPump)
	select {
	case <-notify:
	case <-time.After(5 * time.Second):
		t.Fatalf("a standing-down transport was not woken by the mode change")
	}
}

// TestTransportModeOrderDeterministic: the election sorts the modes and takes
// the first available one. maps.Keys is randomly ordered, so without a stable
// comparator the winner could differ on every pass — flipping the active mode
// and thrashing the gates.
func TestTransportModeOrderDeterministic(t *testing.T) {
	order := func() []TransportMode {
		orderedModes := slices.Collect(maps.Keys(transportModePreferences))
		slices.SortFunc(orderedModes, func(a TransportMode, b TransportMode) int {
			preferenceA := modePreference(a)
			preferenceB := modePreference(b)
			if preferenceA < preferenceB {
				return -1
			} else if preferenceB < preferenceA {
				return 1
			}
			return strings.Compare(string(a), string(b))
		})
		return orderedModes
	}

	want := []TransportMode{
		TransportModeH1,
		TransportModeH3,
		TransportModeH3Dns,
		TransportModeH3DnsPump,
	}
	for range 50 {
		got := order()
		if !slices.Equal(got, want) {
			t.Fatalf("election order = %v, want %v", got, want)
		}
	}
}
