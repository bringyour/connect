package connect

import (
	"math"
	"testing"
)

func TestChallengerWinsHysteresis(t *testing.T) {
	tests := []struct {
		name          string
		incumbent     float64
		challenger    float64
		hysteresisPct float64
		wantWins      bool
	}{
		// Positive incumbent cases (original brief tests)
		{"positive: 105 does not beat 100 at 10%", 100, 105, 10, false},
		{"positive: 111 beats 100 at 10%", 100, 111, 10, true},
		{"positive: zero hysteresis is plain >", 100, 100.5, 0, true},

		// Zero incumbent: hysteresis margin disappears, plain > applies.
		{"zero incumbent: challenger > incumbent", 0, 0.1, 10, true},
		{"zero incumbent: challenger <= incumbent", 0, 0, 10, false},
		{"zero incumbent with 0% hysteresis", 0, 0.1, 0, true},

		// Negative incumbent: the critical bug case.
		// With old formula: incumbent=-5, pct=10 -> threshold=-5.5, so -5.2 "wins" (WRONG).
		// With new formula: incumbent=-5, pct=10 -> threshold=-4.5, so -5.2 rejects, -4 accepts (CORRECT).
		{"negative: -4 beats -5 at 10%", -5, -4, 10, true},
		{"negative: -5.2 does not beat -5 at 10%", -5, -5.2, 10, false},
		{"negative: zero hysteresis is plain >", -5, -4.9, 0, true},
		{"negative: equal does not win", -5, -5, 10, false},

		// Boundary: large negative incumbent with large hysteresis.
		{"large negative: -100 needs 10% to reach -90", -100, -91, 10, false},
		{"large negative: -100 at 10% margin beats -89", -100, -89, 10, true},

		// Extremely close values near zero.
		{"tiny positive challenger beats by margin", 0.001, 0.00111, 10, true},
		{"tiny negative incumbent", -0.001, 0, 10, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := challengerWins(tc.incumbent, tc.challenger, tc.hysteresisPct)
			if got != tc.wantWins {
				t.Errorf("challengerWins(%f, %f, %f) = %v, want %v",
					tc.incumbent, tc.challenger, tc.hysteresisPct, got, tc.wantWins)
			}
		})
	}
}

func TestExitScoreLatencyPrefersLowRtt(t *testing.T) {
	w := classWeights(ClassLatency)
	fast := exitScore(ExitMetrics{RttMillis: 20, GoodputBytesPerSec: 1e6}, w)
	slow := exitScore(ExitMetrics{RttMillis: 200, GoodputBytesPerSec: 1e6}, w)
	if !(fast > slow) {
		t.Fatalf("latency class must rank low-RTT higher: fast=%f slow=%f", fast, slow)
	}
}

func TestExitScoreBulkPrefersGoodput(t *testing.T) {
	w := classWeights(ClassBulk)
	fat := exitScore(ExitMetrics{RttMillis: 80, GoodputBytesPerSec: 5e6}, w)
	thin := exitScore(ExitMetrics{RttMillis: 80, GoodputBytesPerSec: 5e5}, w)
	if !(fat > thin) {
		t.Fatalf("bulk class must rank high-goodput higher: fat=%f thin=%f", fat, thin)
	}
}

func TestExitScoreSanitizesHostileInputs(t *testing.T) {
	w := classWeights(ClassLatency)

	// Baseline: a healthy exit with perfect latency and jitter.
	healthy := exitScore(ExitMetrics{RttMillis: 50, GoodputBytesPerSec: 1e6, Jitter: 20, StallEvents: 0}, w)

	// Assertion 1: Hostile RTT must score WORSE than healthy (not better).
	hostileRttCases := []struct {
		name string
		rtt  float64
	}{
		{"negative RTT", -100},
		{"+Inf RTT", math.Inf(1)},
		{"NaN RTT", math.NaN()},
	}
	for _, tc := range hostileRttCases {
		hostile := exitScore(ExitMetrics{RttMillis: tc.rtt, GoodputBytesPerSec: 1e6, Jitter: 20, StallEvents: 0}, w)
		if !(hostile < healthy) {
			t.Errorf("hostile %s: score=%f should be < healthy=%f", tc.name, hostile, healthy)
		}
		// Assertion 2: Hostile RTT must not win at 10% hysteresis.
		if challengerWins(healthy, hostile, 10) {
			t.Errorf("hostile %s should not win challengerWins against healthy at 10%%, score=%f vs incumbent=%f",
				tc.name, hostile, healthy)
		}
	}

	// Assertion 3: Hostile Jitter must score WORSE than healthy.
	hostileJitterCases := []struct {
		name   string
		jitter float64
	}{
		{"negative Jitter", -50},
		{"+Inf Jitter", math.Inf(1)},
		{"NaN Jitter", math.NaN()},
	}
	for _, tc := range hostileJitterCases {
		hostile := exitScore(ExitMetrics{RttMillis: 50, GoodputBytesPerSec: 1e6, Jitter: tc.jitter, StallEvents: 0}, w)
		if !(hostile < healthy) {
			t.Errorf("hostile %s: score=%f should be < healthy=%f", tc.name, hostile, healthy)
		}
		// Assertion 4: Hostile Jitter must not win at 10% hysteresis.
		if challengerWins(healthy, hostile, 10) {
			t.Errorf("hostile %s should not win challengerWins against healthy at 10%%, score=%f vs incumbent=%f",
				tc.name, hostile, healthy)
		}
	}

	// Assertion 5: Finiteness check (no NaN/Inf output).
	allHostileMetrics := []ExitMetrics{
		{RttMillis: -100, GoodputBytesPerSec: 1e6, Jitter: 0, StallEvents: 0},
		{RttMillis: 50, GoodputBytesPerSec: math.Inf(1), Jitter: 0, StallEvents: 0},
		{RttMillis: 50, GoodputBytesPerSec: 1e6, Jitter: math.NaN(), StallEvents: 0},
		{RttMillis: math.Inf(1), GoodputBytesPerSec: 1e6, Jitter: 0, StallEvents: 0},
		{RttMillis: math.NaN(), GoodputBytesPerSec: 1e6, Jitter: 0, StallEvents: 0},
	}

	for i, m := range allHostileMetrics {
		score := exitScore(m, w)
		if math.IsNaN(score) || math.IsInf(score, 0) {
			t.Errorf("exitScore case %d returned non-finite: %f", i, score)
		}
	}
}

func TestExitScoreGuardsStallEvents(t *testing.T) {
	w := classWeights(ClassLatency)

	// Baseline: healthy exit with 0 stalls.
	healthy := exitScore(ExitMetrics{RttMillis: 50, GoodputBytesPerSec: 1e6, Jitter: 20, StallEvents: 0}, w)

	// Assertion 1: Negative StallEvents must not score higher than 0.
	// Corrupt counter claiming -50 stalls must not become a reward.
	negativeStalls := exitScore(ExitMetrics{RttMillis: 50, GoodputBytesPerSec: 1e6, Jitter: 20, StallEvents: -50}, w)
	if negativeStalls > healthy {
		t.Errorf("negative stalls: score=%f should not be > healthy=%f", negativeStalls, healthy)
	}

	// Assertion 2: Negative StallEvents must not win at 10% hysteresis.
	if challengerWins(healthy, negativeStalls, 10) {
		t.Errorf("negative stalls should not win challengerWins against healthy at 10%%, score=%f vs incumbent=%f",
			negativeStalls, healthy)
	}

	// Assertion 3: Ordering preserved across normal stall counts (0 < 1 < 5).
	stall1 := exitScore(ExitMetrics{RttMillis: 50, GoodputBytesPerSec: 1e6, Jitter: 20, StallEvents: 1}, w)
	stall5 := exitScore(ExitMetrics{RttMillis: 50, GoodputBytesPerSec: 1e6, Jitter: 20, StallEvents: 5}, w)

	if !(healthy > stall1) {
		t.Errorf("ordering: 0 stalls=%f should be > 1 stall=%f", healthy, stall1)
	}
	if !(stall1 > stall5) {
		t.Errorf("ordering: 1 stall=%f should be > 5 stalls=%f", stall1, stall5)
	}

	// Assertion 4: Extreme stall count produces bounded, finite score.
	// Cap is at 3.0 penalty, so stalls above 30 should all give the same score.
	stall30 := exitScore(ExitMetrics{RttMillis: 50, GoodputBytesPerSec: 1e6, Jitter: 20, StallEvents: 30}, w)
	stall100 := exitScore(ExitMetrics{RttMillis: 50, GoodputBytesPerSec: 1e6, Jitter: 20, StallEvents: 100}, w)

	if math.IsNaN(stall30) || math.IsInf(stall30, 0) {
		t.Errorf("stall30 returned non-finite: %f", stall30)
	}
	if math.IsNaN(stall100) || math.IsInf(stall100, 0) {
		t.Errorf("stall100 returned non-finite: %f", stall100)
	}
	// Both should be identical (capped).
	if stall30 != stall100 {
		t.Errorf("both stall30=%f and stall100=%f should be capped identically, not %f",
			stall30, stall100, stall100-stall30)
	}
}

func TestDemotionNeedsNofM(t *testing.T) {
	d := &demotionState{}
	if d.observe(false, 3) || d.observe(false, 3) {
		t.Fatal("demoted before 3 consecutive bad")
	}
	if !d.observe(false, 3) {
		t.Fatal("should demote on 3rd consecutive bad")
	}
	d2 := &demotionState{}
	d2.observe(false, 3)
	d2.observe(true, 3) // recovery resets
	if d2.observe(false, 3) {
		t.Fatal("a good sample must reset the streak")
	}
}

func TestLessLoadedTieBreak(t *testing.T) {
	// within 10% margin → prefer less-loaded (b has fewer flows)
	if !lessLoadedTieBreak(100, 105, 30, 5, 10) {
		t.Fatal("near-tie should prefer the less-loaded exit b")
	}
	// b clearly worse → keep a even though a is more loaded
	if lessLoadedTieBreak(100, 60, 30, 5, 10) {
		t.Fatal("clear score gap must beat load tie-break")
	}
}

func TestLessLoadedTieBreakNegativeScores(t *testing.T) {
	// Both exits are degraded (negative scores), in a near-tie within 10% margin:
	// a=-5, b=-4.9. Threshold for b: -5 + 0.5 = -4.5; -4.9 > -4.5 is false.
	// Reverse: -4.9 + 0.49 = -4.41; -5 > -4.41 is false.
	// Neither wins outright → flow comparison decides.
	// With b less loaded (5 flows), prefer b.
	if !lessLoadedTieBreak(-5, -4.9, 30, 5, 10) {
		t.Fatal("near-tie with negative scores should prefer the less-loaded exit b")
	}
	// Same scores, but now a is less loaded (5 flows): should NOT prefer b.
	if lessLoadedTieBreak(-5, -4.9, 5, 30, 10) {
		t.Fatal("near-tie with negative scores should prefer the less-loaded exit a, not b")
	}
	// a clearly wins even though both negative: a=-2, b=-10 is a clear gap.
	if lessLoadedTieBreak(-2, -10, 30, 5, 10) {
		t.Fatal("clear win for negative a should not be displaced by b's load")
	}
}

// TestDefaultMultiClientSettingsKnobsAreOnByDefault pins the actual
// regression risk post-Task-8: DefaultMultiClientSettings must keep these
// four knobs on. Task 8 (feat(routing): enable class-aware scored placement
// by default) deliberately turned them on -- it is the one task in the
// smart-routing phase permitted to change a default -- so a future
// well-meaning "restore the zero-value-off convention" edit would silently
// revert every default build to the pre-Phase-2 placement/reward behavior.
// This test (renamed from
// TestDefaultMultiClientSettingsKnobsAreZeroValueOff, which asserted the
// pre-Task-8 contract) catches that regression. See also
// routing_defaults_test.go, which pins all six Task 8 knobs together
// including the session banner rendering.
func TestDefaultMultiClientSettingsKnobsAreOnByDefault(t *testing.T) {
	s := DefaultMultiClientSettings()
	if !s.ScoredPlacement {
		t.Fatal("DefaultMultiClientSettings must leave ScoredPlacement on (true)")
	}
	if s.PlacementHysteresisPct != 10 {
		t.Fatalf("DefaultMultiClientSettings must set PlacementHysteresisPct to 10, got %v", s.PlacementHysteresisPct)
	}
	if s.PlacementDemoteConsecutive != 3 {
		t.Fatalf("DefaultMultiClientSettings must set PlacementDemoteConsecutive to 3, got %v", s.PlacementDemoteConsecutive)
	}
	if !s.RewardInstrumentation {
		t.Fatal("DefaultMultiClientSettings must leave RewardInstrumentation on (true)")
	}
}

// TestNewReliabilityKnobsZeroValueOff asserts the four smart-routing knobs
// follow the same zero-value-off contract as every other ReliabilitySettings
// field for a nil override (or one built from an older struct): that must
// still leave them all off, and ReliabilitySettingsFrom must faithfully copy
// every knob through when it is set. This is the backward-compatibility
// contract -- an old caller that never heard of these fields must see
// exactly the pre-Phase-2 behavior -- which is orthogonal to
// DefaultMultiClientSettings' own default, which Task 8 turns on (see
// TestDefaultMultiClientSettingsKnobsAreOnByDefault above).
//
// The copy-fidelity half deliberately does NOT source its input from
// DefaultMultiClientSettings(): Task 8 means the defaults no longer leave
// all four knobs at false/0, so a comparison built on the defaults (got.X !=
// s.X) would be true != true regardless of whether ReliabilitySettingsFrom
// copies the field at all -- deleting the copy line entirely could still
// pass by coincidence. Instead an explicit, fully-populated
// MultiClientSettings is round-tripped, with distinct non-zero values per
// field (not the same 1 four times) so a copy line wired to the wrong
// source field is caught too, not just a missing one.
func TestNewReliabilityKnobsZeroValueOff(t *testing.T) {
	z := ReliabilitySettingsFrom(nil) // nil → zero value
	if z.ScoredPlacement || z.PlacementHysteresisPct != 0 ||
		z.PlacementDemoteConsecutive != 0 || z.RewardInstrumentation {
		t.Fatal("new knobs must be zero-value-off (legacy behavior)")
	}

	src := &MultiClientSettings{
		ScoredPlacement:            true,
		PlacementHysteresisPct:     12.5,
		PlacementDemoteConsecutive: 3,
		RewardInstrumentation:      true,
	}
	got := ReliabilitySettingsFrom(src)
	if got.ScoredPlacement != true {
		t.Fatal("ReliabilitySettingsFrom must copy ScoredPlacement")
	}
	if got.PlacementHysteresisPct != 12.5 {
		t.Fatalf("ReliabilitySettingsFrom must copy PlacementHysteresisPct: got %v, want 12.5", got.PlacementHysteresisPct)
	}
	if got.PlacementDemoteConsecutive != 3 {
		t.Fatalf("ReliabilitySettingsFrom must copy PlacementDemoteConsecutive: got %v, want 3", got.PlacementDemoteConsecutive)
	}
	if got.RewardInstrumentation != true {
		t.Fatal("ReliabilitySettingsFrom must copy RewardInstrumentation")
	}
}
