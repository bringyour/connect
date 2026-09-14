package connect

import (
	"strings"
	"testing"
)

// --- Task 8: the owner-visible switch ---
//
// This is the ONLY task in the smart-routing phase permitted to change a
// default. Every other knob in this family ships zero-value-off; these six
// turn ON in DefaultMultiClientSettings so scoredplacement=1 actually means
// something for the owner without hand-flipping a developer-menu override on
// every build.
//
// QuarantineReentryRamp is deliberately NOT in this task's list -- its decay
// story is separate -- and must stay at its zero-value-off default.

// TestDefaultMultiClientSettingsEnablesScoredRoutingByDefault pins the six
// field values directly on the settings struct.
func TestDefaultMultiClientSettingsEnablesScoredRoutingByDefault(t *testing.T) {
	s := DefaultMultiClientSettings()

	if !s.LightClassifier {
		t.Error("DefaultMultiClientSettings must turn LightClassifier on")
	}
	if !s.ScoredPlacement {
		t.Error("DefaultMultiClientSettings must turn ScoredPlacement on")
	}
	if !s.RewardInstrumentation {
		t.Error("DefaultMultiClientSettings must turn RewardInstrumentation on")
	}
	if s.PlacementHysteresisPct != 10 {
		t.Errorf("DefaultMultiClientSettings must set PlacementHysteresisPct=10, got %v", s.PlacementHysteresisPct)
	}
	if s.PlacementDemoteConsecutive != 3 {
		t.Errorf("DefaultMultiClientSettings must set PlacementDemoteConsecutive=3, got %v", s.PlacementDemoteConsecutive)
	}
	if !s.QuarantineDampening {
		t.Error("DefaultMultiClientSettings must turn QuarantineDampening on")
	}

	// out of scope for this task: QuarantineReentryRamp's decay story is
	// separate and must stay zero-value-off.
	if s.QuarantineReentryRamp != 0 {
		t.Errorf("DefaultMultiClientSettings must leave QuarantineReentryRamp at 0 (not in this task's scope), got %v", s.QuarantineReentryRamp)
	}
}

// TestDefaultMultiClientSettingsBannerRendersScoredRoutingOn proves the
// reflection-based session banner (relSettingsFields) shows the new defaults
// with no hand-edited banner list -- setting the fields in
// DefaultMultiClientSettings is sufficient, the same contract every other
// knob in this family already relies on.
func TestDefaultMultiClientSettingsBannerRendersScoredRoutingOn(t *testing.T) {
	settings := ReliabilitySettingsFrom(DefaultMultiClientSettings())
	lines := relSessionBannerLines("", settings, relLineMaxChars)
	if len(lines) != 1 {
		t.Fatalf("expected a single banner line, got %d: %v", len(lines), lines)
	}
	body := lines[0]

	for _, want := range []string{
		"lightclassifier=1",
		"scoredplacement=1",
		"rewardinstrumentation=1",
		"placementhysteresispct=10.00",
		"placementdemoteconsecutive=3",
		"quarantinedampening=1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("default banner does not render %s: %s", want, body)
		}
	}

	// out of scope for this task: must still render off in the banner too.
	if !strings.Contains(body, "quarantinereentryramp=0") {
		t.Errorf("default banner must still render quarantinereentryramp=0 (out of this task's scope): %s", body)
	}
}
