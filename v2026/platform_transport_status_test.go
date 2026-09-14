package connect

import (
	"maps"
	"testing"
)

func TestPlatformTransportAutoEligibility(t *testing.T) {
	allModes := map[TransportMode]bool{
		TransportModeH3:        true,
		TransportModeH1:        true,
		TransportModeH3Dns:     true,
		TransportModeH3DnsPump: true,
	}
	tests := []struct {
		name        string
		budget      ByteCount
		preferences map[TransportMode]int
		want        map[TransportMode]bool
	}{
		{
			name:        "combined Auto fits",
			budget:      10,
			preferences: DefaultTransportModePreferences(),
			want:        allModes,
		},
		{
			name:        "H1 precedence degrades combined Auto",
			budget:      9,
			preferences: DefaultTransportModePreferences(),
			want:        map[TransportMode]bool{TransportModeH1: true},
		},
		{
			name:   "H3-only Auto uses standalone budget",
			budget: 8,
			preferences: map[TransportMode]int{
				TransportModeH3:        1,
				TransportModeH3DnsPump: 2,
			},
			want: map[TransportMode]bool{
				TransportModeH3:        true,
				TransportModeH3DnsPump: true,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			settings := DefaultPlatformTransportSettings()
			settings.PlatformTransportBudget = NewPlatformTransportBudget(test.budget, 1)
			settings.H1BudgetByteCount = 2
			settings.H3BudgetByteCount = 8
			settings.ModePreferences = test.preferences
			if got := PlatformTransportAutoEligibility(settings); !maps.Equal(got, test.want) {
				t.Fatalf("eligibility = %v, want %v", got, test.want)
			}
		})
	}
}

func TestPlatformTransportAutoEligibilityWithoutBudget(t *testing.T) {
	settings := DefaultPlatformTransportSettings()
	settings.PlatformTransportBudget = nil
	if got := PlatformTransportAutoEligibility(settings); len(got) != 4 {
		t.Fatalf("unbudgeted Auto eligibility = %v, want every configured mode", got)
	}
}
