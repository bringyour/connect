package connect

import "testing"

func TestReliabilitySettingsReusesConstructedDefaults(t *testing.T) {
	settings := DefaultMultiClientSettings()
	defaults := ReliabilitySettingsFrom(settings)
	client := &RemoteUserNatMultiClient{
		settings:                   settings,
		defaultReliabilitySettings: defaults,
	}

	var got *ReliabilitySettings
	allocations := testing.AllocsPerRun(100, func() {
		got = client.reliabilitySettings()
	})
	if got != defaults {
		t.Fatal("default reliability projection was not reused")
	}
	if allocations != 0 {
		t.Fatalf("reliabilitySettings allocated %.0f objects per read, want 0", allocations)
	}

	override := &ReliabilitySettings{HeartbeatInterval: 1}
	client.reliability.Store(override)
	if got := client.reliabilitySettings(); got != override {
		t.Fatal("runtime reliability override did not take precedence")
	}
	client.reliability.Store(nil)
	if got := client.reliabilitySettings(); got != defaults {
		t.Fatal("clearing runtime override did not restore cached defaults")
	}
}
