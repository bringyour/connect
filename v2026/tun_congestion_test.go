// This file pins the provider high-BDP auto-tuning headroom.
package connect

import "testing"

// The provider-side TCP proxy can grow beyond its memory-scaled initial window
// far enough to cover a 100 Mbit/s, one-second bandwidth-delay product.
func TestDefaultTcpBufferSettingsGrowAcrossOneSecondBdp(t *testing.T) {
	settings := DefaultTcpBufferSettings()
	const oneHundredMegabitOneSecondBdp = 100_000_000 / 8
	if settings.MinWindowSize != 64*1024 {
		t.Fatalf("minimum window=%d, want=%d", settings.MinWindowSize, 64*1024)
	}
	if settings.MaxWindowSize < oneHundredMegabitOneSecondBdp {
		t.Fatalf(
			"maximum window=%d, need at least=%d",
			settings.MaxWindowSize,
			oneHundredMegabitOneSecondBdp,
		)
	}
}
