package connect

// PlatformTransportAutoEligibility reports which configured Auto modes can be
// admitted by one otherwise-idle PlatformTransport under the structural byte
// and socket limits in settings. It deliberately ignores current budget usage:
// temporary candidate contention is not a stable platform capability and must
// not make settings UIs flicker between eligible and constrained.
//
// H3, H3-over-DNS, and H3-over-DNS-pump share one H3 reservation. When H1 is
// also configured, H1's reservation has precedence and every H3-family mode is
// eligible only if the budget can hold H1 and that shared H3 reservation
// together. With no H1 in the Auto policy, H3 can use the budget by itself.
func PlatformTransportAutoEligibility(
	settings *PlatformTransportSettings,
) map[TransportMode]bool {
	if settings == nil {
		settings = DefaultPlatformTransportSettings()
	}
	preferences := normalizeTransportModePreferences(settings.ModePreferences)
	eligible := map[TransportMode]bool{}
	if settings.PlatformTransportBudget == nil {
		for mode := range preferences {
			eligible[mode] = true
		}
		return eligible
	}

	budget := settings.PlatformTransportBudget.Stats()
	hasTransportSlot := budget.MaxTransportCount <= 0 || 1 <= budget.MaxTransportCount
	if !hasTransportSlot {
		return eligible
	}

	_, h1Configured := preferences[TransportModeH1]
	h1ByteCount := settings.H1BudgetByteCount
	if h1ByteCount <= 0 {
		h1ByteCount = MemoryScaledByteCount(kib(512), kib(256))
	}
	h3ByteCount := settings.H3BudgetByteCount
	if h3ByteCount <= 0 {
		h3ByteCount = MemoryScaledByteCount(mib(8), mib(3))
	}
	h1Eligible := !h1Configured || h1ByteCount <= budget.TotalByteCount
	if h1Configured && h1Eligible {
		eligible[TransportModeH1] = true
	}

	h3GroupByteCount := h3ByteCount
	if h1Configured {
		// run waits for H1 admission before it starts the H3 mode group, so an
		// H1 claim that cannot fit prevents every Auto runner from starting.
		if !h1Eligible {
			return eligible
		}
		h3GroupByteCount += h1ByteCount
	}
	if budget.TotalByteCount < h3GroupByteCount {
		return eligible
	}
	for mode := range preferences {
		if isH3TransportMode(mode) {
			eligible[mode] = true
		}
	}
	return eligible
}
