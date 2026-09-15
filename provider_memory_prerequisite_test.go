package connect

import "testing"

// The public constructors must reach QUIC's actual receive windows at small,
// reference and desktop targets, with reservations backing those windows.
func TestProviderMemoryH3TargetsReachActualQuicWindows(t *testing.T) {
	previous := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(previous) })
	for _, test := range []struct {
		target ByteCount
		reservation ByteCount
		stream ByteCount
		connection ByteCount
	}{
		{target: 0, reservation: mib(8), stream: mib(3), connection: mib(4)},
		{target: mib(1), reservation: mib(3), stream: kib(384), connection: kib(512)},
		{target: mib(4), reservation: mib(3), stream: kib(384), connection: kib(512)},
		{target: mib(8), reservation: mib(3), stream: kib(768), connection: mib(1)},
		{target: mib(20), reservation: mib(3), stream: kib(1920), connection: kib(2560)},
		{target: mib(24), reservation: mib(3), stream: kib(2304), connection: mib(3)},
		{target: mib(32), reservation: mib(4), stream: mib(3), connection: mib(4)},
		{target: mib(64), reservation: mib(8), stream: mib(6), connection: mib(8)},
		{target: mib(256), reservation: mib(32), stream: mib(24), connection: mib(32)},
		{target: gib(1), reservation: mib(128), stream: mib(96), connection: mib(128)},
	} {
		SetMemoryBudget(test.target)
		for _, settings := range []*PlatformTransportSettings{
			DefaultPlatformTransportSettings(),
			DefaultPlatformTransportSettingsWithMemoryTarget(test.target),
		} {
			config := newPlatformQuicConfig(settings, 1)
			if ByteCount(config.MaxStreamReceiveWindow) != test.stream || ByteCount(config.MaxConnectionReceiveWindow) != test.connection || settings.H3BudgetByteCount != test.reservation {
				t.Fatalf("target %d: actual H3 stream=%d connection=%d reservation=%d, want %d/%d/%d", test.target, config.MaxStreamReceiveWindow, config.MaxConnectionReceiveWindow, settings.H3BudgetByteCount, test.stream, test.connection, test.reservation)
			}
			if config.InitialStreamReceiveWindow > config.MaxStreamReceiveWindow || config.InitialConnectionReceiveWindow > config.MaxConnectionReceiveWindow || ByteCount(config.MaxConnectionReceiveWindow) > settings.H3BudgetByteCount {
				t.Fatalf("target %d: H3 windows exceed their working or admission bounds", test.target)
			}
		}
		partial := DefaultPlatformTransportSettings()
		partial.H3MaxStreamReceiveWindowByteCount, partial.H3MaxConnectionReceiveWindowByteCount, partial.H3BudgetByteCount = 0, 0, 0
		config := newPlatformQuicConfig(partial, 1)
		transport := &PlatformTransport{settings: partial}
		if ByteCount(config.MaxStreamReceiveWindow) != test.stream || ByteCount(config.MaxConnectionReceiveWindow) != test.connection || transport.h3BudgetByteCount() != test.reservation {
			t.Fatalf("target %d: partial H3 settings retain the old window or reservation ceiling", test.target)
		}
	}
}

// An explicit device target must not borrow another device's process budget.
func TestProviderMemoryH3TargetIgnoresUnrelatedProcessBudget(t *testing.T) {
	previous := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(previous) })
	for _, process := range []ByteCount{0, mib(8), mib(24), mib(256), gib(8)} {
		SetMemoryBudget(process)
		settings := DefaultPlatformTransportSettingsWithMemoryTarget(mib(64))
		config := newPlatformQuicConfig(settings, 1)
		if settings.H3BudgetByteCount != mib(8) || config.MaxStreamReceiveWindow != uint64(mib(6)) || config.MaxConnectionReceiveWindow != uint64(mib(8)) {
			t.Fatalf("process budget %d replaced the explicit64MiB device H3 target", process)
		}
	}
}

// Auto eligibility and the runtime reservation use the same fallback cost.
// A 256MiB process must not advertise a32MiB window against the old8MiB claim.
func TestProviderMemoryH3EligibilityAccountsForActualFallbackReservation(t *testing.T) {
	previous := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(previous) })
	SetMemoryBudget(mib(256))
	for _, test := range []struct {
		total ByteCount
		h1 bool
		wantH3 bool
	}{
		{total: mib(32)-1, h1: false, wantH3: false},
		{total: mib(32), h1: false, wantH3: true},
		{total: mib(32), h1: true, wantH3: false},
		{total: mib(32)+kib(512), h1: true, wantH3: true},
	} {
		settings := DefaultPlatformTransportSettings()
		settings.H3BudgetByteCount = 0
		settings.H1BudgetByteCount = kib(512)
		settings.ModePreferences = map[TransportMode]int{TransportModeH3: 1}
		if test.h1 {
			settings.ModePreferences[TransportModeH1] = 1
		}
		settings.PlatformTransportBudget = NewPlatformTransportBudget(test.total, 1)
		eligible := PlatformTransportAutoEligibility(settings)
		if eligible[TransportModeH3] != test.wantH3 || eligible[TransportModeH1] != test.h1 {
			t.Fatalf("budget %d h1=%t: Auto H3 eligibility=%t, want=%t", test.total, test.h1, eligible[TransportModeH3], test.wantH3)
		}
	}
}
