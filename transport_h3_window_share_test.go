package connect

import "testing"

// THROUGHPUTFIX §42.1: the client's H3 stream and connection receive windows
// are a draw on the memory budget rather than constants sized for one host.
//
// The finding this guards, the same one 687b61c guards for the transfer share
// (§37.22, §44.2 constraint 1). `memoryScale` returns one at or above the
// 64 MiB reference and a fraction below it, so every window in this file was
// sized for a 64 MiB device and could only shrink from there: a desktop with a
// 256 MiB budget ran a 64 MiB device's window, and no amount of memory ever
// made the download path faster. Both helpers have that cap —
// `MemoryScaledByteCount` and `MemoryTargetScaledByteCount` call the same
// `memoryTargetScale`, and the difference between them is which budget they
// read, not whether they can grow.
//
// This row exists because the wrong pattern is the local idiom: every adjacent
// line in this file scales a constant, and copying one is the natural way to
// write a share. Substituting the idiom for the fraction makes this fail at
// the 64-to-256 MiB step, which is the negative of §44.4: below the reference
// the broken form and the right one are arithmetically identical, so a row
// that only checks small budgets cannot tell them apart.
//
// A row rather than a runtime guard, for the reason
// ip_tcp_ack_compression_floor_test.go gives: the relationship asserted here
// is among shipping constants and is computable without running a carrier, so
// it needs no seam, no timing and no scheduling, and it keeps failing years
// later when someone moves one of them.
//
// Predictions, recorded before the run: each of the three rows doubles when
// the budget doubles, at budgets above the reference as well as below; the
// reference budget reproduces today's shipping values exactly, so no host
// regresses; and an unbudgeted process keeps today's constants rather than
// falling to a floor.
func TestTheH3ReceiveWindowsAreADrawOnTheBudget(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	type sample struct {
		budget      ByteCount
		reservation ByteCount
		stream      ByteCount
		connection  ByteCount
	}

	// The resolved `quic.Config`, not the setting. 687b61c's second finding was
	// that every term can be right while the number the mechanism actually
	// clamps to never moves, so the assertion is on what quic-go is handed.
	resolved := func(settings *PlatformTransportSettings) (ByteCount, ByteCount) {
		config := newPlatformQuicConfig(settings, 1)
		return ByteCount(config.MaxStreamReceiveWindow),
			ByteCount(config.MaxConnectionReceiveWindow)
	}

	samples := []sample{}
	// The ladder starts above every row's floor crossing, which is where
	// §44.2 constraint 1 states the property: the reservation's 3 MiB floor
	// crosses at 24 MiB, so below that it is flat by design rather than by
	// the defect. The floors themselves are asserted separately below.
	for _, budget := range []ByteCount{mib(32), mib(64), mib(256), mib(1024)} {
		SetMemoryBudget(budget)
		settings := DefaultPlatformTransportSettings()
		stream, connection := resolved(settings)
		samples = append(samples, sample{
			budget:      budget,
			reservation: settings.H3BudgetByteCount,
			stream:      stream,
			connection:  connection,
		})
	}
	for _, s := range samples {
		t.Logf(
			"budget %d: reservation %d (%.3f of it), stream window %d, connection window %d",
			s.budget, s.reservation, float64(s.reservation)/float64(s.budget),
			s.stream, s.connection,
		)
	}

	rows := []struct {
		name  string
		value func(sample) ByteCount
	}{
		{"H3 reservation", func(s sample) ByteCount { return s.reservation }},
		{"stream receive window", func(s sample) ByteCount { return s.stream }},
		{"connection receive window", func(s sample) ByteCount { return s.connection }},
	}
	for _, row := range rows {
		for i := 1; i < len(samples); i += 1 {
			previous, current := samples[i-1], samples[i]
			wantRatio := float64(current.budget) / float64(previous.budget)
			gotRatio := float64(row.value(current)) / float64(max(row.value(previous), 1))
			if gotRatio < 0.99*wantRatio || 1.01*wantRatio < gotRatio {
				t.Errorf(
					"the budget went from %d to %d, %.1f times, and the %s went %d to %d, %.2f times; a draw on the budget is a fraction of it, and one that stops growing above the 64 MiB reference is a memory-scaled constant wearing a new name",
					previous.budget, current.budget, wantRatio, row.name,
					row.value(previous), row.value(current), gotRatio,
				)
			}
		}
	}

	// The windows keep the ratio they have always had, 3 MiB against 4 at the
	// reference, so a campaign that moves one has to move the other.
	for _, s := range samples {
		if s.connection*3 != s.stream*4 {
			t.Errorf(
				"at a %d byte budget the stream window is %d and the connection window %d, which is not the 3:4 the carrier has always had; the connection window bounds the stream windows sharing it, so moving one alone makes one of them unreachable",
				s.budget, s.stream, s.connection,
			)
		}
	}

	// The figures at the reference, which §43.2 moved. The stream window takes
	// six eighths of the reservation and the connection window the whole of it,
	// so at the reference they read 6 and 8 MiB against the 3 and 4 that shipped
	// before. The reservation itself does not move, at the reference or anywhere:
	// only the fractions inside it do.
	SetMemoryBudget(mib(64))
	referenceSettings := DefaultPlatformTransportSettings()
	referenceStream, referenceConnection := resolved(referenceSettings)
	for _, expected := range []struct {
		name  string
		got   ByteCount
		want  ByteCount
		shape string
	}{
		{"H3 reservation", referenceSettings.H3BudgetByteCount, mib(8), "one eighth of the reference budget"},
		{"stream receive window", referenceStream, mib(6), "six eighths of the reservation (§43.2)"},
		{"connection receive window", referenceConnection, mib(8), "the whole reservation (§43.2)"},
	} {
		if expected.got != expected.want {
			t.Errorf(
				"at the 64 MiB reference the %s is %d rather than %d (%s); these are the figures §43.2's landing is computed from, and a fraction moved without the design fails here rather than in a campaign's plateau",
				expected.name, expected.got, expected.want, expected.shape,
			)
		}
	}

	// No host regresses, which is the constraint the equalities above used to
	// carry and now cannot, since §43.2 deliberately raises both windows. Stated
	// directly instead: at every budget the resolved window is at least the
	// memory-scaled constant it replaced. This is the assertion that makes the
	// change safe to land without a migration, and it is strictly stronger than
	// the three equalities, because it holds at every budget rather than at one.
	for _, budget := range []ByteCount{
		mib(1), mib(4), mib(8), mib(16), mib(20), mib(24), mib(32), mib(48),
		mib(64), mib(128), mib(256), mib(1024),
	} {
		SetMemoryBudget(budget)
		budgetSettings := DefaultPlatformTransportSettings()
		budgetStream, budgetConnection := resolved(budgetSettings)
		for _, expected := range []struct {
			name    string
			got     ByteCount
			replace ByteCount
		}{
			{"stream receive window", budgetStream, MemoryTargetScaledByteCount(budget, mib(3), kib(384))},
			{"connection receive window", budgetConnection, MemoryTargetScaledByteCount(budget, mib(4), kib(512))},
		} {
			if expected.got < expected.replace {
				t.Errorf(
					"at a %d byte budget the %s is %d against the %d the memory-scaled constant gave; the raise has to be a raise at every budget, or some host is slower the day it lands",
					budget, expected.name, expected.got, expected.replace,
				)
			}
		}
	}

	// The absence of a budget is the absence of the surface, not a small share:
	// an unbudgeted process keeps today's constants. Falling to a floor here
	// would make every unbudgeted process — which is what a provider is
	// (§37.22) — slower the moment this lands, which is the opposite of the
	// point.
	SetMemoryBudget(0)
	unbudgetedSettings := DefaultPlatformTransportSettings()
	unbudgetedStream, unbudgetedConnection := resolved(unbudgetedSettings)
	for _, expected := range []struct {
		name string
		got  ByteCount
		want ByteCount
	}{
		{"H3 reservation", unbudgetedSettings.H3BudgetByteCount, MemoryScaledByteCount(mib(8), mib(3))},
		{"stream receive window", unbudgetedStream, MemoryScaledByteCount(mib(3), kib(384))},
		{"connection receive window", unbudgetedConnection, MemoryScaledByteCount(mib(4), kib(512))},
	} {
		if expected.got != expected.want {
			t.Errorf(
				"an unbudgeted process's %s is %d rather than today's %d; it has no budget to draw on, and a draw of nothing is the absence of the surface rather than a share of zero",
				expected.name, expected.got, expected.want,
			)
		}
	}

	// The floor case, §44.2 constraint 3. The reservation's 3 MiB floor is an
	// admission minimum — it exists so one explicitly selected H3 carrier fits
	// the 8 MiB legacy total (`newDefaultPlatformTransportBudget`) — and it is
	// deliberately not inherited by the windows, which have their own working
	// floors. Taking six eighths of the floored reservation would advertise
	// 2.25 MiB of stream credit on a host whose whole budget is 8 MiB, and more
	// than twice the entire budget at 1 MiB.
	//
	// §43.2 moved where the floors release rather than what they are: at 4 MiB
	// the draw's six and eight eighths land exactly on the two floors, above it
	// the draw carries both windows, and below it the floors hold them where
	// they have always been.
	for _, floor := range []struct {
		budget     ByteCount
		stream     ByteCount
		connection ByteCount
	}{
		{mib(8), kib(768), mib(1)},
		{mib(4), kib(384), kib(512)},
		{mib(1), kib(384), kib(512)},
	} {
		SetMemoryBudget(floor.budget)
		floorSettings := DefaultPlatformTransportSettings()
		floorStream, floorConnection := resolved(floorSettings)
		if floorStream != floor.stream || floorConnection != floor.connection {
			t.Errorf(
				"at a %d byte budget the windows are stream %d and connection %d rather than %d and %d; the reservation's floor is an admission minimum, and inheriting it into the windows grants a small host more credit than it can back",
				floor.budget, floorStream, floorConnection, floor.stream, floor.connection,
			)
		}
	}

	// The same defect, and the same fix, at the per-owner target. A device that
	// sets its own target rather than the process budget reaches the same
	// windows: `MemoryTargetScaledByteCount` shares the capped scale, so this
	// site could not grow either.
	SetMemoryBudget(0)
	targetSamples := []sample{}
	for _, target := range []ByteCount{mib(64), mib(256), mib(1024)} {
		settings := DefaultPlatformTransportSettingsWithMemoryTarget(target)
		stream, connection := resolved(settings)
		targetSamples = append(targetSamples, sample{
			budget:      target,
			reservation: settings.H3BudgetByteCount,
			stream:      stream,
			connection:  connection,
		})
	}
	for _, s := range targetSamples {
		t.Logf(
			"owner target %d: reservation %d, stream window %d, connection window %d",
			s.budget, s.reservation, s.stream, s.connection,
		)
	}
	for _, row := range rows {
		for i := 1; i < len(targetSamples); i += 1 {
			previous, current := targetSamples[i-1], targetSamples[i]
			wantRatio := float64(current.budget) / float64(previous.budget)
			gotRatio := float64(row.value(current)) / float64(max(row.value(previous), 1))
			if gotRatio < 0.99*wantRatio || 1.01*wantRatio < gotRatio {
				t.Errorf(
					"the owner target went from %d to %d, %.1f times, and the %s went %d to %d, %.2f times; the per-owner target reads the same capped scale as the process budget, so it needs the same draw",
					previous.budget, current.budget, wantRatio, row.name,
					row.value(previous), row.value(current), gotRatio,
				)
			}
		}
	}
}
