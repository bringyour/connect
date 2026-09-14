package connect

import "testing"

// THROUGHPUTFIX §44.2's shape constraint, applied to the pair that nothing
// asserts: the client's H3 stream receive window and the tun's TCP maximum.
//
// Why they are one row rather than two. The two ceilings sit in series on the
// download path — the H3 carrier's stream window admits bytes into the client,
// and the tun's receive buffer is what the inner TCP stack may hold — so the
// smaller of them binds and raising either alone moves nothing. Each was proved
// in isolation, by `TestTheH3ReceiveWindowsAreADrawOnTheBudget` and
// `TestTheTunsMaximaAreADrawOnTheBudget`, and neither says anything about the
// other. The reach arithmetic of §42 and §43 assumes both move with the budget
// together, and a tree where one is a draw and the other is still a
// memory-scaled constant reads as a raise that delivers nothing.
//
// The ratio is fixed by the fractions, not chosen here: the H3 carrier draws an
// eighth of the budget and its stream window takes three eighths of that draw,
// which after §43.2's landing is 6M/64, against the tun's M/8. So the stream
// window is three quarters of the tun's maximum at every budget where neither
// floor binds. The ratio is read from the fraction constants rather than
// written as a number here, so that a campaign sweeping the table moves this
// row with the table and a fraction moved in one layer alone still fails it.
//
// This row is a DEFECT ROW rather than a guard, and on this branch it fails.
// The tun's maxima are a draw here; the H3 windows are still memory-scaled
// constants and go flat above the reference, so the pair diverges at the
// 64-to-128 MiB step. It passes once the H3 draw lands. That is the point of
// it: the two layers were built on separate branches and this is the assertion
// that says whether they were integrated, rather than whether each was written.
func TestTheH3AndTunCeilingsMoveTogether(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	type sample struct {
		budget   ByteCount
		h3Stream ByteCount
		tunMax   ByteCount
	}
	samples := []sample{}
	// from 8 MiB up, which is where the H3 window's floor releases and both
	// ceilings are on their draws
	for _, budget := range []ByteCount{mib(8), mib(16), mib(32), mib(64), mib(128), mib(256), mib(1024)} {
		SetMemoryBudget(budget)
		samples = append(samples, sample{
			budget:   budget,
			h3Stream: DefaultPlatformTransportSettings().H3MaxStreamReceiveWindowByteCount,
			tunMax:   ByteCount(DefaultTunSettings().TcpReceiveBuffer.Max),
		})
	}
	for _, s := range samples {
		t.Logf(
			"budget %d: h3 stream window %d, tun maximum %d (stream is %.3f of the tun)",
			s.budget, s.h3Stream, s.tunMax, float64(s.h3Stream)/float64(s.tunMax),
		)
	}

	// the ratio the two fractions fix, at every budget. The H3 stream window is
	// `M/h3BudgetShareDivisor × num/den` and the tun's maximum `M/tunBudgetShareDivisor`,
	// so the ratio is the fractions cross-multiplied and nothing is written twice.
	streamMultiple := ByteCount(h3ReceiveWindowShareDenominator * h3BudgetShareDivisor)
	tunMultiple := ByteCount(tunBudgetShareDivisor * h3StreamReceiveWindowShareNumerator)
	for _, s := range samples {
		if streamMultiple*s.h3Stream != tunMultiple*s.tunMax {
			t.Errorf(
				"at a %d byte budget the H3 stream window is %d against a tun maximum of %d, a ratio of %.3f rather than the %.3f the fractions fix. These two ceilings are in series on the download path, so the smaller binds: if one is a draw on the budget and the other is still a memory-scaled constant, the raise on the drawn side is inert and the reach arithmetic of §42 and §43 does not hold",
				s.budget, s.h3Stream, s.tunMax, float64(s.h3Stream)/float64(s.tunMax),
				float64(tunMultiple)/float64(streamMultiple),
			)
		}
	}

	// and both double when the budget doubles, which is the joint form of
	// §44.2's first constraint
	for i := 1; i < len(samples); i += 1 {
		previous, current := samples[i-1], samples[i]
		wantRatio := float64(current.budget) / float64(previous.budget)
		for _, layer := range []struct {
			name              string
			previous, current ByteCount
		}{
			{"the H3 stream window", previous.h3Stream, current.h3Stream},
			{"the tun's maximum", previous.tunMax, current.tunMax},
		} {
			gotRatio := float64(layer.current) / float64(max(layer.previous, 1))
			if gotRatio < 0.99*wantRatio || 1.01*wantRatio < gotRatio {
				t.Errorf(
					"the budget went from %d to %d, %.1f times, and %s went %d to %d, %.2f times; both layers have to move with the budget or the one that stopped is what a host's memory now buys nothing at",
					previous.budget, current.budget, wantRatio,
					layer.name, layer.previous, layer.current, gotRatio,
				)
			}
		}
	}
}

// C-31 at the targets that ship. The row above walks the PROCESS budget, but a
// shipped H3 carrier never reads it: the sdk sizes the carrier from the WHOLE
// per-device memory target, not the platform share of it
// (`sdk/device_local.go`, where `deviceMemoryShares` discards the share and the
// generator passes `settings.MemoryTargetByteCount` to
// `DefaultPlatformTransportSettingsWithMemoryTarget`). The device target is
// 20 MiB on desktop and 24 MiB on mobile, so the shipped stream window is
// 6T/64 of those after §43.2's landing, 1.875 and 2.25 MiB, and not the
// 384 KiB floor.
//
// This row is where §43.2 is read on a shipped client rather than at the
// 256 MiB budget its reach arithmetic is computed at. Before the landing the
// fractions made the draw bit-identical to the scaled constant at and below the
// reference — three eighths of T/8 is 3T/64, exactly
// `MemoryTargetScaledByteCount(T, 3 MiB, 384 KiB)` — and since no shipped
// target reaches the reference (§48.2), the H3 ceiling work was inert on every
// shipped device. Six eighths is what makes it not inert: these two numbers
// double, 34 to 68 Mb/s of stream goodput at 200 ms on Apple's target and 40 to
// 80 on Android's.
//
// It still fails if the carrier is resized from the platform share, if a floor
// starts binding at a shipped target, or if the process budget leaks into the
// per-device surface, which is what it was written for.
func TestTheH3WindowsAtTheShippedDeviceTargets(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	for _, c := range []struct {
		name       string
		target     ByteCount
		stream     ByteCount
		connection ByteCount
	}{
		{"the 20 MiB desktop device target", mib(20), kib(1920), kib(2560)},
		{"the 24 MiB mobile device target", mib(24), kib(2304), kib(3072)},
	} {
		// the process budget must not reach the per-device surface: read the
		// same target under a small and a large process budget
		for _, processBudget := range []ByteCount{0, mib(8), mib(32), mib(256)} {
			SetMemoryBudget(processBudget)
			settings := DefaultPlatformTransportSettingsWithMemoryTarget(c.target)
			if settings.H3MaxStreamReceiveWindowByteCount != c.stream ||
				settings.H3MaxConnectionReceiveWindowByteCount != c.connection {
				t.Errorf(
					"%s under a %d byte process budget gives an H3 stream window of %d and connection window of %d rather than %d and %d; a shipped carrier is sized from the whole device target, so a smaller window here means it is being sized from the platform share, the floor, or the process budget",
					c.name, processBudget,
					settings.H3MaxStreamReceiveWindowByteCount, settings.H3MaxConnectionReceiveWindowByteCount,
					c.stream, c.connection,
				)
			}
		}
		t.Logf("%s: H3 stream window %d, connection window %d", c.name, c.stream, c.connection)
	}
}
