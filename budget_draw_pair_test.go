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
// which is 3M/64, against the tun's M/8. So the stream window is three eighths
// of the tun's maximum at every budget where neither floor binds — above 8 MiB,
// where the H3 window's 384 KiB floor releases last.
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

	// the ratio the two fractions fix, at every budget
	for _, s := range samples {
		if 8*s.h3Stream != 3*s.tunMax {
			t.Errorf(
				"at a %d byte budget the H3 stream window is %d against a tun maximum of %d, a ratio of %.3f rather than the 0.375 the fractions fix. These two ceilings are in series on the download path, so the smaller binds: if one is a draw on the budget and the other is still a memory-scaled constant, the raise on the drawn side is inert and the reach arithmetic of §42 and §43 does not hold",
				s.budget, s.h3Stream, s.tunMax, float64(s.h3Stream)/float64(s.tunMax),
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
// 3M/64 of those, about 960 KiB and 1.125 MiB, and not the 384 KiB floor.
//
// Below the reference the draw and the scaled constant it replaces are the same
// number by construction (three eighths of M/8 is 3M/64), so this is a guard
// that passes on both forms. It fails if the carrier is resized from the
// platform share, if the floor starts binding at a shipped target, or if the
// process budget leaks into the per-device surface.
func TestTheH3WindowsAtTheShippedDeviceTargets(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	for _, c := range []struct {
		name       string
		target     ByteCount
		stream     ByteCount
		connection ByteCount
	}{
		{"the 20 MiB desktop device target", mib(20), kib(960), kib(1280)},
		{"the 24 MiB mobile device target", mib(24), kib(1152), kib(1536)},
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
