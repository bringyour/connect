package connect

import "testing"

// The defect underneath this entire program, stated in prose in half a dozen
// sections of THROUGHPUTFIX and asserted nowhere until now: the memory scale is
// capped at one, so every constant that passes through it can only shrink.
//
// `memoryTargetScale` returns one at or above `referenceMemoryBudgetByteCount`
// and budget/reference below it. `MemoryScaledByteCount` and
// `MemoryTargetScaledByteCount` both call it — they differ in which budget they
// read, not in whether they can grow — so every constant written in that idiom
// was sized for a 64 MiB device and can only have memory taken away from it. A
// host with eight gigabytes runs a 64 MiB device's buffers. That is why no
// amount of memory ever made this system faster, and why every ceiling this
// program raises had to become a draw on the budget instead
// (`tunBudgetShareByteCount`, `transferBudgetShareByteCount`).
//
// This is a guard rather than a defect row, and it is worth being explicit
// about that: the tree is correct today and this passes before and after. It
// exists because the cap is exactly the kind of thing that gets "fixed" back,
// and both directions of that fix are silent fleet-wide events with nothing to
// read afterwards:
//
//   - make the scale exceed one and every memory-scaled constant in the tree
//     inflates at once, including per-connection ceilings that a hosted proxy
//     multiplies by tenant and by flow with no aggregate bound;
//   - raise the reference and every one of them shrinks on every host below the
//     new reference, which is a fleet-wide slowdown that looks like nothing.
//
// Neither had a row that fails today. Both fail here.
func TestTheMemoryScaleOnlyShrinks(t *testing.T) {
	// the shipping (unscaled, floor) pairs this idiom is used with, named to
	// the site so a reader can check them
	scaled := []struct {
		name     string
		unscaled ByteCount
		floor    ByteCount
	}{
		{"DefaultSendBufferSettings().ResendQueueMaxByteCount", mib(2), kib(256)},
		{"DefaultReceiveBufferSettings().ReceiveQueueMaxByteCount", mib(2) + kib(512), kib(320)},
		{"DefaultTunSettings().TcpReceiveBuffer.Default", mib(1), kib(128)},
		{"DefaultWebRtcSettings().ReceiveBufferSize", kib(512), kib(256)},
	}

	// the cap: no budget, however large, scales a constant above its unscaled
	// value. `MemoryTargetScaledByteCount` is the same arithmetic against an
	// explicit budget, so this is computed rather than run.
	for _, budget := range []ByteCount{
		mib(1), mib(8), mib(32), mib(64), mib(128), mib(256), mib(1024), mib(8192),
	} {
		for _, s := range scaled {
			got := MemoryTargetScaledByteCount(budget, s.unscaled, s.floor)
			if s.unscaled < got {
				t.Errorf(
					"at a %d byte budget %s scaled to %d, above its unscaled %d. A scale above one inflates every memory-scaled constant in the tree at once, including per-connection ceilings a hosted proxy multiplies by tenant and by flow",
					budget, s.name, got, s.unscaled,
				)
			}
		}
	}

	// the scale itself never leaves (0, 1], at any budget
	for _, budget := range []ByteCount{
		-1, 0, mib(1), mib(20), mib(24), mib(64), mib(65), mib(1024), mib(8192),
	} {
		if scale := memoryTargetScale(budget); scale <= 0 || 1 < scale {
			t.Errorf(
				"memoryTargetScale(%d) is %f, outside (0, 1]; a scale above one inflates every memory-scaled constant at once",
				budget, scale,
			)
		}
	}

	// the reference, pinned as a number. The checks after this one read the
	// symbol, so a moved reference moves them with it and they cannot see it:
	// a mutation raising the reference to 128 MiB passed all of them. This and
	// the literal-budget reads below are what fail on that mutation.
	if referenceMemoryBudgetByteCount != mib(64) {
		t.Errorf(
			"the memory reference is %d rather than 64 MiB; every memory-scaled constant is whole only at or above it, so raising it shrinks each one on every host below the new value, a fleet-wide slowdown with nothing to read afterwards",
			referenceMemoryBudgetByteCount,
		)
	}
	for _, s := range scaled {
		if got := MemoryTargetScaledByteCount(mib(64), s.unscaled, s.floor); got != s.unscaled {
			t.Errorf(
				"%s reads %d at a literal 64 MiB budget rather than its unscaled %d; the reference has moved above 64 MiB",
				s.name, got, s.unscaled,
			)
		}
		if got, want := MemoryTargetScaledByteCount(mib(32), s.unscaled, s.floor), max(s.floor, s.unscaled/2); got != want {
			t.Errorf(
				"%s reads %d at a literal 32 MiB budget rather than %d; the reference is no longer 64 MiB",
				s.name, got, want,
			)
		}
	}

	// the shape against the symbol: at the reference a scaled value is exactly
	// its unscaled value, at half the reference exactly half, and above the
	// reference unchanged. These catch a scale that is not proportional or not
	// saturating; a moved reference is caught above, not here.
	for _, s := range scaled {
		atReference := MemoryTargetScaledByteCount(referenceMemoryBudgetByteCount, s.unscaled, s.floor)
		if atReference != s.unscaled {
			t.Errorf(
				"%s reads %d at the %d byte reference rather than its unscaled %d; the reference is the budget at which a scaled constant is whole, and moving it shrinks every one of them on every host below the new value",
				s.name, atReference, referenceMemoryBudgetByteCount, s.unscaled,
			)
		}
		half := MemoryTargetScaledByteCount(referenceMemoryBudgetByteCount/2, s.unscaled, s.floor)
		if want := max(s.floor, s.unscaled/2); half != want {
			t.Errorf(
				"%s reads %d at half the reference rather than %d; below the reference the scale is proportional, and a value that is not is either floored or no longer scaled",
				s.name, half, want,
			)
		}
		above := MemoryTargetScaledByteCount(4*referenceMemoryBudgetByteCount, s.unscaled, s.floor)
		if above != s.unscaled {
			t.Errorf(
				"%s reads %d at four times the reference rather than its unscaled %d; the scale saturates at one, and this row is what says so",
				s.name, above, s.unscaled,
			)
		}
	}

	// and the wiring: the shipping settings carry the cap, so a host's memory
	// does not reach them
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	SetMemoryBudget(referenceMemoryBudgetByteCount)
	atReference := DefaultSendBufferSettings().ResendQueueMaxByteCount
	tunDrawAtReference := DefaultTunSettings().TcpReceiveBuffer.Max
	SetMemoryBudget(4 * referenceMemoryBudgetByteCount)
	aboveReference := DefaultSendBufferSettings().ResendQueueMaxByteCount
	tunDrawAbove := DefaultTunSettings().TcpReceiveBuffer.Max

	if atReference != aboveReference {
		t.Errorf(
			"the shipping resend window moved from %d to %d when the budget quadrupled past the reference; if a memory-scaled constant has started growing, the cap this program is built on is gone and every ceiling in the tree moved with it",
			atReference, aboveReference,
		)
	}
	// the contrast that makes the finding legible: same budget change, same
	// file, and the drawn ceiling quadruples while the scaled one does not
	if tunDrawAbove <= tunDrawAtReference {
		t.Errorf(
			"the tun's drawn maximum read %d at the reference and %d at four times it; a draw has to grow with the budget or it is a scaled constant wearing a new name, and then nothing in the tree answers a host's memory",
			tunDrawAtReference, tunDrawAbove,
		)
	}
	t.Logf(
		"scaled constant %d -> %d (capped) against drawn ceiling %d -> %d across a fourfold budget raise",
		atReference, aboveReference, tunDrawAtReference, tunDrawAbove,
	)
}
