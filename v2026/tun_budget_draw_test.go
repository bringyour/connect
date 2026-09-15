package connect

import (
	"testing"

	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
)

// THROUGHPUTFIX §43.1's ceiling, written in the shape of
// `TestTheTransferShareIsADrawOnTheBudget` because it is the same defect one
// layer over.
//
// Every window and buffer maximum in this tree was a memory-scaled constant,
// and that scale returns one at or above the 64 MiB reference and a fraction
// below. So each was sized for a reference host and can only shrink from it: a
// provider with eight gigabytes ran a 64 MiB device's buffers, and no amount of
// memory ever made this layer faster. The two maxima here are now a draw on the
// budget, one eighth of it each.
//
// This row exists because the wrong pattern is the local idiom — every adjacent
// line in `tun.go` scales a constant, and copying one is the natural way to
// write this — so an assertion is the only thing that holds. It is written
// against the shipping settings rather than against the share function, so that
// it fails on a tree where the maxima are constants instead of failing to
// compile, and so that it keeps testing the wiring and not just the arithmetic.
//
// Prediction, recorded before the run: the resolved maximum doubles when the
// budget doubles, at budgets above the reference as well as below. Above the
// reference is the whole point, because below it a memory-scaled constant is
// already proportional and looks correct; at and above it the scale saturates
// at one and the constant goes flat. Substituting
// `int(MemoryScaledByteCount(mib(4), kib(512)))` for the draw fails this row at
// the 64-to-256 MiB step.
//
// Scope, because a row that implied otherwise would be wrong: this ceiling is
// ours only where the client's inner TCP stack is this tree's gVisor, which is
// the hosted, simulated and probe modes. A shipped native desktop, phone or
// extension creates no gVisor tun — its OS tun hands packets to
// `DeviceLocal.SendPacket` — and the equivalent ceiling there belongs to the
// operating system. Nothing asserted here reaches such a host.
func TestTheTunsMaximaAreADrawOnTheBudget(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	type sample struct {
		budget  ByteCount
		receive ByteCount
		send    ByteCount
	}
	samples := []sample{}
	// The ladder carries the budgets that actually ship as well as the round
	// ones. THROUGHPUT-TESTGAPS §1.2 asks for the 20 and 24 MiB targets, and
	// they are here, but note which surface each number is: 20 and 24 MiB are
	// per-DEVICE memory targets (`sdk/device_local.go`), and this layer does
	// not read them — no per-device tun constructor exists, so the tun draws on
	// the PROCESS budget. The process budgets that ship are the Apple hosts'
	// 8, 32, 48 and 64 MiB (`PacketTunnelProvider.swift`, `AppDelegate.swift`).
	// Every one of them is below the 64 MiB reference or at it, which is the
	// region where the old scaled form shrinks, so this is where a campaign
	// would otherwise read a null and conclude the work was worthless.
	for _, budget := range []ByteCount{
		mib(16), mib(20), mib(24), mib(32), mib(48), mib(64), mib(256), mib(1024),
	} {
		SetMemoryBudget(budget)
		settings := DefaultTunSettings()
		samples = append(samples, sample{
			budget:  budget,
			receive: ByteCount(settings.TcpReceiveBuffer.Max),
			send:    ByteCount(settings.TcpSendBuffer.Max),
		})
	}
	for _, s := range samples {
		t.Logf(
			"budget %d: receive max %d, send max %d (%.3f of the budget each)",
			s.budget, s.receive, s.send, float64(s.receive)/float64(s.budget),
		)
	}

	// the draw: doubling the budget doubles each maximum
	for i := 1; i < len(samples); i += 1 {
		previous, current := samples[i-1], samples[i]
		wantRatio := float64(current.budget) / float64(previous.budget)
		for _, direction := range []struct {
			name              string
			previous, current ByteCount
		}{
			{"receive", previous.receive, current.receive},
			{"send", previous.send, current.send},
		} {
			gotRatio := float64(direction.current) / float64(max(direction.previous, 1))
			if gotRatio < 0.99*wantRatio || 1.01*wantRatio < gotRatio {
				t.Errorf(
					"the budget went from %d to %d, %.1f times, and the tun's %s maximum went %d to %d, %.2f times; a maximum that stops growing above the reference is a memory-scaled constant wearing a new name, and it is what made a host's memory buy nothing at this layer",
					previous.budget, current.budget, wantRatio,
					direction.name, direction.previous, direction.current, gotRatio,
				)
			}
		}
	}

	// the figures §43.1 commits to, so that a divisor moved without the design
	// fails here rather than in a campaign's plateau
	for _, want := range []struct {
		budget ByteCount
		max    ByteCount
	}{
		{mib(64), mib(8)},
		{mib(128), mib(16)},
		{mib(256), mib(32)},
	} {
		SetMemoryBudget(want.budget)
		settings := DefaultTunSettings()
		if ByteCount(settings.TcpReceiveBuffer.Max) != want.max ||
			ByteCount(settings.TcpSendBuffer.Max) != want.max {
			t.Errorf(
				"at a %d byte budget the tun's maxima are receive %d and send %d rather than %d, which is what §43.1's reach arithmetic is computed against",
				want.budget, settings.TcpReceiveBuffer.Max, settings.TcpSendBuffer.Max, want.max,
			)
		}
	}

	// no host regresses: the draw is never below the constant it replaces, at
	// any budget. `MemoryTargetScaledByteCount` is the old expression evaluated
	// against an explicit budget, so this compares the two forms directly.
	for _, budget := range []ByteCount{
		mib(1), mib(4), mib(8), mib(16), mib(32), mib(64), mib(128), mib(256), mib(1024),
	} {
		SetMemoryBudget(budget)
		settings := DefaultTunSettings()
		today := MemoryTargetScaledByteCount(budget, mib(4), kib(512))
		if ByteCount(settings.TcpReceiveBuffer.Max) < today ||
			ByteCount(settings.TcpSendBuffer.Max) < today {
			t.Errorf(
				"at a %d byte budget the draw gives receive %d and send %d against the constant's %d; the raise has to be a raise at every budget, or some host is slower the day it lands",
				budget, settings.TcpReceiveBuffer.Max, settings.TcpSendBuffer.Max, today,
			)
		}
	}

	// the absence of a budget is the absence of the surface rather than a small
	// share: an unbudgeted process keeps today's constant. Written as a
	// fraction alone this collapses to the floor, which would make every
	// unbudgeted host eight times slower at this layer the moment the rule was
	// turned on.
	SetMemoryBudget(0)
	unbudgeted := DefaultTunSettings()
	if ByteCount(unbudgeted.TcpReceiveBuffer.Max) != mib(4) ||
		ByteCount(unbudgeted.TcpSendBuffer.Max) != mib(4) {
		t.Errorf(
			"an unbudgeted process resolved receive %d and send %d rather than today's %d; it has no budget to draw on, and falling to a floor there is a silent regression on every host that never set one",
			unbudgeted.TcpReceiveBuffer.Max, unbudgeted.TcpSendBuffer.Max, mib(4),
		)
	}

	// only the maxima move. The minimum and the default are the bet, and §43.1
	// leaves both where they are.
	SetMemoryBudget(mib(256))
	settings := DefaultTunSettings()
	for _, unchanged := range []struct {
		name string
		got  int
		want ByteCount
	}{
		{"receive min", settings.TcpReceiveBuffer.Min, 4 * 1024},
		{"send min", settings.TcpSendBuffer.Min, 4 * 1024},
		{"receive default", settings.TcpReceiveBuffer.Default, mib(1)},
		{"send default", settings.TcpSendBuffer.Default, mib(1)},
	} {
		if ByteCount(unchanged.got) != unchanged.want {
			t.Errorf(
				"the tun's %s is %d rather than %d; this change raises the ceiling a stream may auto-tune to and moves nothing else",
				unchanged.name, unchanged.got, unchanged.want,
			)
		}
	}
}

// The maximum has to survive the handoff to gVisor, which is a separate claim
// from the settings carrying it.
//
// gVisor declares `tcp.MaxBufferSize` of 4 MiB, the same figure as the constant
// being replaced, and if that were a ceiling on the option the whole change
// would be inert. It is not: `SetOption` validates only that the minimum is
// positive and that minimum <= default <= maximum, and `MaxBufferSize` is the
// protocol's own default and the fallback used when the option cannot be read
// (`endpoint.go`, `maxReceiveBufferSize`). Both auto-tuning paths then cap
// against the option — receive moderation against `maxReceiveBufferSize()` and
// `computeTCPSendBufferSize` against `GetTCPSendBufferLimits(...).Max` — so the
// resolved option is the ceiling a stream grows to.
//
// Read back from a real stack rather than argued, because the argument is
// against a vendored dependency that can move under us.
func TestTheTunStackResolvesTheDrawnMaximum(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	// The tun draws on the PROCESS budget (`SdkSetMemoryLimit`), not the
	// per-device target, so the shipped rungs are the Apple hosts' process
	// budgets: 8 MiB legacy, 32 MiB iOS extension, 48 MiB macOS, 64 MiB. 48 and
	// 64 MiB resolve above gvisor's own 4 MiB and are the shipped rungs where a
	// clamp would show; 256 MiB is the desktop reach case.
	for _, c := range []struct {
		budget ByteCount
		want   ByteCount
	}{
		{mib(8), mib(1)},
		{mib(32), mib(4)},
		{mib(48), mib(6)},
		{mib(64), mib(8)},
		{mib(256), mib(32)},
	} {
		SetMemoryBudget(c.budget)
		settings := DefaultTunSettings()
		s := newTunStack(
			settings.TcpReceiveBuffer,
			settings.TcpSendBuffer,
			settings.TcpMaxRto,
			settings.TcpMinRto,
		)

		var receive tcpip.TCPReceiveBufferSizeRangeOption
		if err := s.TransportProtocolOption(tcp.ProtocolNumber, &receive); err != nil {
			s.Close()
			t.Fatalf("could not read back the receive range: %s", err)
		}
		var send tcpip.TCPSendBufferSizeRangeOption
		if err := s.TransportProtocolOption(tcp.ProtocolNumber, &send); err != nil {
			s.Close()
			t.Fatalf("could not read back the send range: %s", err)
		}
		s.Close()
		t.Logf(
			"at a %d byte budget: resolved receive max %d, send max %d, against gvisor's own %d",
			c.budget, receive.Max, send.Max, tcp.MaxBufferSize,
		)

		if ByteCount(receive.Max) != c.want || ByteCount(send.Max) != c.want {
			t.Errorf(
				"a %d byte budget resolved receive %d and send %d in the stack rather than %d; if gvisor clamped the option at its own %d the raise would be inert and the reach arithmetic of §43.1 would be wrong",
				c.budget, receive.Max, send.Max, c.want, tcp.MaxBufferSize,
			)
		}
	}
}

// THROUGHPUTFIX §44.2's third constraint, which is the one place the share
// table can lie.
//
// A floor is a byte count that does not scale, so floors are what a budget
// change cannot move. Their sum per backing must fit the smallest supported
// host, and the heap-backed rows this package resolves are the transport total
// (which carries the H3 reservation inside it) and the tun's two maxima. The
// pool-backed rows are a different backing and are sized from outside this
// module, so they are not summed here.
//
// The finding this row was written for, recorded because it is what the row is
// worth: §43.1's first form floored each maximum at 4 MiB, and 4 + 4 beside the
// transport total's 3 MiB floor asks 11 MiB of an 8 MiB host — the whole budget
// and a third again, before a single pooled frame. The fix was the floor and
// not the assertion: a fraction of a floored reservation inflates small hosts,
// so each maximum keeps its own working floor and the draw alone grows. Restore
// a 4 MiB floor here and this row fails at 8 MiB, which is the point of it.
func TestTheBudgetFloorsFitTheSmallestSupportedHost(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	// every supported minimum, not only the smallest. `memory_budget.go` names
	// the 8 MiB legacy target as the one the transport total's 3 MiB floor
	// exists for, and the rows have to fit at each step above it too.
	for _, budget := range []ByteCount{
		mib(8), mib(16), mib(20), mib(24), mib(32), mib(48), mib(64),
	} {
		SetMemoryBudget(budget)
		settings := DefaultTunSettings()
		transportTotal := DefaultPlatformTransportBudget().Stats().TotalByteCount
		receive := ByteCount(settings.TcpReceiveBuffer.Max)
		send := ByteCount(settings.TcpSendBuffer.Max)
		committed := transportTotal + receive + send
		t.Logf(
			"at a %d byte budget: transport total %d, tun receive %d, tun send %d, committed %d",
			budget, transportTotal, receive, send, committed,
		)

		if budget < committed {
			t.Errorf(
				"the heap-backed rows commit %d at a %d byte budget (transport total %d + tun receive %d + tun send %d), which is more than the host has. A floor is the one quantity a budget change cannot move, so this is a finding to act on in the table rather than an assertion to loosen",
				committed, budget, transportTotal, receive, send,
			)
		}

		// the special case `memory_budget.go` documents in prose and nothing
		// asserted: the aggregate transport total has to clear the H3
		// reservation's own floor, or an explicitly selected H3 carrier waits
		// forever on an aggregate that can never admit it. Raising the H3 floor
		// without raising the total is the way that breaks.
		h3Reservation := DefaultPlatformTransportSettings().H3BudgetByteCount
		if transportTotal < h3Reservation {
			t.Errorf(
				"at a %d byte budget the aggregate transport total is %d against an H3 reservation of %d; a carrier whose reservation exceeds the aggregate it draws from can never be admitted, and an explicit H3 selection waits forever rather than failing",
				budget, transportTotal, h3Reservation,
			)
		}
	}
}
