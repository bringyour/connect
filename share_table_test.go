package connect

import (
	"testing"
	"time"
)

// THROUGHPUTFIX §44, the share table: every ceiling this program raised, as a
// draw `max(floor, surface × fraction)` on a memory surface rather than as a
// constant sized for one reference host. The rows exist in four files and were
// built on four branches; this is the first place they are one object, and the
// three constraints of §44.2 are assertions over it rather than prose.
//
// WHICH SURFACE EACH ROW READS, because §44 is written as though there is one M
// and the tree has two. The question was put to the record and answered in
// §48.4: M is the process budget and the only number a host sets, a device's
// target T must be a slice of M, and within an owner every row is a fraction of
// that owner's slice. The chain is broken in one place that remains — T is a
// constant beside M rather than a slice of it, 20 or 24 MiB whatever M is — and
// that fix is the sdk's default target, not this tree's. So the rows here carry
// an owner column and the constraints below are applied per surface, which is
// the honest form until the chain is closed:
//
//	row                      surface   set by
//	transfer send window      M        connect.SetMemoryBudget, from sdk.SetMemoryLimit
//	transfer receive hold     M        the same
//	tun receive maximum       M        the same
//	tun send maximum          M        the same
//	carrier aggregate         T        DeviceLocalSettings.MemoryTargetByteCount
//	H3 reservation            T        the same
//	H3 stream window          T        the same
//	H3 connection window      T        the same
//
// Two of those are on a surface their owner does not set, and both are
// findings rather than choices. The tun rows are per-device state — the hosted
// proxy builds one private gVisor stack per client — read from the process
// budget, and the hosted proxy sets no process budget at all, so §43.1's raise
// resolves to the unscaled 4 MiB constant on the only production gVisor tun
// (§48.5, and `TestTheHostedTunPermissionHasNoAggregateBehindIt`). The transfer
// rows are permissions over a per-device pool whose ceiling is read from M
// while the budget behind it is a fraction of T (§48.2); the chain of §48.4 is
// what makes those two agree, and the backing row below asserts that it does.
//
// The rows are read through the shipping constructors, never recomputed here. A
// table that copied the fractions would agree with itself forever; this one
// fails when a constructor stops honoring the fraction it declares.
//
// Every row in this file is an invariant among constants, computed without
// running anything: no sleeps, no wall clock, no goroutines, and no cell. The
// rates in the shape and binder rows are arithmetic on those constants and the
// tree's own `goodputFactor`, not measurements.

type shareSurfaceKind int

const (
	// M, the process budget, sampled when a Default*Settings constructor runs
	shareSurfaceProcess shareSurfaceKind = iota + 1
	// T, the per-device memory target, passed to a *WithMemoryTarget constructor
	shareSurfaceDevice
)

func (self shareSurfaceKind) String() string {
	if self == shareSurfaceDevice {
		return "the device target"
	}
	return "the process budget"
}

type shareBackingKind int

const (
	// the queue holds pooled frames, so the row is a permission over a pool
	// rather than memory of its own (§44.1)
	shareBackingPooledFrames shareBackingKind = iota + 1
	// heap outside the pools, held by quic-go
	shareBackingHeap
	// heap outside the pools, held by gVisor, with no aggregate accounting
	shareBackingGvisor
)

// One row: what fraction of which surface, with the floor that does not scale,
// the loop whose round trip sets its need, and how the shipping tree resolves
// it.
type shareTableRow struct {
	name    string
	surface shareSurfaceKind
	backing shareBackingKind
	// the fraction, taken from the shipping constants so it is never written
	// twice
	numerator   ByteCount
	denominator ByteCount
	// the working minimum, the one quantity a budget change cannot move
	floorByteCount ByteCount
	// a draw that is also capped by its surface (the carrier aggregate, which
	// may never exceed the target it draws on)
	cappedBySurface bool
	// the round trip of the loop this row's window closes at the design point,
	// zero for a row that is an admission claim rather than a window
	loopRoundTrip time.Duration
	// whether the row's bytes are framed, which costs the goodput factor
	framed bool
	// read from the shipping constructors at this value of the row's surface
	resolve func(surfaceByteCount ByteCount) ByteCount
}

// The design point: 200 ms of path with the delay on the client's hop, which is
// the placement every figure in §42, §43 and §48 is computed under. The loops
// are §37.23's — the carrier hop, the transfer sequence at about 5 ms more, and
// the inner TCP at about 10 — with one hop carrying the delay rather than the
// even split, so `rtt_D` is P rather than P/2.
const (
	shareTableDesignPath         = 200 * time.Millisecond
	shareTableTransferLoopExcess = 5 * time.Millisecond
	shareTableInnerTcpLoopExcess = 10 * time.Millisecond
	shareTableCarrierLoopRtt     = shareTableDesignPath
	shareTableTransferLoopRtt    = shareTableDesignPath + shareTableTransferLoopExcess
	shareTableInnerTcpLoopRtt    = shareTableDesignPath + shareTableInnerTcpLoopExcess
)

// The sdk's own splits, mirrored with their provenance because they are the
// other half of the chain and this package cannot import them. The mirror is
// the risk these three constants carry: if the sdk moves a ratio, the rows that
// read them here keep asserting the old one. They are stated rather than
// inferred so that a reader checking the sdk has one place to look.
//
//	sdk/sdk.go:514–517          the process split, pools 12 + 2 of 34 parts,
//	                            the remaining 20 being the per-device target
//	sdk/device_local.go:99–103  the device split, dns 2 : client 9 :
//	                            platform carriers 5 : provider 4, of 20
const (
	sdkProcessRatioPacketPool      = 12
	sdkProcessRatioLargeObjectPool = 2
	sdkProcessRatioDeviceTarget    = 20
	sdkProcessRatioParts           = 34

	sdkDeviceRatioDns               = 2
	sdkDeviceRatioClient            = 9
	sdkDeviceRatioPlatformTransport = 5
	sdkDeviceRatioProvider          = 4
	sdkDeviceRatioParts             = 20
)

// The share table, resolved through the shipping constructors.
//
// The fractions are read from the constants the shipping code uses, so a
// campaign that sweeps a divisor moves this table with it and a divisor moved
// in one layer alone fails the shape row rather than passing everywhere.
func shareTableRows() []shareTableRow {
	atProcessBudget := func(read func() ByteCount) func(ByteCount) ByteCount {
		return func(budgetByteCount ByteCount) ByteCount {
			SetMemoryBudget(budgetByteCount)
			return read()
		}
	}
	h3Window := func(read func(*PlatformTransportSettings) ByteCount) func(ByteCount) ByteCount {
		return func(targetByteCount ByteCount) ByteCount {
			// the resolved quic.Config, not the setting: §42.1's second finding
			// was that every term can be right while the number the mechanism
			// clamps to never moves
			settings := DefaultPlatformTransportSettingsWithMemoryTarget(targetByteCount)
			return read(settings)
		}
	}

	return []shareTableRow{
		{
			name:           "transfer send window",
			surface:        shareSurfaceProcess,
			backing:        shareBackingPooledFrames,
			numerator:      1,
			denominator:    transferBudgetShareDivisor,
			floorByteCount: 0,
			loopRoundTrip:  shareTableTransferLoopRtt,
			framed:         true,
			resolve: atProcessBudget(func() ByteCount {
				settings := DefaultSendBufferSettingsWithBufferSize(defaultTransferBufferSize)
				settings.WindowSizing = WindowSizingFromDelivery
				settings.ApplyWindowSizing()
				if settings.ResendQueueBudget == nil {
					return 0
				}
				return settings.ResendQueueBudget.TotalByteCount()
			}),
		},
		{
			name:           "transfer receive hold",
			surface:        shareSurfaceProcess,
			backing:        shareBackingPooledFrames,
			numerator:      1,
			denominator:    transferBudgetShareDivisor,
			floorByteCount: 0,
			loopRoundTrip:  shareTableTransferLoopRtt,
			framed:         true,
			resolve: atProcessBudget(func() ByteCount {
				settings := DefaultReceiveBufferSettingsWithBufferSize(defaultTransferBufferSize)
				settings.WindowSizing = WindowSizingFromDelivery
				settings.ApplyWindowSizing()
				if settings.ReceiveQueueBudget == nil {
					return 0
				}
				return settings.ReceiveQueueBudget.TotalByteCount()
			}),
		},
		{
			name:           "tun receive maximum",
			surface:        shareSurfaceProcess,
			backing:        shareBackingGvisor,
			numerator:      1,
			denominator:    tunBudgetShareDivisor,
			floorByteCount: kib(512),
			loopRoundTrip:  shareTableInnerTcpLoopRtt,
			resolve: atProcessBudget(func() ByteCount {
				return ByteCount(DefaultTunSettings().TcpReceiveBuffer.Max)
			}),
		},
		{
			name:           "tun send maximum",
			surface:        shareSurfaceProcess,
			backing:        shareBackingGvisor,
			numerator:      1,
			denominator:    tunBudgetShareDivisor,
			floorByteCount: kib(512),
			loopRoundTrip:  shareTableInnerTcpLoopRtt,
			resolve: atProcessBudget(func() ByteCount {
				return ByteCount(DefaultTunSettings().TcpSendBuffer.Max)
			}),
		},
		{
			name:            "carrier aggregate",
			surface:         shareSurfaceDevice,
			backing:         shareBackingHeap,
			numerator:       1,
			denominator:     4,
			floorByteCount:  mib(3),
			cappedBySurface: true,
			resolve: func(targetByteCount ByteCount) ByteCount {
				return NewPlatformTransportBudgetForMemoryTarget(targetByteCount).
					Stats().TotalByteCount
			},
		},
		{
			name:           "H3 reservation",
			surface:        shareSurfaceDevice,
			backing:        shareBackingHeap,
			numerator:      1,
			denominator:    h3BudgetShareDivisor,
			floorByteCount: mib(3),
			resolve: h3Window(func(settings *PlatformTransportSettings) ByteCount {
				return settings.H3BudgetByteCount
			}),
		},
		{
			name:           "H3 stream window",
			surface:        shareSurfaceDevice,
			backing:        shareBackingHeap,
			numerator:      h3StreamReceiveWindowShareNumerator,
			denominator:    h3ReceiveWindowShareDenominator * h3BudgetShareDivisor,
			floorByteCount: kib(384),
			loopRoundTrip:  shareTableCarrierLoopRtt,
			framed:         true,
			resolve: h3Window(func(settings *PlatformTransportSettings) ByteCount {
				return ByteCount(newPlatformQuicConfig(settings, 1).MaxStreamReceiveWindow)
			}),
		},
		{
			name:           "H3 connection window",
			surface:        shareSurfaceDevice,
			backing:        shareBackingHeap,
			numerator:      h3ConnectionReceiveWindowShareNumerator,
			denominator:    h3ReceiveWindowShareDenominator * h3BudgetShareDivisor,
			floorByteCount: kib(512),
			loopRoundTrip:  shareTableCarrierLoopRtt,
			framed:         true,
			resolve: h3Window(func(settings *PlatformTransportSettings) ByteCount {
				return ByteCount(newPlatformQuicConfig(settings, 1).MaxConnectionReceiveWindow)
			}),
		},
	}
}

// drawByteCount is what the row's declared fraction says it should be at this
// value of its surface.
func (self shareTableRow) drawByteCount(surfaceByteCount ByteCount) ByteCount {
	if surfaceByteCount <= 0 {
		return 0
	}
	draw := max(self.floorByteCount, surfaceByteCount*self.numerator/self.denominator)
	if self.cappedBySurface {
		draw = min(surfaceByteCount, draw)
	}
	return draw
}

// floorCrossingByteCount is the surface value at which the fraction overtakes
// the floor. Below it the row is flat by design rather than by the defect, so
// the scaling constraint is stated above it (§44.2, constraint 1).
func (self shareTableRow) floorCrossingByteCount() ByteCount {
	return self.floorByteCount * self.denominator / self.numerator
}

// the ladder the constraints walk, from the smallest supported host to a
// gibibyte, with the targets and budgets that ship interleaved so a row cannot
// be right only at the round numbers (§48.1)
func shareTableSurfaceLadder() []ByteCount {
	return []ByteCount{
		mib(8), mib(16), mib(20), mib(24), mib(32), mib(48), mib(64),
		mib(128), mib(256), mib(512), mib(1024),
	}
}

// doublesWithItsSurface is the predicate of §44.2's first constraint, factored
// out so that the negative row can put a constant through the same test and
// show it failing. It reports the first surface step at which the row failed to
// double, or an empty string.
func shareTableScalingFailure(
	draw func(surfaceByteCount ByteCount) ByteCount,
	ladder []ByteCount,
) (previousSurface ByteCount, currentSurface ByteCount, previous ByteCount, current ByteCount, failed bool) {
	for i := 1; i < len(ladder); i += 1 {
		previousSurface, currentSurface = ladder[i-1], ladder[i]
		previous, current = draw(previousSurface), draw(currentSurface)
		wantRatio := float64(currentSurface) / float64(previousSurface)
		gotRatio := float64(current) / float64(max(previous, 1))
		if gotRatio < 0.99*wantRatio || 1.01*wantRatio < gotRatio {
			return previousSurface, currentSurface, previous, current, true
		}
	}
	return 0, 0, 0, 0, false
}

// §44.2, constraint 1: every row doubles when its surface doubles above its
// floor's crossing, and no row passes through `MemoryScaledByteCount`.
//
// This is the constraint the whole program rests on, applied to every row at
// once rather than one row per branch. The per-layer rows
// (`TestTheTransferShareIsADrawOnTheBudget`,
// `TestTheH3ReceiveWindowsAreADrawOnTheBudget`,
// `TestTheTunsMaximaAreADrawOnTheBudget`) each assert their own wiring and stay;
// this one is the obligation a new row inherits by being added to the table, so
// a fifth ceiling cannot land as a scaled constant while three tests pass.
//
// The surface each row walks is its own: the process budget for the transfer
// and tun rows, the device target for the carrier rows. Walking the wrong one
// reads a flat line for a row that is perfectly proportional to the surface its
// owner sets, which is exactly the confusion §48.4 resolved.
//
// A guard on the tree as it stands, deliberately: every row is already a draw,
// and this passes before and after. What it fails on is the next row written in
// the local idiom, which is the trap §37.22 documents — every adjacent line in
// each of these files scales a constant.
func TestEveryShareTableRowIsADrawOnItsOwnSurface(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	ladder := shareTableSurfaceLadder()
	for _, row := range shareTableRows() {
		// the floors have to release inside the ladder, or the constraint is
		// being asserted where the row is flat by design
		crossing := row.floorCrossingByteCount()
		if ladder[len(ladder)-1] <= crossing {
			t.Errorf(
				"%s crosses its floor only at %d, above the top of the ladder; a row whose fraction never overtakes its floor is a constant with extra steps",
				row.name, crossing,
			)
			continue
		}

		scalingLadder := []ByteCount{}
		for _, surface := range ladder {
			if crossing <= surface {
				scalingLadder = append(scalingLadder, surface)
			}
		}
		previousSurface, currentSurface, previous, current, failed :=
			shareTableScalingFailure(row.resolve, scalingLadder)
		if failed {
			t.Errorf(
				"%s: %s went from %d to %d, %.1f times, and the row went %d to %d, %.2f times. A draw is a fraction of its surface; one that stops growing above the 64 MiB reference is a memory-scaled constant wearing a new name, which is why no amount of memory ever made this system faster",
				row.name, row.surface, previousSurface, currentSurface,
				float64(currentSurface)/float64(previousSurface),
				previous, current, float64(current)/float64(max(previous, 1)),
			)
		}

		// and the row is the fraction it declares, at every rung including the
		// ones below the crossing where the floor holds it
		for _, surface := range ladder {
			if got, want := row.resolve(surface), row.drawByteCount(surface); got != want {
				t.Errorf(
					"%s reads %d at a %d byte %s rather than the %d its declared fraction %d/%d with a %d floor gives; the table and the constructor have to be the same arithmetic or the table is describing a tree that does not exist",
					row.name, got, surface, row.surface, want,
					row.numerator, row.denominator, row.floorByteCount,
				)
			}
		}
		t.Logf(
			"%s: %d/%d of %s, floor %d, crossing at %d, %d at 64 MiB and %d at 256",
			row.name, row.numerator, row.denominator, row.surface,
			row.floorByteCount, crossing, row.resolve(mib(64)), row.resolve(mib(256)),
		)
	}
}

// §44.2, constraint 2: what can be occupied at once is at most what backs it,
// at every level of the chain.
//
// The levels, because the constraint means a different sum at each and §44
// wrote them as one. Within a carrier, a stream's window is under the
// connection's, and the connection's is under the reservation the carrier holds
// against the aggregate — that chain is what makes the reservation something a
// connection can actually occupy rather than a number. Within a device, the
// carriers' aggregate is the platform-transport fifth of the sdk's twentieths.
// Within a process, the device targets plus the pools are at most M.
//
// The pooled rows are the case §44.1 turns on: a byte in the send queue or the
// receive hold is a pool byte, so those rows are permissions rather than memory
// of their own, and permissions may sum past M because on every path shorter
// than the knee they are not held. What must not exceed its backing is what can
// be occupied at once, and the backing for those two rows is not the pools' free
// list — that bounds retention, and on mobile it is capped at 768 KiB, far below
// what one sequence may hold in flight — but the device's transfer budgets,
// `max(3/7 × client share, 1 MiB)` and `max(4/7 × client share, 1.5 MiB)`
// (`sdk/device_local.go:214–224`). So the row that matters is whether the two
// permissions fit the client share, and they do only once the device target is a
// slice of the process budget, which is the chain of §48.4. That is asserted
// here at the derived target, and it is the arithmetic reason the chain matters
// rather than being a tidiness argument.
//
// The tun rows have no row here, and that is the known gap rather than an
// oversight: there is no aggregate for gVisor's buffers to be backed by. See
// `TestTheHostedTunPermissionHasNoAggregateBehindIt`.
func TestTheShareTableIsBackedAtEveryLevel(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	rows := map[string]shareTableRow{}
	for _, row := range shareTableRows() {
		rows[row.name] = row
	}

	for _, surface := range shareTableSurfaceLadder() {
		// within the carrier: stream under connection under reservation under
		// the aggregate
		aggregate := rows["carrier aggregate"].resolve(surface)
		reservation := rows["H3 reservation"].resolve(surface)
		stream := rows["H3 stream window"].resolve(surface)
		connection := rows["H3 connection window"].resolve(surface)
		h1 := DefaultPlatformTransportSettingsWithMemoryTarget(surface).H1BudgetByteCount
		for _, nested := range []struct {
			inner, outer         ByteCount
			innerName, outerName string
			why                  string
		}{
			{stream, connection, "H3 stream window", "H3 connection window",
				"a connection's window bounds every stream sharing it, so a stream window above it is credit the connection can never honor"},
			{connection, reservation, "H3 connection window", "H3 reservation",
				"the reservation is what the carrier holds against the aggregate, and a connection may hold its whole connection window, so a window above the reservation is memory held outside what was admitted"},
			{reservation, aggregate, "H3 reservation", "carrier aggregate",
				"a carrier whose reservation exceeds the aggregate it draws from can never be admitted, and an explicit H3 selection waits forever rather than failing"},
		} {
			if nested.outer < nested.inner {
				t.Errorf(
					"at a %d byte device target the %s is %d, above the %s of %d; %s",
					surface, nested.innerName, nested.inner,
					nested.outerName, nested.outer, nested.why,
				)
			}
		}

		// the tight form §43.2 landed: the reservation is not half idle. Above
		// the reservation's floor crossing the connection window is the whole
		// draw, so what one connection may occupy is exactly what was reserved
		// for it.
		if rows["H3 reservation"].floorCrossingByteCount() <= surface && connection != reservation {
			t.Errorf(
				"at a %d byte device target the connection window is %d against a reservation of %d; §43.2 puts the connection window at the whole draw so that the reservation is occupiable rather than half idle, and a reservation of twice the window is memory claimed against the aggregate that no connection can use",
				surface, connection, reservation,
			)
		}

		// within the device: the carriers' aggregate is the sdk's
		// platform-transport fifth of the target
		if crossing := rows["carrier aggregate"].floorCrossingByteCount(); crossing <= surface {
			sdkShare := surface * sdkDeviceRatioPlatformTransport / sdkDeviceRatioParts
			if aggregate != sdkShare {
				t.Errorf(
					"at a %d byte device target the carrier aggregate is %d against the sdk's platform-transport share of %d (%d of %d parts); connect's quarter and the sdk's fifth-of-twenty are the same number by arithmetic, and a change to either that does not move the other puts the carriers outside the share the device split gave them",
					surface, aggregate, sdkShare,
					sdkDeviceRatioPlatformTransport, sdkDeviceRatioParts,
				)
			}
		}

		// within the process: the device target the chain derives plus the
		// pools' draw are at most M
		deviceTarget := surface * sdkProcessRatioDeviceTarget / sdkProcessRatioParts
		pools := surface *
			(sdkProcessRatioPacketPool + sdkProcessRatioLargeObjectPool) /
			sdkProcessRatioParts
		if surface < deviceTarget+pools {
			t.Errorf(
				"at a %d byte process budget the derived device target %d plus the pools' %d is %d, more than the process has; the device target has to be a slice of M for the levels to compose (§48.4)",
				surface, deviceTarget, pools, deviceTarget+pools,
			)
		}

		// the pooled rows against the backing that actually bounds them: the
		// device's client share, at the target the chain derives
		clientShare := deviceTarget * sdkDeviceRatioClient / sdkDeviceRatioParts
		send := rows["transfer send window"].resolve(surface)
		hold := rows["transfer receive hold"].resolve(surface)
		if clientShare < send+hold {
			t.Errorf(
				"at a %d byte process budget the transfer send permission %d and receive hold %d sum to %d against a client share of %d at the derived device target %d; the two permissions are pool bytes bounded by the device's transfer budgets, and a ceiling read from M against a budget read from T is the incoherence the chain removes",
				surface, send, hold, send+hold, clientShare, deviceTarget,
			)
		}

		t.Logf(
			"%d: aggregate %d, reservation %d, connection %d, stream %d, H1 %d | derived target %d, pools %d, client share %d against transfer %d+%d",
			surface, aggregate, reservation, connection, stream, h1,
			deviceTarget, pools, clientShare, send, hold,
		)
	}
}

// §44.2, constraint 3: the floors' sum per backing fits the smallest supported
// value of each surface.
//
// A floor is a byte count that does not scale, so floors are the one place the
// table can lie: every other quantity moves with the budget, and a host small
// enough for the floors to bind is a host where the table's arithmetic stops
// being a fraction of anything. The record predicted this fails at 8 MiB for a
// hosted client, where §43.1's first form floored each tun maximum at 4 MiB and
// 4 + 4 beside the transport total's 3 MiB asked 11 MiB of an 8 MiB host. It
// was acted on rather than loosened: each tun maximum keeps its own 512 KiB
// working floor and the draw alone grows, so the sum now fits with room. This
// row is therefore a guard, and it is the reinstatement of a 4 MiB floor that
// it exists to fail — `TestTheBudgetFloorsFitTheSmallestSupportedHost` holds
// the same line for the process surface at every supported minimum, and this
// one adds the device surface and the carrier level the tun row cannot see.
//
// One thing it deliberately does not assert. At an 8 MiB surface the H3
// reservation's 3 MiB floor and H1's 256 KiB together exceed the aggregate's
// 3 MiB floor, so an 8 MiB host admits one carrier and not both. That is a
// decision rather than a defect: H1 registers first and H3 is left unstarted,
// which `TestMemoryBudgetFloors` pins deliberately and the Apple extension's
// budget comment records as the reason the constrained profile sits at 32 MiB.
// Asserting the sum would fail permanently against a documented choice, so it
// is logged with its arithmetic instead.
func TestTheShareTableFloorsFitEverySurfaceMinimum(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	// the smallest supported value of each surface: `memory_budget.go` names
	// the 8 MiB legacy host target, which the Apple extension still sets on
	// pre-iOS 16 hosts, and no shipped device target is below 20 MiB
	smallestSupported := map[shareSurfaceKind]ByteCount{
		shareSurfaceProcess: mib(8),
		shareSurfaceDevice:  mib(8),
	}

	floorSums := map[shareSurfaceKind]ByteCount{}
	for _, row := range shareTableRows() {
		// a permission over pooled frames commits nothing of its own, and the
		// carrier rows sit inside the aggregate rather than beside it
		if row.backing == shareBackingPooledFrames ||
			row.name == "H3 reservation" ||
			row.name == "H3 stream window" ||
			row.name == "H3 connection window" {
			continue
		}
		floorSums[row.surface] += row.drawByteCount(smallestSupported[row.surface])
	}

	for surface, committed := range floorSums {
		minimum := smallestSupported[surface]
		if minimum < committed {
			t.Errorf(
				"the rows drawing on %s commit %d at its %d byte minimum, which is more than the host has. A floor is the one quantity a budget change cannot move, so this is a finding to act on in the table rather than an assertion to loosen",
				surface, committed, minimum,
			)
		}
		t.Logf("%s: %d committed at its %d byte minimum", surface, committed, minimum)
	}

	// the carrier level at the minimum and at the targets that ship: the
	// windows keep their own floors rather than inheriting the reservation's,
	// which is what keeps a small host from advertising credit it cannot back
	for _, target := range []ByteCount{mib(8), mib(20), mib(24)} {
		settings := DefaultPlatformTransportSettingsWithMemoryTarget(target)
		config := newPlatformQuicConfig(settings, 1)
		stream := ByteCount(config.MaxStreamReceiveWindow)
		connection := ByteCount(config.MaxConnectionReceiveWindow)
		reservation := settings.H3BudgetByteCount
		aggregate := NewPlatformTransportBudgetForMemoryTarget(target).Stats().TotalByteCount
		h1 := settings.H1BudgetByteCount

		if aggregate < reservation {
			t.Errorf(
				"at a %d byte device target the aggregate is %d against an H3 reservation of %d; the reservation's floor exists so one explicitly selected H3 carrier fits the smallest supported host, and a reservation above the aggregate can never be admitted",
				target, aggregate, reservation,
			)
		}
		if connection > reservation {
			t.Errorf(
				"at a %d byte device target the connection window's floor %d exceeds the reservation %d; a window floor above the admission floor grants credit the carrier never reserved",
				target, connection, reservation,
			)
		}
		t.Logf(
			"device target %d: aggregate %d, reservation %d (+ H1 %d = %d), stream %d, connection %d",
			target, aggregate, reservation, h1, reservation+h1, stream, connection,
		)
	}
}

// §44.4's negative: a row set as a constant in the local idiom, which must fail
// the scaling constraint at the first surface step above the reference.
//
// A test suite that only ever runs correct rows through its predicate proves
// nothing about the predicate. This puts the exact expression the tree is full
// of — `MemoryTargetScaledByteCount(surface, constant, floor)`, the form every
// adjacent line in `transport.go`, `tun.go` and `transfer.go` uses — through
// the same `shareTableScalingFailure` the constraint row uses, and asserts that
// it fails, at the step above the reference and not below it.
//
// Below the reference the broken form and the right one are the same number by
// construction, which is the whole reason this trap is invisible: a row that
// only walked 8 to 64 MiB would pass on a constant. The assertion is therefore
// two-sided — the constant passes below the reference and fails above it — so
// that a predicate weakened at either end stops being able to tell them apart.
func TestAShareTableRowWrittenAsAScaledConstantFailsTheScalingRow(t *testing.T) {
	// the H3 stream window's own pair, as it was written before it became a
	// draw (`transport.go`, the no-target fallback)
	idiom := func(surfaceByteCount ByteCount) ByteCount {
		return MemoryTargetScaledByteCount(surfaceByteCount, mib(3), kib(384))
	}

	belowReference := []ByteCount{mib(8), mib(16), mib(32), mib(64)}
	if _, _, _, _, failed := shareTableScalingFailure(idiom, belowReference); failed {
		t.Error(
			"the memory-scaled idiom failed the scaling predicate below the reference, where it is proportional and indistinguishable from a draw; a predicate that rejects it there is rejecting the arithmetic rather than the defect, and the negative no longer shows what it is meant to",
		)
	}

	acrossReference := []ByteCount{mib(64), mib(128), mib(256), mib(1024)}
	_, currentSurface, previous, current, failed :=
		shareTableScalingFailure(idiom, acrossReference)
	if !failed {
		t.Error(
			"the memory-scaled idiom passed the scaling predicate above the reference. The scale saturates at one, so the constant is flat there while a draw doubles: if this passes, the constraint cannot tell a draw from the constant it replaced, and every row in the table is unguarded",
		)
	} else {
		t.Logf(
			"the idiom goes flat as designed: %d at the step to %d, against %d before it",
			current, currentSurface, previous,
		)
	}

	// and the shipping row it was replaced by passes the same predicate over
	// the same ladder, so the difference is the row and not the ladder
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })
	for _, row := range shareTableRows() {
		if row.name != "H3 stream window" {
			continue
		}
		if _, _, _, _, failed := shareTableScalingFailure(row.resolve, acrossReference); failed {
			t.Error("the shipping H3 stream window failed the predicate the idiom is meant to fail alone")
		}
	}
}
