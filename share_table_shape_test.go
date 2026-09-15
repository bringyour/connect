package connect

import (
	"testing"
	"time"
)

// THROUGHPUTFIX §44.4: the rows that make a wrong table fail rather than a
// wrong row. §44.2's three constraints hold a row to its own fraction; these
// hold the fractions to each other, to the loops they are derived from, and to
// the plateau the record commits to.
//
// All three are arithmetic over the table and the tree's own constants. Nothing
// here runs a carrier, a stack or a fixture, so there is no sleep, no clock and
// no scheduling to race: the claim is a relationship among numbers a campaign
// then measures, and a row that needed a measurement to fail could not fail in
// CI at all.

// shareTableSeriesRow is one of the three windows in series on the download
// path, with the loop whose round trip sets its need.
type shareTableSeriesRow struct {
	name string
	row  shareTableRow
}

// the three windows in series on a download, innermost last (§37.23's loops D,
// C and B). Their surfaces differ, so a shape comparison has to name the value
// of each; see the tolerance argument in the shape row.
func shareTableSeriesRows(t *testing.T) []shareTableSeriesRow {
	t.Helper()
	byName := map[string]shareTableRow{}
	for _, row := range shareTableRows() {
		byName[row.name] = row
	}
	series := []shareTableSeriesRow{}
	for _, name := range []string{
		"H3 stream window",
		"transfer receive hold",
		"tun receive maximum",
	} {
		row, ok := byName[name]
		if !ok {
			t.Fatalf("the share table no longer has a %s row", name)
		}
		series = append(series, shareTableSeriesRow{name: name, row: row})
	}
	return series
}

// needByteCount is what §44.3 derives a layer's need as: the target throughput
// times its own loop's round trip, divided by the goodput factor where the
// layer counts framed bytes.
func shareTableNeedByteCount(row shareTableRow) float64 {
	need := float64(targetGoodputByteRate) *
		(float64(row.loopRoundTrip) / float64(time.Second))
	if row.framed {
		need /= goodputFactor
	}
	return need
}

// goodputByteRate is what a window of this many bytes permits over its own
// loop, in goodput bytes per second: the binder arithmetic of §37.23, §42 and
// §43, stated once.
func shareTableGoodputByteRate(row shareTableRow, windowByteCount ByteCount) float64 {
	rate := float64(windowByteCount) / (float64(row.loopRoundTrip) / float64(time.Second))
	if row.framed {
		rate *= goodputFactor
	}
	return rate
}

// The tolerance the shape constraint is stated at. §44.4 asks for the rows'
// ratios "within a tolerance"; this is that number, chosen rather than fitted,
// and the row's comment says what it admits and what it rejects.
const shareTableShapeTolerance = 1.6

// §44.4's shape row: the three windows in series stand in the ratio their
// loops' needs imply, so that a change to one row without the others fails.
//
// The loops, and the correction this row carries. §37.23 puts them at
// `P/2 : P + 5 : P + 10` — the carrier hop, the transfer sequence, the inner
// TCP — and §44.3 reads 1 : 2 : 2 off that and derives an H3 reservation at
// M/16 against transfer and tun rows at M/8. The P/2 is the even-hop case. Every
// figure §42, §43 and §48 compute is for the delay on the client's hop, where
// one hop carries the whole path and `rtt_D` is P, and then the three loops are
// 200, 205 and 210 ms and the needs are within five per cent of each other
// rather than in a 1 : 2 : 2 ratio. Both placements are in §37.23; the landings
// are all computed under one of them, and this row is stated under that one. A
// table built for the even split would halve the carrier row and is a different
// table, not this one with a looser tolerance.
//
// What the tolerance admits. At equal surfaces the three rows are 6/64, 1/8 and
// 1/8, so against needs of 1 : 1.025 : 0.887 the worst ratio-of-ratios is 1.50,
// the tun against the H3 stream. Before §43.2 the H3 stream window was 3/64 and
// that figure was 3.01, so this row fails on the tree as it stood at the start
// of this branch and passes on it now. The remaining half is structural rather
// than a fraction left unswept: the stream window cannot exceed the connection
// window, which is now the whole reservation, so proportionality to need is
// unreachable while the reservation is an eighth. Closing it is the other half
// of §43.2's sentence, a larger H3 draw, and it is a table change with its own
// backing arithmetic rather than a window change.
//
// The surfaces differ and the row says so rather than hiding it. It compares at
// equal surface values, which is the comparison §44.3 makes. Under the chain of
// §48.4, where the device target is a slice of the process budget, the carrier
// rows are additionally multiplied by that slice — 20/34 at the sdk's own
// reference split — and the H3 stream window falls to 0.055 of M against the
// tun's 0.125, a ratio-of-ratios of 2.55. So closing the chain makes the shape
// worse, and the H3 fraction is what has to absorb it. That is the prediction
// this row records ahead of the sdk's default-target change.
func TestTheShareTableShapeFollowsItsLoops(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	// one surface value for every row, so the comparison is of fractions and
	// not of two different budgets. Above every floor crossing in the table.
	const surface = ByteCount(256 * 1024 * 1024)

	series := shareTableSeriesRows(t)
	type shaped struct {
		name      string
		draw      ByteCount
		need      float64
		shareOfIt float64
	}
	shapes := []shaped{}
	for _, s := range series {
		draw := s.row.resolve(surface)
		shapes = append(shapes, shaped{
			name:      s.name,
			draw:      draw,
			need:      shareTableNeedByteCount(s.row),
			shareOfIt: float64(draw) / float64(surface),
		})
	}
	for _, shape := range shapes {
		t.Logf(
			"%s: %d at a %d byte surface (%.4f of it), need %.0f bytes over its loop",
			shape.name, shape.draw, surface, shape.shareOfIt, shape.need,
		)
	}

	for i := range shapes {
		for j := range shapes {
			if i == j {
				continue
			}
			// how far the drawn ratio is from the needed ratio, as a factor
			// either way
			drawnRatio := float64(shapes[i].draw) / float64(shapes[j].draw)
			neededRatio := shapes[i].need / shapes[j].need
			factor := drawnRatio / neededRatio
			if factor < 1 {
				factor = 1 / factor
			}
			if shareTableShapeTolerance < factor {
				t.Errorf(
					"%s draws %d against %s's %d, a ratio of %.3f, where their loops' needs stand at %.3f — a factor of %.2f out, past the %.2f this constraint is stated at. The rows are in series on one download, so a layer drawn out of proportion to its loop is either memory bought that its loop can never use or the binder for every other layer's raise",
					shapes[i].name, shapes[i].draw, shapes[j].name, shapes[j].draw,
					drawnRatio, neededRatio, factor, shareTableShapeTolerance,
				)
			}
		}
	}

	// The fractions inside the carrier, which §43.2 sets and which no ratio of
	// loops fixes: the stream window is three quarters of the reservation, the
	// connection window is the whole of it, and the two keep the 3:4 they have
	// always had. A change to one without the others fails here.
	target := surface
	settings := DefaultPlatformTransportSettingsWithMemoryTarget(target)
	config := newPlatformQuicConfig(settings, 1)
	reservation := settings.H3BudgetByteCount
	stream := ByteCount(config.MaxStreamReceiveWindow)
	connection := ByteCount(config.MaxConnectionReceiveWindow)
	for _, shape := range []struct {
		name                   string
		got, want              ByteCount
		numerator, denominator ByteCount
		why                    string
	}{
		{"stream window", stream, reservation * 3 / 4, 3, 4,
			"a stream window above three quarters leaves the connection window nothing to bound, and below it leaves the reservation partly unusable by the one stream the carrier runs"},
		{"connection window", connection, reservation, 1, 1,
			"the connection window at the whole reservation is what makes the reservation occupiable; at less, the difference is memory claimed against the aggregate that no connection can hold"},
	} {
		if shape.got != shape.want {
			t.Errorf(
				"the %s is %d against a reservation of %d, which is not %d/%d of it; %s",
				shape.name, shape.got, reservation,
				shape.numerator, shape.denominator, shape.why,
			)
		}
	}
	if connection*3 != stream*4 {
		t.Errorf(
			"the stream window is %d and the connection window %d, which is not the 3:4 the carrier has always had; the connection window bounds the stream windows sharing it, so moving one alone makes one of them unreachable",
			stream, connection,
		)
	}
}

// One pair of surfaces as a host sets them: the per-device memory target T and
// the process budget M. §52 is the decision this type encodes — T is a constant
// set beside M rather than derived from it, so the pair is the unit a host
// chooses and the two constraints below are checks on the pair rather than a
// formula that produces one number from the other.
type shareTableMemoryPair struct {
	name string
	// T, passed to the *WithMemoryTarget constructors
	deviceTarget ByteCount
	// M, set through sdk.SetMemoryLimit and read here as the process budget
	processBudget ByteCount
	// the record's figure for the binding row at this target, in Mb/s of
	// goodput (§51.3's table)
	recordedMbps float64
	// why this pair violates a constraint deliberately, empty for a pair that
	// has to satisfy both. A declared exception is a kill-limit-bound host
	// (§52.4), not a pair someone rounded.
	exception string
}

// §52.2's backing constraint: the device targets plus the message pools fit
// inside the process budget. The pools take 14 of M's 34 parts, so the targets
// have 20, and on a single-device host T is at most 20/34 of M.
func shareTableBackingConstraintHolds(pair shareTableMemoryPair) bool {
	return pair.deviceTarget <=
		pair.processBudget*sdkProcessRatioDeviceTarget/sdkProcessRatioParts
}

// §52.2's collector constraint: the process budget is at least three times the
// device target, because the live heap a device holds amplifies roughly
// threefold at the Go runtime — what the target permits, the garbage the same
// path made and the collector has not swept, and the copies between them — and
// a target close to its process's soft limit collects continuously rather than
// failing. This is the constraint that dominates: it admits T at a third of M
// where the backing constraint admits 20/34 of it.
const shareTableCollectorMultiple = 3

func shareTableCollectorConstraintHolds(pair shareTableMemoryPair) bool {
	return pair.deviceTarget*shareTableCollectorMultiple <= pair.processBudget
}

// The pairs: what ships and what §52.3 proposes. A pair added here that
// violates either constraint without a reason in `exception` fails the binder
// row, which is the point of holding them in one place — a bad pair fails at
// test time rather than in a phone's memory graph.
func shareTableMemoryPairs() []shareTableMemoryPair {
	return []shareTableMemoryPair{
		{
			name:          "iOS, a 20 MiB target in the extension's 32 MiB budget",
			deviceTarget:  mib(20),
			processBudget: mib(32),
			// §51.3: a 1.875 MiB stream window at a 20 MiB target
			recordedMbps: 66,
			exception:    "the packet tunnel provider is killed above 50 MiB and the Go runtime takes about 16 of it (§48.1), so neither number is a choice; the phone pays the continuous collection the collector constraint names and §48.6 step 0a has no memory to give it",
		},
		{
			name:          "Android, a 24 MiB target in a 32 MiB budget",
			deviceTarget:  mib(24),
			processBudget: mib(32),
			// §51.3: a 2.25 MiB stream window at a 24 MiB target
			recordedMbps: 80,
			exception:    "Android mirrors the iOS budget by decision rather than by platform limit (§48.6 step 0b), and sits further outside both bounds than iOS does; a raise is the product decision that step names, not a change to this table",
		},
		{
			name:          "the desktop's first step, 128 MiB in 384",
			deviceTarget:  mib(128),
			processBudget: mib(384),
			// §51.3: a 12 MiB stream window at a 128 MiB target
			recordedMbps: 425,
		},
		{
			name:          "the desktop's second step, 256 MiB in 768",
			deviceTarget:  mib(256),
			processBudget: mib(768),
			// §43.2's 830, which §51.3 computes as 851 before the record's
			// rounding of MiB to MB
			recordedMbps: 830,
		},
	}
}

// §44.4's binder row, under §52's decision: for a pair of surfaces a host
// chooses and a given path, the expected plateau is the smallest row's product
// over its own loop, and the row that produces it is the one a campaign will
// measure.
//
// This is the acceptance arm made deterministic. The cell it stands in for runs
// a real carrier and a real stack and reads a plateau; what can be asserted
// without running anything is the arithmetic that predicts which row that
// plateau belongs to and what it is, and that arithmetic is where the record's
// 425 and 830 come from. A campaign that reads a plateau materially different
// from this row's figure has found either a layer the table does not list or a
// constant this table does not govern, and either is the finding.
//
// WHAT CHANGED, because the row's shape follows a decision rather than a
// measurement. It used to take the two surfaces as free numbers, assert the
// binder with the device target at the whole of the process budget, and log
// what §48.4's two candidate derivations of T from M would produce — the 500
// Mb/s that a 20/34 slice of a 256 MiB budget gives. §52 withdraws the
// derivation: T is a constant beside M, a host sets both, and the two numbers
// are related by constraints to check rather than by a formula. So the row
// takes the pairs a host actually sets, asserts the constraints on each, and
// computes the plateau at the pair. No derived target is computed anywhere, and
// 500 belongs to no pair.
//
// The scenario, stated because every figure depends on it: 200 ms of path with
// the delay on the client's hop, download, the H3 carrier, every layer below
// the carrier ours (the hosted, simulated and probe shapes — on a native
// desktop the inner TCP layer is the operating system's and is not this
// table's, §48.7), and the provider's send window large enough not to bind,
// which today means a provider that sets a process budget at all (§48.1, and
// §48.6's step 0d).
//
// It fails on the tree as it stood at the start of this branch: the binder is
// the same row, and its figure is half.
func TestTheShareTableBinderIsTheH3StreamWindow(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	series := shareTableSeriesRows(t)
	for _, scenario := range shareTableMemoryPairs() {
		// §52.2's two constraints on the pair, before its plateau is worth
		// computing. A pair that fails them produces the windows below and then
		// spends the path's CPU on the collector, which is why the constraint is
		// checked here rather than left to a campaign: nothing in the table is
		// visibly violated when it is the collector that binds.
		backing := shareTableBackingConstraintHolds(scenario)
		collector := shareTableCollectorConstraintHolds(scenario)
		t.Logf(
			"%s: target %d in budget %d — backing (T ≤ %d/%d M) %t, collector (%d T ≤ M) %t%s",
			scenario.name, scenario.deviceTarget, scenario.processBudget,
			ByteCount(sdkProcessRatioDeviceTarget), ByteCount(sdkProcessRatioParts),
			backing, ByteCount(shareTableCollectorMultiple), collector,
			map[bool]string{true: "", false: " (declared exception)"}[scenario.exception == ""],
		)
		if scenario.exception == "" {
			if !backing {
				t.Errorf(
					"%s: the device target %d is above %d/%d of the %d byte process budget, which is %d. The device targets and the message pools share M and the pools take %d of its %d parts, so a target above the remainder is memory promised twice (§52.2)",
					scenario.name, scenario.deviceTarget,
					ByteCount(sdkProcessRatioDeviceTarget), ByteCount(sdkProcessRatioParts),
					scenario.processBudget,
					scenario.processBudget*sdkProcessRatioDeviceTarget/sdkProcessRatioParts,
					ByteCount(sdkProcessRatioPacketPool+sdkProcessRatioLargeObjectPool),
					ByteCount(sdkProcessRatioParts),
				)
			}
			if !collector {
				t.Errorf(
					"%s: the %d byte process budget is below %d times the %d byte device target, %d. The live heap amplifies about threefold at the runtime, so a target this close to the process's soft limit collects continuously and reads as a plateau below every window in this table rather than as a memory failure — declare the pair as an exception with its reason, or lower the target (§52.2, §52.4)",
					scenario.name, scenario.processBudget,
					ByteCount(shareTableCollectorMultiple), scenario.deviceTarget,
					scenario.deviceTarget*shareTableCollectorMultiple,
				)
			}
		} else if backing && collector {
			t.Errorf(
				"%s: the pair is declared an exception (%s) but now satisfies both constraints. A stale declaration exempts a pair that no longer needs exempting, and the next pair to drift past a bound would inherit the exemption silently; remove the declaration",
				scenario.name, scenario.exception,
			)
		}

		binderName := ""
		binderRate := 0.0
		binderWindow := ByteCount(0)
		for _, s := range series {
			surface := scenario.processBudget
			if s.row.surface == shareSurfaceDevice {
				surface = scenario.deviceTarget
			}
			window := s.row.resolve(surface)
			rate := shareTableGoodputByteRate(s.row, window)
			t.Logf(
				"%s: %s permits %.1f Mb/s (%d bytes over %s)",
				scenario.name, s.name, rate*8/1e6, window, s.row.loopRoundTrip,
			)
			if binderName == "" || rate < binderRate {
				binderName, binderRate, binderWindow = s.name, rate, window
			}
		}

		if binderName != "H3 stream window" {
			t.Errorf(
				"%s: the binding row is the %s at %.1f Mb/s rather than the H3 stream window. Every landing figure in §43 and §48 names the carrier's stream window as the binder at this point, and a different binder means the reach arithmetic is computed against the wrong row",
				scenario.name, binderName, binderRate*8/1e6,
			)
		}
		gotMbps := binderRate * 8 / 1e6
		if gotMbps < 0.9*scenario.recordedMbps || 1.1*scenario.recordedMbps < gotMbps {
			t.Errorf(
				"%s: the binding row permits %.1f Mb/s against the %.1f the record commits to, outside the tenth this row allows for the record's own rounding of MiB to MB. The plateau a campaign reads is this number, so a table that moves it without the record moving is a table whose figures no longer describe it",
				scenario.name, gotMbps, scenario.recordedMbps,
			)
		}

		// and the landing's own claim, which is a ratio rather than a rate:
		// §43.2 doubles the binder by taking the stream window from three
		// eighths of the reservation to six. Computed from the shipping
		// reservation so it cannot drift from the fractions.
		settings := DefaultPlatformTransportSettingsWithMemoryTarget(scenario.deviceTarget)
		reservationDraw := scenario.deviceTarget / h3BudgetShareDivisor
		beforeTheLanding := reservationDraw * 3 / h3ReceiveWindowShareDenominator
		if binderWindow != 2*beforeTheLanding {
			t.Errorf(
				"%s: the binding window is %d against the %d the preceding fractions gave, %.2f times rather than twice. §43.2's worth is exactly that doubling, so a fraction swept without the record fails here rather than in a campaign's plateau",
				scenario.name, binderWindow, beforeTheLanding,
				float64(binderWindow)/float64(max(beforeTheLanding, 1)),
			)
		}
		// the reservation is its draw wherever the draw clears the 3 MiB
		// admission floor, which the phones' targets do not: at 20 MiB the
		// eighth is 2.5 MiB and the reservation reads its floor, while the
		// windows are fractions of the draw itself and keep their own floors
		// (§51.1). So the figures above are the draw's at every pair, and this
		// holds the reservation to it only where the floor is not what it reads.
		if reservationDraw >= mib(3) && settings.H3BudgetByteCount != reservationDraw {
			t.Errorf(
				"%s: the reservation is %d rather than the %d its eighth gives; the binder arithmetic above is computed from the draw, and a floored reservation here would mean the figures belong to a different budget",
				scenario.name, settings.H3BudgetByteCount, reservationDraw,
			)
		}
	}
}

// The hosted proxy's tun rows, which §44.2's backing constraint cannot be
// written for, and the honest test that remains.
//
// Why it cannot. Every other row in the table is backed by something that
// counts: the transfer rows by a `TransferMemoryBudget`, the carriers by a
// `PlatformTransportBudget`, the dns by a `MemoryTarget`, each with an
// admission call that refuses or waits when the total is gone. gVisor's TCP
// buffers have no such object anywhere in the tree, and the range option that
// sets them is per stack with no shared accounting. The hosted proxy builds one
// private stack per client (`server/proxy/proxy_device.go`), `MaxClients` is
// 65,536 per process, TCP flows per client are uncapped, the device manager has
// a count gauge and no aggregate budget, and the service declares no container
// memory limit. So hosted tun memory is clients × flows × (receive + send) with
// no term bounded by the process, and a row asserting the sum against a budget
// would be asserting against a budget that does not exist (§48.5).
//
// What is asserted instead, and why this is the useful row rather than a
// comment. The exposure is a product of three numbers; two of them are the
// proxy's and one is this table's. This row pins the one this tree owns: the
// per-connection permission at the proxy's actual configuration, which sets no
// process budget at all, so §43.1's draw resolves to the unscaled constant and
// each connection may hold 8 MiB across the two directions. It fails the moment
// that number grows — including the day someone gives the proxy a process
// budget without first giving it an aggregate, which is exactly the change
// §48.5 says must be sequenced, and which would otherwise land silently as a
// raise on a per-connection ceiling multiplied by tenant and by flow.
//
// A guard, deliberately and explicitly: the tree is at the recorded value
// today, so this passes before and after. The number it holds is a ceiling on
// a product with no other bound, and the interim §48.5 accepted — a 64 MiB
// process budget on the proxy, doubling the permission to 16 MiB per connection
// — is recorded here as the next value and its arithmetic logged, so that the
// step is taken deliberately rather than by raising the desktop's figure
// everywhere.
func TestTheHostedTunPermissionHasNoAggregateBehindIt(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	// `server/proxy/server.go`, mirrored with its provenance: the per-process
	// client cap, which is the only term bounding the product today
	const proxyMaxClients = 65536
	// `server/proxy/proxy_device.go`: the per-device target the proxy sets,
	// which sizes every DeviceLocal-owned row and does not reach the tun
	const proxyDeviceTarget = ByteCount(24 * 1024 * 1024)

	for _, c := range []struct {
		name          string
		processBudget ByteCount
		perDirection  ByteCount
	}{
		{
			name:          "the proxy as it is configured, with no process budget",
			processBudget: 0,
			perDirection:  mib(4),
		},
		{
			name:          "§48.5's interim, the smallest value that is strictly a raise",
			processBudget: mib(64),
			perDirection:  mib(8),
		},
	} {
		SetMemoryBudget(c.processBudget)
		settings := DefaultTunSettings()
		receive := ByteCount(settings.TcpReceiveBuffer.Max)
		send := ByteCount(settings.TcpSendBuffer.Max)
		perConnection := receive + send
		if receive != c.perDirection || send != c.perDirection {
			t.Errorf(
				"%s: one gVisor stack permits receive %d and send %d rather than %d each. This number is multiplied by flows and by up to %d clients with nothing in the tree bounding the product, so it may not move until an aggregate exists to move it against",
				c.name, receive, send, c.perDirection, proxyMaxClients,
			)
		}
		t.Logf(
			"%s: %d per connection, %d per client at one flow, %d at %d clients; the device target beside it is %d and carries no tun share",
			c.name, perConnection, perConnection,
			perConnection*proxyMaxClients, proxyMaxClients, proxyDeviceTarget,
		)
	}

	// The device target the proxy does set allocates every one of its twenty
	// parts (dns 2 : client 9 : carriers 5 : provider 4) and none of them to the
	// tun, so a hosted client's two gVisor buffers are unaccounted heap beside a
	// fully subscribed target. Logged as the arithmetic of the gap rather than
	// asserted, because the fix is a share in the sdk's split or an aggregate in
	// this tree, and neither is a number this row could hold.
	SetMemoryBudget(0)
	unbudgeted := DefaultTunSettings()
	unaccounted := ByteCount(unbudgeted.TcpReceiveBuffer.Max + unbudgeted.TcpSendBuffer.Max)
	t.Logf(
		"a hosted client's tun permits %d beside a %d byte device target whose %d parts are fully allocated: %.2f of the target, unaccounted",
		unaccounted, proxyDeviceTarget, ByteCount(sdkDeviceRatioParts),
		float64(unaccounted)/float64(proxyDeviceTarget),
	)
}
