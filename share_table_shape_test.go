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

// §44.4's binder row: for a given budget and path, the expected plateau is the
// smallest row's product over its own loop, and the row that produces it is the
// one a campaign will measure.
//
// This is the acceptance arm made deterministic. The cell it stands in for runs
// a real carrier and a real stack and reads a plateau; what can be asserted
// without running anything is the arithmetic that predicts which row that
// plateau belongs to and what it is, and that arithmetic is where the record's
// 415 and 830 come from. A campaign that reads a plateau materially different
// from this row's figure has found either a layer the table does not list or a
// constant this table does not govern, and either is the finding.
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
	for _, scenario := range []struct {
		name string
		// the two surfaces, separately, because the chain does not yet make one
		// from the other
		processBudget ByteCount
		deviceTarget  ByteCount
		// the record's figure for the binding row, in Mb/s of goodput
		recordedMbps float64
	}{
		{
			name:          "a 256 MiB budget with the device target at the whole of it",
			processBudget: mib(256),
			deviceTarget:  mib(256),
			// §43.2: 830 Mb/s at 200 ms, against 415 at the fractions this
			// branch replaced
			recordedMbps: 830,
		},
		{
			name:          "the 64 MiB reference with the device target at the whole of it",
			processBudget: mib(64),
			deviceTarget:  mib(64),
			// §48.4's 109 Mb/s is the 3 MiB stream window; §43.2 doubles the
			// fraction, so the reference reads twice that
			recordedMbps: 218,
		},
	} {
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
		if settings.H3BudgetByteCount != reservationDraw {
			t.Errorf(
				"%s: the reservation is %d rather than the %d its eighth gives; the binder arithmetic above is computed from the draw, and a floored reservation here would mean the figures belong to a different budget",
				scenario.name, settings.H3BudgetByteCount, reservationDraw,
			)
		}
	}

	// The same arithmetic under the chain of §48.4, where the device target is a
	// slice of the process budget rather than a constant beside it. Logged
	// rather than asserted, because which slice it is has not been decided —
	// §48.4 leaves T = M and T = M less the pools' 14/34 open, and the two give
	// materially different plateaus. This is the number that decision produces,
	// recorded so the decision is made against it.
	for _, slice := range []struct {
		name                   string
		numerator, denominator ByteCount
	}{
		{"the whole process budget", 1, 1},
		{"the process budget less the pools", sdkProcessRatioDeviceTarget, sdkProcessRatioParts},
	} {
		processBudget := mib(256)
		target := processBudget * slice.numerator / slice.denominator
		settings := DefaultPlatformTransportSettingsWithMemoryTarget(target)
		stream := ByteCount(newPlatformQuicConfig(settings, 1).MaxStreamReceiveWindow)
		rate := float64(stream) * goodputFactor /
			(float64(shareTableCarrierLoopRtt) / float64(time.Second))
		t.Logf(
			"a %d byte process budget with the device target at %s (%d): stream window %d, %.1f Mb/s at 200 ms",
			processBudget, slice.name, target, stream, rate*8/1e6,
		)
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
