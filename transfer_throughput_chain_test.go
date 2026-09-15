package connect

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// The end-to-end measurement the throughput program does not have, and the
// instrument the campaign's open values are to be swept on.
//
// Every rate in THROUGHPUTFIX is either a constant read at a line multiplied by
// a round trip, or a single in-process reading of one arm. §47.9 separates the
// two lists and the derived list is the long one. Two cells here close part of
// that: `TestTheChainAtTheDesignPoint` measures what the window rule is worth at
// the round trips it is designed for, as a ratio between two arms run on the
// same host in the same process, interleaved, repeated and reported against the
// instrument's own measured ceiling; `TestTheShareTableShape` computes which row
// of the share table binds at every setting of the two budget surfaces, which
// costs nothing to run and is what makes a divisor sweep read as a shape rather
// than as a search (§44.3).
//
// Both are parameters of a run rather than a recompile. Every knob below is an
// environment variable, and `TestTheSweepSeams` prints, for each value the
// campaign has left open, whether this fixture can reach it and what harness
// does if it cannot. That last cell is the important one for a reader who wants
// a number this file does not produce: three of the four open values are not in
// this fixture and no amount of running it will settle them.
//
// # The instrument rule (§50.4), which is the shape of this whole file
//
// Two instruments in this program hit their own ceilings on the same day and
// both readings were first written down as properties of the tree. The
// in-process fixture's was the larger: a believed ~50 Mb/s byte ceiling that no
// window could move turned out to be a frame ceiling, about 7,800 frames a
// second whatever the payload, which is this fixture's goroutine-per-frame delay
// pump and not a stage in anything. Doubling two buffer depths moved 16 KiB
// payloads from 509 to 1,313 Mb/s.
//
// So the rule: a throughput assertion is meaningful only below the instrument's
// own ceiling, and the ceiling is measured before the assertion is written, by
// running the instrument with the constraint under test removed and the window
// seeded past any bound.
//
// This cell measures its ceiling in the same process and the same buffer
// configuration, at every payload it reports, once per repetition. Every rate it
// prints carries the headroom between that rate and that ceiling, and a rate
// within `censorFractionOfCeiling` of it prints as CENSORED rather than as a
// result. Nothing here is compared against 509 or 1,313: those are another
// machine's numbers and the point of the rule is that they do not travel.
//
// # What the fixture contains, and what it does not
//
// It contains exactly one row of the share table: the transfer send window and
// the receive hold, which are the two rows that read the process budget M
// through `transferBudgetShareByteCount` (§51.2). A sender `Client` and a
// receiver `Client` joined by four Go channels, with a goroutine-per-frame pump
// imposing the delay on the acknowledgement half. That covers the send window,
// the resend queue, the receive hold, the acknowledgement compression timer, the
// round-trip estimator and the rule that sizes the window from delivery.
//
// It contains no kernel, no socket, no QUIC, no gVisor tun, no NAT and no
// contract manager — the harness peers are `NewNoContractClientOob`. So it
// cannot see, and no reading here may be read as having measured:
//
//   - the H3 reservation, stream window or connection window. There is no
//     quic-go in this fixture at all, so the stream numerator that §51.1 just
//     moved from 3/8 to 6/8 — the largest single lever in the table — is
//     invisible here.
//   - the tun send and receive maxima (§51.1, hosted and simulated modes only).
//   - the steady acknowledgement cadence k, which is a `TcpBufferSettings` field
//     on the provider's NAT TCP path (`ip.go:457`).
//   - the contract announce threshold, which needs contracts.
//   - any operating-system socket or TCP ceiling. §47.8's first unknown, that no
//     cell in this record describes a native desktop, is untouched by this one.
//
// # The two budget surfaces are independent inputs
//
// M, the process budget of `SetMemoryBudget`, and T, the per-device target, are
// separate numbers and are swept separately here (§51.2, and the decision that T
// is a constant beside M rather than a slice of it). The fixture drives M, which
// is the surface its one row reads. It records T and uses it in the shape table,
// where the carrier rows are computed rather than measured — and says so in the
// same row, so that a T sweep is never mistaken for a T measurement.
//
// # Gate
//
// `CONNECT_THROUGHPUT_MEASURE` must be set, matching the convention of
// `CONNECT_WEBRTC_WINDOW_MEASURE` and its siblings. The measurement takes
// minutes and asserts nothing about absolute rates, so it has no business in an
// ordinary suite; `testing.Short` is not the gate, because a run that is merely
// not short should still not spend twenty minutes here. `go test` buffers a
// passing package's output, so `-v` is required to see any of it.
//
//	CONNECT_THROUGHPUT_MEASURE=1 go test -run TestTheChainAtTheDesignPoint \
//	    -v -count=1 -timeout 3600s .
//
//	# the shape at a setting, which needs no measurement and returns at once
//	CONNECT_THROUGHPUT_MEASURE=1 CONNECT_THROUGHPUT_TARGETS=20,24,64,256 \
//	    go test -run 'TestTheShareTableShape|TestTheSweepSeams' -v -count=1 .
//
// Knobs, all optional, all comma-separated lists where plural:
//
//	CONNECT_THROUGHPUT_REPETITIONS        repetitions            (7)
//	CONNECT_THROUGHPUT_OFFER_SECONDS      seconds per arm        (6)
//	CONNECT_THROUGHPUT_CEILING_SECONDS    seconds per ceiling    (3)
//	CONNECT_THROUGHPUT_BUDGETS            M, MiB                 (20,24,32,64,128,256)
//	CONNECT_THROUGHPUT_TARGETS            T, MiB                 (20,24,32,64,128,256)
//	CONNECT_THROUGHPUT_PAYLOADS           KiB                    (1,4,16)
//	CONNECT_THROUGHPUT_ROUND_TRIPS        ms                     (200,400)
//	CONNECT_THROUGHPUT_TRANSFER_DIVISORS  the transfer row's f   (production)
//	CONNECT_THROUGHPUT_WINDOW_SCALES      the rule's k           (production)
//	CONNECT_THROUGHPUT_ACK_COMPRESS_MS    the transfer timer     (production)
//
// The last three are the sweeps. Each adds one arm per value, at the headline
// budget and at 16 KiB, so a divisor sweep is one environment variable and not a
// rebuild.
//
// What it costs, stated because the defaults are not cheap and a reader should
// choose rather than discover: the default grid is 22 arms and 3 ceiling cells
// per repetition, and a cell costs its offer window plus about two seconds of
// construction, pool settling and teardown. At the defaults that is about fifty
// minutes. Narrow `CONNECT_THROUGHPUT_ROUND_TRIPS` to one value, or
// `CONNECT_THROUGHPUT_PAYLOADS` to `16`, when the question does not need the
// whole grid: halving the grid halves the time and costs nothing in resolution,
// which is set by the repetition count alone.
func TestTheChainAtTheDesignPoint(t *testing.T) {
	requireTheMeasureGate(t)

	repetitions := envInt(t, "CONNECT_THROUGHPUT_REPETITIONS", defaultThroughputRepetitions)
	offerWindow := envSeconds(t, "CONNECT_THROUGHPUT_OFFER_SECONDS", defaultThroughputOfferSeconds)
	ceilingWindow := envSeconds(t, "CONNECT_THROUGHPUT_CEILING_SECONDS", defaultThroughputCeilingSeconds)

	// One ceiling per payload is only sound if the quantities that set the
	// ceiling do not move with the budget. They are plain constants today —
	// `defaultTransferBufferSize`, and the sequence buffers' `kib(256)` — but
	// the whole subject of this program is constants that were quietly
	// memory-scaled, so this is checked rather than assumed. If it ever fails,
	// the ceiling has to be measured per budget and this cell is wrong until it
	// is.
	assertTheCeilingDoesNotMoveWithTheBudget(t)

	arms := chainArms(t)
	payloads := envByteCounts(t, "CONNECT_THROUGHPUT_PAYLOADS", kib(1), []ByteCount{
		kib(1), kib(4), kib(16),
	})

	readings := make([]map[string]chainReading, repetitions)
	ceilings := make([]map[ByteCount]float64, repetitions)

	startLoad := hostLoadAverage()
	fmt.Printf(
		"\n# the chain at the design point\n"+
			"# %d repetitions, %s per arm with the rate taken over the second half,\n"+
			"# %s per ceiling cell, %d arms, %d cores, load %s at the start\n",
		repetitions, offerWindow, ceilingWindow, len(arms), runtime.NumCPU(),
		formatLoad(startLoad),
	)

	for repetition := range repetitions {
		readings[repetition] = map[string]chainReading{}
		ceilings[repetition] = map[ByteCount]float64{}

		// The ceiling first, every repetition, so host drift shows up in the
		// instrument before it shows up in an arm. A repetition whose ceiling
		// has moved is a repetition whose absolute rates cannot be compared with
		// any other's, which is what `contaminated` decides below.
		for _, payload := range payloads {
			var rate float64
			t.Run(fmt.Sprintf("ceiling/rep%d/%s", repetition, formatBytes(payload)),
				func(t *testing.T) {
					rate = measureChainCell(t, ceilingArm(payload), ceilingWindow).rate
				})
			ceilings[repetition][payload] = rate
		}

		// Interleaved, and the order of the two window arms of a pair alternates
		// with the repetition. Running every off arm and then every on arm lets
		// an hour of host drift read as the effect under test.
		for _, arm := range interleavedArms(arms, repetition) {
			var reading chainReading
			t.Run(fmt.Sprintf("%s/rep%d", arm.key(), repetition), func(t *testing.T) {
				reading = measureChainCell(t, arm, offerWindow)
			})
			readings[repetition][arm.key()] = reading
		}
	}

	reportTheChain(t, arms, payloads, readings, ceilings, startLoad, hostLoadAverage())
}

// Which row of the share table binds, at every setting of the two surfaces.
//
// Costs nothing to run and measures nothing. It is arithmetic over the
// production row functions — `transferBudgetShareByteCount`,
// `tunBudgetShareByteCount`, `h3MaxStreamReceiveWindowByteCountForMemoryTarget`
// and their siblings — so a divisor changed in `transfer.go`, `tun.go` or
// `transport.go` changes this table without anything here being edited. That is
// the property that matters: the table cannot drift from the tree, because it
// has no copy of the tree's arithmetic in it.
//
// §44.3 asks that a sweep read as a shape rather than as a search, and names the
// reading that makes it one: which row binds at each setting. That is what the
// binder column is. A campaign moving a divisor reads this first, sees which
// settings its change can possibly affect, and measures only those.
//
// The rate arithmetic is §51.3's: the window times `goodputFactor` over the
// round trip, which is a steady-state bound and lands at 0.85 to 0.97 of the
// figure in this record's own readings (§50.3). It is a bound and not a
// prediction of what any instrument will read.
func TestTheShareTableShape(t *testing.T) {
	requireTheMeasureGate(t)

	budgets := envByteCounts(t, "CONNECT_THROUGHPUT_BUDGETS", mib(1), defaultChainBudgets())
	targets := envByteCounts(t, "CONNECT_THROUGHPUT_TARGETS", mib(1), defaultChainTargets())
	roundTrips := envDurations(t, "CONNECT_THROUGHPUT_ROUND_TRIPS", time.Millisecond,
		[]time.Duration{200 * time.Millisecond, 400 * time.Millisecond})

	restore := MemoryBudget()
	defer SetMemoryBudget(restore)

	fmt.Printf("\n# the share table, as the tree computes it\n")
	for _, roundTrip := range roundTrips {
		fmt.Printf("\n## at a %s round trip\n\n", roundTrip)
		fmt.Printf("%-9s  %-9s  %11s  %11s  %11s  %11s  %11s  %-22s  %s\n",
			"M", "T", "transfer", "H3 stream", "H3 conn", "tun (host)", "H3 resv",
			"binds", "bound rate")
		for _, budget := range budgets {
			for _, target := range targets {
				rows := chainShareTableRows(budget, target)
				binder, rate := bindingRow(rows, roundTrip)
				fmt.Printf("%-9s  %-9s  %11s  %11s  %11s  %11s  %11s  %-22s  %.0f Mb/s\n",
					formatBytes(budget), formatBytes(target),
					formatBytes(rows["transfer window"]),
					formatBytes(rows["H3 stream window"]),
					formatBytes(rows["H3 connection window"]),
					formatBytes(rows["tun maximum"]),
					formatBytes(rows["H3 reservation"]),
					binder, rate*8/1e6,
				)
			}
		}
	}
	fmt.Printf(
		"\n" +
			"transfer, tun: draws on the process budget M. H3 rows: draws on the per-device\n" +
			"target T. The two are independent inputs and are swept independently here.\n" +
			"\n" +
			"binds: the smallest row's window over the round trip, times goodputFactor; the\n" +
			"bound the layer imposes in steady state, not a reading. The H3 reservation is\n" +
			"an admission minimum rather than a per-flow window and is shown but never\n" +
			"treated as a binder. The tun row exists only on the hosted, simulated and probe\n" +
			"modes; on a native client that layer is the operating system's own maximum and\n" +
			"is not ours (§51.2), so `(host)` marks it and it binds only where it exists.\n" +
			"\n" +
			"Every number here is arithmetic over the production row functions in\n" +
			"transfer.go, tun.go and transport.go. Change a divisor there and this table\n" +
			"changes; nothing in this file carries a copy of it.\n" +
			"\n" +
			"The transfer row is consulted only under WindowSizingFromDelivery, which shipped\n" +
			"on at f8d564b and is now off by default (transfer.go init). With the rule off the transfer window is\n" +
			"MemoryScaledByteCount(mib(2), kib(256)) and caps at 2 MiB for any M at or above\n" +
			"the reference, which binds below every H3 value in the table at every T. So\n" +
			"with the rule off, raising T moves the H3 rows and then stops at the transfer\n" +
			"row, and this table promises a rate the constant policy cannot deliver.\n",
	)
}

// The four values the campaign has left open, against what this fixture can
// reach.
//
// Printed rather than asserted, because it is a statement about which harness
// settles which number, and getting it wrong is how a measurement of one layer
// gets filed as a measurement of another. Three of the four are not in this
// fixture. Each row names the seam that exists — every one of them is an
// existing public settings field, so none of these sweeps needs a production
// change — and the harness that has the layer.
func TestTheSweepSeams(t *testing.T) {
	requireTheMeasureGate(t)

	type seam struct {
		value   string
		site    string
		surface string
		reached string
		harness string
	}
	seams := []seam{
		{
			value:   "transfer share divisor (1/8)",
			site:    "transfer.go transferBudgetShareDivisor",
			surface: "SendBufferSettings.ResendQueueBudget, ReceiveBufferSettings.ReceiveQueueBudget",
			reached: "YES",
			harness: "this cell; CONNECT_THROUGHPUT_TRANSFER_DIVISORS",
		},
		{
			value:   "window rule scale k (2)",
			site:    "transfer.go deliverySizedWindowScale",
			surface: "SendBufferSettings.DeliverySizedWindowScale",
			reached: "YES",
			harness: "this cell; CONNECT_THROUGHPUT_WINDOW_SCALES",
		},
		{
			value:   "transfer ack compression (10 ms)",
			site:    "transfer.go:1034 AckCompressTimeout",
			surface: "ReceiveBufferSettings.AckCompressTimeout",
			reached: "YES",
			harness: "this cell; CONNECT_THROUGHPUT_ACK_COMPRESS_MS",
		},
		{
			value:   "steady ack cadence k (16 segments)",
			site:    "ip.go:457 SteadyAckEverySegments",
			surface: "TcpBufferSettings.SteadyAckEverySegments",
			reached: "no: this fixture has no NAT and no inner TCP",
			harness: "ip_tcp_steady_ack_cadence_test.go, which has the NAT path",
		},
		{
			value:   "H3 stream numerator (6/8)",
			site:    "transport.go h3StreamReceiveWindowShareNumerator",
			surface: "PlatformTransportSettings H3 window fields",
			reached: "no: this fixture has no quic-go carrier",
			harness: "transport_h3_window_share_test.go, and a two-endpoint H3 cell for rate",
		},
		{
			value:   "carrier and tun divisors (1/8)",
			site:    "transport.go h3BudgetShareDivisor, tun.go tunBudgetShareDivisor",
			surface: "PlatformTransportSettings, TunSettings maxima",
			reached: "no: no carrier and no gVisor tun here",
			harness: "share_table_test.go for the invariant; a hosted cell for the rate",
		},
		{
			value:   "contract announce threshold",
			site:    "not built; contract acknowledged-ahead is in flight",
			surface: "none yet",
			reached: "no: the harness peers are NewNoContractClientOob",
			harness: "a contract-bearing pair, once the threshold exists",
		},
	}

	fmt.Printf("\n# the open values, and which harness settles each\n\n")
	fmt.Printf("%-34s  %-52s  %-46s  %s\n", "value", "site", "seam (all existing public settings)", "in this fixture")
	for _, row := range seams {
		fmt.Printf("%-34s  %-52s  %-46s  %s\n", row.value, row.site, row.surface, row.reached)
	}
	fmt.Printf("\n")
	for _, row := range seams {
		if strings.HasPrefix(row.reached, "YES") {
			continue
		}
		fmt.Printf("%-34s  %s\n", row.value, row.harness)
	}
	fmt.Printf(
		"\n" +
			"No row needs a production seam added. Every one is already a settings field\n" +
			"a caller may set, which is what makes each of these a run parameter rather\n" +
			"than a recompile — in the harness that has the layer. Three of the four open\n" +
			"values are not in this fixture and no amount of running it will settle them.\n" +
			"\n" +
			"One caveat on the two divisors this fixture does reach: the constraint of\n" +
			"§44.2 is that the reciprocals sum to at most one per backing, so a divisor is\n" +
			"not free to move alone. Sweeping the transfer row here moves it against rows\n" +
			"this fixture cannot see, and share_table_test.go is what holds the sum. A\n" +
			"divisor chosen from this cell alone would satisfy the rate and not the\n" +
			"constraint.\n",
	)
}

const throughputMeasureEnv = "CONNECT_THROUGHPUT_MEASURE"

func requireTheMeasureGate(t *testing.T) {
	t.Helper()
	if os.Getenv(throughputMeasureEnv) == "" {
		t.Skipf(
			"set %s to run the throughput instrument; it reports rather than asserts, and needs -v to print",
			throughputMeasureEnv,
		)
	}
}

// Seven, not five. A calibration run in this program set an arm against itself
// and found, at five repetitions, that 15 of 17 cells called one side faster
// than the other side of the same configuration, with a mean paired difference
// of 3.1 per cent and a standard deviation of 20.3. The paired mean's standard
// error is that spread over the square root of the count, so five resolves
// nothing under about 18 per cent at two standard errors and seven nothing under
// about 15. Seven is chosen because the headline ratio this cell exists to
// produce is expected near 2.5 times, far outside that band, while the
// individual steps of a budget or divisor sweep are not, and the report says so
// per row rather than leaving a reader to assume every printed difference is
// real. The band shrinks as the square root, so twenty-eight repetitions buys
// one halving over seven.
const defaultThroughputRepetitions = 7

// Long enough that the second half is steady state. The rule climbs by doubling
// from a 320 KiB initial bet, so five or six doublings at one round trip each is
// 1.2 s at 200 ms and 2.4 s at 400; the first half of a six second offer pays
// that ramp and the second half does not. §50.1 states the same separation for
// quic-go's own ramp: a run averaged over its whole length pays the ramp as a
// fraction of the run, a steady-state reading after it pays nothing.
const defaultThroughputOfferSeconds = 6

const defaultThroughputCeilingSeconds = 3

// The margin of §50.4, which the rule leaves to the campaign. A rate above seven
// tenths of the instrument's own ceiling prints as censored rather than as a
// result: not because the number is wrong but because the instrument is part of
// it and there is no way to tell from the number alone how much. The headroom
// multiple is printed beside every rate as well, so a reader who wants a
// different margin can apply it without rerunning.
const censorFractionOfCeiling = 0.70

// A repetition whose measured ceiling is this far from the median ceiling is
// contaminated in a way the load average may not show: something else on the
// host took the cores for part of it. Its absolute rates are dropped. Its paired
// ratios are not, because a ratio taken inside one repetition divides the
// instrument out, and that distinction is what keeps a contended host from
// erasing the one result this cell exists to produce.
const ceilingDriftTolerance = 0.20

// The minimum clean repetitions a cell needs before a median is printed at all.
const minimumCleanRepetitions = 3

// The load at which a cell's reading is discarded rather than labelled. Two
// runnable threads per core is already a machine that is not measuring what it
// thinks it is; this host has been seen at 20 to 40 on ten cores, and a previous
// harness in this program refused to report medians taken at 141, which was the
// right call. Labelling without discarding would put the number in the table and
// rely on a reader noticing the flag.
func contentionRefusalLoad() float64 {
	return 2 * float64(runtime.NumCPU())
}

type chainArm struct {
	name      string
	mode      WindowSizingPolicyKind
	budget    ByteCount
	target    ByteCount
	roundTrip time.Duration
	payload   ByteCount
	// Overrides, each zero for the production value. Every one is an existing
	// public settings field rather than a seam added for this cell.
	transferDivisor int
	windowScale     int
	ackCompress     time.Duration
	// The ceiling cell: the send window seeded past any bound and the hold left
	// at the harness's own 64 MiB rather than re-derived, so that neither is the
	// binder and what is left is the frame pipeline. What it must not change is
	// anything that sets the rate of that pipeline — the send buffer's depth and
	// the sequence buffers' byte counts — because those are the instrument, and
	// the instrument has to be the one the arms run on.
	ceiling bool
}

func (self chainArm) key() string {
	return fmt.Sprintf("%s/%s/%dms", self.name, formatBytes(self.payload), self.roundTrip.Milliseconds())
}

// The ceiling arrangement: a 4 MiB send window against a 1 ms delay element,
// which permits about 32 Gb/s and cannot be the binder at any rate this fixture
// has ever reached.
//
// Not a zero delay and not a 64 MiB window, though §50.4's words are "the delay
// element at zero" and "seeded past any bound", and the departure is worth the
// paragraph because it is itself an instrument finding. The harness's data half
// spawns a goroutine per frame and those goroutines race to write into the route
// channel, so the reorder distance is whatever the window allows to be in flight
// at once. At 64 MiB and zero delay that is four thousand frames, the receive
// sequence spends the run filling gaps, and what gets measured is a retransmit
// storm: the first run of this cell read the 16 KiB ceiling as zero, and with
// the hold raised as well it read 2,476, then 1,660, then 1,912 Mb/s across
// three repetitions of the same configuration. A 4 MiB window bounds the flight
// to 256 frames at 16 KiB and the reordering with it.
//
// So the seeding rule that matters is kept and the literal one is not: the
// window over the delay must permit far more than the instrument can carry, and
// `assertTheWindowIsNotTheCeilingsBinder` checks that against what was actually
// read rather than trusting the arithmetic. This is the arrangement
// `TestTheFixturePayloadCeiling` already uses, arrived at independently.
const ceilingAckDelay = time.Millisecond

func ceilingSeedWindow() ByteCount { return mib(4) }

func ceilingArm(payload ByteCount) chainArm {
	return chainArm{
		name:      "ceiling",
		mode:      WindowSizingConstant,
		budget:    0,
		roundTrip: ceilingAckDelay,
		payload:   payload,
		ceiling:   true,
	}
}

type chainReading struct {
	// steady-state goodput, counted at the receiver's callback over the second
	// half of the offer
	rate float64
	// what the sender thought its window was at the end
	window SendWindowEstimate
	// the higher of the load averages read either side of the cell
	loadPeak float64
}

// The arms.
//
// The headline pair is the window rule off against on at the round trips the
// product is designed for (§38). `WindowSizingConstant` is the tree before
// f8d564b byte for byte, and again the shipping default since the rule was
// measured at a loss on short paths (the `init` in transfer.go);
// `WindowSizingFromDelivery` is the rule, one call away. The switch is the
// production surface rather than a test seam, which is what makes the two arms
// the same binary with one call changed.
//
// The headline runs at a 64 MiB process budget, the reference, because that is
// where `memoryTargetScale` returns one and the constant arm's window is its full
// unscaled 2 MiB. Comparing at a smaller budget would credit the rule with the
// scaling of the constant it replaces.
//
// Then the sweeps, each at 16 KiB where the frame pump binds least: the budget,
// and then whichever of the three reachable knobs the environment asks for.
func chainArms(t *testing.T) []chainArm {
	t.Helper()

	payloads := envByteCounts(t, "CONNECT_THROUGHPUT_PAYLOADS", kib(1), []ByteCount{
		kib(1), kib(4), kib(16),
	})
	roundTrips := envDurations(t, "CONNECT_THROUGHPUT_ROUND_TRIPS", time.Millisecond,
		[]time.Duration{200 * time.Millisecond, 400 * time.Millisecond})
	budgets := envByteCounts(t, "CONNECT_THROUGHPUT_BUDGETS", mib(1), defaultChainBudgets())
	divisors := envInts(t, "CONNECT_THROUGHPUT_TRANSFER_DIVISORS", nil)
	scales := envInts(t, "CONNECT_THROUGHPUT_WINDOW_SCALES", nil)
	ackCompressions := envDurations(t, "CONNECT_THROUGHPUT_ACK_COMPRESS_MS", time.Millisecond, nil)

	sweepPayload := payloads[len(payloads)-1]
	arms := []chainArm{}
	for _, roundTrip := range roundTrips {
		for _, payload := range payloads {
			arms = append(arms,
				chainArm{
					name:      "rule-off",
					mode:      WindowSizingConstant,
					budget:    headlineBudget(),
					target:    headlineBudget(),
					roundTrip: roundTrip,
					payload:   payload,
				},
				chainArm{
					name:      "rule-on",
					mode:      WindowSizingFromDelivery,
					budget:    headlineBudget(),
					target:    headlineBudget(),
					roundTrip: roundTrip,
					payload:   payload,
				},
			)
		}
		for _, budget := range budgets {
			if budget == headlineBudget() && sweepPayload == kib(16) {
				// already measured as the headline's on arm
				continue
			}
			arms = append(arms, chainArm{
				name:      fmt.Sprintf("M=%s", formatBytes(budget)),
				mode:      WindowSizingFromDelivery,
				budget:    budget,
				target:    budget,
				roundTrip: roundTrip,
				payload:   sweepPayload,
			})
		}
		for _, divisor := range divisors {
			arms = append(arms, chainArm{
				name:            fmt.Sprintf("f=1/%d", divisor),
				mode:            WindowSizingFromDelivery,
				budget:          headlineBudget(),
				target:          headlineBudget(),
				roundTrip:       roundTrip,
				payload:         sweepPayload,
				transferDivisor: divisor,
			})
		}
		for _, scale := range scales {
			arms = append(arms, chainArm{
				name:        fmt.Sprintf("k=%d", scale),
				mode:        WindowSizingFromDelivery,
				budget:      headlineBudget(),
				target:      headlineBudget(),
				roundTrip:   roundTrip,
				payload:     sweepPayload,
				windowScale: scale,
			})
		}
		for _, ackCompress := range ackCompressions {
			arms = append(arms, chainArm{
				name:        fmt.Sprintf("ack=%dms", ackCompress.Milliseconds()),
				mode:        WindowSizingFromDelivery,
				budget:      headlineBudget(),
				target:      headlineBudget(),
				roundTrip:   roundTrip,
				payload:     sweepPayload,
				ackCompress: ackCompress,
			})
		}
	}
	return arms
}

// The reference, where `memoryTargetScale` returns one.
func headlineBudget() ByteCount { return mib(64) }

// The shipped and proposed values of each surface. 20 and 24 are the shipped
// per-device targets of §48.1, 32 the iOS extension's process budget, 64 the
// macOS extension's and the reference, and 128 and 256 the raise §47.5 step 0
// proposes and has never measured.
func defaultChainBudgets() []ByteCount {
	return []ByteCount{mib(20), mib(24), mib(32), mib(64), mib(128), mib(256)}
}

func defaultChainTargets() []ByteCount {
	return []ByteCount{mib(20), mib(24), mib(32), mib(64), mib(128), mib(256)}
}

// The arms of one repetition, ordered so the two window arms of a pair are
// adjacent and their order alternates with the repetition.
func interleavedArms(arms []chainArm, repetition int) []chainArm {
	ordered := append([]chainArm{}, arms...)
	if repetition%2 == 1 {
		for i := 0; i+1 < len(ordered); i += 2 {
			ordered[i], ordered[i+1] = ordered[i+1], ordered[i]
		}
	}
	return ordered
}

// One cell: build the pair, offer for the window, and take the rate over the
// second half.
//
// The meter is the receiver's callback, so what is counted is goodput that
// arrived. The sender's `WriteByteCount` is admission and includes framing and
// resends, and using it would credit an arm for bytes the far end never saw.
//
// Pool ownership: this cell borrows nothing. It counts frame lengths inside the
// receive callback and returns immediately, and the harness owns every buffer on
// both halves.
func measureChainCell(t *testing.T, arm chainArm, offerWindow time.Duration) chainReading {
	assertMessagePoolOwnership(t)

	loadBefore := hostLoadAverage()

	// The two process surfaces, set before the harness constructs its settings
	// and restored after. Both are production calls: `SetMemoryBudget` is what
	// the sdk's `SetMemoryLimit` calls and `SetWindowSizing` is the one-call
	// rollback f8d564b was built around. Neither is a seam added for this cell.
	restoreBudget := MemoryBudget()
	restoreSizing := DefaultWindowSizing()
	defer func() {
		SetMemoryBudget(restoreBudget)
		SetWindowSizing(restoreSizing)
	}()
	SetMemoryBudget(arm.budget)
	SetWindowSizing(arm.mode)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness := newSendWindowHarnessWithClient(t, ctx, arm.roundTrip,
		func(settings *SendBufferSettings) {
			if arm.ceiling {
				settings.ResendQueueMaxByteCount = ceilingSeedWindow()
				return
			}
			if 0 < arm.windowScale {
				settings.DeliverySizedWindowScale = arm.windowScale
			}
			// The divisor sweep goes through the budget the sdk itself attaches
			// (`deviceLocalTransferBudgets`), which is the same seam and not a
			// new one: a divisor is only ever visible as the size of the budget
			// the queue draws on.
			if 0 < arm.transferDivisor {
				settings.ResendQueueBudget = NewTransferMemoryBudget(arm.budget / ByteCount(arm.transferDivisor))
			}
		},
		func(settings *ClientSettings) {
			if arm.ceiling {
				return
			}
			// The harness pins the hold at 64 MiB, which predates the budget
			// surface and would hand every arm a receiver the shipping
			// configuration does not have. Re-derived from the production
			// constructor at the budget and policy this arm runs at, rather than
			// recomputed here, so the arm is the shipping shape and this cell
			// owns none of the arithmetic.
			pristine := DefaultReceiveBufferSettings()
			settings.ReceiveBufferSettings.ReceiveQueueMaxByteCount = pristine.ReceiveQueueMaxByteCount
			settings.ReceiveBufferSettings.ReceiveQueueBudget = pristine.ReceiveQueueBudget
			settings.ReceiveBufferSettings.AdvertiseReceiveWindow = pristine.AdvertiseReceiveWindow
			if 0 < arm.transferDivisor {
				share := arm.budget / ByteCount(arm.transferDivisor)
				settings.ReceiveBufferSettings.ReceiveQueueMaxByteCount = max(
					share, settings.ReceiveBufferSettings.ReceiveQueueMinByteCount)
				settings.ReceiveBufferSettings.ReceiveQueueBudget = NewTransferMemoryBudget(share)
			}
			if 0 < arm.ackCompress {
				settings.ReceiveBufferSettings.AckCompressTimeout = arm.ackCompress
			}
		},
	)

	delivered := &atomic.Int64{}
	harness.receiver.AddReceiveCallback(
		func(_ TransferPath, frames []*protocol.Frame, _ Peer) {
			delivered.Add(int64(len(frames)) * int64(arm.payload))
		},
	)

	// The midpoint sample. Everything before it is the rule's ramp plus the
	// estimator acquiring its first round-trip samples; everything after is the
	// steady state the derived figures in this record are bounds on.
	var midByteCount int64
	var midAt time.Time
	midSampled := make(chan struct{})
	go func() {
		defer close(midSampled)
		select {
		case <-time.After(offerWindow / 2):
		case <-ctx.Done():
			return
		}
		midAt = time.Now()
		midByteCount = delivered.Load()
	}()

	harness.offer(t, int(arm.payload), offerWindow)
	endAt := time.Now()
	endByteCount := delivered.Load()
	<-midSampled

	window := harness.sender.DestinationSendStats(harness.receiverId).SendWindow

	rate := float64(0)
	if span := endAt.Sub(midAt); 0 < span && !midAt.IsZero() {
		rate = float64(endByteCount-midByteCount) / span.Seconds()
	}
	if arm.ceiling {
		assertTheWindowIsNotTheCeilingsBinder(t, rate)
	}

	loadPeak := max(loadBefore, hostLoadAverage())
	t.Logf(
		"%s: %.1f Mb/s steady, window %s (sized %v, target-bound %v, ceiling %s, rtt %s, samples %d), load %s",
		arm.key(), rate*8/1e6,
		formatBytes(window.Window), window.Sized, window.TargetBound,
		formatBytes(window.Ceiling), window.RoundTrip, window.SampleCount,
		formatLoad(loadPeak),
	)
	return chainReading{rate: rate, window: window, loadPeak: loadPeak}
}

// The ceiling cell's window must permit far more than the ceiling cell read, or
// what was read is the window and not the instrument. Checked against the
// reading rather than asserted from the arithmetic, because the effective round
// trip at a 1 ms delay element is whatever the scheduler makes it.
func assertTheWindowIsNotTheCeilingsBinder(t *testing.T, rate float64) {
	t.Helper()
	if rate <= 0 {
		t.Errorf("the ceiling cell delivered nothing, so no arm in this repetition has an instrument to be read against")
		return
	}
	permitted := float64(ceilingSeedWindow()) / ceilingAckDelay.Seconds()
	if permitted < ceilingWindowHeadroom*rate {
		t.Errorf(
			"the ceiling cell's own window permits %.0f Mb/s against the %.0f Mb/s it read, under the %gx this cell requires; the ceiling is then a window reading and every headroom below it is wrong",
			permitted*8/1e6, rate*8/1e6, ceilingWindowHeadroom,
		)
	}
}

const ceilingWindowHeadroom = 8.0

// The single ceiling per payload is only sound if the budget does not move the
// quantities that set it. Asserted rather than assumed, at the extremes of the
// sweep.
func assertTheCeilingDoesNotMoveWithTheBudget(t *testing.T) {
	t.Helper()

	restoreBudget := MemoryBudget()
	defer SetMemoryBudget(restoreBudget)

	type buffers struct {
		send            int
		sequence        int
		sequenceBytes   ByteCount
		h1SequenceBytes ByteCount
	}
	read := func(budget ByteCount) buffers {
		SetMemoryBudget(budget)
		settings := DefaultClientSettings()
		return buffers{
			send:            settings.SendBufferSize,
			sequence:        settings.ReceiveBufferSettings.SequenceBufferSize,
			sequenceBytes:   settings.ReceiveBufferSettings.SequenceBufferByteCount,
			h1SequenceBytes: settings.ReceiveBufferSettings.H1SequenceBufferByteCount,
		}
	}

	budgets := defaultChainBudgets()
	smallest := read(budgets[0])
	largest := read(budgets[len(budgets)-1])
	if smallest != largest {
		t.Fatalf(
			"the frame pipeline's buffers move with the memory budget (%+v at %s against %+v at %s), so one instrument ceiling cannot stand for the whole sweep and every cell in this table would be measuring a different instrument; measure the ceiling per budget before trusting any row",
			smallest, formatBytes(budgets[0]), largest, formatBytes(budgets[len(budgets)-1]),
		)
	}
}

// The share table's rows at one setting of the two surfaces, read from the
// production functions.
func chainShareTableRows(budget ByteCount, target ByteCount) map[string]ByteCount {
	restore := MemoryBudget()
	defer SetMemoryBudget(restore)
	SetMemoryBudget(budget)

	return map[string]ByteCount{
		"transfer window":      transferBudgetShareByteCount(),
		"tun maximum":          tunBudgetShareByteCount(),
		"H3 reservation":       h3BudgetByteCountForMemoryTarget(target),
		"H3 stream window":     h3MaxStreamReceiveWindowByteCountForMemoryTarget(target),
		"H3 connection window": h3MaxConnectionReceiveWindowByteCountForMemoryTarget(target),
	}
}

// Which row binds, and at what rate. The reservation is an admission minimum
// rather than a per-flow window and is never a binder.
func bindingRow(rows map[string]ByteCount, roundTrip time.Duration) (string, float64) {
	binder := ""
	bound := ByteCount(0)
	for _, name := range []string{
		"transfer window", "H3 stream window", "H3 connection window", "tun maximum",
	} {
		value := rows[name]
		if value <= 0 {
			continue
		}
		if binder == "" || value < bound {
			binder, bound = name, value
		}
	}
	if binder == "" {
		return "none", 0
	}
	return binder, float64(bound) * goodputFactor / roundTrip.Seconds()
}

// The report. Medians over the clean repetitions, the spread, the instrument's
// own ceiling measured in the same run, the headroom, and then the thing this
// cell exists for: the ratio between the arms.
func reportTheChain(
	t *testing.T,
	arms []chainArm,
	payloads []ByteCount,
	readings []map[string]chainReading,
	ceilings []map[ByteCount]float64,
	startLoad float64,
	endLoad float64,
) {
	t.Helper()

	repetitions := len(readings)

	medianCeiling := map[ByteCount]float64{}
	for _, payload := range payloads {
		values := []float64{}
		for repetition := range repetitions {
			values = append(values, ceilings[repetition][payload])
		}
		medianCeiling[payload] = median(values)
	}

	contaminated := make([]bool, repetitions)
	for repetition := range repetitions {
		for _, payload := range payloads {
			reference := medianCeiling[payload]
			if reference <= 0 || ceilingDriftTolerance < relativeDeviation(ceilings[repetition][payload], reference) {
				contaminated[repetition] = true
			}
		}
	}
	cleanRepetitions := 0
	for _, dirty := range contaminated {
		if !dirty {
			cleanRepetitions++
		}
	}

	fmt.Printf("\n## the instrument, measured in this run\n\n")
	fmt.Printf("%-10s  %14s  %12s  %12s  %s\n", "payload", "median", "frames/s", "spread", "per repetition")
	for _, payload := range payloads {
		values := []float64{}
		marks := []string{}
		for repetition := range repetitions {
			values = append(values, ceilings[repetition][payload])
			mark := ""
			if contaminated[repetition] {
				mark = "!"
			}
			marks = append(marks, fmt.Sprintf("%.0f%s", ceilings[repetition][payload]*8/1e6, mark))
		}
		fmt.Printf("%-10s  %14s  %12.0f  %12s  %s\n",
			formatBytes(payload),
			fmt.Sprintf("%.0f Mb/s", medianCeiling[payload]*8/1e6),
			medianCeiling[payload]/float64(payload),
			formatSpread(values),
			strings.Join(marks, " "),
		)
	}
	fmt.Printf(
		"\n"+
			"The ceiling is this fixture with the window rule off, the send window a %s\n"+
			"constant and the delay element at %s: it permits about %.0f Gb/s and cannot be\n"+
			"the binder. It is what the harness can carry, not what anything in the tree\n"+
			"can. A `!` marks a repetition whose ceiling drifted more than %.0f%% from the\n"+
			"median: its absolute rates are dropped below, its paired ratios are not, since\n"+
			"a ratio taken inside one repetition divides the instrument out. %d of %d\n"+
			"repetitions are clean.\n",
		formatBytes(ceilingSeedWindow()), ceilingAckDelay,
		float64(ceilingSeedWindow())*8/1e9/ceilingAckDelay.Seconds(),
		ceilingDriftTolerance*100, cleanRepetitions, repetitions,
	)

	fmt.Printf("\n## the arms\n\n")
	fmt.Printf("%-14s  %8s  %6s  %12s  %11s  %12s  %9s  %10s  %s\n",
		"arm", "payload", "rtt", "median", "spread", "ceiling", "headroom", "window", "flags")

	summaries := map[string]*chainSummary{}
	for _, arm := range arms {
		summary := summarize(arm, readings, contaminated, medianCeiling[arm.payload])
		summaries[arm.key()] = summary
		fmt.Printf("%-14s  %8s  %6s  %12s  %11s  %12s  %9s  %10s  %s\n",
			arm.name, formatBytes(arm.payload),
			fmt.Sprintf("%dms", arm.roundTrip.Milliseconds()),
			summary.medianText(), summary.spreadText(),
			fmt.Sprintf("%.0f Mb/s", medianCeiling[arm.payload]*8/1e6),
			summary.headroomText(), formatBytes(summary.medianWindow()),
			summary.flagsText(),
		)
	}

	fmt.Printf(
		"\n"+
			"median: the median over the clean repetitions of the goodput counted at the\n"+
			"receiver over the second half of each offer, so the rule's ramp is excluded.\n"+
			"spread: the larger of the two deviations from the median, and the n behind it.\n"+
			"headroom: the instrument's ceiling over this median. CENSORED marks a rate\n"+
			"above %.0f%% of the ceiling, which is a reading of the harness. TARGET marks an\n"+
			"arm whose window was clamped by the %d Mb/s goodput target rather than by its\n"+
			"share, and two target-clamped arms cannot show a window effect between them.\n"+
			"UNSIZED marks an arm where the rule did not engage; COLLAPSED one whose median\n"+
			"window ended at or below the %s initial bet, which is the rule sizing itself\n"+
			"from a delivery rate the host had already taken away.\n",
		censorFractionOfCeiling*100, targetGoodputByteRate*8/1e6,
		formatBytes(defaultInitialWindowByteCount()),
	)

	fmt.Printf("\n## the headline: the window rule off against on\n\n")
	fmt.Printf("%-8s  %6s  %12s  %12s  %14s  %12s  %4s  %s\n",
		"payload", "rtt", "off", "on", "ratio (paired)", "ratio range", "n", "flags")
	for _, arm := range arms {
		if arm.name != "rule-off" {
			continue
		}
		off := summaries[arm.key()]
		on := summaries[chainArm{name: "rule-on", roundTrip: arm.roundTrip, payload: arm.payload}.key()]
		if off == nil || on == nil {
			continue
		}
		ratios := pairedRatios(off, on)
		fmt.Printf("%-8s  %6s  %12s  %12s  %14s  %12s  %4d  %s\n",
			formatBytes(arm.payload), fmt.Sprintf("%dms", arm.roundTrip.Milliseconds()),
			off.medianText(), on.medianText(),
			formatRatio(median(ratios)), formatRatioRange(ratios), len(ratios),
			strings.TrimSpace(off.flagsText()+" "+on.flagsText()),
		)
	}
	fmt.Printf(
		"\n"+
			"The ratio is paired: both arms of a row run adjacent inside one repetition, in\n"+
			"an order that alternates with the repetition, so the ratio is taken within a\n"+
			"repetition and the median taken over repetitions. The range is the smallest and\n"+
			"largest of those per-repetition ratios and n is how many contributed. Absolute\n"+
			"rates on this host mean nothing; the multiple is the result.\n"+
			"\n"+
			"At %d repetitions this cell resolves a difference of roughly 15%% and no less.\n"+
			"A ratio inside 1.0 +- 0.15 is not a finding here whatever the median says, and\n"+
			"a range that straddles 1.0 is not one either.\n",
		repetitions,
	)

	fmt.Printf("\n## the sweeps, against the headline arm at the same round trip\n\n")
	fmt.Printf("%-14s  %8s  %6s  %12s  %14s  %12s  %4s  %10s  %s\n",
		"arm", "payload", "rtt", "median", "ratio (paired)", "ratio range", "n", "window", "flags")
	for _, arm := range arms {
		if arm.name == "rule-off" || arm.name == "rule-on" {
			continue
		}
		summary := summaries[arm.key()]
		reference := summaries[chainArm{
			name: "rule-on", roundTrip: arm.roundTrip, payload: arm.payload,
		}.key()]
		ratioText, rangeText, n := "-", "-", 0
		if reference != nil {
			ratios := pairedRatios(reference, summary)
			ratioText, rangeText, n = formatRatio(median(ratios)), formatRatioRange(ratios), len(ratios)
		}
		fmt.Printf("%-14s  %8s  %6s  %12s  %14s  %12s  %4d  %10s  %s\n",
			arm.name, formatBytes(arm.payload),
			fmt.Sprintf("%dms", arm.roundTrip.Milliseconds()),
			summary.medianText(), ratioText, rangeText, n,
			formatBytes(summary.medianWindow()), summary.flagsText(),
		)
	}
	fmt.Printf(
		"\n" +
			"M=x sweeps the process budget of `SetMemoryBudget`, from which each transfer\n" +
			"direction draws M/8. It is NOT the per-device target: the target sizes the H3\n" +
			"carrier rows and this fixture has no carrier in it. On a shipped client the sdk\n" +
			"attaches a target-derived budget to the device, which bypasses M/8 for these\n" +
			"two queues entirely (§48.2), so this sweep is the shape of the surface and not\n" +
			"the number a shipped phone reads.\n" +
			"\n" +
			"f=1/x sweeps the transfer row's divisor through the budget the sdk itself\n" +
			"attaches, k=x the rule's scale, ack=x the transfer layer's compression timer.\n" +
			"None of the three is ip.go's SteadyAckEverySegments, the H3 stream numerator or\n" +
			"the tun divisor: see TestTheSweepSeams for which harness reaches those.\n" +
			"\n" +
			"§44.3 asks that a sweep read as a shape rather than a search. The shape is in\n" +
			"TestTheShareTableShape, which says which row binds at each setting; this table\n" +
			"is only the one row this fixture can measure.\n",
	)

	fmt.Printf(
		"\n## host\n\n"+
			"%d cores, load %s at the start and %s at the end. A cell read above %.0f is\n"+
			"discarded; a cell with fewer than %d clean repetitions left prints REFUSED.\n",
		runtime.NumCPU(), formatLoad(startLoad), formatLoad(endLoad),
		contentionRefusalLoad(), minimumCleanRepetitions,
	)

	refused := 0
	for _, summary := range summaries {
		if !summary.reportable() {
			refused++
		}
	}
	if 0 < refused {
		fmt.Printf(
			"\n%d of %d arms are REFUSED for want of clean repetitions. Those rows are not\n"+
				"results. A ratio beside a REFUSED row may still be, since it is paired.\n",
			refused, len(summaries),
		)
	}
	fmt.Printf("\n")
}

type chainSummary struct {
	arm chainArm
	// rates from repetitions that were both load-clean and instrument-clean
	rates   []float64
	windows []ByteCount
	// per repetition, the load-clean rate or -1; the basis of every paired ratio
	byRepetition []float64
	ceiling      float64
	targetBound  bool
	unsized      bool
	loadDropped  int
}

func summarize(
	arm chainArm,
	readings []map[string]chainReading,
	contaminated []bool,
	ceiling float64,
) *chainSummary {
	summary := &chainSummary{arm: arm, ceiling: ceiling}
	summary.byRepetition = make([]float64, len(readings))
	for repetition := range readings {
		summary.byRepetition[repetition] = -1
		reading, found := readings[repetition][arm.key()]
		if !found || reading.rate <= 0 {
			continue
		}
		if 0 <= reading.loadPeak && contentionRefusalLoad() < reading.loadPeak {
			summary.loadDropped++
			continue
		}
		// Load-clean: good enough to pair, because a ratio inside a repetition
		// divides the instrument out.
		summary.byRepetition[repetition] = reading.rate
		if reading.window.TargetBound {
			summary.targetBound = true
		}
		if arm.mode == WindowSizingFromDelivery && !reading.window.Sized {
			summary.unsized = true
		}
		if contaminated[repetition] {
			continue
		}
		// Instrument-clean as well: good enough for an absolute median.
		summary.rates = append(summary.rates, reading.rate)
		summary.windows = append(summary.windows, reading.window.Window)
	}
	return summary
}

func (self *chainSummary) reportable() bool {
	return minimumCleanRepetitions <= len(self.rates)
}

func (self *chainSummary) medianRate() float64 { return median(self.rates) }

func (self *chainSummary) medianWindow() ByteCount {
	values := []float64{}
	for _, window := range self.windows {
		values = append(values, float64(window))
	}
	return ByteCount(median(values))
}

func (self *chainSummary) medianText() string {
	if !self.reportable() {
		return "REFUSED"
	}
	return fmt.Sprintf("%.0f Mb/s", self.medianRate()*8/1e6)
}

func (self *chainSummary) spreadText() string {
	if !self.reportable() {
		return "-"
	}
	return formatSpread(self.rates)
}

func (self *chainSummary) headroomText() string {
	if !self.reportable() || self.ceiling <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.2fx", self.ceiling/self.medianRate())
}

func (self *chainSummary) censored() bool {
	return self.reportable() && 0 < self.ceiling &&
		censorFractionOfCeiling*self.ceiling <= self.medianRate()
}

func (self *chainSummary) collapsed() bool {
	return self.arm.mode == WindowSizingFromDelivery && 0 < len(self.windows) &&
		self.medianWindow() <= defaultInitialWindowByteCount()
}

func (self *chainSummary) flagsText() string {
	flags := []string{}
	if !self.reportable() {
		flags = append(flags, "REFUSED")
	}
	if self.censored() {
		flags = append(flags, "CENSORED")
	}
	if self.targetBound {
		flags = append(flags, "TARGET")
	}
	if self.unsized {
		flags = append(flags, "UNSIZED")
	}
	if self.collapsed() {
		flags = append(flags, "COLLAPSED")
	}
	if 0 < self.loadDropped {
		flags = append(flags, fmt.Sprintf("load-dropped=%d", self.loadDropped))
	}
	return strings.Join(flags, " ")
}

// The per-repetition ratios of two arms, over the repetitions where both are
// load-clean. Pairing inside a repetition is the whole point of interleaving: a
// ratio of two medians taken an hour apart is a ratio of two host loads.
func pairedRatios(denominator *chainSummary, numerator *chainSummary) []float64 {
	if denominator == nil || numerator == nil {
		return nil
	}
	ratios := []float64{}
	for repetition := range denominator.byRepetition {
		below, above := denominator.byRepetition[repetition], numerator.byRepetition[repetition]
		if below <= 0 || above <= 0 {
			continue
		}
		ratios = append(ratios, above/below)
	}
	return ratios
}

func median(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64{}, values...)
	sort.Float64s(sorted)
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2
}

func relativeDeviation(value float64, reference float64) float64 {
	if reference <= 0 {
		return 1
	}
	deviation := (value - reference) / reference
	if deviation < 0 {
		return -deviation
	}
	return deviation
}

// The larger of the two deviations from the median, as a percentage. Reported
// rather than a standard deviation because with seven points the extremes are
// what a reader needs: a cell whose worst repetition is 40 per cent off its
// median has not measured a 10 per cent effect.
func formatSpread(values []float64) string {
	if len(values) == 0 {
		return "-"
	}
	middle := median(values)
	if middle <= 0 {
		return "-"
	}
	worst := float64(0)
	for _, value := range values {
		worst = max(worst, relativeDeviation(value, middle))
	}
	return fmt.Sprintf("+-%.0f%% n=%d", worst*100, len(values))
}

func formatRatio(ratio float64) string {
	if ratio <= 0 {
		return "-"
	}
	return fmt.Sprintf("%.2fx", ratio)
}

func formatRatioRange(ratios []float64) string {
	if len(ratios) == 0 {
		return "-"
	}
	sorted := append([]float64{}, ratios...)
	sort.Float64s(sorted)
	return fmt.Sprintf("%.2f-%.2f", sorted[0], sorted[len(sorted)-1])
}

func formatBytes(byteCount ByteCount) string {
	switch {
	case byteCount <= 0:
		return "0"
	case mib(1) <= byteCount && byteCount%mib(1) == 0:
		return fmt.Sprintf("%d MiB", byteCount/mib(1))
	case mib(1) <= byteCount:
		return fmt.Sprintf("%.1f MiB", float64(byteCount)/float64(mib(1)))
	case kib(1) <= byteCount:
		return fmt.Sprintf("%.0f KiB", float64(byteCount)/float64(kib(1)))
	default:
		return fmt.Sprintf("%d B", byteCount)
	}
}

func formatLoad(load float64) string {
	if load < 0 {
		return "unreadable"
	}
	return fmt.Sprintf("%.1f", load)
}

// The one-minute load average, or a negative number where it cannot be read.
//
// A shared host is the other instrument defect and it does not announce itself
// in the rate: the readings simply spread. This is read either side of every
// cell so a contaminated reading can be dropped rather than averaged into a
// median that then looks precise.
func hostLoadAverage() float64 {
	switch runtime.GOOS {
	case "linux":
		content, err := os.ReadFile("/proc/loadavg")
		if err != nil {
			return -1
		}
		return parseLoadField(strings.Fields(string(content)), 0)
	case "darwin":
		// `{ 1.23 4.56 7.89 }`
		output, err := exec.Command("sysctl", "-n", "vm.loadavg").Output()
		if err != nil {
			return -1
		}
		return parseLoadField(strings.Fields(string(output)), 1)
	}
	return -1
}

func parseLoadField(fields []string, index int) float64 {
	if len(fields) <= index {
		return -1
	}
	load, err := strconv.ParseFloat(fields[index], 64)
	if err != nil {
		return -1
	}
	return load
}

func envInt(t *testing.T, name string, fallback int) int {
	t.Helper()
	values := envInts(t, name, nil)
	if len(values) == 0 {
		return fallback
	}
	return values[0]
}

func envInts(t *testing.T, name string, fallback []int) []int {
	t.Helper()
	text := os.Getenv(name)
	if text == "" {
		return fallback
	}
	values := []int{}
	for _, field := range strings.Split(text, ",") {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		value, err := strconv.Atoi(field)
		if err != nil || value <= 0 {
			t.Fatalf("%s must be positive integers, got %q", name, text)
		}
		values = append(values, value)
	}
	return values
}

func envByteCounts(t *testing.T, name string, unit ByteCount, fallback []ByteCount) []ByteCount {
	t.Helper()
	values := envInts(t, name, nil)
	if len(values) == 0 {
		return fallback
	}
	byteCounts := []ByteCount{}
	for _, value := range values {
		byteCounts = append(byteCounts, ByteCount(value)*unit)
	}
	return byteCounts
}

func envDurations(t *testing.T, name string, unit time.Duration, fallback []time.Duration) []time.Duration {
	t.Helper()
	values := envInts(t, name, nil)
	if len(values) == 0 {
		return fallback
	}
	durations := []time.Duration{}
	for _, value := range values {
		durations = append(durations, time.Duration(value)*unit)
	}
	return durations
}

func envSeconds(t *testing.T, name string, fallback int) time.Duration {
	t.Helper()
	return time.Duration(envInt(t, name, fallback)) * time.Second
}
