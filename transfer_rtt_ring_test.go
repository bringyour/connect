package connect

import (
	"testing"
	"time"
)

// THROUGHPUT-TESTGAPS U-10, THROUGHPUTFIX §38.15. `RttEstimate.SampleCount` is
// a ring occupancy and not a population count, and the difference cost this
// program an inference: the count read 128 at both path lengths while the
// acknowledgement writes behind it differed fivefold, the sampled mean was
// compared against a whole-transfer residence measured over the population,
// and the thirty per cent that comparison produced was discarded. §38.15
// states the rule it left behind — "a count that does not move with traffic is
// a ring, and a ring's mean says nothing about what it does not hold" — and
// nothing in the tree asserts it.
//
// What this row is. It is a CHARACTERISATION and a GUARD, not a defect row,
// and the distinction matters because U-10's literal contract is that the
// estimate should report its own truncation. It does not, and adding a
// truncation field is a production change this row does not presume. What the
// row can do without one is make the two properties that mislead a reader
// impossible to change silently: the count saturates at the ring's size, and
// the mean is the mean of the last ring-full of samples rather than of
// everything written.
//
// What refutes it. A window that grows with traffic instead of evicting, a
// mean that becomes an exponentially weighted average over the population, or
// a shipped `RttWindowSize` that stops being the size the window is built
// with. Any of the three changes what every reader of `Rtt.Mean` is measuring,
// and all three are invisible in a diff of the call site.
//
// Note what it deliberately does not assert: that 128 is the right size. The
// size is a campaign's to choose. What is asserted is that whatever the size,
// the reading is bounded by it.
//
// If truncation is ever exposed — an explicit flag, or a written count beside
// the held count — this row should be extended to assert it rather than
// retired, since the saturation itself does not go away.
func TestTheRoundTripEstimateIsARingOccupancyAndNotAPopulation(t *testing.T) {
	// the shipping settings, so this tests the wiring and not the arithmetic:
	// these are the five values `newSendSequence` passes to `NewRttWindow`
	// (`transfer.go`), read here rather than restated
	sendBufferSettings := DefaultSendBufferSettings()
	windowSize := sendBufferSettings.RttWindowSize
	if windowSize <= 0 {
		t.Fatalf("the shipping RttWindowSize is %d, so there is no ring to measure", windowSize)
	}

	rttWindow := NewRttWindow(
		nil,
		windowSize,
		sendBufferSettings.RttWindowTimeout,
		sendBufferSettings.RttScale,
		sendBufferSettings.MinResendInterval,
		sendBufferSettings.RttMinResendInterval,
		sendBufferSettings.MaxResendInterval,
	)

	// Five times the ring, which is the ratio §38.15's two path lengths
	// differed by, so the row reproduces the exact comparison that misled the
	// inference rather than an arbitrary overfill.
	writeCount := 5 * windowSize
	base := time.Unix(1_700_000_000, 0)
	// One sample per millisecond with a strictly rising round trip, so the
	// held window and the population have visibly different means and the
	// whole run stays inside `RttWindowTimeout` — nothing here ages out, and
	// every eviction is the ring's own.
	sampleRtt := func(i int) time.Duration { return time.Duration(i+1) * time.Millisecond }
	for i := 0; i < writeCount; i += 1 {
		receiveTime := base.Add(time.Duration(i) * time.Millisecond)
		rttWindow.closeSendTime(
			uint64(receiveTime.Add(-sampleRtt(i)).UnixMilli()),
			receiveTime,
		)
	}
	readTime := base.Add(time.Duration(writeCount) * time.Millisecond)
	estimate := rttWindow.estimate(readTime)

	if !estimate.Sampled() {
		t.Fatal("the estimate reports no samples after a full ring of writes")
	}
	if estimate.SampleCount != windowSize {
		t.Errorf(
			"SampleCount reads %d after %d writes into a ring of %d. It is the ring's occupancy: it saturates and then stops moving, so it cannot be read as how much traffic backed the estimate, and comparing a mean over it against anything measured over the population compares two populations (THROUGHPUTFIX §38.15)",
			estimate.SampleCount,
			writeCount,
			windowSize,
		)
	}
	if writeCount <= estimate.SampleCount {
		t.Errorf(
			"SampleCount %d did not saturate below the %d writes, so the reading no longer distinguishes a ring from a population",
			estimate.SampleCount,
			writeCount,
		)
	}

	// The mean is the mean of what the ring holds, which is the last
	// `windowSize` writes, and it is not the mean of what was written.
	heldTotal := time.Duration(0)
	for i := writeCount - windowSize; i < writeCount; i += 1 {
		heldTotal += sampleRtt(i)
	}
	heldMean := heldTotal / time.Duration(windowSize)
	populationTotal := time.Duration(0)
	for i := 0; i < writeCount; i += 1 {
		populationTotal += sampleRtt(i)
	}
	populationMean := populationTotal / time.Duration(writeCount)

	if estimate.Mean != heldMean {
		t.Errorf(
			"Mean reads %s against %s, the mean of the last %d samples the ring holds. The window's mean is defined over its occupancy; a mean that is anything else has changed what every reader of it is measuring",
			estimate.Mean,
			heldMean,
			windowSize,
		)
	}
	if heldMean == populationMean {
		t.Fatalf(
			"the fixture's held mean and population mean are both %s, so this row cannot tell them apart and proves nothing",
			heldMean,
		)
	}
	if estimate.Mean == populationMean {
		t.Errorf(
			"Mean reads the population mean %s over all %d writes rather than the %s of the %d samples held. §38.15's discarded inference is exactly this reading",
			populationMean,
			writeCount,
			heldMean,
			windowSize,
		)
	}

	// The minimum travels under the same rule: it is the smallest LIVE sample,
	// so on a rising path it is the oldest one the ring still holds and not the
	// smallest ever written. A reader that takes it for a path floor over the
	// transfer is making §38.15's mistake in the other direction.
	oldestHeld := sampleRtt(writeCount - windowSize)
	if estimate.Min != oldestHeld {
		t.Errorf(
			"Min reads %s against %s, the smallest sample the ring still holds. Min is bounded by the ring exactly as Mean is",
			estimate.Min,
			oldestHeld,
		)
	}
	if estimate.Min == sampleRtt(0) {
		t.Errorf(
			"Min reads %s, the smallest sample ever written, which left the ring %d writes ago",
			estimate.Min,
			writeCount-windowSize,
		)
	}

	t.Logf(
		"ring %d, writes %d: SampleCount=%d Mean=%s Min=%s, against a population mean of %s over the writes",
		windowSize,
		writeCount,
		estimate.SampleCount,
		estimate.Mean,
		estimate.Min,
		populationMean,
	)
}
