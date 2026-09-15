package connect

import (
	"fmt"
	"testing"
	"time"
)

// The relay-path throughput findings of THROUGHPUT-RIG-REVIEW.md, each as a
// deterministic scenario on the path simulator of pathsim_test.go.
//
// Every scenario asserts orderings or ratios between its own arms, never an
// absolute rate: the rig's numbers are the rig's and do not travel. The
// values each scenario produced when it was written are recorded beside the
// assertion, and where the simulation does not reproduce a rig finding the
// test asserts the simulated truth and says so, rather than being bent to
// agree. What the simulator contains and what it does not is the file
// header of pathsim_test.go; the short of it is that the transfer layer is
// real and everything below the route channel — the relay's queue, the
// wire, loss — is a model, and there is no kernel TCP on either end.
//
// The fast tier runs by default: a few virtual seconds per arm and a few
// wall seconds for the whole file. `CONNECT_PATHSIM_FULL=1` runs the long
// offers and the whole S7 grid.

// One gigabit per second: the relay path's rate class, above the ~800 Mb/s
// the rig's hosts reached and far below the instrument's ceiling.
const pathGigabit = ByteCount(125 * 1000 * 1000)

// A short relay path: the rig's 0.3 ms datacenter path, rounded up.
const pathShortRoundTrip = time.Millisecond

// The rig's second client, about 100 ms away.
const pathLongRoundTrip = 100 * time.Millisecond

// The relay's forward queue, in messages. The relay's is 4096 messages of
// mostly MTU-sized packets; at 16 KiB frames this depth carries the same
// bytes as about 350 of those, so the scenarios that need the queue to bind
// state their own depth.
const pathRelayQueueMessages = 4096

func pathScenarioArm(
	name string,
	hops []pathHop,
	lanes int,
	offer time.Duration,
	sizing WindowSizingPolicyKind,
	configure func(sender *ClientSettings, receiver *ClientSettings),
) pathArm {
	return pathArm{
		Name:             name,
		Hops:             hops,
		Lanes:            lanes,
		PayloadByteCount: pathPayloadByteCount,
		Offer:            offer,
		Drain:            30 * time.Second,
		StallAfter:       5 * time.Second,
		Seed:             7,
		WindowSizing:     sizing,
		MemoryBudget:     pathReferenceBudget,
		Configure:        configure,
	}
}

func pathSizingName(mode WindowSizingPolicyKind) string {
	if mode == WindowSizingFromDelivery {
		return "on"
	}
	return "off"
}

func pathPercent(loss float64) string {
	return fmt.Sprintf("%g%%", loss*100)
}

// S1. Loss on a short path costs throughput, and more loss costs more.
//
// THROUGHPUT-RIG-REVIEW §2: +0.5% item loss after the provider's route
// write cost 33% of throughput at 8 flows on the rig, before the receiver
// fixes, with about 6 resends per loss and 21% of wall time head-blocked.
//
// Produced (fast tier, 1 lane, 1 Gb/s, 1 ms): 993.5, 971.0, 443.0 Mb/s
// steady at 0, 0.5% and 2% — strictly decreasing, 2% costing 55%. At 0.5%
// every loss cost exactly one selective-gap resend, no duplicate and no
// head-of-line time, so the cost was 2.3%, not the rig's third. The rig
// disagrees on that magnitude, and the simulation is the reason it can:
// this tree's receiver writes its selective acks sorted and wakes early on
// a gap (the fixes the rig measured), and there is no kernel TCP above the
// transfer layer to react to the delay a hole adds. At 8 lanes (993.5,
// 992.9, 976.0) a lane's hole holds only that lane's eighth of the link.
func TestPathsimS1LossCostOnAShortPath(t *testing.T) {
	offer := pathOffer(2*time.Second, 8*time.Second)
	losses := []float64{0, 0.005, 0.02}
	results := []pathResult{}
	for _, loss := range losses {
		results = append(results, runPathArm(t, pathScenarioArm(
			"S1/short/lanes=1/loss="+pathPercent(loss),
			[]pathHop{pathRelayHop("relay", pathShortRoundTrip, pathGigabit, pathRelayQueueMessages, loss)},
			1, offer, WindowSizingConstant, nil,
		)))
	}
	reportPathScenario(t, "S1 loss cost on a short path", results)

	for i := 1; i < len(results); i++ {
		if results[i].steadyGoodput() >= results[i-1].steadyGoodput() {
			t.Errorf("S1: %s read %.1f Mb/s, not below %s at %.1f Mb/s; loss is supposed to cost throughput",
				results[i].arm, results[i].steadyGoodput()*8/1e6,
				results[i-1].arm, results[i-1].steadyGoodput()*8/1e6)
		}
	}
	if ratio := pathRatio(results[2], results[0]); 0.75 < ratio {
		t.Errorf("S1: 2%% loss kept %.2f of the lossless rate, above 0.75; produced 0.45", ratio)
	}
	for _, result := range results[1:] {
		if losses := result.forwardDrops(); losses <= 0 || float64(result.resendCount) > 1.1*float64(losses) {
			t.Errorf("S1: %s resent %d items for %d losses; more than 1.1 per loss is the spurious resend the sorted selective acks removed",
				result.arm, result.resendCount, losses)
		}
	}
}

// S2. The receiver's gap wake: fewer head-blocked intervals under loss.
//
// THROUGHPUT-RIG-REVIEW §2: selective acks written sorted, and the
// compression wait ended early when `AckGapWakeSelectiveCount` (3)
// selective acks are pending, took the rig from 600 to 709 Mb/s at 8 flows
// and 707 to 812 at 1 flow, with head-blocked time from 20% to 5%.
//
// The wake is a receiver setting and both arms run here. The write order is
// not: the sorted write has no seam and this file adds no production knob
// to unsort it, so its half of the finding is pinned by the receiver's own
// unit tests (transfer_receive_ack_gap_test.go) and not simulated.
//
// Produced (fast tier, 0.5% loss, 1 ms, 1 Gb/s): 1 lane, wake 3 against 0:
// 971.0 against 875.4 Mb/s steady (1.11x), head-of-line time 0 against
// 48.9 ms, exactly one resend per loss and no duplicate on either arm. At 8
// lanes 992.9 against 991.6: a lane's hole holds only its own share.
func TestPathsimS2ReceiverGapWake(t *testing.T) {
	offer := pathOffer(2*time.Second, 8*time.Second)
	results := []pathResult{}
	for _, lanes := range []int{1, 8} {
		for _, wake := range []int{3, 0} {
			results = append(results, runPathArm(t, pathScenarioArm(
				fmt.Sprintf("S2/short/lanes=%d/wake=%d", lanes, wake),
				[]pathHop{pathRelayHop("relay", pathShortRoundTrip, pathGigabit, pathRelayQueueMessages, 0.005)},
				lanes, offer, WindowSizingConstant,
				func(sender *ClientSettings, receiver *ClientSettings) {
					receiver.ReceiveBufferSettings.AckGapWakeSelectiveCount = wake
				},
			)))
		}
	}
	reportPathScenario(t, "S2 receiver gap wake", results)

	for i := 0; i < len(results); i += 2 {
		wake, noWake := results[i], results[i+1]
		// not lower, to within the 1% the eight-lane pair moves either way
		if wake.steadyGoodput() < 0.99*noWake.steadyGoodput() {
			t.Errorf("S2: %s read %.1f Mb/s below %s at %.1f; the wake is not supposed to cost throughput",
				wake.arm, wake.steadyGoodput()*8/1e6, noWake.arm, noWake.steadyGoodput()*8/1e6)
		}
		if noWake.holBlocked < wake.holBlocked {
			t.Errorf("S2: %s was head-blocked %s against %s without the wake; the wake exists to end those waits early",
				wake.arm, wake.holBlocked, noWake.holBlocked)
		}
		for _, result := range []pathResult{wake, noWake} {
			if result.duplicates != 0 {
				t.Errorf("S2: %s delivered %d duplicates; with sorted selective acks a hole costs one resend and no duplicate",
					result.arm, result.duplicates)
			}
		}
	}
	// the single-flow gain, where a hole holds the whole link
	if ratio := pathRatio(results[0], results[1]); ratio < 1.05 {
		t.Errorf("S2: the wake bought %.2fx at one lane, under 1.05; produced 1.11", ratio)
	}
	if results[1].holBlocked <= results[0].holBlocked {
		t.Errorf("S2: without the wake one lane was head-blocked %s, not above the %s with it",
			results[1].holBlocked, results[0].holBlocked)
	}
}

// S3. The delivery-sized window rule against the constant, on a short and
// on a long path, for one flow and for eight.
//
// THROUGHPUT-RIG-REVIEW §1: on the rig the rule lost 59-67% on the 0.3 ms
// path at every flow count, gained 57% for one flow at 100 ms, and lost 38%
// for eight flows at 100 ms on a budgeted client. It is off by default and
// `SetWindowSizing(WindowSizingFromDelivery)` turns it on.
//
// Produced (fast tier, 1 Gb/s, 64 MiB budget so the share is 8 MiB):
//
//	short 1 ms, 1 lane:  off 993.3, on 474.0 Mb/s over the offer (0.48x);
//	                     steady 993.5 against 750.9, the window at 1.4 MiB
//	short 1 ms, 8 lanes: off 993.5, on 993.5 steady (1.00x)
//	long 100 ms, 1 lane: off 148.8, on 588.9 steady (3.96x), window 7.8 MiB
//	long 100 ms, 8 lanes: off 143.7, on 592.4 steady (4.12x), window 6.0 MiB
//
// The long-path gain for one flow reproduces. The short-path cost for one
// flow reproduces over the offer but not at steady state: the rule climbs
// from its 320 KiB initial bet slowly on a short path, and at the full
// tier's 8 s offer the second half reads the link rate (993.5 against
// 993.5, 0.87x over the offer), at the rig's own 0.3 ms as much as at 1 ms.
// The rig lost 59-67% at steady state over 30 s runs, so that cell
// disagrees on mechanism — here the cost is a ramp — and the assertion is
// on the whole-offer goodput, which holds at both tiers. Two more cells
// disagree with the rig and are asserted as the simulation has them: the
// rule does not lose at eight lanes on the short path here, and it gains
// rather than loses at eight lanes on the long path. Every disagreeing
// rig cell involves what this simulator does not contain — the client's
// kernel TCP and its memory-budgeted receive side, and a relay doing
// per-message work — and the eight-lane cells here run eight equal shares
// of one pool rather than eight kernel flows.
func TestPathsimS3WindowRuleRegimes(t *testing.T) {
	offer := pathOffer(2*time.Second, 8*time.Second)
	type cell struct {
		roundTrip time.Duration
		lanes     int
	}
	cells := []cell{
		{roundTrip: pathShortRoundTrip, lanes: 1},
		{roundTrip: pathShortRoundTrip, lanes: 8},
		{roundTrip: pathLongRoundTrip, lanes: 1},
		{roundTrip: pathLongRoundTrip, lanes: 8},
	}
	results := []pathResult{}
	for _, c := range cells {
		for _, mode := range []WindowSizingPolicyKind{WindowSizingConstant, WindowSizingFromDelivery} {
			results = append(results, runPathArm(t, pathScenarioArm(
				fmt.Sprintf("S3/rtt=%s/lanes=%d/rule=%s", c.roundTrip, c.lanes, pathSizingName(mode)),
				[]pathHop{pathRelayHop("relay", c.roundTrip, pathGigabit, pathRelayQueueMessages, 0)},
				c.lanes, offer, mode, nil,
			)))
		}
	}
	reportPathScenario(t, "S3 window rule regimes", results)

	ratio := func(i int) float64 { return pathRatio(results[2*i+1], results[2*i]) }
	for i := range cells {
		if on := results[2*i+1]; !on.window.Sized {
			t.Errorf("S3: %s ran with the rule off (%s); the arm measures nothing", on.arm, on.window.Reason)
		}
	}
	// short path, one flow: the rule costs its ramp over the offer (rig:
	// -67% at steady state; produced 0.48x over 2 s and 0.87x over 8 s,
	// with the steady state converging to the constant's)
	if off, on := results[0], results[1]; 0.95*off.goodput() < on.goodput() {
		t.Errorf("S3: on the short path for one lane the rule read %.2fx the constant over the offer, not under 0.95; the report's §8.3 short-path defect, as a ramp here",
			on.goodput()/off.goodput())
	}
	t.Logf("S3: short path one lane, the rule against the constant: %.2fx over the offer, %.2fx steady", results[1].goodput()/results[0].goodput(), ratio(0))
	// short path, eight flows: no difference here (rig: -59%)
	if r := ratio(1); r < 0.97 || 1.03 < r {
		t.Errorf("S3: on the short path for eight lanes the rule read %.2fx the constant; the simulation had them equal (1.00x), the rig had the rule at 0.41x", r)
	}
	// long path, one flow: the rule gains (rig: +57%; produced 4.15x)
	if r := ratio(2); r < 1.5 {
		t.Errorf("S3: on the long path for one lane the rule read %.2fx the constant, under 1.5; the rule's design point", r)
	}
	// long path, eight flows: the rule gains here too (rig: -38%)
	if r := ratio(3); r < 1.5 {
		t.Errorf("S3: on the long path for eight lanes the rule read %.2fx the constant, under 1.5; the simulation had 4.12x where the rig's budgeted client had 0.62x", r)
	}
}

// S4. A bigger constant window into a bounded relay queue: more drops, more
// resends, no more throughput.
//
// THROUGHPUT-RIG-REVIEW §1 ("why a larger window does not pay"): with the
// constant window raised by hand the relay's loss rate rose with in-flight
// bytes — 2, 4, 8 MiB gave ~180, ~505, ~635 resends/s and 639, 613, 499 Mb/s
// at 8 flows, and 745 against 322 Mb/s for 1 flow at 2 against 4 MiB. The
// candidate is the relay's non-blocking 4096-message forward queue.
//
// The queue here is 200 messages of 16 KiB frames, so that 2 MiB (126
// items) fits and 4 and 8 MiB do not. Produced (fast tier, 1 ms, 1 Gb/s):
//
//	1 lane:  drops 0, 457, 1092; resends 0, 457, 1092;
//	         993.5, 183.0, 129.5 Mb/s steady
//	8 lanes: drops 0, 42, 641; resends 0, 64, 827;
//	         993.5, 993.5, 993.5 Mb/s steady
//
// Drops and resends rise with the window on both flow counts, as on the
// rig. The single flow collapses harder than the rig's (0.18x against
// 0.43x): a tail-drop burst of consecutive holes is recovered a few gap
// resends per round here, and the rig's kernel TCP above the transfer
// layer kept its own pipe fuller across those rounds. The eight lanes hold
// the link because each lane's holes stall only its share.
func TestPathsimS4RelayQueueOverflowVersusWindow(t *testing.T) {
	offer := pathOffer(2*time.Second, 8*time.Second)
	const queueMessages = 200
	windows := []ByteCount{mib(2), mib(4), mib(8)}
	results := []pathResult{}
	for _, lanes := range []int{1, 8} {
		for _, window := range windows {
			results = append(results, runPathArm(t, pathScenarioArm(
				fmt.Sprintf("S4/queue=%d/lanes=%d/window=%s", queueMessages, lanes, formatBytes(window)),
				[]pathHop{pathRelayHop("relay", pathShortRoundTrip, pathGigabit, queueMessages, 0)},
				lanes, offer, WindowSizingConstant,
				func(sender *ClientSettings, receiver *ClientSettings) {
					sender.SendBufferSettings.ResendQueueMaxByteCount = window
					receiver.ReceiveBufferSettings.ReceiveQueueMaxByteCount = 4 * window
				},
			)))
		}
	}
	reportPathScenario(t, "S4 relay queue overflow versus window", results)

	for lane := 0; lane < 2; lane++ {
		arms := results[3*lane : 3*lane+3]
		for i := 1; i < len(arms); i++ {
			if arms[i].forwardDrops() <= arms[i-1].forwardDrops() {
				t.Errorf("S4: %s dropped %d at the relay, not above %s at %d; a bigger window is a bigger burst into the queue",
					arms[i].arm, arms[i].forwardDrops(), arms[i-1].arm, arms[i-1].forwardDrops())
			}
			if arms[i].resendCount <= arms[i-1].resendCount {
				t.Errorf("S4: %s resent %d, not above %s at %d",
					arms[i].arm, arms[i].resendCount, arms[i-1].arm, arms[i-1].resendCount)
			}
			if 1.01*arms[i-1].steadyGoodput() < arms[i].steadyGoodput() {
				t.Errorf("S4: %s read %.1f Mb/s above %s at %.1f; a larger window does not pay on this path",
					arms[i].arm, arms[i].steadyGoodput()*8/1e6, arms[i-1].arm, arms[i-1].steadyGoodput()*8/1e6)
			}
		}
	}
	if ratio := pathRatio(results[1], results[0]); 0.5 < ratio {
		t.Errorf("S4: one lane at 4 MiB kept %.2f of its 2 MiB rate, above 0.5; produced 0.18, the rig 0.43", ratio)
	}
}

// S5. A receive hold smaller than the sender's window, with a hole and
// reordering: the committed-prefix receiver never withdraws an
// acknowledgement, the old evicting receiver does.
//
// THROUGHPUT-REPORT §3.11c and THROUGHPUT-RIG-REVIEW §6: on the older
// receive path an item already selectively acknowledged could be evicted to
// admit an earlier one, and the sender, holding a 60 s lease on it, stalled
// — silent reneging. Main's committed-prefix policy evicts only items not
// yet acknowledged. The old behaviour is kept as an arm of a production
// setting (`ReceiveHoldEvict`, with `EvictionNotice` off so the sender is
// not told), which is the seam this scenario uses.
//
// A hole alone does not reach the eviction path: a hole's resend is the
// head and is delivered without hold space. Reordering does — a later item
// held and acknowledged, an earlier non-head item arriving to a full hold —
// so the forward link carries 40 ms of reordering jitter, and the hold is a
// quarter of the window with no advertisement (a legacy receiver).
//
// Produced (fast tier, 20 ms, 250 Mb/s, hold 512 KiB against a 2 MiB
// window): the current receiver evicted 0 acknowledged items (86
// tentative), drained 1.57 s after the offer, longest gap 420 ms; the old
// receiver evicted 101 acknowledged items and still drained, 3.96 s after
// the offer, longest gap 358 ms, because the sender's acknowledgement-tail
// probes (159 against 8) re-fetched them. Neither stalled for the lease.
// So the 60 s stall of §3.11c does not reproduce on one route here, and
// what is asserted is what does: no withdrawn acknowledgement on the
// current tree, withdrawn ones on the old, and no stall on either. Both
// arms' throughput collapses (0.7 and 1.7 Mb/s steady) under this
// overrun, which is the report's §37.20 regime; at the full tier's 6 s
// offer the committed-prefix arm shows an 8.2 s gap and drains in 15.3 s
// against the evicting arm's 6.8 s, so no drain ordering is asserted.
func TestPathsimS5SilentReneging(t *testing.T) {
	offer := pathOffer(2*time.Second, 6*time.Second)
	arms := []pathArm{}
	for _, old := range []bool{false, true} {
		hop := pathRelayHop("relay", 20*time.Millisecond, pathGigabit/4, pathRelayQueueMessages, 0)
		hop.Forward.DropOffered = []int64{40}
		hop.Forward.Jitter = 40 * time.Millisecond
		hop.Forward.Reorder = true
		arm := pathScenarioArm(
			fmt.Sprintf("S5/hold=512KiB/reorder/evicting=%v", old),
			[]pathHop{hop},
			1, offer, WindowSizingConstant,
			func(sender *ClientSettings, receiver *ClientSettings) {
				receiver.ReceiveBufferSettings.ReceiveQueueMaxByteCount = 512 * 1024
				if old {
					receiver.ReceiveBufferSettings.ReceiveHoldPolicy = ReceiveHoldEvict
					receiver.ReceiveBufferSettings.EvictionNotice = false
				}
			},
		)
		arm.Drain = 90 * time.Second
		arm.StallAfter = 60 * time.Second
		arms = append(arms, arm)
	}
	results := []pathResult{}
	for _, arm := range arms {
		results = append(results, runPathArm(t, arm))
	}
	reportPathScenario(t, "S5 silent reneging", results)

	current, old := results[0], results[1]
	if current.receiverEvictions != 0 {
		t.Errorf("S5: the committed-prefix receiver withdrew %d acknowledgements; it must withdraw none", current.receiverEvictions)
	}
	if old.receiverEvictions == 0 {
		t.Errorf("S5: the evicting receiver withdrew no acknowledgement, so the arm does not reproduce the old behaviour")
	}
	for _, result := range results {
		if result.stalled {
			t.Errorf("S5: %s stalled for %s; produced no stall on either policy", result.arm, result.maxGap)
		}
		if !result.drained {
			t.Errorf("S5: %s did not drain within %s", result.arm, arms[0].Drain)
		}
	}
	// No ordering between the two drains is asserted: at the fast tier's
	// 2 s offer the evicting receiver drained slower (3.96 s against 1.58 s),
	// at the full tier's 6 s offer faster (6.8 s against 15.3 s, with an
	// 8.2 s gap on the committed-prefix arm). Both are the collapse of a
	// legacy receiver under a permanent overrun; a modern receiver advertises
	// its hold and a sender never overruns it.
	t.Logf("S5: committed-prefix drained in %s with longest gap %s; evicting drained in %s with longest gap %s",
		current.drainTime, current.maxGap, old.drainTime, old.maxGap)
}

// S7. The heavy latency grid: one-way delays of 50 to 300 ms, with and
// without loss, rule off and on, one flow.
//
// An instrument for the work the rig has not reached rather than a rig
// finding. Produced (1 Gb/s, Mb/s steady, rule off / on; the 50 and 100 ms
// rows from the fast tier's 4 s offer, the 200 and 300 ms rows from the
// full tier's 8 s):
//
//	one-way   0% loss         0.5% loss
//	 50 ms    148.8 / 580.5   117.3 / 359.2
//	100 ms     78.3 / 322.3    61.5 / 178.4
//	200 ms     39.6 / 145.6    34.9 /  81.2
//	300 ms     24.8 / 105.6    16.7 /  49.0
//
// Without loss both policies fall monotonically with delay, as a
// window-bound flow must (the constant reads its 2 MiB per round trip plus
// one compression interval), and the rule is above the constant at every
// delay. Loss costs at every cell. The loss columns were monotonic at 8 s
// and not at an earlier 4 s grid (nine against six losses decided the
// 200 and 300 ms cells), so only the lossless columns assert
// monotonicity. The fast tier runs the 50 and 100 ms rows; the full tier
// runs all four.
func TestPathsimS7HeavyLatencyGrid(t *testing.T) {
	offer := pathOffer(4*time.Second, 8*time.Second)
	oneWays := []time.Duration{50 * time.Millisecond, 100 * time.Millisecond}
	if pathsimFull() {
		oneWays = append(oneWays, 200*time.Millisecond, 300*time.Millisecond)
	}
	losses := []float64{0, 0.005}
	modes := []WindowSizingPolicyKind{WindowSizingConstant, WindowSizingFromDelivery}
	results := map[string]pathResult{}
	ordered := []pathResult{}
	key := func(oneWay time.Duration, loss float64, mode WindowSizingPolicyKind) string {
		return fmt.Sprintf("S7/oneway=%s/loss=%s/rule=%s", oneWay, pathPercent(loss), pathSizingName(mode))
	}
	for _, oneWay := range oneWays {
		for _, loss := range losses {
			for _, mode := range modes {
				result := runPathArm(t, pathScenarioArm(
					key(oneWay, loss, mode),
					[]pathHop{pathRelayHop("relay", 2*oneWay, pathGigabit, pathRelayQueueMessages, loss)},
					1, offer, mode, nil,
				))
				results[result.arm] = result
				ordered = append(ordered, result)
			}
		}
	}
	reportPathScenario(t, "S7 heavy latency grid", ordered)

	for _, mode := range modes {
		for i := 1; i < len(oneWays); i++ {
			slower, faster := results[key(oneWays[i], 0, mode)], results[key(oneWays[i-1], 0, mode)]
			if faster.steadyGoodput() <= slower.steadyGoodput() {
				t.Errorf("S7: %s read %.1f Mb/s, not below %s at %.1f; a window-bound flow falls with the round trip",
					slower.arm, slower.steadyGoodput()*8/1e6, faster.arm, faster.steadyGoodput()*8/1e6)
			}
		}
	}
	for _, oneWay := range oneWays {
		for _, loss := range losses {
			off, on := results[key(oneWay, loss, WindowSizingConstant)], results[key(oneWay, loss, WindowSizingFromDelivery)]
			if on.steadyGoodput() <= off.steadyGoodput() {
				t.Errorf("S7: %s read %.1f Mb/s, not above %s at %.1f; the rule is designed for these round trips",
					on.arm, on.steadyGoodput()*8/1e6, off.arm, off.steadyGoodput()*8/1e6)
			}
		}
		for _, mode := range modes {
			lossless, lossy := results[key(oneWay, 0, mode)], results[key(oneWay, 0.005, mode)]
			if lossless.steadyGoodput() <= lossy.steadyGoodput() {
				t.Errorf("S7: %s read %.1f Mb/s, not below %s at %.1f; loss costs at every delay",
					lossy.arm, lossy.steadyGoodput()*8/1e6, lossless.arm, lossless.steadyGoodput()*8/1e6)
			}
		}
	}
	for _, result := range ordered {
		if result.stalled {
			t.Errorf("S7: %s stalled for %s", result.arm, result.maxGap)
		}
	}
}

// S8. Multi-hop: two and three relay hops with their own queues, without
// loss and with loss on the far hop.
//
// An instrument for the multi-hop work rather than a rig finding. The
// sender is the provider, so the hops run provider-relay (0.3 ms one way),
// relay-relay (50 ms) and relay-client (100 ms), each with a 256-message
// drop-on-full queue; the two-hop path omits the middle relay.
//
// Produced (offer 4 s, 1 Gb/s, Mb/s steady): two hops 81.7 lossless and
// 71.8 at 0.5%; three hops 57.5 and 45.7. Both are the constant window over
// the round trip (2 MiB over 211 and 311 ms), and the longest gap without
// loss is about one round trip (184 and 285 ms); with loss it stays under
// two (211 and 606 ms) and everything admitted drains.
func TestPathsimS8MultiHop(t *testing.T) {
	offer := pathOffer(4*time.Second, 8*time.Second)
	const queueMessages = 256
	twoHops := func(loss float64) []pathHop {
		return []pathHop{
			pathRelayHop("provider-relay", 600*time.Microsecond, pathGigabit, queueMessages, 0),
			pathRelayHop("relay-client", 200*time.Millisecond, pathGigabit, queueMessages, loss),
		}
	}
	threeHops := func(loss float64) []pathHop {
		return []pathHop{
			pathRelayHop("provider-relay", 600*time.Microsecond, pathGigabit, queueMessages, 0),
			pathRelayHop("relay-relay", 100*time.Millisecond, pathGigabit, queueMessages, 0),
			pathRelayHop("relay-client", 200*time.Millisecond, pathGigabit, queueMessages, loss),
		}
	}
	type cell struct {
		name string
		hops []pathHop
	}
	cells := []cell{
		{name: "S8/hops=2/loss=0%", hops: twoHops(0)},
		{name: "S8/hops=2/loss=0.5%", hops: twoHops(0.005)},
		{name: "S8/hops=3/loss=0%", hops: threeHops(0)},
		{name: "S8/hops=3/loss=0.5%", hops: threeHops(0.005)},
	}
	results := []pathResult{}
	for _, c := range cells {
		results = append(results, runPathArm(t, pathScenarioArm(c.name, c.hops, 1, offer, WindowSizingConstant, nil)))
	}
	reportPathScenario(t, "S8 multi-hop", results)

	for i, result := range results {
		roundTrip := pathRoundTrip(cells[i].hops)
		if result.stalled || !result.drained {
			t.Errorf("S8: %s stalled (%s) or did not drain", result.arm, result.maxGap)
		}
		lossless := i%2 == 0
		if lossless && 2*roundTrip+100*time.Millisecond < result.maxGap {
			t.Errorf("S8: %s went %s without a delivery on a %s round trip with no loss", result.arm, result.maxGap, roundTrip)
		}
		if !lossless && 5*roundTrip < result.maxGap {
			t.Errorf("S8: %s went %s without a delivery on a %s round trip; recovery is supposed to be bounded by a few round trips", result.arm, result.maxGap, roundTrip)
		}
		if lossless && result.resendCount != 0 {
			t.Errorf("S8: %s resent %d items with no loss on any hop", result.arm, result.resendCount)
		}
	}
	if results[2].steadyGoodput() >= results[0].steadyGoodput() {
		t.Errorf("S8: three hops read %.1f Mb/s, not below two hops at %.1f", results[2].steadyGoodput()*8/1e6, results[0].steadyGoodput()*8/1e6)
	}
	if results[1].steadyGoodput() >= results[0].steadyGoodput() || results[3].steadyGoodput() >= results[2].steadyGoodput() {
		t.Errorf("S8: loss on the far hop did not cost throughput")
	}
}
