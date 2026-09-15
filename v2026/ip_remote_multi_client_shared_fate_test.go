package connect

// Tests for the 2026-08-05 betanet fixes: the shared-fate verdict gate, the
// stall hold while quarantined, and the proof-of-delivery gates on flow
// inheritance.
//
// The field capture that motivates all three (stable providers): every stall
// conviction in the window landed 6-41s after a receive-silence bench,
// executing the flows the bench was protecting; the flows then re-raced onto
// a fresh dial that had never delivered a byte (destination-keyed
// qualification made it read as proven), which executed 90s later with 32
// flows -- one hiccup amplified into two. The phone's uplink blipped 42 times
// in 2.4h: correlated silence across exits is one fact about the shared path.

import (
	"context"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// --- the shared-fate recorder ---

func TestSharedFateRecorder(t *testing.T) {
	m := newReliabilityMetrics()
	now := time.Now()
	window := 10 * time.Second

	a, b, c := NewId(), NewId(), NewId()

	// empty: no peers
	AssertEqual(t, m.sharedFatePeers(a, now, window), 0)

	// self-evidence is not a peer
	m.recordSharedFate(a, now)
	AssertEqual(t, m.sharedFatePeers(a, now, window), 0)

	// two distinct others are two peers; re-recording deduplicates
	m.recordSharedFate(b, now)
	m.recordSharedFate(b, now)
	m.recordSharedFate(c, now)
	AssertEqual(t, m.sharedFatePeers(a, now, window), 2)

	// evidence outside the window is pruned, and re-recording refreshes
	m.recordSharedFate(b, now.Add(-2*window))
	AssertEqual(t, m.sharedFatePeers(a, now, window), 1)
	m.recordSharedFate(b, now)
	AssertEqual(t, m.sharedFatePeers(a, now, window), 2)

	// zero window is off, and a nil receiver is inert
	AssertEqual(t, m.sharedFatePeers(a, now, 0), 0)
	var nilMetrics *reliabilityMetrics
	nilMetrics.recordSharedFate(a, now)
	AssertEqual(t, nilMetrics.sharedFatePeers(a, now, window), 0)
}

// identify gives a bare fixture channel its own client id: ClientId() falls
// back to the args id, and two fixtures sharing the zero id collapse into one
// entry everywhere identity is the key (the shared-fate map, the candidate
// gather) -- which silently un-tests exactly what these tests assert.
func identify(client *multiClientChannel) *multiClientChannel {
	client.args = &multiClientChannelArgs{
		MultiClientGeneratorClientArgs: MultiClientGeneratorClientArgs{ClientId: NewId()},
	}
	return client
}

// --- fix 1: the stall hold while quarantined ---

// A benched exit's lifecycle belongs to the quarantine: acquittal on receive
// progress or execution on sustained silence. The busy-probe stall path must
// hold rather than execute mid-bench -- and the stall clock is deliberately
// not refreshed, so once the bench lifts a real stall convicts on carried
// evidence.
func TestStallConvictionHeldWhileQuarantined(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stallTimeout := 20 * time.Millisecond

		client := busyProbeTestChannel(t, func(timeout time.Duration, ackCallback func(error)) (bool, error) {
			// queued, never answered: the conviction state
			return true, nil
		})
		stallPast(client, stallTimeout)

		sibling := receivingSibling()
		window := busyProbeTestWindow(40*time.Millisecond, client, sibling)

		AssertEqual(t, client.setQuarantined(blackholeNoReceiveAck), true)
		AssertEqual(t, window.convictSendStalls(stallTimeout), false)
		AssertEqual(t, client.IsDone(), false)
		client.stateLock.Lock()
		endErr := client.endErr
		client.stateLock.Unlock()
		AssertEqual(t, endErr == nil, true)

		// the bench lifts with the stall still real: the carried evidence
		// convicts once the probe budget of a fresh pass elapses
		client.clearQuarantine()
		sibling.stateLock.Lock()
		sibling.lastReceiveAckTime = time.Now()
		sibling.stateLock.Unlock()
		AssertEqual(t, window.convictSendStalls(stallTimeout), true)
		AssertEqual(t, client.IsDone(), true)
	})
}

// --- fix 3: the shared-fate gate on the stall path ---

func TestStallConvictionHeldOnSharedFate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stallTimeout := 20 * time.Millisecond

		metrics := newReliabilityMetrics()
		client := busyProbeTestChannel(t, func(timeout time.Duration, ackCallback func(error)) (bool, error) {
			return true, nil
		})
		client.reliabilityMetricsFunc = func() *reliabilityMetrics { return metrics }
		stallPast(client, stallTimeout)

		sibling := receivingSibling()
		window := busyProbeTestWindow(40*time.Millisecond, client, sibling)
		window.reliabilitySettingsFunc = func() *ReliabilitySettings {
			return &ReliabilitySettings{
				BusyProbe:          true,
				BusyProbeBudget:    40 * time.Millisecond,
				SharedFateMinExits: 2,
				SharedFateWindow:   time.Minute,
			}
		}

		// another exit developed silence evidence moments ago: two exits inside
		// one window is the shared-path signature at MinExits=2
		metrics.recordSharedFate(NewId(), time.Now())

		AssertEqual(t, window.convictSendStalls(stallTimeout), false)
		AssertEqual(t, client.IsDone(), false)
		AssertEqual(t, metrics.verdictsHeldSharedFate.Load() >= 1, true)

		// the correlation clears (the peer's evidence ages out): the carried
		// stall evidence convicts on the next pass
		metrics.sharedFateLock.Lock()
		for id := range metrics.sharedFateEvidences {
			if id != client.ClientId() {
				metrics.sharedFateEvidences[id] = time.Now().Add(-2 * time.Minute)
			}
		}
		metrics.sharedFateLock.Unlock()

		sibling.stateLock.Lock()
		sibling.lastReceiveAckTime = time.Now()
		sibling.stateLock.Unlock()
		AssertEqual(t, window.convictSendStalls(stallTimeout), true)
		AssertEqual(t, client.IsDone(), true)
	})
}

// The stall pass itself records evidence: two exits stalling in the same
// pass see each other and both hold, even though neither was recorded before
// the pass began.
func TestStallPassCrossRecordsSharedFate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stallTimeout := 20 * time.Millisecond

		metrics := newReliabilityMetrics()
		newStalled := func() *multiClientChannel {
			c := busyProbeTestChannel(t, func(timeout time.Duration, ackCallback func(error)) (bool, error) {
				return true, nil
			})
			c.reliabilityMetricsFunc = func() *reliabilityMetrics { return metrics }
			return c
		}
		clientA := identify(newStalled())
		clientB := identify(newStalled())
		// both must be past the bar: the pass judges only clients sendStalled
		// reports, and the point of the test is that TWO of them see each other
		clientA.addSend(1440, udpTestPath(4))
		clientB.addSend(1440, udpTestPath(4))
		time.Sleep(stallTimeout + 30*time.Millisecond)
		AssertEqual(t, clientA.sendStalled(stallTimeout), true)
		AssertEqual(t, clientB.sendStalled(stallTimeout), true)

		window := busyProbeTestWindow(40*time.Millisecond, clientA, clientB, receivingSibling())
		window.reliabilitySettingsFunc = func() *ReliabilitySettings {
			return &ReliabilitySettings{
				BusyProbe:          true,
				BusyProbeBudget:    40 * time.Millisecond,
				SharedFateMinExits: 2,
				SharedFateWindow:   time.Minute,
			}
		}

		window.convictSendStalls(stallTimeout)
		// neither co-sufferer is executed: each saw the other's evidence,
		// recorded by the same pass that judged them
		AssertEqual(t, clientA.IsDone(), false)
		AssertEqual(t, clientB.IsDone(), false)
		AssertEqual(t, 2 <= metrics.verdictsHeldSharedFate.Load(), true)
	})
}

// --- fix 3: the shared-fate gate at the blackhole verdict site ---

// The wiring anchor, in the house style: the gate must sit in detectBlackhole
// between the verdict decision and addError -- endErr is first-write-wins, so
// a hold that runs after addError holds nothing -- and it must FALL THROUGH
// to the pass wait rather than continue, or a held exit spins the verdict
// loop hot.
func TestBlackholeSharedFateGateCallSites(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *multiClientChannel) detectBlackhole(")
	if !ok {
		t.Fatal("could not find detectBlackhole")
	}
	record := indexOfOrFatal(t, body, "self.metrics().recordSharedFate(")
	gate := indexOfOrFatal(t, body, "self.metrics().sharedFatePeers(")
	execute := indexOfOrFatal(t, body, `self.addError(fmt.Errorf(`)
	if !(record < gate && gate < execute) {
		t.Error("the shared-fate record and gate must precede the blackhole execution: record, then gate, then addError")
	}
	if heldVar := indexOfOrFatal(t, body, "sharedFateHeld := false"); !(heldVar < execute) {
		t.Error("the hold must be expressed as a fall-through guard around the execution, not a continue past the pass wait")
	}
}

func indexOfOrFatal(t *testing.T, body string, needle string) int {
	t.Helper()
	i := strings.Index(body, needle)
	if i < 0 {
		t.Fatalf("%q not found", needle)
	}
	return i
}

// --- fix 2: proof of delivery before flow inheritance ---

// A candidate that has never delivered a byte sorts behind every proven
// candidate, whatever its event recency: destination-keyed qualification
// makes a fresh dial read as proven while its transport is unproven.
func TestRebindCandidatesUnprovenSortLast(t *testing.T) {
	parent := bindFlowTestParent()
	proven := identify(bindFlowTestChannel(parent))
	unproven := identify(bindFlowTestChannel(parent))
	proven.log = NewNoopLogger()
	unproven.log = NewNoopLogger()
	proven.packetStats = &clientWindowStats{log: NewNoopLogger()}
	unproven.packetStats = &clientWindowStats{log: NewNoopLogger()}

	// the proven candidate delivered long ago; the unproven one is newer by
	// event recency and must still sort last
	proven.stateLock.Lock()
	proven.lastReceiveAckTime = time.Now().Add(-time.Minute)
	proven.stateLock.Unlock()
	unproven.stateLock.Lock()
	unproven.packetStats.lastEventTime = time.Now()
	unproven.stateLock.Unlock()

	// the candidate gather is exercised directly: the window offer plumbing
	// is not what this test is about
	parent.rebindCandidatesFunc = nil
	window := &multiClientWindow{
		ctx:      context.Background(),
		log:      NewNoopLogger(),
		settings: parent.settings,
		clients: map[Id]*multiClientChannel{
			proven.ClientId():   proven,
			unproven.ClientId(): unproven,
		},
	}
	parent.windows = map[WindowType]*multiClientWindow{WindowTypeQuality: window}

	candidates := parent.rebindCandidates(nil)
	AssertEqual(t, len(candidates), 2)
	AssertEqual(t, candidates[0] == proven, true)
	AssertEqual(t, candidates[1] == unproven, true)
}
