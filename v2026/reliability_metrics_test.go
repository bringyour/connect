package connect

import (
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func TestReliabilityMetricsBlastRadius(t *testing.T) {
	m := newReliabilityMetrics()

	// two exits die, costing 3 and 5 flows
	m.exitLost(testRecoveryKeys(3, 1))
	m.exitLost(testRecoveryKeys(5, 2))

	s := m.snapshot()
	if s.ExitLossEvents != 2 {
		t.Errorf("exit loss events = %d, want 2", s.ExitLossEvents)
	}
	if s.FlowsLostToExit != 8 {
		t.Errorf("flows lost = %d, want 8", s.FlowsLostToExit)
	}
	if s.MaxFlowsLostInOneEvent != 5 {
		t.Errorf("max flows lost in one event = %d, want 5", s.MaxFlowsLostInOneEvent)
	}
	if s.MeanFlowsLostPerExitLoss != 4.0 {
		t.Errorf("blast radius = %v, want 4.0", s.MeanFlowsLostPerExitLoss)
	}
}

// An exit that dies carrying nothing costs the user nothing, but it is still a
// failure event -- counting it keeps the blast radius honest for a fix that
// works by spreading flows thinner.
func TestReliabilityMetricsEmptyExitLoss(t *testing.T) {
	m := newReliabilityMetrics()

	m.exitLost(nil)

	s := m.snapshot()
	if s.ExitLossEvents != 1 {
		t.Errorf("exit loss events = %d, want 1", s.ExitLossEvents)
	}
	if s.FlowsLostToExit != 0 {
		t.Errorf("flows lost = %d, want 0", s.FlowsLostToExit)
	}
	if s.MeanFlowsLostPerExitLoss != 0.0 {
		t.Errorf("blast radius = %v, want 0", s.MeanFlowsLostPerExitLoss)
	}
	if s.RecoveryPending != 0 {
		t.Errorf("pending = %d, want 0", s.RecoveryPending)
	}
}

func TestReliabilityMetricsRecovery(t *testing.T) {
	m := newReliabilityMetrics()

	ip := net.ParseIP("93.184.216.34").To4()
	m.exitLost([]recoveryKey{newRecoveryKey(ip, 443)})

	s := m.snapshot()
	if s.RecoveryPending != 1 {
		t.Fatalf("pending = %d, want 1", s.RecoveryPending)
	}
	if s.RecoveryCount != 0 {
		t.Fatalf("recovery count = %d, want 0 before the destination answers", s.RecoveryCount)
	}

	time.Sleep(20 * time.Millisecond)
	m.destinationReachable(ip, 443, testLocalPort)

	s = m.snapshot()
	if s.RecoveryCount != 1 {
		t.Fatalf("recovery count = %d, want 1", s.RecoveryCount)
	}
	if s.RecoveryPending != 0 {
		t.Errorf("pending = %d, want 0 once recovered", s.RecoveryPending)
	}
	if s.RecoveryMeanNanos <= 0 {
		t.Errorf("recovery mean = %d, want positive", s.RecoveryMeanNanos)
	}
	if s.RecoveryMeanNanos != s.RecoveryMaxNanos {
		t.Errorf("mean %d != max %d for a single sample", s.RecoveryMeanNanos, s.RecoveryMaxNanos)
	}
}

// Traffic from somewhere that never lost an exit must not be counted as a
// recovery, or ordinary browsing would manufacture good numbers.
func TestReliabilityMetricsIgnoresUnrelatedTraffic(t *testing.T) {
	m := newReliabilityMetrics()

	lost := net.ParseIP("93.184.216.34").To4()
	other := net.ParseIP("1.1.1.1").To4()
	m.exitLost([]recoveryKey{newRecoveryKey(lost, 443)})

	// different host
	m.destinationReachable(other, 443, testLocalPort)
	// right host, different port -- a different service, not the flow that died
	m.destinationReachable(lost, 80, testLocalPort)

	s := m.snapshot()
	if s.RecoveryCount != 0 {
		t.Errorf("recovery count = %d, want 0", s.RecoveryCount)
	}
	if s.RecoveryPending != 1 {
		t.Errorf("pending = %d, want 1", s.RecoveryPending)
	}
}

// Several flows to one host die together; the user is waiting from the first
// of them, so the earliest loss is the one that must be timed.
func TestReliabilityMetricsKeepsEarliestLoss(t *testing.T) {
	m := newReliabilityMetrics()

	ip := net.ParseIP("93.184.216.34").To4()
	key := newRecoveryKey(ip, 443)

	m.exitLost([]recoveryKey{key})
	time.Sleep(20 * time.Millisecond)
	m.exitLost([]recoveryKey{key})

	m.pendingLock.Lock()
	elapsed := time.Since(m.pending[key].lostTime)
	m.pendingLock.Unlock()

	if elapsed < 20*time.Millisecond {
		t.Fatalf("second loss overwrote the first: elapsed %v, want >= 20ms", elapsed)
	}
}

// A destination that never comes back has to be counted, otherwise abandoning
// flows entirely would score better than recovering them slowly.
func TestReliabilityMetricsMissedRecovery(t *testing.T) {
	m := newReliabilityMetrics()

	ip := net.ParseIP("93.184.216.34").To4()
	m.exitLost([]recoveryKey{newRecoveryKey(ip, 443)})

	// age the entry past the window rather than sleeping a minute
	m.pendingLock.Lock()
	for key, entry := range m.pending {
		entry.lostTime = time.Now().Add(-2 * recoveryTrackerMaxAge)
		m.pending[key] = entry
	}
	m.pendingLock.Unlock()

	// eviction runs on the next loss
	m.exitLost(testRecoveryKeys(1, 9))

	s := m.snapshot()
	if s.RecoveryMissed != 1 {
		t.Errorf("missed = %d, want 1", s.RecoveryMissed)
	}
	if s.RecoveryCount != 0 {
		t.Errorf("recovery count = %d, want 0", s.RecoveryCount)
	}

	// the expired destination coming back late is not a recovery
	m.destinationReachable(ip, 443, testLocalPort)
	if got := m.snapshot().RecoveryCount; got != 0 {
		t.Errorf("recovery count = %d after a late answer, want 0", got)
	}
}

// Eviction is lazy -- it only runs when another exit dies -- so a pending
// destination can outlive the age bound. If it then answers, it is the user
// revisiting the site, not the tunnel recovering, and counting it inflates the
// average without limit. On device this reported "avg 2m, worst 9m" for a
// tunnel that was recovering in seconds.
func TestReliabilityMetricsLateAnswerIsNotARecovery(t *testing.T) {
	m := newReliabilityMetrics()

	ip := net.ParseIP("93.184.216.34").To4()
	m.exitLost([]recoveryKey{newRecoveryKey(ip, 443)})

	// age it past the window without triggering eviction
	m.pendingLock.Lock()
	for key, entry := range m.pending {
		entry.lostTime = time.Now().Add(-2 * recoveryTrackerMaxAge)
		m.pending[key] = entry
	}
	m.pendingLock.Unlock()

	m.destinationReachable(ip, 443, testLocalPort)

	s := m.snapshot()
	if s.RecoveryCount != 0 {
		t.Errorf("a late answer was counted as a recovery: count %d, mean %dns", s.RecoveryCount, s.RecoveryMeanNanos)
	}
	if s.RecoveryMissed != 1 {
		t.Errorf("missed = %d, want 1: the destination never came back inside the window", s.RecoveryMissed)
	}
	if s.RecoveryPending != 0 {
		t.Errorf("pending = %d, want 0: the entry must be retired either way", s.RecoveryPending)
	}
}

// Losses arriving faster than they expire must not grow the tracker without
// bound -- this is the path that runs when an exit carrying every flow dies.
func TestReliabilityMetricsBoundsPending(t *testing.T) {
	m := newReliabilityMetrics()

	m.exitLost(testRecoveryKeys(recoveryTrackerMaxEntries+500, 1))

	s := m.snapshot()
	if recoveryTrackerMaxEntries < s.RecoveryPending {
		t.Fatalf("pending %d exceeds bound %d", s.RecoveryPending, recoveryTrackerMaxEntries)
	}
	if s.RecoveryMissed != 500 {
		t.Errorf("missed = %d, want 500", s.RecoveryMissed)
	}
}

func TestReliabilityMetricsReset(t *testing.T) {
	m := newReliabilityMetrics()

	ip := net.ParseIP("93.184.216.34").To4()
	m.flowOpened()
	m.exitLost([]recoveryKey{newRecoveryKey(ip, 443)})
	m.destinationReachable(ip, 443, testLocalPort)

	m.reset()

	s := m.snapshot()
	if s.FlowsOpened != 0 || s.ExitLossEvents != 0 || s.FlowsLostToExit != 0 {
		t.Errorf("counters not cleared: %+v", s)
	}
	if s.RecoveryCount != 0 || s.RecoveryMissed != 0 || s.RecoveryMaxNanos != 0 {
		t.Errorf("recovery not cleared: %+v", s)
	}
	if s.RecoveryPending != 0 {
		t.Errorf("pending = %d, want 0", s.RecoveryPending)
	}
}

// The counters are written from the packet path, so concurrent use has to be
// clean under -race.
func TestReliabilityMetricsConcurrent(t *testing.T) {
	m := newReliabilityMetrics()

	wg := sync.WaitGroup{}
	for i := range 8 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			keys := testRecoveryKeys(16, i)
			for range 50 {
				m.flowOpened()
				m.exitLost(keys)
				for _, key := range keys {
					m.destinationReachable([]byte(key.ip), key.port, testLocalPort)
				}
				m.snapshot()
			}
		}(i)
	}
	wg.Wait()

	s := m.snapshot()
	if s.FlowsOpened != 8*50 {
		t.Errorf("flows opened = %d, want %d", s.FlowsOpened, 8*50)
	}
	if s.ExitLossEvents != 8*50 {
		t.Errorf("exit loss events = %d, want %d", s.ExitLossEvents, 8*50)
	}
}

// The verdict-hold counters are defined here and incremented by the gating
// logic that lands separately; what this package owes them is the same
// contract as every other counter -- they count, they snapshot, they reset.
func TestReliabilityMetricsVerdictHoldCounters(t *testing.T) {
	m := newReliabilityMetrics()

	m.verdictHeldUplinkStale()
	m.verdictHeldUplinkStale()
	m.verdictHeldTransportDown()
	m.removalDeferred()
	m.removalDeferred()
	m.removalDeferred()

	s := m.snapshot()
	if s.VerdictsHeldUplinkStale != 2 {
		t.Errorf("verdicts held (uplink stale) = %d, want 2", s.VerdictsHeldUplinkStale)
	}
	if s.VerdictsHeldTransportDown != 1 {
		t.Errorf("verdicts held (transport down) = %d, want 1", s.VerdictsHeldTransportDown)
	}
	if s.RemovalsDeferred != 3 {
		t.Errorf("removals deferred = %d, want 3", s.RemovalsDeferred)
	}

	m.reset()
	s = m.snapshot()
	if s.VerdictsHeldUplinkStale != 0 || s.VerdictsHeldTransportDown != 0 || s.RemovalsDeferred != 0 {
		t.Errorf("verdict-hold counters not cleared: %+v", s)
	}
}

// Measurement sits on the packet path, so a client assembled without metrics
// has to keep forwarding traffic. Tests build RemoteUserNatMultiClient as a
// struct literal, which leaves the field nil -- and a panic there would take
// down the data path, not just the counters.
func TestReliabilityMetricsNilReceiver(t *testing.T) {
	var m *reliabilityMetrics

	ip := net.ParseIP("93.184.216.34").To4()
	m.flowOpened()
	m.exitLost([]recoveryKey{newRecoveryKey(ip, 443)})
	m.destinationReachable(ip, 443, testLocalPort)
	m.verdictHeldUplinkStale()
	m.verdictHeldTransportDown()
	m.removalDeferred()
	m.reset()

	s := m.snapshot()
	if s == nil {
		t.Fatal("snapshot on a nil receiver returned nil")
	}
	if s.FlowsOpened != 0 || s.ExitLossEvents != 0 {
		t.Errorf("nil receiver produced counts: %+v", s)
	}
	if s.VerdictsHeldUplinkStale != 0 || s.VerdictsHeldTransportDown != 0 || s.RemovalsDeferred != 0 {
		t.Errorf("nil receiver produced verdict-hold counts: %+v", s)
	}
}

// testLocalPort is the local (app-side) source port the tests answer on.
// Teardown-armed entries never consult it; the rebind classification tests
// vary it deliberately.
const testLocalPort = 54321

// A rebound flow's destination answering the same local source port is the
// server accepting the quic path migration; a different port is the app
// re-dialing. Both close the recovery measurement itself as usual.
func TestReliabilityMetricsRebindAcceptedVsRedialed(t *testing.T) {
	m := newReliabilityMetrics()

	accepted := net.ParseIP("93.184.216.34").To4()
	redialed := net.ParseIP("93.184.216.35").To4()
	m.exitLostRebound([]reboundFlow{
		{key: newRecoveryKey(accepted, 443), localPort: 40001},
		{key: newRecoveryKey(redialed, 443), localPort: 40002},
	})

	s := m.snapshot()
	if s.FlowsRebound != 2 {
		t.Fatalf("flows rebound = %d, want 2", s.FlowsRebound)
	}
	if s.RecoveryPending != 2 {
		t.Fatalf("pending = %d, want 2: a rebound flow still owes a recovery measurement", s.RecoveryPending)
	}

	// the rebound socket itself receives again -- migration accepted
	m.destinationReachable(accepted, 443, 40001)
	// a different local port answers -- the app re-dialed
	m.destinationReachable(redialed, 443, 40999)

	s = m.snapshot()
	if s.RebindsAccepted != 1 {
		t.Errorf("rebinds accepted = %d, want 1", s.RebindsAccepted)
	}
	if s.RebindsRedialed != 1 {
		t.Errorf("rebinds redialed = %d, want 1", s.RebindsRedialed)
	}
	if s.RecoveryCount != 2 {
		t.Errorf("recovery count = %d, want 2: classification must not eat the recovery itself", s.RecoveryCount)
	}
	if s.FlowsLostToExit != 0 {
		t.Errorf("flows lost = %d, want 0: a rebound flow is not lost", s.FlowsLostToExit)
	}
}

// A teardown-armed entry closes unclassified no matter what port answers --
// only entries armed by an actual rebind may move the migration counters.
func TestReliabilityMetricsRebindTeardownEntryUnclassified(t *testing.T) {
	m := newReliabilityMetrics()

	ip := net.ParseIP("93.184.216.34").To4()
	m.exitLost([]recoveryKey{newRecoveryKey(ip, 443)})
	m.destinationReachable(ip, 443, 40001)

	s := m.snapshot()
	if s.RecoveryCount != 1 {
		t.Fatalf("recovery count = %d, want 1", s.RecoveryCount)
	}
	if s.RebindsAccepted != 0 || s.RebindsRedialed != 0 {
		t.Errorf("teardown entry was classified: accepted %d, redialed %d, want 0/0", s.RebindsAccepted, s.RebindsRedialed)
	}
}

// The earliest-death rule holds across the two arming paths: an entry a
// teardown already armed is not re-armed (or reclassified) by a later rebind
// to the same destination.
func TestReliabilityMetricsRebindKeepsEarlierTeardownEntry(t *testing.T) {
	m := newReliabilityMetrics()

	ip := net.ParseIP("93.184.216.34").To4()
	key := newRecoveryKey(ip, 443)
	m.exitLost([]recoveryKey{key})
	m.exitLostRebound([]reboundFlow{{key: key, localPort: 40001}})

	m.pendingLock.Lock()
	entry := m.pending[key]
	m.pendingLock.Unlock()
	if entry.reboundLocalPort != -1 {
		t.Errorf("rebind overwrote the earlier teardown entry: reboundLocalPort = %d, want -1", entry.reboundLocalPort)
	}
	if got := m.snapshot().FlowsRebound; got != 1 {
		t.Errorf("flows rebound = %d, want 1: the rebind itself still counts", got)
	}
}

// A rebound destination that never answers inside the window is a missed
// recovery and classifies as neither accepted nor redialed -- the sum of the
// two may lag FlowsRebound, and that gap is meaningful.
func TestReliabilityMetricsRebindNeverAnsweredIsNeither(t *testing.T) {
	m := newReliabilityMetrics()

	ip := net.ParseIP("93.184.216.34").To4()
	m.exitLostRebound([]reboundFlow{{key: newRecoveryKey(ip, 443), localPort: 40001}})

	m.pendingLock.Lock()
	for key, entry := range m.pending {
		entry.lostTime = time.Now().Add(-2 * recoveryTrackerMaxAge)
		m.pending[key] = entry
	}
	m.pendingLock.Unlock()

	m.destinationReachable(ip, 443, 40001)

	s := m.snapshot()
	if s.RebindsAccepted != 0 || s.RebindsRedialed != 0 {
		t.Errorf("late answer was classified: accepted %d, redialed %d, want 0/0", s.RebindsAccepted, s.RebindsRedialed)
	}
	if s.RecoveryMissed != 1 {
		t.Errorf("missed = %d, want 1", s.RecoveryMissed)
	}
}

// The rebind counters honor the same contract as every other counter: they
// reset, and a nil receiver stays a silent no-op on the packet path.
func TestReliabilityMetricsRebindResetAndNilReceiver(t *testing.T) {
	m := newReliabilityMetrics()

	ip := net.ParseIP("93.184.216.34").To4()
	m.exitLostRebound([]reboundFlow{{key: newRecoveryKey(ip, 443), localPort: 40001}})
	m.destinationReachable(ip, 443, 40001)

	m.reset()
	s := m.snapshot()
	if s.FlowsRebound != 0 || s.RebindsAccepted != 0 || s.RebindsRedialed != 0 {
		t.Errorf("rebind counters not cleared: %+v", s)
	}

	var nilMetrics *reliabilityMetrics
	nilMetrics.exitLostRebound([]reboundFlow{{key: newRecoveryKey(ip, 443), localPort: 40001}})
	if got := nilMetrics.snapshot().FlowsRebound; got != 0 {
		t.Errorf("nil receiver produced counts: %d", got)
	}
}

func testRecoveryKeys(n int, seed int) []recoveryKey {
	keys := []recoveryKey{}
	for i := range n {
		ip := net.ParseIP(fmt.Sprintf("10.%d.%d.%d", seed%256, (i/256)%256, i%256)).To4()
		keys = append(keys, newRecoveryKey(ip, 443))
	}
	return keys
}
