package connect

// Memory-pressure regression tests for the IpMux/UpgradeMux stack.
//
// The crash scenario on a memory-constrained host (the iOS network extension runs the
// whole SDK under a 24MiB Go memory limit) is a browser-style DNS burst while the tunnel
// is still establishing (black-holed): every claimed query held its own resolution
// pipeline for the full resolve budget. Before in-flight question coalescing and the
// question/fan-out caps, a burst of 450 claimed queries held ~830 goroutines and ~+9MiB
// for ~60s. These tests pin the burst cost so it cannot regress.

import (
	"fmt"
	"runtime"
	"testing"
	"time"

	"context"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestUpgradeMuxDnsBurstMemoryPressure drives a burst of claimed DNS queries — 120 distinct
// names, each sent 3 times (a retransmit and a second transaction, as client stub resolvers
// do when unanswered) — into a mux whose upstream accepts and drops everything, which is
// exactly a tunnel that is still establishing. It then bounds what the burst may cost:
//
//   - distinct in-flight questions cap at MaxInflightQueries (over-cap queries drop)
//   - goroutines stay structurally bounded (pipelines + the capped resolver fan-out)
//   - the Go runtime's heap+stack growth stays bounded (skipped under -race, which
//     inflates memory by design)
//   - after Close, goroutines drain back to near the baseline (nothing leaks)
func TestUpgradeMuxDnsBurstMemoryPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("memory pressure burst test skipped in -short")
	}

	const names = 120
	const goroutineBudget = 320               // measured ~180 after the caps; ~830 before them
	const memoryBudgetBytes = 6 * 1024 * 1024 // measured ~2.5MiB after the caps; ~8MiB before them
	const mibBytes = float64(1024 * 1024)     // for reporting

	// let goroutines from prior tests' async teardown settle so the baseline is stable
	stableAt := func() int {
		last := -1
		for range 100 {
			n := runtime.NumGoroutine()
			if n == last {
				return n
			}
			last = n
			select {
			case <-time.After(100 * time.Millisecond):
			}
		}
		return last
	}
	baselineGoroutines := stableAt()
	baselineMemory := heapAndStackInuse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultUpgradeMuxSettings()
	// tunnel-only: no host-egress fallback traffic from the suite; the tunnel-DoH dials all
	// die in the black-holed upstream below
	settings.Dns.Fallback = nil
	settings.Dns.LocalFallbackTimeout = 0
	// a short resolve budget so the burst pipelines drain within the test
	settings.Dns.ResolveTimeout = 3 * time.Second

	rec := &ipMuxRecorder{}
	mux, err := NewUpgradeMux(ctx, TransferPath{}, protocol.ProvideMode_Network, 0, rec.receive, settings, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Close()
	// the establishing tunnel: accept and drop every upstream packet
	mux.SetUpstream(func(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool {
		return true
	})

	// the burst: for each name, an initial send, an exact retransmit (same transaction id and
	// port), and a second transaction (new id) — the unanswered-client pattern
	for i := range names {
		name := fmt.Sprintf("burst%03d.example.test.", i)
		port := 42000 + i
		for _, id := range []uint16{uint16(0x1000 + i), uint16(0x1000 + i), uint16(0x8000 + i)} {
			if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, dnsQueryPacketFrom(t, name, id, port), 0) {
				t.Fatalf("query %d was not claimed", i)
			}
		}
	}

	// the question cap binds immediately: attach is synchronous, resolution cannot finish
	inflightCount := func() int {
		mux.inflightLock.Lock()
		defer mux.inflightLock.Unlock()
		return len(mux.inflight)
	}
	if got := inflightCount(); got != defaultMaxInflightDnsQueries {
		t.Fatalf("in-flight questions after burst = %d, want the cap (%d): %d names over cap must drop", got, defaultMaxInflightDnsQueries, names)
	}

	// watch the burst window (the pipelines hold for the 3s resolve budget)
	peakGoroutines := 0
	peakMemory := uint64(0)
	end := time.Now().Add(4 * time.Second)
	for time.Now().Before(end) {
		if n := runtime.NumGoroutine(); peakGoroutines < n {
			peakGoroutines = n
		}
		if got := inflightCount(); defaultMaxInflightDnsQueries < got {
			t.Fatalf("in-flight questions grew past the cap: %d", got)
		}
		if m := heapAndStackInuse(); peakMemory < m {
			peakMemory = m
		}
		select {
		case <-time.After(50 * time.Millisecond):
		}
	}

	if goroutineBudget < peakGoroutines-baselineGoroutines {
		t.Fatalf("burst peak goroutines = %d over a %d baseline, budget %d", peakGoroutines, baselineGoroutines, goroutineBudget)
	}
	// -race instruments allocations and stacks, so the byte bound only holds without it
	if !raceEnabled && baselineMemory+memoryBudgetBytes < peakMemory {
		t.Fatalf("burst peak heap+stack = %.1fMiB over a %.1fMiB baseline, budget %.0fMiB",
			float64(peakMemory)/mibBytes, float64(baselineMemory)/mibBytes, float64(memoryBudgetBytes)/mibBytes)
	}
	// nothing resolved (the tunnel is black-holed), so nothing may have been sent downstream
	if _, received := rec.counts(); received != 0 {
		t.Fatalf("received %d downstream replies from a black-holed tunnel, want 0", received)
	}

	// teardown: everything the burst spawned must drain (dial contexts die with the resolve
	// budget and the mux ctx; gVisor teardown is asynchronous, so poll)
	mux.Close()
	drained := func() bool {
		return runtime.NumGoroutine() <= baselineGoroutines+24
	}
	endDrain := time.Now().Add(30 * time.Second)
	for !drained() && time.Now().Before(endDrain) {
		select {
		case <-time.After(250 * time.Millisecond):
		}
	}
	if !drained() {
		t.Fatalf("goroutines did not drain after Close: %d, baseline %d", runtime.NumGoroutine(), baselineGoroutines)
	}
}

// heapAndStackInuse is the Go runtime's live heap plus goroutine stack usage, after a
// collection — the part of the process footprint these tests bound.
func heapAndStackInuse() uint64 {
	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return m.HeapInuse + m.StackInuse
}

// TestIpMuxSafeReceiveNoAlloc pins the per-packet downstream dispatch: safeReceive is on the
// path of every received packet, so it must not allocate (it exists to replace a per-packet
// HandleError closure).
func TestIpMuxSafeReceiveNoAlloc(t *testing.T) {
	if raceEnabled {
		t.Skip("alloc counts are not stable under -race")
	}
	receiver := ReceivePacketFunction(func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
	})
	packet := make([]byte, 64)
	allocs := testing.AllocsPerRun(1000, func() {
		safeReceive(receiver, TransferPath{}, protocol.ProvideMode_Network, nil, packet)
	})
	if 0 < allocs {
		t.Fatalf("safeReceive allocates %.1f/op; the per-packet receive path must not allocate", allocs)
	}
}
