package connect

// Memory-pressure regression tests for IpAssoc.
//
// The risk shape on a memory-constrained host (the iOS network extension runs under a
// 24MiB Go memory limit) is the matrix at its caps: before the dense block storage,
// bounded pair fan-out, reusable clustering scratch, and component bound, a saturated
// default matrix held ~30MiB live, allocated ~82MiB per dirty clustering epoch, and an
// in-flight pass kept churning for ~10s after Close. These tests pin the saturated
// cost so it cannot regress.

import (
	"context"
	"net/netip"
	"runtime"
	"testing"
	"time"
)

func testingIpAssocFeed(ipAssoc *IpAssoc, i int) {
	ipAssoc.AddEgressPacket(&IpPath{
		Version:       4,
		Protocol:      IpProtocolUdp,
		DestinationIp: netip.AddrFrom4([4]byte{10, byte(i >> 16), byte(i >> 8), byte(i)}).AsSlice(),
	})
}

// TestIpAssocSaturationMemoryPressure drives the matrix to its per-block caps across
// the full block history — clique bursts of MaxEntityCount entities per block — while
// keeping the clustering epoch dirty, and bounds what saturation may cost:
//
//   - every block respects its structural caps (entities, pairs)
//   - retained heap stays bounded (skipped under -race, which inflates memory)
//   - a dirty clustering epoch allocates a bounded amount (scratch is reused)
//   - Close drops the matrix promptly and the run goroutine exits (no post-Close
//     churn tail)
func TestIpAssocSaturationMemoryPressure(t *testing.T) {
	if testing.Short() {
		t.Skip("memory pressure saturation test skipped in -short")
	}

	const retainedBudgetBytes = 16 * 1024 * 1024 // measured ~11.6MiB after the rework; ~30MiB before (and that under-filled the caps)
	const epochBudgetBytes = 8 * 1024 * 1024     // measured ~0.2MiB/epoch after the rework; ~82MiB before
	const mibBytes = float64(1024 * 1024)

	baselineMemory := heapAndStackInuse()
	baselineGoroutines := runtime.NumGoroutine()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultIpAssocSettings()
	// blocks long enough that the churn window below (~1.2s) runs against the full
	// saturated history (blocks rotate only as new ones are created); granule small so
	// every feed counts; association time short so the isolated pair below stays its
	// own component
	settings.AssociationBlockDuration = 2 * time.Second
	settings.ActivityGranule = 1 * time.Millisecond
	settings.PacketAssociationTime = 500 * time.Millisecond
	settings.ClusterEpoch = 100 * time.Millisecond

	ipAssoc := NewIpAssoc(ctx, settings)
	defer ipAssoc.Close()

	// an isolated co-active pair, fed before the saturation cliques and outside their
	// association window: it must still cluster, which proves the clustering passes
	// keep running (and keep working) while the degenerate components are skipped
	pairA := netip.AddrFrom4([4]byte{10, 99, 99, 1})
	pairB := netip.AddrFrom4([4]byte{10, 99, 99, 2})
	ipAssoc.AddEgressPacket(&IpPath{Version: 4, Protocol: IpProtocolUdp, DestinationIp: pairA.AsSlice()})
	ipAssoc.AddEgressPacket(&IpPath{Version: 4, Protocol: IpProtocolUdp, DestinationIp: pairB.AsSlice()})
	time.Sleep(settings.PacketAssociationTime + 100*time.Millisecond)

	// fill each block with a clique burst at the entity cap
	for block := range settings.AssociationBlockCount {
		for i := range settings.MaxEntityCount {
			testingIpAssocFeed(ipAssoc, block*settings.MaxEntityCount+i)
		}
		time.Sleep(settings.AssociationBlockDuration + 50*time.Millisecond)
	}

	// the isolated pair clustered; the saturated cliques were skipped as degenerate
	if len(ipAssoc.GetClusterAddrs(pairA)) != 2 {
		t.Fatalf("isolated pair cluster = %v, want the pair (clustering must keep running under saturation)", ipAssoc.GetClusterAddrs(pairA))
	}
	if 0 < len(ipAssoc.GetClusterAddrs(netip.AddrFrom4([4]byte{10, 0, 0, 1}))) {
		t.Fatal("a saturated clique entity clustered; degenerate components should be skipped")
	}

	// structural caps hold for every live block
	func() {
		ipAssoc.stateLock.Lock()
		defer ipAssoc.stateLock.Unlock()
		if len(ipAssoc.blocks) == 0 {
			t.Fatal("no blocks after saturation")
		}
		for _, block := range ipAssoc.blocks {
			if settings.MaxEntityCount < len(block.addrs) {
				t.Fatalf("block entities = %d, cap %d", len(block.addrs), settings.MaxEntityCount)
			}
			if settings.MaxAssociationCount < len(block.coCounts) {
				t.Fatalf("block pairs = %d, cap %d", len(block.coCounts), settings.MaxAssociationCount)
			}
		}
	}()

	// keep the matrix dirty across epochs and measure the per-epoch allocation churn
	// (the first epoch also fills the reusable scratch, so the retained measurement
	// below covers it)
	var m0 runtime.MemStats
	runtime.ReadMemStats(&m0)
	epochs := 10
	for range epochs {
		testingIpAssocFeed(ipAssoc, 0)
		time.Sleep(settings.ClusterEpoch + 20*time.Millisecond)
	}
	var m1 runtime.MemStats
	runtime.ReadMemStats(&m1)
	perEpoch := (m1.TotalAlloc - m0.TotalAlloc) / uint64(epochs)
	t.Logf("saturated dirty epoch: %.2fMiB allocated", float64(perEpoch)/mibBytes)
	// -race instruments allocations, so byte bounds only hold without it
	if !raceEnabled && epochBudgetBytes < perEpoch {
		t.Fatalf("saturated dirty epoch allocates %.1fMiB, budget %.0fMiB",
			float64(perEpoch)/mibBytes, float64(epochBudgetBytes)/mibBytes)
	}

	// retained covers the blocks, activity/name maps, and the reusable scratch
	retained := heapAndStackInuse()
	t.Logf("saturated retained: %.2fMiB over a %.2fMiB baseline", float64(retained)/mibBytes, float64(baselineMemory)/mibBytes)
	if !raceEnabled && baselineMemory+retainedBudgetBytes < retained {
		t.Fatalf("saturated retained heap+stack = %.1fMiB over a %.1fMiB baseline, budget %.0fMiB",
			float64(retained)/mibBytes, float64(baselineMemory)/mibBytes, float64(retainedBudgetBytes)/mibBytes)
	}

	// teardown: the matrix drops, the run goroutine exits, and allocation churn stops
	ipAssoc.Close()
	blocksDropped := func() bool {
		ipAssoc.stateLock.Lock()
		defer ipAssoc.stateLock.Unlock()
		return ipAssoc.blocks == nil
	}
	deadline := time.Now().Add(10 * time.Second)
	for !blocksDropped() && time.Now().Before(deadline) {
		select {
		case <-time.After(50 * time.Millisecond):
		}
	}
	if !blocksDropped() {
		t.Fatal("the matrix was not dropped after Close")
	}
	for runtime.NumGoroutine() > baselineGoroutines && time.Now().Before(deadline) {
		select {
		case <-time.After(50 * time.Millisecond):
		}
	}
	var m2 runtime.MemStats
	runtime.ReadMemStats(&m2)
	time.Sleep(1 * time.Second)
	var m3 runtime.MemStats
	runtime.ReadMemStats(&m3)
	// no clustering tail: after the drop, at most stray test noise allocates
	if tail := m3.TotalAlloc - m2.TotalAlloc; !raceEnabled && 1024*1024 < tail {
		t.Fatalf("allocation continued after Close: %.1fMiB in 1s", float64(tail)/mibBytes)
	}
	// the dropped matrix is collectable: live heap returns near the baseline
	afterClose := heapAndStackInuse()
	t.Logf("after Close: %.2fMiB", float64(afterClose)/mibBytes)
	if !raceEnabled && baselineMemory+4*1024*1024 < afterClose {
		t.Fatalf("heap after Close = %.1fMiB over a %.1fMiB baseline; the matrix should be dropped",
			float64(afterClose)/mibBytes, float64(baselineMemory)/mibBytes)
	}
}

// TestIpAssocPairPartnerCap: a burst of n concurrently active entities mints at most
// n*MaxPairPartnersPerActivity pairs (a bounded sample), not the n^2/2 clique.
func TestIpAssocPairPartnerCap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultIpAssocSettings()
	settings.ActivityGranule = 1 * time.Millisecond
	settings.PacketAssociationTime = 10 * time.Second
	settings.ClusterEpoch = 60 * time.Second
	settings.MaxPairPartnersPerActivity = 4

	ipAssoc := NewIpAssoc(ctx, settings)
	defer ipAssoc.Close()

	const n = 100
	for i := range n {
		testingIpAssocFeed(ipAssoc, i)
	}

	func() {
		ipAssoc.stateLock.Lock()
		defer ipAssoc.stateLock.Unlock()
		pairs := len(ipAssoc.blocks[0].coCounts)
		if pairs == 0 {
			t.Fatal("no pairs recorded")
		}
		if n*settings.MaxPairPartnersPerActivity < pairs {
			t.Fatalf("pairs = %d, want at most n*k = %d (clique would be %d)",
				pairs, n*settings.MaxPairPartnersPerActivity, n*(n-1)/2)
		}
	}()
}

// TestIpAssocShedMemory: the host memory-pressure hook drops the matrix and publishes
// empty clusters (with a version bump so consumers re-evaluate), and the assoc keeps
// working afterward.
func TestIpAssocShedMemory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultIpAssocSettings()
	settings.ActivityGranule = 1 * time.Millisecond
	settings.PacketAssociationTime = 10 * time.Second
	settings.ClusterEpoch = 60 * time.Second

	ipAssoc := NewIpAssoc(ctx, settings)
	defer ipAssoc.Close()

	testingIpAssocFeed(ipAssoc, 1)
	testingIpAssocFeed(ipAssoc, 2)
	ipAssoc.updateClusters()
	if len(ipAssoc.GetClusterAddrs(netip.AddrFrom4([4]byte{10, 0, 0, 1}))) == 0 {
		t.Fatal("expected a cluster before shed")
	}
	version := ipAssoc.ClusterVersion()

	ipAssoc.ShedMemory()

	if 0 < len(ipAssoc.GetClusterAddrs(netip.AddrFrom4([4]byte{10, 0, 0, 1}))) {
		t.Fatal("clusters survive shed")
	}
	if ipAssoc.ClusterVersion() == version {
		t.Fatal("shed must bump the cluster version so consumers re-evaluate")
	}
	func() {
		ipAssoc.stateLock.Lock()
		defer ipAssoc.stateLock.Unlock()
		if ipAssoc.blocks != nil {
			t.Fatal("blocks survive shed")
		}
		if 0 < len(ipAssoc.baseNames) || 0 < len(ipAssoc.lastActive) {
			t.Fatal("activity state survives shed")
		}
	}()

	// the matrix rebuilds from new activity
	testingIpAssocFeed(ipAssoc, 3)
	testingIpAssocFeed(ipAssoc, 4)
	ipAssoc.updateClusters()
	if len(ipAssoc.GetClusterAddrs(netip.AddrFrom4([4]byte{10, 0, 0, 3}))) == 0 {
		t.Fatal("expected a cluster to rebuild after shed")
	}
}

// TestIpAssocCloseDropsMatrix: teardown (Close or parent ctx cancel) drops the matrix
// via the run loop's exit path.
func TestIpAssocCloseDropsMatrix(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultIpAssocSettings()
	settings.ActivityGranule = 1 * time.Millisecond
	settings.ClusterEpoch = 20 * time.Millisecond

	ipAssoc := NewIpAssoc(ctx, settings)
	for i := range 10 {
		testingIpAssocFeed(ipAssoc, i)
	}
	ipAssoc.Close()

	dropped := func() bool {
		ipAssoc.stateLock.Lock()
		defer ipAssoc.stateLock.Unlock()
		return ipAssoc.blocks == nil
	}
	deadline := time.Now().Add(5 * time.Second)
	for !dropped() && time.Now().Before(deadline) {
		select {
		case <-time.After(10 * time.Millisecond):
		}
	}
	if !dropped() {
		t.Fatal("the matrix was not dropped after Close; blocks remain")
	}
}
