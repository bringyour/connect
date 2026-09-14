package connect

import (
	"context"
	"net/netip"
	"testing"
	"time"
)

func testingIpAssocAddr(s string) netip.Addr {
	return netip.MustParseAddr(s)
}

func testingIpAssocPath(s string) *IpPath {
	addr := testingIpAssocAddr(s)
	version := 4
	if addr.Is6() {
		version = 6
	}
	return &IpPath{
		Version:       version,
		Protocol:      IpProtocolTcp,
		DestinationIp: addr.AsSlice(),
	}
}

func TestClusterIpAssocThresholdSplit(t *testing.T) {
	a := testingIpAssocAddr("1.0.0.1")
	b := testingIpAssocAddr("1.0.0.2")
	c := testingIpAssocAddr("1.0.0.3")
	d := testingIpAssocAddr("1.0.0.4")

	// a, b, c strongly co-associated. d weakly attached to a only
	counts := map[netip.Addr]uint32{
		a: 10,
		b: 10,
		c: 10,
		d: 10,
	}
	coCounts := map[ipAssocPair]uint32{
		newIpAssocPair(a, b): 9,
		newIpAssocPair(a, c): 9,
		newIpAssocPair(b, c): 9,
		newIpAssocPair(a, d): 1,
	}

	members := clusterIpAssoc(counts, coCounts, map[netip.Addr][]string{}, 0.5)

	AssertEqual(t, 3, len(members[a]))
	AssertEqual(t, 3, len(members[b]))
	AssertEqual(t, 3, len(members[c]))
	_, ok := members[d]
	AssertEqual(t, false, ok)
}

func TestSplitIpAssocClusterPreservesRemovalOrder(t *testing.T) {
	// Nodes 0 and 1 are strongly associated. Node 3 is weakest and is removed
	// before node 2; the returned rest must retain that order for re-clustering.
	scratch := &ipAssocScratch{
		nodeCounts: []uint64{1, 1, 1, 1},
		adjInit:    []int32{0, 3, 6, 9, 12},
		adjNodes: []uint32{
			1, 2, 3,
			0, 2, 3,
			0, 1, 3,
			0, 1, 2,
		},
		adjPs: []float64{
			1, 0.2, 0.1,
			1, 0.2, 0.1,
			0.2, 0.2, 0,
			0.1, 0.1, 0,
		},
	}
	pool := []uint32{0, 1, 2, 3}

	cluster, rest := splitIpAssocCluster(scratch, pool, 0.8)

	AssertEqual(t, []uint32{0, 1}, cluster)
	AssertEqual(t, []uint32{3, 2}, rest)
}

func TestSplitIpAssocClusterNoSurvivorPreservesRemovalOrder(t *testing.T) {
	scratch := &ipAssocScratch{
		nodeCounts: []uint64{1, 1, 1},
		adjInit:    []int32{0, 0, 0, 0},
	}
	pool := []uint32{0, 1, 2}

	cluster, rest := splitIpAssocCluster(scratch, pool, 0.5)

	AssertEqual(t, 0, len(cluster))
	AssertEqual(t, []uint32{0, 1, 2}, rest)
}

func TestSplitIpAssocClusterSteadyStateDoesNotAllocate(t *testing.T) {
	const nodeCount = 64
	scratch := &ipAssocScratch{
		nodeCounts: make([]uint64, nodeCount),
		adjInit:    make([]int32, nodeCount+1),
		adjNodes:   make([]uint32, nodeCount*(nodeCount-1)),
		adjPs:      make([]float64, nodeCount*(nodeCount-1)),
	}
	originalPool := make([]uint32, nodeCount)
	workPool := make([]uint32, nodeCount)
	edge := 0
	for node := range nodeCount {
		scratch.nodeCounts[node] = 1
		originalPool[node] = uint32(node)
		scratch.adjInit[node] = int32(edge)
		for neighbor := range nodeCount {
			if neighbor == node {
				continue
			}
			scratch.adjNodes[edge] = uint32(neighbor)
			if 16 <= node && 16 <= neighbor {
				scratch.adjPs[edge] = 1
			} else {
				scratch.adjPs[edge] = 0.1
			}
			edge += 1
		}
	}
	scratch.adjInit[nodeCount] = int32(edge)
	copy(workPool, originalPool)
	splitIpAssocCluster(scratch, workPool, 0.8)

	allocs := testing.AllocsPerRun(100, func() {
		copy(workPool, originalPool)
		splitIpAssocCluster(scratch, workPool, 0.8)
	})

	AssertEqual(t, 0.0, allocs)
}

func TestClusterIpAssocComponents(t *testing.T) {
	a := testingIpAssocAddr("1.0.0.1")
	b := testingIpAssocAddr("1.0.0.2")
	c := testingIpAssocAddr("1.0.0.3")
	d := testingIpAssocAddr("1.0.0.4")

	// two independent pairs
	counts := map[netip.Addr]uint32{
		a: 10,
		b: 10,
		c: 4,
		d: 4,
	}
	coCounts := map[ipAssocPair]uint32{
		newIpAssocPair(a, b): 8,
		newIpAssocPair(c, d): 4,
	}

	members := clusterIpAssoc(counts, coCounts, map[netip.Addr][]string{}, 0.5)

	AssertEqual(t, 2, len(members[a]))
	AssertEqual(t, 2, len(members[b]))
	AssertEqual(t, 2, len(members[c]))
	AssertEqual(t, 2, len(members[d]))
	memberSet := map[netip.Addr]bool{}
	for _, member := range members[a] {
		memberSet[member] = true
	}
	AssertEqual(t, true, memberSet[a])
	AssertEqual(t, true, memberSet[b])
	AssertEqual(t, false, memberSet[c])
}

func TestClusterIpAssocBaseNameMerge(t *testing.T) {
	a := testingIpAssocAddr("1.0.0.1")
	b := testingIpAssocAddr("1.0.0.2")
	c := testingIpAssocAddr("1.0.0.3")

	// a and b share a base name with no co-occurrence.
	// c co-occurs with a only, but the merged node carries the association
	counts := map[netip.Addr]uint32{
		a: 10,
		b: 2,
		c: 10,
	}
	coCounts := map[ipAssocPair]uint32{
		newIpAssocPair(a, c): 8,
	}
	baseNames := map[netip.Addr][]string{
		a: {"example.com"},
		b: {"example.com"},
	}

	members := clusterIpAssoc(counts, coCounts, baseNames, 0.5)

	// all three cluster together: a+b merged by name, c associated with the merged node
	AssertEqual(t, 3, len(members[a]))
	AssertEqual(t, 3, len(members[b]))
	AssertEqual(t, 3, len(members[c]))
}

func TestClusterIpAssocBaseNameOnly(t *testing.T) {
	a := testingIpAssocAddr("1.0.0.1")
	b := testingIpAssocAddr("1.0.0.2")

	// no co-occurrence at all. the shared base name alone forms a cluster
	counts := map[netip.Addr]uint32{
		a: 1,
		b: 1,
	}
	baseNames := map[netip.Addr][]string{
		a: {"example.com"},
		b: {"example.com"},
	}

	members := clusterIpAssoc(counts, map[ipAssocPair]uint32{}, baseNames, 0.5)

	AssertEqual(t, 2, len(members[a]))
	AssertEqual(t, 2, len(members[b]))
}

func TestIpAssocActivityClustering(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultIpAssocSettings()
	settings.ActivityGranule = 1 * time.Millisecond
	settings.PacketAssociationTime = 100 * time.Millisecond
	// keep the epoch long so the test drives updateClusters directly
	settings.ClusterEpoch = 60 * time.Second

	ipAssoc := NewIpAssoc(ctx, settings)
	defer ipAssoc.Close()

	// a and b are active together
	for range 8 {
		ipAssoc.AddEgressPacket(testingIpAssocPath("1.0.0.1"))
		ipAssoc.AddIngressPacket(&IpPath{
			Version:  4,
			Protocol: IpProtocolTcp,
			SourceIp: testingIpAssocAddr("1.0.0.2").AsSlice(),
		})
		time.Sleep(2 * time.Millisecond)
	}

	// c is active alone, outside the association time
	time.Sleep(150 * time.Millisecond)
	for range 8 {
		ipAssoc.AddEgressPacket(testingIpAssocPath("1.0.0.3"))
		time.Sleep(2 * time.Millisecond)
	}

	ipAssoc.updateClusters()

	cluster := ipAssoc.GetCluster(testingIpAssocPath("1.0.0.1"))
	AssertEqual(t, 2, len(cluster))
	memberSet := map[string]bool{}
	for _, memberPath := range cluster {
		memberSet[memberPath.DestinationIp.String()] = true
	}
	AssertEqual(t, true, memberSet["1.0.0.1"])
	AssertEqual(t, true, memberSet["1.0.0.2"])

	AssertEqual(t, 0, len(ipAssoc.GetCluster(testingIpAssocPath("1.0.0.3"))))

	// version changes only when the clustering changes
	version := ipAssoc.ClusterVersion()
	ipAssoc.AddEgressPacket(testingIpAssocPath("1.0.0.1"))
	ipAssoc.updateClusters()
	AssertEqual(t, version, ipAssoc.ClusterVersion())
}

func TestIpAssocGranuleDedup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultIpAssocSettings()
	settings.ActivityGranule = 10 * time.Second
	settings.ClusterEpoch = 60 * time.Second

	ipAssoc := NewIpAssoc(ctx, settings)
	defer ipAssoc.Close()

	for range 100 {
		ipAssoc.AddEgressPacket(testingIpAssocPath("1.0.0.1"))
	}

	func() {
		ipAssoc.stateLock.Lock()
		defer ipAssoc.stateLock.Unlock()
		AssertEqual(t, 1, len(ipAssoc.blocks))
		AssertEqual(t, uint32(1), ipAssoc.blocks[0].countFor(testingIpAssocAddr("1.0.0.1")))
	}()
}

func TestIpAssocEntityCap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultIpAssocSettings()
	settings.ActivityGranule = 1 * time.Millisecond
	settings.ClusterEpoch = 60 * time.Second
	settings.MaxEntityCount = 2

	ipAssoc := NewIpAssoc(ctx, settings)
	defer ipAssoc.Close()

	ipAssoc.AddEgressPacket(testingIpAssocPath("1.0.0.1"))
	ipAssoc.AddEgressPacket(testingIpAssocPath("1.0.0.2"))
	ipAssoc.AddEgressPacket(testingIpAssocPath("1.0.0.3"))

	func() {
		ipAssoc.stateLock.Lock()
		defer ipAssoc.stateLock.Unlock()
		AssertEqual(t, 2, len(ipAssoc.blocks[0].addrs))
		_, ok := ipAssoc.blocks[0].indexes[testingIpAssocAddr("1.0.0.3")]
		AssertEqual(t, false, ok)
	}()
}

func TestIpAssocBlockRotation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultIpAssocSettings()
	settings.ActivityGranule = 1 * time.Millisecond
	settings.AssociationBlockDuration = 20 * time.Millisecond
	settings.AssociationBlockCount = 2
	settings.ClusterEpoch = 60 * time.Second

	ipAssoc := NewIpAssoc(ctx, settings)
	defer ipAssoc.Close()

	for range 4 {
		ipAssoc.AddEgressPacket(testingIpAssocPath("1.0.0.1"))
		time.Sleep(25 * time.Millisecond)
	}

	func() {
		ipAssoc.stateLock.Lock()
		defer ipAssoc.stateLock.Unlock()
		AssertEqual(t, 2, len(ipAssoc.blocks))
	}()
}

type testingServerNameLookup struct {
	serverNames map[string][]string
}

func (self *testingServerNameLookup) ServerNames(ip string) []string {
	return self.serverNames[ip]
}

func TestIpAssocMultipleIpsPerHost(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultIpAssocSettings()
	settings.ActivityGranule = 1 * time.Millisecond
	settings.PacketAssociationTime = 100 * time.Millisecond
	settings.ClusterEpoch = 60 * time.Second

	ipAssoc := NewIpAssoc(ctx, settings)
	defer ipAssoc.Close()

	// one host name resolves to multiple ips (multiple a records).
	// activity may land on either ip, and both must associate with the cluster
	ipAssoc.SetServerNameLookup(&testingServerNameLookup{
		serverNames: map[string][]string{
			"1.0.0.1": {"cdn.example.com"},
			"1.0.0.2": {"cdn.example.com"},
		},
	})

	// only the first host ip co-occurs with the api ip
	for range 8 {
		ipAssoc.AddEgressPacket(testingIpAssocPath("1.0.0.1"))
		ipAssoc.AddEgressPacket(testingIpAssocPath("9.0.0.9"))
		time.Sleep(2 * time.Millisecond)
	}
	// the second host ip is active outside the association time
	time.Sleep(150 * time.Millisecond)
	ipAssoc.AddEgressPacket(testingIpAssocPath("1.0.0.2"))

	ipAssoc.updateClusters()

	// either host ip resolves to the full cluster: both host ips + the associated api ip
	for _, hostIp := range []string{"1.0.0.1", "1.0.0.2"} {
		cluster := ipAssoc.GetCluster(testingIpAssocPath(hostIp))
		memberSet := map[string]bool{}
		for _, memberPath := range cluster {
			memberSet[memberPath.DestinationIp.String()] = true
		}
		AssertEqual(t, 3, len(cluster))
		AssertEqual(t, true, memberSet["1.0.0.1"])
		AssertEqual(t, true, memberSet["1.0.0.2"])
		AssertEqual(t, true, memberSet["9.0.0.9"])
	}
}

func TestIpAssocServerNameLookup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultIpAssocSettings()
	settings.ActivityGranule = 1 * time.Millisecond
	settings.ClusterEpoch = 60 * time.Second

	ipAssoc := NewIpAssoc(ctx, settings)
	defer ipAssoc.Close()

	// subdomains collapse to the base name, so both ips merge into one node
	ipAssoc.SetServerNameLookup(&testingServerNameLookup{
		serverNames: map[string][]string{
			"1.0.0.1": {"a.example.com"},
			"1.0.0.2": {"b.c.example.com"},
		},
	})

	ipAssoc.AddEgressPacket(testingIpAssocPath("1.0.0.1"))
	// outside the association time in block terms is fine.
	// the base name alone merges the entities
	time.Sleep(2 * time.Millisecond)
	ipAssoc.AddEgressPacket(testingIpAssocPath("1.0.0.2"))

	ipAssoc.updateClusters()

	cluster := ipAssoc.GetCluster(testingIpAssocPath("1.0.0.1"))
	AssertEqual(t, 2, len(cluster))
}
