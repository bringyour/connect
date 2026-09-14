package connect

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"
)

func TestTcpAckPerformanceUsesProgressNotSequenceOrigin(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	var performance tcpAckPerformance

	// The first ACK is the origin's random TCP sequence baseline, not four
	// billion useful bytes.
	performance.observe(start, true, 0xf0000000)
	performance.observe(start.Add(time.Millisecond), true, 0xf0000000+25_000)
	// A duplicate/reordered ACK contributes nothing.
	performance.observe(start.Add(2*time.Millisecond), true, 0xf0000000+10_000)

	peak, total := performance.snapshot()
	if total != 25_000 {
		t.Fatalf("acked bytes=%d, want exact cumulative advance 25000", total)
	}
	// The 250 ms partial-bucket floor caps a compressed one-millisecond ACK
	// burst at 100 kB/s rather than fabricating a 25 MB/s peak.
	if peak != 100_000 {
		t.Fatalf("peak ACK rate=%v B/s, want compression-capped 100000", peak)
	}
}

func TestTcpAckPerformanceHandlesSequenceWrap(t *testing.T) {
	start := time.Unix(1_700_000_000, 0)
	var performance tcpAckPerformance
	performance.observe(start, true, ^uint32(0)-49)
	performance.observe(start.Add(tcpAckPerformancePartialFloor), true, 50)
	_, total := performance.snapshot()
	if total != 100 {
		t.Fatalf("wrapped ACK advance=%d, want 100", total)
	}
}

func affinityPerformanceTestPath(sourcePort int) *IpPath {
	return &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        net.ParseIP("10.44.0.2").To4(),
		SourcePort:      sourcePort,
		DestinationIp:   net.ParseIP("198.51.100.40").To4(),
		DestinationPort: 443,
		Syn:             true,
	}
}

func affinityPerformanceTestClient(stats DestinationStats) *multiClientChannel {
	client := reputationTestClient(stats)
	client.args.Destination = RequireMultiHopId(NewId())
	return client
}

func affinityPerformanceTestCandidateUpdate(path *IpPath) *multiClientChannelUpdate {
	update := &multiClientChannelUpdate{
		ipPath:           path,
		affinityIp4Paths: map[Ip4Path]bool{},
		affinityIp6Paths: map[Ip6Path]bool{},
	}
	update.affinityIp4Paths[(&IpPath{ServerName: "bloomberg.com"}).ToIp4Path()] = true
	return update
}

func recordAffinityPerformanceTestSample(
	t testing.TB,
	parent *RemoteUserNatMultiClient,
	client *multiClientChannel,
	sourcePort int,
	ackedBytes uint32,
	activeDuration time.Duration,
	leaveOutsideWindows ...bool,
) *multiClientChannelUpdate {
	t.Helper()
	if len(leaveOutsideWindows) == 0 || !leaveOutsideWindows[0] {
		attached := false
		for _, window := range parent.windows {
			if window == nil {
				continue
			}
			if window.clients == nil {
				window.clients = map[Id]*multiClientChannel{}
			}
			window.clients[client.ClientId()] = client
			attached = true
			break
		}
		if !attached {
			parent.windows = map[WindowType]*multiClientWindow{
				WindowTypeQuality: {
					clients: map[Id]*multiClientChannel{client.ClientId(): client},
				},
			}
		}
	}
	path := affinityPerformanceTestPath(sourcePort)
	update := newMultiClientChannelUpdate(context.Background(), path)
	update.openTime = time.Now().Add(-activeDuration)
	update.activityTime = time.Now()
	update.receivedInbound.Store(true)
	update.affinityIp4Paths[(&IpPath{ServerName: "bloomberg.com"}).ToIp4Path()] = true

	start := update.openTime
	update.ackPerformance.observe(start, true, 1_000_000)
	if 0 < ackedBytes {
		update.ackPerformance.observe(start.Add(time.Millisecond), true, 1_000_000+ackedBytes)
	}
	parent.recordAffinityPerformance(update, client)
	return update
}

func attachLiveAffinityPerformanceTestSample(
	parent *RemoteUserNatMultiClient,
	client *multiClientChannel,
	sourcePort int,
	ackedBytes uint32,
	activeDuration time.Duration,
) *multiClientChannelUpdate {
	path := affinityPerformanceTestPath(sourcePort)
	update := newMultiClientChannelUpdate(context.Background(), path)
	update.openTime = time.Now().Add(-activeDuration)
	update.activityTime = time.Now()
	update.receivedInbound.Store(true)
	update.client.Store(client)
	update.affinityIp4Paths[(&IpPath{ServerName: "bloomberg.com"}).ToIp4Path()] = true
	update.ackPerformance.observe(update.openTime, true, 1_000_000)
	if 0 < ackedBytes {
		update.ackPerformance.observe(update.openTime.Add(time.Millisecond), true, 1_000_000+ackedBytes)
	}
	if parent.clientUpdates == nil {
		parent.clientUpdates = map[*multiClientChannel]map[*multiClientChannelUpdate]bool{}
	}
	parent.clientUpdates[client] = map[*multiClientChannelUpdate]bool{update: true}
	return update
}

func TestAffinityPerformanceUsesAdvertisedRateAsUnmeasuredNull(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	parent.reliabilityMetrics = newReliabilityMetrics()
	low := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	fresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	secondFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	thirdFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	fourthFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	path := affinityPerformanceTestPath(44300)

	if score, prior, measured := parent.affinityPerformanceScore(fresh, path, time.Now()); measured || score != 5_000_000 || prior != 5_000_000 {
		t.Fatalf("unmeasured score/prior/measured=%v/%v/%t, want advertised null 5MB/s", score, prior, measured)
	}

	update := recordAffinityPerformanceTestSample(t, parent, low, 44301, 25_000, time.Second)
	defer update.Close()
	if parent.affinityPerformanceAllowsDonor(low, path) {
		t.Fatal("low measured Bloomberg donor was inherited ahead of the advertised-rate null")
	}
	if update.client.Load() != nil {
		t.Fatal("recording performance unexpectedly assigned or moved the measured flow")
	}

	preferred := parent.preferAffinityPerformanceCandidates(
		[]*multiClientChannel{low, fresh, secondFresh, thirdFresh, fourthFresh},
		affinityPerformanceTestCandidateUpdate(path),
	)
	if len(preferred) != 4 || preferred[0] != fresh || preferred[1] != secondFresh || preferred[2] != thirdFresh || preferred[3] != fourthFresh {
		t.Fatal("unmeasured providers did not outrank the low-ACK provider on the next TLS flow")
	}
	metrics := parent.ReliabilityMetrics()
	if metrics.AffinityPerformanceSamples != 1 ||
		metrics.AffinityPerformanceDonorBypasses != 1 ||
		metrics.AffinityPerformanceCandidatesFiltered != 1 {
		t.Fatalf("affinity-performance metrics=%+v, want sample/bypass/filter 1/1/1", metrics)
	}
}

func TestAffinityPerformanceKeepsFourRouteTailInsurance(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	parent.reliabilityMetrics = newReliabilityMetrics()
	fresh := affinityPerformanceTestClient(DestinationStats{
		EstimatedBytesPerSecond: 5_000_000,
	})
	low := [5]*multiClientChannel{}
	for i := range low {
		low[i] = affinityPerformanceTestClient(DestinationStats{
			EstimatedBytesPerSecond: 5_000_000,
		})
		update := recordAffinityPerformanceTestSample(
			t,
			parent,
			low[i],
			44360+i,
			25_000,
			time.Second,
		)
		update.Close()
	}

	path := affinityPerformanceTestPath(44370)
	preferred := parent.preferAffinityPerformanceCandidates(
		[]*multiClientChannel{low[0], fresh, low[1], low[2], low[3], low[4]},
		affinityPerformanceTestCandidateUpdate(path),
	)
	if len(preferred) != affinityPerformanceMinRaceCandidates {
		t.Fatalf("preferred route count=%d, want exploration floor %d",
			len(preferred), affinityPerformanceMinRaceCandidates)
	}
	if preferred[0] != fresh || preferred[1] != low[0] ||
		preferred[2] != low[1] || preferred[3] != low[2] {
		t.Fatal("four-route floor did not retain the best route plus stable tail insurance")
	}
	if filtered := parent.ReliabilityMetrics().AffinityPerformanceCandidatesFiltered; filtered != 2 {
		t.Fatalf("filtered candidates=%d, want 2 beyond the four-route floor", filtered)
	}
}

func TestAffinityPerformanceRecordsOnlyActiveHealthyRoute(t *testing.T) {
	newFixture := func() (*RemoteUserNatMultiClient, *multiClientChannel) {
		parent := reputationTestParent("www.bloomberg.com")
		parent.reliabilityMetrics = newReliabilityMetrics()
		client := affinityPerformanceTestClient(DestinationStats{
			EstimatedBytesPerSecond: 5_000_000,
		})
		return parent, client
	}
	record := func(
		parent *RemoteUserNatMultiClient,
		client *multiClientChannel,
		port int,
		leaveOutsideWindows ...bool,
	) {
		update := recordAffinityPerformanceTestSample(
			t,
			parent,
			client,
			port,
			25_000,
			time.Second,
			leaveOutsideWindows...,
		)
		update.Close()
	}

	t.Run("active healthy route", func(t *testing.T) {
		parent, client := newFixture()
		record(parent, client, 44350)
		if len(parent.affinityPerformance) == 0 ||
			parent.ReliabilityMetrics().AffinityPerformanceSamples != 1 {
			t.Fatal("active healthy route did not publish performance evidence")
		}
	})

	t.Run("canceled route", func(t *testing.T) {
		parent, client := newFixture()
		ctx, cancel := context.WithCancel(context.Background())
		client.ctx = ctx
		cancel()
		record(parent, client, 44351)
		if len(parent.affinityPerformance) != 0 ||
			parent.ReliabilityMetrics().AffinityPerformanceSamples != 0 {
			t.Fatal("canceled route poisoned performance evidence")
		}
	})

	t.Run("warned route", func(t *testing.T) {
		parent, client := newFixture()
		client.setWarning(true, warnUnhealthy)
		record(parent, client, 44352)
		if len(parent.affinityPerformance) != 0 ||
			parent.ReliabilityMetrics().AffinityPerformanceSamples != 0 {
			t.Fatal("unhealthy warned route poisoned performance evidence")
		}
	})

	t.Run("quarantined route", func(t *testing.T) {
		parent, client := newFixture()
		client.setQuarantined(blackholeNoReceiveAck)
		record(parent, client, 44353)
		if len(parent.affinityPerformance) != 0 ||
			parent.ReliabilityMetrics().AffinityPerformanceSamples != 0 {
			t.Fatal("quarantined route poisoned performance evidence")
		}
	})

	t.Run("route absent from live windows", func(t *testing.T) {
		parent, client := newFixture()
		parent.windows = map[WindowType]*multiClientWindow{
			WindowTypeQuality: {clients: map[Id]*multiClientChannel{}},
		}
		record(parent, client, 44354, true)
		if len(parent.affinityPerformance) != 0 ||
			parent.ReliabilityMetrics().AffinityPerformanceSamples != 0 {
			t.Fatal("route outside the live window poisoned performance evidence")
		}
	})
}

func TestAffinityPerformanceColdFreshRaceAllocatesNothing(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	// Production clients install this immutable projection at construction;
	// bare fixtures intentionally synthesize one per read for mutability.
	parent.defaultReliabilitySettings = ReliabilitySettingsFrom(parent.settings)
	first := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	second := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	candidates := []*multiClientChannel{first, second}
	path := affinityPerformanceTestPath(44305)
	update := affinityPerformanceTestCandidateUpdate(path)

	allocs := testing.AllocsPerRun(1000, func() {
		got := parent.preferAffinityPerformanceCandidates(candidates, update)
		if len(got) != 2 {
			panic("cold race unexpectedly narrowed")
		}
	})
	if allocs != 0 {
		t.Fatalf("cold fresh-flow performance check allocated %.2f objects/run, want 0", allocs)
	}
}

func TestAffinityPerformanceMeasuredFreshRaceAllocatesNothing(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	parent.defaultReliabilitySettings = ReliabilitySettingsFrom(parent.settings)
	low := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	fresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	secondFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	thirdFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	fourthFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	recordAffinityPerformanceTestSample(t, parent, low, 46999, 25_000, time.Second)
	storage := [5]*multiClientChannel{}
	path := affinityPerformanceTestPath(47000)
	update := affinityPerformanceTestCandidateUpdate(path)
	allocs := testing.AllocsPerRun(100, func() {
		// The filter deliberately compacts this placement-local field in place.
		// Restore the synthetic field just as raceCandidates does for each real
		// fresh flow.
		storage[0], storage[1], storage[2], storage[3], storage[4] = low, fresh, secondFresh, thirdFresh, fourthFresh
		if got := parent.preferAffinityPerformanceCandidates(storage[:], update); len(got) != 4 || got[0] != fresh || got[1] != secondFresh || got[2] != thirdFresh || got[3] != fourthFresh {
			t.Fatalf("measured race = %v, want the four unmeasured providers", got)
		}
	})
	if allocs != 0 {
		t.Fatalf("measured fresh-flow performance filter allocated %.2f objects/run, want 0", allocs)
	}
}

func TestAffinityPerformanceUsesStillOpenH2EvidenceForNextFreshFlow(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	low := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	fresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	secondFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	thirdFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	fourthFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	live := attachLiveAffinityPerformanceTestSample(parent, low, 47010, 25_000, time.Second)
	defer live.Close()

	path := affinityPerformanceTestPath(47011)
	preferred := parent.preferAffinityPerformanceCandidates(
		[]*multiClientChannel{low, fresh, secondFresh, thirdFresh, fourthFresh},
		affinityPerformanceTestCandidateUpdate(path),
	)
	if len(preferred) != 4 || preferred[0] != fresh || preferred[1] != secondFresh || preferred[2] != thirdFresh || preferred[3] != fourthFresh {
		t.Fatalf("fresh race with open low-rate H2 flow = %v, want four unmeasured providers", preferred)
	}
	if len(parent.affinityPerformance) != 0 {
		t.Fatal("live evidence unexpectedly allocated or retained completed-flow history")
	}
}

func TestAffinityPerformanceEqualStillOpenOutcomesKeepFullRace(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	first := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	second := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	firstLive := attachLiveAffinityPerformanceTestSample(parent, first, 47020, 25_000, time.Second)
	secondLive := attachLiveAffinityPerformanceTestSample(parent, second, 47021, 25_000, time.Second)
	defer firstLive.Close()
	defer secondLive.Close()

	candidates := []*multiClientChannel{first, second}
	preferred := parent.preferAffinityPerformanceCandidates(
		candidates,
		affinityPerformanceTestCandidateUpdate(affinityPerformanceTestPath(47022)),
	)
	if len(preferred) != 2 || preferred[0] != first || preferred[1] != second {
		t.Fatalf("equal live short outcomes changed the race: %v", preferred)
	}
}

func TestAffinityPerformanceLiveFreshRaceAllocatesNothing(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	parent.defaultReliabilitySettings = ReliabilitySettingsFrom(parent.settings)
	low := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	fresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	secondFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	thirdFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	fourthFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	live := attachLiveAffinityPerformanceTestSample(parent, low, 47030, 25_000, time.Second)
	defer live.Close()
	storage := [5]*multiClientChannel{}
	update := affinityPerformanceTestCandidateUpdate(affinityPerformanceTestPath(47031))
	allocs := testing.AllocsPerRun(100, func() {
		storage[0], storage[1], storage[2], storage[3], storage[4] = low, fresh, secondFresh, thirdFresh, fourthFresh
		if got := parent.preferAffinityPerformanceCandidates(storage[:], update); len(got) != 4 || got[0] != fresh || got[1] != secondFresh || got[2] != thirdFresh || got[3] != fourthFresh {
			t.Fatalf("live measured race = %v, want four unmeasured providers", got)
		}
	})
	if allocs != 0 {
		t.Fatalf("live fresh-flow performance filter allocated %.2f objects/run, want 0", allocs)
	}
}

func TestAffinityPerformanceEqualShortOutcomesKeepEqualWeight(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	first := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	second := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	recordAffinityPerformanceTestSample(t, parent, first, 44310, 25_000, time.Second)
	recordAffinityPerformanceTestSample(t, parent, second, 44311, 25_000, time.Second)

	path := affinityPerformanceTestPath(44312)
	firstScore, _, _ := parent.affinityPerformanceScore(first, path, time.Now())
	secondScore, _, _ := parent.affinityPerformanceScore(second, path, time.Now())
	if firstScore != secondScore {
		t.Fatalf("equal short outcomes scored differently: %v vs %v", firstScore, secondScore)
	}
	preferred := parent.preferAffinityPerformanceCandidates(
		[]*multiClientChannel{first, second},
		affinityPerformanceTestCandidateUpdate(path),
	)
	if len(preferred) != 2 || preferred[0] != first || preferred[1] != second {
		t.Fatal("equal short outcomes did not preserve the full equal-weight race")
	}
}

func TestAffinityPerformanceKeepsFastDonor(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	fast := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	// Ten MB/s after the compression floor is above the five MB/s prior.
	recordAffinityPerformanceTestSample(t, parent, fast, 44320, 2_500_000, time.Second)
	if !parent.affinityPerformanceAllowsDonor(fast, affinityPerformanceTestPath(44321)) {
		t.Fatal("a donor that beat its advertised prior was unnecessarily scattered")
	}
}

func TestPerformanceAwareAffinityNeverMovesEstablishedFlow(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	low := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	established := recordAffinityPerformanceTestSample(t, parent, low, 44330, 25_000, time.Second)
	established.client.Store(low)

	if parent.affinityPerformanceAllowsDonor(low, affinityPerformanceTestPath(44331)) {
		t.Fatal("positive control: low donor unexpectedly remained affinity-eligible")
	}
	if established.client.Load() != low || established.IsDone() {
		t.Fatal("anti-bias moved or closed the already-established TLS flow")
	}
}

func TestPerformanceAwareAffinityChecksLiveDonorBeforeInheriting(t *testing.T) {
	parent := bindFlowTestParent()
	parent.ip4PathUpdates = map[Ip4Path]*multiClientChannelUpdate{}
	parent.settings.PerformanceAwareAffinity = true
	parent.defaultReliabilitySettings = ReliabilitySettingsFrom(parent.settings)
	parent.config.Store(&multiClientConfig{
		performanceProfile: parent.settings.DefaultPerformanceProfile,
		serverNameLookup:   stubServerNameLookup{names: []string{"www.bloomberg.com"}},
	})
	donor := bindFlowTestChannel(parent)
	donor.args = &multiClientChannelArgs{
		DestinationStats: DestinationStats{EstimatedBytesPerSecond: 5_000_000},
		Destination:      RequireMultiHopId(NewId()),
	}
	donorPath := affinityPerformanceTestPath(44335)
	donorUpdate := newMultiClientChannelUpdate(context.Background(), donorPath)
	defer donorUpdate.Close()
	donorUpdate.openTime = time.Now().Add(-time.Second)
	donorUpdate.receivedInbound.Store(true)
	donorUpdate.client.Store(donor)
	donorUpdate.ackPerformance.observe(donorUpdate.openTime, true, 1_000_000)
	donorUpdate.ackPerformance.observe(donorUpdate.openTime.Add(time.Millisecond), true, 1_025_000)
	parent.ip4PathUpdates[donorPath.ToIp4Path()] = donorUpdate

	fresh := newMultiClientChannelUpdate(context.Background(), affinityPerformanceTestPath(44336))
	defer fresh.Close()
	verdict, _ := parent.inheritAffinityClient4WithLock(
		fresh,
		map[Ip4Path]time.Time{donorPath.ToIp4Path(): time.Now()},
	)
	if verdict != donorRefused || fresh.client.Load() != nil {
		t.Fatal("fresh video flow blindly inherited a live low-ACK affinity donor")
	}
	if donorUpdate.client.Load() != donor || donorUpdate.IsDone() {
		t.Fatal("checking live affinity performance moved or closed the donor flow")
	}
}

func TestPerformanceAwareAffinityExplicitPinStillWins(t *testing.T) {
	parent := bindFlowTestParent()
	parent.ip4PathUpdates = map[Ip4Path]*multiClientChannelUpdate{}
	parent.settings.PerformanceAwareAffinity = true
	parent.defaultReliabilitySettings = ReliabilitySettingsFrom(parent.settings)
	parent.config.Store(&multiClientConfig{
		performanceProfile: parent.settings.DefaultPerformanceProfile,
		serverNameLookup:   stubServerNameLookup{names: []string{"www.bloomberg.com"}},
	})
	donor := bindFlowTestChannel(parent)
	donor.args = &multiClientChannelArgs{DestinationStats: DestinationStats{
		EstimatedBytesPerSecond: 5_000_000,
	}, Destination: RequireMultiHopId(NewId())}
	recordAffinityPerformanceTestSample(t, parent, donor, 44340, 25_000, time.Second)

	donorPath := affinityPerformanceTestPath(44341)
	donorUpdate := newMultiClientChannelUpdate(context.Background(), donorPath)
	defer donorUpdate.Close()
	donorUpdate.client.Store(donor)
	parent.ip4PathUpdates[donorPath.ToIp4Path()] = donorUpdate

	fresh := newMultiClientChannelUpdate(context.Background(), affinityPerformanceTestPath(44342))
	defer fresh.Close()
	fresh.pinned = true
	verdict, _ := parent.inheritAffinityClient4WithLock(
		fresh,
		map[Ip4Path]time.Time{donorPath.ToIp4Path(): time.Now()},
	)
	if verdict == donorRefused || fresh.client.Load() != donor {
		t.Fatal("performance anti-bias overrode an explicit app pin")
	}
}

func TestAffinityPerformanceTableIsBoundedAndExpires(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	client := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	parent.windows = map[WindowType]*multiClientWindow{
		WindowTypeQuality: {
			clients: map[Id]*multiClientChannel{client.ClientId(): client},
		},
	}
	for i := 0; i < affinityPerformanceMaxEntries+10; i++ {
		update := newMultiClientChannelUpdate(context.Background(), affinityPerformanceTestPath(45000+i))
		update.openTime = time.Now().Add(-time.Second)
		update.activityTime = time.Now()
		update.receivedInbound.Store(true)
		update.affinityIp4Paths[(&IpPath{ServerName: "video" + strconv.Itoa(i) + ".example"}).ToIp4Path()] = true
		update.ackPerformance.observe(update.openTime, true, 1000)
		parent.recordAffinityPerformance(update, client)
		update.Close()
	}
	if len(parent.affinityPerformance) != affinityPerformanceMaxEntries {
		t.Fatalf("affinity performance entries=%d, want cap %d", len(parent.affinityPerformance), affinityPerformanceMaxEntries)
	}
	lastName := "video" + strconv.Itoa(affinityPerformanceMaxEntries+9) + ".example"
	parent.config.Store(&multiClientConfig{serverNameLookup: stubServerNameLookup{names: []string{lastName}}})
	if _, _, measured := parent.affinityPerformanceScore(client, affinityPerformanceTestPath(46000), time.Now()); !measured {
		t.Fatal("positive control: newest bounded-table evidence was not retained")
	}
	for _, entry := range parent.affinityPerformance {
		entry.lastUpdate = time.Now().Add(-affinityPerformanceTTL - time.Second)
	}
	if _, _, measured := parent.affinityPerformanceScore(client, affinityPerformanceTestPath(46000), time.Now()); measured {
		t.Fatal("expired affinity-performance evidence still affected placement")
	}
}

func TestPerformanceAwareAffinityDefaultAndOverrideProjection(t *testing.T) {
	settings := DefaultMultiClientSettings()
	if settings.FreshFlowAffinity {
		t.Fatal("ordinary fresh-flow affinity must be disabled in production defaults")
	}
	if !settings.PerformanceAwareAffinity {
		t.Fatal("performance-aware affinity must be enabled in the production defaults")
	}
	if !ReliabilitySettingsFrom(settings).PerformanceAwareAffinity {
		t.Fatal("ReliabilitySettingsFrom dropped performance-aware affinity")
	}
	if ReliabilitySettingsFrom(nil).PerformanceAwareAffinity {
		t.Fatal("a zero-value runtime override must disable performance-aware affinity")
	}
	settings.FreshFlowAffinity = true
	if !ReliabilitySettingsFrom(settings).FreshFlowAffinity {
		t.Fatal("runtime projection dropped the legacy fresh-affinity A/B override")
	}
}

func BenchmarkTcpAckPerformanceAdvance(b *testing.B) {
	var performance tcpAckPerformance
	now := time.Unix(1_700_000_000, 0)
	sequence := uint32(1_000_000)
	performance.observe(now, true, sequence)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sequence += 1448
		now = now.Add(100 * time.Microsecond)
		performance.observe(now, true, sequence)
	}
}

func BenchmarkTcpAckPerformanceDuplicate(b *testing.B) {
	var performance tcpAckPerformance
	performance.observe(time.Unix(1_700_000_000, 0), true, 1_000_000)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if performance.needsTimestamp(true, 1_000_000) {
			b.Fatal("duplicate ACK unexpectedly requested a clock read")
		}
		performance.observe(time.Time{}, true, 1_000_000)
	}
}

func BenchmarkAffinityPerformanceColdFreshRace(b *testing.B) {
	parent := reputationTestParent("www.bloomberg.com")
	parent.defaultReliabilitySettings = ReliabilitySettingsFrom(parent.settings)
	first := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	second := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	candidates := []*multiClientChannel{first, second}
	path := affinityPerformanceTestPath(47000)
	update := affinityPerformanceTestCandidateUpdate(path)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if len(parent.preferAffinityPerformanceCandidates(candidates, update)) != 2 {
			b.Fatal("cold race narrowed")
		}
	}
}

func BenchmarkAffinityPerformanceMeasuredFreshRace(b *testing.B) {
	parent := reputationTestParent("www.bloomberg.com")
	parent.defaultReliabilitySettings = ReliabilitySettingsFrom(parent.settings)
	low := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	fresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	secondFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	thirdFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	fourthFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	recordAffinityPerformanceTestSample(b, parent, low, 47001, 25_000, time.Second)
	candidates := []*multiClientChannel{low, fresh, secondFresh, thirdFresh, fourthFresh}
	path := affinityPerformanceTestPath(47002)
	update := affinityPerformanceTestCandidateUpdate(path)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		candidates[0], candidates[1], candidates[2], candidates[3], candidates[4] = low, fresh, secondFresh, thirdFresh, fourthFresh
		if preferred := parent.preferAffinityPerformanceCandidates(candidates, update); len(preferred) != 4 || preferred[0] != fresh || preferred[1] != secondFresh || preferred[2] != thirdFresh || preferred[3] != fourthFresh {
			b.Fatal("measured race was not narrowed")
		}
	}
}

func BenchmarkAffinityPerformanceLiveFreshRace(b *testing.B) {
	parent := reputationTestParent("www.bloomberg.com")
	parent.defaultReliabilitySettings = ReliabilitySettingsFrom(parent.settings)
	low := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	fresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	secondFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	thirdFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	fourthFresh := affinityPerformanceTestClient(DestinationStats{EstimatedBytesPerSecond: 5_000_000})
	live := attachLiveAffinityPerformanceTestSample(parent, low, 47003, 25_000, time.Second)
	defer live.Close()
	candidates := []*multiClientChannel{low, fresh, secondFresh, thirdFresh, fourthFresh}
	update := affinityPerformanceTestCandidateUpdate(affinityPerformanceTestPath(47004))
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		candidates[0], candidates[1], candidates[2], candidates[3], candidates[4] = low, fresh, secondFresh, thirdFresh, fourthFresh
		if preferred := parent.preferAffinityPerformanceCandidates(candidates, update); len(preferred) != 4 || preferred[0] != fresh || preferred[1] != secondFresh || preferred[2] != thirdFresh || preferred[3] != fourthFresh {
			b.Fatal("live measured race was not narrowed")
		}
	}
}
