package connect

import (
	"context"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"testing"
	"time"
)

func reputationTestClient(stats DestinationStats) *multiClientChannel {
	settings := DefaultMultiClientSettings()
	return &multiClientChannel{
		ctx:      context.Background(),
		settings: settings,
		args: &multiClientChannelArgs{
			DestinationStats: stats,
		},
	}
}

func reputationTestParent(names ...string) *RemoteUserNatMultiClient {
	parent := &RemoteUserNatMultiClient{
		settings: DefaultMultiClientSettings(),
		log:      NewNoopLogger(),
	}
	parent.config.Store(&multiClientConfig{
		serverNameLookup: stubServerNameLookup{names: names},
	})
	return parent
}

func reputationTestUpdate(path *IpPath, affinityName string) *multiClientChannelUpdate {
	update := &multiClientChannelUpdate{
		ipPath:           path,
		affinityIp4Paths: map[Ip4Path]bool{},
		affinityIp6Paths: map[Ip6Path]bool{},
	}
	if affinityName != "" {
		update.affinityIp4Paths[(&IpPath{ServerName: affinityName}).ToIp4Path()] = true
	}
	return update
}

func TestNormalizeProviderReputationFailuresIsBoundedAndCanonical(t *testing.T) {
	raw := " Bloomberg , *.BLOOMBERG.com.,canva,bloomberg,, "
	got := normalizeProviderReputationFailures(raw)
	want := []string{"bloomberg", "bloomberg.com", "canva"}
	if !slices.Equal(got, want) {
		t.Fatalf("normalized failures=%q, want %q", got, want)
	}

	many := ""
	for i := 0; i < providerReputationFailureMaxCount+10; i++ {
		many += "vendor" + strconv.Itoa(i) + ","
	}
	if got := normalizeProviderReputationFailures(many); len(got) != providerReputationFailureMaxCount {
		t.Fatalf("normalized failure count=%d, want cap=%d", len(got), providerReputationFailureMaxCount)
	}
}

func TestReputationFailureMatchesOnlyTheIntendedSite(t *testing.T) {
	tests := []struct {
		failure string
		name    string
		want    bool
	}{
		{"bloomberg", "www.bloomberg.com", true},
		{"bloomberg.com", "assets.bloomberg.com", true},
		{"bloomberg.com", "bloomberg.com.evil.test", false},
		{"bloomberg", "notbloomberg.com", false},
		{"canva", "www.canva.com", true},
	}
	for _, test := range tests {
		if got := reputationFailureMatchesServerName(test.failure, test.name); got != test.want {
			t.Errorf("match(%q,%q)=%t, want %t", test.failure, test.name, got, test.want)
		}
	}
}

func TestFreshBloombergRaceExcludesExternallyRejectedExit(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	bad := reputationTestClient(DestinationStats{ReputationFailures: []string{"bloomberg"}})
	clean := reputationTestClient(DestinationStats{})
	path := &IpPath{DestinationIp: net.ParseIP("198.51.100.40"), DestinationPort: 443}
	update := reputationTestUpdate(path, "bloomberg.com")

	got := parent.filterReputationFailedCandidates([]*multiClientChannel{bad, clean}, update)
	if len(got) != 1 || got[0] != clean {
		t.Fatalf("fresh Bloomberg candidates=%p, want clean exit only", got)
	}

	// The negative result is domain-specific, not a provider-wide health
	// verdict: the same exit remains eligible for unrelated destinations.
	update = reputationTestUpdate(path, "example.com")
	got = parent.filterReputationFailedCandidates([]*multiClientChannel{bad, clean}, update)
	if len(got) != 2 || got[0] != bad || got[1] != clean {
		t.Fatal("Bloomberg reputation result leaked into an unrelated site")
	}
}

func TestFreshBloombergReputationFilterAllocatesNothing(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	bad := reputationTestClient(DestinationStats{ReputationFailures: []string{"bloomberg"}})
	clean := reputationTestClient(DestinationStats{})
	storage := [2]*multiClientChannel{}
	path := &IpPath{DestinationIp: net.ParseIP("198.51.100.40"), DestinationPort: 443}
	update := reputationTestUpdate(path, "bloomberg.com")
	allocs := testing.AllocsPerRun(100, func() {
		storage[0], storage[1] = bad, clean
		if got := parent.filterReputationFailedCandidates(storage[:], update); len(got) != 1 || got[0] != clean {
			t.Fatalf("filtered field=%v, want clean provider", got)
		}
	})
	if allocs != 0 {
		t.Fatalf("fresh reputation filter allocated %.2f objects/run, want 0", allocs)
	}
}

func TestReputationFilterPreservesAvailabilityWhenEveryExitFailed(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	first := reputationTestClient(DestinationStats{ReputationFailures: []string{"bloomberg"}})
	second := reputationTestClient(DestinationStats{ReputationFailures: []string{"bloomberg.com"}})
	candidates := []*multiClientChannel{first, second}
	path := &IpPath{DestinationIp: net.ParseIP("198.51.100.40"), DestinationPort: 443}
	update := reputationTestUpdate(path, "bloomberg.com")

	got := parent.filterReputationFailedCandidates(candidates, update)
	if len(got) != 2 || got[0] != first || got[1] != second {
		t.Fatal("all-negative reputation field failed closed instead of preserving availability")
	}
}

func TestBloombergDnsHintSkipsExternallyRejectedDonor(t *testing.T) {
	parent := dnsAffinityTestParent()
	bad := reputationTestClient(DestinationStats{ReputationFailures: []string{"bloomberg"}})
	clean := reputationTestClient(DestinationStats{})
	affinityName := affinityNameForServerName("www.bloomberg.com")
	address := netip.MustParseAddr("198.51.100.41")
	now := time.Now()
	parent.dnsAddressExitHints[address] = dnsExitHint{
		client: bad, affinityName: affinityName, createTime: now,
	}
	parent.dnsExitHints[affinityName] = dnsExitHint{
		client: clean, affinityName: affinityName, createTime: now,
	}

	path := destinationServiceTestPath(4, address.String(), 443)
	update := newMultiClientChannelUpdate(context.Background(), path)
	defer update.Close()
	parent.stateLock.Lock()
	bound := parent.inheritDnsExitHintWithLock(update, path, []*IpPath{{ServerName: affinityName}})
	parent.stateLock.Unlock()
	if !bound || update.client.Load() != clean {
		t.Fatal("fresh Bloomberg handshake followed the reputation-rejected DNS donor")
	}
}

func TestFreshQualityMediaPrefersMeasuredFastSameNetworkExit(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	public := reputationTestClient(DestinationStats{EstimatedBytesPerSecond: 20_000_000, Tier: 0})
	peer := reputationTestClient(DestinationStats{
		EstimatedBytesPerSecond: fastSameNetworkBytesPerSecond,
		Tier:                    0,
		NetworkOnly:             true,
	})
	path := &IpPath{DestinationIp: net.ParseIP("198.51.100.42"), DestinationPort: 443}

	got := parent.preferFastSameNetworkCandidates([]*multiClientChannel{public, peer}, path, "")
	if len(got) != 1 || got[0] != peer {
		t.Fatal("measured >=40 Mbit/s same-network exit did not win the fresh media field")
	}

	peer.args.EstimatedBytesPerSecond = fastSameNetworkBytesPerSecond - 1
	got = parent.preferFastSameNetworkCandidates([]*multiClientChannel{public, peer}, path, "")
	if len(got) != 2 {
		t.Fatal("an under-threshold same-network exit displaced the ordinary quality field")
	}

	peer.args.EstimatedBytesPerSecond = fastSameNetworkBytesPerSecond
	nonMedia := &IpPath{DestinationIp: net.ParseIP("198.51.100.42"), DestinationPort: 5000}
	got = parent.preferFastSameNetworkCandidates([]*multiClientChannel{public, peer}, nonMedia, "")
	if len(got) != 2 {
		t.Fatal("same-network media preference leaked into an unclassified non-web flow")
	}
}

func TestFreshQualitySameNetworkPreferenceAllocatesNothing(t *testing.T) {
	parent := reputationTestParent("www.bloomberg.com")
	public := reputationTestClient(DestinationStats{EstimatedBytesPerSecond: 20_000_000, Tier: 0})
	peer := reputationTestClient(DestinationStats{
		EstimatedBytesPerSecond: fastSameNetworkBytesPerSecond,
		Tier:                    0,
		NetworkOnly:             true,
	})
	storage := [2]*multiClientChannel{}
	path := &IpPath{DestinationIp: net.ParseIP("198.51.100.42"), DestinationPort: 443}
	allocs := testing.AllocsPerRun(100, func() {
		storage[0], storage[1] = public, peer
		if got := parent.preferFastSameNetworkCandidates(storage[:], path, ""); len(got) != 1 || got[0] != peer {
			t.Fatalf("preferred field=%v, want same-network provider", got)
		}
	})
	if allocs != 0 {
		t.Fatalf("fresh same-network preference allocated %.2f objects/run, want 0", allocs)
	}
}

func TestClassifierStreamingUsesQualityWindowAndFastPeerOffPort443(t *testing.T) {
	parent := reputationTestParent("media.example.test")
	parent.generator = &testingEmptyMultiClientGenerator{}
	parent.SetFlowClassifier(fixedClassifier{class: ClassStreaming})
	path := &IpPath{DestinationIp: net.ParseIP("198.51.100.43"), DestinationPort: 8444}
	packet := &parsedPacket{ipPath: path}
	if got := parent.selectWindowTypes(packet, "video-app"); !slices.Equal(got, []WindowType{WindowTypeQuality, WindowTypeSpeed}) {
		t.Fatalf("streaming windows=%v, want quality then speed", got)
	}

	public := reputationTestClient(DestinationStats{EstimatedBytesPerSecond: 20_000_000})
	peer := reputationTestClient(DestinationStats{
		EstimatedBytesPerSecond: fastSameNetworkBytesPerSecond,
		NetworkOnly:             true,
	})
	got := parent.preferFastSameNetworkCandidates([]*multiClientChannel{public, peer}, path, "video-app")
	if len(got) != 1 || got[0] != peer {
		t.Fatal("classifier-identified off-port streaming did not prefer the fast same-network exit")
	}
}
