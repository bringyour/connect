package connect

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sync"
	"testing"
	"time"

	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/layers"

	"github.com/urnetwork/connect/v2026/protocol"
)

func TestBlockActionMatcher(t *testing.T) {
	exactOverride := &BlockActionOverride{
		OverrideId:    NewId(),
		Hosts:         []string{"Example.com "},
		BlockOverride: &BlockOverride{Block: true},
	}
	wildcardOverride := &BlockActionOverride{
		OverrideId:    NewId(),
		Hosts:         []string{"*.tracker.net"},
		BlockOverride: &BlockOverride{Block: true},
	}
	wildcardBaseOverride := &BlockActionOverride{
		OverrideId:    NewId(),
		Hosts:         []string{"**.ads.io"},
		BlockOverride: &BlockOverride{Block: true},
	}
	subnetOverride := &BlockActionOverride{
		OverrideId:    NewId(),
		Hosts:         []string{"10.9.0.0/16"},
		RouteOverride: &RouteOverride{Local: true},
	}
	addrOverride := &BlockActionOverride{
		OverrideId:    NewId(),
		Hosts:         []string{"1.2.3.4"},
		RouteOverride: &RouteOverride{Local: true},
	}
	// the v6 forms of the subnet and exact-ip rules
	subnet6Override := &BlockActionOverride{
		OverrideId:    NewId(),
		Hosts:         []string{"2001:db8:9::/48"},
		RouteOverride: &RouteOverride{Local: true},
	}
	addr6Override := &BlockActionOverride{
		OverrideId:    NewId(),
		Hosts:         []string{"2001:DB8::1:2:3:4"},
		RouteOverride: &RouteOverride{Local: true},
	}

	matcher := newBlockActionMatcher([]*BlockActionOverride{
		exactOverride,
		wildcardOverride,
		wildcardBaseOverride,
		subnetOverride,
		addrOverride,
		subnet6Override,
		addr6Override,
	})

	match := func(addr string, serverNames ...string) *blockActionMatch {
		m := &blockActionMatch{}
		matcher.matchAddr(m, netip.MustParseAddr(addr), serverNames)
		return m
	}

	// exact host, case and space normalized
	m := match("9.9.9.9", "example.com")
	AssertEqual(t, true, m.blockOverride != nil)
	AssertEqual(t, exactOverride.OverrideId, m.blockOverrideId)
	// exact host does not match subdomains
	AssertEqual(t, false, match("9.9.9.9", "sub.example.com").any())

	// *.h matches subdomains only
	AssertEqual(t, true, match("9.9.9.9", "a.tracker.net").any())
	AssertEqual(t, true, match("9.9.9.9", "A.B.tracker.net").any())
	AssertEqual(t, false, match("9.9.9.9", "tracker.net").any())

	// **.h matches the base and subdomains
	AssertEqual(t, true, match("9.9.9.9", "ads.io").any())
	AssertEqual(t, true, match("9.9.9.9", "cdn.ads.io").any())

	// subnet
	m = match("10.9.42.1")
	AssertEqual(t, true, m.routeOverride != nil)
	AssertEqual(t, subnetOverride.OverrideId, m.routeOverrideId)
	AssertEqual(t, false, match("10.8.0.1").any())

	// exact ip
	AssertEqual(t, true, match("1.2.3.4").any())
	AssertEqual(t, false, match("1.2.3.5").any())

	// v6 subnet: inside the /48 matches, the neighboring /48 does not
	m = match("2001:db8:9:42::1")
	AssertEqual(t, true, m.routeOverride != nil)
	AssertEqual(t, subnet6Override.OverrideId, m.routeOverrideId)
	AssertEqual(t, false, match("2001:db8:8::1").any())

	// v6 exact ip, case normalized; a v4 rule never matches a v6 address
	// and the v6 rules never match v4
	AssertEqual(t, true, match("2001:db8::1:2:3:4").any())
	AssertEqual(t, false, match("2001:db8::1:2:3:5").any())
	AssertEqual(t, false, match("::ffff:1.2.3.5").any())
	m = match("2001:db8::1:2:3:4")
	AssertEqual(t, true, m.matchedIps[netip.MustParseAddr("2001:db8::1:2:3:4")])

	// B1: the exact host and ip that triggered a match are recorded (original case),
	// so the UI can render which cluster members an override rule hit
	m = match("1.2.3.4", "example.com")
	AssertEqual(t, true, m.matchedIps[netip.MustParseAddr("1.2.3.4")])
	AssertEqual(t, true, m.matchedHosts["example.com"])
	// a wildcard match records the matched subdomain, not the pattern; the ip here
	// matches nothing, so no ip is recorded
	m = match("9.9.9.9", "a.tracker.net")
	AssertEqual(t, true, m.matchedHosts["a.tracker.net"])
	AssertEqual(t, 0, len(m.matchedIps))
	// a non-matching name/ip records nothing
	m = match("5.5.5.5", "unmatched.example")
	AssertEqual(t, 0, len(m.matchedHosts))
	AssertEqual(t, 0, len(m.matchedIps))

	// no overrides compiles to nil
	AssertEqual(t, true, newBlockActionMatcher(nil) == nil)
}

func TestBlockActionMatchMerge(t *testing.T) {
	blockTrue := &BlockActionOverride{
		OverrideId:    NewId(),
		BlockOverride: &BlockOverride{Block: true},
	}
	blockFalse := &BlockActionOverride{
		OverrideId:    NewId(),
		BlockOverride: &BlockOverride{Block: false},
	}
	localTrue := &BlockActionOverride{
		OverrideId:    NewId(),
		RouteOverride: &RouteOverride{Local: true},
	}
	localFalse := &BlockActionOverride{
		OverrideId:    NewId(),
		RouteOverride: &RouteOverride{Local: false},
	}

	// block=true wins over block=false, in either order
	m := &blockActionMatch{}
	m.merge(blockFalse)
	m.merge(blockTrue)
	AssertEqual(t, true, m.blockOverride.Block)
	AssertEqual(t, blockTrue.OverrideId, m.blockOverrideId)

	m = &blockActionMatch{}
	m.merge(blockTrue)
	m.merge(blockFalse)
	AssertEqual(t, true, m.blockOverride.Block)
	AssertEqual(t, blockTrue.OverrideId, m.blockOverrideId)

	// local=true wins over local=false, in either order
	m = &blockActionMatch{}
	m.merge(localFalse)
	m.merge(localTrue)
	AssertEqual(t, true, m.routeOverride.Local)
	AssertEqual(t, localTrue.OverrideId, m.routeOverrideId)

	m = &blockActionMatch{}
	m.merge(localTrue)
	m.merge(localFalse)
	AssertEqual(t, true, m.routeOverride.Local)
	AssertEqual(t, localTrue.OverrideId, m.routeOverrideId)
}

func TestBlockActionApply(t *testing.T) {
	blockTrue := &blockActionMatch{blockOverride: &BlockOverride{Block: true}}
	blockFalse := &blockActionMatch{blockOverride: &BlockOverride{Block: false}}
	localTrue := &blockActionMatch{routeOverride: &RouteOverride{Local: true}}
	localFalse := &blockActionMatch{routeOverride: &RouteOverride{Local: false}}
	blockFalseLocalFalse := &blockActionMatch{
		blockOverride: &BlockOverride{Block: false},
		routeOverride: &RouteOverride{Local: false},
	}
	blockTrueLocalTrue := &blockActionMatch{
		blockOverride: &BlockOverride{Block: true},
		routeOverride: &RouteOverride{Local: true},
	}

	type applyCase struct {
		r      SecurityPolicyResult
		bypass bool
		match  *blockActionMatch
		block  bool
		local  bool
	}
	cases := []applyCase{
		// defaults with no overrides
		{SecurityPolicyResultAllow, false, nil, false, false},
		{SecurityPolicyResultAllow, true, nil, false, false},
		{SecurityPolicyResultDrop, false, nil, true, false},
		{SecurityPolicyResultDrop, true, nil, false, true},
		{SecurityPolicyResultIncident, true, nil, true, false},
		// block override
		{SecurityPolicyResultAllow, false, blockTrue, true, false},
		{SecurityPolicyResultDrop, true, blockTrue, true, false},
		// un-blocked drop traffic never egresses. it routes local
		{SecurityPolicyResultDrop, false, blockFalse, false, true},
		{SecurityPolicyResultDrop, false, blockFalseLocalFalse, false, true},
		// route override
		{SecurityPolicyResultAllow, false, localTrue, false, true},
		{SecurityPolicyResultDrop, true, localFalse, false, true},
		// block wins over local
		{SecurityPolicyResultAllow, false, blockTrueLocalTrue, true, false},
		// incident is not overridable
		{SecurityPolicyResultIncident, false, blockFalse, true, false},
		{SecurityPolicyResultIncident, false, localTrue, true, false},
	}
	for i, c := range cases {
		block, local := blockActionApply(c.r, c.bypass, false, c.match)
		AssertEqual(t, c.block, block)
		AssertEqual(t, c.local, local)
		if c.block != block || c.local != local {
			t.Logf("case %d failed: %+v", i, c)
		}
	}
}

func TestBlockActionCollector(t *testing.T) {
	collector := newBlockActionCollector(8, nil)

	AssertEqual(t, false, collector.hasCallbacks())

	var flushed [][]*BlockAction
	unsub := collector.addCallback(func(blockActions []*BlockAction) {
		flushed = append(flushed, blockActions)
	})
	AssertEqual(t, true, collector.hasCallbacks())

	a := netip.MustParseAddr("1.0.0.1")
	b := netip.MustParseAddr("1.0.0.2")
	overrideId := NewId()
	match := &blockActionMatch{
		blockOverride:   &BlockOverride{Block: true},
		blockOverrideId: overrideId,
	}
	// a cluster of two ips. decisions on either member aggregate into one action
	clusterDecision := &blockActionDecision{
		clusterKey:   a,
		clusterIps:   []netip.Addr{a, b},
		clusterHosts: []string{"example.com"},
	}
	otherDecision := &blockActionDecision{
		clusterKey: b,
		clusterIps: []netip.Addr{b},
	}

	// two packets aggregate into one action per key
	collector.add(clusterDecision, true, false, match, 100)
	collector.add(clusterDecision, true, false, match, 50)
	// a different decision for the same cluster is a separate action
	collector.add(clusterDecision, false, false, nil, 25)
	collector.add(otherDecision, false, true, nil, 10)

	collector.flush()
	AssertEqual(t, 1, len(flushed))
	blockActions := flushed[0]
	AssertEqual(t, 3, len(blockActions))

	byKey := map[string]*BlockAction{}
	for _, blockAction := range blockActions {
		byKey[fmt.Sprintf("%s-%t-%t", blockAction.Ips[0], blockAction.Block, blockAction.Local)] = blockAction
	}
	blocked := byKey["1.0.0.1-true-false"]
	AssertEqual(t, 2, blocked.PacketCount)
	AssertEqual(t, ByteCount(150), blocked.ByteCount)
	// the cluster action carries all the cluster ips and hosts
	AssertEqual(t, []netip.Addr{a, b}, blocked.Ips)
	AssertEqual(t, []string{"example.com"}, blocked.Hosts)
	AssertEqual(t, true, blocked.BlockOverrideId != nil)
	AssertEqual(t, overrideId, *blocked.BlockOverrideId)
	AssertEqual(t, true, blocked.RouteOverrideId == nil)

	localAction := byKey["1.0.0.2-false-true"]
	AssertEqual(t, []netip.Addr{b}, localAction.Ips)
	AssertEqual(t, 0, len(localAction.Hosts))

	// the epoch was drained
	collector.flush()
	AssertEqual(t, 1, len(flushed))

	unsub()
	AssertEqual(t, false, collector.hasCallbacks())
}

// TestBlockActionCollectorMatchedDisjoint (B1): when an override hit specific
// cluster members, the flushed action reports them in MatchedHosts/MatchedIps and
// EXCLUDES them from Hosts/Ips, so nothing is shown or counted twice.
func TestBlockActionCollectorMatchedDisjoint(t *testing.T) {
	collector := newBlockActionCollector(8, nil)
	var flushed [][]*BlockAction
	collector.addCallback(func(blockActions []*BlockAction) {
		flushed = append(flushed, blockActions)
	})

	a := netip.MustParseAddr("3.0.0.1")
	b := netip.MustParseAddr("3.0.0.2")
	c := netip.MustParseAddr("3.0.0.3")
	overrideId := NewId()
	// an override hit ip b and host ads.example.com within the cluster
	match := &blockActionMatch{
		blockOverride:   &BlockOverride{Block: true},
		blockOverrideId: overrideId,
		matchedIps:      map[netip.Addr]bool{b: true},
		matchedHosts:    map[string]bool{"ads.example.com": true},
	}
	decision := &blockActionDecision{
		clusterKey:   a,
		clusterIps:   []netip.Addr{a, b, c},
		clusterHosts: []string{"example.com", "ads.example.com", "cdn.other.com"},
	}
	collector.add(decision, true, false, match, 100)
	collector.flush()

	AssertEqual(t, 1, len(flushed))
	AssertEqual(t, 1, len(flushed[0]))
	ba := flushed[0][0]

	// the matched sets carry exactly what the override hit
	AssertEqual(t, []netip.Addr{b}, ba.MatchedIps)
	AssertEqual(t, []string{"ads.example.com"}, ba.MatchedHosts)
	// the rest are disjoint: Hosts/Ips exclude the matched members
	AssertEqual(t, []netip.Addr{a, c}, ba.Ips)
	AssertEqual(t, []string{"example.com", "cdn.other.com"}, ba.Hosts)

	// disjointness: no member appears in both sets
	matchedIp := map[netip.Addr]bool{}
	for _, ip := range ba.MatchedIps {
		matchedIp[ip] = true
	}
	for _, ip := range ba.Ips {
		AssertEqual(t, false, matchedIp[ip])
	}
	matchedHost := map[string]bool{}
	for _, host := range ba.MatchedHosts {
		matchedHost[host] = true
	}
	for _, host := range ba.Hosts {
		AssertEqual(t, false, matchedHost[host])
	}
}

// TestBlockActionCollectorHostsAdopted: when an aggregate is seeded by an
// empty-hosts decision and a later same-key decision in the same epoch carries
// hosts, the flushed action adopts the hosts (so a name learned mid-epoch is
// reported, not masked by the first ip-only decision).
func TestBlockActionCollectorHostsAdopted(t *testing.T) {
	collector := newBlockActionCollector(8, nil)
	var flushed [][]*BlockAction
	collector.addCallback(func(blockActions []*BlockAction) {
		flushed = append(flushed, blockActions)
	})

	a := netip.MustParseAddr("2.0.0.1")
	emptyHosts := &blockActionDecision{clusterKey: a, clusterIps: []netip.Addr{a}}
	named := &blockActionDecision{clusterKey: a, clusterIps: []netip.Addr{a}, clusterHosts: []string{"pbs.com"}}

	// ip-only decision first, then the named decision for the same cluster/key
	collector.add(emptyHosts, true, false, nil, 10)
	collector.add(named, true, false, nil, 20)

	collector.flush()
	AssertEqual(t, 1, len(flushed))
	AssertEqual(t, 1, len(flushed[0]))
	action := flushed[0][0]
	AssertEqual(t, 2, action.PacketCount)
	AssertEqual(t, []string{"pbs.com"}, action.Hosts)

	// the reverse (named first) already reports hosts and must not be cleared by
	// a later empty-hosts decision
	collector.add(named, true, false, nil, 5)
	collector.add(emptyHosts, true, false, nil, 5)
	collector.flush()
	AssertEqual(t, 2, len(flushed))
	AssertEqual(t, []string{"pbs.com"}, flushed[1][0].Hosts)
}

// a security policy with a fixed egress result
type testingFixedSecurityPolicy struct {
	stats  *SecurityPolicyStatsCollector
	result SecurityPolicyResult
}

func (self *testingFixedSecurityPolicy) Stats() *SecurityPolicyStatsCollector {
	return self.stats
}

func (self *testingFixedSecurityPolicy) InspectEgress(provideMode protocol.ProvideMode, ipPath *IpPath, payload []byte) (SecurityPolicyResult, error) {
	return self.result, nil
}

func (self *testingFixedSecurityPolicy) InspectIngress(provideMode protocol.ProvideMode, ipPath *IpPath, payload []byte) (SecurityPolicyResult, error) {
	return SecurityPolicyResultAllow, nil
}

func (self *testingFixedSecurityPolicy) RefreshEgress(ipPath *IpPath) {
}

func (self *testingFixedSecurityPolicy) RefreshIngress(ipPath *IpPath) {
}

// a generator with no available destinations
type testingEmptyMultiClientGenerator struct {
}

func (self *testingEmptyMultiClientGenerator) NextDestinations(count int, excludeDestinations []MultiHopId, rankMode string) (map[MultiHopId]DestinationStats, error) {
	return map[MultiHopId]DestinationStats{}, nil
}

func (self *testingEmptyMultiClientGenerator) NewClientArgs() (*MultiClientGeneratorClientArgs, error) {
	return nil, fmt.Errorf("no clients")
}

func (self *testingEmptyMultiClientGenerator) RemoveClientArgs(args *MultiClientGeneratorClientArgs) {
}

func (self *testingEmptyMultiClientGenerator) RemoveClientWithArgs(client *Client, args *MultiClientGeneratorClientArgs) {
}

func (self *testingEmptyMultiClientGenerator) NewClientSettings() *ClientSettings {
	return DefaultClientSettings()
}

func (self *testingEmptyMultiClientGenerator) NewClient(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error) {
	return nil, fmt.Errorf("no clients")
}

func (self *testingEmptyMultiClientGenerator) FixedDestinationSize() (int, bool) {
	return 0, false
}

func testingUdp4Packet(sourceIp string, destinationIp string, destinationPort int, payload []byte) []byte {
	ip := &layers.IPv4{
		Version:  4,
		TTL:      64,
		SrcIP:    net.ParseIP(sourceIp).To4(),
		DstIP:    net.ParseIP(destinationIp).To4(),
		Protocol: layers.IPProtocolUDP,
	}
	udp := &layers.UDP{
		SrcPort: layers.UDPPort(40000),
		DstPort: layers.UDPPort(destinationPort),
	}
	udp.SetNetworkLayerForChecksum(ip)
	buffer := gopacket.NewSerializeBuffer()
	err := gopacket.SerializeLayers(
		buffer,
		gopacket.SerializeOptions{ComputeChecksums: true, FixLengths: true},
		ip,
		udp,
		gopacket.Payload(payload),
	)
	if err != nil {
		panic(err)
	}
	packet := make([]byte, len(buffer.Bytes()))
	copy(packet, buffer.Bytes())
	return packet
}

// testBlockActionSourceIp is the tunnel-side source of the crafted packets.
func testBlockActionSourceIp(ipVersion int) string {
	if ipVersion == 4 {
		return "10.0.0.5"
	}
	return "fd00::5"
}

// testBlockActionLoopbackSubnet is a prefix that contains the family's
// loopback, for the subnet forms of the override and ignore rules.
func testBlockActionLoopbackSubnet(ipVersion int) string {
	if ipVersion == 4 {
		return "127.0.0.0/8"
	}
	return "::/96"
}

// newBlockActionTestMultiClient is a multi client over an empty generator
// whose security policy always drops, so every crafted packet reaches the
// block-action decision.
func newBlockActionTestMultiClient(ctx context.Context) (*RemoteUserNatMultiClient, *MultiClientSettings) {
	securityPolicy := &testingFixedSecurityPolicy{
		stats:  DefaultSecurityPolicyStatsCollector(),
		result: SecurityPolicyResultDrop,
	}

	settings := DefaultMultiClientSettings()
	settings.EventEpoch = 20 * time.Millisecond
	settings.SecurityPolicyGenerator = func(ctx context.Context, stats *SecurityPolicyStatsCollector) SecurityPolicy {
		return securityPolicy
	}

	multiClient := NewRemoteUserNatMultiClient(
		ctx,
		&testingEmptyMultiClientGenerator{},
		func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		},
		protocol.ProvideMode_Network,
		settings,
	)
	return multiClient, settings
}

func TestMultiClientBlockActionOverrides(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		multiClient, _ := newBlockActionTestMultiClient(ctx)
		defer multiClient.Close()

		source := SourceId(NewId())

		blockActionsChannel := make(chan []*BlockAction, 16)
		unsub := multiClient.AddBlockActionCallback(func(blockActions []*BlockAction) {
			blockActionsChannel <- blockActions
		})
		defer unsub()

		nextBlockActions := func() []*BlockAction {
			select {
			case blockActions := <-blockActionsChannel:
				return blockActions
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for block actions")
				return nil
			}
		}

		loopback := testLoopbackIp(ipVersion)

		// drop policy, no bypass, no overrides -> blocked
		packet := testingUdpPacket(ipVersion, testBlockActionSourceIp(ipVersion), loopback, 9, []byte("hello"))
		success := multiClient.SendPacket(source, protocol.ProvideMode_Network, packet, 0)
		AssertEqual(t, false, success)

		blockActions := nextBlockActions()
		AssertEqual(t, 1, len(blockActions))
		AssertEqual(t, true, blockActions[0].Block)
		AssertEqual(t, false, blockActions[0].Local)
		AssertEqual(t, true, blockActions[0].BlockOverrideId == nil)
		AssertEqual(t, 1, blockActions[0].PacketCount)

		packetStats := multiClient.PacketStats()
		AssertEqual(t, int64(1), packetStats.BlockEgressPacketCount)
		AssertEqual(t, ByteCount(len(packet)), packetStats.BlockEgressByteCount)

		// an un-block override routes the drop-classified traffic locally, never egress
		unblockOverride := &BlockActionOverride{
			OverrideId:    NewId(),
			Hosts:         []string{loopback},
			BlockOverride: &BlockOverride{Block: false},
		}
		multiClient.SetBlockActionOverrides([]*BlockActionOverride{unblockOverride})

		success = multiClient.SendPacket(source, protocol.ProvideMode_Network, packet, 0)
		AssertEqual(t, true, success)

		blockActions = nextBlockActions()
		AssertEqual(t, 1, len(blockActions))
		AssertEqual(t, false, blockActions[0].Block)
		AssertEqual(t, true, blockActions[0].Local)
		AssertEqual(t, true, blockActions[0].BlockOverrideId != nil)
		AssertEqual(t, unblockOverride.OverrideId, *blockActions[0].BlockOverrideId)

		packetStats = multiClient.PacketStats()
		AssertEqual(t, int64(1), packetStats.LocalEgressPacketCount)
		AssertEqual(t, int64(1), packetStats.BlockEgressPacketCount)

		// with bypass on, a block override blocks traffic that would route local
		multiClient.SetLocalSecurityBypass(true)
		blockOverride := &BlockActionOverride{
			OverrideId:    NewId(),
			Hosts:         []string{testBlockActionLoopbackSubnet(ipVersion)},
			BlockOverride: &BlockOverride{Block: true},
		}
		multiClient.SetBlockActionOverrides([]*BlockActionOverride{blockOverride})

		success = multiClient.SendPacket(source, protocol.ProvideMode_Network, packet, 0)
		AssertEqual(t, false, success)

		blockActions = nextBlockActions()
		AssertEqual(t, true, blockActions[0].Block)
		AssertEqual(t, blockOverride.OverrideId, *blockActions[0].BlockOverrideId)

		// packet stats listener fires on the epoch with the cumulative counts
		packetStatsChannel := make(chan *PacketStats, 16)
		unsubPacketStats := multiClient.AddPacketStatsCallback(func(packetStats *PacketStats) {
			packetStatsChannel <- packetStats
		})
		defer unsubPacketStats()

		multiClient.SetBlockActionOverrides(nil)
		success = multiClient.SendPacket(source, protocol.ProvideMode_Network, packet, 0)
		AssertEqual(t, true, success)

		select {
		case packetStats = <-packetStatsChannel:
			AssertEqual(t, int64(2), packetStats.LocalEgressPacketCount)
			AssertEqual(t, int64(2), packetStats.BlockEgressPacketCount)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for packet stats")
		}
	})
}

func TestMultiClientBlockActionIgnoreHosts(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		multiClient, settings := newBlockActionTestMultiClient(ctx)
		defer multiClient.Close()

		source := SourceId(NewId())

		blockActionsChannel := make(chan []*BlockAction, 16)
		unsub := multiClient.AddBlockActionCallback(func(blockActions []*BlockAction) {
			blockActionsChannel <- blockActions
		})
		defer unsub()

		nextBlockActions := func() []*BlockAction {
			select {
			case blockActions := <-blockActionsChannel:
				return blockActions
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for block actions")
				return nil
			}
		}

		loopback := testLoopbackIp(ipVersion)

		// an ignored destination is excluded from the override logic:
		// the un-block override must not match, so the drop policy blocks
		unblockOverride := &BlockActionOverride{
			OverrideId:    NewId(),
			Hosts:         []string{loopback},
			BlockOverride: &BlockOverride{Block: false},
		}
		multiClient.SetBlockActionOverrides([]*BlockActionOverride{unblockOverride})
		multiClient.SetBlockActionIgnoreHosts([]string{loopback})

		packet := testingUdpPacket(ipVersion, testBlockActionSourceIp(ipVersion), loopback, 9, []byte("hello"))
		success := multiClient.SendPacket(source, protocol.ProvideMode_Network, packet, 0)
		AssertEqual(t, false, success)

		// no block action is surfaced for the ignored destination
		select {
		case blockActions := <-blockActionsChannel:
			t.Fatalf("expected no block actions for the ignored destination, got %d", len(blockActions))
		case <-time.After(4 * settings.EventEpoch):
		}

		// the default decision and packet stats still apply
		packetStats := multiClient.PacketStats()
		AssertEqual(t, int64(1), packetStats.BlockEgressPacketCount)

		// clearing the ignore list restores the override match and the block actions
		multiClient.SetBlockActionIgnoreHosts(nil)

		success = multiClient.SendPacket(source, protocol.ProvideMode_Network, packet, 0)
		AssertEqual(t, true, success)

		blockActions := nextBlockActions()
		AssertEqual(t, 1, len(blockActions))
		AssertEqual(t, false, blockActions[0].Block)
		AssertEqual(t, true, blockActions[0].Local)
		AssertEqual(t, true, blockActions[0].BlockOverrideId != nil)

		// ignore by subnet also excludes the destination
		multiClient.SetBlockActionIgnoreHosts([]string{testBlockActionLoopbackSubnet(ipVersion)})

		success = multiClient.SendPacket(source, protocol.ProvideMode_Network, packet, 0)
		AssertEqual(t, false, success)

		select {
		case blockActions := <-blockActionsChannel:
			t.Fatalf("expected no block actions for the ignored subnet, got %d", len(blockActions))
		case <-time.After(4 * settings.EventEpoch):
		}
	})
}

// testHardCodedRemoteDohIps are the DefaultDnsResolverSettings remote doh
// resolver ips of the family, and a same-operator neighbor that is not in
// the baseline.
func testHardCodedRemoteDohIps(ipVersion int) (hardCodedIps []string, controlIp string) {
	if ipVersion == 4 {
		return []string{"1.1.1.1", "8.8.8.8", "9.9.9.9", "208.67.222.222"}, "1.0.0.1"
	}
	return []string{"2606:4700:4700::1111", "2001:4860:4860::8888", "2620:fe::fe", "2620:119:35::35"}, "2606:4700:4700::1001"
}

// TestMultiClientBlockActionDefaultRemoteDohIgnored verifies the built-in
// ignore baseline: the hard coded remote doh resolver ips are excluded from
// override decisions with NO SetBlockActionIgnoreHosts wiring at all, while
// a neighboring non-baseline ip still matches overrides. Both families: the
// baseline carries the v6 resolver endpoints too.
func TestMultiClientBlockActionDefaultRemoteDohIgnored(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		multiClient, settings := newBlockActionTestMultiClient(ctx)
		defer multiClient.Close()

		source := SourceId(NewId())

		blockActionsChannel := make(chan []*BlockAction, 16)
		unsub := multiClient.AddBlockActionCallback(func(blockActions []*BlockAction) {
			blockActionsChannel <- blockActions
		})
		defer unsub()

		// an un-block override covering the hard coded resolver ips and a
		// non-baseline control ip. note: no SetBlockActionIgnoreHosts call.
		hardCodedIps, controlIp := testHardCodedRemoteDohIps(ipVersion)
		unblockOverride := &BlockActionOverride{
			OverrideId:    NewId(),
			Hosts:         append(append([]string{}, hardCodedIps...), controlIp),
			BlockOverride: &BlockOverride{Block: false},
		}
		multiClient.SetBlockActionOverrides([]*BlockActionOverride{unblockOverride})

		// the baseline ignores the override for every hard coded resolver ip:
		// the drop policy blocks, and no block action is surfaced
		for _, ip := range hardCodedIps {
			packet := testingUdpPacket(ipVersion, testBlockActionSourceIp(ipVersion), ip, 9, []byte("hello"))
			success := multiClient.SendPacket(source, protocol.ProvideMode_Network, packet, 0)
			AssertEqual(t, false, success)
		}
		select {
		case blockActions := <-blockActionsChannel:
			t.Fatalf("expected no block actions for the hard coded resolver ips, got %d", len(blockActions))
		case <-time.After(4 * settings.EventEpoch):
		}

		// the control ip is not in the baseline: the override applies
		packet := testingUdpPacket(ipVersion, testBlockActionSourceIp(ipVersion), controlIp, 9, []byte("hello"))
		success := multiClient.SendPacket(source, protocol.ProvideMode_Network, packet, 0)
		AssertEqual(t, true, success)

		select {
		case blockActions := <-blockActionsChannel:
			AssertEqual(t, 1, len(blockActions))
			AssertEqual(t, false, blockActions[0].Block)
			AssertEqual(t, true, blockActions[0].BlockOverrideId != nil)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for the control ip block action")
		}
	})
}

// testingLearningServerNameLookup is a ServerNameLookup that can learn names at runtime and
// notify learned subscribers, mirroring the UpgradeMux reverse index it stands in for
// (it implements ServerNamesLearnedNotifier).
type testingLearningServerNameLookup struct {
	stateLock sync.Mutex
	names     map[string][]string
	learned   *CallbackList[ServerNamesLearnedFunction]
}

func newTestingLearningServerNameLookup() *testingLearningServerNameLookup {
	return &testingLearningServerNameLookup{
		names:   map[string][]string{},
		learned: NewCallbackList[ServerNamesLearnedFunction](),
	}
}

func (self *testingLearningServerNameLookup) ServerNames(ip string) []string {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return append([]string{}, self.names[ip]...)
}

func (self *testingLearningServerNameLookup) AddServerNamesLearnedCallback(callback ServerNamesLearnedFunction) func() {
	callbackId := self.learned.Add(callback)
	return func() {
		self.learned.Remove(callbackId)
	}
}

// learn records a server name for an ip and fires the learned notification, the way a
// DoH resolution does in the mux.
func (self *testingLearningServerNameLookup) learn(ip string, name string) {
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		self.names[ip] = append(self.names[ip], name)
	}()
	addr := netip.MustParseAddr(ip)
	for _, callback := range self.learned.Get() {
		callback([]netip.Addr{addr})
	}
}

// TestMultiClientBlockActionServerNames verifies block-action events report the server
// name instead of the ip once the name is learned. A name learned after a decision is
// cached invalidates that decision (via the learned notification), so the next flow to
// the same ip rebuilds it and its event carries the host.
func TestMultiClientBlockActionServerNames(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		multiClient, _ := newBlockActionTestMultiClient(ctx)
		defer multiClient.Close()

		lookup := newTestingLearningServerNameLookup()
		multiClient.SetServerNameLookup(lookup)

		source := SourceId(NewId())

		blockActionsChannel := make(chan []*BlockAction, 16)
		unsub := multiClient.AddBlockActionCallback(func(blockActions []*BlockAction) {
			blockActionsChannel <- blockActions
		})
		defer unsub()

		nextBlockActions := func() []*BlockAction {
			select {
			case blockActions := <-blockActionsChannel:
				return blockActions
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for block actions")
				return nil
			}
		}

		dstIp := testExampleComIp(ipVersion)
		dstAddr := netip.MustParseAddr(dstIp)
		packet := testingUdpPacket(ipVersion, testBlockActionSourceIp(ipVersion), dstIp, 9, []byte("hello"))

		// before the name is known, the event reports the ip and no host
		success := multiClient.SendPacket(source, protocol.ProvideMode_Network, packet, 0)
		AssertEqual(t, false, success)
		blockActions := nextBlockActions()
		AssertEqual(t, 1, len(blockActions))
		AssertEqual(t, []netip.Addr{dstAddr}, blockActions[0].Ips)
		AssertEqual(t, 0, len(blockActions[0].Hosts))

		// learning the name fires the learned notification, invalidating the cached
		// decision for that ip
		lookup.learn(dstIp, "example.com")

		// the next flow to the same ip now reports the server name
		success = multiClient.SendPacket(source, protocol.ProvideMode_Network, packet, 0)
		AssertEqual(t, false, success)
		blockActions = nextBlockActions()
		AssertEqual(t, 1, len(blockActions))
		AssertEqual(t, []netip.Addr{dstAddr}, blockActions[0].Ips)
		AssertEqual(t, []string{"example.com"}, blockActions[0].Hosts)
	})
}
