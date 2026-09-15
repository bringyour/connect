package connect

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"
)

func lateNamePath(sourcePort int) *IpPath {
	return &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        net.ParseIP("10.11.12.13"),
		SourcePort:      sourcePort,
		DestinationIp:   net.ParseIP("93.184.216.34"),
		DestinationPort: 443,
	}
}

func lateNameClient(ctx context.Context, lookup ServerNameLookup, bridge bool) *RemoteUserNatMultiClient {
	mc := &RemoteUserNatMultiClient{
		ctx: ctx,
		settings: &MultiClientSettings{
			DestinationAffinity:      true,
			FreshFlowAffinity:        true,
			ServerNameAffinityBridge: bridge,
			SequenceIdleTimeout:      10 * time.Minute,
			TcpSequenceIdleTimeout:   10 * time.Minute,
		},
		ip4PathUpdates:   map[Ip4Path]*multiClientChannelUpdate{},
		ip6PathUpdates:   map[Ip6Path]*multiClientChannelUpdate{},
		affinityIp4Paths: map[Ip4Path]map[Ip4Path]time.Time{},
		affinityIp6Paths: map[Ip6Path]map[Ip6Path]time.Time{},
		clientUpdates:    map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
	}
	mc.config.Store(&multiClientConfig{serverNameLookup: lookup})
	return mc
}

// THE hard constraint. Providers terminate tcp, so moving a live flow to a
// different exit breaks it -- that is the freeze this whole change exists to
// prevent. A hostname learned after a flow was established must leave that
// flow's exit, and its affinity bookkeeping, completely alone.
func TestLateServerNameEstablishedFlowKeepsItsExit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lookup := newTestingLearningServerNameLookup()
	mc := lateNameClient(ctx, lookup, true)

	clientA := &multiClientChannel{ctx: ctx}

	// flow 1 is created before any name is known, and commits to clientA the
	// way the race would have
	flow1 := lateNamePath(40000)
	update1, _, _ := mc.sendUpdate(flow1, flowPin{})
	update1.client.Store(clientA)
	flow1Key := flow1.ToIp4Path()

	// the mux learns the name from the tls ClientHello, an rtt later
	lookup.learn("93.184.216.34", "foo.com")

	// flow 2 keys on the base domain, whose group is empty
	update2, _, current2 := mc.sendUpdate(lateNamePath(40001), flowPin{})

	// it converges onto the established exit instead of racing for a new one
	AssertEqual(t, current2 == clientA, true)
	AssertEqual(t, update2.client.Load() == clientA, true)

	// flow 1 is untouched: same exit, same update, same affinity groups
	AssertEqual(t, update1.client.Load() == clientA, true)
	AssertEqual(t, mc.ip4PathUpdates[flow1Key] == update1, true)

	// specifically, flow 1 was never added to the server-name group. this is
	// what fails if anyone reimplements this by backfilling in
	// invalidateServerNames
	AssertEqual(t, update1.affinityIp4Paths[(&IpPath{ServerName: "foo.com"}).ToIp4Path()], false)
}

// the destination groups are read as donors, never joined
func TestAffinityBridgeDoesNotJoinTheDestinationGroup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lookup := newTestingLearningServerNameLookup()
	mc := lateNameClient(ctx, lookup, true)

	flow1 := lateNamePath(40000)
	update1, _, _ := mc.sendUpdate(flow1, flowPin{})
	update1.client.Store(&multiClientChannel{ctx: ctx})

	destinationKey := (&IpPath{
		Version:         4,
		DestinationIp:   net.ParseIP("93.184.216.34"),
		DestinationPort: 443,
	}).ToIp4Path()
	AssertEqual(t, len(mc.affinityIp4Paths[destinationKey]), 1)

	lookup.learn("93.184.216.34", "foo.com")
	mc.sendUpdate(lateNamePath(40001), flowPin{})

	// still just flow 1 -- the bridge consulted this group without joining it
	AssertEqual(t, len(mc.affinityIp4Paths[destinationKey]), 1)
}

// off must reproduce the previous behavior: the late-named flow finds no donor
func TestAffinityBridgeDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lookup := newTestingLearningServerNameLookup()
	mc := lateNameClient(ctx, lookup, false)

	update1, _, _ := mc.sendUpdate(lateNamePath(40000), flowPin{})
	update1.client.Store(&multiClientChannel{ctx: ctx})

	lookup.learn("93.184.216.34", "foo.com")
	_, _, current2 := mc.sendUpdate(lateNamePath(40001), flowPin{})

	AssertEqual(t, current2 == nil, true)
}

// a dying exit must not be handed to a whole site
func TestAffinityBridgeSkipsDoneDonors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lookup := newTestingLearningServerNameLookup()
	mc := lateNameClient(ctx, lookup, true)

	doneCtx, doneCancel := context.WithCancel(context.Background())
	doneCancel()

	update1, _, _ := mc.sendUpdate(lateNamePath(40000), flowPin{})
	update1.client.Store(&multiClientChannel{ctx: doneCtx})

	lookup.learn("93.184.216.34", "foo.com")
	_, _, current2 := mc.sendUpdate(lateNamePath(40001), flowPin{})

	AssertEqual(t, current2 == nil, true)
}

// ports outside the web set have port-only or global no-name keys, so bridging
// into them would pin every named site to one exit
func TestAffinityFallbackIgnoresNonWebPorts(t *testing.T) {
	mc := lateNameClient(context.Background(), nil, true)

	ipPath := lateNamePath(40000)
	ipPath.DestinationPort = 8080

	AssertEqual(t, len(mc.affinityFallbackIpPathsWithLock(ipPath)), 0)
}

// the same bare-fixture invariant the cluster fallback had to learn
func TestAffinityFallbackPathsBareClient(t *testing.T) {
	mc := &RemoteUserNatMultiClient{}
	mc.config.Store(&multiClientConfig{})

	AssertEqual(t, len(mc.affinityFallbackIpPathsWithLock(lateNamePath(40000))), 0)
}

// with clustering on, the representative is offered as a second, broader donor
// group after the exact destination
func TestAffinityFallbackUsesClusterRepresentative(t *testing.T) {
	mc := lateNameClient(context.Background(), nil, true)
	mc.settings.ClusterAffinityFallback = true

	member := netip.MustParseAddr("93.184.216.7")
	target := netip.MustParseAddr("93.184.216.34")
	ia := &IpAssoc{}
	ia.clusters.Store(&ipAssocClusters{
		members: map[netip.Addr][]netip.Addr{
			target: {target, member},
			member: {target, member},
		},
	})
	mc.ipAssoc = ia

	fallbackPaths := mc.affinityFallbackIpPathsWithLock(lateNamePath(40000))
	AssertEqual(t, len(fallbackPaths), 2)
	// most specific first
	AssertEqual(t, fallbackPaths[0].DestinationIp.String(), "93.184.216.34")
	AssertEqual(t, fallbackPaths[1].DestinationIp.String(), "93.184.216.7")
}

func TestDefaultMultiClientSettingsEnablesServerNameAffinityBridge(t *testing.T) {
	AssertEqual(t, DefaultMultiClientSettings().ServerNameAffinityBridge, true)
}
