package connect

import (
	"context"
	"math"
	"net"
	"testing"
	"time"

	// "slices"
	"sync"

	"github.com/urnetwork/connect/v2026/protocol"
)

type stubServerNameLookup struct {
	names []string
}

func (self stubServerNameLookup) ServerNames(ip string) []string {
	return self.names
}

// TestMultiClientServerNameAffinity verifies ServerName path affinity collapses to the
// base domain: a.foo.com, b.c.foo.com and foo.com all share a single foo.com path.
func TestMultiClientServerNameAffinity(t *testing.T) {
	ipPath := &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		DestinationIp:   net.ParseIP("93.184.216.34"),
		DestinationPort: 443,
	}

	// with a lookup returning sub-domains of one site, affinity is one base-domain path
	{
		mc := &RemoteUserNatMultiClient{}
		mc.config.Store(&multiClientConfig{
			serverNameLookup: stubServerNameLookup{names: []string{"a.foo.com", "b.c.foo.com", "foo.com"}},
		})
		paths := mc.affinityIpPathsWithLock(ipPath)
		if len(paths) != 1 {
			t.Fatalf("affinity paths = %d, want 1 (collapsed to base domain): %+v", len(paths), paths)
		}
		if paths[0].ServerName != "foo.com" {
			t.Fatalf("affinity ServerName = %q, want foo.com", paths[0].ServerName)
		}
	}

	// distinct sites get distinct affinity paths
	{
		mc := &RemoteUserNatMultiClient{}
		mc.config.Store(&multiClientConfig{
			serverNameLookup: stubServerNameLookup{names: []string{"a.foo.com", "x.bar.com"}},
		})
		paths := mc.affinityIpPathsWithLock(ipPath)
		names := map[string]bool{}
		for _, p := range paths {
			names[p.ServerName] = true
		}
		if !names["foo.com"] || !names["bar.com"] || len(paths) != 2 {
			t.Fatalf("affinity paths = %+v, want {foo.com, bar.com}", paths)
		}
	}

	// with no lookup, :443 falls back to destination ip/port affinity (no ServerName)
	{
		mc := &RemoteUserNatMultiClient{}
		mc.config.Store(&multiClientConfig{})
		paths := mc.affinityIpPathsWithLock(ipPath)
		if len(paths) != 1 || paths[0].ServerName != "" || !paths[0].DestinationIp.Equal(ipPath.DestinationIp) {
			t.Fatalf("affinity paths = %+v, want one destination-ip path (no lookup)", paths)
		}
	}
}

func TestMultiClientUdp4(t *testing.T) {
	testClient(t, testingNewMultiClient, udp4Packet, (*IpPath).ToIp4Path)
}

func TestMultiClientTcp4(t *testing.T) {
	testClient(t, testingNewMultiClient, tcp4Packet, (*IpPath).ToIp4Path)
}

func TestMultiClientUdp6(t *testing.T) {
	testClient(t, testingNewMultiClient, udp6Packet, (*IpPath).ToIp6Path)
}

func TestMultiClientTcp6(t *testing.T) {
	testClient(t, testingNewMultiClient, tcp6Packet, (*IpPath).ToIp6Path)
}

func testMultiClientGenerator(providerClient *Client) *TestMultiClientGenerator {
	mutex := sync.Mutex{}
	unsubs := map[*Client]func(){}

	return &TestMultiClientGenerator{
		nextDestinations: func(count int, excludeDestinations []MultiHopId, rankMode string) (map[MultiHopId]DestinationStats, error) {
			next := map[MultiHopId]DestinationStats{}
			containsTail := func() bool {
				for _, destination := range excludeDestinations {
					if 0 < destination.Len() && destination.Tail() == providerClient.ClientId() {
						return true
					}
				}
				return false
			}
			if !containsTail() {
				next[RequireMultiHopId(providerClient.ClientId())] = DestinationStats{
					EstimatedBytesPerSecond: ByteCount(0),
					Tier:                    0,
				}
			}
			return next, nil
		},
		newClientArgs: func() (*MultiClientGeneratorClientArgs, error) {
			args := &MultiClientGeneratorClientArgs{
				ClientId:   NewId(),
				ClientAuth: nil,
			}
			return args, nil
		},
		removeClientArgs: func(args *MultiClientGeneratorClientArgs) {
			// do nothing
		},
		removeClientWithArgs: func(client *Client, args *MultiClientGeneratorClientArgs) {
			var unsub func()
			var ok bool
			func() {
				mutex.Lock()
				defer mutex.Unlock()
				unsub, ok = unsubs[client]
				if ok {
					delete(unsubs, client)
				}
			}()
			if ok {
				unsub()
			}
		},
		newClientSettings: func() *ClientSettings {
			return DefaultClientSettingsWithBufferSize(testClientTransferBufferSize)
		},
		newClient: func(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error) {
			client := NewClient(ctx, args.ClientId, NewNoContractClientOob(), clientSettings)

			routeSend := make(chan []byte)
			routeReceive := make(chan []byte)

			transportSend := NewSendGatewayTransport()
			transportReceive := NewReceiveGatewayTransport()
			client.RouteManager().UpdateTransport(transportSend, []Route{routeSend})
			client.RouteManager().UpdateTransport(transportReceive, []Route{routeReceive})

			client.ContractManager().AddNoContractPeer(providerClient.ClientId())

			providerTransportSend := NewSendClientTransport(DestinationId(args.ClientId))
			providerTransportReceive := NewReceiveGatewayTransport()
			providerClient.RouteManager().UpdateTransport(providerTransportReceive, []Route{routeSend})
			providerClient.RouteManager().UpdateTransport(providerTransportSend, []Route{routeReceive})

			providerClient.ContractManager().AddNoContractPeer(client.ClientId())

			unsub := func() {
				client.RouteManager().RemoveTransport(transportSend)
				client.RouteManager().RemoveTransport(transportReceive)
				providerClient.RouteManager().RemoveTransport(providerTransportReceive)
				providerClient.RouteManager().RemoveTransport(providerTransportSend)
			}

			func() {
				mutex.Lock()
				defer mutex.Unlock()
				unsubs[client] = unsub
			}()

			return client, nil
		},
	}
}

func testingNewMultiClient(ctx context.Context, providerClient *Client, receivePacketCallback ReceivePacketFunction) (UserNatClient, error) {
	generator := testMultiClientGenerator(providerClient)

	settings := DefaultMultiClientSettings()
	// TODO the tcp packets must use real seq numbers for this to work
	settings.TcpCollapsePrevention = false
	// testClient is a Transfer/routing stress fixture: it deliberately creates
	// 48 long-lived tuples behind one synthetic exit and later verifies every
	// payload and echo. Keep lifecycle policy out of that contract. Production
	// defaults cap exits and reap idle UDP flows (with an ICMP teardown), which
	// dedicated flow-cap and flow-reaper tests cover; allowing either here turns
	// an expected policy action into a packet the echo-only callback cannot parse.
	settings.MaxFlowsPerExit = 0
	settings.SequenceIdleTimeout = time.Hour
	settings.TcpSequenceIdleTimeout = time.Hour
	settings.UdpTeardownSignal = false

	multiClient := NewRemoteUserNatMultiClient(
		ctx,
		generator,
		receivePacketCallback,
		protocol.ProvideMode_Network,
		settings,
	)

	return multiClient, nil
}

type TestMultiClientGenerator struct {
	nextDestinations     func(count int, excludeDestinations []MultiHopId, rankMode string) (map[MultiHopId]DestinationStats, error)
	newClientArgs        func() (*MultiClientGeneratorClientArgs, error)
	removeClientArgs     func(args *MultiClientGeneratorClientArgs)
	removeClientWithArgs func(client *Client, args *MultiClientGeneratorClientArgs)
	newClientSettings    func() *ClientSettings
	newClient            func(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error)
}

func (self *TestMultiClientGenerator) NextDestinations(count int, excludeDestinations []MultiHopId, rankMode string) (map[MultiHopId]DestinationStats, error) {
	return self.nextDestinations(count, excludeDestinations, rankMode)
}

func (self *TestMultiClientGenerator) NewClientArgs() (*MultiClientGeneratorClientArgs, error) {
	return self.newClientArgs()
}

func (self *TestMultiClientGenerator) RemoveClientArgs(args *MultiClientGeneratorClientArgs) {
	self.removeClientArgs(args)
}

func (self *TestMultiClientGenerator) RemoveClientWithArgs(client *Client, args *MultiClientGeneratorClientArgs) {
	self.removeClientWithArgs(client, args)
}

func (self *TestMultiClientGenerator) NewClientSettings() *ClientSettings {
	return self.newClientSettings()
}

func (self *TestMultiClientGenerator) NewClient(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error) {
	return self.newClient(ctx, args, clientSettings)
}

func (self *TestMultiClientGenerator) FixedDestinationSize() (int, bool) {
	return 1, true
}

func TestMultiClientChannelWindowStats(t *testing.T) {
	// ensure that the bucket counts are bounded
	// if this is broken, the coalesce logic is broken and there will be a memory issue

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	timeout := 10 * time.Second

	m := 6
	n := 6
	repeatCount := 6
	parallelCount := 6

	generator := &TestMultiClientGenerator{
		nextDestinations: func(count int, excludedDestinations []MultiHopId, rankMode string) (map[MultiHopId]DestinationStats, error) {
			// not used
			return nil, nil
		},
		newClientArgs: func() (*MultiClientGeneratorClientArgs, error) {
			args := &MultiClientGeneratorClientArgs{
				ClientId:   NewId(),
				ClientAuth: nil,
			}
			return args, nil
		},
		removeClientArgs: func(args *MultiClientGeneratorClientArgs) {
			// do nothing
		},
		removeClientWithArgs: func(client *Client, args *MultiClientGeneratorClientArgs) {
			// do nothing
		},
		newClientSettings: DefaultClientSettings,
		newClient: func(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error) {
			client := NewClient(ctx, args.ClientId, NewNoContractClientOob(), clientSettings)
			return client, nil
		},
	}

	clientReceivePacket := func(client *multiClientChannel, source TransferPath, provideMode protocol.ProvideMode, transportType TransportType, ipPath *IpPath, packet []byte) {
		// Do nothing
	}

	contractStatus := func(contractStatus *ContractStatus) {
		// Do nothing
	}

	settings := DefaultMultiClientSettings()
	settings.StatsWindowBucketDuration = 100 * time.Millisecond
	settings.StatsWindowDuration = 1 * time.Second
	settings.BlackholeTimeout = 300 * time.Second

	// the coalesce logic trims from the last event in a bucket
	// if events are uniformly distributed in a bucket, this means there will be an extra bucket
	maxBucketCount := 1 + int(math.Ceil(float64(settings.StatsWindowDuration)/float64(settings.StatsWindowBucketDuration)))

	args, err := generator.NewClientArgs()
	channelArgs := &multiClientChannelArgs{
		MultiClientGeneratorClientArgs: *args,
		Destination:                    RequireMultiHopId(NewId()),
		DestinationStats: DestinationStats{
			EstimatedBytesPerSecond: 0,
			Tier:                    0,
		},
	}
	AssertEqual(t, nil, err)

	clientChannel, err := newMultiClientChannel(ctx, channelArgs, generator, clientReceivePacket, nil, DefaultSecurityPolicy(ctx), contractStatus, func(contractStatsEvents []*ContractStatsEvent) {}, func() {}, nil, settings, nil, nil, nil, nil, nil, nil, nil, nil, nil)
	AssertEqual(t, nil, err)

	cancelCtxs := []context.Context{}

	for p := 0; p < parallelCount; p += 1 {
		cancelCtx, cancel := context.WithCancel(ctx)
		cancelCtxs = append(cancelCtxs, cancelCtx)
		go func() {
			defer cancel()
			for endTime := time.Now().Add(timeout); time.Now().Before(endTime); {
				for s := 0; s < m; s += 1 {
					for i := 0; i < n; i += 1 {
						for j := 0; j < n; j += 1 {
							for k := 0; k < n; k += 1 {
								for a := 0; a < repeatCount; a += 1 {
									packet, _ := udp4Packet(s, i, j, k)
									ipPath, err := ParseIpPath(packet)
									AssertEqual(t, nil, err)

									clientChannel.addSendNack(1)
									clientChannel.addSendAck(1)
									clientChannel.addReceiveAck(1)
									clientChannel.addSource(ipPath)

								}
							}
						}
					}
				}
			}
		}()
	}

	for _, cancelCtx := range cancelCtxs {
		<-cancelCtx.Done()
	}

	stats, err := clientChannel.windowStatsWithCoalesce(false)
	AssertEqual(t, nil, err)

	// [1, maxBucketCount]
	AssertEqual(t, true, 1 <= stats.bucketCount)
	AssertEqual(t, true, stats.bucketCount <= maxBucketCount)

	stats, err = clientChannel.WindowStats()
	AssertEqual(t, nil, err)

	// [1, maxBucketCount]
	AssertEqual(t, true, 1 <= stats.bucketCount)
	AssertEqual(t, true, stats.bucketCount <= maxBucketCount)
}

func TestMultiClientOverrideAllowDirect(t *testing.T) {
	// `OverrideAllowDirect` hard-overrides the profile's direct mode in
	// either direction, superseding both the performance profile and the
	// same-network force. false is the cloud-hosted hard limit (a direct
	// connection would leak that the client is hosted and where it is
	// hosted); true forces direct mode on regardless of the profile.

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	newMultiClient := func(provideMode protocol.ProvideMode, overrideAllowDirect *bool, defaultPerformanceProfile *PerformanceProfile) *RemoteUserNatMultiClient {
		settings := DefaultMultiClientSettings()
		settings.OverrideAllowDirect = overrideAllowDirect
		settings.DefaultPerformanceProfile = defaultPerformanceProfile
		return NewRemoteUserNatMultiClient(
			ctx,
			&testingEmptyMultiClientGenerator{},
			func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
			},
			provideMode,
			settings,
		)
	}
	boolPtr := func(b bool) *bool { return &b }

	allowDirectProfile := &PerformanceProfile{
		WindowType:  WindowTypeQuality,
		WindowSize:  DefaultWindowSizeSettings(),
		AllowDirect: true,
	}
	noDirectProfile := &PerformanceProfile{
		WindowType:  WindowTypeQuality,
		WindowSize:  DefaultWindowSizeSettings(),
		AllowDirect: false,
	}

	// baseline: without an override, the same-network force enables direct
	// mode even with no profile set
	multiClient := newMultiClient(protocol.ProvideMode_Network, nil, nil)
	pp := multiClient.config.Load().performanceProfile
	AssertEqual(t, true, pp != nil)
	AssertEqual(t, true, pp.AllowDirect)
	multiClient.Close()

	// override false supersedes the same-network force
	multiClient = newMultiClient(protocol.ProvideMode_Network, boolPtr(false), nil)
	defer multiClient.Close()
	pp = multiClient.config.Load().performanceProfile
	AssertEqual(t, true, pp != nil)
	AssertEqual(t, false, pp.AllowDirect)

	// override false supersedes a profile that allows direct,
	// on both the set path and the constructor default path
	multiClient.SetPerformanceProfile(allowDirectProfile)
	pp = multiClient.config.Load().performanceProfile
	AssertEqual(t, true, pp != nil)
	AssertEqual(t, false, pp.AllowDirect)
	// the rest of the profile is preserved
	AssertEqual(t, WindowTypeQuality, pp.WindowType)
	// the input profile is not mutated in place
	AssertEqual(t, true, allowDirectProfile.AllowDirect)

	multiClientWithDefault := newMultiClient(protocol.ProvideMode_Network, boolPtr(false), allowDirectProfile)
	defer multiClientWithDefault.Close()
	pp = multiClientWithDefault.config.Load().performanceProfile
	AssertEqual(t, true, pp != nil)
	AssertEqual(t, false, pp.AllowDirect)

	// override true forces direct mode on over a profile that disables it,
	// with no same-network force in play (provide mode Public), on both the
	// constructor default path and the set path
	multiClientForcedOn := newMultiClient(protocol.ProvideMode_Public, boolPtr(true), noDirectProfile)
	defer multiClientForcedOn.Close()
	pp = multiClientForcedOn.config.Load().performanceProfile
	AssertEqual(t, true, pp != nil)
	AssertEqual(t, true, pp.AllowDirect)
	// the input profile is not mutated in place
	AssertEqual(t, false, noDirectProfile.AllowDirect)

	multiClientForcedOn.SetPerformanceProfile(noDirectProfile)
	pp = multiClientForcedOn.config.Load().performanceProfile
	AssertEqual(t, true, pp != nil)
	AssertEqual(t, true, pp.AllowDirect)

	// without an override and without the same-network force, the profile
	// passes through
	multiClientPlain := newMultiClient(protocol.ProvideMode_Public, nil, noDirectProfile)
	defer multiClientPlain.Close()
	pp = multiClientPlain.config.Load().performanceProfile
	AssertEqual(t, true, pp != nil)
	AssertEqual(t, false, pp.AllowDirect)
}

func TestMultiClientEquivalentPerformanceProfileIsNoOp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultMultiClientSettings()
	settings.DefaultPerformanceProfile = &PerformanceProfile{
		WindowType: WindowTypeAuto,
		WindowSize: WindowSizeSettings{
			WindowSizeMin: 17,
			WindowSizeMax: 23,
		},
		PostQuantumEncryption: true,
	}
	multiClient := NewRemoteUserNatMultiClient(
		ctx,
		&testingEmptyMultiClientGenerator{},
		func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		},
		protocol.ProvideMode_Public,
		settings,
	)
	defer multiClient.Close()

	before := multiClient.config.Load()
	multiClient.SetPerformanceProfile(&PerformanceProfile{
		WindowType: WindowTypeAuto,
		// Auto mode ignores the reconstructed presentation value's window
		// size, so this remains the same installed behavior.
		WindowSize: WindowSizeSettings{
			WindowSizeMin: 41,
			WindowSizeMax: 47,
		},
		PostQuantumEncryption: true,
	})
	after := multiClient.config.Load()
	if after != before {
		t.Fatalf("equivalent profile published a new config and would shuffle windows")
	}
}

// TestMultiClientNilAndAutoPerformanceProfilesAreEquivalent protects the
// common first-presentation migration from an absent profile to explicit
// auto defaults.
func TestMultiClientNilAndAutoPerformanceProfilesAreEquivalent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	multiClient := NewRemoteUserNatMultiClient(
		ctx,
		&testingEmptyMultiClientGenerator{},
		func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		},
		protocol.ProvideMode_Public,
		DefaultMultiClientSettings(),
	)
	defer multiClient.Close()

	before := multiClient.config.Load()
	multiClient.SetPerformanceProfile(&PerformanceProfile{
		WindowType: WindowTypeAuto,
	})
	after := multiClient.config.Load()
	if after != before {
		t.Fatalf("nil and explicit auto profiles must install the same behavior")
	}
}

// TestMultiClientChangedPerformanceProfilePublishesConfig ensures the no-op
// guard cannot suppress an actual transport-policy change.
func TestMultiClientChangedPerformanceProfilePublishesConfig(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	multiClient := NewRemoteUserNatMultiClient(
		ctx,
		&testingEmptyMultiClientGenerator{},
		func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		},
		protocol.ProvideMode_Public,
		DefaultMultiClientSettings(),
	)
	defer multiClient.Close()

	before := multiClient.config.Load()
	multiClient.SetPerformanceProfile(&PerformanceProfile{
		WindowType:            WindowTypeAuto,
		PostQuantumEncryption: true,
	})
	after := multiClient.config.Load()
	if after == before {
		t.Fatalf("changed profile did not publish a replacement config")
	}
	if after.performanceProfile == nil || !after.performanceProfile.PostQuantumEncryption {
		t.Fatalf("changed profile was not installed: %+v", after.performanceProfile)
	}
}
