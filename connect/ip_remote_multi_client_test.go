package connect

import (
	"context"
	"math"
	"testing"
	"time"
	// "slices"
	"sync"

	"github.com/go-playground/assert/v2"

	"bringyour.com/protocol"
)

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

func testingNewMultiClient(ctx context.Context, providerClient *Client, receivePacketCallback ReceivePacketFunction) (UserNatClient, error) {

	mutex := sync.Mutex{}
	unsubs := map[*Client]func(){}

	generator := &TestMultiClientGenerator{
		nextDestinations: func(count int, excludeDestinations []MultiHopId) (map[MultiHopId]ByteCount, error) {
			next := map[MultiHopId]ByteCount{}
			containsTail := func() bool {
				for _, destination := range excludeDestinations {
					if 0 < destination.Len() && destination.Tail() == providerClient.ClientId() {
						return true
					}
				}
				return false
			}
			if !containsTail() {
				next[RequireMultiHopId(providerClient.ClientId())] = ByteCount(0)
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
			settings := DefaultClientSettings()
			settings.SendBufferSettings.SequenceBufferSize = 0
			settings.SendBufferSettings.AckBufferSize = 0
			settings.ReceiveBufferSettings.SequenceBufferSize = 0
			// settings.ReceiveBufferSettings.AckBufferSize = 0
			settings.ForwardBufferSettings.SequenceBufferSize = 0
			return settings
		},
		newClient: func(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error) {
			client := NewClient(ctx, args.ClientId, NewNoContractClientOob(), clientSettings)

			routesSend := []Route{
				make(chan []byte),
			}
			routesReceive := []Route{
				make(chan []byte),
			}

			transportSend := NewSendGatewayTransport()
			transportReceive := NewReceiveGatewayTransport()
			client.RouteManager().UpdateTransport(transportSend, routesSend)
			client.RouteManager().UpdateTransport(transportReceive, routesReceive)

			client.ContractManager().AddNoContractPeer(providerClient.ClientId())

			providerTransportSend := NewSendClientTransport(DestinationId(args.ClientId))
			providerTransportReceive := NewReceiveGatewayTransport()
			providerClient.RouteManager().UpdateTransport(providerTransportReceive, routesSend)
			providerClient.RouteManager().UpdateTransport(providerTransportSend, routesReceive)

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

	multiClient := NewRemoteUserNatMultiClientWithDefaults(
		ctx,
		generator,
		receivePacketCallback,
		protocol.ProvideMode_Network,
	)

	return multiClient, nil
}

type TestMultiClientGenerator struct {
	nextDestinations     func(count int, excludeDestinations []MultiHopId) (map[MultiHopId]ByteCount, error)
	newClientArgs        func() (*MultiClientGeneratorClientArgs, error)
	removeClientArgs     func(args *MultiClientGeneratorClientArgs)
	removeClientWithArgs func(client *Client, args *MultiClientGeneratorClientArgs)
	newClientSettings    func() *ClientSettings
	newClient            func(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error)
}

func (self *TestMultiClientGenerator) NextDestinations(count int, excludeDestinations []MultiHopId) (map[MultiHopId]ByteCount, error) {
	return self.nextDestinations(count, excludeDestinations)
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
		nextDestinations: func(count int, excludedDestinations []MultiHopId) (map[MultiHopId]ByteCount, error) {
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

	receivePacket := func(source TransferPath, ipProtocol IpProtocol, packet []byte) {
		// Do nothing
	}

	settings := DefaultMultiClientSettings()
	settings.StatsWindowBucketDuration = 100 * time.Millisecond
	settings.StatsWindowDuration = 1 * time.Second

	// the coalesce logic trims from the last event in a bucket
	// if events are uniformly distributed in a bucket, this means there will be an extra bucket
	maxBucketCount := 1 + int(math.Ceil(float64(settings.StatsWindowDuration)/float64(settings.StatsWindowBucketDuration)))

	args, err := generator.NewClientArgs()
	channelArgs := &multiClientChannelArgs{
		MultiClientGeneratorClientArgs: *args,
		Destination:                    RequireMultiHopId(NewId()),
		EstimatedBytesPerSecond:        0,
	}
	assert.Equal(t, nil, err)
	clientChannel, err := newMultiClientChannel(ctx, channelArgs, generator, receivePacket, settings)
	assert.Equal(t, nil, err)

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
									assert.Equal(t, nil, err)

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
	assert.Equal(t, nil, err)

	assert.Equal(t, maxBucketCount, stats.bucketCount)

	stats, err = clientChannel.WindowStats()
	assert.Equal(t, nil, err)

	assert.Equal(t, maxBucketCount, stats.bucketCount)
}
