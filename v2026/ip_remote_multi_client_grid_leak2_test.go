package connect

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestMultiClientMonitorPairingFlappingProviders reproduces the other real-world
// "unable to connect" flavor: providers that ACK the evaluation ping (so clients
// become Added and the window looks momentarily satisfied) and then go dark, so
// the client turns unhealthy and is removed, and the window expands again with
// fresh candidates — a Connected/Connecting flap sustained forever. This engages
// the whole Added -> unhealthy -> Removed machinery (resize health checks,
// warned clients, collapse) that the never-acking test cannot reach. The grid
// mirror must stay bounded: every Added eventually gets Removed, every
// InEvaluation gets a terminal, and reaped points must not resurrect.
func TestMultiClientMonitorPairingFlappingProviders(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultMultiClientSettings()
	// compress the timescales
	settings.PingWriteTimeout = 200 * time.Millisecond
	settings.PingTimeout = 500 * time.Millisecond
	settings.WindowResizeTimeout = 300 * time.Millisecond
	settings.WindowExpandTimeout = 400 * time.Millisecond
	settings.WindowEnumerateErrorTimeout = 100 * time.Millisecond
	settings.WindowExpandArgsTimeout = 2 * time.Second
	// health windows: a dark client goes unhealthy quickly
	settings.StatsWindowMaxUnhealthyDuration = 1 * time.Second
	settings.StatsWindowWarnUnhealthyDuration = 500 * time.Millisecond
	settings.StatsWindowKeepUnhealthyDuration = 2 * time.Second
	settings.StatsWindowGraceperiod = 1 * time.Second
	settings.AckTimeout = 1 * time.Second
	settings.BlackholeTimeout = 1 * time.Second
	settings.BlackholeConnectTimeout = 2 * time.Second
	// compress the continuous ping so dead idle clients are detected fast
	settings.CPingWriteTimeout = 200 * time.Millisecond
	settings.CPingTimeout = 500 * time.Millisecond
	settings.CPingRestTimeout = 300 * time.Millisecond

	generator := newFlappingProviderGenerator(ctx)
	defer generator.close()

	multi := NewRemoteUserNatMultiClient(
		ctx,
		generator,
		func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {},
		protocol.ProvideMode_Public,
		settings,
	)
	defer multi.Close()

	const removeTimeout = 800 * time.Millisecond
	mirror := newGridMirror(removeTimeout)

	monitor := multi.Monitor()
	unsub := monitor.AddMonitorEventCallback(mirror.monitorEventCallback)
	defer unsub()
	windowExpandEvent, providerEvents := monitor.Events()
	mirror.monitorEventCallback(windowExpandEvent, providerEvents, true)

	testDuration := 45 * time.Second
	sampleInterval := 3 * time.Second
	endTime := time.Now().Add(testDuration)
	samples := []int{}
	stuckSamples := []int{}
	for time.Now().Before(endTime) {
		select {
		case <-ctx.Done():
			t.Fatal("multi client died")
		case <-time.After(sampleInterval):
		}
		live, stuck, maxLive := mirror.sample(settings.PingTimeout + settings.WindowExpandTimeout + settings.StatsWindowMaxUnhealthyDuration + settings.WindowResizeTimeout + removeTimeout + 3*time.Second)
		samples = append(samples, live)
		stuckSamples = append(stuckSamples, stuck)
		fmt.Printf("[leaktest2] live=%d stuck=%d maxLive=%d totalSeen=%d states=%v\n", live, stuck, maxLive, mirror.totalSeen(), mirror.states())
	}

	live, stuck, maxLive := mirror.sample(settings.PingTimeout + settings.WindowExpandTimeout + settings.StatsWindowMaxUnhealthyDuration + settings.WindowResizeTimeout + removeTimeout + 3*time.Second)
	fmt.Printf("[leaktest2] final live=%d stuck=%d maxLive=%d totalSeen=%d\n", live, stuck, maxLive, mirror.totalSeen())

	// DERIVED from the settings under test rather than copied out of them, for
	// the same reason as its twin in ip_remote_multi_client_grid_leak_test.go:
	// this was the identical stale literal `3 * (12 + 4)`.
	bound := 3 * (settings.WindowSizes[WindowTypeQuality].WindowSizeHardMax +
		settings.WindowSizes[WindowTypeSpeed].WindowSizeHardMax)
	if maxLive > bound {
		t.Errorf("live point set grew beyond bound: maxLive=%d > %d", maxLive, bound)
	}
	// with the rank-keep unhealthy cap and the continuous ping, dead clients
	// are always detected and reclaimed within the compressed timeouts; a
	// transient stuck sample can still coincide with a detection window, but
	// SUSTAINED stuck points indicate lost terminal events
	if 3 <= len(stuckSamples) {
		last3 := stuckSamples[len(stuckSamples)-3:]
		if 0 < last3[0] && 0 < last3[1] && 0 < last3[2] {
			t.Errorf("sustained stuck non-terminal points: %v", stuckSamples)
		}
	}
	if len(samples) >= 6 {
		early := max(samples[2], samples[3])
		last := samples[len(samples)-1]
		if last > 2*max(early, 10) {
			t.Errorf("live point set is growing: early=%d last=%d samples=%v", early, last, samples)
		}
	}
}

// flappingProviderGenerator wires each candidate client to live provider
// clients that ack protocol traffic (so the evaluation ping succeeds and the
// client is Added), then severs the candidate's transports shortly after, so the
// client goes dark and the window has to remove it and expand again.
type flappingProviderGenerator struct {
	ctx context.Context

	mu              sync.Mutex
	providerClients []*Client
	unsubs          map[*Client]func()
}

func newFlappingProviderGenerator(ctx context.Context) *flappingProviderGenerator {
	providerClients := []*Client{}
	for range 8 {
		providerClients = append(providerClients, NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings()))
	}
	return &flappingProviderGenerator{
		ctx:             ctx,
		providerClients: providerClients,
		unsubs:          map[*Client]func(){},
	}
}

func (self *flappingProviderGenerator) close() {
	for _, providerClient := range self.providerClients {
		providerClient.Close()
	}
}

func (self *flappingProviderGenerator) NextDestinations(count int, excludeDestinations []MultiHopId, rankMode string) (map[MultiHopId]DestinationStats, error) {
	// the real provider destinations, excluding those already in the window
	excluded := map[Id]bool{}
	for _, destination := range excludeDestinations {
		if 0 < destination.Len() {
			excluded[destination.Tail()] = true
		}
	}
	next := map[MultiHopId]DestinationStats{}
	for _, providerClient := range self.providerClients {
		if len(next) == count {
			break
		}
		if !excluded[providerClient.ClientId()] {
			next[RequireMultiHopId(providerClient.ClientId())] = DestinationStats{
				EstimatedBytesPerSecond: ByteCount(0),
				Tier:                    0,
			}
		}
	}
	return next, nil
}

func (self *flappingProviderGenerator) NewClientArgs() (*MultiClientGeneratorClientArgs, error) {
	return &MultiClientGeneratorClientArgs{
		ClientId:   NewId(),
		ClientAuth: nil,
	}, nil
}

func (self *flappingProviderGenerator) RemoveClientArgs(args *MultiClientGeneratorClientArgs) {
}

func (self *flappingProviderGenerator) RemoveClientWithArgs(client *Client, args *MultiClientGeneratorClientArgs) {
	var unsub func()
	var ok bool
	func() {
		self.mu.Lock()
		defer self.mu.Unlock()
		unsub, ok = self.unsubs[client]
		if ok {
			delete(self.unsubs, client)
		}
	}()
	if ok {
		unsub()
	}
}

func (self *flappingProviderGenerator) NewClientSettings() *ClientSettings {
	settings := DefaultClientSettings()
	settings.SendBufferSettings.SequenceBufferSize = 0
	settings.SendBufferSettings.AckBufferSize = 0
	settings.ReceiveBufferSettings.SequenceBufferSize = 0
	settings.ForwardBufferSettings.SequenceBufferSize = 0
	return settings
}

func (self *flappingProviderGenerator) NewClient(ctx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error) {
	client := NewClient(ctx, args.ClientId, NewNoContractClientOob(), clientSettings)

	routeSend := make(chan []byte)
	routeReceive := make(chan []byte)

	transportSend := NewSendGatewayTransport()
	transportReceive := NewReceiveGatewayTransport()
	client.RouteManager().UpdateTransport(transportSend, []Route{routeSend})
	client.RouteManager().UpdateTransport(transportReceive, []Route{routeReceive})

	// wire the candidate to every provider: each provider receives the client's
	// egress and the addressed provider acks, so the evaluation ping succeeds
	severs := []func(){}
	for _, providerClient := range self.providerClients {
		providerTransportSend := NewSendClientTransport(DestinationId(args.ClientId))
		providerTransportReceive := NewReceiveGatewayTransport()

		client.ContractManager().AddNoContractPeer(providerClient.ClientId())
		providerClient.ContractManager().AddNoContractPeer(client.ClientId())
		providerClient.RouteManager().UpdateTransport(providerTransportReceive, []Route{routeSend})
		providerClient.RouteManager().UpdateTransport(providerTransportSend, []Route{routeReceive})

		p := providerClient
		severs = append(severs, func() {
			p.RouteManager().RemoveTransport(providerTransportReceive)
			p.RouteManager().RemoveTransport(providerTransportSend)
		})
	}

	sever := func() {
		client.RouteManager().RemoveTransport(transportSend)
		client.RouteManager().RemoveTransport(transportReceive)
		for _, s := range severs {
			s()
		}
	}

	func() {
		self.mu.Lock()
		defer self.mu.Unlock()
		self.unsubs[client] = sever
	}()

	// the provider goes dark shortly after: transports severed, the client's
	// subsequent traffic goes nowhere and it turns unhealthy
	go HandleError(func() {
		select {
		case <-ctx.Done():
			return
		case <-self.ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
		var unsub func()
		var ok bool
		func() {
			self.mu.Lock()
			defer self.mu.Unlock()
			unsub, ok = self.unsubs[client]
			if ok {
				delete(self.unsubs, client)
			}
		}()
		if ok {
			unsub()
		}
	})

	return client, nil
}

func (self *flappingProviderGenerator) FixedDestinationSize() (int, bool) {
	return 0, false
}
