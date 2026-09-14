package connect

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestRecoveryKernel is the measurement kernel for the multi-client dead-peer
// recovery bake-off. Env-gated (URNET_RECOVERY=1) so normal suites skip it;
// each invocation measures one configuration and emits one machine-readable
// [recovery] line.
//
// Adapted from upstream main e05ecee's multi_client_recovery_kernel_test.go
// (TestMultiClientRecoveryKernel) to OUR APIs. The goal is the instrument —
// a neutral, in-process detect/gap/recover measurement — not fidelity to
// their multi-client internals. Deltas from the upstream original:
//   - URNET_REC_BUSYSTALE_MS (their CPingBusyStaleTimeout) is dropped: the
//     busy-probe knob does not exist in this tree yet (P2 introduces our
//     busy-probe with different settings). URNET_REC_DEGRADED (their
//     settings.DegradedMode) is dropped for the same reason.
//   - URNET_REC_SENDSTALL_MS is added: our headline detector is the
//     SendStallTimeout bar, so the sweep can drive it. Unset keeps the
//     tree default.
//   - the provider NAT uses DefaultProviderLocalUserNatSettings (no
//     WithMemoryTarget variant here).
//   - sent packets are NOT MessagePoolReturn'ed: our ipOosUdpPacket
//     allocates ordinary GC'd buffers and our SendPacket takes ownership on
//     the race path (mirrors the ip_test.go e2e send loop).
//   - the recovery-failure diagnostic dump reads our channel surface
//     (WindowStats counters + IsDone) instead of their packetStats fields
//     and busyStale probe state.
//
// Topology (fully in-process): a RemoteUserNatMultiClient over N provider
// backends (each a Client + provider LocalUserNat + RemoteUserNatProvider
// egressing to a local udp echo), wired with buffered destination-addressed
// in-memory routes (mirroring the production buffered transports). A
// constant-rate udp flow pins to one window client (busy, above the cping
// idle gate); mid-transfer the pinned provider is killed abruptly
// (client cancel — routes stay up, acks stop: the relay-blackhole case).
//
// Emitted metrics:
//   - detect_ms: kill -> monitor ProviderStateRemoved (detection + removal)
//   - gap_ms: last echo before the gap -> first echo after
//   - recover90_ms: kill -> first 1s window at >=90% of the pre-kill rate
//   - refill_ms: kill -> monitor ProviderStateAdded (replacement joined)
func TestRecoveryKernel(t *testing.T) {
	if os.Getenv("URNET_RECOVERY") == "" {
		t.Skip("recovery kernel: set URNET_RECOVERY=1")
	}
	// one measurement per family: the flow, its echo target and the exits'
	// category follow the version under test (IPV6.md D3), so each family
	// emits its own [recovery] line
	forEachIpVersion(t, runRecoveryKernel)
}

// runRecoveryKernel is one configuration of the kernel over one ip family.
func runRecoveryKernel(t *testing.T, ipVersion int) {
	providerCount := recoveryEnvInt("URNET_REC_PROVIDERS", 3)
	pps := recoveryEnvInt("URNET_REC_PPS", 200)
	payloadByteCount := recoveryEnvInt("URNET_REC_PAYLOAD", 1200)
	steadyMs := recoveryEnvInt("URNET_REC_STEADY_MS", 3000)
	maxWaitMs := recoveryEnvInt("URNET_REC_MAXWAIT_MS", 60000)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// local udp echo egress target on the family's loopback
	echoConn, err := net.ListenUDP(testUdpNetwork(ipVersion), testLoopbackUdpAddr(ipVersion))
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	defer echoConn.Close()
	go HandleError(func() {
		buffer := make([]byte, 2048)
		for {
			n, addr, err := echoConn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			echoConn.WriteToUDP(buffer[0:n], addr)
		}
	})
	echoPort := echoConn.LocalAddr().(*net.UDPAddr).Port

	// provider backends
	type recoveryProvider struct {
		clientId Id
		client   *Client
		nat      *LocalUserNat
		provider *RemoteUserNatProvider
		cancel   context.CancelFunc
	}
	var providersMutex sync.Mutex
	providers := map[Id]*recoveryProvider{}
	live := map[Id]bool{}
	newProvider := func() *recoveryProvider {
		providerCtx, providerCancel := context.WithCancel(ctx)
		clientId := NewId()
		clientSettings := DefaultClientSettings()
		clientSettings.Log = NewNoopLogger()
		client := NewClient(providerCtx, clientId, NewNoContractClientOob(), clientSettings)
		natSettings := DefaultProviderLocalUserNatSettings()
		natSettings.Log = NewNoopLogger()
		nat := NewLocalUserNat(providerCtx, clientId.String(), natSettings)
		provider := NewRemoteUserNatProvider(client, nat, DefaultRemoteUserNatProviderSettings())
		client.ContractManager().SetProvideModesWithReturnTraffic(map[protocol.ProvideMode]bool{
			protocol.ProvideMode_Network: true,
			protocol.ProvideMode_Public:  true,
		})
		p := &recoveryProvider{
			clientId: clientId,
			client:   client,
			nat:      nat,
			provider: provider,
			cancel:   providerCancel,
		}
		providersMutex.Lock()
		providers[clientId] = p
		live[clientId] = true
		providersMutex.Unlock()
		return p
	}
	for i := 0; i < providerCount; i += 1 {
		newProvider()
	}

	// generator: offers the live providers as destinations; each window
	// client binds to ONE provider (via NewClientArgsForDestination) over a
	// buffered gateway route pair (mirrors the buffered production transport).
	providerClientOf := func(id Id) *Client {
		providersMutex.Lock()
		defer providersMutex.Unlock()
		if p, ok := providers[id]; ok {
			return p.client
		}
		return nil
	}
	generator := &recoveryGenerator{
		windowProvider: map[Id]Id{},
		unsubs:         map[*Client]func(){},
		nextDestinations: func(excluded map[Id]bool) map[MultiHopId]DestinationStats {
			next := map[MultiHopId]DestinationStats{}
			providersMutex.Lock()
			defer providersMutex.Unlock()
			for clientId := range providers {
				if live[clientId] && !excluded[clientId] {
					next[RequireMultiHopId(clientId)] = DestinationStats{EstimatedBytesPerSecond: 0, Tier: 0}
				}
			}
			return next
		},
		anyLiveProvider: func() (Id, bool) {
			providersMutex.Lock()
			defer providersMutex.Unlock()
			for clientId := range providers {
				if live[clientId] {
					return clientId, true
				}
			}
			return Id{}, false
		},
		providerClientOf: providerClientOf,
	}

	// echo receive accounting
	var echoMutex sync.Mutex
	echoTimes := []time.Time{}
	receivePacketCallback := func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		// the return echo replies arrive on our udp flow (src port 40001,
		// dst the echo server); other callbacks (e.g. the oos rst emitted on
		// client removal) are not on this flow
		if ipPath == nil || ipPath.Protocol != IpProtocolUdp {
			return
		}
		if ipPath.SourcePort != 40001 || ipPath.DestinationPort != echoPort {
			return
		}
		echoMutex.Lock()
		echoTimes = append(echoTimes, time.Now())
		echoMutex.Unlock()
	}

	settings := DefaultMultiClientSettings()
	settings.TcpCollapsePrevention = false
	settings.AckTimeout = time.Duration(recoveryEnvInt("URNET_REC_ACK_MS", 30000)) * time.Millisecond
	settings.StatsWindowDuration = time.Duration(recoveryEnvInt("URNET_REC_STATSWIN_MS", 30000)) * time.Millisecond
	settings.StatsWindowBucketDuration = time.Duration(recoveryEnvInt("URNET_REC_BUCKET_MS", 1000)) * time.Millisecond
	settings.BlackholeTimeout = time.Duration(recoveryEnvInt("URNET_REC_BLACKHOLE_MS", 5000)) * time.Millisecond
	settings.WindowResizeTimeout = time.Duration(recoveryEnvInt("URNET_REC_RESIZE_MS", 15000)) * time.Millisecond
	settings.CPingRestTimeout = time.Duration(recoveryEnvInt("URNET_REC_CPING_REST_MS", 10000)) * time.Millisecond
	settings.CPingTimeout = time.Duration(recoveryEnvInt("URNET_REC_CPING_TIMEOUT_MS", 30000)) * time.Millisecond
	if sendStallMs := recoveryEnvInt("URNET_REC_SENDSTALL_MS", -1); 0 <= sendStallMs {
		// our fork's headline detector; unset keeps the tree default
		settings.SendStallTimeout = time.Duration(sendStallMs) * time.Millisecond
	}
	if recoveryEnvInt("URNET_REC_PING_ALWAYS", 0) == 1 {
		// lift the idle-only gate: the continuous ping runs even on busy
		// flows — the active-probe hypothesis
		settings.CPingMaxByteCountPerSecond = 1 << 40
	}

	multiClient := NewRemoteUserNatMultiClient(
		ctx,
		generator,
		receivePacketCallback,
		protocol.ProvideMode_Network,
		settings,
	)
	defer multiClient.Close()

	// monitor: timestamp Removed (detection) and Added (refill) receipts
	var monitorMutex sync.Mutex
	var removedTime, addedTime time.Time
	var killTime time.Time
	unsubMonitor := multiClient.Monitor().AddMonitorEventCallback(func(windowExpandEvent *WindowExpandEvent, providerEvents map[Id]*ProviderEvent, reset bool) {
		now := time.Now()
		monitorMutex.Lock()
		defer monitorMutex.Unlock()
		for _, event := range providerEvents {
			switch event.State {
			case ProviderStateRemoved:
				if !killTime.IsZero() && removedTime.IsZero() {
					removedTime = now
				}
			case ProviderStateAdded:
				if !killTime.IsZero() && addedTime.IsZero() {
					addedTime = now
				}
			}
		}
	})
	defer unsubMonitor()

	// one pinned busy flow: constant-rate udp to the echo server, over the
	// family under test
	source := SourceId(NewId())
	flowSourceIp := net.IPv4(72, 0, 0, 1)
	if ipVersion == 6 {
		flowSourceIp = net.ParseIP("2001:db8::72:1")
	}
	flowIpPath := &IpPath{
		Version:         ipVersion,
		Protocol:        IpProtocolUdp,
		SourceIp:        flowSourceIp,
		SourcePort:      40001,
		DestinationIp:   net.ParseIP(testLoopbackIp(ipVersion)),
		DestinationPort: echoPort,
	}
	payload := make([]byte, payloadByteCount)
	var sendSeq uint64
	sendPacket := func() {
		sendSeq += 1
		binary.BigEndian.PutUint64(payload, sendSeq)
		packet := ipOosUdpPacket(flowIpPath, payload)
		// blocking send (-1): the window forms lazily on first traffic; a
		// non-blocking send would drop before any client is pinned. the
		// packet buffer is ordinary GC'd memory here and SendPacket takes
		// ownership on the race path — no pool return (see the e2e loop)
		multiClient.SendPacket(source, protocol.ProvideMode_Network, packet, -1)
	}

	sendDone := make(chan struct{})
	go HandleError(func() {
		interval := time.Second / time.Duration(pps)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-sendDone:
				return
			case <-ticker.C:
				sendPacket()
			}
		}
	})
	defer close(sendDone)

	// steady state: require echoes flowing
	steadyDeadline := time.Now().Add(time.Duration(steadyMs) * time.Millisecond)
	for time.Now().Before(steadyDeadline) {
		time.Sleep(100 * time.Millisecond)
	}
	echoRateAt := func(start time.Time, end time.Time) float64 {
		echoMutex.Lock()
		defer echoMutex.Unlock()
		count := 0
		for _, at := range echoTimes {
			if at.After(start) && !at.After(end) {
				count += 1
			}
		}
		return float64(count) / end.Sub(start).Seconds()
	}
	preKillRate := echoRateAt(time.Now().Add(-2*time.Second), time.Now())
	if preKillRate < float64(pps)/2 {
		t.Fatalf("[recovery] steady state did not establish: %.1f echo/s (want >= %.1f)", preKillRate, float64(pps)/2)
	}

	// find the pinned provider (the one carrying the flow) by egress stats,
	// and kill it abruptly: cancel its ctx — routes stay up, acks stop
	var killed *recoveryProvider
	providersMutex.Lock()
	for _, p := range providers {
		stats := p.provider.PacketStats()
		if 0 < stats.RemoteIngressPacketCount {
			if killed == nil || killed.provider.PacketStats().RemoteIngressPacketCount < stats.RemoteIngressPacketCount {
				killed = p
			}
		}
	}
	if killed != nil {
		live[killed.clientId] = false
	}
	providersMutex.Unlock()
	if killed == nil {
		t.Fatalf("[recovery] no provider carried the flow")
	}
	monitorMutex.Lock()
	killTime = time.Now()
	monitorMutex.Unlock()
	killed.cancel()

	// watch for recovery
	var gapStart, gapEnd time.Time
	var recover90 time.Time
	maxWait := time.Now().Add(time.Duration(maxWaitMs) * time.Millisecond)
	for time.Now().Before(maxWait) {
		time.Sleep(100 * time.Millisecond)
		echoMutex.Lock()
		if 0 < len(echoTimes) {
			last := echoTimes[len(echoTimes)-1]
			if gapStart.IsZero() && time.Second < time.Since(last) {
				// the gap has opened
				gapStart = last
			}
			if !gapStart.IsZero() && gapEnd.IsZero() && last.After(gapStart.Add(time.Second)) {
				gapEnd = last
			}
		}
		echoMutex.Unlock()
		if !gapEnd.IsZero() && recover90.IsZero() {
			now := time.Now()
			if float64(preKillRate)*0.9 <= echoRateAt(now.Add(-time.Second), now) {
				recover90 = now
			}
		}
		if !recover90.IsZero() {
			break
		}
	}

	ms := func(at time.Time) int64 {
		if at.IsZero() {
			return -1
		}
		return at.Sub(killTime).Milliseconds()
	}
	monitorMutex.Lock()
	removedAt := removedTime
	addedAt := addedTime
	monitorMutex.Unlock()
	if recover90.IsZero() {
		// Emit the killed destination's live channel state when recovery
		// fails, identifying whether detection, removal, or flow reassignment
		// is stuck. Adapted to our channel surface: WindowStats counters +
		// IsDone (their packetStats fields and busyStale do not exist here).
		for windowType, window := range multiClient.windows {
			for _, channel := range window.unorderedClients() {
				generator.mutex.Lock()
				providerId := generator.windowProvider[channel.ClientId()]
				generator.mutex.Unlock()
				if providerId != killed.clientId {
					continue
				}
				stats, statsErr := channel.WindowStats()
				if stats != nil {
					fmt.Printf("[recovery-state] window=%d client=%s send_acks=%d send_nacks=%d receive_acks=%d last_event_ms=%d healthy=%t err=%v done=%t\n",
						windowType, channel.ClientId(),
						stats.sendAckCount, stats.sendNackCount, stats.receiveAckCount,
						time.Since(stats.lastEventTime).Milliseconds(), stats.healthy,
						statsErr, channel.IsDone())
				} else {
					fmt.Printf("[recovery-state] window=%d client=%s stats=nil err=%v done=%t\n",
						windowType, channel.ClientId(), statsErr, channel.IsDone())
				}
			}
		}
		multiClient.stateLock.Lock()
		var flowUpdate *multiClientChannelUpdate
		if ipVersion == 4 {
			flowUpdate = multiClient.ip4PathUpdates[flowIpPath.ToIp4Path()]
		} else {
			flowUpdate = multiClient.ip6PathUpdates[flowIpPath.ToIp6Path()]
		}
		var flowClient *multiClientChannel
		if flowUpdate != nil {
			flowClient = flowUpdate.client.Load()
		}
		multiClient.stateLock.Unlock()
		if flowClient == nil {
			fmt.Printf("[recovery-state] flow_client=nil update=%t\n", flowUpdate != nil)
		} else {
			generator.mutex.Lock()
			flowProvider := generator.windowProvider[flowClient.ClientId()]
			generator.mutex.Unlock()
			fmt.Printf("[recovery-state] flow_client=%s flow_provider=%s killed=%t done=%t\n",
				flowClient.ClientId(), flowProvider, flowProvider == killed.clientId, flowClient.IsDone())
		}
		providersMutex.Lock()
		for id, provider := range providers {
			stats := provider.provider.PacketStats()
			fmt.Printf("[recovery-state] provider=%s live=%t ingress=%d egress=%d\n",
				id, live[id], stats.RemoteIngressPacketCount, stats.RemoteEgressPacketCount)
		}
		providersMutex.Unlock()
	}
	var gapMs int64 = -1
	if !gapStart.IsZero() && !gapEnd.IsZero() {
		gapMs = gapEnd.Sub(gapStart).Milliseconds()
	}
	fmt.Printf("[recovery] family=v%d providers=%d pps=%d ack_ms=%d statswin_ms=%d blackhole_ms=%d resize_ms=%d ping_always=%d cping_rest_ms=%d cping_timeout_ms=%d sendstall_ms=%d | pre_rate=%.1f detect_ms=%d refill_ms=%d gap_ms=%d recover90_ms=%d\n",
		ipVersion, providerCount, pps,
		recoveryEnvInt("URNET_REC_ACK_MS", 30000),
		recoveryEnvInt("URNET_REC_STATSWIN_MS", 30000),
		recoveryEnvInt("URNET_REC_BLACKHOLE_MS", 5000),
		recoveryEnvInt("URNET_REC_RESIZE_MS", 15000),
		recoveryEnvInt("URNET_REC_PING_ALWAYS", 0),
		recoveryEnvInt("URNET_REC_CPING_REST_MS", 10000),
		recoveryEnvInt("URNET_REC_CPING_TIMEOUT_MS", 30000),
		int64(settings.SendStallTimeout/time.Millisecond),
		preKillRate,
		ms(removedAt),
		ms(addedAt),
		gapMs,
		ms(recover90))

	if recover90.IsZero() {
		t.Fatalf("[recovery] no recovery within %d ms", maxWaitMs)
	}
}

func recoveryEnvInt(name string, defaultValue int) int {
	if value := os.Getenv(name); value != "" {
		if parsed, err := strconv.Atoi(value); err == nil {
			return parsed
		}
	}
	return defaultValue
}

// recoveryGenerator is a multi-provider MultiClientGenerator +
// MultiClientGeneratorWithDestination + MultiClientGeneratorWithIpFamily for
// the recovery kernel: each window client binds to one provider over a
// buffered gateway route pair. Every provider egresses to the loopback echo
// of either family, so the family-aware discovery reports each one as
// dualstack; without that, a v6 flow would have no exit able to carry it
// (a generator that answers only the plain NextDestinations reads as legacy
// v4-only).
type recoveryGenerator struct {
	mutex            sync.Mutex
	windowProvider   map[Id]Id // window clientId -> provider clientId
	unsubs           map[*Client]func()
	nextDestinations func(excluded map[Id]bool) map[MultiHopId]DestinationStats
	anyLiveProvider  func() (Id, bool)
	providerClientOf func(id Id) *Client
}

func (self *recoveryGenerator) NextDestinations(count int, excludeDestinations []MultiHopId, rankMode string) (map[MultiHopId]DestinationStats, error) {
	excluded := map[Id]bool{}
	for _, destination := range excludeDestinations {
		if 0 < destination.Len() {
			excluded[destination.Tail()] = true
		}
	}
	return self.nextDestinations(excluded), nil
}

// NextDestinationsWithIpFamily offers the same live providers, each stamped
// dualstack, for any filter a dualstack provider satisfies; an exact
// single-family filter matches none of them.
func (self *recoveryGenerator) NextDestinationsWithIpFamily(count int, excludeDestinations []MultiHopId, rankMode string, ipFamily IpFamilyFilter) (map[MultiHopId]DestinationStats, error) {
	destinations, err := self.NextDestinations(count, excludeDestinations, rankMode)
	if err != nil {
		return nil, err
	}
	if !ipFamily.Matches(IpFamilyDualstack) {
		return map[MultiHopId]DestinationStats{}, nil
	}
	for destination, stats := range destinations {
		stats.IpFamily = IpFamilyDualstack
		destinations[destination] = stats
	}
	return destinations, nil
}

func (self *recoveryGenerator) NewClientArgs() (*MultiClientGeneratorClientArgs, error) {
	providerId, ok := self.anyLiveProvider()
	if !ok {
		return nil, fmt.Errorf("no live provider")
	}
	return self.newArgsForProvider(providerId), nil
}

func (self *recoveryGenerator) NewClientArgsForDestination(destination MultiHopId) (*MultiClientGeneratorClientArgs, error) {
	if destination.Len() == 0 {
		return self.NewClientArgs()
	}
	return self.newArgsForProvider(destination.Tail()), nil
}

func (self *recoveryGenerator) newArgsForProvider(providerId Id) *MultiClientGeneratorClientArgs {
	windowId := NewId()
	self.mutex.Lock()
	self.windowProvider[windowId] = providerId
	self.mutex.Unlock()
	return &MultiClientGeneratorClientArgs{ClientId: windowId, ClientAuth: nil}
}

func (self *recoveryGenerator) RemoveClientArgs(args *MultiClientGeneratorClientArgs) {}

func (self *recoveryGenerator) RemoveClientWithArgs(client *Client, args *MultiClientGeneratorClientArgs) {
	self.mutex.Lock()
	unsub, ok := self.unsubs[client]
	if ok {
		delete(self.unsubs, client)
	}
	delete(self.windowProvider, args.ClientId)
	self.mutex.Unlock()
	if ok {
		unsub()
	}
}

func (self *recoveryGenerator) NewClientSettings() *ClientSettings {
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	return settings
}

func (self *recoveryGenerator) NewClient(clientCtx context.Context, args *MultiClientGeneratorClientArgs, clientSettings *ClientSettings) (*Client, error) {
	self.mutex.Lock()
	providerId := self.windowProvider[args.ClientId]
	self.mutex.Unlock()
	providerClient := self.providerClientOf(providerId)
	if providerClient == nil {
		return nil, fmt.Errorf("provider gone")
	}

	client := NewClient(clientCtx, args.ClientId, NewNoContractClientOob(), clientSettings)

	// A genuinely deep route approximates a live relay that keeps absorbing
	// writes after the peer dies, isolating the ack/probe path from the
	// route-full WriteTimeout mode. At 200 pps a small buffer fills in
	// hundreds of ms and accidentally measures the latter. Set a small
	// explicit URNET_REC_ROUTEBUF (for example 64) to exercise route-full
	// recovery.
	routeBuf := recoveryEnvInt("URNET_REC_ROUTEBUF", 16384)
	routeToProvider := make(chan []byte, routeBuf)
	routeToClient := make(chan []byte, routeBuf)

	sendTransport := NewSendGatewayTransport()
	receiveTransport := NewReceiveGatewayTransport()
	client.RouteManager().UpdateTransport(sendTransport, []Route{routeToProvider})
	client.RouteManager().UpdateTransport(receiveTransport, []Route{routeToClient})
	client.ContractManager().AddNoContractPeer(providerClient.ClientId())

	providerSendTransport := NewSendClientTransport(DestinationId(args.ClientId))
	providerReceiveTransport := NewReceiveGatewayTransport()
	providerClient.RouteManager().UpdateTransport(providerReceiveTransport, []Route{routeToProvider})
	providerClient.RouteManager().UpdateTransport(providerSendTransport, []Route{routeToClient})
	providerClient.ContractManager().AddNoContractPeer(args.ClientId)

	unsub := func() {
		client.RouteManager().RemoveTransport(sendTransport)
		client.RouteManager().RemoveTransport(receiveTransport)
		providerClient.RouteManager().RemoveTransport(providerReceiveTransport)
		providerClient.RouteManager().RemoveTransport(providerSendTransport)
	}
	self.mutex.Lock()
	self.unsubs[client] = unsub
	self.mutex.Unlock()

	return client, nil
}

// FixedDestinationSize false: not a fixed single-destination window, so the
// window expands across the live providers.
func (self *recoveryGenerator) FixedDestinationSize() (int, bool) {
	return 0, false
}
