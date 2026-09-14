// Tracks pooled ownership across complete client and fixture-worker lifecycles.
// Exact-owner barriers distinguish late callback handoffs from sampling noise.
package connect

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestMultiClientLifecyclePoolBalance pins the message-pool balance across the
// multi-client's whole lifecycle: build a RemoteUserNatMultiClient against an
// in-memory exit, push packets through the egress path, tear everything down, and
// require every pooled buffer back. This is the reconfiguration cycle a device
// performs on every destination change, so a single lost return here compounds by
// reconnect count in production.
func TestMultiClientLifecyclePoolBalance(t *testing.T) {
	poolOutstanding := func() int64 {
		taken, returned, _ := MessagePoolCounts()
		return int64(taken) - int64(returned)
	}
	poolOutstandingByClass := func() map[int]int64 {
		outstanding := map[int]int64{}
		for _, stats := range GetMessagePoolClassStats() {
			outstanding[stats.Size] = int64(stats.Taken) - int64(stats.Returned)
		}
		return outstanding
	}
	settle := func() int64 {
		prev := poolOutstanding()
		stableCount := 0
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
			n := poolOutstanding()
			if n == prev {
				stableCount += 1
				if 4 <= stableCount {
					break
				}
			} else {
				stableCount = 0
				prev = n
			}
		}
		return prev
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// warmup cycle to initialize process-global pools before the baseline
	runMultiClientPoolCycle(ctx, t, nil)
	before := settle()
	beforeByClass := poolOutstandingByClass()

	const cycles = 10
	for i := 0; i < cycles; i += 1 {
		runMultiClientPoolCycle(ctx, t, nil)
	}

	after := settle()
	if before < after {
		afterByClass := poolOutstandingByClass()
		// Diagnose whether CloseAndWait lost an owner permanently or published
		// before a delayed transport return. Either violates this lifecycle
		// boundary; the late count keeps the failure signature actionable.
		time.Sleep(2 * time.Second)
		late := settle()
		growthByClass := map[int]int64{}
		for size, afterCount := range afterByClass {
			if growth := afterCount - beforeByClass[size]; growth != 0 {
				growthByClass[size] = growth
			}
		}
		t.Errorf("pool buffers not returned across %d multi-client lifecycles: outstanding %d -> %d (+%d), late=%d, class growth=%v",
			cycles, before, after, after-before, late, growthByClass)
	}
}

// A receive callback captured before unsubscribe may publish its pooled echo
// after the sender stops. Complete fixture teardown must return that exact owner.
func TestMultiClientLifecyclePoolBalanceReturnsLateProviderEcho(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	captures := make(chan *lifecyclePoolCapture, 1)
	releaseEcho := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseEcho) })
	var captured atomic.Bool
	var capture *lifecyclePoolCapture
	defer func() {
		if capture != nil {
			capture.cleanup()
		}
	}()
	hooks := &multiClientPoolCycleHooks{
		beforeEchoPublish: func(packet []byte) {
			if captured.CompareAndSwap(false, true) {
				captures <- newLifecyclePoolCapture(packet)
				select {
				case <-releaseEcho:
				case <-ctx.Done():
				}
			}
		},
		beforeClose: func() {
			select {
			case capture = <-captures:
				capture.requireOwnerLive(t, "in-flight provider echo")
			case <-ctx.Done():
				t.Fatalf("wait for captured provider echo: %v", ctx.Err())
			}
		},
		afterEchoWorkerDone: func() {
			// The sender can no longer consume this publication. The client
			// join still waits for the callback that is about to produce it.
			releaseOnce.Do(func() { close(releaseEcho) })
		},
	}
	runMultiClientPoolCycle(ctx, t, hooks)
	if capture == nil {
		t.Fatal("cycle did not capture a provider echo")
	}
	capture.requireOwnerReturned(t, "provider echo published after sender shutdown")
}

// The simple single-destination client uses the same raw v2 envelope as the
// multi-client. A successful send must consume exactly the caller's packet
// reference; taking an extra read-only share here leaves one reference
// outstanding on every packet even though all transfer queues drain cleanly.
func TestRemoteUserNatClientRawSendPoolBalance(t *testing.T) {
	poolOutstanding := func() int64 {
		taken, returned, _ := MessagePoolCounts()
		return int64(taken) - int64(returned)
	}
	settle := func() int64 {
		prev := poolOutstanding()
		stableCount := 0
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
			n := poolOutstanding()
			if n == prev {
				stableCount += 1
				if 4 <= stableCount {
					break
				}
			} else {
				stableCount = 0
				prev = n
			}
		}
		return prev
	}

	ctx, cancel := context.WithCancel(context.Background())
	settings := DefaultClientSettingsWithBufferSize(32)
	providerClient := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)

	received := make(chan struct{}, 32)
	providerClient.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, peer Peer) {
		for _, frame := range frames {
			if frame.MessageType == protocol.MessageType_IpIpPacketToProvider {
				select {
				case received <- struct{}{}:
				default:
				}
			}
		}
	})
	natClient, err := testingNewClient(
		ctx,
		providerClient,
		func(TransferPath, protocol.ProvideMode, *IpPath, []byte) {},
	)
	if err != nil {
		t.Fatal(err)
	}

	before := settle()
	source := SourceId(NewId())
	const packetCount = 16
	for i := 0; i < packetCount; i += 1 {
		packet := poolBalanceUdp4Packet(
			net.ParseIP("10.0.0.1"),
			40000+i,
			net.ParseIP("203.0.113.7"),
			33434,
			[]byte("single-client pool balance"),
		)
		if !natClient.SendPacket(source, protocol.ProvideMode_Network, packet, time.Second) {
			MessagePoolReturn(packet)
			t.Fatal("single-destination send was not accepted")
		}
	}
	for i := 0; i < packetCount; i += 1 {
		select {
		case <-received:
		case <-time.After(5 * time.Second):
			t.Fatalf("provider received %d/%d packets", i, packetCount)
		}
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	if joinableNatClient, ok := natClient.(interface {
		CloseAndWait(context.Context) error
	}); ok {
		if err := joinableNatClient.CloseAndWait(closeCtx); err != nil {
			t.Fatalf("join single-destination local NAT: %v", err)
		}
	} else {
		natClient.Close()
	}
	if err := providerClient.CloseAndWait(closeCtx); err != nil {
		t.Fatalf("join single-destination provider client: %v", err)
	}
	cancel()
	after := settle()
	if before < after {
		t.Fatalf("single-destination raw sends left pooled buffers outstanding: %d -> %d (+%d)",
			before, after, after-before)
	}
}

// A rejected multi-client race candidate never takes ownership. The race
// helper must undo only its read-only share and leave the caller's original
// packet live for another candidate or retry.
func TestMultiClientRejectedRaceAttemptRetainsOriginalPacket(t *testing.T) {
	clientCtx, clientCancel := context.WithCancel(context.Background())
	client := NewClient(clientCtx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	clientCancel()
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := client.CloseAndWait(closeCtx); err != nil {
			t.Errorf("join rejected race client: %v", err)
		}
	})

	settings := DefaultMultiClientSettings()
	channelCtx, channelCancel := context.WithCancel(context.Background())
	defer channelCancel()
	channel := &multiClientChannel{
		ctx:                       channelCtx,
		cancel:                    channelCancel,
		log:                       NewNoopLogger(),
		args:                      &multiClientChannelArgs{},
		settings:                  settings,
		client:                    client,
		eventBuckets:              []*multiClientEventBucket{},
		ip4DestinationSourceCount: map[Ip4Path]map[Ip4Path]int{},
		ip6DestinationSourceCount: map[Ip6Path]map[Ip6Path]int{},
		packetStats:               &clientWindowStats{log: NewNoopLogger()},
	}

	ipPath := &IpPath{
		Version:         4,
		Protocol:        IpProtocolUdp,
		SourceIp:        net.ParseIP("10.0.0.1"),
		SourcePort:      40000,
		DestinationIp:   net.ParseIP("203.0.113.7"),
		DestinationPort: 443,
	}
	packet := poolBalanceUdp4Packet(
		ipPath.SourceIp,
		ipPath.SourcePort,
		ipPath.DestinationIp,
		ipPath.DestinationPort,
		[]byte("rejected race ownership"),
	)

	if sendMultiClientRaceAttempt(channel, packet, ipPath, 0) {
		MessagePoolReturn(packet)
		t.Fatal("canceled client accepted race packet")
	}
	if pooled, _ := MessagePoolCheck(packet); !pooled {
		t.Fatal("rejected race attempt returned the caller's original packet")
	}
	if returned := MessagePoolReturn(packet); !returned {
		t.Fatal("original packet was not the final live reference after rejected race attempt")
	}
}

// A rejected race candidate and an accepted sibling overlap in production:
// the multi-client releases the original race owner when any candidate wins,
// then SendSequence releases the winner's share asynchronously. The rejected
// candidate must not consume either of those two references.
func TestMultiClientRejectedRaceAttemptRetainsSuccessfulSiblingPacket(t *testing.T) {
	clientCtx, clientCancel := context.WithCancel(context.Background())
	client := NewClient(clientCtx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	clientCancel()
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := client.CloseAndWait(closeCtx); err != nil {
			t.Errorf("join mixed race client: %v", err)
		}
	})

	channelCtx, channelCancel := context.WithCancel(context.Background())
	defer channelCancel()
	channel := &multiClientChannel{
		ctx:                       channelCtx,
		cancel:                    channelCancel,
		log:                       NewNoopLogger(),
		args:                      &multiClientChannelArgs{},
		settings:                  DefaultMultiClientSettings(),
		client:                    client,
		eventBuckets:              []*multiClientEventBucket{},
		ip4DestinationSourceCount: map[Ip4Path]map[Ip4Path]int{},
		ip6DestinationSourceCount: map[Ip6Path]map[Ip6Path]int{},
		packetStats:               &clientWindowStats{log: NewNoopLogger()},
	}
	ipPath := &IpPath{
		Version:         4,
		Protocol:        IpProtocolUdp,
		SourceIp:        net.ParseIP("10.0.0.1"),
		SourcePort:      40000,
		DestinationIp:   net.ParseIP("203.0.113.7"),
		DestinationPort: 443,
	}
	packet := poolBalanceUdp4Packet(
		ipPath.SourceIp,
		ipPath.SourcePort,
		ipPath.DestinationIp,
		ipPath.DestinationPort,
		[]byte("mixed race ownership"),
	)
	successfulSiblingPacket := MessagePoolShareReadOnly(packet)

	if sendMultiClientRaceAttempt(channel, packet, ipPath, 0) {
		MessagePoolReturn(successfulSiblingPacket)
		MessagePoolReturn(packet)
		t.Fatal("canceled client accepted race packet")
	}
	if returned := MessagePoolReturn(packet); returned {
		t.Fatal("original race owner was the final reference while a successful sibling remained")
	}
	if returned := MessagePoolReturn(successfulSiblingPacket); !returned {
		t.Fatal("successful sibling did not retain the final live packet reference")
	}
}

// The production multi-client race fans one original packet into every
// candidate. When every candidate refuses admission, each candidate returns
// exactly its share and sendPacket leaves the original with its caller.
func TestMultiClientRejectedProductionRaceRetainsOriginalPacket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultMultiClientSettings()
	settings.DestinationAffinity = false
	generator := &TestMultiClientGenerator{}
	clients := map[Id]*multiClientChannel{}
	for clientIndex := 0; clientIndex < 2; clientIndex += 1 {
		clientCtx, clientCancel := context.WithCancel(ctx)
		client := NewClient(
			clientCtx,
			NewId(),
			NewNoContractClientOob(),
			DefaultClientSettings(),
		)
		clientCancel()
		t.Cleanup(func() {
			closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer closeCancel()
			if err := client.CloseAndWait(closeCtx); err != nil {
				t.Errorf("join production race client: %v", err)
			}
		})
		channelCtx, channelCancel := context.WithCancel(ctx)
		defer channelCancel()
		channel := &multiClientChannel{
			ctx:                       channelCtx,
			cancel:                    channelCancel,
			log:                       NewNoopLogger(),
			args:                      &multiClientChannelArgs{},
			settings:                  settings,
			client:                    client,
			eventBuckets:              []*multiClientEventBucket{},
			ip4DestinationSourceCount: map[Ip4Path]map[Ip4Path]int{},
			ip6DestinationSourceCount: map[Ip6Path]map[Ip6Path]int{},
			packetStats:               &clientWindowStats{log: NewNoopLogger()},
		}
		clients[client.ClientId()] = channel
	}
	window := &multiClientWindow{
		ctx:        ctx,
		log:        NewNoopLogger(),
		generator:  generator,
		windowType: WindowTypeQuality,
		settings:   settings,
		clients:    clients,
	}
	multiClient := &RemoteUserNatMultiClient{
		ctx:                ctx,
		cancel:             cancel,
		log:                NewNoopLogger(),
		generator:          generator,
		settings:           settings,
		windows:            map[WindowType]*multiClientWindow{WindowTypeQuality: window},
		ip4PathUpdates:     map[Ip4Path]*multiClientChannelUpdate{},
		ip6PathUpdates:     map[Ip6Path]*multiClientChannelUpdate{},
		affinityIp4Paths:   map[Ip4Path]map[Ip4Path]time.Time{},
		affinityIp6Paths:   map[Ip6Path]map[Ip6Path]time.Time{},
		clientUpdates:      map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
		reliabilityMetrics: newReliabilityMetrics(),
	}
	if candidates := multiClient.raceCandidates(window, 0); len(candidates) != 2 {
		t.Fatalf("production race candidates=%d, want=2", len(candidates))
	}
	packet := poolBalanceUdp4Packet(
		net.ParseIP("10.0.0.1"),
		40000,
		net.ParseIP("203.0.113.7"),
		33434,
		[]byte("rejected production race ownership"),
	)
	parsedPacket, err := newParsedPacket(packet)
	if err != nil {
		MessagePoolReturn(packet)
		t.Fatalf("parse production race packet: %v", err)
	}
	if multiClient.sendPacket(
		SourceId(NewId()),
		protocol.ProvideMode_Network,
		parsedPacket,
		0,
	) {
		MessagePoolReturn(packet)
		t.Fatal("canceled production candidates accepted race packet")
	}
	if pooled, _ := MessagePoolCheck(packet); !pooled {
		t.Fatal("rejected production race returned the caller's original packet")
	}
	if returned := MessagePoolReturn(packet); !returned {
		t.Fatal("production race caller did not retain the final packet reference")
	}
}

// Ordering barriers for the exact-owner regression; ordinary cycles leave them nil.
type multiClientPoolCycleHooks struct {
	beforeEchoPublish   func([]byte)
	beforeClose         func()
	afterEchoWorkerDone func()
}

// runMultiClientPoolCycle is one destination-change cycle: an in-memory exit, a
// multi-client over it, a burst of egress packets, then teardown of both.
func runMultiClientPoolCycle(ctx context.Context, t *testing.T, hooks *multiClientPoolCycleHooks) {
	t.Helper()
	cycleCtx, cycleCancel := context.WithCancel(ctx)
	defer cycleCancel()

	clientSettings := DefaultClientSettingsWithBufferSize(32)
	providerClient := NewClient(cycleCtx, NewId(), NewNoContractClientOob(), clientSettings)

	type providerEcho struct {
		frame       *protocol.Frame
		destination Id
		transferKey TransferKey
	}
	providerEchoes := make(chan providerEcho, 32)
	var providerEchoWaitGroup sync.WaitGroup
	providerEchoWaitGroup.Add(1)
	go func() {
		defer providerEchoWaitGroup.Done()
		for {
			select {
			case <-cycleCtx.Done():
				return
			case echo := <-providerEchoes:
				if !providerClient.SendWithTimeout(
					echo.frame,
					echo.destination,
					func(error) {},
					time.Second,
					echo.transferKey,
				) {
					MessagePoolReturn(echo.frame.MessageBytes)
				}
			}
		}
	}()

	// echo any IpPacketToProvider back with the path reversed, like an exit would
	providerReceiveUnsub := providerClient.AddReceiveCallback(func(src TransferPath, frames []*protocol.Frame, peer Peer) {
		for _, frame := range frames {
			if frame.MessageType != protocol.MessageType_IpIpPacketToProvider {
				continue
			}
			message, err := FromFrame(frame)
			if err != nil {
				continue
			}
			ipPacketToProvider, ok := message.(*protocol.IpPacketToProvider)
			if !ok {
				continue
			}
			ipPath, payload, err := ParseIpPathWithPayload(ipPacketToProvider.IpPacket.PacketBytes)
			if err != nil {
				continue
			}
			reversed := ipPath.Reverse()
			packet := poolBalanceUdp4Packet(reversed.SourceIp, reversed.SourcePort, reversed.DestinationIp, reversed.DestinationPort, payload)
			frame, err := ipPacketFromProviderFrame(packet, DefaultProtocolVersion)
			if err != nil {
				MessagePoolReturn(packet)
				continue
			}
			if hooks != nil && hooks.beforeEchoPublish != nil {
				hooks.beforeEchoPublish(frame.MessageBytes)
			}
			select {
			case providerEchoes <- providerEcho{
				frame:       frame,
				destination: src.SourceId,
				transferKey: peer.TransferKey,
			}:
			default:
				MessagePoolReturn(frame.MessageBytes)
			}
		}
	})
	defer func() {
		providerReceiveUnsub()
		cycleCancel()
		providerClient.Cancel()
		providerEchoWaitGroup.Wait()
		if hooks != nil && hooks.afterEchoWorkerDone != nil {
			hooks.afterEchoWorkerDone()
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := providerClient.CloseAndWait(closeCtx); err != nil {
			t.Errorf("join pool-balance provider client: %v", err)
		}
		// Unsubscribe does not join callbacks that already took a snapshot.
		// Quiesce the sender's client calls, join those callback producers,
		// then drain once no worker can publish another fixture-owned echo.
		for {
			select {
			case echo := <-providerEchoes:
				MessagePoolReturn(echo.frame.MessageBytes)
			default:
				return
			}
		}
	}()

	multiSettings := DefaultMultiClientSettings()
	multiSettings.SecurityPolicyGenerator = DisableSecurityPolicyWithStats
	received := make(chan struct{}, 64)
	generator := testMultiClientGenerator(providerClient)
	// RemoteUserNatMultiClient owns its windows, while the generator owns the
	// Clients it creates. Mirror ApiMultiClientGenerator's retirement join so
	// this pool assertion measures a complete lifecycle rather than sampling
	// generator-owned clients that the lightweight fixture left unjoined.
	var generatedClientsMutex sync.Mutex
	var generatedClients []*Client
	newClient := generator.newClient
	generator.newClient = func(
		ctx context.Context,
		args *MultiClientGeneratorClientArgs,
		settings *ClientSettings,
	) (*Client, error) {
		client, err := newClient(ctx, args, settings)
		if err == nil {
			generatedClientsMutex.Lock()
			generatedClients = append(generatedClients, client)
			generatedClientsMutex.Unlock()
		}
		return client, err
	}
	multi := NewRemoteUserNatMultiClient(
		cycleCtx,
		generator,
		func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
			select {
			case received <- struct{}{}:
			default:
			}
		},
		protocol.ProvideMode_Network,
		multiSettings,
	)
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := multi.CloseAndWait(closeCtx); err != nil {
			t.Errorf("join multi-client local NAT: %v", err)
		}
		generatedClientsMutex.Lock()
		clients := append([]*Client(nil), generatedClients...)
		generatedClientsMutex.Unlock()
		for _, client := range clients {
			client.Cancel()
		}
		for _, client := range clients {
			if err := client.CloseAndWait(closeCtx); err != nil {
				t.Errorf("join generator-owned client: %v", err)
			}
		}
	}()

	source := SourceId(NewId())
	for i := 0; i < 20; i += 1 {
		packet := poolBalanceUdp4Packet(net.ParseIP("10.0.0.1"), 40000+i, net.ParseIP("203.0.113.7"), 33434, []byte("pool balance probe"))
		if !multi.SendPacket(source, protocol.ProvideMode_Network, packet, 1*time.Second) {
			MessagePoolReturn(packet)
		}
	}
	if hooks != nil && hooks.beforeClose != nil {
		hooks.beforeClose()
		return
	}

	// wait briefly for echoes so the ingress path also runs
	echoDeadline := time.NewTimer(2 * time.Second)
	defer echoDeadline.Stop()
	echoes := 0
	for echoes < 1 {
		select {
		case <-received:
			echoes += 1
		case <-echoDeadline.C:
			t.Logf("cycle saw %d echoes (echo not required for balance)", echoes)
			return
		}
	}
}

// udp4Packet hand-rolls a valid IPv4/UDP packet into a pooled buffer.
func poolBalanceUdp4Packet(srcIp net.IP, srcPort int, dstIp net.IP, dstPort int, payload []byte) []byte {
	total := 28 + len(payload)
	packet := MessagePoolGet(total)
	packet[0] = 0x45
	packet[1] = 0
	packet[2] = byte(total >> 8)
	packet[3] = byte(total)
	packet[4], packet[5], packet[6], packet[7] = 0, 0, 0, 0
	packet[8] = 64
	packet[9] = 17
	packet[10], packet[11] = 0, 0
	copy(packet[12:16], srcIp.To4())
	copy(packet[16:20], dstIp.To4())
	packet[20] = byte(srcPort >> 8)
	packet[21] = byte(srcPort)
	packet[22] = byte(dstPort >> 8)
	packet[23] = byte(dstPort)
	udpLen := 8 + len(payload)
	packet[24] = byte(udpLen >> 8)
	packet[25] = byte(udpLen)
	packet[26], packet[27] = 0, 0
	copy(packet[28:], payload)
	return packet
}
