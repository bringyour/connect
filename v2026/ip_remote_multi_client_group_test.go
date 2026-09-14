// Homogeneous packet-group routing, policy, and ownership regressions.
package connect

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Records payload-aware policy calls and can reject a later group member.
type groupTestSecurityPolicy struct {
	stats        *SecurityPolicyStatsCollector
	inspectCount atomic.Int32
	refreshCount atomic.Int32
}

// Returns the collector used by the ordinary client diagnostics.
func (self *groupTestSecurityPolicy) Stats() *SecurityPolicyStatsCollector {
	return self.stats
}

// Blocks the member carrying the test's explicit marker.
func (self *groupTestSecurityPolicy) InspectEgress(
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	self.inspectCount.Add(1)
	if len(payload) != 0 && payload[0] == 0xff {
		return SecurityPolicyResultDrop, nil
	}
	return SecurityPolicyResultAllow, nil
}

// Allows the unused return direction.
func (self *groupTestSecurityPolicy) InspectIngress(
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	return SecurityPolicyResultAllow, nil
}

// Counts activity refreshes beside the inspections.
func (self *groupTestSecurityPolicy) RefreshEgress(ipPath *IpPath) {
	self.refreshCount.Add(1)
}

// The return direction is unused by these send-path tests.
func (self *groupTestSecurityPolicy) RefreshIngress(ipPath *IpPath) {
}

// Builds a bare parent whose real routing path is replaced by deterministic
// selected-client seams. The returned close function owns the update context.
func groupTestParent(
	t *testing.T,
	policy SecurityPolicy,
) (*RemoteUserNatMultiClient, *multiClientChannelUpdate, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	settings := DefaultMultiClientSettings()
	settings.TcpCollapsePrevention = false
	parent := &RemoteUserNatMultiClient{
		ctx:                  ctx,
		cancel:               cancel,
		generator:            &testingEmptyMultiClientGenerator{},
		settings:             settings,
		provideMode:          protocol.ProvideMode_Network,
		log:                  NewNoopLogger(),
		securityPolicy:       policy,
		localUserNat:         NewLocalUserNat(ctx, "group test", DefaultLocalUserNatSettings()),
		blockActionCache:     newBlockActionCache(time.Minute, 16),
		blockActionCollector: newBlockActionCollector(16, NewNoopLogger()),
		packetStatsCounters:  &packetStatsCounters{},
		reliabilityMetrics:   newReliabilityMetrics(),
		windows:              map[WindowType]*multiClientWindow{},
		clientUpdates:        map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
	}
	parent.config.Store(&multiClientConfig{})
	parent.blockActionState.Store(&blockActionState{})
	parent.blockActionIgnoreState.Store(&blockActionIgnoreState{})
	update := newMultiClientChannelUpdate(ctx, nil)
	parent.sendClientPathForTest = func(
		ipPath *IpPath,
		pin flowPin,
		callback func(*multiClientChannelUpdate, *multiClientChannel),
	) {
		update.ipPath = ipPath
		callback(update, update.client.Load())
	}
	return parent, update, func() {
		update.Close()
		parent.localUserNat.Close()
		cancel()
	}
}

// Retains one reference per packet so a successful sender can prove it
// consumed exactly the caller-owned reference without racing pool reuse.
func groupTestPacketWitnesses(t *testing.T, packets [][]byte) [][]byte {
	t.Helper()
	witnesses := make([][]byte, len(packets))
	for packetIndex, packet := range packets {
		if pooled, _ := MessagePoolCheck(packet); !pooled {
			for _, witness := range witnesses[:packetIndex] {
				MessagePoolReturn(witness)
			}
			t.Fatalf("packet %d is not pooled", packetIndex)
		}
		witnesses[packetIndex] = MessagePoolShareReadOnly(packet)
	}
	return witnesses
}

// Proves every production owner was returned before releasing the retained
// witnesses. On failure it also releases the leaked owner to isolate tests.
func requireGroupTestWitnessesReleased(t *testing.T, packets [][]byte, witnesses [][]byte) {
	t.Helper()
	for packetIndex, witness := range witnesses {
		if MessagePoolReturn(witness) {
			continue
		}
		MessagePoolReturn(packets[packetIndex])
		t.Errorf("packet %d retained an owner after successful group send", packetIndex)
	}
}

func groupTestPoolOutstanding() int64 {
	taken, returned, _ := MessagePoolCounts()
	return int64(taken) - int64(returned)
}

// Builds the minimal real channel state needed by the stalled-send ownership
// paths. No client is required because a stalled channel never enters Transfer.
func groupTestStalledChannel(protocolVersion int) *multiClientChannel {
	settings := DefaultMultiClientSettings()
	settings.ProtocolVersion = protocolVersion
	client := &multiClientChannel{
		ctx:                       context.Background(),
		log:                       NewNoopLogger(),
		settings:                  settings,
		eventBuckets:              []*multiClientEventBucket{},
		ip4DestinationSourceCount: map[Ip4Path]map[Ip4Path]int{},
		ip6DestinationSourceCount: map[Ip6Path]map[Ip6Path]int{},
		packetStats:               &clientWindowStats{log: NewNoopLogger()},
	}
	client.stalled.Store(true)
	return client
}

// Converts ordinary packets into the owned-header group used by the send path.
func requireGroupTestPacketGroup(t *testing.T, packets ...[]byte) *ipPacketGroup {
	t.Helper()
	groups, rejectedPackets := groupIpPackets(packets)
	if len(rejectedPackets) != 0 || len(groups) != 1 {
		t.Fatalf("grouped %d groups with %d rejects, want one group and no rejects", len(groups), len(rejectedPackets))
	}
	return groups[0]
}

// A later payload verdict applies to the whole group before any provider is
// selected. The old singular send could transmit the first member first.
func TestMultiClientPacketGroupLaterPolicyBlockRejectsWholeGroup(t *testing.T) {
	policy := &groupTestSecurityPolicy{stats: DefaultSecurityPolicyStatsCollector()}
	parent, _, closeParent := groupTestParent(t, policy)
	defer closeParent()

	selectionCount := atomic.Int32{}
	parent.sendClientPathForTest = func(
		ipPath *IpPath,
		pin flowPin,
		callback func(*multiClientChannelUpdate, *multiClientChannel),
	) {
		selectionCount.Add(1)
		t.Fatal("blocked group reached provider selection")
	}

	packet1 := ipOosUdpPacket(udpTestPath(4), []byte{1})
	packet2 := ipOosUdpPacket(udpTestPath(4), []byte{0xff})
	group := requireGroupTestPacketGroup(t, packet1, packet2)
	if parent.sendPacketGroup(SourceId(NewId()), protocol.ProvideMode_Network, group, 0) {
		t.Fatal("group with a blocked later member was accepted")
	}
	if got := policy.inspectCount.Load(); got != 2 {
		t.Errorf("policy inspections = %d, want 2 ordered member scans", got)
	}
	if got := policy.refreshCount.Load(); got != 1 {
		t.Errorf("policy refreshes = %d, want one group activity refresh", got)
	}
	if got := selectionCount.Load(); got != 0 {
		t.Errorf("provider selections = %d, want 0", got)
	}
	stats := parent.PacketStats()
	if stats.BlockEgressPacketCount != 2 {
		t.Errorf("blocked packet count = %d, want 2", stats.BlockEgressPacketCount)
	}
}

// One selected-client call owns the complete group in source order.
func TestMultiClientPacketGroupSelectedClientAdmitsWholeGroupOnce(t *testing.T) {
	parent, update, closeParent := groupTestParent(t, DisableSecurityPolicy())
	defer closeParent()

	var callCount atomic.Int32
	var finalAcceptedOwnerReturns atomic.Int32
	var receivedPayloads [][]byte
	client := &multiClientChannel{
		ctx:      parent.ctx,
		settings: parent.settings,
		sendGroupForTest: func(group *parsedPacketGroup, timeout time.Duration, ack bool) (bool, error) {
			callCount.Add(1)
			for packetIndex := range group.packets {
				receivedPayloads = append(receivedPayloads, append([]byte(nil), group.packets[packetIndex].payload...))
				if MessagePoolReturn(group.packets[packetIndex].packet) {
					finalAcceptedOwnerReturns.Add(1)
				}
			}
			return true, nil
		},
	}
	update.client.Store(client)

	packet1 := MessagePoolCopy(ipOosUdpPacket(udpTestPath(4), []byte{1}))
	packet2 := MessagePoolCopy(ipOosUdpPacket(udpTestPath(4), []byte{2}))
	packet3 := MessagePoolCopy(ipOosUdpPacket(udpTestPath(4), []byte{3}))
	packets := [][]byte{packet1, packet2, packet3}
	witnesses := groupTestPacketWitnesses(t, packets)
	group := requireGroupTestPacketGroup(t, packet1, packet2, packet3)
	if !parent.sendPacketGroup(SourceId(NewId()), protocol.ProvideMode_Network, group, 0) {
		t.Fatal("selected client rejected group")
	}
	if got := callCount.Load(); got != 1 {
		t.Fatalf("selected-client calls = %d, want 1", got)
	}
	if got := finalAcceptedOwnerReturns.Load(); got != 0 {
		t.Errorf("accepted owner final returns = %d, want 0 while witnesses are retained", got)
	}
	for packetIndex, payload := range receivedPayloads {
		if len(payload) != 1 || payload[0] != byte(packetIndex+1) {
			t.Errorf("payload %d = %v, want [%d]", packetIndex, payload, packetIndex+1)
		}
	}
	requireGroupTestWitnessesReleased(t, packets, witnesses)
}

// A one-candidate first send has no selected-client success commit. Its
// ordered SYN/RST controls must therefore reset stale collapse state before
// candidate admission, as the singular path did.
func TestMultiClientPacketGroupOneCandidateResetsControlSequenceBeforeSend(t *testing.T) {
	parent, update, closeParent := groupTestParent(t, DisableSecurityPolicy())
	defer closeParent()

	update.ackSequenceNumber = 91
	update.sequenceNumber = 92
	update.sequencePacketCount = 93
	update.sequenceTime = time.Time{}

	tcpPath := udpTestPath(4)
	tcpPath.Protocol = IpProtocolTcp
	const synSequence = uint32(101)
	const rstSequence = uint32(202)
	packets := [][]byte{
		MessagePoolCopy(ipOosTcpPacketSequence(tcpPath, tcpFlagSyn, synSequence, nil)),
		MessagePoolCopy(ipOosTcpPacketSequence(tcpPath, tcpFlagRst, rstSequence, nil)),
	}
	witnesses := groupTestPacketWitnesses(t, packets)
	group := requireGroupTestPacketGroup(t, packets...)

	type sequenceState struct {
		ackSequenceNumber uint32
		sequenceNumber    uint32
		packetCount       int
		sequenceTime      time.Time
	}
	observedState := sequenceState{}
	var callCount atomic.Int32
	client := &multiClientChannel{
		ctx:      parent.ctx,
		settings: parent.settings,
		sendGroupForTest: func(group *parsedPacketGroup, timeout time.Duration, ack bool) (bool, error) {
			callCount.Add(1)
			update.stateLock.Lock()
			observedState = sequenceState{
				ackSequenceNumber: update.ackSequenceNumber,
				sequenceNumber:    update.sequenceNumber,
				packetCount:       update.sequencePacketCount,
				sequenceTime:      update.sequenceTime,
			}
			update.stateLock.Unlock()
			for packetIndex := range group.packets {
				MessagePoolReturn(group.packets[packetIndex].packet)
			}
			return true, nil
		},
	}
	parent.groupRaceCandidatesForTest = func(group *parsedPacketGroup) []*multiClientChannel {
		return []*multiClientChannel{client}
	}

	if !parent.sendPacketGroup(SourceId(NewId()), protocol.ProvideMode_Network, group, 0) {
		t.Error("one-candidate control group was rejected")
	}
	if got := callCount.Load(); got != 1 {
		t.Errorf("candidate sends = %d, want 1", got)
	}
	if observedState.ackSequenceNumber != 0 ||
		observedState.sequenceNumber != rstSequence ||
		observedState.packetCount != 0 ||
		observedState.sequenceTime.IsZero() {
		t.Errorf(
			"pre-send sequence = ack:%d sequence:%d packets:%d time-zero:%t; want ack:0 sequence:%d packets:0 time-zero:false",
			observedState.ackSequenceNumber,
			observedState.sequenceNumber,
			observedState.packetCount,
			observedState.sequenceTime.IsZero(),
			rstSequence,
		)
	}
	if update.client.Load() != client {
		t.Error("one accepted candidate was not bound")
	}
	requireGroupTestWitnessesReleased(t, packets, witnesses)
}

// Every candidate gets every member as a read-only share, and the originals
// transfer exactly once only after all barrier-held attempts finish.
func TestMultiClientPacketGroupRaceKeepsMembersTogether(t *testing.T) {
	parent, update, closeParent := groupTestParent(t, DisableSecurityPolicy())
	defer closeParent()

	type attempt struct {
		client  *multiClientChannel
		packets [][]byte
	}
	attempts := make(chan attempt, 2)
	finishAttempts := make(chan struct{})
	var finishOnce sync.Once
	finish := func() {
		finishOnce.Do(func() {
			close(finishAttempts)
		})
	}
	defer finish()
	var finalAcceptedOwnerReturns atomic.Int32
	makeClient := func() *multiClientChannel {
		client := &multiClientChannel{
			ctx:      parent.ctx,
			settings: parent.settings,
		}
		client.sendGroupForTest = func(group *parsedPacketGroup, timeout time.Duration, ack bool) (bool, error) {
			packets := make([][]byte, len(group.packets))
			for packetIndex := range group.packets {
				packets[packetIndex] = group.packets[packetIndex].packet
			}
			attempts <- attempt{client: client, packets: packets}
			<-finishAttempts
			for _, packet := range packets {
				if MessagePoolReturn(packet) {
					finalAcceptedOwnerReturns.Add(1)
				}
			}
			return true, nil
		}
		return client
	}
	client1 := makeClient()
	client2 := makeClient()
	parent.groupRaceCandidatesForTest = func(group *parsedPacketGroup) []*multiClientChannel {
		return []*multiClientChannel{client1, client2}
	}

	packet1 := MessagePoolCopy(ipOosUdpPacket(udpTestPath(4), []byte{1}))
	packet2 := MessagePoolCopy(ipOosUdpPacket(udpTestPath(4), []byte{2}))
	packets := [][]byte{packet1, packet2}
	witnesses := groupTestPacketWitnesses(t, packets)
	group := requireGroupTestPacketGroup(t, packet1, packet2)
	result := make(chan bool, 1)
	go func() {
		result <- parent.sendPacketGroup(SourceId(NewId()), protocol.ProvideMode_Network, group, time.Second)
	}()

	attempt1 := <-attempts
	attempt2 := <-attempts
	for attemptIndex, observed := range []attempt{attempt1, attempt2} {
		if len(observed.packets) != 2 {
			t.Errorf("attempt %d packets = %d, want 2", attemptIndex, len(observed.packets))
		}
		for packetIndex, packet := range observed.packets {
			pooled, shared := MessagePoolCheck(packet)
			if !pooled || !shared {
				t.Errorf("attempt %d packet %d pooled/shared = %t/%t, want true/true", attemptIndex, packetIndex, pooled, shared)
			}
		}
	}
	finish()
	if !<-result {
		t.Error("whole-group race reported no accepted candidate")
	}
	if update.client.Load() != nil {
		t.Error("send-side group race committed before response evidence")
	}
	if attempt1.client == attempt2.client {
		t.Error("both attempts used the same candidate")
	}
	if got := finalAcceptedOwnerReturns.Load(); got != 0 {
		t.Errorf("accepted race-share final returns = %d, want 0 while original witness is retained", got)
	}
	requireGroupTestWitnessesReleased(t, packets, witnesses)
}

// A provider response may race the local queue-admission return. The flow and
// every candidate must be registered before SendGroup can make that response
// observable; registering afterwards drops a real SYN-ACK as "no race and no
// client" and leaves the kernel retransmitting through exits that did answer.
func TestMultiClientRaceRegistersCandidatesBeforeSend(t *testing.T) {
	parent, update, closeParent := groupTestParent(t, DisableSecurityPolicy())
	defer closeParent()
	parent.settings.MultiRaceSetOnResponseTimeout = time.Hour
	parent.settings.MultiRaceClientEarlyCompleteFraction = 2
	delivered := make(chan struct{}, 1)
	parent.SetReceivePacketCallback(func(
		source TransferPath,
		provideMode protocol.ProvideMode,
		ipPath *IpPath,
		packet []byte,
	) {
		delivered <- struct{}{}
	})

	tcpPath := udpTestPath(4)
	tcpPath.Protocol = IpProtocolTcp
	packet := MessagePoolCopy(ipOosTcpPacketSequence(tcpPath, tcpFlagSyn, 1000, nil))
	group := requireGroupTestPacketGroup(t, packet)
	parent.ip4PathUpdates = map[Ip4Path]*multiClientChannelUpdate{
		tcpPath.ToIp4Path(): update,
	}

	started := make(chan *multiClientChannel, 2)
	deliverResponse := make(chan struct{})
	responseDelivered := make(chan struct{})
	finishSends := make(chan struct{})
	var finishOnce sync.Once
	finish := func() {
		finishOnce.Do(func() {
			close(finishSends)
		})
	}
	defer finish()

	makeClient := func(respond bool) *multiClientChannel {
		client := groupTestStalledChannel(parent.settings.ProtocolVersion)
		client.ctx = parent.ctx
		client.settings = parent.settings
		client.sendGroupForTest = func(group *parsedPacketGroup, timeout time.Duration, ack bool) (bool, error) {
			started <- client
			if respond {
				<-deliverResponse
				parent.clientReceivePacketResolve(
					client,
					TransferPath{},
					protocol.ProvideMode_Network,
					group.ipPath,
					[]byte{1},
					tcpControlObservation{},
				)
				close(responseDelivered)
			}
			<-finishSends
			for packetIndex := range group.packets {
				MessagePoolReturn(group.packets[packetIndex].packet)
			}
			return true, nil
		}
		return client
	}
	client1 := makeClient(true)
	client2 := makeClient(false)
	parent.groupRaceCandidatesForTest = func(group *parsedPacketGroup) []*multiClientChannel {
		return []*multiClientChannel{client1, client2}
	}

	result := make(chan bool, 1)
	go func() {
		result <- parent.sendPacketGroup(SourceId(NewId()), protocol.ProvideMode_Network, group, time.Second)
	}()
	<-started
	<-started
	close(deliverResponse)
	<-responseDelivered

	update.stateLock.Lock()
	race := update.race
	packetCount := 0
	registered := false
	if race != nil {
		packetCount = race.packetCount
		_, registered = race.clientStates[client1]
	}
	update.stateLock.Unlock()
	if race == nil || !registered || packetCount != 1 {
		finish()
		<-result
		t.Fatalf(
			"synchronous response race = race:%t candidate:%t packets:%d; want true/true/1",
			race != nil,
			registered,
			packetCount,
		)
	}

	// Complete the retained race through its ordinary async path. The buffered
	// packet is not merely a winner vote: it must establish the flow before a
	// later dial-failure signal can act on it.
	race.completeMonitor.NotifyAll()
	finish()
	if !<-result {
		t.Fatal("race rejected candidates after retaining the synchronous response")
	}
	select {
	case <-delivered:
	case <-time.After(2 * time.Second):
		t.Fatal("retained synchronous response was not delivered")
	}
	if got := update.client.Load(); got != client1 {
		t.Fatalf("response race committed %p, want responding candidate %p", got, client1)
	}
	if !update.receivedInbound.Load() {
		t.Error("race-selected response did not establish the flow")
	}
}

// The one-candidate fast path has the same queue-admission boundary as a wide
// race. Its sole exit must be visible to receive resolution before SendGroup
// runs; otherwise the first SYN-ACK is dropped even though no winner decision
// is needed.
func TestMultiClientOneCandidateRegistersBeforeSend(t *testing.T) {
	parent, update, closeParent := groupTestParent(t, DisableSecurityPolicy())
	defer closeParent()
	poolOutstandingBefore := groupTestPoolOutstanding()

	tcpPath := udpTestPath(4)
	tcpPath.Protocol = IpProtocolTcp
	packet := MessagePoolCopy(ipOosTcpPacketSequence(tcpPath, tcpFlagSyn, 1000, nil))
	group := requireGroupTestPacketGroup(t, packet)
	parent.ip4PathUpdates = map[Ip4Path]*multiClientChannelUpdate{
		tcpPath.ToIp4Path(): update,
	}
	var delivered atomic.Int32
	parent.SetReceivePacketCallback(func(
		source TransferPath,
		provideMode protocol.ProvideMode,
		ipPath *IpPath,
		packet []byte,
	) {
		delivered.Add(1)
	})

	client := &multiClientChannel{
		ctx:      parent.ctx,
		settings: parent.settings,
	}
	client.sendGroupForTest = func(group *parsedPacketGroup, timeout time.Duration, ack bool) (bool, error) {
		responsePacket := MessagePoolCopy([]byte{1})
		parent.clientReceivePacketResolve(
			client,
			TransferPath{},
			protocol.ProvideMode_Network,
			group.ipPath,
			responsePacket,
			tcpControlObservation{},
		)
		MessagePoolReturn(responsePacket)
		for packetIndex := range group.packets {
			MessagePoolReturn(group.packets[packetIndex].packet)
		}
		return true, nil
	}
	parent.groupRaceCandidatesForTest = func(group *parsedPacketGroup) []*multiClientChannel {
		return []*multiClientChannel{client}
	}

	if !parent.sendPacketGroup(SourceId(NewId()), protocol.ProvideMode_Network, group, 0) {
		t.Fatal("one-candidate SYN was not accepted")
	}
	if got := update.client.Load(); got != client {
		t.Fatalf("one-candidate response committed %p, want %p", got, client)
	}
	if !update.receivedInbound.Load() {
		t.Error("one-candidate synchronous response did not establish the flow")
	}
	if got := delivered.Load(); got != 1 {
		t.Errorf("delivered responses = %d, want 1", got)
	}
	if poolOutstandingAfter := groupTestPoolOutstanding(); poolOutstandingAfter != poolOutstandingBefore {
		t.Errorf(
			"one-candidate synchronous response pool ownership = %d, want %d",
			poolOutstandingAfter,
			poolOutstandingBefore,
		)
	}
}

// Two device senders can snapshot an unbound flow before either enters
// provider selection. Once the first sender commits the sole candidate, the
// second must use that newly committed client instead of retrying forever with
// its stale nil snapshot.
func TestMultiClientOneCandidateStaleSnapshotUsesCommittedClient(t *testing.T) {
	parent, update, closeParent := groupTestParent(t, DisableSecurityPolicy())
	defer closeParent()

	var sendCount atomic.Int32
	client := &multiClientChannel{
		ctx:      parent.ctx,
		settings: parent.settings,
		sendGroupForTest: func(group *parsedPacketGroup, timeout time.Duration, ack bool) (bool, error) {
			sendCount.Add(1)
			for packetIndex := range group.packets {
				MessagePoolReturn(group.packets[packetIndex].packet)
			}
			return true, nil
		},
	}
	parent.groupRaceCandidatesForTest = func(group *parsedPacketGroup) []*multiClientChannel {
		return []*multiClientChannel{client}
	}

	entered := make(chan int, 2)
	releases := []chan struct{}{make(chan struct{}), make(chan struct{})}
	var pathCallCount atomic.Int32
	parent.sendClientPathForTest = func(
		ipPath *IpPath,
		pin flowPin,
		callback func(*multiClientChannelUpdate, *multiClientChannel),
	) {
		// Capture before either callback can commit the client. This is the
		// ordinary concurrent sendUpdate boundary reproduced without timing.
		snapshot := update.client.Load()
		callIndex := int(pathCallCount.Add(1)) - 1
		entered <- callIndex
		select {
		case <-parent.ctx.Done():
			return
		case <-releases[callIndex]:
		}
		update.ipPath = ipPath
		callback(update, snapshot)
	}

	send := func(payload byte) (<-chan bool, []byte) {
		packet := MessagePoolCopy(ipOosUdpPacket(udpTestPath(4), []byte{payload}))
		group := requireGroupTestPacketGroup(t, packet)
		result := make(chan bool, 1)
		go func() {
			result <- parent.sendPacketGroup(
				SourceId(NewId()),
				protocol.ProvideMode_Network,
				group,
				0,
			)
		}()
		return result, packet
	}

	result1, packet1 := send(1)
	if callIndex := <-entered; callIndex != 0 {
		t.Fatalf("first path call index = %d, want 0", callIndex)
	}
	result2, packet2 := send(2)
	if callIndex := <-entered; callIndex != 1 {
		t.Fatalf("second path call index = %d, want 1", callIndex)
	}
	close(releases[0])
	if !<-result1 {
		MessagePoolReturn(packet1)
		t.Fatal("first stale-snapshot sender did not commit the candidate")
	}
	if got := update.client.Load(); got != client {
		t.Fatalf("first sender committed %p, want %p", got, client)
	}

	close(releases[1])
	if !<-result2 {
		MessagePoolReturn(packet2)
		t.Fatal("second stale-snapshot sender did not use the committed client")
	}
	if got := sendCount.Load(); got != 2 {
		t.Fatalf("candidate sends = %d, want 2", got)
	}
}

// The adjacent no-response fallback exists for send-only traffic. It must not
// commit a TCP handshake when a custom timeout or packet limit reaches that
// branch: silence is not winner evidence for a SYN, and a caller can otherwise
// stay pinned to an arbitrary silent exit until its own deadline.
func TestMultiClientNoResponseRaceKeepsTCPHandshakeUncommitted(t *testing.T) {
	parent, update, closeParent := groupTestParent(t, DisableSecurityPolicy())
	defer closeParent()
	parent.settings.MultiRaceSetOnNoResponseTimeout = 0
	parent.settings.MultiRaceSetOnResponseTimeout = time.Hour

	var client1Sends atomic.Int32
	var client2Sends atomic.Int32
	makeClient := func(sendCount *atomic.Int32) *multiClientChannel {
		return &multiClientChannel{
			ctx:      parent.ctx,
			settings: parent.settings,
			sendGroupForTest: func(group *parsedPacketGroup, timeout time.Duration, ack bool) (bool, error) {
				sendCount.Add(1)
				for packetIndex := range group.packets {
					MessagePoolReturn(group.packets[packetIndex].packet)
				}
				return true, nil
			},
		}
	}
	client1 := makeClient(&client1Sends)
	client2 := makeClient(&client2Sends)
	parent.groupRaceCandidatesForTest = func(group *parsedPacketGroup) []*multiClientChannel {
		return []*multiClientChannel{client1, client2}
	}

	tcpProbeCount := dialProbeMaxSends + 1
	for sendIndex := 0; sendIndex < tcpProbeCount; sendIndex++ {
		tcpPath := udpTestPath(4)
		tcpPath.Protocol = IpProtocolTcp
		packet := MessagePoolCopy(ipOosTcpPacketSequence(tcpPath, tcpFlagSyn, 1000, nil))
		group := requireGroupTestPacketGroup(t, packet)
		if !parent.sendPacketGroup(SourceId(NewId()), protocol.ProvideMode_Network, group, 0) {
			t.Fatalf("SYN %d was not accepted by the race", sendIndex+1)
		}
	}

	if client := update.client.Load(); client != nil {
		t.Fatalf("an unanswered TCP handshake committed candidate %p", client)
	}
	if got := client1Sends.Load(); got != int32(tcpProbeCount) {
		t.Errorf("candidate 1 sends = %d, want %d", got, tcpProbeCount)
	}
	if got := client2Sends.Load(); got != int32(tcpProbeCount) {
		t.Errorf("candidate 2 sends = %d, want %d", got, tcpProbeCount)
	}
}

// QUIC is also a request-response handshake. Keep its initial probe budget in
// the wide race; otherwise the same no-evidence commitment strands QUIC on an
// arbitrary exit before its PTO recovery can find a responder.
func TestMultiClientNoResponseRaceKeepsQUICHandshakeUncommitted(t *testing.T) {
	parent, update, closeParent := groupTestParent(t, DisableSecurityPolicy())
	defer closeParent()
	parent.settings.MultiRaceSetOnNoResponseTimeout = 0
	parent.settings.MultiRaceSetOnResponseTimeout = time.Hour

	var sendCount atomic.Int32
	makeClient := func() *multiClientChannel {
		return &multiClientChannel{
			ctx:      parent.ctx,
			settings: parent.settings,
			sendGroupForTest: func(group *parsedPacketGroup, timeout time.Duration, ack bool) (bool, error) {
				sendCount.Add(1)
				for packetIndex := range group.packets {
					MessagePoolReturn(group.packets[packetIndex].packet)
				}
				return true, nil
			},
		}
	}
	client1 := makeClient()
	client2 := makeClient()
	parent.groupRaceCandidatesForTest = func(group *parsedPacketGroup) []*multiClientChannel {
		return []*multiClientChannel{client1, client2}
	}

	for sendIndex := 0; sendIndex < dialProbeMaxSends; sendIndex++ {
		packet := MessagePoolCopy(ipOosUdpPacket(udpTestPath(4), []byte{byte(sendIndex)}))
		group := requireGroupTestPacketGroup(t, packet)
		if !parent.sendPacketGroup(SourceId(NewId()), protocol.ProvideMode_Network, group, 0) {
			t.Fatalf("QUIC probe %d was not accepted", sendIndex+1)
		}
	}
	if client := update.client.Load(); client != nil {
		t.Fatalf("an unanswered QUIC handshake committed candidate %p inside its probe budget", client)
	}
	if got := sendCount.Load(); got != 2*dialProbeMaxSends {
		t.Errorf("QUIC candidate sends = %d, want %d", got, 2*dialProbeMaxSends)
	}

	// The existing stream guard remains bounded: after the response-probe
	// budget, a send-only UDP/443 flow may commit instead of racing forever.
	packet := MessagePoolCopy(ipOosUdpPacket(udpTestPath(4), []byte{0xff}))
	group := requireGroupTestPacketGroup(t, packet)
	if !parent.sendPacketGroup(SourceId(NewId()), protocol.ProvideMode_Network, group, 0) {
		t.Fatal("post-budget QUIC-shaped stream packet was not accepted")
	}
	if client := update.client.Load(); client == nil {
		t.Fatal("post-budget UDP/443 stream did not make its bounded no-response commitment")
	}
}

// When only one exit is eligible there is no race to preserve. Bind it, but
// start its silence clock on the first SYN; starting on the next retransmit
// consumes an extra rung of exponential backoff before a later exit can help.
func TestMultiClientOneCandidateTCPHandshakeStartsSilenceClock(t *testing.T) {
	parent, update, closeParent := groupTestParent(t, DisableSecurityPolicy())
	defer closeParent()

	client := &multiClientChannel{
		ctx:      parent.ctx,
		settings: parent.settings,
		sendGroupForTest: func(group *parsedPacketGroup, timeout time.Duration, ack bool) (bool, error) {
			for packetIndex := range group.packets {
				MessagePoolReturn(group.packets[packetIndex].packet)
			}
			return true, nil
		},
	}
	parent.groupRaceCandidatesForTest = func(group *parsedPacketGroup) []*multiClientChannel {
		return []*multiClientChannel{client}
	}

	tcpPath := udpTestPath(4)
	tcpPath.Protocol = IpProtocolTcp
	packet := MessagePoolCopy(ipOosTcpPacketSequence(tcpPath, tcpFlagSyn, 1000, nil))
	group := requireGroupTestPacketGroup(t, packet)
	if !parent.sendPacketGroup(SourceId(NewId()), protocol.ProvideMode_Network, group, 0) {
		t.Fatal("one-candidate SYN was not accepted")
	}
	if got := update.client.Load(); got != client {
		t.Fatalf("one-candidate SYN committed %p, want %p", got, client)
	}
	update.stateLock.Lock()
	waitClient := update.synWaitClient
	waitStart := update.synWaitStart
	waitSendCount := update.synWaitSendCount
	update.stateLock.Unlock()
	if waitClient != client || waitStart.IsZero() || waitSendCount != 1 {
		t.Fatalf(
			"one-candidate silence clock = client:%p start-zero:%t sends:%d; want client:%p start-zero:false sends:1",
			waitClient,
			waitStart.IsZero(),
			waitSendCount,
			client,
		)
	}
}

// A truly one-way UDP flow cannot supply response evidence. Preserve the
// historical bounded no-response commitment for ports that do not denote a
// request-response protocol.
func TestMultiClientNoResponseRaceCommitsOneWayUDP(t *testing.T) {
	parent, update, closeParent := groupTestParent(t, DisableSecurityPolicy())
	defer closeParent()
	parent.settings.MultiRaceSetOnNoResponseTimeout = 0
	parent.settings.MultiRaceSetOnResponseTimeout = time.Hour

	makeClient := func() *multiClientChannel {
		return &multiClientChannel{
			ctx:      parent.ctx,
			settings: parent.settings,
			sendGroupForTest: func(group *parsedPacketGroup, timeout time.Duration, ack bool) (bool, error) {
				for packetIndex := range group.packets {
					MessagePoolReturn(group.packets[packetIndex].packet)
				}
				return true, nil
			},
		}
	}
	client1 := makeClient()
	client2 := makeClient()
	parent.groupRaceCandidatesForTest = func(group *parsedPacketGroup) []*multiClientChannel {
		return []*multiClientChannel{client1, client2}
	}

	path := udpTestPath(4)
	path.DestinationPort = 5001
	packet := MessagePoolCopy(ipOosUdpPacket(path, []byte{1}))
	group := requireGroupTestPacketGroup(t, packet)
	if !parent.sendPacketGroup(SourceId(NewId()), protocol.ProvideMode_Network, group, 0) {
		t.Fatal("one-way UDP packet was not accepted")
	}
	if got := update.client.Load(); got != client1 && got != client2 {
		t.Fatalf("one-way UDP committed candidate %p, want one of %p or %p", got, client1, client2)
	}
}

// A simulated stalled exit consumes the packet just like an admitted Transfer
// while retaining its outstanding health accounting. Raw and legacy framing
// must both release every accepted buffer owner.
func TestMultiClientPacketStalledSendReturnsAcceptedOwner(t *testing.T) {
	for _, protocolVersion := range []int{DefaultProtocolVersion, 1} {
		t.Run(protocolVersionName(protocolVersion), func(t *testing.T) {
			before := groupTestPoolOutstanding()
			packet := MessagePoolCopy(ipOosUdpPacket(udpTestPath(4), []byte{1}))
			packets := [][]byte{packet}
			witnesses := groupTestPacketWitnesses(t, packets)
			ipPath, payload, err := ParseIpPathWithPayload(packet)
			if err != nil {
				t.Fatalf("parse packet: %v", err)
			}
			client := groupTestStalledChannel(protocolVersion)
			success, err := client.SendDetailedWithAck(&parsedPacket{
				packet:  packet,
				ipPath:  ipPath,
				payload: payload,
			}, 0, false)
			if err != nil || !success {
				t.Fatalf("stalled singleton send = %t, %v; want true, nil", success, err)
			}
			requireGroupTestWitnessesReleased(t, packets, witnesses)
			if after := groupTestPoolOutstanding(); after != before {
				t.Errorf("stalled singleton pool outstanding = %d -> %d", before, after)
			}
		})
	}
}

// The group stalled path has the singleton contract for every member and must
// also release every legacy wrapper built before the simulated blackhole.
func TestMultiClientPacketGroupStalledSendReturnsAcceptedOwners(t *testing.T) {
	for _, protocolVersion := range []int{DefaultProtocolVersion, 1} {
		t.Run(protocolVersionName(protocolVersion), func(t *testing.T) {
			before := groupTestPoolOutstanding()
			packets := [][]byte{
				MessagePoolCopy(ipOosUdpPacket(udpTestPath(4), []byte{1})),
				MessagePoolCopy(ipOosUdpPacket(udpTestPath(4), []byte{2})),
				MessagePoolCopy(ipOosUdpPacket(udpTestPath(4), []byte{3})),
			}
			witnesses := groupTestPacketWitnesses(t, packets)
			group := requireGroupTestPacketGroup(t, packets...)
			parsedPackets := make([]parsedPacket, len(packets))
			for packetIndex, packet := range packets {
				_, payload, err := ParseIpPathWithPayload(packet)
				if err != nil {
					t.Fatalf("parse group packet %d: %v", packetIndex, err)
				}
				parsedPackets[packetIndex] = parsedPacket{
					packet:  packet,
					ipPath:  group.ipPath,
					payload: payload,
				}
			}
			client := groupTestStalledChannel(protocolVersion)
			success, err := client.SendGroupDetailedWithAck(&parsedPacketGroup{
				packets:   parsedPackets,
				ipPath:    group.ipPath,
				byteCount: group.byteCount,
			}, 0, false)
			if err != nil || !success {
				t.Fatalf("stalled group send = %t, %v; want true, nil", success, err)
			}
			requireGroupTestWitnessesReleased(t, packets, witnesses)
			if after := groupTestPoolOutstanding(); after != before {
				t.Errorf("stalled group pool outstanding = %d -> %d", before, after)
			}
		})
	}
}

func protocolVersionName(protocolVersion int) string {
	if 2 <= protocolVersion {
		return "raw"
	}
	return "legacy"
}
