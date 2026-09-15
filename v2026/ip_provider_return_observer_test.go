// Provider return observer regressions pin exact item identity across batch
// chunking, downstream rejection, worker shutdown, and callback failure.
package connect

import (
	"bytes"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// providerReturnObserverItemFixture retains one test witness per packet while
// the provider item owns the original message-pool references.
type providerReturnObserverItemFixture struct {
	item       *providerReturnItem
	witnesses  [][]byte
	byteCounts []ByteCount
	totalBytes ByteCount
	flowKey    RemoteUserNatProviderReturnFlowKey
}

// newProviderReturnObserverItem builds one single-packet or batch return with
// distinct packet sizes so partial chunk accounting can be checked exactly.
func newProviderReturnObserverItem(
	t *testing.T,
	peerId Id,
	packetCount int,
	batch bool,
) *providerReturnObserverItemFixture {
	return newProviderReturnObserverItemForFlow(
		t,
		peerId,
		packetCount,
		batch,
		net.ParseIP("203.0.113.7"),
		8080,
		net.ParseIP("10.0.0.9"),
		42001,
	)
}

// A configurable tuple lets identity tests vary the authenticated peer and IP
// flow independently without changing item ownership or batching behavior.
func newProviderReturnObserverItemForFlow(
	t *testing.T,
	peerId Id,
	packetCount int,
	batch bool,
	sourceIp net.IP,
	sourcePort int,
	destinationIp net.IP,
	destinationPort int,
) *providerReturnObserverItemFixture {
	t.Helper()
	fixture := &providerReturnObserverItemFixture{
		item: &providerReturnItem{
			source:       SourceId(peerId),
			transferKey:  TransferKey{CompanionContract: true},
			provideMode:  protocol.ProvideMode_Public,
			recoveryMode: receiveRecoveryModeNonblocking,
			ipProtocol:   IpProtocolUdp,
			batch:        batch,
		},
		flowKey: providerReturnObserverExpectedFlowKey(
			t,
			peerId,
			sourceIp,
			sourcePort,
			destinationIp,
			destinationPort,
		),
	}
	for packetIndex := range packetCount {
		packet := MessagePoolCopy(craftSecurityPacket(
			IpProtocolUdp,
			sourceIp,
			sourcePort,
			destinationIp,
			destinationPort,
			false,
			bytes.Repeat([]byte{byte(packetIndex + 1)}, packetIndex+1),
		))
		pooled, _ := MessagePoolCheck(packet)
		if !pooled {
			MessagePoolReturn(packet)
			t.Fatalf("provider observer packet %d was not pooled", packetIndex)
		}
		fixture.witnesses = append(fixture.witnesses, MessagePoolShareReadOnly(packet))
		packetByteCount := ByteCount(len(packet))
		fixture.byteCounts = append(fixture.byteCounts, packetByteCount)
		fixture.totalBytes += packetByteCount
		if batch {
			fixture.item.packets = append(fixture.item.packets, packet)
		} else {
			if packetIndex != 0 {
				t.Fatal("single provider observer item received multiple packets")
			}
			fixture.item.packet = packet
		}
	}
	fixture.item.packetByteCount = fixture.totalBytes
	return fixture
}

// Expected keys are built without the production parser so exact address and
// port assertions cannot pass through a shared conversion bug.
func providerReturnObserverExpectedFlowKey(
	t *testing.T,
	peerId Id,
	sourceIp net.IP,
	sourcePort int,
	destinationIp net.IP,
	destinationPort int,
) RemoteUserNatProviderReturnFlowKey {
	t.Helper()
	key := RemoteUserNatProviderReturnFlowKey{
		DestinationId:   peerId,
		Protocol:        IpProtocolUdp,
		SourcePort:      uint16(sourcePort),
		DestinationPort: uint16(destinationPort),
		Valid:           true,
	}
	if sourceIp4 := sourceIp.To4(); sourceIp4 != nil {
		destinationIp4 := destinationIp.To4()
		if destinationIp4 == nil {
			t.Fatal("provider observer test tuple mixes IPv4 and IPv6")
		}
		copy(key.SourceIp[:], sourceIp4)
		copy(key.DestinationIp[:], destinationIp4)
		key.IpVersion = 4
		return key
	}
	sourceIp6 := sourceIp.To16()
	destinationIp6 := destinationIp.To16()
	if sourceIp6 == nil || destinationIp6 == nil || destinationIp.To4() != nil {
		t.Fatal("provider observer test tuple is not one valid IP family")
	}
	copy(key.SourceIp[:], sourceIp6)
	copy(key.DestinationIp[:], destinationIp6)
	key.IpVersion = 6
	return key
}

// releaseProviderReturnObserverWitnesses proves every provider/SendPack owner
// was returned before releasing the test's final reference.
func (self *providerReturnObserverItemFixture) releaseWitnesses(t *testing.T) {
	t.Helper()
	for packetIndex, witness := range self.witnesses {
		if !MessagePoolReturn(witness) {
			t.Fatalf("provider observer retained packet %d", packetIndex)
		}
	}
}

// providerReturnObserverEvents records an optional observer without blocking
// the provider's callback or ordered worker.
func providerReturnObserverEvents() (
	func(RemoteUserNatProviderReturnSendObservation),
	<-chan RemoteUserNatProviderReturnSendObservation,
) {
	events := make(chan RemoteUserNatProviderReturnSendObservation, 64)
	return func(observation RemoteUserNatProviderReturnSendObservation) {
		select {
		case events <- observation:
		default:
			panic("provider return observer test event overflow")
		}
	}, events
}

// waitProviderReturnObservation uses its context only as a liveness bound.
func waitProviderReturnObservation(
	t *testing.T,
	ctx context.Context,
	events <-chan RemoteUserNatProviderReturnSendObservation,
) RemoteUserNatProviderReturnSendObservation {
	t.Helper()
	select {
	case observation := <-events:
		return observation
	case <-ctx.Done():
		t.Fatalf("wait for provider return observation: %v", ctx.Err())
		return RemoteUserNatProviderReturnSendObservation{}
	}
}

// requireProviderReturnObservation verifies one exact observer phase.
func requireProviderReturnObservation(
	t *testing.T,
	observation RemoteUserNatProviderReturnSendObservation,
	phase RemoteUserNatProviderReturnSendPhase,
	token uint64,
	flowKey RemoteUserNatProviderReturnFlowKey,
	packetCount int,
	packetByteCount ByteCount,
	sent bool,
) {
	t.Helper()
	if observation.Phase != phase || observation.Token != token || token == 0 ||
		observation.FlowKey != flowKey ||
		observation.PacketCount != packetCount ||
		observation.PacketByteCount != packetByteCount || observation.Sent != sent {
		t.Fatalf(
			"provider return observation=%+v, want phase=%d token=%d flow=%+v count=%d bytes=%d sent=%t",
			observation,
			phase,
			token,
			flowKey,
			packetCount,
			packetByteCount,
			sent,
		)
	}
}

// waitProviderReturnObserverRelease joins one worker-owned item release.
func waitProviderReturnObserverRelease(
	t *testing.T,
	ctx context.Context,
	releases <-chan struct{},
) {
	t.Helper()
	select {
	case <-releases:
	case <-ctx.Done():
		t.Fatalf("wait for provider return item release: %v", ctx.Err())
	}
}

// A disabled observer leaves the item untouched, including its otherwise
// observable invalid destination key, so normal return traffic does no parsing.
func TestRemoteUserNatProviderReturnObserverDisabledSkipsFlowIdentity(t *testing.T) {
	settings := DefaultRemoteUserNatProviderSettings()
	provider := &RemoteUserNatProvider{settings: settings}
	item := &providerReturnItem{
		source: SourceId(NewId()),
		packet: []byte{0xff},
	}
	provider.beginReturnSendObservation(item)
	if item.observer != nil || item.observerToken != 0 ||
		item.observerFlowKey != (RemoteUserNatProviderReturnFlowKey{}) {
		t.Fatalf("disabled provider observer mutated item identity: %+v", item)
	}
}

// Authenticated destination and full IP tuple are independent parts of the
// key, so neither another peer nor another UDP flow can satisfy attribution.
func TestRemoteUserNatProviderReturnObserverSeparatesFlowIdentity(t *testing.T) {
	observer, events := providerReturnObserverEvents()
	settings := DefaultRemoteUserNatProviderSettings()
	settings.ReturnSendObserver = observer
	provider := &RemoteUserNatProvider{settings: settings}
	firstPeerId := NewId()
	secondPeerId := NewId()
	first := newProviderReturnObserverItem(t, firstPeerId, 1, false)
	otherPeer := newProviderReturnObserverItem(t, secondPeerId, 1, false)
	otherTuple := newProviderReturnObserverItemForFlow(
		t,
		firstPeerId,
		1,
		false,
		net.ParseIP("198.51.100.11"),
		9443,
		net.ParseIP("10.0.0.23"),
		51002,
	)
	fixtures := []*providerReturnObserverItemFixture{first, otherPeer, otherTuple}
	started := make([]RemoteUserNatProviderReturnSendObservation, len(fixtures))
	for fixtureIndex, fixture := range fixtures {
		provider.beginReturnSendObservation(fixture.item)
		started[fixtureIndex] = <-events
		requireProviderReturnObservation(
			t,
			started[fixtureIndex],
			RemoteUserNatProviderReturnSendPhaseStarted,
			started[fixtureIndex].Token,
			fixture.flowKey,
			1,
			fixture.totalBytes,
			false,
		)
	}
	if started[0].FlowKey == started[1].FlowKey {
		t.Fatal("identical IP tuples on different authenticated destinations shared a key")
	}
	if started[0].FlowKey == started[2].FlowKey {
		t.Fatal("different IP tuples on one authenticated destination shared a key")
	}
	for fixtureIndex, fixture := range fixtures {
		provider.completeReturnSendObservation(fixture.item, providerReturnSendResult{
			packetCount:     1,
			packetByteCount: fixture.totalBytes,
			sent:            true,
		})
		completed := <-events
		requireProviderReturnObservation(
			t,
			completed,
			RemoteUserNatProviderReturnSendPhaseCompleted,
			started[fixtureIndex].Token,
			fixture.flowKey,
			1,
			fixture.totalBytes,
			true,
		)
		fixture.item.returnPackets()
		fixture.releaseWitnesses(t)
	}
}

// A batch stores one immutable flow key before sending; completion reuses it
// after every packet buffer has left the provider item.
func TestRemoteUserNatProviderReturnObserverRetainsBatchFlowAfterBufferRelease(t *testing.T) {
	observer, events := providerReturnObserverEvents()
	settings := DefaultRemoteUserNatProviderSettings()
	settings.ReturnSendObserver = observer
	provider := &RemoteUserNatProvider{settings: settings}
	fixture := newProviderReturnObserverItem(t, NewId(), 5, true)
	provider.beginReturnSendObservation(fixture.item)
	started := <-events
	requireProviderReturnObservation(
		t,
		started,
		RemoteUserNatProviderReturnSendPhaseStarted,
		started.Token,
		fixture.flowKey,
		5,
		fixture.totalBytes,
		false,
	)
	result := providerReturnSendResult{
		packetCount:     fixture.item.packetCount(),
		packetByteCount: fixture.item.packetByteCount,
		sent:            true,
	}
	fixture.item.returnPackets()
	if fixture.item.firstPacket() != nil {
		t.Fatal("provider batch retained a packet after explicit ownership release")
	}
	provider.completeReturnSendObservation(fixture.item, result)
	completed := <-events
	requireProviderReturnObservation(
		t,
		completed,
		RemoteUserNatProviderReturnSendPhaseCompleted,
		started.Token,
		fixture.flowKey,
		5,
		fixture.totalBytes,
		true,
	)
	fixture.releaseWitnesses(t)
}

// Malformed packet bytes retain an explicit invalid key and the authenticated
// destination across both phases instead of suppressing observer accounting.
func TestRemoteUserNatProviderReturnObserverPairsInvalidFlowKey(t *testing.T) {
	observer, events := providerReturnObserverEvents()
	settings := DefaultRemoteUserNatProviderSettings()
	settings.ReturnSendObserver = observer
	provider := &RemoteUserNatProvider{settings: settings}
	peerId := NewId()
	packet := MessagePoolCopy([]byte{0xff})
	witness := MessagePoolShareReadOnly(packet)
	item := &providerReturnItem{
		source:          SourceId(peerId),
		packet:          packet,
		packetByteCount: 1,
	}
	expectedKey := RemoteUserNatProviderReturnFlowKey{DestinationId: peerId}
	provider.beginReturnSendObservation(item)
	started := <-events
	requireProviderReturnObservation(
		t,
		started,
		RemoteUserNatProviderReturnSendPhaseStarted,
		started.Token,
		expectedKey,
		1,
		1,
		false,
	)
	item.returnPackets()
	provider.completeReturnSendObservation(item, providerReturnSendResult{
		packetCount:     1,
		packetByteCount: 1,
		sent:            false,
	})
	completed := <-events
	requireProviderReturnObservation(
		t,
		completed,
		RemoteUserNatProviderReturnSendPhaseCompleted,
		started.Token,
		expectedKey,
		1,
		1,
		false,
	)
	if !MessagePoolReturn(witness) {
		t.Fatal("invalid observer packet retained provider ownership")
	}
}

// One socket drain larger than the provider fairness cap emits one observer
// pair with aggregate accounting, even though it creates three logical groups.
func TestRemoteUserNatProviderReturnObserverPairsMultichunkBatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observer, events := providerReturnObserverEvents()
	settings := DefaultRemoteUserNatProviderSettings()
	settings.WriteTimeout = 0
	settings.ReturnSendWorkerCount = 1
	settings.ReturnSendQueueSize = 4
	settings.ReturnSendObserver = observer
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	peerId := NewId()
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:       peerId,
		CompanionContract: true,
	})
	sequence.packs = make(chan *SendPack, 3)
	releases := make(chan struct{}, 1)
	provider.afterReturnReleaseForTest = func() { releases <- struct{}{} }
	fixture := newProviderReturnObserverItem(t, peerId, 33, true)
	if !provider.enqueueReturnItem(fixture.item) {
		t.Fatal("multichunk provider return was not admitted")
	}
	started := waitProviderReturnObservation(t, ctx, events)
	completed := waitProviderReturnObservation(t, ctx, events)
	waitProviderReturnObserverRelease(t, ctx, releases)
	requireProviderReturnObservation(
		t,
		started,
		RemoteUserNatProviderReturnSendPhaseStarted,
		started.Token,
		fixture.flowKey,
		33,
		fixture.totalBytes,
		false,
	)
	requireProviderReturnObservation(
		t,
		completed,
		RemoteUserNatProviderReturnSendPhaseCompleted,
		started.Token,
		fixture.flowKey,
		33,
		fixture.totalBytes,
		true,
	)
	for chunkIndex, wantFrameCount := range []int{16, 16, 1} {
		queued := waitProviderReturnTestPack(t, sequence)
		if !queued.logicalGroup || len(queued.Frames) != wantFrameCount {
			t.Fatalf(
				"multichunk logical group %d = (logical=%t, frames=%d), want (true,%d)",
				chunkIndex,
				queued.logicalGroup,
				len(queued.Frames),
				wantFrameCount,
			)
		}
		queued.returnFrames()
	}
	if eventCount := len(events); eventCount != 0 {
		t.Fatalf("multichunk item emitted %d extra observations", eventCount)
	}
	fixture.releaseWitnesses(t)
}

// If only the first logical group enters Transfer, the item completion remains
// aggregate and false while congestion accounting names only failed groups.
func TestRemoteUserNatProviderReturnObserverPairsPartialChunkFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observer, events := providerReturnObserverEvents()
	settings := DefaultRemoteUserNatProviderSettings()
	settings.WriteTimeout = 0
	settings.ReturnSendWorkerCount = 1
	settings.ReturnSendQueueSize = 2
	settings.ReturnSendObserver = observer
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	peerId := NewId()
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:       peerId,
		CompanionContract: true,
	})
	sequence.packs = make(chan *SendPack, 1)
	releases := make(chan struct{}, 1)
	provider.afterReturnReleaseForTest = func() { releases <- struct{}{} }
	fixture := newProviderReturnObserverItem(t, peerId, 33, true)
	if !provider.enqueueReturnItem(fixture.item) {
		t.Fatal("partial-failure provider return was not admitted")
	}
	started := waitProviderReturnObservation(t, ctx, events)
	completed := waitProviderReturnObservation(t, ctx, events)
	waitProviderReturnObserverRelease(t, ctx, releases)
	requireProviderReturnObservation(
		t,
		started,
		RemoteUserNatProviderReturnSendPhaseStarted,
		started.Token,
		fixture.flowKey,
		33,
		fixture.totalBytes,
		false,
	)
	requireProviderReturnObservation(
		t,
		completed,
		RemoteUserNatProviderReturnSendPhaseCompleted,
		started.Token,
		fixture.flowKey,
		33,
		fixture.totalBytes,
		false,
	)
	queued := waitProviderReturnTestPack(t, sequence)
	if !queued.logicalGroup || len(queued.Frames) != 16 {
		t.Fatalf(
			"partial-failure admitted group = (logical=%t, frames=%d), want (true,16)",
			queued.logicalGroup,
			len(queued.Frames),
		)
	}
	queued.returnFrames()
	var failedBytes ByteCount
	for _, byteCount := range fixture.byteCounts[16:] {
		failedBytes += byteCount
	}
	drops := provider.CongestionDropStats()
	if drops.ReturnSendPacketCount != 17 || drops.ReturnSendByteCount != failedBytes {
		t.Fatalf("partial group failure drops=%+v, want 17/%d", drops, failedBytes)
	}
	if eventCount := len(events); eventCount != 0 {
		t.Fatalf("partial-failure item emitted %d extra observations", eventCount)
	}
	fixture.releaseWitnesses(t)
}

// A full queue rejects one exact started token, while provider Close completes
// both the active item and its queued successor. A post-close attempt retains
// the same pairing contract without substituting or emitting a zero token.
func TestRemoteUserNatProviderReturnObserverPairsQueueRejectAndCloseDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	observer, events := providerReturnObserverEvents()
	settings := DefaultRemoteUserNatProviderSettings()
	settings.WriteTimeout = 0
	settings.ReturnSendWorkerCount = 1
	settings.ReturnSendQueueSize = 1
	settings.ReturnSendObserver = observer
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	peerId := NewId()
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:       peerId,
		CompanionContract: true,
	})
	sequence.packs = make(chan *SendPack)
	activeEntered := make(chan struct{})
	releaseActive := make(chan struct{})
	var activeOnce sync.Once
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseActive) }) }
	defer release()
	provider.returnSendStarted = func() {
		activeOnce.Do(func() {
			close(activeEntered)
			<-releaseActive
		})
	}
	active := newProviderReturnObserverItem(t, peerId, 1, false)
	queued := newProviderReturnObserverItem(t, peerId, 1, false)
	rejected := newProviderReturnObserverItem(t, peerId, 1, false)
	if !provider.enqueueReturnItem(active.item) {
		t.Fatal("active provider return was not admitted")
	}
	activeStarted := waitProviderReturnObservation(t, ctx, events)
	waitProviderReturnBarrier(t, activeEntered, "active observer return worker")
	if !provider.enqueueReturnItem(queued.item) {
		t.Fatal("queued provider return was not admitted")
	}
	queuedStarted := waitProviderReturnObservation(t, ctx, events)
	if provider.enqueueReturnItem(rejected.item) {
		t.Fatal("full provider return queue admitted overflow")
	}
	rejectedStarted := waitProviderReturnObservation(t, ctx, events)
	rejectedCompleted := waitProviderReturnObservation(t, ctx, events)
	for observationIndex, started := range []RemoteUserNatProviderReturnSendObservation{
		activeStarted,
		queuedStarted,
		rejectedStarted,
	} {
		fixture := []*providerReturnObserverItemFixture{active, queued, rejected}[observationIndex]
		requireProviderReturnObservation(
			t,
			started,
			RemoteUserNatProviderReturnSendPhaseStarted,
			started.Token,
			fixture.flowKey,
			1,
			fixture.totalBytes,
			false,
		)
	}
	requireProviderReturnObservation(
		t,
		rejectedCompleted,
		RemoteUserNatProviderReturnSendPhaseCompleted,
		rejectedStarted.Token,
		rejected.flowKey,
		1,
		rejected.totalBytes,
		false,
	)
	if activeStarted.Token == queuedStarted.Token ||
		activeStarted.Token == rejectedStarted.Token ||
		queuedStarted.Token == rejectedStarted.Token {
		t.Fatalf(
			"provider queue items reused tokens active=%d queued=%d rejected=%d",
			activeStarted.Token,
			queuedStarted.Token,
			rejectedStarted.Token,
		)
	}
	if eventCount := len(events); eventCount != 0 {
		t.Fatalf("held provider queue emitted %d premature completions", eventCount)
	}
	closed := make(chan struct{})
	go func() {
		provider.Close()
		close(closed)
	}()
	release()
	waitProviderReturnBarrier(t, closed, "provider observer close drain")
	completedByToken := map[uint64]RemoteUserNatProviderReturnSendObservation{}
	for range 2 {
		completed := waitProviderReturnObservation(t, ctx, events)
		if completed.Phase != RemoteUserNatProviderReturnSendPhaseCompleted ||
			completed.Token == 0 {
			t.Fatalf("provider close completion=%+v", completed)
		}
		if _, duplicate := completedByToken[completed.Token]; duplicate {
			t.Fatalf("duplicate provider close completion token=%d", completed.Token)
		}
		completedByToken[completed.Token] = completed
	}
	requireProviderReturnObservation(
		t,
		completedByToken[activeStarted.Token],
		RemoteUserNatProviderReturnSendPhaseCompleted,
		activeStarted.Token,
		active.flowKey,
		1,
		active.totalBytes,
		false,
	)
	requireProviderReturnObservation(
		t,
		completedByToken[queuedStarted.Token],
		RemoteUserNatProviderReturnSendPhaseCompleted,
		queuedStarted.Token,
		queued.flowKey,
		1,
		queued.totalBytes,
		false,
	)
	if eventCount := len(events); eventCount != 0 {
		t.Fatalf("provider close drain emitted %d extra observations", eventCount)
	}
	afterClose := newProviderReturnObserverItem(t, peerId, 1, false)
	if provider.enqueueReturnItem(afterClose.item) {
		t.Fatal("closed provider admitted a return item")
	}
	closedStarted := waitProviderReturnObservation(t, ctx, events)
	closedCompleted := waitProviderReturnObservation(t, ctx, events)
	requireProviderReturnObservation(
		t,
		closedStarted,
		RemoteUserNatProviderReturnSendPhaseStarted,
		closedStarted.Token,
		afterClose.flowKey,
		1,
		afterClose.totalBytes,
		false,
	)
	requireProviderReturnObservation(
		t,
		closedCompleted,
		RemoteUserNatProviderReturnSendPhaseCompleted,
		closedStarted.Token,
		afterClose.flowKey,
		1,
		afterClose.totalBytes,
		false,
	)
	if closedStarted.Token == activeStarted.Token ||
		closedStarted.Token == queuedStarted.Token ||
		closedStarted.Token == rejectedStarted.Token {
		t.Fatalf("post-close provider return reused token=%d", closedStarted.Token)
	}
	if eventCount := len(events); eventCount != 0 {
		t.Fatalf("post-close rejection emitted %d extra observations", eventCount)
	}
	active.releaseWitnesses(t)
	queued.releaseWitnesses(t)
	rejected.releaseWitnesses(t)
	afterClose.releaseWitnesses(t)
}

// Completion uses the observer captured at Started and suppresses calls with
// token zero or a token already completed.
func TestRemoteUserNatProviderReturnObserverRejectsUnpairedCompletion(t *testing.T) {
	firstObserver, firstEvents := providerReturnObserverEvents()
	secondObserver, secondEvents := providerReturnObserverEvents()
	settings := DefaultRemoteUserNatProviderSettings()
	settings.ReturnSendObserver = firstObserver
	provider := &RemoteUserNatProvider{settings: settings}
	item := &providerReturnItem{packetByteCount: 19}
	result := providerReturnSendResult{packetCount: 1, packetByteCount: 19, sent: false}
	provider.completeReturnSendObservation(item, result)
	if eventCount := len(firstEvents); eventCount != 0 {
		t.Fatalf("token-zero completion emitted %d observations", eventCount)
	}
	provider.nextReturnSendObserverToken.Store(^uint64(0))
	provider.beginReturnSendObservation(item)
	provider.beginReturnSendObservation(item)
	if item.observerToken != 1 {
		t.Fatalf("provider observer wrap token=%d, want 1", item.observerToken)
	}
	settings.ReturnSendObserver = secondObserver
	provider.completeReturnSendObservation(item, result)
	provider.completeReturnSendObservation(item, result)
	if eventCount := len(firstEvents); eventCount != 2 {
		t.Fatalf("captured provider observer event count=%d, want 2", eventCount)
	}
	started := <-firstEvents
	completed := <-firstEvents
	requireProviderReturnObservation(
		t, started, RemoteUserNatProviderReturnSendPhaseStarted,
		started.Token, RemoteUserNatProviderReturnFlowKey{}, 1, 19, false,
	)
	requireProviderReturnObservation(
		t, completed, RemoteUserNatProviderReturnSendPhaseCompleted,
		started.Token, RemoteUserNatProviderReturnFlowKey{}, 1, 19, false,
	)
	if eventCount := len(secondEvents); eventCount != 0 {
		t.Fatalf("replacement observer received %d unpaired completions", eventCount)
	}
}

// Panics from both phases are contained; the same ordered worker continues to
// process a second item and produces two complete, nonzero token pairs.
func TestRemoteUserNatProviderReturnObserverPanicDoesNotStopWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	events := make(chan RemoteUserNatProviderReturnSendObservation, 8)
	settings := DefaultRemoteUserNatProviderSettings()
	settings.WriteTimeout = 0
	settings.ReturnSendWorkerCount = 1
	settings.ReturnSendQueueSize = 2
	settings.ReturnSendObserver = func(observation RemoteUserNatProviderReturnSendObservation) {
		events <- observation
		panic("provider observer panic")
	}
	provider, client, _ := newProviderTransferKeyTestFixtureWithSettings(t, settings)
	peerId := NewId()
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:       peerId,
		CompanionContract: true,
	})
	sequence.packs = make(chan *SendPack, 2)
	releases := make(chan struct{}, 2)
	provider.afterReturnReleaseForTest = func() { releases <- struct{}{} }
	first := newProviderReturnObserverItem(t, peerId, 1, false)
	second := newProviderReturnObserverItem(t, peerId, 1, false)
	if !provider.enqueueReturnItem(first.item) || !provider.enqueueReturnItem(second.item) {
		t.Fatal("panic-isolation provider items were not both admitted")
	}
	waitProviderReturnObserverRelease(t, ctx, releases)
	waitProviderReturnObserverRelease(t, ctx, releases)
	for range 2 {
		queued := waitProviderReturnTestPack(t, sequence)
		queued.returnFrames()
		queued.releaseRaw()
	}
	phases := map[uint64]map[RemoteUserNatProviderReturnSendPhase]RemoteUserNatProviderReturnSendObservation{}
	for range 4 {
		observation := waitProviderReturnObservation(t, ctx, events)
		if observation.Token == 0 {
			t.Fatalf("panic observer emitted token zero: %+v", observation)
		}
		if phases[observation.Token] == nil {
			phases[observation.Token] = map[RemoteUserNatProviderReturnSendPhase]RemoteUserNatProviderReturnSendObservation{}
		}
		if _, duplicate := phases[observation.Token][observation.Phase]; duplicate {
			t.Fatalf("panic observer duplicated phase: %+v", observation)
		}
		phases[observation.Token][observation.Phase] = observation
	}
	if len(phases) != 2 {
		t.Fatalf("panic observer completed token count=%d, want 2", len(phases))
	}
	for token, tokenPhases := range phases {
		started := tokenPhases[RemoteUserNatProviderReturnSendPhaseStarted]
		completed := tokenPhases[RemoteUserNatProviderReturnSendPhaseCompleted]
		requireProviderReturnObservation(
			t, started, RemoteUserNatProviderReturnSendPhaseStarted,
			token, first.flowKey, 1, started.PacketByteCount, false,
		)
		requireProviderReturnObservation(
			t, completed, RemoteUserNatProviderReturnSendPhaseCompleted,
			token, first.flowKey, 1, started.PacketByteCount, true,
		)
	}
	if eventCount := len(events); eventCount != 0 {
		t.Fatalf("panic observer emitted %d extra events", eventCount)
	}
	first.releaseWitnesses(t)
	second.releaseWitnesses(t)
}
