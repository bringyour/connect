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

func newProviderSourceLifecycleTestFixture(
	t *testing.T,
	configure func(*RemoteUserNatProviderSettings),
) (*RemoteUserNatProvider, *LocalUserNat, *Client) {
	return newProviderSourceLifecycleTestFixtureWithOob(
		t,
		NewNoContractClientOob(),
		configure,
	)
}

// Builds the same lifecycle fixture with an observable control-plane owner.
func newProviderSourceLifecycleTestFixtureWithOob(
	t *testing.T,
	clientOob OutOfBandControl,
	configure func(*RemoteUserNatProviderSettings),
) (*RemoteUserNatProvider, *LocalUserNat, *Client) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	client := NewClient(
		ctx,
		NewId(),
		clientOob,
		DefaultClientSettings(),
	)
	localUserNat := NewLocalUserNatWithDefaults(ctx, "source-lifecycle-test")
	settings := DefaultRemoteUserNatProviderSettings()
	settings.EventEpoch = time.Hour
	if configure != nil {
		configure(settings)
	}
	provider := NewRemoteUserNatProvider(client, localUserNat, settings)
	t.Cleanup(func() {
		provider.Close()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := localUserNat.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close local user NAT: %v", err)
		}
		if err := client.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close client: %v", err)
		}
		cancel()
	})
	return provider, localUserNat, client
}

// Counts only contract creation while honoring control-frame ownership.
type providerSourceLifecycleCountingOob struct {
	createContractCount atomic.Int32
}

func (self *providerSourceLifecycleCountingOob) SendControl(
	frames []*protocol.Frame,
	callback OobResultFunction,
) {
	for _, frame := range frames {
		message, err := FromFrame(frame)
		if err == nil {
			if _, ok := message.(*protocol.CreateContract); ok {
				self.createContractCount.Add(1)
			}
		}
		MessagePoolReturn(frame.MessageBytes)
	}
	if callback != nil {
		callback(nil, nil)
	}
}

func providerSourceReliabilityStatus(sourceId Id) *ContractStatus {
	contractError := protocol.ContractError_Reliability
	return &ContractStatus{
		Key:   ContractKey{Destination: DestinationId(sourceId)},
		Error: &contractError,
	}
}

func waitProviderSourceLifecycleBarrier(
	t *testing.T,
	barrier <-chan struct{},
	name string,
) {
	t.Helper()
	select {
	case <-barrier:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

func requireProviderSourceLifecycleBlocked(
	t *testing.T,
	barrier <-chan struct{},
	name string,
) {
	t.Helper()
	select {
	case <-barrier:
		t.Fatalf("%s completed before its release boundary", name)
	default:
	}
}

// A terminal status received before first use creates a generation-lifetime
// tombstone. Duplicate, unrelated, and structurally mismatched statuses do not
// create additional state or reject healthy sources.
func TestRemoteUserNatProviderReliabilityRejectsExactSourceBeforeAdmission(t *testing.T) {
	provider, _, client := newProviderSourceLifecycleTestFixture(t, func(settings *RemoteUserNatProviderSettings) {
		settings.MaxSourceCount = 8
	})
	rejectedSourceId := NewId()
	healthySourceId := NewId()
	retired := make(chan struct{}, 1)
	provider.afterSourceRetirementForTest = func(sourceId Id) {
		if sourceId == rejectedSourceId {
			retired <- struct{}{}
		}
	}

	client.ContractManager().contractStatus(providerSourceReliabilityStatus(rejectedSourceId))
	if sourceLifecycle := provider.acquireSourceLifecycle(rejectedSourceId); sourceLifecycle != nil {
		provider.releaseSourceLifecycle(rejectedSourceId, sourceLifecycle)
		t.Fatal("terminal source was admitted after Reliability")
	}
	client.ContractManager().contractStatus(providerSourceReliabilityStatus(rejectedSourceId))

	nonReliability := protocol.ContractError_NoPermission
	client.ContractManager().contractStatus(&ContractStatus{
		Key:   ContractKey{Destination: DestinationId(healthySourceId)},
		Error: &nonReliability,
	})
	client.ContractManager().contractStatus(&ContractStatus{
		Key: ContractKey{Destination: TransferPath{
			SourceId:      NewId(),
			DestinationId: healthySourceId,
		}},
		Error: providerSourceReliabilityStatus(healthySourceId).Error,
	})
	client.ContractManager().contractStatus(providerSourceReliabilityStatus(client.ClientId()))
	healthyLifecycle := provider.acquireSourceLifecycle(healthySourceId)
	if healthyLifecycle == nil {
		t.Fatal("healthy source was rejected by an unrelated status")
	}
	provider.releaseSourceLifecycle(healthySourceId, healthyLifecycle)

	waitProviderSourceLifecycleBarrier(t, retired, "exact source retirement")
	provider.stateLock.Lock()
	defer provider.stateLock.Unlock()
	if provider.terminalSourceCount != 1 || len(provider.sourceLifecycles) != 1 {
		t.Fatalf(
			"duplicate or unrelated status changed terminal state: terminal=%d lifecycles=%d",
			provider.terminalSourceCount,
			len(provider.sourceLifecycles),
		)
	}
}

// A terminal verdict that reaches the provider lock first rejects a source
// whose first admission is paused just before that lock.
func TestRemoteUserNatProviderReliabilityWinsFirstAdmissionRace(t *testing.T) {
	provider, _, _ := newProviderSourceLifecycleTestFixture(t, nil)
	sourceId := NewId()
	admissionEntered := make(chan struct{})
	admissionRelease := make(chan struct{})
	provider.beforeSourceAdmissionForTest = func(candidateId Id) {
		if candidateId == sourceId {
			close(admissionEntered)
			<-admissionRelease
		}
	}
	result := make(chan *providerSourceLifecycle, 1)
	go func() {
		result <- provider.acquireSourceLifecycle(sourceId)
	}()
	waitProviderSourceLifecycleBarrier(t, admissionEntered, "pre-admission hook")
	provider.contractStatus(providerSourceReliabilityStatus(sourceId))
	close(admissionRelease)
	if sourceLifecycle := <-result; sourceLifecycle != nil {
		provider.releaseSourceLifecycle(sourceId, sourceLifecycle)
		t.Fatal("source admitted after terminal status won the lock")
	}
}

// An operation that reaches the gate first remains tracked and is canceled by
// a later terminal verdict; retirement cannot finish until it releases.
func TestRemoteUserNatProviderFirstAdmissionWinsReliabilityRace(t *testing.T) {
	provider, _, _ := newProviderSourceLifecycleTestFixture(t, nil)
	sourceId := NewId()
	sourceLifecycle := provider.acquireSourceLifecycle(sourceId)
	if sourceLifecycle == nil {
		t.Fatal("first healthy source was not admitted")
	}
	retired := make(chan struct{})
	provider.afterSourceRetirementForTest = func(candidateId Id) {
		if candidateId == sourceId {
			close(retired)
		}
	}
	provider.contractStatus(providerSourceReliabilityStatus(sourceId))
	select {
	case <-sourceLifecycle.ctx.Done():
	default:
		t.Fatal("Reliability did not synchronously cancel the admitted source")
	}
	requireProviderSourceLifecycleBlocked(t, retired, "source retirement")
	provider.releaseSourceLifecycle(sourceId, sourceLifecycle)
	waitProviderSourceLifecycleBarrier(t, retired, "source retirement after admission")
}

// Healthy gates count concurrent work rather than lifetime-unique clients.
// Sequential churn therefore reuses the full bound indefinitely.
func TestRemoteUserNatProviderHealthySourceLifecycleIsReclaimed(t *testing.T) {
	provider, _, _ := newProviderSourceLifecycleTestFixture(t, func(settings *RemoteUserNatProviderSettings) {
		settings.MaxSourceCount = 2
	})
	for range 32 {
		sourceId := NewId()
		sourceLifecycle := provider.acquireSourceLifecycle(sourceId)
		if sourceLifecycle == nil {
			t.Fatal("sequential healthy churn exhausted the source bound")
		}
		provider.releaseSourceLifecycle(sourceId, sourceLifecycle)
	}
	provider.stateLock.Lock()
	defer provider.stateLock.Unlock()
	if len(provider.sourceLifecycles) != 0 || provider.sourceLifecycleSaturated {
		t.Fatalf(
			"healthy lifecycle was retained: count=%d saturated=%t",
			len(provider.sourceLifecycles),
			provider.sourceLifecycleSaturated,
		)
	}
}

// Cleanup is a fixed cohort behind a size-one wake, even when the configured
// source capacity is enormous. Pending jobs reuse their terminal lifecycle
// objects rather than allocating a capacity-sized ring or channel.
func TestRemoteUserNatProviderHighSourceCapUsesFixedRetirementWorkers(t *testing.T) {
	provider, _, _ := newProviderSourceLifecycleTestFixture(t, func(settings *RemoteUserNatProviderSettings) {
		settings.MaxSourceCount = 1 << 30
		settings.ReturnSendWorkerCount = 1 << 20
	})
	provider.stateLock.Lock()
	if provider.sourceRetirementNotify != nil || provider.sourceRetirementWorkerCount != 0 {
		provider.stateLock.Unlock()
		t.Fatal("constructor allocated source-capacity retirement storage")
	}
	provider.stateLock.Unlock()

	sourceId := NewId()
	retirementEntered := make(chan struct{})
	retirementRelease := make(chan struct{})
	provider.beforeSourceRetirementWaitForTest = func(candidateId Id) {
		if candidateId == sourceId {
			close(retirementEntered)
			<-retirementRelease
		}
	}
	provider.contractStatus(providerSourceReliabilityStatus(sourceId))
	waitProviderSourceLifecycleBarrier(t, retirementEntered, "retirement worker")
	provider.stateLock.Lock()
	notifyCapacity := cap(provider.sourceRetirementNotify)
	workerCount := provider.sourceRetirementWorkerCount
	provider.stateLock.Unlock()
	if notifyCapacity != 1 || workerCount != 8 {
		t.Fatalf("capacity-dependent cleanup allocation: notify=%d workers=%d", notifyCapacity, workerCount)
	}
	close(retirementRelease)
}

// One blocked source cleanup cannot head-of-line block later terminal gates,
// and even a blocked saturation owner callback is never run inline with the
// shared contract-status producer.
func TestRemoteUserNatProviderReliabilityCleanupAndSaturationDoNotBlockStatus(t *testing.T) {
	callbackEntered := make(chan struct{})
	callbackRelease := make(chan struct{})
	var callbackOnce sync.Once
	var callbackCount atomic.Int32
	provider, _, _ := newProviderSourceLifecycleTestFixture(t, func(settings *RemoteUserNatProviderSettings) {
		settings.MaxSourceCount = 2
		settings.ReturnSendWorkerCount = 1
		settings.SourceLifecycleSaturated = func() {
			callbackCount.Add(1)
			callbackOnce.Do(func() { close(callbackEntered) })
			<-callbackRelease
		}
	})
	firstSourceId := NewId()
	secondSourceId := NewId()
	cleanupEntered := make(chan struct{})
	cleanupRelease := make(chan struct{})
	provider.beforeSourceRetirementWaitForTest = func(sourceId Id) {
		if sourceId == firstSourceId {
			close(cleanupEntered)
			<-cleanupRelease
		}
	}
	provider.contractStatus(providerSourceReliabilityStatus(firstSourceId))
	waitProviderSourceLifecycleBarrier(t, cleanupEntered, "blocked first cleanup")

	secondReturned := make(chan struct{})
	go func() {
		provider.contractStatus(providerSourceReliabilityStatus(secondSourceId))
		close(secondReturned)
	}()
	waitProviderSourceLifecycleBarrier(t, secondReturned, "second status return")
	waitProviderSourceLifecycleBarrier(t, callbackEntered, "saturation callback")
	if lifecycle := provider.acquireSourceLifecycle(secondSourceId); lifecycle != nil {
		provider.releaseSourceLifecycle(secondSourceId, lifecycle)
		t.Fatal("second source gate remained open behind blocked cleanup")
	}

	thirdReturned := make(chan struct{})
	go func() {
		provider.contractStatus(providerSourceReliabilityStatus(NewId()))
		close(thirdReturned)
	}()
	waitProviderSourceLifecycleBarrier(t, thirdReturned, "status behind blocked owner callback")
	close(callbackRelease)
	close(cleanupRelease)
	provider.Close()
	if callbackCount.Load() != 1 {
		t.Fatalf("saturation callback count=%d, want 1", callbackCount.Load())
	}
}

// Saturation notification is an out-of-band ownership handoff. Provider
// Close must not join an owner callback that may be waiting for the same SDK
// state lock under which that owner synchronously called Close.
func TestRemoteUserNatProviderCloseDoesNotJoinSaturationOwnerHandoff(t *testing.T) {
	callbackEntered := make(chan struct{})
	callbackRelease := make(chan struct{})
	callbackDone := make(chan struct{})
	var releaseOnce sync.Once
	provider, _, _ := newProviderSourceLifecycleTestFixture(t, func(settings *RemoteUserNatProviderSettings) {
		settings.MaxSourceCount = 1
		settings.SourceLifecycleSaturated = func() {
			defer close(callbackDone)
			close(callbackEntered)
			<-callbackRelease
		}
	})
	t.Cleanup(func() { releaseOnce.Do(func() { close(callbackRelease) }) })
	provider.contractStatus(providerSourceReliabilityStatus(NewId()))
	waitProviderSourceLifecycleBarrier(t, callbackEntered, "saturation owner handoff")
	closeDone := make(chan struct{})
	go func() {
		provider.Close()
		close(closeDone)
	}()
	waitProviderSourceLifecycleBarrier(t, closeDone, "provider close behind owner handoff")
	requireProviderSourceLifecycleBlocked(t, callbackDone, "blocked owner callback")
	releaseOnce.Do(func() { close(callbackRelease) })
	waitProviderSourceLifecycleBarrier(t, callbackDone, "owner callback release")
}

// LocalUserNat tombstones are exact-source and owner-refcounted. Releasing an
// old provider generation cannot clear a replacement generation's claim.
func TestLocalUserNatSourceRetirementIsOwnerRefcounted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	localUserNat := NewLocalUserNatWithDefaults(ctx, "source-owner-test")
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := localUserNat.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close local user NAT: %v", err)
		}
	}()

	sourceId := NewId()
	tombstoned := 0
	released := 0
	unsub := localUserNat.addSourceRetirementCallback(func(candidateId Id, retired bool) []<-chan struct{} {
		if candidateId != sourceId {
			return nil
		}
		if retired {
			tombstoned += 1
		} else {
			released += 1
		}
		return nil
	})
	defer unsub()
	firstOwner := localUserNat.newSourceRetirementOwner()
	secondOwner := localUserNat.newSourceRetirementOwner()
	localUserNat.retireSourceForOwner(firstOwner, sourceId)
	localUserNat.retireSourceForOwner(firstOwner, sourceId)
	localUserNat.retireSourceForOwner(secondOwner, sourceId)
	if tombstoned != 1 || released != 0 {
		t.Fatalf("unexpected initial retirement transitions: retired=%d released=%d", tombstoned, released)
	}
	localUserNat.releaseSourceRetirementOwner(firstOwner)
	if released != 0 {
		t.Fatal("first owner cleared a sibling generation's tombstone")
	}
	localUserNat.releaseSourceRetirementOwner(secondOwner)
	if tombstoned != 1 || released != 1 {
		t.Fatalf("unexpected final retirement transitions: retired=%d released=%d", tombstoned, released)
	}
}

// Provider retirement waits for the exact LocalUserNat flow completion set
// before releasing source policy state or the generation's tombstone owner.
func TestRemoteUserNatProviderRetirementJoinsLocalUserNatSourceFlows(t *testing.T) {
	provider, localUserNat, _ := newProviderSourceLifecycleTestFixture(t, nil)
	sourceId := NewId()
	flowDone := make(chan struct{})
	tombstonePublished := make(chan struct{})
	var publishOnce sync.Once
	unsub := localUserNat.addSourceRetirementCallback(func(candidateId Id, retired bool) []<-chan struct{} {
		if candidateId == sourceId && retired {
			publishOnce.Do(func() { close(tombstonePublished) })
			return []<-chan struct{}{flowDone}
		}
		return nil
	})
	defer unsub()
	retired := make(chan struct{})
	provider.afterSourceRetirementForTest = func(candidateId Id) {
		if candidateId == sourceId {
			close(retired)
		}
	}
	provider.contractStatus(providerSourceReliabilityStatus(sourceId))
	waitProviderSourceLifecycleBarrier(t, tombstonePublished, "LocalUserNat tombstone")
	requireProviderSourceLifecycleBlocked(t, retired, "source retirement flow join")
	close(flowDone)
	waitProviderSourceLifecycleBarrier(t, retired, "source retirement flow join")
}

// Close joins a healthy callback already captured by Client callback dispatch;
// unsubscribe alone cannot prove that borrowed callback arguments are gone.
func TestRemoteUserNatProviderCloseJoinsHealthyClientReceive(t *testing.T) {
	provider, _, _ := newProviderSourceLifecycleTestFixture(t, nil)
	sourceId := NewId()
	callbackEntered := make(chan struct{})
	callbackRelease := make(chan struct{})
	provider.afterSourceAdmissionForTest = func(candidateId Id) {
		if candidateId == sourceId {
			close(callbackEntered)
			<-callbackRelease
		}
	}
	callbackDone := make(chan struct{})
	go func() {
		provider.ClientReceive(SourceId(sourceId), nil, Peer{})
		close(callbackDone)
	}()
	waitProviderSourceLifecycleBarrier(t, callbackEntered, "healthy ClientReceive callback")
	closeDone := make(chan struct{})
	go func() {
		provider.Close()
		close(closeDone)
	}()
	requireProviderSourceLifecycleBlocked(t, closeDone, "provider close")
	close(callbackRelease)
	waitProviderSourceLifecycleBarrier(t, callbackDone, "healthy ClientReceive callback completion")
	waitProviderSourceLifecycleBarrier(t, closeDone, "provider close after healthy callback")
}

// An internal ContractManager callback may already be in a copy-on-write
// dispatch snapshot when provider Close unsubscribes it. The explicit callback
// admission joins that captured call before Close releases provider state.
func TestRemoteUserNatProviderCloseJoinsCapturedContractStatusCallback(t *testing.T) {
	provider, _, client := newProviderSourceLifecycleTestFixture(t, nil)
	sourceId := NewId()
	callbackEntered := make(chan struct{})
	callbackRelease := make(chan struct{})
	provider.afterContractStatusAdmissionForTest = func() {
		close(callbackEntered)
		<-callbackRelease
	}
	statusDone := make(chan struct{})
	go func() {
		client.ContractManager().contractStatus(providerSourceReliabilityStatus(sourceId))
		close(statusDone)
	}()
	waitProviderSourceLifecycleBarrier(t, callbackEntered, "captured status callback")
	closeDone := make(chan struct{})
	go func() {
		provider.Close()
		close(closeDone)
	}()
	requireProviderSourceLifecycleBlocked(t, closeDone, "provider close")
	close(callbackRelease)
	waitProviderSourceLifecycleBarrier(t, statusDone, "captured status callback completion")
	waitProviderSourceLifecycleBarrier(t, closeDone, "provider close after status callback")
}

// Transfer cancellation snapshots run only after the terminal source gate has
// closed. Attempts arriving after either snapshot cannot create new provider
// work, while a sibling source remains admissible throughout cleanup.
func TestRemoteUserNatProviderTerminalGateClosesTransferSnapshotRaces(t *testing.T) {
	provider, _, _ := newProviderSourceLifecycleTestFixture(t, nil)
	rejectedSourceId := NewId()
	siblingSourceId := NewId()
	afterReceiveSnapshot := make(chan struct{})
	receiveSnapshotRelease := make(chan struct{})
	provider.afterReceiveSourceRetirementForTest = func(sourceId Id) {
		if sourceId == rejectedSourceId {
			close(afterReceiveSnapshot)
			<-receiveSnapshotRelease
		}
	}
	afterSendSnapshot := make(chan struct{})
	sendSnapshotRelease := make(chan struct{})
	provider.afterSendSourceRetirementForTest = func(sourceId Id) {
		if sourceId == rejectedSourceId {
			close(afterSendSnapshot)
			<-sendSnapshotRelease
		}
	}
	retired := make(chan struct{})
	provider.afterSourceRetirementForTest = func(sourceId Id) {
		if sourceId == rejectedSourceId {
			close(retired)
		}
	}
	provider.contractStatus(providerSourceReliabilityStatus(rejectedSourceId))
	waitProviderSourceLifecycleBarrier(t, afterReceiveSnapshot, "ReceiveBuffer source snapshot")
	provider.ClientReceive(SourceId(rejectedSourceId), nil, Peer{})
	if lifecycle := provider.acquireSourceLifecycle(rejectedSourceId); lifecycle != nil {
		provider.releaseSourceLifecycle(rejectedSourceId, lifecycle)
		t.Fatal("post-ReceiveBuffer-snapshot source admission escaped the tombstone")
	}
	siblingLifecycle := provider.acquireSourceLifecycle(siblingSourceId)
	if siblingLifecycle == nil {
		t.Fatal("rejected source cleanup blocked a sibling admission")
	}
	provider.releaseSourceLifecycle(siblingSourceId, siblingLifecycle)
	close(receiveSnapshotRelease)
	waitProviderSourceLifecycleBarrier(t, afterSendSnapshot, "SendBuffer destination snapshot")
	item := provider.takeReturnItem()
	item.source = SourceId(rejectedSourceId)
	if provider.enqueueReturnItem(item) {
		t.Fatal("post-SendBuffer-snapshot return recreated terminal source work")
	}
	close(sendSnapshotRelease)
	waitProviderSourceLifecycleBarrier(t, retired, "snapshot-race retirement")
}

// An already queued return retains its exact source admission. Reliability
// cancels the item's send context immediately, and retirement cannot complete
// until final queue disposition releases that admission.
func TestRemoteUserNatProviderReliabilityDrainsQueuedReturnAdmission(t *testing.T) {
	provider, _, _ := newProviderSourceLifecycleTestFixture(t, nil)
	sourceId := NewId()
	sourceLifecycle := provider.acquireSourceLifecycle(sourceId)
	if sourceLifecycle == nil {
		t.Fatal("queued return source was not admitted")
	}
	item := provider.takeReturnItem()
	item.source = SourceId(sourceId)
	item.sourceLifecycle = sourceLifecycle
	retired := make(chan struct{})
	provider.afterSourceRetirementForTest = func(candidateId Id) {
		if candidateId == sourceId {
			close(retired)
		}
	}
	provider.contractStatus(providerSourceReliabilityStatus(sourceId))
	select {
	case <-item.sendContext(provider.ctx).Done():
	default:
		t.Fatal("queued return retained a live send context after Reliability")
	}
	requireProviderSourceLifecycleBlocked(t, retired, "queued return retirement")
	provider.releaseReturnItem(item)
	waitProviderSourceLifecycleBarrier(t, retired, "queued return final disposition")
}

// Every LocalUserNat return converges on enqueueReturnItem. Once Reliability
// closes that exact gate, repeated returns must be rejected synchronously
// before they can create a SendSequence or issue another CreateContract.
func TestRemoteUserNatProviderReliabilitySuppressesRepeatedReturnContracts(t *testing.T) {
	clientOob := &providerSourceLifecycleCountingOob{}
	provider, _, client := newProviderSourceLifecycleTestFixtureWithOob(
		t,
		clientOob,
		nil,
	)
	sourceId := NewId()
	provider.contractStatus(providerSourceReliabilityStatus(sourceId))
	for range 32 {
		item := provider.takeReturnItem()
		item.source = SourceId(sourceId)
		item.packet = MessagePoolCopy([]byte("synthetic rejected return"))
		item.packetByteCount = ByteCount(len(item.packet))
		if provider.enqueueReturnItem(item) {
			t.Fatal("terminal source admitted a repeated provider return")
		}
	}
	client.sendBuffer.mutex.Lock()
	sendSequenceCount := 0
	for sequenceId := range client.sendBuffer.sendSequences {
		if sequenceId.Destination == sourceId {
			sendSequenceCount += 1
		}
	}
	client.sendBuffer.mutex.Unlock()
	if sendSequenceCount != 0 || clientOob.createContractCount.Load() != 0 {
		t.Fatalf(
			"terminal returns created send work: sequences=%d contracts=%d",
			sendSequenceCount,
			clientOob.createContractCount.Load(),
		)
	}
}

// A provider callback can finish after handing a batch to LocalUserNat while
// that batch is still queued or inside a shard. The disposition admission must
// keep the terminal tombstone owner alive until that packet reaches protocol
// ownership (and the protocol flow is then joined).
func TestRemoteUserNatProviderReliabilityJoinsQueuedLocalUserNatIngress(t *testing.T) {
	provider, localUserNat, _ := newProviderSourceLifecycleTestFixture(t, func(settings *RemoteUserNatProviderSettings) {
		settings.SecurityPolicyGenerator = DisableSecurityPolicyWithStats
	})
	sourceId := NewId()
	dispositionEntered := make(chan struct{})
	dispositionRelease := make(chan struct{})
	var dispositionOnce sync.Once
	localUserNat.afterSendPacketForTest = func() {
		dispositionOnce.Do(func() { close(dispositionEntered) })
		<-dispositionRelease
	}
	packet := craftSecurityPacket(
		IpProtocolUdp,
		net.ParseIP("192.0.2.10"),
		41000,
		net.ParseIP("198.51.100.20"),
		42000,
		false,
		[]byte("synthetic provider queue"),
	)
	frame, err := ToFrame(&protocol.IpPacketToProvider{
		IpPacket: &protocol.IpPacket{PacketBytes: packet},
	}, DefaultProtocolVersion)
	if err != nil {
		t.Fatalf("encode synthetic provider packet: %v", err)
	}
	provider.ClientReceive(
		SourceId(sourceId),
		[]*protocol.Frame{frame},
		Peer{ProvideMode: protocol.ProvideMode_Public},
	)
	waitProviderSourceLifecycleBarrier(t, dispositionEntered, "LocalUserNat queued disposition")
	retired := make(chan struct{})
	provider.afterSourceRetirementForTest = func(candidateId Id) {
		if candidateId == sourceId {
			close(retired)
		}
	}
	provider.contractStatus(providerSourceReliabilityStatus(sourceId))
	requireProviderSourceLifecycleBlocked(t, retired, "queued LocalUserNat ingress retirement")
	close(dispositionRelease)
	waitProviderSourceLifecycleBarrier(t, retired, "queued LocalUserNat ingress disposition")
}

// Reliability can close the source gate after ClientReceive has copied an
// allowed packet but before its secondary LocalUserNat queue admission. That
// losing branch must account and release the copy without touching the NAT.
func TestRemoteUserNatProviderReliabilityRejectsPendingLocalUserNatAdmission(t *testing.T) {
	provider, _, _ := newProviderSourceLifecycleTestFixture(t, func(settings *RemoteUserNatProviderSettings) {
		settings.SecurityPolicyGenerator = DisableSecurityPolicyWithStats
	})
	sourceId := NewId()
	admissionEntered := make(chan struct{})
	admissionRelease := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(admissionRelease) }) })
	packetWitness := make(chan []byte, 1)
	provider.beforeLocalUserNatAdmissionForTest = func(
		candidateId Id,
		packets [][]byte,
	) {
		if candidateId != sourceId {
			return
		}
		packetWitness <- MessagePoolShareReadOnly(packets[0])
		close(admissionEntered)
		<-admissionRelease
	}

	packet := MessagePoolCopy(craftSecurityPacket(
		IpProtocolUdp,
		net.ParseIP("192.0.2.30"),
		43000,
		net.ParseIP("198.51.100.40"),
		44000,
		false,
		[]byte("synthetic pending admission"),
	))
	packetByteCount := len(packet)
	defer func() {
		if packet != nil {
			MessagePoolReturn(packet)
		}
	}()
	frame, err := ToFrame(&protocol.IpPacketToProvider{
		IpPacket: &protocol.IpPacket{PacketBytes: packet},
	}, DefaultProtocolVersion)
	if err != nil {
		t.Fatalf("encode synthetic provider packet: %v", err)
	}
	callbackDone := make(chan struct{})
	go func() {
		provider.ClientReceive(
			SourceId(sourceId),
			[]*protocol.Frame{frame},
			Peer{ProvideMode: protocol.ProvideMode_Public},
		)
		close(callbackDone)
	}()
	waitProviderSourceLifecycleBarrier(t, admissionEntered, "pending LocalUserNat admission")
	provider.contractStatus(providerSourceReliabilityStatus(sourceId))
	releaseOnce.Do(func() { close(admissionRelease) })
	waitProviderSourceLifecycleBarrier(t, callbackDone, "rejected LocalUserNat admission")

	MessagePoolReturn(packet)
	packet = nil
	witness := <-packetWitness
	if !MessagePoolReturn(witness) {
		t.Fatal("terminal secondary admission retained its copied packet owner")
	}
	drops := provider.CongestionDropStats()
	if drops.IngressNatPacketCount != 1 ||
		drops.IngressNatByteCount != ByteCount(packetByteCount) {
		t.Fatalf(
			"terminal secondary admission drops=(%d,%d), want (1,%d)",
			drops.IngressNatPacketCount,
			drops.IngressNatByteCount,
			packetByteCount,
		)
	}
}

// Transfer snapshot cancellation joins the exact current source/destination
// sequence generations and leaves sibling routes untouched. Provider source
// gates (tested above) prevent a later provider admission from creating a new
// generation after these snapshots.
func TestRemoteUserNatProviderTransferRetirementJoinsExactSequences(t *testing.T) {
	rejectedSourceId := NewId()
	siblingSourceId := NewId()
	rejectedSendCtx, rejectedSendCancel := context.WithCancel(context.Background())
	siblingSendCtx, siblingSendCancel := context.WithCancel(context.Background())
	defer siblingSendCancel()
	rejectedSend := &SendSequence{
		ctx:         rejectedSendCtx,
		cancel:      rejectedSendCancel,
		destination: rejectedSourceId,
		done:        make(chan struct{}),
	}
	siblingSend := &SendSequence{
		ctx:         siblingSendCtx,
		cancel:      siblingSendCancel,
		destination: siblingSourceId,
		done:        make(chan struct{}),
	}
	sendBuffer := &SendBuffer{
		sendSequences: map[sendSequenceId]*SendSequence{
			{Destination: rejectedSourceId}: rejectedSend,
			{Destination: siblingSourceId}:  siblingSend,
		},
		sendSequencesByDestination: map[Id]map[*SendSequence]bool{},
	}

	rejectedReceiveCtx, rejectedReceiveCancel := context.WithCancel(context.Background())
	siblingReceiveCtx, siblingReceiveCancel := context.WithCancel(context.Background())
	defer siblingReceiveCancel()
	rejectedReceive := &ReceiveSequence{
		ctx:    rejectedReceiveCtx,
		cancel: rejectedReceiveCancel,
		done:   make(chan struct{}),
	}
	siblingReceive := &ReceiveSequence{
		ctx:    siblingReceiveCtx,
		cancel: siblingReceiveCancel,
		done:   make(chan struct{}),
	}
	receiveBuffer := &ReceiveBuffer{
		receiveSequences: map[receiveSequenceId]*ReceiveSequence{
			{Source: SourceId(rejectedSourceId), SequenceId: NewId()}: rejectedReceive,
			{Source: SourceId(siblingSourceId), SequenceId: NewId()}:  siblingReceive,
		},
	}

	sendDone := make(chan struct{})
	go func() {
		sendBuffer.cancelDestinationAndWait(rejectedSourceId)
		close(sendDone)
	}()
	receiveDone := make(chan struct{})
	go func() {
		receiveBuffer.cancelSourceAndWait(rejectedSourceId)
		close(receiveDone)
	}()
	waitProviderSourceLifecycleBarrier(t, rejectedSendCtx.Done(), "exact SendSequence cancellation")
	waitProviderSourceLifecycleBarrier(t, rejectedReceiveCtx.Done(), "exact ReceiveSequence cancellation")
	requireProviderSourceLifecycleBlocked(t, sendDone, "SendSequence join")
	requireProviderSourceLifecycleBlocked(t, receiveDone, "ReceiveSequence join")
	select {
	case <-siblingSendCtx.Done():
		t.Fatal("SendBuffer cancellation reached a sibling destination")
	default:
	}
	select {
	case <-siblingReceiveCtx.Done():
		t.Fatal("ReceiveBuffer cancellation reached a sibling source")
	default:
	}
	close(rejectedSend.done)
	close(rejectedReceive.done)
	waitProviderSourceLifecycleBarrier(t, sendDone, "SendSequence join completion")
	waitProviderSourceLifecycleBarrier(t, receiveDone, "ReceiveSequence join completion")
}

// Each protocol buffer publishes a completion handle for only the rejected
// source, cancels that source immediately, and preserves sibling flow state.
func TestLocalUserNatProtocolBuffersRetireExactSource(t *testing.T) {
	rejectedSource := SourceId(NewId())
	siblingSource := SourceId(NewId())

	udpBuffer := newUdpBuffer[int](context.Background(), nil, DefaultUdpBufferSettings())
	rejectedUdp := newUdpSequenceWithTransferKey(
		context.Background(), nil, rejectedSource, TransferKey{}, protocol.ProvideMode_Public,
		4, nil, 0, nil, 0, DefaultUdpBufferSettings(),
	)
	siblingUdp := newUdpSequenceWithTransferKey(
		context.Background(), nil, siblingSource, TransferKey{}, protocol.ProvideMode_Public,
		4, nil, 0, nil, 0, DefaultUdpBufferSettings(),
	)
	udpBuffer.sequences[1] = rejectedUdp
	udpBuffer.sequences[2] = siblingUdp
	udpBuffer.sourceSequences[rejectedSource] = map[int]*UdpSequence{1: rejectedUdp}
	udpBuffer.sourceSequences[siblingSource] = map[int]*UdpSequence{2: siblingUdp}

	tcpBuffer := newTcpBuffer[int](context.Background(), nil, DefaultTcpBufferSettings())
	rejectedTcpCtx, rejectedTcpCancel := context.WithCancel(context.Background())
	siblingTcpCtx, siblingTcpCancel := context.WithCancel(context.Background())
	defer siblingTcpCancel()
	rejectedTcp := &TcpSequence{ctx: rejectedTcpCtx, cancel: rejectedTcpCancel, retirementDone: make(chan struct{})}
	siblingTcp := &TcpSequence{ctx: siblingTcpCtx, cancel: siblingTcpCancel, retirementDone: make(chan struct{})}
	tcpBuffer.sequences[1] = rejectedTcp
	tcpBuffer.sequences[2] = siblingTcp
	tcpBuffer.sourceSequences[rejectedSource] = map[int]*TcpSequence{1: rejectedTcp}
	tcpBuffer.sourceSequences[siblingSource] = map[int]*TcpSequence{2: siblingTcp}

	icmpBuffer := newIcmpBuffer[int](context.Background(), nil, DefaultIcmpBufferSettings())
	rejectedIcmpCtx, rejectedIcmpCancel := context.WithCancel(context.Background())
	siblingIcmpCtx, siblingIcmpCancel := context.WithCancel(context.Background())
	defer siblingIcmpCancel()
	rejectedIcmp := &IcmpSequence{ctx: rejectedIcmpCtx, cancel: rejectedIcmpCancel, retirementDone: make(chan struct{})}
	siblingIcmp := &IcmpSequence{ctx: siblingIcmpCtx, cancel: siblingIcmpCancel, retirementDone: make(chan struct{})}
	icmpBuffer.sequences[1] = rejectedIcmp
	icmpBuffer.sequences[2] = siblingIcmp
	icmpBuffer.sourceSequences[rejectedSource] = map[int]*IcmpSequence{1: rejectedIcmp}
	icmpBuffer.sourceSequences[siblingSource] = map[int]*IcmpSequence{2: siblingIcmp}

	doneChannels := udpBuffer.setSourceRetired(rejectedSource.SourceId, true)
	doneChannels = append(doneChannels, tcpBuffer.setSourceRetired(rejectedSource.SourceId, true)...)
	doneChannels = append(doneChannels, icmpBuffer.setSourceRetired(rejectedSource.SourceId, true)...)
	if len(doneChannels) != 3 {
		t.Fatalf("expected one completion per protocol, got %d", len(doneChannels))
	}
	for name, flowCtx := range map[string]context.Context{
		"udp":  rejectedUdp.ctx,
		"tcp":  rejectedTcp.ctx,
		"icmp": rejectedIcmp.ctx,
	} {
		select {
		case <-flowCtx.Done():
		default:
			t.Fatalf("%s source flow was not canceled", name)
		}
	}
	for name, flowCtx := range map[string]context.Context{
		"udp":  siblingUdp.ctx,
		"tcp":  siblingTcp.ctx,
		"icmp": siblingIcmp.ctx,
	} {
		select {
		case <-flowCtx.Done():
			t.Fatalf("%s sibling flow was canceled", name)
		default:
		}
	}
	if len(udpBuffer.sequences) != 1 || len(tcpBuffer.sequences) != 1 || len(icmpBuffer.sequences) != 1 {
		t.Fatalf(
			"exact retirement did not preserve siblings: udp=%d tcp=%d icmp=%d",
			len(udpBuffer.sequences),
			len(tcpBuffer.sequences),
			len(icmpBuffer.sequences),
		)
	}
	if sent, err := udpBuffer.udpSend(
		3, rejectedSource, TransferKey{}, protocol.ProvideMode_Public,
		4, &parsedUdp{}, 0, nil,
	); err != nil || sent {
		t.Fatalf("retired UDP source was recreated: sent=%t err=%v", sent, err)
	}
	if sent, err := tcpBuffer.tcpSend(
		3, rejectedSource, TransferKey{}, protocol.ProvideMode_Public,
		4, &parsedTcp{}, 0, nil,
	); err != nil || sent {
		t.Fatalf("retired TCP source was recreated: sent=%t err=%v", sent, err)
	}
	if sent, err := icmpBuffer.icmpSend(
		3, rejectedSource, TransferKey{}, protocol.ProvideMode_Public,
		4, &parsedIcmp{}, 0, nil,
	); err != nil || sent {
		t.Fatalf("retired ICMP source was recreated: sent=%t err=%v", sent, err)
	}

	close(rejectedUdp.retirementDone)
	close(rejectedTcp.retirementDone)
	close(rejectedIcmp.retirementDone)
	siblingUdp.Close()
}
