// Deterministic lifecycle barriers verify every manager-owned worker joins and
// that shutdown wins exactly against later worker admission.
package connect

import (
	"context"
	"crypto/ed25519"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// blockingLifecycleOob consumes one request, then holds its synchronous
// launcher after ownership crossed into the OOB implementation.
type blockingLifecycleOob struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

// SendControl provides the legacy interface through the same deterministic
// ownership barrier.
func (self *blockingLifecycleOob) SendControl(
	frames []*protocol.Frame,
	callback OobResultFunction,
) {
	self.send(frames)
}

// SendControlWithCtx provides the production context-aware boundary while the
// test deliberately holds an implementation that does not honor cancellation.
func (self *blockingLifecycleOob) SendControlWithCtx(
	ctx context.Context,
	frames []*protocol.Frame,
	callback OobResultFunction,
) {
	self.send(frames)
}

// send consumes frame ownership before exposing the held external call.
func (self *blockingLifecycleOob) send(frames []*protocol.Frame) {
	for _, frame := range frames {
		MessagePoolReturn(frame.MessageBytes)
	}
	self.once.Do(func() { close(self.entered) })
	<-self.release
}

// TestLifecycleAdmissionCloseRejectsPausedProducer proves close is the exact
// boundary: a producer paused before the lock cannot enter after close wins.
func TestLifecycleAdmissionCloseRejectsPausedProducer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gate := newLifecycleAdmission()
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	var startEnteredOnce sync.Once
	gate.beforeStartLockForTest = func() {
		startEnteredOnce.Do(func() { close(startEntered) })
		<-releaseStart
	}
	admitted := make(chan bool, 1)
	go func() { admitted <- gate.start() }()
	waitCloseWaitBarrier(t, ctx, startEntered, "paused lifecycle admission")

	gate.close()
	close(releaseStart)
	select {
	case result := <-admitted:
		if result {
			t.Fatal("producer was admitted after lifecycle close")
		}
	case <-ctx.Done():
		t.Fatalf("wait for rejected lifecycle admission: %v", ctx.Err())
	}
	waitCloseWaitBarrier(t, ctx, gate.Done(), "closed empty lifecycle")
}

// TestLifecycleAdmissionWaitsForAdmittedProducer proves done is withheld until
// the final producer admitted before close publishes completion.
func TestLifecycleAdmissionWaitsForAdmittedProducer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gate := newLifecycleAdmission()
	if !gate.start() {
		t.Fatal("open lifecycle rejected its first producer")
	}
	gate.close()
	select {
	case <-gate.Done():
		t.Fatal("lifecycle completed before admitted producer finished")
	default:
	}
	gate.finish()
	waitCloseWaitBarrier(t, ctx, gate.Done(), "finished lifecycle")
}

// TestClientCloseAndWaitJoinsPausedStreamOpen proves a close that invalidates
// an admitted asynchronous OpenStream waits for its terminal cleanup and does
// not let that worker publish a post-close sequence.
func TestClientCloseAndWaitJoinsPausedStreamOpen(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	streamBuffer := client.streamManager.streamBuffer
	constructEntered := make(chan struct{})
	releaseConstruct := make(chan struct{})
	joinEntered := make(chan struct{})
	var constructEnteredOnce sync.Once
	var releaseConstructOnce sync.Once
	var joinEnteredOnce sync.Once
	streamBuffer.beforeStreamSequenceConstructForTest = func(request *streamOpenRequest) {
		constructEnteredOnce.Do(func() { close(constructEntered) })
		<-releaseConstruct
	}
	streamBuffer.beforeCloseWaitForTest = func() {
		joinEnteredOnce.Do(func() { close(joinEntered) })
	}
	joined := false
	defer func() {
		releaseConstructOnce.Do(func() { close(releaseConstruct) })
		if !joined {
			_ = client.CloseAndWait(ctx)
		}
	}()

	destinationId := NewId()
	streamId := NewId()
	success, err := streamBuffer.OpenStream(nil, &destinationId, streamId)
	if err != nil || !success {
		t.Fatalf("open held stream = (%t, %v)", success, err)
	}
	waitCloseWaitBarrier(t, ctx, constructEntered, "stream construction barrier")

	result := make(chan error, 1)
	go func() { result <- client.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, joinEntered, "stream open-worker join")
	requireCloseWaitBlocked(t, result, "Client.CloseAndWait stream open join")
	releaseConstructOnce.Do(func() { close(releaseConstruct) })
	waitCloseWaitResult(t, ctx, result, "join paused stream open")
	joined = true

	streamBuffer.managementStateLock.Lock()
	managedCount := len(streamBuffer.managedOpenRequests)
	pendingCount := len(streamBuffer.pendingOpenRequests)
	workerCount := len(streamBuffer.openWorkers)
	streamBuffer.managementStateLock.Unlock()
	streamBuffer.mutex.Lock()
	sequenceCount := len(streamBuffer.streamSequences)
	streamBuffer.mutex.Unlock()
	if managedCount != 0 || pendingCount != 0 || workerCount != 0 || sequenceCount != 0 {
		t.Fatalf(
			"post-close stream state = managed:%d pending:%d workers:%d sequences:%d",
			managedCount,
			pendingCount,
			workerCount,
			sequenceCount,
		)
	}
}

// TestClientCloseAndWaitJoinsIntermediaryStreamChildren proves one live
// multihop generation joins both P2P associations and both forwarding workers.
func TestClientCloseAndWaitJoinsIntermediaryStreamChildren(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := closeWaitClientSettings()
	childEntered := make(chan string, 4)
	releaseChildren := make(chan struct{})
	childrenStarted := make(chan struct{})
	sequencePublished := make(chan struct{})
	joinEntered := make(chan struct{})
	var childrenStartedOnce sync.Once
	var sequencePublishedOnce sync.Once
	var joinEnteredOnce sync.Once
	settings.StreamManagerSettings.StreamBufferSettings.
		P2pTransportSettings.beforeRunDoneForTest = func(streamId Id, peerType PeerType) {
		childEntered <- "p2p"
		<-releaseChildren
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	streamBuffer := client.streamManager.streamBuffer
	streamBuffer.configureStreamSequenceForTest = func(sequence *StreamSequence) {
		sequence.beforeForwardWorkerDoneForTest = func() {
			childEntered <- "forward"
			<-releaseChildren
		}
		sequence.afterChildrenStartedForTest = func() {
			childrenStartedOnce.Do(func() { close(childrenStarted) })
		}
	}
	streamBuffer.afterStreamSequencePublishForTest = func(sequence *StreamSequence) {
		sequencePublishedOnce.Do(func() { close(sequencePublished) })
	}
	streamBuffer.beforeCloseWaitForTest = func() {
		joinEnteredOnce.Do(func() { close(joinEntered) })
	}
	joined := false
	var releaseChildrenOnce sync.Once
	defer func() {
		releaseChildrenOnce.Do(func() { close(releaseChildren) })
		if !joined {
			_ = client.CloseAndWait(ctx)
		}
	}()

	sourceId := NewId()
	destinationId := NewId()
	success, err := streamBuffer.OpenStream(&sourceId, &destinationId, NewId())
	if err != nil || !success {
		t.Fatalf("open intermediary stream = (%t, %v)", success, err)
	}
	waitCloseWaitBarrier(t, ctx, sequencePublished, "intermediary publication")
	waitCloseWaitBarrier(t, ctx, childrenStarted, "intermediary child launch")

	result := make(chan error, 1)
	go func() { result <- client.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, joinEntered, "intermediary sequence join")
	childCounts := map[string]int{}
	for range 4 {
		select {
		case name := <-childEntered:
			childCounts[name] += 1
		case <-ctx.Done():
			t.Fatalf("wait for intermediary child cleanup: %v", ctx.Err())
		}
	}
	if childCounts["p2p"] != 2 || childCounts["forward"] != 2 {
		t.Fatalf("intermediary cleanup workers = %v, want p2p:2 forward:2", childCounts)
	}
	requireCloseWaitBlocked(t, result, "Client.CloseAndWait intermediary children")
	releaseChildrenOnce.Do(func() { close(releaseChildren) })
	waitCloseWaitResult(t, ctx, result, "join intermediary children")
	joined = true
}

// TestControlSyncCloseAndWaitJoinsRetryWorker proves cancellation alone does
// not publish completion before its admitted supersession watcher exits.
func TestControlSyncCloseAndWaitJoinsRetryWorker(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	controlSync := NewControlSync(ctx, client, "lifecycle")
	workerEntered := make(chan struct{})
	joinEntered := make(chan struct{})
	releaseWorker := make(chan struct{})
	var workerEnteredOnce sync.Once
	var joinEnteredOnce sync.Once
	var releaseWorkerOnce sync.Once
	controlSync.beforeWorkerDoneForTest = func() {
		workerEnteredOnce.Do(func() { close(workerEntered) })
		<-releaseWorker
	}
	controlSync.beforeCloseWaitForTest = func() {
		joinEnteredOnce.Do(func() { close(joinEntered) })
	}
	defer func() {
		releaseWorkerOnce.Do(func() { close(releaseWorker) })
		_ = client.CloseAndWait(ctx)
	}()

	frame, err := ToFrame(&protocol.SimpleMessage{MessageIndex: 1}, DefaultProtocolVersion)
	if err != nil {
		t.Fatal(err)
	}
	controlSync.Send(frame, nil, nil)
	result := make(chan error, 1)
	go func() { result <- controlSync.closeAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, workerEntered, "control sync worker cleanup")
	waitCloseWaitBarrier(t, ctx, joinEntered, "control sync join")
	requireCloseWaitBlocked(t, result, "ControlSync close join")
	releaseWorkerOnce.Do(func() { close(releaseWorker) })
	waitCloseWaitResult(t, ctx, result, "join control sync")
}

// TestControlSyncOobCloseAndWaitJoinsLauncher proves a synchronous OOB call is
// joined until the implementation accepts the external ownership transfer.
func TestControlSyncOobCloseAndWaitJoinsLauncher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	oob := &blockingLifecycleOob{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	client := NewClient(ctx, NewId(), oob, closeWaitClientSettings())
	controlSync := NewControlSyncOob(ctx, client, "lifecycle-oob")
	joinEntered := make(chan struct{})
	var joinEnteredOnce sync.Once
	var releaseOnce sync.Once
	controlSync.beforeCloseWaitForTest = func() {
		joinEnteredOnce.Do(func() { close(joinEntered) })
	}
	defer func() {
		releaseOnce.Do(func() { close(oob.release) })
		_ = client.CloseAndWait(ctx)
	}()

	frame, err := ToFrame(&protocol.SimpleMessage{MessageIndex: 1}, DefaultProtocolVersion)
	if err != nil {
		t.Fatal(err)
	}
	controlSync.Send(frame, nil)
	waitCloseWaitBarrier(t, ctx, oob.entered, "blocking OOB launcher")
	result := make(chan error, 1)
	go func() { result <- controlSync.closeAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, joinEntered, "OOB control join")
	requireCloseWaitBlocked(t, result, "ControlSyncOob close join")
	releaseOnce.Do(func() { close(oob.release) })
	waitCloseWaitResult(t, ctx, result, "join OOB control launcher")
}

// TestClientCloseAndWaitJoinsContractManagerWorkers proves every long-lived
// manager loop, including the lazy stats loop, remains behind the client join.
func TestClientCloseAndWaitJoinsContractManagerWorkers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := closeWaitClientSettings()
	settings.ContractManagerSettings.ProvidePingTimeout = time.Hour
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	manager := client.contractManager
	workerEntered := make(chan string, 3)
	releaseWorkers := make(chan struct{})
	joinEntered := make(chan struct{})
	var joinEnteredOnce sync.Once
	var releaseWorkersOnce sync.Once
	manager.beforeWorkerDoneForTest = func(name string) {
		workerEntered <- name
		<-releaseWorkers
	}
	manager.beforeCloseWaitForTest = func() {
		joinEnteredOnce.Do(func() { close(joinEntered) })
	}
	manager.AddContractStatsCallback(func(events []*ContractStatsEvent) {})
	joined := false
	defer func() {
		releaseWorkersOnce.Do(func() { close(releaseWorkers) })
		if !joined {
			_ = client.CloseAndWait(ctx)
		}
	}()

	result := make(chan error, 1)
	go func() { result <- client.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, joinEntered, "contract manager join")
	workerCounts := map[string]int{}
	for range 3 {
		select {
		case name := <-workerEntered:
			workerCounts[name] += 1
		case <-ctx.Done():
			t.Fatalf("wait for contract manager cleanup: %v", ctx.Err())
		}
	}
	for _, name := range []string{"provide ping", "contract expiry", "contract stats"} {
		if workerCounts[name] != 1 {
			t.Fatalf("contract worker %q count = %d, all workers = %v", name, workerCounts[name], workerCounts)
		}
	}
	requireCloseWaitBlocked(t, result, "Client.CloseAndWait contract workers")
	releaseWorkersOnce.Do(func() { close(releaseWorkers) })
	waitCloseWaitResult(t, ctx, result, "join contract manager workers")
	joined = true
}

// Cancel is the compatibility spelling for a non-joining client close. It
// must still close every manager's worker admission; cancellation alone leaves
// each contract-close join worker waiting forever on an admission that can
// never publish completion.
func TestClientCancelClosesContractManagerAdmission(t *testing.T) {
	client := NewClient(
		context.Background(),
		NewId(),
		NewNoContractClientOob(),
		DefaultClientSettings(),
	)
	contractManager := client.ContractManager()
	client.Cancel()

	contractManager.mutex.Lock()
	closed := contractManager.closed
	contractManager.mutex.Unlock()
	if !closed {
		t.Fatal("client cancel left contract-manager worker admission open")
	}
	select {
	case <-contractManager.workers.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("client cancel did not release contract-manager workers")
	}
}

// TestClientCloseAndWaitJoinsContractStatusCallback proves a callback already
// executing at shutdown is part of manager lifecycle, not a detached observer.
func TestClientCloseAndWaitJoinsContractStatusCallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	manager := client.contractManager
	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	joinEntered := make(chan struct{})
	var callbackEnteredOnce sync.Once
	var releaseCallbackOnce sync.Once
	var joinEnteredOnce sync.Once
	unsub := manager.AddContractStatusCallback(func(status *ContractStatus) {
		callbackEnteredOnce.Do(func() { close(callbackEntered) })
		<-releaseCallback
	})
	defer unsub()
	manager.beforeCloseWaitForTest = func() {
		joinEnteredOnce.Do(func() { close(joinEntered) })
	}
	joined := false
	defer func() {
		releaseCallbackOnce.Do(func() { close(releaseCallback) })
		if !joined {
			_ = client.CloseAndWait(ctx)
		}
	}()
	manager.contractStatus(&ContractStatus{Key: ContractKey{Destination: DestinationId(NewId())}})
	waitCloseWaitBarrier(t, ctx, callbackEntered, "contract status callback")

	result := make(chan error, 1)
	go func() { result <- client.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, joinEntered, "contract callback join")
	requireCloseWaitBlocked(t, result, "Client.CloseAndWait contract callback")
	releaseCallbackOnce.Do(func() { close(releaseCallback) })
	waitCloseWaitResult(t, ctx, result, "join contract status callback")
	joined = true
}

func TestContractStatusWindowDispatcherDoesNotCreatePerClientWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Close()
	manager := client.ContractManager()
	delivered := make(chan *ContractStatus, 1)
	unsub := manager.addContractStatusDispatchCallback(func(status *ContractStatus) {
		delivered <- status
	})

	if got := len(manager.contractStatusCallbacks.Get()); got != 0 {
		t.Fatalf("per-client status workers = %d, want 0", got)
	}
	status := &ContractStatus{Key: ContractKey{Destination: DestinationId(NewId())}}
	manager.contractStatus(status)
	select {
	case got := <-delivered:
		if got != status {
			t.Fatal("direct dispatcher changed status identity")
		}
	case <-time.After(time.Second):
		t.Fatal("direct status dispatcher did not run")
	}

	unsub()
	manager.contractStatus(status)
	select {
	case <-delivered:
		t.Fatal("unsubscribed direct status dispatcher still ran")
	case <-time.After(20 * time.Millisecond):
	}
}

func TestContractStatusWindowDispatcherRejectsAdmissionAfterClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	manager := client.ContractManager()
	client.Close()

	called := false
	unsub := manager.addContractStatusDispatchCallback(func(*ContractStatus) {
		called = true
	})
	unsub()
	manager.contractStatus(&ContractStatus{})
	if called {
		t.Fatal("closed manager admitted a direct status dispatcher")
	}
	if got := len(manager.contractStatusDispatchCallbacks.Get()); got != 0 {
		t.Fatalf("closed manager direct dispatchers = %d, want 0", got)
	}
}

// TestContractManagerCloseRejectsPausedCallbackAdmission proves callback
// registration cannot create a detached worker after shutdown wins the lock.
func TestContractManagerCloseRejectsPausedCallbackAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	manager := client.contractManager
	admissionEntered := make(chan struct{})
	releaseAdmission := make(chan struct{})
	var admissionEnteredOnce sync.Once
	manager.beforeCallbackAdmissionLockForTest = func() {
		admissionEnteredOnce.Do(func() { close(admissionEntered) })
		<-releaseAdmission
	}
	unsubResult := make(chan func(), 1)
	go func() {
		unsubResult <- manager.AddContractStatusCallback(func(status *ContractStatus) {})
	}()
	waitCloseWaitBarrier(t, ctx, admissionEntered, "contract callback admission")
	manager.Close()
	close(releaseAdmission)
	select {
	case unsub := <-unsubResult:
		unsub()
	case <-ctx.Done():
		t.Fatalf("wait for rejected contract callback: %v", ctx.Err())
	}
	manager.beforeCallbackAdmissionLockForTest = nil
	if count := len(manager.contractStatusCallbacks.Get()); count != 0 {
		t.Fatalf("post-close contract callback count = %d, want 0", count)
	}
	if err := client.CloseAndWait(ctx); err != nil {
		t.Fatalf("join client after callback rejection: %v", err)
	}
}

// TestClientCloseAndWaitJoinsClientKeyPublisher proves the constructor's real
// identity-key publication cannot survive joined client teardown.
func TestClientCloseAndWaitJoinsClientKeyPublisher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := closeWaitClientSettings()
	publishEntered := make(chan struct{})
	releasePublish := make(chan struct{})
	joinEntered := make(chan struct{})
	var publishEnteredOnce sync.Once
	var releasePublishOnce sync.Once
	var joinEnteredOnce sync.Once
	settings.beforeClientKeyPublishForTest = func() {
		publishEnteredOnce.Do(func() { close(publishEntered) })
		<-releasePublish
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	client.clientKeyManager.beforeCloseWaitForTest = func() {
		joinEnteredOnce.Do(func() { close(joinEntered) })
	}
	joined := false
	defer func() {
		releasePublishOnce.Do(func() { close(releasePublish) })
		if !joined {
			_ = client.CloseAndWait(ctx)
		}
	}()
	waitCloseWaitBarrier(t, ctx, publishEntered, "client key publisher")

	result := make(chan error, 1)
	go func() { result <- client.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, joinEntered, "client key manager join")
	requireCloseWaitBlocked(t, result, "Client.CloseAndWait client key publisher")
	releasePublishOnce.Do(func() { close(releasePublish) })
	waitCloseWaitResult(t, ctx, result, "join client key publisher")
	joined = true
}

// TestClientKeyManagerCloseRejectsPausedSetSeed proves a rotation paused before
// admission cannot mutate the key or start a publisher after close wins.
func TestClientKeyManagerCloseRejectsPausedSetSeed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	manager := client.clientKeyManager
	originalPublicKey := manager.PublicKey()
	admissionEntered := make(chan struct{})
	releaseAdmission := make(chan struct{})
	var admissionEnteredOnce sync.Once
	manager.beforePublisherAdmissionLockForTest = func() {
		admissionEnteredOnce.Do(func() { close(admissionEntered) })
		<-releaseAdmission
	}
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	setResult := make(chan error, 1)
	go func() { setResult <- manager.SetSeed(seed) }()
	waitCloseWaitBarrier(t, ctx, admissionEntered, "client key rotation admission")
	manager.Close()
	close(releaseAdmission)
	select {
	case err := <-setResult:
		if err == nil {
			t.Fatal("SetSeed succeeded after manager close won admission")
		}
	case <-ctx.Done():
		t.Fatalf("wait for rejected SetSeed: %v", ctx.Err())
	}
	manager.beforePublisherAdmissionLockForTest = nil
	if !ed25519.PublicKey(originalPublicKey).Equal(manager.PublicKey()) {
		t.Fatal("rejected SetSeed mutated the client public key")
	}
	if err := client.CloseAndWait(ctx); err != nil {
		t.Fatalf("join client after SetSeed rejection: %v", err)
	}
}

// TestClientCloseAndWaitJoinsEncryptedKeyPublisher proves the actual TLS-cert
// commitment publisher belongs to the encryption manager join.
func TestClientCloseAndWaitJoinsEncryptedKeyPublisher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := closeWaitClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOpportunistic
	publishEntered := make(chan struct{})
	releasePublish := make(chan struct{})
	joinEntered := make(chan struct{})
	var publishEnteredOnce sync.Once
	var releasePublishOnce sync.Once
	var joinEnteredOnce sync.Once
	settings.EncryptionSettings.beforeEncryptedKeyPublishForTest = func() {
		publishEnteredOnce.Do(func() { close(publishEntered) })
		<-releasePublish
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	client.encryptionSessionManager.beforeCloseWaitForTest = func() {
		joinEnteredOnce.Do(func() { close(joinEntered) })
	}
	joined := false
	defer func() {
		releasePublishOnce.Do(func() { close(releasePublish) })
		if !joined {
			_ = client.CloseAndWait(ctx)
		}
	}()
	waitCloseWaitBarrier(t, ctx, publishEntered, "encrypted key publisher")

	result := make(chan error, 1)
	go func() { result <- client.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, joinEntered, "encryption manager join")
	requireCloseWaitBlocked(t, result, "Client.CloseAndWait encrypted key publisher")
	releasePublishOnce.Do(func() { close(releasePublish) })
	waitCloseWaitResult(t, ctx, result, "join encrypted key publisher")
	joined = true
}

// TestPeerEncryptionSessionRunJoinsKeyFetcher proves the session supervisor
// cannot finish while an admitted identity-key fetch still owns session state.
func TestPeerEncryptionSessionRunJoinsKeyFetcher(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	fetchEntered := make(chan struct{})
	releaseFetch := make(chan struct{})
	runStarted := make(chan struct{})
	workerWaitEntered := make(chan struct{})
	var fetchEnteredOnce sync.Once
	var releaseFetchOnce sync.Once
	var runStartedOnce sync.Once
	var workerWaitEnteredOnce sync.Once
	encryptionSettings := DefaultEncryptionSettings()
	encryptionSettings.NewPeerClientPublicKeyFetcher = func(peerId Id) func(context.Context) ([]byte, error) {
		return func(fetchCtx context.Context) ([]byte, error) {
			fetchEnteredOnce.Do(func() { close(fetchEntered) })
			<-releaseFetch
			publicKey := make([]byte, ed25519.PublicKeySize)
			publicKey[0] = 1
			return publicKey, nil
		}
	}
	session := newPeerEncryptionSession(
		ctx,
		client.encryptionSessionManager,
		client,
		NewId(),
		sequenceTlsRoleServer,
		encryptionSettings,
		nil,
		false,
	)
	session.afterRunStartedForTest = func() {
		runStartedOnce.Do(func() { close(runStarted) })
	}
	session.beforeWorkerWaitForTest = func() {
		workerWaitEnteredOnce.Do(func() { close(workerWaitEntered) })
	}
	runDone := make(chan struct{})
	go func() {
		session.Run()
		close(runDone)
	}()
	defer func() {
		releaseFetchOnce.Do(func() { close(releaseFetch) })
		session.close()
		waitCloseWaitBarrier(t, ctx, runDone, "encryption session cleanup")
		_ = client.CloseAndWait(ctx)
	}()
	waitCloseWaitBarrier(t, ctx, runStarted, "encryption session supervisor")
	publicKey := make(ed25519.PublicKey, ed25519.PublicKeySize)
	publicKey[0] = 1
	session.SetPeerClientPublicKey(publicKey)
	waitCloseWaitBarrier(t, ctx, fetchEntered, "identity key fetch")
	session.close()
	waitCloseWaitBarrier(t, ctx, workerWaitEntered, "session child-worker join")
	select {
	case <-runDone:
		t.Fatal("encryption session exited before key fetcher cleanup")
	default:
	}
	releaseFetchOnce.Do(func() { close(releaseFetch) })
	waitCloseWaitBarrier(t, ctx, runDone, "joined encryption session")
}

// TestClientCloseAndWaitJoinsEncryptionSessionSupervisor proves a live session
// remains visible to the manager gate through its complete child cleanup.
func TestClientCloseAndWaitJoinsEncryptionSessionSupervisor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	manager := client.encryptionSessionManager
	manager.settings.Mode = EncryptionModeOpportunistic
	workerEntered := make(chan struct{})
	releaseWorker := make(chan struct{})
	joinEntered := make(chan struct{})
	var workerEnteredOnce sync.Once
	var releaseWorkerOnce sync.Once
	var joinEnteredOnce sync.Once
	manager.beforeWorkerDoneForTest = func(name string) {
		if name != "encryption session" {
			return
		}
		workerEnteredOnce.Do(func() { close(workerEntered) })
		<-releaseWorker
	}
	manager.beforeCloseWaitForTest = func() {
		joinEnteredOnce.Do(func() { close(joinEntered) })
	}
	if session := manager.Acquire(NewId(), sequenceTlsRoleServer, false); session == nil {
		t.Fatal("encryption manager did not create a lifecycle session")
	}
	joined := false
	defer func() {
		releaseWorkerOnce.Do(func() { close(releaseWorker) })
		if !joined {
			_ = client.CloseAndWait(ctx)
		}
	}()

	result := make(chan error, 1)
	go func() { result <- client.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, workerEntered, "encryption session supervisor cleanup")
	waitCloseWaitBarrier(t, ctx, joinEntered, "encryption session manager join")
	requireCloseWaitBlocked(t, result, "Client.CloseAndWait encryption session")
	releaseWorkerOnce.Do(func() { close(releaseWorker) })
	waitCloseWaitResult(t, ctx, result, "join encryption session supervisor")
	joined = true
}

// TestEncryptionManagerCloseRejectsPausedSessionAdmission proves a session
// paused before the manager lock cannot publish after close wins.
func TestEncryptionManagerCloseRejectsPausedSessionAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	manager := client.encryptionSessionManager
	manager.settings.Mode = EncryptionModeOpportunistic
	admissionEntered := make(chan struct{})
	releaseAdmission := make(chan struct{})
	var admissionEnteredOnce sync.Once
	manager.beforeSessionAdmissionLockForTest = func() {
		admissionEnteredOnce.Do(func() { close(admissionEntered) })
		<-releaseAdmission
	}
	sessionResult := make(chan *peerEncryptionSession, 1)
	go func() {
		sessionResult <- manager.Acquire(NewId(), sequenceTlsRoleServer, false)
	}()
	waitCloseWaitBarrier(t, ctx, admissionEntered, "encryption session admission")
	manager.Close()
	close(releaseAdmission)
	select {
	case session := <-sessionResult:
		if session != nil {
			t.Fatal("session was admitted after encryption manager close")
		}
	case <-ctx.Done():
		t.Fatalf("wait for rejected encryption session: %v", ctx.Err())
	}
	manager.beforeSessionAdmissionLockForTest = nil
	manager.stateLock.Lock()
	sessionCount := len(manager.sessions)
	manager.stateLock.Unlock()
	if sessionCount != 0 {
		t.Fatalf("post-close encryption session count = %d, want 0", sessionCount)
	}
	if err := client.CloseAndWait(ctx); err != nil {
		t.Fatalf("join client after session rejection: %v", err)
	}
}
