// Exact lifecycle regressions cover callback admission, context callbacks,
// transport drains, and pooled-owner rejection at shutdown.
package connect

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/urnetwork/connect/v2026/protocol"
)

// lifecyclePoolCapture retains two witnesses beside one production owner.
// The first witness proves the owner is live before release; the second proves
// the production path returned the owner afterward.
type lifecyclePoolCapture struct {
	owner         []byte
	liveWitness   []byte
	finalWitness  []byte
	liveReturned  bool
	finalReturned bool
}

// newLifecyclePoolCapture shares one exact pooled owner twice.
func newLifecyclePoolCapture(message []byte) *lifecyclePoolCapture {
	return &lifecyclePoolCapture{
		owner:        message,
		liveWitness:  MessagePoolShareReadOnly(message),
		finalWitness: MessagePoolShareReadOnly(message),
	}
}

// requireOwnerLive consumes the first witness while the production owner must
// still exist.
func (self *lifecyclePoolCapture) requireOwnerLive(t *testing.T, name string) {
	t.Helper()
	if pooled, _ := MessagePoolCheck(self.owner); !pooled {
		t.Fatalf("%s was not allocated from the message pool", name)
	}
	if MessagePoolReturn(self.liveWitness) {
		self.liveReturned = true
		self.finalReturned = true
		t.Fatalf("%s returned its production owner before release", name)
	}
	self.liveReturned = true
}

// requireOwnerReturned consumes the last witness and requires production to
// have relinquished the only other reference.
func (self *lifecyclePoolCapture) requireOwnerReturned(t *testing.T, name string) {
	t.Helper()
	if MessagePoolReturn(self.finalWitness) {
		self.finalReturned = true
		return
	}
	self.finalReturned = true
	MessagePoolReturn(self.owner)
	t.Fatalf("%s retained its production message-pool owner", name)
}

// cleanup releases outstanding references after an earlier assertion.
func (self *lifecyclePoolCapture) cleanup() {
	if !self.liveReturned {
		MessagePoolReturn(self.liveWitness)
		self.liveReturned = true
	}
	if !self.finalReturned {
		if !MessagePoolReturn(self.finalWitness) {
			MessagePoolReturn(self.owner)
		}
		self.finalReturned = true
	}
}

// heldLifecycleSignalReceiver blocks after the compatibility callback owns its
// lazily framed pooled bytes.
type heldLifecycleSignalReceiver struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

// lifecycleDataChannelConn adapts a net.Conn to Pion's detached data-channel
// interface for terminal-publication tests.
type lifecycleDataChannelConn struct {
	net.Conn
}

// ReadDataChannel delegates one binary message read.
func (self *lifecycleDataChannelConn) ReadDataChannel(message []byte) (int, bool, error) {
	n, err := self.Read(message)
	return n, false, err
}

// WriteDataChannel delegates one binary message write.
func (self *lifecycleDataChannelConn) WriteDataChannel(message []byte, isString bool) (int, error) {
	_ = isString
	return self.Write(message)
}

// ReceiveSignal holds the callback-scoped frame until the test releases it.
func (self *heldLifecycleSignalReceiver) ReceiveSignal(
	TransferPath,
	TransferKey,
	*protocol.Frame,
) error {
	self.once.Do(func() { close(self.entered) })
	<-self.release
	return nil
}

// TestClientCloseAndWaitJoinsSignalCompatibilityDelivery proves a Client join
// retains an in-flight compatibility frame until its callback returns, then
// performs the exact final pool return.
func TestClientCloseAndWaitJoinsSignalCompatibilityDelivery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client := NewClient(ctx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	receiver := &heldLifecycleSignalReceiver{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	frameClosed := make(chan bool, 1)
	dispatcher := client.signalDispatcher
	dispatcher.receiver = receiver
	joinEntered := make(chan struct{})
	var joinEnteredOnce sync.Once
	dispatcher.beforeCloseWaitForTest = func() {
		joinEnteredOnce.Do(func() { close(joinEntered) })
	}
	for _, shard := range dispatcher.shards {
		shard.receiver = receiver
		shard.frameClosedForTest = frameClosed
	}

	frame := newTestingCompatibilitySignalFrame(t, NewId())
	defer MessagePoolReturn(frame.MessageBytes)
	dispatcher.handleControlFrame(SourceId(NewId()), TransferKey{}, frame)
	waitCloseWaitBarrier(t, ctx, receiver.entered, "signal compatibility callback")

	result := make(chan error, 1)
	go func() { result <- client.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, joinEntered, "signal dispatcher join")
	requireCloseWaitBlocked(t, result, "Client.CloseAndWait signal callback")
	select {
	case returned := <-frameClosed:
		t.Fatalf("signal compatibility bytes returned before callback exit: %t", returned)
	default:
	}
	close(receiver.release)
	select {
	case returned := <-frameClosed:
		if !returned {
			t.Fatal("signal callback did not perform the exact final pool return")
		}
	case <-ctx.Done():
		t.Fatalf("wait for signal frame return: %v", ctx.Err())
	}
	waitCloseWaitResult(t, ctx, result, "join signal compatibility delivery")
}

// TestClientSignalReceiverCloseRejectsPausedWorkerStart proves Close wins
// against a shard whose lazy worker start is paused before gate admission.
func TestClientSignalReceiverCloseRejectsPausedWorkerStart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	shardCtx, shardCancel := context.WithCancel(ctx)
	workers := newLifecycleAdmission()
	startEntered := make(chan struct{})
	releaseStart := make(chan struct{})
	var enteredOnce sync.Once
	workers.beforeStartLockForTest = func() {
		enteredOnce.Do(func() { close(startEntered) })
		<-releaseStart
	}
	shard := &clientSignalReceiver{
		client:       &Client{log: NewNoopLogger()},
		ctx:          shardCtx,
		cancel:       shardCancel,
		queueLimit:   1,
		queueMonitor: NewMonitor(),
		dropWarnings: make(chan signalDropWarning, 1),
		workers:      workers,
	}
	startDone := make(chan struct{})
	go func() {
		shard.start()
		close(startDone)
	}()
	waitCloseWaitBarrier(t, ctx, startEntered, "paused signal worker admission")
	shard.Close()
	close(releaseStart)
	waitCloseWaitBarrier(t, ctx, startDone, "rejected signal worker start")
	waitCloseWaitBarrier(t, ctx, workers.Done(), "closed signal worker gate")
}

// TestClientWebRtcCloseIsNonJoiningAndCloseAndWaitIsRetryable proves the
// compatibility Close returns while final cleanup is held, a canceled wait
// fails, and a later wait joins the same generation successfully.
func TestClientWebRtcCloseIsNonJoiningAndCloseAndWaitIsRetryable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	manager := client.webRtcManager
	cleanupEntered := make(chan struct{})
	releaseCleanup := make(chan struct{})
	var cleanupOnce sync.Once
	manager.beforeCloseDoneForTest = func() {
		cleanupOnce.Do(func() { close(cleanupEntered) })
		<-releaseCleanup
	}
	closeReturned := make(chan struct{})
	go func() {
		client.Close()
		close(closeReturned)
	}()
	waitCloseWaitBarrier(t, ctx, cleanupEntered, "held WebRTC final cleanup")
	waitCloseWaitBarrier(t, ctx, closeReturned, "non-joining Client.Close")

	canceledCtx, cancelWait := context.WithCancel(context.Background())
	cancelWait()
	if err := client.CloseAndWait(canceledCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Client.CloseAndWait error = %v, want context.Canceled", err)
	}
	result := make(chan error, 1)
	go func() { result <- client.CloseAndWait(ctx) }()
	requireCloseWaitBlocked(t, result, "retry WebRTC close join")
	close(releaseCleanup)
	waitCloseWaitResult(t, ctx, result, "retry WebRTC close join")
}

// TestWebRtcManagerJoinsInFlightContextCloseCallback proves explicit Close
// cannot publish closeDone while its context.AfterFunc callback is held.
func TestWebRtcManagerJoinsInFlightContextCloseCallback(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	parentCtx, cancelParent := context.WithCancel(context.Background())
	manager := lifecycleTestManager(parentCtx, t)
	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	var enteredOnce sync.Once
	manager.beforeContextCloseForTest = func() {
		enteredOnce.Do(func() { close(callbackEntered) })
		<-releaseCallback
	}
	cancelParent()
	waitCloseWaitBarrier(t, ctx, callbackEntered, "WebRTC context close callback")
	manager.Close()
	result := make(chan error, 1)
	go func() { result <- manager.closeAndWait(ctx) }()
	requireCloseWaitBlocked(t, result, "WebRTC context callback join")
	close(releaseCallback)
	waitCloseWaitResult(t, ctx, result, "join WebRTC context callback")
}

// TestWebRtcManagerCloseRejectsPausedPeerAdmission proves an API caller paused
// before manager state cannot publish a peer generation after Close wins.
func TestWebRtcManagerCloseRejectsPausedPeerAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	manager := lifecycleTestManager(ctx, t)
	admissionEntered := make(chan struct{})
	releaseAdmission := make(chan struct{})
	var enteredOnce sync.Once
	manager.beforePeerConnAdmissionStateLockForTest = func() {
		enteredOnce.Do(func() { close(admissionEntered) })
		<-releaseAdmission
	}
	result := make(chan error, 1)
	go func() {
		_, err := manager.NewP2pConnActive(
			ctx,
			NewTransferPath(NewId(), NewId(), NewId()),
		)
		result <- err
	}()
	waitCloseWaitBarrier(t, ctx, admissionEntered, "paused WebRTC peer admission")
	if err := manager.closeAndWait(ctx); err != nil {
		t.Fatalf("close WebRTC manager before paused admission resumes: %v", err)
	}
	close(releaseAdmission)
	select {
	case err := <-result:
		if !errors.Is(err, os.ErrClosed) {
			t.Fatalf("post-close peer admission error = %v, want os.ErrClosed", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for rejected peer admission: %v", ctx.Err())
	}
}

// TestPeerConnPionCallbackGateJoinsAndRejectsLateCallback proves a callback
// already dispatched by Pion is joined while a callback arriving after gate
// close is rejected without executing its body.
func TestPeerConnPionCallbackGateJoinsAndRejectsLateCallback(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	peerCtx, cancelPeer := context.WithCancel(context.Background())
	peer := &peerConn{
		ctx:     peerCtx,
		cancel:  cancelPeer,
		workers: newLifecycleAdmission(),
	}
	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	callbackReturned := make(chan bool, 1)
	go func() {
		callbackReturned <- peer.runPionCallback("testing held callback", func() {
			close(callbackEntered)
			<-releaseCallback
		})
	}()
	waitCloseWaitBarrier(t, ctx, callbackEntered, "held Pion callback")
	cancelPeer()
	peer.workers.close()
	lateRan := false
	if peer.runPionCallback("testing late callback", func() { lateRan = true }) {
		t.Fatal("Pion callback was admitted after peer gate close")
	}
	if lateRan {
		t.Fatal("rejected Pion callback executed its body")
	}
	select {
	case <-peer.workers.Done():
		t.Fatal("peer callback gate completed before in-flight callback returned")
	default:
	}
	close(releaseCallback)
	select {
	case admitted := <-callbackReturned:
		if !admitted {
			t.Fatal("held Pion callback was not admitted")
		}
	case <-ctx.Done():
		t.Fatalf("wait for held Pion callback: %v", ctx.Err())
	}
	waitCloseWaitBarrier(t, ctx, peer.workers.Done(), "joined Pion callback gate")
}

// TestPeerConnWorkerGateJoinsAndRejectsLateLaunch proves the shared async
// launcher used by Run, connected dispatch, SCTP progress, native fast-path
// RTCP drain, and fast-path warmup cannot escape peer completion.
func TestPeerConnWorkerGateJoinsAndRejectsLateLaunch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	peer := &peerConn{workers: newLifecycleAdmission()}
	workerEntered := make(chan struct{})
	releaseWorker := make(chan struct{})
	if !peer.startWorker("fast path warmup", func() {
		close(workerEntered)
		<-releaseWorker
	}) {
		t.Fatal("open peer gate rejected async worker")
	}
	waitCloseWaitBarrier(t, ctx, workerEntered, "peer async worker")
	peer.workers.close()
	lateRan := false
	if peer.startWorker("fast path RTCP drain", func() { lateRan = true }) {
		t.Fatal("peer async worker was admitted after gate close")
	}
	if lateRan {
		t.Fatal("rejected peer async worker executed")
	}
	select {
	case <-peer.workers.Done():
		t.Fatal("peer gate completed before admitted async worker")
	default:
	}
	close(releaseWorker)
	waitCloseWaitBarrier(t, ctx, peer.workers.Done(), "joined peer async worker")
}

// TestPeerConnCanceledCallbacksCannotRepublishTerminalState exercises each
// callback publication lock after cancellation wins.
func TestPeerConnCanceledCallbacksCannotRepublishTerminalState(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()

	candidateCtx, cancelCandidate := context.WithCancel(context.Background())
	candidatePeer := &peerConn{ctx: candidateCtx}
	candidateEntered := make(chan struct{})
	releaseCandidate := make(chan struct{})
	candidatePeer.beforeIceCandidateStateLockForTest = func() {
		close(candidateEntered)
		<-releaseCandidate
	}
	candidateDone := make(chan struct{})
	go func() {
		candidatePeer.handleLocalIceCandidate(&webrtc.ICECandidate{})
		close(candidateDone)
	}()
	waitCloseWaitBarrier(t, ctx, candidateEntered, "candidate publication barrier")
	cancelCandidate()
	candidatePeer.stateLock.Lock()
	candidatePeer.iceCandidateBuffer = nil
	candidatePeer.stateLock.Unlock()
	close(releaseCandidate)
	waitCloseWaitBarrier(t, ctx, candidateDone, "canceled candidate callback")
	candidatePeer.stateLock.Lock()
	candidateCount := len(candidatePeer.iceCandidateBuffer)
	candidatePeer.stateLock.Unlock()
	if candidateCount != 0 {
		t.Fatalf("canceled callback republished %d ICE candidates", candidateCount)
	}

	connectedCtx, cancelConnected := context.WithCancel(context.Background())
	connectedPeer := &peerConn{
		ctx:              connectedCtx,
		log:              NewNoopLogger(),
		connectedMonitor: NewMonitor(),
	}
	connectedEntered := make(chan struct{})
	releaseConnected := make(chan struct{})
	connectedPeer.beforeConnectedStateLockForTest = func() {
		close(connectedEntered)
		<-releaseConnected
	}
	connectedDone := make(chan struct{})
	go func() {
		connectedPeer.setConnected(true)
		close(connectedDone)
	}()
	waitCloseWaitBarrier(t, ctx, connectedEntered, "connected publication barrier")
	cancelConnected()
	close(releaseConnected)
	waitCloseWaitBarrier(t, ctx, connectedDone, "canceled connected callback")
	if connectedPeer.Connected() {
		t.Fatal("canceled callback republished connected=true")
	}

	openCtx, cancelOpen := context.WithCancel(context.Background())
	openPeer := &peerConn{
		ctx:         openCtx,
		connMonitor: NewMonitor(),
	}
	openEntered := make(chan struct{})
	releaseOpen := make(chan struct{})
	openPeer.beforeOpenDataChannelStateLockForTest = func() {
		close(openEntered)
		<-releaseOpen
	}
	localPipe, remoteConn := net.Pipe()
	localConn := &lifecycleDataChannelConn{Conn: localPipe}
	defer remoteConn.Close()
	openResult := make(chan bool, 1)
	go func() { openResult <- openPeer.installOpenDataChannel(localConn) }()
	waitCloseWaitBarrier(t, ctx, openEntered, "data-channel publication barrier")
	cancelOpen()
	openPeer.stateLock.Lock()
	openPeer.conn = nil
	openPeer.stateLock.Unlock()
	close(releaseOpen)
	select {
	case accepted := <-openResult:
		if accepted {
			t.Fatal("canceled callback published a detached data channel")
		}
	case <-ctx.Done():
		t.Fatalf("wait for canceled data-channel callback: %v", ctx.Err())
	}
	openPeer.stateLock.Lock()
	installed := openPeer.conn
	openPeer.stateLock.Unlock()
	if installed != nil {
		t.Fatal("canceled callback left a detached connection installed")
	}
	if _, err := localConn.Write([]byte{1}); err == nil {
		t.Fatal("rejected detached connection was not closed")
	}
}

// TestPeerConnTeardownPublishesDisconnectedState proves a peer that was live
// before cancellation cannot remain observably connected after teardown.
func TestPeerConnTeardownPublishesDisconnectedState(t *testing.T) {
	peerCtx, cancelCause := context.WithCancelCause(context.Background())
	peer := &peerConn{
		ctx:              peerCtx,
		cancel:           func() { cancelCause(context.Canceled) },
		cancelCause:      cancelCause,
		log:              NewNoopLogger(),
		connected:        true,
		connMonitor:      NewMonitor(),
		connectedMonitor: NewMonitor(),
		teardownDone:     make(chan struct{}),
	}
	peer.cancel()
	peer.teardown()
	if peer.Connected() {
		t.Fatal("joined peer teardown left connected=true")
	}
	select {
	case <-peer.teardownDone:
	default:
		t.Fatal("peer teardown did not publish completion")
	}
}

// TestPeerConnCanceledInboundSignalCannotRepublishState proves cancellation
// that wins after the initial signal check but before signalLock prevents a
// stale candidate from repopulating the retired generation.
func TestPeerConnCanceledInboundSignalCannotRepublishState(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	peerCtx, cancelPeer := context.WithCancel(context.Background())
	peer := &peerConn{ctx: peerCtx}
	lockEntered := make(chan struct{})
	releaseLock := make(chan struct{})
	peer.beforeReceiveSignalLockForTest = func() {
		close(lockEntered)
		<-releaseLock
	}
	candidateBytes := []byte(`{"candidate":"candidate:0 1 udp 2122252543 127.0.0.1 40000 typ host"}`)
	result := make(chan error, 1)
	go func() {
		result <- peer.receiveSignalFromPeerWithTransferKey(
			&protocol.ExchangeSignal{
				SignalType:   protocol.SignalType_IceCandidate,
				IceCandidate: candidateBytes,
			},
			Id{},
			false,
			TransferKey{},
			true,
		)
	}()
	waitCloseWaitBarrier(t, ctx, lockEntered, "inbound signal state lock")
	cancelPeer()
	peer.signalLock.Lock()
	peer.remoteIceCandidateBuffer = nil
	peer.remoteIceCandidateBufferBytes = 0
	peer.signalLock.Unlock()
	close(releaseLock)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("canceled inbound signal returned error: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for canceled inbound signal: %v", ctx.Err())
	}
	peer.signalLock.Lock()
	candidateCount := len(peer.remoteIceCandidateBuffer)
	peer.signalLock.Unlock()
	if candidateCount != 0 {
		t.Fatalf("canceled inbound signal republished %d candidates", candidateCount)
	}
	peer.setSignalReplyTransferKey(TransferKey{ForceStream: true})
	peer.stateLock.Lock()
	replyKeySet := peer.signalReplyTransferKeySet
	peer.stateLock.Unlock()
	if replyKeySet {
		t.Fatal("canceled inbound signal republished its reply key")
	}
}

// TestWebRtcManagerSignalBatchGateJoinsAndRejectsLateBatch proves the manager's
// concrete decoded-signal entry point owns a peer lifecycle admission across
// the whole batch and rejects a batch that arrives after gate closure.
func TestWebRtcManagerSignalBatchGateJoinsAndRejectsLateBatch(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	peerCtx, cancelPeer := context.WithCancel(context.Background())
	peerId := NewId()
	streamId := NewId()
	peer := &peerConn{
		ctx:     peerCtx,
		log:     NewNoopLogger(),
		key:     peerConnKey{PeerId: peerId, StreamId: streamId},
		workers: newLifecycleAdmission(),
	}
	batchEntered := make(chan struct{})
	releaseBatch := make(chan struct{})
	peer.beforeReceiveSignalBatchForTest = func() {
		close(batchEntered)
		<-releaseBatch
	}
	manager := &WebRtcManager{
		log:       NewNoopLogger(),
		peerConns: map[peerConnKey]*peerConn{peer.key: peer},
	}
	candidate := &protocol.ExchangeSignals{
		StreamId: streamId.Bytes(),
		Signals: []*protocol.ExchangeSignal{{
			SignalType: protocol.SignalType_IceCandidate,
			IceCandidate: []byte(
				`{"candidate":"candidate:0 1 udp 2122252543 127.0.0.1 40000 typ host"}`,
			),
		}},
	}
	result := make(chan error, 1)
	go func() {
		result <- manager.ReceiveExchangeSignals(SourceId(peerId), TransferKey{}, candidate)
	}()
	waitCloseWaitBarrier(t, ctx, batchEntered, "manager signal batch admission")
	peer.workers.close()
	lateErr := manager.ReceiveExchangeSignals(SourceId(peerId), TransferKey{}, candidate)
	if lateErr != nil {
		t.Fatalf("late rejected signal batch returned error: %v", lateErr)
	}
	select {
	case <-peer.workers.Done():
		t.Fatal("peer signal gate completed before admitted batch returned")
	default:
	}
	cancelPeer()
	close(releaseBatch)
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("canceled admitted signal batch returned error: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for admitted signal batch: %v", ctx.Err())
	}
	waitCloseWaitBarrier(t, ctx, peer.workers.Done(), "joined manager signal batch")
	peer.signalLock.Lock()
	candidateCount := len(peer.remoteIceCandidateBuffer)
	peer.signalLock.Unlock()
	if candidateCount != 0 {
		t.Fatalf("closed signal gate retained %d late candidates", candidateCount)
	}
}

// lifecycleReadBarrierConn exposes the exact blocking Read entry used by the
// receive transport test.
type lifecycleReadBarrierConn struct {
	net.Conn
	readEntered      chan struct{}
	readMessageBytes chan []byte
	readOnce         sync.Once
}

// Read announces entry before delegating to the blocking pipe.
func (self *lifecycleReadBarrierConn) Read(message []byte) (int, error) {
	self.readOnce.Do(func() {
		if self.readMessageBytes != nil {
			self.readMessageBytes <- message
		}
		close(self.readEntered)
	})
	return self.Conn.Read(message)
}

// lifecycleImmediateCloseConn records the shutdown deadline and exposes any
// subsequent Read as the exact lost-interrupt failure. Its Read remains
// releasable so a failing regression can still clean up its worker.
type lifecycleImmediateCloseConn struct {
	net.Conn
	shutdownDeadlineSet chan struct{}
	readEntered         chan struct{}
	releaseRead         chan struct{}
	deadlineOnce        sync.Once
	readOnce            sync.Once
}

// SetReadDeadline records Close's expired deadline before delegating it.
func (self *lifecycleImmediateCloseConn) SetReadDeadline(deadline time.Time) error {
	if !deadline.IsZero() {
		self.deadlineOnce.Do(func() { close(self.shutdownDeadlineSet) })
	}
	return self.Conn.SetReadDeadline(deadline)
}

// Read publishes the old lost-interrupt behavior and waits for test cleanup.
func (self *lifecycleImmediateCloseConn) Read([]byte) (int, error) {
	self.readOnce.Do(func() { close(self.readEntered) })
	<-self.releaseRead
	return 0, io.EOF
}

// TestP2pSendTransportDoneFollowsFinalPoolDrain proves the send child done
// signal is published only after its final route drain returns pooled bytes.
func TestP2pSendTransportDoneFollowsFinalPoolDrain(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	transportCtx, cancelTransport := context.WithCancel(context.Background())
	localConn, remoteConn := net.Pipe()
	defer localConn.Close()
	defer remoteConn.Close()
	transportValue, route := NewP2pSendTransport(
		transportCtx,
		cancelTransport,
		localConn,
		NewId(),
		DefaultP2pTransportSettings(),
	)
	transport := transportValue.(*P2pSendTransport)
	lifecycle := transportValue.(P2pRouteLifecycle)
	admissionClosed := make(chan struct{})
	releaseAdmission := make(chan struct{})
	drainReached := make(chan struct{})
	releaseDrain := make(chan struct{})
	transport.testingAfterProbeSendAdmissionClosed = func() {
		close(admissionClosed)
		<-releaseAdmission
	}
	transport.testingAfterProbeSendDrain = func() {
		close(drainReached)
		<-releaseDrain
	}
	joinResult := make(chan error, 1)
	go func() { joinResult <- lifecycle.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, admissionClosed, "send final-drain admission close")
	capture := newLifecyclePoolCapture(MessagePoolGet(512))
	defer capture.cleanup()
	route <- capture.owner
	capture.requireOwnerLive(t, "send final-drain frame")
	close(releaseAdmission)
	waitCloseWaitBarrier(t, ctx, drainReached, "send final pool drain")
	capture.requireOwnerReturned(t, "send final-drain frame")
	select {
	case <-lifecycle.Done():
		t.Fatal("send child published done before final-drain hook returned")
	default:
	}
	requireCloseWaitBlocked(t, joinResult, "low-level P2P send join")
	close(releaseDrain)
	waitCloseWaitResult(t, ctx, joinResult, "low-level P2P send join")
	waitCloseWaitBarrier(t, ctx, lifecycle.Done(), "send transport done")
}

// TestP2pReceiveTransportDoneFollowsFinalPoolDrain proves cancellation joins
// the blocking reader and drains its queued pool owner before done.
func TestP2pReceiveTransportDoneFollowsFinalPoolDrain(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	transportCtx, cancelTransport := context.WithCancel(context.Background())
	localPipe, remotePipe := net.Pipe()
	conn := &lifecycleReadBarrierConn{
		Conn:             localPipe,
		readEntered:      make(chan struct{}),
		readMessageBytes: make(chan []byte, 1),
	}
	defer localPipe.Close()
	defer remotePipe.Close()
	transportValue, _ := NewP2pReceiveTransport(
		transportCtx,
		cancelTransport,
		conn,
		NewId(),
		DefaultP2pTransportSettings(),
	)
	transport := transportValue.(*P2pReceiveTransport)
	lifecycle := transportValue.(P2pRouteLifecycle)
	drainReached := make(chan struct{})
	releaseDrain := make(chan struct{})
	transport.testingBeforeDoneForTest = func() {
		close(drainReached)
		<-releaseDrain
	}
	waitCloseWaitBarrier(t, ctx, conn.readEntered, "P2P receive read")
	var blockedReadCapture *lifecyclePoolCapture
	select {
	case message := <-conn.readMessageBytes:
		blockedReadCapture = newLifecyclePoolCapture(message)
	case <-ctx.Done():
		t.Fatalf("capture P2P blocked-read owner: %v", ctx.Err())
	}
	defer blockedReadCapture.cleanup()
	blockedReadCapture.requireOwnerLive(t, "P2P blocked-read buffer")
	capture := newLifecyclePoolCapture(MessagePoolGet(512))
	defer capture.cleanup()
	// Exercise the native-datagram carrier handoff. Reliable SCTP now waits
	// directly on its exact route; only the fast lane owns the bounded pending
	// queue whose final drain this test pins.
	if !transport.offerReceive(capture.owner, true, 0, false, false) {
		t.Fatal("receive final-drain frame was not admitted")
	}
	if retained := transport.pendingReceiveByteCount.Load(); retained != 512 {
		t.Fatalf("receive final-drain retained bytes = %d, want 512", retained)
	}
	if retained := transport.pendingReceiveMessageCount.Load(); retained != 1 {
		t.Fatalf("receive final-drain retained messages = %d, want 1", retained)
	}
	capture.requireOwnerLive(t, "receive final-drain frame")
	joinResult := make(chan error, 1)
	go func() { joinResult <- lifecycle.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, drainReached, "receive final pool drain")
	blockedReadCapture.requireOwnerReturned(t, "P2P blocked-read buffer")
	capture.requireOwnerReturned(t, "receive final-drain frame")
	select {
	case <-lifecycle.Done():
		t.Fatal("receive child published done before final-drain hook returned")
	default:
	}
	requireCloseWaitBlocked(t, joinResult, "low-level P2P receive join")
	close(releaseDrain)
	waitCloseWaitResult(t, ctx, joinResult, "low-level P2P receive join")
	waitCloseWaitBarrier(t, ctx, lifecycle.Done(), "receive transport done")
}

// TestP2pReceiveTransportImmediateCloseCannotLoseReadInterrupt pauses before
// the worker clears its inherited handshake deadline, then closes first. The
// canceled worker must publish completion instead of clearing Close's expired
// deadline and entering an unbounded steady-state Read.
func TestP2pReceiveTransportImmediateCloseCannotLoseReadInterrupt(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	transportCtx, cancelTransport := context.WithCancel(context.Background())
	defer cancelTransport()
	localPipe, remotePipe := net.Pipe()
	defer localPipe.Close()
	defer remotePipe.Close()
	deadlineClearEntered := make(chan struct{})
	releaseDeadlineClear := make(chan struct{})
	var releaseDeadlineClearOnce sync.Once
	defer releaseDeadlineClearOnce.Do(func() { close(releaseDeadlineClear) })
	settings := DefaultP2pTransportSettings()
	settings.beforeReceiveSteadyStateDeadlineClearForTest = func() {
		close(deadlineClearEntered)
		<-releaseDeadlineClear
	}
	conn := &lifecycleImmediateCloseConn{
		Conn:                localPipe,
		shutdownDeadlineSet: make(chan struct{}),
		readEntered:         make(chan struct{}),
		releaseRead:         make(chan struct{}),
	}
	defer close(conn.releaseRead)
	transportValue, _ := NewP2pReceiveTransport(
		transportCtx,
		cancelTransport,
		conn,
		NewId(),
		settings,
	)
	lifecycle := transportValue.(P2pRouteLifecycle)
	waitCloseWaitBarrier(
		t,
		ctx,
		deadlineClearEntered,
		"receive steady-state deadline clear",
	)
	joinResult := make(chan error, 1)
	go func() { joinResult <- lifecycle.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(
		t,
		ctx,
		conn.shutdownDeadlineSet,
		"receive shutdown deadline",
	)
	releaseDeadlineClearOnce.Do(func() { close(releaseDeadlineClear) })
	select {
	case err := <-joinResult:
		if err != nil {
			t.Fatalf("immediate low-level receive join: %v", err)
		}
	case <-conn.readEntered:
		t.Fatal("canceled receive worker entered Read after clearing Close's deadline")
	case <-ctx.Done():
		t.Fatalf("wait for immediate low-level receive join: %v", ctx.Err())
	}
	waitCloseWaitBarrier(t, ctx, lifecycle.Done(), "immediate receive transport done")
}

// lifecycleFastPathReader independently releases Read and holds the context
// deadline callback so their completion ordering is deterministic.
type lifecycleFastPathReader struct {
	readEntered     chan struct{}
	releaseRead     chan struct{}
	deadlineEntered chan struct{}
	releaseDeadline chan struct{}
	readOnce        sync.Once
	deadlineOnce    sync.Once
}

// Read waits until the test permits EOF.
func (self *lifecycleFastPathReader) Read([]byte) (int, error) {
	self.readOnce.Do(func() { close(self.readEntered) })
	<-self.releaseRead
	return 0, io.EOF
}

// SetReadDeadline holds the exact context.AfterFunc callback.
func (self *lifecycleFastPathReader) SetReadDeadline(time.Time) error {
	self.deadlineOnce.Do(func() { close(self.deadlineEntered) })
	<-self.releaseDeadline
	return nil
}

// TestWebRtcFastPathReceiveJoinsDeadlineCallback proves receiveDone cannot
// publish while its cancellation callback is still inside SetReadDeadline.
func TestWebRtcFastPathReceiveJoinsDeadlineCallback(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	fastCtx, cancelFast := context.WithCancel(context.Background())
	fastPath := &webRtcFastPath{
		ctx:                     fastCtx,
		log:                     NewNoopLogger(),
		maximumMessageByteCount: 64 * 1024,
		messages:                make(chan p2pFastPathReceivedMessage, 1),
		receiveDone:             make(chan struct{}),
		ready:                   make(chan struct{}),
	}
	reader := &lifecycleFastPathReader{
		readEntered:     make(chan struct{}),
		releaseRead:     make(chan struct{}),
		deadlineEntered: make(chan struct{}),
		releaseDeadline: make(chan struct{}),
	}
	fastPath.startReceive(reader)
	waitCloseWaitBarrier(t, ctx, reader.readEntered, "fast-path receive read")
	cancelFast()
	waitCloseWaitBarrier(t, ctx, reader.deadlineEntered, "fast-path deadline callback")
	close(reader.releaseRead)
	select {
	case <-fastPath.receiveDone:
		t.Fatal("fast-path receive completed before deadline callback returned")
	default:
	}
	close(reader.releaseDeadline)
	waitCloseWaitBarrier(t, ctx, fastPath.receiveDone, "fast-path receive completion")
	fastPath.closeAndWait()
}

// TestPeerConnectionTeardownWatchdogStopJoinsInFlightCallback proves stopping
// an already-fired diagnostic timer waits for its callback generation.
func TestPeerConnectionTeardownWatchdogStopJoinsInFlightCallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var stage atomic.Int32
	stage.Store(int32(peerConnectionTeardownClosingPeer))
	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	watchdog := startPeerConnectionTeardownWatchdog(
		0,
		&stage,
		func(peerConnectionTeardownStage) {
			close(callbackEntered)
			<-releaseCallback
		},
	)
	waitCloseWaitBarrier(t, ctx, callbackEntered, "teardown watchdog callback")
	stopDone := make(chan struct{})
	go func() {
		watchdog.StopAndWait()
		close(stopDone)
	}()
	select {
	case <-stopDone:
		t.Fatal("watchdog stop returned before in-flight callback")
	default:
	}
	close(releaseCallback)
	waitCloseWaitBarrier(t, ctx, stopDone, "joined teardown watchdog callback")
}

// TestControlSyncOobCloseRejectsPausedSend proves a Send paused before its
// generation lock cannot reinstall currentCancel or launch OOB work after
// Close wins.
func TestControlSyncOobCloseRejectsPausedSend(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	oob := &blockingLifecycleOob{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	client := NewClient(ctx, NewId(), oob, closeWaitClientSettings())
	control := NewControlSyncOob(ctx, client, "paused-send")
	lockEntered := make(chan struct{})
	releaseLock := make(chan struct{})
	var enteredOnce sync.Once
	control.beforeSendLockForTest = func() {
		enteredOnce.Do(func() { close(lockEntered) })
		<-releaseLock
	}
	frame, err := ToFrame(&protocol.SimpleMessage{Content: "paused"}, 0)
	if err != nil {
		t.Fatal(err)
	}
	capture := newLifecyclePoolCapture(frame.MessageBytes)
	defer capture.cleanup()
	sendDone := make(chan struct{})
	go func() {
		control.Send(frame, nil)
		close(sendDone)
	}()
	waitCloseWaitBarrier(t, ctx, lockEntered, "paused OOB Send lock")
	capture.requireOwnerLive(t, "paused OOB input frame")
	control.Close()
	close(releaseLock)
	waitCloseWaitBarrier(t, ctx, sendDone, "rejected OOB Send")
	capture.requireOwnerReturned(t, "paused OOB input frame")
	select {
	case <-oob.entered:
		t.Fatal("post-close OOB Send launched external work")
	default:
	}
	control.sendLock.Lock()
	closed := control.closed
	currentCancel := control.currentCancel
	control.sendLock.Unlock()
	if !closed || currentCancel != nil {
		t.Fatalf("post-close OOB state = closed:%t currentCancel:%v", closed, currentCancel != nil)
	}
	if err := control.closeAndWait(ctx); err != nil {
		t.Fatalf("join rejected OOB Send: %v", err)
	}
	close(oob.release)
	if err := client.CloseAndWait(ctx); err != nil {
		t.Fatalf("join paused-OOB test client: %v", err)
	}
}

// TestEncryptedControlPackRejectionReturnsMarshalOwner proves an admitted
// encryption worker racing SendBuffer shutdown returns ProtoMarshal ownership
// when Pack rejects it.
func TestEncryptedControlPackRejectionReturnsMarshalOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	settings := closeWaitClientSettings()
	captured := make(chan *lifecyclePoolCapture, 1)
	releasePack := make(chan struct{})
	var captureOnce sync.Once
	settings.SendBufferSettings.beforeEncryptedControlPackForTest = func(message []byte) {
		captureOnce.Do(func() {
			captured <- newLifecyclePoolCapture(message)
			<-releasePack
		})
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	manager := client.encryptionSessionManager
	if !manager.workers.start() {
		t.Fatal("could not admit testing encryption sender")
	}
	sendResult := make(chan bool, 1)
	go func() {
		defer manager.workers.finish()
		sendResult <- client.sendBuffer.SendEncryptedControl(
			manager.ctx,
			NewId(),
			sequenceTlsRoleClient,
			&protocol.EncryptedControl{
				ControlType: protocol.EncryptedControlType_EncryptedControlHandshake,
				Payload:     []byte{1, 2, 3},
			},
			false,
			false,
			false,
			false,
		)
	}()
	var capture *lifecyclePoolCapture
	select {
	case capture = <-captured:
	case <-ctx.Done():
		t.Fatalf("wait for encrypted-control marshal owner: %v", ctx.Err())
	}
	defer capture.cleanup()
	capture.requireOwnerLive(t, "encrypted-control marshal bytes")
	// Establish the rejection boundary synchronously before releasing the
	// sender paused immediately ahead of Pack.
	client.Close()
	joinResult := make(chan error, 1)
	go func() { joinResult <- client.CloseAndWait(ctx) }()
	requireCloseWaitBlocked(t, joinResult, "encryption worker Pack rejection")
	close(releasePack)
	select {
	case success := <-sendResult:
		if success {
			t.Fatal("encrypted control was admitted after client close")
		}
	case <-ctx.Done():
		t.Fatalf("wait for rejected encrypted control: %v", ctx.Err())
	}
	capture.requireOwnerReturned(t, "encrypted-control marshal bytes")
	waitCloseWaitResult(t, ctx, joinResult, "join rejected encrypted-control worker")
}

// TestProvidePingSendRejectionReturnsFrameOwner proves the real manager worker
// returns ToFrame ownership when shutdown rejects its control send.
func TestProvidePingSendRejectionReturnsFrameOwner(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	settings := closeWaitClientSettings()
	settings.ContractManagerSettings.ProvidePingTimeout = time.Nanosecond
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	manager := client.contractManager
	captured := make(chan *lifecyclePoolCapture, 1)
	releaseSend := make(chan struct{})
	var captureOnce sync.Once
	manager.beforeProvidePingSendForTest = func(frame *protocol.Frame) {
		captureOnce.Do(func() {
			captured <- newLifecyclePoolCapture(frame.MessageBytes)
			<-releaseSend
		})
	}
	manager.mutex.Lock()
	manager.provideModes[protocol.ProvideMode_Public] = true
	manager.mutex.Unlock()
	manager.provideMonitor.NotifyAll()
	var capture *lifecyclePoolCapture
	select {
	case capture = <-captured:
	case <-ctx.Done():
		t.Fatalf("wait for provide-ping frame owner: %v", ctx.Err())
	}
	defer capture.cleanup()
	capture.requireOwnerLive(t, "provide-ping frame")
	joinResult := make(chan error, 1)
	go func() { joinResult <- client.CloseAndWait(ctx) }()
	requireCloseWaitBlocked(t, joinResult, "provide-ping send rejection")
	close(releaseSend)
	waitCloseWaitResult(t, ctx, joinResult, "join rejected provide ping")
	capture.requireOwnerReturned(t, "provide-ping frame")
}

// TestClientSignalDispatcherJoinsInFlightContextCloseCallback proves explicit
// dispatcher close cannot publish completion while its context callback is
// paused before Close.
func TestClientSignalDispatcherJoinsInFlightContextCloseCallback(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	clientCtx, cancelClient := context.WithCancel(context.Background())
	client := NewClient(clientCtx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	dispatcher := client.signalDispatcher
	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	var enteredOnce sync.Once
	dispatcher.beforeContextCloseForTest = func() {
		enteredOnce.Do(func() { close(callbackEntered) })
		<-releaseCallback
	}
	cancelClient()
	waitCloseWaitBarrier(t, ctx, callbackEntered, "signal dispatcher context callback")
	dispatcher.Close()
	result := make(chan error, 1)
	go func() { result <- dispatcher.closeAndWait(ctx) }()
	requireCloseWaitBlocked(t, result, "signal dispatcher context callback join")
	close(releaseCallback)
	waitCloseWaitResult(t, ctx, result, "join signal dispatcher context callback")
	if err := client.CloseAndWait(ctx); err != nil {
		t.Fatalf("join signal-context test client: %v", err)
	}
}

// TestContractStatusDonePrecedesManagerFinish proves the worker publishes its
// own completion before releasing its parent manager lifecycle admission.
func TestContractStatusDonePrecedesManagerFinish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	finishedEntered := make(chan struct{})
	releaseFinished := make(chan struct{})
	worker := newContractStatusCallbackWorker(
		ctx,
		func(*ContractStatus) {},
		1,
		func() {
			close(finishedEntered)
			<-releaseFinished
		},
	)
	worker.Close()
	waitCloseWaitBarrier(t, ctx, finishedEntered, "contract status manager finish")
	select {
	case <-worker.done:
	default:
		t.Fatal("contract status manager admission finished before worker.done")
	}
	close(releaseFinished)
}

// TestPeerConnFastPathWarmupUsesOwnedWorker pins the actual warmup call site to
// peerConn.startWorker rather than only testing that launcher in isolation.
func TestPeerConnFastPathWarmupUsesOwnedWorker(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	peerCtx, cancelPeer := context.WithCancel(context.Background())
	peer := &peerConn{
		ctx:     peerCtx,
		workers: newLifecycleAdmission(),
	}
	fastPath := &webRtcFastPath{
		ctx:   peerCtx,
		log:   NewNoopLogger(),
		track: &webRtcFastPathTrack{},
	}
	peer.fastPath.Store(fastPath)
	workerDoneEntered := make(chan struct{})
	releaseWorkerDone := make(chan struct{})
	peer.beforeWorkerDoneForTest = func(name string) {
		if name == "fast path warmup" {
			close(workerDoneEntered)
			<-releaseWorkerDone
		}
	}
	peer.startFastPathWarmup()
	cancelPeer()
	peer.workers.close()
	waitCloseWaitBarrier(t, ctx, workerDoneEntered, "owned fast-path warmup")
	select {
	case <-peer.workers.Done():
		t.Fatal("peer completed before warmup worker terminal hook")
	default:
	}
	close(releaseWorkerDone)
	waitCloseWaitBarrier(t, ctx, peer.workers.Done(), "joined fast-path warmup")
}

// consumingLifecycleSignalSender releases every signal frame synchronously.
type consumingLifecycleSignalSender struct{}

// SendSignal consumes the signal according to SignalSender's contract.
func (consumingLifecycleSignalSender) SendSignal(
	_ Id,
	signal *protocol.Frame,
	_ ...any,
) {
	MessagePoolReturn(signal.MessageBytes)
}

// TestPeerConnPionStartupAndTeardownAreSerialized pins the cancellation race
// that previously let Run create an ICE agent after teardown had already
// closed the PeerConnection. A non-mutating setup hook must not block physical
// teardown, while Run must recheck cancellation under the Pion mutation gate
// before SetLocalDescription.
func TestPeerConnPionStartupAndTeardownAreSerialized(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	peerCtx, cancelPeer := context.WithCancel(context.Background())
	defer cancelPeer()
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.EnableDatagramFastPath = false
	peer, err := newPeerConn(
		peerCtx,
		peerConnKey{PeerId: NewId(), StreamId: NewId()},
		NewId(),
		true,
		consumingLifecycleSignalSender{},
		settings,
		func() (*webrtc.PeerConnection, context.CancelFunc, error) {
			pc, createErr := webrtc.NewPeerConnection(webrtc.Configuration{})
			return pc, func() {}, createErr
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	startupEntered := make(chan struct{})
	releaseStartup := make(chan struct{})
	peer.beforeSetLocalDescriptionForTest = func() {
		close(startupEntered)
		<-releaseStartup
	}
	if !peer.startWorker("peer connection run", peer.Run) {
		t.Fatal("could not start peer Run")
	}
	waitCloseWaitBarrier(t, ctx, startupEntered, "Pion startup mutation")
	if !peer.pionLifecycleLock.TryLock() {
		t.Fatal("non-mutating startup hook retained the Pion lifecycle owner")
	}
	peer.pionLifecycleLock.Unlock()
	go peer.teardown()
	waitCloseWaitBarrier(t, ctx, peer.ctx.Done(), "peer cancellation before teardown")
	waitCloseWaitBarrier(t, ctx, peer.teardownDone, "Pion teardown during setup hook")
	close(releaseStartup)
	waitCloseWaitBarrier(t, ctx, peer.workers.Done(), "canceled Pion startup worker")
	if peer.pc.LocalDescription() != nil {
		t.Fatal("canceled startup set a local description after teardown began")
	}
}

// TestPeerConnPionRegistrationUsesCallbackGate starts the real Run callback
// registration and makes Pion dispatch a terminal state callback. The peer
// gate must retain that concrete callback until its body returns.
func TestPeerConnPionRegistrationUsesCallbackGate(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	peerCtx, cancelPeer := context.WithCancel(context.Background())
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.EnableDatagramFastPath = false
	peer, err := newPeerConn(
		peerCtx,
		peerConnKey{PeerId: NewId(), StreamId: NewId()},
		NewId(),
		false,
		consumingLifecycleSignalSender{},
		settings,
		func() (*webrtc.PeerConnection, context.CancelFunc, error) {
			pc, createErr := webrtc.NewPeerConnection(webrtc.Configuration{})
			return pc, func() {}, createErr
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	registered := make(chan struct{})
	peer.afterPionCallbacksRegisteredForTest = func() { close(registered) }
	callbackEntered := make(chan struct{})
	releaseCallback := make(chan struct{})
	var callbackOnce sync.Once
	peer.beforeWorkerDoneForTest = func(name string) {
		if name == "peer connection state callback" {
			callbackOnce.Do(func() { close(callbackEntered) })
			<-releaseCallback
		}
	}
	if !peer.startWorker("peer connection run", peer.Run) {
		t.Fatal("could not start peer Run")
	}
	waitCloseWaitBarrier(t, ctx, registered, "Pion callback registration")
	closeReturned := make(chan struct{})
	go func() {
		_ = peer.pc.Close()
		close(closeReturned)
	}()
	waitCloseWaitBarrier(t, ctx, callbackEntered, "Pion connection-state callback")
	cancelPeer()
	peer.workers.close()
	select {
	case <-peer.workers.Done():
		t.Fatal("peer completed before registered Pion callback returned")
	default:
	}
	close(releaseCallback)
	waitCloseWaitBarrier(t, ctx, closeReturned, "Pion close")
	waitCloseWaitBarrier(t, ctx, peer.workers.Done(), "registered Pion callback join")
}

// TestPeerConnFastPathOnTrackUsesCallbackGate establishes a real loopback
// SRTP pair and holds the passive peer's concrete Pion OnTrack callback. Peer
// completion must wait for that callback and reject later callback admission.
func TestPeerConnFastPathOnTrackUsesCallbackGate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	trackEntered := make(chan struct{})
	releaseTrack := make(chan struct{})
	var trackOnce sync.Once
	settingsA := DefaultWebRtcSettings()
	settingsB := DefaultWebRtcSettings()
	for _, settings := range []*WebRtcSettings{settingsA, settingsB} {
		settings.Log = NewNoopLogger()
		settings.IceServerUrls = nil
		settings.UseLoopbackOnlyIceInterfaces = true
		settings.EnableDatagramFastPath = true
	}
	settingsB.beforeFastPathOnTrackBodyForTest = func() {
		trackOnce.Do(func() { close(trackEntered) })
		<-releaseTrack
	}
	signalPipeA := newSignalPipe(nil)
	signalPipeB := newSignalPipe(nil)
	managerA := newTestWebRtcManager(t, ctx, signalPipeA, settingsA)
	managerB := newTestWebRtcManager(t, ctx, signalPipeB, settingsB)
	signalPipeA.SetSignalReceiver(managerB)
	signalPipeB.SetSignalReceiver(managerA)
	streamId := NewId()
	peerIdA := NewId()
	peerIdB := NewId()
	passiveValue, err := managerB.NewP2pConnPassive(
		ctx,
		NewTransferPath(peerIdB, peerIdA, streamId),
	)
	if err != nil {
		t.Fatal(err)
	}
	activeValue, err := managerA.NewP2pConnActive(
		ctx,
		NewTransferPath(peerIdA, peerIdB, streamId),
	)
	if err != nil {
		passiveValue.Close()
		t.Fatal(err)
	}
	passive := passiveValue.(*peerConn)
	waitCloseWaitBarrier(t, ctx, trackEntered, "real fast-path OnTrack callback")
	passive.cancel()
	passive.workers.close()
	select {
	case <-passive.workers.Done():
		t.Fatal("peer completed before real OnTrack callback returned")
	default:
	}
	lateRan := false
	if passive.runPionCallback("fast path track callback", func() { lateRan = true }) {
		t.Fatal("late OnTrack callback was admitted after peer close")
	}
	if lateRan {
		t.Fatal("rejected late OnTrack callback executed")
	}
	close(releaseTrack)
	waitCloseWaitBarrier(t, ctx, passive.workers.Done(), "joined real OnTrack callback")
	activeValue.Close()
	passiveValue.Close()
	if err := managerA.closeAndWait(ctx); err != nil {
		t.Fatalf("join active OnTrack test manager: %v", err)
	}
	if err := managerB.closeAndWait(ctx); err != nil {
		t.Fatalf("join passive OnTrack test manager: %v", err)
	}
}

// TestPeerConnFastPathRtcpUsesOwnedWorker pins configureFastPath's concrete
// RTCP drain launch to the peer worker gate.
func TestPeerConnFastPathRtcpUsesOwnedWorker(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	peerCtx, cancelPeer := context.WithCancel(context.Background())
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.EnableDatagramFastPath = true
	mediaEngine, err := newWebRtcMediaEngine(settings)
	if err != nil {
		t.Fatal(err)
	}
	api := webrtc.NewAPI(webrtc.WithMediaEngine(mediaEngine))
	peer, err := newPeerConn(
		peerCtx,
		peerConnKey{PeerId: NewId(), StreamId: NewId()},
		NewId(),
		false,
		consumingLifecycleSignalSender{},
		settings,
		func() (*webrtc.PeerConnection, context.CancelFunc, error) {
			pc, createErr := api.NewPeerConnection(webrtc.Configuration{})
			return pc, func() {}, createErr
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	workerDoneEntered := make(chan struct{})
	releaseWorkerDone := make(chan struct{})
	peer.beforeWorkerDoneForTest = func(name string) {
		if name == "fast path RTCP drain" {
			close(workerDoneEntered)
			<-releaseWorkerDone
		}
	}
	if err := peer.configureFastPath(); err != nil {
		t.Fatalf("configure fast path: %v", err)
	}
	cancelPeer()
	peer.workers.close()
	_ = closeWebRtcPeerConnection(peer.pc)
	peer.closeFastPath()
	waitCloseWaitBarrier(t, ctx, workerDoneEntered, "owned fast-path RTCP drain")
	select {
	case <-peer.workers.Done():
		t.Fatal("peer completed before RTCP drain terminal hook")
	default:
	}
	close(releaseWorkerDone)
	waitCloseWaitBarrier(t, ctx, peer.workers.Done(), "joined fast-path RTCP drain")
}

// lifecycleParentWebRtcConn is a deterministic message-oriented connection:
// the first read returns the peer ready marker and later reads block on Close.
type lifecycleParentWebRtcConn struct {
	closed            chan struct{}
	closeOnce         sync.Once
	readCount         atomic.Uint32
	steadyReadOnce    sync.Once
	steadyReadMessage chan []byte
	callbackLock      sync.Mutex
	callbacks         map[uint64]func(bool)
	nextCallback      uint64
}

// Read returns the setup marker once, then waits for teardown.
func (self *lifecycleParentWebRtcConn) Read(message []byte) (int, error) {
	if self.readCount.Add(1) == 1 {
		return copy(message, []byte(ReadyHeader)), nil
	}
	self.steadyReadOnce.Do(func() {
		if self.steadyReadMessage != nil {
			self.steadyReadMessage <- message
		}
	})
	<-self.closed
	return 0, io.EOF
}

// Write accepts one whole data-channel message.
func (self *lifecycleParentWebRtcConn) Write(message []byte) (int, error) {
	select {
	case <-self.closed:
		return 0, io.ErrClosedPipe
	default:
		return len(message), nil
	}
}

// Close releases every blocked read.
func (self *lifecycleParentWebRtcConn) Close() error {
	self.closeOnce.Do(func() { close(self.closed) })
	return nil
}

// LocalAddr returns a stable synthetic address.
func (self *lifecycleParentWebRtcConn) LocalAddr() net.Addr { return lifecycleTestAddr("local") }

// RemoteAddr returns a stable synthetic address.
func (self *lifecycleParentWebRtcConn) RemoteAddr() net.Addr { return lifecycleTestAddr("remote") }

// SetDeadline is a no-op for the deterministic connection.
func (self *lifecycleParentWebRtcConn) SetDeadline(time.Time) error { return nil }

// SetReadDeadline is a no-op for the deterministic connection.
func (self *lifecycleParentWebRtcConn) SetReadDeadline(time.Time) error { return nil }

// SetWriteDeadline is a no-op for the deterministic connection.
func (self *lifecycleParentWebRtcConn) SetWriteDeadline(time.Time) error { return nil }

// Connected reports the synthetic association as ready.
func (self *lifecycleParentWebRtcConn) Connected() bool { return true }

// AddConnectedCallback immediately publishes the stable ready state.
func (self *lifecycleParentWebRtcConn) AddConnectedCallback(callback func(bool)) func() {
	self.callbackLock.Lock()
	if self.callbacks == nil {
		self.callbacks = map[uint64]func(bool){}
	}
	self.nextCallback++
	callbackId := self.nextCallback
	self.callbacks[callbackId] = callback
	self.callbackLock.Unlock()
	callback(true)
	return func() {
		self.callbackLock.Lock()
		delete(self.callbacks, callbackId)
		self.callbackLock.Unlock()
	}
}

// triggerConnected snapshots callbacks before invocation, reproducing an
// in-flight callback that unsubscribe cannot retract.
func (self *lifecycleParentWebRtcConn) triggerConnected(connected bool) {
	self.callbackLock.Lock()
	callbacks := make([]func(bool), 0, len(self.callbacks))
	for _, callback := range self.callbacks {
		callbacks = append(callbacks, callback)
	}
	self.callbackLock.Unlock()
	for _, callback := range callbacks {
		callback(connected)
	}
}

// ImmediateReconnect never fires for the synthetic association.
func (self *lifecycleParentWebRtcConn) ImmediateReconnect() <-chan struct{} {
	return make(chan struct{})
}

// lifecycleTestAddr is a stable net.Addr value for synthetic connections.
type lifecycleTestAddr string

// Network identifies the synthetic network.
func (self lifecycleTestAddr) Network() string { return "lifecycle" }

// String identifies one endpoint.
func (self lifecycleTestAddr) String() string { return string(self) }

// TestP2pTransportParentJoinsChildDone proves the parent association cannot
// publish done while either physical route child is held before its done.
func TestP2pTransportParentJoinsChildDone(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	manager := lifecycleTestManager(ctx, t)
	fakeConn := &lifecycleParentWebRtcConn{
		closed:            make(chan struct{}),
		steadyReadMessage: make(chan []byte, 1),
	}
	manager.newP2pConnForTest = func(context.Context, TransferPath, bool) (WebRtcConn, error) {
		return fakeConn, nil
	}
	settings := DefaultP2pTransportSettings()
	childCreated := make(chan struct{}, 2)
	childCleanup := make(chan struct{}, 2)
	releaseChildren := make(chan struct{})
	settings.afterSendTransportForTest = func(child *P2pSendTransport) {
		child.testingAfterProbeSendDrain = func() {
			childCleanup <- struct{}{}
			<-releaseChildren
		}
		childCreated <- struct{}{}
	}
	settings.afterReceiveTransportForTest = func(child *P2pReceiveTransport) {
		child.testingBeforeDoneForTest = func() {
			childCleanup <- struct{}{}
			<-releaseChildren
		}
		childCreated <- struct{}{}
	}
	sendRoutes := NewRouteManager(ctx, "parent-child-send")
	receiveRoutes := NewRouteManager(ctx, "parent-child-receive")
	transport := NewP2pTransport(
		ctx,
		client,
		manager,
		sendRoutes,
		receiveRoutes,
		NewId(),
		NewId(),
		PeerTypeDestination,
		settings,
	)
	for index := 0; index < 2; index++ {
		waitCloseWaitBarrier(t, ctx, childCreated, "P2P route child creation")
	}
	var blockedReadCapture *lifecyclePoolCapture
	select {
	case message := <-fakeConn.steadyReadMessage:
		blockedReadCapture = newLifecyclePoolCapture(message)
	case <-ctx.Done():
		t.Fatalf("capture parent P2P blocked-read owner: %v", ctx.Err())
	}
	defer blockedReadCapture.cleanup()
	blockedReadCapture.requireOwnerLive(t, "parent P2P blocked-read buffer")
	transport.Close()
	joinResult := make(chan error, 1)
	go func() { joinResult <- transport.CloseAndWait(ctx) }()
	for index := 0; index < 2; index++ {
		waitCloseWaitBarrier(t, ctx, childCleanup, "P2P route child cleanup")
	}
	blockedReadCapture.requireOwnerReturned(t, "parent P2P blocked-read buffer")
	requireCloseWaitBlocked(t, joinResult, "exported P2P child join")
	close(releaseChildren)
	waitCloseWaitResult(t, ctx, joinResult, "exported P2P parent child join")
	if err := manager.closeAndWait(ctx); err != nil {
		t.Fatalf("join synthetic WebRTC manager: %v", err)
	}
	if err := client.CloseAndWait(ctx); err != nil {
		t.Fatalf("join parent-child test client: %v", err)
	}
}

// TestP2pTransportSendDrainWaitsForRouteWriterRetirement proves a writer
// admitted to the installed parent route cannot enqueue after the physical
// child has performed its final drain.
func TestP2pTransportSendDrainWaitsForRouteWriterRetirement(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	manager := lifecycleTestManager(ctx, t)
	fakeConn := &lifecycleParentWebRtcConn{closed: make(chan struct{})}
	manager.newP2pConnForTest = func(context.Context, TransferPath, bool) (WebRtcConn, error) {
		return fakeConn, nil
	}
	peerId := NewId()
	streamId := NewId()
	settings := DefaultP2pTransportSettings()
	sendChildCreated := make(chan struct{})
	routeInstalled := make(chan struct{})
	routeRetirementWaitEntered := make(chan struct{})
	routeRetirementWaitFinished := make(chan struct{})
	drainEntered := make(chan struct{})
	releaseDrain := make(chan struct{})
	var routeInstalledOnce sync.Once
	settings.afterSendTransportForTest = func(child *P2pSendTransport) {
		child.testingBeforeRouteRetirementWait = func() {
			close(routeRetirementWaitEntered)
		}
		child.testingAfterRouteRetirementWait = func() {
			close(routeRetirementWaitFinished)
		}
		child.testingAfterProbeSendDrain = func() {
			close(drainEntered)
			<-releaseDrain
		}
		close(sendChildCreated)
	}
	settings.RouteStateObserver = func(state P2pRouteState) {
		if state.Send && state.Connected {
			routeInstalledOnce.Do(func() { close(routeInstalled) })
		}
	}
	sendRoutes := NewRouteManager(ctx, "parent-writer-send")
	receiveRoutes := NewRouteManager(ctx, "parent-writer-receive")
	writer := sendRoutes.OpenMultiRouteWriter(DestinationId(peerId))
	defer sendRoutes.CloseMultiRouteWriter(writer)
	transport := NewP2pTransport(
		ctx,
		client,
		manager,
		sendRoutes,
		receiveRoutes,
		peerId,
		streamId,
		PeerTypeDestination,
		settings,
	)
	waitCloseWaitBarrier(t, ctx, sendChildCreated, "parent send child creation")
	waitCloseWaitBarrier(t, ctx, routeInstalled, "parent send route installation")
	capture := newLifecyclePoolCapture(MessagePoolGet(512))
	defer capture.cleanup()
	snapshotAcquired, removalWaiting, resumeWriter := TestingPauseMultiRouteWriterSnapshot(writer)
	defer resumeWriter()
	writeResult := make(chan error, 1)
	go func() {
		success, err := writer.WriteDetailed(ctx, capture.owner, -1)
		if err == nil && !success {
			err = errors.New("paused P2P writer did not send")
		}
		writeResult <- err
	}()
	waitCloseWaitBarrier(t, ctx, snapshotAcquired, "admitted P2P route writer")
	capture.requireOwnerLive(t, "paused P2P route writer frame")
	transport.Close()
	joinResult := make(chan error, 1)
	go func() { joinResult <- transport.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, removalWaiting, "P2P route writer retirement")
	select {
	case <-routeRetirementWaitEntered:
	case <-drainEntered:
		t.Fatal("P2P send child drained without waiting for route retirement")
	case <-ctx.Done():
		t.Fatalf("wait for P2P child route-retirement wait: %v", ctx.Err())
	}
	resumeWriter()
	select {
	case err := <-writeResult:
		if err != nil {
			t.Fatalf("resume admitted P2P route writer: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for admitted P2P route writer: %v", ctx.Err())
	}
	waitCloseWaitBarrier(
		t,
		ctx,
		routeRetirementWaitFinished,
		"retired P2P child route",
	)
	waitCloseWaitBarrier(t, ctx, drainEntered, "P2P send final drain")
	capture.requireOwnerReturned(t, "paused P2P route writer frame")
	requireCloseWaitBlocked(t, joinResult, "P2P parent final-drain join")
	close(releaseDrain)
	waitCloseWaitResult(t, ctx, joinResult, "P2P route writer retirement join")
	if err := manager.closeAndWait(ctx); err != nil {
		t.Fatalf("join P2P writer test manager: %v", err)
	}
	if err := client.CloseAndWait(ctx); err != nil {
		t.Fatalf("join P2P writer test client: %v", err)
	}
}

// TestP2pTransportJoinsInFlightRouteCallbackBeforeSendDrain holds a connected
// callback after its live-context check. Teardown must join that callback and
// retire the route it mutates before the send child drains a pooled owner.
func TestP2pTransportJoinsInFlightRouteCallbackBeforeSendDrain(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	manager := lifecycleTestManager(ctx, t)
	fakeConn := &lifecycleParentWebRtcConn{closed: make(chan struct{})}
	manager.newP2pConnForTest = func(context.Context, TransferPath, bool) (WebRtcConn, error) {
		return fakeConn, nil
	}
	peerId := NewId()
	streamId := NewId()
	settings := DefaultP2pTransportSettings()
	sendChildCreated := make(chan struct{})
	routeInstalled := make(chan struct{})
	callbackBeforeUpdateEntered := make(chan struct{})
	releaseCallbackBeforeUpdate := make(chan struct{})
	callbackAfterUpdateEntered := make(chan struct{})
	releaseCallbackAfterUpdate := make(chan struct{})
	routeCallbackJoinEntered := make(chan struct{})
	routeRetirementWaitEntered := make(chan struct{})
	drainEntered := make(chan struct{})
	releaseDrain := make(chan struct{})
	var routeInstalledOnce sync.Once
	var callbackJoinOnce sync.Once
	var beforeSendUpdateCount atomic.Uint32
	var afterSendUpdateCount atomic.Uint32
	var releaseBeforeOnce sync.Once
	var releaseAfterOnce sync.Once
	var releaseDrainOnce sync.Once
	defer releaseBeforeOnce.Do(func() { close(releaseCallbackBeforeUpdate) })
	defer releaseAfterOnce.Do(func() { close(releaseCallbackAfterUpdate) })
	defer releaseDrainOnce.Do(func() { close(releaseDrain) })
	settings.afterSendTransportForTest = func(child *P2pSendTransport) {
		child.testingBeforeRouteRetirementWait = func() {
			close(routeRetirementWaitEntered)
		}
		child.testingAfterProbeSendDrain = func() {
			close(drainEntered)
			<-releaseDrain
		}
		close(sendChildCreated)
	}
	settings.RouteStateObserver = func(state P2pRouteState) {
		if state.Send && state.Connected {
			routeInstalledOnce.Do(func() { close(routeInstalled) })
		}
	}
	settings.beforeRouteUpdateForTest = func(send bool) {
		if send && beforeSendUpdateCount.Add(1) == 2 {
			close(callbackBeforeUpdateEntered)
			<-releaseCallbackBeforeUpdate
		}
	}
	settings.afterRouteUpdateForTest = func(send bool) {
		if send && afterSendUpdateCount.Add(1) == 2 {
			close(callbackAfterUpdateEntered)
			<-releaseCallbackAfterUpdate
		}
	}
	settings.beforeRouteCallbackJoinForTest = func(send bool) {
		if send {
			callbackJoinOnce.Do(func() { close(routeCallbackJoinEntered) })
		}
	}
	sendRoutes := NewRouteManager(ctx, "parent-callback-send")
	receiveRoutes := NewRouteManager(ctx, "parent-callback-receive")
	writer := sendRoutes.OpenMultiRouteWriter(DestinationId(peerId))
	defer sendRoutes.CloseMultiRouteWriter(writer)
	transport := NewP2pTransport(
		ctx,
		client,
		manager,
		sendRoutes,
		receiveRoutes,
		peerId,
		streamId,
		PeerTypeDestination,
		settings,
	)
	waitCloseWaitBarrier(t, ctx, sendChildCreated, "callback test send child creation")
	waitCloseWaitBarrier(t, ctx, routeInstalled, "callback test send route installation")
	callbackReturned := make(chan struct{})
	go func() {
		fakeConn.triggerConnected(true)
		close(callbackReturned)
	}()
	waitCloseWaitBarrier(
		t,
		ctx,
		callbackBeforeUpdateEntered,
		"in-flight connected callback",
	)
	transport.Close()
	joinResult := make(chan error, 1)
	go func() { joinResult <- transport.CloseAndWait(ctx) }()
	waitCloseWaitBarrier(t, ctx, routeCallbackJoinEntered, "connected callback join")
	waitCloseWaitBarrier(
		t,
		ctx,
		routeRetirementWaitEntered,
		"callback test route-retirement wait",
	)
	requireCloseWaitBlocked(t, joinResult, "in-flight P2P route callback join")
	releaseBeforeOnce.Do(func() { close(releaseCallbackBeforeUpdate) })
	waitCloseWaitBarrier(
		t,
		ctx,
		callbackAfterUpdateEntered,
		"canceled callback route update",
	)
	capture := newLifecyclePoolCapture(MessagePoolGet(512))
	defer capture.cleanup()
	success, err := writer.WriteDetailed(ctx, capture.owner, -1)
	if err != nil {
		t.Fatalf("write through canceled callback route: %v", err)
	}
	if !success {
		t.Fatal("canceled callback route did not accept the pooled frame")
	}
	capture.requireOwnerLive(t, "canceled callback route frame")
	releaseAfterOnce.Do(func() { close(releaseCallbackAfterUpdate) })
	waitCloseWaitBarrier(t, ctx, callbackReturned, "connected callback return")
	waitCloseWaitBarrier(t, ctx, drainEntered, "callback test send final drain")
	capture.requireOwnerReturned(t, "canceled callback route frame")
	requireCloseWaitBlocked(t, joinResult, "callback test final-drain join")
	releaseDrainOnce.Do(func() { close(releaseDrain) })
	waitCloseWaitResult(t, ctx, joinResult, "in-flight P2P route callback join")
	if err := manager.closeAndWait(ctx); err != nil {
		t.Fatalf("join P2P callback test manager: %v", err)
	}
	if err := client.CloseAndWait(ctx); err != nil {
		t.Fatalf("join P2P callback test client: %v", err)
	}
}

// lifecycleFastReceiveConn adds the optional datagram receive capability to a
// blocking legacy connection.
type lifecycleFastReceiveConn struct {
	*lifecycleReadBarrierConn
	messages chan p2pFastPathReceivedMessage
}

// FastPathReady reports the synthetic carrier as negotiated.
func (self *lifecycleFastReceiveConn) FastPathReady() bool { return true }

// WaitFastPathReady reports immediate readiness.
func (self *lifecycleFastReceiveConn) WaitFastPathReady(context.Context, time.Duration) bool {
	return true
}

// WriteFastPathMessage accepts a synthetic message.
func (self *lifecycleFastReceiveConn) WriteFastPathMessage([]byte) (int, error) {
	return 1, nil
}

// FastPathMessages returns the owned-message handoff.
func (self *lifecycleFastReceiveConn) FastPathMessages() <-chan p2pFastPathReceivedMessage {
	return self.messages
}

// TestP2pReceiveTransportJoinsFastWorker proves done and final pool ownership
// wait for the optional datagram child, not only the legacy reader.
func TestP2pReceiveTransportJoinsFastWorker(t *testing.T) {
	ctx, cancelWait := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelWait()
	transportCtx, cancelTransport := context.WithCancel(context.Background())
	localPipe, remotePipe := net.Pipe()
	defer remotePipe.Close()
	conn := &lifecycleFastReceiveConn{
		lifecycleReadBarrierConn: &lifecycleReadBarrierConn{
			Conn:        localPipe,
			readEntered: make(chan struct{}),
		},
		messages: make(chan p2pFastPathReceivedMessage, 1),
	}
	handlerEntered := make(chan struct{})
	releaseHandler := make(chan struct{})
	transportValue, _ := newP2pReceiveTransport(
		transportCtx,
		cancelTransport,
		conn,
		NewId(),
		DefaultP2pTransportSettings(),
		nil,
		func([]byte) bool {
			close(handlerEntered)
			<-releaseHandler
			return true
		},
	)
	transport := transportValue.(*P2pReceiveTransport)
	waitCloseWaitBarrier(t, ctx, conn.readEntered, "fast-worker legacy read")
	capture := newLifecyclePoolCapture(MessagePoolGet(512))
	defer capture.cleanup()
	conn.messages <- p2pFastPathReceivedMessage{message: capture.owner, fragmentCount: 1}
	waitCloseWaitBarrier(t, ctx, handlerEntered, "fast-worker message handler")
	capture.requireOwnerLive(t, "fast-worker message")
	cancelTransport()
	_ = localPipe.Close()
	select {
	case <-transport.done:
		t.Fatal("receive transport completed before fast worker returned")
	default:
	}
	close(releaseHandler)
	waitCloseWaitBarrier(t, ctx, transport.done, "receive fast-worker join")
	capture.requireOwnerReturned(t, "fast-worker message")
}
