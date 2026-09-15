// WebRTC peer transports keep signaling, admission, and data-plane lifecycle
// bounded while a client changes between exchange and direct routes.
package connect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pion/datachannel"
	"github.com/pion/logging"
	"github.com/pion/transport/v4"
	"github.com/pion/webrtc/v4"

	"github.com/urnetwork/connect/v2026/protocol"
)

type WebRtcConn interface {
	net.Conn
	Connected() bool
	AddConnectedCallback(func(connected bool)) func()
	// closed when the conn cancels for a reason where the outer transport
	// should reconnect without backoff (e.g., remote requested fresh
	// negotiation or host network change). The channel is persistent and
	// one-shot; implementations without this signal return a never-closed
	// channel.
	ImmediateReconnect() <-chan struct{}
}

// SignalSender consumes signal.MessageBytes whether delivery succeeds or fails.
type SignalSender interface {
	SendSignal(destinationId Id, signal *protocol.Frame, opts ...any)
}

// SignalReceiver borrows signal.MessageBytes for the duration of the call.
type SignalReceiver interface {
	ReceiveSignal(source TransferPath, transferKey TransferKey, signal *protocol.Frame) error
}

// exchangeSignalReceiver is the owned-value fast path for signal dispatch.
// The Client receive callback's frame bytes are callback-scoped, so the
// asynchronous signal queue decodes them once into an owning protobuf value
// before returning. Implementations of only SignalReceiver retain the framed
// compatibility path.
type exchangeSignalReceiver interface {
	ReceiveExchangeSignals(
		source TransferPath,
		transferKey TransferKey,
		signals *protocol.ExchangeSignals,
	) error
}

// Partitions callback-scoped signals by peer and stream while preserving each
// stream's delivery order. Its methods are safe for concurrent use.
type clientSignalDispatcher struct {
	client               *Client
	receiver             SignalReceiver
	ctx                  context.Context
	cancel               context.CancelFunc
	closeOnce            sync.Once
	shards               []*clientSignalReceiver
	contextCloseLock     sync.Mutex
	contextCloseStop     func() bool
	contextCloseDone     chan struct{}
	contextCloseDoneOnce sync.Once
	// Nil test barrier exposes dispatcher join entry.
	beforeCloseWaitForTest func()
	// Nil test barrier pauses the context callback before Close.
	beforeContextCloseForTest func()
}

// signalNetworkPeerObserver is implemented by WebRtcManager. The transfer
// receive callback has authenticated the Network relationship, but repeated
// WebRTC signaling is negotiation machinery rather than fresh application
// demand. Only the first signal grants an admission lease; later signals keep
// trust/window classification without indefinitely refreshing stale peers.
type signalNetworkPeerObserver interface {
	ObserveNetworkPeerSignal(peerId Id)
}

func prioritizeNetworkSignalPeer(
	receiver SignalReceiver,
	source TransferPath,
	frames []*protocol.Frame,
	peer Peer,
) {
	if peer.ProvideMode != protocol.ProvideMode_Network || source.SourceId == (Id{}) {
		return
	}
	for _, frame := range frames {
		if frame != nil && frame.MessageType == protocol.MessageType_TransferExchangeSignals {
			if observer, ok := receiver.(signalNetworkPeerObserver); ok {
				observer.ObserveNetworkPeerSignal(source.SourceId)
			}
			return
		}
	}
}

// Adapts Client transfer delivery to the signaling ownership contract.
type ClientSignalSender struct {
	client *Client
}

// Creates a sender whose lifetime is owned by the supplied client.
func NewClientSignalSender(client *Client) *ClientSignalSender {
	return &ClientSignalSender{
		client: client,
	}
}

// signalSendNonBlocking marks a signal send that originates on a receive
// path (a peer's inbound signal producing a response). Per the receive
// contract (CODESTYLE: receive callbacks must not block), such sends use
// timeout 0 — enqueue if there is room, drop otherwise — instead of the
// sender-context default of blocking for backpressure. A dropped response is
// recovered by the signaling retry machinery (offer replay on
// WaitingForSdpOffer, candidate re-flush, transport reconnect), while a
// blocked receive path can wedge signal delivery for every peer.
type signalSendNonBlocking struct{}

// Reduces the detailed send result to a bounded diagnostic vocabulary. The
// legacy transfer queues use both punctuated and unpunctuated Done errors for
// context, client, sequence, and capability closure; keep those causes grouped
// until their owning layer exposes a stronger typed result.
func signalSendFailureReason(err error) string {
	if err == nil {
		return "not-admitted"
	}
	if errors.Is(err, ErrEncryptionRequiredNotEstablished) {
		return "encryption-not-ready"
	}
	if err.Error() == "Done" || err.Error() == "Done." {
		return "canceled-or-closed"
	}
	return "other"
}

// Uses normal sender backpressure unless a receive-originated reply explicitly
// requests a nonblocking handoff. The supplied frame is consumed in all cases.
func (self *ClientSignalSender) SendSignal(destinationId Id, signal *protocol.Frame, opts ...any) {
	timeout := time.Duration(-1)
	mode := "sender"
	sendOpts := make([]any, 0, len(opts))
	for _, opt := range opts {
		if _, ok := opt.(signalSendNonBlocking); ok {
			timeout = 0
			mode = "receive-reply"
			continue
		}
		sendOpts = append(sendOpts, opt)
	}
	success, err := self.client.SendWithTimeoutDetailed(signal, destinationId, nil, timeout, sendOpts...)
	// A failed signal delays p2p setup until transport retry, so keep it loud.
	// Preserve SendWithTimeout's success-and-no-error contract exactly. The
	// V(1) positive is the send-side half of the signal delivery trace.
	if !success || err != nil {
		MessagePoolReturn(signal.MessageBytes)
		signal.MessageBytes = nil
		self.client.log.Infof(
			"[signal]send failed mode=%s reason=%s\n",
			mode,
			signalSendFailureReason(err),
		)
	} else if self.client.log.V(1).Enabled() {
		self.client.log.Infof("[signal]send mode=%s\n", mode)
	}
}

// Owns one bounded dispatch shard. Enqueue never blocks, while its worker
// preserves serial delivery for every peer and stream mapped to the shard.
type clientSignalReceiver struct {
	client             *Client
	receiver           SignalReceiver
	ctx                context.Context
	cancel             context.CancelFunc
	queueLock          sync.Mutex
	closed             bool
	queueLimit         int
	receiveFrames      []*receivedSignalFrame
	receiveFrameHead   int
	receiveFrameCount  int
	queueMonitor       *Monitor
	droppedSignalCount atomic.Uint64
	dropWarnings       chan signalDropWarning
	runOnce            sync.Once
	workers            *lifecycleAdmission

	// Tests receive the final-return result for each exact compatibility frame.
	// A nil channel makes this integration point a production no-op.
	frameClosedForTest chan<- bool
}

// signalDropWarning identifies one bounded-shard drop without retaining a frame.
type signalDropWarning struct {
	sourceId           Id
	streamId           Id
	droppedSignalCount uint64
}

// Owns a decoded signal value or a pooled copy after the Client callback ends.
type receivedSignalFrame struct {
	source          TransferPath
	transferKey     TransferKey
	frame           *protocol.Frame
	exchangeSignals *protocol.ExchangeSignals
	candidateBatch  bool
	candidateCount  int
	candidateBytes  int
	keyValid        bool
	key             receivedSignalFrameKey

	// Tests use a buffered channel to observe this exact frame's final pooled
	// return without reading process-global pool counters.
	closedForTest chan<- bool
}

// Identifies the peer and stream whose signaling order must be preserved.
type receivedSignalFrameKey struct {
	source   TransferPath
	streamId Id
}

// Installs a bounded asynchronous signaling bridge and returns its unsubscribe
// function. Sharding permits independent negotiations to advance concurrently.
func ReceiveSignalsFromClient(client *Client, receiver SignalReceiver) func() {
	_, unsub := receiveSignalsFromClient(client, receiver)
	return unsub
}

// Builds the signaling bridge and also returns its concrete lifecycle owner to
// Client, while preserving the exported compatibility function above.
func receiveSignalsFromClient(
	client *Client,
	receiver SignalReceiver,
) (*clientSignalDispatcher, func()) {
	cancelCtx, cancel := context.WithCancel(client.Ctx())
	bufferSize := defaultTransferBufferSize
	if client.settings != nil && client.settings.ReceiveBufferSettings != nil {
		bufferSize = client.settings.ReceiveBufferSettings.SequenceBufferSize
	}
	bufferSize = max(1, bufferSize)
	workerCount := 4
	if client.settings != nil && client.settings.WebRtcSettings != nil &&
		0 < client.settings.WebRtcSettings.SignalReceiveWorkerCount {
		workerCount = client.settings.WebRtcSettings.SignalReceiveWorkerCount
	}
	workerCount = min(bufferSize, max(1, workerCount))
	dispatcher := &clientSignalDispatcher{
		client:           client,
		receiver:         receiver,
		ctx:              cancelCtx,
		cancel:           cancel,
		shards:           make([]*clientSignalReceiver, 0, workerCount),
		contextCloseDone: make(chan struct{}),
	}
	for workerIndex := range workerCount {
		queueLimit := bufferSize / workerCount
		if workerIndex < bufferSize%workerCount {
			queueLimit++
		}
		shard := &clientSignalReceiver{
			client:       client,
			receiver:     receiver,
			ctx:          cancelCtx,
			cancel:       cancel,
			queueLimit:   queueLimit,
			queueMonitor: NewMonitor(),
			dropWarnings: make(chan signalDropWarning, 1),
			workers:      newLifecycleAdmission(),
		}
		dispatcher.shards = append(dispatcher.shards, shard)
	}
	stopContextClose := context.AfterFunc(cancelCtx, func() {
		if dispatcher.beforeContextCloseForTest != nil {
			dispatcher.beforeContextCloseForTest()
		}
		dispatcher.Close()
		dispatcher.contextCloseDoneOnce.Do(func() {
			close(dispatcher.contextCloseDone)
		})
	})
	dispatcher.contextCloseLock.Lock()
	dispatcher.contextCloseStop = stopContextClose
	dispatcher.contextCloseLock.Unlock()
	unsub := client.AddReceiveCallback(dispatcher.Receive)
	return dispatcher, func() {
		unsub()
		dispatcher.Close()
	}
}

// ReceiveFunction. Frames for one peer/stream hash to one worker and stay
// ordered; independent peers can negotiate in parallel. The sum of shard
// capacities remains the original receive queue limit. A full shard drops and
// counts the signal instead of blocking the shared Client receive callback.
func (self *clientSignalDispatcher) Receive(source TransferPath, frames []*protocol.Frame, peer Peer) {
	prioritizeNetworkSignalPeer(self.receiver, source, frames, peer)
	for _, frame := range frames {
		self.handleControlFrame(source, peer.TransferKey, frame)
	}
}

// handleControlFrame hands one borrowed signal to its stable bounded shard.
func (self *clientSignalDispatcher) handleControlFrame(
	source TransferPath,
	transferKey TransferKey,
	frame *protocol.Frame,
) {
	switch frame.MessageType {
	case protocol.MessageType_TransferExchangeSignals:
		select {
		case <-self.ctx.Done():
			return
		default:
		}

		if self.client.log.V(1).Enabled() {
			self.client.log.Infof("[signal]receive from %s\n", source)
		}
		received, err := newReceivedSignalFrame(source, transferKey, frame)
		if err != nil {
			self.client.log.Infof("[signal]receive frame err=%s\n", err)
			return
		}
		shardIndex := receivedSignalShardIndex(received, len(self.shards))
		shard := self.shards[shardIndex]
		shard.start()
		if !shard.enqueue(received) {
			received.Close()
		}
	}
}

// Stops every shard and releases all queued frame ownership.
func (self *clientSignalDispatcher) Close() {
	self.closeOnce.Do(func() {
		self.contextCloseLock.Lock()
		stopContextClose := self.contextCloseStop
		self.contextCloseStop = nil
		self.contextCloseLock.Unlock()
		if stopContextClose != nil && stopContextClose() {
			self.contextCloseDoneOnce.Do(func() {
				close(self.contextCloseDone)
			})
		}
		self.cancel()
		for _, shard := range self.shards {
			shard.Close()
		}
	})
}

// closeAndWait joins delivery and diagnostic workers after queued signal
// ownership has been released, or returns when ctx expires.
func (self *clientSignalDispatcher) closeAndWait(ctx context.Context) error {
	self.Close()
	if self.beforeCloseWaitForTest != nil {
		self.beforeCloseWaitForTest()
	}
	var result error
	for shardIndex, shard := range self.shards {
		if err := shard.closeAndWait(ctx); err != nil {
			result = errors.Join(
				result,
				fmt.Errorf("signal shard %d: %w", shardIndex, err),
			)
		}
	}
	if err := waitForLifecycleDone(
		ctx,
		self.contextCloseDone,
		"signal context cleanup",
	); err != nil {
		result = errors.Join(result, err)
	}
	return result
}

// ReceiveFunction
func (self *clientSignalReceiver) Receive(source TransferPath, frames []*protocol.Frame, peer Peer) {
	prioritizeNetworkSignalPeer(self.receiver, source, frames, peer)
	for _, frame := range frames {
		self.handleControlFrame(source, peer.TransferKey, frame)
	}
}

// handleControlFrame hands one borrowed signal to this receiver's bounded queue.
func (self *clientSignalReceiver) handleControlFrame(
	source TransferPath,
	transferKey TransferKey,
	frame *protocol.Frame,
) {
	switch frame.MessageType {
	case protocol.MessageType_TransferExchangeSignals:
		select {
		case <-self.ctx.Done():
			return
		default:
		}

		// receive-side half of the signal delivery trace (send side:
		// [signal]send)
		if self.client.log.V(1).Enabled() {
			self.client.log.Infof("[signal]receive from %s\n", source)
		}

		received, err := newReceivedSignalFrame(source, transferKey, frame)
		if err != nil {
			self.client.log.Infof("[signal]receive frame err=%s\n", err)
			return
		}
		self.start()
		if !self.enqueue(received) {
			received.Close()
		}
	}
}

// Starts this shard's delivery and diagnostic workers at most once.
func (self *clientSignalReceiver) start() {
	self.runOnce.Do(func() {
		self.queueLock.Lock()
		if self.workers == nil {
			self.workers = newLifecycleAdmission()
		}
		workers := self.workers
		closed := self.closed
		self.queueLock.Unlock()
		if closed {
			workers.close()
		}
		cancel := func() {
			self.cancel()
		}
		startWorker := func(run func()) {
			if !workers.start() {
				return
			}
			go func() {
				defer workers.finish()
				HandleError(run, cancel)
			}()
		}
		startWorker(self.run)
		startWorker(self.runDropWarnings)
	})
}

// Detaches the bounded queue atomically, then releases its frames outside the
// queue lock so teardown cannot stall an enqueue critical section.
func (self *clientSignalReceiver) Close() {
	var receiveFrames []*receivedSignalFrame
	var receiveFrameHead int
	var receiveFrameCount int
	self.queueLock.Lock()
	if self.workers == nil {
		self.workers = newLifecycleAdmission()
	}
	workers := self.workers
	if !self.closed {
		self.closed = true
		receiveFrames = self.receiveFrames
		receiveFrameHead = self.receiveFrameHead
		receiveFrameCount = self.receiveFrameCount
		self.receiveFrames = nil
		self.receiveFrameHead = 0
		self.receiveFrameCount = 0
		self.queueMonitor.NotifyAll()
	}
	self.queueLock.Unlock()
	for i := range receiveFrameCount {
		index := (receiveFrameHead + i) % len(receiveFrames)
		if received := receiveFrames[index]; received != nil {
			received.Close()
		}
	}
	self.cancel()
	workers.close()
}

// closeAndWait joins this shard's delivery and diagnostic workers.
func (self *clientSignalReceiver) closeAndWait(ctx context.Context) error {
	self.Close()
	return waitForLifecycleDone(ctx, self.workers.Done(), "signal receiver workers")
}

// runDropWarnings emits bounded diagnostics outside the shared receive callback.
func (self *clientSignalReceiver) runDropWarnings() {
	for {
		select {
		case <-self.ctx.Done():
			return
		case warning := <-self.dropWarnings:
			self.client.log.Warningf(
				"[signal]receive drop full source=%s stream=%s dropped=%d\n",
				warning.sourceId,
				warning.streamId,
				warning.droppedSignalCount,
			)
		}
	}
}

// Drains owned signals serially until cancellation or queue closure.
func (self *clientSignalReceiver) run() {
	for {
		received := self.dequeue()
		if received == nil {
			return
		}
		func() {
			defer received.Close()
			if receiver, ok := self.receiver.(exchangeSignalReceiver); ok && received.exchangeSignals != nil {
				if err := receiver.ReceiveExchangeSignals(
					received.source,
					received.transferKey,
					received.exchangeSignals,
				); err != nil {
					self.client.log.Infof("[signal]receive err=%s\n", err)
				}
				return
			}
			if err := received.prepareFrame(); err != nil {
				self.client.log.Infof("[signal]receive frame err=%s\n", err)
				return
			}
			if err := self.receiver.ReceiveSignal(
				received.source,
				received.transferKey,
				received.frame,
			); err != nil {
				self.client.log.Infof("[signal]receive err=%s\n", err)
			}
		}()
	}
}

// newReceivedSignalFrame creates an owned value for asynchronous dispatch.
func newReceivedSignalFrame(
	source TransferPath,
	transferKey TransferKey,
	frame *protocol.Frame,
) (*receivedSignalFrame, error) {
	exchangeSignals := &protocol.ExchangeSignals{}
	if err := ProtoUnmarshal(frame.MessageBytes, exchangeSignals); err != nil {
		return &receivedSignalFrame{
			source:      source,
			transferKey: transferKey,
			frame: &protocol.Frame{
				MessageType:  frame.MessageType,
				Raw:          frame.Raw,
				MessageBytes: MessagePoolCopy(frame.MessageBytes),
			},
		}, nil
	}

	candidateBatch := false
	var key receivedSignalFrameKey
	keyValid := false
	if streamId, err := IdFromBytes(exchangeSignals.StreamId); err == nil {
		keyValid = true
		key = receivedSignalFrameKey{
			source:   source,
			streamId: streamId,
		}
		if isCandidateBatch(exchangeSignals) {
			candidateBatch = true
		}
	}
	received := &receivedSignalFrame{
		source:      source,
		transferKey: transferKey,
		frame: &protocol.Frame{
			MessageType: frame.MessageType,
			Raw:         frame.Raw,
		},
		exchangeSignals: exchangeSignals,
		keyValid:        keyValid,
		key:             key,
	}
	if candidateBatch {
		received.candidateBatch = true
		for _, signal := range exchangeSignals.Signals {
			received.candidateCount++
			received.candidateBytes += len(signal.IceCandidate)
		}
	}
	return received, nil
}

// Maps one valid peer/stream ordering key to a stable dispatch shard.
func receivedSignalShardIndex(received *receivedSignalFrame, shardCount int) int {
	if shardCount <= 1 || received == nil || !received.keyValid {
		return 0
	}
	// Allocation-free FNV-1a over the stable ordering key. SourceId +
	// streamId is sufficient: WebRtcManager uses exactly this pair to find a
	// peer connection.
	hash := uint32(2166136261)
	for _, b := range received.key.source.SourceId {
		hash = (hash ^ uint32(b)) * 16777619
	}
	for _, b := range received.key.streamId {
		hash = (hash ^ uint32(b)) * 16777619
	}
	return int(hash % uint32(shardCount))
}

// Releases any pooled frame bytes and invalidates the owned decoded value.
func (self *receivedSignalFrame) Close() {
	if self.frame != nil {
		if self.frame.MessageBytes != nil {
			returned := MessagePoolReturn(self.frame.MessageBytes)
			if self.closedForTest != nil {
				select {
				case self.closedForTest <- returned:
				default:
				}
			}
		}
		self.frame = nil
	}
	self.exchangeSignals = nil
}

// Lazily recreates the compatibility frame only for receivers that need bytes.
func (self *receivedSignalFrame) prepareFrame() error {
	if self.frame == nil || self.frame.MessageBytes != nil || self.exchangeSignals == nil {
		return nil
	}
	messageBytes, err := ProtoMarshal(self.exchangeSignals)
	if err != nil {
		return err
	}
	self.frame.MessageBytes = messageBytes
	return nil
}

// Reports whether the decoded value contains only coalescible ICE candidates.
func (self *receivedSignalFrame) isCandidateBatch() bool {
	return isCandidateBatch(self.exchangeSignals)
}

// Accepts only non-reset batches containing at least one ICE candidate.
func isCandidateBatch(exchangeSignals *protocol.ExchangeSignals) bool {
	if exchangeSignals == nil || exchangeSignals.ResetSignals || len(exchangeSignals.Signals) == 0 {
		return false
	}
	for _, signal := range exchangeSignals.Signals {
		if signal == nil || signal.SignalType != protocol.SignalType_IceCandidate {
			return false
		}
	}
	return true
}

// Coalesces an adjacent candidate batch without exceeding the receiver's
// bounded candidate count or byte budget.
func (self *receivedSignalFrame) appendCandidateBatch(received *receivedSignalFrame) bool {
	if !self.candidateBatch || !received.candidateBatch || self.key != received.key ||
		self.transferKey != received.transferKey {
		return false
	}
	// Coalescing is an allocation optimization, not permission to bypass the
	// bounded queue. Once a batch reaches the same count/byte ceiling as the
	// pre-SDP candidate buffer, leave the next frame as a queue entry; a full
	// shard drops the next signal without stalling unrelated receive work.
	if maxBufferedRemoteIceCandidateCount < self.candidateCount+received.candidateCount ||
		maxBufferedRemoteIceCandidateBytes < self.candidateBytes+received.candidateBytes {
		return false
	}
	self.exchangeSignals.Signals = append(self.exchangeSignals.Signals, received.exchangeSignals.Signals...)
	self.candidateCount += received.candidateCount
	self.candidateBytes += received.candidateBytes
	return true
}

// Takes ownership when it can append or coalesce the frame. A full or closed
// shard rejects the frame immediately so the receive callback never blocks.
func (self *clientSignalReceiver) enqueue(received *receivedSignalFrame) bool {
	received.closedForTest = self.frameClosedForTest
	self.queueLock.Lock()
	if self.closed || self.ctx.Err() != nil {
		self.queueLock.Unlock()
		return false
	}
	if received.candidateBatch {
		if batch := self.tailLocked(); batch != nil && batch.candidateBatch && batch.key == received.key {
			if batch.appendCandidateBatch(received) {
				self.queueLock.Unlock()
				received.Close()
				return true
			}
		}
	}
	if self.queueLimit <= self.queueLenLocked() {
		droppedSignalCount := self.droppedSignalCount.Add(1)
		self.queueLock.Unlock()
		select {
		case self.dropWarnings <- signalDropWarning{
			sourceId:           received.source.SourceId,
			streamId:           received.key.streamId,
			droppedSignalCount: droppedSignalCount,
		}:
		default:
		}
		return false
	}
	if self.receiveFrames == nil {
		// A fixed-capacity ring makes the queue's backing storage obey
		// the same bound as its live item count. The former
		// append-plus-head scheme retained an ever-growing nil prefix
		// whenever sustained traffic kept the queue from becoming
		// completely empty.
		self.receiveFrames = make([]*receivedSignalFrame, self.queueLimit)
	}
	tail := (self.receiveFrameHead + self.receiveFrameCount) % len(self.receiveFrames)
	self.receiveFrames[tail] = received
	self.receiveFrameCount++
	self.queueMonitor.NotifyAll()
	self.queueLock.Unlock()
	return true
}

// Reports the live ring size while queueLock is held.
func (self *clientSignalReceiver) queueLenLocked() int {
	return self.receiveFrameCount
}

// Returns the newest queued frame without removing it while queueLock is held.
func (self *clientSignalReceiver) tailLocked() *receivedSignalFrame {
	if self.receiveFrameCount == 0 {
		return nil
	}
	index := (self.receiveFrameHead + self.receiveFrameCount - 1) % len(self.receiveFrames)
	return self.receiveFrames[index]
}

// Waits for and transfers ownership of the oldest frame, or returns nil after
// cancellation or closure.
func (self *clientSignalReceiver) dequeue() *receivedSignalFrame {
	for {
		var queueNotify chan struct{}
		self.queueLock.Lock()
		if 0 < self.receiveFrameCount {
			received := self.receiveFrames[self.receiveFrameHead]
			self.receiveFrames[self.receiveFrameHead] = nil
			self.receiveFrameCount--
			if self.receiveFrameCount == 0 {
				self.receiveFrameHead = 0
			} else {
				self.receiveFrameHead = (self.receiveFrameHead + 1) % len(self.receiveFrames)
			}
			self.queueLock.Unlock()
			return received
		}
		if self.closed || self.ctx.Err() != nil {
			self.queueLock.Unlock()
			return nil
		}
		queueNotify = self.queueMonitor.NotifyChannel()
		self.queueLock.Unlock()

		select {
		case <-self.ctx.Done():
			return nil
		case <-queueNotify:
		}
	}
}

func DefaultWebRtcSettings() *WebRtcSettings {
	return &WebRtcSettings{
		// FIXME
		// SendBufferSize: mib(1),

		// sctp receive buffer per peer connection, so scaled by the memory
		// budget. sized so a handful of peer connections plus the transfer
		// queues coexist within the client memory target — each new
		// connection reserves this amount from the shared client budget
		// (see MemoryBudget)
		ReceiveBufferSize: MemoryScaledByteCount(kib(512), kib(256)),
		// Pion's receive MTU is a per-packet demux buffer, not the SCTP
		// receive window. Its SCTP path MTU stays below Ethernet's 1500-byte
		// packet size, so the former 4 KiB value only enlarged persistent and
		// scratch buffers.
		ReceiveMtu: 1500,
		// Match P2pTransportSettings.MaxMessageByteCount. Pion otherwise
		// advertises a ~1 GiB SCTP message and permits the association to
		// reassemble far beyond the transport's bounded read buffer.
		MaxMessageSize: 64 * 1024,
		// each peer connection holds the pion ice/dtls/sctp stack plus up to
		// `ReceiveBufferSize` of queued data, so the count is bounded (scaled
		// by the memory budget). At the cap new p2p setups are refused and
		// those streams stay on the platform transport; the p2p transport
		// retries with backoff, so capacity recovers as connections close.
		MaxPeerConnectionCount: MemoryScaledCount(32, 8),
		DisconnectedTimeout:    30 * time.Second,
		FailedTimeout:          30 * time.Second,
		// Pion's two-second default halves idle STUN/radio wakeups versus the
		// former one-second override without changing the 30-second
		// disconnect/failure thresholds.
		KeepAliveTimeout: 2 * time.Second,
		// Host candidates are available immediately. Bound the lifetime of
		// dead STUN socket/gather goroutines instead of accepting Pion's
		// five-second default for every URL and address family.
		StunGatherTimeout: 2 * time.Second,

		DataChannelLabel: "data",
		// TransferFrames carry their own sequence numbers and already reorder
		// across concurrent routes. Keeping SCTP reliable but unordered lets a
		// later frame reach that recovery layer when an earlier SCTP fragment
		// is lost, instead of adding a second head-of-line barrier. A
		// deterministic one-datagram-loss measurement reduced the next-frame
		// tail from about 110 ms to below 1.4 ms.
		DataChannelOrdered:       false,
		SignalReceiveWorkerCount: 4,
		// SNAP exchanges SCTP INIT state in the already-required SDP,
		// removing the cookie handshake when both native peers support it.
		// The attribute is negotiated; mixed/older peers fall back to the
		// standard association handshake.
		EnableSctpSnap: true,
		// SCTP runs inside authenticated DTLS for WebRTC. RFC 9653 negotiates
		// whether that stronger integrity check may replace SCTP's redundant
		// CRC32c; a peer that does not advertise support continues to receive
		// and send normal checksums. This removes one checksum on each DATA/
		// SACK path when both directions support it.
		EnableSctpZeroChecksum: true,
		// Native peers negotiate a custom RTP codec carried by the existing
		// ICE/DTLS/SRTP association. Transfer frames sent on that lane avoid
		// SCTP's duplicate reliability and head-of-line behavior; an older peer
		// simply omits the codec and continues on the data channel.
		EnableDatagramFastPath: true,
		// At one gigabit, a 64-message queue covers only about 1.5 ms of
		// ordinary two-packet Transfer messages. That made a normal scheduler
		// turn or short GC pause look like network loss on the device receive
		// side. Keep roughly 24 ms of burst absorption; the queue stores pooled
		// messages only while the route worker is actually behind.
		DatagramFastPathReceiveBufferSize: 1024,
		UdpSocketBufferByteCount:          int(mib(4)),
		DatagramFastPathWriteQueueSize:    256,
		DatagramFastPathWriteBatchSize:    64,
		// Pion's Reno-style congestion avoidance otherwise adds one ~1.2 KiB
		// MTU only after a complete cwnd is acknowledged. On a measured 50 ms
		// path with independent wireless loss, recovery from the observed
		// 20-50 KiB window took seconds and held throughput below 1 MiB/s.
		// A steeper step retains loss response and a zero minimum (unlike a
		// forced floor, which trades a standing queue / bufferbloat latency for
		// throughput), while making recovery competitive with modern
		// transports. Three deterministic repetitions
		// (TestWebRtcSctpCwndCAStepKnee, see OPTIMIZENETWORKPEER1.md) select
		// 8 MTU as the predictable knee: versus 4 MTU it raised median goodput
		// 16% at 1% independent loss and 81% on a 50 Mbps shallow-queue path,
		// without regressing 8 Mbps. 16 MTU gained another 21% only in the
		// independent-loss row but collapsed to one MTU in one of three
		// 50 Mbps runs and had a slower 8 Mbps run. All WebRTC p2p is a direct
		// ICE path, never the cellular relay.
		SctpCwndCAStep: 8 * 1200,
		// ICE consent can remain healthy while the SCTP/data plane is
		// half-open. Once a write leaves unacknowledged SCTP bytes, require a
		// forward acknowledgement within this bound or rebuild the
		// association. A peer-advertised zero receive window pauses the bound:
		// that is intentional receiver backpressure, not a dead path. The
		// worker is lazy and has no idle timer/radio wakeups.
		SctpNoProgressTimeout: 10 * time.Second,
		// openrelay.metered.ca and stun.stunprotocol.org are defunct — every
		// gather against them burned a multi-second i/o timeout per attempt
		// (observed on-device 2026-07-25) and delayed candidate gathering.
		// Keep a small set of live anycast servers.
		IceServerUrls: []string{
			"stun:stun.cloudflare.com:3478",
			"stun:stun.l.google.com:19302",
		},
	}
}

type WebRtcSettings struct {
	// Log, when set, is used by the webrtc manager, p2p transports, and the
	// pion stack. nil resolves to `DefaultLogger()`.
	// `NewClientWithTag` propagates the client log here when nil.
	Log Logger

	ReceiveBufferSize ByteCount
	ReceiveMtu        ByteCount
	MaxMessageSize    ByteCount
	// the maximum number of live peer connections across all peers and
	// streams. 0 is no limit.
	MaxPeerConnectionCount int
	// MemoryBudget, when set, is the shared client transfer budget: each new
	// peer connection reserves `ReceiveBufferSize` from it at creation and
	// releases it at teardown, so p2p setups are admitted only while the
	// client memory target has headroom (refused setups stay on the platform
	// transport, as at the count cap). nil disables byte admission.
	MemoryBudget *TransferMemoryBudget
	// NetworkPeerReceiveBufferSize, when > 0, is the SCTP receive window used
	// for a trusted network peer (an explicitly selected / ProvideMode_Network
	// peer, marked via PrioritizePeer) instead of ReceiveBufferSize. It lets the
	// serving side match the client's larger selected-peer window so
	// device-to-device is symmetric, without multiplying that footprint across
	// the many public peers. Requires NetworkPeerMemoryBudget; without it, a
	// network peer falls back to the public window (Fix 1).
	NetworkPeerReceiveBufferSize ByteCount
	// NetworkPeerMemoryBudget is the dedicated admission budget for network-peer
	// connections (each reserves NetworkPeerReceiveBufferSize). Separate from
	// MemoryBudget so a bounded number of large network-peer windows never
	// starve — or is starved by — the many small public windows. nil disables
	// the network-peer window (falls back to the public window/budget).
	NetworkPeerMemoryBudget *TransferMemoryBudget
	// InitialNetworkPeerIds marks explicitly selected, already-authenticated
	// Network destinations before Client construction can start its first P2P
	// offer. Signal-driven promotion remains the provider-side path. The
	// manager copies this bounded seed; callers may reuse the settings slice.
	InitialNetworkPeerIds []Id
	DisconnectedTimeout   time.Duration
	FailedTimeout         time.Duration
	KeepAliveTimeout      time.Duration
	StunGatherTimeout     time.Duration

	DataChannelLabel   string
	DataChannelOrdered bool
	// SignalReceiveWorkerCount hashes each peer/stream onto one ordered
	// worker. Separate peers negotiate concurrently, while each stream's SDP
	// and ICE ordering and the bounded receive callback backpressure remain
	// intact.
	SignalReceiveWorkerCount int
	EnableSctpSnap           bool
	EnableSctpZeroChecksum   bool
	// EnableDatagramFastPath advertises the native SRTP datagram lane. It is
	// capability-negotiated and does not remove the legacy data channel.
	EnableDatagramFastPath bool
	// DatagramFastPathReceiveBufferSize bounds complete reassembled messages
	// waiting for the route worker. Full queues drop datagrams; the inner
	// transport recovers direct-IP loss without a second Transfer retry loop.
	// UdpSocketBufferByteCount is requested as the kernel send and receive
	// buffer of every ICE UDP socket; the kernel clamps it to its maximum
	// (net.core.rmem_max/wmem_max on Linux). Zero keeps the kernel default.
	UdpSocketBufferByteCount          int
	DatagramFastPathReceiveBufferSize int
	// DatagramFastPathWriteQueueSize bounds the native WebRTC socket's copied
	// userspace send buffer. A full queue blocks the carrier writer, preserving
	// backpressure instead of dropping before the kernel socket.
	DatagramFastPathWriteQueueSize int
	// DatagramFastPathWriteBatchSize bounds a ready-only socket drain. Linux
	// sends each drain with sendmmsg; other systems overlap sequential kernel
	// sends with Transfer and crypto work without adding an idle delay.
	DatagramFastPathWriteBatchSize int
	// DataPlaneStats receives carrier-level drops before a complete Transfer
	// message reaches P2pReceiveTransport. NewClient aligns this pointer with
	// the stream transport settings; direct WebRTC users may leave it nil.
	DataPlaneStats *P2pDataPlaneStats
	// SCTP congestion controls are byte counts. Zero retains Pion's RFC-style
	// default. CwndCAStep changes only additive recovery; MinCwnd is a hard
	// floor and FastRtxWnd is the loss-retransmit burst cap.
	SctpMinCwnd    uint32
	SctpFastRtxWnd uint32
	SctpCwndCAStep uint32
	// SctpNoProgressTimeout bounds a half-open data plane after outbound
	// activity. Zero disables the watchdog. It observes forward
	// acknowledgements and pauses while the peer advertises a zero receive
	// window, so transfer send/receive/forward callbacks retain their
	// intentional synchronous backpressure semantics.
	SctpNoProgressTimeout time.Duration
	// FastPathNoProgressTimeout bounds a native RTP/SRTP fast path that
	// accepts writes but delivers nothing. The RTP lane has no
	// acknowledgements of its own, so the bound needs a receiver progress
	// report; until candidate L1 (FLIGHTGATEFIX §7) supplies one this value
	// is recorded and not acted on. Zero disables it.
	FastPathNoProgressTimeout time.Duration
	// UseEgressOnlyIceInterfaces gathers host/server-reflexive candidates
	// only from the current default-route IPv4/IPv6 addresses. Device VPN
	// clients enable this to exclude their own tunnel, stale utun, bridge,
	// AWDL, and VM interfaces; generic server callers leave it false for
	// deliberate multihoming.
	UseEgressOnlyIceInterfaces bool
	// UseLoopbackOnlyIceInterfaces gathers host candidates only from
	// loopback interfaces, and takes precedence over
	// UseEgressOnlyIceInterfaces. Hermetic same-process tests enable this so
	// a local connect does not sweep the host's full local×remote candidate
	// cross-product: a multihomed development host (utun, bridge, AWDL, and
	// VM interfaces) otherwise floods ICE with unroutable pairs and pushes a
	// loopback-capable connect past the test deadline at STUN check pacing.
	// Production callers leave it false.
	UseLoopbackOnlyIceInterfaces bool

	// Network, when set, supplies native Pion's socket network. Tests use it
	// to put ICE, DTLS, SCTP, and SRTP below a userspace impairment model.
	// Nil retains normal interface selection. The caller owns the network;
	// peer teardown closes its sockets but not this shared object. Browser
	// WebRTC ignores it.
	Network transport.Net
	// Nil in production; tests provide a context-aware DNS boundary without
	// depending on the host resolver or packet timing.
	iceResolverForTest *net.Resolver
	// Nil in production; tests can pause native fast-path publication before
	// the remaining peer setup continues.
	afterFastPathPublishForTest func()
	// Nil in production; tests observe an admitted concrete Pion callback.
	beforePionCallbackForTest func(string)
	// Nil in production; tests hold the native fast-path OnTrack body after
	// Pion dispatch, independently of the lifecycle wrapper.
	beforeFastPathOnTrackBodyForTest func()
	// Zero in production; tests use the previous warmup version to prove that
	// a rolling upgrade selects the compatible SCTP path.
	datagramFastPathWarmupVersionForTest byte
	// Nil in production; tests observe reception of one exact warmup version.
	afterFastPathWarmupReceiveForTest func(byte)

	// add stun:xxx urls here
	IceServerUrls []string
}

func webRtcDataChannelInit(settings *WebRtcSettings) *webrtc.DataChannelInit {
	ordered := settings.DataChannelOrdered
	return &webrtc.DataChannelInit{
		Ordered: &ordered,
		// Reliability remains SCTP's default. The upper transfer layer can
		// recover loss, but retaining lower-layer recovery avoids turning a
		// cold path's first loss into its conservative application resend
		// timeout. Unordered delivery alone removes the duplicate HOL barrier.
	}
}

// pionLoggerFactory routes pion logs through a `Logger`, so the webrtc stack
// follows the same logger as the peer connection that created it (and is
// silenced with it). Without this, pion writes to its own default factory
// (stdout), bypassing per-client logging entirely.
// pion levels map: Error->Errorf, Warn->Warningf, Info->V(1), Debug/Trace->V(2).
type pionLoggerFactory struct {
	log Logger
}

func (self *pionLoggerFactory) NewLogger(scope string) logging.LeveledLogger {
	return &pionLeveledLogger{
		log:   self.log,
		scope: scope,
	}
}

type pionLeveledLogger struct {
	log   Logger
	scope string
}

func (self *pionLeveledLogger) Trace(msg string) {
	if self.log.V(2).Enabled() {
		self.log.Infof("[pion:%s]%s", self.scope, msg)
	}
}

func (self *pionLeveledLogger) Tracef(format string, args ...any) {
	if v := self.log.V(2); v.Enabled() {
		v.Infof("[pion:"+self.scope+"]"+format, args...)
	}
}

func (self *pionLeveledLogger) Debug(msg string) {
	if self.log.V(2).Enabled() {
		self.log.Infof("[pion:%s]%s", self.scope, msg)
	}
}

func (self *pionLeveledLogger) Debugf(format string, args ...any) {
	if v := self.log.V(2); v.Enabled() {
		v.Infof("[pion:"+self.scope+"]"+format, args...)
	}
}

func (self *pionLeveledLogger) Info(msg string) {
	if self.log.V(1).Enabled() {
		self.log.Infof("[pion:%s]%s", self.scope, msg)
	}
}

func (self *pionLeveledLogger) Infof(format string, args ...any) {
	if v := self.log.V(1); v.Enabled() {
		v.Infof("[pion:"+self.scope+"]"+format, args...)
	}
}

func (self *pionLeveledLogger) Warn(msg string) {
	if v := self.log.V(1); v.Enabled() {
		self.log.Warningf("[pion:%s]%s", self.scope, msg)
	}
}

func (self *pionLeveledLogger) Warnf(format string, args ...any) {
	if v := self.log.V(1); v.Enabled() {
		self.log.Warningf("[pion:"+self.scope+"]"+format, args...)
	}
}

func (self *pionLeveledLogger) Error(msg string) {
	self.log.Errorf("[pion:%s]%s", self.scope, msg)
}

func (self *pionLeveledLogger) Errorf(format string, args ...any) {
	self.log.Errorf("[pion:"+self.scope+"]"+format, args...)
}

// peerConnectionAdmissionState is the ownership half of a shared byte budget.
// TransferMemoryBudget already makes the byte ceiling exact across managers;
// this bounded registry makes reclamation equally global. Without it, a fresh
// window-client manager could see a full device pool but could neither identify
// an obsolete owner in another manager nor see that another teardown was
// already releasing capacity.
type peerConnectionAdmissionState struct {
	stateLock sync.Mutex
	owners    map[*peerConnectionAdmissionOwner]struct{}
}

// peerConnectionAdmissionOwner exists from exact byte reservation until
// physical peer teardown. All mutable fields are protected by the owning
// peerConnectionAdmissionState stateLock.
type peerConnectionAdmissionOwner struct {
	budget        *TransferMemoryBudget
	byteCount     ByteCount
	peerId        Id
	createdAt     time.Time
	priorityUntil time.Time
	ctx           context.Context
	cancel        context.CancelFunc
	retiring      bool
}

func (self *TransferMemoryBudget) getPeerConnectionAdmissionState(
	create bool,
) *peerConnectionAdmissionState {
	if self == nil {
		return nil
	}
	if state := self.peerConnectionAdmissionState.Load(); state != nil || !create {
		return state
	}
	candidate := &peerConnectionAdmissionState{
		owners: map[*peerConnectionAdmissionOwner]struct{}{},
	}
	if self.peerConnectionAdmissionState.CompareAndSwap(nil, candidate) {
		return candidate
	}
	return self.peerConnectionAdmissionState.Load()
}

// tryReservePeerConnectionOwner atomically pairs exact budget admission with a
// visible lifetime owner. Raw transfer-queue reservations can still coexist:
// TryReserve remains the single byte-ceiling authority.
func (self *TransferMemoryBudget) tryReservePeerConnectionOwner(
	owner *peerConnectionAdmissionOwner,
	byteCount ByteCount,
) bool {
	if self == nil || owner == nil || byteCount <= 0 {
		return false
	}
	state := self.getPeerConnectionAdmissionState(true)
	state.stateLock.Lock()
	defer state.stateLock.Unlock()
	if !self.TryReserve(byteCount) {
		return false
	}
	owner.budget = self
	owner.byteCount = byteCount
	state.owners[owner] = struct{}{}
	return true
}

func (self *peerConnectionAdmissionOwner) markRetiring() {
	if self == nil || self.budget == nil {
		return
	}
	state := self.budget.getPeerConnectionAdmissionState(false)
	if state == nil {
		return
	}
	state.stateLock.Lock()
	if _, exists := state.owners[self]; exists {
		self.retiring = true
	}
	state.stateLock.Unlock()
}

func (self *peerConnectionAdmissionOwner) cancelForReclamation() {
	if self == nil {
		return
	}
	self.markRetiring()
	if self.cancel != nil {
		self.cancel()
	}
}

func (self *peerConnectionAdmissionOwner) setPriorityUntil(until time.Time) {
	if self == nil || self.budget == nil {
		return
	}
	state := self.budget.getPeerConnectionAdmissionState(false)
	if state == nil {
		return
	}
	state.stateLock.Lock()
	if _, exists := state.owners[self]; exists {
		self.priorityUntil = until
	}
	state.stateLock.Unlock()
}

func (self *peerConnectionAdmissionOwner) release() {
	if self == nil || self.budget == nil {
		return
	}
	state := self.budget.getPeerConnectionAdmissionState(false)
	if state == nil {
		return
	}
	state.stateLock.Lock()
	if _, exists := state.owners[self]; !exists {
		state.stateLock.Unlock()
		return
	}
	delete(state.owners, self)
	// Keep owner visibility and byte release one transaction. A waiter woken
	// by Release can run immediately, but it cannot inspect a full budget with
	// neither an owner nor a pending release while this lock is held.
	self.budget.Release(self.byteCount)
	state.stateLock.Unlock()
	if self.cancel != nil {
		self.cancel()
	}
}

func (self *TransferMemoryBudget) peerConnectionOwnerCounts() (
	liveCount int,
	retiringCount int,
) {
	state := self.getPeerConnectionAdmissionState(false)
	if state == nil {
		return
	}
	state.stateLock.Lock()
	defer state.stateLock.Unlock()
	for owner := range state.owners {
		if owner.retiring {
			retiringCount++
		} else {
			liveCount++
		}
	}
	return
}

func (self *TransferMemoryBudget) peerConnectionReleasePending() bool {
	_, retiringCount := self.peerConnectionOwnerCounts()
	return 0 < retiringCount
}

// claimOldestPeerConnectionOwner marks exactly one pool-wide owner for
// reclamation. A pending retirement suppresses another claim so concurrent
// managers cannot drain every healthy association while one teardown is
// already creating sufficient headroom. The caller performs cancellation after
// releasing its own manager lock.
func (self *TransferMemoryBudget) claimOldestPeerConnectionOwner(
	protectPriority bool,
	now time.Time,
) *peerConnectionAdmissionOwner {
	state := self.getPeerConnectionAdmissionState(false)
	if state == nil {
		return nil
	}
	state.stateLock.Lock()
	defer state.stateLock.Unlock()

	for owner := range state.owners {
		if owner.retiring {
			return nil
		}
	}

	var oldest *peerConnectionAdmissionOwner
	oldestCanceled := false
	for owner := range state.owners {
		if protectPriority && now.Before(owner.priorityUntil) {
			continue
		}
		canceled := owner.ctx != nil && owner.ctx.Err() != nil
		if oldest == nil ||
			(canceled && !oldestCanceled) ||
			(canceled == oldestCanceled && owner.createdAt.Before(oldest.createdAt)) {
			oldest = owner
			oldestCanceled = canceled
		}
	}
	if oldest != nil {
		oldest.retiring = true
	}
	return oldest
}

type WebRtcManager struct {
	ctx          context.Context
	cancel       context.CancelFunc
	log          Logger
	signalSender SignalSender
	settings     *WebRtcSettings

	stateLock         sync.Mutex
	closed            bool
	closeDone         chan struct{}
	contextCloseStop  func() bool
	contextCloseDone  chan struct{}
	contextCloseOnce  sync.Once
	peerConnWorkers   sync.WaitGroup
	peerConnLifecycle *lifecycleAdmission
	peerConns         map[peerConnKey]*peerConn
	// retiringPeerConns tracks canceled generations independently of
	// peerConns. A make-before-break replacement removes the old generation
	// from the keyed map before its teardown releases the receive-window
	// reservation. Admission must still see that pending release; otherwise a
	// third setup cancels the newest usable generation too and repeated
	// retries can keep every generation retiring.
	retiringPeerConns          map[*peerConn]struct{}
	prioritizedPeers           map[Id]time.Time
	pendingPrioritizedPeerSlot map[Id]time.Time
	// networkPeers remembers authenticated ProvideMode_Network identities
	// independently of their short admission-priority lease. Priority may
	// expire while an idle connection is healthy, but a later path rebuild
	// must still select the dedicated SCTP window/budget. The map is hard
	// bounded and does not reserve capacity by itself.
	networkPeers map[Id]time.Time

	peerConnectionFactoryLock        sync.Mutex
	peerConnectionFactory            *webRtcPeerConnectionFactory
	peerConnectionFactoryInitErr     error
	peerConnectionFactoryInitialized bool
	peerConnectionFactoryRetryTime   time.Time
	peerConnectionFactoryClosed      bool
	// The DTLS certificate outlives path-bound ICE factory state. A network
	// change rebuilds SettingEngine/API/socket state but reuses the manager
	// identity, avoiding a P-256 key/certificate generation pause on reconnect.
	peerConnectionCertificate *webrtc.Certificate
	newPeerConnectionFactory  func(
		*WebRtcSettings,
		*webrtc.Certificate,
	) (*webRtcPeerConnectionFactory, *webrtc.Certificate, error)

	capacityMonitor *Monitor
	// Internal waiters use reason-specific signals. A shared buffered count
	// token wakes at most one setup per released map slot; admissionStateMonitor
	// is reserved for rare classification/priority changes. The exported
	// broadcast monitor remains for API compatibility.
	countCapacityNotify   chan struct{}
	admissionStateMonitor *Monitor

	// Admission is manager-wide but attempts arrive through many independent
	// P2P transports. Per-transport logging therefore still creates a startup
	// stampede at a full count or byte budget. Separate fixed counters preserve
	// reason-local totals while allowing power-of-two summaries across streams.
	admissionPriorityRefusalCount atomic.Uint64
	admissionCountRefusalCount    atomic.Uint64
	admissionBudgetRefusalCount   atomic.Uint64
	admissionOtherRefusalCount    atomic.Uint64

	networkChangeLock   sync.Mutex
	networkChangeWorker *coalescingCallbackWorker
	networkChangeUnsub  func()
	// Nil in production; tests observe the asynchronous worker immediately
	// before it attempts to acquire manager state.
	beforeNetworkChangeStateLockForTest func()
	// Nil in production; lifecycle tests observe successful peer-map
	// registration after newP2pConn has released manager state.
	testingAfterPeerConnRegistered func(TransferPath, bool)
	// Nil test barrier pauses a peer admission before manager state is locked.
	beforePeerConnAdmissionStateLockForTest func()
	// Nil test factory bypasses Pion construction for parent/child lifecycle
	// integration tests.
	newP2pConnForTest func(context.Context, TransferPath, bool) (WebRtcConn, error)
	// Nil test barrier pauses final done publication after all owned cleanup.
	beforeCloseDoneForTest func()
	// Nil test barrier pauses the manager-context close callback before Close.
	beforeContextCloseForTest func()
	// Nil test barrier exposes context-aware joined-wait entry.
	beforeCloseWaitForTest func()
}

const peerConnectionFactoryRetryTimeout = time.Second
const peerConnectionPriorityTimeout = 30 * time.Second
const maxPeerConnectionPriorityCount = 64
const maxRememberedNetworkPeerCount = 64

func NewWebRtcManager(ctx context.Context, signalSender SignalSender, settings *WebRtcSettings) *WebRtcManager {
	managerCtx, cancel := context.WithCancel(ctx)
	manager := &WebRtcManager{
		ctx:                        managerCtx,
		cancel:                     cancel,
		log:                        loggerOrDefault(settings.Log),
		signalSender:               signalSender,
		settings:                   settings,
		closeDone:                  make(chan struct{}),
		contextCloseDone:           make(chan struct{}),
		peerConnLifecycle:          newLifecycleAdmission(),
		peerConns:                  map[peerConnKey]*peerConn{},
		retiringPeerConns:          map[*peerConn]struct{}{},
		prioritizedPeers:           map[Id]time.Time{},
		pendingPrioritizedPeerSlot: map[Id]time.Time{},
		networkPeers:               map[Id]time.Time{},
		capacityMonitor:            NewMonitor(),
		admissionStateMonitor:      NewMonitor(),
		newPeerConnectionFactory:   newWebRtcPeerConnectionFactory,
	}
	if 0 < settings.MaxPeerConnectionCount {
		// A token represents one released peerConns map slot. Limit retained
		// stale tokens even if an embedder configures an unusually large cap;
		// the retry timer remains the liveness fallback above this bound.
		manager.countCapacityNotify = make(
			chan struct{},
			min(settings.MaxPeerConnectionCount, maxPeerConnectionPriorityCount),
		)
	}
	now := time.Now()
	for _, peerId := range settings.InitialNetworkPeerIds {
		if peerId != (Id{}) {
			manager.rememberNetworkPeerLocked(peerId, now)
		}
	}
	stop := context.AfterFunc(managerCtx, func() {
		if manager.beforeContextCloseForTest != nil {
			manager.beforeContextCloseForTest()
		}
		manager.Close()
		manager.contextCloseOnce.Do(func() {
			close(manager.contextCloseDone)
		})
	})
	manager.stateLock.Lock()
	if manager.closed {
		manager.stateLock.Unlock()
		if stop() {
			manager.contextCloseOnce.Do(func() {
				close(manager.contextCloseDone)
			})
		}
	} else {
		manager.contextCloseStop = stop
		manager.stateLock.Unlock()
	}
	return manager
}

// Close prevents later peer admission and requests teardown without joining
// Pion, signaling callbacks, or peer workers.
func (self *WebRtcManager) Close() {
	self.stateLock.Lock()
	if self.closed {
		self.stateLock.Unlock()
		return
	}
	self.closed = true
	stopContextClose := self.contextCloseStop
	self.contextCloseStop = nil
	for _, conn := range self.peerConns {
		self.retirePeerConnLocked(conn)
	}
	self.stateLock.Unlock()

	if stopContextClose != nil && stopContextClose() {
		self.contextCloseOnce.Do(func() {
			close(self.contextCloseDone)
		})
	}
	self.cancel()
	self.peerConnLifecycle.close()
	networkChangeWorker := self.stopNetworkChangeWorker()

	go func() {
		defer close(self.closeDone)
		<-self.contextCloseDone
		if networkChangeWorker != nil {
			networkChangeWorker.Wait()
		}
		self.peerConnWorkers.Wait()
		<-self.peerConnLifecycle.Done()
		self.closePeerConnectionFactory()

		self.stateLock.Lock()
		self.peerConns = nil
		self.retiringPeerConns = nil
		self.prioritizedPeers = nil
		self.pendingPrioritizedPeerSlot = nil
		self.networkPeers = nil
		self.stateLock.Unlock()
		self.capacityMonitor.NotifyAll()
		self.admissionStateMonitor.NotifyAll()
		if self.beforeCloseDoneForTest != nil {
			self.beforeCloseDoneForTest()
		}
	}()
}

// closeAndWait joins all peer generations and manager-owned ICE state, or
// returns when ctx expires.
func (self *WebRtcManager) closeAndWait(ctx context.Context) error {
	self.Close()
	if self.beforeCloseWaitForTest != nil {
		self.beforeCloseWaitForTest()
	}
	return waitForLifecycleDone(ctx, self.closeDone, "WebRTC manager")
}

func (self *WebRtcManager) prunePeerPrioritiesLocked(now time.Time) {
	for peerId, until := range self.prioritizedPeers {
		if !now.Before(until) {
			delete(self.prioritizedPeers, peerId)
		}
	}
	for peerId, until := range self.pendingPrioritizedPeerSlot {
		if !now.Before(until) {
			delete(self.pendingPrioritizedPeerSlot, peerId)
		}
	}
}

// rememberNetworkPeerLocked records an authenticated network-peer identity
// without tying it to the short-lived admission-priority lease. The record is
// bounded and carries no reservation, so idle peers cannot pin count or memory
// capacity. When the bound is full, an identity with no live association is
// evicted before one that still supplies the admission class for active
// streams. Call with stateLock held.
func (self *WebRtcManager) rememberNetworkPeerLocked(peerId Id, now time.Time) (newlyRemembered bool) {
	if self.networkPeers == nil {
		self.networkPeers = map[Id]time.Time{}
	}
	_, exists := self.networkPeers[peerId]
	if !exists && maxRememberedNetworkPeerCount <= len(self.networkPeers) {
		var oldestPeerId Id
		var oldestSeen time.Time
		oldestHasLiveConnection := true
		for candidatePeerId, candidateSeen := range self.networkPeers {
			candidateHasLiveConnection := self.hasLivePeerConnLocked(candidatePeerId)
			if oldestSeen.IsZero() ||
				(oldestHasLiveConnection && !candidateHasLiveConnection) ||
				(oldestHasLiveConnection == candidateHasLiveConnection &&
					candidateSeen.Before(oldestSeen)) {
				oldestPeerId = candidatePeerId
				oldestSeen = candidateSeen
				oldestHasLiveConnection = candidateHasLiveConnection
			}
		}
		delete(self.networkPeers, oldestPeerId)
	}
	self.networkPeers[peerId] = now
	return !exists
}

func (self *WebRtcManager) hasLivePeerConnLocked(peerId Id) bool {
	for key, conn := range self.peerConns {
		if key.PeerId == peerId && conn.ctx.Err() == nil {
			return true
		}
	}
	return false
}

func (self *WebRtcManager) hasPeerConnLocked(peerId Id) bool {
	for key := range self.peerConns {
		if key.PeerId == peerId {
			return true
		}
	}
	return false
}

func (self *WebRtcManager) hasPeerConnForAdmissionBudgetLocked(
	peerId Id,
	budget *TransferMemoryBudget,
) bool {
	for key, conn := range self.peerConns {
		if key.PeerId == peerId &&
			conn.ctx.Err() == nil &&
			conn.admissionBudget == budget {
			return true
		}
	}
	return false
}

// usesNetworkPeerAdmissionLocked reports whether peerId should use the
// dedicated receive window and budget. Call with stateLock held.
func (self *WebRtcManager) usesNetworkPeerAdmissionLocked(peerId Id) bool {
	_, trusted := self.networkPeers[peerId]
	if !trusted {
		// Bounded identity churn must not downgrade another stream from an
		// already-authenticated live Network association to the public Pion
		// API/budget. The immutable admission choice on that association is a
		// lifetime-safe trust witness even if the auxiliary LRU is at its
		// hard bound.
		for key, conn := range self.peerConns {
			if key.PeerId == peerId && conn.ctx.Err() == nil && conn.networkPeer {
				trusted = true
				break
			}
		}
	}
	if !trusted {
		return false
	}
	return 0 < self.settings.NetworkPeerReceiveBufferSize &&
		self.settings.NetworkPeerMemoryBudget != nil
}

// peerAdmissionLocked resolves the actual resource domain used by peerId.
// The budget pointer, rather than the public/Network label, defines the domain:
// selected window clients deliberately use one shared budget for both labels
// so their hard ceiling is not silently doubled.
func (self *WebRtcManager) peerAdmissionLocked(
	peerId Id,
) (
	networkPeer bool,
	budget *TransferMemoryBudget,
	reserveByteCount ByteCount,
) {
	networkPeer = self.usesNetworkPeerAdmissionLocked(peerId)
	budget = self.settings.MemoryBudget
	reserveByteCount = self.settings.ReceiveBufferSize
	if networkPeer {
		budget = self.settings.NetworkPeerMemoryBudget
		reserveByteCount = self.settings.NetworkPeerReceiveBufferSize
	}
	return
}

// oldestEvictablePeerConnLocked returns bounded capacity that a trusted,
// explicitly selected network peer may reclaim. Priority expires unless the
// provider continues to observe Network traffic, so an abandoned selection
// cannot pin a slot forever.
func (self *WebRtcManager) oldestEvictablePeerConnLocked(now time.Time) *peerConn {
	return self.oldestPeerConnLocked(true, now)
}

// oldestPeerConnLocked returns global count capacity. Count admission is one
// resource domain across public and Network peers, so the forced fallback must
// not filter by the unrelated byte-budget class.
func (self *WebRtcManager) oldestPeerConnLocked(
	protectPriority bool,
	now time.Time,
) *peerConn {
	var oldest *peerConn
	for _, conn := range self.peerConns {
		if conn.ctx.Err() != nil ||
			(protectPriority && now.Before(conn.priorityUntil)) {
			continue
		}
		if oldest == nil || conn.createdAt.Before(oldest.createdAt) {
			oldest = conn
		}
	}
	return oldest
}

// oldestPeerConnForAdmissionBudgetLocked returns the least recently observed
// association backed by the requested budget. A byte-budget reclamation must
// stay within that actual resource domain: labels are insufficient because a
// selected window client deliberately shares one budget between its public
// fallback and Network admission views. When protectPriority is true, live
// short-lease associations are skipped; callers may retry without that
// protection when every bounded slot is lease-protected.
//
// networkPeer is taken from the immutable connection admission choice, not the
// bounded remembered-identity map. A live dedicated association remains a
// valid reclamation target even if later identity churn evicts its trust record.
func (self *WebRtcManager) oldestPeerConnForAdmissionBudgetLocked(
	budget *TransferMemoryBudget,
	protectPriority bool,
	now time.Time,
) *peerConn {
	var oldest *peerConn
	var oldestSeen time.Time
	for _, conn := range self.peerConns {
		if conn.ctx.Err() != nil || conn.admissionBudget != budget ||
			(protectPriority && now.Before(conn.priorityUntil)) {
			continue
		}
		seen := conn.createdAt
		if conn.networkPeer {
			if rememberedSeen, remembered := self.networkPeers[conn.key.PeerId]; remembered {
				seen = rememberedSeen
			}
		}
		if oldest == nil ||
			seen.Before(oldestSeen) ||
			(seen.Equal(oldestSeen) && conn.createdAt.Before(oldest.createdAt)) {
			oldest = conn
			oldestSeen = seen
		}
	}
	return oldest
}

// peerConnReleasePendingLocked reports whether a prior reclamation is already
// tearing down. Admission retries are wake-driven, but several can observe the
// still-reserved bytes before teardown completes. Canceling another victim on
// each retry would drain healthy capacity instead of replacing one slot.
func (self *WebRtcManager) peerConnReleasePendingLocked() bool {
	for _, conn := range self.peerConns {
		if conn.ctx.Err() != nil {
			return true
		}
	}
	// An off-map replacement still owns bytes while it tears down, but its
	// keyed count slot is already occupied by the replacement. It will not
	// release count capacity, so it must not suppress count reclamation.
	return false
}

// peerConnBudgetReleasePendingLocked is the byte-budget counterpart to
// peerConnReleasePendingLocked. A canceled association in the other admission
// pool cannot create capacity here and must not suppress the required
// same-pool reclamation.
func (self *WebRtcManager) peerConnBudgetReleasePendingLocked(
	budget *TransferMemoryBudget,
) bool {
	if budget != nil && budget.peerConnectionReleasePending() {
		return true
	}
	for _, conn := range self.peerConns {
		if conn.ctx.Err() != nil && conn.admissionBudget == budget {
			return true
		}
	}
	for conn := range self.retiringPeerConns {
		if conn.admissionBudget == budget {
			return true
		}
	}
	return false
}

// markPeerConnRetiringLocked records the generation before cancellation or
// map replacement makes it invisible to keyed admission scans. The teardown
// worker removes it only after releasing its count/byte ownership.
func (self *WebRtcManager) markPeerConnRetiringLocked(conn *peerConn) {
	if conn == nil {
		return
	}
	conn.admissionOwner.markRetiring()
	if self.retiringPeerConns == nil {
		self.retiringPeerConns = map[*peerConn]struct{}{}
	}
	self.retiringPeerConns[conn] = struct{}{}
}

func (self *WebRtcManager) retirePeerConnLocked(conn *peerConn) {
	if conn == nil {
		return
	}
	self.markPeerConnRetiringLocked(conn)
	conn.Cancel()
}

// pendingPriorityBlocksAdmissionLocked reports whether an ordinary admission
// would consume capacity reserved by pending authenticated peers. It reserves
// only the cardinality actually needed: surplus slots in the same pool and
// independent slots in the other byte pool continue making progress. A
// pending signal with no matching setup must not freeze an entire pool for the
// priority lease.
func (self *WebRtcManager) pendingPriorityBlocksAdmissionLocked(
	networkPeer bool,
	growsPeerConnectionCount bool,
) bool {
	if len(self.pendingPrioritizedPeerSlot) == 0 {
		return false
	}

	pendingCountReservations := 0
	candidateBudget := self.settings.MemoryBudget
	candidateReserveByteCount := self.settings.ReceiveBufferSize
	if networkPeer {
		candidateBudget = self.settings.NetworkPeerMemoryBudget
		candidateReserveByteCount = self.settings.NetworkPeerReceiveBufferSize
	}
	budgetBlocked := false
	var availableAfterCandidate ByteCount
	budgetReservationsMatter :=
		candidateBudget != nil && 0 < candidateReserveByteCount
	if budgetReservationsMatter {
		availableAfterCandidate = candidateBudget.Available()
		if availableAfterCandidate < candidateReserveByteCount {
			budgetBlocked = true
		} else {
			availableAfterCandidate -= candidateReserveByteCount
		}
	}
	for peerId := range self.pendingPrioritizedPeerSlot {
		if !self.hasPeerConnLocked(peerId) {
			pendingCountReservations++
		}
		_, pendingBudget, pendingReserveByteCount :=
			self.peerAdmissionLocked(peerId)
		if budgetReservationsMatter &&
			pendingBudget == candidateBudget &&
			pendingBudget != nil &&
			0 < pendingReserveByteCount &&
			!self.hasPeerConnForAdmissionBudgetLocked(peerId, pendingBudget) {
			// Subtract each reservation as it is encountered instead of
			// summing ByteCount. Besides making the cardinality explicit, this
			// cannot overflow int64 when several callers configure very large
			// but individually valid receive windows.
			if availableAfterCandidate < pendingReserveByteCount {
				budgetBlocked = true
			} else {
				availableAfterCandidate -= pendingReserveByteCount
			}
		}
	}

	if maxCount := self.settings.MaxPeerConnectionCount; 0 < maxCount {
		prospectiveCount := len(self.peerConns) + pendingCountReservations
		if growsPeerConnectionCount {
			prospectiveCount++
		}
		if maxCount < prospectiveCount {
			return true
		}
	}

	if budgetBlocked {
		// The candidate needs one new lifetime reservation even when replacing
		// a map entry: newP2pConn never overdraws the hard budget while the old
		// generation tears down. Each pending peer that does not already own a
		// reservation in this pool needs one more.
		return true
	}
	return false
}

func (self *WebRtcManager) markPendingPriorityLocked(
	peerId Id,
	priorityUntil time.Time,
) {
	if priorityUntil.IsZero() {
		return
	}
	if self.pendingPrioritizedPeerSlot == nil {
		self.pendingPrioritizedPeerSlot = map[Id]time.Time{}
	}
	self.pendingPrioritizedPeerSlot[peerId] = priorityUntil
}

func (self *WebRtcManager) pendingPriorityRetryAfterLocked(now time.Time) time.Duration {
	var earliest time.Time
	for _, until := range self.pendingPrioritizedPeerSlot {
		if earliest.IsZero() || until.Before(earliest) {
			earliest = until
		}
	}
	if earliest.IsZero() {
		return 0
	}
	return max(time.Millisecond, earliest.Sub(now))
}

// PrioritizePeer gives a trusted same-network peer prompt access to the
// existing bounded P2P capacity. Providers can have every reservation occupied
// by speculative public negotiations; without preemption, an explicitly
// selected network peer competes in a wake-all retry lottery and may remain on
// the relay indefinitely. No count or byte ceiling is raised: at most one
// oldest non-priority connection is retired, and ordinary admissions pause
// until the selected peer consumes the released slot.
func (self *WebRtcManager) PrioritizePeer(peerId Id) {
	if peerId == (Id{}) {
		return
	}
	now := time.Now()
	until := now.Add(peerConnectionPriorityTimeout)

	var victim *peerConn
	var sharedVictim *peerConnectionAdmissionOwner
	var upgradeVictims []*peerConn
	var notifyAdmission bool
	self.stateLock.Lock()
	if self.closed {
		self.stateLock.Unlock()
		return
	}
	if self.prioritizedPeers == nil {
		self.prioritizedPeers = map[Id]time.Time{}
	}
	if self.pendingPrioritizedPeerSlot == nil {
		self.pendingPrioritizedPeerSlot = map[Id]time.Time{}
	}
	self.prunePeerPrioritiesLocked(now)
	if self.rememberNetworkPeerLocked(peerId, now) {
		// A waiter may have subscribed to the public budget immediately before
		// the authenticated Network relationship arrived. Wake it so it can
		// re-arm against the dedicated pool without waiting for the fallback
		// timer.
		notifyAdmission = true
	}
	if _, exists := self.prioritizedPeers[peerId]; !exists &&
		maxPeerConnectionPriorityCount <= len(self.prioritizedPeers) {
		var oldestPeerId Id
		var oldestUntil time.Time
		for candidatePeerId, candidateUntil := range self.prioritizedPeers {
			if oldestUntil.IsZero() || candidateUntil.Before(oldestUntil) {
				oldestPeerId = candidatePeerId
				oldestUntil = candidateUntil
			}
		}
		delete(self.prioritizedPeers, oldestPeerId)
		delete(self.pendingPrioritizedPeerSlot, oldestPeerId)
	}
	self.prioritizedPeers[peerId] = until

	hasLivePeerConn := false
	needsNetworkUpgrade := false
	for key, conn := range self.peerConns {
		if key.PeerId == peerId && conn.ctx.Err() == nil {
			conn.priorityUntil = until
			conn.admissionOwner.setPriorityUntil(until)
			hasLivePeerConn = true
			if self.usesNetworkPeerAdmissionLocked(peerId) && !conn.networkPeer {
				// The provider learns ProvideMode_Network after its passive
				// stream may already have constructed a public-window
				// association. Rebuild it immediately. ImmediateReconnect is a
				// persistent signal, and the normal waiting-for-offer exchange
				// replays negotiation state after this replacement.
				conn.requestImmediateReconnect()
				upgradeVictims = append(upgradeVictims, conn)
				needsNetworkUpgrade = true
			}
		}
	}
	if hasLivePeerConn && !needsNetworkUpgrade {
		// Do not create a reservation merely because an already-served peer's
		// priority was refreshed. Equally, do not clear an existing one here:
		// it can belong to a second stream whose admission failed after this
		// first association was created. Only that stream's successful
		// newP2pConn consumes the reservation; its original deadline bounds
		// stale state if the stream disappears.
	} else {
		_, alreadyPending := self.pendingPrioritizedPeerSlot[peerId]
		self.pendingPrioritizedPeerSlot[peerId] = until
		notifyAdmission = notifyAdmission || !alreadyPending
		if !alreadyPending {
			countFull := 0 < self.settings.MaxPeerConnectionCount &&
				self.settings.MaxPeerConnectionCount <= len(self.peerConns)
			// This slot is for a prioritized (Network) peer, so test the pool it
			// will actually admit against: the dedicated network-peer budget and
			// window when provisioned, else the public budget.
			_, budget, reserveByteCount :=
				self.peerAdmissionLocked(peerId)
			budgetFull := budget != nil && budget.Available() < reserveByteCount
			if budgetFull && !self.peerConnBudgetReleasePendingLocked(budget) {
				victim = self.oldestPeerConnForAdmissionBudgetLocked(budget, true, now)
				if victim == nil {
					victim = self.oldestPeerConnForAdmissionBudgetLocked(budget, false, now)
				}
				if victim == nil {
					sharedVictim = budget.claimOldestPeerConnectionOwner(true, now)
					if sharedVictim == nil {
						sharedVictim = budget.claimOldestPeerConnectionOwner(false, now)
					}
				}
			} else if countFull && !self.peerConnReleasePendingLocked() {
				victim = self.oldestEvictablePeerConnLocked(now)
				if victim == nil {
					victim = self.oldestPeerConnLocked(false, now)
				}
			}
		}
	}
	for _, upgradeVictim := range upgradeVictims {
		self.markPeerConnRetiringLocked(upgradeVictim)
	}
	self.markPeerConnRetiringLocked(victim)
	self.stateLock.Unlock()

	if notifyAdmission {
		self.capacityMonitor.NotifyAll()
		self.admissionStateMonitor.NotifyAll()
	}
	for _, upgradeVictim := range upgradeVictims {
		if self.log.V(1).Enabled() {
			self.log.Infof("[p2p]network peer %s upgrades public-window connection %s\n", peerId, upgradeVictim.key)
		}
		upgradeVictim.Cancel()
	}
	if victim != nil {
		if self.log.V(1).Enabled() {
			self.log.Infof("[p2p]priority peer %s evicts non-priority peer %s\n", peerId, victim.key.PeerId)
		}
		victim.Cancel()
	}
	if sharedVictim != nil {
		if self.log.V(1).Enabled() {
			self.log.Infof(
				"[p2p]priority peer %s reclaims shared-budget owner %s\n",
				peerId,
				sharedVictim.peerId,
			)
		}
		sharedVictim.cancelForReclamation()
	}
}

// ObserveNetworkPeerSignal grants the first authenticated signal the normal
// prompt-admission lease, then treats repeated negotiation signals only as
// trust evidence. Otherwise an orphaned StreamOpen can refresh its own lease
// forever, continually evicting live peers using nothing but failed WebRTC
// retries. Actual relayed Network data still calls PrioritizePeer and refreshes
// demand while the selected route is in use.
func (self *WebRtcManager) ObserveNetworkPeerSignal(peerId Id) {
	if peerId == (Id{}) {
		return
	}

	var upgradeVictims []*peerConn
	self.stateLock.Lock()
	if self.closed {
		self.stateLock.Unlock()
		return
	}
	_, remembered := self.networkPeers[peerId]
	if remembered {
		for key, conn := range self.peerConns {
			if key.PeerId == peerId &&
				conn.ctx.Err() == nil &&
				self.usesNetworkPeerAdmissionLocked(peerId) &&
				!conn.networkPeer {
				conn.requestImmediateReconnect()
				upgradeVictims = append(upgradeVictims, conn)
			}
		}
		for _, upgradeVictim := range upgradeVictims {
			self.markPeerConnRetiringLocked(upgradeVictim)
		}
	}
	self.stateLock.Unlock()

	if !remembered {
		self.PrioritizePeer(peerId)
		return
	}
	for _, upgradeVictim := range upgradeVictims {
		upgradeVictim.Cancel()
	}
}

// startNetworkChangeWorker is lazy for the same reason as the Pion factory:
// every transfer Client owns a WebRtcManager, but many never create a peer
// connection. Those clients need neither an OS-path listener nor an idle
// goroutine. Once a manager does bind ICE state to a path, host callbacks may
// run on a UI/NetworkExtension-owned thread, so teardown is dispatched onto a
// single coalescing worker and repeated notifications remain bounded.
func (self *WebRtcManager) startNetworkChangeWorker() {
	self.networkChangeLock.Lock()
	defer self.networkChangeLock.Unlock()
	if self.networkChangeWorker != nil || self.ctx.Err() != nil {
		return
	}
	worker := newCoalescingCallbackWorker(self.ctx, self.networkChanged)
	self.networkChangeWorker = worker
	self.networkChangeUnsub = AddNetworkChangeListener(worker.Dispatch)
}

// stopNetworkChangeWorker detaches and cancels path observation without
// waiting for a possibly blocked callback.
func (self *WebRtcManager) stopNetworkChangeWorker() *coalescingCallbackWorker {
	self.networkChangeLock.Lock()
	worker := self.networkChangeWorker
	unsub := self.networkChangeUnsub
	self.networkChangeWorker = nil
	self.networkChangeUnsub = nil
	self.networkChangeLock.Unlock()
	if unsub != nil {
		unsub()
	}
	if worker != nil {
		worker.Close()
	}
	return worker
}

// closeNetworkChangeWorker preserves the synchronous helper used by focused
// manager tests and non-client owners.
func (self *WebRtcManager) closeNetworkChangeWorker() {
	worker := self.stopNetworkChangeWorker()
	if worker != nil {
		worker.Wait()
	}
}

// webRtcPeerConnectionFactory owns immutable Pion state reused by all peer
// connections in one manager. It is initialized lazily: a multi-client that
// never receives a stream does not pay for an API, certificate, or native ICE
// resources.
type webRtcPeerConnectionFactory struct {
	// newPeerConnection builds a peer connection using the network-peer SCTP
	// receive window when networkPeer is true, else the public window. Its
	// cancel function owns address resolution for exactly that generation.
	newPeerConnection func(
		networkPeer bool,
	) (*webrtc.PeerConnection, context.CancelFunc, error)
	close func() error
}

func (self *webRtcPeerConnectionFactory) NewPeerConnection(
	networkPeer bool,
) (*webrtc.PeerConnection, context.CancelFunc, error) {
	return self.newPeerConnection(networkPeer)
}

func (self *webRtcPeerConnectionFactory) Close() error {
	if self == nil || self.close == nil {
		return nil
	}
	return self.close()
}

func (self *WebRtcManager) newPeerConnection(
	networkPeer bool,
) (*webrtc.PeerConnection, context.CancelFunc, error) {
	self.startNetworkChangeWorker()

	self.peerConnectionFactoryLock.Lock()
	defer self.peerConnectionFactoryLock.Unlock()

	if self.peerConnectionFactoryClosed || self.ctx.Err() != nil {
		return nil, nil, os.ErrClosed
	}
	if !self.peerConnectionFactoryInitialized ||
		(self.peerConnectionFactoryInitErr != nil &&
			!time.Now().Before(self.peerConnectionFactoryRetryTime)) {
		var certificate *webrtc.Certificate
		self.peerConnectionFactory, certificate, self.peerConnectionFactoryInitErr =
			self.newPeerConnectionFactory(self.settings, self.peerConnectionCertificate)
		self.peerConnectionFactoryInitialized = true
		if self.peerConnectionFactoryInitErr != nil {
			// Certificate/random-source and native socket setup failures can
			// be transient. Cache briefly to collapse a many-stream retry
			// stampede, but never poison this manager for its entire lifetime.
			self.peerConnectionFactoryRetryTime =
				time.Now().Add(peerConnectionFactoryRetryTimeout)
		} else {
			self.peerConnectionFactoryRetryTime = time.Time{}
			if certificate != nil {
				self.peerConnectionCertificate = certificate
			}
		}
	}
	if self.peerConnectionFactoryInitErr != nil {
		return nil, nil, self.peerConnectionFactoryInitErr
	}
	return self.peerConnectionFactory.NewPeerConnection(networkPeer)
}

func (self *WebRtcManager) closePeerConnectionFactory() {
	self.peerConnectionFactoryLock.Lock()
	if self.peerConnectionFactoryClosed {
		self.peerConnectionFactoryLock.Unlock()
		return
	}
	self.peerConnectionFactoryClosed = true
	factory := self.peerConnectionFactory
	self.peerConnectionFactory = nil
	self.peerConnectionCertificate = nil
	self.peerConnectionFactoryLock.Unlock()

	if err := factory.Close(); err != nil && self.log.V(1).Enabled() {
		self.log.Infof("[peerconn]factory close err = %s\n", err)
	}
}

// networkChanged immediately retires peer connections bound to the old path
// and invalidates manager-scoped ICE state. The outer P2P transports retain
// their platform route and re-negotiate without the normal reconnect delay;
// the next admission lazily rebuilds the factory against current interfaces.
func (self *WebRtcManager) networkChanged() {
	var factory *webRtcPeerConnectionFactory
	if self.beforeNetworkChangeStateLockForTest != nil {
		self.beforeNetworkChangeStateLockForTest()
	}
	self.stateLock.Lock()
	if self.closed {
		self.stateLock.Unlock()
		return
	}
	for _, conn := range self.peerConns {
		conn.requestImmediateReconnect()
		self.retirePeerConnLocked(conn)
	}
	self.peerConnectionFactoryLock.Lock()
	if !self.peerConnectionFactoryClosed {
		factory = self.peerConnectionFactory
		self.peerConnectionFactory = nil
		self.peerConnectionFactoryInitErr = nil
		self.peerConnectionFactoryInitialized = false
		self.peerConnectionFactoryRetryTime = time.Time{}
	}
	self.peerConnectionFactoryLock.Unlock()
	self.stateLock.Unlock()

	if err := factory.Close(); err != nil && self.log.V(1).Enabled() {
		self.log.Infof("[peerconn]network-change factory close err = %s\n", err)
	}
}

// peerConnectionAdmissionError identifies a temporary capacity refusal. The
// stream remains usable over the platform route while the P2P transport waits
// for a count/budget release instead of polling and rebuilding state.
type peerConnectionAdmissionReason string

const (
	peerConnectionAdmissionPriority peerConnectionAdmissionReason = "priority"
	peerConnectionAdmissionCount    peerConnectionAdmissionReason = "count"
	peerConnectionAdmissionBudget   peerConnectionAdmissionReason = "budget"
)

type peerConnectionAdmissionError struct {
	message    string
	retryAfter time.Duration
	reason     peerConnectionAdmissionReason
}

func (self *peerConnectionAdmissionError) Error() string {
	return self.message
}

func (self *WebRtcManager) observeAdmissionRefusal(
	reason peerConnectionAdmissionReason,
) (uint64, bool) {
	var counter *atomic.Uint64
	switch reason {
	case peerConnectionAdmissionPriority:
		counter = &self.admissionPriorityRefusalCount
	case peerConnectionAdmissionCount:
		counter = &self.admissionCountRefusalCount
	case peerConnectionAdmissionBudget:
		counter = &self.admissionBudgetRefusalCount
	default:
		counter = &self.admissionOtherRefusalCount
	}
	count := counter.Add(1)
	return count, count != 0 && count&(count-1) == 0
}

// AdmissionNotify returns notifications for manager state/count and the byte
// budget that peerId will actually use. Capture both before attempting
// admission so a release or Network promotion cannot be lost between the
// failed check and the wait.
func (self *WebRtcManager) AdmissionNotify(peerId Id) (countNotify <-chan struct{}, budgetNotify <-chan struct{}) {
	countNotify = self.capacityMonitor.NotifyChannel()
	self.stateLock.Lock()
	_, budget, _ := self.peerAdmissionLocked(peerId)
	self.stateLock.Unlock()
	if budget != nil {
		budgetNotify = budget.CapacityNotify()
	}
	return
}

// admissionNotify arms a reusable, threshold-aware budget waiter for the P2P
// lifecycle. The exported AdmissionNotify retains its broadcast contract for
// compatibility; the internal path prevents one freed receive window from
// scheduling every speculative setup.
func (self *WebRtcManager) admissionNotify(
	peerId Id,
	budgetWaiter *transferMemoryBudgetWaiter,
) (countNotify <-chan struct{}, budgetNotify <-chan struct{}) {
	countNotify = self.countCapacityNotify
	self.stateLock.Lock()
	_, budget, requiredByteCount := self.peerAdmissionLocked(peerId)
	self.stateLock.Unlock()
	budgetNotify = budgetWaiter.subscribe(budget, requiredByteCount)
	return
}

func (self *WebRtcManager) notifyCountCapacity() {
	if self.countCapacityNotify == nil {
		return
	}
	select {
	case self.countCapacityNotify <- struct{}{}:
	default:
	}
}

// ReceiveSignal decodes and applies one borrowed signaling frame.
func (self *WebRtcManager) ReceiveSignal(
	source TransferPath,
	transferKey TransferKey,
	frame *protocol.Frame,
) error {
	message, err := FromFrame(frame)
	if err != nil {
		return err
	}
	if v, ok := message.(*protocol.ExchangeSignals); ok {
		return self.ReceiveExchangeSignals(source, transferKey, v)
	}
	return nil
}

// ReceiveExchangeSignals applies one owned decoded signaling batch.
func (self *WebRtcManager) ReceiveExchangeSignals(
	source TransferPath,
	transferKey TransferKey,
	v *protocol.ExchangeSignals,
) error {
	streamId, err := IdFromBytes(v.StreamId)
	if err != nil {
		return err
	}
	var senderGenerationId Id
	senderGenerationSet := false
	if len(v.SenderGenerationId) != 0 {
		senderGenerationId, err = IdFromBytes(v.SenderGenerationId)
		if err != nil {
			return err
		}
		senderGenerationSet = true
	}
	key := peerConnKey{
		PeerId:   source.SourceId,
		StreamId: streamId,
	}
	var conn *peerConn
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		conn = self.peerConns[key]
		if self.log.V(2).Enabled() && conn == nil {
			self.log.Infof("[signal]miss %s (%v)\n", key, self.peerConns)
		}
	}()
	if conn == nil {
		return nil
	}
	// Signal delivery mutates the same Pion/state generation as asynchronous
	// Pion callbacks. Admit the complete batch before any reply-key or SDP/ICE
	// publication so teardown joins an in-flight batch and rejects a late one.
	connWorkers := conn.lifecycleWorkers()
	if !connWorkers.start() {
		return nil
	}
	defer connWorkers.finish()
	if conn.beforeReceiveSignalBatchForTest != nil {
		conn.beforeReceiveSignalBatchForTest()
	}
	if conn.ctx != nil && conn.ctx.Err() != nil {
		return nil
	}
	resetOffer := false
	if v.ResetSignals {
		for _, signal := range v.Signals {
			if signal != nil && signal.SignalType == protocol.SignalType_SdpOffer {
				resetOffer = true
				break
			}
		}
		if !resetOffer {
			return errors.New("reset_signals requires an SDP offer")
		}
	}
	conn.setSignalReplyTransferKey(transferKey)
	if resetOffer && conn.resetRemoteSignals(senderGenerationId, senderGenerationSet) {
		// A fresh active PeerConnection is offering against a passive
		// association that still looks connected. This is the asymmetric
		// idle-blackhole case: the active side can observe its unacknowledged
		// data and restart, while the passive side has no outbound SCTP work
		// from which to infer failure. The old behavior treated the fresh
		// offer as a duplicate forever. Retire the passive association; its
		// outer transport immediately creates a new one and sends
		// WaitingForSdpOffer, which makes the active side replay the cached
		// offer without another reset.
		self.log.V(1).Infof("[peerconn]fresh offer replaces negotiated passive %s\n", conn.key)
		conn.requestImmediateReconnect()
		conn.cancelBecause(errors.New("fresh remote offer replaced negotiated passive association"))
		return nil
	}
	var firstCandidateError error
	for _, signal := range v.Signals {
		if signal == nil {
			continue
		}
		if self.log.V(2).Enabled() {
			self.log.Infof("[signal]%s\n", signal.SignalType)
		}
		if err := conn.receiveSignalFromPeerWithTransferKey(
			signal,
			senderGenerationId,
			senderGenerationSet,
			transferKey,
			true,
		); err != nil {
			if signal.SignalType == protocol.SignalType_IceCandidate {
				// One malformed trickle candidate must not suppress every
				// later candidate in the same bounded batch.
				if firstCandidateError == nil {
					firstCandidateError = err
				}
				continue
			}
			return err
		}
	}
	return firstCandidateError
}

func (self *WebRtcManager) NewP2pConnActive(ctx context.Context, path TransferPath) (WebRtcConn, error) {
	if self.newP2pConnForTest != nil {
		return self.newP2pConnForTest(ctx, path, true)
	}
	conn, err := self.newP2pConn(ctx, path, true)
	if err == nil && self.testingAfterPeerConnRegistered != nil {
		self.testingAfterPeerConnRegistered(path, true)
	}
	return conn, err
}

func (self *WebRtcManager) NewP2pConnPassive(ctx context.Context, path TransferPath) (WebRtcConn, error) {
	if self.newP2pConnForTest != nil {
		return self.newP2pConnForTest(ctx, path, false)
	}
	conn, err := self.newP2pConn(ctx, path, false)
	if err == nil && self.testingAfterPeerConnRegistered != nil {
		self.testingAfterPeerConnRegistered(path, false)
	}
	return conn, err
}

func (self *WebRtcManager) newP2pConn(ctx context.Context, path TransferPath, active bool) (conn *peerConn, err error) {
	if err = ctx.Err(); err != nil {
		return
	}
	if err = self.ctx.Err(); err != nil {
		return
	}
	if self.beforePeerConnAdmissionStateLockForTest != nil {
		self.beforePeerConnAdmissionStateLockForTest()
	}
	var sharedVictim *peerConnectionAdmissionOwner
	self.stateLock.Lock()
	defer func() {
		self.stateLock.Unlock()
		if sharedVictim != nil {
			sharedVictim.cancelForReclamation()
		}
	}()
	if self.closed {
		err = os.ErrClosed
		return
	}
	// Stream teardown can race while waiting for another setup to release the
	// manager lock. Do not allocate Pion state or reserve capacity for work
	// whose owner has already gone away.
	if err = ctx.Err(); err != nil {
		return
	}
	if err = self.ctx.Err(); err != nil {
		return
	}

	key := peerConnKey{
		PeerId:   path.DestinationId,
		StreamId: path.StreamId,
	}
	now := time.Now()
	self.prunePeerPrioritiesLocked(now)
	priorityUntil, priority := self.prioritizedPeers[key.PeerId]
	networkPeer := self.usesNetworkPeerAdmissionLocked(key.PeerId)
	_, replacing := self.peerConns[key]

	// Once trusted Network traffic requests a slot, do not let one of the
	// wake-all speculative waiters steal capacity from the same admission
	// pool or the last global count slot. Independent public/dedicated pools
	// continue making progress when count capacity remains.
	if !priority && self.pendingPriorityBlocksAdmissionLocked(networkPeer, !replacing) {
		err = &peerConnectionAdmissionError{
			message:    "peer connection waiting for prioritized network peer",
			retryAfter: self.pendingPriorityRetryAfterLocked(now),
			reason:     peerConnectionAdmissionPriority,
		}
		return
	}

	// refuse new peer connections at the cap. A create for an existing key
	// replaces that connection (the map does not grow), so it is allowed.
	if maxCount := self.settings.MaxPeerConnectionCount; 0 < maxCount && maxCount <= len(self.peerConns) {
		if _, ok := self.peerConns[key]; !ok {
			if priority && !self.peerConnReleasePendingLocked() {
				if victim := self.oldestEvictablePeerConnLocked(now); victim != nil {
					self.retirePeerConnLocked(victim)
				} else {
					if victim := self.oldestPeerConnLocked(false, now); victim != nil {
						self.retirePeerConnLocked(victim)
					}
				}
			}
			if priority {
				// PrioritizePeer removes pending state when this peer already
				// owns any live association. A second stream can still hit the
				// global cap; retain its place after the failed attempt so an
				// ordinary wake-all waiter cannot steal the released slot.
				self.markPendingPriorityLocked(key.PeerId, priorityUntil)
			}
			err = &peerConnectionAdmissionError{
				message: fmt.Sprintf("peer connection limit reached (%d)", maxCount),
				reason:  peerConnectionAdmissionCount,
			}
			return
		}
	}

	// A trusted network peer uses the larger network-peer SCTP receive window
	// and its own bounded budget, so that footprint never multiplies across the
	// many public peers (Fix 1). Authenticated Network identity is remembered
	// separately from the short priority lease so idle/network-change rebuilds
	// do not silently fall back to the small public window. The network-peer
	// path is taken only when both the larger window and its dedicated budget
	// are provisioned, so an unbudgeted caller safely falls back to public
	// admission.
	_, reserveBudget, receiveBufferByteCount :=
		self.peerAdmissionLocked(key.PeerId)

	// byte admission against the (public or network-peer) budget: a peer
	// connection can queue up to its receive window, so each conn owns that
	// reservation for its lifetime. A new setup is refused when the budget has
	// no headroom (the stream stays on the platform transport, as at the count
	// cap); a replacement also needs real headroom. If it cannot coexist
	// briefly with the old connection, cancel the old one and retry on its
	// release rather than overdrawing the supposedly fixed memory ceiling.
	var reserveByteCount ByteCount
	admissionCtx := ctx
	var admissionOwner *peerConnectionAdmissionOwner
	if reserveBudget != nil {
		reserveByteCount = receiveBufferByteCount
		ownerCtx, ownerCancel := context.WithCancel(ctx)
		candidateOwner := &peerConnectionAdmissionOwner{
			peerId:        key.PeerId,
			createdAt:     now,
			priorityUntil: priorityUntil,
			ctx:           ownerCtx,
			cancel:        ownerCancel,
		}
		if !reserveBudget.tryReservePeerConnectionOwner(
			candidateOwner,
			reserveByteCount,
		) {
			ownerCancel()
			replacing := self.peerConns[key] != nil
			if replacedConn := self.peerConns[key]; replacedConn != nil {
				// A prior replacement may already be off-map and releasing
				// this same budget. Preserve the newest keyed generation while
				// that release is pending instead of turning every retry into
				// another break-before-make teardown.
				if !self.peerConnBudgetReleasePendingLocked(reserveBudget) {
					self.retirePeerConnLocked(replacedConn)
				}
			} else if priority && !self.peerConnBudgetReleasePendingLocked(reserveBudget) {
				if victim := self.oldestPeerConnForAdmissionBudgetLocked(
					reserveBudget,
					true,
					now,
				); victim != nil {
					self.retirePeerConnLocked(victim)
				} else {
					if victim := self.oldestPeerConnForAdmissionBudgetLocked(
						reserveBudget,
						false,
						now,
					); victim != nil {
						self.retirePeerConnLocked(victim)
					} else {
						sharedVictim =
							reserveBudget.claimOldestPeerConnectionOwner(true, now)
						if sharedVictim == nil {
							sharedVictim =
								reserveBudget.claimOldestPeerConnectionOwner(false, now)
						}
					}
				}
			}
			samePeerConnectionCount := 0
			for candidateKey := range self.peerConns {
				if candidateKey.PeerId == key.PeerId {
					samePeerConnectionCount += 1
				}
			}
			if priority {
				// The same multi-stream hole exists for byte admission. Once
				// the canceled owner releases its reservation, this failed
				// selected stream must remain ahead of speculative admissions.
				self.markPendingPriorityLocked(key.PeerId, priorityUntil)
			}
			sharedLiveCount, sharedRetiringCount :=
				reserveBudget.peerConnectionOwnerCounts()
			err = &peerConnectionAdmissionError{
				message: fmt.Sprintf(
					"peer connection memory budget exhausted (used=%d total=%d need=%d networkPeer=%v live=%d retiring=%d sharedLive=%d sharedRetiring=%d samePeer=%d replacing=%v)",
					reserveBudget.UsedByteCount(),
					reserveBudget.TotalByteCount(),
					reserveByteCount,
					networkPeer,
					len(self.peerConns),
					len(self.retiringPeerConns),
					sharedLiveCount,
					sharedRetiringCount,
					samePeerConnectionCount,
					replacing,
				),
				reason: peerConnectionAdmissionBudget,
			}
			return
		}
		admissionOwner = candidateOwner
		admissionCtx = ownerCtx
	}

	conn, err = newPeerConn(
		admissionCtx,
		key,
		path.SourceId,
		active,
		self.signalSender,
		self.settings,
		func() (*webrtc.PeerConnection, context.CancelFunc, error) {
			return self.newPeerConnection(networkPeer)
		},
	)
	if err != nil {
		if admissionOwner != nil {
			admissionOwner.release()
			admissionOwner.cancelForReclamation()
		} else if 0 < reserveByteCount {
			reserveBudget.Release(reserveByteCount)
		}
		return
	}
	conn.priorityUntil = priorityUntil
	conn.networkPeer = networkPeer
	conn.admissionBudget = reserveBudget
	conn.admissionByteCount = reserveByteCount
	conn.admissionOwner = admissionOwner
	if !self.peerConnLifecycle.start() {
		panic("open WebRTC manager rejected peer lifecycle admission")
	}
	// Resource teardown is cancellation-driven rather than Run-return-driven.
	// A synchronous signal send is intentional backpressure and may still be
	// blocked when a generation is replaced; it cannot be allowed to retain
	// the old receive-window reservation and prevent the replacement itself.
	if !conn.startWorker("peer connection run", conn.Run, func(err error) {
		conn.cancelBecause(fmt.Errorf("peer connection run panic: %w", err))
	}) {
		panic("new peer connection rejected Run worker")
	}
	self.peerConnWorkers.Add(1)
	go HandleError(func() {
		defer self.peerConnWorkers.Done()
		defer self.peerConnLifecycle.finish()
		<-conn.ctx.Done()
		conn.workers.close()
		self.stateLock.Lock()
		self.markPeerConnRetiringLocked(conn)
		self.stateLock.Unlock()
		// A defensive panic boundary is required inside the lifecycle worker:
		// the outer HandleError can recover a library teardown panic, but it
		// cannot resume the function to release admission ownership.
		HandleError(conn.teardown)
		releasedCountCapacity := false
		if conn.admissionOwner != nil {
			// The pool-wide owner remains marked retiring while the local maps
			// are cleared. A concurrent manager therefore still sees the
			// pending release, while a waiter woken by the subsequent byte
			// release can never observe stale local retirement state.
			self.stateLock.Lock()
			delete(self.retiringPeerConns, conn)
			if conn == self.peerConns[key] {
				delete(self.peerConns, key)
				releasedCountCapacity = true
			}
			self.stateLock.Unlock()
			conn.admissionOwner.release()
		} else {
			if conn.admissionBudget != nil && 0 < conn.admissionByteCount {
				// Compatibility for lightweight synthetic connections with no
				// pool-wide owner: release before removing the canceled map
				// entry so admission still sees a pending local teardown.
				conn.admissionBudget.Release(conn.admissionByteCount)
			}
			self.stateLock.Lock()
			delete(self.retiringPeerConns, conn)
			if conn == self.peerConns[key] {
				delete(self.peerConns, key)
				releasedCountCapacity = true
			}
			self.stateLock.Unlock()
		}
		if releasedCountCapacity {
			self.notifyCountCapacity()
		}
		self.capacityMonitor.NotifyAll()
		<-conn.workers.Done()
	})

	replacedConn := self.peerConns[key]
	if replacedConn != nil {
		self.retirePeerConnLocked(replacedConn)
	}
	self.peerConns[key] = conn
	delete(self.pendingPrioritizedPeerSlot, key.PeerId)
	return
}

type peerConnKey struct {
	PeerId   Id
	StreamId Id
}

func (self peerConnKey) String() string {
	return fmt.Sprintf("s(%s) <>%s", self.StreamId, self.PeerId)
}

type peerConnectionTeardownStage int32

const (
	peerConnectionTeardownStarting peerConnectionTeardownStage = iota
	peerConnectionTeardownStoppingDtls
	peerConnectionTeardownClosingPeer
	peerConnectionTeardownClosingFastPath
	peerConnectionTeardownClosingDataChannel
	peerConnectionTeardownClearingSignals
	peerConnectionTeardownComplete
)

func (self peerConnectionTeardownStage) String() string {
	switch self {
	case peerConnectionTeardownStarting:
		return "starting"
	case peerConnectionTeardownStoppingDtls:
		return "stopping-dtls"
	case peerConnectionTeardownClosingPeer:
		return "closing-peer"
	case peerConnectionTeardownClosingFastPath:
		return "closing-fast-path"
	case peerConnectionTeardownClosingDataChannel:
		return "closing-data-channel"
	case peerConnectionTeardownClearingSignals:
		return "clearing-signals"
	case peerConnectionTeardownComplete:
		return "complete"
	default:
		return fmt.Sprintf("unknown-%d", self)
	}
}

const peerConnectionSlowTeardownTimeout = 5 * time.Second

// logPeerConnectionTeardownStacks keeps each goroutine in its own log record.
// The glog backend truncates one record at 15 KB; one monolithic runtime.Stack
// dump therefore hides precisely the later goroutine that owns a shutdown
// dependency when the process has many workers.
func logPeerConnectionTeardownStacks(
	log Logger,
	stage peerConnectionTeardownStage,
	key peerConnKey,
) {
	stackBuffer := make([]byte, 256*1024)
	for {
		stackByteCount := runtime.Stack(stackBuffer, true)
		if stackByteCount < len(stackBuffer) {
			goroutineStacks := strings.Split(
				strings.TrimSpace(string(stackBuffer[:stackByteCount])),
				"\n\n",
			)
			for stackIndex, goroutineStack := range goroutineStacks {
				log.Infof(
					"[peerconn]teardown goroutine %d/%d at %s %s:\n%s\n",
					stackIndex+1,
					len(goroutineStacks),
					stage,
					key,
					goroutineStack,
				)
			}
			return
		}
		stackBuffer = make([]byte, 2*len(stackBuffer))
	}
}

func startPeerConnectionTeardownWatchdog(
	timeout time.Duration,
	stage *atomic.Int32,
	onStall func(peerConnectionTeardownStage),
) *peerConnectionTeardownWatchdog {
	watchdog := &peerConnectionTeardownWatchdog{
		done: make(chan struct{}),
	}
	watchdog.timer = time.AfterFunc(timeout, func() {
		defer watchdog.doneOnce.Do(func() {
			close(watchdog.done)
		})
		onStall(peerConnectionTeardownStage(stage.Load()))
	})
	return watchdog
}

// peerConnectionTeardownWatchdog owns its timer callback until StopAndWait.
type peerConnectionTeardownWatchdog struct {
	timer    *time.Timer
	done     chan struct{}
	doneOnce sync.Once
}

// Stop prevents a callback that has not begun and reports whether it won.
func (self *peerConnectionTeardownWatchdog) Stop() bool {
	stopped := self.timer.Stop()
	if stopped {
		self.doneOnce.Do(func() {
			close(self.done)
		})
	}
	return stopped
}

// StopAndWait prevents or joins the exact diagnostic callback generation.
func (self *peerConnectionTeardownWatchdog) StopAndWait() {
	self.Stop()
	<-self.done
}

// conforms to WebRtcConn
type peerConn struct {
	ctx         context.Context
	cancel      context.CancelFunc
	cancelCause context.CancelCauseFunc
	log         Logger

	key       peerConnKey
	sourceId  Id
	active    bool
	createdAt time.Time
	// networkPeer is immutable after creation. It records which Pion API and
	// admission pool back this association so a late authenticated Network
	// promotion can replace a public-window connection exactly once.
	networkPeer bool
	// admissionBudget and admissionByteCount are immutable after manager
	// admission. The pointer is the true resource-domain identity: SDK window
	// clients intentionally share one budget between public fallback and
	// Network views even though their labels differ.
	admissionBudget    *TransferMemoryBudget
	admissionByteCount ByteCount
	// admissionOwner makes this fixed reservation visible to every manager
	// sharing admissionBudget. nil is retained for lightweight tests and
	// unbudgeted callers.
	admissionOwner *peerConnectionAdmissionOwner
	// priorityUntil is protected by WebRtcManager.stateLock. It lets an
	// explicitly selected Network peer retain bounded P2P admission while
	// traffic refreshes the priority, without permanently pinning a slot.
	priorityUntil time.Time

	signalSender SignalSender
	settings     *WebRtcSettings
	fastPath     atomic.Pointer[webRtcFastPath]

	// api *webrtc.API
	pc *webrtc.PeerConnection
	// Pion's transport.Net resolver API has no context. The native factory
	// supplies a per-generation context so teardown can cancel a STUN/TURN name
	// lookup before PeerConnection.Close joins ICE candidate gathering.
	cancelIceResolve context.CancelFunc
	// pionLifecycleLock serializes every operation that can lazily create or
	// advance Pion-owned ICE state with physical teardown. Run is allowed to
	// remain blocked in SignalSender after startup, so teardown cannot join the
	// whole Run worker before closing Pion; it must instead join this bounded
	// mutation section. Without this gate a canceled replacement could close a
	// pristine PeerConnection between Run's context check and
	// SetLocalDescription, after which Run could create an ICE task loop on the
	// already-closed connection with no remaining owner able to stop it.
	pionLifecycleLock sync.Mutex

	connectedCallbacks *CallbackList[*connectedCallback]
	connMonitor        *Monitor
	connectedMonitor   *Monitor
	connectedDispatch  sync.Once
	outboundProgress   chan struct{}
	outboundByteCount  atomic.Uint64
	progressWatchOnce  sync.Once
	workers            *lifecycleAdmission
	// Nil test barrier pauses an owned worker before joined completion.
	beforeWorkerDoneForTest func(string)
	// Nil test barriers expose admitted Pion callbacks before terminal state
	// publication, where teardown must win without leaving stale resources.
	beforeIceCandidateStateLockForTest    func()
	beforeConnectedStateLockForTest       func()
	beforeOpenDataChannelStateLockForTest func()
	beforeReceiveSignalLockForTest        func()
	beforeReceiveSignalBatchForTest       func()
	// Nil test barrier confirms Run installed its Pion callback registrations.
	afterPionCallbacksRegisteredForTest func()
	// Nil test barrier pauses active startup after offer creation and before
	// the cancellation recheck and SetLocalDescription mutation.
	beforeSetLocalDescriptionForTest func()

	// Closed once when the outer transport should reconnect without honoring
	// the usual backoff delay. A persistent one-shot channel cannot lose a
	// notification if network change or remote restart races the caller's
	// subscription.
	immediateReconnect     chan struct{}
	immediateReconnectOnce sync.Once
	// teardown is driven by context cancellation, independently of Run.
	// Run can be intentionally backpressured inside SignalSender; that must
	// not retain Pion sockets, receive-window admission, or blocked data-plane
	// I/O after this association has been replaced.
	teardownOnce sync.Once
	teardownDone chan struct{}

	// Pion's offer/answer state machine is not safe to advance concurrently.
	// Client signal sharding serializes a peer/stream in production, but this
	// lock also protects direct SignalReceiver users and teardown races.
	// Never call SignalSender while holding it: sender-owned sends may apply
	// synchronous backpressure and can synchronously deliver the response;
	// receive/Pion-callback sends use the zero-timeout marker.
	signalLock sync.Mutex
	stateLock  sync.Mutex
	conn       datachannel.ReadWriteCloserDeadliner
	connected  bool
	// Incremented with each connected transition. Callback entries use the
	// generation to suppress duplicate/late initial delivery and preserve
	// ordering when registration races an ICE state change.
	connectedGeneration uint64
	offer               *protocol.ExchangeSignal
	answer              *protocol.ExchangeSignal
	// signalGeneration identifies this PeerConnection generation in every
	// outbound signal batch. remoteSignalGeneration is learned from a valid
	// offer/answer and lets WaitingForSdpOffer distinguish delayed startup
	// signaling from a genuinely replaced passive association.
	signalGeneration          Id
	remoteSignalGeneration    Id
	remoteSignalGenerationSet bool
	// Inbound signaling supplies the local lane/session used by replies. Active
	// setup has no key until the peer answers, so the presence bit is separate.
	signalReplyTransferKey    TransferKey
	signalReplyTransferKeySet bool
	// ICE callbacks are deferred by Pion. Pin their reply key to the SDP
	// negotiation that started gathering so a later signal on another logical
	// lane cannot retarget already-scheduled candidates.
	iceCandidateReplyTransferKey    TransferKey
	iceCandidateReplyTransferKeySet bool

	// candidates emitted before sdp negotiation completes are buffered
	// here so they aren't dropped. flushed once iceCandidatesReady is set.
	iceCandidateBuffer []*webrtc.ICECandidate
	iceCandidatesReady bool

	// Remote candidates can legally race ahead of SDP when signaling crosses
	// queues or transports. Pion rejects AddICECandidate before a remote
	// description, so retain a strictly bounded set and apply it immediately
	// after SDP succeeds instead of silently losing the path.
	remoteDescriptionSet          bool
	remoteIceCandidateBuffer      []remoteIceCandidate
	remoteIceCandidateBufferBytes int
	remoteIceCandidateOverflowLog bool

	readDeadline  time.Time
	writeDeadline time.Time
}

const (
	maxBufferedRemoteIceCandidateCount = 64
	maxBufferedRemoteIceCandidateBytes = 64 * 1024
	maxIceCandidatesPerSignalFrame     = 32
	maxIceCandidateBytesPerSignalFrame = 32 * 1024
)

type remoteIceCandidate struct {
	value               webrtc.ICECandidateInit
	senderGenerationId  Id
	senderGenerationSet bool
}

func newPeerConn(
	ctx context.Context,
	key peerConnKey,
	sourceId Id,
	active bool,
	signalSender SignalSender,
	settings *WebRtcSettings,
	newPeerConnection func() (*webrtc.PeerConnection, context.CancelFunc, error),
) (*peerConn, error) {
	pc, cancelIceResolve, err := newPeerConnection()
	if err != nil {
		return nil, err
	}

	cancelCtx, cancelCause := context.WithCancelCause(ctx)
	cancel := func() {
		cancelCause(context.Canceled)
	}

	conn := &peerConn{
		ctx:              cancelCtx,
		cancel:           cancel,
		cancelCause:      cancelCause,
		log:              loggerOrDefault(settings.Log),
		key:              key,
		sourceId:         sourceId,
		active:           active,
		createdAt:        time.Now(),
		signalSender:     signalSender,
		settings:         settings,
		signalGeneration: NewId(),
		// api:                api,
		pc:                 pc,
		cancelIceResolve:   cancelIceResolve,
		connectedCallbacks: NewCallbackList[*connectedCallback](),
		connMonitor:        NewMonitor(),
		connectedMonitor:   NewMonitor(),
		outboundProgress:   make(chan struct{}, 1),
		immediateReconnect: make(chan struct{}),
		teardownDone:       make(chan struct{}),
		workers:            newLifecycleAdmission(),
	}
	return conn, nil
}

// startWorker admits one Run, callback-dispatch, or progress-watchdog worker
// before launch.
func (self *peerConn) startWorker(name string, run func(), handlers ...any) bool {
	workers := self.lifecycleWorkers()
	if !workers.start() {
		return false
	}
	go func() {
		defer workers.finish()
		HandleError(run, handlers...)
		if self.beforeWorkerDoneForTest != nil {
			self.beforeWorkerDoneForTest(name)
		}
	}()
	return true
}

// runPionCallback owns one synchronously dispatched Pion callback body under
// the same generation gate as peer workers. Teardown closes this gate before
// PeerConnection.Close, so callbacks already in flight are joined and later
// callbacks cannot mutate or signal through a retired generation.
func (self *peerConn) runPionCallback(name string, run func(), handlers ...any) bool {
	workers := self.lifecycleWorkers()
	if !workers.start() {
		return false
	}
	defer workers.finish()
	if self.settings != nil && self.settings.beforePionCallbackForTest != nil {
		self.settings.beforePionCallbackForTest(name)
	}
	HandleError(run, handlers...)
	if self.beforeWorkerDoneForTest != nil {
		self.beforeWorkerDoneForTest(name)
	}
	return true
}

// lifecycleWorkers returns the generation gate. Production constructors set
// it eagerly; lazy initialization preserves lightweight synthetic peerConn
// values used by compatibility tests.
func (self *peerConn) lifecycleWorkers() *lifecycleAdmission {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.workers == nil {
		self.workers = newLifecycleAdmission()
	}
	return self.workers
}

// withPionMutation admits one bounded operation that can initialize or advance
// PeerConnection state. It establishes the single lock order used by startup,
// signaling, fast-path configuration, and teardown, and releases the owner
// even if the Pion boundary panics into the caller's recovery handler.
func (self *peerConn) withPionMutation(run func() error) error {
	self.pionLifecycleLock.Lock()
	defer self.pionLifecycleLock.Unlock()
	if self.ctx != nil && self.ctx.Err() != nil {
		if err := context.Cause(self.ctx); err != nil {
			return err
		}
		return self.ctx.Err()
	}
	return run()
}

func (self *peerConn) Run() {
	if self.ctx.Err() != nil {
		return
	}

	// Each bounded Pion mutation is serialized with teardown. Deliberately
	// blocking hooks and SignalSender calls stay outside the gate so physical
	// teardown never waits on application work.
	if err := self.withPionMutation(func() error {
		self.pc.OnICEConnectionStateChange(func(state webrtc.ICEConnectionState) {
			self.runPionCallback("ICE connection state callback", func() {
				self.handleICEConnectionState(state)
			}, self.cancel)
		})
		self.pc.OnConnectionStateChange(func(state webrtc.PeerConnectionState) {
			self.runPionCallback("peer connection state callback", func() {
				self.handlePeerConnectionState(state)
			}, self.cancel)
		})
		return nil
	}); err != nil {
		return
	}

	if err := self.configureFastPath(); err != nil {
		self.cancelBecause(fmt.Errorf("configure datagram fast path: %w", err))
		return
	}

	startupErr := self.withPionMutation(func() error {
		// Register the candidate handler before SetLocalDescription so
		// candidates emitted during gathering are not dropped.
		self.pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
			self.runPionCallback("ICE candidate callback", func() {
				self.handleLocalIceCandidate(candidate)
			}, self.cancel)
		})
		if self.active {
			dc, err := self.pc.CreateDataChannel(
				self.settings.DataChannelLabel,
				webRtcDataChannelInit(self.settings),
			)
			if err != nil {
				return fmt.Errorf("create data channel: %w", err)
			}
			dc.OnOpen(func() {
				self.runPionCallback("data channel open callback", func() {
					self.handleOpenDataChannel(dc)
				}, self.cancel)
			})
		} else {
			self.pc.OnDataChannel(func(dc *webrtc.DataChannel) {
				self.runPionCallback("data channel callback", func() {
					if dc.Label() != self.settings.DataChannelLabel {
						self.log.V(1).Infof("[peerconn]ignoring unexpected data channel label %q\n", dc.Label())
						// Installing a custom handler replaces Pion's default
						// handler. Close labels this transport does not consume.
						if err := dc.Close(); err != nil && self.log.V(1).Enabled() {
							self.log.Infof("[peerconn]unexpected data channel close err = %s\n", err)
						}
						return
					}
					dc.OnOpen(func() {
						self.runPionCallback("data channel open callback", func() {
							self.handleOpenDataChannel(dc)
						}, self.cancel)
					})
				}, self.cancel)
			})
		}
		return nil
	})
	if startupErr != nil {
		if self.ctx.Err() == nil {
			self.cancelBecause(startupErr)
		}
		return
	}
	if self.afterPionCallbacksRegisteredForTest != nil {
		self.afterPionCallbacksRegisteredForTest()
	}

	if self.active {
		var offer webrtc.SessionDescription
		err := self.withPionMutation(func() (err error) {
			offer, err = self.pc.CreateOffer(nil)
			return
		})
		if err != nil {
			if self.ctx.Err() == nil {
				self.cancelBecause(fmt.Errorf("create offer: %w", err))
			}
			return
		}
		if self.beforeSetLocalDescriptionForTest != nil {
			self.beforeSetLocalDescriptionForTest()
		}
		err = self.withPionMutation(func() error {
			return self.pc.SetLocalDescription(offer)
		})
		if err != nil {
			if self.ctx.Err() == nil {
				self.cancelBecause(fmt.Errorf("set local offer: %w", err))
			}
			return
		}
		offerBytes, err := json.Marshal(&offer)
		if err != nil {
			self.cancelBecause(fmt.Errorf("encode local offer: %w", err))
			return
		}
		startupSignal := &protocol.ExchangeSignal{
			SignalType: protocol.SignalType_SdpOffer,
			Sdp:        offerBytes,
		}
		self.setOfferSignal(startupSignal)
		// Mark only the first offer from this PeerConnection generation. A
		// remote passive association may still answer ICE consent after its
		// SCTP data plane went stale; ResetSignals distinguishes this fresh
		// generation from an ordinary duplicate/replay of our cached offer.
		self.sendSignalsWithReset([]*protocol.ExchangeSignal{startupSignal}, true)
	} else {
		// Signal receive can legitimately win the scheduler race and process an
		// offer before this Run goroutine starts. Avoid the now-redundant
		// startup frame in that common case. A remaining check/send race is
		// harmless because the generation marker lets the active peer identify
		// it as belonging to the answer it already accepted.
		self.signalLock.Lock()
		shouldSendWaiting := self.offerSignal() == nil
		self.signalLock.Unlock()
		if shouldSendWaiting {
			signal := &protocol.ExchangeSignal{
				SignalType: protocol.SignalType_WaitingForSdpOffer,
			}
			self.sendSignal(signal)
		}
	}

	select {
	case <-self.ctx.Done():
	}
}

// teardown releases resources owned by the peer generation. It deliberately
// does not wait for Run: SignalSender is synchronous intentional backpressure,
// so a canceled generation can still be returning from that callback. Closing
// the Pion stack is safe in parallel; Run observes its canceled context and
// exits without reusing the retired generation once the callback returns.
func (self *peerConn) teardown() {
	self.teardownOnce.Do(func() {
		if self.teardownDone != nil {
			defer close(self.teardownDone)
		}
		var teardownStage atomic.Int32
		teardownStage.Store(int32(peerConnectionTeardownStarting))
		slowTeardownTimer := startPeerConnectionTeardownWatchdog(
			peerConnectionSlowTeardownTimeout,
			&teardownStage,
			func(stage peerConnectionTeardownStage) {
				loggerOrDefault(self.log).Infof(
					"[peerconn]teardown stalled for %s at %s %s\n",
					peerConnectionSlowTeardownTimeout,
					stage,
					self.key,
				)
				logPeerConnectionTeardownStacks(
					loggerOrDefault(self.log),
					stage,
					self.key,
				)
			},
		)
		defer slowTeardownTimer.StopAndWait()
		self.cancel()
		// Direct peer owners and the manager lifecycle worker share teardown.
		// Closing the gate here makes callback rejection an invariant of the
		// resource owner rather than an assumption about its caller.
		self.lifecycleWorkers().close()
		if self.cancelIceResolve != nil {
			self.cancelIceResolve()
		}

		// Break the data transport before PeerConnection.Close starts its normal
		// SCTP-first shutdown. Pion's SCTP Abort waits for its read loop after
		// setting a deadline on the DTLS-backed net.Conn. On physical Android
		// that wait was observed stranded for hours after an idle peer vanished:
		// peerConns was empty while both make-before-break receive-window
		// reservations remained charged. Every later same-peer setup was then
		// refused by the full fixed budget. Stopping DTLS closes that SCTP-facing
		// connection before Close performs its idempotent component cleanup. Do
		// not stop ICE here: ICE Stop joins its agent and mux readers and was
		// observed blocking this pre-close stage indefinitely on Linux.
		func() {
			self.pionLifecycleLock.Lock()
			defer self.pionLifecycleLock.Unlock()
			if self.pc == nil {
				return
			}
			stopTransport := webRtcPeerConnectionTransportStop(self.pc)
			if err := closeTransportBeforePeerConnection(
				func() error {
					teardownStage.Store(int32(peerConnectionTeardownStoppingDtls))
					if stopTransport == nil {
						return nil
					}
					return stopTransport()
				},
				func() error {
					teardownStage.Store(int32(peerConnectionTeardownClosingPeer))
					return self.pc.Close()
				},
			); err != nil &&
				self.log.V(1).Enabled() {
				self.log.Infof("[peerconn]close err = %s\n", err)
			}
		}()

		teardownStage.Store(int32(peerConnectionTeardownClosingFastPath))
		self.closeFastPath()

		teardownStage.Store(int32(peerConnectionTeardownClosingDataChannel))
		var conn datachannel.ReadWriteCloserDeadliner
		self.stateLock.Lock()
		conn = self.conn
		self.conn = nil
		self.iceCandidateBuffer = nil
		if self.connected {
			self.connected = false
			self.connectedGeneration++
		}
		self.stateLock.Unlock()
		if conn != nil {
			_ = conn.SetReadDeadline(time.Now())
			_ = conn.SetWriteDeadline(time.Now())
			if err := conn.Close(); err != nil && self.log.V(1).Enabled() {
				self.log.Infof("[peerconn]detached data channel close err = %s\n", err)
			}
		}

		teardownStage.Store(int32(peerConnectionTeardownClearingSignals))
		self.signalLock.Lock()
		self.remoteIceCandidateBuffer = nil
		self.remoteIceCandidateBufferBytes = 0
		self.signalLock.Unlock()

		self.connMonitor.NotifyAll()
		self.connectedMonitor.NotifyAll()
		teardownStage.Store(int32(peerConnectionTeardownComplete))
	})
}

// closeTransportBeforePeerConnection is kept independent of Pion so the
// ordering and error behavior of the physical-stall fix are deterministic in
// tests. Both operations are attempted: a transport-stop error must not skip
// PeerConnection cleanup.
func closeTransportBeforePeerConnection(
	stopTransport func() error,
	closePeerConnection func() error,
) error {
	var errs []error
	if stopTransport != nil {
		if err := stopTransport(); err != nil {
			errs = append(errs, err)
		}
	}
	if closePeerConnection != nil {
		if err := closePeerConnection(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

func closeWebRtcPeerConnection(pc *webrtc.PeerConnection) error {
	if pc == nil {
		return nil
	}
	stopTransport := webRtcPeerConnectionTransportStop(pc)
	return closeTransportBeforePeerConnection(stopTransport, pc.Close)
}

func (self *peerConn) startConnectedDispatch() {
	self.connectedDispatch.Do(func() {
		// A single dispatch goroutine serializes callback invocation and emits
		// the latest state/generation, so flap-collapsing remains in order and
		// user callbacks never run on Pion's state-change goroutine.
		self.startWorker("connected dispatch", func() {
			for {
				notify := self.connectedMonitor.NotifyChannel()
				var current bool
				var generation uint64
				func() {
					self.stateLock.Lock()
					defer self.stateLock.Unlock()
					current = self.connected
					generation = self.connectedGeneration
				}()
				for _, callback := range self.connectedCallbacks.Get() {
					callback.deliver(generation, current)
				}
				select {
				case <-self.ctx.Done():
					return
				case <-notify:
				}
			}
		}, self.cancel)
	})
}

func (self *peerConn) handleICEConnectionState(state webrtc.ICEConnectionState) {
	connected := state == webrtc.ICEConnectionStateConnected ||
		state == webrtc.ICEConnectionStateCompleted
	if self.log.V(2).Enabled() {
		self.log.Infof("[peerconn]state=%v (%t)\n", state, connected)
	}
	self.setConnected(connected)
	if state == webrtc.ICEConnectionStateFailed ||
		state == webrtc.ICEConnectionStateClosed {
		// A failed ICE agent does not necessarily close PeerConnection or the
		// detached data channel. Without explicit cancellation the route
		// disappears but its peer slot/reservation and outer setup loop can
		// remain stranded forever.
		if self.ctx.Err() == nil {
			self.cancelBecause(fmt.Errorf("ICE connection state %s", state))
		}
	}
}

func (self *peerConn) handlePeerConnectionState(state webrtc.PeerConnectionState) {
	if state == webrtc.PeerConnectionStateFailed ||
		state == webrtc.PeerConnectionStateClosed {
		// Covers DTLS/SCTP failures that occur while ICE itself still reports
		// connected. Cancellation is idempotent with ICE and manager/network
		// change teardown.
		if self.ctx.Err() == nil {
			self.cancelBecause(fmt.Errorf("peer connection state %s", state))
		}
	}
}

func (self *peerConn) ReceiveSignalFromPeer(signal *protocol.ExchangeSignal) error {
	return self.receiveSignalFromPeer(signal, Id{}, false)
}

// Applies one signal using the latest reply lane for direct callers that do
// not carry callback metadata.
func (self *peerConn) receiveSignalFromPeer(
	signal *protocol.ExchangeSignal,
	senderGenerationId Id,
	senderGenerationSet bool,
) error {
	transferKey, transferKeySet := self.signalReplyKey()
	return self.receiveSignalFromPeerWithTransferKey(
		signal,
		senderGenerationId,
		senderGenerationSet,
		transferKey,
		transferKeySet,
	)
}

// Applies one signal with the immutable callback lane that received it.
func (self *peerConn) receiveSignalFromPeerWithTransferKey(
	signal *protocol.ExchangeSignal,
	senderGenerationId Id,
	senderGenerationSet bool,
	transferKey TransferKey,
	transferKeySet bool,
) error {
	if signal == nil {
		return nil
	}
	select {
	case <-self.ctx.Done():
		// A closed conn must treat late signals as a cheap no-op: the peer's
		// candidates/answers for a dead association are meaningless (the
		// outer transport recreates the conn, and fresh signals target the
		// replacement), and feeding them forward reaches a closed pion
		// agent — observed as a per-signal "the agent is closed" error loop
		// filling the stall window of a wedged attempt. The manager
		// deregisters a closed conn, but teardown is asynchronous: this
		// guard covers the closed-but-still-registered window. Returning nil
		// (not an error) keeps one dead conn from failing a batch that may
		// also carry signals the caller handles for other purposes.
		return nil
	default:
	}

	if self.beforeReceiveSignalLockForTest != nil {
		self.beforeReceiveSignalLockForTest()
	}
	var toSend []*protocol.ExchangeSignal
	var flushLocalCandidates bool
	var immediateReconnect bool
	var fatal bool
	var err error
	func() {
		// Pion lifecycle is the outer lock everywhere it composes with
		// generation signaling. Teardown needs only this lock while closing
		// Pion and therefore cannot form a signal->Pion lock cycle.
		self.pionLifecycleLock.Lock()
		defer self.pionLifecycleLock.Unlock()
		self.signalLock.Lock()
		defer self.signalLock.Unlock()
		if self.ctx != nil && self.ctx.Err() != nil {
			return
		}
		toSend, flushLocalCandidates, immediateReconnect, fatal, err =
			self.receiveSignalFromPeerLocked(
				signal,
				senderGenerationId,
				senderGenerationSet,
				transferKey,
				transferKeySet,
			)
	}()

	if err != nil {
		if fatal {
			self.requestImmediateReconnect()
			self.cancelBecause(fmt.Errorf("fatal remote signal: %w", err))
		}
		return err
	}
	// Keep the immediate call outside signalLock so a synchronous response
	// cannot form a lock cycle. The receive-originated transfer handoff itself
	// is marked zero-timeout and drops under congestion.
	if len(toSend) != 0 {
		self.sendSignalsWithTransferKey(
			toSend,
			false,
			true,
			transferKey,
			transferKeySet,
		)
	}
	if flushLocalCandidates {
		self.flushIceCandidatesNonBlocking()
	}
	if immediateReconnect {
		self.log.V(1).Infof("[peerconn]waiting-for-offer after answer; requesting immediate reconnect\n")
		self.requestImmediateReconnect()
		self.cancelBecause(errors.New("remote peer requested a fresh offer"))
	}
	return nil
}

// resetRemoteSignals applies ExchangeSignals.reset_signals. It returns true
// when this passive PeerConnection has already accepted an offer and therefore
// must be replaced rather than mutated in place. Pion does not support
// rewinding an established offer/answer/ICE/DTLS/SCTP stack to a pristine
// generation.
func (self *peerConn) resetRemoteSignals(
	senderGenerationId Id,
	senderGenerationSet bool,
) bool {
	self.signalLock.Lock()
	defer self.signalLock.Unlock()
	if self.ctx != nil && self.ctx.Err() != nil {
		return false
	}

	if self.active {
		return false
	}
	if self.offerSignal() != nil {
		if senderGenerationSet &&
			self.remoteSignalGenerationSet &&
			senderGenerationId == self.remoteSignalGeneration {
			// Transfer signaling is reliable and may retransmit the first
			// reset-marked offer before its ACK. It is a duplicate of the
			// accepted generation, not permission to destroy and recreate the
			// passive association.
			return false
		}
		return true
	}
	// A fresh passive connection can accept the reset's offer directly, but
	// any candidates received before the generation boundary belong to the
	// abandoned association and must not be applied to it.
	self.remoteIceCandidateBuffer = nil
	self.remoteIceCandidateBufferBytes = 0
	self.remoteIceCandidateOverflowLog = false
	return false
}

// Advances Pion signaling while signalLock serializes one peer generation.
func (self *peerConn) receiveSignalFromPeerLocked(
	signal *protocol.ExchangeSignal,
	senderGenerationId Id,
	senderGenerationSet bool,
	transferKey TransferKey,
	transferKeySet bool,
) (
	toSend []*protocol.ExchangeSignal,
	flushLocalCandidates bool,
	immediateReconnect bool,
	fatal bool,
	err error,
) {
	switch signal.SignalType {
	case protocol.SignalType_SdpOffer:
		if self.active {
			return
		}
		if self.offerSignal() != nil {
			// already accepted an offer; ignore the duplicate
			return
		}
		// Decode and apply before committing our cached offer. A malformed or
		// semantically invalid first frame must not poison later retransmits.
		var offer webrtc.SessionDescription
		if err = json.Unmarshal(signal.Sdp, &offer); err != nil {
			return
		}
		if err = self.pc.SetRemoteDescription(offer); err != nil {
			return
		}
		self.setIceCandidateReplyKey(transferKey, transferKeySet)
		self.setOfferSignal(signal)
		self.remoteSignalGeneration = senderGenerationId
		self.remoteSignalGenerationSet = senderGenerationSet
		self.remoteDescriptionSet = true
		self.flushRemoteIceCandidatesLocked()

		var answer webrtc.SessionDescription
		answer, err = self.pc.CreateAnswer(nil)
		if err != nil {
			fatal = true
			return
		}
		if err = self.pc.SetLocalDescription(answer); err != nil {
			fatal = true
			return
		}
		var answerBytes []byte
		answerBytes, err = json.Marshal(&answer)
		if err != nil {
			fatal = true
			return
		}
		answerSignal := &protocol.ExchangeSignal{
			SignalType: protocol.SignalType_SdpAnswer,
			Sdp:        answerBytes,
		}
		self.setAnswerSignal(answerSignal)
		toSend = []*protocol.ExchangeSignal{answerSignal}
		flushLocalCandidates = true

	case protocol.SignalType_SdpAnswer:
		if !self.active {
			return
		}
		if self.answerSignal() != nil {
			// already accepted an answer; ignore the duplicate
			return
		}
		// As with offers, do not cache an invalid first answer.
		var answer webrtc.SessionDescription
		if err = json.Unmarshal(signal.Sdp, &answer); err != nil {
			return
		}
		if err = self.pc.SetRemoteDescription(answer); err != nil {
			return
		}
		self.setIceCandidateReplyKey(transferKey, transferKeySet)
		self.setAnswerSignal(signal)
		self.remoteSignalGeneration = senderGenerationId
		self.remoteSignalGenerationSet = senderGenerationSet
		self.remoteDescriptionSet = true
		self.flushRemoteIceCandidatesLocked()
		flushLocalCandidates = true

	case protocol.SignalType_IceCandidate:
		var candidate webrtc.ICECandidateInit
		if err = json.Unmarshal(signal.IceCandidate, &candidate); err != nil {
			return
		}
		if !self.remoteDescriptionSet {
			self.bufferRemoteIceCandidateLocked(
				candidate,
				senderGenerationId,
				senderGenerationSet,
			)
			return
		}
		if self.remoteSignalGenerationSet &&
			senderGenerationSet &&
			senderGenerationId != self.remoteSignalGeneration {
			// A replacement peer can reuse the same stream key while delayed
			// candidate batches from the retired generation are still in the
			// transfer queue. Applying their ICE credentials/addresses to the
			// new agent creates useless pairs and can prevent prompt setup.
			return
		}
		self.addRemoteIceCandidateLocked(candidate)

	case protocol.SignalType_WaitingForSdpOffer:
		if !self.active {
			break
		}
		if self.answerSignal() == nil {
			// not yet negotiated; re-send our cached offer
			if signal := self.offerSignal(); signal != nil {
				toSend = []*protocol.ExchangeSignal{signal}
			}
		} else if senderGenerationSet &&
			self.remoteSignalGenerationSet &&
			senderGenerationId == self.remoteSignalGeneration {
			// The passive Run goroutine can be scheduled after its signal
			// receiver has already processed our offer and sent an answer.
			// Its delayed initial WaitingForSdpOffer is from the same remote
			// generation, not a restart. Network delivery can reorder these
			// messages too, so generation identity—not timing—is the safe
			// discriminator.
		} else {
			// peer is asking for a fresh offer despite our prior answer.
			// they likely restarted; signal the outer transport to reconnect
			// without backoff, then cancel.
			immediateReconnect = true
		}
	}
	return
}

func iceCandidateInitByteCount(candidate webrtc.ICECandidateInit) int {
	n := len(candidate.Candidate)
	if candidate.SDPMid != nil {
		n += len(*candidate.SDPMid)
	}
	if candidate.UsernameFragment != nil {
		n += len(*candidate.UsernameFragment)
	}
	return n
}

// signalLock must be held.
func (self *peerConn) bufferRemoteIceCandidateLocked(
	candidate webrtc.ICECandidateInit,
	senderGenerationId Id,
	senderGenerationSet bool,
) {
	byteCount := iceCandidateInitByteCount(candidate)
	if maxBufferedRemoteIceCandidateCount <= len(self.remoteIceCandidateBuffer) ||
		maxBufferedRemoteIceCandidateBytes < self.remoteIceCandidateBufferBytes+byteCount {
		if !self.remoteIceCandidateOverflowLog {
			self.remoteIceCandidateOverflowLog = true
			self.log.Infof(
				"[peerconn]remote ICE pre-SDP buffer full (count=%d bytes=%d); dropping excess candidates\n",
				len(self.remoteIceCandidateBuffer),
				self.remoteIceCandidateBufferBytes,
			)
		}
		return
	}
	self.remoteIceCandidateBuffer = append(self.remoteIceCandidateBuffer, remoteIceCandidate{
		value:               candidate,
		senderGenerationId:  senderGenerationId,
		senderGenerationSet: senderGenerationSet,
	})
	self.remoteIceCandidateBufferBytes += byteCount
}

// signalLock must be held.
func (self *peerConn) addRemoteIceCandidateLocked(candidate webrtc.ICECandidateInit) {
	// A malformed individual candidate should not discard a valid SDP
	// negotiation or other paths. Pion also treats several unsupported
	// candidate forms as a deliberate no-op.
	if err := self.pc.AddICECandidate(candidate); err != nil && self.log.V(1).Enabled() {
		self.log.Infof("[peerconn]AddICECandidate err = %s\n", err)
	}
}

// signalLock must be held.
func (self *peerConn) flushRemoteIceCandidatesLocked() {
	candidates := self.remoteIceCandidateBuffer
	self.remoteIceCandidateBuffer = nil
	self.remoteIceCandidateBufferBytes = 0
	for _, candidate := range candidates {
		if self.remoteSignalGenerationSet &&
			candidate.senderGenerationSet &&
			candidate.senderGenerationId != self.remoteSignalGeneration {
			continue
		}
		self.addRemoteIceCandidateLocked(candidate.value)
	}
}

// Sends one gathered candidate with the negotiation's reply-key snapshot.
func (self *peerConn) sendIceCandidate(candidate *webrtc.ICECandidate) {
	self.sendIceCandidates([]*webrtc.ICECandidate{candidate})
}

// Serializes gathered candidates with one reply-key snapshot per batch.
func (self *peerConn) sendIceCandidates(candidates []*webrtc.ICECandidate) {
	self.sendIceCandidatesWithOpts(candidates, false)
}

// Serializes gathered candidates with the currently pinned negotiation lane.
func (self *peerConn) sendIceCandidatesWithOpts(candidates []*webrtc.ICECandidate, nonBlocking bool) {
	transferKey, transferKeySet := self.iceCandidateReplyKey()
	self.sendIceCandidatesWithTransferKey(candidates, transferKey, transferKeySet, nonBlocking)
}

// Serializes gathered candidates with an immutable negotiation lane.
func (self *peerConn) sendIceCandidatesWithTransferKey(
	candidates []*webrtc.ICECandidate,
	transferKey TransferKey,
	transferKeySet bool,
	nonBlocking bool,
) {
	signals := make([]*protocol.ExchangeSignal, 0, min(len(candidates), maxIceCandidatesPerSignalFrame))
	signalBytes := 0
	flush := func() {
		if len(signals) != 0 {
			self.sendSignalsWithTransferKey(
				signals,
				false,
				nonBlocking,
				transferKey,
				transferKeySet,
			)
			signals = make([]*protocol.ExchangeSignal, 0, min(len(candidates), maxIceCandidatesPerSignalFrame))
			signalBytes = 0
		}
	}
	for _, candidate := range candidates {
		if candidate == nil {
			continue
		}
		candidateBytes, err := json.Marshal(candidate.ToJSON())
		if err != nil {
			continue
		}
		if 0 < len(signals) &&
			(maxIceCandidatesPerSignalFrame <= len(signals) ||
				maxIceCandidateBytesPerSignalFrame < signalBytes+len(candidateBytes)) {
			// Keep signal frames bounded. If an unusual multihomed server
			// gathers many candidates, additional frames flow through the
			// normal synchronous send callback/backpressure rather than
			// creating one oversized protobuf.
			flush()
		}
		signals = append(signals, &protocol.ExchangeSignal{
			SignalType:   protocol.SignalType_IceCandidate,
			IceCandidate: candidateBytes,
		})
		signalBytes += len(candidateBytes)
	}
	flush()
}

// Marks buffered local candidates ready and sends them with one lane snapshot.
func (self *peerConn) flushIceCandidates() {
	self.flushIceCandidatesWithOpts(false)
}

// flushIceCandidatesNonBlocking is the receive-path variant: the flush send
// uses timeout 0 (see signalSendNonBlocking).
func (self *peerConn) flushIceCandidatesNonBlocking() {
	self.flushIceCandidatesWithOpts(true)
}

// Publishes buffered candidates with the SDP generation's immutable lane.
func (self *peerConn) flushIceCandidatesWithOpts(nonBlocking bool) {
	var toSend []*webrtc.ICECandidate
	var transferKey TransferKey
	var transferKeySet bool
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		self.iceCandidatesReady = true
		toSend = self.iceCandidateBuffer
		self.iceCandidateBuffer = nil
		if !self.iceCandidateReplyTransferKeySet && self.signalReplyTransferKeySet {
			self.iceCandidateReplyTransferKey = self.signalReplyTransferKey
			self.iceCandidateReplyTransferKeySet = true
		}
		transferKey = self.iceCandidateReplyTransferKey
		transferKeySet = self.iceCandidateReplyTransferKeySet
		if !transferKeySet {
			transferKey = self.signalReplyTransferKey
			transferKeySet = self.signalReplyTransferKeySet
		}
	}()
	// Candidates gathered before SDP readiness are already adjacent and
	// share one destination. Send one protobuf frame instead of one transfer
	// callback/frame per interface, cutting allocations and queue pressure
	// without adding a timer or delaying late trickle candidates.
	self.sendIceCandidatesWithTransferKey(toSend, transferKey, transferKeySet, nonBlocking)
}

// Buffers a local candidate until SDP is ready or sends it using the pinned
// negotiation lane. Pion may invoke this callback after later signals arrive.
func (self *peerConn) handleLocalIceCandidate(candidate *webrtc.ICECandidate) {
	if candidate == nil {
		return
	}
	if self.beforeIceCandidateStateLockForTest != nil {
		self.beforeIceCandidateStateLockForTest()
	}
	var send bool
	var transferKey TransferKey
	var transferKeySet bool
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		if self.ctx != nil && self.ctx.Err() != nil {
			return
		}
		if self.iceCandidatesReady {
			send = true
			transferKey = self.iceCandidateReplyTransferKey
			transferKeySet = self.iceCandidateReplyTransferKeySet
			if !transferKeySet {
				transferKey = self.signalReplyTransferKey
				transferKeySet = self.signalReplyTransferKeySet
			}
		} else {
			self.iceCandidateBuffer = append(self.iceCandidateBuffer, candidate)
		}
	}()
	if send {
		// Pion invokes this inline on its gathering path. It is a receive/event
		// callback, so a full Transfer queue must drop the candidate rather than
		// park Pion and its other associations (CODESTYLE.md).
		self.sendIceCandidatesWithTransferKey(
			[]*webrtc.ICECandidate{candidate},
			transferKey,
			transferKeySet,
			true,
		)
	}
}

// ImmediateReconnect returns a persistent one-shot channel that closes when
// the outer transport should reconnect without backoff.
func (self *peerConn) ImmediateReconnect() <-chan struct{} {
	return self.immediateReconnect
}

func (self *peerConn) requestImmediateReconnect() {
	self.immediateReconnectOnce.Do(func() {
		close(self.immediateReconnect)
	})
}

func (self *peerConn) Connected() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	return self.connected
}

func (self *peerConn) setConnected(connected bool) {
	changed := false
	if self.beforeConnectedStateLockForTest != nil {
		self.beforeConnectedStateLockForTest()
	}

	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		if connected && self.ctx != nil && self.ctx.Err() != nil {
			return
		}

		if self.connected != connected {
			self.connected = connected
			self.connectedGeneration++
			changed = true
		}
	}()

	if changed {
		if connected {
			self.startFastPathWarmup()
		}
		if connected && self.log.V(1).Enabled() {
			// One low-frequency line identifies whether the formed route is
			// direct and which address family won ICE. This is essential when
			// a nominally connected network peer is actually using a slow
			// server-reflexive path; keep it off the packet hot path.
			self.log.Infof(
				"[peerconn]connected %s local=%s remote=%s\n",
				self.key,
				self.LocalAddr(),
				self.RemoteAddr(),
			)
		}
		// signal the dispatch goroutine; it will read the latest state
		// under the lock and serialize callback invocation. this avoids
		// out-of-order observation if two setConnected calls race.
		self.connectedMonitor.NotifyAll()
	}
}

func (self *peerConn) AddConnectedCallback(callbackFunc func(connected bool)) func() {
	callback := &connectedCallback{
		callback: callbackFunc,
	}
	callbackId := self.connectedCallbacks.Add(callback)
	self.startConnectedDispatch()
	// Fire current state so a late subscriber doesn't miss a transition that
	// the dispatch goroutine has already emitted. Generation-aware delivery
	// makes this safe if a newer transition is concurrently delivered first.
	self.stateLock.Lock()
	connected := self.connected
	generation := self.connectedGeneration
	self.stateLock.Unlock()
	callback.deliver(generation, connected)
	return func() {
		callback.close()
		self.connectedCallbacks.Remove(callbackId)
	}
}

type connectedCallback struct {
	lock           sync.Mutex
	callback       func(bool)
	lastGeneration uint64
	delivered      bool
	closed         atomic.Bool
}

func (self *connectedCallback) deliver(generation uint64, connected bool) {
	if self.closed.Load() {
		return
	}
	self.lock.Lock()
	defer self.lock.Unlock()
	if self.closed.Load() || (self.delivered && generation <= self.lastGeneration) {
		return
	}
	self.delivered = true
	self.lastGeneration = generation
	HandleError(func() {
		self.callback(connected)
	})
}

func (self *connectedCallback) close() {
	// Unsubscribe must not wait on arbitrary callback work. In particular a
	// callback is allowed to unsubscribe itself, and teardown must still make
	// progress if a route observer is parked. A delivery already in progress
	// may finish; every later generation observes closed and is dropped.
	self.closed.Store(true)
}

func (self *peerConn) setOfferSignal(offer *protocol.ExchangeSignal) bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.offer != nil {
		return false
	}
	self.offer = offer
	return true
}

func (self *peerConn) offerSignal() *protocol.ExchangeSignal {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.offer
}

func (self *peerConn) setAnswerSignal(answer *protocol.ExchangeSignal) bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.answer != nil {
		return false
	}
	self.answer = answer
	return true
}

func (self *peerConn) answerSignal() *protocol.ExchangeSignal {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.answer
}

// setSignalReplyTransferKey records the receiver-visible lane/session before
// processing a signal that may synchronously produce a response.
func (self *peerConn) setSignalReplyTransferKey(transferKey TransferKey) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.ctx != nil && self.ctx.Err() != nil {
		return
	}
	self.signalReplyTransferKey = transferKey
	self.signalReplyTransferKeySet = true
}

// signalReplyKey returns one stable snapshot without holding state across a send.
func (self *peerConn) signalReplyKey() (TransferKey, bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.signalReplyTransferKey, self.signalReplyTransferKeySet
}

// Pins the reply lane to the SDP negotiation that produces local candidates.
func (self *peerConn) setIceCandidateReplyKey(transferKey TransferKey, transferKeySet bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.iceCandidateReplyTransferKey = transferKey
	self.iceCandidateReplyTransferKeySet = transferKeySet
}

// Returns the pinned candidate lane, falling back only before SDP has bound it.
func (self *peerConn) iceCandidateReplyKey() (TransferKey, bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.iceCandidateReplyTransferKeySet {
		return self.iceCandidateReplyTransferKey, true
	}
	return self.signalReplyTransferKey, self.signalReplyTransferKeySet
}

// Sends one signal using a stable reply-key snapshot.
func (self *peerConn) sendSignal(signal *protocol.ExchangeSignal) {
	self.sendSignals([]*protocol.ExchangeSignal{signal})
}

// Sends one batch using a stable reply-key snapshot.
func (self *peerConn) sendSignals(signalValues []*protocol.ExchangeSignal) {
	self.sendSignalsWithOpts(signalValues, false, false)
}

// sendSignalsNonBlocking is the receive-path variant: the send uses timeout 0
// (see signalSendNonBlocking).
func (self *peerConn) sendSignalsNonBlocking(signalValues []*protocol.ExchangeSignal) {
	self.sendSignalsWithOpts(signalValues, false, true)
}

// Sends one batch with a fresh-generation marker when requested.
func (self *peerConn) sendSignalsWithReset(signalValues []*protocol.ExchangeSignal, resetSignals bool) {
	self.sendSignalsWithOpts(signalValues, resetSignals, false)
}

// Snapshots the current immediate-reply lane before building a signal frame.
func (self *peerConn) sendSignalsWithOpts(signalValues []*protocol.ExchangeSignal, resetSignals bool, nonBlocking bool) {
	transferKey, transferKeySet := self.signalReplyKey()
	self.sendSignalsWithTransferKey(
		signalValues,
		resetSignals,
		nonBlocking,
		transferKey,
		transferKeySet,
	)
}

// Builds and sends one signal frame using an immutable logical reply lane.
func (self *peerConn) sendSignalsWithTransferKey(
	signalValues []*protocol.ExchangeSignal,
	resetSignals bool,
	nonBlocking bool,
	transferKey TransferKey,
	transferKeySet bool,
) {
	if len(signalValues) == 0 {
		return
	}
	signals := &protocol.ExchangeSignals{
		StreamId:           self.key.StreamId.Bytes(),
		ResetSignals:       resetSignals,
		Signals:            signalValues,
		SenderGenerationId: self.signalGeneration.Bytes(),
	}
	// passive peers send signals in the return direction of the stream,
	// so they ask for a companion contract (verified as
	// ProvideMode_Stream). this is what the destination is willing to
	// accept in the asymmetric case where it only enables Stream for
	// return traffic.
	//
	// active peers ride the stream itself (`ForceStream`): p2p exists only
	// for a stream, and a network peer's data path is ForceStream (the
	// multi-client forces AllowDirect). The peer is frequently an ephemeral
	// per-window provider client that grants ONLY the stream contract for
	// this pair — the initiator has no plain contract to it — so a
	// default-opts (plain contract) offer is undeliverable while data flows
	// fine over the stream. Sending the offer with ForceStream routes it onto
	// the same stream contract the data uses, so it reaches the peer. This is
	// the §16-class alignment: a send-sequence-keying option invisible to the
	// signaling layer must match the data path or the signal forks onto an
	// undeliverable sequence. See PACKETRESEARCH1 §17.
	var opts []any
	if transferKeySet {
		opts = append(opts, transferKey)
	}
	if self.active {
		opts = append(
			opts,
			transferOptionsSetForceStream{ForceStream: true},
			transferOptionsSetCompanionContract{CompanionContract: false},
		)
	} else {
		opts = append(
			opts,
			transferOptionsSetForceStream{ForceStream: false},
			transferOptionsSetCompanionContract{CompanionContract: true},
		)
	}
	// A full transfer send queue is intentional backpressure while this peer
	// generation is live. Bind that wait to the generation, not the entire
	// client: replacement/cancellation must release an obsolete offer or ICE
	// candidate instead of pinning its Run goroutine and admission forever.
	if self.ctx != nil {
		opts = append(opts, Ctx(self.ctx))
	}
	if nonBlocking {
		opts = append(opts, signalSendNonBlocking{})
	}
	self.signalSender.SendSignal(
		self.key.PeerId,
		RequireToFrameWithDefaultProtocolVersion(signals),
		opts...,
	)
}

func (self *peerConn) handleOpenDataChannel(dc *webrtc.DataChannel) {
	if err := self.setOpenDataChannel(dc); err != nil {
		self.log.V(1).Infof("[peerconn]data channel detach err = %s\n", err)
		self.requestImmediateReconnect()
		self.cancelBecause(fmt.Errorf("data channel detach: %w", err))
	}
}

func (self *peerConn) setOpenDataChannel(dc *webrtc.DataChannel) error {
	conn, err := detachWithDeadline(dc)
	if err != nil {
		return err
	}
	self.installOpenDataChannel(conn)
	return nil
}

// installOpenDataChannel publishes one already-detached connection only while
// its peer generation remains live. A callback admitted before cancellation
// may reach this boundary after teardown cleared the old connection; reject
// and close that late resource instead of repopulating retired state.
func (self *peerConn) installOpenDataChannel(conn datachannel.ReadWriteCloserDeadliner) bool {
	if self.beforeOpenDataChannelStateLockForTest != nil {
		self.beforeOpenDataChannelStateLockForTest()
	}

	accepted := false
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		if (self.ctx == nil || self.ctx.Err() == nil) && self.conn == nil {
			self.conn = conn
			accepted = true
			self.connMonitor.NotifyAll()
		}
	}()
	if !accepted {
		// One peer connection carries one transport. A duplicate or a late
		// callback must not replace the live/retired generation's connection.
		_ = conn.Close()
	}
	return accepted
}

func (self *peerConn) dataChannelConn(deadline time.Time) (datachannel.ReadWriteCloserDeadliner, error) {
	conn := func() (datachannel.ReadWriteCloserDeadliner, chan struct{}) {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		return self.conn, self.connMonitor.NotifyChannel()
	}
	var deadlineTimer *time.Timer
	var deadlineC <-chan time.Time
	if !deadline.IsZero() {
		timeout := time.Until(deadline)
		if timeout <= 0 {
			return nil, os.ErrDeadlineExceeded
		}
		deadlineTimer = time.NewTimer(timeout)
		deadlineC = deadlineTimer.C
		defer deadlineTimer.Stop()
	}
	for {
		c, update := conn()
		if c != nil {
			return c, nil
		}
		select {
		case <-self.ctx.Done():
			return nil, os.ErrClosed
		case <-update:
		case <-deadlineC:
			return nil, os.ErrDeadlineExceeded
		}
	}
}

func (self *peerConn) Read(b []byte) (n int, err error) {
	var deadline time.Time
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		deadline = self.readDeadline
	}()
	var c datachannel.ReadWriteCloserDeadliner
	c, err = self.dataChannelConn(deadline)
	if err != nil {
		return
	}
	c.SetReadDeadline(deadline)
	n, err = c.Read(b)
	return
}

func (self *peerConn) noteOutboundSctpActivity() {
	if self.settings.SctpNoProgressTimeout <= 0 {
		return
	}
	self.progressWatchOnce.Do(func() {
		self.startWorker("SCTP progress watchdog", self.runSctpProgressWatchdog, self.cancel)
	})
	select {
	case self.outboundProgress <- struct{}{}:
	default:
	}
}

// acknowledgedSctpByteCount derives definite forward progress from accepted
// user bytes and Pion's pending+in-flight byte count. Reverse packet traffic is
// deliberately absent: heartbeats or unrelated inbound DATA can continue on a
// half-open association without acknowledging the outbound queue.
func acknowledgedSctpByteCount(outboundByteCount uint64, bufferedAmount int) uint64 {
	if bufferedAmount <= 0 {
		return outboundByteCount
	}
	bufferedByteCount := uint64(bufferedAmount)
	if outboundByteCount <= bufferedByteCount {
		return 0
	}
	return outboundByteCount - bufferedByteCount
}

func observeAcknowledgedSctpByteCount(
	previousAcknowledgedByteCount uint64,
	outboundByteCount uint64,
	bufferedAmount int,
) (acknowledgedByteCount uint64, progressed bool) {
	acknowledgedByteCount = acknowledgedSctpByteCount(
		outboundByteCount,
		bufferedAmount,
	)
	if previousAcknowledgedByteCount < acknowledgedByteCount {
		return acknowledgedByteCount, true
	}
	// Accepted writes and aggregate queue observations can race. Definite
	// acknowledgement is monotonic, so never move the observation backward.
	return previousAcknowledgedByteCount, false
}

// runSctpProgressWatchdog closes a half-open data plane that ICE consent
// cannot see. Pion's reliable SCTP association retries indefinitely; if the
// remote SCTP endpoint disappears while its ICE agent/socket still answers,
// PeerConnection remains "connected" and a small first write after an idle
// period can otherwise sit unacknowledged forever.
//
// The worker starts only after the first successful application write. It has
// no idle ticker: while the SCTP buffered amount is zero it waits on the
// coalescing outbound-activity channel.
// With data outstanding, only a decrease in pending+in-flight user bytes
// refreshes the deadline. Counting arbitrary reverse SCTP packets lets
// heartbeats or unrelated inbound DATA mask a permanently unacknowledged send.
// A zero peer receive window pauses the deadline because it is the transport
// representation of intentional receiver callback backpressure. This observes
// transport failure without putting a timeout around that backpressure.
func (self *peerConn) runSctpProgressWatchdog() {
	timeout := self.settings.SctpNoProgressTimeout
	sampleInterval := min(250*time.Millisecond, timeout/4)
	if sampleInterval <= 0 {
		sampleInterval = time.Millisecond
	}
	var sampleTimer *time.Timer
	defer func() {
		if sampleTimer != nil {
			sampleTimer.Stop()
		}
	}()

	for {
		select {
		case <-self.ctx.Done():
			return
		case <-self.outboundProgress:
		}

		bufferedAmount, ok := webRtcSctpBufferedAmount(self.pc)
		if !ok {
			continue
		}
		acknowledgedByteCount := acknowledgedSctpByteCount(
			self.outboundByteCount.Load(),
			bufferedAmount,
		)
		lastProgress := time.Now()
		for 0 < bufferedAmount {
			remaining := timeout - time.Since(lastProgress)
			if remaining <= 0 {
				// The timer is deliberately based on the last observed ACK,
				// but the buffered amount above came from the preceding
				// sample. Take one current sample before declaring failure.
				// Otherwise an ACK in the final sampling interval—or while
				// this goroutine was descheduled—can be ignored and a healthy
				// association torn down from stale queue state.
				bufferedAmount, ok = webRtcSctpBufferedAmount(self.pc)
				if !ok {
					break
				}
				nextAcknowledgedByteCount, progressed :=
					observeAcknowledgedSctpByteCount(
						acknowledgedByteCount,
						self.outboundByteCount.Load(),
						bufferedAmount,
					)
				if progressed {
					acknowledgedByteCount = nextAcknowledgedByteCount
					lastProgress = time.Now()
					continue
				}
				if bufferedAmount <= 0 {
					break
				}
				receiverWindow, receiverWindowOk :=
					webRtcSctpReceiverWindow(self.pc)
				if receiverWindowOk && receiverWindow == 0 {
					// The remote SCTP stack is alive and explicitly asking us
					// to stop because its bounded receive queue is full.
					// Treating this as a blackhole would turn an intentionally
					// stalled transfer callback into connection churn and data
					// replay. This slower stats query runs only at the timeout
					// boundary, not on every progress sample.
					lastProgress = time.Now()
					continue
				}
				noProgressErr := fmt.Errorf(
					"SCTP no progress for %s with %d bytes buffered and rwnd=%d",
					timeout,
					bufferedAmount,
					receiverWindow,
				)
				self.log.Infof(
					"[peerconn]%s %s; reconnecting\n",
					noProgressErr,
					self.key,
				)
				self.requestImmediateReconnect()
				// Use the common failure boundary so the exact cause survives
				// cancellation and shared admission sees this owner retiring
				// synchronously, before the teardown worker is scheduled.
				self.cancelBecause(noProgressErr)
				return
			}

			sample := min(sampleInterval, remaining)
			sampleC := resetOrCreateTimer(&sampleTimer, sample)
			select {
			case <-self.ctx.Done():
				return
			case <-sampleC:
			}

			bufferedAmount, ok = webRtcSctpBufferedAmount(self.pc)
			if !ok {
				break
			}
			nextAcknowledgedByteCount, progressed :=
				observeAcknowledgedSctpByteCount(
					acknowledgedByteCount,
					self.outboundByteCount.Load(),
					bufferedAmount,
				)
			if progressed {
				acknowledgedByteCount = nextAcknowledgedByteCount
				lastProgress = time.Now()
			}
		}
	}
}

func (self *peerConn) Write(b []byte) (n int, err error) {
	var deadline time.Time
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		deadline = self.writeDeadline
	}()
	var c datachannel.ReadWriteCloserDeadliner
	c, err = self.dataChannelConn(deadline)
	if err != nil {
		return
	}
	c.SetWriteDeadline(deadline)
	n, err = c.Write(b)
	if 0 < n {
		self.outboundByteCount.Add(uint64(n))
		self.noteOutboundSctpActivity()
	}
	return
}

// LocalAddr returns the local network address, if known.
func (self *peerConn) LocalAddr() net.Addr {
	sctp := self.pc.SCTP()
	if sctp == nil {
		return newWebRtcAddr("")
	}
	dtls := sctp.Transport()
	if dtls == nil {
		return newWebRtcAddr("")
	}
	ice := dtls.ICETransport()
	if ice == nil {
		return newWebRtcAddr("")
	}
	pair, err := ice.GetSelectedCandidatePair()
	if err != nil || pair == nil {
		return newWebRtcAddr("")
	}
	return newWebRtcAddr(pair.Local.Address)
}

// RemoteAddr returns the remote network address, if known.
func (self *peerConn) RemoteAddr() net.Addr {
	sctp := self.pc.SCTP()
	if sctp == nil {
		return newWebRtcAddr("")
	}
	dtls := sctp.Transport()
	if dtls == nil {
		return newWebRtcAddr("")
	}
	ice := dtls.ICETransport()
	if ice == nil {
		return newWebRtcAddr("")
	}
	pair, err := ice.GetSelectedCandidatePair()
	if err != nil || pair == nil {
		return newWebRtcAddr("")
	}
	return newWebRtcAddr(pair.Remote.Address)
}

func (self *peerConn) SetDeadline(t time.Time) error {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.readDeadline = t
	self.writeDeadline = t

	return nil
}

func (self *peerConn) SetReadDeadline(t time.Time) error {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.readDeadline = t

	return nil
}

func (self *peerConn) SetWriteDeadline(t time.Time) error {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.writeDeadline = t

	return nil
}

func (self *peerConn) Close() error {
	self.cancelBecause(errors.New("local peer connection close"))
	return nil
}

func (self *peerConn) Cancel() {
	self.cancelBecause(errors.New("manager peer connection cancel"))
}

func (self *peerConn) cancelBecause(err error) {
	if err == nil {
		err = context.Canceled
	}
	self.admissionOwner.markRetiring()
	if self.cancelCause != nil {
		self.cancelCause(err)
	} else if self.cancel != nil {
		// Compatibility for lightweight test/fake peerConn values that only
		// install the historical CancelFunc.
		self.cancel()
	}
}

// conforms to `net.Addr`
type webRtcAddr struct {
	addr string
}

func newWebRtcAddr(addr string) net.Addr {
	return &webRtcAddr{
		addr: addr,
	}
}

func (self *webRtcAddr) Network() string {
	return "udp"
}

func (self *webRtcAddr) String() string {
	return self.addr
}
