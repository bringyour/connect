// Stream controls publish bounded lifecycle work; receive callbacks cancel or
// enqueue, while one owned worker performs generation-ordered blocking joins.
package connect

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

func DefaultStreamManagerSettings() *StreamManagerSettings {
	return &StreamManagerSettings{
		StreamBufferSettings: DefaultStreamBufferSettings(),
		// WebRtcSettings:       DefaultWebRtcSettings(),
	}
}

func DefaultStreamBufferSettings() *StreamBufferSettings {
	return &StreamBufferSettings{
		ReadTimeout:          time.Duration(-1),
		WriteTimeout:         time.Duration(-1),
		P2pTransportSettings: DefaultP2pTransportSettings(),
	}
}

type StreamManagerSettings struct {
	StreamBufferSettings *StreamBufferSettings
}

type StreamManager struct {
	ctx context.Context

	client *Client

	webRtcManager *WebRtcManager

	streamBuffer *StreamBuffer

	streamManagerSettings *StreamManagerSettings
	// A provider-owned client must retire every disallowed P2P stream
	// direction. An ordinary destination client preserves all StreamOpen
	// directions because they are transport state, not provider policy.
	providerStreamPolicy bool

	// A refused StreamOpen would otherwise be terminal for the resident's
	// lifetime, so the first refusal is reported at default verbosity. Later
	// refusals stay at V(1) to keep a churning peer off the hot log path.
	rejectedStreamLogOnce sync.Once

	rejectedStreamsLock sync.Mutex
	rejectedStreams     map[Id]*rejectedStreamOpen
}

// Close retires stream admission and requests teardown without waiting.
func (self *StreamManager) Close() {
	self.streamBuffer.Close()
}

// closeAndWait retires every stream generation and joins its lifecycle and
// transport workers, or returns when ctx expires.
func (self *StreamManager) closeAndWait(ctx context.Context) error {
	self.Close()
	return self.streamBuffer.closeAndWait(ctx)
}

func NewStreamManager(ctx context.Context, client *Client, webRtcManager *WebRtcManager, streamManagerSettings *StreamManagerSettings) *StreamManager {
	streamManager := &StreamManager{
		ctx:                   ctx,
		client:                client,
		streamManagerSettings: streamManagerSettings,
		providerStreamPolicy:  client.settings.ProviderStreamPolicy,
		rejectedStreams:       map[Id]*rejectedStreamOpen{},
	}

	// webRtcManager := NewWebRtcManager(ctx, streamManagerSettings.WebRtcSettings)

	streamManager.initBuffers(webRtcManager)

	return streamManager
}

func (self *StreamManager) initBuffers(webRtcManager *WebRtcManager) {
	self.webRtcManager = webRtcManager
	self.streamBuffer = NewStreamBuffer(self.ctx, self, self.streamManagerSettings.StreamBufferSettings)
}

func (self *StreamManager) Client() *Client {
	return self.client
}

func (self *StreamManager) WebRtcManager() *WebRtcManager {
	return self.webRtcManager
}

// allowStreamOpen rejects provider/relay work when this client no longer
// advertises the corresponding provide mode. A source id means traffic enters
// this client from a peer: source-only is a provider endpoint, while source +
// destination is an intermediary relay. Only destination-only streams are
// outbound client work and independent of this client's provide policy.
func (self *StreamManager) allowStreamOpen(sourceId *Id, destinationId *Id) bool {
	if !self.providerStreamPolicy {
		// An ordinary destination/window client does not serve provider work.
		// Its source-only, destination-only, and paired entries are transport
		// directions for its own connection, including return/companion state
		// restored by StreamReset. Provider contract verification remains the
		// authorization boundary; do not infer provider policy from direction.
		return true
	}

	// A public-capable provider accepts clients that never appeared in its
	// same-network peer list. An explicit disconnect marker is different: it
	// proves this particular identity's old stream is stale, even while Public
	// providing remains enabled. Reject delayed relists until a peer reconnect
	// update clears the marker.
	if sourceId != nil && self.client.peerManager.isDisconnectedNetworkPeer(*sourceId) {
		return false
	}
	if destinationId != nil && self.client.peerManager.isDisconnectedNetworkPeer(*destinationId) {
		return false
	}

	allowAny, allowNetwork := self.client.contractManager.inboundProviderStreamPolicy()
	if allowAny {
		return true
	}
	if !allowNetwork || (sourceId == nil) == (destinationId == nil) {
		// no adjacent peer, or two adjacent peers (public relay)
		return false
	}
	peerId := destinationId
	if sourceId != nil {
		peerId = sourceId
	}
	return self.client.peerManager.isNetworkPeer(*peerId)
}

// maxRejectedStreamOpens bounds the rejected StreamOpen witnesses retained for
// reconsideration. A client holds a small number of concurrent streams, and
// this state is fed by remote control frames, so it is hard bounded.
const maxRejectedStreamOpens = 32

// rejectedStreamOpen is a StreamOpen that provider policy refused. The
// platform re-sends a hop only on its added-transition or in a new resident's
// StreamReset snapshot, so without reconsideration a refusal is terminal for
// the resident's lifetime: the far side rebuilds a PeerConnection every 15 s
// that nothing will ever answer, and the pair silently stays on the relay.
//
// A refusal is expected to be transient exactly once: the StreamOpen can
// arrive before the first contract that proves the relationship. Physically
// observed at 295 ms.
type rejectedStreamOpen struct {
	sourceId      *Id
	destinationId *Id
	rejectTime    time.Time
}

func (self *StreamManager) retainRejectedStreamOpen(sourceId *Id, destinationId *Id, streamId Id) {
	self.rejectedStreamsLock.Lock()
	defer self.rejectedStreamsLock.Unlock()

	if _, ok := self.rejectedStreams[streamId]; ok {
		return
	}
	for maxRejectedStreamOpens <= len(self.rejectedStreams) {
		var oldestId Id
		var oldestTime time.Time
		first := true
		for id, rejected := range self.rejectedStreams {
			if first || rejected.rejectTime.Before(oldestTime) {
				oldestId = id
				oldestTime = rejected.rejectTime
				first = false
			}
		}
		if first {
			break
		}
		delete(self.rejectedStreams, oldestId)
	}
	self.rejectedStreams[streamId] = &rejectedStreamOpen{
		sourceId:      sourceId,
		destinationId: destinationId,
		rejectTime:    time.Now(),
	}
}

func (self *StreamManager) forgetRejectedStreamOpen(streamId Id) {
	self.rejectedStreamsLock.Lock()
	defer self.rejectedStreamsLock.Unlock()
	delete(self.rejectedStreams, streamId)
}

// reconsiderRejectedStreamOpens re-runs provider policy against every retained
// refusal. It is called when the state that policy reads changes: a peer
// membership update, and the first verified `ProvideMode_Network` contract
// from an id, which is the proof that arrives after the StreamOpen.
//
// A stream that is now allowed is opened exactly as if it had been accepted
// when the platform sent it. A stream that is still refused stays retained,
// because a later proof may still arrive.
func (self *StreamManager) reconsiderRejectedStreamOpens() {
	if !self.providerStreamPolicy {
		return
	}

	type pending struct {
		streamId Id
		rejected *rejectedStreamOpen
	}
	var allowed []pending
	func() {
		self.rejectedStreamsLock.Lock()
		defer self.rejectedStreamsLock.Unlock()
		for streamId, rejected := range self.rejectedStreams {
			if self.allowStreamOpen(rejected.sourceId, rejected.destinationId) {
				allowed = append(allowed, pending{streamId: streamId, rejected: rejected})
			}
		}
		for _, p := range allowed {
			delete(self.rejectedStreams, p.streamId)
		}
	}()

	// Open outside the lock: OpenStream starts the stream sequence and its
	// P2P transport, which must not run under this bookkeeping lock.
	for _, p := range allowed {
		if self.client.log.V(1).Enabled() {
			self.client.log.Infof(
				"[sm]%s reconsider s(%s) source=%v destination=%v\n",
				self.client.ClientTag(),
				p.streamId,
				p.rejected.sourceId,
				p.rejected.destinationId,
			)
		}
		if _, err := self.streamBuffer.OpenStream(
			p.rejected.sourceId,
			p.rejected.destinationId,
			p.streamId,
		); err != nil {
			self.client.log.Infof(
				"[sm]%s reconsider s(%s) open err = %s\n",
				self.client.ClientTag(),
				p.streamId,
				err,
			)
		}
	}
}

// NetworkPeerWindowClientAuthenticated records a window client proven
// same-network by a verified Network contract, and reconsiders any StreamOpen
// refused before that proof existed. Called from the receive path after
// contract verification.
func (self *StreamManager) NetworkPeerWindowClientAuthenticated(clientId Id) {
	if !self.providerStreamPolicy || self.client.peerManager == nil {
		return
	}
	if self.client.peerManager.addNetworkPeerWindowClient(clientId) {
		self.reconsiderRejectedStreamOpens()
	}
}

// reconcileInboundProviderStreams retires already-open provider streams after
// a provide-mode/pause change. Merely unregistering the provide key stops new
// contracts at the platform, but old StreamOpen state otherwise keeps its P2P
// transports alive indefinitely, including admission and crypto retry loops.
func (self *StreamManager) reconcileInboundProviderStreams(allowAny bool, allowNetwork bool) {
	self.streamBuffer.CloseDisallowedInboundProviderStreams(
		allowAny,
		allowNetwork,
		self.providerStreamPolicy,
		// Must match the admission predicate. Resolving retirement against
		// announced top-level peers alone would immediately close every
		// window client stream that admission correctly allowed.
		self.client.peerManager.isNetworkPeer,
	)
	// A policy or membership change can also make a previously refused stream
	// allowable.
	self.reconsiderRejectedStreamOpens()
}

// closeDisconnectedPeerStreams retires streams owned by identities that the
// platform explicitly disconnected. This is independent of Public provider
// policy: allowing unknown public clients must not retain known-dead Network
// clients and their P2P admission/crypto retry loops.
func (self *StreamManager) closeDisconnectedPeerStreams(peerIds map[Id]bool) {
	if !self.providerStreamPolicy || len(peerIds) == 0 {
		return
	}
	self.streamBuffer.CloseDisconnectedPeerStreams(peerIds)
}

// ReceiveFunction
func (self *StreamManager) Receive(source TransferPath, frames []*protocol.Frame, peer Peer) {
	if source.IsControlSource() {
		for _, frame := range frames {
			// ignore error
			self.handleControlFrame(frame)
		}
	}
}

func (self *StreamManager) handleControlFrame(frame *protocol.Frame) error {
	switch frame.MessageType {
	case protocol.MessageType_TransferStreamOpen, protocol.MessageType_TransferStreamClose, protocol.MessageType_TransferStreamReset:
		if message, err := FromFrame(frame); err == nil {

			streamOpenIds := func(v *protocol.StreamOpen) (sourceId *Id, destinationId *Id, streamId Id, err error) {
				if v.SourceId != nil {
					var sourceId_ Id
					sourceId_, err = IdFromBytes(v.SourceId)
					if err != nil {
						return
					}
					sourceId = &sourceId_
				}

				if v.DestinationId != nil {
					var destinationId_ Id
					destinationId_, err = IdFromBytes(v.DestinationId)
					if err != nil {
						return
					}
					destinationId = &destinationId_
				}

				streamId, err = IdFromBytes(v.StreamId)
				return
			}

			streamOpen := func(v *protocol.StreamOpen) error {
				sourceId, destinationId, streamId, err := streamOpenIds(v)
				if err != nil {
					return err
				}
				if !self.allowStreamOpen(sourceId, destinationId) {
					// Retained so the first verified Network contract or peer
					// update from this identity can reconsider it. The proof
					// can legitimately arrive after the StreamOpen.
					self.retainRejectedStreamOpen(sourceId, destinationId, streamId)
					logged := false
					self.rejectedStreamLogOnce.Do(func() {
						logged = true
						self.client.log.Infof(
							"[sm]%s reject disabled provider s(%s) source=%v destination=%v (retained for reconsideration)\n",
							self.client.ClientTag(),
							streamId,
							sourceId,
							destinationId,
						)
					})
					if !logged && self.client.log.V(1).Enabled() {
						self.client.log.Infof(
							"[sm]%s reject disabled provider s(%s) source=%v destination=%v\n",
							self.client.ClientTag(),
							streamId,
							sourceId,
							destinationId,
						)
					}
					return nil
				}
				self.forgetRejectedStreamOpen(streamId)

				if self.client.log.V(1).Enabled() {
					self.client.log.Infof("[sm]%s open s(%s) %v->%v\n", self.client.ClientTag(), streamId, sourceId, destinationId)
				}
				if _, err := self.streamBuffer.OpenStream(sourceId, destinationId, streamId); err != nil {
					return err
				}
				return nil
			}

			switch v := message.(type) {
			case *protocol.StreamOpen:
				err := streamOpen(v)
				if err != nil {
					return err
				}

			case *protocol.StreamClose:
				streamId, err := IdFromBytes(v.StreamId)
				if err != nil {
					return err
				}

				if self.client.log.V(1).Enabled() {
					self.client.log.Infof("[sm]%s close s(%s)\n", self.client.ClientTag(), streamId)
				}
				// The platform retired this hop; a later proof must not
				// resurrect it.
				self.forgetRejectedStreamOpen(streamId)
				self.streamBuffer.CloseStream(streamId)

			case *protocol.StreamReset:
				// reconcile instead of tear down:
				// keep the sequences of relisted streams so that their state
				// (including p2p transports) survives a resident migration.
				// streams not in the list are canceled,
				// and listed streams not yet open are opened below
				keep := map[streamSequenceId]bool{}
				for _, m := range v.Streams {
					sourceId, destinationId, streamId, err := streamOpenIds(m)
					if err != nil {
						continue
					}
					if !self.allowStreamOpen(sourceId, destinationId) {
						continue
					}
					keep[newStreamSequenceId(sourceId, destinationId, streamId)] = true
				}
				if self.client.log.V(1).Enabled() {
					self.client.log.Infof("[sm]%s reset streams = %d\n", self.client.ClientTag(), len(v.Streams))
				}
				self.streamBuffer.ResetStreams(keep)
				for _, m := range v.Streams {
					if err := streamOpen(m); err != nil {
						// skip and continue: one malformed or un-openable
						// entry must not strand the remaining listed
						// streams (they would stay closed until the next
						// reset)
						if self.client.log.V(1).Enabled() {
							self.client.log.Infof("[sm]%s reset open err = %s\n", self.client.ClientTag(), err)
						}
						continue
					}
				}
			}
		}
	}
	return nil
}

func (self *StreamManager) IsStreamOpen(streamId Id) bool {
	return self.streamBuffer.IsStreamOpen(streamId)
}

// Retains a final-peer route alias only when an endpoint StreamSequence owns a
// live scope for the contract's authenticated stream. The RouteManager records
// early authentication and activates it when StreamOpen arrives.
func (self *StreamManager) authenticateStreamDestination(streamId Id, destinationId Id) bool {
	return self.client.RouteManager().authenticateWriterStreamDestination(streamId, destinationId)
}

// StreamBufferSettings configures stream forwarding and adjacent P2P workers.
type StreamBufferSettings struct {
	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	P2pTransportSettings *P2pTransportSettings
}

// streamSequenceId is the exact directional identity of one stream generation.
type streamSequenceId struct {
	SourceId       Id
	HasSource      bool
	DestinationId  Id
	HasDestination bool
	StreamId       Id
}

// maxPendingStreamOpenRequests bounds remote control state waiting for the
// lifecycle worker. A normal client has only a small number of adjacent
// streams, while the fixed limit prevents a stalled teardown from turning
// repeated StreamOpen frames into unbounded memory growth.
const maxPendingStreamOpenRequests = 128

// maxManagedStreamOpenRequests bounds active lifecycle work, including
// requests currently blocked while joining a retired generation. Terminal
// requests leave this registry, so sequential stream churn cannot exhaust it.
const maxManagedStreamOpenRequests = 1024

// Hard-bounds published sequence state independently from transient lifecycle
// work. Replacement of an existing stream id remains admissible at the limit.
const maxLiveStreamSequences = 1024

// streamOpenRequest is an immutable StreamOpen snapshot owned by the lifecycle
// worker. Generation distinguishes a superseding open or close for the same
// stream id while an older generation is waiting for teardown.
type streamOpenRequest struct {
	sourceId      *Id
	destinationId *Id
	streamId      Id
	generation    uint64
}

// streamLifecycleSnapshot captures one managed request or live sequence for a
// generation-conditional control retirement.
type streamLifecycleSnapshot struct {
	id         streamSequenceId
	request    *streamOpenRequest
	sequence   *StreamSequence
	generation uint64
}

// newStreamSequenceId snapshots optional endpoint identities into a map key.
func newStreamSequenceId(sourceId *Id, destinationId *Id, streamId Id) streamSequenceId {
	streamSequenceId := streamSequenceId{
		StreamId: streamId,
	}
	if sourceId != nil {
		streamSequenceId.SourceId = *sourceId
		streamSequenceId.HasSource = true
	}
	if destinationId != nil {
		streamSequenceId.DestinationId = *destinationId
		streamSequenceId.HasDestination = true
	}
	return streamSequenceId
}

// StreamBuffer indexes live sequences and owns bounded asynchronous Open work.
type StreamBuffer struct {
	ctx context.Context

	streamManager *StreamManager

	streamBufferSettings *StreamBufferSettings

	// Lifecycle code that needs both locks takes managementStateLock before
	// mutex. It never invokes RouteManager or another component while either
	// state lock is held.
	managementStateLock       sync.Mutex
	closed                    bool
	managedOpenRequests       map[Id]*streamOpenRequest
	pendingOpenRequests       map[Id]*streamOpenRequest
	openWorkers               map[Id]bool
	openWorkerAdmission       *lifecycleAdmission
	sequenceWorkerAdmission   *lifecycleAdmission
	mutex                     sync.Mutex
	streamSequences           map[streamSequenceId]*StreamSequence
	streamSequencesByStreamId map[Id]*StreamSequence

	// Tests use this hook to stall old-generation teardown before its owned
	// join, without changing production lifecycle behavior.
	beforeRetiredStreamJoinForTest func(*StreamSequence)
	// Tests can pause construction before any sequence resource is opened.
	beforeStreamSequenceConstructForTest func(*streamOpenRequest)
	// Tests observe that closeAndWait reached the open-worker join boundary.
	beforeCloseWaitForTest func()
	// Tests can pause after RouteManager orders an Open but before publication.
	afterStreamOpenGenerationForTest func(*streamOpenRequest)
	// Tests configure a sequence before its Run worker becomes visible.
	configureStreamSequenceForTest func(*StreamSequence)
	// Tests observe exact publication without polling asynchronous lifecycle
	// state. The hook runs after both StreamBuffer locks are released.
	afterStreamSequencePublishForTest func(*StreamSequence)
	// Tests observe terminal handling for one queued control after all managed
	// request state is released.
	afterStreamOpenProcessedForTest func(*streamOpenRequest)
	// Tests observe map cleanup after a Run worker has published done and both
	// StreamBuffer indexes have released its exact sequence pointer.
	afterStreamSequenceRemoveForTest func(*StreamSequence)
	// Tests pause an epoch-ordered close before its conditional alias clear.
	beforeCloseAliasClearForTest func()
	// Tests pause a reset after its lifecycle snapshot and before any
	// generation-conditional alias removal.
	afterResetLifecycleSnapshotForTest func()
	// Tests pause a snapshotted policy clear before RouteManager publication.
	beforePolicyAliasClearForTest func(Id, uint64)
	// Tests pause a disconnected-peer snapshot before its conditional clear.
	beforeDisconnectedAliasClearForTest func(Id, uint64)
}

// NewStreamBuffer initializes bounded per-stream lifecycle ownership.
func NewStreamBuffer(ctx context.Context, streamManager *StreamManager, streamBufferSettings *StreamBufferSettings) *StreamBuffer {
	return &StreamBuffer{
		ctx:                       ctx,
		streamManager:             streamManager,
		streamBufferSettings:      streamBufferSettings,
		managedOpenRequests:       map[Id]*streamOpenRequest{},
		pendingOpenRequests:       map[Id]*streamOpenRequest{},
		openWorkers:               map[Id]bool{},
		openWorkerAdmission:       newLifecycleAdmission(),
		sequenceWorkerAdmission:   newLifecycleAdmission(),
		streamSequences:           map[streamSequenceId]*StreamSequence{},
		streamSequencesByStreamId: map[Id]*StreamSequence{},
	}
}

// ResetStreams cancels all stream sequences except those in `keep`.
// A reset that relists the current streams reconciles instead of tearing down,
// so the kept sequences (and their p2p transports) survive a resident migration
func (self *StreamBuffer) ResetStreams(keep map[streamSequenceId]bool) {
	keepStreamIds := map[Id]bool{}
	for id := range keep {
		keepStreamIds[id.StreamId] = true
	}
	resetGeneration := self.streamManager.Client().
		RouteManager().
		writerStreamAliasGenerationCheckpoint()
	self.streamManager.Client().RouteManager().finishWriterStreamAliasGenerationsThrough(
		resetGeneration,
	)
	snapshots := self.lifecycleSnapshot()
	if self.afterResetLifecycleSnapshotForTest != nil {
		self.afterResetLifecycleSnapshotForTest()
	}

	self.streamManager.Client().RouteManager().clearWriterStreamAliasScopesExceptThroughGenerationAsync(
		keepStreamIds,
		resetGeneration,
	)
	for _, snapshot := range snapshots {
		if resetGeneration < snapshot.generation {
			continue
		}
		if keep[snapshot.id] {
			// Relisted StreamOpen is enqueued after ResetStreams returns. Stop an
			// older unpublished duplicate so only that fresh control can publish.
			if snapshot.request != nil {
				self.streamManager.Client().RouteManager().finishWriterStreamAliasGeneration(
					snapshot.id.StreamId,
					snapshot.generation,
				)
				self.invalidateOpenRequestIfCurrent(snapshot.request)
			}
			continue
		}
		if keepStreamIds[snapshot.id.StreamId] && snapshot.generation != 0 {
			self.streamManager.Client().RouteManager().clearWriterStreamAliasScopeThroughGenerationAsync(
				snapshot.id.StreamId,
				resetGeneration,
			)
		}
		if snapshot.request != nil {
			self.invalidateOpenRequestIfCurrent(snapshot.request)
		}
		if snapshot.sequence != nil {
			snapshot.sequence.CancelThroughGeneration(resetGeneration)
		}
	}
}

// OpenStream records an authoritative open and returns without waiting for an
// older same-id generation to finish. One bounded worker per active stream id
// preserves same-id order without head-of-line blocking unrelated streams.
func (self *StreamBuffer) OpenStream(sourceId *Id, destinationId *Id, streamId Id) (bool, error) {
	select {
	case <-self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}

	request := &streamOpenRequest{
		sourceId:      cloneOptionalId(sourceId),
		destinationId: cloneOptionalId(destinationId),
		streamId:      streamId,
	}

	// Reject obvious saturation before allocating a RouteManager generation.
	// Capacity is checked again at publication to cover concurrent opens.
	self.managementStateLock.Lock()
	if self.closed {
		self.managementStateLock.Unlock()
		return false, errors.New("Done.")
	}
	_, alreadyPending := self.pendingOpenRequests[streamId]
	if !alreadyPending && maxPendingStreamOpenRequests <= len(self.pendingOpenRequests) {
		self.managementStateLock.Unlock()
		return false, errors.New("Stream open queue full.")
	}
	if _, managed := self.managedOpenRequests[streamId]; !managed && maxManagedStreamOpenRequests <= len(self.managedOpenRequests) {
		self.managementStateLock.Unlock()
		return false, errors.New("Managed stream open limit reached.")
	}
	self.managementStateLock.Unlock()
	self.mutex.Lock()
	_, alreadyLive := self.streamSequencesByStreamId[streamId]
	tooManyLiveStreams := !alreadyLive && maxLiveStreamSequences <= len(self.streamSequencesByStreamId)
	self.mutex.Unlock()
	if tooManyLiveStreams {
		return false, errors.New("Live stream limit reached.")
	}

	generation, ok := self.streamManager.Client().RouteManager().beginWriterStreamAliasGeneration(streamId)
	if !ok {
		return false, errors.New("Stream alias generation limit reached.")
	}
	request.generation = generation
	if self.afterStreamOpenGenerationForTest != nil {
		self.afterStreamOpenGenerationForTest(request)
	}
	if !self.streamManager.Client().RouteManager().isWriterStreamAliasGenerationCurrent(
		streamId,
		generation,
	) {
		return true, nil
	}

	streamSequenceId := newStreamSequenceId(request.sourceId, request.destinationId, streamId)
	for {
		self.managementStateLock.Lock()
		if self.closed {
			self.managementStateLock.Unlock()
			self.streamManager.Client().RouteManager().finishWriterStreamAliasGeneration(streamId, generation)
			return false, errors.New("Done.")
		}
		alreadyPending = self.pendingOpenRequests[streamId] != nil
		if !alreadyPending && maxPendingStreamOpenRequests <= len(self.pendingOpenRequests) {
			self.managementStateLock.Unlock()
			self.streamManager.Client().RouteManager().finishWriterStreamAliasGeneration(streamId, generation)
			return false, errors.New("Stream open queue full.")
		}
		if existing := self.managedOpenRequests[streamId]; existing != nil && generation < existing.generation {
			self.managementStateLock.Unlock()
			self.streamManager.Client().RouteManager().finishWriterStreamAliasGeneration(streamId, generation)
			return true, nil
		}
		if _, managed := self.managedOpenRequests[streamId]; !managed && maxManagedStreamOpenRequests <= len(self.managedOpenRequests) {
			self.managementStateLock.Unlock()
			self.streamManager.Client().RouteManager().finishWriterStreamAliasGeneration(streamId, generation)
			return false, errors.New("Managed stream open limit reached.")
		}
		self.managementStateLock.Unlock()

		self.mutex.Lock()
		existingExact := self.streamSequences[streamSequenceId]
		self.mutex.Unlock()
		if existingExact != nil && !existingExact.reserveLifecycleGeneration(generation) {
			self.streamManager.Client().RouteManager().finishWriterStreamAliasGeneration(streamId, generation)
			return true, nil
		}

		self.managementStateLock.Lock()
		self.mutex.Lock()
		if self.closed {
			self.mutex.Unlock()
			self.managementStateLock.Unlock()
			self.streamManager.Client().RouteManager().finishWriterStreamAliasGeneration(streamId, generation)
			return false, errors.New("Done.")
		}
		if self.streamSequences[streamSequenceId] != existingExact {
			self.mutex.Unlock()
			self.managementStateLock.Unlock()
			continue
		}
		if existing := self.managedOpenRequests[streamId]; existing != nil && generation < existing.generation {
			self.mutex.Unlock()
			self.managementStateLock.Unlock()
			self.streamManager.Client().RouteManager().finishWriterStreamAliasGeneration(streamId, generation)
			return true, nil
		}
		alreadyPending = self.pendingOpenRequests[streamId] != nil
		if !alreadyPending && maxPendingStreamOpenRequests <= len(self.pendingOpenRequests) {
			self.mutex.Unlock()
			self.managementStateLock.Unlock()
			self.streamManager.Client().RouteManager().finishWriterStreamAliasGeneration(streamId, generation)
			return false, errors.New("Stream open queue full.")
		}
		if _, managed := self.managedOpenRequests[streamId]; !managed && maxManagedStreamOpenRequests <= len(self.managedOpenRequests) {
			self.mutex.Unlock()
			self.managementStateLock.Unlock()
			self.streamManager.Client().RouteManager().finishWriterStreamAliasGeneration(streamId, generation)
			return false, errors.New("Managed stream open limit reached.")
		}
		self.managedOpenRequests[streamId] = request
		self.pendingOpenRequests[streamId] = request
		startWorker := !self.openWorkers[streamId]
		if startWorker {
			if !self.openWorkerAdmission.start() {
				delete(self.openWorkers, streamId)
				delete(self.pendingOpenRequests, streamId)
				delete(self.managedOpenRequests, streamId)
				self.mutex.Unlock()
				self.managementStateLock.Unlock()
				self.streamManager.Client().RouteManager().finishWriterStreamAliasGeneration(streamId, generation)
				return false, errors.New("Done.")
			}
			self.openWorkers[streamId] = true
		}
		self.mutex.Unlock()
		self.managementStateLock.Unlock()
		if startWorker {
			go func() {
				defer self.openWorkerAdmission.finish()
				self.runOpenRequests(streamId)
			}()
		}
		return true, nil
	}
}

// cloneOptionalId snapshots one optional control-frame identity for the
// asynchronous lifecycle worker.
func cloneOptionalId(id *Id) *Id {
	if id == nil {
		return nil
	}
	cloned := *id
	return &cloned
}

// runOpenRequests owns potentially blocking replacement joins for one stream.
// A stalled replacement cannot block construction for an unrelated stream.
func (self *StreamBuffer) runOpenRequests(streamId Id) {
	for {
		self.managementStateLock.Lock()
		request := self.pendingOpenRequests[streamId]
		delete(self.pendingOpenRequests, streamId)
		if request == nil {
			delete(self.openWorkers, streamId)
			self.managementStateLock.Unlock()
			return
		}
		self.managementStateLock.Unlock()

		if err := self.processOpenRequest(request); err != nil {
			select {
			case <-self.ctx.Done():
				continue
			default:
			}
			if self.streamManager.Client().log.V(1).Enabled() {
				self.streamManager.Client().log.Infof(
					"[sm]%s async open s(%s) err = %s\n",
					self.streamManager.Client().ClientTag(),
					request.streamId,
					err,
				)
			}
		}
	}
}

// processOpenRequest opens or refreshes one stream. A failed sequence is
// retired and retried once, preserving the previous OpenStream behavior.
func (self *StreamBuffer) processOpenRequest(request *streamOpenRequest) error {
	defer self.finishOpenRequest(request)

	var skipped *StreamSequence
	for openAttempt := 0; openAttempt < 2; {
		streamSequence, retired, current := self.prepareOpenRequest(request, skipped)
		if !current {
			return nil
		}
		if retired != nil {
			if self.beforeRetiredStreamJoinForTest != nil {
				self.beforeRetiredStreamJoinForTest(retired)
			}
			retired.CloseAndWait()
			continue
		}

		if !streamSequence.activateWriterStreamAliasScope(request.generation) {
			if !self.streamManager.Client().RouteManager().isWriterStreamAliasGenerationCurrent(
				request.streamId,
				request.generation,
			) {
				return nil
			}
			skipped = streamSequence
			openAttempt += 1
			continue
		}
		_, err := streamSequence.Open()
		if err == nil {
			return nil
		}
		skipped = streamSequence
		openAttempt += 1
	}
	return errors.New("Stream sequence closed during open.")
}

// finishOpenRequest releases terminal pending/in-flight state without changing
// a live alias scope. A newer request remains authoritative.
func (self *StreamBuffer) finishOpenRequest(request *streamOpenRequest) {
	self.streamManager.Client().RouteManager().finishWriterStreamAliasGeneration(
		request.streamId,
		request.generation,
	)

	self.managementStateLock.Lock()
	if self.managedOpenRequests[request.streamId] == request {
		delete(self.managedOpenRequests, request.streamId)
	}
	self.managementStateLock.Unlock()
	if self.afterStreamOpenProcessedForTest != nil {
		self.afterStreamOpenProcessedForTest(request)
	}
}

// prepareOpenRequest selects an existing sequence, retires a conflicting
// generation, or publishes a fresh sequence. Construction happens outside
// lifecycle locks because it opens a RouteManager alias scope.
func (self *StreamBuffer) prepareOpenRequest(
	request *streamOpenRequest,
	skipped *StreamSequence,
) (streamSequence *StreamSequence, retired *StreamSequence, current bool) {
	streamSequenceId := newStreamSequenceId(
		request.sourceId,
		request.destinationId,
		request.streamId,
	)

	if !self.streamManager.Client().RouteManager().isWriterStreamAliasGenerationCurrent(
		request.streamId,
		request.generation,
	) {
		return nil, nil, false
	}
	self.managementStateLock.Lock()
	if self.managedOpenRequests[request.streamId] != request {
		self.managementStateLock.Unlock()
		return nil, nil, false
	}
	self.mutex.Lock()
	streamSequence = self.streamSequences[streamSequenceId]
	if streamSequence != nil && streamSequence != skipped {
		self.mutex.Unlock()
		self.managementStateLock.Unlock()
		return streamSequence, nil, true
	}
	if streamSequence != nil {
		retired = streamSequence
		self.removeStreamSequenceWithLock(retired)
	} else if streamSequence = self.streamSequencesByStreamId[request.streamId]; streamSequence != nil {
		retired = streamSequence
		self.removeStreamSequenceWithLock(retired)
	}
	self.mutex.Unlock()
	self.managementStateLock.Unlock()
	if retired != nil {
		return nil, retired, true
	}

	if self.beforeStreamSequenceConstructForTest != nil {
		self.beforeStreamSequenceConstructForTest(request)
	}
	streamSequence = NewStreamSequence(
		self.ctx,
		self.streamManager,
		request.sourceId,
		request.destinationId,
		request.streamId,
		self.streamBufferSettings,
	)
	if self.configureStreamSequenceForTest != nil {
		self.configureStreamSequenceForTest(streamSequence)
	}
	if !streamSequence.reserveLifecycleGeneration(request.generation) {
		streamSequence.Cancel()
		return nil, nil, false
	}

	if !self.streamManager.Client().RouteManager().isWriterStreamAliasGenerationCurrent(
		request.streamId,
		request.generation,
	) {
		streamSequence.Cancel()
		return nil, nil, false
	}
	self.managementStateLock.Lock()
	if self.managedOpenRequests[request.streamId] != request {
		self.managementStateLock.Unlock()
		streamSequence.Cancel()
		return nil, nil, false
	}
	self.mutex.Lock()
	if existing := self.streamSequencesByStreamId[request.streamId]; existing != nil {
		self.removeStreamSequenceWithLock(existing)
		self.mutex.Unlock()
		self.managementStateLock.Unlock()
		streamSequence.Cancel()
		return nil, existing, true
	}
	if maxLiveStreamSequences <= len(self.streamSequencesByStreamId) {
		self.mutex.Unlock()
		self.managementStateLock.Unlock()
		streamSequence.Cancel()
		return nil, nil, false
	}
	if !self.sequenceWorkerAdmission.start() {
		self.mutex.Unlock()
		self.managementStateLock.Unlock()
		streamSequence.Cancel()
		return nil, nil, false
	}
	self.streamSequences[streamSequenceId] = streamSequence
	self.streamSequencesByStreamId[request.streamId] = streamSequence
	self.mutex.Unlock()
	self.managementStateLock.Unlock()
	if self.afterStreamSequencePublishForTest != nil {
		self.afterStreamSequencePublishForTest(streamSequence)
	}

	go HandleError(func() {
		defer self.sequenceWorkerAdmission.finish()
		defer func() {
			self.mutex.Lock()
			self.removeStreamSequenceWithLock(streamSequence)
			self.mutex.Unlock()
			if self.afterStreamSequenceRemoveForTest != nil {
				self.afterStreamSequenceRemoveForTest(streamSequence)
			}
		}()
		streamSequence.Run()
	})
	return streamSequence, nil, true
}

// Close prevents later stream publication, invalidates pending generations,
// and requests teardown without joining callback-facing control handling.
func (self *StreamBuffer) Close() {
	var requests []*streamOpenRequest
	self.managementStateLock.Lock()
	if self.closed {
		self.managementStateLock.Unlock()
		return
	}
	self.closed = true
	for _, request := range self.managedOpenRequests {
		requests = append(requests, request)
	}
	clear(self.managedOpenRequests)
	clear(self.pendingOpenRequests)
	self.managementStateLock.Unlock()

	self.openWorkerAdmission.close()
	self.sequenceWorkerAdmission.close()
	for _, request := range requests {
		self.streamManager.Client().RouteManager().finishWriterStreamAliasGeneration(
			request.streamId,
			request.generation,
		)
	}

	self.mutex.Lock()
	sequences := make([]*StreamSequence, 0, len(self.streamSequences))
	for _, sequence := range self.streamSequences {
		sequences = append(sequences, sequence)
	}
	self.mutex.Unlock()
	for _, sequence := range sequences {
		sequence.Cancel()
	}
}

// closeAndWait joins every open-request and live-sequence worker admitted
// before Close won the publication boundary.
func (self *StreamBuffer) closeAndWait(ctx context.Context) error {
	self.Close()
	if self.beforeCloseWaitForTest != nil {
		self.beforeCloseWaitForTest()
	}
	var result error
	if err := waitForLifecycleDone(
		ctx,
		self.openWorkerAdmission.Done(),
		"stream open workers",
	); err != nil {
		result = errors.Join(result, err)
	}
	if err := waitForLifecycleDone(
		ctx,
		self.sequenceWorkerAdmission.Done(),
		"stream sequence workers",
	); err != nil {
		result = errors.Join(result, err)
	}
	return result
}

// lifecycleSnapshot captures every pending/in-flight request and live
// sequence without invoking sequence methods while StreamBuffer locks are held.
func (self *StreamBuffer) lifecycleSnapshot() []streamLifecycleSnapshot {
	self.managementStateLock.Lock()
	self.mutex.Lock()

	snapshots := make([]streamLifecycleSnapshot, 0, len(self.managedOpenRequests)+len(self.streamSequences))
	for _, request := range self.managedOpenRequests {
		snapshots = append(snapshots, streamLifecycleSnapshot{
			id:         newStreamSequenceId(request.sourceId, request.destinationId, request.streamId),
			request:    request,
			generation: request.generation,
		})
	}
	for id, sequence := range self.streamSequences {
		snapshots = append(snapshots, streamLifecycleSnapshot{
			id:       id,
			sequence: sequence,
		})
	}

	self.mutex.Unlock()
	self.managementStateLock.Unlock()
	for index := range snapshots {
		if snapshots[index].sequence != nil {
			snapshots[index].generation = snapshots[index].sequence.LifecycleGeneration()
		}
	}
	return snapshots
}

// invalidateOpenRequestIfCurrent retires exactly one snapshotted lifecycle
// request without disturbing a newer same-id open.
func (self *StreamBuffer) invalidateOpenRequestIfCurrent(request *streamOpenRequest) bool {
	self.managementStateLock.Lock()
	defer self.managementStateLock.Unlock()
	if self.managedOpenRequests[request.streamId] != request {
		return false
	}
	delete(self.managedOpenRequests, request.streamId)
	if self.pendingOpenRequests[request.streamId] == request {
		delete(self.pendingOpenRequests, request.streamId)
	}
	return true
}

// streamAllowedByProviderPolicy evaluates one stream identity against the
// current inbound provider policy without holding StreamBuffer state locks.
func streamAllowedByProviderPolicy(
	id streamSequenceId,
	allowAny bool,
	allowNetwork bool,
	isNetworkPeer func(Id) bool,
) bool {
	if allowAny {
		return true
	}
	if allowNetwork && id.HasSource != id.HasDestination {
		peerId := id.DestinationId
		if id.HasSource {
			peerId = id.SourceId
		}
		return isNetworkPeer(peerId)
	}
	return false
}

// Removes one sequence from both indexes while mutex is held.
func (self *StreamBuffer) removeStreamSequenceWithLock(streamSequence *StreamSequence) {
	streamSequenceId := newStreamSequenceId(
		streamSequence.sourceId,
		streamSequence.destinationId,
		streamSequence.streamId,
	)
	if self.streamSequences[streamSequenceId] == streamSequence {
		delete(self.streamSequences, streamSequenceId)
	}
	if self.streamSequencesByStreamId[streamSequence.streamId] == streamSequence {
		delete(self.streamSequencesByStreamId, streamSequence.streamId)
	}
}

// CloseStream orders an authoritative epoch, clears aliases, and cancels old
// generations without joining their Run workers.
func (self *StreamBuffer) CloseStream(streamId Id) {
	closeGeneration := self.streamManager.Client().
		RouteManager().
		writerStreamAliasGenerationCheckpoint()
	snapshots := self.lifecycleSnapshot()
	if self.beforeCloseAliasClearForTest != nil {
		self.beforeCloseAliasClearForTest()
	}
	self.streamManager.Client().RouteManager().clearWriterStreamAliasScopeThroughGenerationAsync(
		streamId,
		closeGeneration,
	)

	for _, snapshot := range snapshots {
		if snapshot.id.StreamId != streamId ||
			closeGeneration < snapshot.generation {
			continue
		}
		if snapshot.request != nil {
			self.invalidateOpenRequestIfCurrent(snapshot.request)
		}
		if snapshot.sequence != nil {
			snapshot.sequence.CancelThroughGeneration(closeGeneration)
		}
	}
}

// CloseDisallowedInboundProviderStreams cancels provider endpoint, return,
// companion, and intermediary relay streams that the current strict provider
// policy no longer permits. Network-only policy retains exactly one-adjacent-
// endpoint streams for known same-network peers; it does not permit public
// endpoints or relaying between two other peers. Ordinary destination/window
// clients bypass this provider policy entirely.
func (self *StreamBuffer) CloseDisallowedInboundProviderStreams(
	allowAny bool,
	allowNetwork bool,
	providerStreamPolicy bool,
	isNetworkPeer func(Id) bool,
) {
	if !providerStreamPolicy {
		return
	}

	for _, snapshot := range self.lifecycleSnapshot() {
		if streamAllowedByProviderPolicy(snapshot.id, allowAny, allowNetwork, isNetworkPeer) {
			continue
		}
		if self.beforePolicyAliasClearForTest != nil {
			self.beforePolicyAliasClearForTest(snapshot.id.StreamId, snapshot.generation)
		}
		if snapshot.generation != 0 {
			self.streamManager.Client().RouteManager().clearWriterStreamAliasScopeThroughGenerationAsync(
				snapshot.id.StreamId,
				snapshot.generation,
			)
		}
		if snapshot.request != nil {
			self.invalidateOpenRequestIfCurrent(snapshot.request)
		}
		if snapshot.sequence != nil {
			snapshot.sequence.CancelThroughGeneration(snapshot.generation)
		}
	}
}

// CloseDisconnectedPeerStreams cancels every sequence adjacent to a known
// disconnected peer. The platform reconnect update clears the tombstone, and
// a subsequent StreamOpen then constructs a fresh generation.
func (self *StreamBuffer) CloseDisconnectedPeerStreams(peerIds map[Id]bool) {
	for _, snapshot := range self.lifecycleSnapshot() {
		if (!snapshot.id.HasSource || !peerIds[snapshot.id.SourceId]) &&
			(!snapshot.id.HasDestination || !peerIds[snapshot.id.DestinationId]) {
			continue
		}
		if self.beforeDisconnectedAliasClearForTest != nil {
			self.beforeDisconnectedAliasClearForTest(snapshot.id.StreamId, snapshot.generation)
		}
		if snapshot.generation != 0 {
			self.streamManager.Client().RouteManager().clearWriterStreamAliasScopeThroughGenerationAsync(
				snapshot.id.StreamId,
				snapshot.generation,
			)
		}
		if snapshot.request != nil {
			self.invalidateOpenRequestIfCurrent(snapshot.request)
		}
		if snapshot.sequence != nil {
			snapshot.sequence.CancelThroughGeneration(snapshot.generation)
		}
	}
}

// IsStreamOpen reports whether a sequence is currently indexed for streamId.
func (self *StreamBuffer) IsStreamOpen(streamId Id) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	_, ok := self.streamSequencesByStreamId[streamId]
	return ok
}

// StreamSequence owns one directional P2P hop generation and its alias scope.
type StreamSequence struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	streamManager *StreamManager

	streamBufferSettings *StreamBufferSettings

	sourceId      *Id
	destinationId *Id
	streamId      Id
	// aliasStateLock protects replacement of the endpoint scope owner. Scope
	// callbacks are invoked after releasing it. It also protects the latest
	// lifecycle generation attached to this live sequence.
	aliasStateLock              sync.Mutex
	closeWriterStreamAliasScope func()
	lifecycleGeneration         uint64
	// Tests can pause after RouteManager opens a scope but before ownership is
	// published under aliasStateLock.
	afterAliasScopeOpenForTest func()
	// Tests inspect the StreamBuffer lock hierarchy immediately after this
	// sequence's lifecycle lock is acquired.
	afterLifecycleStateLockForTest func()
	// Tests can hold teardown before alias removal and done publication.
	exitBarrierForTest func()
	// Tests can inspect both private intermediary directions before either
	// P2P association starts publishing routes.
	intermediaryRouteManagersForTest func(*RouteManager, *RouteManager)
	// Tests can hold a forwarding child after route cleanup but before the
	// sequence's child-worker join completes.
	beforeForwardWorkerDoneForTest func()
	// Tests observe that every intermediary child has been launched.
	afterChildrenStartedForTest func()

	idleCondition *IdleCondition
}

// NewStreamSequence constructs an unpublished, inactive stream generation.
func NewStreamSequence(
	ctx context.Context,
	streamManager *StreamManager,
	sourceId *Id,
	destinationId *Id,
	streamId Id,
	streamBufferSettings *StreamBufferSettings) *StreamSequence {
	cancelCtx, cancel := context.WithCancel(ctx)

	streamSequence := &StreamSequence{
		ctx:                  cancelCtx,
		cancel:               cancel,
		done:                 make(chan struct{}),
		streamManager:        streamManager,
		streamBufferSettings: streamBufferSettings,
		sourceId:             sourceId,
		destinationId:        destinationId,
		streamId:             streamId,
		idleCondition:        NewIdleCondition(),
	}
	return streamSequence
}

// activateWriterStreamAliasScope installs generation-gated ownership for an
// endpoint sequence. Intermediary sequences do not expose a final-peer alias.
func (self *StreamSequence) activateWriterStreamAliasScope(generation uint64) bool {
	if (self.sourceId == nil) == (self.destinationId == nil) {
		self.aliasStateLock.Lock()
		if generation < self.lifecycleGeneration {
			self.aliasStateLock.Unlock()
			return false
		}
		select {
		case <-self.ctx.Done():
			self.aliasStateLock.Unlock()
			return false
		default:
		}
		self.lifecycleGeneration = generation
		self.aliasStateLock.Unlock()
		return true
	}

	closeScope, ok := self.streamManager.Client().
		RouteManager().
		openWriterStreamAliasScopeForGeneration(self.streamId, generation)
	if !ok {
		return false
	}
	if self.afterAliasScopeOpenForTest != nil {
		self.afterAliasScopeOpenForTest()
	}

	self.aliasStateLock.Lock()
	if generation < self.lifecycleGeneration {
		self.aliasStateLock.Unlock()
		closeScope()
		return false
	}
	select {
	case <-self.ctx.Done():
		self.aliasStateLock.Unlock()
		closeScope()
		return false
	default:
	}
	previousCloseScope := self.closeWriterStreamAliasScope
	self.closeWriterStreamAliasScope = closeScope
	self.lifecycleGeneration = generation
	self.aliasStateLock.Unlock()
	if previousCloseScope != nil {
		previousCloseScope()
	}
	return true
}

// reserveLifecycleGeneration records monotonic ownership before publication so
// a stale worker or control snapshot cannot lower or cancel a newer refresh.
func (self *StreamSequence) reserveLifecycleGeneration(generation uint64) bool {
	self.aliasStateLock.Lock()
	if self.afterLifecycleStateLockForTest != nil {
		self.afterLifecycleStateLockForTest()
	}
	defer self.aliasStateLock.Unlock()
	if generation < self.lifecycleGeneration {
		return false
	}
	self.lifecycleGeneration = generation
	return true
}

// LifecycleGeneration returns the latest authoritative generation attached to
// this sequence for conditional policy/reset retirement.
func (self *StreamSequence) LifecycleGeneration() uint64 {
	self.aliasStateLock.Lock()
	defer self.aliasStateLock.Unlock()
	return self.lifecycleGeneration
}

// CancelThroughGeneration atomically cancels only when this sequence has not
// been refreshed by a generation after the caller's control snapshot.
func (self *StreamSequence) CancelThroughGeneration(generation uint64) bool {
	self.aliasStateLock.Lock()
	defer self.aliasStateLock.Unlock()
	if generation < self.lifecycleGeneration {
		return false
	}
	self.cancel()
	return true
}

// closeWriterStreamAliases releases the most recent endpoint alias scope.
func (self *StreamSequence) closeWriterStreamAliases() {
	self.aliasStateLock.Lock()
	closeScope := self.closeWriterStreamAliasScope
	self.closeWriterStreamAliasScope = nil
	self.aliasStateLock.Unlock()
	if closeScope != nil {
		closeScope()
	}
}

// Open refreshes the sequence idle lease when its context remains live.
func (self *StreamSequence) Open() (bool, error) {
	select {
	case <-self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}

	if !self.idleCondition.UpdateOpen() {
		return false, errors.New("Done.")
	}
	defer self.idleCondition.UpdateClose()

	return true, nil
}

// Run owns adjacent transports, forwarding workers, alias teardown, and done.
func (self *StreamSequence) Run() {
	defer close(self.done)
	childWorkers := newLifecycleAdmission()
	p2pTransports := []*P2pTransport{}
	defer func() {
		self.cancel()
		for _, p2pTransport := range p2pTransports {
			p2pTransport.cancel()
		}
		for _, p2pTransport := range p2pTransports {
			<-p2pTransport.done
		}
		childWorkers.close()
		<-childWorkers.Done()
		self.closeWriterStreamAliases()
	}()
	if self.exitBarrierForTest != nil {
		defer self.exitBarrierForTest()
	}

	if self.sourceId == nil || self.destinationId == nil {
		clientRouteManager := self.streamManager.Client().RouteManager()
		var endpointTransport *P2pTransport

		if self.sourceId != nil {
			endpointTransport = newP2pTransport(
				self.ctx,
				self.streamManager.Client(),
				self.streamManager.WebRtcManager(),
				clientRouteManager,
				clientRouteManager,
				*self.sourceId,
				self.streamId,
				PeerTypeSource,
				self.streamBufferSettings.P2pTransportSettings,
				true,
			)
		} else if self.destinationId != nil {
			endpointTransport = newP2pTransport(
				self.ctx,
				self.streamManager.Client(),
				self.streamManager.WebRtcManager(),
				clientRouteManager,
				clientRouteManager,
				*self.destinationId,
				self.streamId,
				PeerTypeDestination,
				self.streamBufferSettings.P2pTransportSettings,
				true,
			)
		} else {
			// the stream must have one of source or destination
			if self.streamManager.client.log.V(1).Enabled() {
				self.streamManager.client.log.Infof("[sm] s(%s) missing source or destination.\n", self.streamId)
			}
			return
		}
		p2pTransports = append(p2pTransports, endpointTransport)
	} else {
		p2pToDestinationRouteManager := NewRouteManagerWithLogger(self.ctx, fmt.Sprintf("->s(%s)", self.streamId), self.streamManager.client.log)
		p2pToSourceRouteManager := NewRouteManagerWithLogger(self.ctx, fmt.Sprintf("<-s(%s)", self.streamId), self.streamManager.client.log)
		if self.intermediaryRouteManagersForTest != nil {
			self.intermediaryRouteManagersForTest(
				p2pToDestinationRouteManager,
				p2pToSourceRouteManager,
			)
		}

		// to destination
		p2pTransports = append(p2pTransports, NewP2pTransport(
			self.ctx,
			self.streamManager.Client(),
			self.streamManager.WebRtcManager(),
			p2pToDestinationRouteManager,
			p2pToSourceRouteManager,
			*self.destinationId,
			self.streamId,
			PeerTypeDestination,
			self.streamBufferSettings.P2pTransportSettings,
		))
		// to source
		p2pTransports = append(p2pTransports, NewP2pTransport(
			self.ctx,
			self.streamManager.Client(),
			self.streamManager.WebRtcManager(),
			p2pToSourceRouteManager,
			p2pToDestinationRouteManager,
			*self.sourceId,
			self.streamId,
			PeerTypeSource,
			self.streamBufferSettings.P2pTransportSettings,
		))

		forward := func(routeManager *RouteManager) {
			defer self.cancel()

			mrr := routeManager.OpenMultiRouteReader(TransferPath{
				StreamId: self.streamId,
			})
			defer routeManager.CloseMultiRouteReader(mrr)
			mrw := routeManager.OpenMultiRouteWriter(TransferPath{
				StreamId: self.streamId,
			})
			defer routeManager.CloseMultiRouteWriter(mrw)

			for {
				checkpointId := self.idleCondition.Checkpoint()
				transferFrameBytes, err := mrr.Read(self.ctx, self.streamBufferSettings.ReadTimeout)
				if err != nil {
					return
				}
				if transferFrameBytes == nil {
					// idle timeout
					if self.idleCondition.Close(checkpointId) {
						// close the sequence
						return
					}
					// else the sequence was opened again
					continue
				}
				success, err := mrw.WriteDetailed(self.ctx, transferFrameBytes, self.streamBufferSettings.WriteTimeout)
				if err != nil {
					MessagePoolReturn(transferFrameBytes)
					return
				}
				if !success {
					// drop it
					MessagePoolReturn(transferFrameBytes)
				}
			}
		}

		startForward := func(routeManager *RouteManager) {
			if !childWorkers.start() {
				return
			}
			go func() {
				defer childWorkers.finish()
				HandleError(func() {
					forward(routeManager)
				}, self.cancel)
				if self.beforeForwardWorkerDoneForTest != nil {
					self.beforeForwardWorkerDoneForTest()
				}
			}()
		}
		startForward(p2pToDestinationRouteManager)
		startForward(p2pToSourceRouteManager)
		if self.afterChildrenStartedForTest != nil {
			self.afterChildrenStartedForTest()
		}
	}

	select {
	case <-self.ctx.Done():
		return
	}
}

// Cancel requests teardown without waiting for transport workers. Receive
// control handling relies on this nonblocking boundary.
func (self *StreamSequence) Cancel() {
	self.cancel()
}

// Join waits until transport and alias teardown for this generation finishes.
func (self *StreamSequence) Join() {
	<-self.done
}

// CloseAndWait retires a generation before a same-stream replacement is
// published, preventing its ready transport from matching the new alias.
func (self *StreamSequence) CloseAndWait() {
	self.Cancel()
	self.Join()
}

// Close requests teardown without joining the generation.
func (self *StreamSequence) Close() {
	self.Cancel()
}
