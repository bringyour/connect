package connect

import (
	"context"
	"errors"
	"fmt"
	"math"
	mathrand "math/rand"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	// "runtime/debug"

	"maps"
)

// manage multiple routes to a destination, allowing weighted reads and writes to the routes
// this assumes the source is a single client

// routes are expected to have flow control and error detection and rejection
type Route = chan []byte

// H1 ACK priority routes are an optional companion to an ordinary platform
// send route. Route intentionally remains a channel alias on the packet hot
// path, so the narrow registry carries the extra writer lane without changing
// every Route user or allocating a wrapper per write. Server/default
// transports never register a companion; the atomic count keeps their ACK
// path out of sync.Map entirely.
var h1AckPriorityRoutes sync.Map
var h1AckPriorityRouteCount atomic.Int64

func registerH1AckPriorityRoute(route Route, priorityRoute Route) {
	if route == nil || priorityRoute == nil {
		return
	}
	if _, loaded := h1AckPriorityRoutes.LoadOrStore(route, priorityRoute); loaded {
		panic("H1 ACK priority route already registered")
	}
	h1AckPriorityRouteCount.Add(1)
}

func unregisterH1AckPriorityRoute(route Route) {
	if route == nil {
		return
	}
	if _, loaded := h1AckPriorityRoutes.LoadAndDelete(route); loaded {
		if h1AckPriorityRouteCount.Add(-1) < 0 {
			panic("negative H1 ACK priority route count")
		}
	}
}

func h1AckPriorityRoute(route Route) (Route, bool) {
	if h1AckPriorityRouteCount.Load() == 0 {
		return nil, false
	}
	value, ok := h1AckPriorityRoutes.Load(route)
	if !ok {
		return nil, false
	}
	priorityRoute, ok := value.(Route)
	return priorityRoute, ok
}

// TransferCarrierProperties describes exact delivery semantics at route
// boundaries. Send-side unreliable fields bound acknowledgement flight;
// ReceiveReliability tells Pack admission whether the physical lane can safely
// propagate fixed-queue backpressure.
type TransferCarrierProperties struct {
	Unreliable bool
	// ReceiveReliability describes the exact physical lane feeding a receive
	// route. Hybrid carriers publish one route per lane so a complete message
	// recovered by a reliable stream can apply bounded backpressure while a
	// DATAGRAM reader remains nonblocking. Unknown preserves compatibility for
	// custom transports that predate lane-aware receive admission.
	ReceiveReliability CarrierReliability
	// UnreliableMaxMessageByteCount refines a hybrid send carrier whose lane is
	// selected by a contiguous message-size threshold. A positive limit means
	// frames at or below the limit are unreliable and larger frames are reliable.
	// It is exported so server exchange headers can preserve the same decision on
	// a remote resident without serializing a callback.
	UnreliableMaxMessageByteCount int
	// UnreliableFlowIsolation enables bounded per-IP-flow scheduling before
	// Transfer assigns sequence numbers. It preserves FIFO order within each
	// flow while preventing a saturated bulk flow from hiding a newly active
	// interactive flow in the sender queue.
	UnreliableFlowIsolation bool
	// UnreliableFlowReserve permits one bounded Transfer message beyond a full
	// ordinary flight for a newly active flow. H3 DATAGRAM can absorb that
	// ninth low-bar message; native P2P already reserves its final receive slot
	// for ACK/control traffic and must leave this disabled.
	UnreliableFlowReserve bool
	// unreliableFlightMessageLimit optionally tightens Transfer's configured
	// unreliable message flight for this carrier. Zero retains the global
	// policy. A native P2P sender derives this from the peer route's
	// complete-message queue while retaining admission for untracked ACK and
	// control traffic.
	unreliableFlightMessageLimit int
	// unreliableFlightByteLimit optionally tightens Transfer's configured byte
	// flight for this carrier. A bounded nonblocking carrier can reserve payload
	// capacity for untracked ACK, compact-recovery, contract, and probe traffic
	// without changing the process-wide Transfer defaults.
	unreliableFlightByteLimit ByteCount
	// unreliableLossyMaxMessageByteCount, when positive, is the largest frame
	// the unreliable lane should carry once loss has pinned the Transfer
	// flight to its floor; larger frames go reliable-only (§13.6).
	unreliableLossyMaxMessageByteCount ByteCount
	// unreliableForMessageByteCount refines a hybrid carrier after an exact route has
	// accepted one complete routed Transfer frame. Nil preserves the historical
	// route-wide meaning of Unreliable. The callback must be safe for concurrent
	// use and must not block.
	unreliableForMessageByteCount func(transferFrameByteCount int) bool
}

func (self TransferCarrierProperties) messageUnreliable(
	transferFrameBytes []byte,
) bool {
	if !self.Unreliable {
		return false
	}
	if self.unreliableForMessageByteCount != nil {
		return self.unreliableForMessageByteCount(len(transferFrameBytes))
	}
	if 0 < self.UnreliableMaxMessageByteCount {
		return len(transferFrameBytes) <= self.UnreliableMaxMessageByteCount
	}
	return true
}

// CarrierReliability is the delivery contract of the exact receive lane that
// produced one complete Transfer frame. It is independent of TransportType: an
// H3 or P2P connection can expose both reliable and unreliable lanes.
type CarrierReliability uint8

const (
	CarrierReliabilityUnknown CarrierReliability = iota
	CarrierReliabilityReliable
	CarrierReliabilityUnreliable
)

// TransportType is the carrier that actually accepted or delivered a routed
// Transfer frame. It is intentionally distinct from TransportMode: p2p and an
// untyped/custom route are observable carriers but are not selectable platform
// transport modes.
type TransportType string

const (
	TransportTypeUnknown   TransportType = "unknown"
	TransportTypeH3        TransportType = "h3"
	TransportTypeH1        TransportType = "h1"
	TransportTypeH3Dns     TransportType = "h3dns"
	TransportTypeH3DnsPump TransportType = "h3dnspump"
	TransportTypeP2p       TransportType = "p2p"
)

// TransportTypes returns the stable public stats order. Unknown is retained
// as a real bucket so admitted logical traffic remains reconcilable while a
// queued Pack is waiting for its first physical route write.
func TransportTypes() []TransportType {
	return []TransportType{
		TransportTypeH3,
		TransportTypeH1,
		TransportTypeH3Dns,
		TransportTypeH3DnsPump,
		TransportTypeP2p,
		TransportTypeUnknown,
	}
}

// DirectCarrierAffinityStats explains how tied H1/H3 writers use their shared
// route set. Preferred writes stay on the selector's first healthy carrier;
// preferred-route pressure backpressures instead of spilling onto the tied
// peer. Fallback writes expose any violation of that strict policy and should
// remain zero. Activations count selector route generations entering the
// tied-direct policy. Route changes include physical withdrawal and the
// evidence-driven H1-to-H3 transition; H1TimeoutFailoverCount distinguishes
// the latter without marking either transport unhealthy.
type DirectCarrierAffinityStats struct {
	PreferredH1WriteCount     uint64
	PreferredH3WriteCount     uint64
	FallbackH1WriteCount      uint64
	FallbackH3WriteCount      uint64
	PreferredBlockedCount     uint64
	ActivationCount           uint64
	RouteChangeCount          uint64
	H1TimeoutFailoverCount    uint64
	H3PreferredAfterH1Timeout bool
}

type directCarrierAffinityCounters struct {
	preferredH1WriteCount     atomic.Uint64
	preferredH3WriteCount     atomic.Uint64
	fallbackH1WriteCount      atomic.Uint64
	fallbackH3WriteCount      atomic.Uint64
	preferredBlockedCount     atomic.Uint64
	activationCount           atomic.Uint64
	routeChangeCount          atomic.Uint64
	h1TimeoutFailoverCount    atomic.Uint64
	h3PreferredAfterH1Timeout atomic.Bool
}

func (self *directCarrierAffinityCounters) snapshot() DirectCarrierAffinityStats {
	if self == nil {
		return DirectCarrierAffinityStats{}
	}
	return DirectCarrierAffinityStats{
		PreferredH1WriteCount:     self.preferredH1WriteCount.Load(),
		PreferredH3WriteCount:     self.preferredH3WriteCount.Load(),
		FallbackH1WriteCount:      self.fallbackH1WriteCount.Load(),
		FallbackH3WriteCount:      self.fallbackH3WriteCount.Load(),
		PreferredBlockedCount:     self.preferredBlockedCount.Load(),
		ActivationCount:           self.activationCount.Load(),
		RouteChangeCount:          self.routeChangeCount.Load(),
		H1TimeoutFailoverCount:    self.h1TimeoutFailoverCount.Load(),
		H3PreferredAfterH1Timeout: self.h3PreferredAfterH1Timeout.Load(),
	}
}

type typedTransport interface {
	TransportType() TransportType
}

func transportTypeOf(transport Transport) TransportType {
	if typed, ok := transport.(typedTransport); ok {
		if transportType := typed.TransportType(); transportType != "" {
			return transportType
		}
	}
	return TransportTypeUnknown
}

// receiveLaneTransport gives one physical receive lane an independent route
// identity while preserving the base transport's matching and priority rules.
// Hybrid H3 and P2P connections use siblings so immutable route properties can
// carry exact lane reliability without changing Route's allocation-free byte
// channel contract.
type receiveLaneTransport struct {
	transportId Id
	base        Transport
}

func newReceiveLaneTransport(base Transport) *receiveLaneTransport {
	return &receiveLaneTransport{transportId: NewId(), base: base}
}

func (self *receiveLaneTransport) TransportId() Id { return self.transportId }
func (self *receiveLaneTransport) TransportType() TransportType {
	return transportTypeOf(self.base)
}
func (self *receiveLaneTransport) Priority() int   { return self.base.Priority() }
func (self *receiveLaneTransport) Weight() float32 { return self.base.Weight() }
func (self *receiveLaneTransport) CanEvalRouteWeight(
	stats *RouteStats,
	remainingStats map[Transport]*RouteStats,
) bool {
	return self.base.CanEvalRouteWeight(stats, remainingStats)
}
func (self *receiveLaneTransport) RouteWeight(
	stats *RouteStats,
	remainingStats map[Transport]*RouteStats,
) float32 {
	return self.base.RouteWeight(stats, remainingStats)
}
func (self *receiveLaneTransport) MatchesSend(destination TransferPath) bool {
	return false
}
func (self *receiveLaneTransport) MatchesReceive(destination TransferPath) bool {
	return self.base.MatchesReceive(destination)
}
func (self *receiveLaneTransport) Downgrade(source TransferPath) {
	self.base.Downgrade(source)
}

const TransportMaxPriority = 0
const TransportMinPriority = 100
const TransportMaxWeight = float32(1)
const TransportMinWeight = float32(0)

// each transport must have a unique local id
// This solves an issue where some transports can be implemented with zero state.
// Zero state transports makes it ambiguous whether the transport pointer can be used as a key.
// see https://github.com/golang/go/issues/65878
type Transport interface {
	TransportId() Id

	// lower priority takes precedence
	Priority() int

	// the intrinsic weight of the transport, [0, 1]
	// if the transport has no preference, use 0
	Weight() float32

	CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool
	// returns the fraction of route weight that should be allocated to this transport
	// the remaining are the lower priority transports
	// call `rematchTransport` to re-evaluate the weights. this is used for a control loop where the weight is adjusted to match the actual distribution
	RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32

	MatchesSend(destination TransferPath) bool
	MatchesReceive(destination TransferPath) bool

	// request that p2p and direct connections be re-established that include the source
	// connections will be denied for sources that have bad audits
	Downgrade(source TransferPath)
}

type MultiRouteWriter interface {
	Write(ctx context.Context, transferFrameBytes []byte, timeout time.Duration) error
	WriteDetailed(ctx context.Context, transferFrameBytes []byte, timeout time.Duration) (bool, error)
	GetActiveRoutes() []Route
	GetInactiveRoutes() []Route
}

type MultiRouteReader interface {
	Read(ctx context.Context, timeout time.Duration) ([]byte, error)
	GetActiveRoutes() []Route
	GetInactiveRoutes() []Route
}

// These optional extensions preserve compatibility with lightweight custom
// MultiRouteWriter/Reader implementations while allowing the production route
// selector to report the carrier selected by the actual channel operation.
type TransportMultiRouteWriter interface {
	WriteDetailedWithTransport(ctx context.Context, transferFrameBytes []byte, timeout time.Duration) (bool, TransportType, error)
}

type TransportMultiRouteReader interface {
	ReadWithTransport(ctx context.Context, timeout time.Duration) ([]byte, TransportType, error)
}

// transferReceiveDisposition remains internal so existing custom reader
// interfaces stay source-compatible while the production selector carries the
// exact route lane into Client admission.
type transferReceiveDisposition struct {
	transportType TransportType
	reliability   CarrierReliability
}

type transferCarrierMultiRouteReader interface {
	readWithCarrier(
		ctx context.Context,
		timeout time.Duration,
	) ([]byte, transferReceiveDisposition, error)
}

// transferWriteDisposition is returned only by the production selector. The
// public optional writer interface remains source-compatible for custom
// writers, while Transfer can distinguish writes accepted by a reliable
// carrier (H1, H3 stream, DNS stream, or legacy P2P) from writes accepted by
// an unreliable message lane such as QUIC DATAGRAM or native P2P.
type transferWriteDisposition struct {
	transportType  TransportType
	unreliable     bool
	reliable       bool
	hybridReliable bool
	route          Route
	// initiallyBlocked is true when no eligible route accepted the frame in
	// the selector's allocation-free first pass. ReceiveSequence uses this to
	// distinguish ACK production/compression time from carrier-queue wait.
	initiallyBlocked bool
	// Reuse the selector's existing deadline clock for the uncommon blocked
	// case instead of surrounding every ACK write with another pair of reads.
	initialWaitDuration time.Duration
}

type transferCarrierMultiRouteWriter interface {
	writeDetailedWithCarrier(
		ctx context.Context,
		transferFrameBytes []byte,
		timeout time.Duration,
	) (bool, transferWriteDisposition, error)
}

// transferReliableOnlyMultiRouteWriter writes on the active carriers that are
// not potentially unreliable. When none exist the write uses the ordinary
// route set, so a sequence never loses a route it could otherwise use.
type transferReliableOnlyMultiRouteWriter interface {
	writeDetailedReliableOnly(
		ctx context.Context,
		transferFrameBytes []byte,
		timeout time.Duration,
	) (bool, transferWriteDisposition, error)
}

type transferCarrierRouteStateProvider interface {
	transferRouteActive(route Route) bool
}

// transferReliableCapacityProvider answers, without locking, whether a route
// whose carrier is not potentially unreliable can accept a frame right now.
type transferReliableCapacityProvider interface {
	reliableRouteHasCapacity() bool
}

// transferRouteAckProgressObserver receives Transfer acknowledgement evidence
// per route, the only delivery proof an unreliable lane ever produces.
type transferRouteAckProgressObserver interface {
	observeRouteAckProgress(route Route)
}

type transferCarrierH1TimeoutFailoverProvider interface {
	transferPreferH3AfterH1Timeout(route Route) bool
}

type RouteManager struct {
	ctx context.Context

	clientTag string
	log       Logger

	// transportUpdateLock serializes the short route-publication phase. Writer
	// retirement waits always happen after this lock is released.
	transportUpdateLock sync.Mutex
	mutex               sync.Mutex
	writerMatchState    *MatchState
	readerMatchState    *MatchState

	// Only snapshots with admitted writers enter these indexes. Physical route
	// teardown joins by transport, while writer close joins by selector; an
	// unrelated transport never waits for another destination's stalled writer.
	pendingWriterSnapshots            map[*routeSnapshot]bool
	pendingWriterSnapshotsByTransport map[Transport]map[*routeSnapshot]bool
	pendingWriterSnapshotsBySelector  map[*MultiRouteSelector]map[*routeSnapshot]bool
	// Verified final destinations outlive individual transfer sequences. A
	// live endpoint StreamSequence owns the matching alias scope; verified
	// contracts may arrive before that scope opens and remain recorded until an
	// authoritative stream retirement clears them.
	writerStreamAuthenticatedDestinations     map[Id]map[TransferPath]bool
	writerStreamAliasScopes                   map[Id]*writerStreamAliasScope
	writerPendingAuthenticatedStreams         []Id
	writerStreamAliasGenerations              map[Id]uint64
	writerNextStreamAliasGeneration           uint64
	writerStreamAuthenticatedDestinationCount int
}

// One endpoint StreamSequence generation owns the persistent alias references
// for a stream. A replacement generation transfers this ownership without
// changing the underlying MatchState reference counts.
type writerStreamAliasScope struct {
	destinations map[TransferPath]bool
	generation   uint64
}

// Bounds verified contract knowledge that arrives before its endpoint
// StreamOpen. Live stream scopes are not part of this bound.
const maxPendingWriterStreamAliases = 32

// Bounds authoritative stream generations retained between StreamOpen and
// StreamClose/StreamReset controls.
const maxWriterStreamAliasGenerations = 1024

// Bounds authenticated final peers retained from any one remotely supplied
// stream identity.
const maxWriterStreamAliasDestinationsPerStream = 32

// Bounds all authenticated stream-to-final peer relationships retained by one
// RouteManager.
const maxWriterStreamAliasDestinations = 4096

// Matches the live StreamBuffer sequence bound.
const maxWriterStreamAliasScopes = 1024

func NewRouteManager(ctx context.Context, clientTag string) *RouteManager {
	return NewRouteManagerWithLogger(ctx, clientTag, nil)
}

func NewRouteManagerWithLogger(ctx context.Context, clientTag string, log Logger) *RouteManager {
	log = loggerOrDefault(log)
	return &RouteManager{
		ctx:              ctx,
		clientTag:        clientTag,
		log:              log,
		writerMatchState: NewMatchState(ctx, clientTag, log, true, Transport.MatchesSend),
		// `weightedRoutes=false` because unless there is a cpu limit this is not needed
		readerMatchState:                      NewMatchState(ctx, clientTag, log, false, Transport.MatchesReceive),
		writerStreamAuthenticatedDestinations: map[Id]map[TransferPath]bool{},
		writerStreamAliasScopes:               map[Id]*writerStreamAliasScope{},
		writerStreamAliasGenerations:          map[Id]uint64{},
		pendingWriterSnapshots:                map[*routeSnapshot]bool{},
		pendingWriterSnapshotsByTransport:     map[Transport]map[*routeSnapshot]bool{},
		pendingWriterSnapshotsBySelector:      map[*MultiRouteSelector]map[*routeSnapshot]bool{},
	}
}

func (self *RouteManager) DowngradeReceiverConnection(source TransferPath) {
	self.readerMatchState.Downgrade(source)
}

func (self *RouteManager) OpenMultiRouteWriter(destination TransferPath) MultiRouteWriter {
	if !destination.IsDestinationMask() {
		panic(fmt.Errorf("Destination required for writer: %s", destination))
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()

	return MultiRouteWriter(self.writerMatchState.openMultiRouteSelector(destination))
}

func (self *RouteManager) CloseMultiRouteWriter(w MultiRouteWriter) {
	self.transportUpdateLock.Lock()
	self.mutex.Lock()
	selector := w.(*MultiRouteSelector)
	self.writerMatchState.closeMultiRouteSelector(selector)
	retiredSnapshots := selector.closeAndTakeRetiredWriterSnapshots()
	self.registerPendingWriterSnapshotsWithLock(retiredSnapshots)
	pendingSnapshots := self.pendingWriterSnapshotsForSelectorWithLock(selector)
	self.mutex.Unlock()
	self.transportUpdateLock.Unlock()

	self.waitAndForgetPendingWriterSnapshots(pendingSnapshots)
}

// Publishes one writer-route mutation, transfers every
// retired snapshot into the pending indexes, and joins current writer work
// after releasing publication and state locks.
func (self *RouteManager) updateWriterMatchState(update func()) {
	self.transportUpdateLock.Lock()
	self.mutex.Lock()
	update()
	retiredSnapshots := self.writerMatchState.takeRetiredWriterSnapshots()
	self.registerPendingWriterSnapshotsWithLock(retiredSnapshots)
	self.mutex.Unlock()
	self.transportUpdateLock.Unlock()

	self.waitAndForgetPendingWriterSnapshots(retiredSnapshots)
}

// Publishes one callback-owned mutation and
// registers every admitted old snapshot before returning without a join.
func (self *RouteManager) updateWriterMatchStateAsync(update func()) {
	self.transportUpdateLock.Lock()
	self.mutex.Lock()
	update()
	retiredSnapshots := self.writerMatchState.takeRetiredWriterSnapshots()
	self.registerPendingWriterSnapshotsWithLock(retiredSnapshots)
	self.mutex.Unlock()
	self.transportUpdateLock.Unlock()
}

// Indexes admitted generations by each
// transport they could still send to and by their owning selector.
func (self *RouteManager) registerPendingWriterSnapshotsWithLock(
	snapshots []*routeSnapshot,
) {
	self.prunePendingWriterSnapshotsWithLock()
	for _, snapshot := range snapshots {
		if self.pendingWriterSnapshots[snapshot] {
			panic("writer snapshot registered twice")
		}
		self.pendingWriterSnapshots[snapshot] = true
		selectorSnapshots := self.pendingWriterSnapshotsBySelector[snapshot.selector]
		if selectorSnapshots == nil {
			selectorSnapshots = map[*routeSnapshot]bool{}
			self.pendingWriterSnapshotsBySelector[snapshot.selector] = selectorSnapshots
		}
		selectorSnapshots[snapshot] = true
		for _, transport := range snapshot.transports {
			transportSnapshots := self.pendingWriterSnapshotsByTransport[transport]
			if transportSnapshots == nil {
				transportSnapshots = map[*routeSnapshot]bool{}
				self.pendingWriterSnapshotsByTransport[transport] = transportSnapshots
			}
			transportSnapshots[snapshot] = true
		}
	}
}

// Removes one completed generation from
// every ownership index.
func (self *RouteManager) removePendingWriterSnapshotWithLock(
	snapshot *routeSnapshot,
) {
	delete(self.pendingWriterSnapshots, snapshot)
	selectorSnapshots := self.pendingWriterSnapshotsBySelector[snapshot.selector]
	delete(selectorSnapshots, snapshot)
	if len(selectorSnapshots) == 0 {
		delete(self.pendingWriterSnapshotsBySelector, snapshot.selector)
	}
	for _, transport := range snapshot.transports {
		transportSnapshots := self.pendingWriterSnapshotsByTransport[transport]
		delete(transportSnapshots, snapshot)
		if len(transportSnapshots) == 0 {
			delete(self.pendingWriterSnapshotsByTransport, transport)
		}
	}
}

// Releases tracker references whose final
// admitted writer has already exited.
func (self *RouteManager) prunePendingWriterSnapshotsWithLock() {
	for snapshot := range self.pendingWriterSnapshots {
		select {
		case <-snapshot.writerDone:
			self.removePendingWriterSnapshotWithLock(snapshot)
		default:
		}
	}
}

// Snapshots only generations that
// could still enqueue to one physical transport.
func (self *RouteManager) pendingWriterSnapshotsForTransportWithLock(
	transport Transport,
) []*routeSnapshot {
	self.prunePendingWriterSnapshotsWithLock()
	return slices.Collect(maps.Keys(self.pendingWriterSnapshotsByTransport[transport]))
}

// Snapshots only generations owned
// by one closing writer selector.
func (self *RouteManager) pendingWriterSnapshotsForSelectorWithLock(
	selector *MultiRouteSelector,
) []*routeSnapshot {
	self.prunePendingWriterSnapshotsWithLock()
	return slices.Collect(maps.Keys(self.pendingWriterSnapshotsBySelector[selector]))
}

// Joins captured generations without
// holding route state, then drops their completed tracker references.
func (self *RouteManager) waitAndForgetPendingWriterSnapshots(
	snapshots []*routeSnapshot,
) {
	waitForRouteSnapshotWriters(snapshots)
	self.transportUpdateLock.Lock()
	self.mutex.Lock()
	for _, snapshot := range snapshots {
		if self.pendingWriterSnapshots[snapshot] {
			self.removePendingWriterSnapshotWithLock(snapshot)
		}
	}
	self.mutex.Unlock()
	self.transportUpdateLock.Unlock()
}

// Adds an alternative transport-match key for every writer to one logical
// destination. Registrations are ref-counted, and removal is idempotent.
func (self *RouteManager) AddWriterDestinationAlias(destination TransferPath, alias TransferPath) func() {
	if !destination.IsDestinationMask() || destination.IsStream() || destination.DestinationId == (Id{}) {
		panic(fmt.Errorf("Final destination required for writer alias: %s", destination))
	}
	if !alias.IsDestinationMask() || alias == (TransferPath{}) {
		panic(fmt.Errorf("Destination mask required for writer alias: %s", alias))
	}

	self.updateWriterMatchState(func() {
		self.writerMatchState.addDestinationAliasWithLock(destination, alias)
	})

	removeOnce := sync.Once{}
	return func() {
		removeOnce.Do(func() {
			self.updateWriterMatchState(func() {
				self.writerMatchState.removeDestinationAliasWithLock(destination, alias)
			})
		})
	}
}

// Removes one stream from the small pending-authentication order. The caller
// holds RouteManager.mutex.
func (self *RouteManager) removePendingAuthenticatedStreamWithLock(streamId Id) {
	for pendingIndex, pendingStreamId := range self.writerPendingAuthenticatedStreams {
		if pendingStreamId == streamId {
			self.writerPendingAuthenticatedStreams = slices.Delete(
				self.writerPendingAuthenticatedStreams,
				pendingIndex,
				pendingIndex+1,
			)
			return
		}
	}
}

// Evicts the oldest authentication records that still have no endpoint scope.
// The caller holds RouteManager.mutex.
func (self *RouteManager) trimPendingAuthenticatedStreamsWithLock() {
	for maxPendingWriterStreamAliases < len(self.writerPendingAuthenticatedStreams) {
		streamId := self.writerPendingAuthenticatedStreams[0]
		self.writerPendingAuthenticatedStreams = self.writerPendingAuthenticatedStreams[1:]
		if self.writerStreamAliasScopes[streamId] == nil {
			self.removeWriterStreamAuthenticatedDestinationsWithLock(streamId)
		}
	}
}

// Drops one stream's
// retained final peers and updates the RouteManager-wide bound.
func (self *RouteManager) removeWriterStreamAuthenticatedDestinationsWithLock(
	streamId Id,
) {
	destinations := self.writerStreamAuthenticatedDestinations[streamId]
	self.writerStreamAuthenticatedDestinationCount -= len(destinations)
	if self.writerStreamAuthenticatedDestinationCount < 0 {
		panic("writer stream authenticated destination count became negative")
	}
	delete(self.writerStreamAuthenticatedDestinations, streamId)
}

// Installs a unique lifecycle token without
// activating aliases. Close/reset invalidates this token atomically with alias
// removal, so construction that finishes late cannot reopen a retired scope.
func (self *RouteManager) beginWriterStreamAliasGeneration(streamId Id) (uint64, bool) {
	if streamId == (Id{}) {
		panic(errors.New("Stream id required for writer alias scope."))
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()
	if _, ok := self.writerStreamAliasGenerations[streamId]; !ok && maxWriterStreamAliasGenerations <= len(self.writerStreamAliasGenerations) {
		return 0, false
	}
	generation := self.nextWriterStreamAliasGenerationWithLock()
	self.writerStreamAliasGenerations[streamId] = generation
	return generation, true
}

// Allocates an ordering epoch without
// installing a stream token. StreamReset uses it to protect later opens.
func (self *RouteManager) writerStreamAliasGenerationCheckpoint() uint64 {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.nextWriterStreamAliasGenerationWithLock()
}

// Allocates a nonzero epoch while
// RouteManager.mutex is held.
func (self *RouteManager) nextWriterStreamAliasGenerationWithLock() uint64 {
	self.writerNextStreamAliasGeneration += 1
	if self.writerNextStreamAliasGeneration == 0 {
		self.writerNextStreamAliasGeneration += 1
	}
	return self.writerNextStreamAliasGeneration
}

// Reports whether a lifecycle request is
// still authoritative after a concurrent close, reset, or superseding open.
func (self *RouteManager) isWriterStreamAliasGenerationCurrent(streamId Id, generation uint64) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return generation != 0 && self.writerStreamAliasGenerations[streamId] == generation
}

// Drops a terminal worker token without
// changing its live scope or authenticated destinations. A newer token wins.
func (self *RouteManager) finishWriterStreamAliasGeneration(streamId Id, generation uint64) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if generation != 0 && self.writerStreamAliasGenerations[streamId] == generation {
		delete(self.writerStreamAliasGenerations, streamId)
	}
}

// Invalidates every unpublished
// token ordered before a reset checkpoint without changing live alias scopes.
func (self *RouteManager) finishWriterStreamAliasGenerationsThrough(generation uint64) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	for streamId, streamGeneration := range self.writerStreamAliasGenerations {
		if streamGeneration <= generation {
			delete(self.writerStreamAliasGenerations, streamId)
		}
	}
}

// Clears a token and its aliases only
// if no newer StreamOpen has superseded it.
func (self *RouteManager) invalidateWriterStreamAliasGeneration(streamId Id, generation uint64) {
	self.updateWriterMatchState(func() {
		if generation != 0 && self.writerStreamAliasGenerations[streamId] == generation {
			self.clearWriterStreamAliasScopeWithLock(streamId)
		}
	})
}

// Activates an endpoint scope only
// while its lifecycle token remains current. A replacement inherits existing
// references, and the old generation's close function becomes a no-op.
func (self *RouteManager) openWriterStreamAliasScopeForGeneration(
	streamId Id,
	generation uint64,
) (func(), bool) {
	var closeScope func()
	var opened bool
	self.updateWriterMatchState(func() {
		if generation == 0 || self.writerStreamAliasGenerations[streamId] != generation {
			return
		}
		previousScope := self.writerStreamAliasScopes[streamId]
		if previousScope == nil && maxWriterStreamAliasScopes <= len(self.writerStreamAliasScopes) {
			return
		}

		scope := &writerStreamAliasScope{
			destinations: map[TransferPath]bool{},
			generation:   generation,
		}
		self.removePendingAuthenticatedStreamWithLock(streamId)

		if previousScope != nil {
			for destination := range previousScope.destinations {
				scope.destinations[destination] = true
			}
		} else {
			alias := StreamId(streamId)
			for destination := range self.writerStreamAuthenticatedDestinations[streamId] {
				self.writerMatchState.addDestinationAliasWithLock(destination, alias)
				scope.destinations[destination] = true
			}
		}
		self.writerStreamAliasScopes[streamId] = scope

		closeOnce := sync.Once{}
		closeScope = func() {
			closeOnce.Do(func() {
				self.updateWriterMatchState(func() {
					if self.writerStreamAliasScopes[streamId] != scope {
						return
					}
					alias := StreamId(streamId)
					for destination := range scope.destinations {
						self.writerMatchState.removeDestinationAliasWithLock(destination, alias)
					}
					delete(self.writerStreamAliasScopes, streamId)
					if 0 < len(self.writerStreamAuthenticatedDestinations[streamId]) {
						self.writerPendingAuthenticatedStreams = append(
							self.writerPendingAuthenticatedStreams,
							streamId,
						)
						self.trimPendingAuthenticatedStreamsWithLock()
					}
				})
			})
		}
		opened = true
	})
	return closeScope, opened
}

// Opens a standalone generation for focused route
// tests. Production StreamSequence lifecycle uses the generation-gated form.
func (self *RouteManager) openWriterStreamAliasScope(streamId Id) func() {
	generation, ok := self.beginWriterStreamAliasGeneration(streamId)
	if !ok {
		panic(errors.New("Writer stream alias generation limit reached."))
	}
	closeScope, ok := self.openWriterStreamAliasScopeForGeneration(streamId, generation)
	if !ok {
		panic(errors.New("Writer stream alias generation superseded."))
	}
	self.finishWriterStreamAliasGeneration(streamId, generation)
	return closeScope
}

// Records one final destination authenticated by a stream-stamped contract.
// Intermediaries never verify endpoint contracts and never open endpoint
// scopes, so they cannot create a live final-destination alias. The return
// reports whether an endpoint scope is currently live.
func (self *RouteManager) authenticateWriterStreamDestination(
	streamId Id,
	destinationId Id,
) bool {
	if streamId == (Id{}) || destinationId == (Id{}) {
		return false
	}
	destination := DestinationId(destinationId)
	alias := StreamId(streamId)

	var scopeLive bool
	self.updateWriterMatchStateAsync(func() {
		destinations := self.writerStreamAuthenticatedDestinations[streamId]
		scope := self.writerStreamAliasScopes[streamId]
		if destinations[destination] {
			scopeLive = scope != nil
			return
		}
		if maxWriterStreamAliasDestinationsPerStream <= len(destinations) ||
			maxWriterStreamAliasDestinations <= self.writerStreamAuthenticatedDestinationCount {
			return
		}
		if destinations == nil {
			destinations = map[TransferPath]bool{}
			self.writerStreamAuthenticatedDestinations[streamId] = destinations
			if self.writerStreamAliasScopes[streamId] == nil {
				self.writerPendingAuthenticatedStreams = append(
					self.writerPendingAuthenticatedStreams,
					streamId,
				)
				self.trimPendingAuthenticatedStreamsWithLock()
			}
		}
		destinations[destination] = true
		self.writerStreamAuthenticatedDestinationCount += 1

		if scope == nil {
			return
		}
		if !scope.destinations[destination] {
			self.writerMatchState.addDestinationAliasWithLock(destination, alias)
			scope.destinations[destination] = true
		}
		scopeLive = true
	})
	return scopeLive
}

// Clears both the live endpoint references and the authenticated destinations
// after an authoritative StreamClose or policy retirement. A stale generation
// close cannot affect a replacement scope because scopes are pointer-owned.
func (self *RouteManager) clearWriterStreamAliasScope(streamId Id) {
	self.updateWriterMatchState(func() {
		self.clearWriterStreamAliasScopeWithLock(streamId)
	})
}

// Clears an authoritative
// snapshot only when no later stream generation or live scope has superseded
// it. The return reports whether the clear was applied.
func (self *RouteManager) clearWriterStreamAliasScopeThroughGeneration(
	streamId Id,
	generation uint64,
) bool {
	cleared := false
	self.updateWriterMatchState(func() {
		if generation < self.writerStreamAliasGenerations[streamId] {
			return
		}
		if scope := self.writerStreamAliasScopes[streamId]; scope != nil && generation < scope.generation {
			return
		}
		self.clearWriterStreamAliasScopeWithLock(streamId)
		cleared = true
	})
	return cleared
}

// Publishes the conditional
// clear and registers admitted old writers for later targeted teardown.
func (self *RouteManager) clearWriterStreamAliasScopeThroughGenerationAsync(
	streamId Id,
	generation uint64,
) bool {
	cleared := false
	self.updateWriterMatchStateAsync(func() {
		if generation < self.writerStreamAliasGenerations[streamId] {
			return
		}
		if scope := self.writerStreamAliasScopes[streamId]; scope != nil && generation < scope.generation {
			return
		}
		self.clearWriterStreamAliasScopeWithLock(streamId)
		cleared = true
	})
	return cleared
}

// Clears one stream while RouteManager.mutex is held.
func (self *RouteManager) clearWriterStreamAliasScopeWithLock(streamId Id) {

	if scope := self.writerStreamAliasScopes[streamId]; scope != nil {
		alias := StreamId(streamId)
		for destination := range scope.destinations {
			self.writerMatchState.removeDestinationAliasWithLock(destination, alias)
		}
		delete(self.writerStreamAliasScopes, streamId)
	}
	self.removeWriterStreamAuthenticatedDestinationsWithLock(streamId)
	delete(self.writerStreamAliasGenerations, streamId)
	self.removePendingAuthenticatedStreamWithLock(streamId)
}

// Clears every live or pending stream not present in an authoritative reset.
func (self *RouteManager) clearWriterStreamAliasScopesExcept(keep map[Id]bool) {
	self.updateWriterMatchState(func() {
		retired := map[Id]bool{}
		for streamId := range self.writerStreamAliasScopes {
			if !keep[streamId] {
				retired[streamId] = true
			}
		}
		for streamId := range self.writerStreamAuthenticatedDestinations {
			if !keep[streamId] {
				retired[streamId] = true
			}
		}
		for streamId := range self.writerStreamAliasGenerations {
			if !keep[streamId] {
				retired[streamId] = true
			}
		}
		for streamId := range retired {
			self.clearWriterStreamAliasScopeWithLock(streamId)
		}
	})
}

// Applies a reset only to
// state at or before its checkpoint; later StreamOpen generations survive.
func (self *RouteManager) clearWriterStreamAliasScopesExceptThroughGeneration(
	keep map[Id]bool,
	generation uint64,
) {
	self.updateWriterMatchState(func() {
		retired := map[Id]bool{}
		for streamId := range self.writerStreamAliasScopes {
			if !keep[streamId] {
				retired[streamId] = true
			}
		}
		for streamId := range self.writerStreamAuthenticatedDestinations {
			if !keep[streamId] {
				retired[streamId] = true
			}
		}
		for streamId := range self.writerStreamAliasGenerations {
			if !keep[streamId] {
				retired[streamId] = true
			}
		}
		for streamId := range retired {
			if generation < self.writerStreamAliasGenerations[streamId] {
				continue
			}
			if scope := self.writerStreamAliasScopes[streamId]; scope != nil && generation < scope.generation {
				continue
			}
			self.clearWriterStreamAliasScopeWithLock(streamId)
		}
	})
}

// Publishes a reset
// and registers admitted old writers for later targeted teardown.
func (self *RouteManager) clearWriterStreamAliasScopesExceptThroughGenerationAsync(
	keep map[Id]bool,
	generation uint64,
) {
	self.updateWriterMatchStateAsync(func() {
		retired := map[Id]bool{}
		for streamId := range self.writerStreamAliasScopes {
			if !keep[streamId] {
				retired[streamId] = true
			}
		}
		for streamId := range self.writerStreamAuthenticatedDestinations {
			if !keep[streamId] {
				retired[streamId] = true
			}
		}
		for streamId := range self.writerStreamAliasGenerations {
			if !keep[streamId] {
				retired[streamId] = true
			}
		}
		for streamId := range retired {
			if generation < self.writerStreamAliasGenerations[streamId] {
				continue
			}
			if scope := self.writerStreamAliasScopes[streamId]; scope != nil && generation < scope.generation {
				continue
			}
			self.clearWriterStreamAliasScopeWithLock(streamId)
		}
	})
}

func (self *RouteManager) OpenMultiRouteReader(destination TransferPath) MultiRouteReader {
	if !destination.IsDestinationMask() {
		panic(fmt.Errorf("Destination required for reader: %s", destination))
	}

	self.mutex.Lock()
	defer self.mutex.Unlock()

	return MultiRouteReader(self.readerMatchState.openMultiRouteSelector(destination))
}

func (self *RouteManager) CloseMultiRouteReader(r MultiRouteReader) {
	self.transportUpdateLock.Lock()
	self.mutex.Lock()
	selector := r.(*MultiRouteSelector)
	self.readerMatchState.closeMultiRouteSelector(selector)
	self.mutex.Unlock()
	self.transportUpdateLock.Unlock()

	selector.Close()
}

func (self *RouteManager) UpdateTransport(transport Transport, routes []Route) {
	self.UpdateTransportWithProperties(transport, routes, TransferCarrierProperties{})
}

// Publishes routes and immutable delivery semantics for one transport
// generation. Removing a transport ignores properties and clears both.
func (self *RouteManager) UpdateTransportWithProperties(
	transport Transport,
	routes []Route,
	properties TransferCarrierProperties,
) {
	self.transportUpdateLock.Lock()
	self.mutex.Lock()
	retiredSnapshots := self.writerMatchState.updateTransportWithProperties(
		transport,
		routes,
		properties,
	)
	retiredSnapshots = append(
		retiredSnapshots,
		self.readerMatchState.updateTransportWithProperties(
			transport,
			routes,
			properties,
		)...,
	)
	self.registerPendingWriterSnapshotsWithLock(retiredSnapshots)
	pendingSnapshots := self.pendingWriterSnapshotsForTransportWithLock(transport)
	self.mutex.Unlock()
	self.transportUpdateLock.Unlock()

	self.waitAndForgetPendingWriterSnapshots(pendingSnapshots)
}

func (self *RouteManager) RemoveTransport(transport Transport) {
	self.UpdateTransport(transport, nil)
}

// HasActiveTransport reports whether any transport is currently registered
// with routes. The transport set is the ground truth for whether this client
// has a carrier at all: transports register on (re)connect via
// UpdateTransport and are removed when their connection dies, so an empty set
// means nothing this client sends can leave the device and nothing can
// arrive. Consumers use that to rule the client's silence inadmissible as
// evidence against the remote end -- see detectBlackhole and sendStalled in
// the multi client. Both match states are read because UpdateTransport writes
// them together but a send-only or receive-only transport is still a carrier.
func (self *RouteManager) HasActiveTransport() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	return 0 < len(self.writerMatchState.transportRoutes) || 0 < len(self.readerMatchState.transportRoutes)
}

// HasActiveUnreliableSendTransport reports whether an active writer carrier
// can lose a complete Transfer frame without reporting a transport failure.
// A registered QUIC DATAGRAM route is the current production example. Callers
// use this distinction when missing Transfer acknowledgements would otherwise
// be treated as proof that the remote provider is dead.
func (self *RouteManager) HasActiveUnreliableSendTransport() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	for transport, routes := range self.writerMatchState.transportRoutes {
		if 0 < len(routes) && self.writerMatchState.transportProperties[transport].Unreliable {
			return true
		}
	}
	return false
}

// DirectCarrierAffinityStats returns lifetime writer-routing counters for this
// client. They remain valid across SendSequence creation and retirement, so a
// measurement can distinguish intentional selector affinity, backpressure,
// physical route replacement, and any unexpected tied-route spill.
func (self *RouteManager) DirectCarrierAffinityStats() DirectCarrierAffinityStats {
	return self.writerMatchState.directAffinity.snapshot()
}

func (self *RouteManager) getTransportStats(transport Transport) (writerStats *RouteStats, readerStats *RouteStats) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	writerStats = self.writerMatchState.getTransportStats(transport)
	readerStats = self.readerMatchState.getTransportStats(transport)
	return
}

type MatchState struct {
	ctx       context.Context
	clientTag string
	log       Logger

	weightedRoutes bool
	matches        func(Transport, TransferPath) bool
	directAffinity *directCarrierAffinityCounters

	transportRoutes     map[Transport][]Route
	transportProperties map[Transport]TransferCarrierProperties

	// destination -> multi route selectors
	destinationMultiRouteSelectors map[TransferPath]map[*MultiRouteSelector]bool
	// logical destination -> alternative transport-match destination -> refs
	destinationAliases map[TransferPath]map[TransferPath]int

	// transport -> destinations
	transportMatchedDestinations map[Transport]map[TransferPath]bool
}

// note weighted routes typically are used by the sender not receiver
func NewMatchState(ctx context.Context, clientTag string, log Logger, weightedRoutes bool, matches func(Transport, TransferPath) bool) *MatchState {
	directAffinity := &directCarrierAffinityCounters{}
	return &MatchState{
		ctx:                            ctx,
		clientTag:                      clientTag,
		log:                            loggerOrDefault(log),
		weightedRoutes:                 weightedRoutes,
		matches:                        matches,
		directAffinity:                 directAffinity,
		transportRoutes:                map[Transport][]Route{},
		transportProperties:            map[Transport]TransferCarrierProperties{},
		destinationMultiRouteSelectors: map[TransferPath]map[*MultiRouteSelector]bool{},
		destinationAliases:             map[TransferPath]map[TransferPath]int{},
		transportMatchedDestinations:   map[Transport]map[TransferPath]bool{},
	}
}

func (self *MatchState) getTransportStats(transport Transport) *RouteStats {
	destinations, ok := self.transportMatchedDestinations[transport]
	if !ok {
		return nil
	}
	netStats := NewRouteStats()
	for destination, _ := range destinations {
		if multiRouteSelectors, ok := self.destinationMultiRouteSelectors[destination]; ok {
			for multiRouteSelector, _ := range multiRouteSelectors {
				if stats := multiRouteSelector.getTransportStats(transport); stats != nil {
					netStats.sendCount += stats.sendCount
					netStats.sendByteCount += stats.sendByteCount
					netStats.receiveCount += stats.receiveCount
					netStats.receiveByteCount += stats.receiveByteCount
				}
			}
		}
	}
	return netStats
}

func (self *MatchState) openMultiRouteSelector(destination TransferPath) *MultiRouteSelector {
	multiRouteSelector := NewMultiRouteSelector(self.ctx, self.clientTag, self.log, destination, self.weightedRoutes)
	multiRouteSelector.directAffinity = self.directAffinity

	multiRouteSelectors, ok := self.destinationMultiRouteSelectors[destination]
	if !ok {
		multiRouteSelectors = map[*MultiRouteSelector]bool{}
		self.destinationMultiRouteSelectors[destination] = multiRouteSelectors
	}
	multiRouteSelectors[multiRouteSelector] = true
	for transport, routes := range self.transportRoutes {
		matchedDestinations, ok := self.transportMatchedDestinations[transport]
		if !ok {
			matchedDestinations = map[TransferPath]bool{}
			self.transportMatchedDestinations[transport] = matchedDestinations
		}

		// use the latest matches state
		if self.matchesMultiRouteSelectorWithLock(transport, multiRouteSelector) {
			matchedDestinations[destination] = true
			multiRouteSelector.updateTransportWithProperties(
				transport,
				routes,
				self.transportProperties[transport],
			)
		}
	}

	return multiRouteSelector
}

// Matches the logical destination or any verified route alias for it.
func (self *MatchState) matchesMultiRouteSelectorWithLock(
	transport Transport,
	multiRouteSelector *MultiRouteSelector,
) bool {
	destination := multiRouteSelector.destination
	if self.matches(transport, destination) {
		return true
	}
	for alias := range self.destinationAliases[destination] {
		if self.matches(transport, alias) {
			return true
		}
	}
	return false
}

// Adds one alias reference and rematches existing selectors on its first use.
func (self *MatchState) addDestinationAliasWithLock(destination TransferPath, alias TransferPath) {
	aliases, ok := self.destinationAliases[destination]
	if !ok {
		aliases = map[TransferPath]int{}
		self.destinationAliases[destination] = aliases
	}
	aliases[alias] += 1
	if aliases[alias] == 1 {
		self.rematchDestinationWithLock(destination)
	}
}

// Removes one alias reference and rematches selectors after its last use.
func (self *MatchState) removeDestinationAliasWithLock(destination TransferPath, alias TransferPath) {
	aliases, ok := self.destinationAliases[destination]
	if !ok || aliases[alias] == 0 {
		return
	}
	aliases[alias] -= 1
	if 0 < aliases[alias] {
		return
	}
	delete(aliases, alias)
	if len(aliases) == 0 {
		delete(self.destinationAliases, destination)
	}
	self.rematchDestinationWithLock(destination)
}

// Reconciles every open selector after route knowledge changes.
func (self *MatchState) rematchDestinationWithLock(destination TransferPath) {
	multiRouteSelectors, ok := self.destinationMultiRouteSelectors[destination]
	if !ok {
		return
	}
	for transport, routes := range self.transportRoutes {
		matchedDestinations, ok := self.transportMatchedDestinations[transport]
		if !ok {
			matchedDestinations = map[TransferPath]bool{}
			self.transportMatchedDestinations[transport] = matchedDestinations
		}
		for multiRouteSelector := range multiRouteSelectors {
			matches := self.matchesMultiRouteSelectorWithLock(transport, multiRouteSelector)
			currentlyMatched := multiRouteSelector.hasTransport(transport)
			if matches == currentlyMatched {
				continue
			}
			if matches {
				multiRouteSelector.updateTransportWithProperties(
					transport,
					routes,
					self.transportProperties[transport],
				)
			} else {
				multiRouteSelector.updateTransport(transport, nil)
			}
		}
		if self.anyMultiRouteSelectorMatchesWithLock(transport, destination) {
			matchedDestinations[destination] = true
		} else {
			delete(matchedDestinations, destination)
		}
	}
}

// Reports whether any selector for one logical destination uses a transport.
func (self *MatchState) anyMultiRouteSelectorMatchesWithLock(
	transport Transport,
	destination TransferPath,
) bool {
	for multiRouteSelector := range self.destinationMultiRouteSelectors[destination] {
		if multiRouteSelector.hasTransport(transport) {
			return true
		}
	}
	return false
}

func (self *MatchState) closeMultiRouteSelector(multiRouteSelector *MultiRouteSelector) {
	// TODO readers do not need to prioritize routes

	destination := multiRouteSelector.destination
	multiRouteSelectors, ok := self.destinationMultiRouteSelectors[destination]
	if !ok {
		// not present
		return
	}
	delete(multiRouteSelectors, multiRouteSelector)

	if len(multiRouteSelectors) == 0 {
		// clean up the destination so the maps don't grow monotonically
		// for every destination ever talked to
		for _, matchedDestinations := range self.transportMatchedDestinations {
			delete(matchedDestinations, destination)
		}
		delete(self.destinationMultiRouteSelectors, destination)
	} else {
		for transport, matchedDestinations := range self.transportMatchedDestinations {
			if self.anyMultiRouteSelectorMatchesWithLock(transport, destination) {
				matchedDestinations[destination] = true
			} else {
				delete(matchedDestinations, destination)
			}
		}
	}
}

func (self *MatchState) updateTransport(
	transport Transport,
	routes []Route,
) []*routeSnapshot {
	return self.updateTransportWithProperties(
		transport,
		routes,
		TransferCarrierProperties{},
	)
}

// Reconciles one transport and its delivery semantics across every selector.
func (self *MatchState) updateTransportWithProperties(
	transport Transport,
	routes []Route,
	properties TransferCarrierProperties,
) []*routeSnapshot {
	if len(routes) == 0 {
		for _, multiRouteSelectors := range self.destinationMultiRouteSelectors {
			for multiRouteSelector := range multiRouteSelectors {
				multiRouteSelector.updateTransport(transport, nil)
			}
		}

		delete(self.transportMatchedDestinations, transport)
		delete(self.transportRoutes, transport)
		delete(self.transportProperties, transport)
	} else {
		matchedDestinations := map[TransferPath]bool{}

		for destination, multiRouteSelectors := range self.destinationMultiRouteSelectors {
			for multiRouteSelector := range multiRouteSelectors {
				if self.matchesMultiRouteSelectorWithLock(transport, multiRouteSelector) {
					multiRouteSelector.updateTransportWithProperties(
						transport,
						routes,
						properties,
					)
					matchedDestinations[destination] = true
				} else if multiRouteSelector.hasTransport(transport) {
					multiRouteSelector.updateTransport(transport, nil)
				}
			}
		}

		self.transportMatchedDestinations[transport] = matchedDestinations
		self.transportRoutes[transport] = routes
		self.transportProperties[transport] = properties
	}

	return self.takeRetiredWriterSnapshots()
}

// Transfers every pending selector generation to
// its route-manager lifecycle operation. The caller holds RouteManager.mutex.
func (self *MatchState) takeRetiredWriterSnapshots() []*routeSnapshot {
	retiredSnapshots := []*routeSnapshot{}
	for _, multiRouteSelectors := range self.destinationMultiRouteSelectors {
		for multiRouteSelector := range multiRouteSelectors {
			retiredSnapshots = append(
				retiredSnapshots,
				multiRouteSelector.takeRetiredWriterSnapshots()...,
			)
		}
	}
	return retiredSnapshots
}

func (self *MatchState) Downgrade(source TransferPath) {
	for transport, _ := range self.transportRoutes {
		transport.Downgrade(source)
	}
}

type MultiRouteSelector struct {
	ctx       context.Context
	cancel    context.CancelFunc
	clientTag string
	log       Logger

	destination    TransferPath
	weightedRoutes bool
	// routeAckProgress holds one clock per route: the unix nanos of the newest
	// Transfer acknowledgement of an item written on it. Stored from the
	// sender goroutine, read by watchdogs and stats.
	routeAckProgress sync.Map

	transportUpdate *Monitor

	mutex               sync.Mutex
	transportRoutes     map[Transport][]Route
	transportProperties map[Transport]TransferCarrierProperties
	routeStats          map[Route]*RouteStats
	routeActive         map[Route]bool
	routeWeight         map[Route]float32
	// preferredDirectRoute keeps one destination-keyed Transfer sequence on
	// the first healthy equal-priority H1/H3 carrier. Both transports remain
	// registered, but transient queue pressure is backpressure rather than
	// permission to stripe onto the sibling congestion controller. Withdrawal
	// closes snapshot notifications and promotes the survivor.
	preferredDirectRoute Route
	directAffinity       *directCarrierAffinityCounters
	nextRouteGeneration  uint64

	// activeRoutesSnapshot is an immutable snapshot of the active routes, their
	// weights, and the transport-update notify channel. it is rebuilt under
	// `mutex` (via `updateActiveRoutesWithLock`) whenever the active routes,
	// weights, or transport set change, and read lock-free on the per-packet
	// `Read`/`Write` path. previously each `Read`/`Write` took `mutex` (and
	// allocated a new slice) in `GetActiveRoutes` plus took the monitor lock in
	// `NotifyChannel` — both on every packet. the route set changes rarely, so
	// the snapshot moves that work off the hot path.
	activeRoutesSnapshot atomic.Pointer[routeSnapshot]
	// retiredWriterSnapshots contains only generations that still had an
	// admitted writer when they were replaced. RouteManager joins them after
	// releasing its state lock.
	retiredWriterSnapshots []*routeSnapshot

	// Nil outside tests. Installed observers receive immutable route-state
	// generations through atomic links, so publication never calls or waits on
	// test code while holding the selector lock.
	testingRouteStateObservers map[*TestingMultiRouteWriterRouteStateObserver]bool

	// A selector reader is one ordered packet stream. Serializing Read also
	// lets it reuse one lazy timeout timer instead of allocating time.After
	// state on every packet. A selector writer is likewise one ordered packet
	// stream; serialization preserves that order and lets blocked writes reuse
	// one bounded timer instead of allocating two runtime timer objects per
	// backpressured transfer frame.
	readMutex  sync.Mutex
	readTimer  *time.Timer
	writeMutex sync.Mutex
	writeTimer *time.Timer
	closeOnce  sync.Once
}

// TestingMultiRouteWriterRouteState identifies one exactly published selector
// generation. Tests use the generation as a barrier so a stale matching count
// from before a requested transition cannot satisfy a later readiness proof.
type TestingMultiRouteWriterRouteState struct {
	Generation         uint64
	ActiveRouteCount   int
	InactiveRouteCount int
}

// A node retains every test-observed generation until the observer closes.
// Publication links the next immutable node before waking waiters.
type testingMultiRouteWriterRouteStateNode struct {
	state   TestingMultiRouteWriterRouteState
	next    atomic.Pointer[testingMultiRouteWriterRouteStateNode]
	changed chan struct{}
}

// TestingMultiRouteWriterRouteStateObserver is an opt-in, exact route-state
// event stream. It has no production publisher or allocation when absent.
// Snapshot, WaitAfter, and Close are safe to call concurrently.
type TestingMultiRouteWriterRouteStateObserver struct {
	selector       *MultiRouteSelector
	head           *testingMultiRouteWriterRouteStateNode
	tail           atomic.Pointer[testingMultiRouteWriterRouteStateNode]
	nextGeneration atomic.Uint64
	closed         atomic.Bool
	closeOnce      sync.Once
}

// An immutable hot-path route view keeps its writer lifecycle and optional
// testing pause in independent atomics.
type routeSnapshot struct {
	selector            *MultiRouteSelector
	transports          []Transport
	routes              []Route
	routeTransportTypes map[Route]TransportType
	// affinityWriteRoutesByTransport retains every route with the requested
	// carrier affinity plus every strictly higher-priority route. Values are
	// sets materialized as slices at publication time, so multiple transports
	// can share affinity without allocating on the write path.
	affinityWriteRoutesByTransport map[TransportType][]Route
	// replyAffineFirstByTransport and replyReliableFirstByTransport are the
	// two orders a reply (an ACK) can use when every affine route of the
	// carrier is potentially unreliable: the affine lanes followed by the
	// reliable lanes, or the reverse. When any affine route is reliable both
	// equal the affinity set, so hybrid H3 keeps its affinity unchanged. The
	// writer picks per reply from channel room and the route ack clock
	// (FLIGHTGATEFIX §13.2).
	replyAffineFirstByTransport   map[TransportType][]Route
	replyReliableFirstByTransport map[TransportType][]Route
	routeCarrierProperties        map[Route]TransferCarrierProperties
	// reliableRoutes is the subset of routes whose carrier is not potentially
	// unreliable, in route order. Reliable-only writes use it when non-empty.
	reliableRoutes               []Route
	generation                   uint64
	unreliableTransferPath       bool
	unreliableFlightByteLimit    ByteCount
	unreliableLossyMaxByteCount  ByteCount
	unreliableFlightMessageLimit int
	unreliableFlowIsolation      bool
	unreliableFlowReserve        bool
	// h1Only is true only when every currently active writer route is an H1
	// carrier. Transfer uses it to recover H1's larger WebSocket message
	// envelope without making an H3 DATAGRAM candidate exceed one tunnel MTU.
	h1Only bool
	// preferDirectRoute is non-nil only when every active route is an H1/H3
	// platform carrier at the same priority and both transport types are live.
	// routes is published with this route first. Writers use only that first
	// route until a new snapshot withdraws it; readers retain every route.
	preferDirectRoute Route
	// weight is the route weight map for `WeightedShuffle`; nil when the
	// selector does not use weighted routes.
	weight map[Route]float32
	// notify is the transport-update channel valid for this snapshot
	// generation. it is closed when the routes change, waking blocked readers
	// so they reload the next snapshot.
	notify chan struct{}

	// writerState uses the value above MaxInt64 as its closed flag and the lower
	// values as an admitted-writer count. A successful hot-path admission and
	// release each require one atomic operation and allocate nothing.
	writerState atomic.Uint64
	// writerDone is closed exactly once: by retireWriter when no writer is
	// active, or by the final releaseWriter after retirement.
	writerDone chan struct{}
	// writerPause is nil in production. Tests install one before admission to
	// make the publication-versus-teardown ordering observable.
	writerPause atomic.Pointer[routeSnapshotWriterPause]
}

func (self *routeSnapshot) observeDirectAffinityWrite(route Route) {
	if self.preferDirectRoute == nil || self.selector.directAffinity == nil {
		return
	}
	transportType := self.transportType(route)
	if route == self.preferDirectRoute {
		switch transportType {
		case TransportTypeH1:
			self.selector.directAffinity.preferredH1WriteCount.Add(1)
		case TransportTypeH3:
			self.selector.directAffinity.preferredH3WriteCount.Add(1)
		}
		return
	}
	switch transportType {
	case TransportTypeH1:
		self.selector.directAffinity.fallbackH1WriteCount.Add(1)
	case TransportTypeH3:
		self.selector.directAffinity.fallbackH3WriteCount.Add(1)
	}
}

func (self *routeSnapshot) observeDirectAffinityBlocked() {
	if self.preferDirectRoute != nil && self.selector.directAffinity != nil {
		self.selector.directAffinity.preferredBlockedCount.Add(1)
	}
}

// One immutable route generation tells a SendSequence whether its current
// carrier needs Transfer-level flight control and which publication wakes a
// sender waiting for that constraint to change.
type transferFlightPolicySnapshot struct {
	generation   uint64
	limited      bool
	byteLimit    ByteCount
	messageLimit int
	// lossyMaxByteCount, when positive, is the largest frame the unreliable
	// lane carries while the flight sits at its loss floor (§13.6).
	lossyMaxByteCount ByteCount
	flowIsolation     bool
	flowReserve       bool
	h1Only            bool
	// reliableRouteAvailable is true while at least one active carrier is not
	// potentially unreliable. A full unreliable flight then writes the
	// overflow reliable-only instead of gating the whole sequence.
	reliableRouteAvailable bool
	notify                 <-chan struct{}
}

// Reads the current carrier policy without taking the selector state lock.
func (self *MultiRouteSelector) transferFlightPolicy() transferFlightPolicySnapshot {
	snapshot := self.activeRoutesSnapshot.Load()
	return transferFlightPolicySnapshot{
		generation:             snapshot.generation,
		limited:                snapshot.unreliableTransferPath,
		byteLimit:              snapshot.unreliableFlightByteLimit,
		lossyMaxByteCount:      snapshot.unreliableLossyMaxByteCount,
		messageLimit:           snapshot.unreliableFlightMessageLimit,
		flowIsolation:          snapshot.unreliableFlowIsolation,
		flowReserve:            snapshot.unreliableFlowReserve,
		h1Only:                 snapshot.h1Only,
		reliableRouteAvailable: 0 < len(snapshot.reliableRoutes),
		notify:                 snapshot.notify,
	}
}

// Writer-state flags reserve the upper bits and leave the lower bits for the
// admitted-writer reference count.
const (
	routeSnapshotWriterPaused        = uint64(math.MaxInt64)/2 + 1
	routeSnapshotWriterClosed        = uint64(math.MaxInt64) + 1
	routeSnapshotWriterReferenceMask = routeSnapshotWriterPaused - 1
)

// A deterministic testing seam holds one real WriteDetailed call after
// admission and before its first old-route send.
type routeSnapshotWriterPause struct {
	acquired     chan struct{}
	waiting      chan struct{}
	resume       chan struct{}
	acquiredOnce sync.Once
	waitingOnce  sync.Once
	resumeOnce   sync.Once
}

// acquireWriter admits one writer unless this snapshot has been retired.
func (self *routeSnapshot) acquireWriter() bool {
	state := self.writerState.Add(1)
	if state < routeSnapshotWriterPaused {
		return true
	}
	if state < routeSnapshotWriterClosed {
		if pause := self.writerPause.Load(); pause != nil {
			pause.acquiredOnce.Do(func() {
				close(pause.acquired)
			})
			<-pause.resume
			self.writerState.And(^routeSnapshotWriterPaused)
		}
		return true
	}
	self.writerState.Add(^uint64(0))
	return false
}

// releaseWriter releases one successful acquireWriter call.
func (self *routeSnapshot) releaseWriter() {
	state := self.writerState.Add(^uint64(0))
	if routeSnapshotWriterClosed <= state && state&routeSnapshotWriterReferenceMask == 0 {
		close(self.writerDone)
	}
}

// retireWriter prevents new admissions and reports whether an admitted writer
// must be joined.
func (self *routeSnapshot) retireWriter() bool {
	previousState := self.writerState.Or(routeSnapshotWriterClosed)
	if previousState&routeSnapshotWriterClosed != 0 {
		panic("route snapshot retired twice")
	}
	if previousState&routeSnapshotWriterReferenceMask == 0 {
		close(self.writerDone)
		return false
	}
	return true
}

// waitWriter joins every writer admitted before retireWriter.
func (self *routeSnapshot) waitWriter() {
	if pause := self.writerPause.Load(); pause != nil {
		pause.waitingOnce.Do(func() {
			close(pause.waiting)
		})
	}
	<-self.writerDone
}

// waitForRouteSnapshotWriters joins retired generations without holding route
// manager or selector state locks.
func waitForRouteSnapshotWriters(snapshots []*routeSnapshot) {
	for _, snapshot := range snapshots {
		snapshot.waitWriter()
	}
}

// shuffled returns the active routes in randomized priority order. for zero or
// one routes it returns the shared immutable slice (no allocation); otherwise
// it returns a freshly shuffled copy so the immutable snapshot is never
// mutated.
func (self *routeSnapshot) shuffled() []Route {
	n := len(self.routes)
	if n <= 1 {
		return self.routes
	}
	routes := make([]Route, n)
	copy(routes, self.routes)
	if self.weight != nil {
		// prioritize the routes (weighted shuffle)
		// if all weights are equal, this is the same as a shuffle
		WeightedShuffle(routes, self.weight)
	} else {
		mathrand.Shuffle(n, func(i int, j int) {
			routes[i], routes[j] = routes[j], routes[i]
		})
	}
	return routes
}

// writeRoutes preserves one selector's direct-carrier affinity while H1 and
// H3 are tied and active together. Reads still shuffle: they consume whichever
// carrier the remote selector chose. Other route combinations retain the
// weighted/random policy unchanged.
func (self *routeSnapshot) writeRoutes() []Route {
	if self.preferDirectRoute != nil {
		return self.routes[:1]
	}
	return self.shuffled()
}

// writeRoutesReliableOnly excludes every potentially unreliable carrier. With
// no reliable carrier active it degrades to the ordinary write set rather than
// refusing the write.
func (self *routeSnapshot) writeRoutesReliableOnly() []Route {
	n := len(self.reliableRoutes)
	if n == 0 {
		return self.writeRoutes()
	}
	if self.preferDirectRoute != nil && !self.routeCarrierProperties[self.preferDirectRoute].Unreliable {
		return []Route{self.preferDirectRoute}
	}
	if n == 1 {
		return self.reliableRoutes
	}
	routes := make([]Route, n)
	copy(routes, self.reliableRoutes)
	if self.weight != nil {
		WeightedShuffle(routes, self.weight)
	} else {
		mathrand.Shuffle(n, func(i int, j int) {
			routes[i], routes[j] = routes[j], routes[i]
		})
	}
	return routes
}

// routeWritePolicy selects which published route order one write uses.
type routeWritePolicy int

const (
	// routeWriteOrdinary is the weighted policy with optional carrier affinity.
	routeWriteOrdinary routeWritePolicy = iota
	// routeWriteReliableOnly is the overflow path of a full unreliable flight.
	routeWriteReliableOnly
	// routeWriteReplyAffineFirst is a reply whose affine unreliable lane is
	// healthy: that lane first, the reliable lanes as the fall-through.
	routeWriteReplyAffineFirst
	// routeWriteReplyReliableFirst is a reply whose affine unreliable lanes
	// are all full or stale: the reliable lanes first, the affine lanes last.
	routeWriteReplyReliableFirst
)

func (self *routeSnapshot) writeRoutesFor(
	preferredTransportType TransportType,
	policy routeWritePolicy,
) []Route {
	switch policy {
	case routeWriteReliableOnly:
		return self.writeRoutesReliableOnly()
	case routeWriteReplyAffineFirst:
		if routes := self.replyAffineFirstByTransport[preferredTransportType]; 0 < len(routes) {
			return routes
		}
	case routeWriteReplyReliableFirst:
		if routes := self.replyReliableFirstByTransport[preferredTransportType]; 0 < len(routes) {
			return routes
		}
	}
	return self.writeRoutesForTransport(preferredTransportType)
}

// writeRoutesForTransport keeps a reply eligible for every active carrier of
// the requested type. Strictly higher-priority carriers are eligible too;
// same- and lower-priority non-affinity carriers are not. The slices are built
// with the immutable route snapshot, so this ACK hot path does not allocate.
func (self *routeSnapshot) writeRoutesForTransport(
	preferredTransportType TransportType,
) []Route {
	if preferredTransportType != TransportTypeUnknown {
		if routes := self.affinityWriteRoutesByTransport[preferredTransportType]; 0 < len(routes) {
			return routes
		}
	}
	return self.writeRoutes()
}

func (self *routeSnapshot) transportType(route Route) TransportType {
	if transportType, ok := self.routeTransportTypes[route]; ok {
		return transportType
	}
	return TransportTypeUnknown
}

func (self *routeSnapshot) receiveDisposition(route Route) transferReceiveDisposition {
	properties := self.routeCarrierProperties[route]
	reliability := properties.ReceiveReliability
	if reliability == CarrierReliabilityUnknown {
		// A carrier that publishes properties but no receive reliability
		// still says whether it can drop. Only a reader with no carrier
		// information stays Unknown, and Unknown counts as unreliable
		// (FLIGHTGATEFIX §19 D4).
		if properties.Unreliable {
			reliability = CarrierReliabilityUnreliable
		} else {
			reliability = CarrierReliabilityReliable
		}
	}
	return transferReceiveDisposition{
		transportType: self.transportType(route),
		reliability:   reliability,
	}
}

func (self *routeSnapshot) writeDisposition(
	route Route,
	transferFrameBytes []byte,
) transferWriteDisposition {
	properties := self.routeCarrierProperties[route]
	unreliable := properties.messageUnreliable(transferFrameBytes)
	return transferWriteDisposition{
		transportType: self.transportType(route),
		unreliable:    unreliable,
		// A successful production-route write that is not explicitly
		// unreliable is owned by that carrier's recovery. Transfer still
		// requires its end-to-end ACK; this flag changes only duplicate-send
		// cadence and enables immediate recovery when the exact route retires.
		reliable: !unreliable,
		// Only the reliable-stream lane of a carrier that also exposes an
		// unreliable message lane uses the long nested-recovery interval.
		// Ordinary H1 keeps its normal Transfer resend interval.
		hybridReliable: properties.Unreliable &&
			properties.unreliableForMessageByteCount != nil && !unreliable,
		route: route,
	}
}

func (self *routeSnapshot) blockedWriteDisposition(
	route Route,
	transferFrameBytes []byte,
	waitDuration time.Duration,
) transferWriteDisposition {
	disposition := self.writeDisposition(route, transferFrameBytes)
	disposition.initiallyBlocked = true
	disposition.initialWaitDuration = waitDuration
	return disposition
}

func (self *MultiRouteSelector) transferRouteActive(route Route) bool {
	if route == nil {
		return false
	}
	snapshot := self.activeRoutesSnapshot.Load()
	_, ok := snapshot.routeCarrierProperties[route]
	return ok
}

// reliableRouteHasCapacity reads the immutable snapshot; a route is a
// buffered channel, so its free capacity is a length read.
func (self *MultiRouteSelector) reliableRouteHasCapacity() bool {
	snapshot := self.activeRoutesSnapshot.Load()
	if snapshot == nil {
		return false
	}
	for _, route := range snapshot.routes {
		if snapshot.routeCarrierProperties[route].Unreliable {
			continue
		}
		if len(route) < cap(route) {
			return true
		}
	}
	return false
}

// observeRouteAckProgress stamps the newest acknowledgement seen for a route.
// The clock survives snapshot rebuilds and is dropped with the route.
func (self *MultiRouteSelector) observeRouteAckProgress(route Route) {
	now := time.Now().UnixNano()
	if clock, ok := self.routeAckProgress.Load(route); ok {
		clock.(*atomic.Int64).Store(now)
		return
	}
	clock := &atomic.Int64{}
	clock.Store(now)
	self.routeAckProgress.Store(route, clock)
}

// RouteAckProgressAge is the time since the newest acknowledgement of an
// item written on the route; ok is false when none was ever observed.
func (self *MultiRouteSelector) RouteAckProgressAge(route Route) (time.Duration, bool) {
	clock, ok := self.routeAckProgress.Load(route)
	if !ok {
		return 0, false
	}
	return time.Since(time.Unix(0, clock.(*atomic.Int64).Load())), true
}

func (self *MultiRouteSelector) canPreferH3AfterH1Timeout(route Route) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	snapshot := self.activeRoutesSnapshot.Load()
	if snapshot == nil || snapshot.preferDirectRoute != route ||
		snapshot.transportType(route) != TransportTypeH1 {
		return false
	}
	for _, candidate := range snapshot.routes {
		if candidate != route && snapshot.transportType(candidate) == TransportTypeH3 {
			return true
		}
	}
	return false
}

// preferH3IfTied changes no transport health or configured priority. It only
// changes sticky writer affinity after the shared Client observed an H1
// end-to-end timeout. Queue backpressure and sibling publication remain
// insufficient evidence.
func (self *MultiRouteSelector) preferH3IfTied() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	snapshot := self.activeRoutesSnapshot.Load()
	if snapshot == nil || snapshot.preferDirectRoute == nil ||
		snapshot.transportType(snapshot.preferDirectRoute) != TransportTypeH1 {
		return false
	}
	for _, candidate := range snapshot.routes {
		if snapshot.transportType(candidate) != TransportTypeH3 {
			continue
		}
		self.preferredDirectRoute = candidate
		if self.directAffinity != nil {
			self.directAffinity.routeChangeCount.Add(1)
		}
		self.transportUpdate.NotifyAll()
		self.updateActiveRoutesWithLock()
		return true
	}
	return false
}

func (self *MultiRouteSelector) transferPreferH3AfterH1Timeout(route Route) bool {
	if !self.canPreferH3AfterH1Timeout(route) {
		return false
	}
	self.directAffinity.h3PreferredAfterH1Timeout.Store(true)
	self.directAffinity.h1TimeoutFailoverCount.Add(1)
	changed := self.preferH3IfTied()
	if changed {
		waitForRouteSnapshotWriters(self.takeRetiredWriterSnapshots())
	}
	return changed
}

func NewMultiRouteSelector(ctx context.Context, clientTag string, log Logger, destination TransferPath, weightedRoutes bool) *MultiRouteSelector {
	cancelCtx, cancel := context.WithCancel(ctx)
	directAffinity := &directCarrierAffinityCounters{}
	multiRouteSelector := &MultiRouteSelector{
		ctx:                 cancelCtx,
		cancel:              cancel,
		clientTag:           clientTag,
		log:                 loggerOrDefault(log),
		destination:         destination,
		weightedRoutes:      weightedRoutes,
		transportUpdate:     NewMonitor(),
		transportRoutes:     map[Transport][]Route{},
		transportProperties: map[Transport]TransferCarrierProperties{},
		routeStats:          map[Route]*RouteStats{},
		routeActive:         map[Route]bool{},
		routeWeight:         map[Route]float32{},
		directAffinity:      directAffinity,
	}
	// publish the initial (empty) snapshot so the hot path always has a non-nil
	// snapshot to read
	multiRouteSelector.updateActiveRoutesWithLock()
	return multiRouteSelector
}

// rebuilds the immutable active-routes snapshot from the current route state
// and publishes it for the lock-free hot path. must be called with `mutex`
// (it reads the route maps and captures the current transport-update channel).
func (self *MultiRouteSelector) updateActiveRoutesWithLock() {
	previousSnapshot := self.activeRoutesSnapshot.Load()
	activeRoutes := []Route{}
	activeTransports := []Transport{}
	routeTransportTypes := map[Route]TransportType{}
	activeRoutesByTransport := map[TransportType][]Route{}
	routePriorities := map[Route]int{}
	routeCarrierProperties := map[Route]TransferCarrierProperties{}
	reliableRoutes := []Route{}
	unreliableTransferPath := false
	unreliableFlightByteLimit := ByteCount(0)
	unreliableLossyMaxByteCount := ByteCount(0)
	unreliableFlightMessageLimit := 0
	unreliableFlowIsolation := true
	unreliableFlowReserve := true
	inactiveRouteCount := 0
	directPriority := 0
	directPrioritySet := false
	directPriorityEqual := true
	hasH1 := false
	hasH3 := false
	for transport, routes := range self.transportRoutes {
		transportActive := false
		for _, route := range routes {
			if self.routeActive[route] {
				activeRoutes = append(activeRoutes, route)
				transportType := transportTypeOf(transport)
				routeTransportTypes[route] = transportType
				activeRoutesByTransport[transportType] = append(
					activeRoutesByTransport[transportType],
					route,
				)
				routePriorities[route] = transport.Priority()
				routeCarrierProperties[route] = self.transportProperties[transport]
				if !self.transportProperties[transport].Unreliable {
					reliableRoutes = append(reliableRoutes, route)
				}
				switch transportType {
				case TransportTypeH1:
					hasH1 = true
				case TransportTypeH3:
					hasH3 = true
				}
				if transportType == TransportTypeH1 || transportType == TransportTypeH3 {
					if !directPrioritySet {
						directPriority = transport.Priority()
						directPrioritySet = true
					} else if transport.Priority() != directPriority {
						directPriorityEqual = false
					}
				}
				transportActive = true
			} else {
				inactiveRouteCount += 1
			}
		}
		if transportActive {
			activeTransports = append(activeTransports, transport)
			properties := self.transportProperties[transport]
			if properties.Unreliable {
				unreliableTransferPath = true
				unreliableFlowIsolation = unreliableFlowIsolation &&
					properties.UnreliableFlowIsolation
				unreliableFlowReserve = unreliableFlowReserve &&
					properties.UnreliableFlowReserve
				if limit := properties.unreliableFlightByteLimit; 0 < limit &&
					(unreliableFlightByteLimit == 0 || limit < unreliableFlightByteLimit) {
					unreliableFlightByteLimit = limit
				}
				if limit := properties.unreliableLossyMaxMessageByteCount; 0 < limit &&
					(unreliableLossyMaxByteCount == 0 || limit < unreliableLossyMaxByteCount) {
					unreliableLossyMaxByteCount = limit
				}
				if limit := properties.unreliableFlightMessageLimit; 0 < limit &&
					(unreliableFlightMessageLimit == 0 || limit < unreliableFlightMessageLimit) {
					unreliableFlightMessageLimit = limit
				}
			}
		}
	}

	allDirectRoutes := 0 < len(activeRoutes)
	h1Only := 0 < len(activeRoutes)
	for _, route := range activeRoutes {
		transportType := routeTransportTypes[route]
		if transportType != TransportTypeH1 {
			h1Only = false
		}
		if transportType != TransportTypeH1 && transportType != TransportTypeH3 {
			allDirectRoutes = false
			break
		}
	}
	previousPreferredDirectRoute := self.preferredDirectRoute
	if !slices.Contains(activeRoutes, self.preferredDirectRoute) {
		self.preferredDirectRoute = nil
	}
	if self.preferredDirectRoute == nil {
		for _, route := range activeRoutes {
			transportType := routeTransportTypes[route]
			if transportType == TransportTypeH1 || transportType == TransportTypeH3 {
				self.preferredDirectRoute = route
				break
			}
		}
	}
	if previousPreferredDirectRoute != nil &&
		self.preferredDirectRoute != nil &&
		previousPreferredDirectRoute != self.preferredDirectRoute &&
		self.directAffinity != nil {
		self.directAffinity.routeChangeCount.Add(1)
	}

	preferDirectRoute := Route(nil)
	if allDirectRoutes && hasH1 && hasH3 && directPriorityEqual {
		preferDirectRoute = self.preferredDirectRoute
		// Publish the preferred route first once per rare route generation so
		// the packet hot path can reuse the immutable slice without allocating.
		if preferredIndex := slices.Index(activeRoutes, preferDirectRoute); 0 < preferredIndex {
			activeRoutes[0], activeRoutes[preferredIndex] = activeRoutes[preferredIndex], activeRoutes[0]
		}
	}
	if preferDirectRoute != nil &&
		(previousSnapshot == nil || previousSnapshot.preferDirectRoute == nil) &&
		self.directAffinity != nil {
		self.directAffinity.activationCount.Add(1)
	}

	// A carrier affinity is a set, not a pin to one route. Preserve every
	// matching transport, then add routes that outrank at least one affinity
	// member. Same- and lower-priority non-affinity routes remain filtered.
	affinityWriteRoutesByTransport := map[TransportType][]Route{}
	for transportType, affinityRoutes := range activeRoutesByTransport {
		if len(affinityRoutes) == 0 {
			continue
		}
		lowestAffinityPriority := routePriorities[affinityRoutes[0]]
		included := make(map[Route]bool, len(activeRoutes))
		for _, route := range affinityRoutes {
			included[route] = true
			lowestAffinityPriority = max(lowestAffinityPriority, routePriorities[route])
		}
		higherPriorityRoutes := make([]Route, 0, len(activeRoutes))
		for _, route := range activeRoutes {
			if !included[route] && routePriorities[route] < lowestAffinityPriority {
				higherPriorityRoutes = append(higherPriorityRoutes, route)
				included[route] = true
			}
		}
		slices.SortStableFunc(higherPriorityRoutes, func(a Route, b Route) int {
			if routePriorities[a] < routePriorities[b] {
				return -1
			}
			if routePriorities[b] < routePriorities[a] {
				return 1
			}
			return 0
		})
		eligible := make([]Route, 0, len(higherPriorityRoutes)+len(affinityRoutes))
		eligible = append(eligible, higherPriorityRoutes...)
		eligible = append(eligible, affinityRoutes...)
		affinityWriteRoutesByTransport[transportType] = eligible
	}
	replyAffineFirstByTransport := map[TransportType][]Route{}
	replyReliableFirstByTransport := map[TransportType][]Route{}
	for transportType, eligible := range affinityWriteRoutesByTransport {
		allUnreliable := true
		for _, route := range eligible {
			if !routeCarrierProperties[route].Unreliable {
				allUnreliable = false
				break
			}
		}
		if !allUnreliable {
			replyAffineFirstByTransport[transportType] = eligible
			replyReliableFirstByTransport[transportType] = eligible
			continue
		}
		included := make(map[Route]bool, len(eligible))
		for _, route := range eligible {
			included[route] = true
		}
		reliable := make([]Route, 0, len(activeRoutes))
		for _, route := range activeRoutes {
			if !included[route] && !routeCarrierProperties[route].Unreliable {
				reliable = append(reliable, route)
			}
		}
		if len(reliable) == 0 {
			replyAffineFirstByTransport[transportType] = eligible
			replyReliableFirstByTransport[transportType] = eligible
			continue
		}
		affineFirst := make([]Route, 0, len(eligible)+len(reliable))
		affineFirst = append(affineFirst, eligible...)
		affineFirst = append(affineFirst, reliable...)
		reliableFirst := make([]Route, 0, len(eligible)+len(reliable))
		reliableFirst = append(reliableFirst, reliable...)
		reliableFirst = append(reliableFirst, eligible...)
		replyAffineFirstByTransport[transportType] = affineFirst
		replyReliableFirstByTransport[transportType] = reliableFirst
	}

	var weight map[Route]float32
	if self.weightedRoutes {
		// copy so the published map is immutable
		weight = make(map[Route]float32, len(self.routeWeight))
		for route, w := range self.routeWeight {
			weight[route] = w
		}
	}

	self.nextRouteGeneration += 1
	retiredSnapshot := self.activeRoutesSnapshot.Swap(&routeSnapshot{
		selector:                       self,
		transports:                     activeTransports,
		routes:                         activeRoutes,
		routeTransportTypes:            routeTransportTypes,
		affinityWriteRoutesByTransport: affinityWriteRoutesByTransport,
		replyAffineFirstByTransport:    replyAffineFirstByTransport,
		replyReliableFirstByTransport:  replyReliableFirstByTransport,
		routeCarrierProperties:         routeCarrierProperties,
		reliableRoutes:                 reliableRoutes,
		generation:                     self.nextRouteGeneration,
		unreliableTransferPath:         unreliableTransferPath,
		unreliableFlightByteLimit:      unreliableFlightByteLimit,
		unreliableLossyMaxByteCount:    unreliableLossyMaxByteCount,
		unreliableFlightMessageLimit:   unreliableFlightMessageLimit,
		unreliableFlowIsolation:        unreliableTransferPath && unreliableFlowIsolation,
		unreliableFlowReserve:          unreliableTransferPath && unreliableFlowReserve,
		h1Only:                         h1Only,
		preferDirectRoute:              preferDirectRoute,
		weight:                         weight,
		notify:                         self.transportUpdate.NotifyChannel(),
		writerDone:                     make(chan struct{}),
	})
	if retiredSnapshot != nil && retiredSnapshot.retireWriter() {
		self.retiredWriterSnapshots = append(
			self.retiredWriterSnapshots,
			retiredSnapshot,
		)
	}
	for observer := range self.testingRouteStateObservers {
		observer.publish(len(activeRoutes), inactiveRouteCount)
	}
}

// acquireWriterSnapshot loads and admits the current immutable route
// generation. A concurrent update closes the old generation and forces a
// retry before any old route can be used.
func (self *MultiRouteSelector) acquireWriterSnapshot() *routeSnapshot {
	for {
		snapshot := self.activeRoutesSnapshot.Load()
		if snapshot.acquireWriter() {
			return snapshot
		}
	}
}

// Pauses the next real WriteDetailed call after atomic admission. The second
// channel closes when teardown reaches its
// writer join, and the function resumes the write. When unused, this seam adds
// no branch, allocation, or non-atomic synchronization to production writes.
func TestingPauseMultiRouteWriterSnapshot(
	writer MultiRouteWriter,
) (<-chan struct{}, <-chan struct{}, func()) {
	selector, ok := writer.(*MultiRouteSelector)
	if !ok {
		panic("unsupported multi-route writer")
	}
	snapshot := selector.activeRoutesSnapshot.Load()
	pause := &routeSnapshotWriterPause{
		acquired: make(chan struct{}),
		waiting:  make(chan struct{}),
		resume:   make(chan struct{}),
	}
	if !snapshot.writerPause.CompareAndSwap(nil, pause) {
		panic("writer snapshot pause already installed")
	}
	previousState := snapshot.writerState.Or(routeSnapshotWriterPaused)
	if routeSnapshotWriterPaused <= previousState {
		panic("writer snapshot is not available for pause")
	}
	resume := func() {
		pause.resumeOnce.Do(func() {
			close(pause.resume)
		})
	}
	return pause.acquired, pause.waiting, resume
}

// TestingObserveMultiRouteWriterRouteState installs an exact generation stream
// at the selector publication seam. Production selectors keep the observer map
// nil, so ordinary route publication performs no observer allocation or call.
func TestingObserveMultiRouteWriterRouteState(
	writer MultiRouteWriter,
) *TestingMultiRouteWriterRouteStateObserver {
	selector, ok := writer.(*MultiRouteSelector)
	if !ok {
		panic("unsupported multi-route writer")
	}

	selector.mutex.Lock()
	activeRouteCount := 0
	inactiveRouteCount := 0
	for _, active := range selector.routeActive {
		if active {
			activeRouteCount += 1
		} else {
			inactiveRouteCount += 1
		}
	}
	head := &testingMultiRouteWriterRouteStateNode{
		state: TestingMultiRouteWriterRouteState{
			Generation:         0,
			ActiveRouteCount:   activeRouteCount,
			InactiveRouteCount: inactiveRouteCount,
		},
		changed: make(chan struct{}),
	}
	observer := &TestingMultiRouteWriterRouteStateObserver{
		selector: selector,
		head:     head,
	}
	observer.tail.Store(head)
	if selector.testingRouteStateObservers == nil {
		selector.testingRouteStateObservers = map[*TestingMultiRouteWriterRouteStateObserver]bool{}
	}
	selector.testingRouteStateObservers[observer] = true
	selector.mutex.Unlock()
	return observer
}

// publish appends one immutable generation and wakes waiters after its link is
// visible. The selector serializes publishers; atomics keep this nonblocking.
func (self *TestingMultiRouteWriterRouteStateObserver) publish(
	activeRouteCount int,
	inactiveRouteCount int,
) {
	next := &testingMultiRouteWriterRouteStateNode{
		state: TestingMultiRouteWriterRouteState{
			Generation:         self.nextGeneration.Add(1),
			ActiveRouteCount:   activeRouteCount,
			InactiveRouteCount: inactiveRouteCount,
		},
		changed: make(chan struct{}),
	}
	previous := self.tail.Swap(next)
	previous.next.Store(next)
	close(previous.changed)
}

// Snapshot returns the latest immutable generation without consuming events.
func (self *TestingMultiRouteWriterRouteStateObserver) Snapshot() TestingMultiRouteWriterRouteState {
	return self.tail.Load().state
}

// WaitAfter returns the first generation newer than the supplied barrier.
// Every intervening generation remains linked, so fast transitions cannot be
// coalesced into the final state. Context cancellation bounds only liveness.
func (self *TestingMultiRouteWriterRouteStateObserver) WaitAfter(
	ctx context.Context,
	generation uint64,
) (TestingMultiRouteWriterRouteState, error) {
	node := self.head
	for {
		if generation < node.state.Generation {
			return node.state, nil
		}
		if next := node.next.Load(); next != nil {
			node = next
			continue
		}
		if self.closed.Load() {
			return TestingMultiRouteWriterRouteState{}, errors.New("route-state observer closed")
		}
		select {
		case <-ctx.Done():
			return TestingMultiRouteWriterRouteState{}, ctx.Err()
		case <-node.changed:
		}
	}
}

// WaitForActiveRouteCountAfter returns the first newer generation with the
// requested active count. The barrier excludes an identical stale state.
func (self *TestingMultiRouteWriterRouteStateObserver) WaitForActiveRouteCountAfter(
	ctx context.Context,
	generation uint64,
	activeRouteCount int,
) (TestingMultiRouteWriterRouteState, error) {
	for {
		state, err := self.WaitAfter(ctx, generation)
		if err != nil {
			return TestingMultiRouteWriterRouteState{}, err
		}
		if state.ActiveRouteCount == activeRouteCount {
			return state, nil
		}
		generation = state.Generation
	}
}

// Close removes the test publisher before waking any remaining waiter.
func (self *TestingMultiRouteWriterRouteStateObserver) Close() {
	self.closeOnce.Do(func() {
		self.selector.mutex.Lock()
		delete(self.selector.testingRouteStateObservers, self)
		if len(self.selector.testingRouteStateObservers) == 0 {
			self.selector.testingRouteStateObservers = nil
		}
		self.closed.Store(true)
		tail := self.tail.Load()
		self.selector.mutex.Unlock()
		close(tail.changed)
	})
}

// Transfers the selector's pending join ownership
// to RouteManager.
func (self *MultiRouteSelector) takeRetiredWriterSnapshots() []*routeSnapshot {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	retiredSnapshots := self.retiredWriterSnapshots
	self.retiredWriterSnapshots = nil
	return retiredSnapshots
}

func (self *MultiRouteSelector) getTransportStats(transport Transport) *RouteStats {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	currentRoutes, ok := self.transportRoutes[transport]
	if !ok {
		return nil
	}
	netStats := NewRouteStats()
	for _, currentRoute := range currentRoutes {
		if stats, ok := self.routeStats[currentRoute]; ok {
			netStats.sendCount += stats.sendCount
			netStats.sendByteCount += stats.sendByteCount
			netStats.receiveCount += stats.receiveCount
			netStats.receiveByteCount += stats.receiveByteCount
		}
	}
	return netStats
}

// Reports membership without exposing a selector's mutable route map.
func (self *MultiRouteSelector) hasTransport(transport Transport) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	_, ok := self.transportRoutes[transport]
	return ok
}

// if weightedRoutes, this applies new priorities and weights. calling this resets all route stats.
// the reason to reset weightedRoutes is that the weight calculation needs to consider only the stats since the previous weight change
func (self *MultiRouteSelector) updateTransport(transport Transport, routes []Route) {
	self.updateTransportWithProperties(
		transport,
		routes,
		TransferCarrierProperties{},
	)
}

// Updates one selector's routes and immutable carrier delivery semantics.
func (self *MultiRouteSelector) updateTransportWithProperties(
	transport Transport,
	routes []Route,
	properties TransferCarrierProperties,
) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// activeRoutes := func()([]Route) {
	//  activeRoutes := []Route{}
	//  for _, routes := range self.transportRoutes {
	//      for _, route := range routes {
	//          if self.routeActive[route] {
	//              activeRoutes = append(activeRoutes, route)
	//          }
	//      }
	//  }
	//  return activeRoutes
	// }

	// preTransportCount := len(self.transportRoutes)
	// preActiveRouteCount := len(activeRoutes())

	if len(routes) == 0 {
		if currentRoutes, ok := self.transportRoutes[transport]; ok {
			for _, currentRoute := range currentRoutes {
				delete(self.routeStats, currentRoute)
				delete(self.routeActive, currentRoute)
				delete(self.routeWeight, currentRoute)
				self.routeAckProgress.Delete(currentRoute)
			}
			delete(self.transportRoutes, transport)
			delete(self.transportProperties, transport)
		} else {
			// transport is not active. nothing to do
			return
		}
	} else {
		if currentRoutes, ok := self.transportRoutes[transport]; ok {
			for _, currentRoute := range currentRoutes {
				if slices.Index(routes, currentRoute) < 0 {
					// no longer present
					delete(self.routeStats, currentRoute)
					delete(self.routeActive, currentRoute)
					delete(self.routeWeight, currentRoute)
				}
			}
			for _, route := range routes {
				if slices.Index(currentRoutes, route) < 0 {
					// new route
					self.routeActive[route] = true
				}
			}
		} else {
			for _, route := range routes {
				// new route
				self.routeActive[route] = true
			}
		}
		// the following will be updated with the new routes in the weighting below
		// - routeStats
		// - routeActive
		// - routeWeights
		self.transportRoutes[transport] = routes
		self.transportProperties[transport] = properties
		for _, route := range routes {
			if self.routeStats[route] == nil {
				self.routeStats[route] = NewRouteStats()
			}
		}
	}

	if self.weightedRoutes {
		self.updateRouteWeights()
	}

	// notify first so the rebuilt snapshot captures the new transport-update
	// channel; readers on the old snapshot wake on the old (now closed) channel
	// and reload
	self.transportUpdate.NotifyAll()
	self.updateActiveRoutesWithLock()
}

func (self *MultiRouteSelector) updateRouteWeights() {
	updatedRouteWeight := map[Route]float32{}

	transportStats := map[Transport]*RouteStats{}
	for transport, currentRoutes := range self.transportRoutes {
		netStats := NewRouteStats()
		for _, currentRoute := range currentRoutes {
			if stats, ok := self.routeStats[currentRoute]; ok {
				netStats.sendCount += stats.sendCount
				netStats.sendByteCount += stats.sendByteCount
				netStats.receiveCount += stats.receiveCount
				netStats.receiveByteCount += stats.receiveByteCount
			}
		}
		transportStats[transport] = netStats
	}

	orderedTransports := slices.Collect(maps.Keys(self.transportRoutes))
	// shuffle the same priority values
	mathrand.Shuffle(len(orderedTransports), func(i int, j int) {
		t := orderedTransports[i]
		orderedTransports[i] = orderedTransports[j]
		orderedTransports[j] = t
	})
	slices.SortStableFunc(orderedTransports, func(a Transport, b Transport) int {
		return a.Priority() - b.Priority()
	})

	n := len(orderedTransports)

	allCanEval := true
	for i := 0; i < n; i += 1 {
		transport := orderedTransports[i]
		routeStats := transportStats[transport]
		remainingStats := map[Transport]*RouteStats{}
		for j := i + 1; j < n; j += 1 {
			remainingStats[orderedTransports[j]] = transportStats[orderedTransports[j]]
		}
		canEval := transport.CanEvalRouteWeight(routeStats, remainingStats)
		allCanEval = allCanEval && canEval
	}

	if allCanEval {
		var allWeight float32
		allWeight = 1.0
		for i := 0; i < n; i += 1 {
			transport := orderedTransports[i]
			routeStats := transportStats[transport]
			remainingStats := map[Transport]*RouteStats{}
			for j := i + 1; j < n; j += 1 {
				remainingStats[orderedTransports[j]] = transportStats[orderedTransports[j]]
			}
			weight := transport.RouteWeight(routeStats, remainingStats)
			for _, route := range self.transportRoutes[transport] {
				updatedRouteWeight[route] = allWeight * weight
			}
			allWeight *= (1.0 - weight)
		}

		self.routeWeight = updatedRouteWeight

		updatedRouteStats := map[Route]*RouteStats{}
		for _, currentRoutes := range self.transportRoutes {
			for _, currentRoute := range currentRoutes {
				// reset the stats
				updatedRouteStats[currentRoute] = NewRouteStats()
			}
		}
		self.routeStats = updatedRouteStats
	}
}

func (self *MultiRouteSelector) GetActiveRoutes() []Route {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	activeRoutes := []Route{}
	for _, routes := range self.transportRoutes {
		for _, route := range routes {
			if self.routeActive[route] {
				activeRoutes = append(activeRoutes, route)
			}
		}
	}

	if self.weightedRoutes {
		// prioritize the routes (weighted shuffle)
		// if all weights are equal, this is the same as a shuffle
		WeightedShuffle(activeRoutes, self.routeWeight)
	} else {
		mathrand.Shuffle(len(activeRoutes), func(i int, j int) {
			activeRoutes[i], activeRoutes[j] = activeRoutes[j], activeRoutes[i]
		})
	}

	return activeRoutes
}

func (self *MultiRouteSelector) GetInactiveRoutes() []Route {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	inactiveRoutes := []Route{}
	for _, routes := range self.transportRoutes {
		for _, route := range routes {
			if !self.routeActive[route] {
				inactiveRoutes = append(inactiveRoutes, route)
			}
		}
	}

	return inactiveRoutes
}

func (self *MultiRouteSelector) setActive(route Route, active bool) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if current, ok := self.routeActive[route]; ok && current != active {
		self.routeActive[route] = active
		// the active set changed, so republish the snapshot. the hot path that
		// deactivated a closed route reloads on its next loop iteration.
		self.updateActiveRoutesWithLock()
	}
}

func (self *MultiRouteSelector) updateSendStats(route Route, sendCount int, sendByteCount ByteCount) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	stats, ok := self.routeStats[route]
	if !ok {
		// A destructive publication already retired this route. The admitted
		// writer may finish its old-route send, but must not recreate state that
		// physical teardown just removed.
		return
	}
	stats.sendCount += sendCount
	stats.sendByteCount += sendByteCount
}

func (self *MultiRouteSelector) updateReceiveStats(route Route, receiveCount int, receiveByteCount ByteCount) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	stats, ok := self.routeStats[route]
	if !ok {
		// A reader can finish an old snapshot after route withdrawal. Its late
		// accounting must not recreate retired route state.
		return
	}
	stats.receiveCount += receiveCount
	stats.receiveByteCount += receiveByteCount
}

func (self *MultiRouteSelector) Write(ctx context.Context, transferFrameBytes []byte, timeout time.Duration) error {
	success, err := self.WriteDetailed(ctx, transferFrameBytes, timeout)
	if err != nil {
		return err
	}
	if !success {
		return errTransferRouteWriteTimeout
	}
	return nil
}

// MultiRouteWriter
func (self *MultiRouteSelector) WriteDetailed(ctx context.Context, transferFrameBytes []byte, timeout time.Duration) (bool, error) {
	success, _, err := self.WriteDetailedWithTransport(ctx, transferFrameBytes, timeout)
	return success, err
}

// WriteDetailedWithTransport reports the immutable transport tag published
// with the exact route whose channel accepted the frame.
func (self *MultiRouteSelector) WriteDetailedWithTransport(ctx context.Context, transferFrameBytes []byte, timeout time.Duration) (bool, TransportType, error) {
	success, disposition, err := self.writeDetailedWithCarrier(
		ctx,
		transferFrameBytes,
		timeout,
	)
	return success, disposition.transportType, err
}

// writeDetailedWithCarrier additionally evaluates the selected route's
// immutable carrier policy against this exact message. A hybrid H3 route can
// therefore report its reliable stream lane without changing the public
// optional TransportMultiRouteWriter interface.
func (self *MultiRouteSelector) writeDetailedWithCarrier(
	ctx context.Context,
	transferFrameBytes []byte,
	timeout time.Duration,
) (bool, transferWriteDisposition, error) {
	return self.writeDetailedWithCarrierPreference(
		ctx,
		transferFrameBytes,
		timeout,
		TransportTypeUnknown,
	)
}

// tryWriteH1AckPriorityWithCarrierPreference bypasses a full ordinary H1
// carrier route when that transport opted into its bounded ACK companion.
// It never waits: a full/missing priority lane falls through to the normal
// writer, preserving its timeout, route withdrawal, and fallback behavior.
// Accounting stays attached to the public route because the companion is one
// writer lane of that exact carrier, not a separately selectable transport.
func (self *MultiRouteSelector) tryWriteH1AckPriorityWithCarrierPreference(
	transferFrameBytes []byte,
	preferredTransportType TransportType,
) (bool, transferWriteDisposition) {
	if h1AckPriorityRouteCount.Load() == 0 {
		return false, transferWriteDisposition{}
	}

	snapshot := self.acquireWriterSnapshot()
	for _, route := range snapshot.writeRoutesForTransport(preferredTransportType) {
		priorityRoute, ok := h1AckPriorityRoute(route)
		if !ok {
			continue
		}
		select {
		case priorityRoute <- transferFrameBytes:
			self.updateSendStats(route, 1, ByteCount(len(transferFrameBytes)))
			snapshot.observeDirectAffinityWrite(route)
			disposition := snapshot.writeDisposition(route, transferFrameBytes)
			snapshot.releaseWriter()
			return true, disposition
		default:
		}
	}
	snapshot.releaseWriter()
	return false, transferWriteDisposition{}
}

// writeDetailedWithCarrierPreference applies the observed inbound carrier as
// an affinity set. Every matching route and every strictly higher-priority
// route remains eligible; equal- or lower-priority non-affinity routes do not
// become queue-pressure fallbacks.
func (self *MultiRouteSelector) writeDetailedWithCarrierPreference(
	ctx context.Context,
	transferFrameBytes []byte,
	timeout time.Duration,
	preferredTransportType TransportType,
) (bool, transferWriteDisposition, error) {
	return self.writeDetailedWithRoutePolicy(
		ctx,
		transferFrameBytes,
		timeout,
		preferredTransportType,
		routeWriteOrdinary,
	)
}

func (self *MultiRouteSelector) writeDetailedReliableOnly(
	ctx context.Context,
	transferFrameBytes []byte,
	timeout time.Duration,
) (bool, transferWriteDisposition, error) {
	return self.writeDetailedWithRoutePolicy(
		ctx,
		transferFrameBytes,
		timeout,
		TransportTypeUnknown,
		routeWriteReliableOnly,
	)
}

// writeDetailedReplyWithCarrierPreference writes a reply (an ACK) with the
// carrier affinity of the Pack it answers. When every affine route is
// potentially unreliable (native p2p at the top priority), the reply keeps
// that lane first only while at least one affine route has channel room and
// its ack clock is not older than staleAfter; otherwise the reliable lanes
// come first and the affine lanes last. A cumulative ACK lost on a full or
// dead datagram lane times out the sender's whole window; on the relay it
// costs one hop of latency. Hybrid carriers whose affinity set includes a
// reliable lane are unchanged. Zero staleAfter disables the clock rule.
func (self *MultiRouteSelector) writeDetailedReplyWithCarrierPreference(
	ctx context.Context,
	transferFrameBytes []byte,
	timeout time.Duration,
	preferredTransportType TransportType,
	staleAfter time.Duration,
	laneLosing bool,
) (bool, transferWriteDisposition, error) {
	policy := routeWriteReplyAffineFirst
	if laneLosing {
		// The lane this Pack arrived on is dropping, so its acknowledgements
		// are dropping too and every lost cumulative ack costs the sender a
		// window. Reply on the reliable carrier until the lane recovers
		// (FLIGHTGATEFIX §18). A carrier whose own lane shows no loss keeps
		// its affinity, so hybrid H3 is unaffected by another lane's trouble.
		policy = routeWriteReplyReliableFirst
	} else if snapshot := self.activeRoutesSnapshot.Load(); snapshot != nil {
		affine := snapshot.affinityWriteRoutesByTransport[preferredTransportType]
		if 0 < len(affine) && len(affine) < len(snapshot.replyAffineFirstByTransport[preferredTransportType]) {
			// every affine route is unreliable and a reliable lane exists
			healthy := false
			for _, route := range affine {
				if cap(route) <= len(route) {
					continue
				}
				if 0 < staleAfter {
					if age, ok := self.RouteAckProgressAge(route); ok && staleAfter <= age {
						continue
					}
				}
				healthy = true
				break
			}
			if !healthy {
				policy = routeWriteReplyReliableFirst
			}
		}
	}
	return self.writeDetailedWithRoutePolicy(
		ctx,
		transferFrameBytes,
		timeout,
		preferredTransportType,
		policy,
	)
}

func (self *MultiRouteSelector) writeDetailedWithRoutePolicy(
	ctx context.Context,
	transferFrameBytes []byte,
	timeout time.Duration,
	preferredTransportType TransportType,
	policy routeWritePolicy,
) (bool, transferWriteDisposition, error) {
	enterTime := time.Now()
	preferredBlockedObserved := false

	// Preserve the allocation-free, lock-free common path. SendSequence owns a
	// writer selector and writes its ordered stream serially; the mutex below is
	// only needed when a write must retain and reuse the selector timer.
	initialSnapshot := self.acquireWriterSnapshot()
	initialRoutes := initialSnapshot.writeRoutesFor(preferredTransportType, policy)
	if self.log.V(2).Enabled() {
		self.log.Infof("[mrw] %s->%s s(%s) routes = %d\n", self.clientTag, self.destination.DestinationId, self.destination.StreamId, len(initialRoutes))
	}
	for _, route := range initialRoutes {
		select {
		case route <- transferFrameBytes:
			self.updateSendStats(route, 1, ByteCount(len(transferFrameBytes)))
			initialSnapshot.observeDirectAffinityWrite(route)
			initialSnapshot.releaseWriter()
			if self.log.V(2).Enabled() {
				self.log.Infof("[mrw]nb %s->%s s(%s)\n", self.clientTag, self.destination.DestinationId, self.destination.StreamId)
			}
			return true, initialSnapshot.writeDisposition(route, transferFrameBytes), nil
		default:
			if route == initialSnapshot.preferDirectRoute && !preferredBlockedObserved {
				initialSnapshot.observeDirectAffinityBlocked()
				preferredBlockedObserved = true
			}
		}
	}
	initialSnapshot.releaseWriter()

	self.writeMutex.Lock()
	defer self.writeMutex.Unlock()
	defer func() {
		if self.writeTimer != nil {
			self.writeTimer.Stop()
		}
	}()

	// Write to the first channel available in the published route policy.
	// Arm the selector's timer only if the nonblocking route pass fails, then
	// retain the same absolute deadline across transport-update retries. The
	// timer object itself is lazy and bounded to one per writer selector.
	timeoutChannel := func() (<-chan time.Time, bool) {
		if timeout < 0 {
			return nil, false
		}
		remainingTimeout := enterTime.Add(timeout).Sub(time.Now())
		if remainingTimeout <= 0 {
			return nil, true
		}
		return resetOrCreateTimer(&self.writeTimer, remainingTimeout), false
	}
	for {
		// read the active routes and the transport-update channel from the
		// lock-free snapshot instead of taking the selector and monitor locks
		// on every packet
		snapshot := self.acquireWriterSnapshot()
		notify := snapshot.notify
		activeRoutes := snapshot.writeRoutesFor(preferredTransportType, policy)

		if self.log.V(2).Enabled() {
			self.log.Infof("[mrw] %s->%s s(%s) routes = %d\n", self.clientTag, self.destination.DestinationId, self.destination.StreamId, len(activeRoutes))
		}

		// non-blocking priority
		for _, route := range activeRoutes {
			select {
			case route <- transferFrameBytes:
				self.updateSendStats(route, 1, ByteCount(len(transferFrameBytes)))
				snapshot.observeDirectAffinityWrite(route)
				snapshot.releaseWriter()
				if self.log.V(2).Enabled() {
					self.log.Infof("[mrw]nb %s->%s s(%s)\n", self.clientTag, self.destination.DestinationId, self.destination.StreamId)
				}
				return true, snapshot.blockedWriteDisposition(
					route,
					transferFrameBytes,
					time.Since(enterTime),
				), nil
			default:
				if route == snapshot.preferDirectRoute && !preferredBlockedObserved {
					snapshot.observeDirectAffinityBlocked()
					preferredBlockedObserved = true
				}
			}
		}

		// fast path for the common cases of up to two active routes (a single
		// transport, or a transport plus a p2p route): a static select avoids
		// the per-call reflect.SelectCase slice and reflect.ValueOf boxing that
		// reflect.Select requires. Unused route slots are nil channels, whose
		// select cases never fire, so 0/1/2 routes all share one static select.
		if len(activeRoutes) <= 2 {
			var route0, route1 Route
			if 1 <= len(activeRoutes) {
				route0 = activeRoutes[0]
			}
			if 2 <= len(activeRoutes) {
				route1 = activeRoutes[1]
			}
			var timeoutChan <-chan time.Time
			var expired bool
			if timeoutChan, expired = timeoutChannel(); expired {
				snapshot.releaseWriter()
				return false, transferWriteDisposition{}, nil
			}
			var selectedRoute Route
			select {
			case <-ctx.Done():
				snapshot.releaseWriter()
				return false, transferWriteDisposition{}, errors.New("Context done")
			case <-self.ctx.Done():
				snapshot.releaseWriter()
				return false, transferWriteDisposition{}, errors.New("Done")
			case <-notify:
				// new routes, try again
				snapshot.releaseWriter()
				continue
			case route0 <- transferFrameBytes:
				selectedRoute = route0
			case route1 <- transferFrameBytes:
				selectedRoute = route1
			case <-timeoutChan:
				snapshot.releaseWriter()
				return false, transferWriteDisposition{}, nil
			}
			self.updateSendStats(selectedRoute, 1, ByteCount(len(transferFrameBytes)))
			snapshot.observeDirectAffinityWrite(selectedRoute)
			snapshot.releaseWriter()
			return true, snapshot.blockedWriteDisposition(
				selectedRoute,
				transferFrameBytes,
				time.Since(enterTime),
			), nil
		}

		// select cases are in order:
		// - ctx.Done
		// - self.ctx.Done
		// - route writes...
		// - transport update
		// - timeout (may not exist)

		selectCases := make([]reflect.SelectCase, 0, 4+len(activeRoutes))

		// add the context done case
		contextDoneIndex := len(selectCases)
		selectCases = append(selectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ctx.Done()),
		})

		// add the done case
		doneIndex := len(selectCases)
		selectCases = append(selectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(self.ctx.Done()),
		})

		// add the update case
		transportUpdateIndex := len(selectCases)
		selectCases = append(selectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(notify),
		})

		// add all the route
		routeStartIndex := len(selectCases)
		if 0 < len(activeRoutes) {
			sendValue := reflect.ValueOf(transferFrameBytes)
			for _, route := range activeRoutes {
				selectCases = append(selectCases, reflect.SelectCase{
					Dir:  reflect.SelectSend,
					Chan: reflect.ValueOf(route),
					Send: sendValue,
				})
			}
		}

		timeoutIndex := len(selectCases)
		if timeoutChan, expired := timeoutChannel(); timeoutChan != nil || expired {
			if expired {
				// add a default case
				selectCases = append(selectCases, reflect.SelectCase{
					Dir: reflect.SelectDefault,
				})
			} else {
				// add a timeout case
				selectCases = append(selectCases, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(timeoutChan),
				})
			}
		}

		if chosenIndex, _, _ := reflect.Select(selectCases); 0 <= chosenIndex {
			switch chosenIndex {
			case contextDoneIndex:
				snapshot.releaseWriter()
				// MessagePoolReturn(transferFrameBytes)
				return false, transferWriteDisposition{}, errors.New("Context done")
			case doneIndex:
				snapshot.releaseWriter()
				// MessagePoolReturn(transferFrameBytes)
				return false, transferWriteDisposition{}, errors.New("Done")
			case transportUpdateIndex:
				snapshot.releaseWriter()
				// new routes, try again
			case timeoutIndex:
				snapshot.releaseWriter()
				// MessagePoolReturn(transferFrameBytes)
				return false, transferWriteDisposition{}, nil
			default:
				// a route
				routeIndex := chosenIndex - routeStartIndex
				route := activeRoutes[routeIndex]
				self.updateSendStats(route, 1, ByteCount(len(transferFrameBytes)))
				snapshot.observeDirectAffinityWrite(route)
				snapshot.releaseWriter()
				if self.log.V(2).Enabled() {
					self.log.Infof("[mrw]b %s->%s s(%s)\n", self.clientTag, self.destination.DestinationId, self.destination.SourceId)
				}
				return true, snapshot.blockedWriteDisposition(
					route,
					transferFrameBytes,
					time.Since(enterTime),
				), nil
			}
		}
	}
}

// MultiRouteReader
func (self *MultiRouteSelector) Read(ctx context.Context, timeout time.Duration) ([]byte, error) {
	transferFrameBytes, _, err := self.readWithCarrier(ctx, timeout)
	return transferFrameBytes, err
}

// ReadWithTransport reports the immutable transport tag published with the
// exact route whose channel delivered the frame.
func (self *MultiRouteSelector) ReadWithTransport(ctx context.Context, timeout time.Duration) ([]byte, TransportType, error) {
	transferFrameBytes, disposition, err := self.readWithCarrier(ctx, timeout)
	return transferFrameBytes, disposition.transportType, err
}

// readWithCarrier reports both the stable transport family and the exact
// receive-lane reliability published with the route that delivered the frame.
func (self *MultiRouteSelector) readWithCarrier(
	ctx context.Context,
	timeout time.Duration,
) ([]byte, transferReceiveDisposition, error) {
	self.readMutex.Lock()
	defer self.readMutex.Unlock()
	defer func() {
		if self.readTimer != nil {
			self.readTimer.Stop()
		}
	}()

	// read from the first channel available, in random priority
	enterTime := time.Now()
	for {
		// read the active routes and the transport-update channel from the
		// lock-free snapshot instead of taking the selector and monitor locks
		// on every packet
		snapshot := self.activeRoutesSnapshot.Load()
		notify := snapshot.notify
		activeRoutes := snapshot.shuffled()

		if self.log.V(2).Enabled() {
			self.log.Infof("[mrr] %s/%s<- s(%s) routes = %d\n", self.clientTag, self.destination.DestinationId, self.destination.StreamId, len(activeRoutes))
		}

		// non-blocking priority
		retry := false
		for _, route := range activeRoutes {
			select {
			case transferFrameBytes, ok := <-route:
				if ok {
					if self.log.V(2).Enabled() {
						self.log.Infof("[mrr]nb %s/%s<- s(%s)\n", self.clientTag, self.destination.DestinationId, self.destination.StreamId)
					}
					self.updateReceiveStats(route, 1, ByteCount(len(transferFrameBytes)))
					return transferFrameBytes, snapshot.receiveDisposition(route), nil
				} else {
					// mark the route as closed, try again
					self.setActive(route, false)
					retry = true
				}
			default:
			}
		}
		if retry {
			continue
		}

		// fast path for the common cases of up to two active routes (a single
		// transport, or a transport plus a p2p route): a static select avoids
		// the per-call reflect.SelectCase slice and reflect.ValueOf boxing that
		// reflect.Select requires. Unused route slots are nil channels, whose
		// select cases never fire, so 0/1/2 routes all share one static select.
		if len(activeRoutes) <= 2 {
			var route0, route1 Route
			if 1 <= len(activeRoutes) {
				route0 = activeRoutes[0]
			}
			if 2 <= len(activeRoutes) {
				route1 = activeRoutes[1]
			}
			var timeoutChan <-chan time.Time
			if 0 <= timeout {
				remainingTimeout := enterTime.Add(timeout).Sub(time.Now())
				if remainingTimeout <= 0 {
					return nil, transferReceiveDisposition{}, nil
				}
				timeoutChan = resetOrCreateTimer(&self.readTimer, remainingTimeout)
			}
			select {
			case <-ctx.Done():
				return nil, transferReceiveDisposition{}, errors.New("Context done")
			case <-self.ctx.Done():
				return nil, transferReceiveDisposition{}, errors.New("Done")
			case <-notify:
				// new routes, try again
				continue
			case transferFrameBytes, ok := <-route0:
				if ok {
					self.updateReceiveStats(route0, 1, ByteCount(len(transferFrameBytes)))
					return transferFrameBytes, snapshot.receiveDisposition(route0), nil
				}
				// mark the route as closed, try again
				self.setActive(route0, false)
				continue
			case transferFrameBytes, ok := <-route1:
				if ok {
					self.updateReceiveStats(route1, 1, ByteCount(len(transferFrameBytes)))
					return transferFrameBytes, snapshot.receiveDisposition(route1), nil
				}
				self.setActive(route1, false)
				continue
			case <-timeoutChan:
				return nil, transferReceiveDisposition{}, nil
			}
		}

		// select cases are in order:
		// - ctx.Done
		// - self.ctx.Done
		// - route reads...
		// - transport update
		// - timeout (may not exist)

		selectCases := make([]reflect.SelectCase, 0, 4+len(activeRoutes))

		// add the context done case
		contextDoneIndex := len(selectCases)
		selectCases = append(selectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(ctx.Done()),
		})

		// add the done case
		doneIndex := len(selectCases)
		selectCases = append(selectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(self.ctx.Done()),
		})

		// add the update case
		transportUpdateIndex := len(selectCases)
		selectCases = append(selectCases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(notify),
		})

		// add all the route
		routeStartIndex := len(selectCases)
		if 0 < len(activeRoutes) {
			for _, route := range activeRoutes {
				selectCases = append(selectCases, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(route),
				})
			}
		}

		timeoutIndex := len(selectCases)
		if 0 <= timeout {
			remainingTimeout := enterTime.Add(timeout).Sub(time.Now())
			if remainingTimeout <= 0 {
				// add a default case
				selectCases = append(selectCases, reflect.SelectCase{
					Dir: reflect.SelectDefault,
				})
			} else {
				// add a timeout case
				timeoutChan := resetOrCreateTimer(&self.readTimer, remainingTimeout)
				selectCases = append(selectCases, reflect.SelectCase{
					Dir:  reflect.SelectRecv,
					Chan: reflect.ValueOf(timeoutChan),
				})
			}
		}

		chosenIndex, value, ok := reflect.Select(selectCases)
		if self.log.V(2).Enabled() {
			self.log.Infof("[mrr]b %s/%s<- s(%s)\n", self.clientTag, self.destination.DestinationId, self.destination.StreamId)
		}

		switch chosenIndex {
		case contextDoneIndex:
			return nil, transferReceiveDisposition{}, errors.New("Context done")
		case doneIndex:
			return nil, transferReceiveDisposition{}, errors.New("Done")
		case transportUpdateIndex:
			// new routes, try again
		case timeoutIndex:
			// FIXME return nil, nil? don't use errors for timeouts
			return nil, transferReceiveDisposition{}, nil
		default:
			// a route
			routeIndex := chosenIndex - routeStartIndex
			route := activeRoutes[routeIndex]
			if ok {
				transferFrameBytes := value.Bytes()
				self.updateReceiveStats(route, 1, ByteCount(len(transferFrameBytes)))
				return transferFrameBytes, snapshot.receiveDisposition(route), nil
			} else {
				// mark the route as closed, try again
				self.setActive(route, false)
			}
		}
	}
}

// closeAndTakeRetiredWriterSnapshots cancels the selector and transfers every
// writer generation that must be joined by its lifecycle owner.
func (self *MultiRouteSelector) closeAndTakeRetiredWriterSnapshots() (
	retiredSnapshots []*routeSnapshot,
) {
	self.closeOnce.Do(func() {
		self.cancel()

		self.mutex.Lock()
		for route := range self.routeActive {
			self.routeActive[route] = false
		}
		self.transportUpdate.NotifyAll()
		self.updateActiveRoutesWithLock()
		retiredSnapshots = self.retiredWriterSnapshots
		self.retiredWriterSnapshots = nil
		self.mutex.Unlock()
	})
	return retiredSnapshots
}

// Close cancels a standalone selector and joins every writer generation it
// retired. RouteManager uses its ordered retirement tracker instead.
func (self *MultiRouteSelector) Close() {
	waitForRouteSnapshotWriters(self.closeAndTakeRetiredWriterSnapshots())
}

type RouteStats struct {
	sendCount        int
	sendByteCount    ByteCount
	receiveCount     int
	receiveByteCount ByteCount
}

func NewRouteStats() *RouteStats {
	return &RouteStats{
		sendCount:        0,
		sendByteCount:    ByteCount(0),
		receiveCount:     0,
		receiveByteCount: ByteCount(0),
	}
}

// conforms to `Transport`
type sendClientTransport struct {
	transportId  Id
	complement   bool
	destinations map[TransferPath]bool
}

func NewSendClientTransport(destinations ...TransferPath) *sendClientTransport {
	return NewSendClientTransportWithComplement(false, destinations...)
}

func NewSendClientTransportWithComplement(complement bool, destinations ...TransferPath) *sendClientTransport {
	destinations_ := map[TransferPath]bool{}
	for _, destination := range destinations {
		destinations_[destination] = true
	}
	return &sendClientTransport{
		transportId:  NewId(),
		complement:   complement,
		destinations: destinations_,
	}
}

func (self *sendClientTransport) TransportId() Id {
	return self.transportId
}

func (self *sendClientTransport) Priority() int {
	return 100
}

func (self *sendClientTransport) Weight() float32 {
	return 0
}

func (self *sendClientTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}

func (self *sendClientTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	// uniform weight
	return 1.0 / float32(1+len(remainingStats))
}

func (self *sendClientTransport) MatchesSend(destination TransferPath) bool {
	return self.complement != self.destinations[destination]
}

func (self *sendClientTransport) MatchesReceive(destination TransferPath) bool {
	return false
}

func (self *sendClientTransport) Downgrade(source TransferPath) {
	// nothing to downgrade
}

// conforms to `Transport`
type sendGatewayTransport struct {
	transportId   Id
	transportType TransportType
}

func NewSendGatewayTransport() *sendGatewayTransport {
	return NewSendGatewayTransportWithType(TransportTypeUnknown)
}

func NewSendGatewayTransportWithType(transportType TransportType) *sendGatewayTransport {
	return &sendGatewayTransport{
		transportId:   NewId(),
		transportType: transportType,
	}
}

func (self *sendGatewayTransport) TransportId() Id {
	return self.transportId
}

func (self *sendGatewayTransport) TransportType() TransportType {
	return self.transportType
}

func (self *sendGatewayTransport) Priority() int {
	return 100
}

func (self *sendGatewayTransport) Weight() float32 {
	return 0
}

func (self *sendGatewayTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}

func (self *sendGatewayTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	// uniform weight
	return 1.0 / float32(1+len(remainingStats))
}

func (self *sendGatewayTransport) MatchesSend(destination TransferPath) bool {
	return true
}

func (self *sendGatewayTransport) MatchesReceive(destination TransferPath) bool {
	return false
}

func (self *sendGatewayTransport) Downgrade(source TransferPath) {
	// nothing to downgrade
}

// conforms to `Transport`
type receiveGatewayTransport struct {
	transportId   Id
	transportType TransportType
}

func NewReceiveGatewayTransport() *receiveGatewayTransport {
	return NewReceiveGatewayTransportWithType(TransportTypeUnknown)
}

func NewReceiveGatewayTransportWithType(transportType TransportType) *receiveGatewayTransport {
	return &receiveGatewayTransport{
		transportId:   NewId(),
		transportType: transportType,
	}
}

func (self *receiveGatewayTransport) TransportId() Id {
	return self.transportId
}

func (self *receiveGatewayTransport) TransportType() TransportType {
	return self.transportType
}

func (self *receiveGatewayTransport) Priority() int {
	return 100
}

func (self *receiveGatewayTransport) Weight() float32 {
	return 0
}

func (self *receiveGatewayTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}

func (self *receiveGatewayTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	// uniform weight
	return 1.0 / float32(1+len(remainingStats))
}

func (self *receiveGatewayTransport) MatchesSend(destination TransferPath) bool {
	return false
}

func (self *receiveGatewayTransport) MatchesReceive(destination TransferPath) bool {
	return true
}

func (self *receiveGatewayTransport) Downgrade(source TransferPath) {
	// nothing to downgrade
}

// conforms to `Transport`
type prioritySendGatewayTransport struct {
	transportId Id
	priority    int
	weight      float32
}

func NewPrioritySendGatewayTransport(priority int, weight float32) *prioritySendGatewayTransport {
	return &prioritySendGatewayTransport{
		transportId: NewId(),
		priority:    priority,
		weight:      weight,
	}
}

func (self *prioritySendGatewayTransport) TransportId() Id {
	return self.transportId
}

func (self *prioritySendGatewayTransport) Priority() int {
	return self.priority
}

func (self *prioritySendGatewayTransport) Weight() float32 {
	return self.weight
}

func (self *prioritySendGatewayTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}

func (self *prioritySendGatewayTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	netWeight := self.weight
	for t, _ := range remainingStats {
		netWeight += t.Weight()
	}
	if 0 < netWeight {
		return self.weight / netWeight
	} else {
		return 1.0 / float32(1+len(remainingStats))
	}
}

func (self *prioritySendGatewayTransport) MatchesSend(destination TransferPath) bool {
	return true
}

func (self *prioritySendGatewayTransport) MatchesReceive(destination TransferPath) bool {
	return false
}

func (self *prioritySendGatewayTransport) Downgrade(source TransferPath) {
	// nothing to downgrade
}

// conforms to `Transport`
type priorityReceiveGatewayTransport struct {
	transportId Id
	priority    int
	weight      float32
}

func NewPriorityReceiveGatewayTransport(priority int, weight float32) *priorityReceiveGatewayTransport {
	return &priorityReceiveGatewayTransport{
		transportId: NewId(),
		priority:    priority,
		weight:      weight,
	}
}

func (self *priorityReceiveGatewayTransport) TransportId() Id {
	return self.transportId
}

func (self *priorityReceiveGatewayTransport) Priority() int {
	return self.priority
}

func (self *priorityReceiveGatewayTransport) Weight() float32 {
	return self.weight
}

func (self *priorityReceiveGatewayTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}

func (self *priorityReceiveGatewayTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	netWeight := self.weight
	for t, _ := range remainingStats {
		netWeight += t.Weight()
	}
	if 0 < netWeight {
		return self.weight / netWeight
	} else {
		return 1.0 / float32(1+len(remainingStats))
	}
}

func (self *priorityReceiveGatewayTransport) MatchesSend(destination TransferPath) bool {
	return false
}

func (self *priorityReceiveGatewayTransport) MatchesReceive(destination TransferPath) bool {
	return true
}

func (self *priorityReceiveGatewayTransport) Downgrade(source TransferPath) {
	// nothing to downgrade
}
