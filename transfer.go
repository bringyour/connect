package connect

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"math"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	// "runtime/debug"
	// "runtime"
	// "reflect"
	mathrand "math/rand"
	"slices"
	"strings"

	"maps"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/protocol"
)

/*
Sends frames to destinations with properties:
- as long the sending client is active, frames are eventually delivered up to timeout
- frames are received in order of send
- sender is notified when frames are received
- sender and receiver account for mutual transfer with a shared contract
- return transfer is accounted to the sender
- support for multiple routes to the destination
- senders are verified with pre-exchanged keys
- high throughput and bounded resource usage

*/

/*
Each transport should apply the forwarding ACL:
- reject if source id does not match network id
- reject if not an active contract between sender and receiver

*/

// The transfer speed of each client is limited by its slowest destination.
// All traffic is multiplexed to a single connection, and blocking
// the connection ultimately limits the rate of `SendWithTimeout`.
// In this a client is similar to a socket. Multiple clients
// can be active in parallel, each limited by their slowest destination.

// *important* note on how "nack" transfer works with contracts
// nack data is associated with a contract, which is sent with ack=true
// on the other side, if the contract_id is not active when the nack arrives,
// the nack is dropped.
// To avoid racing the nack message with the ack contract,
// nacks are sent as ack until the contract is acked

// use 0 for deadlock testing
const defaultTransferBufferSize = 32
const defaultReceiveSequenceBufferSize = 256

var DebugTransferCopyOnWrite = false

// errTransferRouteWriteTimeout identifies a bounded route write that found no
// accepting carrier. Keep the historical text for logs/API callers while
// giving higher layers a typed distinction from structural sequence,
// contract, and encryption failures.
var errTransferRouteWriteTimeout = errors.New("Timeout.")

// dropErrThrottle rate-limits `[r]drop`. A route that stops accepting writes
// produces one of these per dropped frame, which under a sustained fault is
// per-message. See logThrottle in log_throttle.go.
var dropErrThrottle = newLogThrottle(time.Minute)

// shouldLogDropErr determines whether a receive-side route drop error should be logged and reports the number of previously suppressed errors.
func shouldLogDropErr() (bool, int64) { return dropErrThrottle.Allow(time.Now()) }

// AckFunction is invoked inline by the owning send path. Blocking is
// intentional backpressure: callers can stop completion from outrunning their
// downstream state. Do not move it to a lossy/coalescing observer worker.
type AckFunction = func(err error)

// sendAckTarget is the allocation-free internal form of an acknowledgement
// callback with one small value attached. Like AckFunction, sendAckResult is
// invoked inline: blocking is intentional backpressure.
type sendAckTarget interface {
	sendAckResult(value ByteCount, err error)
}

type sendAckRecord struct {
	callback               AckFunction
	target                 sendAckTarget
	value                  ByteCount
	lifecycle              sendPackLifecycleRecord
	group                  *sendGroupCompletion
	retainAfterAckTimeout  bool
	transportWriteObserver func(TransportType)
}

func (self sendAckRecord) observeTransportWrite(transportType TransportType) {
	if self.transportWriteObserver != nil {
		self.transportWriteObserver(transportType)
	}
}

func (self sendAckRecord) empty() bool {
	return self.callback == nil && self.target == nil && self.lifecycle.empty() &&
		self.group == nil && !self.retainAfterAckTimeout
}

// retainPastAckTimeout reports whether this record contains bytes whose
// upstream owner cannot regenerate them after Transfer admission. A logical
// group's immutable parent record owns that promise for every physical chunk.
func (self sendAckRecord) retainPastAckTimeout() bool {
	if self.retainAfterAckTimeout {
		return true
	}
	return self.group != nil && self.group.ack.retainAfterAckTimeout
}

func (self sendAckRecord) invoke(err error) {
	if self.group != nil {
		self.group.terminal(err)
		return
	}
	defer self.lifecycle.terminal(err)
	if self.target == nil {
		safeAck(self.callback, err)
		return
	}
	defer func() {
		if r := recover(); r != nil {
			if !IsDoneError(r) {
				DefaultLogger().Warningf("Unexpected error: %s\n", ErrorJson(r, debug.Stack()))
			}
		}
	}()
	self.target.sendAckResult(self.value, err)
}

func (self sendAckRecord) firstRouteWrite(err error) {
	if self.group != nil {
		self.group.firstRouteWrite(err)
		return
	}
	self.lifecycle.firstRouteWrite(err)
}

// One original SendPack retains one immutable identity across coalescing,
// route disposition, resend ownership, and terminal acknowledgement.
type sendPackLifecycleRecord struct {
	observer            func(SendPackLifecycleObservation)
	clientId            Id
	destinationId       Id
	token               uint64
	ackRequired         bool
	messageType         protocol.MessageType
	upstreamRecoverable bool
}

// safeSendPackLifecycleObserve prevents optional measurement code from
// terminating or corrupting the send lifecycle it is observing.
func safeSendPackLifecycleObserve(
	observer func(SendPackLifecycleObservation),
	observation SendPackLifecycleObservation,
) {
	if observer == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			if !IsDoneError(r) {
				DefaultLogger().Warningf("Unexpected error: %s\n", ErrorJson(r, debug.Stack()))
			}
		}
	}()
	observer(observation)
}

// Keeps optional wire diagnostics outside the reliability lifecycle even if
// measurement code panics.
func safeTransferWireMessageObserve(
	observer func(TransferWireMessageObservation),
	observation TransferWireMessageObservation,
) {
	if observer == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			if !IsDoneError(r) {
				DefaultLogger().Warningf("Unexpected error: %s\n", ErrorJson(r, debug.Stack()))
			}
		}
	}()
	observer(observation)
}

// An empty record adds no callback or token work to the normal send path.
func (self sendPackLifecycleRecord) empty() bool {
	return self.observer == nil
}

// observe publishes one ordered lifecycle phase for this original Pack.
func (self sendPackLifecycleRecord) observe(phase SendPackLifecyclePhase, err error) {
	if self.observer == nil {
		return
	}
	safeSendPackLifecycleObserve(self.observer, SendPackLifecycleObservation{
		Phase:               phase,
		ClientId:            self.clientId,
		DestinationId:       self.destinationId,
		Token:               self.token,
		AckRequired:         self.ackRequired,
		MessageType:         self.messageType,
		UpstreamRecoverable: self.upstreamRecoverable,
		Err:                 err,
	})
}

// firstRouteWrite publishes the disposition of the first writer attempt.
func (self sendPackLifecycleRecord) firstRouteWrite(err error) {
	self.observe(SendPackLifecyclePhaseFirstRouteWrite, err)
}

// terminal publishes peer acknowledgement or the final sequence error.
func (self sendPackLifecycleRecord) terminal(err error) {
	self.observe(SendPackLifecyclePhaseTerminal, err)
}

// withoutRouteWrite closes both remaining phases for a Pack that never reached
// a writer. Error publication precedes each phase callback by construction.
func (self sendPackLifecycleRecord) withoutRouteWrite(err error) {
	self.firstRouteWrite(err)
	self.terminal(err)
}

// NoAck observation is independent of acknowledgement: a requested NoAck pack
// may temporarily use wire Ack while its opening contract is unacknowledged.
type noAckSendRecord struct {
	observer      func(NoAckSendObservation)
	clientId      Id
	destinationId Id
	token         uint64
	group         *sendGroupCompletion
}

// An empty record adds no callback or token work to the normal send path.
func (self noAckSendRecord) empty() bool {
	return self.observer == nil && self.group == nil
}

// Completion invokes the nonblocking observer with the immutable identity.
func (self noAckSendRecord) complete(err error) {
	if self.group != nil {
		self.group.noAckComplete(err)
		return
	}
	if self.observer == nil {
		return
	}
	self.observer(NoAckSendObservation{
		Phase:         NoAckSendPhaseCompleted,
		ClientId:      self.clientId,
		DestinationId: self.destinationId,
		Token:         self.token,
		Err:           err,
	})
}

// sendGroupCompletion joins the independently acknowledged wire chunks of one
// logical group back into the admission's single callback and observation
// identity. Each phase waits for every chunk disposition; no chunk can publish
// an early success while a later materialization or writer fails.
type sendGroupCompletion struct {
	mutex sync.Mutex

	chunkCount int

	firstRouteCount int
	firstRouteErr   error
	firstRouteDone  bool
	firstRouteReady chan struct{}

	noAckCount int
	noAckErr   error
	noAckDone  bool

	terminalCount int
	terminalErr   error
	terminalDone  bool

	ack   sendAckRecord
	noAck noAckSendRecord
}

func newSendGroupCompletion(sendPack *SendPack, chunkCount int) *sendGroupCompletion {
	return &sendGroupCompletion{
		chunkCount:      chunkCount,
		ack:             sendPack.ackRecord(),
		noAck:           sendPack.noAckRecord(),
		firstRouteReady: make(chan struct{}),
	}
}

func (self *sendGroupCompletion) chunkAckRecord() sendAckRecord {
	return sendAckRecord{
		group:                  self,
		transportWriteObserver: self.observeTransportWrite,
	}
}

func (self *sendGroupCompletion) observeTransportWrite(transportType TransportType) {
	self.mutex.Lock()
	observer := self.ack.transportWriteObserver
	if observer != nil {
		self.ack.transportWriteObserver = nil
	}
	self.mutex.Unlock()
	if observer != nil {
		observer(transportType)
	}
}

func (self *sendGroupCompletion) chunkNoAckRecord() noAckSendRecord {
	if self.noAck.empty() {
		return noAckSendRecord{}
	}
	return noAckSendRecord{group: self}
}

func (self *sendGroupCompletion) firstRouteWrite(err error) {
	self.mutex.Lock()
	if self.firstRouteDone {
		self.mutex.Unlock()
		return
	}
	self.firstRouteCount += 1
	self.firstRouteErr = errors.Join(self.firstRouteErr, err)
	if self.firstRouteCount < self.chunkCount {
		self.mutex.Unlock()
		return
	}
	self.firstRouteDone = true
	result := self.firstRouteErr
	lifecycle := self.ack.lifecycle
	// The lifecycle callback is synchronous intentional backpressure. Retain
	// the completion lock until it returns so terminal cannot overtake this
	// phase; the callback has no handle back into this private aggregator.
	lifecycle.firstRouteWrite(result)
	close(self.firstRouteReady)
	self.mutex.Unlock()
}

func (self *sendGroupCompletion) noAckComplete(err error) {
	self.mutex.Lock()
	if self.noAckDone {
		self.mutex.Unlock()
		return
	}
	self.noAckCount += 1
	self.noAckErr = errors.Join(self.noAckErr, err)
	if self.noAckCount < self.chunkCount {
		self.mutex.Unlock()
		return
	}
	self.noAckDone = true
	result := self.noAckErr
	record := self.noAck
	self.mutex.Unlock()

	record.complete(result)
}

func (self *sendGroupCompletion) terminal(err error) {
	self.mutex.Lock()
	if self.terminalDone {
		self.mutex.Unlock()
		return
	}
	self.terminalCount += 1
	self.terminalErr = errors.Join(self.terminalErr, err)
	if self.terminalCount < self.chunkCount {
		self.mutex.Unlock()
		return
	}
	self.terminalDone = true
	result := self.terminalErr
	record := self.ack
	firstRouteReady := self.firstRouteReady
	self.mutex.Unlock()

	// Lifecycle phases for one logical admission remain ordered even when a
	// reliable Ack races the last chunk's first-route observer.
	<-firstRouteReady
	record.invoke(result)
}

// The common packet coalescer needs at most two records. H1 can additionally
// drain a larger already-ready burst without waiting; keep that uncommon tail
// out of every long-lived sendItem and reuse it through a small process-wide
// pool. Expanding the inline array to the H1 limit would add roughly a
// kilobyte to every item in the 1,024-entry send-item pool.
const sendRecordOverflowPoolCapacity = 32

type noAckSendSetOverflow struct {
	records [sendPackH1GroupMaxFrames - sendPackBatchMaxFrames]noAckSendRecord
}

var noAckSendSetOverflowPool = make(
	chan *noAckSendSetOverflow,
	sendRecordOverflowPoolCapacity,
)

func takeNoAckSendSetOverflow() *noAckSendSetOverflow {
	select {
	case overflow := <-noAckSendSetOverflowPool:
		return overflow
	default:
		return &noAckSendSetOverflow{}
	}
}

func returnNoAckSendSetOverflow(overflow *noAckSendSetOverflow) {
	if overflow == nil {
		return
	}
	*overflow = noAckSendSetOverflow{}
	select {
	case noAckSendSetOverflowPool <- overflow:
	default:
	}
}

type noAckSendSet struct {
	count    uint8
	records  [sendPackBatchMaxFrames]noAckSendRecord
	overflow *noAckSendSetOverflow
}

// Coalescing retains one completion for every original NoAck pack.
func (self *noAckSendSet) add(record noAckSendRecord) {
	if record.empty() {
		return
	}
	index := int(self.count)
	if sendPackH1GroupMaxFrames <= index {
		panic("NoAck send observation set overflow")
	}
	if index < len(self.records) {
		self.records[index] = record
	} else {
		if self.overflow == nil {
			self.overflow = takeNoAckSendSetOverflow()
		}
		self.overflow.records[index-len(self.records)] = record
	}
	self.count += 1
}

// One initial route-write disposition completes every coalesced record.
func (self *noAckSendSet) complete(err error) {
	defer func() {
		returnNoAckSendSetOverflow(self.overflow)
		self.overflow = nil
	}()
	inlineCount := min(int(self.count), len(self.records))
	for index := range inlineCount {
		self.records[index].complete(err)
	}
	for index := inlineCount; index < int(self.count); index++ {
		self.overflow.records[index-len(self.records)].complete(err)
	}
}

type sendAckSetOverflow struct {
	records [sendPackH1GroupMaxFrames - sendPackBatchMaxFrames]sendAckRecord
}

var sendAckSetOverflowPool = make(
	chan *sendAckSetOverflow,
	sendRecordOverflowPoolCapacity,
)

func takeSendAckSetOverflow() *sendAckSetOverflow {
	select {
	case overflow := <-sendAckSetOverflowPool:
		return overflow
	default:
		return &sendAckSetOverflow{}
	}
}

func returnSendAckSetOverflow(overflow *sendAckSetOverflow) {
	if overflow == nil {
		return
	}
	*overflow = sendAckSetOverflow{}
	select {
	case sendAckSetOverflowPool <- overflow:
	default:
	}
}

// sendAckSet keeps the historical two-record common case inline. An H1-only
// ready drain may attach a pooled overflow, retaining exactly-once, in-order
// callback semantics without growing every in-flight sendItem.
type sendAckSet struct {
	count    uint8
	records  [sendPackBatchMaxFrames]sendAckRecord
	overflow *sendAckSetOverflow
}

func (self *sendAckSet) add(record sendAckRecord) {
	if record.empty() {
		return
	}
	index := int(self.count)
	if sendPackH1GroupMaxFrames <= index {
		panic("send ack set overflow")
	}
	if index < len(self.records) {
		self.records[index] = record
	} else {
		if self.overflow == nil {
			self.overflow = takeSendAckSetOverflow()
		}
		self.overflow.records[index-len(self.records)] = record
	}
	self.count++
}

func (self *sendAckSet) invoke(err error) {
	defer func() {
		returnSendAckSetOverflow(self.overflow)
		self.overflow = nil
	}()
	inlineCount := min(int(self.count), len(self.records))
	for index := range inlineCount {
		self.records[index].invoke(err)
	}
	for index := inlineCount; index < int(self.count); index++ {
		self.overflow.records[index-len(self.records)].invoke(err)
	}
}

// One physical coalesced write publishes one disposition for every original
// Pack record while retaining their distinct identities.
func (self *sendAckSet) firstRouteWrite(err error) {
	inlineCount := min(int(self.count), len(self.records))
	for index := range inlineCount {
		self.records[index].firstRouteWrite(err)
	}
	for index := inlineCount; index < int(self.count); index++ {
		self.overflow.records[index-len(self.records)].firstRouteWrite(err)
	}
}

func (self *sendAckSet) observeTransportWrite(transportType TransportType) {
	inlineCount := min(int(self.count), len(self.records))
	for index := range inlineCount {
		self.records[index].observeTransportWrite(transportType)
	}
	for index := inlineCount; index < int(self.count); index++ {
		self.overflow.records[index-len(self.records)].observeTransportWrite(transportType)
	}
}

// One retained record makes the complete serialized item non-discardable: an
// Ack covers the whole item, so Transfer cannot terminally release only the
// other coalesced records at the deadline.
func (self *sendAckSet) retainPastAckTimeout() bool {
	inlineCount := min(int(self.count), len(self.records))
	for index := range inlineCount {
		if self.records[index].retainPastAckTimeout() {
			return true
		}
	}
	for index := inlineCount; index < int(self.count); index++ {
		if self.overflow.records[index-len(self.records)].retainPastAckTimeout() {
			return true
		}
	}
	return false
}

// The receiver-visible identity of a transfer lane. The callback carries the
// source separately; peers reproduce these fields when constructing a reply.
type TransferKey struct {
	ForceStream         bool
	CompanionContract   bool
	EncryptionRole      protocol.SequenceRole
	EncryptionCompanion bool
	// LogicalLane is the bounded receiver-visible ordering lane. Zero is the
	// legacy/control lane; values 1..8 are accepted only after capability
	// negotiation and are reproduced on replies.
	LogicalLane uint32
}

// the identity of the source of received frames.
// `ProvideMode` is the mode of where these frames are from: network, friends and family, public.
// `Roles` and `Principal` are the source client's identity from the active contract,
// set only when the provide mode is network; nil roles and empty principal otherwise.
// `TransferKey` is immutable for the receive sequence and can be passed as a
// send option when constructing a reply.
type Peer struct {
	ProvideMode protocol.ProvideMode
	Roles       []string
	Principal   string
	TransferKey TransferKey
	// TransportType is the physical carrier that delivered this Pack. It is
	// immutable through receive ordering and batching.
	TransportType TransportType
}

// ReceiveFunction is invoked inline and must not block. A handoff uses a
// bounded queue with a zero timeout and drops when full. The frames, frame
// objects, and message bytes are borrowed and valid only until the callback
// returns; share or copy data that must outlive it.
type ReceiveFunction = func(source TransferPath, frames []*protocol.Frame, peer Peer)

// A forward callback receives a transfer frame addressed to another
// destination. It is inline and must not block. The bytes are borrowed for the
// call; a queued handoff must first share or copy them and use a zero timeout.
type ForwardFunction = func(path TransferPath, transferFrameBytes []byte)

func DefaultClientSettings() *ClientSettings {
	settings := DefaultClientSettingsWithBufferSize(defaultTransferBufferSize)
	// Unreliable carriers can grow one destination flight to 256 complete
	// messages. Give their zero-wait receive handoff matching count headroom,
	// while ReceiveSequence's independent byte budget bounds retained payload.
	settings.ReceiveBufferSettings = DefaultReceiveBufferSettings()
	return settings
}

func DefaultClientSettingsWithBufferSize(bufferSize int) *ClientSettings {
	settings := &ClientSettings{
		SendBufferSize:          bufferSize,
		ForwardBufferSize:       bufferSize,
		ReadTimeout:             30 * time.Second,
		BufferTimeout:           15 * time.Second,
		ControlPingTimeout:      time.Duration(0),
		SendBufferSettings:      DefaultSendBufferSettingsWithBufferSize(bufferSize),
		ReceiveBufferSettings:   DefaultReceiveBufferSettingsWithBufferSize(bufferSize),
		ForwardBufferSettings:   DefaultForwardBufferSettingsWithBufferSize(bufferSize),
		ContractManagerSettings: DefaultContractManagerSettingsWithBufferSize(bufferSize),
		StreamManagerSettings:   DefaultStreamManagerSettings(),
		PeerManagerSettings:     DefaultPeerManagerSettings(),
		WebRtcSettings:          DefaultWebRtcSettings(),
		EncryptionSettings:      DefaultEncryptionSettings(),
		ProtocolVersion:         DefaultProtocolVersion,
		DefaultTransferOpts:     DefaultTransferOpts(),
	}
	// A per-peer session is ref-held by both a send and a receive sequence, so
	// it must outlive the longer of the two — otherwise the next burst (after a
	// transport reform or lull) churns a fresh handshake instead of reusing the
	// live cipher.
	settings.EncryptionSettings.IdleTimeout = max(
		settings.SendBufferSettings.IdleTimeout,
		settings.ReceiveBufferSettings.IdleTimeout,
	)
	return settings
}

func DefaultClientSettingsNoNetworkEvents() *ClientSettings {
	clientSettings := DefaultClientSettings()
	clientSettings.ContractManagerSettings = DefaultContractManagerSettingsNoNetworkEvents()
	return clientSettings
}

// THROUGHPUTFIX §37.4's configuration surface, as one switch.
//
// Three knobs that must agree is how the defect this program exists to fix
// arrived, so a deployment sets one thing and everything else is derived from
// it and from machinery the hosts already set.
type WindowSizingPolicyKind int

const (
	// A constant send window, no memory budget and no rule: today's behaviour
	// exactly, byte for byte, which is what makes turning the rule on
	// reversible by one change.
	WindowSizingConstant WindowSizingPolicyKind = iota
	// Size the send window from what the path delivers, with the scale, the
	// ceiling, the shared budget and the initial bet all derived.
	WindowSizingFromDelivery
)

// The target throughput, in goodput bytes per second. One gigabit per second,
// settled. It is per client on a provider and per process on a phone
// (§37.4), and it is what turns a round trip into a window.
const targetGoodputByteRate = ByteCount(1000 * 1000 * 1000 / 8)

// Framed bytes per goodput byte, derived rather than fitted in §36.3, so that
// a goodput target means the same bytes at every layer.
const goodputFactor = 0.845

// The rule's scale: a window of k times what the path delivered per round
// trip, k = 2, so a window-limited flow doubles each round trip until it is no
// longer window-limited (§36.7).
const deliverySizedWindowScale = 2

// The fraction of the process memory budget each transfer direction draws.
//
// One eighth, which is 8 MiB at the 64 MiB reference: above today's 2 MiB
// window and 2.5 MiB hold, which is the point, and small enough that the two
// directions plus the carriers' quarter leave the process most of its budget.
// The fraction is the one quantity chosen here rather than derived.
const transferBudgetShareDivisor = 8

// The shared byte budget the transfer queues draw on when the rule is on.
//
// A draw on the total budget, proportional to it. It must never be a
// memory-scaled constant, and the distinction is the whole finding: the memory
// scale returns one at or above the 64 MiB reference and a fraction below, so
// every constant in this system was sized for a reference host and can only
// shrink from there. A provider with eight gigabytes runs a 64 MiB device's
// window, which is why no amount of memory has ever made this system faster.
// A share computed by scaling a constant would carry that defect forward under
// a new name, and the adjacent lines in this file all do exactly that, so
// `TestTheTransferShareIsADrawOnTheBudget` fails if this is ever rewritten in
// the local idiom.
//
// Zero means the process has no budget. That is not a small share, it is the
// absence of the surface, and the caller must then keep today's constant
// rather than fall to a floor.
func transferBudgetShareByteCount() ByteCount {
	budget := MemoryBudget()
	if budget <= 0 {
		return 0
	}
	return budget / transferBudgetShareDivisor
}

// The initial bet: what a sender may have outstanding before it has heard
// anything from its peer.
//
// Derived rather than chosen. A blind sender must assume the worst peer it
// could be talking to, and the worst peer is the one with the smallest hold,
// which is a client at the memory floor holding 320 KiB. Expressed as the
// assumed round trip the surface asks for, that is hold over target, 2.6 ms;
// expressed as a window it is the hold itself, because T x (hold/T) = hold.
// The blind period is one round trip rather than the life of the sequence,
// because the peer's real hold arrives on the first acknowledgement (§37.3).
func defaultInitialWindowByteCount() ByteCount {
	return kib(320)
}

// The process-wide send window policy. Set once by the host at start, the way
// the memory budget already is, so that turning the rule on is one change.
var defaultWindowSizing atomic.Int32

// SetWindowSizing chooses how every send window in this process is sized.
// Constant is today's behaviour exactly; from-delivery turns on the rule with
// the scale, the ceiling, the shared budget and the target all derived.
func SetWindowSizing(policy WindowSizingPolicyKind) {
	defaultWindowSizing.Store(int32(policy))
}

func DefaultWindowSizing() WindowSizingPolicyKind {
	return WindowSizingPolicyKind(defaultWindowSizing.Load())
}

// ApplyWindowSizing derives everything the rule needs from the policy, so a
// deployment sets one thing rather than three that must agree. Constant leaves
// the settings exactly as a tree without this program would build them.
func (self *SendBufferSettings) ApplyWindowSizing() {
	switch self.WindowSizing {
	case WindowSizingFromDelivery:
		self.DeliverySizedWindowScale = deliverySizedWindowScale
		self.TargetGoodputByteRate = targetGoodputByteRate
		// The budget first: derived from the process share, or the one a
		// caller attached. An unbudgeted process cannot participate in the
		// surface at all, and the right answer for it is today's behaviour
		// rather than a floor, because a share of nothing is nothing and
		// falling to the floor would make every unbudgeted provider slower the
		// moment the rule is turned on.
		if self.ResendQueueBudget == nil {
			if share := transferBudgetShareByteCount(); 0 < share {
				self.ResendQueueBudget = NewTransferMemoryBudget(share)
			}
		}
		// Then the ceiling, from whatever budget is attached, derived or
		// given. This was nested inside the share test and it is the defect
		// that made the whole rule inert: with no process budget set, a
		// caller's own attached budget left the ceiling at zero, the estimate
		// fell back to the initial size for it, and the resolved ceiling read
		// exactly 2 MiB at process budgets of 16, 64, 256 and 1024 MiB alike.
		// A sixty-four-fold increase in memory moved the window not at all,
		// and the whole fix measured four per cent slower than the constant it
		// was replacing.
		if self.ResendQueueBudget != nil && self.DeliverySizedWindowCeilingByteCount <= 0 {
			self.DeliverySizedWindowCeilingByteCount =
				self.ResendQueueBudget.TotalByteCount()
		}
		if self.ResendQueueBudget == nil {
			// Nothing to draw on: say so in the settings rather than leaving a
			// switch that reads on and does nothing. Attaching a budget is a
			// precondition for the fix rather than a tuning step.
			self.DeliverySizedWindowScale = 0
			self.TargetGoodputByteRate = 0
			self.DeliverySizedWindowCeilingByteCount = 0
		}
	default:
		self.DeliverySizedWindowScale = 0
		self.DeliverySizedWindowCeilingByteCount = 0
		self.TargetGoodputByteRate = 0
		self.ResendQueueBudget = nil
	}
}

// WindowSizingActive reports whether the rule can actually act, which is not
// the same as the switch being set: without a budget to draw on there is no
// share, so the rule holds today's constant. A caller that turns the switch on
// and gets false here has a process with no memory budget attached.
func (self *SendBufferSettings) WindowSizingActive() bool {
	return 0 < self.DeliverySizedWindowScale && self.ResendQueueBudget != nil
}

func DefaultSendBufferSettings() *SendBufferSettings {
	return DefaultSendBufferSettingsWithBufferSize(defaultTransferBufferSize)
}

func DefaultSendBufferSettingsWithBufferSize(bufferSize int) *SendBufferSettings {
	settings := &SendBufferSettings{
		CreateContractTimeout: 30 * time.Second,
		// Retry a failed/absent contract promptly. A same-network peer connect can
		// briefly return NoPermission while the target's provide registration is
		// still committing; the send sequence blocks on this interval before
		// retrying, so a long first interval turns that race into a multi-second
		// stall. Subsequent failures back off to the max to avoid multiplying
		// contract-control/API load for a persistently unavailable destination.
		CreateContractRetryInterval:    1 * time.Second,
		CreateContractRetryMaxInterval: 5 * time.Second,
		// the COLD resend floor: applies only while no rtt samples exist
		// (nothing acked yet) — no evidence, so retry conservatively
		MinResendInterval: 2 * time.Second,
		// the resend floor once the path rtt is measured: a lost packet on a
		// fast path retries in hundreds of ms instead of the cold floor —
		// this bounds per-loss pauses AND how fast traffic shifts off a
		// stalled route onto a sibling. The per-item exponential backoff
		// (see the resend loop) caps the duplicate cost of an eager retry,
		// and the receiver dedups by sequence number.
		RttMinResendInterval: 300 * time.Millisecond,
		MaxResendInterval:    8 * time.Second,
		// QUIC DATAGRAM has no lower-layer payload retransmission. Its byte and
		// message flight controller already contracts on loss, so keep the
		// oldest missing Pack moving often enough that an inner TCP sender does
		// not exhaust its own retry budget behind Transfer's exponential tail.
		UnreliableMaxResendInterval: 2 * time.Second,
		// scale over the MEAN window rtt: headroom for jitter/ack-compress
		// (10ms) without variance tracking. 1.2 was tight enough that the
		// floor always governed; 2.0 makes the rtt-scaled value meaningful
		// on paths slower than the floor.
		RttScale:         2.0,
		RttWindowSize:    128,
		RttWindowTimeout: 60 * time.Second,
		AckTimeout:       60 * time.Second,
		// A live lossy datagram carrier can need longer than MultiClient's
		// ordinary 30-second provider-failure bar to recover one Pack. The
		// liveness watchdog owns early dead-exit conviction; this is only the
		// final reliable-delivery lifetime and must not bypass those gates.
		UnreliableAckTimeout: 90 * time.Second,
		IdleTimeout:          300 * time.Second,
		// pause on resend for selectively acknowledged messages
		SelectiveAckTimeout: 60 * time.Second,
		// Three distinct later deliveries are the conventional reordering-safe
		// evidence for one missing Pack. Recovery is limited to one immediate
		// retransmit per item; ordinary timeout/backoff owns any further loss.
		SelectiveAckGapThreshold: 3,
		// Recover a small scoreboard of proven holes per ACK round. QUIC still
		// paces these writes; the cap prevents a window-wide retry burst.
		SelectiveAckGapBurstSize: 4,
		// Once a path RTT exists, cumulative ACK progress can pace up to two
		// probes of the oldest remaining Pack before its queue-inflated/cold RTO.
		AckTailProbeLimit: 2,
		// An unreliable carrier has no payload retransmit below Transfer. Keep
		// its cold acknowledgement flight below the measured cell-edge queue,
		// then let receiver progress grow it. Reliable carriers ignore these.
		UnreliableInitialFlightByteCount: 8 * 1024,
		UnreliableMinimumFlightByteCount: 8 * 1024,
		UnreliableMaximumFlightByteCount: 256 * 1024,
		// A byte-only flight still overruns a packet-count queue when the
		// messages are small TCP ACKs. Bound complete Transfer messages too;
		// eight stays below the measured 13-packet one-bar queue.
		UnreliableInitialFlightMessageCount: 8,
		UnreliableMinimumFlightMessageCount: 4,
		UnreliableMaximumFlightMessageCount: 256,
		// Transfer sits above QUIC's own startup controller. Grow its separate
		// delivery window by 25% per acknowledged window, not another 100%.
		UnreliableSlowStartGrowthDivisor: 4,
		// Approximately one conservative QUIC DATAGRAM payload. After loss,
		// one fully acknowledged window adds about this much capacity.
		UnreliableFlightIncreaseByteCount:         1150,
		UnreliableFlightIncreaseMessageCount:      1,
		UnreliableFloorSingleFlight:               false,
		DeferTimeoutResendWhileCumulativeProgress: true,
		TimeoutResendDeferLimit:                   2,
		SequenceBufferSize:                        bufferSize,
		AckBufferSize:                             bufferSize,
		MinMessageByteCount:                       ByteCount(1),
		ContractWaitLogThreshold:                  50 * time.Millisecond,
		// this includes transport reconnections
		WriteTimeout: 15 * time.Second,
		// per send sequence (per peer), so scaled by the memory budget.
		// when a shared budget is set, the max acts as the per-sequence
		// borrow cap and the min as the guaranteed floor.
		ResendQueueMaxByteCount: MemoryScaledByteCount(mib(2), kib(256)),
		ResendQueueMinByteCount: kib(256),
		// THROUGHPUTFIX §37.17 guard two. On by default: a selective
		// acknowledgement earned by a route that has since died is not proof
		// the receiver still holds the item, and the sender's mark otherwise
		// stands for a minute. Off is the pre-guard behaviour and exists so a
		// cell can measure the difference in one binary.
		CarrierChangeVoidsSelectiveAck: true,
		// derived from WindowSizing below; a cell may still set them directly
		DeliverySizedWindowScale:            0,
		DeliverySizedWindowCeilingByteCount: 0,
		// zero preserves the behavior of every tree before THROUGHPUTFIX §27;
		// the lane campaign sets it
		LaneFloorByteCount: 0,
		// Off by default: the bound is implemented to §22 and measured on
		// the §21.5 instrument, where it holds the resend queue below the
		// budget as designed but costs about twice the transfer time in
		// every arm and acts on the shallow-queue arm the design calls
		// inert. Landing it on awaits a shape where it pays
		// (FLIGHTGATEFIX §22.4).
		ReliableAdmissionBoundedByDelivery: false,
		DeferredItemIsLateForTheScoreboard: false,
		DeferTimeoutResendBackoff:          true,
		ReliableTimerUsesDeviation:         false,
		ReliableLaneProvenRecovery:         false,
		ContractFillFraction:               0.8,
		PrewarmOpeningContract:             true,
		CompactContractHead:                true,
		// Disabled until the 1/4/8-lane low-bar campaign selects a measured
		// default. Receivers always understand and advertise bounded lanes, so a
		// rollout can enable senders independently without breaking legacy peers.
		LogicalDataLaneCount: 0,
		ProtocolVersion:      DefaultProtocolVersion,
	}
	settings.WindowSizing = DefaultWindowSizing()
	settings.ApplyWindowSizing()
	return settings
}

// ApplyWindowSizing derives the hold from the surface.
//
// The hold has to move with the window or it becomes the binder the moment
// windows can grow: 2.5 MiB is 90 Mb/s at a 200 ms round trip, and the whole
// raise is inert above it. Deriving both from the same share also makes the
// window-under-hold relationship hold at every pair of budgets by
// construction, between peers that advertise; against a legacy sender,
// committed-prefix acknowledgement makes an overrun cost bandwidth rather than
// correctness, so no floor raise is needed anywhere.
//
// An honest limit, stated here rather than only in the design. A phone cannot
// reach the target on a long path within its budget, by arithmetic: one window
// at one gigabit per second on a 200 ms path is 29 MB framed against 25 MB for
// the whole process. At an 8 MiB receive share the plateau is 290 Mb/s at
// 200 ms, which is the target at 58 ms and below. That is acceptable because it
// fails visibly — the window equals the advertised capacity and the estimate
// names the term that bound it — rather than silently at a constant, which is
// the whole difference between this and what it replaces.
func (self *ReceiveBufferSettings) ApplyWindowSizing() {
	switch self.WindowSizing {
	case WindowSizingFromDelivery:
		if share := transferBudgetShareByteCount(); 0 < share {
			self.ReceiveQueueMaxByteCount = max(share, self.ReceiveQueueMinByteCount)
			if self.ReceiveQueueBudget == nil {
				self.ReceiveQueueBudget = NewTransferMemoryBudget(share)
			}
		}
		self.AdvertiseReceiveWindow = true
	default:
		self.AdvertiseReceiveWindow = false
	}
}

func DefaultReceiveBufferSettings() *ReceiveBufferSettings {
	return DefaultReceiveBufferSettingsWithBufferSize(defaultReceiveSequenceBufferSize)
}

func DefaultReceiveBufferSettingsWithBufferSize(bufferSize int) *ReceiveBufferSettings {
	settings := &ReceiveBufferSettings{
		GapTimeout: 60 * time.Second,
		// the receive idle timeout should be a bit longer than the send idle timeout
		IdleTimeout:          120 * time.Second,
		SequenceBufferSize:   bufferSize,
		H1SequenceBufferSize: bufferSize,
		// Count headroom absorbs a flight of small tunnel packets. Retained
		// encoded Transfer bytes remain independently bounded, so large frames
		// cannot multiply the channel capacity into a memory spike.
		SequenceBufferByteCount:   kib(256),
		H1SequenceBufferByteCount: kib(256),
		// A reliable carrier reader may retain its one already-read frame until
		// the bounded per-sequence queue drains or the sequence/client closes.
		// This restores stream backpressure without enlarging any queue.
		ReliablePackHandoffTimeout: -1,
		// AckBufferSize: DefaultTransferBufferSize,
		// coalesce acks into a periodic cumulative head ack
		// without coalescing, every received message emits an ack frame, which
		// doubles the per-pair message volume on the relay path for one-way
		// streams and feeds resend storms under load. The window is far below
		// the resend floors (RttMinResendInterval 300ms / cold 2s), so it does
		// not affect resends; it does inflate measured rtt by up to 10ms,
		// which the RttScale headroom absorbs.
		AckCompressTimeout:  10 * time.Millisecond,
		MinMessageByteCount: ByteCount(1),
		// ResendAbuseThreshold: 4,
		// ResendAbuseMultiple:  0.5,
		MaxPeerAuditDuration: 60 * time.Second,
		// this includes transport reconnections
		WriteTimeout: 15 * time.Second,
		// per receive sequence (per peer), so scaled by the memory budget.
		// when a shared budget is set, the max acts as the per-sequence
		// borrow cap and the min as the guaranteed floor.
		ReceiveQueueMaxByteCount: MemoryScaledByteCount(mib(2)+kib(512), kib(320)),
		EvictionNotice:           true,
		ReceiveQueueMinByteCount: kib(320),
		AllowLegacyNack:          true,
		MaxOpenReceiveContract:   4,
		ProtocolVersion:          DefaultProtocolVersion,
	}
	settings.WindowSizing = DefaultWindowSizing()
	settings.ApplyWindowSizing()
	return settings
}

func DefaultForwardBufferSettings() *ForwardBufferSettings {
	return DefaultForwardBufferSettingsWithBufferSize(defaultTransferBufferSize)
}

func DefaultForwardBufferSettingsWithBufferSize(bufferSize int) *ForwardBufferSettings {
	return &ForwardBufferSettings{
		IdleTimeout:        300 * time.Second,
		SequenceBufferSize: bufferSize,
		WriteTimeout:       15 * time.Second,
	}
}

type SendPack struct {
	TransferOptions

	// frame and destination is repacked by the send buffer into a Pack,
	// with destination and frame from the tframe, and other pack properties filled in by the buffer.
	// Frames normally carries a batch coalesced into ONE wire Pack (one sequence
	// number, one Ack). A logicalGroup is the explicit exception: it owns the
	// whole slice at one admission, then its SendSequence advances ordered
	// transport-bounded chunks without another routing decision. Frame is the
	// single-frame form; exactly one of Frame / Frames is set. See frameList.
	// Ownership: the pack owns the Frames slice (referenced asynchronously
	// until marshal) — the sender must not reuse its backing array, per the
	// message pool send-ownership rule.
	Frame  *protocol.Frame
	Frames []*protocol.Frame
	// logicalGroup marks Frames as one admission/completion unit. The owning
	// SendSequence may split it into ordered wire Packs at the transport-safe
	// frame/byte bounds without returning to Client routing or admission.
	logicalGroup    bool
	groupFrameIndex int
	groupCompletion *sendGroupCompletion
	// Logical-group chunk limits are pinned when the first chunk enters its
	// SendSequence. H1 can safely carry a larger WebSocket message; pinning
	// keeps completion accounting stable if carrier availability changes while
	// later chunks are still subject to resend-budget backpressure.
	groupChunkMaxFrames           int
	groupChunkMaxMessageByteCount ByteCount
	// singleFrame backs frameList for the common single-frame form so the
	// multi-frame generalization does not add one slice allocation to every
	// legacy send.
	singleFrame [1]*protocol.Frame
	// singleFrameValue is used by internal raw-frame senders whose frame has
	// exactly the SendPack lifetime. Embedding it fuses two escaping objects
	// into the one pack allocation without changing the public Frame API.
	singleFrameValue protocol.Frame
	Destination      Id
	IntermediaryIds  MultiHopId
	// called (true) when the pack is ack'd, or (false) if not ack'd (closed before ack)
	AckCallback AckFunction
	ackTarget   sendAckTarget
	ackValue    ByteCount
	// MessageByteCount is the enqueue-time observation retained for API
	// compatibility and diagnostics. Contract accounting must use
	// serializedMessageByteCount instead: Frame is asynchronous, and trusting
	// duplicated mutable metadata can terminate a sequence if a buggy caller
	// lets a borrowed frame escape its callback lifetime.
	MessageByteCount ByteCount
	Ctx              context.Context
	// ForceUnwrapped pins the wire frame to plaintext for the item's lifetime,
	// including retransmits. Used by session control messages (TLS handshake
	// bytes) that bootstrap the cipher: they must never be sent encrypted, since
	// the peer may not have completed its half of the handshake even after our
	// local cipher is established.
	ForceUnwrapped bool
	// EncryptionRole selects which per-peer session this pack uses, keying the
	// SendSequence so the roles run as distinct sequences: client (the default —
	// the client's own outbound data, whose handshake it initiates/restarts) or
	// server (EncryptedControl carriers and server-session replies, which never
	// restart the handshake).
	EncryptionRole sequenceTlsRole
	// EncryptionCompanion is the per-peer session identity companion this pack
	// uses — keys the SendSequence and its session, distinct from
	// `TransferOptions.CompanionContract` (the contract it rides; the two differ
	// only for a server-role EncryptedControl reply carrier).
	EncryptionCompanion bool
	// logicalLane is selected by SendBuffer immediately before sequence lookup.
	// An explicit TransferKey reproduces a received lane; otherwise a negotiated
	// peer and valid schedulingKey select one bounded hashed data lane.
	logicalLane         uint32
	logicalLaneExplicit bool
	// rawPool is set only on internal v2 raw packs. The pack returns to this
	// bounded pool after its frames have been synchronously serialized (or the
	// queue rejects it); public SendPack ownership is unchanged.
	rawPool chan *SendPack
	// Optional NoAck observation travels independently from Ack callback/target
	// and is consumed by the first route-write attempt only.
	noAckObserver func(NoAckSendObservation)
	noAckClientId Id
	noAckToken    uint64
	// Optional all-Pack lifecycle observation follows this original Pack
	// through coalescing and terminal Ack/error disposition.
	lifecycleObserver            func(SendPackLifecycleObservation)
	lifecycleClientId            Id
	lifecycleToken               uint64
	lifecycleMessageType         protocol.MessageType
	lifecycleUpstreamRecoverable bool
	// retainAfterAckTimeout transfers the only recoverable copy to Transfer.
	// Its serialized resend item remains owned until peer Ack or lifecycle
	// cancellation instead of becoming a silent loss at the ordinary deadline.
	retainAfterAckTimeout  bool
	transportWriteObserver func(TransportType)
	// schedulingKey is local-only pre-sequence metadata. Its exact five-tuple is
	// never put on the wire; after negotiation only its bounded lane hash enters
	// the sequence identity. It is cleared when this Pack is recycled.
	schedulingKey sendSchedulingKey
	// admission is non-nil while this original Pack occupies one bounded
	// pre-sequence slot, including while a logical group is between chunks.
	admission    *sendPackAdmission
	admissionKey sendSchedulingKey
}

func (self *SendPack) ackRecord() sendAckRecord {
	return sendAckRecord{
		callback:               self.AckCallback,
		target:                 self.ackTarget,
		value:                  self.ackValue,
		lifecycle:              self.lifecycleRecord(),
		retainAfterAckTimeout:  self.retainAfterAckTimeout,
		transportWriteObserver: self.transportWriteObserver,
	}
}

// The immutable lifecycle identity follows this Pack into the send item.
func (self *SendPack) lifecycleRecord() sendPackLifecycleRecord {
	return sendPackLifecycleRecord{
		observer:            self.lifecycleObserver,
		clientId:            self.lifecycleClientId,
		destinationId:       self.Destination,
		token:               self.lifecycleToken,
		ackRequired:         self.Ack,
		messageType:         self.lifecycleMessageType,
		upstreamRecoverable: self.lifecycleUpstreamRecoverable,
	}
}

// A Pack rejected before sequence ownership closes both remaining phases;
// the unsuccessful public send deliberately does not invoke its Ack callback.
func (self *SendPack) completeLifecycleWithoutRouteWrite(err error) {
	self.lifecycleRecord().withoutRouteWrite(err)
}

// Sequence-owned failure publishes the missing writer disposition only. Its
// Ack record remains the single owner of terminal callback publication.
func (self *SendPack) completeLifecycleFirstRouteWrite(err error) {
	self.lifecycleRecord().firstRouteWrite(err)
}

// The immutable route-write observation follows this pack through coalescing.
func (self *SendPack) noAckRecord() noAckSendRecord {
	return noAckSendRecord{
		observer:      self.noAckObserver,
		clientId:      self.noAckClientId,
		destinationId: self.Destination,
		token:         self.noAckToken,
	}
}

// Rejection and shutdown paths report the pack before returning its buffers.
func (self *SendPack) completeNoAck(err error) {
	self.noAckRecord().complete(err)
}

func (self *SendPack) invokeAck(err error) {
	self.ackRecord().invoke(err)
}

func (self *SendPack) releaseAdmission() {
	if self.admission == nil {
		return
	}
	admission := self.admission
	self.admission = nil
	admissionKey := self.admissionKey
	self.admissionKey = sendSchedulingKey{}
	admission.release(admissionKey)
}

func (self *SendPack) releaseRaw() {
	// This is the terminal SendPack handoff. Release the pre-sequence slot and
	// clear every field before returning the object to its pool. A caller must
	// not read or write self after this call: another sender may immediately
	// acquire and initialize the same object.
	self.releaseAdmission()
	if self.rawPool == nil {
		return
	}
	pool := self.rawPool
	*self = SendPack{}
	select {
	case pool <- self:
	default:
		// The explicit cap bounds retained memory; excess concurrent packs are
		// left for the GC rather than blocking the packet path.
	}
}

// sendPackBatchMaxFrames / sendPackBatchMaxMessageByteCount bound
// opportunistic sequence coalescing so the complete TransferFrame remains
// below both the platform transport limit and H3's bounded DATAGRAM-carrier
// threshold. The byte bound matches the global tunnel MTU: one full tunnel
// packet or multiple smaller packets with no more than one MTU of message
// bytes remain eligible for the hybrid packet lane. There is no batching wait:
// the sequence only takes a second Pack when it is already queued.
const sendPackBatchMaxFrames = 2
const sendPackBatchMaxMessageByteCount = DefaultMtu
const sendPackH1GroupMaxFrames = 16
const sendPackH1GroupMaxMessageByteCount = 3 * 1024

// Once no contract frame can ride on the Pack, three complete tunnel-MTU
// packets still fit the ordinary 4-KiB encrypted H1 data envelope. Opening and
// rotating contracts retain the 3-KiB bound above; handshake carriers use the
// larger transport minimum.
const sendPackH1EstablishedMaxMessageByteCount = 3 * DefaultMtu
const rawSendPackPoolCapacity = 8

type ReceivePack struct {
	Source             TransferPath
	SequenceId         Id
	Pack               *protocol.Pack
	decodedOwner       *decodedPackOwner
	ReceiveCallback    ReceiveFunction
	MessageByteCount   ByteCount
	TransferFrameBytes []byte
	// sequenceQueueByteCount is owned by ReceiveSequence between successful
	// Pack admission and channel dequeue. It is the per-flow logical encoded
	// byte charge and is zero outside that interval.
	sequenceQueueByteCount ByteCount
	// sequenceQueueBudgetByteCount is the independently selected aggregate
	// charge. Opt-in diagnostics can use the retained allocation computed by
	// receiveQueueByteCount; default mobile, desktop, and server paths retain
	// encoded-byte accounting. It is released with sequenceQueueByteCount.
	sequenceQueueBudgetByteCount ByteCount
	// sequenceQueueBudget is the optional aggregate budget reservation paired
	// with sequenceQueueBudgetByteCount. It is cleared at the same dequeue
	// boundary.
	sequenceQueueBudget *TransferMemoryBudget
	// retainedQueueByteCount memoizes the allocation scan shared by handoff and
	// reorder accounting. Zero means not computed; every live Pack has a
	// positive charge.
	retainedQueueByteCount ByteCount
	TransportType          TransportType
	Ctx                    context.Context
	// Unwrapped is true when the inbound TransferFrame arrived as plaintext (no
	// outer wrap). The ack for this pack (and any aggregated ack including it) is
	// sent plaintext to mirror, so a peer whose cipher isn't up yet isn't handed
	// a wrapped ack it can't open.
	Unwrapped bool
	// EncryptionRole is the local per-peer session role that owns this inbound
	// stream — the complement of the sender's role, keying the ReceiveSequence
	// and the session it holds. Normal peer data (peer is the TLS client) maps
	// to our server session (the default); EncryptedControl carriers and
	// server-session replies map to our client session.
	EncryptionRole sequenceTlsRole
	// EncryptionCompanion is the local session identity companion that owns this
	// inbound stream (shared by both peers, not complemented). Derived from the
	// wire companion hint, the decrypting session, or the EncryptedControl;
	// defaults false. Keys the ReceiveSequence and its session.
	EncryptionCompanion bool
}

// A decoded owner is 968 bytes on the measured arm64 mobile target. Charge a
// rounded KiB so the aggregate receive budget covers the pipeline envelope as
// well as both pooled byte roots and remains conservative on other targets.
const decodedPackOwnerQueueByteCount = ByteCount(1024)

func addReceiveQueueByteCount(total ByteCount, value ByteCount) ByteCount {
	value = max(0, value)
	if ByteCount(math.MaxInt64)-value < total {
		return ByteCount(math.MaxInt64)
	}
	return total + value
}

// receiveQueueByteCount is the retained allocation charge after a Pack moves
// from the handoff channel into the reorder queue. MessageByteCount is only
// payload/contract accounting: every queued Pack also holds the encrypted
// carrier-frame root, one pooled root per decoded Frame, and its owner object.
func (self *ReceivePack) receiveQueueByteCount() ByteCount {
	if self == nil {
		return 0
	}
	if 0 < self.retainedQueueByteCount {
		return self.retainedQueueByteCount
	}
	byteCount := MessagePoolRootByteCount(self.TransferFrameBytes)
	if self.Pack != nil {
		for _, frame := range self.Pack.Frames {
			if frame != nil {
				byteCount = addReceiveQueueByteCount(
					byteCount,
					MessagePoolRootByteCount(frame.MessageBytes),
				)
			}
		}
		if frame := self.Pack.ContractFrame; frame != nil {
			byteCount = addReceiveQueueByteCount(
				byteCount,
				MessagePoolRootByteCount(frame.MessageBytes),
			)
		}
	}
	if self.decodedOwner != nil {
		byteCount = addReceiveQueueByteCount(byteCount, decodedPackOwnerQueueByteCount)
	}
	self.retainedQueueByteCount = max(1, max(byteCount, self.MessageByteCount))
	return self.retainedQueueByteCount
}

func (self *ReceivePack) messagePoolReturn() {
	if self == nil {
		return
	}
	// Capture and clear the outer frame before releasing decodedOwner. In the
	// owned hot path this ReceivePack is embedded in that owner, which can be
	// taken and reset by another decoder as soon as release returns.
	transferFrameBytes := self.TransferFrameBytes
	self.TransferFrameBytes = nil
	if self.decodedOwner != nil {
		owner := self.decodedOwner
		self.decodedOwner = nil
		MessagePoolReturn(transferFrameBytes)
		owner.release()
		return
	}
	returnDecodedPackMessageBytes(self.Pack)
	self.Pack = nil
	MessagePoolReturn(transferFrameBytes)
}

type ForwardPack struct {
	Destination        TransferPath
	TransferFrameBytes []byte
	Ctx                context.Context
}

type TransferOptions struct {
	// items can choose to not be acked
	// in this case, the ack callback is called on send, and no retry is done
	// when false, items may arrive out of order amongst un-acked sequence neighbors
	Ack bool
	// use a companion contract
	// a companion contract replies to an existing contract
	// using this option limits the destination to clients that have an active contract to the sender
	CompanionContract bool
	// force contract streams, even when there are zero intermediaries
	ForceStream bool
	// NetworkPeer selects the bounded no-escrow Network contract policy. It is
	// deliberately independent of ForceStream: public direct streams may use
	// ForceStream but must retain ordinary escrow sizing and expiry.
	NetworkPeer bool
}

func DefaultTransferOpts() TransferOptions {
	return TransferOptions{
		Ack:               true,
		CompanionContract: false,
		ForceStream:       false,
		NetworkPeer:       false,
	}
}

type transferOptionsSetAck struct {
	Ack bool
}

func NoAck() transferOptionsSetAck {
	return transferOptionsSetAck{
		Ack: false,
	}
}

type transferOptionsSetCompanionContract struct {
	CompanionContract bool
}

func CompanionContract() transferOptionsSetCompanionContract {
	return transferOptionsSetCompanionContract{
		CompanionContract: true,
	}
}

type transferOptionsSetForceStream struct {
	ForceStream bool
}

// transportWriteOption is an internal logical-traffic observer. It fires once
// after the Pack's first successful physical route write, including a later
// resend when the initial route attempt failed.
type transportWriteOption struct {
	observer func(TransportType)
}

// sendPackRecoveryOption is internal ownership metadata. UpstreamRecoverable
// describes the caller before admission for lifecycle measurement. A retained
// Pack moves its only recoverable bytes into Transfer on admission, so its
// bounded resend item must live until peer Ack or lifecycle cancellation.
type sendPackRecoveryOption struct {
	upstreamRecoverable   bool
	retainAfterAckTimeout bool
}

// A send option naming the `sendAckTarget` of the Pack, for callers whose
// entry point takes an `AckFunction`. A target takes precedence over the
// callback in `sendAckRecord.invoke`.
type sendAckTargetOption struct {
	target sendAckTarget
}

func observeTransportWrite(observer func(TransportType)) transportWriteOption {
	return transportWriteOption{observer: observer}
}

func ForceStream() transferOptionsSetForceStream {
	return transferOptionsSetForceStream{
		ForceStream: true,
	}
}

type transferCtx struct {
	Ctx context.Context
}

func Ctx(ctx context.Context) transferCtx {
	return transferCtx{
		Ctx: ctx,
	}
}

type ClientSettings struct {
	SendBufferSize    int
	ForwardBufferSize int
	ReadTimeout       time.Duration
	BufferTimeout     time.Duration
	// if 0, the client will not send control pings
	ControlPingTimeout time.Duration

	// Log, when set, is used by the client and all nested components
	// (propagated to nested settings `Log` fields that are nil).
	// nil resolves to `DefaultLogger()`. See log.go.
	Log Logger

	SendBufferSettings      *SendBufferSettings
	ReceiveBufferSettings   *ReceiveBufferSettings
	ForwardBufferSettings   *ForwardBufferSettings
	ContractManagerSettings *ContractManagerSettings
	StreamManagerSettings   *StreamManagerSettings
	PeerManagerSettings     *PeerManagerSettings
	WebRtcSettings          *WebRtcSettings
	EncryptionSettings      *EncryptionSettings

	// ProviderStreamPolicy marks a top-level client whose P2P streams exist to
	// serve/relay provider traffic. Its stream manager applies provide-mode
	// reductions to every StreamOpen direction, including return/companion
	// streams left over from an old public contract. Leave false for ordinary
	// destination clients: their destination-only streams are outbound work
	// and must survive their return-traffic provide registration.
	ProviderStreamPolicy bool

	// ClientKeySeed, when set, is the long-lived Ed25519 client identity key
	// seed (`ed25519.NewKeyFromSeed`); must be `ed25519.SeedSize` (32) bytes.
	// When empty, `ClientKeyManager` generates a fresh seed. Persist the running
	// value (`Client.ClientKeyManager().Seed()`) and reload it on the next run
	// to keep the published `ClientKey` (and contract bindings to it) stable
	// across process lifetimes.
	ClientKeySeed []byte

	// Require processed platform registration before ClientKeyManager reports
	// readiness. The real ApiOutOfBandControl returns controller/storage errors;
	// custom delivery-only control implementations must leave this disabled.
	ClientKeyRegistrationRequired bool

	ProtocolVersion int

	DefaultTransferOpts TransferOptions

	// Nil test barrier exposes the exact point where CloseAndWait has closed the
	// client and is about to join its reader tree.
	beforeRunDoneWaitForTest func()
	// Nil test barrier pauses loopback cleanup after delivery but before its
	// SendPack returns message-pool ownership.
	beforeLoopbackReleaseForTest func()
	// Nil test barrier pauses the actual client-key publisher after readiness.
	beforeClientKeyPublishForTest func()
}

// MinimumMessageLenLimit returns the smallest per-transport framer
// `MaxMessageLen` (and receive-side caps, e.g. `websocket.SetReadLimit`) the
// runtime can reliably operate under. Below it, the per-peer handshake can
// deadlock: the TLS server flight ships as one large `EncryptedControl{Handshake}`
// Pack, and if any hop's framer rejects it as oversized the stream closes
// mid-handshake, the retransmit re-sends the same oversized pack, and both sides
// time out.
//
// Full-carrier measurements with the active TLS profile (TLS 1.3,
// X25519MLKEM768 hybrid group, ephemeral ECDSA P-256 cert, mTLS) produce
// 4,946–4,950-byte H1 messages after the encryption and protocol wraps. The
// integrated carrier measurement is the admission baseline; component-only
// TLS estimates do not include every byte emitted by the current sender.
//
// Round up to the existing 8 KiB message-pool class. This leaves more than
// 3 KiB for ASN.1 cert-size jitter, a future larger post-quantum key share, and
// protobuf field-tag drift. Tests and embedded callers should plumb it through
// their framer caps (and matching receive-side limits):
//
//	settings.FramerSettings.MaxMessageLen = max(yourValue, int(client.MinimumMessageLenLimit()))
func (self *ClientSettings) MinimumMessageLenLimit() ByteCount {
	return ByteCount(8 * 1024)
}

// An immutable, lock-free view of receive-pump admission loss. Pack bytes are
// application message bytes, not encoded carrier bytes. Counters are monotonic
// for the lifetime of one Client.
type ClientReceiveStatsSnapshot struct {
	PackHandoffDropCount            uint64
	PackHandoffDropByteCount        uint64
	PackHandoffWaitCount            uint64
	PackHandoffWaitSuccess          uint64
	PackHandoffMaxCount             uint64
	PackHandoffMaxByteCount         uint64
	PackHandoffSaturationCount      uint64
	PackHandoffDepthGrowCount       uint64
	PackHandoffDeepenedFlows        uint64
	PackHandoffAdaptiveMaxDepth     uint64
	PackHandoffAdaptiveMaxByteCount uint64
	// ReceiveQueueDropCount and ReceiveQueueDropByteCount are arrivals the
	// receive queue could not admit: it held ReceiveQueueMaxByteCount and
	// the arrival sat above everything in it, so nothing could be evicted to
	// fit. That is the drop FLIGHTGATEFIX §34.2 identifies as what destroys
	// the acknowledgements a lane proof depends on, and until now it was
	// visible only as a log line. Observation only.
	ReceiveQueueDropCount     uint64
	ReceiveQueueDropByteCount uint64
	// ReceiveQueueEvictionCount and ReceiveQueueEvictionByteCount are items
	// the hold already held and removed to admit an earlier arrival. A drop is
	// an arrival refused and an eviction is a promise withdrawn: the sender was
	// told the item was received, and a selective acknowledgement leases the
	// item for SelectiveAckTimeout rather than releasing it, so until the
	// eviction notice reaches the sender those bytes are invisible to every
	// resend path. Until this counter existed the drop count was the only
	// reading of hold pressure and it is a lower bound (THROUGHPUTFIX §37.16).
	// Under the committed-prefix policy this must stay at zero: an
	// acknowledged item is never discarded, which is the whole of what the
	// policy buys over plain eviction.
	ReceiveQueueEvictionCount     uint64
	ReceiveQueueEvictionByteCount uint64
	// Items evicted while still held tentatively, which the sender was never
	// told had arrived. Not a withdrawal, so it costs the sender's resend
	// rather than a lease, and it is expected to be high against a sender that
	// overruns the hold.
	ReceiveQueueTentativeEvictionCount     uint64
	ReceiveQueueTentativeEvictionByteCount uint64
	// held items that crossed the boundary and were acknowledged
	ReceiveQueueCommitCount uint64
	// no-acknowledgement Packs offered by a caller, handed to the write path,
	// and turned away by admission. Offered above written is the mode being
	// dropped under back pressure.
	SendNoAckOfferedCount uint64
	SendNoAckWriteCount   uint64
	SendNoAckRefusedCount uint64
	// no-acknowledgement packs discarded after admission, on a failed route
	// write or a contract that could not be created
	SendNoAckDiscardCount uint64
	// evictions the notice could not carry, which are the ones that still cost
	// a sixty second lease
	ReceiveQueueEvictionNoticeOverflow uint64
	// items a sender resent because the receiver told it they were evicted
	SendEvictionResendCount    uint64
	AckHandoffDropCount        uint64
	AckHandoffQueueFullCount   uint64
	AckHandoffMissCount        uint64
	AckHandoffWaitCount        uint64
	AckHandoffWaitSuccess      uint64
	AckRouteWriteCount         uint64
	AckRoutePriorityWriteCount uint64
	AckRouteWriteBlockedCount  uint64
	AckRouteWriteErrorCount    uint64
	AckRouteWriteWaitDuration  time.Duration
	AckRouteWriteMaxWait       time.Duration
	// FLIGHTGATEFIX §8: the ack path per carrier the answered Pack arrived on
	// (M2). Only carriers with at least one write appear.
	AckRouteWriteCountByTransport   map[TransportType]uint64
	AckRouteWriteWaitByTransport    map[TransportType]time.Duration
	AckRouteWriteTimeoutByTransport map[TransportType]uint64
}

// ackTransportSlot maps a carrier type to a fixed counter slot so the ack hot
// path indexes an array instead of a map.
const ackTransportSlotCount = 6

func ackTransportSlot(transportType TransportType) int {
	switch transportType {
	case TransportTypeH1:
		return 1
	case TransportTypeH3:
		return 2
	case TransportTypeH3Dns:
		return 3
	case TransportTypeH3DnsPump:
		return 4
	case TransportTypeP2p:
		return 5
	}
	return 0
}

var ackTransportSlotTypes = [ackTransportSlotCount]TransportType{
	TransportTypeUnknown,
	TransportTypeH1,
	TransportTypeH3,
	TransportTypeH3Dns,
	TransportTypeH3DnsPump,
	TransportTypeP2p,
}

func updateAtomicMaximum(target *atomic.Uint64, value uint64) {
	for current := target.Load(); current < value; current = target.Load() {
		if target.CompareAndSwap(current, value) {
			return
		}
	}
}

// An immutable view of Transfer recovery writes and unreliable-carrier flight
// behavior. Counters are monotonic for one Client.
type ClientSendRecoveryStatsSnapshot struct {
	InitialWriteCount            uint64
	InitialFrameCount            uint64
	InitialMessageByteCount      uint64
	TimeoutResendWriteCount      uint64
	AckPendingResendPreemptCount uint64
	// RTO resends of reliable-carried items deferred by one scaled RTT
	// because the cumulative ack was still advancing (FLIGHTGATEFIX §13.5).
	TimeoutResendDeferCount uint64
	CarrierChangeWriteCount uint64
	// selective acknowledgements voided because the route that earned them was
	// retired (THROUGHPUTFIX §37.17 guard two)
	CarrierChangeSelectiveAckVoidCount uint64
	// items resent because the receiver told the sender it had evicted them
	// (THROUGHPUTFIX §37.16)
	SendEvictionResendCount               uint64
	SelectiveGapWriteCount                uint64
	AckTailProbeWriteCount                uint64
	CumulativeProbeWriteCount             uint64
	RecoveryWriteErrorCount               uint64
	MissingContractWriteCount             uint64
	MissingContractRequestCount           uint64
	CompactRecoveryAckCount               uint64
	CompactRecoveryContractCount          uint64
	UnreliableFlowIsolationBypassCount    uint64
	UnreliableNoAckAdmissionBypassCount   uint64
	UnreliableFlowReserveSelectionCount   uint64
	UnreliableFlowReserveUseCount         uint64
	UnreliableFlightWaitCount             uint64
	UnreliableFlightWaitDuration          time.Duration
	UnreliableFlightMaximumWaitDuration   time.Duration
	UnreliableFlightGapCount              uint64
	UnreliableFlightTimeoutCount          uint64
	UnreliableFlightReductionCount        uint64
	UnreliableFlightMaximumByteCount      uint64
	UnreliableFlightMaximumLimitByteCount uint64
	UnreliableFlightMaximumMessageCount   uint64
	UnreliableFlightMaximumMessageLimit   uint64
	// FLIGHTGATEFIX §8. Send-loop iterations that waited on a full unreliable
	// flight while a route on a carrier that is not potentially unreliable had
	// channel capacity: the share of a stall that is the admission gate (M1).
	UnreliableFlightBlockedWithReliableCapacity uint64
	// Selective-gap recoveries whose missing item was acknowledged before the
	// gap resend was written: reordering read as loss (M3).
	UnreliableFlightGapReorderSuspected uint64
	// Ordinary RTO resends issued while the cumulative ack advanced within the
	// last scaled RTT: the spurious whole-window timeout cascade (M4).
	TimeoutResendWithRecentCumulativeProgress uint64
	// ReliableAdmissionWaitCount and ReliableAdmissionWaitDuration are how
	// often and how long a new Pack waited because the reliable lane
	// already held what it had shown it could carry, and
	// ReliableAdmissionByteLimitMinimum is the smallest bound that was
	// applied (FLIGHTGATEFIX §22). All three read zero where the bound is
	// inert, which is how the inertness claim is measured rather than
	// asserted.
	// SelectiveGapWritesOfDeferredItems attributes a scoreboard recovery to
	// the lane that carried the hole, counting only items whose own timeout
	// had already been deferred, indexed by gapHoleCarrier*. Those are the
	// recoveries the deferred retransmit declined to write and the
	// scoreboard wrote instead, which is the residue §23 needs attributed.
	// RouteGenerationChangeCount is how many route generations a sequence
	// saw, read beside the carrier-change writes.
	SelectiveGapWritesOfDeferredItems [gapHoleCarrierCount]uint64
	RouteGenerationChangeCount        uint64
	// LaneProbeWriteCount is how many retransmits were written as a silent
	// reliable lane's single probe, LaneProbeRideCount how many items rode
	// behind one instead of being rewritten themselves, and
	// LaneProvenTimeoutWriteCount how many were written because the item's
	// own route had acknowledged past it, which is an endpoint drop. Probes
	// also count in TimeoutResendWriteCount, so a campaign's total recovery
	// writes sees them (FLIGHTGATEFIX §27.4).
	// LaneHeadPromotionCount is how many times an acknowledgement moved a
	// lane's new oldest unacknowledged item forward to one probe round trip,
	// which is the interval a dropped batch then drains at, one position per
	// promotion (FLIGHTGATEFIX §34.3).
	LaneProbeWriteCount         uint64
	LaneProbeRideCount          uint64
	LaneProvenTimeoutWriteCount uint64
	LaneHeadPromotionCount      uint64
	// The longest stretch a reliable lane went without acknowledging
	// anything, and, at the first timer firing of that stretch, the resend
	// interval the timer read, how many items the sequence held, and how far
	// into the sequence's life the stretch began. These distinguish one
	// campaign seed from another independently of any candidate: §27.1
	// predicts the collapsing seed reads the smallest interval at onset.
	// No behaviour depends on them.
	// RouteUnacknowledgedDuration is how long the route carrying this
	// sequence's oldest outstanding item has gone without acknowledging
	// anything, and RouteRetainedItemCount how many of its items are still
	// held. Together they make the exchange-h3 hang visible in a campaign
	// without a log dive: a route that has acknowledged nothing for the
	// whole of a workload while holding items their flow retains past their
	// acknowledgement timeout. Observation only; no behaviour depends on
	// either, and the decision to retire such a route belongs to the
	// multi-client rather than to the recovery path (FLIGHTGATEFIX §31).
	RouteUnacknowledgedDuration       time.Duration
	RouteRetainedItemCount            uint64
	ReliableLaneLongestAckGap         time.Duration
	ReliableLaneStallOnsetInterval    time.Duration
	ReliableLaneStallOnsetOutstanding uint64
	ReliableLaneStallOnsetOffset      time.Duration
	ReliableAdmissionWaitCount        uint64
	ReliableAdmissionWaitDuration     time.Duration
	ReliableAdmissionByteLimitMinimum uint64
	// Age of the newest acknowledgement of an item carried by an unreliable
	// lane; zero when none was ever acknowledged (feeds a lane watchdog, M6).
	UnreliableCarrierLastAckAge time.Duration
}

// The Transfer endpoint. All callbacks are wrapped to check for nil and
// recover from errors.
type Client struct {
	ctx    context.Context
	cancel context.CancelFunc

	clientId  Id
	clientTag string
	clientOob OutOfBandControl

	log Logger

	settings *ClientSettings

	receiveCallbacks *CallbackList[ReceiveFunction]
	forwardCallbacks *CallbackList[ForwardFunction]
	// subprotocol codecs, raw listeners, pending queries and counters
	// (subprotocol.go); read lock-free by the receive path
	subprotocols *subprotocolRegistry
	// Cached method value used by every ReceivePack. Constructing
	// self.receive at the packet site allocates a closure per inbound pack.
	receiveCallback ReceiveFunction

	loopback chan *SendPack
	// rawSendPacks bounds reuse of the v2 per-packet asynchronous envelope.
	// Objects are allocated lazily, and no more than eight are retained per
	// client after a burst.
	rawSendPacks chan *SendPack
	// Used only when the nil-by-default NoAck observer is configured.
	noAckSendToken atomic.Uint64
	// Used only when the nil-by-default all-Pack observer is configured.
	sendPackLifecycleToken atomic.Uint64
	// The shared receive pump remains nonblocking for H3/unknown. H1 may spend a
	// separately configured bounded wait after a queue fills; primitive counters
	// expose those rare waits and drops without a metrics handoff or hot log.
	receivePackHandoffDropCount            atomic.Uint64
	receivePackHandoffDropByteCount        atomic.Uint64
	receivePackHandoffWaitCount            atomic.Uint64
	receivePackHandoffWaitSuccess          atomic.Uint64
	receivePackHandoffMaxCount             atomic.Uint64
	receivePackHandoffMaxByteCount         atomic.Uint64
	receivePackHandoffSaturationCount      atomic.Uint64
	receivePackHandoffDepthGrowCount       atomic.Uint64
	receivePackHandoffDeepenedFlowCount    atomic.Uint64
	receivePackHandoffAdaptiveMaxDepth     atomic.Uint64
	receivePackHandoffAdaptiveMaxByteCount atomic.Uint64
	receiveQueueDropCount                  atomic.Uint64
	receiveQueueDropByteCount              atomic.Uint64
	receiveQueueEvictionCount              atomic.Uint64
	receiveQueueEvictionByteCount          atomic.Uint64
	receiveQueueTentativeEvictionCount     atomic.Uint64
	receiveQueueTentativeEvictionByteCount atomic.Uint64
	receiveQueueCommitCount                atomic.Uint64
	// A no-acknowledgement Pack asks for delivery out of sequence with no ack
	// and no retry, so it should never be queued behind a retransmit buffer it
	// will never occupy, and never dropped because one is full. Offered counts
	// what a caller handed in, written counts what reached the write path, and
	// refused counts what admission turned away. A forwarder that discards
	// traffic because a reliability buffer it does not use is full would be a
	// defect that looks like ordinary loss, so it needs a counter rather than
	// an inference.
	sendNoAckOfferedCount               atomic.Uint64
	sendNoAckWriteCount                 atomic.Uint64
	sendNoAckRefusedCount               atomic.Uint64
	sendNoAckDiscardCount               atomic.Uint64
	receiveQueueEvictionNoticeOverflow  atomic.Uint64
	sendEvictionResendCount             atomic.Uint64
	receiveAckHandoffDropCount          atomic.Uint64
	receiveAckHandoffQueueFullCount     atomic.Uint64
	receiveAckHandoffMissCount          atomic.Uint64
	receiveAckHandoffWaitCount          atomic.Uint64
	receiveAckHandoffWaitSuccess        atomic.Uint64
	receiveAckRouteWriteCount           atomic.Uint64
	receiveAckRoutePriorityWriteCount   atomic.Uint64
	receiveAckRouteWriteBlockedCount    atomic.Uint64
	receiveAckRouteWriteErrorCount      atomic.Uint64
	receiveAckRouteWriteWaitNanoseconds atomic.Uint64
	receiveAckRouteWriteMaxWaitNanos    atomic.Uint64
	initialSendWriteCount               atomic.Uint64
	initialSendFrameCount               atomic.Uint64
	initialSendMessageByteCount         atomic.Uint64
	selectiveGapWriteCount              atomic.Uint64
	timeoutResendWriteCount             atomic.Uint64
	ackPendingResendPreemptCount        atomic.Uint64
	timeoutResendDeferCount             atomic.Uint64
	carrierChangeWriteCount             atomic.Uint64
	carrierChangeSelectiveAckVoidCount  atomic.Uint64
	ackTailProbeWriteCount              atomic.Uint64
	cumulativeProbeWriteCount           atomic.Uint64
	recoveryWriteErrorCount             atomic.Uint64
	missingContractWriteCount           atomic.Uint64
	missingContractRequestCount         atomic.Uint64
	compactRecoveryAckCount             atomic.Uint64
	compactRecoveryContractCount        atomic.Uint64
	unreliableFlowIsolationBypassCount  atomic.Uint64
	unreliableNoAckAdmissionBypassCount atomic.Uint64
	unreliableFlowReserveSelectionCount atomic.Uint64
	unreliableFlowReserveUseCount       atomic.Uint64
	unreliableFlightWaitCount           atomic.Uint64
	unreliableFlightWaitNanoseconds     atomic.Uint64
	unreliableFlightMaximumWaitNanos    atomic.Uint64
	unreliableFlightGapCount            atomic.Uint64
	unreliableFlightTimeoutCount        atomic.Uint64
	unreliableFlightReductionCount      atomic.Uint64
	unreliableFlightMaximumBytes        atomic.Uint64
	unreliableFlightMaximumLimit        atomic.Uint64
	unreliableFlightMaximumMessages     atomic.Uint64
	unreliableFlightMaximumMessageLimit atomic.Uint64
	// FLIGHTGATEFIX §8: attribution counters. Atomic adds only.
	unreliableFlightBlockedWithReliableCapacity atomic.Uint64
	unreliableFlightGapReorderSuspected         atomic.Uint64
	timeoutResendWithRecentCumulativeProgress   atomic.Uint64
	selectiveGapWritesOfDeferredItems           [gapHoleCarrierCount]atomic.Uint64
	routeGenerationChangeCount                  atomic.Uint64
	laneProbeWriteCount                         atomic.Uint64
	laneProbeRideCount                          atomic.Uint64
	laneHeadPromotionCount                      atomic.Uint64
	laneProvenTimeoutWriteCount                 atomic.Uint64
	routeUnacknowledgedNanos                    atomic.Uint64
	routeRetainedItemCount                      atomic.Uint64
	reliableLaneLongestAckGapNanos              atomic.Uint64
	reliableLaneStallOnsetIntervalNanos         atomic.Uint64
	reliableLaneStallOnsetOutstanding           atomic.Uint64
	reliableLaneStallOnsetOffsetNanos           atomic.Uint64
	reliableAdmissionWaitCount                  atomic.Uint64
	reliableAdmissionWaitNanos                  atomic.Uint64
	reliableAdmissionByteLimitMinimum           atomic.Uint64
	unreliableCarrierLastAckNanos               atomic.Int64
	receiveAckRouteWriteCountByTransport        [ackTransportSlotCount]atomic.Uint64
	receiveAckRouteWriteWaitNanosByTransport    [ackTransportSlotCount]atomic.Uint64
	receiveAckRouteWriteTimeoutByTransport      [ackTransportSlotCount]atomic.Uint64

	routeManager             *RouteManager
	contractManager          *ContractManager
	webRtcManager            *WebRtcManager
	streamManager            *StreamManager
	peerManager              *PeerManager
	sendBuffer               *SendBuffer
	receiveBuffer            *ReceiveBuffer
	forwardBuffer            *ForwardBuffer
	clientKeyManager         *ClientKeyManager
	encryptionSessionManager *EncryptionSessionManager
	signalDispatcher         *clientSignalDispatcher

	// ready is closed by NewClientWithTag right before it returns, once every
	// manager, buffer, callback, and the `run` loop are wired up. See
	// ReadyNotify for the gating contract.
	ready chan struct{}
	// runDone closes after the main reader and both child workers have exited.
	// Close remains non-blocking; CloseAndWait is the opt-in lifecycle join.
	runDone chan struct{}
	// beforeRunDoneWaitForTest is nil outside deterministic lifecycle tests.
	beforeRunDoneWaitForTest func()
	// beforeLoopbackReleaseForTest is nil outside deterministic lifecycle tests.
	beforeLoopbackReleaseForTest func()

	// contractManagerUnsub func()
	webRtcManagerUnsub func()
	streamManagerUnsub func()
	peerManagerUnsub   func()
}

func NewClientWithDefaults(
	ctx context.Context,
	clientId Id,
	clientOob OutOfBandControl,
) *Client {
	return NewClient(
		ctx,
		clientId,
		clientOob,
		DefaultClientSettings(),
	)
}

func NewClient(
	ctx context.Context,
	clientId Id,
	clientOob OutOfBandControl,
	settings *ClientSettings,
) *Client {
	clientTag := clientId.String()
	return NewClientWithTag(ctx, clientId, clientTag, clientOob, settings)
}

func NewClientWithTag(
	ctx context.Context,
	clientId Id,
	clientTag string,
	clientOob OutOfBandControl,
	settings *ClientSettings,
) *Client {
	cancelCtx, cancel := context.WithCancel(ctx)
	log := loggerOrDefault(settings.Log)
	// nested components without a client reference resolve their own settings
	// `Log`. Propagate so a client-level logger covers the entire client
	// tree. Copy instead of writing through the caller's settings: the
	// caller may share them with concurrent client constructions (see the
	// platform transport framer settings for the same rule).
	if settings.WebRtcSettings != nil {
		var p2pSettings *P2pTransportSettings
		if settings.StreamManagerSettings != nil &&
			settings.StreamManagerSettings.StreamBufferSettings != nil {
			p2pSettings = settings.StreamManagerSettings.
				StreamBufferSettings.
				P2pTransportSettings
		}
		legacyOnly := p2pSettings != nil &&
			p2pSettings.DataPlaneMode == P2pDataPlaneModeLegacyOnly
		var dataPlaneStats *P2pDataPlaneStats
		if p2pSettings != nil {
			dataPlaneStats = p2pSettings.DataPlaneStats
		}
		if settings.WebRtcSettings.Log == nil ||
			(legacyOnly && settings.WebRtcSettings.EnableDatagramFastPath) ||
			settings.WebRtcSettings.DataPlaneStats != dataPlaneStats {
			copied := *settings
			webRtcCopied := *copied.WebRtcSettings
			if webRtcCopied.Log == nil {
				webRtcCopied.Log = log
			}
			if legacyOnly {
				// A legacy-only peer must not advertise a lane it will refuse to
				// receive. Otherwise an Auto peer selects fast asymmetrically.
				webRtcCopied.EnableDatagramFastPath = false
			}
			webRtcCopied.DataPlaneStats = dataPlaneStats
			copied.WebRtcSettings = &webRtcCopied
			settings = &copied
		}
	}
	client := &Client{
		ctx:                          cancelCtx,
		cancel:                       cancel,
		clientId:                     clientId,
		clientTag:                    clientTag,
		clientOob:                    clientOob,
		log:                          log,
		settings:                     settings,
		receiveCallbacks:             NewCallbackList[ReceiveFunction](),
		forwardCallbacks:             NewCallbackList[ForwardFunction](),
		subprotocols:                 newSubprotocolRegistry(),
		loopback:                     make(chan *SendPack),
		rawSendPacks:                 make(chan *SendPack, rawSendPackPoolCapacity),
		ready:                        make(chan struct{}),
		runDone:                      make(chan struct{}),
		beforeRunDoneWaitForTest:     settings.beforeRunDoneWaitForTest,
		beforeLoopbackReleaseForTest: settings.beforeLoopbackReleaseForTest,
	}
	client.receiveCallback = client.receive

	// Every manager is owned by this client generation, not by the caller's
	// potentially process-long parent context. Using the parent here let
	// route/contract/stream/peer and ICE work survive Client.Close, overlap a
	// subsequent connect, and produce the macOS reconnect CPU/pause pattern.
	routeManager := NewRouteManagerWithLogger(client.ctx, clientTag, log)
	contractManager := NewContractManager(client.ctx, client, settings.ContractManagerSettings)
	webRtcManager := NewWebRtcManager(client.ctx, NewClientSignalSender(client), settings.WebRtcSettings)
	streamManager := NewStreamManager(client.ctx, client, webRtcManager, settings.StreamManagerSettings)
	peerManager := NewPeerManager(client.ctx, client, settings.PeerManagerSettings)
	// ClientKeyManager must precede EncryptionSessionManager — the latter holds
	// a reference to sign the published TLS cert
	// (`EncryptedKey.ClientKeySignedTlsCertificate`) and per-peer identity proofs.
	clientKeyManager, err := NewClientKeyManager(client.ctx, client)
	if err != nil {
		log.Errorf("[key]%s could not initialize client key: %s\n", client.ClientTag(), err)
		clientKeyManager = nil
	}
	encryptionSessionManager := NewEncryptionSessionManager(client.ctx, client, clientKeyManager, client.settings.EncryptionSettings)

	// client.contractManagerUnsub = client.AddReceiveCallback(contractManager.Receive)
	client.signalDispatcher, client.webRtcManagerUnsub =
		receiveSignalsFromClient(client, webRtcManager)
	client.peerManager = peerManager
	// Peer state must be applied before StreamOpen/StreamReset from the same
	// control batch. A Network-only provider uses that state to distinguish a
	// valid same-network endpoint from stale public provider work.
	client.peerManagerUnsub = client.AddReceiveCallback(peerManager.Receive)
	client.streamManagerUnsub = client.AddReceiveCallback(streamManager.Receive)

	client.initBuffers(routeManager, contractManager, webRtcManager, streamManager, clientKeyManager, encryptionSessionManager)

	go func() {
		defer close(client.runDone)
		HandleError(client.run, cancel)
	}()

	// Mark the client fully constructed: manager goroutines started above (e.g.
	// `publishEncryptedKey`, `providePing`) gate their first send on this so they
	// don't race the wiring above.
	close(client.ready)

	return client
}

// ReadyNotify returns a channel closed once `NewClientWithTag` has finished
// wiring the client (managers, callbacks, buffers, `run` loop). Any goroutine
// launched during construction must wait on it (or `ctx.Done()`) before its
// first send into the client's send path.
func (self *Client) ReadyNotify() <-chan struct{} {
	return self.ready
}

// Log is the logger used by this client and its nested components.
func (self *Client) Log() Logger {
	return self.log
}

func (self *Client) initBuffers(
	routeManager *RouteManager,
	contractManager *ContractManager,
	webRtcManager *WebRtcManager,
	streamManager *StreamManager,
	clientKeyManager *ClientKeyManager,
	encryptionSessionManager *EncryptionSessionManager,
) {
	self.routeManager = routeManager
	self.contractManager = contractManager
	self.webRtcManager = webRtcManager
	self.streamManager = streamManager
	self.clientKeyManager = clientKeyManager
	self.encryptionSessionManager = encryptionSessionManager

	// sendBuffer / receiveBuffer / forwardBuffer come first because
	// `EncryptionSessionManager` publishes its cert (via `EncryptedKey`)
	// at construction time, and the publish path goes through
	// `sendBuffer.Pack`.
	self.sendBuffer = NewSendBuffer(self.ctx, self, self.settings.SendBufferSettings)
	self.receiveBuffer = NewReceiveBuffer(self.ctx, self, self.settings.ReceiveBufferSettings)
	self.forwardBuffer = NewForwardBuffer(self.ctx, self, self.settings.ForwardBufferSettings)
}

func (self *Client) EncryptionSessionManager() *EncryptionSessionManager {
	return self.encryptionSessionManager
}

// unwrapFrame opens an outer-wrapped TransferFrame from `sourceId`. `roleHint`
// (the sender's session role; may be `SequenceRoleUnknown`) selects the
// complement local session to try first; `companionHint` (the sender's session
// companion; nil when the sender omitted it) further pins the exact companion
// session. With a role hint but no companion hint, both companion sessions of
// the complement role are tried; with no role hint, every per-peer session is
// tried. Each candidate session's ciphers are tried (established plus, briefly
// during a rekey, the prior established) until one authenticates. Returns the
// plaintext inner bytes and the local session role and companion that
// decrypted them (used as the receive sequence's role/companion). Wait-free:
// it never blocks the receive loop.
func (self *Client) unwrapFrame(sourceId Id, roleHint protocol.SequenceRole, companionHint *bool, wrapped []byte) ([]byte, sequenceTlsRole, bool, error) {
	if self.encryptionSessionManager == nil {
		return nil, sequenceTlsRoleServer, false, fmt.Errorf("encryption disabled")
	}
	// A wrapped frame can only be opened by the complement of the sender's
	// session role (the other local session is the opposite TLS direction
	// with a different key), so a present role hint narrows us to that role —
	// and a present companion hint pins exactly one session (Option 1). A role
	// hint without a companion hint leaves both companion sessions as
	// candidates. With no role hint — the sender omitted it for on-wire
	// anonymity — trial-decrypt against every per-peer session (Option 2).
	var ordered []*peerEncryptionSession
	if senderRole, ok := sequenceTlsRoleFromProtobuf(roleHint); ok {
		complement := senderRole.complement()
		if companionHint != nil {
			if s := self.encryptionSessionManager.Lookup(sourceId, complement, *companionHint); s != nil {
				ordered = append(ordered, s)
			}
		} else {
			ordered = self.encryptionSessionManager.sessionsForPeerRole(sourceId, complement)
		}
	} else {
		ordered = self.encryptionSessionManager.sessionsForPeer(sourceId)
	}
	if len(ordered) == 0 {
		return nil, sequenceTlsRoleServer, false, fmt.Errorf("no encryption session for peer %s", sourceId)
	}
	for _, session := range ordered {
		for _, cipher := range session.decryptCiphers() {
			if plaintext, err := cipher.Open(wrapped); err == nil {
				return plaintext, session.role, session.companion, nil
			}
		}
	}
	return nil, sequenceTlsRoleServer, false, fmt.Errorf("no encryption session for peer %s could decrypt", sourceId)
}

func (self *Client) ClientKeyManager() *ClientKeyManager {
	return self.clientKeyManager
}

func (self *Client) RouteManager() *RouteManager {
	return self.routeManager
}

func (self *Client) ContractManager() *ContractManager {
	return self.contractManager
}

func (self *Client) ClientId() Id {
	return self.clientId
}

func (self *Client) ClientTag() string {
	return self.clientTag
}

// Reads independently updated counters without stopping receive processing.
// A snapshot taken during traffic may straddle one Pack update, so byte and
// message counts are consistent-enough telemetry rather than a transaction.
func (self *Client) ReceiveStats() ClientReceiveStatsSnapshot {
	snapshot := ClientReceiveStatsSnapshot{
		PackHandoffDropCount:                   self.receivePackHandoffDropCount.Load(),
		PackHandoffDropByteCount:               self.receivePackHandoffDropByteCount.Load(),
		PackHandoffWaitCount:                   self.receivePackHandoffWaitCount.Load(),
		PackHandoffWaitSuccess:                 self.receivePackHandoffWaitSuccess.Load(),
		PackHandoffMaxCount:                    self.receivePackHandoffMaxCount.Load(),
		PackHandoffMaxByteCount:                self.receivePackHandoffMaxByteCount.Load(),
		PackHandoffSaturationCount:             self.receivePackHandoffSaturationCount.Load(),
		PackHandoffDepthGrowCount:              self.receivePackHandoffDepthGrowCount.Load(),
		PackHandoffDeepenedFlows:               self.receivePackHandoffDeepenedFlowCount.Load(),
		PackHandoffAdaptiveMaxDepth:            self.receivePackHandoffAdaptiveMaxDepth.Load(),
		PackHandoffAdaptiveMaxByteCount:        self.receivePackHandoffAdaptiveMaxByteCount.Load(),
		ReceiveQueueDropCount:                  self.receiveQueueDropCount.Load(),
		ReceiveQueueDropByteCount:              self.receiveQueueDropByteCount.Load(),
		ReceiveQueueEvictionCount:              self.receiveQueueEvictionCount.Load(),
		ReceiveQueueTentativeEvictionCount:     self.receiveQueueTentativeEvictionCount.Load(),
		ReceiveQueueTentativeEvictionByteCount: self.receiveQueueTentativeEvictionByteCount.Load(),
		ReceiveQueueCommitCount:                self.receiveQueueCommitCount.Load(),
		SendNoAckOfferedCount:                  self.sendNoAckOfferedCount.Load(),
		SendNoAckWriteCount:                    self.sendNoAckWriteCount.Load(),
		SendNoAckRefusedCount:                  self.sendNoAckRefusedCount.Load(),
		SendNoAckDiscardCount:                  self.sendNoAckDiscardCount.Load(),
		ReceiveQueueEvictionByteCount:          self.receiveQueueEvictionByteCount.Load(),
		ReceiveQueueEvictionNoticeOverflow:     self.receiveQueueEvictionNoticeOverflow.Load(),
		AckHandoffDropCount:                    self.receiveAckHandoffDropCount.Load(),
		AckHandoffQueueFullCount:               self.receiveAckHandoffQueueFullCount.Load(),
		AckHandoffMissCount:                    self.receiveAckHandoffMissCount.Load(),
		AckHandoffWaitCount:                    self.receiveAckHandoffWaitCount.Load(),
		AckHandoffWaitSuccess:                  self.receiveAckHandoffWaitSuccess.Load(),
		AckRouteWriteCount:                     self.receiveAckRouteWriteCount.Load(),
		AckRoutePriorityWriteCount:             self.receiveAckRoutePriorityWriteCount.Load(),
		AckRouteWriteBlockedCount:              self.receiveAckRouteWriteBlockedCount.Load(),
		AckRouteWriteErrorCount:                self.receiveAckRouteWriteErrorCount.Load(),
		AckRouteWriteWaitDuration: time.Duration(
			self.receiveAckRouteWriteWaitNanoseconds.Load(),
		),
		AckRouteWriteMaxWait: time.Duration(
			self.receiveAckRouteWriteMaxWaitNanos.Load(),
		),
	}
	// The per-carrier maps exist only when a carrier has written: a client
	// that has not acked stays allocation-free, which the multi-client memory
	// snapshot relies on.
	for slot, transportType := range ackTransportSlotTypes {
		if count := self.receiveAckRouteWriteCountByTransport[slot].Load(); 0 < count {
			if snapshot.AckRouteWriteCountByTransport == nil {
				snapshot.AckRouteWriteCountByTransport = map[TransportType]uint64{}
				snapshot.AckRouteWriteWaitByTransport = map[TransportType]time.Duration{}
				snapshot.AckRouteWriteTimeoutByTransport = map[TransportType]uint64{}
			}
			snapshot.AckRouteWriteCountByTransport[transportType] = count
			snapshot.AckRouteWriteWaitByTransport[transportType] = time.Duration(
				self.receiveAckRouteWriteWaitNanosByTransport[slot].Load(),
			)
			snapshot.AckRouteWriteTimeoutByTransport[transportType] =
				self.receiveAckRouteWriteTimeoutByTransport[slot].Load()
		}
	}
	return snapshot
}

// Reads recovery-write counters without stopping send processing.
func (self *Client) SendRecoveryStats() ClientSendRecoveryStatsSnapshot {
	return ClientSendRecoveryStatsSnapshot{
		InitialWriteCount:                   self.initialSendWriteCount.Load(),
		InitialFrameCount:                   self.initialSendFrameCount.Load(),
		InitialMessageByteCount:             self.initialSendMessageByteCount.Load(),
		TimeoutResendWriteCount:             self.timeoutResendWriteCount.Load(),
		AckPendingResendPreemptCount:        self.ackPendingResendPreemptCount.Load(),
		TimeoutResendDeferCount:             self.timeoutResendDeferCount.Load(),
		CarrierChangeWriteCount:             self.carrierChangeWriteCount.Load(),
		CarrierChangeSelectiveAckVoidCount:  self.carrierChangeSelectiveAckVoidCount.Load(),
		SendEvictionResendCount:             self.sendEvictionResendCount.Load(),
		SelectiveGapWriteCount:              self.selectiveGapWriteCount.Load(),
		AckTailProbeWriteCount:              self.ackTailProbeWriteCount.Load(),
		CumulativeProbeWriteCount:           self.cumulativeProbeWriteCount.Load(),
		RecoveryWriteErrorCount:             self.recoveryWriteErrorCount.Load(),
		MissingContractWriteCount:           self.missingContractWriteCount.Load(),
		MissingContractRequestCount:         self.missingContractRequestCount.Load(),
		CompactRecoveryAckCount:             self.compactRecoveryAckCount.Load(),
		CompactRecoveryContractCount:        self.compactRecoveryContractCount.Load(),
		UnreliableFlowIsolationBypassCount:  self.unreliableFlowIsolationBypassCount.Load(),
		UnreliableNoAckAdmissionBypassCount: self.unreliableNoAckAdmissionBypassCount.Load(),
		UnreliableFlowReserveSelectionCount: self.unreliableFlowReserveSelectionCount.Load(),
		UnreliableFlowReserveUseCount:       self.unreliableFlowReserveUseCount.Load(),
		UnreliableFlightWaitCount:           self.unreliableFlightWaitCount.Load(),
		UnreliableFlightWaitDuration: time.Duration(
			self.unreliableFlightWaitNanoseconds.Load(),
		),
		UnreliableFlightMaximumWaitDuration: time.Duration(
			self.unreliableFlightMaximumWaitNanos.Load(),
		),
		UnreliableFlightGapCount:                    self.unreliableFlightGapCount.Load(),
		UnreliableFlightTimeoutCount:                self.unreliableFlightTimeoutCount.Load(),
		UnreliableFlightReductionCount:              self.unreliableFlightReductionCount.Load(),
		UnreliableFlightMaximumByteCount:            self.unreliableFlightMaximumBytes.Load(),
		UnreliableFlightMaximumLimitByteCount:       self.unreliableFlightMaximumLimit.Load(),
		UnreliableFlightMaximumMessageCount:         self.unreliableFlightMaximumMessages.Load(),
		UnreliableFlightMaximumMessageLimit:         self.unreliableFlightMaximumMessageLimit.Load(),
		UnreliableFlightBlockedWithReliableCapacity: self.unreliableFlightBlockedWithReliableCapacity.Load(),
		UnreliableFlightGapReorderSuspected:         self.unreliableFlightGapReorderSuspected.Load(),
		TimeoutResendWithRecentCumulativeProgress:   self.timeoutResendWithRecentCumulativeProgress.Load(),
		SelectiveGapWritesOfDeferredItems: [gapHoleCarrierCount]uint64{
			self.selectiveGapWritesOfDeferredItems[gapHoleCarrierReliable].Load(),
			self.selectiveGapWritesOfDeferredItems[gapHoleCarrierUnreliable].Load(),
		},
		RouteGenerationChangeCount:  self.routeGenerationChangeCount.Load(),
		LaneProbeWriteCount:         self.laneProbeWriteCount.Load(),
		LaneProbeRideCount:          self.laneProbeRideCount.Load(),
		LaneHeadPromotionCount:      self.laneHeadPromotionCount.Load(),
		LaneProvenTimeoutWriteCount: self.laneProvenTimeoutWriteCount.Load(),
		RouteUnacknowledgedDuration: time.Duration(self.routeUnacknowledgedNanos.Load()),
		RouteRetainedItemCount:      self.routeRetainedItemCount.Load(),
		ReliableLaneLongestAckGap: time.Duration(
			self.reliableLaneLongestAckGapNanos.Load()),
		ReliableLaneStallOnsetInterval: time.Duration(
			self.reliableLaneStallOnsetIntervalNanos.Load()),
		ReliableLaneStallOnsetOutstanding: self.reliableLaneStallOnsetOutstanding.Load(),
		ReliableLaneStallOnsetOffset: time.Duration(
			self.reliableLaneStallOnsetOffsetNanos.Load()),
		ReliableAdmissionWaitCount:        self.reliableAdmissionWaitCount.Load(),
		ReliableAdmissionWaitDuration:     time.Duration(self.reliableAdmissionWaitNanos.Load()),
		ReliableAdmissionByteLimitMinimum: self.reliableAdmissionByteLimitMinimum.Load(),
		UnreliableCarrierLastAckAge:       self.unreliableCarrierLastAckAge(),
	}
}

func (self *Client) unreliableCarrierLastAckAge() time.Duration {
	nanos := self.unreliableCarrierLastAckNanos.Load()
	if nanos == 0 {
		return 0
	}
	return time.Since(time.Unix(0, nanos))
}

func (self *Client) observeUnreliableFlightWait(waitDuration time.Duration) {
	if waitDuration <= 0 {
		return
	}
	waitNanoseconds := uint64(waitDuration)
	self.unreliableFlightWaitNanoseconds.Add(waitNanoseconds)
	for maximumWait := self.unreliableFlightMaximumWaitNanos.Load(); maximumWait < waitNanoseconds &&
		!self.unreliableFlightMaximumWaitNanos.CompareAndSwap(maximumWait, waitNanoseconds); maximumWait = self.unreliableFlightMaximumWaitNanos.Load() {
	}
}

// Samples bounded flight state only at send, acknowledgement, and policy
// transitions. Atomics keep PERFVAR observation independent of sequence work.
func (self *Client) observeUnreliableFlight(controller *sendFlightController) {
	if !controller.limited {
		return
	}
	byteCount := uint64(max(controller.byteCount, 0))
	for maximumByteCount := self.unreliableFlightMaximumBytes.Load(); maximumByteCount < byteCount &&
		!self.unreliableFlightMaximumBytes.CompareAndSwap(maximumByteCount, byteCount); maximumByteCount = self.unreliableFlightMaximumBytes.Load() {
	}
	byteLimit := uint64(max(controller.byteLimit, 0))
	for maximumByteLimit := self.unreliableFlightMaximumLimit.Load(); maximumByteLimit < byteLimit &&
		!self.unreliableFlightMaximumLimit.CompareAndSwap(maximumByteLimit, byteLimit); maximumByteLimit = self.unreliableFlightMaximumLimit.Load() {
	}
	messageCount := uint64(max(controller.messageCount, 0))
	for maximumMessageCount := self.unreliableFlightMaximumMessages.Load(); maximumMessageCount < messageCount &&
		!self.unreliableFlightMaximumMessages.CompareAndSwap(maximumMessageCount, messageCount); maximumMessageCount = self.unreliableFlightMaximumMessages.Load() {
	}
	messageLimit := uint64(max(controller.messageLimit, 0))
	for maximumMessageLimit := self.unreliableFlightMaximumMessageLimit.Load(); maximumMessageLimit < messageLimit &&
		!self.unreliableFlightMaximumMessageLimit.CompareAndSwap(maximumMessageLimit, messageLimit); maximumMessageLimit = self.unreliableFlightMaximumMessageLimit.Load() {
	}
}

// Records one physical recovery attempt after it leaves the resend queue. A
// route refusal is retained separately: attempting a write is not proof that
// the carrier admitted the recovery frame.
// deferredResendInterval is how long a deferral waits. It backs off like
// the rewrite it replaces (FLIGHTGATEFIX §24.3): a deferral that re-arms at
// the same scaled round trip and leaves sendCount alone lets a window
// stalled mid-transfer re-fire whole every round trip, and the deferral
// limit then writes the third firing of every item into the stall, where
// merged's rewrite doubles its interval each attempt and backs off.
// §13.5's branch reads it before the deferral count is advanced, so an
// item's first deferral is one interval as it has always been and its
// second is twice that. The lane rule's branch (§32.4) reads it after, so
// a deferred timer keeps the schedule the rewrite's own timer runs, due
// at one, three, seven intervals: the rewrite counts its write first and
// then reads the interval, and the re-arm is that timer without the
// write.
func (self *SendSequence) deferredResendInterval(
	item *sendItem,
	scaledRtt time.Duration,
) time.Duration {
	if !self.sendBufferSettings.DeferTimeoutResendBackoff {
		return scaledRtt
	}
	return self.resendIntervalForItem(item, item.sendCount+item.timeoutDeferCount)
}

// observeReliableLaneFiring takes the reading §27.1 asks for at the first
// timer firing of a reliable lane's current silence: the interval the timer
// read, the items the sequence held, and how far into its life the silence
// began. No behaviour depends on it.
func (self *SendSequence) observeReliableLaneFiring(
	item *sendItem,
	interval time.Duration,
	now time.Time,
) {
	if item == nil || !item.reliableCarrierObserved || item.unreliableFlightTracked {
		return
	}
	if self.lastReliableAckTime.IsZero() || self.stallOnsetTaken {
		return
	}
	self.stallOnsetTaken = true
	self.stallOnsetInterval = interval
	self.stallOnsetOutstanding = len(self.sendItems)
	self.stallOnsetOffset = now.Sub(self.sequenceStartTime)
}

// observeReliableLaneAck ends the current silence and commits its reading
// if it was the longest of the run.
func (self *SendSequence) observeReliableLaneAck(item *sendItem, at time.Time) {
	if item == nil || !item.reliableCarrierObserved || item.unreliableFlightTracked {
		return
	}
	if !self.lastReliableAckTime.IsZero() {
		if gap := at.Sub(self.lastReliableAckTime); 0 < gap &&
			time.Duration(self.client.reliableLaneLongestAckGapNanos.Load()) < gap {
			self.client.reliableLaneLongestAckGapNanos.Store(uint64(gap))
			if self.stallOnsetTaken {
				self.client.reliableLaneStallOnsetIntervalNanos.Store(
					uint64(self.stallOnsetInterval))
				self.client.reliableLaneStallOnsetOutstanding.Store(
					uint64(self.stallOnsetOutstanding))
				self.client.reliableLaneStallOnsetOffsetNanos.Store(
					uint64(self.stallOnsetOffset))
			}
		}
	}
	self.lastReliableAckTime = at
	self.stallOnsetTaken = false
}

// laneAckSlotCount bounds the per-route acknowledgement table. A route
// snapshot publishes a handful of carriers, and a linear scan of eight is
// cheaper than any map and allocates nothing (FLIGHTGATEFIX §26.2).
const laneAckSlotCount = 8

type laneAckSlot struct {
	route                      Route
	highestAckedSequenceNumber uint64
	// highestSentSequenceNumber is the newest item this route has carried,
	// which with the oldest-outstanding test identifies a lane's one tail
	// (FLIGHTGATEFIX §29.3).
	highestSentSequenceNumber uint64
	// lastAckNanos is when this route last acknowledged anything. It, and
	// not the sequence's cumulative clock, decides whether the route is
	// draining: on a mixed route a relay whose head is stuck while
	// direct-lane acknowledgements keep the cumulative ack moving must read
	// as silent and be probed, not deferred to its limit and written
	// (FLIGHTGATEFIX §27.3).
	lastAckNanos int64
	set          bool
}

// resetLaneAcks forgets every route's acknowledgement history, which a
// route generation change requires: the same channel may not be the same
// carrier, and a retired carrier's recovery is
// scheduleRetiredReliableCarrierRecovery's business.
func (self *SendSequence) resetLaneAcks(generation uint64) {
	for index := range self.laneAcks {
		self.laneAcks[index] = laneAckSlot{}
	}
	self.laneAckGeneration = generation
}

// observeLaneAck records that this item's route has acknowledged up to at
// least this sequence number.
func (self *SendSequence) observeLaneAck(item *sendItem, at time.Time) {
	if item == nil || item.carrierRoute == nil || item.carrierChanged {
		// an item that has been on more than one lane is acknowledged by
		// whichever copy arrived, which says nothing about its last lane
		return
	}
	free := -1
	for index := range self.laneAcks {
		slot := &self.laneAcks[index]
		if !slot.set {
			if free < 0 {
				free = index
			}
			continue
		}
		if slot.route == item.carrierRoute {
			if slot.highestAckedSequenceNumber < item.sequenceNumber {
				slot.highestAckedSequenceNumber = item.sequenceNumber
			}
			slot.lastAckNanos = at.UnixNano()
			return
		}
	}
	if 0 <= free {
		self.laneAcks[free] = laneAckSlot{
			route:                      item.carrierRoute,
			highestAckedSequenceNumber: item.sequenceNumber,
			lastAckNanos:               at.UnixNano(),
			set:                        true,
		}
	}
}

// laneSlotFor finds or assigns this route's slot, or -1 when the table is
// full, which a snapshot with more than laneAckSlotCount carriers would
// reach; the rule then falls back to counting every later acknowledgement,
// which is merged's behaviour.
func (self *SendSequence) laneSlotFor(route Route) int {
	if route == nil {
		return -1
	}
	free := -1
	for index := range self.laneAcks {
		slot := &self.laneAcks[index]
		if !slot.set {
			if free < 0 {
				free = index
			}
			continue
		}
		if slot.route == route {
			return index
		}
	}
	if 0 <= free {
		self.laneAcks[free] = laneAckSlot{route: route, set: true}
		return free
	}
	return -1
}

// laneHighestAcked reports the highest sequence number this route has
// acknowledged, and whether it has acknowledged anything at all.
func (self *SendSequence) laneHighestAcked(route Route) (uint64, bool) {
	if route == nil {
		return 0, false
	}
	for index := range self.laneAcks {
		slot := &self.laneAcks[index]
		if slot.set && slot.route == route {
			return slot.highestAckedSequenceNumber, true
		}
	}
	return 0, false
}

// observeLaneSend records that this route has now carried up to at least
// this sequence number, which with the oldest-outstanding test identifies
// a lane's one tail (FLIGHTGATEFIX §29.3).
func (self *SendSequence) observeLaneSend(item *sendItem) {
	if item == nil || item.carrierRoute == nil {
		return
	}
	if slot := self.laneSlotFor(item.carrierRoute); 0 <= slot {
		if self.laneAcks[slot].highestSentSequenceNumber < item.sequenceNumber {
			self.laneAcks[slot].highestSentSequenceNumber = item.sequenceNumber
		}
	}
}

// laneHighestSent reports the newest sequence number this route carried.
func (self *SendSequence) laneHighestSent(route Route) (uint64, bool) {
	if route == nil {
		return 0, false
	}
	for index := range self.laneAcks {
		slot := &self.laneAcks[index]
		if slot.set && slot.route == route {
			return slot.highestSentSequenceNumber, true
		}
	}
	return 0, false
}

// laneLoneTail reports whether this item is the only thing outstanding on
// its route: the oldest outstanding on it and the newest it carried. A
// lane has exactly one such item, so a probe conditioned on it cannot
// storm however deep the window (FLIGHTGATEFIX §29.3).
func (self *SendSequence) laneLoneTail(item *sendItem) bool {
	if !self.laneProvenRecovery(item) {
		return false
	}
	if highest, ok := self.laneHighestSent(item.carrierRoute); !ok ||
		highest != item.sequenceNumber {
		return false
	}
	return self.laneOldestOutstanding(item.carrierRoute) == item
}

// observeRouteStall records how long the route carrying the oldest
// outstanding item has gone without acknowledging anything, and how many of
// its items are still held. Observation only (FLIGHTGATEFIX §31.3).
func (self *SendSequence) observeRouteStall(now time.Time) {
	var oldest *sendItem
	for _, item := range self.sendItems {
		if item == nil || item.selectiveAcked || item.carrierRoute == nil {
			continue
		}
		oldest = item
		break
	}
	if oldest == nil {
		return
	}
	retained := 0
	for _, item := range self.sendItems {
		if item != nil && item.carrierRoute == oldest.carrierRoute {
			retained += 1
		}
	}
	unacknowledged := now.Sub(oldest.sendTime)
	if lastAck, ok := self.laneLastAck(oldest.carrierRoute); ok {
		unacknowledged = now.Sub(lastAck)
	}
	if unacknowledged <= 0 {
		return
	}
	for {
		current := self.client.routeUnacknowledgedNanos.Load()
		if uint64(unacknowledged) <= current {
			break
		}
		if self.client.routeUnacknowledgedNanos.CompareAndSwap(current, uint64(unacknowledged)) {
			self.client.routeRetainedItemCount.Store(uint64(retained))
			break
		}
	}
}

// laneLastAck reports when this route last acknowledged anything.
func (self *SendSequence) laneLastAck(route Route) (time.Time, bool) {
	if route == nil {
		return time.Time{}, false
	}
	for index := range self.laneAcks {
		slot := &self.laneAcks[index]
		if slot.set && slot.route == route && slot.lastAckNanos != 0 {
			return time.Unix(0, slot.lastAckNanos), true
		}
	}
	return time.Time{}, false
}

// laneTimerVerdict is what a reliable-carried item's timer firing means,
// read from its own route's clocks (FLIGHTGATEFIX §32.4).
type laneTimerVerdict int

const (
	// the rule is off, or the item is not reliable-carried
	laneTimerNotApplicable laneTimerVerdict = iota
	// the route delivered something sent after this item, so this item was
	// dropped at an endpoint
	laneTimerEndpointDrop
	// something below this item was acknowledged on its own lane since this
	// item last looked: on a FIFO lane the lane is draining toward it and it
	// is next, so re-arm with backoff
	laneTimerDraining
	// nothing on this item's lane has moved since it last looked: if it is
	// the lane's oldest unacknowledged item it is written, otherwise it
	// rides that head
	laneTimerSilent
)

// laneTimerVerdictFor reads this item's own lane and its position in it.
//
// §32 established that an alive-but-slow lane and a stalled one are not
// separable by any test on a gap: the scaled round trip, the item's own
// interval and progress since the last deferral each reproduced the failure
// at a queue step larger than the threshold. §34.2 then showed why the
// answer that replaced them, re-arming any firing that no later same-lane
// acknowledgement had proven, wedges. That proof is a chain: a later
// same-lane item has to exist, be delivered, and have its acknowledgement
// credited to this lane. The receiver's own queue drops break every link at
// once, because an arrival above a hole it cannot queue is dropped
// unacknowledged, so an item whose successors were all dropped is deferred
// forever with nothing but silence to read.
//
// §34.3 keeps the estimate as the pacer and decides by position, which no
// drop can destroy. It is the item's own lane throughout, never the route as
// a whole and never a window: any acknowledgement anywhere on a busy route
// satisfies a route-level test while this item's own position goes
// unexamined, which is the same error in a new place.
func (self *SendSequence) laneTimerVerdictFor(item *sendItem) laneTimerVerdict {
	if !self.laneProvenRecovery(item) {
		return laneTimerNotApplicable
	}
	highest, acked := self.laneHighestAcked(item.carrierRoute)

	// 1. anything acknowledged above this item, ever. Either it was
	// delivered and its own acknowledgement was lost, or it was dropped;
	// either way it is written, a duplicate into a live lane at worst.
	if acked && item.sequenceNumber < highest {
		return laneTimerEndpointDrop
	}

	// 2. anything acknowledged below it since it last looked. On a FIFO lane
	// the lane is draining toward it and it is next, so it waits. This is a
	// fact about position rather than a test on a gap: a lane whose
	// acknowledgements are arbitrarily slow still satisfies it, which is why
	// it holds where every threshold failed. An item that has never looked
	// has everything to look at, so its first firing on a lane that has
	// acknowledged anything is one free deferral.
	if acked && item.laneAckedAtLastFiring < highest {
		return laneTimerDraining
	}

	// 3. nothing on this lane has moved since it last looked.
	return laneTimerSilent
}

// laneProvenRecovery reports whether this sequence reads a reliable lane's
// silence by lane. A hole the direct lane carried keeps merged's rule
// either way, since that lane does not retransmit below Transfer.
func (self *SendSequence) laneProvenRecovery(item *sendItem) bool {
	return self.sendBufferSettings.ReliableLaneProvenRecovery &&
		item != nil && item.carrierRoute != nil && !item.carrierChanged &&
		item.reliableCarrierObserved && !item.unreliableFlightTracked
}

// laneOldestOutstanding reports the oldest item still awaiting
// acknowledgement on this route, which is the only item a silent lane
// probes.
func (self *SendSequence) laneOldestOutstanding(route Route) *sendItem {
	for _, item := range self.sendItems {
		if item == nil || item.selectiveAcked || item.carrierChanged {
			continue
		}
		if item.carrierRoute == route {
			return item
		}
	}
	return nil
}

// promoteLaneHeads brings the new oldest unacknowledged item of each named
// lane forward to one probe round trip from now, and tells it where its lane
// has got to (FLIGHTGATEFIX §34.3).
//
// The two halves are one act: the item is told what the lane has
// acknowledged and asked to look again in a round trip. If nothing further
// moves in that round trip, rule 2 no longer holds and rule 3 writes it,
// because it is its lane's oldest unacknowledged item. So a batch the
// receiver dropped drains at one round trip per item: each write is
// acknowledged, and that acknowledgement promotes the next position. A lane
// that is really draining answers within the round trip instead, and the
// item is acknowledged and gone before its promoted firing arrives, so the
// drain writes nothing.
//
// Only an acknowledgement of a retransmit promotes. That is what separates a
// batch being dragged forward one write at a time from a lane making its own
// forward progress: in the first the acknowledgement exists because the
// sender wrote the position again, and the next position needs the same
// treatment; in the second the lane delivered on the first write and the
// next position is owed rule 2's free deferral, which is M4's profile and
// where promoting regardless writes an item on a lane that is answering. The
// test is the acknowledged item's send count, which counts its own timer's
// writes; a position the scoreboard recovered instead is not promoted, and
// does not need to be, since the scoreboard writes every position it proves
// in one round.
//
// The firing is set to the round trip in both directions, never only pulled
// in. A new lane head has usually been riding the old one and carries that
// head's firing time, which is stale the moment the head is acknowledged:
// left alone it fires almost at once, is given rule 2's free deferral for
// an acknowledgement it has just been shown, and waits out a doubled
// backoff. Measured on a drain of seven dropped positions that alternated,
// a round trip for one position and up to eight seconds for the next.
// Setting the firing outright is also what makes the drain write nothing:
// a lane delivering faster than a round trip acknowledges the head before
// the firing it was given.
//
// The probe round trip is the scaled minimum rather than the mean: this is
// the interval in which a live lane must answer, and one deep queue must not
// turn it into the multi-second retransmit timeout it precedes.
func (self *SendSequence) promoteLaneHeads(lanes uint32, at time.Time) {
	promoteTime := at.Add(self.rttWindow.probeRtt(at))
	for index := range self.laneAcks {
		if lanes&(uint32(1)<<uint(index)) == 0 {
			continue
		}
		slot := &self.laneAcks[index]
		head := self.laneOldestOutstanding(slot.route)
		if head == nil || !self.laneProvenRecovery(head) {
			continue
		}
		if self.resendQueue.RemoveByMessageId(head.messageId) == nil {
			continue
		}
		head.laneAckedAtLastFiring = slot.highestAckedSequenceNumber
		head.resendTime = promoteTime
		self.resendQueue.Add(head)
		self.client.laneHeadPromotionCount.Add(1)
	}
}

// gapHoleCarrier indexes a recovery counter by the lane that carried the
// hole being recovered (FLIGHTGATEFIX §23.3).
type gapHoleCarrier int

const (
	gapHoleCarrierReliable gapHoleCarrier = iota
	gapHoleCarrierUnreliable
	gapHoleCarrierCount
)

// gapHoleCarrierOf reports which lane carried this item, for attribution.
func gapHoleCarrierOf(item *sendItem) gapHoleCarrier {
	if item != nil && item.unreliableCarrierObserved {
		return gapHoleCarrierUnreliable
	}
	return gapHoleCarrierReliable
}

// shouldDeferTimeoutResend reports whether a due timeout is the spurious
// cascade of §13.5 (F12) rather than a stalled lane: the cumulative ack
// advanced within one scaled round trip of this item's send, so the
// reliable lane is alive and its queue is deeper than the estimate, and
// waiting one more round trip is right. Each further deferral needs the
// cumulative ack to have advanced since the last one, so a hole nothing
// can acknowledge is deferred once and then retransmitted.
//
// D5 (FLIGHTGATEFIX §19): the direct lane's latch is not a term here. It
// drives the scoreboard's conclusiveness and nothing else; withholding
// this deferral while the lane was latched is what let the relay's
// spurious timeouts storm onto the direct lane.
func (self *SendSequence) shouldDeferTimeoutResend(
	item *sendItem,
	scaledRtt time.Duration,
) bool {
	return self.sendBufferSettings.DeferTimeoutResendWhileCumulativeProgress &&
		!item.unreliableCarrierObserved &&
		item.timeoutDeferCount < self.sendBufferSettings.TimeoutResendDeferLimit &&
		self.lastCumulativeAckTime.After(item.sendTime.Add(-scaledRtt)) &&
		self.lastCumulativeAckTime.After(item.timeoutDeferAckTime)
}

func (self *Client) recordSendRecovery(recoveryKind sendRecoveryKind, writeErr error) {
	switch recoveryKind {
	case sendRecoveryNone:
		self.timeoutResendWriteCount.Add(1)
	case sendRecoveryCarrierChange:
		self.carrierChangeWriteCount.Add(1)
	case sendRecoverySelectiveGap:
		self.selectiveGapWriteCount.Add(1)
	case sendRecoveryAckTailProbe:
		self.ackTailProbeWriteCount.Add(1)
	case sendRecoveryCumulativeProbe:
		self.cumulativeProbeWriteCount.Add(1)
	case sendRecoveryContractMissing:
		self.missingContractWriteCount.Add(1)
	case sendRecoveryEviction:
		self.sendEvictionResendCount.Add(1)
	}
	if writeErr != nil {
		self.recoveryWriteErrorCount.Add(1)
	}
}

func (self *Client) ClientOob() OutOfBandControl {
	return self.clientOob
}

// a peer of this client on the network
type NetworkPeer struct {
	ClientId Id
	// the peer's enabled provide modes
	ProvideModes []protocol.ProvideMode
	// whether the peer has the network provide mode enabled
	ProvideEnabled bool
	Principal      string
	Roles          []string
	DeviceSpec     string
	DeviceName     string
}

func (self *Client) PeerManager() *PeerManager {
	return self.peerManager
}

// NetworkPeers enumerates the connected peers and the count of
// recently disconnected peers.
// The platform announces peers only to top-level clients;
// all other clients have no network peers.
func (self *Client) NetworkPeers() (connected []*NetworkPeer, disconnectedCount int) {
	return self.peerManager.NetworkPeers()
}

func (self *Client) ReportAbuse(source TransferPath) {
	peerAudit := NewSequencePeerAudit(self, source, 0)
	peerAudit.Update(func(peerAudit *PeerAudit) {
		peerAudit.Abuse = true
	})
	peerAudit.Complete()
}

func (self *Client) ForwardWithTimeout(transferFrameBytes []byte, timeout time.Duration, opts ...any) bool {
	success, err := self.ForwardWithTimeoutDetailed(transferFrameBytes, timeout, opts...)
	return success && err == nil
}

func (self *Client) ForwardWithTimeoutDetailed(transferFrameBytes []byte, timeout time.Duration, opts ...any) (bool, error) {
	select {
	case <-self.ctx.Done():
		return false, errors.New("Done")
	default:
	}

	path, err := FilteredTransferPath(transferFrameBytes)
	if err != nil {
		// bad protobuf
		return false, err
	}

	destination := path.DestinationMask()

	ctx := self.ctx
	for _, opt := range opts {
		switch v := opt.(type) {
		case transferCtx:
			ctx = v.Ctx
		}
	}

	forwardPack := &ForwardPack{
		Destination:        destination,
		TransferFrameBytes: transferFrameBytes,
		Ctx:                ctx,
	}

	return self.forwardBuffer.Pack(forwardPack, timeout)
}

func (self *Client) Forward(transferFrameBytes []byte, opts ...any) bool {
	return self.ForwardWithTimeout(transferFrameBytes, -1, opts...)
}

// Enqueues one direct frame within a bounded wait. A TransferKey option
// reproduces a received lane and encryption session on the reply.
func (self *Client) SendWithTimeout(
	frame *protocol.Frame,
	destinationId Id,
	ackCallback AckFunction,
	timeout time.Duration,
	opts ...any,
) bool {
	success, err := self.SendWithTimeoutDetailed(frame, destinationId, ackCallback, timeout, opts...)
	return success && err == nil
}

// Returns the enqueue error as well as the bounded-send result.
func (self *Client) SendWithTimeoutDetailed(
	frame *protocol.Frame,
	destinationId Id,
	ackCallback AckFunction,
	timeout time.Duration,
	opts ...any,
) (bool, error) {
	return self.sendWithTimeoutDetailed(
		frame,
		destinationId,
		MultiHopId{},
		ackCallback,
		timeout,
		opts...,
	)
}

// Enqueues one frame through a nonempty intermediary path.
func (self *Client) SendMultiHopWithTimeout(
	frame *protocol.Frame,
	destination MultiHopId,
	ackCallback AckFunction,
	timeout time.Duration,
	opts ...any,
) bool {
	success, err := self.SendMultiHopWithTimeoutDetailed(frame, destination, ackCallback, timeout, opts...)
	return success && err == nil
}

// Returns the multi-hop enqueue error as well as the bounded-send result.
func (self *Client) SendMultiHopWithTimeoutDetailed(
	frame *protocol.Frame,
	destination MultiHopId,
	ackCallback AckFunction,
	timeout time.Duration,
	opts ...any,
) (bool, error) {
	if destination.Len() == 0 {
		return false, errors.New("Must have at least one destination id.")
	}
	intermediaryIds, destinationId := destination.SplitTail()
	// note we do not force stream here
	// legacy no-intermediary will not use streams by default
	return self.sendWithTimeoutDetailed(
		frame,
		destinationId,
		intermediaryIds,
		ackCallback,
		timeout,
		opts...,
	)
}

func (self *Client) sendWithTimeout(
	frame *protocol.Frame,
	destinationId Id,
	intermediaryIds MultiHopId,
	ackCallback AckFunction,
	timeout time.Duration,
	opts ...any,
) bool {
	success, err := self.sendWithTimeoutDetailed(frame, destinationId, intermediaryIds, ackCallback, timeout, opts...)
	return success && err == nil
}

// frameList returns the pack's frames (the batch when set, else the single
// frame, else empty for a contract-only pack)
func (self *SendPack) frameList() []*protocol.Frame {
	if self.Frames != nil {
		return self.Frames
	}
	if self.Frame != nil {
		self.singleFrame[0] = self.Frame
		return self.singleFrame[:]
	}
	return nil
}

// serializedMessageByteCount is the authoritative contract charge for a
// queued pack: it observes the exact frames that sendWithSetContractRecords
// will serialize. Keeping accounting and the eventual send on one source of
// truth prevents a stale enqueue-time size from becoming a fatal
// "Bad accounting X <> Y" on acknowledgement.
func (self *SendPack) serializedMessageByteCount() ByteCount {
	return MessageByteCount(self.frameList())
}

// nextSerializedMessageByteCount returns the exact contract debit for the next
// transport-bounded unit. Logical groups retain one admission while advancing
// bounded chunks, so eligibility must not mistake the whole remaining group
// for the one chunk that will be serialized in this Run iteration.
func (self *SendPack) nextSerializedMessageByteCount() ByteCount {
	if !self.logicalGroup {
		return self.serializedMessageByteCount()
	}
	if len(self.Frames) <= self.groupFrameIndex {
		return 0
	}
	maxFrames, maxMessageByteCount := self.groupChunkLimits()
	end := nextSendGroupChunkEndWithLimits(
		self.Frames,
		self.groupFrameIndex,
		maxFrames,
		maxMessageByteCount,
	)
	return MessageByteCount(self.Frames[self.groupFrameIndex:end])
}

// returnFrames frees every frame's message bytes back to the pool
func (self *SendPack) returnFrames() {
	if self.Frames != nil {
		for _, frame := range self.Frames {
			MessagePoolReturn(frame.MessageBytes)
		}
	} else if self.Frame != nil {
		MessagePoolReturn(self.Frame.MessageBytes)
	}
}

// nextSendGroupChunkEnd returns the exclusive end of the next ordered wire
// chunk. This phase deliberately retains the historical two-frame / one-MTU
// message-payload heuristic. That is not exact final TransferFrame sizing;
// exact protobuf, contract, and encryption-envelope accounting is separate
// work. One oversized frame still advances alone: admission already owns it,
// and refusing to advance would strand the group's remaining owners.
func nextSendGroupChunkEndWithLimits(
	frames []*protocol.Frame,
	start int,
	maxFrames int,
	maxMessageByteCount ByteCount,
) int {
	if maxFrames <= 0 {
		maxFrames = sendPackBatchMaxFrames
	}
	if maxMessageByteCount <= 0 {
		maxMessageByteCount = sendPackBatchMaxMessageByteCount
	}
	end := start
	messageByteCount := ByteCount(0)
	for end < len(frames) {
		nextMessageByteCount := messageByteCount + ByteCount(len(frames[end].MessageBytes))
		if start < end && (maxFrames <= end-start ||
			maxMessageByteCount < nextMessageByteCount) {
			break
		}
		messageByteCount = nextMessageByteCount
		end += 1
		if maxFrames <= end-start {
			break
		}
	}
	return end
}

func nextSendGroupChunkEnd(frames []*protocol.Frame, start int) int {
	return nextSendGroupChunkEndWithLimits(
		frames,
		start,
		sendPackBatchMaxFrames,
		sendPackBatchMaxMessageByteCount,
	)
}

func sendGroupChunkCountWithLimits(
	frames []*protocol.Frame,
	maxFrames int,
	maxMessageByteCount ByteCount,
) int {
	chunkCount := 0
	for start := 0; start < len(frames); {
		start = nextSendGroupChunkEndWithLimits(
			frames,
			start,
			maxFrames,
			maxMessageByteCount,
		)
		chunkCount += 1
	}
	return chunkCount
}

func sendGroupChunkCount(frames []*protocol.Frame) int {
	return sendGroupChunkCountWithLimits(
		frames,
		sendPackBatchMaxFrames,
		sendPackBatchMaxMessageByteCount,
	)
}

func (self *SendPack) groupChunkLimits() (int, ByteCount) {
	if self.groupChunkMaxFrames <= 0 || self.groupChunkMaxMessageByteCount <= 0 {
		return sendPackBatchMaxFrames, sendPackBatchMaxMessageByteCount
	}
	return self.groupChunkMaxFrames, self.groupChunkMaxMessageByteCount
}

func (self *SendPack) pinGroupChunkLimits(policy transferFlightPolicySnapshot) {
	if self.groupChunkMaxFrames != 0 || self.groupChunkMaxMessageByteCount != 0 {
		return
	}
	self.groupChunkMaxFrames, self.groupChunkMaxMessageByteCount =
		sendPackChunkLimits(policy)
}

// h1EstablishedEnvelopeAvailable proves that every remaining chunk in one
// logical group stays on a contract-free wire shape. Checking the entire
// remainder matters: limits are pinned for callback accounting, so a group
// must not rotate its contract halfway through a 3,300-byte chunk policy.
func (self *SendSequence) h1EstablishedEnvelopeAvailable(
	messageByteCount ByteCount,
) bool {
	if self.client.ContractManager().SendNoContract(self.destination) {
		return true
	}
	metadata := self.contractMetadata()
	return self.sendContract != nil && self.sendContractAcked &&
		self.sendContractMetadataGeneration == metadata.generation &&
		self.sendContract.canUpdate(messageByteCount)
}

func (self *SendSequence) pinLogicalGroupChunkLimits(
	sendPack *SendPack,
	policy transferFlightPolicySnapshot,
) {
	if sendPack.groupChunkMaxFrames != 0 ||
		sendPack.groupChunkMaxMessageByteCount != 0 {
		return
	}
	sendPack.pinGroupChunkLimits(policy)
	if !policy.h1Only || len(sendPack.Frames) <= sendPack.groupFrameIndex {
		return
	}
	if self.h1EstablishedEnvelopeAvailable(
		MessageByteCount(sendPack.Frames[sendPack.groupFrameIndex:]),
	) {
		sendPack.groupChunkMaxMessageByteCount =
			sendPackH1EstablishedMaxMessageByteCount
	}
}

func (self *SendSequence) readyDrainChunkLimits(
	policy transferFlightPolicySnapshot,
) (int, ByteCount) {
	maxFrames, maxMessageByteCount := sendPackReadyDrainLimits(policy)
	if policy.h1Only && self.h1EstablishedEnvelopeAvailable(0) {
		maxMessageByteCount = sendPackH1EstablishedMaxMessageByteCount
	}
	return maxFrames, maxMessageByteCount
}

// H1 is a reliable ordered byte stream whose ordinary data groups target the
// 4-KiB pooled class inside the larger transport message envelope. It can
// combine more already-ready small frames than the shared H3-compatible path
// without adding a batching timer. Mixed, H3, P2P, and unknown routes retain
// the conservative DATAGRAM-safe bounds.
func sendPackChunkLimits(policy transferFlightPolicySnapshot) (int, ByteCount) {
	if policy.h1Only {
		return sendPackH1GroupMaxFrames, sendPackH1GroupMaxMessageByteCount
	}
	return sendPackBatchMaxFrames, sendPackBatchMaxMessageByteCount
}

// Independently queued Packs use the same physical carrier bounds as logical
// groups, but keep a separate decision point so performance experiments can
// isolate ready-drain scheduling from provider-return group formation.
func sendPackReadyDrainLimits(policy transferFlightPolicySnapshot) (int, ByteCount) {
	return sendPackChunkLimits(policy)
}

// disposeUnsentGroup completes and releases only chunks that have not reached
// serialization. Already materialized chunks retain their independent send
// item ownership and converge on the same group completion during teardown.
func (self *SendPack) disposeUnsentGroup(err error) {
	if !self.logicalGroup || self.groupCompletion == nil {
		self.completeLifecycleFirstRouteWrite(err)
		self.completeNoAck(err)
		self.invokeAck(err)
		self.returnFrames()
		self.releaseRaw()
		return
	}

	for self.groupFrameIndex < len(self.Frames) {
		maxFrames, maxMessageByteCount := self.groupChunkLimits()
		end := nextSendGroupChunkEndWithLimits(
			self.Frames,
			self.groupFrameIndex,
			maxFrames,
			maxMessageByteCount,
		)
		for _, frame := range self.Frames[self.groupFrameIndex:end] {
			MessagePoolReturn(frame.MessageBytes)
		}
		self.groupFrameIndex = end
		self.groupCompletion.firstRouteWrite(err)
		self.groupCompletion.noAckComplete(err)
		self.groupCompletion.terminal(err)
	}
	self.releaseRaw()
}

// SendMultiWithTimeout sends a batch of frames as ONE wire Pack to
// destination (one sequence number, one ack covering the batch). The frames
// must share a destination and ack lifetime — the return egress path uses it
// to coalesce a flow's socket-read batch, collapsing the per-frame
// route/transport handoffs to one for the whole batch. ackCallback fires
// once for the batch. Not a loopback path.
//
// Ownership follows the message pool send rule (see the message_pool.go
// header): the send takes ownership of the frames' message bytes AND of the
// `frames` slice itself, which the pack references asynchronously until the
// sequence marshals it. The caller must not reuse the slice's backing array
// after a successful send — build each batch in a fresh slice (or
// share/copy to retain).
func (self *Client) SendMultiWithTimeout(
	frames []*protocol.Frame,
	destinationId Id,
	ackCallback AckFunction,
	timeout time.Duration,
	opts ...any,
) bool {
	if len(frames) == 0 {
		return true
	}

	select {
	case <-self.ctx.Done():
		return false
	default:
	}

	resolved := self.resolveSendOptions(opts)

	sendPack := &SendPack{
		TransferOptions:              resolved.transferOptions,
		Frames:                       frames,
		Destination:                  destinationId,
		AckCallback:                  ackCallback,
		MessageByteCount:             MessageByteCount(frames),
		Ctx:                          resolved.ctx,
		EncryptionRole:               resolved.encryptionRole,
		EncryptionCompanion:          resolved.encryptionCompanion,
		transportWriteObserver:       resolved.transportWriteObserver,
		schedulingKey:                resolved.schedulingKey,
		logicalLane:                  resolved.logicalLane,
		logicalLaneExplicit:          resolved.logicalLaneExplicit,
		lifecycleUpstreamRecoverable: resolved.upstreamRecoverable,
		retainAfterAckTimeout:        resolved.retainAfterAckTimeout,
	}
	success, err := self.enqueueSendPack(sendPack, timeout)
	return success && err == nil
}

// sendMultiHopGroupWithTimeoutDetailed admits one logical frame group through
// a nonempty intermediary path. Wire-size chunking remains inside the selected
// SendSequence, so chunks preserve ordering and cannot rerun caller routing.
func (self *Client) sendMultiHopGroupWithTimeoutDetailed(
	frames []*protocol.Frame,
	destination MultiHopId,
	ackCallback AckFunction,
	timeout time.Duration,
	opts ...any,
) (bool, error) {
	if destination.Len() == 0 {
		return false, errors.New("Must have at least one destination id.")
	}
	intermediaryIds, destinationId := destination.SplitTail()
	return self.sendGroupToWithTimeoutDetailed(
		frames,
		destinationId,
		intermediaryIds,
		ackCallback,
		timeout,
		opts...,
	)
}

// sendGroupWithTimeoutDetailed is the direct-destination counterpart used by
// provider socket-return batches. It retains one logical admission/callback
// while the selected SendSequence chooses H1- or H3-safe wire chunks.
func (self *Client) sendGroupWithTimeoutDetailed(
	frames []*protocol.Frame,
	destinationId Id,
	ackCallback AckFunction,
	timeout time.Duration,
	opts ...any,
) (bool, error) {
	return self.sendGroupToWithTimeoutDetailed(
		frames,
		destinationId,
		MultiHopId{},
		ackCallback,
		timeout,
		opts...,
	)
}

func (self *Client) sendGroupToWithTimeoutDetailed(
	frames []*protocol.Frame,
	destinationId Id,
	intermediaryIds MultiHopId,
	ackCallback AckFunction,
	timeout time.Duration,
	opts ...any,
) (bool, error) {
	if len(frames) == 0 {
		return true, nil
	}

	select {
	case <-self.ctx.Done():
		return false, errors.New("Done")
	default:
	}

	resolved := self.resolveSendOptions(opts)

	sendPack := &SendPack{
		TransferOptions:              resolved.transferOptions,
		Frames:                       frames,
		logicalGroup:                 true,
		Destination:                  destinationId,
		IntermediaryIds:              intermediaryIds,
		AckCallback:                  ackCallback,
		ackTarget:                    resolved.ackTarget,
		MessageByteCount:             MessageByteCount(frames),
		Ctx:                          resolved.ctx,
		EncryptionRole:               resolved.encryptionRole,
		EncryptionCompanion:          resolved.encryptionCompanion,
		transportWriteObserver:       resolved.transportWriteObserver,
		schedulingKey:                resolved.schedulingKey,
		logicalLane:                  resolved.logicalLane,
		logicalLaneExplicit:          resolved.logicalLaneExplicit,
		lifecycleUpstreamRecoverable: resolved.upstreamRecoverable,
		retainAfterAckTimeout:        resolved.retainAfterAckTimeout,
	}
	return self.enqueueSendPack(sendPack, timeout)
}

func (self *Client) sendWithTimeoutDetailed(
	frame *protocol.Frame,
	destinationId Id,
	intermediaryIds MultiHopId,
	ackCallback AckFunction,
	timeout time.Duration,
	opts ...any,
) (bool, error) {
	select {
	case <-self.ctx.Done():
		return false, errors.New("Done")
	default:
	}

	resolved := self.resolveSendOptions(opts)

	messageByteCount := ByteCount(len(frame.MessageBytes))
	sendPack := &SendPack{
		TransferOptions: resolved.transferOptions,
		Frame:           frame,
		Destination:     destinationId,
		IntermediaryIds: intermediaryIds,
		// store the raw callback; invoked via safeAck so no per-send wrapper
		// closure is allocated.
		AckCallback:                  ackCallback,
		ackTarget:                    resolved.ackTarget,
		MessageByteCount:             messageByteCount,
		Ctx:                          resolved.ctx,
		EncryptionRole:               resolved.encryptionRole,
		EncryptionCompanion:          resolved.encryptionCompanion,
		transportWriteObserver:       resolved.transportWriteObserver,
		schedulingKey:                resolved.schedulingKey,
		logicalLane:                  resolved.logicalLane,
		logicalLaneExplicit:          resolved.logicalLaneExplicit,
		lifecycleUpstreamRecoverable: resolved.upstreamRecoverable,
		retainAfterAckTimeout:        resolved.retainAfterAckTimeout,
	}
	noAck := !resolved.transferOptions.Ack
	if noAck {
		self.sendNoAckOfferedCount.Add(1)
	}
	success, err := self.enqueueSendPack(sendPack, timeout)
	if noAck && !(success && err == nil) {
		self.sendNoAckRefusedCount.Add(1)
	}
	return success, err
}

// The fully resolved values shared by single, batch, and raw sends.
type resolvedSendOptions struct {
	ctx                    context.Context
	transferOptions        TransferOptions
	encryptionRole         sequenceTlsRole
	encryptionCompanion    bool
	transportWriteObserver func(TransportType)
	schedulingKey          sendSchedulingKey
	logicalLane            uint32
	logicalLaneExplicit    bool
	upstreamRecoverable    bool
	retainAfterAckTimeout  bool
	ackTarget              sendAckTarget
}

// Applies options left-to-right. A received TransferKey reproduces the exact
// receiver-visible lane and local encryption session; a later explicit option
// may intentionally derive a different reply contract policy.
func (self *Client) resolveSendOptions(opts []any) resolvedSendOptions {
	resolved := resolvedSendOptions{
		ctx:                 self.ctx,
		transferOptions:     self.settings.DefaultTransferOpts,
		encryptionRole:      sequenceTlsRoleClient,
		encryptionCompanion: self.settings.DefaultTransferOpts.CompanionContract,
	}
	transferKeySession := false
	for _, opt := range opts {
		switch v := opt.(type) {
		case TransferOptions:
			resolved.transferOptions = v
			if !transferKeySession {
				resolved.encryptionCompanion = v.CompanionContract
			}
		case transferOptionsSetAck:
			resolved.transferOptions.Ack = v.Ack
		case transferOptionsSetForceStream:
			resolved.transferOptions.ForceStream = v.ForceStream
		case transferOptionsSetCompanionContract:
			resolved.transferOptions.CompanionContract = v.CompanionContract
			if !transferKeySession {
				resolved.encryptionCompanion = v.CompanionContract
			}
		case TransferKey:
			transferKeySession = true
			resolved.transferOptions.ForceStream = v.ForceStream
			resolved.transferOptions.CompanionContract = v.CompanionContract
			if role, ok := sequenceTlsRoleFromProtobuf(v.EncryptionRole); ok {
				resolved.encryptionRole = role
			}
			resolved.encryptionCompanion = v.EncryptionCompanion
			// A key that states a lane reproduces it; a key that states none
			// leaves the lane to this sender's own gate. Zero is the legacy
			// and control lane and is also the value a key carries when it
			// says nothing about lanes, and the two cannot be told apart in
			// the field, so a stated zero is read as unstated: the gate's own
			// answer when it decides nothing is lane zero anyway, so the only
			// behaviour this changes is that a sender whose count and
			// capability allow hashing is no longer pinned by a key that never
			// meant to pin it (THROUGHPUTFIX §30.2).
			if 0 < v.LogicalLane {
				resolved.logicalLaneExplicit = true
				if v.LogicalLane <= maxLogicalDataLaneCount {
					resolved.logicalLane = v.LogicalLane
				} else {
					resolved.logicalLane = 0
				}
			}
		case transferCtx:
			resolved.ctx = v.Ctx
		case transportWriteOption:
			resolved.transportWriteObserver = v.observer
		case sendSchedulingKeyOption:
			resolved.schedulingKey = v.key
		case sendPackRecoveryOption:
			resolved.upstreamRecoverable = v.upstreamRecoverable
			resolved.retainAfterAckTimeout = v.retainAfterAckTimeout
		case sendAckTargetOption:
			resolved.ackTarget = v.target
		}
	}
	return resolved
}

// sendRawWithTimeoutDetailed is the internal v2+ raw-frame path for a direct
// destination. The frame is embedded in SendPack because no caller needs a
// separate protocol object; messageBytes ownership remains identical to
// SendWithTimeoutDetailed.
func (self *Client) sendRawWithTimeoutDetailed(
	messageType protocol.MessageType,
	messageBytes []byte,
	destinationId Id,
	ackTarget sendAckTarget,
	ackValue ByteCount,
	timeout time.Duration,
	opts ...any,
) (bool, error) {
	return self.sendRawToWithTimeoutDetailed(
		messageType,
		messageBytes,
		destinationId,
		MultiHopId{},
		ackTarget,
		ackValue,
		timeout,
		opts...,
	)
}

// sendRawMultiHopWithTimeoutDetailed is the multi-hop counterpart to
// sendRawWithTimeoutDetailed.
func (self *Client) sendRawMultiHopWithTimeoutDetailed(
	messageType protocol.MessageType,
	messageBytes []byte,
	destination MultiHopId,
	ackTarget sendAckTarget,
	ackValue ByteCount,
	timeout time.Duration,
	opts ...any,
) (bool, error) {
	if destination.Len() == 0 {
		return false, errors.New("Must have at least one destination id.")
	}
	intermediaryIds, destinationId := destination.SplitTail()
	return self.sendRawToWithTimeoutDetailed(
		messageType,
		messageBytes,
		destinationId,
		intermediaryIds,
		ackTarget,
		ackValue,
		timeout,
		opts...,
	)
}

func (self *Client) sendRawToWithTimeoutDetailed(
	messageType protocol.MessageType,
	messageBytes []byte,
	destinationId Id,
	intermediaryIds MultiHopId,
	ackTarget sendAckTarget,
	ackValue ByteCount,
	timeout time.Duration,
	opts ...any,
) (bool, error) {
	select {
	case <-self.ctx.Done():
		return false, errors.New("Done")
	default:
	}

	resolved := self.resolveSendOptions(opts)
	var sendPack *SendPack
	select {
	case sendPack = <-self.rawSendPacks:
	default:
		sendPack = &SendPack{}
	}
	*sendPack = SendPack{
		TransferOptions:              resolved.transferOptions,
		Destination:                  destinationId,
		IntermediaryIds:              intermediaryIds,
		ackTarget:                    ackTarget,
		ackValue:                     ackValue,
		MessageByteCount:             ByteCount(len(messageBytes)),
		Ctx:                          resolved.ctx,
		EncryptionRole:               resolved.encryptionRole,
		EncryptionCompanion:          resolved.encryptionCompanion,
		transportWriteObserver:       resolved.transportWriteObserver,
		schedulingKey:                resolved.schedulingKey,
		logicalLane:                  resolved.logicalLane,
		logicalLaneExplicit:          resolved.logicalLaneExplicit,
		lifecycleUpstreamRecoverable: resolved.upstreamRecoverable,
		retainAfterAckTimeout:        resolved.retainAfterAckTimeout,
		rawPool:                      self.rawSendPacks,
	}
	sendPack.singleFrameValue = protocol.Frame{
		MessageType:  messageType,
		MessageBytes: messageBytes,
		Raw:          true,
	}
	sendPack.Frame = &sendPack.singleFrameValue
	success, err := self.enqueueSendPack(sendPack, timeout)
	if !success {
		sendPack.releaseRaw()
	}
	return success, err
}

func (self *Client) enqueueSendPack(sendPack *SendPack, timeout time.Duration) (bool, error) {
	ctx := sendPack.Ctx
	if sendPack.Destination == self.clientId {
		// loopback
		// fast path without arming a timer
		select {
		case self.loopback <- sendPack:
			return true, nil
		default:
		}

		if timeout < 0 {
			select {
			case <-ctx.Done():
				return false, errors.New("Done")
			case <-self.ctx.Done():
				return false, errors.New("Done")
			case self.loopback <- sendPack:
				return true, nil
			}
		} else if timeout == 0 {
			select {
			case <-ctx.Done():
				return false, errors.New("Done")
			case <-self.ctx.Done():
				return false, errors.New("Done")
			case self.loopback <- sendPack:
				return true, nil
			default:
				return false, nil
			}
		} else {
			select {
			case <-ctx.Done():
				return false, errors.New("Done")
			case <-self.ctx.Done():
				return false, errors.New("Done")
			case self.loopback <- sendPack:
				return true, nil
			case <-time.After(timeout):
				return false, nil
			}
		}
	} else {
		self.startSendPackLifecycle(sendPack)
		noAckObserver := self.settings.SendBufferSettings.NoAckSendObserver
		if noAckObserver != nil && !sendPack.Ack {
			token := self.noAckSendToken.Add(1)
			sendPack.noAckObserver = noAckObserver
			sendPack.noAckClientId = self.clientId
			sendPack.noAckToken = token
			noAckObserver(NoAckSendObservation{
				Phase:         NoAckSendPhaseStarted,
				ClientId:      self.clientId,
				DestinationId: sendPack.Destination,
				Token:         token,
			})
		}
		success, err := self.sendBuffer.Pack(sendPack, timeout)
		if !success {
			observationErr := err
			if observationErr == nil {
				observationErr = ErrSendPackNotAdmitted
			}
			sendPack.completeLifecycleWithoutRouteWrite(observationErr)
			if sendPack.noAckObserver != nil {
				noAckErr := err
				if noAckErr == nil {
					noAckErr = ErrNoAckSendNotAdmitted
				}
				sendPack.completeNoAck(noAckErr)
			}
		}
		return success, err
	}
}

// startSendPackLifecycle assigns the per-Client token before admission and
// publishes the immutable Started identity. A rebuilt Client intentionally
// restarts this counter; shared trackers namespace each observer registration.
func (self *Client) startSendPackLifecycle(sendPack *SendPack) {
	lifecycleObserver := self.settings.SendBufferSettings.SendPackLifecycleObserver
	if lifecycleObserver == nil {
		return
	}
	token := self.sendPackLifecycleToken.Add(1)
	messageType := protocol.MessageType(0)
	if sendPack.Frame != nil {
		messageType = sendPack.Frame.MessageType
	} else if 0 < len(sendPack.Frames) && sendPack.Frames[0] != nil {
		messageType = sendPack.Frames[0].MessageType
	}
	sendPack.lifecycleObserver = lifecycleObserver
	sendPack.lifecycleClientId = self.clientId
	sendPack.lifecycleToken = token
	sendPack.lifecycleMessageType = messageType
	safeSendPackLifecycleObserve(lifecycleObserver, SendPackLifecycleObservation{
		Phase:               SendPackLifecyclePhaseStarted,
		ClientId:            self.clientId,
		DestinationId:       sendPack.Destination,
		Token:               token,
		AckRequired:         sendPack.Ack,
		MessageType:         messageType,
		UpstreamRecoverable: sendPack.lifecycleUpstreamRecoverable,
	})
}

// Enqueues one control frame within a bounded wait.
func (self *Client) SendControlWithTimeout(
	frame *protocol.Frame,
	ackCallback AckFunction,
	timeout time.Duration,
	opts ...any,
) bool {
	return self.SendWithTimeout(
		frame,
		ControlId,
		ackCallback,
		timeout,
		opts...,
	)
}

// Enqueues one direct frame with sender backpressure. A reply passes the
// callback source id as the destination and peer.TransferKey as an option.
func (self *Client) Send(
	frame *protocol.Frame,
	destinationId Id,
	ackCallback AckFunction,
	opts ...any,
) bool {
	return self.SendWithTimeout(frame, destinationId, ackCallback, -1, opts...)
}

// Enqueues one control frame with sender backpressure.
func (self *Client) SendControl(
	frame *protocol.Frame,
	ackCallback AckFunction,
	opts ...any,
) bool {
	return self.Send(
		frame,
		ControlId,
		ackCallback,
		opts...,
	)
}

// Enqueues one frame through a nonempty intermediary path with backpressure.
func (self *Client) SendMultiHop(
	frame *protocol.Frame,
	destination MultiHopId,
	ackCallback AckFunction,
	opts ...any,
) bool {
	return self.SendMultiHopWithTimeout(frame, destination, ackCallback, -1, opts...)
}

// ReceiveFunction
func (self *Client) receive(source TransferPath, frames []*protocol.Frame, peer Peer) {
	// subprotocol frames and queries are consumed here (subprotocol.go); the
	// generic callbacks get the rest of the batch
	// a batch made entirely of them is finished here; an empty batch handed
	// in still reaches the callbacks as it always has
	if 0 < len(frames) {
		frames = self.dispatchSubprotocolFrames(source, frames, peer)
		if len(frames) == 0 {
			return
		}
	}
	for _, receiveCallback := range self.receiveCallbacks.Get() {
		c := func() any {
			return HandleError(func() {
				receiveCallback(source, frames, peer)
			})
		}
		if self.log.V(2).Enabled() {
			TraceWithReturn(
				fmt.Sprintf("[c]receive callback %s %s", self.clientTag, CallbackName(receiveCallback)),
				c,
			)
		} else {
			c()
		}
	}
}

// Counts a receive-side Pack that could not be handed to its sequence
// immediately. Transfer retransmission is responsible for recovery.
func (self *Client) recordReceivePackHandoffDrop(messageByteCount ByteCount) {
	count := self.receivePackHandoffDropCount.Add(1)
	byteCount := self.receivePackHandoffDropByteCount.Add(uint64(messageByteCount))
	if count&(count-1) == 0 && self.log.V(1).Enabled() {
		self.log.Infof(
			"[cr]drop pack handoff count=%d bytes=%d\n",
			count,
			byteCount,
		)
	}
}

// Counts an inbound ACK that could not be handed to a live send sequence
// immediately. A later cumulative ACK or an ordinary Transfer retry recovers.
func (self *Client) recordReceiveAckHandoffDrop() {
	count := self.receiveAckHandoffDropCount.Add(1)
	if count&(count-1) == 0 && self.log.V(1).Enabled() {
		self.log.Infof("[cr]drop ack handoff count=%d\n", count)
	}
}

func (self *Client) recordReceiveAckHandoff(result receiveAckHandoffResult) {
	switch result {
	case receiveAckHandoffAccepted:
		return
	case receiveAckHandoffAcceptedAfterWait:
		self.receiveAckHandoffWaitCount.Add(1)
		self.receiveAckHandoffWaitSuccess.Add(1)
		return
	case receiveAckHandoffQueueWaitTimeout:
		self.receiveAckHandoffWaitCount.Add(1)
		self.receiveAckHandoffQueueFullCount.Add(1)
	case receiveAckHandoffQueueFull:
		self.receiveAckHandoffQueueFullCount.Add(1)
	case receiveAckHandoffSequenceMissing:
		self.receiveAckHandoffMissCount.Add(1)
	}
	self.recordReceiveAckHandoffDrop()
}

// Records only the ACK writer's carrier-queue boundary. Marshal, encryption,
// and ACK compression happen before this interval; a blocked result therefore
// identifies the contention a priority lane would actually remove. The
// counters are intentionally primitive so the mobile sampler can read them
// without installing an observer on the ACK hot path.
func (self *Client) recordReceiveAckRouteWrite(
	transportType TransportType,
	waitDuration time.Duration,
	blocked bool,
	priority bool,
	err error,
) {
	slot := ackTransportSlot(transportType)
	self.receiveAckRouteWriteCount.Add(1)
	self.receiveAckRouteWriteCountByTransport[slot].Add(1)
	if priority {
		self.receiveAckRoutePriorityWriteCount.Add(1)
	}
	if err != nil {
		self.receiveAckRouteWriteErrorCount.Add(1)
		self.receiveAckRouteWriteTimeoutByTransport[slot].Add(1)
	}
	if !blocked || waitDuration <= 0 {
		return
	}
	self.receiveAckRouteWriteBlockedCount.Add(1)
	waitNanoseconds := uint64(waitDuration)
	self.receiveAckRouteWriteWaitNanoseconds.Add(waitNanoseconds)
	self.receiveAckRouteWriteWaitNanosByTransport[slot].Add(waitNanoseconds)
	updateAtomicMaximum(&self.receiveAckRouteWriteMaxWaitNanos, waitNanoseconds)
}

// ForwardFunction
// forward dispatches to the forward callbacks. It is itself a `ForwardFunction`:
// the bytes are valid only for the duration of the call, and the caller returns
// them after (mirrors `receive`).
func (self *Client) forward(path TransferPath, transferFrameBytes []byte) {
	for _, forwardCallback := range self.forwardCallbacks.Get() {
		c := func() any {
			return HandleError(func() {
				forwardCallback(path, transferFrameBytes)
			})
		}
		if self.log.V(2).Enabled() {
			TraceWithReturn(
				fmt.Sprintf("[c]forward callback %s %s", self.clientTag, CallbackName(forwardCallback)),
				c,
			)
		} else {
			c()
		}
	}
}

func (self *Client) AddReceiveCallback(receiveCallback ReceiveFunction) func() {
	callbackId := self.receiveCallbacks.Add(receiveCallback)
	return func() {
		self.receiveCallbacks.Remove(callbackId)
	}
}

func (self *Client) AddForwardCallback(forwardCallback ForwardFunction) func() {
	callbackId := self.forwardCallbacks.Add(forwardCallback)
	return func() {
		self.forwardCallbacks.Remove(callbackId)
	}
}

func (self *Client) run() {
	var workerWaitGroup sync.WaitGroup
	defer func() {
		self.cancel()
		workerWaitGroup.Wait()
	}()

	// receive
	multiRouteReader := self.routeManager.OpenMultiRouteReader(DestinationId(self.clientId))
	defer self.routeManager.CloseMultiRouteReader(multiRouteReader)

	updatePeerAudit := func(source TransferPath, callback func(*PeerAudit)) {
		// immediately send peer audits at this level
		peerAudit := NewSequencePeerAudit(self, source, 0)
		peerAudit.Update(callback)
		peerAudit.Complete()
	}

	// control ping
	if self.clientId != ControlId && 0 < self.settings.ControlPingTimeout {
		workerWaitGroup.Add(1)
		go func() {
			defer workerWaitGroup.Done()
			HandleError(func() {
				for {
					// uniform timeout with mean `ControlPingTimeout`
					timeout := time.Duration(mathrand.Int63n(int64(2 * self.settings.ControlPingTimeout)))
					select {
					case <-self.ctx.Done():
						return
					case <-WakeupAfter(timeout, self.settings.ControlPingTimeout):
					}

					ack := make(chan error)
					frame, err := ToFrame(&protocol.ControlPing{}, self.settings.ProtocolVersion)
					if err != nil {
						self.log.Errorf("[c]could not create ping frame = %s", err)
						continue
					}

					success := self.SendControl(frame, func(err error) {
						select {
						case ack <- err:
						case <-self.ctx.Done():
						}
					})
					if !success {
						// the send did not take the frame: no ack will ever fire, so
						// free the frame and try again next interval instead of
						// wedging this loop on an ack that cannot come
						MessagePoolReturn(frame.MessageBytes)
						continue
					}
					// wait for the ack before sending another ping
					select {
					case err := <-ack:
						if err == nil {
							self.log.Infof("[c]ping\n")
						} else {
							self.log.Infof("[c]ping err = %s\n", err)
						}
					case <-self.ctx.Done():
						return
					}
				}
			})
		}()
	}

	// loopback messages must be serialized
	workerWaitGroup.Add(1)
	go func() {
		defer workerWaitGroup.Done()
		HandleError(func() {
			for {
				select {
				case <-self.ctx.Done():
					return
				case sendPack := <-self.loopback:
					func() {
						defer func() {
							if self.beforeLoopbackReleaseForTest != nil {
								self.beforeLoopbackReleaseForTest()
							}
							sendPack.returnFrames()
							sendPack.releaseRaw()
						}()
						HandleError(func() {
							source := SourceId(self.clientId)
							self.receive(
								source,
								sendPack.frameList(),
								Peer{
									ProvideMode: protocol.ProvideMode_Network,
									TransferKey: TransferKey{
										ForceStream:         sendPack.ForceStream,
										CompanionContract:   sendPack.CompanionContract,
										EncryptionRole:      sendPack.EncryptionRole.complement().toProtobuf(),
										EncryptionCompanion: sendPack.EncryptionCompanion,
										LogicalLane:         sendPack.logicalLane,
									},
								},
							)
							sendPack.invokeAck(nil)
						}, func(err error) {
							sendPack.invokeAck(err)
						})
					}()
				}
			}
		}, self.cancel)
	}()

	for {
		select {
		case <-self.ctx.Done():
			return
		default:
		}

		var transferFrameBytes []byte
		var transportType TransportType
		var carrierReliability CarrierReliability
		var err error
		c := func() error {
			if carrierReader, ok := multiRouteReader.(transferCarrierMultiRouteReader); ok {
				var disposition transferReceiveDisposition
				transferFrameBytes, disposition, err = carrierReader.readWithCarrier(
					self.ctx,
					self.settings.ReadTimeout,
				)
				transportType = disposition.transportType
				carrierReliability = disposition.reliability
			} else if transportReader, ok := multiRouteReader.(TransportMultiRouteReader); ok {
				transferFrameBytes, transportType, err = transportReader.ReadWithTransport(
					self.ctx,
					self.settings.ReadTimeout,
				)
				carrierReliability = CarrierReliabilityUnknown
			} else {
				transferFrameBytes, err = multiRouteReader.Read(self.ctx, self.settings.ReadTimeout)
				transportType = TransportTypeUnknown
				carrierReliability = CarrierReliabilityUnknown
			}
			return err
		}
		if self.log.V(2).Enabled() {
			TraceWithReturn(
				fmt.Sprintf("[c]multi route read %s<-", self.clientTag),
				c,
			)
		} else {
			c()
		}
		if err != nil {
			continue
		}

		// at this point, the route is expected to have already parsed the transfer frame
		// and applied basic validation and source/destination checks
		// because of this, errors in parsing the `FilteredTransferFrame` are not expected
		// decode a minimal subset of the full message needed to make a routing decision
		path, err := FilteredTransferPath(transferFrameBytes)
		if err != nil {
			// bad protobuf (unexpected, see route note above)
			MessagePoolReturn(transferFrameBytes)
			continue
		}
		if path.IsStream() {
			if self.log.V(1).Enabled() {
				self.log.Infof("[cr] %s cannot route message with stream\n", self.clientTag)
			}
			MessagePoolReturn(transferFrameBytes)
			continue
		}

		source := path.SourceMask()

		if self.log.V(1).Enabled() {
			self.log.Infof("[cr] %s %s<-%s s(%s)\n", self.clientTag, path.DestinationId, path.SourceId, path.StreamId)
		}

		if path.DestinationId == self.clientId {
			// the transports have typically not parsed the full `TransferFrame`
			// on error, discard the message and report the peer
			decodedFrame := inboundDecodedTransferFrames.take()
			transferFrame := &decodedFrame.frame
			// hand-rolled copy-safe decode (no reflection); skips the outer
			// transfer_path (routing already parsed it via FilteredTransferPath)
			// and the deprecated message_type. See frame_protobuf.go.
			if !unmarshalOwnedTransferFrame(transferFrameBytes, decodedFrame, false) {
				// bad protobuf
				updatePeerAudit(source, func(a *PeerAudit) {
					a.badMessage(ByteCount(len(transferFrameBytes)))
				})
				inboundDecodedTransferFrames.put(decodedFrame)
				MessagePoolReturn(transferFrameBytes)
				continue
			}

			// unwrapped tracks whether the frame arrived on the wire as
			// plaintext (true) or wrapped (false). Propagated through the
			// ReceivePack → receiveItem → ack path so an ack mirrors the
			// wrap state of the messages it acknowledges. Mirroring keeps
			// acks legible to peers whose ciphers haven't come up yet.
			unwrapped := true

			// receiveRole is the local per-peer session role that owns this
			// inbound stream, handed to the ReceiveBuffer so the
			// ReceiveSequence holds the right session. Default server: normal
			// peer data (the peer is the TLS client) decrypts under our
			// server session. Adjusted below to the role that actually
			// decrypted a wrapped frame, and to client for a plaintext
			// EncryptedControl carrier (the peer's server-role stream).
			receiveRole := sequenceTlsRoleServer
			// receiveCompanion is the local session identity companion owning
			// this inbound stream (not complemented). Default false; set below
			// from the decrypting session, the plaintext companion hint, or the
			// EncryptedControl.
			receiveCompanion := false

			// outer encrypted wrap: the inner bytes are themselves a
			// `TransferFrame`. A per-peer session for `source` carries the
			// cipher. Forwarders never see this branch — they only look at
			// the outer TransferPath, which is plaintext.
			if 0 < len(transferFrame.EncryptedTransferFrame) {
				unwrapped = false
				// Unwrap is fully non-blocking: if no session can decrypt
				// yet, drop the frame and let the sender's resend recover. A
				// client-role send sequence restarts the handshake on its
				// next burst, so a peer that lost (or never built) its
				// responder session rebuilds it — the drop is transient, not
				// a wedge. Keeping the unwrap path wait-free means no single
				// peer can park the single-threaded, all-peers receive loop.
				unwrappedTransferFrameBytes, decryptRole, decryptCompanion, err := self.unwrapFrame(
					path.SourceId, transferFrame.GetSessionRole(), transferFrame.SessionCompanion, transferFrame.EncryptedTransferFrame)
				if err != nil {
					if self.log.V(1).Enabled() {
						self.log.Infof("[cr]unwrap err = %s\n", err)
					}
					// event-driven desync recovery: tell the sealer no local
					// session could open this wrap so it re-handshakes now
					// instead of resending into the void until its sequence
					// lifecycle recovers (rate-limited; requires the role
					// hint to pin the session — anonymous wraps keep the
					// timeout path). See EncryptedControlUnknownWrapNack.
					if senderRole, ok := sequenceTlsRoleFromProtobuf(transferFrame.GetSessionRole()); ok {
						nackCompanion := false
						if transferFrame.SessionCompanion != nil {
							nackCompanion = *transferFrame.SessionCompanion
						}
						self.encryptionSessionManager.NotifyUndecryptableWrap(
							path.SourceId, senderRole, nackCompanion)
					}
					inboundDecodedTransferFrames.put(decodedFrame)
					MessagePoolReturn(transferFrameBytes)
					continue
				}
				receiveRole = decryptRole
				receiveCompanion = decryptCompanion
				// inner frame: decode the path too — it is tamper-checked against
				// the routing path below.
				innerDecodedFrame := inboundDecodedTransferFrames.take()
				if !unmarshalOwnedTransferFrame(unwrappedTransferFrameBytes, innerDecodedFrame, true) {
					updatePeerAudit(source, func(a *PeerAudit) {
						a.badMessage(ByteCount(len(transferFrameBytes)))
					})
					inboundDecodedTransferFrames.put(decodedFrame)
					inboundDecodedTransferFrames.put(innerDecodedFrame)
					MessagePoolReturn(transferFrameBytes)
					MessagePoolReturn(unwrappedTransferFrameBytes)
					continue
				}
				unwrappedTransferFrame := &innerDecodedFrame.frame
				// the inner TransferPath is AEAD-authenticated; the outer
				// is only the routing hint. A mismatch implies tampering
				// in flight or a routing/sender bug. Drop and audit.
				unwrappedPath, err := TransferPathFromProtobuf(unwrappedTransferFrame.TransferPath)
				if err != nil || unwrappedPath != path {
					if self.log.V(1).Enabled() {
						self.log.Infof("[cr] %s outer/inner TransferPath mismatch from %s\n", self.clientTag, path.SourceId)
					}
					updatePeerAudit(source, func(a *PeerAudit) {
						a.badMessage(ByteCount(len(transferFrameBytes)))
					})
					inboundDecodedTransferFrames.put(decodedFrame)
					inboundDecodedTransferFrames.put(innerDecodedFrame)
					MessagePoolReturn(transferFrameBytes)
					MessagePoolReturn(unwrappedTransferFrameBytes)
					continue
				}
				inboundDecodedTransferFrames.put(decodedFrame)
				MessagePoolReturn(transferFrameBytes)
				transferFrameBytes = unwrappedTransferFrameBytes
				decodedFrame = innerDecodedFrame
				transferFrame = unwrappedTransferFrame
			}

			// A plaintext pack with a sender-role hint is the peer's
			// EncryptedControl carrier (its server-role stream). Map the whole
			// sequence to the complement local session — across both the EC packs
			// and the non-EC open/contract packs — so they share one receive
			// sequence; deriving the role per-pack (from the EC frames below)
			// would split the open pack off and gap the handshake. Wrapped packs
			// use the decrypt role from above; the no-hint default is server. The
			// companion hint, when present, pins the companion session (shared by
			// both peers, so taken as-is, not complemented).
			if unwrapped {
				if senderRole, ok := sequenceTlsRoleFromProtobuf(transferFrame.GetSessionRole()); ok {
					receiveRole = senderRole.complement()
				}
				if transferFrame.SessionCompanion != nil {
					receiveCompanion = transferFrame.GetSessionCompanion()
				}
			}

			ack := transferFrame.Ack
			pack := transferFrame.Pack

			if frame := transferFrame.GetFrame(); frame != nil {

				switch frame.GetMessageType() {
				case protocol.MessageType_TransferAck:
					ack = &protocol.Ack{}
					if err := ProtoUnmarshal(frame.GetMessageBytes(), ack); err != nil {
						// bad protobuf
						updatePeerAudit(source, func(a *PeerAudit) {
							a.badMessage(ByteCount(len(transferFrameBytes)))
						})
						inboundDecodedTransferFrames.put(decodedFrame)
						MessagePoolReturn(transferFrameBytes)
						continue
					}

				case protocol.MessageType_TransferPack:
					pack = &protocol.Pack{}
					if err := ProtoUnmarshal(frame.GetMessageBytes(), pack); err != nil {
						// bad protobuf
						updatePeerAudit(source, func(a *PeerAudit) {
							a.badMessage(ByteCount(len(transferFrameBytes)))
						})
						inboundDecodedTransferFrames.put(decodedFrame)
						MessagePoolReturn(transferFrameBytes)
						continue
					}

				default:
					updatePeerAudit(source, func(a *PeerAudit) {
						a.badMessage(ByteCount(len(transferFrameBytes)))
					})
					inboundDecodedTransferFrames.put(decodedFrame)
					MessagePoolReturn(transferFrameBytes)
					continue
				}
				// The v1 carrier bytes were pooled by the outer decoder only
				// for this synchronous inner unmarshal. The resulting ack/pack
				// owns its own proto-decoded fields.
				returnDecodedFrameMessageBytes(frame)
				transferFrame.Frame = nil
			}

			// TransferFrame carries exactly one data-plane body. Rejecting both
			// absent closes a buffer leak on malformed input; rejecting both
			// present avoids handing the same owned wire buffer to two consumers.
			if (ack == nil) == (pack == nil) {
				updatePeerAudit(source, func(a *PeerAudit) {
					a.badMessage(ByteCount(len(transferFrameBytes)))
				})
				if decodedFrame.packOwner == nil {
					returnDecodedPackMessageBytes(pack)
				}
				inboundDecodedTransferFrames.put(decodedFrame)
				MessagePoolReturn(transferFrameBytes)
				continue
			}

			if ack != nil {
				c := func() bool {
					defer MessagePoolReturn(transferFrameBytes)
					receiveAck, err := receiveAckMessageFromProtocol(ack)
					if err != nil {
						self.recordReceiveAckHandoffDrop()
						return false
					}
					ackHandoffTimeout := self.settings.ReceiveBufferSettings.
						ackHandoffTimeout(transportType)
					result := self.sendBuffer.ackMessageDetailed(
						source.SourceId,
						receiveAck,
						ackHandoffTimeout,
					)
					self.recordReceiveAckHandoff(result)
					return result == receiveAckHandoffAccepted ||
						result == receiveAckHandoffAcceptedAfterWait
				}
				if self.log.V(2).Enabled() {
					TraceWithReturn(
						fmt.Sprintf("[cr]ack %s %s<-%s s(%s)", self.clientTag, path.DestinationId, path.SourceId, path.SourceId),
						c,
					)
				} else {
					c()
				}
				inboundDecodedTransferFrames.put(decodedFrame)
				continue
			}
			{
				sequenceId, err := IdFromBytes(pack.SequenceId)
				if err != nil {
					// bad protobuf
					if decodedFrame.packOwner == nil {
						returnDecodedPackMessageBytes(pack)
					}
					inboundDecodedTransferFrames.put(decodedFrame)
					MessagePoolReturn(transferFrameBytes)
					continue
				}
				// Optimistic EC apply: deliver EncryptedControl frames straight to
				// the per-peer session from the receive loop, bypassing the in-order
				// ReceiveSequence drain (which can stall on a sequence gap from a
				// transport reform or loss). EC frames only piggyback that ordering
				// to reuse the retransmit/route plumbing; each handler below is safe
				// to invoke off-order:
				//   - Handshake: gated on `IsAwaitingClientFinished` + a record-prefix
				//     check in `OptimisticallyDeliverHandshake` that rejects
				//     ClientHello-shaped retransmits, so no duplicate bytes reach the
				//     TLS state machine.
				//   - IdentityProof: `receivePeerIdentityProof` short-circuits once
				//     verified, failed, or already buffered — safe to re-deliver.
				// The ReceiveSequence's later in-order delivery still runs and
				// short-circuits in both handlers (just a re-unmarshal). Gated on
				// `unwrapped` (EC packs are always ForceUnwrapped) to skip the
				// wrapped app-data hot path.
				if unwrapped && self.encryptionSessionManager != nil {
					for _, frame := range pack.Frames {
						if frame == nil || frame.MessageType != protocol.MessageType_TransferEncryptedControl {
							continue
						}
						ec := &protocol.EncryptedControl{}
						if err := ProtoUnmarshal(frame.MessageBytes, ec); err != nil {
							continue
						}
						senderRole, ok := sequenceTlsRoleFromProtobuf(ec.SessionRole)
						if !ok {
							continue
						}
						// This stream maps to the complement local session —
						// the one the EncryptedControl drives — keyed by the
						// EC's echoed identity companion. The receive sequence
						// holds it (keeping it alive through the handshake),
						// matching where the EC routes below.
						receiveRole = senderRole.complement()
						receiveCompanion = ec.GetCompanion()
						// Optimistically apply to the complement local session
						// if it already exists; the ReceiveSequence's in-order
						// delivery getOrCreates it otherwise.
						session := self.encryptionSessionManager.Lookup(path.SourceId, senderRole.complement(), ec.GetCompanion())
						if session == nil {
							continue
						}
						switch ec.ControlType {
						case protocol.EncryptedControlType_EncryptedControlHandshake:
							if session.IsAwaitingClientFinished() {
								session.OptimisticallyDeliverHandshake(ec.Payload)
							}
						case protocol.EncryptedControlType_EncryptedControlIdentityProof:
							// Optimistic path must not create epoch state from a
							// stale/reordered/retransmitted proof; only deliver
							// against an epoch that already exists. The in-order
							// path (DeliverEncryptedControl) still handles a proof
							// that races ahead of the local handshake by creating
							// the epoch to buffer it.
							//
							// The epoch (generation) MUST be carried here, exactly
							// as the in-order path carries it: a proof belongs to
							// the epoch that signed it. Delivering one without its
							// generation lets a stale proof occupy this epoch's
							// single pending-proof slot — the real proof is then
							// refused as "already buffered", and the stale one is
							// finally verified against this epoch's exporter,
							// fails, and terminally tombstones a session the peer
							// is still encrypting into (a permanent stall).
							if session.currentEpoch() != nil {
								var proofEpochId Id
								if raw := ec.GetEpochId(); 0 < len(raw) {
									if parsed, err := IdFromBytes(raw); err == nil {
										proofEpochId = parsed
									}
								}
								session.receivePeerIdentityProofForEpoch(ec.Payload, proofEpochId)
							}
						}
					}
				}
				messageByteCount := MessageByteCount(pack.Frames)
				decodedOwner := decodedFrame.detachPackOwner()
				c := func() bool {
					var receivePack *ReceivePack
					if decodedOwner != nil {
						receivePack = &decodedOwner.receivePack
					} else {
						receivePack = &ReceivePack{}
					}
					*receivePack = ReceivePack{
						Source:              source,
						SequenceId:          sequenceId,
						Pack:                pack,
						decodedOwner:        decodedOwner,
						ReceiveCallback:     self.receiveCallback,
						MessageByteCount:    messageByteCount,
						TransferFrameBytes:  transferFrameBytes,
						TransportType:       transportType,
						Unwrapped:           unwrapped,
						EncryptionRole:      receiveRole,
						EncryptionCompanion: receiveCompanion,
					}
					handoffTimeout := self.settings.ReceiveBufferSettings.
						packHandoffTimeout(transportType, carrierReliability)
					success, err := self.receiveBuffer.Pack(receivePack, handoffTimeout)
					if !success {
						if err == nil {
							self.recordReceivePackHandoffDrop(messageByteCount)
						}
						receivePack.messagePoolReturn()
					}
					return success && err == nil
				}
				if self.log.V(2).Enabled() {
					TraceWithReturn(
						fmt.Sprintf("[cr]pack %s %s<-%s s(%s)", self.clientTag, path.DestinationId, path.SourceId, path.StreamId),
						c,
					)
				} else {
					c()
				}
				inboundDecodedTransferFrames.put(decodedFrame)
			}
		} else {
			c := func() {
				// forward is a callback: the bytes are valid only for its duration
				// and are returned here. Without the return, a client with no
				// forwarder leaks one pool buffer for every frame that arrives
				// addressed to another destination (e.g. control-addressed frames
				// accepted by a gateway transport).
				defer MessagePoolReturn(transferFrameBytes)
				self.forward(
					path,
					transferFrameBytes,
				)
			}
			if self.log.V(1).Enabled() {
				Trace(
					fmt.Sprintf("[cr]forward %s %s<-%s s(%s)", self.clientTag, path.DestinationId, path.SourceId, path.StreamId),
					c,
				)
			} else {
				c()
			}
		}
	}
}

// DestinationSendStats reports what this client's live send sequences to one
// destination have written, split into first writes and recovery rewrites.
func (self *Client) DestinationSendStats(destinationId Id) SendDestinationStats {
	return self.sendBuffer.DestinationSendStats(destinationId)
}

func (self *Client) ResendQueueSize(destinationId Id, intermediaryIds MultiHopId, companionContract bool, forceStream bool) (int, ByteCount, Id) {
	count, byteSize, sequenceId, _ := self.ResendQueueSizeAndMessageTypes(destinationId, intermediaryIds, companionContract, forceStream)
	return count, byteSize, sequenceId
}

func (self *Client) ResendQueueSizeAndMessageTypes(
	destinationId Id,
	intermediaryIds MultiHopId,
	companionContract bool,
	forceStream bool,
) (
	int,
	ByteCount,
	Id,
	[]protocol.MessageType,
) {
	if self.sendBuffer == nil {
		return 0, 0, Id{}, nil
	} else {
		return self.sendBuffer.ResendQueueSizeAndMessageTypes(destinationId, intermediaryIds, companionContract, forceStream)
	}
}

func (self *Client) ReceiveQueueSize(source TransferPath, sequenceId Id) (int, ByteCount) {
	count, byteSize, _ := self.ReceiveQueueSizeAndMessageTypes(source, sequenceId)
	return count, byteSize
}

func (self *Client) ReceiveQueueSizeAndMessageTypes(source TransferPath, sequenceId Id) (int, ByteCount, []protocol.MessageType) {
	if self.receiveBuffer == nil {
		return 0, 0, nil
	} else {
		return self.receiveBuffer.ReceiveQueueSizeAndMessageTypes(source, sequenceId)
	}
}

func (self *Client) IsDone() bool {
	select {
	case <-self.ctx.Done():
		return true
	default:
		return false
	}
}

func (self *Client) Done() <-chan struct{} {
	return self.ctx.Done()
}

func (self *Client) Ctx() context.Context {
	return self.ctx
}

// this does not need to be called if `Cancel` is called
// Event-escape contract for teardown (Close / Cancel):
//
// The general rule holds — do NOT rely on any listener event firing after
// Close/Cancel. Cancelling the ctx stops the epoch workers and the sequence
// goroutines; any events they would emit during teardown are best-effort and
// may be dropped (e.g. runContractStats does a single backstop emit on ctx-done,
// but it can only surface closes already marked before the worker exits).
//
// The one exception that must be delivered deterministically is contract-stats
// CLOSES (the Open=false ContractStatsEvent per open contract). These do NOT
// escape from Close/Cancel on their own. To deliver them, the owner MUST call
// CloseContractStats() BEFORE Close/Cancel and BEFORE removing stats listeners,
// so the closes fire synchronously while listeners are still attached. This is
// required so a torn-down client's (e.g. a removed multi-client provider's)
// contracts don't linger open in the contract-details UI. See
// ContractManager.CloseAllContractStats and the multi-client channel teardown.
func (self *Client) Close() {
	self.cancel()
	self.webRtcManagerUnsub()

	if self.streamManager != nil {
		self.streamManager.Close()
	}
	if self.contractManager != nil {
		self.contractManager.Close()
	}
	if self.encryptionSessionManager != nil {
		self.encryptionSessionManager.Close()
	}
	if self.clientKeyManager != nil {
		self.clientKeyManager.Close()
	}
	self.sendBuffer.Close()
	self.receiveBuffer.Close()
	self.forwardBuffer.Close()
	if self.webRtcManager != nil {
		self.webRtcManager.Close()
	}

	// self.contractManagerUnsub()
	self.streamManagerUnsub()
	self.peerManagerUnsub()
}

// CloseAndWait closes the client and joins its reader, client-created signal
// dispatcher, stream transports, transfer sequences, WebRTC peers, and
// contract/encryption manager work. It returns only after those workers have
// relinquished their message-pool ownership, or ctx expires. Close remains the
// non-joining compatibility API.
//
// The join covers only Client-owned work. Arbitrary transports registered in
// RouteManager, including externally created PlatformTransport or direct
// P2pTransport values, remain caller-owned and require their own close/join.
// Extra dispatchers installed through public ReceiveSignalsFromClient only
// unsubscribe when their returned function is called; their lifecycle is also
// caller-owned. OOB work after SendControl returns is owned by the OOB
// implementation, and the process-global probe observer dispatcher is
// process-owned.
//
// Owners must quiesce concurrent Client API calls before using a successful
// join as permission to reclaim the Client. A call paused before an admission
// lock is rejected when it resumes, but its caller goroutine is not owned or
// joined here. A Client-owned callback must not call CloseAndWait because it
// can self-join. It may call non-joining Close/Cancel and ask an external owner
// goroutine to perform the wait.
func (self *Client) CloseAndWait(ctx context.Context) error {
	self.Close()

	var result error
	if self.beforeRunDoneWaitForTest != nil {
		self.beforeRunDoneWaitForTest()
	}
	if err := waitForLifecycleDone(ctx, self.runDone, "client reader"); err != nil {
		result = errors.Join(result, err)
	}
	if self.signalDispatcher != nil {
		if err := self.signalDispatcher.closeAndWait(ctx); err != nil {
			result = errors.Join(result, err)
		}
	}
	if self.streamManager != nil {
		if err := self.streamManager.closeAndWait(ctx); err != nil {
			result = errors.Join(result, err)
		}
	}
	if self.webRtcManager != nil {
		if err := self.webRtcManager.closeAndWait(ctx); err != nil {
			result = errors.Join(result, err)
		}
	}
	if err := self.sendBuffer.closeAndWait(ctx); err != nil {
		result = errors.Join(result, err)
	}
	if err := self.receiveBuffer.closeAndWait(ctx); err != nil {
		result = errors.Join(result, err)
	}
	if err := self.forwardBuffer.closeAndWait(ctx); err != nil {
		result = errors.Join(result, err)
	}
	// Successful control sends transfer their frame ownership into a transfer
	// buffer, so join those buffers before the managers that launched them.
	if self.encryptionSessionManager != nil {
		if err := self.encryptionSessionManager.closeAndWait(ctx); err != nil {
			result = errors.Join(result, err)
		}
	}
	if self.clientKeyManager != nil {
		if err := self.clientKeyManager.closeAndWait(ctx); err != nil {
			result = errors.Join(result, err)
		}
	}
	if self.contractManager != nil {
		if err := self.contractManager.closeAndWait(ctx); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

// waitForLifecycleDone gives an already-published completion precedence over
// an already-canceled wait context. The second completion check resolves the
// select race when both become ready together.
func waitForLifecycleDone(
	ctx context.Context,
	done <-chan struct{},
	name string,
) error {
	select {
	case <-done:
		return nil
	default:
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		select {
		case <-done:
			return nil
		default:
			return fmt.Errorf("wait for %s: %w", name, ctx.Err())
		}
	}
}

func (self *Client) Cancel() {
	self.Close()
}

// CloseContractStats fires the close events for all of this client's open
// contracts synchronously, so they reach attached stats listeners before the
// client is cancelled and the listeners are removed. Call it just before
// Cancel/Close at teardown. See ContractManager.CloseAllContractStats.
func (self *Client) CloseContractStats() {
	self.contractManager.CloseAllContractStats()
}

func (self *Client) Flush() {
	self.sendBuffer.Flush()
	self.receiveBuffer.Flush()
	self.forwardBuffer.Flush()

	self.contractManager.Flush(false)
}

type SendBufferSettings struct {
	CreateContractTimeout time.Duration
	// CreateContractRetryInterval is the fast first retry interval.
	CreateContractRetryInterval time.Duration
	// CreateContractRetryMaxInterval caps exponential retry backoff. Zero
	// preserves the historical constant-interval behavior for callers that
	// construct settings without the new field.
	CreateContractRetryMaxInterval time.Duration

	// resend timeout is the initial time between successive send attempts. Does linear backoff
	// MinResendInterval is the resend floor while no rtt samples exist (the
	// cold floor); RttMinResendInterval is the floor once the path rtt is
	// measured (0 = keep the cold floor for sampled paths too).
	MinResendInterval    time.Duration
	RttMinResendInterval time.Duration
	MaxResendInterval    time.Duration
	// UnreliableMaxResendInterval caps per-item exponential backoff while any
	// active carrier lacks payload retransmission. Zero preserves
	// MaxResendInterval.
	UnreliableMaxResendInterval time.Duration
	// ResendBackoffScale float32

	RttScale         float32
	RttWindowSize    int
	RttWindowTimeout time.Duration

	// on ack timeout, no longer attempt to retransmit and notify of ack failure
	AckTimeout time.Duration
	// UnreliableAckTimeout extends (never shortens) an item's delivery lifetime
	// after it has used a carrier without payload retransmission. Zero preserves
	// AckTimeout. The extension is sticky per item across route changes.
	UnreliableAckTimeout time.Duration
	IdleTimeout          time.Duration

	SelectiveAckTimeout time.Duration
	// The number of distinct later selectively acknowledged Packs required to
	// recover the earliest missing Pack before its resend timeout. Zero disables
	// gap recovery; each item can trigger it at most once.
	SelectiveAckGapThreshold int
	// Bounds distinct missing Packs released from one selective-ACK snapshot.
	// Zero disables gap recovery even when the threshold is configured.
	SelectiveAckGapBurstSize int
	// Bounds minimum-RTT-paced probes of the oldest remaining Pack after ACK
	// progress. This covers one- and two-Packet tail loss, where too few later
	// selective ACKs exist to prove a gap. Zero disables tail probing.
	AckTailProbeLimit int

	// Bounds acknowledged Transfer bytes admitted to a carrier that does not
	// retransmit payload. The limit starts at Initial, never falls below
	// Minimum, grows no higher than Maximum, and uses Increase as its additive
	// recovery quantum after receiver-proven loss. Zero Initial disables it.
	UnreliableInitialFlightByteCount     ByteCount
	UnreliableMinimumFlightByteCount     ByteCount
	UnreliableMaximumFlightByteCount     ByteCount
	UnreliableSlowStartGrowthDivisor     int
	UnreliableFlightIncreaseByteCount    ByteCount
	UnreliableInitialFlightMessageCount  int
	UnreliableMinimumFlightMessageCount  int
	UnreliableMaximumFlightMessageCount  int
	UnreliableFlightIncreaseMessageCount int
	// DeferTimeoutResendWhileCumulativeProgress delays the whole-window RTO
	// resend of a reliable-carried item by one scaled RTT, at most
	// TimeoutResendDeferLimit times, while the cumulative ack advanced within
	// the last scaled RTT (FLIGHTGATEFIX §13.5, §15). A retransmit timer
	// started when an item was queued cannot be met by a lane that drains at
	// link rate, so without this the whole window is rewritten every
	// interval against a path that is demonstrably delivering.
	DeferTimeoutResendWhileCumulativeProgress bool
	TimeoutResendDeferLimit                   int
	// UnreliableFloorSingleFlight keeps at most one message in flight on an
	// unreliable carrier whose flight limit has collapsed to its floor after
	// repeated loss, while a reliable carrier takes everything else. The lossy
	// lane still proves itself with that one message (an ACK reopens growth)
	// but no longer stripes an ordered stream with a carrier that drops it.
	UnreliableFloorSingleFlight bool

	SequenceBufferSize int
	AckBufferSize      int

	MinMessageByteCount ByteCount

	// ContractWaitLogThreshold is the contract-acquisition wait above which
	// the wait is logged. Deliberately well under a second: contract
	// acquisition blocks the send sequence, so a few hundred ms of it would
	// dominate every request while never appearing in a log.
	ContractWaitLogThreshold time.Duration

	WriteTimeout time.Duration

	ResendQueueMaxByteCount ByteCount
	// ResendQueueMinByteCount is the guaranteed per-sequence floor when
	// `ResendQueueBudget` is set: below it admission never consults the
	// shared budget, so every sequence progresses on floor capacity alone
	ResendQueueMinByteCount ByteCount
	// DeliverySizedWindowScale turns the send window from a constant into a
	// measurement of the path: the window becomes what this lane delivered
	// over the last acknowledgement round trip, multiplied by this, clamped
	// between `ResendQueueMaxByteCount` as the floor and the ceiling below.
	//
	// Why a multiple of delivery. A window-limited flow delivers exactly its
	// window per round trip by definition, so a scale of two doubles the
	// window each round trip until the flow is no longer window-limited and
	// then holds. From a 2 MiB floor to a 16 MiB ceiling is three round trips,
	// under a tenth of a second at 25 ms, and the converging phase is never
	// worse than today's constant because the floor is today's constant.
	//
	// The estimate's errors are asymmetric and both bounded. Too short a round
	// trip shrinks the delivered count and the window, which is today's
	// behaviour. Too long grows it toward the ceiling, costing memory and at
	// most one round trip of extra queueing for the client's other flows,
	// which is why the scale should not exceed two.
	//
	// Zero, the shipping default, keeps the constant. The before and after are
	// then the same binary with one field changed, which is what measuring a
	// multiple on one cell needs.
	DeliverySizedWindowScale int
	// CarrierChangeVoidsSelectiveAck lets the retired-carrier recovery move
	// selectively acknowledged items too (THROUGHPUTFIX §37.17 guard two).
	CarrierChangeVoidsSelectiveAck bool
	// WindowSizing is the one switch: constant is today's behaviour exactly,
	// from-delivery derives the scale, the ceiling, the budget and the initial
	// bet and turns the rule on (THROUGHPUTFIX §37.4).
	WindowSizing WindowSizingPolicyKind
	// TargetGoodputByteRate caps the window at the target's bandwidth-delay
	// product, so a path faster than the target does not take more than the
	// target. Zero leaves the target term out.
	TargetGoodputByteRate ByteCount
	// The ceiling for the rule above. It is a share of a budget rather than a
	// per-sequence constant: forty simultaneous downloaders at 16 MiB would
	// retain over a gigabyte on one provider, so a constant that works in a
	// one-client cell is exactly what fails in production. When
	// `ResendQueueBudget` is set the effective ceiling is the smaller of this
	// and what that budget will lend, and the queue's existing floor-and-
	// borrow admission does the sharing; a deployment that turns the scale on
	// sets a budget in the same change.
	DeliverySizedWindowCeilingByteCount ByteCount
	// LaneFloorByteCount is that floor for a nonzero logical data lane, whose
	// queue is otherwise given none: every byte it holds is borrowed from one
	// shared pool the size of a single `ResendQueueMaxByteCount`, and each
	// lane's cap is that whole pool, so one bulk flow's lane can hold all of
	// it and a light lane keeps only the single item an empty queue
	// guarantees (THROUGHPUTFIX §27).
	//
	// It is an exemption, not a reservation: bytes are consumed only by queued
	// packets, so a client whose flows hash to one lane pays nothing for the
	// lanes it never opens, and an active lane may still borrow the whole pool
	// above its floor. What it buys is that no lane is reduced below its floor
	// by another lane's occupancy.
	//
	// Zero, the shipping default, preserves today's behavior so a campaign can
	// set it and the trees stay comparable. The candidate scale is
	// `ResendQueueMinByteCount`, which is what lane zero and every distinct
	// destination on an sdk-hosted provider already keep.
	//
	// Deployment rule, because the default has a sharp edge: enabling lanes
	// with this at zero ships the starvation. `LogicalDataLaneCount` and this
	// are one decision, set in a single change; a nonzero lane count with a
	// zero floor is not a configuration this program endorses.
	LaneFloorByteCount ByteCount
	// ReliableAdmissionBoundedByDelivery bounds what a sequence may hold
	// unacknowledged on a reliable lane by what that lane has shown it can
	// carry: the bytes it acknowledged over the last scaled round trip,
	// floored at ResendQueueMinByteCount (FLIGHTGATEFIX §22). Delivery is a
	// count the sequence already has rather than a model, and no queue
	// depth can inflate it, where the window's mean round trip lags an
	// inflation by design and is what the retransmit timer already reads.
	// Inert where the floor is the budget, which is the mobile envelope,
	// and on any lane delivering more than the budget per scaled round
	// trip.
	ReliableAdmissionBoundedByDelivery bool
	// DeferredItemIsLateForTheScoreboard makes F11b's grace and the
	// retransmit timer agree about one item (FLIGHTGATEFIX §23.2). The grace
	// is anchored on the item's send time plus the scaled round trip, which
	// is exactly when its own timer first comes due, so once the deferred
	// retransmit extends that timer the grace has already expired and the
	// scoreboard writes at the next round of later acknowledgements what the
	// timer just declined to write. With this set, an item holding a
	// deferral is late rather than lost for the scoreboard until the
	// deferral expires. Off by default: it is a candidate to be read in one
	// campaign after the A/A calibration of §23.3, not a landing.
	DeferredItemIsLateForTheScoreboard bool
	// DeferTimeoutResendBackoff makes a deferral back off like the rewrite
	// it replaces (FLIGHTGATEFIX §24.3). Without it a deferral re-arms at
	// the same scaled round trip and leaves sendCount alone, so a window
	// stalled mid-transfer re-fires whole every round trip and the deferral
	// limit writes the third firing of every item into the stall; merged's
	// rewrite doubles its interval each attempt and backs off. Parallel
	// flows make the window several times deeper, so every firing costs
	// that much more, which is why only that workload shows it.
	DeferTimeoutResendBackoff bool
	// ReliableTimerUsesDeviation reads the retransmit timer of a
	// reliable-carried item as RFC 6298 does, the mean round trip plus four
	// deviations, instead of the mean times RttScale (FLIGHTGATEFIX §25.2).
	// The relay of the exchange cells has a mean under a second and goes
	// 2.75 s without acknowledging, a three-times excursion that a fixed
	// margin cannot cover without lengthening every lane's retransmit. Off
	// by default: a candidate to be read in one campaign, and it divides the
	// work with the deferral backoff rather than replacing it, since a stall
	// longer than anything sampled is the backoff's to bound.
	ReliableTimerUsesDeviation bool
	// ReliableLaneProvenRecovery reads a reliable lane's silence as a lane
	// event rather than as one per item (FLIGHTGATEFIX §26.2). A reliable
	// carrier retransmits below Transfer, so an item unacknowledged while
	// later items on the same route are acknowledged was dropped at an
	// endpoint, and one unacknowledged while nothing later on that route is
	// acknowledged is queued or stalled, never lost. So gap recovery of a
	// reliable-carried hole counts only later acknowledgements from its own
	// route, with no time grace, and a timer firing on a route that has
	// acknowledged nothing past the item probes the route's oldest
	// outstanding item and holds the rest behind it.
	//
	// Off by default, and the staging flip was attempted and withdrawn. With
	// it on, TestSendSequenceQueueInflatedRelayRttDoesNotFireWholeWindowTimeouts
	// fails two runs in eight under the race detector and none in four with
	// it off: on a single reliable lane whose acknowledgement delay has grown
	// past its own scaled round trip, the silent verdict of §27.3 fires a
	// probe where §13.5 would defer, which is M4's contract. The lagging mean
	// is the same one §22 and §25 identified, and using the item's own
	// interval as the window does not close it either. The setting stays so
	// the flip is one boolean once that is resolved.
	ReliableLaneProvenRecovery bool
	// ResendQueueBudget, when set, is a byte budget shared across sequences
	// (typically all clients of one device): resend queue bytes above the
	// floor reserve from it, and admission pauses above the floor while it
	// is empty. nil keeps independent per-sequence caps.
	ResendQueueBudget *TransferMemoryBudget

	// NoAckSendObserver is a nil-by-default integration seam. It receives one
	// started event before a non-loopback NoAck SendPack attempts SendBuffer
	// admission and one completion event with the same token after that exact
	// pack's first route write succeeds or fails (including enqueue rejection
	// and shutdown). Started alone does not transfer message ownership.
	// The callback must not block. Ack sends are deliberately excluded because
	// their completion means peer acknowledgement rather than initial route
	// serialization.
	NoAckSendObserver func(NoAckSendObservation)

	// SendPackLifecycleObserver is a nil-by-default measurement seam for every
	// original non-loopback Pack, including Ack, NoAck, raw, and coalesced
	// sends. The callback must not block; a panic is recovered and logged. See
	// SendPackLifecycleObservation for exact phase and ownership semantics.
	SendPackLifecycleObserver func(SendPackLifecycleObservation)

	// Borrows the exact bytes immediately before one physical route attempt.
	// WireMessageBytes includes the optional encryption wrapper while
	// TransferFrameBytes is the corresponding plaintext Transfer frame. The
	// callback must not retain either slice or block; a panic is recovered.
	TransferWireMessageObserver func(TransferWireMessageObservation)

	// Nil test barriers are copied into SendBuffer during construction. Tests
	// set them before NewClient starts lifecycle goroutines, which keeps the
	// seam race-free without synchronizing production paths.
	beforeCreateSendSequenceForTest func(sendSequenceId)
	beforeRunSendSequenceForTest    func(sendSequenceId)
	beforeCloseWaitForTest          func(sendSequenceId)
	beforeResendCapacityWaitForTest func(sendSequenceId)
	afterRunSendSequenceForTest     func(sendSequenceId)
	// Runs synchronously after one reliable send item reaches terminal Ack
	// disposition and before the Ack worker advances to another item.
	afterInitialWriteQueuedForTest        func(sendSequenceId, uint64)
	afterAckCoalescedForTest              func(sendSequenceId, uint64)
	afterAckSendItemForTest               func(sendSequenceId, uint64)
	beforeDueResendForTest                func(sendSequenceId, uint64)
	afterCreateSendGroupCompletionForTest func(sendSequenceId, int)
	// Nil test barrier pauses one encrypted-control owner before Pack.
	beforeEncryptedControlPackForTest    func([]byte)
	beforeContractFailureClassifyForTest func(sendSequenceId)
	beforeTakeContractForTest            func(sendSequenceId)
	forceAckTimeoutForTest               func(sendSequenceId) bool
	forceContractFailureForTest          func(sendSequenceId) bool
	forceResendForTest                   func(sendSequenceId) bool

	// as this ->1, there is more risk that noack messages will get dropped due to out of sync contracts
	ContractFillFraction float32

	// PrewarmOpeningContract requests a sequence's first contract as the
	// sequence starts, rather than waiting until a message needs one.
	//
	// Every later contract is already queued asynchronously the moment its
	// predecessor is taken, which is why renewals mid-stream are fast. The first
	// has nothing ahead of it to trigger that, so acquiring it blocks the first
	// send for a full round trip to the platform -- measured at ~260ms on a
	// device, paid by every new destination. Firing the request as the sequence
	// starts overlaps that round trip with the work that produced the first
	// message.
	//
	// off restores the previous behavior of requesting it on demand.
	PrewarmOpeningContract bool

	// CompactContractHead replaces a repeated full contract on a new head with
	// the id of the contract the receiver already acknowledged. This keeps an
	// ordinary packet below the H3 bounded packet-lane threshold after a quiet
	// flight.
	// It activates only after the receiver advertises explicit missing-contract
	// recovery, so legacy peers continue to receive complete contracts.
	CompactContractHead bool

	// LogicalDataLaneCount enables a bounded number of five-tuple-hashed data
	// ordering lanes after the peer advertises transferLogicalLaneVersion on a
	// delivery Ack. Zero disables lane selection; values above eight are clamped.
	// Lane zero remains the compatibility/control lane in every configuration.
	LogicalDataLaneCount int

	ProtocolVersion int
}

// SendPackLifecyclePhase identifies one ordered phase of an original Pack.
type SendPackLifecyclePhase uint8

const (
	SendPackLifecyclePhaseStarted SendPackLifecyclePhase = iota + 1
	SendPackLifecyclePhaseFirstRouteWrite
	SendPackLifecyclePhaseTerminal
)

// SendPackLifecycleObservation follows one original non-loopback SendPack.
// Token is unique within one Client instance; observers shared across rebuilt
// Clients must namespace it by observer registration. Started precedes buffer
// admission. FirstRouteWrite reports the first writer's actual disposition, or
// an error when the Pack never reached a writer. Terminal follows removal from
// reliable resend ownership and reports peer Ack or the final sequence error.
// AckRequired is the caller's requested policy; an opening contract may still
// temporarily put a requested NoAck Pack on the reliable wire lane. MessageType
// is the original Frame type, or the first Frame type for an explicitly batched
// Pack, and remains unchanged through every phase.
type SendPackLifecycleObservation struct {
	Phase         SendPackLifecyclePhase
	ClientId      Id
	DestinationId Id
	Token         uint64
	AckRequired   bool
	MessageType   protocol.MessageType
	// UpstreamRecoverable is true only when the caller explicitly identifies
	// an enclosing transport that retains or can regenerate this Pack after a
	// failed attempt. It is observation metadata and never weakens delivery.
	UpstreamRecoverable bool
	Err                 error
}

// Correlates an encrypted carrier message with its inspectable Transfer
// contents without weakening or bypassing encryption in integration tests.
type TransferWireMessageObservation struct {
	WireMessageBytes                 []byte
	TransferFrameBytes               []byte
	MessageId                        Id
	SequenceNumber                   uint64
	SendCount                        int
	Resend                           bool
	PromotedHead                     bool
	CompactContractRecoverySupported bool
}

// ErrSendPackNotAdmitted completes lifecycle observation when bounded
// admission returns false without a more specific error.
var ErrSendPackNotAdmitted = errors.New("send Pack was not admitted")

// NoAckSendPhase makes event pairing explicit at asynchronous observers.
type NoAckSendPhase uint8

const (
	NoAckSendPhaseStarted NoAckSendPhase = iota + 1
	NoAckSendPhaseCompleted
)

// A NoAck send emits exactly one phase pair. ClientId namespaces the token
// when one observer is shared by generated clients. Started precedes Pack and
// does not itself mean ownership transferred; completion with nil Err means
// the initial route write accepted ownership.
type NoAckSendObservation struct {
	Phase         NoAckSendPhase
	ClientId      Id
	DestinationId Id
	Token         uint64
	Err           error
}

// ErrNoAckSendNotAdmitted is reported only to the optional observer when the
// public send API returns false without a more specific error.
var ErrNoAckSendNotAdmitted = errors.New("no-Ack send was not admitted")

type sendSequenceId struct {
	Destination       Id
	CompanionContract bool
	ForceStream       bool
	LogicalLane       uint32
	// EncryptionRole separates the client-role send sequence (normal
	// application data, which restarts the handshake) from the server-role
	// send sequence (EncryptedControl carriers + server replies, which never
	// restart). Zero value is client.
	EncryptionRole sequenceTlsRole
	// EncryptionCompanion is the per-peer session identity companion, distinct
	// from `CompanionContract`: a server-role reply carrier echoes the
	// initiator's bit while riding EncryptionControlUseCompanion, so it must key
	// the sequence separately to keep each session's carrier distinct.
	EncryptionCompanion bool
}

// sendSequenceWireId is the part of a sender sequence that the destination can
// distinguish in ReceiveBuffer's head key. The local ClientId becomes the
// receiver's Source and is constant for this SendBuffer. ForceStream and
// CompanionContract and LogicalLane are stamped on every Pack (fields
// 10/11/12) so the receiver
// keys its head slot per lane and same-class sequences on different lanes
// coexist. Intermediaries are contract-acquisition metadata on the sequence,
// not sequence identity: every send to this logical peer/lane shares the same
// destination-keyed writer and may use any live route.
type sendSequenceWireId struct {
	Destination         Id
	EncryptionRole      sequenceTlsRole
	EncryptionCompanion bool
	ForceStream         bool
	CompanionContract   bool
	LogicalLane         uint32
}

func (self sendSequenceId) wireId() sendSequenceWireId {
	return sendSequenceWireId{
		Destination:         self.Destination,
		EncryptionRole:      self.EncryptionRole,
		EncryptionCompanion: self.EncryptionCompanion,
		ForceStream:         self.ForceStream,
		CompanionContract:   self.CompanionContract,
		LogicalLane:         self.LogicalLane,
	}
}

func (self sendSequenceId) logicalLaneBase() sendSequenceId {
	self.LogicalLane = 0
	return self
}

type SendBuffer struct {
	ctx    context.Context
	client *Client
	log    Logger

	sendBufferSettings *SendBufferSettings

	mutex                      sync.Mutex
	closed                     bool
	sendSequences              map[sendSequenceId]*SendSequence
	wireSendSequences          map[sendSequenceWireId]*SendSequence
	sendSequencesBySequenceId  map[Id]*SendSequence
	sendSequencesByDestination map[Id]map[*SendSequence]bool
	sendSequenceDestinations   map[*SendSequence]map[Id]bool
	// activeSendSequences retains every lifecycle worker through its final
	// queue drain, including workers already removed from the lookup indexes.
	activeSendSequences map[*SendSequence]bool
	// logicalLaneVersions is keyed by the exact lane-zero sequence class. A
	// capability is valid only while that lane-zero sequence is alive.
	logicalLaneVersions map[sendSequenceId]uint32
	// An immutable copy of `logicalLaneVersions`, published whenever it
	// changes, so the per-Pack lane gate reads the version with an atomic load
	// rather than the buffer-wide mutex every sequence of the client shares.
	//
	// Enabling a nonzero lane count used to add that acquisition to every
	// Pack, and a count of zero returned before it, which is why the cost
	// appeared only when a count was set: a harness arm with the count at
	// eight and no lane ever engaging ran 13 to 17 per cent below the same
	// fixture at zero, with nothing else on the packet path differing
	// (THROUGHPUTFIX §30.3).
	logicalLaneVersionSnapshot atomic.Pointer[map[sendSequenceId]uint32]
	// reports what the lane gate saw on each Pack; nil is a production no-op
	// The lane gate's decision, for a cell that needs to read which gate bound
	// rather than infer it from the lane. Atomic because a cell installs it on
	// a running client and the gate runs on the send goroutine.
	logicalLaneGateObserverForTest atomic.Pointer[logicalLaneGateObserver]
	// When the caller did not provide a device-wide resend budget, every
	// nonzero lane still shares this one fixed pool instead of receiving one
	// independent ResendQueueMaxByteCount allocation per lane.
	logicalLaneResendBudget *TransferMemoryBudget

	// Nil test barriers expose exact sequence lifecycle boundaries without
	// changing production behavior or relying on scheduler timing in regressions.
	beforeCreateSendSequenceForTest       func(sendSequenceId)
	beforeRunSendSequenceForTest          func(sendSequenceId)
	beforeCloseWaitForTest                func(sendSequenceId)
	beforeResendCapacityWaitForTest       func(sendSequenceId)
	afterRunSendSequenceForTest           func(sendSequenceId)
	afterInitialWriteQueuedForTest        func(sendSequenceId, uint64)
	afterAckCoalescedForTest              func(sendSequenceId, uint64)
	afterAckSendItemForTest               func(sendSequenceId, uint64)
	beforeDueResendForTest                func(sendSequenceId, uint64)
	afterCreateSendGroupCompletionForTest func(sendSequenceId, int)
	beforeEncryptedControlPackForTest     func([]byte)
	beforeContractFailureClassifyForTest  func(sendSequenceId)
	beforeTakeContractForTest             func(sendSequenceId)
	forceAckTimeoutForTest                func(sendSequenceId) bool
	forceContractFailureForTest           func(sendSequenceId) bool
	forceResendForTest                    func(sendSequenceId) bool
}

func NewSendBuffer(ctx context.Context,
	client *Client,
	sendBufferSettings *SendBufferSettings) *SendBuffer {
	return &SendBuffer{
		ctx:                                   ctx,
		client:                                client,
		log:                                   client.log,
		sendBufferSettings:                    sendBufferSettings,
		sendSequences:                         map[sendSequenceId]*SendSequence{},
		wireSendSequences:                     map[sendSequenceWireId]*SendSequence{},
		sendSequencesBySequenceId:             map[Id]*SendSequence{},
		sendSequencesByDestination:            map[Id]map[*SendSequence]bool{},
		sendSequenceDestinations:              map[*SendSequence]map[Id]bool{},
		activeSendSequences:                   map[*SendSequence]bool{},
		logicalLaneVersions:                   map[sendSequenceId]uint32{},
		beforeCreateSendSequenceForTest:       sendBufferSettings.beforeCreateSendSequenceForTest,
		beforeRunSendSequenceForTest:          sendBufferSettings.beforeRunSendSequenceForTest,
		beforeCloseWaitForTest:                sendBufferSettings.beforeCloseWaitForTest,
		beforeResendCapacityWaitForTest:       sendBufferSettings.beforeResendCapacityWaitForTest,
		afterRunSendSequenceForTest:           sendBufferSettings.afterRunSendSequenceForTest,
		afterInitialWriteQueuedForTest:        sendBufferSettings.afterInitialWriteQueuedForTest,
		afterAckCoalescedForTest:              sendBufferSettings.afterAckCoalescedForTest,
		afterAckSendItemForTest:               sendBufferSettings.afterAckSendItemForTest,
		beforeDueResendForTest:                sendBufferSettings.beforeDueResendForTest,
		afterCreateSendGroupCompletionForTest: sendBufferSettings.afterCreateSendGroupCompletionForTest,
		beforeEncryptedControlPackForTest:     sendBufferSettings.beforeEncryptedControlPackForTest,
		beforeContractFailureClassifyForTest:  sendBufferSettings.beforeContractFailureClassifyForTest,
		beforeTakeContractForTest:             sendBufferSettings.beforeTakeContractForTest,
		forceAckTimeoutForTest:                sendBufferSettings.forceAckTimeoutForTest,
		forceContractFailureForTest:           sendBufferSettings.forceContractFailureForTest,
		forceResendForTest:                    sendBufferSettings.forceResendForTest,
	}
}

// One installed reader of the lane gate's decision.
type logicalLaneGateObserver struct {
	observe func(logicalLaneGateObservation)
}

// selectLogicalLane reads capability state before sequence assignment. An
// explicit TransferKey always reproduces its already-negotiated lane; ordinary
// IP traffic hashes only after this exact lane-zero class has acknowledged
// support.
func (self *SendBuffer) selectLogicalLane(sendPack *SendPack) uint32 {
	observedBase := sendSequenceId{}
	observe := func(gate string, version uint32, lane uint32) uint32 {
		if observer := self.logicalLaneGateObserverForTest.Load(); observer != nil {
			observer.observe(logicalLaneGateObservation{
				base:            observedBase,
				explicit:        sendPack.logicalLaneExplicit,
				explicitLane:    sendPack.logicalLane,
				schedulingValid: sendPack.schedulingKey.valid,
				version:         version,
				bindingGate:     gate,
				lane:            lane,
			})
		}
		return lane
	}
	if sendPack.logicalLaneExplicit {
		// The reply key decides. A provider's return carries the client's
		// whole transfer key with only the companion bit changed, so a client
		// at lane zero pins every return to lane zero whatever the provider's
		// own count is, and the scheduling key below is never consulted
		// (THROUGHPUTFIX §30.2).
		return observe(
			"explicit reply key",
			0,
			min(sendPack.logicalLane, uint32(maxLogicalDataLaneCount)),
		)
	}
	// A nonzero count is set together with SendBufferSettings.LaneFloorByteCount
	// and never alone: without the floor every lane above zero borrows all of
	// its bytes from one shared pool and a light lane beside a bulk one keeps
	// a single Pack in flight (THROUGHPUTFIX §27).
	count := min(
		max(0, self.sendBufferSettings.LogicalDataLaneCount),
		maxLogicalDataLaneCount,
	)
	if self.client.settings.ContractManagerSettings.LegacyCreateContract {
		count = 0
	}
	if count == 0 {
		return observe("zero count", 0, 0)
	}
	if !sendPack.schedulingKey.valid {
		return observe("no scheduling key", 0, 0)
	}
	base := sendSequenceId{
		Destination:         sendPack.Destination,
		CompanionContract:   sendPack.TransferOptions.CompanionContract,
		ForceStream:         sendPack.TransferOptions.ForceStream,
		EncryptionRole:      sendPack.EncryptionRole,
		EncryptionCompanion: sendPack.EncryptionCompanion,
	}
	observedBase = base
	// A lock-free read of the published snapshot. Taking the buffer mutex here
	// would put a client-wide acquisition on every Pack of every sequence.
	version := uint32(0)
	if snapshot := self.logicalLaneVersionSnapshot.Load(); snapshot != nil {
		version = (*snapshot)[base]
	}
	if version < transferLogicalLaneVersion {
		return observe("unadvertised version", version, 0)
	}
	return observe("hashed", version, sendPack.schedulingKey.logicalLaneForCount(count))
}

// What the lane gate saw and which of its gates decided, for the rows that ask
// whether a provider's returns can ride a data lane at all. Test only.
type logicalLaneGateObservation struct {
	// the lane-zero class whose advertised version the gate consults
	base            sendSequenceId
	explicit        bool
	explicitLane    uint32
	schedulingValid bool
	version         uint32
	bindingGate     string
	lane            uint32
}

// Publishes an immutable copy of the version map for the lock-free gate.
// Called with the buffer mutex held, from every site that changes the map.
func (self *SendBuffer) publishLogicalLaneVersionsWithLock() {
	snapshot := make(map[sendSequenceId]uint32, len(self.logicalLaneVersions))
	maps.Copy(snapshot, self.logicalLaneVersions)
	self.logicalLaneVersionSnapshot.Store(&snapshot)
}

// observeLogicalLaneVersion accepts capability evidence only from the live
// lane-zero sequence and only after its Ack worker matched the message to an
// outstanding item. A missing capability clears stale rollout evidence and
// retires data lanes so their original Packs retry through lane zero.
func (self *SendBuffer) observeLogicalLaneVersion(
	sequence *SendSequence,
	version uint32,
) {
	id := sequence.id()
	if id.LogicalLane != 0 {
		return
	}
	base := id.logicalLaneBase()
	var cancel []*SendSequence
	self.mutex.Lock()
	if self.sendSequences[id] != sequence {
		self.mutex.Unlock()
		return
	}
	if transferLogicalLaneVersion <= version {
		self.logicalLaneVersions[base] = min(version, transferLogicalLaneVersion)
		self.publishLogicalLaneVersionsWithLock()
	} else {
		delete(self.logicalLaneVersions, base)
		self.publishLogicalLaneVersionsWithLock()
		for candidateId, candidate := range self.sendSequences {
			if candidateId.LogicalLane != 0 &&
				candidateId.logicalLaneBase() == base {
				cancel = append(cancel, candidate)
			}
		}
	}
	self.mutex.Unlock()
	for _, candidate := range cancel {
		candidate.Cancel()
	}
}

// lookupSendSequence is the per-pack fast path. It deliberately contains no
// closure that can retain id: sendSequenceId is a large comparable value, and
// capturing it in Pack's former creation closure forced one heap allocation on
// every packet even when the sequence already existed.
func (self *SendBuffer) lookupSendSequence(id sendSequenceId, skip *SendSequence) *SendSequence {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	sendSequence, ok := self.sendSequences[id]
	if !ok {
		return nil
	}
	if skip == nil || skip != sendSequence {
		return sendSequence
	}
	sendSequence.Cancel()
	delete(self.sendSequences, id)
	wireId := id.wireId()
	if self.wireSendSequences[wireId] == sendSequence {
		delete(self.wireSendSequences, wireId)
	}
	return nil
}

// createSendSequence is the uncommon slow path. Recheck under the lock because
// another concurrent sender may have populated the key after lookup released
// it. Only this path lets the lifecycle goroutine retain id.
func (self *SendBuffer) createSendSequence(id sendSequenceId, sendPack *SendPack) *SendSequence {
	if self.beforeCreateSendSequenceForTest != nil {
		self.beforeCreateSendSequenceForTest(id)
	}
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if self.closed {
		return nil
	}
	if sendSequence, ok := self.sendSequences[id]; ok {
		return sendSequence
	}
	if id.LogicalLane != 0 && !sendPack.logicalLaneExplicit &&
		self.logicalLaneVersions[id.logicalLaneBase()] < transferLogicalLaneVersion {
		return nil
	}
	var logicalLaneBaseSequence *SendSequence
	if id.LogicalLane != 0 && !sendPack.logicalLaneExplicit {
		// Capability is scoped to the live lane-zero generation. Keep that
		// generation non-idle while any negotiated data lane depends on it; an
		// otherwise quiet lane zero must not expire every IdleTimeout and tear
		// down active traffic. UpdateOpen fails if retirement already won.
		logicalLaneBaseSequence = self.sendSequences[id.logicalLaneBase()]
		if logicalLaneBaseSequence == nil ||
			!logicalLaneBaseSequence.idleCondition.UpdateOpen() {
			delete(self.logicalLaneVersions, id.logicalLaneBase())
			self.publishLogicalLaneVersionsWithLock()
			return nil
		}
	}

	wireId := id.wireId()
	logicalLaneResendBudget := self.logicalLaneResendBudget
	if id.LogicalLane != 0 &&
		self.sendBufferSettings.ResendQueueBudget == nil &&
		logicalLaneResendBudget == nil {
		// Keep disabled/legacy clients allocation-neutral. The buffer lock makes
		// this one lazily materialized pool shared by every nonzero lane.
		logicalLaneResendBudget = NewTransferMemoryBudget(
			self.sendBufferSettings.ResendQueueMaxByteCount,
		)
		self.logicalLaneResendBudget = logicalLaneResendBudget
	}
	sendSequence := newSendSequenceWithLogicalLane(
		self.ctx,
		self.client,
		self,
		sendPack.Destination,
		sendPack.IntermediaryIds,
		sendPack.TransferOptions.CompanionContract,
		sendPack.TransferOptions.ForceStream,
		sendPack.TransferOptions.NetworkPeer,
		sendPack.EncryptionRole,
		sendPack.EncryptionCompanion,
		id.LogicalLane,
		self.sendBufferSettings,
		logicalLaneResendBudget,
	)
	if logicalLaneBaseSequence != nil {
		sendSequence.logicalLaneBaseSequence = logicalLaneBaseSequence
		sendSequence.logicalLaneBasePinned = true
	}
	self.sendSequences[id] = sendSequence
	self.wireSendSequences[wireId] = sendSequence
	self.sendSequencesBySequenceId[sendSequence.sequenceId] = sendSequence
	self.activeSendSequences[sendSequence] = true
	// note we do not associate destination here
	// the sequence will call `AssociateDestination` before it writes
	go self.runSendSequence(id, wireId, sendSequence)
	return sendSequence
}

// closeSendSequence removes all buffer-wide indexes before draining the
// sequence. Draining invokes intentionally synchronous acknowledgement
// callbacks, so it must happen after the map lock is released.
func (self *SendBuffer) closeSendSequence(
	id sendSequenceId,
	wireId sendSequenceWireId,
	sendSequence *SendSequence,
) {
	self.mutex.Lock()
	wasCurrent := sendSequence == self.sendSequences[id]
	// clean up
	if wasCurrent {
		delete(self.sendSequences, id)
	}
	if sendSequence == self.wireSendSequences[wireId] {
		delete(self.wireSendSequences, wireId)
	}
	if sendSequence == self.sendSequencesBySequenceId[sendSequence.sequenceId] {
		delete(self.sendSequencesBySequenceId, sendSequence.sequenceId)
	}
	if destinations, ok := self.sendSequenceDestinations[sendSequence]; ok {
		for destination := range destinations {
			if sendSequences, ok := self.sendSequencesByDestination[destination]; ok {
				delete(sendSequences, sendSequence)
				if len(sendSequences) == 0 {
					delete(self.sendSequencesByDestination, destination)
				}
			}
		}
		delete(self.sendSequenceDestinations, sendSequence)
	}
	var cancelLogicalLanes []*SendSequence
	if wasCurrent && id.LogicalLane == 0 {
		base := id.logicalLaneBase()
		delete(self.logicalLaneVersions, base)
		self.publishLogicalLaneVersionsWithLock()
		for candidateId, candidate := range self.sendSequences {
			if candidateId.LogicalLane != 0 &&
				candidateId.logicalLaneBase() == base {
				cancelLogicalLanes = append(cancelLogicalLanes, candidate)
			}
		}
	}
	self.mutex.Unlock()
	if sendSequence.logicalLaneBasePinned {
		sendSequence.logicalLaneBasePinned = false
		sendSequence.logicalLaneBaseSequence.idleCondition.UpdateClose()
	}
	for _, candidate := range cancelLogicalLanes {
		candidate.Cancel()
	}

	// Close drains queued packs and invokes their completion callbacks.
	// Those callbacks are intentional backpressure and may block. Never
	// invoke them while holding the buffer-wide sequence-map lock: one
	// stalled destination must not prevent unrelated destinations from
	// finding or creating their own send sequence.
	sendSequence.Close()
}

func (self *SendBuffer) runSendSequence(id sendSequenceId, wireId sendSequenceWireId, sendSequence *SendSequence) {
	defer func() {
		self.mutex.Lock()
		delete(self.activeSendSequences, sendSequence)
		close(sendSequence.done)
		self.mutex.Unlock()
	}()
	if self.beforeRunSendSequenceForTest != nil {
		self.beforeRunSendSequenceForTest(id)
	}
	HandleError(func() {
		defer func() {
			self.closeSendSequence(id, wireId, sendSequence)
			if self.afterRunSendSequenceForTest != nil {
				self.afterRunSendSequenceForTest(id)
			}
		}()
		sendSequence.Run()
	})
}

func (self *SendBuffer) Pack(sendPack *SendPack, timeout time.Duration) (bool, error) {
	var sendSequence *SendSequence
	var success bool
	var err error
	for i := 0; i < 3; i += 1 {
		select {
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		default:
		}
		sendPack.logicalLane = self.selectLogicalLane(sendPack)
		id := sendSequenceId{
			Destination:         sendPack.Destination,
			CompanionContract:   sendPack.TransferOptions.CompanionContract,
			ForceStream:         sendPack.TransferOptions.ForceStream,
			EncryptionRole:      sendPack.EncryptionRole,
			EncryptionCompanion: sendPack.EncryptionCompanion,
			LogicalLane:         sendPack.logicalLane,
		}
		nextSendSequence := self.lookupSendSequence(id, sendSequence)
		if nextSendSequence == nil {
			nextSendSequence = self.createSendSequence(id, sendPack)
		}
		if nextSendSequence == nil {
			// Capability may have been withdrawn between selection and creation.
			// Re-evaluate once through lane zero rather than rejecting ownership.
			if id.LogicalLane != 0 && !sendPack.logicalLaneExplicit {
				sendPack.logicalLane = 0
				sendSequence = nil
				continue
			}
			return false, errors.New("Done.")
		}
		sendSequence = nextSendSequence
		if success, err = sendSequence.Pack(sendPack, timeout); err == nil {
			return success, nil
		}
		if errors.Is(err, ErrEncryptionRequiredNotEstablished) {
			// Not a sequence problem: the Required entry gate refused the
			// send. Retrying on a recreated sequence would wait the same
			// budget again against the same unestablished session.
			return false, err
		}
		// sequence closed
	}
	return success, err
}

// SendEncryptedControl enqueues an `EncryptedControl` to `destination` as a
// regular Pack Frame (`MessageType = TransferEncryptedControl`); routing,
// retransmit, and in-order delivery reuse the sequence machinery, and the
// destination's ReceiveSequence intercepts these frames into the per-peer
// session.
//
// `ctx` gates whether the spawned goroutine may enqueue (it bails if done). The
// pack uses the SendBuffer's ctx — the session ctx must not propagate into
// `SendPack.Ctx`, since SendBuffer.Pack treats a canceled `SendPack.Ctx` as a
// sequence problem and cancels the SendSequence.
func (self *SendBuffer) SendEncryptedControl(
	ctx context.Context,
	peerId Id,
	role sequenceTlsRole,
	ec *protocol.EncryptedControl,
	encryptionCompanion bool,
	contractCompanion bool,
	forceStream bool,
	networkPeer bool,
) bool {
	select {
	case <-ctx.Done():
		return false
	default:
	}
	ecBytes, err := ProtoMarshal(ec)
	if err != nil {
		return false
	}
	// Pack transfers ecBytes only when it returns success. Every shutdown,
	// timeout, and admission rejection leaves ownership here.
	ownedEcBytes := true
	defer func() {
		if ownedEcBytes {
			MessagePoolReturn(ecBytes)
		}
	}()
	frame := &protocol.Frame{
		MessageType:  protocol.MessageType_TransferEncryptedControl,
		MessageBytes: ecBytes,
	}
	// Mirror the transfer options the application's data path uses for this
	// destination, especially the force-stream lane learned at send-session
	// acquisition. The Pack carries both lane fields, so the carrier and data
	// must select the same receiver-visible head rather than splitting one
	// logical encrypted flow across separate sequence lanes.
	//
	// The carrier rides one send sequence per (peer, companion, role).
	// `contractCompanion` (the session's carrierCompanion) is which contract it
	// rides; `encryptionCompanion` (the session identity bit) keys the
	// sequence/session. They differ only for a server reply, where the identity
	// is the initiator's echoed bit but the contract is
	// EncryptionControlUseCompanion. Symmetric config: both false.
	opts := self.client.settings.DefaultTransferOpts
	opts.Ack = true
	opts.CompanionContract = contractCompanion
	// companion carriers stay off streams: the platform rejects companion
	// stream contracts (see the V(2) diagnostic below)
	opts.ForceStream = forceStream && !contractCompanion
	opts.NetworkPeer = networkPeer && !contractCompanion
	// V(2) diagnostic: in symmetric mode no encryption-control carrier should
	// be a companion. Log the decision so a companion carrier (whose Stream-mode
	// contract the platform rejects → handshake stalls) can be caught.
	if self.log.V(2).Enabled() {
		self.log.Infof(
			"[sb][enc-ctrl]%s peer=%s role=%v companion=%t contract-companion=%t\n",
			self.client.ClientTag(), peerId, role, encryptionCompanion, contractCompanion,
		)
	}
	sendPack := &SendPack{
		TransferOptions:  opts,
		Frame:            frame,
		Destination:      peerId,
		AckCallback:      func(error) {},
		MessageByteCount: ByteCount(len(ecBytes)),
		Ctx:              self.ctx,
		// Pin to plaintext on every (re)send. These frames bootstrap the
		// per-peer cipher; sending them wrapped would deadlock the
		// handshake whenever the local cipher becomes available before
		// the peer's side completes its half. See writeMaybeWrappedBytes.
		ForceUnwrapped: true,
		// Carry EncryptedControl on the send sequence of the originating
		// session's role (client-session handshake bytes on the (peer,client)
		// sequence, server-session bytes on the (peer,server) one). For the
		// client role this is the same sequence the application data uses, so
		// the ClientHello produced by that sequence's own restart rides it
		// without spawning a second sequence (no recursion); the restart is a
		// no-op while a handshake is already in flight. The EncryptedControl's
		// `session_role` + `companion` tell the receiver which complement
		// session to route each frame to.
		EncryptionRole:      role,
		EncryptionCompanion: encryptionCompanion,
	}
	if self.beforeEncryptedControlPackForTest != nil {
		self.beforeEncryptedControlPackForTest(ecBytes)
	}
	for {
		if success, _ := self.Pack(sendPack, self.client.settings.BufferTimeout); success {
			ownedEcBytes = false
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-self.ctx.Done():
			return false
		default:
		}
	}
}

func (self *SendBuffer) Ack(destinationId Id, ack *protocol.Ack, timeout time.Duration) bool {
	receiveAck, err := receiveAckMessageFromProtocol(ack)
	if err != nil {
		return false
	}
	result := self.ackMessageDetailed(destinationId, receiveAck, timeout)
	return result == receiveAckHandoffAccepted ||
		result == receiveAckHandoffAcceptedAfterWait
}

func (self *SendBuffer) ackMessage(
	destinationId Id,
	ack receiveAckMessage,
	timeout time.Duration,
) bool {
	result := self.ackMessageDetailed(destinationId, ack, timeout)
	return result == receiveAckHandoffAccepted ||
		result == receiveAckHandoffAcceptedAfterWait
}

func (self *SendBuffer) ackMessageDetailed(
	destinationId Id,
	ack receiveAckMessage,
	timeout time.Duration,
) receiveAckHandoffResult {
	self.mutex.Lock()
	sequence := self.sendSequencesBySequenceId[ack.sequenceId]
	if sequence != nil && sequence.destination != destinationId {
		sequence = nil
	}
	self.mutex.Unlock()
	if sequence == nil {
		if self.log.V(1).Enabled() {
			self.log.Infof("[sb]ack miss sequence does not exist %s\n", destinationId)
		}
		return receiveAckHandoffSequenceMissing
	}
	result, _ := sequence.ackMessageDetailed(ack, timeout)
	return result
}

// What every live send sequence to one destination has put on the wire,
// first writes and recovery rewrites separately (THROUGHPUTFIX §13). A
// diagnostic read under the buffer lock; the counters themselves are
// per-sequence atomics advanced at the write.
type SendDestinationStats struct {
	SequenceCount        int
	WriteCount           uint64
	WriteByteCount       uint64
	ResendWriteCount     uint64
	ResendWriteByteCount uint64
	// SendWindow is what the sequences to this destination may hold
	// unacknowledged, and the evidence the rule derived it from. `Sized` is
	// false when the rule is off or had no samples, which a campaign needs in
	// order to tell a setting that engaged from one that silently did nothing.
	SendWindow SendWindowEstimate
	// Rtt is the acknowledgement round trip over the sequences to this
	// destination, carried with its evidence: an unsampled estimate and a
	// measured sub-millisecond one are different facts and the type keeps them
	// apart.
	//
	// It is here because a per-destination ceiling is its resend queue over
	// the effective acknowledgement round trip, and that divisor is the
	// network's round trip plus terms of ours. Whether it is mostly ours or
	// mostly the network's decides between raising the queue, which buys
	// throughput proportionally and costs memory proportionally, and
	// shrinking the divisor, which buys the same throughput for no memory.
	// The sequence already measures the quantity; nothing outside could read
	// it.
	//
	// Interim: this is a reader on an accumulating surface, where the rule
	// would put a decision input on a mechanism surface that the deciding
	// predicate itself reads. The next mechanism change is where that shape
	// gets established, rather than by retrofitting stats types that work.
	Rtt RttEstimate
	// how many of this destination's sequences contributed a sampled estimate
	RttSequenceCount int
}

func (self *SendBuffer) DestinationSendStats(destinationId Id) SendDestinationStats {
	self.mutex.Lock()
	sequences := map[*SendSequence]bool{}
	for id, sequence := range self.sendSequences {
		if id.Destination == destinationId {
			sequences[sequence] = true
		}
	}
	for sequence := range self.sendSequencesByDestination[destinationId] {
		sequences[sequence] = true
	}
	self.mutex.Unlock()

	stats := SendDestinationStats{SequenceCount: len(sequences)}
	meanRttTotal := time.Duration(0)
	newestSampleAge := time.Duration(0)
	minimumRtt := time.Duration(0)
	now := time.Now()
	for sequence := range sequences {
		stats.WriteCount += sequence.writeCount.Load()
		stats.WriteByteCount += sequence.writeByteCount.Load()
		stats.ResendWriteCount += sequence.resendWriteCount.Load()
		stats.ResendWriteByteCount += sequence.resendWriteByteCount.Load()
		// the window's own lock is a leaf, and the buffer lock above is
		// already released
		// the largest window any of them computed, with its evidence
		if windowEstimate := sequence.sendWindowEstimate(now); stats.SendWindow.Window < windowEstimate.Window ||
			(windowEstimate.Sized && !stats.SendWindow.Sized) {
			stats.SendWindow = windowEstimate
		}
		if estimate := sequence.rttWindow.Estimate(); estimate.Sampled() {
			meanRttTotal += estimate.Mean
			stats.Rtt.SampleCount += estimate.SampleCount
			if stats.RttSequenceCount == 0 || estimate.NewestSampleAge < newestSampleAge {
				newestSampleAge = estimate.NewestSampleAge
			}
			// the smallest live sample over the destination's sequences
			if stats.RttSequenceCount == 0 || estimate.Min < minimumRtt {
				minimumRtt = estimate.Min
			}
			stats.RttSequenceCount += 1
		}
	}
	if 0 < stats.RttSequenceCount {
		stats.Rtt.Mean = meanRttTotal / time.Duration(stats.RttSequenceCount)
		stats.Rtt.Min = minimumRtt
		stats.Rtt.NewestSampleAge = newestSampleAge
	}
	return stats
}

func (self *SendBuffer) ResendQueueSizeAndMessageTypes(destinationId Id, _ MultiHopId, companionContract bool, forceStream bool) (int, ByteCount, Id, []protocol.MessageType) {
	self.mutex.Lock()
	sequences := make([]*SendSequence, 0, maxLogicalDataLaneCount+1)
	for logicalLane := uint32(0); logicalLane <= maxLogicalDataLaneCount; logicalLane++ {
		if sequence := self.sendSequences[sendSequenceId{
			Destination:       destinationId,
			CompanionContract: companionContract,
			ForceStream:       forceStream,
			LogicalLane:       logicalLane,
		}]; sequence != nil {
			sequences = append(sequences, sequence)
		}
	}
	self.mutex.Unlock()

	var count int
	var byteSize ByteCount
	var sequenceId Id
	haveSequenceId := false
	var messageTypes []protocol.MessageType
	for _, sequence := range sequences {
		sequenceCount, sequenceByteSize, currentSequenceId, sequenceMessageTypes :=
			sequence.ResendQueueSizeAndMessageTypes()
		if !haveSequenceId || sequence.logicalLane == 0 {
			// Preserve the historical lane-zero signal id when it exists. With
			// only data lanes alive, return a representative live id.
			sequenceId = currentSequenceId
			haveSequenceId = true
		}
		count += sequenceCount
		byteSize += sequenceByteSize
		messageTypes = append(messageTypes, sequenceMessageTypes...)
	}
	return count, byteSize, sequenceId, messageTypes
}

// called before a send sequence writes a transfer frame with a stream id,
// once per destination
func (self *SendBuffer) AssociateDestination(sendSequence *SendSequence, destinationId Id) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	sendSequences, ok := self.sendSequencesByDestination[destinationId]
	if !ok {
		sendSequences = map[*SendSequence]bool{}
		self.sendSequencesByDestination[destinationId] = sendSequences
	}
	sendSequences[sendSequence] = true

	destinations, ok := self.sendSequenceDestinations[sendSequence]
	if !ok {
		destinations = map[Id]bool{}
		self.sendSequenceDestinations[sendSequence] = destinations
	}
	destinations[destinationId] = true
}

// Cancels and joins every send sequence that can carry one exact destination.
// The destination index covers multi-hop associations; the primary id scan
// also catches a fresh sequence before its first route association.
func (self *SendBuffer) cancelDestinationAndWait(destinationId Id) {
	self.mutex.Lock()
	sequences := map[*SendSequence]bool{}
	for id, sequence := range self.sendSequences {
		if id.Destination == destinationId {
			sequences[sequence] = true
		}
	}
	for sequence := range self.sendSequencesByDestination[destinationId] {
		sequences[sequence] = true
	}
	self.mutex.Unlock()

	for sequence := range sequences {
		sequence.Cancel()
	}
	for sequence := range sequences {
		<-sequence.done
	}
}

func (self *SendBuffer) Close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.closed = true
	// cancel all open sequences
	// the control of the sequence will close it
	for _, sendSequence := range self.sendSequences {
		sendSequence.Cancel()
	}
}

func (self *SendBuffer) Cancel() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.closed = true
	// cancel all open sequences
	for _, sendSequence := range self.sendSequences {
		sendSequence.Cancel()
	}
}

// closeAndWait closes sequence admission and joins every lifecycle worker that
// was admitted before the close boundary. Worker completion is published only
// after SendSequence.Close has drained all queue and resend ownership.
func (self *SendBuffer) closeAndWait(ctx context.Context) error {
	self.Close()

	self.mutex.Lock()
	sequences := make([]*SendSequence, 0, len(self.activeSendSequences))
	for sendSequence := range self.activeSendSequences {
		sequences = append(sequences, sendSequence)
	}
	self.mutex.Unlock()

	for _, sendSequence := range sequences {
		if self.beforeCloseWaitForTest != nil {
			self.beforeCloseWaitForTest(sendSequence.id())
		}
		if err := waitForLifecycleDone(ctx, sendSequence.done, "send sequence"); err != nil {
			return err
		}
	}
	return nil
}

func (self *SendBuffer) Flush() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// cancel all open sequences
	for _, sendSequence := range self.sendSequences {
		// if !sendSequenceId.Destination.IsControlDestination() {
		sendSequence.Cancel()
		// }
	}
}

type SendSequence struct {
	ctx    context.Context
	cancel context.CancelFunc
	// done closes after the owning SendBuffer removes every index and drains
	// all Pack, resend, callback, and message-pool ownership.
	done chan struct{}

	client     *Client
	sendBuffer *SendBuffer
	log        Logger

	destination Id
	// contractStateLock protects the route hint and the cancelable acquisition
	// generation derived from it. Pack callers may promote a direct-created
	// sequence while Run is waiting for a contract.
	contractStateLock          sync.Mutex
	intermediaryIds            MultiHopId
	contractContext            context.Context
	cancelContractContext      context.CancelFunc
	contractMetadataGeneration uint64

	companionContract bool
	forceStream       bool
	logicalLane       uint32
	// An implicitly negotiated data lane holds one IdleCondition reference on
	// the exact lane-zero generation that advertised support. closeSendSequence
	// releases it exactly once after removing the data lane from public indexes.
	logicalLaneBaseSequence *SendSequence
	logicalLaneBasePinned   bool
	// networkPeer is immutable contract policy captured from the first Pack.
	// It is intentionally absent from sendSequenceId/wire identity: changing a
	// local sizing hint must never fork two receiver-indistinguishable
	// sequences.
	networkPeer bool
	// encryptionRole is the per-peer session role this send sequence uses:
	// client for normal application data (the default), server for
	// EncryptedControl carriers and server-session replies.
	encryptionRole sequenceTlsRole
	// encryptionCompanion is the per-peer session identity companion this
	// sequence uses (distinct from `companionContract`). Keys the acquired
	// session and is stamped on every pack as the `session_companion` wire hint.
	encryptionCompanion bool
	sequenceId          Id

	sendBufferSettings *SendBufferSettings

	// the head contract. this contract is also in `openSendContracts`
	sendContract                   *sequenceContract
	sendContractAcked              bool
	sendContractMetadataGeneration uint64
	// contracts are closed when the data are acked
	// these contracts are waiting for acks to close
	openSendContracts map[Id]*sequenceContract

	// packMutex protects packs from Close and coordinates the idle-close
	// checkpoint. Pack only needs a read lock: multiple callers must be able
	// to wait on the bounded queue independently. With an exclusive lock, one
	// application send using an infinite timeout could hold the mutex while
	// the queue was full, preventing a finite-time liveness probe behind it
	// from observing its own timeout. That hid a route-full condition
	// indefinitely. Close takes the write lock after canceling the sequence,
	// which wakes every blocked Pack before the channel is closed.
	packMutex sync.RWMutex
	packs     chan *SendPack
	// packAdmission counts both channel-resident and scheduler-resident Packs,
	// so flow isolation cannot expand the configured memory bound.
	packAdmission *sendPackAdmission
	// Published by Run after each route-policy snapshot so concurrent Pack
	// callers never read the goroutine-owned multi-route writer directly.
	flowIsolation atomic.Bool
	ackMutex      sync.Mutex
	acks          chan receiveAckMessage
	// ackWindow is the allocation-free cumulative/selective ACK coalescer shared
	// by the normal ACK worker and the saturated-handoff fallback. Publishing it
	// at construction lets a full compact channel fold progress into the same
	// window instead of dropping an ACK and waiting for Transfer recovery.
	ackWindow *sequenceAckWindow

	resendQueue        *resendQueue
	sendItems          []*sendItem
	nextSequenceNumber uint64
	flightController   *sendFlightController
	// Set only after three-later-ACK evidence proves this sequence has holes.
	// Tail probing stays off for reliable ordered carriers that show no gap.
	selectiveGapRecoveryActive bool

	// contract acquisition blocks this sequence, so track how much of its life
	// goes into waiting for one. atomics so stats can be read without taking
	// the sequence lock.
	contractWaitNanos atomic.Int64
	contractWaitCount atomic.Int64
	// contractTakenForTest is a nil production seam that lets regressions pause
	// after queue ownership transfers but before the contract becomes the head.
	contractTakenForTest func(sendContractMetadata)

	idleCondition *IdleCondition

	rttWindow *RttWindow
	// lastHeadAckTime is when the cumulative (head) ACK last advanced.
	lastHeadAckTime time.Time
	// what this sequence has put on the wire: first writes and recovery
	// rewrites, counted at the write, so a destination's egress can be
	// split into delivery and retransmission (THROUGHPUTFIX §13)
	writeCount           atomic.Uint64
	writeByteCount       atomic.Uint64
	resendWriteCount     atomic.Uint64
	resendWriteByteCount atomic.Uint64

	contractMultiRouteWriter MultiRouteWriter
	// deliveredBytes is a short history of what this lane has acknowledged:
	// sixteen samples of (time, running acknowledged total), advanced on a
	// cumulative acknowledgement when the newest is older than a quarter of
	// the retransmit pacing floor. It measures the drain by delivery rather
	// than by the round-trip mean, which lags an inflation by design
	// (FLIGHTGATEFIX §22). Allocated once with the sequence: 256 bytes.
	// nil unless ReliableAdmissionBoundedByDelivery is set: an off flag must
	// not retain bytes (FLIGHTGATEFIX §29.4).
	// The ring is advanced by the acknowledgement worker and read by the
	// window rule, which a stats caller on any goroutine can reach through
	// DestinationSendStats. A leaf lock: nothing is taken under it.
	deliveredBytesMutex sync.Mutex
	deliveredBytes      []deliveredBytesSample
	deliveredBytesHead  int
	deliveredBytesCount int
	deliveredByteTotal  ByteCount
	// the receiver's latest advertised hold (THROUGHPUTFIX §37.3)
	receiveWindowByteCount atomic.Uint64
	receiveWindowSet       atomic.Bool
	// When the first advertisement arrived, which is when the window steps
	// from the blind bet to the peer's capacity. The delivery cap is lagged
	// against it: delivery measured before the step was measured at the
	// smaller window and must not be allowed to drag the window back down
	// (THROUGHPUTFIX §37.21).
	receiveWindowSetAtNanos atomic.Int64
	// whether any acknowledgement has arrived at all, which is what separates
	// a sender that is still blind from one whose peer does not advertise
	ackSeen atomic.Bool
	// Sequence numbers the receiver says it evicted, handed over from the ack
	// worker for the sequence goroutine to act on. The item state an eviction
	// changes belongs to the sequence goroutine, so the notice crosses here
	// under a leaf lock rather than being applied where it arrives.
	pendingEvictedMutex    sync.Mutex
	pendingEvictedSequence []uint64
	// laneAcks is the highest acknowledged sequence number per carrier route,
	// a fixed array scanned linearly since a snapshot has a handful of
	// routes, reset on a route generation change. It is what lets the
	// recovery path tell an endpoint drop from a queue or a stall without an
	// estimate (FLIGHTGATEFIX §26.2).
	laneAcks          [laneAckSlotCount]laneAckSlot
	laneAckGeneration uint64
	// The reliable lane's silence, exported for the campaign (§27.1). The
	// stallOnset fields hold the reading taken at the first firing of the
	// current silence; it is committed when that silence ends and proves to
	// be the longest of the run.
	sequenceStartTime     time.Time
	lastReliableAckTime   time.Time
	stallOnsetTaken       bool
	stallOnsetInterval    time.Duration
	stallOnsetOutstanding int
	stallOnsetOffset      time.Duration
	// lastCumulativeAckTime is when the cumulative ack last advanced; an RTO
	// inside one scaled RTT of it is counted as spurious (M4).
	lastCumulativeAckTime               time.Time
	contractMultiRouteWriterDestination TransferPath
	contractMultiRouteWriterAlias       TransferPath
	removeContractMultiRouteWriterAlias func()

	contractSeqIndex uint64

	// session is the per-peer TLS session shared by every local SendSequence
	// and ReceiveSequence to the same peer/stream. Acquired from the
	// `EncryptionSessionManager` at construction; released when the sequence
	// terminates. Nil when encryption is disabled on this client.
	session *peerEncryptionSession
}

func NewSendSequence(
	ctx context.Context,
	client *Client,
	sendBuffer *SendBuffer,
	destinationId Id,
	intermediaryIds MultiHopId,
	companionContract bool,
	forceStream bool,
	networkPeer bool,
	encryptionRole sequenceTlsRole,
	encryptionCompanion bool,
	sendBufferSettings *SendBufferSettings) *SendSequence {
	return newSendSequenceWithLogicalLane(
		ctx,
		client,
		sendBuffer,
		destinationId,
		intermediaryIds,
		companionContract,
		forceStream,
		networkPeer,
		encryptionRole,
		encryptionCompanion,
		0,
		sendBufferSettings,
		nil,
	)
}

func newSendSequenceWithLogicalLane(
	ctx context.Context,
	client *Client,
	sendBuffer *SendBuffer,
	destinationId Id,
	intermediaryIds MultiHopId,
	companionContract bool,
	forceStream bool,
	networkPeer bool,
	encryptionRole sequenceTlsRole,
	encryptionCompanion bool,
	logicalLane uint32,
	sendBufferSettings *SendBufferSettings,
	logicalLaneResendBudget *TransferMemoryBudget,
) *SendSequence {
	cancelCtx, cancel := context.WithCancel(ctx)
	contractCtx, cancelContractCtx := context.WithCancel(cancelCtx)
	sequenceBufferSize := logicalLaneSequenceBufferSize(
		sendBufferSettings.SequenceBufferSize,
		logicalLane,
	)
	ackBufferSize := logicalLaneSequenceBufferSize(
		sendBufferSettings.AckBufferSize,
		logicalLane,
	)
	resendQueueBudget := sendBufferSettings.ResendQueueBudget
	resendQueueMinByteCount := sendBufferSettings.ResendQueueMinByteCount
	if logicalLane != 0 {
		// the lane's own floor, which is zero unless one is configured
		resendQueueMinByteCount = max(0, sendBufferSettings.LaneFloorByteCount)
		if resendQueueBudget == nil {
			resendQueueBudget = logicalLaneResendBudget
		}
	}

	rttWindow := NewRttWindow(
		client.log,
		sendBufferSettings.RttWindowSize,
		sendBufferSettings.RttWindowTimeout,
		sendBufferSettings.RttScale,
		sendBufferSettings.MinResendInterval,
		sendBufferSettings.RttMinResendInterval,
		sendBufferSettings.MaxResendInterval,
	)

	var deliveredBytes []deliveredBytesSample
	if sendBufferSettings.ReliableAdmissionBoundedByDelivery ||
		0 < sendBufferSettings.DeliverySizedWindowScale {
		// the ring is the instrument both rules read, in opposite directions:
		// one bounds admission below the queue, the other raises the queue
		// above its constant
		deliveredBytes = make([]deliveredBytesSample, deliveredBytesRingSize)
	}

	seq := &SendSequence{
		sequenceStartTime:              time.Now(),
		deliveredBytes:                 deliveredBytes,
		ctx:                            cancelCtx,
		cancel:                         cancel,
		done:                           make(chan struct{}),
		client:                         client,
		sendBuffer:                     sendBuffer,
		log:                            client.log,
		destination:                    destinationId,
		intermediaryIds:                intermediaryIds,
		contractContext:                contractCtx,
		cancelContractContext:          cancelContractCtx,
		contractMetadataGeneration:     0,
		companionContract:              companionContract,
		forceStream:                    forceStream,
		logicalLane:                    logicalLane,
		networkPeer:                    networkPeer,
		encryptionRole:                 encryptionRole,
		encryptionCompanion:            encryptionCompanion,
		sequenceId:                     NewId(),
		sendBufferSettings:             sendBufferSettings,
		sendContract:                   nil,
		sendContractAcked:              false,
		sendContractMetadataGeneration: 0,
		openSendContracts:              map[Id]*sequenceContract{},
		packs:                          make(chan *SendPack, sequenceBufferSize),
		packAdmission:                  newSendPackAdmission(sequenceBufferSize),
		acks:                           make(chan receiveAckMessage, ackBufferSize),
		ackWindow:                      newSequenceAckWindow(),
		resendQueue:                    newResendQueue(resendQueueBudget, resendQueueMinByteCount),
		sendItems:                      []*sendItem{},
		nextSequenceNumber:             0,
		flightController:               newSendFlightController(sendBufferSettings),
		idleCondition:                  NewIdleCondition(),
		rttWindow:                      rttWindow,
		contractSeqIndex:               0,
	}
	// Never encrypt control-plane traffic. A SendSequence's data source is
	// always this client (sourceId == client.ClientId()) and its destination
	// is destination.DestinationId; when `SendNoSession` holds for either
	// endpoint, no session is acquired and traffic flows in plaintext.
	if client != nil && client.encryptionSessionManager != nil &&
		!client.encryptionSessionManager.SendNoSession(destinationId) {
		// Acquire the (peer, encryptionRole) session. A client-role send
		// sequence restarts the handshake (recovery: every new client send
		// re-initiates, rebuilding a peer's lost responder session); a
		// server-role send sequence (EncryptedControl carrier / server
		// reply) never restarts.
		seq.session = client.encryptionSessionManager.acquireForLogicalLaneSend(
			destinationId,
			encryptionRole,
			encryptionCompanion,
			forceStream,
			networkPeer,
			logicalLane,
		)
	}
	return seq
}

// sendContractMetadata is one immutable view of the contract-acquisition
// route hint. The context is canceled when a direct hint is promoted, which
// wakes a TakeContract still waiting on the obsolete queue generation.
type sendContractMetadata struct {
	key        ContractKey
	ctx        context.Context
	generation uint64
}

// contractMetadataWithLock builds a snapshot while contractStateLock is held.
func (self *SendSequence) contractMetadataWithLock() sendContractMetadata {
	return sendContractMetadata{
		key: ContractKey{
			Destination:         DestinationId(self.destination),
			IntermediaryIds:     self.intermediaryIds,
			CompanionContract:   self.companionContract,
			ForceStream:         self.forceStream,
			NetworkPeer:         self.networkPeer,
			EncryptionRole:      self.encryptionRole,
			EncryptionCompanion: self.encryptionCompanion,
			LogicalLane:         self.logicalLane,
		},
		ctx:        self.contractContext,
		generation: self.contractMetadataGeneration,
	}
}

// contractMetadata returns one internally consistent acquisition snapshot.
func (self *SendSequence) contractMetadata() sendContractMetadata {
	self.contractStateLock.Lock()
	defer self.contractStateLock.Unlock()
	return self.contractMetadataWithLock()
}

// contractIntermediaryIds returns the current route hint for diagnostics.
func (self *SendSequence) contractIntermediaryIds() MultiHopId {
	return self.contractMetadata().key.IntermediaryIds
}

// adoptContractIntermediaryIds promotes a direct-created sequence to the first
// explicit multihop route. Empty direct packs never erase that route, and
// alternate explicit routes wait for a new sequence instead of continually
// invalidating one live sequence's contract queue.
func (self *SendSequence) adoptContractIntermediaryIds(intermediaryIds MultiHopId) {
	if intermediaryIds.Len() == 0 {
		return
	}

	var previousMetadata sendContractMetadata
	var cancelPreviousContext context.CancelFunc
	self.contractStateLock.Lock()
	if self.intermediaryIds.Len() != 0 {
		self.contractStateLock.Unlock()
		return
	}
	previousMetadata = self.contractMetadataWithLock()
	cancelPreviousContext = self.cancelContractContext
	self.intermediaryIds = intermediaryIds
	self.contractContext, self.cancelContractContext = context.WithCancel(self.ctx)
	self.contractMetadataGeneration += 1
	self.contractStateLock.Unlock()

	// Wake an acquisition blocked on the direct key before force-removing its
	// queue. Flush closes queued contracts, resets its used-id history, and
	// drains the old generation so no waiter can consume it after promotion.
	cancelPreviousContext()
	self.client.ContractManager().FlushContractQueue(previousMetadata.key, true)
}

// id reconstructs the immutable logical lookup identity. Contract path
// metadata deliberately stays off this key so all routes to one peer/lane
// share one ordered sequence.
func (self *SendSequence) id() sendSequenceId {
	return sendSequenceId{
		Destination:         self.destination,
		CompanionContract:   self.companionContract,
		ForceStream:         self.forceStream,
		EncryptionRole:      self.encryptionRole,
		EncryptionCompanion: self.encryptionCompanion,
		LogicalLane:         self.logicalLane,
	}
}

func (self *SendSequence) ResendQueueSizeAndMessageTypes() (int, ByteCount, Id, []protocol.MessageType) {
	unpackMessageTypes := func(item *sendItem) any {
		var messageTypes []protocol.MessageType
		var transferFrame protocol.TransferFrame
		err := proto.Unmarshal(item.transferFrameBytes, &transferFrame)
		if err == nil && transferFrame.Pack != nil {
			for _, frame := range transferFrame.Pack.Frames {
				messageTypes = append(messageTypes, frame.MessageType)
			}
		}
		return messageTypes
	}
	count, byteSize, summary := self.resendQueue.QueueSizeAndSummary(unpackMessageTypes)
	var messageTypes []protocol.MessageType
	for _, summaryMessageTypes := range summary {
		messageTypes = append(messageTypes, summaryMessageTypes.([]protocol.MessageType)...)
	}
	return count, byteSize, self.sequenceId, messageTypes
}

// acquirePackAdmission spends the same caller timeout as channel admission.
// The returned timeout is the remaining budget for the channel handoff.
func (self *SendSequence) acquirePackAdmission(
	sendPack *SendPack,
	timeout time.Duration,
) (bool, error, time.Duration) {
	if self.packAdmission == nil {
		return true, nil, timeout
	}
	startTime := time.Now()
	var timer *time.Timer
	var timeoutChannel <-chan time.Time
	if 0 < timeout {
		timer = time.NewTimer(timeout)
		timeoutChannel = timer.C
		defer timer.Stop()
	}
	for {
		admissionKey := sendPack.schedulingKey
		if !self.flowIsolation.Load() {
			// A carrier without explicit flow isolation gets the complete legacy
			// admission capacity; only H3 reserves one slot for another flow.
			admissionKey = sendSchedulingKey{}
		}
		acquired, closed, notify := self.packAdmission.tryAcquire(admissionKey)
		if acquired {
			sendPack.admission = self.packAdmission
			sendPack.admissionKey = admissionKey
			if 0 < timeout {
				timeout = max(time.Duration(0), timeout-time.Since(startTime))
			}
			return true, nil, timeout
		}
		if closed {
			return false, errors.New("Done."), timeout
		}
		if timeout == 0 {
			return false, nil, timeout
		}
		select {
		case <-sendPack.Ctx.Done():
			return false, errors.New("Done."), timeout
		case <-self.ctx.Done():
			return false, errors.New("Done."), timeout
		case <-notify:
		case <-timeoutChannel:
			return false, nil, timeout
		}
	}
}

// success, error
func (self *SendSequence) Pack(sendPack *SendPack, timeout time.Duration) (bool, error) {
	self.packMutex.RLock()
	defer self.packMutex.RUnlock()

	select {
	case <-sendPack.Ctx.Done():
		return false, errors.New("Done.")
	case <-self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}

	if !self.idleCondition.UpdateOpen() {
		return false, errors.New("Done.")
	}
	defer self.idleCondition.UpdateClose()

	// A handshake/control pack can create this logical sequence before the
	// application supplies its explicit multihop route. Promote the shared
	// sequence before enqueueing so every subsequent contract request uses the
	// routable metadata; a direct pack is deliberately a no-op here.
	self.adoptContractIntermediaryIds(sendPack.IntermediaryIds)
	// Fail-closed entry gate (EncryptionModeRequired): an application pack does
	// not enter the sequence until the per-peer cipher is established. The gate
	// must run here — before a sequence number is assigned — because the
	// client-role handshake rides this same sequence
	// (`SendBuffer.SendEncryptedControl`): holding or dropping an
	// already-sequenced plaintext frame leaves a gap in the strictly-ordered
	// receive side, and the ClientHello queued behind the gap is never
	// delivered (the optimistic receive path deliberately skips initial
	// ClientHellos), deadlocking the very handshake that would clear the gate.
	// At entry, handshake controls (ForceUnwrapped) pass freely and claim the
	// first sequence numbers; application data waits within the caller's
	// timeout budget and is refused — unsent, never plaintext — if
	// establishment outlasts the budget. Holding the idle condition open while
	// waiting keeps the sequence (and the session it references) alive through
	// the establishment it is waiting on.
	if !sendPack.ForceUnwrapped && self.session != nil && self.session.RequireEncryption() {
		enterTime := time.Now()
		blockedNotified := false
		for self.session.Cipher() == nil {
			if timeout == 0 {
				// non-blocking contract: refuse rather than wait. The typed
				// error lets callers distinguish "encryption not established"
				// from transport backpressure (`false, nil`).
				self.session.NotifyRequiredSendBlocked(
					"application send refused: session not established",
				)
				return false, ErrEncryptionRequiredNotEstablished
			}
			if 0 < timeout && timeout <= time.Since(enterTime) {
				self.session.NotifyRequiredSendBlocked(fmt.Sprintf(
					"application send refused: session not established within %s",
					timeout,
				))
				return false, ErrEncryptionRequiredNotEstablished
			}
			// A wait that outlives the establishment bound is surfaced even
			// though the caller keeps waiting (e.g. an infinite-timeout Send):
			// past TlsTimeout the establishment attempts are failing and
			// retrying on cooldowns, which an operator watching events should
			// see without waiting for the caller to give up.
			if tlsTimeout := self.session.TlsTimeoutSetting(); !blockedNotified &&
				0 < tlsTimeout && tlsTimeout <= time.Since(enterTime) {
				blockedNotified = true
				self.session.NotifyRequiredSendBlocked(fmt.Sprintf(
					"application send waiting past establishment bound %s",
					tlsTimeout,
				))
			}
			// A waiting send must also drive re-establishment: the parked Pack
			// holds the idle condition open, so the sequence never idles out
			// and `AcquireForSend` (the only other restart trigger) never runs
			// again. Without this nudge a failed first epoch would leave the
			// send parked forever with nothing retrying the handshake. The
			// restart is internally guarded — a no-op while an establishment
			// is in flight or the initial-retry cooldown holds — and only the
			// client role may initiate (the server role follows the peer's
			// ClientHello).
			if self.encryptionRole == sequenceTlsRoleClient {
				self.session.restartHandshake()
			}
			select {
			case <-sendPack.Ctx.Done():
				return false, errors.New("Done.")
			case <-self.ctx.Done():
				return false, errors.New("Done.")
			case <-time.After(self.session.RequiredCipherPollInterval()):
				// re-check the cipher; establishment is bounded by TlsTimeout
			}
		}
		if 0 < timeout {
			// spend the remaining budget on the enqueue; a fully consumed
			// budget degrades to the non-blocking fast path below
			timeout = max(time.Duration(0), timeout-time.Since(enterTime))
		}
	}

	admitted, err, timeout := self.acquirePackAdmission(sendPack, timeout)
	if err != nil || !admitted {
		return false, err
	}
	queued := false
	defer func() {
		if !queued {
			sendPack.releaseAdmission()
		}
	}()

	// fast path without arming a timer
	select {
	case self.packs <- sendPack:
		queued = true
		return true, nil
	default:
	}

	if timeout < 0 {
		select {
		case <-sendPack.Ctx.Done():
			return false, errors.New("Done.")
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		case self.packs <- sendPack:
			queued = true
			return true, nil
		}
	} else if timeout == 0 {
		select {
		case <-sendPack.Ctx.Done():
			return false, errors.New("Done.")
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		case self.packs <- sendPack:
			queued = true
			return true, nil
		default:
			return false, nil
		}
	} else {
		select {
		case <-sendPack.Ctx.Done():
			return false, errors.New("Done.")
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		case self.packs <- sendPack:
			queued = true
			return true, nil
		case <-time.After(timeout):
			return false, nil
		}
	}
}

type receiveAckMessage struct {
	messageId                        Id
	sequenceId                       Id
	missingContractId                Id
	tag                              sequenceTag
	logicalLaneVersion               uint32
	selective                        bool
	contractMissing                  bool
	compactContractRecoverySupported bool
	// what the receiver said it can still hold out of order; unset is a
	// legacy peer, and zero is a receiver that is currently full. A receive
	// hold is never near four gibibytes, so this is the narrow type and packs
	// against the tail rather than adding a word of its own.
	receiveWindowSet       bool
	receiveWindowByteCount uint32
	// Set when the receiver removed items from its hold after acknowledging
	// them. A pointer rather than a slice so this struct stays comparable and
	// so the common case, which is every acknowledgement that evicts nothing,
	// costs one word.
	evictions *ackEvictionNotice
}

// The items a receiver removed from its hold after acknowledging them.
type ackEvictionNotice struct {
	sequenceNumbers []uint64
}

type receiveAckHandoffResult uint8

const (
	receiveAckHandoffAccepted receiveAckHandoffResult = iota
	receiveAckHandoffAcceptedAfterWait
	receiveAckHandoffQueueFull
	receiveAckHandoffQueueWaitTimeout
	receiveAckHandoffSequenceMissing
	receiveAckHandoffSequenceClosed
)

func receiveAckMessageFromProtocol(ack *protocol.Ack) (receiveAckMessage, error) {
	if ack == nil {
		return receiveAckMessage{}, errors.New("Missing ACK.")
	}
	messageId, err := IdFromBytes(ack.MessageId)
	if err != nil {
		return receiveAckMessage{}, err
	}
	sequenceId, err := IdFromBytes(ack.SequenceId)
	if err != nil {
		return receiveAckMessage{}, err
	}
	receiveAck := receiveAckMessage{
		messageId:                        messageId,
		sequenceId:                       sequenceId,
		tag:                              sequenceTagFromProtocol(ack.Tag),
		logicalLaneVersion:               ack.LogicalLaneVersion,
		selective:                        ack.Selective,
		compactContractRecoverySupported: ack.CompactContractRecovery,
	}
	if ack.ReceiveWindowByteCount != nil {
		receiveAck.receiveWindowByteCount = uint32(min(*ack.ReceiveWindowByteCount, math.MaxUint32))
		receiveAck.receiveWindowSet = true
	}
	if 0 < len(ack.EvictedSequenceNumbers) {
		receiveAck.evictions = &ackEvictionNotice{
			sequenceNumbers: ack.EvictedSequenceNumbers,
		}
	}
	if 0 < len(ack.MissingContractId) {
		receiveAck.missingContractId, err = IdFromBytes(ack.MissingContractId)
		if err != nil {
			return receiveAckMessage{}, err
		}
		receiveAck.contractMissing = true
	}
	return receiveAck, nil
}

func (self *SendSequence) Ack(ack *protocol.Ack, timeout time.Duration) (bool, error) {
	receiveAck, err := receiveAckMessageFromProtocol(ack)
	if err != nil {
		return false, err
	}
	return self.ackMessage(receiveAck, timeout)
}

func (self *SendSequence) ackMessage(
	ack receiveAckMessage,
	timeout time.Duration,
) (bool, error) {
	result, err := self.ackMessageDetailed(ack, timeout)
	return result == receiveAckHandoffAccepted ||
		result == receiveAckHandoffAcceptedAfterWait, err
}

func (self *SendSequence) ackMessageDetailed(
	ack receiveAckMessage,
	timeout time.Duration,
) (receiveAckHandoffResult, error) {
	self.ackMutex.Lock()
	defer self.ackMutex.Unlock()

	if self.sequenceId != ack.sequenceId {
		// ack is for a different send sequence that no longer exists
		return receiveAckHandoffSequenceMissing, nil
	}

	select {
	case <-self.ctx.Done():
		return receiveAckHandoffSequenceClosed, errors.New("Done.")
	default:
	}

	// fast path without arming a timer
	select {
	case self.acks <- ack:
		return receiveAckHandoffAccepted, nil
	default:
	}

	// The ACK worker already folds cumulative and selective progress into one
	// allocation-free window. If its compact handoff channel is momentarily
	// full, publish this ACK to that same window instead of dropping progress or
	// retaining the carrier reader in a timed wait. Older queued cumulative ACKs
	// can arrive afterward safely: sequenceAckWindow is monotonic and absorbs
	// stale heads while preserving selective and contract-recovery state.
	if self.ackWindow != nil && self.resendQueue != nil {
		self.coalesceReceivedAck(self.ackWindow, ack)
		return receiveAckHandoffAccepted, nil
	}

	if timeout < 0 {
		select {
		case <-self.ctx.Done():
			return receiveAckHandoffSequenceClosed, errors.New("Done.")
		case self.acks <- ack:
			return receiveAckHandoffAcceptedAfterWait, nil
		}
	} else if timeout == 0 {
		select {
		case <-self.ctx.Done():
			return receiveAckHandoffSequenceClosed, errors.New("Done.")
		case self.acks <- ack:
			return receiveAckHandoffAccepted, nil
		default:
			return receiveAckHandoffQueueFull, nil
		}
	} else {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-self.ctx.Done():
			return receiveAckHandoffSequenceClosed, errors.New("Done.")
		case self.acks <- ack:
			return receiveAckHandoffAcceptedAfterWait, nil
		case <-timer.C:
			return receiveAckHandoffQueueWaitTimeout, nil
		}
	}
}

// coalesceReceivedAck performs the ACK worker's bounded validation and folds
// one live ACK into the shared monotonic window. It is safe from either the
// worker or a saturated receive callback: resendQueue and sequenceAckWindow
// provide their own short critical sections, and all counters are atomic.
func (self *SendSequence) coalesceReceivedAck(
	ackWindow *sequenceAckWindow,
	ack receiveAckMessage,
) {
	// Before the pending check: an eviction notice rides whatever
	// acknowledgement is next, which may be one whose own message this sender
	// has already released.
	self.observeEvictions(ack)
	sequenceNumber, ok := self.resendQueue.ContainsMessageId(ack.messageId)
	if !ok {
		return
	}
	if self.sendBuffer != nil {
		self.sendBuffer.observeLogicalLaneVersion(
			self,
			ack.logicalLaneVersion,
		)
	}
	self.observeReceiveWindowAdvertisement(ack)
	if ack.compactContractRecoverySupported && self.client != nil {
		self.client.compactRecoveryAckCount.Add(1)
	}
	sequenceAck := sequenceAck{
		messageId:                        ack.messageId,
		sequenceNumber:                   sequenceNumber,
		selective:                        ack.selective,
		tag:                              ack.tag,
		compactContractRecoverySupported: ack.compactContractRecoverySupported,
	}
	if ack.contractMissing {
		sequenceAck.contractMissing = true
		sequenceAck.missingContractId = ack.missingContractId
		ackWindow.UpdateContractMissing(sequenceAck)
		return
	}
	ackWindow.Update(sequenceAck)
	if self.sendBuffer != nil && self.sendBuffer.afterAckCoalescedForTest != nil {
		self.sendBuffer.afterAckCoalescedForTest(self.id(), sequenceNumber)
	}
}

// processLogicalGroupChunk materializes at most one transport-safe wire Pack.
// Returning one chunk per Run iteration preserves resend-budget backpressure;
// the retained SendPack cursor remains the sole owner of every later frame.
func (self *SendSequence) processLogicalGroupChunk(
	sendPack *SendPack,
	withoutAckPromotion bool,
	flightPolicy transferFlightPolicySnapshot,
) (complete bool, success bool, deferForRecoveryAdmission bool) {
	// A new sequence has no writer when the scheduler takes its first Pack, so
	// the loop's initial policy snapshot is necessarily conservative. Open only
	// at the point where a logical group is actually ready to send, then refresh
	// the chunk policy. This puts H1 grouping on the first response burst without
	// allocating a selector for singleton sequences still blocked on admission.
	if self.contractMultiRouteWriter == nil && self.sendBuffer != nil {
		self.openContractMultiRouteWriter()
		flightPolicy = self.transferFlightPolicy()
	}
	self.pinLogicalGroupChunkLimits(sendPack, flightPolicy)
	maxFrames, maxMessageByteCount := sendPack.groupChunkLimits()
	start := sendPack.groupFrameIndex
	end := nextSendGroupChunkEndWithLimits(
		sendPack.Frames,
		start,
		maxFrames,
		maxMessageByteCount,
	)
	frames := sendPack.Frames[start:end]
	messageByteCount := MessageByteCount(frames)
	contractUpdated := false
	var contractErr error
	if withoutAckPromotion {
		contractUpdated, deferForRecoveryAdmission, contractErr =
			self.updateContractWithoutAckPromotionOutcome(messageByteCount)
	} else {
		contractUpdated, contractErr = self.updateContractOutcome(messageByteCount)
	}
	if deferForRecoveryAdmission {
		return false, true, true
	}
	if !contractUpdated {
		err := self.classifyContractCreationFailure(contractErr)
		sendPack.disposeUnsentGroup(err)
		return true, false, false
	}
	if withoutAckPromotion {
		self.client.unreliableNoAckAdmissionBypassCount.Add(1)
	}
	if sendPack.groupCompletion == nil && end == len(sendPack.Frames) {
		self.sendRecordForSchedulingKey(
			frames,
			sendPack.ackRecord(),
			sendPack.noAckRecord(),
			sendPack.Ack,
			sendPack.ForceUnwrapped,
			sendPack.schedulingKey,
		)
		sendPack.groupFrameIndex = end
		sendPack.releaseRaw()
		return true, true, false
	}
	if sendPack.groupCompletion == nil {
		chunkCount := sendGroupChunkCountWithLimits(
			sendPack.Frames,
			maxFrames,
			maxMessageByteCount,
		)
		sendPack.groupCompletion = newSendGroupCompletion(sendPack, chunkCount)
		if self.sendBuffer != nil &&
			self.sendBuffer.afterCreateSendGroupCompletionForTest != nil {
			self.sendBuffer.afterCreateSendGroupCompletionForTest(self.id(), chunkCount)
		}
	}
	self.sendRecordForSchedulingKey(
		frames,
		sendPack.groupCompletion.chunkAckRecord(),
		sendPack.groupCompletion.chunkNoAckRecord(),
		sendPack.Ack,
		sendPack.ForceUnwrapped,
		sendPack.schedulingKey,
	)
	sendPack.groupFrameIndex = end
	if end < len(sendPack.Frames) {
		return false, true, false
	}
	sendPack.releaseRaw()
	return true, true, false
}

// Uses receiver evidence to schedule a small scoreboard of missing Packs. Three
// or more distinct later selective ACKs distinguish each hole from ordinary
// reordering, and a burst cap leaves QUIC responsible for packet pacing. The
// return value reports whether at least one scheduled gap still occupies the
// unreliable flight, so a reliable-stream gap cannot contract the DATAGRAM
// window. If no Pack is missing but the oldest item remains selectively
// acknowledged, one RTT-paced duplicate solicits the cumulative ACK that may
// have been lost.
func (self *SendSequence) scheduleSelectiveAckRecovery(currentTime time.Time) bool {
	reschedule := func(item *sendItem, resendTime time.Time, recoveryKind sendRecoveryKind) {
		removed := self.resendQueue.RemoveByMessageId(item.messageId)
		if removed != item {
			panic(errors.New("Missing selective recovery item"))
		}
		item.resendTime = resendTime
		item.recoveryKind = recoveryKind
		self.resendQueue.Add(item)
	}

	selectiveAckCount := 0
	// §26.2: acknowledgements counted per carrier route, so a hole the
	// reliable lane carried is proven only by later acknowledgements from
	// its own route. A reliable carrier retransmits below Transfer, so an
	// acknowledgement that overtook this hole on another lane says nothing
	// about it.
	laneAckCounts := [laneAckSlotCount]int{}
	laneRuleActive := self.sendBufferSettings.ReliableLaneProvenRecovery
	for _, item := range self.sendItems {
		if item != nil && item.selectiveAcked {
			selectiveAckCount += 1
			if laneRuleActive && !item.carrierChanged {
				if slot := self.laneSlotFor(item.carrierRoute); 0 <= slot {
					laneAckCounts[slot] += 1
				}
			}
		}
	}

	var firstItem *sendItem
	var gapItem *sendItem
	threshold := self.sendBufferSettings.SelectiveAckGapThreshold
	burstSize := self.sendBufferSettings.SelectiveAckGapBurstSize
	gapRecoveryCount := 0
	unreliableGapRecovery := false
	remainingSelectiveAckCount := selectiveAckCount
	for _, item := range self.sendItems {
		if item == nil {
			continue
		}
		if firstItem == nil {
			firstItem = item
		}
		if item.selectiveAcked {
			remainingSelectiveAckCount -= 1
			if laneRuleActive && !item.carrierChanged {
				if slot := self.laneSlotFor(item.carrierRoute); 0 <= slot {
					laneAckCounts[slot] -= 1
				}
			}
			continue
		}
		if gapItem == nil {
			gapItem = item
		}
		// Mixed lanes: an item carried by the reliable route is routinely
		// acknowledged after later items that rode a direct datagram lane
		// with a fraction of its latency. Until it is older than the reliable
		// lane's RTT it is late, not lost; a gap resend now only doubles the
		// relay traffic. Its own RTO still covers a real loss.
		lateNotLost := self.flightController != nil && self.flightController.limited &&
			item.reliableCarrierObserved && !item.unreliableFlightTracked &&
			currentTime.Before(item.sendTime.Add(self.rttWindow.ScaledRtt()))
		// §26.2: a hole the reliable lane carried is proven by later
		// acknowledgements from its own route and by nothing else, and needs
		// no time grace, because the lane's own acknowledgements say whether
		// it was dropped at an endpoint or is merely queued behind.
		provingAckCount := remainingSelectiveAckCount
		laneLoneTail := false
		if laneRuleActive && self.laneProvenRecovery(item) {
			lateNotLost = false
			provingAckCount = 0
			if slot := self.laneSlotFor(item.carrierRoute); 0 <= slot {
				provingAckCount = laneAckCounts[slot]
			}
			// §29.3: the information to tell a dropped tail from a stalled
			// one exists at neither end, but the response does not need it,
			// because a lane has exactly one tail. A hole that is its route's
			// only outstanding item, with three later acknowledgements from
			// any lane, is probed at the window's minimum rather than waiting
			// for the head probe: the probe is written p2p-first, so with
			// room in the direct flight it heals through the direct lane, and
			// with none it costs one duplicate. The probe round trip never
			// exceeds the scaled round trip, so this is never later than
			// merged's own timer and is earlier whenever the relay's minimum
			// is under its mean.
			laneLoneTail = provingAckCount < threshold &&
				threshold <= remainingSelectiveAckCount &&
				self.laneLoneTail(item)
		}
		if laneLoneTail && item.ackTailProbeCount < self.sendBufferSettings.AckTailProbeLimit {
			probeTime := item.sendTime.Add(self.rttWindow.probeRtt(currentTime))
			if probeTime.Before(currentTime) {
				probeTime = currentTime
			}
			if probeTime.Before(item.resendTime) {
				item.ackTailProbeCount += 1
				self.selectiveGapRecoveryActive = true
				reschedule(item, probeTime, sendRecoveryAckTailProbe)
			}
			continue
		}
		if laneRuleActive && 0 < provingAckCount && provingAckCount < threshold &&
			item.recoveryKind == sendRecoveryNone && !item.selectiveGapRecovered &&
			self.laneProvenRecovery(item) {
			// §32.4: a hole its own lane has acknowledged past is an
			// endpoint drop, and its timer writes it at the next firing
			// (row 9: at most one interval past the proof). The
			// unconditional re-arm above must not stretch that interval,
			// so a proof below the gap rule's threshold schedules the
			// firing at the proof plus one interval when the item's
			// backed-off timer is later. The interval is the protection
			// §30.3 names: time for a hole below to fill and this item's
			// cumulative coverage to arrive.
			provenTime := currentTime.Add(self.resendIntervalForItem(item, 0))
			if provenTime.Before(item.resendTime) {
				reschedule(item, provenTime, sendRecoveryNone)
			}
		}
		// FLIGHTGATEFIX §23.2: the grace above ends exactly when the item's
		// own timer first comes due, so a deferral of that timer leaves the
		// scoreboard free to write what the timer declined. While the item
		// holds a deferral it is late, not lost, until the deferral expires.
		if !lateNotLost && self.sendBufferSettings.DeferredItemIsLateForTheScoreboard &&
			item.deferralOutstanding && currentTime.Before(item.resendTime) {
			lateNotLost = true
		}
		if 0 < threshold && gapRecoveryCount < burstSize && !lateNotLost &&
			!item.selectiveGapRecovered &&
			(item.ackTailProbeCount == 0 || item.recoveryKind != sendRecoveryNone) &&
			threshold <= provingAckCount {
			item.selectiveGapRecovered = true
			self.selectiveGapRecoveryActive = true
			reschedule(item, currentTime, sendRecoverySelectiveGap)
			gapRecoveryCount += 1
			unreliableGapRecovery = unreliableGapRecovery || item.unreliableFlightTracked
		}
	}
	if 0 < gapRecoveryCount {
		return unreliableGapRecovery
	}

	if firstItem == nil || !firstItem.selectiveAcked {
		if !self.selectiveGapRecoveryActive || gapItem == nil ||
			self.sendBufferSettings.AckTailProbeLimit <= gapItem.ackTailProbeCount {
			return false
		}
		probeTime := currentTime.Add(self.rttWindow.probeRtt(currentTime))
		if probeTime.Before(gapItem.resendTime) {
			gapItem.ackTailProbeCount += 1
			reschedule(gapItem, probeTime, sendRecoveryAckTailProbe)
		}
		return false
	}
	probeTime := firstItem.sendTime.Add(self.rttWindow.probeRtt(currentTime))
	if probeTime.Before(currentTime) {
		probeTime = currentTime
	}
	if probeTime.Before(firstItem.resendTime) {
		reschedule(firstItem, probeTime, sendRecoveryCumulativeProbe)
	}
	return false
}

// A successful reliable-carrier write is normally left to that carrier's own
// recovery for one full Transfer resend interval. If that exact route is
// withdrawn, its stream can no longer recover bytes accepted by the retired
// connection. Move every still-unacknowledged item from that route to the
// front immediately so a parallel or replacement route can take ownership.
//
// Merely publishing another equal-priority route is not sufficient evidence:
// the original route may still be draining normally.
//
// THROUGHPUTFIX §37.17's second guard, and a correction to what this function
// used to assume. Selectively acknowledged items were excluded here because
// "the receiver already proved delivery". It did not: a selective
// acknowledgement says the receiver is holding the item out of order, and a
// hold that must admit an earlier arrival removes a later held item without
// telling anyone (§37.16). The sender's mark then survives for
// SelectiveAckTimeout, sixty seconds, and every resend path skips a marked
// item, so the transfer stalls on bytes both ends believe are in hand.
//
// A route death is exactly when that happens: the dead route's items are
// resent, each is earlier than what the hold accumulated past the hole, and
// each admission evicts. So a carrier change voids the selective
// acknowledgements the dead route earned, and a minute becomes a round trip.
// The cost is redundant: one window per route death at worst, and a duplicate
// of an item the receiver still holds is discarded by message id. This is the
// guard that can be deployed on providers alone, protecting clients that are
// never updated.
func (self *SendSequence) scheduleRetiredReliableCarrierRecovery(
	currentTime time.Time,
) {
	provider, ok := self.contractMultiRouteWriter.(transferCarrierRouteStateProvider)
	if !ok {
		return
	}
	voidSelectiveAcks := self.sendBufferSettings.CarrierChangeVoidsSelectiveAck
	for _, item := range self.sendItems {
		if item == nil ||
			(item.selectiveAcked && !voidSelectiveAcks) ||
			!item.reliableCarrierObserved || item.reliableRoute == nil ||
			provider.transferRouteActive(item.reliableRoute) ||
			!currentTime.Before(item.resendTime) {
			continue
		}

		removed := self.resendQueue.RemoveByMessageId(item.messageId)
		if removed != item {
			panic(errors.New("Missing retired-carrier recovery item"))
		}
		// Consume the retirement evidence before the write. If no replacement
		// route is ready, the failed attempt returns to ordinary bounded RTO
		// recovery instead of spinning on the same retired route.
		item.reliableCarrierObserved = false
		item.reliableRoute = nil
		item.hybridReliableCarrierObserved = false
		if item.selectiveAcked {
			// the lease is void: the route that earned the acknowledgement is
			// gone, and the hold may have dropped the item to make room
			item.selectiveAcked = false
			if self.client != nil {
				self.client.carrierChangeSelectiveAckVoidCount.Add(1)
			}
		}
		item.resendTime = currentTime
		item.recoveryKind = sendRecoveryCarrierChange
		self.resendQueue.Add(item)
	}
}

func (self *SendSequence) preferH3AfterH1Timeout(item *sendItem) bool {
	if item == nil || item.recoveryKind != sendRecoveryNone ||
		!item.reliableCarrierObserved || item.reliableRoute == nil {
		return false
	}
	provider, ok := self.contractMultiRouteWriter.(transferCarrierH1TimeoutFailoverProvider)
	return ok && provider.transferPreferH3AfterH1Timeout(item.reliableRoute)
}

// Schedules one conservative follow-up when a receiver-proven gap write is
// itself lost. One minimum probe interval gives a successful recovery time to
// produce its cumulative ACK; the item then returns to ordinary RTO ownership.
func (self *SendSequence) scheduleGapRecoveryProbe(
	item *sendItem,
	currentTime time.Time,
	ackDeadline time.Time,
) bool {
	if item.gapFollowupScheduled ||
		self.sendBufferSettings.AckTailProbeLimit <= item.ackTailProbeCount ||
		!currentTime.Before(ackDeadline) {
		return false
	}

	probeInterval := self.rttWindow.probeRtt(currentTime)
	probeTime := currentTime.Add(probeInterval)
	if ackDeadline.Before(probeTime) {
		probeTime = ackDeadline
	}
	item.gapFollowupScheduled = true
	item.ackTailProbeCount += 1
	item.resendTime = probeTime
	item.recoveryKind = sendRecoveryAckTailProbe
	return true
}

// Implemented by the destination selector without expanding the public
// MultiRouteWriter contract. A custom writer therefore keeps historical
// unlimited behavior unless it explicitly exposes carrier semantics.
type transferFlightPolicyProvider interface {
	transferFlightPolicy() transferFlightPolicySnapshot
}

// Reads the current immutable route generation. Before the first route write
// opens a selector, or for a custom writer without policy support, admission is
// intentionally unchanged.
func (self *SendSequence) transferFlightPolicy() transferFlightPolicySnapshot {
	if provider, ok := self.contractMultiRouteWriter.(transferFlightPolicyProvider); ok {
		return provider.transferFlightPolicy()
	}
	return transferFlightPolicySnapshot{}
}

// ackTimeoutForPolicy keeps the ordinary reliable-carrier lifetime while
// allowing a carrier with no payload retransmission to recover through a
// longer burst-loss interval. The caller stores the result on each item so a
// later route change can extend, but never shorten, its established deadline.
func (self *SendSequence) ackTimeoutForPolicy(
	policy transferFlightPolicySnapshot,
) time.Duration {
	timeout := self.sendBufferSettings.AckTimeout
	if policy.limited && 0 < self.sendBufferSettings.UnreliableAckTimeout {
		timeout = max(timeout, self.sendBufferSettings.UnreliableAckTimeout)
	}
	return timeout
}

// resendIntervalForPolicy applies the existing per-item exponential backoff,
// with a lower ceiling only when the active route includes an unreliable
// payload carrier. Flight admission contracts independently on each timeout,
// which bounds duplicate queue pressure while this ceiling bounds silence.
func (self *SendSequence) resendIntervalForPolicy(
	policy transferFlightPolicySnapshot,
	sendCount int,
) time.Duration {
	maxInterval := self.sendBufferSettings.MaxResendInterval
	if policy.limited && 0 < self.sendBufferSettings.UnreliableMaxResendInterval {
		maxInterval = min(maxInterval, self.sendBufferSettings.UnreliableMaxResendInterval)
	}
	baseRtt := self.rttWindow.ScaledRtt()
	if self.sendBufferSettings.ReliableTimerUsesDeviation && !policy.limited {
		// §25.2: a reliable-carried item's timer follows the lane's own
		// spread. A direct-carried item keeps the scaled mean, since its cap
		// and its recovery are the unreliable flight's business.
		baseRtt = self.rttWindow.DeviationRtt()
	}
	interval := min(baseRtt, maxInterval)
	if shift := uint(min(max(sendCount-1, 0), 16)); 0 < shift {
		interval = min(interval<<shift, maxInterval)
	}
	return interval
}

// A message selected for a hybrid H3 reliable stream is already owned by
// QUIC's recovery. Keep the end-to-end Transfer ACK, but do not start a second
// rapid per-item retransmit train while that stream is draining. Ordinary H1
// retains its normal Transfer cadence; DATAGRAM-observed items retain the
// shorter unreliable cadence.
func (self *SendSequence) resendIntervalForItem(
	item *sendItem,
	sendCount int,
) time.Duration {
	if item.unreliableCarrierObserved {
		return self.resendIntervalForPolicy(item.unreliableRecoveryPolicy(), sendCount)
	}
	if item.hybridReliableCarrierObserved {
		return self.sendBufferSettings.MaxResendInterval
	}
	return self.resendIntervalForPolicy(transferFlightPolicySnapshot{}, sendCount)
}

// noAckPackCanBypassRecoveryAdmission reports whether this exact queued Pack
// is guaranteed to remain outside the resend queue and unreliable Transfer
// flight. A requested NoAck Pack is temporarily promoted to Ack while opening
// or rotating a contract, so only an already-acknowledged current contract (or
// an explicit no-contract destination) may use the bypass. A logical group is
// evaluated one exact bounded chunk at a time; a later chunk that crosses a
// contract boundary returns to ordinary admission before it is serialized.
func (self *SendSequence) noAckPackCanBypassRecoveryAdmission(
	sendPack *SendPack,
) bool {
	if sendPack == nil || sendPack.Ack {
		return false
	}
	if self.client.ContractManager().SendNoContract(self.destination) {
		return true
	}
	metadata := self.contractMetadata()
	return self.sendContract != nil && self.sendContractAcked &&
		self.sendContractMetadataGeneration == metadata.generation &&
		self.sendContract.canUpdate(sendPack.nextSerializedMessageByteCount())
}

func (self *SendSequence) Run() {
	ackWorkerDone := make(chan struct{})
	ackWorkerStarted := false
	defer func() {
		if r := recover(); r != nil {
			self.log.Errorf("[s]%s->%s...%s s(%s) abnormal exit =  %s\n", self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId, r)
			panic(r)
		}
	}()
	defer func() {
		self.cancel()
		if ackWorkerStarted {
			<-ackWorkerDone
		}

		// close contract
		for _, sendContract := range self.openSendContracts {
			self.client.ContractManager().CloseContract(
				sendContract.contractId,
				sendContract.ackedByteCount,
				sendContract.unackedByteCount,
			)
			// flush queued contracts for already sent contracts
			// contractKey = ContractKey{
			// 	Destination:       sendContract.path.DestinationMask(),
			// 	IntermediaryIds:   self.intermediaryIds,
			// 	CompanionContract: self.companionContract,
			// 	ForceStream:       self.forceStream,
			// }
			// self.client.ContractManager().FlushContractQueue(contractKey, true)
		}

		// drain the buffer, releasing any borrowed budget
		for _, item := range self.resendQueue.Clear() {
			item.acks.invoke(errors.New("Send sequence closed."))
			item.messagePoolReturn()
		}

		// flush queued contracts (used ids were closed above). Keyed by
		// (EncryptionRole, EncryptionCompanion) so this exit-flush doesn't discard
		// a peer-paired sequence's pending contracts — the EC carrier and normal
		// data are separate sequences to the same destination.
		self.client.ContractManager().FlushContractQueue(
			self.contractMetadata().key,
			true,
		)

		self.closeContractMultiRouteWriter()

		if self.session != nil {
			// No explicit close: a closing SendSequence must not tear down
			// the shared session (a concurrent ReceiveSequence may still be
			// using it) and must not emit anything on the wire. A future
			// initiator SendSequence resets the handshake when it resumes.
			self.session.Release()
		}
	}()

	self.prewarmOpeningContract()

	ackWindow := self.ackWindow
	if ackWindow == nil {
		// Directly constructed test sequences may predate constructor-owned ACK
		// state. Keep their historical channel-only handoff semantics; production
		// sequences publish the shared window before they are indexed.
		ackWindow = newSequenceAckWindow()
	}
	ackWorkerStarted = true
	go func() {
		defer close(ackWorkerDone)
		HandleError(func() {
			defer self.cancel()

			for {
				select {
				case <-self.ctx.Done():
					return
				case ack, ok := <-self.acks:
					if !ok {
						return
					}
					self.coalesceReceivedAck(ackWindow, ack)
				}
			}
		}, self.cancel)
	}()

	// reusable idle/resend timer: a per-iteration time.After would allocate a
	// timer per packet on this hot loop. created already-fired; the Reset before
	// each blocking select arms it (go1.23+ delivers no stale fire after Reset).
	idleTimer := time.NewTimer(0)
	defer idleTimer.Stop()

	scheduler := newSendPackScheduler()
	// FLIGHTGATEFIX §22: when the reliable admission bound first blocks, this
	// records the moment so the wait's duration is charged once.
	reliableAdmissionWaitStart := time.Time{}
	// FLIGHTGATEFIX §23.3: the route generation this sequence last saw, so a
	// change is counted beside the carrier-change writes it produces.
	lastRouteGeneration := uint64(0)
	disposeScheduledPack := func(sendPack *SendPack) {
		err := errors.New("Send sequence closed.")
		sendPack.disposeUnsentGroup(err)
	}
	defer scheduler.Drain(disposeScheduledPack)
	var processingPacks [sendPackH1GroupMaxFrames]*SendPack
	defer func() {
		for packIndex, sendPack := range processingPacks {
			if sendPack != nil {
				disposeScheduledPack(sendPack)
				processingPacks[packIndex] = nil
			}
		}
	}()
	packsClosed := false
	drainPacks := func() {
		for !packsClosed {
			select {
			case sendPack, ok := <-self.packs:
				if !ok {
					packsClosed = true
					return
				}
				scheduler.Push(sendPack)
			default:
				return
			}
		}
	}
sendSequenceLoop:
	for {
		flightPolicy := self.transferFlightPolicy()
		if flightPolicy.generation != lastRouteGeneration {
			if lastRouteGeneration != 0 {
				self.client.routeGenerationChangeCount.Add(1)
			}
			lastRouteGeneration = flightPolicy.generation
			// the same channel need not be the same carrier in a new
			// generation, and a retired carrier's recovery belongs to
			// scheduleRetiredReliableCarrierRecovery (FLIGHTGATEFIX §26.2)
			self.resetLaneAcks(flightPolicy.generation)
		}
		self.flowIsolation.Store(flightPolicy.flowIsolation)
		flightPolicyChanged := self.flightController.applyPolicy(flightPolicy)
		if flightPolicyChanged {
			self.client.observeUnreliableFlight(self.flightController)
		}
		if flightPolicy.flowIsolation {
			// H3 alone drains into the bounded per-flow scheduler so a newly
			// active flow can be discovered behind a saturated bulk flow.
			drainPacks()
		}

		// apply the acks
		ackSnapshot := ackWindow.Snapshot(true)
		ackUpdated := 0 < ackSnapshot.ackUpdateCount || 0 < len(ackSnapshot.selectiveAcks)
		if 0 < ackSnapshot.ackUpdateCount {
			self.lastHeadAckTime = time.Now()
			self.receiveAck(
				ackSnapshot.headAck.messageId,
				false,
				ackSnapshot.headAck.tag,
				ackSnapshot.headAck.compactContractRecoverySupported,
			)
		}
		for messageId, ack := range ackSnapshot.selectiveAcks {
			self.receiveAck(
				messageId,
				true,
				ack.tag,
				ack.compactContractRecoverySupported,
			)
		}
		for messageId, ack := range ackSnapshot.contractMissingAcks {
			self.receiveContractMissing(messageId, ack.missingContractId)
		}

		sendTime := time.Now()
		// before the recovery scans, so an evicted item is due on this pass
		self.resendEvicted(self.takePendingEvictions())
		if flightPolicyChanged {
			self.scheduleRetiredReliableCarrierRecovery(sendTime)
		}
		if ackUpdated && self.scheduleSelectiveAckRecovery(sendTime) {
			self.client.unreliableFlightGapCount.Add(1)
			if self.flightController.reduceForLoss() {
				self.client.unreliableFlightReductionCount.Add(1)
			}
			self.client.observeUnreliableFlight(self.flightController)
		}
		var timeout time.Duration

		if self.resendQueue.Len() == 0 {
			timeout = self.sendBufferSettings.IdleTimeout
		} else {
			timeout = self.sendBufferSettings.AckTimeout

			for {
				item := self.resendQueue.PeekFirst()
				if item == nil {
					break
				}

				// Once an item has traversed an unreliable payload carrier, retain
				// that longer recovery lifetime even if routes reform before its
				// acknowledgement. A later unreliable generation may extend an item
				// that began on a reliable route; no transition may shorten it.
				item.ackTimeout = max(
					item.ackTimeout,
					self.ackTimeoutForPolicy(item.unreliableRecoveryPolicy()),
				)
				retainPastAckTimeout := item.acks.retainPastAckTimeout()
				itemAckTimeout := item.sendTime.Add(item.ackTimeout).Sub(sendTime)
				if self.sendBuffer != nil && self.sendBuffer.forceAckTimeoutForTest != nil &&
					self.sendBuffer.forceAckTimeoutForTest(self.id()) {
					itemAckTimeout = 0
				}
				if itemAckTimeout <= 0 && !retainPastAckTimeout {
					// message took too long to ack
					// close the sequence
					if self.log.V(1).Enabled() {
						self.log.Infof(
							"[s]%s->%s...%s s(%s) exit ack timeout (%s) seq=%d sends=%d head=%t full_contract=%t compact_contract=%t promoted=%t selective=%t recovery=%d policy_limited=%t flight_limited=%t transport_write=%t pending=%d\n",
							self.client.ClientTag(),
							self.contractIntermediaryIds(),
							self.destination,
							self.contractMultiRouteWriterAlias.StreamId,
							item.ackTimeout,
							item.sequenceNumber,
							item.sendCount,
							item.head,
							item.hasContractFrame,
							item.contractId != nil && !item.hasContractFrame,
							item.promotedHead,
							item.selectiveAcked,
							item.recoveryKind,
							flightPolicy.limited,
							self.flightController.limited,
							item.transportWriteObserved,
							self.resendQueue.Len(),
						)
					}
					return
				}
				if !retainPastAckTimeout && itemAckTimeout < timeout {
					timeout = itemAckTimeout
				}
				if self.sendBuffer != nil && self.sendBuffer.forceResendForTest != nil &&
					self.sendBuffer.forceResendForTest(self.id()) {
					item.resendTime = sendTime
				}

				if sendTime.Before(item.resendTime) {
					itemResendTimeout := item.resendTime.Sub(sendTime)
					if itemResendTimeout < timeout {
						timeout = itemResendTimeout
					}
					break
				}
				if self.sendBuffer != nil && self.sendBuffer.beforeDueResendForTest != nil {
					self.sendBuffer.beforeDueResendForTest(self.id(), item.sequenceNumber)
				}
				// An Ack may have reached the coalescer after this iteration took
				// its snapshot. Apply that receiver evidence before an already-due
				// recovery write; otherwise a busy sender can emit one spurious
				// retransmit for every snapshot/arrival race. The lock is paid only
				// on the due-recovery path, never for an ordinary initial write.
				if ackWindow.PendingDispositionFor(item.sequenceNumber, item.messageId) {
					self.client.ackPendingResendPreemptCount.Add(1)
					continue sendSequenceLoop
				}
				self.preferH3AfterH1Timeout(item)
				self.resendQueue.RemoveByMessageId(item.messageId)

				// A selective recovery is receiver-paced evidence rather than an
				// RTO. Consume its marker before the write and do not increase the
				// item's timeout backoff; a lost recovery returns to its prior
				// ordinary cadence. Any resend awaits fresh acknowledgement state.
				recoveryKind := item.recoveryKind
				item.recoveryKind = sendRecoveryNone
				// §34.3: what this firing means is decided by this item's own
				// lane and by its position in it. Anything acknowledged above
				// it means write it; anything below it since it last looked
				// means the lane is draining toward it, so wait; neither means
				// write it if it is the lane's oldest unacknowledged item and
				// otherwise ride that head.
				laneVerdict := laneTimerNotApplicable
				if recoveryKind == sendRecoveryNone {
					laneVerdict = self.laneTimerVerdictFor(item)
					if laneVerdict != laneTimerNotApplicable {
						// this firing has now looked: the next one asks what
						// moved on this lane since
						if highest, acked := self.laneHighestAcked(item.carrierRoute); acked {
							item.laneAckedAtLastFiring = highest
						}
					}
					self.observeReliableLaneFiring(
						item,
						self.resendIntervalForItem(item, item.sendCount),
						sendTime,
					)
				}
				if laneVerdict == laneTimerEndpointDrop {
					// the route delivered past this item, so it was dropped at
					// an endpoint: written with backoff, as today
					self.client.laneProvenTimeoutWriteCount.Add(1)
				} else if laneVerdict == laneTimerSilent {
					// §34.3 rule 3. Nothing on this item's lane has moved
					// since it last looked, so no acknowledgement is coming to
					// prove it and none will: the receiver's own drops can
					// remove every later same-lane item, which is how the
					// proof chain breaks. The lane's oldest unacknowledged
					// item is written on its own timer, with setHead where it
					// is the sequence head, which is also the only path that
					// re-establishes a receiver that silently lost the
					// sequence. Everything else on the lane rides that head,
					// so one write recovers a dropped batch a position at a
					// time rather than a window at a time.
					head := self.laneOldestOutstanding(item.carrierRoute)
					if head != nil && head != item {
						// Held until the head's next firing. When the head is
						// due in this pass and not yet written, that is the
						// interval its write is about to schedule; never a
						// time already past, which would spin this item
						// through the loop.
						holdUntil := head.resendTime
						if !sendTime.Before(holdUntil) {
							holdUntil = sendTime.Add(
								self.resendIntervalForItem(head, head.sendCount+1))
						}
						item.resendTime = holdUntil
						item.recoveryKind = sendRecoveryNone
						self.resendQueue.Add(item)
						self.client.laneProbeRideCount.Add(1)
						continue
					}
					self.client.laneProbeWriteCount.Add(1)
				} else if laneVerdict == laneTimerDraining {
					// §34.3 rule 2. Something below this item was
					// acknowledged on its own lane since it last looked, so
					// on a FIFO lane the lane is draining toward it and it is
					// next: re-armed with backoff, no limit, no since-last
					// term and no estimate read. §32.4 re-armed here on the
					// absence of a later same-lane acknowledgement instead,
					// which is unbounded when the receiver's own drops remove
					// every item that could carry that proof; this re-arm
					// rests on an acknowledgement that arrived, so a lane
					// that stops answering leaves it at once. This is §13.5's
					// deferral and §27.3's ride collapsed into the one action
					// they were both answers to.
					// The count is advanced before the interval is read, so
					// the re-arms keep the rewrite's own timer: a timer that
					// fired at one interval is next due at three, then seven.
					item.timeoutDeferCount += 1
					item.timeoutDeferAckTime = self.lastCumulativeAckTime
					item.deferralOutstanding = true
					// No bound on this re-arm, and in particular no
					// liveness due time to clamp the sequence head to.
					// Re-establishing a receiver that silently lost the
					// sequence needs a setHead rewrite, and rule 3 is what
					// produces it: a receiver in that state acknowledges
					// nothing, so nothing on the lane ever moves, so the head
					// is never held here in the first place.
					item.resendTime = sendTime.Add(
						self.deferredResendInterval(item, self.rttWindow.ScaledRtt()))
					self.resendQueue.Add(item)
					self.client.timeoutResendDeferCount.Add(1)
					continue
				} else if recoveryKind == sendRecoveryNone && !self.lastCumulativeAckTime.IsZero() {
					scaledRtt := self.rttWindow.ScaledRtt()
					if sendTime.Sub(self.lastCumulativeAckTime) < scaledRtt {
						// M4: a whole-window timeout while the cumulative ack is
						// still advancing is the spurious cascade, not a stalled lane.
						self.client.timeoutResendWithRecentCumulativeProgress.Add(1)
					}
					if self.shouldDeferTimeoutResend(item, scaledRtt) {
						// FLIGHTGATEFIX §13.5 (F12): the cumulative ack advanced
						// within one scaled RTT of this item's send, so the reliable
						// lane is alive and its queue is deeper than the estimate.
						// Wait one more round trip. Each further deferral needs the
						// cumulative ack to have advanced since the last one, so a
						// hole nothing can acknowledge is deferred once and then
						// retransmitted: deferring is right while the queue drains
						// and wrong once the Pack is gone (§16).
						deferInterval := self.deferredResendInterval(item, scaledRtt)
						item.timeoutDeferCount += 1
						item.timeoutDeferAckTime = self.lastCumulativeAckTime
						item.deferralOutstanding = true
						item.resendTime = sendTime.Add(deferInterval)
						self.resendQueue.Add(item)
						self.client.timeoutResendDeferCount.Add(1)
						continue
					}
				}
				// the deferral, if any, is over: this timeout is being written
				item.deferralOutstanding = false
				reliableOnlyResend := false
				if recoveryKind == sendRecoveryNone && item.unreliableFlightTracked {
					reliableOnlyResend = self.observeUnreliableResendTimeout(item, flightPolicy)
				}
				item.selectiveAcked = false

				// resend
				var transferFrameBytes []byte
				if self.sendItems[0].sequenceNumber == item.sequenceNumber &&
					!item.head {
					// Set head after cumulative progress. A negotiated compact head stays
					// compact through every loss recovery; only an explicit receiver
					// request reconstructs its complete contract.
					var err error
					var hasContractFrame bool
					transferFrameBytes, hasContractFrame, err = self.setHead(item, false)
					if err != nil {
						self.log.Errorf("[s]%s->%s...%s s(%s) exit could not set head = %s\n", self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId, err)
						return
					}
					MessagePoolReturn(item.transferFrameBytes)
					item.head = true
					item.hasContractFrame = hasContractFrame
					item.promotedHead = true
					item.transferFrameBytes = transferFrameBytes
				} else {
					// var err error
					// transferFrameBytes, err = self.setTag(item)
					// if err != nil {
					// 	self.log.Errorf("[s]%s->%s...%s s(%s) exit could not set tag = %s\n", self.client.ClientTag(), self.intermediaryIds, self.destination, self.contractMultiRouteWriterAlias.StreamId, err)
					// 	return
					// }
					transferFrameBytes = item.transferFrameBytes
				}

				// resend uses the same path the item was originally sent on
				resendPath := sendTransferPath(self.client.ClientId(), DestinationId(self.destination))
				resendBytes := transferFrameBytes
				resendForceUnwrapped := item.forceUnwrapped
				var resendDisposition transferWriteDisposition
				var resendErr error
				c := func() error {
					var writeErr error
					resendDisposition, writeErr = self.writeMaybeWrappedBytes(
						resendBytes,
						resendPath,
						resendForceUnwrapped,
						item,
						true,
						reliableOnlyResend,
					)
					return writeErr
				}
				if self.log.V(2).Enabled() {
					resendErr = TraceWithReturn(
						fmt.Sprintf(
							"[s]resend %d multi route write %s->%s...%s s(%s)",
							item.sequenceNumber,
							self.client.ClientTag(),
							self.contractIntermediaryIds(),
							self.destination,
							self.contractMultiRouteWriterAlias.StreamId,
						),
						c,
					)
				} else {
					resendErr = c()
					if resendErr != nil {
						if self.log.V(1).Enabled() {
							self.log.Infof("[s]resend drop = %s", resendErr)
						}
					}
				}
				if resendErr == nil {
					if !item.transportWriteObserved {
						item.transportWriteObserved = true
						item.acks.observeTransportWrite(resendDisposition.transportType)
					}
					self.observeCarrierWrite(item, resendDisposition)
					self.resendWriteCount.Add(1)
					self.resendWriteByteCount.Add(uint64(len(transferFrameBytes)))
				}
				self.client.recordSendRecovery(recoveryKind, resendErr)
				if recoveryKind == sendRecoverySelectiveGap && 0 < item.timeoutDeferCount {
					// a recovery the deferred retransmit declined to write
					// and the scoreboard wrote instead (FLIGHTGATEFIX §23.3)
					self.client.selectiveGapWritesOfDeferredItems[gapHoleCarrierOf(item)].Add(1)
				}
				if recoveryKind == sendRecoverySelectiveGap &&
					self.scheduleGapRecoveryProbe(
						item,
						time.Now(),
						item.sendTime.Add(self.sendBufferSettings.AckTimeout),
					) {
					self.resendQueue.Add(item)
					continue
				}

				if recoveryKind == sendRecoveryNone {
					item.sendCount += 1
				}
				// back off the resend timeout multiplicatively with each resend
				// of the same item, up to `MaxResendInterval`. When acks are
				// delayed (not lost) by queueing, a flat timeout re-sends the
				// whole in-flight window every interval, and the duplicates
				// feed the congestion that delayed the acks in the first place.
				// §34.3: a lane head written under rule 3 is re-armed on its
				// own backed-off interval. eeca11f re-armed it on a fixed
				// cold cadence instead, justified as a constant that cannot
				// lag, and that justification is true but not sufficient: a
				// constant still fires while the lane is demonstrably
				// draining, which is the M4 failure in a new place. Rule 2
				// already answers that with a fact about position, and a fact
				// beats a constant, so the cadence goes.
				itemResendTimeout := self.resendIntervalForItem(item, item.sendCount)
				if !retainPastAckTimeout && itemAckTimeout <= itemResendTimeout {
					item.resendTime = sendTime.Add(itemAckTimeout)
				} else {
					item.resendTime = sendTime.Add(itemResendTimeout)
				}
				self.resendQueue.Add(item)
			}
		}

		checkpointId := self.idleCondition.Checkpoint()

		resendCapacity := self.resendQueue.CanAdd(
			0,
			self.sendWindowEstimate(sendTime).Window,
		)
		self.observeRouteStall(sendTime)
		// FLIGHTGATEFIX §22: a reliable lane may hold what it has shown it
		// can carry. Beyond that the queue only adds delay, the scaled timer
		// fires on depth rather than on loss, and the whole window is
		// rewritten against a lane that is still delivering.
		reliableAdmission := self.reliableAdmissionAvailable(flightPolicy, sendTime)
		if !reliableAdmission {
			if reliableAdmissionWaitStart.IsZero() {
				reliableAdmissionWaitStart = sendTime
				self.client.reliableAdmissionWaitCount.Add(1)
			}
		} else if !reliableAdmissionWaitStart.IsZero() {
			self.client.reliableAdmissionWaitNanos.Add(
				uint64(sendTime.Sub(reliableAdmissionWaitStart)),
			)
			reliableAdmissionWaitStart = time.Time{}
		}
		resendCapacity = resendCapacity && reliableAdmission
		// The unreliable flight only gates admission while no reliable carrier
		// can take the overflow; otherwise a full flight is written reliable-only
		// (see writeMaybeWrappedBytes) instead of stalling the sequence.
		flightGates := self.unreliableFlightGates(flightPolicy)
		flightEligible := func(sendPack *SendPack) bool {
			return flightPolicy.flowIsolation &&
				self.noAckPackCanBypassRecoveryAdmission(sendPack) ||
				!flightGates ||
				self.flightController.canSendForKey(sendPack.schedulingKey)
		}
		sendEligible := func(sendPack *SendPack) bool {
			return self.noAckPackCanBypassRecoveryAdmission(sendPack) ||
				resendCapacity &&
					(!flightGates || self.flightController.canSendForKey(sendPack.schedulingKey))
		}
		var sendPack *SendPack
		bypassedRecoveryAdmission := false
		if flightPolicy.flowIsolation {
			ordinaryFlightAvailable := self.flightController.canSend()
			fifoHead := scheduler.FifoHead()
			sendPack = scheduler.TakeEligible(sendEligible)
			if sendPack != nil {
				bypassedRecoveryAdmission =
					self.noAckPackCanBypassRecoveryAdmission(sendPack) &&
						(!resendCapacity || !ordinaryFlightAvailable)
				if fifoHead != nil && sendPack != fifoHead {
					self.client.unreliableFlowIsolationBypassCount.Add(1)
				}
				if !bypassedRecoveryAdmission && !ordinaryFlightAvailable {
					self.client.unreliableFlowReserveSelectionCount.Add(1)
				}
			}
		} else if resendCapacity {
			sendPack = scheduler.TakeFifoEligible(flightEligible)
		}
		if sendPack != nil {
			processingPacks[0] = sendPack
			// The first selected Pack is the earliest point at which opening a
			// destination writer is useful. Refresh its carrier policy here so the
			// first H1 burst receives the H1 ready-drain bound; an H3/mixed first
			// burst still receives the conservative bound and flight policy.
			if self.contractMultiRouteWriter == nil && self.sendBuffer != nil {
				self.openContractMultiRouteWriter()
				flightPolicy = self.transferFlightPolicy()
				self.flowIsolation.Store(flightPolicy.flowIsolation)
				if self.flightController.applyPolicy(flightPolicy) {
					self.client.observeUnreliableFlight(self.flightController)
				}
			}
			processPack := func() bool {
				if sendPack.logicalGroup {
					complete, success, deferForRecoveryAdmission :=
						self.processLogicalGroupChunk(
							sendPack,
							bypassedRecoveryAdmission,
							flightPolicy,
						)
					if deferForRecoveryAdmission {
						scheduler.PushFront(sendPack)
						processingPacks[0] = nil
						return !packsClosed
					}
					if !complete {
						// Physical chunks may rotate between active flows, but the
						// remaining cursor stays ahead of later groups in this flow.
						// Requeueing at the tail stripes one TCP stream's segments
						// across independently admitted provider-return groups.
						scheduler.PushFront(sendPack)
					}
					processingPacks[0] = nil
					return success && !packsClosed
				}

				sendPacks := [sendPackH1GroupMaxFrames]*SendPack{sendPack}
				sendPackCount := 1
				frameCount := len(sendPack.frameList())
				messageByteCount := sendPack.serializedMessageByteCount()
				maxFrames, maxMessageByteCount := self.readyDrainChunkLimits(flightPolicy)

				// H3's explicit flow reserve needs same-flow coalescing. Carriers
				// without that reserve retain ingress-order coalescing. H1 drains
				// only Packs that are already ready; it never waits to fill a batch,
				// so a sparse request or TCP ACK keeps its original latency.
				for !bypassedRecoveryAdmission &&
					sendPackCount < len(sendPacks) &&
					frameCount < maxFrames {
					var nextSendPack *SendPack
					if flightPolicy.flowIsolation {
						drainPacks()
						nextSendPack = scheduler.TakeSameFlow(sendPack.schedulingKey)
					} else {
						nextSendPack = scheduler.TakeFifoEligible(func(*SendPack) bool {
							return true
						})
						if nextSendPack == nil {
							select {
							case queuedSendPack, ok := <-self.packs:
								if !ok {
									packsClosed = true
								} else {
									nextSendPack = queuedSendPack
								}
							default:
							}
						}
					}
					if nextSendPack == nil {
						break
					}
					processingPacks[sendPackCount] = nextSendPack
					nextFrameCount := len(nextSendPack.frameList())
					nextMessageByteCount := messageByteCount + nextSendPack.serializedMessageByteCount()
					contractSafe := self.client.ContractManager().SendNoContract(self.destination) ||
						(self.sendContract != nil &&
							self.sendContractAcked &&
							0 < len(self.sendItems) &&
							self.sendContract.canUpdate(nextMessageByteCount))
					compatible := sendPack.Ack == nextSendPack.Ack &&
						sendPack.ForceUnwrapped == nextSendPack.ForceUnwrapped &&
						frameCount+nextFrameCount <= maxFrames &&
						nextMessageByteCount <= maxMessageByteCount &&
						contractSafe
					if compatible {
						sendPacks[sendPackCount] = nextSendPack
						sendPackCount += 1
						frameCount += nextFrameCount
						messageByteCount = nextMessageByteCount
					} else {
						scheduler.PushFront(nextSendPack)
						processingPacks[sendPackCount] = nil
						break
					}
				}

				schedulingKey := sendPack.schedulingKey
				if !flightPolicy.flowIsolation {
					// A cross-flow Pack has no exact flow identity. Keep this local
					// metadata invalid if the carrier later changes generations.
					schedulingKey = sendSchedulingKey{}
				}

				// Messages smaller than MinMessageByteCount are still charged at
				// that minimum by the contract implementation.
				contractUpdated := false
				deferForRecoveryAdmission := false
				var contractErr error
				if bypassedRecoveryAdmission {
					contractUpdated, deferForRecoveryAdmission, contractErr =
						self.updateContractWithoutAckPromotionOutcome(messageByteCount)
				} else {
					contractUpdated, contractErr = self.updateContractOutcome(messageByteCount)
				}
				if contractUpdated {
					if bypassedRecoveryAdmission {
						self.client.unreliableNoAckAdmissionBypassCount.Add(1)
					}
					if sendPackCount == 1 {
						self.sendRecordForSchedulingKey(
							sendPack.frameList(),
							sendPack.ackRecord(),
							sendPack.noAckRecord(),
							sendPack.Ack,
							sendPack.ForceUnwrapped,
							schedulingKey,
						)
					} else {
						var frameValues [sendPackH1GroupMaxFrames]*protocol.Frame
						frames := frameValues[:0]
						var acks sendAckSet
						var noAckSends noAckSendSet
						for packIndex := range sendPackCount {
							frames = append(frames, sendPacks[packIndex].frameList()...)
							acks.add(sendPacks[packIndex].ackRecord())
							noAckSends.add(sendPacks[packIndex].noAckRecord())
						}
						self.sendRecordsForSchedulingKey(
							frames,
							acks,
							noAckSends,
							sendPack.Ack,
							sendPack.ForceUnwrapped,
							schedulingKey,
						)
					}
					for packIndex := range sendPackCount {
						sendPacks[packIndex].releaseRaw()
						processingPacks[packIndex] = nil
					}
					return !packsClosed
				}
				if deferForRecoveryAdmission {
					scheduler.PushFront(sendPack)
					processingPacks[0] = nil
					return !packsClosed
				}

				err := self.classifyContractCreationFailure(contractErr)
				for packIndex := range sendPackCount {
					// same silent discard as a failed write, by a different
					// route: no retry, and the error reaches nothing that counts
					if !sendPacks[packIndex].Ack && self.client != nil {
						self.client.sendNoAckDiscardCount.Add(1)
					}
					sendPacks[packIndex].completeLifecycleFirstRouteWrite(err)
					sendPacks[packIndex].completeNoAck(err)
					sendPacks[packIndex].invokeAck(err)
					sendPacks[packIndex].returnFrames()
					sendPacks[packIndex].releaseRaw()
					processingPacks[packIndex] = nil
				}
				return false
			}

			if !processPack() {
				return
			}
			continue
		}

		flightBlocked := flightGates &&
			(!self.flightController.canSend() ||
				0 < scheduler.Len() && !scheduler.HasEligible(flightEligible))
		if flightBlocked {
			self.client.unreliableFlightWaitCount.Add(1)
			if provider, ok := self.contractMultiRouteWriter.(transferReliableCapacityProvider); ok &&
				provider.reliableRouteHasCapacity() {
				// M1: the gate, not the carrier, is what holds this Pack.
				self.client.unreliableFlightBlockedWithReliableCapacity.Add(1)
			}
		}
		if (!resendCapacity || flightBlocked) && self.sendBuffer != nil &&
			self.sendBuffer.beforeResendCapacityWaitForTest != nil {
			self.sendBuffer.beforeResendCapacityWaitForTest(self.id())
		}
		var flightWaitStart time.Time
		if flightBlocked {
			flightWaitStart = time.Now()
		}
		packIngress := (<-chan *SendPack)(self.packs)
		if !flightPolicy.flowIsolation &&
			(!resendCapacity || flightBlocked || 0 < scheduler.Len()) {
			// Preserve the historical channel boundary on P2P/H1. Producers may
			// fill its fixed capacity, but this sequence does not hide those Packs
			// in a second queue while it is waiting for an Ack.
			packIngress = nil
		} else if self.packAdmission == nil && (!resendCapacity || 0 < scheduler.Len()) {
			// SequenceBufferSize == 0 is an explicit synchronous-admission
			// contract used by callers and memory-budget tests. Do not turn
			// that unbuffered handoff into a hidden scheduler queue while the
			// resend/flight boundary is closed.
			packIngress = nil
		}

		// A flow-isolating carrier keeps ingress armed while one flow is
		// flight-limited. That exposes a newly active flow to the fair scheduler;
		// a carrier with a reserve may send it immediately, while an isolation-only
		// carrier gives it the next ordinary acknowledgement opening.
		idleTimer.Reset(timeout)
		select {
		case <-self.ctx.Done():
			if !flightWaitStart.IsZero() {
				self.client.observeUnreliableFlightWait(time.Since(flightWaitStart))
			}
			return
		case <-ackSnapshot.ackNotify:
		case <-flightPolicy.notify:
		case nextSendPack, ok := <-packIngress:
			if !ok {
				packsClosed = true
			} else {
				scheduler.Push(nextSendPack)
			}
		case <-idleTimer.C:
			if self.resendQueue.Len() == 0 && scheduler.Len() == 0 {
				done := false
				func() {
					self.packMutex.Lock()
					defer self.packMutex.Unlock()
					if self.idleCondition.Close(checkpointId) {
						done = true
					}
				}()
				if done {
					if self.log.V(1).Enabled() {
						self.log.Infof("[s]%s->%s...%s s(%s) exit idle timeout\n", self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId)
					}
					return
				}
			}
		}
		if !flightWaitStart.IsZero() {
			self.client.observeUnreliableFlightWait(time.Since(flightWaitStart))
		}
		if packsClosed {
			return
		}
	}
}

// Reports the exact terminal outcome captured inside contract acquisition.
// A later sequence cancellation cannot reclassify an already-live exhaustion.
func (self *SendSequence) classifyContractCreationFailure(err error) error {
	if self.sendBuffer != nil && self.sendBuffer.beforeContractFailureClassifyForTest != nil {
		self.sendBuffer.beforeContractFailureClassifyForTest(self.id())
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		if self.log.V(1).Enabled() {
			self.log.Infof(
				"[s]%s->%s...%s s(%s) exit contract creation canceled = %s\n",
				self.client.ClientTag(),
				self.contractIntermediaryIds(),
				self.destination,
				self.contractMultiRouteWriterAlias.StreamId,
				err,
			)
		}
		return err
	}

	if err == nil {
		err = errors.New("No contract")
	}
	self.log.Errorf(
		"[s]%s->%s...%s s(%s) exit could not create contract.\n",
		self.client.ClientTag(),
		self.contractIntermediaryIds(),
		self.destination,
		self.contractMultiRouteWriterAlias.StreamId,
	)
	return err
}

func (self *SendSequence) updateContract(messageByteCount ByteCount) bool {
	updated, _ := self.updateContractOutcome(messageByteCount)
	return updated
}

// Preserves the terminal acquisition cause for the send loop's diagnostics.
func (self *SendSequence) updateContractOutcome(messageByteCount ByteCount) (bool, error) {
	updated, _, err := self.updateContractWithAckPromotion(messageByteCount, true)
	return updated, err
}

// updateContractWithoutAckPromotion debits only an established contract that
// keeps a requested NoAck Pack off the resend/flight path. The second return
// value asks the caller to retry through ordinary recovery admission when the
// contract changed between scheduler eligibility and debit. A genuine contract
// failure remains terminal and is not converted into an admission wait.
func (self *SendSequence) updateContractWithoutAckPromotion(
	messageByteCount ByteCount,
) (updated bool, deferForRecoveryAdmission bool) {
	updated, deferForRecoveryAdmission, _ =
		self.updateContractWithoutAckPromotionOutcome(messageByteCount)
	return
}

// Preserves the terminal acquisition cause while retaining the recovery-defer
// outcome used by unreliable no-ack admission.
func (self *SendSequence) updateContractWithoutAckPromotionOutcome(
	messageByteCount ByteCount,
) (updated bool, deferForRecoveryAdmission bool, err error) {
	return self.updateContractWithAckPromotion(messageByteCount, false)
}

func (self *SendSequence) updateContractWithAckPromotion(
	messageByteCount ByteCount,
	allowAckPromotion bool,
) (updated bool, deferForRecoveryAdmission bool, err error) {
	if self.sendBuffer != nil && self.sendBuffer.forceContractFailureForTest != nil &&
		self.sendBuffer.forceContractFailureForTest(self.id()) {
		return false, false, errors.New("No contract")
	}
	// `sendNoContract` is a mutual configuration
	// both sides must configure themselves to require no contract from each other
	if self.client.ContractManager().SendNoContract(self.destination) {
		// the destination newly requires no contract.
		// drop the active contract so that subsequent items are not attributed
		// to it: an item sent without a contract debit would over credit the
		// contract accounting when its ack arrives.
		// do not close the current contract unless it has no pending data;
		// the contract is tracked in `openSendContracts` and will be closed on ack
		if self.sendContract != nil {
			if self.sendContract.unackedByteCount == 0 {
				self.client.ContractManager().CloseContract(
					self.sendContract.contractId,
					self.sendContract.ackedByteCount,
					self.sendContract.unackedByteCount,
				)
				delete(self.openSendContracts, self.sendContract.contractId)
			}
			self.sendContract = nil
		}
		return true, false, nil
	}

	metadata := self.contractMetadata()
	// Promotion changes the contract path required by every future pack. Keep
	// an obsolete head only in openSendContracts so outstanding ACK accounting
	// can finish; it must not debit or authenticate this promoted pack merely
	// because the old direct contract still has ample capacity.
	if self.sendContract != nil &&
		self.sendContractMetadataGeneration != metadata.generation {
		retiredContract := self.sendContract
		self.sendContract = nil
		self.sendContractAcked = false
		if retiredContract.unackedByteCount == 0 {
			self.client.ContractManager().CloseContract(
				retiredContract.contractId,
				retiredContract.ackedByteCount,
				retiredContract.unackedByteCount,
			)
			delete(self.openSendContracts, retiredContract.contractId)
		}
	}
	if self.sendContract != nil &&
		(allowAckPromotion || self.sendContractAcked) &&
		self.sendContract.update(messageByteCount) {
		return true, false, nil
	}
	if !allowAckPromotion {
		return false, true, nil
	}

	var contractErr error
	createContract := func() bool {
		// the max overhead of the pack frame
		// this is needed because the size of the contract pack is counted against the contract
		// maxContractMessageByteCount := ByteCount(256)

		effectiveContractTransferByteCount := ByteCount(float32(self.client.ContractManager().StandardContractTransferByteCount()) * self.sendBufferSettings.ContractFillFraction)
		if effectiveContractTransferByteCount < messageByteCount+self.sendBufferSettings.MinMessageByteCount /*+ maxContractMessageByteCount*/ {
			// this pack does not fit into a standard contract
			// TODO allow requesting larger contracts
			panic(fmt.Errorf("Message too large for contract. It can never be sent (%d).", messageByteCount))
		}

		setNextContract := func(
			contract *protocol.Contract,
			metadata sendContractMetadata,
		) bool {
			nextSendContract, err := newSequenceContract(
				self.log,
				"s",
				contract,
				self.sendBufferSettings.MinMessageByteCount,
				self.sendBufferSettings.ContractFillFraction,
			)
			if err != nil {
				// malformed
				self.log.Errorf("[s]%s->%s...%s s(%s) exit next contract malformed error = %s\n", self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId, err)
				return false
			}

			if _, ok := self.openSendContracts[nextSendContract.contractId]; ok {
				return false
			}

			// note `update(0)` will use `MinMessageByteCount` byte count
			// the min message byte count is used to avoid spam
			if nextSendContract.update(0) && nextSendContract.update(messageByteCount) {
				self.setContract(nextSendContract, metadata.generation)

				// Append the contract to the sequence. The contract-open
				// ride-along carries only the contract frame — no application
				// payload — so pre-cipher it is pinned plaintext
				// (ForceUnwrapped, sticky across resends like the handshake
				// controls): under EncryptionModeRequired the fail-closed
				// write path would otherwise refuse it, and a refused open
				// gaps the sequence ahead of the handshake controls and
				// wedges establishment. The pin also keeps a pre-cipher open
				// legible on resend after the local cipher comes up while the
				// peer's has not (the EC-frame rationale). Once the cipher is
				// up the open pack is queued unpinned and wraps normally,
				// re-sealed per write like any other frame.
				forceUnwrapped := self.session != nil && self.session.Cipher() == nil
				self.sendWithSetContract(
					nil,
					self.contractOpenAckCallback(nextSendContract),
					true,
					true,
					forceUnwrapped,
				)

				// FIXME
				self.log.Infof("[s]%s->%s...%s s(%s) contract set %s\n", self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId, nextSendContract.contractId)

				return true

			} else {
				// this contract doesn't fit the message
				// the contract was requested with the correct size, so this is an error somewhere
				// just close it and let the platform time out the other side
				self.log.Errorf("[s]%s->%s...%s s(%s) contract too small %s\n", self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId, nextSendContract.contractId)
				self.client.ContractManager().CloseContract(nextSendContract.contractId, 0, 0)
				return false
			}
		}

		nextContract := func(timeout time.Duration) bool {
			metadata := self.contractMetadata()
			if self.sendBuffer != nil && self.sendBuffer.beforeTakeContractForTest != nil {
				self.sendBuffer.beforeTakeContractForTest(self.id())
			}
			contract := self.client.ContractManager().TakeContract(
				metadata.ctx,
				metadata.key,
				timeout,
			)
			if contract == nil {
				return false
			}
			if self.contractTakenForTest != nil {
				self.contractTakenForTest(metadata)
			}
			if setNextContract(contract, metadata) {
				self.contractSeqIndex += 1
				// async queue up the next contract.
				//
				// Skipped while the backend is unreachable. A sequence that
				// still holds queued contracts keeps satisfying TakeContract,
				// so without this check it would keep prefetching and keep the
				// OOB storm going for exactly the sequences that still have
				// work to do. The contract just taken is unaffected: only the
				// prefetch of the following one waits for the backend.
				if !isBackendDegraded() {
					prefetchMetadata := self.contractMetadata()
					self.client.ContractManager().CreateContract(
						prefetchMetadata.key,
						self.contractSeqIndex,
						ByteCount(32+float32(messageByteCount+self.sendBufferSettings.MinMessageByteCount)/self.sendBufferSettings.ContractFillFraction),
					)
				}
				return true
			}
			return false
		}
		traceNextContract := func(timeout time.Duration) bool {
			if self.log.V(2).Enabled() {
				return TraceWithReturn(
					fmt.Sprintf("[s]%s->%s...%s s(%s) next contract", self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId),
					func() bool {
						return nextContract(timeout)
					},
				)
			} else {
				return nextContract(timeout)
			}
		}

		endTime := time.Now().Add(self.sendBufferSettings.CreateContractTimeout)
		retryInterval := self.sendBufferSettings.CreateContractRetryInterval
		maxRetryInterval := self.sendBufferSettings.CreateContractRetryMaxInterval
		if maxRetryInterval <= 0 {
			maxRetryInterval = retryInterval
		}
		// The fast first retry exists to cover a single dropped control
		// message. While the backend is unreachable there is nothing to cover:
		// no contract can be authorized until it returns, so start at the
		// backed-off interval instead of walking up from 1s on every sequence.
		if isBackendDegraded() {
			retryInterval = maxRetryInterval
		}

		if self.sendContract != nil {
			// there should be a queued up contract
			if traceNextContract(min(self.sendBufferSettings.CreateContractTimeout, retryInterval)) {
				return true
			}
			retryInterval = nextCreateContractRetryInterval(retryInterval, maxRetryInterval)
		}

		for {
			select {
			case <-self.ctx.Done():
				contractErr = self.ctx.Err()
				return false
			default:
			}

			timeout := endTime.Sub(time.Now())
			if timeout <= 0 {
				contractErr = errors.New("No contract")
				return false
			}

			// async queue up the next contract
			metadata := self.contractMetadata()
			// Skip the request entirely while the backend is unreachable. Each
			// CreateContract is an OOB control round-trip; with the API down
			// every one of them fails, and on a provider carrying many
			// sequences that is a continuous storm of requests that cannot
			// succeed. The loop still waits out the retry interval, so the
			// sequence resumes promptly once a successful auth or OOB
			// round-trip clears the degraded state.
			if !isBackendDegraded() {
				self.client.ContractManager().CreateContract(
					metadata.key,
					self.contractSeqIndex,
					ByteCount(32+float32(messageByteCount+messageByteCount+self.sendBufferSettings.MinMessageByteCount)/self.sendBufferSettings.ContractFillFraction),
				)
			}

			if traceNextContract(min(timeout, retryInterval)) {
				return true
			}
			retryInterval = nextCreateContractRetryInterval(retryInterval, maxRetryInterval)
		}
	}

	createStartTime := time.Now()
	var ok bool
	if self.log.V(2).Enabled() {
		ok = TraceWithReturn(
			fmt.Sprintf("[s]create contract c=%t %s->%s...%s s(%s)", self.companionContract, self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId),
			createContract,
		)
	} else {
		ok = createContract()
	}
	// surface slow contract acquisition at default verbosity. The send
	// sequence blocks here, so a slow create (e.g. a companion request that
	// cannot match an origin contract) stalls the entire sequence.
	//
	// The threshold was 1s, which hid the case that matters most. Device
	// measurements put ~350ms of unexplained latency between a connection being
	// established and its first byte arriving -- large enough to dominate every
	// request, small enough to never log. Contract acquisition blocks in exactly
	// that window, so it has to be observable well below a second to be ruled in
	// or out.
	contractWaitTime := time.Since(createStartTime)
	self.addContractWaitTime(contractWaitTime)
	if d := contractWaitTime; self.sendBufferSettings.ContractWaitLogThreshold <= d {
		self.log.Infof("[s]contract wait %.0fms ok=%t c=%t %s->%s...%s s(%s)\n", float64(d.Microseconds())/1000.0, ok, self.companionContract, self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId)
	}
	return ok, false, contractErr
}

func nextCreateContractRetryInterval(current time.Duration, maximum time.Duration) time.Duration {
	if maximum <= 0 || maximum <= current {
		return current
	}
	if current <= 0 {
		return maximum
	}
	if maximum/2 < current {
		return maximum
	}
	return min(2*current, maximum)
}

func (self *SendSequence) setContract(
	nextSendContract *sequenceContract,
	metadataGeneration uint64,
) {
	// do not close the current contract unless it has no pending data
	// the contract is tracked in `openSendContracts` and will be closed on ack
	if self.sendContract != nil && self.sendContract.unackedByteCount == 0 {
		self.client.ContractManager().CloseContract(
			self.sendContract.contractId,
			self.sendContract.ackedByteCount,
			self.sendContract.unackedByteCount,
		)
		delete(self.openSendContracts, self.sendContract.contractId)
	}
	self.openSendContracts[nextSendContract.contractId] = nextSendContract
	self.sendContract = nextSendContract
	self.sendContractAcked = false
	self.sendContractMetadataGeneration = metadataGeneration
	if self.client.streamManager != nil &&
		nextSendContract.path.StreamId != (Id{}) &&
		nextSendContract.path.SourceId == self.client.ClientId() &&
		nextSendContract.path.DestinationId == self.destination {
		self.client.streamManager.authenticateStreamDestination(
			nextSendContract.path.StreamId,
			self.destination,
		)
	}
	nextSendContract.statsEntry = self.client.ContractManager().registerContractStats(
		nextSendContract.contractId,
		false,
		self.companionContract,
		nextSendContract.path,
		nextSendContract.transferByteCount,
	)
	// The contract carries the destination's `ProvideTlsCertificate`
	// commitment (possibly empty). Fold the chain into the session's
	// trusted-peer-cert set so the peer's TLS-handshake cert can be matched
	// against any cert version the destination has ever published — both
	// the cert in this contract and any cert in a previously seen contract.
	// An empty chain turns off verification entirely (the destination is
	// not committing to a TLS identity).
	//
	// In addition, contracts carry the destination's long-lived client
	// public identity key plus the destination's signature over the cert
	// chain by that key. Pass the public key to the session so it can
	// (a) verify the cert chain before trusting it (defeats a platform
	// MITM that substitutes the cert), and (b) verify the post-handshake
	// identity proof exchanged inside the per-peer TLS session (defeats
	// an active MITM that re-handshakes TLS on each leg).
	if self.session != nil {
		if 0 < len(nextSendContract.destinationClientPublicKey) {
			self.session.SetPeerClientPublicKey(ed25519.PublicKey(nextSendContract.destinationClientPublicKey))
		}
		self.session.AddTrustedPeerCertChain(
			nextSendContract.provideTlsCertificate,
			nextSendContract.destinationClientKeySignedTlsCertificate,
		)
	}
}

func (self *SendSequence) setContractAcked(nextSendContract *sequenceContract, ack bool) {
	if self.sendContract == nextSendContract {
		self.sendContractAcked = ack
	}
}

// A failed terminal disposition must not promote an opening contract. The
// current-contract guard also prevents a late callback from mutating its
// replacement.
func (self *SendSequence) contractOpenAckCallback(
	nextSendContract *sequenceContract,
) AckFunction {
	return func(err error) {
		self.setContractAcked(nextSendContract, err == nil)
	}
}

func (self *SendSequence) send(
	frames []*protocol.Frame,
	ackCallback AckFunction,
	ack bool,
	forceUnwrapped bool,
) {
	self.sendRecord(
		frames,
		sendAckRecord{callback: ackCallback},
		noAckSendRecord{},
		ack,
		forceUnwrapped,
	)
}

func (self *SendSequence) sendRecord(
	frames []*protocol.Frame,
	ack sendAckRecord,
	noAckSend noAckSendRecord,
	ackRequired bool,
	forceUnwrapped bool,
) {
	self.sendRecordForSchedulingKey(
		frames,
		ack,
		noAckSend,
		ackRequired,
		forceUnwrapped,
		sendSchedulingKey{},
	)
}

func (self *SendSequence) sendRecordForSchedulingKey(
	frames []*protocol.Frame,
	ack sendAckRecord,
	noAckSend noAckSendRecord,
	ackRequired bool,
	forceUnwrapped bool,
	schedulingKey sendSchedulingKey,
) {
	var acks sendAckSet
	acks.add(ack)
	var noAckSends noAckSendSet
	noAckSends.add(noAckSend)
	self.sendRecordsForSchedulingKey(
		frames,
		acks,
		noAckSends,
		ackRequired,
		forceUnwrapped,
		schedulingKey,
	)
}

func (self *SendSequence) sendWithSetContract(
	sendFrames []*protocol.Frame,
	ackCallback AckFunction,
	ack bool,
	setContract bool,
	forceUnwrapped bool,
) {
	var acks sendAckSet
	acks.add(sendAckRecord{callback: ackCallback})
	self.sendWithSetContractRecords(
		sendFrames,
		acks,
		noAckSendSet{},
		ack,
		setContract,
		forceUnwrapped,
		sendSchedulingKey{},
	)
}

func (self *SendSequence) sendRecords(
	sendFrames []*protocol.Frame,
	acks sendAckSet,
	noAckSends noAckSendSet,
	ack bool,
	forceUnwrapped bool,
) {
	self.sendRecordsForSchedulingKey(
		sendFrames,
		acks,
		noAckSends,
		ack,
		forceUnwrapped,
		sendSchedulingKey{},
	)
}

func (self *SendSequence) sendRecordsForSchedulingKey(
	sendFrames []*protocol.Frame,
	acks sendAckSet,
	noAckSends noAckSendSet,
	ack bool,
	forceUnwrapped bool,
	schedulingKey sendSchedulingKey,
) {
	if !ack && self.client != nil {
		self.client.sendNoAckWriteCount.Add(1)
	}
	self.sendWithSetContractRecords(
		sendFrames,
		acks,
		noAckSends,
		ack,
		false,
		forceUnwrapped,
		schedulingKey,
	)
}

// Separates transport-local stream steering from the end-to-end path encoded
// in a TransferFrame.
func sendTransferPath(sourceId Id, destination TransferPath) TransferPath {
	return destination.LocalMask().AddSource(sourceId)
}

func (self *SendSequence) sendWithSetContractRecords(
	sendFrames []*protocol.Frame,
	acks sendAckSet,
	noAckSends noAckSendSet,
	ack bool,
	setContract bool,
	forceUnwrapped bool,
	schedulingKey sendSchedulingKey,
) {
	sendTime := time.Now()
	messageId := NewId()

	var contractId *Id
	if self.sendContract != nil {
		contractId = &self.sendContract.contractId

		if !self.sendContractAcked {
			// (see note above about contracts and nack)
			// send nack messages as ack until the send contract is acked
			// this avoid racing the messages with the contract
			ack = true
		}
	}
	var head bool
	var sequenceNumber uint64
	if ack {
		head = (0 == len(self.sendItems))
		sequenceNumber = self.nextSequenceNumber
		self.nextSequenceNumber += 1
	} else {
		head = false
		sequenceNumber = 0
	}
	compactContractHead := head && self.sendContract != nil &&
		self.sendContractAcked && self.sendBufferSettings.CompactContractHead &&
		self.sendContract.compactContractRecoverySupported &&
		!setContract

	var contractFrame *protocol.Frame
	var contractMessageBytes []byte
	if (setContract || head && !compactContractHead) && self.sendContract != nil {
		contractMessageBytes, _ = ProtoMarshal(self.sendContract.contract)
		contractFrame = &protocol.Frame{
			MessageType:  protocol.MessageType_TransferContract,
			MessageBytes: contractMessageBytes,
		}
	}

	// var path TransferPath
	// if self.sendContract == nil {
	// 	path = self.destination.AddSource(self.client.ClientId())
	// } else {
	// 	path = self.sendContract.path.LocalMask()
	// }
	// The destination mask may carry the platform-authenticated stream used to
	// select a local route. Stream identity is transport-local and must not be
	// copied into the end-to-end TransferFrame: the final endpoint consumes a
	// local-mask path, while every intermediary already knows which stream to
	// forward from the P2P transport that delivered the frame.
	path := sendTransferPath(self.client.ClientId(), DestinationId(self.destination))
	messageByteCount := MessageByteCount(sendFrames)

	// Session role/companion stamping (applies to both encodings below):
	// A server-role sequence is the peer's EncryptedControl carrier. Stamp its
	// role on every pack — including the non-EC open/contract packs that carry no
	// EC frame to derive it from — so the receiver maps the whole sequence to one
	// complement session; otherwise the open pack splits into a separate receive
	// sequence and the handshake bytes (ServerHello, identity proof) gap forever.
	// Only the server role is marked: a client-role stream is the unencrypted
	// default, already the receiver's complement, so it stays off the wire.
	// Companion mirrors the role stamp (for either role): stamped only when true,
	// since false is the receiver's default and stays off the wire.

	var transferFrameBytes []byte
	if 2 <= self.sendBufferSettings.ProtocolVersion {
		// hand-rolled marshal of the hot TransferFrame{Pack}: wire-identical to
		// the proto structs in the legacy branch below (verified byte-for-byte in
		// frame_protobuf_test.go), without the intermediate Pack/TransferFrame/Tag/
		// TransferPath structs, the Id.Bytes() escapes, or reflection.
		spf := sendPackFrame{
			path:           path,
			messageId:      messageId,
			sequenceId:     self.sequenceId,
			sequenceNumber: sequenceNumber,
			head:           head,
			nack:           !ack,
			frames:         sendFrames,
			contractFrame:  contractFrame,
			tagSendTime:    uint64(sendTime.UnixMilli()),
		}
		if contractId != nil && (!ack || compactContractHead) {
			spf.contractId = contractId
		}
		if self.encryptionRole == sequenceTlsRoleServer {
			spf.sessionRole = self.encryptionRole.toProtobuf()
			spf.sessionRoleSet = true
		}
		if self.encryptionCompanion {
			spf.companion = true
		}
		// Sequence discriminators (Pack fields 10/11/12) make the sender's
		// local route options and bounded logical lane receiver-visible, so
		// distinct sequences coexist instead of superseding each other (see
		// receiveSequenceHeadKey).
		spf.forceStream = self.forceStream
		spf.companionContract = self.companionContract
		spf.logicalLane = self.logicalLane
		transferFrameBytes = marshalSendPackTransferFrame(&spf)
	} else {
		// legacy (<v2) path: build and marshal via the proto structs.
		// ProtoMarshal's reflection makes Pack.Frames escape in compiler
		// analysis. Clone this legacy-only slice so that conservative escape
		// does not also force the v2 coalescer's fixed frame array onto the heap.
		legacyFrames := slices.Clone(sendFrames)
		pack := &protocol.Pack{
			MessageId:         messageId.Bytes(),
			SequenceId:        self.sequenceId.Bytes(),
			SequenceNumber:    sequenceNumber,
			Head:              head,
			Frames:            legacyFrames,
			ContractFrame:     contractFrame,
			Nack:              !ack,
			Tag:               self.rttWindow.OpenTag(),
			ForceStream:       self.forceStream,
			CompanionContract: self.companionContract,
			LogicalLane:       self.logicalLane,
		}
		if contractId != nil && (!ack || compactContractHead) {
			pack.ContractId = contractId.Bytes()
		}
		packBytes, _ := ProtoMarshal(pack)
		transferFrame := &protocol.TransferFrame{
			TransferPath: path.ToProtobuf(),
			Frame: &protocol.Frame{
				MessageType:  protocol.MessageType_TransferPack,
				MessageBytes: packBytes,
			},
		}
		if self.encryptionRole == sequenceTlsRoleServer {
			sessionRole := self.encryptionRole.toProtobuf()
			transferFrame.SessionRole = &sessionRole
		}
		if self.encryptionCompanion {
			sessionCompanion := true
			transferFrame.SessionCompanion = &sessionCompanion
		}
		transferFrameBytes, _ = ProtoMarshal(transferFrame)
		MessagePoolReturn(packBytes)
	}

	// Serialization above is synchronous and transferFrameBytes owns the wire
	// representation. Release source buffers here rather than in a defer: this
	// shortens their live range and lets the coalescer's fixed frame array stay
	// on the stack instead of escaping through a deferred closure.
	if contractMessageBytes != nil {
		MessagePoolReturn(contractMessageBytes)
	}
	for _, frame := range sendFrames {
		MessagePoolReturn(frame.MessageBytes)
	}

	item := takeSendItem()
	*item = sendItem{
		transferItem: transferItem{
			messageId:        messageId,
			sequenceNumber:   sequenceNumber,
			messageByteCount: messageByteCount,
		},
		contractId: contractId,
		sendTime:   sendTime,
		resendTime: sendTime.Add(self.resendIntervalForPolicy(
			transferFlightPolicySnapshot{},
			1,
		)),
		ackTimeout:         self.ackTimeoutForPolicy(transferFlightPolicySnapshot{}),
		sendCount:          1,
		head:               head,
		hasContractFrame:   (contractFrame != nil),
		transferFrameBytes: transferFrameBytes,
		acks:               acks,
		forceUnwrapped:     forceUnwrapped,
		schedulingKey:      schedulingKey,
	}
	self.client.initialSendWriteCount.Add(1)
	self.client.initialSendFrameCount.Add(uint64(len(sendFrames)))
	self.client.initialSendMessageByteCount.Add(uint64(messageByteCount))
	if ack {
		// Publish acknowledgement identity before any observer or route can expose
		// the bytes to the peer. A direct route can return its Ack synchronously
		// inside the write; validation must find the item instead of discarding that
		// progress and leaving resend admission closed until the recovery timer.
		self.sendItems = append(self.sendItems, item)
		self.resendQueue.Add(item)
	}

	var writeDisposition transferWriteDisposition
	c := func() error {
		var writeErr error
		writeDisposition, writeErr = self.writeMaybeWrappedBytes(
			item.transferFrameBytes,
			path,
			item.forceUnwrapped,
			item,
			false,
			false,
		)
		return writeErr
	}
	var err error
	if self.log.V(2).Enabled() {
		err = TraceWithReturn(
			fmt.Sprintf("[s]multi route write %s->%s...%s s(%s)", self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId),
			c,
		)
	} else {
		err = c()
		if err != nil {
			if self.log.V(1).Enabled() {
				self.log.Infof("[s]drop = %s", err)
			}
		}
	}
	if err == nil {
		item.transportWriteObserved = true
		item.acks.observeTransportWrite(writeDisposition.transportType)
		self.writeCount.Add(1)
		self.writeByteCount.Add(uint64(len(item.transferFrameBytes)))
	}
	// The first physical writer attempt is distinct from terminal reliable
	// acknowledgement. Coalesced records retain one phase per original Pack.
	acks.firstRouteWrite(err)
	// Requested NoAck completion is an initial route-write disposition, not a
	// wire-ack disposition. Invoke it once here even when an unacknowledged
	// opening contract temporarily forced this Pack onto the Ack lane.
	//
	// A failure here is the end of the road for these packs: there is no retry
	// by construction, and the error goes only to an acknowledgement callback
	// that is a no-op for IP callers and to an observer that is nil by
	// default. Count it, or it is a silent discard that looks like ordinary
	// loss (THROUGHPUTFIX §37.23).
	if err != nil && 0 < noAckSends.count {
		self.client.sendNoAckDiscardCount.Add(uint64(noAckSends.count))
	}
	noAckSends.complete(err)

	if ack {
		if err == nil {
			self.observeCarrierWrite(item, writeDisposition)
			item.resendTime = sendTime.Add(self.resendIntervalForItem(item, 1))
		}
		if self.sendBuffer != nil && self.sendBuffer.afterInitialWriteQueuedForTest != nil {
			self.sendBuffer.afterInitialWriteQueuedForTest(self.id(), sequenceNumber)
		}
		// ignore the write error since the item will be resent
	} else {
		// immediately ack
		if err == nil {
			self.ackItem(item)
		} else {
			item.acks.invoke(err)
			item.messagePoolReturn()
		}
	}
}

func (self *SendSequence) setHead(
	item *sendItem,
	forceFullContract bool,
) ([]byte, bool, error) {
	if self.log.V(1).Enabled() {
		self.log.Infof("[s]set head %s->%s...%s s(%s)\n", self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId)
	}

	var transferFrame protocol.TransferFrame
	err := ProtoUnmarshal(item.transferFrameBytes, &transferFrame)
	if err != nil {
		return nil, false, err
	}

	var pack *protocol.Pack
	if transferFrame.Pack != nil {
		pack = transferFrame.Pack
	} else {
		pack = &protocol.Pack{}
		err = ProtoUnmarshal(transferFrame.Frame.MessageBytes, pack)
		if err != nil {
			return nil, false, err
		}
	}

	pack.Head = true
	pack.Tag = self.rttWindow.OpenTag()
	// A non-head promoted after cumulative progress references the contract the
	// receiver already verified when that contract negotiated recovery support.
	// Explicit missing-state recovery is the only path that forces the complete
	// proof back onto a negotiated compact item.
	if item.contractId != nil && !item.hasContractFrame {
		sendContract, ok := self.openSendContracts[*item.contractId]
		if !ok {
			return nil, false, errors.New("Missing send contract")
		}
		if !forceFullContract && self.sendBufferSettings.CompactContractHead &&
			sendContract.compactContractRecoverySupported {
			pack.ContractId = item.contractId.Bytes()
		} else {
			contractMessageBytes, marshalErr := ProtoMarshal(sendContract.contract)
			if marshalErr != nil {
				return nil, false, marshalErr
			}
			pack.ContractFrame = &protocol.Frame{
				MessageType:  protocol.MessageType_TransferContract,
				MessageBytes: contractMessageBytes,
			}
			defer MessagePoolReturn(contractMessageBytes)
		}
	}

	if transferFrame.Pack != nil {
		transferFrame.Pack = pack
	} else {
		packBytes, err := ProtoMarshal(pack)
		if err != nil {
			return nil, false, err
		}
		defer MessagePoolReturn(packBytes)
		transferFrame.Frame.MessageBytes = packBytes
	}

	transferFrameBytesWithHead, err := ProtoMarshal(&transferFrame)
	if err != nil {
		return nil, false, err
	}

	return transferFrameBytesWithHead, pack.ContractFrame != nil, nil
}

// Rebuilds one still-pending compact head after the receiver explicitly says
// it lost the verified contract. This signal is receiver state recovery, not a
// delivery Ack or congestion event, so it schedules an immediate full proof
// without changing the item's timeout backoff.
func (self *SendSequence) receiveContractMissing(
	messageId Id,
	missingContractId Id,
) bool {
	item := self.resendQueue.GetByMessageId(messageId)
	if item == nil || item.contractId == nil ||
		*item.contractId != missingContractId || item.hasContractFrame {
		return false
	}
	removed := self.resendQueue.RemoveByMessageId(messageId)
	if removed != item {
		panic(errors.New("Missing item"))
	}
	transferFrameBytes, hasContractFrame, err := self.setHead(item, true)
	if err != nil {
		self.resendQueue.Add(item)
		self.log.Errorf(
			"[s]%s->%s...%s s(%s) could not restore missing contract = %s\n",
			self.client.ClientTag(),
			self.contractIntermediaryIds(),
			self.destination,
			self.contractMultiRouteWriterAlias.StreamId,
			err,
		)
		return false
	}
	MessagePoolReturn(item.transferFrameBytes)
	item.transferFrameBytes = transferFrameBytes
	item.head = true
	item.hasContractFrame = hasContractFrame
	item.sendTime = time.Now()
	item.resendTime = item.sendTime
	item.recoveryKind = sendRecoveryContractMissing
	self.resendQueue.Add(item)
	return true
}

/*
func (self *SendSequence) setTag(item *sendItem) ([]byte, error) {
	self.log.V(1).Infof("[s]set tag %s->%s...%s s(%s)\n", self.client.ClientTag(), self.intermediaryIds, self.destination, self.contractMultiRouteWriterAlias.StreamId)

	var transferFrame protocol.TransferFrame
	err := proto.Unmarshal(item.transferFrameBytes, &transferFrame)
	if err != nil {
		return nil, err
	}

	var pack protocol.Pack
	err = proto.Unmarshal(transferFrame.Frame.MessageBytes, &pack)
	if err != nil {
		return nil, err
	}

	pack.Tag = self.rttWindow.OpenTag()

	packBytes, err := proto.Marshal(&pack)
	if err != nil {
		return nil, err
	}
	transferFrame.Frame.MessageBytes = packBytes

	transferFrameBytesWithTag, err := proto.Marshal(&transferFrame)
	if err != nil {
		return nil, err
	}

	return transferFrameBytesWithTag, nil
}
*/

func (self *sendItem) unreliableRecoveryPolicy() transferFlightPolicySnapshot {
	return transferFlightPolicySnapshot{limited: self.unreliableCarrierObserved}
}

// observeCarrierWrite makes DATAGRAM recovery sticky for an item once any
// successful attempt can be lost below Transfer. A later reliable retry cannot
// prove that the earlier DATAGRAM was delivered, so the item keeps the shorter
// retry ceiling and longer lifetime until its Transfer ACK arrives. Otherwise
// the exact reliable route is remembered so route retirement can recover
// immediately without running a competing Transfer retransmit train.
func (self *SendSequence) observeCarrierWrite(
	item *sendItem,
	disposition transferWriteDisposition,
) {
	if item.carrierRoute != nil && item.carrierRoute != disposition.route {
		item.carrierChanged = true
	}
	item.carrierRoute = disposition.route
	self.observeLaneSend(item)
	if !disposition.unreliable {
		if disposition.reliable && !item.unreliableCarrierObserved {
			item.reliableCarrierObserved = true
			item.reliableRoute = disposition.route
			item.hybridReliableCarrierObserved = disposition.hybridReliable
		} else {
			item.reliableCarrierObserved = false
			item.reliableRoute = nil
			item.hybridReliableCarrierObserved = false
		}
		return
	}
	item.reliableCarrierObserved = false
	item.reliableRoute = nil
	item.hybridReliableCarrierObserved = false
	item.unreliableCarrierObserved = true
	item.ackTimeout = max(
		item.ackTimeout,
		self.ackTimeoutForPolicy(item.unreliableRecoveryPolicy()),
	)
	self.trackUnreliableFlight(item)
}

// trackUnreliableFlight adds a message only after the selected carrier says
// this exact write can use a lossy lane. TCP still remains ACK-required on
// reliable lanes; this accounting controls admission, not delivery semantics.
func (self *SendSequence) trackUnreliableFlight(item *sendItem) {
	if item.unreliableFlightTracked {
		return
	}
	item.unreliableFlightTracked = true
	item.unreliableFlowReserve = self.flightController.sendForKey(
		item.MessageByteCount(),
		item.schedulingKey,
	)
	if item.unreliableFlowReserve {
		self.client.unreliableFlowReserveUseCount.Add(1)
	}
	self.client.observeUnreliableFlight(self.flightController)
}

// observeItemAck reports acknowledgement evidence for the lane that carried
// the item: the per-route progress clock on the writer, the unreliable last-ack
// age, and reordering suspected when a scheduled gap resend was never needed.
func (self *SendSequence) observeItemAck(item *sendItem) {
	if item.recoveryKind == sendRecoverySelectiveGap {
		self.client.unreliableFlightGapReorderSuspected.Add(1)
	}
	if item.carrierRoute != nil {
		if observer, ok := self.contractMultiRouteWriter.(transferRouteAckProgressObserver); ok {
			observer.observeRouteAckProgress(item.carrierRoute)
		}
	}
	if item.unreliableCarrierObserved {
		self.client.unreliableCarrierLastAckNanos.Store(time.Now().UnixNano())
	}
}

func (self *SendSequence) releaseUnreliableFlight(item *sendItem) {
	if !item.unreliableFlightTracked {
		return
	}
	item.unreliableFlightTracked = false
	self.flightController.acknowledgeForKey(
		item.MessageByteCount(),
		item.schedulingKey,
		item.unreliableFlowReserve,
	)
	item.unreliableFlowReserve = false
	self.client.observeUnreliableFlight(self.flightController)
}

// deliveredBytesRingSize is how many delivery samples a sequence keeps.
//
// Sixty-four at the window rule's ten millisecond cadence is 640 ms of
// history, which spans two round trips at the slowest delay the cell imposes
// and many at the fastest. The rate the window rule reads must cover several
// acknowledgement bursts or it projects the burst ratio rather than the path
// (THROUGHPUTFIX §36.6), and a ring that holds less than a round trip cannot.
// The ring is allocated only for a sequence whose settings turn one of the
// delivery-sized rules on, so an unconfigured client keeps none of it.
const deliveredBytesRingSize = 64

// The window rule's sampling cadence. Fine enough that a rate can be read
// across a short round trip, against the pacing-floor quarter that serves the
// reliable-admission bound, which reads a sum over a horizon and does not care
// (THROUGHPUTFIX §36.6: the 75 ms cadence is coarser than a 25 ms round trip,
// which is why the sum form cannot serve a short path).
const deliverySizedWindowSampleInterval = 10 * time.Millisecond

// The receive hold a legacy peer is known to hold, which is the ceiling until
// that peer advertises its own (THROUGHPUTFIX §37.12). It is the shipping
// `ReceiveQueueMaxByteCount`, memory-scaled exactly as the hold is, so a small
// host advertises less rather than more.
func receiveHoldShippingByteCount() ByteCount {
	return MemoryScaledByteCount(mib(2)+kib(512), kib(320))
}

// deliveredBytesSample is one running total of acknowledged bytes and when
// it was taken.
type deliveredBytesSample struct {
	atNanos int64
	total   ByteCount
}

// observeDeliveredBytes advances the ring on a cumulative acknowledgement.
// The running total only grows, so a sample is a checkpoint rather than a
// rate, and the ring is sized by time rather than by acknowledgement count.
func (self *SendSequence) observeDeliveredBytes(byteCount ByteCount, at time.Time) {
	if byteCount <= 0 || self.deliveredBytes == nil {
		return
	}
	self.deliveredBytesMutex.Lock()
	defer self.deliveredBytesMutex.Unlock()
	self.deliveredByteTotal += byteCount
	atNanos := at.UnixNano()
	interval := self.deliveredBytesSampleInterval().Nanoseconds()
	if 0 < self.deliveredBytesCount {
		newest := self.deliveredBytes[self.deliveredBytesHead]
		if atNanos-newest.atNanos < interval {
			// still inside the newest sample's interval: move its total up
			self.deliveredBytes[self.deliveredBytesHead].total = self.deliveredByteTotal
			return
		}
	}
	self.deliveredBytesHead = (self.deliveredBytesHead + 1) % len(self.deliveredBytes)
	self.deliveredBytes[self.deliveredBytesHead] = deliveredBytesSample{
		atNanos: atNanos,
		total:   self.deliveredByteTotal,
	}
	if self.deliveredBytesCount < len(self.deliveredBytes) {
		self.deliveredBytesCount += 1
	}
}

// The cadence the ring advances at. The window rule needs samples fine enough
// to read a rate across a short round trip; the reliable-admission bound reads
// a sum over a horizon and keeps the pacing-floor quarter it was built with.
func (self *SendSequence) deliveredBytesSampleInterval() time.Duration {
	if 0 < self.sendBufferSettings.DeliverySizedWindowScale {
		return deliverySizedWindowSampleInterval
	}
	return self.sendBufferSettings.RttMinResendInterval / 4
}

// deliveredRate reports the bytes acknowledged between the newest sample and
// the newest sample at least minSpan older than it, with the span it actually
// measured over. A rate rather than a sum over a horizon: the ring advances on
// its own cadence, so a sum returns whatever happened since the newest sample
// older than the horizon, between one and twelve times a short round trip's
// worth depending on where the ring happened to sit (THROUGHPUTFIX §36.6).
// Dividing by the span it measured removes that dependence.
func (self *SendSequence) deliveredRate(minSpan time.Duration) (ByteCount, time.Duration, int64, bool) {
	if self.deliveredBytes == nil {
		return 0, 0, 0, false
	}
	self.deliveredBytesMutex.Lock()
	defer self.deliveredBytesMutex.Unlock()
	if self.deliveredBytesCount < 2 {
		return 0, 0, 0, false
	}
	newest := self.deliveredBytes[self.deliveredBytesHead]
	horizon := newest.atNanos - max(0, minSpan).Nanoseconds()
	older := newest
	for offset := 1; offset < self.deliveredBytesCount; offset += 1 {
		index := (self.deliveredBytesHead - offset + len(self.deliveredBytes)) %
			len(self.deliveredBytes)
		older = self.deliveredBytes[index]
		if older.atNanos <= horizon {
			break
		}
	}
	span := time.Duration(newest.atNanos - older.atNanos)
	if span <= 0 {
		return 0, 0, 0, false
	}
	return max(0, newest.total-older.total), span, older.atNanos, true
}

// Records the receiver's latest advertised hold. Stored atomically because it
// is written from the acknowledgement worker and read by the send loop when it
// sizes its window.
func (self *SendSequence) observeReceiveWindowAdvertisement(ack receiveAckMessage) {
	// Every acknowledgement, advertised or not: a sender that has heard
	// nothing is blind and takes the floor bet, while one whose peer answers
	// without the field has a peer that does not advertise and takes its own
	// constant. Those are different facts and the window is different for
	// each (THROUGHPUTFIX §37.21).
	self.ackSeen.Store(true)
	if !ack.receiveWindowSet {
		return
	}
	if !self.receiveWindowSet.Load() {
		self.receiveWindowSetAtNanos.Store(time.Now().UnixNano())
	}
	self.receiveWindowByteCount.Store(uint64(ack.receiveWindowByteCount))
	self.receiveWindowSet.Store(true)
}

// Resends the items the receiver says it removed from its hold after
// acknowledging them (THROUGHPUTFIX §37.16).
//
// A selective acknowledgement does not release the item. It marks it, pushes
// its resend time out by SelectiveAckTimeout and returns it to the resend
// queue, and every resend path skips a marked item: the paced resend, the gap
// recovery and the carrier-change resend that fires on route death. Only the
// timeout resend clears the mark, a minute later, by which time the sequence's
// own ack timeout is due from the same refreshed send time. So an eviction the
// sender is not told about is a silent withdrawal, and what the failover cell
// measured past the hold threshold is not extra retransmission but a stalled
// transfer.
//
// Clearing the mark and making the item due now is the whole of the fix. It
// takes the unbounded path rather than the gap recovery's burst of four per
// scan, because an eviction generation is as large as what was in flight on a
// dead route.
// Hands an eviction notice to the sequence goroutine.
func (self *SendSequence) observeEvictions(ack receiveAckMessage) {
	if ack.evictions == nil || len(ack.evictions.sequenceNumbers) == 0 {
		return
	}
	self.pendingEvictedMutex.Lock()
	defer self.pendingEvictedMutex.Unlock()
	self.pendingEvictedSequence = append(
		self.pendingEvictedSequence, ack.evictions.sequenceNumbers...)
}

func (self *SendSequence) takePendingEvictions() []uint64 {
	self.pendingEvictedMutex.Lock()
	defer self.pendingEvictedMutex.Unlock()
	if len(self.pendingEvictedSequence) == 0 {
		return nil
	}
	evicted := self.pendingEvictedSequence
	self.pendingEvictedSequence = nil
	return evicted
}

func (self *SendSequence) resendEvicted(evictedSequenceNumbers []uint64) {
	if len(evictedSequenceNumbers) == 0 {
		return
	}
	now := time.Now()
	for _, sequenceNumber := range evictedSequenceNumbers {
		item := self.resendQueue.GetBySequenceNumber(sequenceNumber)
		if item == nil || !item.selectiveAcked {
			// already released, already resent, or never ours
			continue
		}
		removed := self.resendQueue.RemoveBySequenceNumber(sequenceNumber)
		if removed != item {
			panic(errors.New("Missing evicted item"))
		}
		item.selectiveAcked = false
		item.resendTime = now
		item.recoveryKind = sendRecoveryEviction
		self.resendQueue.Add(item)
	}
}

// The latest hold the receiver advertised, and whether it has advertised one.
// A peer that never has is legacy, and the window rule holds at the shipping
// receive hold against it rather than growing to a size it could not take.
func (self *SendSequence) receivedWindowAdvertisement() (ByteCount, bool) {
	if !self.receiveWindowSet.Load() {
		return 0, false
	}
	return ByteCount(self.receiveWindowByteCount.Load()), true
}

// The hold's share less what it currently holds: what this receiver can still
// take out of order. A sender clamps its window to it, because a Pack above
// the hold is dropped and must be sent again, so a window larger than the hold
// turns one loss into one window of retransmission (THROUGHPUTFIX §37.3).
//
// The share is this sequence's own bound, further bounded by what a shared
// receive budget will lend, so a receiver never advertises memory it would
// have to borrow from its other sequences.
// Records an eviction for the next acknowledgement to carry.
func (self *ReceiveSequence) noteEviction(sequenceNumber uint64) {
	self.evictedMutex.Lock()
	defer self.evictedMutex.Unlock()
	if evictionNoticeMaxCount <= len(self.evictedSequenceNumbers) {
		self.client.receiveQueueEvictionNoticeOverflow.Add(1)
		return
	}
	self.evictedSequenceNumbers = append(self.evictedSequenceNumbers, sequenceNumber)
}

// Takes the pending evictions for one acknowledgement to carry.
func (self *ReceiveSequence) takeEvictions() []uint64 {
	self.evictedMutex.Lock()
	defer self.evictedMutex.Unlock()
	if len(self.evictedSequenceNumbers) == 0 {
		return nil
	}
	evicted := self.evictedSequenceNumbers
	self.evictedSequenceNumbers = nil
	return evicted
}

// What the receiver may hold out of order, measured from the delivered point:
// its capacity, not its free space.
//
// THROUGHPUTFIX §37.16 corrects §37.3 here. Capacity less what is held double
// counts, because a selective acknowledgement does not release the item — the
// sender keeps it in its resend queue on a lease — so held bytes are already
// inside the sender's outstanding count. Subtracting them would shrink the
// window by the held amount for nothing and, as the hold fills after a route
// death, pull the right edge of the window inward, which is the one thing a
// window must never do. The figure is monotone except when the budget moves.
//
// The rule this pairs with on the sender: outstanding-from-delivered at most
// this. A sender that keeps to it can never force an eviction whatever the gap
// structure, because the hold would have to contain more than the sender has
// outstanding.
func (self *ReceiveSequence) receiveWindowAdvertisement() uint64 {
	share := self.receiveBufferSettings.ReceiveQueueMaxByteCount
	if budget := self.receiveQueue.Budget(); budget != nil {
		share = min(share, budget.TotalByteCount())
	}
	return uint64(max(0, share))
}

// How many samples the ring holds, for the window rule's evidence.
func (self *SendSequence) deliveredSampleCount() int {
	if self.deliveredBytes == nil {
		return 0
	}
	self.deliveredBytesMutex.Lock()
	defer self.deliveredBytesMutex.Unlock()
	return self.deliveredBytesCount
}

// deliveredBytesOver reports what this lane acknowledged in the last d: the
// running total now, less the total at the newest sample older than d. A
// scan of at most sixteen entries, allocation-free.
func (self *SendSequence) deliveredBytesOver(d time.Duration, now time.Time) ByteCount {
	if self.deliveredBytes == nil || d <= 0 {
		return 0
	}
	self.deliveredBytesMutex.Lock()
	defer self.deliveredBytesMutex.Unlock()
	if self.deliveredBytesCount == 0 {
		return 0
	}
	horizon := now.Add(-d).UnixNano()
	base := ByteCount(0)
	found := false
	for offset := range self.deliveredBytesCount {
		index := (self.deliveredBytesHead - offset + len(self.deliveredBytes)) %
			len(self.deliveredBytes)
		sample := self.deliveredBytes[index]
		if sample.atNanos <= horizon {
			base = sample.total
			found = true
			break
		}
	}
	if !found {
		// the whole history is inside the window; the oldest sample is the
		// earliest total this sequence can attribute
		oldest := (self.deliveredBytesHead - self.deliveredBytesCount + 1 +
			len(self.deliveredBytes)) % len(self.deliveredBytes)
		base = self.deliveredBytes[oldest].total
	}
	return max(0, self.deliveredByteTotal-base)
}

// What the window rule computed and the evidence it computed it from, carried
// together for the same reason the round-trip estimate is: a campaign has to
// confirm the rule engaged, and this program has twice run campaigns against
// settings that silently did nothing.
type SendWindowEstimate struct {
	// what the sequence may hold unacknowledged, which is the initial size
	// when the rule is off or holding
	Window ByteCount
	// Sized is whether the rule is on and had every bound it needs. False
	// means Window is the initial size, which is a different fact from a
	// measured window that happens to equal it, and Reason says which bound
	// was missing.
	Sized  bool
	Reason string
	// the rate the window was computed from: bytes acknowledged over the span
	// they were acknowledged in, times the minimum round trip, times the scale
	DeliveredByteCount ByteCount
	Interval           time.Duration
	RoundTrip          time.Duration
	SampleCount        int
	// the bounds the rule clamped between. Initial is the pre-sample value
	// and is not a lower clamp: once sampled the rule may shrink below it, to
	// Floor, which is a working minimum of a few packets.
	Initial ByteCount
	Floor   ByteCount
	Ceiling ByteCount
}

// sendWindowEstimate is the window this sequence may hold unacknowledged, and
// the evidence behind it.
//
// The form is `scale × rate × minimum round trip`, clamped. Three parts of
// that are deliberate and each replaced something that did not work.
//
// The multiplier is the minimum round trip and not the mean. The sender stamps
// its tag ahead of the writer, so the mean contains the queue the window
// itself creates: on a path bound below this sequence a window sized from the
// mean feeds back on its own output and converges to its ceiling rather than
// to the path. The minimum is the sample that queued least, which is the only
// one that describes the path (THROUGHPUTFIX §36.7).
//
// The delivery term is a rate and not a sum over a horizon, for the reason
// `deliveredRate` gives.
//
// A consequence worth stating because it removes a feature rather than adding
// one: the plateau is this rule's fixed point without any detector. A window
// larger than the path can carry produces no more delivery, so the rate stops
// rising and the window stops with it, at the scale times the path's delivery
// per round trip. The ceiling is then the backstop it was designed as, and a
// well-behaved path never reaches it.
//
// The policy for what happens when a bound is missing is one sentence: the
// sender sizes only against a bound it can see, and holds the initial size
// where it cannot. A budget for memory, an advertisement for the receiver, an
// estimate with samples for the round trip. That makes the safe configuration
// the default and the unsafe one unreachable, rather than documented.
func (self *SendSequence) sendWindowEstimate(now time.Time) SendWindowEstimate {
	// The initial size is the value before the estimate has samples, and
	// nothing after it. It is a bet, and a bet that cannot be walked back is
	// not a bet: as the rule's lower clamp a wide-area initial would stand as
	// a second of queue on a slow last mile for the life of the sequence
	// (THROUGHPUTFIX §37.13). Once sampled the rule may shrink to what the
	// path shows, and the only floor under it is the working minimum that
	// reliable admission already uses.
	//
	// `initial` here is this sender's own constant window, which is what a
	// peer that answers without the advertisement gets. The blind bet is the
	// receive hold's floor and is applied below.
	initial := self.sendBufferSettings.ResendQueueMaxByteCount
	floor := self.sendBufferSettings.ResendQueueMinByteCount
	if floor <= 0 {
		floor = initial
	}
	estimate := SendWindowEstimate{Window: initial, Initial: initial, Floor: floor}
	scale := self.sendBufferSettings.DeliverySizedWindowScale
	if scale <= 0 || self.deliveredBytes == nil {
		estimate.Reason = "the rule is off"
		return estimate
	}

	// Memory it can see: absent a shared budget there is nothing bounding what
	// every sequence of a provider holds together, so the window holds at the
	// initial size rather than growing against a per-sequence maximum that
	// forty downloaders would multiply.
	budget := self.resendQueue.Budget()
	if budget == nil {
		// Not a small share: the absence of the surface. An unbudgeted process
		// keeps today's constant rather than falling to a floor, which would
		// make every unbudgeted provider slower the moment the rule is on
		// (THROUGHPUTFIX §37.22). Named so it is legible rather than silent.
		estimate.Reason = "no memory budget: holding today's constant"
		return estimate
	}
	ceiling := self.sendBufferSettings.DeliverySizedWindowCeilingByteCount
	if ceiling <= 0 {
		ceiling = initial
	}
	ceiling = max(ceiling, floor)
	// What this sequence can obtain rather than what the pool holds. With many
	// sequences drawing on one budget — a provider serving many downloaders is
	// the case, and the only one where a per-sequence window multiplies
	// against a fixed total — every sequence reporting the pool's total is
	// every sequence claiming permission none of them has. Measured with eight
	// sequences on a 2 MiB pool before this: all eight reported a 2 MiB
	// ceiling while none could have held more than a fraction of it.
	//
	// The floor is unconditional, so nothing here can starve a sequence: a
	// queue below its floor is admitted whatever the pool says, which is also
	// why the aggregate bound is the floors plus the budget rather than the
	// budget alone.
	if obtainable := self.resendQueue.ObtainableByteCount(); 0 < obtainable {
		ceiling = min(ceiling, max(obtainable, floor))
	} else {
		ceiling = min(ceiling, budget.TotalByteCount())
	}

	// The receiver it can see, and the three cases are different facts
	// (THROUGHPUTFIX §37.21).
	//
	// Advertised: the peer's capacity is the largest harmless window, because
	// permission is not occupancy and the one harm of an oversized window is
	// the overrun the advertisement bounds. The window steps to it the moment
	// it is learned rather than climbing toward it, or the derivation buys
	// nothing.
	//
	// Answered without the field: a peer that does not advertise gets today's
	// window at this sender's own scale. Not the shipping hold constant, which
	// would be a raise on no evidence, and not the blind floor, which would be
	// a regression for every peer not yet updated. The status quo, whose stall
	// guard two mitigates.
	//
	// Silent: still blind, and a blind sender may assume only what every
	// receiver already ships, the receive hold's floor. That is a constant the
	// receiver owns rather than one configured here, which is the distinction
	// that removes the third knob: the surface is a target and a budget, and
	// the initial size is read rather than chosen. The blind period is one
	// round trip, not the life of the sequence, and it is ordinarily free: the
	// encryption handshake rides this sequence and is sent acknowledged, so
	// the advertisement returns before the first data pack, and where data
	// does come first a TCP flow needs four or five round trips of slow start
	// to reach 320 KiB of congestion window, so this window is not the binder
	// meanwhile. Only a datagram source at full rate on a fresh sequence pays
	// it, once per sequence, and it pays one round trip rather than a ramp.
	stepAtNanos := int64(0)
	if advertised, ok := self.receivedWindowAdvertisement(); ok {
		ceiling = min(ceiling, advertised)
		stepAtNanos = self.receiveWindowSetAtNanos.Load()
	} else if self.ackSeen.Load() {
		ceiling = min(ceiling, initial)
	} else {
		ceiling = min(ceiling, defaultInitialWindowByteCount())
	}
	estimate.Ceiling = ceiling
	// The rule, stated here because the next layer to gain an initial size
	// will need it and because omitting it is what this implementation got
	// wrong first.
	//
	// A peer's advertised bound is the outermost clamp. It applies to the
	// initial bet before there are samples and to the sized window after, from
	// the first Pack rather than from the first sample. A receiver's capacity
	// does not depend on the sender having measured a round trip, and
	// §37.16's rule — outstanding-from-delivered at most the advertised
	// capacity — has to hold from the first Pack or the opening burst is
	// exactly what overruns the hold. Measured before this clamp existed: a
	// 4 MiB initial against a 256 KiB advertised hold gave 35 evictions and
	// 330 refused arrivals while the sized window was reporting 64 KiB, which
	// is a window rule that looks correct in its own fields and overruns the
	// receiver anyway.
	//
	// The same applies to every layer that later takes its initial bet from
	// §37.4's configuration surface: the bet is what a layer believes about a
	// path it has not measured, and a bound its peer has stated is not a
	// belief. Clamp the bet, not only the estimate.
	estimate.Window = max(min(initial, ceiling), floor)

	// The round trip it can see.
	roundTrip := self.rttWindow.Estimate()
	if !roundTrip.Sampled() {
		estimate.Reason = "no round trip samples"
		return estimate
	}
	estimate.RoundTrip = roundTrip.Min
	estimate.SampleCount = self.deliveredSampleCount()

	// The target's own bandwidth-delay product, so a path faster than the
	// target does not take more than the target (§37.4). Framed bytes, so the
	// goodput target divides by the factor §36.3 derives.
	if 0 < self.sendBufferSettings.TargetGoodputByteRate {
		targetWindow := ByteCount(
			float64(self.sendBufferSettings.TargetGoodputByteRate) *
				roundTrip.Min.Seconds() / goodputFactor,
		)
		ceiling = min(ceiling, max(targetWindow, floor))
		estimate.Ceiling = ceiling
		estimate.Window = max(min(initial, ceiling), floor)
	}

	// the rate must span several acknowledgement bursts, and at least a couple
	// of round trips where those are long
	minSpan := max(
		2*roundTrip.Min,
		4*self.deliveredBytesSampleInterval(),
	)
	delivered, span, spanStartNanos, ok := self.deliveredRate(minSpan)
	if !ok {
		estimate.Reason = "no delivery rate samples"
		return estimate
	}
	estimate.DeliveredByteCount = delivered
	estimate.Interval = span

	// The delivery term, one-sided and lagged.
	//
	// One-sided: it may lower the window below the peer's capacity but never
	// raise it above what the peer said it can hold. Lagged: it acts only on
	// delivery measured wholly after the window stepped to that capacity.
	// Both are needed together. After the step, the delivery measured during
	// the blind round trip is small, and a two-sided cap reading it would drag
	// the window straight back down and reimpose the ramp the step exists to
	// remove. With the lag, a path carrying the full capacity per round trip
	// reads twice that and the cap does not bind, while a path carrying less
	// comes down on its own evidence. The protection against oversizing is
	// intact and it only ever acts downward.
	if spanStartNanos < stepAtNanos {
		estimate.Reason = "delivery measured before the window stepped"
		return estimate
	}
	estimate.Sized = true
	perRoundTrip := ByteCount(int64(delivered) * roundTrip.Min.Nanoseconds() / span.Nanoseconds())
	if capped := ByteCount(scale) * perRoundTrip; capped < estimate.Window {
		estimate.Window = max(capped, floor)
		estimate.Reason = "delivery"
	} else {
		estimate.Reason = estimate.bindingTerm(self)
	}
	return estimate
}

// Names the bound the window landed on, so a flow that cannot reach the target
// says why in its own fields rather than sitting silently at a number. That
// visibility is the difference between this rule and the constants it
// replaces: a phone on a long path is bound by its advertised capacity and
// says so, where before it was bound by a constant and said nothing.
func (self SendWindowEstimate) bindingTerm(sequence *SendSequence) string {
	if self.Window <= self.Floor {
		return "floor"
	}
	if self.Window < self.Ceiling {
		return "the initial bet"
	}
	if advertised, ok := sequence.receivedWindowAdvertisement(); ok &&
		advertised <= self.Ceiling {
		return "the peer's advertised capacity"
	}
	if budget := sequence.resendQueue.Budget(); budget != nil &&
		budget.TotalByteCount() <= self.Ceiling {
		return "the memory budget's share"
	}
	if 0 < sequence.sendBufferSettings.TargetGoodputByteRate {
		return "the target"
	}
	return "sized"
}

// reliableAdmissionByteLimit is what this sequence may hold unacknowledged
// on a reliable lane: what the lane delivered over one scaled round trip,
// never below the per-sequence floor. A lane may hold what it has shown it
// can carry, which is a statement about the lane rather than an estimate of
// its rate (FLIGHTGATEFIX §22).
func (self *SendSequence) reliableAdmissionByteLimit(now time.Time) ByteCount {
	floor := self.sendBufferSettings.ResendQueueMinByteCount
	delivered := self.deliveredBytesOver(self.rttWindow.ScaledRtt(), now)
	return max(floor, delivered)
}

// reliableUnackedBytes is what the reliable lane holds: everything awaiting
// acknowledgement less what the unreliable flight already bounds.
func (self *SendSequence) reliableUnackedBytes() ByteCount {
	_, queued := self.resendQueue.QueueSize()
	if self.flightController == nil {
		return queued
	}
	return max(0, queued-self.flightController.byteCount)
}

// reliableAdmissionAvailable reports whether a new Pack may be admitted to
// a reliable lane now. It applies to a Pack whose write would be
// reliable-only, and to every new Pack when no unreliable carrier is
// active; retransmits, probes and gap recoveries are not admissions.
func (self *SendSequence) reliableAdmissionAvailable(
	policy transferFlightPolicySnapshot,
	now time.Time,
) bool {
	if !self.sendBufferSettings.ReliableAdmissionBoundedByDelivery {
		return true
	}
	if policy.limited && !self.reliableOnlyWrite(policy) {
		// the unreliable flight admits this Pack and bounds it already
		return true
	}
	limit := self.reliableAdmissionByteLimit(now)
	if self.sendBufferSettings.ResendQueueMinByteCount < limit {
		for {
			current := self.client.reliableAdmissionByteLimitMinimum.Load()
			if current != 0 && current <= uint64(limit) {
				break
			}
			if self.client.reliableAdmissionByteLimitMinimum.CompareAndSwap(current, uint64(limit)) {
				break
			}
		}
	}
	return self.reliableUnackedBytes() < limit
}

// unreliableFlightGates reports whether a full unreliable flight must stop
// admitting packs. That is only the case when no reliable carrier is active:
// with one, the overflow is written reliable-only instead of stalling the
// sequence (and, on a client, its entire receive path behind it).
// observeAckRtt feeds the sequence's RTT window from an acknowledged item,
// except for items carried by an unreliable lane. A direct datagram lane
// answers in ~20 ms while the relay carrier takes 150-300 ms; mixing both into
// one window drags the scaled RTO down to its floor and every relay-carried
// item is resent before its ACK can arrive (thousands of spurious timeout
// resends per second while p2p is live). The unreliable lane has its own
// bounded recovery policy and does not depend on this estimate.
func (self *SendSequence) observeAckRtt(item *sendItem, tag sequenceTag) {
	if !tag.set || item == nil || item.unreliableCarrierObserved {
		return
	}
	self.rttWindow.CloseSendTime(tag.sendTime)
}

func (self *SendSequence) unreliableFlightGates(
	policy transferFlightPolicySnapshot,
) bool {
	return self.flightController.limited && !policy.reliableRouteAvailable
}

// reliableOnlyWrite reports whether the next write must avoid the unreliable
// carrier because its flight is full and a reliable carrier can take it.
func (self *SendSequence) reliableOnlyWrite(
	policy transferFlightPolicySnapshot,
) bool {
	if !policy.reliableRouteAvailable || !self.flightController.limited {
		return false
	}
	if !self.flightController.canSend() {
		return true
	}
	// A carrier that loss has reduced to its floor keeps proving itself with
	// a single message in flight; the ordered stream is not striped onto it.
	return self.sendBufferSettings.UnreliableFloorSingleFlight &&
		self.flightController.atFloor() &&
		0 < self.flightController.messageCount
}

// observeUnreliableResendTimeout applies the RTO of an unreliable-tracked item.
// An RTO is the only congestion evidence available for a lost tail or a lost
// cumulative Ack. QUIC does not retransmit the DATAGRAM payload, so its
// lower-layer congestion response cannot release this Transfer flight; halve
// admission before retrying. When a reliable carrier is active the resend goes
// there, so the item leaves the unreliable flight now instead of holding the
// whole sequence until the (possibly dead) unreliable carrier acknowledges it.
// Returns whether the resend must be written reliable-only.
func (self *SendSequence) observeUnreliableResendTimeout(
	item *sendItem,
	policy transferFlightPolicySnapshot,
) bool {
	self.client.unreliableFlightTimeoutCount.Add(1)
	if self.flightController.reduceForLoss() {
		self.client.unreliableFlightReductionCount.Add(1)
	}
	if !policy.reliableRouteAvailable {
		self.client.observeUnreliableFlight(self.flightController)
		return false
	}
	self.forgetUnreliableFlight(item)
	return true
}

// forgetUnreliableFlight drops a timed-out item from the unreliable flight
// so the reliable lane can carry its resend; unlike releaseUnreliableFlight
// it credits no delivery, so the window that reduceForLoss just halved
// does not grow back on the same timeout.
func (self *SendSequence) forgetUnreliableFlight(item *sendItem) {
	if !item.unreliableFlightTracked {
		return
	}
	item.unreliableFlightTracked = false
	self.flightController.forget(
		item.MessageByteCount(),
		item.schedulingKey,
		item.unreliableFlowReserve,
	)
	item.unreliableFlowReserve = false
	self.client.observeUnreliableFlight(self.flightController)
}

func (self *SendSequence) receiveAck(
	messageId Id,
	selective bool,
	tag sequenceTag,
	compactContractRecoverySupported bool,
) {
	item := self.resendQueue.GetByMessageId(messageId)
	if item == nil {
		if self.log.V(1).Enabled() {
			self.log.Infof("[s]ack miss %s->%s...%s s(%s)\n", self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId)
		}
		// message not pending ack
		return
	}

	self.observeAckRtt(item, tag)

	if selective {
		if self.log.V(1).Enabled() {
			self.log.Infof("[s]ack selective %s->%s...%s s(%s)\n", self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId)
		}
		removed := self.resendQueue.RemoveByMessageId(messageId)
		if removed == nil {
			panic(errors.New("Missing item"))
		}
		if !item.selectiveAcked {
			self.observeItemAck(item)
			self.releaseUnreliableFlight(item)
		}
		ackTime := time.Now()
		// §34.3: whether this item was its lane's oldest unacknowledged has
		// to be read before it is marked, because that is what the mark
		// changes.
		laneHeadAcked := self.laneOldestOutstanding(item.carrierRoute) == item
		self.observeLaneAck(item, ackTime)
		self.observeReliableLaneAck(item, ackTime)
		// refresh sendTime so the ack-timeout deadline includes the selective-ack window
		item.sendTime = time.Now()
		item.resendTime = item.sendTime.Add(self.sendBufferSettings.SelectiveAckTimeout)
		item.selectiveAcked = true
		self.resendQueue.Add(item)
		if laneHeadAcked && !item.carrierChanged && 1 < item.sendCount {
			if slot := self.laneSlotFor(item.carrierRoute); 0 <= slot {
				self.promoteLaneHeads(uint32(1)<<uint(slot), ackTime)
			}
		}
		return
	}

	// `ackItem` returns each acknowledged item to the process-wide send-item
	// pool. The target item can therefore be zeroed and reused by another
	// SendSequence while this cumulative-ack loop is still advancing. Snapshot
	// the boundary before returning anything; never read `item` afterward.
	ackSequenceNumber := item.sequenceNumber
	if self.log.V(1).Enabled() {
		self.log.Infof("[s]ack %d %s->%s...%s s(%s)\n", ackSequenceNumber, self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId)
	}

	self.lastCumulativeAckTime = time.Now()
	// FLIGHTGATEFIX §22: what this lane delivered, the measure the reliable
	// admission bound reads.
	cumulativeByteCount := ByteCount(0)
	// §34.3: an acknowledgement is cumulative, so every lane it touches has
	// had its oldest unacknowledged item acknowledged. Collect those lanes
	// and promote their new heads once the acknowledged prefix is gone.
	promoteLanes := uint32(0)
	var promoteRoute Route
	// acks are cumulative
	// implicitly ack all earlier items in the sequence
	i := 0
	for ; i < len(self.sendItems); i += 1 {
		implicitItem := self.sendItems[i]
		implicitSequenceNumber := implicitItem.sequenceNumber
		if ackSequenceNumber < implicitSequenceNumber {
			if self.log.V(2).Enabled() {
				self.log.Infof("[s]ack %d <> %d/%d (stop) %s->%s...%s s(%s)\n", ackSequenceNumber, implicitSequenceNumber, self.nextSequenceNumber-1, self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId)
			}
			break
		}

		var a int
		var b ByteCount
		if self.log.V(2).Enabled() {
			a, b = self.resendQueue.QueueSize()
		}

		// self.ackedSequenceNumbers[implicitItem.sequenceNumber] = true
		removed := self.resendQueue.RemoveByMessageId(implicitItem.messageId)
		if removed == nil {
			panic(errors.New("Missing item"))
		}

		cumulativeByteCount += implicitItem.MessageByteCount()
		self.observeLaneAck(implicitItem, self.lastCumulativeAckTime)
		self.observeReliableLaneAck(implicitItem, self.lastCumulativeAckTime)
		if implicitItem.carrierRoute != nil && !implicitItem.carrierChanged &&
			implicitItem.carrierRoute != promoteRoute {
			promoteRoute = implicitItem.carrierRoute
			// this is the lane's oldest unacknowledged item, since the
			// acknowledged items are a prefix of the sequence. Its send
			// count is above one exactly when the sender had to write it
			// again to get this acknowledgement.
			if slot := self.laneSlotFor(promoteRoute); 0 <= slot &&
				1 < implicitItem.sendCount {
				promoteLanes |= uint32(1) << uint(slot)
			}
		}
		if !implicitItem.selectiveAcked {
			self.observeItemAck(implicitItem)
			self.releaseUnreliableFlight(implicitItem)
		}
		// A compact head is safe only after this receiver acknowledged a full
		// proof while explicitly advertising how it recovers missing state.
		// Scope the capability to the current contract so a rotation must prove
		// it again and a legacy peer never receives a compact-only head.
		if compactContractRecoverySupported && implicitItem.hasContractFrame &&
			implicitItem.contractId != nil {
			if itemSendContract, ok := self.openSendContracts[*implicitItem.contractId]; ok {
				if !itemSendContract.compactContractRecoverySupported {
					itemSendContract.compactContractRecoverySupported = true
					self.client.compactRecoveryContractCount.Add(1)
				}
			}
		}
		self.ackItem(implicitItem)
		if self.sendBuffer != nil && self.sendBuffer.afterAckSendItemForTest != nil {
			self.sendBuffer.afterAckSendItemForTest(self.id(), implicitSequenceNumber)
		}
		self.sendItems[i] = nil

		if self.log.V(2).Enabled() {
			c, d := self.resendQueue.QueueSize()
			self.log.Infof("[s]ack %d <> %d/%d (pass %d->%d %dB->%dB) %s->%s...%s s(%s)\n", ackSequenceNumber, implicitSequenceNumber, self.nextSequenceNumber-1, a, c, b, d, self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId)
		}
	}
	self.sendItems = self.sendItems[i:]
	if promoteLanes != 0 {
		self.promoteLaneHeads(promoteLanes, self.lastCumulativeAckTime)
	}
	self.observeDeliveredBytes(cumulativeByteCount, self.lastCumulativeAckTime)
	if self.log.V(2).Enabled() {
		a, b := self.resendQueue.QueueSize()
		self.log.Infof("[s]ack %d/%d (stop %d %dB %d) %s->%s...%s s(%s)\n", ackSequenceNumber, self.nextSequenceNumber-1, a, b, len(self.sendItems), self.client.ClientTag(), self.contractIntermediaryIds(), self.destination, self.contractMultiRouteWriterAlias.StreamId)
	}
}

func (self *SendSequence) ackItem(item *sendItem) {
	if item.contractId != nil {
		if itemSendContract, ok := self.openSendContracts[*item.contractId]; ok {
			itemSendContract.ack(item.messageByteCount)
			// not current and closed
			if self.sendContract != itemSendContract && itemSendContract.unackedByteCount == 0 {
				self.client.ContractManager().CloseContract(
					itemSendContract.contractId,
					itemSendContract.ackedByteCount,
					itemSendContract.unackedByteCount,
				)
				delete(self.openSendContracts, itemSendContract.contractId)
			}
		}
	}
	item.acks.invoke(nil)
	item.messagePoolReturn()
}

// writeMaybeWrappedBytes writes `transferFrameBytes` through the contract
// multi-route writer. When the per-peer session has a cipher, the bytes are
// outer-wrapped as `TransferFrame{TransferPath, encryptedTransferFrame:
// <ciphertext>}` before being written. Encryption is a binary property of
// the session: cipher set → wrap; cipher nil → pass-through. `path` is the
// wire TransferPath the outer wrap reproduces so forwarders see the same
// routing path either way.
//
// `forceUnwrapped` pins this frame to plaintext regardless of cipher
// state. TLS handshake EncryptedControl frames use this to keep the
// handshake bootstrap legible to the peer — including on retransmit,
// where the local cipher may have become available after the original
// send but the peer has not yet completed its half of the handshake.
//
// Before wrapping, the peer's TLS certificate is verified against the
// active contract's `ProvideTlsCertificate` commitment. A mismatch is a
// loud error: the frame is dropped (the SendSequence will retry, and
// eventually time out, rather than transmit application data sealed under
// the wrong identity).
func (self *SendSequence) writeMaybeWrappedBytes(
	transferFrameBytes []byte,
	path TransferPath,
	forceUnwrapped bool,
	item *sendItem,
	resend bool,
	reliableOnly bool,
) (transferWriteDisposition, error) {
	writer := self.openContractMultiRouteWriter()
	// A full unreliable flight must not stall this sequence while a reliable
	// carrier is active: route the overflow reliable-only so it is neither
	// tracked in the flight nor lost with the unreliable carrier.
	reliableOnly = reliableOnly || self.reliableOnlyWrite(self.transferFlightPolicy())
	var cipher *sequenceCipher
	if self.session != nil && !forceUnwrapped {
		cipher = self.session.Cipher()
	}
	if cipher == nil && self.session != nil && !forceUnwrapped && self.session.RequireEncryption() {
		// Fail-closed backstop (EncryptionModeRequired): an application frame
		// reached the writer without a cipher. The entry gate
		// (`SendSequence.Pack`) admits application packs only once the cipher
		// is established, and an established session keeps serving a cipher
		// through rekeys (`Cipher()`), so this fires only on a narrow race
		// (e.g. the session torn down between enqueue and write). Refuse the
		// write — the item stays queued for resend and the sequence winds down
		// via its own timeouts — rather than ever emitting plaintext.
		return transferWriteDisposition{}, fmt.Errorf(
			"encryption required but no cipher for peer %s (fail-closed; not sent)",
			self.destination,
		)
	}
	if cipher == nil {
		// guard the V(2) diagnostic: this is the per-packet plaintext write path,
		// and the disabled-level call would still box ClientTag/DestinationId/
		// StreamId/len into []any on the heap every packet.
		if self.log.V(2).Enabled() {
			self.log.Infof(
				"[s]%s->%s s(%s) write plaintext %d bytes (forceUnwrapped=%t, session=%t, cipher=nil)\n",
				self.client.ClientTag(),
				self.destination,
				self.contractMultiRouteWriterAlias.StreamId,
				len(transferFrameBytes),
				forceUnwrapped,
				self.session != nil,
			)
		}
		bytes := transferFrameBytes
		if DebugTransferCopyOnWrite {
			bytes = MessagePoolCopy(transferFrameBytes)
			// the copy is scoped to this write; the item's transferFrameBytes stays
			// owned by the sequence
			defer MessagePoolReturn(bytes)
		}
		self.observeTransferWireMessage(bytes, transferFrameBytes, item, resend)
		shared := MessagePoolShareReadOnly(bytes)
		disposition, err := writeMultiRouteWithCarrier(
			writer,
			self.ctx,
			shared,
			self.sendBufferSettings.WriteTimeout,
			reliableOnly,
		)
		if err != nil {
			// on failure (abort/timeout) no route consumer took the message, so
			// ownership stays here: undo the consumer's share or the buffer can
			// never reach zero references and silently leaves the pool
			MessagePoolReturn(shared)
		}
		return disposition, err
	}
	if err := self.verifyPeerCertAgainstContract(); err != nil {
		return transferWriteDisposition{}, err
	}
	wrapped, err := cipher.SealOuterFrame(
		path,
		transferFrameBytes,
		self.session.role.toProtobuf(),
		self.session.companion,
	)
	if err != nil {
		return transferWriteDisposition{}, fmt.Errorf("outer wrap seal: %w", err)
	}
	// guard the V(2) diagnostic: this is the per-packet wrapped write path; see
	// the plaintext branch above for why the disabled-level call still allocates.
	if self.log.V(2).Enabled() {
		self.log.Infof(
			"[s]%s->%s s(%s) write wrapped %d -> %d bytes\n",
			self.client.ClientTag(),
			self.destination,
			self.contractMultiRouteWriterAlias.StreamId,
			len(transferFrameBytes), len(wrapped),
		)
	}
	defer MessagePoolReturn(wrapped)
	self.observeTransferWireMessage(wrapped, transferFrameBytes, item, resend)
	shared := MessagePoolShareReadOnly(wrapped)
	disposition, err := writeMultiRouteWithCarrier(
		writer,
		self.ctx,
		shared,
		self.sendBufferSettings.WriteTimeout,
		reliableOnly,
	)
	if err != nil {
		// see the plaintext branch: a failed write leaves ownership here
		MessagePoolReturn(shared)
	}
	return disposition, err
}

// Publishes borrowed wire/Transfer bytes plus the send-item state needed to
// distinguish initial heads from cumulative-progress promotions. The contract
// lookup is skipped entirely when diagnostics are disabled.
func (self *SendSequence) observeTransferWireMessage(
	wireMessageBytes []byte,
	transferFrameBytes []byte,
	item *sendItem,
	resend bool,
) {
	observer := self.sendBufferSettings.TransferWireMessageObserver
	if observer == nil {
		return
	}
	compactContractRecoverySupported := false
	if item.contractId != nil {
		if sendContract, ok := self.openSendContracts[*item.contractId]; ok {
			compactContractRecoverySupported =
				sendContract.compactContractRecoverySupported
		}
	}
	safeTransferWireMessageObserve(
		observer,
		TransferWireMessageObservation{
			WireMessageBytes:                 wireMessageBytes,
			TransferFrameBytes:               transferFrameBytes,
			MessageId:                        item.messageId,
			SequenceNumber:                   item.sequenceNumber,
			SendCount:                        item.sendCount,
			Resend:                           resend,
			PromotedHead:                     item.promotedHead,
			CompactContractRecoverySupported: compactContractRecoverySupported,
		},
	)
}

func writeMultiRouteWithCarrier(
	writer MultiRouteWriter,
	ctx context.Context,
	transferFrameBytes []byte,
	timeout time.Duration,
	reliableOnly bool,
) (transferWriteDisposition, error) {
	if reliableOnly {
		if reliableWriter, ok := writer.(transferReliableOnlyMultiRouteWriter); ok {
			success, disposition, err := reliableWriter.writeDetailedReliableOnly(
				ctx,
				transferFrameBytes,
				timeout,
			)
			if err != nil {
				return transferWriteDisposition{}, err
			}
			if !success {
				return transferWriteDisposition{}, errTransferRouteWriteTimeout
			}
			if disposition.transportType == "" {
				disposition.transportType = TransportTypeUnknown
			}
			return disposition, nil
		}
	}
	if carrierWriter, ok := writer.(transferCarrierMultiRouteWriter); ok {
		success, disposition, err := carrierWriter.writeDetailedWithCarrier(
			ctx,
			transferFrameBytes,
			timeout,
		)
		if err != nil {
			return transferWriteDisposition{}, err
		}
		if !success {
			return transferWriteDisposition{}, errTransferRouteWriteTimeout
		}
		if disposition.transportType == "" {
			disposition.transportType = TransportTypeUnknown
		}
		return disposition, nil
	}
	if transportWriter, ok := writer.(TransportMultiRouteWriter); ok {
		success, transportType, err := transportWriter.WriteDetailedWithTransport(
			ctx,
			transferFrameBytes,
			timeout,
		)
		if err != nil {
			return transferWriteDisposition{}, err
		}
		if !success {
			return transferWriteDisposition{}, errTransferRouteWriteTimeout
		}
		if transportType == "" {
			transportType = TransportTypeUnknown
		}
		return transferWriteDisposition{transportType: transportType}, nil
	}
	return transferWriteDisposition{transportType: TransportTypeUnknown}, writer.Write(ctx, transferFrameBytes, timeout)
}

// verifyPeerCertAgainstContract checks (and caches) that the peer's TLS cert
// matches a chain the destination committed to in some contract this session
// has seen. The trusted set is maintained by `AddTrustedPeerCertChain` (from
// `setContract`); every cert the peer has published is acceptable, so rotation
// is tolerated without breaking in-flight sessions. Skipped when:
//
//   - this is a companion-mode reply: the companion sender re-uses the session
//     cipher established by the original direction's handshake.
//   - the trusted set is empty (no contract seen, or all carried an empty
//     `ProvideTlsCertificate`): skip without latching, so a later contract with
//     a cert re-arms verification.
//
// Once matched, the result is cached and not re-run for this session.
func (self *SendSequence) verifyPeerCertAgainstContract() error {
	if self.session == nil {
		return nil
	}
	if self.companionContract {
		if self.log.V(1).Enabled() {
			self.log.Infof(
				"[s]%s->%s s(%s) companion reply: reusing per-peer session cipher; skipping cert verification\n",
				self.client.ClientTag(),
				self.destination,
				self.contractMultiRouteWriterAlias.StreamId,
			)
		}
		return nil
	}
	verified, noCommitment := self.session.CertVerificationState()
	if verified || noCommitment {
		return nil
	}
	expected := self.session.trustedPeerCertSnapshot()
	// V(2) diagnostic: verify against the established epoch (whose cipher seals
	// this frame), not the in-flight currentEpoch() whose ConnectionState() blocks
	// on the running handshake. Logged so reaching this path (trusted set armed) is
	// observable.
	if self.log.V(2).Enabled() {
		self.log.Infof(
			"[s][cert-verify]%s->%s s(%s) verifying established-epoch peer certs (non-blocking); trustedSet=%d companion=%t\n",
			self.client.ClientTag(),
			self.destination,
			self.contractMultiRouteWriterAlias.StreamId,
			len(expected),
			self.companionContract,
		)
	}
	peerCerts := self.session.establishedPeerCertificates()
	ok, err := verifyPeerCertificateAgainstContract(peerCerts, expected)
	if err != nil {
		self.log.Errorf(
			"[s]%s->%s s(%s) sequence TLS cert verification failed: %s (peer presented %d cert(s); trusted set has %d)\n",
			self.client.ClientTag(),
			self.destination,
			self.contractMultiRouteWriterAlias.StreamId,
			err,
			len(peerCerts),
			len(expected),
		)
		return fmt.Errorf("sequence TLS cert verification failed: %w", err)
	}
	if !ok {
		self.log.Errorf(
			"[s]%s->%s s(%s) sequence TLS cert mismatch (peer presented %d cert(s); trusted set has %d)\n",
			self.client.ClientTag(),
			self.destination,
			self.contractMultiRouteWriterAlias.StreamId,
			len(peerCerts),
			len(expected),
		)
		return errors.New("sequence TLS cert verification failed")
	}
	self.session.MarkCertVerified()
	return nil
}

// Opens one destination-keyed writer. Verified stream contracts contribute
// shared route aliases without changing the logical or serialized path.
func (self *SendSequence) openContractMultiRouteWriter() MultiRouteWriter {
	self.updateContractMultiRouteWriterAlias()
	destination := DestinationId(self.destination)
	if self.contractMultiRouteWriter == nil || self.contractMultiRouteWriterDestination != destination {
		if self.contractMultiRouteWriter != nil {
			self.client.RouteManager().CloseMultiRouteWriter(self.contractMultiRouteWriter)
		}
		self.contractMultiRouteWriter = self.client.RouteManager().OpenMultiRouteWriter(destination)
		self.contractMultiRouteWriterDestination = destination

		// associate the destination with this sequence to receive acks
		self.sendBuffer.AssociateDestination(self, self.destination)
	}
	return self.contractMultiRouteWriter
}

// Replaces the live stream route alias when the head contract changes.
func (self *SendSequence) updateContractMultiRouteWriterAlias() {
	alias := TransferPath{}
	if self.sendContract != nil && self.sendContract.path.IsStream() {
		alias = StreamId(self.sendContract.path.StreamId)
	}
	if self.contractMultiRouteWriterAlias == alias {
		return
	}
	if self.removeContractMultiRouteWriterAlias != nil {
		self.removeContractMultiRouteWriterAlias()
		self.removeContractMultiRouteWriterAlias = nil
		self.contractMultiRouteWriterAlias = TransferPath{}
	}
	if alias != (TransferPath{}) {
		self.removeContractMultiRouteWriterAlias = self.client.RouteManager().AddWriterDestinationAlias(
			DestinationId(self.destination),
			alias,
		)
		self.contractMultiRouteWriterAlias = alias
	}
}

// Releases the selector and any stream alias owned by this sequence.
func (self *SendSequence) closeContractMultiRouteWriter() {
	if self.removeContractMultiRouteWriterAlias != nil {
		self.removeContractMultiRouteWriterAlias()
		self.removeContractMultiRouteWriterAlias = nil
		self.contractMultiRouteWriterAlias = TransferPath{}
	}
	if self.contractMultiRouteWriter != nil {
		self.client.RouteManager().CloseMultiRouteWriter(self.contractMultiRouteWriter)
		self.contractMultiRouteWriter = nil
		self.contractMultiRouteWriterDestination = TransferPath{}
	}
}

func (self *SendSequence) Close() {
	self.cancel()
	if self.packAdmission != nil {
		self.packAdmission.close()
	}

	func() {
		self.packMutex.Lock()
		defer self.packMutex.Unlock()
		close(self.packs)
	}()

	func() {
		self.ackMutex.Lock()
		defer self.ackMutex.Unlock()
		close(self.acks)
	}()

	// drain the channel
	func() {
		for {
			select {
			case sendPack, ok := <-self.packs:
				if !ok {
					return
				}
				err := errors.New("Send sequence closed.")
				sendPack.disposeUnsentGroup(err)
			default:
				return
			}
		}
	}()
}

func (self *SendSequence) Cancel() {
	self.cancel()
}

// Classifies a receiver-evidenced resend so it does not advance timeout backoff
// and can be measured independently from ordinary RTO recovery.
type sendRecoveryKind uint8

const (
	sendRecoveryNone sendRecoveryKind = iota
	sendRecoveryCarrierChange
	sendRecoverySelectiveGap
	sendRecoveryAckTailProbe
	sendRecoveryCumulativeProbe
	sendRecoveryContractMissing
	// the receiver told us it removed an item it had already acknowledged
	// (THROUGHPUTFIX §37.16). Not a gap: the gap recovery's burst is sized for
	// a few late packets and an eviction generation is bounded by what was in
	// flight on a dead route, so these ride the unbounded path.
	sendRecoveryEviction
)

type sendItem struct {
	transferItem

	contractId         *Id
	head               bool
	hasContractFrame   bool
	sendTime           time.Time
	resendTime         time.Time
	ackTimeout         time.Duration
	sendCount          int
	transferFrameBytes []byte
	acks               sendAckSet
	// selectiveAcked marks an item whose resend is paused by a selective ack
	// (see receiveAck). selectiveGapRecovered and ackTailProbeCount bound receiver-
	// paced data recovery per item. recoveryKind marks the scheduled attempt so it
	// does not inflate ordinary timeout backoff and remains observable.
	selectiveAcked        bool
	selectiveGapRecovered bool
	// deferralOutstanding is set while this item's own retransmit is waiting
	// out a deferral and cleared when that retransmit is finally written. It
	// sits in the existing bool run, so the item gains no bytes
	// (FLIGHTGATEFIX §23.2).
	deferralOutstanding  bool
	gapFollowupScheduled bool
	ackTailProbeCount    int
	recoveryKind         sendRecoveryKind
	promotedHead         bool
	// forceUnwrapped pins this item to plaintext on every (re)send, so the
	// outer wrap is skipped even if the per-peer cipher becomes available
	// between the initial send and a retransmit.
	forceUnwrapped                bool
	transportWriteObserved        bool
	unreliableCarrierObserved     bool
	reliableCarrierObserved       bool
	reliableRoute                 Route
	hybridReliableCarrierObserved bool
	unreliableFlightTracked       bool
	unreliableFlowReserve         bool
	// carrierChanged is set once a write of this item lands on a route other
	// than the one its previous write took. From then on an acknowledgement
	// of it proves nothing about either lane, since the copy the receiver
	// acknowledged may be any of them: a direct-lane item resent through the
	// relay while the direct flight was full is acknowledged by its direct
	// copy, and crediting that to the relay would prove every relay item
	// below it dropped. Such an item is excluded from lane attribution and
	// keeps merged's timer (FLIGHTGATEFIX §32.6).
	carrierChanged bool
	schedulingKey  sendSchedulingKey
	// carrierRoute is the route of the newest successful write, any carrier;
	// acknowledgement progress is reported per route from it (M6 watchdog).
	carrierRoute Route
	// laneAckedAtLastFiring is the highest sequence number this item's own
	// lane had acknowledged when this item last looked, which is its timer's
	// last firing or the promotion that moved that firing forward. The next
	// firing asks whether anything below it has moved since. §34.5 expected
	// the per-route slots to carry this and they cannot: "since X's last
	// firing" is per item and per position, and a slot holds one number for
	// the whole lane. It sits against the 8-byte fields at the end of the
	// struct, so the item grows by its own 8 bytes and no padding
	// (FLIGHTGATEFIX §34.3).
	laneAckedAtLastFiring uint64
	// timeoutDeferCount is how many RTOs of this item were deferred while the
	// cumulative ack kept advancing (§13.5), and timeoutDeferAckTime is the
	// cumulative ack the last deferral saw: the next one requires the ack to
	// have advanced since, so a hole nothing can acknowledge is deferred at
	// most once (FLIGHTGATEFIX §16).
	timeoutDeferCount   int
	timeoutDeferAckTime time.Time

	// messageType protocol.MessageType
}

// sendItem survives until the transfer acknowledgement, so it cannot be
// stack-allocated. A process-wide bounded pool captures the steady in-flight
// working set without multiplying retained objects by every window Client.
// 1024 items cover the measured steady flight and retain less than 256 KiB of
// metadata process-wide; bursts beyond the cap fall back to GC.
const sendItemPoolCapacity = 1024

var sendItemPool = make(chan *sendItem, sendItemPoolCapacity)

func takeSendItem() *sendItem {
	select {
	case item := <-sendItemPool:
		return item
	default:
		return &sendItem{}
	}
}

func clearSendItemPool() {
	func() {
		for {
			select {
			case <-sendItemPool:
			default:
				return
			}
		}
	}()
	func() {
		for {
			select {
			case <-sendAckSetOverflowPool:
			default:
				return
			}
		}
	}()
	for {
		select {
		case <-noAckSendSetOverflowPool:
		default:
			return
		}
	}
}

func (self *sendItem) messagePoolReturn() {
	MessagePoolReturn(self.transferFrameBytes)
	*self = sendItem{}
	select {
	case sendItemPool <- self:
	default:
		// Preserve a hard process-wide retention cap. An acknowledgement never
		// waits for reuse capacity.
	}
}

// the resend queue accounts items by their actual transfer frame size rather
// than the app message size (`messageByteCount`, which still drives contract
// accounting). For small messages the frame overhead dominates, and
// content-based accounting would let `ResendQueueMaxByteCount` admit tens of
// thousands of in-flight messages, far more than the path can ack inside the
// resend timeout under load.
// note the resend loop only mutates `transferFrameBytes` (set head) while the
// item is removed from the queue, so the add/remove accounting stays consistent.
func (self *sendItem) MessageByteCount() ByteCount {
	return ByteCount(len(self.transferFrameBytes))
}

// Send queues use the encoded frame length for both their per-sequence limit
// and their shared resend budget. transferItem.QueueByteCount cannot delegate
// to this type's MessageByteCount through the embedded receiver, so implement
// the retained-accounting method explicitly instead of silently charging the
// base transferItem's (unused) messageByteCount field.
func (self *sendItem) QueueByteCount() ByteCount {
	return self.MessageByteCount()
}

// a send event queue which is the union of:
// - resend times
// - ack timeouts
type resendQueue = transferQueue[*sendItem]

func newResendQueue(budget *TransferMemoryBudget, minByteCount ByteCount) *resendQueue {
	queue := newTransferQueue[*sendItem](func(a *sendItem, b *sendItem) int {
		if a.resendTime.Before(b.resendTime) {
			return -1
		} else if b.resendTime.Before(a.resendTime) {
			return 1
		} else {
			return 0
		}
	})
	queue.setBudget(budget, minByteCount)
	return queue
}

// How a full receive hold treats an arrival earlier than what it holds.
type ReceiveHoldPolicyKind int

const (
	// Keep the hold sequence-earliest by evicting the latest held item, and
	// acknowledge a held item only once it can no longer be evicted
	// (THROUGHPUTFIX §37.20). The default, and receiver-only: it needs its own
	// capacity, its delivery point, its held set and a frame size, and nothing
	// of its peer.
	ReceiveHoldCommittedPrefix ReceiveHoldPolicyKind = iota
	// Evict the latest held item and acknowledge on admission, which is every
	// tree before §37.17. Kept as an arm: an evicted item was acknowledged, so
	// its removal is a withdrawal the sender learns of only when an
	// acknowledgement-tail probe reaches it, serialised, twice, and then a
	// minute (§37.19).
	ReceiveHoldEvict
	// Refuse the arrival and never evict, which is §37.17's guard one as first
	// landed. Truthful, and it starves at high overrun because the only way a
	// full hold empties is the head draining its contiguous prefix, and a
	// middle gap that would extend that prefix is refused: measured at a
	// 6.4 times overrun, none of four runs completed against evicting's two,
	// with 53,509 loss events against 25,227 (§37.20).
	ReceiveHoldRefuse
)

type ReceiveBufferSettings struct {
	GapTimeout  time.Duration
	IdleTimeout time.Duration

	SequenceBufferSize int
	// H1SequenceBufferSize optionally gives reliable H1 arrivals more burst
	// handoff slots than other carriers. Nonpositive values inherit
	// SequenceBufferSize. The channel is allocated at the larger count, while
	// Pack enforces the carrier-specific limit so H3 cannot consume H1's memory
	// spend. Encoded bytes remain independently bounded below.
	H1SequenceBufferSize int
	// H1SequenceBufferAdaptiveMaxSize optionally lets a continuously saturated
	// reliable H1 flow deepen beyond H1SequenceBufferSize. The channel reserves
	// pointer slots up to this hard maximum, while Pack ownership is admitted
	// incrementally and remains subject to the unchanged byte limits and shared
	// PackQueueBudget. Nonpositive or <= H1SequenceBufferSize disables growth.
	H1SequenceBufferAdaptiveMaxSize int
	// H1SequenceBufferAdaptiveStepSize is the number of slots granted per
	// qualifying saturation epoch. Nonpositive disables growth.
	H1SequenceBufferAdaptiveStepSize int
	// H1SequenceBufferAdaptiveSaturationThreshold is the number of distinct
	// full-queue Pack calls required before one step is granted.
	H1SequenceBufferAdaptiveSaturationThreshold int
	// H1SequenceBufferAdaptiveSaturationWindow bounds the elapsed time between
	// qualifying full-queue calls. A later episode starts a new streak.
	// Nonpositive disables growth.
	H1SequenceBufferAdaptiveSaturationWindow time.Duration
	// H1SequenceBufferAdaptiveMaxByteCount is the maximum logical encoded-byte
	// allowance earned alongside adaptive H1 count depth. The shared exact
	// retained-allocation budget remains the hard device-wide memory bound.
	// Nonpositive or <= H1SequenceBufferByteCount keeps the byte limit fixed.
	H1SequenceBufferAdaptiveMaxByteCount ByteCount
	// H1SequenceBufferAdaptiveStepByteCount is the logical byte allowance
	// granted with each qualifying count step. Nonpositive keeps bytes fixed.
	H1SequenceBufferAdaptiveStepByteCount ByteCount
	// SequenceBufferByteCount bounds encoded TransferFrame bytes waiting in
	// one ReceiveSequence handoff channel. Nonpositive values retain legacy
	// count-only behavior for explicitly constructed settings.
	SequenceBufferByteCount ByteCount
	// H1SequenceBufferByteCount optionally gives reliable H1 arrivals a larger
	// encoded-byte burst allowance. Nonpositive values inherit
	// SequenceBufferByteCount. Count and bytes are selected from the same
	// carrier, so an H3 arrival cannot consume this H1-only allowance.
	H1SequenceBufferByteCount ByteCount
	// PackQueueBudget, when set, is an exact byte budget shared across every
	// ReceiveSequence handoff channel using these settings. Per-sequence count
	// and byte limits still preserve local fairness; this aggregate prevents a
	// large flow fan-out from multiplying those independent burst allowances.
	PackQueueBudget *TransferMemoryBudget
	// PackQueueRetainedByteAccounting charges PackQueueBudget for pooled outer
	// and decoded message roots plus the decoded-owner envelope. The per-flow
	// SequenceBufferByteCount limits remain encoded logical bytes. This is an
	// opt-in diagnostic; default mobile, desktop, and server paths avoid the
	// extra scan.
	PackQueueRetainedByteAccounting bool
	// H1PackHandoffTimeout applies bounded reader backpressure only after an
	// unclassified legacy H1 ReceiveSequence handoff is full. Exact production
	// lanes use ReliablePackHandoffTimeout below. This field remains for custom
	// readers that report only TransportType.
	// A positive value is a total wait bound, not a per-retry delay. A negative
	// value waits until capacity or sequence/client cancellation, extending an
	// already-reliable H1 stream's backpressure without enlarging the channel.
	H1PackHandoffTimeout time.Duration
	// ReliablePackHandoffTimeout applies to an exact route explicitly published
	// as reliable (H1, H3/DNS QUIC stream, SCTP, or a framed server exchange).
	// A negative value waits for capacity or cancellation without enlarging the
	// queue. Zero preserves nonblocking behavior for explicitly customized
	// settings; production defaults to a cancellation-bounded wait.
	ReliablePackHandoffTimeout time.Duration
	// H1AckHandoffTimeout applies the same reliable-carrier backpressure rule
	// when an inbound ACK burst momentarily fills its SendSequence queue. ACK
	// objects are compact values; this wait does not retain the carrier frame.
	// H3 and unknown carriers always remain nonblocking.
	H1AckHandoffTimeout time.Duration
	// AckBufferSize int

	AckCompressTimeout time.Duration

	MinMessageByteCount ByteCount

	// min number of resends before checking abuse
	// ResendAbuseThreshold int
	// max legit fraction of sends that are resends
	// ResendAbuseMultiple float64

	MaxPeerAuditDuration time.Duration

	WriteTimeout time.Duration

	ReceiveQueueMaxByteCount ByteCount
	// ReceiveQueueMinByteCount is the guaranteed per-sequence floor when
	// `ReceiveQueueBudget` is set (see `ResendQueueMinByteCount`)
	ReceiveQueueMinByteCount ByteCount
	// ReceiveQueueBudget, when set, is a byte budget shared across sequences
	// (see `ResendQueueBudget`)
	ReceiveQueueBudget *TransferMemoryBudget
	// WindowSizing derives the hold from the surface, the same one switch the
	// send side takes (THROUGHPUTFIX §37.4). Constant is today's hold exactly.
	WindowSizing WindowSizingPolicyKind
	// AdvertiseReceiveWindow puts what this receiver can still hold out of
	// order on the acknowledgement, so the sender may clamp its window to it
	// (THROUGHPUTFIX §37.3). Off by default and held out of the landing until
	// the multi-route failover cell of §37.15 reports: the one reachable case
	// for it is a route dying with frames striped across two routes, which no
	// cell has yet run, and the shipping window is under the hold by an
	// accident of ordering rather than by design.
	AdvertiseReceiveWindow bool
	// ReceiveHoldPolicy decides what a full hold does with an arrival that is
	// earlier than what it holds (THROUGHPUTFIX §37.20). One field with three
	// arms, because that is what made the two preceding policies measurable
	// against each other in one binary.
	ReceiveHoldPolicy ReceiveHoldPolicyKind
	// EvictionNotice puts the sequence numbers of items this receiver removed
	// from its hold after acknowledging them onto the next acknowledgement, so
	// the sender resends them instead of holding a sixty second lease on bytes
	// that are gone (THROUGHPUTFIX §37.16). On by default: silent reneging is a
	// correctness defect that exists without any window change, and a receiver
	// whose hold is smaller than its peer's window can reach it today. Off is
	// the pre-fix behaviour and exists so a cell can measure the difference in
	// one binary.
	EvictionNotice bool
	// ReceiveQueueRetainedByteAccounting charges the shared queue budget for
	// carrier/frame backing classes plus the decoded owner rather than payload
	// bytes alone. Per-sequence ReceiveQueueMaxByteCount remains a logical
	// payload window. Constrained mobile profiles enable this; server/default
	// paths retain their historical accounting and avoid the extra scan.
	ReceiveQueueRetainedByteAccounting bool

	// whether to allow nacks without a contract_id
	AllowLegacyNack bool

	MaxOpenReceiveContract int

	ProtocolVersion int

	// Nil test barriers expose exact worker and close-join boundaries without
	// changing production behavior.
	beforeCreateReceiveSequenceForTest func(receiveSequenceId)
	beforeRunReceiveSequenceForTest    func(receiveSequenceId)
	beforeCloseWaitForTest             func(receiveSequenceId)
	afterRunReceiveSequenceForTest     func(receiveSequenceId)
	beforeAckCompressWaitForTest       func(receiveSequenceId)
	afterAckWriteForTest               func(receiveSequenceId)
	h1SaturationNowForTest             func() time.Time
	beforeAckWorkerStopForTest         func(receiveSequenceId)
	afterAckWriterOpenForTest          func(receiveSequenceId, MultiRouteWriter)
	afterAckWritesCanceledForTest      func(receiveSequenceId)
}

func (self *ReceiveBufferSettings) packHandoffTimeout(
	transportType TransportType,
	reliabilities ...CarrierReliability,
) time.Duration {
	if self == nil {
		return 0
	}
	if 0 < len(reliabilities) {
		switch reliabilities[0] {
		case CarrierReliabilityReliable:
			return self.ReliablePackHandoffTimeout
		case CarrierReliabilityUnreliable:
			return 0
		}
	}
	if transportType == TransportTypeH1 {
		return self.H1PackHandoffTimeout
	}
	return 0
}

func (self *ReceiveBufferSettings) ackHandoffTimeout(transportType TransportType) time.Duration {
	if self != nil && transportType == TransportTypeH1 {
		return self.H1AckHandoffTimeout
	}
	return 0
}

type receiveSequenceId struct {
	Source      TransferPath
	SequenceId  Id
	LogicalLane uint32
	// EncryptionRole separates the inbound streams that map to our server
	// session (normal peer data — the default) from those that map to our
	// client session (the peer's EncryptedControl carrier + server replies).
	// SequenceId alone is already unique; the role makes the owning session
	// explicit and keys the per-role head tracking.
	EncryptionRole sequenceTlsRole
	// EncryptionCompanion separates the inbound streams owned by the companion
	// session from those owned by the regular session of the same role, so a
	// peer running both modes maps each stream to the right per-peer session.
	EncryptionCompanion bool
}

// receiveSequenceHeadKey identifies the head (newest) receive sequence for a
// given (source, companion, role, lane). Supersession — drop-older /
// upgrade-newer by SequenceId — happens within a single key: the peer's
// client and server streams, its companion and regular streams, and its
// sequence lanes (force-stream / companion-contract / logical lane, Pack
// fields 10/11/12)
// reform independently, so they must not supersede each other. Packs from
// peers that predate the lane fields decode to the false/false lane, which
// is exactly the legacy merged behavior.
type receiveSequenceHeadKey struct {
	Source              TransferPath
	EncryptionRole      sequenceTlsRole
	EncryptionCompanion bool
	ForceStream         bool
	CompanionContract   bool
	LogicalLane         uint32
}

// Converts the complete receive-buffer lane discriminator into callback and
// reply metadata. Transport-local StreamId is removed at this boundary.
func (self receiveSequenceHeadKey) transferKey() TransferKey {
	return TransferKey{
		ForceStream:         self.ForceStream,
		CompanionContract:   self.CompanionContract,
		EncryptionRole:      self.EncryptionRole.toProtobuf(),
		EncryptionCompanion: self.EncryptionCompanion,
		LogicalLane:         self.LogicalLane,
	}
}

// rejectedReceiveSequenceCapacity bounds permanent receive-sequence
// tombstones. One bad contract is deterministic for that sequence id; without
// a tombstone, every sender retransmit recreated the sequence, reverified the
// same contract, and emitted the same error chain at the cold resend cadence.
const rejectedReceiveSequenceCapacity = 1024

type ReceiveBuffer struct {
	ctx    context.Context
	client *Client
	log    Logger

	receiveBufferSettings *ReceiveBufferSettings

	mutex  sync.Mutex
	closed bool
	// the head receive sequences
	// source id -> receive sequence
	receiveSequences       map[receiveSequenceId]*ReceiveSequence
	headReceiveSequenceIds map[receiveSequenceHeadKey]receiveSequenceId
	// rejectedReceiveSequenceIds stores the newest permanently rejected
	// sequence per source/session key. The FIFO bounds memory across a
	// process-long succession of hostile or stale peers.
	rejectedReceiveSequenceIds   map[receiveSequenceHeadKey]Id
	rejectedReceiveSequenceOrder []receiveSequenceHeadKey
	// activeReceiveSequences retains workers through their final Pack-channel
	// drain, which occurs after their public head/index entries are removed.
	activeReceiveSequences map[*ReceiveSequence]bool
	// Nonzero receive lanes share one fixed reorder pool when the caller did
	// not already install a device-wide ReceiveQueueBudget.
	logicalLaneReceiveBudget *TransferMemoryBudget

	beforeCreateReceiveSequenceForTest func(receiveSequenceId)
	beforeRunReceiveSequenceForTest    func(receiveSequenceId)
	beforeCloseWaitForTest             func(receiveSequenceId)
	afterRunReceiveSequenceForTest     func(receiveSequenceId)
}

func NewReceiveBuffer(ctx context.Context,
	client *Client,
	receiveBufferSettings *ReceiveBufferSettings) *ReceiveBuffer {
	return &ReceiveBuffer{
		ctx:                    ctx,
		client:                 client,
		log:                    client.log,
		receiveBufferSettings:  receiveBufferSettings,
		receiveSequences:       map[receiveSequenceId]*ReceiveSequence{},
		headReceiveSequenceIds: map[receiveSequenceHeadKey]receiveSequenceId{},
		activeReceiveSequences: map[*ReceiveSequence]bool{},
		beforeCreateReceiveSequenceForTest: receiveBufferSettings.
			beforeCreateReceiveSequenceForTest,
		beforeRunReceiveSequenceForTest: receiveBufferSettings.
			beforeRunReceiveSequenceForTest,
		beforeCloseWaitForTest: receiveBufferSettings.beforeCloseWaitForTest,
		afterRunReceiveSequenceForTest: receiveBufferSettings.
			afterRunReceiveSequenceForTest,
	}
}

// removeRejectedReceiveSequenceWithLock removes one tombstone and its bounded
// FIFO entry. Caller holds mutex; rejection is rare, so the bounded linear
// removal avoids another index map.
func (self *ReceiveBuffer) removeRejectedReceiveSequenceWithLock(
	headKey receiveSequenceHeadKey,
) {
	if _, ok := self.rejectedReceiveSequenceIds[headKey]; !ok {
		return
	}
	delete(self.rejectedReceiveSequenceIds, headKey)
	for i, key := range self.rejectedReceiveSequenceOrder {
		if key != headKey {
			continue
		}
		copy(
			self.rejectedReceiveSequenceOrder[i:],
			self.rejectedReceiveSequenceOrder[i+1:],
		)
		lastIndex := len(self.rejectedReceiveSequenceOrder) - 1
		self.rejectedReceiveSequenceOrder[lastIndex] = receiveSequenceHeadKey{}
		self.rejectedReceiveSequenceOrder =
			self.rejectedReceiveSequenceOrder[:lastIndex]
		return
	}
}

// rejectReceiveSequenceWithLock records the newest deterministic contract
// failure for one source/session key. Caller holds mutex.
func (self *ReceiveBuffer) rejectReceiveSequenceWithLock(
	headKey receiveSequenceHeadKey,
	sequenceId Id,
) {
	// Contract rejection is exceptional. Allocating the full tombstone FIFO
	// for every Client made the normal provider path pay roughly 48 KiB per
	// receive buffer even when no peer had ever presented a bad contract.
	// Keep the exact bounded capacity, but materialize it only on first use.
	if self.rejectedReceiveSequenceIds == nil {
		self.rejectedReceiveSequenceIds = map[receiveSequenceHeadKey]Id{}
		self.rejectedReceiveSequenceOrder = make(
			[]receiveSequenceHeadKey,
			0,
			rejectedReceiveSequenceCapacity,
		)
	}
	if rejectedSequenceId, ok := self.rejectedReceiveSequenceIds[headKey]; ok {
		if rejectedSequenceId.LessThan(sequenceId) {
			self.rejectedReceiveSequenceIds[headKey] = sequenceId
		}
		return
	}
	if rejectedReceiveSequenceCapacity <= len(self.rejectedReceiveSequenceOrder) {
		oldestHeadKey := self.rejectedReceiveSequenceOrder[0]
		delete(self.rejectedReceiveSequenceIds, oldestHeadKey)
		copy(
			self.rejectedReceiveSequenceOrder,
			self.rejectedReceiveSequenceOrder[1:],
		)
		lastIndex := len(self.rejectedReceiveSequenceOrder) - 1
		self.rejectedReceiveSequenceOrder[lastIndex] = receiveSequenceHeadKey{}
		self.rejectedReceiveSequenceOrder =
			self.rejectedReceiveSequenceOrder[:lastIndex]
	}
	self.rejectedReceiveSequenceIds[headKey] = sequenceId
	self.rejectedReceiveSequenceOrder =
		append(self.rejectedReceiveSequenceOrder, headKey)
}

// rejectReceiveSequenceRetransmitWithLock reports whether an incoming sequence
// is the rejected id (or an older one it superseded). A genuinely newer
// sequence clears the tombstone and can present a fresh contract. Caller holds
// mutex.
func (self *ReceiveBuffer) rejectReceiveSequenceRetransmitWithLock(
	headKey receiveSequenceHeadKey,
	sequenceId Id,
) bool {
	rejectedSequenceId, ok := self.rejectedReceiveSequenceIds[headKey]
	if !ok {
		return false
	}
	if sequenceId == rejectedSequenceId ||
		sequenceId.LessThan(rejectedSequenceId) {
		return true
	}
	self.removeRejectedReceiveSequenceWithLock(headKey)
	return false
}

// removeReceiveSequenceWithLock removes one completed worker without disturbing
// a newer worker that already became the head. Caller holds mutex. Run has
// returned (or WaitForExit has completed), so rejectRetransmits is stable.
func (self *ReceiveBuffer) removeReceiveSequenceWithLock(
	receiveSequenceId receiveSequenceId,
	headKey receiveSequenceHeadKey,
	receiveSequence *ReceiveSequence,
) {
	if receiveSequence != self.receiveSequences[receiveSequenceId] {
		return
	}
	if receiveSequence.rejectRetransmits {
		self.rejectReceiveSequenceWithLock(
			headKey,
			receiveSequenceId.SequenceId,
		)
	}
	delete(self.receiveSequences, receiveSequenceId)
	if self.headReceiveSequenceIds[headKey] == receiveSequenceId {
		delete(self.headReceiveSequenceIds, headKey)
	}
}

func (self *ReceiveBuffer) Pack(receivePack *ReceivePack, timeout time.Duration) (bool, error) {
	logicalLane := uint32(0)
	if receivePack.Pack != nil {
		logicalLane = receivePack.Pack.LogicalLane
		if maxLogicalDataLaneCount < logicalLane {
			return false, fmt.Errorf("logical lane %d exceeds maximum %d", logicalLane, maxLogicalDataLaneCount)
		}
	}
	receiveSequenceId := receiveSequenceId{
		Source:              receivePack.Source,
		SequenceId:          receivePack.SequenceId,
		EncryptionRole:      receivePack.EncryptionRole,
		EncryptionCompanion: receivePack.EncryptionCompanion,
		LogicalLane:         logicalLane,
	}
	// Head/supersession is tracked per (source, companion, role, lane): the
	// peer's client and server streams, its companion and regular streams,
	// and its sequence lanes reform independently and must not supersede each
	// other. A pack without lane fields (a pre-lane peer, or a caller that
	// carries no Pack) maps to the false/false legacy lane.
	headKey := receiveSequenceHeadKey{
		Source:              receiveSequenceId.Source,
		EncryptionRole:      receiveSequenceId.EncryptionRole,
		EncryptionCompanion: receiveSequenceId.EncryptionCompanion,
	}
	if receivePack.Pack != nil {
		headKey.ForceStream = receivePack.Pack.ForceStream
		headKey.CompanionContract = receivePack.Pack.CompanionContract
		headKey.LogicalLane = logicalLane
	}

	initReceiveSequence := func(skip *ReceiveSequence) *ReceiveSequence {
		for {
			if self.beforeCreateReceiveSequenceForTest != nil {
				self.beforeCreateReceiveSequenceForTest(receiveSequenceId)
			}
			self.mutex.Lock()

			if self.closed {
				self.mutex.Unlock()
				receivePack.messagePoolReturn()
				return nil
			}
			if self.rejectReceiveSequenceRetransmitWithLock(
				headKey,
				receiveSequenceId.SequenceId,
			) {
				self.mutex.Unlock()
				receivePack.messagePoolReturn()
				return nil
			}

			receiveSequence, ok := self.receiveSequences[receiveSequenceId]
			if ok {
				if skip == nil || skip != receiveSequence {
					self.mutex.Unlock()
					return receiveSequence
				}
				if headReceiveSequenceId := self.headReceiveSequenceIds[headKey]; headReceiveSequenceId != receiveSequenceId {
					self.mutex.Unlock()
					panic(fmt.Errorf("[r]incorrect head sequence %s != %s\n", headReceiveSequenceId.SequenceId, receivePack.SequenceId))
				}

				// The shared receive pump passes a zero timeout. It must not wait
				// for a closing worker: cancel, drop, and let Transfer resend after
				// lifecycle cleanup removes the old generation.
				receiveSequence.Cancel()
				self.mutex.Unlock()
				if timeout == 0 {
					self.client.recordReceivePackHandoffDrop(receivePack.MessageByteCount)
					receivePack.messagePoolReturn()
					return nil
				}
				receiveSequence.WaitForExit()
				self.mutex.Lock()
				self.removeReceiveSequenceWithLock(
					receiveSequenceId,
					headKey,
					receiveSequence,
				)
				self.mutex.Unlock()
				continue
			}

			if headReceiveSequenceId, headOk := self.headReceiveSequenceIds[headKey]; headOk {
				if receivePack.SequenceId.LessThan(headReceiveSequenceId.SequenceId) {
					// drop older sequences for source
					// this case happens when a client closes a sequence, then opens a new one,
					// before messages from the first are received.
					// A PERSISTENT stream of these drops for one source is the
					// signature of a sender-side sequence-key fork: two live send
					// sequences whose frames are indistinguishable on the wire
					// (same source, role, companion) — see `carrierForceStream`.
					if self.log.V(1).Enabled() {
						self.log.Infof("[r]drop older sequence %s < %s (%s %s c=%t)\n",
							receivePack.SequenceId, headReceiveSequenceId.SequenceId,
							receivePack.Source, receivePack.EncryptionRole, receivePack.EncryptionCompanion)
					}
					self.mutex.Unlock()
					receivePack.messagePoolReturn()
					return nil
				}
				if headReceiveSequenceId.SequenceId == receivePack.SequenceId {
					self.mutex.Unlock()
					panic(fmt.Errorf("[r]upgrade older sequence %s = %s\n", headReceiveSequenceId.SequenceId, receivePack.SequenceId))
				}
				if self.log.V(1).Enabled() {
					self.log.Infof("[r]upgrade older sequence %s < %s (%s %s c=%t)\n",
						headReceiveSequenceId.SequenceId, receivePack.SequenceId,
						receivePack.Source, receivePack.EncryptionRole, receivePack.EncryptionCompanion)
				}
				headReceiveSequence := self.receiveSequences[headReceiveSequenceId]
				if headReceiveSequence == nil {
					// Heal a stale index defensively. Normal lifecycle cleanup
					// removes both entries atomically under this lock.
					delete(self.headReceiveSequenceIds, headKey)
					self.mutex.Unlock()
					continue
				}
				headReceiveSequence.Cancel()
				self.mutex.Unlock()
				if timeout == 0 {
					// Do not install the replacement until the old worker has
					// finished. Dropping this Pack is safe: it was not admitted,
					// and its sender retains it until a Transfer ACK arrives.
					self.client.recordReceivePackHandoffDrop(receivePack.MessageByteCount)
					receivePack.messagePoolReturn()
					return nil
				}

				// Sequence shutdown must finish before replacement for this
				// source. Wait outside the buffer-wide map lock so other peers
				// can keep finding and creating their sequences.
				headReceiveSequence.WaitForExit()
				self.mutex.Lock()
				self.removeReceiveSequenceWithLock(
					headReceiveSequenceId,
					headKey,
					headReceiveSequence,
				)
				self.mutex.Unlock()
				continue
			}

			if self.log.V(2).Enabled() {
				self.log.Infof("[r]new sequence %s\n", receivePack.SequenceId)
			}

			logicalLaneReceiveBudget := self.logicalLaneReceiveBudget
			if logicalLane != 0 &&
				self.receiveBufferSettings.ReceiveQueueBudget == nil &&
				logicalLaneReceiveBudget == nil {
				// As on send, preserve the disabled path's allocation profile and
				// create exactly one fixed byte pool on first negotiated data lane.
				logicalLaneReceiveBudget = NewTransferMemoryBudget(
					self.receiveBufferSettings.ReceiveQueueMaxByteCount,
				)
				self.logicalLaneReceiveBudget = logicalLaneReceiveBudget
			}
			receiveSequence = newReceiveSequenceWithLogicalLaneBudget(
				self.ctx,
				self.client,
				headKey.Source,
				receivePack.SequenceId,
				headKey.transferKey(),
				self.receiveBufferSettings,
				logicalLaneReceiveBudget,
			)
			self.receiveSequences[receiveSequenceId] = receiveSequence
			self.headReceiveSequenceIds[headKey] = receiveSequenceId
			self.activeReceiveSequences[receiveSequence] = true
			go self.runReceiveSequence(
				receiveSequenceId,
				headKey,
				receiveSequence,
			)
			self.mutex.Unlock()
			return receiveSequence
		}
	}

	var receiveSequence *ReceiveSequence
	var success bool
	var err error
	for i := 0; i < 2; i += 1 {
		select {
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		default:
		}
		receiveSequence = initReceiveSequence(receiveSequence)
		if receiveSequence == nil {
			// drop
			return true, nil
		}
		if success, err = receiveSequence.Pack(receivePack, timeout); err == nil {
			return success, nil
		}
		// sequence closed
	}
	return success, err
}

// runReceiveSequence owns one admitted worker through index removal and the
// final Pack-channel drain. Completion is published under the admission lock,
// so CloseAndWait cannot miss a worker between removal and cleanup.
func (self *ReceiveBuffer) runReceiveSequence(
	receiveSequenceId receiveSequenceId,
	headKey receiveSequenceHeadKey,
	receiveSequence *ReceiveSequence,
) {
	defer func() {
		self.mutex.Lock()
		delete(self.activeReceiveSequences, receiveSequence)
		close(receiveSequence.done)
		self.mutex.Unlock()
	}()
	if self.beforeRunReceiveSequenceForTest != nil {
		self.beforeRunReceiveSequenceForTest(receiveSequenceId)
	}
	HandleError(func() {
		defer func() {
			if self.afterRunReceiveSequenceForTest != nil {
				self.afterRunReceiveSequenceForTest(receiveSequenceId)
			}
		}()
		defer receiveSequence.Close()
		defer func() {
			self.mutex.Lock()
			self.removeReceiveSequenceWithLock(
				receiveSequenceId,
				headKey,
				receiveSequence,
			)
			self.mutex.Unlock()
		}()
		receiveSequence.Run()
	})
}

func (self *ReceiveBuffer) ReceiveQueueSizeAndMessageTypes(source TransferPath, sequenceId Id) (int, ByteCount, []protocol.MessageType) {
	self.mutex.Lock()
	sequences := make([]*ReceiveSequence, 0, 1)
	for id, sequence := range self.receiveSequences {
		if id.Source == source && id.SequenceId == sequenceId {
			sequences = append(sequences, sequence)
		}
	}
	self.mutex.Unlock()

	var count int
	var byteSize ByteCount
	var messageTypes []protocol.MessageType
	for _, sequence := range sequences {
		sequenceCount, sequenceByteSize, sequenceMessageTypes :=
			sequence.ReceiveQueueSizeAndMessageTypes()
		count += sequenceCount
		byteSize += sequenceByteSize
		messageTypes = append(messageTypes, sequenceMessageTypes...)
	}
	return count, byteSize, messageTypes
}

// Cancels and joins every inbound Transfer sequence authenticated as one
// source. This prevents already-queued Packs from recreating provider work
// after the source gate has accepted a terminal verdict.
func (self *ReceiveBuffer) cancelSourceAndWait(sourceId Id) {
	self.mutex.Lock()
	sequences := map[*ReceiveSequence]bool{}
	for id, sequence := range self.receiveSequences {
		if id.Source.SourceId == sourceId {
			sequences[sequence] = true
		}
	}
	self.mutex.Unlock()

	for sequence := range sequences {
		sequence.Cancel()
	}
	for sequence := range sequences {
		<-sequence.done
	}
}

func (self *ReceiveBuffer) Close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.closed = true
	// cancel all open sequences
	// the control of the sequence will close it
	for _, receiveSequence := range self.receiveSequences {
		receiveSequence.Cancel()
	}
}

func (self *ReceiveBuffer) Cancel() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.closed = true
	// cancel all open sequences
	for _, receiveSequence := range self.receiveSequences {
		receiveSequence.Cancel()
	}
}

// closeAndWait closes receive-sequence admission and joins every admitted
// worker after its Run cleanup and queued ReceivePack drain are complete.
func (self *ReceiveBuffer) closeAndWait(ctx context.Context) error {
	self.Close()

	self.mutex.Lock()
	sequences := make([]*ReceiveSequence, 0, len(self.activeReceiveSequences))
	for receiveSequence := range self.activeReceiveSequences {
		sequences = append(sequences, receiveSequence)
	}
	self.mutex.Unlock()

	for _, receiveSequence := range sequences {
		if self.beforeCloseWaitForTest != nil {
			self.beforeCloseWaitForTest(receiveSequence.id())
		}
		if err := waitForLifecycleDone(ctx, receiveSequence.done, "receive sequence"); err != nil {
			return err
		}
	}
	return nil
}

func (self *ReceiveBuffer) Flush() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// cancel all open sequences
	for _, receiveSequence := range self.receiveSequences {
		// if !receiveSequenceId.Source.IsControlSource() {
		receiveSequence.Cancel()
		// }
	}
}

// The most eviction notices one acknowledgement will carry. An eviction
// generation after a route death is bounded by what was in flight on the dead
// route, so at a 16 MiB window and 4 KiB items it is a few thousand; anything
// past this cap keeps the sixty second lease and is counted, because the one
// thing this must not do is make the harm invisible again.
const evictionNoticeMaxCount = 4096

type ReceiveSequence struct {
	ctx    context.Context
	cancel context.CancelFunc
	// done closes after the owning ReceiveBuffer has removed the worker and
	// drained every queued ReceivePack. exit retains its older Run-only meaning.
	done chan struct{}

	client *Client
	log    Logger

	source     TransferPath
	sequenceId Id
	// Sequence numbers the hold removed after acknowledging them, drained onto
	// the next acknowledgement. Written by the pack worker and read by the ack
	// writer, so it takes its own leaf lock.
	evictedMutex           sync.Mutex
	evictedSequenceNumbers []uint64
	// the largest message the hold has taken, the conservative estimate of
	// what a missing item below the boundary would cost
	maxHeldByteCount ByteCount
	// reused by commitHeldPrefix so walking the hold in order allocates
	// nothing per arrival
	heldScratch []*receiveItem
	// immutable receiver-visible lane metadata copied into every Peer callback.
	transferKey TransferKey
	// encryptionRole is the local per-peer session role that owns this
	// inbound stream (complement of the sender's role): server for normal
	// peer data (the default), client for the peer's EncryptedControl
	// carrier + server replies.
	encryptionRole sequenceTlsRole
	// encryptionCompanion is the per-peer session identity companion that owns
	// this inbound stream (not complemented); with encryptionRole it selects
	// which session the sequence holds.
	encryptionCompanion bool

	receiveBufferSettings *ReceiveBufferSettings

	openReceiveContracts      map[Id]*sequenceContract
	receiveContract           *sequenceContract
	contractWriterAlias       TransferPath
	removeContractWriterAlias func()

	packMutex sync.Mutex
	packs     chan *ReceivePack
	// H1 may use a larger reliable-carrier burst allowance than H3/unknown
	// without widening every mobile sequence queue. Producers are serialized by
	// packMutex; the worker decrements the atomic count as soon as it dequeues.
	packQueueCount                  atomic.Int64
	packQueueBaseLimit              int64
	packQueueH1Limit                int64
	packQueueH1AdaptiveMaxLimit     int64
	packQueueH1AdaptiveStep         int64
	packQueueH1SaturationThreshold  int
	packQueueH1SaturationWindow     time.Duration
	packQueueH1AdaptiveMaxByteLimit ByteCount
	packQueueH1AdaptiveByteStep     ByteCount
	packQueueH1SaturationStreak     int
	packQueueH1LastSaturationTime   time.Time
	packQueueH1Deepened             bool
	// Producers are serialized by packMutex, while the worker releases bytes
	// after a channel receive. The atomic keeps this byte budget independent of
	// the zero-wait channel admission operation.
	packQueueByteCount     atomic.Int64
	packQueueBaseByteLimit ByteCount
	packQueueH1ByteLimit   ByteCount
	// A coalesced edge wakes a bounded Pack waiter when the sequence worker
	// releases count/byte ownership. Producers are serialized by packMutex.
	packQueueSpace chan struct{}
	// packTimer is serialized by packMutex and reused by finite-timeout Pack
	// calls. An unbuffered receive sequence can briefly miss its consumer on
	// every packet; allocating time.After for each miss creates avoidable GC
	// pressure even when the channel becomes writable immediately afterward.
	packTimer *time.Timer

	receiveQueue       *receiveQueue
	nextSequenceNumber uint64

	idleCondition *IdleCondition

	peerAudit *SequencePeerAudit

	ackWindow *sequenceAckWindow

	exit chan struct{}

	// session is the per-peer TLS session that decrypts this inbound stream,
	// of role `encryptionRole` (the complement of the sender's role).
	// Acquired from the `EncryptionSessionManager` at construction without
	// starting a handshake — a ReceiveSequence follows the peer's handshake,
	// it never initiates one. Holding it keeps the session (and its cipher)
	// alive for the stream's lifetime; released when the sequence terminates.
	// Nil when encryption is disabled or this is control-plane traffic.
	session *peerEncryptionSession

	// rejectRetransmits is set when this sequence presents a malformed,
	// unverifiable, or otherwise unusable contract. The owning ReceiveBuffer
	// reads it after Run returns and tombstones the deterministic failure.
	rejectRetransmits bool

	// deliverItems/deliverFrames buffer consecutive in-order head items so
	// their app frames dispatch in ONE receive callback per drain burst
	// instead of one per pack (see receiveHead / flushDeliver). Batch depth
	// mirrors the packs channel occupancy: a lone item flushes on the very
	// next loop pass (no added latency), a saturated stream flushes at
	// receiveDeliverBatchMaxFrames. Items are retained here until flush,
	// which sends their acks and returns their pool buffers.
	deliverItems  []*receiveItem
	deliverFrames []*protocol.Frame
	deliverPeer   Peer
}

// id reconstructs the immutable receive-buffer lookup identity used by test
// lifecycle barriers and close diagnostics.
func (self *ReceiveSequence) id() receiveSequenceId {
	return receiveSequenceId{
		Source:              self.source,
		SequenceId:          self.sequenceId,
		EncryptionRole:      self.encryptionRole,
		EncryptionCompanion: self.encryptionCompanion,
		LogicalLane:         self.transferKey.LogicalLane,
	}
}

// receiveDeliverBatchMaxFrames bounds the frames buffered for one combined
// receive callback (memory and worst-case delivery latency under saturation).
const receiveDeliverBatchMaxFrames = 64

func NewReceiveSequence(
	ctx context.Context,
	client *Client,
	source TransferPath,
	sequenceId Id,
	encryptionRole sequenceTlsRole,
	encryptionCompanion bool,
	receiveBufferSettings *ReceiveBufferSettings) *ReceiveSequence {
	return newReceiveSequence(
		ctx,
		client,
		source.LocalMask(),
		sequenceId,
		TransferKey{
			EncryptionRole:      encryptionRole.toProtobuf(),
			EncryptionCompanion: encryptionCompanion,
		},
		receiveBufferSettings,
	)
}

// Constructs a worker from the exact ReceiveBuffer head discriminator so its
// callback metadata cannot drift from the lane used for supersession.
func newReceiveSequence(
	ctx context.Context,
	client *Client,
	source TransferPath,
	sequenceId Id,
	transferKey TransferKey,
	receiveBufferSettings *ReceiveBufferSettings,
) *ReceiveSequence {
	return newReceiveSequenceWithLogicalLaneBudget(
		ctx,
		client,
		source,
		sequenceId,
		transferKey,
		receiveBufferSettings,
		nil,
	)
}

func newReceiveSequenceWithLogicalLaneBudget(
	ctx context.Context,
	client *Client,
	source TransferPath,
	sequenceId Id,
	transferKey TransferKey,
	receiveBufferSettings *ReceiveBufferSettings,
	logicalLaneReceiveBudget *TransferMemoryBudget,
) *ReceiveSequence {
	cancelCtx, cancel := context.WithCancel(ctx)
	encryptionRole, ok := sequenceTlsRoleFromProtobuf(transferKey.EncryptionRole)
	if !ok {
		encryptionRole = sequenceTlsRoleServer
		transferKey.EncryptionRole = encryptionRole.toProtobuf()
	}
	sequenceBufferSize := logicalLaneSequenceBufferSize(
		receiveBufferSettings.SequenceBufferSize,
		transferKey.LogicalLane,
	)
	h1SequenceBufferSize := receiveBufferSettings.H1SequenceBufferSize
	if h1SequenceBufferSize <= 0 {
		h1SequenceBufferSize = receiveBufferSettings.SequenceBufferSize
	}
	h1SequenceBufferSize = logicalLaneSequenceBufferSize(
		h1SequenceBufferSize,
		transferKey.LogicalLane,
	)
	h1SequenceBufferAdaptiveMaxSize := h1SequenceBufferSize
	h1SequenceBufferAdaptiveStepSize := 0
	h1SequenceBufferAdaptiveSaturationThreshold := 0
	h1SequenceBufferAdaptiveSaturationWindow := time.Duration(0)
	if h1SequenceBufferSize > 0 &&
		receiveBufferSettings.H1SequenceBufferAdaptiveMaxSize > 0 &&
		receiveBufferSettings.H1SequenceBufferAdaptiveStepSize > 0 &&
		receiveBufferSettings.H1SequenceBufferAdaptiveSaturationThreshold > 0 &&
		receiveBufferSettings.H1SequenceBufferAdaptiveSaturationWindow > 0 {
		configuredMax := logicalLaneSequenceBufferSize(
			receiveBufferSettings.H1SequenceBufferAdaptiveMaxSize,
			transferKey.LogicalLane,
		)
		if h1SequenceBufferSize < configuredMax {
			h1SequenceBufferAdaptiveMaxSize = configuredMax
			h1SequenceBufferAdaptiveStepSize = logicalLaneSequenceBufferSize(
				receiveBufferSettings.H1SequenceBufferAdaptiveStepSize,
				transferKey.LogicalLane,
			)
			h1SequenceBufferAdaptiveSaturationThreshold =
				receiveBufferSettings.H1SequenceBufferAdaptiveSaturationThreshold
			h1SequenceBufferAdaptiveSaturationWindow =
				receiveBufferSettings.H1SequenceBufferAdaptiveSaturationWindow
		}
	}
	h1SequenceBufferByteCount := receiveBufferSettings.H1SequenceBufferByteCount
	if h1SequenceBufferByteCount <= 0 {
		h1SequenceBufferByteCount = receiveBufferSettings.SequenceBufferByteCount
	}
	h1SequenceBufferAdaptiveMaxByteCount := h1SequenceBufferByteCount
	h1SequenceBufferAdaptiveStepByteCount := ByteCount(0)
	if h1SequenceBufferAdaptiveMaxSize > h1SequenceBufferSize &&
		receiveBufferSettings.H1SequenceBufferAdaptiveMaxByteCount > h1SequenceBufferByteCount &&
		receiveBufferSettings.H1SequenceBufferAdaptiveStepByteCount > 0 {
		h1SequenceBufferAdaptiveMaxByteCount =
			receiveBufferSettings.H1SequenceBufferAdaptiveMaxByteCount
		h1SequenceBufferAdaptiveStepByteCount =
			receiveBufferSettings.H1SequenceBufferAdaptiveStepByteCount
	}
	channelBufferSize := max(sequenceBufferSize, h1SequenceBufferAdaptiveMaxSize)
	receiveQueueBudget := receiveBufferSettings.ReceiveQueueBudget
	receiveQueueMinByteCount := receiveBufferSettings.ReceiveQueueMinByteCount
	if transferKey.LogicalLane != 0 {
		receiveQueueMinByteCount = 0
		if receiveQueueBudget == nil {
			receiveQueueBudget = logicalLaneReceiveBudget
		}
	}
	source = source.LocalMask()
	seq := &ReceiveSequence{
		ctx:                             cancelCtx,
		cancel:                          cancel,
		done:                            make(chan struct{}),
		client:                          client,
		log:                             client.log,
		source:                          source,
		sequenceId:                      sequenceId,
		transferKey:                     transferKey,
		encryptionRole:                  encryptionRole,
		encryptionCompanion:             transferKey.EncryptionCompanion,
		receiveBufferSettings:           receiveBufferSettings,
		openReceiveContracts:            map[Id]*sequenceContract{},
		receiveContract:                 nil,
		packs:                           make(chan *ReceivePack, channelBufferSize),
		packQueueBaseLimit:              int64(sequenceBufferSize),
		packQueueH1Limit:                int64(h1SequenceBufferSize),
		packQueueH1AdaptiveMaxLimit:     int64(h1SequenceBufferAdaptiveMaxSize),
		packQueueH1AdaptiveStep:         int64(h1SequenceBufferAdaptiveStepSize),
		packQueueH1SaturationThreshold:  h1SequenceBufferAdaptiveSaturationThreshold,
		packQueueH1SaturationWindow:     h1SequenceBufferAdaptiveSaturationWindow,
		packQueueH1AdaptiveMaxByteLimit: h1SequenceBufferAdaptiveMaxByteCount,
		packQueueH1AdaptiveByteStep:     h1SequenceBufferAdaptiveStepByteCount,
		packQueueBaseByteLimit:          receiveBufferSettings.SequenceBufferByteCount,
		packQueueH1ByteLimit:            h1SequenceBufferByteCount,
		packQueueSpace:                  make(chan struct{}, 1),
		receiveQueue:                    newReceiveQueue(receiveQueueBudget, receiveQueueMinByteCount),
		nextSequenceNumber:              0,
		idleCondition:                   NewIdleCondition(),
		ackWindow:                       newSequenceAckWindow(),
		exit:                            make(chan struct{}),
	}
	// Never encrypt control-plane traffic. A ReceiveSequence's data source is
	// the peer (source.SourceId) and its destination is always this client
	// (client.ClientId()); when `ReceiveNoSession` holds for either endpoint,
	// no session is acquired and inbound traffic is taken in plaintext.
	if client != nil && client.encryptionSessionManager != nil &&
		!client.encryptionSessionManager.ReceiveNoSession(source.SourceId) {
		seq.session = client.encryptionSessionManager.Acquire(
			source.SourceId,
			encryptionRole,
			transferKey.EncryptionCompanion,
		)
	}
	return seq
}

func (self *ReceiveSequence) ReceiveQueueSizeAndMessageTypes() (int, ByteCount, []protocol.MessageType) {
	unpackMessageTypes := func(item *receiveItem) any {
		var messageTypes []protocol.MessageType
		var transferFrame protocol.TransferFrame
		err := proto.Unmarshal(item.transferFrameBytes, &transferFrame)
		if err == nil && transferFrame.Pack != nil {
			for _, frame := range transferFrame.Pack.Frames {
				messageTypes = append(messageTypes, frame.MessageType)
			}
		}
		return messageTypes
	}
	count, byteSize, summary := self.receiveQueue.QueueSizeAndSummary(unpackMessageTypes)
	var messageTypes []protocol.MessageType
	for _, summaryMessageTypes := range summary {
		messageTypes = append(messageTypes, summaryMessageTypes.([]protocol.MessageType)...)
	}
	return count, byteSize, messageTypes
}

// tryDeepenH1PackQueue grants one bounded count and/or logical-byte step only
// after distinct Pack calls repeatedly observe a full H1 queue without a
// substantial drain. It does not reserve message memory: the resulting
// per-flow limits and exact shared Pack budget still decide the subsequent
// admission.
func (self *ReceiveSequence) tryDeepenH1PackQueue(
	currentCount int64,
	currentByteCount ByteCount,
	byteCount ByteCount,
	budgetByteCount ByteCount,
	saturationRecorded *bool,
) bool {
	if *saturationRecorded {
		return false
	}
	*saturationRecorded = true
	self.client.receivePackHandoffSaturationCount.Add(1)

	threshold := self.packQueueH1SaturationThreshold
	if threshold <= 0 ||
		(self.packQueueH1AdaptiveMaxLimit <= self.packQueueH1Limit &&
			self.packQueueH1AdaptiveMaxByteLimit <= self.packQueueH1ByteLimit) {
		return false
	}
	now := time.Now()
	if nowForTest := self.receiveBufferSettings.h1SaturationNowForTest; nowForTest != nil {
		now = nowForTest()
	}
	if elapsed := now.Sub(self.packQueueH1LastSaturationTime); self.packQueueH1LastSaturationTime.IsZero() ||
		elapsed < 0 || self.packQueueH1SaturationWindow < elapsed {
		self.packQueueH1SaturationStreak = 0
	}
	self.packQueueH1LastSaturationTime = now
	if self.packQueueH1SaturationStreak < threshold {
		self.packQueueH1SaturationStreak++
	}
	if self.packQueueH1SaturationStreak < threshold {
		return false
	}

	previousLimit := self.packQueueH1Limit
	nextLimit := min(
		self.packQueueH1AdaptiveMaxLimit,
		previousLimit+self.packQueueH1AdaptiveStep,
	)
	previousByteLimit := self.packQueueH1ByteLimit
	nextByteLimit := previousByteLimit
	if 0 < self.packQueueH1AdaptiveByteStep &&
		previousByteLimit < self.packQueueH1AdaptiveMaxByteLimit {
		nextByteLimit = min(
			self.packQueueH1AdaptiveMaxByteLimit,
			previousByteLimit+self.packQueueH1AdaptiveByteStep,
		)
	}
	if nextLimit <= previousLimit && nextByteLimit <= previousByteLimit {
		return false
	}
	// Earning a limit allocates no Pack ownership, but require the next step to
	// admit the packet that demonstrated saturation. This avoids recording a
	// useless count increase when a fixed logical byte cap is the real bound.
	if 0 < nextLimit && nextLimit <= currentCount {
		return false
	}
	if 0 < nextByteLimit && currentByteCount != 0 &&
		(currentByteCount > nextByteLimit || nextByteLimit-currentByteCount < byteCount) {
		return false
	}
	if budget := self.receiveBufferSettings.PackQueueBudget; budget != nil &&
		budget.Available() < budgetByteCount {
		return false
	}
	self.packQueueH1Limit = nextLimit
	self.packQueueH1ByteLimit = nextByteLimit
	self.packQueueH1SaturationStreak = 0
	self.client.receivePackHandoffDepthGrowCount.Add(1)
	if !self.packQueueH1Deepened {
		self.packQueueH1Deepened = true
		self.client.receivePackHandoffDeepenedFlowCount.Add(1)
	}
	updateAtomicMaximum(
		&self.client.receivePackHandoffAdaptiveMaxDepth,
		uint64(nextLimit),
	)
	updateAtomicMaximum(
		&self.client.receivePackHandoffAdaptiveMaxByteCount,
		uint64(nextByteLimit),
	)
	return true
}

// success, error
func (self *ReceiveSequence) Pack(receivePack *ReceivePack, timeout time.Duration) (bool, error) {
	self.packMutex.Lock()
	defer self.packMutex.Unlock()

	select {
	case <-self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}

	if !self.idleCondition.UpdateOpen() {
		return false, errors.New("Done.")
	}
	defer self.idleCondition.UpdateClose()

	h1SaturationRecorded := false
	packQueueBudget := self.receiveBufferSettings.PackQueueBudget
	reserve := func() bool {
		byteCount := ByteCount(len(receivePack.TransferFrameBytes))
		if byteCount <= 0 {
			byteCount = max(1, receivePack.MessageByteCount)
		}
		budgetByteCount := byteCount
		if packQueueBudget != nil &&
			self.receiveBufferSettings.PackQueueRetainedByteAccounting {
			budgetByteCount = receivePack.receiveQueueByteCount()
		}
		countLimit := self.packQueueBaseLimit
		if receivePack.TransportType == TransportTypeH1 {
			countLimit = self.packQueueH1Limit
		}
		var count int64
		for {
			current := self.packQueueCount.Load()
			// A nonpositive count retains the legacy unbuffered/synchronous
			// behavior: channel readiness, rather than this reservation, decides.
			if 0 < countLimit && countLimit <= current {
				if receivePack.TransportType == TransportTypeH1 &&
					self.tryDeepenH1PackQueue(
						current,
						self.packQueueByteCount.Load(),
						byteCount,
						budgetByteCount,
						&h1SaturationRecorded,
					) {
					countLimit = self.packQueueH1Limit
					continue
				}
				return false
			}
			if self.packQueueCount.CompareAndSwap(current, current+1) {
				count = current + 1
				break
			}
		}
		byteLimit := self.packQueueBaseByteLimit
		if receivePack.TransportType == TransportTypeH1 {
			byteLimit = self.packQueueH1ByteLimit
		}
		for {
			current := self.packQueueByteCount.Load()
			// One oversized message may enter an empty queue so a configured
			// byte limit cannot deadlock progress.
			if 0 < byteLimit && current != 0 && byteLimit-current < byteCount {
				if receivePack.TransportType == TransportTypeH1 &&
					self.tryDeepenH1PackQueue(
						self.packQueueCount.Load(),
						current,
						byteCount,
						budgetByteCount,
						&h1SaturationRecorded,
					) {
					byteLimit = self.packQueueH1ByteLimit
					continue
				}
				self.packQueueCount.Add(-1)
				return false
			}
			if self.packQueueByteCount.CompareAndSwap(current, current+byteCount) {
				if packQueueBudget != nil && !packQueueBudget.TryReserve(budgetByteCount) {
					if remaining := self.packQueueByteCount.Add(-byteCount); remaining < 0 {
						panic("negative receive sequence handoff byte count")
					}
					if remaining := self.packQueueCount.Add(-1); remaining < 0 {
						panic("negative receive sequence handoff count")
					}
					return false
				}
				receivePack.sequenceQueueByteCount = byteCount
				if packQueueBudget != nil {
					receivePack.sequenceQueueBudgetByteCount = budgetByteCount
					receivePack.sequenceQueueBudget = packQueueBudget
				}
				updateAtomicMaximum(&self.client.receivePackHandoffMaxCount, uint64(count))
				updateAtomicMaximum(
					&self.client.receivePackHandoffMaxByteCount,
					uint64(current+byteCount),
				)
				return true
			}
		}
	}
	release := func() {
		self.releasePackQueue(receivePack)
	}

	// Fast path without arming a timer. A failed reservation and a full channel
	// are both ordinary nonblocking loss when the caller selected timeout zero.
	if reserve() {
		select {
		case self.packs <- receivePack:
			return true, nil
		default:
			release()
		}
	}
	if timeout == 0 {
		return false, nil
	}

	self.client.receivePackHandoffWaitCount.Add(1)
	var timeoutChan <-chan time.Time
	if 0 < timeout {
		timeoutChan = resetOrCreateTimer(&self.packTimer, timeout)
	}
	stopTimer := func() {
		if self.packTimer != nil {
			self.packTimer.Stop()
		}
	}

	// H1's carrier is already reliable. Wait for a coalesced release edge and
	// retry the count/byte reservation until the one total timeout expires,
	// then reserve while waiting for the channel send. This turns a short
	// scheduler mismatch into transport backpressure rather than a synthetic
	// Transfer loss/retransmission cycle.
	for {
		var budgetNotify <-chan struct{}
		if budget := self.receiveBufferSettings.PackQueueBudget; budget != nil {
			// Subscribe before admission so a concurrent release cannot be lost.
			budgetNotify = budget.CapacityNotify()
		}
		if reserve() {
			break
		}
		select {
		case <-self.ctx.Done():
			stopTimer()
			return false, errors.New("Done.")
		case <-self.packQueueSpace:
		case <-budgetNotify:
		case <-timeoutChan:
			return false, nil
		}
	}
	select {
	case <-self.ctx.Done():
		stopTimer()
		release()
		return false, errors.New("Done.")
	case self.packs <- receivePack:
		stopTimer()
		self.client.receivePackHandoffWaitSuccess.Add(1)
		return true, nil
	case <-timeoutChan:
		release()
		return false, nil
	}
}

// Releases bytes as soon as the sequence worker owns a dequeued Pack. Later
// receiveQueue ownership has its own budget and must not be double-counted as
// handoff-channel retention.
func (self *ReceiveSequence) releasePackQueue(receivePack *ReceivePack) {
	if receivePack == nil || receivePack.sequenceQueueByteCount <= 0 {
		return
	}
	byteCount := receivePack.sequenceQueueByteCount
	receivePack.sequenceQueueByteCount = 0
	budget := receivePack.sequenceQueueBudget
	receivePack.sequenceQueueBudget = nil
	if budget != nil {
		budgetByteCount := receivePack.sequenceQueueBudgetByteCount
		receivePack.sequenceQueueBudgetByteCount = 0
		budget.Release(budgetByteCount)
	}
	if remaining := self.packQueueByteCount.Add(-byteCount); remaining < 0 {
		panic("negative receive sequence handoff byte count")
	}
	if remaining := self.packQueueCount.Add(-1); remaining < 0 {
		panic("negative receive sequence handoff count")
	}
	select {
	case self.packQueueSpace <- struct{}{}:
	default:
	}
}

func (self *ReceiveSequence) Run() {
	ackWorkerDone := make(chan struct{})
	ackWorkerStop := make(chan struct{})
	ackWorkerStarted := false
	// Sequence cancellation stops Pack processing, but cleanup still has to
	// flush delivered items into the ACK window before it explicitly stops the
	// ACK worker. A sibling context keeps ordinary ACK writes alive until that
	// final drain begins; cleanup then cancels route waits before joining them.
	ackWriteCtx, cancelAckWrites := context.WithCancel(context.WithoutCancel(self.ctx))
	defer cancelAckWrites()
	defer func() {
		if r := recover(); r != nil {
			self.log.Errorf("[r]%s<-%s s(%s) abnormal exit =  %s\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId, r)
			panic(r)
		}
	}()
	defer func() {
		// deliver-then-die: an exit path (error, idle, cancel) must not strand
		// buffered head items — deliver their frames, send their acks, and
		// return their pool buffers
		var deliverPanic any
		func() {
			defer func() {
				deliverPanic = recover()
			}()
			self.flushDeliver()
		}()
		// The ACK worker owns its route writer. Its stop path snapshots the final
		// ACK window before closing. Cancel route waits before joining: Write
		// still tries every immediately writable route before consulting the
		// context, preserving final ACK delivery without letting a backpressured
		// or absent route deadlock teardown (including an infinite WriteTimeout).
		if ackWorkerStarted {
			if self.receiveBufferSettings.beforeAckWorkerStopForTest != nil {
				self.receiveBufferSettings.beforeAckWorkerStopForTest(self.id())
			}
			close(ackWorkerStop)
			cancelAckWrites()
			if self.receiveBufferSettings.afterAckWritesCanceledForTest != nil {
				self.receiveBufferSettings.afterAckWritesCanceledForTest(self.id())
			}
			<-ackWorkerDone
		}
		self.closeContractWriterAlias()
		self.cancel()

		// close previous contracts and checkpoint the current contract
		for _, receiveContract := range self.openReceiveContracts {
			if self.receiveContract != receiveContract {
				if receiveContract.unackedByteCount != 0 {
					self.log.Infof("[r]%s<-%s s(%s) close contract with unacked =  %d\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId, receiveContract.unackedByteCount)
				}
				self.client.ContractManager().CloseContract(
					receiveContract.contractId,
					receiveContract.ackedByteCount,
					receiveContract.unackedByteCount,
				)
			}
		}
		if self.receiveContract != nil {
			// the sender may send again with this contract (set as head)
			// checkpoint the contract but do not close it
			if self.receiveContract.unackedByteCount != 0 {
				self.log.Infof("[r]%s<-%s s(%s) checkpoint contract with unacked =  %d\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId, self.receiveContract.unackedByteCount)
			}
			self.client.ContractManager().CheckpointContract(
				self.receiveContract.contractId,
				self.receiveContract.ackedByteCount,
				self.receiveContract.unackedByteCount,
			)
		}

		// drain the buffer, releasing any borrowed budget
		for _, item := range self.receiveQueue.Clear() {
			self.peerAudit.Update(func(a *PeerAudit) {
				a.discard(item.messageByteCount)
			})
			// MessagePoolReturn(item.transferFrameBytes)
			item.messagePoolReturn()
		}

		self.peerAudit.Complete()

		if self.session != nil {
			self.session.Release()
		}

		close(self.exit)
		if deliverPanic != nil {
			panic(deliverPanic)
		}
	}()

	self.peerAudit = NewSequencePeerAudit(
		self.client,
		self.source,
		self.receiveBufferSettings.MaxPeerAuditDuration,
	)

	// compress and send acks
	ackWorkerStarted = true
	go HandleError(func() {
		defer close(ackWorkerDone)
		defer self.cancel()

		ackDestination := DestinationId(self.source.SourceId)
		ackMultiRouteWriter := self.client.RouteManager().OpenMultiRouteWriter(
			ackDestination,
		)
		defer self.client.RouteManager().CloseMultiRouteWriter(ackMultiRouteWriter)
		if self.receiveBufferSettings.afterAckWriterOpenForTest != nil {
			self.receiveBufferSettings.afterAckWriterOpenForTest(
				self.id(),
				ackMultiRouteWriter,
			)
		}

		writeAck := func(sendAck sequenceAck) {
			path := sendTransferPath(self.client.ClientId(), ackDestination)

			// what this receiver can still hold out of order, so the sender may
			// clamp its window to it (THROUGHPUTFIX §37.3). A receiver that
			// does not advertise is indistinguishable from a legacy peer, and
			// the sender falls back to assuming the shipped hold.
			receiveWindowByteCount := uint64(0)
			advertiseReceiveWindow := self.receiveBufferSettings.AdvertiseReceiveWindow
			if advertiseReceiveWindow {
				receiveWindowByteCount = self.receiveWindowAdvertisement()
			}
			// Evictions ride the next acknowledgement whatever the
			// advertisement setting: a receiver that withdraws bytes has to say
			// so even to a peer that never told it anything
			// (THROUGHPUTFIX §37.16).
			evictedSequenceNumbers := []uint64(nil)
			if self.receiveBufferSettings.EvictionNotice {
				evictedSequenceNumbers = self.takeEvictions()
			}

			var transferFrameBytes []byte
			if 2 <= self.receiveBufferSettings.ProtocolVersion {
				// hand-rolled marshal of the hot Ack TransferFrame; wire-identical
				// to the proto structs in the legacy branch (see frame_protobuf_test.go).
				saf := sendAckFrame{
					path:                    path,
					messageId:               sendAck.messageId,
					sequenceId:              self.sequenceId,
					selective:               sendAck.selective,
					tagSendTime:             sendAck.tag.sendTime,
					tagSet:                  sendAck.tag.set,
					compactContractRecovery: sendAck.compactContractRecoverySupported,
					logicalLaneVersion:      transferLogicalLaneVersion,
					receiveWindowByteCount:  receiveWindowByteCount,
					receiveWindowSet:        advertiseReceiveWindow,
					evictedSequenceNumbers:  evictedSequenceNumbers,
				}
				if sendAck.contractMissing {
					saf.missingContractId = &sendAck.missingContractId
				}
				transferFrameBytes = marshalSendAckTransferFrame(&saf)
			} else {
				ack := &protocol.Ack{
					MessageId:               sendAck.messageId.Bytes(),
					SequenceId:              self.sequenceId.Bytes(),
					Selective:               sendAck.selective,
					Tag:                     sendAck.tag.protocol(),
					CompactContractRecovery: sendAck.compactContractRecoverySupported,
					LogicalLaneVersion:      transferLogicalLaneVersion,
				}
				if advertiseReceiveWindow {
					ack.ReceiveWindowByteCount = &receiveWindowByteCount
				}
				ack.EvictedSequenceNumbers = evictedSequenceNumbers
				if sendAck.contractMissing {
					ack.MissingContractId = sendAck.missingContractId.Bytes()
				}
				ackBytes, _ := ProtoMarshal(ack)
				defer MessagePoolReturn(ackBytes)
				transferFrame := &protocol.TransferFrame{
					TransferPath: path.ToProtobuf(),
					Frame: &protocol.Frame{
						MessageType:  protocol.MessageType_TransferAck,
						MessageBytes: ackBytes,
					},
				}
				transferFrameBytes, _ = ProtoMarshal(transferFrame)
			}
			defer MessagePoolReturn(transferFrameBytes)
			writeFrame := func(frameBytes []byte) error {
				shared := MessagePoolShareReadOnly(frameBytes)
				var writeErr error
				blocked := false
				priority := false
				var waitDuration time.Duration
				// The carrier the acknowledgement actually left on, which is
				// what a campaign needs to read; the carrier of the Pack it
				// answers is only the preference (FLIGHTGATEFIX §20.3).
				writtenTransportType := sendAck.transportType
				if selector, ok := ackMultiRouteWriter.(*MultiRouteSelector); ok {
					if success, _ := selector.tryWriteH1AckPriorityWithCarrierPreference(
						shared,
						sendAck.transportType,
					); success {
						priority = true
						writtenTransportType = TransportTypeH1
					} else if sendAck.transportType != TransportTypeUnknown {
						var success bool
						var disposition transferWriteDisposition
						success, disposition, writeErr = selector.writeDetailedReplyWithCarrierPreference(
							ackWriteCtx,
							shared,
							self.receiveBufferSettings.WriteTimeout,
							sendAck.transportType,
						)
						blocked = disposition.initiallyBlocked
						waitDuration = disposition.initialWaitDuration
						if disposition.transportType != "" {
							writtenTransportType = disposition.transportType
						}
						if writeErr == nil && !success {
							writeErr = errTransferRouteWriteTimeout
						}
					} else {
						writeErr = ackMultiRouteWriter.Write(
							ackWriteCtx,
							shared,
							self.receiveBufferSettings.WriteTimeout,
						)
					}
				} else {
					writeErr = ackMultiRouteWriter.Write(
						ackWriteCtx,
						shared,
						self.receiveBufferSettings.WriteTimeout,
					)
				}
				self.client.recordReceiveAckRouteWrite(
					writtenTransportType,
					waitDuration,
					blocked,
					priority,
					writeErr,
				)
				if writeErr != nil {
					// A failed write leaves ownership here: undo the consumer's share.
					MessagePoolReturn(shared)
				}
				return writeErr
			}
			c := func() error {
				// outer-wrap the ack TransferFrame with the per-peer
				// session cipher when available. Mirror the wrap state
				// of the acked pack: if any pack covered by this ack
				// arrived plaintext, send the ack plaintext too — the
				// sender's cipher may not yet be established (it sent
				// plaintext because it had no cipher at send time), so
				// a wrapped ack would be unreadable on arrival.
				var cipher *sequenceCipher
				if self.session != nil && !sendAck.unwrapped {
					cipher = self.session.Cipher()
				}
				if cipher == nil {
					return writeFrame(transferFrameBytes)
				}
				wrapped, sealErr := cipher.SealOuterFrame(
					path,
					transferFrameBytes,
					self.session.role.toProtobuf(),
					self.session.companion,
				)
				if sealErr != nil {
					return fmt.Errorf("ack outer wrap seal: %w", sealErr)
				}
				defer MessagePoolReturn(wrapped)
				return writeFrame(wrapped)
			}
			if self.log.V(2).Enabled() {
				TraceWithReturn(
					fmt.Sprintf(
						"[r]multi route write (ack %d) %s->%s s(%s)",
						sendAck.sequenceNumber,
						self.client.ClientTag(),
						self.source.SourceId,
						self.source.StreamId,
					),
					c,
				)
			} else {
				err := c()
				if err != nil {
					if ok, suppressed := shouldLogDropErr(); ok {
						if suppressed > 0 {
							self.log.Infof("[r]drop = %s (%d suppressed)", err, suppressed)
						} else {
							self.log.Infof("[r]drop = %s", err)
						}
					} else if v := self.log.V(1); v.Enabled() {
						v.Infof("[r]drop = %s", err)
					}
				}
			}
		}

		// reusable ack-compress timer (avoids a per-iteration time.After alloc on
		// the ack hot path). created already-fired; Reset before the blocking
		// select arms it (go1.23+ delivers no stale fire after Reset).
		ackCompressTimer := time.NewTimer(0)
		defer ackCompressTimer.Stop()
		writeSnapshot := func(ackSnapshot sequenceAckWindowSnapshot) bool {
			wrote := false
			if 0 < ackSnapshot.ackUpdateCount {
				writeAck(ackSnapshot.headAck)
				wrote = true
			}
			for messageId, ack := range ackSnapshot.selectiveAcks {
				ack.messageId = messageId
				ack.selective = true
				writeAck(ack)
				wrote = true
			}
			for messageId, ack := range ackSnapshot.contractMissingAcks {
				ack.messageId = messageId
				ack.contractMissing = true
				writeAck(ack)
				wrote = true
			}
			return wrote
		}
		lastAckWriteTime := time.Time{}
		writePending := func() {
			if writeSnapshot(self.ackWindow.Snapshot(true)) {
				lastAckWriteTime = time.Now()
				if self.receiveBufferSettings.afterAckWriteForTest != nil {
					self.receiveBufferSettings.afterAckWriteForTest(self.id())
				}
			}
		}
		drainAndStop := func() {
			writePending()
		}
		// ctxDone is disabled after its first edge. Cancellation may drain the
		// ACKs already visible at that instant, but only ackWorkerStop may end
		// this worker: Run cleanup can publish more ACKs while flushing its final
		// delivered batch.
		ctxDone := self.ctx.Done()
		drainCanceledSequence := func() {
			ctxDone = nil
			writePending()
		}

		for {
			select {
			case <-ctxDone:
				drainCanceledSequence()
			case <-ackWorkerStop:
				drainAndStop()
				return
			default:
			}

			if !self.ackWindow.Pending() {
				// wait for one ack
				select {
				case <-ctxDone:
					drainCanceledSequence()
				case <-ackWorkerStop:
					drainAndStop()
					return
				case <-self.ackWindow.Notify():
				}
			}

			// An idle sequence has no ACK traffic to compress, so publish its first
			// cumulative ACK immediately. During a sustained stream, retain the
			// same maximum ACK rate by waiting only until the previous write is one
			// compression interval old. This removes a fixed 10 ms from sparse H1
			// request/response turns without recreating one ACK per data Pack.
			ackCompressWait := time.Duration(0)
			if timeout := self.receiveBufferSettings.AckCompressTimeout; 0 < timeout && !lastAckWriteTime.IsZero() {
				ackCompressWait = time.Until(lastAckWriteTime.Add(timeout))
			}
			if 0 < ackCompressWait {
				ackCompressTimer.Reset(ackCompressWait)
				if self.receiveBufferSettings.beforeAckCompressWaitForTest != nil {
					self.receiveBufferSettings.beforeAckCompressWaitForTest(self.id())
				}
				select {
				case <-ctxDone:
					drainCanceledSequence()
				case <-ackWorkerStop:
					drainAndStop()
					return
				case <-ackCompressTimer.C:
				}
			}

			writePending()
		}
	}, self.cancel)

	// reusable idle/gap timer (avoids a per-iteration time.After alloc on the
	// receive hot path). created already-fired; Reset before the blocking select
	// arms it (go1.23+ delivers no stale fire after Reset).
	idleTimer := time.NewTimer(0)
	defer idleTimer.Stop()

	for {
		receiveTime := time.Now()
		var timeout time.Duration

		if queueSize, _ := self.receiveQueue.QueueSize(); 0 == queueSize {
			timeout = self.receiveBufferSettings.IdleTimeout
		} else {
			timeout = self.receiveBufferSettings.GapTimeout
			for {
				item := self.receiveQueue.PeekFirst()
				if item == nil {
					break
				}

				itemGapTimeout := item.receiveTime.Add(self.receiveBufferSettings.GapTimeout).Sub(receiveTime)
				if itemGapTimeout < 0 {
					self.log.Errorf("[r]%s<-%s s(%s) exit gap timeout\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId)
					// did not receive a preceding message in time
					return
				}

				if self.nextSequenceNumber < item.sequenceNumber {
					if itemGapTimeout < timeout {
						timeout = itemGapTimeout
					}
					break
				}
				// item.sequenceNumber <= self.nextSequenceNumber

				self.receiveQueue.RemoveByMessageId(item.messageId)

				if self.nextSequenceNumber == item.sequenceNumber {
					// this item is the head of sequence
					if err := self.registerContracts(item); err != nil {
						self.log.Errorf("[r]%s<-%s s(%s) exit could not register contracts = %s\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId, err)
						return
					}
					if self.updateContract(item) {
						if self.log.V(1).Enabled() {
							self.log.Infof("[r]seq+ %d->%d (queue) %s<-%s s(%s)\n", self.nextSequenceNumber, self.nextSequenceNumber+1, self.client.ClientTag(), self.source.SourceId, self.source.StreamId)
						}
						self.nextSequenceNumber = self.nextSequenceNumber + 1
						self.receiveHead(item)
					} else {
						// no valid contract. it should have been attached to the head
						self.log.Errorf("[r]drop head no contract %s<-%s s(%s)\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId)
						return
					}
				} else {
					// this item is a resend of a previous item
					if item.ack {
						self.sendAck(
							item.sequenceNumber,
							item.messageId,
							false,
							sequenceTag{},
							item.unwrapped,
							item.transportType,
						)
					}
					item.messagePoolReturn()
				}
			}
			// The delivery point has moved, so items that were held
			// tentatively may now be safe: fewer can be missing below them
			// (THROUGHPUTFIX §37.20).
			if self.receiveBufferSettings.ReceiveHoldPolicy == ReceiveHoldCommittedPrefix {
				self.commitHeldPrefix()
			}
		}

		processPack := func(receivePack *ReceivePack, ok bool) bool {
			if !ok {
				return false
			}

			if receivePack.Pack.Nack {
				received, err := self.receiveNack(receivePack)
				if err != nil {
					// bad message
					// close the sequence
					self.log.Infof("[r]%s<-%s s(%s) exit could not receive nack = %s\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId, err)
					self.peerAudit.Update(func(a *PeerAudit) {
						a.badMessage(receivePack.MessageByteCount)
					})
					receivePack.messagePoolReturn()
					return false
				} else if !received {
					if self.log.V(1).Enabled() {
						self.log.Infof("[r]drop nack %s<-%s s(%s)\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId)
					}
					// drop the message
					self.peerAudit.Update(func(a *PeerAudit) {
						a.discard(receivePack.MessageByteCount)
					})
					receivePack.messagePoolReturn()
				}

				// note messages of `size < MinMessageByteCount` get counted as `MinMessageByteCount` against the contract
			} else {
				received, err := self.receive(receivePack)
				if err != nil {
					// bad message
					// close the sequence
					self.log.Errorf("[r]%s<-%s s(%s) exit could not receive ack = %s\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId, err)
					self.peerAudit.Update(func(a *PeerAudit) {
						a.badMessage(receivePack.MessageByteCount)
					})
					receivePack.messagePoolReturn()
					return false
				} else if !received {
					if self.log.V(1).Enabled() {
						self.log.Infof("[r]drop ack %s<-%s s(%s)\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId)
					}
					// drop the message
					self.peerAudit.Update(func(a *PeerAudit) {
						a.discard(receivePack.MessageByteCount)
					})
					receivePack.messagePoolReturn()
				}
			}
			return true
		}

		// fast path without arming a timer
		select {
		case <-self.ctx.Done():
			return
		case receivePack, ok := <-self.packs:
			self.releasePackQueue(receivePack)
			if !processPack(receivePack, ok) {
				return
			}
			continue
		default:
		}

		// the packs channel is drained: dispatch the buffered head items in
		// one combined receive callback before waiting
		self.flushDeliver()

		checkpointId := self.idleCondition.Checkpoint()
		idleTimer.Reset(timeout)
		select {
		case <-self.ctx.Done():
			return
		case receivePack, ok := <-self.packs:
			self.releasePackQueue(receivePack)
			if !processPack(receivePack, ok) {
				return
			}
		case <-idleTimer.C:
			if 0 == self.receiveQueue.Len() {
				done := false
				func() {
					self.packMutex.Lock()
					defer self.packMutex.Unlock()
					// idle timeout
					if self.idleCondition.Close(checkpointId) {
						done = true
					}
					// else there are pending updates
				}()
				if done {
					// close the sequence
					if self.log.V(1).Enabled() {
						self.log.Infof("[r]%s<-%s s(%s) exit idle timeout\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId)
					}
					return
				}
			}
		}
	}
}

func (self *ReceiveSequence) sendAck(
	sequenceNumber uint64,
	messageId Id,
	selective bool,
	tag sequenceTag,
	unwrapped bool,
	transportType TransportType,
) {
	ack := sequenceAck{
		sequenceNumber:                   sequenceNumber,
		messageId:                        messageId,
		selective:                        selective,
		tag:                              tag,
		compactContractRecoverySupported: true,
		unwrapped:                        unwrapped,
		transportType:                    transportType,
	}
	self.ackWindow.Update(ack)
}

func (self *ReceiveSequence) sendContractMissing(
	sequenceNumber uint64,
	messageId Id,
	contractId Id,
	unwrapped bool,
	transportType TransportType,
) {
	self.client.missingContractRequestCount.Add(1)
	self.ackWindow.UpdateContractMissing(sequenceAck{
		sequenceNumber:                   sequenceNumber,
		messageId:                        messageId,
		contractMissing:                  true,
		missingContractId:                contractId,
		compactContractRecoverySupported: true,
		unwrapped:                        unwrapped,
		transportType:                    transportType,
	})
}

// Decodes the optional reference shared by compact acknowledged heads and
// Nack packets. The id is only useful inside this receive sequence's map of
// contracts that were already verified in full.
func receivePackContractId(pack *protocol.Pack) (*Id, error) {
	if pack == nil || pack.ContractId == nil {
		return nil, nil
	}
	contractId, err := IdFromBytes(pack.ContractId)
	if err != nil {
		return nil, errors.New("Bad contract_id")
	}
	return &contractId, nil
}

func (self *ReceiveSequence) receive(receivePack *ReceivePack) (bool, error) {
	receiveTime := time.Now()

	sequenceNumber := receivePack.Pack.SequenceNumber
	contractId, err := receivePackContractId(receivePack.Pack)
	if err != nil {
		return false, err
	}
	messageId, err := IdFromBytes(receivePack.Pack.MessageId)
	if err != nil {
		return false, errors.New("Bad message_id")
	}

	// note the receive contract is the contract active when this is at the head of the queue
	var item *receiveItem
	if receivePack.decodedOwner != nil {
		item = &receivePack.decodedOwner.receiveItem
	} else {
		item = &receiveItem{}
	}
	queueByteCount := receivePack.MessageByteCount
	if self.receiveBufferSettings.ReceiveQueueRetainedByteAccounting {
		queueByteCount = receivePack.receiveQueueByteCount()
	}
	*item = receiveItem{
		transferItem: transferItem{
			messageId:        messageId,
			sequenceNumber:   sequenceNumber,
			messageByteCount: receivePack.MessageByteCount,
			queueByteCount:   queueByteCount,
		},

		contractId:         contractId,
		receiveTime:        receiveTime,
		frames:             receivePack.Pack.Frames,
		contractFrame:      receivePack.Pack.ContractFrame,
		receiveCallback:    receivePack.ReceiveCallback,
		head:               receivePack.Pack.Head,
		ack:                !receivePack.Pack.Nack,
		tag:                sequenceTagFromProtocol(receivePack.Pack.Tag),
		decodedOwner:       receivePack.decodedOwner,
		transferFrameBytes: receivePack.TransferFrameBytes,
		transportType:      receivePack.TransportType,
		unwrapped:          receivePack.Unwrapped,
	}

	// A compact head is meaningful only against a contract verified earlier in
	// this sequence. Keep the sequence number pending and request the proof;
	// treating this as delivery or a malformed contract would either lose data
	// or tombstone a recoverable receiver restart.
	if item.head && item.contractFrame == nil && item.contractId != nil {
		if _, ok := self.openReceiveContracts[*item.contractId]; !ok {
			self.sendContractMissing(
				item.sequenceNumber,
				item.messageId,
				*item.contractId,
				item.unwrapped,
				item.transportType,
			)
			return false, nil
		}
	}

	// this case happens when the receiver is reformed or loses state.
	// the sequence id guarantees the sender is the same for the sequence
	// past head items are retransmits. Future head items depend on previous ack,
	// which represent some state the sender has that the receiver is missing
	// advance the receiver state to the latest from the sender
	if item.head && self.nextSequenceNumber < item.sequenceNumber {
		if self.log.V(2).Enabled() {
			self.log.Infof("[r]seq= %d->%d %s<-%s s(%s)\n", self.nextSequenceNumber, item.sequenceNumber, self.client.ClientTag(), self.source.SourceId, self.source.StreamId)
		}
		self.nextSequenceNumber = item.sequenceNumber
		// the head must have a contract frame to reset the contract
	}

	if removedItem := self.receiveQueue.RemoveBySequenceNumber(sequenceNumber); removedItem != nil {
		self.peerAudit.Update(func(a *PeerAudit) {
			a.resend(removedItem.messageByteCount)
		})
		removedItem.messagePoolReturn()
	}

	// replace with the latest value (check both messageId and sequenceNumber)
	if removedItem := self.receiveQueue.RemoveByMessageId(messageId); removedItem != nil {
		self.peerAudit.Update(func(a *PeerAudit) {
			a.resend(removedItem.messageByteCount)
		})
		removedItem.messagePoolReturn()
	}

	if sequenceNumber <= self.nextSequenceNumber {
		if self.nextSequenceNumber == sequenceNumber {
			// this item is the head of sequence
			if self.log.V(2).Enabled() {
				self.log.Infof("[r]seq+ %d->%d %s<-%s s(%s)\n", self.nextSequenceNumber, self.nextSequenceNumber+1, self.client.ClientTag(), self.source.SourceId, self.source.StreamId)
			}
			self.nextSequenceNumber = self.nextSequenceNumber + 1

			if err := self.registerContracts(item); err != nil {
				self.log.Errorf("[r]%s<-%s s(%s) ack could not register contracts = %s\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId, err)
				return false, err
			}
			if self.updateContract(item) {
				self.receiveHead(item)
				return true, nil
			} else {
				// no valid contract. it should have been attached to the head
				self.log.Errorf("[r]drop queue head no contract %s<-%s s(%s): head=%t, contract=%t, rcontract=%t\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId, item.head, item.contractFrame != nil, self.receiveContract != nil)
				return false, errors.New("No contract")
			}
		} else {
			if self.log.V(1).Enabled() {
				self.log.Infof("[r]drop past sequence number %d <> %d ack=%t %s<-%s s(%s)\n", sequenceNumber, self.nextSequenceNumber, item.ack, self.client.ClientTag(), self.source.SourceId, self.source.StreamId)
			}
			// this item is a resend of a previous item
			if item.ack {
				self.sendAck(
					sequenceNumber,
					messageId,
					false,
					sequenceTag{},
					item.unwrapped,
					item.transportType,
				)
			}
			return false, nil
		}
	} else {
		// store only up to a max size in the receive queue.
		// an empty queue always admits at least one item (see CanAdd).
		canQueue := func(item *receiveItem) bool {
			return self.receiveQueue.CanAddWithQueueByteCount(
				item.MessageByteCount(),
				item.QueueByteCount(),
				self.receiveBufferSettings.ReceiveQueueMaxByteCount,
			)
		}

		// THROUGHPUTFIX §37.20: what a full hold does with an arrival that is
		// earlier than what it holds.
		//
		// Two things had to be true at once and the first two policies each
		// had one of them.
		//
		// Drainage. The only way a full hold empties is the head arriving and
		// draining the contiguous run behind it. A middle gap — earlier than
		// held items but not the head — must be held, so a hold that refuses
		// it never extends its run, and what it does hold is whatever arrived
		// first, an arbitrary set in sequence order. Evicting the latest held
		// item to admit an earlier one is monotone: the highest held sequence
		// number only falls and what replaces it is earlier, so the hold
		// converges on the items adjacent to the delivery point and the runs
		// are long. Measured at a 6.4 times overrun, four runs each: evicting
		// completed two of four with 25,227 loss events, refusing none of four
		// with 53,509.
		//
		// Truth. An evicted item had been acknowledged, and a selective
		// acknowledgement does not release the item at the sender: it leases
		// it, and every resend path skips a marked item except the
		// acknowledgement-tail probe, which fires twice and then leaves the
		// sixty second timeout. So eviction buys drainage with a lie.
		//
		// Committed-prefix acknowledgement has both. Keep the hold
		// sequence-earliest exactly as eviction does, and acknowledge a held
		// item only once it can no longer be evicted. Below that boundary an
		// item is acknowledged and never discarded; above it the item is held
		// tentatively, unacknowledged, and evictable with nothing withdrawn.
		// Against a sender honouring the advertisement every held item commits
		// at once and this is today's behaviour exactly; against one that does
		// not, the cost is that sender's resends of tentative items rather
		// than a lease.
		if policy := self.receiveBufferSettings.ReceiveHoldPolicy; policy != ReceiveHoldRefuse {
			for !canQueue(item) {
				lastItem := self.receiveQueue.PeekLast()
				if lastItem == nil ||
					lastItem.sequenceNumber <= receivePack.Pack.SequenceNumber {
					break
				}
				if policy == ReceiveHoldCommittedPrefix && lastItem.committed {
					// an acknowledged item is never discarded; the boundary is
					// supposed to make this unreachable, and if it is reached
					// the gap estimate under-counted
					break
				}
				self.receiveQueue.RemoveByMessageId(lastItem.messageId)
				if lastItem.committed {
					self.client.receiveQueueEvictionCount.Add(1)
					self.client.receiveQueueEvictionByteCount.Add(
						uint64(max(lastItem.MessageByteCount(), 0)))
					// the sender was told this item arrived; tell it that it
					// did not survive (THROUGHPUTFIX §37.16)
					self.noteEviction(lastItem.sequenceNumber)
				} else {
					self.client.receiveQueueTentativeEvictionCount.Add(1)
					self.client.receiveQueueTentativeEvictionByteCount.Add(
						uint64(max(lastItem.MessageByteCount(), 0)))
				}
				lastItem.messagePoolReturn()
			}
		}

		if canQueue(item) {
			// the largest frame seen is the conservative gap estimate:
			// over-counting commits fewer items and costs churn at the
			// boundary, under-counting commits an item that is later evicted,
			// which is the lie the policy exists to remove
			self.maxHeldByteCount = max(self.maxHeldByteCount, item.MessageByteCount())
			self.receiveQueue.Add(item)
			if self.receiveBufferSettings.ReceiveHoldPolicy == ReceiveHoldCommittedPrefix {
				self.commitHeldPrefix()
			} else {
				item.committed = true
				self.sendAck(
					sequenceNumber,
					messageId,
					true,
					item.tag,
					item.unwrapped,
					item.transportType,
				)
			}
			return true, nil
		} else {
			self.client.receiveQueueDropCount.Add(1)
			self.client.receiveQueueDropByteCount.Add(uint64(max(item.MessageByteCount(), 0)))
			if self.log.V(1).Enabled() {
				self.log.Infof("[r]drop ack cannot queue %s<-%s s(%s)\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId)
			}
			return false, nil
		}
	}
}

// commitHeldPrefix acknowledges every held item that can no longer be evicted,
// and only those (THROUGHPUTFIX §37.20).
//
// An item is evicted only by an earlier arrival at a full hold, so it is safe
// once every item missing below it could arrive and it would still fit.
// Sequence numbers are dense, one per Pack, so with the delivery point D and
// the held items in ascending order the number missing below the i-th is
// (seq_i - D) - i, and the item is committed when
//
//	missing_below(i) x maxFrameBytes + sum of held sizes through i <= capacity
//
// The boundary only rises as the head drains, and an item's acknowledgement is
// sent as it crosses. One second-order cost: a tentative item gives the sender
// no proving acknowledgement, so a gap just below the boundary recovers on the
// paced resend rather than on gap recovery. Bounded, and removed along with
// the rest by the receive advertisement.
func (self *ReceiveSequence) commitHeldPrefix() {
	capacity := self.receiveBufferSettings.ReceiveQueueMaxByteCount
	frameByteCount := max(self.maxHeldByteCount, 1)

	// The whole hold commits together whenever the newest held item would
	// survive every gap below it filling, which is the ordinary case: a sender
	// honouring the advertisement never gives the hold more than it can take,
	// and a hold well under capacity has room for its gaps whatever the
	// sender does. Deciding that needs only the newest held item, the count
	// and the byte total, all of which the queue answers without a walk, and
	// it skips the sort the general case pays on every out-of-order arrival.
	heldCount, heldTotal := self.receiveQueue.QueueSize()
	if heldCount == 0 {
		return
	}
	if newest := self.receiveQueue.PeekLast(); newest != nil {
		missing := int64(newest.sequenceNumber) -
			int64(self.nextSequenceNumber) - int64(heldCount-1)
		if missing < 0 {
			missing = 0
		}
		if ByteCount(missing)*frameByteCount+heldTotal <= capacity {
			self.heldScratch = self.receiveQueue.UnorderedItems(self.heldScratch)
			for _, held := range self.heldScratch {
				if held.committed {
					continue
				}
				self.commitHeldItem(held)
			}
			return
		}
	}

	self.heldScratch = self.receiveQueue.AscendingItems(self.heldScratch)
	heldByteCount := ByteCount(0)
	for i, held := range self.heldScratch {
		heldByteCount += held.MessageByteCount()
		missing := int64(held.sequenceNumber) - int64(self.nextSequenceNumber) - int64(i)
		if missing < 0 {
			missing = 0
		}
		if capacity < ByteCount(missing)*frameByteCount+heldByteCount {
			break
		}
		if held.committed {
			continue
		}
		self.commitHeldItem(held)
	}
}

// Acknowledges one held item as it crosses the boundary.
func (self *ReceiveSequence) commitHeldItem(held *receiveItem) {
	held.committed = true
	self.client.receiveQueueCommitCount.Add(1)
	self.sendAck(
		held.sequenceNumber,
		held.messageId,
		true,
		held.tag,
		held.unwrapped,
		held.transportType,
	)
}

func (self *ReceiveSequence) receiveNack(receivePack *ReceivePack) (bool, error) {

	receiveTime := time.Now()

	sequenceNumber := receivePack.Pack.SequenceNumber
	// var contractId *Id
	// if self.receiveContract != nil {
	// 	contractId = &self.receiveContract.contractId
	// }
	messageId, err := IdFromBytes(receivePack.Pack.MessageId)
	if err != nil {
		return false, errors.New("Bad message_id")
	}

	contractId, err := receivePackContractId(receivePack.Pack)
	if err != nil {
		return false, err
	}

	if contractId == nil && !self.receiveBufferSettings.AllowLegacyNack {
		self.log.Infof("[r]drop nack required contract id %s<-%s s(%s)\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId)
		return false, nil
	}

	var item *receiveItem
	if receivePack.decodedOwner != nil {
		item = &receivePack.decodedOwner.receiveItem
	} else {
		item = &receiveItem{}
	}
	queueByteCount := receivePack.MessageByteCount
	if self.receiveBufferSettings.ReceiveQueueRetainedByteAccounting {
		queueByteCount = receivePack.receiveQueueByteCount()
	}
	*item = receiveItem{
		transferItem: transferItem{
			messageId:        messageId,
			sequenceNumber:   sequenceNumber,
			messageByteCount: receivePack.MessageByteCount,
			queueByteCount:   queueByteCount,
		},
		contractId:         contractId,
		receiveTime:        receiveTime,
		frames:             receivePack.Pack.Frames,
		contractFrame:      receivePack.Pack.ContractFrame,
		receiveCallback:    receivePack.ReceiveCallback,
		head:               receivePack.Pack.Head,
		ack:                !receivePack.Pack.Nack,
		tag:                sequenceTagFromProtocol(receivePack.Pack.Tag),
		decodedOwner:       receivePack.decodedOwner,
		transferFrameBytes: receivePack.TransferFrameBytes,
		transportType:      receivePack.TransportType,
		// nack items send no ack, so `unwrapped` was historically unused here;
		// the EncryptionModeRequired receive gate in `receiveHead` now reads it
		// to refuse plaintext application frames on this path too
		unwrapped: receivePack.Unwrapped,
	}

	if err := self.registerContracts(item); err != nil {
		self.log.Errorf("[r]%s<-%s s(%s) nack could not register contracts = %s\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId, err)
		return false, err
	}

	if contractId != nil {
		if _, ok := self.openReceiveContracts[*contractId]; !ok {
			self.log.Infof("[r]drop nack contract mismatch %s<-%s s(%s)\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId)
			return false, nil
		}
	}

	if self.updateContract(item) {
		self.receiveHead(item)
		return true, nil
	} else {
		// no valid contract
		// drop the message. since this is a nack it will not block the sequence
		self.log.Infof("[r]drop nack no contract %s<-%s s(%s)\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId)
		return false, nil
	}
}

// Extracts a platform-authenticated local stream without changing the
// end-to-end callback source. A mismatched contract cannot describe the
// observed relationship.
func receiveContractStreamId(
	source TransferPath,
	clientId Id,
	receiveContract *sequenceContract,
) Id {
	if receiveContract == nil ||
		receiveContract.path.StreamId == (Id{}) ||
		receiveContract.path.SourceId != source.SourceId ||
		receiveContract.path.DestinationId != clientId {
		return Id{}
	}
	return receiveContract.path.StreamId
}

func (self *ReceiveSequence) receiveHead(item *receiveItem) {
	if self.log.V(1).Enabled() {
		frameMessageTypes := []string{}
		for _, frame := range item.frames {
			frameMessageTypes = append(frameMessageTypes, fmt.Sprintf("%v", frame.MessageType))
		}
		frameMessageTypesStr := strings.Join(frameMessageTypes, ", ")
		if item.ack {
			self.log.Infof("[r]head %d (%s) %s<-%s s(%s)\n", item.sequenceNumber, frameMessageTypesStr, self.client.ClientTag(), self.source.SourceId, self.source.StreamId)
		} else {
			self.log.Infof("[r]head nack (%s) %s<-%s s(%s)\n", frameMessageTypesStr, self.client.ClientTag(), self.source.SourceId, self.source.StreamId)
		}
	}
	self.peerAudit.Update(func(a *PeerAudit) {
		a.received(item.messageByteCount)
	})
	var peer Peer

	if item.contractId != nil {
		receiveContract := self.openReceiveContracts[*item.contractId]
		receiveContract.ack(item.messageByteCount)
		peer = Peer{
			ProvideMode:   receiveContract.provideMode,
			Roles:         receiveContract.roles,
			Principal:     receiveContract.principal,
			TransferKey:   self.transferKey,
			TransportType: item.transportType,
		}
	} else {
		// no contract peers are considered in network
		peer = Peer{
			ProvideMode:   protocol.ProvideMode_Network,
			TransferKey:   self.transferKey,
			TransportType: item.transportType,
		}
	}
	// EncryptedControl frames are routed into the per-peer session instead
	// of bubbling up to the receive callback. They carry the TLS handshake
	// bytes that bootstrap the per-peer cipher; the application shouldn't
	// see them.
	appFrames := item.frames
	if self.session != nil {
		appFrames = self.deliverEncryptedControlFrames(item.frames)
		if item.unwrapped && 0 < len(appFrames) && self.session.RequireEncryption() {
			// Fail-closed receive gate (EncryptionModeRequired): a plaintext
			// application frame from a peer for which a session is expected is
			// never delivered to the application — closing the downgrade where
			// a peer or on-path attacker strips the wrap and the receiver
			// accepts the plaintext. The item still advances the sequence and
			// is acked (ack-and-discard): withholding the ack would gap the
			// strictly-ordered sequence and starve handshake controls queued
			// behind the gap, wedging both sides. The handshake controls
			// themselves were already routed to the session above; the peer
			// audit records the policy violation.
			if self.log.V(1).Enabled() {
				self.log.Infof(
					"[r]%s<-%s s(%s) discarded %d plaintext application frame(s) (encryption required)\n",
					self.client.ClientTag(), self.source.SourceId, self.source.StreamId, len(appFrames),
				)
			}
			self.peerAudit.Update(func(a *PeerAudit) {
				a.badMessage(item.messageByteCount)
			})
			self.session.NotifyRequiredReceiveDiscarded(
				"plaintext application frames discarded",
			)
			appFrames = nil
		}
	}

	// buffer for a combined dispatch: a Peer identity change (contract
	// rotation) is a batch boundary because one callback carries one peer
	peerEqual := func(a Peer, b Peer) bool {
		return a.ProvideMode == b.ProvideMode &&
			a.Principal == b.Principal &&
			a.TransferKey == b.TransferKey &&
			a.TransportType == b.TransportType &&
			slices.Equal(a.Roles, b.Roles)
	}
	if 0 < len(self.deliverItems) && !peerEqual(peer, self.deliverPeer) {
		self.flushDeliver()
	}
	self.deliverPeer = peer
	self.deliverItems = append(self.deliverItems, item)
	self.deliverFrames = append(self.deliverFrames, appFrames...)
	if receiveDeliverBatchMaxFrames <= len(self.deliverFrames) {
		self.flushDeliver()
	}
}

// flushDeliver dispatches the buffered head items' app frames in one receive
// callback, then sends their acks (deliver-before-ack, as the per-item path
// did) and returns their pool buffers. The batch is taken out of the sequence
// fields BEFORE the callback runs. Client.receive isolates each registered
// application callback panic; this guard covers a failure in that internal
// dispatcher itself. Such a failure loses the un-acked batch so the sender can
// resend it, instead of letting the exit-path flush re-deliver a partially
// dispatched batch or ack frames whose dispatch failed.
func (self *ReceiveSequence) flushDeliver() {
	if len(self.deliverItems) == 0 {
		return
	}
	items := slices.Clone(self.deliverItems)
	frames := slices.Clone(self.deliverFrames)
	peer := self.deliverPeer
	clear(self.deliverItems)
	self.deliverItems = self.deliverItems[:0]
	clear(self.deliverFrames)
	self.deliverFrames = self.deliverFrames[:0]

	// pool buffers return exactly once even when the callback panics
	defer func() {
		for _, item := range items {
			item.messagePoolReturn()
		}
	}()

	if 0 < len(frames) {
		// all items of one sequence share the client's receive callback
		items[0].receiveCallback(
			self.source,
			frames,
			peer,
		)
	}
	for _, item := range items {
		if item.ack {
			self.sendAck(
				item.sequenceNumber,
				item.messageId,
				false,
				item.tag,
				item.unwrapped,
				item.transportType,
			)
		}
	}
}

// deliverEncryptedControlFrames splits an incoming Pack's frames: any
// `TransferEncryptedControl` frames are decoded and routed into the per-peer
// session of the complement of the sender's role (a client-role control —
// the peer's ClientHello — drives our server session, and vice versa),
// creating that session if needed. The remaining application frames are
// returned for delivery to the receive callback.
func (self *ReceiveSequence) deliverEncryptedControlFrames(frames []*protocol.Frame) []*protocol.Frame {
	var passthrough []*protocol.Frame
	for _, frame := range frames {
		if frame == nil {
			continue
		}
		if frame.MessageType != protocol.MessageType_TransferEncryptedControl {
			passthrough = append(passthrough, frame)
			continue
		}
		if self.client == nil || self.client.encryptionSessionManager == nil {
			continue
		}
		ec := &protocol.EncryptedControl{}
		if err := ProtoUnmarshal(frame.MessageBytes, ec); err != nil {
			if self.log.V(1).Enabled() {
				self.log.Infof("[r]%s<-%s bad encrypted control = %s\n", self.client.ClientTag(), self.source.SourceId, err)
			}
			continue
		}
		senderRole, ok := sequenceTlsRoleFromProtobuf(ec.SessionRole)
		if !ok {
			if self.log.V(1).Enabled() {
				self.log.Infof("[r]%s<-%s encrypted control with no session role — dropped\n", self.client.ClientTag(), self.source.SourceId)
			}
			continue
		}
		self.client.encryptionSessionManager.DeliverEncryptedControl(self.source.SourceId, senderRole.complement(), ec)
	}
	return passthrough
}

func (self *ReceiveSequence) registerContracts(item *receiveItem) error {
	if item.contractFrame == nil {
		return nil
	}

	var contract protocol.Contract
	err := ProtoUnmarshal(item.contractFrame.MessageBytes, &contract)
	if err != nil {
		self.rejectRetransmits = true
		// bad message
		// close sequence
		self.peerAudit.Update(func(a *PeerAudit) {
			a.badMessage(item.messageByteCount)
		})
		return err
	}

	// check the hmac with the local provider secret key
	if !self.client.ContractManager().Verify(
		contract.StoredContractHmac,
		contract.StoredContractBytes,
		contract.ProvideMode) {
		self.rejectRetransmits = true
		self.log.Errorf("[r]%s<-%s s(%s) exit contract verification failed (%s)\n", self.client.ClientTag(), self.source.SourceId, self.source.StreamId, contract.ProvideMode)
		// Close only a stream authenticated by the last verified contract. The
		// callback source is a local mask, and the failing bytes are untrusted;
		// neither may name a stream to tear down.
		verifiedStreamId := receiveContractStreamId(
			self.source,
			self.client.ClientId(),
			self.receiveContract,
		)
		if self.client.streamManager != nil && verifiedStreamId != (Id{}) {
			self.client.streamManager.streamBuffer.CloseStream(verifiedStreamId)
		}
		// bad contract
		// close sequence
		self.peerAudit.Update(func(a *PeerAudit) {
			a.badContract()
		})
		return errors.New("Contract verification failed.")
	}

	nextReceiveContract, err := newSequenceContract(
		self.log,
		"r",
		&contract,
		self.receiveBufferSettings.MinMessageByteCount,
		1.0,
	)
	if err != nil {
		self.rejectRetransmits = true
		// bad contract
		// close sequence
		self.peerAudit.Update(func(a *PeerAudit) {
			a.badContract()
		})
		return err
	}
	if nextReceiveContract.path.SourceId != self.source.SourceId ||
		nextReceiveContract.path.DestinationId != self.client.ClientId() {
		self.rejectRetransmits = true
		self.peerAudit.Update(func(a *PeerAudit) {
			a.badContract()
		})
		return errors.New("Contract path does not match receive path.")
	}

	if err := self.setContract(nextReceiveContract); err != nil {
		self.rejectRetransmits = true
		// the next contract has already been used
		// bad contract
		// close sequence
		self.peerAudit.Update(func(a *PeerAudit) {
			a.badContract()
		})
		return err
	}
	verifiedStreamId := receiveContractStreamId(
		self.source,
		self.client.ClientId(),
		self.receiveContract,
	)
	self.peerAudit.SetStreamId(verifiedStreamId)
	if self.client.streamManager != nil && verifiedStreamId != (Id{}) {
		self.client.streamManager.authenticateStreamDestination(
			verifiedStreamId,
			self.source.SourceId,
		)
	}

	// This contract verified against this client's own Network provider
	// secret key, so it authenticates the sender as a same-network endpoint.
	// The platform's peer list can never name this id — it belongs to an
	// ephemeral window client, and only top-level clients are registered as
	// peers — so this is the sole authenticated witness that lets provider
	// stream policy admit the peer's P2P stream. Reported here rather than
	// from `receiveHead`, which labels no-contract receives Network without
	// any such proof.
	if contract.ProvideMode == protocol.ProvideMode_Network &&
		self.client.streamManager != nil {
		self.client.streamManager.NetworkPeerWindowClientAuthenticated(self.source.SourceId)
	}

	return nil
}

func (self *ReceiveSequence) setContract(nextReceiveContract *sequenceContract) error {
	// contract already set
	if self.receiveContract != nil && self.receiveContract.contractId == nextReceiveContract.contractId {
		return nil
	}

	if receiveContract, ok := self.openReceiveContracts[nextReceiveContract.contractId]; ok {
		// switch to the current contract
		self.receiveContract = receiveContract
		self.updateContractWriterAlias()
		return nil
	}

	// a genuinely new (typically larger) contract supersedes the current one.
	// Close the superseded contract's STATS now so it stops showing as an open
	// contract, mirroring the send side, which closes a drained predecessor in
	// `ackItem`. This is a stats-only close, NOT a wire-level CloseContract: the
	// superseded contract stays in `openReceiveContracts` for the sender's
	// resend/reorder window (see MaxOpenReceiveContract), and the overflow trim
	// below does the real close once the buffer fills. Without this, up to
	// MaxOpenReceiveContract exhausted receive contracts linger open forever
	// under continuous traffic (the sequence never ends and the trim never
	// triggers at <= the buffer size), accumulating in the UI.
	superseded := self.receiveContract

	self.openReceiveContracts[nextReceiveContract.contractId] = nextReceiveContract
	self.receiveContract = nextReceiveContract
	self.updateContractWriterAlias()
	// the receive side does not know companion-ness (the wire contract does not
	// carry it). listeners pair contracts to companions with the peer client id
	nextReceiveContract.statsEntry = self.client.ContractManager().registerContractStats(
		nextReceiveContract.contractId,
		true,
		false,
		nextReceiveContract.path,
		nextReceiveContract.transferByteCount,
	)

	if superseded != nil {
		self.client.ContractManager().closeContractStats(superseded.contractId)
	}

	if d := len(self.openReceiveContracts) - self.receiveBufferSettings.MaxOpenReceiveContract; 0 < d {
		// remove the least recently added
		orderedReceiveContracts := slices.Collect(maps.Values(self.openReceiveContracts))
		// ascending where earliest created are first
		slices.SortFunc(orderedReceiveContracts, func(a *sequenceContract, b *sequenceContract) int {
			return a.localId.Cmp(b.localId)
		})
		for _, receiveContract := range orderedReceiveContracts[:d] {
			if receiveContract != self.receiveContract {
				self.client.ContractManager().CloseContract(
					receiveContract.contractId,
					receiveContract.ackedByteCount,
					receiveContract.unackedByteCount,
				)
				delete(self.openReceiveContracts, receiveContract.contractId)
			}
		}
	}

	return nil
}

// Tracks the verified stream that can carry destination-only ACK traffic back
// to the final source. Contract rotation replaces the shared reference.
func (self *ReceiveSequence) updateContractWriterAlias() {
	streamId := receiveContractStreamId(
		self.source,
		self.client.ClientId(),
		self.receiveContract,
	)
	alias := TransferPath{}
	if streamId != (Id{}) {
		alias = StreamId(streamId)
	}
	if self.contractWriterAlias == alias {
		return
	}
	self.closeContractWriterAlias()
	if alias != (TransferPath{}) && self.source.SourceId != ControlId {
		self.removeContractWriterAlias = self.client.RouteManager().AddWriterDestinationAlias(
			DestinationId(self.source.SourceId),
			alias,
		)
		self.contractWriterAlias = alias
	}
}

// Releases the shared ACK route after the final ACK worker has drained.
func (self *ReceiveSequence) closeContractWriterAlias() {
	if self.removeContractWriterAlias != nil {
		self.removeContractWriterAlias()
		self.removeContractWriterAlias = nil
	}
	self.contractWriterAlias = TransferPath{}
}

func (self *ReceiveSequence) updateContract(item *receiveItem) bool {
	// always use a contract if present
	// the sender may send contracts even if `receiveNoContract` is set locally
	if item.contractId != nil {
		if receiveContract, ok := self.openReceiveContracts[*item.contractId]; ok && receiveContract.update(item.messageByteCount) {
			return true
		}
	} else if self.receiveContract != nil && self.receiveContract.update(item.messageByteCount) {
		item.contractId = &self.receiveContract.contractId
		return true
	}
	// `receiveNoContract` is a mutual configuration
	// both sides must configure themselves to require no contract from each other
	if self.client.ContractManager().ReceiveNoContract(self.source.SourceId) {
		return true
	}
	return false
}

func (self *ReceiveSequence) Close() {
	self.cancel()

	func() {
		self.packMutex.Lock()
		defer self.packMutex.Unlock()
		if self.packTimer != nil {
			self.packTimer.Stop()
		}
		close(self.packs)
	}()

	// drain the channel
	func() {
		for {
			select {
			case receivePack, ok := <-self.packs:
				if !ok {
					return
				}
				self.releasePackQueue(receivePack)
				receivePack.messagePoolReturn()
			default:
				return
			}
		}
	}()
}

func (self *ReceiveSequence) Cancel() {
	self.cancel()
}

func (self *ReceiveSequence) WaitForExit() {
	select {
	case <-self.exit:
	}
}

type receiveItem struct {
	transferItem

	contractId         *Id
	head               bool
	receiveTime        time.Time
	frames             []*protocol.Frame
	contractFrame      *protocol.Frame
	receiveCallback    ReceiveFunction
	ack                bool
	tag                sequenceTag
	decodedOwner       *decodedPackOwner
	transferFrameBytes []byte
	transportType      TransportType
	// committed is set once this held item has been selectively acknowledged,
	// which under the committed-prefix policy happens only when it can no
	// longer be evicted (THROUGHPUTFIX §37.20). A committed item is never
	// discarded.
	committed bool
	// unwrapped is true when the originating TransferFrame arrived on
	// the wire as plaintext (no outer encrypted wrap). Propagated into
	// the sequenceAck so the ack format mirrors the incoming pack.
	unwrapped bool
}

func (self *receiveItem) messagePoolReturn() {
	// Like ReceivePack, the owned hot-path item is embedded in decodedOwner.
	// Return the independent outer frame before releasing the owner so this
	// method never touches storage that may already have been reused.
	transferFrameBytes := self.transferFrameBytes
	self.transferFrameBytes = nil
	if self.decodedOwner != nil {
		owner := self.decodedOwner
		self.decodedOwner = nil
		MessagePoolReturn(transferFrameBytes)
		owner.release()
		return
	}
	for _, frame := range self.frames {
		returnDecodedFrameMessageBytes(frame)
	}
	returnDecodedFrameMessageBytes(self.contractFrame)
	self.frames = nil
	self.contractFrame = nil
	MessagePoolReturn(transferFrameBytes)
}

// ordered by sequenceNumber
type receiveQueue = transferQueue[*receiveItem]

func newReceiveQueue(budget *TransferMemoryBudget, minByteCount ByteCount) *receiveQueue {
	queue := newTransferQueue[*receiveItem](func(a *receiveItem, b *receiveItem) int {
		if a.sequenceNumber < b.sequenceNumber {
			return -1
		} else if b.sequenceNumber < a.sequenceNumber {
			return 1
		} else {
			return 0
		}
	})
	queue.setBudget(budget, minByteCount)
	return queue
}

type sequenceTag struct {
	sendTime uint64
	set      bool
}

func sequenceTagFromProtocol(tag *protocol.Tag) sequenceTag {
	if tag == nil {
		return sequenceTag{}
	}
	// Retain only the wire value. A generated protobuf message contains
	// protoimpl.MessageState (including synchronization state) and must not be
	// copied through ACK-window values/snapshots. Tag currently has one field,
	// so this is also the minimal, lifetime-independent representation.
	return sequenceTag{sendTime: tag.SendTime, set: true}
}

func (tag *sequenceTag) protocol() *protocol.Tag {
	if tag == nil || !tag.set {
		return nil
	}
	return &protocol.Tag{SendTime: tag.sendTime}
}

type sequenceAck struct {
	sequenceNumber uint64
	messageId      Id
	selective      bool
	tag            sequenceTag
	// transportType is the carrier that delivered the packet covered by this
	// ACK. The receiver uses it only as reply affinity; it is not serialized.
	transportType                    TransportType
	contractMissing                  bool
	missingContractId                Id
	compactContractRecoverySupported bool
	// unwrapped is true when any pack covered by this ack arrived on
	// the wire as plaintext. The ack writer mirrors that state — a
	// plaintext-acked window emits a plaintext ack — so peers whose
	// ciphers haven't been established yet can read the ack. Cumulative
	// head acks or-in the bit across every absorbed lower ack.
	unwrapped bool
}

type sequenceAckWindowSnapshot struct {
	ackNotify           <-chan struct{}
	headAck             sequenceAck
	ackUpdateCount      int
	selectiveAcks       map[Id]sequenceAck
	contractMissingAcks map[Id]sequenceAck
}

type sequenceAckWindow struct {
	// There is exactly one Snapshot consumer per receive sequence. A
	// capacity-one signal coalesces any number of updates while that consumer
	// is running and avoids allocating/closing a broadcast channel per packet.
	ackNotify      chan struct{}
	ackLock        sync.Mutex
	headAck        sequenceAck
	hasHeadAck     bool
	ackUpdateCount int
	selectiveAcks  map[Id]sequenceAck
	// Recovery requests never acknowledge delivery and therefore remain
	// separate from both cumulative and selective acknowledgement windows.
	contractMissingAcks map[Id]sequenceAck
}

func newSequenceAckWindow() *sequenceAckWindow {
	return &sequenceAckWindow{
		ackNotify:           make(chan struct{}, 1),
		ackUpdateCount:      0,
		selectiveAcks:       map[Id]sequenceAck{},
		contractMissingAcks: map[Id]sequenceAck{},
	}
}

// Notify is the stable coalesced edge consumed by the one sequence worker.
// It is safe to fetch without a lock because the channel never changes.
func (self *sequenceAckWindow) Notify() <-chan struct{} {
	return self.ackNotify
}

// Pending checks whether a worker can proceed without constructing a
// snapshot. In particular, the ACK-compression worker uses this before its
// wait so a large selective-ACK map is copied only once, when the worker
// actually drains it after the compression interval.
func (self *sequenceAckWindow) Pending() bool {
	self.ackLock.Lock()
	defer self.ackLock.Unlock()
	return 0 < self.ackUpdateCount ||
		0 < len(self.selectiveAcks) ||
		0 < len(self.contractMissingAcks)
}

// PendingDispositionFor reports whether the not-yet-snapshotted window can
// retire or materially rewrite one exact due item. Unrelated ACK progress must
// not postpone its recovery: on a busy sequence, duplicate/newer selective
// ACKs can otherwise keep Pending true indefinitely while the actual hole is
// never retransmitted.
func (self *sequenceAckWindow) PendingDispositionFor(
	sequenceNumber uint64,
	messageId Id,
) bool {
	self.ackLock.Lock()
	defer self.ackLock.Unlock()
	if 0 < self.ackUpdateCount && self.hasHeadAck &&
		sequenceNumber <= self.headAck.sequenceNumber {
		return true
	}
	if ack, ok := self.selectiveAcks[messageId]; ok &&
		ack.sequenceNumber == sequenceNumber {
		return true
	}
	_, contractMissing := self.contractMissingAcks[messageId]
	return contractMissing
}

func (self *sequenceAckWindow) UpdateContractMissing(ack sequenceAck) {
	self.ackLock.Lock()
	defer self.ackLock.Unlock()
	if prior, ok := self.contractMissingAcks[ack.messageId]; ok {
		if prior.unwrapped {
			ack.unwrapped = true
		}
		if prior.compactContractRecoverySupported {
			ack.compactContractRecoverySupported = true
		}
		if ack.transportType == TransportTypeUnknown {
			ack.transportType = prior.transportType
		}
	}
	self.contractMissingAcks[ack.messageId] = ack
	select {
	case self.ackNotify <- struct{}{}:
	default:
	}
}

func (self *sequenceAckWindow) Update(ack sequenceAck) {
	self.ackLock.Lock()
	defer self.ackLock.Unlock()

	if !self.hasHeadAck || self.headAck.sequenceNumber < ack.sequenceNumber {
		if ack.selective {
			if prior, ok := self.selectiveAcks[ack.messageId]; ok {
				if prior.unwrapped {
					// Coalesced selective Ack for the same message preserves any
					// prior plaintext bit so one late wrapped resend cannot upgrade
					// the Ack format past the sender's reach.
					ack.unwrapped = true
				}
				if prior.compactContractRecoverySupported {
					ack.compactContractRecoverySupported = true
				}
				if ack.transportType == TransportTypeUnknown {
					ack.transportType = prior.transportType
				}
			}
			self.selectiveAcks[ack.messageId] = ack
		} else {
			// cumulative head ack: or-in the prior head's plaintext bit
			// (and any absorbed selective acks below the new head) so a
			// single plaintext pack anywhere under the head keeps the
			// ack plaintext. Selective acks at or below the new head are
			// already dropped by the Snapshot pass.
			if self.hasHeadAck && self.headAck.unwrapped {
				ack.unwrapped = true
			}
			if self.hasHeadAck && self.headAck.compactContractRecoverySupported {
				ack.compactContractRecoverySupported = true
			}
			if self.hasHeadAck && ack.transportType == TransportTypeUnknown {
				ack.transportType = self.headAck.transportType
			}
			if !ack.unwrapped {
				for _, sel := range self.selectiveAcks {
					if sel.unwrapped && sel.sequenceNumber <= ack.sequenceNumber {
						ack.unwrapped = true
						break
					}
				}
			}
			if !ack.compactContractRecoverySupported {
				for _, selectiveAck := range self.selectiveAcks {
					if selectiveAck.compactContractRecoverySupported &&
						selectiveAck.sequenceNumber <= ack.sequenceNumber {
						ack.compactContractRecoverySupported = true
						break
					}
				}
			}
			self.ackUpdateCount += 1
			self.headAck = ack
			self.hasHeadAck = true
			// no need to clean up `selectiveAcks` here
			// selective acks with sequence number <= head are ignored in a final pass during update
		}
	} else {
		// past the head
		// resend the head — fold this late ack's plaintext bit into the
		// head so the resend covers it. Snapshots copy the value, so the
		// internal value can be updated under ackLock without a published
		// pointer or copy-on-write allocation.
		if ack.unwrapped && self.hasHeadAck && !self.headAck.unwrapped {
			self.headAck.unwrapped = true
		}
		if ack.compactContractRecoverySupported && self.hasHeadAck &&
			!self.headAck.compactContractRecoverySupported {
			self.headAck.compactContractRecoverySupported = true
		}
		self.ackUpdateCount += 1
	}

	select {
	case self.ackNotify <- struct{}{}:
	default:
	}
}

// Snapshot is returned by value: it is consumed immediately by the caller and
// never retained, so a heap allocation per snapshot is pure waste. The caller
// always receives a copy of (or nil for) the selective acks, never the live
// map, so the live map can be cleared and reused on reset.
func (self *sequenceAckWindow) Snapshot(reset bool) sequenceAckWindowSnapshot {
	self.ackLock.Lock()
	defer self.ackLock.Unlock()

	// build the selective-ack copy lazily so the common in-order case (a
	// cumulative head ack with no selective acks) allocates no map.
	var selectiveAcksAfterHead map[Id]sequenceAck
	if 0 < self.ackUpdateCount {
		for messageId, ack := range self.selectiveAcks {
			if self.headAck.sequenceNumber < ack.sequenceNumber {
				if selectiveAcksAfterHead == nil {
					selectiveAcksAfterHead = map[Id]sequenceAck{}
				}
				selectiveAcksAfterHead[messageId] = ack
			}
		}
	} else if 0 < len(self.selectiveAcks) {
		selectiveAcksAfterHead = maps.Clone(self.selectiveAcks)
	}

	var contractMissingAcks map[Id]sequenceAck
	if 0 < len(self.contractMissingAcks) {
		contractMissingAcks = maps.Clone(self.contractMissingAcks)
	}

	snapshot := sequenceAckWindowSnapshot{
		ackNotify:           self.ackNotify,
		headAck:             self.headAck,
		ackUpdateCount:      self.ackUpdateCount,
		selectiveAcks:       selectiveAcksAfterHead,
		contractMissingAcks: contractMissingAcks,
	}

	if reset {
		// keep the head ack in place. clear() reuses the live map's storage
		// instead of allocating a fresh map; the caller holds only a copy.
		self.ackUpdateCount = 0
		clear(self.selectiveAcks)
		clear(self.contractMissingAcks)
		// The signal corresponds to state included in this snapshot. Drain it
		// while ackLock excludes Update so the next empty snapshot cannot wake
		// on a stale token.
		select {
		case <-self.ackNotify:
		default:
		}
	}

	return snapshot
}

type sequenceContract struct {
	log                        Logger
	localId                    Id
	tag                        string
	contract                   *protocol.Contract
	contractId                 Id
	transferByteCount          ByteCount
	effectiveTransferByteCount ByteCount
	provideMode                protocol.ProvideMode

	minUpdateByteCount ByteCount

	path TransferPath

	ackedByteCount   ByteCount
	unackedByteCount ByteCount

	// when set, the sequence stores the used byte count here on each debit,
	// so ongoing contract usage can be reported to stats listeners
	// (see transfer_contract_stats.go)
	statsEntry *contractStatsEntry

	// Set on the send side only after a delivery Ack covers this contract's
	// complete proof and advertises explicit missing-state recovery.
	compactContractRecoverySupported bool

	// provideTlsCertificate is the PEM-encoded X.509 chain (leaf first)
	// that the destination committed to as its server TLS identity for
	// this contract. Empty when the destination did not publish a
	// certificate via `ContractManager.SetProvideTlsCertificate`. The
	// SendSequence uses this to verify the peer presented during the
	// per-peer TLS handshake against the platform-signed contract.
	provideTlsCertificate [][]byte
	// destinationClientPublicKey is the peer's 32-byte Ed25519
	// long-lived public identity key, as committed by the platform in
	// `Contract.destination_client_public_key`. The sender uses it to
	// (a) verify `destinationClientKeySignedTlsCertificate` against
	// `provideTlsCertificate` — only then is the cert chain admitted
	// to the per-peer session's trusted set — and (b) verify the
	// peer's post-handshake identity proof exchanged inside the per-
	// peer TLS session. Empty when the contract carries no key.
	destinationClientPublicKey []byte
	// destinationClientKeySignedTlsCertificate is the peer's Ed25519
	// signature over the canonical concatenation of every PEM block
	// in `provideTlsCertificate`. The signing key is the peer's
	// long-lived client identity key (private half held only by the
	// peer); the verifier is `destinationClientPublicKey`. Empty when
	// the contract carries no signature.
	destinationClientKeySignedTlsCertificate []byte

	// roles and principal are the source client's identity, sealed into the
	// platform-signed contract bytes. Honored only when the provide mode is
	// network; nil/empty for all other provide modes.
	roles     []string
	principal string
}

func newSequenceContract(log Logger, tag string, contract *protocol.Contract, minUpdateByteCount ByteCount, contractFillFraction float32) (*sequenceContract, error) {
	storedContract := &protocol.StoredContract{}
	err := ProtoUnmarshal(contract.StoredContractBytes, storedContract)
	if err != nil {
		return nil, err
	}

	contractId, err := IdFromBytes(storedContract.ContractId)
	if err != nil {
		return nil, err
	}

	path, err := TransferPathFromBytes(
		storedContract.SourceId,
		storedContract.DestinationId,
		storedContract.StreamId,
	)
	if err != nil {
		return nil, err
	}

	// The platform-signed `StoredContract.ProvideTlsCertificate` is the
	// authoritative cert commitment (signed under `storedContractHmac`); the
	// outer `Contract.ProvideTlsCertificate` is a convenience copy for
	// clients that don't unmarshal the stored bytes. Prefer the stored value;
	// fall back to the outer value only when the inner is missing.
	provideTlsCertificate := storedContract.ProvideTlsCertificate
	if len(provideTlsCertificate) == 0 && contract != nil {
		provideTlsCertificate = contract.ProvideTlsCertificate
	}

	// Same prefer-stored-fallback-to-outer convention for the destination's
	// client-identity public key and the destination's signature over the
	// cert chain (Option 1 of the long-lived-identity verification design).
	destinationClientPublicKey := storedContract.DestinationClientPublicKey
	if len(destinationClientPublicKey) == 0 && contract != nil {
		destinationClientPublicKey = contract.DestinationClientPublicKey
	}
	destinationClientKeySignedTlsCertificate := storedContract.DestinationClientKeySignedTlsCertificate
	if len(destinationClientKeySignedTlsCertificate) == 0 && contract != nil {
		destinationClientKeySignedTlsCertificate = contract.DestinationClientKeySignedTlsCertificate
	}

	// roles/principal live only in the signed stored bytes (no outer copy)
	// and apply only to network provide mode
	var roles []string
	var principal string
	if contract.ProvideMode == protocol.ProvideMode_Network {
		roles = storedContract.Roles
		principal = storedContract.Principal
	}

	return &sequenceContract{
		log:                                      log,
		localId:                                  NewId(),
		tag:                                      tag,
		contract:                                 contract,
		contractId:                               contractId,
		transferByteCount:                        ByteCount(storedContract.TransferByteCount),
		effectiveTransferByteCount:               ByteCount(float32(storedContract.TransferByteCount) * contractFillFraction),
		provideMode:                              contract.ProvideMode,
		minUpdateByteCount:                       minUpdateByteCount,
		path:                                     path,
		ackedByteCount:                           ByteCount(0),
		unackedByteCount:                         ByteCount(0),
		provideTlsCertificate:                    provideTlsCertificate,
		destinationClientPublicKey:               destinationClientPublicKey,
		destinationClientKeySignedTlsCertificate: destinationClientKeySignedTlsCertificate,
		roles:                                    roles,
		principal:                                principal,
	}, nil
}

func (self *sequenceContract) update(byteCount ByteCount) bool {
	effectiveByteCount := max(self.minUpdateByteCount, byteCount)

	if !self.canUpdate(byteCount) {
		// doesn't fit in contract
		// if self.log.V(1).Enabled() {
		self.log.Infof(
			"[%s]debit contract %s failed +%d->%d (%d/%d total %.1f%% full)\n",
			self.tag,
			self.contractId,
			effectiveByteCount,
			self.ackedByteCount+self.unackedByteCount+effectiveByteCount,
			self.ackedByteCount+self.unackedByteCount,
			self.effectiveTransferByteCount,
			100.0*float32(self.ackedByteCount+self.unackedByteCount)/float32(self.effectiveTransferByteCount),
		)
		// }
		return false
	}
	self.unackedByteCount += effectiveByteCount
	if self.statsEntry != nil {
		self.statsEntry.updateUsedByteCount(self.ackedByteCount + self.unackedByteCount)
	}
	if self.log.V(1).Enabled() {
		self.log.Infof(
			"[%s]debit contract %s passed +%d->%d (%d/%d total %.1f%% full)\n",
			self.tag,
			self.contractId,
			effectiveByteCount,
			self.ackedByteCount+self.unackedByteCount,
			self.ackedByteCount+self.unackedByteCount,
			self.effectiveTransferByteCount,
			100.0*float32(self.ackedByteCount+self.unackedByteCount)/float32(self.effectiveTransferByteCount),
		)
	}
	return true
}

// canUpdate reports whether byteCount can be debited without mutating the
// contract. The send sequence uses it before coalescing queued Packs: a batch
// must not cross a contract boundary, because a newly attached contract frame
// can push an otherwise-small batch over the transport message limit.
func (self *sequenceContract) canUpdate(byteCount ByteCount) bool {
	effectiveByteCount := max(self.minUpdateByteCount, byteCount)
	return self.ackedByteCount+self.unackedByteCount+effectiveByteCount <= self.effectiveTransferByteCount
}

func (self *sequenceContract) ack(byteCount ByteCount) {
	effectiveByteCount := max(self.minUpdateByteCount, byteCount)

	if self.unackedByteCount < effectiveByteCount {
		// debug.PrintStack()
		panic(fmt.Errorf("Bad accounting %d <> %d", self.unackedByteCount, byteCount))
	}

	self.unackedByteCount -= effectiveByteCount
	self.ackedByteCount += effectiveByteCount
}

type ForwardBufferSettings struct {
	IdleTimeout time.Duration

	SequenceBufferSize int

	WriteTimeout time.Duration

	// Nil test barriers expose exact worker and close-join boundaries without
	// changing production behavior.
	beforeCreateForwardSequenceForTest func(TransferPath)
	beforeRunForwardSequenceForTest    func(TransferPath)
	beforeCloseWaitForTest             func(TransferPath)
	afterRunForwardSequenceForTest     func(TransferPath)
}

type ForwardBuffer struct {
	ctx    context.Context
	client *Client

	forwardBufferSettings *ForwardBufferSettings

	mutex  sync.Mutex
	closed bool
	// destination -> forward sequence
	forwardSequences map[TransferPath]*ForwardSequence
	// activeForwardSequences retains workers after lookup removal until their
	// final queued ForwardPack drain has completed.
	activeForwardSequences map[*ForwardSequence]bool

	beforeCreateForwardSequenceForTest func(TransferPath)
	beforeRunForwardSequenceForTest    func(TransferPath)
	beforeCloseWaitForTest             func(TransferPath)
	afterRunForwardSequenceForTest     func(TransferPath)
}

func NewForwardBuffer(ctx context.Context,
	client *Client,
	forwardBufferSettings *ForwardBufferSettings) *ForwardBuffer {
	return &ForwardBuffer{
		ctx:                    ctx,
		client:                 client,
		forwardBufferSettings:  forwardBufferSettings,
		forwardSequences:       map[TransferPath]*ForwardSequence{},
		activeForwardSequences: map[*ForwardSequence]bool{},
		beforeCreateForwardSequenceForTest: forwardBufferSettings.
			beforeCreateForwardSequenceForTest,
		beforeRunForwardSequenceForTest: forwardBufferSettings.
			beforeRunForwardSequenceForTest,
		beforeCloseWaitForTest: forwardBufferSettings.beforeCloseWaitForTest,
		afterRunForwardSequenceForTest: forwardBufferSettings.
			afterRunForwardSequenceForTest,
	}
}

func (self *ForwardBuffer) Pack(forwardPack *ForwardPack, timeout time.Duration) (bool, error) {
	initForwardSequence := func(skip *ForwardSequence) *ForwardSequence {
		if self.beforeCreateForwardSequenceForTest != nil {
			self.beforeCreateForwardSequenceForTest(forwardPack.Destination)
		}
		self.mutex.Lock()
		defer self.mutex.Unlock()

		if self.closed {
			return nil
		}
		forwardSequence, ok := self.forwardSequences[forwardPack.Destination]
		if ok {
			if skip == nil || skip != forwardSequence {
				return forwardSequence
			} else {
				forwardSequence.Cancel()
				// delete(self.forwardSequences, forwardPack.Destination)
			}
		}
		forwardSequence = NewForwardSequence(
			self.ctx,
			self.client,
			forwardPack.Destination,
			self.forwardBufferSettings,
		)
		self.forwardSequences[forwardPack.Destination] = forwardSequence
		self.activeForwardSequences[forwardSequence] = true
		go self.runForwardSequence(forwardPack.Destination, forwardSequence)
		return forwardSequence
	}

	var forwardSequence *ForwardSequence
	var success bool
	var err error
	for i := 0; i < 2; i += 1 {
		select {
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		default:
		}
		forwardSequence = initForwardSequence(forwardSequence)
		if forwardSequence == nil {
			return false, errors.New("Done.")
		}
		if success, err = forwardSequence.Pack(forwardPack, timeout); err == nil {
			return success, nil
		}
		// sequence closed
	}
	return success, err
}

// runForwardSequence owns one admitted worker through lookup removal and its
// final ForwardPack drain.
func (self *ForwardBuffer) runForwardSequence(
	destination TransferPath,
	forwardSequence *ForwardSequence,
) {
	defer func() {
		self.mutex.Lock()
		delete(self.activeForwardSequences, forwardSequence)
		close(forwardSequence.done)
		self.mutex.Unlock()
	}()
	if self.beforeRunForwardSequenceForTest != nil {
		self.beforeRunForwardSequenceForTest(destination)
	}
	HandleError(func() {
		defer func() {
			if self.afterRunForwardSequenceForTest != nil {
				self.afterRunForwardSequenceForTest(destination)
			}
		}()
		defer forwardSequence.Close()
		defer func() {
			self.mutex.Lock()
			if forwardSequence == self.forwardSequences[destination] {
				delete(self.forwardSequences, destination)
			}
			self.mutex.Unlock()
		}()
		forwardSequence.Run()
	})
}

func (self *ForwardBuffer) Close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.closed = true
	// cancel all open sequences
	// the control of the sequence will close it
	for _, forwardSequence := range self.forwardSequences {
		forwardSequence.Cancel()
	}
}

func (self *ForwardBuffer) Cancel() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	self.closed = true
	// cancel all open sequences
	for _, forwardSequence := range self.forwardSequences {
		forwardSequence.Cancel()
	}
}

// closeAndWait closes forward-sequence admission and joins every admitted
// worker after its route writer and queued message ownership are released.
func (self *ForwardBuffer) closeAndWait(ctx context.Context) error {
	self.Close()

	self.mutex.Lock()
	sequences := make([]*ForwardSequence, 0, len(self.activeForwardSequences))
	for forwardSequence := range self.activeForwardSequences {
		sequences = append(sequences, forwardSequence)
	}
	self.mutex.Unlock()

	for _, forwardSequence := range sequences {
		if self.beforeCloseWaitForTest != nil {
			self.beforeCloseWaitForTest(forwardSequence.destination)
		}
		if err := waitForLifecycleDone(ctx, forwardSequence.done, "forward sequence"); err != nil {
			return err
		}
	}
	return nil
}

func (self *ForwardBuffer) Flush() {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	// cancel all open sequences
	for _, forwardSequence := range self.forwardSequences {
		// if !destination.IsControlDestination() {
		forwardSequence.Cancel()
		// }
	}
}

type ForwardSequence struct {
	ctx    context.Context
	cancel context.CancelFunc
	// done closes after the owning ForwardBuffer removes the worker, closes its
	// route writer, and drains all queued message-pool ownership.
	done chan struct{}

	client    *Client
	clientId  Id
	clientTag string
	log       Logger

	destination TransferPath

	forwardBufferSettings *ForwardBufferSettings

	packMutex sync.Mutex
	packs     chan *ForwardPack

	idleCondition *IdleCondition

	multiRouteWriter MultiRouteWriter
}

func NewForwardSequence(
	ctx context.Context,
	client *Client,
	destination TransferPath,
	forwardBufferSettings *ForwardBufferSettings) *ForwardSequence {
	cancelCtx, cancel := context.WithCancel(ctx)
	return &ForwardSequence{
		ctx:                   cancelCtx,
		cancel:                cancel,
		done:                  make(chan struct{}),
		client:                client,
		log:                   client.log,
		destination:           destination,
		forwardBufferSettings: forwardBufferSettings,
		packs:                 make(chan *ForwardPack, forwardBufferSettings.SequenceBufferSize),
		idleCondition:         NewIdleCondition(),
	}
}

// success, error
func (self *ForwardSequence) Pack(forwardPack *ForwardPack, timeout time.Duration) (bool, error) {
	self.packMutex.Lock()
	defer self.packMutex.Unlock()

	select {
	case <-forwardPack.Ctx.Done():
		return false, errors.New("Done.")
	case <-self.ctx.Done():
		return false, errors.New("Done.")
	default:
	}

	if !self.idleCondition.UpdateOpen() {
		return false, errors.New("Done.")
	}
	defer self.idleCondition.UpdateClose()

	// fast path without arming a timer
	select {
	case self.packs <- forwardPack:
		return true, nil
	default:
	}

	if timeout < 0 {
		select {
		case <-forwardPack.Ctx.Done():
			return false, errors.New("Done.")
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		case self.packs <- forwardPack:
			return true, nil
		}
	} else if timeout == 0 {
		select {
		case <-forwardPack.Ctx.Done():
			return false, errors.New("Done.")
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		case self.packs <- forwardPack:
			return true, nil
		default:
			return false, nil
		}
	} else {
		select {
		case <-forwardPack.Ctx.Done():
			return false, errors.New("Done.")
		case <-self.ctx.Done():
			return false, errors.New("Done.")
		case self.packs <- forwardPack:
			return true, nil
		case <-time.After(timeout):
			return false, nil
		}
	}
}

func (self *ForwardSequence) Run() {
	defer self.cancel()

	self.multiRouteWriter = self.client.RouteManager().OpenMultiRouteWriter(self.destination)
	defer self.client.RouteManager().CloseMultiRouteWriter(self.multiRouteWriter)

	// reusable idle timer (avoids a per-iteration time.After alloc on the
	// forward hot path). created already-fired; Reset before the blocking select
	// arms it (go1.23+ delivers no stale fire after Reset).
	idleTimer := time.NewTimer(0)
	defer idleTimer.Stop()

	for {
		processPack := func(forwardPack *ForwardPack, ok bool) bool {
			if !ok {
				return false
			}
			c := func() error {
				transferFrameBytes := forwardPack.TransferFrameBytes
				if DebugTransferCopyOnWrite {
					transferFrameBytes = MessagePoolCopy(forwardPack.TransferFrameBytes)
					// the write proceeds on the copy; the original is done here
					MessagePoolReturn(forwardPack.TransferFrameBytes)
				}
				defer MessagePoolReturn(transferFrameBytes)
				shared := MessagePoolShareReadOnly(transferFrameBytes)
				err := self.multiRouteWriter.Write(
					self.ctx,
					shared,
					self.forwardBufferSettings.WriteTimeout,
				)
				if err != nil {
					// a failed write leaves ownership here: undo the consumer's share
					MessagePoolReturn(shared)
				}
				return err
			}
			if self.log.V(2).Enabled() {
				TraceWithReturn(
					fmt.Sprintf("[f]multi route write %s->%s s(%s)", self.clientTag, self.destination.DestinationId, self.destination.StreamId),
					c,
				)
			} else {
				err := c()
				if err != nil {
					if self.log.V(2).Enabled() {
						self.log.Infof("[f]drop = %s", err)
					}
				}
			}
			return true
		}

		// fast path without arming a timer
		select {
		case <-self.ctx.Done():
			return
		case forwardPack, ok := <-self.packs:
			if !processPack(forwardPack, ok) {
				return
			}
			continue
		default:
		}

		checkpointId := self.idleCondition.Checkpoint()
		idleTimer.Reset(self.forwardBufferSettings.IdleTimeout)
		select {
		case <-self.ctx.Done():
			return
		case forwardPack, ok := <-self.packs:
			if !processPack(forwardPack, ok) {
				return
			}
		case <-idleTimer.C:
			done := false
			func() {
				self.packMutex.Lock()
				defer self.packMutex.Unlock()
				// idle timeout
				if self.idleCondition.Close(checkpointId) {
					done = true
				}
				// else there are pending updates
			}()
			if done {
				// close the sequence
				if self.log.V(1).Enabled() {
					self.log.Infof("[f]exit idle timeout %s->%s s(%s)", self.clientTag, self.destination.DestinationId, self.destination.StreamId)
				}
				return
			}
		}
	}
}

func (self *ForwardSequence) Close() {
	self.cancel()

	func() {
		self.packMutex.Lock()
		defer self.packMutex.Unlock()
		close(self.packs)
	}()

	// drain the channel (mirrors SendSequence.Close/ReceiveSequence.Close: queued
	// packs are owned by the sequence and must be returned)
	func() {
		for {
			select {
			case forwardPack, ok := <-self.packs:
				if !ok {
					return
				}
				MessagePoolReturn(forwardPack.TransferFrameBytes)
			default:
				return
			}
		}
	}()
}

func (self *ForwardSequence) Cancel() {
	self.cancel()
}

type PeerAudit struct {
	startTime           time.Time
	lastModifiedTime    time.Time
	Abuse               bool
	BadContractCount    int
	DiscardedByteCount  ByteCount
	DiscardedCount      int
	BadMessageByteCount ByteCount
	BadMessageCount     int
	SendByteCount       ByteCount
	SendCount           int
	ResendByteCount     ByteCount
	ResendCount         int
}

func NewPeerAudit(startTime time.Time) *PeerAudit {
	return &PeerAudit{
		startTime:           startTime,
		lastModifiedTime:    startTime,
		BadContractCount:    0,
		DiscardedByteCount:  ByteCount(0),
		DiscardedCount:      0,
		BadMessageByteCount: ByteCount(0),
		BadMessageCount:     0,
		SendByteCount:       ByteCount(0),
		SendCount:           0,
		ResendByteCount:     ByteCount(0),
		ResendCount:         0,
	}
}

func (self *PeerAudit) badMessage(byteCount ByteCount) {
	self.BadMessageCount += 1
	self.BadMessageByteCount += byteCount
}

func (self *PeerAudit) discard(byteCount ByteCount) {
	self.DiscardedCount += 1
	self.DiscardedByteCount += byteCount
}

func (self *PeerAudit) badContract() {
	self.BadContractCount += 1
}

func (self *PeerAudit) received(byteCount ByteCount) {
	self.SendCount += 1
	self.SendByteCount += byteCount
}

func (self *PeerAudit) resend(byteCount ByteCount) {
	self.ResendCount += 1
	self.ResendByteCount += byteCount
}

type SequencePeerAudit struct {
	client           *Client
	log              Logger
	source           TransferPath
	maxAuditDuration time.Duration

	peerAudit *PeerAudit
}

func NewSequencePeerAudit(client *Client, source TransferPath, maxAuditDuration time.Duration) *SequencePeerAudit {
	return &SequencePeerAudit{
		client:           client,
		log:              client.log,
		source:           source,
		maxAuditDuration: maxAuditDuration,
		peerAudit:        nil,
	}
}

// Moves subsequent observations onto the stream authenticated by a verified
// contract. A route change completes the prior attribution first.
func (self *SequencePeerAudit) SetStreamId(streamId Id) {
	if self.source.StreamId == streamId {
		return
	}
	self.Complete()
	self.source.StreamId = streamId
}

func (self *SequencePeerAudit) Update(callback func(*PeerAudit)) {
	auditTime := time.Now()

	if self.peerAudit != nil && self.maxAuditDuration <= auditTime.Sub(self.peerAudit.startTime) {
		self.Complete()
	}
	if self.peerAudit == nil {
		self.peerAudit = NewPeerAudit(auditTime)
	}

	callback(self.peerAudit)
	self.peerAudit.lastModifiedTime = auditTime
	// TODO auto complete the peer audit after timeout
}

func (self *SequencePeerAudit) Complete() {
	if self.peerAudit == nil {
		return
	}

	peerAudit := &protocol.PeerAudit{
		PeerId:              self.source.SourceId.Bytes(),
		StreamId:            self.source.StreamId.Bytes(),
		Duration:            uint64(math.Ceil((self.peerAudit.lastModifiedTime.Sub(self.peerAudit.startTime)).Seconds())),
		Abuse:               self.peerAudit.Abuse,
		BadContractCount:    uint64(self.peerAudit.BadContractCount),
		DiscardedByteCount:  uint64(self.peerAudit.DiscardedByteCount),
		DiscardedCount:      uint64(self.peerAudit.DiscardedCount),
		BadMessageByteCount: uint64(self.peerAudit.BadMessageByteCount),
		BadMessageCount:     uint64(self.peerAudit.BadMessageCount),
		SendByteCount:       uint64(self.peerAudit.SendByteCount),
		SendCount:           uint64(self.peerAudit.SendCount),
		ResendByteCount:     uint64(self.peerAudit.ResendByteCount),
		ResendCount:         uint64(self.peerAudit.ResendCount),
	}
	frame, err := ToFrame(peerAudit, DefaultProtocolVersion)
	if err != nil {
		self.log.Errorf("[c]could not create audit frame = %s", err)
		return
	}
	self.client.ClientOob().SendControl(
		[]*protocol.Frame{frame},
		func(resultFrames []*protocol.Frame, err error) {},
	)
	self.peerAudit = nil
}

// contract frames are not counted towards the message byte count
// this is required since contracts can be attached post-hoc
func MessageByteCount(frames []*protocol.Frame) ByteCount {
	// messageByteCount := ByteCount(0)
	// for _, frame := range frames {
	// 	if frame.MessageType != protocol.MessageType_TransferContract {
	// 		messageByteCount += ByteCount(len(frame.MessageBytes))
	// 	}
	// }
	// return messageByteCount
	messageByteCount := ByteCount(0)
	for _, frame := range frames {
		messageByteCount += ByteCount(len(frame.MessageBytes))
	}
	return messageByteCount
}

// func MessageFrames(frames []*protocol.Frame) []*protocol.Frame {
// 	messages := []*protocol.Frame{}
// 	for _, frame := range frames {
// 		if frame.MessageType != protocol.MessageType_TransferContract {
// 			messages = append(messages, frame)
// 		}
// 	}
// 	return messages
// }

// prewarmOpeningContract requests this sequence's first contract as the
// sequence starts, so the round trip to the platform overlaps producing the
// first message instead of blocking it.
//
// Contracts after the first are already queued the moment their predecessor is
// taken, which is why mid-stream renewals complete in under 50ms while the
// opening one costs ~260ms. Every new destination is a new sequence, so web
// browsing pays that opening cost constantly.
//
// The request is fire-and-forget: CreateContract hands the frame to the control
// channel with a callback and returns, so this never blocks the caller. The
// size is left to the contract manager's own ramp -- only a floor is passed.
func (self *SendSequence) prewarmOpeningContract() {
	if !self.sendBufferSettings.PrewarmOpeningContract {
		return
	}

	// mirror the first thing updateContract checks. A destination configured to
	// require no contract never takes one, so requesting it would open a queue
	// that is only cleaned up by the janitor -- and it charges a control round
	// trip for a contract that cannot be used.
	if self.client.ContractManager().SendNoContract(self.destination) {
		return
	}

	self.client.ContractManager().CreateContract(
		self.contractMetadata().key,
		self.contractSeqIndex,
		ByteCount(float32(self.sendBufferSettings.MinMessageByteCount)/self.sendBufferSettings.ContractFillFraction),
	)
}

// addContractWaitTime records time the send sequence spent blocked acquiring a
// contract. Device measurements showed ~350ms unaccounted for between a
// connection being established and its first byte arriving; this is the one
// blocking step in that window, so it needs to be measurable rather than
// inferred.
func (self *SendSequence) addContractWaitTime(contractWaitTime time.Duration) {
	self.contractWaitNanos.Add(int64(contractWaitTime))
	self.contractWaitCount.Add(1)
}

// ContractWaitTime is the total time this sequence has spent blocked acquiring
// contracts, and how many acquisitions that covers.
func (self *SendSequence) ContractWaitTime() (time.Duration, int64) {
	return time.Duration(self.contractWaitNanos.Load()), self.contractWaitCount.Load()
}
