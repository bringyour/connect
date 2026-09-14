package connect

// This file defines the versioned, bounded message layer carried by QUIC
// DATAGRAM. One envelope belongs to one H3 connection generation; Transfer,
// above it, remains responsible for acknowledgement and retransmission.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/urnetwork/connect/v2026/protocol"
)

const (
	// Bumped only when an endpoint cannot decode the preceding envelope.
	// Version 2 changes negotiated carrier semantics from DATAGRAM-only to a
	// hybrid: small routed frames use DATAGRAM and larger frames remain on the
	// reliable QUIC stream. Bumping makes old/new peers safely fall back to the
	// legacy stream instead of disagreeing about whether stream data is valid.
	H3DatagramProtocolVersion uint32 = 2

	// H3InitialPacketByteCount is QUIC's minimum packet size. Starting at the
	// minimum keeps a QUIC UDP payload plus IPv4 and UDP headers within the
	// IPv6-mandated 1,280-byte cellular path floor, even though the tunnel is
	// currently IPv4-only. DPLPMTUD remains enabled and can safely grow the
	// live packet size after validating a larger path.
	H3InitialPacketByteCount = 1200
	// H3InitialDatagramByteCount is quic-go's conservative DATAGRAM payload
	// allowance after its maximum short header, AEAD tag, and DATAGRAM frame
	// overhead are reserved from the initial packet.
	H3InitialDatagramByteCount = H3InitialPacketByteCount - 40

	// This optimistic target allows DPLPMTUD to grow a validated path to the
	// previous Ethernet-sized ceiling. quic-go synchronously reports the
	// smaller initial or migrated-path ceiling through DatagramTooLargeError;
	// the hybrid sender records that live limit before choosing the lane.
	defaultH3DatagramTargetByteCount = 1360

	// Wire bytes preceding each fragment payload.
	H3DatagramHeaderByteCount               = 28
	defaultH3HybridDatagramMessageByteCount = defaultH3DatagramTargetByteCount -
		H3DatagramHeaderByteCount
	// A complete routed message either fits one QUIC DATAGRAM or uses the
	// reliable stream. Splitting one Transfer message across multiple lossy
	// DATAGRAMs multiplies its loss probability and made full-MTU TCP 19% slower
	// in the corrected one-bar full-TUN benchmark.
	defaultH3DatagramMaxFragments  = 1
	defaultH3DatagramMaxMessages   = 32
	defaultH3DatagramReplayIds     = 256
	defaultH3DatagramMessageBytes  = 8 * 1024
	defaultH3DatagramPeerBytes     = 64 * 1024
	defaultH3DatagramProcessBytes  = 8 * 1024 * 1024
	defaultH3DatagramReassemblyTtl = 5 * time.Second

	// The hybrid dispatcher must not turn lane separation into another large
	// per-connection buffer. The byte limit counts the actual retained slice
	// capacity (including message-pool metadata), and reservations stay live
	// until the stream writer returns the message. Reserve metadata for every
	// count slot so 32 packet-pool buffers fit the intended 64 KiB payload-
	// capacity budget; one maximum uint16-framed message also fits.
	H3HybridStreamQueueMessageCount = 32
	H3HybridStreamQueueByteCount    = 64*1024 +
		H3HybridStreamQueueMessageCount*MessagePoolMetaByteCount
)

var (
	h3DatagramMagic = [3]byte{'U', 'R', 'D'}

	// Returned when the complete Transfer frame cannot fit within negotiated
	// fragmentation limits. The caller must leave recovery to Transfer.
	ErrH3DatagramMessageTooLarge = errors.New("H3 DATAGRAM message exceeds fragmentation limits")
)

// initialH3DatagramPathByteCount asks quic-go for the current synchronous
// DATAGRAM ceiling before publishing the route. A deliberately impossible
// 2,048-byte payload is above quic-go v0.61.0's 1,452-byte packet buffer, so it
// is never queued or put on the wire; DatagramTooLargeError carries the live
// path value without a trial application message. If the dependency cannot
// report it, retain the conservative 1,200-byte QUIC startup geometry.
func initialH3DatagramPathByteCount(
	configuredMaximum int,
	send func([]byte) error,
) int {
	initialMaximum := min(configuredMaximum, H3InitialDatagramByteCount)
	var probe [2048]byte
	err := send(probe[:])
	var tooLargeErr *quic.DatagramTooLargeError
	if !errors.As(err, &tooLargeErr) ||
		int(tooLargeErr.MaxDatagramPayloadSize) <= H3DatagramHeaderByteCount {
		return initialMaximum
	}
	return min(configuredMaximum, int(tooLargeErr.MaxDatagramPayloadSize))
}

// H3DatagramSettings places hard byte, fragment, message, and age limits on a
// connection generation. A zero value is invalid; use the default constructor.
type H3DatagramSettings struct {
	TargetDatagramByteCount    int
	MaxFragmentCount           int
	MaxMessageByteCount        int
	MaxReassemblyMessageCount  int
	MaxReassemblyByteCount     int
	ProcessReassemblyByteCount int64
	ReassemblyTimeout          time.Duration
	// HybridDatagramMessageByteCount is the largest complete routed Transfer
	// frame selected for the bounded DATAGRAM carrier. The current path may
	// require more than one envelope fragment. Larger frames use the reliable
	// stream.
	HybridDatagramMessageByteCount int
}

// Returns bounded limits for a hybrid carrier and a path that may grow from
// QUIC's 1,200-byte minimum after DPLPMTUD validation. Small complete Transfer
// frames use one DATAGRAM; larger frames use the reliable stream. The process
// budget is shared by all connections owned by one PlatformTransport or
// ConnectHandler.
func DefaultH3DatagramSettings() *H3DatagramSettings {
	return &H3DatagramSettings{
		TargetDatagramByteCount:        defaultH3DatagramTargetByteCount,
		MaxFragmentCount:               defaultH3DatagramMaxFragments,
		MaxMessageByteCount:            defaultH3DatagramMessageBytes,
		MaxReassemblyMessageCount:      defaultH3DatagramMaxMessages,
		MaxReassemblyByteCount:         defaultH3DatagramPeerBytes,
		ProcessReassemblyByteCount:     defaultH3DatagramProcessBytes,
		ReassemblyTimeout:              defaultH3DatagramReassemblyTtl,
		HybridDatagramMessageByteCount: defaultH3HybridDatagramMessageByteCount,
	}
}

// SetH3DatagramAuthOffer resets response-only state and advertises exactly one
// envelope version when the candidate is enabled.
func SetH3DatagramAuthOffer(auth *protocol.Auth, enabled bool) {
	auth.H3DatagramVersion = 0
	auth.H3DatagramAcceptedVersion = 0
	if enabled {
		auth.H3DatagramVersion = H3DatagramProtocolVersion
	}
}

// AcceptH3DatagramAuthOffer returns the server response and whether routed
// Transfer frames may leave the reliable stream on this connection. QUIC's
// transport parameters and the application envelope version must both agree.
func AcceptH3DatagramAuthOffer(
	auth *protocol.Auth,
	enabled bool,
	localQuicSupport bool,
	remoteQuicSupport bool,
) (*protocol.Auth, bool) {
	accepted := enabled && localQuicSupport && remoteQuicSupport &&
		auth.H3DatagramVersion == H3DatagramProtocolVersion
	response := &protocol.Auth{
		ByJwt:             auth.ByJwt,
		AppVersion:        auth.AppVersion,
		InstanceId:        bytes.Clone(auth.InstanceId),
		H3DatagramVersion: auth.H3DatagramVersion,
	}
	if accepted {
		response.H3DatagramAcceptedVersion = H3DatagramProtocolVersion
	}
	return response, accepted
}

// ValidateH3DatagramAuthResponse authenticates the echoed request fields before
// acting on a server acceptance. A zero acceptance is the intentional legacy
// fallback, while an unknown nonzero version is a protocol error.
func ValidateH3DatagramAuthResponse(
	request *protocol.Auth,
	response *protocol.Auth,
	enabled bool,
	localQuicSupport bool,
	remoteQuicSupport bool,
) (bool, error) {
	if response == nil || response.ByJwt != request.ByJwt ||
		response.AppVersion != request.AppVersion ||
		!bytes.Equal(response.InstanceId, request.InstanceId) ||
		response.H3DatagramVersion != request.H3DatagramVersion {
		return false, fmt.Errorf("H3 auth response mismatched identity or capability offer")
	}
	if response.H3DatagramAcceptedVersion == 0 {
		return false, nil
	}
	if response.H3DatagramAcceptedVersion != H3DatagramProtocolVersion ||
		request.H3DatagramVersion != H3DatagramProtocolVersion {
		return false, fmt.Errorf("unsupported H3 DATAGRAM accepted version %d", response.H3DatagramAcceptedVersion)
	}
	if !enabled || !localQuicSupport || !remoteQuicSupport {
		return false, fmt.Errorf("H3 DATAGRAM accepted without local QUIC capability")
	}
	return true, nil
}

// Validates limits before they are used on an untrusted receive path.
func (self *H3DatagramSettings) Validate() error {
	if self == nil {
		return fmt.Errorf("missing H3 DATAGRAM settings")
	}
	if self.TargetDatagramByteCount <= H3DatagramHeaderByteCount {
		return fmt.Errorf("H3 DATAGRAM target byte count %d <= header byte count %d", self.TargetDatagramByteCount, H3DatagramHeaderByteCount)
	}
	if self.MaxFragmentCount < 1 || int(^uint16(0)) < self.MaxFragmentCount {
		return fmt.Errorf("invalid H3 DATAGRAM fragment limit %d", self.MaxFragmentCount)
	}
	if self.MaxMessageByteCount < 1 ||
		uint64(^uint32(0)) < uint64(self.MaxMessageByteCount) {
		return fmt.Errorf("invalid H3 DATAGRAM message byte limit %d", self.MaxMessageByteCount)
	}
	if self.MaxReassemblyMessageCount < 1 {
		return fmt.Errorf("invalid H3 DATAGRAM reassembly message limit %d", self.MaxReassemblyMessageCount)
	}
	if self.MaxReassemblyByteCount < self.MaxMessageByteCount {
		return fmt.Errorf("H3 DATAGRAM peer byte limit %d < message byte limit %d", self.MaxReassemblyByteCount, self.MaxMessageByteCount)
	}
	if self.ProcessReassemblyByteCount < int64(self.MaxMessageByteCount) {
		return fmt.Errorf("H3 DATAGRAM process byte limit %d < message byte limit %d", self.ProcessReassemblyByteCount, self.MaxMessageByteCount)
	}
	if self.ReassemblyTimeout <= 0 {
		return fmt.Errorf("invalid H3 DATAGRAM reassembly timeout %s", self.ReassemblyTimeout)
	}
	if self.HybridDatagramMessageByteCount < 1 {
		return fmt.Errorf(
			"invalid H3 hybrid DATAGRAM message threshold %d",
			self.HybridDatagramMessageByteCount,
		)
	}
	return nil
}

// UseDatagram reports the negotiated hybrid lane for one complete routed
// Transfer frame. Authentication and empty liveness frames are never passed to
// this selector and remain on the reliable stream. The production fragment
// limit is one, so selecting DATAGRAM never creates partial-message loss.
func (self *H3DatagramSettings) UseDatagram(messageByteCount int) bool {
	return self.UseDatagramForPath(
		messageByteCount,
		self.TargetDatagramByteCount,
	)
}

// UseDatagramForPath applies the last QUIC DATAGRAM payload limit observed by
// the connection. A normal 1,100-byte encrypted packet does not fit one
// DATAGRAM at QUIC's minimum packet size and therefore uses the stream.
// Contract-only control and other larger messages do likewise. A smaller
// migrated path moves any newly oversized message to the stream.
func (self *H3DatagramSettings) UseDatagramForPath(
	messageByteCount int,
	maxDatagramByteCount int,
) bool {
	if messageByteCount <= 0 ||
		min(self.HybridDatagramMessageByteCount, self.MaxMessageByteCount) < messageByteCount {
		return false
	}
	fragmentPayloadByteCount := maxDatagramByteCount - H3DatagramHeaderByteCount
	if fragmentPayloadByteCount < 1 {
		return false
	}
	fragmentCount := (messageByteCount + fragmentPayloadByteCount - 1) /
		fragmentPayloadByteCount
	return fragmentCount <= self.MaxFragmentCount
}

// H3DatagramTransferFrameByteLimit returns the largest complete Transfer frame
// selected for DATAGRAM on one observed QUIC path. UseDatagramForPath is a
// contiguous size predicate, so this limit can be serialized across the server
// exchange and reconstructed without losing the stream-vs-DATAGRAM decision.
func H3DatagramTransferFrameByteLimit(
	settings *H3DatagramSettings,
	maxDatagramByteCount int,
) int {
	if settings == nil || settings.MaxFragmentCount <= 0 {
		return 0
	}
	fragmentPayloadByteCount := maxDatagramByteCount - H3DatagramHeaderByteCount
	if fragmentPayloadByteCount <= 0 {
		return 0
	}
	pathLimit := int64(fragmentPayloadByteCount) * int64(settings.MaxFragmentCount)
	limit := min(
		int64(settings.HybridDatagramMessageByteCount),
		int64(settings.MaxMessageByteCount),
		pathLimit,
	)
	if limit <= 0 {
		return 0
	}
	if int64(int(limit)) != limit {
		return int(^uint(0) >> 1)
	}
	return int(limit)
}

// H3DatagramStatsSnapshot is one lock-free lifetime view of the candidate data
// carrier. All byte counters contain envelope bytes passed to or accepted by
// quic-go, not UDP/IP overhead.
type H3DatagramStatsSnapshot struct {
	SentMessageCount                     uint64
	SentMessageByteCount                 uint64
	SentFragmentCount                    uint64
	SentByteCount                        uint64
	SendErrorCount                       uint64
	ReceivedMessageCount                 uint64
	ReceivedMessageByteCount             uint64
	ReceivedFragmentCount                uint64
	ReceivedByteCount                    uint64
	DuplicateFragmentCount               uint64
	MalformedFragmentCount               uint64
	ChecksumFailureCount                 uint64
	ReassemblyTimeoutCount               uint64
	ReassemblyLimitCount                 uint64
	StreamSentMessageCount               uint64
	StreamSentMessageByteCount           uint64
	StreamReceivedMessageCount           uint64
	StreamReceivedMessageByteCount       uint64
	HybridStreamQueueCurrentMessageCount uint64
	HybridStreamQueueCurrentByteCount    uint64
	HybridStreamQueueMaximumMessageCount uint64
	HybridStreamQueueMaximumByteCount    uint64
	HybridStreamQueueWaitCount           uint64
	HybridStreamQueueWaitDuration        time.Duration
	HybridStreamQueueOversizeCount       uint64
}

// H3DatagramStats aggregates connection generations without adding locks to
// either carrier pump.
type H3DatagramStats struct {
	sentMessageCount                     atomic.Uint64
	sentMessageByteCount                 atomic.Uint64
	sentFragmentCount                    atomic.Uint64
	sentByteCount                        atomic.Uint64
	sendErrorCount                       atomic.Uint64
	receivedMessageCount                 atomic.Uint64
	receivedMessageByteCount             atomic.Uint64
	receivedFragmentCount                atomic.Uint64
	receivedByteCount                    atomic.Uint64
	duplicateFragmentCount               atomic.Uint64
	malformedFragmentCount               atomic.Uint64
	checksumFailureCount                 atomic.Uint64
	reassemblyTimeoutCount               atomic.Uint64
	reassemblyLimitCount                 atomic.Uint64
	streamSentMessageCount               atomic.Uint64
	streamSentMessageByteCount           atomic.Uint64
	streamReceivedMessageCount           atomic.Uint64
	streamReceivedMessageByteCount       atomic.Uint64
	hybridStreamQueueCurrentMessageCount atomic.Int64
	hybridStreamQueueCurrentByteCount    atomic.Int64
	hybridStreamQueueMaximumMessageCount atomic.Uint64
	hybridStreamQueueMaximumByteCount    atomic.Uint64
	hybridStreamQueueWaitCount           atomic.Uint64
	hybridStreamQueueWaitDuration        atomic.Uint64
	hybridStreamQueueOversizeCount       atomic.Uint64
}

// Snapshot returns counters from all connection generations sharing this
// collector.
func (self *H3DatagramStats) Snapshot() H3DatagramStatsSnapshot {
	if self == nil {
		return H3DatagramStatsSnapshot{}
	}
	return H3DatagramStatsSnapshot{
		SentMessageCount:               self.sentMessageCount.Load(),
		SentMessageByteCount:           self.sentMessageByteCount.Load(),
		SentFragmentCount:              self.sentFragmentCount.Load(),
		SentByteCount:                  self.sentByteCount.Load(),
		SendErrorCount:                 self.sendErrorCount.Load(),
		ReceivedMessageCount:           self.receivedMessageCount.Load(),
		ReceivedMessageByteCount:       self.receivedMessageByteCount.Load(),
		ReceivedFragmentCount:          self.receivedFragmentCount.Load(),
		ReceivedByteCount:              self.receivedByteCount.Load(),
		DuplicateFragmentCount:         self.duplicateFragmentCount.Load(),
		MalformedFragmentCount:         self.malformedFragmentCount.Load(),
		ChecksumFailureCount:           self.checksumFailureCount.Load(),
		ReassemblyTimeoutCount:         self.reassemblyTimeoutCount.Load(),
		ReassemblyLimitCount:           self.reassemblyLimitCount.Load(),
		StreamSentMessageCount:         self.streamSentMessageCount.Load(),
		StreamSentMessageByteCount:     self.streamSentMessageByteCount.Load(),
		StreamReceivedMessageCount:     self.streamReceivedMessageCount.Load(),
		StreamReceivedMessageByteCount: self.streamReceivedMessageByteCount.Load(),
		HybridStreamQueueCurrentMessageCount: uint64(max(
			int64(0),
			self.hybridStreamQueueCurrentMessageCount.Load(),
		)),
		HybridStreamQueueCurrentByteCount: uint64(max(
			int64(0),
			self.hybridStreamQueueCurrentByteCount.Load(),
		)),
		HybridStreamQueueMaximumMessageCount: self.hybridStreamQueueMaximumMessageCount.Load(),
		HybridStreamQueueMaximumByteCount:    self.hybridStreamQueueMaximumByteCount.Load(),
		HybridStreamQueueWaitCount:           self.hybridStreamQueueWaitCount.Load(),
		HybridStreamQueueWaitDuration: time.Duration(
			self.hybridStreamQueueWaitDuration.Load(),
		),
		HybridStreamQueueOversizeCount: self.hybridStreamQueueOversizeCount.Load(),
	}
}

// RecordStreamSent and RecordStreamReceived account the reliable lane of one
// negotiated hybrid generation at the same complete-Transfer boundary used by
// DATAGRAM message counters.
func (self *H3DatagramStats) RecordStreamSent(messageByteCount int) {
	if self == nil || messageByteCount <= 0 {
		return
	}
	self.streamSentMessageCount.Add(1)
	self.streamSentMessageByteCount.Add(uint64(messageByteCount))
}

func (self *H3DatagramStats) RecordStreamReceived(messageByteCount int) {
	if self == nil || messageByteCount <= 0 {
		return
	}
	self.streamReceivedMessageCount.Add(1)
	self.streamReceivedMessageByteCount.Add(uint64(messageByteCount))
}

func updateH3DatagramMaximum(target *atomic.Uint64, value uint64) {
	for {
		current := target.Load()
		if value <= current || target.CompareAndSwap(current, value) {
			return
		}
	}
}

func (self *H3DatagramStats) recordHybridStreamQueueAcquire(byteCount int) {
	if self == nil {
		return
	}
	messageCount := self.hybridStreamQueueCurrentMessageCount.Add(1)
	retainedByteCount := self.hybridStreamQueueCurrentByteCount.Add(int64(byteCount))
	updateH3DatagramMaximum(
		&self.hybridStreamQueueMaximumMessageCount,
		uint64(messageCount),
	)
	updateH3DatagramMaximum(
		&self.hybridStreamQueueMaximumByteCount,
		uint64(retainedByteCount),
	)
}

func (self *H3DatagramStats) recordHybridStreamQueueRelease(byteCount int) {
	if self == nil {
		return
	}
	if remaining := self.hybridStreamQueueCurrentByteCount.Add(-int64(byteCount)); remaining < 0 {
		panic("negative H3 hybrid stream queue byte count")
	}
	if remaining := self.hybridStreamQueueCurrentMessageCount.Add(-1); remaining < 0 {
		panic("negative H3 hybrid stream queue message count")
	}
}

func (self *H3DatagramStats) recordHybridStreamQueueWait(waitDuration time.Duration) {
	if self == nil {
		return
	}
	self.hybridStreamQueueWaitCount.Add(1)
	self.hybridStreamQueueWaitDuration.Add(uint64(max(time.Duration(0), waitDuration)))
}

func (self *H3DatagramStats) recordHybridStreamQueueOversize() {
	if self != nil {
		self.hybridStreamQueueOversizeCount.Add(1)
	}
}

// H3HybridStreamSendBudget bounds pooled ownership between the H3 lane
// dispatcher and reliable-stream writer. Unlike a channel capacity alone, it
// counts the backing allocation retained by every queued, pending, or actively
// written message. A release wakes all blocked admissions so cancellation and
// changing message sizes cannot strand the dispatcher behind a stale token.
type H3HybridStreamSendBudget struct {
	stateLock       sync.Mutex
	maxMessageCount int
	maxByteCount    int
	messageCount    int
	byteCount       int
	notify          chan struct{}
	stats           *H3DatagramStats
}

func NewH3HybridStreamSendBudget(
	maxMessageCount int,
	maxByteCount int,
	stats *H3DatagramStats,
) *H3HybridStreamSendBudget {
	if maxMessageCount <= 0 || maxByteCount <= 0 {
		panic(fmt.Sprintf(
			"invalid H3 hybrid stream queue limits messages=%d bytes=%d",
			maxMessageCount,
			maxByteCount,
		))
	}
	return &H3HybridStreamSendBudget{
		maxMessageCount: maxMessageCount,
		maxByteCount:    maxByteCount,
		notify:          make(chan struct{}),
		stats:           stats,
	}
}

func (self *H3HybridStreamSendBudget) MaxByteCount() int {
	return self.maxByteCount
}

// Acquire waits only on the send side. It never admits a message that would
// exceed either hard limit, and returns false on cancellation or when one
// backing allocation is itself larger than the entire queue budget.
func (self *H3HybridStreamSendBudget) Acquire(
	ctx context.Context,
	retainedByteCount int,
) bool {
	if retainedByteCount < 0 || self.maxByteCount < retainedByteCount {
		self.stats.recordHybridStreamQueueOversize()
		return false
	}
	var waitStart time.Time
	for {
		self.stateLock.Lock()
		if self.messageCount < self.maxMessageCount &&
			self.byteCount <= self.maxByteCount-retainedByteCount {
			self.messageCount += 1
			self.byteCount += retainedByteCount
			self.stateLock.Unlock()
			if !waitStart.IsZero() {
				self.stats.recordHybridStreamQueueWait(time.Since(waitStart))
			}
			self.stats.recordHybridStreamQueueAcquire(retainedByteCount)
			return true
		}
		notify := self.notify
		self.stateLock.Unlock()
		if waitStart.IsZero() {
			waitStart = time.Now()
		}
		select {
		case <-ctx.Done():
			self.stats.recordHybridStreamQueueWait(time.Since(waitStart))
			return false
		case <-notify:
		}
	}
}

// Release must be called exactly once for every successful Acquire, after the
// corresponding message has left all stream batches and pending slots.
func (self *H3HybridStreamSendBudget) Release(retainedByteCount int) {
	self.stateLock.Lock()
	if retainedByteCount < 0 || self.messageCount <= 0 || self.byteCount < retainedByteCount {
		self.stateLock.Unlock()
		panic(fmt.Sprintf(
			"invalid H3 hybrid stream queue release messages=%d bytes=%d release=%d",
			self.messageCount,
			self.byteCount,
			retainedByteCount,
		))
	}
	self.messageCount -= 1
	self.byteCount -= retainedByteCount
	close(self.notify)
	self.notify = make(chan struct{})
	self.stateLock.Unlock()
	self.stats.recordHybridStreamQueueRelease(retainedByteCount)
}

// H3HybridStreamRetainedByteCount measures the backing bytes held alive by a
// queued slice. MessagePool slices expose their complete size-class allocation
// (including metadata) through cap, so this is stricter than len(message).
func H3HybridStreamRetainedByteCount(message []byte) int {
	return cap(message)
}

// H3DatagramReassemblyBudget bounds incomplete message storage across every
// connection owned by one higher-level transport or server handler.
type H3DatagramReassemblyBudget struct {
	limit int64
	used  atomic.Int64
}

// Creates a shared process/handler budget. The settings validator rejects a
// limit smaller than one complete message.
func NewH3DatagramReassemblyBudget(limit int64) *H3DatagramReassemblyBudget {
	return &H3DatagramReassemblyBudget{limit: limit}
}

// Tries to reserve bytes without ever exceeding the configured limit.
func (self *H3DatagramReassemblyBudget) tryReserve(byteCount int) bool {
	if self == nil || byteCount < 0 {
		return false
	}
	for {
		used := self.used.Load()
		if self.limit-used < int64(byteCount) {
			return false
		}
		if self.used.CompareAndSwap(used, used+int64(byteCount)) {
			return true
		}
	}
}

// Releases a reservation after completion, rejection, expiry, or close.
func (self *H3DatagramReassemblyBudget) release(byteCount int) {
	if self == nil || byteCount < 0 {
		return
	}
	if used := self.used.Add(-int64(byteCount)); used < 0 {
		panic(fmt.Errorf("H3 DATAGRAM reassembly budget became negative: %d", used))
	}
}

// Used returns the currently reserved incomplete-message bytes.
func (self *H3DatagramReassemblyBudget) Used() int64 {
	if self == nil {
		return 0
	}
	return self.used.Load()
}

// H3DatagramFragmenter serializes complete Transfer frames into versioned
// datagrams. It is owned by one sender goroutine and is not concurrency-safe.
type H3DatagramFragmenter struct {
	settings      *H3DatagramSettings
	stats         *H3DatagramStats
	nextMessageId uint64
	scratch       []byte
}

// Creates a connection-generation sender after validating all hard limits.
func NewH3DatagramFragmenter(settings *H3DatagramSettings, stats *H3DatagramStats) (*H3DatagramFragmenter, error) {
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	if stats == nil {
		stats = &H3DatagramStats{}
	}
	return &H3DatagramFragmenter{settings: settings, stats: stats}, nil
}

// Send emits every fragment through the provided blocking sender. quic-go
// copies each slice before returning, so one bounded scratch buffer is reused.
// A failure may follow earlier fragments; Transfer will retry the whole frame
// with a new message id.
func (self *H3DatagramFragmenter) send(
	message []byte,
	maxDatagramByteCount int,
	send func(datagram []byte) error,
) (fragmentCount int, err error) {
	if len(message) < 1 || self.settings.MaxMessageByteCount < len(message) {
		return 0, ErrH3DatagramMessageTooLarge
	}
	fragmentPayloadByteCount := maxDatagramByteCount - H3DatagramHeaderByteCount
	if fragmentPayloadByteCount < 1 {
		return 0, ErrH3DatagramMessageTooLarge
	}
	fragmentCount = (len(message) + fragmentPayloadByteCount - 1) / fragmentPayloadByteCount
	if self.settings.MaxFragmentCount < fragmentCount || int(^uint16(0)) < fragmentCount {
		return 0, ErrH3DatagramMessageTooLarge
	}

	self.nextMessageId += 1
	if self.nextMessageId == 0 {
		self.nextMessageId += 1
	}
	messageId := self.nextMessageId
	checksum := crc32.ChecksumIEEE(message)
	if cap(self.scratch) < maxDatagramByteCount {
		self.scratch = make([]byte, maxDatagramByteCount)
	}

	for fragmentIndex := range fragmentCount {
		offset := fragmentIndex * fragmentPayloadByteCount
		end := min(len(message), offset+fragmentPayloadByteCount)
		datagram := self.scratch[:H3DatagramHeaderByteCount+end-offset]
		copy(datagram[0:3], h3DatagramMagic[:])
		datagram[3] = byte(H3DatagramProtocolVersion)
		binary.BigEndian.PutUint64(datagram[4:12], messageId)
		binary.BigEndian.PutUint32(datagram[12:16], uint32(len(message)))
		binary.BigEndian.PutUint32(datagram[16:20], checksum)
		binary.BigEndian.PutUint32(datagram[20:24], uint32(offset))
		binary.BigEndian.PutUint16(datagram[24:26], uint16(fragmentIndex))
		binary.BigEndian.PutUint16(datagram[26:28], uint16(fragmentCount))
		copy(datagram[H3DatagramHeaderByteCount:], message[offset:end])
		if err := send(datagram); err != nil {
			return fragmentIndex, err
		}
		self.stats.sentFragmentCount.Add(1)
		self.stats.sentByteCount.Add(uint64(len(datagram)))
	}
	self.stats.sentMessageCount.Add(1)
	self.stats.sentMessageByteCount.Add(uint64(len(message)))
	return fragmentCount, nil
}

// Send emits one complete Transfer frame through the bounded DATAGRAM carrier.
// Public callers observe any failed carrier attempt in SendErrorCount.
func (self *H3DatagramFragmenter) Send(
	message []byte,
	maxDatagramByteCount int,
	send func(datagram []byte) error,
) (fragmentCount int, err error) {
	fragmentCount, err = self.send(message, maxDatagramByteCount, send)
	if err != nil {
		self.stats.sendErrorCount.Add(1)
	}
	return fragmentCount, err
}

// SendHybrid sends one complete Transfer frame through a bounded number of
// DATAGRAMs at the current path limit. quic-go rejects an oversized DATAGRAM
// synchronously before queueing it; that result is path-size discovery, not a
// failed application send. The lower limit is retained and the complete frame
// is retried once under a new message id, or moved to the reliable stream when
// it no longer fits the bounded packet lane.
func (self *H3DatagramFragmenter) SendHybrid(
	message []byte,
	maxDatagramByteCount int,
	send func(datagram []byte) error,
) (useStream bool, nextMaxDatagramByteCount int, err error) {
	nextMaxDatagramByteCount = maxDatagramByteCount
	if !self.settings.UseDatagramForPath(
		len(message),
		maxDatagramByteCount,
	) {
		return true, nextMaxDatagramByteCount, nil
	}
	if _, err = self.send(message, maxDatagramByteCount, send); err == nil {
		return false, nextMaxDatagramByteCount, nil
	}

	var tooLargeErr *quic.DatagramTooLargeError
	if !errors.As(err, &tooLargeErr) ||
		int(tooLargeErr.MaxDatagramPayloadSize) <= H3DatagramHeaderByteCount ||
		maxDatagramByteCount <= int(tooLargeErr.MaxDatagramPayloadSize) {
		self.stats.sendErrorCount.Add(1)
		return false, nextMaxDatagramByteCount, err
	}
	nextMaxDatagramByteCount = int(tooLargeErr.MaxDatagramPayloadSize)
	if !self.settings.UseDatagramForPath(
		len(message),
		nextMaxDatagramByteCount,
	) {
		return true, nextMaxDatagramByteCount, nil
	}
	_, err = self.send(message, nextMaxDatagramByteCount, send)
	if err != nil {
		self.stats.sendErrorCount.Add(1)
	}
	return false, nextMaxDatagramByteCount, err
}

type h3DatagramFragmentSlot struct {
	offset    int
	byteCount int
	received  bool
}

type h3DatagramPendingMessage struct {
	messageId      uint64
	totalByteCount int
	checksum       uint32
	fragmentCount  int
	receivedCount  int
	expiresAt      time.Time
	message        []byte
	fragments      []h3DatagramFragmentSlot
}

// H3DatagramReassembler accepts reordered and duplicated fragments while
// bounding all retained bytes. Methods are safe for concurrent expiry and
// receive calls. A returned message is pooled and transfers ownership to the
// caller, which must eventually call MessagePoolReturn.
type H3DatagramReassembler struct {
	settings     *H3DatagramSettings
	globalBudget *H3DatagramReassemblyBudget
	stats        *H3DatagramStats

	stateLock                sync.Mutex
	pendingMessageIdMessages map[uint64]*h3DatagramPendingMessage
	reassemblyByteCount      int
	retiredMessageIds        map[uint64]bool
	retiredMessageIdOrder    []uint64
	retiredMessageIdOffset   int
	closed                   bool
}

// Creates a connection-generation receiver after validating both local and
// shared byte limits.
func NewH3DatagramReassembler(
	settings *H3DatagramSettings,
	globalBudget *H3DatagramReassemblyBudget,
	stats *H3DatagramStats,
) (*H3DatagramReassembler, error) {
	if err := settings.Validate(); err != nil {
		return nil, err
	}
	if globalBudget == nil || globalBudget.limit < settings.ProcessReassemblyByteCount {
		return nil, fmt.Errorf("missing or undersized H3 DATAGRAM shared budget")
	}
	if stats == nil {
		stats = &H3DatagramStats{}
	}
	return &H3DatagramReassembler{
		settings:                 settings,
		globalBudget:             globalBudget,
		stats:                    stats,
		pendingMessageIdMessages: map[uint64]*h3DatagramPendingMessage{},
		retiredMessageIds:        map[uint64]bool{},
		retiredMessageIdOrder:    make([]uint64, defaultH3DatagramReplayIds),
	}, nil
}

// Removes one incomplete message from local state and returns its external
// storage owner. The caller holds stateLock.
func (self *H3DatagramReassembler) removePendingWithLock(messageId uint64) *h3DatagramPendingMessage {
	pending := self.pendingMessageIdMessages[messageId]
	if pending == nil {
		return nil
	}
	delete(self.pendingMessageIdMessages, messageId)
	self.reassemblyByteCount -= pending.totalByteCount
	return pending
}

// Releases storage only after the caller has dropped stateLock.
func (self *H3DatagramReassembler) releasePending(pending *h3DatagramPendingMessage) {
	if pending == nil {
		return
	}
	self.globalBudget.release(pending.totalByteCount)
	if pending.message != nil {
		MessagePoolReturn(pending.message)
	}
}

// Collects expired local owners without calling the pool or shared budget. The
// caller holds stateLock.
func (self *H3DatagramReassembler) expireWithLock(now time.Time) []*h3DatagramPendingMessage {
	var expiredPendingMessages []*h3DatagramPendingMessage
	for messageId, pending := range self.pendingMessageIdMessages {
		if !now.Before(pending.expiresAt) {
			self.rememberRetiredWithLock(messageId)
			expiredPendingMessages = append(
				expiredPendingMessages,
				self.removePendingWithLock(messageId),
			)
		}
	}
	return expiredPendingMessages
}

// Remembers a bounded set of completed, expired, or corrupt ids so late
// fragments do not resurrect a retired carrier message. Transfer itself remains
// the final duplicate authority after an id ages out of this small window. The
// caller holds stateLock.
func (self *H3DatagramReassembler) rememberRetiredWithLock(messageId uint64) {
	if self.retiredMessageIds[messageId] {
		return
	}
	oldMessageId := self.retiredMessageIdOrder[self.retiredMessageIdOffset]
	if oldMessageId != 0 {
		delete(self.retiredMessageIds, oldMessageId)
	}
	self.retiredMessageIdOrder[self.retiredMessageIdOffset] = messageId
	self.retiredMessageIdOffset = (self.retiredMessageIdOffset + 1) % len(self.retiredMessageIdOrder)
	self.retiredMessageIds[messageId] = true
}

// Expire releases messages at their original deadline. Duplicate fragments do
// not move that deadline.
func (self *H3DatagramReassembler) Expire(now time.Time) {
	var expiredPendingMessages []*h3DatagramPendingMessage
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		expiredPendingMessages = self.expireWithLock(now)
	}()
	for _, pending := range expiredPendingMessages {
		self.releasePending(pending)
	}
	if 0 < len(expiredPendingMessages) {
		self.stats.reassemblyTimeoutCount.Add(uint64(len(expiredPendingMessages)))
	}
}

// Accept validates one authenticated QUIC DATAGRAM and returns a complete
// Transfer frame only after exact non-overlapping coverage and checksum pass.
// Malformed or incomplete input is dropped without terminating the carrier.
func (self *H3DatagramReassembler) Accept(datagram []byte, now time.Time) []byte {
	self.stats.receivedFragmentCount.Add(1)
	self.stats.receivedByteCount.Add(uint64(len(datagram)))
	if len(datagram) <= H3DatagramHeaderByteCount ||
		!bytes.Equal(datagram[0:3], h3DatagramMagic[:]) ||
		datagram[3] != byte(H3DatagramProtocolVersion) {
		self.stats.malformedFragmentCount.Add(1)
		return nil
	}

	messageId := binary.BigEndian.Uint64(datagram[4:12])
	totalByteCount := int(binary.BigEndian.Uint32(datagram[12:16]))
	checksum := binary.BigEndian.Uint32(datagram[16:20])
	offset := int(binary.BigEndian.Uint32(datagram[20:24]))
	fragmentIndex := int(binary.BigEndian.Uint16(datagram[24:26]))
	fragmentCount := int(binary.BigEndian.Uint16(datagram[26:28]))
	payload := datagram[H3DatagramHeaderByteCount:]
	if messageId == 0 || totalByteCount < 1 || self.settings.MaxMessageByteCount < totalByteCount ||
		fragmentCount < 1 || self.settings.MaxFragmentCount < fragmentCount ||
		fragmentCount <= fragmentIndex || offset < 0 || totalByteCount <= offset ||
		totalByteCount-offset < len(payload) {
		self.stats.malformedFragmentCount.Add(1)
		return nil
	}

	for {
		var expiredPendingMessages []*h3DatagramPendingMessage
		var removedPendingMessage *h3DatagramPendingMessage
		var message []byte
		closed := false
		duplicate := false
		malformed := false
		checksumFailure := false
		limitRefusal := false
		needsPendingMessage := false

		func() {
			self.stateLock.Lock()
			defer self.stateLock.Unlock()
			expiredPendingMessages = self.expireWithLock(now)
			if self.closed {
				closed = true
				return
			}
			if self.retiredMessageIds[messageId] {
				duplicate = true
				return
			}

			pending := self.pendingMessageIdMessages[messageId]
			if pending == nil {
				if self.settings.MaxReassemblyMessageCount <= len(self.pendingMessageIdMessages) ||
					self.settings.MaxReassemblyByteCount-self.reassemblyByteCount < totalByteCount {
					limitRefusal = true
					return
				}
				needsPendingMessage = true
				return
			}
			if pending.totalByteCount != totalByteCount || pending.checksum != checksum || pending.fragmentCount != fragmentCount {
				removedPendingMessage = self.removePendingWithLock(messageId)
				self.rememberRetiredWithLock(messageId)
				malformed = true
				return
			}

			slot := &pending.fragments[fragmentIndex]
			if slot.received {
				if slot.offset == offset && slot.byteCount == len(payload) &&
					bytes.Equal(pending.message[offset:offset+len(payload)], payload) {
					duplicate = true
					return
				}
				removedPendingMessage = self.removePendingWithLock(messageId)
				self.rememberRetiredWithLock(messageId)
				malformed = true
				return
			}
			copy(pending.message[offset:offset+len(payload)], payload)
			slot.offset = offset
			slot.byteCount = len(payload)
			slot.received = true
			pending.receivedCount += 1
			if pending.receivedCount != pending.fragmentCount {
				return
			}

			end := 0
			for _, fragment := range pending.fragments {
				if !fragment.received || fragment.offset != end || fragment.byteCount < 1 {
					removedPendingMessage = self.removePendingWithLock(messageId)
					self.rememberRetiredWithLock(messageId)
					malformed = true
					return
				}
				end += fragment.byteCount
			}
			if end != pending.totalByteCount {
				removedPendingMessage = self.removePendingWithLock(messageId)
				self.rememberRetiredWithLock(messageId)
				malformed = true
				return
			}
			if crc32.ChecksumIEEE(pending.message) != pending.checksum {
				removedPendingMessage = self.removePendingWithLock(messageId)
				self.rememberRetiredWithLock(messageId)
				checksumFailure = true
				return
			}

			removedPendingMessage = self.removePendingWithLock(messageId)
			message = removedPendingMessage.message
			removedPendingMessage.message = nil
			self.rememberRetiredWithLock(messageId)
		}()

		for _, pending := range expiredPendingMessages {
			self.releasePending(pending)
		}
		if 0 < len(expiredPendingMessages) {
			self.stats.reassemblyTimeoutCount.Add(uint64(len(expiredPendingMessages)))
		}
		self.releasePending(removedPendingMessage)
		if closed {
			return nil
		}
		if duplicate {
			self.stats.duplicateFragmentCount.Add(1)
			return nil
		}
		if malformed {
			self.stats.malformedFragmentCount.Add(1)
			return nil
		}
		if checksumFailure {
			self.stats.checksumFailureCount.Add(1)
			return nil
		}
		if limitRefusal {
			self.stats.reassemblyLimitCount.Add(1)
			return nil
		}
		if message != nil {
			self.stats.receivedMessageCount.Add(1)
			self.stats.receivedMessageByteCount.Add(uint64(len(message)))
			return message
		}
		if !needsPendingMessage {
			return nil
		}

		if !self.globalBudget.tryReserve(totalByteCount) {
			self.stats.reassemblyLimitCount.Add(1)
			return nil
		}
		candidate := &h3DatagramPendingMessage{
			messageId:      messageId,
			totalByteCount: totalByteCount,
			checksum:       checksum,
			fragmentCount:  fragmentCount,
			expiresAt:      now.Add(self.settings.ReassemblyTimeout),
			message:        MessagePoolGet(totalByteCount),
			fragments:      make([]h3DatagramFragmentSlot, fragmentCount),
		}
		installed := false
		candidateClosed := false
		candidateDuplicate := false
		candidateLimitRefusal := false
		func() {
			self.stateLock.Lock()
			defer self.stateLock.Unlock()
			if self.closed {
				candidateClosed = true
				return
			}
			if self.retiredMessageIds[messageId] {
				candidateDuplicate = true
				return
			}
			if self.pendingMessageIdMessages[messageId] != nil {
				return
			}
			if self.settings.MaxReassemblyMessageCount <= len(self.pendingMessageIdMessages) ||
				self.settings.MaxReassemblyByteCount-self.reassemblyByteCount < totalByteCount {
				candidateLimitRefusal = true
				return
			}
			self.pendingMessageIdMessages[messageId] = candidate
			self.reassemblyByteCount += totalByteCount
			installed = true
		}()
		if !installed {
			self.releasePending(candidate)
			if candidateClosed {
				return nil
			}
			if candidateDuplicate {
				self.stats.duplicateFragmentCount.Add(1)
				return nil
			}
			if candidateLimitRefusal {
				self.stats.reassemblyLimitCount.Add(1)
				return nil
			}
		}
		// Process this fragment against the installed or concurrently created
		// message while retaining the same original expiry timestamp.
	}
}

// Close releases every incomplete pooled message and shared reservation. It is
// idempotent and prevents later input from allocating again.
func (self *H3DatagramReassembler) Close() {
	var pendingMessages []*h3DatagramPendingMessage
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		if self.closed {
			return
		}
		self.closed = true
		for messageId := range self.pendingMessageIdMessages {
			pendingMessages = append(
				pendingMessages,
				self.removePendingWithLock(messageId),
			)
		}
	}()
	for _, pending := range pendingMessages {
		self.releasePending(pending)
	}
}
