// This file defines the native P2P datagram carrier's wire fragmentation,
// bounded reassembly, selection controls, and observable counters. The carrier
// moves an already end-to-end-encrypted TransferFrame; it never decrypts the
// application payload at an intermediary stream hop.
package connect

import (
	"context"
	"encoding/binary"
	"errors"
	"sync/atomic"
	"time"
)

const (
	p2pFastPathFragmentHeaderByteCount = 16
	// A full fragment must fit IPv6's 1,280-byte minimum link MTU. The wire
	// envelope is IPv6(40) + UDP(8) + RTP(12) + SRTP GCM tag(16) + this
	// carrier header(16), leaving 1,188 bytes for the transfer payload.
	p2pFastPathFragmentPayloadByteCount = 1188
	p2pFastPathMaximumFragmentCount     = 64
	p2pFastPathReassemblySlotCount      = 64
	p2pFastPathReassemblyTimeout        = 2 * time.Second
	p2pFastPathWarmupInterval           = 25 * time.Millisecond
	p2pFastPathWarmupTimeout            = 5 * time.Second
	p2pFastPathRtpClockRate             = 90000
	p2pFastPathRtpPayloadType           = 127
	// The unchanged codec name lets mixed-version peers establish WebRTC and
	// use SCTP. The versioned warmup below prevents them from selecting fast
	// carriers with incompatible fragment geometry.
	p2pFastPathMimeType = "video/urnetwork-fast-path"
	p2pFastPathVersion  = 2
)

var (
	errP2pFastPathMessageTooLarge = errors.New("p2p fast-path message is too large")
	errP2pFastPathNotReady        = errors.New("p2p fast-path carrier is not ready")
	errP2pFastPathPacket          = errors.New("invalid p2p fast-path packet")
)

// P2pDataPlaneMode controls whether a P2P route requires, forbids, or
// automatically selects the negotiated datagram carrier.
type P2pDataPlaneMode int

const (
	P2pDataPlaneModeAuto P2pDataPlaneMode = iota
	P2pDataPlaneModeLegacyOnly
	P2pDataPlaneModeFastOnly
)

// P2pDataPlaneStatsSnapshot is an immutable view of P2P carrier use. Message
// counts refer to complete TransferFrame messages; fragment counts refer to
// independently authenticated SRTP datagrams.
type P2pDataPlaneStatsSnapshot struct {
	ActiveSendRouteCount            int64
	ActiveReceiveRouteCount         int64
	FastSendMessageCount            uint64
	FastSendByteCount               uint64
	FastSendFragmentCount           uint64
	FastReceiveMessageCount         uint64
	FastReceiveByteCount            uint64
	FastReceiveFragmentCount        uint64
	LegacySendMessageCount          uint64
	LegacySendByteCount             uint64
	LegacyReceiveMessageCount       uint64
	LegacyReceiveByteCount          uint64
	LegacyReceiveQueueDropCount     uint64
	LegacyReceiveQueueDropByteCount uint64
	FastReceiveQueueDropCount       uint64
	FastReceiveQueueDropByteCount   uint64
	FastFallbackCount               uint64
	FastDropCount                   uint64
	// FLIGHTGATEFIX §8 (M5). Fragments per sent message, bucketed
	// 1, 2-4, 5-8, 9-16, 17+; one lost fragment loses the whole message.
	FastSendFragmentHistogram [p2pFastPathFragmentHistogramBucketCount]uint64
	// Incomplete reassembly slots discarded on expiry or slot reuse.
	FastReassemblyEvictionCount uint64
}

const p2pFastPathFragmentHistogramBucketCount = 5

// p2pFastPathFragmentHistogramBucket maps a fragment count to its bucket.
func p2pFastPathFragmentHistogramBucket(fragmentCount int) int {
	switch {
	case fragmentCount <= 1:
		return 0
	case fragmentCount <= 4:
		return 1
	case fragmentCount <= 8:
		return 2
	case fragmentCount <= 16:
		return 3
	}
	return 4
}

// P2pDataPlaneStats holds lock-free counters shared by all P2P streams owned
// by one client settings tree.
type P2pDataPlaneStats struct {
	activeSendRouteCount            atomic.Int64
	activeReceiveRouteCount         atomic.Int64
	fastSendMessageCount            atomic.Uint64
	fastSendByteCount               atomic.Uint64
	fastSendFragmentCount           atomic.Uint64
	fastReceiveMessageCount         atomic.Uint64
	fastReceiveByteCount            atomic.Uint64
	fastReceiveFragmentCount        atomic.Uint64
	legacySendMessageCount          atomic.Uint64
	legacySendByteCount             atomic.Uint64
	legacyReceiveMessageCount       atomic.Uint64
	legacyReceiveByteCount          atomic.Uint64
	legacyReceiveQueueDropCount     atomic.Uint64
	legacyReceiveQueueDropByteCount atomic.Uint64
	fastReceiveQueueDropCount       atomic.Uint64
	fastReceiveQueueDropByteCount   atomic.Uint64
	fastFallbackCount               atomic.Uint64
	fastDropCount                   atomic.Uint64
	fastSendFragmentHistogram       [p2pFastPathFragmentHistogramBucketCount]atomic.Uint64
	fastReassemblyEvictionCount     atomic.Uint64
}

// observeFastSendFragments buckets one sent message by its fragment count.
func (self *P2pDataPlaneStats) observeFastSendFragments(fragmentCount int) {
	if self == nil {
		return
	}
	self.fastSendFragmentHistogram[p2pFastPathFragmentHistogramBucket(fragmentCount)].Add(1)
}

// Snapshot reads a consistent-enough lock-free view without stopping packet
// processing. Active route gauges can move in either direction; traffic
// counters are monotonic.
func (self *P2pDataPlaneStats) Snapshot() P2pDataPlaneStatsSnapshot {
	if self == nil {
		return P2pDataPlaneStatsSnapshot{}
	}
	snapshot := P2pDataPlaneStatsSnapshot{
		ActiveSendRouteCount:            self.activeSendRouteCount.Load(),
		ActiveReceiveRouteCount:         self.activeReceiveRouteCount.Load(),
		FastSendMessageCount:            self.fastSendMessageCount.Load(),
		FastSendByteCount:               self.fastSendByteCount.Load(),
		FastSendFragmentCount:           self.fastSendFragmentCount.Load(),
		FastReceiveMessageCount:         self.fastReceiveMessageCount.Load(),
		FastReceiveByteCount:            self.fastReceiveByteCount.Load(),
		FastReceiveFragmentCount:        self.fastReceiveFragmentCount.Load(),
		LegacySendMessageCount:          self.legacySendMessageCount.Load(),
		LegacySendByteCount:             self.legacySendByteCount.Load(),
		LegacyReceiveMessageCount:       self.legacyReceiveMessageCount.Load(),
		LegacyReceiveByteCount:          self.legacyReceiveByteCount.Load(),
		LegacyReceiveQueueDropCount:     self.legacyReceiveQueueDropCount.Load(),
		LegacyReceiveQueueDropByteCount: self.legacyReceiveQueueDropByteCount.Load(),
		FastReceiveQueueDropCount:       self.fastReceiveQueueDropCount.Load(),
		FastReceiveQueueDropByteCount:   self.fastReceiveQueueDropByteCount.Load(),
		FastFallbackCount:               self.fastFallbackCount.Load(),
		FastDropCount:                   self.fastDropCount.Load(),
		FastReassemblyEvictionCount:     self.fastReassemblyEvictionCount.Load(),
	}
	for bucket := range snapshot.FastSendFragmentHistogram {
		snapshot.FastSendFragmentHistogram[bucket] = self.fastSendFragmentHistogram[bucket].Load()
	}
	return snapshot
}

// A p2pFastPathFragmentHeader precedes every RTP payload. Every fragment
// repeats the total length so reassembly can begin after reordering.
type p2pFastPathFragmentHeader struct {
	messageId     uint32
	messageLength int
	fragmentIndex int
	fragmentCount int
}

// p2pFastPathFragmentCount returns the exact number of independently carried
// fragments needed for one message.
func p2pFastPathFragmentCount(messageByteCount int) int {
	return (messageByteCount + p2pFastPathFragmentPayloadByteCount - 1) /
		p2pFastPathFragmentPayloadByteCount
}

// writeP2pFastPathFragmentHeader serializes one fixed-size routing header.
func writeP2pFastPathFragmentHeader(
	packet []byte,
	header p2pFastPathFragmentHeader,
) error {
	if len(packet) < p2pFastPathFragmentHeaderByteCount ||
		header.messageId == 0 ||
		header.messageLength <= 0 ||
		header.fragmentCount <= 0 ||
		p2pFastPathMaximumFragmentCount < header.fragmentCount ||
		header.fragmentIndex < 0 ||
		header.fragmentCount <= header.fragmentIndex {
		return errP2pFastPathPacket
	}
	packet[0] = 'U'
	packet[1] = 'R'
	packet[2] = 'D'
	packet[3] = p2pFastPathVersion
	binary.BigEndian.PutUint32(packet[4:8], header.messageId)
	binary.BigEndian.PutUint32(packet[8:12], uint32(header.messageLength))
	binary.BigEndian.PutUint16(packet[12:14], uint16(header.fragmentIndex))
	binary.BigEndian.PutUint16(packet[14:16], uint16(header.fragmentCount))
	return nil
}

// parseP2pFastPathFragmentHeader validates one fixed-size routing header.
func parseP2pFastPathFragmentHeader(packet []byte) (p2pFastPathFragmentHeader, error) {
	if len(packet) <= p2pFastPathFragmentHeaderByteCount ||
		packet[0] != 'U' ||
		packet[1] != 'R' ||
		packet[2] != 'D' ||
		packet[3] != p2pFastPathVersion {
		return p2pFastPathFragmentHeader{}, errP2pFastPathPacket
	}
	messageLength := binary.BigEndian.Uint32(packet[8:12])
	if uint64(messageLength) > uint64(^uint(0)>>1) {
		return p2pFastPathFragmentHeader{}, errP2pFastPathPacket
	}
	header := p2pFastPathFragmentHeader{
		messageId:     binary.BigEndian.Uint32(packet[4:8]),
		messageLength: int(messageLength),
		fragmentIndex: int(binary.BigEndian.Uint16(packet[12:14])),
		fragmentCount: int(binary.BigEndian.Uint16(packet[14:16])),
	}
	if header.messageId == 0 ||
		header.messageLength <= 0 ||
		header.fragmentCount != p2pFastPathFragmentCount(header.messageLength) ||
		p2pFastPathMaximumFragmentCount < header.fragmentCount ||
		header.fragmentIndex < 0 ||
		header.fragmentCount <= header.fragmentIndex {
		return p2pFastPathFragmentHeader{}, errP2pFastPathPacket
	}
	fragmentByteCount := len(packet) - p2pFastPathFragmentHeaderByteCount
	expectedFragmentByteCount := min(
		p2pFastPathFragmentPayloadByteCount,
		header.messageLength-header.fragmentIndex*p2pFastPathFragmentPayloadByteCount,
	)
	if fragmentByteCount != expectedFragmentByteCount {
		return p2pFastPathFragmentHeader{}, errP2pFastPathPacket
	}
	return header, nil
}

// One p2pFastPathReassemblySlot owns a pooled complete-message buffer until
// all fragments arrive, the slot collides, or its deadline expires.
type p2pFastPathReassemblySlot struct {
	messageId      uint32
	message        []byte
	fragmentCount  int
	receivedBits   uint64
	receivedCount  int
	expirationTime time.Time
}

// p2pFastPathReassembler bounds incomplete messages without a map allocation
// on the receive path. Message ids select fixed slots; a collision drops only
// the older incomplete message.
type p2pFastPathReassembler struct {
	maximumMessageByteCount int
	slots                   [p2pFastPathReassemblySlotCount]p2pFastPathReassemblySlot
	// dataPlaneStats, when set, counts incomplete messages this reassembler
	// discards on slot reuse or expiry.
	dataPlaneStats *P2pDataPlaneStats

	// Tests retain the exact allocated buffer before ownership can move to the
	// complete-message queue. Nil is a production no-op.
	afterMessageAllocatedForTest func([]byte)
}

// newP2pFastPathReassembler creates a generation-local reassembler.
func newP2pFastPathReassembler(maximumMessageByteCount int) *p2pFastPathReassembler {
	return &p2pFastPathReassembler{
		maximumMessageByteCount: maximumMessageByteCount,
	}
}

// clearP2pFastPathReassemblySlot releases any incomplete owning buffer.
func clearP2pFastPathReassemblySlot(slot *p2pFastPathReassemblySlot) {
	if slot.message != nil {
		MessagePoolReturn(slot.message)
	}
	*slot = p2pFastPathReassemblySlot{}
}

// accept copies one authenticated fragment and returns a complete pooled
// message when the final missing fragment arrives.
func (self *p2pFastPathReassembler) accept(packet []byte, now time.Time) ([]byte, error) {
	header, err := parseP2pFastPathFragmentHeader(packet)
	if err != nil || self.maximumMessageByteCount < header.messageLength {
		return nil, errP2pFastPathPacket
	}
	slot := &self.slots[int(header.messageId)%len(self.slots)]
	if slot.messageId != 0 &&
		(slot.messageId != header.messageId || slot.expirationTime.Before(now)) {
		if slot.message != nil && self.dataPlaneStats != nil {
			self.dataPlaneStats.fastReassemblyEvictionCount.Add(1)
		}
		clearP2pFastPathReassemblySlot(slot)
	}
	if slot.messageId == 0 {
		*slot = p2pFastPathReassemblySlot{
			messageId:      header.messageId,
			message:        MessagePoolGet(header.messageLength),
			fragmentCount:  header.fragmentCount,
			expirationTime: now.Add(p2pFastPathReassemblyTimeout),
		}
		if self.afterMessageAllocatedForTest != nil {
			self.afterMessageAllocatedForTest(slot.message)
		}
	}
	if len(slot.message) != header.messageLength ||
		slot.fragmentCount != header.fragmentCount {
		clearP2pFastPathReassemblySlot(slot)
		return nil, errP2pFastPathPacket
	}
	bit := uint64(1) << uint(header.fragmentIndex)
	if slot.receivedBits&bit != 0 {
		return nil, nil
	}
	fragment := packet[p2pFastPathFragmentHeaderByteCount:]
	offset := header.fragmentIndex * p2pFastPathFragmentPayloadByteCount
	copy(slot.message[offset:offset+len(fragment)], fragment)
	slot.receivedBits |= bit
	slot.receivedCount += 1
	if slot.receivedCount != slot.fragmentCount {
		return nil, nil
	}
	message := slot.message
	slot.message = nil
	*slot = p2pFastPathReassemblySlot{}
	return message, nil
}

// close releases every incomplete message owned by the reassembler.
func (self *p2pFastPathReassembler) close() {
	for slotIndex := range self.slots {
		clearP2pFastPathReassemblySlot(&self.slots[slotIndex])
	}
}

// webRtcFastPathConn is the optional native capability used by P2P transport.
// Browser and old connection implementations simply do not implement it.
type webRtcFastPathConn interface {
	FastPathReady() bool
	WaitFastPathReady(ctx context.Context, timeout time.Duration) bool
	WriteFastPathMessage(message []byte) (fragmentCount int, err error)
	FastPathMessages() <-chan p2pFastPathReceivedMessage
}

// p2pFastPathReceivedMessage transfers ownership of one pooled, completely
// reassembled message from the WebRTC receiver to the P2P route worker.
type p2pFastPathReceivedMessage struct {
	message       []byte
	fragmentCount int
}
