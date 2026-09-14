package connect

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// IPv4 fragmentation is required at the product MTU: QUIC Initial datagrams
// are at least 1,200 bytes, while the tunnel carries 1,100-byte IP packets.
// Reassembly is deliberately small and local to one NAT send shard. It is not
// a general-purpose fragment cache: the limits below bound both benign bursts
// and adversarial incomplete datagrams.
//
// The IPv6 half (Fragment extension header, RFC 8200) lives in
// ip_ipv6_fragment.go with the same bounds. The family-agnostic surface --
// isIpFragmentPacket, ipFragmentGate, ipFragmentProcessResult and
// returnIpFragmentProcessResult -- dispatches to whichever reassembler the
// packet version needs; the pre-dual-stack v4 names are kept as aliases for
// callers that still gate on isIpv4FragmentPacket.
const (
	ipv4FragmentReassemblyTimeout          = 15 * time.Second
	ipv4FragmentReassemblyMaxDatagrams     = 16
	ipv4FragmentReassemblyMaxFragments     = 64
	ipv4FragmentReassemblyMaxRetainedBytes = 64 * 1024
	defaultUdpReadBufferByteCount          = 2048
)

var (
	errInvalidIpv4Packet = errors.New("invalid IPv4 packet")
	errIpv4MtuTooSmall   = errors.New("IPv4 MTU is too small for fragmentation")
)

type ipv4FragmentKey struct {
	source         TransferPath
	transferKey    TransferKey
	provideMode    protocol.ProvideMode
	sourceIp       [4]byte
	destinationIp  [4]byte
	protocol       byte
	identification uint16
}

type ipv4RetainedFragment struct {
	packet           []byte
	headerByteCount  int
	offset           int
	payloadByteCount int
}

func (self *ipv4RetainedFragment) payload() []byte {
	return self.packet[self.headerByteCount : self.headerByteCount+self.payloadByteCount]
}

type ipv4FragmentDatagram struct {
	createdAt             time.Time
	updatedAt             time.Time
	fragments             []ipv4RetainedFragment
	firstHeaderByteCount  int
	finalPayloadByteCount int
}

type ipv4FragmentReassembler struct {
	datagrams         map[ipv4FragmentKey]*ipv4FragmentDatagram
	retainedByteCount int
}

// ipFragmentProcessResult is the result of processing one packet through a
// fragment gate, for either family.
type ipFragmentProcessResult struct {
	// packet is the complete packet: the original input for an unfragmented
	// packet or a new owned reassembly for a completed fragment set.
	packet []byte
	// fragments is populated only when the caller requests the original
	// fragment owners on completion. They are sorted by fragment offset.
	fragments [][]byte
	fragment  bool
	accepted  bool
}

// the pre-dual-stack name
type ipv4FragmentProcessResult = ipFragmentProcessResult

// ipFragmentGate serializes zero-value fragment caches, one per family, for
// callers whose packet entry points may run concurrently. LocalUserNat uses
// one cache per already-serialized send shard and does not pay this lock.
type ipFragmentGate struct {
	mutex        sync.Mutex
	reassembler4 *ipv4FragmentReassembler
	reassembler6 *ipv6FragmentReassembler
}

// the pre-dual-stack name
type ipv4FragmentGate = ipFragmentGate

// processOwned reassembles a fragment of either family. An unfragmented
// packet comes back unchanged; a completed datagram comes back with the
// original fragment owners attached (see ipFragmentProcessResult).
func (self *ipFragmentGate) processOwned(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	packet []byte,
) ipFragmentProcessResult {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if 0 < len(packet) && packet[0]>>4 == 6 {
		if self.reassembler6 == nil {
			self.reassembler6 = newIpv6FragmentReassembler()
		}
		return self.reassembler6.processResultAt(
			source,
			transferKey,
			provideMode,
			packet,
			time.Now(),
			true,
		)
	}
	if self.reassembler4 == nil {
		self.reassembler4 = newIpv4FragmentReassembler()
	}
	return self.reassembler4.processResultAt(
		source,
		transferKey,
		provideMode,
		packet,
		time.Now(),
		true,
	)
}

func (self *ipFragmentGate) close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.reassembler4 != nil {
		self.reassembler4.close()
		self.reassembler4 = nil
	}
	if self.reassembler6 != nil {
		self.reassembler6.close()
		self.reassembler6 = nil
	}
}

func newIpv4FragmentReassembler() *ipv4FragmentReassembler {
	return &ipv4FragmentReassembler{
		datagrams: make(map[ipv4FragmentKey]*ipv4FragmentDatagram),
	}
}

func isIpv4FragmentPacket(packet []byte) bool {
	return Ipv4HeaderSizeWithoutExtensions <= len(packet) &&
		packet[0]>>4 == 4 &&
		binary.BigEndian.Uint16(packet[6:8])&0x3fff != 0
}

// isIpFragmentPacket reports whether the packet is a fragment of either
// family that must be reassembled before it is parsed.
func isIpFragmentPacket(packet []byte) bool {
	if len(packet) == 0 {
		return false
	}
	switch packet[0] >> 4 {
	case 4:
		return isIpv4FragmentPacket(packet)
	case 6:
		return isIpv6FragmentPacket(packet)
	default:
		return false
	}
}

func returnIpFragmentProcessResult(result ipFragmentProcessResult) {
	MessagePoolReturn(result.packet)
	for _, fragment := range result.fragments {
		MessagePoolReturn(fragment)
	}
}

// the pre-dual-stack name
func returnIpv4FragmentProcessResult(result ipFragmentProcessResult) {
	returnIpFragmentProcessResult(result)
}

// process consumes packet only when it is an IPv4 fragment. An unfragmented
// packet is returned unchanged. A nil result means the fragment was retained
// for completion or rejected and returned to the message pool.
func (self *ipv4FragmentReassembler) process(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	packet []byte,
) []byte {
	return self.processAt(source, transferKey, provideMode, packet, time.Now())
}

func (self *ipv4FragmentReassembler) processAt(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	packet []byte,
	now time.Time,
) []byte {
	return self.processResultAt(
		source,
		transferKey,
		provideMode,
		packet,
		now,
		false,
	).packet
}

func (self *ipv4FragmentReassembler) processResultAt(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	packet []byte,
	now time.Time,
	retainCompletedFragments bool,
) ipFragmentProcessResult {
	if len(packet) < Ipv4HeaderSizeWithoutExtensions || packet[0]>>4 != 4 {
		return ipFragmentProcessResult{packet: packet, accepted: true}
	}
	flagsAndOffset := binary.BigEndian.Uint16(packet[6:8])
	if flagsAndOffset&0x3fff == 0 {
		return ipFragmentProcessResult{packet: packet, accepted: true}
	}

	self.expire(now)

	key := ipv4FragmentKey{
		source:         source,
		transferKey:    transferKey,
		provideMode:    provideMode,
		protocol:       packet[9],
		identification: binary.BigEndian.Uint16(packet[4:6]),
	}
	copy(key.sourceIp[:], packet[12:16])
	copy(key.destinationIp[:], packet[16:20])

	drop := func() ipFragmentProcessResult {
		self.releaseDatagram(key)
		MessagePoolReturn(packet)
		return ipFragmentProcessResult{fragment: true}
	}

	// The reserved flag is invalid. gVisor marks locally fragmented datagrams
	// with DF as well as MF/offset, so DF cannot be treated as a rejection at
	// the product TUN boundary; the canonical reassembly clears both flags.
	if flagsAndOffset&0x8000 != 0 {
		return drop()
	}
	headerByteCount := int(packet[0]&0x0f) * 4
	totalByteCount := int(binary.BigEndian.Uint16(packet[2:4]))
	if headerByteCount < Ipv4HeaderSizeWithoutExtensions ||
		totalByteCount < headerByteCount || len(packet) < totalByteCount {
		return drop()
	}
	payloadByteCount := totalByteCount - headerByteCount
	moreFragments := flagsAndOffset&0x2000 != 0
	offset := int(flagsAndOffset&0x1fff) * 8
	if payloadByteCount == 0 ||
		(moreFragments && payloadByteCount%8 != 0) ||
		offset+payloadByteCount > 0xffff-Ipv4HeaderSizeWithoutExtensions {
		return drop()
	}

	datagram := self.datagrams[key]
	if datagram == nil {
		self.makeDatagramRoom(now)
		packetCost := cap(packet)
		if ipv4FragmentReassemblyMaxRetainedBytes < packetCost {
			MessagePoolReturn(packet)
			return ipFragmentProcessResult{fragment: true}
		}
		for self.retainedByteCount+packetCost > ipv4FragmentReassemblyMaxRetainedBytes {
			if !self.releaseOldestDatagram(ipv4FragmentKey{}) {
				MessagePoolReturn(packet)
				return ipFragmentProcessResult{fragment: true}
			}
		}
		datagram = &ipv4FragmentDatagram{
			createdAt:             now,
			updatedAt:             now,
			finalPayloadByteCount: -1,
		}
		self.datagrams[key] = datagram
	} else if self.retainedByteCount+cap(packet) > ipv4FragmentReassemblyMaxRetainedBytes {
		return drop()
	}

	fragmentEnd := offset + payloadByteCount
	fragmentPayload := packet[headerByteCount:totalByteCount]
	for _, retained := range datagram.fragments {
		retainedEnd := retained.offset + retained.payloadByteCount
		if offset == retained.offset && fragmentEnd == retainedEnd {
			// Exact retransmissions are harmless. Conflicting duplicates poison
			// the whole datagram just like every other overlap.
			if bytes.Equal(fragmentPayload, retained.payload()) {
				MessagePoolReturn(packet)
				datagram.updatedAt = now
				return ipFragmentProcessResult{fragment: true, accepted: true}
			}
			return drop()
		}
		if offset < retainedEnd && retained.offset < fragmentEnd {
			return drop()
		}
	}
	if len(datagram.fragments) >= ipv4FragmentReassemblyMaxFragments {
		return drop()
	}
	if !moreFragments {
		if datagram.finalPayloadByteCount >= 0 && datagram.finalPayloadByteCount != fragmentEnd {
			return drop()
		}
		// A final fragment that ends before a previously retained fragment
		// would otherwise let a contiguous prefix complete and then make the
		// outlying fragment write beyond the reassembled packet.
		for _, retained := range datagram.fragments {
			if fragmentEnd < retained.offset+retained.payloadByteCount {
				return drop()
			}
		}
		datagram.finalPayloadByteCount = fragmentEnd
	} else if datagram.finalPayloadByteCount >= 0 && datagram.finalPayloadByteCount <= fragmentEnd {
		return drop()
	}
	if offset == 0 {
		if datagram.firstHeaderByteCount != 0 && datagram.firstHeaderByteCount != headerByteCount {
			return drop()
		}
		datagram.firstHeaderByteCount = headerByteCount
	}
	if datagram.firstHeaderByteCount != 0 &&
		datagram.finalPayloadByteCount > 0xffff-datagram.firstHeaderByteCount {
		return drop()
	}

	// Retain the original pool allocation rather than copying each fragment.
	// cap is charged because it is the actual backing memory kept alive.
	packet = packet[:totalByteCount]
	datagram.fragments = append(datagram.fragments, ipv4RetainedFragment{
		packet:           packet,
		headerByteCount:  headerByteCount,
		offset:           offset,
		payloadByteCount: payloadByteCount,
	})
	datagram.updatedAt = now
	self.retainedByteCount += cap(packet)

	if datagram.firstHeaderByteCount == 0 || datagram.finalPayloadByteCount < 0 {
		return ipFragmentProcessResult{fragment: true, accepted: true}
	}
	slices.SortFunc(datagram.fragments, func(a ipv4RetainedFragment, b ipv4RetainedFragment) int {
		if a.offset < b.offset {
			return -1
		}
		if b.offset < a.offset {
			return 1
		}
		return 0
	})
	position := 0
	for _, fragment := range datagram.fragments {
		if fragment.offset != position {
			return ipFragmentProcessResult{fragment: true, accepted: true}
		}
		position += fragment.payloadByteCount
	}
	if position != datagram.finalPayloadByteCount {
		return ipFragmentProcessResult{fragment: true, accepted: true}
	}

	packetByteCount := datagram.firstHeaderByteCount + datagram.finalPayloadByteCount
	reassembled := MessagePoolGet(packetByteCount)
	for _, fragment := range datagram.fragments {
		if fragment.offset == 0 {
			copy(reassembled[:datagram.firstHeaderByteCount], fragment.packet[:datagram.firstHeaderByteCount])
			break
		}
	}
	for _, fragment := range datagram.fragments {
		copy(
			reassembled[datagram.firstHeaderByteCount+fragment.offset:],
			fragment.payload(),
		)
	}
	binary.BigEndian.PutUint16(reassembled[2:4], uint16(packetByteCount))
	// A reassembled packet is no longer a fragment. Clear gVisor's DF+MF form
	// as well as ordinary MF/offset state.
	binary.BigEndian.PutUint16(reassembled[6:8], 0)
	writeIpv4HeaderChecksum(reassembled[:datagram.firstHeaderByteCount])
	if retainCompletedFragments {
		retained := self.detachDatagram(key)
		fragments := make([][]byte, len(retained))
		// gVisor emits locally fragmented DF packets with identification zero.
		// That is only unambiguous while one datagram's fragments remain
		// contiguous. Connect's H3 stream and DATAGRAM lanes intentionally run
		// in parallel, so two complete groups can interleave before the next
		// reassembly boundary. Give every owned group a canonical, nonzero wire
		// identity before it crosses any asynchronous route and clear DF, which
		// is not meaningful on an already fragmented packet.
		identification := nextIpv4FragmentIdentification()
		for i := range retained {
			packet := retained[i].packet
			headerByteCount := int(packet[0]&0x0f) * 4
			binary.BigEndian.PutUint16(packet[4:6], identification)
			flagsAndOffset := binary.BigEndian.Uint16(packet[6:8]) &^ uint16(0x4000)
			binary.BigEndian.PutUint16(packet[6:8], flagsAndOffset)
			writeIpv4HeaderChecksum(packet[:headerByteCount])
			fragments[i] = packet
		}
		return ipFragmentProcessResult{
			packet:    reassembled,
			fragments: fragments,
			fragment:  true,
			accepted:  true,
		}
	}
	self.releaseDatagram(key)
	return ipFragmentProcessResult{
		packet:   reassembled,
		fragment: true,
		accepted: true,
	}
}

func (self *ipv4FragmentReassembler) makeDatagramRoom(now time.Time) {
	self.expire(now)
	for len(self.datagrams) >= ipv4FragmentReassemblyMaxDatagrams {
		if !self.releaseOldestDatagram(ipv4FragmentKey{}) {
			return
		}
	}
}

// exclude is currently the zero key for new-datagram eviction. Keeping it in
// the helper makes accidental eviction of an active datagram explicit if the
// byte policy is extended later.
func (self *ipv4FragmentReassembler) releaseOldestDatagram(exclude ipv4FragmentKey) bool {
	var oldestKey ipv4FragmentKey
	var oldestTime time.Time
	found := false
	for key, datagram := range self.datagrams {
		if key == exclude && exclude != (ipv4FragmentKey{}) {
			continue
		}
		if !found || datagram.updatedAt.Before(oldestTime) {
			oldestKey = key
			oldestTime = datagram.updatedAt
			found = true
		}
	}
	if found {
		self.releaseDatagram(oldestKey)
	}
	return found
}

func (self *ipv4FragmentReassembler) expire(now time.Time) {
	for key, datagram := range self.datagrams {
		// Lifetime is measured from the first fragment, not the most recent.
		// Duplicate or deliberately paced fragments cannot pin a slot forever.
		if ipv4FragmentReassemblyTimeout <= now.Sub(datagram.createdAt) {
			self.releaseDatagram(key)
		}
	}
}

func (self *ipv4FragmentReassembler) releaseDatagram(key ipv4FragmentKey) {
	for _, fragment := range self.detachDatagram(key) {
		MessagePoolReturn(fragment.packet)
	}
}

func (self *ipv4FragmentReassembler) detachDatagram(key ipv4FragmentKey) []ipv4RetainedFragment {
	datagram := self.datagrams[key]
	if datagram == nil {
		return nil
	}
	delete(self.datagrams, key)
	for _, fragment := range datagram.fragments {
		self.retainedByteCount -= cap(fragment.packet)
	}
	return datagram.fragments
}

func (self *ipv4FragmentReassembler) close() {
	for key := range self.datagrams {
		self.releaseDatagram(key)
	}
}

func writeIpv4HeaderChecksum(header []byte) {
	header[10] = 0
	header[11] = 0
	binary.BigEndian.PutUint16(header[10:12], checksumFinish(checksumAdd(0, header)))
}

var ipv4FragmentIdentification atomic.Uint32

func nextIpv4FragmentIdentification() uint16 {
	for {
		identification := uint16(ipv4FragmentIdentification.Add(1))
		if identification != 0 {
			return identification
		}
	}
}

// fragmentIpv4Packet converts one complete IPv4 packet into RFC 791
// fragments. It consumes packet on every return path.
func fragmentIpv4Packet(packet []byte, mtu int) ([][]byte, error) {
	if len(packet) < Ipv4HeaderSizeWithoutExtensions || packet[0]>>4 != 4 {
		MessagePoolReturn(packet)
		return nil, errInvalidIpv4Packet
	}
	headerByteCount := int(packet[0]&0x0f) * 4
	if headerByteCount < Ipv4HeaderSizeWithoutExtensions || len(packet) < headerByteCount {
		MessagePoolReturn(packet)
		return nil, errInvalidIpv4Packet
	}
	fragmentPayloadByteCount := (mtu - headerByteCount) &^ 7
	if fragmentPayloadByteCount < 8 {
		MessagePoolReturn(packet)
		return nil, errIpv4MtuTooSmall
	}
	ipPayload := packet[headerByteCount:]
	identification := nextIpv4FragmentIdentification()
	fragments := make([][]byte, 0, (len(ipPayload)+fragmentPayloadByteCount-1)/fragmentPayloadByteCount)
	for offset := 0; offset < len(ipPayload); {
		end := min(offset+fragmentPayloadByteCount, len(ipPayload))
		fragment := MessagePoolGet(headerByteCount + end - offset)
		copy(fragment[:headerByteCount], packet[:headerByteCount])
		copy(fragment[headerByteCount:], ipPayload[offset:end])
		binary.BigEndian.PutUint16(fragment[2:4], uint16(len(fragment)))
		binary.BigEndian.PutUint16(fragment[4:6], identification)
		flagsAndOffset := uint16(offset / 8)
		if end < len(ipPayload) {
			flagsAndOffset |= 0x2000
		}
		binary.BigEndian.PutUint16(fragment[6:8], flagsAndOffset)
		writeIpv4HeaderChecksum(fragment[:headerByteCount])
		fragments = append(fragments, fragment)
		offset = end
	}
	MessagePoolReturn(packet)
	return fragments, nil
}
