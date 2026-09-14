package connect

import (
	"bytes"
	"encoding/binary"
	"errors"
	"slices"
	"sync/atomic"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// ip_ipv6_fragment.go — IPv6 fragment reassembly and emission (RFC 8200
// §4.5), the v6 half of ip_fragment.go. The shape deliberately mirrors the
// IPv4 reassembler: one small cache per NAT send shard, bounded in datagrams,
// fragments and retained bytes, lifetime measured from the first fragment,
// overlapping fragments poisoning the whole datagram (RFC 5722), and the
// original fragment owners optionally handed back on completion so a policy
// decision made on the whole datagram can forward the untouched wire
// fragments.
//
// What differs is the wire format. A v6 fragment carries an unfragmentable
// part (the base header plus any extension headers before the Fragment
// header) that every fragment repeats, then the 8-byte Fragment header, then
// a slice of the fragmentable part. Reassembly takes the unfragmentable part
// from the offset-zero fragment, points its trailing Next Header field at the
// fragmentable part's first header (dropping the Fragment header), and
// rewrites the payload length. There is no header checksum to fix and no DF
// flag to clear. The identification is 32 bits.

const (
	// same bounds as the v4 reassembler: the two caches share a shard
	ipv6FragmentReassemblyTimeout          = ipv4FragmentReassemblyTimeout
	ipv6FragmentReassemblyMaxDatagrams     = ipv4FragmentReassemblyMaxDatagrams
	ipv6FragmentReassemblyMaxFragments     = ipv4FragmentReassemblyMaxFragments
	ipv6FragmentReassemblyMaxRetainedBytes = ipv4FragmentReassemblyMaxRetainedBytes
)

var (
	errInvalidIpv6Packet            = errors.New("invalid IPv6 packet")
	errIpv6MtuTooSmall              = errors.New("IPv6 MTU is too small for fragmentation")
	errIpv6ExtensionHeadersFragment = errors.New("IPv6 packet with extension headers cannot be fragmented here")
)

type ipv6FragmentKey struct {
	source        TransferPath
	transferKey   TransferKey
	provideMode   protocol.ProvideMode
	sourceIp      [16]byte
	destinationIp [16]byte
	// rfc 8200 keys reassembly on (source, destination, identification) only;
	// the protocol is inside the fragmentable part
	identification uint32
}

type ipv6RetainedFragment struct {
	packet []byte
	// unfragmentableByteCount is the Fragment header offset; the bytes before
	// it are the unfragmentable part this fragment carries
	unfragmentableByteCount int
	offset                  int
	payloadByteCount        int
}

func (self *ipv6RetainedFragment) payload() []byte {
	start := self.unfragmentableByteCount + ipv6FragmentHeaderSize
	return self.packet[start : start+self.payloadByteCount]
}

type ipv6FragmentDatagram struct {
	createdAt time.Time
	updatedAt time.Time
	fragments []ipv6RetainedFragment
	// from the offset-zero fragment. Zero until it arrives.
	firstUnfragmentableByteCount int
	// the Next Header field that pointed at the Fragment header, rewritten
	// to firstNextHeader on reassembly
	firstNextHeaderFieldOffset int
	firstNextHeader            byte
	finalPayloadByteCount      int
}

type ipv6FragmentReassembler struct {
	datagrams         map[ipv6FragmentKey]*ipv6FragmentDatagram
	retainedByteCount int
}

func newIpv6FragmentReassembler() *ipv6FragmentReassembler {
	return &ipv6FragmentReassembler{
		datagrams: make(map[ipv6FragmentKey]*ipv6FragmentDatagram),
	}
}

// process consumes packet only when it is a non-atomic IPv6 fragment. An
// unfragmented packet is returned unchanged. A nil result means the fragment
// was retained for completion or rejected and returned to the message pool.
func (self *ipv6FragmentReassembler) process(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	packet []byte,
) []byte {
	return self.processAt(source, transferKey, provideMode, packet, time.Now())
}

func (self *ipv6FragmentReassembler) processAt(
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

func (self *ipv6FragmentReassembler) processResultAt(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
	packet []byte,
	now time.Time,
	retainCompletedFragments bool,
) ipFragmentProcessResult {
	if len(packet) < Ipv6HeaderSize || packet[0]>>4 != 6 {
		return ipFragmentProcessResult{packet: packet, accepted: true}
	}
	if !isIpv6ExtensionHeader(packet[6]) {
		// the common case: no extension chain, so no fragment header
		return ipFragmentProcessResult{packet: packet, accepted: true}
	}
	walk, ok := walkIpv6ExtensionHeaders(packet)
	if !ok || !walk.fragmented {
		// not a fragment (or malformed, which the parse will drop)
		return ipFragmentProcessResult{packet: packet, accepted: true}
	}

	self.expire(now)

	key := ipv6FragmentKey{
		source:         source,
		transferKey:    transferKey,
		provideMode:    provideMode,
		identification: walk.fragment.identification,
	}
	copy(key.sourceIp[:], packet[8:24])
	copy(key.destinationIp[:], packet[24:40])

	drop := func() ipFragmentProcessResult {
		self.releaseDatagram(key)
		MessagePoolReturn(packet)
		return ipFragmentProcessResult{fragment: true}
	}

	unfragmentableByteCount := walk.fragment.headerOffset
	payloadByteCount := walk.payloadEnd - walk.transportOffset
	moreFragments := walk.fragment.moreFragments
	offset := walk.fragment.offset
	// rfc 8200 §4.5: every fragment but the last carries a multiple of 8
	// octets, and the reassembled payload length must fit the 16 bit field
	if payloadByteCount <= 0 ||
		(moreFragments && payloadByteCount%8 != 0) ||
		0xffff < (unfragmentableByteCount-Ipv6HeaderSize)+offset+payloadByteCount {
		return drop()
	}

	datagram := self.datagrams[key]
	if datagram == nil {
		self.makeDatagramRoom(now)
		packetCost := cap(packet)
		if ipv6FragmentReassemblyMaxRetainedBytes < packetCost {
			MessagePoolReturn(packet)
			return ipFragmentProcessResult{fragment: true}
		}
		for self.retainedByteCount+packetCost > ipv6FragmentReassemblyMaxRetainedBytes {
			if !self.releaseOldestDatagram() {
				MessagePoolReturn(packet)
				return ipFragmentProcessResult{fragment: true}
			}
		}
		datagram = &ipv6FragmentDatagram{
			createdAt:             now,
			updatedAt:             now,
			finalPayloadByteCount: -1,
		}
		self.datagrams[key] = datagram
	} else if self.retainedByteCount+cap(packet) > ipv6FragmentReassemblyMaxRetainedBytes {
		return drop()
	}

	fragmentEnd := offset + payloadByteCount
	fragmentPayload := packet[walk.transportOffset:walk.payloadEnd]
	for _, retained := range datagram.fragments {
		retainedEnd := retained.offset + retained.payloadByteCount
		if offset == retained.offset && fragmentEnd == retainedEnd {
			// an exact retransmission is harmless; a conflicting duplicate
			// poisons the datagram like every other overlap (rfc 5722)
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
	if len(datagram.fragments) >= ipv6FragmentReassemblyMaxFragments {
		return drop()
	}
	if !moreFragments {
		if datagram.finalPayloadByteCount >= 0 && datagram.finalPayloadByteCount != fragmentEnd {
			return drop()
		}
		// a final fragment ending before a retained fragment would let a
		// contiguous prefix complete and then write past the reassembly
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
		if datagram.firstUnfragmentableByteCount != 0 &&
			datagram.firstUnfragmentableByteCount != unfragmentableByteCount {
			return drop()
		}
		datagram.firstUnfragmentableByteCount = unfragmentableByteCount
		datagram.firstNextHeaderFieldOffset = walk.fragment.precedingNextHeaderFieldOffset
		datagram.firstNextHeader = walk.fragment.nextHeader
	}
	if datagram.firstUnfragmentableByteCount != 0 &&
		0 <= datagram.finalPayloadByteCount &&
		0xffff < (datagram.firstUnfragmentableByteCount-Ipv6HeaderSize)+datagram.finalPayloadByteCount {
		return drop()
	}

	// retain the original pool allocation rather than copying each fragment.
	// cap is charged because it is the backing memory actually kept alive.
	packet = packet[:walk.payloadEnd]
	datagram.fragments = append(datagram.fragments, ipv6RetainedFragment{
		packet:                  packet,
		unfragmentableByteCount: unfragmentableByteCount,
		offset:                  offset,
		payloadByteCount:        payloadByteCount,
	})
	datagram.updatedAt = now
	self.retainedByteCount += cap(packet)

	if datagram.firstUnfragmentableByteCount == 0 || datagram.finalPayloadByteCount < 0 {
		return ipFragmentProcessResult{fragment: true, accepted: true}
	}
	slices.SortFunc(datagram.fragments, func(a ipv6RetainedFragment, b ipv6RetainedFragment) int {
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

	packetByteCount := datagram.firstUnfragmentableByteCount + datagram.finalPayloadByteCount
	reassembled := MessagePoolGet(packetByteCount)
	for _, fragment := range datagram.fragments {
		if fragment.offset == 0 {
			copy(
				reassembled[:datagram.firstUnfragmentableByteCount],
				fragment.packet[:datagram.firstUnfragmentableByteCount],
			)
			break
		}
	}
	for _, fragment := range datagram.fragments {
		copy(
			reassembled[datagram.firstUnfragmentableByteCount+fragment.offset:],
			fragment.payload(),
		)
	}
	// the reassembled packet has no Fragment header: the header that pointed
	// at it now points at the fragmentable part's first header
	reassembled[datagram.firstNextHeaderFieldOffset] = datagram.firstNextHeader
	binary.BigEndian.PutUint16(reassembled[4:6], uint16(packetByteCount-Ipv6HeaderSize))
	if retainCompletedFragments {
		retained := self.detachDatagram(key)
		fragments := make([][]byte, len(retained))
		// like the v4 owned groups: give every group a canonical nonzero
		// identification before it crosses an asynchronous route, so two
		// complete groups that interleave on parallel lanes stay distinct
		identification := nextIpv6FragmentIdentification()
		for i := range retained {
			packet := retained[i].packet
			binary.BigEndian.PutUint32(
				packet[retained[i].unfragmentableByteCount+4:retained[i].unfragmentableByteCount+8],
				identification,
			)
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

func (self *ipv6FragmentReassembler) makeDatagramRoom(now time.Time) {
	self.expire(now)
	for len(self.datagrams) >= ipv6FragmentReassemblyMaxDatagrams {
		if !self.releaseOldestDatagram() {
			return
		}
	}
}

func (self *ipv6FragmentReassembler) releaseOldestDatagram() bool {
	var oldestKey ipv6FragmentKey
	var oldestTime time.Time
	found := false
	for key, datagram := range self.datagrams {
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

func (self *ipv6FragmentReassembler) expire(now time.Time) {
	for key, datagram := range self.datagrams {
		// lifetime from the first fragment, so paced duplicates cannot pin a
		// slot forever
		if ipv6FragmentReassemblyTimeout <= now.Sub(datagram.createdAt) {
			self.releaseDatagram(key)
		}
	}
}

func (self *ipv6FragmentReassembler) releaseDatagram(key ipv6FragmentKey) {
	for _, fragment := range self.detachDatagram(key) {
		MessagePoolReturn(fragment.packet)
	}
}

func (self *ipv6FragmentReassembler) detachDatagram(key ipv6FragmentKey) []ipv6RetainedFragment {
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

func (self *ipv6FragmentReassembler) close() {
	for key := range self.datagrams {
		self.releaseDatagram(key)
	}
}

var ipv6FragmentIdentification atomic.Uint32

func nextIpv6FragmentIdentification() uint32 {
	for {
		identification := ipv6FragmentIdentification.Add(1)
		if identification != 0 {
			return identification
		}
	}
}

// fragmentIpv6Packet converts one complete IPv6 packet with no extension
// headers -- the only shape this package's builders emit -- into RFC 8200
// fragments. The base header becomes the unfragmentable part, a Fragment
// header is inserted after it, and the transport header plus payload is the
// fragmentable part. It consumes packet on every return path.
func fragmentIpv6Packet(packet []byte, mtu int) ([][]byte, error) {
	if len(packet) < Ipv6HeaderSize || packet[0]>>4 != 6 {
		MessagePoolReturn(packet)
		return nil, errInvalidIpv6Packet
	}
	declaredEnd := Ipv6HeaderSize + int(binary.BigEndian.Uint16(packet[4:6]))
	if len(packet) < declaredEnd {
		MessagePoolReturn(packet)
		return nil, errInvalidIpv6Packet
	}
	if isIpv6ExtensionHeader(packet[6]) {
		// the unfragmentable part would have to be split from the
		// fragmentable extension headers; nothing here builds such a packet
		MessagePoolReturn(packet)
		return nil, errIpv6ExtensionHeadersFragment
	}
	fragmentPayloadByteCount := (mtu - Ipv6HeaderSize - ipv6FragmentHeaderSize) &^ 7
	if fragmentPayloadByteCount < 8 {
		MessagePoolReturn(packet)
		return nil, errIpv6MtuTooSmall
	}
	nextHeader := packet[6]
	ipPayload := packet[Ipv6HeaderSize:declaredEnd]
	identification := nextIpv6FragmentIdentification()
	fragments := make([][]byte, 0, (len(ipPayload)+fragmentPayloadByteCount-1)/fragmentPayloadByteCount)
	for offset := 0; offset < len(ipPayload); {
		end := min(offset+fragmentPayloadByteCount, len(ipPayload))
		fragment := MessagePoolGet(Ipv6HeaderSize + ipv6FragmentHeaderSize + end - offset)
		copy(fragment[:Ipv6HeaderSize], packet[:Ipv6HeaderSize])
		binary.BigEndian.PutUint16(fragment[4:6], uint16(len(fragment)-Ipv6HeaderSize))
		fragment[6] = ipv6NextHeaderFragment
		fragmentHeader := fragment[Ipv6HeaderSize : Ipv6HeaderSize+ipv6FragmentHeaderSize]
		fragmentHeader[0] = nextHeader
		fragmentHeader[1] = 0
		offsetAndFlags := uint16(offset)
		if end < len(ipPayload) {
			offsetAndFlags |= 0x1
		}
		binary.BigEndian.PutUint16(fragmentHeader[2:4], offsetAndFlags)
		binary.BigEndian.PutUint32(fragmentHeader[4:8], identification)
		copy(fragment[Ipv6HeaderSize+ipv6FragmentHeaderSize:], ipPayload[offset:end])
		fragments = append(fragments, fragment)
		offset = end
	}
	MessagePoolReturn(packet)
	return fragments, nil
}
