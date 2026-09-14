package connect

import (
	"encoding/binary"
	"fmt"
	"net"
)

// out-of-sequence packet builders for the security paths. these packets are
// built rarely (flow teardown and abuse edges), so they use plain (non pool)
// allocations; callers own the returned packet and never return it to the
// message pool.

// out-of-sequence packet is not aligned with the flow sequence.
// returns nil for a protocol with no builder (e.g. icmp); callers deliver
// through paths that drop a nil packet.
func ipOosPacket(ipPath *IpPath, payload []byte) []byte {
	switch ipPath.Protocol {
	case IpProtocolUdp:
		return ipOosUdpPacket(ipPath, payload)
	case IpProtocolTcp:
		// seq, ack, and flags are not aligned with any flow state
		return ipOosTcpPacket(ipPath, 0, payload)
	default:
		return nil
	}
}

// out-of-sequence reset packet for the flow
// if tcp, send rst
// udp has no in-band reset; see `ipOosUnreachable`
//
// The reset carries sequence 0, which the exit accepts (it tears down on any
// rst for a known bufferId, with no sequence check) but a real tcp stack does
// not. Toward the source, use `ipOosRstSequence`.
func ipOosRst(ipPath *IpPath) ([]byte, bool) {
	return ipOosRstSequence(ipPath, 0)
}

// out-of-sequence reset carrying an explicit sequence number.
//
// RFC 5961 section 3.2, and `tcp_validate_incoming` in linux, accept a reset
// only when SEG.SEQ is exactly RCV.NXT -- anything else earns a challenge ack
// and the reset is ignored. Sequence 0 essentially never matches an established
// connection, so a reset built by `ipOosRst` is silently discarded by the
// source's stack, and the flow freezes rather than resetting.
//
// The value the source needs is the sequence it expects next from the
// destination, which is the ack number it has been sending -- tracked per flow
// as `multiClientChannelUpdate.ackSequenceNumber`.
func ipOosRstSequence(ipPath *IpPath, sequenceNumber uint32) ([]byte, bool) {
	switch ipPath.Protocol {
	case IpProtocolTcp:
		switch ipPath.Version {
		case 4, 6:
			return ipOosTcpPacketSequence(ipPath, tcpFlagRst, sequenceNumber, nil), true
		default:
			return nil, false
		}
	default:
		return nil, false
	}
}

const (
	// type, code, checksum, unused
	icmpUnreachableHeaderSize = 8

	icmpv4TypeDestinationUnreachable = 3
	// port unreachable, delivered as ECONNREFUSED.
	//
	// This was host unreachable (code 1) first, on the reasoning that the port
	// is fine and only the path died. That is semantically truer and it does
	// not work: linux maps ICMP_HOST_UNREACH to {EHOSTUNREACH, fatal=0} in
	// icmp_err_convert, and __udp4_lib_err discards a non-fatal error on any
	// socket without IP_RECVERR -- `if (!harderr || ...) goto out`. Chrome's
	// quic sockets and the android resolver do not set IP_RECVERR, so the
	// signal never reached the application on the one platform reporting the
	// freeze. Port unreachable is {ECONNREFUSED, fatal=1} and is delivered.
	//
	// The original worry -- that ECONNREFUSED would make a resolver mark the
	// tunnel's fixed dns address dead -- does not apply: IpMux.Receive
	// short-circuits isLocalDestination traffic to the internal stack, so the
	// mux-terminated resolver flows never reach this teardown path at all.
	icmpv4CodePortUnreachable = 3

	icmpv6TypeDestinationUnreachable = 1
	// the v6 equivalent, also ECONNREFUSED and also fatal. v6 has no fatal
	// EHOSTUNREACH at all, so matching errno across families and being
	// delivered on both is only possible here.
	icmpv6CodePortUnreachable = 4

	// host unreachable / no route, used for the provider dial-failure signal
	// on tcp flows. Deliberately NOT the port-unreachable used by teardown:
	// the two must stay distinguishable, because they mean opposite things --
	// "your exit died, reconnect" versus "this provider could not open the
	// upstream, quietly try another".
	//
	// The delivery concern that forced teardown onto port-unreachable does not
	// apply here. This signal is consumed by the client's own channel-level
	// intercept (see ipParseIcmpUnreachable), which matches on the embedded
	// tuple -- it never reaches an application socket. Clients without the
	// intercept drop all icmp at ParseIpPath ("No support for protocol 1"),
	// so nothing downstream ever sees the code either way.
	icmpv4CodeHostUnreachable = 1
	icmpv6CodeNoRoute         = 0

	// path-mtu signals: a source that received a packet larger than its path
	// carries answers with the mtu it can take. v4 embeds the next-hop mtu in
	// the second half of the unused word of a fragmentation-needed
	// destination-unreachable (rfc 1191); v6 has a dedicated type whose whole
	// unused word is the mtu (rfc 4443 §3.2).
	icmpv4CodeFragmentationNeeded = 4
	icmpv6TypePacketTooBig        = 2
)

// out-of-sequence destination-unreachable for a flow whose path is gone.
//
// Two distinct uses share this builder, distinguished by protocol and code:
//
// udp teardown (port unreachable): a udp flow whose exit is removed goes
// silent and stalls until the application's own timeout -- a dns query that
// never returns, or a quic session (udp 443) that hangs instead of falling
// back to tcp. the code is port unreachable, chosen for delivery over
// semantics: on linux only the fatal codes reach a socket that has not opted
// into IP_RECVERR. (tcp teardown uses `ipOosRstSequence` instead.)
//
// provider dial failure (tcp, host unreachable / v6 no-route): a provider
// whose upstream dial fails -- out of sockets, or its own upstream proxy
// refusing -- previously said nothing, leaving the source in syn-retransmit
// backoff: measured hangs of 3, 7, 15, 31 and 63 seconds, each landing on a
// retransmit boundary. the provider now answers with this packet. it is
// consumed by the client's channel intercept (`ipParseIcmpUnreachable`) and
// deliberately carries a different code from teardown, because the two mean
// opposite things: "your exit died, reconnect" versus "this provider could
// not open the upstream, quietly race another".
//
// the embedded datagram mirrors the original egress packet: the ip header
// plus the 8 transport bytes rfc 792 requires. for tcp the embedded sequence
// number is the source's own (`ipPath.SequenceNumber`, parsed from its syn),
// so a stack that does validate the embed accepts it.
//
// `ipPath` is the flow's own direction (source to destination). the returned
// packet is addressed back the other way, as if from the destination.
func ipOosUnreachable(ipPath *IpPath) ([]byte, bool) {
	// check the version before building anything: the embedded builders panic
	// on an unsupported version, and this must return false the way ipOosRst
	// does
	switch ipPath.Version {
	case 4, 6:
	default:
		return nil, false
	}

	var embedded []byte
	var v4Code byte
	var v6Code byte
	switch ipPath.Protocol {
	case IpProtocolUdp:
		embedded = ipOosUdpPacket(ipPath, nil)
		v4Code = icmpv4CodePortUnreachable
		v6Code = icmpv6CodePortUnreachable
	case IpProtocolTcp:
		// mirror the original syn, with the source's own sequence number
		embedded = ipOosTcpPacketSequence(ipPath, tcpFlagSyn, ipPath.SequenceNumber, nil)
		v4Code = icmpv4CodeHostUnreachable
		v6Code = icmpv6CodeNoRoute
	default:
		return nil, false
	}
	reverse := ipPath.Reverse()

	writeHeader := func(icmp []byte, icmpType byte, code byte) {
		icmp[0] = icmpType
		icmp[1] = code
		// checksum, set by the caller
		icmp[2] = 0
		icmp[3] = 0
		// unused
		binary.BigEndian.PutUint32(icmp[4:8], 0)
		copy(icmp[icmpUnreachableHeaderSize:], embedded)
	}

	switch ipPath.Version {
	case 4:
		packet, icmp := ipTransportPacket(reverse, ipProtocolNumberIcmp4, icmpUnreachableHeaderSize+len(embedded))
		writeHeader(icmp, icmpv4TypeDestinationUnreachable, v4Code)
		// icmpv4 checksums the message alone, with no pseudo header
		binary.BigEndian.PutUint16(icmp[2:4], checksumFinish(checksumAdd(0, icmp)))
		return packet, true
	case 6:
		packet, icmp := ipTransportPacket(reverse, ipProtocolNumberIcmp6, icmpUnreachableHeaderSize+len(embedded))
		writeHeader(icmp, icmpv6TypeDestinationUnreachable, v6Code)
		// icmpv6 checksums with the ipv6 pseudo header, like tcp and udp
		binary.BigEndian.PutUint16(icmp[2:4], transportChecksum(
			ipProtocolNumberIcmp6,
			reverse.SourceIp.To16(),
			reverse.DestinationIp.To16(),
			icmp,
		))
		return packet, true
	default:
		return nil, false
	}
}

// ipParseIcmpUnreachable recognizes an icmp destination-unreachable and
// recovers the flow it refers to.
//
// The returned path is the ORIGINAL egress direction (source to destination),
// reconstructed from the datagram embedded in the icmp body -- which is
// exactly the key the multi client uses for its flow maps. `ParseIpPath`
// rejects icmp outright ("No support for protocol 1"), so without this the
// provider dial-failure signal built by `ipOosUnreachable` is dropped at the
// channel before anything can act on it.
//
// Checksums are deliberately not verified. The packet arrives over the
// authenticated provider channel, and a provider cannot relay genuine network
// icmp anyway -- its kernel consumes errors on connected tcp/udp sockets as
// errno, never as packets. So the only icmp a channel can carry is one a
// provider built, and length checks are the integrity that matters for
// matching. A stack that consumed this packet for real would verify; the
// channel intercept does not need to.
//
// v6 extension headers on the outer packet and on the embed are walked (see
// ip_ipv6_ext.go); the builders in this package emit none, but a stack in the
// path may. Any unreachable code is accepted -- the caller distinguishes
// teardown from dial failure by context, not code, since a matched flow with
// no inbound data can only mean the dial failed.
func ipParseIcmpUnreachable(packet []byte) (*IpPath, bool) {
	if len(packet) == 0 {
		return nil, false
	}

	// outer header: find the icmp body
	icmp, ok := ipIcmpBody(packet)
	if !ok || len(icmp) < icmpUnreachableHeaderSize {
		return nil, false
	}
	switch uint8(packet[0]) >> 4 {
	case 4:
		if icmp[0] != icmpv4TypeDestinationUnreachable {
			return nil, false
		}
	case 6:
		if icmp[0] != icmpv6TypeDestinationUnreachable {
			return nil, false
		}
	}
	return ipParseIcmpEmbeddedPath(icmp[icmpUnreachableHeaderSize:])
}

// ipIcmpBody finds the icmp message of a packet whose transport is icmp (v4)
// or icmpv6 (v6), walking any v6 extension headers. ok is false for any other
// protocol or a malformed header.
func ipIcmpBody(packet []byte) ([]byte, bool) {
	if len(packet) == 0 {
		return nil, false
	}
	switch uint8(packet[0]) >> 4 {
	case 4:
		if len(packet) < Ipv4HeaderSizeWithoutExtensions {
			return nil, false
		}
		ihl := int(packet[0]&0x0f) * 4
		if ihl < Ipv4HeaderSizeWithoutExtensions || len(packet) < ihl {
			return nil, false
		}
		if ipProtocolNumber(packet[9]) != ipProtocolNumberIcmp4 {
			return nil, false
		}
		return packet[ihl:], true
	case 6:
		nextHeader, transportOffset, payloadEnd, ok := ipv6TransportOffset(packet)
		if !ok || nextHeader != ipProtocolNumberIcmp6 {
			return nil, false
		}
		return packet[transportOffset:payloadEnd], true
	default:
		return nil, false
	}
}

// ipParseIcmpEmbeddedPath recovers the flow an icmp error refers to from the
// original datagram embedded in its body: ip header plus at least 8 transport
// bytes. Parsed by hand rather than with parseTcpPacket/parseUdpPacket, which
// require full transport headers -- rfc 792 only guarantees 8 bytes, and 8 is
// what the builders here embed for udp. A v6 embed with extension headers is
// walked; a v6 fragment embed (no transport header to read) is rejected.
func ipParseIcmpEmbeddedPath(embedded []byte) (*IpPath, bool) {
	if len(embedded) == 0 {
		return nil, false
	}

	var embeddedProtocol ipProtocolNumber
	var sourceIp, destinationIp net.IP
	var transport []byte
	version := int(uint8(embedded[0]) >> 4)
	switch version {
	case 4:
		if len(embedded) < Ipv4HeaderSizeWithoutExtensions {
			return nil, false
		}
		ihl := int(embedded[0]&0x0f) * 4
		if ihl < Ipv4HeaderSizeWithoutExtensions || len(embedded) < ihl+8 {
			return nil, false
		}
		embeddedProtocol = ipProtocolNumber(embedded[9])
		sourceIp = net.IP(embedded[12:16])
		destinationIp = net.IP(embedded[16:20])
		transport = embedded[ihl:]
	case 6:
		if len(embedded) < Ipv6HeaderSize+8 {
			return nil, false
		}
		transportOffset := Ipv6HeaderSize
		embeddedProtocol = ipProtocolNumber(embedded[6])
		if isIpv6ExtensionHeader(embedded[6]) {
			// an embed is truncated to what the sender chose to include, so
			// the declared payload length may exceed the bytes present: walk
			// against a header whose length is clamped to the embed
			walk, ok := walkIpv6ExtensionHeaders(ipv6EmbedForWalk(embedded))
			if !ok || walk.fragmented {
				return nil, false
			}
			embeddedProtocol = walk.nextHeader
			transportOffset = walk.transportOffset
		}
		if len(embedded) < transportOffset+8 {
			return nil, false
		}
		sourceIp = net.IP(embedded[8:24])
		destinationIp = net.IP(embedded[24:40])
		transport = embedded[transportOffset:]
	default:
		return nil, false
	}

	var protocol IpProtocol
	switch embeddedProtocol {
	case ipProtocolNumberTcp:
		protocol = IpProtocolTcp
	case ipProtocolNumberUdp:
		protocol = IpProtocolUdp
	default:
		return nil, false
	}

	// copy the ips out of the caller's buffer, which may be pooled and
	// recycled after the handoff -- same convention as ParseIpPathWithPayload
	ipBacking := make(net.IP, len(sourceIp)+len(destinationIp))
	sn := copy(ipBacking, sourceIp)
	copy(ipBacking[sn:], destinationIp)

	ipPath := &IpPath{
		Version:         version,
		Protocol:        protocol,
		SourceIp:        ipBacking[:sn:sn],
		SourcePort:      int(binary.BigEndian.Uint16(transport[0:2])),
		DestinationIp:   ipBacking[sn:],
		DestinationPort: int(binary.BigEndian.Uint16(transport[2:4])),
	}
	if protocol == IpProtocolTcp {
		// bytes 4:8 of a tcp header are the sequence number, inside the 8
		// bytes rfc 792 guarantees
		ipPath.SequenceNumber = binary.BigEndian.Uint32(transport[4:8])
	}
	return ipPath, true
}

// ipv6EmbedForWalk returns a copy of a truncated v6 embed whose payload
// length is clamped to the bytes present, so the extension walker's length
// check passes on an embed the sender cut short. Embeds are rare (one per
// icmp error), so the copy is not on any hot path.
func ipv6EmbedForWalk(embedded []byte) []byte {
	clamped := make([]byte, len(embedded))
	copy(clamped, embedded)
	if declared := Ipv6HeaderSize + int(binary.BigEndian.Uint16(clamped[4:6])); len(clamped) < declared {
		binary.BigEndian.PutUint16(clamped[4:6], uint16(len(clamped)-Ipv6HeaderSize))
	}
	return clamped
}

// ipParseIcmpv4FragmentationNeeded recognizes an icmpv4 fragmentation-needed
// (destination unreachable, code 4) in an icmp body and returns the next-hop
// mtu with the flow the embedded datagram describes. A zero next-hop mtu
// (rfc 1191 pre-dates the field) is not a usable signal and is rejected.
func ipParseIcmpv4FragmentationNeeded(icmp []byte) (mtu int, embedded *IpPath, ok bool) {
	if len(icmp) < icmpUnreachableHeaderSize ||
		icmp[0] != icmpv4TypeDestinationUnreachable ||
		icmp[1] != icmpv4CodeFragmentationNeeded {
		return 0, nil, false
	}
	mtu = int(binary.BigEndian.Uint16(icmp[6:8]))
	if mtu == 0 {
		return 0, nil, false
	}
	embedded, ok = ipParseIcmpEmbeddedPath(icmp[icmpUnreachableHeaderSize:])
	if !ok {
		return 0, nil, false
	}
	return mtu, embedded, true
}

// ipParseIcmpv6PacketTooBig recognizes an icmpv6 packet-too-big (type 2) in
// an icmpv6 body and returns the reported mtu with the flow the embedded
// datagram describes.
func ipParseIcmpv6PacketTooBig(icmp []byte) (mtu int, embedded *IpPath, ok bool) {
	if len(icmp) < icmpUnreachableHeaderSize || icmp[0] != icmpv6TypePacketTooBig {
		return 0, nil, false
	}
	mtu = int(binary.BigEndian.Uint32(icmp[4:8]))
	if mtu <= 0 || 0xffff < mtu {
		return 0, nil, false
	}
	embedded, ok = ipParseIcmpEmbeddedPath(icmp[icmpUnreachableHeaderSize:])
	if !ok {
		return 0, nil, false
	}
	return mtu, embedded, true
}

// ipOosPacketTooBig builds the icmp path-mtu signal a stack would send for a
// packet that arrived larger than `mtu`: fragmentation-needed for v4,
// packet-too-big for v6. `original` is the oversized packet's own direction
// and the message is addressed back to its sender. The embed is the original
// ip header plus 8 transport bytes, mirroring ipOosUnreachable.
func ipOosPacketTooBig(original *IpPath, mtu int) ([]byte, bool) {
	switch original.Version {
	case 4, 6:
	default:
		return nil, false
	}
	var embedded []byte
	switch original.Protocol {
	case IpProtocolUdp:
		embedded = ipOosUdpPacket(original, nil)
	case IpProtocolTcp:
		embedded = ipOosTcpPacketSequence(original, tcpFlagAck, original.SequenceNumber, nil)
	default:
		return nil, false
	}
	reverse := original.Reverse()
	switch original.Version {
	case 4:
		packet, icmp := ipTransportPacket(reverse, ipProtocolNumberIcmp4, icmpUnreachableHeaderSize+len(embedded))
		icmp[0] = icmpv4TypeDestinationUnreachable
		icmp[1] = icmpv4CodeFragmentationNeeded
		icmp[2], icmp[3] = 0, 0
		binary.BigEndian.PutUint16(icmp[4:6], 0)
		binary.BigEndian.PutUint16(icmp[6:8], uint16(mtu))
		copy(icmp[icmpUnreachableHeaderSize:], embedded)
		binary.BigEndian.PutUint16(icmp[2:4], checksumFinish(checksumAdd(0, icmp)))
		return packet, true
	default:
		packet, icmp := ipTransportPacket(reverse, ipProtocolNumberIcmp6, icmpUnreachableHeaderSize+len(embedded))
		icmp[0] = icmpv6TypePacketTooBig
		icmp[1] = 0
		icmp[2], icmp[3] = 0, 0
		binary.BigEndian.PutUint32(icmp[4:8], uint32(mtu))
		copy(icmp[icmpUnreachableHeaderSize:], embedded)
		binary.BigEndian.PutUint16(icmp[2:4], transportChecksum(
			ipProtocolNumberIcmp6,
			reverse.SourceIp.To16(),
			reverse.DestinationIp.To16(),
			icmp,
		))
		return packet, true
	}
}

// builds a fresh packet in the path direction (source to destination) sized
// for the transport, normalizing the addresses to the wire form for the
// version
func ipTransportPacket(ipPath *IpPath, ipProtocol ipProtocolNumber, transportByteCount int) (packet []byte, transport []byte) {
	switch ipPath.Version {
	case 4:
		packet = make([]byte, Ipv4HeaderSizeWithoutExtensions+transportByteCount)
		writeIpv4Header(packet, ipProtocol, ipPath.SourceIp.To4(), ipPath.DestinationIp.To4())
		transport = packet[Ipv4HeaderSizeWithoutExtensions:]
	case 6:
		packet = make([]byte, Ipv6HeaderSize+transportByteCount)
		writeIpv6Header(packet, ipProtocol, ipPath.SourceIp.To16(), ipPath.DestinationIp.To16())
		transport = packet[Ipv6HeaderSize:]
	default:
		panic(fmt.Errorf("Bad ip version: %d", ipPath.Version))
	}
	return
}

// computes the transport checksum with the wire form of the path addresses
func ipPathTransportChecksum(ipPath *IpPath, ipProtocol ipProtocolNumber, transport []byte) uint16 {
	switch ipPath.Version {
	case 4:
		return transportChecksum(ipProtocol, ipPath.SourceIp.To4(), ipPath.DestinationIp.To4(), transport)
	default:
		return transportChecksum(ipProtocol, ipPath.SourceIp.To16(), ipPath.DestinationIp.To16(), transport)
	}
}

func ipOosUdpPacket(ipPath *IpPath, payload []byte) []byte {
	packet, udp := ipTransportPacket(ipPath, ipProtocolNumberUdp, UdpHeaderSize+len(payload))
	binary.BigEndian.PutUint16(udp[0:2], uint16(ipPath.SourcePort))
	binary.BigEndian.PutUint16(udp[2:4], uint16(ipPath.DestinationPort))
	binary.BigEndian.PutUint16(udp[4:6], uint16(UdpHeaderSize+len(payload)))
	// checksum, set below
	udp[6] = 0
	udp[7] = 0
	copy(udp[UdpHeaderSize:], payload)
	checksum := ipPathTransportChecksum(ipPath, ipProtocolNumberUdp, udp)
	if checksum == 0 {
		// zero means no checksum
		checksum = 0xffff
	}
	binary.BigEndian.PutUint16(udp[6:8], checksum)
	return packet
}

func ipOosTcpPacket(ipPath *IpPath, flags byte, payload []byte) []byte {
	return ipOosTcpPacketSequence(ipPath, flags, 0, payload)
}

func ipOosTcpPacketSequence(ipPath *IpPath, flags byte, sequenceNumber uint32, payload []byte) []byte {
	packet, tcp := ipTransportPacket(ipPath, ipProtocolNumberTcp, TcpHeaderSizeWithoutExtensions+len(payload))
	binary.BigEndian.PutUint16(tcp[0:2], uint16(ipPath.SourcePort))
	binary.BigEndian.PutUint16(tcp[2:4], uint16(ipPath.DestinationPort))
	// ack is not aligned with any flow state; zero like the historical builders
	binary.BigEndian.PutUint32(tcp[4:8], sequenceNumber)
	binary.BigEndian.PutUint32(tcp[8:12], 0)
	// data offset, no options
	tcp[12] = byte(TcpHeaderSizeWithoutExtensions/4) << 4
	tcp[13] = flags
	binary.BigEndian.PutUint16(tcp[14:16], 4096)
	// checksum, set below
	tcp[16] = 0
	tcp[17] = 0
	// urgent
	tcp[18] = 0
	tcp[19] = 0
	copy(tcp[TcpHeaderSizeWithoutExtensions:], payload)
	binary.BigEndian.PutUint16(tcp[16:18], ipPathTransportChecksum(ipPath, ipProtocolNumberTcp, tcp))
	return packet
}
