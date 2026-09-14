package connect

import (
	"encoding/binary"
)

// ip_ipv6_ext.go — the one IPv6 extension-header walker shared by every
// packet parser and peek in this package (IPV6.md C3). Before it existed each
// site read the transport protocol from the base header's Next Header byte,
// so any packet carrying a Hop-by-Hop, Routing, Destination Options or
// Fragment header decoded as an unsupported protocol and was silently dropped,
// mis-sharded, or fell through a fast-path peek to a full parse that could not
// handle it either.
//
// The walker is allocation free and stops at the first header that is not an
// extension header: normally the transport protocol, but also ESP (encrypted,
// nothing beyond it can be read), "no next header" (59) or a value this code
// does not know. Callers switch on `nextHeader` exactly as they switched on
// the base header byte before.
//
// A non-atomic fragment (offset != 0 or M set) ends the walk at the fragment
// header: the bytes after it are a slice of the ORIGINAL packet's fragmentable
// part, not a header chain, and `fragmented` tells the caller to reassemble
// (ip_fragment.go) before parsing. An atomic fragment (offset 0, M clear, RFC
// 6946) is walked through like any other extension header.

// IPv6 next-header values that are extension headers (RFC 8200 §4 and IANA),
// plus the terminal values the walker recognizes by name.
const (
	ipv6NextHeaderHopByHop           = 0
	ipv6NextHeaderRouting            = 43
	ipv6NextHeaderFragment           = 44
	ipv6NextHeaderEsp                = 50
	ipv6NextHeaderAuthentication     = 51
	ipv6NextHeaderNoNextHeader       = 59
	ipv6NextHeaderDestinationOptions = 60
	ipv6NextHeaderMobility           = 135
	ipv6NextHeaderHostIdentity       = 139
	ipv6NextHeaderShim6              = 140

	// the fixed size of a Fragment extension header
	ipv6FragmentHeaderSize = 8
	// bounds the walk so a crafted chain cannot loop the parser; real packets
	// carry at most a handful of extension headers
	ipv6MaxExtensionHeaderCount = 16
)

// ipv6FragmentInfo is the Fragment extension header as the walker found it.
type ipv6FragmentInfo struct {
	// headerOffset is where the fragment header starts; everything before it
	// is the unfragmentable part (RFC 8200 §4.5)
	headerOffset int
	// precedingNextHeaderFieldOffset is the offset of the Next Header byte that
	// points at the fragment header: 6 for the base header, else inside the
	// preceding extension header. Reassembly rewrites this byte to `nextHeader`
	// so the reassembled packet has no fragment header.
	precedingNextHeaderFieldOffset int
	// nextHeader is the fragment header's own Next Header: the first header of
	// the original packet's fragmentable part
	nextHeader     byte
	offset         int
	moreFragments  bool
	identification uint32
}

// ipv6HeaderWalk is the result of walkIpv6ExtensionHeaders.
type ipv6HeaderWalk struct {
	// nextHeader is the first non-extension header: the transport protocol
	// for a normal packet
	nextHeader ipProtocolNumber
	// transportOffset is where `nextHeader` starts (for a non-atomic fragment,
	// where the fragment's slice of the fragmentable part starts)
	transportOffset int
	// payloadEnd is Ipv6HeaderSize plus the declared payload length, which is
	// at most len(packet)
	payloadEnd int
	// fragment is present when a Fragment header was walked (atomic or not)
	fragment        ipv6FragmentInfo
	fragmentPresent bool
	// fragmented is set for a non-atomic fragment: the bytes at
	// transportOffset are not a header chain and the packet must be
	// reassembled before it is parsed
	fragmented bool
}

// isIpv6ExtensionHeader reports whether a next-header value is one the walker
// steps through. Used by hot paths to skip the walk for the overwhelmingly
// common packet whose base header points straight at tcp, udp or icmpv6.
func isIpv6ExtensionHeader(nextHeader byte) bool {
	switch nextHeader {
	case ipv6NextHeaderHopByHop,
		ipv6NextHeaderRouting,
		ipv6NextHeaderFragment,
		ipv6NextHeaderAuthentication,
		ipv6NextHeaderDestinationOptions,
		ipv6NextHeaderMobility,
		ipv6NextHeaderHostIdentity,
		ipv6NextHeaderShim6:
		return true
	default:
		return false
	}
}

// walkIpv6ExtensionHeaders validates the base header and walks the extension
// chain. ok is false when the packet is not IPv6, is shorter than its declared
// payload length, or any extension header is truncated, repeated where it may
// not be, or exceeds the header count bound. Nothing is allocated.
func walkIpv6ExtensionHeaders(packet []byte) (walk ipv6HeaderWalk, ok bool) {
	if len(packet) < Ipv6HeaderSize || packet[0]>>4 != 6 {
		return
	}
	payloadByteCount := int(binary.BigEndian.Uint16(packet[4:6]))
	end := Ipv6HeaderSize + payloadByteCount
	if len(packet) < end {
		return
	}
	nextHeader := packet[6]
	offset := Ipv6HeaderSize
	nextHeaderFieldOffset := 6
	for i := 0; i < ipv6MaxExtensionHeaderCount; i += 1 {
		switch nextHeader {
		case ipv6NextHeaderHopByHop,
			ipv6NextHeaderRouting,
			ipv6NextHeaderDestinationOptions,
			ipv6NextHeaderMobility,
			ipv6NextHeaderHostIdentity,
			ipv6NextHeaderShim6:
			// next header, then length in 8-octet units not counting the
			// first 8 octets
			if end < offset+8 {
				return
			}
			headerByteCount := (int(packet[offset+1]) + 1) * 8
			if end < offset+headerByteCount {
				return
			}
			nextHeader = packet[offset]
			nextHeaderFieldOffset = offset
			offset += headerByteCount
		case ipv6NextHeaderAuthentication:
			// rfc 4302: length in 4-octet units minus 2
			if end < offset+8 {
				return
			}
			headerByteCount := (int(packet[offset+1]) + 2) * 4
			if end < offset+headerByteCount {
				return
			}
			nextHeader = packet[offset]
			nextHeaderFieldOffset = offset
			offset += headerByteCount
		case ipv6NextHeaderFragment:
			if end < offset+ipv6FragmentHeaderSize {
				return
			}
			if walk.fragmentPresent {
				// a second fragment header is malformed
				return
			}
			offsetAndFlags := binary.BigEndian.Uint16(packet[offset+2 : offset+4])
			walk.fragment = ipv6FragmentInfo{
				headerOffset:                   offset,
				precedingNextHeaderFieldOffset: nextHeaderFieldOffset,
				nextHeader:                     packet[offset],
				offset:                         int(offsetAndFlags &^ 0x7),
				moreFragments:                  offsetAndFlags&0x1 != 0,
				identification:                 binary.BigEndian.Uint32(packet[offset+4 : offset+8]),
			}
			walk.fragmentPresent = true
			nextHeader = packet[offset]
			nextHeaderFieldOffset = offset
			offset += ipv6FragmentHeaderSize
			if walk.fragment.offset != 0 || walk.fragment.moreFragments {
				// the rest is a slice of the fragmentable part, not headers
				walk.nextHeader = ipProtocolNumber(nextHeader)
				walk.transportOffset = offset
				walk.payloadEnd = end
				walk.fragmented = true
				return walk, true
			}
			// an atomic fragment carries a complete packet after the header
		default:
			walk.nextHeader = ipProtocolNumber(nextHeader)
			walk.transportOffset = offset
			walk.payloadEnd = end
			return walk, true
		}
	}
	// too many extension headers
	return
}

// ipv6TransportOffset is the peek form of the walk: the transport protocol
// and where it starts, without exposing the fragment bookkeeping. A
// non-atomic fragment reports ok false, since there is no transport header to
// find. The common no-extension packet is answered without the walk.
func ipv6TransportOffset(packet []byte) (nextHeader ipProtocolNumber, transportOffset int, payloadEnd int, ok bool) {
	if len(packet) < Ipv6HeaderSize || packet[0]>>4 != 6 {
		return
	}
	if !isIpv6ExtensionHeader(packet[6]) {
		payloadEnd = Ipv6HeaderSize + int(binary.BigEndian.Uint16(packet[4:6]))
		if len(packet) < payloadEnd {
			return
		}
		return ipProtocolNumber(packet[6]), Ipv6HeaderSize, payloadEnd, true
	}
	walk, walkOk := walkIpv6ExtensionHeaders(packet)
	if !walkOk || walk.fragmented {
		return
	}
	return walk.nextHeader, walk.transportOffset, walk.payloadEnd, true
}

// isIpv6FragmentPacket reports whether the packet is a non-atomic IPv6
// fragment, which must be reassembled before it is parsed. The v6 analog of
// isIpv4FragmentPacket.
func isIpv6FragmentPacket(packet []byte) bool {
	if len(packet) < Ipv6HeaderSize || packet[0]>>4 != 6 {
		return false
	}
	if !isIpv6ExtensionHeader(packet[6]) {
		return false
	}
	walk, ok := walkIpv6ExtensionHeaders(packet)
	return ok && walk.fragmented
}
