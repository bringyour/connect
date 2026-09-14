package connect

// Standards payload classifier for protocols that egress treats as sanctioned
// when a flow would otherwise merely look fully encrypted. Recognizing one of
// these on the opening packets is what keeps the entropy heuristic from dropping
// legitimate encrypted web and real-time communication traffic.
//
// All checks are clean-room from the RFCs: TLS RFC 8446/5246, DTLS RFC 6347/9147,
// QUIC RFC 9000/9369, STUN RFC 8489, TURN RFC 8656, and RTP/RTCP RFC 3550 with
// the RFC 7983 demultiplexing ranges. TURN and RTCP use strict single-payload
// framing. RTP exposes a parsed header to the DMCA flow state, which requires
// continuity across packets before allowing the flow.

import (
	"encoding/binary"
)

// WebStandardSettings selects which web and real-time communication standards
// are recognized (and therefore allowed through the egress encrypted-traffic
// heuristic). Use DefaultWebStandardSettings for reasonable defaults.
type WebStandardSettings struct {
	// Enabled is the master switch. When false, match always returns false (no
	// standard is recognized, so the encrypted heuristic is not vetoed).
	Enabled bool
	Tls     bool
	Dtls    bool
	Quic    bool
	Stun    bool
	Turn    bool
	Rtp     bool
	Rtcp    bool
}

func DefaultWebStandardSettings() *WebStandardSettings {
	return &WebStandardSettings{
		Enabled: true,
		Tls:     true,
		Dtls:    true,
		Quic:    true,
		Stun:    true,
		Turn:    true,
		Rtp:     true,
		Rtcp:    true,
	}
}

type webStandardDetector struct {
	settings *WebStandardSettings
}

func newWebStandardDetector(settings *WebStandardSettings) *webStandardDetector {
	return &webStandardDetector{
		settings: settings,
	}
}

// match reports whether payload is a complete packet/frame of a recognized,
// enabled stateless standard for ipPath's protocol.
func (self *webStandardDetector) match(ipPath *IpPath, payload []byte) bool {
	if !self.settings.Enabled {
		return false
	}
	switch ipPath.Protocol {
	case IpProtocolTcp:
		return (self.settings.Tls && isTlsClientHello(payload)) ||
			(self.settings.Stun && isStunStream(payload)) ||
			(self.settings.Turn && isTurnChannelDataStream(payload))
	case IpProtocolUdp:
		return (self.settings.Dtls && isDtlsClientHello(payload)) ||
			(self.settings.Quic && isQuicLongHeader(payload)) ||
			(self.settings.Stun && isStun(payload)) ||
			(self.settings.Turn && isTurnChannelData(payload)) ||
			(self.settings.Rtcp && isRtcp(payload))
	}
	return false
}

// rtpHeader parses a possible UDP RTP/SRTP packet. A structural match alone is
// deliberately not returned by match: RFC 7983's first-byte RTP range covers a
// quarter of all byte values, so dmcaFlowState validates sequence continuity
// across packets before issuing a terminal allow.
func (self *webStandardDetector) rtpHeader(ipPath *IpPath, payload []byte) (rtpHeader, bool) {
	if !self.settings.Enabled || !self.settings.Rtp || ipPath.Protocol != IpProtocolUdp {
		return rtpHeader{}, false
	}
	return parseRtpHeader(payload)
}

const (
	// DTLS uses 1's-complement version numbers (RFC 6347/9147).
	dtlsVersionMajor          = 0xFE
	dtlsVersion10Minor        = 0xFF
	dtlsVersion12Minor        = 0xFD
	stunMagicCookie           = 0x2112A442
	quicVersion1       uint32 = 0x00000001
	quicVersion2       uint32 = 0x6B3343CF
)

// TLS RFC 8446/5246 record: handshake content type, 0x03xx version, first
// handshake byte == ClientHello (0x01).
func isTlsClientHello(b []byte) bool {
	if len(b) < 6 {
		return false
	}
	if TlsContentTypeHandshake != b[0] {
		return false
	}
	if 0x03 != b[1] || 0x04 < b[2] {
		return false
	}
	return 0x01 == b[5]
}

// DTLS RFC 6347/9147 record (13-byte header) carrying a ClientHello (0x01).
func isDtlsClientHello(b []byte) bool {
	if len(b) < 14 {
		return false
	}
	if TlsContentTypeHandshake != b[0] {
		return false
	}
	if dtlsVersionMajor != b[1] {
		return false
	}
	if dtlsVersion10Minor != b[2] && dtlsVersion12Minor != b[2] {
		return false
	}
	return 0x01 == b[13]
}

// QUIC RFC 9000/9369 long header: header-form + fixed bit set, recognized version.
func isQuicLongHeader(b []byte) bool {
	if len(b) < 5 {
		return false
	}
	if 0xc0 != b[0]&0xc0 {
		return false
	}
	version := binary.BigEndian.Uint32(b[1:5])
	switch {
	case quicVersion1 == version, quicVersion2 == version:
		return true
	case 0x00000000 == version:
		// version negotiation
		return true
	case 0x0a0a0a0a == version&0x0f0f0f0f:
		// forced-negotiation greasing pattern (RFC 9000 section 15)
		return true
	case 0xff000000 == version&0xffffff00:
		// IETF drafts
		return true
	}
	return false
}

// STUN RFC 8489 and RFC 7983: first byte 0-3, message length a multiple of 4,
// fixed magic cookie at offset 4, and an exact framed length. Covers WebRTC
// connectivity checks and TURN control messages over UDP and TCP.
func isStun(b []byte) bool {
	n, ok := stunMessageSize(b)
	return ok && n == len(b)
}

func isStunStream(b []byte) bool {
	matched := false
	for 0 < len(b) {
		n, ok := stunMessageSize(b)
		if !ok {
			return false
		}
		matched = true
		b = b[n:]
	}
	return matched
}

func stunMessageSize(b []byte) (int, bool) {
	if len(b) < 20 {
		return 0, false
	}
	if 3 < b[0] {
		return 0, false
	}
	messageLength := int(binary.BigEndian.Uint16(b[2:4]))
	if 0 != messageLength%4 {
		return 0, false
	}
	if stunMagicCookie != binary.BigEndian.Uint32(b[4:8]) {
		return 0, false
	}
	total := 20 + messageLength
	return total, total <= len(b)
}

// TURN RFC 8656 ChannelData, restricted to the 0x4000-0x4fff channel range
// allocated by RFC 7983 for multiplexing with DTLS-SRTP. UDP permits either an
// exact data length or optional alignment padding; TCP requires padding and may
// coalesce multiple complete frames in one segment.
func isTurnChannelData(b []byte) bool {
	if len(b) < 4 || !isRfc7983TurnChannel(binary.BigEndian.Uint16(b[:2])) {
		return false
	}
	total := 4 + int(binary.BigEndian.Uint16(b[2:4]))
	if total == len(b) {
		return true
	}
	return align4(total) == len(b)
}

func isTurnChannelDataStream(b []byte) bool {
	matched := false
	for 0 < len(b) {
		if len(b) < 4 || !isRfc7983TurnChannel(binary.BigEndian.Uint16(b[:2])) {
			return false
		}
		total := align4(4 + int(binary.BigEndian.Uint16(b[2:4])))
		if len(b) < total {
			return false
		}
		matched = true
		b = b[total:]
	}
	return matched
}

func isRfc7983TurnChannel(channel uint16) bool {
	return 0x4000 <= channel && channel <= 0x4fff
}

func align4(n int) int {
	return (n + 3) &^ 3
}

// RTP RFC 3550. The fixed RTP/SRTP header remains visible even when the media
// payload is encrypted. RTCP's RFC 7983-overlapping second-byte range is
// excluded and handled by isRtcp.
type rtpHeader struct {
	ssrc        uint32
	timestamp   uint32
	sequence    uint16
	payloadType uint8
}

func parseRtpHeader(b []byte) (rtpHeader, bool) {
	if len(b) < 12 || b[0]&0xc0 != 0x80 {
		return rtpHeader{}, false
	}
	// RFC 5761 reserves the RTCP packet-type range when RTP and RTCP are
	// multiplexed on the same port. The marker bit is part of this byte.
	if 192 <= b[1] && b[1] <= 223 {
		return rtpHeader{}, false
	}

	headerLength := 12 + 4*int(b[0]&0x0f)
	if len(b) < headerLength {
		return rtpHeader{}, false
	}
	if 0 != b[0]&0x10 { // X: header extension present
		if len(b) < headerLength+4 {
			return rtpHeader{}, false
		}
		extensionWords := int(binary.BigEndian.Uint16(b[headerLength+2 : headerLength+4]))
		headerLength += 4 + 4*extensionWords
		if len(b) < headerLength {
			return rtpHeader{}, false
		}
	}
	// A zero-length media payload provides little evidence and is not useful to
	// the encrypted-traffic exception. SRTP authentication tags count here.
	if len(b) == headerLength {
		return rtpHeader{}, false
	}

	return rtpHeader{
		ssrc:        binary.BigEndian.Uint32(b[8:12]),
		timestamp:   binary.BigEndian.Uint32(b[4:8]),
		sequence:    binary.BigEndian.Uint16(b[2:4]),
		payloadType: b[1] & 0x7f,
	}, true
}

// RTCP RFC 3550 compound/reduced-size framing. Every sub-packet must be
// complete, version 2, use the RFC 7983 RTCP packet-type range, satisfy the
// base length for its type, and consume the datagram exactly. Padding is valid
// only on the final sub-packet.
func isRtcp(b []byte) bool {
	if len(b) < 8 || len(b)%4 != 0 {
		return false
	}
	for offset := 0; offset < len(b); {
		if len(b)-offset < 4 {
			return false
		}
		first := b[offset]
		packetType := b[offset+1]
		if first&0xc0 != 0x80 || packetType < 192 || 223 < packetType {
			return false
		}
		packetLength := (int(binary.BigEndian.Uint16(b[offset+2:offset+4])) + 1) * 4
		if packetLength < 8 || len(b)-offset < packetLength {
			return false
		}
		contentLength := packetLength
		padded := 0 != first&0x20
		if padded {
			if offset+packetLength != len(b) {
				return false
			}
			paddingLength := int(b[offset+packetLength-1])
			if paddingLength == 0 || packetLength-4 < paddingLength {
				return false
			}
			contentLength -= paddingLength
		}
		count := int(first & 0x1f)
		if contentLength < minimumRtcpPacketLength(packetType, count) {
			return false
		}
		offset += packetLength
	}
	return true
}

func minimumRtcpPacketLength(packetType byte, count int) int {
	switch packetType {
	case 200: // Sender Report: sender info plus reception-report blocks.
		return 28 + 24*count
	case 201: // Receiver Report.
		return 8 + 24*count
	case 202, 203: // SDES chunks or BYE SSRCs.
		return 4 + 4*count
	case 204: // APP: SSRC plus four-byte name.
		return 12
	case 205, 206: // RTPFB / PSFB: sender and media SSRCs.
		return 12
	case 207: // Extended Report: sender SSRC.
		return 8
	default:
		// Other registered/experimental RTCP packet types still require a
		// common header and an SSRC-sized body before they are trusted.
		return 8
	}
}
