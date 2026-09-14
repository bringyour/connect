package connect

import (
	"encoding/binary"
	"testing"
)

func turnChannelData(channel uint16, data []byte, padded bool) []byte {
	total := 4 + len(data)
	if padded {
		total = align4(total)
	}
	b := make([]byte, total)
	binary.BigEndian.PutUint16(b[0:2], channel)
	binary.BigEndian.PutUint16(b[2:4], uint16(len(data)))
	copy(b[4:], data)
	return b
}

func rtpPacket(ssrc uint32, sequence uint16, timestamp uint32, payloadType byte) []byte {
	b := make([]byte, 12+512)
	b[0] = 0x80
	b[1] = payloadType & 0x7f
	binary.BigEndian.PutUint16(b[2:4], sequence)
	binary.BigEndian.PutUint32(b[4:8], timestamp)
	binary.BigEndian.PutUint32(b[8:12], ssrc)
	copy(b[12:], encryptedPayload(512))
	return b
}

func rtcpReceiverReport(ssrc uint32) []byte {
	b := make([]byte, 8)
	b[0] = 0x80
	b[1] = 201
	binary.BigEndian.PutUint16(b[2:4], 1) // two 32-bit words total
	binary.BigEndian.PutUint32(b[4:8], ssrc)
	return b
}

func rtcpPictureLossIndication(senderSsrc, mediaSsrc uint32) []byte {
	b := make([]byte, 12)
	b[0] = 0x81 // V=2, FMT=1
	b[1] = 206  // payload-specific feedback
	binary.BigEndian.PutUint16(b[2:4], 2)
	binary.BigEndian.PutUint32(b[4:8], senderSsrc)
	binary.BigEndian.PutUint32(b[8:12], mediaSsrc)
	return b
}

func TestWebStandardDetection(t *testing.T) {
	d := newWebStandardDetector(DefaultWebStandardSettings())
	cases := []struct {
		name  string
		proto IpProtocol
		b     []byte
		want  bool
	}{
		{"tls", IpProtocolTcp, tlsClientHello(), true},
		{"dtls", IpProtocolUdp, dtlsClientHello(), true},
		{"quic", IpProtocolUdp, quicInitial(), true},
		{"stun udp", IpProtocolUdp, stunBinding(), true},
		{"stun tcp", IpProtocolTcp, stunBinding(), true},
		{"turn channel udp", IpProtocolUdp, turnChannelData(0x4001, []byte{1, 2, 3}, false), true},
		{"turn channel tcp", IpProtocolTcp, turnChannelData(0x4fff, []byte{1, 2, 3}, true), true},
		{"rtcp", IpProtocolUdp, rtcpReceiverReport(0x12345678), true},
		{"rtp requires flow state", IpProtocolUdp, rtpPacket(0x12345678, 10, 48000, 111), false},
		{"random not standard", IpProtocolUdp, encryptedPayload(256), false},
		{"bittorrent handshake not standard", IpProtocolTcp, btHandshake(), false},
		{"tls bytes over udp not standard", IpProtocolUdp, tlsClientHello(), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := d.match(&IpPath{Protocol: c.proto}, c.b); got != c.want {
				t.Fatalf("match = %v, want %v", got, c.want)
			}
		})
	}
}

func TestStunFraming(t *testing.T) {
	valid := stunBinding()
	if !isStun(valid) || !isStunStream(valid) {
		t.Fatal("valid STUN message did not match")
	}
	two := append(append([]byte{}, valid...), valid...)
	if isStun(two) {
		t.Fatal("two STUN messages matched one UDP datagram frame")
	}
	if !isStunStream(two) {
		t.Fatal("two complete STUN messages did not match TCP stream framing")
	}

	tests := []struct {
		name string
		b    []byte
	}{
		{"short", valid[:19]},
		{"first byte outside rfc7983 stun range", append([]byte{4}, valid[1:]...)},
		{"bad cookie", func() []byte {
			b := append([]byte{}, valid...)
			binary.BigEndian.PutUint32(b[4:8], 0)
			return b
		}()},
		{"declared attribute length truncated", func() []byte {
			b := append([]byte{}, valid...)
			binary.BigEndian.PutUint16(b[2:4], 4)
			return b
		}()},
		{"length not aligned", func() []byte {
			b := append([]byte{}, valid...)
			binary.BigEndian.PutUint16(b[2:4], 1)
			return append(b, 0)
		}()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if isStun(tt.b) || isStunStream(tt.b) {
				t.Fatal("malformed STUN payload matched")
			}
		})
	}
}

func TestTurnChannelDataFraming(t *testing.T) {
	data := []byte{0xde, 0xad, 0xbe}
	if !isTurnChannelData(turnChannelData(0x4000, data, false)) {
		t.Fatal("exact UDP ChannelData frame did not match")
	}
	if !isTurnChannelData(turnChannelData(0x4fff, data, true)) {
		t.Fatal("padded UDP ChannelData frame did not match")
	}
	if !isTurnChannelData(turnChannelData(0x4001, nil, false)) {
		t.Fatal("zero-length UDP ChannelData frame is valid")
	}
	if !isTurnChannelDataStream(turnChannelData(0x4001, data, true)) {
		t.Fatal("padded TCP ChannelData frame did not match")
	}
	frames := append(
		turnChannelData(0x4001, data, true),
		turnChannelData(0x4ffe, []byte{1, 2, 3, 4}, true)...,
	)
	if !isTurnChannelDataStream(frames) {
		t.Fatal("coalesced TCP ChannelData frames did not match")
	}

	tests := []struct {
		name string
		udp  []byte
		tcp  []byte
	}{
		{"channel below rfc7983 range", turnChannelData(0x3fff, data, false), turnChannelData(0x3fff, data, true)},
		{"channel above rfc7983 range", turnChannelData(0x5000, data, false), turnChannelData(0x5000, data, true)},
		{"truncated", turnChannelData(0x4001, data, false)[:6], turnChannelData(0x4001, data, true)[:6]},
		{"surplus beyond optional padding", append(turnChannelData(0x4001, data, true), 0), append(turnChannelData(0x4001, data, true), 0)},
		{"tcp missing required padding", nil, turnChannelData(0x4001, data, false)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.udp != nil && isTurnChannelData(tt.udp) {
				t.Error("malformed UDP ChannelData matched")
			}
			if tt.tcp != nil && isTurnChannelDataStream(tt.tcp) {
				t.Error("malformed TCP ChannelData matched")
			}
		})
	}
}

func TestRtpHeaderParsing(t *testing.T) {
	b := rtpPacket(0x12345678, 65535, 0xfffffff0, 111)
	header, ok := parseRtpHeader(b)
	if !ok {
		t.Fatal("valid RTP packet did not parse")
	}
	if header.ssrc != 0x12345678 || header.sequence != 65535 || header.timestamp != 0xfffffff0 || header.payloadType != 111 {
		t.Fatalf("parsed unexpected RTP header: %+v", header)
	}

	withCsrc := append([]byte{}, b...)
	withCsrc[0] = 0x82
	withCsrc = append(withCsrc[:12], append(make([]byte, 8), withCsrc[12:]...)...)
	if _, ok := parseRtpHeader(withCsrc); !ok {
		t.Fatal("valid RTP CSRC list did not parse")
	}

	withExtension := append([]byte{}, b...)
	withExtension[0] = 0x90
	extension := make([]byte, 12)
	binary.BigEndian.PutUint16(extension[2:4], 2)
	withExtension = append(withExtension[:12], append(extension, withExtension[12:]...)...)
	if _, ok := parseRtpHeader(withExtension); !ok {
		t.Fatal("valid RTP header extension did not parse")
	}

	tests := []struct {
		name string
		b    []byte
	}{
		{"short", b[:11]},
		{"wrong version", append([]byte{0x40}, b[1:]...)},
		{"rtcp packet type", rtcpReceiverReport(0x12345678)},
		{"truncated csrc list", func() []byte {
			p := append([]byte{}, b[:13]...)
			p[0] = 0x82
			return p
		}()},
		{"truncated extension header", func() []byte {
			p := append([]byte{}, b[:14]...)
			p[0] = 0x90
			return p
		}()},
		{"truncated extension body", func() []byte {
			p := append([]byte{}, b[:18]...)
			p[0] = 0x90
			binary.BigEndian.PutUint16(p[14:16], 2)
			return p
		}()},
		{"no media payload", b[:12]},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, ok := parseRtpHeader(tt.b); ok {
				t.Fatal("malformed RTP packet parsed")
			}
		})
	}
}

func TestRtcpFraming(t *testing.T) {
	rr := rtcpReceiverReport(0x12345678)
	pli := rtcpPictureLossIndication(0x11111111, 0x22222222)
	if !isRtcp(rr) {
		t.Fatal("valid receiver report did not match")
	}
	if !isRtcp(pli) {
		t.Fatal("valid reduced-size feedback did not match")
	}
	if !isRtcp(append(append([]byte{}, rr...), pli...)) {
		t.Fatal("valid compound RTCP did not match")
	}
	padded := append(append([]byte{}, rr...), 0, 0, 0, 4)
	padded[0] |= 0x20
	binary.BigEndian.PutUint16(padded[2:4], 2)
	if !isRtcp(padded) {
		t.Fatal("valid padded RTCP did not match")
	}

	tests := []struct {
		name string
		b    []byte
	}{
		{"short", rr[:7]},
		{"not multiple of four", append(append([]byte{}, rr...), 0)},
		{"wrong version", append([]byte{0x40}, rr[1:]...)},
		{"packet type outside rtcp range", append([]byte{0x80, 100}, rr[2:]...)},
		{"declared length truncated", func() []byte {
			b := append([]byte{}, rr...)
			binary.BigEndian.PutUint16(b[2:4], 2)
			return b
		}()},
		{"sender report too short", func() []byte {
			b := append([]byte{}, rr...)
			b[1] = 200
			return b
		}()},
		{"padding on non-final packet", append([]byte{0xa0, 201, 0, 1, 0, 0, 0, 4}, rr...)},
		{"zero padding length", func() []byte {
			b := append(append([]byte{}, rr...), 0, 0, 0, 0)
			b[0] |= 0x20
			binary.BigEndian.PutUint16(b[2:4], 2)
			return b
		}()},
		{"padding consumes required body", []byte{0xa0, 201, 0, 1, 0, 0, 0, 4}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if isRtcp(tt.b) {
				t.Fatal("malformed RTCP payload matched")
			}
		})
	}
}

func TestWebStandardToggles(t *testing.T) {
	// Disabling one protocol stops it matching but leaves the others.
	s := DefaultWebStandardSettings()
	s.Quic = false
	s.Turn = false
	s.Rtp = false
	s.Rtcp = false
	d := newWebStandardDetector(s)
	if d.match(&IpPath{Protocol: IpProtocolUdp}, quicInitial()) {
		t.Fatal("quic disabled but still matched")
	}
	if d.match(&IpPath{Protocol: IpProtocolUdp}, turnChannelData(0x4001, []byte{1}, false)) {
		t.Fatal("turn disabled but still matched")
	}
	if d.match(&IpPath{Protocol: IpProtocolUdp}, rtcpReceiverReport(1)) {
		t.Fatal("rtcp disabled but still matched")
	}
	if _, ok := d.rtpHeader(&IpPath{Protocol: IpProtocolUdp}, rtpPacket(1, 1, 1, 111)); ok {
		t.Fatal("rtp disabled but still parsed")
	}
	if !d.match(&IpPath{Protocol: IpProtocolUdp}, stunBinding()) {
		t.Fatal("stun still enabled should match")
	}

	// Master switch off disables stateless and stateful recognition.
	off := DefaultWebStandardSettings()
	off.Enabled = false
	d2 := newWebStandardDetector(off)
	if d2.match(&IpPath{Protocol: IpProtocolTcp}, tlsClientHello()) {
		t.Fatal("detector disabled but tls matched")
	}
	if _, ok := d2.rtpHeader(&IpPath{Protocol: IpProtocolUdp}, rtpPacket(1, 1, 1, 111)); ok {
		t.Fatal("detector disabled but rtp parsed")
	}
}
