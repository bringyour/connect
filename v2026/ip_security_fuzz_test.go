package connect

import (
	"context"
	"encoding/binary"
	"net"
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

// fuzzIpv4Packet hand-rolls a minimal IPv4 packet (no options, no checksum — the
// parser does not verify checksums) so the fuzz seeds start from valid shapes.
func fuzzIpv4Packet(proto byte, srcPort uint16, dstPort uint16, payload []byte) []byte {
	transportLen := 8 // udp header
	if proto == 6 {
		transportLen = 20 // tcp header, no options
	}
	b := make([]byte, 20+transportLen+len(payload))
	b[0] = 0x45 // version 4, ihl 5
	binary.BigEndian.PutUint16(b[2:], uint16(len(b)))
	b[8] = 64 // ttl
	b[9] = proto
	copy(b[12:16], net.IPv4(10, 0, 0, 1).To4())
	copy(b[16:20], net.IPv4(203, 0, 113, 7).To4())
	binary.BigEndian.PutUint16(b[20:], srcPort)
	binary.BigEndian.PutUint16(b[22:], dstPort)
	switch proto {
	case 6:
		b[20+12] = 0x50 // data offset 5 words
	case 17:
		binary.BigEndian.PutUint16(b[24:], uint16(8+len(payload)))
	}
	copy(b[20+transportLen:], payload)
	return b
}

// FuzzParseIpPathWithPayload fuzzes the raw packet parser that fronts every
// security-policy decision and every provider egress. It must never panic, and on
// success the parsed path must be internally coherent (a lying header must not
// produce a payload slice outside the packet).
func FuzzParseIpPathWithPayload(f *testing.F) {
	f.Add(fuzzIpv4Packet(6, 41000, 8443, tlsClientHello()))
	f.Add(fuzzIpv4Packet(17, 41001, 51413, btHandshake()))
	f.Add(fuzzIpv4Packet(17, 41002, 443, quicInitial()))
	f.Add(fuzzIpv4Packet(6, 41003, 8080, []byte("GET / HTTP/1.1\r\nHost: x\r\n\r\n")))
	// truncations and lies
	f.Add(fuzzIpv4Packet(6, 41000, 8443, nil)[:7])  // mid-header
	f.Add(fuzzIpv4Packet(6, 41000, 8443, nil)[:21]) // mid-transport
	f.Add([]byte{0x60, 0, 0, 0, 0, 0, 17, 64})      // ipv6 fragmentary
	f.Add([]byte{0x45})                             // one byte
	f.Add([]byte{})                                 // empty
	lyingIhl := fuzzIpv4Packet(17, 1, 2, []byte{1, 2, 3})
	lyingIhl[0] = 0x4f // ihl 15 words > packet
	f.Add(lyingIhl)

	f.Fuzz(func(t *testing.T, packet []byte) {
		ipPath, payload, err := ParseIpPathWithPayload(packet)
		if err != nil {
			return
		}
		if ipPath == nil {
			t.Fatal("nil ipPath with nil error")
		}
		if ipPath.Version != 4 && ipPath.Version != 6 {
			t.Fatalf("parsed version %d", ipPath.Version)
		}
		if len(payload) > len(packet) {
			t.Fatalf("payload (%d) larger than packet (%d)", len(payload), len(packet))
		}
	})
}

// FuzzSecurityPolicyInspectEgress fuzzes the full egress decision chain (CFAA gate ->
// DMCA DPI incl. the bencode/uTP/QUIC/STUN/TURN/RTP/RTCP/TLS matchers -> encrypted heuristic) with
// arbitrary payloads and 5-tuples. The policy must never panic and must return one of
// the defined results. Flow state accumulates across iterations (shared policy), which
// also fuzzes the state machine's packet-sequence handling, not just single packets.
func FuzzSecurityPolicyInspectEgress(f *testing.F) {
	ctx, cancel := context.WithCancel(context.Background())
	f.Cleanup(cancel)

	dmca := DefaultDmcaSecurityPolicySettings()
	dmca.FlowTtl = 0    // no scan goroutine
	dmca.MaxFlows = 512 // bound state across the whole fuzz run
	policy := NewSecurityPolicy(ctx, DefaultCfaaSecurityPolicySettings(), dmca, DefaultWebStandardSettings(), DefaultSecurityPolicyStatsCollector())

	f.Add(uint8(6), uint16(41000), uint16(8443), true, tlsClientHello())
	f.Add(uint8(6), uint16(41001), uint16(51413), false, btHandshake())
	f.Add(uint8(17), uint16(41002), uint16(51415), false, []byte("d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe"))
	f.Add(uint8(17), uint16(41003), uint16(443), false, quicInitial())
	f.Add(uint8(17), uint16(41004), uint16(3478), false, stunBinding())
	f.Add(uint8(17), uint16(41005), uint16(50000), false, encryptedPayload(512))
	f.Add(uint8(6), uint16(41006), uint16(6881), true, []byte{})
	f.Add(uint8(17), uint16(41007), uint16(123), false, []byte("x"))
	f.Add(uint8(17), uint16(41008), uint16(3478), false, turnChannelData(0x4001, encryptedPayload(64), false))
	f.Add(uint8(17), uint16(41009), uint16(50000), false, rtpPacket(1, 1, 960, 111))
	f.Add(uint8(17), uint16(41010), uint16(50000), false, rtcpReceiverReport(1))

	f.Fuzz(func(t *testing.T, proto uint8, srcPort uint16, dstPort uint16, syn bool, payload []byte) {
		var ipProto IpProtocol
		switch proto % 3 {
		case 0:
			ipProto = IpProtocolTcp
		case 1:
			ipProto = IpProtocolUdp
		default:
			ipProto = IpProtocol(proto) // arbitrary protocol values must also be safe
		}
		ipPath := &IpPath{
			Version:         4,
			Protocol:        ipProto,
			SourceIp:        net.IPv4(10, 0, 0, 2),
			SourcePort:      int(srcPort),
			DestinationIp:   net.IPv4(203, 0, 113, 9),
			DestinationPort: int(dstPort),
			Syn:             syn,
		}
		r, err := policy.InspectEgress(protocol.ProvideMode_Public, ipPath, payload)
		if err != nil {
			return
		}
		switch r {
		case SecurityPolicyResultAllow, SecurityPolicyResultDrop, SecurityPolicyResultIncident:
		default:
			t.Fatalf("undefined result %v", r)
		}
		policy.RefreshEgress(ipPath)
		policy.RefreshIngress(ipPath.Reverse())
	})
}
