package connect

import (
	"testing"
)

func icmpTcpTestPath(version int) *IpPath {
	ipPath := udpTestPath(version)
	ipPath.Protocol = IpProtocolTcp
	ipPath.SequenceNumber = 0x1234abcd
	ipPath.Syn = true
	return ipPath
}

// The property the channel intercept depends on: the path recovered from the
// icmp embed is the same flow-map key as the original egress packet. If this
// round trip drifts, dial failures stop matching their flows and the signal
// silently does nothing.
func TestIcmpUnreachableRoundTripTcp(t *testing.T) {
	for _, version := range []int{4, 6} {
		egress := icmpTcpTestPath(version)

		packet, ok := ipOosUnreachable(egress)
		if !ok {
			t.Fatalf("v%d: build failed", version)
		}

		parsed, ok := ipParseIcmpUnreachable(packet)
		if !ok {
			t.Fatalf("v%d: parse failed on our own packet", version)
		}

		if parsed.Version != version || parsed.Protocol != IpProtocolTcp {
			t.Errorf("v%d: version/protocol mismatch: %+v", version, parsed)
		}
		if !parsed.SourceIp.Equal(egress.SourceIp) || parsed.SourcePort != egress.SourcePort {
			t.Errorf("v%d: source mismatch: %s:%d", version, parsed.SourceIp, parsed.SourcePort)
		}
		if !parsed.DestinationIp.Equal(egress.DestinationIp) || parsed.DestinationPort != egress.DestinationPort {
			t.Errorf("v%d: destination mismatch: %s:%d", version, parsed.DestinationIp, parsed.DestinationPort)
		}
		if parsed.SequenceNumber != egress.SequenceNumber {
			t.Errorf("v%d: seq %x, want %x", version, parsed.SequenceNumber, egress.SequenceNumber)
		}

		// the actual map-key equivalence the intercept uses
		switch version {
		case 4:
			if parsed.ToIp4Path() != egress.ToIp4Path() {
				t.Errorf("v4: flow map key mismatch")
			}
		case 6:
			if parsed.ToIp6Path() != egress.ToIp6Path() {
				t.Errorf("v6: flow map key mismatch")
			}
		}
	}
}

func TestIcmpUnreachableRoundTripUdp(t *testing.T) {
	for _, version := range []int{4, 6} {
		egress := udpTestPath(version)

		packet, ok := ipOosUnreachable(egress)
		if !ok {
			t.Fatalf("v%d: build failed", version)
		}
		parsed, ok := ipParseIcmpUnreachable(packet)
		if !ok {
			t.Fatalf("v%d: parse failed", version)
		}
		if parsed.Protocol != IpProtocolUdp {
			t.Errorf("v%d: protocol %v, want udp", version, parsed.Protocol)
		}
		if !parsed.SourceIp.Equal(egress.SourceIp) || parsed.SourcePort != egress.SourcePort ||
			!parsed.DestinationIp.Equal(egress.DestinationIp) || parsed.DestinationPort != egress.DestinationPort {
			t.Errorf("v%d: tuple mismatch: %+v", version, parsed)
		}
	}
}

// RFC 792 only guarantees 8 transport bytes in the embed. A router-built
// unreachable truncated to exactly that must still parse -- ports and, for
// tcp, the sequence number all sit inside those 8 bytes.
func TestIcmpUnreachableParsesMinimalEmbed(t *testing.T) {
	egress := icmpTcpTestPath(4)
	packet, ok := ipOosUnreachable(egress)
	if !ok {
		t.Fatal("build failed")
	}

	// outer ipv4 20 + icmp 8 + embedded ipv4 20 + 8 transport bytes
	truncated := packet[:Ipv4HeaderSizeWithoutExtensions+icmpUnreachableHeaderSize+Ipv4HeaderSizeWithoutExtensions+8]

	parsed, ok := ipParseIcmpUnreachable(truncated)
	if !ok {
		t.Fatal("minimal embed rejected")
	}
	if parsed.SourcePort != egress.SourcePort || parsed.DestinationPort != egress.DestinationPort {
		t.Errorf("tuple mismatch on minimal embed: %+v", parsed)
	}
	if parsed.SequenceNumber != egress.SequenceNumber {
		t.Errorf("seq %x, want %x", parsed.SequenceNumber, egress.SequenceNumber)
	}

	// one byte short of the guarantee must be rejected, not misparsed
	if _, ok := ipParseIcmpUnreachable(truncated[:len(truncated)-1]); ok {
		t.Error("accepted an embed short of 8 transport bytes")
	}
}

func TestIcmpUnreachableRejectsNonIcmp(t *testing.T) {
	forEachIpVersion(t, testIcmpUnreachableRejectsNonIcmp)
}

func testIcmpUnreachableRejectsNonIcmp(t *testing.T, ipVersion int) {
	// a plain tcp packet
	tcpPacket := ipOosTcpPacketSequence(icmpTcpTestPath(ipVersion), tcpFlagSyn, 1, nil)
	if _, ok := ipParseIcmpUnreachable(tcpPacket); ok {
		t.Error("accepted a tcp packet")
	}
	// a plain udp packet
	udpPacket := ipOosUdpPacket(udpTestPath(ipVersion), nil)
	if _, ok := ipParseIcmpUnreachable(udpPacket); ok {
		t.Error("accepted a udp packet")
	}
	// empty
	if _, ok := ipParseIcmpUnreachable(nil); ok {
		t.Error("accepted an empty packet")
	}
	// wrong icmp type: flip dest-unreachable to echo request
	packet, _ := ipOosUnreachable(udpTestPath(ipVersion))
	if ipVersion == 4 {
		packet[Ipv4HeaderSizeWithoutExtensions] = 8
	} else {
		packet[Ipv6HeaderSize] = 128
	}
	if _, ok := ipParseIcmpUnreachable(packet); ok {
		t.Error("accepted an icmp echo")
	}
}

// Documents the rollout property: a client without the intercept drops the
// signal at ParseIpPath rather than misrouting it. If ParseIpPath ever learns
// icmp, the intercept ordering in the channel receive loop must be revisited.
func TestParseIpPathStillRejectsIcmp(t *testing.T) {
	forEachIpVersion(t, testParseIpPathStillRejectsIcmp)
}

func testParseIpPathStillRejectsIcmp(t *testing.T, ipVersion int) {
	packet, ok := ipOosUnreachable(icmpTcpTestPath(ipVersion))
	if !ok {
		t.Fatal("build failed")
	}
	if _, err := ParseIpPath(packet); err == nil {
		t.Error("ParseIpPath now accepts icmp; the channel intercept ordering depends on it rejecting")
	}
}
