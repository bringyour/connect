package connect

import (
	"encoding/binary"
	"net"
	"net/netip"
	"testing"
)

// ip_ipv6_dns_tcp_test.go — DNS-over-TCP interception in both families
// (IPV6.md C5): the address rewrite that redirects a client's TCP/53 flow to
// the internal server and back, the flow bookkeeping keyed by client
// address, and the fail-closed rule when the stack has no address for the
// client's family.

func dnsTcpTestPath(ipVersion int) *IpPath {
	ipPath := udpTestPath(ipVersion)
	ipPath.Protocol = IpProtocolTcp
	ipPath.DestinationPort = 53
	ipPath.SequenceNumber = 0x100
	return ipPath
}

func testDnsTcpLocalAddr(ipVersion int) netip.Addr {
	if ipVersion == 6 {
		return netip.MustParseAddr("fd00:7572:6e65::53")
	}
	return netip.MustParseAddr("169.254.0.53")
}

// checksums must be valid after a rewrite: the v4 header checksum and the
// transport checksum with the family's pseudo header both sum to zero
func assertDnsTcpChecksums(t *testing.T, packet []byte) {
	t.Helper()
	switch packet[0] >> 4 {
	case 4:
		headerByteCount := int(packet[0]&0x0f) * 4
		if got := checksumFinish(checksumAdd(0, packet[:headerByteCount])); got != 0 {
			t.Fatalf("v4 header checksum residue = %x", got)
		}
		transport := packet[headerByteCount:int(binary.BigEndian.Uint16(packet[2:4]))]
		if got := transportChecksum(ipProtocolNumberTcp, packet[12:16], packet[16:20], transport); got != 0 {
			t.Fatalf("v4 tcp checksum residue = %x", got)
		}
	case 6:
		_, transportOffset, payloadEnd, ok := ipv6TransportOffset(packet)
		if !ok {
			t.Fatal("v6 transport not found")
		}
		transport := packet[transportOffset:payloadEnd]
		if got := transportChecksum(ipProtocolNumberTcp, packet[8:24], packet[24:40], transport); got != 0 {
			t.Fatalf("v6 tcp checksum residue = %x", got)
		}
	}
}

func TestRewriteDnsTcpAddressDualstack(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ipPath := dnsTcpTestPath(ipVersion)
		localAddr := testDnsTcpLocalAddr(ipVersion)
		query := []byte{0, 12, 0x12, 0x34, 1, 0, 0, 1, 0, 0, 0, 0, 0, 0}

		// destination rewrite: the client's query redirected to the server
		packet := MessagePoolCopy(ipOosTcpPacketSequence(ipPath, tcpFlagAck|tcpFlagPsh, 1, query))
		defer MessagePoolReturn(packet)
		if !rewriteDnsTcpAddress(packet, localAddr, false) {
			t.Fatal("destination rewrite refused")
		}
		rewritten, payload, err := ParseIpPathWithPayload(packet)
		if err != nil {
			t.Fatal(err)
		}
		if got, _ := netIPAddr(rewritten.DestinationIp); got != localAddr {
			t.Fatalf("destination = %v, want %v", got, localAddr)
		}
		if !rewritten.SourceIp.Equal(ipPath.SourceIp) || rewritten.SourcePort != ipPath.SourcePort ||
			rewritten.DestinationPort != 53 || string(payload) != string(query) {
			t.Fatalf("rewrite changed more than the destination: %+v %x", rewritten, payload)
		}
		assertDnsTcpChecksums(t, packet)

		// source rewrite: the server's reply restored to the client's server
		serverAddr, _ := netIPAddr(ipPath.DestinationIp)
		if !rewriteDnsTcpAddress(packet, serverAddr, true) {
			t.Fatal("source rewrite refused")
		}
		restored, _, err := ParseIpPathWithPayload(packet)
		if err != nil {
			t.Fatal(err)
		}
		if !restored.SourceIp.Equal(ipPath.DestinationIp) {
			t.Fatalf("source = %v, want %v", restored.SourceIp, ipPath.DestinationIp)
		}
		assertDnsTcpChecksums(t, packet)

		// the other family's address is refused
		other := testDnsTcpLocalAddr(4)
		if ipVersion == 4 {
			other = testDnsTcpLocalAddr(6)
		}
		if rewriteDnsTcpAddress(packet, other, false) {
			t.Fatal("rewrite accepted an address of the other family")
		}
		// a v4-mapped address is not a v6 address
		if ipVersion == 6 && rewriteDnsTcpAddress(packet, netip.MustParseAddr("::ffff:169.254.0.53"), false) {
			t.Fatal("rewrite accepted a v4-mapped address for a v6 packet")
		}
		// udp is not rewritten
		udp := MessagePoolCopy(ipOosUdpPacket(udpTestPath(ipVersion), query))
		defer MessagePoolReturn(udp)
		if rewriteDnsTcpAddress(udp, localAddr, false) {
			t.Fatal("rewrite accepted a udp packet")
		}
	})

	// a v6 packet with an extension chain is rewritten with the transport
	// found through the walk; a fragment has no transport and is refused
	packet := testIpv6Tcp(testIpv6Src, testIpv6Dst, testIpv6ChainAll, 40000, 53, tcpFlagAck|tcpFlagPsh, 7, []byte("query"))
	localAddr := testDnsTcpLocalAddr(6)
	if !rewriteDnsTcpAddress(packet, localAddr, false) {
		t.Fatal("rewrite refused a v6 packet with extension headers")
	}
	if got := net.IP(packet[24:40]); !got.Equal(net.IP(localAddr.AsSlice())) {
		t.Fatalf("destination = %v, want %v", got, localAddr)
	}
	assertDnsTcpChecksums(t, packet)
	fragment := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberTcp), 8, true, 99, make([]byte, 16))
	if rewriteDnsTcpAddress(fragment, localAddr, false) {
		t.Fatal("rewrite accepted a v6 fragment")
	}
}

// the flow map remembers a client's chosen server per family and hands it
// back for the reply; a flow whose client and server are of different
// families is never remembered
func TestDnsTcpFlowBookkeepingDualstack(t *testing.T) {
	mux := &UpgradeMux{dnsTcpFlows: map[dnsTcpFlowKey]dnsTcpFlow{}}
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ipPath := dnsTcpTestPath(ipVersion)
		if !mux.rememberDnsTcpFlow(ipPath) {
			t.Fatal("flow not remembered")
		}
		reply := ipPath.Reverse()
		serverAddr, ok := mux.dnsTcpServerForClient(reply)
		want, _ := netIPAddr(ipPath.DestinationIp)
		if !ok || serverAddr != want {
			t.Fatalf("server for client = %v, %t; want %v", serverAddr, ok, want)
		}
		// an unknown client has no server
		unknown := ipPath.Reverse()
		unknown.DestinationPort += 1
		if _, ok := mux.dnsTcpServerForClient(unknown); ok {
			t.Fatal("unknown client found a server")
		}
	})
	// both families coexist in the map
	if len(mux.dnsTcpFlows) != 2 {
		t.Fatalf("flow map holds %d flows, want 2", len(mux.dnsTcpFlows))
	}
	mixed := dnsTcpTestPath(6)
	mixed.DestinationIp = net.ParseIP("10.0.0.53")
	if mux.rememberDnsTcpFlow(mixed) {
		t.Fatal("remembered a flow whose client and server families differ")
	}
}

// with no internal address for the client's family the packet is claimed
// and dropped -- never forwarded to the advertised identity -- and nothing
// is remembered about it
func TestHandleDnsTcpPacketFailsClosedWithoutFamilyAddress(t *testing.T) {
	mux := &UpgradeMux{
		dnsTcpFlows:     map[dnsTcpFlowKey]dnsTcpFlow{},
		dnsTcpLocalAddr: testDnsTcpLocalAddr(4),
	}
	ipPath := dnsTcpTestPath(6)
	packet := ipOosTcpPacketSequence(ipPath, tcpFlagSyn, 1, nil)
	if !mux.handleDnsTcpPacket(ipPath, packet) {
		t.Fatal("v6 dns-over-tcp with no v6 address was not claimed")
	}
	if len(mux.dnsTcpFlows) != 0 {
		t.Fatalf("flow map holds %d flows, want 0", len(mux.dnsTcpFlows))
	}
	if mux.dnsTcpLocalAddrFor(6).IsValid() || !mux.dnsTcpLocalAddrFor(4).IsValid() || mux.dnsTcpLocalAddrFor(5).IsValid() {
		t.Fatal("dnsTcpLocalAddrFor family mapping is wrong")
	}
}
