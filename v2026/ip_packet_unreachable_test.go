package connect

import (
	"encoding/binary"
	"net"
	"testing"
)

func udpTestPath(version int) *IpPath {
	if version == 4 {
		return &IpPath{
			Version:         4,
			Protocol:        IpProtocolUdp,
			SourceIp:        net.ParseIP("10.11.12.13"),
			SourcePort:      54321,
			DestinationIp:   net.ParseIP("93.184.216.34"),
			DestinationPort: 443,
		}
	}
	return &IpPath{
		Version:         6,
		Protocol:        IpProtocolUdp,
		SourceIp:        net.ParseIP("fd00::1"),
		SourcePort:      54321,
		DestinationIp:   net.ParseIP("2606:2800:220:1::1"),
		DestinationPort: 443,
	}
}

// a udp flow (dns, quic) must get an unreachable, since ipOosRst only builds
// resets for tcp and would otherwise leave the flow to stall silently
func TestIpOosUnreachableUdpV4(t *testing.T) {
	ipPath := udpTestPath(4)

	_, rstOk := ipOosRst(ipPath.Reverse())
	AssertEqual(t, rstOk, false)

	packet, ok := ipOosUnreachable(ipPath)
	AssertEqual(t, ok, true)

	// addressed back toward the source, as if from the destination
	AssertEqual(t, packet[9], byte(ipProtocolNumberIcmp4))
	AssertEqual(t, net.IP(packet[12:16]).String(), ipPath.DestinationIp.String())
	AssertEqual(t, net.IP(packet[16:20]).String(), ipPath.SourceIp.String())

	icmp := packet[Ipv4HeaderSizeWithoutExtensions:]
	AssertEqual(t, icmp[0], byte(icmpv4TypeDestinationUnreachable))
	// port unreachable, which linux marks fatal in icmp_err_convert and so
	// actually delivers to a socket without IP_RECVERR -- host unreachable is
	// non-fatal and is silently discarded. see ipOosUnreachable.
	AssertEqual(t, icmp[1], byte(icmpv4CodePortUnreachable))

	// the embedded datagram is the original flow direction, so the source can
	// match the error to its own socket
	embedded := icmp[icmpUnreachableHeaderSize:]
	AssertEqual(t, net.IP(embedded[12:16]).String(), ipPath.SourceIp.String())
	AssertEqual(t, net.IP(embedded[16:20]).String(), ipPath.DestinationIp.String())
	embeddedUdp := embedded[Ipv4HeaderSizeWithoutExtensions:]
	AssertEqual(t, int(binary.BigEndian.Uint16(embeddedUdp[0:2])), ipPath.SourcePort)
	AssertEqual(t, int(binary.BigEndian.Uint16(embeddedUdp[2:4])), ipPath.DestinationPort)

	// icmpv4 checksums the message alone; summing a correct message yields zero
	AssertEqual(t, checksumFinish(checksumAdd(0, icmp)), uint16(0))
}

func TestIpOosUnreachableUdpV6(t *testing.T) {
	ipPath := udpTestPath(6)

	packet, ok := ipOosUnreachable(ipPath)
	AssertEqual(t, ok, true)

	AssertEqual(t, packet[6], byte(ipProtocolNumberIcmp6))

	icmp := packet[Ipv6HeaderSize:]
	AssertEqual(t, icmp[0], byte(icmpv6TypeDestinationUnreachable))
	AssertEqual(t, icmp[1], byte(icmpv6CodePortUnreachable))

	// icmpv6 checksums with the pseudo header, unlike v4
	reverse := ipPath.Reverse()
	AssertEqual(t, transportChecksum(
		ipProtocolNumberIcmp6,
		reverse.SourceIp.To16(),
		reverse.DestinationIp.To16(),
		icmp,
	), uint16(0))
}

// tcp teardown keeps its rst. The unreachable builder now also serves the
// provider dial-failure signal for tcp flows, so this guard moved up a level:
// it no longer asserts the builder cannot do tcp, it asserts a tcp teardown
// still comes out as a tcp rst rather than an icmp -- which is what the old
// builder-level assertion actually protected.
func TestIpOosUnreachableTcpUnchanged(t *testing.T) {
	forEachIpVersion(t, testIpOosUnreachableTcpUnchanged)
}

func testIpOosUnreachableTcpUnchanged(t *testing.T, ipVersion int) {
	ipPath := udpTestPath(ipVersion)
	ipPath.Protocol = IpProtocolTcp

	// the builder now produces the dial-failure signal for tcp
	packet, ok := ipOosUnreachable(ipPath)
	AssertEqual(t, ok, true)
	_, icmpOk := ipParseIcmpUnreachable(packet)
	AssertEqual(t, icmpOk, true)

	// but teardown still prefers the rst for tcp flows
	c := &RemoteUserNatMultiClient{settings: &MultiClientSettings{UdpTeardownSignal: true}}
	teardown, ok := c.teardownSourcePacket(ipPath, 1234)
	AssertEqual(t, ok, true)
	teardownPath, err := ParseIpPath(teardown)
	AssertEqual(t, err == nil, true)
	AssertEqual(t, teardownPath.Protocol, IpProtocolTcp)
	AssertEqual(t, teardownPath.Rst, true)

	_, rstOk := ipOosRst(ipPath.Reverse())
	AssertEqual(t, rstOk, true)
}

// the setting must be an honest switch: off reproduces the previous behavior
// exactly, where udp had no teardown signal at all
func TestTeardownSourcePacketRespectsSetting(t *testing.T) {
	forEachIpVersion(t, testTeardownSourcePacketRespectsSetting)
}

func testTeardownSourcePacketRespectsSetting(t *testing.T, ipVersion int) {
	udpPath := udpTestPath(ipVersion)
	tcpPath := udpTestPath(ipVersion)
	tcpPath.Protocol = IpProtocolTcp

	on := &RemoteUserNatMultiClient{settings: &MultiClientSettings{UdpTeardownSignal: true}}
	off := &RemoteUserNatMultiClient{settings: &MultiClientSettings{UdpTeardownSignal: false}}

	_, ok := on.teardownSourcePacket(udpPath, 0)
	AssertEqual(t, ok, true)

	_, ok = off.teardownSourcePacket(udpPath, 0)
	AssertEqual(t, ok, false)

	// tcp is unaffected either way
	for _, c := range []*RemoteUserNatMultiClient{on, off} {
		_, ok = c.teardownSourcePacket(tcpPath, 0)
		AssertEqual(t, ok, true)
	}
}

func TestDefaultMultiClientSettingsEnablesUdpTeardownSignal(t *testing.T) {
	AssertEqual(t, DefaultMultiClientSettings().UdpTeardownSignal, true)
}
