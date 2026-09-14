package connect

import (
	"bytes"
	"encoding/binary"
	"net"
	"net/netip"
	"strings"
	"testing"
)

// ip_ipv6_ext_test.go — the extension-header walker and every parser and
// peek that shares it (IPV6.md C3): a flow whose packets carry hop-by-hop,
// routing, destination-options, authentication or atomic fragment headers must
// parse, classify, shard and sniff exactly like its plain packets, and a
// non-atomic fragment must be recognized as one everywhere.

// testIpv6Ext is one extension header for the test builder: `kind` is the
// next-header value that names it and `extra8` is how many 8-octet units it
// carries beyond the first 8.
type testIpv6Ext struct {
	kind   byte
	extra8 int
}

// testIpv6ExtChain encodes a chain of options-style extension headers so that
// the last one points at `transport`. The base header is not included.
func testIpv6ExtChain(exts []testIpv6Ext, transport byte) []byte {
	out := []byte{}
	for i, ext := range exts {
		next := transport
		if i+1 < len(exts) {
			next = exts[i+1].kind
		}
		var header []byte
		switch ext.kind {
		case ipv6NextHeaderAuthentication:
			// rfc 4302: length in 4-octet units minus 2; keep it at the
			// minimum 12 bytes plus extra8*8
			byteCount := 12 + ext.extra8*8
			header = make([]byte, byteCount)
			header[0] = next
			header[1] = byte(byteCount/4 - 2)
		default:
			byteCount := 8 * (1 + ext.extra8)
			header = make([]byte, byteCount)
			header[0] = next
			header[1] = byte(ext.extra8)
			// pad-n options fill the rest; a walker never reads them
		}
		out = append(out, header...)
	}
	return out
}

// testIpv6Packet builds a v6 packet: base header, the extension chain, then
// `transport` bytes for protocol `transportProtocol`.
func testIpv6Packet(src string, dst string, exts []testIpv6Ext, transportProtocol byte, transport []byte) []byte {
	chain := testIpv6ExtChain(exts, transportProtocol)
	packet := make([]byte, Ipv6HeaderSize+len(chain)+len(transport))
	packet[0] = 0x60
	binary.BigEndian.PutUint16(packet[4:6], uint16(len(chain)+len(transport)))
	if 0 < len(exts) {
		packet[6] = exts[0].kind
	} else {
		packet[6] = transportProtocol
	}
	packet[7] = 64
	copy(packet[8:24], net.ParseIP(src).To16())
	copy(packet[24:40], net.ParseIP(dst).To16())
	copy(packet[Ipv6HeaderSize:], chain)
	copy(packet[Ipv6HeaderSize+len(chain):], transport)
	return packet
}

// testIpv6Udp builds a checksummed udp packet with the given extension chain.
func testIpv6Udp(src string, dst string, exts []testIpv6Ext, srcPort int, dstPort int, payload []byte) []byte {
	udp := make([]byte, UdpHeaderSize+len(payload))
	binary.BigEndian.PutUint16(udp[0:2], uint16(srcPort))
	binary.BigEndian.PutUint16(udp[2:4], uint16(dstPort))
	binary.BigEndian.PutUint16(udp[4:6], uint16(len(udp)))
	copy(udp[UdpHeaderSize:], payload)
	checksum := transportChecksum(ipProtocolNumberUdp, net.ParseIP(src).To16(), net.ParseIP(dst).To16(), udp)
	if checksum == 0 {
		checksum = 0xffff
	}
	binary.BigEndian.PutUint16(udp[6:8], checksum)
	return testIpv6Packet(src, dst, exts, byte(ipProtocolNumberUdp), udp)
}

// testIpv6Tcp builds a checksummed tcp packet (no options) with the given
// extension chain.
func testIpv6Tcp(src string, dst string, exts []testIpv6Ext, srcPort int, dstPort int, flags byte, seq uint32, payload []byte) []byte {
	tcp := make([]byte, TcpHeaderSizeWithoutExtensions+len(payload))
	binary.BigEndian.PutUint16(tcp[0:2], uint16(srcPort))
	binary.BigEndian.PutUint16(tcp[2:4], uint16(dstPort))
	binary.BigEndian.PutUint32(tcp[4:8], seq)
	tcp[12] = byte(TcpHeaderSizeWithoutExtensions/4) << 4
	tcp[13] = flags
	binary.BigEndian.PutUint16(tcp[14:16], 4096)
	copy(tcp[TcpHeaderSizeWithoutExtensions:], payload)
	binary.BigEndian.PutUint16(tcp[16:18], transportChecksum(ipProtocolNumberTcp, net.ParseIP(src).To16(), net.ParseIP(dst).To16(), tcp))
	return testIpv6Packet(src, dst, exts, byte(ipProtocolNumberTcp), tcp)
}

// testIpv6FragmentHeader encodes a fragment header pointing at `next`.
func testIpv6FragmentHeader(next byte, offset int, more bool, identification uint32) []byte {
	header := make([]byte, ipv6FragmentHeaderSize)
	header[0] = next
	offsetAndFlags := uint16(offset)
	if more {
		offsetAndFlags |= 1
	}
	binary.BigEndian.PutUint16(header[2:4], offsetAndFlags)
	binary.BigEndian.PutUint32(header[4:8], identification)
	return header
}

// testIpv6FragmentPacket builds a raw fragment: base header, optional
// hop-by-hop header, then the fragment header, then `data`.
func testIpv6FragmentPacket(src string, dst string, hopByHop bool, next byte, offset int, more bool, identification uint32, data []byte) []byte {
	body := []byte{}
	if hopByHop {
		body = append(body, testIpv6ExtChain([]testIpv6Ext{{kind: ipv6NextHeaderHopByHop}}, ipv6NextHeaderFragment)...)
	}
	body = append(body, testIpv6FragmentHeader(next, offset, more, identification)...)
	body = append(body, data...)
	packet := make([]byte, Ipv6HeaderSize+len(body))
	packet[0] = 0x60
	binary.BigEndian.PutUint16(packet[4:6], uint16(len(body)))
	if hopByHop {
		packet[6] = ipv6NextHeaderHopByHop
	} else {
		packet[6] = ipv6NextHeaderFragment
	}
	packet[7] = 64
	copy(packet[8:24], net.ParseIP(src).To16())
	copy(packet[24:40], net.ParseIP(dst).To16())
	copy(packet[Ipv6HeaderSize:], body)
	return packet
}

const (
	testIpv6Src = "fd00::1"
	testIpv6Dst = "2606:2800:220:1::1"
)

var testIpv6ChainAll = []testIpv6Ext{
	{kind: ipv6NextHeaderHopByHop},
	{kind: ipv6NextHeaderDestinationOptions, extra8: 1},
	{kind: ipv6NextHeaderRouting, extra8: 2},
	{kind: ipv6NextHeaderAuthentication, extra8: 1},
	{kind: ipv6NextHeaderDestinationOptions},
}

func testIpv6ChainByteCount(exts []testIpv6Ext) int {
	return len(testIpv6ExtChain(exts, byte(ipProtocolNumberUdp)))
}

func TestWalkIpv6ExtensionHeaders(t *testing.T) {
	udp := make([]byte, UdpHeaderSize+4)
	binary.BigEndian.PutUint16(udp[4:6], uint16(len(udp)))

	// no extension chain
	plain := testIpv6Packet(testIpv6Src, testIpv6Dst, nil, byte(ipProtocolNumberUdp), udp)
	walk, ok := walkIpv6ExtensionHeaders(plain)
	if !ok || walk.nextHeader != ipProtocolNumberUdp || walk.transportOffset != Ipv6HeaderSize ||
		walk.payloadEnd != len(plain) || walk.fragmentPresent || walk.fragmented {
		t.Fatalf("plain walk = %+v, %t", walk, ok)
	}

	// every options-style header plus authentication, chained
	chained := testIpv6Packet(testIpv6Src, testIpv6Dst, testIpv6ChainAll, byte(ipProtocolNumberUdp), udp)
	walk, ok = walkIpv6ExtensionHeaders(chained)
	wantOffset := Ipv6HeaderSize + testIpv6ChainByteCount(testIpv6ChainAll)
	if !ok || walk.nextHeader != ipProtocolNumberUdp || walk.transportOffset != wantOffset ||
		walk.payloadEnd != len(chained) || walk.fragmentPresent {
		t.Fatalf("chained walk = %+v, %t, want transport at %d", walk, ok, wantOffset)
	}

	// an atomic fragment (offset 0, m clear) is walked through
	atomic := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, true, byte(ipProtocolNumberUdp), 0, false, 0x11223344, udp)
	walk, ok = walkIpv6ExtensionHeaders(atomic)
	if !ok || walk.fragmented || !walk.fragmentPresent || walk.nextHeader != ipProtocolNumberUdp ||
		walk.transportOffset != Ipv6HeaderSize+8+ipv6FragmentHeaderSize ||
		walk.fragment.identification != 0x11223344 ||
		walk.fragment.headerOffset != Ipv6HeaderSize+8 ||
		walk.fragment.precedingNextHeaderFieldOffset != Ipv6HeaderSize {
		t.Fatalf("atomic fragment walk = %+v, %t", walk, ok)
	}

	// a non-atomic fragment ends the walk at the fragment header
	first := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberUdp), 0, true, 7, make([]byte, 16))
	walk, ok = walkIpv6ExtensionHeaders(first)
	if !ok || !walk.fragmented || walk.fragment.offset != 0 || !walk.fragment.moreFragments ||
		walk.fragment.identification != 7 || walk.transportOffset != Ipv6HeaderSize+ipv6FragmentHeaderSize ||
		walk.fragment.precedingNextHeaderFieldOffset != 6 {
		t.Fatalf("first fragment walk = %+v, %t", walk, ok)
	}
	last := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberUdp), 16, false, 7, make([]byte, 5))
	walk, ok = walkIpv6ExtensionHeaders(last)
	if !ok || !walk.fragmented || walk.fragment.offset != 16 || walk.fragment.moreFragments {
		t.Fatalf("last fragment walk = %+v, %t", walk, ok)
	}

	// esp and no-next-header stop the walk with their own value
	for _, terminal := range []byte{ipv6NextHeaderEsp, ipv6NextHeaderNoNextHeader} {
		packet := testIpv6Packet(testIpv6Src, testIpv6Dst, []testIpv6Ext{{kind: ipv6NextHeaderHopByHop}}, terminal, make([]byte, 8))
		walk, ok = walkIpv6ExtensionHeaders(packet)
		if !ok || walk.nextHeader != ipProtocolNumber(terminal) || walk.transportOffset != Ipv6HeaderSize+8 {
			t.Fatalf("terminal %d walk = %+v, %t", terminal, walk, ok)
		}
	}

	// malformed: a truncated extension header
	truncated := append([]byte(nil), chained...)
	truncated = truncated[:Ipv6HeaderSize+12]
	binary.BigEndian.PutUint16(truncated[4:6], 12)
	if _, ok := walkIpv6ExtensionHeaders(truncated); ok {
		t.Fatal("truncated extension header walked")
	}
	// malformed: a declared payload longer than the packet
	short := append([]byte(nil), plain...)
	binary.BigEndian.PutUint16(short[4:6], uint16(len(short)))
	if _, ok := walkIpv6ExtensionHeaders(short); ok {
		t.Fatal("over-declared payload walked")
	}
	// malformed: two fragment headers
	double := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, ipv6NextHeaderFragment, 0, false, 1,
		append(testIpv6FragmentHeader(byte(ipProtocolNumberUdp), 0, false, 2), udp...))
	if _, ok := walkIpv6ExtensionHeaders(double); ok {
		t.Fatal("double fragment header walked")
	}
	// malformed: a chain longer than the bound
	long := []testIpv6Ext{}
	for i := 0; i <= ipv6MaxExtensionHeaderCount; i += 1 {
		long = append(long, testIpv6Ext{kind: ipv6NextHeaderDestinationOptions})
	}
	if _, ok := walkIpv6ExtensionHeaders(testIpv6Packet(testIpv6Src, testIpv6Dst, long, byte(ipProtocolNumberUdp), udp)); ok {
		t.Fatal("over-long chain walked")
	}
	// not ipv6
	if _, ok := walkIpv6ExtensionHeaders(testingUdp4Packet("10.0.0.1", "203.0.113.7", 53, nil)); ok {
		t.Fatal("ipv4 walked as ipv6")
	}
	if _, ok := walkIpv6ExtensionHeaders(nil); ok {
		t.Fatal("empty walked")
	}
}

func TestParseIpPathIpv6ExtensionHeaders(t *testing.T) {
	payload := []byte("hello")
	udp := testIpv6Udp(testIpv6Src, testIpv6Dst, testIpv6ChainAll, 40001, 53, payload)
	ipPath, got, err := ParseIpPathWithPayload(udp)
	if err != nil {
		t.Fatalf("udp with extension chain: %v", err)
	}
	if ipPath.Version != 6 || ipPath.Protocol != IpProtocolUdp || ipPath.SourcePort != 40001 ||
		ipPath.DestinationPort != 53 || !bytes.Equal(got, payload) ||
		ipPath.SourceIp.String() != testIpv6Src || ipPath.DestinationIp.String() != testIpv6Dst {
		t.Fatalf("udp path = %+v payload=%q", ipPath, got)
	}

	tcp := testIpv6Tcp(testIpv6Src, testIpv6Dst, []testIpv6Ext{{kind: ipv6NextHeaderDestinationOptions, extra8: 3}}, 40002, 443, tcpFlagSyn, 0x1234, nil)
	ipPath, err = ParseIpPath(tcp)
	if err != nil {
		t.Fatalf("tcp with destination options: %v", err)
	}
	if ipPath.Protocol != IpProtocolTcp || !ipPath.Syn || ipPath.SequenceNumber != 0x1234 || ipPath.DestinationPort != 443 {
		t.Fatalf("tcp path = %+v", ipPath)
	}

	// an atomic fragment carries a whole packet
	udpTransport := udp[Ipv6HeaderSize+testIpv6ChainByteCount(testIpv6ChainAll):]
	atomic := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberUdp), 0, false, 9, udpTransport)
	if ipPath, err = ParseIpPath(atomic); err != nil || ipPath.DestinationPort != 53 {
		t.Fatalf("atomic fragment = %+v, %v", ipPath, err)
	}

	// the icmp path also walks the chain
	echo := testingIcmp6EchoPacket(testIpv6Src, testIpv6Dst, 0x77, 1, []byte("ping"))
	icmp := echo[Ipv6HeaderSize:]
	wrapped := testIpv6Packet(testIpv6Src, testIpv6Dst, []testIpv6Ext{{kind: ipv6NextHeaderHopByHop}}, byte(ipProtocolNumberIcmp6), icmp)
	if ipPath, err = ParseIpPath(wrapped); err != nil || ipPath.Protocol != IpProtocolIcmp || ipPath.SourcePort != 0x77 {
		t.Fatalf("icmp with hop-by-hop = %+v, %v", ipPath, err)
	}
}

// the v6 analog of TestParseIpPathIpv4FragmentDrop: only a non-atomic
// fragment is rejected, since it has no transport header to read
func TestParseIpPathIpv6FragmentDrop(t *testing.T) {
	udp := testIpv6Udp(testIpv6Src, testIpv6Dst, nil, 40001, 53, []byte("payload"))
	transport := udp[Ipv6HeaderSize:]
	cases := []struct {
		name       string
		offset     int
		more       bool
		expectDrop bool
	}{
		{name: "atomic", offset: 0, more: false, expectDrop: false},
		{name: "first", offset: 0, more: true, expectDrop: true},
		{name: "middle", offset: 8, more: true, expectDrop: true},
		{name: "last", offset: 8, more: false, expectDrop: true},
	}
	for _, c := range cases {
		packet := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberUdp), c.offset, c.more, 5, transport)
		_, err := ParseIpPath(packet)
		if c.expectDrop != (err != nil) {
			t.Errorf("%s: expectDrop=%v err=%v", c.name, c.expectDrop, err)
		}
	}
}

func TestSendShardIpv6ExtensionHeaders(t *testing.T) {
	shardCount := 257
	payload := []byte("abc")
	plain := testIpv6Udp(testIpv6Src, testIpv6Dst, nil, 40001, 53, payload)
	shard := sendShard(plain, shardCount)
	for _, exts := range [][]testIpv6Ext{
		{{kind: ipv6NextHeaderHopByHop}},
		{{kind: ipv6NextHeaderDestinationOptions, extra8: 2}},
		testIpv6ChainAll,
	} {
		packet := testIpv6Udp(testIpv6Src, testIpv6Dst, exts, 40001, 53, payload)
		if got := sendShard(packet, shardCount); got != shard {
			t.Fatalf("extension chain %v moved shard %d -> %d", exts, shard, got)
		}
	}
	// a different flow moves
	if got := sendShard(testIpv6Udp(testIpv6Src, testIpv6Dst, nil, 40002, 53, payload), shardCount); got == shard {
		// possible by collision but vanishingly unlikely with 257 shards
		// across the port change below; try one more port
		if got2 := sendShard(testIpv6Udp(testIpv6Src, testIpv6Dst, nil, 40003, 53, payload), shardCount); got2 == shard {
			t.Fatal("distinct flows pinned to one shard")
		}
	}

	// every fragment of one datagram shares a shard, including the first
	// fragment which does carry the ports
	transport := plain[Ipv6HeaderSize:]
	first := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberUdp), 0, true, 0xabcd, transport[:8])
	last := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberUdp), 8, false, 0xabcd, transport[8:])
	if sendShard(first, shardCount) != sendShard(last, shardCount) {
		t.Fatal("fragments of one IPv6 datagram map to different NAT send shards")
	}
	other := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberUdp), 8, false, 0xabce, transport[8:])
	if sendShard(other, shardCount) == sendShard(last, shardCount) {
		// a collision is possible; check a third identification
		other2 := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberUdp), 8, false, 0xabcf, transport[8:])
		if sendShard(other2, shardCount) == sendShard(last, shardCount) {
			t.Fatal("fragments of distinct datagrams always pinned together")
		}
	}
}

func TestPeekClaimIpv6ExtensionHeaders(t *testing.T) {
	hop := []testIpv6Ext{{kind: ipv6NextHeaderHopByHop}}
	dest := []testIpv6Ext{{kind: ipv6NextHeaderDestinationOptions, extra8: 1}}
	tcpTo := func(exts []testIpv6Ext, port int) []byte {
		return testIpv6Tcp(testIpv6Src, testIpv6Dst, exts, 40001, port, tcpFlagSyn, 1, nil)
	}
	cases := []struct {
		name   string
		packet []byte
		want   peekResult
	}{
		{"hop tcp 80", tcpTo(hop, 80), peekHttp},
		{"hop tcp 53", tcpTo(hop, 53), peekDns},
		{"chain tcp 443", tcpTo(testIpv6ChainAll, 443), peekTls},
		{"dest udp 53", testIpv6Udp(testIpv6Src, testIpv6Dst, dest, 40001, 53, nil), peekDns},
		{"dest udp 4500", testIpv6Udp(testIpv6Src, testIpv6Dst, dest, 40001, 4500, nil), peekOther},
		{"hop icmpv6", testIpv6Packet(testIpv6Src, testIpv6Dst, hop, byte(ipProtocolNumberIcmp6), make([]byte, 8)), peekOther},
		{"hop esp", testIpv6Packet(testIpv6Src, testIpv6Dst, hop, ipv6NextHeaderEsp, make([]byte, 8)), peekOther},
		{"first fragment", testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberTcp), 0, true, 1, make([]byte, 24)), peekUndecided},
		{"truncated chain", testIpv6Packet(testIpv6Src, testIpv6Dst, hop, byte(ipProtocolNumberTcp), nil)[:Ipv6HeaderSize+4], peekUndecided},
	}
	for _, c := range cases {
		var seg tlsSegment
		got := peekClaim(c.packet, &seg)
		if got != c.want {
			t.Errorf("%s: peekClaim = %d, want %d", c.name, got, c.want)
		}
		if c.want == peekTls && (seg.flow.dstPort != 443 || seg.flow.srcAddr != netip.MustParseAddr(testIpv6Src)) {
			t.Errorf("%s: peekTls seg = %+v", c.name, seg.flow)
		}
	}
}

func TestPeekTlsSegmentIpv6ExtensionHeaders(t *testing.T) {
	hello := buildClientHello("example.test")
	for _, exts := range [][]testIpv6Ext{nil, {{kind: ipv6NextHeaderHopByHop}}, testIpv6ChainAll} {
		packet := testIpv6Tcp(testIpv6Src, testIpv6Dst, exts, 40001, 443, tcpFlagAck, 1, hello)
		seg, ok := peekTlsSegment(packet)
		if !ok {
			t.Fatalf("chain %v: no segment", exts)
		}
		if seg.flow.srcPort != 40001 || seg.flow.dstPort != 443 || !bytes.Equal(seg.payload, hello) {
			t.Fatalf("chain %v: seg = %+v", exts, seg.flow)
		}
		name, _, ok := sniFromClientHello(seg.payload)
		if !ok || name != "example.test" {
			t.Fatalf("chain %v: sni = %q, %t", exts, name, ok)
		}
	}
	// a fragment has no segment
	fragment := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberTcp), 0, true, 3, make([]byte, 40))
	if _, ok := peekTlsSegment(fragment); ok {
		t.Fatal("fragment produced a tls segment")
	}
}

func TestFirstLoadTcpPeekIpv6(t *testing.T) {
	local := netip.MustParseAddr(testIpv6Src)
	server := netip.MustParseAddr(testIpv6Dst)
	for _, exts := range [][]testIpv6Ext{nil, {{kind: ipv6NextHeaderHopByHop}}, testIpv6ChainAll} {
		syn := testIpv6Tcp(testIpv6Src, testIpv6Dst, exts, 40001, 443, 0x02, 1, nil)
		remoteAddr, remotePort, localPort, flags, payloadLen, ok := firstLoadTcpPeek(syn, false)
		if !ok || remoteAddr != server || remotePort != 443 || localPort != 40001 || flags != 0x02 || payloadLen != 0 {
			t.Fatalf("chain %v egress syn peek: %v %v %v %v %v %v", exts, remoteAddr, remotePort, localPort, flags, payloadLen, ok)
		}
		data := testIpv6Tcp(testIpv6Dst, testIpv6Src, exts, 443, 40001, 0x18, 1, make([]byte, 512))
		remoteAddr, _, _, _, payloadLen, ok = firstLoadTcpPeek(data, true)
		if !ok || remoteAddr != server || payloadLen != 512 {
			t.Fatalf("chain %v ingress payload peek: %v %v %v", exts, remoteAddr, payloadLen, ok)
		}
	}
	_ = local
	// udp and fragments are skipped
	if _, _, _, _, _, ok := firstLoadTcpPeek(testIpv6Udp(testIpv6Src, testIpv6Dst, nil, 40001, 443, nil), false); ok {
		t.Fatal("udp must not peek")
	}
	fragment := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberTcp), 0, true, 3, make([]byte, 40))
	if _, _, _, _, _, ok := firstLoadTcpPeek(fragment, false); ok {
		t.Fatal("fragment must not peek")
	}
}

func TestIcmpUnreachableIpv6ExtensionHeaders(t *testing.T) {
	egress := icmpTcpTestPath(6)
	packet, ok := ipOosUnreachable(egress)
	if !ok {
		t.Fatal("build failed")
	}
	// re-wrap the outer packet with a hop-by-hop header before the icmp
	icmp := packet[Ipv6HeaderSize:]
	wrapped := testIpv6Packet(egress.DestinationIp.String(), egress.SourceIp.String(), []testIpv6Ext{{kind: ipv6NextHeaderHopByHop}}, byte(ipProtocolNumberIcmp6), icmp)
	parsed, ok := ipParseIcmpUnreachable(wrapped)
	if !ok {
		t.Fatal("outer extension header defeated the parse")
	}
	if parsed.ToIp6Path() != egress.ToIp6Path() || parsed.SequenceNumber != egress.SequenceNumber {
		t.Fatalf("flow map key mismatch: %+v", parsed)
	}

	// an embed whose original packet carried destination options, truncated
	// to the 8 transport bytes rfc 792 guarantees
	original := testIpv6Tcp(egress.SourceIp.String(), egress.DestinationIp.String(), []testIpv6Ext{{kind: ipv6NextHeaderDestinationOptions}}, egress.SourcePort, egress.DestinationPort, tcpFlagSyn, egress.SequenceNumber, nil)
	embed := original[:Ipv6HeaderSize+8+8]
	body := make([]byte, icmpUnreachableHeaderSize+len(embed))
	body[0] = icmpv6TypeDestinationUnreachable
	body[1] = icmpv6CodeNoRoute
	copy(body[icmpUnreachableHeaderSize:], embed)
	outer := testIpv6Packet(egress.DestinationIp.String(), egress.SourceIp.String(), nil, byte(ipProtocolNumberIcmp6), body)
	parsed, ok = ipParseIcmpUnreachable(outer)
	if !ok {
		t.Fatal("embedded extension header defeated the parse")
	}
	if parsed.ToIp6Path() != egress.ToIp6Path() || parsed.SequenceNumber != egress.SequenceNumber {
		t.Fatalf("embedded flow map key mismatch: %+v", parsed)
	}

	// an embedded fragment has no transport header to recover
	fragmentEmbed := testIpv6FragmentPacket(egress.SourceIp.String(), egress.DestinationIp.String(), false, byte(ipProtocolNumberTcp), 8, true, 1, make([]byte, 8))
	body = make([]byte, icmpUnreachableHeaderSize+len(fragmentEmbed))
	body[0] = icmpv6TypeDestinationUnreachable
	copy(body[icmpUnreachableHeaderSize:], fragmentEmbed)
	outer = testIpv6Packet(egress.DestinationIp.String(), egress.SourceIp.String(), nil, byte(ipProtocolNumberIcmp6), body)
	if _, ok := ipParseIcmpUnreachable(outer); ok {
		t.Fatal("embedded fragment parsed")
	}
}

// link-local control chatter fails the parse with its own error, so a
// caller can drop it without logging a malformed-packet line
func TestParseIpPathIcmpv6LinkControlError(t *testing.T) {
	for _, icmpType := range []byte{130, 131, 132, 133, 134, 135, 136, 137} {
		packet := testingIcmp6EchoPacket("fe80::1", "ff02::1", 1, 1, nil)
		packet[Ipv6HeaderSize] = icmpType
		_, err := ParseIpPath(packet)
		if err != errIcmpv6LinkControl {
			t.Fatalf("type %d: err = %v, want errIcmpv6LinkControl", icmpType, err)
		}
	}
	// a genuinely unsupported type keeps the generic error
	packet := testingIcmp6EchoPacket("fe80::1", "ff02::1", 1, 1, nil)
	packet[Ipv6HeaderSize] = 100
	if _, err := ParseIpPath(packet); err == nil || !strings.Contains(err.Error(), "Unsupported") {
		t.Fatalf("type 100: err = %v", err)
	}
}
