package connect

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// ip_ipv6_fragment_test.go — IPv6 fragment emission and reassembly (IPV6.md
// C3), the dual-stack fragment gate, path-mtu clamping, and the local NAT
// carrying fragmented v6 datagrams in both directions.

func fragmentTestPacketsForVersion(t *testing.T, ipVersion int, payload []byte) [][]byte {
	t.Helper()
	packets, err := newFragmentTestStream(ipVersion).DataPackets(payload, len(payload), DefaultMtu)
	if err != nil {
		t.Fatalf("fragment v%d UDP payload: %v", ipVersion, err)
	}
	t.Cleanup(func() {
		for _, packet := range packets {
			MessagePoolReturn(packet)
		}
	})
	return packets
}

func ipv6FragmentFields(t *testing.T, packet []byte) ipv6FragmentInfo {
	t.Helper()
	walk, ok := walkIpv6ExtensionHeaders(packet)
	if !ok || !walk.fragmented {
		t.Fatalf("not a v6 fragment: %+v %t", walk, ok)
	}
	return walk.fragment
}

func TestIpv6UdpFragmentationReassemblyAtProductMtu(t *testing.T) {
	payload := fragmentTestPayload(1400)
	fragments := fragmentTestPacketsForVersion(t, 6, payload)
	if len(fragments) != 2 {
		t.Fatalf("fragment count = %d, want 2", len(fragments))
	}
	first := ipv6FragmentFields(t, fragments[0])
	last := ipv6FragmentFields(t, fragments[1])
	if first.identification == 0 || first.identification != last.identification {
		t.Fatalf("identification = %d / %d", first.identification, last.identification)
	}
	for i, fragment := range fragments {
		if DefaultMtu < len(fragment) {
			t.Fatalf("fragment %d length = %d, MTU = %d", i, len(fragment), DefaultMtu)
		}
		if fragment[6] != ipv6NextHeaderFragment {
			t.Fatalf("fragment %d next header = %d, want fragment", i, fragment[6])
		}
		if declared := Ipv6HeaderSize + int(binary.BigEndian.Uint16(fragment[4:6])); declared != len(fragment) {
			t.Fatalf("fragment %d payload length %d != %d", i, declared, len(fragment))
		}
	}
	if first.offset != 0 || !first.moreFragments || first.nextHeader != byte(ipProtocolNumberUdp) {
		t.Fatalf("first fragment = %+v", first)
	}
	firstPayloadByteCount := len(fragments[0]) - Ipv6HeaderSize - ipv6FragmentHeaderSize
	if firstPayloadByteCount%8 != 0 {
		t.Fatalf("first fragment payload %d is not a multiple of 8", firstPayloadByteCount)
	}
	if last.offset != firstPayloadByteCount || last.moreFragments {
		t.Fatalf("last fragment = %+v, want offset %d", last, firstPayloadByteCount)
	}
	if sendShard(fragments[0], 257) != sendShard(fragments[1], 257) {
		t.Fatal("fragments of one IPv6 datagram map to different NAT send shards")
	}

	reassembler := newIpv6FragmentReassembler()
	defer reassembler.close()
	source := SourceId(NewId())
	transferKey := TransferKey{ForceStream: true}
	// reassembly tolerates reordering
	if packet := reassembler.process(source, transferKey, protocol.ProvideMode_Public, MessagePoolCopy(fragments[1])); packet != nil {
		MessagePoolReturn(packet)
		t.Fatal("final fragment completed the datagram without offset zero")
	}
	reassembled := reassembler.process(source, transferKey, protocol.ProvideMode_Public, MessagePoolCopy(fragments[0]))
	if reassembled == nil {
		t.Fatal("complete fragment set did not produce a packet")
	}
	defer MessagePoolReturn(reassembled)

	ipProtocol, sourceIp, destinationIp, transport, ok := parseIpv6(reassembled)
	if !ok || ipProtocol != ipProtocolNumberUdp {
		t.Fatalf("reassembled IPv6 parse = (%d, %t), want UDP", ipProtocol, ok)
	}
	if reassembled[6] != byte(ipProtocolNumberUdp) {
		t.Fatalf("reassembled next header = %d, want udp (fragment header removed)", reassembled[6])
	}
	if declared := Ipv6HeaderSize + int(binary.BigEndian.Uint16(reassembled[4:6])); declared != len(reassembled) {
		t.Fatalf("reassembled payload length %d != %d", declared, len(reassembled))
	}
	if got := transportChecksum(ipProtocolNumberUdp, sourceIp, destinationIp, transport); got != 0 {
		t.Fatalf("reassembled UDP checksum residual = %#x", got)
	}
	var udp parsedUdp
	if !parseUdpPacket(sourceIp, destinationIp, transport, &udp) {
		t.Fatal("reassembled UDP packet did not parse")
	}
	if udp.sourcePort != 443 || udp.destinationPort != 42000 {
		t.Fatalf("reassembled UDP ports = %d -> %d", udp.sourcePort, udp.destinationPort)
	}
	if !bytes.Equal(udp.payload, payload) {
		t.Fatalf("reassembled UDP payload differs: got %d bytes, want %d", len(udp.payload), len(payload))
	}
	if len(reassembler.datagrams) != 0 || reassembler.retainedByteCount != 0 {
		t.Fatalf("completed reassembly retained %d datagrams / %d bytes", len(reassembler.datagrams), reassembler.retainedByteCount)
	}
}

// the unfragmentable part is carried through: a datagram whose fragments
// carry a hop-by-hop header before the fragment header reassembles with the
// hop-by-hop header intact and pointing at the transport
func TestIpv6FragmentReassemblyKeepsUnfragmentablePart(t *testing.T) {
	udp := testIpv6Udp(testIpv6Src, testIpv6Dst, nil, 40001, 53, fragmentTestPayload(100))
	transport := udp[Ipv6HeaderSize:]
	first := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, true, byte(ipProtocolNumberUdp), 0, true, 42, transport[:64])
	last := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, true, byte(ipProtocolNumberUdp), 64, false, 42, transport[64:])

	reassembler := newIpv6FragmentReassembler()
	defer reassembler.close()
	source := SourceId(NewId())
	if packet := reassembler.process(source, TransferKey{}, protocol.ProvideMode_Public, MessagePoolCopy(first)); packet != nil {
		t.Fatal("first fragment completed alone")
	}
	reassembled := reassembler.process(source, TransferKey{}, protocol.ProvideMode_Public, MessagePoolCopy(last))
	if reassembled == nil {
		t.Fatal("no reassembly")
	}
	defer MessagePoolReturn(reassembled)
	if reassembled[6] != ipv6NextHeaderHopByHop || reassembled[Ipv6HeaderSize] != byte(ipProtocolNumberUdp) {
		t.Fatalf("headers = %d / %d, want hop-by-hop -> udp", reassembled[6], reassembled[Ipv6HeaderSize])
	}
	ipPath, payload, err := ParseIpPathWithPayload(reassembled)
	if err != nil || ipPath.DestinationPort != 53 || !bytes.Equal(payload, fragmentTestPayload(100)) {
		t.Fatalf("reassembled parse = %+v, %v", ipPath, err)
	}
}

func TestIpv6FragmentReassemblyRejectsOverlapAndExpiresIncompleteState(t *testing.T) {
	fragments := fragmentTestPacketsForVersion(t, 6, fragmentTestPayload(1400))
	reassembler := newIpv6FragmentReassembler()
	defer reassembler.close()
	source := SourceId(NewId())
	now := time.Now()

	if packet := reassembler.processAt(source, TransferKey{}, protocol.ProvideMode_Public, MessagePoolCopy(fragments[0]), now); packet != nil {
		t.Fatal("first fragment completed alone")
	}
	// an overlapping fragment: the final fragment shifted to offset 8 with
	// different bytes poisons the datagram
	overlap := MessagePoolCopy(fragments[1])
	offsetAndFlags := binary.BigEndian.Uint16(overlap[Ipv6HeaderSize+2 : Ipv6HeaderSize+4])
	binary.BigEndian.PutUint16(overlap[Ipv6HeaderSize+2:Ipv6HeaderSize+4], (offsetAndFlags&1)|8)
	if packet := reassembler.processAt(source, TransferKey{}, protocol.ProvideMode_Public, overlap, now); packet != nil {
		t.Fatal("overlapping fragment completed a datagram")
	}
	if len(reassembler.datagrams) != 0 || reassembler.retainedByteCount != 0 {
		t.Fatalf("overlap left %d datagrams / %d bytes", len(reassembler.datagrams), reassembler.retainedByteCount)
	}

	// an incomplete datagram expires from its first fragment
	if packet := reassembler.processAt(source, TransferKey{}, protocol.ProvideMode_Public, MessagePoolCopy(fragments[0]), now); packet != nil {
		t.Fatal("first fragment completed alone")
	}
	if len(reassembler.datagrams) != 1 {
		t.Fatalf("retained datagrams = %d, want 1", len(reassembler.datagrams))
	}
	reassembler.expire(now.Add(ipv6FragmentReassemblyTimeout))
	if len(reassembler.datagrams) != 0 || reassembler.retainedByteCount != 0 {
		t.Fatalf("expiry left %d datagrams / %d bytes", len(reassembler.datagrams), reassembler.retainedByteCount)
	}
	// and a late final fragment after expiry cannot complete anything
	if packet := reassembler.processAt(source, TransferKey{}, protocol.ProvideMode_Public, MessagePoolCopy(fragments[1]), now.Add(ipv6FragmentReassemblyTimeout)); packet != nil {
		t.Fatal("late final fragment completed a datagram")
	}
	reassembler.close()
}

func TestIpv6FragmentReassemblyRejectsMalformed(t *testing.T) {
	udp := testIpv6Udp(testIpv6Src, testIpv6Dst, nil, 40001, 53, fragmentTestPayload(100))
	transport := udp[Ipv6HeaderSize:]
	reassembler := newIpv6FragmentReassembler()
	defer reassembler.close()
	source := SourceId(NewId())
	process := func(packet []byte) []byte {
		return reassembler.process(source, TransferKey{}, protocol.ProvideMode_Public, MessagePoolCopy(packet))
	}

	// a non-final fragment must carry a multiple of 8 octets
	if packet := process(testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberUdp), 0, true, 1, transport[:60])); packet != nil {
		t.Fatal("unaligned fragment accepted")
	}
	// an empty fragment carries nothing
	if packet := process(testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberUdp), 8, false, 2, nil)); packet != nil {
		t.Fatal("empty fragment accepted")
	}
	// a final fragment that ends before a retained fragment is rejected
	if packet := process(testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberUdp), 64, true, 3, transport[64:72])); packet != nil {
		t.Fatal("middle fragment completed")
	}
	if packet := process(testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberUdp), 8, false, 3, transport[8:40])); packet != nil {
		t.Fatal("short final fragment completed")
	}
	// an atomic fragment (offset 0, m clear) is a whole packet, not a
	// fragment: it passes through untouched even while a datagram with the
	// same identification is pending
	atomic := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberUdp), 0, false, 3, transport)
	if packet := process(atomic); packet == nil {
		t.Fatal("atomic fragment consumed")
	} else {
		MessagePoolReturn(packet)
	}
	reassembler.close()
	if len(reassembler.datagrams) != 0 || reassembler.retainedByteCount != 0 {
		t.Fatalf("rejections left %d datagrams / %d bytes", len(reassembler.datagrams), reassembler.retainedByteCount)
	}
}

func TestIpv6FragmentReassemblyIsByteAndDatagramBounded(t *testing.T) {
	fragments := fragmentTestPacketsForVersion(t, 6, fragmentTestPayload(1400))
	reassembler := newIpv6FragmentReassembler()
	defer reassembler.close()
	source := SourceId(NewId())
	now := time.Now()
	for i := 0; i < ipv6FragmentReassemblyMaxDatagrams*2; i += 1 {
		first := MessagePoolCopy(fragments[0])
		binary.BigEndian.PutUint32(first[Ipv6HeaderSize+4:Ipv6HeaderSize+8], uint32(1000+i))
		if packet := reassembler.processAt(source, TransferKey{}, protocol.ProvideMode_Public, first, now); packet != nil {
			t.Fatal("first fragment completed alone")
		}
		if ipv6FragmentReassemblyMaxDatagrams < len(reassembler.datagrams) {
			t.Fatalf("retained datagrams = %d, want at most %d", len(reassembler.datagrams), ipv6FragmentReassemblyMaxDatagrams)
		}
		if ipv6FragmentReassemblyMaxRetainedBytes < reassembler.retainedByteCount {
			t.Fatalf("retained bytes = %d, want at most %d", reassembler.retainedByteCount, ipv6FragmentReassemblyMaxRetainedBytes)
		}
	}
}

func TestIsIpFragmentPacket(t *testing.T) {
	v4 := fragmentTestPacketsForVersion(t, 4, fragmentTestPayload(1400))
	v6 := fragmentTestPacketsForVersion(t, 6, fragmentTestPayload(1400))
	for _, packet := range append(append([][]byte{}, v4...), v6...) {
		if !isIpFragmentPacket(packet) {
			t.Fatalf("v%d fragment not recognized", packet[0]>>4)
		}
	}
	if isIpv6FragmentPacket(v4[0]) || isIpv4FragmentPacket(v6[0]) {
		t.Fatal("family mixup")
	}
	plain6 := testIpv6Udp(testIpv6Src, testIpv6Dst, nil, 40001, 53, []byte("x"))
	atomic := testIpv6FragmentPacket(testIpv6Src, testIpv6Dst, false, byte(ipProtocolNumberUdp), 0, false, 1, plain6[Ipv6HeaderSize:])
	if isIpFragmentPacket(plain6) || isIpFragmentPacket(atomic) || isIpFragmentPacket(testingUdp4Packet("10.0.0.1", "203.0.113.7", 53, nil)) || isIpFragmentPacket(nil) {
		t.Fatal("non-fragment recognized as a fragment")
	}
}

// the gate serves both families and hands back owned fragment groups with a
// fresh identification, as the v4 path does
func TestIpFragmentGateDualstack(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		fragments := fragmentTestPacketsForVersion(t, ipVersion, fragmentTestPayload(1400))
		var gate ipFragmentGate
		defer gate.close()
		source := SourceId(NewId())
		partial := gate.processOwned(source, TransferKey{}, protocol.ProvideMode_Public, MessagePoolCopy(fragments[1]))
		if partial.packet != nil || !partial.fragment || !partial.accepted {
			t.Fatalf("partial = %+v", partial)
		}
		result := gate.processOwned(source, TransferKey{}, protocol.ProvideMode_Public, MessagePoolCopy(fragments[0]))
		defer returnIpFragmentProcessResult(result)
		if result.packet == nil || len(result.fragments) != 2 {
			t.Fatalf("result = %+v", result)
		}
		ipPath, payload, err := ParseIpPathWithPayload(result.packet)
		if err != nil || ipPath.Version != ipVersion || !bytes.Equal(payload, fragmentTestPayload(1400)) {
			t.Fatalf("reassembled = %+v, %v", ipPath, err)
		}
		var identifications []uint32
		for i, fragment := range result.fragments {
			if !isIpFragmentPacket(fragment) {
				t.Fatalf("retained %d is not a fragment", i)
			}
			switch ipVersion {
			case 4:
				identifications = append(identifications, uint32(binary.BigEndian.Uint16(fragment[4:6])))
			case 6:
				identifications = append(identifications, ipv6FragmentFields(t, fragment).identification)
			}
		}
		if identifications[0] == 0 || identifications[0] != identifications[1] {
			t.Fatalf("retained identifications = %v", identifications)
		}
		// the retained group is in offset order
		if sendShard(result.fragments[0], 257) != sendShard(result.fragments[1], 257) {
			t.Fatal("retained fragments split across shards")
		}
	})
}

func TestOversizedIpv6UdpFragmentsAtIpLayer(t *testing.T) {
	payload := fragmentTestPayload(1200)
	packets, err := newFragmentTestStream(6).DataPackets(payload, len(payload), DefaultMtu)
	if err != nil {
		t.Fatalf("oversized IPv6 UDP: %v", err)
	}
	defer func() {
		for _, packet := range packets {
			MessagePoolReturn(packet)
		}
	}()
	if len(packets) != 2 {
		t.Fatalf("packets = %d, want 2 fragments", len(packets))
	}
	for _, packet := range packets {
		if !isIpv6FragmentPacket(packet) || DefaultMtu < len(packet) {
			t.Fatalf("packet len=%d fragment=%t", len(packet), isIpv6FragmentPacket(packet))
		}
	}
}

func TestFragmentIpv6PacketRejects(t *testing.T) {
	udp := testIpv6Udp(testIpv6Src, testIpv6Dst, nil, 40001, 53, fragmentTestPayload(100))
	if _, err := fragmentIpv6Packet(MessagePoolCopy(udp), Ipv6HeaderSize+ipv6FragmentHeaderSize+7); err != errIpv6MtuTooSmall {
		t.Fatalf("tiny mtu err = %v", err)
	}
	withHop := testIpv6Udp(testIpv6Src, testIpv6Dst, []testIpv6Ext{{kind: ipv6NextHeaderHopByHop}}, 40001, 53, fragmentTestPayload(100))
	if _, err := fragmentIpv6Packet(MessagePoolCopy(withHop), DefaultMtu); err != errIpv6ExtensionHeadersFragment {
		t.Fatalf("extension header err = %v", err)
	}
	if _, err := fragmentIpv6Packet(MessagePoolCopy(testingUdp4Packet("10.0.0.1", "203.0.113.7", 53, nil)), DefaultMtu); err != errInvalidIpv6Packet {
		t.Fatalf("ipv4 err = %v", err)
	}
}

// a learned path mtu shrinks udp datagrams and tcp segments in both families,
// never below the family minimum, and never grows again
func TestPathMtuClampsDataPackets(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		stream := newFragmentTestStream(ipVersion)
		payload := fragmentTestPayload(900)
		packets, err := stream.DataPackets(payload, len(payload), 1500)
		if err != nil || len(packets) != 1 {
			t.Fatalf("baseline = %d packets, %v", len(packets), err)
		}
		for _, packet := range packets {
			MessagePoolReturn(packet)
		}
		// far below the floor clamps to the floor, which still fragments 900
		// bytes only for v4 (576) and not for v6 (1280)
		stream.applyPathMtu(100)
		if got := int(stream.pathMtu.Load()); got != ipMinimumPathMtu(ipVersion) {
			t.Fatalf("path mtu = %d, want floor %d", got, ipMinimumPathMtu(ipVersion))
		}
		packets, err = stream.DataPackets(payload, len(payload), 1500)
		if err != nil {
			t.Fatal(err)
		}
		for _, packet := range packets {
			if ipMinimumPathMtu(ipVersion) < len(packet) {
				t.Fatalf("packet %d exceeds the path mtu %d", len(packet), ipMinimumPathMtu(ipVersion))
			}
			MessagePoolReturn(packet)
		}
		if ipVersion == 4 && len(packets) < 2 {
			t.Fatalf("v4 900 bytes at 576 = %d packets", len(packets))
		}
		if ipVersion == 6 && len(packets) != 1 {
			t.Fatalf("v6 900 bytes at 1280 = %d packets", len(packets))
		}
		// a larger signal never grows the learned value
		stream.applyPathMtu(1400)
		if got := int(stream.pathMtu.Load()); got != ipMinimumPathMtu(ipVersion) {
			t.Fatalf("path mtu grew to %d", got)
		}

		connection := &ConnectionState{ipVersion: ipVersion, sourceIp: stream.sourceIp, destinationIp: stream.destinationIp}
		connection.applyPathMtu(1300)
		segments, err := connection.DataPackets(payload, len(payload), 1500)
		if err != nil {
			t.Fatal(err)
		}
		for _, segment := range segments {
			if 1300 < len(segment) {
				t.Fatalf("segment %d exceeds the path mtu 1300", len(segment))
			}
			MessagePoolReturn(segment)
		}
		if ipVersion == 6 && len(segments) != 1 {
			t.Fatalf("v6 900 bytes at 1300 = %d segments", len(segments))
		}
	})
}

// a udp echo server on loopback for the NAT tests, answering every datagram
// with `reply` (or the datagram itself when reply is nil)
func testUdpEchoServer(t *testing.T, ipVersion int, reply []byte) *net.UDPConn {
	t.Helper()
	conn, err := net.ListenUDP(testUdpNetwork(ipVersion), &net.UDPAddr{IP: net.ParseIP(testLoopbackIp(ipVersion)), Port: 0})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	go HandleError(func() {
		buffer := make([]byte, 4096)
		for {
			n, addr, err := conn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			if reply != nil {
				conn.WriteToUDP(reply, addr)
			} else {
				conn.WriteToUDP(buffer[:n], addr)
			}
		}
	})
	return conn
}

// collects the local NAT's return packets
type natReceiveCollector struct {
	mutex   sync.Mutex
	packets [][]byte
	notify  chan struct{}
}

func newNatReceiveCollector() *natReceiveCollector {
	return &natReceiveCollector{notify: make(chan struct{}, 64)}
}

func (self *natReceiveCollector) receive(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
	self.mutex.Lock()
	self.packets = append(self.packets, append([]byte(nil), packet...))
	self.mutex.Unlock()
	select {
	case self.notify <- struct{}{}:
	default:
	}
}

func (self *natReceiveCollector) wait(t *testing.T, count int, timeout time.Duration) [][]byte {
	t.Helper()
	deadline := time.After(timeout)
	for {
		self.mutex.Lock()
		packets := append([][]byte{}, self.packets...)
		self.mutex.Unlock()
		if count <= len(packets) {
			return packets
		}
		select {
		case <-self.notify:
		case <-deadline:
			t.Fatalf("received %d packets, want %d", len(packets), count)
		}
	}
}

// the return path: a datagram from the server larger than the tunnel mtu
// leaves the NAT as fragments that reassemble to the server's bytes
func TestLocalUserNatUdpReturnFragments(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reply := fragmentTestPayload(1400)
		echo := testUdpEchoServer(t, ipVersion, reply)

		localUserNat := NewLocalUserNat(ctx, "testReturnFragments", DefaultLocalUserNatSettings())
		defer localUserNat.Close()
		collector := newNatReceiveCollector()
		defer localUserNat.AddReceivePacketCallback(collector.receive)()

		ipPath := udpTestPath(ipVersion)
		ipPath.DestinationIp = net.ParseIP(testLoopbackIp(ipVersion))
		ipPath.DestinationPort = echo.LocalAddr().(*net.UDPAddr).Port
		if !localUserNat.SendPacket(SourceId(NewId()), protocol.ProvideMode_Network, MessagePoolCopy(ipOosUdpPacket(ipPath, []byte("ping"))), -1) {
			t.Fatal("send not queued")
		}
		packets := collector.wait(t, 2, 10*time.Second)
		var gate ipFragmentGate
		defer gate.close()
		var result ipFragmentProcessResult
		source := SourceId(NewId())
		for _, packet := range packets[:2] {
			if !isIpFragmentPacket(packet) || DefaultMtu < len(packet) {
				t.Fatalf("return packet len=%d fragment=%t", len(packet), isIpFragmentPacket(packet))
			}
			result = gate.processOwned(source, TransferKey{}, protocol.ProvideMode_Network, MessagePoolCopy(packet))
		}
		defer returnIpFragmentProcessResult(result)
		if result.packet == nil {
			t.Fatal("return fragments did not reassemble")
		}
		returnPath, payload, err := ParseIpPathWithPayload(result.packet)
		if err != nil || returnPath.Version != ipVersion || returnPath.SourcePort != ipPath.DestinationPort || !bytes.Equal(payload, reply) {
			t.Fatalf("reassembled return = %+v, %v, %d bytes", returnPath, err, len(payload))
		}
	})
}

// the send path: a datagram that arrives at the NAT as fragments is
// reassembled before the egress socket write, so the server sees one datagram
func TestLocalUserNatUdpIngressFragments(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		echo := testUdpEchoServer(t, ipVersion, nil)

		localUserNat := NewLocalUserNat(ctx, "testIngressFragments", DefaultLocalUserNatSettings())
		defer localUserNat.Close()
		collector := newNatReceiveCollector()
		defer localUserNat.AddReceivePacketCallback(collector.receive)()

		payload := fragmentTestPayload(1400)
		ipPath := udpTestPath(ipVersion)
		ipPath.DestinationIp = net.ParseIP(testLoopbackIp(ipVersion))
		ipPath.DestinationPort = echo.LocalAddr().(*net.UDPAddr).Port
		var fragments [][]byte
		var err error
		switch ipVersion {
		case 4:
			fragments, err = fragmentIpv4Packet(MessagePoolCopy(ipOosUdpPacket(ipPath, payload)), DefaultMtu)
		case 6:
			fragments, err = fragmentIpv6Packet(MessagePoolCopy(ipOosUdpPacket(ipPath, payload)), DefaultMtu)
		}
		if err != nil || len(fragments) != 2 {
			t.Fatalf("fragments = %d, %v", len(fragments), err)
		}
		// out of order, as a network may deliver them
		source := SourceId(NewId())
		for _, fragment := range [][]byte{fragments[1], fragments[0]} {
			if !localUserNat.SendPacket(source, protocol.ProvideMode_Network, fragment, -1) {
				t.Fatal("send not queued")
			}
		}
		packets := collector.wait(t, 2, 10*time.Second)
		var gate ipFragmentGate
		defer gate.close()
		var result ipFragmentProcessResult
		for _, packet := range packets[:2] {
			result = gate.processOwned(source, TransferKey{}, protocol.ProvideMode_Network, MessagePoolCopy(packet))
		}
		defer returnIpFragmentProcessResult(result)
		if result.packet == nil {
			t.Fatal("echoed fragments did not reassemble")
		}
		_, echoed, err := ParseIpPathWithPayload(result.packet)
		if err != nil || !bytes.Equal(echoed, payload) {
			t.Fatalf("echo = %v, %d bytes", err, len(echoed))
		}
	})
}
