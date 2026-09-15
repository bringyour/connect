package connect

// behavior tests for the icmp echo path (see ICMP.md) at each area the
// design touches: the parse gate, the LocalUserNat dispatch and egress, the
// cfaa policy branch, the dmca safe defaults, reset/oos synthesis, the mux
// peek classification, the send shard hash, and the multi client gate and
// affinity. the client send gate is additionally pinned by
// TestMultiClientUnsupportedPacketLoggingIsSparse for non-icmp protocols.

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/layers"

	"golang.org/x/net/icmp"

	"github.com/urnetwork/connect/v2026/protocol"
)

// builds an ipv4 icmp packet with the given type/code
func testingIcmp4Packet(sourceIp string, destinationIp string, icmpType uint8, code uint8, id int, seq int, payload []byte) []byte {
	ip := &layers.IPv4{
		Version:  4,
		TTL:      64,
		SrcIP:    net.ParseIP(sourceIp).To4(),
		DstIP:    net.ParseIP(destinationIp).To4(),
		Protocol: layers.IPProtocolICMPv4,
	}
	icmpLayer := &layers.ICMPv4{
		TypeCode: layers.CreateICMPv4TypeCode(icmpType, code),
		Id:       uint16(id),
		Seq:      uint16(seq),
	}
	buffer := gopacket.NewSerializeBuffer()
	err := gopacket.SerializeLayers(
		buffer,
		gopacket.SerializeOptions{ComputeChecksums: true, FixLengths: true},
		ip,
		icmpLayer,
		gopacket.Payload(payload),
	)
	if err != nil {
		panic(err)
	}
	packet := make([]byte, len(buffer.Bytes()))
	copy(packet, buffer.Bytes())
	return packet
}

func testingIcmp4EchoPacket(sourceIp string, destinationIp string, id int, seq int, payload []byte) []byte {
	return testingIcmp4Packet(sourceIp, destinationIp, layers.ICMPv4TypeEchoRequest, 0, id, seq, payload)
}

// builds an ipv6 icmp echo packet; `echoType` is 128 (request) or 129 (reply)
func testingIcmp6EchoTypePacket(sourceIp string, destinationIp string, echoType uint8, id int, seq int, payload []byte) []byte {
	ip := &layers.IPv6{
		Version:    6,
		HopLimit:   64,
		SrcIP:      net.ParseIP(sourceIp).To16(),
		DstIP:      net.ParseIP(destinationIp).To16(),
		NextHeader: layers.IPProtocolICMPv6,
	}
	icmpLayer := &layers.ICMPv6{
		TypeCode: layers.CreateICMPv6TypeCode(echoType, 0),
	}
	icmpLayer.SetNetworkLayerForChecksum(ip)
	echo := &layers.ICMPv6Echo{
		Identifier: uint16(id),
		SeqNumber:  uint16(seq),
	}
	buffer := gopacket.NewSerializeBuffer()
	err := gopacket.SerializeLayers(
		buffer,
		gopacket.SerializeOptions{ComputeChecksums: true, FixLengths: true},
		ip,
		icmpLayer,
		echo,
		gopacket.Payload(payload),
	)
	if err != nil {
		panic(err)
	}
	packet := make([]byte, len(buffer.Bytes()))
	copy(packet, buffer.Bytes())
	return packet
}

// testingIcmpPathIps is the (source, destination) pair the icmp path tests
// use for a family: a private source and a public resolver destination.
func testingIcmpPathIps(ipVersion int) (net.IP, net.IP) {
	if ipVersion == 6 {
		return net.ParseIP("fd00::1"), net.ParseIP("2606:4700:4700::1111")
	}
	return net.ParseIP("10.0.0.1").To4(), net.ParseIP("203.0.113.7").To4()
}

func testingIcmp6EchoPacket(sourceIp string, destinationIp string, id int, seq int, payload []byte) []byte {
	return testingIcmp6EchoTypePacket(sourceIp, destinationIp, 128, id, seq, payload)
}

// skips the test where the platform refuses an unprivileged datagram icmp
// socket (see the icmpEgress backends)
func requireIcmpEgress(t *testing.T) {
	t.Helper()
	if probe, err := icmp.ListenPacket("udp4", ""); err != nil {
		t.Skipf("no unprivileged icmp socket: %v", err)
	} else {
		probe.Close()
	}
}

// the parse convention: an echo request carries the identifier as the source
// port, an echo reply as the destination port, so the reply path is exactly
// the reversed request path
func TestParseIpPathIcmpEcho(t *testing.T) {
	payload := []byte("ping payload")

	request4 := testingIcmp4EchoPacket("10.0.0.1", "203.0.113.7", 0x1234, 7, payload)
	requestPath, requestPayload, err := ParseIpPathWithPayload(request4)
	if err != nil {
		t.Fatalf("request parse = %v", err)
	}
	if requestPath.Protocol != IpProtocolIcmp ||
		requestPath.Version != 4 ||
		requestPath.SourcePort != 0x1234 ||
		requestPath.DestinationPort != 0 ||
		!requestPath.SourceIp.Equal(net.ParseIP("10.0.0.1")) ||
		!requestPath.DestinationIp.Equal(net.ParseIP("203.0.113.7")) {
		t.Fatalf("request path = %+v", requestPath)
	}
	if !bytes.Equal(requestPayload, payload) {
		t.Fatalf("request payload = %v", requestPayload)
	}

	reply4 := testingIcmp4Packet("203.0.113.7", "10.0.0.1", layers.ICMPv4TypeEchoReply, 0, 0x1234, 7, payload)
	replyPath, err := ParseIpPath(reply4)
	if err != nil {
		t.Fatalf("reply parse = %v", err)
	}
	if replyPath.Protocol != IpProtocolIcmp ||
		replyPath.SourcePort != 0 ||
		replyPath.DestinationPort != 0x1234 {
		t.Fatalf("reply path = %+v", replyPath)
	}
	// the reply matches the reversed request flow key
	reversedRequestPath := requestPath.ReverseValue()
	if replyPath.ToIp4Path() != reversedRequestPath.ToIp4Path() {
		t.Fatalf("reply path %+v does not reverse to the request path", replyPath)
	}

	request6 := testingIcmp6EchoPacket("fd00::1", "2606:4700:4700::1111", 0x4321, 9, payload)
	requestPath6, err := ParseIpPath(request6)
	if err != nil {
		t.Fatalf("request6 parse = %v", err)
	}
	if requestPath6.Protocol != IpProtocolIcmp ||
		requestPath6.Version != 6 ||
		requestPath6.SourcePort != 0x4321 ||
		requestPath6.DestinationPort != 0 {
		t.Fatalf("request6 path = %+v", requestPath6)
	}
	reply6 := testingIcmp6EchoTypePacket("2606:4700:4700::1111", "fd00::1", 129, 0x4321, 9, payload)
	replyPath6, err := ParseIpPath(reply6)
	if err != nil {
		t.Fatalf("reply6 parse = %v", err)
	}
	reversedRequestPath6 := requestPath6.ReverseValue()
	if replyPath6.ToIp6Path() != reversedRequestPath6.ToIp6Path() {
		t.Fatalf("reply6 path %+v does not reverse to the request6 path", replyPath6)
	}
}

// echo-only: every other icmp type, a nonzero code, a truncated header, and
// a version/protocol mismatch all fail the parse
func TestParseIpPathIcmpUnsupportedTypes(t *testing.T) {
	// timestamp request
	timestamp := testingIcmp4Packet("10.0.0.1", "203.0.113.7", 13, 0, 1, 1, nil)
	if _, err := ParseIpPath(timestamp); err == nil {
		t.Fatal("timestamp parsed")
	}

	// echo with a nonzero code
	badCode := testingIcmp4Packet("10.0.0.1", "203.0.113.7", layers.ICMPv4TypeEchoRequest, 1, 1, 1, nil)
	if _, err := ParseIpPath(badCode); err == nil {
		t.Fatal("nonzero code parsed")
	}

	// icmpv6 neighbor solicitation type never parses
	ndp := testingIcmp6EchoPacket("fe80::1", "fe80::2", 1, 1, nil)
	ndp[Ipv6HeaderSize] = 135
	if _, err := ParseIpPath(ndp); err == nil {
		t.Fatal("ndp parsed")
	}

	// truncated icmp header
	truncated := testingIcmp4EchoPacket("10.0.0.1", "203.0.113.7", 1, 1, nil)
	truncated = truncated[:Ipv4HeaderSizeWithoutExtensions+4]
	// fix the total length to match the truncation
	truncated[2] = 0
	truncated[3] = byte(len(truncated))
	if _, err := ParseIpPath(truncated); err == nil {
		t.Fatal("truncated parsed")
	}

	// the icmp variant must match the ip version: protocol 58 inside ipv4
	mismatch := testingIcmp4EchoPacket("10.0.0.1", "203.0.113.7", 1, 1, nil)
	mismatch[9] = 58
	if _, err := ParseIpPath(mismatch); err == nil || !strings.Contains(err.Error(), "No support for protocol 58") {
		t.Fatalf("version mismatch = %v", err)
	}
}

// only echo requests create egress flows: an outbound echo reply is an
// orphan and drops at the dispatch, as does a non-echo type
func TestLocalUserNatIcmpOrphanDrop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	localUserNat := NewLocalUserNat(ctx, "testIcmpOrphan", DefaultLocalUserNatSettings())
	defer localUserNat.Close()

	var receiveCount atomic.Int64
	unsub := localUserNat.AddReceivePacketCallback(func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		receiveCount.Add(1)
	})
	defer unsub()

	orphanReply := testingIcmp4Packet("10.0.0.1", "203.0.113.7", layers.ICMPv4TypeEchoReply, 0, 0x1234, 1, []byte("ping"))
	if success := localUserNat.SendPacket(SourceId(NewId()), protocol.ProvideMode_Network, orphanReply, -1); !success {
		t.Fatal("send not queued")
	}
	timestamp := testingIcmp4Packet("10.0.0.1", "203.0.113.7", 13, 0, 1, 1, nil)
	if success := localUserNat.SendPacket(SourceId(NewId()), protocol.ProvideMode_Network, timestamp, -1); !success {
		t.Fatal("send not queued")
	}

	// the drops are silent; give the dispatch loop time to run
	select {
	case <-time.After(200 * time.Millisecond):
	case <-ctx.Done():
	}
	if count := receiveCount.Load(); count != 0 {
		t.Fatalf("orphan icmp unexpectedly received %d", count)
	}
}

// end to end through the LocalUserNat: an echo request to loopback returns
// an echo reply with the inner identifier, sequence, and payload restored.
// skips where the platform refuses an unprivileged datagram icmp socket.
func TestIpEgressIcmp4Loopback(t *testing.T) {
	requireIcmpEgress(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	localUserNat := NewLocalUserNat(ctx, "testIcmpEgress", DefaultLocalUserNatSettings())
	defer localUserNat.Close()

	type receivedPacket struct {
		ipPath *IpPath
		packet []byte
	}
	received := make(chan receivedPacket, 16)
	unsub := localUserNat.AddReceivePacketCallback(func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		// the callback borrows the packet; copy out
		packetCopy := make([]byte, len(packet))
		copy(packetCopy, packet)
		select {
		case received <- receivedPacket{ipPath: ipPath, packet: packetCopy}:
		default:
		}
	})
	defer unsub()

	payload := []byte("urnetwork icmp echo test")
	request := testingIcmp4EchoPacket("10.0.0.1", "127.0.0.1", 0x2345, 3, payload)
	if success := localUserNat.SendPacket(SourceId(NewId()), protocol.ProvideMode_Network, request, -1); !success {
		t.Fatal("send not queued")
	}

	select {
	case <-ctx.Done():
		t.Fatal("no echo reply")
	case r := <-received:
		// the flow identity is the request direction
		if r.ipPath.Protocol != IpProtocolIcmp || r.ipPath.SourcePort != 0x2345 {
			t.Fatalf("flow path = %+v", r.ipPath)
		}
		replyPath, replyPayload, err := ParseIpPathWithPayload(r.packet)
		if err != nil {
			t.Fatalf("reply parse = %v", err)
		}
		if replyPath.DestinationPort != 0x2345 ||
			!replyPath.SourceIp.Equal(net.ParseIP("127.0.0.1")) ||
			!replyPath.DestinationIp.Equal(net.ParseIP("10.0.0.1")) {
			t.Fatalf("reply path = %+v", replyPath)
		}
		if r.packet[Ipv4HeaderSizeWithoutExtensions] != 0 {
			t.Fatalf("reply type = %d", r.packet[Ipv4HeaderSizeWithoutExtensions])
		}
		seq := int(r.packet[Ipv4HeaderSizeWithoutExtensions+6])<<8 | int(r.packet[Ipv4HeaderSizeWithoutExtensions+7])
		if seq != 3 {
			t.Fatalf("reply seq = %d", seq)
		}
		if !bytes.Equal(replyPayload, payload) {
			t.Fatalf("reply payload = %q", replyPayload)
		}
	}
}

// the reply builder: correct headers, identifier restoration, and checksums
// (plain rfc 1071 for v4, pseudo-header for v6); oversized replies drop
func TestIcmpEchoReplyPacketBuild(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	payload := []byte("reply payload")

	sequence4 := NewIcmpSequence(
		ctx,
		nil,
		TransferPath{},
		protocol.ProvideMode_Network,
		4,
		net.ParseIP("10.0.0.1").To4(),
		0x1234,
		net.ParseIP("203.0.113.7").To4(),
		DefaultIcmpBufferSettings(),
	)
	packet4 := sequence4.echoReplyPacket(7, payload)
	replyPath, replyPayload, err := ParseIpPathWithPayload(packet4)
	if err != nil {
		t.Fatalf("reply4 parse = %v", err)
	}
	if !replyPath.SourceIp.Equal(net.ParseIP("203.0.113.7")) ||
		!replyPath.DestinationIp.Equal(net.ParseIP("10.0.0.1")) ||
		replyPath.DestinationPort != 0x1234 {
		t.Fatalf("reply4 path = %+v", replyPath)
	}
	if !bytes.Equal(replyPayload, payload) {
		t.Fatalf("reply4 payload = %q", replyPayload)
	}
	// verify the plain icmp checksum: recomputing over the message with the
	// stored checksum in place sums to zero-complement
	icmp4 := packet4[Ipv4HeaderSizeWithoutExtensions:]
	if checksumFinish(checksumAdd(0, icmp4)) != 0 {
		t.Fatal("reply4 icmp checksum invalid")
	}
	// cross-check the decode with gopacket
	decoded := gopacket.NewPacket(packet4, layers.LayerTypeIPv4, gopacket.Default)
	if icmpLayer, ok := decoded.Layer(layers.LayerTypeICMPv4).(*layers.ICMPv4); !ok {
		t.Fatal("reply4 gopacket decode failed")
	} else if icmpLayer.Id != 0x1234 || icmpLayer.Seq != 7 || icmpLayer.TypeCode.Type() != layers.ICMPv4TypeEchoReply {
		t.Fatalf("reply4 gopacket = %+v", icmpLayer)
	}

	sequence6 := NewIcmpSequence(
		ctx,
		nil,
		TransferPath{},
		protocol.ProvideMode_Network,
		6,
		net.ParseIP("fd00::1").To16(),
		0x4321,
		net.ParseIP("2606:4700:4700::1111").To16(),
		DefaultIcmpBufferSettings(),
	)
	packet6 := sequence6.echoReplyPacket(9, payload)
	replyPath6, _, err := ParseIpPathWithPayload(packet6)
	if err != nil {
		t.Fatalf("reply6 parse = %v", err)
	}
	if replyPath6.DestinationPort != 0x4321 {
		t.Fatalf("reply6 path = %+v", replyPath6)
	}
	// verify the pseudo-header checksum: transportChecksum over the message
	// with the stored checksum in place is zero
	icmp6 := packet6[Ipv6HeaderSize:]
	if transportChecksum(ipProtocolNumberIcmp6, net.ParseIP("2606:4700:4700::1111").To16(), net.ParseIP("fd00::1").To16(), icmp6) != 0 {
		t.Fatal("reply6 icmp checksum invalid")
	}

	// oversized for the mtu drops
	oversized := sequence4.echoReplyPacket(1, make([]byte, DefaultMtu))
	if oversized != nil {
		t.Fatal("oversized reply built")
	}
}

// icmp flows hash stably to one send shard: the identifier is the flow
// identity, not the per-packet type/code/checksum or sequence
func TestSendShardIcmpFlowStable(t *testing.T) {
	shardCount := 4
	shard := sendShard(testingIcmp4EchoPacket("10.0.0.1", "203.0.113.7", 0x1234, 1, []byte("a")), shardCount)
	for seq := 2; seq < 32; seq += 1 {
		payload := bytes.Repeat([]byte{byte(seq)}, seq)
		packet := testingIcmp4EchoPacket("10.0.0.1", "203.0.113.7", 0x1234, seq, payload)
		if got := sendShard(packet, shardCount); got != shard {
			t.Fatalf("seq %d moved shard %d -> %d", seq, shard, got)
		}
	}
	shard6 := sendShard(testingIcmp6EchoPacket("fd00::1", "2606:4700:4700::1111", 0x4321, 1, []byte("a")), shardCount)
	for seq := 2; seq < 32; seq += 1 {
		packet := testingIcmp6EchoPacket("fd00::1", "2606:4700:4700::1111", 0x4321, seq, bytes.Repeat([]byte{byte(seq)}, seq))
		if got := sendShard(packet, shardCount); got != shard6 {
			t.Fatalf("seq %d moved shard6 %d -> %d", seq, shard6, got)
		}
	}
}

// the policy branch: icmp bypasses the port policy in both address families
// while the blocked-ip reputation check still applies
func TestCfaaInspectIcmp(t *testing.T) {
	d := newCfaaDetector(DefaultCfaaSecurityPolicySettings())

	if got := d.inspect(net.ParseIP("203.0.113.7"), 0, IpProtocolIcmp, 4); got != cfaaAllow {
		t.Fatalf("icmp4 clean = %v, want allow", got)
	}
	if got := d.inspect(net.ParseIP("2001:db8::7"), 0, IpProtocolIcmp, 6); got != cfaaAllow {
		t.Fatalf("icmp6 clean = %v, want allow", got)
	}

	if cfaaBlockedPrefixCount < 10_000 {
		t.Fatalf("default CFAA IPv4 records = %d, want at least reviewed minimum 10000", cfaaBlockedPrefixCount)
	}
	lo4, _ := cfaaRangeAt(0)
	blocked4 := net.IPv4(byte(lo4>>24), byte(lo4>>16), byte(lo4>>8), byte(lo4))
	if got := d.inspect(blocked4, 0, IpProtocolIcmp, 4); got != cfaaDrop {
		t.Fatalf("icmp4 blocked ip = %v, want drop", got)
	}

	if cfaaBlockedPrefix6Count < 100 {
		t.Fatalf("default CFAA IPv6 records = %d, want at least reviewed minimum 100", cfaaBlockedPrefix6Count)
	}
	data6 := cfaaBlockedPrefix6Data
	blocked6 := net.IP(make([]byte, net.IPv6len))
	copy(blocked6, data6[:net.IPv6len])
	if got := d.inspect(blocked6, 0, IpProtocolIcmp, 6); got != cfaaDrop {
		t.Fatalf("icmp6 blocked ip = %v, want drop", got)
	}
}

// the dpi safe default: icmp (like any non-transport protocol) is allowed
// without payload inspection and creates no tracked flow state
func TestDmcaNonTransportProtocolAllow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	dmca := newDmcaDetector(ctx, DefaultDmcaSecurityPolicySettings(), newWebStandardDetector(DefaultWebStandardSettings()))
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		sourceIp, destinationIp := testingIcmpPathIps(ipVersion)
		for _, proto := range []IpProtocol{IpProtocolIcmp, IpProtocol(99)} {
			ipPath := &IpPath{
				Version:         ipVersion,
				Protocol:        proto,
				SourceIp:        sourceIp,
				SourcePort:      0,
				DestinationIp:   destinationIp,
				DestinationPort: 8443,
			}
			if got := dmca.classify(ipPath, []byte{0x16, 0x03, 0x01}); got != dmcaAllow {
				t.Fatalf("protocol %v classify = %v, want allow", proto, got)
			}
			dmca.touchEgress(ipPath)
			dmca.touchIngress(ipPath.Reverse())
		}
		if count := dmca.flowCount(); count != 0 {
			t.Fatalf("flow count = %d, want 0", count)
		}
	})
}

// reset/oos synthesis safe defaults: non-tcp paths get no synthetic reset,
// and the oos builder returns nil rather than panicking for icmp
func TestIpOosNonTransportNil(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		sourceIp, destinationIp := testingIcmpPathIps(ipVersion)
		for _, proto := range []IpProtocol{IpProtocolUdp, IpProtocolIcmp, IpProtocolUnknown, IpProtocol(99)} {
			ipPath := &IpPath{
				Version:         ipVersion,
				Protocol:        proto,
				SourceIp:        sourceIp,
				SourcePort:      1000,
				DestinationIp:   destinationIp,
				DestinationPort: 2000,
			}
			if packet, ok := ipOosRst(ipPath); ok || packet != nil {
				t.Errorf("protocol %v: rst = %v, %v; want nil, false", proto, packet, ok)
			}
			if proto != IpProtocolUdp {
				if packet := ipOosPacket(ipPath, []byte("payload")); packet != nil {
					t.Errorf("protocol %v: oos packet = %v; want nil", proto, packet)
				}
			}
		}
	})
}

// mux classification: icmp passes through unclaimed with no parse in both
// address families
func TestPeekClaimIcmpPassthrough(t *testing.T) {
	var tls4 tlsSegment
	packet4 := testingIcmp4EchoPacket("10.0.0.1", "203.0.113.7", 0x1234, 1, []byte("ping"))
	if got := peekClaim(packet4, &tls4); got != peekOther {
		t.Fatalf("icmp4 peek = %v, want other", got)
	}

	var tls6 tlsSegment
	packet6 := testingIcmp6EchoPacket("fd00::1", "2606:4700:4700::1111", 0x1234, 1, []byte("ping"))
	if got := peekClaim(packet6, &tls6); got != peekOther {
		t.Fatalf("icmp6 peek = %v, want other", got)
	}
}

// the client send gate: icmp drops with its own sparse counter while the
// gate is off, and proceeds into the policy when enabled
func TestMultiClientIcmpGate(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		request := testingIcmp4EchoPacket("10.0.0.1", "203.0.113.7", 0x1234, 1, []byte("ping"))
		if ipVersion == 6 {
			request = testingIcmp6EchoPacket("fd00::1", "2606:4700:4700::1111", 0x1234, 1, []byte("ping"))
		}

		disabled := &RemoteUserNatMultiClient{
			ctx:      ctx,
			log:      NewNoopLogger(),
			settings: DefaultMultiClientSettings(),
		}
		if disabled.SendPacket(TransferPath{}, protocol.ProvideMode_Network, request, 0) {
			t.Fatal("gated icmp packet was accepted")
		}
		if count := disabled.sendIcmpDisabledDropCount.Load(); count != 1 {
			t.Fatalf("icmp disabled drop count = %d, want 1", count)
		}
		if count := disabled.sendParseDropCount.Load(); count != 0 {
			t.Fatalf("parse drop count = %d, want 0", count)
		}

		enabledSettings := DefaultMultiClientSettings()
		enabledSettings.EnableIcmp = true
		enabled := &RemoteUserNatMultiClient{
			ctx:            ctx,
			log:            NewNoopLogger(),
			settings:       enabledSettings,
			securityPolicy: &failingEgressSecurityPolicy{stats: DefaultSecurityPolicyStatsCollector()},
		}
		if enabled.SendPacket(TransferPath{}, protocol.ProvideMode_Network, request, 0) {
			t.Fatal("packet accepted through failing policy")
		}
		// reaching the policy proves the gate passed
		if count := enabled.sendPolicyDropCount.Load(); count != 1 {
			t.Fatalf("policy drop count = %d, want 1", count)
		}
		if count := enabled.sendIcmpDisabledDropCount.Load(); count != 0 {
			t.Fatalf("icmp disabled drop count = %d, want 0", count)
		}
	})
}

// icmp affinity is per destination ip, ahead of the port buckets: raw-ip
// pings to one host share a bucket instead of collapsing into the shared
// port-0 (below 1024) bucket
func TestMultiClientAffinityIcmp(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		sourceIp, destinationIp := testingIcmpPathIps(ipVersion)
		multiClient := &RemoteUserNatMultiClient{}
		multiClient.config.Store(&multiClientConfig{
			performanceProfile: nil,
			serverNameLookup:   nil,
		})

		icmpPath := &IpPath{
			Version:       ipVersion,
			Protocol:      IpProtocolIcmp,
			SourceIp:      sourceIp,
			SourcePort:    0x1234,
			DestinationIp: destinationIp,
		}
		affinityPaths := multiClient.affinityIpPathsWithLock(icmpPath)
		if len(affinityPaths) != 1 {
			t.Fatalf("affinity paths = %d, want 1", len(affinityPaths))
		}
		if !affinityPaths[0].DestinationIp.Equal(destinationIp) ||
			affinityPaths[0].DestinationPort != 0 ||
			affinityPaths[0].ServerName != "" {
			t.Fatalf("affinity path = %+v", affinityPaths[0])
		}

		// contrast: a port-0 udp path collapses into the per-port bucket with no
		// destination ip
		udpPath := &IpPath{
			Version:       ipVersion,
			Protocol:      IpProtocolUdp,
			SourceIp:      sourceIp,
			SourcePort:    40000,
			DestinationIp: destinationIp,
		}
		udpAffinityPaths := multiClient.affinityIpPathsWithLock(udpPath)
		if len(udpAffinityPaths) != 1 || udpAffinityPaths[0].DestinationIp != nil {
			t.Fatalf("udp port 0 affinity path = %+v", udpAffinityPaths[0])
		}
	})
}

// the icmp budget item of the provider byte-cost model: floors dominate a
// small target, the item scales with a large target, and it is additive —
// the udp/tcp shares are untouched by it. Unlimited flows without a target,
// whatever the process budget.
func TestProviderIcmpSettings(t *testing.T) {
	defer SetMemoryBudget(0)

	// small (mobile) target: the functional floors dominate the byte math
	smallTarget := DefaultProviderLocalUserNatSettingsWithMemoryTarget(4 * 1024 * 1024)
	AssertEqual(t, smallTarget.IcmpBufferSettings.UserLimit, providerMinIcmpUserLimit)
	AssertEqual(t, smallTarget.IcmpBufferSettings.GlobalLimit, providerMinIcmpGlobalLimit)

	// large target: the item derives from the byte-cost model
	// (natTarget/16/providerIcmpFlowByteCount)
	largeTarget := DefaultProviderLocalUserNatSettingsWithMemoryTarget(128 * 1024 * 1024)
	AssertEqual(t, largeTarget.IcmpBufferSettings.UserLimit, 256)
	AssertEqual(t, largeTarget.IcmpBufferSettings.GlobalLimit, 512)
	// the derived caps stay within the item's share of the nat target
	icmpTargetByteCount := ByteCount(largeTarget.IcmpBufferSettings.GlobalLimit) * providerIcmpFlowByteCount
	if mib(128)/2/providerIcmpTargetDivisor < icmpTargetByteCount {
		t.Errorf("icmp caps claim %d bytes, over the item share", icmpTargetByteCount)
	}
	// additive: the udp/tcp shares keep their historical derivations,
	// untouched by the icmp item
	AssertEqual(t, largeTarget.UdpBufferSettings.UserLimit, 4915)
	AssertEqual(t, largeTarget.UdpBufferSettings.GlobalLimit, 19660)
	AssertEqual(t, largeTarget.TcpBufferSettings.UserLimit, 1638)
	AssertEqual(t, largeTarget.TcpBufferSettings.GlobalLimit, 3276)

	unbudgeted := DefaultProviderLocalUserNatSettings()
	AssertEqual(t, unbudgeted.IcmpBufferSettings.UserLimit, 0)
	AssertEqual(t, unbudgeted.IcmpBufferSettings.GlobalLimit, 0)

	// a process budget (the ios packet tunnel's 24 MiB here) is not a flow
	// policy: the targetless provider keeps its unlimited table
	SetMemoryBudget(24 * 1024 * 1024)
	budgeted := DefaultProviderLocalUserNatSettings()
	AssertEqual(t, budgeted.IcmpBufferSettings.UserLimit, 0)
	AssertEqual(t, budgeted.IcmpBufferSettings.GlobalLimit, 0)
}
