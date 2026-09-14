package connect

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// ip_ipv6_icmp_test.go — the icmp control messages the NAT acts on or drops
// (IPV6.md C4): path-mtu signals (v4 fragmentation-needed, v6 packet-too-big)
// shrink the flow they describe, and v6 link-local control chatter (neighbor
// and router discovery, mld, redirect) is dropped silently.

// the path-mtu message round trips through the parsers in both families and
// for both transports, and the rejects hold: a zero v4 next-hop mtu, the
// wrong type, and an embed short of 8 transport bytes
func TestIcmpPacketTooBigRoundTrip(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		for _, original := range []*IpPath{udpTestPath(ipVersion), icmpTcpTestPath(ipVersion)} {
			packet, ok := ipOosPacketTooBig(original, 1280)
			if !ok {
				t.Fatalf("%s build failed", original.Protocol)
			}
			// addressed back to the oversized packet's sender
			outer, err := ParseIpPath(packet)
			if err == nil {
				t.Fatalf("%s: ParseIpPath accepted an icmp control message: %+v", original.Protocol, outer)
			}
			icmp, ok := ipIcmpBody(packet)
			if !ok {
				t.Fatalf("%s: icmp body not found", original.Protocol)
			}
			var mtu int
			var embedded *IpPath
			switch ipVersion {
			case 4:
				mtu, embedded, ok = ipParseIcmpv4FragmentationNeeded(icmp)
				if _, _, ptbOk := ipParseIcmpv6PacketTooBig(icmp); ptbOk {
					t.Fatalf("v4 message parsed as a v6 packet-too-big")
				}
			case 6:
				mtu, embedded, ok = ipParseIcmpv6PacketTooBig(icmp)
				if _, _, fnOk := ipParseIcmpv4FragmentationNeeded(icmp); fnOk {
					t.Fatalf("v6 message parsed as a v4 fragmentation-needed")
				}
			}
			if !ok || mtu != 1280 {
				t.Fatalf("%s: parse = %t, mtu %d; want true, 1280", original.Protocol, ok, mtu)
			}
			if embedded.Version != original.Version ||
				embedded.Protocol != original.Protocol ||
				!embedded.SourceIp.Equal(original.SourceIp) ||
				!embedded.DestinationIp.Equal(original.DestinationIp) ||
				embedded.SourcePort != original.SourcePort ||
				embedded.DestinationPort != original.DestinationPort {
				t.Fatalf("%s: embedded = %+v, want the original flow %+v", original.Protocol, embedded, original)
			}
			if original.Protocol == IpProtocolTcp && embedded.SequenceNumber != original.SequenceNumber {
				t.Fatalf("embedded seq = %x, want %x", embedded.SequenceNumber, original.SequenceNumber)
			}

			// an ordinary unreachable is not a path-mtu signal
			unreachable, _ := ipOosUnreachable(original)
			unreachableIcmp, _ := ipIcmpBody(unreachable)
			if _, _, ok := ipParseIcmpv4FragmentationNeeded(unreachableIcmp); ok {
				t.Fatalf("%s: unreachable parsed as fragmentation-needed", original.Protocol)
			}
			if _, _, ok := ipParseIcmpv6PacketTooBig(unreachableIcmp); ok {
				t.Fatalf("%s: unreachable parsed as packet-too-big", original.Protocol)
			}
			// a truncated embed is rejected rather than misparsed
			embeddedHeaderSize := Ipv6HeaderSize
			if ipVersion == 4 {
				embeddedHeaderSize = Ipv4HeaderSizeWithoutExtensions
			}
			short := icmp[:icmpUnreachableHeaderSize+embeddedHeaderSize+7]
			if _, _, ok := ipParseIcmpv4FragmentationNeeded(short); ok {
				t.Fatalf("%s: truncated embed accepted (v4)", original.Protocol)
			}
			if _, _, ok := ipParseIcmpv6PacketTooBig(short); ok {
				t.Fatalf("%s: truncated embed accepted (v6)", original.Protocol)
			}
		}
	})
	// rfc 1191 predates the next-hop mtu field: a zero mtu is no signal
	zero, ok := ipOosPacketTooBig(udpTestPath(4), 0)
	if !ok {
		t.Fatal("v4 zero-mtu build failed")
	}
	if icmp, _ := ipIcmpBody(zero); true {
		if _, _, ok := ipParseIcmpv4FragmentationNeeded(icmp); ok {
			t.Fatal("v4 zero next-hop mtu accepted")
		}
	}
	if _, ok := ipOosPacketTooBig(&IpPath{Version: 4, Protocol: IpProtocolIcmp}, 1280); ok {
		t.Fatal("built a path-mtu signal for a non-transport flow")
	}
}

// the NAT applies a path-mtu signal from the source to the live flow it
// names: after the signal, the return datagrams of that flow leave in
// fragments no larger than the reported mtu. The tunnel mtu is raised above
// the v6 floor (1280, rfc 8200 §5) so a v6 signal has room to shrink; the
// product mtu of 1100 is already below that floor, where a v6 signal is
// correctly ignored.
func TestLocalUserNatPathMtuShrinksReturn(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		reply := fragmentTestPayload(1400)
		echo := testUdpEchoServer(t, ipVersion, reply)

		const tunnelMtu = 1500
		settings := DefaultLocalUserNatSettings()
		settings.UdpBufferSettings.Mtu = tunnelMtu
		localUserNat := NewLocalUserNat(ctx, "testPathMtu", settings)
		defer localUserNat.Close()
		collector := newNatReceiveCollector()
		defer localUserNat.AddReceivePacketCallback(collector.receive)()

		source := SourceId(NewId())
		ipPath := udpTestPath(ipVersion)
		ipPath.DestinationIp = net.ParseIP(testLoopbackIp(ipVersion))
		ipPath.DestinationPort = echo.LocalAddr().(*net.UDPAddr).Port
		if !localUserNat.SendPacket(source, protocol.ProvideMode_Network, MessagePoolCopy(ipOosUdpPacket(ipPath, []byte("ping"))), -1) {
			t.Fatal("send not queued")
		}
		reassemble := func(packets [][]byte) *IpPath {
			t.Helper()
			var gate ipFragmentGate
			defer gate.close()
			var result ipFragmentProcessResult
			for _, packet := range packets {
				result = gate.processOwned(source, TransferKey{}, protocol.ProvideMode_Network, MessagePoolCopy(packet))
			}
			defer returnIpFragmentProcessResult(result)
			if result.packet == nil {
				t.Fatal("return fragments did not reassemble")
			}
			returnPath, payload, err := ParseIpPathWithPayload(result.packet)
			if err != nil || !bytes.Equal(payload, reply) {
				t.Fatalf("reassembled return = %+v, %v, %d bytes", returnPath, err, len(payload))
			}
			return returnPath
		}
		// before the signal the reply fits the tunnel mtu in one packet
		packets := collector.wait(t, 1, 10*time.Second)
		if tunnelMtu < len(packets[0]) || isIpFragmentPacket(packets[0]) {
			t.Fatalf("pre-signal return len=%d fragment=%t", len(packets[0]), isIpFragmentPacket(packets[0]))
		}
		returnPath := reassemble(packets[:1])

		// the source reports that its path toward it carries at most 1300
		const pathMtu = 1300
		signal, ok := ipOosPacketTooBig(returnPath, pathMtu)
		if !ok {
			t.Fatal("path-mtu signal build failed")
		}
		if !localUserNat.SendPacket(source, protocol.ProvideMode_Network, MessagePoolCopy(signal), -1) {
			t.Fatal("signal not queued")
		}
		// the signal itself is consumed, never forwarded; the next reply
		// arrives in fragments no larger than the path mtu
		if !localUserNat.SendPacket(source, protocol.ProvideMode_Network, MessagePoolCopy(ipOosUdpPacket(ipPath, []byte("ping"))), -1) {
			t.Fatal("second send not queued")
		}
		packets = collector.wait(t, 3, 10*time.Second)
		for i, packet := range packets[1:3] {
			if pathMtu < len(packet) {
				t.Fatalf("post-signal return packet %d len=%d exceeds the path mtu %d", i, len(packet), pathMtu)
			}
			if !isIpFragmentPacket(packet) {
				t.Fatalf("post-signal return packet %d is not a fragment", i)
			}
		}
		reassemble(packets[1:3])
		if 3 < len(packets) {
			t.Fatalf("received %d packets, want exactly 3 (the signal must not be forwarded)", len(packets))
		}

		// a signal below the family floor never shrinks the flow further
		below, _ := ipOosPacketTooBig(returnPath, 100)
		if !localUserNat.SendPacket(source, protocol.ProvideMode_Network, MessagePoolCopy(below), -1) {
			t.Fatal("floor signal not queued")
		}
		if !localUserNat.SendPacket(source, protocol.ProvideMode_Network, MessagePoolCopy(ipOosUdpPacket(ipPath, []byte("ping"))), -1) {
			t.Fatal("third send not queued")
		}
		// at the floor the reply (udp header plus payload) splits into
		// ceil(ipPayload / fragmentPayload) fragments
		floor := ipMinimumPathMtu(ipVersion)
		fragmentPayloadByteCount := (floor - Ipv4HeaderSizeWithoutExtensions) &^ 7
		if ipVersion == 6 {
			fragmentPayloadByteCount = (floor - Ipv6HeaderSize - ipv6FragmentHeaderSize) &^ 7
		}
		ipPayloadByteCount := UdpHeaderSize + len(reply)
		floorFragmentCount := (ipPayloadByteCount + fragmentPayloadByteCount - 1) / fragmentPayloadByteCount
		if floorFragmentCount < 2 {
			t.Fatalf("floor %d does not fragment a %d byte datagram", floor, ipPayloadByteCount)
		}
		packets = collector.wait(t, 3+floorFragmentCount, 10*time.Second)
		for i, packet := range packets[3 : 3+floorFragmentCount] {
			if floor < len(packet) || !isIpFragmentPacket(packet) {
				t.Fatalf("floor-signal return packet %d len=%d fragment=%t, want a fragment within the family floor %d", i, len(packet), isIpFragmentPacket(packet), floor)
			}
		}
		reassemble(packets[3 : 3+floorFragmentCount])
		if 3+floorFragmentCount < len(packets) {
			t.Fatalf("received %d packets, want %d", len(packets), 3+floorFragmentCount)
		}
	})
}

// neighbor and router discovery, mld and redirect are dropped silently at
// the NAT; the flow machinery keeps working afterwards
func TestLocalUserNatDropsIcmpv6LinkControl(t *testing.T) {
	requireIpv6Loopback(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	echo := testUdpEchoServer(t, 6, nil)

	localUserNat := NewLocalUserNat(ctx, "testLinkControl", DefaultLocalUserNatSettings())
	defer localUserNat.Close()
	collector := newNatReceiveCollector()
	defer localUserNat.AddReceivePacketCallback(collector.receive)()

	source := SourceId(NewId())
	for _, icmpType := range []byte{130, 131, 132, 133, 134, 135, 136, 137} {
		// solicited-node multicast, as a real neighbor solicitation targets
		packet := testingIcmp6EchoTypePacket("fe80::1", "ff02::1:ff00:1", icmpType, 0, 0, make([]byte, 24))
		if !localUserNat.SendPacket(source, protocol.ProvideMode_Network, MessagePoolCopy(packet), -1) {
			t.Fatalf("type %d: send not queued", icmpType)
		}
	}
	select {
	case <-collector.notify:
		t.Fatal("link-local control message produced a return packet")
	case <-time.After(300 * time.Millisecond):
	}

	ipPath := udpTestPath(6)
	ipPath.DestinationIp = net.ParseIP("::1")
	ipPath.DestinationPort = echo.LocalAddr().(*net.UDPAddr).Port
	if !localUserNat.SendPacket(source, protocol.ProvideMode_Network, MessagePoolCopy(ipOosUdpPacket(ipPath, []byte("still alive"))), -1) {
		t.Fatal("send not queued")
	}
	packets := collector.wait(t, 1, 10*time.Second)
	if _, payload, err := ParseIpPathWithPayload(packets[0]); err != nil || string(payload) != "still alive" {
		t.Fatalf("echo after link-control drop = %v, %q", err, payload)
	}
}
