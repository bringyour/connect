package connect

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

func TestTunProductMtuEmitsAndReassemblesQuicMinimumUdpDatagram(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	settings := DefaultTunSettings()
	settings.Mtu = DefaultMtu
	tun, err := CreateTun(ctx, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer tun.Close()
	conn, err := tun.ListenUDP(&net.UDPAddr{
		IP:   net.IP(tun.LocalAddresses()[0].AsSlice()),
		Port: 0,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	payload := fragmentTestPayload(1200)
	if n, writeErr := conn.WriteTo(
		payload,
		&net.UDPAddr{IP: net.IPv4(198, 51, 100, 7), Port: 443},
	); writeErr != nil || n != len(payload) {
		t.Fatalf("write QUIC-minimum UDP datagram n=%d err=%v", n, writeErr)
	}

	var gate ipv4FragmentGate
	defer gate.close()
	var result ipv4FragmentProcessResult
	source := SourceId(NewId())
	for result.packet == nil {
		packet, readErr := tun.Read()
		if readErr != nil {
			t.Fatalf("read fragmented TUN packet: %v", readErr)
		}
		if DefaultMtu < len(packet) {
			MessagePoolReturn(packet)
			t.Fatalf("TUN emitted %d-byte packet above MTU %d", len(packet), DefaultMtu)
		}
		if !isIpv4FragmentPacket(packet) {
			MessagePoolReturn(packet)
			t.Fatalf("1,200-byte UDP datagram emitted unfragmented packet of %d bytes", len(packet))
		}
		result = gate.processOwned(
			source,
			TransferKey{},
			protocol.ProvideMode_Network,
			packet,
		)
		if !result.accepted {
			t.Fatal("gVisor IPv4 fragment was rejected")
		}
	}
	defer returnIpv4FragmentProcessResult(result)
	if len(result.fragments) != 2 {
		t.Fatalf("TUN fragment count = %d, want 2", len(result.fragments))
	}
	ipProtocol, sourceIp, destinationIp, transport, ok := parseIpv4(result.packet)
	if !ok || ipProtocol != ipProtocolNumberUdp {
		t.Fatalf("reassembled TUN packet parse = (%d, %t), want UDP", ipProtocol, ok)
	}
	var udp parsedUdp
	if !parseUdpPacket(sourceIp, destinationIp, transport, &udp) {
		t.Fatal("reassembled TUN UDP packet did not parse")
	}
	if udp.destinationPort != 443 || !bytes.Equal(udp.payload, payload) {
		t.Fatalf("reassembled TUN UDP destination=%d payload=%d", udp.destinationPort, len(udp.payload))
	}
}

func TestRetainedIpv4FragmentsNormalizeGVisorIdentityBeforeAsyncRouting(t *testing.T) {
	source := SourceId(NewId())
	transferKey := TransferKey{}
	var sender ipv4FragmentGate
	defer sender.close()

	buildGroup := func(marker byte) ipv4FragmentProcessResult {
		t.Helper()
		payload := fragmentTestPayload(1200)
		payload[0] = marker
		complete := craftSecurityPacket(
			IpProtocolUdp,
			net.IPv4(10, 0, 0, 9),
			42000,
			net.IPv4(93, 184, 216, 34),
			443,
			false,
			payload,
		)
		fragments, err := fragmentIpv4Packet(MessagePoolCopy(complete), DefaultMtu)
		if err != nil {
			t.Fatal(err)
		}
		var result ipv4FragmentProcessResult
		for _, fragment := range fragments {
			// Match gVisor's product-TUN form: every locally fragmented
			// datagram reuses ID zero and retains DF on every member.
			binary.BigEndian.PutUint16(fragment[4:6], 0)
			flagsAndOffset := binary.BigEndian.Uint16(fragment[6:8]) | 0x4000
			binary.BigEndian.PutUint16(fragment[6:8], flagsAndOffset)
			writeIpv4HeaderChecksum(fragment[:Ipv4HeaderSizeWithoutExtensions])
			result = sender.processOwned(
				source,
				transferKey,
				protocol.ProvideMode_Network,
				fragment,
			)
		}
		if result.packet == nil || len(result.fragments) != 2 {
			returnIpv4FragmentProcessResult(result)
			t.Fatalf("gVisor group did not complete: %+v", result)
		}
		return result
	}

	first := buildGroup(0x41)
	defer returnIpv4FragmentProcessResult(first)
	second := buildGroup(0x42)
	defer returnIpv4FragmentProcessResult(second)
	firstIdentification := binary.BigEndian.Uint16(first.fragments[0][4:6])
	secondIdentification := binary.BigEndian.Uint16(second.fragments[0][4:6])
	if firstIdentification == 0 || secondIdentification == 0 ||
		firstIdentification == secondIdentification {
		t.Fatalf(
			"normalized fragment identities = (%d, %d)",
			firstIdentification,
			secondIdentification,
		)
	}
	for groupIndex, group := range []ipv4FragmentProcessResult{first, second} {
		for fragmentIndex, fragment := range group.fragments {
			flagsAndOffset := binary.BigEndian.Uint16(fragment[6:8])
			if flagsAndOffset&0x4000 != 0 {
				t.Fatalf("group %d fragment %d retained DF: %#x", groupIndex, fragmentIndex, flagsAndOffset)
			}
			if got := checksumFinish(checksumAdd(0, fragment[:Ipv4HeaderSizeWithoutExtensions])); got != 0 {
				t.Fatalf("group %d fragment %d checksum residual = %#x", groupIndex, fragmentIndex, got)
			}
		}
	}

	// Parallel H3 lanes may deliver the first fragment of both datagrams
	// before either tail. Distinct canonical identities must let both finish.
	var receiver ipv4FragmentGate
	defer receiver.close()
	interleaved := [][]byte{
		first.fragments[0],
		second.fragments[0],
		first.fragments[1],
		second.fragments[1],
	}
	completedMarkers := map[byte]bool{}
	for _, fragment := range interleaved {
		result := receiver.processOwned(
			source,
			transferKey,
			protocol.ProvideMode_Network,
			MessagePoolCopy(fragment),
		)
		if result.packet != nil {
			ipProtocol, sourceIp, destinationIp, transport, ok := parseIpv4(result.packet)
			var udp parsedUdp
			if !ok || ipProtocol != ipProtocolNumberUdp ||
				!parseUdpPacket(sourceIp, destinationIp, transport, &udp) || len(udp.payload) == 0 {
				returnIpv4FragmentProcessResult(result)
				t.Fatal("interleaved normalized datagram did not parse")
			}
			completedMarkers[udp.payload[0]] = true
		}
		returnIpv4FragmentProcessResult(result)
	}
	if !completedMarkers[0x41] || !completedMarkers[0x42] || len(completedMarkers) != 2 {
		t.Fatalf("completed interleaved payload markers = %#v", completedMarkers)
	}
}

func TestProviderInspectsCompleteUdpBeforeForwardingOriginalIpv4Fragments(t *testing.T) {
	provider, _, localUserNat := newProviderTransferKeyTestFixture(t)
	payload := fragmentTestPayload(1200)
	complete := craftSecurityPacket(
		IpProtocolUdp,
		net.IPv4(10, 0, 0, 9),
		42000,
		net.IPv4(93, 184, 216, 34),
		443,
		false,
		payload,
	)
	fragments, err := fragmentIpv4Packet(MessagePoolCopy(complete), DefaultMtu)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, fragment := range fragments {
			MessagePoolReturn(fragment)
		}
	}()
	frames := make([]*protocol.Frame, len(fragments))
	for i, fragment := range fragments {
		// Match the DF+fragment form emitted by the product gVisor TUN.
		flagsAndOffset := binary.BigEndian.Uint16(fragment[6:8]) | 0x4000
		binary.BigEndian.PutUint16(fragment[6:8], flagsAndOffset)
		writeIpv4HeaderChecksum(fragment[:Ipv4HeaderSizeWithoutExtensions])
		frame, frameErr := ipPacketToProviderFrame(fragment, DefaultProtocolVersion)
		if frameErr != nil {
			t.Fatal(frameErr)
		}
		frames[i] = frame
	}
	source := SourceId(NewId())
	transferKey := TransferKey{ForceStream: true}
	provider.ClientReceive(source, frames, Peer{
		ProvideMode: protocol.ProvideMode_Public,
		TransferKey: transferKey,
	})

	var queued *SendPacket
	select {
	case queued = <-localUserNat.sendPackets:
	case <-time.After(5 * time.Second):
		t.Fatal("provider did not forward the inspected fragment group")
	}
	defer queued.finish()
	if queued.source != source.LocalMask() || queued.transferKey != transferKey {
		t.Fatalf("queued fragment identity source=%v key=%+v", queued.source, queued.transferKey)
	}
	if len(queued.packets) != len(fragments) {
		t.Fatalf("queued fragment count=%d want=%d", len(queued.packets), len(fragments))
	}
	for i, packet := range queued.packets {
		if !isIpv4FragmentPacket(packet) || DefaultMtu < len(packet) {
			t.Fatalf("queued member %d is not an MTU-sized IPv4 fragment: %d bytes", i, len(packet))
		}
	}

	reassembler := newIpv4FragmentReassembler()
	defer reassembler.close()
	var reassembled []byte
	for _, packet := range queued.packets {
		reassembled = reassembler.process(
			queued.source,
			queued.transferKey,
			queued.provideMode,
			packet,
		)
	}
	queued.packets = nil
	if reassembled == nil {
		t.Fatal("provider-forwarded fragments did not reconstruct")
	}
	defer MessagePoolReturn(reassembled)
	_, sourceIp, destinationIp, transport, ok := parseIpv4(reassembled)
	var udp parsedUdp
	if !ok || !parseUdpPacket(sourceIp, destinationIp, transport, &udp) ||
		udp.destinationPort != 443 || !bytes.Equal(udp.payload, payload) {
		t.Fatal("provider changed the policy-inspected UDP datagram")
	}
	allowKey := SecurityDestination{Version: 4, Protocol: IpProtocolUdp, Ip: "", Port: 443}
	if provider.SecurityPolicyStats(false)[SecurityPolicyResultAllow][allowKey] == 0 {
		t.Fatal("provider policy did not inspect the completed UDP datagram")
	}
}

func TestClientBlocksFragmentedUdpOnlyAfterCompletePolicyInspection(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	remote := &RemoteUserNatClient{
		securityPolicy: DefaultSecurityPolicy(ctx),
		provideMode:    protocol.ProvideMode_Network,
	}
	defer remote.egressIpv4Fragments.close()
	complete := craftSecurityPacket(
		IpProtocolUdp,
		net.IPv4(10, 0, 0, 9),
		42000,
		net.IPv4(127, 0, 0, 1),
		443,
		false,
		fragmentTestPayload(1200),
	)
	fragments, err := fragmentIpv4Packet(MessagePoolCopy(complete), DefaultMtu)
	if err != nil {
		t.Fatal(err)
	}
	source := SourceId(NewId())
	for i, fragment := range fragments {
		if !remote.SendPacket(source, protocol.ProvideMode_Public, fragment, 0) {
			t.Fatalf("fragment %d ownership was not consumed", i)
		}
		fragments[i] = nil
	}
	incidentKey := SecurityDestination{Version: 4, Protocol: IpProtocolUdp, Ip: "", Port: 443}
	stats := remote.SecurityPolicyStats(false)
	if stats[SecurityPolicyResultIncident][incidentKey] == 0 {
		t.Fatalf("client policy did not reject the completed private-destination UDP datagram: %v", stats)
	}
}

func TestProviderBlocksFragmentedUdpOnlyAfterCompletePolicyInspection(t *testing.T) {
	provider, _, localUserNat := newProviderTransferKeyTestFixture(t)
	complete := craftSecurityPacket(
		IpProtocolUdp,
		net.IPv4(10, 0, 0, 9),
		42000,
		net.IPv4(127, 0, 0, 1),
		443,
		false,
		fragmentTestPayload(1200),
	)
	fragments, err := fragmentIpv4Packet(MessagePoolCopy(complete), DefaultMtu)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, fragment := range fragments {
			MessagePoolReturn(fragment)
		}
	}()
	frames := make([]*protocol.Frame, len(fragments))
	for i, fragment := range fragments {
		frame, frameErr := ipPacketToProviderFrame(fragment, DefaultProtocolVersion)
		if frameErr != nil {
			t.Fatal(frameErr)
		}
		frames[i] = frame
	}
	provider.ClientReceive(SourceId(NewId()), frames, Peer{ProvideMode: protocol.ProvideMode_Public})
	if len(localUserNat.sendPackets) != 0 {
		t.Fatal("provider forwarded a fragmented private-destination UDP datagram")
	}
	stats := provider.PacketStats()
	if stats.BlockIngressPacketCount != int64(len(fragments)) {
		t.Fatalf("provider blocked fragment count=%d want=%d", stats.BlockIngressPacketCount, len(fragments))
	}
}

func TestFragmentedTcpCannotBypassSmtpRoutingOrProviderPolicy(t *testing.T) {
	complete := craftSecurityPacket(
		IpProtocolTcp,
		net.IPv4(10, 0, 0, 9),
		42000,
		net.IPv4(93, 184, 216, 34),
		25,
		false,
		fragmentTestPayload(1200),
	)
	if complete == nil {
		t.Fatal("craft fragmented SMTP packet")
	}

	t.Run("client", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		localUserNat := &LocalUserNat{
			ctx:         ctx,
			sendPackets: make(chan *SendPacket, 1),
		}
		remote := &RemoteUserNatClient{
			securityPolicy: DefaultSecurityPolicy(ctx),
			provideMode:    protocol.ProvideMode_Network,
			localUserNat:   localUserNat,
		}
		defer remote.egressIpv4Fragments.close()

		fragments, err := fragmentIpv4Packet(MessagePoolCopy(complete), DefaultMtu)
		if err != nil {
			t.Fatal(err)
		}
		source := SourceId(NewId())
		for i, fragment := range fragments {
			if !remote.SendPacket(source, protocol.ProvideMode_Network, fragment, 0) {
				t.Fatalf("fragment %d ownership was not consumed", i)
			}
			fragments[i] = nil
		}
		if len(localUserNat.sendPackets) != 0 {
			t.Fatal("fragmented TCP/25 reached the local SMTP exception")
		}
		remote.egressIpv4Fragments.mutex.Lock()
		defer remote.egressIpv4Fragments.mutex.Unlock()
		if remote.egressIpv4Fragments.reassembler4 == nil ||
			len(remote.egressIpv4Fragments.reassembler4.datagrams) != 0 ||
			remote.egressIpv4Fragments.reassembler4.retainedByteCount != 0 {
			t.Fatal("rejected fragmented TCP retained reassembly state")
		}
	})

	t.Run("multi-client", func(t *testing.T) {
		remote := &RemoteUserNatMultiClient{}
		defer remote.egressIpv4Fragments.close()
		fragments, err := fragmentIpv4Packet(MessagePoolCopy(complete), DefaultMtu)
		if err != nil {
			t.Fatal(err)
		}
		source := SourceId(NewId())
		for i, fragment := range fragments {
			if !remote.SendPacket(source, protocol.ProvideMode_Network, fragment, 0) {
				t.Fatalf("fragment %d ownership was not consumed", i)
			}
			fragments[i] = nil
		}
		remote.egressIpv4Fragments.mutex.Lock()
		defer remote.egressIpv4Fragments.mutex.Unlock()
		if remote.egressIpv4Fragments.reassembler4 == nil ||
			len(remote.egressIpv4Fragments.reassembler4.datagrams) != 0 ||
			remote.egressIpv4Fragments.reassembler4.retainedByteCount != 0 {
			t.Fatal("multi-client retained rejected fragmented TCP state")
		}
	})

	t.Run("provider", func(t *testing.T) {
		provider, _, localUserNat := newProviderTransferKeyTestFixture(t)
		fragments, err := fragmentIpv4Packet(MessagePoolCopy(complete), DefaultMtu)
		if err != nil {
			t.Fatal(err)
		}
		defer func() {
			for _, fragment := range fragments {
				MessagePoolReturn(fragment)
			}
		}()
		frames := make([]*protocol.Frame, len(fragments))
		for i, fragment := range fragments {
			frame, frameErr := ipPacketToProviderFrame(fragment, DefaultProtocolVersion)
			if frameErr != nil {
				t.Fatal(frameErr)
			}
			frames[i] = frame
		}
		provider.ClientReceive(
			SourceId(NewId()),
			frames,
			Peer{ProvideMode: protocol.ProvideMode_Public},
		)
		if len(localUserNat.sendPackets) != 0 {
			t.Fatal("provider forwarded fragmented TCP/25")
		}
	})
}

func newFragmentTestStream(ipVersion int) *StreamState {
	if ipVersion == 6 {
		return &StreamState{
			ipVersion:       6,
			sourceIp:        net.ParseIP("2001:db8::2").To16(),
			sourcePort:      42000,
			destinationIp:   net.ParseIP("2001:db8::1").To16(),
			destinationPort: 443,
		}
	}
	return &StreamState{
		ipVersion:       4,
		sourceIp:        net.IPv4(10, 0, 0, 2).To4(),
		sourcePort:      42000,
		destinationIp:   net.IPv4(198, 51, 100, 7).To4(),
		destinationPort: 443,
	}
}

func fragmentTestPayload(byteCount int) []byte {
	payload := make([]byte, byteCount)
	for i := range payload {
		payload[i] = byte(i*31 + 17)
	}
	return payload
}

func fragmentTestPackets(t *testing.T, payload []byte) [][]byte {
	t.Helper()
	packets, err := newFragmentTestStream(4).DataPackets(payload, len(payload), DefaultMtu)
	if err != nil {
		t.Fatalf("fragment UDP payload: %v", err)
	}
	t.Cleanup(func() {
		for _, packet := range packets {
			MessagePoolReturn(packet)
		}
	})
	return packets
}

func TestIpv4UdpFragmentationReassemblyAtProductMtu(t *testing.T) {
	payload := fragmentTestPayload(1400)
	fragments := fragmentTestPackets(t, payload)
	if len(fragments) != 2 {
		t.Fatalf("fragment count = %d, want 2", len(fragments))
	}

	identification := binary.BigEndian.Uint16(fragments[0][4:6])
	if identification == 0 {
		t.Fatal("fragment identification is zero")
	}
	for i, fragment := range fragments {
		if DefaultMtu < len(fragment) {
			t.Fatalf("fragment %d length = %d, MTU = %d", i, len(fragment), DefaultMtu)
		}
		if got := binary.BigEndian.Uint16(fragment[4:6]); got != identification {
			t.Fatalf("fragment %d identification = %d, want %d", i, got, identification)
		}
		if got := checksumFinish(checksumAdd(0, fragment[:Ipv4HeaderSizeWithoutExtensions])); got != 0 {
			t.Fatalf("fragment %d IPv4 checksum residual = %#x", i, got)
		}
	}
	if flags := binary.BigEndian.Uint16(fragments[0][6:8]); flags&0x2000 == 0 || flags&0x1fff != 0 {
		t.Fatalf("first fragment flags/offset = %#x, want MF at offset zero", flags)
	}
	firstPayloadByteCount := len(fragments[0]) - Ipv4HeaderSizeWithoutExtensions
	if flags := binary.BigEndian.Uint16(fragments[1][6:8]); flags&0x2000 != 0 || int(flags&0x1fff)*8 != firstPayloadByteCount {
		t.Fatalf("final fragment flags/offset = %#x, want offset %d", flags, firstPayloadByteCount)
	}
	if sendShard(fragments[0], 257) != sendShard(fragments[1], 257) {
		t.Fatal("fragments of one IPv4 datagram map to different NAT send shards")
	}

	reassembler := newIpv4FragmentReassembler()
	defer reassembler.close()
	source := SourceId(NewId())
	transferKey := TransferKey{ForceStream: true}
	// Reassembly must tolerate normal IP reordering.
	if packet := reassembler.process(
		source,
		transferKey,
		protocol.ProvideMode_Public,
		MessagePoolCopy(fragments[1]),
	); packet != nil {
		MessagePoolReturn(packet)
		t.Fatal("final fragment completed the datagram without offset zero")
	}
	reassembled := reassembler.process(
		source,
		transferKey,
		protocol.ProvideMode_Public,
		MessagePoolCopy(fragments[0]),
	)
	if reassembled == nil {
		t.Fatal("complete fragment set did not produce a packet")
	}
	defer MessagePoolReturn(reassembled)

	ipProtocol, sourceIp, destinationIp, transport, ok := parseIpv4(reassembled)
	if !ok || ipProtocol != ipProtocolNumberUdp {
		t.Fatalf("reassembled IPv4 parse = (%d, %t), want UDP", ipProtocol, ok)
	}
	if got := checksumFinish(checksumAdd(0, reassembled[:Ipv4HeaderSizeWithoutExtensions])); got != 0 {
		t.Fatalf("reassembled IPv4 checksum residual = %#x", got)
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

func TestIpv4FragmentReassemblyRejectsOverlapAndExpiresIncompleteState(t *testing.T) {
	fragments := fragmentTestPackets(t, fragmentTestPayload(1400))
	reassembler := newIpv4FragmentReassembler()
	defer reassembler.close()
	source := SourceId(NewId())
	base := time.Unix(1_700_000_000, 0)

	if packet := reassembler.processAt(
		source,
		TransferKey{},
		protocol.ProvideMode_Network,
		MessagePoolCopy(fragments[0]),
		base,
	); packet != nil {
		MessagePoolReturn(packet)
		t.Fatal("incomplete first fragment unexpectedly produced a packet")
	}
	duplicate := MessagePoolCopy(fragments[0])
	if packet := reassembler.processAt(
		source,
		TransferKey{},
		protocol.ProvideMode_Network,
		duplicate,
		base.Add(ipv4FragmentReassemblyTimeout-time.Second),
	); packet != nil {
		MessagePoolReturn(packet)
		t.Fatal("exact duplicate unexpectedly produced a packet")
	}
	if len(reassembler.datagrams) != 1 {
		t.Fatalf("duplicate changed pending datagram count to %d", len(reassembler.datagrams))
	}
	// An exact duplicate near expiry must not extend the hard lifetime.
	reassembler.expire(base.Add(ipv4FragmentReassemblyTimeout))
	if len(reassembler.datagrams) != 0 || reassembler.retainedByteCount != 0 {
		t.Fatalf("duplicate extended fragment lifetime: %d datagrams / %d bytes", len(reassembler.datagrams), reassembler.retainedByteCount)
	}
	if packet := reassembler.processAt(
		source,
		TransferKey{},
		protocol.ProvideMode_Network,
		MessagePoolCopy(fragments[0]),
		base.Add(ipv4FragmentReassemblyTimeout+time.Second),
	); packet != nil {
		MessagePoolReturn(packet)
		t.Fatal("incomplete first fragment unexpectedly produced a packet")
	}

	overlap := MessagePoolCopy(fragments[1])
	// Eight bytes into the first fragment is a conflicting overlap.
	binary.BigEndian.PutUint16(overlap[6:8], 1)
	writeIpv4HeaderChecksum(overlap[:Ipv4HeaderSizeWithoutExtensions])
	if packet := reassembler.processAt(
		source,
		TransferKey{},
		protocol.ProvideMode_Network,
		overlap,
		base.Add(ipv4FragmentReassemblyTimeout+2*time.Second),
	); packet != nil {
		MessagePoolReturn(packet)
		t.Fatal("overlapping fragment unexpectedly produced a packet")
	}
	if len(reassembler.datagrams) != 0 || reassembler.retainedByteCount != 0 {
		t.Fatalf("overlap retained %d datagrams / %d bytes", len(reassembler.datagrams), reassembler.retainedByteCount)
	}

	if packet := reassembler.processAt(
		source,
		TransferKey{},
		protocol.ProvideMode_Network,
		MessagePoolCopy(fragments[0]),
		base.Add(ipv4FragmentReassemblyTimeout+3*time.Second),
	); packet != nil {
		MessagePoolReturn(packet)
		t.Fatal("incomplete first fragment unexpectedly produced a packet")
	}
	reassembler.expire(base.Add(3*time.Second + 2*ipv4FragmentReassemblyTimeout))
	if len(reassembler.datagrams) != 0 || reassembler.retainedByteCount != 0 {
		t.Fatalf("expired reassembly retained %d datagrams / %d bytes", len(reassembler.datagrams), reassembler.retainedByteCount)
	}
}

func TestIpv4FragmentReassemblyRejectsFinalBeforeRetainedTail(t *testing.T) {
	fragments := fragmentTestPackets(t, fragmentTestPayload(1400))
	reassembler := newIpv4FragmentReassembler()
	defer reassembler.close()
	source := SourceId(NewId())

	retainedTail := MessagePoolCopy(fragments[0])
	// Move a non-final fragment beyond the legitimate final length while
	// keeping the same IPv4 reassembly identity.
	binary.BigEndian.PutUint16(retainedTail[6:8], 0x2000|200)
	writeIpv4HeaderChecksum(retainedTail[:Ipv4HeaderSizeWithoutExtensions])
	for _, packet := range [][]byte{
		retainedTail,
		MessagePoolCopy(fragments[0]),
		MessagePoolCopy(fragments[1]),
	} {
		if completed := reassembler.process(
			source,
			TransferKey{},
			protocol.ProvideMode_Public,
			packet,
		); completed != nil {
			MessagePoolReturn(completed)
			t.Fatal("fragment set with data beyond its final boundary completed")
		}
	}
	if len(reassembler.datagrams) != 0 || reassembler.retainedByteCount != 0 {
		t.Fatalf("invalid final boundary retained %d datagrams / %d bytes", len(reassembler.datagrams), reassembler.retainedByteCount)
	}
}

func TestIpv4FragmentReassemblyIsByteAndDatagramBounded(t *testing.T) {
	reassembler := newIpv4FragmentReassembler()
	defer reassembler.close()
	source := SourceId(NewId())
	base := time.Unix(1_700_000_000, 0)

	for i := 0; i < ipv4FragmentReassemblyMaxDatagrams+4; i++ {
		fragments := fragmentTestPackets(t, fragmentTestPayload(1400+i))
		if packet := reassembler.processAt(
			source,
			TransferKey{},
			protocol.ProvideMode_Public,
			MessagePoolCopy(fragments[0]),
			base.Add(time.Duration(i)*time.Millisecond),
		); packet != nil {
			MessagePoolReturn(packet)
			t.Fatal("incomplete first fragment unexpectedly produced a packet")
		}
		if ipv4FragmentReassemblyMaxDatagrams < len(reassembler.datagrams) {
			t.Fatalf("pending datagrams = %d, limit = %d", len(reassembler.datagrams), ipv4FragmentReassemblyMaxDatagrams)
		}
		if ipv4FragmentReassemblyMaxRetainedBytes < reassembler.retainedByteCount {
			t.Fatalf("retained bytes = %d, limit = %d", reassembler.retainedByteCount, ipv4FragmentReassemblyMaxRetainedBytes)
		}
	}
	if len(reassembler.datagrams) != ipv4FragmentReassemblyMaxDatagrams {
		t.Fatalf("pending datagrams = %d, want bounded window of %d", len(reassembler.datagrams), ipv4FragmentReassemblyMaxDatagrams)
	}
	reassembler.close()
	if len(reassembler.datagrams) != 0 || reassembler.retainedByteCount != 0 {
		t.Fatalf("close retained %d datagrams / %d bytes", len(reassembler.datagrams), reassembler.retainedByteCount)
	}
}

func TestUdpDefaultsReceiveQuicInitialWithoutChangingPacketMtu(t *testing.T) {
	settings := DefaultUdpBufferSettings()
	if settings.Mtu != 1100 {
		t.Fatalf("UDP packet MTU = %d, want product MTU 1100", settings.Mtu)
	}
	if settings.ReadBufferByteCount < 1200 {
		t.Fatalf("UDP read buffer = %d, cannot receive a QUIC Initial", settings.ReadBufferByteCount)
	}
}
