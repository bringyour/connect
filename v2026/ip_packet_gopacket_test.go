package connect

import (
	"bytes"
	"encoding/binary"
	"math"
	"net"
	"testing"

	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/layers"
)

// differential tests for the hand-rolled packet builders against gopacket
// references that replicate the historical gopacket-based implementations.
// gopacket is a test-only dependency: its ~1 MiB of static layer-registry
// init maps must stay out of the shipped binaries.

func TestSynAckMatchesGopacketReference(t *testing.T) {
	const mtu = 1440

	synAckReference := func(state *ConnectionState) []byte {
		headerSize := 0
		var ip gopacket.NetworkLayer
		switch state.ipVersion {
		case 4:
			ip = &layers.IPv4{
				Version:  4,
				TTL:      64,
				SrcIP:    state.destinationIp,
				DstIP:    state.sourceIp,
				Protocol: layers.IPProtocolTCP,
			}
			headerSize += Ipv4HeaderSizeWithoutExtensions
		case 6:
			ip = &layers.IPv6{
				Version:    6,
				HopLimit:   64,
				SrcIP:      state.destinationIp,
				DstIP:      state.sourceIp,
				NextHeader: layers.IPProtocolTCP,
			}
			headerSize += Ipv6HeaderSize
		}

		opts := []layers.TCPOption{}
		mssBytes := make([]byte, 2)
		binary.BigEndian.PutUint16(mssBytes, uint16(mtu-headerSize-TcpHeaderSizeWithoutExtensions))
		opts = append(opts, layers.TCPOption{
			OptionType:   layers.TCPOptionKindMSS,
			OptionLength: 4,
			OptionData:   mssBytes,
		})
		if state.enableWindowScale {
			windowScaleBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(windowScaleBytes[0:4], state.windowScale)
			opts = append(opts, layers.TCPOption{
				OptionType:   layers.TCPOptionKindWindowScale,
				OptionLength: 3,
				OptionData:   windowScaleBytes[3:4],
			})
		}

		tcp := layers.TCP{
			SrcPort: layers.TCPPort(state.destinationPort),
			DstPort: layers.TCPPort(state.sourcePort),
			Seq:     state.receiveSeq,
			Ack:     state.sendSeq,
			ACK:     true,
			SYN:     true,
			Window:  uint16(min(state.windowSize, uint32(math.MaxUint16))),
			Options: opts,
		}
		tcp.SetNetworkLayerForChecksum(ip)

		buffer := gopacket.NewSerializeBuffer()
		err := gopacket.SerializeLayers(buffer, gopacket.SerializeOptions{ComputeChecksums: true, FixLengths: true},
			ip.(gopacket.SerializableLayer),
			&tcp,
		)
		AssertEqual(t, err, nil)
		reference := make([]byte, len(buffer.Bytes()))
		copy(reference, buffer.Bytes())
		return reference
	}

	type synAckCase struct {
		ipVersion         int
		sourceIp          net.IP
		destinationIp     net.IP
		windowSize        uint32
		windowScale       uint32
		enableWindowScale bool
	}
	cases := []synAckCase{
		{4, net.IP{72, 0, 0, 1}, net.IP{72, 2, 3, 4}, 4096, 0, false},
		{4, net.IP{72, 0, 0, 1}, net.IP{72, 2, 3, 4}, 262144, 3, true},
		{6, net.ParseIP("2001:db8::1").To16(), net.ParseIP("2001:db8::2").To16(), 4096, 0, false},
		{6, net.ParseIP("2001:db8::1").To16(), net.ParseIP("2001:db8::2").To16(), 262144, 3, true},
	}
	for _, c := range cases {
		connectionState := &ConnectionState{
			ipVersion:         c.ipVersion,
			sourceIp:          c.sourceIp,
			sourcePort:        40000,
			destinationIp:     c.destinationIp,
			destinationPort:   443,
			receiveSeq:        0x01020304,
			sendSeq:           0x0a0b0c0d,
			windowSize:        c.windowSize,
			windowScale:       c.windowScale,
			enableWindowScale: c.enableWindowScale,
		}
		packet, err := connectionState.SynAck(mtu)
		AssertEqual(t, err, nil)
		reference := synAckReference(connectionState)
		if !bytes.Equal(reference, packet) {
			t.Fatalf("syn ack mismatch (v%d ws=%v):\n  packet    %x\n  reference %x", c.ipVersion, c.enableWindowScale, packet, reference)
		}
		MessagePoolReturn(packet)
	}
}

func TestOosPacketsMatchGopacketReference(t *testing.T) {
	oosReference := func(ipPath *IpPath, transport gopacket.SerializableLayer, payload []byte) []byte {
		var ip gopacket.NetworkLayer
		var ipProtocol layers.IPProtocol
		switch ipPath.Protocol {
		case IpProtocolUdp:
			ipProtocol = layers.IPProtocolUDP
		case IpProtocolTcp:
			ipProtocol = layers.IPProtocolTCP
		}
		switch ipPath.Version {
		case 4:
			ip = &layers.IPv4{
				Version:  4,
				TTL:      64,
				SrcIP:    ipPath.SourceIp.To4(),
				DstIP:    ipPath.DestinationIp.To4(),
				Protocol: ipProtocol,
			}
		case 6:
			ip = &layers.IPv6{
				Version:    6,
				HopLimit:   64,
				SrcIP:      ipPath.SourceIp.To16(),
				DstIP:      ipPath.DestinationIp.To16(),
				NextHeader: ipProtocol,
			}
		}
		switch v := transport.(type) {
		case *layers.TCP:
			v.SetNetworkLayerForChecksum(ip)
		case *layers.UDP:
			v.SetNetworkLayerForChecksum(ip)
		}
		buffer := gopacket.NewSerializeBuffer()
		serializable := []gopacket.SerializableLayer{
			ip.(gopacket.SerializableLayer),
			transport,
		}
		if payload != nil {
			serializable = append(serializable, gopacket.Payload(payload))
		}
		err := gopacket.SerializeLayers(buffer, gopacket.SerializeOptions{ComputeChecksums: true, FixLengths: true}, serializable...)
		AssertEqual(t, err, nil)
		reference := make([]byte, len(buffer.Bytes()))
		copy(reference, buffer.Bytes())
		return reference
	}

	payloadSizes := []int{0, 1, 3, 64, 1200}

	for _, ipVersion := range []int{4, 6} {
		var sourceIp net.IP
		var destinationIp net.IP
		switch ipVersion {
		case 4:
			sourceIp = net.IP{72, 0, 0, 1}
			destinationIp = net.IP{72, 2, 3, 4}
		case 6:
			sourceIp = net.ParseIP("2001:db8::1").To16()
			destinationIp = net.ParseIP("2001:db8::2").To16()
		}

		udpPath := &IpPath{
			Version:         ipVersion,
			Protocol:        IpProtocolUdp,
			SourceIp:        sourceIp,
			SourcePort:      40000,
			DestinationIp:   destinationIp,
			DestinationPort: 53,
		}
		for _, payloadSize := range payloadSizes {
			payload := testingEgressPayload(7, payloadSize)
			packet := ipOosUdpPacket(udpPath, payload)
			reference := oosReference(udpPath, &layers.UDP{
				SrcPort: layers.UDPPort(udpPath.SourcePort),
				DstPort: layers.UDPPort(udpPath.DestinationPort),
			}, payload)
			if !bytes.Equal(reference, packet) {
				t.Fatalf("oos udp mismatch (v%d n=%d):\n  packet    %x\n  reference %x", ipVersion, payloadSize, packet, reference)
			}
		}

		tcpPath := &IpPath{
			Version:         ipVersion,
			Protocol:        IpProtocolTcp,
			SourceIp:        sourceIp,
			SourcePort:      40000,
			DestinationIp:   destinationIp,
			DestinationPort: 443,
		}
		for _, payloadSize := range payloadSizes {
			payload := testingEgressPayload(9, payloadSize)
			packet := ipOosTcpPacket(tcpPath, 0, payload)
			reference := oosReference(tcpPath, &layers.TCP{
				SrcPort: layers.TCPPort(tcpPath.SourcePort),
				DstPort: layers.TCPPort(tcpPath.DestinationPort),
				Seq:     0,
				Ack:     0,
				Window:  4096,
			}, payload)
			if !bytes.Equal(reference, packet) {
				t.Fatalf("oos tcp mismatch (v%d n=%d):\n  packet    %x\n  reference %x", ipVersion, payloadSize, packet, reference)
			}
		}

		// the oos rst
		packet, ok := ipOosRst(tcpPath)
		AssertEqual(t, ok, true)
		reference := oosReference(tcpPath, &layers.TCP{
			SrcPort: layers.TCPPort(tcpPath.SourcePort),
			DstPort: layers.TCPPort(tcpPath.DestinationPort),
			Seq:     0,
			Ack:     0,
			RST:     true,
			Window:  4096,
		}, nil)
		if !bytes.Equal(reference, packet) {
			t.Fatalf("oos rst mismatch (v%d):\n  packet    %x\n  reference %x", ipVersion, packet, reference)
		}

		// udp is not resettable
		_, ok = ipOosRst(udpPath)
		AssertEqual(t, ok, false)
	}
}
