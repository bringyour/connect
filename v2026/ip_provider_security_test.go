package connect

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/layers"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestRemoteUserNatProviderEnforcesReversedPolicy pins the provider (exit) role's
// security enforcement: a RemoteUserNatProvider egresses a REMOTE client's traffic,
// so on its ingress (the outbound packet received from the tunnel) it must run the
// client-egress DPI via the reversed policy. A remote client that tunnels a
// BitTorrent handshake through the provider must be blocked (incident, reported as
// abuse) and never egressed; ordinary web traffic must pass.
//
// This is the provider-side counterpart to the device/mux tests (which cover the
// client-egress role). It drives the provider's ClientReceive directly with crafted
// IpPacketToProvider frames and asserts on the provider's own security stats.
func TestRemoteUserNatProviderEnforcesReversedPolicy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientSettings := DefaultClientSettings()
	clientSettings.SendBufferSettings.SequenceBufferSize = 0
	clientSettings.SendBufferSettings.AckBufferSize = 0
	clientSettings.ReceiveBufferSettings.SequenceBufferSize = 0
	clientSettings.ForwardBufferSettings.SequenceBufferSize = 0
	providerClient := NewClient(ctx, NewId(), NewNoContractClientOob(), clientSettings)
	defer providerClient.Cancel()

	// the exit-side LocalUserNat. Nothing should egress for a blocked flow; a
	// receive callback lets us confirm blocked traffic never reaches egress.
	localUserNat := NewLocalUserNatWithDefaults(ctx, "test-exit")
	egressed := make(chan *IpPath, 16)
	localUserNat.AddReceivePacketCallback(func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		select {
		case egressed <- ipPath:
		default:
		}
	})

	provider := NewRemoteUserNatProvider(providerClient, localUserNat, DefaultRemoteUserNatProviderSettings())
	defer provider.Close()

	source := SourceId(NewId())
	dstIP := net.ParseIP("203.0.113.7") // TEST-NET-3: public unicast, unrouteable

	toProviderFrame := func(proto IpProtocol, srcPort int, dstPort int, syn bool, payload []byte) *protocol.Frame {
		packet := craftSecurityPacket(proto, net.ParseIP("10.0.0.9"), srcPort, dstIP, dstPort, syn, payload)
		frame, err := ToFrame(&protocol.IpPacketToProvider{
			IpPacket: &protocol.IpPacket{PacketBytes: packet},
		}, DefaultProtocolVersion)
		if err != nil {
			t.Fatalf("to frame: %v", err)
		}
		return frame
	}

	// a remote client tunnels a BitTorrent handshake through the provider ->
	// the reversed policy runs the client-egress DPI and reports an incident
	provider.ClientReceive(source, []*protocol.Frame{
		toProviderFrame(IpProtocolTcp, 42000, 51413, false, bittorrentHandshakePacketPayload()),
	}, Peer{ProvideMode: protocol.ProvideMode_Public})

	// an ordinary plaintext HTTP request must pass the provider policy
	provider.ClientReceive(source, []*protocol.Frame{
		toProviderFrame(IpProtocolTcp, 42001, 8080, false, []byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n")),
	}, Peer{ProvideMode: protocol.ProvideMode_Public})

	stats := provider.SecurityPolicyStats(false)
	A, I := SecurityPolicyResultAllow, SecurityPolicyResultIncident

	incidentKey := SecurityDestination{Version: 4, Protocol: IpProtocolTcp, Ip: "", Port: 51413}
	if stats[I][incidentKey] == 0 {
		t.Errorf("provider did not flag the tunneled BitTorrent handshake as an incident (reversed client-egress DPI not enforced): stats=%v", stats)
	}
	allowKey := SecurityDestination{Version: 4, Protocol: IpProtocolTcp, Ip: "", Port: 8080}
	if stats[A][allowKey] == 0 {
		t.Errorf("provider blocked ordinary plaintext HTTP (dst 8080): stats=%v", stats)
	}

	// the blocked BitTorrent flow must never have egressed; only the allowed flow
	// (dst 8080) may reach the exit LocalUserNat
	deadline := time.After(2 * time.Second)
	for {
		select {
		case ipPath := <-egressed:
			if ipPath.DestinationPort == 51413 {
				t.Errorf("blocked BitTorrent flow egressed to the exit (dst 51413) despite the incident verdict")
			}
		case <-deadline:
			return
		}
	}
}

// bittorrentHandshakePacketPayload is the 68-byte BEP 3 handshake.
func bittorrentHandshakePacketPayload() []byte {
	b := make([]byte, 68)
	copy(b, bittorrentHandshakePrefix)
	return b
}

// craftSecurityPacket builds a valid IPv4 TCP/UDP packet with the given 5-tuple and
// payload (checksums + lengths filled), for feeding the provider's ClientReceive.
func craftSecurityPacket(proto IpProtocol, srcIP net.IP, srcPort int, dstIP net.IP, dstPort int, syn bool, payload []byte) []byte {
	ip := &layers.IPv4{
		Version: 4,
		TTL:     64,
		SrcIP:   srcIP.To4(),
		DstIP:   dstIP.To4(),
	}
	var transport gopacket.SerializableLayer
	switch proto {
	case IpProtocolTcp:
		ip.Protocol = layers.IPProtocolTCP
		tcp := &layers.TCP{
			SrcPort: layers.TCPPort(srcPort),
			DstPort: layers.TCPPort(dstPort),
			SYN:     syn,
			Seq:     1,
			Window:  65535,
		}
		tcp.SetNetworkLayerForChecksum(ip)
		transport = tcp
	case IpProtocolUdp:
		ip.Protocol = layers.IPProtocolUDP
		udp := &layers.UDP{
			SrcPort: layers.UDPPort(srcPort),
			DstPort: layers.UDPPort(dstPort),
		}
		udp.SetNetworkLayerForChecksum(ip)
		transport = udp
	default:
		return nil
	}
	buf := gopacket.NewSerializeBuffer()
	if err := gopacket.SerializeLayers(
		buf,
		gopacket.SerializeOptions{ComputeChecksums: true, FixLengths: true},
		ip, transport, gopacket.Payload(payload),
	); err != nil {
		return nil
	}
	return append([]byte(nil), buf.Bytes()...)
}
