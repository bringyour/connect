package connect

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestRemoteUserNatProviderPacketStats pins the provider's relayed-traffic
// counters: traffic received from remote clients over the tunnel counts as
// remote ingress once handed to the exit local user nat, return traffic into
// the tunnel counts as remote egress once handed to the client send buffer,
// and traffic dropped by the provider security policy counts as blocked.
// Also exercises the epoch packet stats callback.
func TestRemoteUserNatProviderPacketStats(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	providerClient := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer providerClient.Cancel()

	localUserNat := NewLocalUserNatWithDefaults(ctx, "test-exit")

	providerSettings := DefaultRemoteUserNatProviderSettings()
	// bound the return-path send so a failed enqueue cannot stall the test
	providerSettings.WriteTimeout = 1 * time.Second
	providerSettings.EventEpoch = 50 * time.Millisecond
	returnSendObservations := make(chan RemoteUserNatProviderReturnSendObservation, 4)
	providerSettings.ReturnSendObserver = func(observation RemoteUserNatProviderReturnSendObservation) {
		returnSendObservations <- observation
	}
	provider := NewRemoteUserNatProvider(providerClient, localUserNat, providerSettings)
	defer provider.Close()
	returnSendResults := make(chan providerReturnSendResult, 2)
	provider.afterReturnSendForTest = func(result providerReturnSendResult) {
		returnSendResults <- result
	}

	packetStatsChannel := make(chan *PacketStats, 16)
	unsubPacketStats := provider.AddPacketStatsCallback(func(packetStats *PacketStats) {
		select {
		case packetStatsChannel <- packetStats:
		default:
		}
	})
	defer unsubPacketStats()

	source := SourceId(NewId())
	srcIP := net.ParseIP("10.0.0.9")
	dstIP := net.ParseIP("203.0.113.7") // TEST-NET-3: public unicast, unrouteable

	toProviderFrame := func(packet []byte) *protocol.Frame {
		frame, err := ToFrame(&protocol.IpPacketToProvider{
			IpPacket: &protocol.IpPacket{PacketBytes: packet},
		}, DefaultProtocolVersion)
		if err != nil {
			t.Fatalf("to frame: %v", err)
		}
		return frame
	}

	// an allowed outbound flow received from the tunnel is the provider's
	// ingress, counted when handed to the exit local user nat
	outboundPacket := craftSecurityPacket(IpProtocolTcp, srcIP, 42001, dstIP, 8080, false, []byte("GET / HTTP/1.1\r\nHost: example.com\r\n\r\n"))
	provider.ClientReceive(source, []*protocol.Frame{toProviderFrame(outboundPacket)}, Peer{ProvideMode: protocol.ProvideMode_Public})

	stats := provider.PacketStats()
	if stats.RemoteIngressPacketCount != 1 || stats.RemoteIngressByteCount != ByteCount(len(outboundPacket)) {
		t.Fatalf("unexpected ingress stats %+v", stats)
	}

	// the packet is a non-SYN with no tcp sequence, so the nat answers with
	// an orphan RST (PROXYDRAIN1.md §3.5) — the provider's first remote
	// egress. The sender completion hook follows its accounting update, so the
	// exact result replaces timing-based counter polling.
	rstResult := waitProviderReturnSendCompletion(t, returnSendResults)
	if !rstResult.sent || rstResult.packetCount != 1 || rstResult.packetByteCount <= 0 {
		t.Fatalf("unexpected orphan rst return result %+v", rstResult)
	}
	rstStarted := <-returnSendObservations
	rstObservation := <-returnSendObservations
	if rstStarted.Phase != RemoteUserNatProviderReturnSendPhaseStarted ||
		rstObservation.Phase != RemoteUserNatProviderReturnSendPhaseCompleted ||
		rstStarted.Token == 0 || rstObservation.Token != rstStarted.Token {
		t.Fatalf("orphan rst observation pair started=%+v completed=%+v", rstStarted, rstObservation)
	}
	if !rstObservation.Sent || rstObservation.PacketCount != rstResult.packetCount ||
		rstObservation.PacketByteCount != rstResult.packetByteCount {
		t.Fatalf("exported orphan rst observation=%+v result=%+v", rstObservation, rstResult)
	}
	rstByteCount := rstResult.packetByteCount
	stats = provider.PacketStats()
	if stats.RemoteEgressPacketCount != 1 || stats.RemoteEgressByteCount != rstByteCount {
		t.Fatalf("unexpected orphan rst egress stats %+v", stats)
	}

	// a tunneled BitTorrent handshake is dropped by the reversed policy DPI
	// and counts as blocked
	blockedPacket := craftSecurityPacket(IpProtocolTcp, srcIP, 42000, dstIP, 51413, false, bittorrentHandshakePacketPayload())
	provider.ClientReceive(source, []*protocol.Frame{toProviderFrame(blockedPacket)}, Peer{ProvideMode: protocol.ProvideMode_Public})

	stats = provider.PacketStats()
	if stats.BlockIngressPacketCount != 1 || stats.BlockIngressByteCount != ByteCount(len(blockedPacket)) {
		t.Fatalf("unexpected blocked stats %+v", stats)
	}
	if stats.RemoteIngressPacketCount != 1 || stats.BlockEgressPacketCount != 0 {
		t.Fatalf("the blocked packet must only count as block ingress %+v", stats)
	}

	// the return of the outbound flow into the tunnel is the provider's
	// egress, counted when handed to the client send buffer
	returnPacket := craftSecurityPacket(IpProtocolTcp, dstIP, 8080, srcIP, 42001, false, []byte("HTTP/1.1 200 OK\r\n\r\n"))
	returnIpPath, err := ParseIpPath(returnPacket)
	if err != nil {
		t.Fatalf("parse ip path: %v", err)
	}
	provider.Receive(source, protocol.ProvideMode_Public, returnIpPath, returnPacket)
	waitProviderReturnSendResult(
		t,
		returnSendResults,
		true,
		1,
		ByteCount(len(returnPacket)),
	)
	returnStarted := <-returnSendObservations
	returnObservation := <-returnSendObservations
	if returnStarted.Phase != RemoteUserNatProviderReturnSendPhaseStarted ||
		returnObservation.Phase != RemoteUserNatProviderReturnSendPhaseCompleted ||
		returnStarted.Token == 0 || returnObservation.Token != returnStarted.Token {
		t.Fatalf("return observation pair started=%+v completed=%+v", returnStarted, returnObservation)
	}
	if !returnObservation.Sent || returnObservation.PacketCount != 1 ||
		returnObservation.PacketByteCount != ByteCount(len(returnPacket)) {
		t.Fatalf("exported return observation=%+v", returnObservation)
	}
	stats = provider.PacketStats()
	if stats.RemoteEgressPacketCount != 2 ||
		stats.RemoteEgressByteCount != rstByteCount+ByteCount(len(returnPacket)) {
		t.Fatalf("unexpected egress stats %+v", stats)
	}

	// a return packet from a statically dropped source endpoint (a bittorrent
	// port) is blocked by the reversed policy's source check
	strayPacket := craftSecurityPacket(IpProtocolTcp, dstIP, 6881, srcIP, 42002, false, []byte("stray"))
	strayIpPath, err := ParseIpPath(strayPacket)
	if err != nil {
		t.Fatalf("parse ip path: %v", err)
	}
	provider.Receive(source, protocol.ProvideMode_Public, strayIpPath, strayPacket)

	stats = provider.PacketStats()
	if stats.BlockEgressPacketCount != 1 || stats.BlockEgressByteCount != ByteCount(len(strayPacket)) {
		t.Fatalf("unexpected stray return stats %+v", stats)
	}
	if stats.BlockIngressPacketCount != 1 || stats.RemoteEgressPacketCount != 2 {
		t.Fatalf("the stray return must only count as block egress %+v", stats)
	}

	// the epoch callback fires with the cumulative counts
	final := *stats
	deadline := time.After(5 * time.Second)
	for {
		select {
		case packetStats := <-packetStatsChannel:
			if packetStatsEqual(packetStats, &final) {
				return
			}
			// an earlier epoch's partial snapshot; keep waiting
		case <-deadline:
			t.Fatalf("expected the epoch packet stats callback to reach %+v", final)
		}
	}
}
