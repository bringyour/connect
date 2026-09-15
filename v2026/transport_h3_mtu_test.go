package connect

import (
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestH3SingleDatagramTunnelMtuSizing pins the exact worst-field wire growth
// from one raw tunnel packet to the encrypted Transfer frame selected by the
// bounded H3 carrier. Keep this beside the carrier so the global TUN MTU is
// never changed from IP/UDP/QUIC overhead alone.
func TestH3SingleDatagramTunnelMtuSizing(t *testing.T) {
	if DefaultMtu != 1100 {
		t.Fatalf("global tunnel MTU=%d want=1100", DefaultMtu)
	}
	path := TransferPath{
		SourceId:      NewId(),
		DestinationId: NewId(),
	}
	settings := DefaultH3DatagramSettings()
	wrappedByteCount := func(packetByteCounts ...int) int {
		frames := make([]*protocol.Frame, 0, len(packetByteCounts))
		for _, packetByteCount := range packetByteCounts {
			packet := make([]byte, packetByteCount)
			packet[0] = 0x45
			frames = append(frames, &protocol.Frame{
				MessageType:  protocol.MessageType_IpIpPacketToProvider,
				MessageBytes: packet,
				Raw:          true,
			})
		}
		plain := marshalSendPackTransferFrame(&sendPackFrame{
			path:           path,
			messageId:      NewId(),
			sequenceId:     NewId(),
			sequenceNumber: ^uint64(0),
			head:           true,
			frames:         frames,
			tagSendTime:    ^uint64(0),
		})
		ciphertextByteCount := sequenceTlsAeadNonceSize + len(plain) + 16
		wrapped := sizeEncryptedOuterTransferFrame(
			path,
			ciphertextByteCount,
			protocol.SequenceRole_SequenceRoleServer,
		)
		MessagePoolReturn(plain)
		return wrapped
	}
	fragmentPayloadByteCount := H3InitialDatagramByteCount -
		H3DatagramHeaderByteCount
	maximumSinglePacketDatagramMtu := 0
	maximumTwoPacketDatagramMtu := 0
	for candidateMtu := 576; candidateMtu <= DefaultMtu; candidateMtu += 1 {
		if wrappedByteCount(candidateMtu) <= fragmentPayloadByteCount {
			maximumSinglePacketDatagramMtu = candidateMtu
		}
		firstPacketByteCount := candidateMtu / 2
		if wrappedByteCount(
			firstPacketByteCount,
			candidateMtu-firstPacketByteCount,
		) <= fragmentPayloadByteCount {
			maximumTwoPacketDatagramMtu = candidateMtu
		}
	}
	if maximumSinglePacketDatagramMtu != 944 {
		t.Fatalf(
			"maximum single-packet one-DATAGRAM tunnel MTU=%d want=944",
			maximumSinglePacketDatagramMtu,
		)
	}
	if maximumTwoPacketDatagramMtu != 934 {
		t.Fatalf(
			"maximum two-packet one-DATAGRAM tunnel MTU=%d want=934",
			maximumTwoPacketDatagramMtu,
		)
	}
	t.Logf(
		"maximum one-DATAGRAM tunnel MTU single=%d two-packet=%d payload_limit=%d wrapped_bytes=%d/%d",
		maximumSinglePacketDatagramMtu,
		maximumTwoPacketDatagramMtu,
		fragmentPayloadByteCount,
		wrappedByteCount(maximumSinglePacketDatagramMtu),
		wrappedByteCount(
			maximumTwoPacketDatagramMtu/2,
			maximumTwoPacketDatagramMtu-maximumTwoPacketDatagramMtu/2,
		),
	)

	for name, packetByteCounts := range map[string][]int{
		"one full packet":       {DefaultMtu},
		"two coalesced packets": {DefaultMtu / 2, DefaultMtu / 2},
	} {
		wrapped := wrappedByteCount(packetByteCounts...)
		if !settings.UseDatagram(wrapped) {
			t.Fatalf(
				"%s encrypted Transfer bytes=%d exceeds hybrid limit=%d",
				name,
				wrapped,
				settings.HybridDatagramMessageByteCount,
			)
		}
		if settings.UseDatagramForPath(wrapped, H3InitialDatagramByteCount) {
			t.Fatalf(
				"%s encrypted Transfer bytes=%d fragmented on bounded initial packet lane=%d",
				name,
				wrapped,
				H3InitialDatagramByteCount,
			)
		}
		fragmentCount := (wrapped + fragmentPayloadByteCount - 1) /
			fragmentPayloadByteCount
		if fragmentCount != 2 {
			t.Fatalf(
				"%s encrypted Transfer bytes=%d uses %d initial DATAGRAMs want=2",
				name,
				wrapped,
				fragmentCount,
			)
		}
	}

	legacyWrapped := wrappedByteCount(1440)
	if H3DatagramHeaderByteCount+legacyWrapped <= settings.TargetDatagramByteCount {
		t.Fatalf(
			"legacy 1440-byte MTU unexpectedly fits the optimistic one-DATAGRAM target: transfer=%d target=%d",
			legacyWrapped,
			settings.TargetDatagramByteCount,
		)
	}
	if settings.UseDatagram(legacyWrapped) {
		t.Fatalf("legacy packet should fall back to the hybrid stream")
	}
}
