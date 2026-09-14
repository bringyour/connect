// These tests and benchmarks isolate provider-side TCP option negotiation from
// carrier performance, including packet ownership and asymmetric path limits.
package connect

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Builds a return packetizer with the same IPv4 and timestamp overhead as an
// established provider flow.
func newTcpOptionPacketizationState(peerMss uint32) *ConnectionState {
	return &ConnectionState{
		ipVersion:       4,
		sourceIp:        net.IPv4(10, 0, 0, 1).To4(),
		sourcePort:      40000,
		destinationIp:   net.IPv4(203, 0, 113, 7).To4(),
		destinationPort: 443,
		receiveSeq:      1000,
		sendSeq:         2000,
		windowSize:      1024 * 1024,
		windowScale:     5,
		enableTimestamp: true,
		timestampRecent: 3000,
		peerMss:         peerMss,
		timestampValueForTest: func() uint32 {
			return 4000
		},
	}
}

// A smaller source MTU produces a smaller advertised MSS. The provider must
// subtract its timestamp option and keep every return packet inside that MTU.
func TestTcpSequenceHonorsPeerMssInReturnPacketization(t *testing.T) {
	settings := DefaultTcpBufferSettingsWithBufferSize(8)
	sequence := newTcpSequenceWithTransferKey(
		t.Context(),
		func(
			source TransferPath,
			transferKey TransferKey,
			provideMode protocol.ProvideMode,
			recoveryMode receiveRecoveryMode,
			ipPath *IpPath,
			packet []byte,
		) {
		},
		TransferPath{},
		TransferKey{},
		protocol.ProvideMode_Public,
		4,
		net.IPv4(10, 0, 0, 1).To4(),
		40000,
		net.IPv4(203, 0, 113, 7).To4(),
		443,
		0,
		settings,
	)
	defer sequence.Close()
	sequence.timestampValueForTest = func() uint32 { return 4000 }

	const receiverMtu = 640
	const peerMss = receiverMtu - Ipv4HeaderSizeWithoutExtensions - TcpHeaderSizeWithoutExtensions
	synOptions := []byte{
		2, 4, 0, 0,
		1, 1, 8, 10,
		0, 0, 0, 1,
		0, 0, 0, 0,
	}
	binary.BigEndian.PutUint16(synOptions[2:4], peerMss)
	sequence.mutex.Lock()
	sequence.initializeSynWithLock(&parsedTcp{
		seq:        1000,
		windowSize: 65535,
		options:    synOptions,
	})
	sequence.mutex.Unlock()
	if sequence.peerMss != peerMss {
		t.Fatalf("peer MSS=%d, want %d", sequence.peerMss, peerMss)
	}

	payload := make([]byte, 8*1024)
	packets, err := sequence.DataPackets(payload, len(payload), settings.Mtu)
	if err != nil {
		t.Fatalf("packetize provider return: %v", err)
	}
	payloadByteCount := 0
	for packetIndex, packet := range packets {
		if receiverMtu < len(packet) {
			t.Errorf(
				"return packet %d is %d bytes, exceeds receiver MTU %d",
				packetIndex,
				len(packet),
				receiverMtu,
			)
		}
		_, _, _, transport, ok := parseIpv4(packet)
		if !ok {
			t.Fatalf("return packet %d is not IPv4", packetIndex)
		}
		tcp := &parsedTcp{}
		if !parseTcpPacket(sequence.destinationIp, sequence.sourceIp, transport, tcp) {
			t.Fatalf("return packet %d is not TCP", packetIndex)
		}
		payloadByteCount += len(tcp.payload)
		MessagePoolReturn(packet)
	}
	if payloadByteCount != len(payload) {
		t.Fatalf("packetized payload=%d bytes, want %d", payloadByteCount, len(payload))
	}
}

// A peer MSS equal to the local fixed-header limit must not reduce the normal
// packet size beyond the timestamp bytes the sender already has to include.
func TestTcpSequencePeerMssPreservesEqualMtuPacketization(t *testing.T) {
	const localMtu = 1440
	const peerMss = localMtu - Ipv4HeaderSizeWithoutExtensions - TcpHeaderSizeWithoutExtensions
	state := newTcpOptionPacketizationState(peerMss)
	packetPayloadByteCount := peerMss - tcpTimestampOptionByteCount
	payload := make([]byte, packetPayloadByteCount)
	packets, err := state.DataPackets(payload, len(payload), localMtu)
	if err != nil {
		t.Fatalf("packetize equal-MTU return: %v", err)
	}
	if len(packets) != 1 || len(packets[0]) != localMtu {
		t.Fatalf(
			"equal-MTU packets=(%d, %d bytes), want (1, %d bytes)",
			len(packets),
			len(packets[0]),
			localMtu,
		)
	}
	MessagePoolReturn(packets[0])
}

// Measures provider packetization and counts only packets a receiver with the
// advertised MTU can accept.
func benchmarkTcpReturnPacketization(b *testing.B, peerMss uint32, receiverMtu int) {
	const providerMtu = 1440
	const payloadByteCount = 64 * 1024
	state := newTcpOptionPacketizationState(peerMss)
	payload := make([]byte, payloadByteCount)
	acceptedPayloadByteCount := 0
	packetCount := 0
	droppedPacketCount := 0

	b.ResetTimer()
	for range b.N {
		packets, err := state.DataPackets(payload, len(payload), providerMtu)
		if err != nil {
			b.Fatal(err)
		}
		for _, packet := range packets {
			packetCount += 1
			if len(packet) <= receiverMtu {
				_, _, _, transport, ok := parseIpv4(packet)
				if !ok {
					b.Fatal("generated packet is not IPv4")
				}
				tcp := &parsedTcp{}
				if !parseTcpPacket(state.destinationIp, state.sourceIp, transport, tcp) {
					b.Fatal("generated packet is not TCP")
				}
				acceptedPayloadByteCount += len(tcp.payload)
			} else {
				droppedPacketCount += 1
			}
			MessagePoolReturn(packet)
		}
	}
	b.StopTimer()
	if b.N == 0 {
		return
	}
	b.SetBytes(int64(acceptedPayloadByteCount / b.N))
	b.ReportMetric(float64(packetCount)/float64(b.N), "packets/op")
	b.ReportMetric(float64(droppedPacketCount)/float64(b.N), "oversized-drops/op")
}

// Historical behavior ignores a smaller peer MSS.
func BenchmarkTcpReturnPacketizationAsymmetricLocalMtu(b *testing.B) {
	benchmarkTcpReturnPacketization(b, 0, 640)
}

// The candidate behavior honors a smaller peer MSS.
func BenchmarkTcpReturnPacketizationAsymmetricPeerMss(b *testing.B) {
	benchmarkTcpReturnPacketization(b, 600, 640)
}

// Historical behavior on the normal equal-MTU path.
func BenchmarkTcpReturnPacketizationEqualLocalMtu(b *testing.B) {
	benchmarkTcpReturnPacketization(b, 0, 1440)
}

// The candidate behavior on the normal equal-MTU path.
func BenchmarkTcpReturnPacketizationEqualPeerMss(b *testing.B) {
	benchmarkTcpReturnPacketization(b, 1400, 1440)
}
