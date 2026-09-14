package connect

import (
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

var ipFrameAllocationSink *protocol.Frame
var frameListAllocationSink []*protocol.Frame
var ipFrameBytesAllocationSink []byte

func TestIpPacketFromProviderFrameAvoidsWrapperAllocations(t *testing.T) {
	packet := make([]byte, 1400)

	wrappedAllocs := testing.AllocsPerRun(1000, func() {
		frame, err := ToFrame(&protocol.IpPacketFromProvider{
			IpPacket: &protocol.IpPacket{PacketBytes: packet},
		}, DefaultProtocolVersion)
		if err != nil {
			panic(err)
		}
		ipFrameAllocationSink = frame
	})
	directAllocs := testing.AllocsPerRun(1000, func() {
		frame, err := ipPacketFromProviderFrame(packet, DefaultProtocolVersion)
		if err != nil {
			panic(err)
		}
		ipFrameAllocationSink = frame
	})

	t.Logf("provider frame allocations: wrapped=%.0f direct=%.0f", wrappedAllocs, directAllocs)
	if wrappedAllocs <= directAllocs {
		t.Fatalf("direct raw-frame helper did not reduce allocations: wrapped=%.0f direct=%.0f",
			wrappedAllocs, directAllocs)
	}
}

func BenchmarkIpPacketFromProviderFrame(b *testing.B) {
	packet := make([]byte, 1400)
	b.Run("wrapped", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			frame, err := ToFrame(&protocol.IpPacketFromProvider{
				IpPacket: &protocol.IpPacket{PacketBytes: packet},
			}, DefaultProtocolVersion)
			if err != nil {
				b.Fatal(err)
			}
			ipFrameAllocationSink = frame
		}
	})
	b.Run("direct", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			frame, err := ipPacketFromProviderFrame(packet, DefaultProtocolVersion)
			if err != nil {
				b.Fatal(err)
			}
			ipFrameAllocationSink = frame
		}
	})
}

func TestRawIpPacketBytesAvoidWrapperAllocations(t *testing.T) {
	packet := make([]byte, 1400)
	toProvider := &protocol.Frame{
		MessageType:  protocol.MessageType_IpIpPacketToProvider,
		MessageBytes: packet,
		Raw:          true,
	}
	fromProvider := &protocol.Frame{
		MessageType:  protocol.MessageType_IpIpPacketFromProvider,
		MessageBytes: packet,
		Raw:          true,
	}

	toAllocs := testing.AllocsPerRun(1000, func() {
		b, err := ipPacketToProviderBytes(toProvider)
		if err != nil {
			panic(err)
		}
		ipFrameBytesAllocationSink = b
	})
	fromAllocs := testing.AllocsPerRun(1000, func() {
		b, err := ipPacketFromProviderBytes(fromProvider)
		if err != nil {
			panic(err)
		}
		ipFrameBytesAllocationSink = b
	})
	if toAllocs != 0 || fromAllocs != 0 {
		t.Fatalf("raw IP frame extraction allocated: to=%.0f from=%.0f", toAllocs, fromAllocs)
	}
}

func TestSendPackSingleFrameListAllocations(t *testing.T) {
	pack := &SendPack{Frame: &protocol.Frame{}}
	allocs := testing.AllocsPerRun(1000, func() {
		frameListAllocationSink = pack.frameList()
	})
	t.Logf("single-frame frameList allocations: %.0f", allocs)
	if allocs != 0 {
		t.Fatalf("single-frame frameList allocates %.0f times, want 0", allocs)
	}
}
