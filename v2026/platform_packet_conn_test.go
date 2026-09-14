package connect

import (
	"net"
	"syscall"
	"testing"
)

type packetBufferSpy struct {
	net.PacketConn
	readBufferByteCount  int
	writeBufferByteCount int
}

func (self *packetBufferSpy) SetReadBuffer(byteCount int) error {
	self.readBufferByteCount = byteCount
	return nil
}

func (self *packetBufferSpy) SetWriteBuffer(byteCount int) error {
	self.writeBufferByteCount = byteCount
	return nil
}

func TestPlatformPacketConnClampsQuicSocketRequests(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		packetConn, err := net.ListenPacket(testUdpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
		if err != nil {
			t.Fatal(err)
		}
		defer packetConn.Close()
		spy := &packetBufferSpy{PacketConn: packetConn}
		capped := capPlatformPacketConn(spy, 256, 128)
		readSetter := capped.(interface{ SetReadBuffer(int) error })
		writeSetter := capped.(interface{ SetWriteBuffer(int) error })
		if err := readSetter.SetReadBuffer(7 * 1024 * 1024); err != nil {
			t.Fatal(err)
		}
		if err := writeSetter.SetWriteBuffer(7 * 1024 * 1024); err != nil {
			t.Fatal(err)
		}
		if spy.readBufferByteCount != 256 || spy.writeBufferByteCount != 128 {
			t.Fatalf(
				"socket buffers = (%d, %d), want capped (256, 128)",
				spy.readBufferByteCount,
				spy.writeBufferByteCount,
			)
		}
	})
}

// The SDK desktop DeviceLocal target is intentionally repeated here instead
// of importing the parent SDK package into Connect. This pins the exact cap
// which quic-go's fixed 7 MiB request must not override in the simulator.
func TestPlatformPacketConnClampsQuicRequestToDeviceMemoryTarget(t *testing.T) {
	const deviceMemoryTargetByteCount = ByteCount(20 * 1024 * 1024)
	const quicSocketBufferRequestByteCount = 7 * 1024 * 1024

	settings := DefaultPlatformTransportSettingsWithMemoryTarget(
		deviceMemoryTargetByteCount,
	)
	wantBufferByteCount := kib(320)
	if settings.H3SocketReadBufferByteCount != wantBufferByteCount ||
		settings.H3SocketWriteBufferByteCount != wantBufferByteCount {
		t.Fatalf(
			"device-target socket caps = (%d, %d), want (%d, %d)",
			settings.H3SocketReadBufferByteCount,
			settings.H3SocketWriteBufferByteCount,
			wantBufferByteCount,
			wantBufferByteCount,
		)
	}

	spy := &packetBufferSpy{}
	capped := capPlatformPacketConn(
		spy,
		settings.H3SocketReadBufferByteCount,
		settings.H3SocketWriteBufferByteCount,
	)
	readSetter := capped.(interface{ SetReadBuffer(int) error })
	writeSetter := capped.(interface{ SetWriteBuffer(int) error })
	if err := readSetter.SetReadBuffer(quicSocketBufferRequestByteCount); err != nil {
		t.Fatal(err)
	}
	if err := writeSetter.SetWriteBuffer(quicSocketBufferRequestByteCount); err != nil {
		t.Fatal(err)
	}
	if spy.readBufferByteCount != int(wantBufferByteCount) ||
		spy.writeBufferByteCount != int(wantBufferByteCount) {
		t.Fatalf(
			"quic-go socket requests resolved to (%d, %d), want capped (%d, %d)",
			spy.readBufferByteCount,
			spy.writeBufferByteCount,
			wantBufferByteCount,
			wantBufferByteCount,
		)
	}
}

func TestPlatformPacketConnPreservesUDPFastPath(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		udpConn, err := net.ListenUDP(testUdpNetwork(ipVersion), &net.UDPAddr{IP: testUnspecifiedIp(ipVersion)})
		if err != nil {
			t.Fatal(err)
		}
		defer udpConn.Close()

		packetConn := capPlatformPacketConn(udpConn, 256, 128)
		capped, ok := packetConn.(*cappedUDPConn)
		if !ok {
			t.Fatalf("UDP cap wrapper = %T, want *cappedUDPConn", packetConn)
		}
		if capped.readBufferByteCount != 256 || capped.writeBufferByteCount != 128 {
			t.Fatalf(
				"UDP limits = (%d, %d), want (256, 128)",
				capped.readBufferByteCount,
				capped.writeBufferByteCount,
			)
		}
		if _, ok := packetConn.(interface {
			SyscallConn() (syscall.RawConn, error)
			SetReadBuffer(int) error
			ReadMsgUDP([]byte, []byte) (int, int, int, *net.UDPAddr, error)
			WriteMsgUDP([]byte, []byte, *net.UDPAddr) (int, int, error)
		}); !ok {
			t.Fatalf("UDP cap wrapper %T lost QUIC OOB/ECN methods", packetConn)
		}
	})
}
