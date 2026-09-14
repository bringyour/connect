package connect

import "net"

// cappedPacketConn prevents quic-go's process-wide 7 MiB socket-buffer target
// from multiplying by every Auto candidate. The UDP specialization preserves
// quic-go's OOB/ECN fast path; generic injected PacketConns retain their normal
// interface plus bounded Set*Buffer forwarding.
type cappedPacketConn struct {
	net.PacketConn
	readBufferByteCount  int
	writeBufferByteCount int
}

func (self *cappedPacketConn) SetReadBuffer(byteCount int) error {
	setter, ok := self.PacketConn.(interface{ SetReadBuffer(int) error })
	if !ok {
		return nil
	}
	return setter.SetReadBuffer(min(byteCount, self.readBufferByteCount))
}

func (self *cappedPacketConn) SetWriteBuffer(byteCount int) error {
	setter, ok := self.PacketConn.(interface{ SetWriteBuffer(int) error })
	if !ok {
		return nil
	}
	return setter.SetWriteBuffer(min(byteCount, self.writeBufferByteCount))
}

type cappedUDPConn struct {
	*net.UDPConn
	readBufferByteCount  int
	writeBufferByteCount int
}

func (self *cappedUDPConn) SetReadBuffer(byteCount int) error {
	return self.UDPConn.SetReadBuffer(min(byteCount, self.readBufferByteCount))
}

func (self *cappedUDPConn) SetWriteBuffer(byteCount int) error {
	return self.UDPConn.SetWriteBuffer(min(byteCount, self.writeBufferByteCount))
}

func capPlatformPacketConn(
	packetConn net.PacketConn,
	readBufferByteCount ByteCount,
	writeBufferByteCount ByteCount,
) net.PacketConn {
	if packetConn == nil || readBufferByteCount <= 0 || writeBufferByteCount <= 0 {
		return packetConn
	}
	readLimit := int(min(readBufferByteCount, ByteCount(maxInt())))
	writeLimit := int(min(writeBufferByteCount, ByteCount(maxInt())))
	if udpConn, ok := packetConn.(*net.UDPConn); ok {
		capped := &cappedUDPConn{
			UDPConn:              udpConn,
			readBufferByteCount:  readLimit,
			writeBufferByteCount: writeLimit,
		}
		_ = capped.SetReadBuffer(readLimit)
		_ = capped.SetWriteBuffer(writeLimit)
		return capped
	}
	capped := &cappedPacketConn{
		PacketConn:           packetConn,
		readBufferByteCount:  readLimit,
		writeBufferByteCount: writeLimit,
	}
	_ = capped.SetReadBuffer(readLimit)
	_ = capped.SetWriteBuffer(writeLimit)
	return capped
}

func maxInt() int {
	return int(^uint(0) >> 1)
}
