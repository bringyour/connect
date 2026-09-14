//go:build linux && !js

// Linux sends a ready WebRTC UDP burst with sendmmsg.
package connect

import (
	"net"

	"github.com/pion/transport/v4"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// p2pUdpBatchWriterLinux retains reusable message and address storage.
type p2pUdpBatchWriterLinux struct {
	connection4 *ipv4.PacketConn
	connection6 *ipv6.PacketConn
	buffers     [][][]byte
	messages4   []ipv4.Message
	messages6   []ipv6.Message
	addresses   []net.UDPAddr
}

// newP2pUdpBatchWriter selects the address family once per socket.
func newP2pUdpBatchWriter(
	connection transport.UDPConn,
	network string,
	batchSize int,
) p2pUdpBatchWriter {
	udpConnection, ok := connection.(*net.UDPConn)
	if !ok {
		return &p2pUdpBatchWriterFallback{connection: connection}
	}
	writer := &p2pUdpBatchWriterLinux{
		buffers:   make([][][]byte, batchSize),
		addresses: make([]net.UDPAddr, batchSize),
	}
	for writeIndex := range batchSize {
		writer.buffers[writeIndex] = make([][]byte, 1)
	}
	if network == "udp6" {
		writer.connection6 = ipv6.NewPacketConn(udpConnection)
		writer.messages6 = make([]ipv6.Message, batchSize)
		for writeIndex := range batchSize {
			writer.messages6[writeIndex].Buffers = writer.buffers[writeIndex]
		}
	} else {
		writer.connection4 = ipv4.NewPacketConn(udpConnection)
		writer.messages4 = make([]ipv4.Message, batchSize)
		for writeIndex := range batchSize {
			writer.messages4[writeIndex].Buffers = writer.buffers[writeIndex]
		}
	}
	return writer
}

// writeBatch fills fixed iovecs and handles partial sendmmsg progress.
func (self *p2pUdpBatchWriterLinux) writeBatch(
	writes []p2pUdpQueuedWrite,
) (int, error) {
	for writeIndex, write := range writes {
		self.buffers[writeIndex][0] = write.packet
		address := write.address
		self.addresses[writeIndex] = *net.UDPAddrFromAddrPort(address)
		if self.connection6 != nil {
			self.messages6[writeIndex].Addr = &self.addresses[writeIndex]
		} else {
			self.messages4[writeIndex].Addr = &self.addresses[writeIndex]
		}
	}
	defer func() {
		for writeIndex := range len(writes) {
			self.buffers[writeIndex][0] = nil
			if self.connection6 != nil {
				self.messages6[writeIndex].Addr = nil
			} else {
				self.messages4[writeIndex].Addr = nil
			}
		}
	}()

	writtenCount := 0
	for writtenCount < len(writes) {
		var count int
		var err error
		if self.connection6 != nil {
			count, err = self.connection6.WriteBatch(
				self.messages6[writtenCount:len(writes)],
				0,
			)
		} else {
			count, err = self.connection4.WriteBatch(
				self.messages4[writtenCount:len(writes)],
				0,
			)
		}
		writtenCount += count
		if err != nil || count == 0 {
			return writtenCount, err
		}
	}
	return writtenCount, nil
}
