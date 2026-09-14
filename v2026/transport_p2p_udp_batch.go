//go:build !js

// This file batches the native WebRTC socket's UDP writes. The first queued
// datagram is written immediately; only datagrams already waiting are gathered,
// so an idle control packet never waits for a batching timer.
package connect

import (
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"os"
	"sync"
	"time"

	"github.com/pion/transport/v4"
)

// p2pUdpBatchNet preserves a selected Pion network implementation and wraps
// only the UDP sockets it creates.
type p2pUdpBatchNet struct {
	transport.Net
	queueSize int
	batchSize int
	// socketBufferByteCount, when positive, is requested as the kernel send and
	// receive buffer of every UDP socket (the kernel clamps it to its maximum).
	// The default ~200 KB Linux buffer overflows under fast-path bursts and
	// drops the peer's ACKs, which times out the sender's whole window.
	socketBufferByteCount int
}

// newP2pUdpBatchNet creates a route-neutral socket wrapper. A zero or negative
// bound disables the wrapper and returns the selected network unchanged.
func newP2pUdpBatchNet(
	selectedNet transport.Net,
	queueSize int,
	batchSize int,
	socketBufferByteCount int,
) transport.Net {
	if selectedNet == nil || queueSize <= 0 || batchSize <= 0 {
		return selectedNet
	}
	return &p2pUdpBatchNet{
		Net:                   selectedNet,
		queueSize:             queueSize,
		batchSize:             min(queueSize, batchSize),
		socketBufferByteCount: socketBufferByteCount,
	}
}

// applyP2pUdpSocketBuffers requests the configured kernel buffers on a socket
// that supports them. Failures are ignored: the kernel default still works,
// just with more loss under bursts.
func applyP2pUdpSocketBuffers(connection any, byteCount int) {
	if byteCount <= 0 {
		return
	}
	if setter, ok := connection.(interface{ SetReadBuffer(int) error }); ok {
		_ = setter.SetReadBuffer(byteCount)
	}
	if setter, ok := connection.(interface{ SetWriteBuffer(int) error }); ok {
		_ = setter.SetWriteBuffer(byteCount)
	}
}

// ListenUDP wraps an ICE UDP socket with the bounded write pipeline.
func (self *p2pUdpBatchNet) ListenUDP(
	network string,
	localAddress *net.UDPAddr,
) (transport.UDPConn, error) {
	connection, err := self.Net.ListenUDP(network, localAddress)
	if err != nil {
		return nil, err
	}
	applyP2pUdpSocketBuffers(connection, self.socketBufferByteCount)
	return newP2pUdpBatchConn(connection, network, self.queueSize, self.batchSize), nil
}

// ListenPacket covers Pion call sites that request the net.PacketConn shape.
func (self *p2pUdpBatchNet) ListenPacket(
	network string,
	address string,
) (net.PacketConn, error) {
	connection, err := self.Net.ListenPacket(network, address)
	if err != nil {
		return nil, err
	}
	udpConnection, ok := connection.(transport.UDPConn)
	if !ok {
		return connection, nil
	}
	applyP2pUdpSocketBuffers(udpConnection, self.socketBufferByteCount)
	return newP2pUdpBatchConn(
		udpConnection,
		network,
		self.queueSize,
		self.batchSize,
	), nil
}

// DialUDP applies the same write behavior to TURN or other connected UDP
// sockets created through Pion's network abstraction.
func (self *p2pUdpBatchNet) DialUDP(
	network string,
	localAddress *net.UDPAddr,
	remoteAddress *net.UDPAddr,
) (transport.UDPConn, error) {
	connection, err := self.Net.DialUDP(network, localAddress, remoteAddress)
	if err != nil {
		return nil, err
	}
	return newP2pUdpBatchConn(connection, network, self.queueSize, self.batchSize), nil
}

// One queued write owns a pooled copy and a value-form destination.
type p2pUdpQueuedWrite struct {
	packet  []byte
	address netip.AddrPort
}

// p2pUdpBatchWriter hides the platform-specific sendmmsg implementation.
type p2pUdpBatchWriter interface {
	writeBatch(writes []p2pUdpQueuedWrite) (int, error)
}

// p2pUdpBatchWriterFallback is used when a platform has no sendmmsg or a
// custom Pion network does not expose a concrete UDP socket.
type p2pUdpBatchWriterFallback struct {
	connection transport.UDPConn
}

// writeBatch completes every ready write or returns the first socket error.
func (self *p2pUdpBatchWriterFallback) writeBatch(
	writes []p2pUdpQueuedWrite,
) (int, error) {
	udpConnection, concrete := self.connection.(*net.UDPConn)
	for writeIndex, write := range writes {
		var byteCount int
		var err error
		if concrete {
			byteCount, err = udpConnection.WriteToUDPAddrPort(
				write.packet,
				write.address,
			)
		} else {
			byteCount, err = self.connection.WriteToUDP(
				write.packet,
				net.UDPAddrFromAddrPort(write.address),
			)
		}
		if err != nil {
			return writeIndex, err
		}
		if byteCount != len(write.packet) {
			return writeIndex, io.ErrShortWrite
		}
	}
	return len(writes), nil
}

// p2pUdpBatchConn adds a bounded userspace socket buffer in front of the
// kernel socket. It implements ICE's optional AddrPortReaderWriter interface
// so wrapping the socket does not reintroduce per-packet net.Addr allocations.
type p2pUdpBatchConn struct {
	transport.UDPConn
	ctx               context.Context
	cancel            context.CancelFunc
	writes            chan p2pUdpQueuedWrite
	batchSize         int
	writer            p2pUdpBatchWriter
	workerWaitGroup   sync.WaitGroup
	closeOnce         sync.Once
	stateLock         sync.Mutex
	closed            bool
	writeError        error
	spaceMonitor      *Monitor
	writeDeadlineLock sync.Mutex
	writeDeadline     time.Time
}

// newP2pUdpBatchConn starts one writer that preserves socket write order.
func newP2pUdpBatchConn(
	connection transport.UDPConn,
	network string,
	queueSize int,
	batchSize int,
) *p2pUdpBatchConn {
	ctx, cancel := context.WithCancel(context.Background())
	batchConnection := &p2pUdpBatchConn{
		UDPConn:      connection,
		ctx:          ctx,
		cancel:       cancel,
		writes:       make(chan p2pUdpQueuedWrite, queueSize),
		batchSize:    batchSize,
		writer:       newP2pUdpBatchWriter(connection, network, batchSize),
		spaceMonitor: NewMonitor(),
	}
	batchConnection.workerWaitGroup.Add(1)
	go HandleError(batchConnection.run)
	return batchConnection
}

// run immediately writes the first item, then drains only items already
// available. Every queued owner is returned even when the socket fails.
func (self *p2pUdpBatchConn) run() {
	defer self.workerWaitGroup.Done()
	defer func() {
		for {
			select {
			case write := <-self.writes:
				MessagePoolReturn(write.packet)
			default:
				return
			}
		}
	}()

	writes := make([]p2pUdpQueuedWrite, self.batchSize)
	for {
		var first p2pUdpQueuedWrite
		select {
		case <-self.ctx.Done():
			return
		case first = <-self.writes:
		}
		self.spaceMonitor.NotifyAll()
		writes[0] = first
		writeCount := 1
	drain:
		for writeCount < len(writes) {
			select {
			case write := <-self.writes:
				writes[writeCount] = write
				writeCount += 1
			default:
				break drain
			}
		}
		self.spaceMonitor.NotifyAll()

		writtenCount, err := self.writer.writeBatch(writes[:writeCount])
		for writeIndex := range writeCount {
			MessagePoolReturn(writes[writeIndex].packet)
			writes[writeIndex] = p2pUdpQueuedWrite{}
		}
		if err == nil && writtenCount != writeCount {
			err = errors.New("p2p UDP batch made partial progress")
		}
		if err != nil {
			self.stop(err)
			return
		}
	}
}

// stop atomically rejects future enqueues before closing the kernel socket.
func (self *p2pUdpBatchConn) stop(err error) error {
	self.stateLock.Lock()
	if self.closed {
		self.stateLock.Unlock()
		return nil
	}
	self.closed = true
	self.writeError = err
	self.cancel()
	self.stateLock.Unlock()
	self.spaceMonitor.NotifyAll()
	return self.UDPConn.Close()
}

// enqueue copies the caller-owned packet before reporting socket acceptance.
func (self *p2pUdpBatchConn) enqueue(
	packet []byte,
	address netip.AddrPort,
) (int, error) {
	if len(packet) == 0 || !address.IsValid() {
		return 0, errors.New("invalid p2p UDP write")
	}
	// RTP and RTCP have the version-two high bits. Keep STUN, DTLS/SCTP, and
	// TURN control on their original synchronous socket path; only the
	// negotiated SRTP media lane benefits from the added userspace pipeline.
	if packet[0]&0xc0 != 0x80 {
		return self.writeDirect(packet, address)
	}
	ownedPacket := MessagePoolCopy(packet)
	write := p2pUdpQueuedWrite{
		packet:  ownedPacket,
		address: address,
	}

	self.writeDeadlineLock.Lock()
	deadline := self.writeDeadline
	self.writeDeadlineLock.Unlock()
	var timer *time.Timer
	if !deadline.IsZero() {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			MessagePoolReturn(ownedPacket)
			return 0, os.ErrDeadlineExceeded
		}
		timer = time.NewTimer(remaining)
		defer timer.Stop()
	}
	for {
		spaceNotify := self.spaceMonitor.NotifyChannel()
		self.stateLock.Lock()
		if self.closed {
			err := self.writeError
			self.stateLock.Unlock()
			MessagePoolReturn(ownedPacket)
			if err == nil {
				err = net.ErrClosed
			}
			return 0, err
		}
		select {
		case self.writes <- write:
			self.stateLock.Unlock()
			return len(packet), nil
		default:
			self.stateLock.Unlock()
		}
		if timer == nil {
			select {
			case <-self.ctx.Done():
			case <-spaceNotify:
				continue
			}
		} else {
			select {
			case <-self.ctx.Done():
			case <-spaceNotify:
				continue
			case <-timer.C:
				MessagePoolReturn(ownedPacket)
				return 0, os.ErrDeadlineExceeded
			}
		}
	}
}

// writeDirect preserves the pre-fast-path socket semantics for every control
// and compatibility datagram.
func (self *p2pUdpBatchConn) writeDirect(
	packet []byte,
	address netip.AddrPort,
) (int, error) {
	if udpConnection, ok := self.UDPConn.(*net.UDPConn); ok {
		return udpConnection.WriteToUDPAddrPort(packet, address)
	}
	return self.UDPConn.WriteToUDP(packet, net.UDPAddrFromAddrPort(address))
}

// WriteToAddrPort is ICE's allocation-free send entry point.
func (self *p2pUdpBatchConn) WriteToAddrPort(
	packet []byte,
	address netip.AddrPort,
) (int, error) {
	return self.enqueue(packet, address)
}

// WriteToUDPAddrPort preserves the concrete UDP method used by newer Pion.
func (self *p2pUdpBatchConn) WriteToUDPAddrPort(
	packet []byte,
	address netip.AddrPort,
) (int, error) {
	return self.enqueue(packet, address)
}

// WriteTo adapts net.PacketConn callers to the value-form queue.
func (self *p2pUdpBatchConn) WriteTo(
	packet []byte,
	address net.Addr,
) (int, error) {
	udpAddress, ok := address.(*net.UDPAddr)
	if !ok || udpAddress == nil {
		return self.UDPConn.WriteTo(packet, address)
	}
	return self.enqueue(packet, udpAddress.AddrPort())
}

// WriteToUDP adapts the legacy UDP address entry point.
func (self *p2pUdpBatchConn) WriteToUDP(
	packet []byte,
	address *net.UDPAddr,
) (int, error) {
	if address == nil {
		return 0, errors.New("nil p2p UDP destination")
	}
	return self.enqueue(packet, address.AddrPort())
}

// Write queues to the connected UDP peer when one exists.
func (self *p2pUdpBatchConn) Write(packet []byte) (int, error) {
	remoteAddress, ok := self.RemoteAddr().(*net.UDPAddr)
	if !ok || remoteAddress == nil {
		return self.UDPConn.Write(packet)
	}
	return self.enqueue(packet, remoteAddress.AddrPort())
}

// ReadFromAddrPort preserves ICE's allocation-free receive entry point.
func (self *p2pUdpBatchConn) ReadFromAddrPort(
	packet []byte,
) (int, netip.AddrPort, error) {
	if connection, ok := self.UDPConn.(interface {
		ReadFromUDPAddrPort([]byte) (int, netip.AddrPort, error)
	}); ok {
		return connection.ReadFromUDPAddrPort(packet)
	}
	byteCount, address, err := self.UDPConn.ReadFromUDP(packet)
	if err != nil || address == nil {
		return byteCount, netip.AddrPort{}, err
	}
	return byteCount, address.AddrPort(), nil
}

// ReadFromUDPAddrPort preserves the concrete UDP receive method.
func (self *p2pUdpBatchConn) ReadFromUDPAddrPort(
	packet []byte,
) (int, netip.AddrPort, error) {
	return self.ReadFromAddrPort(packet)
}

// SetWriteDeadline applies to both enqueue backpressure and kernel writes.
func (self *p2pUdpBatchConn) SetWriteDeadline(deadline time.Time) error {
	self.writeDeadlineLock.Lock()
	self.writeDeadline = deadline
	self.writeDeadlineLock.Unlock()
	return self.UDPConn.SetWriteDeadline(deadline)
}

// SetDeadline updates the queue's write side and delegates both socket sides.
func (self *p2pUdpBatchConn) SetDeadline(deadline time.Time) error {
	self.writeDeadlineLock.Lock()
	self.writeDeadline = deadline
	self.writeDeadlineLock.Unlock()
	return self.UDPConn.SetDeadline(deadline)
}

// Close rejects future writes, closes the socket, and returns queued owners.
func (self *p2pUdpBatchConn) Close() error {
	var closeErr error
	self.closeOnce.Do(func() {
		closeErr = self.stop(net.ErrClosed)
		self.workerWaitGroup.Wait()
	})
	return closeErr
}
