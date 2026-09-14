//go:build !js

// This file verifies the native WebRTC UDP queue's ownership, batching,
// backpressure, deadline, and failure behavior without relying on scheduler
// timing or a particular operating system's batch syscall.
package connect

import (
	"bytes"
	"context"
	"errors"
	"net"
	"net/netip"
	"os"
	"sync"
	"testing"
	"time"
)

// p2pUdpBatchTestWriter copies each observed batch before releasing the
// production queue's pooled owners.
type p2pUdpBatchTestWriter struct {
	calls   chan []p2pUdpQueuedWrite
	entered chan struct{}
	release chan struct{}
	err     error
	once    sync.Once
}

// writeBatch records exact values and can hold the kernel-write stage to make
// queue backpressure deterministic.
func (self *p2pUdpBatchTestWriter) writeBatch(
	writes []p2pUdpQueuedWrite,
) (int, error) {
	self.once.Do(func() {
		if self.entered != nil {
			close(self.entered)
		}
	})
	if self.release != nil {
		<-self.release
	}
	copiedWrites := make([]p2pUdpQueuedWrite, len(writes))
	for writeIndex, write := range writes {
		copiedWrites[writeIndex] = p2pUdpQueuedWrite{
			packet:  bytes.Clone(write.packet),
			address: write.address,
		}
	}
	if self.calls != nil {
		self.calls <- copiedWrites
	}
	if self.err != nil {
		return 0, self.err
	}
	return len(writes), nil
}

// newP2pUdpBatchTestConn constructs the production worker around a controlled
// writer and a real closeable UDP socket.
func newP2pUdpBatchTestConn(
	t *testing.T,
	ipVersion int,
	queueSize int,
	batchSize int,
	writer p2pUdpBatchWriter,
	start bool,
) *p2pUdpBatchConn {
	t.Helper()
	udpConnection, err := net.ListenUDP(testUdpNetwork(ipVersion), testLoopbackUdpAddr(ipVersion))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	connection := &p2pUdpBatchConn{
		UDPConn:      udpConnection,
		ctx:          ctx,
		cancel:       cancel,
		writes:       make(chan p2pUdpQueuedWrite, queueSize),
		batchSize:    batchSize,
		writer:       writer,
		spaceMonitor: NewMonitor(),
	}
	if start {
		connection.workerWaitGroup.Add(1)
		go HandleError(connection.run)
	}
	t.Cleanup(func() {
		connection.Close()
	})
	return connection
}

// startP2pUdpBatchTestConn starts a worker after a test has deliberately
// prefetched several queue entries.
func startP2pUdpBatchTestConn(connection *p2pUdpBatchConn) {
	connection.workerWaitGroup.Add(1)
	go HandleError(connection.run)
}

// TestP2pUdpBatchConnCopiesAndDrainsReadyBurst proves Write does not retain a
// caller buffer and one worker turn gathers every already-ready datagram.
func TestP2pUdpBatchConnCopiesAndDrainsReadyBurst(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		writes := make(chan []p2pUdpQueuedWrite, 1)
		writer := &p2pUdpBatchTestWriter{calls: writes}
		connection := newP2pUdpBatchTestConn(t, ipVersion, 8, 4, writer, false)
		destination := netip.MustParseAddrPort(testLoopbackHostPort(ipVersion, 32123))
		expected := make([][]byte, 4)
		for packetIndex := range len(expected) {
			packet := []byte{0x80, byte(packetIndex + 1), 0x42}
			expected[packetIndex] = bytes.Clone(packet)
			if byteCount, err := connection.WriteToAddrPort(
				packet,
				destination,
			); err != nil || byteCount != len(packet) {
				t.Fatalf("queue packet %d bytes=%d err=%v", packetIndex, byteCount, err)
			}
			clear(packet)
		}
		startP2pUdpBatchTestConn(connection)

		select {
		case batch := <-writes:
			if len(batch) != len(expected) {
				t.Fatalf("ready batch count=%d want=%d", len(batch), len(expected))
			}
			for packetIndex, write := range batch {
				if !bytes.Equal(write.packet, expected[packetIndex]) {
					t.Fatalf("packet %d changed: %x", packetIndex, write.packet)
				}
				if write.address != destination {
					t.Fatalf("packet %d address=%s", packetIndex, write.address)
				}
			}
		case <-time.After(time.Second):
			t.Fatal("ready UDP batch was not written")
		}
	})
}

// TestP2pUdpBatchConnLeavesControlWritesSynchronous verifies that DTLS/SCTP
// and STUN packets retain the legacy socket path instead of entering the
// media queue used by negotiated SRTP data.
func TestP2pUdpBatchConnLeavesControlWritesSynchronous(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		writes := make(chan []p2pUdpQueuedWrite, 1)
		connection := newP2pUdpBatchTestConn(
			t,
			ipVersion,
			8,
			4,
			&p2pUdpBatchTestWriter{calls: writes},
			true,
		)
		receiver, err := net.ListenUDP(testUdpNetwork(ipVersion), testLoopbackUdpAddr(ipVersion))
		if err != nil {
			t.Fatal(err)
		}
		defer receiver.Close()
		controlPacket := []byte{0x16, 0xfe, 0xfd, 1}
		destination := receiver.LocalAddr().(*net.UDPAddr).AddrPort()
		if _, err := connection.WriteToAddrPort(controlPacket, destination); err != nil {
			t.Fatal(err)
		}
		if err := receiver.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
		readPacket := make([]byte, 32)
		byteCount, _, err := receiver.ReadFromUDPAddrPort(readPacket)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(readPacket[:byteCount], controlPacket) {
			t.Fatalf("control packet changed: %x", readPacket[:byteCount])
		}
		select {
		case batch := <-writes:
			t.Fatalf("control packet entered media batch: %+v", batch)
		default:
		}
	})
}

// TestP2pUdpBatchConnHonorsQueueDeadline makes the writer and one queued item
// occupy all bounded capacity, then verifies the next enqueue times out.
func TestP2pUdpBatchConnHonorsQueueDeadline(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		entered := make(chan struct{})
		release := make(chan struct{})
		writer := &p2pUdpBatchTestWriter{
			entered: entered,
			release: release,
		}
		connection := newP2pUdpBatchTestConn(t, ipVersion, 1, 1, writer, true)
		destination := netip.MustParseAddrPort(testLoopbackHostPort(ipVersion, 32124))
		if _, err := connection.WriteToAddrPort([]byte{0x80, 1}, destination); err != nil {
			t.Fatal(err)
		}
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("UDP writer did not enter")
		}
		if _, err := connection.WriteToAddrPort([]byte{0x80, 2}, destination); err != nil {
			t.Fatal(err)
		}
		if err := connection.SetWriteDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		if _, err := connection.WriteToAddrPort([]byte{0x80, 3}, destination); !errors.Is(
			err,
			os.ErrDeadlineExceeded,
		) {
			t.Fatalf("full queue error=%v", err)
		}
		close(release)
	})
}

// TestP2pUdpBatchConnPublishesWriterFailure checks that an asynchronous socket
// error closes the queue and becomes the deterministic error of later writes.
func TestP2pUdpBatchConnPublishesWriterFailure(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		writeErr := errors.New("injected UDP write failure")
		writes := make(chan []p2pUdpQueuedWrite, 1)
		writer := &p2pUdpBatchTestWriter{
			calls: writes,
			err:   writeErr,
		}
		connection := newP2pUdpBatchTestConn(t, ipVersion, 1, 1, writer, true)
		destination := netip.MustParseAddrPort(testLoopbackHostPort(ipVersion, 32125))
		if _, err := connection.WriteToAddrPort([]byte{0x80, 1}, destination); err != nil {
			t.Fatal(err)
		}
		select {
		case <-writes:
		case <-time.After(time.Second):
			t.Fatal("failing UDP writer was not called")
		}
		select {
		case <-connection.ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("failing UDP writer did not close the queue")
		}
		if _, err := connection.WriteToAddrPort([]byte{0x80, 2}, destination); !errors.Is(
			err,
			writeErr,
		) {
			t.Fatalf("future write error=%v", err)
		}
	})
}
