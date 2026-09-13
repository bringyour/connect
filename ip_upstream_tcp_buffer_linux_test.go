//go:build linux

package connect

import (
	"bytes"
	"context"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"
)

// The provider terminates TCP and reads the origin through an upstream socket
// that is already connected when it is configured. On Linux, setting SO_RCVBUF
// on that socket locks receive autotuning, and autotuning is the only thing
// that raises the window clamp chosen at SYN time from the default buffer. The
// advertised window to the origin then stays at about 64 KB for the life of the
// flow, capping one download near window/RTT (~210 Mb/s at a 2 ms origin RTT)
// no matter how fast the tunnel is. The upstream socket must leave the receive
// buffer to the kernel.
//
// Linux only: macOS rejects an oversized SO_RCVBUF with ENOBUFS and keeps
// autotuning, so there the explicit set is a silent no-op, not a lock.
func TestUpstreamTcpConnLeavesReceiveBufferToAutotuning(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			close(accepted)
			return
		}
		accepted <- conn
	}()

	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if peer, ok := <-accepted; ok {
		defer peer.Close()
	}
	tcpConn := conn.(*net.TCPConn)

	before := tcpSocketReceiveBufferSize(t, tcpConn)
	configureUpstreamTcpConn(tcpConn)
	after := tcpSocketReceiveBufferSize(t, tcpConn)

	if after != before {
		t.Fatalf(
			"upstream socket receive buffer changed from %d to %d after connect; an explicit SO_RCVBUF locks autotuning and freezes the window clamp at its SYN-time size",
			before,
			after,
		)
	}
}

// The behavior the fix exists for, observed where it happens: an upstream socket
// dialed and configured exactly as TcpSequence.Run does must let the kernel grow
// its receive buffer while it reads. The configure-only test above cannot catch
// a fix that moves the explicit SO_RCVBUF instead of removing it, for example
// into the dialer's Control before connect: that also locks autotuning and also
// pins the window, and this test fails on it, as it does on the original code.
//
// Autotuning growth is visible through SO_RCVBUF because tcp_rcv_space_adjust
// raises sk_rcvbuf; with an explicit SO_RCVBUF the kernel sets
// SOCK_RCVBUF_LOCK and never changes it again.
func TestUpstreamTcpConnReceiveBufferGrowsThroughDialPath(t *testing.T) {
	if moderate, err := os.ReadFile("/proc/sys/net/ipv4/tcp_moderate_rcvbuf"); err != nil ||
		!bytes.HasPrefix(bytes.TrimSpace(moderate), []byte("1")) {
		t.Skip("receive autotuning (net.ipv4.tcp_moderate_rcvbuf) is disabled on this host")
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	const transferByteCount = 256 * 1024 * 1024
	served := make(chan error, 1)
	go func() {
		peer, err := listener.Accept()
		if err != nil {
			served <- err
			return
		}
		defer peer.Close()
		chunk := make([]byte, 64*1024)
		for sent := 0; sent < transferByteCount; sent += len(chunk) {
			if _, err := peer.Write(chunk); err != nil {
				served <- err
				return
			}
		}
		served <- nil
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	tcpBufferSettings := DefaultTcpBufferSettings()
	socket, err := tcpBufferSettings.DialContext(ctx, "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	tcpConn, ok := socket.(*net.TCPConn)
	if !ok {
		t.Fatalf("upstream dial returned %T, not a TCP connection", socket)
	}
	configureUpstreamTcpConn(tcpConn)

	before := tcpSocketReceiveBufferSize(t, tcpConn)
	if err := tcpConn.SetReadDeadline(time.Now().Add(60 * time.Second)); err != nil {
		t.Fatal(err)
	}
	received, err := io.Copy(io.Discard, tcpConn)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-served; err != nil {
		t.Fatal(err)
	}
	if received != transferByteCount {
		t.Fatalf("received %d of %d bytes", received, transferByteCount)
	}
	after := tcpSocketReceiveBufferSize(t, tcpConn)

	if after <= before {
		t.Fatalf(
			"upstream socket receive buffer stayed at %d (was %d) across a %d MiB transfer; receive autotuning is locked, so the window advertised to the origin cannot grow",
			after,
			before,
			transferByteCount/(1024*1024),
		)
	}
}

func tcpSocketReceiveBufferSize(t *testing.T, tcpConn *net.TCPConn) int {
	t.Helper()
	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var size int
	var sockoptErr error
	if err := rawConn.Control(func(fd uintptr) {
		size, sockoptErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	}); err != nil {
		t.Fatal(err)
	}
	if sockoptErr != nil {
		t.Fatal(sockoptErr)
	}
	return size
}
