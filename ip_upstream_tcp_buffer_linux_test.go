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
// Structural concession, recorded rather than left blank: what the buffer does
// to a flow's *rate* has no in-process assertion. The invariant these rows can
// hold is a state — which buffer the socket has and whether the kernel may
// still change it — and that is decidable on one host in one run. The rate is
// window over round trip on a path, so it needs a path, a real origin and
// repetitions against a null band (§7); no arrangement of loopback and netem
// makes it an assertion a single run can decide, and this file's own
// measurements produced both signs on one kernel. The provider-upstream
// download and upload cells own it.
func TestUpstreamTcpConnLeavesReceiveBufferToAutotuning(t *testing.T) {
	tcpConn := dialUpstreamTestTcpConn(t)

	before := tcpSocketReceiveBufferSize(t, tcpConn)
	configureUpstreamTcpConn(tcpConn, int(DefaultTcpBufferSettings().MaxWindowSize), socketBufferPolicy{}, false)
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

// The send mirror. An upload through the provider is carried by the provider's
// writes to this same upstream socket, and on Linux SO_SNDBUF locks send
// autotuning exactly as SO_RCVBUF locks receive autotuning (tcp(7)). The locked
// value is clamped to net.core.wmem_max first, so the explicit call caps the
// bytes the upstream flow may hold unacknowledged at a number that has nothing
// to do with the path's bandwidth-delay product, and on a stock host is below
// where tcp_wmem's maximum would have taken it. There is no window clamp here,
// so this is a ceiling rather than the receive side's freeze; it went unnoticed
// because a download barely uses the direction it caps.
func TestUpstreamTcpSendBufferIsNotPinned(t *testing.T) {
	tcpConn := dialUpstreamTestTcpConn(t)

	before := tcpSocketSendBufferSize(t, tcpConn)
	configureUpstreamTcpConn(tcpConn, int(DefaultTcpBufferSettings().MaxWindowSize), socketBufferPolicy{}, false)
	after := tcpSocketSendBufferSize(t, tcpConn)

	if after != before {
		t.Fatalf(
			"upstream socket send buffer changed from %d to %d after connect; an explicit SO_SNDBUF locks send autotuning and caps upload in-flight bytes at a clamped constant",
			before,
			after,
		)
	}
}

// a connected loopback client socket, in the state the provider configures its
// upstream in; both ends live for the test
func dialUpstreamTestTcpConn(t *testing.T) *net.TCPConn {
	t.Helper()
	tcpConn, _ := dialUpstreamTestTcpConnWithPeer(t)
	return tcpConn
}

// the same socket with its peer, for the rows that have to move bytes over it
func dialUpstreamTestTcpConnWithPeer(t *testing.T) (*net.TCPConn, net.Conn) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
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
	t.Cleanup(func() { conn.Close() })
	peer, ok := <-accepted
	if !ok {
		t.Fatal("the upstream test peer did not accept")
	}
	t.Cleanup(func() { peer.Close() })
	return conn.(*net.TCPConn), peer
}

func tcpSocketReceiveBufferSize(t *testing.T, tcpConn *net.TCPConn) int {
	t.Helper()
	return tcpSocketBufferSize(t, tcpConn, syscall.SO_RCVBUF)
}

func tcpSocketSendBufferSize(t *testing.T, tcpConn *net.TCPConn) int {
	t.Helper()
	return tcpSocketBufferSize(t, tcpConn, syscall.SO_SNDBUF)
}

func tcpSocketBufferSize(t *testing.T, tcpConn *net.TCPConn, option int) int {
	t.Helper()
	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var size int
	var sockoptErr error
	if err := rawConn.Control(func(fd uintptr) {
		size, sockoptErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, option)
	}); err != nil {
		t.Fatal(err)
	}
	if sockoptErr != nil {
		t.Fatal(sockoptErr)
	}
	return size
}

// THROUGHPUTFIX §5 row 1, second half: the adopted row says the receive buffer
// was not replaced, this says autotuning still owns it. An explicit SO_RCVBUF
// sets SOCK_RCVBUF_LOCK and the kernel then refuses to grow the buffer for the
// life of the flow, so a socket that never moves under a bulk download is one
// whose autotuning was locked.
//
// The buffer, not the window clamp, is what separates the two trees. Which way
// the clamp goes after a lock is kernel-dependent: on 7.0.12-linuxkit it
// follows the pinned buffer up, so a clamp or rcv_ssthresh assertion passes on
// the pinned tree and decides nothing.
func TestUpstreamTcpReceiveBufferGrowsUnderLoad(t *testing.T) {
	receiveAutotuneMax := upstreamSysctlValues(t, "net/ipv4/tcp_rmem")[2]
	tcpConn, peer := dialUpstreamTestTcpConnWithPeer(t)
	// read before the setup, not after: a tree that pins the buffer has
	// already replaced the establishment value by then, and a pin above the
	// tcp_rmem maximum would make the skip below swallow the failure
	establishment := tcpSocketReceiveBufferSize(t, tcpConn)
	if receiveAutotuneMax <= establishment {
		t.Skipf(
			"the socket is established with a %d byte receive buffer against a net.ipv4.tcp_rmem maximum of %d, so there is nothing to grow into",
			establishment,
			receiveAutotuneMax,
		)
	}
	configureUpstreamTcpConn(tcpConn, int(DefaultTcpBufferSettings().MaxWindowSize), socketBufferPolicy{}, false)
	configured := tcpSocketReceiveBufferSize(t, tcpConn)

	go func() {
		block := make([]byte, 1024*1024)
		for {
			if _, err := peer.Write(block); err != nil {
				return
			}
		}
	}()

	// autotuning sizes the buffer from what the reader drains per round trip,
	// so the loop drains and stops at the first growth rather than reading a
	// fixed volume
	const maxReadByteCount = 512 * 1024 * 1024
	block := make([]byte, 1024*1024)
	receiveBufferSize := configured
	for readByteCount := 0; readByteCount < maxReadByteCount; readByteCount += len(block) {
		if _, err := io.ReadFull(tcpConn, block); err != nil {
			t.Fatal(err)
		}
		if receiveBufferSize = tcpSocketReceiveBufferSize(t, tcpConn); configured < receiveBufferSize {
			return
		}
	}

	t.Fatalf(
		"upstream socket receive buffer stayed at %d over %d bytes read, from a %d byte establishment value and against the net.ipv4.tcp_rmem maximum %d; an explicit SO_RCVBUF locks the buffer and autotuning never raises it again",
		receiveBufferSize,
		maxReadByteCount,
		establishment,
		receiveAutotuneMax,
	)
}

// THROUGHPUTFIX §9.4 U2. The row above is the value-unchanged proxy: it says
// the call was not made. This says what the call cost. An explicit SO_SNDBUF
// sets SOCK_SNDBUF_LOCK, and the kernel refuses to expand a locked send
// buffer, so the socket stops following the congestion window for the life of
// the flow at whatever the clamped request happened to be. Left alone the
// buffer grows to tcp_wmem's maximum, which is the ceiling an upload through
// the provider can fill; this drives a flow until it gets there.
func TestUpstreamTcpSendBufferGrowsUnderLoad(t *testing.T) {
	sendAutotuneMax := upstreamSysctlValues(t, "net/ipv4/tcp_wmem")[2]
	tcpConn, peer := dialUpstreamTestTcpConnWithPeer(t)
	// the peer drains continuously, so the flow's congestion window can grow
	// and the send buffer with it
	go io.Copy(io.Discard, peer)
	// read before the setup, not after: a tree that pins the buffer has
	// already replaced the establishment value by then, and a pin above the
	// tcp_wmem maximum would make the skip below swallow the failure
	establishment := tcpSocketSendBufferSize(t, tcpConn)
	if sendAutotuneMax <= establishment {
		t.Skipf(
			"the socket is established with a %d byte send buffer against a net.ipv4.tcp_wmem maximum of %d, so there is nothing to grow into",
			establishment,
			sendAutotuneMax,
		)
	}
	configureUpstreamTcpConn(tcpConn, int(DefaultTcpBufferSettings().MaxWindowSize), socketBufferPolicy{}, false)

	// enough writes to take the congestion window past the point where the
	// kernel's own sizing exceeds the maximum; it stops there, so the loop
	// stops there too rather than writing a fixed volume
	const maxWriteByteCount = 512 * 1024 * 1024
	block := make([]byte, 1024*1024)
	sendBufferSize := establishment
	for writtenByteCount := 0; writtenByteCount < maxWriteByteCount; writtenByteCount += len(block) {
		if _, err := tcpConn.Write(block); err != nil {
			t.Fatal(err)
		}
		if sendBufferSize = tcpSocketSendBufferSize(t, tcpConn); sendBufferSize == sendAutotuneMax {
			return
		}
	}

	t.Fatalf(
		"upstream socket send buffer settled at %d from %d over %d bytes written, want the net.ipv4.tcp_wmem maximum %d; an explicit SO_SNDBUF locks the buffer at its clamped request and the flow can never hold more than that unacknowledged",
		sendBufferSize,
		establishment,
		maxWriteByteCount,
		sendAutotuneMax,
	)
}
