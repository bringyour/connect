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

// a socket dialed the way the provider dials its upstream, through
// ConnectSettings and whatever control hook the settings carry; both ends live
// for the test
func dialUpstreamTestTcpConnWithSettings(
	t *testing.T,
	connectSettings *ConnectSettings,
) (*net.TCPConn, net.Conn) {
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

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	conn, err := connectSettings.DialContext(ctx, "tcp", listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	peer, ok := <-accepted
	if !ok {
		t.Fatal("the upstream test peer did not accept")
	}
	t.Cleanup(func() { peer.Close() })
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		t.Fatalf("the upstream dial returned %T, not a TCP connection", conn)
	}
	return tcpConn, peer
}

// THROUGHPUTFIX §15's decision has a second half that nothing else reads. The
// rows above and the policy rows in ip_upstream_buffer_sizing_linux_test.go
// decide *whether* to pin; the pin itself is carried by a dialer control hook,
// and a hook that is built but never reaches the socket pins nothing while
// every arithmetic row still passes. Nothing in this tree dialled through
// ConnectSettings.DialControl at all before this row.
//
// Recorded because it is the reason this row exists: main carried a row of its
// own here (TestUpstreamTcpConnReceiveBufferGrowsThroughDialPath, dropped in
// the merge) which dialled the real path and required the receive buffer to
// grow unconditionally. That cannot be this tree's assertion — here a pin above
// the autotuning ceiling is deliberate and locks the buffer on purpose — so
// what that row pinned is stated as the decision reaching the socket, plus the
// growth half in the row below on the hosts where nothing is pinned.
//
// Every expected value is the kernel's own arithmetic, read from /proc: an
// explicit request obtains min(request, core max), doubled on Linux.
func TestTheUpstreamDialControlAppliesThePolicyToTheSocket(t *testing.T) {
	host := readSocketBufferPolicy()
	if !host.known {
		t.Skip("the kernel's buffer maxima are unreadable, so no pin can be predicted")
	}
	settings := DefaultTcpBufferSettings()
	requestByteCount := int(settings.MaxWindowSize)

	// the wiring: the shipped settings carry a hook exactly when the host's
	// policy calls for a pin at the shipped window
	pins := host.explicitSend(requestByteCount) || host.explicitReceive(requestByteCount)
	if hooked := settings.ConnectSettings.DialControl != nil; hooked != pins {
		t.Fatalf(
			"the shipped upstream settings carry a dial control hook = %t against a policy that pins send = %t and receive = %t for a %d byte window; the decision and what applies it have come apart",
			hooked,
			host.explicitSend(requestByteCount),
			host.explicitReceive(requestByteCount),
			requestByteCount,
		)
	}
	// an undecided policy never builds a hook, so the default dialer keeps
	// none it does not need
	if hook := upstreamSocketBufferControl(requestByteCount, socketBufferPolicy{}); hook != nil {
		t.Fatal("an unknown policy built a dial control hook, so a host whose kernel cannot be read would be pinned blind")
	}

	// What the hook does, through the dial the provider actually uses. The
	// policy is synthetic so this does not depend on the host's ceilings: zero
	// ceilings make the pin worth making by the same rule the shipped policy
	// applies, while the clamp and the doubling stay the host's own.
	pinning := host
	pinning.sendCeilingByteCount = 0
	pinning.receiveCeilingByteCount = 0
	if !pinning.explicitSend(requestByteCount) || !pinning.explicitReceive(requestByteCount) {
		t.Fatalf(
			"a policy with no ceiling at all declined to pin a %d byte request against core maxima %d and %d, so the rule no longer reads the ceiling",
			requestByteCount,
			pinning.sendCoreMaxByteCount,
			pinning.receiveCoreMaxByteCount,
		)
	}
	connectSettings := *DefaultConnectSettings()
	connectSettings.DialControl = upstreamSocketBufferControl(requestByteCount, pinning)
	if connectSettings.DialControl == nil {
		t.Fatal("a policy that pins both directions built no dial control hook")
	}
	pinned, _ := dialUpstreamTestTcpConnWithSettings(t, &connectSettings)

	wantSend := pinning.obtained(requestByteCount, pinning.sendCoreMaxByteCount)
	wantReceive := pinning.obtained(requestByteCount, pinning.receiveCoreMaxByteCount)
	if sendByteCount := tcpSocketSendBufferSize(t, pinned); sendByteCount != wantSend {
		t.Errorf(
			"the dialled upstream socket has a %d byte send buffer, want %d for a %d byte request against a %d byte net.core.wmem_max; the pre-connect pin did not reach the socket",
			sendByteCount,
			wantSend,
			requestByteCount,
			pinning.sendCoreMaxByteCount,
		)
	}
	if receiveByteCount := tcpSocketReceiveBufferSize(t, pinned); receiveByteCount != wantReceive {
		t.Errorf(
			"the dialled upstream socket has a %d byte receive buffer, want %d for a %d byte request against a %d byte net.core.rmem_max; the pre-connect pin did not reach the socket, so the SYN-time window clamp was not set either",
			receiveByteCount,
			wantReceive,
			requestByteCount,
			pinning.receiveCoreMaxByteCount,
		)
	}

	// and the other direction of the same wire: a dial with no hook leaves the
	// socket at the kernel's establishment values, which are the ones the
	// growth rows watch move
	unpinnedSettings := *DefaultConnectSettings()
	unpinnedSettings.DialControl = nil
	unpinned, _ := dialUpstreamTestTcpConnWithSettings(t, &unpinnedSettings)
	if sendByteCount := tcpSocketSendBufferSize(t, unpinned); sendByteCount == wantSend {
		t.Errorf(
			"a dial with no control hook still produced the pinned %d byte send buffer, so this row cannot tell a pin from an establishment value on this host",
			sendByteCount,
		)
	}
}

// The growth half of the row above, and the part of main's discarded row that
// this tree can keep: on a host where the policy pins nothing, the provider's
// own dial path must leave receive autotuning alone. The configure-only rows
// above cannot catch a pin that moves rather than disappears — into the
// dialer's control hook, or into the dialer itself — because they never dial
// through it. This one does, and it fails on any tree that locks the buffer
// before connect on a host the rule says to leave alone.
func TestUpstreamTcpReceiveBufferGrowsThroughTheDialPath(t *testing.T) {
	if moderate, err := os.ReadFile("/proc/sys/net/ipv4/tcp_moderate_rcvbuf"); err != nil ||
		!bytes.HasPrefix(bytes.TrimSpace(moderate), []byte("1")) {
		t.Skip("receive autotuning (net.ipv4.tcp_moderate_rcvbuf) is disabled on this host")
	}

	receiveAutotuneMax := upstreamSysctlValues(t, "net/ipv4/tcp_rmem")[2]
	settings := DefaultTcpBufferSettings()
	requestByteCount := int(settings.MaxWindowSize)
	host := readSocketBufferPolicy()
	if host.known && host.explicitReceive(requestByteCount) {
		t.Skipf(
			"this host's policy pins the receive buffer at %d, above the %d byte autotuning ceiling, so the lock is the decision rather than a defect",
			host.obtained(requestByteCount, host.receiveCoreMaxByteCount),
			host.receiveCeilingByteCount,
		)
	}
	tcpConn, peer := dialUpstreamTestTcpConnWithSettings(t, &settings.ConnectSettings)
	// the dial ran whatever hook the settings carry, which is what
	// configureUpstreamTcpConn is told so it adds nothing after connect
	configureUpstreamTcpConn(tcpConn, requestByteCount, host, true)
	establishment := tcpSocketReceiveBufferSize(t, tcpConn)
	if receiveAutotuneMax <= establishment {
		t.Skipf(
			"the socket is established with a %d byte receive buffer against a net.ipv4.tcp_rmem maximum of %d, so there is nothing to grow into",
			establishment,
			receiveAutotuneMax,
		)
	}

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
	receiveBufferSize := establishment
	for readByteCount := 0; readByteCount < maxReadByteCount; readByteCount += len(block) {
		if _, err := io.ReadFull(tcpConn, block); err != nil {
			t.Fatal(err)
		}
		if receiveBufferSize = tcpSocketReceiveBufferSize(t, tcpConn); establishment < receiveBufferSize {
			return
		}
	}

	t.Fatalf(
		"the receive buffer of a socket dialled through the provider's own dial path stayed at %d over %d bytes read, from a %d byte establishment value and against the net.ipv4.tcp_rmem maximum %d; something on the dial path set SO_RCVBUF, which locks autotuning and freezes the window clamp at its SYN-time size",
		receiveBufferSize,
		maxReadByteCount,
		establishment,
		receiveAutotuneMax,
	)
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
