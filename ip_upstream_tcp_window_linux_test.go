//go:build linux

package connect

import (
	"io"
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

// The measurement campaign's bistable collapse: at a provider budget of 32 MiB
// five of ten download runs across both arms ran at 15.4 to 16.1 Mbit/s with
// the upstream receive window stuck at exactly 104,448 bytes, and the other
// five did not. This names that state and its arithmetic, so a flow found at a
// fixed window is diagnosed rather than re-derived.
//
// 104,448 is `tcp_win_from_space(131072)`: the window the kernel derives from
// `net.ipv4.tcp_rmem`'s default buffer, at the socket's measured payload
// scaling ratio (204/256 there, 195/256 on the runner, which gives 99,840 from
// the same buffer). So a flow stuck at that window is a flow whose receive
// buffer never left the tcp_rmem default, which is to say one where receive
// autotuning never engaged. At the campaign's round trip that window is 16.7
// Mbit/s, which is the rate observed.
//
// This row is characterisation, not a guard: it produces the collapse state
// deliberately, because the trigger is not reproducible in process. Locking the
// buffer is the per-socket form of autotuning not engaging, and it is what the
// tree before this program did on every upstream flow. The guard that the
// provider's own socket is not in this state is
// `TestUpstreamTcpReceiveBufferGrowsUnderLoad`.
func TestUpstreamTcpReceiveWindowWithoutAutotuning(t *testing.T) {
	receiveDefault := upstreamSysctlValues(t, "net/ipv4/tcp_rmem")[1]
	// the tree deleted the only explicit receive buffer it had, so autotuning
	// is now the sole thing that raises an upstream flow's window; a host with
	// it off leaves every provider flow at the default buffer for its life
	if moderate := upstreamSysctlValues(t, "net/ipv4/tcp_moderate_rcvbuf")[0]; moderate == 0 {
		t.Errorf(
			"net.ipv4.tcp_moderate_rcvbuf is 0, so receive autotuning is off on this host and an upstream flow has no receive window floor at all: it runs at the window of the %d byte tcp_rmem default for its life",
			receiveDefault,
		)
	}

	// 64 MiB moves an autotuned loopback socket well past its default; the
	// control arm below asserts that rather than assuming it
	const transferByteCount = 64 * 1024 * 1024
	drain := func(tcpConn *net.TCPConn, peer net.Conn) {
		t.Helper()
		go func() {
			block := make([]byte, 1024*1024)
			for {
				if _, err := peer.Write(block); err != nil {
					return
				}
			}
		}()
		block := make([]byte, 1024*1024)
		for readByteCount := 0; readByteCount < transferByteCount; readByteCount += len(block) {
			if _, err := io.ReadFull(tcpConn, block); err != nil {
				t.Fatal(err)
			}
		}
	}

	// the collapse arm: the buffer is locked at exactly the tcp_rmem default,
	// which is where a socket whose autotuning never engages stays
	lockedConn, lockedPeer := dialUpstreamTestTcpConnWithPeer(t)
	if err := lockedConn.SetReadBuffer(receiveDefault / 2); err != nil {
		t.Fatal(err)
	}
	if locked := tcpSocketReceiveBufferSize(t, lockedConn); locked != receiveDefault {
		t.Fatalf(
			"locking the receive buffer at half the %d byte tcp_rmem default gave %d, not the default; the kernel's clamp and doubling are not what this row assumes",
			receiveDefault,
			locked,
		)
	}
	drain(lockedConn, lockedPeer)
	lockedBuffer := tcpSocketReceiveBufferSize(t, lockedConn)
	lockedWindow := tcpSocketWindowClamp(t, lockedConn)

	// the control arm: the same load on a socket the kernel still owns
	autotunedConn, autotunedPeer := dialUpstreamTestTcpConnWithPeer(t)
	drain(autotunedConn, autotunedPeer)
	autotunedBuffer := tcpSocketReceiveBufferSize(t, autotunedConn)
	autotunedWindow := tcpSocketWindowClamp(t, autotunedConn)

	if autotunedBuffer <= receiveDefault {
		t.Fatalf(
			"the control socket's receive buffer is %d after %d bytes, no larger than the %d byte default, so this load does not decide whether autotuning engaged",
			autotunedBuffer,
			transferByteCount,
			receiveDefault,
		)
	}
	if lockedBuffer != receiveDefault {
		t.Errorf(
			"the locked socket's receive buffer moved to %d from the %d byte default; a locked buffer is supposed to stay where it was put",
			lockedBuffer,
			receiveDefault,
		)
	}
	if lockedWindow <= receiveDefault/2 || receiveDefault <= lockedWindow {
		t.Errorf(
			"the locked socket's receive window is %d, outside the band the kernel derives from its %d byte buffer; the collapse signature is a window just under the buffer it came from",
			lockedWindow,
			receiveDefault,
		)
	}
	if autotunedWindow <= lockedWindow {
		t.Errorf(
			"the autotuned socket's receive window is %d against the locked socket's %d over the same load; autotuning is supposed to be the thing that raises it",
			autotunedWindow,
			lockedWindow,
		)
	}

	// the arithmetic the campaign needs: the stuck window over the round trip
	t.Logf(
		"a flow stuck at the %d byte window of the %d byte tcp_rmem default carries %.1f Mbit/s at a 50 ms round trip and %.1f Mbit/s at 10 ms; the autotuned control reached a %d byte window from a %d byte buffer over the same %d bytes",
		lockedWindow,
		receiveDefault,
		float64(lockedWindow)*8/0.05/1e6,
		float64(lockedWindow)*8/0.01/1e6,
		autotunedWindow,
		autotunedBuffer,
		transferByteCount,
	)
}

// the receive window the kernel will advertise at most, which is what a stuck
// flow shows and what `ss` reports
func tcpSocketWindowClamp(t *testing.T, tcpConn *net.TCPConn) int {
	t.Helper()
	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var windowClamp int
	var sockoptErr error
	if err := rawConn.Control(func(fd uintptr) {
		windowClamp, sockoptErr = unix.GetsockoptInt(int(fd), unix.IPPROTO_TCP, unix.TCP_WINDOW_CLAMP)
	}); err != nil {
		t.Fatal(err)
	}
	if sockoptErr != nil {
		t.Fatal(sockoptErr)
	}
	return windowClamp
}
