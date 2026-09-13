//go:build linux

package connect

import (
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

// THROUGHPUTFIX H3. The upstream socket buffers the provider used to set were
// sized from `TcpBufferSettings.MaxWindowSize`, which the process memory
// policy scales, so a provider on a phone asked the kernel for a different
// window than a provider on a server. This pins what that policy actually
// requests at every budget a shipping host sets, and what the kernel does with
// the request on a socket in the state the provider configures its upstream
// in: already connected.
//
// The kernel rows measure the counterfactual the fix removed rather than
// calling the provider's setup, so they characterize the kernel and pass on
// both trees; that is deliberate, so this file runs unchanged against a tree
// from before the program. The guard that the provider no longer makes the
// call is `TestUpstreamTcpConnLeavesReceiveBufferToAutotuning` and its send
// mirror.
//
// Linux only, for the reason the sibling file gives: on macOS an oversized
// explicit set is refused and autotuning continues, so it is a silent no-op
// there rather than a lock, and nothing here can be decided.
func TestUpstreamBufferSizingUnderMobilePolicy(t *testing.T) {
	defer SetMemoryBudget(0)

	// What the pre-fix upstream setup asked for. The 256 KiB floor is only
	// reached at a 1 MiB budget, which is below every shipping target: the
	// phone case is megabytes, not the floor.
	windowSizes := []struct {
		budgetByteCount ByteCount
		maxWindowSize   uint32
		host            string
	}{
		{budgetByteCount: 0, maxWindowSize: uint32(mib(16)), host: "unbudgeted server or desktop provider"},
		{budgetByteCount: mib(32), maxWindowSize: uint32(mib(8)), host: "ios packet tunnel, android cap"},
		{budgetByteCount: mib(24), maxWindowSize: uint32(mib(4)), host: "android 32 MiB heap class"},
		{budgetByteCount: mib(8), maxWindowSize: uint32(mib(2)), host: "pre-ios-16 packet tunnel"},
		{budgetByteCount: mib(1), maxWindowSize: uint32(kib(256)), host: "below every shipping target"},
	}
	for _, windowSize := range windowSizes {
		SetMemoryBudget(windowSize.budgetByteCount)
		maxWindowSize := DefaultTcpBufferSettings().MaxWindowSize
		if maxWindowSize != windowSize.maxWindowSize {
			t.Errorf(
				"a %d byte memory budget (%s) requests a %d byte upstream window, want %d",
				windowSize.budgetByteCount,
				windowSize.host,
				maxWindowSize,
				windowSize.maxWindowSize,
			)
		}
	}

	SetMemoryBudget(mib(32))
	mobileWindowSize := int(DefaultTcpBufferSettings().MaxWindowSize)
	SetMemoryBudget(0)
	serverWindowSize := int(DefaultTcpBufferSettings().MaxWindowSize)

	receiveMax := upstreamSysctlValues(t, "net/core/rmem_max")[0]
	sendMax := upstreamSysctlValues(t, "net/core/wmem_max")[0]
	// tcp_{r,w}mem is min/default/max; the last is where autotuning may go,
	// and it is not clamped by net.core.{r,w}mem_max
	receiveAutotuneMax := upstreamSysctlValues(t, "net/ipv4/tcp_rmem")[2]
	sendAutotuneMax := upstreamSysctlValues(t, "net/ipv4/tcp_wmem")[2]

	mobileReceivePin := pinnedUpstreamBufferSize(t, syscall.SO_RCVBUF, mobileWindowSize)
	serverReceivePin := pinnedUpstreamBufferSize(t, syscall.SO_RCVBUF, serverWindowSize)
	mobileSendPin := pinnedUpstreamBufferSize(t, syscall.SO_SNDBUF, mobileWindowSize)
	serverSendPin := pinnedUpstreamBufferSize(t, syscall.SO_SNDBUF, serverWindowSize)

	// The kernel clamps the request to net.core.{r,w}mem_max and then doubles
	// what it accepts, because half of a socket buffer covers per-packet
	// overhead rather than payload. Two consequences: a buffer read back from
	// getsockopt is about twice the payload it can hold, and any request at or
	// above the system maximum pins the same buffer as any other.
	pins := []struct {
		direction  string
		pin        int
		windowSize int
		systemMax  int
	}{
		{direction: "receive", pin: mobileReceivePin, windowSize: mobileWindowSize, systemMax: receiveMax},
		{direction: "receive", pin: serverReceivePin, windowSize: serverWindowSize, systemMax: receiveMax},
		{direction: "send", pin: mobileSendPin, windowSize: mobileWindowSize, systemMax: sendMax},
		{direction: "send", pin: serverSendPin, windowSize: serverWindowSize, systemMax: sendMax},
	}
	for _, pin := range pins {
		if want := 2 * min(pin.windowSize, pin.systemMax); pin.pin != want {
			t.Errorf(
				"an explicit %d byte %s window pinned the buffer at %d, want twice the system maximum-clamped request, %d",
				pin.windowSize,
				pin.direction,
				pin.pin,
				want,
			)
		}
	}

	// So the memory policy only changes what the provider pins where the
	// system maximum is above the scaled window. On a host that leaves the
	// maximum at its stock 208 KiB, every budget from a phone to a server
	// pins the same buffer and the mobile scaling costs nothing at all.
	directions := []struct {
		direction string
		mobilePin int
		serverPin int
		systemMax int
	}{
		{direction: "receive", mobilePin: mobileReceivePin, serverPin: serverReceivePin, systemMax: receiveMax},
		{direction: "send", mobilePin: mobileSendPin, serverPin: serverSendPin, systemMax: sendMax},
	}
	for _, direction := range directions {
		if direction.systemMax <= mobileWindowSize {
			if direction.mobilePin != direction.serverPin {
				t.Errorf(
					"the %s buffer pinned at %d under the mobile policy and %d unbudgeted, but both windows are at or above the system maximum %d so the kernel clamps them alike",
					direction.direction,
					direction.mobilePin,
					direction.serverPin,
					direction.systemMax,
				)
			}
		} else if direction.serverPin <= direction.mobilePin {
			t.Errorf(
				"the mobile %s window %d pinned the buffer at %d, which is not below the unbudgeted window %d at %d, although the system maximum %d leaves room to differ",
				direction.direction,
				mobileWindowSize,
				direction.mobilePin,
				serverWindowSize,
				direction.serverPin,
				direction.systemMax,
			)
		}
	}

	// H3's claim, in the form the kernel decides it: the explicit set is
	// clamped to net.core.{r,w}mem_max and autotuning is not, so wherever an
	// operator leaves that maximum below tcp_{r,w}mem's maximum the pin sits
	// below the buffer autotuning could have reached. Both are host tunables,
	// so this is a comparison rather than a fixed number, and a host tuned the
	// other way records the inversion instead of failing: there the explicit
	// set was the larger buffer and removing it is a trade, not a win.
	for _, direction := range []struct {
		direction     string
		pin           int
		systemMax     int
		systemMaxName string
		autotuneMax   int
		autotuneName  string
	}{
		{
			direction:     "receive",
			pin:           mobileReceivePin,
			systemMax:     receiveMax,
			systemMaxName: "net.core.rmem_max",
			autotuneMax:   receiveAutotuneMax,
			autotuneName:  "net.ipv4.tcp_rmem",
		},
		{
			direction:     "send",
			pin:           mobileSendPin,
			systemMax:     sendMax,
			systemMaxName: "net.core.wmem_max",
			autotuneMax:   sendAutotuneMax,
			autotuneName:  "net.ipv4.tcp_wmem",
		},
	} {
		if direction.autotuneMax <= direction.systemMax {
			t.Logf(
				"%s %d is not below the %s maximum %d, so a mobile provider's %d byte %s pin is not below autotuning's reach on this host",
				direction.systemMaxName,
				direction.systemMax,
				direction.autotuneName,
				direction.autotuneMax,
				direction.pin,
				direction.direction,
			)
		} else if direction.autotuneMax <= direction.pin {
			t.Errorf(
				"the mobile %s window pinned the buffer at %d, which is not below autotuning's ceiling %d",
				direction.direction,
				direction.pin,
				direction.autotuneMax,
			)
		}
	}
}

// the buffer the kernel settles on when a window is set explicitly on a
// connected socket, which is what the provider's upstream setup used to do
func pinnedUpstreamBufferSize(t *testing.T, option int, windowSize int) int {
	t.Helper()
	tcpConn := upstreamSizingTestTcpConn(t)
	switch option {
	case syscall.SO_RCVBUF:
		if err := tcpConn.SetReadBuffer(windowSize); err != nil {
			t.Fatal(err)
		}
	case syscall.SO_SNDBUF:
		if err := tcpConn.SetWriteBuffer(windowSize); err != nil {
			t.Fatal(err)
		}
	default:
		t.Fatalf("socket option %d is neither buffer", option)
	}
	rawConn, err := tcpConn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var bufferSize int
	var sockoptErr error
	if err := rawConn.Control(func(fd uintptr) {
		bufferSize, sockoptErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, option)
	}); err != nil {
		t.Fatal(err)
	}
	if sockoptErr != nil {
		t.Fatal(sockoptErr)
	}
	return bufferSize
}

// a connected loopback client socket, in the state the provider configures its
// upstream in; both ends live for the test. kept separate from the sibling
// file's dial helper so this file runs unchanged against a tree from before
// the program, where that helper does not exist
func upstreamSizingTestTcpConn(t *testing.T) *net.TCPConn {
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
	if peer, ok := <-accepted; ok {
		t.Cleanup(func() { peer.Close() })
	}
	return conn.(*net.TCPConn)
}

// the whitespace separated values of one /proc/sys entry
func upstreamSysctlValues(t *testing.T, name string) []int {
	t.Helper()
	content, err := os.ReadFile("/proc/sys/" + name)
	if err != nil {
		t.Skipf("%s is unreadable, so the kernel's clamp cannot be read: %v", name, err)
	}
	values := []int{}
	for _, field := range strings.Fields(string(content)) {
		value, err := strconv.Atoi(field)
		if err != nil {
			t.Fatalf("%s = %q", name, string(content))
		}
		values = append(values, value)
	}
	return values
}
