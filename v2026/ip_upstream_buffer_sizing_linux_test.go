//go:build linux

// Two rules this program earned, recorded here because they are conditions on
// how any row in it is read, not only on the campaign's ledger.
//
// Record the resolved value of every setting, and read it before any result.
// A memory policy, a kernel maximum and a shipping default each silently
// decide what a row measures; several claims in this program were wrong
// because a value was assumed rather than read, and the rows below read every
// one of them at runtime and print them beside the number.
//
// Read per-run values before pooling them. A pooled retransmission count read
// as noise across arms while the per-run values were a perfect binary
// indicator: five of five collapsed runs had exactly one timeout and ten of
// ten healthy runs had none. Pooling first destroys that.

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

// The deletion in this program is a trade, not a win, and this is the
// condition that decides its sign. An explicit request is clamped to
// net.core.{r,w}mem_max and doubled; autotuning is clamped to
// net.ipv4.tcp_{r,w}mem's maximum and nothing else. So the pin beats
// autotuning exactly where the doubled clamped request exceeds that ceiling,
// and loses everywhere else. The campaign measured both signs from one tree:
// +363 to +403 per cent for the deletion at a 1 MiB budget, where the pin sat
// below the ceiling, null at 8 MiB and unbudgeted, and -20.8 per cent at
// 32 MiB with zero of five repetitions better, where it sat above.
//
// Two things are asserted rather than logged. On the fleet's stock host every
// shipping budget pins at the same 425,984 bytes, below both ceilings, so the
// deletion is a gain at every budget there and the memory policy does not
// enter into it. And the classification is monotone in the budget on any host:
// a larger budget can only move a flow toward the pin being the better of the
// two, never back.
func TestUpstreamBufferPinBeatsAutotuningOnlyAboveItsCeiling(t *testing.T) {
	defer SetMemoryBudget(0)

	// Debian, Ubuntu, Fedora and Amazon Linux as shipped
	const stockCoreMax = 212992
	const stockSendCeiling = 4 * 1024 * 1024
	const stockReceiveCeiling = 6 * 1024 * 1024

	// ascending by the window they produce, so the classification may only
	// move from below the ceiling to above it; 0 is unscaled, the largest
	budgetByteCounts := []ByteCount{mib(1), mib(8), mib(24), mib(32), mib(48), 0}
	sendCoreMax := upstreamSysctlValues(t, "net/core/wmem_max")[0]
	receiveCoreMax := upstreamSysctlValues(t, "net/core/rmem_max")[0]
	sendCeiling := upstreamSysctlValues(t, "net/ipv4/tcp_wmem")[2]
	receiveCeiling := upstreamSysctlValues(t, "net/ipv4/tcp_rmem")[2]

	// the kernel's own arithmetic, as the sizing rows above measured it
	pinnedByteCount := func(windowSize int, coreMax int) int {
		return 2 * min(windowSize, coreMax)
	}
	sign := func(pin int, ceiling int) string {
		switch {
		case ceiling < pin:
			return "the pin is the larger buffer, so deleting it loses"
		case pin < ceiling:
			return "autotuning reaches further, so deleting the pin gains"
		default:
			return "the two are equal, so deleting the pin is null"
		}
	}

	previousAbove := false
	for _, budgetByteCount := range budgetByteCounts {
		SetMemoryBudget(budgetByteCount)
		windowSize := int(DefaultTcpBufferSettings().MaxWindowSize)

		// the fleet's case: the core maximum is an order of magnitude below
		// either ceiling, so it, not the window, decides the pin
		if stockPin := pinnedByteCount(windowSize, stockCoreMax); stockPin != 2*stockCoreMax {
			t.Errorf(
				"a %d byte budget asks for a %d byte window, which on a stock host pins %d rather than twice the %d byte core maximum; a shipping budget below the core maximum would make the memory policy decide the pin",
				budgetByteCount,
				windowSize,
				stockPin,
				stockCoreMax,
			)
		} else {
			if stockSendCeiling <= stockPin {
				t.Errorf(
					"a %d byte budget pins %d on a stock host, at or above the %d byte tcp_wmem ceiling; the send deletion would be a loss on the fleet's own hosts",
					budgetByteCount,
					stockPin,
					stockSendCeiling,
				)
			}
			if stockReceiveCeiling <= stockPin {
				t.Errorf(
					"a %d byte budget pins %d on a stock host, at or above the %d byte tcp_rmem ceiling; the receive deletion would be a loss on the fleet's own hosts",
					budgetByteCount,
					stockPin,
					stockReceiveCeiling,
				)
			}
		}

		sendPin := pinnedByteCount(windowSize, sendCoreMax)
		receivePin := pinnedByteCount(windowSize, receiveCoreMax)
		above := sendCeiling < sendPin
		if previousAbove && !above {
			t.Errorf(
				"a %d byte budget puts the send pin at %d, back below the %d byte ceiling a smaller budget had passed; the classification must be monotone in the budget",
				budgetByteCount,
				sendPin,
				sendCeiling,
			)
		}
		previousAbove = previousAbove || above

		t.Logf(
			"budget %d: window %d, send pin %d against ceiling %d (%s), receive pin %d against ceiling %d (%s)",
			budgetByteCount,
			windowSize,
			sendPin,
			sendCeiling,
			sign(sendPin, sendCeiling),
			receivePin,
			receiveCeiling,
			sign(receivePin, receiveCeiling),
		)
	}
}

// THROUGHPUTFIX §15: the tree no longer deletes the explicit request or keeps
// it, it decides. The four rows in the sibling file pass an unknown policy and
// so only pin that an undecided policy never pins; this pins the decision
// itself, on synthetic policies so it does not move with the host's sysctls.
//
// The rule is the one the sizing rows above measured out of the kernel: an
// explicit request obtains `min(request, coreMax)`, doubled on Linux, and it is
// worth making exactly when that exceeds what autotuning would reach on its
// own. Both signs were measured from one tree, +363 to +403 per cent for the
// deletion below the ceiling and -20.8 per cent above it.
func TestUpstreamSocketBufferPolicyPinsOnlyAboveTheAutotuningCeiling(t *testing.T) {
	// Debian, Ubuntu, Fedora and Amazon Linux as shipped: the core maximum is
	// an order of magnitude below either ceiling, so nothing is ever pinned
	stock := socketBufferPolicy{
		known:                   true,
		doubled:                 true,
		sendCoreMaxByteCount:    212992,
		receiveCoreMaxByteCount: 212992,
		sendCeilingByteCount:    4 * 1024 * 1024,
		receiveCeilingByteCount: 6 * 1024 * 1024,
	}
	// the runner, and a rig tuned the way tuning guides raise both together
	tuned := socketBufferPolicy{
		known:                   true,
		doubled:                 true,
		sendCoreMaxByteCount:    4 * 1024 * 1024,
		receiveCoreMaxByteCount: 4 * 1024 * 1024,
		sendCeilingByteCount:    4 * 1024 * 1024,
		receiveCeilingByteCount: 33 * 1024 * 1024,
	}
	// XNU stores the request as it is, against a lower autoscaling maximum
	darwin := socketBufferPolicy{
		known:                   true,
		doubled:                 false,
		sendCoreMaxByteCount:    8 * 1024 * 1024,
		receiveCoreMaxByteCount: 8 * 1024 * 1024,
		sendCeilingByteCount:    4 * 1024 * 1024,
		receiveCeilingByteCount: 4 * 1024 * 1024,
	}

	decisions := []struct {
		name             string
		policy           socketBufferPolicy
		requestByteCount int
		explicitSend     bool
		explicitReceive  bool
	}{
		{name: "unknown policy, unbudgeted window", policy: socketBufferPolicy{}, requestByteCount: int(mib(16))},
		{name: "known policy, no request", policy: tuned, requestByteCount: 0},
		{name: "stock host, unbudgeted window", policy: stock, requestByteCount: int(mib(16))},
		{name: "stock host, 32 MiB budget window", policy: stock, requestByteCount: int(mib(8))},
		{name: "stock host, floor window", policy: stock, requestByteCount: int(kib(256))},
		{name: "tuned host, unbudgeted window", policy: tuned, requestByteCount: int(mib(16)), explicitSend: true},
		{name: "tuned host, 32 MiB budget window", policy: tuned, requestByteCount: int(mib(8)), explicitSend: true},
		{name: "tuned host, 8 MiB budget window", policy: tuned, requestByteCount: int(mib(2))},
		{name: "tuned host, floor window", policy: tuned, requestByteCount: int(kib(256))},
		{name: "darwin, 32 MiB budget window", policy: darwin, requestByteCount: int(mib(8)), explicitSend: true, explicitReceive: true},
		{name: "darwin, 8 MiB budget window", policy: darwin, requestByteCount: int(mib(2))},
	}
	for _, decision := range decisions {
		if explicitSend := decision.policy.explicitSend(decision.requestByteCount); explicitSend != decision.explicitSend {
			t.Errorf(
				"%s: a %d byte send request obtains %d against a %d byte ceiling, pinned = %t, want %t",
				decision.name,
				decision.requestByteCount,
				decision.policy.obtained(decision.requestByteCount, decision.policy.sendCoreMaxByteCount),
				decision.policy.sendCeilingByteCount,
				explicitSend,
				decision.explicitSend,
			)
		}
		if explicitReceive := decision.policy.explicitReceive(decision.requestByteCount); explicitReceive != decision.explicitReceive {
			t.Errorf(
				"%s: a %d byte receive request obtains %d against a %d byte ceiling, pinned = %t, want %t",
				decision.name,
				decision.requestByteCount,
				decision.policy.obtained(decision.requestByteCount, decision.policy.receiveCoreMaxByteCount),
				decision.policy.receiveCeilingByteCount,
				explicitReceive,
				decision.explicitReceive,
			)
		}
	}

	// and the established socket follows the decision: a policy that calls for
	// a send pin gets one, a policy that does not is left to the kernel, and a
	// dial that already applied the pin is not pinned twice
	requestByteCount := int(mib(16))
	pinned := dialUpstreamTestTcpConn(t)
	pinnedBefore := tcpSocketSendBufferSize(t, pinned)
	configureUpstreamTcpConn(pinned, requestByteCount, tuned, false)
	if pinnedAfter := tcpSocketSendBufferSize(t, pinned); pinnedAfter == pinnedBefore {
		t.Errorf(
			"a policy whose %d byte request obtains %d against a %d byte ceiling left the send buffer at %d; the request is the larger buffer and is the point of keeping it",
			requestByteCount,
			tuned.obtained(requestByteCount, tuned.sendCoreMaxByteCount),
			tuned.sendCeilingByteCount,
			pinnedAfter,
		)
	}

	unpinned := dialUpstreamTestTcpConn(t)
	unpinnedBefore := tcpSocketSendBufferSize(t, unpinned)
	configureUpstreamTcpConn(unpinned, requestByteCount, stock, false)
	if unpinnedAfter := tcpSocketSendBufferSize(t, unpinned); unpinnedAfter != unpinnedBefore {
		t.Errorf(
			"a stock policy pinned the send buffer from %d to %d; its %d byte request obtains only %d against a %d byte ceiling, so autotuning reaches further",
			unpinnedBefore,
			unpinnedAfter,
			requestByteCount,
			stock.obtained(requestByteCount, stock.sendCoreMaxByteCount),
			stock.sendCeilingByteCount,
		)
	}

	preConnect := dialUpstreamTestTcpConn(t)
	preConnectBefore := tcpSocketSendBufferSize(t, preConnect)
	configureUpstreamTcpConn(preConnect, requestByteCount, tuned, true)
	if preConnectAfter := tcpSocketSendBufferSize(t, preConnect); preConnectAfter != preConnectBefore {
		t.Errorf(
			"a dial that already applied the buffers before connect was pinned again after it, from %d to %d",
			preConnectBefore,
			preConnectAfter,
		)
	}
}
