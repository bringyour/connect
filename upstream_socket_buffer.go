// The provider's upstream socket buffers: whether an explicit request beats
// what the kernel's autotuning would reach, decided from the kernel's own
// maxima (THROUGHPUTFIX §15).
//
// An explicit SO_SNDBUF or SO_RCVBUF locks that direction's autotuning. The
// kernel clamps the request to its core maximum (and Linux doubles it), so
// what the socket actually gets is `min(request, coreMax)`, doubled on Linux.
// Autotuning instead grows the buffer toward its own ceiling, `tcp_wmem[2]`
// or `tcp_rmem[2]` on Linux and `net.inet.tcp.auto{snd,rcv}bufmax` on
// Darwin. The explicit request is therefore better exactly when what it
// obtains exceeds that ceiling, and worse otherwise; measured as +363 to
// +403 per cent for the deletion where the pin sat below the ceiling and
// -20.8 per cent where it sat above. Both directions follow the same
// arithmetic. A receive pin is applied only before connect, through the
// dialer's control hook, because a post-connect receive pin freezes the
// window clamp at its SYN-time value on some kernel generations; a send pin
// has no clamp and may be applied either way.
package connect

import (
	"net"
	"sync"
	"syscall"
)

// What the kernel allows and what its autotuning would reach, per direction.
// Zero values mean unknown, and an unknown policy never pins.
type socketBufferPolicy struct {
	known bool
	// Linux stores twice the clamped request; Darwin stores it as is
	doubled bool
	// the clamp applied to an explicit request
	sendCoreMaxByteCount    int
	receiveCoreMaxByteCount int
	// the most autotuning would reach without a request
	sendCeilingByteCount    int
	receiveCeilingByteCount int
}

var defaultSocketBufferPolicyOnce sync.Once
var defaultSocketBufferPolicyValue socketBufferPolicy

// The process-wide policy, read from the kernel once.
func defaultSocketBufferPolicy() socketBufferPolicy {
	defaultSocketBufferPolicyOnce.Do(func() {
		defaultSocketBufferPolicyValue = readSocketBufferPolicy()
	})
	return defaultSocketBufferPolicyValue
}

// What an explicit request obtains on this kernel.
func (self socketBufferPolicy) obtained(requestByteCount int, coreMaxByteCount int) int {
	obtained := min(requestByteCount, coreMaxByteCount)
	if self.doubled {
		obtained *= 2
	}
	return obtained
}

// Whether an explicit send request beats the autotuning ceiling.
func (self socketBufferPolicy) explicitSend(requestByteCount int) bool {
	return self.known && 0 < requestByteCount &&
		self.sendCeilingByteCount < self.obtained(requestByteCount, self.sendCoreMaxByteCount)
}

// Whether an explicit receive request beats the autotuning ceiling.
func (self socketBufferPolicy) explicitReceive(requestByteCount int) bool {
	return self.known && 0 < requestByteCount &&
		self.receiveCeilingByteCount < self.obtained(requestByteCount, self.receiveCoreMaxByteCount)
}

// A dialer control hook that applies the explicit buffers the policy calls
// for before the socket connects, so a receive request also sets the
// SYN-time window clamp. Nil when the policy pins nothing, so the default
// dialer keeps no hook it does not need.
func upstreamSocketBufferControl(
	requestByteCount int,
	policy socketBufferPolicy,
) func(network string, address string, c syscall.RawConn) error {
	explicitSend := policy.explicitSend(requestByteCount)
	explicitReceive := policy.explicitReceive(requestByteCount)
	if !explicitSend && !explicitReceive {
		return nil
	}
	return func(network string, address string, c syscall.RawConn) error {
		return c.Control(func(fd uintptr) {
			// best effort, as the post-connect setters are: a refused request
			// leaves autotuning in place
			setSocketBufferByteCounts(fd, explicitSend, explicitReceive, requestByteCount)
		})
	}
}

// Prepares the established upstream socket the provider proxies through.
// `preConnectApplied` says the dial ran the control hook above, in which
// case nothing more is set. Otherwise the dial was opaque (a host-supplied
// DialContext), and only the send buffer may still be pinned here: a
// post-connect send pin behaves as a pre-connect one, while a post-connect
// receive pin freezes the window clamp on some kernels and is never applied.
func configureUpstreamTcpConn(
	tcpConn *net.TCPConn,
	requestByteCount int,
	policy socketBufferPolicy,
	preConnectApplied bool,
) {
	tcpConn.SetKeepAlive(true)
	tcpConn.SetNoDelay(true)
	if !preConnectApplied && policy.explicitSend(requestByteCount) {
		// the os may silently cap this at its limits
		tcpConn.SetWriteBuffer(requestByteCount)
	}
}
