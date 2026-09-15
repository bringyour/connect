//go:build windows

package connect

import "syscall"

// Sets the explicit socket buffers on a raw handle before it connects. The
// policy is unknown on Windows today, so this is reached only by an injected
// policy; Windows autotunes its receive window and the send buffer is a
// per-socket default, so nothing is pinned by default.
func setSocketBufferByteCounts(fd uintptr, send bool, receive bool, byteCount int) {
	if send {
		_ = syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, byteCount)
	}
	if receive {
		_ = syscall.SetsockoptInt(syscall.Handle(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, byteCount)
	}
}
