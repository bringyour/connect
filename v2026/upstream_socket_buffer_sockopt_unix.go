//go:build unix

package connect

import "syscall"

// Sets the explicit socket buffers on a raw descriptor before it connects.
func setSocketBufferByteCounts(fd uintptr, send bool, receive bool, byteCount int) {
	if send {
		_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_SNDBUF, byteCount)
	}
	if receive {
		_ = syscall.SetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF, byteCount)
	}
}
