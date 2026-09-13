//go:build !linux

package connect

import "net"

// Only Linux exposes a per-socket drop counter (SO_MEMINFO); elsewhere the
// kernel's UDP receive drops stay uncounted.
func udpSocketReceiveDropCount(socket net.Conn) uint64 {
	return 0
}
