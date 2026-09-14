//go:build darwin

// macOS socket binding keeps controlled provider traffic outside a packet tunnel.
package connect

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// applyEgressInterface pins a macOS socket to the physical interface selected
// by the caller. The acceptance peer provider uses this to remain independent
// of the product packet tunnel running on the same host.
//
// The socket family is not available to the Control callback, so both options
// are attempted. A single success is sufficient; if both applicable attempts
// fail, using the socket would silently route the provider back into the client
// tunnel and invalidate the peer-to-peer test.
func applyEgressInterface(fd uintptr, index4 uint32, index6 uint32) error {
	attempted := 0
	var lastErr error
	if index4 != 0 {
		attempted++
		if err := unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_BOUND_IF, int(index4)); err != nil {
			lastErr = err
		} else {
			return nil
		}
	}
	if index6 != 0 {
		attempted++
		if err := unix.SetsockoptInt(int(fd), unix.IPPROTO_IPV6, unix.IPV6_BOUND_IF, int(index6)); err != nil {
			lastErr = err
		} else {
			return nil
		}
	}
	if attempted == 0 || lastErr == nil {
		return nil
	}
	return fmt.Errorf(
		"connect: could not pin the socket to the macOS egress interface (v4=%d v6=%d): %w",
		index4,
		index6,
		lastErr,
	)
}
