//go:build windows

package connect

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// IP_UNICAST_IF (IPPROTO_IP, option 31) forces the outgoing interface for
// unicast, overriding the route table. The interface index must be passed in
// NETWORK byte order. IPV6_UNICAST_IF (IPPROTO_IPV6, option 31) does the same
// for IPv6 but takes the index in HOST byte order. This asymmetry matches
// wireguard-windows' bindSocketRoute.
const (
	ipUnicastIf   = 31
	ipv6UnicastIf = 31
)

func htonl(x uint32) uint32 {
	return (x&0x000000ff)<<24 | (x&0x0000ff00)<<8 | (x&0x00ff0000)>>8 | (x&0xff000000)>>24
}

// applyEgressInterface sets the forced egress interface on the socket.
//
// Both families are attempted, because the caller does not know which one this
// socket is: setting the wrong family's option on a single-family socket fails
// harmlessly, and treating that as an error would break every connection. So a
// PER-OPTION failure is still ignored, as in wireguard-windows.
//
// What is NOT ignored is every attempted option failing. That means the socket
// carries no forced interface at all, and a socket with no forced interface
// follows the route table — which on the machine this exists for is the tunnel
// this very process provides. Returning nil there made the R1 self-exclusion
// fail open and silent: the deadlock it prevents would happen with nothing
// anywhere saying why. The dominant cause is an interface index that has been
// reclaimed since it was chosen, which is exactly the case worth reporting.
//
// The error travels back through egressControl to net.Dialer.Control, so the
// dial fails and shows up in the existing [net]dial log line. A failed dial
// retries; an unpinned socket blackholes.
func applyEgressInterface(fd uintptr, index4 uint32, index6 uint32) error {
	h := windows.Handle(fd)
	var attempted int
	var lastErr error
	if index4 != 0 {
		attempted++
		if err := windows.SetsockoptInt(
			h,
			windows.IPPROTO_IP,
			ipUnicastIf,
			int(htonl(index4)),
		); err != nil {
			lastErr = err
		} else {
			return nil
		}
	}
	if index6 != 0 {
		attempted++
		if err := windows.SetsockoptInt(
			h,
			windows.IPPROTO_IPV6,
			ipv6UnicastIf,
			int(index6),
		); err != nil {
			lastErr = err
		} else {
			return nil
		}
	}
	if attempted == 0 || lastErr == nil {
		return nil
	}
	return fmt.Errorf(
		"connect: could not pin the socket to the egress interface (v4=%d v6=%d): %w; "+
			"the socket would follow the route table and loop into the tunnel",
		index4,
		index6,
		lastErr,
	)
}
