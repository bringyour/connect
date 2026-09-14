package connect

import (
	"net"
	"sync/atomic"
	"syscall"
)

// Socket self-exclusion for Windows VPN services and controlled macOS peers.
//
// When this process itself provides a VPN tunnel (a DeviceLocal writing a tun
// adapter), its own outbound sockets must not route back into that tunnel, or
// the platform/provider connections loop. Windows has no per-application
// exclusion, so connect binds its outbound sockets to the physical interface
// via IP_UNICAST_IF / IPV6_UNICAST_IF (the wireguard-windows pattern). A
// controlled same-host macOS acceptance provider uses IP_BOUND_IF /
// IPV6_BOUND_IF so its traffic stays outside the app's packet tunnel. A socket
// with a forced egress interface ignores the tun's default route.
//
// The binding is a no-op unless an egress index is set, and a no-op on every
// platform except Windows and macOS, so ordinary mobile and app builds remain
// inert.

var egressInterfaceIndex4 atomic.Uint32
var egressInterfaceIndex6 atomic.Uint32

// SetEgressInterfaceIndex binds connect's outbound sockets to the given
// physical interface indices (IPv4 and IPv6). Pass 0 for a family to leave that
// family unbound. Safe to call repeatedly, e.g. from a network-change handler.
// Only takes effect on Windows and macOS.
func SetEgressInterfaceIndex(index4 uint32, index6 uint32) {
	egressInterfaceIndex4.Store(index4)
	egressInterfaceIndex6.Store(index6)
}

// EgressInterfaceIndex returns the current (index4, index6). Zero means unset.
func EgressInterfaceIndex() (uint32, uint32) {
	return egressInterfaceIndex4.Load(), egressInterfaceIndex6.Load()
}

// egressControl is a net.Dialer / net.ListenConfig Control callback that applies
// the egress interface binding to a socket at creation time.
func egressControl(_ string, _ string, c syscall.RawConn) error {
	index4, index6 := EgressInterfaceIndex()
	if index4 == 0 && index6 == 0 {
		return nil
	}
	var innerErr error
	err := c.Control(func(fd uintptr) {
		innerErr = applyEgressInterface(fd, index4, index6)
	})
	if err != nil {
		return err
	}
	return innerErr
}

// egressDialer installs the egress control on d, chaining any existing Control.
func egressDialer(d *net.Dialer) *net.Dialer {
	prev := d.Control
	d.Control = func(network string, address string, c syscall.RawConn) error {
		if prev != nil {
			if err := prev(network, address, c); err != nil {
				return err
			}
		}
		return egressControl(network, address, c)
	}
	return d
}

// applyEgress binds an already-created socket-backed conn (e.g. *net.UDPConn) to
// the egress interface. A no-op when no egress index is set or off Windows and
// macOS.
func applyEgress(conn syscall.Conn) error {
	index4, index6 := EgressInterfaceIndex()
	if index4 == 0 && index6 == 0 {
		return nil
	}
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}
	return egressControl("", "", raw)
}
