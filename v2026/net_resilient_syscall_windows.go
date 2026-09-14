//go:build windows

package connect

import (
	"net"
	"net/netip"
	"syscall"
)

type SocketHandle = syscall.Handle

func duplicateSocketHandle(conn *net.TCPConn) (SocketHandle, func(), error) {
	file, err := conn.File()
	if err != nil {
		return 0, nil, err
	}
	return SocketHandle(file.Fd()), func() { _ = file.Close() }, nil
}

// socketTtlOption selects the socket option that controls the outgoing hop
// count for THIS socket's address family. See the unix file for why IP_TTL
// alone silently disabled the reorder technique on IPv6 sockets.
func socketTtlOption(fd SocketHandle) (level int, opt int) {
	if sockaddr, err := syscall.Getsockname(fd); err == nil {
		if in6, ok := sockaddr.(*syscall.SockaddrInet6); ok {
			if !netip.AddrFrom16(in6.Addr).Is4In6() {
				return syscall.IPPROTO_IPV6, syscall.IPV6_UNICAST_HOPS
			}
		}
	}
	return syscall.IPPROTO_IP, syscall.IP_TTL
}

// GetSocketTtl reads the outgoing hop count. See the unix file for the
// contract: 0 means unreadable, and a negative value is the kernel-default
// sentinel that SetSocketTtl accepts back.
func GetSocketTtl(fd SocketHandle) int {
	level, opt := socketTtlOption(fd)
	nativeTtl, err := syscall.GetsockoptInt(fd, level, opt)
	if err != nil {
		return socketTtlUnreadable
	}
	return nativeTtl
}

// SetSocketTtl sets the outgoing TTL (IPv6: hop limit) on the socket. The
// error is returned rather than discarded so callers can tell a rejected TTL
// from an applied one. Windows accepts IP_TTL=0 where Linux rejects it, so the
// discarded error also hid a real behaviour difference between the two
// platforms.
func SetSocketTtl(fd SocketHandle, ttl int) error {
	level, opt := socketTtlOption(fd)
	return syscall.SetsockoptInt(fd, level, opt, ttl)
}
