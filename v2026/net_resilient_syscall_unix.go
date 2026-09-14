//go:build unix

package connect

import (
	"net"
	"net/netip"
	"syscall"
)

type SocketHandle = int

// duplicateSocketHandle retains a socket handle for the complete TTL
// choreography without calling TCPConn.File().Fd(). File().Fd() deliberately
// clears O_NONBLOCK on Unix; duplicated descriptors share that status flag, so
// it also turns the original net.Conn into a blocking socket and can deadlock
// HTTP/2 Close behind a raw read. Dup leaves the shared status flags unchanged.
func duplicateSocketHandle(conn *net.TCPConn) (SocketHandle, func(), error) {
	rawConn, err := conn.SyscallConn()
	if err != nil {
		return -1, nil, err
	}
	duplicate := -1
	var duplicateErr error
	if err := rawConn.Control(func(fd uintptr) {
		duplicate, duplicateErr = syscall.Dup(int(fd))
		if duplicateErr == nil {
			syscall.CloseOnExec(duplicate)
		}
	}); err != nil {
		return -1, nil, err
	}
	if duplicateErr != nil {
		return -1, nil, duplicateErr
	}
	return duplicate, func() { _ = syscall.Close(duplicate) }, nil
}

// socketTtlOption selects the socket option that controls the outgoing hop
// count for THIS socket's address family. IP_TTL is an IPv4 option: on an
// AF_INET6 socket a get returns 0 and a set fails with EINVAL, which silently
// took the reorder technique off every IPv6 connection (the caller reads a
// native ttl of 0 as "cannot restore" and writes the record whole).
//
// The local address decides it, not the peer: an AF_INET6 socket carrying a
// v4-mapped destination emits IPv4 packets whose ttl is IP_TTL, and its local
// address is v4-mapped too, so one test covers both.
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

// GetSocketTtl reads the outgoing hop count. 0 means the syscall failed and
// nothing is restorable; every other value, INCLUDING the negative sentinel,
// is a value SetSocketTtl accepts back. BSD kernels report an unset
// IPV6_UNICAST_HOPS as -1 ("use the kernel default") and take -1 to restore
// it, so a caller must test socketTtlReadable rather than `0 < ttl`.
func GetSocketTtl(fd SocketHandle) int {
	level, opt := socketTtlOption(fd)
	nativeTtl, err := syscall.GetsockoptInt(fd, level, opt)
	if err != nil {
		return socketTtlUnreadable
	}
	return nativeTtl
}

// SetSocketTtl sets the outgoing TTL (IPv6: hop limit) on the socket. The
// error is returned rather than discarded because the option only accepts
// 1-255: Linux fails a 0 with EINVAL, so a swallowed error let the reorder
// technique silently degrade to plain fragmentation. Callers decide whether a
// failure is fatal (see resilientLowTtl and the TTL helpers in
// net_resilient.go).
func SetSocketTtl(fd SocketHandle, ttl int) error {
	level, opt := socketTtlOption(fd)
	return syscall.SetsockoptInt(fd, level, opt, ttl)
}
