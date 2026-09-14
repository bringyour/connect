//go:build js

package connect

import (
	"errors"
	"net"
)

type SocketHandle = int

func duplicateSocketHandle(conn *net.TCPConn) (SocketHandle, func(), error) {
	return 0, nil, errors.ErrUnsupported
}

func GetSocketTtl(fd SocketHandle) int {
	// not supported: socketTtlUnreadable keeps every caller on the
	// single-unmodified-write fallback
	return socketTtlUnreadable
}

// SetSocketTtl reports that the TTL cannot be set. There is no socket option
// surface under js/wasm, and returning nil would claim the low TTL was applied
// when nothing happened, so the reorder technique would look live when it is
// not. Nothing in the resilient path reaches here: GetSocketTtl above returns
// socketTtlUnreadable, and both reorder branches bail out to a single
// unmodified write before any SetSocketTtl call. The unsupported error is for any
// other caller.
func SetSocketTtl(fd SocketHandle, ttl int) error {
	// not supported
	return errors.ErrUnsupported
}
