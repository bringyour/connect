//go:build darwin

// Darwin socket tests cover fail-closed physical-interface binding.
package connect

import (
	"net"
	"testing"

	"golang.org/x/sys/unix"
)

// A valid socket binds while an invalid descriptor fails closed.
func TestApplyEgressInterfaceDarwin(t *testing.T) {
	loopback, err := net.InterfaceByName("lo0")
	if err != nil {
		t.Fatal(err)
	}
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(fd)
	if err := applyEgressInterface(uintptr(fd), uint32(loopback.Index), uint32(loopback.Index)); err != nil {
		t.Fatalf("pin valid IPv4 socket: %v", err)
	}
	if err := applyEgressInterface(^uintptr(0), uint32(loopback.Index), uint32(loopback.Index)); err == nil {
		t.Fatal("invalid socket was accepted without an egress binding")
	}
}
