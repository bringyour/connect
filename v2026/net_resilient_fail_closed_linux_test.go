//go:build linux

package connect

import (
	"os"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

// countOpenFds counts the process's open file descriptors via /proc/self/fd,
// which only exists on Linux.
func countOpenFds(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatalf("read /proc/self/fd: %v", err)
	}
	return len(entries)
}

// TestResilientTlsConnFragmentDoesNotLeakFileDescriptors verifies the
// fragment+reorder path does not leak a socket fd on failure.
func TestResilientTlsConnFragmentDoesNotLeakFileDescriptors(t *testing.T) {
	record := buildClientHelloRecord(t)

	before := countOpenFds(t)
	for i := 0; i < 20; i++ {
		client, server := newTcpPair(t)
		client.SetWriteDeadline(time.Now().Add(-time.Second))
		rconn := NewResilientTlsConn(client, true, true)
		if _, err := rconn.Write(record); err == nil {
			t.Fatalf("iteration %d: expected write error, got nil", i)
		}
		client.Close()
		server.Close()
	}
	after := countOpenFds(t)
	if after > before+5 {
		t.Fatalf("file descriptors grew from %d to %d over 20 failed fragment writes", before, after)
	}
}

// TestResilientTlsConnReorderOnlyDoesNotLeakFileDescriptors verifies the
// reorder-only path does not leak a socket fd on failure.
func TestResilientTlsConnReorderOnlyDoesNotLeakFileDescriptors(t *testing.T) {
	record := buildClientHelloRecord(t)

	before := countOpenFds(t)
	for i := 0; i < 20; i++ {
		client, server := newTcpPair(t)
		client.SetWriteDeadline(time.Now().Add(-time.Second))
		rconn := NewResilientTlsConn(client, false, true)
		if _, err := rconn.Write(record); err == nil {
			t.Fatalf("iteration %d: expected write error, got nil", i)
		}
		client.Close()
		server.Close()
	}
	after := countOpenFds(t)
	if after > before+5 {
		t.Fatalf("file descriptors grew from %d to %d over 20 failed reorder writes", before, after)
	}
}

// TestResilientTlsConnTtlChoreographyKeepsSocketNonblocking is the regression
// test for an HTTP/2 shutdown deadlock seen by the Linux acceptance runner.
// TCPConn.File().Fd() makes its duplicate blocking, and O_NONBLOCK is shared by
// duplicated descriptors on Unix. The old TTL implementation therefore also
// made the original connection blocking; an HTTP/2 reader then stayed in a raw
// read syscall while Close waited forever for its read lock. Inspecting the
// original descriptor's flag makes the failure deterministic without relying
// on goroutine scheduling or a timeout.
func TestResilientTlsConnTtlChoreographyKeepsSocketNonblocking(t *testing.T) {
	record := buildClientHelloRecord(t)
	for _, tc := range []struct {
		name     string
		fragment bool
		reorder  bool
	}{
		{name: "fragment-and-reorder", fragment: true, reorder: true},
		{name: "reorder-only", reorder: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, server := newTcpPair(t)
			defer client.Close()
			defer server.Close()

			rconn := NewResilientTlsConn(client, tc.fragment, tc.reorder)
			if _, err := rconn.Write(record); err != nil {
				t.Fatalf("write: %v", err)
			}
			readTlsRecords(t, server, len(record)-5)

			rawConn, err := client.SyscallConn()
			if err != nil {
				t.Fatalf("syscall connection: %v", err)
			}
			flags := 0
			if err := rawConn.Control(func(fd uintptr) {
				flags, err = unix.FcntlInt(fd, unix.F_GETFL, 0)
			}); err != nil {
				t.Fatalf("inspect socket flags: %v", err)
			}
			if err != nil {
				t.Fatalf("get socket flags: %v", err)
			}
			if flags&unix.O_NONBLOCK == 0 {
				t.Fatalf("socket flags %#x do not include O_NONBLOCK after resilient TLS write", flags)
			}
		})
	}
}
