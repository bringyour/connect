//go:build unix || windows

package connect

// net_resilient_ipv6_test.go — the resilient TLS layer over IPv6 sockets.
//
// The TTL reorder technique reads and sets the socket's outgoing hop count
// through GetSocketTtl/SetSocketTtl. The option is family-specific:
// IPPROTO_IP/IP_TTL on an AF_INET socket, IPPROTO_IPV6/IPV6_UNICAST_HOPS on an
// AF_INET6 one. Reading the v4 option on a v6 socket fails with EINVAL, which
// the resilient path used to read as "unusable" — so the technique was
// silently absent on every IPv6 connection. These tests pin that it is
// present on both families, including the BSD kernel-default sentinel that
// reads back as -1 and restores the default when written back.

import (
	"bytes"
	"io"
	"testing"
)

// The reorder technique re-frames the record on BOTH families: the peer
// receives several records whose payloads concatenate to the original
// handshake, and the connection stays usable afterwards.
func TestResilientTlsConnReordersOnBothFamilies(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, server := newTcpPairOnFamily(t, ipVersion)

		if ttl := socketTtl(t, client); !socketTtlReadable(ttl) {
			t.Fatalf("v%d socket hop count is unreadable (%d); the reorder technique cannot run", ipVersion, ttl)
		}

		rconn := NewResilientTlsConn(client, true, true)
		n, err := rconn.Write(record)
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if n != len(record) {
			t.Fatalf("write n=%d want %d", n, len(record))
		}
		if !rconn.Enabled() {
			t.Fatal("layer disabled by a successful write")
		}

		got := readTlsRecords(t, server, len(record)-5)
		if !bytes.Equal(got, record[5:]) {
			t.Fatal("peer received different payload than written")
		}

		followup := []byte("after")
		if _, err := client.Write(followup); err != nil {
			t.Fatalf("follow-up write on the still-open connection: %v", err)
		}
		echo := make([]byte, len(followup))
		if _, err := io.ReadFull(server, echo); err != nil {
			t.Fatalf("follow-up read: %v", err)
		}
		if !bytes.Equal(echo, followup) {
			t.Fatal("follow-up bytes differ")
		}
	})
}

// The hop count is readable, settable and restorable on both families, and
// the restored value is exactly what was read — including a negative
// kernel-default sentinel, which BSD reports for an unset IPV6_UNICAST_HOPS
// and accepts back to mean "use the default again".
func TestSocketTtlRoundTripsOnBothFamilies(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		client, _ := newTcpPairOnFamily(t, ipVersion)
		f, err := client.File()
		if err != nil {
			t.Fatalf("file: %v", err)
		}
		defer f.Close()
		fd := SocketHandle(f.Fd())

		nativeTtl := GetSocketTtl(fd)
		if !socketTtlReadable(nativeTtl) {
			t.Fatalf("v%d native hop count = %d, want a readable value", ipVersion, nativeTtl)
		}
		if err := SetSocketTtl(fd, resilientLowTtl); err != nil {
			t.Fatalf("v%d set low hop count: %v", ipVersion, err)
		}
		if got := GetSocketTtl(fd); got != resilientLowTtl {
			t.Fatalf("v%d hop count after set = %d, want %d", ipVersion, got, resilientLowTtl)
		}
		if err := SetSocketTtl(fd, nativeTtl); err != nil {
			t.Fatalf("v%d restore hop count %d: %v", ipVersion, nativeTtl, err)
		}
		if got := GetSocketTtl(fd); got != nativeTtl {
			t.Fatalf("v%d restored hop count = %d, want %d", ipVersion, got, nativeTtl)
		}
	})
}
