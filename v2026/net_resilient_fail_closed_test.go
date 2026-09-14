//go:build unix || windows

package connect

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

// buildClientHelloRecord builds a TLS record (content type 22) carrying a
// ClientHello with a server_name extension, the shape UnmarshalClientHello
// needs to route into the fragment/reorder path.
func buildClientHelloRecord(t *testing.T) []byte {
	t.Helper()

	var clientHello bytes.Buffer
	clientHello.Write([]byte{0x03, 0x03}) // version TLS 1.2
	clientHello.Write(make([]byte, 32))   // random
	clientHello.WriteByte(0)              // session id length
	clientHello.Write([]byte{0x00, 0x02}) // cipher suites length
	clientHello.Write([]byte{0x13, 0x01}) // TLS_AES_128_GCM_SHA256
	clientHello.WriteByte(1)              // compression methods length
	clientHello.WriteByte(0)              // null

	var serverName bytes.Buffer
	serverName.WriteByte(0)              // host_name
	serverName.Write([]byte{0x00, 0x0b}) // name length
	serverName.WriteString("example.com")
	var sniList bytes.Buffer
	binary.Write(&sniList, binary.BigEndian, uint16(serverName.Len()))
	sniList.Write(serverName.Bytes())

	var extensions bytes.Buffer
	binary.Write(&extensions, binary.BigEndian, uint16(0)) // server_name extension type
	binary.Write(&extensions, binary.BigEndian, uint16(sniList.Len()))
	extensions.Write(sniList.Bytes())

	binary.Write(&clientHello, binary.BigEndian, uint16(extensions.Len()))
	clientHello.Write(extensions.Bytes())

	var handshake bytes.Buffer
	handshake.WriteByte(1) // ClientHello
	// uint24 length, written byte-wise (PutUint32 needs 4 bytes)
	l := clientHello.Len()
	handshake.WriteByte(byte(l >> 16))
	handshake.WriteByte(byte(l >> 8))
	handshake.WriteByte(byte(l))
	handshake.Write(clientHello.Bytes())

	record := make([]byte, 0, 5+handshake.Len())
	record = append(record, 22) // handshake content type
	record = append(record, 0x03, 0x03)
	binary.BigEndian.PutUint16(append([]byte{}, 0, 0), uint16(handshake.Len()))
	record = append(record, byte(handshake.Len()>>8), byte(handshake.Len()))
	record = append(record, handshake.Bytes()...)

	if _, meta := UnmarshalClientHello(handshake.Bytes()); meta == nil || meta.ServerNameValueEnd <= meta.ServerNameValueStart {
		t.Fatalf("test ClientHello did not parse into the fragment path")
	}
	return record
}

// newTcpPair returns a connected TCP client/server pair on the v4 loopback,
// for callers that do not run under forEachIpVersion.
func newTcpPair(t *testing.T) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	return newTcpPairOnFamily(t, 4)
}

// newTcpPairOnFamily returns a connected TCP client/server pair on the
// loopback of the given ip version.
func newTcpPairOnFamily(t *testing.T, ipVersion int) (*net.TCPConn, *net.TCPConn) {
	t.Helper()
	ln, err := net.Listen(testTcpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	serverCh := make(chan *net.TCPConn, 1)
	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		serverCh <- conn.(*net.TCPConn)
	}()
	client, err := net.Dial(testTcpNetwork(ipVersion), ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	var server *net.TCPConn
	select {
	case server = <-serverCh:
	case err := <-errCh:
		t.Fatalf("accept: %v", err)
	case <-time.After(5 * time.Second):
		t.Fatalf("accept timeout")
	}
	// the listener is only needed to accept; close it so tests that create
	// many pairs (e.g. the fd-leak check) do not accumulate listener fds
	ln.Close()
	t.Cleanup(func() {
		client.Close()
		server.Close()
	})
	return client.(*net.TCPConn), server
}

// socketTtl reads the socket TTL through a dup'd fd, like the resilient path.
func socketTtl(t *testing.T, conn *net.TCPConn) int {
	t.Helper()
	f, err := conn.File()
	if err != nil {
		t.Fatalf("file: %v", err)
	}
	defer f.Close()
	return GetSocketTtl(SocketHandle(f.Fd()))
}

// setSocketTtl sets the socket TTL to ttl for the duration of the test.
// The restore is best-effort: it runs during cleanup, after which the conn
// may already be closed, so a failure there must not fail the test.
func setSocketTtl(t *testing.T, conn *net.TCPConn, ttl int) {
	t.Helper()
	f, err := conn.File()
	if err != nil {
		t.Fatalf("file: %v", err)
	}
	// checked: the tests that call this assert against ttl as the native
	// value, so a silently refused set would make them assert against a
	// number the socket never held.
	if err := SetSocketTtl(SocketHandle(f.Fd()), ttl); err != nil {
		f.Close()
		t.Fatalf("set ttl %d: %v", ttl, err)
	}
	f.Close()
	t.Cleanup(func() {
		f, err := conn.File()
		if err != nil {
			return // conn already closed; nothing to restore
		}
		_ = SetSocketTtl(SocketHandle(f.Fd()), 64)
		f.Close()
	})
}

// readTlsRecords reads raw TLS records from r and returns their
// concatenated payloads. The resilient fragment path re-frames a record's
// payload as multiple standalone TLS records, so reassembly is required to
// compare against the original payload.
func readTlsRecords(t *testing.T, r io.Reader, wantPayloadLen int) []byte {
	t.Helper()
	var payload []byte
	for len(payload) < wantPayloadLen {
		var hdr [5]byte
		if _, err := io.ReadFull(r, hdr[:]); err != nil {
			t.Fatalf("read record header: %v", err)
		}
		if hdr[0] != 22 {
			t.Fatalf("record content type = %d, want 22", hdr[0])
		}
		recLen := int(hdr[3])<<8 | int(hdr[4])
		rec := make([]byte, recLen)
		if _, err := io.ReadFull(r, rec); err != nil {
			t.Fatalf("read record body: %v", err)
		}
		payload = append(payload, rec...)
	}
	return payload
}

func TestResilientTlsConnFragmentRestoresTtlAndClosesFd(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, server := newTcpPairOnFamily(t, ipVersion)
		setSocketTtl(t, client, 42)

		rconn := NewResilientTlsConn(client, true, true)
		n, err := rconn.Write(record)
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if n != len(record) {
			t.Fatalf("write n=%d want %d", n, len(record))
		}

		// The socket TTL must be restored to the native value after the
		// fragment write, on the success path.
		if got := socketTtl(t, client); got != 42 {
			t.Fatalf("socket TTL after fragmented write = %d, want 42 (native restored)", got)
		}

		// The peer must receive the full record payload (fragmentation re-frames
		// the payload into standalone TLS records, so payloads concatenate back
		// to the original handshake bytes).
		got := readTlsRecords(t, server, len(record)-5)
		if !bytes.Equal(got, record[5:]) {
			t.Fatalf("peer received different payload than written")
		}
	})
}

func TestResilientTlsConnFragmentFailureDisablesAndRestoresTtl(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, _ := newTcpPairOnFamily(t, ipVersion)

		// Keep a dup'd fd open across the failure: failConnection closes the
		// original conn, but the socket TTL is a socket-level option visible
		// through any fd, so the restore is checkable after the conn is closed.
		probe, err := client.File()
		if err != nil {
			t.Fatalf("file: %v", err)
		}
		defer probe.Close()
		if err := SetSocketTtl(SocketHandle(probe.Fd()), 42); err != nil {
			t.Fatalf("set ttl: %v", err)
		}

		// Expire the write deadline so the first fragment write fails
		// deterministically after the fd and native TTL are acquired.
		client.SetWriteDeadline(time.Now().Add(-time.Second))

		rconn := NewResilientTlsConn(client, true, true)
		_, err = rconn.Write(record)
		if err == nil {
			t.Fatalf("write with expired deadline: expected error, got nil")
		}

		// The layer must be disabled and the connection closed so a retry
		// cannot re-fragment the partially-sent record or append to it.
		if rconn.Enabled() {
			t.Fatalf("layer still enabled after fragment write failure")
		}
		if len(rconn.buffer) != 0 {
			t.Fatalf("buffer not dropped after fragment write failure: %d bytes", len(rconn.buffer))
		}

		// The socket TTL must be restored even on the failure path (via the
		// dup'd fd; the original conn is closed by failConnection).
		if got := GetSocketTtl(SocketHandle(probe.Fd())); got != 42 {
			t.Fatalf("socket TTL after failed fragmented write = %d, want 42 (restored)", got)
		}

		// A subsequent Write must fail: the connection is closed after the
		// indeterminate fragment state, so retries cannot corrupt the stream.
		client.SetWriteDeadline(time.Time{})
		if _, err := rconn.Write(record); err == nil {
			t.Fatalf("write after fragment failure: expected error (conn closed), got nil")
		}
	})
}

func TestResilientTlsConnOffDrainsPartialRecord(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, server := newTcpPairOnFamily(t, ipVersion)

		rconn := NewResilientTlsConn(client, true, false)

		// Write only part of a record: the header and 10 payload bytes. Write
		// returns len(b), nil and buffers the rest.
		partial := record[:15]
		n, err := rconn.Write(partial)
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if n != len(partial) {
			t.Fatalf("write n=%d want %d", n, len(partial))
		}
		if len(rconn.buffer) != len(partial) {
			t.Fatalf("buffered %d bytes, want %d", len(rconn.buffer), len(partial))
		}

		// Off must drain the buffered bytes to the wire before disabling, so
		// the bytes an earlier Write accepted are not stranded. A successful
		// drain returns nil.
		if err := rconn.Off(); err != nil {
			t.Fatalf("Off: unexpected error %v", err)
		}
		if rconn.Enabled() {
			t.Fatalf("layer still enabled after Off")
		}
		if len(rconn.buffer) != 0 {
			t.Fatalf("buffer not drained by Off: %d bytes", len(rconn.buffer))
		}

		got := make([]byte, len(partial))
		if _, err := io.ReadFull(server, got); err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(got, partial) {
			t.Fatalf("peer received different bytes than the drained partial record")
		}
	})
}

func TestResilientTlsConnReorderOnlyFragmentsOnFailure(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, _ := newTcpPairOnFamily(t, ipVersion)

		probe, err := client.File()
		if err != nil {
			t.Fatalf("file: %v", err)
		}
		defer probe.Close()
		if err := SetSocketTtl(SocketHandle(probe.Fd()), 42); err != nil {
			t.Fatalf("set ttl: %v", err)
		}

		client.SetWriteDeadline(time.Now().Add(-time.Second))
		rconn := NewResilientTlsConn(client, false, true)
		_, err = rconn.Write(record)
		if err == nil {
			t.Fatalf("write with expired deadline: expected error, got nil")
		}
		if rconn.Enabled() {
			t.Fatalf("layer still enabled after reorder write failure")
		}
		if got := GetSocketTtl(SocketHandle(probe.Fd())); got != 42 {
			t.Fatalf("socket TTL after failed reorder write = %d, want 42 (restored)", got)
		}
		client.SetWriteDeadline(time.Time{})
		if _, err := rconn.Write(record); err == nil {
			t.Fatalf("write after reorder failure: expected error (conn closed), got nil")
		}
	})
}

func TestResilientTlsConnReorderOnlySuccessRestoresTtl(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, server := newTcpPairOnFamily(t, ipVersion)
		setSocketTtl(t, client, 42)

		rconn := NewResilientTlsConn(client, false, true)
		n, err := rconn.Write(record)
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if n != len(record) {
			t.Fatalf("write n=%d want %d", n, len(record))
		}

		// The reorder-only path writes the record as a sequence of raw byte
		// blocks (not re-wrapped as separate TLS records), so the peer must
		// see the exact original bytes once reassembled.
		if got := socketTtl(t, client); got != 42 {
			t.Fatalf("socket TTL after reorder-only write = %d, want 42 (native restored)", got)
		}

		got := make([]byte, len(record))
		if _, err := io.ReadFull(server, got); err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(got, record) {
			t.Fatalf("peer received different bytes than written")
		}
	})
}

func TestResilientTlsConnFragmentOnlySuccess(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, server := newTcpPairOnFamily(t, ipVersion)

		// fragment without reorder: this path does not touch the fd or TTL at
		// all, only splits the record into standalone TLS records.
		rconn := NewResilientTlsConn(client, true, false)
		n, err := rconn.Write(record)
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if n != len(record) {
			t.Fatalf("write n=%d want %d", n, len(record))
		}

		got := readTlsRecords(t, server, len(record)-5)
		if !bytes.Equal(got, record[5:]) {
			t.Fatalf("peer received different payload than written")
		}
	})
}

func TestResilientTlsConnFragmentOnlyFailureDropsBuffer(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, _ := newTcpPairOnFamily(t, ipVersion)

		// Expire the deadline so the first fragment write fails deterministically.
		client.SetWriteDeadline(time.Now().Add(-time.Second))

		rconn := NewResilientTlsConn(client, true, false)
		_, err := rconn.Write(record)
		if err == nil {
			t.Fatalf("write with expired deadline: expected error, got nil")
		}

		if rconn.Enabled() {
			t.Fatalf("layer still enabled after fragment-only write failure")
		}
		if len(rconn.buffer) != 0 {
			t.Fatalf("buffer not dropped after fragment-only write failure: %d bytes", len(rconn.buffer))
		}

		// A retry after the failure must fail: the connection is closed after
		// the indeterminate fragment state, so retries cannot re-fragment the
		// stale record or append to the corrupt stream.
		client.SetWriteDeadline(time.Time{})
		if _, err := rconn.Write(record); err == nil {
			t.Fatalf("write after fragment failure: expected error (conn closed), got nil")
		}
	})
}

// countingFailConn wraps a net.Conn but is deliberately never a
// *net.TCPConn, so ResilientTlsConn.Write must take the non-fd fallback
// branch (self.conn.Write) for fragment handling instead of the TCPConn/fd
// branch. The call-th Write (1-indexed) returns failErr instead of
// forwarding to the underlying conn; every other call is forwarded so the
// peer still observes the bytes that were actually sent. With shortWriteAt
// set, that call returns a nil-error short write (writes shortN bytes, nil)
// to exercise the short-write fail-closed paths.
type countingFailConn struct {
	net.Conn
	calls        int
	failAt       int
	failErr      error
	shortWriteAt int
	shortN       int
	closed       bool
}

// Close records that the resilient layer closed the connection, so the
// fail-closed tests can assert the close actually happened rather than
// inferring it from the buffer and enabled flag alone.
func (c *countingFailConn) Close() error {
	c.closed = true
	return c.Conn.Close()
}

func (c *countingFailConn) Write(b []byte) (int, error) {
	c.calls++
	if c.shortWriteAt > 0 && c.calls == c.shortWriteAt {
		n := c.shortN
		if n > len(b) {
			n = len(b)
		}
		if n, err := c.Conn.Write(b[:n]); err != nil {
			return n, err
		}
		return n, nil
	}
	if c.failAt > 0 && c.calls == c.failAt {
		return 0, c.failErr
	}
	return c.Conn.Write(b)
}

func TestResilientTlsConnNonTCPConnFragmentSuccess(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, server := newTcpPairOnFamily(t, ipVersion)

		wrapped := &countingFailConn{Conn: client}
		rconn := NewResilientTlsConn(wrapped, true, false)

		n, err := rconn.Write(record)
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if n != len(record) {
			t.Fatalf("write n=%d want %d", n, len(record))
		}
		if wrapped.calls == 0 {
			t.Fatalf("expected writes to be forwarded through the wrapped non-TCPConn")
		}

		got := readTlsRecords(t, server, len(record)-5)
		if !bytes.Equal(got, record[5:]) {
			t.Fatalf("peer received different payload than written")
		}
	})
}

func TestResilientTlsConnNonTCPConnFragmentFailureDropsBuffer(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, _ := newTcpPairOnFamily(t, ipVersion)

		failErr := errors.New("injected write failure")
		wrapped := &countingFailConn{Conn: client, failAt: 1, failErr: failErr}
		rconn := NewResilientTlsConn(wrapped, true, false)

		_, err := rconn.Write(record)
		if !errors.Is(err, failErr) {
			t.Fatalf("write error = %v, want %v", err, failErr)
		}

		if rconn.Enabled() {
			t.Fatalf("layer still enabled after non-TCPConn fragment write failure")
		}
		if len(rconn.buffer) != 0 {
			t.Fatalf("buffer not dropped after non-TCPConn fragment write failure: %d bytes", len(rconn.buffer))
		}

		// The layer must also close the underlying connection, so a later
		// write fails instead of appending to the corrupt stream. Without this
		// the test would pass even if failConnection stopped closing.
		if !wrapped.closed {
			t.Fatalf("underlying connection not closed after a failed write")
		}
	})
}

func TestResilientTlsConnOffNoopWhenBufferEmpty(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		client, server := newTcpPairOnFamily(t, ipVersion)
		rconn := NewResilientTlsConn(client, true, false)

		if len(rconn.buffer) != 0 {
			t.Fatalf("buffer not empty before Off: %d bytes", len(rconn.buffer))
		}

		// Off on an empty buffer is a no-op and returns nil.
		if err := rconn.Off(); err != nil {
			t.Fatalf("Off: unexpected error %v", err)
		}

		if rconn.Enabled() {
			t.Fatalf("layer still enabled after Off")
		}
		if len(rconn.buffer) != 0 {
			t.Fatalf("buffer unexpectedly non-empty after a no-op Off: %d bytes", len(rconn.buffer))
		}

		// Nothing should have been written to the peer since the buffer was
		// empty; the read must time out rather than return data.
		server.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		one := make([]byte, 1)
		_, err := server.Read(one)
		if err == nil {
			t.Fatalf("peer unexpectedly received data from a no-op Off")
		}
		if ne, ok := err.(net.Error); !ok || !ne.Timeout() {
			t.Fatalf("unexpected error waiting for no data: %v", err)
		}
	})
}

// buildRawRecord builds a single TLS record of the given content type
// wrapping payload, using TlsVersion1_2 as the record version. Unlike
// buildClientHelloRecord, the payload is not required to parse as a
// ClientHello, so this is used to reach the raw-record flush branches
// (non-handshake content types, and handshake records that are not a
// ClientHello with a server_name extension).
func buildRawRecord(contentType TlsContentType, payload []byte) []byte {
	header := &tlsHeader{contentType: contentType, tlsVersion: TlsVersion1_2}
	return header.reconstruct(payload)
}

// buildNonClientHelloHandshakeRecord builds a Handshake-content-type record
// whose body is not a ClientHello (message type 2, e.g. a ServerHello
// framing), so UnmarshalClientHello returns (nil, nil) and Write must take
// the raw-record flush branch instead of the fragment/reorder path.
func buildNonClientHelloHandshakeRecord() []byte {
	handshakeBody := []byte{0xAA, 0xBB, 0xCC}
	handshake := make([]byte, 0, 4+len(handshakeBody))
	handshake = append(handshake, 2) // ServerHello, not ClientHello (type 1)
	l := len(handshakeBody)
	handshake = append(handshake, byte(l>>16), byte(l>>8), byte(l))
	handshake = append(handshake, handshakeBody...)
	return buildRawRecord(TlsContentTypeHandshake, handshake)
}

// shortWriteConn wraps a net.Conn and, on the shortAt-th call (1-indexed) to
// Write, forwards only shortN of the requested bytes to the underlying conn
// and returns (shortN, nil) instead of the full length. This exercises the
// "short write, no error" branch the PR added checks for: prior to the PR,
// only err != nil was checked, so a short write with a nil error would have
// been treated as a full, successful send. Close is tracked so tests can
// confirm failConnection actually closes the wrapped connection.
type shortWriteConn struct {
	net.Conn
	calls   int
	shortAt int
	shortN  int
	closed  bool
}

func (c *shortWriteConn) Write(b []byte) (int, error) {
	c.calls++
	if c.shortAt > 0 && c.calls == c.shortAt {
		n := c.shortN
		if len(b) < n {
			n = len(b)
		}
		if 0 < n {
			if _, err := c.Conn.Write(b[:n]); err != nil {
				return 0, err
			}
		}
		return n, nil
	}
	return c.Conn.Write(b)
}

func (c *shortWriteConn) Close() error {
	c.closed = true
	return c.Conn.Close()
}

func TestResilientTlsConnNonHandshakeRecordFlushSuccess(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildRawRecord(TlsContentTypeApplicationData, []byte("application data payload"))
		client, server := newTcpPairOnFamily(t, ipVersion)

		// non-handshake content types (e.g. application data) never enter the
		// fragment/reorder logic; Write must flush the raw record unmodified.
		rconn := NewResilientTlsConn(client, true, true)
		n, err := rconn.Write(record)
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if n != len(record) {
			t.Fatalf("write n=%d want %d", n, len(record))
		}

		got := make([]byte, len(record))
		if _, err := io.ReadFull(server, got); err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(got, record) {
			t.Fatalf("peer received different bytes than written")
		}
	})
}

func TestResilientTlsConnNonHandshakeRecordShortWriteFailsConnection(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildRawRecord(TlsContentTypeApplicationData, []byte("application data payload"))
		client, _ := newTcpPairOnFamily(t, ipVersion)

		// short-write (nil error, n < len) on the very first flush of the raw
		// record must be treated as a failure, not a successful send.
		wrapped := &shortWriteConn{Conn: client, shortAt: 1, shortN: 3}
		rconn := NewResilientTlsConn(wrapped, true, true)

		n, err := rconn.Write(record)
		// The flush branch's short-write check must surface a non-nil error
		// (io.ErrShortWrite) when the write comes up short with a nil error, so
		// the caller never mistakes a closed connection for success.
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("write error = %v, want io.ErrShortWrite", err)
		}
		if n != 0 {
			t.Fatalf("write n=%d want 0", n)
		}
		if rconn.Enabled() {
			t.Fatalf("layer still enabled after short-write flush failure")
		}
		if len(rconn.buffer) != 0 {
			t.Fatalf("buffer not dropped after short-write flush failure: %d bytes", len(rconn.buffer))
		}
		if !wrapped.closed {
			t.Fatalf("underlying connection not closed after short-write flush failure")
		}
	})
}

func TestResilientTlsConnHandshakeWithoutClientHelloFlushSuccess(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildNonClientHelloHandshakeRecord()
		client, server := newTcpPairOnFamily(t, ipVersion)

		// a Handshake-content-type record that is not a ClientHello with SNI
		// must be flushed as a raw record rather than routed into the
		// fragment/reorder logic.
		rconn := NewResilientTlsConn(client, true, true)
		n, err := rconn.Write(record)
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if n != len(record) {
			t.Fatalf("write n=%d want %d", n, len(record))
		}

		got := make([]byte, len(record))
		if _, err := io.ReadFull(server, got); err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(got, record) {
			t.Fatalf("peer received different bytes than written")
		}
	})
}

func TestResilientTlsConnHandshakeWithoutClientHelloShortWriteFailsConnection(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildNonClientHelloHandshakeRecord()
		client, _ := newTcpPairOnFamily(t, ipVersion)

		wrapped := &shortWriteConn{Conn: client, shortAt: 1, shortN: 2}
		rconn := NewResilientTlsConn(wrapped, true, true)

		n, err := rconn.Write(record)
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("write error = %v, want io.ErrShortWrite", err)
		}
		if n != 0 {
			t.Fatalf("write n=%d want 0", n)
		}
		if rconn.Enabled() {
			t.Fatalf("layer still enabled after short-write flush failure")
		}
		if len(rconn.buffer) != 0 {
			t.Fatalf("buffer not dropped after short-write flush failure: %d bytes", len(rconn.buffer))
		}
		if !wrapped.closed {
			t.Fatalf("underlying connection not closed after short-write flush failure")
		}
	})
}

func TestResilientTlsConnTcpNeitherFragmentNorReorderSuccess(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, server := newTcpPairOnFamily(t, ipVersion)

		// fragment=false, reorder=false with a *net.TCPConn takes the plain
		// tcpConn.Write(record) branch: no ttl/fd manipulation, no splitting.
		rconn := NewResilientTlsConn(client, false, false)
		n, err := rconn.Write(record)
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if n != len(record) {
			t.Fatalf("write n=%d want %d", n, len(record))
		}

		got := make([]byte, len(record))
		if _, err := io.ReadFull(server, got); err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(got, record) {
			t.Fatalf("peer received different bytes than written")
		}
	})
}

func TestResilientTlsConnTcpNeitherFragmentNorReorderFailureClosesConnection(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, _ := newTcpPairOnFamily(t, ipVersion)

		client.SetWriteDeadline(time.Now().Add(-time.Second))
		rconn := NewResilientTlsConn(client, false, false)

		if _, err := rconn.Write(record); err == nil {
			t.Fatalf("write with expired deadline: expected error, got nil")
		}
		if rconn.Enabled() {
			t.Fatalf("layer still enabled after write failure")
		}
		if len(rconn.buffer) != 0 {
			t.Fatalf("buffer not dropped after write failure: %d bytes", len(rconn.buffer))
		}

		client.SetWriteDeadline(time.Time{})
		if _, err := rconn.Write(record); err == nil {
			t.Fatalf("write after failure: expected error (conn closed), got nil")
		}
	})
}

func TestResilientTlsConnNonTCPConnNoFragmentSuccess(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, server := newTcpPairOnFamily(t, ipVersion)

		// self.conn is not a *net.TCPConn and fragment is false: Write must
		// take the self.conn.Write(record) fallback (net_resilient.go's
		// "else" branch under "if self.fragment {...} else {...}" for the
		// non-TCPConn path), sending the whole record as one call.
		wrapped := &countingFailConn{Conn: client}
		rconn := NewResilientTlsConn(wrapped, false, true)

		n, err := rconn.Write(record)
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if n != len(record) {
			t.Fatalf("write n=%d want %d", n, len(record))
		}
		if wrapped.calls != 1 {
			t.Fatalf("expected exactly one forwarded write, got %d", wrapped.calls)
		}

		got := make([]byte, len(record))
		if _, err := io.ReadFull(server, got); err != nil {
			t.Fatalf("read: %v", err)
		}
		if !bytes.Equal(got, record) {
			t.Fatalf("peer received different bytes than written")
		}
	})
}

func TestResilientTlsConnNonTCPConnNoFragmentShortWriteFailsConnection(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, _ := newTcpPairOnFamily(t, ipVersion)

		wrapped := &shortWriteConn{Conn: client, shortAt: 1, shortN: 4}
		rconn := NewResilientTlsConn(wrapped, false, true)

		n, err := rconn.Write(record)
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("write error = %v, want io.ErrShortWrite", err)
		}
		if n != 0 {
			t.Fatalf("write n=%d want 0", n)
		}
		if rconn.Enabled() {
			t.Fatalf("layer still enabled after non-TCPConn short-write failure")
		}
		if len(rconn.buffer) != 0 {
			t.Fatalf("buffer not dropped after non-TCPConn short-write failure: %d bytes", len(rconn.buffer))
		}
		if !wrapped.closed {
			t.Fatalf("underlying connection not closed after non-TCPConn short-write failure")
		}
	})
}

func TestResilientTlsConnOffDrainFailureClosesConnection(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, _ := newTcpPairOnFamily(t, ipVersion)

		// Fail the drain write through the wrapper rather than pre-closing the
		// conn: pre-closing makes the close that Off performs unobservable, so
		// the test could not distinguish a fail-closed Off from one that leaves
		// the connection open.
		failErr := errors.New("drain failed")
		wrapped := &countingFailConn{Conn: client, failAt: 1, failErr: failErr}
		rconn := NewResilientTlsConn(wrapped, true, false)

		partial := record[:15]
		n, err := rconn.Write(partial)
		if err != nil {
			t.Fatalf("write: %v", err)
		}
		if n != len(partial) {
			t.Fatalf("write n=%d want %d", n, len(partial))
		}
		if len(rconn.buffer) != len(partial) {
			t.Fatalf("buffered %d bytes, want %d", len(rconn.buffer), len(partial))
		}

		// A failed drain must surface as a non-nil error, not a silent success:
		// the caller hands the connection back as established, so a silent
		// failure would present a closed conn as live.
		if err := rconn.Off(); err == nil {
			t.Fatalf("Off: expected drain error, got nil")
		}

		if rconn.Enabled() {
			t.Fatalf("layer still enabled after Off")
		}
		// A failed drain leaves the wire state indeterminate: the connection is
		// closed and the buffer cleared so nothing can be appended to the
		// partial stream.
		if len(rconn.buffer) != 0 {
			t.Fatalf("buffer not cleared after a failed drain: got %d bytes", len(rconn.buffer))
		}

		// The layer must also close the underlying connection, so a later
		// write fails instead of appending to the corrupt stream. Without this
		// the test would pass even if failConnection stopped closing.
		if !wrapped.closed {
			t.Fatalf("underlying connection not closed after a failed write")
		}
	})
}

// TestResilientTlsConnNonTCPConnFragmentShortWrite verifies a short write
// with a nil error on the fragment path fails the connection and returns a
// non-nil error (io.ErrShortWrite), never a nil error after closing.
func TestResilientTlsConnNonTCPConnFragmentShortWrite(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		record := buildClientHelloRecord(t)
		client, _ := newTcpPairOnFamily(t, ipVersion)

		wrapped := &countingFailConn{Conn: client, shortWriteAt: 1, shortN: 1}
		rconn := NewResilientTlsConn(wrapped, true, false)

		_, err := rconn.Write(record)
		if err == nil {
			t.Fatalf("short write: expected non-nil error, got nil")
		}
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("short write error = %v, want io.ErrShortWrite", err)
		}
		if rconn.Enabled() {
			t.Fatalf("layer still enabled after short write")
		}
		if len(rconn.buffer) != 0 {
			t.Fatalf("buffer not dropped after short write: %d bytes", len(rconn.buffer))
		}

		// The layer must also close the underlying connection, so a later
		// write fails instead of appending to the corrupt stream. Without this
		// the test would pass even if failConnection stopped closing.
		if !wrapped.closed {
			t.Fatalf("underlying connection not closed after a failed write")
		}
	})
}

// TestResilientTlsConnNonTCPConnRawRecordShortWrite verifies a short write
// on the raw-record flush path (non-handshake content type) fails the
// connection and returns io.ErrShortWrite. The test uses a plain TLS
// record with an application-data content type so it takes the raw flush
// path rather than the fragment path.
func TestResilientTlsConnNonTCPConnRawRecordShortWrite(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		// Build a non-handshake TLS record: content type 23 (application data)
		// so Write takes the raw flush path.
		payload := []byte("hello world")
		record := make([]byte, 0, 5+len(payload))
		record = append(record, 23)
		record = append(record, 0x03, 0x03)
		record = append(record, byte(len(payload)>>8), byte(len(payload)))
		record = append(record, payload...)

		client, _ := newTcpPairOnFamily(t, ipVersion)
		wrapped := &countingFailConn{Conn: client, shortWriteAt: 1, shortN: 2}
		rconn := NewResilientTlsConn(wrapped, true, false)

		_, err := rconn.Write(record)
		if err == nil {
			t.Fatalf("short write: expected non-nil error, got nil")
		}
		if !errors.Is(err, io.ErrShortWrite) {
			t.Fatalf("short write error = %v, want io.ErrShortWrite", err)
		}
		if rconn.Enabled() {
			t.Fatalf("layer still enabled after short write")
		}

		// The layer must also close the underlying connection, so a later
		// write fails instead of appending to the corrupt stream. Without this
		// the test would pass even if failConnection stopped closing.
		if !wrapped.closed {
			t.Fatalf("underlying connection not closed after a failed write")
		}
	})
}
