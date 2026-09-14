//go:build unix || windows

package connect

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// ttlSeam stands in for the SetSocketTtl syscall on a single
// ResilientTlsConn, so the reorder paths' TTL choreography can be observed and
// made to fail without a router in the way. It is installed on the instance's
// setTtl field, which scopes it to the one connection under test and lets each
// test pick its own failure policy.
//
// The seam is only ever called from the goroutine running Write, so it needs no
// locking; the tests below never write concurrently.
type ttlSeam struct {
	// applied lists the TTLs the socket actually took, in order. A refused
	// value is deliberately not recorded. Recording the attempt instead would
	// make "the sequence starts at the low TTL" hold even on a platform that
	// rejects that TTL outright — which is precisely the bug these tests
	// exist to catch, since the assertion would then pass against a socket
	// whose TTL never moved.
	applied []int
	// failOn maps a TTL to the error to return instead of applying it, so a
	// test can refuse exactly one value in the sequence.
	failOn map[int]error
	// passthrough performs the real SetSocketTtl for every value not in
	// failOn. Without it a test only proves the production code called the
	// seam; with it the test also proves the kernel accepts the value the
	// production code chose, which is the half that fails on Linux with a
	// low TTL of 0.
	passthrough bool
}

// set is the seam function installed on ResilientTlsConn.setTtl.
func (self *ttlSeam) set(fd SocketHandle, ttl int) error {
	if err, ok := self.failOn[ttl]; ok {
		return err
	}
	if self.passthrough {
		if err := SetSocketTtl(fd, ttl); err != nil {
			return err
		}
	}
	self.applied = append(self.applied, ttl)
	return nil
}

// TestResilientLowTtlIsOne pins the low TTL to the only value that does
// anything, and is the assertion that catches a silent regression on every
// platform. An in-range 1..255 check is exactly as permissive as IP_TTL itself
// and pins nothing: any value above 1 survives the first L3 hop, so the
// fragment arrives first try, nothing is retransmitted and nothing arrives out
// of order — the technique is as inert at 100 as it was at 0, only without a
// syscall error to hint at it. And 0 is not emittable at all. That leaves 1.
func TestResilientLowTtlIsOne(t *testing.T) {
	if resilientLowTtl != 1 {
		t.Fatalf("resilientLowTtl = %d, want exactly 1: a higher TTL survives the first L3 hop and reorders nothing, so the reorder technique is silently inert", resilientLowTtl)
	}
}

// TestSetSocketTtlReportsRejection is the direct regression test for the
// syscall wrapper. It asserts both halves that the discarded error hid: that a
// rejected TTL is reported at all, and that the value the production path
// actually uses is accepted and reads back. The second assertion is the one
// that fails on Linux with a low TTL of 0.
func TestSetSocketTtlReportsRejection(t *testing.T) {
	client, _ := newTcpPair(t)
	f, err := client.File()
	if err != nil {
		t.Fatalf("file: %v", err)
	}
	defer f.Close()
	fd := SocketHandle(f.Fd())

	// 256 is out of range on every platform measured, so it exercises the
	// error return itself rather than a platform quirk.
	if err := SetSocketTtl(fd, 256); err == nil {
		t.Fatalf("SetSocketTtl(fd, 256) = nil, want an error (an out-of-range TTL must be reported, not discarded)")
	}

	if err := SetSocketTtl(fd, resilientLowTtl); err != nil {
		t.Fatalf("SetSocketTtl(fd, resilientLowTtl=%d) = %v, want nil (the reorder technique is inert if the kernel refuses this value)", resilientLowTtl, err)
	}
	if got := GetSocketTtl(fd); got != resilientLowTtl {
		t.Fatalf("socket TTL after setting resilientLowTtl = %d, want %d", got, resilientLowTtl)
	}
}

// TestResilientTlsConnFragmentReorderAppliesLowTtlAndRestores is the test the
// existing restore assertions cannot make: they check only the TTL left on the
// socket at the end, which is the native value whether the TTL ever moved or
// not. Recording the accepted sequence distinguishes "lowered then restored"
// from "never lowered", and the passthrough seam means the kernel has to accept
// the low TTL for the first entry to appear at all.
func TestResilientTlsConnFragmentReorderAppliesLowTtlAndRestores(t *testing.T) {
	record := buildClientHelloRecord(t)
	client, server := newTcpPair(t)
	// pin the native TTL so the assertions compare against a known value
	// instead of a per-platform default (64 on Linux, 128 on Windows)
	setSocketTtl(t, client, 42)
	nativeTtl := 42

	// a vacuous test otherwise: if the low and native TTLs were equal, the
	// first and last entries would match no matter what the code did
	if nativeTtl == resilientLowTtl {
		t.Fatalf("test setup: native TTL %d equals resilientLowTtl, the sequence assertions would be vacuous", nativeTtl)
	}

	seam := &ttlSeam{passthrough: true}
	rconn := NewResilientTlsConn(client, true, true)
	rconn.setTtl = seam.set

	n, err := rconn.Write(record)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if n != len(record) {
		t.Fatalf("write n=%d want %d", n, len(record))
	}
	if rconn.ttlErr != nil {
		t.Fatalf("ttlErr = %v, want nil (every TTL in the sequence should apply on a loopback socket)", rconn.ttlErr)
	}

	// The technique's whole purpose: the first fragment must go out at the low
	// TTL so it dies in flight and its retransmit arrives after the fragments
	// that followed it.
	if len(seam.applied) < 2 {
		t.Fatalf("applied TTL sequence = %v, want at least a low TTL and a restore", seam.applied)
	}
	if seam.applied[0] != resilientLowTtl {
		t.Fatalf("applied TTL sequence = %v, want it to begin with resilientLowTtl=%d (the socket TTL never actually dropped, so the reorder technique did nothing)", seam.applied, resilientLowTtl)
	}
	// The sequence must end native, otherwise every packet after the
	// handshake fragments leaves at the low TTL and is discarded at the first
	// L3 hop.
	if last := seam.applied[len(seam.applied)-1]; last != nativeTtl {
		t.Fatalf("applied TTL sequence = %v, want it to end with the native TTL %d", seam.applied, nativeTtl)
	}
	// and the socket really holds the native value, not just the seam's record
	if got := socketTtl(t, client); got != nativeTtl {
		t.Fatalf("socket TTL after fragment+reorder write = %d, want %d (native restored)", got, nativeTtl)
	}

	// The reordering must not cost any bytes: fragmentation re-frames the
	// payload into standalone TLS records, so the payloads concatenate back to
	// the original handshake bytes.
	got := readTlsRecords(t, server, len(record)-5)
	if !bytes.Equal(got, record[5:]) {
		t.Fatalf("peer received different payload than written")
	}
}

// TestResilientTlsConnReorderOnlyAlternatesTtl pins the full TTL choreography
// of the reorder-only path, which alternates by block index rather than
// randomly and is therefore exactly predictable. Asserting the whole sequence
// catches an alternation that collapses to a single TTL, which the
// end-state-only restore assertions cannot see.
func TestResilientTlsConnReorderOnlyAlternatesTtl(t *testing.T) {
	record := buildClientHelloRecord(t)
	client, server := newTcpPair(t)
	setSocketTtl(t, client, 42)
	nativeTtl := 42

	// mirrors the block size in net_resilient.go's reorder-only path. If the
	// two diverge this test fails on sequence length, which is the right
	// outcome: the alternation asserted here is defined by that boundary.
	const blockSize = 64

	seam := &ttlSeam{passthrough: true}
	rconn := NewResilientTlsConn(client, false, true)
	rconn.setTtl = seam.set

	n, err := rconn.Write(record)
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if n != len(record) {
		t.Fatalf("write n=%d want %d", n, len(record))
	}

	// the reorder-only path writes the record as raw blocks, so the block
	// count follows directly from the record length
	var want []int
	for i := 0; i*blockSize < len(record); i += 1 {
		if 0 == i%2 {
			want = append(want, resilientLowTtl)
		} else {
			want = append(want, nativeTtl)
		}
	}

	// The alternation across the block loop is pinned exactly. The tail is not:
	// the explicit restore and the deferred one both write the native TTL
	// today, so asserting a fixed count of trailing entries would fail anyone
	// who skips the redundant deferred write once the explicit one succeeded.
	// What matters is that at least one restore follows the loop and the socket
	// is left native.
	if len(seam.applied) < len(want)+1 {
		t.Fatalf("applied TTL sequence = %v, want the %d block TTLs %v followed by at least one restore", seam.applied, len(want), want)
	}
	for i := range want {
		if seam.applied[i] != want[i] {
			t.Fatalf("applied TTL sequence = %v, want it to begin %v (first difference at index %d)", seam.applied, want, i)
		}
	}
	for i := len(want); i < len(seam.applied); i += 1 {
		if seam.applied[i] != nativeTtl {
			t.Fatalf("applied TTL sequence = %v, want only native-TTL restores after the %d block TTLs; index %d is %d", seam.applied, len(want), i, seam.applied[i])
		}
	}

	// the reorder-only path does not re-frame the record, so the peer must see
	// the exact original bytes
	got := make([]byte, len(record))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, record) {
		t.Fatalf("peer received different bytes than written")
	}
}

// TestResilientTlsConnLowTtlFailureStillWritesRecord locks in the non-fatal
// half of the asymmetric error policy, on the fragment+reorder path. A socket
// that refuses the low TTL — an
// AF_INET6 socket refusing IPPROTO_IP/IP_TTL, for instance — must still get a
// coherent record on the wire and a usable connection; only the reorder
// property is lost, and only ttlErr records that.
func TestResilientTlsConnLowTtlFailureStillWritesRecord(t *testing.T) {
	record := buildClientHelloRecord(t)
	client, server := newTcpPair(t)
	setSocketTtl(t, client, 42)

	// refuse only the low TTL; the native restores still succeed, so the
	// failure under test is isolated to the best-effort half
	lowTtlErr := errors.New("injected low ttl failure")
	seam := &ttlSeam{failOn: map[int]error{resilientLowTtl: lowTtlErr}}
	rconn := NewResilientTlsConn(client, true, true)
	rconn.setTtl = seam.set

	n, err := rconn.Write(record)
	if err != nil {
		t.Fatalf("write: %v (a refused low TTL must not fail the connection — that would trade a working connection for no connection)", err)
	}
	if n != len(record) {
		t.Fatalf("write n=%d want %d", n, len(record))
	}

	// the connection stays live: the fragments went out whole at the native
	// TTL, so nothing about the stream is indeterminate
	if !rconn.Enabled() {
		t.Fatalf("layer disabled after a refused low TTL, want still enabled")
	}
	if !errors.Is(rconn.ttlErr, lowTtlErr) {
		t.Fatalf("ttlErr = %v, want %v (the lost reorder property must be recorded even though it is not fatal)", rconn.ttlErr, lowTtlErr)
	}
	if len(seam.applied) == 0 {
		t.Fatalf("no TTL applied at all; the native restore should still have run")
	}

	// and the peer still gets every byte
	got := readTlsRecords(t, server, len(record)-5)
	if !bytes.Equal(got, record[5:]) {
		t.Fatalf("peer received different payload than written")
	}
}

// TestResilientTlsConnNativeTtlRestoreFailureFailsClosed locks in the fatal
// half, on the fragment+reorder path. A failed restore is not confined to one
// fragment: every later packet on the socket, the tail record and the rest of
// the handshake included, would leave at the low TTL and be discarded at the
// first L3 hop. A dial failure is strictly better than a handshake that hangs.
func TestResilientTlsConnNativeTtlRestoreFailureFailsClosed(t *testing.T) {
	record := buildClientHelloRecord(t)
	client, _ := newTcpPair(t)
	setSocketTtl(t, client, 42)
	nativeTtl := 42

	// refuse the native TTL. The mid-loop native writes are best effort and
	// only record, so the error Write returns can only have come from the
	// checked restore before the tail write.
	restoreErr := errors.New("injected native ttl restore failure")
	seam := &ttlSeam{failOn: map[int]error{nativeTtl: restoreErr}}
	rconn := NewResilientTlsConn(client, true, true)
	rconn.setTtl = seam.set

	_, err := rconn.Write(record)
	if !errors.Is(err, restoreErr) {
		t.Fatalf("write error = %v, want %v (a failed native restore must be surfaced, not swallowed)", err, restoreErr)
	}

	// fail closed: the layer is off, the buffered record is dropped so it is
	// never re-sent, and the connection is closed
	if rconn.Enabled() {
		t.Fatalf("layer still enabled after a failed native TTL restore")
	}
	if len(rconn.buffer) != 0 {
		t.Fatalf("buffer not dropped after a failed native TTL restore: %d bytes", len(rconn.buffer))
	}
	// the fatal failure must also land in ttlErr: a metric that reads it to
	// count lost reorder attempts would otherwise report zero for exactly the
	// TTL failures that killed connections
	if !errors.Is(rconn.ttlErr, restoreErr) {
		t.Fatalf("ttlErr = %v, want %v (the fatal TTL failure must be recorded, not only returned)", rconn.ttlErr, restoreErr)
	}
	if _, err := rconn.Write(record); err == nil {
		t.Fatalf("write after a failed native TTL restore: expected error (conn closed), got nil")
	}
}

// TestResilientTlsConnReorderOnlyLowTtlFailureStillWritesRecord is the
// reorder-only twin of the non-fatal half. Without it the mid-alternation write
// in the reorder-only path is untested policy: turning it into a checked,
// fail-closed restore breaks nothing that the fragment+reorder tests cover.
func TestResilientTlsConnReorderOnlyLowTtlFailureStillWritesRecord(t *testing.T) {
	record := buildClientHelloRecord(t)
	client, server := newTcpPair(t)
	setSocketTtl(t, client, 42)

	lowTtlErr := errors.New("injected low ttl failure")
	seam := &ttlSeam{failOn: map[int]error{resilientLowTtl: lowTtlErr}}
	rconn := NewResilientTlsConn(client, false, true)
	rconn.setTtl = seam.set

	n, err := rconn.Write(record)
	if err != nil {
		t.Fatalf("write: %v (a refused low TTL must not fail the connection on the reorder-only path either)", err)
	}
	if n != len(record) {
		t.Fatalf("write n=%d want %d", n, len(record))
	}
	if !rconn.Enabled() {
		t.Fatalf("layer disabled after a refused low TTL, want still enabled")
	}
	if !errors.Is(rconn.ttlErr, lowTtlErr) {
		t.Fatalf("ttlErr = %v, want %v", rconn.ttlErr, lowTtlErr)
	}

	// the reorder-only path writes raw blocks, so the peer sees the exact bytes
	got := make([]byte, len(record))
	if _, err := io.ReadFull(server, got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if !bytes.Equal(got, record) {
		t.Fatalf("peer received different bytes than written")
	}
}

// TestResilientTlsConnReorderOnlyNativeTtlRestoreFailureFailsClosed is the
// reorder-only twin of the fatal half, and the case where the fail-closed
// decision is least obvious: by the time this path restores, every block has
// been written, so the peer holds a complete and coherent record and nothing is
// indeterminate. The connection is closed anyway, purely because the socket's
// TTL is left wrong for everything the handshake still has to send. Without
// this test, downgrading that restore to best effort breaks nothing.
func TestResilientTlsConnReorderOnlyNativeTtlRestoreFailureFailsClosed(t *testing.T) {
	record := buildClientHelloRecord(t)
	client, _ := newTcpPair(t)
	setSocketTtl(t, client, 42)
	nativeTtl := 42

	restoreErr := errors.New("injected native ttl restore failure")
	seam := &ttlSeam{failOn: map[int]error{nativeTtl: restoreErr}}
	rconn := NewResilientTlsConn(client, false, true)
	rconn.setTtl = seam.set

	_, err := rconn.Write(record)
	if !errors.Is(err, restoreErr) {
		t.Fatalf("write error = %v, want %v (a failed native restore must be surfaced on the reorder-only path too)", err, restoreErr)
	}
	if rconn.Enabled() {
		t.Fatalf("layer still enabled after a failed native TTL restore")
	}
	if len(rconn.buffer) != 0 {
		t.Fatalf("buffer not dropped after a failed native TTL restore: %d bytes", len(rconn.buffer))
	}
	if !errors.Is(rconn.ttlErr, restoreErr) {
		t.Fatalf("ttlErr = %v, want %v", rconn.ttlErr, restoreErr)
	}
	if _, err := rconn.Write(record); err == nil {
		t.Fatalf("write after a failed native TTL restore: expected error (conn closed), got nil")
	}
}
