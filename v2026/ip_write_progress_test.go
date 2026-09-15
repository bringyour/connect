package connect

// The user-nat's upstream write deadline must bound ZERO-progress time, not
// total batch time. See writeWithProgressDeadline.
//
// This is deliberately a unit test over a fake conn rather than a slow-socket
// integration test: the discriminating window for a real socket sits between
// "the kernel absorbs the whole transfer" (which never exercises the deadline)
// and "the peer stalls past the budget" (which correctly fails either version),
// and its width depends on the host's socket buffer auto-tuning. A test tuned
// to that window on one machine is a flake on the next.

import (
	"errors"
	"net"
	"testing"
	"time"
)

// progressConn models a peer that drains steadily but slowly: every write
// accepts a fixed amount and then reports a timeout, so each call makes
// progress and no call completes the batch.
type progressConn struct {
	net.Conn
	acceptPerWrite int
	written        int
	writeCalls     int
	// stalled models a peer that has stopped accepting data entirely: every
	// write reports a timeout with NO progress
	stalled bool
}

type writeTimeoutError struct{}

func (writeTimeoutError) Error() string   { return "i/o timeout" }
func (writeTimeoutError) Timeout() bool   { return true }
func (writeTimeoutError) Temporary() bool { return true }

func (self *progressConn) Write(b []byte) (int, error) {
	self.writeCalls += 1
	if self.stalled {
		return 0, writeTimeoutError{}
	}
	n := min(self.acceptPerWrite, len(b))
	self.written += n
	if n < len(b) {
		return n, writeTimeoutError{}
	}
	return n, nil
}

func (self *progressConn) SetWriteDeadline(t time.Time) error { return nil }

// TestWriteProgressDeadlineSurvivesSlowPeer is the regression test for
// mid-stream flow resets against a slow-but-alive upstream.
//
// Ordinary flow control — a peer accepting data steadily but slowly, which is
// what a saturated tunnel produces on the return path — makes a batch take
// longer than the deadline without ever stalling. Failing it reset the flow
// mid-transfer; only a peer accepting NOTHING for a full timeout may fail it.
//
// Without the fix the first partial write's timeout fails the whole batch.
func TestWriteProgressDeadlineSurvivesSlowPeer(t *testing.T) {
	payload := make([]byte, 64*1024)
	conn := &progressConn{acceptPerWrite: 4 * 1024}

	n, err := writeWithProgressDeadline(
		conn,
		net.Buffers{payload},
		100*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("a peer that keeps accepting data must not fail the flow: %s (wrote %d of %d)", err, n, len(payload))
	}
	if n != int64(len(payload)) {
		t.Fatalf("wrote %d of %d bytes", n, len(payload))
	}
	// the point of the fix: the batch spans several deadline windows, one per
	// progress step
	if conn.writeCalls < 2 {
		t.Fatalf("write calls = %d, want the batch split across several deadline windows", conn.writeCalls)
	}
}

// TestWriteProgressDeadlineFailsStalledPeer is the other half of the contract:
// a peer that accepts nothing must still fail the flow, or a dead upstream
// would hold it open forever.
func TestWriteProgressDeadlineFailsStalledPeer(t *testing.T) {
	payload := make([]byte, 64*1024)
	conn := &progressConn{acceptPerWrite: 4 * 1024, stalled: true}

	n, err := writeWithProgressDeadline(
		conn,
		net.Buffers{payload},
		10*time.Millisecond,
	)
	if err == nil {
		t.Fatal("a peer accepting nothing for a full timeout must fail the flow")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected a timeout error, got %T %v", err, err)
	}
	if n != 0 {
		t.Fatalf("wrote %d bytes to a stalled peer", n)
	}
}
