// Test-only connection reads that distinguish a live, slowly progressing
// stream from a zero-progress stall while retaining the caller's absolute cap.
package connect

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"time"
)

// Reads exactly one buffer while rearming the relative deadline only after
// bytes arrive. The caller deadline remains an absolute upper bound.
func readFullWithProgressDeadline(
	ctx context.Context,
	conn net.Conn,
	buffer []byte,
	progressTimeout time.Duration,
) (int, error) {
	readByteCount := 0
	for readByteCount < len(buffer) {
		if err := ctx.Err(); err != nil {
			return readByteCount, err
		}

		deadline, hasDeadline := ctx.Deadline()
		if 0 < progressTimeout {
			progressDeadline := time.Now().Add(progressTimeout)
			if !hasDeadline || progressDeadline.Before(deadline) {
				deadline = progressDeadline
				hasDeadline = true
			}
		}
		if hasDeadline {
			if err := conn.SetReadDeadline(deadline); err != nil {
				return readByteCount, err
			}
		}

		n, err := conn.Read(buffer[readByteCount:])
		if 0 < n {
			readByteCount += n
			if readByteCount == len(buffer) {
				return readByteCount, nil
			}
		}
		if err == nil {
			if n == 0 {
				return readByteCount, io.ErrNoProgress
			}
			continue
		}

		var netErr net.Error
		if 0 < n && errors.As(err, &netErr) && netErr.Timeout() {
			// The completed bytes prove the stream moved inside this window;
			// retry only the remaining suffix with a fresh progress deadline.
			continue
		}
		return readByteCount, err
	}
	return readByteCount, nil
}

// One scripted result returned by the deterministic connection below.
type syntheticProgressReadResult struct {
	bytes []byte
	err   error
}

// A non-concurrent fake that records deadlines and returns exact read results.
type syntheticProgressConn struct {
	net.Conn
	results        []syntheticProgressReadResult
	resultIndex    int
	readDeadlines  []time.Time
	writeCount     int
	writeDeadlines []time.Time
}

// Returns the next configured result without wall-clock scheduling.
func (self *syntheticProgressConn) Read(buffer []byte) (int, error) {
	if len(self.results) <= self.resultIndex {
		return 0, io.EOF
	}
	result := self.results[self.resultIndex]
	self.resultIndex += 1
	return copy(buffer, result.bytes), result.err
}

// Records the exact deadline selected by the helper.
func (self *syntheticProgressConn) SetReadDeadline(deadline time.Time) error {
	self.readDeadlines = append(self.readDeadlines, deadline)
	return nil
}

// Records an attempted write without using a real socket.
func (self *syntheticProgressConn) Write(buffer []byte) (int, error) {
	self.writeCount += 1
	return len(buffer), nil
}

// Records each write-deadline mutation without using wall-clock blocking.
func (self *syntheticProgressConn) SetWriteDeadline(deadline time.Time) error {
	self.writeDeadlines = append(self.writeDeadlines, deadline)
	return nil
}

// Timed progress must preserve its bytes and receive a fresh window for the
// remaining suffix.
func TestReadFullWithProgressDeadlineCompletesAfterTimedProgress(t *testing.T) {
	conn := &syntheticProgressConn{results: []syntheticProgressReadResult{
		{bytes: []byte("abc"), err: os.ErrDeadlineExceeded},
		{bytes: []byte("def")},
	}}
	buffer := make([]byte, 6)
	n, err := readFullWithProgressDeadline(context.Background(), conn, buffer, time.Minute)
	if err != nil || n != len(buffer) || string(buffer) != "abcdef" {
		t.Fatalf("progress read = %d, %q, %v; want 6, abcdef, nil", n, buffer, err)
	}
	if conn.resultIndex != 2 || len(conn.readDeadlines) != 2 {
		t.Fatalf("progress read operations = %d/%d, want 2/2", conn.resultIndex, len(conn.readDeadlines))
	}
}

// A timeout that returns no new bytes is the terminal stall discriminator.
func TestReadFullWithProgressDeadlineStopsAfterZeroProgressTimeout(t *testing.T) {
	conn := &syntheticProgressConn{results: []syntheticProgressReadResult{
		{bytes: []byte("abc"), err: os.ErrDeadlineExceeded},
		{err: os.ErrDeadlineExceeded},
		{bytes: []byte("must-not-be-read")},
	}}
	buffer := make([]byte, 6)
	n, err := readFullWithProgressDeadline(context.Background(), conn, buffer, time.Minute)
	if n != 3 || !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("zero-progress result = %d, %v; want 3, deadline exceeded", n, err)
	}
	if conn.resultIndex != 2 {
		t.Fatalf("zero-progress timeout launched read %d, want 2", conn.resultIndex)
	}
}

// The caller's absolute deadline must win over a longer progress window.
func TestReadFullWithProgressDeadlineUsesEarlierContextDeadline(t *testing.T) {
	ctxDeadline := time.Now().Add(time.Minute)
	ctx, cancel := context.WithDeadline(context.Background(), ctxDeadline)
	defer cancel()
	conn := &syntheticProgressConn{results: []syntheticProgressReadResult{{bytes: []byte("ok")}}}
	buffer := make([]byte, 2)
	n, err := readFullWithProgressDeadline(ctx, conn, buffer, time.Hour)
	if err != nil || n != len(buffer) {
		t.Fatalf("context-bounded read = %d, %v; want 2, nil", n, err)
	}
	if len(conn.readDeadlines) != 1 || !conn.readDeadlines[0].Equal(ctxDeadline) {
		t.Fatalf("read deadline = %v, want caller deadline %v", conn.readDeadlines, ctxDeadline)
	}
}

// An expired caller deadline must prevent every subsequent socket operation.
func TestProgressDeadlineHelpersRejectExpiredContextWithoutIo(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer cancel()
	conn := &syntheticProgressConn{results: []syntheticProgressReadResult{{bytes: []byte("must-not-be-read")}}}
	n, err := readFullWithProgressDeadline(ctx, conn, make([]byte, 1), time.Minute)
	if n != 0 || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired read = %d, %v; want 0, deadline exceeded", n, err)
	}
	if conn.resultIndex != 0 || len(conn.readDeadlines) != 0 {
		t.Fatalf("expired context performed read/deadline operations = %d/%d", conn.resultIndex, len(conn.readDeadlines))
	}

	err = writeConnPhaseWithDeadline(ctx, conn, []byte("must-not-be-written"), time.Minute)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired write = %v, want deadline exceeded", err)
	}
	if conn.writeCount != 0 || len(conn.writeDeadlines) != 0 {
		t.Fatalf("expired context performed write/deadline operations = %d/%d", conn.writeCount, len(conn.writeDeadlines))
	}
}

// A broken Reader returning no bytes and no error cannot spin the helper.
func TestReadFullWithProgressDeadlineRejectsZeroByteNil(t *testing.T) {
	conn := &syntheticProgressConn{results: []syntheticProgressReadResult{{}}}
	n, err := readFullWithProgressDeadline(context.Background(), conn, make([]byte, 1), time.Minute)
	if n != 0 || !errors.Is(err, io.ErrNoProgress) {
		t.Fatalf("zero-byte nil read = %d, %v; want 0, no progress", n, err)
	}
	if conn.resultIndex != 1 {
		t.Fatalf("zero-byte nil launched %d reads, want 1", conn.resultIndex)
	}
}

// EOF and non-timeout failures remain terminal even when they carry bytes.
func TestReadFullWithProgressDeadlineKeepsTerminalErrors(t *testing.T) {
	syntheticErr := errors.New("synthetic terminal read error")
	for _, candidateErr := range []error{io.EOF, syntheticErr} {
		conn := &syntheticProgressConn{results: []syntheticProgressReadResult{
			{bytes: []byte("abc"), err: candidateErr},
			{bytes: []byte("must-not-be-read")},
		}}
		n, err := readFullWithProgressDeadline(context.Background(), conn, make([]byte, 6), time.Minute)
		if n != 3 || !errors.Is(err, candidateErr) {
			t.Errorf("terminal read for %v = %d, %v; want 3 and the same error", candidateErr, n, err)
		}
		if conn.resultIndex != 1 {
			t.Errorf("terminal read for %v launched %d reads, want 1", candidateErr, conn.resultIndex)
		}
	}
}
