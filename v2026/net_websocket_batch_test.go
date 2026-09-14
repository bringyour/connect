// WebSocket write-batch tests pin pass-through, byte identity, buffer bounds,
// terminal write behavior, and steady-state allocation.
package connect

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

type recordingWriteConn struct {
	writes     [][]byte
	writeErr   error
	shortWrite bool
}

func (self *recordingWriteConn) Read(buffer []byte) (int, error) {
	return 0, io.EOF
}

func (self *recordingWriteConn) Write(buffer []byte) (int, error) {
	self.writes = append(self.writes, bytes.Clone(buffer))
	if self.writeErr != nil {
		return 0, self.writeErr
	}
	if self.shortWrite && 0 < len(buffer) {
		return len(buffer) - 1, nil
	}
	return len(buffer), nil
}

func (self *recordingWriteConn) Close() error {
	return nil
}

func (self *recordingWriteConn) LocalAddr() net.Addr {
	return nil
}

func (self *recordingWriteConn) RemoteAddr() net.Addr {
	return nil
}

func (self *recordingWriteConn) SetDeadline(deadline time.Time) error {
	return nil
}

func (self *recordingWriteConn) SetReadDeadline(deadline time.Time) error {
	return nil
}

func (self *recordingWriteConn) SetWriteDeadline(deadline time.Time) error {
	return nil
}

func TestWebSocketWriteBatchPassesThroughBeforeBegin(t *testing.T) {
	underlying := &recordingWriteConn{}
	conn := NewWebSocketWriteBatchConn(underlying)
	message := []byte("upgrade handshake")

	n, err := conn.Write(message)
	if err != nil {
		t.Fatal(err)
	}
	AssertEqual(t, n, len(message))
	AssertEqual(t, len(underlying.writes), 1)
	if !bytes.Equal(underlying.writes[0], message) {
		t.Fatal("pass-through write changed the handshake bytes")
	}
}

func TestWebSocketWriteBatchCoalescesWithoutChangingBytes(t *testing.T) {
	underlying := &recordingWriteConn{}
	conn := NewWebSocketWriteBatchConn(underlying)
	messages := [][]byte{
		[]byte("first frame"),
		[]byte("second frame"),
		[]byte("third frame"),
		[]byte("fourth frame"),
	}

	conn.BeginWriteBatch()
	for _, message := range messages {
		if _, err := conn.Write(message); err != nil {
			t.Fatal(err)
		}
	}
	if len(underlying.writes) != 0 {
		t.Fatal("batch wrote before its explicit flush boundary")
	}
	if err := conn.FlushWriteBatch(); err != nil {
		t.Fatal(err)
	}
	AssertEqual(t, len(underlying.writes), 1)
	if !bytes.Equal(underlying.writes[0], bytes.Join(messages, nil)) {
		t.Fatal("coalesced write changed byte order or content")
	}
}

// A WebSocket reader may send a control frame while the data writer starts a
// ready batch. Both accesses meet at one barrier so the race detector sees the
// old unsynchronized batching field on every run.
func TestWebSocketWriteBatchSerializesConcurrentControlWrite(t *testing.T) {
	underlying := &recordingWriteConn{}
	conn := NewWebSocketWriteBatchConn(underlying)
	beginReady := make(chan struct{})
	writeReady := make(chan struct{})
	release := make(chan struct{})
	conn.beforeBeginWriteBatchForTest = func() {
		close(beginReady)
		<-release
	}
	conn.beforeWriteForTest = func() {
		close(writeReady)
		<-release
	}

	var workers sync.WaitGroup
	workers.Add(2)
	go func() {
		defer workers.Done()
		conn.BeginWriteBatch()
	}()
	control := []byte("websocket control frame")
	var writtenByteCount int
	var writeErr error
	go func() {
		defer workers.Done()
		writtenByteCount, writeErr = conn.Write(control)
	}()
	<-beginReady
	<-writeReady
	close(release)
	workers.Wait()
	if writeErr != nil {
		t.Fatal(writeErr)
	}
	if writtenByteCount != len(control) {
		t.Fatalf("control write byte count=%d, want=%d", writtenByteCount, len(control))
	}
	if err := conn.FlushWriteBatch(); err != nil {
		t.Fatal(err)
	}

	joinedBytes := bytes.Join(underlying.writes, nil)
	if !bytes.Equal(joinedBytes, control) {
		t.Fatalf("control write changed at concurrent batch boundary: %q", joinedBytes)
	}
}

func TestWebSocketWriteBatchBoundsRetainedBuffer(t *testing.T) {
	underlying := &recordingWriteConn{}
	conn := NewWebSocketWriteBatchConn(underlying)
	first := bytes.Repeat([]byte{0x11}, 10*1024)
	second := bytes.Repeat([]byte{0x22}, 10*1024)

	conn.BeginWriteBatch()
	if _, err := conn.Write(first); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(second); err != nil {
		t.Fatal(err)
	}
	if err := conn.FlushWriteBatch(); err != nil {
		t.Fatal(err)
	}

	AssertEqual(t, cap(conn.writeBuffer), webSocketWriteBatchMaxByteCount)
	AssertEqual(t, len(underlying.writes), 2)
	if !bytes.Equal(underlying.writes[0], first) ||
		!bytes.Equal(underlying.writes[1], second) {
		t.Fatal("bounded flush changed byte order or content")
	}
}

func TestWebSocketWriteBatchAbortDropsUnflushedBytes(t *testing.T) {
	underlying := &recordingWriteConn{}
	conn := NewWebSocketWriteBatchConn(underlying)

	conn.BeginWriteBatch()
	if _, err := conn.Write([]byte("retired connection data")); err != nil {
		t.Fatal(err)
	}
	conn.AbortWriteBatch()
	if err := conn.FlushWriteBatch(); err != nil {
		t.Fatal(err)
	}
	if len(underlying.writes) != 0 {
		t.Fatal("aborted batch reached the retired connection")
	}
}

// A delegated flush error ends the batch and cannot retain bytes for a later
// connection write.
func TestWebSocketWriteBatchFlushErrorEndsBatch(t *testing.T) {
	writeErr := errors.New("injected batch flush error")
	underlying := &recordingWriteConn{writeErr: writeErr}
	conn := NewWebSocketWriteBatchConn(underlying)

	conn.BeginWriteBatch()
	if _, err := conn.Write([]byte("complete frame")); err != nil {
		t.Fatal(err)
	}
	if err := conn.FlushWriteBatch(); !errors.Is(err, writeErr) {
		t.Fatalf("flush error = %v, want %v", err, writeErr)
	}
	underlying.writeErr = nil
	if _, err := conn.Write([]byte("replacement connection frame")); err != nil {
		t.Fatal(err)
	}
	if len(underlying.writes) != 2 {
		t.Fatalf("delegated write count = %d, want 2", len(underlying.writes))
	}
	if !bytes.Equal(underlying.writes[1], []byte("replacement connection frame")) {
		t.Fatal("failed batch bytes escaped into the later pass-through write")
	}
}

// A short delegated write is terminal even when the connection reports no
// explicit error.
func TestWebSocketWriteBatchFlushRejectsShortWrite(t *testing.T) {
	underlying := &recordingWriteConn{shortWrite: true}
	conn := NewWebSocketWriteBatchConn(underlying)

	conn.BeginWriteBatch()
	if _, err := conn.Write([]byte("complete frame")); err != nil {
		t.Fatal(err)
	}
	if err := conn.FlushWriteBatch(); !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("flush error = %v, want %v", err, io.ErrShortWrite)
	}
}

func TestWebSocketWriteBatchSteadyStateDoesNotAllocate(t *testing.T) {
	underlying := &immediateWriteConn{}
	conn := NewWebSocketWriteBatchConn(underlying)
	message := make([]byte, 3*1024)
	conn.BeginWriteBatch()
	conn.AbortWriteBatch()

	allocCount := testing.AllocsPerRun(1_000, func() {
		conn.BeginWriteBatch()
		for range platformWebSocketWriteBatchMaxMessages {
			if _, err := conn.Write(message); err != nil {
				panic(err)
			}
		}
		if err := conn.FlushWriteBatch(); err != nil {
			panic(err)
		}
	})
	AssertEqual(t, allocCount, float64(0))
}

func TestPlatformWebSocketReadyDrainBalancesAcksAndPayloadBytes(t *testing.T) {
	readyCount := func(messageByteCount int) (count int, byteCount int) {
		count = 1
		byteCount = messageByteCount
		for platformWebSocketWriteBatchCanDrain(count, byteCount) {
			count += 1
			byteCount += messageByteCount
		}
		return
	}

	if count, bytes := readyCount(128); count != 32 || bytes != 4*1024 {
		t.Fatalf("ACK-sized ready drain=(%d, %dB), want (32, 4096B)", count, bytes)
	}
	if count, bytes := readyCount(DefaultMtu); count != 12 ||
		bytes >= webSocketWriteBatchMaxByteCount {
		t.Fatalf("tunnel-MTU ready drain=(%d, %dB), want 12 below 16KiB", count, bytes)
	}
	if count, bytes := readyCount(4 * 1024); count != 3 ||
		bytes != platformWebSocketWriteBatchDrainByteCount {
		t.Fatalf("maximum H1 ready drain=(%d, %dB), want (3, 12288B)", count, bytes)
	}
}

func BenchmarkWebSocketWriteBatchReadyMessages(b *testing.B) {
	underlying := &immediateWriteConn{}
	conn := NewWebSocketWriteBatchConn(underlying)
	message := make([]byte, 3*1024)
	conn.BeginWriteBatch()
	conn.AbortWriteBatch()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		conn.BeginWriteBatch()
		for range platformWebSocketWriteBatchMaxMessages {
			if _, err := conn.Write(message); err != nil {
				b.Fatal(err)
			}
		}
		if err := conn.FlushWriteBatch(); err != nil {
			b.Fatal(err)
		}
	}
}
