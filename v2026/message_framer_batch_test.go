// This file verifies that stream batching preserves the established Framer
// wire format, bounds malformed input, and performs one underlying write.
package connect

import (
	"bytes"
	"errors"
	"testing"
)

// One recording writer counts complete stream handoffs.
type framerBatchTestWriter struct {
	bytes.Buffer
	writeCount int
}

// A framerDiscardWriter consumes bytes without retaining or allocating them.
type framerDiscardWriter struct{}

// Write consumes a complete batch in one call.
func (self *framerDiscardWriter) Write(buffer []byte) (int, error) {
	return len(buffer), nil
}

// Write records one underlying handoff while retaining its bytes.
func (self *framerBatchTestWriter) Write(buffer []byte) (int, error) {
	self.writeCount += 1
	return self.Buffer.Write(buffer)
}

// TestFramerWriteBatchPreservesMessages verifies compatibility with the
// existing singular reader and exactly one underlying stream write.
func TestFramerWriteBatchPreservesMessages(t *testing.T) {
	settings := DefaultFramerSettings(64 * 1024)
	framer := NewFramer(settings)
	writer := &framerBatchTestWriter{}
	messages := [][]byte{
		[]byte("one"),
		bytes.Repeat([]byte{0x22}, 2048),
		[]byte("three"),
	}
	if err := framer.WriteBatch(writer, messages); err != nil {
		t.Fatal(err)
	}
	if writer.writeCount != 1 {
		t.Fatalf("write count=%d want=1", writer.writeCount)
	}
	for messageIndex, expected := range messages {
		actual, err := framer.Read(&writer.Buffer)
		if err != nil {
			t.Fatalf("read message %d: %s", messageIndex, err)
		}
		if !bytes.Equal(actual, expected) {
			MessagePoolReturn(actual)
			t.Fatalf("message %d changed", messageIndex)
		}
		MessagePoolReturn(actual)
	}
	if writer.Buffer.Len() != 0 {
		t.Fatalf("unread wire bytes=%d", writer.Buffer.Len())
	}
}

// Caller-owned storage preserves the ordinary batch wire without allocating
// per ready batch.
func TestFramerWriteBatchWithStoragePreservesMessages(t *testing.T) {
	settings := DefaultFramerSettings(64 * 1024)
	framer := NewFramer(settings)
	writer := &framerBatchTestWriter{}
	messages := [][]byte{
		[]byte("one"),
		bytes.Repeat([]byte{0x44}, 2048),
		[]byte("three"),
	}
	storage := make([]byte, 64*1024)
	if err := framer.WriteBatchWithStorage(writer, messages, storage); err != nil {
		t.Fatal(err)
	}
	if writer.writeCount != 1 {
		t.Fatalf("write count=%d, want=1", writer.writeCount)
	}
	for messageIndex, expected := range messages {
		actual, err := framer.Read(&writer.Buffer)
		if err != nil {
			t.Fatalf("read message %d: %v", messageIndex, err)
		}
		if !bytes.Equal(actual, expected) {
			MessagePoolReturn(actual)
			t.Fatalf("message %d changed", messageIndex)
		}
		MessagePoolReturn(actual)
	}
}

// An undersized scratch buffer is rejected before the writer sees a partial
// batch, so a caller can fail the whole ownership unit deterministically.
func TestFramerWriteBatchWithStorageRejectsInsufficientStorage(t *testing.T) {
	framer := NewFramer(DefaultFramerSettings(1024))
	writer := &framerBatchTestWriter{}
	messages := [][]byte{[]byte("one"), []byte("two")}
	if err := framer.WriteBatchWithStorage(writer, messages, make([]byte, 13)); err == nil {
		t.Fatal("undersized batch storage was accepted")
	}
	if writer.writeCount != 0 || writer.Buffer.Len() != 0 {
		t.Fatalf(
			"rejected storage wrote count=%d bytes=%d",
			writer.writeCount,
			writer.Buffer.Len(),
		)
	}
}

// Reusing one writer-owned buffer keeps the saturated H3 framing boundary at
// zero steady-state Go allocations.
func TestFramerWriteBatchWithStorageDoesNotAllocate(t *testing.T) {
	framer := NewFramer(DefaultFramerSettings(2048))
	writer := &framerDiscardWriter{}
	payload := make([]byte, 1380)
	messages := [][]byte{payload, payload, payload, payload}
	storage := make([]byte, 64*1024)
	allocationCount := testing.AllocsPerRun(100, func() {
		if err := framer.WriteBatchWithStorage(writer, messages, storage); err != nil {
			panic(err)
		}
	})
	if allocationCount != 0 {
		t.Fatalf("steady-state allocations=%f, want=0", allocationCount)
	}
}

// TestFramerWriteBatchRejectsOversizedMessage verifies validation occurs
// before any prefix from the batch reaches the stream.
func TestFramerWriteBatchRejectsOversizedMessage(t *testing.T) {
	framer := NewFramer(DefaultFramerSettings(8))
	writer := &framerBatchTestWriter{}
	err := framer.WriteBatch(writer, [][]byte{
		[]byte("small"),
		bytes.Repeat([]byte{0x33}, 9),
	})
	if err == nil {
		t.Fatal("oversized batch was accepted")
	}
	if writer.writeCount != 0 || writer.Buffer.Len() != 0 {
		t.Fatalf(
			"rejected batch wrote count=%d bytes=%d",
			writer.writeCount,
			writer.Buffer.Len(),
		)
	}
}

// framerFullWriteErrorWriter models the legal io.Writer result that consumes
// every byte while still reporting a terminal error.
type framerFullWriteErrorWriter struct {
	err error
}

// Write reports the configured error together with full byte progress.
func (self *framerFullWriteErrorWriter) Write(buffer []byte) (int, error) {
	return len(buffer), self.err
}

// TestFramerWritesPreserveFullProgressError ensures batching does not hide a
// stream failure merely because the final write also consumed its bytes.
func TestFramerWritesPreserveFullProgressError(t *testing.T) {
	writeErr := errors.New("injected full-progress write failure")
	writer := &framerFullWriteErrorWriter{err: writeErr}
	framer := NewFramer(DefaultFramerSettings(1024))
	if err := framer.Write(writer, []byte("one")); !errors.Is(err, writeErr) {
		t.Fatalf("singular write error=%v", err)
	}
	if err := framer.WriteBatch(writer, [][]byte{
		[]byte("one"),
		[]byte("two"),
	}); !errors.Is(err, writeErr) {
		t.Fatalf("batch write error=%v", err)
	}
}
