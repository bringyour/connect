package connect

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	// "time"
	// "github.com/urnetwork/connect/v2026"
)

// a message framer that optimizes memory copies to reduce cpu+memory usage
// on a typical connection, writing into the connection buffer will trigger a packet send
// and incur some fixed overhead
// to avoid small packets and excessive write calls, this framer approach breaks
// messages above a threshold into exactly two writes, resulting in an effective
// halving of the packet size on the wire
// the benefit of this approach is the framing can be done with zero additional memory allocation
// and a small constant memory copies before handing the message to the connection
// versus allocating and copying into a new framed message buffer,
// this approach is ~2x more cpu+memory efficient to send framed messages on a tcp/udp connection
// the framer read/write op is called billions of times in a typical user hour

type FramerSettings struct {
	// Log, when set, is used by the framer. nil resolves to `DefaultLogger()`.
	// The platform transport propagates its log here when nil.
	Log Logger

	// MaxMessageLen is the maximum message (payload) length, in bytes, this
	// framer will read or write. The on-wire frame is `MaxMessageLen + 4`:
	// the framer prepends a 4-byte length header and accounts for it
	// internally (see NewFramer). There is intentionally no global default
	// max -- every framer must declare the largest message its context can
	// carry (see DefaultFramerSettings), so a transport or relay hop cannot
	// silently inherit a cap too small for, e.g., the per-peer encryption
	// handshake (ClientSettings.MinimumMessageLenLimit).
	MaxMessageLen int
	// SplitMinimumLen is the minimum message length above which `Write`
	// splits the body into two `io.Writer.Write` calls to save a memcpy.
	// This is a stream-transport optimization.
	SplitMinimumLen int
}

// DefaultFramerSettings returns framer settings for a context whose maximum
// message (payload) length is maxMessageLen bytes. There is no default
// maxMessageLen: each call site must pass the largest message its context
// carries, making the cap explicit rather than an inherited global.
func DefaultFramerSettings(maxMessageLen int) *FramerSettings {
	return &FramerSettings{
		MaxMessageLen:   maxMessageLen,
		SplitMinimumLen: 256,
	}
}

// Read and Write must be called from a single goroutine each
type Framer struct {
	// maxFrameLen is the maximum on-wire frame length the framer reads or
	// writes: the configured max message (payload) length plus the 4-byte
	// length header it prepends.
	maxFrameLen int
	settings    *FramerSettings
	log         Logger
}

func NewFramer(settings *FramerSettings) *Framer {
	return &Framer{
		maxFrameLen: settings.MaxMessageLen + 4,
		settings:    settings,
		log:         loggerOrDefault(settings.Log),
	}
}

func (self *Framer) Read(r io.Reader) ([]byte, error) {
	var h [4]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return nil, err
	}

	messageLen := int(binary.BigEndian.Uint16(h[0:2]))

	if self.maxFrameLen < messageLen+4 {
		// Surface framer length rejection on the read path so an oversized frame
		// (e.g. an encryption handshake flight too large for a hop's cap) shows
		// up in logs rather than silently closing the transport.
		self.log.Infof(
			"[framer][reject]read messageLen=%d > MaxMessageLen=%d (maxFrameLen=%d)\n",
			messageLen, self.settings.MaxMessageLen, self.maxFrameLen,
		)
		return nil, fmt.Errorf("Max message len exceeded (%d<%d)", self.settings.MaxMessageLen, messageLen)
	}

	message := MessagePoolGet(messageLen)

	if _, err := io.ReadFull(r, message); err != nil {
		MessagePoolReturn(message)
		return nil, err
	}

	return message, nil
}

// Write emits a length-prefixed framed message to a stream writer (TCP,
// QUIC stream, WebSocket frame body, etc.). For messages at or above
// `SplitMinimumLen`, the body is written as two `io.Writer.Write` calls —
// header + first half, then second half — saving one memcpy of the second
// half. This is unsafe on packet transports because each Write becomes one
// packet on the wire and there is no in-band way to detect a dropped or
// reordered second packet; message-preserving transports should bypass
// the framer and write/read directly.
func (self *Framer) Write(w io.Writer, message []byte) error {
	messageLen := len(message)
	if self.maxFrameLen < messageLen+4 {
		// Surface framer length rejection on the write path so a component
		// trying to send a frame larger than its framer cap (the classic
		// encryption-handshake deadlock trigger) shows up in logs.
		self.log.Infof(
			"[framer][reject]write messageLen=%d > MaxMessageLen=%d (maxFrameLen=%d)\n",
			messageLen, self.settings.MaxMessageLen, self.maxFrameLen,
		)
		return fmt.Errorf("Max message len exceeded (%d<%d)", self.settings.MaxMessageLen, messageLen)
	}
	if math.MaxUint16 < messageLen {
		return fmt.Errorf("Max possible message len exceeded (%d<%d)", math.MaxUint16, messageLen)
	}
	if messageLen < max(16, self.settings.SplitMinimumLen) {
		messageWithHeader := MessagePoolGet(messageLen + 4)
		defer MessagePoolReturn(messageWithHeader)
		binary.BigEndian.PutUint16(messageWithHeader[0:2], uint16(messageLen))
		binary.BigEndian.PutUint16(messageWithHeader[2:4], uint16(0))
		copy(messageWithHeader[4:4+messageLen], message)
		if nw, writeErr := w.Write(messageWithHeader[0 : messageLen+4]); writeErr != nil {
			return writeErr
		} else if nw < messageLen+4 {
			return io.ErrShortWrite
		}
		return nil
	}
	splitIndex := messageLen / 2
	h := MessagePoolGet(splitIndex + 4)
	defer MessagePoolReturn(h)
	binary.BigEndian.PutUint16(h[0:2], uint16(messageLen))
	binary.BigEndian.PutUint16(h[2:4], uint16(splitIndex))
	copy(h[4:4+splitIndex], message[0:splitIndex])
	if nw, writeErr := w.Write(h[0 : 4+splitIndex]); writeErr != nil {
		return writeErr
	} else if nw < 4+splitIndex {
		return io.ErrShortWrite
	}
	if nw, writeErr := w.Write(message[splitIndex:messageLen]); writeErr != nil {
		return writeErr
	} else if nw < len(message)-splitIndex {
		return io.ErrShortWrite
	}
	return nil
}

// WriteBatch emits several ordinary frames in one stream write. The wire is
// identical to repeated Write calls with split index zero; only the syscall
// and QUIC-stream handoff are coalesced. Message ownership remains with the
// caller on every result.
func (self *Framer) WriteBatch(w io.Writer, messages [][]byte) error {
	if len(messages) == 0 {
		return nil
	}
	if len(messages) == 1 {
		return self.Write(w, messages[0])
	}
	totalByteCount, err := self.writeBatchByteCount(messages)
	if err != nil {
		return err
	}

	batchBytes := MessagePoolGet(totalByteCount)
	defer MessagePoolReturn(batchBytes)
	return writeFramerBatch(w, messages, batchBytes)
}

// WriteBatchWithStorage emits the same wire batch using caller-owned scratch
// storage. The caller must provide exclusive storage for the duration of the
// call and may reuse it after return. Message ownership always stays with the
// caller. An undersized buffer is rejected before any stream byte is written.
func (self *Framer) WriteBatchWithStorage(
	w io.Writer,
	messages [][]byte,
	storage []byte,
) error {
	if len(messages) == 0 {
		return nil
	}
	if len(messages) == 1 {
		return self.Write(w, messages[0])
	}
	totalByteCount, err := self.writeBatchByteCount(messages)
	if err != nil {
		return err
	}
	if len(storage) < totalByteCount {
		return fmt.Errorf(
			"Framer batch storage too small (%d<%d)",
			len(storage),
			totalByteCount,
		)
	}
	return writeFramerBatch(w, messages, storage[:totalByteCount])
}

// writeBatchByteCount validates every message before the writer can observe a
// prefix and returns the exact framed byte count.
func (self *Framer) writeBatchByteCount(messages [][]byte) (int, error) {
	totalByteCount := 0
	for _, message := range messages {
		messageByteCount := len(message)
		if self.maxFrameLen < messageByteCount+4 {
			self.log.Infof(
				"[framer][reject]write batch messageLen=%d > MaxMessageLen=%d (maxFrameLen=%d)\n",
				messageByteCount,
				self.settings.MaxMessageLen,
				self.maxFrameLen,
			)
			return 0, fmt.Errorf(
				"Max message len exceeded (%d<%d)",
				self.settings.MaxMessageLen,
				messageByteCount,
			)
		}
		if math.MaxUint16 < messageByteCount {
			return 0, fmt.Errorf(
				"Max possible message len exceeded (%d<%d)",
				math.MaxUint16,
				messageByteCount,
			)
		}
		if math.MaxInt-totalByteCount < messageByteCount+4 {
			return 0, fmt.Errorf("Framer batch byte count overflow.")
		}
		totalByteCount += messageByteCount + 4
	}
	return totalByteCount, nil
}

// writeFramerBatch fills exact-sized caller storage and performs one stream
// write after all validation has completed.
func writeFramerBatch(w io.Writer, messages [][]byte, batchBytes []byte) error {
	offset := 0
	for _, message := range messages {
		messageByteCount := len(message)
		binary.BigEndian.PutUint16(
			batchBytes[offset:offset+2],
			uint16(messageByteCount),
		)
		binary.BigEndian.PutUint16(batchBytes[offset+2:offset+4], 0)
		copy(batchBytes[offset+4:offset+4+messageByteCount], message)
		offset += messageByteCount + 4
	}
	if writtenByteCount, err := w.Write(batchBytes); err != nil {
		return err
	} else if writtenByteCount < len(batchBytes) {
		return io.ErrShortWrite
	}
	return nil
}
