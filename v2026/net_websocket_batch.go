// WebSocket write batching coalesces complete, already-ready frames above the
// TLS boundary without changing their WebSocket framing or adding idle delay.
package connect

import (
	"io"
	"net"
	"sync"
	"time"
)

const webSocketWriteBatchMaxByteCount = 16 * 1024

// WebSocketWriteBatchConn preserves the WebSocket byte stream while allowing
// one transport writer to combine several already-queued messages into one
// TLS Write. It starts in pass-through mode so the HTTP upgrade handshake
// cannot be retained; PlatformTransport explicitly brackets each ready batch.
//
// One data writer brackets batches. A WebSocket reader may concurrently emit a
// control frame, so stateLock also serializes every Write and delegated socket
// write. A control frame that arrives inside a ready-only batch joins that
// byte-stream batch in arrival order. Read, deadlines, and Close retain
// net.Conn's concurrent contract and can interrupt a blocked delegated write.
type WebSocketWriteBatchConn struct {
	conn        net.Conn
	stateLock   sync.Mutex
	writeBuffer []byte
	batching    bool

	// Tests place batch activation and a concurrent control write at one exact
	// boundary. Nil in production.
	beforeBeginWriteBatchForTest func()
	beforeWriteForTest           func()
}

// Creates a pass-through connection whose explicit batches are bounded by the
// package's fixed 16 KiB coalescing buffer.
func NewWebSocketWriteBatchConn(conn net.Conn) *WebSocketWriteBatchConn {
	return &WebSocketWriteBatchConn{
		conn: conn,
	}
}

// Starts one explicit ready-only batch on the connection's single writer.
func (self *WebSocketWriteBatchConn) BeginWriteBatch() {
	if self.beforeBeginWriteBatchForTest != nil {
		self.beforeBeginWriteBatchForTest()
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if self.batching {
		panic("websocket write batch already active")
	}
	if self.writeBuffer == nil {
		self.writeBuffer = make([]byte, 0, webSocketWriteBatchMaxByteCount)
	} else {
		self.writeBuffer = self.writeBuffer[:0]
	}
	self.batching = true
}

// Discards bytes that have not reached the delegated connection.
func (self *WebSocketWriteBatchConn) AbortWriteBatch() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.batching = false
	self.writeBuffer = self.writeBuffer[:0]
}

// Writes retained bytes while the state lock keeps concurrent control writes
// from interleaving with the delegated stream write.
func (self *WebSocketWriteBatchConn) flushWriteBufferWithLock() error {
	if len(self.writeBuffer) == 0 {
		return nil
	}
	writeByteCount := len(self.writeBuffer)
	n, err := self.conn.Write(self.writeBuffer)
	self.writeBuffer = self.writeBuffer[:0]
	if err == nil && n != writeByteCount {
		return io.ErrShortWrite
	}
	return err
}

// Ends the active batch and writes every retained complete frame byte.
func (self *WebSocketWriteBatchConn) FlushWriteBatch() error {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if !self.batching {
		return nil
	}
	self.batching = false
	return self.flushWriteBufferWithLock()
}

// Delegates reads without sharing the single-writer batching state.
func (self *WebSocketWriteBatchConn) Read(buffer []byte) (int, error) {
	return self.conn.Read(buffer)
}

// Passes through outside a batch and otherwise retains a bounded byte prefix.
func (self *WebSocketWriteBatchConn) Write(buffer []byte) (int, error) {
	if self.beforeWriteForTest != nil {
		self.beforeWriteForTest()
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if !self.batching {
		return self.conn.Write(buffer)
	}
	if webSocketWriteBatchMaxByteCount < len(self.writeBuffer)+len(buffer) {
		if err := self.flushWriteBufferWithLock(); err != nil {
			return 0, err
		}
	}
	if webSocketWriteBatchMaxByteCount < len(buffer) {
		return self.conn.Write(buffer)
	}
	self.writeBuffer = append(self.writeBuffer, buffer...)
	return len(buffer), nil
}

// Closes the delegated connection and interrupts its blocked I/O.
func (self *WebSocketWriteBatchConn) Close() error {
	return self.conn.Close()
}

// Returns the delegated local address.
func (self *WebSocketWriteBatchConn) LocalAddr() net.Addr {
	return self.conn.LocalAddr()
}

// Returns the delegated remote address.
func (self *WebSocketWriteBatchConn) RemoteAddr() net.Addr {
	return self.conn.RemoteAddr()
}

// Sets the delegated read and write deadlines.
func (self *WebSocketWriteBatchConn) SetDeadline(deadline time.Time) error {
	return self.conn.SetDeadline(deadline)
}

// Sets the delegated read deadline.
func (self *WebSocketWriteBatchConn) SetReadDeadline(deadline time.Time) error {
	return self.conn.SetReadDeadline(deadline)
}

// Sets the delegated write deadline.
func (self *WebSocketWriteBatchConn) SetWriteDeadline(deadline time.Time) error {
	return self.conn.SetWriteDeadline(deadline)
}
