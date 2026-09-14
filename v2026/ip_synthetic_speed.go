package connect

// Synthetic speed test endpoint served inside the provider's local NAT.
//
// TCP flows whose destination falls in the RFC 2544 benchmarking range
// 198.18.0.0/15 are terminated locally by an in-memory synthetic server
// instead of dialing upstream. The full tunnel path — client tun, per-peer
// encryption, transfer sequences, transports, provider NAT — is exercised;
// only the upstream internet hop is replaced, so measurements isolate the
// tunnel itself from origin/network variability. The range is reserved for
// benchmarking and never publicly routable, so no real destination is
// shadowed.
//
// The server speaks minimal HTTP/1.1 so any client on the device (browser,
// fetch) can drive it:
//
//	GET /ping             -> 200, 1-byte body ("p"): flow-setup + RTT probe
//	GET /download/<bytes> -> 200, streams exactly <bytes> patterned bytes
//	                         (clamped to syntheticSpeedMaxByteCount)
//	GET <anything else>   -> 200, streams syntheticSpeedDefaultByteCount
//	POST/PUT <any>        -> reads the request body, then 200 with a small
//	                         body reporting the byte count: upload rush sink
//
// Connections are close-delimited per response (Connection: close), which
// keeps the state machine one-shot and measurement bursts independent.

import (
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	syntheticSpeedDefaultByteCount = ByteCount(100 * 1024 * 1024)
	syntheticSpeedMaxByteCount     = ByteCount(10 * 1024 * 1024 * 1024)
	// payload chunk served repeatedly. Patterned (not constant) so link-level
	// compression cannot inflate measured speed.
	syntheticSpeedChunkByteCount = 64 * 1024
)

// isSyntheticSpeedIp reports whether ip is inside the benchmarking ranges:
// 198.18.0.0/15 (RFC 2544) or 2001:2::/48 (RFC 5180).
func isSyntheticSpeedIp(ip net.IP) bool {
	if ip4 := ip.To4(); ip4 != nil {
		return ip4[0] == 198 && (ip4[1] == 18 || ip4[1] == 19)
	}
	ip16 := ip.To16()
	if ip16 == nil {
		return false
	}
	return ip16[0] == 0x20 && ip16[1] == 0x01 && ip16[2] == 0x00 && ip16[3] == 0x02 &&
		ip16[4] == 0x00 && ip16[5] == 0x00
}

var syntheticSpeedChunk = func() []byte {
	b := make([]byte, syntheticSpeedChunkByteCount)
	// xorshift fill; fixed seed keeps it deterministic
	x := uint32(0x9E3779B9)
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x)
	}
	return b
}()

// syntheticSpeedConn is an in-memory net.Conn that stands in for the upstream
// socket of a provider TcpSequence. Write consumes request bytes (called from
// the sequence's send pipeline); Read produces the response (called from the
// sequence's receive pump). The two sides synchronize on stateLock + notify.
type syntheticSpeedConn struct {
	stateLock sync.Mutex
	notify    chan struct{}
	closed    chan struct{}
	closeOnce sync.Once

	// request accumulation until the header terminator
	request []byte
	// non-empty once a response is staged; Read drains head then payload
	head     string
	headSent int
	// remaining payload bytes to serve after head
	remaining ByteCount
	// upload sink state
	uploadActive    bool
	uploadRemaining ByteCount
	uploadTotal     ByteCount
}

func newSyntheticSpeedConn() *syntheticSpeedConn {
	return &syntheticSpeedConn{
		notify: make(chan struct{}, 1),
		closed: make(chan struct{}),
	}
}

func (self *syntheticSpeedConn) signal() {
	select {
	case self.notify <- struct{}{}:
	default:
	}
}

// Write consumes client->server bytes (the HTTP request).
func (self *syntheticSpeedConn) Write(b []byte) (int, error) {
	select {
	case <-self.closed:
		return 0, io.ErrClosedPipe
	default:
	}
	n := len(b)

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if self.uploadActive {
		consume := min(ByteCount(n), self.uploadRemaining)
		self.uploadRemaining -= consume
		if self.uploadRemaining <= 0 {
			self.finishUploadWithLock()
		}
		// any overflow beyond the declared body is ignored (pipelining is
		// not supported; the response is Connection: close)
		return n, nil
	}
	if self.head != "" {
		// request already answered; drain
		return n, nil
	}
	// accumulate until end of headers, with a sanity cap
	self.request = append(self.request, b...)
	if len(self.request) > 64*1024 {
		self.respondWithLock("400 Bad Request", []byte("request too large"))
		return n, nil
	}
	if i := strings.Index(string(self.request), "\r\n\r\n"); i >= 0 {
		header := string(self.request[:i])
		bodyStart := len(self.request) - (i + 4)
		self.routeWithLock(header, ByteCount(bodyStart))
	}
	return n, nil
}

func (self *syntheticSpeedConn) routeWithLock(header string, bodyStartByteCount ByteCount) {
	lines := strings.Split(header, "\r\n")
	parts := strings.SplitN(lines[0], " ", 3)
	if len(parts) < 2 {
		self.respondWithLock("400 Bad Request", []byte("bad request line"))
		return
	}
	method, target := parts[0], parts[1]

	switch method {
	case "GET", "HEAD":
		if target == "/ping" {
			self.respondWithLock("200 OK", []byte("p"))
			return
		}
		byteCount := syntheticSpeedDefaultByteCount
		if rest, ok := strings.CutPrefix(target, "/download/"); ok {
			if v, err := strconv.ParseInt(strings.TrimSuffix(rest, "/"), 10, 64); err == nil && v > 0 {
				byteCount = min(ByteCount(v), syntheticSpeedMaxByteCount)
			}
		}
		self.head = fmt.Sprintf(
			"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: %d\r\nConnection: close\r\n\r\n",
			byteCount,
		)
		if method == "HEAD" {
			byteCount = 0
		}
		self.remaining = byteCount
		self.request = nil
		self.signal()
	case "POST", "PUT":
		var contentLength ByteCount
		for _, line := range lines[1:] {
			if k, v, ok := strings.Cut(line, ":"); ok && strings.EqualFold(strings.TrimSpace(k), "Content-Length") {
				if n, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
					contentLength = ByteCount(n)
				}
			}
		}
		self.uploadActive = true
		self.uploadTotal = contentLength
		self.uploadRemaining = contentLength - bodyStartByteCount
		self.request = nil
		if self.uploadRemaining <= 0 {
			self.finishUploadWithLock()
		}
	default:
		self.respondWithLock("405 Method Not Allowed", []byte("method not allowed"))
	}
}

func (self *syntheticSpeedConn) finishUploadWithLock() {
	self.uploadActive = false
	self.respondWithLock("200 OK", []byte(fmt.Sprintf("received %d\n", self.uploadTotal)))
}

func (self *syntheticSpeedConn) respondWithLock(status string, body []byte) {
	self.head = fmt.Sprintf(
		"HTTP/1.1 %s\r\nContent-Type: text/plain\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s",
		status,
		len(body),
		body,
	)
	self.remaining = 0
	self.request = nil
	self.signal()
}

// Read produces server->client bytes (the HTTP response head then payload).
func (self *syntheticSpeedConn) Read(b []byte) (int, error) {
	for {
		n, done := func() (int, bool) {
			self.stateLock.Lock()
			defer self.stateLock.Unlock()
			if self.headSent < len(self.head) {
				n := copy(b, self.head[self.headSent:])
				self.headSent += n
				return n, false
			}
			if self.head != "" && self.remaining > 0 {
				n := min(ByteCount(len(b)), self.remaining)
				written := 0
				for written < int(n) {
					written += copy(b[written:int(n)], syntheticSpeedChunk)
				}
				self.remaining -= n
				return int(n), false
			}
			if self.head != "" {
				// response complete: close-delimited
				return 0, true
			}
			return 0, false
		}()
		if n > 0 {
			return n, nil
		}
		if done {
			return 0, io.EOF
		}
		// no response staged yet: wait for the request to arrive
		select {
		case <-self.closed:
			return 0, io.ErrClosedPipe
		case <-self.notify:
		}
	}
}

func (self *syntheticSpeedConn) Close() error {
	self.closeOnce.Do(func() {
		close(self.closed)
	})
	return nil
}

type syntheticSpeedAddr struct{}

func (syntheticSpeedAddr) Network() string { return "synthetic-speed" }
func (syntheticSpeedAddr) String() string  { return "synthetic-speed" }

func (self *syntheticSpeedConn) LocalAddr() net.Addr                { return syntheticSpeedAddr{} }
func (self *syntheticSpeedConn) RemoteAddr() net.Addr               { return syntheticSpeedAddr{} }
func (self *syntheticSpeedConn) SetDeadline(t time.Time) error      { return nil }
func (self *syntheticSpeedConn) SetReadDeadline(t time.Time) error  { return nil }
func (self *syntheticSpeedConn) SetWriteDeadline(t time.Time) error { return nil }
