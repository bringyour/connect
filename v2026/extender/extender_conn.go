package extender

import (
	"bufio"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go/http3"
)

// Connection adapters shared by the three carriers.

// connWithInitialBytes puts already-read bytes back in front of a connection.
// The tcp carrier reads four bytes to tell a v1 length-prefixed header from an
// http request (A3); when it is an http request those bytes belong to the
// request line or the h2 preface and must be seen by the server that follows.
//
// It also carries the server name of the terminated connection, which is the
// only place the requested name survives once the connection is no longer a
// *tls.Conn (A5).
//
// Close is idempotent and closes the channel the carrier joins on, so the
// carrier can wait for the http server to finish with the connection without
// racing the handler.
type connWithInitialBytes struct {
	net.Conn
	initialBytes []byte
	serverName   string
	closeOnce    sync.Once
	closed       chan struct{}
}

func newConnWithInitialBytes(
	conn net.Conn,
	initialBytes []byte,
	serverName string,
) *connWithInitialBytes {
	return &connWithInitialBytes{
		Conn:         conn,
		initialBytes: initialBytes,
		serverName:   serverName,
		closed:       make(chan struct{}),
	}
}

func (self *connWithInitialBytes) Read(b []byte) (int, error) {
	m := min(len(self.initialBytes), len(b))
	if 0 < m {
		copy(b[0:m], self.initialBytes[0:m])
		self.initialBytes = self.initialBytes[m:]
		// return only the prefix; mixing in a socket read would block on
		// bytes the peer may not have sent yet
		return m, nil
	}
	return self.Conn.Read(b)
}

func (self *connWithInitialBytes) Close() error {
	var err error
	self.closeOnce.Do(func() {
		err = self.Conn.Close()
		close(self.closed)
	})
	return err
}

// Closed reports connection release, which is how a carrier joins an http
// server it handed the connection to.
func (self *connWithInitialBytes) Closed() <-chan struct{} {
	return self.closed
}

// connWithReader reads through a buffered reader that already holds bytes from
// the connection. An http server hands back such a reader when a handler
// hijacks, and it may already hold the first inner bytes.
type connWithReader struct {
	net.Conn
	reader *bufio.Reader
}

func newConnWithReader(conn net.Conn, reader *bufio.Reader) *connWithReader {
	return &connWithReader{
		Conn:   conn,
		reader: reader,
	}
}

func (self *connWithReader) Read(b []byte) (int, error) {
	return self.reader.Read(b)
}

// singleConnListener hands one already-accepted connection to an http server
// and then blocks until the listener is closed. The extender terminates its
// own tls and decides per connection whether the bytes are v1 or http, so it
// cannot give the http server a listener of its own.
type singleConnListener struct {
	conn       net.Conn
	addr       net.Addr
	acceptOnce sync.Once
	closeOnce  sync.Once
	closed     chan struct{}
}

func newSingleConnListener(conn net.Conn, addr net.Addr) *singleConnListener {
	return &singleConnListener{
		conn:   conn,
		addr:   addr,
		closed: make(chan struct{}),
	}
}

func (self *singleConnListener) Accept() (net.Conn, error) {
	var conn net.Conn
	self.acceptOnce.Do(func() {
		conn = self.conn
	})
	if conn != nil {
		return conn, nil
	}
	<-self.closed
	return nil, net.ErrClosed
}

func (self *singleConnListener) Close() error {
	self.closeOnce.Do(func() {
		close(self.closed)
	})
	return nil
}

func (self *singleConnListener) Addr() net.Addr {
	return self.addr
}

// streamConn adapts one taken-over http3 stream to a connection, so the udp
// carriers reach the same relay as the tcp carrier. The quic connection stays
// owned by the carrier, so closing this closes only the stream.
type streamConn struct {
	stream     *http3.Stream
	localAddr  net.Addr
	remoteAddr net.Addr
}

func newStreamConn(stream *http3.Stream, localAddr net.Addr, remoteAddr net.Addr) *streamConn {
	return &streamConn{
		stream:     stream,
		localAddr:  localAddr,
		remoteAddr: remoteAddr,
	}
}

func (self *streamConn) Read(b []byte) (int, error) {
	return self.stream.Read(b)
}

func (self *streamConn) Write(b []byte) (int, error) {
	return self.stream.Write(b)
}

func (self *streamConn) Close() error {
	return self.stream.Close()
}

func (self *streamConn) LocalAddr() net.Addr {
	return self.localAddr
}

func (self *streamConn) RemoteAddr() net.Addr {
	return self.remoteAddr
}

func (self *streamConn) SetDeadline(t time.Time) error {
	return self.stream.SetDeadline(t)
}

func (self *streamConn) SetReadDeadline(t time.Time) error {
	return self.stream.SetReadDeadline(t)
}

func (self *streamConn) SetWriteDeadline(t time.Time) error {
	return self.stream.SetWriteDeadline(t)
}
