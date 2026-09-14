// The connection adapters the three carriers share (EXTENDER.md A3, A5, A9):
// the one-shot listener the tcp carrier hands the http server, the prefix the
// carrier puts back in front of a terminated connection, and the source key
// every per-source bound is counted against.

package extender

import (
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// closeCountingConn counts the closes reaching the socket, so a test can prove
// an adapter releases it exactly once.
type closeCountingConn struct {
	net.Conn
	stateLock  sync.Mutex
	closeCount int
}

func newCloseCountingConn(conn net.Conn) *closeCountingConn {
	return &closeCountingConn{Conn: conn}
}

func (self *closeCountingConn) Close() error {
	self.stateLock.Lock()
	self.closeCount += 1
	self.stateLock.Unlock()
	return self.Conn.Close()
}

func (self *closeCountingConn) closes() int {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.closeCount
}

// The one-shot listener yields the accepted connection exactly once, then
// blocks until it is closed, and answers every later Accept with net.ErrClosed
// (A3). Close is idempotent, because both the carrier and the http server
// reach it.
func TestSingleConnListenerYieldsOneConnection(t *testing.T) {
	conn, peer := net.Pipe()
	defer conn.Close()
	defer peer.Close()
	addr := &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 443}
	listener := newSingleConnListener(conn, addr)

	if listener.Addr() != net.Addr(addr) {
		t.Fatalf("listener address = %v, expected the carrier address", listener.Addr())
	}
	accepted, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if accepted != conn {
		t.Fatal("the listener yielded another connection")
	}

	// the second Accept holds the http server until the carrier releases the
	// listener; closing is what it waits for, not a deadline
	blockedErrs := make(chan error, 1)
	blockedEarly := make(chan bool, 1)
	closing := make(chan struct{})
	go func() {
		_, err := listener.Accept()
		select {
		case <-closing:
			blockedEarly <- false
		default:
			blockedEarly <- true
		}
		blockedErrs <- err
	}()

	close(closing)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if <-blockedEarly {
		t.Fatal("the second Accept returned before the listener was closed")
	}
	if err := <-blockedErrs; !errors.Is(err, net.ErrClosed) {
		t.Fatalf("the blocked Accept returned %v, expected net.ErrClosed", err)
	}

	// closing twice is not a double close of the release channel
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := listener.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Accept after close returned %v, expected net.ErrClosed", err)
	}
}

// The bytes the tcp carrier already read are served before the socket, and one
// Read never mixes the two: the peer may not have sent the rest yet (A3). The
// requested name of the terminated connection rides along, because the served
// connection is no longer a *tls.Conn (A5).
func TestConnWithInitialBytesServesThePrefixFirst(t *testing.T) {
	socket, peer := net.Pipe()
	defer peer.Close()
	countingConn := newCloseCountingConn(socket)
	conn := newConnWithInitialBytes(countingConn, []byte("ABCD"), testServerName)

	if conn.serverName != testServerName {
		t.Fatalf("server name = %q, expected %q", conn.serverName, testServerName)
	}

	// a short buffer takes part of the prefix
	buffer := make([]byte, 2)
	n, err := conn.Read(buffer)
	if err != nil {
		t.Fatal(err)
	}
	if string(buffer[0:n]) != "AB" {
		t.Fatalf("first read = %q, expected the first prefix bytes", buffer[0:n])
	}

	// a long buffer takes only what is left of the prefix, never the socket
	peerDone := make(chan error, 1)
	go func() {
		_, err := peer.Write([]byte("EF"))
		peerDone <- err
	}()
	buffer = make([]byte, 8)
	n, err = conn.Read(buffer)
	if err != nil {
		t.Fatal(err)
	}
	if string(buffer[0:n]) != "CD" {
		t.Fatalf("second read = %q, expected the rest of the prefix alone", buffer[0:n])
	}

	// the prefix is spent, so the next read is the socket
	n, err = conn.Read(buffer)
	if err != nil {
		t.Fatal(err)
	}
	if string(buffer[0:n]) != "EF" {
		t.Fatalf("third read = %q, expected the socket bytes", buffer[0:n])
	}
	if err := <-peerDone; err != nil {
		t.Fatal(err)
	}

	select {
	case <-conn.Closed():
		t.Fatal("the connection reports released before it was closed")
	default:
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-conn.Closed():
	case <-time.After(5 * time.Second):
		t.Fatal("the close did not release the carrier")
	}
	// a second close must not release the socket again, and must not close the
	// release channel twice
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if closes := countingConn.closes(); closes != 1 {
		t.Fatalf("the socket was closed %d times, expected exactly one", closes)
	}
}

// The per-source key of every bound is the address without its port, so a
// source cannot multiply its budget by using more ports, and an ipv6 source
// keys on the address alone (A5, A6, A9).
func TestConnectionSourceAddressDropsThePort(t *testing.T) {
	cases := []struct {
		address string
		source  string
	}{
		{address: "192.0.2.5:443", source: "192.0.2.5"},
		// the same source on another port shares one budget
		{address: "192.0.2.5:51234", source: "192.0.2.5"},
		{address: "[2001:db8::1]:443", source: "2001:db8::1"},
		{address: "[2001:db8::1]:51234", source: "2001:db8::1"},
		// an address with no port is its own key
		{address: "192.0.2.5", source: "192.0.2.5"},
		{address: "2001:db8::1", source: "2001:db8::1"},
		{address: "", source: ""},
	}
	for _, c := range cases {
		if source := connectionSourceAddress(c.address); source != c.source {
			t.Errorf("source of %q = %q, expected %q", c.address, source, c.source)
		}
	}

	addrCases := []struct {
		remoteAddr net.Addr
		source     string
	}{
		{remoteAddr: &net.TCPAddr{IP: net.ParseIP("192.0.2.5"), Port: 443}, source: "192.0.2.5"},
		{remoteAddr: &net.TCPAddr{IP: net.ParseIP("192.0.2.5"), Port: 51234}, source: "192.0.2.5"},
		{remoteAddr: &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}, source: "2001:db8::1"},
		{remoteAddr: &net.UDPAddr{IP: net.ParseIP("2001:db8::1"), Port: 51234}, source: "2001:db8::1"},
		// an in-memory pipe has no address at all
		{remoteAddr: nil, source: ""},
	}
	for _, c := range addrCases {
		if source := connectionSource(c.remoteAddr); source != c.source {
			t.Errorf("source of %v = %q, expected %q", c.remoteAddr, source, c.source)
		}
	}
}
