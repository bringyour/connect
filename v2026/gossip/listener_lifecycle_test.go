// The lifecycle of the extender's in-process gossip listener (EXTENDER.md A8,
// D2).
//
// The listener is the boundary between the extender's connection goroutine and
// the mesh, so what matters here is that it never parks that goroutine and
// never releases a stream twice. Nothing in this file opens a socket; a pipe
// stands in for the taken-over carrier stream.

package gossip

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/transport"

	ma "github.com/multiformats/go-multiaddr"
)

// How long a lifecycle assertion waits for something that has already been
// released. Nothing waits this long when the listener is correct.
const testListenerTimeout = 10 * time.Second

// A stream handed over after the listener is closed is refused at once and
// never queued, so nothing is left holding a stream the mesh will never accept
// (A8, D2).
func TestGossipInProcessListenerRefusesAfterClose(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	listener := NewInProcessListener(ctx, DefaultInProcessListenerSettings())
	listener.Close()

	handled := make(chan struct{})
	go func() {
		defer close(handled)
		listener.Handle(newTestPipe(t))
	}()
	select {
	case <-handled:
	case <-time.After(testListenerTimeout):
		t.Fatal("a handler on a closed listener was not released")
	}
	if queuedCount := len(listener.conns); queuedCount != 0 {
		t.Errorf("the closed listener queued %d connections", queuedCount)
	}
}

// The release of one handler happens exactly once however many times the mesh
// closes the connection, and the mesh still sees the carrier's own close error
// (A8).
func TestGossipInProcessConnReleasesOnce(t *testing.T) {
	closeErr := fmt.Errorf("the carrier stream is already gone")
	conn := &testCloseConn{
		Conn:     newTestPipe(t),
		closeErr: closeErr,
	}
	handled := &inProcessConn{
		Conn:     conn,
		released: make(chan struct{}),
	}

	if err := handled.Close(); err != closeErr {
		t.Fatalf("close err = %v, expected the carrier's own error", err)
	}
	select {
	case <-handled.released:
	default:
		t.Fatal("the close did not release the handler")
	}
	// a second close must not close the release channel again, and still
	// reports what the carrier says
	if err := handled.Close(); err != closeErr {
		t.Fatalf("second close err = %v, expected the carrier's own error", err)
	}
	select {
	case <-handled.released:
	default:
		t.Fatal("the release channel was reopened")
	}
	if closeCount := conn.closeCount(); closeCount != 2 {
		t.Errorf("the carrier was closed %d times, expected 2", closeCount)
	}
}

// An accept after the listener or the view is closed reports the error libp2p
// reads as an orderly listener shutdown, not an arbitrary failure (D2).
func TestGossipInProcessListenerAcceptIsClosed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	// the shared listener is closed
	listener := NewInProcessListener(ctx, DefaultInProcessListenerSettings())
	listener.Close()
	if _, err := listener.accept(make(chan struct{})); err != transport.ErrListenerClosed {
		t.Errorf("accept on a closed listener = %v, expected %v", err, transport.ErrListenerClosed)
	}

	// one view is closed, the shared listener is not
	viewListener := NewInProcessListener(ctx, DefaultInProcessListenerSettings())
	t.Cleanup(viewListener.Close)
	done := make(chan struct{})
	close(done)
	if _, err := viewListener.accept(done); err != transport.ErrListenerClosed {
		t.Errorf("accept on a closed view = %v, expected %v", err, transport.ErrListenerClosed)
	}
}

// The advertised addresses are handed out as a copy, so a caller that walks
// them cannot rewrite what this extender says it is reachable at (D2).
func TestGossipInProcessListenerAddrsAreACopy(t *testing.T) {
	listenAddrs, err := ExtenderListenAddrs(testAddrs(t, "192.0.2.1", "2001:db8::1"), 8443)
	if err != nil {
		t.Fatal(err)
	}
	settings := DefaultInProcessListenerSettings()
	settings.ListenAddrs = listenAddrs
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	listener := NewInProcessListener(ctx, settings)
	t.Cleanup(listener.Close)

	otherAddr, err := ma.NewMultiaddr("/ip4/198.51.100.1/tcp/8443")
	if err != nil {
		t.Fatal(err)
	}
	taken := listener.ListenAddrs()
	if len(taken) != 2 {
		t.Fatalf("listen addrs = %v, expected two", taken)
	}
	taken[0] = otherAddr

	expects := []string{"/ip4/192.0.2.1/tcp/8443", "/ip6/2001:db8::1/tcp/8443"}
	for i, expect := range expects {
		if listenAddr := listener.ListenAddrs()[i]; listenAddr.String() != expect {
			t.Errorf("listen addr = %s, expected %s", listenAddr, expect)
		}
	}
}

// The accept queue is bounded by the default unless a positive bound is given,
// and no settings at all is the default (D2).
func TestGossipInProcessListenerAcceptQueueBound(t *testing.T) {
	if DefaultInProcessAcceptQueueCount != 16 {
		t.Errorf("the default accept queue is %d, expected 16", DefaultInProcessAcceptQueueCount)
	}
	if acceptQueueCount := DefaultInProcessListenerSettings().AcceptQueueCount; acceptQueueCount != DefaultInProcessAcceptQueueCount {
		t.Errorf(
			"the default settings queue %d, expected %d",
			acceptQueueCount,
			DefaultInProcessAcceptQueueCount,
		)
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	cases := []struct {
		name             string
		acceptQueueCount int
		expect           int
	}{
		{name: "zero", acceptQueueCount: 0, expect: DefaultInProcessAcceptQueueCount},
		{name: "negative", acceptQueueCount: -1, expect: DefaultInProcessAcceptQueueCount},
		{name: "one", acceptQueueCount: 1, expect: 1},
		{name: "four", acceptQueueCount: 4, expect: 4},
	}
	for _, c := range cases {
		settings := DefaultInProcessListenerSettings()
		settings.AcceptQueueCount = c.acceptQueueCount
		listener := NewInProcessListener(ctx, settings)
		t.Cleanup(listener.Close)
		if acceptQueueCount := cap(listener.conns); acceptQueueCount != c.expect {
			t.Errorf("%s: accept queue = %d, expected %d", c.name, acceptQueueCount, c.expect)
		}
	}

	// no settings at all is a listener that still advertises nothing and
	// queues the default
	defaultListener := NewInProcessListener(ctx, nil)
	t.Cleanup(defaultListener.Close)
	if acceptQueueCount := cap(defaultListener.conns); acceptQueueCount != DefaultInProcessAcceptQueueCount {
		t.Errorf(
			"the default listener queue = %d, expected %d",
			acceptQueueCount,
			DefaultInProcessAcceptQueueCount,
		)
	}
	if listenAddrs := defaultListener.ListenAddrs(); len(listenAddrs) != 0 {
		t.Errorf("the default listener advertises %v", listenAddrs)
	}
}

// One carrier stream whose close is observable: the mesh's close must reach
// the carrier, and its error must reach the mesh.
type testCloseConn struct {
	net.Conn
	closeErr error

	stateLock sync.Mutex
	closes    int
}

func (self *testCloseConn) Close() error {
	self.stateLock.Lock()
	self.closes += 1
	self.stateLock.Unlock()
	self.Conn.Close()
	return self.closeErr
}

func (self *testCloseConn) closeCount() int {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.closes
}
