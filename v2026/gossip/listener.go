// The extender's in-process gossip listener (EXTENDER.md A8, D2).
//
// An extender has no socket of its own for the mesh. A member reaches it by
// opening the gossip service on one of the extender's carriers, and the
// extender hands the taken-over stream to `GossipConnHandler`, which owns it
// for the duration of the call and no longer: the extender closes the stream
// when the callback returns and keeps it in its shutdown set meanwhile (A8).
//
// `Handle` is therefore the whole contract of this file. It queues the
// connection for `Accept` and blocks until that connection is closed, which is
// what keeps the extender from closing a stream the mesh is still using. A
// queue that is full refuses the connection immediately rather than waiting,
// because the extender's connection goroutine is not a place to park.
//
// One listener serves every family the extender activated. libp2p opens one
// transport listener per advertised address, and all of them accept from the
// same queue: the connection carries its own remote address, so which
// advertised address it arrived on is not something the mesh needs to know.
//
// The listener is safe for concurrent use.

package gossip

import (
	"context"
	"net"
	"sync"

	"github.com/libp2p/go-libp2p/core/transport"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"

	"github.com/urnetwork/connect/v2026"
)

// Connections negotiated but not yet accepted before `Handle` refuses (D2).
// The same bound libp2p's own upgrader uses for accepted connections.
const DefaultInProcessAcceptQueueCount = 16

type InProcessListenerSettings struct {
	Log connect.Logger

	// The addresses this extender is reachable at, one per activated family
	// (D2). They are what the mesh advertises for this node, and the node
	// listens on exactly them.
	ListenAddrs []ma.Multiaddr

	// Connections waiting for `Accept` before `Handle` refuses. <= 0 takes the
	// default.
	AcceptQueueCount int
}

func DefaultInProcessListenerSettings() *InProcessListenerSettings {
	return &InProcessListenerSettings{
		AcceptQueueCount: DefaultInProcessAcceptQueueCount,
	}
}

// InProcessListener bridges the extender's gossip service to a libp2p
// transport listener.
type InProcessListener struct {
	ctx       context.Context
	cancel    context.CancelFunc
	closeOnce sync.Once
	log       connect.Logger

	settings *InProcessListenerSettings

	conns chan *inProcessConn
}

func NewInProcessListener(
	ctx context.Context,
	settings *InProcessListenerSettings,
) *InProcessListener {
	if settings == nil {
		settings = DefaultInProcessListenerSettings()
	}
	acceptQueueCount := settings.AcceptQueueCount
	if acceptQueueCount <= 0 {
		acceptQueueCount = DefaultInProcessAcceptQueueCount
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	return &InProcessListener{
		ctx:      cancelCtx,
		cancel:   cancel,
		log:      loggerOrDefault(settings.Log),
		settings: settings,
		conns:    make(chan *inProcessConn, acceptQueueCount),
	}
}

// The addresses this extender advertises (D2).
func (self *InProcessListener) ListenAddrs() []ma.Multiaddr {
	return append([]ma.Multiaddr{}, self.settings.ListenAddrs...)
}

// Handle is what an extender installs as `ExtenderSettings.GossipConnHandler`
// (A8). It owns the connection until it returns, so it returns only when the
// mesh has released it or the listener is closed. A connection that cannot be
// queued is refused at once, which closes it on the extender side.
func (self *InProcessListener) Handle(conn net.Conn) {
	select {
	case <-self.ctx.Done():
		return
	default:
	}
	handled := &inProcessConn{
		Conn:     conn,
		released: make(chan struct{}),
	}
	select {
	case self.conns <- handled:
	default:
		// the mesh is not keeping up with new connections; refusing is the
		// only bounded answer, since this runs on the extender's connection
		// goroutine
		if self.log.V(1).Enabled() {
			self.log.Infof("[gossip]accept queue is full, refusing %s\n", conn.RemoteAddr())
		}
		return
	}
	select {
	case <-handled.released:
	case <-self.ctx.Done():
	}
}

// Ends every waiting `Handle`, which releases the streams back to the extender.
func (self *InProcessListener) Close() {
	self.closeOnce.Do(func() {
		self.cancel()
	})
}

// The next connection the extender handed over, or an error once the listener
// or the view is closed.
func (self *InProcessListener) accept(done chan struct{}) (*inProcessConn, error) {
	select {
	case conn := <-self.conns:
		return conn, nil
	case <-done:
		return nil, transport.ErrListenerClosed
	case <-self.ctx.Done():
		return nil, transport.ErrListenerClosed
	}
}

// inProcessConn releases the extender's `Handle` when the mesh closes the
// connection. The extender closes the underlying stream itself once `Handle`
// returns (A8), so closing here is only the release: the embedded Close is
// still called, because a mesh that is done with a connection expects it to
// stop reading at once.
type inProcessConn struct {
	net.Conn
	released  chan struct{}
	closeOnce sync.Once
}

func (self *inProcessConn) Close() error {
	err := self.Conn.Close()
	self.closeOnce.Do(func() {
		close(self.released)
	})
	return err
}

// inProcessMaListener is one libp2p view of the shared listener, bound to one
// advertised address. Closing a view ends only that view; the extender's
// connections are released when the shared listener closes.
type inProcessMaListener struct {
	listener  *InProcessListener
	multiaddr ma.Multiaddr
	done      chan struct{}
	closeOnce sync.Once
}

func newInProcessMaListener(
	listener *InProcessListener,
	multiaddr ma.Multiaddr,
) *inProcessMaListener {
	return &inProcessMaListener{
		listener:  listener,
		multiaddr: multiaddr,
		done:      make(chan struct{}),
	}
}

func (self *inProcessMaListener) Accept() (manet.Conn, error) {
	conn, err := self.listener.accept(self.done)
	if err != nil {
		return nil, err
	}
	return newMaConn(conn, self.multiaddr, remoteMultiaddrOrDefault(conn, self.multiaddr)), nil
}

func (self *inProcessMaListener) Close() error {
	self.closeOnce.Do(func() {
		close(self.done)
	})
	return nil
}

func (self *inProcessMaListener) Multiaddr() ma.Multiaddr {
	return self.multiaddr
}

func (self *inProcessMaListener) Addr() net.Addr {
	if netAddr, err := manet.ToNetAddr(self.multiaddr); err == nil {
		return netAddr
	}
	return &net.TCPAddr{}
}

// maConn is the manet.Conn the upgrader consumes. The addresses are carried
// rather than derived, because a carrier stream's own addresses are the udp
// endpoints of a quic connection on two of the three carriers, which describe
// the extender's socket and not the address the mesh knows it by.
type maConn struct {
	net.Conn
	localMultiaddr  ma.Multiaddr
	remoteMultiaddr ma.Multiaddr
}

func newMaConn(
	conn net.Conn,
	localMultiaddr ma.Multiaddr,
	remoteMultiaddr ma.Multiaddr,
) *maConn {
	return &maConn{
		Conn:            conn,
		localMultiaddr:  localMultiaddr,
		remoteMultiaddr: remoteMultiaddr,
	}
}

func (self *maConn) LocalMultiaddr() ma.Multiaddr {
	return self.localMultiaddr
}

func (self *maConn) RemoteMultiaddr() ma.Multiaddr {
	return self.remoteMultiaddr
}

// The peer's address as a multiaddr, falling back to the listener's own
// address when the carrier presents something that is not an ip endpoint. The
// fallback keeps the resource manager and the peer store working; it never
// carries identity, which comes from the security handshake alone.
func remoteMultiaddrOrDefault(conn net.Conn, defaultMultiaddr ma.Multiaddr) ma.Multiaddr {
	if remoteAddr := conn.RemoteAddr(); remoteAddr != nil {
		if remoteMultiaddr, err := manet.FromNetAddr(remoteAddr); err == nil {
			return remoteMultiaddr
		}
	}
	return defaultMultiaddr
}

func loggerOrDefault(log connect.Logger) connect.Logger {
	if log == nil {
		return connect.DefaultLogger()
	}
	return log
}
