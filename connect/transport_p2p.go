package connect

import (
	"context"
	"io"
	"net"
	"slices"
	"time"
)

// Assumptions about our peer-to-peer connections:
// - a limited transmit buffer that uses semi-reliable delivery as flow control.
//   While the transfer client is the ultimate source of reliable delivery,
//   we require the p2p connection use semi-reliable delivery to back pressure the transfer rate,
//   which propagates through the entire multi-hop stream.
//   Without flow control we would have more mismatches in transfer rate
//   and retransmits from the transfer clients.
// - disconnect detection. Both peers should be aware when either side disconnects.
//   This is typically manifested in clean disconnect messages and heartbeat timeouts.
// - directed initializaton. One side of the connection will offer to connect
//   and the other side will respond. We assume this in our architecture. However,
//   directed is usually a superset of undirected, so this does not prevent an undirected
//   initializtion either.

// important - changing this will break compatibility with older clients
const ReadyHeader = "rdy"

func DefaultP2pTransportSettings() *P2pTransportSettings {
	return &P2pTransportSettings{
		MaxMessageSize:   ByteCount(1024 * 1024),
		ReconnectTimeout: 5 * time.Second,
	}
}

type PeerType = string

const (
	// the peer who initiates the transfer
	PeerTypeSource PeerType = "source"
	// the peer who is the destination of the transfer
	PeerTypeDestination PeerType = "destination"
)

type P2pTransportSettings struct {
	MaxMessageSize   ByteCount
	ReconnectTimeout time.Duration
}

type P2pTransport struct {
	ctx    context.Context
	cancel context.CancelFunc

	client *Client

	webRtcManager *WebRtcManager

	sendRouteManager    *RouteManager
	receiveRouteManager *RouteManager

	peerId   Id
	streamId Id
	peerType PeerType

	sendReady    chan struct{}
	receiveReady chan struct{}

	p2pTransportSettings *P2pTransportSettings
}

func NewP2pTransport(
	ctx context.Context,
	client *Client,
	webRtcManager *WebRtcManager,
	sendRouteManager *RouteManager,
	receiveRouteManager *RouteManager,
	peerId Id,
	streamId Id,
	// this is the peer type of `peerId`. The current client is the complement.
	peerType PeerType,
	sendReady chan struct{},
	receiveReady chan struct{},
	p2pTransportSettings *P2pTransportSettings,
) *P2pTransport {
	cancelCtx, cancel := context.WithCancel(ctx)
	p2pTransport := &P2pTransport{
		ctx:                  cancelCtx,
		cancel:               cancel,
		client:               client,
		webRtcManager:        webRtcManager,
		sendRouteManager:     sendRouteManager,
		receiveRouteManager:  receiveRouteManager,
		peerId:               peerId,
		streamId:             streamId,
		peerType:             peerType,
		sendReady:            sendReady,
		receiveReady:         receiveReady,
		p2pTransportSettings: p2pTransportSettings,
	}
	return p2pTransport
}

func (self *P2pTransport) run() {
	defer self.cancel()

	for {
		// TODO using net.Conn as a stand in for the actual interface

		reconnect := NewReconnect(self.p2pTransportSettings.ReconnectTimeout)
		var conn net.Conn
		var err error
		// note, one side of the P2P connection will be driving the setup process (active).
		// We arbitrarily choose the sender (peer is destination) as active.
		switch self.peerType {
		case PeerTypeDestination:
			conn, err = self.webRtcManager.NewP2pConnActive(self.ctx, self.peerId, self.streamId)
		case PeerTypeSource:
			conn, err = self.webRtcManager.NewP2pConnPassive(self.ctx, self.peerId, self.streamId)
		default:
			// unknown peer type
			return
		}
		if err != nil {
			select {
			case <-self.ctx.Done():
				return
			case <-reconnect.After():
			}
			continue
		}

		// at this point, the connection should be able to ping the other side
		// now we wait for the entire stream to be ready by propagating the `ReaderHeader`
		c := func() {
			defer conn.Close()

			handleCtx, handleCancel := context.WithCancel(self.ctx)
			defer handleCancel()

			go func() {
				defer self.cancel()

				select {
				case <-handleCtx.Done():
					return
				case <-self.receiveReady:
				}

				_, err := conn.Write([]byte(ReadyHeader))
				if err != nil {
					return
				}

				t, route := NewP2pReceiveTransport(handleCtx, handleCancel, conn, self.streamId, self.p2pTransportSettings)

				self.receiveRouteManager.UpdateTransport(t, []Route{route})
				defer self.receiveRouteManager.RemoveTransport(t)

				select {
				case <-handleCtx.Done():
					return
				}
			}()

			go func() {
				defer self.cancel()

				select {
				case <-handleCtx.Done():
					return
				default:
				}

				header := make([]byte, len(ReadyHeader))
				_, err := io.ReadFull(conn, header)
				if err != nil {
					return
				}
				if !slices.Equal(header, []byte(ReadyHeader)) {
					return
				}

				close(self.sendReady)

				t, route := NewP2pSendTransport(handleCtx, handleCancel, conn, self.streamId, self.p2pTransportSettings)

				self.sendRouteManager.UpdateTransport(t, []Route{route})
				defer self.sendRouteManager.RemoveTransport(t)

				select {
				case <-handleCtx.Done():
					return
				}
			}()

			select {
			case <-handleCtx.Done():
				return
			}
		}

		reconnect = NewReconnect(self.p2pTransportSettings.ReconnectTimeout)
		c()
		select {
		case <-self.ctx.Done():
			return
		case <-reconnect.After():
		}
	}
}

type P2pSendTransport struct {
	transportId Id

	ctx      context.Context
	cancel   context.CancelFunc
	conn     net.Conn
	streamId Id
	send     chan []byte

	p2pTransportSettings *P2pTransportSettings
}

func NewP2pSendTransport(
	ctx context.Context,
	cancel context.CancelFunc,
	conn net.Conn,
	streamId Id,
	p2pTransportSettings *P2pTransportSettings,
) (Transport, Route) {
	send := make(chan []byte)
	p2pSendTransport := &P2pSendTransport{
		transportId:          NewId(),
		ctx:                  ctx,
		cancel:               cancel,
		conn:                 conn,
		streamId:             streamId,
		send:                 send,
		p2pTransportSettings: p2pTransportSettings,
	}
	go p2pSendTransport.run()
	return p2pSendTransport, send
}

func (self *P2pSendTransport) run() {
	defer self.cancel()

	for {
		select {
		case <-self.ctx.Done():
			return
		case transferFrameBytes, ok := <-self.send:
			if !ok {
				return
			}

			if ByteCount(len(transferFrameBytes)) <= self.p2pTransportSettings.MaxMessageSize {
				_, err := self.conn.Write(transferFrameBytes)
				if err != nil {
					return
				}
			} else {
				// drop it
				// FIXME log
			}
		}
	}
}

func (self *P2pSendTransport) TransportId() Id {
	return self.transportId
}

// lower priority takes precedence
func (self *P2pSendTransport) Priority() int {
	return 0
}

func (self *P2pSendTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}

func (self *P2pSendTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	return 1.0
}

func (self *P2pSendTransport) MatchesSend(destination TransferPath) bool {
	return destination.StreamId == self.streamId
}

func (self *P2pSendTransport) MatchesReceive(destination TransferPath) bool {
	return false
}

func (self *P2pSendTransport) Downgrade(source TransferPath) {
	if source.StreamId == self.streamId {
		self.cancel()
	}
}

type P2pReceiveTransport struct {
	transportId Id

	ctx      context.Context
	cancel   context.CancelFunc
	conn     net.Conn
	streamId Id
	receive  chan []byte

	p2pTransportSettings *P2pTransportSettings
}

func NewP2pReceiveTransport(
	ctx context.Context,
	cancel context.CancelFunc,
	conn net.Conn,
	streamId Id,
	p2pTransportSettings *P2pTransportSettings,
) (Transport, Route) {
	receive := make(chan []byte)
	p2pReceiveTransport := &P2pReceiveTransport{
		transportId:          NewId(),
		ctx:                  ctx,
		cancel:               cancel,
		conn:                 conn,
		streamId:             streamId,
		receive:              receive,
		p2pTransportSettings: p2pTransportSettings,
	}
	go p2pReceiveTransport.run()
	return p2pReceiveTransport, receive
}

func (self *P2pReceiveTransport) run() {
	defer self.cancel()

	buffer := make([]byte, self.p2pTransportSettings.MaxMessageSize)

	for {
		select {
		case <-self.ctx.Done():
			return
		default:
		}

		n, err := self.conn.Read(buffer)
		if err != nil {
			return
		}
		if 0 < n {
			transferFrameBytes := make([]byte, n)
			copy(transferFrameBytes, buffer[0:n])
			select {
			case <-self.ctx.Done():
				return
			case self.receive <- transferFrameBytes:
			}
		}
	}
}

func (self *P2pReceiveTransport) TransportId() Id {
	return self.transportId
}

// lower priority takes precedence
func (self *P2pReceiveTransport) Priority() int {
	return 0
}

func (self *P2pReceiveTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}

func (self *P2pReceiveTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	return 1.0
}

func (self *P2pReceiveTransport) MatchesSend(destination TransferPath) bool {
	return false
}

func (self *P2pReceiveTransport) MatchesReceive(destination TransferPath) bool {
	return true
}

func (self *P2pReceiveTransport) Downgrade(source TransferPath) {
	if source.StreamId == self.streamId {
		self.cancel()
	}
}
