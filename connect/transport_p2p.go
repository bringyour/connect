package connect



// transport implementation with webrtc
// halving ramp up prioritization as long as the route is sending data at the fraction requested

// a + (1 - a) / 2 = b
// a  + 1/2 - a/2 = b
// a * (1 - 1/2) + 1/2 = b
// a = 2 * (b - 1/2)

// e error
// a + (1 - a) / 2 = 1 - e
// second step
// (a + (1 - a) / 2) + (1 - (a + (1 - a) / 2)) / 2

// n steps
// (2^n - 1 + a)/(2^n)

// Log_2[(1-a)/e]


type PeerType = string
const (
	PeerTypeSource PeerType = "source"
	PeerTypeDestination PeerType = "destination"
)


type P2pTransport struct {
	ctx context.Context
	cancel context.CancelFunc

	client *Client

	sendRouteManager *RouteManager
	receiveRouteManager *RouteManager

	peerId Id
	streamId Id
	peerType PeerType

	sendReady chan struct{}
	receiveReady chan struct{}
}

func NewP2pTransport(
	ctx context.Context,
	client *Client,
	sendRouteManager *RouteManager,
	receiveRouteManager *RouteManager,
	peerId Id,
	streamId Id,
	peerType PeerType,
	sendReady chan struct{},
	receiveReady chan struct{},
) *P2pTransport {
	cancelCtx, cancel := context.WithCancel(ctx)
	p2pTransport := &P2pTransport{
		ctx: cancelCtx,
		cancel: cancel,
		client: client,
		sendRouteManager: sendRouteManager,
		receiveRouteManager: receiveRouteManager,
		peerId: peerId,
		streamId: streamId,
		PeerType: peerType,
		sendReady: sendReady,
		receiveReady: receiveReady,
	}
	return p2pTransport
}


func (self *P2pTransport) run() {
	defer self.cancel()

	for {
		// todo using net.Conn as a stand in for the actual interface
		var conn net.Conn
		// note here, one side of the P2P connection will be driving the setup process
		// this is the active side. Choose one side arbitrarily.
		// Here the sender (peer is destination) is active
		switch peerType {
			case PeerTypeDestination:
				conn = P2pActive(peerId, streamId)
			case PeerTypeSource:
				conn = P2pPassive(peerId, streamId)
			default:
				// unknown peer type
				return
		}

		// at this point, the connection should be able to ping the other side
		// now we wait for the entire stream to be ready to send and receive
		c := func() {
			defer conn.Close()

			handleCtx, handleCancel := context.WithCancel(self.ctx)
			defer handleCancel()

			go func() {
				defer cancel()

				select {
				case <- handleCtx.Done():
					return
				case <- receiveReady:
				}

				conn.SetWriteDeadline(WRITETIMEOUT)
				n, err := conn.Write(READY)
				if err != nil {
					return
				}

				t, route := NewP2pReceiveTransport(handleCtx, handleCancel, conn, streamId)

				receiveRouteManager.UpdateTransport(t, []Route{receive})
				defer receiveRouteManager.RemoveTransport(t)

				select {
				case <- handleCtx.Done():
					return
				}
			}

			go func() {
				defer self.cancel()

				select {
				case <- handleCtx.Done():
					return
				default:
				}

				header := make([]byte, len(READY))
				conn.SetReadDeadline(READTIMEOUT)
				n, err := conn.Read(header)
				if err != nil {
					return
				}
				if n < len(READY) {
					return
				}

				if !slices.Equal(header, READY) {
					return
				}

				close(sendReady)

				t, route := NewP2pSendTransport(handleCtx, handleCancel, conn, streamId)

				sendRouteManager.UpdateTransport(t, []Route{receive})
				defer sendRouteManager.RemoveTransport(t)

				select {
				case <- handleCtx.Done():
					return
				}
			}


			select {
	        case <- self.handleCtx.Done():
	            return
	        }
	    }

    	c()

		select {
        case <- self.ctx.Done():
            return
        case <- time.After(self.settings.ReconnectTimeout):
        }
	}
}


func NewP2pSendTransport(handleCtx, handleCancel, conn, streamId) (Transport, Route) {

}


func NewP2pReceiveTransport(handleCtx, handleCancel, conn, streamId) (Transport, Route) {

}


func P2pActive(peerId, streamId) net.Conn {

}


func P2pPassive(peerId, streamId) net.Conn {

}


