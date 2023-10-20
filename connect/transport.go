package connect

import (
	"context"
	"time"
	"net"

	"github.com/gorilla/websocket"

	"bringyour.com/protocol"
)


// note that it is possible to have multiple transports for the same client destination
// e.g. platform, p2p, and a bunch of extenders


// extenders are identified and credited with the platform by ip address
// they forward to a special port, 8443, that whitelists their ip without rate limiting
// when an extender gets an http message from a client, it always connects tcp to connect.bringyour.com:8443
// appends the proxy protocol headers, and then forwards the bytes from the client
// https://docs.nginx.com/nginx/admin-guide/load-balancer/using-proxy-protocol/
// rate limit using $proxy_protocol_addr https://www.nginx.com/blog/rate-limiting-nginx/
// add the source ip as the X-Extender header


const DefaultHttpConnectTimeout = 5 * time.Second
const DefaultWsHandshakeTimeout = 5 * time.Second
const DefaultAuthTimeout = 5 * time.Second
const DefaultReconnectTimeout = 15 * time.Second
const DefaultPingTimeout = 30 * time.Second


type PlatformTransportSettings struct {
	HttpConnectTimeout time.Duration
	WsHandshakeTimeout time.Duration
	AuthTimeout time.Duration
	ReconnectTimeout time.Duration
	PingTimeout time.Duration
}

func DefaultPlatformTransportSettings() *PlatformTransportSettings {
	return &PlatformTransportSettings{
		HttpConnectTimeout: DefaultHttpConnectTimeout,
		WsHandshakeTimeout: DefaultWsHandshakeTimeout,
		AuthTimeout: DefaultAuthTimeout,
		ReconnectTimeout: DefaultReconnectTimeout,
		PingTimeout: DefaultPingTimeout,
	}
}


type ClientAuth struct {
	ByJwt string
	InstanceId Id
	AppVersion string
}


// (ctx, network, address)
type DialContextFunc func(ctx context.Context, network string, address string) (net.Conn, error)


type PlatformTransport struct {
	ctx context.Context
	cancel context.CancelFunc

	platformUrl string
	auth *ClientAuth
	dialContext DialContextFunc
	settings *PlatformTransportSettings
}

func NewPlatformTransportWithDefaults(
	ctx context.Context,
	platformUrl string,
	auth *ClientAuth,
) *PlatformTransport {
	settings := DefaultPlatformTransportSettings()

	dialer := &net.Dialer{
    	Timeout: settings.HttpConnectTimeout,
  	}

  	return NewPlatformTransport(
		ctx,
		platformUrl,
		auth,
		dialer.DialContext,
		settings,
	)
}

func NewPlatformTransportWithExtender(
	ctx context.Context,
	extenderUrl string,
	platformUrl string,
	auth *ClientAuth,
	settings *PlatformTransportSettings,
) *PlatformTransport {
	return NewPlatformTransport(
		ctx,
		platformUrl,
		auth,
		NewExtenderDialContext(extenderUrl, settings),
		settings,
	)
}

func NewPlatformTransport(
	ctx context.Context,
	platformUrl string,
	auth *ClientAuth,
	dialContext DialContextFunc,
	settings *PlatformTransportSettings,
) *PlatformTransport {
	cancelCtx, cancel := context.WithCancel(ctx)
	transport := &PlatformTransport{
		ctx: cancelCtx,
		cancel: cancel,
		platformUrl: platformUrl,
		auth: auth,
		dialContext: dialContext,
		settings: settings,
	}
	return transport
}

func (self *PlatformTransport) Run(routeManager *RouteManager) {
	// connect and update route manager for this transport

	sendTransport := newPlatformSendTransport()
	receiveTransport := newPlatformReceiveTransport()

	defer func() {
		routeManager.RemoveTransport(sendTransport)
		routeManager.RemoveTransport(receiveTransport)
		sendTransport.Close()
		receiveTransport.Close()
	}()

	authBytes, err := EncodeFrame(&protocol.Auth{
		ByJwt: self.auth.ByJwt,
		AppVersion: self.auth.AppVersion,
		InstanceId: self.auth.InstanceId.Bytes(),
	})
	if err != nil {
		return
	}

	for {
		wsDialer := &websocket.Dialer{
			NetDialContext: self.dialContext,
			HandshakeTimeout: self.settings.WsHandshakeTimeout,
		}

		ws, _, err := wsDialer.Dial(self.platformUrl, nil)
		if err != nil {
			select {
			case <- self.ctx.Done():
				return
			case <- time.After(self.settings.ReconnectTimeout):
				continue
			}
		}

		ws.SetWriteDeadline(time.Now().Add(self.settings.AuthTimeout))
		if err := ws.WriteMessage(websocket.BinaryMessage, authBytes); err != nil {
			ws.Close()
			continue
		}
		ws.SetReadDeadline(time.Now().Add(self.settings.AuthTimeout))
		if _, _, err := ws.ReadMessage(); err != nil {
			ws.Close()
			continue
		}

		ws.SetWriteDeadline(time.UnixMilli(0))
		ws.SetReadDeadline(time.UnixMilli(0))

		func() {
			handleCtx, handleCancel := context.WithCancel(self.ctx)

			routeManager.UpdateTransport(sendTransport, []Route{sendTransport.send})
			routeManager.UpdateTransport(receiveTransport, []Route{receiveTransport.receive})

			closeHandle := func() {
				routeManager.RemoveTransport(sendTransport)
				routeManager.RemoveTransport(receiveTransport)
				handleCancel()
				ws.Close()
			}
			defer closeHandle()

			go func() {
				defer closeHandle()

				for {
					select {
					case <- handleCtx.Done():
						return
					default:
					}

					if messageType, message, err := ws.ReadMessage(); err != nil {
						switch messageType {
						case websocket.BinaryMessage:
							select {
							case <- handleCtx.Done():
								return
							case receiveTransport.receive <- message:
							}
						}
					}
				}
			}()

			for {
				select {
				case <- handleCtx.Done():
					return
				case message := <- sendTransport.send:
					if err := ws.WriteMessage(websocket.BinaryMessage, message); err != nil {
						return
					}
				case <- time.After(self.settings.PingTimeout):
					if err := ws.WriteMessage(websocket.PingMessage, nil); err != nil {
		                return
		            }
		        }
			}
		}()

		select {
		case <- self.ctx.Done():
			return
		case <- time.After(self.settings.ReconnectTimeout):
		}		
	}
}

func (self *PlatformTransport) Close() {
	self.cancel()
}


// conforms to `connect.Transport`
type platformSendTransport struct {
	send chan []byte
}

func newPlatformSendTransport() *platformSendTransport {
	return &platformSendTransport{
		send: make(chan []byte),
	}
}

func (self *platformSendTransport) Priority() int {
	return 100
}

func (self *platformSendTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}

func (self *platformSendTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	// uniform weight
	return 1.0 / float32(1 + len(remainingStats))
}

func (self *platformSendTransport) MatchesSend(destinationId Id) bool {
	// the platform can route any destination,
	// since every client has a platform transport
	return true
}

func (self *platformSendTransport) MatchesReceive(destinationId Id) bool {
	return false
}

func (self *platformSendTransport) Downgrade(sourceId Id) {
	// nothing to downgrade
}

func (self *platformSendTransport) Close() {
	close(self.send)
}


// conforms to `connect.Transport`
type platformReceiveTransport struct {
	receive chan []byte
}

func newPlatformReceiveTransport() *platformReceiveTransport {
	return &platformReceiveTransport{
		receive: make(chan []byte),
	}
}

func (self *platformReceiveTransport) Priority() int {
	return 100
}

func (self *platformReceiveTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}

func (self *platformReceiveTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	// uniform weight
	return 1.0 / float32(1 + len(remainingStats))
}

func (self *platformReceiveTransport) MatchesSend(destinationId Id) bool {
	return false
}

func (self *platformReceiveTransport) MatchesReceive(destinationId Id) bool {
	return true
}

func (self *platformReceiveTransport) Downgrade(sourceId Id) {
	// nothing to downgrade
}

func (self *platformReceiveTransport) Close() {
	close(self.receive)
}


// an extender uses an independent url that is hard-coded to forward to the platform
// the `platformUrl` here must match the hard coded url in the extender, which is
// done by using a prior vetted extender
// The connection to the platform is end-to-end encrypted with TLS,
// using the hostname from `platformUrl`


func NewExtenderDialContext(
	extenderUrl string,
	settings *PlatformTransportSettings,
) DialContextFunc {
	return func(
		ctx context.Context,
		network string,
		address string,
	) (net.Conn, error) {
		dialer := &net.Dialer{
	    	Timeout: settings.HttpConnectTimeout,
	  	}

		wsDialer := &websocket.Dialer{
			NetDialContext: dialer.DialContext,
			HandshakeTimeout: settings.WsHandshakeTimeout,
		}

		ws, _, err := wsDialer.Dial(extenderUrl, nil)
		if err != nil {
			return nil, err
		}

		return newWsForwardingConn(ws), nil
	}
}


// conforms to `net.Conn`
type wsForwardingConn struct {
	ws *websocket.Conn
	readBuffer []byte
}

func newWsForwardingConn(ws *websocket.Conn) *wsForwardingConn {
	return &wsForwardingConn{
		ws: ws,
		readBuffer: []byte{},
	}
}

func (self *wsForwardingConn) Read(b []byte) (int, error) {
	i := min(len(b), len(self.readBuffer))
	if 0 < i {
		copy(b, self.readBuffer[:i])
		self.readBuffer = self.readBuffer[i:]
	}
	for i < len(b) {
		messageType, message, err := self.ws.ReadMessage()
		if err != nil {
			return i, err
		}
		switch messageType {
		case websocket.BinaryMessage:
			j := min(len(b) - i, len(message))
			copy(b[i:], message[:j])
			i += j
			if j < len(message) {
				self.readBuffer = append(self.readBuffer, message[j:]...)
			}
		}
	}
	return i, nil
}

func (self *wsForwardingConn) Write(b []byte) (int, error) {
	err := self.ws.WriteMessage(websocket.BinaryMessage, b)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}

func (self *wsForwardingConn) Close() error {
	return self.ws.Close()
}

func (self *wsForwardingConn) LocalAddr() net.Addr {
	return self.ws.LocalAddr()
}

func (self *wsForwardingConn) RemoteAddr() net.Addr {
	return self.ws.RemoteAddr()
}

func (self *wsForwardingConn) SetDeadline(t time.Time) error {
	if err := self.ws.SetReadDeadline(t); err != nil {
		return err
	}
	if err := self.ws.SetWriteDeadline(t); err != nil {
		return err
	}
	return nil
}

func (self *wsForwardingConn) SetReadDeadline(t time.Time) error {
	return self.ws.SetReadDeadline(t)
}

func (self *wsForwardingConn) SetWriteDeadline(t time.Time) error {
	return self.ws.SetWriteDeadline(t)
}

