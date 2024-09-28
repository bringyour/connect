package connect

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"time"

	"github.com/gorilla/websocket"

	"github.com/golang/glog"

	"bringyour.com/protocol"
)

const TransportBufferSize = 1

// note that it is possible to have multiple transports for the same client destination
// e.g. platform, p2p, and a bunch of extenders

// extenders are identified and credited with the platform by ip address
// they forward to a special port, 8443, that whitelists their ip without rate limiting
// when an extender gets an http message from a client, it always connects tcp to connect.bringyour.com:8443
// appends the proxy protocol headers, and then forwards the bytes from the client
// https://docs.nginx.com/nginx/admin-guide/load-balancer/using-proxy-protocol/
// rate limit using $proxy_protocol_addr https://www.nginx.com/blog/rate-limiting-nginx/
// add the source ip as the X-Extender header

type PlatformTransportSettings struct {
	HttpConnectTimeout time.Duration
	WsHandshakeTimeout time.Duration
	AuthTimeout        time.Duration
	ReconnectTimeout   time.Duration
	PingTimeout        time.Duration
	WriteTimeout       time.Duration
	ReadTimeout        time.Duration
	TransportGenerator func() (sendTransport Transport, receiveTransport Transport)
}

func DefaultPlatformTransportSettings() *PlatformTransportSettings {
	pingTimeout := 1 * time.Second
	return &PlatformTransportSettings{
		HttpConnectTimeout: 2 * time.Second,
		WsHandshakeTimeout: 2 * time.Second,
		AuthTimeout:        2 * time.Second,
		ReconnectTimeout:   5 * time.Second,
		PingTimeout:        pingTimeout,
		WriteTimeout:       5 * time.Second,
		ReadTimeout:        15 * time.Second,
	}
}

type ClientAuth struct {
	ByJwt string
	// ClientId Id
	InstanceId Id
	AppVersion string
}

func (self *ClientAuth) ClientId() (Id, error) {
	byJwt, err := ParseByJwtUnverified(self.ByJwt)
	if err != nil {
		return Id{}, err
	}
	return byJwt.ClientId, nil
}

// (ctx, network, address)
type DialContextFunc func(ctx context.Context, network string, address string) (net.Conn, error)

type PlatformTransport struct {
	ctx    context.Context
	cancel context.CancelFunc

	clientStrategy *ClientStrategy
	routeManager   *RouteManager

	platformUrl string
	auth        *ClientAuth

	settings *PlatformTransportSettings
}

func NewPlatformTransportWithDefaults(
	ctx context.Context,
	clientStrategy *ClientStrategy,
	routeManager *RouteManager,
	platformUrl string,
	auth *ClientAuth,
) *PlatformTransport {
	return NewPlatformTransport(
		ctx,
		clientStrategy,
		routeManager,
		platformUrl,
		auth,
		DefaultPlatformTransportSettings(),
	)
}

func NewPlatformTransport(
	ctx context.Context,
	clientStrategy *ClientStrategy,
	routeManager *RouteManager,
	platformUrl string,
	auth *ClientAuth,
	settings *PlatformTransportSettings,
) *PlatformTransport {
	cancelCtx, cancel := context.WithCancel(ctx)
	transport := &PlatformTransport{
		ctx:            cancelCtx,
		cancel:         cancel,
		clientStrategy: clientStrategy,
		routeManager:   routeManager,
		platformUrl:    platformUrl,
		auth:           auth,
		settings:       settings,
	}
	go transport.run()
	return transport
}

func (self *PlatformTransport) run() {
	// connect and update route manager for this transport
	defer self.cancel()

	clientId, _ := self.auth.ClientId()

	authBytes, err := EncodeFrame(&protocol.Auth{
		ByJwt:      self.auth.ByJwt,
		AppVersion: self.auth.AppVersion,
		InstanceId: self.auth.InstanceId.Bytes(),
	})
	if err != nil {
		return
	}

	for {
		reconnect := NewReconnect(self.settings.ReconnectTimeout)
		connect := func() (*websocket.Conn, error) {
			ws, _, err := self.clientStrategy.WsDialContext(self.ctx, self.platformUrl, nil)
			if err != nil {
				return nil, err
			}

			success := false
			defer func() {
				if !success {
					ws.Close()
				}
			}()

			ws.SetWriteDeadline(time.Now().Add(self.settings.AuthTimeout))
			if err := ws.WriteMessage(websocket.BinaryMessage, authBytes); err != nil {
				return nil, err
			}
			ws.SetReadDeadline(time.Now().Add(self.settings.AuthTimeout))
			if messageType, message, err := ws.ReadMessage(); err != nil {
				return nil, err
			} else {
				// verify the auth echo
				switch messageType {
				case websocket.BinaryMessage:
					if !bytes.Equal(authBytes, message) {
						return nil, fmt.Errorf("Auth response error: bad bytes.")
					}
				default:
					return nil, fmt.Errorf("Auth response error.")
				}
			}

			success = true
			return ws, nil
		}

		var ws *websocket.Conn
		var err error
		if glog.V(2) {
			ws, err = TraceWithReturnError(fmt.Sprintf("[t]connect %s", clientId), connect)
		} else {
			ws, err = connect()
		}
		if err != nil {
			glog.Infof("[t]auth error %s = %s\n", clientId, err)
			select {
			case <-self.ctx.Done():
				return
			case <-reconnect.After():
				continue
			}
		}

		c := func() {
			defer ws.Close()

			handleCtx, handleCancel := context.WithCancel(self.ctx)
			defer handleCancel()

			send := make(chan []byte, TransportBufferSize)
			receive := make(chan []byte, TransportBufferSize)

			// the platform can route any destination,
			// since every client has a platform transport
			var sendTransport Transport
			var receiveTransport Transport
			if self.settings.TransportGenerator != nil {
				sendTransport, receiveTransport = self.settings.TransportGenerator()
			} else {
				sendTransport = NewSendGatewayTransport()
				receiveTransport = NewReceiveGatewayTransport()
			}

			self.routeManager.UpdateTransport(sendTransport, []Route{send})
			self.routeManager.UpdateTransport(receiveTransport, []Route{receive})

			defer func() {
				self.routeManager.RemoveTransport(sendTransport)
				self.routeManager.RemoveTransport(receiveTransport)

				// note `send` is not closed. This channel is left open.
				// it used to be closed after a delay, but it is not needed to close it.
			}()

			go func() {
				defer handleCancel()

				for {
					select {
					case <-handleCtx.Done():
						return
					case message, ok := <-send:
						if !ok {
							return
						}

						ws.SetWriteDeadline(time.Now().Add(self.settings.WriteTimeout))
						if err := ws.WriteMessage(websocket.BinaryMessage, message); err != nil {
							// note that for websocket a dealine timeout cannot be recovered
							glog.Infof("[ts]%s-> error = %s\n", clientId, err)
							return
						}
						glog.V(2).Infof("[ts]%s->\n", clientId)
					case <-time.After(self.settings.PingTimeout):
						ws.SetWriteDeadline(time.Now().Add(self.settings.WriteTimeout))
						if err := ws.WriteMessage(websocket.BinaryMessage, make([]byte, 0)); err != nil {
							// note that for websocket a dealine timeout cannot be recovered
							return
						}
					}
				}
			}()

			go func() {
				defer func() {
					handleCancel()
					close(receive)
				}()

				for {
					select {
					case <-handleCtx.Done():
						return
					default:
					}

					ws.SetReadDeadline(time.Now().Add(self.settings.ReadTimeout))
					messageType, message, err := ws.ReadMessage()
					if err != nil {
						glog.Infof("[tr]%s<- error = %s\n", clientId, err)
						return
					}

					switch messageType {
					case websocket.BinaryMessage:
						if 0 == len(message) {
							// ping
							glog.V(2).Infof("[tr]ping %s<-\n", clientId)
							continue
						}

						select {
						case <-handleCtx.Done():
							return
						case receive <- message:
							glog.V(2).Infof("[tr]%s<-\n", clientId)
						case <-time.After(self.settings.ReadTimeout):
							glog.Infof("[tr]drop %s<-\n", clientId)
						}
					default:
						glog.V(2).Infof("[tr]other=%s %s<-\n", messageType, clientId)
					}
				}
			}()

			select {
			case <-handleCtx.Done():
			}
		}
		reconnect = NewReconnect(self.settings.ReconnectTimeout)
		if glog.V(2) {
			Trace(fmt.Sprintf("[t]connect run %s", clientId), c)
		} else {
			c()
		}
		select {
		case <-self.ctx.Done():
			return
		case <-reconnect.After():
		}
	}
}

func (self *PlatformTransport) Close() {
	self.cancel()
}
