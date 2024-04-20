package connect

import (
    "context"
    "time"
    "net"
    "fmt"
    "bytes"

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
    AuthTimeout time.Duration
    ReconnectTimeout time.Duration
    PingTimeout time.Duration
    WriteTimeout time.Duration
    ReadTimeout time.Duration
    TransportDrainTimeout time.Duration
}


func DefaultPlatformTransportSettings() *PlatformTransportSettings {
    pingTimeout := 5 * time.Second
    return &PlatformTransportSettings{
        HttpConnectTimeout: 2 * time.Second,
        WsHandshakeTimeout: 2 * time.Second,
        AuthTimeout: 2 * time.Second,
        ReconnectTimeout: 5 * time.Second,
        PingTimeout: pingTimeout,
        WriteTimeout: 5 * time.Second,
        ReadTimeout: 2 * pingTimeout,
        TransportDrainTimeout: 30 * time.Second,
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
    ctx context.Context
    cancel context.CancelFunc

    platformUrl string
    auth *ClientAuth
    dialContextGen func()(DialContextFunc)
    settings *PlatformTransportSettings

    routeManager *RouteManager
}

func NewPlatformTransportWithDefaults(
    ctx context.Context,
    platformUrl string,
    auth *ClientAuth,
    routeManager *RouteManager,
) *PlatformTransport {
    settings := DefaultPlatformTransportSettings()

    dialContextGen := func()(DialContextFunc) {
        dialer := &net.Dialer{
            Timeout: settings.HttpConnectTimeout,
        }
        return dialer.DialContext
    }

    return NewPlatformTransport(
        ctx,
        platformUrl,
        auth,
        dialContextGen,
        settings,
        routeManager,
    )
}

func NewPlatformTransportWithExtender(
    ctx context.Context,
    extenderUrl string,
    platformUrl string,
    auth *ClientAuth,
    settings *PlatformTransportSettings,
    routeManager *RouteManager,
) *PlatformTransport {
    return NewPlatformTransport(
        ctx,
        platformUrl,
        auth,
        NewExtenderDialContextGenerator(extenderUrl, settings),
        settings,
        routeManager,
    )
}

func NewPlatformTransport(
    ctx context.Context,
    platformUrl string,
    auth *ClientAuth,
    dialContextGen func()(DialContextFunc),
    settings *PlatformTransportSettings,
    routeManager *RouteManager,
) *PlatformTransport {
    cancelCtx, cancel := context.WithCancel(ctx)
    transport := &PlatformTransport{
        ctx: cancelCtx,
        cancel: cancel,
        platformUrl: platformUrl,
        auth: auth,
        dialContextGen: dialContextGen,
        settings: settings,
        routeManager: routeManager,
    }
    go transport.run()
    return transport
}

func (self *PlatformTransport) run() {
    // connect and update route manager for this transport
    defer self.cancel()

    clientId, _ := self.auth.ClientId()

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
            NetDialContext: self.dialContextGen(),
            HandshakeTimeout: self.settings.WsHandshakeTimeout,
        }

        ws, err := func()(*websocket.Conn, error) {
            ws, _, err := wsDialer.DialContext(self.ctx, self.platformUrl, nil)
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
        }()
        if err != nil {
            glog.Infof("[t]auth error %s = %s\n", clientId, err)
            select {
            case <- self.ctx.Done():
                return
            case <- time.After(self.settings.ReconnectTimeout):
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
            sendTransport := NewSendGatewayTransport()
            receiveTransport := NewReceiveGatewayTransport()

            self.routeManager.UpdateTransport(sendTransport, []Route{send})
            self.routeManager.UpdateTransport(receiveTransport, []Route{receive})

            defer func() {
                self.routeManager.RemoveTransport(sendTransport)
                self.routeManager.RemoveTransport(receiveTransport)

                go func() {
                    select {
                    case <- handleCtx.Done():
                    case <- time.After(self.settings.TransportDrainTimeout):
                    }

                    close(send)
                }()
            }()

            go func() {
                defer handleCancel()

                for {
                    select {
                    case <- handleCtx.Done():
                        return
                    case message, ok := <- send:
                        if !ok {
                            return
                        }

                        ws.SetWriteDeadline(time.Now().Add(self.settings.WriteTimeout))
                        if err := ws.WriteMessage(websocket.BinaryMessage, message); err != nil {
                            // note that for websocket a dealine timeout cannot be recovered
                            glog.V(2).Infof("[ts]%s-> error = %s\n", clientId, err)
                            return
                        }
                        glog.V(2).Infof("[ts]%s->\n", clientId)
                    case <- time.After(self.settings.PingTimeout):
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
                    case <- handleCtx.Done():
                        return
                    default:
                    }

                    ws.SetReadDeadline(time.Now().Add(self.settings.ReadTimeout))
                    messageType, message, err := ws.ReadMessage()
                    if err != nil {
                        glog.V(2).Infof("[tr]%s<- error = %s\n", clientId, err)
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
                        case <- handleCtx.Done():
                            return
                        case receive <- message:
                            glog.V(2).Infof("[tr]%s<-\n", clientId)
                        case <- time.After(self.settings.ReadTimeout):
                            glog.Infof("[tr]drop %s<-\n", clientId)
                        }
                    }
                }
            }()

            select {
            case <- handleCtx.Done():
            }
        }
        if glog.V(2) {
            Trace(fmt.Sprintf("[t]connect %s", clientId), c)
        } else {
            c()
        }

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


// an extender uses an independent url that is hard-coded to forward to the platform
// the `platformUrl` here must match the hard coded url in the extender, which is
// done by using a prior vetted extender
// The connection to the platform is end-to-end encrypted with TLS,
// using the hostname from `platformUrl`


func NewExtenderDialContextGenerator(
    extenderUrl string,
    settings *PlatformTransportSettings,
) func()(DialContextFunc) {
    return func()(DialContextFunc) {
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

            ws, _, err := wsDialer.DialContext(ctx, extenderUrl, nil)
            if err != nil {
                return nil, err
            }

            return newWsForwardingConn(ws, settings), nil
        }
    }
}


// conforms to `net.Conn`
type wsForwardingConn struct {
    ws *websocket.Conn
    readBuffer []byte
    settings *PlatformTransportSettings
}

func newWsForwardingConn(ws *websocket.Conn, settings *PlatformTransportSettings) *wsForwardingConn {
    return &wsForwardingConn{
        ws: ws,
        readBuffer: make([]byte, 0),
        settings: settings,
    }
}

func (self *wsForwardingConn) Read(b []byte) (int, error) {
    i := min(len(b), len(self.readBuffer))
    if 0 < i {
        copy(b, self.readBuffer[:i])
        self.readBuffer = self.readBuffer[i:]
    }
    for i < len(b) {
        self.ws.SetReadDeadline(time.Now().Add(self.settings.ReadTimeout))
        messageType, message, err := self.ws.ReadMessage()
        if err != nil {
            return i, err
        }
        switch messageType {
        case websocket.BinaryMessage:
            if 0 == len(message) {
                // ping
                continue
            }

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
    self.ws.SetWriteDeadline(time.Now().Add(self.settings.WriteTimeout))
    err := self.ws.WriteMessage(websocket.BinaryMessage, b)
    if err != nil {
        // note that for websocket a dealine timeout cannot be recovered
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

