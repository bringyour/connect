package connect


// FIXME switch to big endian order
// FIXME the extender context should be a tls context. do the tls handshake at the end. 
// this is easier to understand and matches the resilient context

import (
    "context"
    "net"
    "net/http"

    // "os"
    "strings"
    "time"
    "fmt"
    "strconv"
    // "slices"


    "crypto/tls"
    "crypto/ecdsa"
    "crypto/ed25519"
    // "crypto/elliptic"
    "crypto/rand"
    "crypto/rsa"
    "crypto/x509"
    "crypto/x509/pkix"
    "encoding/pem"
    // "encoding/json"
    // "flag"
    "log"
    "math/big"


    // "crypto/md5"
    "encoding/binary"
    // "encoding/hex"
    // "syscall"

    mathrand "math/rand"

    

    // "golang.org/x/crypto/cryptobyte"
    "golang.org/x/net/idna"

    "google.golang.org/protobuf/proto"
    quic "github.com/quic-go/quic-go"

    // "src.agwa.name/tlshacks"

    "bringyour.com/protocol"
)



// an extender uses an independent url that is hard-coded to forward to the platform
// the `platformUrl` here must match the hard coded url in the extender, which is
// done by using a prior vetted extender
// The connection to the platform is end-to-end encrypted with TLS,
// using the hostname from `platformUrl`








// in toolbar of app, including login screen, be able to set extender list


const ReadTimeout = 30 * time.Second
const WriteTimeout = 30 * time.Second

const ValidFrom = 180 * 24 * time.Hour
const ValidFor = 180 * 24 * time.Hour


type ExtenderConnectMode string
const (
    ExtenderConnectModeTcpTls ExtenderConnectMode = "tcptls"
    ExtenderConnectModeQuic ExtenderConnectMode = "quic"
    ExtenderConnectModeUdp ExtenderConnectMode = "udp"
)


type ExtenderConfig struct {
    ExtenderSecrets []string
    SpoofHosts []string
    ExtenderIps []net.IP
    ExtenderPorts map[int][]ExtenderConnectMode
    
    // FIXME reliable udp config
}


func TestExtenderConfig() *ExtenderConfig {
    return &ExtenderConfig{
        ExtenderSecrets: []string{"moonshot"},
        SpoofHosts: []string{"example.org"},
        ExtenderIps: []net.IP{
            // net.ParseIP("185.209.49.33"),
            // net.ParseIP("94.141.96.105"),
            // net.ParseIP("78.140.246.78"),
            net.ParseIP("92.246.130.21"),
        },
        ExtenderPorts: []int{
            53,
            // https and secure dns
            // 443,
            // // alt https
            // 8443,
            /*
            // dns
            853,
            // ldap
            636,
            // ftp
            989,
            990,
            // telnet
            992,
            // irc
            994,
            // docker
            2376,
            // ldap
            3269,
            // sip
            5061,
            // powershell
            5986,
            // tor
            9001,
            // imap
            993,
            // pop
            995,
            // smtp
            465,
            // ntp, nts
            4460,
            // cpanel
            2083,
            2096,
            // webhost mail
            2087,
            */
        },
    }
}



func NewExtenderHttpClient(
    connectMode ExtenderConnectMode,
    config *ExtenderConfig,
    tlsConfig *tls.Config,
) *http.Client {
    dialer := &net.Dialer{
        Timeout:   30 * time.Second,
        KeepAlive: 30 * time.Second,
    }
    transport := &http.Transport{
        DialContext: NewExtenderDialContext(connectMode, dialer, config),
        TLSClientConfig: tlsConfig,
        ForceAttemptHTTP2:     true,
        MaxIdleConns:          100,
        IdleConnTimeout:       90 * time.Second,
        TLSHandshakeTimeout:   10 * time.Second,
        ExpectContinueTimeout: 1 * time.Second,
    }
    return &http.Client{
        Transport: transport,
        Timeout: 90 * time.Second,
    }
}


// 
// newResilientHttpClient




// client

// http client can plug in DialContext as part of Transport
// https://cs.opensource.google/go/go/+/refs/tags/go1.22.4:src/net/http/transport.go;l=95

// create a tls connect to (destinationHost, destinationPort) on the connection
// returned by this
// the returned connection is not a tls connection
func NewExtenderTlsDialContext(
    dialer *net.Dialer,
    config *ExtenderConfig,
    tlsConfig *tls.Config,
) DialContextFunc {
    return func(
        ctx context.Context,
        network string,
        address string,
    ) (net.Conn, error) {
        if network != "tcp" {
            panic(fmt.Errorf("Extender only support tcp network."))
        }

        host, portStr, err := net.SplitHostPort(address)
        if err != nil {
            panic(err)
        }
        port, err := strconv.Atoi(portStr)
        if err != nil {
            panic(err)
        }
        // fmt.Printf("EXTEND TO %s:%d\n", host, port)

        extenderIp := config.ExtenderIps[mathrand.Intn(len(config.ExtenderIps))]
        extenderPort := config.ExtenderPorts[mathrand.Intn(len(config.ExtenderPorts))]
        authority := net.JoinHostPort(
            extenderIp.String(),
            fmt.Sprintf("%d", extenderPort),
        )


        spoofHost := config.SpoofHosts[mathrand.Intn(len(config.SpoofHosts))]


        // fmt.Printf("Extender client 1\n")


        // fmt.Printf("Extender client 2\n")

        // fragmentConn(conn)
        // fragment handshake records
        // set ttl 0 on every other handshake records

        var serverConn net.Conn

        extenderTlsConfig := &tls.Config{
            ServerName: spoofHost,
            InsecureSkipVerify: true,
            // require 1.3 to mask self-signed certs
            MinVersion: tls.VersionTLS13,
        }

        if connectMode == ExtenderConnectModeTcpTls {

            conn, err := dialer.Dial("tcp", authority)
            if err != nil {
                return nil, err
            }

            rconn := NewResilientTlsConn(conn)
            tlsServerConn := tls.Client(
                rconn,
                // conn,
                extenderTlsConfig,
            )

            err = tlsServerConn.HandshakeContext(ctx)
            if err != nil {
                return nil, err
            }
            // once the stream is established, no longer need the resilient features
            rconn.Off()

            serverConn = tlsServerConn

            // fragmentConn.Off()

            // fmt.Printf("Extender client 3\n")

        } else if connectMode == ExtenderConnectModeQuic {
            // quic

            quicConfig := &quic.Config{}
            conn, err := quic.DialAddr(ctx, authority, extenderTlsConfig, quicConfig)
            if err != nil {
                return nil, err
            }

            fmt.Printf("quic conn\n")

            stream, err := conn.OpenStream()
            if err != nil {
                return nil, err
            }

            fmt.Printf("quic stream\n")

            serverConn = newStreamConn(stream)

        } else if connectMode == ExtenderConnectModeUdp {

            conn, err := dialer.Dial("udp", authority)
            if err != nil {
                return nil, err
            }

            serverConn = newClientPacketStream(conn, UdpMtu)



        } else {
            panic(fmt.Errorf("bad connect mode %s", connectMode))
        }


        extenderSecret := config.ExtenderSecrets[mathrand.Intn(len(config.ExtenderSecrets))]
        headerMessageBytes, err := proto.Marshal(&protocol.ExtenderHeader{
            Secret: extenderSecret,
            DestinationHost: host,
            DestinationPort: uint32(port),
        })
        if err != nil {
            return nil, err
        }

        // fmt.Printf("Extender client 4\n")

        headerBytes := make([]byte, 4 + len(headerMessageBytes))
        binary.LittleEndian.PutUint32(headerBytes[0:4], uint32(len(headerMessageBytes)))
        copy(headerBytes[4:4+len(headerMessageBytes)], headerMessageBytes)
        _, err = serverConn.Write(headerBytes)
        if err != nil {
            return nil, err
        }

        // fmt.Printf("Extender client 5\n")

        // return serverConn, nil

        tlsServerConn := tls.Client(
            serverConn,
            tlsConfig,
        )

        err = tlsServerConn.HandshakeContext(ctx)
        if err != nil {
            return nil, err
        }

        return tlsServerConn, nil
    }
}




// basic flow control protocol
// [packet_index,length,total_length]
// server buffers m lowest sized packets
// ack sent of min buffer on each ack
// client resend packet after fixed timeout if no ack


type clientPacketStream struct {
    conn net.Conn
    mtu int

    buffer []byte
}

func newClientPacketStream(conn net.Conn, mtu int) *clientPacketStream {



}


func (self *clientPacketStream) Read(b []byte) (int, error) {
    // read from buffer
    // if empty, read from conn into buffer
    if len(b) == 0 {
        return 0, nil
    }

    if len(self.buffer) == 0 {
        readBuffer := make([]byte, UdpMtu)
        n, err := self.conn.Read(readBuffer)
        if err != nil {
            return 0, err
        }
        self.buffer = append(self.buffer, readBuffer[0:n]...)
    }

    m = min(len(self.buffer), len(b))
    if 0 < m {
        copy(b[0:m], self.buffer[0:m])
        self.buffer = self.buffer[m:]
        
    }
    return m, nil
}

func (self *clientPacketStream) Write(b []byte) (int, error) {
    // packetize and write to conn

    m := 0
    for i := 0; i < len(b); i += self.mtu {
        j := min(len(b), i + self.mtu)
        n, err := self.conn.Write(b[i:j])
        m += n
        if err != nil {
            return m, err
        }
    }

    return m, err

}

func (self *clientPacketStream) Close() error {
    return self.stream.Close()
}

func (self *clientPacketStream) LocalAddr() net.Addr {
    return self.conn.LocalAddr()
}

func (self *clientPacketStream) RemoteAddr() net.Addr {
    return self.conn.RemoteAddr()
}

func (self *clientPacketStream) SetDeadline(t time.Time) error {
    return self.stream.SetDeadline(t)
}

func (self *clientPacketStream) SetReadDeadline(t time.Time) error {
    return self.stream.SetReadDeadline(t)
}

func (self *clientPacketStream) SetWriteDeadline(t time.Time) error {
    return self.stream.SetWriteDeadline(t)
} 














type serverPacketStream struct {
    packetConn net.PacketConn
    addr net.Addr
    mtu int
    
    mutex *sync.Mutex
    bufferCond *sync.Cond
    buffer []byte
}

func newServerPacketStream(packetConn net.PacketConn, addr net.Addr, mtu int) *serverPacketStream {

}

func Add(b []byte) {
    self.mutex.Lock()
    defer self.mutex.Unlock()
    self.buffer = append(self.buffer, b...)
    self.bufferCond.Broadcast()
}


func (self *serverPacketStream) Read(b []byte) (int, error) {
    if len(b) == 0 {
        return 0, nil
    }
    self.mutex.Lock()
    defer self.mutex.Unlock()
    for len(self.buffer) == 0 {
        self.bufferCond.Wait()
    }
    m := min(len(self.buffer), len(b))
    copy(b[0:m], self.buffer[0:m])
    self.buffer = self.buffer[m:]
    return m, nil
}

func (self *serverPacketStream) Write(b []byte) (int, error) {
    // packetize and write to conn

    m := 0
    for i := 0; i < len(b); i += self.mtu {
        j := min(len(b), i + self.mtu)
        n, err := self.conn.Write(b[i:j])
        m += n
        if err != nil {
            return m, err
        }
    }

    return m, err
}

func (self *serverPacketStream) Close() error {
    return self.conn.Close()
}

func (self *serverPacketStream) LocalAddr() net.Addr {
    return self.conn.LocalAddr()
}

func (self *serverPacketStream) RemoteAddr() net.Addr {
    return self.conn.RemoteAddr()
}

func (self *serverPacketStream) SetDeadline(t time.Time) error {
    return self.conn.SetDeadline(t)
}

func (self *serverPacketStream) SetReadDeadline(t time.Time) error {
    return self.conn.SetReadDeadline(t)
}

func (self *serverPacketStream) SetWriteDeadline(t time.Time) error {
    return self.conn.SetWriteDeadline(t)
}







// server listens for a tls connect and replies with a self-signed cert
// server set up to forward to only subdomains of a root domain
// otherwise close connection

// https://go.dev/src/crypto/tls/generate_cert.go


type ExtenderServer struct {
    ctx context.Context
    cancel context.CancelFunc

    allowedSecrets []string
    // exact (x) or wildcard (*.x)
    // wildcard *.x does not match exact x
    allowedHosts []string
    ports map[int][]ExtenderConnectMode
    forwardDialer *net.Dialer
}

func NewExtenderServer(ctx context.Context, allowedSecrets []string, allowedHosts []string, ports []int, forwardDialer *net.Dialer) *ExtenderServer {


    cancelCtx, cancel := context.WithCancel(ctx)

    return &ExtenderServer{
        ctx: cancelCtx,
        cancel: cancel,
        allowedSecrets: allowedSecrets,
        allowedHosts: allowedHosts,
        ports: ports,
        forwardDialer: forwardDialer,
    }


}

func (self *ExtenderServer) ListenAndServe() error {

    listeners := map[int]net.Listener{}
    quicListeners := map[int]*quic.Listener{}

    for _, port := range self.ports {
        fmt.Printf("listen tcp %d\n", port)
        listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
        if err != nil {
            fmt.Printf("%s\n", err)
            return err
        }
        listeners[port] = listener
        go func() {
            defer self.cancel()


            for {
                select {
                case <- self.ctx.Done():
                    return
                default:
                }

                conn, err := listener.Accept()
                if err != nil {
                    fmt.Printf("%s\n", err)
                    return
                }
                // fmt.Printf("Extender pre\n")
                go self.HandleExtenderConnection(self.ctx, conn)
            }
        }()
    }
    
    for _, port := range self.ports {
        fmt.Printf("listen quic %d\n", port)
        // certPemBytes, keyPemBytes, err := selfSign(
        //     []string{"example.org"},
        //     guessOrganizationName("example.org"),
        // )
        // if err != nil {
        //     return err
        // }
        // // X509KeyPair
        // cert, err := tls.X509KeyPair(certPemBytes, keyPemBytes)
        // if err != nil {
        //     return err
        // }

        tlsConfig := &tls.Config{
            GetConfigForClient: func(clientHello *tls.ClientHelloInfo) (*tls.Config, error) {
                certPemBytes, keyPemBytes, err := selfSign(
                    []string{clientHello.ServerName},
                    guessOrganizationName(clientHello.ServerName),
                )
                if err != nil {
                    return nil, err
                }
                // X509KeyPair
                cert, err := tls.X509KeyPair(certPemBytes, keyPemBytes)
                return &tls.Config{
                    Certificates: []tls.Certificate{cert},
                }, err
            },
        }
        quicConfig := &quic.Config{}
        listener, err := quic.ListenAddr(fmt.Sprintf(":%d", port), tlsConfig, quicConfig)
        if err != nil {
            fmt.Printf("%s\n", err)
            return err
        }
        quicListeners[port] = listener
        go func() {
            defer self.cancel()


            for {
                select {
                case <- self.ctx.Done():
                    return
                default:
                }

                conn, err := listener.Accept(self.ctx)
                if err != nil {
                    fmt.Printf("%s\n", err)
                    return
                }
                // fmt.Printf("Extender pre\n")
                go self.HandleQuicExtenderConnection(self.ctx, conn)
            }
        }()
    }

    for _, port := range self.ports {
        fmt.Printf("listen udp %d\n", port)
        packetConn, err := net.ListenPacket("udp", fmt.Sprintf(":%d", port))
        go func() {
            defer self.cancel()

            packetStreams := map[src]*packetStream{}

            buffer := make([]byte, 4096)

            for {
                select {
                case <- self.ctx.Done():
                    return
                default:
                }



                n, addr, err := packetConn.ReadFrom(buffer)
                if err != nil {
                    fmt.Printf("%s\n", err)
                    return
                }

                address := addr.String()

                packetSteam, ok := packetStreams[address]
                if !ok {
                    packetStream = newServerPacketStream(packetConn, addr, UdpMtu)
                    // fixme clean up packet stream
                    // go func() {
                    //     select {
                    //     case <- packetStream.Done():
                    //     }

                    // }()
                    go HandleExtenderConnection(self.ctx, packetStream)
                }

                packetStream.AddPacket(buffer[0:n])
            }

        }()
    }


    select {
    case <- self.ctx.Done():
    }
    for _, listener := range listeners {
        listener.Close()
    }
    for _, listener := range quicListeners {
        listener.Close()
    }

    return nil
}

func (self *ExtenderServer) Close() {
    self.cancel()
}

func (self *ExtenderServer) IsAllowedSecret(secret string) bool {
    for _, allowedSecret := range self.allowedSecrets {
        if allowedSecret == secret {
            return true
        }
    }
    return false
}

func (self *ExtenderServer) IsAllowedHost(host string) bool {
    _, err := idna.ToUnicode(host)
    if err != nil {
        // not a valid host
        return false
    }
    for _, allowedHost := range self.allowedHosts {
        if host == allowedHost {
            return true
        }
        if strings.HasPrefix(allowedHost, "*.") {
            if strings.HasSuffix(host, allowedHost[1:]) {
                return true
            }
        }
    }
    return false
}

func (self *ExtenderServer) HandleExtenderConnection(ctx context.Context, conn net.Conn) {

    handleCtx, handleCancel := context.WithCancel(ctx)
    defer handleCancel()

    defer conn.Close()

    // fmt.Printf("Extender 1\n")




    /*
    handshakeBytes, clientHello, err := func()([]byte, *tlshacks.ClientHelloInfo, error) {
        handshakeBytes := make([]byte, 8192)
        handshakeBytesCount := 0
        for handshakeBytesCount < len(handshakeBytes) {
            // wait a short time for fragmented packets
            conn.SetReadDeadline(time.Now().Add(ReadTimeout))
            n, err := conn.Read(handshakeBytes[handshakeBytesCount:])
            if err != nil {
                return nil, nil, err
            }
            handshakeBytesCount += n

            fmt.Printf("Extender handshake 1: %s\n", string(handshakeBytes[0:handshakeBytesCount]))
            clientHello := UnmarshalClientHello(handshakeBytes[0:handshakeBytesCount])
            if clientHello != nil {
                return handshakeBytes[0:handshakeBytesCount], clientHello, nil
            }
            fmt.Printf("Extender handshake deepen\n")
        }
        return nil, nil, fmt.Errorf("Did not read complete handshake after %d bytes.", len(handshakeBytes))
    }()	
    if err != nil {
        return
    }
    */
    /*
    recordReader := newReaderRecordInitialBytes(conn)
    handshakeReader := tlshacks.NewHandshakeReader(recordReader)
    handshakeBytes, err := handshakeReader.ReadMessage()
    if err != nil {
        return
    }

    clientHello := UnmarshalClientHello(handshakeBytes)
    if clientHello == nil {
        return
    }



    fmt.Printf("Extender 2\n")

    if clientHello.Info.ServerName == nil {
        return
    }


    fmt.Printf("Extender 3: %s\n", *clientHello.Info.ServerName)

	// generate a cert for that server name

	// start a tls server connection using the cert and pass in the hello bytes
	// pass in future bytes to the connection

    certPemBytes, keyPemBytes, err := selfSign(
        []string{*clientHello.Info.ServerName},
        guessOrganizationName(*clientHello.Info.ServerName),
    )
    if err != nil {
        return
    }
    // X509KeyPair
    cert, err := tls.X509KeyPair(certPemBytes, keyPemBytes)
    if err != nil {
        return
    }

    fmt.Printf("Extender 4 with initial bytes: %s\n", string(recordReader.InitialBytes()))
    fmt.Printf("Cert: %s\n\n", string(certPemBytes))
    fmt.Printf("Key: %s\n\n", string(keyPemBytes))



	// todo need a net.COnn implementation that allows inserting bytes back at the front
	

    tlsConfig := &tls.Config{
        Certificates: []tls.Certificate{cert},
        ServerName: *clientHello.Info.ServerName,
    }
    // put the handshake bytes back in front
    rewindConn := newConnWithInitialBytes(conn, recordReader.InitialBytes())
	clientConn := tls.Server(rewindConn, tlsConfig)
    defer clientConn.Close()
    */


    tlsConfig := &tls.Config{
        GetCertificate: func(clientHello *tls.ClientHelloInfo) (*tls.Certificate, error) {
            certPemBytes, keyPemBytes, err := selfSign(
                []string{clientHello.ServerName},
                guessOrganizationName(clientHello.ServerName),
            )
            if err != nil {
                return nil, err
            }
            // X509KeyPair
            cert, err := tls.X509KeyPair(certPemBytes, keyPemBytes)
            return &cert, err
        },
    }
    clientConn := tls.Server(conn, tlsConfig)
    defer clientConn.Close()

    // fmt.Printf("Extender 5\n")

    err := clientConn.HandshakeContext(handleCtx)
    if err != nil {
        return
    }

    // fmt.Printf("Extender 6\n")


	// read extender header
    headerBytes := make([]byte, 1024)


    // TODO is header parsing doesn't work, forward the traffic to the SNI site and write the header bytes




    clientConn.SetReadDeadline(time.Now().Add(ReadTimeout))
    for i := 0; i < 4; {
        n, err := clientConn.Read(headerBytes[i:4])
        if err != nil {
            return
        }
        i += n
    }
    headerByteCount := int(binary.LittleEndian.Uint32(headerBytes[0:4]))
    if 1024 < headerByteCount {
        // bad data
        return
    }
    // fmt.Printf("Extender 6: %d\n", headerByteCount)
    for i := 0; i < headerByteCount; {
        clientConn.SetReadDeadline(time.Now().Add(ReadTimeout))
        n, err := clientConn.Read(headerBytes[i:headerByteCount])
        if err != nil {
            return
        }
        i += n
    }
    // fmt.Printf("Extender 7\n")

    header := &protocol.ExtenderHeader{}
    err = proto.Unmarshal(headerBytes[0:headerByteCount], header)
    if err != nil {
        return
    }

    if !self.IsAllowedSecret(header.Secret) {
        // fmt.Printf("Extender secret failed: %s\n", header.Secret)
        return
    }

    if !self.IsAllowedHost(header.DestinationHost) {
        // fmt.Printf("Extender destination failed: %s\n", header.DestinationHost)
        return
    }

    forwardConn, err := self.forwardDialer.Dial("tcp", net.JoinHostPort(
        header.DestinationHost,
        fmt.Sprintf("%d", header.DestinationPort),
    ))
    if err != nil {
        return
    }
    defer forwardConn.Close()


    go func() {
        // read packet from clientConn, write to forwardConn
        defer handleCancel()

        buffer := make([]byte, 4096)

        for {
            select {
            case <- handleCtx.Done():
                return
            default:
            }

            clientConn.SetReadDeadline(time.Now().Add(ReadTimeout))
            n, err := clientConn.Read(buffer)
            if err != nil {
                return
            }
            forwardConn.SetWriteDeadline(time.Now().Add(WriteTimeout))
            _, err = forwardConn.Write(buffer[0:n])
            if err != nil {
                return
            }
        }
    }()


    go func() {
        // read packet from forwardConn, write to clientConn
        defer handleCancel()

        buffer := make([]byte, 4096)

        for {
            select {
            case <- handleCtx.Done():
                return
            default:
            }

            forwardConn.SetReadDeadline(time.Now().Add(ReadTimeout))
            n, err := forwardConn.Read(buffer)
            if err != nil {
                return
            }
            clientConn.SetWriteDeadline(time.Now().Add(WriteTimeout))
            _, err = clientConn.Write(buffer[0:n])
            if err != nil {
                return
            }
        }
    }()


    select {
    case <- handleCtx.Done():
    }
}



func (self *ExtenderServer) HandleQuicExtenderConnection(ctx context.Context, conn quic.Connection) {

    fmt.Printf("quic conn\n")

    handleCtx, handleCancel := context.WithCancel(ctx)
    defer handleCancel()

    clientStream, err := conn.AcceptStream(ctx)
    if err != nil {
        return
    }
    defer clientStream.Close()

    fmt.Printf("quic stream\n")


    // read extender header
    headerBytes := make([]byte, 1024)

    fmt.Printf("q 1\n")

    clientStream.SetReadDeadline(time.Now().Add(ReadTimeout))
    for i := 0; i < 4; {
        n, err := clientStream.Read(headerBytes[i:4])
        if err != nil {
            return
        }
        i += n
    }
    fmt.Printf("q 2\n")
    headerByteCount := int(binary.LittleEndian.Uint32(headerBytes[0:4]))
    if 1024 < headerByteCount {
        // bad data
        return
    }
    fmt.Printf("q 3\n")
    // fmt.Printf("Extender 6: %d\n", headerByteCount)
    for i := 0; i < headerByteCount; {
        clientStream.SetReadDeadline(time.Now().Add(ReadTimeout))
        n, err := clientStream.Read(headerBytes[i:headerByteCount])
        if err != nil {
            return
        }
        i += n
    }
    // fmt.Printf("Extender 7\n")
    fmt.Printf("q 4\n")

    header := &protocol.ExtenderHeader{}
    err = proto.Unmarshal(headerBytes[0:headerByteCount], header)
    if err != nil {
        return
    }

    fmt.Printf("q 5\n")

    if !self.IsAllowedSecret(header.Secret) {
        // fmt.Printf("Extender secret failed: %s\n", header.Secret)
        return
    }

    fmt.Printf("q 6\n")

    if !self.IsAllowedHost(header.DestinationHost) {
        // fmt.Printf("Extender destination failed: %s\n", header.DestinationHost)
        return
    }

    fmt.Printf("q 7: %s %d\n", header.DestinationHost, header.DestinationPort)

    var resolvedHost string 
    if header.DestinationHost == "api.bringyour.com" {
        resolvedHost = "65.19.157.41"
    } else if header.DestinationHost == "connect.bringyour.com" {
        resolvedHost = "65.49.70.71"
    } else {
        resolvedHost = header.DestinationHost
    }

    forwardConn, err := self.forwardDialer.Dial("tcp", net.JoinHostPort(
        // header.DestinationHost,
        resolvedHost,
        fmt.Sprintf("%d", header.DestinationPort),
    ))
    if err != nil {
        return
    }
    defer forwardConn.Close()

    fmt.Printf("q 8\n")


    go func() {
        // read packet from clientConn, write to forwardConn
        defer handleCancel()

        buffer := make([]byte, 4096)

        for {
            select {
            case <- handleCtx.Done():
                return
            default:
            }

            clientStream.SetReadDeadline(time.Now().Add(ReadTimeout))
            n, err := clientStream.Read(buffer)
            if err != nil {
                return
            }
            forwardConn.SetWriteDeadline(time.Now().Add(WriteTimeout))
            _, err = forwardConn.Write(buffer[0:n])
            if err != nil {
                fmt.Printf("q r end\n")
                return
            }
        }
    }()


    go func() {
        // read packet from forwardConn, write to clientConn
        defer handleCancel()

        buffer := make([]byte, 4096)

        for {
            select {
            case <- handleCtx.Done():
                return
            default:
            }

            forwardConn.SetReadDeadline(time.Now().Add(ReadTimeout))
            n, err := forwardConn.Read(buffer)
            if err != nil {
                return
            }
            clientStream.SetWriteDeadline(time.Now().Add(WriteTimeout))
            _, err = clientStream.Write(buffer[0:n])
            if err != nil {
                fmt.Printf("q w end\n")
                return
            }
        }
    }()


    select {
    case <- handleCtx.Done():
    }

    fmt.Printf("q end\n")
}


func guessOrganizationName(host string) string {

    // FIXME bringyour api for organization name
    /* For the following hostname, tell me your best guess at the organization name. Only list the full organization name and nothing else. The hostname: yandex.ru
    */
    
    // FIXME
    return host
}




/*
type readerRecordInitialBytes struct {
    conn net.Conn
    initialBytes []byte
}

func newReaderRecordInitialBytes(conn net.Conn) *readerRecordInitialBytes {
    return &readerRecordInitialBytes{
        conn: conn,
    }
}

func (self *readerRecordInitialBytes) InitialBytes() []byte {
    return slices.Clone(self.initialBytes)
}

func (self *readerRecordInitialBytes) Read(b []byte) (int, error) {
    n, err := self.conn.Read(b)
    if 0 < n {
        self.initialBytes = append(self.initialBytes, b[0:n]...)
    }
    return n, err
}




type connWithInitialBytes struct {
    conn net.Conn
    initialBytes []byte
}

func newConnWithInitialBytes(conn net.Conn, initialBytes []byte) *connWithInitialBytes {
    return &connWithInitialBytes{
        conn: conn,
        initialBytes: initialBytes,
    }
}

func (self *connWithInitialBytes) Read(b []byte) (int, error) {
    m := min(len(self.initialBytes), len(b))
    if 0 < m {
        copy(b[0:m], self.initialBytes[0:m])
        self.initialBytes = self.initialBytes[m:]
    }
    if len(b) <= m {
        return m, nil
    }
    n, err := self.conn.Read(b[m:])
    return m + n, err
}

func (self *connWithInitialBytes) Write(b []byte) (int, error) {
    return self.conn.Write(b)
}

func (self *connWithInitialBytes) Close() error {
    return self.conn.Close()
}

func (self *connWithInitialBytes) LocalAddr() net.Addr {
    return self.conn.LocalAddr()
}

func (self *connWithInitialBytes) RemoteAddr() net.Addr {
    return self.conn.RemoteAddr()
}

func (self *connWithInitialBytes) SetDeadline(t time.Time) error {
    return self.conn.SetDeadline(t)
}

func (self *connWithInitialBytes) SetReadDeadline(t time.Time) error {
    return self.conn.SetReadDeadline(t)
}

func (self *connWithInitialBytes) SetWriteDeadline(t time.Time) error {
    return self.conn.SetWriteDeadline(t)
}
*/





// https://github.com/AGWA/tlshacks/blob/main/client_hello.go
// https://pkg.go.dev/crypto/tls#ClientHelloInfo
// https://www.agwa.name/blog/post/parsing_tls_client_hello_with_cryptobyte


// client issues tls connect to for a spoof name and ip:port, and does not check the tls cert
// on top of that connection, sends a header (protocol/extender) that lists the upstream host
// and then makes a tls connection through that




// https://go.dev/src/crypto/tls/generate_cert.go


func selfSign(hosts []string, organization string) (certPemBytes []byte, keyPemBytes []byte, returnErr error) {

    var priv any
    var err error

    priv, err = rsa.GenerateKey(rand.Reader, 2048)
    // priv, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
    if err != nil {
        returnErr = err
        return
    }


    publicKey := func(priv any) any {
        switch k := priv.(type) {
        case *rsa.PrivateKey:
            return &k.PublicKey
        case *ecdsa.PrivateKey:
            return &k.PublicKey
        case ed25519.PrivateKey:
            return k.Public().(ed25519.PublicKey)
        default:
            return nil
        }
    }


    // ECDSA, ED25519 and RSA subject keys should have the DigitalSignature
    // KeyUsage bits set in the x509.Certificate template
    keyUsage := x509.KeyUsageDigitalSignature
    // Only RSA subject keys should have the KeyEncipherment KeyUsage bits set. In
    // the context of TLS this KeyUsage is particular to RSA key exchange and
    // authentication.
    if _, isRSA := priv.(*rsa.PrivateKey); isRSA {
        keyUsage |= x509.KeyUsageKeyEncipherment
    }

    notBefore := time.Now().Add(-ValidFrom)
    notAfter := notBefore.Add(ValidFor)

    serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
    serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
    if err != nil {
        log.Fatalf("Failed to generate serial number: %v", err)
    }


    template := x509.Certificate{
        SerialNumber: serialNumber,
        Subject: pkix.Name{
            Organization: []string{organization},
        },
        NotBefore: notBefore,
        NotAfter:  notAfter,

        KeyUsage:              keyUsage,
        ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
        BasicConstraintsValid: true,
    }

    for _, h := range hosts {
        if ip := net.ParseIP(h); ip != nil {
            template.IPAddresses = append(template.IPAddresses, ip)
        } else {
            template.DNSNames = append(template.DNSNames, h)
        }
    }

    // we hope the client is using tls1.3 which hides the self signed cert
    template.IsCA = true
    template.KeyUsage |= x509.KeyUsageCertSign

    derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, publicKey(priv), priv)
    if err != nil {
        returnErr = err
        return
    }
    certPemBytes = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})
    
    privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
    if err != nil {
        returnErr = err
        return
    }
    keyPemBytes = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})

    return
}






type streamConn struct {
    stream quic.Stream
}

func newStreamConn(stream quic.Stream) *streamConn {
    return &streamConn{
        stream: stream,
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
    return nil
}

func (self *streamConn) RemoteAddr() net.Addr {
    return nil
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

