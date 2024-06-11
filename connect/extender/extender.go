package extender


import (
    "context"
    "net"
    "net/http"

    // "os"
    "strings"
    "time"
    "fmt"
    // "strconv"
    "slices"


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
    

    

    "golang.org/x/crypto/cryptobyte"
    "golang.org/x/net/idna"

    "google.golang.org/protobuf/proto"

    "src.agwa.name/tlshacks"

    "bringyour.com/protocol"
)



// (ctx, network, address)
type DialContextFunc func(ctx context.Context, network string, address string) (net.Conn, error)




// TODO function to search ports and organization/host names for connectivity
// uses api.bringyour.com/hello with the extender client
// listen on secure mail, 443, and other ports




// in toolbar of app, including login screen, be able to set extender list


const ReadTimeout = 30 * time.Second
const WriteTimeout = 30 * time.Second

const ValidFrom = 180 * 24 * time.Hour
const ValidFor = 180 * 24 * time.Hour



type ExtenderConfig struct {
    SpoofHost string
    ExtenderIp net.IP
    ExtenderPort int
    DestinationHost string
    DestinationPort int
}



func NewExtenderHttpClient(
    config *ExtenderConfig,
    tlsConfig *tls.Config,
) *http.Client {
    dialer := &net.Dialer{
        Timeout:   30 * time.Second,
        KeepAlive: 30 * time.Second,
    }
    transport := &http.Transport{
        DialContext: NewExtenderDialContext(dialer, config),
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



// client

// http client can plug in DialContext as part of Transport
// https://cs.opensource.google/go/go/+/refs/tags/go1.22.4:src/net/http/transport.go;l=95

// create a tls connect to (destinationHost, destinationPort) on the connection
// returned by this
func NewExtenderDialContext(
    dialer *net.Dialer,
    config *ExtenderConfig,
) DialContextFunc {
    return func(
        ctx context.Context,
        network string,
        address string,
    ) (net.Conn, error) {

        authority := net.JoinHostPort(
            config.ExtenderIp.String(),
            fmt.Sprintf("%d", config.ExtenderPort),
        )

        fmt.Printf("Extender client 1\n")

        conn, err := dialer.Dial("tcp", authority)
        if err != nil {
            return nil, err
        }

        fmt.Printf("Extender client 2\n")

        serverConn := tls.Client(conn, &tls.Config{
            ServerName: config.SpoofHost,
            InsecureSkipVerify: true,
        })

        err = serverConn.HandshakeContext(ctx)
        if err != nil {
            return nil, err
        }

        fmt.Printf("Extender client 3\n")


        headerMessageBytes, err := proto.Marshal(&protocol.ExtenderHeader{
            DestinationHost: config.DestinationHost,
            DestinationPort: uint32(config.DestinationPort),
        })
        if err != nil {
            return nil, err
        }

        fmt.Printf("Extender client 4\n")

        headerBytes := make([]byte, 4 + len(headerMessageBytes))
        binary.LittleEndian.PutUint32(headerBytes[0:4], uint32(len(headerMessageBytes)))
        copy(headerBytes[4:4+len(headerMessageBytes)], headerMessageBytes)
        _, err = serverConn.Write(headerBytes)
        if err != nil {
            return nil, err
        }

        fmt.Printf("Extender client 5\n")

        return serverConn, nil
    }
}









// server listens for a tls connect and replies with a self-signed cert
// server set up to forward to only subdomains of a root domain
// otherwise close connection

// https://go.dev/src/crypto/tls/generate_cert.go


type ExtenderServer struct {
    ctx context.Context
    cancel context.CancelFunc

    // exact (x) or wildcard (*.x)
    // wildcard *.x does not match exact x
    allowedHosts []string
    ports []int
}

func NewExtenderServer(ctx context.Context, allowedHosts []string, ports []int) *ExtenderServer {


    cancelCtx, cancel := context.WithCancel(ctx)

    return &ExtenderServer{
        ctx: cancelCtx,
        cancel: cancel,
        allowedHosts: allowedHosts,
        ports: ports,
    }


}

func (self *ExtenderServer) ListenAndServe() error {

    listeners := map[int]net.Listener{}

    for _, port := range self.ports {
        listener, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
        if err != nil {
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
                    return
                }
                fmt.Printf("Extender pre\n")
                go self.HandleExtenderConnection(self.ctx, conn)
            }
        }()
    }


    select {
    case <- self.ctx.Done():
    }
    for _, listener := range listeners {
        listener.Close()
    }

    return nil
}

func (self *ExtenderServer) Close() {
    self.cancel()
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

    fmt.Printf("Extender 1\n")




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

    fmt.Printf("Extender 5\n")

    err := clientConn.HandshakeContext(handleCtx)
    if err != nil {
        panic(err)
    }

    fmt.Printf("Extender 6\n")


	// read extender header
    headerBytes := make([]byte, 1024)

    clientConn.SetReadDeadline(time.Now().Add(ReadTimeout))
    for i := 0; i < 4; {
        n, err := clientConn.Read(headerBytes[i:4])
        if err != nil {
            return
        }
        i += n
    }
    headerByteCount := int(binary.LittleEndian.Uint32(headerBytes[0:4]))
    fmt.Printf("Extender 6: %d\n", headerByteCount)
    for i := 0; i < headerByteCount; {
        clientConn.SetReadDeadline(time.Now().Add(ReadTimeout))
        n, err := clientConn.Read(headerBytes[i:headerByteCount])
        if err != nil {
            return
        }
        i += n
    }
    fmt.Printf("Extender 7\n")

    header := &protocol.ExtenderHeader{}
    err = proto.Unmarshal(headerBytes[0:headerByteCount], header)
    if err != nil {
        return
    }


    // if header error, return http misconfigured server error with hostname

    if !self.IsAllowedHost(header.DestinationHost) {
        // return http misconfigured server error with hostname
        return
    }

    forwardConn, err := net.Dial("tcp", net.JoinHostPort(
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


func guessOrganizationName(host string) string {

    // FIXME bringyour api for organization name
    /* For the following hostname, tell me your best guess at the organization name. Only list the full organization name and nothing else. The hostname: yandex.ru
    */
    
    // FIXME
    return host
}





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






// https://github.com/AGWA/tlshacks/blob/main/client_hello.go
// https://pkg.go.dev/crypto/tls#ClientHelloInfo
// https://www.agwa.name/blog/post/parsing_tls_client_hello_with_cryptobyte


// client issues tls connect to for a spoof name and ip:port, and does not check the tls cert
// on top of that connection, sends a header (protocol/extender) that lists the upstream host
// and then makes a tls connection through that




// https://github.com/AGWA/tlshacks/blob/main/client_hello.go


func UnmarshalClientHello(handshakeBytes []byte) *tlshacks.ClientHelloInfo {
    info := &tlshacks.ClientHelloInfo{Raw: handshakeBytes}
    handshakeMessage := cryptobyte.String(handshakeBytes)

    var messageType uint8
    if !handshakeMessage.ReadUint8(&messageType) || messageType != 1 {
        fmt.Printf("hello 1\n")
        return nil
    }

    var clientHello cryptobyte.String
    if !handshakeMessage.ReadUint24LengthPrefixed(&clientHello) || !handshakeMessage.Empty() {
        fmt.Printf("hello 2\n")
        return nil
    }

    if !clientHello.ReadUint16((*uint16)(&info.Version)) {
        fmt.Printf("hello 3\n")
        return nil
    }

    if !clientHello.ReadBytes(&info.Random, 32) {
        fmt.Printf("hello 4\n")
        return nil
    }

    if !clientHello.ReadUint8LengthPrefixed((*cryptobyte.String)(&info.SessionID)) {
        fmt.Printf("hello 5\n")
        return nil
    }

    var cipherSuites cryptobyte.String
    if !clientHello.ReadUint16LengthPrefixed(&cipherSuites) {
        fmt.Printf("hello 6\n")
        return nil
    }
    info.CipherSuites = []tlshacks.CipherSuite{}
    for !cipherSuites.Empty() {
        var suite uint16
        if !cipherSuites.ReadUint16(&suite) {
            fmt.Printf("hello 7\n")
            return nil
        }
        info.CipherSuites = append(info.CipherSuites, tlshacks.MakeCipherSuite(suite))
    }

    var compressionMethods cryptobyte.String
    if !clientHello.ReadUint8LengthPrefixed(&compressionMethods) {
        fmt.Printf("hello 8\n")
        return nil
    }
    info.CompressionMethods = []tlshacks.CompressionMethod{}
    for !compressionMethods.Empty() {
        var method uint8
        if !compressionMethods.ReadUint8(&method) {
            fmt.Printf("hello 9\n")
            return nil
        }
        info.CompressionMethods = append(info.CompressionMethods, tlshacks.CompressionMethod(method))
    }

    info.Extensions = []tlshacks.Extension{}

    if clientHello.Empty() {
        fmt.Printf("hello 10\n")
        return info
    }
    var extensions cryptobyte.String
    if !clientHello.ReadUint16LengthPrefixed(&extensions) {
        fmt.Printf("hello 11\n")
        return nil
    }
    for !extensions.Empty() {
        var extType uint16
        var extData cryptobyte.String
        if !extensions.ReadUint16(&extType) || !extensions.ReadUint16LengthPrefixed(&extData) {
            fmt.Printf("hello 12\n")
            return nil
        }

        parseData := extensionParsers[extType]
        if parseData == nil {
            parseData = tlshacks.ParseUnknownExtensionData
        }
        data := parseData(extData)

        info.Extensions = append(info.Extensions, tlshacks.Extension{
            Type:    extType,
            Name:    tlshacks.Extensions[extType].Name,
            Grease:  tlshacks.Extensions[extType].Grease,
            Private: tlshacks.Extensions[extType].Private,
            Data:    data,
        })

        switch extType {
        case 0:
            info.Info.ServerName = &data.(*tlshacks.ServerNameData).HostName
        case 16:
            info.Info.Protocols = data.(*tlshacks.ALPNData).Protocols
        case 18:
            info.Info.SCTs = true
        }

    }

    if !clientHello.Empty() {
        fmt.Printf("hello 13\n")
        return nil
    }

    info.Info.JA3String = tlshacks.JA3String(info)
    info.Info.JA3Fingerprint = tlshacks.JA3Fingerprint(info.Info.JA3String)

    fmt.Printf("hello 14\n")
    return info
}

var extensionParsers = map[uint16]func([]byte) tlshacks.ExtensionData{
    0:  tlshacks.ParseServerNameData,
    10: tlshacks.ParseSupportedGroupsData,
    11: tlshacks.ParseECPointFormatsData,
    16: tlshacks.ParseALPNData,
    18: tlshacks.ParseEmptyExtensionData,
    22: tlshacks.ParseEmptyExtensionData,
    23: tlshacks.ParseEmptyExtensionData,
    49: tlshacks.ParseEmptyExtensionData,
}




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



