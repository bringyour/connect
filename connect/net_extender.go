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






const ReadTimeout = 30 * time.Second
const WriteTimeout = 30 * time.Second

const ValidFrom = 180 * 24 * time.Hour
const ValidFor = 180 * 24 * time.Hour


type ExtenderConnectMode string
const (
    ExtenderConnectModeTcpTls ExtenderConnectMode = "tcptls"
    ExtenderConnectModeQuic ExtenderConnectMode = "quic"
    // TODO
    // ExtenderConnectModeUdp ExtenderConnectMode = "udp"
)


// extenders will do a TLS connection using the given server name to the given port
// comparable
type ExtenderProfile struct {
    ExtenderConnectMode ExtenderConnectMode
    ServerName string
    Port int
}

type ExtenderConfig struct {
    ExtenderProfile ExtenderProfile
    Ip net.IP
    Secret string
}



// client

// http client can plug in DialContext as part of Transport
// https://cs.opensource.google/go/go/+/refs/tags/go1.22.4:src/net/http/transport.go;l=95

// create a tls connect to (destinationHost, destinationPort) on the connection
// returned by this
// the returned connection is not a tls connection
func NewExtenderDialTlsContext(
    dialer *net.Dialer,
    extenderConfig *ExtenderConfig,
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

        authority := net.JoinHostPort(
            ip.String(),
            fmt.Sprintf("%d", profile.Port),
        )


        // fmt.Printf("Extender client 1\n")


        // fmt.Printf("Extender client 2\n")

        // fragmentConn(conn)
        // fragment handshake records
        // set ttl 0 on every other handshake records

        var serverConn net.Conn

        extenderTlsConfig := &tls.Config{
            ServerName: profile.ServerName,
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

            // TODO
        // } else if connectMode == ExtenderConnectModeUdp {

        //     conn, err := dialer.Dial("udp", authority)
        //     if err != nil {
        //         return nil, err
        //     }

        //     serverConn = newClientPacketStream(conn, UdpMtu)



        } else {
            panic(fmt.Errorf("bad connect mode %s", connectMode))
        }

        header := &protocol.ExtenderHeader{
            DestinationHost: host,
            DestinationPort: uint32(port),
            Timestamp: time.Now().UnixMilli(),
        }
        if secret != "" {
            nonce := NewId()
            header.Nonce = nonce
            header.Signature = PASSWORDHASH(header.Timestamp, nonce, secret)
        }

        headerMessageBytes, err := proto.Marshal()
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



