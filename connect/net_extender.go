package connect

import (
	"context"
	"net"
	"net/http"
	"net/netip"

	// "os"
	// "strings"
	"fmt"
	"strconv"
	"time"
	// "slices"

	"crypto/tls"
	// "crypto/ecdsa"
	// "crypto/ed25519"
	// "crypto/elliptic"
	// "crypto/rand"
	// "crypto/rsa"
	// "crypto/x509"
	// "crypto/x509/pkix"
	"crypto/hmac"
	"crypto/sha256"
	// "encoding/pem"
	// "encoding/json"
	// "flag"
	// "log"
	// "math/big"

	// "crypto/md5"
	"encoding/binary"
	// "encoding/hex"
	// "syscall"

	// mathrand "math/rand"

	// "golang.org/x/crypto/cryptobyte"
	// "golang.org/x/net/idna"

	quic "github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"

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
	ExtenderConnectModeQuic   ExtenderConnectMode = "quic"
	// TODO
	// ExtenderConnectModeUdp ExtenderConnectMode = "udp"
)

// extenders will do a TLS connection using the given server name to the given port
// comparable
type ExtenderProfile struct {
	ConnectMode ExtenderConnectMode
	ServerName  string
	Port        int
	Fragment    bool
	Reorder     bool
}

type ExtenderConfig struct {
	Profile ExtenderProfile
	Ip      netip.Addr
	Secret  string
}

func NewExtenderHttpClient(
	connectSettings *ConnectSettings,
	extenderConfig *ExtenderConfig,
) *http.Client {
	transport := &http.Transport{
		DialTLSContext: NewExtenderDialTlsContext(connectSettings, extenderConfig),
	}
	return &http.Client{
		Transport: transport,
		Timeout:   connectSettings.RequestTimeout,
	}
}

// client

// http client can plug in DialContext as part of Transport
// https://cs.opensource.google/go/go/+/refs/tags/go1.22.4:src/net/http/transport.go;l=95

// create a tls connect to (destinationHost, destinationPort) on the connection
// returned by this
// the returned connection is not a tls connection
func NewExtenderDialTlsContext(
	connectSettings *ConnectSettings,
	extenderConfig *ExtenderConfig,
) DialTlsContextFunction {
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
			extenderConfig.Ip.String(),
			fmt.Sprintf("%d", extenderConfig.Profile.Port),
		)

		// fmt.Printf("Extender client 1\n")

		// fmt.Printf("Extender client 2\n")

		// fragmentConn(conn)
		// fragment handshake records
		// set ttl 0 on every other handshake records

		var serverConn net.Conn

		extenderTlsConfig := &tls.Config{
			ServerName:         extenderConfig.Profile.ServerName,
			InsecureSkipVerify: true,
			// require 1.3 to mask self-signed certs
			MinVersion: tls.VersionTLS13,
		}

		switch extenderConfig.Profile.ConnectMode {
		case ExtenderConnectModeTcpTls:
			netDialer := &net.Dialer{
				Timeout: connectSettings.ConnectTimeout,
			}

			conn, err := netDialer.Dial("tcp", authority)
			if err != nil {
				return nil, err
			}

			if extenderConfig.Profile.Fragment || extenderConfig.Profile.Reorder {
				rconn := NewResilientTlsConn(conn, extenderConfig.Profile.Fragment, extenderConfig.Profile.Reorder)
				tlsServerConn := tls.Client(
					rconn,
					// conn,
					extenderTlsConfig,
				)

				func() {
					tlsCtx, tlsCancel := context.WithTimeout(ctx, connectSettings.TlsTimeout)
					defer tlsCancel()
					err = tlsServerConn.HandshakeContext(tlsCtx)
				}()

				if err != nil {
					return nil, err
				}
				// once the stream is established, no longer need the resilient features
				rconn.Off()

				serverConn = tlsServerConn
			} else {
				tlsServerConn := tls.Client(
					conn,
					// conn,
					extenderTlsConfig,
				)

				func() {
					tlsCtx, tlsCancel := context.WithTimeout(ctx, connectSettings.TlsTimeout)
					defer tlsCancel()
					err = tlsServerConn.HandshakeContext(tlsCtx)
				}()
				if err != nil {
					return nil, err
				}

				serverConn = tlsServerConn
			}

			// fragmentConn.Off()

			// fmt.Printf("Extender client 3\n")
		case ExtenderConnectModeQuic:
			// quic

			// the quic connect combines connect, tls, and handshake
			quicConfig := &quic.Config{
				HandshakeIdleTimeout: connectSettings.ConnectTimeout + connectSettings.TlsTimeout + connectSettings.HandshakeTimeout,
			}
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
		// case ExtenderConnectModeUdp:

		//     conn, err := dialer.Dial("udp", authority)
		//     if err != nil {
		//         return nil, err
		//     }

		//     serverConn = newClientPacketStream(conn, UdpMtu)
		default:
			panic(fmt.Errorf("bad connect mode %s", extenderConfig.Profile.ConnectMode))
		}

		header := &protocol.ExtenderHeader{
			DestinationHost: host,
			DestinationPort: uint32(port),
			Timestamp:       uint64(time.Now().UnixMilli()),
		}
		if extenderConfig.Secret != "" {
			nonce := NewId()
			header.Nonce = nonce.Bytes()

			mac := hmac.New(sha256.New, []byte(extenderConfig.Secret))
			timestampBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(timestampBytes[0:8], header.Timestamp)
			mac.Write(timestampBytes)
			mac.Write(header.Nonce)
			header.Signature = mac.Sum(nil)
		}

		headerMessageBytes, err := proto.Marshal(header)
		if err != nil {
			return nil, err
		}

		// fmt.Printf("Extender client 4\n")

		headerBytes := make([]byte, 4+len(headerMessageBytes))
		binary.BigEndian.PutUint32(headerBytes[0:4], uint32(len(headerMessageBytes)))
		copy(headerBytes[4:4+len(headerMessageBytes)], headerMessageBytes)
		_, err = serverConn.Write(headerBytes)
		if err != nil {
			return nil, err
		}

		// fmt.Printf("Extender client 5\n")

		// return serverConn, nil

		tlsServerConn := tls.Client(
			serverConn,
			connectSettings.TlsConfig,
		)

		err = tlsServerConn.HandshakeContext(ctx)
		if err != nil {
			return nil, err
		}

		return tlsServerConn, nil
	}
}
