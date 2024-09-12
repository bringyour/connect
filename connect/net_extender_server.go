package connect

import (
	"context"
	"net"
	// "net/http"

	// "os"
	"fmt"
	"strings"
	"time"
	// "strconv"
	"slices"

	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/tls"
	// "crypto/elliptic"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
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

	// mathrand "math/rand"

	// "golang.org/x/crypto/cryptobyte"
	"golang.org/x/net/idna"

	quic "github.com/quic-go/quic-go"
	"google.golang.org/protobuf/proto"

	// "src.agwa.name/tlshacks"

	"bringyour.com/protocol"
)

// server listens for a tls connect and replies with a self-signed cert
// server set up to forward to only subdomains of a root domain
// otherwise close connection

// if the header is not detected, proxy the request
// in this way the server looks like any CDN with misconfigured certs

// note anyone can host an extender server on their IP
// the IP can be manually entered into the app
// the default is to not require signatures, to allow all users
// signatures can be used to make the traffic private

// https://go.dev/src/crypto/tls/generate_cert.go

type ExtenderServer struct {
	ctx    context.Context
	cancel context.CancelFunc

	requireSignature bool
	allowedSecrets   []string
	// exact (x) or wildcard (*.x)
	// wildcard *.x does not match exact x
	allowedHosts  []string
	ports         map[int][]ExtenderConnectMode
	forwardDialer *net.Dialer
}

func NewExtenderServer(ctx context.Context, allowedSecrets []string, allowedHosts []string, ports map[int][]ExtenderConnectMode, forwardDialer *net.Dialer) *ExtenderServer {

	cancelCtx, cancel := context.WithCancel(ctx)

	return &ExtenderServer{
		ctx:            cancelCtx,
		cancel:         cancel,
		allowedSecrets: allowedSecrets,
		allowedHosts:   allowedHosts,
		ports:          ports,
		forwardDialer:  forwardDialer,
	}

}

func (self *ExtenderServer) ListenAndServe() error {

	listeners := map[int]net.Listener{}
	quicListeners := map[int]*quic.Listener{}

	for port, connectModes := range self.ports {
		if !slices.Contains(connectModes, ExtenderConnectModeTcpTls) {
			continue
		}

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
				case <-self.ctx.Done():
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

	for port, connectModes := range self.ports {
		if !slices.Contains(connectModes, ExtenderConnectModeQuic) {
			continue
		}

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
				case <-self.ctx.Done():
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

	// TODO
	/*
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
	*/

	select {
	case <-self.ctx.Done():
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

func (self *ExtenderServer) IsAllowedSecret(header *protocol.ExtenderHeader) bool {
	for _, secret := range self.allowedSecrets {
		mac := hmac.New(sha256.New, []byte(secret))
		timestampBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(timestampBytes[0:8], header.Timestamp)
		mac.Write(timestampBytes)
		mac.Write(header.Nonce)
		signature := mac.Sum(nil)
		if slices.Equal(signature, header.Signature) {
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

	// FIXME switch to normal proxy if there are no tls fragments

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
	headerByteCount := int(binary.BigEndian.Uint32(headerBytes[0:4]))
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

	if !self.IsAllowedSecret(header) {
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
			case <-handleCtx.Done():
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
			case <-handleCtx.Done():
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
	case <-handleCtx.Done():
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
	headerByteCount := int(binary.BigEndian.Uint32(headerBytes[0:4]))
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

	if !self.IsAllowedSecret(header) {
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
			case <-handleCtx.Done():
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
			case <-handleCtx.Done():
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
	case <-handleCtx.Done():
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
    	// FIXME need to make a copy
        self.initialBytes = append(self.initialBytes, COPY(b[0:n])...)
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
