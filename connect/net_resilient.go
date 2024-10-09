package connect

import (
	"context"
	"net"
	// "net/http"

	// "os"
	// "strings"
	"fmt"
	"time"
	// "strconv"
	// "slices"

	"crypto/tls"
	// "crypto/ecdsa"
	// "crypto/ed25519"
	// "crypto/elliptic"
	// "crypto/rand"
	// "crypto/rsa"
	// "crypto/x509"
	// "crypto/x509/pkix"
	// "encoding/pem"
	// "encoding/json"
	// "flag"
	// "log"
	// "math/big"

	// "crypto/md5"
	"encoding/binary"
	// "encoding/hex"
	"syscall"

	mathrand "math/rand"

	"golang.org/x/crypto/cryptobyte"
	// "golang.org/x/net/idna"

	// "google.golang.org/protobuf/proto"

	"src.agwa.name/tlshacks"
)

// see https://upb-syssec.github.io/blog/2023/record-fragmentation/

// (ctx, network, address)
// type DialContextFunc func(ctx context.Context, network string, address string) (net.Conn, error)

// set this as the `DialTLSContext` or equivalent
// returns a tls connection
func NewResilientDialTlsContext(
	connectSettings *ConnectSettings,
	fragment bool,
	reorder bool,
) DialTlsContextFunction {
	return func(
		ctx context.Context,
		network string,
		address string,
	) (net.Conn, error) {
		if network != "tcp" {
			panic(fmt.Errorf("Resilient connections only support tcp network."))
		}

		host, _, err := net.SplitHostPort(address)
		if err != nil {
			panic(err)
		}

		// fmt.Printf("Extender client 1\n")

		netDialer := &net.Dialer{
			Timeout:         connectSettings.ConnectTimeout,
			KeepAlive:       connectSettings.KeepAliveTimeout,
			KeepAliveConfig: connectSettings.KeepAliveConfig,
		}
		conn, err := netDialer.Dial("tcp", address)
		if err != nil {
			return nil, err
		}

		rconn := NewResilientTlsConn(conn, fragment, reorder)
		tlsServerConn := tls.Client(rconn, &tls.Config{
			ServerName: host,
			MinVersion: tls.VersionTLS12,
		})

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

		return tlsServerConn, nil
	}
}

// adapts techniques to overcome adversarial networks
// the network uses this to the connect to the platform and extenders
// inspiraton for techniques taken from the Jigsaw project Outline SDK

type ResilientTlsConn struct {
	conn     net.Conn
	fragment bool
	reorder  bool
	buffer   []byte
	enabled  bool
}

// must be created before the tls connection starts
func NewResilientTlsConn(conn net.Conn, fragment bool, reorder bool) *ResilientTlsConn {
	return &ResilientTlsConn{
		conn:     conn,
		fragment: fragment,
		reorder:  reorder,
		buffer:   []byte{},
		enabled:  true,
	}
}

func (self *ResilientTlsConn) Off() {
	// can't turn back on after off because we don't know where to align the tls header
	self.enabled = false
}

func (self *ResilientTlsConn) Write(b []byte) (int, error) {
	if self.enabled {
		self.buffer = append(self.buffer, b...)
		for 5 <= len(self.buffer) {
			tlsHeader := parseTlsHeader(self.buffer[0:5])
			if 5+int(tlsHeader.contentLength) <= len(self.buffer) {
				if tlsHeader.contentType == 22 {
					// handshake
					handshakeBytes := self.buffer[5 : 5+tlsHeader.contentLength]
					clientHello, meta := UnmarshalClientHello(handshakeBytes)
					if clientHello != nil && clientHello.Info.ServerName != nil {
						// send the server name one character at a time
						// for each fragment, alternate the ttl of the connection to force retransmits and out-of-order arrival

						// initialSplitLen := mathrand.Intn((meta.ServerNameValueEnd+meta.ServerNameValueStart)/2-meta.ServerNameValueStart)
						split := meta.ServerNameValueStart + mathrand.Intn((meta.ServerNameValueEnd+meta.ServerNameValueStart)/2-meta.ServerNameValueStart)
						step := 1 + mathrand.Intn(meta.ServerNameValueEnd-split)
						blockSize := 64

						if tcpConn, ok := self.conn.(*net.TCPConn); ok {

							if self.fragment && self.reorder {
								tcpConn.SetNoDelay(true)

								f, _ := tcpConn.File()
								fd := int(f.Fd())

								nativeTtl, _ := syscall.GetsockoptInt(fd, syscall.IPPROTO_IP, syscall.IP_TTL)

								// fmt.Printf("native ttl=%d, server name start=%d, end=%d\n", nativeTtl, meta.ServerNameValueStart, meta.ServerNameValueEnd)

								syscall.SetsockoptInt(fd, syscall.IPPROTO_IP, syscall.IP_TTL, 0)
								if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes[0:split])); err != nil {
									return 0, err
								}
								// fmt.Printf("frag ttl=0\n")

								for i := split; i < meta.ServerNameValueEnd; i += step {
									var ttl int
									if 0 == mathrand.Intn(2) {
										ttl = 0
									} else {
										ttl = nativeTtl
									}
									syscall.SetsockoptInt(fd, syscall.IPPROTO_IP, syscall.IP_TTL, ttl)
									if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes[i:min(i+step, meta.ServerNameValueEnd)])); err != nil {
										return 0, err
									}
									// fmt.Printf("frag ttl=%d\n", ttl)
								}

								syscall.SetsockoptInt(fd, syscall.IPPROTO_IP, syscall.IP_TTL, nativeTtl)

								if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes[meta.ServerNameValueEnd:])); err != nil {
									return 0, err
								}
								// fmt.Printf("frag ttl=%d\n", nativeTtl)
							} else if self.fragment {

								if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes[0:split])); err != nil {
									return 0, err
								}

								for i := split; i < meta.ServerNameValueEnd; i += step {
									if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes[i:min(i+step, meta.ServerNameValueEnd)])); err != nil {
										return 0, err
									}
								}

								if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes[meta.ServerNameValueEnd:])); err != nil {
									return 0, err
								}

							} else if self.reorder {

								tlsBytes := tlsHeader.reconstruct(handshakeBytes)

								tcpConn.SetNoDelay(true)

								f, _ := tcpConn.File()
								fd := int(f.Fd())

								nativeTtl, _ := syscall.GetsockoptInt(fd, syscall.IPPROTO_IP, syscall.IP_TTL)

								for i := 0; i*blockSize < len(tlsBytes); i += 1 {
									var ttl int
									if 0 == i%2 {
										ttl = 0
									} else {
										ttl = nativeTtl
									}
									syscall.SetsockoptInt(fd, syscall.IPPROTO_IP, syscall.IP_TTL, ttl)
									b := tlsBytes[i*blockSize : min((i+1)*blockSize, len(tlsBytes))]
									if _, err := tcpConn.Write(b); err != nil {
										return 0, err
									}
								}

								syscall.SetsockoptInt(fd, syscall.IPPROTO_IP, syscall.IP_TTL, nativeTtl)

							} else {
								if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes)); err != nil {
									return 0, err
								}
							}

						} else {

							if self.fragment {
								if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes[0:split])); err != nil {
									return 0, err
								}

								for i := split; i < meta.ServerNameValueEnd; i += step {
									if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes[i:min(i+step, meta.ServerNameValueEnd)])); err != nil {
										return 0, err
									}
								}

								if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes[meta.ServerNameValueEnd:])); err != nil {
									return 0, err
								}
							} else {
								if _, err := tcpConn.Write(tlsHeader.reconstruct(handshakeBytes)); err != nil {
									return 0, err
								}
							}

						}

					} else {
						// flush the raw record
						_, err := self.conn.Write(self.buffer[0 : 5+tlsHeader.contentLength])
						if err != nil {
							return 0, err
						}
					}
				} else {
					// flush the raw record
					_, err := self.conn.Write(self.buffer[0 : 5+tlsHeader.contentLength])
					if err != nil {
						return 0, err
					}
				}

				self.buffer = self.buffer[5+tlsHeader.contentLength:]
			} else {
				break
			}
		}
		return len(b), nil
	} else {
		return self.conn.Write(b)
	}
}

func (self *ResilientTlsConn) Read(b []byte) (int, error) {
	return self.conn.Read(b)
}

func (self *ResilientTlsConn) Close() error {
	return self.conn.Close()
}

func (self *ResilientTlsConn) LocalAddr() net.Addr {
	return self.conn.LocalAddr()
}

func (self *ResilientTlsConn) RemoteAddr() net.Addr {
	return self.conn.RemoteAddr()
}

func (self *ResilientTlsConn) SetDeadline(t time.Time) error {
	return self.conn.SetDeadline(t)
}

func (self *ResilientTlsConn) SetReadDeadline(t time.Time) error {
	return self.conn.SetReadDeadline(t)
}

func (self *ResilientTlsConn) SetWriteDeadline(t time.Time) error {
	return self.conn.SetWriteDeadline(t)
}

type tlsHeader struct {
	contentType   byte
	tlsVersion    uint16
	contentLength uint16
}

func parseTlsHeader(b []byte) *tlsHeader {
	return &tlsHeader{
		contentType:   b[0],
		tlsVersion:    binary.BigEndian.Uint16(b[1:3]),
		contentLength: binary.BigEndian.Uint16(b[3:5]),
	}
}

func (self *tlsHeader) reconstruct(content []byte) []byte {
	b := make([]byte, 5+len(content))
	b[0] = self.contentType
	binary.BigEndian.PutUint16(b[1:3], self.tlsVersion)
	binary.BigEndian.PutUint16(b[3:5], uint16(len(content)))
	copy(b[5:5+len(content)], content)
	return b
}

// https://github.com/AGWA/tlshacks/blob/main/client_hello.go

type clientHelloMeta struct {
	ServerNameValueStart int
	ServerNameValueEnd   int
}

func UnmarshalClientHello(handshakeBytes []byte) (*tlshacks.ClientHelloInfo, *clientHelloMeta) {
	info := &tlshacks.ClientHelloInfo{Raw: handshakeBytes}
	meta := &clientHelloMeta{}
	handshakeMessage := cryptobyte.String(handshakeBytes)

	handshakeMessageLength := len(handshakeMessage)

	var messageType uint8
	if !handshakeMessage.ReadUint8(&messageType) || messageType != 1 {
		// fmt.Printf("hello 1\n")
		return nil, nil
	}

	handshakeStart := handshakeMessageLength - len(handshakeMessage)

	var clientHello cryptobyte.String
	if !handshakeMessage.ReadUint24LengthPrefixed(&clientHello) || !handshakeMessage.Empty() {
		// fmt.Printf("hello 2\n")
		return nil, nil
	}

	clientHelloLength := len(clientHello)

	if !clientHello.ReadUint16((*uint16)(&info.Version)) {
		// fmt.Printf("hello 3\n")
		return nil, nil
	}

	if !clientHello.ReadBytes(&info.Random, 32) {
		// fmt.Printf("hello 4\n")
		return nil, nil
	}

	if !clientHello.ReadUint8LengthPrefixed((*cryptobyte.String)(&info.SessionID)) {
		// fmt.Printf("hello 5\n")
		return nil, nil
	}

	var cipherSuites cryptobyte.String
	if !clientHello.ReadUint16LengthPrefixed(&cipherSuites) {
		// fmt.Printf("hello 6\n")
		return nil, nil
	}
	info.CipherSuites = []tlshacks.CipherSuite{}
	for !cipherSuites.Empty() {
		var suite uint16
		if !cipherSuites.ReadUint16(&suite) {
			// fmt.Printf("hello 7\n")
			return nil, nil
		}
		info.CipherSuites = append(info.CipherSuites, tlshacks.MakeCipherSuite(suite))
	}

	var compressionMethods cryptobyte.String
	if !clientHello.ReadUint8LengthPrefixed(&compressionMethods) {
		// fmt.Printf("hello 8\n")
		return nil, nil
	}
	info.CompressionMethods = []tlshacks.CompressionMethod{}
	for !compressionMethods.Empty() {
		var method uint8
		if !compressionMethods.ReadUint8(&method) {
			// fmt.Printf("hello 9\n")
			return nil, nil
		}
		info.CompressionMethods = append(info.CompressionMethods, tlshacks.CompressionMethod(method))
	}

	info.Extensions = []tlshacks.Extension{}

	if clientHello.Empty() {
		// fmt.Printf("hello 10\n")
		return info, meta
	}

	clientHelloStart := clientHelloLength - len(clientHello)

	var extensions cryptobyte.String
	if !clientHello.ReadUint16LengthPrefixed(&extensions) {
		// fmt.Printf("hello 11\n")
		return nil, nil
	}
	extensionsLength := len(extensions)

	extensionParsers := map[uint16]func([]byte) tlshacks.ExtensionData{
		0:  tlshacks.ParseServerNameData,
		10: tlshacks.ParseSupportedGroupsData,
		11: tlshacks.ParseECPointFormatsData,
		16: tlshacks.ParseALPNData,
		18: tlshacks.ParseEmptyExtensionData,
		22: tlshacks.ParseEmptyExtensionData,
		23: tlshacks.ParseEmptyExtensionData,
		49: tlshacks.ParseEmptyExtensionData,
	}

	for !extensions.Empty() {
		var extType uint16
		var extData cryptobyte.String

		start := extensionsLength - len(extensions)
		if !extensions.ReadUint16(&extType) || !extensions.ReadUint16LengthPrefixed(&extData) {
			// fmt.Printf("hello 12\n")
			return nil, nil
		}
		end := extensionsLength - len(extensions)

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
			meta.ServerNameValueStart = handshakeStart + clientHelloStart + start
			meta.ServerNameValueEnd = handshakeStart + clientHelloStart + end
		case 16:
			info.Info.Protocols = data.(*tlshacks.ALPNData).Protocols
		case 18:
			info.Info.SCTs = true
		}

	}

	if !clientHello.Empty() {
		// fmt.Printf("hello 13\n")
		return nil, nil
	}

	info.Info.JA3String = tlshacks.JA3String(info)
	info.Info.JA3Fingerprint = tlshacks.JA3Fingerprint(info.Info.JA3String)

	// fmt.Printf("hello 14\n")
	return info, meta
}
