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
	"io"
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
	// "encoding/binary"
	// "encoding/hex"
	// "syscall"

	mathrand "math/rand"
	"sync/atomic"
	// "golang.org/x/crypto/cryptobyte"
	// "golang.org/x/net/idna"
	// "google.golang.org/protobuf/proto"
	// "src.agwa.name/tlshacks"
	// "github.com/urnetwork/glog/v2026"
)

// see https://upb-syssec.github.io/blog/2023/record-fragmentation/

// socketTtlUnreadable is what GetSocketTtl returns when the socket option
// could not be read at all. It is deliberately 0 rather than -1: a negative
// value is a legitimate, restorable reading on IPv6 sockets (BSD kernels
// report an unset IPV6_UNICAST_HOPS as -1, meaning "use the kernel default",
// and accept -1 to put it back), while a hop count of 0 is never a value a
// socket can usefully carry -- packets would be discarded at the first hop.
const socketTtlUnreadable = 0

// socketTtlReadable reports whether GetSocketTtl returned something the
// reorder technique can restore afterwards. Testing `0 < ttl` instead is the
// bug this replaces: it read the IPv6 kernel-default sentinel as a failure and
// silently took the reorder technique off every IPv6 connection.
func socketTtlReadable(ttl int) bool {
	return ttl != socketTtlUnreadable
}

// resilientLowTtl is the TTL applied to fragments the reorder technique intends
// to have dropped in flight, so the retransmit arrives after the fragments that
// followed it. IP_TTL accepts 1-255 but only 1 is useful: a packet at TTL 1 is
// discarded at the first L3 hop, and anything higher survives that hop, arrives
// first try, and reorders nothing. There is no reorder effect for an on-link
// peer either — a bridged L2 path, or the loopback the tests run over, delivers
// TTL 1 intact — so the technique only bites once a router is in the path.
//
// The value was 0, which no host may emit. Linux rejects that sockopt with
// EINVAL, so the technique was inert there. Windows accepts the sockopt, but
// RFC 1122 3.2.1.7 forbids emitting TTL 0, so it is likely dropped locally
// rather than by a router; the loss is the same either way, but that mechanism
// was never observed, only the sockopt's acceptance.
const resilientLowTtl = 1

// set this as the `DialTLSContext` or equivalent
// returns a tls connection
func NewResilientDialTlsContext(
	connectSettings *ConnectSettings,
	fragment bool,
	reorder bool,
) DialTlsContextFunction {
	return newResilientDialTlsContext(connectSettings, fragment, reorder, nil)
}

func newResilientDialTlsContext(
	connectSettings *ConnectSettings,
	fragment bool,
	reorder bool,
	nextProtos []string,
) DialTlsContextFunction {
	baseTlsConfig := newClientTlsConfig(connectSettings.TlsConfig, nextProtos)
	return func(
		ctx context.Context,
		network string,
		addr string,
	) (net.Conn, error) {
		switch network {
		case "tcp", "tcp4", "tcp6":
		default:
			panic(fmt.Errorf("Resilient connections only support tcp network."))
		}

		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			panic(err)
		}

		// the handshake half of the dial, split out so the address-family
		// fallback can own the connection between the connect and the
		// handshake -- it has to read the family off it before a failed
		// handshake takes it away.
		handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
			rconn := NewResilientTlsConn(conn, fragment, reorder)

			// copy and extend
			tlsConfig := baseTlsConfig.Clone()
			tlsConfig.ServerName = host
			tlsConn := tls.Client(rconn, tlsConfig)

			var err error
			func() {
				tlsCtx, tlsCancel := context.WithTimeout(ctx, connectSettings.TlsTimeout)
				defer tlsCancel()
				err = tlsConn.HandshakeContext(tlsCtx)
			}()
			if err != nil {
				tlsConn.Close()
				return nil, err
			}
			// once the stream is established, no longer need the resilient features
			if err := offResilientTlsConn(ctx, rconn, connectSettings.ConnectTimeout); err != nil {
				tlsConn.Close()
				return nil, err
			}

			return tlsConn, nil
		}

		// The resilient dialers need the family fallback as much as the normal
		// one does, and wiring it into newNormalDialTlsContext alone is the
		// wrong half to have. Api posts do not race the dialers: they go
		// through HttpSerial -> serialEval, which sorts the dialers it has
		// already seen succeed by priority, and "fragment" is priority 0 while
		// "normal" is 25 -- so once a launch is warm, the fragment dialer is
		// the FIRST one every serial post tries. Before that it is one of the
		// dialers the parallel hello runs. Either way a post over a
		// blackholed ipv6 path stalled here, in a dialer with no timeout
		// classification and no strike, until serialEval's whole budget was
		// gone -- the exact stall this feature exists to remove, and with
		// nothing recorded to show for it.
		return dialControlTlsWithFamilyFallback(
			ctx, connectSettings, "tcp", addr, connectSettings.DialContext, handshake)
	}
}

// offResilientTlsConn bounds the partial-record drain that Off may perform.
// The completed TLS handshake's context cannot interrupt that raw Write.
func offResilientTlsConn(
	ctx context.Context,
	rconn *ResilientTlsConn,
	phaseTimeout time.Duration,
) error {
	return withConnWritePhaseDeadline(ctx, rconn, phaseTimeout, rconn.Off)
}

// adapts techniques to overcome adversarial networks
// the network uses this to the connect to the platform and extenders
// inspiraton for techniques taken from the Jigsaw project Outline SDK

type ResilientTlsConn struct {
	conn     net.Conn
	fragment bool
	reorder  bool
	buffer   []byte

	// setTtl replaces the SetSocketTtl syscall; nil means call it directly.
	// This is a test seam so the reorder paths can be observed and made to
	// fail without a router in the way. It is a field rather than a package
	// var so that installing a seam is scoped to one connection.
	setTtl func(SocketHandle, int) error
	// ttlErr holds the first TTL failure seen on this connection, fatal ones
	// included, and is the hook a future metric would read.
	ttlErr error

	enabled atomic.Bool
}

// must be created before the tls connection starts
func NewResilientTlsConn(conn net.Conn, fragment bool, reorder bool) *ResilientTlsConn {
	resilientTlsConn := &ResilientTlsConn{
		conn:     conn,
		fragment: fragment,
		reorder:  reorder,
		buffer:   []byte{},
	}
	resilientTlsConn.enabled.Store(true)
	return resilientTlsConn
}

// Off permanently disables the resilient fragment/reorder layer. It drains
// any partially-buffered record first — an earlier Write already returned
// len(b), nil for those bytes, so stranding them would silently lose data
// the caller believes was sent. A partial or failed drain leaves the wire
// state indeterminate: the connection is failed closed (closed, so the
// caller must not hand it back as established) and the drain error is
// returned — io.ErrShortWrite when the drain came up short with a nil
// error. Returns nil on a successful drain or when the buffer is empty.
// Off is not safe for concurrent use with Write.
func (self *ResilientTlsConn) Off() error {
	if 0 < len(self.buffer) {
		n, err := self.conn.Write(self.buffer)
		if err != nil || n < len(self.buffer) {
			if err == nil {
				err = io.ErrShortWrite
			}
			self.failConnection()
			return err
		}
		self.buffer = nil
	}
	// can't turn back on after off because we don't know where to align the tls header
	self.enabled.Store(false)
	return nil
}

func (self *ResilientTlsConn) Enabled() bool {
	return self.enabled.Load()
}

// failConnection marks the connection unusable after an indeterminate write
// (a partial or failed record send: the peer has part of the bytes and the
// wire state is unknowable). The buffered record is dropped so it is never
// re-sent, the resilient layer is disabled, and the underlying connection is
// closed so later writes fail instead of appending to a corrupt stream.
func (self *ResilientTlsConn) failConnection() {
	self.buffer = nil
	self.enabled.Store(false)
	self.conn.Close()
}

// applyTtl sets the socket TTL, routing through the test seam when one is
// installed so tests can observe and fail the TTL sequence without needing a
// router to actually drop packets. It is the single choke point for every TTL
// write in the resilient paths; the policy for what to do with the error lives
// in applyTtlBestEffort and restoreNativeTtl.
func (self *ResilientTlsConn) applyTtl(fd SocketHandle, ttl int) error {
	if self.setTtl != nil {
		return self.setTtl(fd, ttl)
	}
	return SetSocketTtl(fd, ttl)
}

// applyTtlBestEffort applies ttl and keeps going if the syscall refuses it,
// recording only the first failure. This is the non-fatal half of a
// deliberately asymmetric policy: a TTL that will not apply costs the reorder
// property for that fragment, but the fragment still goes out whole at the
// native TTL and the record on the wire stays coherent. Failing the dial here
// would trade a working connection for no connection on any socket that
// refuses the family's hop-count option. Rejection is often value-dependent
// rather than blanket (Linux refuses 0 and accepts 64), so a later write may
// well succeed; the first error is kept because it is the most diagnostic, not
// because it predicts the rest.
func (self *ResilientTlsConn) applyTtlBestEffort(fd SocketHandle, ttl int) {
	if err := self.applyTtl(fd, ttl); err != nil && self.ttlErr == nil {
		self.ttlErr = err
	}
}

// restoreNativeTtl puts the socket back to its native TTL and fails the
// connection closed if it cannot. This is the fatal half of the policy: unlike
// the low-TTL writes, a failure here is not confined to one fragment — every
// later packet on the socket, the rest of the handshake included, would leave
// at resilientLowTtl and be discarded at the first L3 hop. Note this closes the
// connection even where nothing on the wire is indeterminate: the reorder-only
// path has written every block by the time it restores, so the record the peer
// holds is complete and coherent, and the connection dies only because the
// socket's future TTL is wrong. A dial failure still beats a handshake that
// hangs. The error is recorded before closing so a metric reading ttlErr does
// not miss precisely the failures that killed connections.
func (self *ResilientTlsConn) restoreNativeTtl(fd SocketHandle, nativeTtl int) error {
	if err := self.applyTtl(fd, nativeTtl); err != nil {
		if self.ttlErr == nil {
			self.ttlErr = err
		}
		self.failConnection()
		return err
	}
	return nil
}

// writeRecord writes record whole to w. On any short or failed write the
// connection is failed closed: a partial record on the wire cannot be
// coherently retried — the buffer still holds the full record, so a retry
// would re-send the bytes already on the wire — and the layer is disabled
// and the connection closed so later writes fail instead of appending to
// the corrupt stream. A short write with a nil error is converted to
// io.ErrShortWrite so the caller never mistakes a closed connection for
// success. The buffer is not advanced here; callers advance it past the
// record only after this returns nil.
func (self *ResilientTlsConn) writeRecord(w io.Writer, record []byte) error {
	n, err := w.Write(record)
	if err == nil && n == len(record) {
		return nil
	}
	if err == nil {
		err = io.ErrShortWrite
	}
	self.failConnection()
	return err
}

func (self *ResilientTlsConn) Write(b []byte) (int, error) {
	if self.Enabled() {
		self.buffer = append(self.buffer, b...)
		for 5 <= len(self.buffer) {
			tlsHeader := parseTlsHeader(self.buffer[0:5])
			if 5+int(tlsHeader.contentLength) <= len(self.buffer) {
				if tlsHeader.contentType == TlsContentTypeHandshake {
					// handshake
					handshakeBytes := self.buffer[5 : 5+tlsHeader.contentLength]
					clientHello, meta := UnmarshalClientHello(handshakeBytes)
					if clientHello != nil && clientHello.Info.ServerName != nil {
						// send the server name one character at a time
						// for each fragment, alternate the ttl of the connection to force retransmits and out-of-order arrival

						// initialSplitLen := mathrand.Intn((meta.ServerNameValueEnd+meta.ServerNameValueStart)/2-meta.ServerNameValueStart)
						// guard mathrand.Intn against zero/negative bounds (very short ServerName values panic Intn)
						splitRangeMid := (meta.ServerNameValueEnd + meta.ServerNameValueStart) / 2
						splitRange := splitRangeMid - meta.ServerNameValueStart
						stepRange := meta.ServerNameValueEnd - (meta.ServerNameValueStart + splitRange)

						if splitRange <= 0 || stepRange <= 0 {
							// the server name is too short to fragment;
							// fall back to a single write
							record := tlsHeader.reconstruct(handshakeBytes)
							if err := self.writeRecord(self.conn, record); err != nil {
								return 0, err
							}
							self.buffer = self.buffer[5+tlsHeader.contentLength:]
							continue
						}
						split := meta.ServerNameValueStart + mathrand.Intn(splitRange)
						step := 1 + mathrand.Intn(stepRange)
						blockSize := 64

						if tcpConn, ok := self.conn.(*net.TCPConn); ok {

							if self.fragment && self.reorder {
								tcpConn.SetNoDelay(true)

								fd, closeFd, err := duplicateSocketHandle(tcpConn)
								if err != nil {
									return 0, err
								}
								defer closeFd()

								nativeTtl := GetSocketTtl(fd)
								if !socketTtlReadable(nativeTtl) {
									// the syscall failed, so there is nothing to
									// restore afterwards (see socketTtlReadable)
									record := tlsHeader.reconstruct(handshakeBytes)
									if err := self.writeRecord(tcpConn, record); err != nil {
										return 0, err
									}
									self.buffer = self.buffer[5+tlsHeader.contentLength:]
									continue
								}
								// restore the TTL on every exit after this point,
								// including fragment-write failures.
								// Best effort: a defer has nobody to report to
								defer func() { _ = self.applyTtl(fd, nativeTtl) }()

								// fmt.Printf("native ttl=%d, server name start=%d, end=%d\n", nativeTtl, meta.ServerNameValueStart, meta.ServerNameValueEnd)

								self.applyTtlBestEffort(fd, resilientLowTtl)
								record := tlsHeader.reconstruct(handshakeBytes[0:split])
								if err := self.writeRecord(tcpConn, record); err != nil {
									return 0, err
								}
								// fmt.Printf("frag ttl=%d\n", resilientLowTtl)

								for i := split; i < meta.ServerNameValueEnd; i += step {
									var ttl int
									if 0 == mathrand.Intn(2) {
										ttl = resilientLowTtl
									} else {
										ttl = nativeTtl
									}
									// not the final restore, so best effort
									self.applyTtlBestEffort(fd, ttl)
									record := tlsHeader.reconstruct(handshakeBytes[i:min(i+step, meta.ServerNameValueEnd)])
									if err := self.writeRecord(tcpConn, record); err != nil {
										return 0, err
									}
									// fmt.Printf("frag ttl=%d\n", ttl)
								}

								// checked: the tail and the rest of the handshake
								// still have to leave this socket
								if err := self.restoreNativeTtl(fd, nativeTtl); err != nil {
									return 0, err
								}

								tailRecord := tlsHeader.reconstruct(handshakeBytes[meta.ServerNameValueEnd:])
								if err := self.writeRecord(tcpConn, tailRecord); err != nil {
									return 0, err
								}
								// fmt.Printf("frag ttl=%d\n", nativeTtl)
							} else if self.fragment {

								record := tlsHeader.reconstruct(handshakeBytes[0:split])
								if err := self.writeRecord(tcpConn, record); err != nil {
									return 0, err
								}

								for i := split; i < meta.ServerNameValueEnd; i += step {
									record := tlsHeader.reconstruct(handshakeBytes[i:min(i+step, meta.ServerNameValueEnd)])
									if err := self.writeRecord(tcpConn, record); err != nil {
										return 0, err
									}
								}

								record = tlsHeader.reconstruct(handshakeBytes[meta.ServerNameValueEnd:])
								if err := self.writeRecord(tcpConn, record); err != nil {
									return 0, err
								}

							} else if self.reorder {

								tlsBytes := tlsHeader.reconstruct(handshakeBytes)

								tcpConn.SetNoDelay(true)

								fd, closeFd, err := duplicateSocketHandle(tcpConn)
								if err != nil {
									return 0, err
								}
								defer closeFd()

								nativeTtl := GetSocketTtl(fd)
								if !socketTtlReadable(nativeTtl) {
									// syscall failed; fall back to a single write
									if err := self.writeRecord(tcpConn, tlsBytes); err != nil {
										return 0, err
									}
									self.buffer = self.buffer[5+tlsHeader.contentLength:]
									continue
								}
								// restore the TTL on every exit after this point,
								// including block-write failures.
								// Best effort: a defer has nobody to report to
								defer func() { _ = self.applyTtl(fd, nativeTtl) }()

								for i := 0; i*blockSize < len(tlsBytes); i += 1 {
									var ttl int
									if 0 == i%2 {
										ttl = resilientLowTtl
									} else {
										ttl = nativeTtl
									}
									// not the final restore, so best effort
									self.applyTtlBestEffort(fd, ttl)
									b := tlsBytes[i*blockSize : min((i+1)*blockSize, len(tlsBytes))]
									if err := self.writeRecord(tcpConn, b); err != nil {
										return 0, err
									}
								}

								// checked: the rest of the handshake still has to
								// leave this socket
								if err := self.restoreNativeTtl(fd, nativeTtl); err != nil {
									return 0, err
								}

							} else {
								record := tlsHeader.reconstruct(handshakeBytes)
								if err := self.writeRecord(tcpConn, record); err != nil {
									return 0, err
								}
							}

						} else {

							if self.fragment {
								record := tlsHeader.reconstruct(handshakeBytes[0:split])
								if err := self.writeRecord(self.conn, record); err != nil {
									return 0, err
								}

								for i := split; i < meta.ServerNameValueEnd; i += step {
									record := tlsHeader.reconstruct(handshakeBytes[i:min(i+step, meta.ServerNameValueEnd)])
									if err := self.writeRecord(self.conn, record); err != nil {
										return 0, err
									}
								}

								record = tlsHeader.reconstruct(handshakeBytes[meta.ServerNameValueEnd:])
								if err := self.writeRecord(self.conn, record); err != nil {
									return 0, err
								}
							} else {
								record := tlsHeader.reconstruct(handshakeBytes)
								if err := self.writeRecord(self.conn, record); err != nil {
									return 0, err
								}
							}

						}

					} else {
						// flush the raw record; a short or failed write leaves a
						// partial record on the wire, so writeRecord fails closed
						if err := self.writeRecord(self.conn, self.buffer[0:5+tlsHeader.contentLength]); err != nil {
							return 0, err
						}
					}
				} else {
					// flush the raw record; a short or failed write leaves a
					// partial record on the wire, so writeRecord fails closed
					if err := self.writeRecord(self.conn, self.buffer[0:5+tlsHeader.contentLength]); err != nil {
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
