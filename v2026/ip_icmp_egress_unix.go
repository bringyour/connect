//go:build !windows && !js

package connect

import (
	"context"
	"encoding/binary"
	"net"
	"sync/atomic"
	"time"

	"golang.org/x/net/icmp"
)

// the unix echo backend: one unprivileged datagram icmp socket per flow
// (`SOCK_DGRAM` with `IPPROTO_ICMP`/`IPPROTO_ICMPV6`).
//
// identifier semantics differ per os. linux ping sockets bind the identifier
// as the local port, rewrite outgoing ids to it, and deliver only matching
// replies. darwin passes the id through unmodified but delivery to datagram
// icmp sockets is promiscuous, so the id is chosen process-locally and every
// read is filtered by it. both paths also filter by the peer address, which
// disambiguates a wrapped id across live flows.
//
// linux gates these sockets by `net.ipv4.ping_group_range` (open by default
// under systemd >= 239, and always on android); a refused socket fails the
// flow soft (see IcmpSequence.Run).

// process-wide identifier allocator for platforms where the kernel does not
// assign one. 16-bit wrap; zero is skipped so an assigned id is always
// distinguishable from an unbound socket.
var icmpEgressNextIdentifier atomic.Uint32

type icmpEgressConn struct {
	conn          *icmp.PacketConn
	ipVersion     int
	destinationIp net.IP
	identifier    uint16

	// writer-goroutine state
	lastTtl     int
	writeBuffer []byte

	// reader-goroutine state
	readBuffer []byte
}

func newIcmpEgress(ctx context.Context, ipVersion int, destinationIp net.IP, settings *IcmpBufferSettings) (icmpEgress, error) {
	var network string
	switch ipVersion {
	case 4:
		network = "udp4"
	case 6:
		network = "udp6"
	default:
		return nil, icmpEgressUnsupportedError(ipVersion)
	}
	conn, err := icmp.ListenPacket(network, "")
	if err != nil {
		return nil, err
	}

	identifier := uint16(0)
	if udpAddr, ok := conn.LocalAddr().(*net.UDPAddr); ok {
		// linux ping sockets assign the echo identifier as the local port
		identifier = uint16(udpAddr.Port)
	}
	for identifier == 0 {
		// no kernel assignment (darwin): choose our own; reads filter by it
		identifier = uint16(icmpEgressNextIdentifier.Add(1))
	}

	return &icmpEgressConn{
		conn:          conn,
		ipVersion:     ipVersion,
		destinationIp: destinationIp,
		identifier:    identifier,
		lastTtl:       -1,
		writeBuffer:   make([]byte, settings.ReadBufferByteCount),
		readBuffer:    make([]byte, settings.ReadBufferByteCount),
	}, nil
}

func (self *icmpEgressConn) WriteEcho(deadline time.Time, ttl int, sequenceNumber uint16, payload []byte) error {
	if 0 < ttl && ttl != self.lastTtl {
		// ttl passthrough, so ttl-limited probes do not dishonestly reach
		// the destination. best-effort: a platform that refuses the option
		// still sends with its default.
		switch self.ipVersion {
		case 4:
			if p := self.conn.IPv4PacketConn(); p != nil {
				if err := p.SetTTL(ttl); err == nil {
					self.lastTtl = ttl
				}
			}
		case 6:
			if p := self.conn.IPv6PacketConn(); p != nil {
				if err := p.SetHopLimit(ttl); err == nil {
					self.lastTtl = ttl
				}
			}
		}
	}

	messageByteCount := IcmpHeaderSize + len(payload)
	if len(self.writeBuffer) < messageByteCount {
		return icmpEgressUnsupportedError(self.ipVersion)
	}
	message := self.writeBuffer[:messageByteCount]
	switch self.ipVersion {
	case 4:
		message[0] = icmp4TypeEchoRequest
	case 6:
		message[0] = icmp6TypeEchoRequest
	}
	message[1] = 0
	// checksum, set below
	message[2] = 0
	message[3] = 0
	binary.BigEndian.PutUint16(message[4:6], self.identifier)
	binary.BigEndian.PutUint16(message[6:8], sequenceNumber)
	copy(message[IcmpHeaderSize:], payload)
	if self.ipVersion == 4 {
		// icmpv4 has no pseudo header. linux rewrites the id and fixes the
		// checksum; darwin sends the message as-is, so it must be correct
		// here. the icmpv6 checksum is always kernel-computed.
		binary.BigEndian.PutUint16(message[2:4], checksumFinish(checksumAdd(0, message)))
	}

	self.conn.SetWriteDeadline(deadline)
	_, err := self.conn.WriteTo(message, &net.UDPAddr{
		IP: self.destinationIp,
	})
	return err
}

func (self *icmpEgressConn) ReadEcho(deadline time.Time) (sequenceNumber uint16, payload []byte, err error) {
	for {
		self.conn.SetReadDeadline(deadline)
		n, peer, readErr := self.conn.ReadFrom(self.readBuffer)
		if readErr != nil {
			return 0, nil, readErr
		}
		message := self.readBuffer[:n]

		if self.ipVersion == 4 && IcmpHeaderSize <= len(message) && message[0]>>4 == 4 {
			// the darwin v4 read includes the ip header (an echo reply's
			// first byte is type 0, so a leading version-4 nibble is
			// unambiguous); strip it
			ipHeaderByteCount := int(message[0]&0xf) * 4
			if Ipv4HeaderSizeWithoutExtensions <= ipHeaderByteCount && ipHeaderByteCount < len(message) {
				message = message[ipHeaderByteCount:]
			}
		}

		if len(message) < IcmpHeaderSize {
			continue
		}
		switch self.ipVersion {
		case 4:
			if message[0] != icmp4TypeEchoReply {
				continue
			}
		case 6:
			if message[0] != icmp6TypeEchoReply {
				continue
			}
		}
		if binary.BigEndian.Uint16(message[4:6]) != self.identifier {
			continue
		}
		if udpAddr, ok := peer.(*net.UDPAddr); !ok || !udpAddr.IP.Equal(self.destinationIp) {
			continue
		}

		return binary.BigEndian.Uint16(message[6:8]), message[IcmpHeaderSize:], nil
	}
}

func (self *icmpEgressConn) Close() {
	self.conn.Close()
}
