package connect

// Orphan RST (PROXYDRAIN1.md §3.5): a non-SYN tcp packet that matches no
// sequence gets a RST back to the source, so an endpoint whose flow state
// was lost here reconnects immediately instead of retransmitting into
// silence. Never resets a reset, and generation is rate limited.

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

type capturedReceive struct {
	source TransferPath
	ipPath *IpPath
	packet []byte
}

func orphanTestBuffer(ctx context.Context, tcpBufferSettings *TcpBufferSettings) (*Tcp4Buffer, chan *capturedReceive) {
	receives := make(chan *capturedReceive, 16)
	buffer := NewTcp4Buffer(ctx, func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		// the callback borrows the packet; copy for the assertion
		packetCopy := make([]byte, len(packet))
		copy(packetCopy, packet)
		select {
		case receives <- &capturedReceive{source: source, ipPath: ipPath, packet: packetCopy}:
		default:
		}
	}, tcpBufferSettings)
	return buffer, receives
}

func sendOrphan(t *testing.T, buffer *Tcp4Buffer, source TransferPath, sourcePort uint16, mutate func(*parsedTcp)) {
	t.Helper()
	packet := MessagePoolGet(32)
	parsed := &parsedTcp{
		sourceIp:        net.IPv4(10, 0, 0, 1).To4(),
		destinationIp:   net.IPv4(203, 0, 113, 7).To4(),
		sourcePort:      sourcePort,
		destinationPort: 443,
		ack:             true,
		psh:             true,
		seq:             5000,
		ackNumber:       9000,
		windowSize:      65535,
		payload:         []byte("mid-stream"),
	}
	if mutate != nil {
		mutate(parsed)
	}
	// an orphan send returns success=false (dropped)
	if success, err := buffer.send(source, protocol.ProvideMode_Network, parsed, -1, packet); err != nil {
		MessagePoolReturn(packet)
		t.Fatalf("orphan send: %v", err)
	} else if success {
		t.Fatalf("orphan send must report dropped")
	}
}

func TestTcpOrphanRst(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tcpBufferSettings := DefaultTcpBufferSettingsWithBufferSize(8)
	tcpBufferSettings.EnableOrphanRst = true
	tcpBufferSettings.OrphanRstPerSecond = 0 // unlimited for this test

	buffer, receives := orphanTestBuffer(ctx, tcpBufferSettings)

	source := SourceId(NewId())
	sendOrphan(t, buffer, source, 40001, nil)

	select {
	case received := <-receives:
		// the reply is addressed back to the segment's source
		AssertEqual(t, source, received.source)
		ipProtocol, sourceIp, destinationIp, transport, ok := parseIpv4(received.packet)
		if !ok {
			t.Fatalf("rst did not parse as ipv4")
		}
		AssertEqual(t, ipProtocolNumberTcp, ipProtocol)
		AssertEqual(t, true, sourceIp.Equal(net.IPv4(203, 0, 113, 7).To4()))
		AssertEqual(t, true, destinationIp.Equal(net.IPv4(10, 0, 0, 1).To4()))
		rst := &parsedTcp{}
		if !parseTcpPacket(sourceIp, destinationIp, transport, rst) {
			t.Fatalf("rst did not parse as tcp")
		}
		AssertEqual(t, true, rst.rst)
		AssertEqual(t, uint16(443), rst.sourcePort)
		AssertEqual(t, uint16(40001), rst.destinationPort)
		// the orphan had ACK set: the reset takes its seq from the ack field
		AssertEqual(t, uint32(9000), rst.seq)
		AssertEqual(t, false, rst.ack)
	case <-time.After(2 * time.Second):
		t.Fatalf("no rst emitted for the orphan packet")
	}

	// an orphan WITHOUT ack: seq 0, ack = seq + payload, RST|ACK
	sendOrphan(t, buffer, source, 40002, func(parsed *parsedTcp) {
		parsed.ack = false
		parsed.ackNumber = 0
	})
	select {
	case received := <-receives:
		_, sourceIp, destinationIp, transport, _ := parseIpv4(received.packet)
		rst := &parsedTcp{}
		if !parseTcpPacket(sourceIp, destinationIp, transport, rst) {
			t.Fatalf("rst did not parse as tcp")
		}
		AssertEqual(t, true, rst.rst)
		AssertEqual(t, true, rst.ack)
		AssertEqual(t, uint32(0), rst.seq)
		AssertEqual(t, uint32(5000+len("mid-stream")), rst.ackNumber)
	case <-time.After(2 * time.Second):
		t.Fatalf("no rst emitted for the no-ack orphan packet")
	}

	// never reset a reset
	sendOrphan(t, buffer, source, 40003, func(parsed *parsedTcp) {
		parsed.rst = true
	})
	select {
	case <-receives:
		t.Fatalf("a rst orphan must not be answered with a rst")
	case <-time.After(500 * time.Millisecond):
	}
}

func TestTcpOrphanRstDetachesPooledIpPath(t *testing.T) {
	for _, ipVersion := range []int{4, 6} {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			settings := DefaultTcpBufferSettingsWithBufferSize(8)
			settings.EnableOrphanRst = true
			settings.OrphanRstPerSecond = 0

			var sourceIp net.IP
			var destinationIp net.IP
			if ipVersion == 4 {
				sourceIp = net.IPv4(10, 20, 30, 40).To4()
				destinationIp = net.IPv4(203, 0, 113, 71).To4()
			} else {
				sourceIp = net.ParseIP("2001:db8::10").To16()
				destinationIp = net.ParseIP("2001:db8::71").To16()
			}

			var callbackPath *IpPath
			receive := func(
				_ TransferPath,
				_ protocol.ProvideMode,
				ipPath *IpPath,
				_ []byte,
			) {
				// tcpSend has already returned the input packet to the pool.
				// Take and overwrite that most-recent buffer before inspecting
				// the path. If IpPath still aliases it, these assertions see
				// the overwrite deterministically (and -race observes the
				// lifetime violation under concurrent reuse).
				clobber := MessagePoolGet(64)
				for i := range clobber {
					clobber[i] = 0xa5
				}
				callbackPath = &IpPath{
					Version:       ipPath.Version,
					SourceIp:      append(net.IP(nil), ipPath.SourceIp...),
					DestinationIp: append(net.IP(nil), ipPath.DestinationIp...),
				}
				MessagePoolReturn(clobber)
			}

			path := &IpPath{
				Version:         ipVersion,
				Protocol:        IpProtocolTcp,
				SourceIp:        sourceIp,
				SourcePort:      40001,
				DestinationIp:   destinationIp,
				DestinationPort: 443,
			}
			packet := MessagePoolCopy(ipOosTcpPacket(path, tcpFlagAck, []byte("orphan")))

			var parsed parsedTcp
			var parsedSourceIp net.IP
			var parsedDestinationIp net.IP
			var transport []byte
			var ok bool
			switch ipVersion {
			case 4:
				_, parsedSourceIp, parsedDestinationIp, transport, ok = parseIpv4(packet)
			case 6:
				_, parsedSourceIp, parsedDestinationIp, transport, ok = parseIpv6(packet)
			}
			if !ok || !parseTcpPacket(parsedSourceIp, parsedDestinationIp, transport, &parsed) {
				MessagePoolReturn(packet)
				t.Fatal("could not parse pooled tcp packet")
			}

			source := SourceId(NewId())
			var success bool
			var err error
			if ipVersion == 4 {
				buffer := NewTcp4Buffer(ctx, receive, settings)
				success, err = buffer.send(source, protocol.ProvideMode_Network, &parsed, -1, packet)
			} else {
				buffer := NewTcp6Buffer(ctx, receive, settings)
				success, err = buffer.send(source, protocol.ProvideMode_Network, &parsed, -1, packet)
			}
			if err != nil {
				t.Fatalf("send orphan: %v", err)
			}
			if success {
				t.Fatal("orphan send must report dropped")
			}
			if callbackPath == nil {
				t.Fatal("orphan reset callback was not invoked")
			}
			if !bytes.Equal(callbackPath.SourceIp, sourceIp) {
				t.Fatalf(
					"ip%d source ip aliases returned pool memory: got %v want %v",
					ipVersion,
					callbackPath.SourceIp,
					sourceIp,
				)
			}
			if !bytes.Equal(callbackPath.DestinationIp, destinationIp) {
				t.Fatalf(
					"ip%d destination ip aliases returned pool memory: got %v want %v",
					ipVersion,
					callbackPath.DestinationIp,
					destinationIp,
				)
			}
		}()
	}
}

func TestTcpOrphanRstRateLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tcpBufferSettings := DefaultTcpBufferSettingsWithBufferSize(8)
	tcpBufferSettings.EnableOrphanRst = true
	tcpBufferSettings.OrphanRstPerSecond = 2

	buffer, receives := orphanTestBuffer(ctx, tcpBufferSettings)

	source := SourceId(NewId())
	for i := range 5 {
		sendOrphan(t, buffer, source, uint16(41001+i), nil)
	}

	rstCount := 0
	deadline := time.After(1 * time.Second)
	for done := false; !done; {
		select {
		case <-receives:
			rstCount += 1
		case <-deadline:
			done = true
		}
	}
	AssertEqual(t, 2, rstCount)
}

func TestTcpOrphanRstDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tcpBufferSettings := DefaultTcpBufferSettingsWithBufferSize(8)
	tcpBufferSettings.EnableOrphanRst = false

	buffer, receives := orphanTestBuffer(ctx, tcpBufferSettings)

	sendOrphan(t, buffer, SourceId(NewId()), 42001, nil)
	select {
	case <-receives:
		t.Fatalf("rst emitted with EnableOrphanRst disabled")
	case <-time.After(500 * time.Millisecond):
	}
}
