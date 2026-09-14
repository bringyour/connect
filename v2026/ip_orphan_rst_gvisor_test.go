package connect

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/buffer"
	"gvisor.dev/gvisor/pkg/tcpip"
	"gvisor.dev/gvisor/pkg/tcpip/adapters/gonet"
	"gvisor.dev/gvisor/pkg/tcpip/header"
	"gvisor.dev/gvisor/pkg/tcpip/link/channel"
	"gvisor.dev/gvisor/pkg/tcpip/network/ipv4"
	"gvisor.dev/gvisor/pkg/tcpip/stack"
)

type orphanRstDialResult struct {
	conn net.Conn
	err  error
}

// A parse-only assertion cannot prove that a TCP implementation accepts an
// orphan reset: the IPv4 checksum, TCP pseudo-header checksum, tuple, and
// sequence all participate after parsing. Drive a real gVisor endpoint through
// SYN/SYN-ACK and inject the exact packet returned by tcpRstForOrphan.
func TestTcpOrphanRstTerminatesGvisorSocket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	clientIP := net.IPv4(10, 55, 0, 2).To4()
	serverIP := net.IPv4(203, 0, 113, 7).To4()
	const serverPort = 443
	const serverSequence = 9000

	gvisorStack := newTunStack(
		TcpBufferRange{Min: 4096, Default: 65536, Max: 262144},
		TcpBufferRange{Min: 4096, Default: 65536, Max: 262144},
		8*time.Second,
	)
	defer gvisorStack.Close()
	const nicID = tcpip.NICID(1)
	endpoint := channel.New(16, DefaultMtu, "")
	defer endpoint.Close()
	if tcpipErr := gvisorStack.CreateNIC(nicID, endpoint); tcpipErr != nil {
		t.Fatalf("create gVisor NIC: %v", tcpipErr)
	}
	if tcpipErr := gvisorStack.AddProtocolAddress(nicID, tcpip.ProtocolAddress{
		Protocol:          ipv4.ProtocolNumber,
		AddressWithPrefix: tcpip.AddrFrom4Slice(clientIP).WithPrefix(),
	}, stack.AddressProperties{}); tcpipErr != nil {
		t.Fatalf("add gVisor client address: %v", tcpipErr)
	}
	gvisorStack.AddRoute(tcpip.Route{Destination: header.IPv4EmptySubnet, NIC: nicID})

	dialResult := make(chan orphanRstDialResult, 1)
	go func() {
		conn, err := gonet.DialContextTCP(ctx, gvisorStack, tcpip.FullAddress{
			NIC:  nicID,
			Addr: tcpip.AddrFrom4Slice(serverIP),
			Port: serverPort,
		}, ipv4.ProtocolNumber)
		dialResult <- orphanRstDialResult{conn: conn, err: err}
	}()

	syn := readGvisorTcpPacket(t, ctx, endpoint)
	if !syn.syn || syn.ack || syn.destinationPort != serverPort {
		t.Fatalf("first outbound segment is not the dial SYN: %+v", syn)
	}
	injectGvisorPacket(t, endpoint, orphanRstSynAck(syn, serverSequence))

	var conn net.Conn
	select {
	case result := <-dialResult:
		if result.err != nil {
			t.Fatalf("gVisor dial after SYN-ACK: %v", result.err)
		}
		conn = result.conn
	case <-ctx.Done():
		t.Fatal("gVisor dial did not accept the SYN-ACK")
	}
	defer conn.Close()

	ack := readGvisorTcpPacket(t, ctx, endpoint)
	if !ack.ack || ack.syn || ack.rst || ack.ackNumber != serverSequence+1 {
		t.Fatalf("post-handshake segment is not the expected ACK: %+v", ack)
	}
	rst := tcpRstForOrphan(4, ack)
	defer MessagePoolReturn(rst)
	injectGvisorPacket(t, endpoint, rst)

	readResult := make(chan error, 1)
	go func() {
		oneByte := make([]byte, 1)
		_, err := conn.Read(oneByte)
		readResult <- err
	}()
	select {
	case err := <-readResult:
		if err == nil {
			t.Fatal("gVisor socket accepted a byte after its orphan reset")
		}
	case <-ctx.Done():
		t.Fatal("gVisor ignored the orphan reset and left the socket blocked")
	}
}

func readGvisorTcpPacket(t *testing.T, ctx context.Context, endpoint *channel.Endpoint) *parsedTcp {
	t.Helper()
	packetBuffer := endpoint.ReadContext(ctx)
	if packetBuffer == nil {
		t.Fatal("gVisor did not emit an expected TCP packet")
	}
	packet := append([]byte(nil), packetBuffer.ToView().AsSlice()...)
	packetBuffer.DecRef()
	_, sourceIP, destinationIP, transport, ok := parseIpv4(packet)
	if !ok {
		t.Fatalf("gVisor packet is not valid IPv4: %x", packet)
	}
	parsed := &parsedTcp{}
	if !parseTcpPacket(sourceIP, destinationIP, transport, parsed) {
		t.Fatalf("gVisor packet is not valid TCP: %x", packet)
	}
	return parsed
}

func orphanRstSynAck(syn *parsedTcp, serverSequence uint32) []byte {
	path := &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        append(net.IP(nil), syn.destinationIp...),
		SourcePort:      int(syn.destinationPort),
		DestinationIp:   append(net.IP(nil), syn.sourceIp...),
		DestinationPort: int(syn.sourcePort),
	}
	packet, tcpHeader := ipTransportPacket(path, ipProtocolNumberTcp, TcpHeaderSizeWithoutExtensions)
	binary.BigEndian.PutUint16(tcpHeader[0:2], uint16(path.SourcePort))
	binary.BigEndian.PutUint16(tcpHeader[2:4], uint16(path.DestinationPort))
	binary.BigEndian.PutUint32(tcpHeader[4:8], serverSequence)
	binary.BigEndian.PutUint32(tcpHeader[8:12], syn.seq+1)
	tcpHeader[12] = byte(TcpHeaderSizeWithoutExtensions/4) << 4
	tcpHeader[13] = tcpFlagSyn | tcpFlagAck
	binary.BigEndian.PutUint16(tcpHeader[14:16], 65535)
	binary.BigEndian.PutUint16(tcpHeader[16:18], ipPathTransportChecksum(path, ipProtocolNumberTcp, tcpHeader))
	return packet
}

func injectGvisorPacket(t *testing.T, endpoint *channel.Endpoint, packet []byte) {
	t.Helper()
	packetBuffer := stack.NewPacketBuffer(stack.PacketBufferOptions{Payload: buffer.MakeWithData(packet)})
	endpoint.InjectInbound(header.IPv4ProtocolNumber, packetBuffer)
	packetBuffer.DecRef()
}
