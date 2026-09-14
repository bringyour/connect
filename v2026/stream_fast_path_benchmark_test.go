// This file compares the compact datagram path with the current WebRTC route
// and measures both serial aggregate work and independently running hop stages.
package connect

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"net/netip"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
	"golang.org/x/net/ipv4"
	"golang.org/x/net/ipv6"
)

// streamFastPathBatchConn is the batch syscall surface shared by x/net's ipv4
// and ipv6 packet conns; their Message types are one alias, so one node serves
// both families.
type streamFastPathBatchConn interface {
	ReadBatch(messages []ipv4.Message, flags int) (int, error)
	WriteBatch(messages []ipv4.Message, flags int) (int, error)
}

// newStreamFastPathBatchConn wraps the socket in the batch conn of its family.
func newStreamFastPathBatchConn(ipVersion int, udpConn *net.UDPConn) streamFastPathBatchConn {
	if ipVersion == 6 {
		return ipv6.NewPacketConn(udpConn)
	}
	return ipv4.NewPacketConn(udpConn)
}

// One socket represents one client in a directional stream. The same shape
// works for the source, every intermediary, and the destination.
type streamFastPathLoopbackNode struct {
	udpConn       *net.UDPConn
	packetConn    streamFastPathBatchConn
	address       *net.UDPAddr
	addressPort   netip.AddrPort
	readBuffers   [][]byte
	readMessages  []ipv4.Message
	readPackets   [][]byte
	writeBuffers  [][][]byte
	writeMessages []ipv4.Message
}

// Buffers and message vectors are fixed before the benchmark timer starts.
func newStreamFastPathLoopbackNode(
	b *testing.B,
	ipVersion int,
	batchSize int,
) *streamFastPathLoopbackNode {
	b.Helper()
	udpConn, err := net.ListenUDP(testUdpNetwork(ipVersion), testLoopbackUdpAddr(ipVersion))
	if err != nil {
		b.Fatalf("listen udp: %s", err)
	}
	if err := udpConn.SetReadBuffer(4 * 1024 * 1024); err != nil {
		udpConn.Close()
		b.Fatalf("set udp read buffer: %s", err)
	}
	if err := udpConn.SetWriteBuffer(4 * 1024 * 1024); err != nil {
		udpConn.Close()
		b.Fatalf("set udp write buffer: %s", err)
	}
	if err := udpConn.SetDeadline(time.Now().Add(10 * time.Minute)); err != nil {
		udpConn.Close()
		b.Fatalf("set udp deadline: %s", err)
	}

	node := &streamFastPathLoopbackNode{
		udpConn:       udpConn,
		packetConn:    newStreamFastPathBatchConn(ipVersion, udpConn),
		address:       udpConn.LocalAddr().(*net.UDPAddr),
		addressPort:   udpConn.LocalAddr().(*net.UDPAddr).AddrPort(),
		readBuffers:   make([][]byte, batchSize),
		readMessages:  make([]ipv4.Message, batchSize),
		readPackets:   make([][]byte, batchSize),
		writeBuffers:  make([][][]byte, batchSize),
		writeMessages: make([]ipv4.Message, batchSize),
	}
	for packetIndex := range batchSize {
		node.readBuffers[packetIndex] = make(
			[]byte,
			streamFastPathMaxWirePacketByteCount,
		)
		node.readMessages[packetIndex] = ipv4.Message{
			Buffers: [][]byte{node.readBuffers[packetIndex]},
		}
		node.writeBuffers[packetIndex] = make([][]byte, 1)
		node.writeMessages[packetIndex] = ipv4.Message{
			Buffers: node.writeBuffers[packetIndex],
		}
	}
	return node
}

// All input bytes are copied into the kernel before return, so their owners
// may be released immediately afterward.
func (self *streamFastPathLoopbackNode) writeBatch(
	packets [][]byte,
	destination *streamFastPathLoopbackNode,
) error {
	if runtime.GOOS != "linux" {
		for _, packet := range packets {
			writtenByteCount, err := self.udpConn.WriteToUDPAddrPort(
				packet,
				destination.addressPort,
			)
			if err != nil {
				return err
			}
			if writtenByteCount != len(packet) {
				return io.ErrShortWrite
			}
		}
		return nil
	}
	for packetIndex, packet := range packets {
		self.writeBuffers[packetIndex][0] = packet
		self.writeMessages[packetIndex].Addr = destination.address
	}
	writtenCount := 0
	for writtenCount < len(packets) {
		count, err := self.packetConn.WriteBatch(
			self.writeMessages[writtenCount:len(packets)],
			0,
		)
		if err != nil {
			return err
		}
		if count == 0 {
			return io.ErrNoProgress
		}
		writtenCount += count
	}
	return nil
}

// Returned packet views borrow the node's fixed read buffers until the next
// call. Linux fills many messages with recvmmsg; other platforms return one.
func (self *streamFastPathLoopbackNode) readBatch(packetCount int) ([][]byte, error) {
	if runtime.GOOS != "linux" {
		for packetIndex := range packetCount {
			readByteCount, _, err := self.udpConn.ReadFromUDPAddrPort(
				self.readBuffers[packetIndex],
			)
			if err != nil {
				return nil, err
			}
			self.readPackets[packetIndex] =
				self.readBuffers[packetIndex][:readByteCount]
		}
		return self.readPackets[:packetCount], nil
	}
	readCount := 0
	for readCount < packetCount {
		count, err := self.packetConn.ReadBatch(
			self.readMessages[readCount:packetCount],
			0,
		)
		if err != nil {
			return nil, err
		}
		if count == 0 {
			return nil, io.ErrNoProgress
		}
		readCount += count
	}
	for packetIndex := range packetCount {
		message := &self.readMessages[packetIndex]
		self.readPackets[packetIndex] = self.readBuffers[packetIndex][:message.N]
		message.N = 0
		message.NN = 0
		message.Flags = 0
		message.Addr = nil
	}
	return self.readPackets[:packetCount], nil
}

// The serial UDP benchmark includes every source/destination cipher,
// intermediary forwarder, and kernel UDP send/receive in one goroutine. It
// measures aggregate work, not the parallel pipeline of deployed hop clients.
func benchmarkStreamFastPathUDPEndToEnd(
	b *testing.B,
	ipVersion int,
	hopCount int,
	batchSize int,
) {
	chain := newStreamFastPathTestChain(b, hopCount)
	nodes := make([]*streamFastPathLoopbackNode, hopCount+1)
	for nodeIndex := range nodes {
		nodes[nodeIndex] = newStreamFastPathLoopbackNode(b, ipVersion, batchSize)
		defer nodes[nodeIndex].udpConn.Close()
	}

	innerPacket := bytes.Repeat([]byte{0x6c}, 1380)
	innerPackets := make([][]byte, batchSize)
	for packetIndex := range batchSize {
		innerPackets[packetIndex] = innerPacket
	}
	wirePackets := make([][]byte, batchSize)
	nextWirePackets := make([][]byte, batchSize)
	openedPackets := make([][]byte, batchSize)

	b.ReportAllocs()
	b.SetBytes(int64(len(innerPacket) * batchSize))
	b.ResetTimer()
	for b.Loop() {
		acceptedCount, err := chain.source.sealBatch(innerPackets, wirePackets)
		if err != nil || acceptedCount != batchSize {
			b.Fatalf("source batch accepted=%d err=%v", acceptedCount, err)
		}
		if err := nodes[0].writeBatch(wirePackets, nodes[1]); err != nil {
			b.Fatal(err)
		}
		for _, wirePacket := range wirePackets {
			MessagePoolReturn(wirePacket)
		}

		for intermediaryIndex, forwarder := range chain.forwarders {
			receivedPackets, err := nodes[intermediaryIndex+1].readBatch(batchSize)
			if err != nil {
				b.Fatal(err)
			}
			acceptedCount, err = forwarder.forwardBatch(
				receivedPackets,
				nextWirePackets,
			)
			if err != nil || acceptedCount != batchSize {
				b.Fatalf(
					"forwarder %d accepted=%d err=%v",
					intermediaryIndex,
					acceptedCount,
					err,
				)
			}
			if err := nodes[intermediaryIndex+1].writeBatch(
				nextWirePackets,
				nodes[intermediaryIndex+2],
			); err != nil {
				b.Fatal(err)
			}
			for _, wirePacket := range nextWirePackets {
				MessagePoolReturn(wirePacket)
			}
		}

		receivedPackets, err := nodes[len(nodes)-1].readBatch(batchSize)
		if err != nil {
			b.Fatal(err)
		}
		acceptedCount, err = chain.destination.openBatch(
			receivedPackets,
			openedPackets,
		)
		if err != nil || acceptedCount != batchSize {
			b.Fatalf("destination accepted=%d err=%v", acceptedCount, err)
		}
		if len(openedPackets[batchSize-1]) != len(innerPacket) ||
			openedPackets[batchSize-1][0] != innerPacket[0] {
			b.Fatal("destination payload mismatch")
		}
	}
}

// This runs each intermediary and the destination as an independent client
// stage. Eight batches may be in flight, enough to fill the stream without
// exceeding the configured socket buffers or making packet loss part of the
// measurement.
func benchmarkStreamFastPathUDPPipeline(
	b *testing.B,
	ipVersion int,
	hopCount int,
	batchSize int,
) {
	chain := newStreamFastPathTestChain(b, hopCount)
	nodes := make([]*streamFastPathLoopbackNode, hopCount+1)
	for nodeIndex := range nodes {
		nodes[nodeIndex] = newStreamFastPathLoopbackNode(b, ipVersion, batchSize)
		defer nodes[nodeIndex].udpConn.Close()
	}

	innerPacket := bytes.Repeat([]byte{0x6d}, 1380)
	innerPackets := make([][]byte, batchSize)
	for packetIndex := range batchSize {
		innerPackets[packetIndex] = innerPacket
	}
	sourceWirePackets := make([][]byte, batchSize)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	credits := make(chan struct{}, 8)
	for range cap(credits) {
		credits <- struct{}{}
	}
	workerErr := make(chan error, hopCount+1)
	destinationDone := make(chan struct{})
	var workers sync.WaitGroup
	var started sync.WaitGroup
	started.Add(len(chain.forwarders) + 1)

	for intermediaryIndex, forwarder := range chain.forwarders {
		workers.Add(1)
		go func() {
			defer workers.Done()
			nextWirePackets := make([][]byte, batchSize)
			started.Done()
			for range b.N {
				receivedPackets, err := nodes[intermediaryIndex+1].readBatch(batchSize)
				if err != nil {
					select {
					case workerErr <- err:
					case <-ctx.Done():
					}
					return
				}
				acceptedCount, err := forwarder.forwardBatch(
					receivedPackets,
					nextWirePackets,
				)
				if err != nil || acceptedCount != batchSize {
					if err == nil {
						err = errors.New("intermediary rejected a valid batch")
					}
					select {
					case workerErr <- err:
					case <-ctx.Done():
					}
					return
				}
				err = nodes[intermediaryIndex+1].writeBatch(
					nextWirePackets,
					nodes[intermediaryIndex+2],
				)
				for _, wirePacket := range nextWirePackets {
					MessagePoolReturn(wirePacket)
				}
				if err != nil {
					select {
					case workerErr <- err:
					case <-ctx.Done():
					}
					return
				}
			}
		}()
	}

	workers.Add(1)
	go func() {
		defer workers.Done()
		openedPackets := make([][]byte, batchSize)
		started.Done()
		for range b.N {
			receivedPackets, err := nodes[len(nodes)-1].readBatch(batchSize)
			if err != nil {
				select {
				case workerErr <- err:
				case <-ctx.Done():
				}
				return
			}
			acceptedCount, err := chain.destination.openBatch(
				receivedPackets,
				openedPackets,
			)
			if err != nil || acceptedCount != batchSize {
				if err == nil {
					err = errors.New("destination rejected a valid batch")
				}
				select {
				case workerErr <- err:
				case <-ctx.Done():
				}
				return
			}
			if len(openedPackets[batchSize-1]) != len(innerPacket) ||
				openedPackets[batchSize-1][0] != innerPacket[0] {
				select {
				case workerErr <- errors.New("destination payload mismatch"):
				case <-ctx.Done():
				}
				return
			}
			select {
			case credits <- struct{}{}:
			case <-ctx.Done():
				return
			}
		}
		close(destinationDone)
	}()

	started.Wait()
	b.ReportAllocs()
	b.SetBytes(int64(len(innerPacket) * batchSize))
	b.ResetTimer()
	for range b.N {
		select {
		case <-credits:
		case err := <-workerErr:
			b.Fatal(err)
		}
		acceptedCount, err := chain.source.sealBatch(
			innerPackets,
			sourceWirePackets,
		)
		if err != nil || acceptedCount != batchSize {
			b.Fatalf("source batch accepted=%d err=%v", acceptedCount, err)
		}
		err = nodes[0].writeBatch(sourceWirePackets, nodes[1])
		for _, wirePacket := range sourceWirePackets {
			MessagePoolReturn(wirePacket)
		}
		if err != nil {
			b.Fatal(err)
		}
	}
	select {
	case <-destinationDone:
	case err := <-workerErr:
		b.Fatal(err)
	}
	b.StopTimer()
	cancel()
	workers.Wait()
}

// This measures useful inner bytes across endpoint encryption and every hop's
// authenticate/open/reseal work in one process. Multi-hop results are aggregate
// CPU work; a deployed stream distributes that work across the hop machines.
func benchmarkStreamFastPathEndToEnd(b *testing.B, hopCount int) {
	chain := newStreamFastPathTestChain(b, hopCount)
	innerPacket := bytes.Repeat([]byte{0x5a}, 1380)

	innerView, owner, err := chain.sendToDestination(innerPacket)
	if err != nil || !bytes.Equal(innerView, innerPacket) {
		MessagePoolReturn(owner)
		b.Fatalf("warm send len=%d err=%v", len(innerView), err)
	}
	MessagePoolReturn(owner)

	b.ReportAllocs()
	b.SetBytes(int64(len(innerPacket)))
	b.ResetTimer()
	for b.Loop() {
		innerView, owner, err = chain.sendToDestination(innerPacket)
		if err != nil || len(innerView) != len(innerPacket) || innerView[0] != innerPacket[0] {
			MessagePoolReturn(owner)
			b.Fatalf("send len=%d err=%v", len(innerView), err)
		}
		MessagePoolReturn(owner)
	}
}

// One direct hop establishes the endpoint codec floor.
func BenchmarkStreamFastPathEndToEndOneHop(b *testing.B) {
	benchmarkStreamFastPathEndToEnd(b, 1)
}

// One intermediary proves that hop resealing has a uniform incremental cost.
func BenchmarkStreamFastPathEndToEndTwoHops(b *testing.B) {
	benchmarkStreamFastPathEndToEnd(b, 2)
}

// Four intermediaries show the middle of the supported stream range.
func BenchmarkStreamFastPathEndToEndFiveHops(b *testing.B) {
	benchmarkStreamFastPathEndToEnd(b, 5)
}

// Eight intermediaries measure the current maximum stream length.
func BenchmarkStreamFastPathEndToEndNineHops(b *testing.B) {
	benchmarkStreamFastPathEndToEnd(b, MaxMultihopLength+1)
}

// This exposes the one-datagram syscall floor without batching.
func BenchmarkStreamFastPathUDPOneHopSinglePacket(b *testing.B) {
	forEachIpVersionBenchmark(b, func(b *testing.B, ipVersion int) {
		benchmarkStreamFastPathUDPEndToEnd(b, ipVersion, 1, 1)
	})
}

// This measures the H1-sized four-packet drain at the P2P UDP boundary.
func BenchmarkStreamFastPathUDPOneHopBatch4(b *testing.B) {
	forEachIpVersionBenchmark(b, func(b *testing.B, ipVersion int) {
		benchmarkStreamFastPathUDPEndToEnd(b, ipVersion, 1, 4)
	})
}

// This measures the proposed eight-packet drain at the P2P UDP boundary.
func BenchmarkStreamFastPathUDPOneHopBatch8(b *testing.B) {
	forEachIpVersionBenchmark(b, func(b *testing.B, ipVersion int) {
		benchmarkStreamFastPathUDPEndToEnd(b, ipVersion, 1, 8)
	})
}

// This is the primary clean-path comparison with the production 64-packet
// drain size. On Linux the socket layer uses sendmmsg and recvmmsg.
func BenchmarkStreamFastPathUDPOneHopBatch64(b *testing.B) {
	forEachIpVersionBenchmark(b, func(b *testing.B, ipVersion int) {
		benchmarkStreamFastPathUDPEndToEnd(b, ipVersion, 1, 64)
	})
}

// This verifies that the identical UDP machinery composes through an
// intermediary without a direct-endpoint special case.
func BenchmarkStreamFastPathUDPTwoHopsBatch64(b *testing.B) {
	forEachIpVersionBenchmark(b, func(b *testing.B, ipVersion int) {
		benchmarkStreamFastPathUDPEndToEnd(b, ipVersion, 2, 64)
	})
}

// This applies the proposed eight-packet drain through one intermediary.
func BenchmarkStreamFastPathUDPTwoHopsBatch8(b *testing.B) {
	forEachIpVersionBenchmark(b, func(b *testing.B, ipVersion int) {
		benchmarkStreamFastPathUDPEndToEnd(b, ipVersion, 2, 8)
	})
}

// This measures the existing maximum of eight intermediaries and nine hops.
func BenchmarkStreamFastPathUDPNineHopsBatch64(b *testing.B) {
	forEachIpVersionBenchmark(b, func(b *testing.B, ipVersion int) {
		benchmarkStreamFastPathUDPEndToEnd(b, ipVersion, MaxMultihopLength+1, 64)
	})
}

// This overlaps source and destination work as separate client stages.
func BenchmarkStreamFastPathUDPPipelineOneHopBatch64(b *testing.B) {
	forEachIpVersionBenchmark(b, func(b *testing.B, ipVersion int) {
		benchmarkStreamFastPathUDPPipeline(b, ipVersion, 1, 64)
	})
}

// This measures a four-packet ready drain with independently scheduled P2P
// endpoint stages.
func BenchmarkStreamFastPathUDPPipelineOneHopBatch4(b *testing.B) {
	forEachIpVersionBenchmark(b, func(b *testing.B, ipVersion int) {
		benchmarkStreamFastPathUDPPipeline(b, ipVersion, 1, 4)
	})
}

// This measures an eight-packet ready drain with independently scheduled P2P
// endpoint stages.
func BenchmarkStreamFastPathUDPPipelineOneHopBatch8(b *testing.B) {
	forEachIpVersionBenchmark(b, func(b *testing.B, ipVersion int) {
		benchmarkStreamFastPathUDPPipeline(b, ipVersion, 1, 8)
	})
}

// This adds one independently running intermediary.
func BenchmarkStreamFastPathUDPPipelineTwoHopsBatch64(b *testing.B) {
	forEachIpVersionBenchmark(b, func(b *testing.B, ipVersion int) {
		benchmarkStreamFastPathUDPPipeline(b, ipVersion, 2, 64)
	})
}

// This applies the eight-packet pipeline through one independently scheduled
// intermediary.
func BenchmarkStreamFastPathUDPPipelineTwoHopsBatch8(b *testing.B) {
	forEachIpVersionBenchmark(b, func(b *testing.B, ipVersion int) {
		benchmarkStreamFastPathUDPPipeline(b, ipVersion, 2, 8)
	})
}

// This fills the supported eight-intermediary stream on independent stages.
func BenchmarkStreamFastPathUDPPipelineNineHopsBatch64(b *testing.B) {
	forEachIpVersionBenchmark(b, func(b *testing.B, ipVersion int) {
		benchmarkStreamFastPathUDPPipeline(b, ipVersion, MaxMultihopLength+1, 64)
	})
}

// This is the direct legacy comparison for the same 1,380 useful bytes. It
// exercises the real detached DataChannel and P2P route queues, including
// SCTP, DTLS, ICE, and UDP on loopback.
func BenchmarkStreamLegacyWebRtcRoute(b *testing.B) {
	benchmarkStreamWebRtcRoute(
		b,
		P2pDataPlaneModeLegacyOnly,
		DefaultP2pTransportSettings().ChannelBufferSize,
	)
}

// This exercises the production SRTP fast carrier through the same P2P route
// queues and loopback ICE association as the forced legacy comparison.
func BenchmarkStreamFastWebRtcRoute(b *testing.B) {
	benchmarkStreamWebRtcRoute(
		b,
		P2pDataPlaneModeFastOnly,
		DefaultP2pTransportSettings().ChannelBufferSize,
	)
}

// Measures the current legacy DataChannel route queue depth.
func BenchmarkStreamLegacyWebRtcRouteChannel4(b *testing.B) {
	benchmarkStreamWebRtcRoute(b, P2pDataPlaneModeLegacyOnly, 4)
}

// Measures an eight-message legacy DataChannel route queue.
func BenchmarkStreamLegacyWebRtcRouteChannel8(b *testing.B) {
	benchmarkStreamWebRtcRoute(b, P2pDataPlaneModeLegacyOnly, 8)
}

// Measures the current fast-carrier route queue depth.
func BenchmarkStreamFastWebRtcRouteChannel4(b *testing.B) {
	benchmarkStreamWebRtcRoute(b, P2pDataPlaneModeFastOnly, 4)
}

// Measures an eight-message fast-carrier route queue.
func BenchmarkStreamFastWebRtcRouteChannel8(b *testing.B) {
	benchmarkStreamWebRtcRoute(b, P2pDataPlaneModeFastOnly, 8)
}

// benchmarkStreamWebRtcRoute runs one forced production P2P carrier without
// changing the payload, route queue, peer setup, or receive verification.
func benchmarkStreamWebRtcRoute(
	b *testing.B,
	dataPlaneMode P2pDataPlaneMode,
	channelBufferSize int,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	settingsA := DefaultWebRtcSettings()
	settingsB := DefaultWebRtcSettings()
	settingsA.Log = NewNoopLogger()
	settingsB.Log = NewNoopLogger()
	settingsA.IceServerUrls = nil
	settingsB.IceServerUrls = nil
	settingsA.MaxPeerConnectionCount = 0
	settingsB.MaxPeerConnectionCount = 0
	settingsA.UseEgressOnlyIceInterfaces = true
	settingsB.UseEgressOnlyIceInterfaces = true

	signalPipeA := newSignalPipe(nil)
	signalPipeB := newSignalPipe(nil)
	managerA := newTestWebRtcManager(b, ctx, signalPipeA, settingsA)
	managerB := newTestWebRtcManager(b, ctx, signalPipeB, settingsB)
	signalPipeA.SetSignalReceiver(managerB)
	signalPipeB.SetSignalReceiver(managerA)

	peerIdA := NewId()
	peerIdB := NewId()
	streamId := NewId()
	passive, err := managerB.NewP2pConnPassive(
		ctx,
		NewTransferPath(peerIdB, peerIdA, streamId),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer passive.Close()
	active, err := managerA.NewP2pConnActive(
		ctx,
		NewTransferPath(peerIdA, peerIdB, streamId),
	)
	if err != nil {
		b.Fatal(err)
	}
	defer active.Close()
	readyDeadline := time.Now().Add(10 * time.Second)
	for !active.Connected() || !passive.Connected() ||
		(dataPlaneMode == P2pDataPlaneModeFastOnly &&
			(!active.(webRtcFastPathConn).FastPathReady() ||
				!passive.(webRtcFastPathConn).FastPathReady())) {
		if time.Now().After(readyDeadline) {
			b.Fatal("WebRTC route did not become ready")
		}
		time.Sleep(time.Millisecond)
	}

	transportSettings := DefaultP2pTransportSettings()
	transportSettings.DataPlaneMode = dataPlaneMode
	transportSettings.ChannelBufferSize = channelBufferSize
	transportCtx, transportCancel := context.WithCancel(ctx)
	defer transportCancel()
	sendTransport, sendRoute := NewP2pSendTransport(
		transportCtx,
		transportCancel,
		active,
		streamId,
		transportSettings,
	)
	receiveTransport, receiveRoute := NewP2pReceiveTransport(
		transportCtx,
		transportCancel,
		passive,
		streamId,
		transportSettings,
	)
	_ = sendTransport
	_ = receiveTransport

	innerPacket := bytes.Repeat([]byte{0x5a}, 1380)
	const maximumInFlightMessageCount = 64
	credits := make(chan struct{}, maximumInFlightMessageCount)
	for range maximumInFlightMessageCount {
		credits <- struct{}{}
	}
	receiveDone := make(chan error, 1)
	go func() {
		for range b.N {
			select {
			case <-transportCtx.Done():
				receiveDone <- transportCtx.Err()
				return
			case receivedPacket := <-receiveRoute:
				if !bytes.Equal(receivedPacket, innerPacket) {
					MessagePoolReturn(receivedPacket)
					receiveDone <- errors.New("legacy WebRTC payload mismatch")
					return
				}
				MessagePoolReturn(receivedPacket)
				credits <- struct{}{}
			}
		}
		receiveDone <- nil
	}()

	b.ReportAllocs()
	b.SetBytes(int64(len(innerPacket)))
	b.ResetTimer()
	for range b.N {
		select {
		case <-transportCtx.Done():
			b.Fatal(transportCtx.Err())
		case <-credits:
		}
		packet := MessagePoolCopy(innerPacket)
		select {
		case <-transportCtx.Done():
			MessagePoolReturn(packet)
			b.Fatal(transportCtx.Err())
		case sendRoute <- packet:
		}
	}
	if err := <-receiveDone; err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

// This is the current endpoint-envelope CPU lower bound: it includes Pack and
// TransferFrame encoding, outer encryption, both decodes, and outer decryption,
// but intentionally excludes Transfer queues, ACKs, SCTP, DTLS, ICE, and UDP.
func BenchmarkStreamLegacyEnvelopeCodec(b *testing.B) {
	c := newFrameCodecTestSequenceCipher(b)
	path := TransferPath{
		SourceId:      NewId(),
		DestinationId: NewId(),
		StreamId:      NewId(),
	}
	innerPacket := bytes.Repeat([]byte{0x5a}, 1380)
	frame := &protocol.Frame{
		MessageType:  protocol.MessageType_IpIpPacketFromProvider,
		MessageBytes: innerPacket,
		Raw:          true,
	}
	sendFrame := &sendPackFrame{
		path:           path,
		messageId:      NewId(),
		sequenceId:     NewId(),
		sequenceNumber: 1,
		head:           true,
		nack:           false,
		frames:         []*protocol.Frame{frame},
		tagSendTime:    1,
		sessionRole:    protocol.SequenceRole_SequenceRoleClient,
		sessionRoleSet: true,
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(innerPacket)))
	b.ResetTimer()
	for b.Loop() {
		sendFrame.sequenceNumber += 1
		innerFrame := marshalSendPackTransferFrame(sendFrame)
		outerFrame, err := c.SealOuterFrame(
			path,
			innerFrame,
			protocol.SequenceRole_SequenceRoleClient,
			false,
		)
		if err != nil {
			MessagePoolReturn(innerFrame)
			b.Fatal(err)
		}

		decodedOuter := inboundDecodedTransferFrames.take()
		if !unmarshalOwnedTransferFrame(outerFrame, decodedOuter, false) {
			inboundDecodedTransferFrames.put(decodedOuter)
			MessagePoolReturn(outerFrame)
			MessagePoolReturn(innerFrame)
			b.Fatal("decode outer frame")
		}
		openedFrame, err := c.Open(decodedOuter.frame.EncryptedTransferFrame)
		if err != nil {
			inboundDecodedTransferFrames.put(decodedOuter)
			MessagePoolReturn(outerFrame)
			MessagePoolReturn(innerFrame)
			b.Fatal(err)
		}
		decodedInner := inboundDecodedTransferFrames.take()
		if !unmarshalOwnedTransferFrame(openedFrame, decodedInner, true) ||
			decodedInner.frame.Pack == nil ||
			len(decodedInner.frame.Pack.Frames) != 1 ||
			len(decodedInner.frame.Pack.Frames[0].MessageBytes) != len(innerPacket) {
			inboundDecodedTransferFrames.put(decodedInner)
			inboundDecodedTransferFrames.put(decodedOuter)
			MessagePoolReturn(openedFrame)
			MessagePoolReturn(outerFrame)
			MessagePoolReturn(innerFrame)
			b.Fatal("decode inner frame")
		}

		inboundDecodedTransferFrames.put(decodedInner)
		inboundDecodedTransferFrames.put(decodedOuter)
		MessagePoolReturn(openedFrame)
		MessagePoolReturn(outerFrame)
		MessagePoolReturn(innerFrame)
	}
}
