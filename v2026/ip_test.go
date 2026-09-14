package connect

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gopacket/gopacket"
	"github.com/gopacket/gopacket/layers"

	"github.com/urnetwork/connect/v2026/protocol"
)

func TestClientUdp4(t *testing.T) {
	testClient(t, testingNewClient, udp4Packet, (*IpPath).ToIp4Path)
}

func TestClientTcp4(t *testing.T) {
	testClient(t, testingNewClient, tcp4Packet, (*IpPath).ToIp4Path)
}

func TestClientUdp6(t *testing.T) {
	testClient(t, testingNewClient, udp6Packet, (*IpPath).ToIp6Path)
}

func TestClientTcp6(t *testing.T) {
	testClient(t, testingNewClient, tcp6Packet, (*IpPath).ToIp6Path)
}

type PacketGeneratorFunction func(int, int, int, int) ([]byte, []byte)

func TestUdp4Path(t *testing.T) {
	packet, _ := udp4Packet(1, 1, 1, 1)
	ipPath, err := ParseIpPath(packet)
	AssertEqual(t, nil, err)

	AssertEqual(t, IpProtocolUdp, ipPath.Protocol)

	AssertEqual(t, &IpPath{
		Version:         4,
		Protocol:        IpProtocolUdp,
		SourceIp:        net.IPv4(byte(72), byte(0), byte(0), byte(1)).To4(),
		SourcePort:      40000 + 1,
		DestinationIp:   net.IPv4(byte(72), byte(1+1), byte(1+1), byte(1+1)).To4(),
		DestinationPort: 443,
	}, ipPath)

	ip4Path := ipPath.ToIp4Path()
	AssertEqual(t, Ip4Path{
		Protocol:        IpProtocolUdp,
		SourceIp:        [4]byte(net.IPv4(byte(72), byte(0), byte(0), byte(1)).To4()),
		SourcePort:      40000 + 1,
		DestinationIp:   [4]byte(net.IPv4(byte(72), byte(1+1), byte(1+1), byte(1+1)).To4()),
		DestinationPort: 443,
	}, ip4Path)
}

func TestTcp4Path(t *testing.T) {
	packet, _ := tcp4Packet(1, 1, 1, 1)
	ipPath, err := ParseIpPath(packet)
	AssertEqual(t, nil, err)

	AssertEqual(t, IpProtocolTcp, ipPath.Protocol)

	AssertEqual(t, &IpPath{
		Version:             4,
		Protocol:            IpProtocolTcp,
		SourceIp:            net.IPv4(byte(72), byte(0), byte(0), byte(1)).To4(),
		SourcePort:          40000 + 1,
		DestinationIp:       net.IPv4(byte(72), byte(1+1), byte(1+1), byte(1+1)).To4(),
		DestinationPort:     443,
		TcpWindowSize:       4096,
		TcpPayloadByteCount: 4,
	}, ipPath)

	ip4Path := ipPath.ToIp4Path()
	AssertEqual(t, Ip4Path{
		Protocol:        IpProtocolTcp,
		SourceIp:        [4]byte(net.IPv4(byte(72), byte(0), byte(0), byte(1)).To4()),
		SourcePort:      40000 + 1,
		DestinationIp:   [4]byte(net.IPv4(byte(72), byte(1+1), byte(1+1), byte(1+1)).To4()),
		DestinationPort: 443,
	}, ip4Path)
}

func TestUdp6Path(t *testing.T) {
	packet, _ := udp6Packet(1, 1, 1, 1)
	ipPath, err := ParseIpPath(packet)
	AssertEqual(t, nil, err)

	AssertEqual(t, IpProtocolUdp, ipPath.Protocol)

	AssertEqual(t, &IpPath{
		Version:         6,
		Protocol:        IpProtocolUdp,
		SourceIp:        net.IPv4(byte(72), byte(0), byte(0), byte(1)).To16(),
		SourcePort:      40000 + 1,
		DestinationIp:   net.IPv4(byte(72), byte(1+1), byte(1+1), byte(1+1)).To16(),
		DestinationPort: 443,
	}, ipPath)

	ip6Path := ipPath.ToIp6Path()
	AssertEqual(t, Ip6Path{
		Protocol:        IpProtocolUdp,
		SourceIp:        [16]byte(net.IPv4(byte(72), byte(0), byte(0), byte(1)).To16()),
		SourcePort:      40000 + 1,
		DestinationIp:   [16]byte(net.IPv4(byte(72), byte(1+1), byte(1+1), byte(1+1)).To16()),
		DestinationPort: 443,
	}, ip6Path)
}

func TestTcp6Path(t *testing.T) {
	packet, _ := tcp6Packet(1, 1, 1, 1)
	ipPath, err := ParseIpPath(packet)
	AssertEqual(t, nil, err)

	AssertEqual(t, IpProtocolTcp, ipPath.Protocol)

	AssertEqual(t, &IpPath{
		Version:             6,
		Protocol:            IpProtocolTcp,
		SourceIp:            net.IPv4(byte(72), byte(0), byte(0), byte(1)).To16(),
		SourcePort:          40000 + 1,
		DestinationIp:       net.IPv4(byte(72), byte(1+1), byte(1+1), byte(1+1)).To16(),
		DestinationPort:     443,
		TcpWindowSize:       4096,
		TcpPayloadByteCount: 4,
	}, ipPath)

	ip6Path := ipPath.ToIp6Path()
	AssertEqual(t, Ip6Path{
		Protocol:        IpProtocolTcp,
		SourceIp:        [16]byte(net.IPv4(byte(72), byte(0), byte(0), byte(1)).To16()),
		SourcePort:      40000 + 1,
		DestinationIp:   [16]byte(net.IPv4(byte(72), byte(1+1), byte(1+1), byte(1+1)).To16()),
		DestinationPort: 443,
	}, ip6Path)
}

func udp4Packet(s int, i int, j int, k int) (packet []byte, payload []byte) {
	payload = make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, uint32(s))

	ipPath := &IpPath{
		Version:         4,
		Protocol:        IpProtocolUdp,
		SourceIp:        net.IPv4(72, 0, 0, 1),
		SourcePort:      40000 + s,
		DestinationIp:   net.IPv4(byte(72), byte(1+i), byte(1+j), byte(1+k)),
		DestinationPort: 443,
	}

	packet = ipOosUdpPacket(ipPath, payload)
	return
}

func tcp4Packet(s int, i int, j int, k int) (packet []byte, payload []byte) {
	payload = make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, uint32(s))

	ipPath := &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        net.IPv4(72, 0, 0, 1),
		SourcePort:      40000 + s,
		DestinationIp:   net.IPv4(byte(72), byte(1+i), byte(1+j), byte(1+k)),
		DestinationPort: 443,
	}

	packet = ipOosTcpPacket(ipPath, 0, payload)
	return
}

func udp6Packet(s int, i int, j int, k int) (packet []byte, payload []byte) {
	payload = make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, uint32(s))

	ipPath := &IpPath{
		Version:         6,
		Protocol:        IpProtocolUdp,
		SourceIp:        net.IPv4(72, 0, 0, 1),
		SourcePort:      40000 + s,
		DestinationIp:   net.IPv4(byte(72), byte(1+i), byte(1+j), byte(1+k)),
		DestinationPort: 443,
	}

	packet = ipOosUdpPacket(ipPath, payload)
	return
}

func tcp6Packet(s int, i int, j int, k int) (packet []byte, payload []byte) {
	payload = make([]byte, 4)
	binary.LittleEndian.PutUint32(payload, uint32(s))

	ipPath := &IpPath{
		Version:         6,
		Protocol:        IpProtocolTcp,
		SourceIp:        net.IPv4(72, 0, 0, 1),
		SourcePort:      40000 + s,
		DestinationIp:   net.IPv4(byte(72), byte(1+i), byte(1+j), byte(1+k)),
		DestinationPort: 443,
	}

	packet = ipOosTcpPacket(ipPath, 0, payload)
	return
}

// This exact-delivery fixture keeps admission loss out of its routing
// assertions. Focused Transfer tests force full queues; this capacity covers
// the fixture's complete one-direction burst without changing production
// defaults.
const testClientTransferBufferSize = 4096

func testingNewClient(ctx context.Context, providerClient *Client, receivePacketCallback ReceivePacketFunction) (UserNatClient, error) {
	settings := DefaultClientSettingsWithBufferSize(testClientTransferBufferSize)
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)

	routeSend := make(chan []byte)
	routeReceive := make(chan []byte)

	transportSend := NewSendGatewayTransport()
	transportReceive := NewReceiveGatewayTransport()
	client.RouteManager().UpdateTransport(transportSend, []Route{routeSend})
	client.RouteManager().UpdateTransport(transportReceive, []Route{routeReceive})

	client.ContractManager().AddNoContractPeer(providerClient.ClientId())

	providerTransportSend := NewSendClientTransport(DestinationId(client.ClientId()))
	providerTransportReceive := NewReceiveGatewayTransport()
	providerClient.RouteManager().UpdateTransport(providerTransportReceive, []Route{routeSend})
	providerClient.RouteManager().UpdateTransport(providerTransportSend, []Route{routeReceive})

	providerClient.ContractManager().AddNoContractPeer(client.ClientId())

	return NewRemoteUserNatClient(
		client,
		receivePacketCallback,
		[]MultiHopId{
			RequireMultiHopId(providerClient.ClientId()),
		},
		protocol.ProvideMode_Network,
	), nil
}

// Test with bounded production Transfer handoffs. Deterministic callback
// regressions cover full and zero-capacity admission; this broader fixture
// verifies routing, retransmission, and exact payload delivery under load.
func testClient[P comparable](
	t *testing.T,
	userNatClientGenerator func(context.Context, *Client, ReceivePacketFunction) (UserNatClient, error),
	packetGenerator PacketGeneratorFunction,
	toComparableIpPath func(*IpPath) P,
) {

	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}

	// runs a send-receive test on the `UserNatClient` produced by `userNatClientGenerator`
	// this is a multi-threaded stress test that is meant to stress the buffers and routing

	// n destinations
	// all have the same receiver callback, put into a channel of messages
	// echo the received packet, with paths reversed

	// create fake packets for all iterations of i,j,k in a range
	// retransmit some packets by increasing source port s
	// make sure all packets are received
	// make sure all packets are echoed back

	timeout := 30 * time.Second

	m := 6
	// Keep enough independent tuples to exercise recovery and routing without
	// making the package's correctness suite a long-duration performance run.
	n := 2
	repeatCount := 6
	parallelCount := 6
	echoCount := 2

	// each packet gets echoed back
	sendCount := parallelCount * m * n * n * n * repeatCount
	totalCount := sendCount * (1 + echoCount)

	// cMutex := sync.Mutex{}
	// cSendCount := 0
	// cReceiveCount := 0

	ctx, cancel := context.WithCancel(context.Background())

	clientId := NewId()
	providerClientId := NewId()

	settings := DefaultClientSettingsWithBufferSize(testClientTransferBufferSize)
	providerClient := NewClient(ctx, providerClientId, NewNoContractClientOob(), settings)

	type receivePacket struct {
		source TransferPath
		packet []byte
	}

	// Receive callbacks may not block. Size the test's single collector for the
	// exact expected workload and make both callback handoffs nonblocking.
	receivePackets := make(chan *receivePacket, totalCount)
	var receivePacketOverflowCount atomic.Int64
	asyncErrors := make(chan error, 1)
	recordAsyncError := func(err error) {
		select {
		case asyncErrors <- err:
		default:
		}
	}

	receivePacketCallback := func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		// record the echo packet

		// cMutex.Lock()
		// cReceiveCount += 1
		// // fmt.Printf("C Receive %d/%d (%.2f%%)\n", cReceiveCount, totalCount, 100.0 * float32(cReceiveCount) / float32(totalCount))
		// cMutex.Unlock()

		ipPath, payload, err := ParseIpPathWithPayload(packet)
		if err != nil {
			recordAsyncError(fmt.Errorf("parse client receive packet: %w", err))
			return
		}
		packet = ipOosPacket(ipPath.Reverse(), payload)

		receivePacket := &receivePacket{
			source: source,
			packet: packet,
		}

		select {
		case <-ctx.Done():
		case receivePackets <- receivePacket:
		default:
			receivePacketOverflowCount.Add(1)
			recordAsyncError(errors.New("client receive collector overflow"))
		}
	}

	natClient, err := userNatClientGenerator(ctx, providerClient, receivePacketCallback)
	AssertEqual(t, err, nil)

	// A receive callback cannot make a blocking send. One bounded queue and a
	// fixed worker set own every echo send, avoiding the old goroutine-per-echo
	// memory spike while preserving all expected echo work.
	echoPackets := make(chan *receivePacket, sendCount)
	var echoWorkerWaitGroup sync.WaitGroup
	for workerIndex := 0; workerIndex < parallelCount; workerIndex += 1 {
		echoWorkerWaitGroup.Add(1)
		go func() {
			defer echoWorkerWaitGroup.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case receivePacket := <-echoPackets:
					ipPath, payload, parseErr := ParseIpPathWithPayload(receivePacket.packet)
					if parseErr != nil {
						recordAsyncError(fmt.Errorf("parse provider echo packet: %w", parseErr))
						continue
					}
					packet := ipOosPacket(ipPath.Reverse(), payload)
					for echoIndex := 0; echoIndex < echoCount; echoIndex += 1 {
						ipPacketFromProvider := &protocol.IpPacketFromProvider{
							IpPacket: &protocol.IpPacket{
								PacketBytes: packet,
							},
						}
						frame, frameErr := ToFrame(ipPacketFromProvider, DefaultProtocolVersion)
						if frameErr != nil {
							recordAsyncError(fmt.Errorf("encode provider echo: %w", frameErr))
							break
						}
						if !providerClient.SendWithTimeout(frame, receivePacket.source.SourceId, func(error) {}, -1) {
							if ctx.Err() == nil {
								recordAsyncError(errors.New("send provider echo"))
							}
							break
						}
					}
				}
			}
		}()
	}
	var sendWorkerWaitGroup sync.WaitGroup
	defer func() {
		natClient.Close()
		cancel()
		providerClient.Cancel()
		sendWorkerWaitGroup.Wait()
		echoWorkerWaitGroup.Wait()
	}()

	providerClient.AddReceiveCallback(func(source TransferPath, frames []*protocol.Frame, peer Peer) {
		// cMutex.Lock()
		// cReceiveCount += 1
		// // fmt.Printf("C Receive %d/%d (%.2f%%)\n", cReceiveCount, totalCount, 100.0 * float32(cReceiveCount) / float32(totalCount))
		// cMutex.Unlock()

		for _, frame := range frames {
			if ipPacketToProvider_, err := FromFrame(frame); err == nil {
				if ipPacketToProvider, ok := ipPacketToProvider_.(*protocol.IpPacketToProvider); ok {
					// Receive callback buffers are only valid for the duration of the
					// callback. This test retains and echoes the packet asynchronously,
					// so keep an owned copy.
					packet := append([]byte(nil), ipPacketToProvider.IpPacket.PacketBytes...)

					receivePacket := &receivePacket{
						source: source,
						packet: packet,
					}

					select {
					case receivePackets <- receivePacket:
					default:
						receivePacketOverflowCount.Add(1)
						recordAsyncError(errors.New("provider receive collector overflow"))
					}

					select {
					case <-ctx.Done():
					case echoPackets <- receivePacket:
					default:
						recordAsyncError(errors.New("provider echo queue overflow"))
					}
				}
			}

		}
	})

	for p := 0; p < parallelCount; p += 1 {
		sendWorkerWaitGroup.Add(1)
		go func() {
			defer sendWorkerWaitGroup.Done()
			source := SourceId(clientId)
			packetCount := 0
			for s := 0; s < m; s += 1 {
				for i := 0; i < n; i += 1 {
					for j := 0; j < n; j += 1 {
						for k := 0; k < n; k += 1 {
							for a := 0; a < repeatCount; a += 1 {
								packetCount += 1
								packet, _ := packetGenerator(s, i, j, k)
								success := natClient.SendPacket(source, protocol.ProvideMode_Network, packet, -1)
								if !success {
									if ctx.Err() == nil {
										recordAsyncError(fmt.Errorf("send packet %d with %T", packetCount, natClient))
									}
									return
								}

								// cMutex.Lock()
								// cSendCount += 1
								// // fmt.Printf("C Send %d/%d (%.2f%%)\n", cSendCount, totalCount, 100.0 * float32(cSendCount) / float32(totalCount))
								// cMutex.Unlock()
							}
						}
					}
				}
			}
		}()
	}

	comparableIpPathPayloads := map[P][][]byte{}
	comparableIpPathSources := map[P]map[TransferPath]bool{}

	for i := 0; i < totalCount; i += 1 {
		if totalCount < 100 || i%(totalCount/100) == 0 {
			fmt.Printf("[testr]%d/%d (%.2f%%)\n", i, totalCount, 100*float32(i)/float32(totalCount))
		}
		select {
		case receivePacket := <-receivePackets:
			// fmt.Printf("Receive %d/%d (%.2f%%)\n", i + 1, totalCount, 100.0 * float32(i + 1) / float32(totalCount))

			ipPath, payload, err := ParseIpPathWithPayload(receivePacket.packet)
			AssertEqual(t, err, nil)

			// var payload []byte
			// switch ipPath.Version {
			// case 4:
			// 	ipv4 := layers.IPv4{}
			// 	ipv4.DecodeFromBytes(receivePacket.packet, gopacket.NilDecodeFeedback)
			// 	payload = ipv4.Payload
			// case 6:
			// 	ipv6 := layers.IPv6{}
			// 	ipv6.DecodeFromBytes(receivePacket.packet, gopacket.NilDecodeFeedback)
			// 	payload = ipv6.Payload
			// }

			// switch ipPath.Protocol {
			// case IpProtocolUdp:
			// 	udp := layers.UDP{}
			// 	udp.DecodeFromBytes(payload, gopacket.NilDecodeFeedback)
			// 	payload = udp.Payload
			// case IpProtocolTcp:
			// 	tcp := layers.TCP{}
			// 	tcp.DecodeFromBytes(payload, gopacket.NilDecodeFeedback)
			// 	payload = tcp.Payload
			// }

			comparableIpPath := toComparableIpPath(ipPath)
			comparableIpPathPayloads[comparableIpPath] = append(comparableIpPathPayloads[comparableIpPath], payload)

			sources, ok := comparableIpPathSources[comparableIpPath]
			if !ok {
				sources = map[TransferPath]bool{}
				comparableIpPathSources[comparableIpPath] = sources
			}
			sources[receivePacket.source] = true
		case <-time.After(timeout):
			fmt.Printf("[TIMEOUT]receive\n")
			t.FailNow()
		case asyncErr := <-asyncErrors:
			t.Fatalf("asynchronous test worker: %v", asyncErr)
		}
	}
	select {
	case <-receivePackets:
		// excesss packets received
		t.FailNow()
	case <-time.After(1 * time.Second):
	}
	if got := receivePacketOverflowCount.Load(); got != 0 {
		t.Fatalf("receive callback collector overflowed %d time(s)", got)
	}

	// make sure all messages were received
	// make sure each path has just one source
	for s := 0; s < m; s += 1 {
		for i := 0; i < n; i += 1 {
			for j := 0; j < n; j += 1 {
				for k := 0; k < n; k += 1 {

					packet, payload := packetGenerator(s, i, j, k)
					ipPath, err := ParseIpPath(packet)
					AssertEqual(t, err, nil)
					comparableIpPath := toComparableIpPath(ipPath)

					payloads := comparableIpPathPayloads[comparableIpPath]

					for _, b := range payloads {
						AssertEqual(t, b, payload)
					}

					AssertEqual(t, parallelCount*repeatCount*(1+echoCount), len(payloads))

					sources := comparableIpPathSources[comparableIpPath]

					if 0 < echoCount {
						AssertEqual(t, 2, len(sources))
					} else {
						AssertEqual(t, 1, len(sources))
					}
				}
			}
		}
	}
}

// egress tests
//
// these tests use a `Tun` as the client network stack, bridged to a
// `LocalUserNat` that egresses to servers on loopback. the bridge mirrors the
// provider data path: tun read -> local user nat send, and
// local user nat receive -> tun write. the bridge is lossless and in order in
// both directions, which the tcp and udp sequence implementations assume.

// connects the packet side of `tun` to `localUserNat`.
// returns a function that removes the receive callback.
func bridgeTunToLocalUserNat(tun *Tun, localUserNat *LocalUserNat, source TransferPath) func() {
	return bridgeTunToLocalUserNatWithBatchSize(tun, localUserNat, source, 64)
}

// Connects the same bridge while making the TUN read-ahead cap explicit for
// batch-depth benchmarks.
func bridgeTunToLocalUserNatWithBatchSize(
	tun *Tun,
	localUserNat *LocalUserNat,
	source TransferPath,
	maximumPacketCount int,
) func() {
	go HandleError(func() {
		packets := make([][]byte, max(1, maximumPacketCount))
		for {
			n, err := tun.ReadBatch(packets)
			if err != nil {
				return
			}
			// the local user nat owns the batch when send succeeds
			batch := make([][]byte, n)
			copy(batch, packets[0:n])
			if !localUserNat.SendPackets(source, protocol.ProvideMode_Network, batch, -1) {
				for _, packet := range batch {
					MessagePoolReturn(packet)
				}
				return
			}
		}
	})
	return localUserNat.AddReceivePacketCallback(func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		// `Tun.Write` copies the packet into the gvisor stack,
		// so the packet is not retained past the callback
		tun.Write(packet)
	})
}

// creates a deterministic payload so that corrupted, reordered,
// or cross-flow echo bytes are detected
func testingEgressPayload(seed int, payloadSize int) []byte {
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(31*seed + i)
	}
	return payload
}

// tests tcp egress through the local user nat with parallel connections and
// payload sizes that cross the socket read buffer and mtu segmentation boundaries
func TestIpEgressTcp4(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// tcp echo server on loopback
	echoListener, err := net.Listen("tcp4", "127.0.0.1:0")
	AssertEqual(t, err, nil)
	defer echoListener.Close()
	go HandleError(func() {
		for {
			conn, err := echoListener.Accept()
			if err != nil {
				return
			}
			go HandleError(func() {
				defer conn.Close()
				io.Copy(conn, conn)
			})
		}
	})

	tun, err := CreateTunWithDefaults(ctx)
	AssertEqual(t, err, nil)
	defer tun.Close()

	localUserNatSettings := DefaultLocalUserNatSettings()
	localUserNatSettings.SendShardCount = 4
	localUserNat := NewLocalUserNat(ctx, "testEgress", localUserNatSettings)
	defer localUserNat.Close()

	removeReceiveCallback := bridgeTunToLocalUserNat(tun, localUserNat, SourceId(NewId()))
	defer removeReceiveCallback()

	// Exercise the TCP socket read boundary on both sides of DefaultMtu minus
	// the maximum 60 bytes of IPv4/TCP headers; data packets segment at the
	// global tunnel MTU.
	maximumPayloadByteCount := DefaultMtu - 60
	payloadSizes := []int{
		1,
		maximumPayloadByteCount - 1,
		maximumPayloadByteCount,
		maximumPayloadByteCount + 1,
		2048,
		16384,
		1 << 20,
	}

	parallelCount := 4
	flowErrs := make(chan error, parallelCount)
	for p := 0; p < parallelCount; p += 1 {
		go HandleError(func() {
			flowErrs <- func() error {
				conn, err := tun.DialContext(ctx, "tcp", echoListener.Addr().String())
				if err != nil {
					return fmt.Errorf("dial: %w", err)
				}
				defer conn.Close()

				for _, payloadSize := range payloadSizes {
					payload := testingEgressPayload(p, payloadSize)

					// read the echo concurrently with the write
					// so that neither side blocks on full buffers
					readErr := make(chan error, 1)
					go HandleError(func() {
						readErr <- func() error {
							echoPayload := make([]byte, payloadSize)
							if n, err := readFullWithProgressDeadline(ctx, conn, echoPayload, 10*time.Second); err != nil {
								stats := tun.Stats()
								return fmt.Errorf(
									"read size=%d received=%d: %w (ip=%d/%d/%d malformed=%d tcp=%d/%d checksum=%d dropped=%d tx_no_buffer=%d)",
									payloadSize,
									n,
									err,
									stats.IP.PacketsReceived.Value(),
									stats.IP.ValidPacketsReceived.Value(),
									stats.IP.PacketsDelivered.Value(),
									stats.IP.MalformedPacketsReceived.Value(),
									stats.TCP.ValidSegmentsReceived.Value(),
									stats.TCP.InvalidSegmentsReceived.Value(),
									stats.TCP.ChecksumErrors.Value(),
									stats.DroppedPackets.Value(),
									stats.NICs.TxPacketsDroppedNoBufferSpace.Value(),
								)
							}
							if !bytes.Equal(payload, echoPayload) {
								return fmt.Errorf("echo mismatch size=%d", payloadSize)
							}
							return nil
						}()
					})

					if err := writeConnPhaseWithDeadline(ctx, conn, payload, 10*time.Second); err != nil {
						return fmt.Errorf("write size=%d: %w", payloadSize, err)
					}
					if err := <-readErr; err != nil {
						return err
					}
				}
				return nil
			}()
		})
	}
	for p := 0; p < parallelCount; p += 1 {
		select {
		case err := <-flowErrs:
			AssertEqual(t, err, nil)
		case <-ctx.Done():
			t.Fatal("timeout")
		}
	}
}

// tests that a server-side close propagates to the client as a fin
// ordered after all of the stream data
func TestIpEgressTcp4ServerClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	closeMessage := testingEgressPayload(7, 4096)

	// server writes one message and closes
	serverListener, err := net.Listen("tcp4", "127.0.0.1:0")
	AssertEqual(t, err, nil)
	defer serverListener.Close()
	go HandleError(func() {
		for {
			conn, err := serverListener.Accept()
			if err != nil {
				return
			}
			go HandleError(func() {
				conn.Write(closeMessage)
				conn.Close()
			})
		}
	})

	tun, err := CreateTunWithDefaults(ctx)
	AssertEqual(t, err, nil)
	defer tun.Close()

	localUserNatSettings := DefaultLocalUserNatSettings()
	localUserNatSettings.SendShardCount = 4
	localUserNat := NewLocalUserNat(ctx, "testEgress", localUserNatSettings)
	defer localUserNat.Close()

	removeReceiveCallback := bridgeTunToLocalUserNat(tun, localUserNat, SourceId(NewId()))
	defer removeReceiveCallback()

	conn, err := tun.DialContext(ctx, "tcp", serverListener.Addr().String())
	AssertEqual(t, err, nil)
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	echoPayload := make([]byte, len(closeMessage))
	_, err = io.ReadFull(conn, echoPayload)
	AssertEqual(t, err, nil)
	AssertEqual(t, bytes.Equal(closeMessage, echoPayload), true)

	// the server close must propagate as eof
	_, err = conn.Read(make([]byte, 1))
	AssertEqual(t, err, io.EOF)
}

// tests that a dial to a destination that refuses the connection
// fails within the tun dial timeout
func TestIpEgressTcp4ConnectFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// reserve a loopback port with no listener
	probeListener, err := net.Listen("tcp4", "127.0.0.1:0")
	AssertEqual(t, err, nil)
	unusedAddr := probeListener.Addr().String()
	probeListener.Close()

	tunSettings := DefaultTunSettings()
	tunSettings.DialRace = 1
	tunSettings.DialRaceTimeout = 500 * time.Millisecond
	tunSettings.DialTimeout = 2 * time.Second
	tun, err := CreateTun(ctx, tunSettings)
	AssertEqual(t, err, nil)
	defer tun.Close()

	localUserNatSettings := DefaultLocalUserNatSettings()
	localUserNatSettings.SendShardCount = 4
	localUserNat := NewLocalUserNat(ctx, "testEgress", localUserNatSettings)
	defer localUserNat.Close()

	removeReceiveCallback := bridgeTunToLocalUserNat(tun, localUserNat, SourceId(NewId()))
	defer removeReceiveCallback()

	_, err = tun.DialContext(ctx, "tcp", unusedAddr)
	AssertNotEqual(t, err, nil)
}

// tests udp egress through the local user nat with parallel flows.
// each flow echoes serially so that the number of packets in flight stays
// below all of the buffer sizes, which makes the test lossless by construction.
func TestIpEgressUdp4(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	// udp echo server on loopback
	echoConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	AssertEqual(t, err, nil)
	defer echoConn.Close()
	go HandleError(func() {
		buffer := make([]byte, 2048)
		for {
			n, addr, err := echoConn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			echoConn.WriteToUDP(buffer[0:n], addr)
		}
	})

	tun, err := CreateTunWithDefaults(ctx)
	AssertEqual(t, err, nil)
	defer tun.Close()

	localUserNatSettings := DefaultLocalUserNatSettings()
	localUserNatSettings.SendShardCount = 4
	localUserNat := NewLocalUserNat(ctx, "testEgress", localUserNatSettings)
	defer localUserNat.Close()

	removeReceiveCallback := bridgeTunToLocalUserNat(tun, localUserNat, SourceId(NewId()))
	defer removeReceiveCallback()

	// the maximum payload that stays a single packet within the tun mtu
	maxPayloadSize := DefaultMtu - Ipv4HeaderSizeWithoutExtensions - UdpHeaderSize
	payloadSizes := []int{1, 512, maxPayloadSize}

	parallelCount := 4
	roundCount := 16
	flowErrs := make(chan error, parallelCount)
	for p := 0; p < parallelCount; p += 1 {
		go HandleError(func() {
			flowErrs <- func() error {
				conn, err := tun.DialContext(ctx, "udp", echoConn.LocalAddr().String())
				if err != nil {
					return fmt.Errorf("dial: %w", err)
				}
				defer conn.Close()

				echoPayload := make([]byte, maxPayloadSize+1)
				for round := 0; round < roundCount; round += 1 {
					for _, payloadSize := range payloadSizes {
						payload := testingEgressPayload(31*p+round, payloadSize)
						if _, err := conn.Write(payload); err != nil {
							return fmt.Errorf("write round=%d size=%d: %w", round, payloadSize, err)
						}
						conn.SetReadDeadline(time.Now().Add(30 * time.Second))
						n, err := conn.Read(echoPayload)
						if err != nil {
							return fmt.Errorf("read round=%d size=%d: %w", round, payloadSize, err)
						}
						if !bytes.Equal(payload, echoPayload[0:n]) {
							return fmt.Errorf("echo mismatch round=%d size=%d n=%d", round, payloadSize, n)
						}
					}
				}
				return nil
			}()
		})
	}
	for p := 0; p < parallelCount; p += 1 {
		select {
		case err := <-flowErrs:
			AssertEqual(t, err, nil)
		case <-ctx.Done():
			t.Fatal("timeout")
		}
	}
}

// egress benchmarks
//
// these measure throughput of the tcp and udp egress flows through the
// local user nat, using the same bridge as the egress tests. run with e.g.
//   go test -run xxx -bench BenchmarkIpEgress -benchtime 3s -cpuprofile profile/cpu -memprofile profile/memory
// the tun buffer size mirrors the server proxy device sequence buffer size,
// so the tun is not the bottleneck.

// creates a tun bridged to a local user nat for benchmarks.
// returns the tun and a function that tears the bridge down.
func newBenchmarkEgressBridge(b *testing.B, ctx context.Context) (*Tun, func()) {
	return newBenchmarkEgressBridgeWithMtu(b, ctx, DefaultMtu)
}

// `newBenchmarkEgressBridge` with the send shard count applied to the
// local user nat. zero uses the default.
// the per flow tcp max window is capped so that the aggregate in-flight
// bytes of parallel flows stay below the tun channel capacity. the tun
// link drops on overflow and the sequences do not support fast retransmit,
// so an overflow collapses tcp throughput to retransmit timeouts.
func newBenchmarkEgressBridgeWithShardCount(b *testing.B, ctx context.Context, sendShardCount int) (*Tun, func()) {
	tunSettings := DefaultTunSettingsWithBufferSize(2048)
	tun, err := CreateTun(ctx, tunSettings)
	if err != nil {
		b.Fatal(err)
	}
	localUserNatSettings := DefaultLocalUserNatSettings()
	if 0 < sendShardCount {
		localUserNatSettings.SendShardCount = sendShardCount
	}
	localUserNatSettings.TcpBufferSettings.MaxWindowSize = uint32(kib(256))
	localUserNat := NewLocalUserNat(ctx, "benchEgress", localUserNatSettings)
	removeReceiveCallback := bridgeTunToLocalUserNat(tun, localUserNat, SourceId(NewId()))
	closeBridge := func() {
		removeReceiveCallback()
		localUserNat.Close()
		tun.Close()
	}
	return tun, closeBridge
}

// `newBenchmarkEgressBridge` with the mtu applied to both the tun and the
// local user nat. for an in-process bridge the mtu is not constrained by any
// network, so this measures the per-packet overhead of the pipeline.
// the maximum is 65535, the ipv4 total length limit.
// production packets must stay at `DefaultMtu` since they are written
// directly into the receiver tap/tun interface. the large mtu benchmark
// variants are diagnostics only.
func newBenchmarkEgressBridgeWithMtu(b *testing.B, ctx context.Context, mtu int) (*Tun, func()) {
	return newBenchmarkEgressBridgeWithMtuAndBatchSize(b, ctx, mtu, 64)
}

// Builds the same bridge while varying only the TUN and per-flow ready-drain
// caps. Queue capacities and flow windows retain their production defaults.
func newBenchmarkEgressBridgeWithMtuAndBatchSize(
	b *testing.B,
	ctx context.Context,
	mtu int,
	maximumPacketCount int,
) (*Tun, func()) {
	tunSettings := DefaultTunSettingsWithBufferSize(2048)
	tunSettings.Mtu = mtu
	tun, err := CreateTun(ctx, tunSettings)
	if err != nil {
		b.Fatal(err)
	}
	localUserNatSettings := DefaultLocalUserNatSettings()
	localUserNatSettings.TcpBufferSettings.Mtu = mtu
	localUserNatSettings.UdpBufferSettings.Mtu = mtu
	localUserNatSettings.TcpBufferSettings.WriteBatchSize = maximumPacketCount
	localUserNatSettings.UdpBufferSettings.WriteBatchSize = maximumPacketCount
	localUserNat := NewLocalUserNat(ctx, "benchEgress", localUserNatSettings)
	removeReceiveCallback := bridgeTunToLocalUserNatWithBatchSize(
		tun,
		localUserNat,
		SourceId(NewId()),
		maximumPacketCount,
	)
	closeBridge := func() {
		removeReceiveCallback()
		localUserNat.Close()
		tun.Close()
	}
	return tun, closeBridge
}

// measures bulk upload throughput, source -> egress socket.
// this exercises the tcp send path: sequence buffering, the write pipeline,
// window adjustment, and pure acks back to the source.
func BenchmarkIpEgressTcp4Up(b *testing.B) {
	benchmarkIpEgressTcp4Up(b, DefaultMtu, 64)
}

// Measures the shared TUN/NAT upload path with an eight-packet ready drain.
func BenchmarkIpEgressTcp4UpBatch8(b *testing.B) {
	benchmarkIpEgressTcp4Up(b, DefaultMtu, 8)
}

// Measures the shared TUN/NAT upload path with its current ready drain.
func BenchmarkIpEgressTcp4UpBatch64(b *testing.B) {
	benchmarkIpEgressTcp4Up(b, DefaultMtu, 64)
}

// `BenchmarkIpEgressTcp4Up` at the maximum mtu.
// the gap to the default mtu run is the per-packet overhead of the pipeline.
func BenchmarkIpEgressTcp4UpMtu64k(b *testing.B) {
	benchmarkIpEgressTcp4Up(b, 65535, 64)
}

func benchmarkIpEgressTcp4Up(b *testing.B, mtu int, maximumPacketCount int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// sink server: reads an 8 byte total, sinks total bytes, then acks with one byte
	sinkListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer sinkListener.Close()
	go HandleError(func() {
		for {
			conn, err := sinkListener.Accept()
			if err != nil {
				return
			}
			go HandleError(func() {
				defer conn.Close()
				header := make([]byte, 8)
				if _, err := io.ReadFull(conn, header); err != nil {
					return
				}
				total := int64(binary.BigEndian.Uint64(header))
				if _, err := io.CopyN(io.Discard, conn, total); err != nil {
					return
				}
				conn.Write([]byte{1})
			})
		}
	})

	tun, closeBridge := newBenchmarkEgressBridgeWithMtuAndBatchSize(
		b,
		ctx,
		mtu,
		maximumPacketCount,
	)
	defer closeBridge()

	conn, err := tun.DialContext(ctx, "tcp", sinkListener.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	chunk := testingEgressPayload(1, 32*1024)
	header := make([]byte, 8)
	binary.BigEndian.PutUint64(header, uint64(len(chunk))*uint64(b.N))

	b.SetBytes(int64(len(chunk)))
	b.ResetTimer()

	if _, err := conn.Write(header); err != nil {
		b.Fatal(err)
	}
	for i := 0; i < b.N; i += 1 {
		if _, err := conn.Write(chunk); err != nil {
			b.Fatal(err)
		}
	}
	// the ack means all bytes reached the egress socket
	conn.SetReadDeadline(time.Now().Add(300 * time.Second))
	if _, err := io.ReadFull(conn, make([]byte, 1)); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

// measures bulk download throughput, egress socket -> source.
// this exercises the tcp receive path: socket reads, data packet
// serialization, and window waits against the source acks.
func BenchmarkIpEgressTcp4Down(b *testing.B) {
	benchmarkIpEgressTcp4Down(b, DefaultMtu, 64)
}

// Measures the shared TUN/NAT download path with an eight-packet ready drain.
func BenchmarkIpEgressTcp4DownBatch8(b *testing.B) {
	benchmarkIpEgressTcp4Down(b, DefaultMtu, 8)
}

// Measures the shared TUN/NAT download path with its current ready drain.
func BenchmarkIpEgressTcp4DownBatch64(b *testing.B) {
	benchmarkIpEgressTcp4Down(b, DefaultMtu, 64)
}

// `BenchmarkIpEgressTcp4Down` at the maximum mtu.
// the gap to the default mtu run is the per-packet overhead of the pipeline.
func BenchmarkIpEgressTcp4DownMtu64k(b *testing.B) {
	benchmarkIpEgressTcp4Down(b, 65535, 64)
}

func benchmarkIpEgressTcp4Down(b *testing.B, mtu int, maximumPacketCount int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	chunkSize := 32 * 1024

	// source server: reads an 8 byte total then writes total bytes
	sourceListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer sourceListener.Close()
	go HandleError(func() {
		for {
			conn, err := sourceListener.Accept()
			if err != nil {
				return
			}
			go HandleError(func() {
				defer conn.Close()
				header := make([]byte, 8)
				if _, err := io.ReadFull(conn, header); err != nil {
					return
				}
				total := int64(binary.BigEndian.Uint64(header))
				chunk := testingEgressPayload(2, chunkSize)
				for written := int64(0); written < total; {
					n := min(int64(len(chunk)), total-written)
					if _, err := conn.Write(chunk[0:n]); err != nil {
						return
					}
					written += n
				}
			})
		}
	})

	tun, closeBridge := newBenchmarkEgressBridgeWithMtuAndBatchSize(
		b,
		ctx,
		mtu,
		maximumPacketCount,
	)
	defer closeBridge()

	conn, err := tun.DialContext(ctx, "tcp", sourceListener.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	total := int64(chunkSize) * int64(b.N)
	header := make([]byte, 8)
	binary.BigEndian.PutUint64(header, uint64(total))

	b.SetBytes(int64(chunkSize))
	b.ResetTimer()

	if _, err := conn.Write(header); err != nil {
		b.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(300 * time.Second))
	if _, err := io.CopyN(io.Discard, conn, total); err != nil {
		b.Fatal(err)
	}
	b.StopTimer()
}

// measures udp round trips with a fixed window of datagrams in flight.
// the per-packet cost dominates, so this surfaces the per-packet overhead of
// the udp flow: decode, sequence dispatch, socket write, and the
// packet serialization on the return path.
func BenchmarkIpEgressUdp4RoundTrip(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// udp echo server on loopback
	echoConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer echoConn.Close()
	go HandleError(func() {
		buffer := make([]byte, 2048)
		for {
			n, addr, err := echoConn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			echoConn.WriteToUDP(buffer[0:n], addr)
		}
	})

	tun, closeBridge := newBenchmarkEgressBridge(b, ctx)
	defer closeBridge()

	conn, err := tun.DialContext(ctx, "udp", echoConn.LocalAddr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	payloadSize := 1200
	windowSize := 16
	payload := testingEgressPayload(3, payloadSize)
	echoPayload := make([]byte, payloadSize)

	// warm up the flow so the sequence setup and socket dial are not measured
	if _, err := conn.Write(payload); err != nil {
		b.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	if _, err := conn.Read(echoPayload); err != nil {
		b.Fatal(err)
	}

	b.SetBytes(int64(payloadSize))
	b.ResetTimer()

	// keep `windowSize` datagrams in flight. the window stays below all of
	// the buffer sizes so the flow is lossless by construction.
	sent := 0
	for ; sent < min(windowSize, b.N); sent += 1 {
		if _, err := conn.Write(payload); err != nil {
			b.Fatal(err)
		}
	}
	for received := 0; received < b.N; received += 1 {
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		if _, err := conn.Read(echoPayload); err != nil {
			b.Fatal(err)
		}
		if sent < b.N {
			if _, err := conn.Write(payload); err != nil {
				b.Fatal(err)
			}
			sent += 1
		}
	}
	b.StopTimer()
}

// measures bulk udp upload throughput, source -> egress socket.
// the sink acks a cumulative count every `creditCount` datagrams, and the
// source keeps at most `windowSize` datagrams unacked. the window stays below
// all of the buffer sizes so the flow is lossless by construction.
func BenchmarkIpEgressUdp4Up(b *testing.B) {
	benchmarkIpEgressUdp4Up(b, 64)
}

// Measures the shared TUN/NAT UDP upload path with an eight-packet drain.
func BenchmarkIpEgressUdp4UpBatch8(b *testing.B) {
	benchmarkIpEgressUdp4Up(b, 8)
}

// Measures the shared TUN/NAT UDP upload path with its current drain.
func BenchmarkIpEgressUdp4UpBatch64(b *testing.B) {
	benchmarkIpEgressUdp4Up(b, 64)
}

func benchmarkIpEgressUdp4Up(b *testing.B, maximumPacketCount int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	payloadSize := 1200
	windowSize := 256
	creditCount := 32

	// sink server: counts data datagrams and acks the cumulative count.
	// a 1 byte datagram requests an immediate ack.
	sinkConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer sinkConn.Close()
	sinkConn.SetReadBuffer(4 * 1024 * 1024)
	go HandleError(func() {
		buffer := make([]byte, 2048)
		ackBuffer := make([]byte, 8)
		receivedCount := uint64(0)
		for {
			n, addr, err := sinkConn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			if 1 < n {
				receivedCount += 1
				if receivedCount%uint64(creditCount) != 0 {
					continue
				}
			}
			binary.BigEndian.PutUint64(ackBuffer, receivedCount)
			sinkConn.WriteToUDP(ackBuffer, addr)
		}
	})

	tun, closeBridge := newBenchmarkEgressBridgeWithMtuAndBatchSize(
		b,
		ctx,
		DefaultMtu,
		maximumPacketCount,
	)
	defer closeBridge()

	conn, err := tun.DialContext(ctx, "udp", sinkConn.LocalAddr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	payload := testingEgressPayload(4, payloadSize)
	flush := []byte{0}
	ackBuffer := make([]byte, 8)

	readAck := func() uint64 {
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := conn.Read(ackBuffer)
		if err != nil || n != 8 {
			b.Fatalf("ack read n=%d err=%v", n, err)
		}
		return binary.BigEndian.Uint64(ackBuffer)
	}

	// warm up the flow so the sequence setup and socket dial are not measured
	if _, err := conn.Write(flush); err != nil {
		b.Fatal(err)
	}
	readAck()

	b.SetBytes(int64(payloadSize))
	b.ResetTimer()

	totalCount := uint64(b.N)
	sentCount := uint64(0)
	ackedCount := uint64(0)
	for ackedCount < totalCount {
		if sentCount < totalCount && sentCount-ackedCount < uint64(windowSize) {
			if _, err := conn.Write(payload); err != nil {
				b.Fatal(err)
			}
			sentCount += 1
		} else if sentCount < totalCount {
			ackedCount = readAck()
		} else {
			// all sent. flush until the sink has counted everything.
			if _, err := conn.Write(flush); err != nil {
				b.Fatal(err)
			}
			ackedCount = readAck()
		}
	}
	b.StopTimer()
}

// measures bulk udp download throughput, egress socket -> source.
// the source acks a cumulative count every `creditCount` datagrams, and the
// server keeps at most `windowSize` datagrams unacked. the window stays below
// all of the buffer sizes so the flow is lossless by construction.
func BenchmarkIpEgressUdp4Down(b *testing.B) {
	benchmarkIpEgressUdp4Down(b, 64)
}

// Measures the shared TUN/NAT UDP download path with an eight-packet drain.
func BenchmarkIpEgressUdp4DownBatch8(b *testing.B) {
	benchmarkIpEgressUdp4Down(b, 8)
}

// Measures the shared TUN/NAT UDP download path with its current drain.
func BenchmarkIpEgressUdp4DownBatch64(b *testing.B) {
	benchmarkIpEgressUdp4Down(b, 64)
}

func benchmarkIpEgressUdp4Down(b *testing.B, maximumPacketCount int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	payloadSize := 1200
	windowSize := 256
	creditCount := 32

	// source server: a 9 byte datagram requests the encoded count of
	// datagrams. an 8 byte datagram is a cumulative receive ack.
	sourceConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer sourceConn.Close()
	go HandleError(func() {
		buffer := make([]byte, 2048)
		payload := testingEgressPayload(5, payloadSize)
		for {
			sourceConn.SetReadDeadline(time.Now().Add(60 * time.Second))
			n, addr, err := sourceConn.ReadFromUDP(buffer)
			if err != nil {
				return
			}
			if n != 9 {
				continue
			}
			requestCount := binary.BigEndian.Uint64(buffer[1:9])
			sentCount := uint64(0)
			ackedCount := uint64(0)
			for sentCount < requestCount {
				if sentCount-ackedCount < uint64(windowSize) {
					if _, err := sourceConn.WriteToUDP(payload, addr); err != nil {
						return
					}
					sentCount += 1
				} else {
					sourceConn.SetReadDeadline(time.Now().Add(30 * time.Second))
					n, _, err := sourceConn.ReadFromUDP(buffer)
					if err != nil {
						return
					}
					if n == 8 {
						ackedCount = binary.BigEndian.Uint64(buffer[0:8])
					}
				}
			}
		}
	})

	tun, closeBridge := newBenchmarkEgressBridgeWithMtuAndBatchSize(
		b,
		ctx,
		DefaultMtu,
		maximumPacketCount,
	)
	defer closeBridge()

	conn, err := tun.DialContext(ctx, "udp", sourceConn.LocalAddr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	b.SetBytes(int64(payloadSize))
	b.ResetTimer()

	totalCount := uint64(b.N)
	start := make([]byte, 9)
	start[0] = 1
	binary.BigEndian.PutUint64(start[1:9], totalCount)
	if _, err := conn.Write(start); err != nil {
		b.Fatal(err)
	}

	buffer := make([]byte, 2048)
	ackBuffer := make([]byte, 8)
	for receivedCount := uint64(0); receivedCount < totalCount; {
		conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		n, err := conn.Read(buffer)
		if err != nil {
			b.Fatal(err)
		}
		if n != payloadSize {
			b.Fatalf("datagram size %d", n)
		}
		receivedCount += 1
		if receivedCount%uint64(creditCount) == 0 || receivedCount == totalCount {
			binary.BigEndian.PutUint64(ackBuffer, receivedCount)
			if _, err := conn.Write(ackBuffer); err != nil {
				b.Fatal(err)
			}
		}
	}
	b.StopTimer()
}

// verifies the hand-rolled packet writers byte for byte against a
// gopacket reference, including checksums
func TestIpPacketWriters(t *testing.T) {
	payloadSizes := []int{0, 1, 2, 3, 64, 1379, 1380}

	for _, ipVersion := range []int{4, 6} {
		var sourceIp net.IP
		var destinationIp net.IP
		switch ipVersion {
		case 4:
			sourceIp = net.IP{72, 0, 0, 1}
			destinationIp = net.IP{72, 2, 3, 4}
		case 6:
			sourceIp = net.ParseIP("2001:db8::1").To16()
			destinationIp = net.ParseIP("2001:db8::2").To16()
		}

		serializeReference := func(ipProtocol layers.IPProtocol, transport gopacket.SerializableLayer, payload []byte) []byte {
			var ip gopacket.NetworkLayer
			switch ipVersion {
			case 4:
				ip = &layers.IPv4{
					Version:  4,
					TTL:      64,
					SrcIP:    destinationIp,
					DstIP:    sourceIp,
					Protocol: ipProtocol,
				}
			case 6:
				ip = &layers.IPv6{
					Version:    6,
					HopLimit:   64,
					SrcIP:      destinationIp,
					DstIP:      sourceIp,
					NextHeader: ipProtocol,
				}
			}
			switch v := transport.(type) {
			case *layers.TCP:
				v.SetNetworkLayerForChecksum(ip)
			case *layers.UDP:
				v.SetNetworkLayerForChecksum(ip)
			}
			buffer := gopacket.NewSerializeBuffer()
			err := gopacket.SerializeLayers(buffer, gopacket.SerializeOptions{ComputeChecksums: true, FixLengths: true},
				ip.(gopacket.SerializableLayer),
				transport,
				gopacket.Payload(payload),
			)
			AssertEqual(t, err, nil)
			reference := make([]byte, len(buffer.Bytes()))
			copy(reference, buffer.Bytes())
			return reference
		}

		connectionState := &ConnectionState{
			ipVersion:       ipVersion,
			sourceIp:        sourceIp,
			sourcePort:      40000,
			destinationIp:   destinationIp,
			destinationPort: 443,
			receiveSeq:      0x01020304,
			sendSeq:         0x0a0b0c0d,
			windowSize:      262144,
			windowScale:     3,
		}

		for _, payloadSize := range payloadSizes {
			payload := testingEgressPayload(9, payloadSize)
			for _, flags := range []byte{tcpFlagAck, tcpFlagAck | tcpFlagFin, tcpFlagAck | tcpFlagRst} {
				packet := connectionState.tcpPacket(flags, connectionState.receiveSeq, payload)
				reference := serializeReference(layers.IPProtocolTCP, &layers.TCP{
					SrcPort: layers.TCPPort(connectionState.destinationPort),
					DstPort: layers.TCPPort(connectionState.sourcePort),
					Seq:     connectionState.receiveSeq,
					Ack:     connectionState.sendSeq,
					FIN:     (flags & tcpFlagFin) != 0,
					RST:     (flags & tcpFlagRst) != 0,
					ACK:     true,
					Window:  connectionState.encodedWindowSize(),
				}, payload)
				AssertEqual(t, bytes.Equal(reference, packet), true)
				MessagePoolReturn(packet)
			}
		}

		streamState := &StreamState{
			ipVersion:       ipVersion,
			sourceIp:        sourceIp,
			sourcePort:      40000,
			destinationIp:   destinationIp,
			destinationPort: 53,
		}

		for _, payloadSize := range payloadSizes {
			payload := testingEgressPayload(11, payloadSize)
			packet := streamState.udpPacket(payload)
			reference := serializeReference(layers.IPProtocolUDP, &layers.UDP{
				SrcPort: layers.UDPPort(streamState.destinationPort),
				DstPort: layers.UDPPort(streamState.sourcePort),
			}, payload)
			AssertEqual(t, bytes.Equal(reference, packet), true)
			MessagePoolReturn(packet)
		}
	}
}

// measures aggregate bulk upload throughput with parallel tcp connections.
// this exercises the send dispatch path under multi-flow load.
func BenchmarkIpEgressTcp4UpParallel(b *testing.B) {
	benchmarkIpEgressTcp4UpParallel(b, 0)
}

// `BenchmarkIpEgressTcp4UpParallel` with eight send shards, for comparison
func BenchmarkIpEgressTcp4UpParallelShard8(b *testing.B) {
	benchmarkIpEgressTcp4UpParallel(b, 8)
}

func benchmarkIpEgressTcp4UpParallel(b *testing.B, sendShardCount int) {
	parallelCount := 8

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// sink server: reads an 8 byte total, sinks total bytes, then acks with one byte
	sinkListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer sinkListener.Close()
	go HandleError(func() {
		for {
			conn, err := sinkListener.Accept()
			if err != nil {
				return
			}
			go HandleError(func() {
				defer conn.Close()
				header := make([]byte, 8)
				if _, err := io.ReadFull(conn, header); err != nil {
					return
				}
				total := int64(binary.BigEndian.Uint64(header))
				if _, err := io.CopyN(io.Discard, conn, total); err != nil {
					return
				}
				conn.Write([]byte{1})
			})
		}
	})

	tun, closeBridge := newBenchmarkEgressBridgeWithShardCount(b, ctx, sendShardCount)
	defer closeBridge()

	chunk := testingEgressPayload(1, 32*1024)

	conns := make([]net.Conn, parallelCount)
	for p := range conns {
		conn, err := tun.DialContext(ctx, "tcp", sinkListener.Addr().String())
		if err != nil {
			b.Fatal(err)
		}
		defer conn.Close()
		conns[p] = conn
	}

	b.SetBytes(int64(len(chunk)))
	b.ResetTimer()

	flowErrs := make(chan error, parallelCount)
	for p := 0; p < parallelCount; p += 1 {
		chunkCount := b.N / parallelCount
		if p == 0 {
			chunkCount += b.N % parallelCount
		}
		conn := conns[p]
		go HandleError(func() {
			flowErrs <- func() error {
				header := make([]byte, 8)
				binary.BigEndian.PutUint64(header, uint64(len(chunk))*uint64(chunkCount))
				if _, err := conn.Write(header); err != nil {
					return err
				}
				for i := 0; i < chunkCount; i += 1 {
					if _, err := conn.Write(chunk); err != nil {
						return err
					}
				}
				// the ack means all bytes reached the egress socket
				conn.SetReadDeadline(time.Now().Add(300 * time.Second))
				if _, err := io.ReadFull(conn, make([]byte, 1)); err != nil {
					return err
				}
				return nil
			}()
		})
	}
	for p := 0; p < parallelCount; p += 1 {
		if err := <-flowErrs; err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()

	stats := tun.Stats()
	b.Logf(
		"ip outgoing errors=%d tcp retransmits=%d tcp timeouts=%d tcp fast retransmits=%d",
		stats.IP.OutgoingPacketErrors.Value(),
		stats.TCP.Retransmits.Value(),
		stats.TCP.Timeouts.Value(),
		stats.TCP.FastRetransmit.Value(),
	)
}

// measures aggregate bulk udp upload throughput with parallel flows.
// each flow uses the same credit scheme as the single flow benchmark.
func BenchmarkIpEgressUdp4UpParallel(b *testing.B) {
	benchmarkIpEgressUdp4UpParallel(b, 0)
}

// `BenchmarkIpEgressUdp4UpParallel` with eight send shards, for comparison
func BenchmarkIpEgressUdp4UpParallelShard8(b *testing.B) {
	benchmarkIpEgressUdp4UpParallel(b, 8)
}

func benchmarkIpEgressUdp4UpParallel(b *testing.B, sendShardCount int) {
	parallelCount := 8
	payloadSize := 1200
	windowSize := 64
	creditCount := 16

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// sink server: counts data datagrams per flow and acks cumulative counts.
	// a 1 byte datagram requests an immediate ack.
	sinkConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 0})
	if err != nil {
		b.Fatal(err)
	}
	defer sinkConn.Close()
	sinkConn.SetReadBuffer(4 * 1024 * 1024)
	go HandleError(func() {
		buffer := make([]byte, 2048)
		ackBuffer := make([]byte, 8)
		receivedCounts := map[netip.AddrPort]uint64{}
		for {
			n, addrPort, err := sinkConn.ReadFromUDPAddrPort(buffer)
			if err != nil {
				return
			}
			if 1 < n {
				receivedCounts[addrPort] += 1
				if receivedCounts[addrPort]%uint64(creditCount) != 0 {
					continue
				}
			}
			binary.BigEndian.PutUint64(ackBuffer, receivedCounts[addrPort])
			sinkConn.WriteToUDPAddrPort(ackBuffer, addrPort)
		}
	})

	tun, closeBridge := newBenchmarkEgressBridgeWithShardCount(b, ctx, sendShardCount)
	defer closeBridge()

	payload := testingEgressPayload(4, payloadSize)
	flush := []byte{0}

	conns := make([]net.Conn, parallelCount)
	for p := range conns {
		conn, err := tun.DialContext(ctx, "udp", sinkConn.LocalAddr().String())
		if err != nil {
			b.Fatal(err)
		}
		defer conn.Close()
		conns[p] = conn
	}

	b.SetBytes(int64(payloadSize))
	b.ResetTimer()

	flowErrs := make(chan error, parallelCount)
	for p := 0; p < parallelCount; p += 1 {
		flowCount := uint64(b.N / parallelCount)
		if p == 0 {
			flowCount += uint64(b.N % parallelCount)
		}
		conn := conns[p]
		go HandleError(func() {
			flowErrs <- func() error {
				ackBuffer := make([]byte, 8)
				readAck := func() (uint64, error) {
					conn.SetReadDeadline(time.Now().Add(30 * time.Second))
					n, err := conn.Read(ackBuffer)
					if err != nil || n != 8 {
						return 0, fmt.Errorf("ack read n=%d err=%v", n, err)
					}
					return binary.BigEndian.Uint64(ackBuffer), nil
				}

				sentCount := uint64(0)
				ackedCount := uint64(0)
				for ackedCount < flowCount {
					if sentCount < flowCount && sentCount-ackedCount < uint64(windowSize) {
						if _, err := conn.Write(payload); err != nil {
							return err
						}
						sentCount += 1
					} else if sentCount < flowCount {
						count, err := readAck()
						if err != nil {
							return err
						}
						ackedCount = count
					} else {
						// all sent. flush until the sink has counted everything.
						if _, err := conn.Write(flush); err != nil {
							return err
						}
						count, err := readAck()
						if err != nil {
							return err
						}
						ackedCount = count
					}
				}
				return nil
			}()
		})
	}
	for p := 0; p < parallelCount; p += 1 {
		if err := <-flowErrs; err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}

// pins the ipv4 fragment guard: fragments are not reassembled, so a packet
// with mf set or a nonzero fragment offset must fail to parse rather than
// misparse payload bytes as transport fields. df alone must still parse.
func TestParseIpPathIpv4FragmentDrop(t *testing.T) {
	cases := []struct {
		name       string
		flagsHigh  byte
		offsetLow  byte
		expectDrop bool
	}{
		{name: "no flags", flagsHigh: 0x00, offsetLow: 0x00, expectDrop: false},
		{name: "df", flagsHigh: 0x40, offsetLow: 0x00, expectDrop: false},
		{name: "mf first fragment", flagsHigh: 0x20, offsetLow: 0x00, expectDrop: true},
		{name: "offset low bits", flagsHigh: 0x00, offsetLow: 0x01, expectDrop: true},
		{name: "offset high bits", flagsHigh: 0x1f, offsetLow: 0x00, expectDrop: true},
		{name: "df with offset", flagsHigh: 0x41, offsetLow: 0x00, expectDrop: true},
	}
	for _, c := range cases {
		packet := testingUdp4Packet("10.0.0.1", "203.0.113.7", 4443, []byte("payload"))
		// the parser does not validate the header checksum, so the flag and
		// offset bytes can be set directly
		packet[6] = c.flagsHigh
		packet[7] = c.offsetLow
		_, err := ParseIpPath(packet)
		if c.expectDrop != (err != nil) {
			t.Errorf("%s: expectDrop=%v err=%v", c.name, c.expectDrop, err)
		}
	}
}
