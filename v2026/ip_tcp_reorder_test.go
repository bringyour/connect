// Deterministic user-NAT tests pin route-crossover TCP reassembly, recovery,
// sequence arithmetic, and pool ownership against an in-memory upstream.
package connect

import (
	"context"
	"errors"
	"io"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Runs one TCP user-NAT sequence against an in-memory upstream socket.
type tcpReorderTestHarness struct {
	t                *testing.T
	cancel           context.CancelFunc
	sequence         *TcpSequence
	upstreamSocket   net.Conn
	source           TransferPath
	nextSeq          uint32
	synAckReceived   chan struct{}
	ackNumbers       chan uint32
	acks             chan parsedTcp
	reorderDecisions chan tcpReorderDisposition
	runDone          chan struct{}
	closeOnce        sync.Once
}

// Establishes one deterministic user-NAT connection.
func newTcpReorderTestHarness(
	t *testing.T,
	initialSynSeq uint32,
	sequenceBufferSize int,
	reorderByteCount int,
) *tcpReorderTestHarness {
	return newTcpReorderTestHarnessWithSetup(
		t,
		initialSynSeq,
		sequenceBufferSize,
		reorderByteCount,
		nil,
	)
}

// Establishes one deterministic user-NAT connection after applying any test
// hooks before the sequence worker starts.
func newTcpReorderTestHarnessWithSetup(
	t *testing.T,
	initialSynSeq uint32,
	sequenceBufferSize int,
	reorderByteCount int,
	setupSequence func(*TcpSequence),
) *tcpReorderTestHarness {
	return newTcpReorderTestHarnessWithSynOptions(
		t,
		initialSynSeq,
		sequenceBufferSize,
		reorderByteCount,
		setupSequence,
		nil,
	)
}

// Establishes the same harness with explicit source SYN options.
func newTcpReorderTestHarnessWithSynOptions(
	t *testing.T,
	initialSynSeq uint32,
	sequenceBufferSize int,
	reorderByteCount int,
	setupSequence func(*TcpSequence),
	synOptions []byte,
) *tcpReorderTestHarness {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	sequenceSocket, upstreamSocket := net.Pipe()
	settings := DefaultTcpBufferSettingsWithBufferSize(sequenceBufferSize)
	settings.ReadTimeout = 5 * time.Second
	settings.WriteTimeout = 5 * time.Second
	settings.IdleTimeout = 5 * time.Second
	settings.AckCompressTimeout = 0
	settings.WriteBatchSize = 1
	if 0 < reorderByteCount {
		settings.MaxWindowSize = uint32(reorderByteCount)
		settings.Mtu = reorderByteCount
	}
	settings.DialContextSettings = &DialContextSettings{
		DialContext: func(dialCtx context.Context, network string, addr string) (net.Conn, error) {
			return sequenceSocket, nil
		},
	}

	harness := &tcpReorderTestHarness{
		t:                t,
		cancel:           cancel,
		upstreamSocket:   upstreamSocket,
		source:           SourceId(NewId()),
		nextSeq:          initialSynSeq + 1,
		synAckReceived:   make(chan struct{}, 1),
		ackNumbers:       make(chan uint32, 64),
		acks:             make(chan parsedTcp, 64),
		reorderDecisions: make(chan tcpReorderDisposition, 64),
		runDone:          make(chan struct{}),
	}
	sourceIp := net.IPv4(10, 0, 0, 1).To4()
	destinationIp := net.IPv4(203, 0, 113, 7).To4()
	harness.sequence = NewTcpSequence(
		ctx,
		func(
			source TransferPath,
			provideMode protocol.ProvideMode,
			ipPath *IpPath,
			packet []byte,
		) {
			_, packetSourceIp, packetDestinationIp, transport, ok := parseIpv4(packet)
			if !ok {
				return
			}
			tcp := &parsedTcp{}
			if !parseTcpPacket(packetSourceIp, packetDestinationIp, transport, tcp) {
				return
			}
			if tcp.syn {
				select {
				case harness.synAckReceived <- struct{}{}:
				default:
				}
				return
			}
			if tcp.ack {
				select {
				case harness.acks <- *tcp:
				default:
				}
				select {
				case harness.ackNumbers <- tcp.ackNumber:
				default:
				}
			}
		},
		harness.source,
		protocol.ProvideMode_Network,
		4,
		sourceIp,
		40001,
		destinationIp,
		443,
		initialSynSeq,
		settings,
	)
	harness.sequence.afterReorderDispositionForTest = func(disposition tcpReorderDisposition) {
		select {
		case harness.reorderDecisions <- disposition:
		default:
		}
	}
	if setupSequence != nil {
		setupSequence(harness.sequence)
	}
	go func() {
		defer close(harness.runDone)
		harness.sequence.Run()
	}()

	synItem := harness.newSendItem(initialSynSeq, nil, false)
	synItem.tcp.syn = true
	synItem.tcp.ack = false
	synItem.tcp.options = synOptions
	harness.sendItem(synItem)
	select {
	case <-harness.synAckReceived:
	case <-time.After(2 * time.Second):
		harness.close()
		t.Fatal("TCP reorder harness did not establish")
	}

	t.Cleanup(harness.close)
	return harness
}

// Waits for one exact crossover decision from the sequence worker.
func (self *tcpReorderTestHarness) waitReorderDecision(want tcpReorderDisposition) {
	self.t.Helper()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	select {
	case got := <-self.reorderDecisions:
		if got != want {
			self.t.Fatalf("TCP reorder disposition=%d, want %d", got, want)
		}
	case <-timer.C:
		self.t.Fatalf("TCP reorder disposition %d was not observed", want)
	}
}

// Builds one pool-owned IPv4/TCP item whose payload aliases its packet exactly
// as it does on the production parser path.
func (self *tcpReorderTestHarness) newSendItem(seq uint32, payload []byte, fin bool) *TcpSendItem {
	self.t.Helper()

	headerByteCount := Ipv4HeaderSizeWithoutExtensions + TcpHeaderSizeWithoutExtensions
	packet := MessagePoolGet(headerByteCount + len(payload))
	clear(packet)
	packet[0] = 0x45
	copy(packet[headerByteCount:], payload)
	return &TcpSendItem{
		source:      self.source,
		provideMode: protocol.ProvideMode_Network,
		tcp: parsedTcp{
			seq:        seq,
			ack:        true,
			ackNumber:  self.nextSeq,
			windowSize: 65535,
			fin:        fin,
			payload:    packet[headerByteCount:],
		},
		ipPacket: packet,
	}
}

// Transfers one test packet's ownership to the sequence.
func (self *tcpReorderTestHarness) sendItem(sendItem *TcpSendItem) {
	self.t.Helper()

	success, err := self.sequence.send(sendItem, -1)
	if err != nil {
		MessagePoolReturn(sendItem.ipPacket)
		self.t.Fatalf("send TCP item: %v", err)
	}
	if !success {
		MessagePoolReturn(sendItem.ipPacket)
		self.t.Fatal("TCP item was not accepted")
	}
}

// Transfers one ordinary payload or payload-plus-FIN item.
func (self *tcpReorderTestHarness) sendPayload(seq uint32, payload string, fin bool) *TcpSendItem {
	self.t.Helper()

	sendItem := self.newSendItem(seq, []byte(payload), fin)
	self.sendItem(sendItem)
	return sendItem
}

// Waits until the source-facing callback observes one expected ACK.
func (self *tcpReorderTestHarness) waitAck(ackNumber uint32) {
	self.t.Helper()

	timeout := time.After(2 * time.Second)
	for {
		select {
		case receivedAckNumber := <-self.ackNumbers:
			if receivedAckNumber == ackNumber {
				return
			}
		case <-timeout:
			self.t.Fatalf("TCP ACK %d was not observed", ackNumber)
		}
	}
}

// Reads and verifies one exact byte sequence from the upstream.
func (self *tcpReorderTestHarness) readPayload(want string) {
	self.t.Helper()

	if err := self.upstreamSocket.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		self.t.Fatalf("set upstream read deadline: %v", err)
	}
	payload := make([]byte, len(want))
	_, err := io.ReadFull(self.upstreamSocket, payload)
	self.upstreamSocket.SetReadDeadline(time.Time{})
	if err != nil {
		self.t.Fatalf("read upstream payload %q: %v", want, err)
	}
	if string(payload) != want {
		self.t.Fatalf("upstream payload=%q, want %q", payload, want)
	}
}

// Verifies an in-order FIN closes the upstream socket.
func (self *tcpReorderTestHarness) requireUpstreamEof() {
	self.t.Helper()

	// The sequence closes its net.Pipe endpoint as soon as it forwards FIN.
	// net.Pipe rejects deadline changes once either endpoint is closed, which is
	// already the condition this helper is about to require.
	if err := self.upstreamSocket.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil && !errors.Is(err, io.ErrClosedPipe) {
		self.t.Fatalf("set upstream FIN deadline: %v", err)
	}
	buffer := make([]byte, 1)
	n, err := self.upstreamSocket.Read(buffer)
	self.upstreamSocket.SetReadDeadline(time.Time{})
	if n != 0 || err != io.EOF {
		self.t.Fatalf("upstream FIN read=(%d, %v), want (0, EOF)", n, err)
	}
}

// The FIN assertion is independent of whether the sequence closes immediately
// before or immediately after the test installs its read deadline.
func TestTcpReorderHarnessAcceptsPeerCloseBeforeEofDeadline(t *testing.T) {
	sequenceSocket, upstreamSocket := net.Pipe()
	if err := sequenceSocket.Close(); err != nil {
		t.Fatal(err)
	}
	defer upstreamSocket.Close()
	(&tcpReorderTestHarness{t: t, upstreamSocket: upstreamSocket}).requireUpstreamEof()
}

// Tears down the sequence and waits until every retained item is owned by
// neither the sequence nor its socket writer.
func (self *tcpReorderTestHarness) close() {
	self.closeOnce.Do(func() {
		self.sequence.Cancel()
		self.cancel()
		self.upstreamSocket.Close()
		select {
		case <-self.runDone:
		case <-time.After(2 * time.Second):
			self.t.Errorf("TCP reorder harness did not stop")
		}
	})
}

// Proves a new-path segment may overtake an old-path segment without losing
// either upstream payload.
func TestTcpSequenceDrainsPlatformP2pCrossover(t *testing.T) {
	const initialSynSeq = uint32(1000)
	harness := newTcpReorderTestHarness(t, initialSynSeq, 8, 0)

	harness.sendPayload(harness.nextSeq+5, "world", false)
	harness.waitAck(harness.nextSeq)
	harness.sendPayload(harness.nextSeq, "hello", false)
	harness.readPayload("helloworld")
}

// Proves contained, leading-overlap, and wholly stale retransmissions never
// duplicate bytes.
func TestTcpSequenceReorderHandlesDuplicatesAndOverlaps(t *testing.T) {
	const initialSynSeq = uint32(2000)
	harness := newTcpReorderTestHarness(t, initialSynSeq, 8, 0)

	harness.sendPayload(harness.nextSeq+4, "EFGHIJ", false)
	harness.waitAck(harness.nextSeq)
	harness.waitReorderDecision(tcpReorderDispositionRetained)
	harness.sendPayload(harness.nextSeq+6, "GHIJ", false)
	harness.waitAck(harness.nextSeq)
	harness.waitReorderDecision(tcpReorderDispositionRejected)
	harness.sendPayload(harness.nextSeq, "ABCD", false)
	harness.readPayload("ABCDEFGHIJ")

	harness.sendPayload(harness.nextSeq+8, "IJKLM", false)
	harness.readPayload("KLM")
	harness.sendPayload(harness.nextSeq+2, "CDE", false)
	harness.waitAck(harness.nextSeq + 13)
	harness.waitReorderDecision(tcpReorderDispositionStale)
}

// Verifies payload and FIN stay ordered when their segment arrives on the
// promoted route first.
func TestTcpSequenceReorderDefersFinUntilGap(t *testing.T) {
	const initialSynSeq = uint32(3000)
	harness := newTcpReorderTestHarness(t, initialSynSeq, 8, 0)

	harness.sendPayload(harness.nextSeq+4, "EF", true)
	harness.waitAck(harness.nextSeq)
	harness.waitReorderDecision(tcpReorderDispositionRetained)
	harness.sendPayload(harness.nextSeq, "ABCD", false)
	harness.readPayload("ABCDEF")
	harness.requireUpstreamEof()
}

// Proves a full retained window rejects excess ownership but advertises the
// gap and accepts retry.
func TestTcpSequenceReorderBoundRecoversByRetransmission(t *testing.T) {
	const initialSynSeq = uint32(4000)
	harness := newTcpReorderTestHarness(t, initialSynSeq, 2, 0)

	harness.sendPayload(harness.nextSeq+2, "C", false)
	harness.waitAck(harness.nextSeq)
	harness.sendPayload(harness.nextSeq+3, "D", false)
	harness.waitAck(harness.nextSeq)
	harness.sendPayload(harness.nextSeq+4, "E", false)
	harness.waitAck(harness.nextSeq)
	harness.sendPayload(harness.nextSeq, "AB", false)
	harness.readPayload("ABCD")
	harness.sendPayload(harness.nextSeq+4, "E", false)
	harness.readPayload("E")
}

// Proves the configured window byte limit rejects excess retained ownership
// independently of the item-count limit and accepts an in-order retry.
func TestTcpSequenceReorderByteBoundRecoversByRetransmission(t *testing.T) {
	const initialSynSeq = uint32(4500)
	harness := newTcpReorderTestHarness(t, initialSynSeq, 8, 80)

	harness.sendPayload(harness.nextSeq+2, "C", false)
	harness.waitAck(harness.nextSeq)
	harness.sendPayload(harness.nextSeq+3, "D", false)
	harness.waitAck(harness.nextSeq)
	harness.sendPayload(harness.nextSeq, "AB", false)
	harness.readPayload("ABC")
	harness.sendPayload(harness.nextSeq+3, "D", false)
	harness.readPayload("D")
}

// Verifies crossover arithmetic at the uint32 boundary follows TCP
// serial-number ordering.
func TestTcpSequenceReorderSupportsSequenceWrap(t *testing.T) {
	const initialSynSeq = uint32(math.MaxUint32 - 3)
	harness := newTcpReorderTestHarness(t, initialSynSeq, 8, 0)

	harness.sendPayload(harness.nextSeq+4, "EF", false)
	harness.waitAck(harness.nextSeq)
	harness.sendPayload(harness.nextSeq, "ABCD", false)
	harness.readPayload("ABCDEF")
	harness.waitAck(harness.nextSeq + 6)
}

// Verifies an out-of-order segment opens the return window immediately and its
// mutable test ACK is not read on drain.
func TestTcpSequenceReorderAppliesAckOnce(t *testing.T) {
	const initialSynSeq = uint32(5000)
	harness := newTcpReorderTestHarness(t, initialSynSeq, 8, 0)

	firstAckNumber := harness.nextSeq + 100
	harness.sequence.mutex.Lock()
	harness.sequence.receiveSeq = harness.nextSeq + 1000
	harness.sequence.receiveWindowEnd = harness.nextSeq
	harness.sequence.receiveWindowSize = 1
	harness.sequence.mutex.Unlock()

	retainedItem := harness.newSendItem(harness.nextSeq+4, []byte("E"), false)
	retainedItem.tcp.ackNumber = firstAckNumber
	retainedItem.tcp.windowSize = 4096
	harness.sendItem(retainedItem)
	harness.waitAck(harness.nextSeq)
	if !retainedItem.ackApplied {
		t.Fatal("out-of-order item did not apply its ACK on arrival")
	}
	harness.sequence.mutex.Lock()
	if harness.sequence.receiveSeqAck != firstAckNumber || harness.sequence.receiveWindowSize != 4096 {
		t.Fatalf(
			"immediate ACK state=(%d, %d), want (%d, 4096)",
			harness.sequence.receiveSeqAck,
			harness.sequence.receiveWindowSize,
			firstAckNumber,
		)
	}
	harness.sequence.mutex.Unlock()

	// The callback above synchronizes with retention. Changing only the ACK
	// fields makes a second application observable without changing payload.
	retainedItem.tcp.ackNumber = harness.nextSeq + 200
	retainedItem.tcp.windowSize = 8192
	gapItem := harness.newSendItem(harness.nextSeq, []byte("ABCD"), false)
	gapItem.tcp.ack = false
	harness.sendItem(gapItem)
	harness.readPayload("ABCDE")
	harness.waitAck(harness.nextSeq + 5)
	harness.sequence.mutex.Lock()
	defer harness.sequence.mutex.Unlock()
	if harness.sequence.receiveSeqAck != firstAckNumber || harness.sequence.receiveWindowSize != 4096 {
		t.Fatalf(
			"drained item reapplied ACK: state=(%d, %d), want (%d, 4096)",
			harness.sequence.receiveSeqAck,
			harness.sequence.receiveWindowSize,
			firstAckNumber,
		)
	}
}

// Pins retained, replacement, duplicate, bound-rejected, and cancellation
// pool ownership transitions.
func TestTcpSequenceReorderPacketOwnership(t *testing.T) {
	const initialSynSeq = uint32(6000)
	harness := newTcpReorderTestHarness(t, initialSynSeq, 2, 0)
	sendWithWitness := func(seq uint32, payload string) []byte {
		item := harness.newSendItem(seq, []byte(payload), false)
		witness := MessagePoolShareReadOnly(item.ipPacket)
		harness.sendItem(item)
		return witness
	}
	requireOwnerReturned := func(witness []byte, description string) {
		if !MessagePoolReturn(witness) {
			t.Fatalf("%s packet owner was not returned", description)
		}
	}

	coveredWitness := sendWithWitness(harness.nextSeq+5, "F")
	harness.waitAck(harness.nextSeq)

	// The wider item replaces and returns the retained item it covers.
	wideWitness := sendWithWitness(harness.nextSeq+4, "EF")
	harness.waitAck(harness.nextSeq)
	requireOwnerReturned(coveredWitness, "covered retained")
	// A contained duplicate is returned instead of consuming another slot.
	duplicateWitness := sendWithWitness(harness.nextSeq+5, "F")
	harness.waitAck(harness.nextSeq)
	requireOwnerReturned(duplicateWitness, "contained duplicate")

	secondRetainedWitness := sendWithWitness(harness.nextSeq+6, "G")
	harness.waitAck(harness.nextSeq)
	// The bounded third item is returned and relies on a source retry.
	boundRejectedWitness := sendWithWitness(harness.nextSeq+7, "H")
	harness.waitAck(harness.nextSeq)
	requireOwnerReturned(boundRejectedWitness, "bound-rejected")

	harness.close()
	requireOwnerReturned(wideWitness, "wide retained close")
	requireOwnerReturned(secondRetainedWitness, "second retained close")
}
