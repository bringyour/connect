package connect

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

func newEstablishedTcpAckFastPathSequence(t *testing.T) *TcpSequence {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	sequence := newTcpSequenceWithTransferKey(
		ctx,
		func(TransferPath, TransferKey, protocol.ProvideMode, receiveRecoveryMode, *IpPath, []byte) {},
		SourceId(NewId()),
		TransferKey{},
		protocol.ProvideMode_Stream,
		4,
		net.IPv4(10, 0, 0, 2).To4(),
		47001,
		net.IPv4(203, 0, 113, 10).To4(),
		443,
		99,
		DefaultTcpBufferSettings(),
	)
	sequence.receiveAckCondition()
	sequence.mutex.Lock()
	sequence.established = true
	sequence.receiveSeq = 500
	sequence.receiveSeqAck = 100
	sequence.receiveWindowSize = 1_000
	sequence.receiveWindowEnd = 1_100
	sequence.receiveWindowEndSet = true
	sequence.mutex.Unlock()
	return sequence
}

func TestEstablishedPureTcpAckBypassesSequenceQueueAndWakesWindow(t *testing.T) {
	sequence := newEstablishedTcpAckFastPathSequence(t)
	condition := sequence.receiveAckCondition()
	waiting := make(chan struct{})
	woke := make(chan struct{})
	go func() {
		sequence.mutex.Lock()
		close(waiting)
		condition.Wait()
		sequence.mutex.Unlock()
		close(woke)
	}()
	<-waiting

	baselineBytes := MessagePoolPacketOutstandingByteCount()
	packet := MessagePoolGet(60)
	ack := &parsedTcp{ack: true, ackNumber: 300, windowSize: 4_096}
	if !sequence.applyEstablishedPureAck(SourceId(NewId()), TransferKey{}, ack, packet) {
		t.Fatal("established pure ACK did not take the direct path")
	}
	select {
	case <-woke:
	case <-time.After(time.Second):
		t.Fatal("direct ACK did not wake the blocked return-window reader")
	}
	if len(sequence.sendItems) != 0 {
		t.Fatalf("direct ACK queued %d sequence items, want zero", len(sequence.sendItems))
	}
	sequence.mutex.Lock()
	ackNumber := sequence.receiveSeqAck
	windowSize := sequence.receiveWindowSize
	sequence.mutex.Unlock()
	if ackNumber != 300 || windowSize != 4_096 {
		t.Fatalf("direct ACK state=(%d, %d), want (300, 4096)", ackNumber, windowSize)
	}
	if got := MessagePoolPacketOutstandingByteCount(); got != baselineBytes {
		t.Fatalf("direct ACK retained packet bytes=%d, baseline=%d", got, baselineBytes)
	}
}

func TestPureTcpAckBeforeHandshakeKeepsOrderedOwnership(t *testing.T) {
	sequence := newEstablishedTcpAckFastPathSequence(t)
	sequence.mutex.Lock()
	sequence.established = false
	sequence.mutex.Unlock()

	baselineBytes := MessagePoolPacketOutstandingByteCount()
	packet := MessagePoolGet(60)
	if sequence.applyEstablishedPureAck(
		SourceId(NewId()),
		TransferKey{},
		&parsedTcp{ack: true, ackNumber: 300, windowSize: 4_096},
		packet,
	) {
		t.Fatal("pre-handshake ACK bypassed the ordered sequence queue")
	}
	if got := MessagePoolPacketOutstandingByteCount(); got != baselineBytes+smallPacketPoolSize {
		t.Fatalf("rejected direct ACK ownership=%d, want %d", got, baselineBytes+smallPacketPoolSize)
	}
	MessagePoolReturn(packet)
}

func TestEstablishedPureTcpAckFastPathDoesNotAllocate(t *testing.T) {
	sequence := newEstablishedTcpAckFastPathSequence(t)
	ack := &parsedTcp{ack: true, ackNumber: 300, windowSize: 4_096}
	// Prime both the packet free list and every lazy sequence helper outside the
	// measured loop.
	packet := MessagePoolGet(60)
	if !sequence.applyEstablishedPureAck(SourceId(NewId()), TransferKey{}, ack, packet) {
		t.Fatal("could not prime direct ACK path")
	}

	allocations := testing.AllocsPerRun(1_000, func() {
		packet := MessagePoolGet(60)
		if !sequence.applyEstablishedPureAck(TransferPath{}, TransferKey{}, ack, packet) {
			panic("direct ACK path closed")
		}
	})
	if allocations != 0 {
		t.Fatalf("direct ACK path allocated %.0f objects, want zero", allocations)
	}
}

func TestTcpBufferDispatchesEstablishedPureAckWithoutQueueing(t *testing.T) {
	sequence := newEstablishedTcpAckFastPathSequence(t)
	source := sequence.source
	tcp := &parsedTcp{
		sourceIp:        sequence.sourceIp,
		sourcePort:      sequence.sourcePort,
		destinationIp:   sequence.destinationIp,
		destinationPort: sequence.destinationPort,
		ack:             true,
		ackNumber:       320,
		windowSize:      4_096,
	}
	bufferId := NewBufferId4(
		source,
		tcp.sourceIp,
		int(tcp.sourcePort),
		tcp.destinationIp,
		int(tcp.destinationPort),
	)
	buffer := &TcpBuffer[BufferId4]{
		ctx:               sequence.ctx,
		tcpBufferSettings: DefaultTcpBufferSettings(),
		sequences:         map[BufferId4]*TcpSequence{bufferId: sequence},
		sourceSequences:   map[TransferPath]map[BufferId4]*TcpSequence{},
	}

	baselineBytes := MessagePoolPacketOutstandingByteCount()
	packet := MessagePoolGet(60)
	success, err := buffer.tcpSend(
		bufferId,
		source,
		TransferKey{},
		protocol.ProvideMode_Stream,
		4,
		tcp,
		0,
		packet,
	)
	if err != nil || !success {
		t.Fatalf("pure ACK dispatch=(%t, %v), want success", success, err)
	}
	if len(sequence.sendItems) != 0 {
		t.Fatalf("pure ACK dispatch queued %d items, want zero", len(sequence.sendItems))
	}
	if got := MessagePoolPacketOutstandingByteCount(); got != baselineBytes {
		t.Fatalf("pure ACK dispatch retained packet bytes=%d, baseline=%d", got, baselineBytes)
	}
}
