package connect

import (
	"context"
	"net"
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

// newTcpGenerationTestBuffer keeps each upstream dial parked until its
// sequence is canceled, making the buffer's generation transitions
// deterministic without opening host sockets.
func newTcpGenerationTestBuffer(
	t *testing.T,
) (context.CancelFunc, *Tcp4Buffer, TransferPath, BufferId4, *parsedTcp) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	settings := DefaultTcpBufferSettingsWithBufferSize(8)
	settings.DialContextSettings = &DialContextSettings{
		DialContext: func(dialCtx context.Context, network string, addr string) (net.Conn, error) {
			<-dialCtx.Done()
			return nil, dialCtx.Err()
		},
	}
	buffer := NewTcp4Buffer(ctx, func(
		source TransferPath,
		provideMode protocol.ProvideMode,
		ipPath *IpPath,
		packet []byte,
	) {
	}, settings)
	source := SourceId(NewId())
	tcp := &parsedTcp{
		sourceIp:        net.IPv4(10, 0, 0, 1).To4(),
		destinationIp:   net.IPv4(203, 0, 113, 7).To4(),
		sourcePort:      40001,
		destinationPort: 443,
		syn:             true,
		seq:             1000,
		windowSize:      65535,
	}
	bufferId := NewBufferId4(
		source,
		tcp.sourceIp,
		int(tcp.sourcePort),
		tcp.destinationIp,
		int(tcp.destinationPort),
	)
	return cancel, buffer, source, bufferId, tcp
}

// sendTcpGenerationTestSyn transfers one pooled input packet to the buffer.
func sendTcpGenerationTestSyn(
	t *testing.T,
	buffer *Tcp4Buffer,
	source TransferPath,
	tcp *parsedTcp,
) {
	t.Helper()

	packet := MessagePoolGet(Ipv4HeaderSizeWithoutExtensions + TcpHeaderSizeWithoutExtensions)
	success, err := buffer.send(source, protocol.ProvideMode_Network, tcp, -1, packet)
	if err != nil {
		MessagePoolReturn(packet)
		t.Fatalf("send SYN: %v", err)
	}
	if !success {
		MessagePoolReturn(packet)
		t.Fatal("SYN was not accepted")
	}
}

// tcpGenerationTestSequence returns the currently indexed sequence.
func tcpGenerationTestSequence(buffer *Tcp4Buffer, bufferId BufferId4) *TcpSequence {
	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	return buffer.sequences[bufferId]
}

func TestTcpBufferDuplicateSynKeepsGeneration(t *testing.T) {
	cancel, buffer, source, bufferId, tcp := newTcpGenerationTestBuffer(t)
	defer cancel()

	sendTcpGenerationTestSyn(t, buffer, source, tcp)
	firstSequence := tcpGenerationTestSequence(buffer, bufferId)
	if firstSequence == nil {
		t.Fatal("first SYN did not create a sequence")
	}

	sendTcpGenerationTestSyn(t, buffer, source, tcp)
	if sequence := tcpGenerationTestSequence(buffer, bufferId); sequence != firstSequence {
		t.Fatal("an exact live SYN retransmission replaced its sequence generation")
	}
}

func TestTcpBufferNewSynReplacesReusedTuple(t *testing.T) {
	cancel, buffer, source, bufferId, tcp := newTcpGenerationTestBuffer(t)
	defer cancel()

	sendTcpGenerationTestSyn(t, buffer, source, tcp)
	firstSequence := tcpGenerationTestSequence(buffer, bufferId)
	if firstSequence == nil {
		t.Fatal("first SYN did not create a sequence")
	}

	replacementSyn := *tcp
	replacementSyn.seq += 1
	sendTcpGenerationTestSyn(t, buffer, source, &replacementSyn)

	replacementSequence := tcpGenerationTestSequence(buffer, bufferId)
	if replacementSequence == nil || replacementSequence == firstSequence {
		t.Fatal("a new SYN generation did not replace the reused four-tuple")
	}
	select {
	case <-firstSequence.ctx.Done():
	default:
		t.Fatal("replaced sequence was not canceled")
	}
	if replacementSequence.initialSynSeq != replacementSyn.seq {
		t.Fatalf(
			"replacement initial sequence=%d, want %d",
			replacementSequence.initialSynSeq,
			replacementSyn.seq,
		)
	}
}

func TestTcpBufferSynReplacesCanceledGeneration(t *testing.T) {
	cancel, buffer, source, bufferId, tcp := newTcpGenerationTestBuffer(t)
	defer cancel()

	sendTcpGenerationTestSyn(t, buffer, source, tcp)
	firstSequence := tcpGenerationTestSequence(buffer, bufferId)
	if firstSequence == nil {
		t.Fatal("first SYN did not create a sequence")
	}
	firstSequence.Cancel()

	sendTcpGenerationTestSyn(t, buffer, source, tcp)
	if sequence := tcpGenerationTestSequence(buffer, bufferId); sequence == nil || sequence == firstSequence {
		t.Fatal("a SYN did not replace the canceled sequence generation")
	}
}

func TestTcpSequenceCloseDeliversFlowLifecycleOnce(t *testing.T) {
	cancel, buffer, source, bufferId, tcp := newTcpGenerationTestBuffer(t)
	defer cancel()
	callbackCount := 0
	var callbackSource TransferPath
	var callbackPath *IpPath
	buffer.flowCloseCallback = func(closedSource TransferPath, closedPath *IpPath) {
		callbackCount += 1
		callbackSource = closedSource
		callbackPath = closedPath
	}

	sendTcpGenerationTestSyn(t, buffer, source, tcp)
	sequence := tcpGenerationTestSequence(buffer, bufferId)
	if sequence == nil {
		t.Fatal("SYN did not create a sequence")
	}
	sequence.Close()
	sequence.Close()

	if callbackCount != 1 {
		t.Fatalf("TCP sequence close delivered %d lifecycle callbacks, want 1", callbackCount)
	}
	if callbackSource != source.LocalMask() {
		t.Fatalf("TCP lifecycle source = %s, want %s", callbackSource, source.LocalMask())
	}
	if callbackPath == nil ||
		!callbackPath.SourceIp.Equal(tcp.sourceIp) ||
		callbackPath.SourcePort != int(tcp.sourcePort) ||
		!callbackPath.DestinationIp.Equal(tcp.destinationIp) ||
		callbackPath.DestinationPort != int(tcp.destinationPort) {
		t.Fatalf("TCP lifecycle path = %v, want canonical outbound tuple", callbackPath)
	}
}
