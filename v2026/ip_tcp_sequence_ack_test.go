// Deterministic TCP acknowledgement tests cover sequence arithmetic and
// pool ownership at cancellation boundaries.
package connect

import (
	"context"
	"encoding/binary"
	"math"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

func newTcpAckOrderingTestSequence() *TcpSequence {
	return &TcpSequence{
		ConnectionState: ConnectionState{
			sendSeq:             200,
			receiveSeq:          2000,
			receiveSeqAck:       1000,
			receiveWindowSize:   1024,
			receiveWindowEnd:    2024,
			receiveWindowEndSet: true,
		},
	}
}

// Proves cancellation after pure-acknowledgement construction returns the
// packet instead of abandoning its sole pool ownership before callback delivery.
func TestTcpSequenceCancelAfterPureAckBuildReturnsPacket(t *testing.T) {
	ackBuilt := make(chan struct{})
	ackWorkerStopped := make(chan struct{})
	releaseAck := make(chan struct{})
	var observeOnce sync.Once
	var observedPacket []byte
	harness := newTcpReorderTestHarnessWithSetup(t, 1000, 8, 0, func(sequence *TcpSequence) {
		sequence.afterPureAckBuildForTest = func(packet []byte) {
			observeOnce.Do(func() {
				observedPacket = MessagePoolShareReadOnly(packet)
				close(ackBuilt)
				<-releaseAck
			})
		}
		sequence.afterPureAckWorkerStopForTest = func() {
			close(ackWorkerStopped)
		}
	})

	harness.sendPayload(harness.nextSeq, "x", false)
	select {
	case <-ackBuilt:
	case <-time.After(2 * time.Second):
		close(releaseAck)
		t.Fatal("pure ACK was not built")
	}

	// Cancellation is already visible when the held worker resumes, forcing
	// the exact pre-delivery ownership branch without scheduler timing.
	harness.sequence.Cancel()
	close(releaseAck)
	select {
	case <-ackWorkerStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("pure-ACK worker did not stop")
	}
	harness.close()
	if MessagePoolReturn(observedPacket) {
		return
	}

	// Reclaim the production reference on old behavior so the regression does
	// not contaminate later pool-balance tests in the same process.
	MessagePoolReturn(observedPacket)
	t.Fatal("canceled pure ACK retained production pool ownership")
}

// Proves Run publishes completion only after its held pure-acknowledgement
// worker has reached terminal cleanup.
func TestTcpSequenceRunDoesNotReturnWhilePureAckWorkerHeld(t *testing.T) {
	ackBuilt := make(chan struct{})
	ackWorkerStopped := make(chan struct{})
	childWorkersWaitStarted := make(chan struct{})
	releaseAck := make(chan struct{})
	var releaseOnce sync.Once
	release := func() {
		releaseOnce.Do(func() { close(releaseAck) })
	}
	defer release()

	harness := newTcpReorderTestHarnessWithSetup(t, 1000, 8, 0, func(sequence *TcpSequence) {
		sequence.afterPureAckBuildForTest = func([]byte) {
			close(ackBuilt)
			<-releaseAck
		}
		sequence.afterPureAckWorkerStopForTest = func() {
			close(ackWorkerStopped)
		}
		sequence.beforeChildWorkersWaitForTest = func() {
			close(childWorkersWaitStarted)
		}
	})
	harness.sendPayload(harness.nextSeq, "x", false)
	select {
	case <-ackBuilt:
	case <-time.After(2 * time.Second):
		t.Fatal("pure ACK was not built")
	}

	harness.sequence.Cancel()
	select {
	case <-childWorkersWaitStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("TcpSequence.Run did not reach its child-worker join")
	}
	release()
	select {
	case <-ackWorkerStopped:
	case <-time.After(2 * time.Second):
		t.Fatal("pure-ACK worker did not stop")
	}
	select {
	case <-harness.runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("TcpSequence.Run did not complete after its child worker stopped")
	}
	harness.close()
}

func TestTcpSequenceAcceptsPureAckAheadOfDeliveredUpload(t *testing.T) {
	sequence := newTcpAckOrderingTestSequence()
	updated := sequence.applySendAckWithLock(&parsedTcp{
		seq:        300,
		ack:        true,
		ackNumber:  1500,
		windowSize: 4096,
	})
	if !updated || sequence.receiveSeqAck != 1500 || sequence.receiveWindowSize != 4096 {
		t.Fatalf(
			"ahead ACK did not advance return window: updated=%t ack=%d window=%d",
			updated,
			sequence.receiveSeqAck,
			sequence.receiveWindowSize,
		)
	}
}

func TestTcpSequenceAcceptsPureAckBehindDeliveredUpload(t *testing.T) {
	sequence := newTcpAckOrderingTestSequence()
	updated := sequence.applySendAckWithLock(&parsedTcp{
		seq:        100,
		ack:        true,
		ackNumber:  1500,
		windowSize: 4096,
	})
	if !updated || sequence.receiveSeqAck != 1500 || sequence.receiveWindowSize != 4096 {
		t.Fatalf(
			"delayed ACK did not advance return window: updated=%t ack=%d window=%d",
			updated,
			sequence.receiveSeqAck,
			sequence.receiveWindowSize,
		)
	}
}

func TestTcpSequenceRejectsAckBeyondEmittedReturnData(t *testing.T) {
	sequence := newTcpAckOrderingTestSequence()
	updated := sequence.applySendAckWithLock(&parsedTcp{
		seq:        200,
		ack:        true,
		ackNumber:  2001,
		windowSize: 4096,
	})
	if updated || sequence.receiveSeqAck != 1000 || sequence.receiveWindowSize != 1024 {
		t.Fatalf(
			"future ACK changed return window: updated=%t ack=%d window=%d",
			updated,
			sequence.receiveSeqAck,
			sequence.receiveWindowSize,
		)
	}
}

func TestTcpSequenceAppliesValidAckIndependentOfPayloadOrder(t *testing.T) {
	sequence := newTcpAckOrderingTestSequence()
	updated := sequence.applySendAckWithLock(&parsedTcp{
		seq:        201,
		ack:        true,
		ackNumber:  1500,
		windowSize: 4096,
		payload:    []byte{1},
	})
	if !updated || sequence.receiveSeqAck != 1500 || sequence.receiveWindowSize != 4096 {
		t.Fatal("valid ACK field on out-of-order payload did not advance return window")
	}
}

func TestTcpSequenceZeroWindowAdvancesToExistingRightEdge(t *testing.T) {
	sequence := newTcpAckOrderingTestSequence()
	sequence.receiveWindowSize = 500
	sequence.receiveWindowEnd = 1500
	updated := sequence.applySendAckWithLock(&parsedTcp{
		seq:        200,
		ack:        true,
		ackNumber:  1500,
		windowSize: 0,
	})
	if !updated || sequence.receiveSeqAck != 1500 || sequence.receiveWindowSize != 0 {
		t.Fatalf(
			"zero-window ACK state: updated=%t ack=%d window=%d",
			updated,
			sequence.receiveSeqAck,
			sequence.receiveWindowSize,
		)
	}
}

func TestTcpSequenceReopensWindowAtSameAck(t *testing.T) {
	sequence := newTcpAckOrderingTestSequence()
	sequence.receiveSeqAck = 1500
	sequence.receiveWindowSize = 0
	sequence.receiveWindowEnd = 1500
	updated := sequence.applySendAckWithLock(&parsedTcp{
		seq:        200,
		ack:        true,
		ackNumber:  1500,
		windowSize: 4096,
	})
	if !updated || sequence.receiveWindowSize != 4096 || sequence.receiveWindowEnd != 5596 {
		t.Fatalf(
			"window-reopen state: updated=%t window=%d end=%d",
			updated,
			sequence.receiveWindowSize,
			sequence.receiveWindowEnd,
		)
	}
}

func TestTcpSequenceStaleDuplicateCannotShrinkReopenedWindow(t *testing.T) {
	sequence := newTcpAckOrderingTestSequence()
	sequence.receiveSeqAck = 1500
	sequence.receiveWindowSize = 4096
	sequence.receiveWindowEnd = 5596
	updated := sequence.applySendAckWithLock(&parsedTcp{
		seq:        100,
		ack:        true,
		ackNumber:  1500,
		windowSize: 0,
	})
	if updated || sequence.receiveWindowSize != 4096 || sequence.receiveWindowEnd != 5596 {
		t.Fatalf(
			"stale window update changed reopened edge: updated=%t window=%d end=%d",
			updated,
			sequence.receiveWindowSize,
			sequence.receiveWindowEnd,
		)
	}
}

func TestTcpSequenceSynWindowIsLiteralBeforeScalingBegins(t *testing.T) {
	sequence := newTcpAckOrderingTestSequence()
	sequence.tcpBufferSettings = &TcpBufferSettings{
		MaxWindowSize: 1024 * 1024,
	}
	tcp := &parsedTcp{
		seq:        9000,
		windowSize: 16384,
		options:    []byte{2, 4, 0x05, 0xb4, 1, 3, 3, 5},
	}
	sequence.mutex.Lock()
	sequence.initializeSynWithLock(tcp)
	sequence.mutex.Unlock()

	if !sequence.enableWindowScale || sequence.receiveWindowScale != 5 {
		t.Fatalf("window scale negotiation enabled=%t scale=%d, want true and 5", sequence.enableWindowScale, sequence.receiveWindowScale)
	}
	if sequence.receiveWindowSize != 16384 {
		t.Fatalf("SYN window became %d, want literal 16384 before scaling", sequence.receiveWindowSize)
	}
	if sequence.receiveWindowEnd != 9000+16384 {
		t.Fatalf("SYN window right edge=%d, want %d", sequence.receiveWindowEnd, 9000+16384)
	}
	if sequence.sendSeq != 9001 || sequence.receiveSeqAck != 9000 {
		t.Fatalf("SYN sequence state send=%d receive_ack=%d, want 9001 and 9000", sequence.sendSeq, sequence.receiveSeqAck)
	}
}

func TestTcpSequenceSynWindowScaleIsClampedToProtocolMaximum(t *testing.T) {
	sequence := newTcpAckOrderingTestSequence()
	sequence.tcpBufferSettings = &TcpBufferSettings{
		MaxWindowSize: 1024 * 1024,
	}
	tcp := &parsedTcp{
		seq:        100,
		windowSize: 4096,
		options:    []byte{3, 3, 255},
	}
	sequence.mutex.Lock()
	sequence.initializeSynWithLock(tcp)
	sequence.mutex.Unlock()

	if !sequence.enableWindowScale || sequence.receiveWindowScale != 14 {
		t.Fatalf("window scale negotiation enabled=%t scale=%d, want true and 14", sequence.enableWindowScale, sequence.receiveWindowScale)
	}
	if sequence.receiveWindowSize != 4096 {
		t.Fatalf("clamped scale changed literal SYN window to %d", sequence.receiveWindowSize)
	}
}

// The SYN window is not scaled even when its option negotiates scaling for
// later packets. This catches the provider warmup failure where a large
// high-BDP maximum reduced the opening advertised window to only a few KiB.
func TestTcpSequenceSynAckUsesLiteralOpeningWindow(t *testing.T) {
	settings := DefaultTcpBufferSettingsWithBufferSize(8)
	sequence := newTcpSequenceWithTransferKey(
		context.Background(),
		func(
			source TransferPath,
			transferKey TransferKey,
			provideMode protocol.ProvideMode,
			recoveryMode receiveRecoveryMode,
			ipPath *IpPath,
			packet []byte,
		) {
		},
		TransferPath{},
		TransferKey{},
		protocol.ProvideMode_Public,
		4,
		net.IP{72, 0, 0, 1},
		40000,
		net.IP{72, 2, 3, 4},
		443,
		0,
		settings,
	)
	defer sequence.Close()

	sequence.mutex.Lock()
	sequence.initializeSynWithLock(&parsedTcp{
		seq:        9000,
		windowSize: math.MaxUint16,
		options:    []byte{3, 3, 8},
	})
	sequence.mutex.Unlock()

	packet, err := sequence.SynAck(settings.Mtu)
	if err != nil {
		t.Fatalf("build SYN-ACK: %v", err)
	}
	defer MessagePoolReturn(packet)

	tcpOffset := Ipv4HeaderSizeWithoutExtensions
	windowSize := binary.BigEndian.Uint16(packet[tcpOffset+14 : tcpOffset+16])
	if windowSize != math.MaxUint16 {
		t.Fatalf("SYN-ACK window=%d, want literal %d", windowSize, math.MaxUint16)
	}
	if sequence.windowSize != settings.InitialWindowSize {
		t.Fatalf(
			"post-handshake warmup window=%d, want configured initial=%d",
			sequence.windowSize,
			settings.InitialWindowSize,
		)
	}
	if sequence.encodedWindowSize() != uint16(settings.InitialWindowSize>>sequence.windowScale) {
		t.Fatalf(
			"scaled post-handshake window=%d, want %d",
			sequence.encodedWindowSize(),
			settings.InitialWindowSize>>sequence.windowScale,
		)
	}
}

// A retransmitted SYN is RTT-ambiguous without timestamps. The provider must
// negotiate the source timestamp and echo the newest value on every later
// segment so the source can sample a high-latency handshake instead of keeping
// its one-second initial retransmission timeout.
func TestTcpSequenceNegotiatesAndEchoesTimestamps(t *testing.T) {
	settings := DefaultTcpBufferSettingsWithBufferSize(8)
	sequence := newTcpSequenceWithTransferKey(
		context.Background(),
		func(
			source TransferPath,
			transferKey TransferKey,
			provideMode protocol.ProvideMode,
			recoveryMode receiveRecoveryMode,
			ipPath *IpPath,
			packet []byte,
		) {
		},
		TransferPath{},
		TransferKey{},
		protocol.ProvideMode_Public,
		4,
		net.IP{72, 0, 0, 1},
		40000,
		net.IP{72, 2, 3, 4},
		443,
		0,
		settings,
	)
	defer sequence.Close()
	sequence.timestampValueForTest = func() uint32 { return 7000 }

	synTimestampValue := uint32(5000)
	synOptions := []byte{
		2, 4, 0x05, 0xb4,
		1, 3, 3, 8,
		1, 1, 8, 10,
		0, 0, 0, 0,
		0, 0, 0, 0,
	}
	binary.BigEndian.PutUint32(synOptions[12:16], synTimestampValue)
	syn := &parsedTcp{
		seq:        9000,
		windowSize: math.MaxUint16,
		options:    synOptions,
	}
	sequence.mutex.Lock()
	sequence.initializeSynWithLock(syn)
	sequence.mutex.Unlock()

	parseGeneratedTcp := func(packet []byte) *parsedTcp {
		ipProtocol, sourceIp, destinationIp, transport, ok := parseIpv4(packet)
		if !ok || ipProtocol != ipProtocolNumberTcp {
			t.Fatalf("generated packet is not valid IPv4 TCP")
		}
		tcp := &parsedTcp{}
		if !parseTcpPacket(sourceIp, destinationIp, transport, tcp) {
			t.Fatalf("generated packet has an invalid TCP header")
		}
		return tcp
	}

	synAck, err := sequence.SynAck(settings.Mtu)
	if err != nil {
		t.Fatalf("build SYN-ACK: %v", err)
	}
	parsedSynAck := parseGeneratedTcp(synAck)
	if !parsedSynAck.enableTimestamp || parsedSynAck.timestampValue != 7000 || parsedSynAck.timestampEcho != synTimestampValue {
		t.Fatalf(
			"SYN-ACK timestamp enabled=%t value=%d echo=%d, want true, 7000, %d",
			parsedSynAck.enableTimestamp,
			parsedSynAck.timestampValue,
			parsedSynAck.timestampEcho,
			synTimestampValue,
		)
	}
	MessagePoolReturn(synAck)

	latestTimestampValue := uint32(6000)
	ackOptions := []byte{1, 1, 8, 10, 0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(ackOptions[4:8], latestTimestampValue)
	ack := &parsedTcp{seq: sequence.sendSeq, options: ackOptions}
	parseTcpOptions(ack)
	sequence.mutex.Lock()
	sequence.updateTimestampRecentWithLock(ack)
	pureAck, err := sequence.PureAck()
	sequence.mutex.Unlock()
	if err != nil {
		t.Fatalf("build pure ACK: %v", err)
	}
	parsedPureAck := parseGeneratedTcp(pureAck)
	if !parsedPureAck.enableTimestamp || parsedPureAck.timestampEcho != latestTimestampValue {
		t.Fatalf(
			"pure ACK timestamp enabled=%t echo=%d, want true and %d",
			parsedPureAck.enableTimestamp,
			parsedPureAck.timestampEcho,
			latestTimestampValue,
		)
	}
	MessagePoolReturn(pureAck)

	futureOptions := append([]byte(nil), ackOptions...)
	binary.BigEndian.PutUint32(futureOptions[4:8], latestTimestampValue+1000)
	future := &parsedTcp{seq: sequence.sendSeq + 1, options: futureOptions}
	parseTcpOptions(future)
	sequence.mutex.Lock()
	sequence.updateTimestampRecentWithLock(future)
	retainedTimestampValue := sequence.timestampRecent
	sequence.mutex.Unlock()
	if retainedTimestampValue != latestTimestampValue {
		t.Fatalf(
			"future segment moved timestamp echo to %d, want retained %d",
			retainedTimestampValue,
			latestTimestampValue,
		)
	}

	payload := make([]byte, settings.Mtu)
	sequence.mutex.Lock()
	packets, err := sequence.DataPackets(payload, len(payload), settings.Mtu)
	sequence.mutex.Unlock()
	if err != nil {
		t.Fatalf("build data packets: %v", err)
	}
	if len(packets) != 2 {
		t.Fatalf("timestamp-aware packet count=%d, want 2", len(packets))
	}
	for packetIndex, packet := range packets {
		if settings.Mtu < len(packet) {
			t.Fatalf("packet %d is %d bytes, exceeds MTU %d", packetIndex, len(packet), settings.Mtu)
		}
		parsedPacket := parseGeneratedTcp(packet)
		if !parsedPacket.enableTimestamp || parsedPacket.timestampEcho != latestTimestampValue {
			t.Fatalf(
				"packet %d timestamp enabled=%t echo=%d, want true and %d",
				packetIndex,
				parsedPacket.enableTimestamp,
				parsedPacket.timestampEcho,
				latestTimestampValue,
			)
		}
		MessagePoolReturn(packet)
	}
}
