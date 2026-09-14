// Receiver-side nat regressions keep authenticated sources paired with their
// complete reply lanes while socket flows survive transfer reformations.
package connect

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// A provider queues synthesized controls but sends bytes consumed from a TCP
// socket synchronously. The SYN+ACK must use that same recovery lane or an
// immediately server-first greeting can overtake it and be discarded by the
// client's not-yet-established TCP stack.
func TestTcpSynAckUsesOrderedSocketRecoveryLane(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sequenceSocket, upstreamSocket := net.Pipe()
	defer upstreamSocket.Close()
	settings := DefaultTcpBufferSettingsWithBufferSize(1)
	settings.DialContextSettings = &DialContextSettings{
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return sequenceSocket, nil
		},
	}

	recoveryModes := make(chan receiveRecoveryMode, 1)
	sequence := newTcpSequenceWithTransferKey(
		ctx,
		func(
			_ TransferPath,
			_ TransferKey,
			_ protocol.ProvideMode,
			recoveryMode receiveRecoveryMode,
			_ *IpPath,
			packet []byte,
		) {
			_, sourceIp, destinationIp, transport, ok := parseIpv4(packet)
			if !ok {
				return
			}
			var tcp parsedTcp
			if parseTcpPacket(sourceIp, destinationIp, transport, &tcp) && tcp.syn && tcp.ack {
				recoveryModes <- recoveryMode
			}
		},
		SourceId(NewId()),
		TransferKey{},
		protocol.ProvideMode_Network,
		4,
		net.IPv4(10, 0, 0, 1).To4(),
		40001,
		net.IPv4(203, 0, 113, 7).To4(),
		587,
		1000,
		settings,
	)
	done := make(chan struct{})
	go func() {
		sequence.Run()
		close(done)
	}()

	synPacket := MessagePoolGet(Ipv4HeaderSizeWithoutExtensions + TcpHeaderSizeWithoutExtensions)
	success, err := sequence.send(
		&TcpSendItem{
			provideMode: protocol.ProvideMode_Network,
			tcp: parsedTcp{
				syn:        true,
				seq:        1000,
				windowSize: 65535,
			},
			ipPacket: synPacket,
		},
		-1,
	)
	if err != nil || !success {
		MessagePoolReturn(synPacket)
		t.Fatalf("send SYN: success=%t err=%v", success, err)
	}

	select {
	case recoveryMode := <-recoveryModes:
		if recoveryMode != receiveRecoveryModeTcpSocket {
			t.Fatalf("SYN+ACK recovery mode=%d, want ordered TCP socket lane", recoveryMode)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SYN+ACK was not returned")
	}

	sequence.Cancel()
	upstreamSocket.Close()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("TCP sequence did not stop")
	}
}

// A provider may finish its upstream dial while the first synthesized SYN-ACK
// is lost at a later routing boundary. The source remains in SYN-SENT and
// retransmits its identical SYN. A pure ACK cannot complete that state; the
// provider must retransmit SYN-ACK until the source acknowledges the handshake.
func TestTcpRetransmittedSynResendsSynAckBeforeHandshakeAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sequenceSocket, upstreamSocket := net.Pipe()
	settings := DefaultTcpBufferSettingsWithBufferSize(2)
	settings.DialContextSettings = &DialContextSettings{
		DialContext: func(context.Context, string, string) (net.Conn, error) {
			return sequenceSocket, nil
		},
	}

	type responseFlags struct {
		syn bool
		ack bool
		seq uint32
	}
	responses := make(chan responseFlags, 2)
	sequence := newTcpSequenceWithTransferKey(
		ctx,
		func(
			_ TransferPath,
			_ TransferKey,
			_ protocol.ProvideMode,
			_ receiveRecoveryMode,
			_ *IpPath,
			packet []byte,
		) {
			_, sourceIp, destinationIp, transport, ok := parseIpv4(packet)
			if !ok {
				return
			}
			var tcp parsedTcp
			if parseTcpPacket(sourceIp, destinationIp, transport, &tcp) {
				responses <- responseFlags{syn: tcp.syn, ack: tcp.ack, seq: tcp.seq}
			}
		},
		SourceId(NewId()),
		TransferKey{},
		protocol.ProvideMode_Network,
		4,
		net.IPv4(10, 0, 0, 1).To4(),
		40001,
		net.IPv4(203, 0, 113, 7).To4(),
		443,
		1000,
		settings,
	)
	done := make(chan struct{})
	go func() {
		sequence.Run()
		close(done)
	}()
	defer func() {
		sequence.Cancel()
		cancel()
		upstreamSocket.Close()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("TCP sequence did not stop")
		}
	}()

	sendSyn := func(label string) {
		t.Helper()
		packet := MessagePoolGet(Ipv4HeaderSizeWithoutExtensions + TcpHeaderSizeWithoutExtensions)
		success, err := sequence.send(
			&TcpSendItem{
				provideMode: protocol.ProvideMode_Network,
				tcp: parsedTcp{
					syn:        true,
					seq:        1000,
					windowSize: 65535,
				},
				ipPacket: packet,
			},
			-1,
		)
		if err != nil || !success {
			MessagePoolReturn(packet)
			t.Fatalf("send %s SYN: success=%t err=%v", label, success, err)
		}
	}
	receive := func(label string) responseFlags {
		t.Helper()
		select {
		case flags := <-responses:
			return flags
		case <-time.After(2 * time.Second):
			t.Fatalf("%s SYN received no provider response", label)
			return responseFlags{}
		}
	}

	sendSyn("initial")
	initialResponse := receive("initial")
	if !initialResponse.syn || !initialResponse.ack {
		t.Fatalf("initial response = SYN:%t ACK:%t, want SYN-ACK", initialResponse.syn, initialResponse.ack)
	}
	sendSyn("retransmitted")
	if flags := receive("retransmitted"); !flags.syn || !flags.ack || flags.seq != initialResponse.seq {
		t.Fatalf(
			"retransmitted response = SYN:%t ACK:%t seq:%d, want SYN-ACK seq:%d",
			flags.syn,
			flags.ack,
			flags.seq,
			initialResponse.seq,
		)
	}

	// The source's ACK and the following stale SYN share one FIFO send
	// sequence, so this deterministically checks the other state boundary with
	// no polling: once the handshake is acknowledged, the stale SYN gets the
	// established-flow pure ACK instead of reopening the handshake.
	ackPacket := MessagePoolGet(Ipv4HeaderSizeWithoutExtensions + TcpHeaderSizeWithoutExtensions)
	ackSuccess, ackErr := sequence.send(
		&TcpSendItem{
			provideMode: protocol.ProvideMode_Network,
			tcp: parsedTcp{
				ack:        true,
				seq:        1001,
				ackNumber:  1001,
				windowSize: 65535,
			},
			ipPacket: ackPacket,
		},
		-1,
	)
	if ackErr != nil || !ackSuccess {
		MessagePoolReturn(ackPacket)
		t.Fatalf("send handshake ACK: success=%t err=%v", ackSuccess, ackErr)
	}
	sendSyn("post-handshake stale")
	if flags := receive("post-handshake stale"); flags.syn || !flags.ack {
		t.Fatalf("post-handshake response = SYN:%t ACK:%t, want pure ACK", flags.syn, flags.ack)
	}
}

// A source/lane snapshot is returned from one NAT callback.
type natTransferSnapshot struct {
	source       TransferPath
	transferKey  TransferKey
	recoveryMode receiveRecoveryMode
}

// Returns one authenticated source and two receive lanes for its flow.
func natTransferKeyPair() (TransferPath, TransferKey, TransferKey) {
	source := TransferPath{
		SourceId: NewId(),
		StreamId: NewId(),
	}
	initial := TransferKey{
		EncryptionRole: protocol.SequenceRole_SequenceRoleClient,
	}
	latest := TransferKey{
		ForceStream:         true,
		CompanionContract:   true,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: true,
	}
	return source, initial, latest
}

// Reproduces a live flow moving to a new transfer lane while its socket stays
// open. Every protocol must return the newest key with the original source.
func TestNatSequencesReturnLatestTransferKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source, initial, latest := natTransferKeyPair()
	sourceIp := net.IPv4(10, 0, 0, 1)
	destinationIp := net.IPv4(203, 0, 113, 1)
	tests := []struct {
		name             string
		wantRecoveryMode receiveRecoveryMode
		receive          func() natTransferSnapshot
	}{
		{
			name:             "udp",
			wantRecoveryMode: receiveRecoveryModeNonblocking,
			receive: func() natTransferSnapshot {
				var received natTransferSnapshot
				sequence := newUdpSequenceWithTransferKey(
					ctx,
					func(source TransferPath, transferKey TransferKey, _ protocol.ProvideMode, recoveryMode receiveRecoveryMode, _ *IpPath, _ []byte) {
						received = natTransferSnapshot{source: source, transferKey: transferKey, recoveryMode: recoveryMode}
					},
					source, initial, protocol.ProvideMode_Network, 4,
					sourceIp, 1000, destinationIp, 2000,
					DefaultUdpBufferSettingsWithBufferSize(1),
				)
				defer sequence.Cancel()
				outbound := MessagePoolCopy([]byte{1})
				success, err := sequence.send(&UdpSendItem{source: source, transferKey: latest, ipPacket: outbound}, 0)
				if err != nil || !success {
					t.Fatalf("update udp lane: success=%t err=%v", success, err)
				}
				queued := <-sequence.sendItems
				defer MessagePoolReturn(queued.ipPacket)
				sequence.receivePacket(MessagePoolCopy([]byte{2}))
				return received
			},
		},
		{
			name:             "tcp",
			wantRecoveryMode: receiveRecoveryModeTcpSocket,
			receive: func() natTransferSnapshot {
				var received natTransferSnapshot
				sequence := newTcpSequenceWithTransferKey(
					ctx,
					func(source TransferPath, transferKey TransferKey, _ protocol.ProvideMode, recoveryMode receiveRecoveryMode, _ *IpPath, _ []byte) {
						received = natTransferSnapshot{source: source, transferKey: transferKey, recoveryMode: recoveryMode}
					},
					source, initial, protocol.ProvideMode_Network, 4,
					sourceIp, 1000, destinationIp, 2000, 1,
					DefaultTcpBufferSettingsWithBufferSize(1),
				)
				defer sequence.Cancel()
				outbound := MessagePoolCopy([]byte{1})
				success, err := sequence.send(&TcpSendItem{source: source, transferKey: latest, ipPacket: outbound}, 0)
				if err != nil || !success {
					t.Fatalf("update tcp lane: success=%t err=%v", success, err)
				}
				queued := <-sequence.sendItems
				defer MessagePoolReturn(queued.ipPacket)
				sequence.receivePacket(MessagePoolCopy([]byte{2}), receiveRecoveryModeTcpSocket)
				return received
			},
		},
		{
			name:             "icmp",
			wantRecoveryMode: receiveRecoveryModeNonblocking,
			receive: func() natTransferSnapshot {
				var received natTransferSnapshot
				sequence := newIcmpSequenceWithTransferKey(
					ctx,
					func(source TransferPath, transferKey TransferKey, _ protocol.ProvideMode, recoveryMode receiveRecoveryMode, _ *IpPath, _ []byte) {
						received = natTransferSnapshot{source: source, transferKey: transferKey, recoveryMode: recoveryMode}
					},
					source, initial, protocol.ProvideMode_Network, 4,
					sourceIp, 1, destinationIp,
					DefaultIcmpBufferSettingsWithBufferSize(1),
				)
				defer sequence.Cancel()
				outbound := MessagePoolCopy([]byte{1})
				success, err := sequence.send(&IcmpSendItem{source: source, transferKey: latest, ipPacket: outbound}, 0)
				if err != nil || !success {
					t.Fatalf("update icmp lane: success=%t err=%v", success, err)
				}
				queued := <-sequence.sendItems
				defer MessagePoolReturn(queued.ipPacket)
				sequence.receivePacket(MessagePoolCopy([]byte{2}))
				return received
			},
		},
	}
	for _, test := range tests {
		want := natTransferSnapshot{
			source:       source.LocalMask(),
			transferKey:  latest,
			recoveryMode: test.wantRecoveryMode,
		}
		if received := test.receive(); received != want {
			t.Errorf("%s transfer snapshot = %#v, want %#v", test.name, received, want)
		}
	}
}

// Verifies every batch callback receives one stable source/key snapshot used
// by the corresponding per-packet path.
func TestNatSequencesBatchOneTransferKeySnapshot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source, _, transferKey := natTransferKeyPair()
	wantSource := source.LocalMask()
	sourceIp := net.IPv4(10, 0, 0, 1)
	destinationIp := net.IPv4(203, 0, 113, 1)
	tests := []struct {
		name             string
		wantRecoveryMode receiveRecoveryMode
		receive          func(receiveTransferPacketsBatchFunction)
	}{
		{
			name:             "udp",
			wantRecoveryMode: receiveRecoveryModeNonblocking,
			receive: func(callback receiveTransferPacketsBatchFunction) {
				sequence := newUdpSequenceWithTransferKey(
					ctx, func(TransferPath, TransferKey, protocol.ProvideMode, receiveRecoveryMode, *IpPath, []byte) {},
					source, transferKey, protocol.ProvideMode_Network, 4,
					sourceIp, 1000, destinationIp, 2000,
					DefaultUdpBufferSettings(),
				)
				defer sequence.Cancel()
				sequence.receiveTransferPacketsCallback = callback
				sequence.receiveBatch([][]byte{MessagePoolCopy([]byte{1})})
			},
		},
		{
			name:             "tcp",
			wantRecoveryMode: receiveRecoveryModeTcpSocket,
			receive: func(callback receiveTransferPacketsBatchFunction) {
				sequence := newTcpSequenceWithTransferKey(
					ctx, func(TransferPath, TransferKey, protocol.ProvideMode, receiveRecoveryMode, *IpPath, []byte) {},
					source, transferKey, protocol.ProvideMode_Network, 4,
					sourceIp, 1000, destinationIp, 2000, 1,
					DefaultTcpBufferSettings(),
				)
				defer sequence.Cancel()
				sequence.receiveTransferPacketsCallback = callback
				sequence.receiveBatch(
					[][]byte{MessagePoolCopy([]byte{1})},
					receiveRecoveryModeTcpSocket,
				)
			},
		},
		{
			name:             "icmp",
			wantRecoveryMode: receiveRecoveryModeNonblocking,
			receive: func(callback receiveTransferPacketsBatchFunction) {
				sequence := newIcmpSequenceWithTransferKey(
					ctx, func(TransferPath, TransferKey, protocol.ProvideMode, receiveRecoveryMode, *IpPath, []byte) {},
					source, transferKey, protocol.ProvideMode_Network, 4,
					sourceIp, 1, destinationIp,
					DefaultIcmpBufferSettings(),
				)
				defer sequence.Cancel()
				sequence.receiveTransferPacketsCallback = callback
				sequence.receivePacket(MessagePoolCopy([]byte{1}))
			},
		},
	}
	for _, test := range tests {
		var received natTransferSnapshot
		test.receive(func(source TransferPath, key TransferKey, _ protocol.ProvideMode, recoveryMode receiveRecoveryMode, _ *IpPath, _ [][]byte) bool {
			received = natTransferSnapshot{source: source, transferKey: key, recoveryMode: recoveryMode}
			return true
		})
		want := natTransferSnapshot{
			source:       wantSource,
			transferKey:  transferKey,
			recoveryMode: test.wantRecoveryMode,
		}
		if received != want {
			t.Errorf("%s transfer snapshot = %#v, want %#v", test.name, received, want)
		}
	}
}

// A flow state never accepts another authenticated source's lane, while a
// lane refresh for the same source remains visible to its return callback.
func TestTransferStateKeepsSourceAndKeyPaired(t *testing.T) {
	source, initial, latest := natTransferKeyPair()
	state := newTransferState(source, initial)
	otherSource := SourceId(NewId())
	state.update(otherSource, latest)
	receivedSource, receivedKey := state.get()
	AssertEqual(t, source.LocalMask(), receivedSource)
	AssertEqual(t, initial, receivedKey)

	state.update(source, latest)
	receivedSource, receivedKey = state.get()
	AssertEqual(t, source.LocalMask(), receivedSource)
	AssertEqual(t, latest, receivedKey)
}
