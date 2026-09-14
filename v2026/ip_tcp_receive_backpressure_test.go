package connect

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestTcpSocketReadPreservesReceiveCallbackBackpressure verifies a blocked
// return callback stops further upstream socket reads after the bounded
// one-batch read-ahead. The callback is an intentional transfer backpressure
// boundary; an independently window-sized delivery queue would hide that
// pressure and reserve memory for every active connection.
func TestTcpSocketReadPreservesReceiveCallbackBackpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sequenceSocket, upstreamSocket := net.Pipe()
	defer upstreamSocket.Close()

	settings := DefaultTcpBufferSettingsWithBufferSize(8)
	settings.ReadBufferByteCount = 4
	settings.ReadTimeout = 2 * time.Second
	settings.WriteBatchSize = 1
	settings.DialContextSettings = &DialContextSettings{
		DialContext: func(dialCtx context.Context, network string, addr string) (net.Conn, error) {
			return sequenceSocket, nil
		},
	}

	synAckReceived := make(chan struct{}, 1)
	callbackEntered := make(chan struct{})
	callbackRelease := make(chan struct{})
	payloads := make(chan []byte, 4)
	var payloadCallbackCount atomic.Int32
	receiveCallback := func(
		source TransferPath,
		provideMode protocol.ProvideMode,
		ipPath *IpPath,
		packet []byte,
	) {
		_, sourceIp, destinationIp, transport, ok := parseIpv4(packet)
		if !ok {
			return
		}
		tcp := &parsedTcp{}
		if !parseTcpPacket(sourceIp, destinationIp, transport, tcp) {
			return
		}
		if tcp.syn {
			select {
			case synAckReceived <- struct{}{}:
			default:
			}
			return
		}
		if len(tcp.payload) == 0 {
			return
		}
		if payloadCallbackCount.Add(1) == 1 {
			close(callbackEntered)
			<-callbackRelease
		}
		payloads <- append([]byte(nil), tcp.payload...)
	}

	sourceIp := net.IPv4(10, 0, 0, 1).To4()
	destinationIp := net.IPv4(203, 0, 113, 7).To4()
	const initialSynSeq = uint32(1000)
	sequence := NewTcpSequence(
		ctx,
		receiveCallback,
		SourceId(NewId()),
		protocol.ProvideMode_Network,
		4,
		sourceIp,
		40001,
		destinationIp,
		443,
		initialSynSeq,
		settings,
	)
	go HandleError(sequence.Run)

	synPacket := MessagePoolGet(Ipv4HeaderSizeWithoutExtensions + TcpHeaderSizeWithoutExtensions)
	success, err := sequence.send(
		&TcpSendItem{
			provideMode: protocol.ProvideMode_Network,
			tcp: parsedTcp{
				syn:        true,
				seq:        initialSynSeq,
				windowSize: 65535,
			},
			ipPacket: synPacket,
		},
		-1,
	)
	if err != nil {
		MessagePoolReturn(synPacket)
		t.Fatalf("send syn: %v", err)
	}
	if !success {
		MessagePoolReturn(synPacket)
		t.Fatal("syn was not accepted")
	}

	select {
	case <-synAckReceived:
	case <-time.After(2 * time.Second):
		t.Fatal("sequence did not establish")
	}

	firstPayload := []byte{1, 2, 3, 4}
	firstWriteDone := make(chan error, 1)
	go func() {
		_, writeErr := upstreamSocket.Write(firstPayload)
		firstWriteDone <- writeErr
	}()
	select {
	case <-callbackEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("receive callback did not start")
	}
	select {
	case writeErr := <-firstWriteDone:
		if writeErr != nil {
			t.Fatalf("first upstream write: %v", writeErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("first upstream write did not reach the socket reader")
	}

	secondPayload := []byte{5, 6, 7, 8}
	secondWriteDone := make(chan error, 1)
	go func() {
		_, writeErr := upstreamSocket.Write(secondPayload)
		secondWriteDone <- writeErr
	}()
	select {
	case writeErr := <-secondWriteDone:
		if writeErr != nil {
			t.Fatalf("second upstream write: %v", writeErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("one-batch read-ahead did not accept the second write")
	}

	thirdPayload := []byte{9, 10, 11, 12}
	thirdWriteDone := make(chan error, 1)
	go func() {
		_, writeErr := upstreamSocket.Write(thirdPayload)
		thirdWriteDone <- writeErr
	}()
	select {
	case writeErr := <-thirdWriteDone:
		if writeErr != nil {
			t.Fatalf("third upstream write: %v", writeErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("socket reader did not reach the bounded queue")
	}

	fourthPayload := []byte{13, 14, 15, 16}
	fourthWriteDone := make(chan error, 1)
	go func() {
		_, writeErr := upstreamSocket.Write(fourthPayload)
		fourthWriteDone <- writeErr
	}()
	select {
	case writeErr := <-fourthWriteDone:
		t.Fatalf("fourth upstream write bypassed callback backpressure: %v", writeErr)
	case <-time.After(50 * time.Millisecond):
	}

	close(callbackRelease)
	select {
	case writeErr := <-fourthWriteDone:
		if writeErr != nil {
			t.Fatalf("fourth upstream write after release: %v", writeErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fourth upstream write did not resume")
	}

	wantPayloads := [][]byte{firstPayload, secondPayload, thirdPayload, fourthPayload}
	for payloadIndex, wantPayload := range wantPayloads {
		select {
		case payload := <-payloads:
			if !bytes.Equal(payload, wantPayload) {
				t.Fatalf("return payload %d=%v, want %v", payloadIndex, payload, wantPayload)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("return payload %d was not delivered", payloadIndex)
		}
	}
}
