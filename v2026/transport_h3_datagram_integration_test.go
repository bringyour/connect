package connect

// This file verifies the live PlatformTransport split: authentication and
// liveness use the reliable QUIC stream while routed frames use DATAGRAM.

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"

	"github.com/urnetwork/connect/v2026/protocol"
)

// New/new peers negotiate the versioned hybrid carrier. Small routed frames
// use DATAGRAM, large frames use the authenticated reliable stream, and both
// lanes deliver through the same bounded route boundary in both directions.
func TestPlatformTransportH3DatagramRoundTrip(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		const hybridStreamIdle = 75 * time.Millisecond
		certPem, keyPem, err := selfSign(
			[]string{testLoopbackIp(ipVersion)},
			testLoopbackIp(ipVersion),
			24*time.Hour,
			24*time.Hour,
		)
		if err != nil {
			t.Fatal(err)
		}
		cert, err := tls.X509KeyPair(certPem, keyPem)
		if err != nil {
			t.Fatal(err)
		}
		const nextProto = "urnetwork-h3-datagram-test"
		listener, err := quic.ListenAddrEarly(
			testLoopbackHostPort(ipVersion, 0),
			&tls.Config{
				Certificates: []tls.Certificate{cert},
				NextProtos:   []string{nextProto},
			},
			&quic.Config{EnableDatagrams: true, MaxIdleTimeout: 30 * time.Second},
		)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = listener.Close()
		})

		testCtx, testCancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer testCancel()
		serverErrors := make(chan error, 4)
		serverReceivedMessages := make(chan []byte, 2)
		serverSentMessages := make(chan struct{}, 1)
		serverCanClose := make(chan struct{})
		framerSettings := DefaultFramerSettings(int(DefaultClientSettings().MinimumMessageLenLimit()))
		datagramSettings := DefaultH3DatagramSettings()
		sequenceId := NewId()
		packBytes, err := ProtoMarshal(&protocol.TransferFrame{
			TransferPath: &protocol.TransferPath{DestinationId: NewId().Bytes()},
			Pack: &protocol.Pack{
				MessageId:      NewId().Bytes(),
				SequenceId:     sequenceId.Bytes(),
				SequenceNumber: 1,
				Head:           true,
				Frames: []*protocol.Frame{{
					MessageType:  protocol.MessageType_TestSimpleMessage,
					MessageBytes: bytes.Repeat([]byte("client-pack-payload-"), 140),
				}},
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		clientMessage := bytes.Clone(packBytes)
		MessagePoolReturn(packBytes)
		ackBytes, err := ProtoMarshal(&protocol.TransferFrame{
			TransferPath: &protocol.TransferPath{DestinationId: NewId().Bytes()},
			Ack: &protocol.Ack{
				MessageId:  NewId().Bytes(),
				SequenceId: sequenceId.Bytes(),
				Selective:  true,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		serverMessage := bytes.Clone(ackBytes)
		MessagePoolReturn(ackBytes)
		if datagramSettings.UseDatagram(len(clientMessage)) {
			t.Fatalf("large fixture unexpectedly selected DATAGRAM: %d bytes", len(clientMessage))
		}
		if !datagramSettings.UseDatagram(len(serverMessage)) {
			t.Fatalf("small fixture unexpectedly selected stream: %d bytes", len(serverMessage))
		}

		go func() {
			connection, acceptErr := listener.Accept(testCtx)
			if acceptErr != nil {
				serverErrors <- acceptErr
				return
			}
			defer connection.CloseWithError(0, "test complete")
			stream, acceptErr := connection.AcceptStream(testCtx)
			if acceptErr != nil {
				serverErrors <- acceptErr
				return
			}
			framer := NewFramer(framerSettings)
			authBytes, readErr := framer.Read(stream)
			if readErr != nil {
				serverErrors <- readErr
				return
			}
			defer MessagePoolReturn(authBytes)
			decoded, decodeErr := DecodeFrame(authBytes)
			if decodeErr != nil {
				serverErrors <- decodeErr
				return
			}
			auth, ok := decoded.(*protocol.Auth)
			if !ok {
				serverErrors <- fmt.Errorf("auth type=%T", decoded)
				return
			}
			connectionState := connection.ConnectionState()
			authResponse, accepted := AcceptH3DatagramAuthOffer(
				auth,
				true,
				connectionState.SupportsDatagrams.Local,
				connectionState.SupportsDatagrams.Remote,
			)
			if !accepted {
				serverErrors <- fmt.Errorf("server did not negotiate H3 DATAGRAM: %+v", connectionState.SupportsDatagrams)
				return
			}
			responseBytes, encodeErr := EncodeFrame(authResponse, DefaultProtocolVersion)
			if encodeErr != nil {
				serverErrors <- encodeErr
				return
			}
			writeErr := framer.Write(stream, responseBytes)
			MessagePoolReturn(responseBytes)
			if writeErr != nil {
				serverErrors <- writeErr
				return
			}

			stats := &H3DatagramStats{}
			fragmenter, fragmenterErr := NewH3DatagramFragmenter(datagramSettings, stats)
			if fragmenterErr != nil {
				serverErrors <- fragmenterErr
				return
			}
			budget := NewH3DatagramReassemblyBudget(datagramSettings.ProcessReassemblyByteCount)
			reassembler, reassemblerErr := NewH3DatagramReassembler(datagramSettings, budget, stats)
			if reassemblerErr != nil {
				serverErrors <- reassemblerErr
				return
			}
			defer reassembler.Close()
			if _, sendErr := fragmenter.Send(serverMessage, datagramSettings.TargetDatagramByteCount, connection.SendDatagram); sendErr != nil {
				serverErrors <- sendErr
				return
			}
			largeMessage, readErr := framer.Read(stream)
			if readErr != nil {
				serverErrors <- readErr
				return
			}
			if !bytes.Equal(largeMessage, clientMessage) {
				MessagePoolReturn(largeMessage)
				serverErrors <- fmt.Errorf("server stream received %d bytes, want %d", len(largeMessage), len(clientMessage))
				return
			}
			serverReceivedMessages <- bytes.Clone(largeMessage)
			MessagePoolReturn(largeMessage)
			for {
				datagram, receiveErr := connection.ReceiveDatagram(testCtx)
				if receiveErr != nil {
					serverErrors <- receiveErr
					return
				}
				message := reassembler.Accept(datagram, time.Now())
				if message == nil {
					continue
				}
				if !bytes.Equal(message, serverMessage) {
					MessagePoolReturn(message)
					serverErrors <- fmt.Errorf("server DATAGRAM received %d bytes, want %d", len(message), len(serverMessage))
					return
				}
				serverReceivedMessages <- bytes.Clone(message)
				MessagePoolReturn(message)
				break
			}
			// Leave the reliable lane idle for longer than the application's normal
			// stream read deadline while the negotiated DATAGRAM lane remains usable.
			// Hybrid liveness belongs to the QUIC connection, not to an empty stream.
			time.Sleep(3 * hybridStreamIdle)
			stream.SetWriteDeadline(time.Now().Add(time.Second))
			if writeErr := framer.Write(stream, clientMessage); writeErr != nil {
				serverErrors <- writeErr
				return
			}
			serverSentMessages <- struct{}{}
			select {
			case <-serverCanClose:
			case <-testCtx.Done():
			}
		}()

		settings := testingPlatformTransportSettings()
		settings.H3Port = listener.Addr().(*net.UDPAddr).Port
		settings.resolveH3AddrsForTest = testingH3LoopbackResolver(ipVersion, settings.H3Port)
		settings.QuicTlsConfig = &tls.Config{
			InsecureSkipVerify: true, // test-only self-signed endpoint
			NextProtos:         []string{nextProto},
		}
		settings.FramerSettings = framerSettings
		settings.EnableH3Datagrams = true
		settings.H3DatagramSettings = datagramSettings
		settings.PingTimeout = 30 * time.Second
		settings.ReadTimeout = hybridStreamIdle
		streamWriteEntered := make(chan struct{})
		releaseStreamWrite := make(chan struct{})
		var streamWriteEnteredOnce sync.Once
		var releaseStreamWriteOnce sync.Once
		settings.beforeH3StreamWriteForTest = func() {
			streamWriteEnteredOnce.Do(func() {
				close(streamWriteEntered)
			})
			select {
			case <-releaseStreamWrite:
			case <-testCtx.Done():
			}
		}
		t.Cleanup(func() {
			releaseStreamWriteOnce.Do(func() {
				close(releaseStreamWrite)
			})
		})
		datagramSendObserved := make(chan struct{}, 1)
		settings.H3SendLaneObserver = func(_ []byte, datagram bool) {
			if datagram {
				select {
				case datagramSendObserved <- struct{}{}:
				default:
				}
			}
		}
		sendRoutes := make(chan Route, 1)
		settings.SendRouteObserver = func(_ Transport, route Route, connected bool) {
			if !connected {
				return
			}
			select {
			case sendRoutes <- route:
			default:
			}
		}
		clientReceivedMessages := make(chan []byte, 2)
		settings.afterH3ReceiveEnqueueForTest = func(message []byte) {
			select {
			case clientReceivedMessages <- bytes.Clone(message):
			default:
			}
		}

		routeManager := NewRouteManager(testCtx, "h3-datagram-round-trip")
		// Production always has the Client receive pump attached. Hybrid reliable
		// stream admission is intentionally unbuffered, so this carrier integration
		// must model that consumer instead of relying on the old route queue to hold
		// a stream frame indefinitely.
		reader := routeManager.OpenMultiRouteReader(DestinationId(NewId()))
		readerDone := make(chan struct{})
		go func() {
			defer close(readerDone)
			for {
				message, readErr := reader.Read(testCtx, time.Second)
				if message != nil {
					MessagePoolReturn(message)
				}
				if readErr != nil {
					return
				}
			}
		}()
		t.Cleanup(func() {
			routeManager.CloseMultiRouteReader(reader)
			<-readerDone
		})
		transport := NewPlatformTransportWithTargetMode(
			testCtx,
			NewClientStrategyWithDefaults(testCtx),
			routeManager,
			"https://"+testLoopbackHost(ipVersion),
			&ClientAuth{ByJwt: "testing", InstanceId: NewId(), AppVersion: "testing"},
			TransportModeH3,
			settings,
		)
		t.Cleanup(transport.Close)

		select {
		case <-sendRoutes:
		case serverErr := <-serverErrors:
			t.Fatal(serverErr)
		case <-testCtx.Done():
			t.Fatalf("wait for negotiated send route: %v", testCtx.Err())
		}
		writer := routeManager.OpenMultiRouteWriter(DestinationId(NewId()))
		t.Cleanup(func() {
			routeManager.CloseMultiRouteWriter(writer)
		})
		if policy := writer.(transferFlightPolicyProvider).transferFlightPolicy(); !policy.limited {
			t.Fatal("negotiated H3 DATAGRAM route did not publish unreliable Transfer semantics")
		}
		carrierWriter := writer.(transferCarrierMultiRouteWriter)
		writeHybridMessage := func(
			message []byte,
			wantUnreliable bool,
			wantReliable bool,
			wantHybridReliable bool,
		) {
			t.Helper()
			pooledMessage := MessagePoolCopy(message)
			success, disposition, writeErr := carrierWriter.writeDetailedWithCarrier(
				testCtx,
				pooledMessage,
				time.Second,
			)
			if writeErr != nil || !success {
				MessagePoolReturn(pooledMessage)
				t.Fatalf("write hybrid message = (%t, %+v, %v)", success, disposition, writeErr)
			}
			if disposition.transportType != TransportTypeH3 ||
				disposition.unreliable != wantUnreliable ||
				disposition.reliable != wantReliable ||
				disposition.hybridReliable != wantHybridReliable {
				t.Fatalf(
					"hybrid disposition = %+v, want H3 unreliable=%t reliable=%t hybrid_reliable=%t",
					disposition,
					wantUnreliable,
					wantReliable,
					wantHybridReliable,
				)
			}
		}
		writeHybridMessage(clientMessage, false, true, true)
		select {
		case <-streamWriteEntered:
		case serverErr := <-serverErrors:
			t.Fatal(serverErr)
		case <-testCtx.Done():
			t.Fatalf("wait for held hybrid stream write: %v", testCtx.Err())
		}
		writeHybridMessage(serverMessage, true, false, false)
		select {
		case <-datagramSendObserved:
		case serverErr := <-serverErrors:
			t.Fatal(serverErr)
		case <-testCtx.Done():
			t.Fatalf("DATAGRAM did not pass held hybrid stream write: %v", testCtx.Err())
		}
		heldStats := transport.DatagramStats()
		if heldStats.HybridStreamQueueCurrentMessageCount != 1 ||
			heldStats.HybridStreamQueueCurrentByteCount == 0 ||
			H3HybridStreamQueueByteCount < heldStats.HybridStreamQueueCurrentByteCount ||
			heldStats.HybridStreamQueueMaximumByteCount != heldStats.HybridStreamQueueCurrentByteCount {
			t.Fatalf("held hybrid stream queue stats=%+v", heldStats)
		}
		releaseStreamWriteOnce.Do(func() {
			close(releaseStreamWrite)
		})

		clientReceived := map[string]bool{}
		for range 2 {
			select {
			case received := <-clientReceivedMessages:
				clientReceived[string(received)] = true
			case serverErr := <-serverErrors:
				t.Fatal(serverErr)
			case <-testCtx.Done():
				t.Fatalf("wait for client hybrid messages: %v", testCtx.Err())
			}
		}
		if !clientReceived[string(serverMessage)] || !clientReceived[string(clientMessage)] {
			t.Fatalf("client did not receive both hybrid lanes: %d messages", len(clientReceived))
		}
		close(serverCanClose)
		serverReceived := map[string]bool{}
		for range 2 {
			select {
			case received := <-serverReceivedMessages:
				serverReceived[string(received)] = true
			case serverErr := <-serverErrors:
				t.Fatal(serverErr)
			case <-testCtx.Done():
				t.Fatalf("wait for server hybrid messages: %v", testCtx.Err())
			}
		}
		if !serverReceived[string(serverMessage)] || !serverReceived[string(clientMessage)] {
			t.Fatalf("server did not receive both hybrid lanes: %d messages", len(serverReceived))
		}
		select {
		case <-serverSentMessages:
		default:
			t.Fatal("server receive completed before its fragmented send was recorded")
		}

		stats := transport.DatagramStats()
		if stats.SentMessageCount != 1 || stats.ReceivedMessageCount != 1 ||
			stats.SentFragmentCount != 1 || stats.ReceivedFragmentCount != 1 ||
			stats.StreamSentMessageCount != 1 || stats.StreamReceivedMessageCount != 1 ||
			stats.HybridStreamQueueCurrentMessageCount != 0 ||
			stats.HybridStreamQueueCurrentByteCount != 0 ||
			stats.HybridStreamQueueMaximumMessageCount != 1 ||
			H3HybridStreamQueueByteCount < stats.HybridStreamQueueMaximumByteCount ||
			stats.HybridStreamQueueOversizeCount != 0 {
			t.Fatalf("client hybrid stats=%+v", stats)
		}
	})
}
