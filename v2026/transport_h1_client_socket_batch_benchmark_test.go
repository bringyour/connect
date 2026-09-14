// Client H1 socket benchmarks preserve the production write stack:
// Gorilla WebSocket -> bounded batch wrapper -> TLS -> real loopback TCP.
// Saturated runs measure ready-drain throughput; sparse runs release the next
// message only after application delivery, proving that ready-only batching
// never waits for a second message.
package connect

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const (
	clientH1TlsSocketBenchmarkPayloadByteCount = 1380
	clientH1TlsSocketBenchmarkMaxMessageCount  = 32
)

// Selects whether all messages remain ready or each next message waits for
// application delivery of its predecessor.
type clientH1TlsSocketBenchmarkWorkload int

const (
	clientH1TlsSocketBenchmarkSaturated clientH1TlsSocketBenchmarkWorkload = iota
	clientH1TlsSocketBenchmarkSparse
)

// Selects singleton writes, ready draining alone, or the complete production
// ready-drain plus above-TLS coalescing shape.
type clientH1TlsSocketBenchmarkMode int

const (
	clientH1TlsSocketBenchmarkSingleton clientH1TlsSocketBenchmarkMode = iota
	clientH1TlsSocketBenchmarkReadySeparate
	clientH1TlsSocketBenchmarkReadyCoalesced
)

// Configures one writer shape and arrival pattern.
type clientH1TlsSocketBenchmarkSettings struct {
	mode              clientH1TlsSocketBenchmarkMode
	workload          clientH1TlsSocketBenchmarkWorkload
	maxMessageCount   int
	maxBatchByteCount int
	payloadByteCount  int
	// ipVersion selects the loopback family the server binds; 0 is v4.
	ipVersion int
}

// Counts client TCP writes and parses complete outbound TLS records. The
// connection sits below tls.Conn, exactly where production reaches the socket.
type clientH1TlsSocketBenchmarkConn struct {
	net.Conn

	tcpWriteCount atomic.Uint64

	tlsRecordStateLock     sync.Mutex
	tlsRecordHeader        [5]byte
	tlsRecordHeaderLen     int
	tlsRecordPayloadRemain int
	tlsRecordCount         uint64
}

// Records the bytes successfully handed to the real loopback TCP connection.
func (self *clientH1TlsSocketBenchmarkConn) Write(buffer []byte) (int, error) {
	self.tcpWriteCount.Add(1)
	writtenByteCount, err := self.Conn.Write(buffer)
	if 0 < writtenByteCount {
		self.noteTlsBytes(buffer[:writtenByteCount])
	}
	return writtenByteCount, err
}

// Parses TLS record boundaries across arbitrary socket write splits.
func (self *clientH1TlsSocketBenchmarkConn) noteTlsBytes(buffer []byte) {
	self.tlsRecordStateLock.Lock()
	defer self.tlsRecordStateLock.Unlock()
	for len(buffer) != 0 {
		if 0 < self.tlsRecordPayloadRemain {
			consumedByteCount := min(self.tlsRecordPayloadRemain, len(buffer))
			self.tlsRecordPayloadRemain -= consumedByteCount
			buffer = buffer[consumedByteCount:]
			continue
		}
		if self.tlsRecordHeaderLen < len(self.tlsRecordHeader) {
			copiedByteCount := copy(
				self.tlsRecordHeader[self.tlsRecordHeaderLen:],
				buffer,
			)
			self.tlsRecordHeaderLen += copiedByteCount
			buffer = buffer[copiedByteCount:]
			if self.tlsRecordHeaderLen < len(self.tlsRecordHeader) {
				continue
			}
			self.tlsRecordPayloadRemain =
				int(self.tlsRecordHeader[3])*256 + int(self.tlsRecordHeader[4])
			self.tlsRecordHeaderLen = 0
			self.tlsRecordCount += 1
		}
	}
}

// Snapshots the number of complete outbound TLS record headers.
func (self *clientH1TlsSocketBenchmarkConn) loadTlsRecordCount() uint64 {
	self.tlsRecordStateLock.Lock()
	defer self.tlsRecordStateLock.Unlock()
	return self.tlsRecordCount
}

// Reports whether the observed outbound byte stream ends on a record boundary.
func (self *clientH1TlsSocketBenchmarkConn) tlsRecordStateComplete() bool {
	self.tlsRecordStateLock.Lock()
	defer self.tlsRecordStateLock.Unlock()
	return self.tlsRecordHeaderLen == 0 && self.tlsRecordPayloadRemain == 0
}

// Carries writer completion and its ready-drain accounting.
type clientH1TlsSocketBenchmarkWriterResult struct {
	err                   error
	readyBatchCount       int
	writeDeadlineSetCount int
}

// Runs one client-outbound transfer through the same wrapper order that
// clientDialer.WsDialer constructs in production.
func benchmarkClientH1TlsSocket(
	b *testing.B,
	settings clientH1TlsSocketBenchmarkSettings,
) {
	b.Helper()
	payloadByteCount := settings.payloadByteCount
	if payloadByteCount == 0 {
		payloadByteCount = clientH1TlsSocketBenchmarkPayloadByteCount
	}
	b.SetBytes(int64(payloadByteCount))
	maxMessageCount := settings.maxMessageCount
	if maxMessageCount == 0 {
		maxMessageCount = platformWebSocketWriteBatchMaxMessages
	}
	maxBatchByteCount := settings.maxBatchByteCount
	if settings.maxMessageCount == 0 && maxBatchByteCount == 0 {
		maxBatchByteCount = platformWebSocketWriteBatchDrainByteCount
	}
	if maxMessageCount < 1 || clientH1TlsSocketBenchmarkMaxMessageCount < maxMessageCount {
		b.Fatalf("invalid maximum message count %d", maxMessageCount)
	}

	payload := make([]byte, payloadByteCount)
	for index := range payload {
		payload[index] = byte(index)
	}

	benchmarkCtx, benchmarkCancel := context.WithCancel(context.Background())
	start := make(chan struct{})
	handlerRelease := make(chan struct{})
	var handlerReleaseOnce sync.Once
	releaseHandler := func() {
		handlerReleaseOnce.Do(func() {
			close(handlerRelease)
		})
	}
	delivered := make(chan struct{}, 1)
	serverResult := make(chan error, 1)

	upgrader := websocket.Upgrader{
		ReadBufferSize:  4 * 1024,
		WriteBufferSize: 4 * 1024,
		CheckOrigin: func(request *http.Request) bool {
			return true
		},
	}
	handler := http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		connection, err := upgrader.Upgrade(response, request, nil)
		if err != nil {
			serverResult <- fmt.Errorf("upgrade: %w", err)
			return
		}
		defer connection.Close()
		select {
		case <-benchmarkCtx.Done():
			serverResult <- benchmarkCtx.Err()
			return
		case <-start:
		}

		receivedPayload := make([]byte, len(payload))
		for range b.N {
			messageType, reader, readErr := connection.NextReader()
			if readErr != nil {
				serverResult <- readErr
				return
			}
			if messageType != websocket.BinaryMessage {
				serverResult <- fmt.Errorf("unexpected message type %d", messageType)
				return
			}
			if _, readErr = io.ReadFull(reader, receivedPayload); readErr != nil {
				serverResult <- readErr
				return
			}
			if receivedPayload[0] != payload[0] ||
				receivedPayload[len(receivedPayload)-1] != payload[len(payload)-1] {
				serverResult <- errors.New("received payload changed")
				return
			}
			var extra [1]byte
			extraByteCount, extraErr := reader.Read(extra[:])
			if extraByteCount != 0 || !errors.Is(extraErr, io.EOF) {
				serverResult <- fmt.Errorf(
					"message boundary extra bytes=%d err=%v",
					extraByteCount,
					extraErr,
				)
				return
			}
			if settings.workload == clientH1TlsSocketBenchmarkSparse {
				delivered <- struct{}{}
			}
		}
		serverResult <- nil
		select {
		case <-benchmarkCtx.Done():
		case <-handlerRelease:
		}
	})

	ipVersion := settings.ipVersion
	if ipVersion == 0 {
		ipVersion = 4
	}
	testServer := newTestingLoopbackHttpServer(b, ipVersion, handler, true)
	b.Cleanup(testServer.Close)

	serverTransport, ok := testServer.Client().Transport.(*http.Transport)
	if !ok || serverTransport.TLSClientConfig == nil {
		benchmarkCancel()
		b.Fatalf("unexpected TLS test transport %T", testServer.Client().Transport)
	}
	clientTlsConfig := serverTransport.TLSClientConfig.Clone()
	clientTlsConfig.NextProtos = append([]string{}, clientWebSocketNextProtos...)
	serverName, _, err := net.SplitHostPort(testServer.Listener.Addr().String())
	if err != nil {
		benchmarkCancel()
		b.Fatal(err)
	}
	clientTlsConfig.ServerName = serverName
	socketConnections := make(chan *clientH1TlsSocketBenchmarkConn, 1)
	netDialer := &net.Dialer{}
	dialTlsContext := func(
		ctx context.Context,
		network string,
		address string,
	) (net.Conn, error) {
		rawConnection, err := netDialer.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		socketConnection := &clientH1TlsSocketBenchmarkConn{Conn: rawConnection}
		tlsConnection := tls.Client(socketConnection, clientTlsConfig.Clone())
		if err = tlsConnection.HandshakeContext(ctx); err != nil {
			rawConnection.Close()
			return nil, err
		}
		socketConnections <- socketConnection
		return tlsConnection, nil
	}
	clientStrategySettings := DefaultClientStrategySettings()
	clientStrategySettings.TlsConfig = clientTlsConfig
	dialer := &clientDialer{
		dialTlsContext: dialTlsContext,
		settings:       clientStrategySettings,
	}
	clientConnection, response, err := dialer.WsDialer(clientStrategySettings).DialContext(
		benchmarkCtx,
		"wss"+strings.TrimPrefix(testServer.URL, "https"),
		nil,
	)
	if response != nil && response.Body != nil {
		defer response.Body.Close()
	}
	if err != nil {
		benchmarkCancel()
		b.Fatal(err)
	}
	b.Cleanup(func() {
		releaseHandler()
		benchmarkCancel()
		clientConnection.Close()
	})

	batchConnection, ok := clientConnection.UnderlyingConn().(*WebSocketWriteBatchConn)
	if !ok {
		b.Fatalf("unexpected client WebSocket connection %T", clientConnection.UnderlyingConn())
	}
	if _, ok = batchConnection.conn.(*tls.Conn); !ok {
		b.Fatalf("batch wrapper is not above TLS: underlying=%T", batchConnection.conn)
	}
	socketConnection := <-socketConnections
	if !socketConnection.tlsRecordStateComplete() {
		b.Fatal("TLS upgrade did not finish on a complete outbound record boundary")
	}
	if settings.mode == clientH1TlsSocketBenchmarkReadyCoalesced {
		// Exclude the production wrapper's one per-connection allocation.
		batchConnection.BeginWriteBatch()
		batchConnection.AbortWriteBatch()
	}
	tcpWriteCountBefore := socketConnection.tcpWriteCount.Load()
	tlsRecordCountBefore := socketConnection.loadTlsRecordCount()

	queueSize := DefaultPlatformTransportSettings().TransportBufferSize
	if settings.workload == clientH1TlsSocketBenchmarkSparse {
		queueSize = 0
	}
	send := make(chan []byte, queueSize)
	preloadedMessageCount := 0
	if settings.workload == clientH1TlsSocketBenchmarkSaturated {
		preloadedMessageCount = min(b.N, queueSize)
	}
	for range preloadedMessageCount {
		send <- payload
	}
	writerResult := make(chan clientH1TlsSocketBenchmarkWriterResult, 1)
	go func() {
		select {
		case <-benchmarkCtx.Done():
			writerResult <- clientH1TlsSocketBenchmarkWriterResult{err: benchmarkCtx.Err()}
			return
		case <-start:
		}

		writtenMessageCount := 0
		readyBatchCount := 0
		writeDeadlineSetCount := 0
		var messageStorage [clientH1TlsSocketBenchmarkMaxMessageCount][]byte
		for writtenMessageCount < b.N {
			var firstMessage []byte
			select {
			case <-benchmarkCtx.Done():
				writerResult <- clientH1TlsSocketBenchmarkWriterResult{
					err:                   benchmarkCtx.Err(),
					readyBatchCount:       readyBatchCount,
					writeDeadlineSetCount: writeDeadlineSetCount,
				}
				return
			case firstMessage = <-send:
			}

			messages := messageStorage[:1:maxMessageCount]
			messages[0] = firstMessage
			batchMessageByteCount := len(firstMessage)
			if settings.mode != clientH1TlsSocketBenchmarkSingleton {
			drainReady:
				for len(messages) < cap(messages) &&
					(maxBatchByteCount <= 0 || batchMessageByteCount < maxBatchByteCount) &&
					writtenMessageCount+len(messages) < b.N {
					select {
					case <-benchmarkCtx.Done():
						writerResult <- clientH1TlsSocketBenchmarkWriterResult{
							err:                   benchmarkCtx.Err(),
							readyBatchCount:       readyBatchCount,
							writeDeadlineSetCount: writeDeadlineSetCount,
						}
						return
					case message := <-send:
						messages = append(messages, message)
						batchMessageByteCount += len(message)
					default:
						break drainReady
					}
				}
			}

			readyBatchCount += 1
			clientConnection.SetWriteDeadline(time.Now().Add(30 * time.Second))
			writeDeadlineSetCount += 1
			if settings.mode == clientH1TlsSocketBenchmarkReadyCoalesced {
				batchConnection.BeginWriteBatch()
			}
			for _, message := range messages {
				if writeErr := clientConnection.WriteMessage(
					websocket.BinaryMessage,
					message,
				); writeErr != nil {
					if settings.mode == clientH1TlsSocketBenchmarkReadyCoalesced {
						batchConnection.AbortWriteBatch()
					}
					writerResult <- clientH1TlsSocketBenchmarkWriterResult{
						err:                   writeErr,
						readyBatchCount:       readyBatchCount,
						writeDeadlineSetCount: writeDeadlineSetCount,
					}
					return
				}
			}
			if settings.mode == clientH1TlsSocketBenchmarkReadyCoalesced {
				if flushErr := batchConnection.FlushWriteBatch(); flushErr != nil {
					writerResult <- clientH1TlsSocketBenchmarkWriterResult{
						err:                   flushErr,
						readyBatchCount:       readyBatchCount,
						writeDeadlineSetCount: writeDeadlineSetCount,
					}
					return
				}
			}
			writtenMessageCount += len(messages)
		}
		writerResult <- clientH1TlsSocketBenchmarkWriterResult{
			readyBatchCount:       readyBatchCount,
			writeDeadlineSetCount: writeDeadlineSetCount,
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	close(start)
	serverCompleted := false
	if settings.workload == clientH1TlsSocketBenchmarkSparse {
		for range b.N {
			select {
			case send <- payload:
			case serverErr := <-serverResult:
				b.StopTimer()
				b.Fatalf("server stopped while releasing sparse frame: %v", serverErr)
			}
			select {
			case <-delivered:
			case serverErr := <-serverResult:
				if serverErr != nil {
					b.StopTimer()
					b.Fatalf("server stopped before sparse delivery: %v", serverErr)
				}
				// Successful completion is published after the final delivery
				// edge. Consume that already-ready edge instead of treating the
				// select's arbitrary choice as a benchmark failure.
				<-delivered
				serverCompleted = true
			}
		}
	} else {
		for range b.N - preloadedMessageCount {
			select {
			case send <- payload:
			case result := <-writerResult:
				b.StopTimer()
				b.Fatalf("writer stopped while producing: %v", result.err)
			case serverErr := <-serverResult:
				b.StopTimer()
				b.Fatalf("server stopped while producing: %v", serverErr)
			}
		}
	}
	result := <-writerResult
	var serverErr error
	if !serverCompleted {
		serverErr = <-serverResult
	}
	b.StopTimer()

	if result.err != nil {
		b.Fatal(result.err)
	}
	if serverErr != nil {
		b.Fatal(serverErr)
	}
	tcpWriteCount := socketConnection.tcpWriteCount.Load() - tcpWriteCountBefore
	tlsRecordCount := socketConnection.loadTlsRecordCount() - tlsRecordCountBefore
	if !socketConnection.tlsRecordStateComplete() {
		b.Fatal("measured write stream ended inside a TLS record")
	}
	if tcpWriteCount == 0 || tlsRecordCount == 0 || result.readyBatchCount == 0 {
		b.Fatalf(
			"empty accounting: TCP=%d TLS=%d ready batches=%d",
			tcpWriteCount,
			tlsRecordCount,
			result.readyBatchCount,
		)
	}
	if settings.mode == clientH1TlsSocketBenchmarkReadyCoalesced {
		// Go TLS adaptively starts with smaller application records, so the
		// first coalesced write may span more than one record. Once warmed, each
		// bounded batch normally fits one record. Preserve that distinction in
		// the reported counts instead of assuming one record per Write call.
		if tlsRecordCount < uint64(result.readyBatchCount) ||
			uint64(b.N) < tlsRecordCount {
			b.Fatalf(
				"coalesced TLS records=%d outside ready-batch/frame bounds [%d,%d]",
				tlsRecordCount,
				result.readyBatchCount,
				b.N,
			)
		}
	} else if tlsRecordCount != uint64(b.N) {
		b.Fatalf("separate TLS records=%d, want one per frame (%d)", tlsRecordCount, b.N)
	}
	if settings.workload == clientH1TlsSocketBenchmarkSparse &&
		result.readyBatchCount != b.N {
		b.Fatalf(
			"sparse ready batches=%d, want exactly one per frame (%d)",
			result.readyBatchCount,
			b.N,
		)
	}
	if settings.workload == clientH1TlsSocketBenchmarkSparse {
		b.ReportMetric(
			float64(b.Elapsed().Nanoseconds())/float64(b.N),
			"delivery-ns/frame",
		)
	}
	b.ReportMetric(
		float64(tcpWriteCount)/float64(b.N),
		"tcp-writes/frame",
	)
	b.ReportMetric(
		float64(b.N)/float64(tcpWriteCount),
		"frames/tcp-write",
	)
	b.ReportMetric(
		float64(tlsRecordCount)/float64(b.N),
		"tls-records/frame",
	)
	b.ReportMetric(
		float64(b.N)/float64(tlsRecordCount),
		"frames/tls-record",
	)
	b.ReportMetric(
		float64(tlsRecordCount)-float64(result.readyBatchCount),
		"tls-split-records/run",
	)
	b.ReportMetric(
		float64(b.N)/float64(result.readyBatchCount),
		"frames/ready-batch",
	)
	b.ReportMetric(
		float64(result.writeDeadlineSetCount)/float64(b.N),
		"write-deadlines/frame",
	)
	releaseHandler()
}

// benchmarkClientH1TlsSocketDualStack runs the benchmark once per ip family.
func benchmarkClientH1TlsSocketDualStack(
	b *testing.B,
	settings clientH1TlsSocketBenchmarkSettings,
) {
	forEachIpVersionBenchmark(b, func(b *testing.B, ipVersion int) {
		settings.ipVersion = ipVersion
		benchmarkClientH1TlsSocket(b, settings)
	})
}

// Measures the historical one-frame-per-deadline and TLS-write baseline.
func BenchmarkClientH1TlsSocketSingletonSaturated(b *testing.B) {
	benchmarkClientH1TlsSocketDualStack(b, clientH1TlsSocketBenchmarkSettings{
		mode:     clientH1TlsSocketBenchmarkSingleton,
		workload: clientH1TlsSocketBenchmarkSaturated,
	})
}

// Isolates scheduler/deadline gains from ready draining without coalescing.
func BenchmarkClientH1TlsSocketReadyDrainSeparateSaturated(b *testing.B) {
	benchmarkClientH1TlsSocketDualStack(b, clientH1TlsSocketBenchmarkSettings{
		mode:     clientH1TlsSocketBenchmarkReadySeparate,
		workload: clientH1TlsSocketBenchmarkSaturated,
	})
}

// Measures the current production ready-drain and above-TLS coalescing path.
func BenchmarkClientH1TlsSocketProductionCoalescedSaturated(b *testing.B) {
	benchmarkClientH1TlsSocketDualStack(b, clientH1TlsSocketBenchmarkSettings{
		mode:     clientH1TlsSocketBenchmarkReadyCoalesced,
		workload: clientH1TlsSocketBenchmarkSaturated,
	})
}

// Measures four-message TLS coalescing with every other axis fixed.
func BenchmarkClientH1TlsSocketCoalescedBatch4Saturated(b *testing.B) {
	benchmarkClientH1TlsSocketDualStack(b, clientH1TlsSocketBenchmarkSettings{
		mode:            clientH1TlsSocketBenchmarkReadyCoalesced,
		workload:        clientH1TlsSocketBenchmarkSaturated,
		maxMessageCount: 4,
	})
}

// Measures eight-message TLS coalescing with every other axis fixed.
func BenchmarkClientH1TlsSocketCoalescedBatch8Saturated(b *testing.B) {
	benchmarkClientH1TlsSocketDualStack(b, clientH1TlsSocketBenchmarkSettings{
		mode:            clientH1TlsSocketBenchmarkReadyCoalesced,
		workload:        clientH1TlsSocketBenchmarkSaturated,
		maxMessageCount: 8,
	})
}

// Measures whether a sixteen-message ready drain buys more ACK-heavy
// throughput without changing the fixed 16-KiB coalescing storage.
func BenchmarkClientH1TlsSocketCoalescedBatch16Saturated(b *testing.B) {
	benchmarkClientH1TlsSocketDualStack(b, clientH1TlsSocketBenchmarkSettings{
		mode:            clientH1TlsSocketBenchmarkReadyCoalesced,
		workload:        clientH1TlsSocketBenchmarkSaturated,
		maxMessageCount: 16,
	})
}

// Measures the production count ceiling while retaining the same 12-KiB
// ready-byte and fixed 16-KiB coalescing-storage bounds.
func BenchmarkClientH1TlsSocketCoalescedBatch32Saturated(b *testing.B) {
	benchmarkClientH1TlsSocketDualStack(b, clientH1TlsSocketBenchmarkSettings{
		mode:              clientH1TlsSocketBenchmarkReadyCoalesced,
		workload:          clientH1TlsSocketBenchmarkSaturated,
		maxMessageCount:   32,
		maxBatchByteCount: platformWebSocketWriteBatchDrainByteCount,
	})
}

func BenchmarkClientH1TlsSocketAckSizedBatch8Saturated(b *testing.B) {
	benchmarkClientH1TlsSocketDualStack(b, clientH1TlsSocketBenchmarkSettings{
		mode:             clientH1TlsSocketBenchmarkReadyCoalesced,
		workload:         clientH1TlsSocketBenchmarkSaturated,
		maxMessageCount:  8,
		payloadByteCount: 128,
	})
}

func BenchmarkClientH1TlsSocketAckSizedBatch16Saturated(b *testing.B) {
	benchmarkClientH1TlsSocketDualStack(b, clientH1TlsSocketBenchmarkSettings{
		mode:             clientH1TlsSocketBenchmarkReadyCoalesced,
		workload:         clientH1TlsSocketBenchmarkSaturated,
		maxMessageCount:  16,
		payloadByteCount: 128,
	})
}

func BenchmarkClientH1TlsSocketAckSizedBatch32Saturated(b *testing.B) {
	benchmarkClientH1TlsSocketDualStack(b, clientH1TlsSocketBenchmarkSettings{
		mode:              clientH1TlsSocketBenchmarkReadyCoalesced,
		workload:          clientH1TlsSocketBenchmarkSaturated,
		maxMessageCount:   32,
		maxBatchByteCount: platformWebSocketWriteBatchDrainByteCount,
		payloadByteCount:  128,
	})
}

func BenchmarkClientH1TlsSocketAckSizedProductionSaturated(b *testing.B) {
	benchmarkClientH1TlsSocketDualStack(b, clientH1TlsSocketBenchmarkSettings{
		mode:             clientH1TlsSocketBenchmarkReadyCoalesced,
		workload:         clientH1TlsSocketBenchmarkSaturated,
		payloadByteCount: 128,
	})
}

// Measures isolated-frame latency with historical singleton writes.
func BenchmarkClientH1TlsSocketSingletonSparse(b *testing.B) {
	benchmarkClientH1TlsSocketDualStack(b, clientH1TlsSocketBenchmarkSettings{
		mode:     clientH1TlsSocketBenchmarkSingleton,
		workload: clientH1TlsSocketBenchmarkSparse,
	})
}

// Proves ready draining alone does not wait for an absent second message.
func BenchmarkClientH1TlsSocketReadyDrainSeparateSparse(b *testing.B) {
	benchmarkClientH1TlsSocketDualStack(b, clientH1TlsSocketBenchmarkSettings{
		mode:     clientH1TlsSocketBenchmarkReadySeparate,
		workload: clientH1TlsSocketBenchmarkSparse,
	})
}

// Measures current production behavior for an isolated above-TLS frame.
func BenchmarkClientH1TlsSocketProductionCoalescedSparse(b *testing.B) {
	benchmarkClientH1TlsSocketDualStack(b, clientH1TlsSocketBenchmarkSettings{
		mode:     clientH1TlsSocketBenchmarkReadyCoalesced,
		workload: clientH1TlsSocketBenchmarkSparse,
	})
}
