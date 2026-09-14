// This file measures the H3 writer's production Framer.WriteBatch shape over
// a real loopback QUIC stream while varying only the ready-drain depth.
package connect

import (
	"context"
	"crypto/tls"
	"fmt"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
)

const (
	platformH3BatchBenchmarkMessageByteCount  = 1380
	platformH3BatchBenchmarkBurstMessageCount = 128
)

// Creates one connected bidirectional QUIC stream and consumes a warm frame so
// handshake and stream publication finish before the benchmark timer starts.
func newPlatformH3BatchBenchmarkStream(
	benchmark *testing.B,
	ipVersion int,
) (*quic.Stream, *quic.Stream) {
	benchmark.Helper()

	certificatePem, keyPem, err := selfSign(
		[]string{testLoopbackIp(ipVersion)},
		testLoopbackIp(ipVersion),
		24*time.Hour,
		24*time.Hour,
	)
	if err != nil {
		benchmark.Fatal(err)
	}
	certificate, err := tls.X509KeyPair(certificatePem, keyPem)
	if err != nil {
		benchmark.Fatal(err)
	}
	const nextProtocol = "urnetwork-h3-batch-benchmark"
	listener, err := quic.ListenAddr(
		testLoopbackHostPort(ipVersion, 0),
		&tls.Config{
			Certificates: []tls.Certificate{certificate},
			NextProtos:   []string{nextProtocol},
		},
		&quic.Config{MaxIdleTimeout: time.Minute},
	)
	if err != nil {
		benchmark.Fatal(err)
	}

	benchmarkContext, cancel := context.WithTimeout(
		context.Background(),
		2*time.Minute,
	)
	serverStreamResult := make(chan *quic.Stream, 1)
	serverError := make(chan error, 1)
	go func() {
		connection, acceptErr := listener.Accept(benchmarkContext)
		if acceptErr != nil {
			serverError <- acceptErr
			return
		}
		stream, acceptErr := connection.AcceptStream(benchmarkContext)
		if acceptErr != nil {
			serverError <- acceptErr
			return
		}
		serverStreamResult <- stream
	}()

	clientConnection, err := quic.DialAddr(
		benchmarkContext,
		listener.Addr().String(),
		&tls.Config{
			InsecureSkipVerify: true, // benchmark-only self-signed endpoint
			NextProtos:         []string{nextProtocol},
		},
		&quic.Config{MaxIdleTimeout: time.Minute},
	)
	if err != nil {
		cancel()
		listener.Close()
		benchmark.Fatal(err)
	}
	clientStream, err := clientConnection.OpenStreamSync(benchmarkContext)
	if err != nil {
		cancel()
		listener.Close()
		clientConnection.CloseWithError(0, "benchmark setup failed")
		benchmark.Fatal(err)
	}

	framer := NewFramer(DefaultFramerSettings(platformH3BatchBenchmarkMessageByteCount))
	warmMessage := make([]byte, platformH3BatchBenchmarkMessageByteCount)
	if err = framer.Write(clientStream, warmMessage); err != nil {
		cancel()
		listener.Close()
		clientConnection.CloseWithError(0, "benchmark warm write failed")
		benchmark.Fatal(err)
	}

	var serverStream *quic.Stream
	select {
	case serverStream = <-serverStreamResult:
	case err = <-serverError:
		cancel()
		listener.Close()
		clientConnection.CloseWithError(0, "benchmark accept failed")
		benchmark.Fatal(err)
	case <-benchmarkContext.Done():
		cancel()
		listener.Close()
		clientConnection.CloseWithError(0, "benchmark accept timed out")
		benchmark.Fatal(benchmarkContext.Err())
	}
	warmReceived, err := framer.Read(serverStream)
	if err != nil {
		cancel()
		listener.Close()
		clientConnection.CloseWithError(0, "benchmark warm read failed")
		benchmark.Fatal(err)
	}
	MessagePoolReturn(warmReceived)

	benchmark.Cleanup(func() {
		cancel()
		clientStream.Close()
		clientConnection.CloseWithError(0, "benchmark complete")
		listener.Close()
	})
	return clientStream, serverStream
}

// Measures a fixed ready backlog while varying only the number of ordinary H3
// frames copied into each QUIC stream write, once per ip family.
func runPlatformH3BatchBenchmark(
	benchmark *testing.B,
	maximumMessageCount int,
	retainedStorage bool,
) {
	forEachIpVersionBenchmark(benchmark, func(benchmark *testing.B, ipVersion int) {
		runPlatformH3BatchBenchmarkIpVersion(benchmark, ipVersion, maximumMessageCount, retainedStorage)
	})
}

// One family's run of runPlatformH3BatchBenchmark.
func runPlatformH3BatchBenchmarkIpVersion(
	benchmark *testing.B,
	ipVersion int,
	maximumMessageCount int,
	retainedStorage bool,
) {
	clientStream, serverStream := newPlatformH3BatchBenchmarkStream(benchmark, ipVersion)
	framer := NewFramer(DefaultFramerSettings(platformH3BatchBenchmarkMessageByteCount))
	payload := make([]byte, platformH3BatchBenchmarkMessageByteCount)
	var messages [platformH3BatchBenchmarkBurstMessageCount][]byte
	for messageIndex := range messages {
		messages[messageIndex] = payload
	}

	totalMessageCount := benchmark.N * len(messages)
	readResult := make(chan error, 1)
	go func() {
		for messageIndex := range totalMessageCount {
			message, readErr := framer.Read(serverStream)
			if readErr != nil {
				readResult <- readErr
				return
			}
			if len(message) != len(payload) {
				MessagePoolReturn(message)
				readResult <- fmt.Errorf(
					"H3 message %d byte count = %d, want %d",
					messageIndex,
					len(message),
					len(payload),
				)
				return
			}
			MessagePoolReturn(message)
		}
		readResult <- nil
	}()

	benchmark.ReportAllocs()
	benchmark.SetBytes(int64(len(payload) * len(messages)))
	benchmark.ResetTimer()
	writeCount := 0
	var writeErr error
	writeBatchStorage := make([]byte, platformH3WriteBatchMaxByteCount)
	for range benchmark.N {
		for firstMessageIndex := 0; firstMessageIndex < len(messages); firstMessageIndex += maximumMessageCount {
			lastMessageIndex := min(
				firstMessageIndex+maximumMessageCount,
				len(messages),
			)
			clientStream.SetWriteDeadline(time.Now().Add(time.Minute))
			writeCount += 1
			if retainedStorage {
				writeErr = framer.WriteBatchWithStorage(
					clientStream,
					messages[firstMessageIndex:lastMessageIndex],
					writeBatchStorage,
				)
			} else {
				writeErr = framer.WriteBatch(
					clientStream,
					messages[firstMessageIndex:lastMessageIndex],
				)
			}
			if writeErr != nil {
				break
			}
		}
		if writeErr != nil {
			break
		}
	}
	if writeErr != nil {
		clientStream.CancelWrite(1)
	}
	readErr := <-readResult
	benchmark.StopTimer()

	if writeErr != nil {
		benchmark.Fatal(writeErr)
	}
	if readErr != nil {
		benchmark.Fatal(readErr)
	}
	benchmark.ReportMetric(
		float64(totalMessageCount)/float64(writeCount),
		"messages/write",
	)
}

// Measures the proposed eight-message H3 ready-drain depth.
func BenchmarkPlatformH3SocketBatch8Loopback(benchmark *testing.B) {
	runPlatformH3BatchBenchmark(benchmark, 8, false)
}

// Measures the current sixteen-message H3 ready-drain depth.
func BenchmarkPlatformH3SocketBatch16Loopback(benchmark *testing.B) {
	runPlatformH3BatchBenchmark(benchmark, 16, false)
}

// Measures production depth with one buffer retained by the H3 writer.
func BenchmarkPlatformH3SocketBatch16RetainedStorageLoopback(benchmark *testing.B) {
	runPlatformH3BatchBenchmark(benchmark, 16, true)
}
