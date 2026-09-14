// These tests exercise QUIC over packet translation, including retry and
// cancellation ownership across lossy DNS-shaped carriers.
package connect

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/tls"

	// "crypto/elliptic"
	// "crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	// "crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"

	"math/big"

	quic "github.com/quic-go/quic-go"

	"golang.org/x/net/dns/dnsmessage"

	"testing"
)

func TestPtDnsEncodeDecode(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ptEncodeDecodeTest(t, ipVersion, PacketTranslationModeDns, PacketTranslationModeDecode53)
	})
}

func TestPtDnsPumpEncodeDecode(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ptEncodeDecodeTest(t, ipVersion, PacketTranslationModeDnsPump, PacketTranslationModeDecode53RequireDnsPump)
	})
}

// A synchronous translated write needs a deadline timer while its encoder owns
// the packet. Keep that fixed cost bounded, and preserve the allocation-free
// ready-read path.
func TestPacketTranslationReadyDeadlineAllocationIsBounded(t *testing.T) {
	measureWrite := func(withDeadline bool) float64 {
		pt := &packetTranslation{
			ctx:                  context.Background(),
			log:                  DefaultLogger(),
			out:                  make(chan *packet, 1),
			writeDeadlineMonitor: NewMonitor(),
		}
		consumerDone := make(chan struct{})
		consumerExited := make(chan struct{})
		defer func() {
			close(consumerDone)
			<-consumerExited
		}()
		go func() {
			defer close(consumerExited)
			for {
				select {
				case queued := <-pt.out:
					MessagePoolReturn(queued.data)
					finishPacketWrite(queued, nil)
				case <-consumerDone:
					return
				}
			}
		}()
		if withDeadline {
			pt.writeDeadline = time.Now().Add(time.Hour)
		}
		packetData := make([]byte, 64)
		addr := &net.UDPAddr{}
		return testing.AllocsPerRun(1000, func() {
			if _, err := pt.WriteTo(packetData, addr); err != nil {
				panic(err)
			}
		})
	}
	measureRead := func(withDeadline bool) float64 {
		pt := &packetTranslation{
			ctx:                 context.Background(),
			log:                 DefaultLogger(),
			in:                  make(chan *packet, 1),
			readDeadlineMonitor: NewMonitor(),
		}
		if withDeadline {
			pt.readDeadline = time.Now().Add(time.Hour)
		}
		packetData := make([]byte, 64)
		addr := &net.UDPAddr{}
		return testing.AllocsPerRun(1000, func() {
			pt.in <- &packet{
				data: MessagePoolGet(64),
				addr: addr,
			}
			if _, _, err := pt.ReadFrom(packetData); err != nil {
				panic(err)
			}
		})
	}

	writeAllocations := measureWrite(false)
	deadlineWriteAllocations := measureWrite(true)
	if writeAllocations+4 < deadlineWriteAllocations {
		t.Fatalf(
			"ready write deadline allocations = %.2f, without deadline %.2f",
			deadlineWriteAllocations,
			writeAllocations,
		)
	}
	AssertEqual(t, measureRead(false), measureRead(true))
}

func TestPtDnsPumpZeroWriteRateDisablesPacing(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sourceConn, err := net.ListenUDP(testUdpNetwork(ipVersion), testLoopbackUdpAddr(ipVersion))
		AssertEqual(t, err, nil)
		defer sourceConn.Close()
		destinationConn, err := net.ListenUDP(testUdpNetwork(ipVersion), testLoopbackUdpAddr(ipVersion))
		AssertEqual(t, err, nil)
		defer destinationConn.Close()

		settings := DefaultPacketTranslationSettings()
		settings.DnsTlds = [][]byte{[]byte("example.com.")}
		settings.DnsPumpTimeout = time.Hour
		settings.WritePacketsPerSecond = 0
		pt, err := NewPacketTranslation(ctx, PacketTranslationModeDnsPump, sourceConn, settings)
		AssertEqual(t, err, nil)
		defer pt.Close()

		_ = destinationConn.SetReadDeadline(time.Now().Add(2 * time.Second))
		if _, err := pt.WriteTo(make([]byte, 64), destinationConn.LocalAddr()); err != nil {
			t.Fatal(err)
		}
		packetData := make([]byte, 2048)
		if _, _, err := destinationConn.ReadFrom(packetData); err != nil {
			t.Fatalf("unpaced dns translation did not write: %v", err)
		}
	})
}

// packetTranslationLifecyclePacketConn holds the first encoded write until
// Close, making the translation's owned out queue deterministic.
type packetTranslationLifecyclePacketConn struct {
	closed       chan struct{}
	writeStarted chan struct{}
	closeOnce    sync.Once
	writeOnce    sync.Once
}

func newPacketTranslationLifecyclePacketConn() *packetTranslationLifecyclePacketConn {
	return &packetTranslationLifecyclePacketConn{
		closed:       make(chan struct{}),
		writeStarted: make(chan struct{}),
	}
}

func (self *packetTranslationLifecyclePacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	<-self.closed
	return 0, nil, net.ErrClosed
}

func (self *packetTranslationLifecyclePacketConn) WriteTo([]byte, net.Addr) (int, error) {
	self.writeOnce.Do(func() { close(self.writeStarted) })
	<-self.closed
	return 0, net.ErrClosed
}

func (self *packetTranslationLifecyclePacketConn) LocalAddr() net.Addr {
	return &net.UDPAddr{}
}

func (self *packetTranslationLifecyclePacketConn) Close() error {
	self.closeOnce.Do(func() { close(self.closed) })
	return nil
}

func (self *packetTranslationLifecyclePacketConn) SetDeadline(time.Time) error {
	return nil
}

func (self *packetTranslationLifecyclePacketConn) SetReadDeadline(time.Time) error {
	return nil
}

func (self *packetTranslationLifecyclePacketConn) SetWriteDeadline(time.Time) error {
	return nil
}

// Close joins a blocked encoder and decoder, wakes every synchronous WriteTo,
// then returns both outbound copies and an inbound packet at the PacketConn edge.
func TestPacketTranslationCloseReturnsQueuedPacketOwnership(t *testing.T) {
	beforeTaken, beforeReturned, _ := MessagePoolCounts()
	beforeOutstanding := int64(beforeTaken) - int64(beforeReturned)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	packetConn := newPacketTranslationLifecyclePacketConn()
	settings := DefaultPacketTranslationSettings()
	settings.DnsTlds = [][]byte{[]byte("example.com.")}
	settings.SequenceBufferSize = 8
	settings.WritePacketsPerSecond = 0
	translation, err := NewPacketTranslation(
		ctx,
		PacketTranslationModeDns,
		packetConn,
		settings,
	)
	if err != nil {
		t.Fatal(err)
	}

	addr := &net.UDPAddr{}
	writeResults := make(chan error, settings.SequenceBufferSize+1)
	startWrite := func(data []byte) {
		go func() {
			_, writeErr := translation.WriteTo(data, addr)
			writeResults <- writeErr
		}()
	}
	startWrite([]byte("blocked encoder"))
	select {
	case <-packetConn.writeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("packet translation encoder did not reach the blocked write")
	}
	select {
	case err := <-writeResults:
		t.Fatalf("WriteTo returned before its wire write completed: %v", err)
	default:
	}
	for packetIndex := 0; packetIndex < settings.SequenceBufferSize; packetIndex++ {
		startWrite([]byte("queued packet"))
	}
	queueDeadline := time.Now().Add(5 * time.Second)
	for len(translation.out) != settings.SequenceBufferSize {
		if queueDeadline.Before(time.Now()) {
			t.Fatalf("queued writes = %d, want %d", len(translation.out), settings.SequenceBufferSize)
		}
		time.Sleep(time.Millisecond)
	}
	translation.in <- &packet{
		data: MessagePoolCopy([]byte("queued inbound packet")),
		addr: addr,
	}

	if err := translation.Close(); err != nil {
		t.Fatal(err)
	}
	for writeIndex := 0; writeIndex < settings.SequenceBufferSize+1; writeIndex++ {
		select {
		case writeErr := <-writeResults:
			if writeErr == nil {
				t.Fatalf("write %d succeeded although its wire operation was interrupted", writeIndex)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("write %d did not return after Close", writeIndex)
		}
	}
	if _, err := translation.WriteTo([]byte("late packet"), addr); err == nil {
		t.Fatal("closed packet translation admitted a late write")
	}

	afterTaken, afterReturned, _ := MessagePoolCounts()
	afterOutstanding := int64(afterTaken) - int64(afterReturned)
	if afterOutstanding != beforeOutstanding {
		t.Fatalf(
			"packet translation close left pooled buffers outstanding: %d -> %d",
			beforeOutstanding,
			afterOutstanding,
		)
	}
}

// The attempt owns its QUIC connection until either context cancellation or
// ordinary cleanup closes it. sync.Once makes cleanup join a cancellation
// close already in progress instead of returning while its socket workers live.
func closePacketTranslationTestQuicConnection(
	ctx context.Context,
	connection interface {
		CloseWithError(quic.ApplicationErrorCode, string) error
	},
	beforeCleanupWaitForTest func(),
) func() {
	var closeOnce sync.Once
	closeConnection := func() {
		closeOnce.Do(func() {
			_ = connection.CloseWithError(0, "packet translation attempt complete")
		})
	}
	stopClose := context.AfterFunc(ctx, closeConnection)
	return func() {
		stopClose()
		if beforeCleanupWaitForTest != nil {
			beforeCleanupWaitForTest()
		}
		closeConnection()
	}
}

// A canceled attempt can enter connection Close while its owner is unwinding.
// Cleanup must join that exact close before it publishes attempt completion.
func TestPacketTranslationAttemptCleanupJoinsContextConnectionClose(t *testing.T) {
	type blockingConnection struct {
		closeStarted  chan struct{}
		closeRelease  chan struct{}
		closeComplete chan struct{}
	}
	connection := &blockingConnection{
		closeStarted:  make(chan struct{}),
		closeRelease:  make(chan struct{}),
		closeComplete: make(chan struct{}),
	}
	closeWithError := func(quic.ApplicationErrorCode, string) error {
		close(connection.closeStarted)
		<-connection.closeRelease
		close(connection.closeComplete)
		return nil
	}
	wrappedConnection := &packetTranslationTestCloseConnection{
		closeWithError: closeWithError,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cleanupEntered := make(chan struct{})
	cleanup := closePacketTranslationTestQuicConnection(
		ctx,
		wrappedConnection,
		func() {
			close(cleanupEntered)
		},
	)
	cancel()
	<-connection.closeStarted

	cleanupComplete := make(chan struct{})
	go func() {
		cleanup()
		close(cleanupComplete)
	}()
	<-cleanupEntered
	select {
	case <-cleanupComplete:
		t.Fatal("attempt cleanup returned before its context close completed")
	default:
	}
	close(connection.closeRelease)
	<-cleanupComplete
	select {
	case <-connection.closeComplete:
	default:
		t.Fatal("attempt cleanup returned before connection close publication")
	}
}

// Adapts a barrier function to the QUIC connection close shape without
// implementing unrelated connection behavior.
type packetTranslationTestCloseConnection struct {
	closeWithError func(quic.ApplicationErrorCode, string) error
}

func (self *packetTranslationTestCloseConnection) CloseWithError(
	code quic.ApplicationErrorCode,
	reason string,
) error {
	return self.closeWithError(code, reason)
}

func ptEncodeDecodeTest(t *testing.T, ipVersion int, clientPtMode PacketTranslationMode, serverPtMode PacketTranslationMode) {
	if testing.Short() {
		return
	}

	iterations := 16
	attempts := 4
	if os.Getenv("CONNECT_PT_STRESS") != "" {
		iterations = 64
		attempts = 8
	}

	ctx := context.Background()

	consecutive := func(n int) []byte {
		out := make([]byte, 4*n)
		for i := range n {
			binary.BigEndian.PutUint32(out[4*i:4*i+4], uint32(i))
		}
		return out
	}

	for i := range iterations {
		headerPrefix := make([]byte, 8)
		binary.BigEndian.PutUint64(headerPrefix, uint64(i+1))

		// FIXME quic does not seem to recover well with packet loss
		packetLossN := i + 100

		fmt.Printf("[%d]dns test (loss=%.1f%%)\n", i, 100.0/float32(packetLossN))
		attemptIndex := 0
		success := runPacketTranslationAttempts(ctx, attempts, 0, func() bool {
			currentAttemptIndex := attemptIndex
			attemptIndex++
			attemptCtx, attemptCancel := context.WithTimeout(ctx, 20*time.Second)
			defer attemptCancel()

			handleCtx, handleCancel := context.WithCancel(attemptCtx)
			defer handleCancel()

			n := 1024 * (8 + (3*i+5*currentAttemptIndex)%8)
			data := consecutive(n)

			tld := []byte("foo.com.")

			serverAddr := testLoopbackUdpAddr(ipVersion)

			ioTimeout := 5 * time.Second
			ioDeadline := func() time.Time {
				deadline := time.Now().Add(ioTimeout)
				if ctxDeadline, ok := handleCtx.Deadline(); ok && ctxDeadline.Before(deadline) {
					return ctxDeadline
				}
				return deadline
			}

			quicConfig := &quic.Config{
				HandshakeIdleTimeout:    ioTimeout,
				MaxIdleTimeout:          ioTimeout,
				KeepAlivePeriod:         5 * time.Second,
				Allow0RTT:               true,
				DisablePathMTUDiscovery: true,
			}

			serverCtx, serverCancel := context.WithCancel(handleCtx)
			errCh := make(chan error, 4)
			reportErr := func(err error) bool {
				if err == nil {
					return false
				}
				select {
				case errCh <- err:
				default:
				}
				handleCancel()
				return true
			}
			// func() {
			serverTlsConfig := &tls.Config{
				GetConfigForClient: func(clientHello *tls.ClientHelloInfo) (*tls.Config, error) {
					certPemBytes, keyPemBytes, err := selfSign(
						[]string{clientHello.ServerName},
						clientHello.ServerName,
						180*24*time.Hour,
						180*24*time.Hour,
					)
					if err != nil {
						return nil, err
					}
					// X509KeyPair
					cert, err := tls.X509KeyPair(certPemBytes, keyPemBytes)
					return &tls.Config{
						Certificates: []tls.Certificate{cert},
					}, err
				},
			}

			serverConn, err := net.ListenUDP(testUdpNetwork(ipVersion), serverAddr)
			AssertEqual(t, err, nil)
			defer serverConn.Close()
			serverAddr = serverConn.LocalAddr().(*net.UDPAddr)

			impairmentOffset := 8*i + 4*currentAttemptIndex
			serverLossConn := newPacketLossPacketConn(
				packetLossN,
				serverConn,
				impairmentOffset,
				impairmentOffset+1,
			)

			serverPtSettings := DefaultPacketTranslationSettings()
			serverPtSettings.DnsTlds = [][]byte{tld}
			// settings.DnsAddr = serverAddr

			serverPtConn, err := NewPacketTranslationWithPrefix(handleCtx, serverPtMode, serverLossConn, serverPtSettings, headerPrefix)
			AssertEqual(t, err, nil)
			defer serverPtConn.Close()

			earlyListener, err := (&quic.Transport{
				Conn: serverPtConn,
				// createdConn: true,
				// isSingleUse: true,
			}).ListenEarly(serverTlsConfig, quicConfig)
			// listenQuic(ctx, earlyListener)
			AssertEqual(t, err, nil)
			defer earlyListener.Close()

			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				defer serverCancel()
				// defer ptConn.Close()
				// defer earlyListener.Close()

				earlyConn, err := earlyListener.Accept(handleCtx)
				if err != nil {
					reportErr(fmt.Errorf("server accept: %w", err))
					return
				}
				closeEarlyConnection := closePacketTranslationTestQuicConnection(
					handleCtx,
					earlyConn,
					nil,
				)
				defer closeEarlyConnection()
				stream, err := earlyConn.AcceptStream(handleCtx)
				if err != nil {
					reportErr(fmt.Errorf("server accept stream: %w", err))
					return
				}

				writeCtx, writeCancel := context.WithCancel(handleCtx)
				go func() {
					defer writeCancel()
					stream.SetWriteDeadline(ioDeadline())
					m, err := stream.Write(data)
					if err != nil {
						reportErr(fmt.Errorf("server write: %w", err))
						return
					}
					if m != len(data) {
						reportErr(fmt.Errorf("server short write: %d != %d", m, len(data)))
					}
				}()
				defer func() {
					closeEarlyConnection()
					<-writeCtx.Done()
				}()

				readData := make([]byte, 0, len(data))
				buf := make([]byte, 2048)

				for len(readData) < len(data) {
					select {
					case <-handleCtx.Done():
						reportErr(handleCtx.Err())
						return
					default:
					}
					stream.SetReadDeadline(ioDeadline())
					m, err := stream.Read(buf[:min(len(buf), len(data)-len(readData))])
					if err != nil {
						reportErr(fmt.Errorf("server read: %w", err))
						return
					}
					readData = append(readData, buf[:m]...)

					// fmt.Printf("read[%d]\n", m)
					fmt.Printf("+")
				}

				if !bytes.Equal(data, readData) {
					reportErr(fmt.Errorf("server read data mismatch"))
					return
				}

				select {
				case err := <-errCh:
					reportErr(err)
					return
				case <-writeCtx.Done():
				}

			}()
			defer func() {
				handleCancel()
				<-serverDone
			}()

			// }()

			clientTlsConfig := &tls.Config{
				ServerName:         string(tld),
				InsecureSkipVerify: true,
			}

			clientConn, err := net.ListenUDP(testUdpNetwork(ipVersion), &net.UDPAddr{IP: testUnspecifiedIp(ipVersion), Port: 0})
			AssertEqual(t, err, nil)
			defer clientConn.Close()

			lossConn := newPacketLossPacketConn(
				packetLossN,
				clientConn,
				impairmentOffset+2,
				impairmentOffset+3,
			)

			ptSettings := DefaultPacketTranslationSettings()
			ptSettings.DnsTlds = [][]byte{tld}
			// ptSettings.DnsAddr = serverAddr
			ptConn, err := NewPacketTranslationWithPrefix(handleCtx, clientPtMode, lossConn, ptSettings, headerPrefix)
			AssertEqual(t, err, nil)
			defer ptConn.Close()

			quicTransport := &quic.Transport{
				Conn: ptConn,
				// createdConn: true,
				// isSingleUse: true,
			}

			// enable 0rtt if possible
			conn, err := quicTransport.DialEarly(handleCtx, serverAddr, clientTlsConfig, quicConfig)
			if err != nil {
				reportErr(fmt.Errorf("client dial: %w", err))
				return false
			}
			closeConnection := closePacketTranslationTestQuicConnection(
				handleCtx,
				conn,
				nil,
			)
			defer closeConnection()

			stream, err := conn.OpenStream()
			if err != nil {
				reportErr(fmt.Errorf("client open stream: %w", err))
				return false
			}

			writeCtx, writeCancel := context.WithCancel(handleCtx)
			go func() {
				defer writeCancel()
				stream.SetWriteDeadline(ioDeadline())
				m, err := stream.Write(data)
				if err != nil {
					reportErr(fmt.Errorf("client write: %w", err))
					return
				}
				if m != len(data) {
					reportErr(fmt.Errorf("client short write: %d != %d", m, len(data)))
				}
			}()
			defer func() {
				closeConnection()
				<-writeCtx.Done()
			}()

			readData := make([]byte, 0, len(data))
			buf := make([]byte, 2048)

			for len(readData) < len(data) {
				select {
				case err := <-errCh:
					fmt.Printf("connection issue: %s\n", err)
					return false
				case <-handleCtx.Done():
					reportErr(handleCtx.Err())
					return false
				default:
				}
				stream.SetReadDeadline(ioDeadline())
				m, err := stream.Read(buf[:min(len(buf), len(data)-len(readData))])
				if err != nil {
					reportErr(fmt.Errorf("client read: %w", err))
					return false
				}
				// AssertEqual(t, err, nil)
				readData = append(readData, buf[:m]...)

				// fmt.Printf("read[%d]\n", m)
				fmt.Printf(".")
			}

			if !bytes.Equal(data, readData) {
				reportErr(fmt.Errorf("client read data mismatch"))
				return false
			}

			select {
			case err := <-errCh:
				fmt.Printf("connection issue: %s\n", err)
				return false
			case <-writeCtx.Done():
				// case <- time.After(60 * time.Second):
				// 	t.FailNow()
			}

			select {
			case err := <-errCh:
				fmt.Printf("connection issue: %s\n", err)
				return false
			case <-serverCtx.Done():
				// case <- time.After(60 * time.Second):
				// 	t.FailNow()
			}
			select {
			case err := <-errCh:
				fmt.Printf("connection issue: %s\n", err)
				return false
			default:
			}

			return true
		})
		fmt.Printf("\n")
		if !success {
			t.FailNow()
		}
	}

}

// runPacketTranslationAttempts reforms the sockets after a retryable QUIC
// failure. The packet-loss tests intentionally make DialEarly and OpenStream
// fail occasionally; an assertion inside the attempt used to call FailNow
// before this loop could exercise its remaining attempts.
func runPacketTranslationAttempts(
	ctx context.Context,
	attempts int,
	retryDelay time.Duration,
	attempt func() bool,
) bool {
	for attemptIndex := 0; attemptIndex < attempts; attemptIndex++ {
		if attempt() {
			return true
		}
		if attemptIndex+1 == attempts {
			return false
		}
		fmt.Printf("\nconnection issue. retry.\n")
		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return false
		case <-timer.C:
		}
	}
	return false
}

func TestPacketTranslationAttemptsRetryRecoverableFailure(t *testing.T) {
	attemptCount := 0
	success := runPacketTranslationAttempts(
		context.Background(),
		4,
		0,
		func() bool {
			attemptCount++
			return 2 <= attemptCount
		},
	)
	if !success {
		t.Fatal("recoverable first attempt prevented a later success")
	}
	if attemptCount != 2 {
		t.Fatalf("attempt count = %d, want 2", attemptCount)
	}
}

// A fixed cadence makes the lossy integration test replay the same independent
// read/write actions on every invocation.
func TestPacketLossPacketConnUsesDeterministicIndependentSchedules(t *testing.T) {
	conn := newPacketLossPacketConn(4, nil, 0, 1)
	expectedReadImpairments := []packetLossImpairment{
		packetLossNone,
		packetLossNone,
		packetLossNone,
		packetLossScramble,
		packetLossNone,
		packetLossNone,
		packetLossNone,
		packetLossDrop,
	}
	expectedWriteImpairments := []packetLossImpairment{
		packetLossNone,
		packetLossNone,
		packetLossScramble,
		packetLossNone,
		packetLossNone,
		packetLossNone,
		packetLossDrop,
		packetLossNone,
	}
	for i := range expectedReadImpairments {
		if impairment := conn.nextImpairment(true); impairment != expectedReadImpairments[i] {
			t.Fatalf("read impairment %d = %d, want %d", i, impairment, expectedReadImpairments[i])
		}
		if impairment := conn.nextImpairment(false); impairment != expectedWriteImpairments[i] {
			t.Fatalf("write impairment %d = %d, want %d", i, impairment, expectedWriteImpairments[i])
		}
	}
}

// Scrambling simulates corruption on the wire, not corruption of the QUIC
// buffer whose ownership remains with the WriteTo caller.
func TestPacketLossPacketConnWriteScramblePreservesCallerBuffer(t *testing.T) {
	packetConn := &packetLossRecordingPacketConn{}
	conn := newPacketLossPacketConn(1, packetConn, 0, 0)
	packetBytes := []byte{0x01, 0x02, 0x03, 0x04}
	originalBytes := bytes.Clone(packetBytes)

	n, err := conn.WriteTo(packetBytes, &net.UDPAddr{})
	if err != nil {
		t.Fatal(err)
	}
	if n != len(packetBytes) {
		t.Fatalf("write size = %d, want %d", n, len(packetBytes))
	}
	if !bytes.Equal(packetBytes, originalBytes) {
		t.Fatalf("caller buffer changed: got %x, want %x", packetBytes, originalBytes)
	}
	if bytes.Equal(packetConn.writtenBytes, originalBytes) {
		t.Fatalf("wire packet was not scrambled: %x", packetConn.writtenBytes)
	}
}

// Minimal synchronous PacketConn records the exact bytes offered by WriteTo.
type packetLossRecordingPacketConn struct {
	writtenBytes []byte
}

// Read is unsupported because the ownership regression exercises only writes.
func (self *packetLossRecordingPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, fmt.Errorf("read is not supported")
}

// Record a private copy so later caller changes cannot alter the observation.
func (self *packetLossRecordingPacketConn) WriteTo(packetBytes []byte, _ net.Addr) (int, error) {
	self.writtenBytes = bytes.Clone(packetBytes)
	return len(packetBytes), nil
}

// The fake has no physical address.
func (self *packetLossRecordingPacketConn) LocalAddr() net.Addr {
	return &net.UDPAddr{}
}

// Closing the synchronous fake has no work to join.
func (self *packetLossRecordingPacketConn) Close() error {
	return nil
}

// Deadlines do not affect its synchronous writes.
func (self *packetLossRecordingPacketConn) SetDeadline(time.Time) error {
	return nil
}

// Read deadlines are accepted for PacketConn conformance.
func (self *packetLossRecordingPacketConn) SetReadDeadline(time.Time) error {
	return nil
}

// Write deadlines are accepted for PacketConn conformance.
func (self *packetLossRecordingPacketConn) SetWriteDeadline(time.Time) error {
	return nil
}

// A reproducible impairment applied by the lossy packet-connection fixture.
type packetLossImpairment int

const (
	packetLossNone packetLossImpairment = iota
	packetLossScramble
	packetLossDrop
)

// Per-direction state keeps the loss cadence independent across read and write.
type packetLossSchedule struct {
	packetIndex     int
	impairmentIndex int
	offset          int
}

// Deterministically impairs one of every n packets without changing buffers
// owned by PacketConn callers. Its methods are safe for concurrent use.
type packetLossPacketConn struct {
	n          int
	packetConn net.PacketConn

	stateLock     sync.Mutex
	readSchedule  packetLossSchedule
	writeSchedule packetLossSchedule
}

// Offsets make endpoint directions exercise different deterministic packets.
func newPacketLossPacketConn(
	n int,
	packetConn net.PacketConn,
	readOffset int,
	writeOffset int,
) *packetLossPacketConn {
	normalizeOffset := func(offset int) int {
		if n <= 0 {
			return 0
		}
		offset %= n
		if offset < 0 {
			offset += n
		}
		return offset
	}
	return &packetLossPacketConn{
		n:             n,
		packetConn:    packetConn,
		readSchedule:  packetLossSchedule{offset: normalizeOffset(readOffset)},
		writeSchedule: packetLossSchedule{offset: normalizeOffset(writeOffset)},
	}
}

// Selects the next fixed action for one direction.
func (self *packetLossPacketConn) nextImpairment(read bool) packetLossImpairment {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	schedule := &self.writeSchedule
	if read {
		schedule = &self.readSchedule
	}
	packetIndex := schedule.packetIndex
	schedule.packetIndex++
	if self.n <= 0 || (packetIndex+schedule.offset+1)%self.n != 0 {
		return packetLossNone
	}
	impairmentIndex := schedule.impairmentIndex
	schedule.impairmentIndex++
	if impairmentIndex%2 == 0 {
		return packetLossScramble
	}
	return packetLossDrop
}

// Applies the read-side schedule after the underlying socket supplies a packet.
func (self *packetLossPacketConn) ReadFrom(p []byte) (n int, addr net.Addr, err error) {
	for {
		n, addr, err = self.packetConn.ReadFrom(p)
		if err != nil {
			return
		}
		switch self.nextImpairment(true) {
		case packetLossScramble:
			if 0 < n {
				p[0] ^= 0xff
			}
			fmt.Printf("s")
		case packetLossDrop:
			fmt.Printf("d")
			continue
		}
		return
	}
}

// Applies the write-side schedule while preserving the caller-owned packet.
func (self *packetLossPacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	switch self.nextImpairment(false) {
	case packetLossScramble:
		wirePacket := bytes.Clone(p)
		if 0 < len(wirePacket) {
			wirePacket[0] ^= 0xff
		}
		fmt.Printf("s")
		return self.packetConn.WriteTo(wirePacket, addr)
	case packetLossDrop:
		fmt.Printf("d")
		return len(p), nil
	}

	return self.packetConn.WriteTo(p, addr)
}

func (self *packetLossPacketConn) LocalAddr() net.Addr {
	return self.packetConn.LocalAddr()
}

func (self *packetLossPacketConn) SetDeadline(t time.Time) error {
	return self.packetConn.SetDeadline(t)
}

func (self *packetLossPacketConn) SetReadDeadline(t time.Time) error {
	return self.packetConn.SetReadDeadline(t)
}

func (self *packetLossPacketConn) SetWriteDeadline(t time.Time) error {
	return self.packetConn.SetWriteDeadline(t)
}

func (self *packetLossPacketConn) Close() error {
	return self.packetConn.Close()
}

func (self *packetLossPacketConn) SetReadBuffer(bytes int) error {
	conn, ok := self.packetConn.(interface{ SetReadBuffer(int) error })
	if !ok {
		return fmt.Errorf("Set read buffer not supporter on underlying packet conn: %T", self.packetConn)
	}
	return conn.SetReadBuffer(bytes)
}

func (self *packetLossPacketConn) SetWriteBuffer(bytes int) error {
	conn, ok := self.packetConn.(interface{ SetWriteBuffer(int) error })
	if !ok {
		return fmt.Errorf("Set write buffer not supporter on underlying packet conn: %T", self.packetConn)
	}
	return conn.SetWriteBuffer(bytes)
}

func selfSign(hosts []string, organization string, validFrom time.Duration, validFor time.Duration) (certPemBytes []byte, keyPemBytes []byte, returnErr error) {

	var priv any
	var err error

	priv, err = rsa.GenerateKey(rand.Reader, 2048)
	// priv, err = ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		returnErr = err
		return
	}

	publicKey := func(priv any) any {
		switch k := priv.(type) {
		case *rsa.PrivateKey:
			return &k.PublicKey
		case *ecdsa.PrivateKey:
			return &k.PublicKey
		case ed25519.PrivateKey:
			return k.Public().(ed25519.PublicKey)
		default:
			return nil
		}
	}

	// ECDSA, ED25519 and RSA subject keys should have the DigitalSignature
	// KeyUsage bits set in the x509.Certificate template
	keyUsage := x509.KeyUsageDigitalSignature
	// Only RSA subject keys should have the KeyEncipherment KeyUsage bits set. In
	// the context of TLS this KeyUsage is particular to RSA key exchange and
	// authentication.
	if _, isRSA := priv.(*rsa.PrivateKey); isRSA {
		keyUsage |= x509.KeyUsageKeyEncipherment
	}

	notBefore := time.Now().Add(-validFrom)
	notAfter := notBefore.Add(validFor)

	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := rand.Int(rand.Reader, serialNumberLimit)
	if err != nil {
		panic(fmt.Errorf("Failed to generate serial number: %v", err))
	}

	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{organization},
		},
		NotBefore: notBefore,
		NotAfter:  notAfter,

		KeyUsage:              keyUsage,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}

	for _, h := range hosts {
		if ip := net.ParseIP(h); ip != nil {
			template.IPAddresses = append(template.IPAddresses, ip)
		} else {
			template.DNSNames = append(template.DNSNames, h)
		}
	}

	// we hope the client is using tls1.3 which hides the self signed cert
	template.IsCA = true
	template.KeyUsage |= x509.KeyUsageCertSign

	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, publicKey(priv), priv)
	if err != nil {
		returnErr = err
		return
	}
	certPemBytes = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: derBytes})

	privBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		returnErr = err
		return
	}
	keyPemBytes = pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privBytes})

	return
}

// A query on a decode53 socket that is not the translation reaches the hook
// with its own bytes and its source address, and a translated packet does not
// (EXTENDER.md A6).
func TestPacketTranslationDeliversOtherDnsQueries(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	type otherQuery struct {
		queryBytes []byte
		addr       net.Addr
	}
	otherQueries := make(chan otherQuery, 4)
	settings := DefaultPacketTranslationSettings()
	settings.DnsTlds = [][]byte{[]byte("pt.example.")}
	settings.DnsOtherHandler = func(queryBytes []byte, addr net.Addr) {
		otherQueries <- otherQuery{queryBytes: queryBytes, addr: addr}
	}
	translation, err := NewPacketTranslation(ctx, PacketTranslationModeDecode53, serverConn, settings)
	if err != nil {
		serverConn.Close()
		t.Fatal(err)
	}
	defer translation.Close()

	clientConn, err := net.DialUDP(
		"udp",
		nil,
		serverConn.LocalAddr().(*net.UDPAddr),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	newQuery := func(name string, qType dnsmessage.Type) []byte {
		builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: 0x4242})
		builder.EnableCompression()
		if err := builder.StartQuestions(); err != nil {
			t.Fatal(err)
		}
		if err := builder.Question(dnsmessage.Question{
			Name:  dnsmessage.MustNewName(name),
			Type:  qType,
			Class: dnsmessage.ClassINET,
		}); err != nil {
			t.Fatal(err)
		}
		queryBytes, err := builder.Finish()
		if err != nil {
			t.Fatal(err)
		}
		return queryBytes
	}

	queryBytes := newQuery("other.example.", dnsmessage.TypeA)
	if _, err := clientConn.Write(queryBytes); err != nil {
		t.Fatal(err)
	}
	select {
	case delivered := <-otherQueries:
		if !bytes.Equal(delivered.queryBytes, queryBytes) {
			t.Fatalf("delivered %x, expected %x", delivered.queryBytes, queryBytes)
		}
		if delivered.addr.String() != clientConn.LocalAddr().String() {
			t.Fatalf("delivered address = %s, expected %s", delivered.addr, clientConn.LocalAddr())
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the query was not delivered to the hook")
	}

	// a txt query under the encoding tld is the translation, not an other query
	if _, err := clientConn.Write(newQuery("aaaa.pt.example.", dnsmessage.TypeTXT)); err != nil {
		t.Fatal(err)
	}
	// the next other query is a barrier: it can only be delivered after the
	// read loop has already handled the translated one
	if _, err := clientConn.Write(newQuery("second.example.", dnsmessage.TypeA)); err != nil {
		t.Fatal(err)
	}
	select {
	case delivered := <-otherQueries:
		var parser dnsmessage.Parser
		if _, err := parser.Start(delivered.queryBytes); err != nil {
			t.Fatal(err)
		}
		question, err := parser.Question()
		if err != nil {
			t.Fatal(err)
		}
		if question.Name.String() != "second.example." {
			t.Fatalf("delivered %s, expected the second query", question.Name)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the second query was not delivered to the hook")
	}
}

// A TXT query outside every encoding tld is a forwarder query, not a
// translation query (EXTENDER.md A6). It used to decode as an empty
// translation packet and be dropped, so the forwarder never saw it.
func TestPacketTranslationDeliversTxtQueriesOutsideTheEncodingTld(t *testing.T) {
	tlds := [][]byte{[]byte("pt.example.")}
	var decodeBuf [1024]byte

	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: 0x5151})
	builder.EnableCompression()
	if err := builder.StartQuestions(); err != nil {
		t.Fatal(err)
	}
	if err := builder.Question(dnsmessage.Question{
		Name:  dnsmessage.MustNewName("mail.other.example."),
		Type:  dnsmessage.TypeTXT,
		Class: dnsmessage.ClassINET,
	}); err != nil {
		t.Fatal(err)
	}
	queryBytes, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}

	_, _, _, tld, err, otherData := decodeDnsRequest(queryBytes, decodeBuf, tlds)
	if err != nil {
		t.Fatal(err)
	}
	if tld != nil {
		t.Fatalf("tld = %q, expected no encoding tld", tld)
	}
	if !otherData {
		t.Fatal("a txt query outside every encoding tld was not an other query")
	}
}

// The same query reaches the hook through a live decode53 socket, which is the
// path the extender's forwarder sits on (A6).
func TestPacketTranslationDeliversTxtQueriesToTheHook(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	serverConn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	otherQueries := make(chan []byte, 4)
	settings := DefaultPacketTranslationSettings()
	settings.DnsTlds = [][]byte{[]byte("pt.example.")}
	settings.DnsOtherHandler = func(queryBytes []byte, addr net.Addr) {
		otherQueries <- queryBytes
	}
	translation, err := NewPacketTranslation(ctx, PacketTranslationModeDecode53, serverConn, settings)
	if err != nil {
		serverConn.Close()
		t.Fatal(err)
	}
	defer translation.Close()

	clientConn, err := net.DialUDP("udp", nil, serverConn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer clientConn.Close()

	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: 0x6161})
	builder.EnableCompression()
	if err := builder.StartQuestions(); err != nil {
		t.Fatal(err)
	}
	if err := builder.Question(dnsmessage.Question{
		Name:  dnsmessage.MustNewName("mail.other.example."),
		Type:  dnsmessage.TypeTXT,
		Class: dnsmessage.ClassINET,
	}); err != nil {
		t.Fatal(err)
	}
	queryBytes, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := clientConn.Write(queryBytes); err != nil {
		t.Fatal(err)
	}
	select {
	case delivered := <-otherQueries:
		if !bytes.Equal(delivered, queryBytes) {
			t.Fatalf("delivered %x, expected %x", delivered, queryBytes)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the txt query was not delivered to the hook")
	}
}
