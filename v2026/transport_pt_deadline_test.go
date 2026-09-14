package connect

import (
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
)

// packetTranslationDeadlineTimer exposes the point at which a deadline wait
// has armed, then lets the test fire that timer without wall-clock sleeps.
type packetTranslationDeadlineTimer struct {
	requested chan time.Duration
	expired   chan time.Time
}

// newPacketTranslationDeadlineTimer creates a single-use deterministic timer.
func newPacketTranslationDeadlineTimer() *packetTranslationDeadlineTimer {
	return &packetTranslationDeadlineTimer{
		requested: make(chan time.Duration, 1),
		expired:   make(chan time.Time, 1),
	}
}

// after records the requested duration and returns the controlled expiry.
func (self *packetTranslationDeadlineTimer) after(timeout time.Duration) <-chan time.Time {
	self.requested <- timeout
	return self.expired
}

// packetTranslationDeadlineReadResult captures one PacketConn read result.
type packetTranslationDeadlineReadResult struct {
	n    int
	addr net.Addr
	err  error
}

// packetTranslationDeadlineWriteResult captures one PacketConn write result.
type packetTranslationDeadlineWriteResult struct {
	n   int
	err error
}

// newPacketTranslationDeadlineTestConn creates the queue and deadline state
// needed to exercise PacketConn operations without translation workers.
func newPacketTranslationDeadlineTestConn(t *testing.T, log Logger) *packetTranslation {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	if log == nil {
		log = NewNoopLogger()
	}
	self := &packetTranslation{
		ctx:                  ctx,
		cancel:               cancel,
		log:                  log,
		in:                   make(chan *packet),
		out:                  make(chan *packet),
		forward:              make(chan *packet),
		readDeadlineMonitor:  NewMonitor(),
		writeDeadlineMonitor: NewMonitor(),
	}
	t.Cleanup(cancel)
	return self
}

// awaitPacketTranslationDeadlineTimer waits for the operation to arm its
// controlled timer. The timeout is only a deadlock guard; the timer hook is
// the synchronization barrier.
func awaitPacketTranslationDeadlineTimer(t *testing.T, timer *packetTranslationDeadlineTimer) time.Duration {
	t.Helper()
	select {
	case timeout := <-timer.requested:
		return timeout
	case <-time.After(5 * time.Second):
		t.Fatal("packet translation did not arm its deadline timer")
		return 0
	}
}

// awaitPacketTranslationDeadlineRead waits for a deterministic read result.
func awaitPacketTranslationDeadlineRead(
	t *testing.T,
	result <-chan packetTranslationDeadlineReadResult,
) packetTranslationDeadlineReadResult {
	t.Helper()
	select {
	case readResult := <-result:
		return readResult
	case <-time.After(5 * time.Second):
		t.Fatal("packet translation deadline read did not return")
		return packetTranslationDeadlineReadResult{}
	}
}

// awaitPacketTranslationDeadlineWrite waits for a deterministic write result.
func awaitPacketTranslationDeadlineWrite(
	t *testing.T,
	result <-chan packetTranslationDeadlineWriteResult,
) packetTranslationDeadlineWriteResult {
	t.Helper()
	select {
	case writeResult := <-result:
		return writeResult
	case <-time.After(5 * time.Second):
		t.Fatal("packet translation deadline write did not return")
		return packetTranslationDeadlineWriteResult{}
	}
}

// assertPacketTranslationDeadlineError verifies both the portable errors.Is
// contract and the net.Error contract used by quic-go's packet read loop.
func assertPacketTranslationDeadlineError(t *testing.T, err error, operation string) {
	t.Helper()
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("%s error does not wrap os.ErrDeadlineExceeded: %v", operation, err)
	}
	var networkError net.Error
	if !errors.As(err, &networkError) {
		t.Fatalf("%s error does not implement net.Error: %T %v", operation, err, err)
	}
	if !networkError.Timeout() {
		t.Fatalf("%s net.Error does not report Timeout: %v", operation, err)
	}
	// quic-go uses Temporary to recognize the SetReadDeadline wakeup it installs
	// while closing its transport.
	if !networkError.Temporary() {
		t.Fatalf("%s net.Error does not report Temporary: %v", operation, err)
	}
	var operationError *net.OpError
	if !errors.As(err, &operationError) {
		t.Fatalf("%s error is not a net.OpError: %T %v", operation, err, err)
	}
	if operationError.Op != operation {
		t.Fatalf("deadline operation = %q, want %q", operationError.Op, operation)
	}
}

// assertPacketTranslationClosedError verifies the portable closed-connection
// identity and the operation attached by the PacketConn implementation.
func assertPacketTranslationClosedError(t *testing.T, err error, operation string) {
	t.Helper()
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("%s error does not wrap net.ErrClosed: %v", operation, err)
	}
	var operationError *net.OpError
	if !errors.As(err, &operationError) {
		t.Fatalf("%s error is not a net.OpError: %T %v", operation, err, err)
	}
	if operationError.Op != operation {
		t.Fatalf("closed operation = %q, want %q", operationError.Op, operation)
	}
}

// attachPacketTranslationDeadlineSocket gives Close a real underlying
// PacketConn of the family under test while the translation queues remain
// controlled by the test.
func attachPacketTranslationDeadlineSocket(t *testing.T, ipVersion int, translation *packetTranslation) {
	t.Helper()
	packetConn, err := net.ListenPacket(testUdpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
	if err != nil {
		t.Fatal(err)
	}
	translation.packetConn = packetConn
	t.Cleanup(func() { _ = packetConn.Close() })
}

// packetTranslationCapturedLogs returns stable copies of captured INFO and
// verbose records.
func packetTranslationCapturedLogs(log *captureLogger) ([]string, []string) {
	log.mu.Lock()
	defer log.mu.Unlock()
	return append([]string(nil), log.info...), append([]string(nil), log.verbose...)
}

func TestPacketTranslationReadImmediateDeadlineErrorContract(t *testing.T) {
	translation := newPacketTranslationDeadlineTestConn(t, nil)
	if err := translation.SetReadDeadline(time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}

	n, addr, err := translation.ReadFrom(make([]byte, 1))
	if n != 0 || addr != nil {
		t.Fatalf("expired read returned n=%d addr=%v", n, addr)
	}
	assertPacketTranslationDeadlineError(t, err, "read")
}

func TestPacketTranslationReadTimerDeadlineErrorContract(t *testing.T) {
	log := &captureLogger{enabled: true}
	translation := newPacketTranslationDeadlineTestConn(t, log)
	timer := newPacketTranslationDeadlineTimer()
	translation.deadlineAfterForTest = timer.after
	if err := translation.SetReadDeadline(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	result := make(chan packetTranslationDeadlineReadResult, 1)
	go func() {
		n, addr, err := translation.ReadFrom(make([]byte, 1))
		result <- packetTranslationDeadlineReadResult{
			n:    n,
			addr: addr,
			err:  err,
		}
	}()
	if timeout := awaitPacketTranslationDeadlineTimer(t, timer); timeout <= 0 {
		t.Fatalf("read timer duration = %s, want positive", timeout)
	}
	timer.expired <- time.Now()

	readResult := awaitPacketTranslationDeadlineRead(t, result)
	if readResult.n != 0 || readResult.addr != nil {
		t.Fatalf("expired read returned n=%d addr=%v", readResult.n, readResult.addr)
	}
	assertPacketTranslationDeadlineError(t, readResult.err, "read")
	info, verbose := packetTranslationCapturedLogs(log)
	if len(info) != 1 || !strings.Contains(info[0], "[pt]read packet timeout") {
		t.Fatalf("elapsed read deadline INFO = %v, want one timeout", info)
	}
	if len(verbose) != 0 {
		t.Fatalf("elapsed read deadline unexpectedly logged at verbose: %v", verbose)
	}
}

func TestPacketTranslationWriteImmediateQueueDeadlineErrorContract(t *testing.T) {
	translation := newPacketTranslationDeadlineTestConn(t, nil)
	if err := translation.SetWriteDeadline(time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}

	n, err := translation.WriteTo([]byte{1}, &net.UDPAddr{})
	if n != 0 {
		t.Fatalf("expired write returned n=%d", n)
	}
	assertPacketTranslationDeadlineError(t, err, "write")
}

func TestPacketTranslationWriteTimerQueueDeadlineErrorContract(t *testing.T) {
	translation := newPacketTranslationDeadlineTestConn(t, nil)
	timer := newPacketTranslationDeadlineTimer()
	translation.deadlineAfterForTest = timer.after
	if err := translation.SetWriteDeadline(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	result := make(chan packetTranslationDeadlineWriteResult, 1)
	go func() {
		n, err := translation.WriteTo([]byte{1}, &net.UDPAddr{})
		result <- packetTranslationDeadlineWriteResult{
			n:   n,
			err: err,
		}
	}()
	if timeout := awaitPacketTranslationDeadlineTimer(t, timer); timeout <= 0 {
		t.Fatalf("write timer duration = %s, want positive", timeout)
	}
	timer.expired <- time.Now()

	writeResult := awaitPacketTranslationDeadlineWrite(t, result)
	if writeResult.n != 0 {
		t.Fatalf("expired write returned n=%d", writeResult.n)
	}
	assertPacketTranslationDeadlineError(t, writeResult.err, "write")
}

func TestPacketTranslationWriteImmediateWireDeadlineErrorContract(t *testing.T) {
	translation := newPacketTranslationDeadlineTestConn(t, nil)
	if err := translation.SetWriteDeadline(time.Unix(1, 0)); err != nil {
		t.Fatal(err)
	}

	n, err := translation.waitForWireWrite(make(chan error), 1)
	if n != 0 {
		t.Fatalf("expired wire write returned n=%d", n)
	}
	assertPacketTranslationDeadlineError(t, err, "write")
}

func TestPacketTranslationWriteTimerWireDeadlineErrorContract(t *testing.T) {
	translation := newPacketTranslationDeadlineTestConn(t, nil)
	timer := newPacketTranslationDeadlineTimer()
	translation.deadlineAfterForTest = timer.after
	if err := translation.SetWriteDeadline(time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}

	result := make(chan packetTranslationDeadlineWriteResult, 1)
	go func() {
		n, err := translation.waitForWireWrite(make(chan error), 1)
		result <- packetTranslationDeadlineWriteResult{
			n:   n,
			err: err,
		}
	}()
	if timeout := awaitPacketTranslationDeadlineTimer(t, timer); timeout <= 0 {
		t.Fatalf("wire write timer duration = %s, want positive", timeout)
	}
	timer.expired <- time.Now()

	writeResult := awaitPacketTranslationDeadlineWrite(t, result)
	if writeResult.n != 0 {
		t.Fatalf("expired wire write returned n=%d", writeResult.n)
	}
	assertPacketTranslationDeadlineError(t, writeResult.err, "write")
}

func TestPacketTranslationClosedReadImmediateErrorContract(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		translation := newPacketTranslationDeadlineTestConn(t, nil)
		attachPacketTranslationDeadlineSocket(t, ipVersion, translation)
		if err := translation.Close(); err != nil {
			t.Fatal(err)
		}

		n, addr, err := translation.ReadFrom(make([]byte, 1))
		if n != 0 || addr != nil {
			t.Fatalf("closed read returned n=%d addr=%v", n, addr)
		}
		assertPacketTranslationClosedError(t, err, "read")
	})
}

func TestPacketTranslationClosedWriteImmediateErrorContract(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		translation := newPacketTranslationDeadlineTestConn(t, nil)
		attachPacketTranslationDeadlineSocket(t, ipVersion, translation)
		if err := translation.Close(); err != nil {
			t.Fatal(err)
		}

		n, err := translation.WriteTo([]byte{1}, &net.UDPAddr{})
		if n != 0 {
			t.Fatalf("closed write returned n=%d", n)
		}
		assertPacketTranslationClosedError(t, err, "write")
	})
}

func TestPacketTranslationClosedReadBlockedErrorContract(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		translation := newPacketTranslationDeadlineTestConn(t, nil)
		attachPacketTranslationDeadlineSocket(t, ipVersion, translation)
		timer := newPacketTranslationDeadlineTimer()
		translation.deadlineAfterForTest = timer.after
		if err := translation.SetReadDeadline(time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}

		result := make(chan packetTranslationDeadlineReadResult, 1)
		go func() {
			n, addr, err := translation.ReadFrom(make([]byte, 1))
			result <- packetTranslationDeadlineReadResult{
				n:    n,
				addr: addr,
				err:  err,
			}
		}()
		if timeout := awaitPacketTranslationDeadlineTimer(t, timer); timeout <= 0 {
			t.Fatalf("read timer duration = %s, want positive", timeout)
		}
		if err := translation.Close(); err != nil {
			t.Fatal(err)
		}

		readResult := awaitPacketTranslationDeadlineRead(t, result)
		if readResult.n != 0 || readResult.addr != nil {
			t.Fatalf("closed read returned n=%d addr=%v", readResult.n, readResult.addr)
		}
		assertPacketTranslationClosedError(t, readResult.err, "read")
	})
}

func TestPacketTranslationClosedWriteQueueBlockedErrorContract(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		translation := newPacketTranslationDeadlineTestConn(t, nil)
		attachPacketTranslationDeadlineSocket(t, ipVersion, translation)
		timer := newPacketTranslationDeadlineTimer()
		translation.deadlineAfterForTest = timer.after
		if err := translation.SetWriteDeadline(time.Now().Add(time.Hour)); err != nil {
			t.Fatal(err)
		}

		result := make(chan packetTranslationDeadlineWriteResult, 1)
		go func() {
			n, err := translation.WriteTo([]byte{1}, &net.UDPAddr{})
			result <- packetTranslationDeadlineWriteResult{
				n:   n,
				err: err,
			}
		}()
		if timeout := awaitPacketTranslationDeadlineTimer(t, timer); timeout <= 0 {
			t.Fatalf("write timer duration = %s, want positive", timeout)
		}
		if err := translation.Close(); err != nil {
			t.Fatal(err)
		}

		writeResult := awaitPacketTranslationDeadlineWrite(t, result)
		if writeResult.n != 0 {
			t.Fatalf("closed write returned n=%d", writeResult.n)
		}
		assertPacketTranslationClosedError(t, writeResult.err, "write")
	})
}

func TestPacketTranslationClosedWriteWireBlockedErrorContract(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		translation := newPacketTranslationDeadlineTestConn(t, nil)
		attachPacketTranslationDeadlineSocket(t, ipVersion, translation)

		result := make(chan packetTranslationDeadlineWriteResult, 1)
		go func() {
			n, err := translation.WriteTo([]byte{1}, &net.UDPAddr{})
			result <- packetTranslationDeadlineWriteResult{
				n:   n,
				err: err,
			}
		}()
		queued := <-translation.out
		defer MessagePoolReturn(queued.data)
		if err := translation.Close(); err != nil {
			t.Fatal(err)
		}

		writeResult := awaitPacketTranslationDeadlineWrite(t, result)
		if writeResult.n != 0 {
			t.Fatalf("closed wire write returned n=%d", writeResult.n)
		}
		assertPacketTranslationClosedError(t, writeResult.err, "write")
	})
}

func TestPacketTranslationQuicCloseDeadlineWakeupIsVerbose(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		log := &captureLogger{enabled: true}
		translation := newPacketTranslationDeadlineTestConn(t, log)
		attachPacketTranslationDeadlineSocket(t, ipVersion, translation)
		t.Cleanup(func() {
			if err := translation.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				t.Errorf("close packet translation: %v", err)
			}
		})

		transport := &quic.Transport{Conn: translation}
		if err := transport.Close(); err != nil {
			t.Fatalf("close quic transport: %v", err)
		}

		info, verbose := packetTranslationCapturedLogs(log)
		for _, line := range info {
			if strings.Contains(line, "[pt]read packet timeout") {
				t.Fatalf("quic close wakeup logged as INFO timeout: %v", info)
			}
		}
		foundWakeup := false
		for _, line := range verbose {
			if strings.Contains(line, "[pt]read packet deadline wakeup") {
				foundWakeup = true
			}
		}
		if !foundWakeup {
			t.Fatalf("quic close wakeup verbose log missing: %v", verbose)
		}
		readDeadline, wakeup, _ := translation.currentReadDeadline()
		if !readDeadline.IsZero() || wakeup {
			t.Fatalf("quic close left read deadline=%s wakeup=%t", readDeadline, wakeup)
		}
	})
}
