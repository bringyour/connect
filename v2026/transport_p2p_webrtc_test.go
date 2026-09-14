//go:build !js

package connect

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	mathrand "math/rand"
	"net"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/webrtc/v4"

	"github.com/urnetwork/connect/v2026/protocol"
)

func TestWebRtc(t *testing.T) {
	// Keep the transport smoke test hermetic. Public STUN resolution and
	// internet reachability are orthogonal to the local SCTP data-path check
	// and made a late-suite failure look like a data-path stall.
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	settingsA := DefaultWebRtcSettings()
	settingsB := DefaultWebRtcSettings()
	settingsA.Log = NewNoopLogger()
	settingsB.Log = NewNoopLogger()
	settingsA.IceServerUrls = nil
	settingsB.IceServerUrls = nil
	// Both peers run on this host, so restrict ICE to loopback. The
	// unrestricted view floods the pair list on a multihomed host (utun,
	// bridge, AWDL, VM interfaces) and STUN check pacing then pushes the
	// local connect past the deadline; egress-only instead turns the test
	// into a macOS UDP-to-self hairpin test, which intermittently drops
	// rapid close/rebind churn. Neither says anything about the real
	// two-device ICE path. The filtered production interface view is
	// validated independently below.
	settingsA.UseLoopbackOnlyIceInterfaces = true
	settingsB.UseLoopbackOnlyIceInterfaces = true
	// This raw net.Conn smoke concatenates several messages with io.ReadFull,
	// so it explicitly requests SCTP ordering. Production keeps the channel
	// unordered and hands self-sequenced TransferFrames to its reorder layer;
	// TestWebRtcMessageRoundTrip exercises that default independently below.
	settingsA.DataChannelOrdered = true
	settingsB.DataChannelOrdered = true

	// each manager sends signals to each other
	signalPipeA := newSignalPipe(nil)
	signalPipeB := newSignalPipe(nil)

	webRtcManagerA := newTestWebRtcManager(t, ctx, signalPipeA, settingsA)
	webRtcManagerB := newTestWebRtcManager(t, ctx, signalPipeB, settingsB)

	signalPipeA.SetSignalReceiver(webRtcManagerB)
	signalPipeB.SetSignalReceiver(webRtcManagerA)

	peerIdA := NewId()
	peerIdB := NewId()
	streamId := NewId()

	connB, err := webRtcManagerB.NewP2pConnPassive(ctx, NewTransferPath(peerIdB, peerIdA, streamId))
	AssertEqual(t, err, nil)
	defer connB.Close()

	// Register the passive endpoint before the active side emits its offer.
	// The in-memory signal pipe deliberately does not queue signals for a
	// receiver that has not registered the corresponding stream yet.
	connA, err := webRtcManagerA.NewP2pConnActive(ctx, NewTransferPath(peerIdA, peerIdB, streamId))
	AssertEqual(t, err, nil)
	defer connA.Close()

	connectedDeadline := time.Now().Add(10 * time.Second)
	for !connA.Connected() || !connB.Connected() {
		if time.Now().After(connectedDeadline) {
			t.Fatalf(
				"local peer setup stalled: active_connected=%t passive_connected=%t",
				connA.Connected(),
				connB.Connected(),
			)
		}
		time.Sleep(time.Millisecond)
	}

	dataDeadline := time.Now().Add(30 * time.Second)
	AssertEqual(t, connA.SetDeadline(dataDeadline), nil)
	AssertEqual(t, connB.SetDeadline(dataDeadline), nil)

	b := make([]byte, 1024*1024)
	mathrand.Read(b)

	type ioResult struct {
		operation string
		payload   []byte
		err       error
	}
	results := make(chan ioResult, 4)

	// send in transport-sized messages: the detached datachannel is
	// message-oriented, and a single message must fit within the
	// per-connection ReceiveBufferSize to be reassembled (production frames
	// are bounded by the transport MaxMessageByteCount default)
	const sendMessageByteCount = 64 * 1024
	send := func(name string, conn net.Conn) {
		for i := 0; i < len(b); i += sendMessageByteCount {
			end := min(i+sendMessageByteCount, len(b))
			n, writeErr := conn.Write(b[i:end])
			if writeErr != nil {
				results <- ioResult{operation: name, err: writeErr}
				return
			}
			if n != end-i {
				results <- ioResult{operation: name, err: io.ErrShortWrite}
				return
			}
		}
		results <- ioResult{operation: name}
	}
	receive := func(name string, conn net.Conn) {
		b2 := make([]byte, len(b))
		if _, readErr := io.ReadFull(conn, b2); readErr != nil {
			results <- ioResult{operation: name, err: readErr}
			return
		}
		results <- ioResult{operation: name, payload: b2}
	}

	go send("A write", connA)
	go receive("A read", connA)
	go send("B write", connB)
	go receive("B read", connB)

	receiveCount := 0
	for range 4 {
		select {
		case <-ctx.Done():
			t.Fatalf("local SCTP transfer stalled: %v", context.Cause(ctx))
		case result := <-results:
			if result.err != nil {
				t.Fatalf("%s failed: %v", result.operation, result.err)
			}
			if result.payload != nil {
				AssertEqual(t, b, result.payload)
				receiveCount++
			}
		}
	}
	if receiveCount != 2 {
		t.Fatalf("completed reads = %d, want 2", receiveCount)
	}
}

func TestDefaultWebRtcCongestionTuningUsesMeasuredPredictableKnee(t *testing.T) {
	settings := DefaultWebRtcSettings()
	if got, want := settings.SctpCwndCAStep, uint32(8*1200); got != want {
		t.Fatalf("SCTP congestion-avoidance step = %d, want measured knee %d", got, want)
	}
	if settings.SctpMinCwnd != 0 {
		t.Fatalf("SCTP minimum cwnd = %d; a floor creates standing-queue latency", settings.SctpMinCwnd)
	}
	if settings.SctpFastRtxWnd != 0 {
		t.Fatalf("SCTP fast-retransmit burst override = %d; expected stock bounded recovery", settings.SctpFastRtxWnd)
	}
}

func TestAcknowledgedSctpByteCountTracksOnlyForwardQueueProgress(t *testing.T) {
	tests := []struct {
		name               string
		outboundByteCount  uint64
		bufferedAmount     int
		acknowledgedAmount uint64
	}{
		{
			name:               "new write remains buffered",
			outboundByteCount:  1024,
			bufferedAmount:     1024,
			acknowledgedAmount: 0,
		},
		{
			name:               "continuous writes offset partial acknowledgements",
			outboundByteCount:  4096,
			bufferedAmount:     3072,
			acknowledgedAmount: 1024,
		},
		{
			name:               "all accepted bytes acknowledged",
			outboundByteCount:  4096,
			bufferedAmount:     0,
			acknowledgedAmount: 4096,
		},
		{
			name:               "untracked association control bytes clamp at zero",
			outboundByteCount:  128,
			bufferedAmount:     256,
			acknowledgedAmount: 0,
		},
	}
	for _, test := range tests {
		if got := acknowledgedSctpByteCount(
			test.outboundByteCount,
			test.bufferedAmount,
		); got != test.acknowledgedAmount {
			t.Errorf(
				"%s: acknowledged bytes = %d, want %d",
				test.name,
				got,
				test.acknowledgedAmount,
			)
		}
	}
}

func TestObservedAcknowledgedSctpByteCountRecognizesDeadlineEdgeProgress(t *testing.T) {
	acknowledged, progressed := observeAcknowledgedSctpByteCount(
		0,
		4096,
		3072,
	)
	if acknowledged != 1024 {
		t.Fatalf("acknowledged bytes = %d, want 1024", acknowledged)
	}
	if !progressed {
		t.Fatal("fresh deadline-edge sample did not report forward progress")
	}
}

func TestObservedAcknowledgedSctpByteCountDoesNotRegressAcrossRacingWrites(t *testing.T) {
	acknowledged, progressed := observeAcknowledgedSctpByteCount(
		1024,
		4096,
		3584,
	)
	if acknowledged != 1024 {
		t.Fatalf("acknowledged bytes regressed to %d, want 1024", acknowledged)
	}
	if progressed {
		t.Fatal("lower racing observation reported forward progress")
	}
}

func TestPeerConnFailureCancellationMarksSharedAdmissionRetiringSynchronously(t *testing.T) {
	const reservation = ByteCount(1024)
	budget := NewTransferMemoryBudget(reservation)
	ownerCtx, ownerCancel := context.WithCancel(context.Background())
	owner := &peerConnectionAdmissionOwner{
		ctx:    ownerCtx,
		cancel: ownerCancel,
	}
	if !budget.tryReservePeerConnectionOwner(owner, reservation) {
		t.Fatal("failed to reserve test admission owner")
	}
	defer owner.release()

	connCtx, cancelCause := context.WithCancelCause(context.Background())
	conn := &peerConn{
		ctx:            connCtx,
		cancelCause:    cancelCause,
		admissionOwner: owner,
	}
	expectedCause := errors.New("transport failed")
	conn.cancelBecause(expectedCause)

	if cause := context.Cause(connCtx); !errors.Is(cause, expectedCause) {
		t.Fatalf("cancellation cause = %v, want %v", cause, expectedCause)
	}
	liveCount, retiringCount := budget.peerConnectionOwnerCounts()
	if liveCount != 0 || retiringCount != 1 {
		t.Fatalf(
			"admission owners after failure = live:%d retiring:%d, want live:0 retiring:1",
			liveCount,
			retiringCount,
		)
	}
}

func TestWebRtcPeerRunStartupFailureRetiresAdmissionSynchronously(t *testing.T) {
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.DataChannelLabel = strings.Repeat("x", 65536)

	factory, _, err := newWebRtcPeerConnectionFactory(settings, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer factory.Close()

	const reservation = ByteCount(1024)
	budget := NewTransferMemoryBudget(reservation)
	ownerCtx, ownerCancel := context.WithCancel(context.Background())
	owner := &peerConnectionAdmissionOwner{
		ctx:    ownerCtx,
		cancel: ownerCancel,
	}
	if !budget.tryReservePeerConnectionOwner(owner, reservation) {
		t.Fatal("failed to reserve test admission owner")
	}
	defer owner.release()

	conn, err := newPeerConn(
		ownerCtx,
		peerConnKey{PeerId: NewId(), StreamId: NewId()},
		NewId(),
		true,
		newSignalPipe(nil),
		settings,
		func() (*webrtc.PeerConnection, context.CancelFunc, error) {
			return factory.NewPeerConnection(false)
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	conn.admissionOwner = owner
	defer conn.teardown()

	conn.Run()
	if cause := context.Cause(conn.ctx); cause == nil ||
		!strings.Contains(cause.Error(), "create data channel") {
		t.Fatalf("startup failure cancellation cause = %v", cause)
	}
	liveCount, retiringCount := budget.peerConnectionOwnerCounts()
	if liveCount != 0 || retiringCount != 1 {
		t.Fatalf(
			"admission owners after startup failure = live:%d retiring:%d, want live:0 retiring:1",
			liveCount,
			retiringCount,
		)
	}
}

// matchExpectedUnorderedP2pMessage consumes one exact, not-yet-seen message
// from an unordered reliable carrier.
func matchExpectedUnorderedP2pMessage(
	message []byte,
	expectedMessages [][]byte,
	seen []bool,
) (int, bool) {
	for messageIndex, expected := range expectedMessages {
		if !seen[messageIndex] && bytes.Equal(message, expected) {
			seen[messageIndex] = true
			return messageIndex, true
		}
	}
	return -1, false
}

// A fixed non-network permutation proves that validation follows the default
// carrier contract instead of silently restoring an ordered-stream assumption.
func TestMatchExpectedUnorderedP2pMessagesAcceptsPermutation(t *testing.T) {
	expectedMessages := [][]byte{
		[]byte("first"),
		[]byte("second"),
		[]byte("third"),
	}
	seen := make([]bool, len(expectedMessages))
	for _, messageIndex := range []int{2, 0, 1} {
		matchedIndex, ok := matchExpectedUnorderedP2pMessage(
			expectedMessages[messageIndex],
			expectedMessages,
			seen,
		)
		if !ok || matchedIndex != messageIndex {
			t.Fatalf(
				"permuted message %d matched index=%d ok=%t",
				messageIndex,
				matchedIndex,
				ok,
			)
		}
	}
}

// Duplicate and corrupted messages remain failures even though position does
// not participate in reliable-unordered validation.
func TestMatchExpectedUnorderedP2pMessagesRejectsInvalidContent(t *testing.T) {
	expectedMessages := [][]byte{[]byte("first"), []byte("second")}
	seen := make([]bool, len(expectedMessages))
	if _, ok := matchExpectedUnorderedP2pMessage(
		expectedMessages[0],
		expectedMessages,
		seen,
	); !ok {
		t.Fatal("first exact message was rejected")
	}
	if _, ok := matchExpectedUnorderedP2pMessage(
		expectedMessages[0],
		expectedMessages,
		seen,
	); ok {
		t.Fatal("duplicate message was accepted")
	}
	if _, ok := matchExpectedUnorderedP2pMessage(
		[]byte("corrupted"),
		expectedMessages,
		seen,
	); ok {
		t.Fatal("corrupted message was accepted")
	}
}

// TestWebRtcMessageRoundTrip verifies the P2P transport's native message
// framing: the detached data channel is message-oriented (one Write becomes one
// SCTP message the peer reads back whole), so consecutive TransferFrames of
// varied sizes must each arrive intact without a length prefix. Their arrival
// order is deliberately unconstrained: production uses reliable-unordered SCTP
// and the Transfer layer orders each self-sequenced frame. The receive side
// mirrors P2pReceiveTransport, including detached Pion's n=0/io.ErrShortBuffer
// behavior for messages above the first 4 KiB attempt.
func TestWebRtcMessageRoundTrip(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	settingsA := DefaultWebRtcSettings()
	settingsB := DefaultWebRtcSettings()
	settingsA.Log = NewNoopLogger()
	settingsB.Log = NewNoopLogger()
	// Hermetic same-host mode, like every other establishing test in this
	// file (see the TestWebRtc comment): the default settings gather the
	// multihomed interface cross-product and query live STUN servers, so
	// establishment time depends on the host's interface population and WAN.
	// That non-determinism failed this test at its full 45s deadline
	// ("write 0: file already closed" — the conn never established) in a
	// full-suite -race run.
	settingsA.IceServerUrls = nil
	settingsB.IceServerUrls = nil
	settingsA.UseLoopbackOnlyIceInterfaces = true
	settingsB.UseLoopbackOnlyIceInterfaces = true

	signalPipeA := newSignalPipe(nil)
	signalPipeB := newSignalPipe(nil)

	webRtcManagerA := newTestWebRtcManager(t, ctx, signalPipeA, settingsA)
	webRtcManagerB := newTestWebRtcManager(t, ctx, signalPipeB, settingsB)

	signalPipeA.signalReceiver = webRtcManagerB
	signalPipeB.signalReceiver = webRtcManagerA

	peerIdA := NewId()
	peerIdB := NewId()
	streamId := NewId()

	connA, err := webRtcManagerA.NewP2pConnActive(ctx, NewTransferPath(peerIdA, peerIdB, streamId))
	AssertEqual(t, err, nil)
	defer connA.Close()

	connB, err := webRtcManagerB.NewP2pConnPassive(ctx, NewTransferPath(peerIdB, peerIdA, streamId))
	AssertEqual(t, err, nil)
	defer connB.Close()

	sizes := []int{1, 100, 255, 256, 257, 1000, int(kib(4)), int(kib(6)), int(kib(12))}
	messages := make([][]byte, len(sizes))
	for i, size := range sizes {
		m := make([]byte, size)
		for j := range m {
			m[j] = byte((i*31 + j) % 256)
		}
		messages[i] = m
	}

	readErr := make(chan error, 1)
	go func() {
		seen := make([]bool, len(messages))
		for receiveIndex := range messages {
			got, err := readP2pMessage(
				connB,
				int(kib(4)),
				int(kib(4)),
				int(settingsB.MaxMessageSize),
			)
			if err != nil {
				readErr <- fmt.Errorf("read %d: %w", receiveIndex, err)
				return
			}
			if _, ok := matchExpectedUnorderedP2pMessage(got, messages, seen); !ok {
				gotByteCount := len(got)
				MessagePoolReturn(got)
				readErr <- fmt.Errorf(
					"read %d returned an unexpected or duplicate %d-byte message",
					receiveIndex,
					gotByteCount,
				)
				return
			}
			MessagePoolReturn(got)
		}
		readErr <- nil
	}()

	for i := range messages {
		if _, err := connA.Write(messages[i]); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	select {
	case <-ctx.Done():
		t.Fatal("timeout waiting for frames")
	case err := <-readErr:
		AssertEqual(t, err, nil)
	}

	// The SCTP association enforces the same hard message bound as the P2P
	// transport instead of advertising Pion's ~1 GiB default and relying on a
	// later 64 KiB read buffer to reject an already-reassembled message.
	if _, err := connA.Write(make([]byte, int(settingsA.MaxMessageSize)+1)); err == nil {
		t.Fatal("SCTP accepted a message above the bounded transport maximum")
	}
}

func TestDefaultWebRtcDataChannelIsReliableUnordered(t *testing.T) {
	init := webRtcDataChannelInit(DefaultWebRtcSettings())
	if init.Ordered == nil {
		t.Fatal("data channel ordering was left at Pion's ordered default")
	}
	AssertEqual(t, *init.Ordered, false)
	AssertEqual(t, init.MaxRetransmits, (*uint16)(nil))
	AssertEqual(t, init.MaxPacketLifeTime, (*uint16)(nil))
}

type shortBufferMessageConn struct {
	lock            sync.Mutex
	message         []byte
	delivered       bool
	reportSize      bool
	readSizes       []int
	deliveredBuffer []byte
	closed          chan struct{}
	closeOnce       sync.Once
}

func newShortBufferMessageConn(message []byte) *shortBufferMessageConn {
	return &shortBufferMessageConn{
		message:    message,
		reportSize: true,
		closed:     make(chan struct{}),
	}
}

func (self *shortBufferMessageConn) Read(b []byte) (int, error) {
	self.lock.Lock()
	self.readSizes = append(self.readSizes, len(b))
	if !self.delivered {
		if len(b) < len(self.message) {
			requiredByteCount := 0
			if self.reportSize {
				requiredByteCount = len(self.message)
			}
			self.lock.Unlock()
			// SCTP reports the complete queued message length, while detached
			// Pion masks it to zero. Both leave the message available to retry.
			return requiredByteCount, io.ErrShortBuffer
		}
		n := copy(b, self.message)
		self.delivered = true
		self.deliveredBuffer = b[:n]
		self.lock.Unlock()
		return n, nil
	}
	closed := self.closed
	self.lock.Unlock()
	<-closed
	return 0, net.ErrClosed
}

func (self *shortBufferMessageConn) Write([]byte) (int, error) {
	return 0, net.ErrClosed
}

func (self *shortBufferMessageConn) Close() error {
	self.closeOnce.Do(func() {
		close(self.closed)
	})
	return nil
}

func (self *shortBufferMessageConn) LocalAddr() net.Addr {
	return testingNetAddr("local")
}

func (self *shortBufferMessageConn) RemoteAddr() net.Addr {
	return testingNetAddr("remote")
}

func (self *shortBufferMessageConn) SetDeadline(time.Time) error {
	return nil
}

func (self *shortBufferMessageConn) SetReadDeadline(time.Time) error {
	return nil
}

func (self *shortBufferMessageConn) SetWriteDeadline(time.Time) error {
	return nil
}

type testingNetAddr string

func (self testingNetAddr) Network() string {
	return "test"
}

func (self testingNetAddr) String() string {
	return string(self)
}

type queuedMessageConn struct {
	lock       sync.Mutex
	messages   [][]byte
	reportSize bool
	readSizes  []int
	closed     chan struct{}
	closeOnce  sync.Once
}

func newQueuedMessageConn(messages ...[]byte) *queuedMessageConn {
	return &queuedMessageConn{
		messages:   messages,
		reportSize: true,
		closed:     make(chan struct{}),
	}
}

func (self *queuedMessageConn) Read(b []byte) (int, error) {
	self.lock.Lock()
	self.readSizes = append(self.readSizes, len(b))
	if 0 < len(self.messages) {
		message := self.messages[0]
		if len(b) < len(message) {
			requiredByteCount := 0
			if self.reportSize {
				requiredByteCount = len(message)
			}
			self.lock.Unlock()
			return requiredByteCount, io.ErrShortBuffer
		}
		self.messages = self.messages[1:]
		n := copy(b, message)
		self.lock.Unlock()
		return n, nil
	}
	closed := self.closed
	self.lock.Unlock()
	<-closed
	return 0, net.ErrClosed
}

func (self *queuedMessageConn) Write([]byte) (int, error) {
	return 0, net.ErrClosed
}

func (self *queuedMessageConn) Close() error {
	self.closeOnce.Do(func() {
		close(self.closed)
	})
	return nil
}

func (self *queuedMessageConn) LocalAddr() net.Addr {
	return testingNetAddr("local")
}

func (self *queuedMessageConn) RemoteAddr() net.Addr {
	return testingNetAddr("remote")
}

func (self *queuedMessageConn) SetDeadline(time.Time) error {
	return nil
}

func (self *queuedMessageConn) SetReadDeadline(time.Time) error {
	return nil
}

func (self *queuedMessageConn) SetWriteDeadline(time.Time) error {
	return nil
}

func TestP2pReadyHeaderPrefetchesUnorderedDataWithinRouteBound(t *testing.T) {
	earlyA := bytes.Repeat([]byte{0xa1}, 12*1024)
	earlyB := bytes.Repeat([]byte{0xb2}, 5*1024)
	earlyDropped := bytes.Repeat([]byte{0xc3}, 6*1024)
	steady := bytes.Repeat([]byte{0xd4}, 7*1024)
	conn := newQueuedMessageConn(
		earlyA,
		earlyB,
		earlyDropped,
		[]byte(ReadyHeader),
		steady,
	)
	// Match detached Pion, which masks SCTP's required byte count to zero.
	conn.reportSize = false
	defer conn.Close()
	settings := DefaultP2pTransportSettings()
	settings.ChannelBufferSize = 2

	prefetched, err := readP2pReadyHeader(conn, settings)
	AssertEqual(t, err, nil)
	if len(prefetched) != settings.ChannelBufferSize {
		t.Fatalf("prefetched %d messages, expected hard bound %d", len(prefetched), settings.ChannelBufferSize)
	}

	ctx, cancel := context.WithCancel(context.Background())
	_, route := newP2pReceiveTransport(ctx, cancel, conn, NewId(), settings, prefetched, nil)
	defer cancel()
	for i, expected := range [][]byte{earlyA, earlyB, steady} {
		select {
		case received := <-route:
			if !bytes.Equal(received, expected) {
				MessagePoolReturn(received)
				t.Fatalf("message %d changed or reordered at prefetch handoff", i)
			}
			MessagePoolReturn(received)
		case <-time.After(time.Second):
			t.Fatalf("message %d not delivered", i)
		}
	}

	conn.lock.Lock()
	readSizes := append([]int(nil), conn.readSizes...)
	conn.lock.Unlock()
	expectedPrefix := []int{
		len(ReadyHeader), 4 * 1024, 8 * 1024, 16 * 1024,
		len(ReadyHeader), 4 * 1024, 8 * 1024,
		len(ReadyHeader), 4 * 1024, 8 * 1024,
		len(ReadyHeader),
		settings.InitialReadBufferByteCount, 8 * 1024,
	}
	if len(readSizes) < len(expectedPrefix) || !slices.Equal(readSizes[:len(expectedPrefix)], expectedPrefix) {
		t.Fatalf("unexpected adaptive ready/steady reads: got=%v want-prefix=%v", readSizes, expectedPrefix)
	}
}

func TestP2pReceiveTransportGrowsBufferWithoutLosingMessage(t *testing.T) {
	for _, reportSize := range []bool{true, false} {
		func() {
			message := make([]byte, 12*1024)
			for i := range message {
				message[i] = byte(i)
			}
			conn := newShortBufferMessageConn(message)
			conn.reportSize = reportSize
			ctx, cancel := context.WithCancel(context.Background())
			settings := DefaultP2pTransportSettings()
			settings.ChannelBufferSize = 1
			settings.ReadTimeout = time.Hour

			_, route := NewP2pReceiveTransport(ctx, cancel, conn, NewId(), settings)
			select {
			case received := <-route:
				if !bytes.Equal(received, message) {
					t.Fatalf("adaptive read changed message: got=%d want=%d", len(received), len(message))
				}
				conn.lock.Lock()
				deliveredBuffer := conn.deliveredBuffer
				conn.lock.Unlock()
				if len(received) == 0 || len(deliveredBuffer) == 0 ||
					&received[0] != &deliveredBuffer[0] {
					t.Fatal("receive transport copied instead of transferring read-buffer ownership")
				}
				MessagePoolReturn(received)
			case <-time.After(time.Second):
				t.Fatal("adaptive receive did not retry the queued message")
			}

			conn.lock.Lock()
			readSizes := append([]int(nil), conn.readSizes...)
			conn.lock.Unlock()
			expected := []int{4 * 1024, len(message)}
			if !reportSize {
				expected = []int{4 * 1024, 8 * 1024, 16 * 1024}
			}
			if len(readSizes) < len(expected) || !slices.Equal(readSizes[:len(expected)], expected) {
				t.Fatalf("unexpected adaptive reads: got=%v want-prefix=%v", readSizes, expected)
			}
			for _, size := range readSizes {
				if settings.MaxMessageByteCount < size {
					t.Fatalf("receive buffer exceeded hard maximum: %d", size)
				}
			}

			cancel()
			AssertEqual(t, conn.Close(), nil)
		}()
	}
}

func TestP2pReceiveTransportRejectsOversizedShortBufferWithoutPanic(t *testing.T) {
	for _, reportSize := range []bool{true, false} {
		func() {
			settings := DefaultP2pTransportSettings()
			conn := newShortBufferMessageConn(make([]byte, settings.MaxMessageByteCount+1))
			conn.reportSize = reportSize
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			NewP2pReceiveTransport(ctx, cancel, conn, NewId(), settings)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
				t.Fatal("oversized message did not terminate the bounded receiver")
			}
			conn.lock.Lock()
			readSizes := append([]int(nil), conn.readSizes...)
			conn.lock.Unlock()
			expected := []int{settings.InitialReadBufferByteCount}
			if !reportSize {
				expected = []int{4 * 1024, 8 * 1024, 16 * 1024, 32 * 1024, 64 * 1024}
			}
			AssertEqual(t, readSizes, expected)
			AssertEqual(t, conn.Close(), nil)
		}()
	}
}

func TestWebRtcBlockingWriteBackpressureAndDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	settingsA := DefaultWebRtcSettings()
	settingsB := DefaultWebRtcSettings()
	settingsA.Log = NewNoopLogger()
	settingsB.Log = NewNoopLogger()
	settingsA.IceServerUrls = nil
	settingsB.IceServerUrls = nil
	// hermetic same-host connect: loopback-only ICE (see TestWebRtc)
	settingsA.UseLoopbackOnlyIceInterfaces = true
	settingsB.UseLoopbackOnlyIceInterfaces = true
	settingsA.ReceiveBufferSize = kib(128)
	settingsB.ReceiveBufferSize = kib(128)

	signalPipeA := newSignalPipe(nil)
	signalPipeB := newSignalPipe(nil)
	managerA := newTestWebRtcManager(t, ctx, signalPipeA, settingsA)
	managerB := newTestWebRtcManager(t, ctx, signalPipeB, settingsB)
	signalPipeA.signalReceiver = managerB
	signalPipeB.signalReceiver = managerA

	peerIdA := NewId()
	peerIdB := NewId()
	streamId := NewId()
	connA, err := managerA.NewP2pConnActive(ctx, NewTransferPath(peerIdA, peerIdB, streamId))
	AssertEqual(t, err, nil)
	defer connA.Close()
	connB, err := managerB.NewP2pConnPassive(ctx, NewTransferPath(peerIdB, peerIdA, streamId))
	AssertEqual(t, err, nil)
	defer connB.Close()

	connectedDeadline := time.Now().Add(5 * time.Second)
	for !connA.Connected() || !connB.Connected() {
		if time.Now().After(connectedDeadline) {
			t.Fatal("peer connections did not connect")
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Never read connB. Its bounded SCTP receive window must eventually
	// propagate all the way back to connA.Write. In non-blocking Pion mode all
	// of these writes merely accumulated in an unbounded pending queue and
	// SetWriteDeadline was explicitly documented as ineffective.
	message := make([]byte, 64*1024)
	var writeErr error
	start := time.Now()
	for range 64 {
		connA.SetWriteDeadline(time.Now().Add(250 * time.Millisecond))
		if _, writeErr = connA.Write(message); writeErr != nil {
			break
		}
	}
	if writeErr == nil {
		t.Fatal("writes did not backpressure after filling the peer receive window")
	}
	if 5*time.Second < time.Since(start) {
		t.Fatalf("write deadline failed to bound backpressure: %s", time.Since(start))
	}
}

func TestWebRtcSctpNoProgressWatchdogPreservesReceiverBackpressure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	settingsA := DefaultWebRtcSettings()
	settingsB := DefaultWebRtcSettings()
	for _, settings := range []*WebRtcSettings{settingsA, settingsB} {
		settings.Log = NewNoopLogger()
		settings.IceServerUrls = nil
		// hermetic same-host connect: loopback-only ICE (see TestWebRtc)
		settings.UseLoopbackOnlyIceInterfaces = true
		settings.ReceiveBufferSize = kib(128)
	}
	settingsA.SctpNoProgressTimeout = 200 * time.Millisecond
	settingsB.SctpNoProgressTimeout = 0

	signalPipeA := newSignalPipe(nil)
	signalPipeB := newSignalPipe(nil)
	managerA := newTestWebRtcManager(t, ctx, signalPipeA, settingsA)
	managerB := newTestWebRtcManager(t, ctx, signalPipeB, settingsB)
	signalPipeA.SetSignalReceiver(managerB)
	signalPipeB.SetSignalReceiver(managerA)

	peerIdA := NewId()
	peerIdB := NewId()
	streamId := NewId()
	passiveValue, err := managerB.NewP2pConnPassive(
		ctx,
		NewTransferPath(peerIdB, peerIdA, streamId),
	)
	if err != nil {
		t.Fatal(err)
	}
	activeValue, err := managerA.NewP2pConnActive(
		ctx,
		NewTransferPath(peerIdA, peerIdB, streamId),
	)
	if err != nil {
		t.Fatal(err)
	}
	active := activeValue.(*peerConn)
	passive := passiveValue.(*peerConn)
	defer active.Close()
	defer passive.Close()

	// Race instrumentation can stretch local ICE/DTLS scheduling several
	// times beyond an ordinary build; setup is not the behavior under test.
	connectedDeadline := time.Now().Add(15 * time.Second)
	for !active.Connected() || !passive.Connected() {
		if time.Now().After(connectedDeadline) {
			t.Fatal("peer connections did not connect")
		}
		time.Sleep(time.Millisecond)
	}

	const messageCount = 8
	message := make([]byte, 64*1024)
	writeDone := make(chan error, 1)
	go func() {
		if err := active.SetWriteDeadline(time.Now().Add(10 * time.Second)); err != nil {
			writeDone <- err
			return
		}
		for range messageCount {
			if n, writeErr := active.Write(message); writeErr != nil {
				writeDone <- writeErr
				return
			} else if n != len(message) {
				writeDone <- io.ErrShortWrite
				return
			}
		}
		writeDone <- nil
	}()

	// Do not read from the passive endpoint. Its bounded receive queue must
	// close the sender's advertised rwnd while accepted writes remain
	// buffered. This is exactly how a deliberately stalled transfer receive
	// or forward callback propagates backpressure through SCTP.
	backpressureDeadline := time.Now().Add(5 * time.Second)
	for {
		sctp := active.pc.SCTP()
		if sctp != nil && sctp.BufferedAmount() != 0 &&
			sctp.Stats().ReceiverWindow == 0 {
			break
		}
		select {
		case writeErr := <-writeDone:
			t.Fatalf("writer completed before receiver backpressure: %v", writeErr)
		default:
		}
		if time.Now().After(backpressureDeadline) {
			t.Fatal("receiver did not advertise a zero SCTP window")
		}
		time.Sleep(time.Millisecond)
	}

	select {
	case <-active.ctx.Done():
		t.Fatal("watchdog canceled intentional receiver backpressure")
	case writeErr := <-writeDone:
		t.Fatalf("writer completed while receiver remained stalled: %v", writeErr)
	case <-time.After(3 * settingsA.SctpNoProgressTimeout):
	}

	if err := passive.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	received := make([]byte, len(message))
	for range messageCount {
		if _, err := io.ReadFull(passive, received); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case writeErr := <-writeDone:
		if writeErr != nil {
			t.Fatal(writeErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not resume after receiver released backpressure")
	}
	select {
	case <-active.ctx.Done():
		t.Fatal("association was canceled after receiver backpressure resumed")
	default:
	}
}

func TestWebRtcSctpSnapMixedCompatibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settingsA := DefaultWebRtcSettings()
	settingsB := DefaultWebRtcSettings()
	settingsA.Log = NewNoopLogger()
	settingsB.Log = NewNoopLogger()
	settingsA.IceServerUrls = nil
	settingsB.IceServerUrls = nil
	// hermetic same-host connect: loopback-only ICE (see TestWebRtc)
	settingsA.UseLoopbackOnlyIceInterfaces = true
	settingsB.UseLoopbackOnlyIceInterfaces = true
	settingsA.EnableSctpSnap = true
	settingsB.EnableSctpSnap = false

	signalPipeA := newSignalPipe(nil)
	signalPipeB := newSignalPipe(nil)
	managerA := newTestWebRtcManager(t, ctx, signalPipeA, settingsA)
	managerB := newTestWebRtcManager(t, ctx, signalPipeB, settingsB)
	signalPipeA.SetSignalReceiver(managerB)
	signalPipeB.SetSignalReceiver(managerA)

	peerIdA := NewId()
	peerIdB := NewId()
	streamId := NewId()
	passive, err := managerB.NewP2pConnPassive(
		ctx,
		NewTransferPath(peerIdB, peerIdA, streamId),
	)
	AssertEqual(t, err, nil)
	defer passive.Close()
	active, err := managerA.NewP2pConnActive(
		ctx,
		NewTransferPath(peerIdA, peerIdB, streamId),
	)
	AssertEqual(t, err, nil)
	defer active.Close()

	payload := []byte("mixed SNAP compatibility")
	AssertEqual(t, active.SetWriteDeadline(time.Now().Add(5*time.Second)), nil)
	received := make(chan []byte, 1)
	go func() {
		b := make([]byte, len(payload))
		if _, readErr := io.ReadFull(passive, b); readErr == nil {
			received <- b
		}
	}()
	_, err = active.Write(payload)
	if err != nil {
		t.Fatalf(
			"SNAP mixed write failed: %v; active={%s} passive={%s}",
			err,
			testingWebRtcConnDiagnostics(active),
			testingWebRtcConnDiagnostics(passive),
		)
	}
	select {
	case b := <-received:
		AssertEqual(t, b, payload)
	case <-ctx.Done():
		t.Fatal("SNAP-enabled peer did not fall back with a disabled peer")
	}
}

func TestWebRtcSctpZeroChecksumMixedCompatibility(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settingsA := DefaultWebRtcSettings()
	settingsB := DefaultWebRtcSettings()
	settingsA.Log = NewNoopLogger()
	settingsB.Log = NewNoopLogger()
	settingsA.IceServerUrls = nil
	settingsB.IceServerUrls = nil
	// hermetic same-host connect: loopback-only ICE (see TestWebRtc)
	settingsA.UseLoopbackOnlyIceInterfaces = true
	settingsB.UseLoopbackOnlyIceInterfaces = true
	settingsA.EnableSctpZeroChecksum = true
	settingsB.EnableSctpZeroChecksum = false

	signalPipeA := newSignalPipe(nil)
	signalPipeB := newSignalPipe(nil)
	managerA := newTestWebRtcManager(t, ctx, signalPipeA, settingsA)
	managerB := newTestWebRtcManager(t, ctx, signalPipeB, settingsB)
	signalPipeA.SetSignalReceiver(managerB)
	signalPipeB.SetSignalReceiver(managerA)

	peerIdA := NewId()
	peerIdB := NewId()
	streamId := NewId()
	passive, err := managerB.NewP2pConnPassive(
		ctx,
		NewTransferPath(peerIdB, peerIdA, streamId),
	)
	AssertEqual(t, err, nil)
	defer passive.Close()
	active, err := managerA.NewP2pConnActive(
		ctx,
		NewTransferPath(peerIdA, peerIdB, streamId),
	)
	AssertEqual(t, err, nil)
	defer active.Close()

	payload := []byte("mixed RFC 9653 compatibility")
	AssertEqual(t, active.SetWriteDeadline(time.Now().Add(5*time.Second)), nil)
	AssertEqual(t, passive.SetReadDeadline(time.Now().Add(5*time.Second)), nil)
	received := make(chan []byte, 1)
	go func() {
		b := make([]byte, len(payload))
		if _, readErr := io.ReadFull(passive, b); readErr == nil {
			received <- b
		}
	}()
	_, err = active.Write(payload)
	if err != nil {
		t.Fatalf(
			"zero-checksum mixed write failed: %v; active={%s} passive={%s}",
			err,
			testingWebRtcConnDiagnostics(active),
			testingWebRtcConnDiagnostics(passive),
		)
	}
	select {
	case b := <-received:
		AssertEqual(t, b, payload)
	case <-ctx.Done():
		t.Fatal("zero-checksum-enabled peer did not interoperate with a disabled peer")
	}

	metadata := func(conn WebRtcConn) *webrtc.SCTPTransportMetadata {
		for _, stat := range conn.(*peerConn).pc.GetStats() {
			if sctpStat, ok := stat.(webrtc.SCTPTransportStats); ok {
				return sctpStat.Metadata
			}
		}
		return nil
	}
	activeMetadata := metadata(active)
	passiveMetadata := metadata(passive)
	if activeMetadata == nil || passiveMetadata == nil {
		t.Fatal("missing SCTP negotiation metadata")
	}
	// Enabling the extension means "I can receive zero checksums." The mixed
	// peer therefore sends zero toward the enabled endpoint but continues to
	// receive normal CRC32c in the other direction. This directional
	// negotiation is the backwards-compatible behavior required by RFC 9653.
	AssertEqual(t, activeMetadata.ZeroChecksumReceivingEnabled, true)
	AssertEqual(t, activeMetadata.ZeroChecksumSendingEnabled, false)
	AssertEqual(t, passiveMetadata.ZeroChecksumReceivingEnabled, false)
	AssertEqual(t, passiveMetadata.ZeroChecksumSendingEnabled, true)
}

// CONNECT_WEBRTC_LATENCY_MEASURE=1 enables a manual warm-factory
// data-channel-ready comparison. It measures through the first successful
// detached write/read, so ICE, DTLS, SCTP, and DCEP are all complete.
func TestWebRtcSctpSnapReadyLatencyMeasurement(t *testing.T) {
	if os.Getenv("CONNECT_WEBRTC_LATENCY_MEASURE") == "" {
		t.Skip("set CONNECT_WEBRTC_LATENCY_MEASURE=1")
	}

	for _, enableSnap := range []bool{false, true} {
		func() {
			const pairCount = 25
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			settingsA := DefaultWebRtcSettings()
			settingsB := DefaultWebRtcSettings()
			settingsA.Log = NewNoopLogger()
			settingsB.Log = NewNoopLogger()
			settingsA.IceServerUrls = nil
			settingsB.IceServerUrls = nil
			settingsA.MaxPeerConnectionCount = 0
			settingsB.MaxPeerConnectionCount = 0
			settingsA.UseEgressOnlyIceInterfaces = true
			settingsB.UseEgressOnlyIceInterfaces = true
			if os.Getenv("CONNECT_WEBRTC_ORDERED") != "" {
				settingsA.DataChannelOrdered = true
				settingsB.DataChannelOrdered = true
			}
			settingsA.EnableSctpSnap = enableSnap
			settingsB.EnableSctpSnap = enableSnap

			signalPipeA := newSignalPipe(nil)
			signalPipeB := newSignalPipe(nil)
			managerA := newTestWebRtcManager(t, ctx, signalPipeA, settingsA)
			managerB := newTestWebRtcManager(t, ctx, signalPipeB, settingsB)
			signalPipeA.SetSignalReceiver(managerB)
			signalPipeB.SetSignalReceiver(managerA)

			peerIdA := NewId()
			peerIdB := NewId()
			latencies := make([]time.Duration, 0, pairCount)
			for range pairCount {
				streamId := NewId()
				passive, err := managerB.NewP2pConnPassive(
					ctx,
					NewTransferPath(peerIdB, peerIdA, streamId),
				)
				AssertEqual(t, err, nil)

				start := time.Now()
				active, err := managerA.NewP2pConnActive(
					ctx,
					NewTransferPath(peerIdA, peerIdB, streamId),
				)
				AssertEqual(t, err, nil)
				AssertEqual(t, active.SetWriteDeadline(time.Now().Add(5*time.Second)), nil)
				AssertEqual(t, passive.SetReadDeadline(time.Now().Add(5*time.Second)), nil)
				readDone := make(chan error, 1)
				go func() {
					var b [1]byte
					_, readErr := passive.Read(b[:])
					readDone <- readErr
				}()
				_, err = active.Write([]byte{1})
				AssertEqual(t, err, nil)
				AssertEqual(t, <-readDone, nil)
				latencies = append(latencies, time.Since(start))
				active.Close()
				passive.Close()
			}

			sort.Slice(latencies, func(i, j int) bool {
				return latencies[i] < latencies[j]
			})
			t.Logf(
				"SNAP=%t ready latency: median=%s p95=%s min=%s max=%s",
				enableSnap,
				latencies[len(latencies)/2],
				latencies[(len(latencies)*95-1)/100],
				latencies[0],
				latencies[len(latencies)-1],
			)
		}()
	}
}

func TestWebRtcSharedBudgetAdmissionIsExactAcrossManagers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const managerCount = 16
	const admittedCount = 2
	reservationSize := kib(128)
	budget := NewTransferMemoryBudget(admittedCount * reservationSize)

	managers := make([]*WebRtcManager, 0, managerCount)
	for range managerCount {
		settings := DefaultWebRtcSettings()
		settings.Log = NewNoopLogger()
		settings.IceServerUrls = nil
		settings.ReceiveBufferSize = reservationSize
		settings.MemoryBudget = budget
		settings.MaxPeerConnectionCount = 0
		managers = append(managers, newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings))
	}

	start := make(chan struct{})
	results := make(chan WebRtcConn, managerCount)
	var wg sync.WaitGroup
	for _, manager := range managers {
		wg.Add(1)
		go func(manager *WebRtcManager) {
			defer wg.Done()
			<-start
			conn, err := manager.NewP2pConnActive(
				ctx,
				NewTransferPath(NewId(), NewId(), NewId()),
			)
			if err == nil {
				results <- conn
				return
			}
			var admissionErr *peerConnectionAdmissionError
			if !errors.As(err, &admissionErr) {
				t.Errorf("unexpected setup error: %v", err)
			}
		}(manager)
	}
	close(start)
	wg.Wait()
	close(results)

	conns := make([]WebRtcConn, 0, admittedCount)
	for conn := range results {
		conns = append(conns, conn)
	}
	AssertEqual(t, len(conns), admittedCount)
	AssertEqual(t, budget.UsedByteCount(), admittedCount*reservationSize)

	notify := budget.CapacityNotify()
	conns[0].Close()
	select {
	case <-notify:
	case <-ctx.Done():
		t.Fatal("peer teardown did not wake shared-budget waiters")
	}
	for _, conn := range conns[1:] {
		conn.Close()
	}
	for budget.UsedByteCount() != 0 {
		select {
		case <-ctx.Done():
			t.Fatalf("peer reservations leaked: used=%d", budget.UsedByteCount())
		case <-time.After(10 * time.Millisecond):
		}
	}
	reserved, released := budget.Counts()
	AssertEqual(t, reserved, released)
	liveOwnerCount, retiringOwnerCount := budget.peerConnectionOwnerCounts()
	AssertEqual(t, liveOwnerCount, 0)
	AssertEqual(t, retiringOwnerCount, 0)
}

func TestWebRtcSharedBudgetPriorityReclaimsOwnerAcrossManagers(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const window = ByteCount(128 * 1024)
	budget := NewTransferMemoryBudget(window)
	newManager := func() *WebRtcManager {
		settings := DefaultWebRtcSettings()
		settings.Log = NewNoopLogger()
		settings.IceServerUrls = nil
		settings.ReceiveBufferSize = window
		settings.MemoryBudget = budget
		settings.MaxPeerConnectionCount = 0
		return newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)
	}
	firstManager := newManager()
	secondManager := newManager()

	firstPeerId := NewId()
	firstManager.PrioritizePeer(firstPeerId)
	first, err := firstManager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), firstPeerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	firstConn := first.(*peerConn)

	// The second manager has no local map entry for the only reservation.
	// Pool-wide ownership must retire it without raising the exact byte ceiling.
	secondPeerId := NewId()
	secondManager.PrioritizePeer(secondPeerId)
	select {
	case <-firstConn.ctx.Done():
	case <-ctx.Done():
		t.Fatal("selected peer could not reclaim a shared-budget owner in another manager")
	}
	for budget.UsedByteCount() != 0 {
		select {
		case <-ctx.Done():
			t.Fatalf("cross-manager victim retained %d bytes", budget.UsedByteCount())
		case <-time.After(time.Millisecond):
		}
	}

	second, err := secondManager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), secondPeerId, NewId()),
	)
	if err != nil {
		t.Fatalf("selected peer was not admitted after cross-manager teardown: %v", err)
	}
	defer second.Close()
	if got := budget.UsedByteCount(); got != window {
		t.Fatalf("replacement reservation = %d, want %d", got, window)
	}
	liveOwnerCount, retiringOwnerCount := budget.peerConnectionOwnerCounts()
	if liveOwnerCount != 1 || retiringOwnerCount != 0 {
		t.Fatalf(
			"shared owners live/retiring = %d/%d, want 1/0",
			liveOwnerCount,
			retiringOwnerCount,
		)
	}
}

func TestWebRtcSharedBudgetPendingRetirementPreventsCrossManagerDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const window = ByteCount(128 * 1024)
	budget := NewTransferMemoryBudget(2 * window)
	newManager := func() *WebRtcManager {
		settings := DefaultWebRtcSettings()
		settings.Log = NewNoopLogger()
		settings.IceServerUrls = nil
		settings.ReceiveBufferSize = window
		settings.MemoryBudget = budget
		settings.MaxPeerConnectionCount = 0
		return newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)
	}
	firstManager := newManager()
	secondManager := newManager()
	waitingManager := newManager()

	first, err := firstManager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := secondManager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	firstConn := first.(*peerConn)
	secondConn := second.(*peerConn)

	// Model the interval after a victim is claimed but before physical Pion
	// teardown releases its receive window. Another manager must wait for that
	// one release instead of canceling the remaining healthy owner.
	firstConn.admissionOwner.markRetiring()
	waitingManager.PrioritizePeer(NewId())
	select {
	case <-secondConn.ctx.Done():
		t.Fatal("a pending shared teardown caused a second healthy owner to be drained")
	default:
	}
	liveOwnerCount, retiringOwnerCount := budget.peerConnectionOwnerCounts()
	if liveOwnerCount != 1 || retiringOwnerCount != 1 {
		t.Fatalf(
			"shared owners live/retiring = %d/%d, want 1/1",
			liveOwnerCount,
			retiringOwnerCount,
		)
	}
}

func TestWebRtcPrioritizedNetworkPeerPreemptsWithoutRaisingAdmissionBounds(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.MaxPeerConnectionCount = 1
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	backgroundPeerId := NewId()
	backgroundConn, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), backgroundPeerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	background := backgroundConn.(*peerConn)
	if got := settings.MemoryBudget.UsedByteCount(); got != settings.ReceiveBufferSize {
		t.Fatalf("background reservation = %d, want %d", got, settings.ReceiveBufferSize)
	}

	priorityPeerId := NewId()
	manager.PrioritizePeer(priorityPeerId)
	select {
	case <-background.ctx.Done():
	case <-ctx.Done():
		t.Fatal("priority did not retire the bounded non-priority connection")
	}

	// The canceled connection releases asynchronously. While that happens,
	// another speculative waiter must not steal the selected peer's slot.
	if _, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), NewId(), NewId()),
	); err == nil {
		t.Fatal("non-priority admission stole a pending priority slot")
	}

	var priority *peerConn
	deadline := time.Now().Add(5 * time.Second)
	for priority == nil {
		var priorityConn WebRtcConn
		priorityConn, err = manager.NewP2pConnActive(
			ctx,
			NewTransferPath(NewId(), priorityPeerId, NewId()),
		)
		if err == nil {
			priority = priorityConn.(*peerConn)
			break
		}
		var admissionErr *peerConnectionAdmissionError
		if !errors.As(err, &admissionErr) {
			t.Fatalf("priority admission error = %v", err)
		}
		if deadline.Before(time.Now()) {
			t.Fatalf("priority never consumed released slot: %v", err)
		}
		time.Sleep(time.Millisecond)
	}
	defer priority.Close()

	manager.stateLock.Lock()
	priorityUntil := priority.priorityUntil
	liveCount := len(manager.peerConns)
	manager.stateLock.Unlock()
	if !time.Now().Before(priorityUntil) {
		t.Fatal("admitted selected peer did not retain bounded priority")
	}
	if liveCount != 1 {
		t.Fatalf("live peer connections = %d, want hard cap 1", liveCount)
	}
	if got := settings.MemoryBudget.UsedByteCount(); got != settings.ReceiveBufferSize {
		t.Fatalf("priority reservation = %d, want unchanged hard bound %d", got, settings.ReceiveBufferSize)
	}
}

// A trusted network peer admits against the dedicated network-peer window and
// budget, while a public peer keeps the small public window — the two pools are
// independent, so one never starves or is starved by the other (Fix 1).
func TestWebRtcNetworkPeerUsesDedicatedWindowAndBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(8 * settings.ReceiveBufferSize)
	settings.NetworkPeerReceiveBufferSize = mib(2)
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(2 * settings.NetworkPeerReceiveBufferSize)
	settings.MaxPeerConnectionCount = 0
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	// A public (non-prioritized) peer reserves the small window from the public
	// budget; the network-peer budget is untouched.
	publicConn, err := manager.NewP2pConnActive(ctx, NewTransferPath(NewId(), NewId(), NewId()))
	if err != nil {
		t.Fatal(err)
	}
	defer publicConn.Close()
	if got := settings.MemoryBudget.UsedByteCount(); got != settings.ReceiveBufferSize {
		t.Fatalf("public reservation = %d, want %d", got, settings.ReceiveBufferSize)
	}
	if got := settings.NetworkPeerMemoryBudget.UsedByteCount(); got != 0 {
		t.Fatalf("network-peer budget touched by a public peer: used=%d", got)
	}

	// A trusted (prioritized / ProvideMode_Network) peer reserves the large
	// window from the dedicated network-peer budget, leaving the public budget
	// unchanged.
	networkPeerId := NewId()
	manager.PrioritizePeer(networkPeerId)
	npConn, err := manager.NewP2pConnActive(ctx, NewTransferPath(NewId(), networkPeerId, NewId()))
	if err != nil {
		t.Fatal(err)
	}
	defer npConn.Close()
	if got := settings.NetworkPeerMemoryBudget.UsedByteCount(); got != settings.NetworkPeerReceiveBufferSize {
		t.Fatalf("network-peer reservation = %d, want %d", got, settings.NetworkPeerReceiveBufferSize)
	}
	if got := settings.MemoryBudget.UsedByteCount(); got != settings.ReceiveBufferSize {
		t.Fatalf("public budget changed by a network peer: used=%d, want %d", got, settings.ReceiveBufferSize)
	}
	if publicConn.(*peerConn).networkPeer {
		t.Fatal("public peer was built with the network-peer Pion API")
	}
	if !npConn.(*peerConn).networkPeer {
		t.Fatal("trusted peer was not built with the network-peer Pion API")
	}
}

func TestWebRtcNetworkPeerAdvertisesDedicatedReceiveWindow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	newSettings := func() *WebRtcSettings {
		settings := DefaultWebRtcSettings()
		settings.Log = NewNoopLogger()
		settings.IceServerUrls = nil
		// hermetic same-host connect: loopback-only ICE (see TestWebRtc)
		settings.UseLoopbackOnlyIceInterfaces = true
		settings.ReceiveBufferSize = kib(128)
		settings.MemoryBudget = NewTransferMemoryBudget(8 * settings.ReceiveBufferSize)
		settings.NetworkPeerReceiveBufferSize = mib(2)
		settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(2 * settings.NetworkPeerReceiveBufferSize)
		settings.MaxPeerConnectionCount = 0
		return settings
	}
	settingsA := newSettings()
	settingsB := newSettings()
	signalPipeA := newSignalPipe(nil)
	signalPipeB := newSignalPipe(nil)
	managerA := newTestWebRtcManager(t, ctx, signalPipeA, settingsA)
	managerB := newTestWebRtcManager(t, ctx, signalPipeB, settingsB)
	signalPipeA.SetSignalReceiver(managerB)
	signalPipeB.SetSignalReceiver(managerA)

	remoteWindow := func(conn *peerConn) uint32 {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			if sctp := conn.pc.SCTP(); sctp != nil {
				if rwnd := sctp.Stats().ReceiverWindow; rwnd != 0 {
					return rwnd
				}
			}
			if deadline.Before(time.Now()) {
				t.Fatal("SCTP association did not advertise a receive window")
			}
			time.Sleep(time.Millisecond)
		}
	}
	newPair := func(peerIdA Id, peerIdB Id) (*peerConn, *peerConn) {
		t.Helper()
		streamId := NewId()
		passiveValue, err := managerB.NewP2pConnPassive(
			ctx,
			NewTransferPath(peerIdB, peerIdA, streamId),
		)
		if err != nil {
			t.Fatal(err)
		}
		activeValue, err := managerA.NewP2pConnActive(
			ctx,
			NewTransferPath(peerIdA, peerIdB, streamId),
		)
		if err != nil {
			t.Fatal(err)
		}
		return activeValue.(*peerConn), passiveValue.(*peerConn)
	}
	assertAdvertisedWindow := func(label string, got uint32, want ByteCount) {
		t.Helper()
		// The data-channel OPEN control message may consume a few bytes before
		// the first stats sample. Require the configured window within one
		// transport message, which still cleanly distinguishes 128 KiB/2 MiB.
		wantWindow := uint32(want)
		tolerance := uint32(sendPackBatchMaxMessageByteCount)
		if wantWindow < got || got < wantWindow-tolerance {
			t.Fatalf("%s receive window = %d, want %d..%d", label, got, wantWindow-tolerance, wantWindow)
		}
	}

	networkPeerIdA := NewId()
	networkPeerIdB := NewId()
	managerA.PrioritizePeer(networkPeerIdB)
	managerB.PrioritizePeer(networkPeerIdA)
	networkA, networkB := newPair(networkPeerIdA, networkPeerIdB)
	defer networkA.Close()
	defer networkB.Close()
	assertAdvertisedWindow(
		"network peer remote",
		remoteWindow(networkA),
		settingsB.NetworkPeerReceiveBufferSize,
	)
	assertAdvertisedWindow(
		"network peer reverse",
		remoteWindow(networkB),
		settingsA.NetworkPeerReceiveBufferSize,
	)

	publicA, publicB := newPair(NewId(), NewId())
	defer publicA.Close()
	defer publicB.Close()
	assertAdvertisedWindow("public peer remote", remoteWindow(publicA), settingsB.ReceiveBufferSize)
	assertAdvertisedWindow("public peer reverse", remoteWindow(publicB), settingsA.ReceiveBufferSize)
}

func TestWebRtcNetworkPeerAdmissionWaitsOnDedicatedBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.NetworkPeerReceiveBufferSize = mib(2)
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(settings.NetworkPeerReceiveBufferSize)
	settings.MaxPeerConnectionCount = 0
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	firstPeerId := NewId()
	manager.PrioritizePeer(firstPeerId)
	first, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), firstPeerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}

	waitingPeerId := NewId()
	manager.PrioritizePeer(waitingPeerId)
	_, budgetNotify := manager.AdmissionNotify(waitingPeerId)
	if budgetNotify == nil {
		t.Fatal("network peer did not subscribe to its dedicated budget")
	}
	if _, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), waitingPeerId, NewId()),
	); err == nil {
		t.Fatal("network peer over-admitted its full dedicated budget")
	} else {
		var admissionErr *peerConnectionAdmissionError
		if !errors.As(err, &admissionErr) {
			t.Fatalf("full dedicated budget error = %v", err)
		}
	}

	// Releasing the network window must wake the exact budget channel captured
	// before the failed admission. Previously AdmissionNotify always returned
	// MemoryBudget, leaving this waiter asleep until its 30-second fallback.
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-budgetNotify:
	case <-ctx.Done():
		t.Fatal("dedicated network-peer budget release did not wake admission")
	}
}

func TestWebRtcAdmissionNotificationWakesOnlyCapacityFit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.MemoryBudget.Reserve(settings.ReceiveBufferSize)
	settings.MaxPeerConnectionCount = 0
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	firstWaiter := newTransferMemoryBudgetWaiter()
	secondWaiter := newTransferMemoryBudgetWaiter()
	defer firstWaiter.reset()
	defer secondWaiter.reset()
	_, firstNotify := manager.admissionNotify(NewId(), firstWaiter)
	_, secondNotify := manager.admissionNotify(NewId(), secondWaiter)

	settings.MemoryBudget.Release(settings.ReceiveBufferSize)
	wokenCount := 0
	select {
	case <-firstNotify:
		wokenCount += 1
	default:
	}
	select {
	case <-secondNotify:
		wokenCount += 1
	default:
	}
	AssertEqual(t, wokenCount, 1)
	AssertEqual(t, settings.MemoryBudget.capacityWaiterCount.Load(), int64(1))
}

func TestWebRtcAdmissionNotificationUsesDedicatedThreshold(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(mib(8))
	settings.NetworkPeerReceiveBufferSize = mib(2)
	settings.NetworkPeerMemoryBudget =
		NewTransferMemoryBudget(settings.NetworkPeerReceiveBufferSize)
	settings.MaxPeerConnectionCount = 0
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	peerId := NewId()
	manager.PrioritizePeer(peerId)
	waiter := newTransferMemoryBudgetWaiter()
	defer waiter.reset()
	_, notify := manager.admissionNotify(peerId, waiter)
	if notify == nil {
		t.Fatal("network-peer admission did not receive a budget notification")
	}
	if waiter.budget != settings.NetworkPeerMemoryBudget {
		t.Fatal("network-peer admission subscribed to the public budget")
	}
	AssertEqual(t, waiter.requiredByteCount, settings.NetworkPeerReceiveBufferSize)
}

func TestWebRtcNewestNetworkPeerReclaimsLeaseProtectedDedicatedBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.NetworkPeerReceiveBufferSize = kib(512)
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(
		2 * settings.NetworkPeerReceiveBufferSize,
	)
	settings.MaxPeerConnectionCount = 0
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	oldestPeerId := NewId()
	manager.PrioritizePeer(oldestPeerId)
	oldestValue, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), oldestPeerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	oldest := oldestValue.(*peerConn)

	recentPeerId := NewId()
	manager.PrioritizePeer(recentPeerId)
	recentValue, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), recentPeerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	recent := recentValue.(*peerConn)
	defer recent.Close()

	manager.stateLock.Lock()
	manager.networkPeers[oldestPeerId] = time.Now().Add(-time.Minute)
	manager.networkPeers[recentPeerId] = time.Now()
	manager.prioritizedPeers[oldestPeerId] = time.Now().Add(time.Minute)
	manager.prioritizedPeers[recentPeerId] = time.Now().Add(time.Minute)
	manager.stateLock.Unlock()

	newestPeerId := NewId()
	manager.PrioritizePeer(newestPeerId)
	select {
	case <-oldest.ctx.Done():
	case <-ctx.Done():
		t.Fatal("new authenticated Network peer did not reclaim the stale dedicated slot")
	}
	select {
	case <-recent.ctx.Done():
		t.Fatal("Network LRU reclaimed the recently observed association")
	default:
	}

	var newest *peerConn
	for newest == nil {
		var newestValue WebRtcConn
		newestValue, err = manager.NewP2pConnActive(
			ctx,
			NewTransferPath(NewId(), newestPeerId, NewId()),
		)
		if err == nil {
			newest = newestValue.(*peerConn)
			break
		}
		var admissionErr *peerConnectionAdmissionError
		if !errors.As(err, &admissionErr) {
			t.Fatalf("newest Network admission error = %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("newest Network peer never consumed the reclaimed slot")
		case <-time.After(time.Millisecond):
		}
	}
	defer newest.Close()

	if got := settings.NetworkPeerMemoryBudget.UsedByteCount(); got !=
		2*settings.NetworkPeerReceiveBufferSize {
		t.Fatalf(
			"dedicated reservation = %d, want hard bound %d",
			got,
			2*settings.NetworkPeerReceiveBufferSize,
		)
	}
}

func TestWebRtcNewestNetworkStreamReclaimsOldestSamePeerAssociation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.NetworkPeerReceiveBufferSize = kib(512)
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(
		2 * settings.NetworkPeerReceiveBufferSize,
	)
	settings.MaxPeerConnectionCount = 0
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	peerId := NewId()
	manager.PrioritizePeer(peerId)
	firstValue, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), peerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	first := firstValue.(*peerConn)
	secondValue, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), peerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	second := secondValue.(*peerConn)
	defer second.Close()

	thirdPath := NewTransferPath(NewId(), peerId, NewId())
	_, err = manager.NewP2pConnActive(ctx, thirdPath)
	var admissionErr *peerConnectionAdmissionError
	if !errors.As(err, &admissionErr) {
		t.Fatalf("third stream admission = %v, want bounded refusal", err)
	}
	select {
	case <-first.ctx.Done():
	case <-ctx.Done():
		t.Fatal("third stream did not retire the oldest same-peer association")
	}
	select {
	case <-second.ctx.Done():
		t.Fatal("third stream retired the newer same-peer association")
	default:
	}

	var third *peerConn
	for third == nil {
		var thirdValue WebRtcConn
		thirdValue, err = manager.NewP2pConnActive(ctx, thirdPath)
		if err == nil {
			third = thirdValue.(*peerConn)
			break
		}
		if !errors.As(err, &admissionErr) {
			t.Fatalf("third stream retry error = %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatal("third stream never consumed the reclaimed slot")
		case <-time.After(time.Millisecond):
		}
	}
	defer third.Close()
	if got := settings.NetworkPeerMemoryBudget.UsedByteCount(); got !=
		2*settings.NetworkPeerReceiveBufferSize {
		t.Fatalf(
			"same-peer reservation = %d, want hard bound %d",
			got,
			2*settings.NetworkPeerReceiveBufferSize,
		)
	}
}

func TestWebRtcDedicatedBudgetReclamationDoesNotEvictPublicAssociation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.NetworkPeerReceiveBufferSize = kib(512)
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(
		settings.NetworkPeerReceiveBufferSize,
	)
	settings.MaxPeerConnectionCount = 0
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	publicValue, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	public := publicValue.(*peerConn)
	defer public.Close()

	oldNetworkPeerId := NewId()
	manager.PrioritizePeer(oldNetworkPeerId)
	oldNetworkValue, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), oldNetworkPeerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	oldNetwork := oldNetworkValue.(*peerConn)

	// Only the dedicated pool is full. Reclaiming the older public
	// association would not release a single byte from that pool and would
	// unnecessarily disrupt unrelated traffic.
	newNetworkPeerId := NewId()
	manager.PrioritizePeer(newNetworkPeerId)
	select {
	case <-oldNetwork.ctx.Done():
	case <-ctx.Done():
		t.Fatal("full dedicated budget did not reclaim its dedicated association")
	}
	select {
	case <-public.ctx.Done():
		t.Fatal("dedicated budget reclamation evicted an unrelated public association")
	default:
	}
	if got := settings.MemoryBudget.UsedByteCount(); got != settings.ReceiveBufferSize {
		t.Fatalf("public reservation = %d, want %d", got, settings.ReceiveBufferSize)
	}
}

func TestWebRtcSharedAdmissionBudgetReclaimsPublicAssociationForNetworkPeer(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const window = ByteCount(128 * 1024)
	sharedBudget := NewTransferMemoryBudget(window)
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = window
	settings.MemoryBudget = sharedBudget
	settings.NetworkPeerReceiveBufferSize = window
	settings.NetworkPeerMemoryBudget = sharedBudget
	settings.MaxPeerConnectionCount = 0
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	publicValue, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	public := publicValue.(*peerConn)
	defer public.Close()

	// Selected SDK window clients deliberately share one hard budget between
	// the public fallback and Network views. Labels differ, but canceling this
	// public association really does release the bytes the selected peer
	// needs, so reclamation must follow budget identity rather than the label.
	networkPeerId := NewId()
	manager.PrioritizePeer(networkPeerId)
	select {
	case <-public.ctx.Done():
	case <-ctx.Done():
		t.Fatal("shared budget did not reclaim its public owner for the selected peer")
	}
	deadline := time.Now().Add(5 * time.Second)
	for sharedBudget.UsedByteCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("shared reservation did not release: %d", sharedBudget.UsedByteCount())
		}
		time.Sleep(time.Millisecond)
	}
	network, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), networkPeerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer network.Close()
	if !network.(*peerConn).networkPeer {
		t.Fatal("selected replacement did not use Network admission")
	}
	if got := sharedBudget.UsedByteCount(); got != window {
		t.Fatalf("shared reservation = %d, want %d", got, window)
	}
}

func TestWebRtcDedicatedAssociationRemainsReclaimableAfterTrustRecordEviction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.NetworkPeerReceiveBufferSize = kib(512)
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(
		settings.NetworkPeerReceiveBufferSize,
	)
	settings.MaxPeerConnectionCount = 0
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	oldPeerId := NewId()
	manager.PrioritizePeer(oldPeerId)
	oldValue, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), oldPeerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	old := oldValue.(*peerConn)

	// The remembered trust map is intentionally hard bounded. Model enough
	// later identities to evict this peer while its already-admitted
	// association is still live and lease protected.
	manager.stateLock.Lock()
	delete(manager.networkPeers, oldPeerId)
	for range maxRememberedNetworkPeerCount {
		manager.networkPeers[NewId()] = time.Now()
	}
	manager.prioritizedPeers[oldPeerId] = time.Now().Add(time.Minute)
	manager.stateLock.Unlock()

	newPeerId := NewId()
	manager.PrioritizePeer(newPeerId)
	select {
	case <-old.ctx.Done():
	case <-ctx.Done():
		t.Fatal("bounded trust-record eviction made a live dedicated association unreclaimable")
	}
}

func TestWebRtcPendingDedicatedPeerDoesNotBlockIndependentPublicAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.NetworkPeerReceiveBufferSize = kib(512)
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(
		settings.NetworkPeerReceiveBufferSize,
	)
	settings.MaxPeerConnectionCount = 2
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	// Model a shared dedicated budget whose only slot belongs to another
	// manager. This manager cannot reclaim it, so its selected peer remains
	// pending until the shared owner releases the reservation.
	if !settings.NetworkPeerMemoryBudget.TryReserve(
		settings.NetworkPeerReceiveBufferSize,
	) {
		t.Fatal("could not reserve the synthetic shared dedicated slot")
	}
	defer settings.NetworkPeerMemoryBudget.Release(
		settings.NetworkPeerReceiveBufferSize,
	)
	networkPeerId := NewId()
	manager.PrioritizePeer(networkPeerId)

	publicValue, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	if err != nil {
		t.Fatalf("independent public admission was blocked by dedicated waiter: %v", err)
	}
	defer publicValue.Close()
	if got := settings.MemoryBudget.UsedByteCount(); got != settings.ReceiveBufferSize {
		t.Fatalf("public reservation = %d, want %d", got, settings.ReceiveBufferSize)
	}
	manager.stateLock.Lock()
	_, pending := manager.pendingPrioritizedPeerSlot[networkPeerId]
	manager.stateLock.Unlock()
	if !pending {
		t.Fatal("independent public admission consumed the selected peer's pending state")
	}
}

func TestWebRtcPendingNetworkPeerReservesOnlyNeededSamePoolCapacity(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	firstOrdinaryPeerId := NewId()
	secondOrdinaryPeerId := NewId()
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.NetworkPeerReceiveBufferSize = kib(128)
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(
		2 * settings.NetworkPeerReceiveBufferSize,
	)
	settings.MaxPeerConnectionCount = 2
	settings.InitialNetworkPeerIds = []Id{
		firstOrdinaryPeerId,
		secondOrdinaryPeerId,
	}
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	pendingPeerId := NewId()
	manager.PrioritizePeer(pendingPeerId)

	// One of two available slots can still carry an ordinary authenticated
	// peer while the other is retained for the selected peer. The former
	// boolean gate froze the entire dedicated pool for the 30-second priority
	// lease after any signal that did not immediately produce a setup.
	first, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), firstOrdinaryPeerId, NewId()),
	)
	if err != nil {
		t.Fatalf("surplus same-pool capacity was blocked: %v", err)
	}
	defer first.Close()

	second, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), secondOrdinaryPeerId, NewId()),
	)
	if err == nil {
		second.Close()
		t.Fatal("ordinary admission consumed the slot reserved for the selected peer")
	}
	var admissionErr *peerConnectionAdmissionError
	if !errors.As(err, &admissionErr) {
		t.Fatalf("reserved-slot refusal = %T %v", err, err)
	}
	manager.stateLock.Lock()
	_, pending := manager.pendingPrioritizedPeerSlot[pendingPeerId]
	manager.stateLock.Unlock()
	if !pending {
		t.Fatal("surplus admission removed the selected peer's reservation")
	}
}

func TestWebRtcPendingNetworkPeerReservesCapacityInSharedPublicBudget(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const window = ByteCount(128 * 1024)
	sharedBudget := NewTransferMemoryBudget(2 * window)
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = window
	settings.MemoryBudget = sharedBudget
	settings.NetworkPeerReceiveBufferSize = window
	settings.NetworkPeerMemoryBudget = sharedBudget
	settings.MaxPeerConnectionCount = 3
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	pendingPeerId := NewId()
	manager.PrioritizePeer(pendingPeerId)
	first, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	if err != nil {
		t.Fatalf("surplus shared capacity was blocked: %v", err)
	}
	defer first.Close()

	second, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	if err == nil {
		second.Close()
		t.Fatal("public admission consumed shared capacity reserved for selected Network peer")
	}
	var admissionErr *peerConnectionAdmissionError
	if !errors.As(err, &admissionErr) {
		t.Fatalf("shared-budget refusal = %T %v", err, err)
	}
	if got := sharedBudget.UsedByteCount(); got != window {
		t.Fatalf("shared budget use = %d, want %d", got, window)
	}
}

func TestWebRtcReleasedCanceledAssociationDoesNotConsumePriorityReservation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const window = ByteCount(128 * 1024)
	sharedBudget := NewTransferMemoryBudget(window)
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = window
	settings.MemoryBudget = sharedBudget
	settings.NetworkPeerReceiveBufferSize = window
	settings.NetworkPeerMemoryBudget = sharedBudget
	settings.MaxPeerConnectionCount = 4
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	pendingPeerId := NewId()
	manager.PrioritizePeer(pendingPeerId)

	// Model the teardown handoff exactly: the association has been canceled and
	// its bytes released (which wakes admission waiters), but its map entry has
	// not yet been removed under the manager lock. That entry no longer owns
	// budget capacity and must not satisfy the selected peer's reservation.
	connCtx, connCancel := context.WithCancel(ctx)
	connCancel()
	key := peerConnKey{
		PeerId:   pendingPeerId,
		StreamId: NewId(),
	}
	manager.stateLock.Lock()
	manager.peerConns[key] = &peerConn{
		ctx:             connCtx,
		cancel:          connCancel,
		key:             key,
		networkPeer:     true,
		admissionBudget: sharedBudget,
	}
	blocked := manager.pendingPriorityBlocksAdmissionLocked(false, true)
	delete(manager.peerConns, key)
	manager.stateLock.Unlock()

	if !blocked {
		t.Fatal("ordinary admission stole bytes released for the pending selected peer")
	}
}

func TestWebRtcPendingPriorityBudgetAccountingDoesNotOverflow(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	firstPendingPeerId := NewId()
	secondPendingPeerId := NewId()
	const networkWindow = ByteCount(1 << 62)
	sharedBudget := NewTransferMemoryBudget(ByteCount(1<<63 - 1))
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = 1
	settings.MemoryBudget = sharedBudget
	settings.NetworkPeerReceiveBufferSize = networkWindow
	settings.NetworkPeerMemoryBudget = sharedBudget
	settings.MaxPeerConnectionCount = 0
	settings.InitialNetworkPeerIds = []Id{
		firstPendingPeerId,
		secondPendingPeerId,
	}
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	manager.stateLock.Lock()
	until := time.Now().Add(time.Minute)
	manager.pendingPrioritizedPeerSlot[firstPendingPeerId] = until
	manager.pendingPrioritizedPeerSlot[secondPendingPeerId] = until
	blocked := manager.pendingPriorityBlocksAdmissionLocked(false, true)
	manager.stateLock.Unlock()

	if !blocked {
		t.Fatal("overflowing pending-window sum bypassed the shared budget ceiling")
	}
}

func TestWebRtcFailedPriorityStreamRetainsReleasedBudgetReservation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const window = ByteCount(128 * 1024)
	ordinaryPeerId := NewId()
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = window
	settings.MemoryBudget = NewTransferMemoryBudget(window)
	settings.NetworkPeerReceiveBufferSize = window
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(window)
	settings.MaxPeerConnectionCount = 2
	settings.InitialNetworkPeerIds = []Id{ordinaryPeerId}
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	selectedPeerId := NewId()
	manager.PrioritizePeer(selectedPeerId)
	first, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), selectedPeerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	firstConn := first.(*peerConn)
	defer first.Close()

	// A refresh sees the first selected-peer stream and clears the original
	// pending flag. Its second stream still needs a reservation from the same
	// one-window pool, so the failed priority attempt must recreate that flag
	// before canceling the old owner.
	manager.PrioritizePeer(selectedPeerId)
	secondPath := NewTransferPath(NewId(), selectedPeerId, NewId())
	if second, secondErr := manager.NewP2pConnActive(ctx, secondPath); secondErr == nil {
		second.Close()
		t.Fatal("second selected stream overdrew the one-window budget")
	} else {
		var admissionErr *peerConnectionAdmissionError
		if !errors.As(secondErr, &admissionErr) {
			t.Fatalf("second selected stream error = %T %v", secondErr, secondErr)
		}
	}
	select {
	case <-firstConn.ctx.Done():
	case <-ctx.Done():
		t.Fatal("failed priority stream did not reclaim its existing budget owner")
	}
	deadline := time.Now().Add(5 * time.Second)
	for settings.NetworkPeerMemoryBudget.UsedByteCount() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("reclaimed selected reservation did not release")
		}
		time.Sleep(time.Millisecond)
	}

	if ordinary, ordinaryErr := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), ordinaryPeerId, NewId()),
	); ordinaryErr == nil {
		ordinary.Close()
		t.Fatal("ordinary waiter stole the released selected-stream reservation")
	}

	second, err := manager.NewP2pConnActive(ctx, secondPath)
	if err != nil {
		t.Fatalf("selected stream could not consume its retained reservation: %v", err)
	}
	defer second.Close()
	manager.stateLock.Lock()
	_, pending := manager.pendingPrioritizedPeerSlot[selectedPeerId]
	manager.stateLock.Unlock()
	if pending {
		t.Fatal("successful selected stream left its reservation pending")
	}
}

func TestWebRtcPriorityRefreshPreservesAnotherStreamReservation(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const window = ByteCount(128 * 1024)
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = window
	settings.MemoryBudget = NewTransferMemoryBudget(window)
	settings.NetworkPeerReceiveBufferSize = window
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(2 * window)
	settings.MaxPeerConnectionCount = 2
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	peerId := NewId()
	manager.PrioritizePeer(peerId)
	first, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), peerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	// Model the pending state installed when a second stream loses an
	// admission race. Provider traffic refreshes peer priority every five
	// seconds; seeing the first live stream must not erase the second stream's
	// reservation and return it to a wake-all lottery.
	manager.stateLock.Lock()
	pendingUntil := time.Now().Add(peerConnectionPriorityTimeout / 2)
	manager.pendingPrioritizedPeerSlot[peerId] = pendingUntil
	manager.stateLock.Unlock()
	manager.PrioritizePeer(peerId)

	manager.stateLock.Lock()
	gotUntil, pending := manager.pendingPrioritizedPeerSlot[peerId]
	manager.stateLock.Unlock()
	if !pending {
		t.Fatal("priority refresh cleared another stream's pending reservation")
	}
	if !gotUntil.Equal(pendingUntil) {
		t.Fatalf(
			"priority refresh extended stale stream reservation: got=%s want=%s",
			gotUntil,
			pendingUntil,
		)
	}
}

func TestWebRtcPendingPriorityAdmissionReportsLeaseRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const window = ByteCount(128 * 1024)
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = window
	settings.MemoryBudget = NewTransferMemoryBudget(window)
	settings.NetworkPeerReceiveBufferSize = 0
	settings.NetworkPeerMemoryBudget = nil
	settings.MaxPeerConnectionCount = 1
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	pendingPeerId := NewId()
	manager.PrioritizePeer(pendingPeerId)
	const lease = 80 * time.Millisecond
	manager.stateLock.Lock()
	manager.pendingPrioritizedPeerSlot[pendingPeerId] = time.Now().Add(lease)
	manager.stateLock.Unlock()

	path := NewTransferPath(NewId(), NewId(), NewId())
	_, err := manager.NewP2pConnActive(ctx, path)
	var admissionErr *peerConnectionAdmissionError
	if !errors.As(err, &admissionErr) {
		t.Fatalf("pending admission = %T %v", err, err)
	}
	if admissionErr.retryAfter <= 0 || lease < admissionErr.retryAfter {
		t.Fatalf(
			"pending admission retry = %s, want within (0,%s]",
			admissionErr.retryAfter,
			lease,
		)
	}

	time.Sleep(lease + 20*time.Millisecond)
	conn, err := manager.NewP2pConnActive(ctx, path)
	if err != nil {
		t.Fatalf("expired priority lease still blocked admission: %v", err)
	}
	defer conn.Close()
}

func TestP2pAdmissionRetryUsesEarlierPriorityLease(t *testing.T) {
	const configured = 30 * time.Second
	const lease = 75 * time.Millisecond
	got := p2pAdmissionRetryTimeout(
		configured,
		&peerConnectionAdmissionError{
			message:    "priority lease",
			retryAfter: lease,
		},
	)
	if got != lease {
		t.Fatalf("admission retry = %s, want priority lease %s", got, lease)
	}
}

func TestP2pAdmissionWaitChannelsAreReasonLocal(t *testing.T) {
	countNotify := make(chan struct{})
	budgetNotify := make(chan struct{})
	stateNotify := make(chan struct{})

	countWait, budgetWait, stateWait := p2pAdmissionWaitChannels(
		&peerConnectionAdmissionError{reason: peerConnectionAdmissionBudget},
		countNotify,
		budgetNotify,
		stateNotify,
	)
	if countWait != nil || budgetWait != budgetNotify || stateWait != stateNotify {
		t.Fatal("budget refusal did not isolate budget/state wakeups")
	}

	countWait, budgetWait, stateWait = p2pAdmissionWaitChannels(
		&peerConnectionAdmissionError{reason: peerConnectionAdmissionCount},
		countNotify,
		budgetNotify,
		stateNotify,
	)
	if countWait != countNotify || budgetWait != nil || stateWait != stateNotify {
		t.Fatal("count refusal did not isolate count/state wakeups")
	}

	countWait, budgetWait, stateWait = p2pAdmissionWaitChannels(
		&peerConnectionAdmissionError{reason: peerConnectionAdmissionPriority},
		countNotify,
		budgetNotify,
		stateNotify,
	)
	if countWait != nil || budgetWait != nil || stateWait != stateNotify {
		t.Fatal("priority refusal did not isolate state wakeups")
	}
}

func TestWebRtcCountAdmissionReleaseWakesOneWaiter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.MaxPeerConnectionCount = 8
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	firstWaiter := newTransferMemoryBudgetWaiter()
	secondWaiter := newTransferMemoryBudgetWaiter()
	defer firstWaiter.reset()
	defer secondWaiter.reset()
	firstNotify, _ := manager.admissionNotify(NewId(), firstWaiter)
	secondNotify, _ := manager.admissionNotify(NewId(), secondWaiter)
	if firstNotify != secondNotify {
		t.Fatal("count admission waiters do not share the bounded token channel")
	}

	manager.notifyCountCapacity()
	wokenCount := 0
	select {
	case <-firstNotify:
		wokenCount += 1
	default:
	}
	select {
	case <-secondNotify:
		wokenCount += 1
	default:
	}
	if wokenCount != 1 {
		t.Fatalf("one count release woke %d waiters", wokenCount)
	}
}

func TestWebRtcInternalAdmissionIgnoresUnrelatedBroadcast(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.MaxPeerConnectionCount = 8
	settings.MemoryBudget = NewTransferMemoryBudget(1)
	settings.MemoryBudget.Reserve(1)
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	waiter := newTransferMemoryBudgetWaiter()
	defer waiter.reset()
	countNotify, budgetNotify := manager.admissionNotify(NewId(), waiter)
	manager.capacityMonitor.NotifyAll()

	select {
	case <-countNotify:
		t.Fatal("compatibility broadcast woke an internal count waiter")
	default:
	}
	select {
	case <-budgetNotify:
		t.Fatal("compatibility broadcast woke an internal budget waiter")
	default:
	}
}

func TestWebRtcAdmissionClassificationChangeHasDedicatedWake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	stateNotify := manager.admissionStateMonitor.NotifyChannel()
	manager.PrioritizePeer(NewId())
	select {
	case <-stateNotify:
	case <-time.After(time.Second):
		t.Fatal("network-peer classification change did not wake admission")
	}
}

func TestP2pAdmissionLogKeyIgnoresChangingCapacityDiagnostics(t *testing.T) {
	first := &peerConnectionAdmissionError{
		message: "budget exhausted used=1048576 live=9 replacing=true",
		reason:  peerConnectionAdmissionBudget,
	}
	second := &peerConnectionAdmissionError{
		message: "budget exhausted used=917504 live=10 replacing=false",
		reason:  peerConnectionAdmissionBudget,
	}
	if first.Error() == second.Error() {
		t.Fatal("test diagnostics do not model changing capacity samples")
	}
	if firstKey, secondKey := p2pSetupErrorKey(first), p2pSetupErrorKey(second); firstKey != secondKey {
		t.Fatalf("same admission streak keys changed: %q != %q", firstKey, secondKey)
	}
}

func TestWebRtcAdmissionRefusalTelemetryIsSparseAndReasonLocal(t *testing.T) {
	manager := &WebRtcManager{}
	budgetEmits := 0
	for i := 0; i < 16; i++ {
		if _, emit := manager.observeAdmissionRefusal(peerConnectionAdmissionBudget); emit {
			budgetEmits += 1
		}
	}
	if budgetEmits != 5 {
		t.Fatalf("budget summary count = %d, want 5 through refusal 16", budgetEmits)
	}

	countEmits := 0
	for i := 0; i < 8; i++ {
		if _, emit := manager.observeAdmissionRefusal(peerConnectionAdmissionCount); emit {
			countEmits += 1
		}
	}
	if countEmits != 4 {
		t.Fatalf("count summary count = %d, want 4 through refusal 8", countEmits)
	}
	if count := manager.admissionBudgetRefusalCount.Load(); count != 16 {
		t.Fatalf("budget refusal count = %d, want 16", count)
	}
	if count := manager.admissionCountRefusalCount.Load(); count != 8 {
		t.Fatalf("count refusal count = %d, want 8", count)
	}
}

func TestWebRtcAdmissionRefusalTelemetryIsSparseUnderConcurrency(t *testing.T) {
	manager := &WebRtcManager{}
	const refusalCount = 128
	var emits atomic.Int64
	var wait sync.WaitGroup
	wait.Add(refusalCount)
	for i := 0; i < refusalCount; i++ {
		go func() {
			defer wait.Done()
			if _, emit := manager.observeAdmissionRefusal(peerConnectionAdmissionBudget); emit {
				emits.Add(1)
			}
		}()
	}
	wait.Wait()

	if count := manager.admissionBudgetRefusalCount.Load(); count != refusalCount {
		t.Fatalf("budget refusal count = %d, want %d", count, refusalCount)
	}
	if count := emits.Load(); count != 8 {
		t.Fatalf("summary count = %d, want 8 through refusal 128", count)
	}
}

func TestP2pSetupFailureStreakEndsOnlyAtReadyBoundary(t *testing.T) {
	failure := &peerConnectionAdmissionError{
		message: "budget exhausted",
		reason:  peerConnectionAdmissionBudget,
	}
	var streak p2pSetupFailureStreak
	if !streak.Observe(failure) {
		t.Fatal("first setup failure was suppressed")
	}
	// A PeerConnection allocation is deliberately not a state transition on
	// this object. If its ready-header exchange later times out, the same
	// failure remains one streak and must stay suppressed.
	if streak.Observe(failure) {
		t.Fatal("unchanged failure logged again before a ready-header recovery")
	}
	if !streak.Recover() {
		t.Fatal("ready boundary did not end the failure streak")
	}
	if !streak.Observe(failure) {
		t.Fatal("new post-recovery failure streak was suppressed")
	}
}

func TestWebRtcLiveNetworkAssociationPreservesTrustAfterRecordEviction(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.NetworkPeerReceiveBufferSize = kib(128)
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(
		2 * settings.NetworkPeerReceiveBufferSize,
	)
	settings.MaxPeerConnectionCount = 2
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	peerId := NewId()
	manager.PrioritizePeer(peerId)
	first, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), peerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if !first.(*peerConn).networkPeer {
		t.Fatal("initial association did not use dedicated admission")
	}

	// Model the unavoidable overflow case where every bounded identity record
	// has a live association. The immutable class of an authenticated live
	// association must still prevent an adjacent stream from silently
	// downgrading to the public window and budget.
	manager.stateLock.Lock()
	delete(manager.networkPeers, peerId)
	delete(manager.prioritizedPeers, peerId)
	manager.stateLock.Unlock()

	second, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), peerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	if !second.(*peerConn).networkPeer {
		t.Fatal("live authenticated association did not preserve dedicated admission")
	}
	if got := settings.NetworkPeerMemoryBudget.UsedByteCount(); got !=
		2*settings.NetworkPeerReceiveBufferSize {
		t.Fatalf(
			"dedicated reservations = %d, want %d",
			got,
			2*settings.NetworkPeerReceiveBufferSize,
		)
	}
	if got := settings.MemoryBudget.UsedByteCount(); got != 0 {
		t.Fatalf("record eviction touched public budget: %d", got)
	}
}

func TestWebRtcNetworkIdentityChurnEvictsInactiveBeforeLiveRecord(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.NetworkPeerReceiveBufferSize = kib(128)
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(
		2 * settings.NetworkPeerReceiveBufferSize,
	)
	settings.MaxPeerConnectionCount = 0
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	livePeerId := NewId()
	manager.PrioritizePeer(livePeerId)
	live, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), livePeerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer live.Close()

	for range maxRememberedNetworkPeerCount + 1 {
		manager.PrioritizePeer(NewId())
	}
	manager.stateLock.Lock()
	_, remembered := manager.networkPeers[livePeerId]
	rememberedCount := len(manager.networkPeers)
	manager.stateLock.Unlock()
	if !remembered {
		t.Fatal("bounded identity churn evicted a live record before inactive records")
	}
	if rememberedCount != maxRememberedNetworkPeerCount {
		t.Fatalf(
			"remembered identity count = %d, want %d",
			rememberedCount,
			maxRememberedNetworkPeerCount,
		)
	}
}

func TestWebRtcNetworkPromotionWakesPublicAdmissionSubscription(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.NetworkPeerReceiveBufferSize = mib(2)
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(
		settings.NetworkPeerReceiveBufferSize,
	)
	settings.MaxPeerConnectionCount = 0
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	peerId := NewId()
	countNotify, stalePublicBudgetNotify := manager.AdmissionNotify(peerId)
	if countNotify == nil || stalePublicBudgetNotify == nil {
		t.Fatal("public admission did not expose both notification sources")
	}
	if stalePublicBudgetNotify != settings.MemoryBudget.CapacityNotify() {
		t.Fatal("untrusted peer did not initially subscribe to the public budget")
	}

	// This models the ordering in P2pTransport.run: notification capture can
	// precede the authenticated ProvideMode_Network signal. Promotion must
	// wake the manager channel because the already-captured public budget will
	// never report capacity changes in the peer's real dedicated pool.
	manager.PrioritizePeer(peerId)
	select {
	case <-countNotify:
	case <-ctx.Done():
		t.Fatal("Network promotion left the public admission subscription asleep")
	}
	select {
	case <-stalePublicBudgetNotify:
		t.Fatal("Network promotion spuriously changed public-budget capacity")
	default:
	}

	_, dedicatedBudgetNotify := manager.AdmissionNotify(peerId)
	if dedicatedBudgetNotify == nil {
		t.Fatal("promoted peer did not expose a dedicated-budget notification")
	}
	if dedicatedBudgetNotify != settings.NetworkPeerMemoryBudget.CapacityNotify() {
		t.Fatal("promoted peer did not re-arm against its dedicated budget")
	}
}

func TestWebRtcNetworkPeerIdentitySurvivesPriorityExpiry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.NetworkPeerReceiveBufferSize = mib(2)
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(settings.NetworkPeerReceiveBufferSize)
	settings.MaxPeerConnectionCount = 0
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	peerId := NewId()
	manager.PrioritizePeer(peerId)
	manager.stateLock.Lock()
	manager.prioritizedPeers[peerId] = time.Now().Add(-time.Second)
	manager.pendingPrioritizedPeerSlot[peerId] = time.Now().Add(-time.Second)
	manager.stateLock.Unlock()

	// Simulate an idle/network-change rebuild after the short admission lease
	// expires. Trust is bounded manager state, not a capacity reservation, and
	// must continue selecting the symmetric large receive window.
	conn, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), peerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if !conn.(*peerConn).networkPeer {
		t.Fatal("expired priority silently reverted a trusted peer to the public window")
	}
	if got := settings.NetworkPeerMemoryBudget.UsedByteCount(); got != settings.NetworkPeerReceiveBufferSize {
		t.Fatalf("network reservation = %d, want %d", got, settings.NetworkPeerReceiveBufferSize)
	}
	if got := settings.MemoryBudget.UsedByteCount(); got != 0 {
		t.Fatalf("idle rebuild used public budget: %d", got)
	}
}

func TestWebRtcInitialNetworkPeerUsesReservedAdmissionBeforeAnySignal(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	peerId := NewId()
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.NetworkPeerReceiveBufferSize = kib(512)
	settings.NetworkPeerMemoryBudget =
		NewTransferMemoryBudget(2 * settings.NetworkPeerReceiveBufferSize)
	settings.InitialNetworkPeerIds = []Id{peerId}
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	conn, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), peerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if !conn.(*peerConn).networkPeer {
		t.Fatal("explicit destination opened its first association as an untrusted public peer")
	}
	if got := settings.NetworkPeerMemoryBudget.UsedByteCount(); got != settings.NetworkPeerReceiveBufferSize {
		t.Fatalf("network reservation = %d, want %d", got, settings.NetworkPeerReceiveBufferSize)
	}
	if got := settings.MemoryBudget.UsedByteCount(); got != 0 {
		t.Fatalf("initial network peer consumed public admission: %d", got)
	}
}

func TestWebRtcLateNetworkPromotionRebuildsPublicWindowConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.NetworkPeerReceiveBufferSize = mib(2)
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(settings.NetworkPeerReceiveBufferSize)
	settings.MaxPeerConnectionCount = 1
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	peerId := NewId()
	streamId := NewId()
	path := NewTransferPath(NewId(), peerId, streamId)
	publicConnValue, err := manager.NewP2pConnActive(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	publicConn := publicConnValue.(*peerConn)
	if publicConn.networkPeer {
		t.Fatal("connection began as a network peer before authentication")
	}

	manager.PrioritizePeer(peerId)
	select {
	case <-publicConn.ImmediateReconnect():
	case <-ctx.Done():
		t.Fatal("late Network promotion did not request an immediate rebuild")
	}
	select {
	case <-publicConn.ctx.Done():
	case <-ctx.Done():
		t.Fatal("late Network promotion did not retire the public-window association")
	}

	networkConnValue, err := manager.NewP2pConnActive(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer networkConnValue.Close()
	networkConn := networkConnValue.(*peerConn)
	if !networkConn.networkPeer {
		t.Fatal("replacement did not select the network-peer Pion API")
	}
	manager.stateLock.Lock()
	_, pending := manager.pendingPrioritizedPeerSlot[peerId]
	manager.stateLock.Unlock()
	if pending {
		t.Fatal("admitted replacement left its priority slot pending")
	}

	deadline := time.Now().Add(5 * time.Second)
	for settings.MemoryBudget.UsedByteCount() != 0 {
		if deadline.Before(time.Now()) {
			t.Fatalf("public-window reservation was not released: %d", settings.MemoryBudget.UsedByteCount())
		}
		time.Sleep(time.Millisecond)
	}
	if got := settings.NetworkPeerMemoryBudget.UsedByteCount(); got != settings.NetworkPeerReceiveBufferSize {
		t.Fatalf("network reservation = %d, want %d", got, settings.NetworkPeerReceiveBufferSize)
	}
}

func TestAuthenticatedNetworkSignalUpgradesExistingPublicWindowConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.NetworkPeerReceiveBufferSize = mib(2)
	settings.NetworkPeerMemoryBudget = NewTransferMemoryBudget(settings.NetworkPeerReceiveBufferSize)
	settings.MaxPeerConnectionCount = 1
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	peerId := NewId()
	streamId := NewId()
	path := NewTransferPath(NewId(), peerId, streamId)
	publicValue, err := manager.NewP2pConnPassive(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	publicConn := publicValue.(*peerConn)
	if publicConn.networkPeer {
		t.Fatal("passive connection began trusted before its authenticated signal")
	}

	dispatcher := newTestingSignalDispatcher(ctx, cancel, manager, 1, 2)
	defer dispatcher.Close()
	frame := testingSignalFrame(t, streamId)
	dispatcher.Receive(
		SourceId(peerId),
		[]*protocol.Frame{frame},
		Peer{ProvideMode: protocol.ProvideMode_Network},
	)
	MessagePoolReturn(frame.MessageBytes)

	select {
	case <-publicConn.ImmediateReconnect():
	case <-ctx.Done():
		t.Fatal("authenticated Network signal did not request window upgrade")
	}
	select {
	case <-publicConn.ctx.Done():
	case <-ctx.Done():
		t.Fatal("authenticated Network signal did not retire public-window connection")
	}

	replacementValue, err := manager.NewP2pConnPassive(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer replacementValue.Close()
	if !replacementValue.(*peerConn).networkPeer {
		t.Fatal("post-signal replacement did not use dedicated network window")
	}
}

func TestWebRtcIncompleteNetworkPeerAdmissionFallsBackToPublicPool(t *testing.T) {
	tests := []struct {
		name          string
		networkWindow ByteCount
		networkBudget *TransferMemoryBudget
	}{
		{
			name:          "window_without_budget",
			networkWindow: mib(2),
		},
		{
			name:          "budget_without_window",
			networkBudget: NewTransferMemoryBudget(mib(2)),
		},
	}
	for _, test := range tests {
		func() {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			settings := DefaultWebRtcSettings()
			settings.Log = NewNoopLogger()
			settings.IceServerUrls = nil
			settings.ReceiveBufferSize = kib(128)
			settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
			settings.NetworkPeerReceiveBufferSize = test.networkWindow
			settings.NetworkPeerMemoryBudget = test.networkBudget
			settings.MaxPeerConnectionCount = 0
			manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

			peerId := NewId()
			manager.PrioritizePeer(peerId)
			conn, err := manager.NewP2pConnActive(
				ctx,
				NewTransferPath(NewId(), peerId, NewId()),
			)
			if err != nil {
				t.Fatalf("%s: %v", test.name, err)
			}
			defer conn.Close()
			if conn.(*peerConn).networkPeer {
				t.Fatalf("%s: incomplete dedicated admission configuration selected network API", test.name)
			}
			if got := settings.MemoryBudget.UsedByteCount(); got != settings.ReceiveBufferSize {
				t.Fatalf("%s: public reservation = %d, want %d", test.name, got, settings.ReceiveBufferSize)
			}
			if test.networkBudget != nil && test.networkBudget.UsedByteCount() != 0 {
				t.Fatalf("%s: incomplete configuration touched dedicated budget: %d", test.name, test.networkBudget.UsedByteCount())
			}
		}()
	}
}

func TestAuthenticatedNetworkSignalPreemptsFullPeerAdmission(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.MaxPeerConnectionCount = 1
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	backgroundConn, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	background := backgroundConn.(*peerConn)
	defer background.Close()

	dispatcher := newTestingSignalDispatcher(ctx, cancel, manager, 1, 2)
	defer dispatcher.Close()
	source := SourceId(NewId())

	publicFrame := testingSignalFrame(t, NewId())
	dispatcher.Receive(
		source,
		[]*protocol.Frame{publicFrame},
		Peer{ProvideMode: protocol.ProvideMode_Public},
	)
	MessagePoolReturn(publicFrame.MessageBytes)
	select {
	case <-background.ctx.Done():
		t.Fatal("untrusted public signal preempted bounded peer admission")
	default:
	}

	networkFrame := testingSignalFrame(t, NewId())
	dispatcher.Receive(
		source,
		[]*protocol.Frame{networkFrame},
		Peer{ProvideMode: protocol.ProvideMode_Network},
	)
	MessagePoolReturn(networkFrame.MessageBytes)
	select {
	case <-background.ctx.Done():
	case <-ctx.Done():
		t.Fatal("authenticated network signal did not preempt full peer admission")
	}

	manager.stateLock.Lock()
	_, prioritized := manager.prioritizedPeers[source.SourceId]
	_, pending := manager.pendingPrioritizedPeerSlot[source.SourceId]
	manager.stateLock.Unlock()
	if !prioritized || !pending {
		t.Fatal("network signal did not reserve the released slot for its peer")
	}
	if got := settings.MemoryBudget.TotalByteCount(); got != settings.ReceiveBufferSize {
		t.Fatalf("signal priority changed hard memory bound: %d", got)
	}
}

func TestWebRtcPrioritizedNetworkPeerDoesNotEvictWhenCapacityIsFree(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = kib(128)
	settings.MemoryBudget = NewTransferMemoryBudget(2 * settings.ReceiveBufferSize)
	settings.MaxPeerConnectionCount = 2
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	backgroundConn, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	background := backgroundConn.(*peerConn)
	defer background.Close()

	priorityPeerId := NewId()
	manager.PrioritizePeer(priorityPeerId)
	select {
	case <-background.ctx.Done():
		t.Fatal("priority evicted a connection despite free bounded capacity")
	default:
	}
	priority, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), priorityPeerId, NewId()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer priority.Close()
	if got := settings.MemoryBudget.UsedByteCount(); got != 2*settings.ReceiveBufferSize {
		t.Fatalf("reservations = %d, want %d", got, 2*settings.ReceiveBufferSize)
	}
}

func TestWebRtcPeerPriorityStateIsHardBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.MaxPeerConnectionCount = 0
	settings.MemoryBudget = nil
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	var newestPeerId Id
	for range 4 * maxPeerConnectionPriorityCount {
		newestPeerId = NewId()
		manager.PrioritizePeer(newestPeerId)
	}
	manager.stateLock.Lock()
	defer manager.stateLock.Unlock()
	if got := len(manager.prioritizedPeers); maxPeerConnectionPriorityCount < got {
		t.Fatalf("prioritized peers retained = %d, max %d", got, maxPeerConnectionPriorityCount)
	}
	if got := len(manager.pendingPrioritizedPeerSlot); maxPeerConnectionPriorityCount < got {
		t.Fatalf("pending priority peers retained = %d, max %d", got, maxPeerConnectionPriorityCount)
	}
	if got := len(manager.networkPeers); maxRememberedNetworkPeerCount < got {
		t.Fatalf("remembered network peers = %d, max %d", got, maxRememberedNetworkPeerCount)
	}
	if _, ok := manager.networkPeers[newestPeerId]; !ok {
		t.Fatal("bounded network-peer identity map evicted the newest promotion")
	}
}

func TestWebRtcRepeatedNetworkSignalDoesNotRefreshAdmissionDemand(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.MaxPeerConnectionCount = 0
	settings.MemoryBudget = nil
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	peerId := NewId()
	manager.ObserveNetworkPeerSignal(peerId)
	expired := time.Now().Add(-time.Second)
	manager.stateLock.Lock()
	manager.prioritizedPeers[peerId] = expired
	manager.pendingPrioritizedPeerSlot[peerId] = expired
	rememberedTime := manager.networkPeers[peerId]
	manager.stateLock.Unlock()

	// A stale P2P stream can emit negotiation signals forever. They retain the
	// authenticated admission class but are not fresh demand and must not
	// renew either the eviction lease or its reserved pending slot.
	manager.ObserveNetworkPeerSignal(peerId)

	manager.stateLock.Lock()
	priorityUntil := manager.prioritizedPeers[peerId]
	pendingUntil := manager.pendingPrioritizedPeerSlot[peerId]
	nextRememberedTime := manager.networkPeers[peerId]
	manager.stateLock.Unlock()
	if !priorityUntil.Equal(expired) {
		t.Fatal("repeated network signal refreshed an expired admission lease")
	}
	if !pendingUntil.Equal(expired) {
		t.Fatal("repeated network signal refreshed an expired pending reservation")
	}
	if !nextRememberedTime.Equal(rememberedTime) {
		t.Fatal("repeated network signal refreshed stale peer recency")
	}
}

func TestWebRtcRepeatedConnectCloseReleasesAdmissionWithoutStall(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const cycles = 12
	const reservationSize = ByteCount(128 * 1024)
	settingsA := DefaultWebRtcSettings()
	settingsB := DefaultWebRtcSettings()
	settingsA.Log = NewNoopLogger()
	settingsB.Log = NewNoopLogger()
	settingsA.IceServerUrls = nil
	settingsB.IceServerUrls = nil
	// This test exercises repeated admission/release, not interface
	// filtering. Both peers are on one host, so restrict ICE to loopback:
	// per-cycle connects stay fast on a multihomed host, and rapid churn
	// does not depend on external-address UDP hairpin behavior.
	settingsA.UseLoopbackOnlyIceInterfaces = true
	settingsB.UseLoopbackOnlyIceInterfaces = true
	settingsA.MaxPeerConnectionCount = 1
	settingsB.MaxPeerConnectionCount = 1
	settingsA.ReceiveBufferSize = reservationSize
	settingsB.ReceiveBufferSize = reservationSize
	settingsA.MemoryBudget = NewTransferMemoryBudget(reservationSize)
	settingsB.MemoryBudget = NewTransferMemoryBudget(reservationSize)

	signalPipeA := newSignalPipe(nil)
	signalPipeB := newSignalPipe(nil)
	managerA := newTestWebRtcManager(t, ctx, signalPipeA, settingsA)
	managerB := newTestWebRtcManager(t, ctx, signalPipeB, settingsB)
	signalPipeA.SetSignalReceiver(managerB)
	signalPipeB.SetSignalReceiver(managerA)

	peerIdA := NewId()
	peerIdB := NewId()
	maxTeardown := time.Duration(0)
	for cycle := range cycles {
		streamId := NewId()
		passive, err := managerB.NewP2pConnPassive(
			ctx,
			NewTransferPath(peerIdB, peerIdA, streamId),
		)
		AssertEqual(t, err, nil)
		active, err := managerA.NewP2pConnActive(
			ctx,
			NewTransferPath(peerIdA, peerIdB, streamId),
		)
		AssertEqual(t, err, nil)

		connectedDeadline := time.Now().Add(5 * time.Second)
		for !active.Connected() || !passive.Connected() {
			if time.Now().After(connectedDeadline) {
				t.Fatalf(
					"cycle %d did not connect: active={%s} passive={%s}",
					cycle,
					testingWebRtcConnDiagnostics(active),
					testingWebRtcConnDiagnostics(passive),
				)
			}
			time.Sleep(time.Millisecond)
		}

		start := time.Now()
		AssertEqual(t, active.Close(), nil)
		AssertEqual(t, passive.Close(), nil)
		for {
			managerA.stateLock.Lock()
			countA := len(managerA.peerConns)
			managerA.stateLock.Unlock()
			managerB.stateLock.Lock()
			countB := len(managerB.peerConns)
			managerB.stateLock.Unlock()
			if countA == 0 && countB == 0 &&
				settingsA.MemoryBudget.UsedByteCount() == 0 &&
				settingsB.MemoryBudget.UsedByteCount() == 0 {
				break
			}
			if 5*time.Second < time.Since(start) {
				t.Fatalf(
					"cycle %d teardown stranded admission: peers=%d/%d budgets=%d/%d",
					cycle,
					countA,
					countB,
					settingsA.MemoryBudget.UsedByteCount(),
					settingsB.MemoryBudget.UsedByteCount(),
				)
			}
			time.Sleep(time.Millisecond)
		}
		maxTeardown = max(maxTeardown, time.Since(start))
	}
	t.Logf("%d connect/close cycles: maximum admission-release latency=%s", cycles, maxTeardown)
}

func testingWebRtcConnDiagnostics(conn WebRtcConn) string {
	peer, ok := conn.(*peerConn)
	if !ok {
		return fmt.Sprintf("type=%T connected=%t", conn, conn.Connected())
	}
	peer.signalLock.Lock()
	remoteDescriptionSet := peer.remoteDescriptionSet
	remoteCandidateCount := len(peer.remoteIceCandidateBuffer)
	remoteCandidateBytes := peer.remoteIceCandidateBufferBytes
	peer.signalLock.Unlock()
	peer.stateLock.Lock()
	connected := peer.connected
	offerSet := peer.offer != nil
	answerSet := peer.answer != nil
	localCandidateCount := len(peer.iceCandidateBuffer)
	localCandidatesReady := peer.iceCandidatesReady
	dataChannelOpen := peer.conn != nil
	peer.stateLock.Unlock()

	localDescriptionCandidateCount := 0
	if localDescription := peer.pc.LocalDescription(); localDescription != nil {
		localDescriptionCandidateCount = strings.Count(localDescription.SDP, "\na=candidate:")
	}
	remoteDescriptionCandidateCount := 0
	if remoteDescription := peer.pc.RemoteDescription(); remoteDescription != nil {
		remoteDescriptionCandidateCount = strings.Count(remoteDescription.SDP, "\na=candidate:")
	}
	var localCandidateStats int
	var remoteCandidateStats int
	var candidatePairStats int
	var candidatePairRequests uint64
	var candidatePairResponses uint64
	for _, stat := range peer.pc.GetStats() {
		switch typed := stat.(type) {
		case webrtc.ICECandidateStats:
			if typed.Type == webrtc.StatsTypeLocalCandidate {
				localCandidateStats++
			} else if typed.Type == webrtc.StatsTypeRemoteCandidate {
				remoteCandidateStats++
			}
		case webrtc.ICECandidatePairStats:
			candidatePairStats++
			candidatePairRequests += typed.RequestsSent
			candidatePairResponses += typed.ResponsesReceived
		}
	}
	return fmt.Sprintf(
		"connected=%t ctx=%v pc=%s ice=%s gather=%s signal=%s offer=%t answer=%t remote_description=%t local_candidates=buffer:%d/ready:%t/sdp:%d/stats:%d remote_candidates=buffer:%d/%dB/sdp:%d/stats:%d pairs=%d requests=%d responses=%d data_channel=%t fds=%d goroutines=%d",
		connected,
		context.Cause(peer.ctx),
		peer.pc.ConnectionState(),
		peer.pc.ICEConnectionState(),
		peer.pc.ICEGatheringState(),
		peer.pc.SignalingState(),
		offerSet,
		answerSet,
		remoteDescriptionSet,
		localCandidateCount,
		localCandidatesReady,
		localDescriptionCandidateCount,
		localCandidateStats,
		remoteCandidateCount,
		remoteCandidateBytes,
		remoteDescriptionCandidateCount,
		remoteCandidateStats,
		candidatePairStats,
		candidatePairRequests,
		candidatePairResponses,
		dataChannelOpen,
		testingOpenFileDescriptorCount(),
		runtime.NumGoroutine(),
	)
}

func testingOpenFileDescriptorCount() int {
	var directory string
	switch runtime.GOOS {
	case "darwin":
		directory = "/dev/fd"
	case "linux":
		directory = "/proc/self/fd"
	default:
		return -1
	}
	entries, err := os.ReadDir(directory)
	if err == nil {
		return len(entries)
	}
	return testingOpenFileDescriptorFallback()
}

func TestClientSignalReceiverCoalescesAdjacentCandidatesOnly(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source := SourceId(NewId())
	streamId := NewId()
	receiver := &clientSignalReceiver{
		ctx:          ctx,
		cancel:       cancel,
		queueLimit:   8,
		queueMonitor: NewMonitor(),
	}

	candidateFrame := func(candidate string) *protocol.Frame {
		messageBytes, err := ProtoMarshal(&protocol.ExchangeSignals{
			StreamId: streamId.Bytes(),
			Signals: []*protocol.ExchangeSignal{
				{
					SignalType:   protocol.SignalType_IceCandidate,
					IceCandidate: []byte(candidate),
				},
			},
		})
		AssertEqual(t, err, nil)
		return &protocol.Frame{
			MessageType:  protocol.MessageType_TransferExchangeSignals,
			MessageBytes: messageBytes,
		}
	}

	sdpFrame := func(sdp string) *protocol.Frame {
		messageBytes, err := ProtoMarshal(&protocol.ExchangeSignals{
			StreamId: streamId.Bytes(),
			Signals: []*protocol.ExchangeSignal{
				{
					SignalType: protocol.SignalType_SdpOffer,
					Sdp:        []byte(sdp),
				},
			},
		})
		AssertEqual(t, err, nil)
		return &protocol.Frame{
			MessageType:  protocol.MessageType_TransferExchangeSignals,
			MessageBytes: messageBytes,
		}
	}

	frames := []*protocol.Frame{
		candidateFrame("c1"),
		sdpFrame("sdp"),
		candidateFrame("c2"),
	}
	defer func() {
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
	}()

	for _, frame := range frames {
		received, err := newReceivedSignalFrame(source, TransferKey{}, frame)
		AssertEqual(t, err, nil)
		AssertEqual(t, receiver.enqueue(received), true)
	}

	readSignals := func() []*protocol.ExchangeSignal {
		received := receiver.dequeue()
		AssertNotEqual(t, received, nil)
		defer received.Close()
		err := received.prepareFrame()
		AssertEqual(t, err, nil)
		exchangeSignals := &protocol.ExchangeSignals{}
		err = ProtoUnmarshal(received.frame.MessageBytes, exchangeSignals)
		AssertEqual(t, err, nil)
		return exchangeSignals.Signals
	}

	signals := readSignals()
	AssertEqual(t, len(signals), 1)
	AssertEqual(t, signals[0].SignalType, protocol.SignalType_IceCandidate)
	AssertEqual(t, string(signals[0].IceCandidate), "c1")

	signals = readSignals()
	AssertEqual(t, len(signals), 1)
	AssertEqual(t, signals[0].SignalType, protocol.SignalType_SdpOffer)
	AssertEqual(t, string(signals[0].Sdp), "sdp")

	signals = readSignals()
	AssertEqual(t, len(signals), 1)
	AssertEqual(t, signals[0].SignalType, protocol.SignalType_IceCandidate)
	AssertEqual(t, string(signals[0].IceCandidate), "c2")
}

func TestClientSignalReceiverCoalescesAdjacentCandidates(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source := SourceId(NewId())
	streamId := NewId()
	receiver := &clientSignalReceiver{
		ctx:          ctx,
		cancel:       cancel,
		queueLimit:   8,
		queueMonitor: NewMonitor(),
	}

	candidateFrame := func(candidate string) *protocol.Frame {
		messageBytes, err := ProtoMarshal(&protocol.ExchangeSignals{
			StreamId: streamId.Bytes(),
			Signals: []*protocol.ExchangeSignal{
				{
					SignalType:   protocol.SignalType_IceCandidate,
					IceCandidate: []byte(candidate),
				},
			},
		})
		AssertEqual(t, err, nil)
		return &protocol.Frame{
			MessageType:  protocol.MessageType_TransferExchangeSignals,
			MessageBytes: messageBytes,
		}
	}

	frames := []*protocol.Frame{
		candidateFrame("c1"),
		candidateFrame("c2"),
	}
	defer func() {
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
	}()

	for _, frame := range frames {
		received, err := newReceivedSignalFrame(source, TransferKey{}, frame)
		AssertEqual(t, err, nil)
		AssertEqual(t, receiver.enqueue(received), true)
	}

	received := receiver.dequeue()
	AssertNotEqual(t, received, nil)
	defer received.Close()
	err := received.prepareFrame()
	AssertEqual(t, err, nil)
	exchangeSignals := &protocol.ExchangeSignals{}
	err = ProtoUnmarshal(received.frame.MessageBytes, exchangeSignals)
	AssertEqual(t, err, nil)
	AssertEqual(t, len(exchangeSignals.Signals), 2)
	AssertEqual(t, string(exchangeSignals.Signals[0].IceCandidate), "c1")
	AssertEqual(t, string(exchangeSignals.Signals[1].IceCandidate), "c2")
}

func TestClientSignalReceiverCandidateCoalescingRemainsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source := SourceId(NewId())
	streamId := NewId()
	receiver := &clientSignalReceiver{
		client:       &Client{log: NewNoopLogger()},
		ctx:          ctx,
		cancel:       cancel,
		queueLimit:   1,
		queueMonitor: NewMonitor(),
	}
	makeReceived := func(signals []*protocol.ExchangeSignal) *receivedSignalFrame {
		messageBytes, err := ProtoMarshal(&protocol.ExchangeSignals{
			StreamId: streamId.Bytes(),
			Signals:  signals,
		})
		AssertEqual(t, err, nil)
		received, err := newReceivedSignalFrame(source, TransferKey{}, &protocol.Frame{
			MessageType:  protocol.MessageType_TransferExchangeSignals,
			MessageBytes: messageBytes,
		})
		AssertEqual(t, err, nil)
		MessagePoolReturn(messageBytes)
		return received
	}

	firstSignals := make([]*protocol.ExchangeSignal, 0, maxBufferedRemoteIceCandidateCount)
	for i := range maxBufferedRemoteIceCandidateCount {
		firstSignals = append(firstSignals, &protocol.ExchangeSignal{
			SignalType:   protocol.SignalType_IceCandidate,
			IceCandidate: []byte(fmt.Sprintf("candidate-%d", i)),
		})
	}
	first := makeReceived(firstSignals)
	AssertEqual(t, receiver.enqueue(first), true)

	second := makeReceived([]*protocol.ExchangeSignal{{
		SignalType:   protocol.SignalType_IceCandidate,
		IceCandidate: []byte("candidate-overflow"),
	}})
	if receiver.enqueue(second) {
		t.Fatal("candidate coalescing bypassed the bounded full-shard drop")
	}
	second.Close()
	AssertEqual(t, receiver.droppedSignalCount.Load(), uint64(1))

	dequeued := receiver.dequeue()
	AssertEqual(t, dequeued, first)
	dequeued.Close()
}

// TestClientSignalReceiverDoesNotCoalesceDifferentTransferKeys verifies that
// receiver-visible lanes remain distinct queue entries.
func TestClientSignalReceiverDoesNotCoalesceDifferentTransferKeys(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source := SourceId(NewId())
	streamId := NewId()
	receiver := &clientSignalReceiver{
		client:       &Client{log: NewNoopLogger()},
		ctx:          ctx,
		cancel:       cancel,
		queueLimit:   2,
		queueMonitor: NewMonitor(),
	}
	defer receiver.Close()

	newCandidate := func(transferKey TransferKey, candidate string) *receivedSignalFrame {
		messageBytes, err := ProtoMarshal(&protocol.ExchangeSignals{
			StreamId: streamId.Bytes(),
			Signals: []*protocol.ExchangeSignal{{
				SignalType:   protocol.SignalType_IceCandidate,
				IceCandidate: []byte(candidate),
			}},
		})
		AssertEqual(t, err, nil)
		received, err := newReceivedSignalFrame(
			source,
			transferKey,
			&protocol.Frame{
				MessageType:  protocol.MessageType_TransferExchangeSignals,
				MessageBytes: messageBytes,
			},
		)
		MessagePoolReturn(messageBytes)
		AssertEqual(t, err, nil)
		return received
	}

	firstKey := TransferKey{
		ForceStream:         true,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: true,
	}
	secondKey := firstKey
	secondKey.EncryptionCompanion = false
	AssertEqual(t, receiver.enqueue(newCandidate(firstKey, "first")), true)
	AssertEqual(t, receiver.enqueue(newCandidate(secondKey, "second")), true)
	AssertEqual(t, receiver.receiveFrameCount, 2)

	first := receiver.dequeue()
	AssertEqual(t, first.transferKey, firstKey)
	AssertEqual(t, len(first.exchangeSignals.Signals), 1)
	AssertEqual(t, string(first.exchangeSignals.Signals[0].IceCandidate), "first")
	first.Close()
	second := receiver.dequeue()
	AssertEqual(t, second.transferKey, secondKey)
	AssertEqual(t, len(second.exchangeSignals.Signals), 1)
	AssertEqual(t, string(second.exchangeSignals.Signals[0].IceCandidate), "second")
	second.Close()
}

func TestClientSignalReceiverQueueBackingStorageRemainsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const queueLimit = 4
	receiver := &clientSignalReceiver{
		ctx:          ctx,
		cancel:       cancel,
		queueLimit:   queueLimit,
		queueMonitor: NewMonitor(),
	}
	defer receiver.Close()

	makeReceived := func() *receivedSignalFrame {
		return &receivedSignalFrame{
			frame: &protocol.Frame{
				MessageType: protocol.MessageType_TransferExchangeSignals,
			},
		}
	}
	for range queueLimit {
		AssertEqual(t, receiver.enqueue(makeReceived()), true)
	}

	// Keep the queue continuously nonempty while advancing it many times.
	// An append-only slice with a logical head still bounds live entries, but
	// retains one additional nil slot per iteration until the queue happens
	// to drain completely. A fixed ring must keep both len and cap bounded.
	for range 10_000 {
		received := receiver.dequeue()
		AssertNotEqual(t, received, nil)
		received.Close()
		AssertEqual(t, receiver.enqueue(makeReceived()), true)
	}

	receiver.queueLock.Lock()
	backingLen := len(receiver.receiveFrames)
	backingCap := cap(receiver.receiveFrames)
	liveCount := receiver.receiveFrameCount
	receiver.queueLock.Unlock()
	AssertEqual(t, liveCount, queueLimit)
	AssertEqual(t, backingLen, queueLimit)
	AssertEqual(t, backingCap, queueLimit)
}

func TestClientSignalReceiverDecodedValueOwnsFrameBytes(t *testing.T) {
	streamId := NewId()
	messageBytes, err := ProtoMarshal(&protocol.ExchangeSignals{
		StreamId:     streamId.Bytes(),
		ResetSignals: true,
		Signals: []*protocol.ExchangeSignal{
			{
				SignalType:   protocol.SignalType_IceCandidate,
				IceCandidate: []byte("candidate"),
			},
		},
	})
	AssertEqual(t, err, nil)

	received, err := newReceivedSignalFrame(SourceId(NewId()), TransferKey{}, &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: messageBytes,
	})
	AssertEqual(t, err, nil)
	defer received.Close()

	// The transfer callback owns messageBytes only until it returns. Destroy
	// that storage now and prove protobuf decoding produced an independent
	// value safe for asynchronous/sharded dispatch.
	clear(messageBytes)
	MessagePoolReturn(messageBytes)

	AssertEqual(t, Id(received.exchangeSignals.StreamId), streamId)
	AssertEqual(t, received.exchangeSignals.ResetSignals, true)
	AssertEqual(t, len(received.exchangeSignals.Signals), 1)
	AssertEqual(t, string(received.exchangeSignals.Signals[0].IceCandidate), "candidate")
}

type testingBlockingSignalReceiver struct {
	blockSource Id
	entered     chan struct{}
	release     chan struct{}
	other       chan struct{}
}

// testingSignalDropLogger retains full-shard warnings for attribution checks.
type testingSignalDropLogger struct {
	Logger
	warnings chan string
}

// Warningf records one warning without blocking the tested receive callback.
func (self *testingSignalDropLogger) Warningf(format string, args ...any) {
	self.warnings <- fmt.Sprintf(format, args...)
}

// ReceiveSignal accepts the framed compatibility path without blocking.
func (self *testingBlockingSignalReceiver) ReceiveSignal(TransferPath, TransferKey, *protocol.Frame) error {
	return nil
}

// ReceiveExchangeSignals blocks only the configured source for shard tests.
func (self *testingBlockingSignalReceiver) ReceiveExchangeSignals(
	source TransferPath,
	_ TransferKey,
	_ *protocol.ExchangeSignals,
) error {
	if source.SourceId == self.blockSource {
		select {
		case self.entered <- struct{}{}:
		default:
		}
		<-self.release
		return nil
	}
	select {
	case self.other <- struct{}{}:
	default:
	}
	return nil
}

// newTestingSignalDispatcher starts a bounded dispatcher with explicit shards.
func newTestingSignalDispatcher(
	ctx context.Context,
	cancel context.CancelFunc,
	receiver SignalReceiver,
	workerCount int,
	queueLimit int,
) *clientSignalDispatcher {
	client := &Client{log: NewNoopLogger()}
	dispatcher := &clientSignalDispatcher{
		client:   client,
		receiver: receiver,
		ctx:      ctx,
		cancel:   cancel,
		shards:   make([]*clientSignalReceiver, 0, workerCount),
	}
	for range workerCount {
		shard := &clientSignalReceiver{
			client:       client,
			receiver:     receiver,
			ctx:          ctx,
			cancel:       cancel,
			queueLimit:   queueLimit,
			queueMonitor: NewMonitor(),
			dropWarnings: make(chan signalDropWarning, 1),
		}
		dispatcher.shards = append(dispatcher.shards, shard)
		shard.start()
	}
	return dispatcher
}

// testingSignalFrame owns one waiting-for-offer signal for the supplied stream.
func testingSignalFrame(t *testing.T, streamId Id) *protocol.Frame {
	messageBytes, err := ProtoMarshal(&protocol.ExchangeSignals{
		StreamId: streamId.Bytes(),
		Signals: []*protocol.ExchangeSignal{
			{SignalType: protocol.SignalType_WaitingForSdpOffer},
		},
	})
	AssertEqual(t, err, nil)
	return &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: messageBytes,
	}
}

func TestClientSignalDispatcherStalledPeerDoesNotBlockOtherShard(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	streamA := NewId()
	sourceA := SourceId(NewId())
	shardA := receivedSignalShardIndex(&receivedSignalFrame{
		keyValid: true,
		key: receivedSignalFrameKey{
			source:   sourceA,
			streamId: streamA,
		},
	}, 2)
	var sourceB TransferPath
	var streamB Id
	for {
		sourceB = SourceId(NewId())
		streamB = NewId()
		if receivedSignalShardIndex(&receivedSignalFrame{
			keyValid: true,
			key: receivedSignalFrameKey{
				source:   sourceB,
				streamId: streamB,
			},
		}, 2) != shardA {
			break
		}
	}

	receiver := &testingBlockingSignalReceiver{
		blockSource: sourceA.SourceId,
		entered:     make(chan struct{}, 1),
		release:     make(chan struct{}),
		other:       make(chan struct{}, 1),
	}
	dispatcher := newTestingSignalDispatcher(ctx, cancel, receiver, 2, 2)
	defer dispatcher.Close()

	frameA := testingSignalFrame(t, streamA)
	dispatcher.handleControlFrame(sourceA, TransferKey{}, frameA)
	MessagePoolReturn(frameA.MessageBytes)
	select {
	case <-receiver.entered:
	case <-time.After(time.Second):
		t.Fatal("blocking peer did not enter its signal callback")
	}

	frameB := testingSignalFrame(t, streamB)
	dispatcher.handleControlFrame(sourceB, TransferKey{}, frameB)
	MessagePoolReturn(frameB.MessageBytes)
	select {
	case <-receiver.other:
	case <-time.After(time.Second):
		t.Fatal("independent peer was blocked behind a stalled signal callback")
	}
	close(receiver.release)
}

// TestClientSignalDispatcherFullShardDropsWithoutBlockingReceiveCallback
// verifies an observable drop without parking the shared callback.
func TestClientSignalDispatcherFullShardDropsWithoutBlockingReceiveCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	source := SourceId(NewId())
	streamId := NewId()
	receiver := &testingBlockingSignalReceiver{
		blockSource: source.SourceId,
		entered:     make(chan struct{}, 1),
		release:     make(chan struct{}),
		other:       make(chan struct{}, 1),
	}
	dispatcher := newTestingSignalDispatcher(ctx, cancel, receiver, 1, 1)
	defer dispatcher.Close()
	dropLog := &testingSignalDropLogger{
		Logger:   NewNoopLogger(),
		warnings: make(chan string, 1),
	}
	dispatcher.client.log = dropLog

	first := testingSignalFrame(t, streamId)
	dispatcher.handleControlFrame(source, TransferKey{}, first)
	MessagePoolReturn(first.MessageBytes)
	select {
	case <-receiver.entered:
	case <-time.After(time.Second):
		t.Fatal("blocking peer did not enter its signal callback")
	}

	second := testingSignalFrame(t, streamId)
	dispatcher.handleControlFrame(source, TransferKey{}, second)
	MessagePoolReturn(second.MessageBytes)

	thirdReturned := make(chan struct{})
	third := testingSignalFrame(t, streamId)
	go func() {
		dispatcher.handleControlFrame(source, TransferKey{}, third)
		MessagePoolReturn(third.MessageBytes)
		close(thirdReturned)
	}()
	select {
	case <-thirdReturned:
	case <-time.After(time.Second):
		t.Fatal("full signal shard blocked the shared receive callback")
	}
	AssertEqual(t, dispatcher.shards[0].droppedSignalCount.Load(), uint64(1))
	dispatcher.shards[0].queueLock.Lock()
	queuedCount := dispatcher.shards[0].receiveFrameCount
	dispatcher.shards[0].queueLock.Unlock()
	AssertEqual(t, queuedCount, 1)
	select {
	case warning := <-dropLog.warnings:
		if !strings.Contains(warning, source.SourceId.String()) ||
			!strings.Contains(warning, streamId.String()) ||
			!strings.Contains(warning, "dropped=1") {
			t.Fatalf("signal drop warning is not attributable: %q", warning)
		}
	case <-time.After(time.Second):
		t.Fatal("full signal shard did not emit a warning")
	}
	close(receiver.release)
}

// testingRecordedSignalSend is one synchronously decoded outbound signal.
type testingRecordedSignalSend struct {
	destinationId Id
	signals       *protocol.ExchangeSignals
	opts          []any
}

// testingTransferKeySignalSender records owned signal sends for reply checks.
type testingTransferKeySignalSender struct {
	sends chan testingRecordedSignalSend
}

// SendSignal consumes the frame and records its destination, value, and options.
func (self *testingTransferKeySignalSender) SendSignal(
	destinationId Id,
	frame *protocol.Frame,
	opts ...any,
) {
	defer MessagePoolReturn(frame.MessageBytes)
	signals := &protocol.ExchangeSignals{}
	if err := ProtoUnmarshal(frame.MessageBytes, signals); err != nil {
		panic(err)
	}
	self.sends <- testingRecordedSignalSend{
		destinationId: destinationId,
		signals:       signals,
		opts:          slices.Clone(opts),
	}
}

// TestClientSignalDispatcherPreservesTransferKeyForReply verifies that async
// dispatch preserves a non-default lane key through its generated reply.
func TestClientSignalDispatcherPreservesTransferKeyForReply(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	peerId := NewId()
	streamId := NewId()
	sender := &testingTransferKeySignalSender{
		sends: make(chan testingRecordedSignalSend, 1),
	}
	conn := &peerConn{
		ctx:              ctx,
		log:              NewNoopLogger(),
		key:              peerConnKey{PeerId: peerId, StreamId: streamId},
		active:           true,
		signalSender:     sender,
		signalGeneration: NewId(),
		offer: &protocol.ExchangeSignal{
			SignalType: protocol.SignalType_SdpOffer,
			Sdp:        []byte("cached offer"),
		},
	}
	manager := &WebRtcManager{
		log:       NewNoopLogger(),
		peerConns: map[peerConnKey]*peerConn{conn.key: conn},
	}
	dispatcher := newTestingSignalDispatcher(ctx, cancel, manager, 1, 4)
	defer dispatcher.Close()

	transferKey := TransferKey{
		CompanionContract:   true,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: true,
	}
	frame := testingSignalFrame(t, streamId)
	dispatcher.Receive(
		SourceId(peerId),
		[]*protocol.Frame{frame},
		Peer{TransferKey: transferKey},
	)
	MessagePoolReturn(frame.MessageBytes)

	var sent testingRecordedSignalSend
	select {
	case sent = <-sender.sends:
	case <-time.After(time.Second):
		t.Fatal("inbound signal did not produce the cached-offer reply")
	}
	AssertEqual(t, sent.destinationId, peerId)
	AssertEqual(t, len(sent.signals.Signals), 1)
	AssertEqual(t, sent.signals.Signals[0].SignalType, protocol.SignalType_SdpOffer)

	transferKeyIndex := -1
	forceStreamIndex := -1
	companionContractIndex := -1
	nonBlocking := false
	for index, opt := range sent.opts {
		switch value := opt.(type) {
		case TransferKey:
			AssertEqual(t, value, transferKey)
			transferKeyIndex = index
		case transferOptionsSetForceStream:
			forceStreamIndex = index
		case transferOptionsSetCompanionContract:
			companionContractIndex = index
		case signalSendNonBlocking:
			nonBlocking = true
		}
	}
	if transferKeyIndex < 0 {
		t.Fatal("signal reply did not carry the receiver-visible TransferKey")
	}
	if forceStreamIndex <= transferKeyIndex {
		t.Fatalf(
			"reply route policy was not derived after the TransferKey: key=%d force_stream=%d",
			transferKeyIndex,
			forceStreamIndex,
		)
	}
	if companionContractIndex <= transferKeyIndex {
		t.Fatalf(
			"reply companion policy was not derived after the TransferKey: key=%d companion=%d",
			transferKeyIndex,
			companionContractIndex,
		)
	}
	if !nonBlocking {
		t.Fatal("receive-path signal reply did not carry the non-blocking marker")
	}

	client := &Client{
		ctx:      ctx,
		settings: DefaultClientSettings(),
	}
	resolved := client.resolveSendOptions(sent.opts)
	AssertEqual(t, resolved.transferOptions.ForceStream, true)
	AssertEqual(t, resolved.transferOptions.CompanionContract, false)
	AssertEqual(t, resolved.encryptionRole, sequenceTlsRoleServer)
	AssertEqual(t, resolved.encryptionCompanion, true)
}

// TestPeerConnPassiveSignalReplyDerivesRouteAfterTransferKey verifies that a
// passive reply changes contract policy without changing its encryption lane.
func TestPeerConnPassiveSignalReplyDerivesRouteAfterTransferKey(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sender := &testingTransferKeySignalSender{
		sends: make(chan testingRecordedSignalSend, 1),
	}
	conn := &peerConn{
		ctx: ctx,
		key: peerConnKey{
			PeerId:   NewId(),
			StreamId: NewId(),
		},
		signalSender:     sender,
		signalGeneration: NewId(),
	}
	transferKey := TransferKey{
		ForceStream:         true,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: true,
	}
	conn.setSignalReplyTransferKey(transferKey)
	conn.sendSignalsNonBlocking([]*protocol.ExchangeSignal{{
		SignalType: protocol.SignalType_SdpAnswer,
	}})

	var sent testingRecordedSignalSend
	select {
	case sent = <-sender.sends:
	case <-time.After(time.Second):
		t.Fatal("passive signal reply was not sent")
	}
	AssertEqual(t, sent.destinationId, conn.key.PeerId)
	client := &Client{
		ctx:      ctx,
		settings: DefaultClientSettings(),
	}
	resolved := client.resolveSendOptions(sent.opts)
	AssertEqual(t, resolved.transferOptions.ForceStream, false)
	AssertEqual(t, resolved.transferOptions.CompanionContract, true)
	AssertEqual(t, resolved.encryptionRole, sequenceTlsRoleServer)
	AssertEqual(t, resolved.encryptionCompanion, true)
}

// A delayed Pion callback after another signal changes the immediate reply
// lane must retain the lane of the SDP negotiation that caused its gathering.
func TestPeerConnDeferredIceCandidateKeepsNegotiationTransferKey(t *testing.T) {
	sender := &testingTransferKeySignalSender{
		sends: make(chan testingRecordedSignalSend, 1),
	}
	conn := &peerConn{
		key: peerConnKey{
			PeerId:   NewId(),
			StreamId: NewId(),
		},
		active:           true,
		signalSender:     sender,
		signalGeneration: NewId(),
	}
	negotiationTransferKey := TransferKey{
		ForceStream:         true,
		EncryptionRole:      protocol.SequenceRole_SequenceRoleServer,
		EncryptionCompanion: true,
	}
	laterTransferKey := negotiationTransferKey
	laterTransferKey.EncryptionCompanion = false
	conn.setSignalReplyTransferKey(negotiationTransferKey)
	// SDP readiness fixes the association before Pion runs its deferred
	// candidate callback.
	conn.flushIceCandidates()

	callbackScheduled := make(chan struct{})
	releaseCallback := make(chan struct{})
	callbackDone := make(chan struct{})
	candidate := &webrtc.ICECandidate{
		Foundation: "deferred",
		Priority:   1,
		Address:    "192.0.2.1",
		Protocol:   webrtc.ICEProtocolUDP,
		Port:       10000,
		Typ:        webrtc.ICECandidateTypeHost,
		Component:  1,
	}
	go func() {
		close(callbackScheduled)
		<-releaseCallback
		conn.sendIceCandidate(candidate)
		close(callbackDone)
	}()
	<-callbackScheduled
	conn.setSignalReplyTransferKey(laterTransferKey)
	close(releaseCallback)

	var sent testingRecordedSignalSend
	select {
	case sent = <-sender.sends:
	case <-time.After(time.Second):
		t.Fatal("deferred ICE candidate was not sent")
	}
	select {
	case <-callbackDone:
	case <-time.After(time.Second):
		t.Fatal("deferred ICE callback did not return")
	}
	var sentTransferKey TransferKey
	transferKeyFound := false
	for _, opt := range sent.opts {
		if transferKey, ok := opt.(TransferKey); ok {
			sentTransferKey = transferKey
			transferKeyFound = true
		}
	}
	if !transferKeyFound {
		t.Fatal("deferred ICE candidate omitted its negotiation TransferKey")
	}
	AssertEqual(t, sentTransferKey, negotiationTransferKey)
	if sentTransferKey == laterTransferKey {
		t.Fatal("deferred ICE candidate was retargeted to a later signal lane")
	}
}

// TestClientSignalSenderFailedSendReturnsMessageBytes verifies that a rejected
// send returns its pooled signal bytes exactly once.
func TestClientSignalSenderFailedSendReturnsMessageBytes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	log := &captureLogger{}
	client := &Client{
		ctx: ctx,
		log: log,
	}
	sender := NewClientSignalSender(client)
	messageBytes := MessagePoolCopy([]byte("rejected signal"))
	if pooled, _ := MessagePoolCheck(messageBytes); !pooled {
		t.Fatal("test signal did not use pooled storage")
	}
	witness := MessagePoolShareReadOnly(messageBytes)
	frame := &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: messageBytes,
	}
	destinationId := NewId()
	sender.SendSignal(destinationId, frame)
	AssertEqual(t, frame.MessageBytes, []byte(nil))
	if !MessagePoolReturn(witness) {
		t.Fatal("rejected signal retained its exact pooled message ownership")
	}
	if len(log.info) != 1 || log.info[0] != "[signal]send failed mode=sender reason=canceled-or-closed\n" {
		t.Fatalf("rejected signal log = %q", log.info)
	}
	if strings.Contains(log.info[0], destinationId.String()) {
		t.Fatalf("rejected signal log retained destination id: %q", log.info[0])
	}
}

// A receive-originated signal must retain its zero-wait contract while making
// the admission refusal distinguishable from lifecycle closure.
func TestClientSignalSenderReportsNonblockingAdmissionRefusal(t *testing.T) {
	ctx := context.Background()
	destinationId := NewId()
	loopback := make(chan *SendPack, 1)
	loopback <- &SendPack{}
	log := &captureLogger{}
	client := &Client{
		ctx:      ctx,
		clientId: destinationId,
		loopback: loopback,
		log:      log,
		settings: &ClientSettings{},
	}
	sender := NewClientSignalSender(client)
	messageBytes := MessagePoolCopy([]byte("nonblocking signal"))
	witness := MessagePoolShareReadOnly(messageBytes)
	frame := &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: messageBytes,
	}

	sender.SendSignal(destinationId, frame, signalSendNonBlocking{})

	AssertEqual(t, frame.MessageBytes, []byte(nil))
	if !MessagePoolReturn(witness) {
		t.Fatal("nonblocking refusal retained its exact pooled message ownership")
	}
	if len(log.info) != 1 || log.info[0] != "[signal]send failed mode=receive-reply reason=not-admitted\n" {
		t.Fatalf("nonblocking refusal log = %q", log.info)
	}
	if strings.Contains(log.info[0], destinationId.String()) {
		t.Fatalf("nonblocking refusal log retained destination id: %q", log.info[0])
	}
}

// A successful handoff still transfers the pooled frame to the Client while
// its verbose trace retains no destination identity.
func TestClientSignalSenderSuccessfulSendTransfersOwnership(t *testing.T) {
	ctx := context.Background()
	destinationId := NewId()
	loopback := make(chan *SendPack, 1)
	log := &captureLogger{enabled: true}
	client := &Client{
		ctx:      ctx,
		clientId: destinationId,
		loopback: loopback,
		log:      log,
		settings: &ClientSettings{},
	}
	sender := NewClientSignalSender(client)
	messageBytes := MessagePoolCopy([]byte("accepted signal"))
	witness := MessagePoolShareReadOnly(messageBytes)
	frame := &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: messageBytes,
	}

	sender.SendSignal(destinationId, frame)

	accepted := <-loopback
	if accepted.Frame != frame {
		t.Fatal("successful signal handoff changed frame ownership")
	}
	MessagePoolReturn(accepted.Frame.MessageBytes)
	accepted.Frame.MessageBytes = nil
	if !MessagePoolReturn(witness) {
		t.Fatal("successful signal retained pooled message ownership after consumer return")
	}
	if len(log.info) != 1 || log.info[0] != "[signal]send mode=sender\n" {
		t.Fatalf("successful signal log = %q", log.info)
	}
	if strings.Contains(log.info[0], destinationId.String()) {
		t.Fatalf("successful signal log retained destination id: %q", log.info[0])
	}
}

// Detailed transfer errors are reduced to fixed labels and never copied into
// the signal log. Unary wrapping of the typed encryption error is preserved.
func TestSignalSendFailureReasonIsBounded(t *testing.T) {
	tests := []struct {
		err  error
		want string
	}{
		{err: nil, want: "not-admitted"},
		{err: ErrEncryptionRequiredNotEstablished, want: "encryption-not-ready"},
		{err: fmt.Errorf("synthetic wrapper: %w", ErrEncryptionRequiredNotEstablished), want: "encryption-not-ready"},
		{err: errors.New("Done"), want: "canceled-or-closed"},
		{err: errors.New("Done."), want: "canceled-or-closed"},
		{err: errors.New("synthetic private detail"), want: "other"},
	}
	for _, test := range tests {
		if got := signalSendFailureReason(test.err); got != test.want {
			t.Errorf("signal send reason for %v = %q, want %q", test.err, got, test.want)
		}
	}
}

func TestWebRtcManagerPeerConnectionFactoryIsLazy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)
	if manager.networkChangeWorker != nil {
		t.Fatal("idle manager eagerly started a network-change worker")
	}

	AssertEqual(t, manager.peerConnectionFactoryInitialized, false)
	conn, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	AssertEqual(t, err, nil)
	AssertEqual(t, manager.peerConnectionFactoryInitialized, true)
	manager.peerConnectionFactoryLock.Lock()
	if manager.peerConnectionCertificate == nil {
		manager.peerConnectionFactoryLock.Unlock()
		t.Fatal("initialized manager did not retain its DTLS certificate")
	}
	manager.peerConnectionFactoryLock.Unlock()
	conn.Close()

	cancel()
	select {
	case <-manager.closeDone:
	case <-time.After(5 * time.Second):
		t.Fatal("manager close did not finish after context cancellation")
	}
	manager.peerConnectionFactoryLock.Lock()
	factoryClosed := manager.peerConnectionFactoryClosed
	factory := manager.peerConnectionFactory
	certificate := manager.peerConnectionCertificate
	manager.peerConnectionFactoryLock.Unlock()
	if !factoryClosed || factory != nil || certificate != nil {
		t.Fatal("manager close retained its factory or certificate")
	}
}

func TestWebRtcManagerFactoryFailureRetriesAfterBoundedCooldown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	factoryErr := errors.New("transient factory failure")
	factoryCalls := 0
	manager.newPeerConnectionFactory = func(
		settings *WebRtcSettings,
		certificate *webrtc.Certificate,
	) (*webRtcPeerConnectionFactory, *webrtc.Certificate, error) {
		factoryCalls++
		if factoryCalls == 1 {
			return nil, nil, factoryErr
		}
		return newWebRtcPeerConnectionFactory(settings, certificate)
	}
	newActive := func() (WebRtcConn, error) {
		return manager.NewP2pConnActive(
			ctx,
			NewTransferPath(NewId(), NewId(), NewId()),
		)
	}
	if _, err := newActive(); !errors.Is(err, factoryErr) {
		t.Fatalf("first factory error = %v, want %v", err, factoryErr)
	}
	if _, err := newActive(); !errors.Is(err, factoryErr) {
		t.Fatalf("cached factory error = %v, want %v", err, factoryErr)
	}
	AssertEqual(t, factoryCalls, 1)

	manager.peerConnectionFactoryLock.Lock()
	manager.peerConnectionFactoryRetryTime = time.Now().Add(-time.Second)
	manager.peerConnectionFactoryLock.Unlock()
	conn, err := newActive()
	AssertEqual(t, err, nil)
	defer conn.Close()
	AssertEqual(t, factoryCalls, 2)
}

func TestWebRtcManagerCanceledStreamDoesNotAllocatePeerConnection(t *testing.T) {
	managerCtx, managerCancel := context.WithCancel(context.Background())
	defer managerCancel()
	streamCtx, streamCancel := context.WithCancel(managerCtx)
	streamCancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	budget := NewTransferMemoryBudget(settings.ReceiveBufferSize)
	settings.MemoryBudget = budget
	manager := newTestWebRtcManager(t, managerCtx, &testing_noopSignalSender{}, settings)

	conn, err := manager.NewP2pConnActive(
		streamCtx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	AssertEqual(t, conn, nil)
	AssertEqual(t, errors.Is(err, context.Canceled), true)
	AssertEqual(t, manager.peerConnectionFactoryInitialized, false)
	AssertEqual(t, budget.UsedByteCount(), ByteCount(0))
}

func TestWebRtcManagerCloseAndWaitReleasesOwnedResources(t *testing.T) {
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.UseEgressOnlyIceInterfaces = false
	settings.MaxPeerConnectionCount = 1
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)

	// Keep both parent and stream contexts live: explicit manager ownership,
	// rather than incidental parent cancellation, must release the peer.
	parentCtx := context.Background()
	streamCtx, streamCancel := context.WithCancel(parentCtx)
	defer streamCancel()
	manager := newTestWebRtcManager(t, parentCtx, &testing_noopSignalSender{}, settings)
	_, err := manager.NewP2pConnActive(
		streamCtx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	AssertEqual(t, err, nil)
	if got := settings.MemoryBudget.UsedByteCount(); got != settings.ReceiveBufferSize {
		t.Fatalf("reservation before close = %d, want %d", got, settings.ReceiveBufferSize)
	}

	closeReturned := make(chan error, 1)
	go func() {
		closeReturned <- manager.closeAndWait(context.Background())
	}()
	select {
	case closeErr := <-closeReturned:
		if closeErr != nil {
			t.Fatalf("manager CloseAndWait = %v", closeErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("manager CloseAndWait did not join peer teardown")
	}

	if got := settings.MemoryBudget.UsedByteCount(); got != 0 {
		t.Fatalf("reservation after close = %d, want 0", got)
	}
	manager.stateLock.Lock()
	closed := manager.closed
	peerCount := len(manager.peerConns)
	manager.stateLock.Unlock()
	if !closed || peerCount != 0 {
		t.Fatalf("closed manager retained state: closed=%t peers=%d", closed, peerCount)
	}
	manager.peerConnectionFactoryLock.Lock()
	factoryClosed := manager.peerConnectionFactoryClosed
	manager.peerConnectionFactoryLock.Unlock()
	if !factoryClosed {
		t.Fatal("manager CloseAndWait returned before its peer-connection factory closed")
	}
	if _, err := manager.NewP2pConnActive(
		streamCtx,
		NewTransferPath(NewId(), NewId(), NewId()),
	); !errors.Is(err, context.Canceled) && !errors.Is(err, os.ErrClosed) {
		t.Fatalf("post-close peer admission error = %v, want closed", err)
	}

	// Idempotent Close must return immediately after the first synchronous
	// teardown rather than starting another lifecycle.
	manager.Close()
}

type blockingPeerSignalSender struct {
	entered     chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

type contextBackpressuredPeerSignalSender struct {
	entered chan struct{}
}

// SendSignal holds its owned frame until the peer generation context ends.
func (self *contextBackpressuredPeerSignalSender) SendSignal(
	_ Id,
	signal *protocol.Frame,
	opts ...any,
) {
	defer MessagePoolReturn(signal.MessageBytes)
	var ctx context.Context
	for _, opt := range opts {
		if value, ok := opt.(transferCtx); ok {
			ctx = value.Ctx
		}
	}
	if ctx == nil {
		panic("peer signal did not carry its generation context")
	}
	close(self.entered)
	<-ctx.Done()
}

func TestWebRtcSignalBackpressureEndsWithPeerGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	sender := &contextBackpressuredPeerSignalSender{
		entered: make(chan struct{}),
	}
	conn := &peerConn{
		ctx:              ctx,
		key:              peerConnKey{PeerId: NewId(), StreamId: NewId()},
		sourceId:         NewId(),
		active:           true,
		signalSender:     sender,
		signalGeneration: NewId(),
	}

	returned := make(chan struct{})
	go func() {
		conn.sendSignal(&protocol.ExchangeSignal{
			SignalType: protocol.SignalType_WaitingForSdpOffer,
		})
		close(returned)
	}()
	select {
	case <-sender.entered:
	case <-time.After(time.Second):
		t.Fatal("signal send did not enter intentional backpressure")
	}
	select {
	case <-returned:
		t.Fatal("live peer signal did not preserve send backpressure")
	default:
	}

	cancel()
	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("canceled peer generation did not release signal backpressure")
	}
}

func newBlockingPeerSignalSender() *blockingPeerSignalSender {
	return &blockingPeerSignalSender{
		entered: make(chan struct{}, 4),
		release: make(chan struct{}),
	}
}

// SendSignal holds its owned frame until the test releases the sender.
func (self *blockingPeerSignalSender) SendSignal(
	_ Id,
	signal *protocol.Frame,
	_ ...any,
) {
	defer MessagePoolReturn(signal.MessageBytes)
	select {
	case self.entered <- struct{}{}:
	default:
	}
	<-self.release
}

func (self *blockingPeerSignalSender) Release() {
	self.releaseOnce.Do(func() {
		close(self.release)
	})
}

func TestWebRtcCanceledPeerReleasesAdmissionWhileSignalSendIsBackpressured(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	sender := newBlockingPeerSignalSender()
	defer sender.Release()
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.UseEgressOnlyIceInterfaces = false
	settings.MaxPeerConnectionCount = 1
	settings.MemoryBudget = NewTransferMemoryBudget(settings.ReceiveBufferSize)
	manager := newTestWebRtcManager(t, ctx, sender, settings)

	path := NewTransferPath(NewId(), NewId(), NewId())
	conn, err := manager.NewP2pConnActive(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-sender.entered:
	case <-ctx.Done():
		t.Fatal("peer did not enter the intentionally backpressured signal send")
	}

	capacityNotify := settings.MemoryBudget.CapacityNotify()
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-capacityNotify:
	case <-time.After(time.Second):
		t.Fatal("canceled peer retained its receive-window reservation behind signal backpressure")
	}
	if got := settings.MemoryBudget.UsedByteCount(); got != 0 {
		t.Fatalf("reservation after cancellation = %d, want 0", got)
	}
	manager.stateLock.Lock()
	retiringCount := len(manager.retiringPeerConns)
	manager.stateLock.Unlock()
	if retiringCount != 0 {
		t.Fatalf("completed teardown retained %d retiring generations", retiringCount)
	}

	replacement, err := manager.NewP2pConnActive(ctx, path)
	if err != nil {
		t.Fatalf("replacement was not admitted after resource teardown: %v", err)
	}
	defer replacement.Close()
	if got := settings.MemoryBudget.UsedByteCount(); got != settings.ReceiveBufferSize {
		t.Fatalf("replacement reservation = %d, want %d", got, settings.ReceiveBufferSize)
	}
}

func TestWebRtcReplacementBudgetPreservesNewestGenerationWhilePriorTeardownIsPending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const window = ByteCount(128 * 1024)
	budget := NewTransferMemoryBudget(2 * window)
	if !budget.TryReserve(2 * window) {
		t.Fatal("failed to reserve modeled replacement generations")
	}
	defer budget.Release(2 * window)

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = window
	settings.MemoryBudget = budget
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	peerId := NewId()
	streamId := NewId()
	key := peerConnKey{PeerId: peerId, StreamId: streamId}
	retiringCtx, retiringCancel := context.WithCancel(ctx)
	retiringCancel()
	retiring := &peerConn{
		ctx:                retiringCtx,
		key:                key,
		admissionBudget:    budget,
		admissionByteCount: window,
	}
	currentCtx, currentCancel := context.WithCancel(ctx)
	current := &peerConn{
		ctx:                currentCtx,
		cancel:             currentCancel,
		key:                key,
		admissionBudget:    budget,
		admissionByteCount: window,
	}
	manager.stateLock.Lock()
	manager.peerConns[key] = current
	manager.retiringPeerConns[retiring] = struct{}{}
	manager.stateLock.Unlock()

	_, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), peerId, streamId),
	)
	var admissionErr *peerConnectionAdmissionError
	if !errors.As(err, &admissionErr) ||
		admissionErr.reason != peerConnectionAdmissionBudget {
		t.Fatalf("replacement admission = %T %v, want budget refusal", err, err)
	}
	select {
	case <-currentCtx.Done():
		t.Fatal("pending old teardown canceled the newest keyed generation")
	default:
	}
	manager.stateLock.Lock()
	retiringCount := len(manager.retiringPeerConns)
	manager.stateLock.Unlock()
	if retiringCount != 1 {
		t.Fatalf("retiring generations = %d, want only the old generation", retiringCount)
	}
}

func TestWebRtcReplacementBudgetRetiresCurrentGenerationWithoutPriorRelease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const window = ByteCount(128 * 1024)
	budget := NewTransferMemoryBudget(window)
	if !budget.TryReserve(window) {
		t.Fatal("failed to reserve modeled current generation")
	}
	defer budget.Release(window)

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.ReceiveBufferSize = window
	settings.MemoryBudget = budget
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	peerId := NewId()
	streamId := NewId()
	key := peerConnKey{PeerId: peerId, StreamId: streamId}
	currentCtx, currentCancel := context.WithCancel(ctx)
	current := &peerConn{
		ctx:                currentCtx,
		cancel:             currentCancel,
		key:                key,
		admissionBudget:    budget,
		admissionByteCount: window,
	}
	manager.stateLock.Lock()
	manager.peerConns[key] = current
	manager.stateLock.Unlock()

	_, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), peerId, streamId),
	)
	var admissionErr *peerConnectionAdmissionError
	if !errors.As(err, &admissionErr) ||
		admissionErr.reason != peerConnectionAdmissionBudget {
		t.Fatalf("replacement admission = %T %v, want budget refusal", err, err)
	}
	select {
	case <-currentCtx.Done():
	default:
		t.Fatal("full replacement budget did not retire the current generation")
	}
	manager.stateLock.Lock()
	_, retiringCurrent := manager.retiringPeerConns[current]
	manager.stateLock.Unlock()
	if !retiringCurrent {
		t.Fatal("retired current generation was not tracked through teardown")
	}
}

func TestWebRtcOffMapByteRetirementDoesNotClaimCountRelease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.MaxPeerConnectionCount = 1
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	currentKey := peerConnKey{PeerId: NewId(), StreamId: NewId()}
	currentCtx, currentCancel := context.WithCancel(ctx)
	current := &peerConn{
		ctx:    currentCtx,
		cancel: currentCancel,
		key:    currentKey,
	}
	retiringCtx, retiringCancel := context.WithCancel(ctx)
	retiringCancel()
	retiring := &peerConn{ctx: retiringCtx, key: currentKey}
	selectedPeerId := NewId()
	manager.stateLock.Lock()
	manager.peerConns[currentKey] = current
	manager.retiringPeerConns[retiring] = struct{}{}
	manager.prioritizedPeers[selectedPeerId] =
		time.Now().Add(peerConnectionPriorityTimeout)
	manager.stateLock.Unlock()

	_, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), selectedPeerId, NewId()),
	)
	var admissionErr *peerConnectionAdmissionError
	if !errors.As(err, &admissionErr) ||
		admissionErr.reason != peerConnectionAdmissionCount {
		t.Fatalf("selected admission = %T %v, want count refusal", err, err)
	}
	select {
	case <-currentCtx.Done():
	default:
		t.Fatal("off-map byte teardown incorrectly suppressed count reclamation")
	}
}

type blockingDetachedDataChannel struct {
	entered   chan struct{}
	closed    chan struct{}
	closeOnce sync.Once
}

func newBlockingDetachedDataChannel() *blockingDetachedDataChannel {
	return &blockingDetachedDataChannel{
		entered: make(chan struct{}, 1),
		closed:  make(chan struct{}),
	}
}

func (self *blockingDetachedDataChannel) Read([]byte) (int, error) {
	select {
	case self.entered <- struct{}{}:
	default:
	}
	<-self.closed
	return 0, net.ErrClosed
}

func (self *blockingDetachedDataChannel) ReadDataChannel(b []byte) (int, bool, error) {
	n, err := self.Read(b)
	return n, false, err
}

func (self *blockingDetachedDataChannel) Write(b []byte) (int, error) {
	return len(b), nil
}

func (self *blockingDetachedDataChannel) WriteDataChannel(b []byte, _ bool) (int, error) {
	return self.Write(b)
}

func (self *blockingDetachedDataChannel) Close() error {
	self.closeOnce.Do(func() {
		close(self.closed)
	})
	return nil
}

func (self *blockingDetachedDataChannel) SetReadDeadline(time.Time) error {
	return nil
}

func (self *blockingDetachedDataChannel) SetWriteDeadline(time.Time) error {
	return nil
}

func TestWebRtcPeerTeardownClosesDetachedDataPlane(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	raw := newBlockingDetachedDataChannel()
	conn := &peerConn{
		ctx:              ctx,
		cancel:           cancel,
		log:              NewNoopLogger(),
		conn:             raw,
		connMonitor:      NewMonitor(),
		connectedMonitor: NewMonitor(),
		teardownDone:     make(chan struct{}),
	}

	readReturned := make(chan error, 1)
	go func() {
		_, err := conn.Read(make([]byte, 1))
		readReturned <- err
	}()
	select {
	case <-raw.entered:
	case <-time.After(time.Second):
		t.Fatal("detached data-plane read did not start")
	}
	cancel()
	conn.teardown()

	select {
	case err := <-readReturned:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("blocked read error = %v, want closed", err)
		}
	case <-time.After(time.Second):
		t.Fatal("peer teardown did not unblock detached data-plane read")
	}
	conn.stateLock.Lock()
	retained := conn.conn
	conn.stateLock.Unlock()
	if retained != nil {
		t.Fatal("peer teardown retained detached data channel")
	}
}

func TestWebRtcPeerTeardownStopsTransportBeforePeerConnection(t *testing.T) {
	transportStopped := make(chan struct{})
	var stopOnce sync.Once
	peerCloseReturned := make(chan struct{})

	err := closeTransportBeforePeerConnection(
		func() error {
			stopOnce.Do(func() {
				close(transportStopped)
			})
			return nil
		},
		func() error {
			select {
			case <-transportStopped:
				close(peerCloseReturned)
				return nil
			case <-time.After(time.Second):
				return errors.New("peer close remained blocked on its physical transport")
			}
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-peerCloseReturned:
	default:
		t.Fatal("peer connection closed before its physical transport stopped")
	}
}

// The pre-close interrupt must target DTLS, whose connection is the SCTP read
// boundary. ICE Stop closes and joins its mux/agent readers; the production
// failure left teardown parked in that join before PeerConnection.Close could
// release SCTP. A pristine PeerConnection makes the selected layer observable:
// stopping DTLS changes only DTLS state, while the old ICE callback closed ICE.
func TestWebRtcPeerConnectionPrecloseStopsDtlsWithoutJoiningIce(t *testing.T) {
	pc, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()

	sctpTransport := pc.SCTP()
	if sctpTransport == nil {
		t.Fatal("PeerConnection has no SCTP transport")
	}
	dtlsTransport := sctpTransport.Transport()
	if dtlsTransport == nil {
		t.Fatal("SCTP transport has no DTLS transport")
	}
	iceTransport := dtlsTransport.ICETransport()
	if iceTransport == nil {
		t.Fatal("DTLS transport has no ICE transport")
	}

	stopTransport := webRtcPeerConnectionTransportStop(pc)
	if stopTransport == nil {
		t.Fatal("native PeerConnection has no pre-close transport stop")
	}
	if err := stopTransport(); err != nil {
		t.Fatal(err)
	}
	if got := dtlsTransport.State(); got != webrtc.DTLSTransportStateClosed {
		t.Fatalf("DTLS state after pre-close = %s, want closed", got)
	}
	if got := iceTransport.State(); got == webrtc.ICETransportStateClosed {
		t.Fatal("pre-close joined ICE instead of interrupting the SCTP-facing DTLS transport")
	}
}

func TestWebRtcPeerTeardownStillClosesPeerAfterTransportStopError(t *testing.T) {
	stopError := errors.New("transport stop failure")
	peerClosed := false
	err := closeTransportBeforePeerConnection(
		func() error {
			return stopError
		},
		func() error {
			peerClosed = true
			return nil
		},
	)
	if !errors.Is(err, stopError) {
		t.Fatalf("teardown error = %v, want transport stop error", err)
	}
	if !peerClosed {
		t.Fatal("transport stop error skipped peer connection cleanup")
	}
}

func TestWebRtcPeerTeardownWatchdogReportsCurrentStage(t *testing.T) {
	var stage atomic.Int32
	stage.Store(int32(peerConnectionTeardownClosingPeer))
	stalled := make(chan peerConnectionTeardownStage, 1)
	timer := startPeerConnectionTeardownWatchdog(
		time.Millisecond,
		&stage,
		func(current peerConnectionTeardownStage) {
			stalled <- current
		},
	)
	defer timer.Stop()

	select {
	case current := <-stalled:
		if current != peerConnectionTeardownClosingPeer {
			t.Fatalf("reported stage = %s, want closing-peer", current)
		}
	case <-time.After(time.Second):
		t.Fatal("teardown watchdog did not report a stalled stage")
	}
}

func TestWebRtcPeerTeardownWatchdogStopPreventsReport(t *testing.T) {
	var stage atomic.Int32
	stage.Store(int32(peerConnectionTeardownStarting))
	stalled := make(chan peerConnectionTeardownStage, 1)
	timer := startPeerConnectionTeardownWatchdog(
		25*time.Millisecond,
		&stage,
		func(current peerConnectionTeardownStage) {
			stalled <- current
		},
	)
	if !timer.Stop() {
		t.Fatal("teardown watchdog fired before immediate stop")
	}

	select {
	case current := <-stalled:
		t.Fatalf("stopped teardown watchdog reported %s", current)
	case <-time.After(4 * 25 * time.Millisecond):
	}
}

func testClientShutdownCancelsAllOwnedManagerContexts(t *testing.T, closeMode string) {
	t.Helper()
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	settings.ControlPingTimeout = 0
	settings.WebRtcSettings.IceServerUrls = nil
	client := NewClient(
		context.Background(),
		NewId(),
		NewNoContractClientOob(),
		settings,
	)

	if closeMode == "close" {
		client.Close()
	} else {
		client.Cancel()
	}

	contexts := map[string]context.Context{
		"route":    client.routeManager.ctx,
		"contract": client.contractManager.ctx,
		"webrtc":   client.webRtcManager.ctx,
		"stream":   client.streamManager.ctx,
		"peer":     client.peerManager.ctx,
	}
	for name, managerCtx := range contexts {
		select {
		case <-managerCtx.Done():
		default:
			t.Errorf("%s manager outlived Client.%s", name, closeMode)
		}
	}
}

func TestClientCloseCancelsAllOwnedManagerContexts(t *testing.T) {
	testClientShutdownCancelsAllOwnedManagerContexts(t, "close")
}

func TestClientCancelCancelsAllOwnedManagerContexts(t *testing.T) {
	testClientShutdownCancelsAllOwnedManagerContexts(t, "cancel")
}

func TestWebRtcConnectedCallbackDropsLateAndPostUnsubscribeDelivery(t *testing.T) {
	var lock sync.Mutex
	var states []bool
	callback := &connectedCallback{
		callback: func(connected bool) {
			lock.Lock()
			states = append(states, connected)
			lock.Unlock()
		},
	}

	callback.deliver(2, false)
	callback.deliver(1, true)
	callback.deliver(2, false)
	callback.close()
	callback.deliver(3, true)

	lock.Lock()
	defer lock.Unlock()
	AssertEqual(t, states, []bool{false})
}

func TestWebRtcConnectedCallbackUnsubscribeDoesNotWaitOnBlockedCallback(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	finished := make(chan struct{})
	var callsLock sync.Mutex
	calls := 0
	callback := &connectedCallback{
		callback: func(bool) {
			callsLock.Lock()
			calls++
			callsLock.Unlock()
			close(started)
			<-release
			close(finished)
		},
	}
	go callback.deliver(1, true)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("connected callback did not start")
	}

	closed := make(chan struct{})
	go func() {
		callback.close()
		close(closed)
	}()
	select {
	case <-closed:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("unsubscribe waited on a blocked connected callback")
	}

	close(release)
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("connected callback did not finish")
	}
	callback.deliver(2, false)
	callsLock.Lock()
	defer callsLock.Unlock()
	if calls != 1 {
		t.Fatalf("post-unsubscribe generation invoked callback; calls=%d", calls)
	}
}

func TestWebRtcConnectedCallbackCanUnsubscribeItself(t *testing.T) {
	done := make(chan struct{})
	var callback *connectedCallback
	callback = &connectedCallback{
		callback: func(bool) {
			callback.close()
			close(done)
		},
	}
	go callback.deliver(1, true)
	select {
	case <-done:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("connected callback deadlocked while unsubscribing itself")
	}
	callback.deliver(2, false)
}

func newTerminalStateTestPeerConn() (*peerConn, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	return &peerConn{
		ctx:                ctx,
		cancel:             cancel,
		log:                NewNoopLogger(),
		connectedMonitor:   NewMonitor(),
		immediateReconnect: make(chan struct{}),
	}, cancel
}

func TestWebRtcIceFailureReleasesPeerInsteadOfStrandingSlot(t *testing.T) {
	conn, cancel := newTerminalStateTestPeerConn()
	defer cancel()
	conn.handleICEConnectionState(webrtc.ICEConnectionStateFailed)
	select {
	case <-conn.ctx.Done():
	default:
		t.Fatal("terminal ICE state did not cancel the peer")
	}
	select {
	case <-conn.ImmediateReconnect():
		t.Fatal("generic ICE failure bypassed reconnect backoff")
	default:
	}
}

func TestWebRtcPeerFailureReleasesPeerInsteadOfStrandingSlot(t *testing.T) {
	conn, cancel := newTerminalStateTestPeerConn()
	defer cancel()
	conn.handlePeerConnectionState(webrtc.PeerConnectionStateFailed)
	select {
	case <-conn.ctx.Done():
	default:
		t.Fatal("terminal peer state did not cancel the peer")
	}
	select {
	case <-conn.ImmediateReconnect():
		t.Fatal("generic peer failure bypassed reconnect backoff")
	default:
	}
}

func TestWebRtcLocalCloseDoesNotBypassReconnectBackoff(t *testing.T) {
	conn, cancel := newTerminalStateTestPeerConn()
	cancel()
	conn.handlePeerConnectionState(webrtc.PeerConnectionStateClosed)
	select {
	case <-conn.ImmediateReconnect():
		t.Fatal("local close incorrectly bypassed reconnect backoff")
	default:
	}
}

func newAnsweredActiveTestPeerConn(remoteGeneration Id, remoteGenerationSet bool) (*peerConn, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	return &peerConn{
		ctx:                       ctx,
		cancel:                    cancel,
		cancelCause:               func(err error) { cancel() },
		log:                       NewNoopLogger(),
		active:                    true,
		answer:                    &protocol.ExchangeSignal{SignalType: protocol.SignalType_SdpAnswer},
		remoteSignalGeneration:    remoteGeneration,
		remoteSignalGenerationSet: remoteGenerationSet,
		immediateReconnect:        make(chan struct{}),
	}, cancel
}

func TestWebRtcLateSameGenerationWaitingForOfferIsIgnored(t *testing.T) {
	waiting := &protocol.ExchangeSignal{SignalType: protocol.SignalType_WaitingForSdpOffer}
	answeredGeneration := NewId()
	conn, cancel := newAnsweredActiveTestPeerConn(answeredGeneration, true)
	defer cancel()
	AssertEqual(t, conn.receiveSignalFromPeer(waiting, answeredGeneration, true), nil)
	select {
	case <-conn.ctx.Done():
		t.Fatalf("same-generation waiting canceled a healthy negotiation: %v", context.Cause(conn.ctx))
	default:
	}
}

func TestWebRtcNewWaitingForOfferGenerationRequestsReplacement(t *testing.T) {
	waiting := &protocol.ExchangeSignal{SignalType: protocol.SignalType_WaitingForSdpOffer}
	conn, cancel := newAnsweredActiveTestPeerConn(NewId(), true)
	defer cancel()
	AssertEqual(t, conn.receiveSignalFromPeer(waiting, NewId(), true), nil)
	select {
	case <-conn.ctx.Done():
	default:
		t.Fatal("new passive generation did not request active replacement")
	}
	select {
	case <-conn.ImmediateReconnect():
	default:
		t.Fatal("new passive generation did not bypass reconnect backoff")
	}
}

func TestWebRtcLegacyWaitingForOfferRetainsReplacement(t *testing.T) {
	waiting := &protocol.ExchangeSignal{SignalType: protocol.SignalType_WaitingForSdpOffer}
	conn, cancel := newAnsweredActiveTestPeerConn(Id{}, false)
	defer cancel()
	AssertEqual(t, conn.ReceiveSignalFromPeer(waiting), nil)
	select {
	case <-conn.ctx.Done():
	default:
		t.Fatal("legacy waiting did not retain replacement behavior")
	}
}

func TestWebRtcResetOfferUsesGenerationInsteadOfRetransmitArrival(t *testing.T) {
	acceptedGeneration := NewId()
	newNegotiatedPassive := func() *peerConn {
		return &peerConn{
			active:                    false,
			offer:                     &protocol.ExchangeSignal{SignalType: protocol.SignalType_SdpOffer},
			remoteSignalGeneration:    acceptedGeneration,
			remoteSignalGenerationSet: true,
		}
	}

	if replace := newNegotiatedPassive().resetRemoteSignals(acceptedGeneration, true); replace {
		t.Fatal("same-generation reset retransmit replaced a negotiated passive connection")
	}
	if replace := newNegotiatedPassive().resetRemoteSignals(NewId(), true); !replace {
		t.Fatal("new active generation did not replace a negotiated passive connection")
	}
	if replace := newNegotiatedPassive().resetRemoteSignals(Id{}, false); !replace {
		t.Fatal("legacy reset did not retain replacement compatibility")
	}
}

func TestWebRtcResetWithoutOfferCannotTearDownPeer(t *testing.T) {
	peerId := NewId()
	streamId := NewId()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &peerConn{
		ctx:                ctx,
		cancel:             cancel,
		log:                NewNoopLogger(),
		immediateReconnect: make(chan struct{}),
	}
	manager := &WebRtcManager{
		log:       NewNoopLogger(),
		peerConns: map[peerConnKey]*peerConn{{PeerId: peerId, StreamId: streamId}: conn},
	}
	err := manager.ReceiveExchangeSignals(
		SourceId(peerId),
		TransferKey{},
		&protocol.ExchangeSignals{
			StreamId:           streamId.Bytes(),
			ResetSignals:       true,
			SenderGenerationId: NewId().Bytes(),
			Signals: []*protocol.ExchangeSignal{
				{SignalType: protocol.SignalType_WaitingForSdpOffer},
			},
		},
	)
	if err == nil {
		t.Fatal("reset_signals without an SDP offer was accepted")
	}
	select {
	case <-ctx.Done():
		t.Fatal("malformed reset tore down the existing peer")
	default:
	}
}

func TestWebRtcManagerNetworkChangeRetiresConnectionsAndFactory(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.MaxPeerConnectionCount = 0
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	conn, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	AssertEqual(t, err, nil)
	oldFactory := func() *webRtcPeerConnectionFactory {
		manager.peerConnectionFactoryLock.Lock()
		defer manager.peerConnectionFactoryLock.Unlock()
		return manager.peerConnectionFactory
	}()
	AssertNotEqual(t, oldFactory, nil)
	manager.peerConnectionFactoryLock.Lock()
	oldCertificate := manager.peerConnectionCertificate
	manager.peerConnectionFactoryLock.Unlock()
	AssertNotEqual(t, oldCertificate, nil)

	reconnect := conn.ImmediateReconnect()
	manager.networkChanged()
	select {
	case <-reconnect:
	default:
		t.Fatal("network change did not persist an immediate-reconnect signal")
	}
	select {
	case <-conn.(*peerConn).ctx.Done():
	default:
		t.Fatal("network change did not retire the old peer connection")
	}

	manager.peerConnectionFactoryLock.Lock()
	AssertEqual(t, manager.peerConnectionFactoryInitialized, false)
	AssertEqual(t, manager.peerConnectionFactory, nil)
	manager.peerConnectionFactoryLock.Unlock()

	replacement, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	AssertEqual(t, err, nil)
	defer replacement.Close()
	newFactory := func() *webRtcPeerConnectionFactory {
		manager.peerConnectionFactoryLock.Lock()
		defer manager.peerConnectionFactoryLock.Unlock()
		return manager.peerConnectionFactory
	}()
	AssertNotEqual(t, newFactory, nil)
	if newFactory == oldFactory {
		t.Fatal("network change reused ICE state bound to the old interfaces")
	}
	manager.peerConnectionFactoryLock.Lock()
	newCertificate := manager.peerConnectionCertificate
	manager.peerConnectionFactoryLock.Unlock()
	if newCertificate != oldCertificate {
		t.Fatal("network change regenerated the manager-scoped DTLS certificate")
	}
}

func TestWebRtcNetworkChangeDispatchDoesNotBlockHostCallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	fastPathPublished := make(chan struct{})
	releaseFastPath := make(chan struct{})
	var releaseFastPathOnce sync.Once
	releaseFastPathConfiguration := func() {
		releaseFastPathOnce.Do(func() {
			close(releaseFastPath)
		})
	}
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.afterFastPathPublishForTest = func() {
		close(fastPathPublished)
		<-releaseFastPath
	}
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)
	defer releaseFastPathConfiguration()
	conn, err := manager.NewP2pConnActive(
		ctx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	AssertEqual(t, err, nil)
	defer conn.Close()
	peer := conn.(*peerConn)
	select {
	case <-fastPathPublished:
	case <-ctx.Done():
		t.Fatal("peer setup did not publish the native fast path")
	}

	// Simulate teardown already holding manager state. The OS path callback
	// must enqueue/coalesce and return instead of blocking its UI/extension
	// thread behind that work.
	workerEntered := make(chan struct{})
	var workerEnteredOnce sync.Once
	manager.beforeNetworkChangeStateLockForTest = func() {
		workerEnteredOnce.Do(func() {
			close(workerEntered)
		})
	}
	manager.stateLock.Lock()
	stateLocked := true
	defer func() {
		if stateLocked {
			manager.stateLock.Unlock()
		}
	}()
	dispatched := make(chan struct{})
	go func() {
		for range 32 {
			manager.networkChangeWorker.Dispatch()
		}
		close(dispatched)
	}()
	select {
	case <-workerEntered:
	case <-ctx.Done():
		t.Fatal("network-change worker did not reach the manager state barrier")
	}
	select {
	case <-dispatched:
	case <-ctx.Done():
		t.Fatal("network-change dispatch did not return while its worker waited for manager state")
	}
	manager.stateLock.Unlock()
	stateLocked = false

	select {
	case <-conn.ImmediateReconnect():
	case <-ctx.Done():
		t.Fatal("coalesced network-change worker did not retire the connection")
	}
	select {
	case <-peer.teardownDone:
	case <-ctx.Done():
		t.Fatal("network-change teardown did not retire the startup fast path")
	}
	if peer.fastPath.Load() != nil {
		t.Fatal("network-change teardown retained the published startup fast path")
	}
	releaseFastPathConfiguration()
}

func TestWebRtcInvalidSdpAndEarlyCandidateDoNotPoisonRetransmit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	settings.UseLoopbackOnlyIceInterfaces = true
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	streamId := NewId()
	passiveWebRtcConn, err := manager.NewP2pConnPassive(
		ctx,
		NewTransferPath(NewId(), NewId(), streamId),
	)
	AssertEqual(t, err, nil)
	passive := passiveWebRtcConn.(*peerConn)

	invalidSdp, err := json.Marshal(&webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  "not valid SDP",
	})
	AssertEqual(t, err, nil)
	if err := passive.ReceiveSignalFromPeer(&protocol.ExchangeSignal{
		SignalType: protocol.SignalType_SdpOffer,
		Sdp:        invalidSdp,
	}); err == nil {
		t.Fatal("invalid semantic SDP was accepted")
	}
	AssertEqual(t, passive.offerSignal(), nil)

	earlyCandidate, err := json.Marshal(webrtc.ICECandidateInit{
		Candidate: "candidate:1 1 udp 1 192.0.2.1 9999 typ host",
	})
	AssertEqual(t, err, nil)
	AssertEqual(t, passive.ReceiveSignalFromPeer(&protocol.ExchangeSignal{
		SignalType:   protocol.SignalType_IceCandidate,
		IceCandidate: earlyCandidate,
	}), nil)
	passive.signalLock.Lock()
	bufferedCandidateCount := len(passive.remoteIceCandidateBuffer)
	passive.signalLock.Unlock()
	AssertEqual(t, bufferedCandidateCount, 1)

	offerPeer, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	AssertEqual(t, err, nil)
	defer offerPeer.Close()
	if _, err := offerPeer.CreateDataChannel("retransmit", nil); err != nil {
		t.Fatal(err)
	}
	validOffer, err := offerPeer.CreateOffer(nil)
	AssertEqual(t, err, nil)
	AssertEqual(t, offerPeer.SetLocalDescription(validOffer), nil)
	validOfferBytes, err := json.Marshal(&validOffer)
	AssertEqual(t, err, nil)
	validOfferSignal := &protocol.ExchangeSignal{
		SignalType: protocol.SignalType_SdpOffer,
		Sdp:        validOfferBytes,
	}
	AssertEqual(t, passive.ReceiveSignalFromPeer(validOfferSignal), nil)
	if passive.offerSignal() != validOfferSignal || passive.answerSignal() == nil {
		t.Fatal("valid retransmit did not establish offer/answer state")
	}
	passive.signalLock.Lock()
	remoteDescriptionSet := passive.remoteDescriptionSet
	remoteIceCandidateCount := len(passive.remoteIceCandidateBuffer)
	remoteIceCandidateBytes := passive.remoteIceCandidateBufferBytes
	passive.signalLock.Unlock()
	AssertEqual(t, remoteDescriptionSet, true)
	AssertEqual(t, remoteIceCandidateCount, 0)
	AssertEqual(t, remoteIceCandidateBytes, 0)
}

func TestWebRtcEarlyCandidateBufferIsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	settings.IceServerUrls = nil
	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)
	webRtcConn, err := manager.NewP2pConnPassive(
		ctx,
		NewTransferPath(NewId(), NewId(), NewId()),
	)
	AssertEqual(t, err, nil)
	defer webRtcConn.Close()
	conn := webRtcConn.(*peerConn)

	for i := range 4 * maxBufferedRemoteIceCandidateCount {
		candidateBytes, marshalErr := json.Marshal(webrtc.ICECandidateInit{
			Candidate: fmt.Sprintf(
				"candidate:%d 1 udp 1 192.0.2.1 %d typ host %s",
				i+1,
				10000+i,
				strings.Repeat("x", 1024),
			),
		})
		AssertEqual(t, marshalErr, nil)
		AssertEqual(t, conn.ReceiveSignalFromPeer(&protocol.ExchangeSignal{
			SignalType:   protocol.SignalType_IceCandidate,
			IceCandidate: candidateBytes,
		}), nil)
	}
	if maxBufferedRemoteIceCandidateCount < len(conn.remoteIceCandidateBuffer) {
		t.Fatalf("candidate count was unbounded: %d", len(conn.remoteIceCandidateBuffer))
	}
	if maxBufferedRemoteIceCandidateBytes < conn.remoteIceCandidateBufferBytes {
		t.Fatalf("candidate bytes were unbounded: %d", conn.remoteIceCandidateBufferBytes)
	}
}

func TestWebRtcMalformedCandidateDoesNotSuppressBatchRemainder(t *testing.T) {
	peerId := NewId()
	streamId := NewId()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &peerConn{
		ctx:                ctx,
		cancel:             cancel,
		log:                NewNoopLogger(),
		immediateReconnect: make(chan struct{}),
	}
	key := peerConnKey{PeerId: peerId, StreamId: streamId}
	manager := &WebRtcManager{
		log:       NewNoopLogger(),
		peerConns: map[peerConnKey]*peerConn{key: conn},
	}
	validCandidate, err := json.Marshal(webrtc.ICECandidateInit{
		Candidate: "candidate:1 1 udp 1 192.0.2.1 9999 typ host",
	})
	AssertEqual(t, err, nil)
	err = manager.ReceiveExchangeSignals(
		SourceId(peerId),
		TransferKey{},
		&protocol.ExchangeSignals{
			StreamId: streamId.Bytes(),
			Signals: []*protocol.ExchangeSignal{
				{
					SignalType:   protocol.SignalType_IceCandidate,
					IceCandidate: []byte("{"),
				},
				{
					SignalType:   protocol.SignalType_IceCandidate,
					IceCandidate: validCandidate,
				},
			},
		},
	)
	if err == nil {
		t.Fatal("malformed candidate error was not reported")
	}
	AssertEqual(t, len(conn.remoteIceCandidateBuffer), 1)
}

func TestWebRtcDropsCandidateFromRetiredSignalGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	currentGeneration := NewId()
	conn := &peerConn{
		ctx:                       ctx,
		cancel:                    cancel,
		log:                       NewNoopLogger(),
		remoteDescriptionSet:      true,
		remoteSignalGeneration:    currentGeneration,
		remoteSignalGenerationSet: true,
	}
	candidateBytes, err := json.Marshal(webrtc.ICECandidateInit{
		Candidate: "candidate:1 1 udp 1 192.0.2.1 9999 typ host",
	})
	AssertEqual(t, err, nil)
	// pc is intentionally nil. A stale candidate must be rejected before
	// AddICECandidate can touch the current Pion generation.
	AssertEqual(t, conn.receiveSignalFromPeer(
		&protocol.ExchangeSignal{
			SignalType:   protocol.SignalType_IceCandidate,
			IceCandidate: candidateBytes,
		},
		NewId(),
		true,
	), nil)
}

type recordingSignalSender struct {
	lock    sync.Mutex
	batches []*protocol.ExchangeSignals
}

// SendSignal decodes and records one owned signaling frame.
func (self *recordingSignalSender) SendSignal(
	_ Id,
	frame *protocol.Frame,
	_ ...any,
) {
	defer MessagePoolReturn(frame.MessageBytes)
	exchangeSignals := &protocol.ExchangeSignals{}
	if err := ProtoUnmarshal(frame.MessageBytes, exchangeSignals); err != nil {
		panic(err)
	}
	self.lock.Lock()
	self.batches = append(self.batches, exchangeSignals)
	self.lock.Unlock()
}

func TestWebRtcSendsGatheredCandidatesInOneFrame(t *testing.T) {
	sender := &recordingSignalSender{}
	signalGeneration := NewId()
	conn := &peerConn{
		key: peerConnKey{
			PeerId:   NewId(),
			StreamId: NewId(),
		},
		sourceId:         NewId(),
		active:           true,
		signalSender:     sender,
		signalGeneration: signalGeneration,
	}
	candidates := make([]*webrtc.ICECandidate, 0, 2)
	for i := range 2 {
		candidates = append(candidates, &webrtc.ICECandidate{
			Foundation: fmt.Sprintf("%d", i+1),
			Priority:   1,
			Address:    fmt.Sprintf("192.0.2.%d", i+1),
			Protocol:   webrtc.ICEProtocolUDP,
			Port:       uint16(10000 + i),
			Typ:        webrtc.ICECandidateTypeHost,
			Component:  1,
		})
	}
	conn.sendIceCandidates(candidates)

	sender.lock.Lock()
	defer sender.lock.Unlock()
	AssertEqual(t, len(sender.batches), 1)
	AssertEqual(t, len(sender.batches[0].Signals), 2)
	AssertEqual(t, sender.batches[0].SenderGenerationId, signalGeneration.Bytes())
}

func TestWebRtcCandidateSendFramesRemainBounded(t *testing.T) {
	sender := &recordingSignalSender{}
	conn := &peerConn{
		key: peerConnKey{
			PeerId:   NewId(),
			StreamId: NewId(),
		},
		sourceId:     NewId(),
		active:       true,
		signalSender: sender,
	}
	candidates := make([]*webrtc.ICECandidate, 0, 2*maxIceCandidatesPerSignalFrame+1)
	for i := range 2*maxIceCandidatesPerSignalFrame + 1 {
		candidates = append(candidates, &webrtc.ICECandidate{
			Foundation: fmt.Sprintf("%d", i+1),
			Priority:   1,
			Address:    fmt.Sprintf("192.0.2.%d", i%254+1),
			Protocol:   webrtc.ICEProtocolUDP,
			Port:       uint16(10000 + i),
			Typ:        webrtc.ICECandidateTypeHost,
			Component:  1,
		})
	}
	conn.sendIceCandidates(candidates)

	sender.lock.Lock()
	defer sender.lock.Unlock()
	AssertEqual(t, len(sender.batches), 3)
	AssertEqual(t, len(sender.batches[0].Signals), maxIceCandidatesPerSignalFrame)
	AssertEqual(t, len(sender.batches[1].Signals), maxIceCandidatesPerSignalFrame)
	AssertEqual(t, len(sender.batches[2].Signals), 1)
}

func TestWebRtcEgressOnlyInterfaceViewIsBounded(t *testing.T) {
	iceNet, ok := newIceInterfaceNet(NewNoopLogger(), true)
	if !ok {
		t.Skip("host has no default-route IPv4 or IPv6 address")
	}
	interfaces, err := iceNet.Interfaces()
	AssertEqual(t, err, nil)
	if len(interfaces) == 0 || 2 < len(interfaces) {
		t.Fatalf("egress-only interface count is not bounded: %d", len(interfaces))
	}
	nativeInterfaces, nativeErr := net.Interfaces()
	nativeIdentity := map[string]bool{}
	if nativeErr == nil {
		for _, ifc := range nativeInterfaces {
			nativeIdentity[fmt.Sprintf("%d/%s", ifc.Index, ifc.Name)] = true
		}
	}
	totalAddresses := 0
	seenFamilies := map[int]bool{}
	for _, ifc := range interfaces {
		if nativeErr == nil && !nativeIdentity[fmt.Sprintf("%d/%s", ifc.Index, ifc.Name)] {
			t.Fatalf("egress-only interface has synthetic identity despite native enumeration: %d/%s", ifc.Index, ifc.Name)
		}
		addrs, addrErr := ifc.Addrs()
		AssertEqual(t, addrErr, nil)
		for _, addr := range addrs {
			ip, _, parseErr := net.ParseCIDR(addr.String())
			AssertEqual(t, parseErr, nil)
			family := 6
			if ip.To4() != nil {
				family = 4
			}
			if seenFamilies[family] {
				t.Fatalf("egress-only interface view duplicated IPv%d: %v", family, interfaces)
			}
			seenFamilies[family] = true
			totalAddresses++
		}
	}
	if totalAddresses == 0 || 2 < totalAddresses {
		t.Fatalf("egress-only address count is not bounded: %d", totalAddresses)
	}
}

// newTestingWaitingSignalFrame creates one valid owned signaling frame for an
// exact stream without constructing a real Pion association.
func newTestingWaitingSignalFrame(streamId Id) *protocol.Frame {
	return RequireToFrameWithDefaultProtocolVersion(&protocol.ExchangeSignals{
		StreamId: streamId.Bytes(),
		Signals: []*protocol.ExchangeSignal{{
			SignalType: protocol.SignalType_WaitingForSdpOffer,
		}},
	})
}

// A passive peer may announce that it is waiting before the active peer has
// registered its stream. The synchronous in-memory carrier must model the
// production receiver's ordinary drop instead of panicking its sender.
func TestSignalPipeDropsBeforeDestinationRegistration(t *testing.T) {
	receiver := &WebRtcManager{}
	destinationId := NewId()
	streamId := NewId()

	dropCount := 0
	direct := newSignalPipe(receiver)
	direct.afterMissingDestinationDropForTest = func() {
		dropCount++
	}
	direct.SendSignal(destinationId, newTestingWaitingSignalFrame(streamId))
	if dropCount != 1 {
		t.Fatalf("direct pre-registration drop count = %d, want 1", dropCount)
	}
}

// delayedSignalRecordingReceiver exposes its manager for test-path source
// reconstruction while recording rather than applying a delivered signal.
type delayedSignalRecordingReceiver struct {
	manager  *WebRtcManager
	received chan TransferPath
}

// testingSignalReceiver exposes the exact registration map used at dispatch.
func (self *delayedSignalRecordingReceiver) testingSignalReceiver() SignalReceiver {
	return self.manager
}

// ReceiveSignal records one borrowed delivery without retaining its frame.
func (self *delayedSignalRecordingReceiver) ReceiveSignal(
	source TransferPath,
	_ TransferKey,
	_ *protocol.Frame,
) error {
	self.received <- source
	return nil
}

// Registration after enqueue but before dispatch must make the delayed signal
// deliverable, matching a real receiver's lookup time rather than send time.
func TestDelayedSignalPipeResolvesDestinationAtDispatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	manager := &WebRtcManager{peerConns: map[peerConnKey]*peerConn{}}
	receiver := &delayedSignalRecordingReceiver{
		manager:  manager,
		received: make(chan TransferPath, 1),
	}
	pipe := newDelayedSignalPipe(ctx, 0, receiver)
	dispatchEntered := make(chan struct{})
	releaseDispatch := make(chan struct{})
	var dispatchOnce sync.Once
	var releaseOnce sync.Once
	pipe.beforeDispatchForTest = func() {
		dispatchOnce.Do(func() { close(dispatchEntered) })
		<-releaseDispatch
	}
	defer func() {
		releaseOnce.Do(func() { close(releaseDispatch) })
		cancel()
		<-pipe.done
	}()

	streamId := NewId()
	destinationId := NewId()
	sourceId := NewId()
	pipe.SendSignal(destinationId, newTestingWaitingSignalFrame(streamId))
	select {
	case <-ctx.Done():
		t.Fatal("delayed signal did not reach the dispatch barrier")
	case <-dispatchEntered:
	}
	manager.stateLock.Lock()
	manager.peerConns[peerConnKey{PeerId: sourceId, StreamId: streamId}] = &peerConn{
		sourceId: destinationId,
	}
	manager.stateLock.Unlock()
	releaseOnce.Do(func() { close(releaseDispatch) })

	select {
	case <-ctx.Done():
		t.Fatal("registered delayed signal was not delivered")
	case source := <-receiver.received:
		if source.SourceId != sourceId || source.StreamId != streamId {
			t.Fatalf("delayed source = %s, want %s/%s", source, sourceId, streamId)
		}
	}
}

// An association still absent at dispatch is dropped deterministically and is
// never delivered later to a generation that happens to reuse the stream.
func TestDelayedSignalPipeDropsMissingDestinationAtDispatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	manager := &WebRtcManager{}
	receiver := &delayedSignalRecordingReceiver{
		manager:  manager,
		received: make(chan TransferPath, 1),
	}
	pipe := newDelayedSignalPipe(ctx, 0, receiver)
	dropped := make(chan struct{})
	var dropOnce sync.Once
	pipe.afterMissingDestinationDropForTest = func() {
		dropOnce.Do(func() { close(dropped) })
	}
	defer func() {
		cancel()
		<-pipe.done
	}()

	pipe.SendSignal(NewId(), newTestingWaitingSignalFrame(NewId()))
	select {
	case <-ctx.Done():
		t.Fatal("missing delayed destination was not resolved")
	case <-dropped:
	}
	select {
	case source := <-receiver.received:
		t.Fatalf("missing delayed destination delivered from %s", source)
	default:
	}
}

// Cancellation returns both the dequeued frame and every queued frame before
// lifecycle completion, so the bounded delay helper cannot leak pooled roots.
func TestDelayedSignalPipeCancellationReturnsOwnedFrames(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pipe := newDelayedSignalPipe(ctx, time.Hour, nil)
	dequeued := make(chan struct{})
	releaseDequeue := make(chan struct{})
	var dequeueOnce sync.Once
	var releaseOnce sync.Once
	pipe.afterDequeueForTest = func() {
		dequeueOnce.Do(func() { close(dequeued) })
		<-releaseDequeue
	}
	defer func() {
		releaseOnce.Do(func() { close(releaseDequeue) })
		cancel()
		<-pipe.done
	}()

	ownedFrames := make([]*lifecyclePoolCapture, 0, 3)
	defer func() {
		for _, ownedFrame := range ownedFrames {
			ownedFrame.cleanup()
		}
	}()
	pipe.afterEnqueueForTest = func(frame *protocol.Frame) {
		ownedFrames = append(ownedFrames, newLifecyclePoolCapture(frame.MessageBytes))
	}
	newFrame := func(value byte) *protocol.Frame {
		return &protocol.Frame{
			MessageType:  protocol.MessageType_TransferExchangeSignals,
			MessageBytes: MessagePoolCopy([]byte{value}),
		}
	}
	pipe.SendSignal(NewId(), newFrame(1))
	select {
	case <-dequeued:
	case <-time.After(time.Second):
		t.Fatal("delayed pipe did not dequeue the first owned frame")
	}
	pipe.SendSignal(NewId(), newFrame(2))
	pipe.SendSignal(NewId(), newFrame(3))
	if len(ownedFrames) != 3 {
		t.Fatalf("owned delayed frames = %d, want 3", len(ownedFrames))
	}
	for frameIndex, frame := range ownedFrames {
		frame.requireOwnerLive(t, fmt.Sprintf("owned delayed frame %d", frameIndex))
	}
	if cap(pipe.queue) != delayedSignalQueueSize {
		t.Fatalf("delayed signal queue capacity = %d, want %d", cap(pipe.queue), delayedSignalQueueSize)
	}

	cancel()
	releaseOnce.Do(func() { close(releaseDequeue) })
	<-pipe.done
	for frameIndex, frame := range ownedFrames {
		frame.requireOwnerReturned(t, fmt.Sprintf("owned delayed frame %d", frameIndex))
	}
}

// A sender waiting behind a full queue must not hold the receiver lock needed
// by the current dispatch, or neither side can make the queue slot available.
func TestDelayedSignalPipeFullQueueDoesNotBlockDispatch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	manager := &WebRtcManager{peerConns: map[peerConnKey]*peerConn{}}
	receiver := &delayedSignalRecordingReceiver{
		manager:  manager,
		received: make(chan TransferPath, delayedSignalQueueSize+1),
	}
	pipe := newDelayedSignalPipe(ctx, 0, receiver)
	dispatchEntered := make(chan struct{})
	releaseDispatch := make(chan struct{})
	var dispatchOnce sync.Once
	var releaseOnce sync.Once
	pipe.beforeDispatchForTest = func() {
		dispatchOnce.Do(func() { close(dispatchEntered) })
		<-releaseDispatch
	}
	defer func() {
		releaseOnce.Do(func() { close(releaseDispatch) })
		cancel()
		<-pipe.done
	}()

	streamId := NewId()
	destinationId := NewId()
	sourceId := NewId()
	manager.peerConns[peerConnKey{PeerId: sourceId, StreamId: streamId}] = &peerConn{
		sourceId: destinationId,
	}
	pipe.SendSignal(destinationId, newTestingWaitingSignalFrame(streamId))
	select {
	case <-ctx.Done():
		t.Fatal("delayed signal did not reach the dispatch barrier")
	case <-dispatchEntered:
	}
	for range delayedSignalQueueSize {
		pipe.SendSignal(destinationId, newTestingWaitingSignalFrame(streamId))
	}
	if len(pipe.queue) != delayedSignalQueueSize {
		t.Fatalf(
			"delayed signal queue length = %d, want %d",
			len(pipe.queue),
			delayedSignalQueueSize,
		)
	}

	blockedEnqueueEntered := make(chan struct{})
	var enqueueOnce sync.Once
	pipe.beforeAdmissionForTest = func() {
		enqueueOnce.Do(func() { close(blockedEnqueueEntered) })
	}
	extraSendDone := make(chan struct{})
	go func() {
		pipe.SendSignal(destinationId, newTestingWaitingSignalFrame(streamId))
		close(extraSendDone)
	}()
	select {
	case <-ctx.Done():
		t.Fatal("capacity sender did not reach the full queue")
	case <-blockedEnqueueEntered:
	}
	if len(pipe.admitted) != delayedSignalOwnedFrameLimit {
		t.Fatalf(
			"admitted delayed frames = %d, want hard limit %d",
			len(pipe.admitted),
			delayedSignalOwnedFrameLimit,
		)
	}
	releaseOnce.Do(func() { close(releaseDispatch) })
	select {
	case <-ctx.Done():
		t.Fatal("full queue blocked the current dispatch")
	case source := <-receiver.received:
		if source.SourceId != sourceId || source.StreamId != streamId {
			t.Fatalf("capacity dispatch source = %s, want %s/%s", source, sourceId, streamId)
		}
	}
	select {
	case <-ctx.Done():
		t.Fatal("capacity sender did not resume after dispatch freed a queue slot")
	case <-extraSendDone:
	}
}

// Cancellation at the hard capacity limit must unblock an additional sender,
// drain every accepted frame, and return the waiting sender's owned input.
func TestDelayedSignalPipeCancellationUnblocksCapacitySender(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	pipe := newDelayedSignalPipe(ctx, time.Hour, nil)
	dequeued := make(chan struct{})
	releaseDequeue := make(chan struct{})
	var dequeueOnce sync.Once
	var releaseOnce sync.Once
	pipe.afterDequeueForTest = func() {
		dequeueOnce.Do(func() { close(dequeued) })
		<-releaseDequeue
	}
	extraSendDone := make(chan struct{})
	extraSendStarted := false
	defer func() {
		cancel()
		releaseOnce.Do(func() { close(releaseDequeue) })
		if extraSendStarted {
			<-extraSendDone
		}
		<-pipe.done
	}()

	pipe.SendSignal(NewId(), newTestingWaitingSignalFrame(NewId()))
	select {
	case <-ctx.Done():
		t.Fatal("delayed signal did not reach the dequeue barrier")
	case <-dequeued:
	}
	for range delayedSignalQueueSize {
		pipe.SendSignal(NewId(), newTestingWaitingSignalFrame(NewId()))
	}
	if len(pipe.admitted) != delayedSignalOwnedFrameLimit {
		t.Fatalf(
			"admitted delayed frames = %d, want hard limit %d",
			len(pipe.admitted),
			delayedSignalOwnedFrameLimit,
		)
	}

	blockedAdmissionEntered := make(chan struct{})
	var admissionOnce sync.Once
	pipe.beforeAdmissionForTest = func() {
		admissionOnce.Do(func() { close(blockedAdmissionEntered) })
	}
	extraFrame := newTestingWaitingSignalFrame(NewId())
	extraFrameCapture := newLifecyclePoolCapture(extraFrame.MessageBytes)
	defer extraFrameCapture.cleanup()
	extraSendStarted = true
	go func() {
		pipe.SendSignal(NewId(), extraFrame)
		close(extraSendDone)
	}()
	select {
	case <-blockedAdmissionEntered:
	case <-time.After(time.Second):
		t.Fatal("capacity sender did not reach admission")
	}
	extraFrameCapture.requireOwnerLive(t, "capacity sender input")
	cancel()
	releaseOnce.Do(func() { close(releaseDequeue) })
	select {
	case <-extraSendDone:
	case <-time.After(time.Second):
		t.Fatal("capacity sender did not return after cancellation")
	}
	<-pipe.done
	extraFrameCapture.requireOwnerReturned(t, "capacity sender input")
	if len(pipe.admitted) != 0 || len(pipe.queue) != 0 {
		t.Fatalf(
			"cancelled delayed pipe retained admitted=%d queued=%d frames",
			len(pipe.admitted),
			len(pipe.queue),
		)
	}
}

type signalPipe struct {
	stateLock                          sync.Mutex
	signalReceiver                     SignalReceiver
	verbose                            bool
	afterMissingDestinationDropForTest func()
}

// testingSignalReceiverWrapper exposes the manager behind a diagnostic receiver.
type testingSignalReceiverWrapper interface {
	testingSignalReceiver() SignalReceiver
}

// testingSignalSource reconstructs the callback source expected by a test
// manager. A signal sent before that stream is registered is an ordinary drop,
// matching the production receiver rather than a sender panic.
func testingSignalSource(
	receiver SignalReceiver,
	destinationId Id,
	frame *protocol.Frame,
) (TransferPath, bool) {
	exchangeSignals := &protocol.ExchangeSignals{}
	if err := ProtoUnmarshal(frame.MessageBytes, exchangeSignals); err != nil {
		panic(err)
	}
	streamId, err := IdFromBytes(exchangeSignals.StreamId)
	if err != nil {
		panic(err)
	}
	for {
		wrapper, ok := receiver.(testingSignalReceiverWrapper)
		if !ok {
			break
		}
		receiver = wrapper.testingSignalReceiver()
	}
	manager, ok := receiver.(*WebRtcManager)
	if !ok {
		panic("in-memory signal receiver is not a WebRtcManager")
	}
	manager.stateLock.Lock()
	defer manager.stateLock.Unlock()
	for key, conn := range manager.peerConns {
		if key.StreamId == streamId && conn.sourceId == destinationId {
			return TransferPath{
				SourceId: key.PeerId,
				StreamId: streamId,
			}, true
		}
	}
	return TransferPath{}, false
}

// testingSignalTransferKey extracts the receiver-visible lane from send options.
func testingSignalTransferKey(opts []any) TransferKey {
	var transferKey TransferKey
	for _, opt := range opts {
		if value, ok := opt.(TransferKey); ok {
			transferKey = value
		}
	}
	return transferKey
}

func newSignalPipe(signalReceiver SignalReceiver) *signalPipe {
	return &signalPipe{
		signalReceiver: signalReceiver,
	}
}

func (self *signalPipe) SetSignalReceiver(signalReceiver SignalReceiver) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.signalReceiver = signalReceiver
}

func (self *signalPipe) SignalReceiver() SignalReceiver {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.signalReceiver
}

// missingDestinationDrop records one deterministic pre-registration drop.
func (self *signalPipe) missingDestinationDrop() {
	self.stateLock.Lock()
	afterDrop := self.afterMissingDestinationDropForTest
	self.stateLock.Unlock()
	if afterDrop != nil {
		afterDrop()
	}
}

// SendSignal synchronously delivers and consumes one owned signaling frame.
func (self *signalPipe) SendSignal(destinationId Id, signal *protocol.Frame, opts ...any) {
	defer MessagePoolReturn(signal.MessageBytes)
	signalReceiver := self.SignalReceiver()
	if signalReceiver != nil {
		source, ok := testingSignalSource(signalReceiver, destinationId, signal)
		if !ok {
			self.missingDestinationDrop()
			if self.verbose {
				fmt.Printf("[signal][%s]drop unregistered ->%s\n", signal.MessageType, destinationId)
			}
			return
		}
		if self.verbose {
			fmt.Printf("[signal][%s]%s->%s\n", signal.MessageType, source, destinationId)
		}
		signalReceiver.ReceiveSignal(source, testingSignalTransferKey(opts), signal)
	} else if self.verbose {
		fmt.Printf("[signal][%s]drop ->%s\n", signal.MessageType, destinationId)
	}
}

type delayedSignalFrame struct {
	destinationId Id
	transferKey   TransferKey
	frame         *protocol.Frame
	due           time.Time
}

// delayedSignalQueueSize bounds frames waiting behind the one being dispatched.
const delayedSignalQueueSize = 256

// delayedSignalOwnedFrameLimit bounds the current dispatch plus queued frames.
const delayedSignalOwnedFrameLimit = delayedSignalQueueSize + 1

// delayedSignalPipe models a propagation delay without serializing a burst:
// each frame is due one delay after its own send time, and adjacent due frames
// are delivered FIFO. This is closer to a signaling link than sleeping once
// per frame, which would incorrectly charge a full RTT for adjacent offer and
// candidate frames.
type delayedSignalPipe struct {
	admissionLock                      sync.Mutex
	stateLock                          sync.Mutex
	senders                            sync.WaitGroup
	ctx                                context.Context
	delay                              time.Duration
	receiver                           SignalReceiver
	queue                              chan delayedSignalFrame
	admitted                           chan struct{}
	done                               chan struct{}
	closed                             bool
	beforeAdmissionForTest             func()
	afterEnqueueForTest                func(*protocol.Frame)
	afterDequeueForTest                func()
	beforeDispatchForTest              func()
	afterMissingDestinationDropForTest func()
}

func newDelayedSignalPipe(
	ctx context.Context,
	delay time.Duration,
	receiver SignalReceiver,
) *delayedSignalPipe {
	pipe := &delayedSignalPipe{
		ctx:      ctx,
		delay:    delay,
		receiver: receiver,
		queue:    make(chan delayedSignalFrame, delayedSignalQueueSize),
		admitted: make(chan struct{}, delayedSignalOwnedFrameLimit),
		done:     make(chan struct{}),
	}
	go pipe.run()
	return pipe
}

func (self *delayedSignalPipe) SetSignalReceiver(receiver SignalReceiver) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.receiver = receiver
}

// SignalReceiver returns the receiver current at dispatch time.
func (self *delayedSignalPipe) SignalReceiver() SignalReceiver {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.receiver
}

// SendSignal copies one owned frame into the delayed delivery queue and consumes it.
func (self *delayedSignalPipe) SendSignal(destinationId Id, frame *protocol.Frame, opts ...any) {
	defer MessagePoolReturn(frame.MessageBytes)
	self.admissionLock.Lock()
	if self.closed {
		self.admissionLock.Unlock()
		return
	}
	self.senders.Add(1)
	self.admissionLock.Unlock()
	defer self.senders.Done()
	if self.beforeAdmissionForTest != nil {
		self.beforeAdmissionForTest()
	}
	select {
	case <-self.ctx.Done():
		return
	case self.admitted <- struct{}{}:
	}
	owned := &protocol.Frame{
		MessageType:  frame.MessageType,
		Raw:          frame.Raw,
		MessageBytes: MessagePoolCopy(frame.MessageBytes),
	}
	select {
	case <-self.ctx.Done():
		MessagePoolReturn(owned.MessageBytes)
		<-self.admitted
	case self.queue <- delayedSignalFrame{
		destinationId: destinationId,
		transferKey:   testingSignalTransferKey(opts),
		frame:         owned,
		due:           time.Now().Add(self.delay),
	}:
		if self.afterEnqueueForTest != nil {
			self.afterEnqueueForTest(owned)
		}
	}
}

// dispatch waits until one frame's original due time, resolves the receiver's
// current stream registration, and releases owned bytes on every exit.
func (self *delayedSignalPipe) dispatch(
	timer **time.Timer,
	signal delayedSignalFrame,
) bool {
	defer func() {
		MessagePoolReturn(signal.frame.MessageBytes)
		<-self.admitted
	}()
	if self.afterDequeueForTest != nil {
		self.afterDequeueForTest()
	}
	timerC := resetOrCreateTimer(timer, time.Until(signal.due))
	select {
	case <-self.ctx.Done():
		return false
	case <-timerC:
	}
	if self.beforeDispatchForTest != nil {
		self.beforeDispatchForTest()
	}
	if self.ctx.Err() != nil {
		return false
	}
	receiver := self.SignalReceiver()
	if receiver == nil {
		return true
	}
	source, ok := testingSignalSource(
		receiver,
		signal.destinationId,
		signal.frame,
	)
	if !ok {
		if self.afterMissingDestinationDropForTest != nil {
			self.afterMissingDestinationDropForTest()
		}
		return true
	}
	_ = receiver.ReceiveSignal(
		source,
		signal.transferKey,
		signal.frame,
	)
	return true
}

// run owns dispatch ordering and returns every queued buffer before completion.
func (self *delayedSignalPipe) run() {
	var timer *time.Timer
	defer func() {
		if timer != nil {
			timer.Stop()
		}
		self.admissionLock.Lock()
		self.closed = true
		self.admissionLock.Unlock()
		self.senders.Wait()
		for {
			select {
			case signal := <-self.queue:
				MessagePoolReturn(signal.frame.MessageBytes)
				<-self.admitted
			default:
				close(self.done)
				return
			}
		}
	}()
	for {
		var signal delayedSignalFrame
		select {
		case <-self.ctx.Done():
			return
		case signal = <-self.queue:
		}
		if !self.dispatch(&timer, signal) {
			return
		}
	}
}

// CONNECT_WEBRTC_SIGNAL_LATENCY_MEASURE=1 enables a manual setup measurement
// with a controlled 25 ms one-way signaling delay. It captures candidate/SDP
// serialization that a zero-delay in-process signal pipe hides.
func TestWebRtcSignalingReadyLatencyMeasurement(t *testing.T) {
	if os.Getenv("CONNECT_WEBRTC_SIGNAL_LATENCY_MEASURE") == "" {
		t.Skip("set CONNECT_WEBRTC_SIGNAL_LATENCY_MEASURE=1")
	}

	const pairCount = 15
	const signalDelay = 25 * time.Millisecond
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	settingsA := DefaultWebRtcSettings()
	settingsB := DefaultWebRtcSettings()
	settingsA.Log = NewNoopLogger()
	settingsB.Log = NewNoopLogger()
	settingsA.IceServerUrls = nil
	settingsB.IceServerUrls = nil
	settingsA.MaxPeerConnectionCount = 0
	settingsB.MaxPeerConnectionCount = 0
	settingsA.UseEgressOnlyIceInterfaces = true
	settingsB.UseEgressOnlyIceInterfaces = true

	signalPipeA := newDelayedSignalPipe(ctx, signalDelay, nil)
	signalPipeB := newDelayedSignalPipe(ctx, signalDelay, nil)
	managerA := newTestWebRtcManager(t, ctx, signalPipeA, settingsA)
	managerB := newTestWebRtcManager(t, ctx, signalPipeB, settingsB)
	signalPipeA.SetSignalReceiver(managerB)
	signalPipeB.SetSignalReceiver(managerA)

	peerIdA := NewId()
	peerIdB := NewId()
	latencies := make([]time.Duration, 0, pairCount)
	for range pairCount {
		streamId := NewId()
		passive, err := managerB.NewP2pConnPassive(
			ctx,
			NewTransferPath(peerIdB, peerIdA, streamId),
		)
		AssertEqual(t, err, nil)

		start := time.Now()
		active, err := managerA.NewP2pConnActive(
			ctx,
			NewTransferPath(peerIdA, peerIdB, streamId),
		)
		AssertEqual(t, err, nil)
		AssertEqual(t, active.SetWriteDeadline(time.Now().Add(5*time.Second)), nil)
		AssertEqual(t, passive.SetReadDeadline(time.Now().Add(5*time.Second)), nil)
		readDone := make(chan error, 1)
		go func() {
			var b [1]byte
			_, readErr := passive.Read(b[:])
			readDone <- readErr
		}()
		_, err = active.Write([]byte{1})
		AssertEqual(t, err, nil)
		AssertEqual(t, <-readDone, nil)
		latencies = append(latencies, time.Since(start))
		active.Close()
		passive.Close()
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	t.Logf(
		"25ms one-way signaling ready latency: median=%s p95=%s min=%s max=%s",
		latencies[len(latencies)/2],
		latencies[(len(latencies)*95-1)/100],
		latencies[0],
		latencies[len(latencies)-1],
	)
}

// BenchmarkCreateWebRtcPeerConnection tracks the setup cost paid before ICE
// gathering begins. Network-peer churn can create several peer connections at
// once, so constructor latency and allocation volume directly affect setup
// tail latency and mobile GC frequency.
func BenchmarkCreateWebRtcPeerConnection(b *testing.B) {
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()

	b.ReportAllocs()
	for range b.N {
		factory, _, err := newWebRtcPeerConnectionFactory(settings, nil)
		if err != nil {
			b.Fatal(err)
		}
		pc, cancelResolve, err := factory.NewPeerConnection(false)
		if err != nil {
			b.Fatal(err)
		}
		cancelResolve()
		if err := pc.Close(); err != nil {
			b.Fatal(err)
		}
		if err := factory.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWebRtcPeerConnectionFactoryReuse(b *testing.B) {
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	factory, _, err := newWebRtcPeerConnectionFactory(settings, nil)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		pc, cancelResolve, err := factory.NewPeerConnection(false)
		if err != nil {
			b.Fatal(err)
		}
		cancelResolve()
		if err := pc.Close(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if err := factory.Close(); err != nil {
		b.Fatal(err)
	}
}

func BenchmarkWebRtcPeerConnectionFactoryRebuildWithCertificate(b *testing.B) {
	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	initialFactory, certificate, err := newWebRtcPeerConnectionFactory(settings, nil)
	if err != nil {
		b.Fatal(err)
	}
	if err := initialFactory.Close(); err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		factory, nextCertificate, createErr := newWebRtcPeerConnectionFactory(settings, certificate)
		if createErr != nil {
			b.Fatal(createErr)
		}
		if nextCertificate != certificate {
			b.Fatal("factory rebuild replaced certificate")
		}
		pc, cancelResolve, createErr := factory.NewPeerConnection(false)
		if createErr != nil {
			b.Fatal(createErr)
		}
		cancelResolve()
		if err := pc.Close(); err != nil {
			b.Fatal(err)
		}
		if err := factory.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPeerConnNoteOutboundSctpActivity(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	conn := &peerConn{
		ctx:      ctx,
		cancel:   cancel,
		log:      NewNoopLogger(),
		settings: &WebRtcSettings{},
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		conn.noteOutboundSctpActivity()
	}
}

func BenchmarkP2pReceiveBufferOwnership(b *testing.B) {
	payload := bytes.Repeat([]byte{0x5a}, sendPackBatchMaxMessageByteCount)
	b.SetBytes(int64(len(payload)))

	b.Run("scratch-then-copy", func(b *testing.B) {
		scratch := make([]byte, len(payload))
		b.ReportAllocs()
		for range b.N {
			copy(scratch, payload)
			owned := MessagePoolCopy(scratch)
			MessagePoolReturn(owned)
		}
	})
	b.Run("direct-owned-read", func(b *testing.B) {
		b.ReportAllocs()
		for range b.N {
			owned := MessagePoolGet(len(payload))
			copy(owned, payload)
			MessagePoolReturn(owned)
		}
	})
}

// CONNECT_WEBRTC_MEASURE=1 enables a manual live-resource probe. It keeps
// multiple connections on the same two managers so changes such as API,
// certificate, candidate filtering, and ICE socket behavior show up in heap,
// goroutine, and candidate-pair deltas. CONNECT_WEBRTC_EGRESS_ONLY=1 selects
// the device-client interface profile.
func TestWebRtcLiveResourceMeasurement(t *testing.T) {
	if os.Getenv("CONNECT_WEBRTC_MEASURE") == "" {
		t.Skip("set CONNECT_WEBRTC_MEASURE=1")
	}
	const pairCount = 8
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	settingsA := DefaultWebRtcSettings()
	settingsB := DefaultWebRtcSettings()
	settingsA.Log = NewNoopLogger()
	settingsB.Log = NewNoopLogger()
	switch os.Getenv("CONNECT_WEBRTC_STUN") {
	case "":
		settingsA.IceServerUrls = nil
		settingsB.IceServerUrls = nil
	case "cloudflare":
		settingsA.IceServerUrls = []string{"stun:stun.cloudflare.com:3478"}
		settingsB.IceServerUrls = []string{"stun:stun.cloudflare.com:3478"}
	case "google":
		settingsA.IceServerUrls = []string{"stun:stun.l.google.com:19302"}
		settingsB.IceServerUrls = []string{"stun:stun.l.google.com:19302"}
	}
	settingsA.MaxPeerConnectionCount = 0
	settingsB.MaxPeerConnectionCount = 0
	egressOnly := os.Getenv("CONNECT_WEBRTC_EGRESS_ONLY") != ""
	settingsA.UseEgressOnlyIceInterfaces = egressOnly
	settingsB.UseEgressOnlyIceInterfaces = egressOnly
	if keepAliveValue := os.Getenv("CONNECT_WEBRTC_KEEPALIVE"); keepAliveValue != "" {
		keepAlive, err := time.ParseDuration(keepAliveValue)
		AssertEqual(t, err, nil)
		settingsA.KeepAliveTimeout = keepAlive
		settingsB.KeepAliveTimeout = keepAlive
	}

	signalPipeA := newSignalPipe(nil)
	signalPipeB := newSignalPipe(nil)
	managerA := newTestWebRtcManager(t, ctx, signalPipeA, settingsA)
	managerB := newTestWebRtcManager(t, ctx, signalPipeB, settingsB)
	signalPipeA.SetSignalReceiver(managerB)
	signalPipeB.SetSignalReceiver(managerA)

	fdCount := func() int {
		if runtime.GOOS != "linux" {
			return -1
		}
		entries, err := os.ReadDir("/proc/self/fd")
		if err != nil {
			return -1
		}
		return len(entries)
	}
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)
	beforeGoroutines := runtime.NumGoroutine()
	beforeFds := fdCount()

	peerIdA := NewId()
	peerIdB := NewId()
	conns := make([]WebRtcConn, 0, 2*pairCount)
	for range pairCount {
		streamId := NewId()
		passive, err := managerB.NewP2pConnPassive(ctx, NewTransferPath(peerIdB, peerIdA, streamId))
		AssertEqual(t, err, nil)
		conns = append(conns, passive)
		active, err := managerA.NewP2pConnActive(ctx, NewTransferPath(peerIdA, peerIdB, streamId))
		AssertEqual(t, err, nil)
		conns = append(conns, active)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		connected := 0
		for _, conn := range conns {
			if conn.Connected() {
				connected++
			}
		}
		if connected == len(conns) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d peer connections connected", connected, len(conns))
		}
		time.Sleep(10 * time.Millisecond)
	}
	if idleValue := os.Getenv("CONNECT_WEBRTC_IDLE"); idleValue != "" {
		idle, err := time.ParseDuration(idleValue)
		AssertEqual(t, err, nil)
		select {
		case <-ctx.Done():
			t.Fatal("context ended during idle measurement")
		case <-time.After(idle):
		}
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	localCandidates := 0
	remoteCandidates := 0
	candidatePairs := 0
	for _, stat := range conns[0].(*peerConn).pc.GetStats() {
		switch stat := stat.(type) {
		case webrtc.ICECandidateStats:
			switch stat.Type {
			case webrtc.StatsTypeLocalCandidate:
				localCandidates++
			case webrtc.StatsTypeRemoteCandidate:
				remoteCandidates++
			}
		case webrtc.ICECandidatePairStats:
			candidatePairs++
		}
	}
	fdDelta := -1
	if afterFds := fdCount(); 0 <= beforeFds && 0 <= afterFds {
		fdDelta = afterFds - beforeFds
	}
	t.Logf(
		"%d live peer connections (egress_only=%t keepalive=%s): heap=%d bytes, heap_objects=%d, goroutines=%d, fds=%d, first_pc_candidates=%d/%d pairs=%d",
		len(conns),
		egressOnly,
		settingsA.KeepAliveTimeout,
		int64(after.HeapAlloc)-int64(before.HeapAlloc),
		int64(after.HeapObjects)-int64(before.HeapObjects),
		runtime.NumGoroutine()-beforeGoroutines,
		fdDelta,
		localCandidates,
		remoteCandidates,
		candidatePairs,
	)
	if profilePath := os.Getenv("CONNECT_WEBRTC_GOROUTINE_PROFILE"); profilePath != "" {
		profile, createErr := os.Create(profilePath)
		AssertEqual(t, createErr, nil)
		AssertEqual(t, pprof.Lookup("goroutine").WriteTo(profile, 0), nil)
		AssertEqual(t, profile.Close(), nil)
	}
	for _, conn := range conns {
		conn.Close()
	}
}

// CONNECT_WEBRTC_ROUTE_THROUGHPUT_MEASURE=1 enables a manual comparison of
// the bounded route-channel depth around a real detached data channel. It
// normally exercises production's 3 KiB transfer batching. Setting
// CONNECT_WEBRTC_ROUTE_MESSAGE_SIZES=1 also measures larger messages to
// quantify the ceiling available to a future capability-negotiated P2P
// aggregate without pretending those sizes fit the platform fallback route.
// This exists to prevent reducing queue memory based on intuition at the
// expense of scheduler-bound throughput.
func TestWebRtcP2pRouteThroughputMeasurement(t *testing.T) {
	if os.Getenv("CONNECT_WEBRTC_ROUTE_THROUGHPUT_MEASURE") == "" {
		t.Skip("set CONNECT_WEBRTC_ROUTE_THROUGHPUT_MEASURE=1")
	}

	messageByteCounts := []int{sendPackBatchMaxMessageByteCount}
	channelBufferSizes := []int{1, 4, 8, 32}
	if os.Getenv("CONNECT_WEBRTC_ROUTE_MESSAGE_SIZES") != "" {
		messageByteCounts = []int{3 * 1024, 6 * 1024, 12 * 1024, 24 * 1024, 48 * 1024, 60 * 1024}
		channelBufferSizes = []int{4}
	}
	for _, channelBufferSize := range channelBufferSizes {
		for _, messageByteCount := range messageByteCounts {
			func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				settingsA := DefaultWebRtcSettings()
				settingsB := DefaultWebRtcSettings()
				settingsA.Log = NewNoopLogger()
				settingsB.Log = NewNoopLogger()
				settingsA.IceServerUrls = nil
				settingsB.IceServerUrls = nil
				settingsA.MaxPeerConnectionCount = 0
				settingsB.MaxPeerConnectionCount = 0
				settingsA.UseEgressOnlyIceInterfaces = true
				settingsB.UseEgressOnlyIceInterfaces = true
				if os.Getenv("CONNECT_WEBRTC_ORDERED") != "" {
					settingsA.DataChannelOrdered = true
					settingsB.DataChannelOrdered = true
				}

				signalPipeA := newSignalPipe(nil)
				signalPipeB := newSignalPipe(nil)
				managerA := newTestWebRtcManager(t, ctx, signalPipeA, settingsA)
				managerB := newTestWebRtcManager(t, ctx, signalPipeB, settingsB)
				signalPipeA.SetSignalReceiver(managerB)
				signalPipeB.SetSignalReceiver(managerA)

				peerIdA := NewId()
				peerIdB := NewId()
				streamId := NewId()
				passive, err := managerB.NewP2pConnPassive(
					ctx,
					NewTransferPath(peerIdB, peerIdA, streamId),
				)
				AssertEqual(t, err, nil)
				defer passive.Close()
				active, err := managerA.NewP2pConnActive(
					ctx,
					NewTransferPath(peerIdA, peerIdB, streamId),
				)
				AssertEqual(t, err, nil)
				defer active.Close()

				transportSettings := DefaultP2pTransportSettings()
				transportSettings.ChannelBufferSize = channelBufferSize
				transportCtx, transportCancel := context.WithCancel(ctx)
				defer transportCancel()
				sendTransport, sendRoute := NewP2pSendTransport(
					transportCtx,
					transportCancel,
					active,
					streamId,
					transportSettings,
				)
				receiveTransport, receiveRoute := NewP2pReceiveTransport(
					transportCtx,
					transportCancel,
					passive,
					streamId,
					transportSettings,
				)
				// Keep both transport owners live for the duration of the route
				// measurement; the routes alone do not express that relationship.
				_ = sendTransport
				_ = receiveTransport

				const totalByteCount = 32 * 1024 * 1024
				messageCount := totalByteCount / messageByteCount
				payload := bytes.Repeat([]byte{0x5a}, messageByteCount)
				receiveDone := make(chan error, 1)
				go func() {
					for range messageCount {
						select {
						case <-transportCtx.Done():
							receiveDone <- transportCtx.Err()
							return
						case received := <-receiveRoute:
							if len(received) != len(payload) || received[0] != payload[0] {
								MessagePoolReturn(received)
								receiveDone <- errors.New("received payload mismatch")
								return
							}
							MessagePoolReturn(received)
						}
					}
					receiveDone <- nil
				}()

				start := time.Now()
				for range messageCount {
					message := MessagePoolCopy(payload)
					select {
					case <-transportCtx.Done():
						MessagePoolReturn(message)
						t.Fatal("transport stopped during throughput measurement")
					case sendRoute <- message:
					}
				}
				AssertEqual(t, <-receiveDone, nil)
				elapsed := time.Since(start)
				t.Logf(
					"queue=%d message=%d ordered=%t: %d bytes in %s = %.1f MiB/s",
					channelBufferSize,
					messageByteCount,
					settingsA.DataChannelOrdered,
					messageCount*messageByteCount,
					elapsed,
					float64(messageCount*messageByteCount)/(1024*1024)/elapsed.Seconds(),
				)
			}()
		}
	}
}
