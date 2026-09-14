package connect

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"
	// "fmt"
)

// Assumptions about our peer-to-peer connections:
// - a limited transmit buffer that uses semi-reliable delivery as flow control.
//   While the transfer client is the ultimate source of reliable delivery,
//   we require the p2p connection use semi-reliable delivery to back pressure the transfer rate,
//   which propagates through the entire multi-hop stream.
//   Without flow control we would have more mismatches in transfer rate
//   and retransmits from the transfer clients.
// - disconnect detection. Both peers should be aware when either side disconnects.
//   This is typically manifested in clean disconnect messages and heartbeat timeouts.
// - directed initializaton. One side of the connection will offer to connect
//   and the other side will respond. We assume this in our architecture. However,
//   directed is usually a superset of undirected, so this does not prevent an undirected
//   initializtion either.

// important - changing this will break compatibility with older clients
const ReadyHeader = "rdy"

// readP2pMessage reads one complete message from a message-oriented conn into
// an owned pool buffer. The buffer remains checked out for the full blocking
// Read, so the connection lifecycle must interrupt and join that read. Pion/
// SCTP itself reports the required size in n with io.ErrShortBuffer, but the
// detached datachannel wrapper intentionally masks that value and returns n=0.
// Grow geometrically in that case; compatibility conns that preserve the size
// still jump directly to it. The queued SCTP message is not consumed on
// io.ErrShortBuffer.
func readP2pMessage(
	conn net.Conn,
	initialByteCount int,
	shortBufferFloorByteCount int,
	maxByteCount int,
) ([]byte, error) {
	maxByteCount = max(1, maxByteCount)
	readByteCount := min(max(1, initialByteCount), maxByteCount)
	shortBufferFloorByteCount = min(
		max(readByteCount, shortBufferFloorByteCount),
		maxByteCount,
	)
	for {
		readBuf := MessagePoolGet(readByteCount)
		n, err := conn.Read(readBuf)
		if errors.Is(err, io.ErrShortBuffer) {
			MessagePoolReturn(readBuf)
			if maxByteCount < n || readByteCount >= maxByteCount {
				return nil, fmt.Errorf(
					"p2p message exceeds maximum %d (reported=%d)",
					maxByteCount,
					n,
				)
			}
			nextReadByteCount := maxByteCount
			if readByteCount <= maxByteCount/2 {
				nextReadByteCount = readByteCount * 2
			}
			nextReadByteCount = max(nextReadByteCount, shortBufferFloorByteCount)
			if readByteCount < n {
				nextReadByteCount = min(maxByteCount, max(nextReadByteCount, n))
			}
			if nextReadByteCount <= readByteCount {
				return nil, fmt.Errorf(
					"p2p message cannot grow beyond %d (max=%d reported=%d)",
					readByteCount,
					maxByteCount,
					n,
				)
			}
			readByteCount = nextReadByteCount
			continue
		}
		if n < 0 || len(readBuf) < n {
			MessagePoolReturn(readBuf)
			return nil, fmt.Errorf(
				"p2p invalid receive byte count %d (buffer=%d)",
				n,
				len(readBuf),
			)
		}
		if n == 0 {
			MessagePoolReturn(readBuf)
			if err != nil {
				return nil, err
			}
			continue
		}
		return readBuf[:n], err
	}
}

// readP2pReadyHeader waits for the setup marker on a reliable-unordered data
// channel. Once one peer reads the other's marker it may install its send
// route before the other peer has read this marker, so a later transfer frame
// can legitimately be delivered first after packet reordering/loss. Retain at
// most the receive-route capacity and drop any excess (the transfer protocol
// retransmits unacked frames); this keeps setup memory bounded while avoiding
// a false header mismatch/reconnect loop.
func readP2pReadyHeader(
	conn net.Conn,
	settings *P2pTransportSettings,
) (prefetched [][]byte, err error) {
	defer func() {
		if err != nil {
			for _, message := range prefetched {
				MessagePoolReturn(message)
			}
			prefetched = nil
		}
	}()

	header := []byte(ReadyHeader)
	maxMessageByteCount := max(1, settings.MaxMessageByteCount)
	initialMessageByteCount := settings.InitialReadBufferByteCount
	if initialMessageByteCount <= 0 {
		initialMessageByteCount = min(4*1024, maxMessageByteCount)
	}
	maxPrefetched := max(0, settings.ChannelBufferSize)
	retain := func(message []byte) {
		if len(prefetched) < maxPrefetched {
			prefetched = append(prefetched, message)
		} else {
			MessagePoolReturn(message)
		}
	}

	for {
		message, readErr := readP2pMessage(
			conn,
			len(header),
			initialMessageByteCount,
			maxMessageByteCount,
		)
		if slices.Equal(message, header) {
			MessagePoolReturn(message)
			return prefetched, nil
		}
		if readErr != nil {
			MessagePoolReturn(message)
			return nil, readErr
		}
		retain(message)
	}
}

func DefaultP2pTransportSettings() *P2pTransportSettings {
	return &P2pTransportSettings{
		WriteTimeout:          15 * time.Second,
		ReadTimeout:           15 * time.Second,
		ConnectTimeout:        15 * time.Second,
		ReconnectTimeout:      5 * time.Second,
		AdmissionRetryTimeout: 30 * time.Second,
		// Five seconds leaves a physical quiet window after a complete probe
		// on the supported one-second regional path. A one-second cadence lets
		// the two endpoints' independent request/response trains interleave
		// continuously at that round-trip time.
		EndToEndProbeInterval: 5 * time.Second,
		EndToEndProbeTimeout:  15 * time.Second,
		// Four transfer batches absorb ordinary goroutine scheduling jitter on
		// send and bound readiness prefetch. A real detached-data-channel
		// measurement sustained the same 53-54 MiB/s at depths 1/4/8/32.
		// Receive handoff has independent count and byte limits below.
		ChannelBufferSize: 4,
		// The carrier readers hand off without waiting. Count and bytes are
		// independent: 256 slots absorb concurrent data, ACK, contract, and
		// probe sequence bursts, while the separate byte bound still permits at
		// most four worst-case 64 KiB messages. Channel metadata adds only a few
		// KiB per connection and cannot expand retained payload ownership.
		ReceiveQueueMessageCount: 256,
		ReceiveQueueByteCount:    kib(256),
		// Transfer batching is capped at 3 KiB, so almost every data-channel
		// message fits in the 4 KiB pooled class. The receiver retries once
		// with Pion's exact required length for a legacy/atypical larger
		// message, then returns to this size.
		InitialReadBufferByteCount: 4 * 1024,
		MaxMessageByteCount:        64 * 1024,
		DataPlaneMode:              P2pDataPlaneModeAuto,
	}
}

type PeerType = string

const (
	// the peer who initiates the transfer
	PeerTypeSource PeerType = "source"
	// the peer who is the destination of the transfer
	PeerTypeDestination PeerType = "destination"
)

// A route observation identifies one installed local direction of a P2P
// stream. Observers must not block.
type P2pRouteState struct {
	PeerId          Id
	StreamId        Id
	PeerType        PeerType
	RouteManagerTag string
	Send            bool
	Connected       bool
}

// A health event identifies one endpoint probe transition. A fixed bounded
// dispatcher isolates observers from transport workers and drops saturation.
type P2pStreamProbeEvent struct {
	Type       string
	StreamId   Id
	Nonce      Id
	RouteEpoch uint64
}

const (
	P2pStreamProbeEventRouteReady           = "route-ready"
	P2pStreamProbeEventRouteCleared         = "route-cleared"
	P2pStreamProbeEventRequestQueued        = "request-queued"
	P2pStreamProbeEventRequestDropped       = "request-dropped"
	P2pStreamProbeEventRequestReceived      = "request-received"
	P2pStreamProbeEventResponseQueued       = "response-queued"
	P2pStreamProbeEventResponseDropped      = "response-dropped"
	P2pStreamProbeEventResponseReceived     = "response-received"
	P2pStreamProbeEventResponseQueueFull    = "response-queue-full"
	P2pStreamProbeEventResponseMatched      = "response-matched"
	P2pStreamProbeEventResponseStale        = "response-stale"
	P2pStreamProbeEventReadinessGranted     = "readiness-granted"
	P2pStreamProbeEventReadinessWithdrawn   = "readiness-withdrawn"
	P2pStreamProbeEventCompatibilityBackoff = "compatibility-backoff"
)

type P2pTransportSettings struct {
	WriteTimeout     time.Duration
	ReadTimeout      time.Duration
	ConnectTimeout   time.Duration
	ReconnectTimeout time.Duration
	// AdmissionRetryTimeout is only the liveness fallback for a saturated
	// peer-connection count/memory budget. Normal retries are woken
	// immediately by a release, avoiding the former sub-second polling storm.
	AdmissionRetryTimeout time.Duration
	// EndToEndProbeInterval controls transport-local stream challenges between
	// endpoints. Intermediaries relay these without application delivery.
	EndToEndProbeInterval time.Duration
	// EndToEndProbeTimeout withdraws a shared stream alias when challenge
	// responses stop crossing the complete path.
	EndToEndProbeTimeout time.Duration
	ChannelBufferSize    int
	// ReceiveQueueMessageCount bounds complete messages waiting between the
	// SCTP/SRTP readers and the RouteManager, including the one currently held
	// by the forwarding worker. ReceiveQueueByteCount is the hard retained
	// payload ceiling for that same queue. Nonpositive values retain
	// compatibility by deriving the old ChannelBufferSize*MaxMessageByteCount
	// bound.
	ReceiveQueueMessageCount int
	ReceiveQueueByteCount    ByteCount
	// InitialReadBufferByteCount is the first pooled receive size.
	// io.ErrShortBuffer retries with Pion's exact required length up to
	// MaxMessageByteCount without consuming the queued SCTP message.
	InitialReadBufferByteCount int
	// MaxMessageByteCount is the largest single message the transport reads or
	// writes. The detached WebRTC data channel is message-oriented: one pion
	// Read returns exactly one whole SCTP user message, and pion/sctp returns
	// io.ErrShortBuffer (leaving the message queued) when the read buffer is
	// smaller than the message. The on-wire framing is therefore the SCTP
	// message boundary itself — no length prefix — and the receive buffer must
	// be >= the largest TransferFrame that can arrive.
	MaxMessageByteCount int
	// DataPlaneMode selects automatic capability fallback or one forced lane.
	// Forced modes exist for deterministic compatibility and performance tests.
	DataPlaneMode P2pDataPlaneMode
	// DataPlaneStats observes the actual negotiated lane. It may be nil when
	// callers do not need instrumentation.
	DataPlaneStats *P2pDataPlaneStats
	// RouteStateObserver is a nil-by-default integration seam for deterministic
	// route attribution. It must not block.
	RouteStateObserver func(P2pRouteState)
	// EndToEndProbeObserver is a nil-by-default diagnostic seam for endpoint
	// stream readiness. A fixed worker shard invokes it out of band; events are
	// dropped when that bounded shard queue is saturated.
	EndToEndProbeObserver func(P2pStreamProbeEvent)
	// Nil test barrier pauses one association after route cleanup but before its
	// done publication.
	beforeRunDoneForTest func(Id, PeerType)
	// Nil test hooks expose child generations created by the parent transport.
	afterSendTransportForTest    func(*P2pSendTransport)
	afterReceiveTransportForTest func(*P2pReceiveTransport)
	// Nil test hooks expose connected-callback admission and route mutation.
	beforeRouteUpdateForTest       func(send bool)
	afterRouteUpdateForTest        func(send bool)
	beforeRouteCallbackJoinForTest func(send bool)
	// Nil test barrier pauses a low-level receive worker before it replaces the
	// inherited handshake deadline with its steady-state deadline.
	beforeReceiveSteadyStateDeadlineClearForTest func()
}

type P2pTransport struct {
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	client *Client

	webRtcManager *WebRtcManager

	sendRouteManager    *RouteManager
	receiveRouteManager *RouteManager

	peerId   Id
	streamId Id
	peerType PeerType
	// endpointProbe is enabled only where the multihop stream terminates.
	// Intermediaries must relay the reserved envelope unchanged.
	endpointProbe bool

	settings *P2pTransportSettings
}

type p2pRouteManager interface {
	UpdateTransport(Transport, []Route)
	UpdateTransportWithProperties(Transport, []Route, TransferCarrierProperties)
	RemoveTransport(Transport)
}

// p2pTransferCarrierProperties exposes the complete P2P receive path to
// Transfer's acknowledgement-flight controller. Both native RTP/SRTP and the
// legacy SCTP lane terminate in a deliberately nonblocking bounded handoff;
// even SCTP can therefore lose a complete Transfer message after its carrier
// write succeeds. Advertising every mode as unreliable keeps at most the
// receiver's data-slot capacity in flight and leaves one slot for cumulative
// ACK, compact-recovery, probe, and contract control messages.
func p2pTransferCarrierProperties(transport Transport) TransferCarrierProperties {
	if _, ok := transport.(*P2pReceiveTransport); ok {
		// The deprecated single-route constructor publishes a reliable immediate
		// route. Its native fast reader still has a bounded zero-wait queue before
		// this route; production publishes separate exact lanes below.
		return TransferCarrierProperties{
			ReceiveReliability: CarrierReliabilityReliable,
		}
	}
	send, ok := transport.(*P2pSendTransport)
	if !ok || send.settings == nil {
		return TransferCarrierProperties{}
	}
	fastConn, supportsFastPath := send.conn.(webRtcFastPathConn)
	potentiallyUnreliable := send.settings.DataPlaneMode != P2pDataPlaneModeLegacyOnly &&
		(supportsFastPath || send.settings.DataPlaneMode == P2pDataPlaneModeFastOnly)
	if !potentiallyUnreliable {
		return TransferCarrierProperties{}
	}
	properties := TransferCarrierProperties{
		Unreliable:                   true,
		unreliableFlightByteLimit:    p2pUnreliableFlightByteLimit(send.settings),
		unreliableFlightMessageLimit: p2pUnreliableFlightMessageLimit(send.settings),
	}
	properties.unreliableForMessageByteCount = func(int) bool {
		if send.settings.DataPlaneMode == P2pDataPlaneModeFastOnly {
			return true
		}
		return supportsFastPath && fastConn.FastPathReady()
	}
	return properties
}

// Reserve at least 16 KiB, or one sixteenth of a larger queue, outside the
// destination-wide Transfer data flight. Carrier admission is deliberately
// nonblocking and also receives untracked cumulative ACK, compact-recovery,
// contract, and probe messages. The reserve prevents an ordinary maximum
// flight from consuming the complete hard byte ceiling while keeping retained
// payload at the same configured bound.
func p2pUnreliableFlightByteLimit(settings *P2pTransportSettings) ByteCount {
	queueByteCount := p2pReceiveQueueByteCount(settings)
	if queueByteCount <= 1 {
		return 1
	}
	reserveByteCount := min(
		queueByteCount-1,
		max(kib(16), queueByteCount/16),
	)
	return queueByteCount - reserveByteCount
}

// Transfer maintains one destination-wide acknowledgement flight. Keep its
// message ceiling below the complete-message receive queue while the separate
// adaptive byte limit remains the tighter retained-payload bound. Reserving
// one queue slot leaves immediate admission for cumulative ACK, compact
// recovery, probe, and contract traffic; a one-slot route retains a progress
// floor of one.
func p2pUnreliableFlightMessageLimit(settings *P2pTransportSettings) int {
	return max(1, p2pReceiveQueueMessageCount(settings)-1)
}

func p2pReceiveQueueMessageCount(settings *P2pTransportSettings) int {
	if settings != nil && 0 < settings.ReceiveQueueMessageCount {
		return settings.ReceiveQueueMessageCount
	}
	if settings == nil {
		return 1
	}
	return max(1, settings.ChannelBufferSize)
}

func p2pReceiveQueueByteCount(settings *P2pTransportSettings) ByteCount {
	if settings == nil {
		return 1
	}
	maximumMessageByteCount := max(1, settings.MaxMessageByteCount)
	if 0 < settings.ReceiveQueueByteCount {
		return max(ByteCount(maximumMessageByteCount), settings.ReceiveQueueByteCount)
	}
	return ByteCount(max(1, settings.ChannelBufferSize)) *
		ByteCount(maximumMessageByteCount)
}

// p2pConnectionRouteTestHooks exposes the two sides of a connected route
// mutation. Production calls leave both fields nil.
type p2pConnectionRouteTestHooks struct {
	beforeConnectedUpdate func()
	afterConnectedUpdate  func()
}

type p2pReceiveRouteLane struct {
	transport   Transport
	route       Route
	reliability CarrierReliability
}

// updateP2pConnectionReceiveRoutes publishes or retires both physical receive
// lanes as one connection-state transition. Each route gets immutable exact
// reliability while the association remains one observable P2P adjacency.
func updateP2pConnectionReceiveRoutes(
	ctx context.Context,
	manager p2pRouteManager,
	lanes []p2pReceiveRouteLane,
	connected bool,
	testHooks ...p2pConnectionRouteTestHooks,
) bool {
	remove := func() {
		for _, lane := range lanes {
			manager.RemoveTransport(lane.transport)
		}
	}
	if !connected || ctx.Err() != nil {
		remove()
		return false
	}
	var hooks p2pConnectionRouteTestHooks
	if 0 < len(testHooks) {
		hooks = testHooks[0]
	}
	if hooks.beforeConnectedUpdate != nil {
		hooks.beforeConnectedUpdate()
	}
	for _, lane := range lanes {
		manager.UpdateTransportWithProperties(
			lane.transport,
			[]Route{lane.route},
			TransferCarrierProperties{ReceiveReliability: lane.reliability},
		)
	}
	if hooks.afterConnectedUpdate != nil {
		hooks.afterConnectedUpdate()
	}
	if ctx.Err() != nil {
		remove()
		return false
	}
	return true
}

// updateP2pConnectionRoute prevents a connected callback that was already in
// flight at cancellation from restoring a retired route after teardown's final
// removal. Connected-callback unsubscribe is deliberately nonblocking, so the
// post-update context check is the required make-progress counterpart.
func updateP2pConnectionRoute(
	ctx context.Context,
	manager p2pRouteManager,
	transport Transport,
	route Route,
	connected bool,
	testHooks ...p2pConnectionRouteTestHooks,
) bool {
	if connected && ctx.Err() == nil {
		var hooks p2pConnectionRouteTestHooks
		if 0 < len(testHooks) {
			hooks = testHooks[0]
		}
		if hooks.beforeConnectedUpdate != nil {
			hooks.beforeConnectedUpdate()
		}
		manager.UpdateTransportWithProperties(
			transport,
			[]Route{route},
			p2pTransferCarrierProperties(transport),
		)
		if hooks.afterConnectedUpdate != nil {
			hooks.afterConnectedUpdate()
		}
		if ctx.Err() != nil {
			manager.RemoveTransport(transport)
			return false
		}
		return true
	}
	manager.RemoveTransport(transport)
	return false
}

// Route gauges change only when a connection callback crosses the installed
// boundary, so duplicate callbacks cannot overcount an adjacency.
func updateP2pActiveRouteCount(
	stats *P2pDataPlaneStats,
	connectedState *atomic.Bool,
	send bool,
	connected bool,
) bool {
	var activeRouteCount *atomic.Int64
	if stats != nil {
		if send {
			activeRouteCount = &stats.activeSendRouteCount
		} else {
			activeRouteCount = &stats.activeReceiveRouteCount
		}
	}
	if connected {
		if connectedState.CompareAndSwap(false, true) {
			if activeRouteCount != nil {
				activeRouteCount.Add(1)
			}
			return true
		}
		return false
	}
	if connectedState.CompareAndSwap(true, false) {
		if activeRouteCount != nil {
			activeRouteCount.Add(-1)
		}
		return true
	}
	return false
}

// The optional route observer sees only real state edges, never duplicate
// connection callbacks.
func (self *P2pTransport) observeRouteState(
	connectedState *atomic.Bool,
	send bool,
	connected bool,
) {
	changed := updateP2pActiveRouteCount(
		self.settings.DataPlaneStats,
		connectedState,
		send,
		connected,
	)
	if changed && self.settings.RouteStateObserver != nil {
		routeManager := self.receiveRouteManager
		if send {
			routeManager = self.sendRouteManager
		}
		routeManagerTag := ""
		if routeManager != nil {
			routeManagerTag = routeManager.clientTag
		}
		self.settings.RouteStateObserver(P2pRouteState{
			PeerId:          self.peerId,
			StreamId:        self.streamId,
			PeerType:        self.peerType,
			RouteManagerTag: routeManagerTag,
			Send:            send,
			Connected:       connected,
		})
	}
}

// Starts one peer association. Stream endpoints use the internal variant to
// terminate end-to-end health envelopes; ordinary/direct callers do not.
func NewP2pTransport(
	ctx context.Context,
	client *Client,
	webRtcManager *WebRtcManager,
	sendRouteManager *RouteManager,
	receiveRouteManager *RouteManager,
	peerId Id,
	streamId Id,
	// this is the peer type of `peerId`. The current client is the complement.
	peerType PeerType,
	settings *P2pTransportSettings,
) *P2pTransport {
	return newP2pTransport(
		ctx,
		client,
		webRtcManager,
		sendRouteManager,
		receiveRouteManager,
		peerId,
		streamId,
		peerType,
		settings,
		false,
	)
}

// Builds one association and optionally terminates the raw end-to-end health
// envelope. Only StreamSequence endpoints enable the final argument.
func newP2pTransport(
	ctx context.Context,
	client *Client,
	webRtcManager *WebRtcManager,
	sendRouteManager *RouteManager,
	receiveRouteManager *RouteManager,
	peerId Id,
	streamId Id,
	peerType PeerType,
	settings *P2pTransportSettings,
	endpointProbe bool,
) *P2pTransport {
	cancelCtx, cancel := context.WithCancel(ctx)
	p2pTransport := &P2pTransport{
		ctx:                 cancelCtx,
		cancel:              cancel,
		done:                make(chan struct{}),
		client:              client,
		webRtcManager:       webRtcManager,
		sendRouteManager:    sendRouteManager,
		receiveRouteManager: receiveRouteManager,
		peerId:              peerId,
		streamId:            streamId,
		peerType:            peerType,
		endpointProbe:       endpointProbe,
		settings:            settings,
	}
	go HandleError(p2pTransport.run, cancel)
	return p2pTransport
}

func p2pAdmissionRetryTimeout(
	configured time.Duration,
	admissionErr *peerConnectionAdmissionError,
) time.Duration {
	timeout := configured
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	if admissionErr != nil &&
		0 < admissionErr.retryAfter &&
		admissionErr.retryAfter < timeout {
		return admissionErr.retryAfter
	}
	return timeout
}

func p2pAdmissionWaitChannels(
	admissionErr *peerConnectionAdmissionError,
	countNotify <-chan struct{},
	budgetNotify <-chan struct{},
	stateNotify <-chan struct{},
) (
	countWait <-chan struct{},
	budgetWait <-chan struct{},
	stateWait <-chan struct{},
) {
	stateWait = stateNotify
	if admissionErr == nil {
		return countNotify, budgetNotify, stateWait
	}
	switch admissionErr.reason {
	case peerConnectionAdmissionBudget:
		// A peer-count release does not make byte capacity available. The
		// threshold-aware budget waiter already wakes only as many setups as
		// the released bytes can admit.
		budgetWait = budgetNotify
	case peerConnectionAdmissionCount:
		// Count tokens are one-consumer notifications for actual map-slot
		// releases; unrelated budget churn must not wake this waiter.
		countWait = countNotify
	case peerConnectionAdmissionPriority:
		// Priority/classification transitions have their own rare broadcast;
		// lease expiration is covered by retryAfter.
	default:
		countWait = countNotify
		budgetWait = budgetNotify
	}
	return
}

func p2pSetupErrorKey(err error) string {
	var admissionErr *peerConnectionAdmissionError
	if errors.As(err, &admissionErr) && admissionErr.reason != "" {
		// Admission diagnostics deliberately include live counters. Those
		// counters fluctuate on every wake-all retry even when the underlying
		// failure has not changed; keying the log streak by the full message
		// turned expected saturation into periodic device log/CPU bursts.
		return "admission:" + string(admissionErr.reason)
	}
	if err == nil {
		return ""
	}
	return err.Error()
}

type p2pSetupFailureStreak struct {
	key string
}

func (self *p2pSetupFailureStreak) Observe(err error) bool {
	key := p2pSetupErrorKey(err)
	if key == self.key {
		return false
	}
	self.key = key
	return true
}

func (self *p2pSetupFailureStreak) Recover() bool {
	if self.key == "" {
		return false
	}
	self.key = ""
	return true
}

func (self *P2pTransport) run() {
	defer self.cancel()
	defer close(self.done)
	if self.settings.beforeRunDoneForTest != nil {
		defer self.settings.beforeRunDoneForTest(self.streamId, self.peerType)
	}

	var setupFailure p2pSetupFailureStreak
	var admissionTimer *time.Timer
	budgetWaiter := newTransferMemoryBudgetWaiter()
	defer func() {
		budgetWaiter.reset()
		if admissionTimer != nil {
			admissionTimer.Stop()
		}
	}()

	for {
		// TODO using net.Conn as a stand in for the actual interface

		reconnect := NewReconnect(self.settings.ReconnectTimeout)
		stateNotify := self.webRtcManager.admissionStateMonitor.NotifyChannel()
		countNotify, budgetNotify :=
			self.webRtcManager.admissionNotify(self.peerId, budgetWaiter)
		var conn WebRtcConn
		var err error
		// note, one side of the P2P connection will be driving the setup process (active).
		// We arbitrarily choose the sender (peer is destination) as active.
		switch self.peerType {
		case PeerTypeDestination:
			conn, err = self.webRtcManager.NewP2pConnActive(self.ctx, NewTransferPath(self.client.ClientId(), self.peerId, self.streamId))
		case PeerTypeSource:
			conn, err = self.webRtcManager.NewP2pConnPassive(self.ctx, NewTransferPath(self.client.ClientId(), self.peerId, self.streamId))
		default:
			// unknown peer type
			return
		}
		if err != nil {
			var admissionErr *peerConnectionAdmissionError
			if errors.As(err, &admissionErr) {
				// Admission capacity is shared by all streams, so logging the
				// first failure of every transport still creates a cold-start
				// stampede. Summarize each reason across the manager at powers
				// of two while retaining the latest path and full diagnostics.
				setupFailure.Observe(err)
				if count, emit := self.webRtcManager.observeAdmissionRefusal(
					admissionErr.reason,
				); emit {
					self.client.log.Infof(
						"[p2p]setup admission refused reason=%s count=%d s(%s) <>%s = %s\n",
						admissionErr.reason,
						count,
						self.streamId,
						self.peerId,
						err,
					)
				}
				// A pending priority lease can expire without a count or byte
				// release. Wake at that exact boundary instead of retaining an
				// ordinary stream for another full fallback period.
				timeout := p2pAdmissionRetryTimeout(
					self.settings.AdmissionRetryTimeout,
					admissionErr,
				)
				countWait, budgetWait, stateWait := p2pAdmissionWaitChannels(
					admissionErr,
					countNotify,
					budgetNotify,
					stateNotify,
				)
				if budgetWait == nil {
					// Do not retain a place in the byte-capacity FIFO when
					// this refusal cannot be resolved by byte capacity.
					budgetWaiter.reset()
				}
				timerC := resetOrCreateTimer(&admissionTimer, timeout)
				select {
				case <-self.ctx.Done():
					return
				case <-countWait:
					admissionTimer.Stop()
				case <-budgetWait:
					admissionTimer.Stop()
				case <-stateWait:
					admissionTimer.Stop()
				case <-timerC:
				}
				budgetWaiter.reset()
				continue
			}
			// Log a non-admission failure streak once. The former
			// log-on-every-poll behavior produced thousands of lines per
			// device, consumed CPU, and evicted the ICE trace needed to
			// diagnose the failure.
			if setupFailure.Observe(err) {
				self.client.log.Infof("[p2p]s(%s) <>%s setup refused = %s\n", self.streamId, self.peerId, err)
			}
			budgetWaiter.reset()
			select {
			case <-self.ctx.Done():
				return
			case <-reconnect.After():
			}
			continue
		}
		budgetWaiter.reset()
		// The signal is a persistent one-shot channel, so network changes or
		// remote restart requests cannot be lost if they race setup.
		immediateReconnect := conn.ImmediateReconnect()

		// at this point, the connection should be able to ping the other side
		// now we wait for the entire stream to be ready by propagating the `ReaderHeader`
		c := func() {
			defer conn.Close()

			handleCtx, handleCancel := context.WithCancel(self.ctx)
			defer handleCancel()
			var streamProbe *p2pStreamProbe
			if self.endpointProbe {
				streamProbe = newP2pStreamProbe(
					handleCtx,
					self.sendRouteManager,
					self.streamId,
					self.settings,
				)
				defer streamProbe.close()
			}

			// The peer's ready header must be consumed by exactly one reader
			// before the receive transport starts. Because the channel is
			// reliable-unordered, that reader also boundedly prefetched any
			// transfer frames delivered ahead of the marker.
			headerRead := make(chan [][]byte)
			setupReady := make(chan struct{})
			var routeWorkers sync.WaitGroup
			var p2pSendTransport *P2pSendTransport
			var p2pReceiveTransport *P2pReceiveTransport
			sendRouteRetired := make(chan struct{})
			routeWorkers.Add(2)

			go HandleError(func() {
				defer routeWorkers.Done()
				defer handleCancel()

				conn.SetWriteDeadline(time.Now().Add(self.settings.ConnectTimeout))
				_, err := conn.Write([]byte(ReadyHeader))
				if err != nil {
					self.client.log.V(1).Infof("[p2p]s(%s) ready header write err = %s\n", self.streamId, err)
					return
				}

				var prefetched [][]byte
				select {
				case <-handleCtx.Done():
					return
				case prefetched = <-headerRead:
				}

				var messageHandler func([]byte) bool
				if streamProbe != nil {
					messageHandler = streamProbe.handle
				}
				var receiveLanes []p2pReceiveRouteLane
				p2pReceiveTransport, receiveLanes = newP2pReceiveTransportWithLanes(
					handleCtx,
					handleCancel,
					conn,
					self.streamId,
					self.settings,
					prefetched,
					messageHandler,
				)
				if self.settings.afterReceiveTransportForTest != nil {
					self.settings.afterReceiveTransportForTest(p2pReceiveTransport)
				}

				var routeConnected atomic.Bool
				routeCallbacks := newLifecycleAdmission()
				applyRoute := func(connected bool) {
					routeInstalled := updateP2pConnectionReceiveRoutes(
						handleCtx,
						self.receiveRouteManager,
						receiveLanes,
						connected,
						p2pConnectionRouteTestHooks{
							beforeConnectedUpdate: func() {
								if self.settings.beforeRouteUpdateForTest != nil {
									self.settings.beforeRouteUpdateForTest(false)
								}
							},
							afterConnectedUpdate: func() {
								if self.settings.afterRouteUpdateForTest != nil {
									self.settings.afterRouteUpdateForTest(false)
								}
							},
						},
					)
					self.observeRouteState(
						&routeConnected,
						false,
						routeInstalled,
					)
				}
				updateRoute := func(connected bool) {
					if !routeCallbacks.start() {
						return
					}
					defer routeCallbacks.finish()
					applyRoute(connected)
				}
				unsub := conn.AddConnectedCallback(updateRoute)
				defer func() {
					unsub()
					routeCallbacks.close()
					if self.settings.beforeRouteCallbackJoinForTest != nil {
						self.settings.beforeRouteCallbackJoinForTest(false)
					}
					<-routeCallbacks.Done()
					applyRoute(false)
				}()

				select {
				case <-handleCtx.Done():
					return
				}
			}, handleCancel)

			go HandleError(func() {
				defer routeWorkers.Done()
				defer handleCancel()

				select {
				case <-handleCtx.Done():
					return
				default:
				}

				// A peer can install its send route as soon as it reads our
				// marker. On the reliable-unordered channel, its subsequent
				// transfer frame may arrive before its own marker after loss.
				// Recognize and boundedly carry those frames into the receive
				// route instead of treating them as a setup failure.
				conn.SetReadDeadline(time.Now().Add(self.settings.ConnectTimeout))
				prefetched, err := readP2pReadyHeader(conn, self.settings)
				if err != nil {
					self.client.log.V(1).Infof("[p2p]s(%s) ready header read err = %s\n", self.streamId, err)
					return
				}
				select {
				case <-handleCtx.Done():
					for _, message := range prefetched {
						MessagePoolReturn(message)
					}
					return
				case headerRead <- prefetched:
				}
				// Our header was already written before the other goroutine
				// waited on headerRead, and the peer's header is now consumed:
				// only this boundary means setup actually recovered. Merely
				// allocating a PeerConnection can still end in the same
				// 15-second header timeout and must not reset a stable failure
				// streak.
				close(setupReady)

				t, route := newP2pSendTransportForPeer(
					handleCtx,
					handleCancel,
					conn,
					self.peerId,
					self.streamId,
					self.settings,
					self.endpointProbe,
					sendRouteRetired,
				)
				p2pSendTransport = t.(*P2pSendTransport)
				if self.settings.afterSendTransportForTest != nil {
					self.settings.afterSendTransportForTest(p2pSendTransport)
				}
				var routeConnected atomic.Bool
				routeCallbacks := newLifecycleAdmission()
				applyRoute := func(connected bool) {
					if streamProbe != nil && !connected {
						streamProbe.clearSendRoute(t, route)
					}
					routeInstalled := updateP2pConnectionRoute(
						handleCtx,
						self.sendRouteManager,
						t,
						route,
						connected,
						p2pConnectionRouteTestHooks{
							beforeConnectedUpdate: func() {
								if self.settings.beforeRouteUpdateForTest != nil {
									self.settings.beforeRouteUpdateForTest(true)
								}
							},
							afterConnectedUpdate: func() {
								if self.settings.afterRouteUpdateForTest != nil {
									self.settings.afterRouteUpdateForTest(true)
								}
							},
						},
					)
					if streamProbe != nil && routeInstalled {
						streamProbe.setSendRoute(t, route)
					}
					self.observeRouteState(
						&routeConnected,
						true,
						routeInstalled,
					)
				}
				updateRoute := func(connected bool) {
					if !routeCallbacks.start() {
						return
					}
					defer routeCallbacks.finish()
					applyRoute(connected)
				}
				unsub := conn.AddConnectedCallback(updateRoute)
				defer func() {
					unsub()
					routeCallbacks.close()
					if self.settings.beforeRouteCallbackJoinForTest != nil {
						self.settings.beforeRouteCallbackJoinForTest(true)
					}
					<-routeCallbacks.Done()
					applyRoute(false)
					close(sendRouteRetired)
				}()

				select {
				case <-handleCtx.Done():
					return
				}
			}, handleCancel)

			markSetupReady := func() {
				if setupFailure.Recover() {
					self.client.log.V(1).Infof("[p2p]s(%s) <>%s setup recovered\n", self.streamId, self.peerId)
				}
			}
			select {
			case <-setupReady:
				markSetupReady()
				<-handleCtx.Done()
			case <-handleCtx.Done():
				// setupReady can close immediately before a route worker
				// cancels handleCtx. Preserve the real recovery transition
				// regardless of which ready select arm the scheduler chooses.
				select {
				case <-setupReady:
					markSetupReady()
				default:
				}
			}
			handleCancel()
			_ = conn.Close()
			routeWorkers.Wait()
			if p2pSendTransport != nil {
				<-p2pSendTransport.done
			}
			if p2pReceiveTransport != nil {
				<-p2pReceiveTransport.done
			}
		}

		c()
		select {
		case <-self.ctx.Done():
			return
		case <-immediateReconnect:
			// peer requested fresh negotiation; skip the backoff delay
		case <-reconnect.After():
		}
	}
}

// Close cancels this caller-owned association without joining its children.
func (self *P2pTransport) Close() {
	self.cancel()
}

// Done closes after endpoint readiness, installed routes, and physical send
// and receive child generations have all been removed.
func (self *P2pTransport) Done() <-chan struct{} {
	return self.done
}

// CloseAndWait cancels this caller-owned association and joins its complete
// route tree, or returns when ctx expires. It may be retried with a fresh ctx.
func (self *P2pTransport) CloseAndWait(ctx context.Context) error {
	self.Close()
	return waitForLifecycleDone(ctx, self.done, "P2P transport")
}

// Cancels the association and waits until its endpoint readiness lease and
// installed routes have been removed.
func (self *P2pTransport) close() {
	self.Close()
	<-self.done
}

// P2pRouteLifecycle is the caller-owned lifetime exposed by the deprecated
// low-level P2P route constructors. Callers must first stop route producers
// and remove the returned route from every RouteManager, then call
// CloseAndWait before reclaiming the route or connection. Close never closes
// the caller-owned connection.
type P2pRouteLifecycle interface {
	Transport
	Close()
	Done() <-chan struct{}
	CloseAndWait(context.Context) error
}

var (
	_ P2pRouteLifecycle = (*P2pSendTransport)(nil)
	_ P2pRouteLifecycle = (*P2pReceiveTransport)(nil)
)

type P2pSendTransport struct {
	transportId Id

	ctx       context.Context
	cancel    context.CancelFunc
	conn      net.Conn
	peerId    Id
	streamId  Id
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once

	endToEndReadinessRequired bool
	endToEndReady             atomic.Bool
	// probeSendAdmission joins direct endpoint-probe producers before the send
	// worker performs its final pooled-route drain.
	probeSendAdmission p2pProbeSendAdmission
	// Parent-owned routes close this only after RemoveTransport has joined all
	// admitted writer snapshots. Standalone transports leave it nil.
	routeRetired <-chan struct{}
	// Test seams are nil in production and expose the final probe admission
	// close/drain boundary without changing transport timing.
	testingAfterProbeSendAdmissionClosed func()
	testingBeforeRouteRetirementWait     func()
	testingAfterRouteRetirementWait      func()
	testingAfterProbeSendDrain           func()

	settings *P2pTransportSettings
}

// NewP2pSendTransport creates one caller-owned send route.
//
// Deprecated: prefer NewP2pTransport, which owns both directions and their
// RouteManager lifetime. Compatibility callers must assert the returned
// Transport to P2pRouteLifecycle and call CloseAndWait after quiescing and
// removing all route producers.
func NewP2pSendTransport(
	ctx context.Context,
	cancel context.CancelFunc,
	conn net.Conn,
	streamId Id,
	settings *P2pTransportSettings,
) (Transport, Route) {
	return NewP2pSendTransportForPeer(ctx, cancel, conn, Id{}, streamId, settings)
}

// NewP2pSendTransportForPeer creates one caller-owned P2P route that can be
// selected by both peer and stream.
//
// Deprecated: prefer NewP2pTransport, which owns both directions and their
// RouteManager lifetime. Compatibility callers must assert the returned
// Transport to P2pRouteLifecycle and call CloseAndWait after quiescing and
// removing all route producers.
func NewP2pSendTransportForPeer(
	ctx context.Context,
	cancel context.CancelFunc,
	conn net.Conn,
	peerId Id,
	streamId Id,
	settings *P2pTransportSettings,
) (Transport, Route) {
	return newP2pSendTransportForPeer(
		ctx,
		cancel,
		conn,
		peerId,
		streamId,
		settings,
		false,
		nil,
	)
}

// Builds one send generation. Endpoint stream transports begin ineligible;
// intermediary and direct compatibility transports begin ready.
func newP2pSendTransportForPeer(
	ctx context.Context,
	cancel context.CancelFunc,
	conn net.Conn,
	peerId Id,
	streamId Id,
	settings *P2pTransportSettings,
	endToEndReadinessRequired bool,
	routeRetired <-chan struct{},
) (Transport, Route) {
	send := make(chan []byte, settings.ChannelBufferSize)
	p2pSendTransport := &P2pSendTransport{
		transportId:               NewId(),
		ctx:                       ctx,
		cancel:                    cancel,
		conn:                      conn,
		peerId:                    peerId,
		streamId:                  streamId,
		send:                      send,
		done:                      make(chan struct{}),
		endToEndReadinessRequired: endToEndReadinessRequired,
		routeRetired:              routeRetired,
		settings:                  settings,
	}
	p2pSendTransport.probeSendAdmission.open = true
	if !endToEndReadinessRequired {
		p2pSendTransport.endToEndReady.Store(true)
	}
	go HandleError(p2pSendTransport.run, cancel)
	return p2pSendTransport, send
}

// Close cancels this send route without closing its caller-owned connection.
func (self *P2pSendTransport) Close() {
	self.closeOnce.Do(self.cancel)
}

// Done closes after all admitted probe producers and queued pooled messages
// have been joined and released.
func (self *P2pSendTransport) Done() <-chan struct{} {
	return self.done
}

// CloseAndWait cancels this send route and joins its owned worker. The caller
// must quiesce and remove external RouteManager producers first. A timed-out
// join may be retried with a fresh context.
func (self *P2pSendTransport) CloseAndWait(ctx context.Context) error {
	self.Close()
	return waitForLifecycleDone(ctx, self.done, "P2P send transport")
}

func (self *P2pSendTransport) run() {
	defer close(self.done)
	defer func() {
		self.probeSendAdmission.close()
		if self.testingAfterProbeSendAdmissionClosed != nil {
			self.testingAfterProbeSendAdmissionClosed()
		}
		self.cancel()
		self.probeSendAdmission.wait()
		if self.routeRetired != nil {
			if self.testingBeforeRouteRetirementWait != nil {
				self.testingBeforeRouteRetirementWait()
			}
			<-self.routeRetired
			if self.testingAfterRouteRetirementWait != nil {
				self.testingAfterRouteRetirementWait()
			}
		}
		// Drain any pooled bytes the route manager or an admitted endpoint probe
		// already enqueued before teardown closed both admission paths.
	drainProbeRoute:
		for {
			select {
			case b, ok := <-self.send:
				if !ok {
					break drainProbeRoute
				}
				MessagePoolReturn(b)
			default:
				break drainProbeRoute
			}
		}
		if self.testingAfterProbeSendDrain != nil {
			self.testingAfterProbeSendDrain()
		}
	}()

	pairRecorded := false
	for {
		select {
		case <-self.ctx.Done():
			return
		case transferFrameBytes, ok := <-self.send:
			if !ok {
				return
			}

			// The detached WebRTC data channel is message-oriented: one Write
			// becomes one whole SCTP user message the peer reads back whole, so
			// the SCTP message boundary frames each TransferFrame natively — no
			// length prefix. Enforce the max message size up front.
			if len(transferFrameBytes) > self.settings.MaxMessageByteCount {
				DefaultLogger().V(1).Infof("[p2p]s(%s) send message too large = %d\n", self.streamId, len(transferFrameBytes))
				MessagePoolReturn(transferFrameBytes)
				return
			}
			messageByteCount := len(transferFrameBytes)
			probeMessage := isP2pStreamProbe(transferFrameBytes)
			fastConn, supportsFastPath := self.conn.(webRtcFastPathConn)
			if self.settings.DataPlaneMode != P2pDataPlaneModeLegacyOnly {
				if supportsFastPath &&
					self.settings.DataPlaneMode == P2pDataPlaneModeFastOnly &&
					!fastConn.FastPathReady() {
					fastConn.WaitFastPathReady(self.ctx, self.settings.ConnectTimeout)
				}
				if supportsFastPath && fastConn.FastPathReady() {
					fragmentCount, err := fastConn.WriteFastPathMessage(transferFrameBytes)
					if err == nil {
						if stats := self.settings.DataPlaneStats; stats != nil && !probeMessage {
							if !pairRecorded {
								recordP2pSelectedPair(stats, self.conn)
								pairRecorded = true
							}
							stats.fastSendMessageCount.Add(1)
							stats.fastSendByteCount.Add(uint64(messageByteCount))
							stats.fastSendFragmentCount.Add(uint64(fragmentCount))
						}
						MessagePoolReturn(transferFrameBytes)
						continue
					}
					if stats := self.settings.DataPlaneStats; stats != nil && !probeMessage {
						stats.fastFallbackCount.Add(1)
					}
					if self.settings.DataPlaneMode == P2pDataPlaneModeFastOnly {
						MessagePoolReturn(transferFrameBytes)
						DefaultLogger().V(1).Infof("[p2p]s(%s) fast send err = %s\n", self.streamId, err)
						return
					}
				} else {
					if stats := self.settings.DataPlaneStats; stats != nil && !probeMessage {
						stats.fastFallbackCount.Add(1)
					}
					if self.settings.DataPlaneMode == P2pDataPlaneModeFastOnly {
						MessagePoolReturn(transferFrameBytes)
						DefaultLogger().V(1).Infof("[p2p]s(%s) fast path was not negotiated\n", self.streamId)
						return
					}
				}
			}

			self.conn.SetWriteDeadline(time.Now().Add(self.settings.WriteTimeout))
			nw, err := self.conn.Write(transferFrameBytes)
			MessagePoolReturn(transferFrameBytes)
			if nw < messageByteCount && err == nil {
				err = io.ErrShortWrite
			}
			if err != nil {
				DefaultLogger().V(1).Infof("[p2p]s(%s) send write err = %s\n", self.streamId, err)
				return
			}
			if stats := self.settings.DataPlaneStats; stats != nil && !probeMessage {
				if !pairRecorded {
					recordP2pSelectedPair(stats, self.conn)
					pairRecorded = true
				}
				stats.legacySendMessageCount.Add(1)
				stats.legacySendByteCount.Add(uint64(messageByteCount))
			}
		}
	}
}

func (self *P2pSendTransport) TransportId() Id {
	return self.transportId
}

func (self *P2pSendTransport) TransportType() TransportType {
	return TransportTypeP2p
}

// lower priority takes precedence
func (self *P2pSendTransport) Priority() int {
	// p2p routes have highest priority
	return 0
}

func (self *P2pSendTransport) Weight() float32 {
	// p2p routes have highest weight
	return 1.0
}

func (self *P2pSendTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}

func (self *P2pSendTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	// p2p routes have highest weight
	return 1.0
}

func (self *P2pSendTransport) MatchesSend(destination TransferPath) bool {
	if self.endToEndReadinessRequired && !self.endToEndReady.Load() {
		return false
	}
	if destination.StreamId == self.streamId {
		return true
	}
	// the stream terminates at the peer,
	// so any destination addressed to the peer matches the stream transport.
	// the peer id must be non-zero so that a missing peer never matches
	// destination masks without a destination id (e.g. control or pure stream masks)
	return self.peerId != (Id{}) && destination.DestinationId == self.peerId
}

// Changes endpoint eligibility after an exact-stream challenge round trip.
// The caller rematches this transport in its RouteManager on a true return.
func (self *P2pSendTransport) setEndToEndReady(ready bool) bool {
	return self.endToEndReady.Swap(ready) != ready
}

func (self *P2pSendTransport) MatchesReceive(destination TransferPath) bool {
	return false
}

func (self *P2pSendTransport) Downgrade(source TransferPath) {
	if source.StreamId == self.streamId {
		self.cancel()
		return
	}
	// mirror `MatchesSend`: the stream terminates at the peer, so an
	// audit/degrade signal for the peer must also shed this transport,
	// not just signals for the stream. The peer id must be non-zero so
	// that a missing peer never matches paths without a destination id.
	if self.peerId != (Id{}) && source.DestinationId == self.peerId {
		self.cancel()
	}
}

type P2pReceiveTransport struct {
	transportId Id

	ctx      context.Context
	cancel   context.CancelFunc
	conn     net.Conn
	streamId Id
	// receive is the reliable SCTP route. unreliableReceive is either the same
	// compatibility route or a distinct production native-datagram route.
	receive           chan []byte
	unreliableReceive chan []byte
	// pendingReceive is only the native SRTP carrier-reader handoff. A separate
	// forwarding worker may wait on RouteManager, but the datagram pump never
	// does. Reliable SCTP bypasses it and applies direct bounded backpressure.
	pendingReceive             chan []byte
	pendingReceiveMessageCount atomic.Int64
	pendingReceiveMessageLimit int64
	pendingReceiveByteCount    atomic.Int64
	pendingReceiveByteLimit    int64
	done                       chan struct{}
	closeOnce                  sync.Once
	// messageHandler consumes endpoint-only raw stream control before Client.
	messageHandler func([]byte) bool
	prefetched     [][]byte

	settings *P2pTransportSettings
	// Test-only barrier reached after reliable immediate admission finds a full
	// route and before the cancellation-bounded wait begins.
	beforeReliableReceiveWaitForTest func()
	// Nil test barrier pauses after pooled receive drain and before done.
	testingBeforeDoneForTest func()
}

// NewP2pReceiveTransport creates one caller-owned receive route.
//
// Deprecated: prefer NewP2pTransport, which owns both directions and their
// RouteManager lifetime. Compatibility callers must assert the returned
// Transport to P2pRouteLifecycle and call CloseAndWait after quiescing and
// removing all route consumers.
func NewP2pReceiveTransport(
	ctx context.Context,
	cancel context.CancelFunc,
	conn net.Conn,
	streamId Id,
	settings *P2pTransportSettings,
) (Transport, Route) {
	return newP2pReceiveTransport(ctx, cancel, conn, streamId, settings, nil, nil)
}

func newP2pReceiveTransport(
	ctx context.Context,
	cancel context.CancelFunc,
	conn net.Conn,
	streamId Id,
	settings *P2pTransportSettings,
	prefetched [][]byte,
	messageHandler func([]byte) bool,
) (Transport, Route) {
	transport, _ := newP2pReceiveTransportCore(
		ctx,
		cancel,
		conn,
		streamId,
		settings,
		prefetched,
		messageHandler,
		false,
	)
	return transport, transport.receive
}

func newP2pReceiveTransportWithLanes(
	ctx context.Context,
	cancel context.CancelFunc,
	conn net.Conn,
	streamId Id,
	settings *P2pTransportSettings,
	prefetched [][]byte,
	messageHandler func([]byte) bool,
) (*P2pReceiveTransport, []p2pReceiveRouteLane) {
	transport, lanes := newP2pReceiveTransportCore(
		ctx,
		cancel,
		conn,
		streamId,
		settings,
		prefetched,
		messageHandler,
		true,
	)
	return transport, lanes
}

func newP2pReceiveTransportCore(
	ctx context.Context,
	cancel context.CancelFunc,
	conn net.Conn,
	streamId Id,
	settings *P2pTransportSettings,
	prefetched [][]byte,
	messageHandler func([]byte) bool,
	splitLanes bool,
) (*P2pReceiveTransport, []p2pReceiveRouteLane) {
	receive := make(chan []byte)
	unreliableReceive := receive
	if splitLanes {
		unreliableReceive = make(chan []byte)
	}
	p2pReceiveTransport := &P2pReceiveTransport{
		transportId:                NewId(),
		ctx:                        ctx,
		cancel:                     cancel,
		conn:                       conn,
		streamId:                   streamId,
		receive:                    receive,
		unreliableReceive:          unreliableReceive,
		pendingReceive:             make(chan []byte, p2pReceiveQueueMessageCount(settings)),
		pendingReceiveMessageLimit: int64(p2pReceiveQueueMessageCount(settings)),
		pendingReceiveByteLimit:    int64(p2pReceiveQueueByteCount(settings)),
		done:                       make(chan struct{}),
		messageHandler:             messageHandler,
		prefetched:                 prefetched,
		settings:                   settings,
	}
	go HandleError(p2pReceiveTransport.run, cancel)
	lanes := []p2pReceiveRouteLane{{
		transport:   p2pReceiveTransport,
		route:       receive,
		reliability: CarrierReliabilityReliable,
	}}
	if splitLanes {
		lanes = append(lanes, p2pReceiveRouteLane{
			transport:   newReceiveLaneTransport(p2pReceiveTransport),
			route:       unreliableReceive,
			reliability: CarrierReliabilityUnreliable,
		})
	}
	return p2pReceiveTransport, lanes
}

// offerReceive transfers one complete Transfer frame according to its exact
// physical lane. Reliable SCTP waits directly for route capacity/cancellation;
// native SRTP enters the bounded zero-wait queue consumed by runReceiveQueue.
func (self *P2pReceiveTransport) offerReceive(
	message []byte,
	fast bool,
	fragmentCount int,
	probeMessage bool,
	countDeliveredStats bool,
) bool {
	if !fast {
		// Keep the ready path nonblocking and give tests an exact full-route
		// barrier. The second select is the only cancellation-bounded wait.
		select {
		case <-self.ctx.Done():
			MessagePoolReturn(message)
			return false
		default:
		}
		select {
		case <-self.ctx.Done():
			MessagePoolReturn(message)
			return false
		case self.receive <- message:
			if stats := self.settings.DataPlaneStats; stats != nil &&
				!probeMessage && countDeliveredStats {
				stats.legacyReceiveMessageCount.Add(1)
				stats.legacyReceiveByteCount.Add(uint64(len(message)))
			}
			return true
		default:
		}
		if self.beforeReliableReceiveWaitForTest != nil {
			self.beforeReliableReceiveWaitForTest()
		}
		select {
		case <-self.ctx.Done():
			MessagePoolReturn(message)
			return false
		case self.receive <- message:
			if stats := self.settings.DataPlaneStats; stats != nil &&
				!probeMessage && countDeliveredStats {
				stats.legacyReceiveMessageCount.Add(1)
				stats.legacyReceiveByteCount.Add(uint64(len(message)))
			}
			return true
		}
	}
	if !self.reservePendingReceive(len(message)) {
		self.recordReceiveQueueDrop(message, fast, probeMessage)
		return true
	}
	select {
	case <-self.ctx.Done():
		self.releasePendingReceive(len(message))
		MessagePoolReturn(message)
		return false
	case self.pendingReceive <- message:
		if stats := self.settings.DataPlaneStats; stats != nil && !probeMessage && countDeliveredStats {
			if fast {
				stats.fastReceiveMessageCount.Add(1)
				stats.fastReceiveByteCount.Add(uint64(len(message)))
				stats.fastReceiveFragmentCount.Add(uint64(max(0, fragmentCount)))
			}
		}
		return true
	default:
		self.releasePendingReceive(len(message))
		self.recordReceiveQueueDrop(message, fast, probeMessage)
		return true
	}
}

func (self *P2pReceiveTransport) recordReceiveQueueDrop(
	message []byte,
	fast bool,
	probeMessage bool,
) {
	if stats := self.settings.DataPlaneStats; stats != nil && !probeMessage {
		if fast {
			stats.fastReceiveQueueDropCount.Add(1)
			stats.fastReceiveQueueDropByteCount.Add(uint64(len(message)))
			stats.fastDropCount.Add(1)
		} else {
			stats.legacyReceiveQueueDropCount.Add(1)
			stats.legacyReceiveQueueDropByteCount.Add(uint64(len(message)))
		}
	}
	MessagePoolReturn(message)
}

func (self *P2pReceiveTransport) reservePendingReceive(byteCount int) bool {
	if byteCount <= 0 {
		return false
	}
	for {
		current := self.pendingReceiveMessageCount.Load()
		if self.pendingReceiveMessageLimit <= current {
			return false
		}
		if self.pendingReceiveMessageCount.CompareAndSwap(current, current+1) {
			break
		}
	}
	delta := int64(byteCount)
	for {
		current := self.pendingReceiveByteCount.Load()
		if self.pendingReceiveByteLimit < current+delta {
			self.releasePendingReceiveMessage()
			return false
		}
		if self.pendingReceiveByteCount.CompareAndSwap(current, current+delta) {
			return true
		}
	}
}

func (self *P2pReceiveTransport) releasePendingReceive(byteCount int) {
	if byteCount <= 0 {
		return
	}
	if remaining := self.pendingReceiveByteCount.Add(-int64(byteCount)); remaining < 0 {
		panic("negative P2P receive queue byte count")
	}
	self.releasePendingReceiveMessage()
}

func (self *P2pReceiveTransport) releasePendingReceiveMessage() {
	if remaining := self.pendingReceiveMessageCount.Add(-1); remaining < 0 {
		panic("negative P2P receive queue message count")
	}
}

// The only native-datagram worker allowed to wait for RouteManager consumption.
// The SRTP reader enqueues to pendingReceive with a zero-wait send.
func (self *P2pReceiveTransport) runReceiveQueue() {
	for {
		select {
		case <-self.ctx.Done():
			return
		case message := <-self.pendingReceive:
			select {
			case <-self.ctx.Done():
				self.releasePendingReceive(len(message))
				MessagePoolReturn(message)
				return
			case self.unreliableReceive <- message:
				self.releasePendingReceive(len(message))
			}
		}
	}
}

// Close cancels this receive route and interrupts its read without closing the
// caller-owned connection. Setting an expired deadline is safe for the paired
// direction and makes a blocked compatibility net.Conn return promptly.
func (self *P2pReceiveTransport) Close() {
	self.closeOnce.Do(func() {
		self.cancel()
		_ = self.conn.SetReadDeadline(time.Now())
	})
}

// Done closes after both receive lanes and all queued pooled messages have
// been joined and released.
func (self *P2pReceiveTransport) Done() <-chan struct{} {
	return self.done
}

// CloseAndWait cancels this receive route and joins its owned workers. A
// timed-out join may be retried with a fresh context.
func (self *P2pReceiveTransport) CloseAndWait(ctx context.Context) error {
	self.Close()
	return waitForLifecycleDone(ctx, self.done, "P2P receive transport")
}

func (self *P2pReceiveTransport) run() {
	defer close(self.done)
	var receiveWorkers sync.WaitGroup
	receiveWorkers.Add(1)
	go HandleError(func() {
		defer receiveWorkers.Done()
		self.runReceiveQueue()
	}, self.cancel)
	if fastConn, ok := self.conn.(webRtcFastPathConn); ok &&
		self.settings.DataPlaneMode != P2pDataPlaneModeLegacyOnly {
		receiveWorkers.Add(1)
		go HandleError(func() {
			defer receiveWorkers.Done()
			self.runFast(fastConn)
		}, self.cancel)
	}
	// drain any pooled bytes we wrote that the route manager hasn't consumed
	// yet at shutdown.
	defer func() {
		self.cancel()
		receiveWorkers.Wait()
		defer func() {
			if self.testingBeforeDoneForTest != nil {
				self.testingBeforeDoneForTest()
			}
		}()
		for {
			select {
			case b := <-self.pendingReceive:
				self.releasePendingReceive(len(b))
				MessagePoolReturn(b)
			default:
				return
			}
		}
	}()

	// The reliable-unordered ready-header reader may observe complete SCTP
	// messages before the marker. Deliver them only after the route worker has
	// started; constructor-time direct delivery would deadlock before RouteManager
	// can publish the lane. Cancellation returns the untouched suffix exactly.
	prefetched := self.prefetched
	self.prefetched = nil
	for messageIndex, message := range prefetched {
		if self.messageHandler != nil && self.messageHandler(message) {
			MessagePoolReturn(message)
			continue
		}
		if !self.offerReceive(
			message,
			false,
			0,
			isP2pStreamProbe(message),
			false,
		) {
			for _, remaining := range prefetched[messageIndex+1:] {
				MessagePoolReturn(remaining)
			}
			return
		}
	}

	// The detached WebRTC data channel is message-oriented. Read directly into
	// the pooled buffer whose ownership is handed to the receive route. The
	// normal transfer batch fits the first 4 KiB attempt. A larger
	// compatibility message retries boundedly; detached Pion masks SCTP's
	// required length, so readP2pMessage grows geometrically when n is zero.
	maxReadByteCount := max(1, self.settings.MaxMessageByteCount)
	initialReadByteCount := self.settings.InitialReadBufferByteCount
	if initialReadByteCount <= 0 {
		initialReadByteCount = min(4*1024, maxReadByteCount)
	}
	initialReadByteCount = min(initialReadByteCount, maxReadByteCount)
	// The ready-header handshake leaves a finite deadline on the shared
	// net.Conn. Clear it once before entering the steady-state reader.
	// Connection liveness comes from ICE/PeerConnection failed-state
	// cancellation; rearming an otherwise ignored 15-second read timeout only
	// woke every idle peer forever.
	if self.settings.beforeReceiveSteadyStateDeadlineClearForTest != nil {
		self.settings.beforeReceiveSteadyStateDeadlineClearForTest()
	}
	self.conn.SetReadDeadline(time.Time{})

	for {
		// Close expires the read deadline after canceling. Cancellation may win
		// immediately before the steady-state clear above, so this one exact
		// pre-read guard prevents that clear from losing the interrupt and
		// entering an unbounded read. It also gates every subsequent read.
		if self.ctx.Err() != nil {
			return
		}
		transferFrameBytes, err := readP2pMessage(
			self.conn,
			initialReadByteCount,
			initialReadByteCount,
			maxReadByteCount,
		)
		if 0 < len(transferFrameBytes) {
			probeMessage := isP2pStreamProbe(transferFrameBytes)
			if self.messageHandler != nil && self.messageHandler(transferFrameBytes) {
				MessagePoolReturn(transferFrameBytes)
				continue
			}
			if self.settings.DataPlaneMode == P2pDataPlaneModeFastOnly {
				MessagePoolReturn(transferFrameBytes)
				if stats := self.settings.DataPlaneStats; stats != nil && !probeMessage {
					stats.fastDropCount.Add(1)
				}
				continue
			}
			// The route owns the exact slice only when immediate admission wins.
			if !self.offerReceive(transferFrameBytes, false, 0, probeMessage, true) {
				return
			}
		}
		if err != nil {
			// A non-WebRTC compatibility conn may still surface a deadline.
			// Do not mistake an idle timeout for a dead route.
			if errors.Is(err, os.ErrDeadlineExceeded) {
				continue
			}
			DefaultLogger().V(1).Infof("[p2p]s(%s) receive read err = %s\n", self.streamId, err)
			return
		}
	}
}

// runFast transfers complete datagram-lane messages into the shared receive
// route. It never propagates route backpressure into the native SRTP reader.
func (self *P2pReceiveTransport) runFast(conn webRtcFastPathConn) {
	messages := conn.FastPathMessages()
	for {
		select {
		case <-self.ctx.Done():
			return
		case received, ok := <-messages:
			if !ok {
				return
			}
			probeMessage := isP2pStreamProbe(received.message)
			if self.messageHandler != nil && self.messageHandler(received.message) {
				MessagePoolReturn(received.message)
				continue
			}
			if !self.offerReceive(
				received.message,
				true,
				received.fragmentCount,
				probeMessage,
				true,
			) {
				return
			}
		}
	}
}

func (self *P2pReceiveTransport) TransportId() Id {
	return self.transportId
}

func (self *P2pReceiveTransport) TransportType() TransportType {
	return TransportTypeP2p
}

// lower priority takes precedence
func (self *P2pReceiveTransport) Priority() int {
	// p2p routes have highest priority
	return 0
}

func (self *P2pReceiveTransport) Weight() float32 {
	// p2p routes have highest weight
	return 1.0
}

func (self *P2pReceiveTransport) CanEvalRouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) bool {
	return true
}

func (self *P2pReceiveTransport) RouteWeight(stats *RouteStats, remainingStats map[Transport]*RouteStats) float32 {
	// p2p routes have highest weight
	return 1.0
}

func (self *P2pReceiveTransport) MatchesSend(destination TransferPath) bool {
	return false
}

func (self *P2pReceiveTransport) MatchesReceive(destination TransferPath) bool {
	return true
}

func (self *P2pReceiveTransport) Downgrade(source TransferPath) {
	if source.StreamId == self.streamId {
		self.cancel()
	}
}
