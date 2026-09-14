// End-to-end stream challenges keep locally connected but incomplete
// multihop paths out of destination-keyed application writers. The envelope
// starts with protobuf field tag zero, so no valid TransferFrame can collide.
package connect

import (
	"context"
	"slices"
	"sync"
	"time"
)

var p2pStreamProbePrefix = [...]byte{0, 'u', 'r', 's', 't', 'r', 1}

const (
	p2pStreamProbeRequestType  = byte(1)
	p2pStreamProbeResponseType = byte(2)
	p2pStreamProbeTypeOffset   = len(p2pStreamProbePrefix)
	p2pStreamProbeStreamOffset = p2pStreamProbeTypeOffset + 1
	p2pStreamProbeNonceOffset  = p2pStreamProbeStreamOffset + 16
	p2pStreamProbeByteCount    = p2pStreamProbeNonceOffset + 16

	// A fixed dispatcher bounds both goroutines and queued observer events even
	// when an application callback stalls forever.
	p2pStreamProbeObserverShardCount = 4
	p2pStreamProbeObserverQueueSize  = 64
)

// A concurrent lifecycle gate joins control-message producers before a
// generation or its physical send worker drains the route it owns.
type p2pProbeSendAdmission struct {
	stateLock sync.Mutex
	open      bool
	writers   sync.WaitGroup
}

// Admits one producer unless teardown already closed the owner.
func (self *p2pProbeSendAdmission) start() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if !self.open {
		return false
	}
	self.writers.Add(1)
	return true
}

// Releases one previously admitted producer.
func (self *p2pProbeSendAdmission) done() {
	self.writers.Done()
}

// Prevents later admission without waiting while a lifecycle lock is
// held. The caller separately joins existing producers.
func (self *p2pProbeSendAdmission) close() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.open = false
}

// Lets an already admitted producer drop instead of enqueueing after
// teardown has begun; the final join still closes the check-to-send race.
func (self *p2pProbeSendAdmission) isOpen() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.open
}

// Joins every producer admitted before close.
func (self *p2pProbeSendAdmission) wait() {
	self.writers.Wait()
}

// One immutable epoch binds direct probe sends and readiness route updates to
// an exact transport and route.
type p2pStreamProbeRouteGeneration struct {
	transport Transport
	route     Route
	epoch     uint64

	sendAdmission p2pProbeSendAdmission
	routeUpdates  sync.WaitGroup
}

// Creates one open route generation.
func newP2pStreamProbeRouteGeneration(
	transport Transport,
	route Route,
	epoch uint64,
) *p2pStreamProbeRouteGeneration {
	generation := &p2pStreamProbeRouteGeneration{
		transport: transport,
		route:     route,
		epoch:     epoch,
	}
	generation.sendAdmission.open = true
	return generation
}

// One immutable dispatch item carries an event and callback to a shared
// bounded observer worker.
type p2pStreamProbeObservation struct {
	observer func(P2pStreamProbeEvent)
	event    P2pStreamProbeEvent
}

// Fixed process-wide queues and workers isolate transport callbacks from
// optional observers. Dispatch is safe from concurrent endpoint workers.
type p2pStreamProbeObserverDispatcher struct {
	startOnce sync.Once
	shards    [p2pStreamProbeObserverShardCount]chan p2pStreamProbeObservation
}

var defaultP2pStreamProbeObserverDispatcher p2pStreamProbeObserverDispatcher

// Initializes the fixed observer workers on first non-nil observation.
func (self *p2pStreamProbeObserverDispatcher) start() {
	self.startOnce.Do(func() {
		for shardIndex := range self.shards {
			observations := make(
				chan p2pStreamProbeObservation,
				p2pStreamProbeObserverQueueSize,
			)
			self.shards[shardIndex] = observations
			go func() {
				for observation := range observations {
					HandleError(func() {
						observation.observer(observation.event)
					})
				}
			}()
		}
	})
}

// Queues one event without waiting; saturation drops diagnostics but
// never stalls a receive or route-lifecycle callback.
func (self *p2pStreamProbeObserverDispatcher) dispatch(
	observer func(P2pStreamProbeEvent),
	event P2pStreamProbeEvent,
) {
	self.start()
	shardIndex := int(event.StreamId[15]) % len(self.shards)
	select {
	case self.shards[shardIndex] <- p2pStreamProbeObservation{
		observer: observer,
		event:    event,
	}:
	default:
	}
}

// One endpoint connection generation owns the challenge nonce and readiness
// lease. Route callbacks and message handling are concurrent-safe and
// nonblocking; the lifecycle synchronously releases readiness on close.
type p2pStreamProbe struct {
	ctx    context.Context
	cancel context.CancelFunc

	routeManager *RouteManager
	streamId     Id
	interval     time.Duration
	timeout      time.Duration
	observer     func(P2pStreamProbeEvent)

	// routeLifecycleLock serializes detach/join/remove/install transitions.
	// Direct producers never acquire it, and stateLock is released before joins
	// or RouteManager publication.
	routeLifecycleLock sync.Mutex
	stateLock          sync.Mutex
	sendGeneration     *p2pStreamProbeRouteGeneration
	routeEpoch         uint64
	routeUpdate        *Monitor

	// Test seams are nil in production and pause only after the corresponding
	// ownership or readiness transition has become observable.
	testingAfterSendAdmission         func(byte, uint64, []byte)
	testingAfterReadyBitSet           func(Transport, Route, uint64)
	testingAfterRouteGenerationClosed func(uint64)
	testingNow                        func() time.Time
	testingProbeTimer                 <-chan time.Time
	testingAfterProbeTimerReset       func(time.Duration)
	testingAfterProbeTimerEdge        func()

	responses chan Id
	done      chan struct{}
}

// Returns the wall clock in production and a controllable clock in lifecycle
// tests that verify interval and compatibility-backoff state transitions.
func (self *p2pStreamProbe) now() time.Time {
	if self.testingNow != nil {
		return self.testingNow()
	}
	return time.Now()
}

// Starts one endpoint challenge lifecycle. A zero or undersized timing value
// is normalized so tests and legacy settings cannot create a busy loop.
func newP2pStreamProbe(
	ctx context.Context,
	routeManager *RouteManager,
	streamId Id,
	settings *P2pTransportSettings,
) *p2pStreamProbe {
	probeCtx, cancel := context.WithCancel(ctx)
	interval := settings.EndToEndProbeInterval
	if interval <= 0 {
		interval = time.Second
	}
	timeout := settings.EndToEndProbeTimeout
	if timeout < 2*interval {
		timeout = 2 * interval
	}
	probe := &p2pStreamProbe{
		ctx:          probeCtx,
		cancel:       cancel,
		routeManager: routeManager,
		streamId:     streamId,
		interval:     interval,
		timeout:      timeout,
		observer:     settings.EndToEndProbeObserver,
		routeUpdate:  NewMonitor(),
		responses:    make(chan Id, 1),
		done:         make(chan struct{}),
	}
	go HandleError(probe.run, cancel)
	return probe
}

// Publishes the exact P2P send route after the local ready-header exchange.
func (self *p2pStreamProbe) setSendRoute(sendTransport Transport, sendRoute Route) {
	self.routeLifecycleLock.Lock()
	defer self.routeLifecycleLock.Unlock()

	self.stateLock.Lock()
	retiredGeneration := self.sendGeneration
	if retiredGeneration != nil &&
		retiredGeneration.transport == sendTransport &&
		retiredGeneration.route == sendRoute {
		self.stateLock.Unlock()
		return
	}
	if retiredGeneration != nil {
		retiredGeneration.sendAdmission.close()
		self.sendGeneration = nil
		self.routeEpoch += 1
		self.routeUpdate.NotifyAll()
	}
	closedHook := self.testingAfterRouteGenerationClosed
	self.stateLock.Unlock()

	if retiredGeneration != nil {
		if closedHook != nil {
			closedHook(retiredGeneration.epoch)
		}
		self.retireSendGeneration(retiredGeneration)
	}

	self.stateLock.Lock()
	self.routeEpoch += 1
	generation := newP2pStreamProbeRouteGeneration(
		sendTransport,
		sendRoute,
		self.routeEpoch,
	)
	self.sendGeneration = generation
	self.routeUpdate.NotifyAll()
	self.stateLock.Unlock()
	self.observe(P2pStreamProbeEventRouteReady, Id{}, generation.epoch)
}

// Revokes the exact local generation before a later connected callback can
// reinstall it. The lifecycle receives the epoch edge and rotates its nonce.
func (self *p2pStreamProbe) clearSendRoute(sendTransport Transport, sendRoute Route) {
	self.routeLifecycleLock.Lock()
	defer self.routeLifecycleLock.Unlock()

	self.stateLock.Lock()
	generation := self.sendGeneration
	if generation == nil ||
		generation.transport != sendTransport ||
		generation.route != sendRoute {
		self.stateLock.Unlock()
		return
	}
	generation.sendAdmission.close()
	self.sendGeneration = nil
	self.routeEpoch += 1
	clearedEpoch := self.routeEpoch
	self.routeUpdate.NotifyAll()
	closedHook := self.testingAfterRouteGenerationClosed
	self.stateLock.Unlock()

	if closedHook != nil {
		closedHook(generation.epoch)
	}
	self.retireSendGeneration(generation)
	self.observe(P2pStreamProbeEventRouteCleared, Id{}, clearedEpoch)
}

// Joins direct sends and readiness publication before
// withdrawing the physical transport from all logical writers.
func (self *p2pStreamProbe) retireSendGeneration(
	generation *p2pStreamProbeRouteGeneration,
) {
	generation.sendAdmission.wait()
	generation.routeUpdates.Wait()
	self.withdrawGeneration(generation)
}

// Returns the current route epoch for diagnostics outside the locked mutation.
func (self *p2pStreamProbe) currentRouteEpoch() uint64 {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.routeEpoch
}

// Returns the current route, its generation, and the update edge armed at the
// same instant.
func (self *p2pStreamProbe) sendRouteState() (
	*p2pStreamProbeRouteGeneration,
	<-chan struct{},
) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	update := self.routeUpdate.NotifyChannel()
	return self.sendGeneration, update
}

// Admits one direct probe producer to both its logical
// generation and physical P2P route owner.
func (self *p2pStreamProbe) acquireSendGeneration(
	generation *p2pStreamProbeRouteGeneration,
	messageType byte,
	message []byte,
) (*P2pSendTransport, bool) {
	self.stateLock.Lock()
	if generation == nil || self.sendGeneration != generation ||
		!generation.sendAdmission.start() {
		self.stateLock.Unlock()
		return nil, false
	}
	p2pSendTransport, _ := generation.transport.(*P2pSendTransport)
	if p2pSendTransport != nil && !p2pSendTransport.probeSendAdmission.start() {
		generation.sendAdmission.done()
		self.stateLock.Unlock()
		return nil, false
	}
	hook := self.testingAfterSendAdmission
	self.stateLock.Unlock()

	if hook != nil {
		hook(messageType, generation.epoch, message)
	}
	return p2pSendTransport, true
}

// Releases both ownership gates acquired for one direct
// probe send.
func (self *p2pStreamProbe) releaseSendGeneration(
	generation *p2pStreamProbeRouteGeneration,
	p2pSendTransport *P2pSendTransport,
) {
	if p2pSendTransport != nil {
		p2pSendTransport.probeSendAdmission.done()
	}
	generation.sendAdmission.done()
}

// Transfers or returns one owned control buffer. A generation
// closed while a test or scheduler pause is in progress drops before enqueue.
func (self *p2pStreamProbe) sendProbeMessage(
	generation *p2pStreamProbeRouteGeneration,
	messageType byte,
	message []byte,
	nonce Id,
	queuedEvent string,
	droppedEvent string,
) bool {
	p2pSendTransport, admitted := self.acquireSendGeneration(
		generation,
		messageType,
		message,
	)
	if !admitted {
		MessagePoolReturn(message)
		self.observe(droppedEvent, nonce, self.currentRouteEpoch())
		return false
	}
	defer self.releaseSendGeneration(generation, p2pSendTransport)
	if !generation.sendAdmission.isOpen() ||
		p2pSendTransport != nil && !p2pSendTransport.probeSendAdmission.isOpen() {
		MessagePoolReturn(message)
		self.observe(droppedEvent, nonce, generation.epoch)
		return false
	}

	select {
	case <-self.ctx.Done():
		MessagePoolReturn(message)
		self.observe(droppedEvent, nonce, generation.epoch)
		return false
	case generation.route <- message:
		self.observe(queuedEvent, nonce, generation.epoch)
		return true
	default:
		MessagePoolReturn(message)
		self.observe(droppedEvent, nonce, generation.epoch)
		return false
	}
}

// Recognizes endpoint-only challenge traffic. Requests are echoed through the
// exact stream route without blocking this receive worker; responses are
// handed to the lifecycle through a bounded nonblocking notification.
func (self *p2pStreamProbe) handle(message []byte) bool {
	recognized, messageType, streamId, nonce := decodeP2pStreamProbe(message)
	if !recognized {
		return false
	}
	if streamId != self.streamId {
		return true
	}
	switch messageType {
	case p2pStreamProbeRequestType:
		self.observe(P2pStreamProbeEventRequestReceived, nonce, self.currentRouteEpoch())
		response := encodeP2pStreamProbe(
			p2pStreamProbeResponseType,
			self.streamId,
			nonce,
		)
		generation, _ := self.sendRouteState()
		self.sendProbeMessage(
			generation,
			p2pStreamProbeResponseType,
			response,
			nonce,
			P2pStreamProbeEventResponseQueued,
			P2pStreamProbeEventResponseDropped,
		)
	case p2pStreamProbeResponseType:
		self.observe(P2pStreamProbeEventResponseReceived, nonce, self.currentRouteEpoch())
		select {
		case self.responses <- nonce:
		default:
			self.observe(P2pStreamProbeEventResponseQueueFull, nonce, self.currentRouteEpoch())
		}
	}
	return true
}

// Emits one optional diagnostic without allocating when observation is off.
func (self *p2pStreamProbe) observe(eventType string, nonce Id, routeEpoch uint64) {
	if self.observer != nil {
		defaultP2pStreamProbeObserverDispatcher.dispatch(self.observer, P2pStreamProbeEvent{
			Type:       eventType,
			StreamId:   self.streamId,
			Nonce:      nonce,
			RouteEpoch: routeEpoch,
		})
	}
}

// Rematches exactly one endpoint send generation. Withdrawal removes it from
// logical writers immediately; a later successful challenge can re-register
// the same still-connected raw route.
func (self *p2pStreamProbe) setReady(
	sendTransport Transport,
	sendRoute Route,
	routeEpoch uint64,
	ready bool,
) bool {
	p2pSendTransport, ok := sendTransport.(*P2pSendTransport)
	if !ok {
		return false
	}
	self.stateLock.Lock()
	generation := self.sendGeneration
	if generation == nil ||
		generation.transport != sendTransport ||
		generation.route != sendRoute ||
		generation.epoch != routeEpoch {
		self.stateLock.Unlock()
		return false
	}
	if !p2pSendTransport.setEndToEndReady(ready) {
		self.stateLock.Unlock()
		return false
	}
	if ready {
		hook := self.testingAfterReadyBitSet
		self.stateLock.Unlock()
		if hook != nil {
			hook(sendTransport, sendRoute, routeEpoch)
		}

		self.stateLock.Lock()
		if self.sendGeneration != generation ||
			!p2pSendTransport.endToEndReady.Load() {
			self.stateLock.Unlock()
			return false
		}
		generation.routeUpdates.Add(1)
		self.stateLock.Unlock()
		// Readiness rematches the same physical generation. Preserve its
		// delivery semantics: a propertyless update here used to erase the
		// native RTP/SRTP lane's unreliable classification immediately after
		// the end-to-end probe succeeded, disabling Transfer flight control for
		// the entire useful lifetime of the P2P route.
		self.routeManager.UpdateTransportWithProperties(
			sendTransport,
			[]Route{sendRoute},
			p2pTransferCarrierProperties(sendTransport),
		)
		generation.routeUpdates.Done()
	} else {
		generation.routeUpdates.Add(1)
		self.stateLock.Unlock()
		self.routeManager.RemoveTransport(sendTransport)
		generation.routeUpdates.Done()
	}
	return true
}

// Clears eligibility independently of the lifecycle's local lease snapshot.
// Route removal is unconditional because a connected callback may already
// have registered an ineligible transport while the atomic bit was false.
func (self *p2pStreamProbe) withdrawGeneration(
	generation *p2pStreamProbeRouteGeneration,
) {
	if p2pSendTransport, ok := generation.transport.(*P2pSendTransport); ok {
		if p2pSendTransport.setEndToEndReady(false) {
			self.observe(
				P2pStreamProbeEventReadinessWithdrawn,
				Id{},
				generation.epoch,
			)
		}
	}
	self.routeManager.RemoveTransport(generation.transport)
}

// Challenges until one response returns, then maintains a renewable route
// lease. Each matching response starts a fresh quiet interval and keeps the
// nonce stable so duplicate responses drain before the next challenge. A
// missed middle hop expires the lease; local teardown cancels this worker and
// releases it synchronously through close.
func (self *p2pStreamProbe) run() {
	defer close(self.done)
	var ready bool
	var readyTransport Transport
	var readyRoute Route
	var readyRouteEpoch uint64
	defer func() {
		if ready {
			self.setReady(readyTransport, readyRoute, readyRouteEpoch, false)
		}
	}()
	probeTimer := time.NewTimer(0)
	defer probeTimer.Stop()
	probeTimerChannel := probeTimer.C
	if self.testingProbeTimer != nil {
		probeTimerChannel = self.testingProbeTimer
	}
	resetProbeTimer := func(interval time.Duration) {
		probeTimer.Reset(interval)
		if self.testingAfterProbeTimerReset != nil {
			self.testingAfterProbeTimerReset(interval)
		}
	}

	nonce := NewId()
	challengeMatched := false
	var challengeRouteEpoch uint64
	capabilityStartTime := self.now()
	lastResponseTime := time.Time{}
	probeInterval := self.interval
	for {
		generation, routeUpdate := self.sendRouteState()
		if ready {
			p2pSendTransport, ok := readyTransport.(*P2pSendTransport)
			if !ok || !p2pSendTransport.endToEndReady.Load() {
				ready = false
				readyTransport = nil
				readyRoute = nil
				readyRouteEpoch = 0
			}
		}
		if generation == nil {
			select {
			case <-self.ctx.Done():
				return
			case <-routeUpdate:
			}
			continue
		}
		sendTransport := generation.transport
		sendRoute := generation.route
		routeEpoch := generation.epoch
		if challengeRouteEpoch != routeEpoch {
			challengeRouteEpoch = routeEpoch
			nonce = NewId()
			challengeMatched = false
			capabilityStartTime = self.now()
			lastResponseTime = time.Time{}
			probeInterval = self.interval
		}

		if challengeMatched {
			nonce = NewId()
			challengeMatched = false
		}
		request := encodeP2pStreamProbe(
			p2pStreamProbeRequestType,
			self.streamId,
			nonce,
		)
		resetProbeTimer(probeInterval)
		self.sendProbeMessage(
			generation,
			p2pStreamProbeRequestType,
			request,
			nonce,
			P2pStreamProbeEventRequestQueued,
			P2pStreamProbeEventRequestDropped,
		)

		handleResponse := func(responseNonce Id) {
			if responseNonce == nonce {
				matchedNonce := nonce
				self.observe(P2pStreamProbeEventResponseMatched, matchedNonce, routeEpoch)
				lastResponseTime = self.now()
				probeInterval = self.interval
				challengeMatched = true
				resetProbeTimer(probeInterval)
				if !ready && self.setReady(sendTransport, sendRoute, routeEpoch, true) {
					ready = true
					readyTransport = sendTransport
					readyRoute = sendRoute
					readyRouteEpoch = routeEpoch
					self.observe(
						P2pStreamProbeEventReadinessGranted,
						matchedNonce,
						routeEpoch,
					)
				}
			} else {
				self.observe(P2pStreamProbeEventResponseStale, responseNonce, routeEpoch)
			}
		}
		waitForInterval := true
		for waitForInterval {
			select {
			case <-self.ctx.Done():
				return
			case <-routeUpdate:
				waitForInterval = false
			case responseNonce := <-self.responses:
				handleResponse(responseNonce)
			case <-probeTimerChannel:
				if self.testingAfterProbeTimerEdge != nil {
					self.testingAfterProbeTimerEdge()
				}
				// A response and its interval edge can become runnable together
				// when the round-trip time equals the probe interval. Prefer the
				// response so its duplicate challenge drains instead of creating
				// a permanent request/response train.
				select {
				case responseNonce := <-self.responses:
					handleResponse(responseNonce)
				default:
					waitForInterval = false
				}
			}
		}

		if ready && self.timeout <= self.now().Sub(lastResponseTime) {
			self.setReady(readyTransport, readyRoute, readyRouteEpoch, false)
			ready = false
			readyTransport = nil
			readyRoute = nil
			readyRouteEpoch = 0
			probeInterval = self.timeout
			self.observe(P2pStreamProbeEventReadinessWithdrawn, nonce, routeEpoch)
		}
		if lastResponseTime.IsZero() &&
			probeInterval != self.timeout &&
			self.timeout <= self.now().Sub(capabilityStartTime) {
			// An older endpoint consumes the invalid protobuf envelope but cannot
			// echo it. Retain compatibility fallback without one probe per second
			// forever; a sparse challenge still discovers a later upgrade.
			probeInterval = self.timeout
			self.observe(P2pStreamProbeEventCompatibilityBackoff, nonce, routeEpoch)
		}
	}
}

// Cancels the lifecycle and waits until any readiness lease is withdrawn.
func (self *p2pStreamProbe) close() {
	self.cancel()
	<-self.done
}

// Encodes one owned raw-stream envelope for the send route.
func encodeP2pStreamProbe(messageType byte, streamId Id, nonce Id) []byte {
	message := MessagePoolGet(p2pStreamProbeByteCount)
	copy(message, p2pStreamProbePrefix[:])
	message[p2pStreamProbeTypeOffset] = messageType
	copy(
		message[p2pStreamProbeStreamOffset:p2pStreamProbeNonceOffset],
		streamId.Bytes(),
	)
	copy(message[p2pStreamProbeNonceOffset:p2pStreamProbeByteCount], nonce.Bytes())
	return message
}

// Decodes only the reserved invalid-protobuf envelope. A recognized message
// with a mismatched stream is still consumed rather than reaching Client.
func decodeP2pStreamProbe(message []byte) (bool, byte, Id, Id) {
	if len(message) != p2pStreamProbeByteCount ||
		!slices.Equal(message[:len(p2pStreamProbePrefix)], p2pStreamProbePrefix[:]) {
		return false, 0, Id{}, Id{}
	}
	streamId, streamErr := IdFromBytes(
		message[p2pStreamProbeStreamOffset:p2pStreamProbeNonceOffset],
	)
	nonce, nonceErr := IdFromBytes(
		message[p2pStreamProbeNonceOffset:p2pStreamProbeByteCount],
	)
	if streamErr != nil || nonceErr != nil {
		return true, 0, Id{}, Id{}
	}
	return true, message[p2pStreamProbeTypeOffset], streamId, nonce
}

// Reports whether physical transport accounting should exclude this endpoint
// health envelope. Intermediaries still forward it unchanged.
func isP2pStreamProbe(message []byte) bool {
	recognized, _, _, _ := decodeP2pStreamProbe(message)
	return recognized
}
