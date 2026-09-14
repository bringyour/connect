package connect

import (
	"net/netip"
	"slices"
	"sync"
	"time"

	"maps"
)

// events surfaced to the end user

// the callback only received the `providerEvent` diffs. To get the full list of provider events, use `Events`
type MonitorEventFunction = func(windowExpandEvent *WindowExpandEvent, providerEvents map[Id]*ProviderEvent, reset bool)

type WindowExpandEvent struct {
	// EventTime   time.Time
	// CurrentSize int
	TargetSize   int
	MinSatisfied bool
	// Reason is the machine-readable stall diagnosis while the window is still
	// forming: one of the WindowStall* constants (evaluating,
	// platform-unreachable, providers-unresponsive, rate-limited,
	// auth-failing). Derived by the window from the dominant recent evaluation
	// failure class; empty (a legacy sender, or a bare fixture) reads as
	// evaluating. See ip_remote_multi_client_outcome.go.
	Reason string
	// Failed is the terminal outcome state: the window hit its outcome
	// deadline twice with zero providers Added. Cleared when a provider lands.
	Failed bool
	// Ipv6Available is whether the window holds an added, unwarned exit that
	// can carry v6 (IPV6.md B6). Merged across windows by OR. While false the
	// apps can tell the user v6 is not carried, the in-tunnel resolver
	// answers AAAA empty, and v6 flows are answered with no-route.
	Ipv6Available bool
}

// provider state machine is:
// ProviderStateInEvaluation
//
//	-> ProviderStateEvaluationFailed (terminal)
//	-> ProviderStateNotAdded (terminal)
//	-> ProviderStateAdded
//	  -> ProviderStateRemoved (terminal)
type ProviderState string

const (
	ProviderStateInEvaluation     ProviderState = "InEvaluation"
	ProviderStateEvaluationFailed ProviderState = "EvaluationFailed"
	ProviderStateNotAdded         ProviderState = "NotAdded"
	ProviderStateAdded            ProviderState = "Added"
	ProviderStateRemoved          ProviderState = "Removed"
)

func (self ProviderState) IsTerminal() bool {
	switch self {
	case ProviderStateEvaluationFailed, ProviderStateNotAdded, ProviderStateRemoved:
		return true
	default:
		return false
	}
}

func (self ProviderState) IsActive() bool {
	switch self {
	case ProviderStateAdded:
		return true
	default:
		return false
	}
}

type ProviderEvent struct {
	// EventTime is when this state was entered. For `ProviderStateAdded` this
	// is the moment the provider became routing-eligible — the connected-since
	// time surfaced to users.
	EventTime time.Time
	// ClientId is the local window client id minted for this window slot.
	ClientId Id
	State    ProviderState
	// EgressClientId is the provider (egress) client id — the destination
	// tail. This is the id that identifies the provider to the user;
	// `ClientId` does not.
	EgressClientId Id
	// Location is the egress provider's location. nil when unknown. Immutable
	// — events are shallow-cloned and the pointee is shared.
	Location *ProviderLocation
	// IpFamily is the egress provider's address-family category. Legacy
	// (empty) reads as v4-only. This is what the apps' histogram and provider
	// rows show.
	IpFamily IpFamily
	// ExtenderIps are the extender addresses carrying this client's live
	// platform transports to the exit right now (K1) -- usually none or one,
	// briefly two across a transport migration, none over a P2P route. They
	// are the local client's extenders and never the provider's own. Events
	// are shallow-cloned, so the slice is copied on every write and treated
	// as immutable by readers.
	ExtenderIps []netip.Addr
}

func DefaultRemoteUserNatMultiClientMonitorSettings() *RemoteUserNatMultiClientMonitorSettings {
	return &RemoteUserNatMultiClientMonitorSettings{
		// EventWindowDuration: 120 * time.Second,
		CallbackPendingProviderEventMaxCount: 64,
	}
}

type RemoteUserNatMultiClientMonitorSettings struct {
	// EventWindowDuration time.Duration
	// CallbackPendingProviderEventMaxCount bounds the per-listener diff set
	// while that listener is slow or blocked. Once the bound is crossed, the
	// pending diffs are replaced by a current reset snapshot. This keeps
	// maintenance publishers non-blocking without allowing an app suspended
	// in the background (or a stuck server observer) to grow memory forever.
	// Values <= 0 use the default.
	CallbackPendingProviderEventMaxCount int
}

type MultiClientMonitor interface {
	AddMonitorEventCallback(monitorEventCallback MonitorEventFunction) func()
	Events() (*WindowExpandEvent, map[Id]*ProviderEvent)
	WindowExpandEvent() *WindowExpandEvent
	ProviderEvents() map[Id]*ProviderEvent
}

// conforms to `MultiClientMonitor`
type RemoteUserNatMultiClientMonitor struct {
	settings *RemoteUserNatMultiClientMonitorSettings

	stateLock sync.Mutex

	windowExpandEvent      WindowExpandEvent
	clientIdProviderEvents map[Id]*ProviderEvent

	monitorEventCallbacks *CallbackList[*monitorEventCallbackWorker]
}

func NewRemoteUserNatMultiClientMonitorWithDefaults() *RemoteUserNatMultiClientMonitor {
	return NewRemoteUserNatMultiClientMonitor(DefaultRemoteUserNatMultiClientMonitorSettings())
}

func NewRemoteUserNatMultiClientMonitor(settings *RemoteUserNatMultiClientMonitorSettings) *RemoteUserNatMultiClientMonitor {
	if settings == nil {
		settings = DefaultRemoteUserNatMultiClientMonitorSettings()
	}
	return &RemoteUserNatMultiClientMonitor{
		settings: settings,
		windowExpandEvent: WindowExpandEvent{
			// EventTime:   time.Now(),
			// CurrentSize: 0,
			TargetSize:   0,
			MinSatisfied: false,
			Reason:       WindowStallEvaluating,
		},
		clientIdProviderEvents: map[Id]*ProviderEvent{},
		monitorEventCallbacks:  NewCallbackList[*monitorEventCallbackWorker](),
	}
}

func (self *RemoteUserNatMultiClientMonitor) AddMonitorEventCallback(monitorEventCallback MonitorEventFunction) func() {
	pendingMax := self.settings.CallbackPendingProviderEventMaxCount
	if pendingMax <= 0 {
		pendingMax = DefaultRemoteUserNatMultiClientMonitorSettings().CallbackPendingProviderEventMaxCount
	}
	worker := newMonitorEventCallbackWorker(
		monitorEventCallback,
		self.Events,
		pendingMax,
	)
	callbackId := self.monitorEventCallbacks.Add(worker)
	return func() {
		self.monitorEventCallbacks.Remove(callbackId)
		worker.Close()
	}
}

// monitorCallbackEvent is one bounded, coalesced callback delivery. A listener
// may block forever (for example, an iOS app-side reverse RPC after UIKit
// suspends the containing app), so callbacks must never run on the window's
// sole resize/enumeration maintenance goroutines.
type monitorCallbackEvent struct {
	windowExpandEvent *WindowExpandEvent
	providerEvents    map[Id]*ProviderEvent
	reset             bool
}

// monitorEventCallbackWorker gives each listener independent failure
// containment: at most one callback is in flight and one coalesced event is
// pending. A permanently blocked callback therefore costs one goroutine and a
// fixed-size state snapshot, not the multi-client maintenance loop or an
// unbounded goroutine/event backlog.
type monitorEventCallbackWorker struct {
	callback MonitorEventFunction
	snapshot func() (*WindowExpandEvent, map[Id]*ProviderEvent)

	pendingProviderEventMaxCount int

	stateLock sync.Mutex
	pending   *monitorCallbackEvent
	closed    bool

	notify chan struct{}
	done   chan struct{}
}

func newMonitorEventCallbackWorker(
	callback MonitorEventFunction,
	snapshot func() (*WindowExpandEvent, map[Id]*ProviderEvent),
	pendingProviderEventMaxCount int,
) *monitorEventCallbackWorker {
	worker := &monitorEventCallbackWorker{
		callback:                     callback,
		snapshot:                     snapshot,
		pendingProviderEventMaxCount: max(1, pendingProviderEventMaxCount),
		notify:                       make(chan struct{}, 1),
		done:                         make(chan struct{}),
	}
	go HandleError(worker.run)
	return worker
}

func cloneWindowExpandEvent(event *WindowExpandEvent) *WindowExpandEvent {
	if event == nil {
		return nil
	}
	cloned := *event
	return &cloned
}

func (self *monitorEventCallbackWorker) Dispatch(
	windowExpandEvent *WindowExpandEvent,
	providerEvents map[Id]*ProviderEvent,
	reset bool,
) {
	self.stateLock.Lock()
	if self.closed {
		self.stateLock.Unlock()
		return
	}

	if self.pending == nil || reset {
		self.pending = &monitorCallbackEvent{
			windowExpandEvent: cloneWindowExpandEvent(windowExpandEvent),
			providerEvents:    maps.Clone(providerEvents),
			reset:             reset,
		}
	} else {
		self.pending.windowExpandEvent = cloneWindowExpandEvent(windowExpandEvent)
		if self.pending.providerEvents == nil {
			self.pending.providerEvents = map[Id]*ProviderEvent{}
		}
		if self.pending.reset {
			// A pending reset is a full active-state snapshot. Fold subsequent
			// diffs into that state: terminal events delete; live states replace.
			for clientId, providerEvent := range providerEvents {
				if providerEvent == nil || providerEvent.State.IsTerminal() {
					delete(self.pending.providerEvents, clientId)
				} else {
					self.pending.providerEvents[clientId] = providerEvent
				}
			}
		} else {
			// Ordinary diffs are last-value per client, matching monitor state.
			maps.Copy(self.pending.providerEvents, providerEvents)
		}
	}

	// A blocked listener can see unbounded unique terminal client ids over
	// time. Collapse those diffs to the monitor's current active state once
	// the cap is crossed. Dispatchers never hold monitor.stateLock while
	// entering here, so taking a snapshot under this worker lock has no lock
	// inversion; concurrent dispatchers simply merge after the newer snapshot.
	if !self.pending.reset &&
		self.pendingProviderEventMaxCount < len(self.pending.providerEvents) &&
		self.snapshot != nil {
		windowSnapshot, providerSnapshot := self.snapshot()
		self.pending.windowExpandEvent = cloneWindowExpandEvent(windowSnapshot)
		self.pending.providerEvents = maps.Clone(providerSnapshot)
		self.pending.reset = true
	}
	self.stateLock.Unlock()

	select {
	case self.notify <- struct{}{}:
	default:
	}
}

func (self *monitorEventCallbackWorker) run() {
	for {
		select {
		case <-self.done:
			return
		case <-self.notify:
		}

		self.stateLock.Lock()
		event := self.pending
		self.pending = nil
		closed := self.closed
		self.stateLock.Unlock()
		if closed {
			return
		}
		if event != nil {
			// A panic is listener-local. In particular, it must not escape to
			// window.resize's HandleError handler, which cancels the whole
			// multi-client on an unexpected panic.
			HandleError(func() {
				self.callback(event.windowExpandEvent, event.providerEvents, event.reset)
			})
		}
	}
}

func (self *monitorEventCallbackWorker) Close() {
	self.stateLock.Lock()
	if self.closed {
		self.stateLock.Unlock()
		return
	}
	self.closed = true
	self.pending = nil
	close(self.done)
	self.stateLock.Unlock()
}

/*
func (self *RemoteUserNatMultiClientMonitor) event() {
	callbacks := self.monitorEventCallbacks.Get()
	if len(callbacks) == 0 {
		return
	}

	var windowExpandEvent *WindowExpandEvent
	clientIdProviderEvents := map[Id]*ProviderEvent{}

	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		self.coalesceProviderEvents()

		windowExpandEvent = self.windowExpandEvent
		for _, providerEvent := range self.providerEvents {
			clientIdProviderEvents[providerEvent.ClientId] = providerEvent
		}
	}()

	for _, callback := range callbacks {
		callback(windowExpandEvent, clientIdProviderEvents)
	}
}
*/

// must be called with `stateLock`
// func (self *RemoteUserNatMultiClientMonitor) coalesceProviderEvents() {
// 	windowStartTime := time.Now().Add(-self.settings.EventWindowDuration)

// 	i := 0
// 	for ; i < len(self.providerEvents) && self.providerEvents[i].EventTime.Before(windowStartTime); i += 1 {
// 		self.providerEvents[i] = nil
// 	}
// 	if 0 < i {
// 		self.providerEvents = self.providerEvents[i:]
// 	}
// }

func (self *RemoteUserNatMultiClientMonitor) Events() (*WindowExpandEvent, map[Id]*ProviderEvent) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	// make a copy
	windowExpandEvent := self.windowExpandEvent
	return &windowExpandEvent, maps.Clone(self.clientIdProviderEvents)
}

func (self *RemoteUserNatMultiClientMonitor) WindowExpandEvent() *WindowExpandEvent {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	// make a copy
	windowExpandEvent := self.windowExpandEvent
	return &windowExpandEvent

}

func (self *RemoteUserNatMultiClientMonitor) ProviderEvents() map[Id]*ProviderEvent {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	// make a copy
	return maps.Clone(self.clientIdProviderEvents)
}

func (self *RemoteUserNatMultiClientMonitor) AddWindowExpandEvent(minSatisfied bool, targetSize int, ipv6Available bool) {
	var windowExpandEvent WindowExpandEvent
	changed := false
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		windowExpandEvent = WindowExpandEvent{
			// EventTime:   time.Now(),
			// CurrentSize: currentSize,
			TargetSize:    targetSize,
			MinSatisfied:  minSatisfied,
			Ipv6Available: ipv6Available,
			// the stall diagnosis is carried, not owned, by the expand event:
			// this call updates the size half only (SetStallStatus owns the
			// other half)
			Reason: self.windowExpandEvent.Reason,
			Failed: self.windowExpandEvent.Failed,
		}

		if self.windowExpandEvent != windowExpandEvent {
			self.windowExpandEvent = windowExpandEvent
			changed = true
		}
	}()

	if changed {
		if callbacks := self.monitorEventCallbacks.Get(); 0 < len(callbacks) {
			for _, callback := range callbacks {
				callback.Dispatch(&windowExpandEvent, nil, false)
			}
		}
	}
}

// SetStallStatus records the window's stall diagnosis (see
// ip_remote_multi_client_outcome.go) on the expand event and dispatches to
// listeners when it changed. Returns whether anything changed, so the caller
// can log the transition — once per change, never per pass.
func (self *RemoteUserNatMultiClientMonitor) SetStallStatus(reason string, failed bool) bool {
	var windowExpandEvent WindowExpandEvent
	changed := false
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		if self.windowExpandEvent.Reason != reason || self.windowExpandEvent.Failed != failed {
			self.windowExpandEvent.Reason = reason
			self.windowExpandEvent.Failed = failed
			changed = true
		}
		windowExpandEvent = self.windowExpandEvent
	}()

	if changed {
		if callbacks := self.monitorEventCallbacks.Get(); 0 < len(callbacks) {
			for _, callback := range callbacks {
				callback.Dispatch(&windowExpandEvent, nil, false)
			}
		}
	}
	return changed
}

// provider events are serialized per `clientId`
func (self *RemoteUserNatMultiClientMonitor) AddProviderEvent(clientId Id, state ProviderState, egressClientId Id, location *ProviderLocation, ipFamily IpFamily) {
	self.AddProviderEventWithExtenderIps(clientId, state, egressClientId, location, ipFamily, nil)
}

// AddProviderEventWithExtenderIps is the same event, carrying the extenders
// that were on this client's transports when it was raised (K1). A later
// change rides `SetProviderExtenderIps`; every event that replaces a live
// one must carry the current addresses, or the replacement would blank the
// dot's rings until the next transport change.
func (self *RemoteUserNatMultiClientMonitor) AddProviderEventWithExtenderIps(
	clientId Id,
	state ProviderState,
	egressClientId Id,
	location *ProviderLocation,
	ipFamily IpFamily,
	extenderIps []netip.Addr,
) {
	var windowExpandEvent WindowExpandEvent
	clientIdProviderEvents := map[Id]*ProviderEvent{}

	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		providerEvent := &ProviderEvent{
			EventTime:      time.Now(),
			ClientId:       clientId,
			State:          state,
			EgressClientId: egressClientId,
			Location:       location,
			IpFamily:       ipFamily,
			ExtenderIps:    slices.Clone(extenderIps),
		}

		// self.providerEvents = append(self.providerEvents, providerEvent)
		// self.coalesceProviderEvents()
		if state.IsTerminal() {
			delete(self.clientIdProviderEvents, clientId)
		} else {
			self.clientIdProviderEvents[clientId] = providerEvent
		}

		windowExpandEvent = self.windowExpandEvent
		clientIdProviderEvents[providerEvent.ClientId] = providerEvent
	}()

	if callbacks := self.monitorEventCallbacks.Get(); 0 < len(callbacks) {
		for _, callback := range callbacks {
			callback.Dispatch(&windowExpandEvent, clientIdProviderEvents, false)
		}
	}
}

// SetProviderExtenderIps rewrites the extender addresses of a provider's
// current event in place (K1) and dispatches the change. Like the family
// rewrite above, the event's state and EventTime are untouched: the provider
// is still Added and its connected-since time must not restart because the
// transport moved to another extender. Returns whether anything changed;
// unknown client ids and an unchanged set are no-ops.
func (self *RemoteUserNatMultiClientMonitor) SetProviderExtenderIps(clientId Id, extenderIps []netip.Addr) bool {
	var windowExpandEvent WindowExpandEvent
	clientIdProviderEvents := map[Id]*ProviderEvent{}
	changed := false

	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		providerEvent, ok := self.clientIdProviderEvents[clientId]
		if !ok || slices.Equal(providerEvent.ExtenderIps, extenderIps) {
			return
		}
		// shallow clone: events are shared with listeners by pointer, so the
		// slice is replaced rather than written through
		updated := *providerEvent
		updated.ExtenderIps = slices.Clone(extenderIps)
		self.clientIdProviderEvents[clientId] = &updated
		windowExpandEvent = self.windowExpandEvent
		clientIdProviderEvents[clientId] = &updated
		changed = true
	}()

	if changed {
		if callbacks := self.monitorEventCallbacks.Get(); 0 < len(callbacks) {
			for _, callback := range callbacks {
				callback.Dispatch(&windowExpandEvent, clientIdProviderEvents, false)
			}
		}
	}
	return changed
}

type MergedMultiClientMonitor struct {
	monitors []MultiClientMonitor
}

func NewMergedMultiClientMonitor(monitors []MultiClientMonitor) *MergedMultiClientMonitor {
	return &MergedMultiClientMonitor{
		monitors: monitors,
	}
}

func (self *MergedMultiClientMonitor) AddMonitorEventCallback(monitorEventCallback MonitorEventFunction) func() {
	settings := DefaultRemoteUserNatMultiClientMonitorSettings()
	worker := newMonitorEventCallbackWorker(
		monitorEventCallback,
		self.Events,
		settings.CallbackPendingProviderEventMaxCount,
	)
	c := func(_ *WindowExpandEvent, providerEvents map[Id]*ProviderEvent, reset bool) {
		windowExpandEvent := self.WindowExpandEvent()
		if reset {
			// An underlying monitor's bounded worker resets to that monitor's
			// snapshot. A merged listener reset must instead be a snapshot of
			// ALL windows, otherwise clearing the consumer would silently drop
			// the other window's live providers.
			windowExpandEvent, providerEvents = self.Events()
		}
		worker.Dispatch(windowExpandEvent, providerEvents, reset)
	}

	subs := []func(){}
	for _, monitor := range self.monitors {
		sub := monitor.AddMonitorEventCallback(c)
		subs = append(subs, sub)
	}
	return func() {
		for _, sub := range subs {
			sub()
		}
		worker.Close()
	}
}

func (self *MergedMultiClientMonitor) Events() (*WindowExpandEvent, map[Id]*ProviderEvent) {
	return self.WindowExpandEvent(), self.ProviderEvents()
}

func (self *MergedMultiClientMonitor) WindowExpandEvent() *WindowExpandEvent {
	netWindowExpandEvent := WindowExpandEvent{
		TargetSize:   0,
		MinSatisfied: false,
	}
	// the stall diagnosis merges by sharpness (stallReasonRank), and Failed
	// only when EVERY window that is actually trying (failed, or a non-zero
	// target) has failed — a disabled window (target 0 under a fixed-window
	// profile) must not veto, and a live window must
	trying := 0
	failed := 0
	for _, monitor := range self.monitors {
		windowExpandEvent := monitor.WindowExpandEvent()
		netWindowExpandEvent.TargetSize += windowExpandEvent.TargetSize
		netWindowExpandEvent.MinSatisfied = netWindowExpandEvent.MinSatisfied || windowExpandEvent.MinSatisfied
		netWindowExpandEvent.Ipv6Available = netWindowExpandEvent.Ipv6Available || windowExpandEvent.Ipv6Available
		if stallReasonRank(netWindowExpandEvent.Reason) < stallReasonRank(windowExpandEvent.Reason) {
			netWindowExpandEvent.Reason = windowExpandEvent.Reason
		}
		if windowExpandEvent.Failed {
			trying += 1
			failed += 1
		} else if 0 < windowExpandEvent.TargetSize {
			trying += 1
		}
	}
	if netWindowExpandEvent.Reason == "" {
		netWindowExpandEvent.Reason = WindowStallEvaluating
	}
	netWindowExpandEvent.Failed = 0 < failed && failed == trying && !netWindowExpandEvent.MinSatisfied
	return &netWindowExpandEvent
}

func (self *MergedMultiClientMonitor) ProviderEvents() map[Id]*ProviderEvent {
	netProviderEvents := map[Id]*ProviderEvent{}
	for _, monitor := range self.monitors {
		providerEvents := monitor.ProviderEvents()
		maps.Copy(netProviderEvents, providerEvents)
	}
	return netProviderEvents
}

// SetProviderIpFamily rewrites the address-family category of a provider's
// current event in place (IPV6.md B5, the local v6 downgrade) and dispatches
// the change. The event's state and EventTime are untouched: the provider is
// still Added and its connected-since time must not restart because its
// category was corrected. Returns whether anything changed; unknown client
// ids and unchanged categories are no-ops.
func (self *RemoteUserNatMultiClientMonitor) SetProviderIpFamily(clientId Id, ipFamily IpFamily) bool {
	var windowExpandEvent WindowExpandEvent
	clientIdProviderEvents := map[Id]*ProviderEvent{}
	changed := false

	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		providerEvent, ok := self.clientIdProviderEvents[clientId]
		if !ok || providerEvent.IpFamily == ipFamily {
			return
		}
		// shallow clone: events are shared with listeners by pointer
		updated := *providerEvent
		updated.IpFamily = ipFamily
		self.clientIdProviderEvents[clientId] = &updated
		windowExpandEvent = self.windowExpandEvent
		clientIdProviderEvents[clientId] = &updated
		changed = true
	}()

	if changed {
		if callbacks := self.monitorEventCallbacks.Get(); 0 < len(callbacks) {
			for _, callback := range callbacks {
				callback.Dispatch(&windowExpandEvent, clientIdProviderEvents, false)
			}
		}
	}
	return changed
}
