package connect

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

func waitForTestCondition(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(message)
}

// stallGate is the reusable fault-injection primitive for callback boundaries:
// park one consumer indefinitely, then assert whether its producer is supposed
// to remain blocked (transfer backpressure) or continue (maintenance/events).
type stallGate struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

// callbackPanicTestLogger captures the expected recovery report without
// emitting it through the process-wide glog sink used by test runners.
type callbackPanicTestLogger struct {
	Logger
	warned chan struct{}
	once   sync.Once
}

func (self *callbackPanicTestLogger) Warningf(string, ...any) {
	self.once.Do(func() { close(self.warned) })
}

func newStallGate() *stallGate {
	return &stallGate{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
}

func (self *stallGate) Wait() {
	self.once.Do(func() { close(self.started) })
	<-self.release
}

func (self *stallGate) Release() {
	select {
	case <-self.release:
	default:
		close(self.release)
	}
}

func waitForStallStart(t *testing.T, gate *stallGate) {
	t.Helper()
	select {
	case <-gate.started:
	case <-time.After(time.Second):
		t.Fatal("injected stall did not start")
	}
}

func assertReturnsWithin(t *testing.T, timeout time.Duration, operation func(), message string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		operation()
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal(message)
	}
}

func assertStillBlocked(t *testing.T, done <-chan struct{}, duration time.Duration, message string) {
	t.Helper()
	select {
	case <-done:
		t.Fatal(message)
	case <-time.After(duration):
	}
}

// A monitor callback used to run inline from window.resize. If it blocked
// (notably an iOS reverse RPC whose containing app had been suspended), resize
// could never remove or add another peer. The worker must isolate the publisher,
// coalesce while blocked, and bound unique terminal ids with a reset snapshot.
func TestMultiClientMonitorBlockedCallbackCannotBlockMaintenance(t *testing.T) {
	settings := DefaultRemoteUserNatMultiClientMonitorSettings()
	settings.CallbackPendingProviderEventMaxCount = 2
	monitor := NewRemoteUserNatMultiClientMonitor(settings)

	block := make(chan struct{})
	started := make(chan struct{})
	delivered := make(chan monitorCallbackEvent, 4)
	var startOnce sync.Once
	unsub := monitor.AddMonitorEventCallback(func(
		windowExpandEvent *WindowExpandEvent,
		providerEvents map[Id]*ProviderEvent,
		reset bool,
	) {
		startOnce.Do(func() { close(started) })
		<-block
		delivered <- monitorCallbackEvent{
			windowExpandEvent: cloneWindowExpandEvent(windowExpandEvent),
			providerEvents:    providerEvents,
			reset:             reset,
		}
	})
	defer unsub()

	id1 := NewId()
	id2 := NewId()
	id3 := NewId()
	id4 := NewId()
	id5 := NewId()
	monitor.AddProviderEvent(id1, ProviderStateInEvaluation, id1, nil, IpFamilyLegacy)
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("callback did not start")
	}

	// Every call below executes on the same path window.resize uses. None may
	// wait for the callback currently parked above.
	publishDone := make(chan struct{})
	go func() {
		defer close(publishDone)
		monitor.AddProviderEvent(id2, ProviderStateAdded, id2, nil, IpFamilyLegacy)
		monitor.AddProviderEvent(id3, ProviderStateEvaluationFailed, id3, nil, IpFamilyLegacy)
		monitor.AddProviderEvent(id4, ProviderStateNotAdded, id4, nil, IpFamilyLegacy)
		monitor.AddProviderEvent(id1, ProviderStateRemoved, id1, nil, IpFamilyLegacy)
		monitor.AddProviderEvent(id5, ProviderStateInEvaluation, id5, nil, IpFamilyLegacy)
	}()
	select {
	case <-publishDone:
	case <-time.After(250 * time.Millisecond):
		t.Fatal("blocked callback stalled monitor publisher/maintenance")
	}

	close(block)
	first := <-delivered
	if first.reset {
		t.Fatal("first differential event unexpectedly reset")
	}
	var coalesced monitorCallbackEvent
	select {
	case coalesced = <-delivered:
	case <-time.After(time.Second):
		t.Fatal("coalesced callback was not delivered")
	}
	if !coalesced.reset {
		t.Fatal("over-cap pending diffs were not collapsed to a bounded reset snapshot")
	}
	if len(coalesced.providerEvents) != 2 {
		t.Fatalf("reset snapshot has %d providers, want 2", len(coalesced.providerEvents))
	}
	if event := coalesced.providerEvents[id2]; event == nil || event.State != ProviderStateAdded {
		t.Fatal("reset snapshot lost active provider id2")
	}
	if event := coalesced.providerEvents[id5]; event == nil || event.State != ProviderStateInEvaluation {
		t.Fatal("reset snapshot lost active provider id5")
	}
	for _, terminalId := range []Id{id1, id3, id4} {
		if _, ok := coalesced.providerEvents[terminalId]; ok {
			t.Fatalf("reset snapshot retained terminal provider %s", terminalId)
		}
	}
}

func TestMultiClientMonitorCallbackPanicIsListenerLocal(t *testing.T) {
	previousLogger := DefaultLogger()
	panicLogger := &callbackPanicTestLogger{
		Logger: NewNoopLogger(),
		warned: make(chan struct{}),
	}
	SetDefaultLogger(panicLogger)
	defer SetDefaultLogger(previousLogger)

	monitor := NewRemoteUserNatMultiClientMonitorWithDefaults()
	var calls atomic.Int32
	unsub := monitor.AddMonitorEventCallback(func(
		_ *WindowExpandEvent,
		_ map[Id]*ProviderEvent,
		_ bool,
	) {
		if calls.Add(1) == 1 {
			panic("listener failure")
		}
	})
	defer unsub()

	monitor.AddProviderEvent(NewId(), ProviderStateAdded, NewId(), nil, IpFamilyLegacy)
	waitForTestCondition(t, time.Second, func() bool {
		return calls.Load() == 1
	}, "first callback did not run")
	select {
	case <-panicLogger.warned:
	case <-time.After(time.Second):
		t.Fatal("listener panic was isolated but not reported")
	}

	monitor.AddProviderEvent(NewId(), ProviderStateAdded, NewId(), nil, IpFamilyLegacy)
	waitForTestCondition(t, time.Second, func() bool {
		return 2 <= calls.Load()
	}, "callback worker died after listener panic")
}

func TestMergedMultiClientMonitorResetIncludesEveryWindow(t *testing.T) {
	first := NewRemoteUserNatMultiClientMonitorWithDefaults()
	second := NewRemoteUserNatMultiClientMonitorWithDefaults()
	merged := NewMergedMultiClientMonitor([]MultiClientMonitor{first, second})
	gate := newStallGate()
	delivered := make(chan monitorCallbackEvent, 4)
	unsub := merged.AddMonitorEventCallback(func(
		windowExpandEvent *WindowExpandEvent,
		providerEvents map[Id]*ProviderEvent,
		reset bool,
	) {
		gate.Wait()
		delivered <- monitorCallbackEvent{
			windowExpandEvent: cloneWindowExpandEvent(windowExpandEvent),
			providerEvents:    providerEvents,
			reset:             reset,
		}
	})
	defer func() {
		gate.Release()
		unsub()
	}()

	firstId := NewId()
	secondId := NewId()
	first.AddProviderEvent(firstId, ProviderStateAdded, firstId, nil, IpFamilyLegacy)
	waitForStallStart(t, gate)
	second.AddProviderEvent(secondId, ProviderStateAdded, secondId, nil, IpFamilyLegacy)
	// Inject the bounded reset that an underlying listener produces on
	// overflow. Call its merge adapter directly so the test does not depend
	// on scheduler timing between two asynchronous callback workers.
	underlyingWorkers := first.monitorEventCallbacks.Get()
	if len(underlyingWorkers) != 1 {
		t.Fatalf("underlying merged callback workers = %d, want 1", len(underlyingWorkers))
	}
	firstWindow, firstProviders := first.Events()
	underlyingWorkers[0].callback(firstWindow, firstProviders, true)

	gate.Release()
	var resetEvent monitorCallbackEvent
	deadline := time.After(time.Second)
	for !resetEvent.reset {
		select {
		case resetEvent = <-delivered:
		case <-deadline:
			t.Fatal("merged reset event was not delivered")
		}
	}
	for _, id := range []Id{firstId, secondId} {
		event := resetEvent.providerEvents[id]
		if event == nil || event.State != ProviderStateAdded {
			t.Fatalf("merged reset lost live provider %s", id)
		}
	}
}

// Removing a provider must also exclude it from discovery. The resize loop
// wakes the moment a client dies, so a removal that only cancelled the client
// would be undone by the next discovery call handing back the same provider.
func TestApiGeneratorExcludesRemovedProvider(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	seeded := NewId()
	removed := NewId()
	generator := &ApiMultiClientGenerator{
		ctx:              ctx,
		excludeClientIds: []Id{seeded},
	}

	if got := generator.ExcludeClientIds(); len(got) != 1 || got[0] != seeded {
		t.Fatalf("initial exclusions = %v, want the seeded id", got)
	}

	generator.ExcludeClientId(removed)
	if got := generator.ExcludeClientIds(); len(got) != 2 || !slices.Contains(got, removed) {
		t.Fatalf("exclusions after remove = %v, want the removed id added", got)
	}

	// repeat removals must not grow the set (the same provider can be swiped
	// again after being re-added by an in-flight discovery)
	generator.ExcludeClientId(removed)
	if got := generator.ExcludeClientIds(); len(got) != 2 {
		t.Fatalf("exclusions = %v, want no duplicate", got)
	}

	// the snapshot handed to discovery must be a copy: the enumerator mutates
	// its own slice while the app can be appending
	snapshot := generator.ExcludeClientIds()
	snapshot[0] = NewId()
	if got := generator.ExcludeClientIds(); got[0] != seeded {
		t.Fatal("discovery snapshot aliases the generator's exclusion set")
	}

	// ExcludeClientId is called from the app thread while discovery reads
	var wait sync.WaitGroup
	for range 8 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			generator.ExcludeClientId(NewId())
		}()
		wait.Add(1)
		go func() {
			defer wait.Done()
			generator.ExcludeClientIds()
		}()
	}
	wait.Wait()
	if got := len(generator.ExcludeClientIds()); got != 10 {
		t.Fatalf("concurrent exclusions = %d, want 10", got)
	}
}

// removeProviderTestGenerator records whether the exclusion was applied, and
// controls whether the connection is fixed-destination only.
type removeProviderTestGenerator struct {
	testingEmptyMultiClientGenerator
	fixed    bool
	excluded []Id
}

func (self *removeProviderTestGenerator) FixedDestinationSize() (int, bool) {
	if self.fixed {
		return 1, true
	}
	return 0, false
}

// MultiClientGeneratorExcluder
func (self *removeProviderTestGenerator) ExcludeClientId(clientId Id) {
	self.excluded = append(self.excluded, clientId)
}

// A window client is identified to the user by its EGRESS (destination tail)
// client id, not the local window client id, so removal must match on the
// tail — and must leave every other provider connected.
func TestWindowRemoveProviderCancelsOnlyTheMatchingDestination(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultMultiClientSettings()
	newClient := func(destination MultiHopId) *multiClientChannel {
		clientCtx, clientCancel := context.WithCancel(ctx)
		return &multiClientChannel{
			ctx:    clientCtx,
			cancel: clientCancel,
			// Cancel records the end error into a stats event bucket, which
			// reads the window settings
			settings: settings,
			args: &multiClientChannelArgs{
				MultiClientGeneratorClientArgs: MultiClientGeneratorClientArgs{ClientId: NewId()},
				Destination:                    destination,
			},
			clientReceiveUnsub: func() {},
		}
	}

	removedEgressId := NewId()
	keptEgressId := NewId()
	// a multihop destination: the tail is the provider, the head is an
	// intermediary and must not be matched
	intermediaryId := NewId()
	multihop, err := NewMultiHopId(intermediaryId, removedEgressId)
	if err != nil {
		t.Fatal(err)
	}

	removedClient := newClient(RequireMultiHopId(removedEgressId))
	multihopClient := newClient(multihop)
	keptClient := newClient(RequireMultiHopId(keptEgressId))

	window := &multiClientWindow{
		ctx:           ctx,
		log:           NewNoopLogger(),
		clients:       map[Id]*multiClientChannel{},
		resizeMonitor: NewMonitor(),
	}
	for _, client := range []*multiClientChannel{removedClient, multihopClient, keptClient} {
		window.clients[client.args.ClientId] = client
	}

	if !window.removeProvider(removedEgressId) {
		t.Fatal("removeProvider did not report removing the provider")
	}

	select {
	case <-removedClient.ctx.Done():
	default:
		t.Fatal("the matching client was not canceled")
	}
	// same provider reached through an intermediary: still that provider
	select {
	case <-multihopClient.ctx.Done():
	default:
		t.Fatal("a multihop route to the removed provider was not canceled")
	}
	select {
	case <-keptClient.ctx.Done():
		t.Fatal("an unrelated provider was canceled")
	default:
	}

	// an id no window client routes to is not an error, just no removal
	if window.removeProvider(NewId()) {
		t.Fatal("removeProvider reported removing an unknown provider")
	}
	// the intermediary is not the provider
	if window.removeProvider(intermediaryId) {
		t.Fatal("removeProvider matched an intermediary instead of the destination tail")
	}
}

// Removing a provider must exclude it from discovery, or the resize loop
// (woken by the client's death) re-discovers it within seconds.
func TestRemoveProviderExcludesFromDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	generator := &removeProviderTestGenerator{}
	multi := &RemoteUserNatMultiClient{
		ctx:       ctx,
		log:       NewNoopLogger(),
		generator: generator,
	}

	egressClientId := NewId()
	// no windows: the return value reports only whether a client was dropped,
	// while the exclusion is applied regardless
	if multi.RemoveProvider(egressClientId) {
		t.Fatal("RemoveProvider reported a removal with no window clients")
	}
	if len(generator.excluded) != 1 || generator.excluded[0] != egressClientId {
		t.Fatalf("excluded = %v, want the removed provider", generator.excluded)
	}
}

// A fixed-destination connection (an explicitly chosen network peer) has
// nothing to replace the provider with, so excluding it would leave the tunnel
// with no destination at all. There the provider is dropped and redialed.
func TestRemoveProviderDoesNotExcludeFixedDestinations(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	generator := &removeProviderTestGenerator{fixed: true}
	multi := &RemoteUserNatMultiClient{
		ctx:       ctx,
		log:       NewNoopLogger(),
		generator: generator,
	}

	multi.RemoveProvider(NewId())

	if 0 < len(generator.excluded) {
		t.Fatalf("a fixed destination was excluded (%v); it would leave no destination", generator.excluded)
	}
}

// End of the wiring that matters: an excluded id must actually reach
// discovery. A fixed spec is resolved without any network call, so this
// asserts the exclusion is applied on the real NextDestinations path.
func TestExcludedProviderIsDroppedFromDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	keptId := NewId()
	removedId := NewId()
	generator := &ApiMultiClientGenerator{
		ctx:      ctx,
		specs:    []*ProviderSpec{{ClientId: &keptId}, {ClientId: &removedId}},
		settings: DefaultApiMultiClientGeneratorSettings(),
		// no persistence: a nil store skips continuity restoration
		identityState: newWindowIdentityState(ctx, nil),
	}

	destinations, err := generator.NextDestinationsContext(ctx, 2, nil, "quality")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := destinations[RequireMultiHopId(removedId)]; !ok {
		t.Fatal("the provider was not discoverable before removal")
	}

	generator.ExcludeClientId(removedId)

	destinations, err = generator.NextDestinationsContext(ctx, 2, nil, "quality")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := destinations[RequireMultiHopId(removedId)]; ok {
		t.Fatal("a removed provider was handed back by discovery")
	}
	if _, ok := destinations[RequireMultiHopId(keptId)]; !ok {
		t.Fatal("removing one provider suppressed the others")
	}
}

// The provider-locations surface derives connected-since and location from the
// retained events. Added events must carry the construction-time stamp, the
// egress (destination tail) client id, and the shared location pointer through
// Events() and callback delivery; terminal events must still delete.
func TestMultiClientMonitorProviderEventCarriesDetails(t *testing.T) {
	monitor := NewRemoteUserNatMultiClientMonitorWithDefaults()

	windowClientId := NewId()
	egressClientId := NewId()
	location := &ProviderLocation{
		Country:           "United States",
		CountryCode:       "us",
		Region:            "California",
		City:              "San Francisco",
		RegionCoordinates: &LocationCoordinates{Lat: 37.2, Lon: -119.3},
		CityCoordinates:   &LocationCoordinates{Lat: 37.7749, Lon: -122.4194},
	}

	delivered := make(chan map[Id]*ProviderEvent, 4)
	unsub := monitor.AddMonitorEventCallback(func(
		_ *WindowExpandEvent,
		providerEvents map[Id]*ProviderEvent,
		_ bool,
	) {
		delivered <- providerEvents
	})
	defer unsub()

	before := time.Now()
	monitor.AddProviderEvent(windowClientId, ProviderStateAdded, egressClientId, location, IpFamilyLegacy)
	after := time.Now()

	_, events := monitor.Events()
	event := events[windowClientId]
	if event == nil {
		t.Fatal("added provider missing from retained events")
	}
	if event.State != ProviderStateAdded {
		t.Fatalf("state = %s, want Added", event.State)
	}
	if event.EgressClientId != egressClientId {
		t.Fatal("egress client id was not carried")
	}
	if event.Location != location {
		t.Fatal("location pointer was not shared through retained events")
	}
	if event.EventTime.Before(before) || event.EventTime.After(after) {
		t.Fatalf("event time %s outside construction window [%s, %s]", event.EventTime, before, after)
	}

	select {
	case diff := <-delivered:
		if diffEvent := diff[windowClientId]; diffEvent == nil ||
			diffEvent.EgressClientId != egressClientId ||
			diffEvent.Location != location ||
			diffEvent.EventTime.IsZero() {
			t.Fatalf("callback diff lost provider details: %+v", diff[windowClientId])
		}
	case <-time.After(time.Second):
		t.Fatal("callback diff was not delivered")
	}

	monitor.AddProviderEvent(windowClientId, ProviderStateRemoved, egressClientId, location, IpFamilyLegacy)
	if _, events := monitor.Events(); events[windowClientId] != nil {
		t.Fatal("terminal event did not delete the retained provider")
	}
}

// End-to-end guard for the original failure: park the merged monitor observer
// while providers repeatedly go dark. The underlying window state must still
// replace client ids, proving resize/evaluation no longer runs through that
// observer.
func TestMultiClientPeerReplacementContinuesWhileMonitorObserverStalls(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultMultiClientSettings()
	settings.WindowSizes[WindowTypeQuality] = WindowSizeSettings{
		WindowSizeMin:     1,
		WindowSizeMax:     1,
		WindowSizeHardMax: 2,
		FixedWindowSize:   1,
	}
	settings.WindowSizes[WindowTypeSpeed] = WindowSizeSettings{
		WindowSizeMin:     1,
		WindowSizeMax:     1,
		WindowSizeHardMax: 2,
		FixedWindowSize:   1,
	}
	settings.PingWriteTimeout = 200 * time.Millisecond
	settings.PingTimeout = 500 * time.Millisecond
	settings.WindowResizeTimeout = 100 * time.Millisecond
	settings.WindowExpandTimeout = 500 * time.Millisecond
	settings.WindowEnumerateErrorTimeout = 25 * time.Millisecond
	settings.StatsWindowMaxUnhealthyDuration = 500 * time.Millisecond
	settings.StatsWindowWarnUnhealthyDuration = 250 * time.Millisecond
	settings.StatsWindowKeepUnhealthyDuration = time.Second
	settings.StatsWindowGraceperiod = 500 * time.Millisecond
	settings.AckTimeout = 500 * time.Millisecond
	settings.BlackholeTimeout = 500 * time.Millisecond
	settings.BlackholeConnectTimeout = time.Second
	settings.CPingWriteTimeout = 100 * time.Millisecond
	settings.CPingTimeout = 250 * time.Millisecond
	settings.CPingRestTimeout = 100 * time.Millisecond

	generator := newFlappingProviderGenerator(ctx)
	defer generator.close()
	multi := NewRemoteUserNatMultiClient(
		ctx,
		generator,
		func(TransferPath, protocol.ProvideMode, *IpPath, []byte) {},
		protocol.ProvideMode_Public,
		settings,
	)
	defer multi.Close()

	gate := newStallGate()
	unsub := multi.Monitor().AddMonitorEventCallback(func(
		*WindowExpandEvent,
		map[Id]*ProviderEvent,
		bool,
	) {
		gate.Wait()
	})
	defer func() {
		gate.Release()
		unsub()
	}()
	waitForStallStart(t, gate)

	activeIds := func() map[Id]bool {
		_, events := multi.Monitor().Events()
		ids := map[Id]bool{}
		for id, event := range events {
			if event != nil && event.State == ProviderStateAdded {
				ids[id] = true
			}
		}
		return ids
	}
	var initial map[Id]bool
	waitForTestCondition(t, 3*time.Second, func() bool {
		initial = activeIds()
		return 0 < len(initial)
	}, "window never formed its initial peers")

	waitForTestCondition(t, 8*time.Second, func() bool {
		for id := range activeIds() {
			if !initial[id] {
				return true
			}
		}
		return false
	}, "peer replacement stopped while monitor observer was stalled")
}

// Removal-generated TCP resets are advisory. If the downstream packet/TUN
// receiver blocks, enqueue must remain non-blocking and retained packets must
// stay fixed at the configured queue capacity.
func TestMultiClientRemovalReceiveIsBoundedAndNonBlocking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{})
	block := make(chan struct{})
	var startOnce sync.Once
	multi := &RemoteUserNatMultiClient{
		ctx:                 ctx,
		log:                 NewNoopLogger(),
		removalReceiveQueue: make(chan receivePacket, 1),
	}
	multi.receivePacketCallback.Store(&receivePacketCallbackHolder{callback: func(
		TransferPath,
		protocol.ProvideMode,
		*IpPath,
		[]byte,
	) {
		startOnce.Do(func() { close(started) })
		<-block
	}})
	go multi.runRemovalReceive()

	multi.enqueueRemovalReceive(&receivePacket{Packet: []byte{1}})
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("removal receive worker did not start")
	}

	begin := time.Now()
	for range 1024 {
		multi.enqueueRemovalReceive(&receivePacket{Packet: []byte{2}})
	}
	if elapsed := time.Since(begin); 250*time.Millisecond < elapsed {
		t.Fatalf("bounded enqueue blocked for %s", elapsed)
	}
	if got := len(multi.removalReceiveQueue); got != cap(multi.removalReceiveQueue) {
		t.Fatalf("queue retained %d packets, want fixed capacity %d", got, cap(multi.removalReceiveQueue))
	}
	if multi.removalReceiveDropCount.Load() == 0 {
		t.Fatal("full bounded queue did not account for dropped advisory packets")
	}
	close(block)
}

type closeDetachesServerNameLookup struct {
	unsubscribed atomic.Bool
}

func (self *closeDetachesServerNameLookup) ServerNames(string) []string {
	return nil
}

func (self *closeDetachesServerNameLookup) AddServerNamesLearnedCallback(ServerNamesLearnedFunction) func() {
	return func() {
		self.unsubscribed.Store(true)
	}
}

func TestMultiClientCloseDetachesRetiredOwnerReferences(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	owner := &struct{ value int }{value: 1}
	multi := NewRemoteUserNatMultiClient(
		ctx,
		&testingEmptyMultiClientGenerator{},
		func(TransferPath, protocol.ProvideMode, *IpPath, []byte) {
			_ = owner.value
		},
		protocol.ProvideMode_Network,
		DefaultMultiClientSettings(),
	)
	lookup := &closeDetachesServerNameLookup{}
	multi.SetServerNameLookup(lookup)

	multi.Close()

	if holder := multi.receivePacketCallback.Load(); holder != nil {
		t.Fatal("close retained the receive callback owner")
	}
	if config := multi.config.Load(); config.serverNameLookup != nil {
		t.Fatal("close retained the server-name lookup in the routing snapshot")
	}
	if multi.ipAssoc != nil {
		if holder := multi.ipAssoc.serverNameLookup.Load(); holder != nil && holder.lookup != nil {
			t.Fatal("close retained the server-name lookup in IP association state")
		}
	}
	if !lookup.unsubscribed.Load() {
		t.Fatal("close did not release the server-name learned subscription")
	}
}

func TestMultiClientStaleFlowCleanupCannotDeleteReplacement(t *testing.T) {
	for _, version := range []int{4, 6} {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			settings := DefaultMultiClientSettings()
			settings.DestinationAffinity = false
			settings.SequenceIdleTimeout = time.Hour
			multi := &RemoteUserNatMultiClient{
				ctx:              ctx,
				cancel:           cancel,
				log:              NewNoopLogger(),
				settings:         settings,
				ip4PathUpdates:   map[Ip4Path]*multiClientChannelUpdate{},
				ip6PathUpdates:   map[Ip6Path]*multiClientChannelUpdate{},
				affinityIp4Paths: map[Ip4Path]map[Ip4Path]time.Time{},
				affinityIp6Paths: map[Ip6Path]map[Ip6Path]time.Time{},
				clientUpdates:    map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
				flowReaperWake:   make(chan struct{}, 1),
			}
			go multi.runFlowReaper()
			path := &IpPath{
				Version:         version,
				Protocol:        IpProtocolTcp,
				SourcePort:      12345,
				DestinationPort: 443,
			}
			if version == 4 {
				path.SourceIp = []byte{10, 0, 0, 1}
				path.DestinationIp = []byte{192, 0, 2, 1}
			} else {
				path.SourceIp = []byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1}
				path.DestinationIp = []byte{0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2}
			}

			old, _, _ := multi.sendUpdate(path, flowPin{})
			if old == nil {
				t.Fatal("initial flow was not created")
			}
			clientCtx, cancelClient := context.WithCancel(ctx)
			defer cancelClient()
			client := &multiClientChannel{ctx: clientCtx}
			replacement := newMultiClientChannelUpdate(ctx, path)
			defer replacement.Close()

			// Hold the parent lock while waking the shared reaper and installing
			// the replacement. This deterministically makes cleanup observe the
			// newer generation when it resumes.
			multi.stateLock.Lock()
			old.client.Store(client)
			multi.clientUpdates[client] = map[*multiClientChannelUpdate]bool{old: true}
			old.cancel()
			multi.notifyFlowReaper()
			if version == 4 {
				multi.ip4PathUpdates[path.ToIp4Path()] = replacement
			} else {
				multi.ip6PathUpdates[path.ToIp6Path()] = replacement
			}
			multi.stateLock.Unlock()

			waitForTestCondition(t, time.Second, func() bool {
				multi.stateLock.Lock()
				defer multi.stateLock.Unlock()
				_, oldRetained := multi.clientUpdates[client]
				return !oldRetained
			}, "stale flow teardown did not finish")

			multi.stateLock.Lock()
			defer multi.stateLock.Unlock()
			if version == 4 {
				if got := multi.ip4PathUpdates[path.ToIp4Path()]; got != replacement {
					t.Fatal("stale IPv4 teardown deleted the replacement flow")
				}
			} else {
				if got := multi.ip6PathUpdates[path.ToIp6Path()]; got != replacement {
					t.Fatal("stale IPv6 teardown deleted the replacement flow")
				}
			}
		}()
	}
}

func TestMultiClientCancelledFlowTableRejectsNewGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	settings := DefaultMultiClientSettings()
	settings.DestinationAffinity = false
	multi := &RemoteUserNatMultiClient{
		ctx:              ctx,
		cancel:           cancel,
		settings:         settings,
		ip4PathUpdates:   map[Ip4Path]*multiClientChannelUpdate{},
		ip6PathUpdates:   map[Ip6Path]*multiClientChannelUpdate{},
		affinityIp4Paths: map[Ip4Path]map[Ip4Path]time.Time{},
		affinityIp6Paths: map[Ip6Path]map[Ip6Path]time.Time{},
		clientUpdates:    map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
	}
	cancel()

	update, _, _ := multi.sendUpdate(&IpPath{
		Version:         4,
		Protocol:        IpProtocolUdp,
		SourceIp:        []byte{10, 0, 0, 1},
		SourcePort:      53000,
		DestinationIp:   []byte{192, 0, 2, 53},
		DestinationPort: 53,
	}, flowPin{})
	if update != nil {
		t.Fatal("cancelled multi-client admitted a post-close flow generation")
	}
	if len(multi.ip4PathUpdates) != 0 || len(multi.ip6PathUpdates) != 0 {
		t.Fatal("cancelled multi-client recreated flow-table state")
	}
}

// Exercise the real teardown path, not only its queue primitive. Clearing
// every flow association is mandatory, but reset construction and retention
// must be bounded by queue capacity even for a large flow table.
func TestMultiClientRemovalClearsAllFlowsWithBoundedResetWork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientCtx, cancelClient := context.WithCancel(ctx)
	cancelClient()
	client := &multiClientChannel{ctx: clientCtx}
	multi := &RemoteUserNatMultiClient{
		ctx:                 ctx,
		log:                 NewNoopLogger(),
		removalReceiveQueue: make(chan receivePacket, 2),
		clientUpdates:       map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
	}
	updates := make(map[*multiClientChannelUpdate]bool, 4096)
	for i := range 4096 {
		update := &multiClientChannelUpdate{
			ipPath: &IpPath{
				Version:         4,
				Protocol:        IpProtocolTcp,
				SourceIp:        []byte{10, 0, byte(i >> 8), byte(i)},
				SourcePort:      10_000 + i,
				DestinationIp:   []byte{192, 0, 2, 1},
				DestinationPort: 443,
			},
		}
		update.client.Store(client)
		updates[update] = true
	}
	multi.clientUpdates[client] = updates

	assertReturnsWithin(t, 250*time.Millisecond, func() {
		multi.removeClient(client)
	}, "large client teardown stalled maintenance")

	if _, ok := multi.clientUpdates[client]; ok {
		t.Fatal("removed client retained its flow set")
	}
	for update := range updates {
		if update.client.Load() != nil {
			t.Fatal("client teardown did not clear every flow association")
		}
	}
	if got := len(multi.removalReceiveQueue); got != 2 {
		t.Fatalf("retained reset count = %d, want queue capacity 2", got)
	}
	if got := multi.removalReceiveDropCount.Load(); got != uint64(len(updates)-2) {
		t.Fatalf("accounted reset drops = %d, want %d", got, len(updates)-2)
	}
}

// Contract status is observer state, not transfer callback backpressure. A
// blocked observer must neither park HandleControlFrame nor grow a status queue
// without bound; latest state per key is retained.
func TestContractStatusObserverStallIsBoundedAndNonBlocking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gate := newStallGate()
	delivered := make(chan *ContractStatus, 8)
	worker := newContractStatusCallbackWorker(ctx, func(status *ContractStatus) {
		gate.Wait()
		delivered <- status
	}, 3)
	defer worker.Close()

	worker.Dispatch(&ContractStatus{Key: ContractKey{Destination: DestinationId(NewId())}})
	waitForStallStart(t, gate)

	var latestKey ContractKey
	assertReturnsWithin(t, 250*time.Millisecond, func() {
		for range 128 {
			latestKey = ContractKey{Destination: DestinationId(NewId())}
			worker.Dispatch(&ContractStatus{Key: latestKey})
		}
		worker.Dispatch(&ContractStatus{Key: latestKey, Premium: true})
	}, "blocked contract-status observer stalled its control-plane producer")

	worker.stateLock.Lock()
	pendingCount := len(worker.pending)
	orderCount := worker.orderCount
	latest := worker.pending[latestKey]
	worker.stateLock.Unlock()
	if pendingCount > 3 || orderCount > 3 {
		t.Fatalf("status backlog exceeded bound: pending=%d order=%d", pendingCount, orderCount)
	}
	if latest == nil || !latest.Premium {
		t.Fatal("status coalescing did not retain the latest value for a contract key")
	}
	gate.Release()
}

func TestMultiClientContractStatusWorkerSizesToLiveContracts(t *testing.T) {
	settings := DefaultMultiClientSettings()
	settings.SequenceBufferSize = 4096
	settings.MaxFlowsPerExit = 7
	settings.WindowSizes[WindowTypeQuality] = WindowSizeSettings{
		WindowSizeMin:     3,
		WindowSizeMax:     3,
		WindowSizeHardMax: 3,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	window := &multiClientWindow{
		ctx:                     ctx,
		windowType:              WindowTypeQuality,
		settings:                settings,
		contractStatusCallbacks: NewCallbackList[*contractStatusCallbackWorker](),
	}
	unsub := window.AddContractStatusCallback(func(*ContractStatus) {})
	defer unsub()
	workers := window.contractStatusCallbacks.Get()
	if len(workers) != 1 {
		t.Fatalf("contract status workers = %d, want 1", len(workers))
	}
	if got, want := workers[0].maxCount, 21; got != want {
		t.Fatalf("contract status pending max = %d, want %d", got, want)
	}
	if workers[0].maxCount == settings.SequenceBufferSize {
		t.Fatal("contract status ring still follows packet SequenceBufferSize")
	}
}

func TestContractStatsObserverStallIsBoundedAndCoalesced(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gate := newStallGate()
	worker := newContractStatsCallbackWorker(ctx, func([]*ContractStatsEvent) {
		gate.Wait()
	}, 2)
	defer worker.Close()

	firstId := NewId()
	worker.Dispatch([]*ContractStatsEvent{{
		ContractId:         firstId,
		UsedByteCountDelta: 1,
		Open:               true,
		Sequence:           1,
	}})
	waitForStallStart(t, gate)

	latestId := NewId()
	assertReturnsWithin(t, 250*time.Millisecond, func() {
		for range 128 {
			worker.Dispatch([]*ContractStatsEvent{{
				ContractId: NewId(),
				Open:       true,
				Sequence:   1,
			}})
		}
		worker.Dispatch([]*ContractStatsEvent{{
			ContractId:         latestId,
			UsedByteCount:      10,
			UsedByteCountDelta: 4,
			Open:               true,
			Sequence:           1,
		}})
		worker.Dispatch([]*ContractStatsEvent{{
			ContractId:         latestId,
			UsedByteCount:      15,
			UsedByteCountDelta: 5,
			Open:               false,
			Sequence:           2,
		}})
	}, "blocked contract-stats observer stalled cleanup/stats production")

	worker.stateLock.Lock()
	pendingCount := len(worker.pending)
	orderCount := worker.orderCount
	latest := worker.pending[contractStatsKey{contractId: latestId}]
	worker.stateLock.Unlock()
	if pendingCount > 2 || orderCount > 2 {
		t.Fatalf("stats backlog exceeded bound: pending=%d order=%d", pendingCount, orderCount)
	}
	if latest == nil || latest.Open || latest.Sequence != 2 ||
		latest.UsedByteCount != 15 || latest.UsedByteCountDelta != 9 {
		t.Fatalf("stats coalescing lost latest/accumulated state: %+v", latest)
	}
	gate.Release()
}

func TestPeerIdentityObserverStallCoalesces(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	gate := newStallGate()
	var calls atomic.Int32
	worker := newCoalescingCallbackWorker(ctx, func() {
		calls.Add(1)
		gate.Wait()
	})
	defer worker.Close()

	worker.Dispatch()
	waitForStallStart(t, gate)
	assertReturnsWithin(t, 250*time.Millisecond, func() {
		for range 1024 {
			worker.Dispatch()
		}
	}, "blocked identity observer stalled its producer")
	if got := len(worker.notify); got != 1 {
		t.Fatalf("coalesced identity backlog = %d, want 1", got)
	}
	gate.Release()
	waitForTestCondition(t, time.Second, func() bool {
		return calls.Load() == 2
	}, "coalesced identity change was not delivered")
}

type stalledLoadIdentityStore struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
	result  []*WindowClientIdentity
}

func (self *stalledLoadIdentityStore) LoadWindowClientIdentities() []*WindowClientIdentity {
	self.once.Do(func() { close(self.started) })
	<-self.release
	return self.result
}

func (*stalledLoadIdentityStore) StoreWindowClientIdentities([]*WindowClientIdentity) {
}

type contextLoadIdentityStore struct {
	started  chan struct{}
	canceled chan struct{}
	once     sync.Once
}

func (*contextLoadIdentityStore) LoadWindowClientIdentities() []*WindowClientIdentity {
	panic("legacy identity load used despite context capability")
}

func (*contextLoadIdentityStore) StoreWindowClientIdentities([]*WindowClientIdentity) {
}

func (self *contextLoadIdentityStore) LoadWindowClientIdentitiesContext(ctx context.Context) []*WindowClientIdentity {
	self.once.Do(func() { close(self.started) })
	<-ctx.Done()
	close(self.canceled)
	return nil
}

func TestWindowIdentityLoadStallCannotStopDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	lateDestination := RequireMultiHopId(NewId())
	store := &stalledLoadIdentityStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
		result: []*WindowClientIdentity{{
			ClientId:    NewId(),
			Destination: lateDestination,
		}},
	}
	state := newWindowIdentityState(ctx, store)

	loadCtx, loadCancel := context.WithTimeout(ctx, 25*time.Millisecond)
	defer loadCancel()
	start := time.Now()
	_, err := state.RestoredDestinationsContext(loadCtx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled identity load error = %v, want deadline exceeded", err)
	}
	if elapsed := time.Since(start); 250*time.Millisecond < elapsed {
		t.Fatalf("identity load deadline took %s", elapsed)
	}

	// Once continuity restoration misses its budget, later discovery calls
	// proceed immediately with fresh identities. Record also stays independent
	// of the still-stuck loader because no external call owns state.mutex.
	assertReturnsWithin(t, 250*time.Millisecond, func() {
		destinations, secondErr := state.RestoredDestinationsContext(ctx)
		if secondErr != nil || len(destinations) != 0 {
			t.Errorf("abandoned identity load = %v, %v; want empty success", destinations, secondErr)
		}
		state.Record(&WindowClientIdentity{
			ClientId:    NewId(),
			Destination: RequireMultiHopId(NewId()),
		})
	}, "abandoned identity load continued to block discovery/state mutation")

	close(store.release)
	waitForTestCondition(t, time.Second, func() bool {
		state.mutex.Lock()
		defer state.mutex.Unlock()
		return state.loadFinished
	}, "late identity load did not finish")
	destinations, err := state.RestoredDestinationsContext(ctx)
	if err != nil || len(destinations) != 0 {
		t.Fatalf("late abandoned identities were resurrected: %v, %v", destinations, err)
	}
}

func TestWindowIdentityDeadlineCancelsContextAwareStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &contextLoadIdentityStore{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
	state := newWindowIdentityState(ctx, store)
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("context-aware identity load did not start")
	}

	loadCtx, cancelLoad := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelLoad()
	if _, err := state.RestoredDestinationsContext(loadCtx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("identity load error = %v, want deadline exceeded", err)
	}
	select {
	case <-store.canceled:
	case <-time.After(time.Second):
		t.Fatal("identity deadline abandoned result but did not cancel underlying store request")
	}
}

func TestWindowIdentityLoadWarmsBeforeDiscovery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &stalledLoadIdentityStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	newWindowIdentityState(ctx, store)
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("identity load did not start until first discovery call")
	}
	close(store.release)
}

// A persisted identity is an optimization. A slow Redis/disk adapter must not
// suppress an already-known fixed provider or force a second enumerate retry.
func TestOptionalIdentityLoadCannotSuppressFixedDestination(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	store := &stalledLoadIdentityStore{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	defer close(store.release)
	fixedId := NewId()
	generator := &ApiMultiClientGenerator{
		ctx:      ctx,
		specs:    []*ProviderSpec{{ClientId: &fixedId}},
		settings: &ApiMultiClientGeneratorSettings{IdentityLoadTimeout: 20 * time.Millisecond},
		identityState: newWindowIdentityState(
			ctx,
			store,
		),
	}

	start := time.Now()
	destinations, err := generator.NextDestinationsContext(ctx, 1, nil, "quality")
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); 250*time.Millisecond < elapsed {
		t.Fatalf("optional identity timeout delayed fixed discovery for %s", elapsed)
	}
	fixedDestination := RequireMultiHopId(fixedId)
	if _, ok := destinations[fixedDestination]; !ok {
		t.Fatal("optional identity timeout suppressed known fixed destination")
	}
}

func testTransferCallbackInlineDispatch(t *testing.T, invoke func(*stallGate)) {
	t.Helper()
	gate := newStallGate()
	done := make(chan struct{})
	go func() {
		defer close(done)
		invoke(gate)
	}()
	waitForStallStart(t, gate)
	assertStillBlocked(t, done, 25*time.Millisecond, "transfer callback was not dispatched inline")
	gate.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("inline transfer callback did not return after release")
	}
}

func TestTransferSendAckCallbackPreservesIntentionalBackpressure(t *testing.T) {
	testTransferCallbackInlineDispatch(t, func(gate *stallGate) {
		safeAck(func(error) { gate.Wait() }, nil)
	})
}

// Receive callbacks are inline so borrowed frames cannot escape. This test
// deliberately installs a contract-violating blocker only to pin that
// ownership property; production callbacks must never block (CODESTYLE.md).
func TestTransferReceiveCallbackDispatchIsInline(t *testing.T) {
	client := &Client{
		log:              DefaultLogger(),
		receiveCallbacks: NewCallbackList[ReceiveFunction](),
	}
	testTransferCallbackInlineDispatch(t, func(gate *stallGate) {
		client.AddReceiveCallback(func(TransferPath, []*protocol.Frame, Peer) {
			gate.Wait()
		})
		client.receive(TransferPath{}, nil, Peer{})
	})
}

// Forward callbacks have the same inline borrowed-bytes ownership contract.
func TestTransferForwardCallbackDispatchIsInline(t *testing.T) {
	client := &Client{
		log:              DefaultLogger(),
		forwardCallbacks: NewCallbackList[ForwardFunction](),
	}
	testTransferCallbackInlineDispatch(t, func(gate *stallGate) {
		client.AddForwardCallback(func(TransferPath, []byte) {
			gate.Wait()
		})
		client.forward(TransferPath{}, nil)
	})
}

func TestTransferCancelDoesNotJoinInFlightReceiveCallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(
		ctx,
		NewId(),
		NewNoContractClientOob(),
		DefaultClientSettings(),
	)
	defer client.Close()
	gate := newStallGate()
	done := make(chan struct{})
	unsub := client.AddReceiveCallback(func(TransferPath, []*protocol.Frame, Peer) {
		gate.Wait()
	})
	defer unsub()
	go func() {
		defer close(done)
		client.receive(TransferPath{}, nil, Peer{})
	}()
	waitForStallStart(t, gate)

	assertReturnsWithin(
		t,
		250*time.Millisecond,
		client.Cancel,
		"client cancellation joined an in-flight receive callback",
	)
	assertStillBlocked(
		t,
		done,
		25*time.Millisecond,
		"cancellation changed inline callback execution",
	)
	gate.Release()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("receive callback did not return after release")
	}
}

type cleanupOrderTestGenerator struct {
	client       *Client
	removeOnce   sync.Once
	removeCalled chan struct{}
}

func (*cleanupOrderTestGenerator) NextDestinations(int, []MultiHopId, string) (map[MultiHopId]DestinationStats, error) {
	return nil, nil
}

func (*cleanupOrderTestGenerator) NewClientArgs() (*MultiClientGeneratorClientArgs, error) {
	return &MultiClientGeneratorClientArgs{ClientId: NewId()}, nil
}

func (*cleanupOrderTestGenerator) RemoveClientArgs(*MultiClientGeneratorClientArgs) {}

func (self *cleanupOrderTestGenerator) RemoveClientWithArgs(*Client, *MultiClientGeneratorClientArgs) {
	self.removeOnce.Do(func() { close(self.removeCalled) })
}

func (*cleanupOrderTestGenerator) NewClientSettings() *ClientSettings {
	settings := DefaultClientSettings()
	settings.ControlPingTimeout = 0
	return settings
}

func (self *cleanupOrderTestGenerator) NewClient(
	ctx context.Context,
	args *MultiClientGeneratorClientArgs,
	settings *ClientSettings,
) (*Client, error) {
	self.client = NewClient(ctx, args.ClientId, NewNoContractClientOob(), settings)
	return self.client, nil
}

func (*cleanupOrderTestGenerator) FixedDestinationSize() (int, bool) {
	return 1, true
}

type maintenanceContextTestGenerator struct{}

func (*maintenanceContextTestGenerator) NextDestinations(int, []MultiHopId, string) (map[MultiHopId]DestinationStats, error) {
	panic("legacy unbounded discovery path used")
}

func (*maintenanceContextTestGenerator) NewClientArgs() (*MultiClientGeneratorClientArgs, error) {
	panic("legacy unbounded auth path used")
}

func (*maintenanceContextTestGenerator) RemoveClientArgs(*MultiClientGeneratorClientArgs) {}

func (*maintenanceContextTestGenerator) RemoveClientWithArgs(*Client, *MultiClientGeneratorClientArgs) {
}

func (*maintenanceContextTestGenerator) NewClientSettings() *ClientSettings {
	return DefaultClientSettings()
}

func (*maintenanceContextTestGenerator) NewClient(context.Context, *MultiClientGeneratorClientArgs, *ClientSettings) (*Client, error) {
	panic("legacy unbounded client setup path used")
}

func (*maintenanceContextTestGenerator) FixedDestinationSize() (int, bool) {
	return 1, true
}

func (*maintenanceContextTestGenerator) NextDestinationsContext(
	ctx context.Context,
	_ int,
	_ []MultiHopId,
	_ string,
) (map[MultiHopId]DestinationStats, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*maintenanceContextTestGenerator) NewClientArgsContext(ctx context.Context) (*MultiClientGeneratorClientArgs, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func (*maintenanceContextTestGenerator) NewClientContext(
	_ context.Context,
	callCtx context.Context,
	_ *MultiClientGeneratorClientArgs,
	_ *ClientSettings,
) (*Client, error) {
	<-callCtx.Done()
	return nil, callCtx.Err()
}

type successfulMaintenanceGenerator struct {
	maintenanceContextTestGenerator
	client           *Client
	sawSetupDeadline atomic.Bool
}

func (self *successfulMaintenanceGenerator) NewClientContext(
	ctx context.Context,
	callCtx context.Context,
	args *MultiClientGeneratorClientArgs,
	settings *ClientSettings,
) (*Client, error) {
	_, hasDeadline := callCtx.Deadline()
	self.sawSetupDeadline.Store(hasDeadline)
	self.client = NewClient(ctx, args.ClientId, NewNoContractClientOob(), settings)
	return self.client, nil
}

type recoveringMaintenanceGenerator struct {
	destination MultiHopId
	discoveries atomic.Int32
	auths       atomic.Int32
}

func (*recoveringMaintenanceGenerator) NextDestinations(int, []MultiHopId, string) (map[MultiHopId]DestinationStats, error) {
	panic("legacy discovery path used")
}

func (*recoveringMaintenanceGenerator) NewClientArgs() (*MultiClientGeneratorClientArgs, error) {
	panic("legacy auth path used")
}

func (*recoveringMaintenanceGenerator) RemoveClientArgs(*MultiClientGeneratorClientArgs) {}

func (*recoveringMaintenanceGenerator) RemoveClientWithArgs(*Client, *MultiClientGeneratorClientArgs) {
}

func (*recoveringMaintenanceGenerator) NewClientSettings() *ClientSettings {
	return DefaultClientSettings()
}

func (*recoveringMaintenanceGenerator) NewClient(context.Context, *MultiClientGeneratorClientArgs, *ClientSettings) (*Client, error) {
	panic("legacy client path used")
}

func (*recoveringMaintenanceGenerator) FixedDestinationSize() (int, bool) {
	return 1, true
}

func (self *recoveringMaintenanceGenerator) NextDestinationsContext(
	ctx context.Context,
	_ int,
	_ []MultiHopId,
	_ string,
) (map[MultiHopId]DestinationStats, error) {
	if self.discoveries.Add(1) == 1 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return map[MultiHopId]DestinationStats{self.destination: {}}, nil
}

func (self *recoveringMaintenanceGenerator) NewClientArgsContext(ctx context.Context) (*MultiClientGeneratorClientArgs, error) {
	if self.auths.Add(1) == 1 {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return &MultiClientGeneratorClientArgs{ClientId: NewId()}, nil
}

func (*recoveringMaintenanceGenerator) NewClientContext(
	context.Context,
	context.Context,
	*MultiClientGeneratorClientArgs,
	*ClientSettings,
) (*Client, error) {
	panic("client creation not used")
}

func TestSchedulerPauseDetectionDoesNotConfuseOrdinaryTimerDelay(t *testing.T) {
	expected := 100 * time.Millisecond
	tolerance := 50 * time.Millisecond
	if schedulerPauseDetected(0, expected, tolerance) {
		t.Fatal("fresh timer was classified as a scheduler pause")
	}
	if !schedulerPauseDetected(time.Second, expected, tolerance) {
		t.Fatal("large scheduler gap was classified as an ordinary peer timeout")
	}
	if schedulerPauseDetected(time.Second, expected, 0) {
		t.Fatal("disabled scheduler-pause detection still classified a pause")
	}
}

func TestMultiClientRemovalDoesNotWaitForPeerConnectionTeardown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	generator := &cleanupOrderTestGenerator{removeCalled: make(chan struct{})}
	settings := DefaultMultiClientSettings()
	settings.CPingRestTimeout = time.Hour
	settings.BlackholeTimeout = time.Hour

	channel, err := newMultiClientChannel(
		ctx,
		&multiClientChannelArgs{
			MultiClientGeneratorClientArgs: MultiClientGeneratorClientArgs{ClientId: NewId()},
			Destination:                    RequireMultiHopId(NewId()),
		},
		generator,
		func(*multiClientChannel, TransferPath, protocol.ProvideMode, TransportType, *IpPath, []byte) {},
		nil,
		DefaultSecurityPolicy(ctx),
		func(*ContractStatus) {},
		func([]*ContractStatsEvent) {},
		func() {},
		nil,
		settings,
		nil, nil, nil, nil, nil, nil, nil, nil, nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	// Model a slow Pion teardown without relying on OS/network timing.
	generator.client.webRtcManager.peerConnWorkers.Add(1)
	releaseTeardown := sync.OnceFunc(generator.client.webRtcManager.peerConnWorkers.Done)
	defer releaseTeardown()

	closeReturned := make(chan struct{})
	go func() {
		channel.Close()
		close(closeReturned)
	}()

	select {
	case <-generator.removeCalled:
	case <-time.After(time.Second):
		t.Fatal("platform client removal waited for peer-connection teardown")
	}
	select {
	case <-closeReturned:
	case <-time.After(time.Second):
		t.Fatal("multi-client channel close waited for peer-connection teardown")
	}

	releaseTeardown()
	select {
	case <-generator.client.webRtcManager.closeDone:
	case <-time.After(time.Second):
		t.Fatal("peer manager did not finish after teardown resumed")
	}
}
