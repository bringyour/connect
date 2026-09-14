package connect

import (
	"context"
	"crypto/tls"
	"maps"
	"net"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	quic "github.com/quic-go/quic-go"
	"github.com/urnetwork/connect/v2026/protocol"
)

func TestMultiClientChannelAcceptsMigrationOnlyFromControl(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	migrator := &recordingWindowTransportMigrator{calls: make(chan time.Time, 2)}
	channel := &multiClientChannel{
		ctx:               ctx,
		args:              &multiClientChannelArgs{},
		transportMigrator: migrator,
	}
	want := time.Now().Add(time.Second).Truncate(time.Millisecond)
	frame := RequireToFrameWithDefaultProtocolVersion(&protocol.ResidentMigrate{
		MigrateTime: uint64(want.UnixMilli()),
	})

	channel.clientReceive(SourceId(NewId()), []*protocol.Frame{frame}, Peer{})
	select {
	case <-migrator.calls:
		t.Fatal("ordinary data source triggered transport migration")
	default:
	}

	channel.clientReceive(SourceId(ControlId), []*protocol.Frame{frame}, Peer{})
	select {
	case got := <-migrator.calls:
		if !got.Equal(want) {
			t.Fatalf("migration time = %s, want %s", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("control-source migration was not forwarded")
	}
}

type recordingWindowTransportMigrator struct {
	calls chan time.Time
}

func (self *recordingWindowTransportMigrator) MigrateClientTransport(
	client *Client,
	args *MultiClientGeneratorClientArgs,
	migrateTime time.Time,
) {
	self.calls <- migrateTime
}

type fakeWindowPlatformTransport struct {
	mutex            sync.Mutex
	connected        bool
	waitingForBudget bool
	// the extenders this transport's live connections run through (K1)
	extenderIps     []netip.Addr
	notify          chan struct{}
	closed          chan struct{}
	closeOnce       sync.Once
	waitStarted     chan struct{}
	waitStartedOnce sync.Once
	// onClose, when set, runs synchronously inside Close before `closed` is
	// signaled — a seam to observe migrator state at the exact instant of
	// close (see TestApiWindowTransportMigrationDisarmsBeforeClosingReplacement).
	onClose func()
}

func newFakeWindowPlatformTransport(connected bool) *fakeWindowPlatformTransport {
	return &fakeWindowPlatformTransport{
		connected:   connected,
		notify:      make(chan struct{}),
		closed:      make(chan struct{}),
		waitStarted: make(chan struct{}),
	}
}

func (self *fakeWindowPlatformTransport) ConnectedNotify() <-chan struct{} {
	self.waitStartedOnce.Do(func() {
		close(self.waitStarted)
	})
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.notify
}

func (self *fakeWindowPlatformTransport) IsConnected() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.connected
}

// ExtenderIps matches the platform transport's reporting seam (K1), so the
// generator answers about a fake transport exactly as it does about a real one.
func (self *fakeWindowPlatformTransport) ExtenderIps() []netip.Addr {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return slices.Clone(self.extenderIps)
}

func (self *fakeWindowPlatformTransport) setExtenderIps(ips ...netip.Addr) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.extenderIps = slices.Clone(ips)
}

func (self *fakeWindowPlatformTransport) IsWaitingForBudget() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.waitingForBudget
}

func (self *fakeWindowPlatformTransport) Close() {
	self.closeOnce.Do(func() {
		if self.onClose != nil {
			self.onClose()
		}
		close(self.closed)
	})
}

func (self *fakeWindowPlatformTransport) connect() {
	self.mutex.Lock()
	if !self.connected {
		self.connected = true
		close(self.notify)
		self.notify = make(chan struct{})
	}
	self.mutex.Unlock()
}

func newApiMigrationTestClient(t *testing.T) (*Client, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	clientSettings := DefaultClientSettings()
	clientSettings.Log = NewNoopLogger()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), clientSettings)
	t.Cleanup(func() {
		client.Close()
		cancel()
	})
	return client, cancel
}

func TestApiWindowTransportMigrationIsMakeBeforeBreakAndDeduplicated(t *testing.T) {
	client, _ := newApiMigrationTestClient(t)
	old := newFakeWindowPlatformTransport(true)
	next := newFakeWindowPlatformTransport(false)
	// K1: each generation is reached through its own extender, and the window
	// reads the current one through the generator
	oldExtenderIp := netip.MustParseAddr("192.0.2.131")
	nextExtenderIp := netip.MustParseAddr("192.0.2.132")
	old.setExtenderIps(oldExtenderIp)
	next.setExtenderIps(nextExtenderIp)
	created := make(chan struct{}, 2)
	settings := DefaultApiMultiClientGeneratorSettings()
	settings.MigrateConnectTimeout = time.Second
	settings.MigrateMaxScheduleDelay = 20 * time.Millisecond
	transportSettings := DefaultPlatformTransportSettings()
	state := &apiWindowClientTransport{
		current:            old,
		settings:           transportSettings,
		auth:               ClientAuth{InstanceId: NewId()},
		extenderIpsMonitor: NewMonitorValue[uint64](0),
	}
	generator := &ApiMultiClientGenerator{
		settings:   settings,
		transports: map[*Client]*apiWindowClientTransport{client: state},
		newPlatformTransport: func(
			client *Client,
			auth *ClientAuth,
			_ TransportMode,
			settings *PlatformTransportSettings,
		) apiWindowPlatformTransport {
			created <- struct{}{}
			return next
		},
	}

	extenderIps, extenderChange := generator.ClientExtenderIps(client)
	if !slices.Equal(extenderIps, []netip.Addr{oldExtenderIp}) {
		t.Fatalf("extender ips = %v, want [%v]", extenderIps, oldExtenderIp)
	}

	// A far-future timestamp is clamped, and a duplicate while pending must
	// not construct a second replacement.
	generator.MigrateClientTransport(client, nil, time.Now().Add(24*time.Hour))
	generator.MigrateClientTransport(client, nil, time.Now())
	select {
	case <-created:
	case <-time.After(time.Second):
		t.Fatal("clamped migration did not construct a replacement")
	}
	select {
	case <-created:
		t.Fatal("duplicate migration constructed another replacement")
	case <-time.After(30 * time.Millisecond):
	}

	select {
	case <-old.closed:
		t.Fatal("old transport closed before replacement connected")
	default:
	}

	next.connect()
	select {
	case <-old.closed:
	case <-time.After(time.Second):
		t.Fatal("old transport was not closed after replacement connected")
	}
	generator.transportLock.Lock()
	current := state.current
	generator.transportLock.Unlock()
	if current != next {
		t.Fatal("connected replacement was not installed")
	}

	// the swap itself is a change: a watcher subscribed before it is woken,
	// and reads the replacement's extender rather than the retired one
	select {
	case <-extenderChange:
	case <-time.After(time.Second):
		t.Fatal("the transport swap did not wake the extender watcher")
	}
	if extenderIps, _ := generator.ClientExtenderIps(client); !slices.Equal(
		extenderIps,
		[]netip.Addr{nextExtenderIp},
	) {
		t.Fatalf("extender ips = %v, want [%v] after the swap", extenderIps, nextExtenderIp)
	}
}

func TestApiWindowTransportMigrationKeepsOldOnTimeout(t *testing.T) {
	client, _ := newApiMigrationTestClient(t)
	old := newFakeWindowPlatformTransport(true)
	next := newFakeWindowPlatformTransport(false)
	settings := DefaultApiMultiClientGeneratorSettings()
	settings.MigrateConnectTimeout = 25 * time.Millisecond
	state := &apiWindowClientTransport{
		current:  old,
		settings: DefaultPlatformTransportSettings(),
		auth:     ClientAuth{InstanceId: NewId()},
	}
	generator := &ApiMultiClientGenerator{
		settings:   settings,
		transports: map[*Client]*apiWindowClientTransport{client: state},
		newPlatformTransport: func(
			client *Client,
			auth *ClientAuth,
			_ TransportMode,
			settings *PlatformTransportSettings,
		) apiWindowPlatformTransport {
			return next
		},
	}

	generator.MigrateClientTransport(client, nil, time.Now())
	select {
	case <-next.closed:
	case <-time.After(time.Second):
		t.Fatal("failed replacement was not closed at timeout")
	}
	select {
	case <-old.closed:
		t.Fatal("old transport was closed after replacement timeout")
	default:
	}
	generator.transportLock.Lock()
	current := state.current
	migrating := state.migrating
	generator.transportLock.Unlock()
	if current != old {
		t.Fatal("failed replacement displaced the old transport")
	}
	if migrating {
		t.Fatal("migration remained armed after timeout")
	}
}

// TestApiWindowTransportMigrationDisarmsBeforeClosingReplacement pins the
// ordering that made TestApiWindowTransportMigrationKeepsOldOnTimeout a
// load-sensitive flake: on the connect-timeout path the migrator must release
// its migration claim (state.migrating = false) BEFORE it closes the failed
// replacement, so the replacement's close is the definitive "migration
// released" signal. The disarm otherwise ran in a defer AFTER the close,
// leaving a window — observable under full-suite load — where the replacement
// was closed but a follow-up migration was still refused as "already
// migrating".
//
// Deterministic by construction: the fake transport's onClose hook samples
// state.migrating at the exact instant of close. Disarm-before-close => false
// at that instant; the deferred-only ordering => true.
func TestApiWindowTransportMigrationDisarmsBeforeClosingReplacement(t *testing.T) {
	client, _ := newApiMigrationTestClient(t)
	old := newFakeWindowPlatformTransport(true)
	next := newFakeWindowPlatformTransport(false)
	settings := DefaultApiMultiClientGeneratorSettings()
	settings.MigrateConnectTimeout = 25 * time.Millisecond
	state := &apiWindowClientTransport{
		current:  old,
		settings: DefaultPlatformTransportSettings(),
		auth:     ClientAuth{InstanceId: NewId()},
	}
	generator := &ApiMultiClientGenerator{
		settings:   settings,
		transports: map[*Client]*apiWindowClientTransport{client: state},
		newPlatformTransport: func(
			client *Client,
			auth *ClientAuth,
			_ TransportMode,
			settings *PlatformTransportSettings,
		) apiWindowPlatformTransport {
			return next
		},
	}

	var migratingAtClose bool
	next.onClose = func() {
		generator.transportLock.Lock()
		migratingAtClose = state.migrating
		generator.transportLock.Unlock()
	}

	generator.MigrateClientTransport(client, nil, time.Now())
	select {
	case <-next.closed:
	case <-time.After(time.Second):
		t.Fatal("failed replacement was not closed at timeout")
	}
	if migratingAtClose {
		t.Fatal("migration was still armed at the instant the replacement was closed: disarm must happen-before the close so the close is the definitive released signal")
	}
}

// Generator teardown must join a migration that has entered replacement
// construction and reject every creator that arrives after the close edge.
func TestApiWindowTransportCreationCloseJoinsHeldMigrationCreator(t *testing.T) {
	client, _ := newApiMigrationTestClient(t)
	old := newFakeWindowPlatformTransport(true)
	next := newFakeWindowPlatformTransport(true)
	creationEntered := make(chan struct{})
	releaseCreation := make(chan struct{})
	lateCreation := make(chan struct{}, 1)
	var creationCount atomic.Int32
	state := &apiWindowClientTransport{
		current:  old,
		settings: DefaultPlatformTransportSettings(),
		auth:     ClientAuth{InstanceId: NewId()},
	}
	generator := &ApiMultiClientGenerator{
		settings:   DefaultApiMultiClientGeneratorSettings(),
		transports: map[*Client]*apiWindowClientTransport{client: state},
		newPlatformTransport: func(
			client *Client,
			auth *ClientAuth,
			_ TransportMode,
			settings *PlatformTransportSettings,
		) apiWindowPlatformTransport {
			if creationCount.Add(1) == 1 {
				close(creationEntered)
				<-releaseCreation
				return next
			}
			lateCreation <- struct{}{}
			return newFakeWindowPlatformTransport(true)
		},
	}

	generator.MigrateClientTransport(client, nil, time.Now())
	<-creationEntered
	closeWaitEntered := make(chan struct{})
	generator.transportCreation.beforeWaitForTest = func() {
		close(closeWaitEntered)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- generator.CloseTransportCreationAndWait(ctx)
	}()
	<-closeWaitEntered
	select {
	case err := <-closeResult:
		t.Fatalf("transport-creation close skipped held migration: %v", err)
	default:
	}
	close(releaseCreation)
	if err := <-closeResult; err != nil {
		t.Fatal(err)
	}

	generator.MigrateClientTransport(client, nil, time.Now())
	select {
	case <-lateCreation:
		t.Fatal("migration created a transport after generator close")
	default:
	}
}

// A runtime Device policy change replaces every live window
// make-before-break and preserves equal Auto priorities in the concrete
// PlatformTransport settings used for the replacement.
func TestApiWindowTransportPolicyChangeIsLiveAndMakeBeforeBreak(t *testing.T) {
	client, _ := newApiMigrationTestClient(t)
	old := newFakeWindowPlatformTransport(true)
	next := newFakeWindowPlatformTransport(false)
	createdSettings := make(chan *PlatformTransportSettings, 1)
	state := &apiWindowClientTransport{
		current:       old,
		settings:      DefaultPlatformTransportSettings(),
		auth:          ClientAuth{InstanceId: NewId()},
		policyVersion: 1,
	}
	generator := &ApiMultiClientGenerator{
		settings:                   DefaultApiMultiClientGeneratorSettings(),
		platformTransportMode:      TransportModeH1,
		platformTransportPolicyVer: 1,
		transports:                 map[*Client]*apiWindowClientTransport{client: state},
		newPlatformTransport: func(
			client *Client,
			auth *ClientAuth,
			_ TransportMode,
			settings *PlatformTransportSettings,
		) apiWindowPlatformTransport {
			createdSettings <- settings
			return next
		},
	}

	preferences := map[TransportMode]int{
		TransportModeH3:        1,
		TransportModeH1:        1,
		TransportModeH3Dns:     2,
		TransportModeH3DnsPump: 3,
	}
	generator.SetPlatformTransportPolicy(TransportModeAuto, preferences)
	var settings *PlatformTransportSettings
	select {
	case settings = <-createdSettings:
	case <-time.After(time.Second):
		t.Fatal("policy change did not construct a replacement")
	}
	if !maps.Equal(settings.ModePreferences, preferences) {
		t.Fatalf("replacement preferences = %v, want %v", settings.ModePreferences, preferences)
	}
	select {
	case <-old.closed:
		t.Fatal("old transport closed before policy replacement connected")
	default:
	}

	next.connect()
	select {
	case <-old.closed:
	case <-time.After(time.Second):
		t.Fatal("old transport was not closed after policy replacement connected")
	}
	if state.current != next {
		t.Fatal("policy replacement was not installed")
	}

	// Canonically identical input is a no-op and cannot churn live routes.
	generator.SetPlatformTransportPolicy(TransportModeAuto, maps.Clone(preferences))
	select {
	case <-createdSettings:
		t.Fatal("identical policy constructed another replacement")
	case <-time.After(50 * time.Millisecond):
	}
}

// Every policy edge uses the same transition path, including a server-driven
// same-policy replacement versus a setter no-op. For changed policies this
// matrix holds the destination disconnected and proves the source remains the
// installed carrier, then connects the exact requested destination and proves
// it becomes current before the source is drained.
func TestApiWindowTransportPolicyTransitionMatrix(t *testing.T) {
	modes := []TransportMode{
		TransportModeH1,
		TransportModeH3,
		TransportModeH3Dns,
		TransportModeH3DnsPump,
		TransportModeAuto,
	}
	preferencesFor := func(mode TransportMode) map[TransportMode]int {
		if mode == TransportModeAuto {
			return DefaultTransportModePreferences()
		}
		return nil
	}

	for _, sourceMode := range modes {
		for _, targetMode := range modes {
			sourceMode := sourceMode
			targetMode := targetMode
			t.Run(string(sourceMode)+"_to_"+string(targetMode), func(t *testing.T) {
				client, _ := newApiMigrationTestClient(t)
				source := newFakeWindowPlatformTransport(true)
				t.Cleanup(source.Close)
				type creation struct {
					mode      TransportMode
					transport *fakeWindowPlatformTransport
				}
				created := make(chan creation, 1)
				state := &apiWindowClientTransport{
					current:       source,
					settings:      DefaultPlatformTransportSettings(),
					auth:          ClientAuth{InstanceId: NewId()},
					policyVersion: 1,
				}
				generatorSettings := DefaultApiMultiClientGeneratorSettings()
				generatorSettings.MigrateConnectTimeout = time.Second
				generator := &ApiMultiClientGenerator{
					settings:                   generatorSettings,
					platformTransportMode:      sourceMode,
					platformModePreferences:    preferencesFor(sourceMode),
					platformTransportPolicyVer: 1,
					transports:                 map[*Client]*apiWindowClientTransport{client: state},
					newPlatformTransport: func(
						_ *Client,
						_ *ClientAuth,
						mode TransportMode,
						_ *PlatformTransportSettings,
					) apiWindowPlatformTransport {
						next := newFakeWindowPlatformTransport(false)
						created <- creation{mode: mode, transport: next}
						return next
					},
				}

				generator.SetPlatformTransportPolicy(targetMode, preferencesFor(targetMode))
				if sourceMode == targetMode {
					select {
					case <-created:
						t.Fatal("identical policy constructed a replacement")
					case <-time.After(25 * time.Millisecond):
					}
					generator.transportLock.Lock()
					current := state.current
					migrating := state.migrating
					generator.transportLock.Unlock()
					if current != source || migrating || !source.IsConnected() {
						t.Fatal("identical policy disturbed the active source transport")
					}
					// A resident/server migration still replaces the carrier under the
					// unchanged policy, covering the five diagonal matrix entries.
					generator.MigrateClientTransport(client, nil, time.Now())
				}

				var replacement creation
				select {
				case replacement = <-created:
				case <-time.After(time.Second):
					t.Fatal("policy transition did not construct a replacement")
				}
				t.Cleanup(replacement.transport.Close)
				if replacement.mode != targetMode {
					t.Fatalf("constructed mode=%q want=%q", replacement.mode, targetMode)
				}
				select {
				case <-replacement.transport.waitStarted:
				case <-time.After(time.Second):
					t.Fatal("transition did not wait for destination activation")
				}
				generator.transportLock.Lock()
				currentBeforeActivation := state.current
				generator.transportLock.Unlock()
				if currentBeforeActivation != source {
					t.Fatal("destination was installed before it became active")
				}
				select {
				case <-source.closed:
					t.Fatal("source drained before destination activation")
				default:
				}

				replacement.transport.connect()
				if !waitForCondition(time.Second, func() bool {
					generator.transportLock.Lock()
					defer generator.transportLock.Unlock()
					return state.current == replacement.transport && !state.migrating
				}) {
					t.Fatal("active destination was not installed as current")
				}
				if !replacement.transport.IsConnected() {
					t.Fatal("installed destination is not active")
				}
				select {
				case <-source.closed:
				case <-time.After(time.Second):
					t.Fatal("source did not drain after destination activation")
				}
			})
		}
	}
}

func TestApiWindowExplicitPolicyKeepsOldCarrierUntilH3ConnectsWhenBudgetBlocked(t *testing.T) {
	client, _ := newApiMigrationTestClient(t)
	old := newFakeWindowPlatformTransport(true)
	next := newFakeWindowPlatformTransport(false)
	next.waitingForBudget = true
	created := make(chan struct{}, 1)
	state := &apiWindowClientTransport{
		current:       old,
		settings:      DefaultPlatformTransportSettings(),
		auth:          ClientAuth{InstanceId: NewId()},
		policyVersion: 1,
	}
	generator := &ApiMultiClientGenerator{
		settings:                   DefaultApiMultiClientGeneratorSettings(),
		platformTransportMode:      TransportModeAuto,
		platformModePreferences:    DefaultTransportModePreferences(),
		platformTransportPolicyVer: 1,
		transports:                 map[*Client]*apiWindowClientTransport{client: state},
		newPlatformTransport: func(
			client *Client,
			auth *ClientAuth,
			_ TransportMode,
			settings *PlatformTransportSettings,
		) apiWindowPlatformTransport {
			created <- struct{}{}
			return next
		},
	}

	generator.SetPlatformTransportPolicy(TransportModeH3, nil)
	select {
	case <-created:
	case <-time.After(time.Second):
		t.Fatal("explicit H3 policy did not construct a replacement")
	}
	select {
	case <-next.waitStarted:
	case <-time.After(time.Second):
		t.Fatal("explicit H3 migration did not start waiting for its replacement")
	}
	select {
	case <-old.closed:
		t.Fatal("budget-blocked explicit H3 closed H1 before H3 connected")
	default:
	}

	next.connect()
	currentTransport := func() apiWindowPlatformTransport {
		generator.transportLock.Lock()
		defer generator.transportLock.Unlock()
		return state.current
	}
	for deadline := time.Now().Add(time.Second); currentTransport() != next && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	if currentTransport() != next {
		t.Fatal("explicit H3 replacement was not installed after connecting")
	}
	select {
	case <-old.closed:
	case <-time.After(time.Second):
		t.Fatal("old H1 carrier did not drain after explicit H3 connected")
	}
}

// This is the complete switching regression: Auto has a live H1 route and its
// shared budget has no room or socket slot for H3. An explicit H3 policy must
// nevertheless establish and authenticate a real QUIC replacement while H1
// remains usable, then close H1 only after the H3 route is published. The
// server auth barrier makes both sides of that ordering deterministic.
func TestApiWindowAutoH1SaturatedBudgetToExplicitH3IsMakeBeforeBreak(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testApiWindowAutoH1SaturatedBudgetToExplicitH3IsMakeBeforeBreak(t, ipVersion)
	})
}

func testApiWindowAutoH1SaturatedBudgetToExplicitH3IsMakeBeforeBreak(t *testing.T, ipVersion int) {
	if ipVersion == 6 {
	}
	// the quic endpoint and its certificate live on the family's loopback, so
	// the replacement H3 carrier binds a socket of that family (IPV6.md A7)
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
	const nextProto = "urnetwork-auto-h1-to-explicit-h3-test"
	listener, err := quic.ListenAddrEarly(
		testLoopbackHostPort(ipVersion, 0),
		&tls.Config{
			Certificates: []tls.Certificate{cert},
			NextProtos:   []string{nextProto},
		},
		&quic.Config{MaxIdleTimeout: 30 * time.Second},
	)
	if err != nil {
		t.Fatal(err)
	}
	serverCtx, serverCancel := context.WithCancel(t.Context())
	authRead := make(chan struct{})
	releaseAuth := make(chan struct{})
	serverErrors := make(chan error, 1)
	serverDone := make(chan struct{})
	var releaseAuthOnce sync.Once
	releaseServerAuth := func() {
		releaseAuthOnce.Do(func() { close(releaseAuth) })
	}
	t.Cleanup(func() {
		releaseServerAuth()
		serverCancel()
		_ = listener.Close()
		select {
		case <-serverDone:
		case <-time.After(5 * time.Second):
		}
	})
	go func() {
		defer close(serverDone)
		connection, acceptErr := listener.Accept(serverCtx)
		if acceptErr != nil {
			if serverCtx.Err() == nil {
				serverErrors <- acceptErr
			}
			return
		}
		stream, acceptErr := connection.AcceptStream(serverCtx)
		if acceptErr != nil {
			serverErrors <- acceptErr
			return
		}
		framer := NewFramer(DefaultFramerSettings(int(DefaultClientSettings().MinimumMessageLenLimit())))
		authBytes, readErr := framer.Read(stream)
		if readErr != nil {
			serverErrors <- readErr
			return
		}
		defer MessagePoolReturn(authBytes)
		close(authRead)
		select {
		case <-releaseAuth:
		case <-serverCtx.Done():
			return
		}
		if writeErr := framer.Write(stream, authBytes); writeErr != nil {
			serverErrors <- writeErr
			return
		}
		<-connection.Context().Done()
	}()

	platform := newTestingPlatformServerOnFamily(t, ipVersion)
	client, _ := newApiMigrationTestClient(t)
	strategy := NewClientStrategyWithDefaults(client.Ctx())
	budget := NewPlatformTransportBudget(4, 1)
	settings := testingPlatformTransportSettings()
	settings.PlatformTransportBudget = budget
	settings.H1BudgetByteCount = 1
	settings.H3BudgetByteCount = 4
	settings.ModeInitialDelay = 0
	settings.ModePreferences = map[TransportMode]int{
		TransportModeH1: 1,
		TransportModeH3: 2,
	}
	settings.H3Port = listener.Addr().(*net.UDPAddr).Port
	settings.QuicTlsConfig = &tls.Config{
		InsecureSkipVerify: true, // test-only self-signed endpoint
		NextProtos:         []string{nextProto},
	}
	auth := ClientAuth{
		ByJwt:      "testing",
		InstanceId: NewId(),
		AppVersion: "testing",
	}
	old := NewPlatformTransportWithTargetMode(
		client.Ctx(),
		strategy,
		client.RouteManager(),
		platform.url,
		&auth,
		TransportModeAuto,
		settings,
	)
	t.Cleanup(old.Close)
	if !testingWaitForActiveMode(old, TransportModeH1, 5*time.Second) {
		t.Fatal("saturated Auto transport did not establish its H1 route")
	}
	if stats := budget.Stats(); stats.UsedByteCount != 1 ||
		stats.UsedTransportCount != 1 {
		t.Fatalf("Auto did not begin with exactly one budget-saturating H1 carrier: %+v", stats)
	}

	created := make(chan *PlatformTransport, 1)
	generatorSettings := DefaultApiMultiClientGeneratorSettings()
	generatorSettings.MigrateConnectTimeout = 5 * time.Second
	generatorSettings.PlatformTransportCreated = func(_ *Client, transport *PlatformTransport) {
		created <- transport
	}
	state := &apiWindowClientTransport{
		current:       old,
		settings:      settings,
		auth:          auth,
		policyVersion: 1,
	}
	generator := &ApiMultiClientGenerator{
		ctx:                        client.Ctx(),
		clientStrategy:             strategy,
		platformUrl:                platform.url,
		settings:                   generatorSettings,
		platformTransportMode:      TransportModeAuto,
		platformModePreferences:    maps.Clone(settings.ModePreferences),
		platformTransportPolicyVer: 1,
		transports:                 map[*Client]*apiWindowClientTransport{client: state},
	}

	generator.SetPlatformTransportPolicy(TransportModeH3, nil)
	var next *PlatformTransport
	select {
	case next = <-created:
	case serverErr := <-serverErrors:
		t.Fatal(serverErr)
	case <-time.After(5 * time.Second):
		t.Fatal("explicit H3 policy did not construct a replacement")
	}
	t.Cleanup(next.Close)
	select {
	case <-authRead:
	case serverErr := <-serverErrors:
		t.Fatal(serverErr)
	case <-time.After(5 * time.Second):
		t.Fatal("budget handoff did not let explicit H3 reach authentication")
	}

	// H3 owns a temporary claim, but it has not authenticated or published a
	// route. H1 must still be the installed, connected carrier at this barrier.
	if next.IsConnected() {
		t.Fatal("H3 reported connected before its authentication response")
	}
	if !old.IsConnected() {
		t.Fatal("old H1 route disappeared before H3 authenticated")
	}
	generator.transportLock.Lock()
	currentBeforeAuth := state.current
	generator.transportLock.Unlock()
	if currentBeforeAuth != old {
		t.Fatal("window replaced H1 before H3 authenticated")
	}
	stats := budget.Stats()
	if stats.ActiveHandoffCount != 1 || stats.ActiveHandoffByteCount != 1 ||
		stats.ActiveHandoffTransportCount != 1 ||
		stats.UsedByteCount != 5 || stats.UsedTransportCount != 2 {
		t.Fatalf("H1/H3 authentication overlap escaped its one-H1 bound: %+v", stats)
	}

	releaseServerAuth()
	if !testingWaitForActiveMode(next, TransportModeH3, 5*time.Second) {
		t.Fatal("explicit H3 did not publish a route after authentication")
	}
	if !waitForCondition(5*time.Second, func() bool {
		generator.transportLock.Lock()
		defer generator.transportLock.Unlock()
		return state.current == next
	}) {
		t.Fatal("authenticated H3 replacement was not installed")
	}
	select {
	case <-old.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("old H1 transport did not drain after H3 connected")
	}
	if !waitForCondition(5*time.Second, func() bool {
		stats := budget.Stats()
		return stats.ActiveHandoffCount == 0 &&
			stats.UsedByteCount == 4 && stats.UsedTransportCount == 1
	}) {
		t.Fatalf("budget did not return to the explicit-H3 cap after H1 drain: %+v", budget.Stats())
	}
}

// A low-memory H3 -> H3-family replacement cannot retain two full H3 working
// sets. If the first replacement dial misses the migration timeout after the
// old carrier is released, the destination must remain installed and continue
// reconnecting; retaining the closed source handle would permanently strand
// the window. The controlled runner activates only after that terminal state.
func TestApiWindowBudgetBreakKeepsRetryingH3DestinationInstalled(t *testing.T) {
	client, _ := newApiMigrationTestClient(t)
	strategy := NewClientStrategyWithDefaults(client.Ctx())
	budget := NewPlatformTransportBudget(4, 1)
	auth := ClientAuth{InstanceId: NewId()}

	oldAssigned := make(chan struct{})
	oldActive := make(chan struct{})
	var old *PlatformTransport
	oldSettings := testingPlatformTransportSettings()
	oldSettings.PlatformTransportBudget = budget
	oldSettings.H3BudgetByteCount = 4
	oldSettings.runH3ModeForTest = func(ctx context.Context, mode TransportMode, _ time.Duration) {
		<-oldAssigned
		old.setModeAvailable(mode, true)
		old.setRegistered(true)
		close(oldActive)
		<-ctx.Done()
		old.setRegistered(false)
		old.setModeAvailable(mode, false)
	}
	old = NewPlatformTransportWithTargetMode(
		client.Ctx(),
		strategy,
		client.RouteManager(),
		"https://127.0.0.1",
		&auth,
		TransportModeH3,
		oldSettings,
	)
	close(oldAssigned)
	t.Cleanup(old.Close)
	select {
	case <-oldActive:
	case <-time.After(time.Second):
		t.Fatal("old H3 did not acquire the saturated budget")
	}

	created := make(chan *PlatformTransport, 1)
	nextRunnerStarted := make(chan struct{})
	activateNext := make(chan struct{})
	state := &apiWindowClientTransport{
		current:       old,
		settings:      oldSettings,
		auth:          auth,
		policyVersion: 1,
	}
	generatorSettings := DefaultApiMultiClientGeneratorSettings()
	generatorSettings.MigrateConnectTimeout = 25 * time.Millisecond
	generator := &ApiMultiClientGenerator{
		settings:                   generatorSettings,
		clientStrategy:             strategy,
		platformTransportMode:      TransportModeH3,
		platformTransportPolicyVer: 1,
		transports:                 map[*Client]*apiWindowClientTransport{client: state},
		newPlatformTransport: func(
			client *Client,
			auth *ClientAuth,
			targetMode TransportMode,
			settings *PlatformTransportSettings,
		) apiWindowPlatformTransport {
			nextAssigned := make(chan struct{})
			var next *PlatformTransport
			settingsValue := *settings
			settingsValue.runH3ModeForTest = func(ctx context.Context, mode TransportMode, _ time.Duration) {
				<-nextAssigned
				close(nextRunnerStarted)
				select {
				case <-activateNext:
					next.setModeAvailable(mode, true)
					next.setRegistered(true)
					<-ctx.Done()
					next.setRegistered(false)
					next.setModeAvailable(mode, false)
				case <-ctx.Done():
				}
			}
			next = NewPlatformTransportWithTargetMode(
				client.Ctx(),
				strategy,
				client.RouteManager(),
				"https://127.0.0.1",
				auth,
				targetMode,
				&settingsValue,
			)
			close(nextAssigned)
			created <- next
			return next
		},
	}

	generator.MigrateClientTransport(client, nil, time.Now())
	var next *PlatformTransport
	select {
	case next = <-created:
	case <-time.After(time.Second):
		t.Fatal("H3 replacement was not constructed")
	}
	t.Cleanup(next.Close)
	select {
	case <-old.Done():
	case <-time.After(time.Second):
		t.Fatal("budget-blocking old H3 was not released")
	}
	select {
	case <-nextRunnerStarted:
	case <-time.After(time.Second):
		t.Fatal("replacement H3 did not acquire the released budget")
	}
	if !waitForCondition(time.Second, func() bool {
		generator.transportLock.Lock()
		defer generator.transportLock.Unlock()
		return state.current == next && !state.migrating
	}) {
		t.Fatal("timed-out replacement was not retained as the reconnect owner")
	}
	select {
	case <-next.Done():
		t.Fatal("retrying replacement was closed at the migration timeout")
	default:
	}
	if stats := budget.Stats(); stats.UsedByteCount != 4 ||
		stats.UsedTransportCount != 1 {
		t.Fatalf("retrying destination does not own the H3 budget: %+v", stats)
	}

	close(activateNext)
	if !waitForCondition(time.Second, next.IsConnected) {
		t.Fatal("installed replacement did not become active on its later retry")
	}
}
