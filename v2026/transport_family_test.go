package connect

// transport_family_test.go — family-pinned platform transports, the provider
// transport group, and the per-family H3 socket and race (IPV6.md A2, A4-A7,
// C6). Servers stand on the loopback of one family so that connecting at all
// proves which family was dialed. The dual-stack hostname is answered by an
// owned wire DNS fixture with both loopbacks, so a pin decides the family
// independently of the host machine's localhost aliases.

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	quic "github.com/quic-go/quic-go"

	"github.com/urnetwork/connect/v2026/protocol"
)

const familyTransportTestHost = "platform.family.test"

// Keep the real resolver and transport paths, supplying only the DNS records
// through the existing caller-owned resolver setting. Literal-only URLs would
// fail to exercise selection when both address families are offered.
func newTestingFamilyStrategySettings(t *testing.T) *ClientStrategySettings {
	t.Helper()
	settings := DefaultClientStrategySettings()
	settings.ConnectSettings.Resolver = newFamilyTestResolver(t,
		netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("::1"))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	addrs, err := settings.ConnectSettings.Resolver.LookupIPAddr(ctx, familyTransportTestHost)
	if err != nil {
		t.Fatalf("owned family hostname must resolve through its wire fixture: %v", err)
	}
	has4, has6 := false, false
	for _, addr := range addrs {
		switch addr.IP.String() {
		case "127.0.0.1":
			has4 = true
		case "::1":
			has6 = true
		}
	}
	if len(addrs) != 2 || !has4 || !has6 {
		t.Fatalf("owned family hostname must offer exactly both loopbacks, got %v", addrs)
	}
	return settings
}

// testingFamilyConnection is one accepted platform-side websocket.
type testingFamilyConnection struct {
	remoteFamily int
	intent       string
	conn         *websocket.Conn
}

// testingFamilyPlatformServer is a silent websocket platform bound to one
// loopback family. It records the family and the intent header of every
// connection, and optionally echoes a v1 auth frame.
type testingFamilyPlatformServer struct {
	server       *httptest.Server
	port         int
	connections  chan testingFamilyConnection
	connectCount atomic.Int64
	echoAuth     bool

	stateLock sync.Mutex
	conns     []*websocket.Conn
}

func newTestingFamilyPlatformServer(t *testing.T, ipVersion int, echoAuth bool) *testingFamilyPlatformServer {
	t.Helper()
	return newTestingFamilyPlatformServerOnPort(t, ipVersion, 0, echoAuth)
}

func newTestingFamilyPlatformServerOnPort(t *testing.T, ipVersion int, port int, echoAuth bool) *testingFamilyPlatformServer {
	t.Helper()
	platform := &testingFamilyPlatformServer{
		connections: make(chan testingFamilyConnection, 16),
		echoAuth:    echoAuth,
	}
	upgrader := websocket.Upgrader{
		CheckOrigin: func(r *http.Request) bool { return true },
	}
	listener, err := net.Listen(testTcpNetwork(ipVersion), testLoopbackHostPort(ipVersion, port))
	if err != nil {
		t.Fatal(err)
	}
	platform.port = listener.Addr().(*net.TCPAddr).Port
	platform.server = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ws, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		platform.connectCount.Add(1)
		func() {
			platform.stateLock.Lock()
			defer platform.stateLock.Unlock()
			platform.conns = append(platform.conns, ws)
		}()
		remoteFamily := 0
		if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
			if ip := net.ParseIP(host); ip != nil {
				remoteFamily = 6
				if ip.To4() != nil {
					remoteFamily = 4
				}
			}
		}
		platform.connections <- testingFamilyConnection{
			remoteFamily: remoteFamily,
			intent:       r.Header.Get(HeaderIpFamily),
			conn:         ws,
		}
		first := true
		for {
			messageType, message, err := ws.ReadMessage()
			if err != nil {
				ws.Close()
				return
			}
			if first && platform.echoAuth {
				first = false
				if err := ws.WriteMessage(messageType, message); err != nil {
					ws.Close()
					return
				}
			}
		}
	}))
	platform.server.Listener = listener
	platform.server.Start()
	t.Cleanup(func() {
		platform.closeConns()
		platform.server.Close()
	})
	return platform
}

func (self *testingFamilyPlatformServer) closeConns() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	for _, ws := range self.conns {
		ws.Close()
	}
	self.conns = nil
}

// url is the ws url by NAME, so the transport's pin decides the family.
func (self *testingFamilyPlatformServer) url() string {
	return fmt.Sprintf("ws://localhost:%d", self.port)
}

func (self *testingFamilyPlatformServer) dualStackURL() string {
	return fmt.Sprintf("ws://%s:%d", familyTransportTestHost, self.port)
}

func receiveFamilyConnection(t *testing.T, platform *testingFamilyPlatformServer, timeout time.Duration) testingFamilyConnection {
	t.Helper()
	select {
	case connection := <-platform.connections:
		return connection
	case <-time.After(timeout):
		t.Fatal("no connection arrived")
		return testingFamilyConnection{}
	}
}

func testingFamilyTransportSettings() *PlatformTransportSettings {
	settings := testingPlatformTransportSettings()
	// keep the pinned backoff short enough for a test to see a recovery
	settings.PinnedReconnectMaxTimeout = 200 * time.Millisecond
	return settings
}

func testingFamilyAuth() *ClientAuth {
	return &ClientAuth{
		ByJwt:      "testing",
		InstanceId: NewId(),
		AppVersion: "testing",
	}
}

// newTestingPinnedTransport is a pinned H1 transport with its own direct
// strategy, the shape the group builds.
func newTestingPinnedTransport(t *testing.T, ctx context.Context, clientSettings *ClientStrategySettings, platformUrl string, ipFamily int, settings *PlatformTransportSettings) *PlatformTransport {
	t.Helper()
	settings.IpFamily = ipFamily
	strategy := NewDirectClientStrategy(ctx, clientSettings, ipFamily)
	t.Cleanup(strategy.Close)
	transport := NewPlatformTransportWithTargetMode(
		ctx,
		strategy,
		NewRouteManager(ctx, "family"),
		platformUrl,
		testingFamilyAuth(),
		TransportModeH1,
		settings,
	)
	t.Cleanup(transport.Close)
	return transport
}

func waitForTransportState(transport *PlatformTransport, want PlatformTransportState, timeout time.Duration) bool {
	return waitForCondition(timeout, func() bool {
		return transport.State() == want
	})
}

// A pinned transport dials only its family and declares it. The server of
// the other family never sees it.
func TestFamilyPinnedTransportDialsOnlyItsFamilyAndDeclaresIt(t *testing.T) {
	clientSettings := newTestingFamilyStrategySettings(t)
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingFamilyPlatformServer(t, ipVersion, false)
		transport := newTestingPinnedTransport(t, ctx, clientSettings, platform.dualStackURL(), ipVersion, testingFamilyTransportSettings())
		if transport.IpFamily() != ipVersion {
			t.Fatalf("IpFamily = %d, want %d", transport.IpFamily(), ipVersion)
		}

		connection := receiveFamilyConnection(t, platform, 15*time.Second)
		if connection.remoteFamily != ipVersion {
			t.Fatalf("connected over ipv%d, want ipv%d", connection.remoteFamily, ipVersion)
		}
		if want := fmt.Sprintf("%d", ipVersion); connection.intent != want {
			t.Fatalf("%s = %q, want %q", HeaderIpFamily, connection.intent, want)
		}
		if !waitForTransportState(transport, PlatformTransportStateConnected, 15*time.Second) {
			t.Fatalf("state = %s, want connected", transport.State())
		}

		// the other pin cannot reach a server that only listens on this
		// family: it must not connect, and its failures are its own
		other := 4
		if ipVersion == 4 {
			other = 6
		}
		otherPlatformCount := platform.connectCount.Load()
		otherTransport := newTestingPinnedTransport(t, ctx, clientSettings, platform.dualStackURL(), other, testingFamilyTransportSettings())
		if waitForCondition(1500*time.Millisecond, func() bool {
			return otherTransport.IsConnected() || otherPlatformCount < platform.connectCount.Load()
		}) {
			t.Fatalf("an ipv%d pin reached an ipv%d-only platform", other, ipVersion)
		}
		if otherTransport.State() != PlatformTransportStateConnecting {
			t.Fatalf("other state = %s, want connecting", otherTransport.State())
		}
	})
}

// A family-agnostic transport declares nothing: the platform reads its
// connection as legacy.
func TestFamilyAgnosticTransportSendsNoIntent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	platform := newTestingFamilyPlatformServer(t, 4, false)
	transport := testingPlatformTransport(t, ctx, platform.url(), testingFamilyTransportSettings())
	connection := receiveFamilyConnection(t, platform, 15*time.Second)
	if connection.intent != "" {
		t.Fatalf("%s = %q on a family-agnostic transport, want none", HeaderIpFamily, connection.intent)
	}
	if transport.IpFamily() != 0 {
		t.Fatalf("IpFamily = %d, want 0", transport.IpFamily())
	}
}

// The v1 in-band auth frame carries the same intent as the v2 header.
func TestFamilyPinnedTransportV1AuthFrameCarriesIntent(t *testing.T) {
	clientSettings := newTestingFamilyStrategySettings(t)
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingFamilyPlatformServer(t, ipVersion, true)
		settings := testingFamilyTransportSettings()
		settings.V2H1Auth = false
		intents := make(chan int32, 4)
		settings.AuthFrameObserver = func(authFrameBytes []byte) {
			decoded, err := DecodeFrame(authFrameBytes)
			if err != nil {
				return
			}
			if auth, ok := decoded.(*protocol.Auth); ok {
				intents <- auth.IpFamily
			}
		}
		transport := newTestingPinnedTransport(t, ctx, clientSettings, platform.dualStackURL(), ipVersion, settings)
		select {
		case intent := <-intents:
			if intent != int32(ipVersion) {
				t.Fatalf("auth ip_family = %d, want %d", intent, ipVersion)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("no auth frame was built")
		}
		if !waitForTransportState(transport, PlatformTransportStateConnected, 15*time.Second) {
			t.Fatalf("state = %s, want connected", transport.State())
		}
	})
}

// A Force that contradicts the pin idles the transport: no dial, no backend
// failure, no spin. Setting the policy back releases it.
func TestFamilyPinnedTransportIdlesUnderContradictingForce(t *testing.T) {
	clientSettings := newTestingFamilyStrategySettings(t)
	SetControlIpFamilyPolicy(IpFamilyForce4)
	t.Cleanup(func() { SetControlIpFamilyPolicy(IpFamilyAuto) })
	noteBackendSuccess()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	platform := newTestingFamilyPlatformServer(t, 6, false)
	transport := newTestingPinnedTransport(t, ctx, clientSettings, platform.dualStackURL(), 6, testingFamilyTransportSettings())
	if !waitForTransportState(transport, PlatformTransportStateIdlePolicy, 5*time.Second) {
		t.Fatalf("state = %s, want idle-policy", transport.State())
	}
	time.Sleep(300 * time.Millisecond)
	if n := platform.connectCount.Load(); n != 0 {
		t.Fatalf("an idled transport connected %d times", n)
	}
	if n := consecutiveBackendFails.Load(); n != 0 {
		t.Fatalf("an idled transport recorded %d backend failures", n)
	}

	SetControlIpFamilyPolicy(IpFamilyAuto)
	connection := receiveFamilyConnection(t, platform, 15*time.Second)
	if connection.remoteFamily != 6 {
		t.Fatalf("connected over ipv%d after the policy lifted, want ipv6", connection.remoteFamily)
	}
	if !waitForTransportState(transport, PlatformTransportStateConnected, 15*time.Second) {
		t.Fatalf("state = %s, want connected", transport.State())
	}
}

// Without a path of its family the transport sleeps instead of dialing, and a
// network change re-probes and wakes it.
func TestFamilyPinnedTransportSleepsWithoutFamilySupport(t *testing.T) {
	clientSettings := newTestingFamilyStrategySettings(t)
	var supported atomic.Bool
	restore := swapControlFamilyProbe(func(family int) bool {
		if family == 6 {
			return supported.Load()
		}
		return true
	})
	t.Cleanup(restore)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	platform := newTestingFamilyPlatformServer(t, 6, false)
	transport := newTestingPinnedTransport(t, ctx, clientSettings, platform.dualStackURL(), 6, testingFamilyTransportSettings())
	if !waitForTransportState(transport, PlatformTransportStateSleeping, 5*time.Second) {
		t.Fatalf("state = %s, want sleeping", transport.State())
	}
	time.Sleep(300 * time.Millisecond)
	if n := platform.connectCount.Load(); n != 0 {
		t.Fatalf("a sleeping transport connected %d times", n)
	}

	supported.Store(true)
	NetworkChanged()
	connection := receiveFamilyConnection(t, platform, 15*time.Second)
	if connection.remoteFamily != 6 {
		t.Fatalf("connected over ipv%d after waking, want ipv6", connection.remoteFamily)
	}
	if !waitForTransportState(transport, PlatformTransportStateConnected, 15*time.Second) {
		t.Fatalf("state = %s, want connected", transport.State())
	}

	// the family going away again puts it back to sleep and drops the
	// connection
	supported.Store(false)
	NetworkChanged()
	if !waitForTransportState(transport, PlatformTransportStateSleeping, 5*time.Second) {
		t.Fatalf("state = %s after the family went away, want sleeping", transport.State())
	}
	if !waitForCondition(5*time.Second, func() bool { return !transport.IsConnected() }) {
		t.Fatal("a sleeping transport kept its connection")
	}
}

// A pinned transport's dial failures feed only its own backoff, never the
// process-wide backend-degraded gate.
func TestFamilyPinnedTransportFailuresDoNotDegradeBackend(t *testing.T) {
	clientSettings := newTestingFamilyStrategySettings(t)
	noteBackendSuccess()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// a port nothing listens on
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	settings := testingFamilyTransportSettings()
	settings.ReconnectTimeout = time.Millisecond
	settings.PinnedReconnectMaxTimeout = 10 * time.Millisecond
	transport := newTestingPinnedTransport(t, ctx, clientSettings, fmt.Sprintf("ws://%s:%d", familyTransportTestHost, port), 6, settings)
	// a failed strategy dial reports only after the strategy has exhausted
	// its dialers, so wait for the first recorded failure rather than sleep
	if !waitForCondition(30*time.Second, func() bool {
		return 0 < transport.pinnedBackoff.delay()
	}) {
		t.Fatal("pinned failures did not grow the transport's own backoff")
	}
	if transport.IsConnected() {
		t.Fatal("connected to a closed port")
	}
	if n := consecutiveBackendFails.Load(); n != 0 {
		t.Fatalf("pinned dial failures recorded %d backend failures", n)
	}
	if isBackendDegraded() {
		t.Fatal("pinned dial failures degraded the backend")
	}
}

// The owner's switch parks and resumes a transport, dropping the live
// connection when parked.
func TestPlatformTransportSetEnabledParksAndResumes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	platform := newTestingFamilyPlatformServer(t, 4, false)
	settings := testingFamilyTransportSettings()
	settings.StartDisabled = true
	transport := testingPlatformTransport(t, ctx, platform.url(), settings)
	if transport.State() != PlatformTransportStateDisabled {
		t.Fatalf("state = %s, want disabled", transport.State())
	}
	time.Sleep(300 * time.Millisecond)
	if n := platform.connectCount.Load(); n != 0 {
		t.Fatalf("a disabled transport connected %d times", n)
	}

	transport.SetEnabled(true)
	receiveFamilyConnection(t, platform, 15*time.Second)
	if !waitForTransportState(transport, PlatformTransportStateConnected, 15*time.Second) {
		t.Fatalf("state = %s, want connected", transport.State())
	}

	transport.SetEnabled(false)
	if !waitForCondition(5*time.Second, func() bool { return !transport.IsConnected() }) {
		t.Fatal("a disabled transport kept its connection")
	}
	connectCount := platform.connectCount.Load()
	time.Sleep(300 * time.Millisecond)
	if platform.connectCount.Load() != connectCount {
		t.Fatal("a disabled transport re-dialed")
	}

	transport.SetEnabled(true)
	receiveFamilyConnection(t, platform, 15*time.Second)
	if !waitForTransportState(transport, PlatformTransportStateConnected, 15*time.Second) {
		t.Fatalf("state = %s after re-enable, want connected", transport.State())
	}
}

// The group's standby dials only after the delay with no pinned transport
// connected, and stands down when a pinned transport connects.
func TestFamilyPlatformTransportGroupStandby(t *testing.T) {
	clientSettings := newTestingFamilyStrategySettings(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// reserve a v4 port for the pinned platform to appear on later, and a
	// dead port for the v6 pin
	reserve := func(network string, host string) int {
		listener, err := net.Listen(network, net.JoinHostPort(host, "0"))
		if err != nil {
			t.Fatal(err)
		}
		port := listener.Addr().(*net.TCPAddr).Port
		listener.Close()
		return port
	}
	v4Port := reserve("tcp4", "127.0.0.1")
	v6Port := reserve("tcp6", "::1")
	standbyPlatform := newTestingFamilyPlatformServer(t, 4, false)

	settings := testingFamilyTransportSettings()
	settings.ReconnectTimeout = 20 * time.Millisecond
	settings.PinnedReconnectMaxTimeout = 100 * time.Millisecond
	groupSettings := &FamilyPlatformTransportGroupSettings{StandbyDelay: 400 * time.Millisecond}
	strategy := NewClientStrategy(ctx, clientSettings)
	defer strategy.Close()
	group := NewFamilyPlatformTransportGroup(
		ctx,
		clientSettings,
		strategy,
		NewRouteManager(ctx, "group"),
		standbyPlatform.dualStackURL(),
		fmt.Sprintf("ws://%s:%d", familyTransportTestHost, v4Port),
		fmt.Sprintf("ws://%s:%d", familyTransportTestHost, v6Port),
		testingFamilyAuth(),
		TransportModeH1,
		settings,
		groupSettings,
	)
	defer group.Close()

	status := group.Status()
	if !status.HasIpv4 || !status.HasIpv6 {
		t.Fatalf("status = %+v, want both pins", status)
	}
	if status.StandbyActive || status.Standby != PlatformTransportStateDisabled {
		t.Fatalf("standby active before the delay: %+v", status)
	}
	time.Sleep(150 * time.Millisecond)
	if n := standbyPlatform.connectCount.Load(); n != 0 {
		t.Fatalf("standby connected %d times before the delay", n)
	}
	if group.IsConnected() {
		t.Fatal("group connected with nothing listening")
	}

	receiveFamilyConnection(t, standbyPlatform, 15*time.Second)
	if !waitForCondition(15*time.Second, group.IsConnected) {
		t.Fatal("standby never connected")
	}
	status = group.Status()
	if !status.StandbyActive || status.Standby != PlatformTransportStateConnected {
		t.Fatalf("standby status after the delay = %+v", status)
	}
	if status.Ipv4 != PlatformTransportStateConnecting || status.Ipv6 != PlatformTransportStateConnecting {
		t.Fatalf("pinned status with dead ports = %+v", status)
	}

	// the v4 platform appears: the v4 pin connects and the standby stands
	// down, and the group stays connected throughout
	v4Platform := newTestingFamilyPlatformServerOnPort(t, 4, v4Port, false)
	connection := receiveFamilyConnection(t, v4Platform, 15*time.Second)
	if connection.remoteFamily != 4 || connection.intent != "4" {
		t.Fatalf("v4 pin connection = %+v", connection)
	}
	if !waitForCondition(15*time.Second, func() bool {
		status := group.Status()
		return status.Ipv4 == PlatformTransportStateConnected &&
			!status.StandbyActive &&
			!group.StandbyTransport().IsConnected()
	}) {
		t.Fatalf("standby did not stand down: %+v", group.Status())
	}
	if !group.IsConnected() {
		t.Fatal("group lost its connection during the handover")
	}
	if len(group.Transports()) != 3 {
		t.Fatalf("transports = %d, want 3", len(group.Transports()))
	}
}

// With no family url configured the group is the legacy single transport and
// the standby dials at once.
func TestFamilyPlatformTransportGroupWithoutPinsIsLegacy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	platform := newTestingFamilyPlatformServer(t, 4, false)
	strategy := NewClientStrategyWithDefaults(ctx)
	defer strategy.Close()
	group := NewFamilyPlatformTransportGroup(
		ctx,
		DefaultClientStrategySettings(),
		strategy,
		NewRouteManager(ctx, "group"),
		platform.url(),
		"",
		"",
		testingFamilyAuth(),
		TransportModeH1,
		testingFamilyTransportSettings(),
		&FamilyPlatformTransportGroupSettings{StandbyDelay: time.Hour},
	)
	defer group.Close()
	connection := receiveFamilyConnection(t, platform, 15*time.Second)
	if connection.intent != "" {
		t.Fatalf("legacy standby declared %q", connection.intent)
	}
	status := group.Status()
	if status.HasIpv4 || status.HasIpv6 || !status.StandbyActive {
		t.Fatalf("status = %+v", status)
	}
	if len(group.Transports()) != 1 {
		t.Fatalf("transports = %d, want 1", len(group.Transports()))
	}
}

// The direct strategy keeps the normal and resilient dialers and nothing that
// would prove a third party's family.
func TestNewDirectClientStrategyDropsExtendersAndProxy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientStrategySettings()
	settings.ExtenderDirectory = NewExtenderDirectoryWithDefaults(ctx)
	defer settings.ExtenderDirectory.Close()
	settings.ExtenderConfigs = []*ExtenderConfig{}
	settings.ConnectSettings.ProxySettings = &ProxySettings{Network: "tcp", Address: "127.0.0.1:1080"}
	plain := NewClientStrategy(ctx, DefaultClientStrategySettings())
	defer plain.Close()
	direct := NewDirectClientStrategy(ctx, settings, 6)
	defer direct.Close()

	if direct.settings.ConnectSettings.ProxySettings != nil {
		t.Fatal("direct strategy kept the proxy")
	}
	if direct.settings.ExtenderDirectory != nil || direct.settings.MaxExtenderCount != 0 {
		t.Fatal("direct strategy kept extender discovery")
	}
	if len(direct.dialers) != len(plain.dialers) {
		t.Fatalf("direct dialers = %d, want the plain set of %d", len(direct.dialers), len(plain.dialers))
	}
	// the caller's settings are untouched
	if settings.ConnectSettings.ProxySettings == nil || settings.ExtenderDirectory == nil {
		t.Fatal("caller settings were mutated")
	}
	if direct.settings.ConnectSettings.DialContextSettings == nil {
		t.Fatal("direct strategy did not install the pin")
	}
	// the pin refuses the other family below resolution
	dial := direct.settings.ConnectSettings.DialContextSettings.DialContext
	if _, err := dial(ctx, "tcp", "127.0.0.1:1"); err == nil {
		t.Fatal("v6 pin dialed a v4 literal")
	}
	if _, err := dial(ctx, "tcp4", "localhost:1"); err == nil {
		t.Fatal("v6 pin accepted tcp4")
	}
}

func TestPinnedDialNetwork(t *testing.T) {
	cases := []struct {
		network string
		family  int
		want    string
		wantErr bool
	}{
		{"tcp", 4, "tcp4", false},
		{"tcp", 6, "tcp6", false},
		{"udp", 6, "udp6", false},
		{"tcp6", 6, "tcp6", false},
		{"tcp4", 6, "", true},
		{"udp6", 4, "", true},
		{"unix", 6, "unix", false},
		{"tcp", 0, "tcp", false},
	}
	for _, c := range cases {
		got, err := pinnedDialNetwork(c.network, c.family)
		if (err != nil) != c.wantErr || got != c.want {
			t.Fatalf("pinnedDialNetwork(%q, %d) = %q, %v; want %q, err=%t", c.network, c.family, got, err, c.want, c.wantErr)
		}
	}
	ctx := withPinnedIpFamily(context.Background(), 6)
	if got := pinnedIpFamilyFromContext(ctx); got != 6 {
		t.Fatalf("context pin = %d, want 6", got)
	}
	if got := pinnedIpFamilyFromContext(withPinnedIpFamily(context.Background(), 5)); got != 0 {
		t.Fatalf("invalid pin = %d, want 0", got)
	}
}

// resolveControlUDPAddrs narrows to the pin, refuses a contradicting literal,
// and interleaves a family-agnostic answer v6 first.
func TestResolveControlUDPAddrs(t *testing.T) {
	clientSettings := newTestingFamilyStrategySettings(t)
	ctx := context.Background()
	strategy := NewClientStrategy(ctx, clientSettings)
	defer strategy.Close()

	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		udpAddrs, err := strategy.resolveControlUDPAddrs(ctx, familyTransportTestHost+":443", ipVersion)
		if err != nil {
			t.Fatal(err)
		}
		if len(udpAddrs) != 1 {
			t.Fatalf("ipv%d pin resolved %v, want its one loopback", ipVersion, udpAddrs)
		}
		for _, udpAddr := range udpAddrs {
			if udpAddrFamily(udpAddr) != ipVersion || udpAddr.Port != 443 {
				t.Fatalf("ipv%d pin resolved %v", ipVersion, udpAddr)
			}
		}
		literal := testLoopbackHostPort(ipVersion, 53)
		if single, err := strategy.resolveControlUDPAddrs(ctx, literal, ipVersion); err != nil || len(single) != 1 {
			t.Fatalf("literal %s = %v, %v", literal, single, err)
		}
		other := 4
		if ipVersion == 4 {
			other = 6
		}
		if _, err := strategy.resolveControlUDPAddrs(ctx, literal, other); err == nil {
			t.Fatalf("ipv%d pin accepted the literal %s", other, literal)
		}
	})

	both, err := strategy.resolveControlUDPAddrs(ctx, familyTransportTestHost+":443", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(both) != 2 || udpAddrFamily(both[0]) != 6 || udpAddrFamily(both[1]) != 4 {
		t.Fatalf("family-agnostic order = %v, want v6 then v4", both)
	}

	ordered := orderControlIPAddrs([]net.IPAddr{
		{IP: net.ParseIP("10.0.0.1")},
		{IP: net.ParseIP("10.0.0.2")},
		{IP: net.ParseIP("2001:db8::1")},
		{IP: net.ParseIP("10.0.0.1")},
	})
	if len(ordered) != 3 || ordered[0].IP.To4() != nil || ordered[1].IP.To4() == nil || ordered[2].IP.To4() == nil {
		t.Fatalf("orderControlIPAddrs = %v", ordered)
	}
}

// The 464XLAT CLAT range counts as an IPv4 path: it is the only IPv4 an
// IPv6-only mobile network offers, and it works.
func TestProbeFamilySupportCountsClatRange(t *testing.T) {
	restore := swapControlFamilyInterfaces(func() ([]controlFamilyInterface, error) {
		return []controlFamilyInterface{
			{
				name:  "v4-rmnet0",
				flags: net.FlagUp | net.FlagPointToPoint,
				addrs: []net.Addr{ipNet("192.0.0.4/29")},
			},
			{
				name:  "rmnet0",
				flags: net.FlagUp,
				addrs: []net.Addr{ipNet("2600:1700:1234:5678::1/64")},
			},
		}, nil
	})
	t.Cleanup(restore)
	if !probeFamilySupport(4) {
		t.Fatal("the CLAT address did not count as an IPv4 path")
	}
	if !probeFamilySupport(6) {
		t.Fatal("the cellular v6 address did not count")
	}
	if !pinnedFamilySupported(4) || !pinnedFamilySupported(6) {
		t.Fatal("pinnedFamilySupported disagrees with the probe on an unbound host")
	}
}

// A policy change wakes a waiter exactly once per change.
func TestControlFamilyPolicyChangeNotifies(t *testing.T) {
	t.Cleanup(func() { SetControlIpFamilyPolicy(IpFamilyAuto) })
	SetControlIpFamilyPolicy(IpFamilyAuto)
	notify := controlFamilyPolicyNotify()
	SetControlIpFamilyPolicy(IpFamilyAuto)
	select {
	case <-notify:
		t.Fatal("an unchanged policy notified")
	default:
	}
	SetControlIpFamilyPolicy(IpFamilyForce6)
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("a policy change did not notify")
	}
	if pinnedFamilyPolicyConflict(6) || !pinnedFamilyPolicyConflict(4) {
		t.Fatal("Force6 conflicts with the wrong pin")
	}
}

// testingH3Connection is one accepted QUIC connection: the family it arrived
// over and the ip_family its auth frame declared (-1 when undecodable).
type testingH3Connection struct {
	remoteFamily int
	intent       int32
}

// testingH3Platform is a QUIC platform bound to one loopback address. It
// echoes the auth frame, then discards traffic, and records the family and
// the declared intent of every accepted connection.
type testingH3Platform struct {
	listener   *quic.EarlyListener
	port       int
	nextProto  string
	remotes    chan testingH3Connection
	cancel     context.CancelFunc
	framerSets *FramerSettings
}

func newTestingH3Platform(t *testing.T, ipVersion int) *testingH3Platform {
	t.Helper()
	host := testLoopbackIp(ipVersion)
	certPem, keyPem, err := selfSign([]string{host}, host, 24*time.Hour, 24*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(certPem, keyPem)
	if err != nil {
		t.Fatal(err)
	}
	nextProto := "urnetwork-platform-family-test"
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
	serverCtx, cancel := context.WithCancel(context.Background())
	platform := &testingH3Platform{
		listener:   listener,
		port:       listener.Addr().(*net.UDPAddr).Port,
		nextProto:  nextProto,
		remotes:    make(chan testingH3Connection, 16),
		cancel:     cancel,
		framerSets: DefaultFramerSettings(int(DefaultClientSettings().MinimumMessageLenLimit())),
	}
	go func() {
		for {
			conn, err := listener.Accept(serverCtx)
			if err != nil {
				return
			}
			remoteFamily := 6
			if udpAddr, ok := conn.RemoteAddr().(*net.UDPAddr); ok && udpAddr.IP.To4() != nil {
				remoteFamily = 4
			}
			go func() {
				stream, err := conn.AcceptStream(serverCtx)
				if err != nil {
					return
				}
				framer := NewFramer(platform.framerSets)
				authBytes, err := framer.Read(stream)
				if err != nil {
					return
				}
				intent := int32(-1)
				if decoded, err := DecodeFrame(authBytes); err == nil {
					if auth, ok := decoded.(*protocol.Auth); ok {
						intent = auth.IpFamily
					}
				}
				writeErr := framer.Write(stream, authBytes)
				MessagePoolReturn(authBytes)
				if writeErr != nil {
					return
				}
				platform.remotes <- testingH3Connection{remoteFamily: remoteFamily, intent: intent}
				for {
					message, err := framer.Read(stream)
					if err != nil {
						return
					}
					MessagePoolReturn(message)
				}
			}()
		}
	}()
	t.Cleanup(func() {
		cancel()
		listener.Close()
	})
	return platform
}

func (self *testingH3Platform) transportSettings() *PlatformTransportSettings {
	settings := testingFamilyTransportSettings()
	settings.H3Port = self.port
	settings.QuicConnectTimeout = 2 * time.Second
	settings.QuicHandshakeTimeout = 2 * time.Second
	settings.QuicTlsConfig = &tls.Config{
		InsecureSkipVerify: true, // test-only self-signed endpoint
		NextProtos:         []string{self.nextProto},
	}
	settings.FramerSettings = self.framerSets
	return settings
}

// A pinned H3 transport binds a socket of its family and reaches a platform
// that only listens on that family, by name.
func TestPlatformTransportH3BindsSocketPerFamily(t *testing.T) {
	clientSettings := newTestingFamilyStrategySettings(t)
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		platform := newTestingH3Platform(t, ipVersion)
		settings := platform.transportSettings()
		settings.IpFamily = ipVersion
		strategy := NewDirectClientStrategy(ctx, clientSettings, ipVersion)
		defer strategy.Close()
		transport := NewPlatformTransportWithTargetMode(
			ctx,
			strategy,
			NewRouteManager(ctx, "h3-family"),
			"https://"+familyTransportTestHost,
			testingFamilyAuth(),
			TransportModeH3,
			settings,
		)
		defer transport.Close()

		select {
		case connection := <-platform.remotes:
			if connection.remoteFamily != ipVersion {
				t.Fatalf("h3 arrived over ipv%d, want ipv%d", connection.remoteFamily, ipVersion)
			}
			if connection.intent != int32(ipVersion) {
				t.Fatalf("h3 auth ip_family = %d, want %d", connection.intent, ipVersion)
			}
		case <-time.After(15 * time.Second):
			t.Fatal("the pinned h3 transport never connected")
		}
		if !waitForCondition(15*time.Second, transport.IsConnected) {
			t.Fatal("h3 routes never registered")
		}
	})
}

// A family-agnostic H3 dial races the families: with v6 black-holed the v4
// candidate wins after the stagger instead of the attempt timing out.
func TestPlatformTransportH3AgnosticRaceSkipsBlackholedFamily(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	platform := newTestingH3Platform(t, 4)
	// nothing listens on this v6 port
	dead, err := net.ListenPacket("udp6", "[::1]:0")
	if err != nil {
		t.Fatal(err)
	}
	deadPort := dead.LocalAddr().(*net.UDPAddr).Port
	dead.Close()

	settings := platform.transportSettings()
	var resolveCount atomic.Int32
	settings.resolveH3AddrsForTest = func(ctx context.Context, address string, ipFamily int) ([]*net.UDPAddr, error) {
		resolveCount.Add(1)
		if ipFamily != 0 {
			t.Errorf("agnostic transport resolved with pin %d", ipFamily)
		}
		return []*net.UDPAddr{
			{IP: net.ParseIP("::1"), Port: deadPort},
			{IP: net.ParseIP("127.0.0.1"), Port: platform.port},
		}, nil
	}
	transport := NewPlatformTransportWithTargetMode(
		ctx,
		NewClientStrategyWithDefaults(ctx),
		NewRouteManager(ctx, "h3-race"),
		"https://localhost",
		testingFamilyAuth(),
		TransportModeH3,
		settings,
	)
	defer transport.Close()

	start := time.Now()
	select {
	case connection := <-platform.remotes:
		if connection.remoteFamily != 4 {
			t.Fatalf("race winner arrived over ipv%d, want ipv4", connection.remoteFamily)
		}
		if connection.intent != 0 {
			t.Fatalf("a family-agnostic h3 transport declared ip_family %d", connection.intent)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("the race never produced a connection")
	}
	if elapsed := time.Since(start); 5*time.Second < elapsed {
		t.Fatalf("the race took %s, the dead family consumed the attempt", elapsed)
	}
	if !waitForCondition(15*time.Second, transport.IsConnected) {
		t.Fatal("h3 routes never registered")
	}
	if resolveCount.Load() == 0 {
		t.Fatal("the resolution seam was not used")
	}
}

// raceH3Dial launches the next candidate immediately on a definitive failure
// and closes a loser that completes after the winner.
func TestRaceH3DialLaunchesNextOnFailureAndClosesLosers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	candidates := []*net.UDPAddr{
		{IP: net.ParseIP("::1"), Port: 1},
		{IP: net.ParseIP("127.0.0.1"), Port: 2},
		{IP: net.ParseIP("127.0.0.2"), Port: 3},
	}
	var launches []int
	var launchLock sync.Mutex
	closed := make(chan int, 3)
	lateRelease := make(chan struct{})
	start := time.Now()
	dial := func(attemptCtx context.Context, udpAddr *net.UDPAddr) (*h3DialAttempt, error) {
		launchLock.Lock()
		launches = append(launches, udpAddr.Port)
		launchLock.Unlock()
		switch udpAddr.Port {
		case 1:
			return nil, fmt.Errorf("refused")
		case 2:
			return &h3DialAttempt{udpAddr: udpAddr, packetConn: &closeRecordingPacketConn{port: 2, closed: closed}}, nil
		default:
			select {
			case <-lateRelease:
				return &h3DialAttempt{udpAddr: udpAddr, packetConn: &closeRecordingPacketConn{port: 3, closed: closed}}, nil
			case <-attemptCtx.Done():
				return nil, attemptCtx.Err()
			}
		}
	}
	winner, err := raceH3Dial(ctx, candidates, dial)
	if err != nil {
		t.Fatal(err)
	}
	if winner.udpAddr.Port != 2 {
		t.Fatalf("winner = %v, want port 2", winner.udpAddr)
	}
	if elapsed := time.Since(start); platformH3FamilyRaceStagger <= elapsed {
		t.Fatalf("a definitive failure did not launch the next candidate immediately (%s)", elapsed)
	}
	launchLock.Lock()
	launched := append([]int{}, launches...)
	launchLock.Unlock()
	if len(launched) < 2 || launched[0] != 1 || launched[1] != 2 {
		t.Fatalf("launch order = %v", launched)
	}
	// a third candidate may have launched on the stagger; if it completes
	// late it is closed, and the winner never is
	close(lateRelease)
	if len(launched) == 3 {
		select {
		case port := <-closed:
			if port != 3 {
				t.Fatalf("closed port %d, want the late loser 3", port)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the late loser was not closed")
		}
	}
	select {
	case port := <-closed:
		t.Fatalf("unexpected close of port %d", port)
	case <-time.After(100 * time.Millisecond):
	}
}

// closeRecordingPacketConn reports its close for the race test.
type closeRecordingPacketConn struct {
	net.PacketConn
	port   int
	closed chan int
}

func (self *closeRecordingPacketConn) Close() error {
	self.closed <- self.port
	return nil
}

// h3dnspump has no v6 name: a v6 pin does not offer the mode and an explicit
// pump target falls back to h3dns; a v4 pin keeps it.
func TestFamilyPinnedTransportV6ExcludesDnsPump(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for _, ipFamily := range []int{4, 6} {
		settings := testingFamilyTransportSettings()
		settings.IpFamily = ipFamily
		// held: the mode shape is decided at construction, no dial is needed
		settings.StartDisabled = true
		strategy := NewDirectClientStrategy(ctx, DefaultClientStrategySettings(), ipFamily)
		transport := NewPlatformTransportWithTargetMode(
			ctx,
			strategy,
			NewRouteManager(ctx, "pump"),
			"https://localhost",
			testingFamilyAuth(),
			TransportModeH3DnsPump,
			settings,
		)
		wantMode := TransportModeH3DnsPump
		wantPump := true
		if ipFamily == 6 {
			wantMode = TransportModeH3Dns
			wantPump = false
		}
		if transport.targetMode != wantMode {
			t.Fatalf("ipv%d target mode = %s, want %s", ipFamily, transport.targetMode, wantMode)
		}
		if offersPump := transport.modePreference(TransportModeH3DnsPump) != modePreferenceNone; offersPump != wantPump {
			t.Fatalf("ipv%d offers h3dnspump = %t, want %t", ipFamily, offersPump, wantPump)
		}
		// the caller's preferences are not edited
		if settings.ModePreferences[TransportModeH3DnsPump] == 0 {
			t.Fatalf("ipv%d pin edited the caller's mode preferences", ipFamily)
		}
		transport.Close()
		strategy.Close()
	}
}

// The default carrier budget admits a provider's three-transport group beside
// the default windows' hard maxima: a parked pinned transport keeps its H1
// claim and slot, so the count cap must cover all of them at once (A7).
func TestPlatformTransportBudgetAdmitsProviderGroupSlots(t *testing.T) {
	budget := newDefaultPlatformTransportBudget(mib(8))
	maxCount := budget.Stats().MaxTransportCount
	windowCount := 0
	for _, windowSize := range DefaultMultiClientSettings().WindowSizes {
		windowCount += windowSize.WindowSizeHardMax
	}
	// v4 pin, v6 pin, standby
	const providerTransportCount = 3
	if maxCount < providerTransportCount+windowCount {
		t.Fatalf("MaxTransportCount = %d, want at least %d provider + %d window transports", maxCount, providerTransportCount, windowCount)
	}
}
