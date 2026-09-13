// The client half of EXTENDER.md K1 and K4, against the in-process extender:
// a websocket dial reports the extender that carried it, and a platform
// transport publishes the extender of its live connection -- on the transport
// itself and as the directory's in-use count -- for exactly as long as that
// connection lives. These live here rather than in connect root because they
// need a real extender, and connect root must not import its own subpackage.

package extender

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/urnetwork/connect"
)

// The url of the websocket the fixture destination serves. The port is the
// default 443 the extender is asked to forward to; the fixture's forward dial
// seam maps the host to its real listener.
const testPlatformWsUrl = "wss://dest.example/ws"

// A strategy whose only dialer is this extender, so the dial under test can
// only have been carried by it.
func newExtenderTestStrategy(
	t *testing.T,
	ctx context.Context,
	fixture *extenderFixture,
	directory *connect.ExtenderDirectory,
) *connect.ClientStrategy {
	t.Helper()
	settings := connect.DefaultClientStrategySettings()
	settings.ConnectSettings = *fixture.connectSettings()
	settings.EnableNormal = false
	settings.EnableResilient = false
	settings.ExtenderConfigs = []*connect.ExtenderConfig{
		fixture.extenderConfig(connect.ExtenderCarrierTcp),
	}
	settings.ExtenderDirectory = directory
	clientStrategy := connect.NewClientStrategy(ctx, settings)
	t.Cleanup(clientStrategy.Close)
	return clientStrategy
}

// A strategy with no extender at all, dialing the destination directly through
// the dial seam, so the same websocket is reached over an ordinary path.
func newDirectTestStrategy(
	t *testing.T,
	ctx context.Context,
	fixture *extenderFixture,
) *connect.ClientStrategy {
	t.Helper()
	settings := connect.DefaultClientStrategySettings()
	settings.ConnectSettings = *fixture.connectSettings()
	settings.EnableResilient = false
	destinationAddress := fixture.destination.familyAddresses["tcp4"]
	settings.ConnectSettings.DialContextSettings = &connect.DialContextSettings{
		DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp4", destinationAddress)
		},
	}
	clientStrategy := connect.NewClientStrategy(ctx, settings)
	t.Cleanup(clientStrategy.Close)
	return clientStrategy
}

// K1: the winning dialer is reported with its extender address, and a direct
// dialer reports none.
func TestWsDialReportsTheExtenderThatCarriedIt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fixture := newExtenderFixture(t, "127.0.0.1", nil)

	extenderStrategy := newExtenderTestStrategy(t, ctx, fixture, nil)
	wsConn, _, dialerInfo, err := extenderStrategy.WsDialContextWithDialer(ctx, testPlatformWsUrl, nil)
	if err != nil {
		if extenderErr, ok := fixture.nextError(); ok {
			t.Fatalf("extender ws dial: %v; extender: %v", err, extenderErr)
		}
		t.Fatalf("extender ws dial: %v", err)
	}
	defer wsConn.Close()
	if dialerInfo == nil {
		t.Fatal("the extender dial reported no dialer")
	}
	if dialerInfo.ExtenderIp != fixture.ip {
		t.Fatalf("extender ip = %v, want %v", dialerInfo.ExtenderIp, fixture.ip)
	}

	directStrategy := newDirectTestStrategy(t, ctx, fixture)
	directConn, _, directInfo, err := directStrategy.WsDialContextWithDialer(ctx, testPlatformWsUrl, nil)
	if err != nil {
		t.Fatalf("direct ws dial: %v", err)
	}
	defer directConn.Close()
	if directInfo == nil {
		t.Fatal("the direct dial reported no dialer")
	}
	if directInfo.ExtenderIp.IsValid() {
		t.Fatalf("direct extender ip = %v, want none", directInfo.ExtenderIp)
	}
}

// K1, K4: the extender of a live platform connection is published by the
// transport and counted in use by the directory, and both are released when
// that connection ends.
func TestPlatformTransportPublishesTheExtenderOfItsConnection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fixture := newExtenderFixture(t, "127.0.0.1", nil)

	directorySettings := connect.DefaultExtenderDirectorySettings()
	directory := connect.NewExtenderDirectory(ctx, directorySettings)
	t.Cleanup(directory.Close)
	// the in-use count is per known address, so the address the configured
	// extender dials has to be one the directory knows
	directory.AddBootstrap(fixture.ip, connect.ExtenderSourceDns)

	clientStrategy := newExtenderTestStrategy(t, ctx, fixture, directory)
	transportSettings := connect.DefaultPlatformTransportSettings()
	transportSettings.ReconnectTimeout = 50 * time.Millisecond
	transport := connect.NewPlatformTransportWithTargetMode(
		ctx,
		clientStrategy,
		connect.NewRouteManager(ctx, "extender-test"),
		testPlatformWsUrl,
		&connect.ClientAuth{
			ByJwt:      "testing",
			InstanceId: connect.NewId(),
			AppVersion: "testing",
		},
		connect.TransportModeH1,
		transportSettings,
	)
	t.Cleanup(transport.Close)

	waitForTransport(t, transport, "the transport never connected through the extender", func() bool {
		return transport.IsConnected()
	})
	if ips := transport.ExtenderIps(); len(ips) != 1 || ips[0] != fixture.ip {
		t.Fatalf("extender ips = %v, want [%v]", ips, fixture.ip)
	}
	if inUse := directoryInUse(directory, fixture.ip); inUse != 1 {
		t.Fatalf("in use = %d, want 1", inUse)
	}

	// the platform goes away for good, so the connection ends and the
	// reconnect cannot restore it
	fixture.destination.refuseWebSockets()

	waitForTransport(t, transport, "the extender was never released", func() bool {
		return len(transport.ExtenderIps()) == 0
	})
	if inUse := directoryInUse(directory, fixture.ip); inUse != 0 {
		t.Fatalf("in use after disconnect = %d, want 0", inUse)
	}
}

// The in-use count the directory reports for one address, -1 when it is no
// longer known.
func directoryInUse(directory *connect.ExtenderDirectory, ip netip.Addr) int {
	for _, entry := range directory.Snapshot().Entries {
		if entry.Ip == ip {
			return entry.InUse
		}
	}
	return -1
}

// Waits for a transport state, driven by the change monitors the transport
// publishes rather than by a timer: the connect state and the extender set
// each close a channel when they move.
func waitForTransport(
	t *testing.T,
	transport *connect.PlatformTransport,
	message string,
	reached func() bool,
) {
	t.Helper()
	timeout := time.After(60 * time.Second)
	for {
		connected := transport.ConnectedNotify()
		_, extenderChange := transport.ExtenderIpsMonitor().Get()
		if reached() {
			return
		}
		select {
		case <-connected:
		case <-extenderChange:
		case <-timeout:
			t.Fatal(message)
		}
	}
}
