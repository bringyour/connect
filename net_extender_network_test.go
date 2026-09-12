package connect

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net"
	"net/netip"
	"testing"
	"time"
)

// Network client tests (EXTENDER.md E3). Every seam the loop touches is
// injected: the clock, the resolver, the hello and the dial, so nothing here
// reaches the network. The end-to-end sample against a real extender lives in
// `extender/extender_feed_client_test.go`.

// The operator gossip identity hello serves in these tests (C6).
const testExtenderGossipPeerId = "12D3KooWtestoperatorpeerid"

// A client strategy whose every carrier dial fails at once, so a sample
// attempt costs nothing.
func newTestDeadDialStrategy(t *testing.T, ctx context.Context) *ClientStrategy {
	t.Helper()
	settings := DefaultClientStrategySettings()
	settings.ConnectSettings.DialContextSettings = &DialContextSettings{
		DialContext: func(ctx context.Context, network string, addr string) (net.Conn, error) {
			return nil, fmt.Errorf("no route in this test")
		},
		PacketConnFactory: func(ctx context.Context) (net.PacketConn, error) {
			return nil, fmt.Errorf("no packet endpoint in this test")
		},
	}
	clientStrategy := NewClientStrategy(ctx, settings)
	t.Cleanup(clientStrategy.Close)
	return clientStrategy
}

// One network client over a directory with the fake clock.
func newTestExtenderNetworkClient(
	t *testing.T,
	clock *testClock,
	configure func(settings *ExtenderNetworkClientSettings),
) (*ExtenderNetworkClient, *ExtenderDirectory, ed25519.PrivateKey) {
	t.Helper()
	rootSeed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	rootPrivateKey, err := ExtenderPrivateKeyFromSeed(rootSeed)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	directorySettings := DefaultExtenderDirectorySettings()
	directorySettings.Now = clock.Now
	directorySettings.NetworkHosts = []string{testExtenderNetworkHost}
	directory := NewExtenderDirectory(ctx, directorySettings)

	settings := DefaultExtenderNetworkClientSettings()
	settings.Now = clock.Now
	settings.ExtenderDnsName = "extender.space.example"
	settings.MinBackoff = time.Millisecond
	settings.MaxBackoff = 10 * time.Millisecond
	settings.DialTimeout = 2 * time.Second
	settings.HelloTimeout = 2 * time.Second
	settings.IpVersionSupported = func(ipVersion int) bool { return true }
	settings.Hello = func(ctx context.Context) (*ExtenderHelloResult, error) {
		return nil, nil
	}
	if configure != nil {
		configure(settings)
	}
	clientStrategy := newTestDeadDialStrategy(t, ctx)
	networkClient := NewExtenderNetworkClient(ctx, clientStrategy, directory, settings)
	t.Cleanup(func() {
		networkClient.Close()
		directory.Close()
		cancel()
	})
	return networkClient, directory, rootPrivateKey
}

// The bootstrap adds the resolver's answers as unverified dns addresses, and
// the hello answer becomes the trust anchor (E3, B4).
func TestExtenderNetworkClientBootstrapsAndAppliesHelloRootKeys(t *testing.T) {
	clock := newTestClock()
	rootSeed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	rootPrivateKey, err := ExtenderPrivateKeyFromSeed(rootSeed)
	if err != nil {
		t.Fatal(err)
	}
	rootPublicKey := rootPrivateKey.Public().(ed25519.PublicKey)

	resolved := make(chan string, 16)
	networkClient, directory, _ := newTestExtenderNetworkClient(t, clock, func(settings *ExtenderNetworkClientSettings) {
		settings.ResolveDns = func(ctx context.Context, name string) ([]netip.Addr, error) {
			select {
			case resolved <- name:
			default:
			}
			return []netip.Addr{
				netip.MustParseAddr("192.0.2.200"),
				netip.MustParseAddr("2001:db8::200"),
			}, nil
		}
		settings.Hello = func(ctx context.Context) (*ExtenderHelloResult, error) {
			return &ExtenderHelloResult{
				RootPublicKeyHexes: []string{ExtenderKeySeedHex(rootPublicKey)},
				GossipPeerId:       testExtenderGossipPeerId,
			}, nil
		}
	})

	select {
	case name := <-resolved:
		if name != "extender.space.example" {
			t.Fatalf("resolved %q, expected the configured name", name)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the bootstrap never resolved")
	}

	deadline := time.Now().Add(10 * time.Second)
	for {
		if testDirectoryKnown(directory, netip.MustParseAddr("192.0.2.200")) &&
			testDirectoryKnown(directory, netip.MustParseAddr("2001:db8::200")) {
			break
		}
		if deadline.Before(time.Now()) {
			t.Fatal("the bootstrap addresses never reached the directory")
		}
		time.Sleep(time.Millisecond)
	}
	entry := testDirectoryEntry(t, directory, netip.MustParseAddr("192.0.2.200"))
	if entry.Source != ExtenderSourceDns {
		t.Fatalf("source = %s, expected dns", entry.Source)
	}
	if entry.State != ExtenderStateUnverified && entry.State != ExtenderStateHold && entry.State != ExtenderStateWarning {
		t.Fatalf("state = %s, expected an unverified bootstrap address", entry.State)
	}

	// the hello keys are the anchor now, so a record signed by them applies
	for deadline := time.Now().Add(10 * time.Second); ; {
		record := signTestRecord(
			t,
			rootPrivateKey,
			newTestExtenderKey(t),
			clock.Now(),
			clock.Now().Add(14*24*time.Hour),
			testExtenderAddress("192.0.2.201"),
		)
		if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err == nil {
			break
		}
		if deadline.Before(time.Now()) {
			t.Fatal("the hello root keys were never applied")
		}
		time.Sleep(time.Millisecond)
	}

	// the same answer carries the operator's gossip identity, which the member
	// role's node dials (C6, D3)
	for deadline := time.Now().Add(10 * time.Second); ; {
		if networkClient.Status().GossipPeerId == testExtenderGossipPeerId {
			break
		}
		if deadline.Before(time.Now()) {
			t.Fatalf("the gossip peer id never reached the status")
		}
		time.Sleep(time.Millisecond)
	}
}

// The first attempt is marked complete even when nothing answers, so the
// startup gate never waits on an attempt that has already failed (E3, E4).
func TestExtenderNetworkClientMarksTheFirstAttemptDone(t *testing.T) {
	clock := newTestClock()
	networkClient, directory, _ := newTestExtenderNetworkClient(t, clock, func(settings *ExtenderNetworkClientSettings) {
		settings.ResolveDns = func(ctx context.Context, name string) ([]netip.Addr, error) {
			return nil, fmt.Errorf("no answer in this test")
		}
	})

	state, update := directory.InitialSampleMonitor().Get()
	for state != ExtenderInitialSampleDone {
		select {
		case <-update:
		case <-time.After(10 * time.Second):
			t.Fatal("the first attempt never completed")
		}
		state, update = directory.InitialSampleMonitor().Get()
	}
	if !networkClient.Status().InitialAttemptDone {
		t.Fatal("the status does not report the completed first attempt")
	}
	if networkClient.Status().LastError == "" {
		t.Fatal("the status carries no error for a failed first attempt")
	}
}

// A directory below the low-water mark re-bootstraps on the next pass, and one
// above it does not (E3).
func TestExtenderNetworkClientRebootstrapsBelowTheLowWaterMark(t *testing.T) {
	clock := newTestClock()
	resolved := make(chan struct{}, 64)
	_, _, _ = newTestExtenderNetworkClient(t, clock, func(settings *ExtenderNetworkClientSettings) {
		settings.LowWaterCount = 4
		settings.ResolveDns = func(ctx context.Context, name string) ([]netip.Addr, error) {
			select {
			case resolved <- struct{}{}:
			default:
			}
			// one address, which is below the low-water mark of four
			return []netip.Addr{netip.MustParseAddr("192.0.2.210")}, nil
		}
	})
	for i := range 3 {
		select {
		case <-resolved:
		case <-time.After(10 * time.Second):
			t.Fatalf("the bootstrap did not repeat below the low-water mark (pass %d)", i)
		}
	}
}

// Above the low-water mark the bootstrap does not repeat until the
// re-bootstrap period, however many passes the loop makes (E3).
func TestExtenderNetworkClientDoesNotRebootstrapAboveTheLowWaterMark(t *testing.T) {
	clock := newTestClock()
	resolved := make(chan struct{}, 64)
	helloCalls := make(chan struct{}, 64)
	_, _, _ = newTestExtenderNetworkClient(t, clock, func(settings *ExtenderNetworkClientSettings) {
		settings.LowWaterCount = 2
		settings.ResolveDns = func(ctx context.Context, name string) ([]netip.Addr, error) {
			select {
			case resolved <- struct{}{}:
			default:
			}
			return []netip.Addr{
				netip.MustParseAddr("192.0.2.220"),
				netip.MustParseAddr("192.0.2.221"),
				netip.MustParseAddr("192.0.2.222"),
			}, nil
		}
		settings.Hello = func(ctx context.Context) (*ExtenderHelloResult, error) {
			select {
			case helloCalls <- struct{}{}:
			default:
			}
			return nil, nil
		}
	})

	select {
	case <-resolved:
	case <-time.After(10 * time.Second):
		t.Fatal("the bootstrap never ran")
	}
	select {
	case <-helloCalls:
	case <-time.After(10 * time.Second):
		t.Fatal("hello never ran")
	}
	// the loop keeps passing -- every pass fails its sample against the dead
	// dialer -- but neither the bootstrap nor hello is due again
	for range 4 {
		select {
		case <-helloCalls:
			t.Fatal("hello repeated before its period")
		case <-resolved:
			t.Fatal("the bootstrap repeated above the low-water mark")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// The carriers of a candidate are tried tcp, then quic, then dns (E3).
func TestExtenderNetworkClientCarrierOrder(t *testing.T) {
	cases := []struct {
		carriers []string
		expect   []string
	}{
		{
			carriers: []string{ExtenderCarrierDns, ExtenderCarrierQuic, ExtenderCarrierTcp},
			expect:   []string{ExtenderCarrierTcp, ExtenderCarrierQuic, ExtenderCarrierDns},
		},
		{
			carriers: []string{ExtenderCarrierDns},
			expect:   []string{ExtenderCarrierDns},
		},
		{
			carriers: []string{"other", ExtenderCarrierQuic},
			expect:   []string{ExtenderCarrierQuic},
		},
	}
	for _, c := range cases {
		ordered := orderedExtenderCarriers(c.carriers)
		if len(ordered) != len(c.expect) {
			t.Fatalf("order = %v, expected %v", ordered, c.expect)
		}
		for i := range ordered {
			if ordered[i] != c.expect[i] {
				t.Fatalf("order = %v, expected %v", ordered, c.expect)
			}
		}
	}
}
