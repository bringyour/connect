package connect

import (
	"crypto/ed25519"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// The family half of the extender expansion and the counts that bound it
// (EXTENDER.md E2).
//
// Expansion draws from two candidate lists and must spend its budget on both,
// or a dual-stack host dials only the family that happens to sort first and
// never learns whether the other works. A family the host cannot prove is not
// drawn at all.

// A record naming both families, so one candidate exists per family.
func applyTestDualStackRecord(
	t *testing.T,
	directory *ExtenderDirectory,
	rootPrivateKey ed25519.PrivateKey,
	clock *testClock,
	v4 string,
	v6 string,
) {
	t.Helper()
	now := clock.Now()
	record := signTestRecord(
		t,
		rootPrivateKey,
		newTestExtenderKey(t),
		now,
		now.Add(24*time.Hour),
		testExtenderAddress(v4, ExtenderCarrierTcp),
		testExtenderAddress(v6, ExtenderCarrierTcp),
	)
	if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}
}

// The extender addresses of the expanded dialers, in expansion order.
func testExtenderDialerIps(clientStrategy *ClientStrategy) []netip.Addr {
	ips := []netip.Addr{}
	for _, dialer := range testExtenderDialers(clientStrategy) {
		ips = append(ips, dialer.extenderConfig.Ip)
	}
	return ips
}

// Expansion interleaves the families, so a budget of two on a dual-stack host
// yields one dialer of each rather than two of the family that sorts first.
func TestClientStrategyInterleavesTheFamilies(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, rootPrivateKey := newTestExtenderStrategy(
		t,
		clock,
		func(settings *ClientStrategySettings) {
			// one dialer per family, so an expansion that spent the budget on
			// one family would leave the other out entirely
			settings.ExpandExtenderProfileCount = 2
		},
	)
	// several addresses of each family, so the interleave has something to
	// choose from on both sides
	for i := 0; i < 4; i += 1 {
		applyTestDualStackRecord(
			t,
			directory,
			rootPrivateKey,
			clock,
			netip.AddrFrom4([4]byte{192, 0, 2, byte(10 + i)}).String(),
			netip.AddrFrom16([16]byte{
				0x20, 0x01, 0x0d, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, byte(10 + i),
			}).String(),
		)
	}

	clientStrategy.expandExtenderDialers()
	ips := testExtenderDialerIps(clientStrategy)
	if len(ips) != 2 {
		t.Fatalf("expanded %d dialers, expected the budget of 2", len(ips))
	}
	v4Count, v6Count := 0, 0
	for _, ip := range ips {
		if ip.Is4() {
			v4Count += 1
		} else {
			v6Count += 1
		}
	}
	if v4Count != 1 || v6Count != 1 {
		t.Fatalf("expanded %d v4 and %d v6 dialers from %v", v4Count, v6Count, ips)
	}
}

// A family the host cannot prove is never drawn, so a v4-only host does not
// spend dials on v6 addresses it cannot reach.
func TestClientStrategyExpandsOnlyTheSupportedFamilies(t *testing.T) {
	cases := []struct {
		name      string
		supported int
	}{
		{name: "v4 only", supported: 4},
		{name: "v6 only", supported: 6},
	}
	for _, c := range cases {
		clock := newTestClock()
		clientStrategy, directory, rootPrivateKey := newTestExtenderStrategy(
			t,
			clock,
			func(settings *ClientStrategySettings) {
				settings.ExpandExtenderProfileCount = 8
			},
		)
		// the fixture installs a dual-stack probe; narrow it for this case
		restoreProbe := swapControlFamilyProbe(func(family int) bool {
			return family == c.supported
		})
		applyTestDualStackRecord(
			t, directory, rootPrivateKey, clock, "192.0.2.10", "2001:db8::10")

		clientStrategy.expandExtenderDialers()
		ips := testExtenderDialerIps(clientStrategy)
		restoreProbe()
		if len(ips) != 1 {
			t.Errorf("%s expanded %v", c.name, ips)
			continue
		}
		wantIs4 := c.supported == 4
		if ips[0].Is4() != wantIs4 {
			t.Errorf("%s expanded %s", c.name, ips[0])
		}
	}
}

// One pass draws at most the expand budget however many candidates the
// directory holds, and a strategy with no budget -- either bound at zero --
// draws nothing at all, which is how a direct strategy stays direct.
func TestClientStrategyExpandBudgetBoundsOnePass(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, rootPrivateKey := newTestExtenderStrategy(
		t,
		clock,
		func(settings *ClientStrategySettings) {
			settings.ExpandExtenderProfileCount = 4
		},
	)
	applyTestExtenderRecords(t, directory, rootPrivateKey, clock, 16)

	expanded := clientStrategy.expandExtenderDialers()
	if len(expanded) != 4 {
		t.Fatalf("one pass expanded %d dialers, expected the budget of 4", len(expanded))
	}
	if count := len(testExtenderDialers(clientStrategy)); count != 4 {
		t.Fatalf("the strategy holds %d extender dialers", count)
	}

	for _, c := range []struct {
		name      string
		configure func(settings *ClientStrategySettings)
	}{
		{
			name: "no expand budget",
			configure: func(settings *ClientStrategySettings) {
				settings.ExpandExtenderProfileCount = 0
			},
		},
		{
			name: "no extender budget",
			configure: func(settings *ClientStrategySettings) {
				settings.ExpandExtenderProfileCount = 8
				settings.MaxExtenderCount = 0
			},
		},
	} {
		idleClock := newTestClock()
		idle, idleDirectory, idleRootPrivateKey := newTestExtenderStrategy(
			t, idleClock, c.configure)
		applyTestExtenderRecords(t, idleDirectory, idleRootPrivateKey, idleClock, 4)
		if expanded := idle.expandExtenderDialers(); 0 < len(expanded) {
			t.Errorf("%s expanded %d dialers", c.name, len(expanded))
		}
		if count := len(testExtenderDialers(idle)); count != 0 {
			t.Errorf("%s left %d extender dialers", c.name, count)
		}
	}
}

// One signed record per synthetic v4 address.
func applyTestExtenderRecords(
	t *testing.T,
	directory *ExtenderDirectory,
	rootPrivateKey ed25519.PrivateKey,
	clock *testClock,
	count int,
) {
	t.Helper()
	for i := 0; i < count; i += 1 {
		now := clock.Now()
		record := signTestRecord(
			t,
			rootPrivateKey,
			newTestExtenderKey(t),
			now,
			now.Add(24*time.Hour),
			testExtenderAddress(
				netip.AddrFrom4([4]byte{192, 0, 2, byte(10 + i)}).String(),
				ExtenderCarrierTcp,
			),
		)
		if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
			t.Fatal(err)
		}
	}
}

// Manual extenders are drawn in address order, so a budget smaller than the
// configured set always takes the same addresses rather than map order.
func TestClientStrategyManualExtendersAreDrawnInAddressOrder(t *testing.T) {
	clock := newTestClock()
	clientStrategy, _, _ := newTestExtenderStrategy(
		t,
		clock,
		func(settings *ClientStrategySettings) {
			// one address' worth of carriers plus one, so the order decides
			// which addresses are drawn at all
			settings.ExpandExtenderProfileCount = 4
		},
	)
	clientStrategy.SetCustomExtenders(map[netip.Addr]string{
		netip.MustParseAddr("2001:db8::10"): "secret-a",
		netip.MustParseAddr("192.0.2.11"):   "secret-b",
		netip.MustParseAddr("192.0.2.10"):   "secret-c",
	})
	expanded := clientStrategy.expandExtenderDialers()
	if len(expanded) != 4 {
		t.Fatalf("expanded %d dialers", len(expanded))
	}
	ips := []netip.Addr{}
	for _, dialer := range expanded {
		ips = append(ips, dialer.extenderConfig.Ip)
	}
	want := []netip.Addr{
		netip.MustParseAddr("192.0.2.10"),
		netip.MustParseAddr("192.0.2.10"),
		netip.MustParseAddr("192.0.2.10"),
		netip.MustParseAddr("192.0.2.11"),
	}
	if !slices.Equal(ips, want) {
		t.Fatalf("expanded %v, expected %v", ips, want)
	}

	// every manual dialer carries its address' own secret and the fixed
	// carrier ports of A1 and L2
	secrets := map[string]string{
		"192.0.2.10": "secret-c",
		"192.0.2.11": "secret-b",
	}
	for _, dialer := range expanded {
		extenderConfig := dialer.extenderConfig
		if want := secrets[extenderConfig.Ip.String()]; extenderConfig.Secret != want {
			t.Errorf("%s carries the secret %q, expected %q",
				extenderConfig.Ip, extenderConfig.Secret, want)
		}
		port := extenderConfig.Profile.Port
		switch extenderConfig.Profile.ConnectMode {
		case ExtenderConnectModeQuic:
			if port != ExtenderQuicPort {
				t.Errorf("quic port = %d", port)
			}
		case ExtenderConnectModeDns:
			if port != ExtenderDnsPort {
				t.Errorf("dns port = %d", port)
			}
			if extenderConfig.Profile.DnsTld != DefaultExtenderDnsTld {
				t.Errorf("dns tld = %q", extenderConfig.Profile.DnsTld)
			}
		default:
			if port != ExtenderTcpPort {
				t.Errorf("tcp port = %d", port)
			}
		}
	}
}

// A v6 candidate expands to dialers that reach a bracketed authority, which is
// the form the request and the dial both have to agree on.
func TestClientStrategyExpandsIpv6Candidates(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, rootPrivateKey := newTestExtenderStrategy(
		t,
		clock,
		func(settings *ClientStrategySettings) {
			settings.ExpandExtenderProfileCount = 8
		},
	)
	now := clock.Now()
	ip := netip.MustParseAddr("2001:db8::10")
	record := signTestRecord(
		t,
		rootPrivateKey,
		newTestExtenderKey(t),
		now,
		now.Add(24*time.Hour),
		&protocol.ExtenderAddress{
			Ip:        ip.String(),
			IpVersion: 6,
			Carriers: []string{
				ExtenderCarrierTcp,
				ExtenderCarrierQuic,
				ExtenderCarrierDns,
			},
		},
	)
	if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}

	clientStrategy.expandExtenderDialers()
	dialers := testExtenderDialers(clientStrategy)
	if len(dialers) != 3 {
		t.Fatalf("expanded %d dialers, expected one per carrier", len(dialers))
	}
	connectModes := []ExtenderConnectMode{}
	for _, dialer := range dialers {
		if dialer.extenderConfig.Ip != ip {
			t.Errorf("dialer address = %s", dialer.extenderConfig.Ip)
		}
		connectModes = append(connectModes, dialer.extenderConfig.Profile.ConnectMode)
		// the request authority brackets a v6 literal
		request := newExtenderRequest(dialer.extenderConfig, []byte("header"), nil)
		if request.URL.Host[0] != '[' {
			t.Errorf("v6 authority = %s", request.URL.Host)
		}
	}
	slices.Sort(connectModes)
	want := []ExtenderConnectMode{
		ExtenderConnectModeDns,
		ExtenderConnectModeQuic,
		ExtenderConnectModeTcpTls,
	}
	if !slices.Equal(connectModes, want) {
		t.Fatalf("connect modes = %v, expected %v", connectModes, want)
	}
}

// The feed dial config follows the carrier: quic takes the record's udp port,
// dns takes its dns port and encoding tld, tcp takes its tcp port, and a
// carrier the record gives no port for is not dialed at all (E3).
func TestExtenderFeedConfigFollowsTheCarrier(t *testing.T) {
	restoreSpoof := setSpoofDomainsForTest(nil)
	defer restoreSpoof()

	publicKey := newTestExtenderKey(t)
	candidate := &ExtenderCandidate{
		Ip:        netip.MustParseAddr("2001:db8::10"),
		IpVersion: 6,
		TcpPort:   1443,
		UdpPort:   2443,
		DnsPort:   3053,
		DnsTld:    "x.example.",
		PublicKey: publicKey,
	}
	cases := []struct {
		connectMode ExtenderConnectMode
		port        int
		dnsTld      string
	}{
		{connectMode: ExtenderConnectModeTcpTls, port: 1443},
		{connectMode: ExtenderConnectModeQuic, port: 2443},
		{connectMode: ExtenderConnectModeDns, port: 3053, dnsTld: "x.example."},
	}
	for _, c := range cases {
		extenderConfig := extenderFeedConfig(candidate, c.connectMode)
		if extenderConfig == nil {
			t.Errorf("%s yielded no config", c.connectMode)
			continue
		}
		if extenderConfig.Profile.Port != c.port {
			t.Errorf("%s port = %d, expected %d", c.connectMode, extenderConfig.Profile.Port, c.port)
		}
		if extenderConfig.Profile.DnsTld != c.dnsTld {
			t.Errorf("%s dns tld = %q, expected %q",
				c.connectMode, extenderConfig.Profile.DnsTld, c.dnsTld)
		}
		// the record key rides the config, so the outer leaf is checked
		if !slices.Equal(extenderConfig.PublicKey, publicKey) {
			t.Errorf("%s carries another key", c.connectMode)
		}
		// with no bundled spoof list the dial presents no name at all
		if extenderConfig.Profile.ServerName != candidate.Ip.String() {
			t.Errorf("%s server name = %q", c.connectMode, extenderConfig.Profile.ServerName)
		}
	}

	// a carrier the record gives no port for is not dialed
	portless := &ExtenderCandidate{
		Ip:        netip.MustParseAddr("192.0.2.10"),
		IpVersion: 4,
	}
	for _, connectMode := range []ExtenderConnectMode{
		ExtenderConnectModeTcpTls,
		ExtenderConnectModeQuic,
		ExtenderConnectModeDns,
	} {
		if extenderConfig := extenderFeedConfig(portless, connectMode); extenderConfig != nil {
			t.Errorf("%s yielded a config on port %d",
				connectMode, extenderConfig.Profile.Port)
		}
	}
}

// The network client defaults of E3, which the loop's cadence and its bounds
// are read from.
func TestExtenderNetworkClientDefaultSettings(t *testing.T) {
	settings := DefaultExtenderNetworkClientSettings()
	cases := []struct {
		name string
		got  any
		want any
	}{
		{name: "Subscribe", got: settings.Subscribe, want: true},
		{name: "SampleCount", got: settings.SampleCount, want: DefaultExtenderFeedSampleCount},
		{name: "MinBackoff", got: settings.MinBackoff, want: time.Second},
		{name: "MaxBackoff", got: settings.MaxBackoff, want: 5 * time.Minute},
		{name: "RebootstrapTimeout", got: settings.RebootstrapTimeout, want: 6 * time.Hour},
		{name: "LowWaterCount", got: settings.LowWaterCount, want: 4},
		{name: "DialTimeout", got: settings.DialTimeout, want: 30 * time.Second},
		{name: "HelloTimeout", got: settings.HelloTimeout, want: 30 * time.Second},
		{name: "SubscribeIdleTimeout", got: settings.SubscribeIdleTimeout, want: 90 * time.Second},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v, expected %v", c.name, c.got, c.want)
		}
	}
	if settings.Now == nil {
		t.Error("the default network client carries no clock")
	}
}

// The activator defaults of G3, which its cadence and its backoff are read
// from.
func TestExtenderActivatorDefaultSettings(t *testing.T) {
	settings := DefaultExtenderActivatorSettings()
	cases := []struct {
		name string
		got  any
		want any
	}{
		{name: "TcpPort", got: settings.TcpPort, want: ExtenderTcpPort},
		{name: "UdpPort", got: settings.UdpPort, want: ExtenderQuicPort},
		{name: "DnsPort", got: settings.DnsPort, want: ExtenderDnsPort},
		{name: "DnsTld", got: settings.DnsTld, want: DefaultExtenderDnsTld},
		{name: "ActivateTimeout", got: settings.ActivateTimeout, want: 24 * time.Hour},
		{name: "AddressCheckTimeout", got: settings.AddressCheckTimeout, want: time.Hour},
		{name: "MinBackoff", got: settings.MinBackoff, want: 10 * time.Minute},
		{name: "MaxBackoff", got: settings.MaxBackoff, want: 6 * time.Hour},
		{name: "RequestTimeout", got: settings.RequestTimeout, want: 60 * time.Second},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v, expected %v", c.name, c.got, c.want)
		}
	}
	if settings.Now == nil {
		t.Error("the default activator carries no clock")
	}
}

// The strategy defaults the expansion is bounded by (E2), and the startup gate
// budget of E4.
func TestClientStrategyExtenderDefaultSettings(t *testing.T) {
	settings := DefaultClientStrategySettings()
	cases := []struct {
		name string
		got  any
		want any
	}{
		{name: "ExpandExtenderProfileCount", got: settings.ExpandExtenderProfileCount, want: 8},
		{name: "MaxExtenderCount", got: settings.MaxExtenderCount, want: 128},
		{name: "ExtenderMinimumWeight", got: settings.ExtenderMinimumWeight, want: float32(0.1)},
		{name: "ExtenderDropTimeout", got: settings.ExtenderDropTimeout, want: 5 * time.Minute},
		{name: "ExtenderInitialSampleTimeout", got: settings.ExtenderInitialSampleTimeout, want: 2 * time.Second},
		{name: "AltUrl", got: settings.AltUrl, want: ""},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v, expected %v", c.name, c.got, c.want)
		}
	}
	if settings.ExtenderDirectory != nil {
		t.Error("the default strategy carries a directory")
	}
	if 0 < len(settings.ExtenderConfigs) {
		t.Error("the default strategy carries configured extenders")
	}
	// the dns encoding tlds the alt whodis carrier draws from
	if len(settings.DnsTlds) != 1 || string(settings.DnsTlds[0]) != DefaultExtenderDnsTld {
		t.Errorf("dns tlds = %v", settings.DnsTlds)
	}
}
