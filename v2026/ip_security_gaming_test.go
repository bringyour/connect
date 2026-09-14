package connect

import (
	"context"
	"encoding/binary"
	"net"
	"net/netip"
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

func steamTestPath(address netip.Addr, transport IpProtocol, port int, syn bool) *IpPath {
	version := 6
	source := net.ParseIP("2001:db8::2")
	if address.Is4() {
		version = 4
		source = net.ParseIP("10.0.0.2")
	}
	return &IpPath{
		Version:         version,
		Protocol:        transport,
		SourceIp:        source,
		SourcePort:      43000,
		DestinationIp:   net.IP(address.AsSlice()),
		DestinationPort: port,
		Syn:             syn,
	}
}

func lastAddressInPrefix(prefix netip.Prefix) netip.Addr {
	prefix = prefix.Masked()
	if prefix.Addr().Is4() {
		address := prefix.Addr().As4()
		value := binary.BigEndian.Uint32(address[:])
		hostBits := 32 - prefix.Bits()
		value |= uint32(1)<<hostBits - 1
		binary.BigEndian.PutUint32(address[:], value)
		return netip.AddrFrom4(address)
	}
	address := prefix.Addr().As16()
	for bit := prefix.Bits(); bit < 128; bit++ {
		address[bit/8] |= byte(1 << (7 - bit%8))
	}
	return netip.AddrFrom16(address)
}

func TestSteamValveNetworkPrefixSnapshot(t *testing.T) {
	expected := []string{
		"45.121.184.0/22",
		"103.10.124.0/23",
		"103.28.54.0/23",
		"146.66.152.0/21",
		"155.133.224.0/19",
		"162.254.192.0/21",
		"185.25.180.0/22",
		"192.69.96.0/22",
		"205.196.6.0/24",
		"208.64.200.0/22",
		"208.78.164.0/22",
		"2404:3fc0::/32",
		"2602:801:f000::/40",
		"2620:f9::/44",
		"2a01:bc80::/32",
	}
	if len(steamValveNetworkPrefixes) != len(expected) {
		t.Fatalf("Steam prefix count = %d, want snapshot count %d", len(steamValveNetworkPrefixes), len(expected))
	}

	v4Count, v6Count := 0, 0
	for i, prefix := range steamValveNetworkPrefixes {
		if prefix != prefix.Masked() {
			t.Fatalf("prefix %d is not masked: %s", i, prefix)
		}
		if got := prefix.String(); got != expected[i] {
			t.Fatalf("prefix %d = %s, want %s", i, got, expected[i])
		}
		if prefix.Addr().Is4() {
			v4Count++
		} else {
			v6Count++
		}
		for j, other := range steamValveNetworkPrefixes {
			if i != j && prefix.Contains(other.Addr()) {
				t.Fatalf("prefix %s contains prefix %s", prefix, other)
			}
		}
	}
	if v4Count != 11 || v6Count != 4 {
		t.Fatalf("Steam prefixes = %d IPv4 / %d IPv6, want 11 / 4", v4Count, v6Count)
	}
}

func TestSteamValveNetworkPrefixBoundaries(t *testing.T) {
	for _, prefix := range steamValveNetworkPrefixes {
		version := 6
		if prefix.Addr().Is4() {
			version = 4
		}
		first := prefix.Masked().Addr()
		last := lastAddressInPrefix(prefix)
		for _, address := range []netip.Addr{first, last} {
			path := steamTestPath(address, IpProtocolUdp, 27000, false)
			if !isSteamValveRemoteEndpoint(path) {
				t.Errorf("Steam prefix boundary %s (v%d) did not match", address, version)
			}
		}
		for _, address := range []netip.Addr{first.Prev(), last.Next()} {
			if !address.IsValid() {
				continue
			}
			path := steamTestPath(address, IpProtocolUdp, 27000, false)
			if isSteamValveRemoteEndpoint(path) {
				t.Errorf("address adjacent to %s matched: %s", prefix, address)
			}
		}
	}
}

func TestSteamRemotePorts(t *testing.T) {
	tests := []struct {
		name      string
		transport IpProtocol
		port      int
		want      bool
	}{
		{"http", IpProtocolTcp, 80, true},
		{"https", IpProtocolTcp, 443, true},
		{"tcp range first", IpProtocolTcp, 27015, true},
		{"tcp range last", IpProtocolTcp, 27050, true},
		{"steamworks stun", IpProtocolUdp, 3478, true},
		{"steamworks p2p", IpProtocolUdp, 4379, true},
		{"steam client", IpProtocolUdp, 4380, true},
		{"udp range first", IpProtocolUdp, 27000, true},
		{"udp range last", IpProtocolUdp, 27250, true},
		{"tcp below web", IpProtocolTcp, 79, false},
		{"tcp between web ports", IpProtocolTcp, 442, false},
		{"tcp below service range", IpProtocolTcp, 27014, false},
		{"tcp above service range", IpProtocolTcp, 27051, false},
		{"udp adjacent to stun", IpProtocolUdp, 3479, false},
		{"udp adjacent to p2p", IpProtocolUdp, 4378, false},
		{"udp adjacent to client", IpProtocolUdp, 4381, false},
		{"udp below game range", IpProtocolUdp, 26999, false},
		{"udp above game range", IpProtocolUdp, 27251, false},
		{"udp https not published", IpProtocolUdp, 443, false},
		{"wrong transport", IpProtocolIcmp, 27015, false},
		{"negative", IpProtocolUdp, -1, false},
		{"too large", IpProtocolTcp, 65536, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSteamRemotePort(tt.transport, tt.port); got != tt.want {
				t.Fatalf("isSteamRemotePort(%s, %d) = %v, want %v", tt.transport, tt.port, got, tt.want)
			}
		})
	}
}

func TestSteamValveEndpointScopeAndSettings(t *testing.T) {
	path := steamTestPath(netip.MustParseAddr("155.133.224.1"), IpProtocolUdp, 27015, false)
	settings := DefaultGamingSecurityPolicySettings()
	if !isSanctionedGamingEndpoint(settings, path) {
		t.Fatal("default Steam exception did not match")
	}

	reversed := path.Reverse()
	if isSanctionedGamingEndpoint(settings, reversed) {
		t.Fatal("reverse-direction tuple matched destination-scoped Steam exception")
	}
	otherProvider := *path
	otherProvider.DestinationIp = net.ParseIP("8.8.8.8")
	if isSanctionedGamingEndpoint(settings, &otherProvider) {
		t.Fatal("non-Valve destination matched Steam exception")
	}
	wrongPort := *path
	wrongPort.DestinationPort = 27251
	if isSanctionedGamingEndpoint(settings, &wrongPort) {
		t.Fatal("undocumented remote port matched Steam exception")
	}
	wrongVersion := *path
	wrongVersion.Version = 6
	if isSanctionedGamingEndpoint(settings, &wrongVersion) {
		t.Fatal("IPv4 address labeled IPv6 matched Steam exception")
	}

	settings.AllowSteam = false
	if isSanctionedGamingEndpoint(settings, path) {
		t.Fatal("disabled Steam exception matched")
	}
	settings.AllowSteam = true
	settings.Enabled = false
	if isSanctionedGamingEndpoint(settings, path) {
		t.Fatal("disabled gaming master switch matched Steam exception")
	}
	if isSanctionedGamingEndpoint(nil, path) {
		t.Fatal("nil gaming settings matched Steam exception")
	}
}

func TestSteamValveEndpointZeroAlloc(t *testing.T) {
	path := steamTestPath(netip.MustParseAddr("155.133.224.1"), IpProtocolUdp, 27015, false)
	if allocations := testing.AllocsPerRun(1000, func() {
		if !isSteamValveRemoteEndpoint(path) {
			t.Fatal("Steam endpoint did not match")
		}
	}); allocations != 0 {
		t.Fatalf("Steam endpoint lookup allocates %.2f objects per call, want 0", allocations)
	}
}

func TestDmcaSteamValveExceptionAndPrecedence(t *testing.T) {
	t.Run("encrypted UDP allowed", func(t *testing.T) {
		settings := DefaultDmcaSecurityPolicySettings()
		detector := newDmcaDetector(nil, settings, newWebStandardDetector(DefaultWebStandardSettings()))
		path := steamTestPath(netip.MustParseAddr("155.133.224.1"), IpProtocolUdp, 27015, false)
		payload := encryptedPayload(settings.MaxInspectionPayload)
		if !payloadLooksEncrypted(payload, settings) {
			t.Fatal("fixture must exercise the encrypted heuristic")
		}
		if verdict := detector.classify(path, payload); verdict != dmcaAllow {
			t.Fatalf("Steam UDP verdict = %d, want allow", verdict)
		}
		if verdict := detector.classify(path, encryptedPayload(512)); verdict != dmcaAllow {
			t.Fatalf("terminal Steam UDP verdict = %d, want allow", verdict)
		}
	})

	t.Run("encrypted IPv6 TCP allowed", func(t *testing.T) {
		settings := DefaultDmcaSecurityPolicySettings()
		detector := newDmcaDetector(nil, settings, newWebStandardDetector(DefaultWebStandardSettings()))
		syn := steamTestPath(netip.MustParseAddr("2602:801:f000::1"), IpProtocolTcp, 27050, true)
		if verdict := detector.classify(syn, nil); verdict != dmcaInspecting {
			t.Fatalf("Steam TCP SYN verdict = %d, want inspecting", verdict)
		}
		data := *syn
		data.Syn = false
		if verdict := detector.classify(&data, encryptedPayload(512)); verdict != dmcaAllow {
			t.Fatalf("Steam IPv6 TCP verdict = %d, want allow", verdict)
		}
	})

	t.Run("BitTorrent signature wins", func(t *testing.T) {
		detector := newDmcaDetector(nil, DefaultDmcaSecurityPolicySettings(), newWebStandardDetector(DefaultWebStandardSettings()))
		path := steamTestPath(netip.MustParseAddr("155.133.224.1"), IpProtocolTcp, 27015, true)
		if verdict := detector.classify(path, btHandshake()); verdict != dmcaBittorrent {
			t.Fatalf("BitTorrent on Steam endpoint = %d, want bittorrent", verdict)
		}
	})

	tests := []struct {
		name      string
		address   string
		port      int
		configure func(*DmcaSecurityPolicySettings)
	}{
		{name: "non-Valve destination", address: "8.8.8.8", port: 27015},
		{name: "undocumented port", address: "155.133.224.1", port: 27251},
		{
			name:    "Steam disabled",
			address: "155.133.224.1",
			port:    27015,
			configure: func(settings *DmcaSecurityPolicySettings) {
				settings.Gaming.AllowSteam = false
			},
		},
		{
			name:    "gaming disabled",
			address: "155.133.224.1",
			port:    27015,
			configure: func(settings *DmcaSecurityPolicySettings) {
				settings.Gaming.Enabled = false
			},
		},
		{
			name:    "nil gaming settings",
			address: "155.133.224.1",
			port:    27015,
			configure: func(settings *DmcaSecurityPolicySettings) {
				settings.Gaming = nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			settings := DefaultDmcaSecurityPolicySettings()
			if tt.configure != nil {
				tt.configure(settings)
			}
			detector := newDmcaDetector(nil, settings, newWebStandardDetector(DefaultWebStandardSettings()))
			path := steamTestPath(netip.MustParseAddr(tt.address), IpProtocolUdp, tt.port, false)
			var verdict dmcaVerdict
			for i := 0; i < settings.EncryptedDecisionPackets; i++ {
				verdict = detector.classify(path, encryptedPayload(512))
			}
			if verdict != dmcaDropEncrypted {
				t.Fatalf("near-miss Steam flow verdict = %d, want encrypted drop", verdict)
			}
		})
	}
}

func TestSecurityPolicySteamValveException(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := DefaultSecurityPolicy(ctx)
	path := steamTestPath(netip.MustParseAddr("155.133.224.1"), IpProtocolUdp, 27015, false)
	for i := 0; i < DefaultDmcaSecurityPolicySettings().EncryptedDecisionPackets+1; i++ {
		result, err := policy.InspectEgress(protocol.ProvideMode_Public, path, encryptedPayload(512))
		if err != nil || result != SecurityPolicyResultAllow {
			t.Fatalf("Steam policy packet %d = (%v, %v), want allow", i, result, err)
		}
	}

	btPath := steamTestPath(netip.MustParseAddr("155.133.224.1"), IpProtocolTcp, 27015, true)
	result, err := policy.InspectEgress(protocol.ProvideMode_Public, btPath, btHandshake())
	if err != nil || result != SecurityPolicyResultIncident {
		t.Fatalf("BitTorrent on Steam policy endpoint = (%v, %v), want incident", result, err)
	}
}
