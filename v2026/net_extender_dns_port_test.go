package connect

import (
	"crypto/ed25519"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// The dns carrier ports a record offers, and the dialers they produce
// (EXTENDER.md L2). A record lists every port that passed its activation
// probe; the client dials 53 first when it is listed, then 4053.

// One signed record whose dns ports are exactly `dnsPorts`, with `dnsPort` as
// the single port an older reader sees.
func signTestDnsPortRecord(
	t *testing.T,
	rootPrivateKey ed25519.PrivateKey,
	clock *testClock,
	ip netip.Addr,
	dnsPort int,
	dnsPorts []int,
) *protocol.ExtenderRecord {
	t.Helper()
	recordDnsPorts := []uint32{}
	for _, recordDnsPort := range dnsPorts {
		recordDnsPorts = append(recordDnsPorts, uint32(recordDnsPort))
	}
	body := &protocol.ExtenderRecordBody{
		PublicKey:    newTestExtenderKey(t),
		Addresses:    []*protocol.ExtenderAddress{testExtenderAddress(ip.String(), ExtenderCarrierDns)},
		TcpPort:      443,
		UdpPort:      443,
		DnsPort:      uint32(dnsPort),
		DnsPorts:     recordDnsPorts,
		DnsTld:       "x.example.",
		IssueTimeMs:  uint64(clock.Now().UnixMilli()),
		ExpireTimeMs: uint64(clock.Now().Add(14 * 24 * time.Hour).UnixMilli()),
		NetworkHost:  testExtenderNetworkHost,
	}
	record, err := SignExtenderRecord(rootPrivateKey, body)
	if err != nil {
		t.Fatal(err)
	}
	return record
}

// A record that lists both dns ports yields one dialer per port, 53 first
// whatever order the record wrote them in.
func TestClientStrategyDialsEveryRecordDnsPort(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, rootPrivateKey := newTestExtenderStrategy(t, clock, nil)

	ip := netip.MustParseAddr("192.0.2.121")
	record := signTestDnsPortRecord(t, rootPrivateKey, clock, ip, 4053, []int{4053, 53})
	if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}

	candidates := directory.Candidates(4, 4)
	if len(candidates) != 1 {
		t.Fatalf("candidates = %d, expected the one address", len(candidates))
	}
	if dnsPorts := candidates[0].DnsPorts; !slices.Equal(dnsPorts, []int{53, 4053}) {
		t.Fatalf("candidate dns ports = %v, expected 53 then 4053", dnsPorts)
	}
	if candidates[0].DnsPort != 53 {
		t.Fatalf("candidate dns port = %d, expected the first listed", candidates[0].DnsPort)
	}

	expandedDialers := clientStrategy.expandExtenderDialers()
	dialedPorts := []int{}
	for _, dialer := range expandedDialers {
		profile := dialer.extenderConfig.Profile
		if profile.ConnectMode != ExtenderConnectModeDns {
			t.Fatalf("connect mode = %s, expected dns", profile.ConnectMode)
		}
		dialedPorts = append(dialedPorts, profile.Port)
	}
	if !slices.Equal(dialedPorts, []int{53, 4053}) {
		t.Fatalf("dialer ports = %v, expected 53 then 4053", dialedPorts)
	}
}

// A record that lists one dns port yields one dialer, which is every record
// written before the port list existed.
func TestClientStrategyDialsTheSingleRecordDnsPort(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, rootPrivateKey := newTestExtenderStrategy(t, clock, nil)

	ip := netip.MustParseAddr("192.0.2.122")
	record := signTestDnsPortRecord(t, rootPrivateKey, clock, ip, ExtenderDnsPort, nil)
	if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}

	candidates := directory.Candidates(4, 4)
	if len(candidates) != 1 {
		t.Fatalf("candidates = %d, expected the one address", len(candidates))
	}
	if dnsPorts := candidates[0].DnsPorts; !slices.Equal(dnsPorts, []int{ExtenderDnsPort}) {
		t.Fatalf("candidate dns ports = %v, expected the single record port", dnsPorts)
	}

	expandedDialers := clientStrategy.expandExtenderDialers()
	if len(expandedDialers) != 1 {
		t.Fatalf("dialers = %d, expected one for the one port", len(expandedDialers))
	}
	if port := expandedDialers[0].extenderConfig.Profile.Port; port != ExtenderDnsPort {
		t.Fatalf("dialer port = %d, expected %d", port, ExtenderDnsPort)
	}
}
