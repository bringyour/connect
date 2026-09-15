package connect

import (
	"context"
	"net"
	"net/http"
	"net/netip"
	"slices"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
	"golang.org/x/net/dns/dnsmessage"
)

func (self *ipMuxRecorder) receivedPackets() [][]byte {
	self.mu.Lock()
	defer self.mu.Unlock()
	out := make([][]byte, len(self.received))
	copy(out, self.received)
	return out
}

// dnsQueryPacketTyped is dnsQueryPacket with the record type controlled, so
// blocker tests can model HTTPS/SVCB (65) and TXT queries for blocked names.
func dnsQueryPacketTyped(t *testing.T, name string, qtype dnsmessage.Type, id uint16) []byte {
	t.Helper()
	qb := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: id, RecursionDesired: true})
	if err := qb.StartQuestions(); err != nil {
		t.Fatal(err)
	}
	if err := qb.Question(dnsmessage.Question{
		Name:  dnsmessage.MustNewName(name),
		Type:  qtype,
		Class: dnsmessage.ClassINET,
	}); err != nil {
		t.Fatal(err)
	}
	queryPayload, err := qb.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return ipOosPacket(&IpPath{
		Version:         4,
		Protocol:        IpProtocolUdp,
		SourceIp:        net.ParseIP("169.254.9.9"),
		SourcePort:      44444,
		DestinationIp:   net.ParseIP("10.0.0.1"),
		DestinationPort: 53,
	}, queryPayload)
}

// parseDnsBlockedReply decodes a downstream packet as the udp dns reply the
// mux synthesized: reversed path (from :53), then the message sections.
// (parseDnsReply in ip_mux_upgrade_test.go is A-record-specific.)
func parseDnsBlockedReply(t *testing.T, packet []byte) (dnsmessage.Header, dnsmessage.Question, []dnsmessage.Resource) {
	t.Helper()
	ipPath, payload, err := ParseIpPathWithPayload(packet)
	if err != nil {
		t.Fatal(err)
	}
	if ipPath.Protocol != IpProtocolUdp || ipPath.SourcePort != 53 {
		t.Fatalf("reply not from :53: %+v", ipPath)
	}
	var p dnsmessage.Parser
	header, err := p.Start(payload)
	if err != nil {
		t.Fatal(err)
	}
	question, err := p.Question()
	if err != nil {
		t.Fatal(err)
	}
	if err := p.SkipAllQuestions(); err != nil {
		t.Fatal(err)
	}
	answers, err := p.AllAnswers()
	if err != nil {
		t.Fatal(err)
	}
	return header, question, answers
}

// TestUpgradeMuxDnsBlocked drives blocked and unblocked queries through the
// mux send path directly and asserts the synthesized replies: A→0.0.0.0,
// AAAA→::, every other type (HTTPS/SVCB 65, TXT) → empty NOERROR; and that
// disabling the blocker (or removing it) returns queries to the resolution
// pipeline without a mux rebuild. Both packet families: the query family
// and the record type are independent, so every reply shape is checked
// over v4 and v6 packets.
func TestUpgradeMuxDnsBlocked(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testUpgradeMuxDnsBlocked(t, ipVersion)
	})
}

func testUpgradeMuxDnsBlocked(t *testing.T, ipVersion int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rec := &ipMuxRecorder{}
	settings := DefaultUpgradeMuxSettings()
	// an empty resolver: unblocked queries enter the pipeline and fail to
	// resolve (no servers), which sends nothing downstream — so every
	// downstream packet in this test is a blocker-synthesized reply.
	settings.Dns.Resolver = &DnsResolverSettings{}
	settings.Dns.Fallback = nil
	settings.Dns.ResolveTimeout = 200 * time.Millisecond
	mux, err := NewUpgradeMux(ctx, TransferPath{}, protocol.ProvideMode_Network, 0, rec.receive, settings, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Close()
	mux.SetUpstream(rec.upstream)

	blocker := blockerTestNew([]string{"ads.example.com"}, nil, nil)
	mux.SetBlocker(blocker)

	waitReceived := func(count int) bool {
		return waitForCondition(5*time.Second, func() bool {
			_, received := rec.counts()
			return count <= received
		})
	}

	responseTtl := settings.Dns.ResponseTtl

	// blocked A: 0.0.0.0 with the settings ttl, echoing the transaction id
	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, dnsQueryPacketTypedVersion(t, ipVersion, "sub.ads.example.com.", dnsmessage.TypeA, 0x0a01), 0) {
		t.Fatal("blocked A query not claimed")
	}
	if !waitReceived(1) {
		t.Fatal("no reply to a blocked A query")
	}
	header, question, answers := parseDnsBlockedReply(t, rec.receivedPackets()[0])
	if header.ID != 0x0a01 || !header.Response || header.RCode != dnsmessage.RCodeSuccess {
		t.Fatalf("blocked A header = %+v", header)
	}
	if question.Type != dnsmessage.TypeA {
		t.Fatalf("blocked A question echoed as %v", question.Type)
	}
	if len(answers) != 1 {
		t.Fatalf("blocked A answers = %d, want 1", len(answers))
	}
	if a, ok := answers[0].Body.(*dnsmessage.AResource); !ok || netip.AddrFrom4(a.A) != netip.IPv4Unspecified() {
		t.Fatalf("blocked A answer = %+v, want 0.0.0.0", answers[0].Body)
	}
	if answers[0].Header.TTL != responseTtl {
		t.Fatalf("blocked A ttl = %d, want %d", answers[0].Header.TTL, responseTtl)
	}

	// blocked AAAA: ::
	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, dnsQueryPacketTypedVersion(t, ipVersion, "ads.example.com.", dnsmessage.TypeAAAA, 0x0a02), 0) {
		t.Fatal("blocked AAAA query not claimed")
	}
	if !waitReceived(2) {
		t.Fatal("no reply to a blocked AAAA query")
	}
	_, _, answers = parseDnsBlockedReply(t, rec.receivedPackets()[1])
	if len(answers) != 1 {
		t.Fatalf("blocked AAAA answers = %d, want 1", len(answers))
	}
	if a, ok := answers[0].Body.(*dnsmessage.AAAAResource); !ok || netip.AddrFrom16(a.AAAA) != netip.IPv6Unspecified() {
		t.Fatalf("blocked AAAA answer = %+v, want ::", answers[0].Body)
	}

	// blocked HTTPS/SVCB (type 65): claimed with an empty NOERROR, so the
	// browser cannot reconnect via the record's ip hints
	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, dnsQueryPacketTypedVersion(t, ipVersion, "ads.example.com.", dnsmessage.Type(65), 0x0a03), 0) {
		t.Fatal("blocked HTTPS query not claimed")
	}
	if !waitReceived(3) {
		t.Fatal("no reply to a blocked HTTPS query")
	}
	header, question, answers = parseDnsBlockedReply(t, rec.receivedPackets()[2])
	if header.RCode != dnsmessage.RCodeSuccess || len(answers) != 0 {
		t.Fatalf("blocked HTTPS reply: rcode=%v answers=%d, want NOERROR/0", header.RCode, len(answers))
	}
	if question.Type != dnsmessage.Type(65) {
		t.Fatalf("blocked HTTPS question echoed as %v", question.Type)
	}

	// blocked TXT: same empty NOERROR shape
	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, dnsQueryPacketTypedVersion(t, ipVersion, "ads.example.com.", dnsmessage.TypeTXT, 0x0a04), 0) {
		t.Fatal("blocked TXT query not claimed")
	}
	if !waitReceived(4) {
		t.Fatal("no reply to a blocked TXT query")
	}
	_, _, answers = parseDnsBlockedReply(t, rec.receivedPackets()[3])
	if len(answers) != 0 {
		t.Fatalf("blocked TXT answers = %d, want 0", len(answers))
	}

	// an unblocked name is not answered by the blocker: it enters the
	// pipeline, fails to resolve (no servers), and sends nothing
	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, dnsQueryPacketTypedVersion(t, ipVersion, "ok.example.org.", dnsmessage.TypeA, 0x0a05), 0) {
		t.Fatal("unblocked A query not claimed")
	}
	// an unblocked HTTPS/SVCB query is claimed and routed to the DoH forward path (not
	// passed through). This test's resolver has remote DoH disabled, so the forward
	// fails fast with a prompt SERVFAIL (never silence on the claimed type).
	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, dnsQueryPacketTypedVersion(t, ipVersion, "ok.example.org.", dnsmessage.Type(65), 0x0a06), 0) {
		t.Fatal("unblocked HTTPS query not claimed")
	}
	// Every other UDP/53 record type is also claimed and forwarded over DoH.
	// With DoH disabled, TXT therefore gets the same prompt SERVFAIL rather
	// than escaping as plaintext or being left unanswered.
	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, dnsQueryPacketTypedVersion(t, ipVersion, "ok.example.org.", dnsmessage.TypeTXT, 0x0a0a), 0) {
		t.Fatal("unblocked TXT query not claimed")
	}
	if !waitReceived(6) {
		t.Fatal("no SERVFAIL for the unblocked forwarded queries with remote DoH off")
	}
	time.Sleep(500 * time.Millisecond)
	// The four blocker-synthesized replies plus one forward SERVFAIL each for
	// HTTPS and TXT; the unblocked A produced none.
	if _, received := rec.counts(); received != 6 {
		t.Fatalf("unexpected downstream replies: %d, want 6", received)
	}
	expectedForwardFailures := map[uint16]dnsmessage.Type{
		0x0a06: dnsmessage.Type(65),
		0x0a0a: dnsmessage.TypeTXT,
	}
	for _, packet := range rec.receivedPackets()[4:6] {
		header, question, answers = parseDnsBlockedReply(t, packet)
		expectedType, ok := expectedForwardFailures[header.ID]
		if !ok || header.RCode != dnsmessage.RCodeServerFailure || len(answers) != 0 {
			t.Fatalf("unblocked forward-failure reply: id=%04x rcode=%v answers=%d", header.ID, header.RCode, len(answers))
		}
		if question.Type != expectedType {
			t.Fatalf("unblocked forward-failure question %04x echoed as %v, want %v", header.ID, question.Type, expectedType)
		}
		delete(expectedForwardFailures, header.ID)
	}
	if len(expectedForwardFailures) != 0 {
		t.Fatalf("missing forward-failure replies: %v", expectedForwardFailures)
	}
	if sent, _ := rec.counts(); sent != 0 {
		t.Fatalf("upstream plaintext DNS pass-throughs: %d, want 0", sent)
	}

	// toggling off returns blocked names to the pipeline (no reply), and
	// toggling back on blocks again — no mux rebuild
	blocker.SetEnabled(false)
	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, dnsQueryPacketTypedVersion(t, ipVersion, "ads.example.com.", dnsmessage.TypeA, 0x0a07), 0) {
		t.Fatal("disabled-blocker A query not claimed")
	}
	time.Sleep(500 * time.Millisecond)
	if _, received := rec.counts(); received != 6 {
		t.Fatalf("disabled blocker still replied: %d", received)
	}
	blocker.SetEnabled(true)
	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, dnsQueryPacketTypedVersion(t, ipVersion, "ads.example.com.", dnsmessage.TypeA, 0x0a08), 0) {
		t.Fatal("re-enabled blocker A query not claimed")
	}
	if !waitReceived(7) {
		t.Fatal("re-enabled blocker did not reply")
	}

	// removing the blocker entirely returns to pipeline behavior
	mux.SetBlocker(nil)
	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, dnsQueryPacketTypedVersion(t, ipVersion, "ads.example.com.", dnsmessage.TypeA, 0x0a09), 0) {
		t.Fatal("nil-blocker A query not claimed")
	}
	time.Sleep(500 * time.Millisecond)
	if _, received := rec.counts(); received != 7 {
		t.Fatalf("nil blocker still replied: %d", received)
	}
}

// TestUpgradeMuxDnsBlockBeatsCache resolves a name with the blocker disabled
// (warming the resolver cache and the reverse index), then enables the
// blocker: the next query must be blocked — the check sits ahead of the
// cache — and blocked answers must not pollute the reverse index.
func TestUpgradeMuxDnsBlockBeatsCache(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		// the resolved address, the doh server and the client's dns path all
		// follow the family under test
		resolved := testFamilyIp(ipVersion, "203.0.113.45")
		const queryName = "cached.blockme.test"

		dohServer := newFamilyHttptestTlsServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeDohWire(w, r, []netip.Addr{netip.MustParseAddr(resolved)}, 60, false)
		}))
		defer dohServer.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		h := newDnsClientHarness(t, ctx, ipVersion, localDohTlsResolverSettings(ipVersion, dohServer))
		defer h.close()

		blocker := blockerTestNew([]string{"blockme.test"}, nil, nil)
		blocker.SetEnabled(false)
		h.mux.SetBlocker(blocker)

		// disabled: resolves normally and records the reverse index
		addrs, err := h.resolver().LookupHost(ctx, queryName)
		if err != nil {
			t.Fatalf("LookupHost (blocker disabled): %v", err)
		}
		if !slices.Contains(addrs, resolved) {
			t.Fatalf("LookupHost = %v, want to contain %s", addrs, resolved)
		}

		// enabled: the same (cached) name is now blocked — subdomain semantics
		// via the blocked base "blockme.test"
		blocker.SetEnabled(true)
		addrs, err = h.resolver().LookupHost(ctx, queryName)
		if err != nil {
			t.Fatalf("LookupHost (blocker enabled): %v", err)
		}
		if slices.Contains(addrs, resolved) {
			t.Fatalf("blocked name still resolves the cached address: %v", addrs)
		}
		if !slices.Contains(addrs, "0.0.0.0") && !slices.Contains(addrs, "::") {
			t.Fatalf("blocked name did not answer the unspecified address: %v", addrs)
		}

		// blocked answers never pollute the reverse index
		if names := h.mux.ServerNames("0.0.0.0"); len(names) != 0 {
			t.Fatalf("reverse index polluted for 0.0.0.0: %v", names)
		}
		if names := h.mux.ServerNames("::"); len(names) != 0 {
			t.Fatalf("reverse index polluted for :: : %v", names)
		}
	})
}
