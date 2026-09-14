package connect

import (
	"net"
	"net/netip"
	"slices"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"github.com/urnetwork/connect/v2026/protocol"
)

// ip_ipv6_dualstack_test.go — dual-stack variants of the sniffer and mux
// tests whose v4 originals build packets from fixed v4 literals (IPV6.md D3).

// tlsSegmentPacket is tls443Packet for either family: a PSH|ACK segment to
// :443 carrying payload.
func tlsSegmentPacket(t *testing.T, ipVersion int, srcPort int, payload []byte) []byte {
	t.Helper()
	if ipVersion == 4 {
		return tls443Packet(t, "10.0.0.5", "93.184.216.34", srcPort, payload)
	}
	return testIpv6Tcp(testIpv6Src, testIpv6Dst, nil, srcPort, 443, tcpFlagAck|tcpFlagPsh, 1, payload)
}

func tlsSegmentDestination(ipVersion int) netip.Addr {
	if ipVersion == 4 {
		return netip.MustParseAddr("93.184.216.34")
	}
	return netip.MustParseAddr(testIpv6Dst)
}

// the sni sniffer captures a single-segment and a split ClientHello in both
// families, keyed by the destination address of that family
func TestSniSnifferDualstack(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		var captured []string
		var capturedAddr netip.Addr
		sniffer := newSniSniffer(func(dstAddr netip.Addr, serverName string) {
			captured = append(captured, serverName)
			capturedAddr = dstAddr
		})

		sniffer.observe(tlsSegmentPacket(t, ipVersion, 40000, buildClientHello("pbs.com")))
		if !slices.Equal(captured, []string{"pbs.com"}) {
			t.Fatalf("captured = %v, want [pbs.com]", captured)
		}
		if capturedAddr != tlsSegmentDestination(ipVersion) {
			t.Fatalf("captured addr = %v, want %v", capturedAddr, tlsSegmentDestination(ipVersion))
		}
		if 0 != sniffer.partialCount.Load() {
			t.Fatalf("partials leaked: %d", sniffer.partialCount.Load())
		}

		hello := buildClientHello("split.example.com")
		cut := len(hello) / 2
		sniffer.observe(tlsSegmentPacket(t, ipVersion, 40001, hello[:cut]))
		if sniffer.partialCount.Load() != 1 {
			t.Fatalf("expected 1 buffered partial after segment 1, got %d", sniffer.partialCount.Load())
		}
		sniffer.observe(tlsSegmentPacket(t, ipVersion, 40001, hello[cut:]))
		if !slices.Equal(captured, []string{"pbs.com", "split.example.com"}) {
			t.Fatalf("captured = %v", captured)
		}
		if 0 != sniffer.partialCount.Load() {
			t.Fatalf("partials leaked after completion: %d", sniffer.partialCount.Load())
		}
		// a bare ack and a non-handshake payload record nothing
		sniffer.observe(tlsSegmentPacket(t, ipVersion, 40003, nil))
		sniffer.observe(tlsSegmentPacket(t, ipVersion, 40004, []byte("GET / HTTP/1.1\r\n")))
		if len(captured) != 2 {
			t.Fatalf("captured %d names from non-ClientHello traffic", len(captured)-2)
		}
	})
}

// dnsQueryPacketTypedVersion is dnsQueryPacketTyped with the client and
// server addresses drawn from the family.
func dnsQueryPacketTypedVersion(t *testing.T, ipVersion int, name string, qtype dnsmessage.Type, id uint16) []byte {
	t.Helper()
	if ipVersion == 4 {
		return dnsQueryPacketTyped(t, name, qtype, id)
	}
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
		Version:         6,
		Protocol:        IpProtocolUdp,
		SourceIp:        net.ParseIP("fd00:7572:6e65::9:9"),
		SourcePort:      44444,
		DestinationIp:   net.ParseIP("2001:db8::65:49:70:65"),
		DestinationPort: 53,
	}, queryPayload)
}

// the mux claims UDP/53 in both families and answers resolver.arpa locally
// with a reply addressed back over the client's family
func TestUpgradeMuxResolverArpaDualstack(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		mux, rec, closeMux := newResolverArpaTestMux(t)
		defer closeMux()

		const id = 0x9470
		query := dnsQueryPacketTypedVersion(t, ipVersion, "_dns.resolver.arpa.", dnsTypeSvcb, id)
		if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, query, 0) {
			t.Fatal("resolver discovery query was not claimed")
		}
		if !waitForCondition(time.Second, func() bool {
			_, received := rec.counts()
			return received == 1
		}) {
			t.Fatal("resolver discovery did not receive a prompt local reply")
		}
		if sent, _ := rec.counts(); sent != 0 {
			t.Fatalf("resolver discovery sent %d upstream, want 0", sent)
		}
		reply := rec.receivedPackets()[0]
		assertResolverArpaNodata(t, reply, id, dnsTypeSvcb)
		replyPath, err := ParseIpPath(reply)
		if err != nil || replyPath.Version != ipVersion || replyPath.SourcePort != 53 {
			t.Fatalf("reply path = %+v, %v; want a v%d reply from :53", replyPath, err, ipVersion)
		}
	})
}
