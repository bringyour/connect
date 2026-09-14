package connect

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
	"golang.org/x/net/dns/dnsmessage"
)

func newResolverArpaTestMux(
	t *testing.T,
) (*UpgradeMux, *ipMuxRecorder, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	rec := &ipMuxRecorder{}
	settings := DefaultUpgradeMuxSettings()
	mux, err := NewUpgradeMux(
		ctx,
		TransferPath{},
		protocol.ProvideMode_Network,
		0,
		rec.receive,
		settings,
		nil,
	)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	mux.SetUpstream(rec.upstream)
	return mux, rec, func() {
		mux.Close()
		cancel()
	}
}

func resolverArpaQueryPayload(t *testing.T, name string, qtype dnsmessage.Type, id uint16) []byte {
	t.Helper()
	packet := dnsQueryPacketTyped(t, name, qtype, id)
	_, payload, err := ParseIpPathWithPayload(packet)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func assertResolverArpaNodata(
	t *testing.T,
	packet []byte,
	id uint16,
	qtype dnsmessage.Type,
) {
	t.Helper()
	header, question, answers := parseDnsBlockedReply(t, packet)
	if header.ID != id || !header.Response || header.RCode != dnsmessage.RCodeSuccess {
		t.Fatalf("resolver.arpa header = %+v", header)
	}
	if question.Type != qtype {
		t.Fatalf("resolver.arpa question type = %v, want %v", question.Type, qtype)
	}
	if len(answers) != 0 {
		t.Fatalf("resolver.arpa answer count = %d, want NODATA", len(answers))
	}
}

func TestUpgradeMuxResolverArpaSvcbIsAnsweredLocally(t *testing.T) {
	mux, rec, closeMux := newResolverArpaTestMux(t)
	defer closeMux()

	const id = 0x9462
	if !mux.SendPacket(
		TransferPath{},
		protocol.ProvideMode_Network,
		dnsQueryPacketTyped(t, "_dns.resolver.arpa.", dnsTypeSvcb, id),
		0,
	) {
		t.Fatal("resolver discovery query was not claimed")
	}
	if !waitForCondition(time.Second, func() bool {
		_, received := rec.counts()
		return received == 1
	}) {
		t.Fatal("resolver discovery did not receive a prompt local reply")
	}
	if sent, received := rec.counts(); sent != 0 || received != 1 {
		t.Fatalf("resolver discovery sent/received = %d/%d, want 0/1", sent, received)
	}
	assertResolverArpaNodata(t, rec.receivedPackets()[0], id, dnsTypeSvcb)
}

func TestUpgradeMuxResolverArpaSubdomainIsAnsweredLocally(t *testing.T) {
	mux, rec, closeMux := newResolverArpaTestMux(t)
	defer closeMux()

	const id = 0x9463
	if !mux.SendPacket(
		TransferPath{},
		protocol.ProvideMode_Network,
		dnsQueryPacketTyped(t, "unexpected.resolver.arpa.", dnsmessage.TypeTXT, id),
		0,
	) {
		t.Fatal("resolver.arpa subdomain query was not claimed")
	}
	if !waitForCondition(time.Second, func() bool {
		_, received := rec.counts()
		return received == 1
	}) {
		t.Fatal("resolver.arpa subdomain did not receive a prompt local reply")
	}
	assertResolverArpaNodata(t, rec.receivedPackets()[0], id, dnsmessage.TypeTXT)
}

func TestUpgradeMuxResolverArpaTcpIsAnsweredLocally(t *testing.T) {
	mux, _, closeMux := newResolverArpaTestMux(t)
	defer closeMux()

	const id = 0x9464
	response := mux.resolveDnsTcpQuery(
		resolverArpaQueryPayload(t, "_dns.resolver.arpa.", dnsTypeSvcb, id),
	)
	if len(response) < 2 || binary.BigEndian.Uint16(response[:2]) != id {
		t.Fatalf("tcp resolver discovery response = %x", response)
	}
	packet := ipOosPacket(&IpPath{
		Version:         4,
		Protocol:        IpProtocolUdp,
		SourceIp:        net.IPv4(10, 0, 0, 1),
		SourcePort:      53,
		DestinationIp:   net.IPv4(10, 0, 0, 2),
		DestinationPort: 44444,
	}, response)
	assertResolverArpaNodata(t, packet, id, dnsTypeSvcb)
}
