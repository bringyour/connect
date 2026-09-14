package connect

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
	"golang.org/x/net/dns/dnsmessage"
)

// While the multi client reports that no exit can carry v6, an AAAA query is
// answered locally with an empty NOERROR (IPV6.md B6): nothing goes upstream,
// the id and question echo, and the reply carries no records. Over either
// packet family, since the query's own family says nothing about the exits.
func TestUpgradeMuxAaaaAnsweredEmptyWhileIpv6Unroutable(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		mux, rec, closeMux := newResolverArpaTestMux(t)
		defer closeMux()
		var unroutable atomic.Bool
		unroutable.Store(true)
		mux.SetIpv6Unroutable(unroutable.Load)

		const id = 0x6666
		if !mux.SendPacket(
			TransferPath{},
			protocol.ProvideMode_Network,
			dnsQueryPacketTypedVersion(t, ipVersion, "dual.example.", dnsmessage.TypeAAAA, id),
			0,
		) {
			t.Fatal("AAAA query was not claimed")
		}
		if !waitForCondition(time.Second, func() bool {
			_, received := rec.counts()
			return received == 1
		}) {
			t.Fatal("AAAA query did not receive a prompt local reply")
		}
		if sent, received := rec.counts(); sent != 0 || received != 1 {
			t.Fatalf("AAAA sent/received = %d/%d, want 0/1 (nothing upstream)", sent, received)
		}
		assertResolverArpaNodata(t, rec.receivedPackets()[0], id, dnsmessage.TypeAAAA)
	})
}

// An A query is unaffected by the v6 predicate: it enters the ordinary
// resolve pipeline (no immediate local reply) rather than being answered empty.
func TestUpgradeMuxAQueryUnaffectedWhileIpv6Unroutable(t *testing.T) {
	mux, rec, closeMux := newResolverArpaTestMux(t)
	defer closeMux()
	mux.SetIpv6Unroutable(func() bool { return true })

	const id = 0x4444
	if !mux.SendPacket(
		TransferPath{},
		protocol.ProvideMode_Network,
		dnsQueryPacketTyped(t, "dual.example.", dnsmessage.TypeA, id),
		0,
	) {
		t.Fatal("A query was not claimed")
	}
	if waitForCondition(200*time.Millisecond, func() bool {
		_, received := rec.counts()
		return 0 < received
	}) {
		t.Fatal("A query was answered locally; it must go through the resolve pipeline")
	}
}

// With the predicate removed, or reporting false, an AAAA query goes through
// the ordinary pipeline like an A query.
func TestUpgradeMuxAaaaResolvedWhileIpv6Routable(t *testing.T) {
	for _, install := range []bool{false, true} {
		mux, rec, closeMux := newResolverArpaTestMux(t)
		if install {
			mux.SetIpv6Unroutable(func() bool { return false })
		}
		const id = 0x6667
		if !mux.SendPacket(
			TransferPath{},
			protocol.ProvideMode_Network,
			dnsQueryPacketTyped(t, "dual.example.", dnsmessage.TypeAAAA, id),
			0,
		) {
			closeMux()
			t.Fatal("AAAA query was not claimed")
		}
		if waitForCondition(200*time.Millisecond, func() bool {
			_, received := rec.counts()
			return 0 < received
		}) {
			closeMux()
			t.Fatalf("AAAA query (predicate installed=%t) was answered locally; it must go through the resolve pipeline", install)
		}
		closeMux()
	}
}
