package connect

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"github.com/urnetwork/connect/v2026/protocol"
)

// answerDnsAddr unpacks a plaintext dns query and packs an answer with the
// given address when the question is the address record type of its family
// (A for v4, AAAA for v6), mirroring the query id and question. A question of
// the other family gets an empty NOERROR (NODATA), so a dual-stack resolver
// that asks both types gets a prompt negative for the family the fake does
// not serve instead of waiting on silence.
func answerDnsAddr(queryPayload []byte, addr netip.Addr) ([]byte, bool) {
	var msg dnsmessage.Message
	if err := msg.Unpack(queryPayload); err != nil {
		return nil, false
	}
	if len(msg.Questions) == 0 {
		return nil, false
	}
	question := msg.Questions[0]
	resp := dnsmessage.Message{
		Header: dnsmessage.Header{
			ID:                 msg.Header.ID,
			Response:           true,
			RecursionAvailable: true,
		},
		Questions: msg.Questions,
	}
	header := dnsmessage.ResourceHeader{
		Name:  question.Name,
		Type:  question.Type,
		Class: dnsmessage.ClassINET,
		TTL:   60,
	}
	switch {
	case question.Type == dnsmessage.TypeA && addr.Is4():
		resp.Answers = []dnsmessage.Resource{{Header: header, Body: &dnsmessage.AResource{A: addr.As4()}}}
	case question.Type == dnsmessage.TypeAAAA && addr.Is6() && !addr.Is4In6():
		resp.Answers = []dnsmessage.Resource{{Header: header, Body: &dnsmessage.AAAAResource{AAAA: addr.As16()}}}
	case question.Type == dnsmessage.TypeA || question.Type == dnsmessage.TypeAAAA:
		// the other family: NODATA
	default:
		return nil, false
	}
	respPayload, err := resp.Pack()
	if err != nil {
		return nil, false
	}
	return respPayload, true
}

func dnsQuestionName(payload []byte) (string, bool) {
	var msg dnsmessage.Message
	if err := msg.Unpack(payload); err != nil {
		return "", false
	}
	if len(msg.Questions) == 0 {
		return "", false
	}
	return strings.TrimSuffix(strings.ToLower(msg.Questions[0].Name.String()), "."), true
}

// TestDohServerNameRemoteDnsResolution verifies that a hostname-form doh
// server name resolves over remote plain dns even when EnableRemoteDns is
// false — the one permitted consumer — and that no other name uses remote
// dns while it is disabled. Runs over both families: the remote dns server,
// the record type and the answer all follow the family under test.
func TestDohServerNameRemoteDnsResolution(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		recordType := testDnsRecordType(ipVersion)
		remoteDnsServer := testDocAddr(ipVersion, 53)
		serverNameAddr := testDocAddr(ipVersion, 50)

		// a plaintext dns server that answers the family's address queries
		// for the doh server name
		pc, err := net.ListenPacket(testUdpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
		if err != nil {
			t.Fatal(err)
		}
		defer pc.Close()
		go func() {
			buf := make([]byte, 2048)
			for {
				n, from, err := pc.ReadFrom(buf)
				if err != nil {
					return
				}
				if respPayload, ok := answerDnsAddr(buf[:n], serverNameAddr); ok {
					pc.WriteTo(respPayload, from)
				}
			}
		}()

		var mu sync.Mutex
		dns53Dials := []string{}
		resolvedServerNames := map[string][]netip.Addr{}

		settings := DefaultDohSettings()
		settings.DnsResolverSettings = remoteDohResolverSettings(ipVersion, "https://doh.test/dns-query")
		// EnableRemoteDns deliberately false: only the doh server name may
		// resolve over these
		setRemoteDnsServers(settings, ipVersion, remoteDnsServer.String())
		settings.DohServerResolvedCallback = func(domain string, addrs []netip.Addr) {
			mu.Lock()
			defer mu.Unlock()
			resolvedServerNames[domain] = addrs
		}
		// the "tunnel" dialer: track plaintext :53 dials and redirect them to the
		// fake server; fail everything else (the doh https dial) fast
		settings.DialContextSettings = &DialContextSettings{
			DialContext: func(dialCtx context.Context, network string, addr string) (net.Conn, error) {
				if strings.HasSuffix(addr, ":53") {
					mu.Lock()
					dns53Dials = append(dns53Dials, addr)
					mu.Unlock()
					return (&net.Dialer{}).DialContext(dialCtx, network, pc.LocalAddr().String())
				}
				return nil, net.ErrClosed
			},
		}
		cache := NewDohCache(settings)

		// the doh server name resolves over remote plain dns despite
		// EnableRemoteDns being false
		addrs, authoritative := cache.QueryResult(ctx, recordType, "doh.test")
		if !authoritative || len(addrs) == 0 || addrs[0] != serverNameAddr {
			t.Fatalf("doh server name did not resolve over remote dns: addrs=%v authoritative=%t", addrs, authoritative)
		}
		wantDialPrefix := net.JoinHostPort(remoteDnsServer.String(), "")
		mu.Lock()
		if len(dns53Dials) == 0 || !strings.HasPrefix(dns53Dials[0], wantDialPrefix) {
			t.Fatalf("expected the resolution to dial the remote dns server %s, dials=%v", remoteDnsServer, dns53Dials)
		}
		if resolved := resolvedServerNames["doh.test"]; len(resolved) == 0 || resolved[0] != serverNameAddr {
			t.Fatalf("expected the server resolved callback, got %v", resolvedServerNames)
		}
		dialCountAfterServerName := len(dns53Dials)
		mu.Unlock()

		// any other name must not use remote dns while it is disabled: the remote
		// doh stage fails (the dialer refuses), and no stage falls through to
		// plaintext :53
		otherCtx, otherCancel := context.WithTimeout(ctx, 5*time.Second)
		defer otherCancel()
		otherAddrs, otherAuthoritative := cache.QueryResult(otherCtx, recordType, "other.test")
		if len(otherAddrs) != 0 || otherAuthoritative {
			t.Fatalf("unexpected resolution for a non server name: addrs=%v authoritative=%t", otherAddrs, otherAuthoritative)
		}
		mu.Lock()
		if len(dns53Dials) != dialCountAfterServerName {
			t.Fatalf("a non server name used remote dns while disabled: dials=%v", dns53Dials)
		}
		mu.Unlock()
	})
}

// TestUpgradeMuxDohServerNameResolvesThroughTunnel verifies the end to end
// path: with a hostname-form doh server configured, a client dns query into
// the mux drives the doh server name resolution as a plaintext dns packet
// that egresses THROUGH the mux to the remote side, is answered there, and
// the resolved server address lands in the mux's ip→hostname reverse index
// (so the block action ignore matcher covers the server name and address).
// Over v6 the server name query leaves the tun as a v6 packet to the v6
// remote dns server and is answered with an AAAA.
func TestUpgradeMuxDohServerNameResolvesThroughTunnel(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		remoteDnsServer := testDocAddr(ipVersion, 53)
		serverNameAddr := testDocAddr(ipVersion, 99)

		var mu sync.Mutex
		serverNameQueried := false
		var mux *UpgradeMux

		// the fake remote side: watch the tunnel egress for the plaintext dns
		// query of the doh server name and answer it
		upstream := func(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool {
			ipPath, payload, err := ParseIpPathWithPayload(packet)
			if err != nil {
				return true
			}
			if ipPath.Protocol != IpProtocolUdp || ipPath.DestinationPort != 53 {
				// the doh https connect attempts and other egress pass through
				return true
			}
			name, ok := dnsQuestionName(payload)
			if !ok || name != "doh.test" {
				return true
			}
			if ipPath.Version != ipVersion || ipPath.DestinationIp.String() != remoteDnsServer.String() {
				t.Errorf("server name query egressed as v%d to %s, want v%d to the configured remote dns server %s", ipPath.Version, ipPath.DestinationIp, ipVersion, remoteDnsServer)
			}
			respPayload, ok := answerDnsAddr(payload, serverNameAddr)
			if !ok {
				return true
			}
			mu.Lock()
			serverNameQueried = true
			mu.Unlock()
			respPath := ipPath.Reverse()
			respPacket := ipOosUdpPacket(respPath, respPayload)
			// deliver the remote answer back through the tunnel
			go mux.Receive(source, provideMode, respPath, respPacket)
			return true
		}

		rec := &ipMuxRecorder{}
		settings := DefaultUpgradeMuxSettings()
		settings.Dns.Resolver = remoteDohResolverSettings(ipVersion, "https://doh.test/dns-query")
		// EnableRemoteDns stays false: the server name is the one permitted
		// plaintext consumer
		if ipVersion == 6 {
			settings.Dns.Resolver.RemoteDnsIpv6 = []string{remoteDnsServer.String()}
		} else {
			settings.Dns.Resolver.RemoteDnsIpv4 = []string{remoteDnsServer.String()}
		}
		// no local fallback: resolution must flow through the tunnel only
		settings.Dns.Fallback = nil

		m, err := NewUpgradeMux(ctx, TransferPath{}, protocol.ProvideMode_Network, 0, rec.receive, settings, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer m.Close()
		mux = m
		mux.SetUpstream(upstream)

		// a client query for any name forces the doh stage, which needs the doh
		// server address, which drives the server name resolution
		clientQuery := dnsQueryPacketTypedVersion(t, ipVersion, "site.example.test.", dnsmessage.TypeA, 0x5e11)
		if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, clientQuery, 0) {
			t.Fatal("SendPacket returned false; the DNS query was not claimed")
		}

		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			queried := serverNameQueried
			mu.Unlock()
			if queried {
				// the resolved server address reaches the reverse index, keyed to
				// the server name (the exclusion linkage)
				names := mux.ServerNames(serverNameAddr.String())
				found := false
				for _, name := range names {
					if name == "doh.test" {
						found = true
						break
					}
				}
				if found {
					return
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
		mu.Lock()
		queried := serverNameQueried
		mu.Unlock()
		if !queried {
			t.Fatal("the doh server name dns query never egressed through the mux to the remote side")
		}
		t.Fatal("the resolved doh server address never reached the mux reverse index")
	})
}
