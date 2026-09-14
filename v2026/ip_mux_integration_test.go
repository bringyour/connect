package connect

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestUpgradeMuxMultiClientIntegration interposes the UpgradeMux on a real
// RemoteUserNatMultiClient backed by a real provider (an echo exit), exactly as
// DeviceLocal wires it, and verifies end to end:
//
//	(a) non-claimed traffic round-trips through the multi-client + provider unchanged,
//	(b) a DNS query is claimed and resolved by the mux (local DoH) without touching the
//	    provider, and
//	(c) the resolution populates the reverse index, which drives the multi-client's
//	    base-domain ServerName affinity (point 4).
func TestUpgradeMuxMultiClientIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	clientId := NewId()
	source := SourceId(clientId)

	// provider/exit: echo each IP packet back with its path reversed
	clientSettings := DefaultClientSettingsWithBufferSize(32)
	providerClient := NewClient(ctx, NewId(), NewNoContractClientOob(), clientSettings)
	defer providerClient.Cancel()

	type echo struct {
		frame       *protocol.Frame
		destination Id
		transferKey TransferKey
	}
	echoes := make(chan echo, 32)
	asyncErrors := make(chan error, 1)
	recordAsyncError := func(err error) {
		select {
		case asyncErrors <- err:
		default:
		}
	}
	var echoWaitGroup sync.WaitGroup
	echoWaitGroup.Add(1)
	go func() {
		defer echoWaitGroup.Done()
		for {
			select {
			case <-ctx.Done():
				for {
					select {
					case echo := <-echoes:
						MessagePoolReturn(echo.frame.MessageBytes)
					default:
						return
					}
				}
			case echo := <-echoes:
				if !providerClient.SendWithTimeout(
					echo.frame,
					echo.destination,
					func(error) {},
					30*time.Second,
					echo.transferKey,
				) {
					MessagePoolReturn(echo.frame.MessageBytes)
					recordAsyncError(fmt.Errorf("send provider echo"))
					return
				}
			}
		}
	}()
	providerReceiveUnsub := providerClient.AddReceiveCallback(func(src TransferPath, frames []*protocol.Frame, peer Peer) {
		for _, frame := range frames {
			msg, err := FromFrame(frame)
			if err != nil {
				continue
			}
			toProvider, ok := msg.(*protocol.IpPacketToProvider)
			if !ok {
				continue
			}
			ipPath, payload, err := ParseIpPathWithPayload(toProvider.IpPacket.PacketBytes)
			if err != nil {
				continue
			}
			fromProvider := &protocol.IpPacketFromProvider{
				IpPacket: &protocol.IpPacket{PacketBytes: ipOosPacket(ipPath.Reverse(), payload)},
			}
			echoFrame, err := ToFrame(fromProvider, DefaultProtocolVersion)
			if err != nil {
				continue
			}
			select {
			case echoes <- echo{
				frame:       echoFrame,
				destination: src.SourceId,
				transferKey: peer.TransferKey,
			}:
			default:
				MessagePoolReturn(echoFrame.MessageBytes)
				recordAsyncError(fmt.Errorf("provider echo queue overflow"))
			}
		}
	})
	defer func() {
		providerReceiveUnsub()
		cancel()
		echoWaitGroup.Wait()
	}()

	// client-side receiver (the "OS TUN")
	received := make(chan []byte, 32)
	clientReceive := func(src TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		packetCopy := append([]byte{}, packet...)
		select {
		case received <- packetCopy:
		default:
			recordAsyncError(fmt.Errorf("client receive collector overflow"))
		}
	}

	// the mux resolves DNS through a local DoH server
	const resolvedIp = "203.0.113.77"
	const queryName = "a.foo.com"
	dohServer, dns := newDohWireServer(t, resolvedIp)
	defer dohServer.Close()

	muxSettings := DefaultUpgradeMuxSettings()
	muxSettings.Dns.Resolver = dns

	// interpose the mux on the multi-client, exactly as DeviceLocal does
	mux, err := NewUpgradeMux(ctx, source, protocol.ProvideMode_Network, 30*time.Second, clientReceive, muxSettings, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Close()

	multiSettings := DefaultMultiClientSettings()
	multiSettings.TcpCollapsePrevention = false
	multi := NewRemoteUserNatMultiClient(ctx, testMultiClientGenerator(providerClient), mux.Receive, protocol.ProvideMode_Network, multiSettings)
	defer multi.Close()
	mux.SetUpstream(multi.SendPacket)
	multi.SetServerNameLookup(mux)

	// client tunnel address, outside the local-address pool so it is never mistaken for
	// the mux's own tun address
	clientIp := net.ParseIP("10.0.0.1")

	// (a) pass-through: a UDP packet to a non-claimed port round-trips via the provider
	{
		passPayload := []byte("integration-ping")
		passPacket := ipOosPacket(&IpPath{
			Version:         4,
			Protocol:        IpProtocolUdp,
			SourceIp:        clientIp,
			SourcePort:      40000,
			DestinationIp:   net.ParseIP("9.9.9.9"),
			DestinationPort: 9999,
		}, passPayload)

		if !mux.SendPacket(source, protocol.ProvideMode_Network, passPacket, 30*time.Second) {
			t.Fatal("pass-through SendPacket failed")
		}
		select {
		case got := <-received:
			_, payload, err := ParseIpPathWithPayload(got)
			if err != nil || string(payload) != string(passPayload) {
				t.Fatalf("pass-through echo payload=%q (err %v), want %q", payload, err, passPayload)
			}
		case asyncErr := <-asyncErrors:
			t.Fatalf("asynchronous mux integration worker: %v", asyncErr)
		case <-time.After(30 * time.Second):
			t.Fatal("pass-through echo not received through the multi-client + provider")
		}
	}

	// (b) DNS: the mux claims the query, resolves via local DoH, and replies to the client
	{
		dnsPacket := ipOosPacket(&IpPath{
			Version:         4,
			Protocol:        IpProtocolUdp,
			SourceIp:        clientIp,
			SourcePort:      40001,
			DestinationIp:   net.ParseIP("1.1.1.1"),
			DestinationPort: 53,
		}, buildDnsQuery(t, queryName))

		if !mux.SendPacket(source, protocol.ProvideMode_Network, dnsPacket, 30*time.Second) {
			t.Fatal("dns SendPacket returned false")
		}
		select {
		case got := <-received:
			_, payload, err := ParseIpPathWithPayload(got)
			if err != nil {
				t.Fatalf("dns reply parse: %v", err)
			}
			if !dnsReplyHasAddr(payload, resolvedIp) {
				t.Fatalf("dns reply missing %s", resolvedIp)
			}
		case asyncErr := <-asyncErrors:
			t.Fatalf("asynchronous mux integration worker: %v", asyncErr)
		case <-time.After(30 * time.Second):
			t.Fatal("dns reply not received")
		}
	}

	// (c) the resolution populated the reverse index, which drives base-domain affinity
	if names := mux.ServerNames(resolvedIp); len(names) == 0 {
		t.Fatalf("reverse index empty for %s after the DNS query", resolvedIp)
	}
	multi.stateLock.Lock()
	affinity := multi.affinityIpPathsWithLock(&IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		DestinationIp:   net.ParseIP(resolvedIp),
		DestinationPort: 443,
	})
	multi.stateLock.Unlock()
	if len(affinity) != 1 || affinity[0].ServerName != "foo.com" {
		t.Fatalf("multi-client affinity = %+v, want one base-domain path foo.com (from the mux reverse index)", affinity)
	}
}

// TestUpgradeMuxDefaultDnsThroughTunnel exercises the DEFAULT app mux DNS path: DoH
// resolved remotely through the tunnel (mux tun → multi-client → a real provider that
// egresses to the host) against a local DoH server. This is the path the apps use
// (remote DoH), distinct from the local-DoH tests, and the prime suspect for the iOS
// "black hole" where claimed :53 queries never get answered.
func TestUpgradeMuxDefaultDnsThroughTunnel(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	clientId := NewId()
	source := SourceId(clientId)

	// provider/exit: a real RemoteUserNatProvider over a host-proxying LocalUserNat, so
	// the mux's DoH connection actually egresses to the local DoH server through the
	// tunnel. Permissive security so the loopback test server is not dropped.
	clientSettings := DefaultClientSettingsWithBufferSize(32)
	providerClient := NewClient(ctx, NewId(), NewNoContractClientOob(), clientSettings)
	defer providerClient.Cancel()

	providerLocalUserNat := NewLocalUserNatWithDefaults(ctx, "test-exit")
	providerNatSettings := DefaultRemoteUserNatProviderSettings()
	providerNatSettings.SecurityPolicyGenerator = DisableSecurityPolicyWithStats
	provider := NewRemoteUserNatProvider(providerClient, providerLocalUserNat, providerNatSettings)
	defer provider.Close()

	// local DoH server (reachable on the host loopback via the provider's LocalUserNat)
	const resolvedIp = "203.0.113.88"
	const queryName = "a.foo.com"
	dohServer, localDns := newDohWireServer(t, resolvedIp)
	defer dohServer.Close()

	received := make(chan []byte, 32)
	var receiveDropCount int
	var receiveDropLock sync.Mutex
	clientReceive := func(src TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		packetCopy := append([]byte{}, packet...)
		select {
		case received <- packetCopy:
		default:
			func() {
				receiveDropLock.Lock()
				defer receiveDropLock.Unlock()
				receiveDropCount += 1
			}()
		}
	}

	// the default app mux, but with DoH pointed at the local server (remote DoH = through
	// the tunnel; reuse the test server's cert pool)
	muxSettings := DefaultUpgradeMuxSettings()
	muxSettings.Dns.Resolver = &DnsResolverSettings{
		EnableRemoteDoh:   true,
		RemoteDohUrlsIpv4: []string{dohServer.URL},
		TlsConfig:         localDns.TlsConfig,
	}
	// This test is specifically the tunnel path. A production local fallback
	// can legitimately win when race instrumentation slows the in-memory
	// transport, which would turn this into a test of the host resolver and
	// make its answer depend on public DNS.
	muxSettings.Dns.Fallback = nil
	mux, err := NewUpgradeMux(ctx, source, protocol.ProvideMode_Network, 30*time.Second, clientReceive, muxSettings, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Close()

	multiSettings := DefaultMultiClientSettings()
	multiSettings.TcpCollapsePrevention = false
	multiSettings.SecurityPolicyGenerator = DisableSecurityPolicyWithStats
	multi := NewRemoteUserNatMultiClient(ctx, testMultiClientGenerator(providerClient), mux.Receive, protocol.ProvideMode_Network, multiSettings)
	defer multi.Close()
	mux.SetUpstream(multi.SendPacket)
	multi.SetServerNameLookup(mux)

	dnsPacket := ipOosPacket(&IpPath{
		Version:         4,
		Protocol:        IpProtocolUdp,
		SourceIp:        net.ParseIP("10.0.0.1"),
		SourcePort:      41000,
		DestinationIp:   net.ParseIP("1.1.1.1"),
		DestinationPort: 53,
	}, buildDnsQuery(t, queryName))

	if !mux.SendPacket(source, protocol.ProvideMode_Network, dnsPacket, 30*time.Second) {
		t.Fatal("dns SendPacket returned false")
	}
	select {
	case got := <-received:
		_, payload, err := ParseIpPathWithPayload(got)
		if err != nil {
			t.Fatalf("dns reply parse: %v", err)
		}
		if !dnsReplyHasAddr(payload, resolvedIp) {
			t.Fatalf("dns reply missing %s (remote DoH through the tunnel resolved nothing)", resolvedIp)
		}
	case <-time.After(25 * time.Second):
		t.Fatal("dns reply not received — remote DoH through the tunnel black-holed the query")
	}
	func() {
		receiveDropLock.Lock()
		defer receiveDropLock.Unlock()
		if receiveDropCount != 0 {
			t.Fatalf("client receive callback dropped %d packet(s)", receiveDropCount)
		}
	}()
}

func buildDnsQuery(t *testing.T, domain string) []byte {
	t.Helper()
	name, err := dnsmessage.NewName(domain + ".")
	if err != nil {
		t.Fatal(err)
	}
	builder := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: 0x1234, RecursionDesired: true})
	if err := builder.StartQuestions(); err != nil {
		t.Fatal(err)
	}
	if err := builder.Question(dnsmessage.Question{Name: name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET}); err != nil {
		t.Fatal(err)
	}
	query, err := builder.Finish()
	if err != nil {
		t.Fatal(err)
	}
	return query
}

func dnsReplyHasAddr(payload []byte, ip string) bool {
	var parser dnsmessage.Parser
	if _, err := parser.Start(payload); err != nil {
		return false
	}
	if err := parser.SkipAllQuestions(); err != nil {
		return false
	}
	answers, err := parser.AllAnswers()
	if err != nil {
		return false
	}
	for _, answer := range answers {
		if answer.Header.Type != dnsmessage.TypeA {
			continue
		}
		if a, ok := answer.Body.(*dnsmessage.AResource); ok {
			if net.IP(a.A[:]).String() == ip {
				return true
			}
		}
	}
	return false
}
