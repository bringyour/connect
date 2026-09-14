package connect

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

type smtpCountingSecurityPolicy struct {
	stats       *SecurityPolicyStatsCollector
	result      SecurityPolicyResult
	egressCalls atomic.Int64
}

func (self *smtpCountingSecurityPolicy) Stats() *SecurityPolicyStatsCollector {
	return self.stats
}

func (self *smtpCountingSecurityPolicy) InspectEgress(
	_ protocol.ProvideMode,
	_ *IpPath,
	_ []byte,
) (SecurityPolicyResult, error) {
	self.egressCalls.Add(1)
	return self.result, nil
}

func (self *smtpCountingSecurityPolicy) InspectIngress(
	_ protocol.ProvideMode,
	_ *IpPath,
	_ []byte,
) (SecurityPolicyResult, error) {
	return SecurityPolicyResultAllow, nil
}

func (self *smtpCountingSecurityPolicy) RefreshEgress(_ *IpPath)  {}
func (self *smtpCountingSecurityPolicy) RefreshIngress(_ *IpPath) {}

func newSmtpIntegrationMulti(
	t *testing.T,
	policy *smtpCountingSecurityPolicy,
	receive ReceivePacketFunction,
) *RemoteUserNatMultiClient {
	t.Helper()
	settings := DefaultMultiClientSettings()
	settings.EventEpoch = 10 * time.Millisecond
	settings.HeartbeatInterval = 0
	settings.ProviderProbe = false
	settings.IpAssocSettings = nil
	settings.SecurityPolicyGenerator = func(
		_ context.Context,
		_ *SecurityPolicyStatsCollector,
	) SecurityPolicy {
		return policy
	}
	return NewRemoteUserNatMultiClient(
		context.Background(),
		&testingEmptyMultiClientGenerator{},
		receive,
		protocol.ProvideMode_Network,
		settings,
	)
}

func newSmtpIntegrationBasicClient(
	t *testing.T,
	policy *smtpCountingSecurityPolicy,
	receive ReceivePacketFunction,
) *RemoteUserNatClient {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	clientSettings := DefaultClientSettings()
	clientSettings.SendBufferSettings.SequenceBufferSize = 0
	clientSettings.SendBufferSettings.AckBufferSize = 0
	clientSettings.ReceiveBufferSettings.SequenceBufferSize = 0
	clientSettings.ForwardBufferSettings.SequenceBufferSize = 0
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), clientSettings)
	remote := NewRemoteUserNatClient(
		client,
		receive,
		nil,
		protocol.ProvideMode_Network,
	)
	remote.securityPolicy = policy
	t.Cleanup(func() {
		remote.Close()
		client.Cancel()
		cancel()
	})
	return remote
}

func nextSmtpBlockActions(t *testing.T, actions <-chan []*BlockAction) []*BlockAction {
	t.Helper()
	select {
	case result := <-actions:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for SMTP routing decision")
		return nil
	}
}

func TestMultiClientPort25BypassesCFAAAndKillSwitchLocally(t *testing.T) {
	policy := &smtpCountingSecurityPolicy{
		stats:  DefaultSecurityPolicyStatsCollector(),
		result: SecurityPolicyResultIncident,
	}
	multi := newSmtpIntegrationMulti(t, policy, func(
		_ TransferPath,
		_ protocol.ProvideMode,
		_ *IpPath,
		_ []byte,
	) {
	})
	defer multi.Close()

	// localSecurityBypass defaults false, which is the enabled kill switch.
	// A remote route override must not move this exception onto a provider.
	routeOverrideId := NewId()
	multi.SetBlockActionOverrides([]*BlockActionOverride{{
		OverrideId: routeOverrideId,
		Hosts:      []string{"203.0.113.10"},
		RouteOverride: &RouteOverride{
			Local: false,
		},
	}})
	actions := make(chan []*BlockAction, 4)
	unsub := multi.AddBlockActionCallback(func(value []*BlockAction) {
		actions <- value
	})
	defer unsub()

	packet := craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("10.0.0.2"),
		48001,
		net.ParseIP("203.0.113.10"),
		smtpLocalPort,
		true,
		nil,
	)
	packetBytes := ByteCount(len(packet))
	if !multi.SendPacket(SourceId(NewId()), protocol.ProvideMode_Network, packet, 0) {
		t.Fatal("TCP/25 SYN was not accepted by the explicit local route")
	}
	if calls := policy.egressCalls.Load(); calls != 0 {
		t.Fatalf("TCP/25 reached CFAA/security inspection %d times", calls)
	}

	stats := multi.PacketStats()
	if stats.LocalEgressPacketCount != 1 || stats.LocalEgressByteCount != packetBytes {
		t.Fatalf("TCP/25 local stats = %d/%dB, want 1/%dB", stats.LocalEgressPacketCount, stats.LocalEgressByteCount, packetBytes)
	}
	if stats.RemoteEgressPacketCount != 0 || stats.BlockEgressPacketCount != 0 {
		t.Fatalf("TCP/25 used a non-local route: remote=%d block=%d", stats.RemoteEgressPacketCount, stats.BlockEgressPacketCount)
	}

	blockActions := nextSmtpBlockActions(t, actions)
	if len(blockActions) != 1 {
		t.Fatalf("TCP/25 routing actions = %d, want 1", len(blockActions))
	}
	action := blockActions[0]
	if action.Block || !action.Local || action.PacketCount != 1 {
		t.Fatalf("TCP/25 action = block:%t local:%t packets:%d", action.Block, action.Local, action.PacketCount)
	}
	if action.RouteOverrideId == nil || *action.RouteOverrideId != routeOverrideId {
		t.Fatal("TCP/25 routing action did not retain the matched override for observability")
	}
}

func TestMultiClientRejectsPlaintextSmtpWithResetBeforeCFAA(t *testing.T) {
	policy := &smtpCountingSecurityPolicy{
		stats:  DefaultSecurityPolicyStatsCollector(),
		result: SecurityPolicyResultAllow,
	}
	resets := make(chan []byte, 4)
	multi := newSmtpIntegrationMulti(t, policy, func(
		_ TransferPath,
		_ protocol.ProvideMode,
		_ *IpPath,
		packet []byte,
	) {
		packetCopy := append([]byte(nil), packet...)
		resets <- packetCopy
	})
	defer multi.Close()

	actions := make(chan []*BlockAction, 4)
	unsub := multi.AddBlockActionCallback(func(value []*BlockAction) {
		actions <- value
	})
	defer unsub()

	packet := smtpTestTcp4Packet(
		byte(tcpFlagAck|tcpFlagPsh),
		12000,
		34000,
		[]byte("EHLO plaintext.example\r\n"),
	)
	packetBytes := ByteCount(len(packet))
	if multi.SendPacket(SourceId(NewId()), protocol.ProvideMode_Network, packet, 0) {
		t.Fatal("plaintext TCP/465 was accepted")
	}
	MessagePoolReturn(packet)

	if calls := policy.egressCalls.Load(); calls != 0 {
		t.Fatalf("rejected TCP/465 reached CFAA/security inspection %d times", calls)
	}
	select {
	case reset := <-resets:
		_, sourceIp, destinationIp, transport, ok := parseIpv4(reset)
		var tcp parsedTcp
		if !ok || !parseTcpPacket(sourceIp, destinationIp, transport, &tcp) || !tcp.rst {
			t.Fatal("plaintext TCP/465 did not receive a valid TCP reset")
		}
	case <-time.After(time.Second):
		t.Fatal("plaintext TCP/465 rejection timed out instead of resetting")
	}

	stats := multi.PacketStats()
	if stats.BlockEgressPacketCount != 1 || stats.BlockEgressByteCount != packetBytes {
		t.Fatalf("plaintext TCP/465 block stats = %d/%dB", stats.BlockEgressPacketCount, stats.BlockEgressByteCount)
	}
	blockActions := nextSmtpBlockActions(t, actions)
	if len(blockActions) != 1 || !blockActions[0].Block || blockActions[0].Local {
		t.Fatalf("plaintext TCP/465 action = %#v", blockActions)
	}
}

func TestMultiClientRejectsPlaintext587BeforeCFAA(t *testing.T) {
	policy := &smtpCountingSecurityPolicy{
		stats:  DefaultSecurityPolicyStatsCollector(),
		result: SecurityPolicyResultAllow,
	}
	var resetCount atomic.Int64
	multi := newSmtpIntegrationMulti(t, policy, func(
		_ TransferPath,
		_ protocol.ProvideMode,
		_ *IpPath,
		_ []byte,
	) {
		resetCount.Add(1)
	})
	defer multi.Close()

	packet := smtpTestTcp4PacketToPort(
		smtpStartTlsPort,
		byte(tcpFlagAck|tcpFlagPsh),
		12500,
		34500,
		[]byte("AUTH PLAIN secret\r\n"),
	)
	if multi.SendPacket(SourceId(NewId()), protocol.ProvideMode_Network, packet, 0) {
		t.Fatal("plaintext TCP/587 AUTH was accepted")
	}
	MessagePoolReturn(packet)
	if calls := policy.egressCalls.Load(); calls != 0 {
		t.Fatalf("rejected TCP/587 reached CFAA/security inspection %d times", calls)
	}
	if got := resetCount.Load(); got != 1 {
		t.Fatalf("plaintext TCP/587 resets = %d, want 1", got)
	}
}

func TestBasicRemoteClientPreservesSmtpPolicyEntryPoint(t *testing.T) {
	t.Run("port 25 stays local before CFAA", func(t *testing.T) {
		policy := &smtpCountingSecurityPolicy{
			stats:  DefaultSecurityPolicyStatsCollector(),
			result: SecurityPolicyResultIncident,
		}
		remote := newSmtpIntegrationBasicClient(t, policy, func(
			_ TransferPath,
			_ protocol.ProvideMode,
			_ *IpPath,
			_ []byte,
		) {
		})

		packet := craftSecurityPacket(
			IpProtocolTcp,
			net.ParseIP("10.0.0.2"),
			48006,
			net.ParseIP("203.0.113.10"),
			smtpLocalPort,
			true,
			nil,
		)
		if !remote.SendPacket(SourceId(NewId()), protocol.ProvideMode_Network, packet, 0) {
			t.Fatal("basic client did not accept TCP/25 on the explicit local route")
		}
		if calls := policy.egressCalls.Load(); calls != 0 {
			t.Fatalf("basic client TCP/25 reached CFAA %d times", calls)
		}
	})

	t.Run("plaintext 465 is reset before CFAA", func(t *testing.T) {
		policy := &smtpCountingSecurityPolicy{
			stats:  DefaultSecurityPolicyStatsCollector(),
			result: SecurityPolicyResultAllow,
		}
		var resetCount atomic.Int64
		remote := newSmtpIntegrationBasicClient(t, policy, func(
			_ TransferPath,
			_ protocol.ProvideMode,
			_ *IpPath,
			_ []byte,
		) {
			resetCount.Add(1)
		})

		packet := smtpTestTcp4Packet(
			byte(tcpFlagAck|tcpFlagPsh),
			12600,
			34600,
			[]byte("EHLO plaintext.example\r\n"),
		)
		if remote.SendPacket(SourceId(NewId()), protocol.ProvideMode_Network, packet, 0) {
			t.Fatal("basic client accepted plaintext TCP/465")
		}
		MessagePoolReturn(packet)
		if calls := policy.egressCalls.Load(); calls != 0 {
			t.Fatalf("basic client plaintext TCP/465 reached CFAA %d times", calls)
		}
		if resets := resetCount.Load(); resets != 1 {
			t.Fatalf("basic client plaintext TCP/465 resets = %d, want 1", resets)
		}
	})

	t.Run("587 starttls batch advances in wire order", func(t *testing.T) {
		policy := &smtpCountingSecurityPolicy{
			stats:  DefaultSecurityPolicyStatsCollector(),
			result: SecurityPolicyResultAllow,
		}
		var resetCount atomic.Int64
		remote := newSmtpIntegrationBasicClient(t, policy, func(
			_ TransferPath,
			_ protocol.ProvideMode,
			_ *IpPath,
			_ []byte,
		) {
			resetCount.Add(1)
		})

		sequence := uint32(12700)
		packets := [][]byte{smtpTestTcp4PacketToPort(
			smtpStartTlsPort,
			byte(tcpFlagSyn),
			sequence,
			0,
			nil,
		)}
		sequence += 1
		for _, payload := range [][]byte{
			[]byte("EHLO ios.example\r\n"),
			[]byte("STARTTLS\r\n"),
			smtpTestClientHello,
		} {
			packets = append(packets, smtpTestTcp4PacketToPort(
				smtpStartTlsPort,
				byte(tcpFlagAck|tcpFlagPsh),
				sequence,
				34700,
				payload,
			))
			sequence += uint32(len(payload))
		}

		// This fixture intentionally has no destination, so the remote sends
		// fail after inspection. All segments still must reach the general policy
		// without the SMTP guard resetting the valid STARTTLS flow.
		if accepted := remote.SendPacketBatch(
			SourceId(NewId()),
			protocol.ProvideMode_Network,
			packets,
			0,
		); accepted != 0 {
			t.Fatalf("providerless basic TCP/587 batch accepted %d packets", accepted)
		}
		if calls := policy.egressCalls.Load(); calls != int64(len(packets)) {
			t.Fatalf("valid basic TCP/587 policy calls = %d, want %d", calls, len(packets))
		}
		if resets := resetCount.Load(); resets != 0 {
			t.Fatalf("valid basic TCP/587 received %d SMTP resets", resets)
		}
	})
}

func TestMultiClientSmtpBatchKeepsOrderedPerPacketRejections(t *testing.T) {
	policy := &smtpCountingSecurityPolicy{
		stats:  DefaultSecurityPolicyStatsCollector(),
		result: SecurityPolicyResultAllow,
	}
	var resetCount atomic.Int64
	multi := newSmtpIntegrationMulti(t, policy, func(
		_ TransferPath,
		_ protocol.ProvideMode,
		_ *IpPath,
		_ []byte,
	) {
		resetCount.Add(1)
	})
	defer multi.Close()

	first := smtpTestTcp4Packet(byte(tcpFlagAck|tcpFlagPsh), 13000, 35000, []byte("AUTH "))
	second := smtpTestTcp4Packet(byte(tcpFlagAck|tcpFlagPsh), 13005, 35000, []byte("PLAIN secret\r\n"))
	if accepted := multi.SendPacketBatch(
		SourceId(NewId()),
		protocol.ProvideMode_Network,
		[][]byte{first, second},
		0,
	); accepted != 0 {
		t.Fatalf("plaintext TCP/465 batch accepted %d packets", accepted)
	}
	if calls := policy.egressCalls.Load(); calls != 0 {
		t.Fatalf("rejected SMTP batch reached CFAA/security inspection %d times", calls)
	}
	if got := resetCount.Load(); got != 2 {
		t.Fatalf("plaintext SMTP batch resets = %d, want 2", got)
	}
	if blocked := multi.PacketStats().BlockEgressPacketCount; blocked != 2 {
		t.Fatalf("plaintext SMTP batch block count = %d, want 2", blocked)
	}
}

// The Apple packet tunnel enters through DeviceLocal.SendPacketBatch, then
// UpgradeMux.SendPacketBatch. The mux's exact-flow fast path used to call the
// multi-client's internal sendPacketGroup directly, below the SMTP gate.
func TestUpgradeMuxBatchPreservesSmtpPolicyEntryPoint(t *testing.T) {
	t.Run("plaintext 465 is reset before CFAA", func(t *testing.T) {
		policy := &smtpCountingSecurityPolicy{
			stats:  DefaultSecurityPolicyStatsCollector(),
			result: SecurityPolicyResultAllow,
		}
		var resetCount atomic.Int64
		multi := newSmtpIntegrationMulti(t, policy, func(
			_ TransferPath,
			_ protocol.ProvideMode,
			_ *IpPath,
			_ []byte,
		) {
			resetCount.Add(1)
		})
		defer multi.Close()

		upgradeMux := &UpgradeMux{mux: &IpMux{}}
		upgradeMux.SetUpstreamBatchClient(multi)
		packet := MessagePoolCopy(smtpTestTcp4Packet(
			byte(tcpFlagAck|tcpFlagPsh),
			17000,
			37000,
			[]byte("EHLO plaintext.example\r\n"),
		))
		if accepted := upgradeMux.SendPacketBatch(
			SourceId(NewId()),
			protocol.ProvideMode_Network,
			[][]byte{packet},
			0,
		); accepted != 0 {
			t.Fatalf("plaintext TCP/465 mux batch accepted %d packets", accepted)
		}
		if calls := policy.egressCalls.Load(); calls != 0 {
			t.Fatalf("mux bypassed SMTP and reached CFAA %d times", calls)
		}
		if resets := resetCount.Load(); resets != 1 {
			t.Fatalf("plaintext TCP/465 mux resets = %d, want 1", resets)
		}
	})

	t.Run("plaintext 587 auth is reset before CFAA", func(t *testing.T) {
		policy := &smtpCountingSecurityPolicy{
			stats:  DefaultSecurityPolicyStatsCollector(),
			result: SecurityPolicyResultAllow,
		}
		var resetCount atomic.Int64
		multi := newSmtpIntegrationMulti(t, policy, func(
			_ TransferPath,
			_ protocol.ProvideMode,
			_ *IpPath,
			_ []byte,
		) {
			resetCount.Add(1)
		})
		defer multi.Close()

		upgradeMux := &UpgradeMux{mux: &IpMux{}}
		upgradeMux.SetUpstreamBatchClient(multi)
		packet := MessagePoolCopy(smtpTestTcp4PacketToPort(
			smtpStartTlsPort,
			byte(tcpFlagAck|tcpFlagPsh),
			17500,
			37500,
			[]byte("AUTH PLAIN secret\r\n"),
		))
		if accepted := upgradeMux.SendPacketBatch(
			SourceId(NewId()),
			protocol.ProvideMode_Network,
			[][]byte{packet},
			0,
		); accepted != 0 {
			t.Fatalf("plaintext TCP/587 mux batch accepted %d packets", accepted)
		}
		if calls := policy.egressCalls.Load(); calls != 0 {
			t.Fatalf("mux bypassed SMTP and reached CFAA %d times", calls)
		}
		if resets := resetCount.Load(); resets != 1 {
			t.Fatalf("plaintext TCP/587 mux resets = %d, want 1", resets)
		}
	})

	t.Run("587 starttls batch advances in wire order", func(t *testing.T) {
		policy := &smtpCountingSecurityPolicy{
			stats:  DefaultSecurityPolicyStatsCollector(),
			result: SecurityPolicyResultAllow,
		}
		var resetCount atomic.Int64
		multi := newSmtpIntegrationMulti(t, policy, func(
			_ TransferPath,
			_ protocol.ProvideMode,
			_ *IpPath,
			_ []byte,
		) {
			resetCount.Add(1)
		})
		defer multi.Close()

		upgradeMux := &UpgradeMux{mux: &IpMux{}}
		upgradeMux.SetUpstreamBatchClient(multi)
		sequence := uint32(17600)
		packets := [][]byte{MessagePoolCopy(smtpTestTcp4PacketToPort(
			smtpStartTlsPort,
			byte(tcpFlagSyn),
			sequence,
			0,
			nil,
		))}
		sequence += 1
		for _, payload := range [][]byte{
			[]byte("EHLO ios.example\r\n"),
			[]byte("STARTTLS\r\n"),
			smtpTestClientHello,
		} {
			packets = append(packets, MessagePoolCopy(smtpTestTcp4PacketToPort(
				smtpStartTlsPort,
				byte(tcpFlagAck|tcpFlagPsh),
				sequence,
				37600,
				payload,
			)))
			sequence += uint32(len(payload))
		}

		// This fixture has no remote provider, so the sends ultimately return
		// false. Reaching the general policy for every segment without a reset is
		// the assertion: the Apple batch entry point preserved SMTP stream order.
		if accepted := upgradeMux.SendPacketBatch(
			SourceId(NewId()),
			protocol.ProvideMode_Network,
			packets,
			0,
		); accepted != 0 {
			t.Fatalf("providerless TCP/587 mux batch accepted %d packets", accepted)
		}
		if calls := policy.egressCalls.Load(); calls != int64(len(packets)) {
			t.Fatalf("valid TCP/587 mux policy calls = %d, want %d", calls, len(packets))
		}
		if resets := resetCount.Load(); resets != 0 {
			t.Fatalf("valid TCP/587 mux received %d SMTP resets", resets)
		}
	})

	t.Run("port 25 remains forced local", func(t *testing.T) {
		policy := &smtpCountingSecurityPolicy{
			stats:  DefaultSecurityPolicyStatsCollector(),
			result: SecurityPolicyResultIncident,
		}
		multi := newSmtpIntegrationMulti(t, policy, func(
			_ TransferPath,
			_ protocol.ProvideMode,
			_ *IpPath,
			_ []byte,
		) {
		})
		defer multi.Close()

		upgradeMux := &UpgradeMux{mux: &IpMux{}}
		upgradeMux.SetUpstreamBatchClient(multi)
		packet := MessagePoolCopy(craftSecurityPacket(
			IpProtocolTcp,
			net.ParseIP("10.0.0.2"),
			48003,
			net.ParseIP("203.0.113.10"),
			smtpLocalPort,
			true,
			nil,
		))
		if accepted := upgradeMux.SendPacketBatch(
			SourceId(NewId()),
			protocol.ProvideMode_Network,
			[][]byte{packet},
			0,
		); accepted != 1 {
			t.Fatalf("TCP/25 mux batch accepted %d packets, want 1", accepted)
		}
		if calls := policy.egressCalls.Load(); calls != 0 {
			t.Fatalf("TCP/25 mux batch reached CFAA %d times", calls)
		}
		stats := multi.PacketStats()
		if stats.LocalEgressPacketCount != 1 || stats.RemoteEgressPacketCount != 0 {
			t.Fatalf(
				"TCP/25 mux route local=%d remote=%d, want 1/0",
				stats.LocalEgressPacketCount,
				stats.RemoteEgressPacketCount,
			)
		}
	})
}

func newSmtpIntegrationProvider(
	t *testing.T,
	policy *smtpCountingSecurityPolicy,
) *RemoteUserNatProvider {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	clientSettings := DefaultClientSettings()
	clientSettings.SendBufferSettings.SequenceBufferSize = 0
	clientSettings.SendBufferSettings.AckBufferSize = 0
	clientSettings.ReceiveBufferSettings.SequenceBufferSize = 0
	clientSettings.ForwardBufferSettings.SequenceBufferSize = 0
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), clientSettings)
	localUserNat := NewLocalUserNatWithDefaults(ctx, "smtp-provider-test")
	settings := DefaultRemoteUserNatProviderSettings()
	if policy != nil {
		settings.SecurityPolicyGenerator = func(
			_ context.Context,
			_ *SecurityPolicyStatsCollector,
		) SecurityPolicy {
			return Reverse(policy)
		}
	}
	provider := NewRemoteUserNatProvider(client, localUserNat, settings)
	t.Cleanup(func() {
		provider.Close()
		localUserNat.Close()
		client.Cancel()
		cancel()
	})
	return provider
}

func sendSmtpProviderPacket(
	t *testing.T,
	provider *RemoteUserNatProvider,
	source TransferPath,
	packet []byte,
) {
	t.Helper()
	pooledPacket := MessagePoolCopy(packet)
	frame, err := ipPacketToProviderFrame(pooledPacket, DefaultProtocolVersion)
	if err != nil {
		MessagePoolReturn(pooledPacket)
		t.Fatal(err)
	}
	provider.ClientReceive(
		source,
		[]*protocol.Frame{frame},
		Peer{ProvideMode: protocol.ProvideMode_Public},
	)
	// ClientReceive borrows each frame. Any accepted asynchronous path took a
	// read-only share before returning.
	MessagePoolReturn(pooledPacket)
}

func requireSmtpProviderNatPacket(
	t *testing.T,
	localUserNat *LocalUserNat,
	want []byte,
) {
	t.Helper()
	select {
	case queued := <-localUserNat.sendPackets:
		defer queued.finish()
		if len(queued.packets) != 1 {
			for _, packet := range queued.packets {
				MessagePoolReturn(packet)
			}
			t.Fatalf("provider NAT packet count = %d, want 1", len(queued.packets))
		}
		got := queued.packets[0]
		matches := bytes.Equal(got, want)
		MessagePoolReturn(got)
		if !matches {
			t.Fatal("provider NAT queue changed the accepted SMTP segment")
		}
	default:
		t.Fatal("provider did not enqueue the accepted SMTP segment into NAT")
	}
}

func requireNoSmtpProviderNatPacket(t *testing.T, localUserNat *LocalUserNat) {
	t.Helper()
	select {
	case queued := <-localUserNat.sendPackets:
		defer queued.finish()
		for _, packet := range queued.packets {
			MessagePoolReturn(packet)
		}
		t.Fatal("provider enqueued a rejected SMTP segment into NAT")
	default:
	}
}

// The provider runs its own Reverse(DefaultSecurityPolicy), independently of
// the client policy. This regression uses that real composition: TCP/587 must
// survive CFAA while the provider SMTP guard advances through STARTTLS.
func TestProviderDefaultReversedPolicyAllowsImplicitTls465(t *testing.T) {
	provider, _, localUserNat := newProviderTransferKeyTestFixture(t)
	segment := smtpTestTcp4Packet(
		byte(tcpFlagAck|tcpFlagPsh),
		19400,
		39400,
		smtpTestClientHello,
	)
	sendSmtpProviderPacket(t, provider, SourceId(NewId()), segment)
	requireSmtpProviderNatPacket(t, localUserNat, segment)

	stats := provider.PacketStats()
	if stats.RemoteIngressPacketCount != 1 || stats.BlockIngressPacketCount != 0 {
		t.Fatalf(
			"default provider TCP/465 ingress remote=%d blocked=%d, want 1/0",
			stats.RemoteIngressPacketCount,
			stats.BlockIngressPacketCount,
		)
	}
}

func TestProviderDefaultReversedPolicyAllows587OnlyThroughStartTls(t *testing.T) {
	provider, _, localUserNat := newProviderTransferKeyTestFixture(t)
	source := SourceId(NewId())
	sequence := uint32(19500)
	segments := [][]byte{
		smtpTestTcp4PacketToPort(smtpStartTlsPort, byte(tcpFlagSyn), sequence, 0, nil),
	}
	sequence += 1
	for _, payload := range [][]byte{
		[]byte("EHLO ios.example\r\n"),
		[]byte("STARTTLS\r\n"),
		smtpTestClientHello,
	} {
		segments = append(segments, smtpTestTcp4PacketToPort(
			smtpStartTlsPort,
			byte(tcpFlagAck|tcpFlagPsh),
			sequence,
			39500,
			payload,
		))
		sequence += uint32(len(payload))
	}

	for _, segment := range segments {
		sendSmtpProviderPacket(t, provider, source, segment)
		requireSmtpProviderNatPacket(t, localUserNat, segment)
	}
	stats := provider.PacketStats()
	if stats.RemoteIngressPacketCount != int64(len(segments)) || stats.BlockIngressPacketCount != 0 {
		t.Fatalf(
			"default provider TCP/587 ingress remote=%d blocked=%d, want %d/0",
			stats.RemoteIngressPacketCount,
			stats.BlockIngressPacketCount,
			len(segments),
		)
	}

	// An independent flow reaching the same real policy cannot issue AUTH before
	// STARTTLS, and the rejected packet must never cross into the local NAT.
	insecure := craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("10.0.0.2"),
		48007,
		net.ParseIP("203.0.113.10"),
		smtpStartTlsPort,
		false,
		[]byte("AUTH PLAIN secret\r\n"),
	)
	sendSmtpProviderPacket(t, provider, source, insecure)
	requireNoSmtpProviderNatPacket(t, localUserNat)
	stats = provider.PacketStats()
	if stats.RemoteIngressPacketCount != int64(len(segments)) || stats.BlockIngressPacketCount != 1 {
		t.Fatalf(
			"default provider insecure TCP/587 ingress remote=%d blocked=%d, want %d/1",
			stats.RemoteIngressPacketCount,
			stats.BlockIngressPacketCount,
			len(segments),
		)
	}
}

// A provider-side SMTP rejection must make a concrete RST trip back through
// the provider return sender; block accounting alone would miss a silent client
// timeout caused by a reset that was synthesized but never delivered.
func TestProviderSmtpRejectionReturnsTcpReset(t *testing.T) {
	provider, client, localUserNat := newProviderTransferKeyTestFixture(t)
	peerId := NewId()
	sequence := installProviderReturnTestSequence(t, provider, client, sendSequenceId{
		Destination:       peerId,
		CompanionContract: true,
	})

	const resetSequence = uint32(40500)
	rejected := smtpTestTcp4Packet(
		byte(tcpFlagAck|tcpFlagPsh),
		20000,
		resetSequence,
		[]byte("EHLO plaintext.example\r\n"),
	)
	sendSmtpProviderPacket(t, provider, SourceId(peerId), rejected)
	requireNoSmtpProviderNatPacket(t, localUserNat)

	queued := waitProviderReturnTestPack(t, sequence)
	defer queued.returnFrames()
	reset := providerCallbackQueuePacketBytes(t, queued)
	_, sourceIp, destinationIp, transport, ok := parseIpv4(reset)
	var tcp parsedTcp
	if !ok || !parseTcpPacket(sourceIp, destinationIp, transport, &tcp) {
		t.Fatal("provider SMTP rejection did not return valid IPv4/TCP")
	}
	if !tcp.rst || tcp.ack || tcp.seq != resetSequence {
		t.Fatalf(
			"provider SMTP reset flags/sequence = rst:%t ack:%t seq:%d, want true/false/%d",
			tcp.rst,
			tcp.ack,
			tcp.seq,
			resetSequence,
		)
	}
	if !sourceIp.Equal(net.IPv4(203, 0, 113, 10)) ||
		!destinationIp.Equal(net.IPv4(10, 0, 0, 2)) ||
		tcp.sourcePort != smtpImplicitTlsPort || tcp.destinationPort != 47001 {
		t.Fatalf(
			"provider SMTP reset endpoints = %s:%d -> %s:%d",
			sourceIp,
			tcp.sourcePort,
			destinationIp,
			tcp.destinationPort,
		)
	}
	if queued.Destination != peerId || !queued.CompanionContract {
		t.Fatalf(
			"provider SMTP reset lane destination=%s companion=%t, want %s/true",
			queued.Destination,
			queued.CompanionContract,
			peerId,
		)
	}
	stats := provider.PacketStats()
	if stats.BlockIngressPacketCount != 1 || stats.RemoteIngressPacketCount != 0 {
		t.Fatalf(
			"provider SMTP reset ingress blocked=%d remote=%d, want 1/0",
			stats.BlockIngressPacketCount,
			stats.RemoteIngressPacketCount,
		)
	}
}

func TestProviderEnforcesSmtpEncryptionBeforeReversedPolicy(t *testing.T) {
	policy := &smtpCountingSecurityPolicy{
		stats:  DefaultSecurityPolicyStatsCollector(),
		result: SecurityPolicyResultAllow,
	}
	provider := newSmtpIntegrationProvider(t, policy)
	source := SourceId(NewId())

	sendSmtpProviderPacket(t, provider, source, smtpTestTcp4Packet(
		byte(tcpFlagAck|tcpFlagPsh),
		18000,
		38000,
		[]byte("EHLO plaintext.example\r\n"),
	))
	if calls := policy.egressCalls.Load(); calls != 0 {
		t.Fatalf("provider plaintext TCP/465 reached reversed policy %d times", calls)
	}
	stats := provider.PacketStats()
	if stats.BlockIngressPacketCount != 1 || stats.RemoteIngressPacketCount != 0 {
		t.Fatalf(
			"provider plaintext TCP/465 ingress blocked=%d remote=%d, want 1/0",
			stats.BlockIngressPacketCount,
			stats.RemoteIngressPacketCount,
		)
	}

	// A second authenticated source can reuse the exact tunnel tuple and still
	// establish implicit TLS; the provider guard is source-namespaced.
	sendSmtpProviderPacket(t, provider, SourceId(NewId()), smtpTestTcp4Packet(
		byte(tcpFlagAck|tcpFlagPsh),
		18000,
		38000,
		smtpTestClientHello,
	))
	if calls := policy.egressCalls.Load(); calls != 1 {
		t.Fatalf("provider valid TCP/465 reversed-policy calls = %d, want 1", calls)
	}
	stats = provider.PacketStats()
	if stats.RemoteIngressPacketCount != 1 {
		t.Fatalf("provider valid TCP/465 remote ingress = %d, want 1", stats.RemoteIngressPacketCount)
	}
}

func TestProviderAllows587NegotiationOnlyThroughTls(t *testing.T) {
	policy := &smtpCountingSecurityPolicy{
		stats:  DefaultSecurityPolicyStatsCollector(),
		result: SecurityPolicyResultAllow,
	}
	provider := newSmtpIntegrationProvider(t, policy)
	source := SourceId(NewId())
	sequence := uint32(19000)
	segments := [][]byte{
		smtpTestTcp4PacketToPort(smtpStartTlsPort, byte(tcpFlagSyn), sequence, 0, nil),
	}
	sequence += 1
	for _, payload := range [][]byte{
		[]byte("EHLO ios.example\r\n"),
		[]byte("STARTTLS\r\n"),
		smtpTestClientHello,
	} {
		segments = append(segments, smtpTestTcp4PacketToPort(
			smtpStartTlsPort,
			byte(tcpFlagAck|tcpFlagPsh),
			sequence,
			39000,
			payload,
		))
		sequence += uint32(len(payload))
	}
	for _, segment := range segments {
		sendSmtpProviderPacket(t, provider, source, segment)
	}
	if calls := policy.egressCalls.Load(); calls != int64(len(segments)) {
		t.Fatalf("provider valid TCP/587 policy calls = %d, want %d", calls, len(segments))
	}
	stats := provider.PacketStats()
	if stats.BlockIngressPacketCount != 0 || stats.RemoteIngressPacketCount != int64(len(segments)) {
		t.Fatalf(
			"provider valid TCP/587 ingress blocked=%d remote=%d, want 0/%d",
			stats.BlockIngressPacketCount,
			stats.RemoteIngressPacketCount,
			len(segments),
		)
	}

	// A different flow cannot authenticate before STARTTLS.
	sendSmtpProviderPacket(t, provider, source, craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("10.0.0.2"),
		48004,
		net.ParseIP("203.0.113.10"),
		smtpStartTlsPort,
		false,
		[]byte("AUTH PLAIN secret\r\n"),
	))
	if calls := policy.egressCalls.Load(); calls != int64(len(segments)) {
		t.Fatalf("provider plaintext TCP/587 reached reversed policy %d times", calls)
	}
	if blocked := provider.PacketStats().BlockIngressPacketCount; blocked != 1 {
		t.Fatalf("provider plaintext TCP/587 blocked ingress = %d, want 1", blocked)
	}
}

func TestProviderRefusesTunneledPort25(t *testing.T) {
	provider := newSmtpIntegrationProvider(t, nil)
	sendSmtpProviderPacket(t, provider, SourceId(NewId()), craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("10.0.0.2"),
		48005,
		net.ParseIP("203.0.113.10"),
		smtpLocalPort,
		true,
		nil,
	))
	stats := provider.PacketStats()
	if stats.BlockIngressPacketCount != 1 || stats.RemoteIngressPacketCount != 0 {
		t.Fatalf(
			"provider tunneled TCP/25 ingress blocked=%d remote=%d, want 1/0",
			stats.BlockIngressPacketCount,
			stats.RemoteIngressPacketCount,
		)
	}
}

func TestMultiClientEncryptedSmtpContinuesToGeneralPolicy(t *testing.T) {
	policy := &smtpCountingSecurityPolicy{
		stats:  DefaultSecurityPolicyStatsCollector(),
		result: SecurityPolicyResultIncident,
	}
	var resetCount atomic.Int64
	multi := newSmtpIntegrationMulti(t, policy, func(
		_ TransferPath,
		_ protocol.ProvideMode,
		_ *IpPath,
		_ []byte,
	) {
		resetCount.Add(1)
	})
	defer multi.Close()

	packet := smtpTestTcp4Packet(byte(tcpFlagAck|tcpFlagPsh), 14000, 36000, smtpTestClientHello)
	if multi.SendPacket(SourceId(NewId()), protocol.ProvideMode_Network, packet, 0) {
		t.Fatal("incident policy unexpectedly accepted encrypted SMTP")
	}
	MessagePoolReturn(packet)
	if calls := policy.egressCalls.Load(); calls != 1 {
		t.Fatalf("valid TLS SMTP reached general policy %d times, want 1", calls)
	}
	if got := resetCount.Load(); got != 0 {
		t.Fatalf("valid TLS SMTP received %d SMTP-policy resets", got)
	}
}

func TestSmtpAllowedTrafficPassesDefaultGeneralPolicy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := DefaultSecurityPolicy(ctx)

	assertAllowed := func(testingT *testing.T, path *IpPath, payload []byte) {
		testingT.Helper()
		path.DestinationIp = net.ParseIP("8.8.8.8")
		result, err := policy.InspectEgress(protocol.ProvideMode_Public, path, payload)
		if err != nil {
			testingT.Fatalf("default SMTP security policy failed: %v", err)
		}
		if result != SecurityPolicyResultAllow {
			testingT.Fatalf("default SMTP security result = %v, want allow", result)
		}
		policy.RefreshEgress(path)
	}

	assertAllowed(t, smtpTestPath(49001, smtpImplicitTlsPort, 15000), smtpTestClientHello)

	sequence := uint32(16000)
	for _, segment := range []struct {
		name    string
		payload []byte
	}{
		{name: "EHLO", payload: []byte("EHLO client.example\r\n")},
		{name: "STARTTLS", payload: []byte("STARTTLS\r\n")},
		{name: "ClientHello", payload: smtpTestClientHello},
	} {
		t.Run(segment.name, func(t *testing.T) {
			assertAllowed(t, smtpTestPath(49002, smtpStartTlsPort, sequence), segment.payload)
		})
		sequence += uint32(len(segment.payload))
	}
}

func TestSmtpReturnPortsPassDefaultClientAndProviderPolicies(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clientPolicy := DefaultSecurityPolicy(ctx)
	providerPolicy := DefaultProviderSecurityPolicy(ctx)

	for _, port := range []int{smtpImplicitTlsPort, smtpStartTlsPort} {
		t.Run(tcpPortName(port), func(t *testing.T) {
			returnPath := &IpPath{
				Version:         4,
				Protocol:        IpProtocolTcp,
				SourceIp:        net.ParseIP("203.0.113.10"),
				SourcePort:      port,
				DestinationIp:   net.ParseIP("10.0.0.2"),
				DestinationPort: 47001,
			}
			clientResult, err := clientPolicy.InspectIngress(
				protocol.ProvideMode_Public,
				returnPath,
				nil,
			)
			if err != nil || clientResult != SecurityPolicyResultAllow {
				t.Fatalf("client SMTP return policy result=%v err=%v, want allow", clientResult, err)
			}
			providerResult, err := providerPolicy.InspectEgress(
				protocol.ProvideMode_Public,
				returnPath,
				nil,
			)
			if err != nil || providerResult != SecurityPolicyResultAllow {
				t.Fatalf("provider SMTP return policy result=%v err=%v, want allow", providerResult, err)
			}
		})
	}
}

func tcpPortName(port int) string {
	if port == smtpImplicitTlsPort {
		return "465"
	}
	return "587"
}
