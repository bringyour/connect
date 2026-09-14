package connect

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// egressRelationship combines a packet source's provide mode with the local
// client's own provide mode into the relationship the security policy enforces on
// egress. ProvideMode is a set of flags, not an ordered scale, so the result must
// be decided per-case: only a genuine same-Network relationship on BOTH sides may
// reach non-public destinations; anything else (including an unspecified None)
// must fall under the public rules.
func TestEgressRelationship(t *testing.T) {
	cases := []struct {
		source protocol.ProvideMode
		client protocol.ProvideMode
		want   protocol.ProvideMode
	}{
		// both Network -> Network (the only combination that may reach a LAN)
		{protocol.ProvideMode_Network, protocol.ProvideMode_Network, protocol.ProvideMode_Network},

		// the hosted proxy: its multiclient is ProvideMode_Public, so even the
		// hard-coded ProvideMode_Network egress source must resolve to Public
		{protocol.ProvideMode_Network, protocol.ProvideMode_Public, protocol.ProvideMode_Public},

		// anything other than "both Network" -> Public
		{protocol.ProvideMode_Public, protocol.ProvideMode_Network, protocol.ProvideMode_Public},
		{protocol.ProvideMode_None, protocol.ProvideMode_Network, protocol.ProvideMode_Public},
		{protocol.ProvideMode_Network, protocol.ProvideMode_None, protocol.ProvideMode_Public},
		{protocol.ProvideMode_None, protocol.ProvideMode_None, protocol.ProvideMode_Public},
		{protocol.ProvideMode_Public, protocol.ProvideMode_Public, protocol.ProvideMode_Public},
		{protocol.ProvideMode_FriendsAndFamily, protocol.ProvideMode_Network, protocol.ProvideMode_Public},
		{protocol.ProvideMode_Stream, protocol.ProvideMode_Network, protocol.ProvideMode_Public},
		{protocol.ProvideMode_Network, protocol.ProvideMode_Stream, protocol.ProvideMode_Public},
	}
	for _, c := range cases {
		if got := egressRelationship(c.source, c.client); got != c.want {
			t.Errorf("egressRelationship(%v, %v) = %v, want %v", c.source, c.client, got, c.want)
		}
	}
}

// testEgressPath is a tcp syn from a private tunnel-side source of the family
// to dstIp:443.
func testEgressPath(ipVersion int, dstIp string) *IpPath {
	sourceIp := net.ParseIP("10.0.0.2")
	if ipVersion == 6 {
		sourceIp = net.ParseIP("fd00:7572:6e65::2")
	}
	return &IpPath{
		Version:         ipVersion,
		Protocol:        IpProtocolTcp,
		SourceIp:        sourceIp,
		SourcePort:      12345,
		DestinationIp:   net.ParseIP(dstIp),
		DestinationPort: 443,
		Syn:             true,
	}
}

// testNonPublicDestinations are the non-public destination classes the
// public rules must refuse, in the family's own forms.
func testNonPublicDestinations(ipVersion int) []string {
	if ipVersion == 4 {
		return []string{
			"10.0.0.5",        // RFC1918
			"192.168.1.10",    // RFC1918
			"127.0.0.1",       // loopback
			"169.254.169.254", // link-local: the cloud metadata endpoint
		}
	}
	return []string{
		"fd00::5",       // ULA (RFC 4193)
		"fc00:1::10",    // ULA
		"::1",           // loopback
		"fe80::1",       // link-local
		"fd00:ec2::254", // the cloud metadata endpoint's v6 identity (ULA)
	}
}

// testMetadataEndpoint is the cloud metadata endpoint of the family.
func testMetadataEndpoint(ipVersion int) string {
	if ipVersion == 4 {
		return "169.254.169.254"
	}
	return "fd00:ec2::254"
}

// testLanDestination is a same-network destination of the family.
func testLanDestination(ipVersion int) string {
	if ipVersion == 4 {
		return "10.0.0.5"
	}
	return "fd00::5"
}

// testPublicResolver is a public unicast destination of the family.
func testPublicResolver(ipVersion int) string {
	if ipVersion == 4 {
		return "8.8.8.8"
	}
	return "2001:4860:4860::8888"
}

// The security guarantee the proxy relies on: a same-Network relationship
// bypasses the public rules (may reach a LAN), while any other relationship
// enforces isPublicUnicast — so a private / loopback / link-local (incl. the
// cloud metadata endpoint) destination is an Incident, which blockActionApply
// treats as a non-overridable block. Both families: the v6 forms are ULA,
// loopback and link-local.
func TestInspectEgressNetworkBypassAndPublicEnforcement(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		policy := DefaultSecurityPolicy(ctx)

		nonPublic := testNonPublicDestinations(ipVersion)

		// ProvideMode_Network: trusted same-network relationship reaches the LAN
		for _, ip := range nonPublic {
			r, err := policy.InspectEgress(protocol.ProvideMode_Network, testEgressPath(ipVersion, ip), nil)
			if err != nil {
				t.Fatalf("InspectEgress(Network, %s): unexpected error %v", ip, err)
			}
			if r != SecurityPolicyResultAllow {
				t.Errorf("InspectEgress(Network, %s) = %v, want Allow (same-network bypass)", ip, r)
			}
		}

		// ProvideMode_Public: the same LAN destinations are blocked as incidents
		for _, ip := range nonPublic {
			r, err := policy.InspectEgress(protocol.ProvideMode_Public, testEgressPath(ipVersion, ip), nil)
			if err != nil {
				t.Fatalf("InspectEgress(Public, %s): unexpected error %v", ip, err)
			}
			if r != SecurityPolicyResultIncident {
				t.Errorf("InspectEgress(Public, %s) = %v, want Incident (isPublicUnicast enforced)", ip, r)
			}
		}

		// a public destination under Public passes isPublicUnicast (not an incident)
		publicIp := testPublicResolver(ipVersion)
		if r, err := policy.InspectEgress(protocol.ProvideMode_Public, testEgressPath(ipVersion, publicIp), nil); err != nil {
			t.Fatalf("InspectEgress(Public, %s): unexpected error %v", publicIp, err)
		} else if r == SecurityPolicyResultIncident {
			t.Errorf("InspectEgress(Public, %s) = Incident, want a public unicast destination to pass", publicIp)
		}
	})
}

// Composition as the proxy experiences it: the DeviceLocal egress hard-codes a
// ProvideMode_Network source, but the multiclient is ProvideMode_Public, so
// egressRelationship resolves to Public and a LAN destination is blocked. A
// genuine same-Network client (client mode Network) may still reach the LAN.
func TestProxyEgressBlocksLocalDestinations(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		policy := DefaultSecurityPolicy(ctx)

		const hostedClientMode = protocol.ProvideMode_Public // the proxy multiclient
		const egressSource = protocol.ProvideMode_Network    // device_local hard-codes this

		proxyRel := egressRelationship(egressSource, hostedClientMode)
		if r, _ := policy.InspectEgress(proxyRel, testEgressPath(ipVersion, testMetadataEndpoint(ipVersion)), nil); r != SecurityPolicyResultIncident {
			t.Errorf("hosted proxy egress to metadata endpoint = %v, want Incident (blocked)", r)
		}

		sameNetworkRel := egressRelationship(egressSource, protocol.ProvideMode_Network)
		if r, _ := policy.InspectEgress(sameNetworkRel, testEgressPath(ipVersion, testLanDestination(ipVersion)), nil); r != SecurityPolicyResultAllow {
			t.Errorf("same-network client egress to LAN = %v, want Allow", r)
		}
	})
}

// newSourceProvideModeTestProvider creates provider state without a source cap
// so tests can isolate record and return behavior from eviction.
func newSourceProvideModeTestProvider() *RemoteUserNatProvider {
	return &RemoteUserNatProvider{
		settings:          &RemoteUserNatProviderSettings{MaxSourceCount: 0},
		sourceProvideMode: map[Id]protocol.ProvideMode{},
	}
}

// TestSourceReturnProvideModeUsesFallbackForUntrackedSource verifies that an
// unknown source does not inherit another source's relationship.
func TestSourceReturnProvideModeUsesFallbackForUntrackedSource(t *testing.T) {
	provider := newSourceProvideModeTestProvider()
	fallback := protocol.ProvideMode_Stream
	if got := provider.sourceReturnProvideMode(NewId(), fallback); got != fallback {
		t.Errorf("untracked = %v, want fallback %v", got, fallback)
	}
}

// TestRecordSourceProvideModeRecordsFirstMode verifies first-use state.
func TestRecordSourceProvideModeRecordsFirstMode(t *testing.T) {
	provider := newSourceProvideModeTestProvider()
	sourceId := NewId()
	provider.recordSourceProvideMode(sourceId, protocol.ProvideMode_Public)
	if got := provider.sourceReturnProvideMode(sourceId, protocol.ProvideMode_Stream); got != protocol.ProvideMode_Public {
		t.Errorf("first-seen = %v, want Public", got)
	}
}

// TestRecordSourceProvideModePrefersNetwork verifies that a source which has
// used the same-network relationship keeps that stronger return relationship.
func TestRecordSourceProvideModePrefersNetwork(t *testing.T) {
	provider := newSourceProvideModeTestProvider()
	sourceId := NewId()
	provider.recordSourceProvideMode(sourceId, protocol.ProvideMode_Public)
	provider.recordSourceProvideMode(sourceId, protocol.ProvideMode_Network)
	if got := provider.sourceReturnProvideMode(sourceId, protocol.ProvideMode_Stream); got != protocol.ProvideMode_Network {
		t.Errorf("public then network = %v, want Network", got)
	}
}

// TestRecordSourceProvideModeKeepsNetwork verifies that a later public update
// cannot weaken an established same-network return relationship.
func TestRecordSourceProvideModeKeepsNetwork(t *testing.T) {
	provider := newSourceProvideModeTestProvider()
	sourceId := NewId()
	provider.recordSourceProvideMode(sourceId, protocol.ProvideMode_Network)
	provider.recordSourceProvideMode(sourceId, protocol.ProvideMode_Public)
	if got := provider.sourceReturnProvideMode(sourceId, protocol.ProvideMode_Stream); got != protocol.ProvideMode_Network {
		t.Errorf("network then public = %v, want Network", got)
	}
}

// TestRecordSourceProvideModeUpdatesLatestNonNetworkMode verifies that ordinary
// non-network updates follow the source's latest observed mode.
func TestRecordSourceProvideModeUpdatesLatestNonNetworkMode(t *testing.T) {
	provider := newSourceProvideModeTestProvider()
	sourceId := NewId()
	provider.recordSourceProvideMode(sourceId, protocol.ProvideMode_Public)
	provider.recordSourceProvideMode(sourceId, protocol.ProvideMode_Stream)
	if got := provider.sourceReturnProvideMode(sourceId, protocol.ProvideMode_Network); got != protocol.ProvideMode_Stream {
		t.Errorf("public then stream = %v, want Stream", got)
	}
}

// TestRecordSourceProvideModeCapsTrackedSources verifies that relationship
// bookkeeping has a predictable memory bound while retaining the newest source.
func TestRecordSourceProvideModeCapsTrackedSources(t *testing.T) {
	provider := &RemoteUserNatProvider{
		settings:          &RemoteUserNatProviderSettings{MaxSourceCount: 2},
		sourceProvideMode: map[Id]protocol.ProvideMode{},
	}
	sourceId1 := NewId()
	sourceId2 := NewId()
	sourceId3 := NewId()
	provider.recordSourceProvideMode(sourceId1, protocol.ProvideMode_Public)
	provider.recordSourceProvideMode(sourceId2, protocol.ProvideMode_Public)
	// adding a third new source evicts one existing entry to stay at the cap
	provider.recordSourceProvideMode(sourceId3, protocol.ProvideMode_Public)
	if got := len(provider.sourceProvideMode); got != 2 {
		t.Errorf("tracked source count = %d, want 2 (capped)", got)
	}
	// the just-recorded source is retained; an evicted source falls back
	if got := provider.sourceReturnProvideMode(sourceId3, protocol.ProvideMode_Stream); got != protocol.ProvideMode_Public {
		t.Errorf("newest source = %v, want Public", got)
	}
}

// Network-provider return data reproduces the force-stream lane carried by
// the request. Active signaling on that lane must share the same sender
// sequence, and the Pack lane discriminator keeps it separate at the receiver.
func TestProviderReturnTransferOptionPreventsForceStreamFork(t *testing.T) {
	transferKey := providerReplyTransferKey(
		TransferKey{
			ForceStream:    true,
			EncryptionRole: protocol.SequenceRole_SequenceRoleClient,
		},
		protocol.ProvideMode_Network,
	)
	networkOption := providerReturnTransferOptions(
		DefaultTransferOpts(),
		protocol.ProvideMode_Network,
		transferKey,
	)
	if !networkOption.ForceStream ||
		!networkOption.NetworkPeer ||
		networkOption.CompanionContract {
		t.Fatalf("Network return option = %#v, want ForceStream", networkOption)
	}
	publicOption := providerReturnTransferOptions(
		DefaultTransferOpts(),
		protocol.ProvideMode_Public,
		providerReplyTransferKey(TransferKey{}, protocol.ProvideMode_Public),
	)
	if !publicOption.CompanionContract ||
		publicOption.ForceStream ||
		publicOption.NetworkPeer {
		t.Fatalf("Public return option = %#v, want CompanionContract", publicOption)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Cancel()

	destinationId := NewId()
	providerReturn := &protocol.Frame{
		MessageType:  protocol.MessageType_IpIpPacketFromProvider,
		MessageBytes: []byte("provider return"),
	}
	if !client.SendWithTimeout(providerReturn, destinationId, nil, time.Second, networkOption, transferKey) {
		t.Fatal("provider return enqueue failed")
	}
	activeSignal := &protocol.Frame{
		MessageType:  protocol.MessageType_TransferExchangeSignals,
		MessageBytes: []byte("active signal"),
	}
	if !client.SendWithTimeout(activeSignal, destinationId, nil, time.Second, ForceStream()) {
		t.Fatal("active signal enqueue failed")
	}

	client.sendBuffer.mutex.Lock()
	defer client.sendBuffer.mutex.Unlock()
	var keys []sendSequenceId
	for key := range client.sendBuffer.sendSequences {
		if key.Destination == destinationId &&
			key.EncryptionRole == sequenceTlsRoleClient &&
			!key.EncryptionCompanion &&
			!key.CompanionContract {
			keys = append(keys, key)
		}
	}
	if len(keys) != 1 {
		t.Fatalf("provider return and active signal forked into %d sequences: %v", len(keys), keys)
	}
	if !keys[0].ForceStream {
		t.Fatal("shared provider return/signal sequence must use ForceStream")
	}
	if !client.sendBuffer.sendSequences[keys[0]].networkPeer {
		t.Fatal("Network provider return must retain Network contract policy")
	}
}

// Provider-return TCP must retain the bytes the proxy's upstream socket can no
// longer reproduce. UDP and ICMP preserve datagram loss semantics.
func TestProviderReturnIpTransferOptionsMatchRecoveryOwner(t *testing.T) {
	defaultOptions := DefaultTransferOpts()
	tcpDefaultOptions := defaultOptions
	// Recovery ownership is a correctness invariant, not a caller default.
	tcpDefaultOptions.Ack = false
	tests := []struct {
		name            string
		options         TransferOptions
		provideMode     protocol.ProvideMode
		transferKey     TransferKey
		ipProtocol      IpProtocol
		wantAck         bool
		wantForceStream bool
		wantCompanion   bool
	}{
		{
			name:            "direct TCP socket data",
			options:         tcpDefaultOptions,
			provideMode:     protocol.ProvideMode_Network,
			transferKey:     TransferKey{ForceStream: true},
			ipProtocol:      IpProtocolTcp,
			wantAck:         true,
			wantForceStream: true,
		},
		{
			name:            "direct pure TCP ACK",
			options:         defaultOptions,
			provideMode:     protocol.ProvideMode_Network,
			transferKey:     TransferKey{ForceStream: true},
			ipProtocol:      IpProtocolTcp,
			wantAck:         true,
			wantForceStream: true,
		},
		{
			name:            "direct TCP SYN ACK",
			options:         tcpDefaultOptions,
			provideMode:     protocol.ProvideMode_Network,
			transferKey:     TransferKey{ForceStream: true},
			ipProtocol:      IpProtocolTcp,
			wantAck:         true,
			wantForceStream: true,
		},
		{
			name:            "direct TCP FIN",
			options:         tcpDefaultOptions,
			provideMode:     protocol.ProvideMode_Network,
			transferKey:     TransferKey{ForceStream: true},
			ipProtocol:      IpProtocolTcp,
			wantAck:         true,
			wantForceStream: true,
		},
		{
			name:            "direct TCP RST",
			options:         tcpDefaultOptions,
			provideMode:     protocol.ProvideMode_Network,
			transferKey:     TransferKey{ForceStream: true},
			ipProtocol:      IpProtocolTcp,
			wantAck:         true,
			wantForceStream: true,
		},
		{
			name:            "direct UDP",
			options:         defaultOptions,
			provideMode:     protocol.ProvideMode_Public,
			transferKey:     TransferKey{ForceStream: true, CompanionContract: true},
			ipProtocol:      IpProtocolUdp,
			wantForceStream: true,
			wantCompanion:   true,
		},
		{
			name:          "platform pure TCP ACK",
			options:       defaultOptions,
			provideMode:   protocol.ProvideMode_Public,
			transferKey:   TransferKey{CompanionContract: true},
			ipProtocol:    IpProtocolTcp,
			wantAck:       true,
			wantCompanion: true,
		},
		{
			name:          "platform TCP ignores NoAck default",
			options:       tcpDefaultOptions,
			provideMode:   protocol.ProvideMode_Public,
			transferKey:   TransferKey{CompanionContract: true},
			ipProtocol:    IpProtocolTcp,
			wantAck:       true,
			wantCompanion: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := providerReturnIpTransferOptions(
				test.options,
				test.provideMode,
				test.transferKey,
				test.ipProtocol,
			)
			if options.Ack != test.wantAck ||
				options.ForceStream != test.wantForceStream ||
				options.CompanionContract != test.wantCompanion {
				t.Fatalf(
					"options = %#v, want Ack=%t ForceStream=%t CompanionContract=%t",
					options,
					test.wantAck,
					test.wantForceStream,
					test.wantCompanion,
				)
			}
		})
	}
}

// A provider replies to an ephemeral per-window source id, not necessarily the
// top-level peer id remembered by PeerManager. The authenticated ProvideMode is
// therefore the authoritative classification for the first return contract.
func TestProviderReturnNetworkContractDoesNotDependOnDestinationIdentity(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer client.Cancel()

	generatedWindowClientId := NewId()
	transferKey := providerReplyTransferKey(
		TransferKey{
			ForceStream:    true,
			EncryptionRole: protocol.SequenceRole_SequenceRoleClient,
		},
		protocol.ProvideMode_Network,
	)
	options := providerReturnTransferOptions(
		client.settings.DefaultTransferOpts,
		protocol.ProvideMode_Network,
		transferKey,
	)
	frame := &protocol.Frame{
		MessageType:  protocol.MessageType_IpIpPacketFromProvider,
		MessageBytes: []byte("provider return"),
	}
	if !client.SendWithTimeout(
		frame,
		generatedWindowClientId,
		nil,
		time.Second,
		options,
		transferKey,
	) {
		t.Fatal("provider return enqueue failed")
	}

	client.sendBuffer.mutex.Lock()
	var sequence *SendSequence
	for key, candidate := range client.sendBuffer.sendSequences {
		if key.Destination == generatedWindowClientId {
			sequence = candidate
			break
		}
	}
	client.sendBuffer.mutex.Unlock()
	if sequence == nil {
		t.Fatal("provider return did not create a send sequence")
	}
	if !sequence.networkPeer {
		t.Fatal("ephemeral Network destination lost its authenticated contract policy")
	}

	byteCount := client.ContractManager().contractByteCount(
		ContractKey{
			Destination: DestinationId(generatedWindowClientId),
			ForceStream: true,
			NetworkPeer: sequence.networkPeer,
		},
		0,
		0,
	)
	AssertEqual(t, mib(1), byteCount)
}
