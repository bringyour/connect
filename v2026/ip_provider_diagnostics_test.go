package connect

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

func TestSecurityPolicyHashIdentifiesEffectiveRules(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	first := SecurityPolicyHash(DefaultProviderSecurityPolicy(ctx))
	second := SecurityPolicyHash(DefaultProviderSecurityPolicy(ctx))
	disabled := SecurityPolicyHash(DisableSecurityPolicy())
	if first != second {
		t.Fatalf("identical built-in policies hashed differently: %q != %q", first, second)
	}
	if first == disabled {
		t.Fatal("enabled and disabled policies have the same identity")
	}
	decoded, err := hex.DecodeString(first)
	if err != nil || len(decoded) != 32 {
		t.Fatalf("policy identity %q is not one SHA-256 digest: bytes=%d err=%v", first, len(decoded), err)
	}
}

func TestProviderDiagnosticsAreSourceScopedAndGenerationGated(t *testing.T) {
	sourceA := NewId()
	sourceB := NewId()
	provider := &RemoteUserNatProvider{
		buildVersion:       "android-provider-321",
		securityPolicyHash: "policy-abc",
		sourceDiagnostics:  map[Id]*providerSourceDiagnostics{},
	}

	provider.recordProviderBlock(sourceA, true, 2, 1200)
	provider.recordProviderBlock(sourceA, false, 1, 800)

	a := provider.providerDiagnosticsMessage(sourceA)
	b := provider.providerDiagnosticsMessage(sourceB)
	if a.BlockIngressPacketCount != 2 || a.BlockIngressByteCount != 1200 ||
		a.BlockEgressPacketCount != 1 || a.BlockEgressByteCount != 800 {
		t.Fatalf("source A counters = %+v", a)
	}
	if b.BlockIngressPacketCount != 0 || b.BlockEgressPacketCount != 0 {
		t.Fatalf("source B observed source A blocks: %+v", b)
	}
	if a.BuildVersion != "android-provider-321" || a.SecurityPolicyHash != "policy-abc" {
		t.Fatalf("provider identity missing: %+v", a)
	}

	provider.markProviderDiagnosticsPublished(sourceA, a.Sequence)
	if duplicate := provider.providerDiagnosticsMessage(sourceA); duplicate != nil {
		t.Fatalf("unchanged generation republished: %+v", duplicate)
	}
	provider.recordProviderBlock(sourceA, true, 1, 64)
	if changed := provider.providerDiagnosticsMessage(sourceA); changed == nil || changed.Sequence <= a.Sequence {
		t.Fatalf("counter change did not advance publication: old=%d new=%+v", a.Sequence, changed)
	}
}

func TestProviderDiagnosticsFrameAndChannelOrdering(t *testing.T) {
	message := &protocol.IpProviderDiagnostics{
		BuildVersion:            "provider-44",
		SecurityPolicyHash:      "hash-44",
		BlockIngressPacketCount: 7,
		BlockIngressByteCount:   700,
		BlockEgressPacketCount:  3,
		BlockEgressByteCount:    300,
		Sequence:                44,
	}
	frame := RequireToFrameWithDefaultProtocolVersion(message)
	defer MessagePoolReturn(frame.MessageBytes)
	if frame.MessageType != protocol.MessageType_IpIpProviderDiagnostics {
		t.Fatalf("message type = %v", frame.MessageType)
	}
	roundTrip, err := FromFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	if got := roundTrip.(*protocol.IpProviderDiagnostics); got.Sequence != 44 || got.SecurityPolicyHash != "hash-44" {
		t.Fatalf("round trip = %+v", got)
	}

	channel := &multiClientChannel{ctx: context.Background()}
	channel.clientReceive(TransferPath{}, []*protocol.Frame{frame}, Peer{})
	snapshot := channel.providerDiagnosticsSnapshot()
	if snapshot == nil || snapshot.Sequence != 44 || snapshot.BlockIngressPacketCount != 7 {
		t.Fatalf("channel snapshot = %+v", snapshot)
	}

	older := RequireToFrameWithDefaultProtocolVersion(&protocol.IpProviderDiagnostics{
		BuildVersion:       "stale",
		SecurityPolicyHash: "stale",
		Sequence:           43,
	})
	defer MessagePoolReturn(older.MessageBytes)
	channel.clientReceive(TransferPath{}, []*protocol.Frame{older}, Peer{})
	if got := channel.providerDiagnosticsSnapshot(); got.Sequence != 44 || got.BuildVersion != "provider-44" {
		t.Fatalf("older reordered diagnostics replaced current snapshot: %+v", got)
	}

	equal := RequireToFrameWithDefaultProtocolVersion(&protocol.IpProviderDiagnostics{
		BuildVersion:       "same-generation-spoof",
		SecurityPolicyHash: "same-generation-spoof",
		Sequence:           44,
	})
	defer MessagePoolReturn(equal.MessageBytes)
	channel.clientReceive(TransferPath{}, []*protocol.Frame{equal}, Peer{})
	if got := channel.providerDiagnosticsSnapshot(); got.BuildVersion != "provider-44" {
		t.Fatalf("equal-generation diagnostics replaced immutable snapshot: %+v", got)
	}
}
