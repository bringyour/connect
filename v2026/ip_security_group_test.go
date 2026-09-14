// Packet-group security tests pin the one-decision contract while proving
// that stateful payload inspection still consumes every member in order.
package connect

import (
	"context"
	"net"
	"slices"
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Records the public compatibility fallback without implementing the
// built-in borrowed group interface.
type securityGroupFallbackPolicy struct {
	stats          *SecurityPolicyStatsCollector
	payloadMarkers []byte
	refreshCount   int
}

func (self *securityGroupFallbackPolicy) Stats() *SecurityPolicyStatsCollector {
	return self.stats
}

func (self *securityGroupFallbackPolicy) InspectEgress(
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	if len(payload) == 0 {
		self.payloadMarkers = append(self.payloadMarkers, 0)
		return SecurityPolicyResultAllow, nil
	}
	self.payloadMarkers = append(self.payloadMarkers, payload[0])
	switch payload[0] {
	case 2:
		return SecurityPolicyResultDrop, nil
	case 3:
		return SecurityPolicyResultIncident, nil
	default:
		return SecurityPolicyResultAllow, nil
	}
}

func (self *securityGroupFallbackPolicy) InspectIngress(
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	return SecurityPolicyResultAllow, nil
}

func (self *securityGroupFallbackPolicy) RefreshEgress(ipPath *IpPath) {
	self.refreshCount += 1
}

func (self *securityGroupFallbackPolicy) RefreshIngress(ipPath *IpPath) {
}

func securityGroupTestPaths(packetCount int) []IpPath {
	paths := make([]IpPath, packetCount)
	for packetIndex := range paths {
		paths[packetIndex] = IpPath{
			Version:         4,
			Protocol:        IpProtocolTcp,
			SourceIp:        net.IPv4(10, 0, 0, 2),
			SourcePort:      42001,
			DestinationIp:   net.IPv4(8, 8, 8, 8),
			DestinationPort: 51413,
		}
	}
	return paths
}

func TestSecurityPolicyGroupFallbackInspectsInOrderAndRefreshesOnce(t *testing.T) {
	policy := &securityGroupFallbackPolicy{
		stats: DefaultSecurityPolicyStatsCollector(),
	}
	ipPaths := securityGroupTestPaths(3)
	payloads := [][]byte{{1}, {2}, {3}}

	result, err := inspectAndRefreshEgressGroupBorrowed(
		policy,
		protocol.ProvideMode_Public,
		ipPaths,
		payloads,
	)
	if err != nil {
		t.Fatalf("inspect group: %v", err)
	}
	if result != SecurityPolicyResultIncident {
		t.Fatalf("group result = %v, want incident", result)
	}
	if !slices.Equal(policy.payloadMarkers, []byte{1, 2, 3}) {
		t.Fatalf("payload inspection order = %v, want [1 2 3]", policy.payloadMarkers)
	}
	if policy.refreshCount != 1 {
		t.Fatalf("flow refresh count = %d, want 1", policy.refreshCount)
	}
}

func TestSecurityPolicyGroupBuiltInDetectsLaterPayload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stats := DefaultSecurityPolicyStatsCollector()
	policy := DefaultSecurityPolicyWithStats(ctx, stats)
	ipPaths := securityGroupTestPaths(2)
	ipPaths[0].Syn = true
	payloads := [][]byte{nil, btHandshake()}

	result, err := inspectAndRefreshEgressGroupBorrowed(
		policy,
		protocol.ProvideMode_Public,
		ipPaths,
		payloads,
	)
	if err != nil {
		t.Fatalf("inspect group: %v", err)
	}
	if result != SecurityPolicyResultIncident {
		t.Fatalf("group result = %v, want later BitTorrent payload incident", result)
	}
	destination := newSecurityDestinationPort(&ipPaths[0])
	if count := stats.Stats(false)[SecurityPolicyResultIncident][destination]; count != 2 {
		t.Fatalf("incident packet count = %d, want whole group count 2", count)
	}
}

func TestSecurityPolicyGroupRejectsMismatchedMetadata(t *testing.T) {
	policy := DisableSecurityPolicy()
	result, err := inspectAndRefreshEgressGroupBorrowed(
		policy,
		protocol.ProvideMode_Public,
		securityGroupTestPaths(1),
		nil,
	)
	if err == nil {
		t.Fatal("mismatched metadata was accepted")
	}
	if result != SecurityPolicyResultIncident {
		t.Fatalf("invalid group result = %v, want incident", result)
	}
}
