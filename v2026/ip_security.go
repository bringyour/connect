package connect

import (
	"context"
	// "encoding/binary"
	// "errors"
	"fmt"
	// "io"
	// "math"
	// mathrand "math/rand"
	"net"
	// "slices"
	"strconv"
	// "strings"
	"sync"
	// "time"
	// "net/netip"

	// "github.com/gopacket/gopacket"
	// "github.com/gopacket/gopacket/layers"

	"maps"

	// "google.golang.org/protobuf/proto"

	// "github.com/urnetwork/glog/v2026"

	"github.com/urnetwork/connect/v2026/protocol"
)

type SecurityPolicyResult int

const (
	SecurityPolicyResultDrop     SecurityPolicyResult = 0
	SecurityPolicyResultAllow    SecurityPolicyResult = 1
	SecurityPolicyResultIncident SecurityPolicyResult = 2
)

func (self SecurityPolicyResult) String() string {
	switch self {
	case SecurityPolicyResultDrop:
		return "drop"
	case SecurityPolicyResultAllow:
		return "allow"
	case SecurityPolicyResultIncident:
		return "incident"
	default:
		return "unknown"
	}
}

type SecurityPolicy interface {
	Stats() *SecurityPolicyStatsCollector
	// ipPath, its address slices, and payload are read-only and valid only for
	// the duration of each call. Implementations that retain them must copy.
	// InspectEgress decides the fate of a packet on the send (client->destination)
	// direction. payload is the L4 payload (may be nil for header-only inspection).
	InspectEgress(provideMode protocol.ProvideMode, ipPath *IpPath, payload []byte) (SecurityPolicyResult, error)
	// InspectIngress decides the fate of a packet on the return (destination->client)
	// direction.
	InspectIngress(provideMode protocol.ProvideMode, ipPath *IpPath, payload []byte) (SecurityPolicyResult, error)
	// RefreshEgress and RefreshIngress refresh a tracked flow's DPI activity time from a sent or
	// received packet respectively, so an active flow is not reclaimed by the idle scan (or the
	// capacity-LRU) while traffic still flows in either direction. They make no security decision;
	// call them at every forwarding point alongside (or in place of) the inspection.
	RefreshEgress(ipPath *IpPath)
	RefreshIngress(ipPath *IpPath)
}

// borrowedEgressSecurityPolicy is the built-in, allocation-free counterpart to
// the public pointer API. A dynamic interface call is conservatively allowed to
// retain its pointer argument, so passing a stack IpPath through SecurityPolicy
// forces one heap object per packet even though the documented contract is
// call-scoped. Built-ins accept the path by value; custom policies continue to
// use the compatible public fallback below.
type borrowedEgressSecurityPolicy interface {
	inspectAndRefreshEgressBorrowed(
		provideMode protocol.ProvideMode,
		ipPath IpPath,
		payload []byte,
	) (SecurityPolicyResult, error)
}

// borrowedSenderEgressSecurityPolicy carries the authenticated peer sender id
// into provider-side stateful inspection. Custom policies retain the public
// sender-agnostic fallback.
type borrowedSenderEgressSecurityPolicy interface {
	inspectAndRefreshEgressForSenderBorrowed(
		senderClientId Id,
		provideMode protocol.ProvideMode,
		ipPath IpPath,
		payload []byte,
	) (SecurityPolicyResult, error)
}

// borrowedEgressGroupSecurityPolicy makes one policy decision for an ordered,
// homogeneous directional-flow group. Implementations may inspect every
// payload internally, but endpoint policy, statistics, and activity refresh
// happen once for the group. The paths, their address slices, and the payloads
// are borrowed for the duration of the call.
type borrowedEgressGroupSecurityPolicy interface {
	inspectAndRefreshEgressGroupBorrowed(
		provideMode protocol.ProvideMode,
		ipPaths []IpPath,
		payloads [][]byte,
	) (SecurityPolicyResult, error)
}

func inspectAndRefreshEgressBorrowed(
	policy SecurityPolicy,
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	if borrowed, ok := policy.(borrowedEgressSecurityPolicy); ok {
		return borrowed.inspectAndRefreshEgressBorrowed(provideMode, ipPath, payload)
	}
	return inspectAndRefreshEgressFallback(policy, provideMode, ipPath, payload)
}

func inspectAndRefreshEgressForSenderBorrowed(
	policy SecurityPolicy,
	senderClientId Id,
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	if borrowed, ok := policy.(borrowedSenderEgressSecurityPolicy); ok {
		return borrowed.inspectAndRefreshEgressForSenderBorrowed(
			senderClientId,
			provideMode,
			ipPath,
			payload,
		)
	}
	return inspectAndRefreshEgressBorrowed(policy, provideMode, ipPath, payload)
}

// Makes one conservative decision for a homogeneous packet group. Custom
// policies retain their existing per-packet inspection API, so the fallback
// calls it in order and folds the results before refreshing the flow once.
func inspectAndRefreshEgressGroupBorrowed(
	policy SecurityPolicy,
	provideMode protocol.ProvideMode,
	ipPaths []IpPath,
	payloads [][]byte,
) (SecurityPolicyResult, error) {
	if len(ipPaths) == 0 || len(ipPaths) != len(payloads) {
		return SecurityPolicyResultIncident, fmt.Errorf(
			"invalid security policy group cardinality paths=%d payloads=%d",
			len(ipPaths),
			len(payloads),
		)
	}
	if borrowed, ok := policy.(borrowedEgressGroupSecurityPolicy); ok {
		return borrowed.inspectAndRefreshEgressGroupBorrowed(
			provideMode,
			ipPaths,
			payloads,
		)
	}
	return inspectAndRefreshEgressGroupFallback(
		policy,
		provideMode,
		ipPaths,
		payloads,
	)
}

// Keep custom pointer calls outside the built-in dispatcher so their path
// copies may escape without forcing the built-in group metadata to escape.
//
//go:noinline
func inspectAndRefreshEgressGroupFallback(
	policy SecurityPolicy,
	provideMode protocol.ProvideMode,
	ipPaths []IpPath,
	payloads [][]byte,
) (SecurityPolicyResult, error) {
	groupResult := SecurityPolicyResultAllow
	for packetIndex := range ipPaths {
		ipPath := ipPaths[packetIndex]
		result, err := policy.InspectEgress(
			provideMode,
			&ipPath,
			payloads[packetIndex],
		)
		if err != nil {
			return result, err
		}
		groupResult = conservativeSecurityPolicyResult(groupResult, result)
	}
	refreshPath := ipPaths[0]
	policy.RefreshEgress(&refreshPath)
	return groupResult, nil
}

// Incidents are never overridable, and a drop in any group member prevents
// the group from reaching a provider. Unknown results are incident-class.
func conservativeSecurityPolicyResult(
	groupResult SecurityPolicyResult,
	memberResult SecurityPolicyResult,
) SecurityPolicyResult {
	if groupResult != SecurityPolicyResultAllow &&
		groupResult != SecurityPolicyResultDrop {
		return SecurityPolicyResultIncident
	}
	switch memberResult {
	case SecurityPolicyResultAllow:
		return groupResult
	case SecurityPolicyResultDrop:
		if groupResult == SecurityPolicyResultAllow {
			return SecurityPolicyResultDrop
		}
		return groupResult
	default:
		return SecurityPolicyResultIncident
	}
}

// Keep the address-taking fallback in a separate non-inlined function. Escape
// analysis is flow-insensitive within a function; placing &ipPath in the fast
// dispatcher would allocate its copy even when the built-in value interface
// succeeds.
//
//go:noinline
func inspectAndRefreshEgressFallback(
	policy SecurityPolicy,
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	// An external implementation may retain a pointer despite the interface
	// contract. Let only this fallback copy escape, never the caller's stack
	// path.
	result, err := policy.InspectEgress(provideMode, &ipPath, payload)
	if err == nil {
		policy.RefreshEgress(&ipPath)
	}
	return result, err
}

// borrowedIngressSecurityPolicy is the return-path counterpart to
// borrowedEgressSecurityPolicy. The public pointer API remains available for
// custom policies, while built-ins can inspect a packet-backed stack value
// without forcing one heap IpPath per received packet.
type borrowedIngressSecurityPolicy interface {
	inspectAndRefreshIngressBorrowed(
		provideMode protocol.ProvideMode,
		ipPath IpPath,
		payload []byte,
	) (SecurityPolicyResult, error)
}

// borrowedSenderIngressSecurityPolicy is the provider receive-side counterpart
// to borrowedSenderEgressSecurityPolicy.
type borrowedSenderIngressSecurityPolicy interface {
	inspectAndRefreshIngressForSenderBorrowed(
		senderClientId Id,
		provideMode protocol.ProvideMode,
		ipPath IpPath,
		payload []byte,
	) (SecurityPolicyResult, error)
}

func inspectAndRefreshIngressBorrowed(
	policy SecurityPolicy,
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	if borrowed, ok := policy.(borrowedIngressSecurityPolicy); ok {
		return borrowed.inspectAndRefreshIngressBorrowed(provideMode, ipPath, payload)
	}
	return inspectAndRefreshIngressFallback(policy, provideMode, ipPath, payload)
}

func inspectAndRefreshIngressForSenderBorrowed(
	policy SecurityPolicy,
	senderClientId Id,
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	if borrowed, ok := policy.(borrowedSenderIngressSecurityPolicy); ok {
		return borrowed.inspectAndRefreshIngressForSenderBorrowed(
			senderClientId,
			provideMode,
			ipPath,
			payload,
		)
	}
	return inspectAndRefreshIngressBorrowed(policy, provideMode, ipPath, payload)
}

// senderFlowSecurityPolicy exposes lifecycle retirement only to in-package
// provider plumbing. The public SecurityPolicy contract remains compatible with
// custom implementations.
type senderFlowSecurityPolicy interface {
	retireEgressFlowForSender(senderClientId Id, ipPath *IpPath)
	retireIngressFlowForSender(senderClientId Id, ipPath *IpPath)
	retireSender(senderClientId Id)
}

func retireIngressSecurityFlowForSender(
	policy SecurityPolicy,
	senderClientId Id,
	ipPath *IpPath,
) {
	if senderPolicy, ok := policy.(senderFlowSecurityPolicy); ok {
		senderPolicy.retireIngressFlowForSender(senderClientId, ipPath)
	}
}

func retireSecuritySender(policy SecurityPolicy, senderClientId Id) {
	if senderPolicy, ok := policy.(senderFlowSecurityPolicy); ok {
		senderPolicy.retireSender(senderClientId)
	}
}

//go:noinline
func inspectAndRefreshIngressFallback(
	policy SecurityPolicy,
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	result, err := policy.InspectIngress(provideMode, &ipPath, payload)
	if err == nil {
		policy.RefreshIngress(&ipPath)
	}
	return result, err
}

// egressRelationship combines the packet source's provide mode with the local
// client's own provide mode into the single relationship the security policy
// enforces on egress. ProvideMode is a set of flags with no ordinal meaning (see
// its definition in protocol) — this is a per-case decision, never max/min:
// egress may reach non-public destinations (e.g. a LAN) only under a genuine
// same-Network relationship on BOTH sides. Anything else, including an
// unspecified None, is treated as Public so the public-destination rules apply.
func egressRelationship(source, client protocol.ProvideMode) protocol.ProvideMode {
	if source == protocol.ProvideMode_Network && client == protocol.ProvideMode_Network {
		return protocol.ProvideMode_Network
	}
	return protocol.ProvideMode_Public
}

// securityPolicy inspects both directions of a flow from one object, so the egress DPI
// detector's flow table is shared with the ingress activity refresh (see dmcaDetector.touch).
type securityPolicy struct {
	stats *SecurityPolicyStatsCollector
	cfaa  *cfaaDetector
	dmca  *dmcaDetector
}

func DefaultSecurityPolicy(ctx context.Context) SecurityPolicy {
	return DefaultSecurityPolicyWithStats(ctx, DefaultSecurityPolicyStatsCollector())
}

func DefaultSecurityPolicyWithStats(ctx context.Context, stats *SecurityPolicyStatsCollector) SecurityPolicy {
	return NewSecurityPolicy(
		ctx,
		DefaultCfaaSecurityPolicySettings(),
		DefaultDmcaSecurityPolicySettings(),
		DefaultWebStandardSettings(),
		stats,
	)
}

func NewSecurityPolicy(ctx context.Context, cfaaSettings *CfaaSecurityPolicySettings, dmcaSettings *DmcaSecurityPolicySettings, webSettings *WebStandardSettings, stats *SecurityPolicyStatsCollector) SecurityPolicy {
	return &securityPolicy{
		stats: stats,
		cfaa:  newCfaaDetector(cfaaSettings),
		dmca:  newDmcaDetector(ctx, dmcaSettings, newWebStandardDetector(webSettings)),
	}
}

// DefaultProviderSecurityPolicy is the policy for the provider (exit) role: it egresses a remote
// client's traffic, so it runs Reverse(client policy). The provider's ingress (the remote client's
// outbound, received from the tunnel) gets the client policy's egress DPI; the provider's egress
// (the return into the tunnel) gets the client policy's ingress source check. A provider keeps its
// own detector + stats, independent of the device's multi-client policy.
func DefaultProviderSecurityPolicy(ctx context.Context) SecurityPolicy {
	return DefaultProviderSecurityPolicyWithStats(ctx, DefaultSecurityPolicyStatsCollector())
}

func DefaultProviderSecurityPolicyWithStats(ctx context.Context, stats *SecurityPolicyStatsCollector) SecurityPolicy {
	return Reverse(DefaultSecurityPolicyWithStats(ctx, stats))
}

func (self *securityPolicy) Stats() *SecurityPolicyStatsCollector {
	return self.stats
}

func (self *securityPolicy) InspectEgress(provideMode protocol.ProvideMode, ipPath *IpPath, payload []byte) (SecurityPolicyResult, error) {
	result, err := self.inspectEgress(provideMode, ipPath, payload)
	if ipPath != nil {
		self.stats.AddDestination(ipPath, result, 1)
	}
	return result, err
}

func (self *securityPolicy) inspectAndRefreshEgressBorrowed(
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	return self.inspectAndRefreshEgressForSenderBorrowed(Id{}, provideMode, ipPath, payload)
}

func (self *securityPolicy) inspectAndRefreshEgressForSenderBorrowed(
	senderClientId Id,
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	result, err := self.inspectEgressForSender(senderClientId, provideMode, &ipPath, payload)
	self.stats.AddDestination(&ipPath, result, 1)
	if err == nil {
		self.dmca.touchEgressForSender(senderClientId, &ipPath)
	}
	return result, err
}

func (self *securityPolicy) inspectAndRefreshEgressGroupBorrowed(
	provideMode protocol.ProvideMode,
	ipPaths []IpPath,
	payloads [][]byte,
) (SecurityPolicyResult, error) {
	ipPath := &ipPaths[0]
	result := SecurityPolicyResultAllow
	if provideMode != protocol.ProvideMode_Network {
		if !isPublicUnicast(ipPath.DestinationIp) {
			result = SecurityPolicyResultIncident
		} else {
			destinationIp, destinationVersion := policyAddress(ipPath.DestinationIp, ipPath.Version)
			switch self.cfaa.inspect(
				destinationIp,
				ipPath.DestinationPort,
				ipPath.Protocol,
				destinationVersion,
			) {
			case cfaaDrop:
				result = SecurityPolicyResultDrop
			case cfaaAllow:
				result = SecurityPolicyResultAllow
			default:
				for packetIndex := range payloads {
					memberResult := self.dmca.result(self.dmca.classify(
						&ipPaths[packetIndex],
						payloads[packetIndex],
					))
					result = conservativeSecurityPolicyResult(result, memberResult)
				}
			}
		}
	}
	self.stats.AddDestination(ipPath, result, uint64(len(ipPaths)))
	self.dmca.touchEgress(ipPath)
	return result, nil
}

func (self *securityPolicy) inspectEgress(provideMode protocol.ProvideMode, ipPath *IpPath, payload []byte) (SecurityPolicyResult, error) {
	return self.inspectEgressForSender(Id{}, provideMode, ipPath, payload)
}

func (self *securityPolicy) inspectEgressForSender(
	senderClientId Id,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	if protocol.ProvideMode_Network == provideMode {
		return SecurityPolicyResultAllow, nil
	}

	// apply public rules:
	// - only public unicast network destinations
	// - block insecure or known unencrypted traffic
	if !isPublicUnicast(ipPath.DestinationIp) {
		return SecurityPolicyResultIncident, nil
	}

	// static endpoint reputation (blocked ips + port policy) on the destination,
	// with a v4-mapped v6 destination judged as its v4 address
	destinationIp, destinationVersion := policyAddress(ipPath.DestinationIp, ipPath.Version)
	switch self.cfaa.inspect(destinationIp, ipPath.DestinationPort, ipPath.Protocol, destinationVersion) {
	case cfaaDrop:
		return SecurityPolicyResultDrop, nil
	case cfaaAllow:
		return SecurityPolicyResultAllow, nil
	default:
		// No static verdict — run stateful payload DPI. Switch on the verdict so
		// the policy reads explicitly: a positive BitTorrent signature is reported;
		// a flow that looks fully encrypted is dropped UNLESS it matched a
		// sanctioned web/communication standard (TLS/QUIC/DTLS/STUN/TURN or
		// RTP/RTCP), or an exact provider-scoped gaming exception — that positive
		// match is the fallback that rescues an otherwise-ambiguous encrypted flow.
		// Enforcement of each verdict honors the detector settings (log-only,
		// drop/report toggles), applied by result().
		switch v := self.dmca.classifyForSender(senderClientId, ipPath, payload); v {
		case dmcaBittorrent:
			return self.dmca.result(v), nil
		case dmcaDropEncrypted:
			return self.dmca.result(v), nil
		default:
			// still inspecting, a sanctioned standard, or benign plaintext
			return SecurityPolicyResultAllow, nil
		}
	}
}

func (self *securityPolicy) InspectIngress(provideMode protocol.ProvideMode, ipPath *IpPath, payload []byte) (SecurityPolicyResult, error) {
	result, err := self.inspectIngress(provideMode, ipPath)
	if ipPath != nil {
		self.stats.AddSource(ipPath, result, 1)
	}
	return result, err
}

func (self *securityPolicy) inspectAndRefreshIngressBorrowed(
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	return self.inspectAndRefreshIngressForSenderBorrowed(Id{}, provideMode, ipPath, payload)
}

func (self *securityPolicy) inspectAndRefreshIngressForSenderBorrowed(
	senderClientId Id,
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	result, err := self.inspectIngress(provideMode, &ipPath)
	self.stats.AddSource(&ipPath, result, 1)
	if err == nil {
		self.dmca.touchIngressForSender(senderClientId, &ipPath)
	}
	return result, err
}

// RefreshEgress/RefreshIngress refresh the flow's DPI activity time without making a decision (see
// the SecurityPolicy interface).
func (self *securityPolicy) RefreshEgress(ipPath *IpPath) {
	if ipPath != nil {
		self.dmca.touchEgress(ipPath)
	}
}

func (self *securityPolicy) RefreshIngress(ipPath *IpPath) {
	if ipPath != nil {
		self.dmca.touchIngress(ipPath)
	}
}

func (self *securityPolicy) retireEgressFlowForSender(senderClientId Id, ipPath *IpPath) {
	self.dmca.retireEgressForSender(senderClientId, ipPath)
}

func (self *securityPolicy) retireIngressFlowForSender(senderClientId Id, ipPath *IpPath) {
	if ipPath == nil {
		return
	}
	self.dmca.retireEgressForSender(senderClientId, ipPath.Reverse())
}

func (self *securityPolicy) retireSender(senderClientId Id) {
	self.dmca.removeSender(senderClientId)
}

func (self *securityPolicy) inspectIngress(provideMode protocol.ProvideMode, ipPath *IpPath) (SecurityPolicyResult, error) {
	// network-relationship traffic (e.g. same network_id) bypasses the public
	// rules, mirroring the egress policy. The return path of a network-mode
	// flow echoes the network provide mode, so a private service on any port
	// (including the p2p range) is not filtered here.
	if protocol.ProvideMode_Network == provideMode {
		return SecurityPolicyResultAllow, nil
	}

	// mirror the egress static drops (blocked ips + port policy), evaluated on the
	// source endpoint, with a v4-mapped v6 source judged as its v4 address
	sourceIp, sourceVersion := policyAddress(ipPath.SourceIp, ipPath.Version)
	if cfaaDrop == self.cfaa.inspect(sourceIp, ipPath.SourcePort, ipPath.Protocol, sourceVersion) {
		return SecurityPolicyResultDrop, nil
	}
	return SecurityPolicyResultAllow, nil
}

type disableSecurityPolicy struct {
	stats *SecurityPolicyStatsCollector
}

func DisableSecurityPolicy() SecurityPolicy {
	return &disableSecurityPolicy{
		stats: DefaultSecurityPolicyStatsCollector(),
	}
}

// DisableSecurityPolicyWithStats matches the SecurityPolicyGenerator signature (ctx is
// unused — the disabled policy keeps no flow state and runs no scan).
func DisableSecurityPolicyWithStats(ctx context.Context, stats *SecurityPolicyStatsCollector) SecurityPolicy {
	return &disableSecurityPolicy{
		stats: stats,
	}
}

func (self *disableSecurityPolicy) Stats() *SecurityPolicyStatsCollector {
	return self.stats
}

func (self *disableSecurityPolicy) InspectEgress(provideMode protocol.ProvideMode, ipPath *IpPath, payload []byte) (SecurityPolicyResult, error) {
	return SecurityPolicyResultAllow, nil
}

func (self *disableSecurityPolicy) inspectAndRefreshEgressBorrowed(
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	return SecurityPolicyResultAllow, nil
}

func (self *disableSecurityPolicy) inspectAndRefreshEgressForSenderBorrowed(
	senderClientId Id,
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	return SecurityPolicyResultAllow, nil
}

func (self *disableSecurityPolicy) inspectAndRefreshEgressGroupBorrowed(
	provideMode protocol.ProvideMode,
	ipPaths []IpPath,
	payloads [][]byte,
) (SecurityPolicyResult, error) {
	return SecurityPolicyResultAllow, nil
}

func (self *disableSecurityPolicy) InspectIngress(provideMode protocol.ProvideMode, ipPath *IpPath, payload []byte) (SecurityPolicyResult, error) {
	return SecurityPolicyResultAllow, nil
}

func (self *disableSecurityPolicy) inspectAndRefreshIngressBorrowed(
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	return SecurityPolicyResultAllow, nil
}

func (self *disableSecurityPolicy) inspectAndRefreshIngressForSenderBorrowed(
	senderClientId Id,
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	return SecurityPolicyResultAllow, nil
}

func (self *disableSecurityPolicy) RefreshEgress(ipPath *IpPath) {}

func (self *disableSecurityPolicy) RefreshIngress(ipPath *IpPath) {}

func (self *disableSecurityPolicy) retireEgressFlowForSender(senderClientId Id, ipPath *IpPath) {
}

func (self *disableSecurityPolicy) retireIngressFlowForSender(senderClientId Id, ipPath *IpPath) {
}

func (self *disableSecurityPolicy) retireSender(senderClientId Id) {}

// reverseSecurityPolicy swaps the egress and ingress directions of an underlying policy — the
// provider's view of a flow. The remote client's egress (the outbound packet the provider receives
// from the tunnel) is the provider's ingress, and the return is the provider's egress; so a provider
// runs Reverse(client policy), wired with the same convention as the multi-client. The flow key is
// unchanged (the underlying policy still keys by the egress 5-tuple), so only the method is swapped.
type reverseSecurityPolicy struct {
	policy SecurityPolicy
}

func Reverse(policy SecurityPolicy) SecurityPolicy {
	return &reverseSecurityPolicy{policy: policy}
}

func (self *reverseSecurityPolicy) Stats() *SecurityPolicyStatsCollector {
	return self.policy.Stats()
}

func (self *reverseSecurityPolicy) InspectEgress(provideMode protocol.ProvideMode, ipPath *IpPath, payload []byte) (SecurityPolicyResult, error) {
	return self.policy.InspectIngress(provideMode, ipPath, payload)
}

func (self *reverseSecurityPolicy) inspectAndRefreshEgressBorrowed(
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	result, err := self.policy.InspectIngress(provideMode, &ipPath, payload)
	if err == nil {
		self.policy.RefreshIngress(&ipPath)
	}
	return result, err
}

func (self *reverseSecurityPolicy) inspectAndRefreshEgressForSenderBorrowed(
	senderClientId Id,
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	return inspectAndRefreshIngressForSenderBorrowed(
		self.policy,
		senderClientId,
		provideMode,
		ipPath,
		payload,
	)
}

func (self *reverseSecurityPolicy) InspectIngress(provideMode protocol.ProvideMode, ipPath *IpPath, payload []byte) (SecurityPolicyResult, error) {
	return self.policy.InspectEgress(provideMode, ipPath, payload)
}

func (self *reverseSecurityPolicy) inspectAndRefreshIngressBorrowed(
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	return inspectAndRefreshEgressBorrowed(self.policy, provideMode, ipPath, payload)
}

func (self *reverseSecurityPolicy) inspectAndRefreshIngressForSenderBorrowed(
	senderClientId Id,
	provideMode protocol.ProvideMode,
	ipPath IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	return inspectAndRefreshEgressForSenderBorrowed(
		self.policy,
		senderClientId,
		provideMode,
		ipPath,
		payload,
	)
}

func (self *reverseSecurityPolicy) RefreshEgress(ipPath *IpPath) {
	self.policy.RefreshIngress(ipPath)
}

func (self *reverseSecurityPolicy) RefreshIngress(ipPath *IpPath) {
	self.policy.RefreshEgress(ipPath)
}

func (self *reverseSecurityPolicy) retireEgressFlowForSender(senderClientId Id, ipPath *IpPath) {
	if senderPolicy, ok := self.policy.(senderFlowSecurityPolicy); ok {
		senderPolicy.retireIngressFlowForSender(senderClientId, ipPath)
	}
}

func (self *reverseSecurityPolicy) retireIngressFlowForSender(senderClientId Id, ipPath *IpPath) {
	if senderPolicy, ok := self.policy.(senderFlowSecurityPolicy); ok {
		senderPolicy.retireEgressFlowForSender(senderClientId, ipPath)
	}
}

func (self *reverseSecurityPolicy) retireSender(senderClientId Id) {
	if senderPolicy, ok := self.policy.(senderFlowSecurityPolicy); ok {
		senderPolicy.retireSender(senderClientId)
	}
}

// Testing_FlowCount reports the number of tracked DMCA flows. Test hook: exact flow-table
// assertions (fill == n, reclaim -> 0) are deterministic where heap-delta assertions are
// not (allocator noise, -race). Reach it by type-asserting a SecurityPolicy to
// interface{ Testing_FlowCount() int }.
func (self *securityPolicy) Testing_FlowCount() int {
	return self.dmca.flowCount()
}

func (self *reverseSecurityPolicy) Testing_FlowCount() int {
	if p, ok := self.policy.(interface{ Testing_FlowCount() int }); ok {
		return p.Testing_FlowCount()
	}
	return 0
}

// isPublicUnicast reports whether an egress destination is on the public
// internet. An IPv4-mapped v6 address (::ffff:a.b.c.d) is judged as the v4
// address it carries, so a v6 packet cannot reach private v4 space by
// wrapping the address; the v6 prefixes that embed or tunnel to v4 (6to4,
// Teredo, NAT64, IPv4-compatible) and the non-routable v6 ranges are not
// public either. See IPV6.md C4.
func isPublicUnicast(ip net.IP) bool {
	if ip4 := ip.To4(); ip4 != nil {
		ip = ip4
	}
	switch {
	case ip.IsPrivate(),
		ip.IsLoopback(),
		ip.IsLinkLocalUnicast(),
		ip.IsMulticast(),
		ip.IsUnspecified():
		return false
	}
	if len(ip) == net.IPv6len && isReservedIpv6(ip) {
		return false
	}
	return true
}

// isReservedIpv6 lists the v6 prefixes that are never a public unicast
// egress destination but pass net.IP's own predicates: the ranges that embed
// a v4 address (6to4 2002::/16, Teredo 2001::/32, NAT64 64:ff9b::/96 and
// its local-use 64:ff9b:1::/48, the deprecated IPv4-compatible ::/96), the
// discard-only 100::/64, documentation 2001:db8::/32, the deprecated
// site-local fec0::/10 and the historical 6bone 3ffe::/16. `ip` must be a
// 16-byte address that is not v4-mapped.
func isReservedIpv6(ip net.IP) bool {
	switch {
	case ip[0] == 0x20 && ip[1] == 0x02:
		// 6to4
		return true
	case ip[0] == 0x20 && ip[1] == 0x01 && ip[2] == 0x00 && ip[3] == 0x00:
		// teredo
		return true
	case ip[0] == 0x20 && ip[1] == 0x01 && ip[2] == 0x0d && ip[3] == 0xb8:
		// documentation
		return true
	case ip[0] == 0x00 && ip[1] == 0x64 && ip[2] == 0xff && ip[3] == 0x9b &&
		((ip[4] == 0 && ip[5] == 0 && ip[6] == 0 && ip[7] == 0 &&
			ip[8] == 0 && ip[9] == 0 && ip[10] == 0 && ip[11] == 0) ||
			(ip[4] == 0 && ip[5] == 1)):
		// nat64 well-known prefix and its local-use prefix
		return true
	case ip[0] == 0x01 && ip[1] == 0x00 &&
		ip[2] == 0 && ip[3] == 0 && ip[4] == 0 && ip[5] == 0 && ip[6] == 0 && ip[7] == 0:
		// discard-only
		return true
	case ip[0] == 0xfe && ip[1]&0xc0 == 0xc0:
		// site-local
		return true
	case ip[0] == 0x3f && ip[1] == 0xfe:
		// 6bone
		return true
	}
	// ipv4-compatible ::a.b.c.d: the first 96 bits zero. :: itself is
	// unspecified and ::1 loopback, both already excluded by the caller, so
	// this only matches the deprecated embedded-v4 form.
	for _, b := range ip[:12] {
		if b != 0 {
			return false
		}
	}
	return true
}

// policyAddress is the address a v4-keyed policy table sees for a packet: an
// IPv4-mapped v6 destination is unwrapped to its v4 address and version so
// the v4 block tables and port policies apply to it, everything else passes
// through unchanged.
func policyAddress(ip net.IP, version int) (net.IP, int) {
	if version == 6 {
		if ip4 := ip.To4(); ip4 != nil {
			return ip4, 4
		}
	}
	return ip, version
}

type SecurityPolicyStats = map[SecurityPolicyResult]map[SecurityDestination]uint64

const (
	// securityPolicyStatsMaxDestinationsPerResult includes one overflow
	// destination. Security-policy statistics are diagnostics, not flow state:
	// a long-lived provider must not retain every ephemeral port it has ever
	// relayed or clone that growing set whenever statistics are inspected.
	securityPolicyStatsMaxDestinationsPerResult = 1024
	securityPolicyStatsUnknownResult            = SecurityPolicyResult(-1)
)

var securityPolicyStatsOverflowDestination = SecurityDestination{}

type SecurityDestination struct {
	Version  int
	Protocol IpProtocol
	Ip       string
	Port     int
}

func newSecurityDestinationPort(ipPath *IpPath) SecurityDestination {
	return SecurityDestination{
		Version:  ipPath.Version,
		Protocol: ipPath.Protocol,
		Ip:       "",
		Port:     ipPath.DestinationPort,
	}
}

func newSecurityDestination(ipPath *IpPath) SecurityDestination {
	return SecurityDestination{
		Version:  ipPath.Version,
		Protocol: ipPath.Protocol,
		Ip:       ipPath.DestinationIp.String(),
		Port:     ipPath.DestinationPort,
	}
}

func newSecuritySourcePort(ipPath *IpPath) SecurityDestination {
	return SecurityDestination{
		Version:  ipPath.Version,
		Protocol: ipPath.Protocol,
		Ip:       "",
		Port:     ipPath.SourcePort,
	}
}

func newSecuritySource(ipPath *IpPath) SecurityDestination {
	return SecurityDestination{
		Version:  ipPath.Version,
		Protocol: ipPath.Protocol,
		Ip:       ipPath.SourceIp.String(),
		Port:     ipPath.SourcePort,
	}
}

func (self *SecurityDestination) Cmp(b SecurityDestination) int {
	if self.Version < b.Version {
		return -1
	} else if b.Version < self.Version {
		return 1
	}

	if self.Protocol < b.Protocol {
		return -1
	} else if b.Protocol < self.Protocol {
		return 1
	}

	if self.Ip < b.Ip {
		return -1
	} else if b.Ip < self.Ip {
		return 1
	}

	if self.Port < b.Port {
		return -1
	} else if b.Port < self.Port {
		return 1
	}

	return 0
}

func (self *SecurityDestination) String() string {
	if *self == securityPolicyStatsOverflowDestination {
		return "other destinations"
	}
	return fmt.Sprintf("ipv%d %s %s",
		self.Version,
		self.Protocol.String(),
		net.JoinHostPort(self.Ip, strconv.Itoa(self.Port)),
	)
}

// get current counts of outcomes per (protocol, destination port)
type SecurityPolicyStatsCollector struct {
	includeIp bool

	stateLock               sync.Mutex
	resultDestinationCounts SecurityPolicyStats
}

func DefaultSecurityPolicyStatsCollector() *SecurityPolicyStatsCollector {
	return &SecurityPolicyStatsCollector{
		includeIp:               false,
		resultDestinationCounts: SecurityPolicyStats{},
	}
}

// add records one diagnostic count while bounding every result's destination
// cardinality. Built-in policies produce the three declared results; callers
// passing another integer share one unknown-result bucket so arbitrary result
// values cannot defeat the memory bound.
func (self *SecurityPolicyStatsCollector) add(
	destination SecurityDestination,
	result SecurityPolicyResult,
	count uint64,
) {
	if count == 0 {
		return
	}
	switch result {
	case SecurityPolicyResultDrop,
		SecurityPolicyResultAllow,
		SecurityPolicyResultIncident:
	default:
		result = securityPolicyStatsUnknownResult
	}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	destinationCounts, ok := self.resultDestinationCounts[result]
	if !ok {
		destinationCounts = map[SecurityDestination]uint64{}
		self.resultDestinationCounts[result] = destinationCounts
	}
	if _, ok := destinationCounts[destination]; !ok &&
		securityPolicyStatsMaxDestinationsPerResult <= len(destinationCounts)+1 {
		// Reserve the final slot for all later destinations. A real IpPath has
		// version 4 or 6, so the zero destination cannot collide with one.
		destination = securityPolicyStatsOverflowDestination
	}
	destinationCounts[destination] += count
}

func (self *SecurityPolicyStatsCollector) AddDestination(ipPath *IpPath, result SecurityPolicyResult, count uint64) {
	var destination SecurityDestination
	if self.includeIp {
		destination = newSecurityDestination(ipPath)
	} else {
		// port only, no ip
		destination = newSecurityDestinationPort(ipPath)
	}
	self.add(destination, result, count)
}

func (self *SecurityPolicyStatsCollector) AddSource(ipPath *IpPath, result SecurityPolicyResult, count uint64) {
	var destination SecurityDestination
	if self.includeIp {
		destination = newSecuritySource(ipPath)
	} else {
		// port only, no ip
		destination = newSecuritySourcePort(ipPath)
	}
	self.add(destination, result, count)
}

func (self *SecurityPolicyStatsCollector) Stats(reset bool) SecurityPolicyStats {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	resultDestinationCounts := SecurityPolicyStats{}
	for result, destinationCounts := range self.resultDestinationCounts {
		resultDestinationCounts[result] = maps.Clone(destinationCounts)
	}
	if reset {
		clear(self.resultDestinationCounts)
	}
	return resultDestinationCounts
}
