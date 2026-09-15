package connect

import (
	"context"
	"encoding/binary"
	mathrand "math/rand"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Provider qualification probes.
//
// WHY A PROBE IS ALLOWED TO EXIST HERE AT ALL
//
// An earlier review rejected probing outright, and was right about the case it
// rejected: probes as NEGATIVE evidence. Anti-bot infrastructure drops
// datacenter-ip traffic as a matter of policy, so a probe that goes unanswered
// says approximately nothing about the provider that carried it -- and a
// mechanism that convicts on that silence manufactures the very "evidence" the
// rest of this file spends thousands of lines refusing to manufacture. The
// cping episode is the pinned lesson: a health check that could convict did
// convict, healthy exits, repeatedly.
//
// This mechanism is the strict inverse. A probe that is ANSWERED is proof the
// provider completed a real upstream dial to a real destination -- providers
// are split-tcp, so the SynAck only exists after the provider's own connect
// succeeded, which no amount of forwarding-without-connecting can fake. That
// proof qualifies the provider. A probe that is NOT answered leaves the
// provider exactly where it stood: unqualified, unstruck, unranked-down,
// unremoved, with not one counter moved anywhere in the judgment machinery.
// False negatives are affordable because the pool is oversized; false positives
// of the convicting kind are not affordable at all.
//
// Everything below exists to make that asymmetry structural rather than
// aspirational:
//
//   - the send bypasses windowStats entirely (sendProbe, not SendDetailed), so
//     a probe to a dead site cannot record an unanswered syn or an outstanding
//     nack. Without this, probing an idle spare would walk it straight into the
//     no-receive-syn verdict -- convicted by its own health check.
//   - the receive records normally. A real SynAck or dns answer through the
//     exit IS delivery, and the ordinary counters (addReceiveAck /
//     addReceiveSyn at the channel, addConnectSuccess here) are the honest
//     place to say so.
//   - probe flows never enter clientUpdates (no flow-cap or blast-radius
//     impact), never join affinity maps, never open a reliabilityMetrics flow,
//     and are removed from the path maps explicitly when the probe ends rather
//     than left for the idle reaper.
//   - probe answers never reach receivePacketCallback. The tun must never see
//     a packet it did not ask for.
//
// F-1 is the mechanism only. Nothing here starts a probe; the startup sweep,
// the joiner hook, the pooling changes and the effectiveTier consumption are
// the next package. With no caller, ProviderProbe defaulting to true is inert.

// probeClass is what a target proves and therefore how it is asked.
type probeClass int

const (
	// probeClassHealth is an ordinary site, dialed with a tcp syn at :443. The
	// answer (SynAck) requires the provider's own upstream connect to have
	// succeeded, which is the whole point.
	probeClassHealth probeClass = 0
	// probeClassDns is a public resolver, asked an address query at udp/53
	// (A over v4, AAAA over v6 -- the family of the resolver ip). Any answer
	// at all is success; the content is irrelevant and is not parsed.
	probeClassDns probeClass = 1
)

// the dns record types the probe asks for and the resolution stage parses
const (
	probeDnsRecordTypeA    uint16 = 1
	probeDnsRecordTypeAAAA uint16 = 28
)

// probeDnsRecordTypeForIpVersion is the address record type a pass over
// ipVersion asks for: the answer must be an address the same pass can dial.
func probeDnsRecordTypeForIpVersion(ipVersion int) uint16 {
	if ipVersion == 6 {
		return probeDnsRecordTypeAAAA
	}
	return probeDnsRecordTypeA
}

// probeTarget is one destination of one probe pass, already resolved.
//
// Resolution is deliberately NOT this package's job. A provider with broken or
// hijacked dns would otherwise fail a tcp probe that was never about dns, and
// the confusion would be indistinguishable from a real dial failure. The
// tunnel's doh cache resolves outside the probed channel and hands the
// addresses in; F-1's tests use literals for the same reason.
//
// Ip carries the address family: registration picks the matching probe
// source address and ip version from it, and every packet crafted for the
// target is built for that version, so a v6 target is asked over v6 with no
// further plumbing (IPV6.md B1: a pass asks only over a family the exit can
// carry, see probeProviderPass).
type probeTarget struct {
	// Host is the name this target came from, for logging and for the dns
	// query name. Empty is legal (a literal-ip target).
	Host string
	Ip   net.IP
	Port int
	// QueryName is the name a dns-class probe asks for. Empty falls back to
	// probeDefaultQueryName.
	QueryName string
	Class     probeClass
	// CaptureAnswer asks the ingress path to keep a copy of a dns-class
	// answer's payload on the probe flow, for the resolution stage
	// (probeResolveNames) to parse. Off for plain probes: the F-1 rule that a
	// dns answer's CONTENT is irrelevant to the pass verdict stays true either
	// way -- capture only decides whether the bytes are retained for the
	// caller, never how the probe is judged.
	CaptureAnswer bool
}

// probeHostTarget builds a health-class target for a resolved hostname.
func probeHostTarget(host string, ip net.IP) probeTarget {
	return probeTarget{
		Host:  host,
		Ip:    ip,
		Port:  443,
		Class: probeClassHealth,
	}
}

// probeResolverTarget builds a dns-class target. queryName may be empty.
func probeResolverTarget(ip net.IP, queryName string) probeTarget {
	return probeTarget{
		Host:      ip.String(),
		Ip:        ip,
		Port:      53,
		QueryName: queryName,
		Class:     probeClassDns,
	}
}

// probeResult is one probe pass against one exit.
//
// There is no error field and no failure taxonomy on purpose. A probe either
// produced positive evidence or produced nothing, and a richer failure story
// would immediately become something a future caller could act on -- which is
// exactly what this design forbids.
type probeResult struct {
	// Sent is how many probes actually left for the provider. A target that
	// could not be registered or whose send was refused is not counted, so the
	// pass is judged only on questions that were really asked.
	Sent int
	// Answered is how many of those came back.
	Answered int
	// Passed is Answered/Sent at or above probePassFraction, with at least one
	// probe sent. A pass that asked nothing never qualifies anyone.
	Passed   bool
	Duration time.Duration
	// Resolved is how many of the pass's hostnames the provider carried a
	// usable dns answer back for, supplied by the caller that ran the
	// resolution stage (probeProviderPass). It is the provider-liveness
	// half of the verdict: a provider that resolves names is demonstrably
	// carrying traffic out and back, so answered==0 alongside Resolved>0 is
	// a statement about the TARGETS (or about egress-ip policy at those
	// targets), while Resolved==0 with nothing else alive usually means
	// nothing carried the query at all.
	//
	// It is an input rather than something probeExit computes, because
	// probeExit only ever sees already-resolved targets. A field that is
	// declared, documented and logged but never assigned is worse than no
	// field: it reports a constant that reads as evidence.
	Resolved int
}

// probeSourceIp4 and probeSourceIp6 are the local addresses probe flows claim.
//
// 198.18.0.1 is from the RFC 2544 benchmarking range 198.18.0.0/15, and
// 2001:2::1 from the RFC 5180 range 2001:2::/48. Both exist precisely so that
// test traffic has addresses that never appear as a real host on a real
// network, which makes "is this packet a probe?" a structural question rather
// than a heuristic: a tun interface is never assigned one of these, so no
// application flow can ever produce a path key that collides with a probe's.
//
// They are only ever a NAT key. The provider dials the destination from its own
// socket, and the destination never learns this address.
var (
	probeSourceIp4 = net.IPv4(198, 18, 0, 1).To4()
	probeSourceIp6 = net.ParseIP("2001:2::1")
)

const (
	// probeSourcePortMin and probeSourcePortMax bound the local source ports
	// probe flows use, inclusive.
	//
	// 61000 is chosen because linux (and therefore android) defaults
	// net.ipv4.ip_local_port_range to 32768-60999: the kernel never
	// auto-assigns a source port at or above 61000, so this range cannot
	// collide with the ephemeral traffic a tun actually carries. 512 ports is
	// far more than the concurrency this mechanism will ever have in flight
	// (one pass is five probes) and leaves headroom for the aggressive startup
	// sweep of the next package.
	//
	// The range is belt to the addresses' braces: the ingress classifier
	// requires BOTH the reserved port and the benchmarking source address, so a
	// hypothetical application that binds 61000 explicitly still cannot have
	// its traffic mistaken for a probe.
	probeSourcePortMin = 61000
	probeSourcePortMax = 61511

	// probeSendTimeout bounds how long one crafted packet may wait for the
	// transport to accept it. Short: a provider whose send buffer is jammed is
	// not going to answer a probe inside the pass anyway, and blocking here
	// would stall a sweep behind its slowest member.
	probeSendTimeout = 1 * time.Second

	// defaultProbeTimeout is the fallback when a caller passes a
	// non-positive timeout. It matches the ProbeTimeout setting default.
	defaultProbeTimeout = 4 * time.Second

	// probeDefaultQueryName is the name a dns probe asks for when the caller
	// supplies none. A name every public resolver answers and nobody's
	// filtering blocks.
	probeDefaultQueryName = "www.google.com"
)

// probeSourcePortSeq round-robins the starting point of the port search so
// concurrent probes to different destinations do not all collide on the first
// port and rescan. Correctness does not depend on it -- the search under the
// parent lock is what guarantees a free key -- so a plain wrapping counter is
// enough.
var probeSourcePortSeq atomic.Uint32

// probeFlow is the live state of one in-flight probe, hung off the path-map
// update so the ingress and dial-failure paths can complete it by flow key
// alone.
type probeFlow struct {
	target probeTarget
	ipPath *IpPath
	// synSequence is the sequence the crafted syn carried, so the courtesy rst
	// can be built with the sequence a real stack would send (isn+1). The
	// provider itself accepts any sequence on a known flow, but building a
	// packet that is only correct by the receiver's leniency is how the
	// ipOosRstSequence bug happened once already.
	synSequence uint32

	done         chan struct{}
	answered     atomic.Bool
	completeOnce sync.Once
	// answer is the dns answer payload, kept only when the target asked for it
	// (CaptureAnswer) and the probe completed answered. Written inside the
	// completeOnce and read only after done is closed, so the channel close is
	// the publication barrier and no lock is needed.
	answer []byte
}

// complete records the probe's outcome exactly once and wakes the waiter. Late
// duplicates (a retransmitted SynAck, a dial-failure icmp arriving after a
// timeout) are absorbed rather than double-counted.
func (self *probeFlow) complete(answered bool) {
	if self == nil {
		return
	}
	self.completeOnce.Do(func() {
		self.answered.Store(answered)
		close(self.done)
	})
}

// completeAnswered is complete(true) carrying the answer payload, for
// capture-marked flows. The payload is stored inside the same once so a racing
// dial-failure complete(false) can never interleave with the write: whichever
// wins the once decides both the outcome and (for this path) the bytes, and
// the loser is absorbed.
func (self *probeFlow) completeAnswered(answer []byte) {
	if self == nil {
		return
	}
	self.completeOnce.Do(func() {
		self.answer = answer
		self.answered.Store(true)
		close(self.done)
	})
}

// isProbe reports whether this flow belongs to the probe mechanism.
//
// The single source of truth is the probe payload pointer, exposed as a
// nil-safe predicate rather than a parallel bool field: every judgment site in
// this file asks `update.isProbe()` before acting, and a flag that could drift
// out of sync with the state it describes is the one failure mode that would
// silently re-arm the behavior this whole design forbids.
func (self *multiClientChannelUpdate) isProbe() bool {
	return self != nil && self.probe != nil
}

// probeReservedPort reports whether a local port is in the probe range.
func probeReservedPort(port int) bool {
	return probeSourcePortMin <= port && port <= probeSourcePortMax
}

// probeSourceAddr reports whether an address is one of the probe source
// addresses. net.IP.Equal normalizes the v4/v4-in-v6 forms, so a path parsed
// either way matches.
func probeSourceAddr(ip net.IP) bool {
	return ip != nil && (ip.Equal(probeSourceIp4) || ip.Equal(probeSourceIp6))
}

// probeIngressPath reports whether an INGRESS-oriented path (remote endpoint is
// the source, as it arrives from the provider) is addressed to a probe flow.
// Both halves are required; see probeSourcePortMin.
func probeIngressPath(ipPath *IpPath) bool {
	return ipPath != nil &&
		probeReservedPort(ipPath.DestinationPort) &&
		probeSourceAddr(ipPath.DestinationIp)
}

// probeEgressPath is probeIngressPath for a path in the egress direction (the
// flow-map key direction), used by the dial-failure intercept.
func probeEgressPath(ipPath *IpPath) bool {
	return ipPath != nil &&
		probeReservedPort(ipPath.SourcePort) &&
		probeSourceAddr(ipPath.SourceIp)
}

// probeSourceIpFor picks the local source address matching the target family.
func probeSourceIpFor(ip net.IP) (net.IP, int, bool) {
	if ip == nil {
		return nil, 0, false
	}
	if ip4 := ip.To4(); ip4 != nil {
		return probeSourceIp4, 4, true
	}
	if ip.To16() != nil {
		return probeSourceIp6, 6, true
	}
	return nil, 0, false
}

// --- packet crafting (lock-free; never called with any lock held) ---

// probeSynPacket builds a bare tcp syn for the probe flow. No options: the mss
// and window-scale a real client would negotiate are irrelevant to a handshake
// that will be reset the moment it completes, and every byte here is budget.
func probeSynPacket(ipPath *IpPath, sequenceNumber uint32) []byte {
	return ipOosTcpPacketSequence(ipPath, tcpFlagSyn, sequenceNumber, nil)
}

// probeCourtesyRstPacket builds the reset that closes a completed probe
// handshake. Sequence isn+1: the syn consumed one sequence number, so this is
// what the provider's peer would accept from a real stack (RFC 793 abort). The
// providers accept any sequence on a flow they know, but see probeFlow.
func probeCourtesyRstPacket(ipPath *IpPath, synSequence uint32) ([]byte, bool) {
	return ipOosRstSequence(ipPath, synSequence+1)
}

// probeDnsQueryPacket builds a udp/53 packet carrying a standard recursive
// address query for name: A over a v4 path, AAAA over a v6 path, so the
// resolution stage gets addresses the same pass can dial. Returns false when
// the name cannot be encoded as a dns question (labels are bounded at 63
// bytes and names at 255).
func probeDnsQueryPacket(ipPath *IpPath, name string, id uint16) ([]byte, bool) {
	question, ok := dnsQuestionType(name, probeDnsRecordTypeForIpVersion(ipPath.Version))
	if !ok {
		return nil, false
	}
	// header: id, flags, qdcount, ancount, nscount, arcount
	payload := make([]byte, 12+len(question))
	binary.BigEndian.PutUint16(payload[0:2], id)
	// standard query, recursion desired
	binary.BigEndian.PutUint16(payload[2:4], 0x0100)
	binary.BigEndian.PutUint16(payload[4:6], 1)
	binary.BigEndian.PutUint16(payload[6:8], 0)
	binary.BigEndian.PutUint16(payload[8:10], 0)
	binary.BigEndian.PutUint16(payload[10:12], 0)
	copy(payload[12:], question)
	return ipOosUdpPacket(ipPath, payload), true
}

// dnsQuestion encodes name as a dns question section for an A query: the
// v4 form of dnsQuestionType.
func dnsQuestion(name string) ([]byte, bool) {
	return dnsQuestionType(name, probeDnsRecordTypeA)
}

// dnsQuestionType encodes name as a dns question section: length-prefixed
// labels, a root label, then the given qtype and qclass IN.
func dnsQuestionType(name string, recordType uint16) ([]byte, bool) {
	name = strings.TrimSuffix(name, ".")
	if name == "" {
		return nil, false
	}
	labels := strings.Split(name, ".")
	// labels + length bytes + root + qtype + qclass
	question := make([]byte, 0, len(name)+2+4)
	for _, label := range labels {
		if len(label) == 0 || 63 < len(label) {
			return nil, false
		}
		question = append(question, byte(len(label)))
		question = append(question, label...)
	}
	question = append(question, 0)
	if 255 < len(question) {
		return nil, false
	}
	// qtype, qclass IN
	question = binary.BigEndian.AppendUint16(question, recordType)
	question = append(question, 0, 1)
	return question, true
}

// probePacket crafts the probe for a target on its registered path.
func probePacket(ipPath *IpPath, target probeTarget, synSequence uint32) ([]byte, bool) {
	switch target.Class {
	case probeClassHealth:
		return probeSynPacket(ipPath, synSequence), true
	case probeClassDns:
		queryName := target.QueryName
		if queryName == "" {
			queryName = probeDefaultQueryName
		}
		// the dns transaction id rides the same draw as the tcp sequence; it
		// only has to be unguessable enough that an off-path answer is not
		// mistaken for the resolver's
		return probeDnsQueryPacket(ipPath, queryName, uint16(synSequence))
	default:
		return nil, false
	}
}

// --- flow registration (parent lock) ---

// probeCtx is the context probe flows are built under. A parent assembled
// literally (the test fixtures) has no ctx, and context.WithCancel on a nil
// parent panics -- probing must degrade to a detached context, not to a crash.
func (self *RemoteUserNatMultiClient) probeCtx() context.Context {
	if self.ctx != nil {
		return self.ctx
	}
	return context.Background()
}

// registerProbeFlow installs a probe-marked update in the path maps, keyed on a
// free source port in the reserved range, and returns it.
//
// This is deliberately NOT sendUpdate. Everything sendUpdate does beyond the
// map insert is bookkeeping a probe must not participate in: the flow-opened
// metric, the affinity-group joins, the idle-reaper goroutine that would
// eventually rst the flow toward the tun, and (via the callers) the
// clientUpdates entry that decides flow caps, blast radius and drain weight. A
// probe is not a flow the user has; it is a question, and the path maps are
// borrowed only long enough to route the answer.
//
// The update is pre-bound to client so the dial-failure intercept can match a
// failure to the very exit that reported it, exactly as it does for real flows.
//
// called without stateLock
func (self *RemoteUserNatMultiClient) registerProbeFlow(
	client *multiClientChannel,
	target probeTarget,
) (*probeFlow, bool) {
	sourceIp, version, ok := probeSourceIpFor(target.Ip)
	if !ok {
		return nil, false
	}

	protocolNumber := IpProtocolTcp
	if target.Class == probeClassDns {
		protocolNumber = IpProtocolUdp
	}

	synSequence := mathrand.Uint32()

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	span := probeSourcePortMax - probeSourcePortMin + 1
	start := int(probeSourcePortSeq.Add(1)-1) % span

	for i := 0; i < span; i += 1 {
		sourcePort := probeSourcePortMin + (start+i)%span

		ipPath := &IpPath{
			Version:         version,
			Protocol:        protocolNumber,
			SourceIp:        sourceIp,
			SourcePort:      sourcePort,
			DestinationIp:   target.Ip,
			DestinationPort: target.Port,
			SequenceNumber:  synSequence,
			Syn:             target.Class == probeClassHealth,
		}

		// the collision check is on the full flow key, which is what the maps
		// are keyed by: two probes to different destinations may share a port
		// harmlessly, and a real flow can never produce this key at all (see
		// probeSourceIp4).
		switch version {
		case 4:
			if _, exists := self.ip4PathUpdates[ipPath.ToIp4Path()]; exists {
				continue
			}
		case 6:
			if _, exists := self.ip6PathUpdates[ipPath.ToIp6Path()]; exists {
				continue
			}
		default:
			return nil, false
		}

		probe := &probeFlow{
			target:      target,
			ipPath:      ipPath,
			synSequence: synSequence,
			done:        make(chan struct{}),
		}
		update := newMultiClientChannelUpdate(self.probeCtx(), ipPath)
		update.probe = probe
		update.activityTime = time.Now()
		update.client.Store(client)

		switch version {
		case 4:
			self.ip4PathUpdates[ipPath.ToIp4Path()] = update
		case 6:
			self.ip6PathUpdates[ipPath.ToIp6Path()] = update
		}
		// note: no clientUpdates entry, no affinity join, no flowOpened, and no
		// idle-reaper goroutine -- see the comment above.
		return probe, true
	}

	// every port in the range is busy toward this destination. A probe is
	// optional by construction, so this is a quiet no-op rather than an error.
	return nil, false
}

// unregisterProbeFlows removes probe updates from the path maps and cancels
// their contexts. Explicit, and the only removal path: probe updates have no
// idle reaper to fall back on, which is the point -- a probe that ended must
// leave no state behind that a later packet could resolve to.
//
// The identity check matters: the ingress and dial-failure intercepts remove
// the entry as they complete a probe, so by the time the pass cleans up, the
// slot may legitimately be free (or, after a full port-range wrap, reused).
//
// called without stateLock
func (self *RemoteUserNatMultiClient) unregisterProbeFlows(probes []*probeFlow) {
	if len(probes) == 0 {
		return
	}

	updates := func() []*multiClientChannelUpdate {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		removed := []*multiClientChannelUpdate{}
		for _, probe := range probes {
			if probe == nil || probe.ipPath == nil {
				continue
			}
			switch probe.ipPath.Version {
			case 4:
				key := probe.ipPath.ToIp4Path()
				if update, ok := self.ip4PathUpdates[key]; ok && update.probe == probe {
					delete(self.ip4PathUpdates, key)
					removed = append(removed, update)
				}
			case 6:
				key := probe.ipPath.ToIp6Path()
				if update, ok := self.ip6PathUpdates[key]; ok && update.probe == probe {
					delete(self.ip6PathUpdates, key)
					removed = append(removed, update)
				}
			}
		}
		return removed
	}()

	// cancel outside the lock: Close takes the update's own leaf lock, and the
	// parent lock must never be held over a leaf
	for _, update := range updates {
		update.Close()
	}
}

// --- the probe pass ---

// probeExit asks one exit to reach a set of destinations and reports what came
// back. It is the entire mechanism; nothing in this package calls it.
//
// All probes go out first and are then waited on together against one deadline,
// rather than serially: five targets at a 4s bound must cost 4s, not 20s, and
// the answers are independent.
//
// Locking: registration and cleanup take the parent stateLock; crafting and
// sending take none. The channel's own lock is never held across either. The
// wait holds nothing at all, which is what lets an answer arriving on the
// ingress path complete a probe while the pass is still waiting for its
// siblings.
func (self *RemoteUserNatMultiClient) probeExit(
	client *multiClientChannel,
	targets []probeTarget,
	timeout time.Duration,
	// resolved: how many hostnames the provider carried a dns answer back
	// for in this pass's resolution stage; 0 from callers that did none
	resolved int,
) probeResult {
	result := probeResult{Resolved: resolved}
	if self == nil || client == nil || len(targets) == 0 {
		return result
	}
	reliabilitySettings := self.reliabilitySettings()
	// the A/B kill switch. off means the mechanism does not exist: no packets,
	// no state, no qualification.
	if !reliabilitySettings.ProviderProbe {
		return result
	}
	// a caller that does not care takes the configured bound, then the built-in
	// one -- so the knob is live for a caller that passes 0 and explicit for
	// one that does not
	if timeout <= 0 {
		timeout = reliabilitySettings.ProbeTimeout
	}
	if timeout <= 0 {
		timeout = defaultProbeTimeout
	}

	startTime := time.Now()

	probes := []*probeFlow{}
	for _, target := range targets {
		probe, ok := self.registerProbeFlow(client, target)
		if !ok {
			continue
		}
		packet, ok := probePacket(probe.ipPath, target, probe.synSequence)
		if !ok {
			self.unregisterProbeFlows([]*probeFlow{probe})
			continue
		}
		if !client.sendProbe(&parsedPacket{packet: packet, ipPath: probe.ipPath}, probeSendTimeout) {
			// the transport refused the packet, so the question was never
			// asked. Not counted as sent, and -- like every other probe failure
			// -- not held against the exit either.
			self.unregisterProbeFlows([]*probeFlow{probe})
			continue
		}
		result.Sent += 1
		self.reliabilityMetrics.probeSent()
		probes = append(probes, probe)
	}

	if len(probes) == 0 {
		// no question was asked -- every target failed to register, or the
		// transport refused every packet. Judging a provider on that (even as a
		// recorded failure, which is non-punitive) would be inventing evidence
		// out of the client's own inability to ask, so the pass records
		// nothing and says nothing.
		result.Duration = time.Since(startTime)
		return result
	}

	// a dying exit ends the pass early rather than burning the full deadline
	var clientDone <-chan struct{}
	if client.ctx != nil {
		clientDone = client.ctx.Done()
	}
	var parentDone <-chan struct{}
	if self.ctx != nil {
		parentDone = self.ctx.Done()
	}

	deadline := time.NewTimer(timeout)
	defer deadline.Stop()

waiting:
	for _, probe := range probes {
		select {
		case <-probe.done:
		case <-deadline.C:
			break waiting
		case <-clientDone:
			break waiting
		case <-parentDone:
			break waiting
		}
	}

	// counted from the flags rather than from the wait, so an answer that
	// landed after the deadline (or while an earlier sibling was still being
	// waited on) still counts -- the evidence is positive either way
	for _, probe := range probes {
		if probe.answered.Load() {
			result.Answered += 1
			self.reliabilityMetrics.probeAnswered()
		}
	}
	self.unregisterProbeFlows(probes)

	result.Duration = time.Since(startTime)
	result.Passed = 0 < result.Sent &&
		probePassFraction <= float64(result.Answered)/float64(result.Sent)

	destination := client.probeDestination()
	if result.Passed {
		self.recordProbePass(destination)
	} else {
		self.recordProbeFail(destination)
	}

	// the silence streak (ProbeSilenceWarnStreak): total silence -- no stage-B
	// answer AND no dns resolution across the whole pass -- counts one strike;
	// any answer at all is proof of life and clears it, PASSED or not. This is
	// the only writer besides the receive-side acquittal in probeSilentStreak,
	// and it is deliberately downstream of the pass/fail recording above: the
	// streak is a placement input, never a qualification one.
	if result.Answered == 0 && result.Resolved == 0 {
		client.recordProbeSilence()
	} else {
		client.recordProbeLife()
	}

	// one line per pass per provider at the default level, per the house rules;
	// the per-packet events are silent. This is the line the acceptance drill
	// reads, so it carries the counts that decide the verdict.
	// The line has to separate "this provider is not there" from "these
	// targets did not answer", because providers here are consumer DEVICES:
	// one that is asleep, backgrounded, or off the network fails every probe
	// while being a perfectly good provider when awake, and that is not the
	// same fact as a provider whose upstream cannot reach the target list.
	//
	// recvage is the provider-side discriminator: a provider carrying live
	// traffic that answers no probes is a TARGET-side or egress-policy
	// story; one whose last receive is minutes old is very likely gone.
	//
	// uplinkstale is the PHONE-side one, and it is the discriminator that
	// matters for this owner's actual case: the vpn runs on a mobile device
	// that sleeps and changes networks, and while the phone's own uplink is
	// silent EVERY provider fails EVERY probe at once. That is one fact
	// about the phone, not fifteen facts about providers, and without this
	// field the capture cannot tell those apart.
	//
	// All three reads are cheap and lock-safe here: probeExit holds nothing
	// at this point (registration and cleanup take the parent lock in their
	// own scopes above), probeReceiveAge takes only the channel's own lock,
	// and uplinkGate/flowCount take the parent lock internally.
	uplinkStale, _ := self.uplinkGate(time.Now())
	loggerOrDefault(self.log).Infof("%s\n", relEvent(
		"probe",
		"exit", client.ClientId(),
		"result", strings.ToLower(probeResultWord(result.Passed)),
		"answered", result.Answered,
		"sent", result.Sent,
		"ms", result.Duration.Round(time.Millisecond),
		// -1 for never, so "no return traffic ever" and "none recently"
		// are distinct facts
		"recvage", client.probeReceiveAge(),
		// hostnames the provider carried a dns answer back for this pass: a
		// live provider whose upstream works resolves names even when the
		// health hosts refuse the probe source
		"dns", result.Resolved,
		"transport", client.hasActiveTransport(),
		"flows", client.flowCount(),
		// 1 on a failing pass means the tunnel as a whole was silent
		"uplinkstale", uplinkStale,
		// consecutive all-silent passes; at ProbeSilenceWarnStreak the resize
		// pass warns the exit out of new-flow placement (event=warn to=silent)
		"streak", client.probeSilentStreak(),
	))

	return result
}

func probeResultWord(passed bool) string {
	if passed {
		return "PASS"
	}
	return "FAIL"
}

// --- ingress: consume, never forward ---

// clientReceiveProbePacket consumes an inbound packet addressed to the probe
// source range. It is called from clientReceivePacket for every such packet and
// ALWAYS consumes it -- matched or not.
//
// Consuming the unmatched case is the invariant, not laziness. A destination
// that retransmits its SynAck after the pass ended, or an answer that races the
// cleanup, would otherwise fall through to the "not in response to outgoing
// traffic" branch and be handed to the tun -- a packet addressed to
// 198.18.0.1, which the tun never asked for and which no application can
// possibly want. Nothing addressed to a probe port at a probe address is ever
// forwarded, full stop.
//
// Locking: the map lookup takes the parent stateLock; the connect-success
// record and the courtesy rst happen after it is released (the channel takes
// its own lock, and the parent lock must never sit above it).
//
// packet is the raw ip packet the answer arrived as. It is only read -- never
// retained -- on this call's stack: a capture-marked dns flow (the resolution
// stage) gets a COPY of the udp payload, because the buffer belongs to the
// transport and may be pooled and reused the moment this returns.
func (self *RemoteUserNatMultiClient) clientReceiveProbePacket(
	sourceClient *multiClientChannel,
	ipPath *IpPath,
	packet []byte,
) {
	// ipPath is ingress-oriented; the probe flow is keyed on the egress
	// direction it was registered with
	egressIpPath := ipPath.Reverse()

	var probe *probeFlow
	var update *multiClientChannelUpdate
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		var found *multiClientChannelUpdate
		switch egressIpPath.Version {
		case 4:
			key := egressIpPath.ToIp4Path()
			if u, ok := self.ip4PathUpdates[key]; ok && u.isProbe() {
				found = u
				delete(self.ip4PathUpdates, key)
			}
		case 6:
			key := egressIpPath.ToIp6Path()
			if u, ok := self.ip6PathUpdates[key]; ok && u.isProbe() {
				found = u
				delete(self.ip6PathUpdates, key)
			}
		}
		if found == nil {
			return
		}
		// removed as it is answered: a retransmitted answer then finds nothing,
		// is consumed by the unmatched path above, and cannot re-complete or
		// re-send a courtesy reset
		update = found
		probe = found.probe
	}()

	if probe == nil {
		// consumed and dropped
		return
	}

	// What counts as an answer, per class.
	//
	// A tcp probe requires a SYN-ACK specifically, NOT merely a packet back.
	// A provider whose own upstream dial is refused answers with RST+ACK
	// (classifyDialFailure -> dialFailureRst), so "anything came back" would
	// qualify a provider that demonstrably could not connect. That is a false
	// positive, and false positives are the one error direction this design
	// cannot afford: a wrongly-qualified provider gets real user traffic, while
	// a wrongly-unqualified one merely waits. The RST still completes the probe
	// -- as unanswered, which records nothing anywhere.
	//
	// A dns probe accepts any datagram from the resolver. The provider had to
	// carry the query out and the reply back for one to exist at all, and
	// parsing the payload would buy nothing but a way to reject valid answers.
	answered := false
	switch probe.target.Class {
	case probeClassHealth:
		answered = ipPath.Syn && ipPath.Ack
	case probeClassDns:
		answered = true
	}

	if answered && probe.target.Class == probeClassDns && probe.target.CaptureAnswer {
		// the resolution stage wants the answer bytes. The payload is copied
		// out of the transport's buffer (see the doc comment) and published
		// through the completion once; a parse failure here degrades to a
		// plain answer, never to a lost completion -- the probe-level evidence
		// ("the resolver answered") does not depend on the content.
		if _, payload, err := ParseIpPathWithPayload(packet); err == nil && 0 < len(payload) {
			answerCopy := make([]byte, len(payload))
			copy(answerCopy, payload)
			probe.completeAnswered(answerCopy)
		} else {
			probe.complete(true)
		}
	} else {
		probe.complete(answered)
	}

	if !answered {
		if update != nil {
			update.Close()
		}
		return
	}

	// A probe answer is real delivery evidence and is recorded like any other:
	// the channel already counted the bytes and the syn in clientReceive, and a
	// SynAck is a proven upstream connect -- the same fact addConnectSuccess
	// exists to record, arrived at by the same means (the provider dialed, the
	// destination answered). This is the positive half of the asymmetry; the
	// failure half records nothing anywhere.
	if sourceClient != nil && ipPath.Syn && ipPath.Ack {
		sourceClient.addConnectSuccess(ipPath.Version)
	}

	// courtesy close, so the destination is not left holding a half-open
	// connection for its own timeout and the provider can release the socket
	if sourceClient != nil && probe.target.Class == probeClassHealth {
		if rstPacket, ok := probeCourtesyRstPacket(probe.ipPath, probe.synSequence); ok {
			sourceClient.sendProbe(&parsedPacket{
				packet: rstPacket,
				ipPath: probe.ipPath,
			}, probeSendTimeout)
		}
	}

	if update != nil {
		update.Close()
	}
}

// --- dial failure: record, never strike ---

// probeDialFailure intercepts a provider dial-failure signal that names a probe
// flow, ahead of every other effect in clientDialFailure. It reports whether the
// signal was a probe's, in which case the caller must do nothing else with it.
//
// What it deliberately does NOT do, and why:
//
//   - no dial strike (addDialFailure). Three strikes across two destinations is
//     dialStarved, which is a +2 effectiveTier demerit -- so probing a spare
//     against sites that drop datacenter ranges would rank it below providers
//     that were never tested. A probe must not be able to demote anyone.
//   - no re-race. There is no application flow here to rescue; the "flow" is
//     the question itself, and unbinding it would only orphan the probe.
//   - no dialFailureIntercepted metric. The counters exist to measure what
//     provider failures cost users, and a probe's failure costs a user nothing.
//   - no forwarding of the icmp when DialFailureRerace is off. The tun must
//     never see probe traffic in either knob position.
//   - no uplink ingress stamp. The stamp makes silence-based verdicts against
//     OTHER exits admissible again, so a probe's failure signal could indirectly
//     help convict a third party. Skipping it can only make verdicts less
//     admissible, which is the safe direction for a mechanism that must never
//     convict.
//
// called without stateLock
func (self *RemoteUserNatMultiClient) probeDialFailure(
	sourceClient *multiClientChannel,
	egressIpPath *IpPath,
) bool {
	// cheap structural reject first: real dial failures pay two comparisons
	if !probeEgressPath(egressIpPath) {
		return false
	}

	var probe *probeFlow
	var update *multiClientChannelUpdate
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		var found *multiClientChannelUpdate
		switch egressIpPath.Version {
		case 4:
			key := egressIpPath.ToIp4Path()
			if u, ok := self.ip4PathUpdates[key]; ok && u.isProbe() {
				found = u
				delete(self.ip4PathUpdates, key)
			}
		case 6:
			key := egressIpPath.ToIp6Path()
			if u, ok := self.ip6PathUpdates[key]; ok && u.isProbe() {
				found = u
				delete(self.ip6PathUpdates, key)
			}
		}
		if found == nil {
			return
		}
		// the same ownership guard the real path applies: a signal from an exit
		// that does not own this probe is stale and is dropped (still consumed
		// -- it named a probe address, so it is ours either way)
		if found.client.Load() != sourceClient {
			// put it back; another exit's probe is still live on this key
			switch egressIpPath.Version {
			case 4:
				self.ip4PathUpdates[egressIpPath.ToIp4Path()] = found
			case 6:
				self.ip6PathUpdates[egressIpPath.ToIp6Path()] = found
			}
			return
		}
		update = found
		probe = found.probe
	}()

	if probe != nil {
		// the answer to the question is "no". That is the entire effect.
		probe.complete(false)
		if update != nil {
			update.Close()
		}
	}

	// consumed either way: an icmp naming a probe address is never the app's
	return true
}

// --- qualification state ---

const (
	// QualificationMaxAge is how long a passed probe keeps a provider
	// qualified. Long enough that a startup sweep's work is not thrown away
	// mid-session, short enough that a provider whose upstream degrades an hour
	// in stops carrying a stale proof. Re-probing a stale provider is
	// opportunistic and idle-only; a loaded exit re-earns its qualification for
	// free through real receive progress.
	QualificationMaxAge = 30 * time.Minute

	// qualificationMaxEntries bounds the table. A client sees far fewer
	// providers than this in a session, so the cap is a memory guarantee rather
	// than a working constraint; eviction is oldest-probed-first.
	qualificationMaxEntries = 256
)

// providerQualification is one provider's probe history, keyed by destination
// so it survives channel incarnations -- the same provider re-dialed is the
// same provider, and re-proving it on every reconnect would waste the sweep.
type providerQualification struct {
	// qualifiedAt is when the most recent PASS landed (zero if never).
	qualifiedAt time.Time
	// lastProbeAt is when any pass last ran, which is also the eviction key.
	lastProbeAt time.Time
	passed      int
	failed      int
}

// probeDestination is the qualification key for a channel, nil-safe for the
// bare fixtures that carry no args.
func (self *multiClientChannel) probeDestination() MultiHopId {
	if self == nil || self.args == nil {
		return MultiHopId{}
	}
	return self.args.Destination
}

// recordProbePass marks a provider qualified as of now.
//
// called without stateLock
func (self *RemoteUserNatMultiClient) recordProbePass(destination MultiHopId) {
	now := time.Now()

	newlyQualified := func() bool {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()

		entry := self.qualificationEntryWithLock(destination)
		was := !entry.qualifiedAt.IsZero() && now.Sub(entry.qualifiedAt) < QualificationMaxAge
		entry.qualifiedAt = now
		entry.lastProbeAt = now
		entry.passed += 1
		self.evictQualificationWithLock()
		return !was
	}()

	// counted on the transition only: the number that matters is how many
	// providers this session actually proved, not how many times it re-proved
	// the same one
	if newlyQualified {
		self.reliabilityMetrics.providerQualified()
	}
}

// recordProbeFail records a pass that did not qualify.
//
// It does not un-qualify anything. A provider that passed and later fails a
// pass keeps its qualification until it ages out -- the failure may be the
// list's most-defended entries refusing a datacenter ip, and demoting on that
// is precisely the negative-evidence use this design rejects. Time, not
// failure, is what ends a qualification.
//
// called without stateLock
func (self *RemoteUserNatMultiClient) recordProbeFail(destination MultiHopId) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	entry := self.qualificationEntryWithLock(destination)
	entry.lastProbeAt = time.Now()
	entry.failed += 1
	self.evictQualificationWithLock()
}

// providerQualified reports whether a provider has a probe pass inside
// QualificationMaxAge. Unknown providers and stale ones are both simply "not
// qualified" -- never "bad".
func (self *RemoteUserNatMultiClient) providerQualified(destination MultiHopId) bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	entry, ok := self.qualification[destination]
	if !ok || entry.qualifiedAt.IsZero() {
		return false
	}
	return time.Since(entry.qualifiedAt) < QualificationMaxAge
}

// must be called with stateLock
func (self *RemoteUserNatMultiClient) qualificationEntryWithLock(destination MultiHopId) *providerQualification {
	if self.qualification == nil {
		// lazily created: parents assembled literally by fixtures have no maps
		self.qualification = map[MultiHopId]*providerQualification{}
	}
	entry, ok := self.qualification[destination]
	if !ok {
		entry = &providerQualification{}
		self.qualification[destination] = entry
	}
	return entry
}

// evictQualificationWithLock bounds the table oldest-probed-first. The cap is
// small enough that an exact scan is cheaper than approximating, and exactness
// means "the most recently probed providers survive" is a property a test can
// assert rather than a tendency.
//
// must be called with stateLock
func (self *RemoteUserNatMultiClient) evictQualificationWithLock() {
	for qualificationMaxEntries < len(self.qualification) {
		var oldestKey MultiHopId
		var oldestTime time.Time
		found := false
		for key, entry := range self.qualification {
			if !found || entry.lastProbeAt.Before(oldestTime) {
				oldestKey, oldestTime, found = key, entry.lastProbeAt, true
			}
		}
		if !found {
			return
		}
		delete(self.qualification, oldestKey)
	}
}

// --- the send seam ---

// sendProbe sends a crafted probe packet through the channel with NONE of the
// window accounting.
//
// This is the seam the whole design rests on, so it is worth being explicit
// about what was and was not skipped relative to SendDetailedWithAck, which it
// otherwise mirrors line for line:
//
//   - addSend is skipped. That call is what records an outstanding send (a
//     nack), starts the send-stall clock, counts a syn, and registers the
//     destination in the stats window. Every one of those is an input to a
//     verdict: unanswered syns feed no-receive-syn, outstanding nacks feed
//     no-send-ack and sendStalled, and the destination count feeds the
//     no-receive-ack corroboration bar. A probe to a site that drops datacenter
//     ips would otherwise convict an idle spare with evidence the client
//     manufactured about itself -- the exact failure this package exists to
//     avoid.
//   - addSendAbandoned is skipped, because it is only the undo of addSend.
//   - addSendAck is skipped, because it is only the retirement of addSend's
//     nack; calling it alone would drive the counter negative.
//   - addError is skipped. An error here says the transport refused a probe,
//     not that the provider is failing the user, and endErr is read by the
//     removal path.
//
// The transport-level ack is NOT skipped. Delivery still rides the ordinary
// reliable send sequence (no NoAck option); the callback is simply nil, so the
// ack retires inside the send buffer and is never reported to the channel's
// judgment counters. A probe is delivered as reliably as any packet and
// observed by nobody.
//
// Returns whether the packet was accepted by the transport. A refusal is not
// recorded anywhere.
func (self *multiClientChannel) sendProbe(parsedPacket *parsedPacket, timeout time.Duration) bool {
	if self == nil || parsedPacket == nil {
		return false
	}

	protocolVersion := DefaultProtocolVersion
	if self.settings != nil {
		protocolVersion = self.settings.ProtocolVersion
	}

	frame, err := ipPacketToProviderFrame(parsedPacket.packet, protocolVersion)
	if err != nil {
		// not addError: see above
		return false
	}

	// a stalled exit swallows the packet, matching SendDetailedWithAck exactly
	// (including its position after the frame build): the probe is reported
	// sent and never answered, which is precisely what a stalled provider does
	// to it. This is also how the exclusion tests model a never-answering exit
	// without a transport.
	if self.stalled.Load() {
		return true
	}

	// bare fixture channels have no underlying client; refuse rather than
	// panic, the same convention ClientId and Tier follow
	if self.client == nil {
		return false
	}

	var opts []any
	if self.performanceProfile != nil && self.performanceProfile.AllowDirect {
		opts = append(opts, ForceStream())
	}

	success, err := self.client.SendMultiHopWithTimeoutDetailed(
		frame,
		self.args.Destination,
		// nil: the transport's own ack machinery still runs, the channel's
		// accounting does not observe it
		nil,
		timeout,
		opts...,
	)
	// ownership mirrors SendDetailedWithAck: the packet is consumed on success,
	// the wrapped marshal buffer is freed on failure. Probe packets are plain
	// allocations rather than pooled ones, so the pool return is a no-op for
	// them -- kept anyway so this stays a faithful copy of the path it mirrors.
	if err != nil || !success {
		if !frame.Raw {
			MessagePoolReturn(frame.MessageBytes)
		}
		return false
	}
	if !frame.Raw {
		MessagePoolReturn(parsedPacket.packet)
	}
	return true
}
