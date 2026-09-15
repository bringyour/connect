package connect

import (
	"encoding/binary"
	"hash/fnv"
	"net"
	"sync"
	"time"
)

// The prober: the F-2 half of provider qualification. F-1
// (ip_remote_multi_client_probe.go) is the mechanism -- probeExit, the flow
// lifecycle, the no-convict guarantees; this file is the caller it was written
// for: the resolution flow that turns sampled hostnames into probe targets
// THROUGH the channel being probed, the loop that decides which providers get
// probed when, and the qualification consumers (the effectiveTier demerit and
// the pooling admit preference live at their consumption sites in
// ip_remote_multi_client.go, anchored by tests here).
//
// WHY RESOLUTION GOES THROUGH THE PROBED CHANNEL
//
// The F-1 design note says resolution is the caller's job and imagined the
// tun's doh cache doing it outside the channel. That imagined dependency is
// exactly what killed the previous probe suite on android: Go's pure resolver
// has no server list there and falls back to [::1]:53, and a tun built without
// resolver settings resolves nothing -- every probe timed out at 15s
// regardless of the tunnel's health, and the harness bug read as a tunnel
// fault (the postmortem is pinned in the DeveloperScreen comment). So F-2 is
// deliberately self-contained: the sampled resolver ip is asked over udp/53
// through the provider under test, which doubles as the dns-class probe --
// one question both proves the provider carries dns and yields the addresses
// the tcp probes need. No os resolver, no doh cache, no dependency that can
// fail for a reason that is not about the provider.
//
// The one confusion that design must not reintroduce: a provider whose sampled
// RESOLVER is down would fail a pass that was never about that resolver. So a
// silent resolver falls the pass back to literal-ip targets (the table carries
// a few precisely so this fallback exists), and the unanswered resolution
// queries feed nothing -- same asymmetry as everything else here.

// --- dns answer parsing ---

// parseDnsAResponse reads the A records out of a dns response: the v4 form
// of parseDnsAddressResponse.
func parseDnsAResponse(payload []byte, id uint16) ([]net.IP, bool) {
	return parseDnsAddressResponse(payload, id, 4)
}

// parseDnsAddressResponse reads the address records of one family out of a
// dns response -- A over v4, AAAA over v6 -- matching it to the query by
// transaction id. It parses only what the prober needs -- the header, enough
// of the question section to skip it, and the answer records' type/class/rdata
// -- and treats everything unexpected as "no records" rather than trying to
// be a resolver.
//
// The return contract: ok reports a well-formed response bearing the expected
// id (an NXDOMAIN or an answerless response is still ok -- the resolver
// answered; the name just yielded nothing). Malformed bytes, a foreign id, or
// a packet that is not a response at all return (nil, false), and the caller
// treats that exactly like silence. A parser error must never be distinguishable
// from no answer, because there is nothing honest to do with the distinction.
func parseDnsAddressResponse(payload []byte, id uint16, ipVersion int) ([]net.IP, bool) {
	wantRecordType := probeDnsRecordTypeForIpVersion(ipVersion)
	wantRdataLength := net.IPv4len
	if ipVersion == 6 {
		wantRdataLength = net.IPv6len
	}
	if len(payload) < 12 {
		return nil, false
	}
	if binary.BigEndian.Uint16(payload[0:2]) != id {
		// an off-path or stale datagram; not our answer
		return nil, false
	}
	flags := binary.BigEndian.Uint16(payload[2:4])
	if flags&0x8000 == 0 {
		// QR unset: a query, not a response. A middlebox (or a test harness)
		// echoing the question back must not read as a resolution.
		return nil, false
	}
	questionCount := int(binary.BigEndian.Uint16(payload[4:6]))
	answerCount := int(binary.BigEndian.Uint16(payload[6:8]))
	// nscount/arcount are deliberately ignored: the records the prober wants
	// are all in the answer section, and parsing stops when they end.

	offset := 12
	for i := 0; i < questionCount; i += 1 {
		next, ok := dnsSkipName(payload, offset)
		if !ok || len(payload) < next+4 {
			return nil, false
		}
		// qtype + qclass
		offset = next + 4
	}

	// rcode != 0 (NXDOMAIN, SERVFAIL, ...) is a well-formed answer with, for
	// our purposes, no records. Parsed this far so a garbage packet with the
	// rcode bits set cannot pass as one.
	if flags&0x000F != 0 {
		return nil, true
	}

	ips := []net.IP{}
	for i := 0; i < answerCount; i += 1 {
		next, ok := dnsSkipName(payload, offset)
		if !ok || len(payload) < next+10 {
			return nil, false
		}
		recordType := binary.BigEndian.Uint16(payload[next : next+2])
		recordClass := binary.BigEndian.Uint16(payload[next+2 : next+4])
		// ttl (4 bytes) is skipped: the prober uses the addresses once,
		// immediately, inside the same pass
		rdataLength := int(binary.BigEndian.Uint16(payload[next+8 : next+10]))
		rdataStart := next + 10
		if len(payload) < rdataStart+rdataLength {
			return nil, false
		}
		// the wanted address type (A, 4-byte rdata, or AAAA, 16-byte rdata),
		// class IN (1); every other record type (CNAME chains, the other
		// family's address, dnssec baggage) is skipped, not an error --
		// resolvers legitimately mix them into an address response
		if recordType == wantRecordType && recordClass == 1 && rdataLength == wantRdataLength {
			ip := make(net.IP, wantRdataLength)
			copy(ip, payload[rdataStart:rdataStart+wantRdataLength])
			ips = append(ips, ip)
		}
		offset = rdataStart + rdataLength
	}
	return ips, true
}

// dnsSkipName advances past a (possibly compressed) dns name starting at
// offset, returning the offset just after it. It never follows a compression
// pointer -- a pointer ends the name in place (RFC 1035 4.1.4), which is all
// skipping requires -- so it cannot loop on malicious pointer cycles.
func dnsSkipName(payload []byte, offset int) (int, bool) {
	for {
		if len(payload) <= offset {
			return 0, false
		}
		length := int(payload[offset])
		switch {
		case length == 0:
			// root label: end of name
			return offset + 1, true
		case length&0xC0 == 0xC0:
			// compression pointer: two bytes, ends the name
			return offset + 2, len(payload) >= offset+2
		case length&0xC0 != 0:
			// 0x40/0x80 label types were never deployed; treat as malformed
			return 0, false
		default:
			offset += 1 + length
		}
	}
}

// --- resolution through the probed channel ---

// probeSampleWidth is how many health hosts a pass asks about: the
// ProbeSampleHostCount setting when positive, else the ENTIRE table. Every
// sampled hostname is resolved -- a pass owes a dial question to each host it
// sampled, and the resolution stage is one small udp datagram per name, all in
// flight together, so width costs bytes, never wall time.
func probeSampleWidth(reliabilitySettings *ReliabilitySettings) int {
	if reliabilitySettings != nil && 0 < reliabilitySettings.ProbeSampleHostCount {
		return reliabilitySettings.ProbeSampleHostCount
	}
	return len(probeHostNames)
}

// probeResolveNames resolves names by asking resolverIp over udp/53 THROUGH
// the probed channel, one address query per name (A over a v4 resolver,
// AAAA over a v6 one, so the answers are dialable over the same family), all
// in flight together against one deadline. It returns the addresses per name
// and whether the resolver answered anything at all -- the signal the
// caller's fallback keys on.
//
// These queries are real probes: each one sent counts probesSent and each
// answer counts probesAnswered (the provider demonstrably carried them), but
// they are NOT part of any pass verdict -- probeExit judges only the targets
// the caller builds afterwards. A resolver being down therefore cannot fail
// the provider; it can only shrink the question list, and the caller refills
// it from the literal-ip table.
//
// Locking mirrors probeExit exactly: registration and cleanup under the parent
// stateLock, crafting and sending lock-free, the wait holding nothing.
func (self *RemoteUserNatMultiClient) probeResolveNames(
	client *multiClientChannel,
	resolverIp net.IP,
	names []string,
	timeout time.Duration,
) (resolved map[string][]net.IP, resolverAnswered bool) {
	resolved = map[string][]net.IP{}
	if self == nil || client == nil || resolverIp == nil || len(names) == 0 {
		return
	}
	// the resolver's family is the pass's family: the query type and the
	// records the parser keeps follow it
	_, ipVersion, ok := probeSourceIpFor(resolverIp)
	if !ok {
		return
	}
	if timeout <= 0 {
		timeout = defaultProbeTimeout
	}

	type resolution struct {
		name  string
		probe *probeFlow
	}
	resolutions := []resolution{}
	for _, name := range names {
		target := probeResolverTarget(resolverIp, name)
		// the answer payload is what this stage exists for; see probeFlow
		target.CaptureAnswer = true
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
			// the question was never asked; not counted, not held against anyone
			self.unregisterProbeFlows([]*probeFlow{probe})
			continue
		}
		self.reliabilityMetrics.probeSent()
		resolutions = append(resolutions, resolution{name: name, probe: probe})
	}
	if len(resolutions) == 0 {
		return
	}

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
	for _, r := range resolutions {
		select {
		case <-r.probe.done:
		case <-deadline.C:
			break waiting
		case <-clientDone:
			break waiting
		case <-parentDone:
			break waiting
		}
	}

	probes := []*probeFlow{}
	for _, r := range resolutions {
		probes = append(probes, r.probe)
		if !r.probe.answered.Load() {
			continue
		}
		// any datagram back is the F-1 dns-probe evidence: the provider carried
		// the query out and the reply back. Whether it RESOLVES anything is a
		// separate question the parser answers.
		self.reliabilityMetrics.probeAnswered()
		resolverAnswered = true
		// the transaction id rode the same draw as the syn sequence (see
		// probePacket), which is what ties the answer to this query
		if ips, ok := parseDnsAddressResponse(r.probe.answer, uint16(r.probe.synSequence), ipVersion); ok && 0 < len(ips) {
			resolved[r.name] = ips
		}
	}
	self.unregisterProbeFlows(probes)
	return
}

// --- the provider pass ---

// probeSeedBase folds a provider's destination into the sampler seed space, so
// each provider walks its own deterministic rotation through the target table
// (F-1's sampleProbeTargets contract: base + passIndex, consecutive passes
// disjoint). fnv-1a because the requirement is determinism and spread, not
// secrecy.
func probeSeedBase(destination MultiHopId) uint64 {
	h := fnv.New64a()
	for _, id := range destination.Ids() {
		h.Write(id.Bytes())
	}
	return h.Sum64()
}

// probeFallbackLiteralTargets is the v4 form of
// probeFallbackLiteralTargetsForIpVersion.
func probeFallbackLiteralTargets() []probeTarget {
	return probeFallbackLiteralTargetsForIpVersion(4)
}

// probeFallbackLiteralTargetsForIpVersion is the resolver-down fallback:
// every health host in the table that is a literal ip of the pass's family
// (a v4 literal stands for its v6 sibling over v6, see probeLiteralHostIp),
// dialable with no resolution at all. This is what keeps "the sampled
// resolver is down" from reading as "the provider is unqualifiable" -- the
// pass still asks real dial questions, just fewer of them.
func probeFallbackLiteralTargetsForIpVersion(ipVersion int) []probeTarget {
	targets := []probeTarget{}
	for _, host := range probeHostNames {
		if ip, ok := probeLiteralHostIp(host, ipVersion); ok {
			targets = append(targets, probeHostTarget(host, ip))
		}
	}
	return targets
}

// probeProviderPass runs one full qualification pass against one exit:
//
//  1. sample this provider's next rotation block (hosts + one resolver),
//  2. resolve the sampled hostnames via that resolver THROUGH the channel
//     (which is also the dns-class probe -- see the file comment),
//  3. tcp-syn the answered addresses plus any literal-ip hosts, judged by
//     probeExit with all of F-1's no-convict machinery.
//
// A silent resolver (or one that answers nothing usable) falls back to the
// table's literal-ip targets, so the pass never becomes unwinnable for a
// reason that is not about the provider. A pass that ends with zero buildable
// targets sends nothing and records nothing -- probeExit's empty-targets
// contract.
//
// WHICH FAMILY A PASS ASKS OVER (IPV6.md B1)
//
// A pass asks its questions over one address family, chosen from the exit's
// category by probeIpVersionForFamily: v4-capable exits (v4-only, legacy) are
// asked over v4, a v6-only exit over v6, and a dualstack exit alternates by
// pass index so both of its families are proven over consecutive passes. The
// family drives everything the pass builds -- the resolver drawn, the literal
// hosts (a v4 literal stands for its v6 sibling), the record type queried
// and parsed, and the packets themselves. A family the exit cannot carry is
// never asked, so a v6-only exit is never read as unqualifiable for failing
// v4 questions that were never about the provider.
//
// Qualification is per provider, not per family: a dualstack exit is proven
// by EITHER family's pass. A v6 pass that fails while the v4 pass succeeds
// records nothing against the exit (no pass ever does, see recordProbeFail),
// and "this host has no v6 path" must not read as a provider fault -- the
// same rule as B5, where only repeated dial failures on one family, with the
// other healthy, downgrade an exit's category.
//
// The two stages share the configured ProbeTimeout each, so a fully silent
// provider costs one pass at most ~2x ProbeTimeout -- bounded, and paid off
// the packet path in the prober's own goroutine.
func (self *RemoteUserNatMultiClient) probeProviderPass(client *multiClientChannel) probeResult {
	if self == nil || client == nil {
		return probeResult{}
	}
	reliabilitySettings := self.reliabilitySettings()
	if !reliabilitySettings.ProviderProbe {
		return probeResult{}
	}
	timeout := reliabilitySettings.ProbeTimeout
	if timeout <= 0 {
		timeout = defaultProbeTimeout
	}

	destination := client.probeDestination()
	// the pass index advances the rotation so repeated passes cover the table;
	// it is read from the same record the pass will update. With the default
	// full-table width the rotation is a no-op (every pass covers everything);
	// it only matters when ProbeSampleHostCount narrows the pass. The same
	// index alternates a dualstack exit's family.
	_, _, passIndex := self.qualificationSnapshot(destination)
	seed := probeSeedBase(destination) + uint64(passIndex)
	ipVersion := probeIpVersionForFamily(seed, client.IpFamily())
	if !client.supportsIpVersion(ipVersion) {
		// unreachable by construction (the category is what the version was
		// chosen from), kept as the guard that makes the no-wrong-family
		// rule structural: a pass over a family the exit cannot carry asks
		// nothing and records nothing -- the empty-targets contract
		return probeResult{}
	}
	hosts, resolver := sampleProbeTargetsForIpVersion(seed, probeSampleWidth(reliabilitySettings), ipVersion)

	targets := []probeTarget{}
	names := []string{}
	for _, host := range hosts {
		if ip, ok := probeLiteralHostIp(host, ipVersion); ok {
			// literal-ip health hosts skip resolution entirely
			targets = append(targets, probeHostTarget(host, ip))
		} else if net.ParseIP(host) != nil {
			// a literal of the other family with no sibling in this one:
			// not dialable over this pass and not resolvable either
			continue
		} else {
			names = append(names, host)
		}
	}

	// how many names the provider carried a usable dns answer back for --
	// the provider-liveness half of the verdict line, so a pass that
	// resolves names but gets no syn answered reads as a target-side story
	// rather than an absent provider
	resolvedCount := 0
	resolverIp := net.ParseIP(resolver)
	if resolverIp != nil && 0 < len(names) {
		resolved, _ := self.probeResolveNames(client, resolverIp, names, timeout)
		resolvedAny := false
		for _, name := range names {
			// first address record only: one dial question per name. More
			// addresses for one name are the same site again, not more
			// evidence.
			if ips := resolved[name]; 0 < len(ips) {
				targets = append(targets, probeHostTarget(name, ips[0]))
				resolvedAny = true
				resolvedCount += 1
			}
		}
		if !resolvedAny {
			// the resolver was silent, or answered nothing usable: the pass
			// falls back to LITERAL-IP-ONLY targets (the table's, a superset
			// of whatever literals the sample held) rather than let one
			// resolver outage read as an unqualifiable provider. The
			// unanswered resolution queries themselves feed nothing.
			targets = probeFallbackLiteralTargetsForIpVersion(ipVersion)
		}
	} else if resolverIp != nil {
		// a sample with no hostnames (all literal) still owes the pass its
		// dns-class question, the F-1 shape: one resolver, any answer counts
		targets = append(targets, probeResolverTarget(resolverIp, ""))
	}

	if len(targets) == 0 {
		// unreachable with the shipped table (it always has literal-ip hosts
		// of both families), but a pass must degrade to asking fewer
		// questions, never to asking none while a fallback exists
		targets = probeFallbackLiteralTargetsForIpVersion(ipVersion)
	}

	return self.probeExit(client, targets, timeout, resolvedCount)
}

// proberSweepTally collects one sweep's per-provider outcomes so the sweep
// can report itself as a whole. Correlated failure is the signal it exists
// for: every provider is reached through the phone's own uplink, so a device
// that slept or changed networks fails every probe at once -- a fact about
// the phone that is indistinguishable, provider by provider, from a bad pool.
//
// `silent` counts passes that asked nothing (no target could be built, or the
// transport refused every packet), a third outcome the pass/fail split would
// otherwise hide inside `failed`.
type proberSweepTally struct {
	lock      sync.Mutex
	scheduled int
	done      int
	passed    int
	failed    int
	silent    int
	// reported latches the one line. `done == scheduled` alone would fire
	// again on any extra landing, and one sweep reporting twice would read
	// as two sweeps in a capture -- exactly the kind of miscount that makes
	// a correlation signal untrustworthy.
	reported bool
}

// proberSweepLine is one finished sweep's counts.
type proberSweepLine struct {
	scheduled int
	passed    int
	failed    int
	silent    int
}

// record folds one pass's result in and reports the line when the sweep is
// complete. Returns ok exactly once per sweep, on the last pass to finish --
// so a sweep whose passes are still in flight never logs a partial verdict.
func (self *proberSweepTally) record(result probeResult) (proberSweepLine, bool) {
	self.lock.Lock()
	defer self.lock.Unlock()

	self.done += 1
	switch {
	case result.Sent <= 0:
		self.silent += 1
	case result.Passed:
		self.passed += 1
	default:
		self.failed += 1
	}
	if self.done < self.scheduled || self.reported {
		return proberSweepLine{}, false
	}
	self.reported = true
	return proberSweepLine{
		scheduled: self.scheduled,
		passed:    self.passed,
		failed:    self.failed,
		silent:    self.silent,
	}, true
}

// --- the prober loop ---

const (
	// proberScanInterval is the membership watch: how often the loop rescans
	// the windows for clients that need probing. A periodic scan rather than a
	// membership callback on purpose -- the windows expose no add/remove hook
	// today beyond the monitor's event stream (which is a ui feed with its own
	// coalescing), and inventing callback plumbing for a 5s-latency requirement
	// buys nothing but coupling. 5s is the "joiner probed within ~5s" bound.
	proberScanInterval = 5 * time.Second

	// proberConcurrency bounds in-flight probe passes. Each pass is a handful
	// of packets but up to ~2x ProbeTimeout of waiting; 4 keeps a cold-start
	// sweep of a full window inside a few scan intervals without ever having
	// more than a few kilobytes of probe traffic outstanding.
	proberConcurrency = 4

	// proberReprobeInterval is how often an idle exit with stale (or no)
	// qualification is re-asked. Half of these passes advance the rotation
	// over the table; loaded exits never wait on this -- their own receive
	// progress refreshes them for free (see touchQualificationOnReceive).
	proberReprobeInterval = 10 * time.Minute

	// proberAttemptMinInterval is the per-exit floor between pass ATTEMPTS,
	// keyed on the attempt rather than the recorded pass: a pass that sends
	// nothing (transport refusing, every registration colliding) records
	// nothing in the qualification table by design, and without this floor the
	// scan would retry such an exit every 5s forever.
	proberAttemptMinInterval = time.Minute
)

// proberCandidate is what the planner knows about one window client. A plain
// value struct so the plan is a pure function of it -- see proberPlan.
type proberCandidate struct {
	// inFlight: a pass for this client is currently running
	inFlight bool
	// flowCount is the client's live flow count (the existing clientFlowCount
	// callback readout)
	flowCount int
	// lastAttemptAt is when the loop last STARTED a pass for this client
	// (zero: never this session)
	lastAttemptAt time.Time
	// lastProbeAt is when a pass last actually recorded for this client's
	// destination (zero: never; survives channel incarnations)
	lastProbeAt time.Time
	// qualifiedAt is when the destination last proved out (zero: never)
	qualifiedAt time.Time
}

// proberPlan decides which candidates get a probe pass this scan. Pure so the
// decision table is testable without windows, clocks, or goroutines; the loop
// is obligated to consult it (anchored by TestProberLoopConsultsPlan).
//
// The rules, in order:
//   - a pass already in flight is never doubled.
//   - attempts are floored at proberAttemptMinInterval per exit.
//   - fresh qualification needs nothing.
//   - never-probed exits are probed now: this one rule is both the startup
//     sweep (everything present at the first scan) and the joiner probe (a
//     new exit fails it on the first scan after it appears).
//   - otherwise only IDLE exits (flowCount 0) are re-probed, and only once
//     their record is proberReprobeInterval old. A loaded exit's traffic is
//     better evidence than any probe and refreshes its qualification for free.
func proberPlan(now time.Time, candidates []proberCandidate) []int {
	picks := []int{}
	for i, candidate := range candidates {
		if candidate.inFlight {
			continue
		}
		if !candidate.lastAttemptAt.IsZero() && now.Sub(candidate.lastAttemptAt) < proberAttemptMinInterval {
			continue
		}
		if !candidate.qualifiedAt.IsZero() && now.Sub(candidate.qualifiedAt) < QualificationMaxAge {
			continue
		}
		if candidate.lastProbeAt.IsZero() {
			picks = append(picks, i)
			continue
		}
		if 0 < candidate.flowCount {
			continue
		}
		if proberReprobeInterval <= now.Sub(candidate.lastProbeAt) {
			picks = append(picks, i)
		}
	}
	return picks
}

// runProber is the qualification loop: the startup sweep, the joiner probes,
// and the staleness re-probes, all falling out of one scan-and-plan cycle.
// Started by the constructor when ProviderProbe is on; the runtime toggle is
// honored per scan (off = the scan does nothing), and probeExit itself checks
// again, so a mid-pass toggle can cost at most one already-started pass.
//
// Lock discipline: the loop's own bookkeeping (in-flight and last-attempt
// maps) lives under a private leaf mutex held only for map reads/writes --
// never across clientFlowCount or qualificationSnapshot (both take the parent
// stateLock inside) and never across a probe pass (which waits on the
// network). The pass goroutines hold no lock at all while probing, which is
// the established F-1 rule.
func (self *RemoteUserNatMultiClient) runProber() {
	sem := make(chan struct{}, proberConcurrency)
	var stateMutex sync.Mutex
	inFlight := map[*multiClientChannel]bool{}
	lastAttempt := map[*multiClientChannel]time.Time{}

	for {
		select {
		case <-self.ctx.Done():
			return
		case <-time.After(proberScanInterval):
		}

		if !self.reliabilitySettings().ProviderProbe {
			continue
		}

		clients := []*multiClientChannel{}
		for _, window := range self.windows {
			clients = append(clients, window.unorderedClients()...)
		}

		// forget departed clients so the maps track the window, not history.
		// in-flight entries clear themselves when their pass ends (the pass
		// notices the dead client and returns quickly on its cancelled ctx).
		func() {
			stateMutex.Lock()
			defer stateMutex.Unlock()
			live := map[*multiClientChannel]bool{}
			for _, client := range clients {
				live[client] = true
			}
			for client := range lastAttempt {
				if !live[client] && !inFlight[client] {
					delete(lastAttempt, client)
				}
			}
		}()

		now := time.Now()
		candidates := make([]proberCandidate, len(clients))
		for i, client := range clients {
			// both reads take the parent stateLock inside; no other lock is
			// held here, per the clientFlowCount contract
			flowCount := self.clientFlowCount(client)
			lastProbeAt, qualifiedAt, _ := self.qualificationSnapshot(client.probeDestination())
			func() {
				stateMutex.Lock()
				defer stateMutex.Unlock()
				candidates[i] = proberCandidate{
					inFlight:      inFlight[client],
					flowCount:     flowCount,
					lastAttemptAt: lastAttempt[client],
					lastProbeAt:   lastProbeAt,
					qualifiedAt:   qualifiedAt,
				}
			}()
		}

		picks := proberPlan(now, candidates)
		if len(picks) == 0 {
			continue
		}

		// the one line per sweep at the default level; the per-provider
		// PASS/FAIL lines come from probeExit itself
		loggerOrDefault(self.log).Infof("%s\n", relEvent(
			"probe_sweep",
			"scheduled", len(picks),
			"exits", len(clients),
		))

		// the sweep RESULT is a separate line, emitted when the last pass
		// lands, and it is the one that answers "are these providers bad, or
		// was my phone away". A per-provider line cannot show CORRELATION,
		// and correlation is the whole discriminator: this vpn runs on a
		// device that sleeps and changes networks, so a sweep where every
		// provider failed at once while the tunnel was silent is ONE fact
		// about the phone -- indistinguishable, one line at a time, from a
		// pool of bad providers.
		sweepTally := &proberSweepTally{scheduled: len(picks)}

		for _, i := range picks {
			client := clients[i]
			func() {
				stateMutex.Lock()
				defer stateMutex.Unlock()
				inFlight[client] = true
				lastAttempt[client] = now
			}()
			go HandleError(func() {
				defer func() {
					stateMutex.Lock()
					defer stateMutex.Unlock()
					delete(inFlight, client)
				}()
				// the semaphore wait respects teardown: a closing parent (or a
				// dying exit) releases the sweep instead of queueing behind it
				select {
				case sem <- struct{}{}:
				case <-self.ctx.Done():
					return
				}
				defer func() {
					<-sem
				}()
				if client.IsDone() {
					return
				}
				result := self.probeProviderPass(client)
				if line, ok := sweepTally.record(result); ok {
					uplinkStale, _ := self.uplinkGate(time.Now())
					loggerOrDefault(self.log).Infof("%s\n", relEvent(
						"probe_sweep_result",
						"passed", line.passed,
						"failed", line.failed,
						"silent", line.silent,
						"scheduled", line.scheduled,
						// the phone-side context for the WHOLE sweep: a sweep
						// that failed entirely while the tunnel was silent is
						// the device, not the pool
						"uplinkstale", uplinkStale,
					))
				}
			})
		}
	}
}

// --- qualification readouts ---

// qualificationSnapshot reads one destination's probe record: when a pass last
// recorded, when it last proved out, and how many passes have run (the
// rotation index). Zero times mean never; an unknown destination is all
// zeroes, which every caller treats as "unprobed", never "bad".
func (self *RemoteUserNatMultiClient) qualificationSnapshot(destination MultiHopId) (lastProbeAt time.Time, qualifiedAt time.Time, passIndex int) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	entry, ok := self.qualification[destination]
	if !ok {
		return
	}
	return entry.lastProbeAt, entry.qualifiedAt, entry.passed + entry.failed
}

// qualificationExitInfo is the per-exit readout for ExitInfo: whether the
// provider currently counts as proven, and how old the proof is (-1: never
// proven). Age is measured from the most recent proof -- a probe pass or the
// receive-refresh -- so a loaded exit shows a young age even between probes.
func (self *RemoteUserNatMultiClient) qualificationExitInfo(destination MultiHopId) (proven bool, probeAge time.Duration) {
	_, qualifiedAt, _ := self.qualificationSnapshot(destination)
	if qualifiedAt.IsZero() {
		return false, -1
	}
	probeAge = time.Since(qualifiedAt)
	return probeAge < QualificationMaxAge, probeAge
}

// --- the receive refresh ---

// qualificationReceiveRefreshInterval bounds how often live receive progress
// re-stamps a provider's qualification. Well under QualificationMaxAge, so an
// exit that is actually delivering can never go stale (and never wastes a
// re-probe); long enough that the refresh -- which takes the parent stateLock
// -- runs a handful of times an hour instead of per packet.
const qualificationReceiveRefreshInterval = 5 * time.Minute

// touchQualificationOnReceive refreshes this exit's qualification from real
// receive progress. Called on every addReceiveAck, so the common case must be
// near-free: one atomic load and a time comparison, no locks, no settings
// read. Only when the interval has elapsed (first ack ever, or 5 minutes since
// the last refresh) does it CAS the stamp and pay for the settings read and
// the parent-lock record.
//
// The refresh IS recordProbePass: a receive-ack through this exit is the same
// fact a probe answer proves -- the provider delivered from a real destination
// -- arrived at by better evidence (user traffic, not a canary). That also
// means a never-probed loaded exit is qualified by its own traffic, which is
// the F-2 rule "loaded exits are proven by their traffic", and the
// providersQualified transition metric stays honest.
//
// Lock discipline: called with NO lock held (addReceiveAck releases its own
// lock first) because qualificationRefreshFunc takes the parent stateLock,
// which must never nest inside a channel lock.
func (self *multiClientChannel) touchQualificationOnReceive() {
	if self == nil || self.qualificationRefreshFunc == nil {
		return
	}
	now := time.Now().UnixNano()
	last := self.qualificationRefreshedNanos.Load()
	if last != 0 && now-last < int64(qualificationReceiveRefreshInterval) {
		return
	}
	if !self.qualificationRefreshedNanos.CompareAndSwap(last, now) {
		// another packet on another goroutine won the refresh
		return
	}
	// the settings read allocates when no override is installed, which is why
	// it sits behind the interval gate rather than on the per-packet path
	if !self.reliabilitySettings().ProviderProbe {
		return
	}
	self.qualificationRefreshFunc(self.probeDestination())
}

// --- pooling admit selection ---

// poolAdmitOrder ranks evaluated-but-unadmitted expand candidates for
// admission: qualified providers first, arrival order preserved within each
// class, truncated to count. Pure so the preference is testable by itself;
// expand is obligated to route every admission through it (anchored by
// TestPoolExpandSourceAnchor).
//
// Prefer-qualified is best-effort by design: at a cold start nothing is
// qualified yet and this degrades to plain arrival order (fastest ping first,
// itself a defensible filter), while mid-session a re-expand meets a
// qualification table warmed by the sweep -- a destination probed through an
// earlier channel incarnation counts for its new candidate. The ongoing
// steering after admission belongs to the effectiveTier demerit, not to this
// one-shot choice.
func poolAdmitOrder(qualified []bool, count int) []int {
	if count <= 0 || len(qualified) == 0 {
		return nil
	}
	order := []int{}
	for i, isQualified := range qualified {
		if isQualified {
			order = append(order, i)
		}
	}
	for i, isQualified := range qualified {
		if !isQualified {
			order = append(order, i)
		}
	}
	if count < len(order) {
		order = order[:count]
	}
	return order
}
