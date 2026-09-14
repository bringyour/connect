package connect

import (
	"context"
	"encoding/binary"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// --- dns answer parsing ---

// dnsTestAnswer builds a dns response for name with the given answer records,
// using the same question encoding the query builder uses -- so the parser is
// tested against the wire form the prober actually produces and receives.
type dnsTestRecord struct {
	// compressed: name as a pointer to the question (the common resolver form)
	recordType  uint16
	recordClass uint16
	rdata       []byte
}

func dnsTestAnswer(t *testing.T, id uint16, name string, flags uint16, records []dnsTestRecord) []byte {
	t.Helper()
	question, ok := dnsQuestion(name)
	if !ok {
		t.Fatalf("could not encode question for %q", name)
	}
	payload := make([]byte, 12, 12+len(question)+16*len(records))
	binary.BigEndian.PutUint16(payload[0:2], id)
	binary.BigEndian.PutUint16(payload[2:4], flags)
	binary.BigEndian.PutUint16(payload[4:6], 1)
	binary.BigEndian.PutUint16(payload[6:8], uint16(len(records)))
	payload = append(payload, question...)
	for _, record := range records {
		// name: compression pointer to offset 12 (the question name)
		payload = append(payload, 0xC0, 0x0C)
		payload = binary.BigEndian.AppendUint16(payload, record.recordType)
		payload = binary.BigEndian.AppendUint16(payload, record.recordClass)
		// ttl
		payload = append(payload, 0, 0, 0, 60)
		payload = binary.BigEndian.AppendUint16(payload, uint16(len(record.rdata)))
		payload = append(payload, record.rdata...)
	}
	return payload
}

func dnsTestARecord(ip net.IP) dnsTestRecord {
	return dnsTestRecord{recordType: 1, recordClass: 1, rdata: ip.To4()}
}

// The parser's decision table: it must read exactly the A records out of a
// well-formed response, ignore record types it does not want, and treat every
// malformed shape as no answer -- never as a partial one.
func TestDnsAResponseParser(t *testing.T) {
	const id = 0x5150
	name := "www.example.com"

	// a single A record
	answer := dnsTestAnswer(t, id, name, 0x8180, []dnsTestRecord{
		dnsTestARecord(net.IPv4(93, 184, 216, 34)),
	})
	ips, ok := parseDnsAResponse(answer, id)
	if !ok || len(ips) != 1 || !ips[0].Equal(net.IPv4(93, 184, 216, 34)) {
		t.Errorf("single A: ips=%v ok=%v", ips, ok)
	}

	// multiple A records mixed with CNAME and AAAA: the As come back in
	// order, everything else is skipped without error
	answer = dnsTestAnswer(t, id, name, 0x8180, []dnsTestRecord{
		{recordType: 5, recordClass: 1, rdata: []byte{3, 'w', 'w', 'w', 0xC0, 0x0C}},
		dnsTestARecord(net.IPv4(93, 184, 216, 34)),
		{recordType: 28, recordClass: 1, rdata: net.ParseIP("2606:2800:220:1::1").To16()},
		dnsTestARecord(net.IPv4(93, 184, 216, 35)),
	})
	ips, ok = parseDnsAResponse(answer, id)
	if !ok || len(ips) != 2 || !ips[0].Equal(net.IPv4(93, 184, 216, 34)) || !ips[1].Equal(net.IPv4(93, 184, 216, 35)) {
		t.Errorf("mixed records: ips=%v ok=%v, want the two As in order", ips, ok)
	}

	// a non-A-only response (AAAA): well-formed, zero records
	answer = dnsTestAnswer(t, id, name, 0x8180, []dnsTestRecord{
		{recordType: 28, recordClass: 1, rdata: net.ParseIP("2606:2800:220:1::1").To16()},
	})
	ips, ok = parseDnsAResponse(answer, id)
	if !ok || len(ips) != 0 {
		t.Errorf("aaaa only: ips=%v ok=%v, want ok with no records", ips, ok)
	}

	// NXDOMAIN: the resolver answered; the name yielded nothing
	answer = dnsTestAnswer(t, id, name, 0x8183, nil)
	ips, ok = parseDnsAResponse(answer, id)
	if !ok || len(ips) != 0 {
		t.Errorf("nxdomain: ips=%v ok=%v, want ok with no records", ips, ok)
	}

	// a query echoed back (QR unset) is not a response -- the shape a
	// middlebox or an echoing test harness produces
	query := dnsTestAnswer(t, id, name, 0x0100, nil)
	if _, ok = parseDnsAResponse(query, id); ok {
		t.Error("an echoed query parsed as a response")
	}

	// a foreign transaction id is not our answer
	answer = dnsTestAnswer(t, id+1, name, 0x8180, []dnsTestRecord{
		dnsTestARecord(net.IPv4(93, 184, 216, 34)),
	})
	if _, ok = parseDnsAResponse(answer, id); ok {
		t.Error("a foreign transaction id parsed as our answer")
	}

	// malformed shapes: truncated header, truncated question, truncated
	// rdata. Each must read as no answer, with no partial records.
	if _, ok = parseDnsAResponse([]byte{0, 1, 0x81}, id); ok {
		t.Error("a truncated header parsed")
	}
	whole := dnsTestAnswer(t, id, name, 0x8180, []dnsTestRecord{
		dnsTestARecord(net.IPv4(93, 184, 216, 34)),
	})
	for _, cut := range []int{13, len(whole) - 10, len(whole) - 2} {
		if ips, ok := parseDnsAResponse(whole[:cut], id); ok && 0 < len(ips) {
			t.Errorf("truncation at %d yielded records %v", cut, ips)
		}
	}
}

// --- resolution through the probed channel ---

// probeFlowsOfProtocol snapshots the registered probe flows matching an ip
// protocol, so the two stages of a pass can be observed separately.
func probeFlowsOfProtocol(parent *RemoteUserNatMultiClient, protocol IpProtocol) []*probeFlow {
	parent.stateLock.Lock()
	defer parent.stateLock.Unlock()
	probes := []*probeFlow{}
	for _, update := range parent.ip4PathUpdates {
		if update.isProbe() && update.probe.ipPath.Protocol == protocol {
			probes = append(probes, update.probe)
		}
	}
	for _, update := range parent.ip6PathUpdates {
		if update.isProbe() && update.probe.ipPath.Protocol == protocol {
			probes = append(probes, update.probe)
		}
	}
	return probes
}

func waitForProbeFlows(t *testing.T, parent *RemoteUserNatMultiClient, protocol IpProtocol, n int) []*probeFlow {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if probes := probeFlowsOfProtocol(parent, protocol); n <= len(probes) {
			return probes
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("did not observe %d probe flows of protocol %v", n, protocol)
	return nil
}

// The full F-2 pass against a cooperating provider: the resolution queries go
// to the sampled resolver THROUGH the channel, their answers resolve the
// sampled hostnames, tcp syns follow to exactly the answered addresses, and
// the pass qualifies the provider.
func TestProbeResolutionThroughChannel(t *testing.T) {
	parent, client, forwarded := probeTestParent(t)
	// narrowed from the full-table default so the fixture answers a handful of
	// queries rather than the whole list; width semantics have their own test
	parent.settings.ProbeSampleHostCount = 4

	// the pass's sample is deterministic from the destination and pass index,
	// so the test can predict what will be asked
	destination := client.probeDestination()
	hosts, resolver := sampleProbeTargets(probeSeedBase(destination), 4)
	// every sampled hostname is resolved; there is no truncation
	expectedNames := []string{}
	expectedLiterals := 0
	for _, host := range hosts {
		if net.ParseIP(host) != nil {
			expectedLiterals += 1
		} else {
			expectedNames = append(expectedNames, host)
		}
	}
	if len(expectedNames) == 0 {
		t.Fatal("the first sample block holds no hostnames; the fixture cannot exercise resolution")
	}

	resultCh := make(chan probeResult, 1)
	go func() {
		resultCh <- parent.probeProviderPass(client)
	}()

	// stage A: one A query per sampled hostname, udp/53, to the sampled
	// resolver, each a probe flow in the reserved source range
	dnsProbes := waitForProbeFlows(t, parent, IpProtocolUdp, len(expectedNames))
	answeredIps := map[string]net.IP{}
	for i, probe := range dnsProbes {
		if got := probe.ipPath.DestinationIp.String(); got != resolver {
			t.Errorf("resolution query went to %s, want the sampled resolver %s", got, resolver)
		}
		if !probe.target.CaptureAnswer {
			t.Error("a resolution query is not capture-marked; its answer would be dropped")
		}
		ip := net.IPv4(198, 51, 100, byte(10+i))
		answeredIps[probe.target.QueryName] = ip
		payload := dnsTestAnswer(t, uint16(probe.synSequence), probe.target.QueryName, 0x8180, []dnsTestRecord{
			dnsTestARecord(ip),
		})
		answerPacket := ipOosUdpPacket(probe.ipPath.Reverse(), payload)
		ingressPath, err := ParseIpPath(answerPacket)
		if err != nil {
			t.Fatal(err)
		}
		parent.clientReceivePacket(client, TransferPath{}, 0, TransportTypeUnknown, ingressPath, answerPacket)
	}

	// stage B: tcp syns to exactly the answered addresses (plus any literal
	// hosts the sample held). Answer each with a SynAck.
	tcpProbes := waitForProbeFlows(t, parent, IpProtocolTcp, len(expectedNames)+expectedLiterals)
	resolvedSeen := 0
	for _, probe := range tcpProbes {
		if expected, ok := answeredIps[probe.target.Host]; ok {
			if !probe.ipPath.DestinationIp.Equal(expected) {
				t.Errorf("tcp probe for %s dialed %s, want the resolved %s",
					probe.target.Host, probe.ipPath.DestinationIp, expected)
			}
			resolvedSeen += 1
		}
		ingressPath, packet := probeTestSynAck(t, probe.ipPath, probe.synSequence)
		parent.clientReceivePacket(client, TransferPath{}, 0, TransportTypeUnknown, ingressPath, packet)
	}
	if resolvedSeen != len(expectedNames) {
		t.Errorf("saw %d tcp probes for resolved names, want %d", resolvedSeen, len(expectedNames))
	}

	var result probeResult
	select {
	case result = <-resultCh:
	case <-time.After(10 * time.Second):
		t.Fatal("the pass did not complete after every probe was answered")
	}

	if !result.Passed {
		t.Errorf("a fully answered pass failed: %d/%d", result.Answered, result.Sent)
	}
	if result.Sent != len(expectedNames)+expectedLiterals {
		t.Errorf("stage B sent %d, want %d (resolved + literal)", result.Sent, len(expectedNames)+expectedLiterals)
	}
	if !parent.providerQualified(destination) {
		t.Error("a passed pass did not qualify the provider")
	}
	// the resolution queries count in the raw metrics alongside the pass's own
	wantSent := uint64(len(expectedNames) + result.Sent)
	if got := parent.reliabilityMetrics.probesSent.Load(); got != wantSent {
		t.Errorf("probesSent = %d, want %d (resolution + stage B)", got, wantSent)
	}
	if n := len(*forwarded); n != 0 {
		t.Errorf("%d probe packet(s) reached the application", n)
	}
}

// A silent resolver must not read as an unqualifiable provider: the pass falls
// back to the table's literal-ip targets, and answering those still qualifies.
func TestProbeResolverDownFallsBackToLiterals(t *testing.T) {
	parent, client, _ := probeTestParent(t)
	// short resolution stage so the fallback is reached quickly; long enough
	// that the fallback's own probes can be answered without racing the pass
	// deadline on a loaded ci machine
	parent.settings.ProbeTimeout = 1 * time.Second

	resultCh := make(chan probeResult, 1)
	go func() {
		resultCh <- parent.probeProviderPass(client)
	}()

	// the resolution stage registers its udp flows... and the test answers
	// nothing: the resolver is down
	waitForProbeFlows(t, parent, IpProtocolUdp, 1)

	// stage B arrives after the resolution deadline, carrying literal-ip
	// targets only
	literalTargets := map[string]bool{}
	for _, target := range probeFallbackLiteralTargets() {
		literalTargets[target.Host] = true
	}
	if len(literalTargets) < 3 {
		t.Fatalf("the table holds %d literal-ip hosts; the fallback needs several", len(literalTargets))
	}
	tcpProbes := waitForProbeFlows(t, parent, IpProtocolTcp, len(literalTargets))
	for _, probe := range tcpProbes {
		if !literalTargets[probe.target.Host] {
			t.Errorf("fallback pass probed %q, not a literal-ip host", probe.target.Host)
		}
		ingressPath, packet := probeTestSynAck(t, probe.ipPath, probe.synSequence)
		parent.clientReceivePacket(client, TransferPath{}, 0, TransportTypeUnknown, ingressPath, packet)
	}

	var result probeResult
	select {
	case result = <-resultCh:
	case <-time.After(10 * time.Second):
		t.Fatal("the fallback pass did not complete")
	}

	if !result.Passed {
		t.Errorf("the literal-only fallback pass failed: %d/%d", result.Answered, result.Sent)
	}
	if !parent.providerQualified(client.probeDestination()) {
		t.Error("a provider with a dead resolver could not be qualified: the fallback is broken")
	}
}

// --- the prober plan ---

// The planner's decision table, driven pure: which clients get probed given
// their qualification ages, flow counts, and attempt history.
func TestProberPlanTable(t *testing.T) {
	now := time.Now()
	cases := []struct {
		name      string
		candidate proberCandidate
		want      bool
	}{
		{"never probed: the startup sweep and the joiner probe",
			proberCandidate{}, true},
		{"in flight is never doubled",
			proberCandidate{inFlight: true}, false},
		{"attempt floor holds even when never recorded",
			proberCandidate{lastAttemptAt: now.Add(-proberAttemptMinInterval / 2)}, false},
		{"attempt floor releases",
			proberCandidate{lastAttemptAt: now.Add(-proberAttemptMinInterval - time.Second)}, true},
		{"fresh qualification needs nothing",
			proberCandidate{
				lastProbeAt: now.Add(-time.Minute),
				qualifiedAt: now.Add(-time.Minute),
			}, false},
		{"stale idle: re-probed past the reprobe interval",
			proberCandidate{
				lastProbeAt: now.Add(-proberReprobeInterval - time.Second),
				qualifiedAt: now.Add(-QualificationMaxAge - time.Second),
			}, true},
		{"stale but recently probed: waits out the interval",
			proberCandidate{
				lastProbeAt: now.Add(-time.Minute),
				qualifiedAt: now.Add(-QualificationMaxAge - time.Second),
			}, false},
		{"stale and LOADED: never re-probed, receive progress refreshes it",
			proberCandidate{
				flowCount:   3,
				lastProbeAt: now.Add(-proberReprobeInterval - time.Second),
				qualifiedAt: now.Add(-QualificationMaxAge - time.Second),
			}, false},
		{"failed before, idle, past the interval: asked again",
			proberCandidate{
				lastProbeAt: now.Add(-proberReprobeInterval - time.Second),
			}, true},
		{"a NEW loaded client is still swept: never probed wins over loaded",
			proberCandidate{flowCount: 5}, true},
	}
	for _, c := range cases {
		picks := proberPlan(now, []proberCandidate{c.candidate})
		if got := len(picks) == 1; got != c.want {
			t.Errorf("%s: probe=%v, want %v", c.name, got, c.want)
		}
	}

	// indexes come back aligned to the input
	picks := proberPlan(now, []proberCandidate{
		{},
		{inFlight: true},
		{},
	})
	if len(picks) != 2 || picks[0] != 0 || picks[1] != 2 {
		t.Errorf("picks = %v, want [0 2]", picks)
	}
}

// The loop must consult the plan and run passes through the bounded semaphore
// -- a planner that is correct but unconsulted probes nothing (or everything).
func TestProberLoopSourceAnchors(t *testing.T) {
	source, err := readSource("ip_remote_multi_client_prober.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *RemoteUserNatMultiClient) runProber()")
	if !ok {
		t.Fatal("could not find runProber")
	}
	for _, required := range []string{
		"proberPlan(",
		"self.probeProviderPass(",
		"proberConcurrency",
		"self.clientFlowCount(",
		"self.qualificationSnapshot(",
	} {
		if !strings.Contains(body, required) {
			t.Errorf("runProber does not contain %s: the loop is not consulting the machinery it exists for", required)
		}
	}

	// and the constructor starts it, gated on the setting
	mainSource, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	ctorBody, ok := functionBody(mainSource, "func NewRemoteUserNatMultiClient(")
	if !ok {
		t.Fatal("could not find NewRemoteUserNatMultiClient")
	}
	if !strings.Contains(ctorBody, "runProber") {
		t.Error("the constructor does not start the prober loop")
	}
	if !strings.Contains(ctorBody, "settings.ProviderProbe") {
		t.Error("the prober loop start is not gated on ProviderProbe")
	}
}

// --- the effectiveTier demerit ---

// The qualification demerit's decision table: +1 for unproven, gone when
// proven, gone when the kill switch is off, and -- the bare-fixture invariant
// every injected func here obeys -- absent entirely when the lookup is not
// wired.
func TestEffectiveTierUnprovenDemerit(t *testing.T) {
	qualifiedAs := func(qualified bool) func(MultiHopId) bool {
		return func(MultiHopId) bool { return qualified }
	}

	// unproven: +1
	unproven := effectiveTierTestChannel(0)
	unproven.providerQualifiedFunc = qualifiedAs(false)
	AssertEqual(t, unproven.effectiveTier(), 1)

	// proven: clean
	proven := effectiveTierTestChannel(0)
	proven.providerQualifiedFunc = qualifiedAs(true)
	AssertEqual(t, proven.effectiveTier(), 0)

	// nil func (bare fixture): no demerit, the probe machinery's absence must
	// never demote anyone
	bare := effectiveTierTestChannel(0)
	AssertEqual(t, bare.effectiveTier(), 0)

	// the kill switch removes the mechanism's every effect
	off := effectiveTierTestChannel(0)
	off.settings.ProviderProbe = false
	off.providerQualifiedFunc = qualifiedAs(false)
	AssertEqual(t, off.effectiveTier(), 0)

	// EffectiveTierSelection off is the static A/B point, demerit included
	static := effectiveTierTestChannel(0)
	static.settings.EffectiveTierSelection = false
	static.providerQualifiedFunc = qualifiedAs(false)
	AssertEqual(t, static.effectiveTier(), 0)

	// it stacks with the evidence demerits, one step behind a starved +2
	stacked := effectiveTierTestChannel(1)
	stacked.providerQualifiedFunc = qualifiedAs(false)
	starveChannel(stacked)
	AssertEqual(t, stacked.effectiveTier(), 4)

	// and the quantum is +1 on purpose: an unproven tier-0 ties a proven
	// tier-1 rather than falling behind it -- unqualified is a starting
	// state, not evidence of failure
	unprovenBest := effectiveTierTestChannel(0)
	unprovenBest.providerQualifiedFunc = qualifiedAs(false)
	provenNext := effectiveTierTestChannel(1)
	provenNext.providerQualifiedFunc = qualifiedAs(true)
	kept := minTierClients([]*multiClientChannel{unprovenBest, provenNext})
	if len(kept) != 2 {
		t.Fatalf("got %d clients, want both: unproven tier-0 must tie proven tier-1, not lose to it", len(kept))
	}
}

// --- the receive refresh ---

// Receive progress refreshes qualification through the atomic interval gate:
// the first ack refreshes, the packets after it do not, an aged stamp does,
// and the kill switch stops it.
func TestProbeReceiveRefreshOnAck(t *testing.T) {
	settings := DefaultMultiClientSettings()
	var refreshes atomic.Int64
	client := &multiClientChannel{
		settings:    settings,
		packetStats: &clientWindowStats{log: loggerOrDefault(settings.Log)},
		qualificationRefreshFunc: func(MultiHopId) {
			refreshes.Add(1)
		},
	}

	// first ack ever: refresh
	client.addReceiveAck(1440)
	AssertEqual(t, refreshes.Load(), int64(1))

	// inside the interval: the gate holds, whatever the traffic
	for i := 0; i < 100; i += 1 {
		client.addReceiveAck(1440)
	}
	AssertEqual(t, refreshes.Load(), int64(1))

	// past the interval: the next ack refreshes again
	client.qualificationRefreshedNanos.Store(
		time.Now().Add(-qualificationReceiveRefreshInterval - time.Second).UnixNano(),
	)
	client.addReceiveAck(1440)
	AssertEqual(t, refreshes.Load(), int64(2))

	// the kill switch: no refresh, however stale the stamp
	offSettings := DefaultMultiClientSettings()
	offSettings.ProviderProbe = false
	client.settings = offSettings
	client.qualificationRefreshedNanos.Store(0)
	client.addReceiveAck(1440)
	AssertEqual(t, refreshes.Load(), int64(2))

	// nil func (bare fixture): no panic -- pinned by every addReceiveAck
	// fixture in the suite, but cheap to say here too
	bare := &multiClientChannel{
		settings:    settings,
		packetStats: &clientWindowStats{log: loggerOrDefault(settings.Log)},
	}
	bare.addReceiveAck(1440)
}

// The refresh must be wired at addReceiveAck, outside its locked section --
// the parent lock the refresh takes must never nest inside the channel's.
func TestProbeReceiveRefreshSiteAnchor(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *multiClientChannel) addReceiveAck(")
	if !ok {
		t.Fatal("could not find addReceiveAck")
	}
	if !strings.Contains(body, "touchQualificationOnReceive()") {
		t.Error("addReceiveAck does not touch the qualification: loaded exits go stale and get re-probed for nothing")
	}
}

// --- qualification readouts ---

func TestQualificationSnapshotAndPassIndex(t *testing.T) {
	parent, client, _ := probeTestParent(t)
	destination := client.probeDestination()

	lastProbeAt, qualifiedAt, passIndex := parent.qualificationSnapshot(destination)
	if !lastProbeAt.IsZero() || !qualifiedAt.IsZero() || passIndex != 0 {
		t.Error("an unknown destination must snapshot as all zeroes")
	}

	parent.recordProbeFail(destination)
	lastProbeAt, qualifiedAt, passIndex = parent.qualificationSnapshot(destination)
	if lastProbeAt.IsZero() || !qualifiedAt.IsZero() || passIndex != 1 {
		t.Errorf("after a fail: lastProbeAt zero=%v qualifiedAt zero=%v passIndex=%d",
			lastProbeAt.IsZero(), qualifiedAt.IsZero(), passIndex)
	}

	parent.recordProbePass(destination)
	_, qualifiedAt, passIndex = parent.qualificationSnapshot(destination)
	if qualifiedAt.IsZero() || passIndex != 2 {
		t.Errorf("after a pass: qualifiedAt zero=%v passIndex=%d", qualifiedAt.IsZero(), passIndex)
	}
}

// Exits carries the proven state and the probe age, so the dev screen can
// show the chip.
func TestExitsReportProvenAndProbeAge(t *testing.T) {
	settings := DefaultMultiClientSettings()
	client := &multiClientChannel{
		ctx:      context.Background(),
		args:     &multiClientChannelArgs{DestinationStats: DestinationStats{Tier: 0}},
		settings: settings,
	}

	mc := &RemoteUserNatMultiClient{
		settings: settings,
		windows: map[WindowType]*multiClientWindow{
			WindowTypeQuality: &multiClientWindow{
				settings: settings,
				clients:  map[Id]*multiClientChannel{{}: client},
			},
		},
	}

	// never probed: not proven, age -1
	exits := mc.Exits()
	AssertEqual(t, len(exits), 1)
	AssertEqual(t, exits[0].Proven, false)
	AssertEqual(t, exits[0].ProbeAge, time.Duration(-1))

	// proven: the chip and a young age
	mc.recordProbePass(client.probeDestination())
	exits = mc.Exits()
	AssertEqual(t, exits[0].Proven, true)
	if exits[0].ProbeAge < 0 || QualificationMaxAge <= exits[0].ProbeAge {
		t.Errorf("ProbeAge = %s, want a young age", exits[0].ProbeAge)
	}

	// stale: the age survives, the chip does not
	func() {
		mc.stateLock.Lock()
		defer mc.stateLock.Unlock()
		mc.qualification[client.probeDestination()].qualifiedAt =
			time.Now().Add(-QualificationMaxAge - time.Minute)
	}()
	exits = mc.Exits()
	AssertEqual(t, exits[0].Proven, false)
	if exits[0].ProbeAge < QualificationMaxAge {
		t.Errorf("ProbeAge = %s, want past QualificationMaxAge", exits[0].ProbeAge)
	}
}

// --- pooling ---

// The admit chooser: qualified first, arrival order within each class, count
// respected.
func TestPoolAdmitOrder(t *testing.T) {
	// nothing qualified (the cold start): plain arrival order
	order := poolAdmitOrder([]bool{false, false, false}, 2)
	if len(order) != 2 || order[0] != 0 || order[1] != 1 {
		t.Errorf("cold start order = %v, want [0 1]", order)
	}

	// qualified candidates jump the line, stable within each class
	order = poolAdmitOrder([]bool{false, true, false, true}, 4)
	want := []int{1, 3, 0, 2}
	if len(order) != 4 {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}

	// the count truncates after the preference is applied
	order = poolAdmitOrder([]bool{false, false, true}, 1)
	if len(order) != 1 || order[0] != 2 {
		t.Errorf("count 1 order = %v, want [2]", order)
	}

	// degenerate inputs
	if order := poolAdmitOrder(nil, 3); order != nil {
		t.Errorf("nil input yielded %v", order)
	}
	if order := poolAdmitOrder([]bool{true}, 0); order != nil {
		t.Errorf("count 0 yielded %v", order)
	}
}

func TestMultiClientExpandPlanCapsConstructedClients(t *testing.T) {
	tests := []struct {
		name               string
		neededCount        int
		evaluationMultiple int
		fixedDestination   bool
		wantAdmit          int
		wantRequest        int
	}{
		{name: "none", neededCount: 0},
		{name: "single pooled", neededCount: 1, evaluationMultiple: 2, wantAdmit: 1, wantRequest: 2},
		{name: "small pooled surplus", neededCount: 3, evaluationMultiple: 2, wantAdmit: 3, wantRequest: 4},
		{name: "large cold start", neededCount: 12, evaluationMultiple: 2, wantAdmit: 4, wantRequest: 4},
		{name: "large low memory", neededCount: 12, evaluationMultiple: 1, wantAdmit: 4, wantRequest: 4},
		{name: "fixed destination", neededCount: 12, evaluationMultiple: 4, fixedDestination: true, wantAdmit: 4, wantRequest: 4},
		{name: "zero value multiple", neededCount: 2, wantAdmit: 2, wantRequest: 2},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			admitCount, requestCount := multiClientExpandPlan(
				test.neededCount,
				test.evaluationMultiple,
				test.fixedDestination,
			)
			if admitCount != test.wantAdmit || requestCount != test.wantRequest {
				t.Fatalf(
					"expand plan = (admit=%d, request=%d), want (%d, %d)",
					admitCount,
					requestCount,
					test.wantAdmit,
					test.wantRequest,
				)
			}
			if multiClientExpandStepMax < admitCount || multiClientExpandStepMax < requestCount {
				t.Fatalf("expand plan exceeded step max %d", multiClientExpandStepMax)
			}
		})
	}
}

// The expand wiring: the multiple applies to the candidate-request count and
// both constructed and admitted candidates pass through the four-client step
// plan; every admission routes through the pure chooser; the surplus is
// cancelled politely into the monitor's NotAdded terminal state; and the
// standing-reserve / hard-max size math stays outside pooling's reach.
func TestPoolExpandSourceAnchor(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *multiClientWindow) expand(")
	if !ok {
		t.Fatal("could not find expand")
	}
	for _, required := range []string{
		// one plan caps both full client constructions and admissions
		"admitBudget, requestCount := multiClientExpandPlan(",
		"EvaluationPoolMultiple",
		// every admission goes through the single pure chooser
		"poolAdmitOrder(",
		// the surplus terminal state
		"ProviderStateNotAdded",
		// fixed-destination generators skip the multiple
		"fixedDestination",
	} {
		if !strings.Contains(body, required) {
			t.Errorf("expand does not contain %q: the pooling contract is not anchored", required)
		}
	}
	if !strings.Contains(body, "for i := 0; i < requestCount; i += 1") {
		t.Error("expand's request loop no longer iterates the multiplied count")
	}

	// the resize size math still applies the standing reserve and the hard max
	// to admitted counts, untouched by pooling
	resizeBody, ok := functionBody(source, "func (self *multiClientWindow) resize()")
	if !ok {
		t.Fatal("could not find resize")
	}
	if !strings.Contains(resizeBody, "standingReserveTarget(") {
		t.Error("resize no longer applies the standing reserve")
	}
	if !strings.Contains(resizeBody, "WindowSizeHardMax") {
		t.Error("resize no longer bounds by WindowSizeHardMax")
	}
	if !strings.Contains(resizeBody, "plannedAdmitCount, _ := multiClientExpandPlan(") {
		t.Error("resize no longer reports the bounded expansion step")
	}
}

// --- settings ---

func TestReliabilitySettingsEvaluationPoolDefaults(t *testing.T) {
	settings := DefaultMultiClientSettings()
	// mainnet-aggressive default: evaluate double, admit the needed count
	AssertEqual(t, settings.EvaluationPoolMultiple, 2)
	AssertEqual(t, settings.ProviderProbe, true)

	// the round trip through the override type -- a missed field zeroes on
	// every settings write, silently turning the behavior off
	reliabilitySettings := ReliabilitySettingsFrom(settings)
	AssertEqual(t, reliabilitySettings.EvaluationPoolMultiple, settings.EvaluationPoolMultiple)
	AssertEqual(t, reliabilitySettings.ProviderProbe, settings.ProviderProbe)
	AssertEqual(t, reliabilitySettings.ProbeTimeout, settings.ProbeTimeout)

	// nil (the bare-fixture state): 0, which expand clamps to 1 -- today's
	// behavior, so fixtures see no pooling
	bare := ReliabilitySettingsFrom(nil)
	AssertEqual(t, bare.EvaluationPoolMultiple, 0)
}

// --- metrics ---

// The probe counters must survive into the snapshot the sdk mirrors.
func TestReliabilityMetricsProbeSnapshot(t *testing.T) {
	metrics := newReliabilityMetrics()
	metrics.probeSent()
	metrics.probeSent()
	metrics.probeAnswered()
	metrics.providerQualified()

	snapshot := metrics.snapshot()
	AssertEqual(t, snapshot.ProbesSent, uint64(2))
	AssertEqual(t, snapshot.ProbesAnswered, uint64(1))
	AssertEqual(t, snapshot.ProvidersQualified, uint64(1))

	metrics.reset()
	snapshot = metrics.snapshot()
	AssertEqual(t, snapshot.ProbesSent, uint64(0))
	AssertEqual(t, snapshot.ProbesAnswered, uint64(0))
	AssertEqual(t, snapshot.ProvidersQualified, uint64(0))
}

// The sweep tally exists to expose CORRELATION: every provider is probed
// through the phone's own uplink, so a device that slept fails the whole
// sweep at once, and that is one fact about the phone rather than N facts
// about providers. The line must therefore land exactly once, on the last
// pass, with an honest three-way split.
func TestProberSweepTallyReportsOnceWithThreeOutcomes(t *testing.T) {
	tally := &proberSweepTally{scheduled: 3}

	// a pass that asked nothing is neither a pass nor a failure
	if _, ok := tally.record(probeResult{Sent: 0}); ok {
		t.Error("the tally reported before the sweep finished")
	}
	if _, ok := tally.record(probeResult{Sent: 3, Answered: 3, Passed: true}); ok {
		t.Error("the tally reported before the sweep finished")
	}

	line, ok := tally.record(probeResult{Sent: 3, Answered: 0})
	if !ok {
		t.Fatal("the tally did not report on the last pass")
	}
	if line.scheduled != 3 || line.passed != 1 || line.failed != 1 || line.silent != 1 {
		t.Errorf("line = %+v, want scheduled 3 / passed 1 / failed 1 / silent 1", line)
	}

	// and never twice: a second landing must not emit another verdict for a
	// sweep that already reported
	if _, ok := tally.record(probeResult{Sent: 1, Passed: true}); ok {
		t.Error("the tally reported a second time for one sweep")
	}
}

// The all-failed-at-once shape the owner's hypothesis predicts.
func TestProberSweepTallyAllFailed(t *testing.T) {
	tally := &proberSweepTally{scheduled: 6}
	var line proberSweepLine
	var ok bool
	for range 6 {
		line, ok = tally.record(probeResult{Sent: 4, Answered: 0})
	}
	if !ok {
		t.Fatal("no report after every pass landed")
	}
	if line.failed != 6 || line.passed != 0 {
		t.Errorf("line = %+v, want all six failed", line)
	}
}

// --- per-family probing (IPV6.md B1) ---

func dnsTestAAAARecord(ip net.IP) dnsTestRecord {
	return dnsTestRecord{recordType: 28, recordClass: 1, rdata: ip.To16()}
}

// dnsTestAddress is a distinct address of the version for the i-th answer.
func dnsTestAddress(ipVersion int, i int) net.IP {
	if ipVersion == 6 {
		return net.ParseIP("2001:db8:ffff::" + string(rune('a'+i)))
	}
	return net.IPv4(198, 51, 100, byte(10+i))
}

func dnsTestAddressRecord(ipVersion int, ip net.IP) dnsTestRecord {
	if ipVersion == 6 {
		return dnsTestAAAARecord(ip)
	}
	return dnsTestARecord(ip)
}

// The family-aware parser keeps exactly the wanted family's address records,
// skips the other family's, and keeps the "malformed is silence" contract.
func TestDnsAddressResponseParser(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		const id = 0x6160
		name := "www.example.com"
		want := dnsTestAddress(ipVersion, 0)
		other := dnsTestAddress(10-ipVersion, 0)

		answer := dnsTestAnswer(t, id, name, 0x8180, []dnsTestRecord{
			dnsTestAddressRecord(10-ipVersion, other),
			dnsTestAddressRecord(ipVersion, want),
			// a cname in the chain is skipped like any other type
			{recordType: 5, recordClass: 1, rdata: []byte{3, 'w', 'w', 'w', 0}},
		})
		ips, ok := parseDnsAddressResponse(answer, id, ipVersion)
		if !ok || len(ips) != 1 || !ips[0].Equal(want) {
			t.Fatalf("v%d: parsed %v ok=%v, want [%v]", ipVersion, ips, ok, want)
		}
		if ipVersion == 6 && ips[0].To4() != nil {
			t.Fatalf("v6 parse yielded a v4 address %v", ips[0])
		}

		// the other family's parser sees only its own records
		ips, ok = parseDnsAddressResponse(answer, id, 10-ipVersion)
		if !ok || len(ips) != 1 || !ips[0].Equal(other) {
			t.Fatalf("v%d: other-family parse %v ok=%v, want [%v]", ipVersion, ips, ok, other)
		}

		// nxdomain: answered, no records
		nx := dnsTestAnswer(t, id, name, 0x8183, nil)
		if ips, ok := parseDnsAddressResponse(nx, id, ipVersion); !ok || len(ips) != 0 {
			t.Fatalf("v%d: nxdomain parsed as %v ok=%v", ipVersion, ips, ok)
		}
		// a foreign id is not our answer
		if _, ok := parseDnsAddressResponse(answer, id+1, ipVersion); ok {
			t.Fatalf("v%d: a foreign transaction id parsed as an answer", ipVersion)
		}
		// an echoed query (QR unset) is not an answer
		query := dnsTestAnswer(t, id, name, 0x0100, nil)
		if _, ok := parseDnsAddressResponse(query, id, ipVersion); ok {
			t.Fatalf("v%d: an echoed query parsed as an answer", ipVersion)
		}
		// truncation never yields a partial answer
		for _, cut := range []int{13, len(answer) - 20, len(answer) - 2} {
			if ips, ok := parseDnsAddressResponse(answer[:cut], id, ipVersion); ok && 0 < len(ips) {
				t.Errorf("v%d: truncation at %d yielded records %v", ipVersion, cut, ips)
			}
		}
		// a record of the wanted type with the wrong rdata length is skipped
		short := dnsTestAnswer(t, id, name, 0x8180, []dnsTestRecord{
			{recordType: probeDnsRecordTypeForIpVersion(ipVersion), recordClass: 1, rdata: []byte{1, 2, 3}},
		})
		if ips, ok := parseDnsAddressResponse(short, id, ipVersion); !ok || len(ips) != 0 {
			t.Fatalf("v%d: a malformed rdata length parsed as %v ok=%v", ipVersion, ips, ok)
		}
	})
}

// The family a pass asks over follows the exit's category: v4-capable
// categories over v4, v6-only over v6, dualstack alternating by seed.
func TestProbeIpVersionForFamily(t *testing.T) {
	for seed := uint64(0); seed < 8; seed += 1 {
		for _, family := range []IpFamily{IpFamilyLegacy, IpFamilyV4Only, IpFamily("something-newer")} {
			if got := probeIpVersionForFamily(seed, family); got != 4 {
				t.Fatalf("seed %d %q -> v%d, want v4", seed, family, got)
			}
		}
		if got := probeIpVersionForFamily(seed, IpFamilyV6Only); got != 6 {
			t.Fatalf("seed %d v6-only -> v%d, want v6", seed, got)
		}
		want := 4
		if seed%2 == 1 {
			want = 6
		}
		if got := probeIpVersionForFamily(seed, IpFamilyDualstack); got != want {
			t.Fatalf("seed %d dualstack -> v%d, want v%d", seed, got, want)
		}
		// the sampler's resolver agrees with the version choice
		_, resolver := sampleProbeTargetsForIpFamily(seed, 4, IpFamilyDualstack)
		if resolverIp := net.ParseIP(resolver); (resolverIp.To4() == nil) != (want == 6) {
			t.Fatalf("seed %d dualstack resolver %s does not match v%d", seed, resolver, want)
		}
	}
}

// Literal hosts and the resolver-down fallback per family: over v4 the
// table's v4 literals, over v6 their siblings, every one a native address of
// the pass's family, and hostnames are never literals.
func TestProbeLiteralHostsByFamily(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		targets := probeFallbackLiteralTargetsForIpVersion(ipVersion)
		if len(targets) < 3 {
			t.Fatalf("v%d: %d fallback literals, the fallback needs several", ipVersion, len(targets))
		}
		seen := map[string]bool{}
		for _, target := range targets {
			if (target.Ip.To4() != nil) != (ipVersion == 4) {
				t.Fatalf("v%d: fallback literal %s (%s) is the wrong family", ipVersion, target.Host, target.Ip)
			}
			if target.Port != 443 || target.Class != probeClassHealth {
				t.Fatalf("v%d: fallback literal %s is not a :443 health target", ipVersion, target.Host)
			}
			if seen[target.Ip.String()] {
				t.Fatalf("v%d: fallback literal %s repeats", ipVersion, target.Ip)
			}
			seen[target.Ip.String()] = true
			// the target keeps the sampled host as its name so a field
			// report names the table entry, whichever family answered
			if net.ParseIP(target.Host) == nil {
				t.Fatalf("v%d: fallback target host %q is not the table's literal", ipVersion, target.Host)
			}
		}
		if ipVersion == 4 {
			// the v4 form is exactly what it was
			if len(targets) != len(probeFallbackLiteralTargets()) {
				t.Fatalf("v4 fallback changed: %d vs %d", len(targets), len(probeFallbackLiteralTargets()))
			}
		} else if len(targets) != len(probeHostLiteralIpv6s) {
			t.Fatalf("v6 fallback has %d targets, want one per sibling (%d)", len(targets), len(probeHostLiteralIpv6s))
		}
		if _, ok := probeLiteralHostIp("www.google.com", ipVersion); ok {
			t.Fatalf("v%d: a hostname read as a literal", ipVersion)
		}
	})
	// a v4 literal with no v6 sibling is neither dialable nor resolvable over v6
	if _, ok := probeLiteralHostIp("203.0.113.9", 6); ok {
		t.Fatal("a sibling-less v4 literal read as a v6 literal")
	}
	// a v6 literal stands for itself over v6 and is nothing over v4
	if ip, ok := probeLiteralHostIp("2001:db8::9", 6); !ok || ip.To4() != nil {
		t.Fatalf("a v6 literal over v6 = %v %v", ip, ok)
	}
	if _, ok := probeLiteralHostIp("2001:db8::9", 4); ok {
		t.Fatal("a v6 literal read as a v4 literal")
	}
}

// The full pass over each family: a v6-only exit is asked v6 questions --
// a v6 resolver, AAAA queries, v6 syns to the answered addresses -- and is
// qualified by them; a legacy exit keeps the v4 pass exactly as it was.
func TestProbeResolutionThroughChannelByFamily(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		parent, client, forwarded := probeTestParent(t)
		parent.settings.ProbeSampleHostCount = 4
		if ipVersion == 6 {
			client.args.DestinationStats.IpFamily = IpFamilyV6Only
		}

		destination := client.probeDestination()
		seed := probeSeedBase(destination)
		if got := probeIpVersionForFamily(seed, client.IpFamily()); got != ipVersion {
			t.Fatalf("the fixture's exit would be probed over v%d, want v%d", got, ipVersion)
		}
		hosts, resolver := sampleProbeTargetsForIpVersion(seed, 4, ipVersion)
		resolverIp := net.ParseIP(resolver)
		if (resolverIp.To4() != nil) != (ipVersion == 4) {
			t.Fatalf("sampled resolver %s is not a v%d address", resolver, ipVersion)
		}
		expectedNames := []string{}
		expectedLiterals := 0
		for _, host := range hosts {
			if _, ok := probeLiteralHostIp(host, ipVersion); ok {
				expectedLiterals += 1
			} else if net.ParseIP(host) == nil {
				expectedNames = append(expectedNames, host)
			}
		}
		if len(expectedNames) == 0 {
			t.Fatal("the first sample block holds no hostnames; the fixture cannot exercise resolution")
		}

		resultCh := make(chan probeResult, 1)
		go func() {
			resultCh <- parent.probeProviderPass(client)
		}()

		// stage A: one address query per sampled hostname, over the pass's
		// family, to the family's resolver, asking for the family's record
		dnsProbes := waitForProbeFlows(t, parent, IpProtocolUdp, len(expectedNames))
		answeredIps := map[string]net.IP{}
		for i, probe := range dnsProbes {
			if probe.ipPath.Version != ipVersion {
				t.Errorf("resolution query is v%d, want v%d", probe.ipPath.Version, ipVersion)
			}
			if !probe.ipPath.DestinationIp.Equal(resolverIp) {
				t.Errorf("resolution query went to %s, want the sampled resolver %s", probe.ipPath.DestinationIp, resolver)
			}
			query, ok := probePacket(probe.ipPath, probe.target, probe.synSequence)
			if !ok {
				t.Fatal("could not rebuild the resolution query")
			}
			queryPath, payload, err := ParseIpPathWithPayload(query)
			if err != nil || queryPath.Version != ipVersion {
				t.Fatalf("the resolution query does not parse as v%d: %v", ipVersion, err)
			}
			if qtype := binary.BigEndian.Uint16(payload[len(payload)-4 : len(payload)-2]); qtype != probeDnsRecordTypeForIpVersion(ipVersion) {
				t.Errorf("v%d resolution query asks qtype %d", ipVersion, qtype)
			}
			ip := dnsTestAddress(ipVersion, i)
			answeredIps[probe.target.QueryName] = ip
			answer := dnsTestAnswer(t, uint16(probe.synSequence), probe.target.QueryName, 0x8180, []dnsTestRecord{
				// the other family's record rides along and must be ignored
				dnsTestAddressRecord(10-ipVersion, dnsTestAddress(10-ipVersion, i)),
				dnsTestAddressRecord(ipVersion, ip),
			})
			answerPacket := ipOosUdpPacket(probe.ipPath.Reverse(), answer)
			ingressPath, err := ParseIpPath(answerPacket)
			if err != nil {
				t.Fatal(err)
			}
			parent.clientReceivePacket(client, TransferPath{}, 0, TransportTypeUnknown, ingressPath, answerPacket)
		}

		// stage B: syns of the pass's family to exactly the answered
		// addresses plus the family's literal hosts
		tcpProbes := waitForProbeFlows(t, parent, IpProtocolTcp, len(expectedNames)+expectedLiterals)
		resolvedSeen := 0
		for _, probe := range tcpProbes {
			if probe.ipPath.Version != ipVersion {
				t.Errorf("tcp probe for %s is v%d, want v%d", probe.target.Host, probe.ipPath.Version, ipVersion)
			}
			if (probe.ipPath.DestinationIp.To4() != nil) != (ipVersion == 4) {
				t.Errorf("tcp probe for %s dialed %s, the wrong family", probe.target.Host, probe.ipPath.DestinationIp)
			}
			if expected, ok := answeredIps[probe.target.Host]; ok {
				if !probe.ipPath.DestinationIp.Equal(expected) {
					t.Errorf("tcp probe for %s dialed %s, want the resolved %s", probe.target.Host, probe.ipPath.DestinationIp, expected)
				}
				resolvedSeen += 1
			}
			syn, ok := probePacket(probe.ipPath, probe.target, probe.synSequence)
			if !ok {
				t.Fatal("could not rebuild the syn")
			}
			if synPath, err := ParseIpPath(syn); err != nil || synPath.Version != ipVersion || !synPath.Syn {
				t.Fatalf("the crafted syn does not parse as a v%d syn: %v", ipVersion, err)
			}
			ingressPath, packet := probeTestSynAck(t, probe.ipPath, probe.synSequence)
			parent.clientReceivePacket(client, TransferPath{}, 0, TransportTypeUnknown, ingressPath, packet)
		}
		if resolvedSeen != len(expectedNames) {
			t.Errorf("saw %d tcp probes for resolved names, want %d", resolvedSeen, len(expectedNames))
		}

		var result probeResult
		select {
		case result = <-resultCh:
		case <-time.After(10 * time.Second):
			t.Fatal("the pass did not complete after every probe was answered")
		}
		if !result.Passed {
			t.Errorf("a fully answered v%d pass failed: %d/%d", ipVersion, result.Answered, result.Sent)
		}
		if result.Sent != len(expectedNames)+expectedLiterals {
			t.Errorf("stage B sent %d, want %d (resolved + literal)", result.Sent, len(expectedNames)+expectedLiterals)
		}
		if !parent.providerQualified(destination) {
			t.Errorf("a passed v%d pass did not qualify the provider", ipVersion)
		}
		if n := len(*forwarded); n != 0 {
			t.Errorf("%d probe packet(s) reached the application", n)
		}
	})
}

// A dualstack exit is asked over alternate families on consecutive passes,
// and is proven by either: a v4 pass that qualified it stands when the next
// pass, over v6, goes unanswered.
func TestProbeDualstackExitAlternatesFamilies(t *testing.T) {
	parent, client, _ := probeTestParent(t)
	parent.settings.ProbeSampleHostCount = 2
	parent.settings.ProbeTimeout = 500 * time.Millisecond
	client.args.DestinationStats.IpFamily = IpFamilyDualstack
	destination := client.probeDestination()
	seed := probeSeedBase(destination)

	// pass 0: answered in full over whichever family the seed picks
	firstVersion := probeIpVersionForFamily(seed, IpFamilyDualstack)
	resultCh := make(chan probeResult, 1)
	go func() {
		resultCh <- parent.probeProviderPass(client)
	}()
	dnsProbes := waitForProbeFlows(t, parent, IpProtocolUdp, 1)
	for _, probe := range dnsProbes {
		if probe.ipPath.Version != firstVersion {
			t.Fatalf("pass 0 asked over v%d, want v%d", probe.ipPath.Version, firstVersion)
		}
	}
	// the resolver stays silent; the fallback literals of the same family
	// answer, which is enough to qualify
	tcpProbes := waitForProbeFlows(t, parent, IpProtocolTcp, len(probeFallbackLiteralTargetsForIpVersion(firstVersion)))
	for _, probe := range tcpProbes {
		if probe.ipPath.Version != firstVersion {
			t.Fatalf("pass 0 fallback dialed over v%d, want v%d", probe.ipPath.Version, firstVersion)
		}
		ingressPath, packet := probeTestSynAck(t, probe.ipPath, probe.synSequence)
		parent.clientReceivePacket(client, TransferPath{}, 0, TransportTypeUnknown, ingressPath, packet)
	}
	select {
	case result := <-resultCh:
		if !result.Passed {
			t.Fatalf("pass 0 failed: %d/%d", result.Answered, result.Sent)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("pass 0 did not complete")
	}
	if !parent.providerQualified(destination) {
		t.Fatal("pass 0 did not qualify the provider")
	}

	// pass 1: the other family, and nothing answers
	secondVersion := probeIpVersionForFamily(seed+1, IpFamilyDualstack)
	if secondVersion == firstVersion {
		t.Fatalf("consecutive dualstack passes both chose v%d", firstVersion)
	}
	go func() {
		resultCh <- parent.probeProviderPass(client)
	}()
	dnsProbes = waitForProbeFlows(t, parent, IpProtocolUdp, 1)
	for _, probe := range dnsProbes {
		if probe.ipPath.Version != secondVersion {
			t.Fatalf("pass 1 asked over v%d, want v%d", probe.ipPath.Version, secondVersion)
		}
	}
	select {
	case result := <-resultCh:
		if result.Passed {
			t.Fatal("an unanswered pass passed")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("pass 1 did not complete")
	}
	// proven by either family: the failed pass over the other family records
	// nothing against the standing qualification
	if !parent.providerQualified(destination) {
		t.Fatal("an unanswered pass over the other family un-qualified a dualstack exit")
	}
	if client.IpFamily() != IpFamilyDualstack {
		t.Fatalf("a failed probe pass changed the exit category to %q", client.IpFamily())
	}
	// and the third pass rotates back
	if got := probeIpVersionForFamily(seed+2, IpFamilyDualstack); got != firstVersion {
		t.Fatalf("pass 2 would ask over v%d, want v%d", got, firstVersion)
	}
}
