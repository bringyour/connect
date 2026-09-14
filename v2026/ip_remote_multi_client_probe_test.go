package connect

import (
	"context"
	"encoding/binary"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// probeTestParent builds a bare parent with the state the probe paths read --
// the path maps, the flow bookkeeping, the ingress plumbing -- plus one channel
// that accepts sends and never answers them. The channel is `stalled`, which is
// the fixture idiom for "the packet left and nothing came back": it is what a
// provider dropping probes looks like, and it exercises the send path all the
// way through the accounting seam without a transport.
//
// forwarded captures everything handed to the application, so "the tun never
// sees probe traffic" is a direct assertion rather than an inference.
func probeTestParent(t *testing.T) (
	parent *RemoteUserNatMultiClient,
	client *multiClientChannel,
	forwarded *[]*receivePacket,
) {
	t.Helper()

	settings := DefaultMultiClientSettings()

	captured := &[]*receivePacket{}
	parent = &RemoteUserNatMultiClient{
		ctx:                 context.Background(),
		log:                 DefaultLogger(),
		settings:            settings,
		securityPolicy:      DisableSecurityPolicy(),
		packetStatsCounters: &packetStatsCounters{},
		ip4PathUpdates:      map[Ip4Path]*multiClientChannelUpdate{},
		ip6PathUpdates:      map[Ip6Path]*multiClientChannelUpdate{},
		affinityIp4Paths:    map[Ip4Path]map[Ip4Path]time.Time{},
		affinityIp6Paths:    map[Ip6Path]map[Ip6Path]time.Time{},
		clientUpdates:       map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
		qualification:       map[MultiHopId]*providerQualification{},
		reliabilityMetrics:  newReliabilityMetrics(),
	}
	parent.SetReceivePacketCallback(func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		*captured = append(*captured, &receivePacket{
			Source:      source,
			ProvideMode: provideMode,
			IpPath:      ipPath,
			Packet:      packet,
		})
	})

	client = probeTestChannel(settings)

	return parent, client, captured
}

// probeTestChannel is a channel that accepts sends and never answers them,
// with the stats maps a real send path writes into -- so the ordinary send
// path can be exercised against it as the positive control.
func probeTestChannel(settings *MultiClientSettings) *multiClientChannel {
	client := &multiClientChannel{
		ctx:                       context.Background(),
		log:                       DefaultLogger(),
		args:                      &multiClientChannelArgs{},
		settings:                  settings,
		packetStats:               &clientWindowStats{log: DefaultLogger()},
		ip4DestinationSourceCount: map[Ip4Path]map[Ip4Path]int{},
		ip6DestinationSourceCount: map[Ip6Path]map[Ip6Path]int{},
	}
	client.stalled.Store(true)
	return client
}

func probeTestTarget() probeTarget {
	return probeHostTarget("example.com", net.ParseIP("93.184.216.34"))
}

// probeTestTargets makes n distinct health targets, so a pass has several
// independent destinations the way a real one does.
func probeTestTargets(n int) []probeTarget {
	targets := []probeTarget{}
	for i := 0; i < n; i += 1 {
		targets = append(targets, probeHostTarget(
			"probe-test.example",
			net.IPv4(93, 184, 216, byte(34+i)),
		))
	}
	return targets
}

// waitForProbeFlow polls the path maps for the registered probe flow. The
// registration happens inside probeExit on another goroutine, so the test has
// to observe it rather than construct it -- which also means the test drives
// exactly the state production builds.
func waitForProbeFlow(t *testing.T, parent *RemoteUserNatMultiClient) *multiClientChannelUpdate {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		update := func() *multiClientChannelUpdate {
			parent.stateLock.Lock()
			defer parent.stateLock.Unlock()
			for _, u := range parent.ip4PathUpdates {
				if u.isProbe() {
					return u
				}
			}
			return nil
		}()
		if update != nil {
			return update
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("no probe flow was registered")
	return nil
}

// probeTestSynAck builds the answer a provider would deliver for a probe syn,
// and parses it back the way the channel does -- so the test drives
// clientReceivePacket with exactly the (ipPath, packet) pair clientReceive
// produces, never a hand-built path that could drift from the wire form.
func probeTestSynAck(t *testing.T, egressIpPath *IpPath, sequenceNumber uint32) (*IpPath, []byte) {
	t.Helper()

	reverse := egressIpPath.Reverse()
	packet := ipOosTcpPacketSequence(reverse, tcpFlagSyn|tcpFlagAck, sequenceNumber, nil)
	ipPath, err := ParseIpPath(packet)
	if err != nil {
		t.Fatalf("could not parse the crafted syn-ack: %s", err)
	}
	return ipPath, packet
}

// --- lifecycle ---

// The core loop: a probe goes out, the SynAck comes back through the channel,
// the probe completes as answered, the pass passes, the provider is qualified
// -- and not one byte of it reaches the application.
func TestProbeSynAckCompletesAndIsNotForwarded(t *testing.T) {
	parent, client, forwarded := probeTestParent(t)

	resultCh := make(chan probeResult, 1)
	go func() {
		resultCh <- parent.probeExit(client, []probeTarget{probeTestTarget()}, 5*time.Second, 0)
	}()

	update := waitForProbeFlow(t, parent)
	egress := update.probe.ipPath
	ingressPath, packet := probeTestSynAck(t, egress, 0x5150)

	parent.clientReceivePacket(client, TransferPath{}, protocol.ProvideMode_Public, TransportTypeUnknown, ingressPath, packet)

	var result probeResult
	select {
	case result = <-resultCh:
	case <-time.After(5 * time.Second):
		t.Fatal("probeExit did not return after the answer landed")
	}

	if result.Sent != 1 || result.Answered != 1 {
		t.Errorf("probe result = %d/%d answered, want 1/1", result.Answered, result.Sent)
	}
	if !result.Passed {
		t.Error("a fully answered pass did not pass")
	}
	if n := len(*forwarded); n != 0 {
		t.Errorf("%d probe packet(s) reached the application; the tun must never see probe traffic", n)
	}

	// the answer is a proven upstream connect, which is the positive evidence
	// the design does want recorded
	if len(client.connectSuccessTimes) != 1 {
		t.Errorf("connectSuccessTimes = %d, want 1: an answered probe is a proven dial", len(client.connectSuccessTimes))
	}

	// and the provider is now qualified
	if !parent.providerQualified(client.probeDestination()) {
		t.Error("a passed probe did not qualify the provider")
	}
	if got := parent.reliabilityMetrics.probesSent.Load(); got != 1 {
		t.Errorf("probesSent = %d, want 1", got)
	}
	if got := parent.reliabilityMetrics.probesAnswered.Load(); got != 1 {
		t.Errorf("probesAnswered = %d, want 1", got)
	}
	if got := parent.reliabilityMetrics.providersQualified.Load(); got != 1 {
		t.Errorf("providersQualified = %d, want 1", got)
	}
}

// A RST is not an answer. A provider whose own upstream dial is refused
// answers with RST+ACK (classifyDialFailure -> dialFailureRst), so a probe that
// counted "any packet back" would qualify a provider that demonstrably could
// not connect -- the one error direction this design cannot afford, because a
// wrongly-qualified provider gets real traffic. The RST still ends the probe,
// and still records nothing against the exit.
func TestProbeRstIsNotAnAnswer(t *testing.T) {
	parent, client, forwarded := probeTestParent(t)

	resultCh := make(chan probeResult, 1)
	go func() {
		resultCh <- parent.probeExit(client, []probeTarget{probeTestTarget()}, 30*time.Second, 0)
	}()

	update := waitForProbeFlow(t, parent)
	egress := update.probe.ipPath

	rstPacket, ok := ipOosRstSequence(egress.Reverse(), 0)
	if !ok {
		t.Fatal("could not build the rst")
	}
	ingressPath, err := ParseIpPath(rstPacket)
	if err != nil {
		t.Fatal(err)
	}
	parent.clientReceivePacket(client, TransferPath{}, protocol.ProvideMode_Public, TransportTypeUnknown, ingressPath, rstPacket)

	var result probeResult
	select {
	case result = <-resultCh:
	case <-time.After(10 * time.Second):
		t.Fatal("the rst did not complete the probe")
	}

	if result.Answered != 0 || result.Passed {
		t.Errorf("a rst counted as a probe answer: %d/%d answered, passed=%v", result.Answered, result.Sent, result.Passed)
	}
	if parent.providerQualified(client.probeDestination()) {
		t.Error("a refused dial qualified the provider")
	}
	if len(client.connectSuccessTimes) != 0 {
		t.Error("a rst recorded a proven connect")
	}
	// and, as ever, nothing punitive and nothing forwarded
	if got := client.dialFailureCount(); got != 0 {
		t.Errorf("dialFailureCount = %d, want 0", got)
	}
	if n := len(*forwarded); n != 0 {
		t.Errorf("%d packet(s) reached the application", n)
	}
}

// A dns probe passes on any datagram from the resolver: the provider had to
// carry the query out and the reply back for one to exist.
func TestProbeDnsAnswerPasses(t *testing.T) {
	parent, client, forwarded := probeTestParent(t)

	target := probeResolverTarget(net.ParseIP("8.8.8.8"), "www.example.com")
	resultCh := make(chan probeResult, 1)
	go func() {
		resultCh <- parent.probeExit(client, []probeTarget{target}, 30*time.Second, 0)
	}()

	update := waitForProbeFlow(t, parent)
	egress := update.probe.ipPath
	if egress.Protocol != IpProtocolUdp || egress.DestinationPort != 53 {
		t.Fatalf("dns probe registered as %v:%d", egress.Protocol, egress.DestinationPort)
	}

	answer := ipOosUdpPacket(egress.Reverse(), []byte{0, 1, 0x81, 0x80, 0, 1, 0, 1, 0, 0, 0, 0})
	ingressPath, err := ParseIpPath(answer)
	if err != nil {
		t.Fatal(err)
	}
	parent.clientReceivePacket(client, TransferPath{}, protocol.ProvideMode_Public, TransportTypeUnknown, ingressPath, answer)

	var result probeResult
	select {
	case result = <-resultCh:
	case <-time.After(10 * time.Second):
		t.Fatal("the dns answer did not complete the probe")
	}

	if result.Answered != 1 || !result.Passed {
		t.Errorf("dns probe result = %d/%d answered, passed=%v", result.Answered, result.Sent, result.Passed)
	}
	if !parent.providerQualified(client.probeDestination()) {
		t.Error("a passed dns probe did not qualify the provider")
	}
	if n := len(*forwarded); n != 0 {
		t.Errorf("%d packet(s) reached the application", n)
	}
}

// Cleanup is explicit, not the idle reaper's job: when the pass ends the path
// maps are clean, and nothing was ever recorded in the flow bookkeeping the
// caps, the blast radius and the drain weight all read.
func TestProbeLeavesNoFlowState(t *testing.T) {
	parent, client, forwarded := probeTestParent(t)

	result := parent.probeExit(client, probeTestTargets(3), 50*time.Millisecond, 0)
	if result.Sent != 3 {
		t.Fatalf("Sent = %d, want 3", result.Sent)
	}

	parent.stateLock.Lock()
	defer parent.stateLock.Unlock()

	if n := len(parent.ip4PathUpdates); n != 0 {
		t.Errorf("%d probe update(s) left in the path map after the pass", n)
	}
	if n := len(parent.clientUpdates); n != 0 {
		t.Errorf("probe flows entered clientUpdates (%d client(s)): they would count against the flow cap and the blast radius", n)
	}
	if n := len(parent.affinityIp4Paths); n != 0 {
		t.Errorf("probe flows joined %d affinity group(s): a probe must never pin a site to an exit", n)
	}
	if got := parent.reliabilityMetrics.flowsOpened.Load(); got != 0 {
		t.Errorf("flowsOpened = %d, want 0: a probe is not a user flow", got)
	}
	if n := len(*forwarded); n != 0 {
		t.Errorf("%d packet(s) reached the application", n)
	}
}

// A late answer -- one that arrives after the pass cleaned up, e.g. a
// retransmitted SynAck -- is consumed and dropped. It must never fall through
// to the "not in response to outgoing traffic" branch and be handed to the tun.
func TestProbeLateAnswerIsConsumedNotForwarded(t *testing.T) {
	parent, client, forwarded := probeTestParent(t)

	// run and finish a pass, then replay an answer for its (now unregistered)
	// flow key
	target := probeTestTarget()
	parent.probeExit(client, []probeTarget{target}, 20*time.Millisecond, 0)

	egress := &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        probeSourceIp4,
		SourcePort:      probeSourcePortMin,
		DestinationIp:   target.Ip,
		DestinationPort: target.Port,
	}
	ingressPath, packet := probeTestSynAck(t, egress, 0x5150)
	parent.clientReceivePacket(client, TransferPath{}, protocol.ProvideMode_Public, TransportTypeUnknown, ingressPath, packet)

	if n := len(*forwarded); n != 0 {
		t.Errorf("a late probe answer was forwarded to the application (%d packet(s))", n)
	}
}

// --- the no-convict pin (the cping lesson) ---

// THE PIN. A full probe pass against an exit that answers nothing must leave
// the exit exactly as it found it: no dial strikes, no window-stats evidence,
// no derivable verdict. This is the class of self-inflicted evidence the whole
// design exists to forbid -- probing an idle spare against sites that drop
// datacenter ip ranges (which is routine) would otherwise walk it straight into
// a no-receive-syn conviction, with the client as the sole author of the case
// against it.
//
// The positive control at the end proves the counters can move at all, so a
// future refactor that silently stops recording anything cannot pass this test
// by accident.
func TestProbeFailureConvictsNothing(t *testing.T) {
	parent, client, forwarded := probeTestParent(t)

	targets := probeTestTargets(5)
	result := parent.probeExit(client, targets, 50*time.Millisecond, 0)

	if result.Sent != 5 {
		t.Fatalf("Sent = %d, want 5 (the fixture accepts every send)", result.Sent)
	}
	if result.Answered != 0 || result.Passed {
		t.Fatalf("result = %d/%d answered, passed=%v; want a failed pass", result.Answered, result.Sent, result.Passed)
	}

	// no strikes -> no dialStarved -> no effectiveTier demerit
	if got := client.dialFailureCount(); got != 0 {
		t.Errorf("dialFailureCount = %d, want 0: a probe failure must never strike an exit", got)
	}
	if client.dialStarved() {
		t.Error("a probe pass starved the exit")
	}

	// no window stats at all: these are the verdict inputs
	stats, err := client.WindowStats()
	if err != nil {
		t.Fatal(err)
	}
	if stats.sendNackCount != 0 {
		t.Errorf("sendNackCount = %d, want 0: probe sends must not register as outstanding", stats.sendNackCount)
	}
	if stats.sendSynCount != 0 {
		t.Errorf("sendSynCount = %d, want 0: an unanswered probe syn is exactly the no-receive-syn evidence", stats.sendSynCount)
	}
	if stats.sendAckCount != 0 || stats.receiveAckCount != 0 || stats.receiveSynCount != 0 {
		t.Errorf("probe traffic moved the ack counters: send %d, receive %d/%d",
			stats.sendAckCount, stats.receiveAckCount, stats.receiveSynCount)
	}
	if stats.sendDestinationCount != 0 {
		t.Errorf("sendDestinationCount = %d, want 0: probe destinations must not corroborate a no-receive-ack verdict", stats.sendDestinationCount)
	}
	if !stats.firstSendNackTime.IsZero() || !stats.firstSendSynTime.IsZero() {
		t.Error("a probe started a verdict clock")
	}

	// and therefore no verdict is derivable from the evidence at any bound --
	// asked of the shipped decision function rather than restated here
	for _, bound := range []time.Duration{0, time.Nanosecond, time.Second} {
		reason, held := blackholeReasonFromStats(
			time.Now().Add(time.Hour),
			stats,
			bound,
			bound,
			bound,
			blackholeGates{},
		)
		if reason != blackholeNone || held != blackholeNone {
			t.Errorf("a probe pass produced a verdict at bound %s: reason=%q held=%q", bound, reason, held)
		}
	}

	// the exit's channel-level state is untouched
	if client.isWarning() || client.isQuarantined() {
		t.Error("a probe pass warned or quarantined the exit")
	}
	if got := client.effectiveTier(); got != client.Tier() {
		t.Errorf("effectiveTier = %d, static Tier = %d: a probe pass demoted the exit", got, client.Tier())
	}

	// nothing reached the application, and no failure metric exists to have
	// been incremented
	if n := len(*forwarded); n != 0 {
		t.Errorf("%d packet(s) reached the application", n)
	}
	if got := parent.reliabilityMetrics.dialFailures.Load(); got != 0 {
		t.Errorf("dialFailures = %d, want 0", got)
	}

	// the provider is simply not qualified -- never "bad"
	if parent.providerQualified(client.probeDestination()) {
		t.Error("a failed pass qualified the provider")
	}

	// POSITIVE CONTROL: the same packet through the ordinary send path DOES
	// record, so the assertions above are about the probe seam and not about a
	// fixture that records nothing.
	control := probeTestChannel(DefaultMultiClientSettings())
	controlPath := &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        probeSourceIp4,
		SourcePort:      probeSourcePortMin,
		DestinationIp:   targets[0].Ip,
		DestinationPort: targets[0].Port,
		Syn:             true,
	}
	control.SendDetailed(&parsedPacket{
		packet: probeSynPacket(controlPath, 1),
		ipPath: controlPath,
	}, 0)
	controlStats, err := control.WindowStats()
	if err != nil {
		t.Fatal(err)
	}
	if controlStats.sendNackCount != 1 || controlStats.sendSynCount != 1 {
		t.Fatalf("positive control: sendNackCount=%d sendSynCount=%d, want 1/1 -- the counters must be able to move",
			controlStats.sendNackCount, controlStats.sendSynCount)
	}
}

// A provider dial-failure signal naming a probe flow answers the probe and does
// nothing else: no strike, no re-race, no metric, no packet to the application
// -- in either position of the re-race knob.
func TestProbeDialFailureRecordsFailureWithoutStrike(t *testing.T) {
	for _, rerace := range []bool{true, false} {
		parent, client, forwarded := probeTestParent(t)
		parent.settings.DialFailureRerace = rerace

		resultCh := make(chan probeResult, 1)
		go func() {
			// a long timeout: the dial failure, not the clock, must be what
			// ends this pass
			resultCh <- parent.probeExit(client, []probeTarget{probeTestTarget()}, 30*time.Second, 0)
		}()

		update := waitForProbeFlow(t, parent)
		egress := update.probe.ipPath

		startTime := time.Now()
		if reraced := parent.clientDialFailure(client, egress); reraced {
			t.Errorf("rerace=%v: a probe flow reported as re-raced", rerace)
		}

		var result probeResult
		select {
		case result = <-resultCh:
		case <-time.After(10 * time.Second):
			t.Fatalf("rerace=%v: the dial failure did not complete the probe", rerace)
		}
		if 20*time.Second <= time.Since(startTime) {
			t.Errorf("rerace=%v: the probe waited out its timeout instead of recording the failure", rerace)
		}

		if result.Answered != 0 || result.Passed {
			t.Errorf("rerace=%v: a dial failure produced a passing probe", rerace)
		}
		if got := client.dialFailureCount(); got != 0 {
			t.Errorf("rerace=%v: dialFailureCount = %d, want 0: a probe's dial failure must not strike the exit", rerace, got)
		}
		if got := parent.reliabilityMetrics.dialFailures.Load(); got != 0 {
			t.Errorf("rerace=%v: dialFailures = %d, want 0: a probe's failure is not a measured event", rerace, got)
		}
		if got := parent.reliabilityMetrics.flowsReraced.Load(); got != 0 {
			t.Errorf("rerace=%v: flowsReraced = %d, want 0", rerace, got)
		}
		if n := len(*forwarded); n != 0 {
			t.Errorf("rerace=%v: %d packet(s) reached the application", rerace, n)
		}
	}
}

// A real flow's dial failure must still behave exactly as before -- the probe
// intercept sits in front of it, so this is the regression guard that it does
// not swallow ordinary signals.
func TestProbeDialFailureLeavesRealFlowsAlone(t *testing.T) {
	egress := icmpTcpTestPath(4)
	parent, client, update, forwarded := dialFailureTestParent(t, true, egress)

	parent.clientDialFailure(client, egress)

	if update.client.Load() != nil {
		t.Error("a real flow was no longer re-raced after the probe intercept was added")
	}
	if got := parent.reliabilityMetrics.dialFailures.Load(); got != 1 {
		t.Errorf("dialFailures = %d, want 1: real dial failures must still be counted", got)
	}
	if got := client.dialFailureCount(); got != 1 {
		t.Errorf("channel dialFailureCount = %d, want 1: real dial failures must still strike", got)
	}
	if n := len(*forwarded); n != 0 {
		t.Errorf("%d packet(s) forwarded, want 0", n)
	}
}

// --- exclusion anchors ---

// The exclusions are properties of specific call sites, and a refactor that
// moves them would re-arm the behavior silently. Pin them the way
// TestResizeWarnsOnDialStarved pins the resize warning.
func TestProbeExclusionCallSites(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	probeSource, err := readSource("ip_remote_multi_client_probe.go")
	if err != nil {
		t.Fatal(err)
	}

	// 1. the send seam records nothing
	sendBody, ok := functionBody(probeSource, "func (self *multiClientChannel) sendProbe(")
	if !ok {
		t.Fatal("could not find sendProbe")
	}
	for _, forbidden := range []string{
		"self.addSend(",
		"self.addSendAck(",
		"self.addSendNack(",
		"self.addSendSyn(",
		"self.addSendAbandoned(",
		"self.addError(",
		"self.addSource(",
	} {
		if strings.Contains(sendBody, forbidden) {
			t.Errorf("sendProbe calls %s: a probe send must not feed the window accounting", forbidden)
		}
	}
	if !strings.Contains(sendBody, "SendMultiHopWithTimeoutDetailed(") {
		t.Error("sendProbe no longer sends through the transport")
	}

	// 2. the ingress paths -- per-packet AND batch -- consume probe packets
	// before anything can forward them. The resolution/delivery tail lives in
	// clientReceivePacketResolve, which is only reachable after each entry's
	// gate; the batch fast path additionally delivers directly, so its gate
	// must precede the batching too (index order pins both).
	for _, entry := range []string{
		"func (self *RemoteUserNatMultiClient) clientReceivePacket(",
		"func (self *RemoteUserNatMultiClient) clientReceivePackets(",
	} {
		receiveBody, ok := functionBody(source, entry)
		if !ok {
			t.Fatalf("could not find %s", entry)
		}
		gate := strings.Index(receiveBody, "probeIngressPath(ipPath)")
		if gate < 0 {
			t.Fatalf("%s has no probe intercept: probe answers can reach the tun", entry)
		}
		if consume := strings.Index(receiveBody, "self.clientReceiveProbePacket("); consume < 0 || consume < gate {
			t.Errorf("%s: the probe intercept does not consume the packet", entry)
		}
		if resolve := strings.Index(receiveBody, "self.clientReceivePacketResolve("); resolve < 0 || resolve < gate {
			t.Errorf("%s: the probe intercept must sit before the flow resolution", entry)
		}
	}

	// 3. the dial-failure path handles probes first, ahead of every effect
	dialBody, ok := functionBody(source, "func (self *RemoteUserNatMultiClient) clientDialFailure(")
	if !ok {
		t.Fatal("could not find clientDialFailure")
	}
	intercept := strings.Index(dialBody, "self.probeDialFailure(")
	if intercept < 0 {
		t.Fatal("clientDialFailure has no probe intercept: a probe failure would strike the exit")
	}
	for _, effect := range []string{
		"dialFailureIntercepted()",
		"self.stampUplinkIngress()",
		"addDialFailure(",
		"flowReraced()",
	} {
		at := strings.Index(dialBody, effect)
		if at < 0 {
			t.Errorf("clientDialFailure no longer contains %s; the intercept ordering can no longer be checked", effect)
			continue
		}
		if at < intercept {
			t.Errorf("%s happens before the probe intercept: a probe failure would feed it", effect)
		}
	}

	// 4. probe registration does not touch the flow bookkeeping
	registerBody, ok := functionBody(probeSource, "func (self *RemoteUserNatMultiClient) registerProbeFlow(")
	if !ok {
		t.Fatal("could not find registerProbeFlow")
	}
	for _, forbidden := range []string{
		"self.clientUpdates[",
		"affinityIpPathsWithLock(",
		"flowOpened()",
		"bindClientFlow(",
	} {
		if strings.Contains(registerBody, forbidden) {
			t.Errorf("registerProbeFlow touches %s: probe flows must stay out of the flow bookkeeping", forbidden)
		}
	}
}

// --- packet crafting ---

// The crafted probes must be real packets. A syn with a bad checksum is
// dropped by the provider's own stack and every probe fails for a reason that
// has nothing to do with the provider -- the worst possible failure mode for a
// mechanism whose only output is "this provider works".
func TestProbePacketsAreWellFormed(t *testing.T) {
	for _, version := range []int{4, 6} {
		destination := net.ParseIP("93.184.216.34")
		if version == 6 {
			destination = net.ParseIP("2606:2800:220:1:248:1893:25c8:1946")
		}

		sourceIp, gotVersion, ok := probeSourceIpFor(destination)
		if !ok || gotVersion != version {
			t.Fatalf("v%d: probeSourceIpFor returned %v %d", version, ok, gotVersion)
		}

		synPath := &IpPath{
			Version:         version,
			Protocol:        IpProtocolTcp,
			SourceIp:        sourceIp,
			SourcePort:      probeSourcePortMin,
			DestinationIp:   destination,
			DestinationPort: 443,
		}
		syn := probeSynPacket(synPath, 0xabcd1234)
		parsed, err := ParseIpPath(syn)
		if err != nil {
			t.Fatalf("v%d: the crafted syn does not parse: %s", version, err)
		}
		if !parsed.Syn || parsed.Ack || parsed.Rst {
			t.Errorf("v%d: crafted syn flags syn=%v ack=%v rst=%v, want a pure syn", version, parsed.Syn, parsed.Ack, parsed.Rst)
		}
		if parsed.SourcePort != probeSourcePortMin || parsed.DestinationPort != 443 {
			t.Errorf("v%d: crafted syn ports %d->%d", version, parsed.SourcePort, parsed.DestinationPort)
		}
		if parsed.SequenceNumber != 0xabcd1234 {
			t.Errorf("v%d: crafted syn sequence = %#x", version, parsed.SequenceNumber)
		}
		assertProbeChecksums(t, version, syn, ipProtocolNumberTcp, synPath)

		// the courtesy reset closes the handshake at isn+1, which is what a
		// real stack sends to abort
		rst, ok := probeCourtesyRstPacket(synPath, 0xabcd1234)
		if !ok {
			t.Fatalf("v%d: could not build the courtesy rst", version)
		}
		parsedRst, err := ParseIpPath(rst)
		if err != nil {
			t.Fatalf("v%d: the crafted rst does not parse: %s", version, err)
		}
		if !parsedRst.Rst || parsedRst.Syn {
			t.Errorf("v%d: courtesy packet is not a pure rst", version)
		}
		if parsedRst.SequenceNumber != 0xabcd1235 {
			t.Errorf("v%d: courtesy rst sequence = %#x, want isn+1", version, parsedRst.SequenceNumber)
		}
		assertProbeChecksums(t, version, rst, ipProtocolNumberTcp, synPath)

		// the dns query
		dnsPath := &IpPath{
			Version:         version,
			Protocol:        IpProtocolUdp,
			SourceIp:        sourceIp,
			SourcePort:      probeSourcePortMin + 1,
			DestinationIp:   destination,
			DestinationPort: 53,
		}
		query, ok := probeDnsQueryPacket(dnsPath, "www.example.com", 0x4142)
		if !ok {
			t.Fatalf("v%d: could not build the dns query", version)
		}
		parsedQuery, payload, err := ParseIpPathWithPayload(query)
		if err != nil {
			t.Fatalf("v%d: the crafted dns query does not parse: %s", version, err)
		}
		if parsedQuery.DestinationPort != 53 || parsedQuery.Protocol != IpProtocolUdp {
			t.Errorf("v%d: dns query is not udp/53", version)
		}
		assertProbeChecksums(t, version, query, ipProtocolNumberUdp, dnsPath)

		if len(payload) < 12 {
			t.Fatalf("v%d: dns payload is %d bytes", version, len(payload))
		}
		if got := binary.BigEndian.Uint16(payload[0:2]); got != 0x4142 {
			t.Errorf("v%d: dns transaction id = %#x", version, got)
		}
		if got := binary.BigEndian.Uint16(payload[2:4]); got != 0x0100 {
			t.Errorf("v%d: dns flags = %#x, want a standard recursive query", version, got)
		}
		if got := binary.BigEndian.Uint16(payload[4:6]); got != 1 {
			t.Errorf("v%d: dns qdcount = %d, want 1", version, got)
		}
		// the query asks for an address the same pass can dial: A over v4,
		// AAAA (28) over v6
		wantQuestion := []byte{
			3, 'w', 'w', 'w',
			7, 'e', 'x', 'a', 'm', 'p', 'l', 'e',
			3, 'c', 'o', 'm',
			0,
			0, 1, 0, 1,
		}
		if version == 6 {
			wantQuestion[len(wantQuestion)-3] = 28
		}
		if got := payload[12:]; string(got) != string(wantQuestion) {
			t.Errorf("v%d: dns question = %v, want %v", version, got, wantQuestion)
		}
	}

	// names that cannot be encoded are refused rather than sent malformed
	badPath := &IpPath{
		Version:         4,
		Protocol:        IpProtocolUdp,
		SourceIp:        probeSourceIp4,
		SourcePort:      probeSourcePortMin,
		DestinationIp:   net.ParseIP("8.8.8.8"),
		DestinationPort: 53,
	}
	for _, name := range []string{"", ".", "a..b", strings.Repeat("x", 64) + ".com"} {
		if _, ok := probeDnsQueryPacket(badPath, name, 1); ok {
			t.Errorf("dns query built for an unencodable name %q", name)
		}
	}
}

// assertProbeChecksums verifies a crafted packet the way a receiving stack
// does: the ones-complement sum over a correct header, checksum field included,
// finishes at zero.
func assertProbeChecksums(t *testing.T, version int, packet []byte, ipProtocol ipProtocolNumber, ipPath *IpPath) {
	t.Helper()

	var transport []byte
	switch version {
	case 4:
		if got := checksumFinish(checksumAdd(0, packet[0:Ipv4HeaderSizeWithoutExtensions])); got != 0 {
			t.Errorf("v4: ip header checksum does not verify (%#x)", got)
		}
		transport = packet[Ipv4HeaderSizeWithoutExtensions:]
	case 6:
		transport = packet[Ipv6HeaderSize:]
	}
	if got := ipPathTransportChecksum(ipPath, ipProtocol, transport); got != 0 {
		t.Errorf("v%d: transport checksum does not verify (%#x)", version, got)
	}
}

// --- the sampler ---

// The sampler must be deterministic (two clients probing the same provider
// agree on what was asked, and a field report is reproducible) and must rotate
// (a provider whose upstream reaches some of the internet and not the rest is
// invisible to a probe that always asks the same four questions).
func TestProbeSamplerIsDeterministicAndRotates(t *testing.T) {
	// a compact width: rotation only matters when ProbeSampleHostCount narrows
	// the pass below the full-table default
	const width = 4
	hosts1, resolver1 := sampleProbeTargets(7, width)
	hosts2, resolver2 := sampleProbeTargets(7, width)
	if strings.Join(hosts1, ",") != strings.Join(hosts2, ",") || resolver1 != resolver2 {
		t.Error("the sampler is not deterministic for a fixed seed")
	}
	if len(hosts1) != width {
		t.Errorf("sampled %d hosts, want %d", len(hosts1), width)
	}
	if resolver1 == "" {
		t.Error("no resolver sampled")
	}

	// consecutive passes for one provider are disjoint
	seen := map[string]bool{}
	for _, host := range hosts1 {
		seen[host] = true
	}
	next, _ := sampleProbeTargets(8, width)
	for _, host := range next {
		if seen[host] {
			t.Errorf("consecutive passes repeat %q: rotation is not advancing a full block", host)
		}
	}

	// and the rotation covers the whole table
	covered := map[string]bool{}
	passes := (len(probeHostNames) + width - 1) / width
	for seed := 0; seed < passes; seed += 1 {
		hosts, _ := sampleProbeTargets(uint64(seed), width)
		for _, host := range hosts {
			covered[host] = true
		}
	}
	if len(covered) != len(probeHostNames) {
		t.Errorf("rotation covered %d/%d hosts in %d passes", len(covered), len(probeHostNames), passes)
	}

	// resolvers rotate too
	resolvers := map[string]bool{}
	for seed := 0; seed < len(probeResolverIps); seed += 1 {
		_, resolver := sampleProbeTargets(uint64(seed), width)
		resolvers[resolver] = true
	}
	if len(resolvers) != len(probeResolverIps) {
		t.Errorf("resolver rotation covered %d/%d", len(resolvers), len(probeResolverIps))
	}

	// degenerate requests are clamped, never panic
	if hosts, _ := sampleProbeTargets(3, 0); len(hosts) != 0 {
		t.Errorf("n=0 returned %d hosts", len(hosts))
	}
	if hosts, _ := sampleProbeTargets(3, len(probeHostNames)+10); len(hosts) != len(probeHostNames) {
		t.Errorf("an oversized request returned %d hosts, want the whole table", len(hosts))
	}
}

// The default pass width is the ENTIRE table -- the setting narrows, never
// widens, and unset/zero/negative all mean everything. This is the contract
// the mainnet-aggressive default rides on.
func TestProbeSampleWidthDefaultsToEntireTable(t *testing.T) {
	if got := probeSampleWidth(nil); got != len(probeHostNames) {
		t.Errorf("nil settings: width %d, want the whole table (%d)", got, len(probeHostNames))
	}
	if got := probeSampleWidth(&ReliabilitySettings{}); got != len(probeHostNames) {
		t.Errorf("zero setting: width %d, want the whole table (%d)", got, len(probeHostNames))
	}
	if got := probeSampleWidth(&ReliabilitySettings{ProbeSampleHostCount: -1}); got != len(probeHostNames) {
		t.Errorf("negative setting: width %d, want the whole table (%d)", got, len(probeHostNames))
	}
	if got := probeSampleWidth(&ReliabilitySettings{ProbeSampleHostCount: 4}); got != 4 {
		t.Errorf("explicit setting: width %d, want 4", got)
	}
	if got := probeSampleWidth(ReliabilitySettingsFrom(DefaultMultiClientSettings())); got != len(probeHostNames) {
		t.Errorf("shipped defaults: width %d, want the whole table (%d)", got, len(probeHostNames))
	}
}

// The reputation class must be unreachable, not merely unsampled. This is a
// policy the csv encodes and the table has to keep: automated probing of the
// sites a provider's reputation is judged by is exactly what gets an egress ip
// listed.
func TestProbeTableExcludesReputationTargets(t *testing.T) {
	// a sample of the reputation-class endpoints from probe-list-v3.csv
	for _, excluded := range []string{
		"www.akamai.com",
		"www.reddit.com",
		"www.epicgames.com",
		"stackoverflow.com",
		"www.reuters.com",
		"www.etsy.com",
		"www.ecosia.org",
		"www.canva.com",
	} {
		for _, host := range probeHostNames {
			if host == excluded {
				t.Errorf("reputation-class target %q is in the probe table", excluded)
			}
		}
	}
	if len(probeHostNames) == 0 || len(probeResolverIps) == 0 {
		t.Fatal("the probe table is empty")
	}
	for _, resolver := range probeResolverIps {
		if net.ParseIP(resolver) == nil {
			t.Errorf("resolver %q is not an ip", resolver)
		}
	}
}

// --- qualification state ---

func TestQualificationAgeAndCounts(t *testing.T) {
	parent, client, _ := probeTestParent(t)
	destination := client.probeDestination()

	if parent.providerQualified(destination) {
		t.Error("an unprobed provider reported qualified")
	}

	parent.recordProbeFail(destination)
	if parent.providerQualified(destination) {
		t.Error("a failed probe qualified the provider")
	}
	if got := parent.reliabilityMetrics.providersQualified.Load(); got != 0 {
		t.Errorf("providersQualified = %d after a failure, want 0", got)
	}

	parent.recordProbePass(destination)
	if !parent.providerQualified(destination) {
		t.Error("a passed probe did not qualify the provider")
	}

	// a later failure does NOT un-qualify: time ends a qualification, evidence
	// of absence does not
	parent.recordProbeFail(destination)
	if !parent.providerQualified(destination) {
		t.Error("a failed probe revoked an existing qualification: probes must never demote")
	}

	parent.stateLock.Lock()
	entry := parent.qualification[destination]
	if entry.passed != 1 || entry.failed != 2 {
		t.Errorf("counts passed=%d failed=%d, want 1/2", entry.passed, entry.failed)
	}
	// age the qualification past the bound
	entry.qualifiedAt = time.Now().Add(-QualificationMaxAge - time.Second)
	parent.stateLock.Unlock()

	if parent.providerQualified(destination) {
		t.Error("a qualification older than QualificationMaxAge is still current")
	}

	// re-qualifying after the lapse counts as a new provider proved
	parent.recordProbePass(destination)
	if got := parent.reliabilityMetrics.providersQualified.Load(); got != 2 {
		t.Errorf("providersQualified = %d, want 2 (one per transition into qualified)", got)
	}
	// while re-proving a still-qualified provider does not
	parent.recordProbePass(destination)
	if got := parent.reliabilityMetrics.providersQualified.Load(); got != 2 {
		t.Errorf("providersQualified = %d, want 2: re-proving is not a new qualification", got)
	}
}

func TestQualificationLruBound(t *testing.T) {
	parent, _, _ := probeTestParent(t)

	// the timestamps are supplied rather than taken from the clock: a loop
	// this tight lands many entries inside one clock tick on windows, and
	// "the oldest is evicted" is only a decidable claim when the entries have
	// a distinct order. The eviction itself is the shipped one.
	base := time.Now().Add(-time.Minute)
	destinations := []MultiHopId{}
	for i := 0; i < qualificationMaxEntries+16; i += 1 {
		destination := RequireMultiHopId(NewId())
		destinations = append(destinations, destination)

		parent.stateLock.Lock()
		entry := parent.qualificationEntryWithLock(destination)
		entry.qualifiedAt = base.Add(time.Duration(i) * time.Millisecond)
		entry.lastProbeAt = base.Add(time.Duration(i) * time.Millisecond)
		entry.passed += 1
		parent.evictQualificationWithLock()
		parent.stateLock.Unlock()
	}

	parent.stateLock.Lock()
	size := len(parent.qualification)
	parent.stateLock.Unlock()
	if size != qualificationMaxEntries {
		t.Errorf("qualification table holds %d entries, want the cap of %d", size, qualificationMaxEntries)
	}

	// the most recently probed survive
	if !parent.providerQualified(destinations[len(destinations)-1]) {
		t.Error("the most recently probed provider was evicted")
	}
	if parent.providerQualified(destinations[0]) {
		t.Error("the oldest entry survived eviction")
	}

	// and the ordinary record path keeps the table at the cap
	parent.recordProbePass(RequireMultiHopId(NewId()))
	parent.stateLock.Lock()
	defer parent.stateLock.Unlock()
	if n := len(parent.qualification); n != qualificationMaxEntries {
		t.Errorf("after a further pass the table holds %d entries, want %d", n, qualificationMaxEntries)
	}
}

// --- the kill switch ---

// ProviderProbe off means the mechanism does not exist: no packets, no state,
// no qualification. This is the A/B comparison point and the safety valve.
func TestProbeDisabledSendsNothing(t *testing.T) {
	parent, client, forwarded := probeTestParent(t)
	parent.settings.ProviderProbe = false

	result := parent.probeExit(client, probeTestTargets(3), time.Second, 0)

	if result.Sent != 0 || result.Answered != 0 || result.Passed {
		t.Errorf("a disabled probe produced %d/%d answered, passed=%v", result.Answered, result.Sent, result.Passed)
	}
	parent.stateLock.Lock()
	defer parent.stateLock.Unlock()
	if n := len(parent.ip4PathUpdates); n != 0 {
		t.Errorf("a disabled probe registered %d flow(s)", n)
	}
	if n := len(parent.qualification); n != 0 {
		t.Errorf("a disabled probe recorded %d qualification entr(ies)", n)
	}
	if got := parent.reliabilityMetrics.probesSent.Load(); got != 0 {
		t.Errorf("probesSent = %d, want 0", got)
	}
	if n := len(*forwarded); n != 0 {
		t.Errorf("%d packet(s) reached the application", n)
	}
}

// The reserved source range is the structural guarantee that no application
// flow can be mistaken for a probe, and vice versa. Both halves are required.
func TestProbeAddressingIsReserved(t *testing.T) {
	if probeSourcePortMin <= 60999 {
		t.Error("the probe port range overlaps the linux ephemeral range (32768-60999)")
	}
	if probeSourcePortMax < probeSourcePortMin {
		t.Fatal("empty probe port range")
	}

	// an ordinary tun flow is not a probe
	ordinary := &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        net.ParseIP("10.0.0.2"),
		SourcePort:      probeSourcePortMin,
		DestinationIp:   net.ParseIP("93.184.216.34"),
		DestinationPort: 443,
	}
	if probeEgressPath(ordinary) {
		t.Error("a tun flow that happens to use a reserved port was classified as a probe")
	}
	if probeIngressPath(ordinary.Reverse()) {
		t.Error("the answer to a tun flow on a reserved port was classified as a probe")
	}

	// nor is probe-addressed traffic on an ordinary port
	strayPort := &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        probeSourceIp4,
		SourcePort:      443,
		DestinationIp:   net.ParseIP("93.184.216.34"),
		DestinationPort: 443,
	}
	if probeEgressPath(strayPort) {
		t.Error("probe addressing on a non-reserved port was classified as a probe")
	}

	// a real probe path is, in both directions and both families
	for _, sourceIp := range []net.IP{probeSourceIp4, probeSourceIp6} {
		version := 4
		destination := net.ParseIP("93.184.216.34")
		if sourceIp.To4() == nil {
			version = 6
			destination = net.ParseIP("2606:2800:220:1:248:1893:25c8:1946")
		}
		egress := &IpPath{
			Version:         version,
			Protocol:        IpProtocolTcp,
			SourceIp:        sourceIp,
			SourcePort:      probeSourcePortMax,
			DestinationIp:   destination,
			DestinationPort: 443,
		}
		if !probeEgressPath(egress) {
			t.Errorf("v%d: a probe egress path was not classified as a probe", version)
		}
		if !probeIngressPath(egress.Reverse()) {
			t.Errorf("v%d: a probe answer was not classified as a probe", version)
		}
	}

	if probeIngressPath(nil) || probeEgressPath(nil) {
		t.Error("a nil path was classified as a probe")
	}
}

// Registration must never hand out a key that is already live, and the source
// port must come from the reserved range.
func TestProbeRegistrationAvoidsCollisions(t *testing.T) {
	parent, client, _ := probeTestParent(t)

	target := probeTestTarget()
	probes := []*probeFlow{}
	ports := map[int]bool{}
	for i := 0; i < 8; i += 1 {
		probe, ok := parent.registerProbeFlow(client, target)
		if !ok {
			t.Fatalf("registration %d failed", i)
		}
		if !probeReservedPort(probe.ipPath.SourcePort) {
			t.Errorf("registered source port %d is outside the reserved range", probe.ipPath.SourcePort)
		}
		if ports[probe.ipPath.SourcePort] {
			t.Errorf("port %d was handed out twice for the same destination", probe.ipPath.SourcePort)
		}
		ports[probe.ipPath.SourcePort] = true
		probes = append(probes, probe)
	}

	parent.stateLock.Lock()
	registered := len(parent.ip4PathUpdates)
	parent.stateLock.Unlock()
	if registered != 8 {
		t.Errorf("%d flows registered, want 8", registered)
	}

	parent.unregisterProbeFlows(probes)

	parent.stateLock.Lock()
	defer parent.stateLock.Unlock()
	if n := len(parent.ip4PathUpdates); n != 0 {
		t.Errorf("%d flow(s) survived unregistration", n)
	}
}
