package connect

import (
	"context"
	"net"
	"strings"
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

// --- fixtures ---
//
// removeClient is fixture-testable the same way the dial-failure handler is
// (see dialFailureTestParent): a bare parent carrying real updates in the
// path maps and clientUpdates, a dying client marked done, and the teardown
// packets captured off receivePacketCallback. The one extra seam is
// rebindCandidatesFunc, which injects the replacement candidates the default
// gather would read from live windows -- the choreography under test
// (re-validation, cap headroom, affinity cohesion, bookkeeping) all runs
// against the injected list exactly as it would against the gathered one.

// rebindTestParent builds a bare parent wired for removeClient. candidates is
// what the rebind gather offers, most-preferred first; rebind sets
// QuicRebindOnExitLoss.
func rebindTestParent(t *testing.T, rebind bool, candidates []*multiClientChannel) (
	parent *RemoteUserNatMultiClient,
	dying *multiClientChannel,
	forwarded *[]*receivePacket,
	gatherCalled *bool,
) {
	t.Helper()

	settings := DefaultMultiClientSettings()
	settings.QuicRebindOnExitLoss = rebind

	captured := &[]*receivePacket{}
	called := new(bool)
	parent = &RemoteUserNatMultiClient{
		log:                DefaultLogger(),
		settings:           settings,
		ip4PathUpdates:     map[Ip4Path]*multiClientChannelUpdate{},
		ip6PathUpdates:     map[Ip6Path]*multiClientChannelUpdate{},
		clientUpdates:      map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
		reliabilityMetrics: newReliabilityMetrics(),
	}
	parent.SetReceivePacketCallback(func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
		*captured = append(*captured, &receivePacket{
			Source:      source,
			ProvideMode: provideMode,
			IpPath:      ipPath,
			Packet:      packet,
		})
	})
	parent.ctx = context.Background()
	parent.rebindCandidatesFunc = func(dying *multiClientChannel) []*multiClientChannel {
		*called = true
		return candidates
	}

	// removeClient requires the dying client to already be done
	doneCtx, cancel := context.WithCancel(context.Background())
	cancel()
	dying = &multiClientChannel{ctx: doneCtx, settings: settings}

	return parent, dying, captured, called
}

// rebindTestCandidate is a healthy, live replacement.
func rebindTestCandidate(settings *MultiClientSettings) *multiClientChannel {
	return &multiClientChannel{ctx: context.Background(), settings: settings}
}

// rebindTestFlow registers a flow bound to client in both the path map and
// clientUpdates, the state removeClient reads.
func rebindTestFlow(parent *RemoteUserNatMultiClient, client *multiClientChannel, ipPath *IpPath, established bool) *multiClientChannelUpdate {
	update := &multiClientChannelUpdate{ipPath: ipPath}
	update.client.Store(client)
	update.receivedInbound.Store(established)
	switch ipPath.Version {
	case 4:
		parent.ip4PathUpdates[ipPath.ToIp4Path()] = update
	case 6:
		parent.ip6PathUpdates[ipPath.ToIp6Path()] = update
	}
	updates, ok := parent.clientUpdates[client]
	if !ok {
		updates = map[*multiClientChannelUpdate]bool{}
		parent.clientUpdates[client] = updates
	}
	updates[update] = true
	return update
}

// distinct destinations per flow so recovery-tracker keys never collide
// between the flows of one test
func rebindUdp443Path(hostOctet byte, sourcePort int) *IpPath {
	return &IpPath{
		Version:         4,
		Protocol:        IpProtocolUdp,
		SourceIp:        net.ParseIP("10.0.0.2").To4(),
		SourcePort:      sourcePort,
		DestinationIp:   net.IP{93, 184, 216, hostOctet},
		DestinationPort: 443,
	}
}

func rebindTcpPath(hostOctet byte, sourcePort int) *IpPath {
	return &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        net.ParseIP("10.0.0.2").To4(),
		SourcePort:      sourcePort,
		DestinationIp:   net.IP{93, 184, 217, hostOctet},
		DestinationPort: 443,
		Syn:             true,
	}
}

func rebindFlowCount(parent *RemoteUserNatMultiClient, client *multiClientChannel) int {
	parent.stateLock.Lock()
	defer parent.stateLock.Unlock()
	return len(parent.clientUpdates[client])
}

// The core proactive rebind: of a dying exit's flows, exactly the
// established udp/443 one moves to the live candidate -- pointer re-stored,
// bookkeeping moved, no teardown signal for it -- while the unestablished
// udp/443 flow and the established tcp flow are torn down exactly as before.
func TestRebindEstablishedQuicFlowMovesToCandidate(t *testing.T) {
	settings := DefaultMultiClientSettings()
	candidate := rebindTestCandidate(settings)
	parent, dying, forwarded, _ := rebindTestParent(t, true, []*multiClientChannel{candidate})

	establishedQuic := rebindTestFlow(parent, dying, rebindUdp443Path(10, 40001), true)
	unestablishedQuic := rebindTestFlow(parent, dying, rebindUdp443Path(11, 40002), false)
	establishedTcp := rebindTestFlow(parent, dying, rebindTcpPath(12, 40003), true)

	parent.removeClient(dying)

	// the established quic flow moved
	if establishedQuic.client.Load() != candidate {
		t.Fatal("established udp/443 flow was not re-pinned to the candidate")
	}
	if !parent.clientUpdates[candidate][establishedQuic] {
		t.Error("rebound flow's clientUpdates bookkeeping did not move to the replacement")
	}
	// it stays established: the rebind must not reopen the dial-failure gate
	if !establishedQuic.receivedInbound.Load() {
		t.Error("rebind cleared receivedInbound")
	}

	// the others died exactly as before
	if unestablishedQuic.client.Load() != nil {
		t.Error("unestablished udp/443 flow was rebound; it has no proven connection to migrate")
	}
	if establishedTcp.client.Load() != nil {
		t.Error("tcp flow was rebound; split-tcp flows are unrecoverable and must fail fast")
	}
	if _, ok := parent.clientUpdates[dying]; ok {
		t.Error("dying client still has clientUpdates entries")
	}

	// teardown signals: exactly one icmp (unestablished udp) and one rst
	// (tcp), and none naming the rebound flow -- an icmp unreachable is
	// RFC-9000-inert for quic and the flow is not dead, it moved
	if n := len(*forwarded); n != 2 {
		t.Fatalf("forwarded %d teardown packet(s), want 2 (icmp for the udp flow, rst for the tcp flow)", n)
	}
	for _, p := range *forwarded {
		if p.IpPath.ToIp4Path() == establishedQuic.ipPath.ToIp4Path() {
			t.Error("a teardown signal was sent for the rebound flow")
		}
	}

	// metrics: one rebind performed, two flows lost, and the tracker entry
	// carries the rebound flow's local source port for later classification
	m := parent.reliabilityMetrics
	if got := m.flowsRebound.Load(); got != 1 {
		t.Errorf("flowsRebound = %d, want 1", got)
	}
	if got := m.flowsLostToExit.Load(); got != 2 {
		t.Errorf("flowsLostToExit = %d, want 2: the rebound flow is not lost", got)
	}
	m.pendingLock.Lock()
	entry, ok := m.pending[newRecoveryKey(establishedQuic.ipPath.DestinationIp, 443)]
	m.pendingLock.Unlock()
	if !ok {
		t.Fatal("no recovery tracker entry for the rebound flow's destination")
	}
	if entry.reboundLocalPort != 40001 {
		t.Errorf("tracker entry reboundLocalPort = %d, want 40001", entry.reboundLocalPort)
	}
}

// The cap is consulted FIRST but is not a licence to destroy: a flow with no
// under-cap candidate is placed over the cap on the last-resort pass rather
// than torn down. Sticky affinity deliberately grows a heavy site past the
// cap, so the biggest groups are exactly the ones no candidate has headroom
// for -- and tearing those down is the failure the whole affinity line
// exists to prevent (review finding, 2026-08-03).
func TestRebindOverCapRatherThanTeardown(t *testing.T) {
	settings := DefaultMultiClientSettings()
	settings.MaxFlowsPerExit = 1
	fullCandidate := rebindTestCandidate(settings)
	parent, dying, forwarded, _ := rebindTestParent(t, true, []*multiClientChannel{fullCandidate})
	parent.settings = settings

	// the candidate already carries its full share
	rebindTestFlow(parent, fullCandidate, rebindUdp443Path(20, 41000), true)

	flow := rebindTestFlow(parent, dying, rebindUdp443Path(21, 41001), true)

	parent.removeClient(dying)

	if flow.client.Load() != fullCandidate {
		t.Fatal("a flow with no under-cap candidate was torn down instead of placed over the cap")
	}
	if got := rebindFlowCount(parent, fullCandidate); got != 2 {
		t.Errorf("candidate flow count = %d, want 2 (over the cap of 1)", got)
	}
	if n := len(*forwarded); n != 0 {
		t.Errorf("forwarded %d packet(s), want 0: nothing was torn down", n)
	}
	if got := parent.reliabilityMetrics.flowsRebound.Load(); got != 1 {
		t.Errorf("flowsRebound = %d, want 1", got)
	}
}

// Done and dying candidates are re-validated away at assignment time even
// though the gathered list offered them -- the list is pre-lock and may be
// stale, and neither can carry a flow. This holds on the last-resort pass
// too: "better than a teardown" admits SOFT states (warned, quarantined,
// over-cap), never a dead channel.
func TestRebindNeverUsesDoneOrDyingCandidates(t *testing.T) {
	settings := DefaultMultiClientSettings()

	doneCtx, cancel := context.WithCancel(context.Background())
	cancel()
	doneCandidate := &multiClientChannel{ctx: doneCtx, settings: settings}

	parent, dying, forwarded, _ := rebindTestParent(t, true, nil)
	// the dying client offered as its own replacement, plus a done one:
	// neither is usable at any tier
	parent.rebindCandidatesFunc = func(d *multiClientChannel) []*multiClientChannel {
		return []*multiClientChannel{dying, doneCandidate}
	}

	flow := rebindTestFlow(parent, dying, rebindUdp443Path(30, 42001), true)

	parent.removeClient(dying)

	if flow.client.Load() != nil {
		t.Fatal("flow was rebound onto a done or dying candidate")
	}
	if n := len(*forwarded); n != 1 {
		t.Errorf("forwarded %d packet(s), want 1 (fallback teardown)", n)
	}
	if got := parent.reliabilityMetrics.flowsRebound.Load(); got != 0 {
		t.Errorf("flowsRebound = %d, want 0", got)
	}
}

// A WARNED (benched) candidate is soft-suspect but alive, and the strict
// pass rightly prefers anything else -- but when it is all there is, the
// flow rides it instead of dying. This is what makes rebindCandidates'
// benched fallback reachable at all: that fallback only ever yields warned
// exits, so a strict !isWarning() test rejected every one of them and the
// fallback could never place a single flow.
func TestRebindUsesWarnedCandidateAsLastResort(t *testing.T) {
	settings := DefaultMultiClientSettings()

	warningCandidate := rebindTestCandidate(settings)
	warningCandidate.setWarning(true, warnUnhealthy)

	parent, dying, forwarded, _ := rebindTestParent(t, true, nil)
	parent.rebindCandidatesFunc = func(d *multiClientChannel) []*multiClientChannel {
		return []*multiClientChannel{warningCandidate}
	}

	flow := rebindTestFlow(parent, dying, rebindUdp443Path(31, 42101), true)

	parent.removeClient(dying)

	if flow.client.Load() != warningCandidate {
		t.Fatal("a flow was torn down while a live benched exit was available")
	}
	if n := len(*forwarded); n != 0 {
		t.Errorf("forwarded %d packet(s), want 0: nothing was torn down", n)
	}
	if got := parent.reliabilityMetrics.flowsRebound.Load(); got != 1 {
		t.Errorf("flowsRebound = %d, want 1", got)
	}
}

// Affinity cohesion: flows sharing an affinity key are one site's
// connections, and the whole group lands on ONE replacement -- the site sees
// a single coordinated egress-ip change. An ungrouped flow spreads
// least-loaded, which after the group landed is the other candidate.
func TestRebindAffinityGroupLandsOnOneReplacement(t *testing.T) {
	settings := DefaultMultiClientSettings()
	first := rebindTestCandidate(settings)
	second := rebindTestCandidate(settings)
	parent, dying, _, _ := rebindTestParent(t, true, []*multiClientChannel{first, second})

	affinityKey := rebindUdp443Path(40, 0).ToIp4Path()
	groupedA := rebindTestFlow(parent, dying, rebindUdp443Path(40, 43001), true)
	groupedA.affinityIp4Paths = map[Ip4Path]bool{affinityKey: true}
	groupedB := rebindTestFlow(parent, dying, rebindUdp443Path(41, 43002), true)
	groupedB.affinityIp4Paths = map[Ip4Path]bool{affinityKey: true}
	ungrouped := rebindTestFlow(parent, dying, rebindUdp443Path(42, 43003), true)

	parent.removeClient(dying)

	if groupedA.client.Load() != first || groupedB.client.Load() != first {
		t.Errorf(
			"affinity group split across replacements: %p / %p, want both on the preferred candidate %p",
			groupedA.client.Load(), groupedB.client.Load(), first,
		)
	}
	// least-loaded for the ungrouped flow: first now carries 2, second 0
	if ungrouped.client.Load() != second {
		t.Error("ungrouped flow did not go to the least-loaded candidate")
	}
	if got := parent.reliabilityMetrics.flowsRebound.Load(); got != 3 {
		t.Errorf("flowsRebound = %d, want 3", got)
	}
}

// A group is split across candidates only when it exceeds every single
// candidate's remaining cap headroom -- and even then the cap holds.
func TestRebindAffinityGroupSplitsOnlyWhenExceedingHeadroom(t *testing.T) {
	settings := DefaultMultiClientSettings()
	settings.MaxFlowsPerExit = 2
	first := rebindTestCandidate(settings)
	second := rebindTestCandidate(settings)
	parent, dying, forwarded, _ := rebindTestParent(t, true, []*multiClientChannel{first, second})
	parent.settings = settings

	affinityKey := rebindUdp443Path(50, 0).ToIp4Path()
	flows := []*multiClientChannelUpdate{}
	for i := range 3 {
		flow := rebindTestFlow(parent, dying, rebindUdp443Path(byte(50+i), 44001+i), true)
		flow.affinityIp4Paths = map[Ip4Path]bool{affinityKey: true}
		flows = append(flows, flow)
	}

	parent.removeClient(dying)

	// no candidate holds 3 under a cap of 2, so the group splits 2/1 in
	// preference order, and nothing is torn down
	if got := rebindFlowCount(parent, first); got != 2 {
		t.Errorf("preferred candidate flow count = %d, want 2 (filled to headroom)", got)
	}
	if got := rebindFlowCount(parent, second); got != 1 {
		t.Errorf("second candidate flow count = %d, want 1 (the spill)", got)
	}
	for i, flow := range flows {
		if flow.client.Load() == nil {
			t.Errorf("flow %d was torn down; the split should have placed all 3", i)
		}
	}
	if n := len(*forwarded); n != 0 {
		t.Errorf("forwarded %d packet(s), want 0", n)
	}
	if got := parent.reliabilityMetrics.flowsRebound.Load(); got != 3 {
		t.Errorf("flowsRebound = %d, want 3", got)
	}
}

// The toggle: off restores the pre-change behavior exactly -- every flow torn
// down with its signal, and the candidate gather never even runs.
func TestRebindToggleOffKeepsTeardown(t *testing.T) {
	settings := DefaultMultiClientSettings()
	candidate := rebindTestCandidate(settings)
	parent, dying, forwarded, gatherCalled := rebindTestParent(t, false, []*multiClientChannel{candidate})

	flow := rebindTestFlow(parent, dying, rebindUdp443Path(60, 45001), true)

	parent.removeClient(dying)

	if *gatherCalled {
		t.Error("candidate gather ran with QuicRebindOnExitLoss off")
	}
	if flow.client.Load() != nil {
		t.Fatal("flow was rebound with the toggle off")
	}
	if n := len(*forwarded); n != 1 {
		t.Errorf("forwarded %d packet(s), want 1 (the teardown icmp)", n)
	}
	if got := parent.reliabilityMetrics.flowsRebound.Load(); got != 0 {
		t.Errorf("flowsRebound = %d, want 0", got)
	}
	if got := parent.reliabilityMetrics.flowsLostToExit.Load(); got != 1 {
		t.Errorf("flowsLostToExit = %d, want 1", got)
	}
}

// The full loop closed end to end: a rebound flow's destination answering on
// the same local port classifies as a server-accepted migration, off the very
// tracker entry removeClient armed.
func TestRebindRecoveryClassifiedFromRemoveClient(t *testing.T) {
	settings := DefaultMultiClientSettings()
	candidate := rebindTestCandidate(settings)
	parent, dying, _, _ := rebindTestParent(t, true, []*multiClientChannel{candidate})

	flow := rebindTestFlow(parent, dying, rebindUdp443Path(70, 46001), true)

	parent.removeClient(dying)
	if flow.client.Load() != candidate {
		t.Fatal("flow was not rebound")
	}

	// the destination answers the rebound flow's own local port -- this is
	// what clientReceivePacket reports pre-reverse (remote endpoint still
	// the source, local port still the destination)
	parent.reliabilityMetrics.destinationReachable(flow.ipPath.DestinationIp, 443, 46001)

	s := parent.reliabilityMetrics.snapshot()
	if s.RebindsAccepted != 1 {
		t.Errorf("rebinds accepted = %d, want 1", s.RebindsAccepted)
	}
	if s.RebindsRedialed != 0 {
		t.Errorf("rebinds redialed = %d, want 0", s.RebindsRedialed)
	}
	if s.RecoveryCount != 1 {
		t.Errorf("recovery count = %d, want 1", s.RecoveryCount)
	}
}

// Source anchors: the mechanism is only real if removeClient consults the
// toggle and routes through the gather and the metrics -- a helper that is
// correct but uncalled is the failure mode this suite pins against.
func TestRebindRemoveClientSourceAnchors(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *RemoteUserNatMultiClient) removeClient(")
	if !ok {
		t.Fatal("could not find removeClient")
	}
	if !strings.Contains(body, "QuicRebindOnExitLoss") {
		t.Error("removeClient does not consult QuicRebindOnExitLoss: the rebind cannot be toggled for A/B")
	}
	if !strings.Contains(body, "self.rebindCandidates(") {
		t.Error("removeClient does not gather rebind candidates before its locked section")
	}
	if !strings.Contains(body, "self.rebindFlowsWithLock(") {
		t.Error("removeClient does not run the rebind assignment")
	}
	if !strings.Contains(body, "exitLostRebound(") {
		t.Error("removeClient does not record rebinds, so the mechanism is invisible to measurement")
	}
	if !strings.Contains(body, "update.receivedInbound.Load()") {
		t.Error("removeClient does not gate the rebind on established flows")
	}

	// and the ingress path must feed the classifier the local port, or the
	// accepted/redialed split silently reads as all-redialed
	body, ok = functionBody(source, "func (self *RemoteUserNatMultiClient) clientReceivePacket(")
	if !ok {
		t.Fatal("could not find clientReceivePacket")
	}
	if !strings.Contains(body, "destinationReachable(ipPath.SourceIp, ipPath.SourcePort, ipPath.DestinationPort)") {
		t.Error("clientReceivePacket does not pass the local port to destinationReachable")
	}
}
