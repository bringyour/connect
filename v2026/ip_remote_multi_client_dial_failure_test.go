package connect

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// dialFailureTestParent builds a bare parent carrying one flow (egressIpPath)
// bound to sourceClient in both the path map and clientUpdates -- the state a
// dial-failure lookup reads. rerace sets DialFailureRerace. forwarded captures
// any packet the handler hands back to the app, so the swallow-vs-forward
// behavior can be asserted directly.
func dialFailureTestParent(t *testing.T, rerace bool, egressIpPath *IpPath) (
	parent *RemoteUserNatMultiClient,
	sourceClient *multiClientChannel,
	update *multiClientChannelUpdate,
	forwarded *[]*receivePacket,
) {
	t.Helper()

	settings := DefaultMultiClientSettings()
	settings.DialFailureRerace = rerace

	captured := &[]*receivePacket{}
	parent = &RemoteUserNatMultiClient{
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

	sourceClient = &multiClientChannel{settings: settings}
	update = &multiClientChannelUpdate{ipPath: egressIpPath}
	update.client.Store(sourceClient)

	switch egressIpPath.Version {
	case 4:
		parent.ip4PathUpdates[egressIpPath.ToIp4Path()] = update
	case 6:
		parent.ip6PathUpdates[egressIpPath.ToIp6Path()] = update
	}
	parent.clientUpdates[sourceClient] = map[*multiClientChannelUpdate]bool{update: true}

	return parent, sourceClient, update, captured
}

// The core re-race: a dial failure on the flow's own client, before any inbound
// data, unbinds the flow (so the app's retransmit races a new exit), swallows
// the icmp, and records the metrics. Nothing reaches the app.
func TestDialFailureMatchedUnbindsAndSwallows(t *testing.T) {
	for _, version := range []int{4, 6} {
		egress := icmpTcpTestPath(version)
		parent, client, update, forwarded := dialFailureTestParent(t, true, egress)

		parent.clientDialFailure(client, egress)

		if update.client.Load() != nil {
			t.Errorf("v%d: flow was not unbound; client is still %p", version, update.client.Load())
		}
		if _, ok := parent.clientUpdates[client]; ok {
			t.Errorf("v%d: update was not removed from clientUpdates", version)
		}
		if n := len(*forwarded); n != 0 {
			t.Errorf("v%d: re-race must swallow the icmp, but %d packet(s) reached the app", version, n)
		}
		if got := parent.reliabilityMetrics.flowsReraced.Load(); got != 1 {
			t.Errorf("v%d: flowsReraced = %d, want 1", version, got)
		}
		if got := parent.reliabilityMetrics.dialFailures.Load(); got != 1 {
			t.Errorf("v%d: dialFailuresIntercepted = %d, want 1", version, got)
		}
		// a matched failure is also a per-channel strike
		if got := client.dialFailureCount(); got != 1 {
			t.Errorf("v%d: channel dialFailureCount = %d, want 1", version, got)
		}
	}
}

// A signal naming a flow no longer in the maps is stale (late, or already
// re-raced). It is counted but otherwise dropped -- nothing to unbind, nothing
// forwarded.
func TestDialFailureNoFlowIsUnmatched(t *testing.T) {
	egress := icmpTcpTestPath(4)
	parent, client, _, forwarded := dialFailureTestParent(t, true, egress)

	// a different flow key than the one in the map
	stray := icmpTcpTestPath(4)
	stray.SourcePort = egress.SourcePort + 1

	parent.clientDialFailure(client, stray)

	if got := parent.reliabilityMetrics.dialFailures.Load(); got != 1 {
		t.Errorf("dialFailuresIntercepted = %d, want 1 (counted even when unmatched)", got)
	}
	if got := parent.reliabilityMetrics.flowsReraced.Load(); got != 0 {
		t.Errorf("flowsReraced = %d, want 0", got)
	}
	if n := len(*forwarded); n != 0 {
		t.Errorf("%d packet(s) forwarded, want 0", n)
	}
}

// A signal from a client that does not own the flow (the flow already re-raced
// onto another exit) must not disturb the flow's current binding.
func TestDialFailureDifferentClientIsUnmatched(t *testing.T) {
	egress := icmpTcpTestPath(4)
	parent, client, update, forwarded := dialFailureTestParent(t, true, egress)

	other := &multiClientChannel{settings: DefaultMultiClientSettings()}
	parent.clientDialFailure(other, egress)

	if update.client.Load() != client {
		t.Error("a signal from a non-owning client unbound the flow")
	}
	if got := parent.reliabilityMetrics.flowsReraced.Load(); got != 0 {
		t.Errorf("flowsReraced = %d, want 0", got)
	}
	if got := parent.reliabilityMetrics.dialFailures.Load(); got != 1 {
		t.Errorf("dialFailuresIntercepted = %d, want 1 (still counted)", got)
	}
	if n := len(*forwarded); n != 0 {
		t.Errorf("%d packet(s) forwarded, want 0", n)
	}
}

// The established-flow guard: once a flow has received inbound data it is
// working, and a stale dial-failure signal must never tear it down. This is the
// crash-safety the whole re-race depends on.
func TestDialFailureEstablishedFlowGuard(t *testing.T) {
	egress := icmpTcpTestPath(4)
	parent, client, update, forwarded := dialFailureTestParent(t, true, egress)

	// the flow already carried inbound data -- it is established
	update.receivedInbound.Store(true)

	parent.clientDialFailure(client, egress)

	if update.client.Load() != client {
		t.Error("an established flow (receivedInbound) was unbound by a dial-failure signal")
	}
	if got := parent.reliabilityMetrics.flowsReraced.Load(); got != 0 {
		t.Errorf("flowsReraced = %d, want 0", got)
	}
	if got := parent.reliabilityMetrics.dialFailures.Load(); got != 1 {
		t.Errorf("dialFailuresIntercepted = %d, want 1 (still counted)", got)
	}
	if n := len(*forwarded); n != 0 {
		t.Errorf("%d packet(s) forwarded, want 0", n)
	}
}

// With the knob off, a matched failure is not swallowed: the flow stays bound
// and the raw icmp is delivered to the app (visible but fast failure), using
// the egress ipPath -- the same convention removeClient uses for teardown.
func TestDialFailureReraceOffForwards(t *testing.T) {
	for _, version := range []int{4, 6} {
		egress := icmpTcpTestPath(version)
		parent, client, update, forwarded := dialFailureTestParent(t, false, egress)

		parent.clientDialFailure(client, egress)

		if update.client.Load() != client {
			t.Errorf("v%d: rerace off must not unbind the flow", version)
		}
		if got := parent.reliabilityMetrics.flowsReraced.Load(); got != 0 {
			t.Errorf("v%d: flowsReraced = %d, want 0 with rerace off", version, got)
		}
		if got := parent.reliabilityMetrics.dialFailures.Load(); got != 1 {
			t.Errorf("v%d: dialFailuresIntercepted = %d, want 1", version, got)
		}
		if len(*forwarded) != 1 {
			t.Fatalf("v%d: forwarded %d packet(s), want exactly 1 (the icmp)", version, len(*forwarded))
		}
		fwd := (*forwarded)[0]
		// delivered with the egress-oriented ipPath, per the teardown convention
		if fwd.IpPath != egress {
			t.Errorf("v%d: forwarded with the wrong ipPath", version)
		}
		// and it is a real icmp unreachable naming exactly this flow
		parsed, ok := ipParseIcmpUnreachable(fwd.Packet)
		if !ok {
			t.Fatalf("v%d: forwarded packet is not a parseable icmp unreachable", version)
		}
		switch version {
		case 4:
			if parsed.ToIp4Path() != egress.ToIp4Path() {
				t.Errorf("v4: forwarded icmp names the wrong flow")
			}
		case 6:
			if parsed.ToIp6Path() != egress.ToIp6Path() {
				t.Errorf("v6: forwarded icmp names the wrong flow")
			}
		}
	}
}

// The starvation window: three failures spanning distinct destinations with
// no successes trips it; a single connect success in the window clears it.
// This is the input to the resize-pass warning, and it must not depend on
// wall-clock sleeps.
func TestDialStarvedWindowing(t *testing.T) {
	client := &multiClientChannel{settings: DefaultMultiClientSettings()}

	if client.dialStarved() {
		t.Fatal("a fresh channel reported starved with no failures")
	}

	client.addDialFailure("93.184.216.34", 4)
	client.addDialFailure("142.250.74.100", 4)
	if client.dialStarved() {
		t.Fatal("2 failures (below the threshold) reported starved")
	}

	client.addDialFailure("93.184.216.34", 4)
	if !client.dialStarved() {
		t.Fatal("3 failures across 2 destinations with no successes did not report starved")
	}
	if got := client.dialFailureCount(); got != 3 {
		t.Errorf("dialFailureCount = %d, want 3", got)
	}

	// a single proven connect in the window resets starvation
	client.addConnectSuccess(4)
	if client.dialStarved() {
		t.Fatal("a connect success in the window did not reset starvation")
	}
}

// The distinct-destination requirement: any number of strikes from a single
// destination is that destination's problem, not the exit's -- a polled-dead
// site retransmitting its dials must not starve-warn (or rank-demote) a
// healthy exit. The same strike count spanning two destinations convicts at
// full speed.
func TestDialStarvedRequiresDistinctDestinations(t *testing.T) {
	client := &multiClientChannel{settings: DefaultMultiClientSettings()}

	// three strikes, one destination: not starvation
	client.addDialFailure("93.184.216.34", 4)
	client.addDialFailure("93.184.216.34", 4)
	client.addDialFailure("93.184.216.34", 4)
	if client.dialStarved() {
		t.Fatal("3 strikes from a single destination reported starved: one dead site convicted the exit")
	}
	// the strikes still count and still surface in the readout
	if got := client.dialFailureCount(); got != 3 {
		t.Errorf("dialFailureCount = %d, want 3", got)
	}

	// a strike for a second destination makes the span, and the exit is
	// starved immediately -- demotion stays fast for the real dud
	client.addDialFailure("142.250.74.100", 4)
	if !client.dialStarved() {
		t.Fatal("strikes spanning 2 destinations did not report starved")
	}
}

// Failures older than the window are pruned on access, so a provider that
// misbehaved and recovered stops being warned rather than being condemned by
// ancient strikes.
func TestDialStarvedPrunesOldFailures(t *testing.T) {
	client := &multiClientChannel{settings: DefaultMultiClientSettings()}

	// three strikes, all older than the window (test setup injects the
	// timestamps; the shipped methods do the pruning)
	old := time.Now().Add(-2 * dialStrikeWindow)
	client.dialFailureTimes = []time.Time{old, old, old}

	if client.dialStarved() {
		t.Error("failures older than the window still counted toward starvation")
	}
	if got := client.dialFailureCount(); got != 0 {
		t.Errorf("dialFailureCount = %d, want 0 after pruning", got)
	}
}

// dialStarved must actually be consulted at the resize warning site, or a
// dial-starved provider keeps attracting new flows and the whole signal does
// nothing. Unit isolation of the resize pass is impractical, so pin the call
// site the way TestDetectBlackholeUsesTheReasonAndOverride pins detectBlackhole.
func TestResizeWarnsOnDialStarved(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *multiClientWindow) resize()")
	if !ok {
		t.Fatal("could not find resize")
	}
	if !strings.Contains(body, "client.dialStarved()") {
		t.Error("resize does not consult dialStarved(): a dial-starved provider is never warned out of new-flow selection")
	}
	if !strings.Contains(body, "client.setWarning(true, warnStarved)") {
		t.Error("resize dial-starved warning site not found (or its cause is no longer warnStarved); the dialStarved wiring may have moved off it")
	}
}

// The HARD REQUIREMENT, tested end to end at the channel: an intercepted icmp
// dial-failure signal must NOT bump the receive counters, or a provider that
// fails every dial would look like it is receiving data and detectBlackhole
// would keep a dead exit alive. A normal packet is the positive control that
// proves the counter can move.
func TestChannelInterceptDoesNotBumpReceiveCounters(t *testing.T) {
	egress := icmpTcpTestPath(4)

	var gotClient *multiClientChannel
	var gotPath *IpPath
	channel := &multiClientChannel{
		ctx:         context.Background(),
		log:         DefaultLogger(),
		args:        &multiClientChannelArgs{},
		settings:    DefaultMultiClientSettings(),
		packetStats: &clientWindowStats{log: DefaultLogger()},
		clientReceivePacketCallback: func(client *multiClientChannel, source TransferPath, provideMode protocol.ProvideMode, transportType TransportType, ipPath *IpPath, packet []byte) {
		},
		dialFailureCallback: func(sourceClient *multiClientChannel, egressIpPath *IpPath) bool {
			gotClient = sourceClient
			gotPath = egressIpPath
			return true
		},
	}

	frameFor := func(packet []byte) *protocol.Frame {
		return RequireToFrameWithDefaultProtocolVersion(&protocol.IpPacketFromProvider{
			IpPacket: &protocol.IpPacket{PacketBytes: packet},
		})
	}
	peer := Peer{ProvideMode: protocol.ProvideMode_Public}

	// positive control: a normal tcp packet is received data and IS counted
	normal, ok := ipOosRst(egress.Reverse())
	if !ok {
		t.Fatal("could not build a normal tcp packet")
	}
	channel.clientReceive(TransferPath{}, []*protocol.Frame{frameFor(normal)}, peer)
	if channel.packetStats.receiveAckCount != 1 {
		t.Fatalf("positive control: receiveAckCount = %d, want 1 (a normal packet must count)", channel.packetStats.receiveAckCount)
	}

	// the intercept: an icmp dial-failure signal must NOT be counted
	icmp, ok := ipOosUnreachable(egress)
	if !ok {
		t.Fatal("could not build the icmp signal")
	}
	channel.clientReceive(TransferPath{}, []*protocol.Frame{frameFor(icmp)}, peer)

	if channel.packetStats.receiveAckCount != 1 {
		t.Errorf("receiveAckCount = %d, want 1: the intercepted icmp must not be counted as received data", channel.packetStats.receiveAckCount)
	}
	if channel.packetStats.receiveSynCount != 0 {
		t.Errorf("receiveSynCount = %d, want 0: the intercepted icmp must not count as a syn", channel.packetStats.receiveSynCount)
	}
	if gotClient != channel {
		t.Error("dialFailureCallback did not receive the intercepting channel")
	}
	if gotPath == nil || gotPath.ToIp4Path() != egress.ToIp4Path() {
		t.Error("dialFailureCallback received the wrong egress path")
	}
}

// --- the dial-probe predicate: which egress packets the inference watches ---

// A pure tcp syn is a probe; packets of established tcp flows are not. For
// udp only the request-response handshake ports count (quic 443, dns 53): a
// udp datagram carries no handshake marker, and send-only protocols
// legitimately never hear back -- re-racing those on silence would bounce
// them between exits forever and strike healthy exits for normal behavior.
func TestDialProbePacket(t *testing.T) {
	quic := udpTestPath(4) // destination port 443
	dns := udpTestPath(4)
	dns.DestinationPort = 53
	telemetry := udpTestPath(4)
	telemetry.DestinationPort = 5001

	syn := icmpTcpTestPath(4) // Syn set, Ack clear
	synAck := icmpTcpTestPath(4)
	synAck.Ack = true
	established := icmpTcpTestPath(4)
	established.Syn = false
	established.Ack = true

	cases := []struct {
		name string
		path *IpPath
		want bool
	}{
		{"tcp pure syn", syn, true},
		{"tcp syn-ack", synAck, false},
		{"tcp established", established, false},
		{"udp quic 443", quic, true},
		{"udp dns 53", dns, true},
		{"udp send-only port", telemetry, false},
	}
	for _, c := range cases {
		if got := dialProbePacket(c.path); got != c.want {
			t.Errorf("%s: dialProbePacket = %v, want %v", c.name, got, c.want)
		}
	}
}

// The quic form of the field failure: a udp/443 flow that never received a
// byte, pinned to an exit that cannot complete upstream connects. The re-race
// machinery is protocol-agnostic; this pins that a udp flow is unbound and
// strikes the exit exactly as a tcp one does. On device three exits held 63
// mostly-quic flows in silence for 29s because only the tcp minority had this
// escape.
func TestDialFailureQuicFlowReraces(t *testing.T) {
	egress := udpTestPath(4)
	parent, client, update, forwarded := dialFailureTestParent(t, true, egress)

	parent.clientDialFailure(client, egress)

	if update.client.Load() != nil {
		t.Error("quic flow was not unbound")
	}
	if got := parent.reliabilityMetrics.flowsReraced.Load(); got != 1 {
		t.Errorf("flowsReraced = %d, want 1", got)
	}
	if got := client.dialFailureCount(); got != 1 {
		t.Errorf("channel dialFailureCount = %d, want 1: quic silence must strike the exit like tcp silence", got)
	}
	if n := len(*forwarded); n != 0 {
		t.Errorf("%d packet(s) reached the app, want 0", n)
	}
}

// A handshake-shaped flow -- a few probes retransmitting into silence --
// trips the wait once the timeout passes on the same exit.
func TestSynWaitHandshakeTrips(t *testing.T) {
	update := &multiClientChannelUpdate{}
	client := &multiClientChannel{settings: DefaultMultiClientSettings()}
	tcpPath := udpTestPath(4)
	tcpPath.Protocol = IpProtocolTcp
	tcpPath.Syn = true

	if update.synWaitExceeded(client, tcpPath, 0) {
		t.Fatal("the first probe must only start the clock, never trip")
	}
	// a zero timeout is already exceeded by the second probe
	if !update.synWaitExceeded(client, tcpPath, 0) {
		t.Error("a retransmitting handshake past the timeout did not trip")
	}
}

// A pure TCP SYN never becomes a one-way stream. Even an unusually persistent
// dial must remain eligible for re-racing after the UDP ambiguity budget is
// exhausted; otherwise a long caller timeout can strand it permanently.
func TestSynWaitTCPHandshakeNeverExhaustsResponseBudget(t *testing.T) {
	update := &multiClientChannelUpdate{}
	client := &multiClientChannel{settings: DefaultMultiClientSettings()}
	tcpPath := udpTestPath(4)
	tcpPath.Protocol = IpProtocolTcp
	tcpPath.Syn = true

	if update.synWaitExceeded(client, tcpPath, time.Hour) {
		t.Fatal("the first SYN tripped the silence clock")
	}
	for range dialProbeMaxSends {
		if update.synWaitExceeded(client, tcpPath, time.Hour) {
			t.Fatal("a SYN inside a deliberately wide timeout tripped the silence clock")
		}
	}
	if !update.synWaitExceeded(client, tcpPath, 0) {
		t.Fatal("a TCP handshake aged into the one-way UDP exemption")
	}
}

// A one-way stream is not a handshake: past dialProbeMaxSends sends with
// nothing back, the inference must leave the flow alone -- re-racing a live
// stream drops its in-flight responses for no diagnostic gain. This is the
// TestMultiClientUdp6 regression: its udp/53 pump was churned whenever the
// first echo ran past the wait under load.
func TestSynWaitStreamIsExempt(t *testing.T) {
	update := &multiClientChannelUpdate{}
	client := &multiClientChannel{settings: DefaultMultiClientSettings()}
	udpPath := udpTestPath(4)

	update.synWaitExceeded(client, udpPath, 0) // starts the clock at count 1
	for range dialProbeMaxSends - 1 {
		update.synWaitExceeded(client, udpPath, time.Hour) // burn the budget, no trip
	}
	// the budget is spent; even a long-exceeded timeout must not trip
	if update.synWaitExceeded(client, udpPath, 0) {
		t.Error("a flow past the probe budget was re-raced: streams belong to the blackhole detector")
	}

	// a re-race onto another exit re-keys clock and budget, so the flow is
	// judged fresh where it actually dials fresh
	other := &multiClientChannel{settings: DefaultMultiClientSettings()}
	if update.synWaitExceeded(other, udpPath, 0) {
		t.Fatal("first probe on a fresh exit must only start the clock")
	}
	if !update.synWaitExceeded(other, udpPath, 0) {
		t.Error("the re-keyed budget did not allow a fresh handshake to trip")
	}
}

// The predicate is only worth anything if the egress path consults it -- pin
// the call site.
func TestSendPathInferenceUsesDialProbePacket(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *RemoteUserNatMultiClient) sendParsedPacketGroup(")
	if !ok {
		t.Fatal("could not find sendParsedPacketGroup")
	}
	if !strings.Contains(body, "dialProbePacket(ipPath)") {
		t.Error("sendParsedPacketGroup does not gate the dial-failure inference on dialProbePacket: udp handshakes have lost their early escape")
	}
}
