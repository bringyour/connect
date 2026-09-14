package connect

import (
	"context"
	"net/netip"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Regression tests for the no-receive-syn evidence chain, ported from the
// pre-merge isBlackholeAt suite onto the verdict architecture
// (blackholeReasonFromStats / comparativeConnectTimeout / verdictAction).
// Two defects are pinned, both found by deterministic reproduction of the
// multiclient cold-start stall (tunnel handshake completes, first byte never
// arrives, no recovery at 180s):
//
//  1. The syn clock must outlive the stats window. The windowed
//     firstSendSynTime has the same ~StatsWindowDuration ceiling
//     BlackholeReceiveTimeout has, so with BlackholeConnectTimeout at the
//     same scale the evidence aged out exactly as the bar matured — the
//     verdict could barely ever fire and a silent route held its flows
//     indefinitely. The lifetime attempt marker (firstUnansweredSendSynTime)
//     fixes this; it is armed by the first SYN of an attempt, held through
//     retransmits, and cleared by any received SYN.
//
//  2. A single-destination window needs a sibling-free path to the short
//     connect bar. A user-selected network peer has exactly one candidate by
//     construction and can never produce comparativeReceivingSiblings, so
//     without the own-send-ack proof it could only ever reach the full bar —
//     30s to reclaim a dead route on a first-class connection.

// evidenceTestChannel builds the minimal bare channel the counter/clock tests
// drive directly. It mirrors the suite's other bare fixtures: zero-value
// hooks, no client, no transports.
func evidenceTestChannel(ctx context.Context, settings *MultiClientSettings) *multiClientChannel {
	log := NewNoopLogger()
	return &multiClientChannel{
		ctx:                       ctx,
		client:                    &Client{ctx: ctx},
		args:                      &multiClientChannelArgs{Destination: RequireMultiHopId(NewId())},
		createTime:                time.Now(),
		log:                       log,
		settings:                  settings,
		eventBuckets:              []*multiClientEventBucket{},
		ip4DestinationSourceCount: map[Ip4Path]map[Ip4Path]int{},
		ip6DestinationSourceCount: map[Ip6Path]map[Ip6Path]int{},
		packetStats:               &clientWindowStats{log: log},
	}
}

// TestMultiClientUnansweredConnectIsBlackhole: a connect attempt unanswered
// past the bar must produce the no-receive-syn verdict — and the verdict must
// come from the LIFETIME marker, because the windowed clock has already aged
// out by the time the bar matures (the second leg pins exactly that
// pre-marker failure mode: same silence, windowed evidence gone, no verdict).
func TestMultiClientUnansweredConnectIsBlackhole(t *testing.T) {
	settings := DefaultMultiClientSettings()
	now := time.Now()

	stats := &clientWindowStats{
		firstUnansweredSendSynTime: now.Add(-(settings.BlackholeConnectTimeout + time.Second)),
	}
	reason, held := blackholeReasonFromStats(
		now,
		stats,
		settings.BlackholeTimeout,
		0,
		settings.BlackholeConnectTimeout,
		blackholeGates{},
	)
	if reason != blackholeNoReceiveSyn || held != blackholeNone {
		t.Fatalf(
			"a connect attempt unanswered for %s must be the no-receive-syn verdict; got reason=%q held=%q — the flow pinned to this route has nothing else to reclaim it",
			settings.BlackholeConnectTimeout, reason, held,
		)
	}

	// the pre-marker failure mode: the same silence with only the windowed
	// clock, which the bucket trim has already emptied. No verdict — this is
	// the defect the marker exists for, kept here as the failing-first
	// contrast.
	aged := &clientWindowStats{}
	reason, _ = blackholeReasonFromStats(
		now,
		aged,
		settings.BlackholeTimeout,
		0,
		settings.BlackholeConnectTimeout,
		blackholeGates{},
	)
	if reason != blackholeNone {
		t.Fatalf("aged-out windowed evidence alone must not fire (got %q): this leg documents the pre-marker behavior", reason)
	}
}

// TestMultiClientAnsweredConnectIsNotBlackhole is the guard against the churn
// regression: an established channel whose handshake has aged out of the
// stats window (both SYN counters zero, marker cleared by the answer) and
// which is carrying heavy traffic must never be a blackhole. A previous fix
// paired a lifetime send clock with the WINDOWED receive count and removed
// healthy routes carrying tens of MB, ~15 per run.
func TestMultiClientAnsweredConnectIsNotBlackhole(t *testing.T) {
	settings := DefaultMultiClientSettings()
	now := time.Now()

	stats := &clientWindowStats{
		// the received SYN cleared the attempt marker; the window has since
		// trimmed all syn history
		firstUnansweredSendSynTime: time.Time{},
		sendSynCount:               0,
		receiveSynCount:            0,
		sendAckCount:               15551,
		receiveAckCount:            14783,
	}
	reason, held := blackholeReasonFromStats(
		now,
		stats,
		settings.BlackholeTimeout,
		0,
		settings.BlackholeConnectTimeout,
		blackholeGates{},
	)
	if reason != blackholeNone || held != blackholeNone {
		t.Fatalf("an answered connect on a channel carrying traffic must never be a verdict (got reason=%q held=%q); removing it churns healthy routes", reason, held)
	}

	// and the windowed fallback must not resurrect a dead attempt once a SYN
	// was answered inside the window
	answered := &clientWindowStats{
		firstSendSynTime: now.Add(-(settings.BlackholeConnectTimeout + time.Second)),
		receiveSynCount:  1,
	}
	if since := unansweredConnectSince(answered); !since.IsZero() {
		t.Fatal("an answered window must not fall back to the windowed syn clock")
	}
}

// TestMultiClientReconnectAfterAnsweredConnectIsBlackhole pins the attempt
// lifecycle on the real counters: arm on the first SYN, HOLD through SYN
// retransmits (the device stack retransmits at 1s/2s/4s/8s — a "latest SYN"
// clock resets on every one and can never reach the threshold, which is
// exactly how the first version of this check stayed inert in integration),
// clear on a received SYN, and re-arm on the next attempt.
func TestMultiClientReconnectAfterAnsweredConnectIsBlackhole(t *testing.T) {
	settings := DefaultMultiClientSettings()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	channel := evidenceTestChannel(ctx, settings)

	synIpPath := &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        []byte{10, 0, 0, 1},
		SourcePort:      40001,
		DestinationIp:   []byte{10, 0, 0, 2},
		DestinationPort: 443,
		Syn:             true,
	}

	// arm
	channel.addSend(64, synIpPath)
	first := channel.packetStats.firstUnansweredSendSynTime
	if first.IsZero() {
		t.Fatal("the first SYN must arm the attempt marker")
	}
	// retransmits must not move it
	time.Sleep(5 * time.Millisecond)
	channel.addSend(64, synIpPath)
	channel.addSend(64, synIpPath)
	if !channel.packetStats.firstUnansweredSendSynTime.Equal(first) {
		t.Fatal("SYN retransmits must not reset the attempt clock: a retransmitting stack would keep the verdict from ever maturing")
	}
	// the answer clears it
	channel.addReceiveSyn(1)
	if !channel.packetStats.firstUnansweredSendSynTime.IsZero() {
		t.Fatal("a received SYN must clear the attempt marker: the route forwards connects")
	}
	// a new attempt re-arms, and matures on its own clock
	channel.addSend(64, synIpPath)
	rearmed := channel.packetStats.firstUnansweredSendSynTime
	if rearmed.IsZero() || rearmed.Equal(first) {
		t.Fatal("a new connect attempt after an answered one must arm a fresh marker")
	}

	stats, err := channel.WindowStats()
	if err != nil {
		t.Fatal(err)
	}
	// the maturity check runs at a synthetic future instant so the windowed
	// receive counters (receiveSynCount=1 from the answered attempt) are the
	// realistic aged-out zero: rebuild the corroboration the way the trim
	// would have left it
	stats.receiveSynCount = 0
	stats.receiveAckCount = 0
	reason, _ := blackholeReasonFromStats(
		rearmed.Add(settings.BlackholeConnectTimeout+time.Second),
		stats,
		settings.BlackholeTimeout,
		0,
		settings.BlackholeConnectTimeout,
		blackholeGates{},
	)
	if reason != blackholeNoReceiveSyn {
		t.Fatalf("a new unanswered connect attempt must be caught even though an earlier attempt was answered (got %q)", reason)
	}
}

// TestMultiClientLivePeerSilentConnectUsesComparativeTimeout covers bad-peer
// detection on a single-destination window — the network-peer case.
//
// The comparative bar exists for the moment the ambiguity of an unanswered
// connect is resolved by positive evidence that everything else works. Two
// receiving siblings are one such proof; the channel's OWN send acks are the
// other, and the only one a lone channel can ever have: a send ack is
// produced by the peer's receive sequence, so a peer that acks our packs
// while never answering a SYN is alive and simply not forwarding.
//
// Without the own-ack arm a lone channel waits the full bar, and this fails.
func TestMultiClientLivePeerSilentConnectUsesComparativeTimeout(t *testing.T) {
	settings := DefaultMultiClientSettings()
	if settings.BlackholeConnectComparativeTimeout <= 0 ||
		settings.BlackholeConnectTimeout <= settings.BlackholeConnectComparativeTimeout {
		t.Skip("comparative timeout is not configured shorter than the connect timeout")
	}
	now := time.Now()
	stats := &clientWindowStats{
		// unanswered past the comparative bar, but well inside the full one
		firstUnansweredSendSynTime: now.Add(-(settings.BlackholeConnectComparativeTimeout + time.Second)),
	}

	// no siblings (a single-destination window): the own-ack proof alone must
	// select the short bar
	got := comparativeConnectTimeout(
		now,
		stats,
		settings.BlackholeConnectTimeout,
		settings.BlackholeConnectComparativeTimeout,
		time.Time{},
		nil,
		func() bool { return true },
	)
	if got != settings.BlackholeConnectComparativeTimeout {
		t.Fatalf(
			"a live peer that never answers a connect must be judged at the comparative bar (%s), got %s; a network peer has no sibling and would wait the whole %s",
			settings.BlackholeConnectComparativeTimeout, got, settings.BlackholeConnectTimeout,
		)
	}

	// and the short bar must then mature the verdict
	reason, _ := blackholeReasonFromStats(
		now,
		stats,
		settings.BlackholeTimeout,
		0,
		got,
		blackholeGates{},
	)
	if reason != blackholeNoReceiveSyn {
		t.Fatalf("the shortened bar must mature the no-receive-syn verdict (got %q)", reason)
	}
}

// TestMultiClientSilentPeerKeepsConservativeTimeout is the other half:
// without positive evidence the path is healthy, the full bar must stand. A
// peer that is not acking may be unreachable because the local network is
// down, where reclaiming the route cannot help and the churn costs flow RSTs.
// This is the guard against the first fix attempted here, which shortened the
// wait with no evidence required and doubled route churn without improving
// anything.
func TestMultiClientSilentPeerKeepsConservativeTimeout(t *testing.T) {
	settings := DefaultMultiClientSettings()
	if settings.BlackholeConnectComparativeTimeout <= 0 ||
		settings.BlackholeConnectTimeout <= settings.BlackholeConnectComparativeTimeout {
		t.Skip("comparative timeout is not configured shorter than the connect timeout")
	}
	now := time.Now()
	stats := &clientWindowStats{
		firstUnansweredSendSynTime: now.Add(-(settings.BlackholeConnectComparativeTimeout + time.Second)),
		// nothing has been acknowledged: no proof the path is usable at all
	}

	for _, ownAck := range []func() bool{nil, func() bool { return false }} {
		got := comparativeConnectTimeout(
			now,
			stats,
			settings.BlackholeConnectTimeout,
			settings.BlackholeConnectComparativeTimeout,
			time.Time{},
			nil,
			ownAck,
		)
		if got != settings.BlackholeConnectTimeout {
			t.Fatalf("without evidence the path is healthy the conservative bar must stand (got %s); reclaiming during an outage churns routes and RSTs flows for nothing", got)
		}
	}

	// and it must still be reclaimed once the full bar matures
	reason, _ := blackholeReasonFromStats(
		now.Add(settings.BlackholeConnectTimeout),
		stats,
		settings.BlackholeTimeout,
		0,
		settings.BlackholeConnectTimeout,
		blackholeGates{},
	)
	if reason != blackholeNoReceiveSyn {
		t.Fatalf("an unanswered connect must still be reclaimed at the full connect timeout (got %q)", reason)
	}
}

// TestMultiClientStaleAckIsNotPathHealth pins the recency requirement on the
// own-ack proof: an acknowledgement from long ago proves nothing about now.
// hasRecentSendAck is the accessor detectBlackhole feeds the comparative cut.
func TestMultiClientStaleAckIsNotPathHealth(t *testing.T) {
	settings := DefaultMultiClientSettings()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	channel := evidenceTestChannel(ctx, settings)

	if channel.hasRecentSendAck(settings.BlackholeTimeout) {
		t.Fatal("a channel that never received a send ack has no liveness proof")
	}
	channel.stateLock.Lock()
	channel.lastSendAckTime = time.Now().Add(-10 * settings.BlackholeTimeout)
	channel.stateLock.Unlock()
	if channel.hasRecentSendAck(settings.BlackholeTimeout) {
		t.Fatal("a stale acknowledgement must not qualify as current path health")
	}
	channel.stateLock.Lock()
	channel.lastSendAckTime = time.Now()
	channel.stateLock.Unlock()
	if !channel.hasRecentSendAck(settings.BlackholeTimeout) {
		t.Fatal("a recent acknowledgement is the liveness proof")
	}
	if channel.hasRecentSendAck(0) {
		t.Fatal("a zero window disables the proof")
	}
}

// silentRouteGenerator yields one client whose sends are accepted by a route
// that never answers — the deterministic stand-in for a provider that goes
// silent between admission and first use.
type silentRouteGenerator struct {
	testingEmptyMultiClientGenerator
	settings *ClientSettings
}

func (self *silentRouteGenerator) NewClientArgs() (*MultiClientGeneratorClientArgs, error) {
	return &MultiClientGeneratorClientArgs{
		ClientId: NewId(),
	}, nil
}

func (self *silentRouteGenerator) NewClientSettings() *ClientSettings {
	return self.settings
}

func (self *silentRouteGenerator) NewClient(
	ctx context.Context,
	args *MultiClientGeneratorClientArgs,
	clientSettings *ClientSettings,
) (*Client, error) {
	client := NewClient(ctx, args.ClientId, NewNoContractClientOob(), clientSettings)
	// a route that accepts every write and never delivers anything back: the
	// send side stays healthy at the transport level while the peer is mute
	out := make(chan []byte, 1024)
	client.RouteManager().UpdateTransport(NewSendGatewayTransport(), []Route{out})
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-out:
			}
		}
	}()
	return client, nil
}

func (self *silentRouteGenerator) RemoveClientArgs(args *MultiClientGeneratorClientArgs) {}
func (self *silentRouteGenerator) RemoveClientWithArgs(client *Client, args *MultiClientGeneratorClientArgs) {
}

// newSilentRouteChannel builds a real channel over the silent route with the
// scaled detection constants, wiring only the hooks the connect-evidence
// tests need (nil-safe elsewhere, like the suite's other bare fixtures).
func newSilentRouteChannel(
	ctx context.Context,
	t *testing.T,
	settings *MultiClientSettings,
) *multiClientChannel {
	clientSettings := DefaultClientSettings()
	clientSettings.SendBufferSettings.AckTimeout = 5 * time.Minute
	generator := &silentRouteGenerator{
		settings: clientSettings,
	}
	args := &multiClientChannelArgs{
		MultiClientGeneratorClientArgs: MultiClientGeneratorClientArgs{ClientId: NewId()},
		Destination:                    RequireMultiHopId(NewId()),
	}
	channel, err := newMultiClientChannel(
		ctx,
		args,
		generator,
		func(*multiClientChannel, TransferPath, protocol.ProvideMode, TransportType, *IpPath, []byte) {},
		nil,
		DefaultSecurityPolicy(ctx),
		func(*ContractStatus) {},
		func([]*ContractStatsEvent) {},
		func() {},
		nil,
		settings,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	return channel
}

// silentRouteEvidenceSettings scales the detector constants down while
// preserving the RELATIONSHIP that makes the windowed clock unusable
// (StatsWindowDuration is not longer than BlackholeConnectTimeout), and
// pushes the busy-stale probe out of range so only the connect detector is
// in play.
func silentRouteEvidenceSettings() *MultiClientSettings {
	settings := DefaultMultiClientSettings()
	settings.StatsWindowDuration = 2 * time.Second
	settings.StatsWindowBucketDuration = 200 * time.Millisecond
	settings.BlackholeTimeout = 2 * time.Second
	// only the connect detector may act: the send-stall conviction and its
	// busy probe, and the idle ping, are pushed out of this test's reach
	settings.SendStallTimeout = 5 * time.Minute
	settings.BusyProbe = false
	settings.CPingTimeout = 5 * time.Minute
	settings.CPingRestTimeout = 5 * time.Minute
	return settings
}

func silentRouteSynPacket(t *testing.T) ([]byte, *IpPath) {
	synPacket := buildTestTcp4Packet(
		netip.MustParseAddr("10.0.0.1"), 40001,
		netip.MustParseAddr("10.0.0.2"), 443,
		0x02, 0,
	)
	var synIpPath IpPath
	if _, err := parseIpPathWithPayloadBorrowed(synPacket, &synIpPath); err != nil {
		t.Fatal(err)
	}
	if !synIpPath.Syn {
		t.Fatal("test packet is not a SYN")
	}
	return synPacket, &synIpPath
}

// TestMultiClientSilentRouteIsReclaimed is the end-to-end reproduction of the
// cold-start stall through the LIVE detector: a route that accepts a TCP
// connect attempt and never answers must be reclaimed, so the flow pinned to
// it can move. The integration test that first exposed this cannot arbitrate
// the fix — its failure rate swings between 1-in-8 and 7-in-8 across an hour
// on an idle machine — so this drives the same code path with no network and
// no provider. Without the lifetime attempt marker the windowed evidence ages
// out before the bar matures and the route is never reclaimed.
func TestMultiClientSilentRouteIsReclaimed(t *testing.T) {
	settings := silentRouteEvidenceSettings()
	settings.BlackholeConnectTimeout = 2 * time.Second
	// no sibling shortcut, no own-ack proof (the transfer acks never come
	// back on the silent route): this exercises the full-bar verdict
	settings.BlackholeConnectComparativeTimeout = 0

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	channel := newSilentRouteChannel(ctx, t, settings)
	defer channel.Cancel()

	synPacket, synIpPath := silentRouteSynPacket(t)
	channel.SendDetailed(&parsedPacket{packet: synPacket, ipPath: synIpPath}, 5*time.Second)

	deadline := time.After(settings.BlackholeConnectTimeout + 15*time.Second)
	select {
	case <-channel.Done():
	case <-deadline:
		t.Fatalf(
			"a route that never answered a connect attempt was never reclaimed after %s; every flow pinned to it is stranded",
			settings.BlackholeConnectTimeout,
		)
	}
}

// TestMultiClientLivePeerReclaimLatency measures, through the live detector,
// how long a single-destination window takes to reclaim a peer that answers
// at the transfer layer but never answers a connect — the network-peer
// bad-peer case. It asserts the latency is the comparative bar rather than
// the conservative one, which is the difference between a page that
// reconnects and a page that hangs. The SYN retransmit loop is load-bearing:
// a "latest SYN" clock resets on every retransmit and never matures — the
// inert-check failure mode the integration runs exposed.
func TestMultiClientLivePeerReclaimLatency(t *testing.T) {
	settings := silentRouteEvidenceSettings()
	settings.BlackholeConnectTimeout = 10 * time.Second
	settings.BlackholeConnectComparativeTimeout = 1 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	channel := newSilentRouteChannel(ctx, t, settings)
	defer channel.Cancel()

	// the peer keeps acknowledging at the transfer layer: alive and
	// reachable, but never answers the connect
	ackDone := make(chan struct{})
	defer close(ackDone)
	go func() {
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ackDone:
				return
			case <-ticker.C:
				channel.stateLock.Lock()
				channel.lastSendAckTime = time.Now()
				channel.stateLock.Unlock()
			}
		}
	}()

	synPacket, synIpPath := silentRouteSynPacket(t)
	start := time.Now()
	channel.SendDetailed(&parsedPacket{packet: synPacket, ipPath: synIpPath}, 5*time.Second)
	// SYN retransmits, faster than the device stack's own backoff
	go func() {
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ackDone:
				return
			case <-ticker.C:
				p, ip := silentRouteSynPacket(t)
				channel.SendDetailed(&parsedPacket{packet: p, ipPath: ip}, time.Second)
			}
		}
	}()

	// allow the detector's poll granularity (BlackholeTimeout/4) plus slack,
	// but require well under the conservative bar
	budget := settings.BlackholeConnectComparativeTimeout + 4*time.Second
	select {
	case <-channel.Done():
		elapsed := time.Since(start)
		t.Logf("reclaimed in %s (comparative=%s conservative=%s)",
			elapsed, settings.BlackholeConnectComparativeTimeout, settings.BlackholeConnectTimeout)
		if settings.BlackholeConnectTimeout <= elapsed {
			t.Fatalf("reclaim took %s: the conservative bar, not the comparative one", elapsed)
		}
	case <-time.After(budget):
		t.Fatalf(
			"a live peer that never answered a connect was not reclaimed within %s; a network peer window has no sibling and falls back to %s",
			budget, settings.BlackholeConnectTimeout,
		)
	}
}
