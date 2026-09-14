//go:build flightgate_next

package connect

// FLIGHTGATEFIX §20.3. The fast path's liveness reporter, its
// old-receiver compatibility and §13.6's size-aware admission. All three
// are removed from the landing: the reporter cost 13 to 57 % on every
// forced-direct repetition and the size cap cost 0.8 and 2.0 Mbit/s of
// goodput. Kept as the specification of §20.5's follow-up (2), an
// ack-progress watchdog with no wire cost, so what was learned about the
// reverse lane is not lost with the mechanism.

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pion/transport/v4/vnet"
)

// Compatibility for 13.3: a peer from before progress reports drops each
// report as one malformed fragment (fastDropCount) and never reports back,
// so a new sender facing it must not retire a healthy lane.
func TestFastPathProgressReportIsHarmlessToOldReceiver(t *testing.T) {
	if testing.Short() {
		t.Skip("vnet fast path compatibility")
	}
	const noProgressTimeout = 300 * time.Millisecond
	pair := newFlightGateVnetPair(t, nil, func(active, passive *WebRtcSettings) {
		active.FastPathNoProgressTimeout = noProgressTimeout
		passive.FastPathNoProgressTimeout = noProgressTimeout
		passive.oldStyleFastPathReceiverForTest = true
	})
	// the old receiver still gets every message
	if loss := measureFastPathMessageLoss(t, pair, 1000, 20); loss != 0 {
		t.Fatalf("old-style receiver lost %.2f of the messages", loss)
	}
	// the new sender keeps writing for longer than its bound and is not retired
	message := bytes.Repeat([]byte{0x3c}, 1000)
	deadline := time.Now().Add(3 * noProgressTimeout)
	for time.Now().Before(deadline) {
		if _, err := pair.activeFast.WriteFastPathMessage(message); err != nil {
			t.Fatal(err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case <-pair.active.ctx.Done():
		t.Fatalf("new sender retired a healthy lane to an old receiver: %v", context.Cause(pair.active.ctx))
	default:
	}
	activeFast := pair.active.fastPath.Load()
	passiveFast := pair.passive.fastPath.Load()
	// the active side received messages from nobody, so it sent no reports;
	// the passive side received and would have reported: with the old-style
	// parse it sent none, and every report the active side does send to it
	// counts as one drop, no more
	if activeFast.remoteReportSeen.Load() {
		t.Fatal("an old-style receiver produced a progress report")
	}
	if sent := passiveFast.progressReportsSent.Load(); sent != 0 {
		t.Fatalf("old-style receiver sent %d reports", sent)
	}
	// a report from the old side is impossible, so drops on the old side come
	// only from reports the new side sent for messages it received: none here
	if drops := pair.passiveStats.Snapshot().FastDropCount; drops != activeFast.progressReportsSent.Load() {
		t.Fatalf("old-style receiver drops = %d, reports sent to it = %d", drops, activeFast.progressReportsSent.Load())
	}
	// now the new side receives one message, reports, and the old side must
	// drop exactly that report without any other effect
	if loss := measureFastPathMessageLossReverse(t, pair, 1000, 1); loss != 0 {
		t.Fatal("the new side did not receive the reverse message")
	}
	time.Sleep(4 * p2pFastPathProgressReportInterval)
	sent := activeFast.progressReportsSent.Load()
	if sent == 0 {
		t.Fatal("the new side received a message and reported nothing")
	}
	deadline = time.Now().Add(time.Second)
	for pair.passiveStats.Snapshot().FastDropCount < sent {
		if deadline.Before(time.Now()) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if drops := pair.passiveStats.Snapshot().FastDropCount; drops != sent {
		t.Fatalf("old-style receiver dropped %d packets for %d reports", drops, sent)
	}
	select {
	case <-pair.passive.ctx.Done():
		t.Fatalf("old-style receiver was retired: %v", context.Cause(pair.passive.ctx))
	default:
	}
}

// §13.6: with size-aware admission on, the native carrier refuses frames
// over FastPathMaximumFragmentCount fragments, and once the Transfer flight
// sits at its loss floor the sequence writes frames over
// FastPathLossyFragmentCount fragments reliable-only. Off by default.
func TestP2pSizeAwareAdmissionBoundsFastPathFrames(t *testing.T) {
	settings := DefaultP2pTransportSettings()
	if settings.FastPathSizeAwareAdmission {
		t.Fatal("size-aware admission must be off by default until the benchmark sweep")
	}
	if settings.FastPathMaximumFragmentCount != 8 || settings.FastPathLossyFragmentCount != 2 {
		t.Fatalf("defaults = %d/%d fragments, want 8/2", settings.FastPathMaximumFragmentCount, settings.FastPathLossyFragmentCount)
	}
	settings.FastPathSizeAwareAdmission = true
	settings.DataPlaneMode = P2pDataPlaneModeFastOnly
	send := &P2pSendTransport{settings: settings}
	properties := p2pTransferCarrierProperties(send)
	if !properties.Unreliable {
		t.Fatal("fast-only carrier is not unreliable")
	}
	small := 2 * p2pFastPathFragmentPayloadByteCount
	large := 9 * p2pFastPathFragmentPayloadByteCount
	if !properties.unreliableForMessageByteCount(small) {
		t.Fatal("a small frame was refused by the fast path")
	}
	if properties.unreliableForMessageByteCount(large) {
		t.Fatal("a frame over the fragment cap was admitted to the fast path")
	}
	if properties.unreliableLossyMaxMessageByteCount != ByteCount(2*p2pFastPathFragmentPayloadByteCount) {
		t.Fatalf("lossy cap = %d bytes", properties.unreliableLossyMaxMessageByteCount)
	}

	sendSettings := DefaultSendBufferSettings()
	sendSettings.UnreliableInitialFlightByteCount = 8192
	sendSettings.UnreliableMinimumFlightByteCount = 8192
	sendSettings.UnreliableMaximumFlightByteCount = 65536
	controller := newSendFlightController(sendSettings)
	policy := transferFlightPolicySnapshot{
		generation:             1,
		limited:                true,
		reliableRouteAvailable: true,
		lossyMaxByteCount:      properties.unreliableLossyMaxMessageByteCount,
	}
	controller.applyPolicy(policy)
	sequence := &SendSequence{client: &Client{}, flightController: controller, sendBufferSettings: sendSettings}
	if sequence.reliableOnlyWrite(policy, ByteCount(3*p2pFastPathFragmentPayloadByteCount)) {
		t.Fatal("a growing flight wrote a mid-size frame reliable-only")
	}
	controller.reduceForLoss()
	if !controller.atFloor() {
		t.Fatal("one loss from the initial limit did not pin the flight to its floor")
	}
	if !sequence.reliableOnlyWrite(policy, ByteCount(3*p2pFastPathFragmentPayloadByteCount)) {
		t.Fatal("at the floor a frame over the lossy cap still rode the fast path")
	}
	if sequence.reliableOnlyWrite(policy, ByteCount(p2pFastPathFragmentPayloadByteCount)) {
		t.Fatal("at the floor a one-fragment frame was pushed off the fast path")
	}
}

// MEMSTEADY gate for §13.3: a progress report costs no more allocations than
// the warmup marker the fast path already sends, so the reporter adds
// nothing per interval beyond the RTP writer's own work.
func TestFastPathProgressReportAllocatesLikeWarmup(t *testing.T) {
	if testing.Short() {
		t.Skip("vnet fast path allocation check")
	}
	pair := newFlightGateVnetPair(t, nil, nil)
	fastPath := pair.active.fastPath.Load()
	warmup := testing.AllocsPerRun(200, func() {
		_ = fastPath.writeWarmup()
	})
	report := testing.AllocsPerRun(200, func() {
		_ = fastPath.writeProgressReport(42)
	})
	t.Logf("allocations per packet: warmup=%.1f report=%.1f", warmup, report)
	if warmup < report {
		t.Fatalf("a progress report allocates %.1f per packet, the warmup marker %.1f", report, warmup)
	}
}

// FLIGHTGATEFIX §20.3. This is merged's own test and it is red on merged:
// merged records FastPathNoProgressTimeout and acts on nothing, so no
// watchdog retires a blackholed association. §13.3's reporter was the
// mechanism that made it pass, and it cost 13 to 57 per cent on every
// forced-direct repetition. It stays here as the specification of §20.5's
// follow-up (2), an ack-progress watchdog with no wire cost.
// M6. After the fast path is ready, its RTP packets are blackholed in the
// active-to-passive direction while STUN consent and DTLS keep flowing. The
// association must be retired within the configured no-progress bound so the
// route generation changes and the sender's flight resets. Expected red on
// the tree this was written against: nothing observes fast-path delivery.
func TestFastPathBlackholeRetiresRouteAndResetsFlight(t *testing.T) {
	if testing.Short() {
		t.Skip("vnet fast path blackhole")
	}
	const noProgressTimeout = 300 * time.Millisecond
	var blackhole atomic.Bool
	activeIp := net.ParseIP("10.3.0.1")
	filter := func(chunk vnet.Chunk) bool {
		if !blackhole.Load() || !rtpUdpPayload(chunk.UserData()) {
			return true
		}
		source, ok := chunk.SourceAddr().(*net.UDPAddr)
		return !ok || !source.IP.Equal(activeIp)
	}
	pair := newFlightGateVnetPair(t, filter, func(active, passive *WebRtcSettings) {
		active.FastPathNoProgressTimeout = noProgressTimeout
	})
	// a healthy lane delivers and is never retired by the bound
	if loss := measureFastPathMessageLoss(t, pair, 1000, 20); loss != 0 {
		t.Fatalf("healthy fast path lost %.2f of its messages", loss)
	}
	select {
	case <-pair.active.ctx.Done():
		t.Fatalf("healthy association was retired: %v", context.Cause(pair.active.ctx))
	case <-time.After(2 * noProgressTimeout):
	}

	blackhole.Store(true)
	writeDone := make(chan struct{})
	go func() {
		defer close(writeDone)
		message := bytes.Repeat([]byte{0x3c}, 1000)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-pair.active.ctx.Done():
				return
			case <-ticker.C:
				if _, err := pair.activeFast.WriteFastPathMessage(message); err != nil {
					return
				}
			}
		}
	}()
	// ICE consent is still flowing: this is not the ordinary ICE failure path
	time.Sleep(noProgressTimeout / 2)
	if state := pair.active.pc.ICEConnectionState(); state != webrtc.ICEConnectionStateConnected &&
		state != webrtc.ICEConnectionStateCompleted {
		t.Fatalf("ICE did not stay connected through the RTP blackhole: %s", state)
	}
	select {
	case <-pair.active.ctx.Done():
	case <-time.After(3 * noProgressTimeout):
		t.Fatalf("fast path blackhole did not retire the association within %s", 3*noProgressTimeout)
	}
	<-writeDone
	cause := context.Cause(pair.active.ctx)
	if cause == nil || !strings.Contains(cause.Error(), "fast path no progress") {
		t.Fatalf("retirement cause = %v, want fast path no progress", cause)
	}
}

// FLIGHTGATEFIX §20.3. §13.2's hybrid H3 parity: a hybrid carrier keeps
// its reply affinity where a datagram-only one does not. The landing has
// merged's blanket reply rule, so this is the specification of §20.5's
// follow-up (1), reply affinity with a storm guard.
// M2 guard (review finding 2 of FLIGHTGATEFIX §4). With H1 and a hybrid H3
// carrier both active and H3 healthy, an ACK for a Pack received over H3
// keeps its H3 affinity. Passes today; a fall-through scoped to "any
// potentially unreliable carrier" would break it.
func TestReceiveSequenceAckKeepsHybridH3Affinity(t *testing.T) {
	pair := newFlightGatePeerPair(t, 3*time.Second)
	inH3 := pair.receiveRoute(t, TransportTypeH3)
	outH3 := pair.ackRoute(t, TransportTypeH3, 16, TransferCarrierProperties{
		Unreliable: true,
		unreliableForMessageByteCount: func(int) bool {
			return false
		},
	})
	outH1 := pair.ackRoute(t, TransportTypeH1, 16, TransferCarrierProperties{})

	first := pair.deliver(t, 0, inH3)
	start := time.Now()
	if !awaitFlightGateAck(t, outH3, first, 5*time.Second) {
		t.Fatal("ACK for the H3-received Pack left the healthy H3 carrier")
	}
	t.Logf("H3 ACK latency %s", time.Since(start))
	if awaitFlightGateAck(t, outH1, first, 200*time.Millisecond) {
		t.Fatal("ACK also appeared on H1")
	}
}
