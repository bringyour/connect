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
