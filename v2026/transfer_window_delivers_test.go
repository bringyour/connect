package connect

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// The contract, asserted as a property rather than as a mechanism: a flow with
// no loss and no queueing should deliver what its window permits.
//
// Why it is written this way. Every named candidate for the shortfall has been
// eliminated by a reading chosen in advance to eliminate it — the window, the
// peer's advertised capacity, admission, a standing queue, the harness's own
// offer path, the estimate's arithmetic, and the acknowledgement handoff, whose
// predicted mechanism was refuted by four counters all reading zero. So this
// row deliberately asserts no mechanism at all. It states the contract and
// measures against it, which means it survives the next hypothesis too, and it
// will still be the right test when the cause is finally found.
//
// The preconditions are asserted rather than assumed, because the contract only
// binds when they hold: nothing refused at the hold, nothing evicted, and
// nothing retransmitted. If a run shows any of those, the shortfall it measures
// is explained and the row says so rather than blaming the window.
//
// The carrier is paced well above what the window permits, so the carrier is
// not the binder. That trap has caught this program twice: an unpaced
// in-process carrier rewards permission far past the bandwidth-delay product
// because it runs a goroutine per frame, and a deep route channel scores a
// larger window higher while delivering the same bytes. Here the pace is three
// times the permitted rate and delivery is counted at the receiver.
//
// Prediction, recorded before the run: with a 512 KiB window at a 200 ms round
// trip the flow is permitted 2.6 MB/s, and the user's observation is that it
// delivers about half.
//
// Measured, and the prediction did not hold here. The flow delivers 0.88 to
// 0.91 of what its window permits, and the residual is framing rather than a
// shortfall: the window is framed bytes and this counts payload, and §36.3's
// derived goodput factor is 0.845, so 0.88 to 0.91 is delivery of everything
// the window permits and a little more than the factor predicts.
//
// The discriminator matters more than the number. Holding the permitted rate
// constant and varying the window fourfold — 512 KiB at 200 ms, 1 MiB at
// 400 ms, 2 MiB at 800 ms, all permitting about 2.6 MB/s — the ratio is flat at
// 0.88, 0.90, 0.91. So whatever produces the shortfall elsewhere does not scale
// with the window and does not reproduce in this fixture at any of three sizes.
// That is a negative result chosen to be informative: it removes the window
// itself, and this fixture's send and receive path, from the search.
//
// The bar is therefore set to distinguish framing from the observation it was
// written for. At 0.85 it passes a healthy flow and fails decisively on a flow
// delivering about half.
func TestAFlowDeliversWhatItsWindowPermits(t *testing.T) {
	assertMessagePoolOwnership(t)

	const propagation = 200 * time.Millisecond
	const window = ByteCount(512 * 1024)
	// three times what the window permits, so the window is what binds
	const bytesPerSecond = ByteCount(8 * 1000 * 1000)
	const payloadByteCount = 4 * 1024
	const offerWindow = 6 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := newRateLimitedSendWindowHarness(t, ctx, propagation, bytesPerSecond,
		func(settings *SendBufferSettings) {
			settings.ResendQueueMaxByteCount = window
		})
	harness.receiveHold(window)
	delivered := &atomic.Int64{}
	harness.receiver.AddReceiveCallback(
		func(_ TransferPath, frames []*protocol.Frame, _ Peer) {
			delivered.Add(int64(len(frames)) * payloadByteCount)
		},
	)

	start := time.Now()
	harness.offer(t, payloadByteCount, offerWindow)
	elapsed := time.Since(start)

	stats := harness.sender.DestinationSendStats(harness.receiverId)
	receiveStats := harness.receiver.ReceiveStats()
	permitted := float64(stats.SendWindow.Window) / stats.Rtt.Min.Seconds()
	achieved := float64(delivered.Load()) / elapsed.Seconds()

	t.Logf(
		"window %d over a %s minimum round trip permits %.2f MB/s; delivered %.2f MB/s (%.2f of it). resends %d bytes, receiver drops %d, evictions %d",
		stats.SendWindow.Window, stats.Rtt.Min, permitted/1e6, achieved/1e6,
		achieved/permitted, stats.ResendWriteByteCount,
		receiveStats.ReceiveQueueDropCount, receiveStats.ReceiveQueueEvictionCount,
	)

	// the preconditions the contract binds under
	if 0 < receiveStats.ReceiveQueueDropCount || 0 < receiveStats.ReceiveQueueEvictionCount {
		t.Fatalf(
			"the receiver refused %d arrivals and evicted %d, so this run's shortfall is explained and the contract does not bind on it",
			receiveStats.ReceiveQueueDropCount, receiveStats.ReceiveQueueEvictionCount,
		)
	}
	if 0 < stats.ResendWriteByteCount {
		t.Fatalf(
			"%d bytes were retransmitted, so this run had loss and the contract does not bind on it",
			stats.ResendWriteByteCount,
		)
	}

	if achieved < 0.85*permitted {
		t.Errorf(
			"the flow delivered %.2f MB/s against the %.2f MB/s its %d byte window permits over a %s round trip, %.2f of it, with no loss and no queueing; a window is permission to have that many bytes outstanding, and a flow that never loses one and never waits behind one should be limited by nothing else",
			achieved/1e6, permitted/1e6, stats.SendWindow.Window,
			stats.Rtt.Min, achieved/permitted,
		)
	}
}
