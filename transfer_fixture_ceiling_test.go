package connect

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// What this in-process fixture can carry, measured rather than assumed.
//
// It matters because every window cell in this program runs here, and a window
// sweep is only meaningful below whatever this ceiling is: above it, a larger
// window reads inside the constant arm's null band for a reason that has
// nothing to do with the tree. That is the shape of the 50 Mb/s wall the record
// names, and it has been treated as a property of the transfer layer rather
// than of the harness.
//
// The arrangement isolates the ceiling from the window deliberately. The window
// is a 4 MiB constant with the rule off, and the propagation is 1 ms, so the
// window permits about 4 GB/s and cannot be the binder; the carrier is unpaced,
// so no rate limit is imposed; and delivery is counted at the receiver, since
// the sender's write count is admission rather than goodput.
//
// The payload sweep is what identifies the binder. If the ceiling is flat in
// bytes per second it is copy or serialisation bandwidth; if it is flat in
// frames per second it is per-frame plumbing, which for this fixture is the
// goroutine it spawns per frame in the delay pump. Those are different findings
// and only the second is an instrument defect.
//
// Derived rather than counted, and named as such: the frame rate below is
// delivered bytes divided by the payload size, not a count of frames on the
// wire, so it assumes one payload per frame and would misread if the sequence
// coalesced several.
func TestTheFixturePayloadCeiling(t *testing.T) {
	assertMessagePoolOwnership(t)

	const propagation = time.Millisecond
	const window = ByteCount(4 * 1024 * 1024)
	const offerWindow = 2 * time.Second

	measure := func(payloadByteCount int, bufferScale int) (float64, float64) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		harness := newSendWindowHarnessWithClient(t, ctx, propagation,
			func(settings *SendBufferSettings) {
				settings.ResendQueueMaxByteCount = window
			},
			func(settings *ClientSettings) {
				settings.SendBufferSize *= bufferScale
				settings.ReceiveBufferSettings.SequenceBufferByteCount *= ByteCount(bufferScale)
				settings.ReceiveBufferSettings.H1SequenceBufferByteCount *= ByteCount(bufferScale)
			})
		delivered := &atomic.Int64{}
		harness.receiver.AddReceiveCallback(
			func(_ TransferPath, frames []*protocol.Frame, _ Peer) {
				delivered.Add(int64(len(frames)) * int64(payloadByteCount))
			},
		)
		start := time.Now()
		harness.offer(t, payloadByteCount, offerWindow)
		elapsed := time.Since(start)
		bytesPerSecond := float64(delivered.Load()) / elapsed.Seconds()
		return bytesPerSecond, bytesPerSecond / float64(payloadByteCount)
	}

	for _, bufferScale := range []int{1, 2} {
		for _, payloadByteCount := range []int{1024, 4 * 1024, 16 * 1024} {
			bytesPerSecond, framesPerSecond := measure(payloadByteCount, bufferScale)
			t.Logf(
				"buffers x%d, payload %d: %.1f MB/s (%.0f Mb/s), %.0f frames/s",
				bufferScale, payloadByteCount,
				bytesPerSecond/1e6, bytesPerSecond*8/1e6, framesPerSecond,
			)
		}
	}
}
