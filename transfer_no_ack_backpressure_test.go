package connect

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// A no-acknowledgement Pack asks for delivery out of sequence with no
// acknowledgement and no retry. The property that decides whether the mode is
// worth anything: such a Pack must never be queued behind a full retransmit
// buffer, and must never be dropped because of one.
//
// Both halves matter and the second is worse. Blocking costs latency and is
// visible. Dropping is silent, and a packet forwarder that discards traffic
// because a reliability buffer it does not use is full is a defect that looks
// like ordinary loss — which is why this row counts rather than infers, the
// same class of instrument as the eviction counter, which existed nowhere and
// was hiding a real defect.
//
// Four mechanisms can produce either, and a row covering one would pass while
// the others failed, so this drives all four at once:
//
//   - the pack channel into the sequence, bounded and first-in-first-out, so a
//     no-acknowledgement pack behind a full channel of reliable ones waits;
//   - the resend capacity gate, which admits to the wire only while the byte
//     bound has room, and is the one a caller is most directly asking about: a
//     pack blocked by a buffer it will never occupy;
//   - the ordered sequence goroutine, where a stalled reliable pack at the head
//     delays everything behind it whatever its semantics;
//   - the memory budget admission, if a no-acknowledgement pack draws on the
//     same pool.
//
// The shape: fill the sequence's resend queue to its bound with reliable
// traffic whose acknowledgements are withheld, so it stays full, then send
// no-acknowledgement packs through the same sequence to the same destination.
//
// Promptness is asserted against the path rather than against a constant,
// because a timing threshold is what makes a test flake. The discriminating
// comparison is the same pack sent with an empty queue.
//
// Predictions, recorded before the run: every no-acknowledgement pack offered
// is written and delivered, offered equals written, and the time to delivery
// behind a full queue is within a small multiple of the same pack's time with
// an empty one — the carrier's service time rather than a resend interval.
//
// THIS ROW FAILS ON THE TREE AS IT STANDS, WHICH IS THE RESULT. Measured: with
// an empty queue, 20 offered, 20 written, 0 refused, all delivered, worst
// 740 microseconds. Behind a full resend queue, 20 offered, ZERO written,
// twenty refused, none delivered.
//
// So the drop is real and it is earlier than expected. The send loop's bypass
// of the resend capacity gate exists and is correct — a no-acknowledgement
// pack is eligible there regardless of capacity or the flight gate — but it is
// never reached, because admission into the sequence sits in front of it and
// refuses first. The pack never enters the scheduler the bypass selects from.
//
// Nothing counted that before this row: the refusal returns false to the
// caller, and an IP caller drops the packet with no counter anywhere, which is
// the same shape this program has now found three times — a real harm
// invisible because nothing counts it. The counters are the deliverable as
// much as the assertion is.
func TestANoAckPackIsNotHeldOrDroppedByAFullResendQueue(t *testing.T) {
	assertMessagePoolOwnership(t)

	const propagation = 10 * time.Millisecond
	const window = ByteCount(256 * 1024)
	const payloadByteCount = 1024
	const reliableCount = 400
	const noAckCount = 20

	run := func(fill bool) (delivered int, latencies []time.Duration, offered, written, refused uint64) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		harness := newSendWindowHarness(t, ctx, propagation,
			func(settings *SendBufferSettings) {
				settings.ResendQueueMaxByteCount = window
			})

		marker := "no-ack-marker"
		arrived := make(chan time.Time, noAckCount*4)
		harness.receiver.AddReceiveCallback(
			func(_ TransferPath, frames []*protocol.Frame, _ Peer) {
				now := time.Now()
				for _, frame := range frames {
					message, err := FromFrame(frame)
					if err != nil {
						continue
					}
					if simple, ok := message.(*protocol.SimpleMessage); ok &&
						len(simple.Content) == len(marker) && simple.Content == marker {
						select {
						case arrived <- now:
						default:
						}
					}
				}
			},
		)

		if fill {
			// withhold every acknowledgement, so the resend queue fills and
			// stays full however much drains
			harness.holdAcks.Store(true)
			payload := string(make([]byte, payloadByteCount))
			var filling sync.WaitGroup
			filling.Add(1)
			go func() {
				defer filling.Done()
				for range reliableCount {
					frame := RequireToFrameWithDefaultProtocolVersion(
						&protocol.SimpleMessage{Content: payload},
					)
					admitted, _ := harness.sender.SendWithTimeoutDetailed(
						frame,
						harness.receiverId,
						nil,
						50*time.Millisecond,
						sendPackRecoveryOption{upstreamRecoverable: true},
					)
					if !admitted {
						MessagePoolReturn(frame.MessageBytes)
					}
				}
			}()
			// let the queue reach its bound before the no-ack traffic starts
			time.Sleep(500 * time.Millisecond)
			defer filling.Wait()
		}

		sent := 0
		for range noAckCount {
			frame := RequireToFrameWithDefaultProtocolVersion(
				&protocol.SimpleMessage{Content: marker},
			)
			start := time.Now()
			admitted, _ := harness.sender.SendWithTimeoutDetailed(
				frame,
				harness.receiverId,
				nil,
				time.Second,
				NoAck(),
			)
			if !admitted {
				MessagePoolReturn(frame.MessageBytes)
				continue
			}
			sent += 1
			select {
			case at := <-arrived:
				latencies = append(latencies, at.Sub(start))
			case <-time.After(5 * time.Second):
			}
		}
		_ = sent

		// let anything still in flight land before the counters are read
		time.Sleep(200 * time.Millisecond)
		stats := harness.sender.ReceiveStats()
		return len(latencies), latencies,
			stats.SendNoAckOfferedCount, stats.SendNoAckWriteCount, stats.SendNoAckRefusedCount
	}

	idleDelivered, idleLatencies, idleOffered, idleWritten, idleRefused := run(false)
	fullDelivered, fullLatencies, fullOffered, fullWritten, fullRefused := run(true)

	worst := func(latencies []time.Duration) time.Duration {
		worst := time.Duration(0)
		for _, latency := range latencies {
			worst = max(worst, latency)
		}
		return worst
	}
	t.Logf(
		"empty queue: %d/%d delivered, worst %s, offered %d written %d refused %d",
		idleDelivered, noAckCount, worst(idleLatencies), idleOffered, idleWritten, idleRefused,
	)
	t.Logf(
		"full queue:  %d/%d delivered, worst %s, offered %d written %d refused %d",
		fullDelivered, noAckCount, worst(fullLatencies), fullOffered, fullWritten, fullRefused,
	)

	if idleDelivered != noAckCount {
		t.Fatalf(
			"%d of %d no-acknowledgement packs arrived with an empty queue, so the cell cannot read the full one",
			idleDelivered, noAckCount,
		)
	}
	// the drop half, which is the worse one
	if fullOffered != fullWritten {
		t.Errorf(
			"%d no-acknowledgement packs were offered and %d reached the write path, %d refused by admission; a mode with no retry that is dropped because a retransmit buffer it never occupies is full loses traffic silently and looks like ordinary loss",
			fullOffered, fullWritten, fullRefused,
		)
	}
	if fullDelivered != noAckCount {
		t.Errorf(
			"%d of %d no-acknowledgement packs arrived behind a full resend queue",
			fullDelivered, noAckCount,
		)
	}
	// and the blocking half, against the path rather than a constant
	if bound := 10 * max(worst(idleLatencies), time.Millisecond); bound < worst(fullLatencies) {
		t.Errorf(
			"a no-acknowledgement pack took %s behind a full resend queue against %s with an empty one; it should leave within the carrier's own service time, not within a resend interval, because it will never occupy the buffer that is full",
			worst(fullLatencies), worst(idleLatencies),
		)
	}
}
