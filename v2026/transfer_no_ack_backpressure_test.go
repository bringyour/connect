package connect

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// two distinct flows, as production's IP layers key them
func testFlowOption(port int) sendSchedulingKeyOption {
	ipPath := IpPath{
		Version:         4,
		Protocol:        IpProtocolUdp,
		SourceIp:        net.ParseIP("10.11.12.13"),
		SourcePort:      port,
		DestinationIp:   net.ParseIP("93.184.216.34"),
		DestinationPort: 443,
	}
	return scheduleIpFlow(&ipPath)
}

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
//     same pool;
//   - and, after admission, a route write that fails or times out, where the
//     pack is discarded with no retry and the error reaches only an
//     acknowledgement callback that is a no-op for IP callers and an observer
//     that is nil by default. SendNoAckDiscardCount covers that one.
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
// What this row found, and what fixed it.
//
// Before: with an empty queue, 20 offered, 20 written, 0 refused, all
// delivered, worst 740 microseconds; behind a full resend queue, 20 offered,
// ZERO written, twenty refused, none delivered. Nothing counted that — the
// refusal returns false to the caller and an IP caller drops the packet — which
// is the same shape this program has now found three times.
//
// The drop was earlier than the design expected. The send loop's bypass of the
// resend capacity gate exists and is correct, but it was never reached: slots
// were held by reliable packs waiting for resend capacity, which cannot
// progress, while no-acknowledgement packs that could progress were refused
// behind them. The boundary is the write, not the queue, so exempting a
// no-acknowledgement pack from admission would have been wrong — it needs
// exactly what admission protects, bounded memory for unwritten frames and
// bounded latency ahead of the write — and the fix keeps one bound with one
// meaning: a reliable pack takes its slot only when it could also enter the
// resend queue, and the loop's bypass now applies without flow isolation too.
//
// After: behind a full resend queue, 20 offered, 20 written, 0 refused, all
// delivered.
//
// One thing this cell had to get right to say anything. Withholding every
// acknowledgement stops the write entirely, and refusing then is correct rather
// than a defect: a pack cannot be written by a writer that has stopped. What it
// drives instead is back pressure — reliable traffic offered faster than the
// carrier drains, acknowledgements flowing — so the resend queue sits at its
// bound and turns over.
func TestANoAckPackIsNotHeldOrDroppedByAFullResendQueue(t *testing.T) {
	assertMessagePoolOwnership(t)

	const propagation = 10 * time.Millisecond
	const window = ByteCount(256 * 1024)
	// slow enough that the reliable traffic below keeps the window at its
	// bound for the whole of the cell
	const bytesPerSecond = ByteCount(4 * 1000 * 1000 / 8)
	const payloadByteCount = 1024
	const reliableCount = 4000
	const noAckCount = 20

	run := func(fill bool) (delivered int, latencies []time.Duration, offered, written, refused uint64) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		// A shallow carrier, so what this measures is the transfer layer rather
		// than the fixture. The deep route channel holds a thousand frames,
		// which at this drain is two seconds of queue: a no-acknowledgement
		// pack written promptly still lands behind every reliable frame
		// already on that wire, and the 766 ms that reading produced was the
		// fixture's buffer and not the sequence's.
		harness := newPacedSendWindowHarness(t, ctx, propagation, bytesPerSecond,
			shallowCarrierFrameCapacity,
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
			// Back pressure rather than a stall: reliable traffic offered
			// faster than the carrier drains, with acknowledgements flowing
			// normally, so the resend queue sits at its bound and turns over.
			// Withholding every acknowledgement instead would stop the write
			// entirely, and refusing then is the correct semantics rather than
			// the defect — a pack cannot be written by a writer that has
			// stopped.
			payload := string(make([]byte, payloadByteCount))
			var filling sync.WaitGroup
			filling.Add(1)
			go func() {
				defer filling.Done()
				for range reliableCount {
					select {
					case <-ctx.Done():
						return
					default:
					}
					frame := RequireToFrameWithDefaultProtocolVersion(
						&protocol.SimpleMessage{Content: payload},
					)
					admitted, _ := harness.sender.SendWithTimeoutDetailed(
						frame,
						harness.receiverId,
						nil,
						50*time.Millisecond,
						sendPackRecoveryOption{upstreamRecoverable: true},
						testFlowOption(41001),
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
				testFlowOption(41002),
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
		if 0 < stats.SendNoAckDiscardCount {
			t.Logf(
				"%d no-acknowledgement packs were discarded after admission, on a failed write or a contract that could not be created",
				stats.SendNoAckDiscardCount,
			)
		}
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
	// The blocking half, against what the boundary actually promises.
	//
	// A no-acknowledgement pack does wait for the packs admitted ahead of it,
	// and that is admission working rather than the defect: admission bounds
	// the population held but not yet written, and every pack there occupies a
	// buffer and a place ahead of the write whatever its semantics. What it
	// must not wait for is the resend queue, which it will never occupy. So
	// the bar is a resend interval: below it, the wait is the carrier's
	// service time for a bounded population; at or above it, the pack is
	// waiting on reliability it does not use.
	//
	// The arithmetic, and it matches: 32 admission slots plus a 32-item pack
	// channel plus the shallow carrier's 8 frames is about 72 KiB, which at
	// this drain is 144 ms. Measured 167 and 182 ms against 1.8 and 2.9 ms
	// with an empty queue.
	resendInterval := DefaultSendBufferSettings().RttMinResendInterval
	if resendInterval <= worst(fullLatencies) {
		t.Errorf(
			"a no-acknowledgement pack took %s behind a full resend queue, at or beyond the %s resend interval, against %s with an empty one; it is waiting on a buffer it will never occupy rather than on the bounded population ahead of the write",
			worst(fullLatencies), resendInterval, worst(idleLatencies),
		)
	}
}

// The invariant the three-stage design rests on: the resend queue is retention,
// and a no-acknowledgement item has nothing to retain.
//
// Items in that queue are charged to the window, resent on a timer, probed, and
// lease-tracked by the selective-acknowledgement machinery. A no-acknowledgement
// pack is written once and never acknowledged, so putting one there would give
// it a lease nothing will ever clear and charge a window it does not use. The
// pre-write queue — admission, the channel, the scheduler — is where a
// no-acknowledgement pack waits, and it keeps its own semantics there: taken by
// the bypass regardless of capacity, written once, never retained.
//
// Prediction, recorded before the run: through a burst of no-acknowledgement
// traffic on a sequence that is also carrying reliable traffic, every item the
// resend queue holds at any moment is one that expects an acknowledgement.
func TestTheResendQueueNeverHoldsAnUnacknowledgedItem(t *testing.T) {
	assertMessagePoolOwnership(t)

	const propagation = 10 * time.Millisecond
	const bytesPerSecond = ByteCount(4 * 1000 * 1000 / 8)
	const payloadByteCount = 1024

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	harness := newPacedSendWindowHarness(t, ctx, propagation, bytesPerSecond,
		shallowCarrierFrameCapacity,
		func(settings *SendBufferSettings) {
			settings.ResendQueueMaxByteCount = ByteCount(256 * 1024)
		})

	var running sync.WaitGroup
	running.Add(2)
	go func() {
		defer running.Done()
		payload := string(make([]byte, payloadByteCount))
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			frame := RequireToFrameWithDefaultProtocolVersion(
				&protocol.SimpleMessage{Content: payload},
			)
			if admitted, _ := harness.sender.SendWithTimeoutDetailed(
				frame, harness.receiverId, nil, 50*time.Millisecond,
				sendPackRecoveryOption{upstreamRecoverable: true},
			); !admitted {
				MessagePoolReturn(frame.MessageBytes)
			}
		}
	}()
	go func() {
		defer running.Done()
		payload := string(make([]byte, payloadByteCount))
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			frame := RequireToFrameWithDefaultProtocolVersion(
				&protocol.SimpleMessage{Content: payload},
			)
			if admitted, _ := harness.sender.SendWithTimeoutDetailed(
				frame, harness.receiverId, nil, 50*time.Millisecond, NoAck(),
			); !admitted {
				MessagePoolReturn(frame.MessageBytes)
			}
		}
	}()
	running.Wait()

	// Counted where the item is added, on the sequence's own goroutine. The
	// items are pooled and reset, so reading their fields from a watcher would
	// be a data race rather than an observation — which the race detector said
	// when this row first tried it.
	stats := harness.sender.ReceiveStats()
	t.Logf(
		"%d no-acknowledgement packs written, %d items put into retention that expect no acknowledgement",
		stats.SendNoAckWriteCount, stats.ResendQueueUnackedItemCount,
	)
	if stats.SendNoAckWriteCount == 0 {
		t.Fatal("no no-acknowledgement pack was written, so this cell reads nothing")
	}
	if 0 < stats.ResendQueueUnackedItemCount {
		t.Errorf(
			"%d items entered the resend queue expecting no acknowledgement; that queue is retention, and an item there is charged to the window, resent on a timer, probed and lease-tracked, none of which a no-acknowledgement pack can ever clear",
			stats.ResendQueueUnackedItemCount,
		)
	}
}
