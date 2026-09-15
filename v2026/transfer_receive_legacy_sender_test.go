package connect

import (
	"context"
	"testing"
)

// The reverse direction, which everything else in this program leaves out.
//
// Every window row built so far tests a modern SENDER against a legacy
// receiver. The opposite case is live as of the delivery-sized window rule
// shipping on by default: `DefaultReceiveBufferSettings().AdvertiseReceiveWindow`
// is now true, so our receiver states a capacity on every acknowledgement - and
// a legacy sender cannot read `receive_window_byte_count`, will not clamp to
// it, and may put more in flight than the hold can take. That provokes the very
// eviction the advertisement exists to prevent, from the one peer that cannot
// participate in the fix.
//
// The honest answer, and what this row pins. The receiver is not protected by
// the advertisement. It is protected by the committed-prefix boundary, and the
// reason that protection holds against a sender which ignores everything is
// that the boundary is computed entirely from quantities the receiver owns: its
// own capacity, its own delivery point, its own held set, and the largest frame
// it has seen. It asks nothing of the sender and therefore cannot be defeated
// by one. The consumer's own comment states the consequence exactly - "against
// a sender honouring the advertisement every held item commits at once and this
// is today's behaviour exactly; against one that does not, the cost is that
// sender's resends of tentative items rather than a lease" - and nothing
// asserted it.
//
// The KNOWN COST, pinned rather than papered over. Against a legacy sender the
// hold does evict. What it evicts is always an item it never acknowledged, so
// nothing is withdrawn; the sender recovers it through its own gap recovery and
// timers, because `evicted_sequence_numbers` cannot reach a peer that does not
// read it. That is a retransmission the modern path would not have paid, and it
// is bounded by the overrun rather than by the sixty-second selective
// acknowledgement timeout, which is the outcome the pre-§37.20 eviction policy
// produced. Trading a bounded resend for an unbounded lease is the whole of the
// protection, and the assertion below is that the trade still holds: no item
// above the boundary is ever acknowledged.
//
// Strictly deterministic: the hold is populated directly and
// `commitHeldPrefix` is called synchronously. No sender, no carrier, no timers,
// no goroutines, and no dependence on scheduling.
func TestTheHoldIsSafeAgainstASenderThatIgnoresTheAdvertisement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	applyTestEncryptionSettings(settings, encryptionModeOff)
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	// The receiver states a capacity, which is the premise of the case: if it
	// did not advertise, a legacy sender would be no different from any other.
	if !DefaultReceiveBufferSettings().AdvertiseReceiveWindow {
		t.Fatal("the shipping receiver does not advertise a capacity, so there is no advertisement for a legacy sender to ignore and this row has no case to test")
	}
	if DefaultReceiveBufferSettings().ReceiveHoldPolicy != ReceiveHoldCommittedPrefix {
		t.Fatal("the shipping hold policy is not committed-prefix, so the protection this row asserts is not the one that ships")
	}

	const frameByteCount = ByteCount(4 * 1024)
	// Ten frames of capacity. A legacy sender that ignored the advertisement
	// has pushed a hold of ten frames that begins four frames beyond the
	// delivery point, so four gaps sit below the oldest held item and every
	// held item carries them.
	const capacity = 10 * frameByteCount
	const deliveryPoint = uint64(100)
	const gapCount = 4
	const heldCount = 10

	newSequence := func(capacity ByteCount, gapCount int) (*ReceiveSequence, []*receiveItem) {
		receiveSettings := DefaultReceiveBufferSettings()
		receiveSettings.ReceiveQueueMaxByteCount = capacity
		sequence := &ReceiveSequence{
			client:                client,
			log:                   NewNoopLogger(),
			receiveBufferSettings: receiveSettings,
			receiveQueue: newReceiveQueue(
				receiveSettings.ReceiveQueueBudget,
				receiveSettings.ReceiveQueueMinByteCount,
			),
			ackWindow:          newSequenceAckWindow(),
			nextSequenceNumber: deliveryPoint,
			// the conservative gap estimate the hold keeps: the largest frame
			// it has taken
			maxHeldByteCount: frameByteCount,
		}
		held := make([]*receiveItem, 0, heldCount)
		for i := range heldCount {
			item := &receiveItem{
				transferItem: transferItem{
					messageId:        NewId(),
					messageByteCount: frameByteCount,
					sequenceNumber:   deliveryPoint + uint64(gapCount) + uint64(i),
				},
			}
			sequence.receiveQueue.Add(item)
			held = append(held, item)
		}
		return sequence, held
	}

	sequence, held := newSequence(capacity, gapCount)
	sequence.commitHeldPrefix()

	// The boundary, from the receiver's own arithmetic: the i-th held item is
	// safe when the gaps that could still arrive below it, at the largest frame
	// seen, plus the held bytes through it, fit the capacity.
	committed := 0
	for _, item := range held {
		if item.committed {
			committed += 1
		}
	}
	t.Logf(
		"capacity %d, %d frames held beyond %d gaps: %d committed, %d held tentatively",
		capacity, heldCount, gapCount, committed, heldCount-committed,
	)

	if committed == 0 {
		t.Fatal("nothing committed at all, so the hold acknowledges nothing and this row cannot distinguish the boundary from a refusal")
	}
	if committed == heldCount {
		// Two readings, and the row cannot tell them apart from outside, so it
		// names both. Either the boundary stopped binding - in which case an
		// acknowledged item is now evictable and the protection is gone - or
		// the fixture no longer constructs an overrun, in which case the
		// tentative region is untested and the row has quietly stopped
		// measuring anything. The check below on the newest held item
		// discriminates: if it is committed, it is the first.
		t.Errorf(
			"every one of the %d held items committed at a capacity of %d with %d gaps below them. Either the boundary no longer binds, which makes an acknowledged item evictable, or this fixture no longer builds an overrun and the tentative region is untested",
			heldCount, capacity, gapCount,
		)
	}

	// The contract: committed is a PREFIX. An item is safe only if every
	// earlier held item is, because admitting an earlier arrival evicts from
	// the newest end. A committed item above an uncommitted one would be an
	// acknowledgement the hold cannot keep.
	seenUncommitted := false
	for i, item := range held {
		if !item.committed {
			seenUncommitted = true
			continue
		}
		if seenUncommitted {
			t.Errorf(
				"held item %d (sequence %d) is committed while an earlier held item is not. The commitment is a prefix: eviction takes from the newest end, so a later item cannot be safe while an earlier one is at risk",
				i, item.sequenceNumber,
			)
		}
	}

	// The safety property, stated as the eviction path reads it: eviction walks
	// from the newest end and stops at the first committed item, so an item
	// above the boundary is exactly what may be discarded and an item below it
	// is exactly what may not. Nothing acknowledged is ever withdrawn, whatever
	// the sender did to fill the hold.
	last := sequence.receiveQueue.PeekLast()
	if last == nil {
		t.Fatal("the hold is empty after the commit pass")
	}
	if last.committed {
		t.Error("the newest held item is committed, so the first thing the eviction path would reach is an acknowledgement it must not withdraw; the gap estimate has under-counted and the boundary is not doing its job")
	}

	// And the boundary moves with the receiver's own quantities and nothing
	// else. More capacity commits more; the same held set with fewer gaps
	// below it - a delivery point that has caught up - commits more. Both are
	// the receiver's own facts, which is why a sender that ignores the
	// advertisement cannot move the boundary at all.
	wider, widerHeld := newSequence(capacity*4, gapCount)
	wider.commitHeldPrefix()
	widerCommitted := 0
	for _, item := range widerHeld {
		if item.committed {
			widerCommitted += 1
		}
	}
	if widerCommitted <= committed {
		t.Errorf(
			"four times the capacity committed %d of %d against %d at the original capacity; the boundary is the receiver's own capacity against its own gaps, so more capacity must commit at least as much and here strictly more",
			widerCommitted, heldCount, committed,
		)
	}

	// the same hold with no gaps below it: the ordinary case, and the one the
	// consumer's comment calls "today's behaviour exactly"
	closed, closedHeld := newSequence(capacity, 0)
	closed.commitHeldPrefix()
	closedCommitted := 0
	for _, item := range closedHeld {
		if item.committed {
			closedCommitted += 1
		}
	}
	t.Logf("with the gaps below the hold closed: %d of %d committed", closedCommitted, heldCount)
	if closedCommitted != heldCount {
		t.Errorf(
			"with no gaps below the hold, %d of %d committed. A hold whose bytes fit its capacity with nothing missing below it has nothing left to fear from an earlier arrival, so it commits whole - this is the behaviour a sender honouring the advertisement gets, and it must be unchanged",
			closedCommitted, heldCount,
		)
	}
}

// The known cost of the reverse direction, stated as a relationship between two
// capability bits rather than measured.
//
// `evicted_sequence_numbers` (field 9) is how a receiver confesses that an item
// it acknowledged did not survive. A legacy sender does not read it, so on that
// path the confession cannot be delivered and the original defect - the sender
// holding a lease for its full selective acknowledgement timeout on bytes the
// receiver has already discarded - would persist untouched. What removes it is
// not field 9 at all: it is that under committed-prefix the receiver never
// discards an item it acknowledged, so there is nothing to confess. Field 9
// therefore becomes compatibility for a receiver this tree does not ship, and
// the protection against a legacy sender is entirely local to the receiver.
//
// This row pins the two settings that make that argument true, because the
// argument is only as good as the defaults behind it, and both are the zero
// value of their kind - which is to say both ship by reason of iota order and
// an unset field, and either could be moved by a reorder with nothing to read
// afterwards.
func TestTheLegacySenderCostRestsOnTheHoldPolicyAndNotOnTheNotice(t *testing.T) {
	settings := DefaultReceiveBufferSettings()

	if settings.ReceiveHoldPolicy != ReceiveHoldCommittedPrefix {
		t.Errorf(
			"the shipping hold policy is %v rather than committed-prefix. Against a legacy sender that ignores the advertised capacity the hold WILL be overrun, and only committed-prefix guarantees that what it discards was never acknowledged; under the evicting policy the sender holds a lease until its selective acknowledgement timeout, and it cannot be told otherwise because it does not read evicted_sequence_numbers",
			settings.ReceiveHoldPolicy,
		)
	}
	if ReceiveHoldCommittedPrefix != 0 {
		t.Errorf(
			"ReceiveHoldCommittedPrefix is %d rather than the zero value of its kind; this row and the settings default both rest on it being what an unset field resolves to",
			ReceiveHoldCommittedPrefix,
		)
	}
	// The advertisement is the other half: it is what keeps a MODERN sender
	// from overrunning at all. Turning it off would not be unsafe, but it would
	// put every peer on the legacy path and make the bounded resend above the
	// ordinary case rather than the compatibility case.
	if !settings.AdvertiseReceiveWindow {
		t.Error("the shipping receiver does not advertise its capacity, so every sender is on the legacy path and the tentative-resend cost is what every transfer pays rather than what an older peer costs")
	}
	t.Logf(
		"hold policy %v, advertise %t, capacity %d, eviction notice %t",
		settings.ReceiveHoldPolicy,
		settings.AdvertiseReceiveWindow,
		settings.ReceiveQueueMaxByteCount,
		settings.EvictionNotice,
	)
}
