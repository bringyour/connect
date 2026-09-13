package connect

// FLIGHTGATEFIX §23.2. F11b's grace and the retransmit timer disagree about
// one item. The grace ends at the item's send time plus the scaled round
// trip, which is exactly when its own timer first comes due, so once the
// deferred retransmit extends that timer the grace has already expired and
// the scoreboard writes at the next round of later acknowledgements what
// the timer just declined to write.

import (
	"testing"
	"time"
)

// deferredGraceSequence is a scoreboard on a mixed route whose relay has
// answered, with one relay-carried hole and three later acknowledgements.
func deferredGraceSequence(t testing.TB, narrowed bool) (*SendSequence, []*sendItem, time.Time) {
	t.Helper()
	sendTime := time.Unix(1_700_000_000, 0)
	sequence, items := newSelectiveAckRecoveryTestSequence(8, sendTime)
	sequence.client = &Client{}
	sequence.sendBufferSettings.DeferredItemIsLateForTheScoreboard = narrowed
	sequence.flightController = newSendFlightController(sequence.sendBufferSettings)
	sequence.flightController.applyPolicy(transferFlightPolicySnapshot{
		generation:             1,
		limited:                true,
		reliableRouteAvailable: true,
	})
	sequence.rttWindow.CloseSendTime(uint64(time.Now().Add(-300 * time.Millisecond).UnixMilli()))
	hole := items[0]
	hole.reliableCarrierObserved = true
	hole.unreliableFlightTracked = false
	for index := 1; index < len(items); index += 1 {
		items[index].selectiveAcked = true
	}
	return sequence, items, sendTime
}

// The two paths disagree today, and the narrowing makes them agree.
func TestDeferredItemIsLateForTheScoreboard(t *testing.T) {
	for _, arm := range []struct {
		name     string
		narrowed bool
		recovers bool
	}{
		{"as landed", false, true},
		{"with the grace and the timer agreeing", true, false},
	} {
		sequence, items, sendTime := deferredGraceSequence(t, arm.narrowed)
		hole := items[0]
		scaledRtt := sequence.rttWindow.ScaledRtt()

		// inside the grace nothing is written on either arm: the hole is
		// younger than the relay's own round trip
		early := sendTime.Add(scaledRtt / 2)
		sequence.scheduleSelectiveAckRecovery(early)
		if hole.selectiveGapRecovered {
			t.Fatalf("%s: a hole younger than the relay's round trip was recovered", arm.name)
		}

		// its timer comes due exactly as the grace ends, and the deferred
		// retransmit extends the timer by one more round trip
		due := sendTime.Add(scaledRtt)
		hole.timeoutDeferCount = 1
		hole.deferralOutstanding = true
		hole.resendTime = due.Add(scaledRtt)

		// one round trip later the deferral is still outstanding
		sequence.scheduleSelectiveAckRecovery(due.Add(scaledRtt / 2))
		if hole.selectiveGapRecovered != arm.recovers {
			t.Fatalf(
				"%s: the scoreboard recovered a hole whose own retransmit is still waiting out a "+
					"deferral: recovered=%v, want %v",
				arm.name, hole.selectiveGapRecovered, arm.recovers,
			)
		}
	}
}

// Once the deferral expires the scoreboard is free again, so the narrowing
// postpones a recovery rather than suppressing it.
func TestDeferredGraceEndsWithTheDeferral(t *testing.T) {
	sequence, items, sendTime := deferredGraceSequence(t, true)
	hole := items[0]
	scaledRtt := sequence.rttWindow.ScaledRtt()
	due := sendTime.Add(scaledRtt)
	hole.timeoutDeferCount = 1
	hole.deferralOutstanding = true
	hole.resendTime = due.Add(scaledRtt)

	sequence.scheduleSelectiveAckRecovery(due.Add(scaledRtt / 2))
	if hole.selectiveGapRecovered {
		t.Fatal("a hole holding a deferral was recovered")
	}
	// the deferral expires and its retransmit is written, which clears the
	// marker; the scoreboard may act again
	hole.deferralOutstanding = false
	sequence.scheduleSelectiveAckRecovery(due.Add(2 * scaledRtt))
	if !hole.selectiveGapRecovered {
		t.Fatal("the scoreboard did not recover the hole after its deferral expired")
	}
}

// The setting is off, so the landed tree is unchanged.
func TestDeferredGraceNarrowingIsOffByDefault(t *testing.T) {
	if DefaultSendBufferSettings().DeferredItemIsLateForTheScoreboard {
		t.Fatal("the grace narrowing is on by default, but it has not been measured in a campaign")
	}
}
