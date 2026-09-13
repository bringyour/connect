//go:build flightgate_next

// FLIGHTGATEFIX §20.3. What a misclassification of the direct lane costs.
// The latch is gone from the landing; this is the specification any
// future lane signal is measured against.

package connect

import (
	"math/rand"
	"testing"
	"time"
)

// FLIGHTGATEFIX §19.5. What a misclassification of the lane costs, rather
// than how often the classification changes. Once a hole is deferred to the
// sequence window's clock rather than dropped, the whole cost of treating a
// losing lane as reordering is one deferral of that clock, and the clock is
// bounded by the item's own resend ceiling. A counter that decays cannot
// hold a steady classification at half a per cent, and with the cost this
// small it does not need to.
func TestLaneClassificationTransitionCostsOneDeferral(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(time.Millisecond)
	newSequence := func(latched bool) (*SendSequence, []*sendItem) {
		sequence, items := newSelectiveAckRecoveryTestSequence(8, sendTime)
		sequence.client = &Client{}
		sequence.flightController = newSendFlightController(sequence.sendBufferSettings)
		sequence.flightController.applyPolicy(transferFlightPolicySnapshot{
			generation:             1,
			limited:                true,
			reliableRouteAvailable: true,
		})
		sequence.rttWindow.CloseSendTime(uint64(time.Now().Add(-300 * time.Millisecond).UnixMilli()))
		if latched {
			sequence.noteUnreliableLaneLoss()
		}
		// a hole the direct lane carried, proven only by acknowledgements
		// that could have overtaken its own reply
		items[0].unreliableCarrierObserved = true
		items[0].unreliableFlightTracked = true
		for index := 1; index < len(items); index += 1 {
			items[index].selectiveAcked = true
		}
		return sequence, items
	}

	losing, losingItems := newSequence(true)
	if !losing.unreliableLaneLosing() {
		t.Fatal("one proven loss did not classify the lane as losing")
	}
	losing.scheduleSelectiveAckRecovery(currentTime)
	if losingItems[0].resendTime.After(currentTime) {
		t.Fatalf("a hole on a lane classified losing still waits %s",
			losingItems[0].resendTime.Sub(currentTime))
	}

	reordering, reorderingItems := newSequence(false)
	if reordering.unreliableLaneLosing() {
		t.Fatal("a lane with no proven loss is classified losing")
	}
	sequenceClock := reordering.rttWindow.ScaledRtt()
	reordering.scheduleSelectiveAckRecovery(currentTime)
	if !reorderingItems[0].gapRecoveryDeferred {
		t.Fatal("a hole on a lane classified clean was not deferred")
	}

	// the whole cost of the wrong classification, and its bound
	cost := reorderingItems[0].resendTime.Sub(losingItems[0].resendTime)
	if want := sendTime.Add(sequenceClock).Sub(currentTime); cost != want {
		t.Fatalf(
			"treating a losing lane as reordering costs %s, want one deferral of the sequence clock %s",
			cost, want,
		)
	}
	if ceiling := reordering.sendBufferSettings.UnreliableMaxResendInterval; ceiling < sequenceClock {
		t.Fatalf(
			"one deferral is %s, past the item's own resend ceiling %s: the cost of a "+
				"misclassification must never exceed what the timeout would have cost anyway",
			sequenceClock, ceiling,
		)
	}

	// and a lane that loses nothing is never classified losing, at any rate
	for _, regime := range []struct {
		name string
		loss *laneLossProcess
	}{
		{"clean or reordering, nothing lost", newLaneLossProcess(1, 0, nil)},
	} {
		sequence, _ := newSequence(false)
		for range 50_000 {
			if regime.loss.lost() {
				sequence.noteUnreliableLaneLoss()
			} else {
				sequence.noteUnreliableLaneProgress()
			}
			if sequence.unreliableLaneLosing() {
				t.Fatalf("%s: a lane that lost nothing was classified losing", regime.name)
			}
		}
	}
}
