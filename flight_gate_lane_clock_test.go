package connect

// FLIGHTGATEFIX §19 D1 and D2. Every clock is keyed to the path an
// acknowledgement can take, not to the carrier that carried the item. With
// a reliable sibling the sequence window describes the relay, the lane the
// acks travel; with no sibling there is no relay to describe and the
// direct lane's own acks are the only samples there are.

import (
	"testing"
	"time"
)

// laneClockSequence is a send sequence on a route whose policy is limited,
// with or without a reliable sibling.
func laneClockSequence(t testing.TB, reliableSibling bool) *SendSequence {
	t.Helper()
	// built through the scoreboard's own helper, which is the sequence shape
	// that carried the direct lane's separate window before D3 deleted it
	sequence, _ := newSelectiveAckRecoveryTestSequence(1, time.Unix(1_700_000_000, 0))
	settings := sequence.sendBufferSettings
	sequence.client = &Client{}
	sequence.flightController = newSendFlightController(settings)
	sequence.flightController.applyPolicy(transferFlightPolicySnapshot{
		generation:             1,
		limited:                true,
		reliableRouteAvailable: reliableSibling,
	})
	return sequence
}

func laneClockAckTag(age time.Duration) sequenceTag {
	return sequenceTag{sendTime: uint64(time.Now().Add(-age).UnixMilli()), set: true}
}

// D1. The retransmit timer of an unreliable-carried item is the sequence
// window's clock, merged's form. A direct item whose reply takes the relay
// is judged by the relay's clock, and so is the same item once §13.1's
// forget has resent it reliable-only.
func TestUnreliableItemTimerIsTheSequenceClock(t *testing.T) {
	sequence := laneClockSequence(t, true)
	// the relay answers at 300 ms and the direct lane at 250 ms, so the two
	// clocks are far enough apart to tell which one timed the item
	relay := &sendItem{transferFrameBytes: make([]byte, 64)}
	sequence.observeCarrierWrite(relay, transferWriteDisposition{reliable: true})
	direct := &sendItem{transferFrameBytes: make([]byte, 64)}
	sequence.observeCarrierWrite(direct, transferWriteDisposition{unreliable: true})
	if !direct.unreliableCarrierObserved {
		t.Fatal("the item was not marked as carried by the direct lane")
	}
	for range 16 {
		sequence.observeAckRtt(relay, laneClockAckTag(300*time.Millisecond))
		sequence.observeAckRtt(direct, laneClockAckTag(250*time.Millisecond))
	}
	sequenceClock := sequence.rttWindow.ScaledRtt()
	if want := 600 * time.Millisecond; sequenceClock != want {
		t.Fatalf("the sequence clock is %s, want %s, twice the relay's 300 ms", sequenceClock, want)
	}
	interval := sequence.resendIntervalForItem(direct, 1)
	if interval != sequenceClock {
		t.Fatalf(
			"a direct-carried item's first retransmit is due in %s, want the sequence clock %s: "+
				"the acknowledgement's path is the receiver's choice, so no carrier's own clock times it "+
				"(500 ms would be the direct lane's own 250 ms doubled)",
			interval, sequenceClock,
		)
	}
}

// D2. On a forced direct route the sequence window is fed by the direct
// lane's own acknowledgements, stock's rule; with a reliable sibling it is
// not, which is F10 and which TestSendSequenceAckRttIgnoresUnreliableCarrier
// guards from the other side.
func TestForcedDirectRouteFeedsTheSequenceWindow(t *testing.T) {
	for _, arm := range []struct {
		name            string
		reliableSibling bool
		moves           bool
	}{
		{"forced direct route", false, true},
		{"mixed route", true, false},
	} {
		sequence := laneClockSequence(t, arm.reliableSibling)
		beforeScaled := sequence.rttWindow.ScaledRtt()
		beforeProbe := sequence.rttWindow.ProbeRtt()
		item := &sendItem{transferFrameBytes: make([]byte, 64)}
		sequence.observeCarrierWrite(item, transferWriteDisposition{unreliable: true})
		for range 8 {
			sequence.observeAckRtt(item, laneClockAckTag(350*time.Millisecond))
		}
		afterScaled := sequence.rttWindow.ScaledRtt()
		afterProbe := sequence.rttWindow.ProbeRtt()
		movedScaled := afterScaled != beforeScaled
		movedProbe := afterProbe != beforeProbe
		if movedScaled != arm.moves || movedProbe != arm.moves {
			t.Errorf(
				"%s: an ack the direct lane carried moved the sequence clock=%v and the probe=%v, want %v: "+
					"with no relay to describe the direct lane's acks are the only samples there are",
				arm.name, movedScaled, movedProbe, arm.moves,
			)
		}
	}
}

// D2, the trade. On a forced direct route the timer is stock's: the cold
// floor until the lane answers, then twice the window mean floored at the
// retransmit pacing floor and capped at the unreliable ceiling. It is
// never a sixteen-sample mean, which is what tracked a cell-edge uplink's
// own serialisation queue faster than the queue moved.
func TestForcedDirectRouteFirstRetransmitMatchesStock(t *testing.T) {
	sequence := laneClockSequence(t, false)
	settings := sequence.sendBufferSettings
	item := &sendItem{transferFrameBytes: make([]byte, 64)}
	sequence.observeCarrierWrite(item, transferWriteDisposition{unreliable: true})

	if cold := sequence.resendIntervalForItem(item, 1); cold != settings.MinResendInterval {
		t.Fatalf("before any ack the first retransmit is due in %s, want the cold floor %s",
			cold, settings.MinResendInterval)
	}
	for range settings.RttWindowSize {
		sequence.observeAckRtt(item, laneClockAckTag(350*time.Millisecond))
	}
	first := sequence.resendIntervalForItem(item, 1)
	if want := 700 * time.Millisecond; first != want {
		t.Fatalf(
			"after %d acks at 350 ms the first retransmit is due in %s, want %s, twice the window mean: "+
				"the clock must be the 128-sample window, never a sixteen-sample mean",
			settings.RttWindowSize, first, want,
		)
	}
	if first <= settings.RttMinResendInterval {
		t.Fatalf("the timer %s is at or under the pacing floor %s, so the lane is not being measured",
			first, settings.RttMinResendInterval)
	}
	// a short burst of fast acknowledgements must not move the clock far: a
	// 128-sample window absorbs sixteen of them, where a sixteen-sample mean
	// would be entirely replaced and collapse to the pacing floor. That
	// collapse is what chased a cell-edge uplink's own serialisation queue
	// faster than the queue moved.
	for range 16 {
		sequence.observeAckRtt(item, laneClockAckTag(50*time.Millisecond))
	}
	afterBurst := sequence.resendIntervalForItem(item, 1)
	if afterBurst < 550*time.Millisecond || 700*time.Millisecond < afterBurst {
		t.Fatalf(
			"after sixteen acks at 50 ms the timer is %s, want it still near %s: a sixteen-sample "+
				"mean would have collapsed to the %s pacing floor",
			afterBurst, first, settings.RttMinResendInterval,
		)
	}
	if capped := sequence.resendIntervalForItem(item, 8); capped != settings.UnreliableMaxResendInterval {
		t.Fatalf("the backed-off timer is %s, want the unreliable ceiling %s",
			capped, settings.UnreliableMaxResendInterval)
	}
}
