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

// FLIGHTGATEFIX §19.7. D2 is withdrawn: a forced direct route keeps
// merged's cold floor for the timer and for probe pacing, and the sequence
// window stays unsampled. RttWindow has no reset, so feeding it there
// would leave a flapping route's direct-lane samples mis-clocking the
// relay for up to 128 acks or 60 s after the relay returns.
func TestForcedDirectRouteKeepsMergedsColdFloor(t *testing.T) {
	for _, arm := range []struct {
		name            string
		reliableSibling bool
	}{
		{"forced direct route", false},
		{"mixed route", true},
	} {
		sequence := laneClockSequence(t, arm.reliableSibling)
		settings := sequence.sendBufferSettings
		beforeScaled := sequence.rttWindow.ScaledRtt()
		beforeProbe := sequence.rttWindow.ProbeRtt()
		item := &sendItem{transferFrameBytes: make([]byte, 64)}
		sequence.observeCarrierWrite(item, transferWriteDisposition{unreliable: true})
		for range settings.RttWindowSize {
			sequence.observeAckRtt(item, laneClockAckTag(350*time.Millisecond))
		}
		if after := sequence.rttWindow.ScaledRtt(); after != beforeScaled {
			t.Errorf(
				"%s: acks the direct lane carried moved the sequence clock %s -> %s; the window "+
					"has no reset, so a flapping route would mis-clock the relay afterwards",
				arm.name, beforeScaled, after,
			)
		}
		if after := sequence.rttWindow.ProbeRtt(); after != beforeProbe {
			t.Errorf("%s: acks the direct lane carried moved the probe clock %s -> %s",
				arm.name, beforeProbe, after)
		}
		if _, sampled := sequence.rttWindow.ScaledRttSampled(); sampled {
			t.Errorf("%s: the sequence window is sampled by direct-lane acknowledgements", arm.name)
		}
		if first := sequence.resendIntervalForItem(item, 1); first != settings.MinResendInterval {
			t.Errorf(
				"%s: the first retransmit of a direct-carried item is due in %s, want merged's "+
					"cold floor %s",
				arm.name, first, settings.MinResendInterval,
			)
		}
	}
}
