package connect

// FLIGHTGATEFIX §24.3. A deferral backs off like the rewrite it replaces.
// Without that, a deferral re-arms at the same scaled round trip and
// leaves sendCount alone, so a window stalled mid-transfer re-fires whole
// every round trip and the deferral limit writes the third firing of every
// item into the stall. Merged's rewrite doubles its interval each attempt.

import (
	"testing"
	"time"
)

func backoffSequence(t testing.TB, backoff bool) *SendSequence {
	t.Helper()
	sequence, _ := newSelectiveAckRecoveryTestSequence(1, time.Unix(1_700_000_000, 0))
	sequence.client = &Client{}
	sequence.sendBufferSettings.DeferTimeoutResendBackoff = backoff
	sequence.flightController = newSendFlightController(sequence.sendBufferSettings)
	// a relay measured at 200 ms, so the scaled round trip is 400 ms
	sequence.rttWindow.CloseSendTime(uint64(time.Now().Add(-200 * time.Millisecond).UnixMilli()))
	return sequence
}

// deferralInterval reads the interval the resend loop itself computes for
// an item sent once and already deferred deferCount times.
func deferralInterval(sequence *SendSequence, deferCount int, _ bool) time.Duration {
	item := &sendItem{sendCount: 1, timeoutDeferCount: deferCount}
	return sequence.deferredResendInterval(item, sequence.rttWindow.ScaledRtt())
}

// The second deferral of an item is twice the first, and every interval is
// capped as every other resend interval is.
func TestDeferralBacksOffLikeARewrite(t *testing.T) {
	sequence := backoffSequence(t, true)
	scaledRtt := sequence.rttWindow.ScaledRtt()
	if scaledRtt != 400*time.Millisecond {
		t.Fatalf("the relay's scaled round trip reads %s, want 400ms", scaledRtt)
	}
	first := deferralInterval(sequence, 0, true)
	if first != scaledRtt {
		t.Fatalf("the first deferral waits %s, want one scaled round trip %s: the first deferral "+
			"is the one that pays, and it must not change", first, scaledRtt)
	}
	second := deferralInterval(sequence, 1, true)
	if second != 2*first {
		t.Fatalf("the second deferral waits %s, want twice the first %s", second, first)
	}
	// and it is capped like every other interval
	far := deferralInterval(sequence, 12, true)
	if far != sequence.sendBufferSettings.MaxResendInterval {
		t.Fatalf("a far deferral waits %s, want the cap %s", far,
			sequence.sendBufferSettings.MaxResendInterval)
	}
	// without the backoff every deferral is the same interval, which is
	// what re-fires a stalled window whole
	flatSequence := backoffSequence(t, false)
	if flat := deferralInterval(flatSequence, 5, false); flat != scaledRtt {
		t.Fatalf("without the backoff a deferral waits %s, want %s", flat, scaledRtt)
	}
}

// A window stalled for 1.5 s at a 400 ms scaled round trip fires at most
// twice per item with the backoff, and never reaches the deferral limit,
// so nothing is written into the stall. Without it the same stall fires
// three times and the limit writes the third.
func TestStalledWindowReFiresLogarithmically(t *testing.T) {
	const (
		stall     = 1500 * time.Millisecond
		itemCount = 700
	)
	for _, arm := range []struct {
		name        string
		backoff     bool
		wantFirings int
		wantWritten bool
	}{
		{"as merged's rewrite does", true, 2, false},
		{"re-arming flat", false, 3, true},
	} {
		sequence := backoffSequence(t, arm.backoff)
		limit := sequence.sendBufferSettings.TimeoutResendDeferLimit
		firings := 0
		written := false
		// one item's schedule through the stall: it fires, is deferred while
		// the count is under the limit, and is written once it is not
		elapsed := time.Duration(0)
		deferCount := 0
		for {
			elapsed += deferralInterval(sequence, deferCount, arm.backoff)
			if stall < elapsed {
				break
			}
			firings += 1
			if limit <= deferCount {
				written = true
				break
			}
			deferCount += 1
		}
		if firings != arm.wantFirings {
			t.Errorf("%s: one item fired %d times in a %s stall, want %d",
				arm.name, firings, stall, arm.wantFirings)
		}
		if written != arm.wantWritten {
			t.Errorf("%s: a timeout written into the stall=%v, want %v",
				arm.name, written, arm.wantWritten)
		}
		t.Logf("%s: %d firings per item, %d over a window of %d items, written into the stall=%v",
			arm.name, firings, firings*itemCount, itemCount, written)
	}
}

// The flag is on: this is a landing, not a candidate.
func TestDeferralBackoffIsOnByDefault(t *testing.T) {
	if !DefaultSendBufferSettings().DeferTimeoutResendBackoff {
		t.Fatal("the deferral backoff is off by default")
	}
}
