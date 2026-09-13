package connect

// FLIGHTGATEFIX §22. The reliable-lane admission bound: a lane may hold
// unacknowledged what it has shown it can carry, measured by delivery
// rather than by the round-trip mean, which lags an inflation by design.

import (
	"testing"
	"time"
)

func admissionSequence(t testing.TB) *SendSequence {
	t.Helper()
	sequence, _ := newSelectiveAckRecoveryTestSequence(1, time.Unix(1_700_000_000, 0))
	sequence.client = &Client{}
	sequence.flightController = newSendFlightController(sequence.sendBufferSettings)
	// the ring is allocated only where the flag is on, as production does,
	// because an off flag must not retain bytes (FLIGHTGATEFIX §29.4)
	sequence.sendBufferSettings.ReliableAdmissionBoundedByDelivery = true
	sequence.deliveredBytes = make([]deliveredBytesSample, deliveredBytesRingSize)
	return sequence
}

// A sequence built with the flag off retains no ring at all.
func TestDeliveredBytesRingIsNotRetainedWhenOff(t *testing.T) {
	if DefaultSendBufferSettings().ReliableAdmissionBoundedByDelivery {
		t.Skip("the bound is on by default, so the ring is always built")
	}
	sequence, _ := newSelectiveAckRecoveryTestSequence(1, time.Unix(1_700_000_000, 0))
	sequence.client = &Client{}
	if sequence.deliveredBytes != nil {
		t.Fatal("a sequence built without the delivery bound still holds its ring")
	}
	// and the accessors are safe on it
	sequence.observeDeliveredBytes(1024, time.Now())
	if over := sequence.deliveredBytesOver(time.Second, time.Now()); over != 0 {
		t.Fatalf("delivery over a second reads %d B with no ring", over)
	}
}

// The measure is delivery over a window, and it follows a step down within
// one window rather than lagging it.
func TestDeliveredBytesMeasuresTheDrain(t *testing.T) {
	sequence := admissionSequence(t)
	now := time.Now()
	// one second at about 1.86 MB/s
	for index := range 40 {
		sequence.observeDeliveredBytes(46_500, now.Add(time.Duration(index)*25*time.Millisecond))
	}
	end := now.Add(time.Second)
	if over := sequence.deliveredBytesOver(300*time.Millisecond, end); over < 400_000 || 520_000 < over {
		t.Fatalf("delivery over 300 ms reads %d B, want about 465 KB", over)
	}
	// the drain steps down by twenty-four times
	for index := range 12 {
		sequence.observeDeliveredBytes(1_900, end.Add(time.Duration(index+1)*25*time.Millisecond))
	}
	stepped := end.Add(300 * time.Millisecond)
	if over := sequence.deliveredBytesOver(300*time.Millisecond, stepped); 60_000 < over {
		t.Fatalf(
			"one window after a step down delivery still reads %d B: the measure must follow the "+
				"drain, which is the whole reason it is not the round-trip mean",
			over,
		)
	}
}

// The bound never falls below the per-sequence floor, so a lane delivering
// more than the floor per scaled round trip is never held back by it.
func TestReliableAdmissionBoundDoesNotStarveAFastLane(t *testing.T) {
	sequence := admissionSequence(t)
	floor := sequence.sendBufferSettings.ResendQueueMinByteCount
	if limit := sequence.reliableAdmissionByteLimit(time.Now()); limit != floor {
		t.Fatalf("with nothing delivered the bound is %d B, want the floor %d B", limit, floor)
	}
	now := time.Now()
	for index := range 16 {
		sequence.observeDeliveredBytes(4<<20, now.Add(time.Duration(index)*100*time.Millisecond))
	}
	if limit := sequence.reliableAdmissionByteLimit(now.Add(1600 * time.Millisecond)); limit <= floor {
		t.Fatalf("a lane delivering megabytes is bounded at %d B, at or under the floor %d B", limit, floor)
	}
}

// Under the mobile budget the floor is the budget, so the bound can never
// admit less than today and the low-bar cells are untouched by
// construction. The wait counter is how that is measured rather than
// asserted.
func TestReliableAdmissionBoundIsInertUnderTheMobileBudget(t *testing.T) {
	sequence := admissionSequence(t)
	settings := sequence.sendBufferSettings
	settings.ReliableAdmissionBoundedByDelivery = true
	settings.ResendQueueMinByteCount = settings.ResendQueueMaxByteCount
	policy := transferFlightPolicySnapshot{generation: 1}
	if !sequence.reliableAdmissionAvailable(policy, time.Now()) {
		t.Fatal("the bound blocked admission where its floor is the whole budget")
	}
	if count := sequence.client.SendRecoveryStats().ReliableAdmissionWaitCount; count != 0 {
		t.Fatalf("the bound waited %d times under the mobile budget, where it must be inert", count)
	}
}

// The default is off, and off is exactly today's behaviour.
func TestReliableAdmissionBoundIsOffByDefault(t *testing.T) {
	if DefaultSendBufferSettings().ReliableAdmissionBoundedByDelivery {
		t.Fatal("the delivery bound is on by default, but it has not measured as a win")
	}
	sequence := admissionSequence(t)
	if !sequence.reliableAdmissionAvailable(transferFlightPolicySnapshot{generation: 1}, time.Now()) {
		t.Fatal("admission was refused with the bound off")
	}
}

// The ring is built with the sequence and neither it nor the admission
// check allocates.
func TestDeliveredBytesRingAllocatesNothing(t *testing.T) {
	sequence := admissionSequence(t)
	sequence.sendBufferSettings.ReliableAdmissionBoundedByDelivery = true
	policy := transferFlightPolicySnapshot{generation: 1}
	now := time.Now()
	index := 0
	if allocs := testing.AllocsPerRun(1000, func() {
		index += 1
		sequence.observeDeliveredBytes(1024, now.Add(time.Duration(index)*time.Millisecond))
		sequence.reliableAdmissionAvailable(policy, now)
	}); allocs != 0 {
		t.Fatalf("the delivery bound allocates %.1f per admission", allocs)
	}
}
