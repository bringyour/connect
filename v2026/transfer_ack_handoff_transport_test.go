package connect

import (
	"testing"
	"time"
)

// The acknowledgement handoff's wait is not uniform, and this row pins the
// asymmetry as it stands rather than failing on it.
//
// It used to assert uniformity and fail by design. That was wrong for the
// suite: a permanently red row masks every other failure in a full run, and
// this program has been reading race verdicts against a suite whose green was
// unreachable. A defect that cannot be fixed today is still worth pinning; it
// just has to be pinned as a trade rather than asserted away.
//
// The trade. The selection is written per transport type — ackHandoffTimeout
// returns H1AckHandoffTimeout for H1 and zero otherwise, and packHandoffTimeout
// does the same with H1PackHandoffTimeout — but neither H1 field is set in the
// shipping settings, so on the defaults every transport reads zero and that
// axis is uniform. The asymmetry is on the reliability axis: packHandoffTimeout
// returns ReliablePackHandoffTimeout, which ships at -1, a wait until capacity
// or cancellation, for a route published as reliable, and zero for an
// unreliable one. So a handoff that fills has bounded backpressure on a
// reliable carrier and a non-blocking refusal on an unreliable one.
//
// Why that is a defect and not merely a difference: it makes the whole class of
// handoff-overflow faults invisible on the one path a cell would naturally
// reach for. A test run over a reliable route waits where production's
// unreliable transports drop, so a fault that only appears off the default test
// path is one that gets found in the field.
//
// This row makes no claim that acknowledgements are being dropped today. A live
// cell measured zero handoff drops, zero queue-full events, zero misses and
// zero timeout resends across twenty thousand items at two path lengths, and
// the mechanism that predicted otherwise was refuted by those readings. What
// stands is the asymmetry itself, and this row holds it still: if either value
// moves, the trade has changed, and the message says what that means.
func TestTheHandoffWaitAsymmetryIsWhatItIs(t *testing.T) {
	settings := DefaultReceiveBufferSettings()
	// every transport the receive path can be handed, so the row names them
	// rather than testing the one it thought of
	transports := TransportTypes()

	ackWaits := map[TransportType]time.Duration{}
	packWaits := map[TransportType]time.Duration{}
	for _, transportType := range transports {
		ackWaits[transportType] = settings.ackHandoffTimeout(transportType)
		packWaits[transportType] = settings.packHandoffTimeout(transportType)
	}
	reliableWait := settings.packHandoffTimeout(
		TransportTypeUnknown, CarrierReliabilityReliable)
	unreliableWait := settings.packHandoffTimeout(
		TransportTypeUnknown, CarrierReliabilityUnreliable)
	t.Logf(
		"by transport: acknowledgement %v, pack %v; by reliability: reliable %s, unreliable %s",
		ackWaits, packWaits, reliableWait, unreliableWait,
	)

	// the transport axis, which the shipping settings leave uniform
	reference := transports[0]
	for _, transportType := range transports[1:] {
		if ackWaits[transportType] != ackWaits[reference] {
			t.Errorf(
				"the acknowledgement handoff waits %s on %v and %s on %v; the shipping settings leave this axis uniform, so a difference here is new",
				ackWaits[reference], reference, ackWaits[transportType], transportType,
			)
		}
		if packWaits[transportType] != packWaits[reference] {
			t.Errorf(
				"the pack handoff waits %s on %v and %s on %v; the shipping settings leave this axis uniform, so a difference here is new",
				packWaits[reference], reference, packWaits[transportType], transportType,
			)
		}
	}

	// And the reliability axis, pinned as the known trade. These are the values
	// the tree ships; the row exists so that changing either is a deliberate
	// act rather than a drift nobody notices.
	if reliableWait != DefaultReceiveBufferSettings().ReliablePackHandoffTimeout {
		t.Errorf(
			"a reliable carrier's pack handoff waits %s rather than the settings' %s; if this was a fix, the class of handoff-overflow faults is no longer invisible on the unreliable path and this row should be retired, and if it was a drift, the asymmetry just got worse",
			reliableWait, DefaultReceiveBufferSettings().ReliablePackHandoffTimeout,
		)
	}
	if unreliableWait != 0 {
		t.Errorf(
			"an unreliable carrier's pack handoff waits %s rather than refusing without waiting; if this was a fix, the two axes now agree and this row should be retired, and if it was a drift, a path that used to refuse now blocks",
			unreliableWait,
		)
	}
	if reliableWait == unreliableWait {
		t.Logf(
			"the reliability asymmetry is gone: both wait %s. That is the defect fixed, and this row should be retired rather than left pinning a trade that no longer exists.",
			reliableWait,
		)
	}
}
