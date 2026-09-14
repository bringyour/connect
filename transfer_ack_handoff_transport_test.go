package connect

import (
	"testing"
	"time"
)

// The acknowledgement handoff's wait is transport-dependent, and that is worth
// asserting against whether or not it is currently causing harm.
//
// A correction to how that was described to me, because the axis matters. The
// selection is written per transport type — `ackHandoffTimeout` returns
// `H1AckHandoffTimeout` for H1 and zero otherwise, and `packHandoffTimeout`
// does the same with `H1PackHandoffTimeout` — but neither H1 field is set in
// the shipping settings, so on the defaults every transport reads zero and that
// axis is uniform. The asymmetry that does exist is on the reliability axis:
// `packHandoffTimeout` returns `ReliablePackHandoffTimeout`, which ships at -1,
// a wait until capacity or cancellation, for a route published as reliable, and
// zero for an unreliable one. So a handoff that fills has bounded backpressure
// on a reliable carrier and a non-blocking refusal on an unreliable one.
//
// Why it matters independently of any current defect. The whole class of
// handoff-overflow faults is invisible on the one path anyone would naturally
// reach for: a cell run over H1, or over a route published as reliable, waits
// where production's other transports drop. A defect that only appears off the
// default test path is one that gets found in the field.
//
// This row makes no claim that acknowledgements are being dropped today. A live
// cell measured zero handoff drops, zero queue-full events, zero misses and
// zero timeout resends across twenty thousand items at two path lengths, and
// the mechanism that predicted otherwise was refuted by those readings. What
// stands is the asymmetry itself.
//
// Prediction, recorded before the run: the acknowledgement handoff wait differs
// between H1 and the other transports on the shipping settings, so this fails
// on the tree as it stands.
func TestTheHandoffWaitIsTheSameForEveryTransport(t *testing.T) {
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
				"the acknowledgement handoff waits %s on %v and %s on %v",
				ackWaits[reference], reference, ackWaits[transportType], transportType,
			)
		}
		if packWaits[transportType] != packWaits[reference] {
			t.Errorf(
				"the pack handoff waits %s on %v and %s on %v",
				packWaits[reference], reference, packWaits[transportType], transportType,
			)
		}
	}

	// and the reliability axis, which is where the asymmetry actually is
	if reliableWait != unreliableWait {
		t.Errorf(
			"the pack handoff waits %s on a reliable carrier and %s on an unreliable one; a handoff that fills has bounded backpressure on one and a non-blocking refusal on the other, so the whole class of overflow faults is invisible on the path a cell would naturally reach for",
			reliableWait, unreliableWait,
		)
	}
}
