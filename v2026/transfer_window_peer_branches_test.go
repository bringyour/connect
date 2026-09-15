package connect

import (
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// THROUGHPUTFIX §37.21. The window consumer has THREE branches on what the
// sender knows about its peer, not two, and the third exists because a sender
// that has heard nothing and a sender whose peer does not advertise are
// different facts:
//
//   - the peer advertised a capacity: the window steps to it;
//   - the peer has acknowledged but never advertised - a legacy peer: the
//     window is this sender's own constant, the shipping receive hold;
//   - nothing has been heard at all: the window is the more conservative of
//     two constants, this sender's own and the receive hold floor every
//     receiver ships.
//
// `TestTheWindowStepsToTheAdvertisedCapacity` pins the first, and pins it as a
// step rather than as a value: it reads the blind window first and then asserts
// the advertised capacity on the very next estimate with no intermediate. This
// row does not restate it. What is unpinned is the second branch entirely - the
// case a modern client talking to an older client takes - and the separation
// that keeps the second and third apart.
//
// Why the separation is the thing to assert rather than the values. `ackSeen`
// is stored separately from `receiveWindowSet` (`observeReceiveWindowAdvertisement`
// sets the first on EVERY acknowledgement and the second only on one carrying
// the field), so the two facts survive independently. Collapse them - read
// `receiveWindowSet` alone, or set `ackSeen` where the field arrives - and
// branch three silently becomes branch two: a sender that has heard nothing at
// all takes the 2 MiB constant instead of the 320 KiB floor. That is a licence
// rather than a reduction, and it is invisible in the estimate's own fields
// because both branches report the same Reason.
//
// Deterministic with no clock at all: the estimate returns at the ceiling with
// "no round trip samples", which is the branch under test and nothing beyond
// it.
func TestTheThreePeerBranchesAreThreeDifferentWindows(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })
	// scale 1, so the shipping constants read at their written values and a
	// failure message names a number a reader can find in the source
	SetMemoryBudget(0)

	const advertised = ByteCount(4 * 1024 * 1024)
	newSequence := func() *SendSequence {
		return newEstimatorFixture(t, func(settings *SendBufferSettings) {
			settings.DeliverySizedWindowScale = deliverySizedWindowScale
			// far above every value under test, so the peer branch is the
			// binding term rather than the share
			settings.ResendQueueBudget = NewTransferMemoryBudget(mib(64))
		})
	}
	initial := DefaultSendBufferSettings().ResendQueueMaxByteCount
	blindBet := min(defaultInitialWindowByteCount(), initial)

	// branch three: nothing heard
	silent := newSequence()
	silentEstimate := silent.sendWindowEstimate(time.Now())

	// branch two: acknowledged, never advertised. This is the legacy peer, and
	// the acknowledgement is delivered through the production observer rather
	// than by setting the flag, so the row tests the wiring that separates the
	// two facts and not a fixture's idea of it.
	legacy := newSequence()
	legacy.observeReceiveWindowAdvertisement(receiveAckMessage{receiveWindowSet: false})
	legacyEstimate := legacy.sendWindowEstimate(time.Now())

	// branch one: advertised
	modern := newSequence()
	modern.observeReceiveWindowAdvertisement(receiveAckMessage{
		receiveWindowSet:       true,
		receiveWindowByteCount: uint32(advertised),
	})
	modernEstimate := modern.sendWindowEstimate(time.Now())

	t.Logf(
		"silent %d, legacy %d, advertised %d; this sender's constant is %d and the blind bet is %d",
		silentEstimate.Window,
		legacyEstimate.Window,
		modernEstimate.Window,
		initial,
		blindBet,
	)

	if silentEstimate.Window != blindBet {
		t.Errorf(
			"a sender that has heard nothing takes a window of %d rather than %d, the more conservative of this sender's own constant %d and the receive hold floor %d every receiver ships. A blind sender may assume only what every receiver already has",
			silentEstimate.Window,
			blindBet,
			initial,
			defaultInitialWindowByteCount(),
		)
	}
	if legacyEstimate.Window != initial {
		t.Errorf(
			"a peer that acknowledges without the field gets a window of %d rather than this sender's own constant %d. Not the blind floor, which would be a regression for every peer not yet updated, and not a raise on no evidence: the status quo is what a legacy peer gets",
			legacyEstimate.Window,
			initial,
		)
	}
	if modernEstimate.Window != advertised {
		t.Errorf(
			"a peer advertising %d gets a window of %d",
			advertised,
			modernEstimate.Window,
		)
	}

	// The separation, asserted as a relationship rather than as three values,
	// because it is the relationship that regresses.
	if legacyEstimate.Window == silentEstimate.Window {
		t.Errorf(
			"the legacy and the silent branches both give %d. They are different facts: `ackSeen` says a peer answered and `receiveWindowSet` says it advertised, and collapsing them makes a sender that has heard nothing take the constant a peer's answer earns. That is a licence rather than a reduction, and both branches report the same Reason, so nothing else in the estimate would show it",
			legacyEstimate.Window,
		)
	}
	if legacyEstimate.Window < silentEstimate.Window {
		t.Errorf(
			"the legacy branch %d is below the silent branch %d; hearing from a peer cannot leave a sender with less than it assumed of an unheard one",
			legacyEstimate.Window,
			silentEstimate.Window,
		)
	}

	// The two facts themselves, read back at the source of truth, so a change
	// that keeps the windows right by accident and loses the distinction is
	// still caught.
	if !legacy.ackSeen.Load() {
		t.Error("a legacy acknowledgement did not set ackSeen, so the sender cannot tell a peer that answered from one that has not")
	}
	if legacy.receiveWindowSet.Load() {
		t.Error("an acknowledgement without the field set receiveWindowSet, so a legacy peer is read as having advertised")
	}
	if _, ok := legacy.receivedWindowAdvertisement(); ok {
		t.Error("receivedWindowAdvertisement reports an advertisement for a peer that never sent one")
	}
	if silent.ackSeen.Load() {
		t.Error("a sequence that has heard nothing reports ackSeen")
	}
}

// Branch three's own property, which the values above do not test: the blind
// bet is the MINIMUM of two constants and not the receive hold floor.
//
// THROUGHPUTFIX §37.21 states it as "the bet is a reduction, not a licence -
// on any configuration whose own constant is below the receive hold's floor,
// the constant is the more conservative of the two and is what a blind sender
// takes". On the shipping settings the floor is the smaller of the two, so a
// tree that wrote `ceiling = min(ceiling, defaultInitialWindowByteCount())`
// would pass every row above and be wrong: a deployment whose own constant is
// small would have its blind window RAISED to a value it never intended to
// send, on no evidence at all.
//
// The row constructs the inversion rather than waiting for a deployment to.
func TestTheBlindBetIsTheSmallerOfTheTwoConstants(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })
	SetMemoryBudget(0)

	// a configuration whose own constant sits BELOW the receive hold floor,
	// which is the case the minimum exists for
	small := defaultInitialWindowByteCount() / 4
	if small <= 0 {
		t.Fatal("the receive hold floor is not positive, so the inversion cannot be constructed")
	}
	sequence := newEstimatorFixture(t, func(settings *SendBufferSettings) {
		settings.DeliverySizedWindowScale = deliverySizedWindowScale
		settings.ResendQueueBudget = NewTransferMemoryBudget(mib(64))
		settings.ResendQueueMaxByteCount = small
		// below the constant, so the floor does not mask the term under test
		settings.ResendQueueMinByteCount = small / 2
	})

	estimate := sequence.sendWindowEstimate(time.Now())
	t.Logf(
		"own constant %d against the receive hold floor %d: blind window %d",
		small,
		defaultInitialWindowByteCount(),
		estimate.Window,
	)
	if estimate.Window != small {
		t.Errorf(
			"a blind sender whose own constant is %d takes a window of %d; the blind bet is min(the receive hold floor %d, this sender's constant), a reduction and never a licence. A tree that clamps to the floor alone raises this deployment's opening burst above anything it configured",
			small,
			estimate.Window,
			defaultInitialWindowByteCount(),
		)
	}
	if defaultInitialWindowByteCount() <= estimate.Window {
		t.Errorf(
			"the blind window %d reached the receive hold floor %d, which this configuration's own constant is below",
			estimate.Window,
			defaultInitialWindowByteCount(),
		)
	}
}

// Guard one under the three branches: an acknowledgement from a legacy peer
// cannot overwrite a capacity a modern peer already advertised.
//
// `observeReceiveWindowAdvertisement` returns early when `receiveWindowSet` is
// false, BEFORE touching `receiveWindowByteCount` or `receiveWindowSet`. Remove
// that early return and an ordinary acknowledgement carrying no field writes a
// zero capacity over a good one, which reads as a receiver with no room at all
// and collapses the window to its floor for the life of the sequence. The
// arrangement that prevents it is one `return` and nothing asserted it.
//
// The step timestamp is asserted alongside, because it is the other thing that
// must not move: `receiveWindowSetAtNanos` is written only on the FIRST
// advertisement, and the delivery term refuses any delivery measured before it.
// Rewriting it on a later acknowledgement would re-lag the delivery term and
// discard evidence the sender had already earned.
func TestALegacyAcknowledgementCannotOverwriteAnAdvertisement(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })
	SetMemoryBudget(0)

	const advertised = ByteCount(4 * 1024 * 1024)
	sequence := newEstimatorFixture(t, func(settings *SendBufferSettings) {
		settings.DeliverySizedWindowScale = deliverySizedWindowScale
		settings.ResendQueueBudget = NewTransferMemoryBudget(mib(64))
	})

	sequence.observeReceiveWindowAdvertisement(receiveAckMessage{
		receiveWindowSet:       true,
		receiveWindowByteCount: uint32(advertised),
	})
	steppedAtNanos := sequence.receiveWindowSetAtNanos.Load()
	if steppedAtNanos == 0 {
		t.Fatal("the first advertisement did not mark the step, so the delivery term's lag has no anchor")
	}

	// an ordinary acknowledgement carrying no capacity, of the kind every
	// acknowledgement from a peer that does not advertise is
	sequence.observeReceiveWindowAdvertisement(receiveAckMessage{receiveWindowSet: false})

	held, ok := sequence.receivedWindowAdvertisement()
	if !ok {
		t.Fatal("an acknowledgement without the field cleared the peer's advertisement, so a single legacy-shaped ack retires a capacity the peer really stated")
	}
	if held != advertised {
		t.Errorf(
			"the peer's advertised capacity reads %d after an acknowledgement carrying no field, against the %d it advertised. Absent is not zero: an acknowledgement that does not carry the field says nothing about the receiver's capacity and must leave it exactly as it was",
			held,
			advertised,
		)
	}
	estimate := sequence.sendWindowEstimate(time.Now())
	if estimate.Window != advertised {
		t.Errorf(
			"the window is %d after a fieldless acknowledgement followed a %d byte advertisement; a zero written over a good capacity reads as a receiver with no room and pins the window at its floor for the life of the sequence",
			estimate.Window,
			advertised,
		)
	}
	if now := sequence.receiveWindowSetAtNanos.Load(); now != steppedAtNanos {
		t.Errorf(
			"the step timestamp moved from %d to %d on a later acknowledgement. It marks the first advertisement, and the delivery term refuses delivery measured before it, so rewriting it discards evidence the sender had already earned and re-lags a window that had already stepped",
			steppedAtNanos,
			now,
		)
	}

	// a SECOND real advertisement updates the capacity and still does not move
	// the step
	const raised = ByteCount(8 * 1024 * 1024)
	sequence.observeReceiveWindowAdvertisement(receiveAckMessage{
		receiveWindowSet:       true,
		receiveWindowByteCount: uint32(raised),
	})
	if held, _ := sequence.receivedWindowAdvertisement(); held != raised {
		t.Errorf("a later advertisement of %d did not update the capacity, which reads %d", raised, held)
	}
	if now := sequence.receiveWindowSetAtNanos.Load(); now != steppedAtNanos {
		t.Errorf(
			"the step timestamp moved from %d to %d on a second advertisement; the step happened once, when the sender stopped being blind",
			steppedAtNanos,
			now,
		)
	}
}

// Guard two: the wire field is optional, so ABSENT and ZERO are different
// messages and must take different branches.
//
// `receive_window_byte_count` is a pointer in the generated Go, and
// `receiveAckMessageFromProtocol` sets `receiveWindowSet` only when the pointer
// is non-nil. Absent means a peer that does not advertise - the legacy branch,
// this sender's own constant. Zero means a receiver stating it has no capacity
// at all - the advertised branch, clamped to nothing and held up only by the
// working floor. Reading a nil pointer as a zero, which is what a hand-rolled
// decode or a proto3 non-optional field would do, silently merges a live
// protocol case into the legacy one and gives a receiver with no room the
// window of a receiver that never spoke.
//
// Driven through the real decode from a real `protocol.Ack`, so the row tests
// the wire contract and not a struct literal's idea of it.
func TestAnAbsentCapacityAndAZeroCapacityTakeDifferentBranches(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })
	SetMemoryBudget(0)

	messageId := NewId()
	sequenceId := NewId()
	newAck := func() *protocol.Ack {
		return &protocol.Ack{
			MessageId:  messageId.Bytes(),
			SequenceId: sequenceId.Bytes(),
		}
	}

	absentAck := newAck()
	absent, err := receiveAckMessageFromProtocol(absentAck)
	if err != nil {
		t.Fatalf("decoding an acknowledgement with no capacity field: %s", err)
	}
	zero := uint64(0)
	zeroAck := newAck()
	zeroAck.ReceiveWindowByteCount = &zero
	zeroed, err := receiveAckMessageFromProtocol(zeroAck)
	if err != nil {
		t.Fatalf("decoding an acknowledgement with a zero capacity: %s", err)
	}

	if absent.receiveWindowSet {
		t.Error("an acknowledgement with no capacity field decoded as having advertised one; a nil pointer read as a zero turns every legacy peer into a receiver claiming no room")
	}
	if !zeroed.receiveWindowSet {
		t.Error("an acknowledgement carrying an explicit zero capacity decoded as not having advertised; zero is a receiver stating it has no room, which is a statement and not a silence")
	}
	if zeroed.receiveWindowByteCount != 0 {
		t.Errorf("an explicit zero capacity decoded as %d", zeroed.receiveWindowByteCount)
	}

	newSequence := func() *SendSequence {
		return newEstimatorFixture(t, func(settings *SendBufferSettings) {
			settings.DeliverySizedWindowScale = deliverySizedWindowScale
			settings.ResendQueueBudget = NewTransferMemoryBudget(mib(64))
		})
	}
	absentSequence := newSequence()
	absentSequence.observeReceiveWindowAdvertisement(absent)
	absentEstimate := absentSequence.sendWindowEstimate(time.Now())

	zeroSequence := newSequence()
	zeroSequence.observeReceiveWindowAdvertisement(zeroed)
	zeroEstimate := zeroSequence.sendWindowEstimate(time.Now())

	settings := DefaultSendBufferSettings()
	initial := settings.ResendQueueMaxByteCount
	floor := settings.ResendQueueMinByteCount
	t.Logf(
		"absent gives window %d (this sender's constant %d), zero gives window %d (the working floor %d)",
		absentEstimate.Window, initial, zeroEstimate.Window, floor,
	)

	if absentEstimate.Window != initial {
		t.Errorf(
			"an absent capacity gives a window of %d rather than this sender's own constant %d; absent is a peer that does not advertise",
			absentEstimate.Window,
			initial,
		)
	}
	if zeroEstimate.Window != floor {
		t.Errorf(
			"a zero capacity gives a window of %d rather than the working floor %d; a receiver stating no room is clamped to nothing and held up only by the floor reliable admission already needs",
			zeroEstimate.Window,
			floor,
		)
	}
	if absentEstimate.Window == zeroEstimate.Window {
		t.Errorf(
			"an absent and a zero capacity both give %d. The field is optional precisely so the two can be told apart, and merging them hands a receiver with no capacity the window of a peer that never spoke",
			absentEstimate.Window,
		)
	}
}
