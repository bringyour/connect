package connect

// THROUGHPUTFIX §39.1, the acknowledged-ahead half of contract pipelining.
//
// The prefetch half already keeps a successor contract in the destination
// queue, so the platform round trip is off the data path. What remained is the
// sender's own wait: at exhaustion it opens the successor and waits for that
// open to be acknowledged, and while it waits every no-acknowledgement Pack is
// promoted onto the acknowledged lane. Measured at 23 per cent of a contract's
// life at a 200 ms round trip and 47 at 400.
//
// The unit is indivisible and these rows read it in the order the bytes move:
// the capability the receiver advertises, the threshold the sender derives, the
// announcement it sends, the receiver storing it without switching, and the
// switch at exhaustion that promotes nothing.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// The capability gate. A sender must not announce ahead to a peer that never
// advertised, and must stop announcing to one that stops: a receiver can
// restart into an older build, and a route can end somewhere else.
//
// Delivery acknowledgements only. A selective acknowledgement says the receiver
// is holding an item out of order; it is not evidence about what the receiver
// does with a contract frame, and reading it would let one selective ack
// between two deliveries flap the capability off.
func TestTheContractAheadCapabilityIsReadFromDeliveryAcknowledgements(t *testing.T) {
	sequence := &SendSequence{}

	if sequence.contractAheadSupported.Load() {
		t.Fatal("a sequence that has heard nothing believes its peer announces ahead")
	}

	// a selective acknowledgement carrying the bit is not evidence
	sequence.observeContractAheadCapability(receiveAckMessage{
		selective:              true,
		contractAheadSupported: true,
	})
	if sequence.contractAheadSupported.Load() {
		t.Error("a selective acknowledgement turned the capability on, so a peer that never delivered anything would be announced to")
	}

	sequence.observeContractAheadCapability(receiveAckMessage{
		contractAheadSupported: true,
	})
	if !sequence.contractAheadSupported.Load() {
		t.Fatal("a delivery acknowledgement carrying the capability did not turn it on")
	}

	// and a selective acknowledgement without it does not turn it off
	sequence.observeContractAheadCapability(receiveAckMessage{selective: true})
	if !sequence.contractAheadSupported.Load() {
		t.Error("a selective acknowledgement retired the capability between two deliveries that carry it")
	}

	// the retirement case: a later delivery acknowledgement omits it
	sequence.observeContractAheadCapability(receiveAckMessage{})
	if sequence.contractAheadSupported.Load() {
		t.Error("a delivery acknowledgement that dropped the capability left it on; a peer that restarted into an older build would keep being announced to")
	}
}

// The capability's four shapes, driven through the production marshaller and
// decoder rather than by assigning the flag, so what is pinned is the wiring
// and not the field.
//
// Never-present. A modern sender against a peer that never sets the bit stays
// on the conservative path for the life of the sequence: it never announces,
// and it never takes a contract out of the destination queue to announce with.
func TestASenderStaysConservativeAgainstAPeerThatNeverAdvertisesContractAhead(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, sequence, _, contract := newSendNoContractHarness(t, ctx)
	contract.ackedByteCount = contract.effectiveTransferByteCount

	// ten deliveries from a legacy receiver, each through the real wire
	for range 10 {
		sequence.observeContractAheadCapability(decodeContractAheadTestAck(t, false, false))
		sequence.maybeAnnounceContractAhead()
	}
	if sequence.contractAheadSupported.Load() {
		t.Fatal("a peer that never set the bit was read as advertising it")
	}
	if sequence.aheadSendContract != nil {
		t.Fatal("a sender announced a contract ahead to a peer that never advertised")
	}
	if sequence.aheadSendContractAttempted {
		t.Error("a sender polled the contract queue to announce to a legacy peer")
	}
}

// Withdrawable, which is the model this bit chose and states in the proto, and
// it has the same two triggers as logical_lane_version. Both are pinned here,
// because the second is the one that goes unpinned for as long as it goes
// unwritten.
//
// Trigger one: a later delivery acknowledgement omits the capability. Trigger
// two: the capability is scoped to the SendSequence that learned it, so a
// second sequence to the same destination starts conservative and must learn it
// again from its own delivery acknowledgement.
func TestTheContractAheadCapabilityIsWithdrawnByBothTriggers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, sequence, destinationId, contract := newSendNoContractHarness(t, ctx)
	contract.ackedByteCount = contract.effectiveTransferByteCount

	sequence.observeContractAheadCapability(decodeContractAheadTestAck(t, true, false))
	if !sequence.contractAheadSupported.Load() {
		t.Fatal("a delivery acknowledgement carrying the capability did not turn it on")
	}

	// trigger one, mid-session: the peer stops saying it
	sequence.observeContractAheadCapability(decodeContractAheadTestAck(t, false, false))
	if sequence.contractAheadSupported.Load() {
		t.Error("a delivery acknowledgement that dropped the capability left it on")
	}
	sequence.maybeAnnounceContractAhead()
	if sequence.aheadSendContract != nil || sequence.aheadSendContractAttempted {
		t.Error("a sender announced after the capability was withdrawn")
	}

	// trigger two: the scope. A second sequence to the same destination is a
	// new sequence, and the evidence belonged to the first.
	second := NewSendSequence(
		ctx,
		client,
		nil,
		destinationId,
		MultiHopId{},
		false,
		false,
		false,
		sequenceTlsRoleClient,
		false,
		DefaultSendBufferSettings(),
	)
	t.Cleanup(second.Close)
	if second.contractAheadSupported.Load() {
		t.Error("a new sequence inherited the capability from a sequence that had learned it; the evidence is per sequence, and a peer that changed underneath is exactly what a new sequence cannot know")
	}
}

// The wire shape. This bit is a plain scalar, so absent and false are one fact:
// a receiver that will not register an announcement. The row asserts that a
// legacy acknowledgement — one marshalled with no field 10 at all — decodes to
// the zero that drives the fallback, through the production marshaller and the
// production decoder.
func TestALegacyAcknowledgementDecodesToTheContractAheadFallback(t *testing.T) {
	legacy := decodeContractAheadTestAck(t, false, false)
	if legacy.contractAheadSupported {
		t.Fatal("an acknowledgement with no contract_ahead field decoded as advertising it")
	}
	modern := decodeContractAheadTestAck(t, true, false)
	if !modern.contractAheadSupported {
		t.Fatal("an acknowledgement carrying contract_ahead decoded without it")
	}
	// and the field really is absent rather than present-and-false, which is
	// what keeps a legacy peer's wire byte for byte what it was
	saf := sendAckFrame{
		path:       DestinationId(NewId()).AddSource(NewId()),
		messageId:  NewId(),
		sequenceId: NewId(),
	}
	frameBytes := marshalSendAckTransferFrame(&saf)
	defer MessagePoolReturn(frameBytes)
	if ackFrameHasField(t, frameBytes, 10) {
		t.Error("an acknowledgement that does not advertise still writes field 10")
	}
}

// The reverse direction: we advertise and the peer ignores it. The cost is
// bounded and it is zero — a receiver that advertises and never receives an
// announcement behaves exactly as it does today, because the only thing the
// advertisement buys is a sender's permission to send one more kind of Pack.
// Stated as an assertion rather than as an assumption that the peer cooperates.
func TestAdvertisingContractAheadCostsNothingAgainstASenderThatIgnoresIt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	receiver, sequence, sourceId := newContractAheadTestReceiveSequence(t, ctx)
	if !receiver.settings.ReceiveBufferSettings.AcceptContractAhead {
		t.Fatal("the shipping receiver does not advertise that it registers announcements")
	}

	// a sender that ignores the advertisement: ordinary opening contracts, one
	// after another, exactly as today
	first := contractAheadTestContractFrame(t, receiver, sourceId, receiver.ClientId())
	if err := sequence.registerContracts(&receiveItem{contractFrame: first}); err != nil {
		t.Fatalf("register the first contract: %v", err)
	}
	firstContract := sequence.receiveContract
	second := contractAheadTestContractFrame(t, receiver, sourceId, receiver.ClientId())
	if err := sequence.registerContracts(&receiveItem{contractFrame: second}); err != nil {
		t.Fatalf("register the second contract: %v", err)
	}
	if sequence.receiveContract == firstContract {
		t.Fatal("an opening contract did not become current, so advertising changed what an ignoring sender gets")
	}
	if len(sequence.openReceiveContracts) != 2 {
		t.Errorf(
			"the receiver holds %d contracts after two opens, want the two it would hold today",
			len(sequence.openReceiveContracts),
		)
	}
	// nothing is reserved for an announcement that never comes
	if sequence.receiveBufferSettings.MaxOpenReceiveContract != DefaultReceiveBufferSettings().MaxOpenReceiveContract {
		t.Error("advertising changed how many contracts the receiver will hold")
	}
}

// The threshold, derived from the sequence's own rate ring and minimum round
// trip — the same two quantities the window rule reads — and the floor where
// there is no evidence to derive from. Both are asserted, because a caller
// cannot tell them apart and a floor silently standing in for a derivation is
// how a constant gets back in.
func TestTheContractAheadThresholdIsDerivedOrTheFloor(t *testing.T) {
	settings := DefaultSendBufferSettings()
	if settings.ContractAheadScale <= 0 {
		t.Fatal("the shipping settings do not announce contracts ahead at all")
	}

	// no samples: the floor, exactly
	blind := newContractAheadTestSequence(t, settings)
	if got := blind.announceAheadByteCount(); got != settings.ContractAheadFloorByteCount {
		t.Errorf(
			"a sequence with no round trip samples derived a %d byte threshold rather than the %d byte floor",
			got,
			settings.ContractAheadFloorByteCount,
		)
	}

	// sampled: scale times what the path delivered per minimum round trip
	const roundTrip = 200 * time.Millisecond
	const deliveredByteCount = ByteCount(4 * 1024 * 1024)
	const span = 2 * time.Second
	sampled := newContractAheadTestSequence(t, settings)
	// The product is read through the window estimate, which is the one owner
	// of the rate ring (TestTheWindowHasOneOwner), and the estimate reaches
	// its delivery term only with a budget to size against and a peer it can
	// see; a sequence with neither holds the constant and reports no rate,
	// and this row would be measuring the floor twice.
	sampled.resendQueue = newResendQueue(
		NewTransferMemoryBudget(mib(64)),
		settings.ResendQueueMinByteCount,
	)
	sampled.ackSeen.Store(true)
	sampled.receiveWindowByteCount.Store(uint64(mib(64)))
	sampled.receiveWindowSet.Store(true)
	addContractAheadRoundTripSample(sampled, roundTrip)
	addContractAheadDeliverySamples(sampled, deliveredByteCount, span)
	// The expectation is the rule applied to the estimate's own readings, not
	// to the numbers this row handed it: the round trip window stamps in
	// milliseconds, so its minimum is the sample rounded, and asserting
	// against the unrounded input would be asserting the fixture rather than
	// the rule.
	estimate := sampled.sendWindowEstimate(time.Now())
	if estimate.Interval <= 0 || estimate.RoundTrip <= 0 {
		t.Fatalf("the sampled arm's estimate carries no delivery rate (%q), so it is measuring the floor", estimate.Reason)
	}
	measured, measuredSpan, roundTripMin := estimate.DeliveredByteCount, estimate.Interval, estimate.RoundTrip
	perRoundTrip := ByteCount(int64(measured) * roundTripMin.Nanoseconds() / measuredSpan.Nanoseconds())
	want := max(
		ByteCount(settings.ContractAheadScale)*perRoundTrip,
		settings.ContractAheadFloorByteCount,
	)
	if got := sampled.announceAheadByteCount(); got != want {
		t.Errorf(
			"a sequence delivering %d bytes over %s at a %s round trip derived a %d byte threshold, want %d (%d per round trip, scale %d)",
			measured, measuredSpan, roundTripMin, got, want, perRoundTrip, settings.ContractAheadScale,
		)
	}
	if want <= settings.ContractAheadFloorByteCount {
		t.Fatal("this row's sampled arm did not exceed the floor, so it is measuring the floor twice")
	}
}

// The announcement is refused in exactly the cases that leave today's
// behaviour, and the refusal is the sender's own state rather than a failed
// send: nothing is taken from the contract queue, so a legacy peer's
// destination queue is not disturbed either.
func TestASenderDoesNotAnnounceAheadToALegacyPeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, sequence, _, contract := newSendNoContractHarness(t, ctx)
	// a contract with nothing left, so the only thing standing between this
	// sequence and an announcement is the capability
	contract.ackedByteCount = contract.effectiveTransferByteCount

	sequence.maybeAnnounceContractAhead()
	if sequence.aheadSendContract != nil {
		t.Fatal("a sender announced a contract ahead to a peer that never advertised the capability")
	}
	if count := client.SendRecoveryStats().ContractAheadAnnounceCount; count != 0 {
		t.Fatalf("%d announcements were made to a legacy peer", count)
	}

	// and with the announcement configured off, even a peer that advertises
	// gets today's behaviour
	sequence.contractAheadSupported.Store(true)
	sequence.sendBufferSettings.ContractAheadScale = 0
	sequence.maybeAnnounceContractAhead()
	if sequence.aheadSendContract != nil {
		t.Fatal("a sender announced a contract ahead with the announcement configured off")
	}
}

// The switch, which is the whole point of the unit: at exhaustion the
// announced successor becomes current with `sendContractAcked` taken from its
// acknowledgement rather than from false, so nothing is promoted, and the next
// Pack carries its frame so the receiver switches where it switches today.
func TestTheAnnouncedContractSwitchesWithoutPromotion(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, sequence, destinationId, contract := newSendNoContractHarness(t, ctx)

	ahead := newContractAheadTestContract(t, client, destinationId)
	ahead.acknowledgedAhead = true
	sequence.openSendContracts[ahead.contractId] = ahead
	sequence.aheadSendContract = ahead
	sequence.aheadSendContractMetadataGeneration = sequence.contractMetadata().generation

	const messageByteCount = ByteCount(1024)
	if !sequence.setAheadContract(messageByteCount) {
		t.Fatal("the announced successor was refused at exhaustion")
	}
	if sequence.sendContract != ahead {
		t.Fatal("the announced successor did not become the current contract")
	}
	if !sequence.sendContractAcked {
		t.Fatal("the switch left the contract unacknowledged, so every no-acknowledgement Pack is promoted for a round trip — which is the wait this change removes")
	}
	if !sequence.sendContractFrameDue {
		t.Fatal("no Pack will carry the new contract's frame, so the receiver never switches to it")
	}
	if sequence.aheadSendContract != nil {
		t.Fatal("the successor is still announced after becoming current")
	}
	if ahead.ackedByteCount+ahead.unackedByteCount < messageByteCount {
		t.Errorf(
			"the message was not debited to the contract it switched to: %d + %d against %d bytes",
			ahead.ackedByteCount, ahead.unackedByteCount, messageByteCount,
		)
	}
	stats := client.SendRecoveryStats()
	if stats.ContractAheadSwitchCount != 1 {
		t.Errorf("the switch was counted %d times", stats.ContractAheadSwitchCount)
	}
	if stats.ContractAheadUnacknowledgedSwitchCount != 0 {
		t.Errorf(
			"%d switches were counted as unacknowledged, and this one was acknowledged before it happened",
			stats.ContractAheadUnacknowledgedSwitchCount,
		)
	}

	// the fallback, in the same shape: an announcement that has not been
	// acknowledged yet still switches, and then promotes exactly as today
	// until its acknowledgement arrives
	_ = contract
	late := newContractAheadTestContract(t, client, destinationId)
	sequence.openSendContracts[late.contractId] = late
	sequence.aheadSendContract = late
	sequence.aheadSendContractMetadataGeneration = sequence.contractMetadata().generation
	if !sequence.setAheadContract(messageByteCount) {
		t.Fatal("an unacknowledged announced successor was refused at exhaustion")
	}
	if sequence.sendContractAcked {
		t.Fatal("an unacknowledged successor was treated as acknowledged")
	}
	if count := client.SendRecoveryStats().ContractAheadUnacknowledgedSwitchCount; count != 1 {
		t.Errorf("the unacknowledged switch was counted %d times", count)
	}
}

// The root cause, pinned: contract acquisition happens on the send path, so a
// sequence that exhausts a contract stops until the platform answers. This row
// asserts the absence of that acquisition rather than the presence of a flag.
//
// Counted, not timed. The wait a sequence pays at exhaustion is a call to
// `TakeContract` from `nextContract`, which is the only place the send path
// acquires a contract, and `beforeTakeContractForTest` fires there and nowhere
// else. So "the send path never waits" is exactly "that hook never fires", and
// this row reads it in both arms:
//
//   - with a successor announced ahead, the same debit that used to acquire
//     goes through with zero acquisitions and nothing promoted;
//   - with no successor announced, which is this tree at the same point and
//     every tree before it, the identical debit goes to acquisition.
//
// The second arm is the fail-before: delete the ahead path and the first arm
// becomes the second. The acquisition timeout is zero so the arm that does
// acquire fails immediately rather than waiting on a platform this fixture
// does not have — the row is about which path is taken, not how long it takes.
func TestTheAnnouncedContractRemovesTheAcquisitionFromTheSendPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const messageByteCount = ByteCount(1024)
	run := func(announced bool) (updated bool, takeCount int, promoted bool) {
		client, sequence, destinationId, contract := newSendNoContractHarness(t, ctx)
		sequence.sendBuffer = client.sendBuffer
		// nothing left in the current contract: the next message is the one
		// that used to stop the sequence
		contract.ackedByteCount = contract.effectiveTransferByteCount
		// an acquisition that fails at once, so the arm that acquires is
		// decided by the path it takes rather than by a timeout
		sequence.sendBufferSettings.CreateContractTimeout = 0
		takes := 0
		client.sendBuffer.beforeTakeContractForTest = func(sendSequenceId) {
			takes += 1
		}
		t.Cleanup(func() { client.sendBuffer.beforeTakeContractForTest = nil })

		if announced {
			ahead := newContractAheadTestContract(t, client, destinationId)
			ahead.acknowledgedAhead = true
			sequence.openSendContracts[ahead.contractId] = ahead
			sequence.aheadSendContract = ahead
			sequence.aheadSendContractMetadataGeneration = sequence.contractMetadata().generation
		}

		updated, _, _ = sequence.updateContractWithAckPromotion(messageByteCount, true)
		return updated, takes, sequence.sendContractAcked
	}

	updated, takeCount, promoted := run(true)
	if !updated {
		t.Fatal("a sequence with an announced successor could not carry the message that exhausted its contract")
	}
	if takeCount != 0 {
		t.Errorf(
			"the send path acquired a contract %d times with a successor already announced and acknowledged; the acquisition is the stall this change removes",
			takeCount,
		)
	}
	if !promoted {
		t.Error("the switched contract is unacknowledged, so every no-acknowledgement Pack is promoted until an opening round trip completes")
	}

	// the same debit with nothing announced: this tree's own behaviour at the
	// point the change acts on, and the shape of a tree without it
	updatedWithout, takeCountWithout, _ := run(false)
	if takeCountWithout != 1 {
		t.Errorf(
			"the send path acquired a contract %d times with nothing announced, want exactly one; if this is zero the row's two arms are the same and it proves nothing",
			takeCountWithout,
		)
	}
	if updatedWithout {
		t.Error("the fixture's contract queue produced a contract, so the arm that was supposed to stall did not")
	}
}

// The acknowledgement callback, which is what the switch reads. A failed
// announcement must leave the flag clear: the successor is then switched to
// unacknowledged and promotes, which is today's behaviour rather than a
// sequence that believes a receiver has a contract it never got.
func TestTheContractAheadAcknowledgementMarksTheSuccessor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, sequence, destinationId, _ := newSendNoContractHarness(t, ctx)
	ahead := newContractAheadTestContract(t, client, destinationId)

	callback := sequence.contractAheadAckCallback(ahead)
	callback(errors.New("the announcement was never acknowledged"))
	if ahead.acknowledgedAhead {
		t.Fatal("a failed announcement marked the successor acknowledged")
	}
	callback(nil)
	if !ahead.acknowledgedAhead {
		t.Fatal("an acknowledged announcement did not mark the successor")
	}
	if count := client.SendRecoveryStats().ContractAheadAcknowledgedCount; count != 1 {
		t.Errorf("the acknowledgement was counted %d times", count)
	}
}

// The announcement on the wire: `contract_ahead` set, the successor's contract
// frame, no data frames, and not a head, because a head carries the current
// contract's frame and this one carries the successor's.
func TestTheContractAheadAnnouncementCarriesTheSuccessorAndNoData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, sequence, destinationId, _ := newSendNoContractHarness(t, ctx)
	sequence.sendBuffer = client.sendBuffer
	route := make(chan []byte, 1)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)

	ahead := newContractAheadTestContract(t, client, destinationId)
	sequence.sendContractAheadAnnouncement(ahead, func(error) {})

	select {
	case transferFrameBytes := <-route:
		pack := decodeCompactContractTestPack(t, transferFrameBytes)
		MessagePoolReturn(transferFrameBytes)
		if !pack.GetContractAhead() {
			t.Error("the announcement did not set contract_ahead, so the receiver would switch to the successor immediately and charge the current contract's Packs to it")
		}
		if pack.GetHead() {
			t.Error("the announcement was sent as a head, which carries the current contract rather than the successor")
		}
		if 0 < len(pack.GetFrames()) {
			t.Errorf("the announcement carried %d data frames; it takes one admission slot and no data", len(pack.GetFrames()))
		}
		contractFrame := pack.GetContractFrame()
		if contractFrame == nil {
			t.Fatal("the announcement carried no contract frame")
		}
		announced := &protocol.Contract{}
		if err := ProtoUnmarshal(contractFrame.MessageBytes, announced); err != nil {
			t.Fatalf("decode the announced contract: %v", err)
		}
		if string(announced.StoredContractBytes) != string(ahead.contract.StoredContractBytes) {
			t.Error("the announcement carried a contract that is not the successor")
		}
	case <-ctx.Done():
		t.Fatal("the announcement was never written")
	}
}

// The receive side, which is one method: verify exactly as an opening contract
// is verified, store, and switch nothing. Switching on the announcement would
// charge every Pack still in flight under the old contract to the new one,
// because an acknowledged Pack carries no contract id and resolves to whatever
// the sequence's current contract is.
func TestAnAnnouncedContractIsStoredWithoutBecomingCurrent(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	settings := DefaultClientSettings()
	receiver := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	t.Cleanup(receiver.Cancel)
	receiver.ContractManager().SetProvideModesWithReturnTraffic(
		map[protocol.ProvideMode]bool{
			protocol.ProvideMode_Network: true,
		},
	)
	sourceId := NewId()
	sequence := NewReceiveSequence(
		ctx,
		receiver,
		SourceId(sourceId),
		NewId(),
		sequenceTlsRoleClient,
		false,
		DefaultReceiveBufferSettings(),
	)
	t.Cleanup(sequence.Close)
	// Run would create this; these rows drive the sequence directly, and the
	// contract paths report to it on every outcome.
	sequence.peerAudit = NewSequencePeerAudit(receiver, SourceId(sourceId), 0)

	current := contractAheadTestContractFrame(t, receiver, sourceId, receiver.ClientId())
	successor := contractAheadTestContractFrame(t, receiver, sourceId, receiver.ClientId())

	// the contract in force, by the path that is in force today
	if err := sequence.registerContracts(&receiveItem{contractFrame: current}); err != nil {
		t.Fatalf("register the current contract: %v", err)
	}
	inForce := sequence.receiveContract
	if inForce == nil {
		t.Fatal("the current contract did not become current")
	}

	// the announcement
	if err := sequence.registerContracts(&receiveItem{
		contractFrame: successor,
		contractAhead: true,
	}); err != nil {
		t.Fatalf("register the announced successor: %v", err)
	}
	if sequence.receiveContract != inForce {
		t.Fatal("the announcement switched the receiver, so every Pack still in flight under the current contract would be charged to the successor")
	}
	if len(sequence.openReceiveContracts) != 2 {
		t.Fatalf(
			"the receiver holds %d contracts across the announcement, want both",
			len(sequence.openReceiveContracts),
		)
	}
	successorId := contractAheadTestContractId(t, successor)
	stored, ok := sequence.openReceiveContracts[successorId]
	if !ok {
		t.Fatal("the announced successor was not stored, so the first Pack under it would be refused")
	}

	// a Pack with no contract id is charged to the contract in force, which is
	// the property the delay exists for
	item := &receiveItem{transferItem: transferItem{messageByteCount: 1024}}
	if !sequence.updateContract(item) {
		t.Fatal("a Pack without a contract id was refused while a contract was in force")
	}
	if item.contractId == nil || *item.contractId != inForce.contractId {
		t.Fatal("a Pack without a contract id was charged to the announced successor rather than to the contract in force")
	}

	// and the switch happens where it happens today: the successor's frame
	// arrives again on the first Pack under it, and the early path finds it
	if err := sequence.registerContracts(&receiveItem{contractFrame: successor}); err != nil {
		t.Fatalf("switch to the announced successor: %v", err)
	}
	if sequence.receiveContract != stored {
		t.Fatal("the re-registered successor did not become current")
	}
	if len(sequence.openReceiveContracts) != 2 {
		t.Fatalf(
			"the switch changed the open contract count to %d; re-registering an id the receiver already holds has to be idempotent",
			len(sequence.openReceiveContracts),
		)
	}
}

// One acknowledgement through the production marshaller and decoder, so a row
// reads the capability the way the receive path reads it rather than by
// assigning a field.
func decodeContractAheadTestAck(t *testing.T, contractAhead bool, selective bool) receiveAckMessage {
	t.Helper()
	saf := sendAckFrame{
		path:          DestinationId(NewId()).AddSource(NewId()),
		messageId:     NewId(),
		sequenceId:    NewId(),
		selective:     selective,
		contractAhead: contractAhead,
	}
	frameBytes := marshalSendAckTransferFrame(&saf)
	defer MessagePoolReturn(frameBytes)
	frame := &protocol.TransferFrame{}
	if !unmarshalTransferFrame(frameBytes, frame, true) {
		t.Fatal("the acknowledgement frame did not decode")
	}
	ack, err := receiveAckMessageFromProtocol(frame.GetAck())
	if err != nil {
		t.Fatal(err)
	}
	return ack
}

// a receive sequence driven directly, with the audit Run would have created
func newContractAheadTestReceiveSequence(
	t *testing.T,
	ctx context.Context,
) (*Client, *ReceiveSequence, Id) {
	t.Helper()
	receiver := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	t.Cleanup(receiver.Cancel)
	receiver.ContractManager().SetProvideModesWithReturnTraffic(
		map[protocol.ProvideMode]bool{
			protocol.ProvideMode_Network: true,
		},
	)
	sourceId := NewId()
	sequence := NewReceiveSequence(
		ctx,
		receiver,
		SourceId(sourceId),
		NewId(),
		sequenceTlsRoleClient,
		false,
		DefaultReceiveBufferSettings(),
	)
	t.Cleanup(sequence.Close)
	sequence.peerAudit = NewSequencePeerAudit(receiver, SourceId(sourceId), 0)
	return receiver, sequence, sourceId
}

// a send sequence with the rule's instruments attached, for the pure threshold
// rows; the same shape windowEstimateForSettings uses
func newContractAheadTestSequence(t *testing.T, settings *SendBufferSettings) *SendSequence {
	t.Helper()
	return &SendSequence{
		sendBufferSettings: settings,
		deliveredBytes:     make([]deliveredBytesSample, deliveredBytesRingSize),
		resendQueue:        newResendQueue(nil, settings.ResendQueueMinByteCount),
		rttWindow: NewRttWindow(
			NewNoopLogger(),
			settings.RttWindowSize,
			settings.RttWindowTimeout,
			settings.RttScale,
			settings.MinResendInterval,
			settings.RttMinResendInterval,
			settings.MaxResendInterval,
		),
	}
}

// One round trip sample, stamped rather than timed: the window reads the
// difference between the two times it is handed, so nothing here waits.
func addContractAheadRoundTripSample(sequence *SendSequence, roundTrip time.Duration) {
	sendTime := time.Now().Add(-roundTrip)
	tag := sequence.rttWindow.openTag(sendTime)
	sequence.rttWindow.closeTag(tag, sendTime.Add(roundTrip))
}

// Two delivery samples `span` apart carrying `byteCount` between them, which is
// what `deliveredRate` reads over that span. Both are stamped, so the rate is a
// computed value rather than an observation of how fast this machine ran.
func addContractAheadDeliverySamples(
	sequence *SendSequence,
	byteCount ByteCount,
	span time.Duration,
) {
	start := time.Now().Add(-span)
	sequence.observeDeliveredBytes(1, start)
	sequence.observeDeliveredBytes(byteCount, start.Add(span))
}

// a verified contract for this client, as the platform would sign it
func contractAheadTestContractFrame(
	t *testing.T,
	client *Client,
	sourceId Id,
	destinationId Id,
) *protocol.Frame {
	t.Helper()
	storedContract := &protocol.StoredContract{
		ContractId:        NewId().Bytes(),
		TransferByteCount: uint64(mib(1)),
		SourceId:          sourceId.Bytes(),
		DestinationId:     destinationId.Bytes(),
	}
	storedContractBytes, err := ProtoMarshal(storedContract)
	if err != nil {
		t.Fatal(err)
	}
	contractManager := client.ContractManager()
	provideSecretKey, ok := contractManager.GetProvideSecretKey(protocol.ProvideMode_Network)
	if !ok {
		t.Fatal("the receiver has no Network provide secret key to verify a contract against")
	}
	contract := &protocol.Contract{
		StoredContractBytes: storedContractBytes,
		StoredContractHmac: SignStoredContract(
			contractManager.settings,
			provideSecretKey,
			storedContractBytes,
		),
		ProvideMode: protocol.ProvideMode_Network,
	}
	contractBytes, err := ProtoMarshal(contract)
	if err != nil {
		t.Fatal(err)
	}
	return &protocol.Frame{
		MessageType:  protocol.MessageType_TransferContract,
		MessageBytes: contractBytes,
	}
}

func contractAheadTestContractId(t *testing.T, contractFrame *protocol.Frame) Id {
	t.Helper()
	contract := &protocol.Contract{}
	if err := ProtoUnmarshal(contractFrame.MessageBytes, contract); err != nil {
		t.Fatal(err)
	}
	storedContract := &protocol.StoredContract{}
	if err := ProtoUnmarshal(contract.StoredContractBytes, storedContract); err != nil {
		t.Fatal(err)
	}
	contractId, err := IdFromBytes(storedContract.ContractId)
	if err != nil {
		t.Fatal(err)
	}
	return contractId
}

// a successor the send side can switch to, accounted the way a taken contract
// is accounted
func newContractAheadTestContract(t *testing.T, client *Client, destinationId Id) *sequenceContract {
	t.Helper()
	return &sequenceContract{
		log:                        client.log,
		localId:                    NewId(),
		tag:                        "s",
		contractId:                 NewId(),
		contract:                   &protocol.Contract{StoredContractBytes: NewId().Bytes()},
		transferByteCount:          mib(1),
		effectiveTransferByteCount: mib(1),
		path: TransferPath{
			SourceId:      client.ClientId(),
			DestinationId: destinationId,
		},
	}
}
