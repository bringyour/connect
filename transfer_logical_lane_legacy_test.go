package connect

import (
	"context"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// `Ack.logical_lane_version` (field 7), the half of its stated contract that
// nothing asserted.
//
// The proto says: "The sender scopes this evidence to the live lane-zero
// SendSequence and retires every nonzero lane when that sequence closes or a
// later delivery Ack omits the capability." Two retirement triggers, one
// sentence. `TestLogicalLaneCapabilityRequiresLiveLaneZero` pins the second -
// a later Ack omitting the capability withdraws it and cancels the dependent
// lanes - and pins the never-negotiated case beside it. The first trigger, the
// lane-zero sequence CLOSING, has no row, and it is the one that fires in
// ordinary operation: a route change, an idle timeout on a quiet lane zero, a
// peer restart behind the same address.
//
// Why it must retire rather than persist. The capability is evidence about one
// peer generation, learned on one sequence. When that sequence closes the
// sender no longer knows what is behind the address: a restarted peer at an
// older build reads `Pack.logical_lane` as nothing, holds one head slot for
// the whole class, and the lanes that were safe a moment ago are now
// alternating traffic through a single slot - the lossy arrangement lanes were
// added to remove. Keeping a stale capability across a sequence close is
// therefore not a small optimism; it is the pre-lane defect, reintroduced at
// exactly the moment the peer might have changed.
//
// Deterministic: the sequences are constructed and registered directly, the
// close is called directly, and the only wait is on a context the close itself
// cancels, bounded so a failure reports rather than hangs.
func TestClosingLaneZeroRetiresEveryNegotiatedLane(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	settings.SendBufferSettings.LogicalDataLaneCount = 8
	applyTestEncryptionSettings(settings, encryptionModeOff)
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	destination := NewId()
	pack := &SendPack{
		TransferOptions: settings.DefaultTransferOpts,
		Destination:     destination,
		schedulingKey:   testLogicalLaneSchedulingKey(54321),
	}

	laneZero := NewSendSequence(
		ctx,
		client,
		client.sendBuffer,
		destination,
		MultiHopId{},
		false,
		false,
		false,
		sequenceTlsRoleClient,
		false,
		settings.SendBufferSettings,
	)
	base := laneZero.id()
	client.sendBuffer.mutex.Lock()
	client.sendBuffer.sendSequences[base] = laneZero
	client.sendBuffer.mutex.Unlock()
	client.sendBuffer.observeLogicalLaneVersion(laneZero, transferLogicalLaneVersion)

	wantLane := pack.schedulingKey.logicalLaneForCount(8)
	if got := client.sendBuffer.selectLogicalLane(pack); got != wantLane {
		t.Fatalf("the capability did not negotiate: lane %d, want %d", got, wantLane)
	}

	dataLane := newSendSequenceWithLogicalLane(
		ctx,
		client,
		client.sendBuffer,
		destination,
		MultiHopId{},
		false,
		false,
		false,
		sequenceTlsRoleClient,
		false,
		wantLane,
		settings.SendBufferSettings,
		NewTransferMemoryBudget(settings.SendBufferSettings.ResendQueueMaxByteCount),
	)
	client.sendBuffer.mutex.Lock()
	client.sendBuffer.sendSequences[dataLane.id()] = dataLane
	client.sendBuffer.mutex.Unlock()

	// the lane-zero sequence the evidence was scoped to closes: an idle
	// timeout, a route change, or a peer that went away and came back
	client.sendBuffer.closeSendSequence(base, base.wireId(), laneZero)

	if got := client.sendBuffer.selectLogicalLane(pack); got != 0 {
		t.Errorf(
			"a new flow selected lane %d after the lane-zero sequence the capability was learned on closed. The evidence is scoped to that sequence: once it is gone the sender does not know what is behind the address, and a restarted peer at an older build holds one head slot for the whole class",
			got,
		)
	}
	select {
	case <-dataLane.ctx.Done():
	case <-time.After(5 * time.Second):
		t.Error("closing the lane-zero sequence did not retire the live data lane. A lane whose capability evidence has gone is alternating traffic through the single head slot a pre-lane peer keeps, which is the arrangement lanes were added to remove")
	}

	client.sendBuffer.mutex.Lock()
	_, versionHeld := client.sendBuffer.logicalLaneVersions[base]
	client.sendBuffer.mutex.Unlock()
	if versionHeld {
		t.Error("the negotiated version survived the close of the sequence it was scoped to")
	}

	client.sendBuffer.mutex.Lock()
	delete(client.sendBuffer.sendSequences, dataLane.id())
	client.sendBuffer.mutex.Unlock()
	dataLane.Cancel()
}

// The wire half of the downgrade, which the existing row does not reach.
//
// `TestLogicalLaneCapabilityRequiresLiveLaneZero` drives the mid-session
// withdrawal by calling `observeLogicalLaneVersion(laneZero, 0)` directly, so
// it asserts what the sender does with a zero and says nothing about where a
// zero comes from. The link it leaves open is the one that carries the
// compatibility: `logical_lane_version` is a plain proto3 `uint32`, not an
// optional, so a peer that predates the field does not omit a value - it sends
// a message from which the field decodes as zero. Absent and zero are the same
// wire fact here, which is the OPPOSITE of `receive_window_byte_count` two
// fields along, where the optional makes them different and a live protocol
// case depends on telling them apart.
//
// That asymmetry between two adjacent capability bits is worth an assertion of
// its own. A field changed from `uint32` to `optional uint32`, or read through
// a decoder that treats a missing scalar as unset rather than as zero, would
// leave a legacy peer looking like a peer that said nothing - and "said
// nothing" is not a case this consumer has, so the downgrade would simply
// stop happening.
func TestAnAcknowledgementWithoutTheLaneVersionDecodesAsALegacyPeer(t *testing.T) {
	messageId := NewId()
	sequenceId := NewId()

	legacyAck := &protocol.Ack{
		MessageId:  messageId.Bytes(),
		SequenceId: sequenceId.Bytes(),
	}
	legacy, err := receiveAckMessageFromProtocol(legacyAck)
	if err != nil {
		t.Fatalf("decoding an acknowledgement with no lane version: %s", err)
	}
	if legacy.logicalLaneVersion != 0 {
		t.Errorf(
			"an acknowledgement carrying no lane version decoded as version %d. The field is a plain uint32, so a peer that predates it produces a zero, and zero is the only signal the sender has that it is talking to a legacy peer",
			legacy.logicalLaneVersion,
		)
	}

	modernAck := &protocol.Ack{
		MessageId:          messageId.Bytes(),
		SequenceId:         sequenceId.Bytes(),
		LogicalLaneVersion: transferLogicalLaneVersion,
	}
	modern, err := receiveAckMessageFromProtocol(modernAck)
	if err != nil {
		t.Fatalf("decoding an acknowledgement with a lane version: %s", err)
	}
	if modern.logicalLaneVersion != transferLogicalLaneVersion {
		t.Errorf(
			"an acknowledgement advertising version %d decoded as %d",
			transferLogicalLaneVersion,
			modern.logicalLaneVersion,
		)
	}
	if transferLogicalLaneVersion == 0 {
		t.Fatal("the shipping lane version is zero, so an advertising peer is indistinguishable from a legacy one and this row can prove nothing")
	}

	// and the decoded legacy value is what withdraws the capability, which is
	// the link between the wire and the retirement the existing row asserts
	// from a literal
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientSettings()
	settings.Log = NewNoopLogger()
	settings.SendBufferSettings.LogicalDataLaneCount = 8
	applyTestEncryptionSettings(settings, encryptionModeOff)
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer client.Cancel()

	destination := NewId()
	pack := &SendPack{
		TransferOptions: settings.DefaultTransferOpts,
		Destination:     destination,
		schedulingKey:   testLogicalLaneSchedulingKey(54321),
	}
	laneZero := NewSendSequence(
		ctx,
		client,
		client.sendBuffer,
		destination,
		MultiHopId{},
		false,
		false,
		false,
		sequenceTlsRoleClient,
		false,
		settings.SendBufferSettings,
	)
	baseId := laneZero.id()
	client.sendBuffer.mutex.Lock()
	client.sendBuffer.sendSequences[baseId] = laneZero
	client.sendBuffer.mutex.Unlock()

	client.sendBuffer.observeLogicalLaneVersion(laneZero, modern.logicalLaneVersion)
	wantLane := pack.schedulingKey.logicalLaneForCount(8)
	if got := client.sendBuffer.selectLogicalLane(pack); got != wantLane {
		t.Fatalf("a decoded advertising acknowledgement did not negotiate: lane %d, want %d", got, wantLane)
	}
	client.sendBuffer.observeLogicalLaneVersion(laneZero, legacy.logicalLaneVersion)
	if got := client.sendBuffer.selectLogicalLane(pack); got != 0 {
		t.Errorf(
			"a decoded legacy acknowledgement left the sender on lane %d. The value that withdraws the capability has to be the value a legacy peer's acknowledgement actually decodes to, or the downgrade is asserted against a literal no peer sends",
			got,
		)
	}

	client.sendBuffer.mutex.Lock()
	delete(client.sendBuffer.sendSequences, baseId)
	client.sendBuffer.mutex.Unlock()
	laneZero.Cancel()
}
