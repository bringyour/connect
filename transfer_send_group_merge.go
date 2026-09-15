// Merges already-queued single-chunk logical groups into one H1 wire Pack.
//
// Every upstream IP packet a client forwards is admitted as a one-element
// logical group, and `processLogicalGroupChunk` materializes each group as
// its own wire Pack, so a download's kernel TCP ACKs reach the relay as one
// websocket message each and relay work scales with message count rather
// than with bytes. The ready-drain loop in `SendSequence.Run` already folds
// independently queued non-group Packs into one Pack on H1; this is the same
// fold for groups. On a relay rig it cut upstream websocket messages by about
// 70 per cent and raised eight-flow throughput by 13 per cent.
//
// Only the H1-only, non-flow-isolated policy is touched, and only groups the
// unmerged path would already write as exactly one Pack. There is no batching
// wait: candidates are taken non-blocking from what is already queued, so a
// sparse ACK keeps its latency. Every other policy and shape goes through the
// unchanged `processLogicalGroupChunk`.
package connect

import (
	"github.com/urnetwork/connect/protocol"
)

// Called from `SendSequence.Run` in place of `processLogicalGroupChunk` when
// `SendBufferSettings.MergeReadyLogicalGroups` is on and the current Pack was
// selected through ordinary admission. It handles the Pack only when the
// whole logical group would be materialized as one wire Pack (the
// `groupCompletion == nil && end == len(Frames)` branch of
// `processLogicalGroupChunk`) and at least one compatible already-queued group
// can be merged in. Otherwise it returns handled=false with no state changed
// beyond what `processLogicalGroupChunk` itself does on entry (writer open and
// chunk-limit pinning), so the caller falls through to the unchanged code.
// A candidate it could not merge is pushed back to the scheduler front,
// exactly as the ready-drain loop does.
//
// Admission: the loop selects the current Pack only when the resend queue has
// window capacity, and it writes one item per iteration under that check.
// The ready-drain loop already writes up to sixteen queued Packs as one item
// behind that same single check; this path adds nothing beyond it, and it is
// skipped entirely for a Pack that bypassed recovery admission. Contract: one
// debit for the combined byte count through the same `updateContractOutcome`
// the unmerged path uses, so the contract-ahead announcement and no-ack
// promotion decisions are unchanged.
//
// Completion: each merged Pack's own ack and no-ack records go into the
// record sets of the single send item, so every AckCallback, NoAck observer
// and lifecycle observer fires exactly once, when that one wire item is
// acknowledged, written, or failed. No `sendGroupCompletion` is created: a
// merged group is a single chunk, the same as the unmerged one-chunk case.
//
// `processingPacks` is the loop's abnormal-exit disposal slate: every Pack
// taken here is recorded in it while owned, and cleared once its frames have
// been serialized or disposed.
func (self *SendSequence) processMergedLogicalGroups(
	sendPack *SendPack,
	scheduler *sendPackScheduler,
	flightPolicy transferFlightPolicySnapshot,
	packsClosed *bool,
	processingPacks []*SendPack,
) (handled bool, success bool) {
	if !flightPolicy.h1Only || flightPolicy.flowIsolation {
		return false, false
	}
	// same entry prelude as processLogicalGroupChunk (idempotent)
	if self.contractMultiRouteWriter == nil && self.sendBuffer != nil {
		self.openContractMultiRouteWriter()
		flightPolicy = self.transferFlightPolicy()
		if !flightPolicy.h1Only || flightPolicy.flowIsolation {
			return false, false
		}
	}
	self.pinLogicalGroupChunkLimits(sendPack, flightPolicy)
	if sendPack.groupCompletion != nil || sendPack.groupFrameIndex != 0 {
		return false, false
	}
	groupMaxFrames, groupMaxMessageByteCount := sendPack.groupChunkLimits()
	end := nextSendGroupChunkEndWithLimits(
		sendPack.Frames,
		0,
		groupMaxFrames,
		groupMaxMessageByteCount,
	)
	if end != len(sendPack.Frames) {
		// a partial chunk needs a sendGroupCompletion; leave it alone
		return false, false
	}

	maxFrames, maxMessageByteCount := self.readyDrainChunkLimits(flightPolicy)
	frameCount := len(sendPack.Frames)
	messageByteCount := MessageByteCount(sendPack.Frames)
	if maxFrames < frameCount || maxMessageByteCount < messageByteCount {
		return false, false
	}
	if len(processingPacks) < maxFrames {
		maxFrames = len(processingPacks)
	}

	var sendPackValues [sendPackH1GroupMaxFrames]*SendPack
	sendPacks := sendPackValues[:0]
	sendPacks = append(sendPacks, sendPack)

	// Drain only what is already queued; never wait. This mirrors the
	// ready-drain loop's take order: scheduler FIFO first, then a non-blocking
	// receive from the ingress channel.
	for len(sendPacks) < len(sendPackValues) && frameCount < maxFrames {
		nextSendPack := scheduler.TakeFifoEligible(func(*SendPack) bool {
			return true
		})
		if nextSendPack == nil && !*packsClosed {
			select {
			case queuedSendPack, ok := <-self.packs:
				if !ok {
					*packsClosed = true
				} else {
					nextSendPack = queuedSendPack
				}
			default:
			}
		}
		if nextSendPack == nil {
			break
		}
		processingPacks[len(sendPacks)] = nextSendPack
		nextFrameCount := len(nextSendPack.frameList())
		nextMessageByteCount := messageByteCount + nextSendPack.serializedMessageByteCount()
		contractSafe := self.client.ContractManager().SendNoContract(self.destination) ||
			(self.sendContract != nil &&
				self.sendContractAcked &&
				0 < len(self.sendItems) &&
				self.sendContract.canUpdate(nextMessageByteCount))
		// only an untouched whole group: a group with a cursor or a completion
		// already owns wire items of its own
		compatible := nextSendPack.logicalGroup &&
			nextSendPack.groupFrameIndex == 0 &&
			nextSendPack.groupCompletion == nil &&
			0 < nextFrameCount &&
			sendPack.Ack == nextSendPack.Ack &&
			sendPack.ForceUnwrapped == nextSendPack.ForceUnwrapped &&
			frameCount+nextFrameCount <= maxFrames &&
			nextMessageByteCount <= maxMessageByteCount &&
			contractSafe
		if !compatible {
			scheduler.PushFront(nextSendPack)
			processingPacks[len(sendPacks)] = nil
			break
		}
		sendPacks = append(sendPacks, nextSendPack)
		frameCount += nextFrameCount
		messageByteCount = nextMessageByteCount
	}

	if len(sendPacks) == 1 {
		// nothing to merge: the unchanged single-group code handles this Pack
		return false, false
	}

	// Pin the merged Packs' chunk limits too, so a contract failure below
	// disposes each one with the same bounds the unmerged path would use.
	for _, mergedSendPack := range sendPacks[1:] {
		self.pinLogicalGroupChunkLimits(mergedSendPack, flightPolicy)
	}

	contractUpdated, contractErr := self.updateContractOutcome(messageByteCount)
	if !contractUpdated {
		err := self.classifyContractCreationFailure(contractErr)
		for packIndex, mergedSendPack := range sendPacks {
			mergedSendPack.disposeUnsentGroup(err)
			processingPacks[packIndex] = nil
		}
		return true, false
	}

	var frameValues [sendPackH1GroupMaxFrames]*protocol.Frame
	frames := frameValues[:0]
	var acks sendAckSet
	var noAckSends noAckSendSet
	for _, mergedSendPack := range sendPacks {
		frames = append(frames, mergedSendPack.Frames...)
		acks.add(mergedSendPack.ackRecord())
		noAckSends.add(mergedSendPack.noAckRecord())
	}
	// A merged Pack spans flows; like the ready-drain path, it carries no exact
	// flow identity.
	self.sendRecordsForSchedulingKey(
		frames,
		acks,
		noAckSends,
		sendPack.Ack,
		sendPack.ForceUnwrapped,
		sendSchedulingKey{},
	)
	if self.client != nil {
		self.client.mergedLogicalGroupWriteCount.Add(1)
		self.client.mergedLogicalGroupCount.Add(uint64(len(sendPacks)))
	}
	for packIndex, mergedSendPack := range sendPacks {
		mergedSendPack.groupFrameIndex = len(mergedSendPack.Frames)
		mergedSendPack.releaseRaw()
		processingPacks[packIndex] = nil
	}
	return true, true
}
