// Tests for transfer_send_group_merge.go: already-queued single-frame logical
// groups fold into one H1 wire Pack, in order, with every completion exactly
// once, and the fold stops at each compatibility and size bound.
package connect

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

const readyLogicalGroupMergeTestPackCount = 64

// One queued burst of single-frame logical groups on a held H1-only sequence.
type readyLogicalGroupMergeBurst struct {
	merge          bool
	packCount      int
	frameByteCount int
	// per-Pack request policy; every Pack acknowledged or none is what makes
	// the callback order assertion meaningful
	ack            func(index int) bool
	forceUnwrapped func(index int) bool
}

// Admits one single-frame logical group. The ForceUnwrapped shape has no
// public option, so it is built the way sendGroupToWithTimeoutDetailed builds
// its Pack and admitted through the same entry.
func admitReadyLogicalGroupForTest(
	client *Client,
	frame *protocol.Frame,
	destinationId Id,
	ack bool,
	forceUnwrapped bool,
	ackCallback AckFunction,
) (bool, error) {
	opts := []any{}
	if !ack {
		opts = append(opts, NoAck())
	}
	if !forceUnwrapped {
		return client.sendGroupWithTimeoutDetailed(
			[]*protocol.Frame{frame},
			destinationId,
			ackCallback,
			time.Second,
			opts...,
		)
	}
	resolved := client.resolveSendOptions(opts)
	frames := []*protocol.Frame{frame}
	sendPack := &SendPack{
		TransferOptions:              resolved.transferOptions,
		Frames:                       frames,
		logicalGroup:                 true,
		Destination:                  destinationId,
		AckCallback:                  ackCallback,
		ackTarget:                    resolved.ackTarget,
		MessageByteCount:             MessageByteCount(frames),
		Ctx:                          resolved.ctx,
		ForceUnwrapped:               true,
		EncryptionRole:               resolved.encryptionRole,
		EncryptionCompanion:          resolved.encryptionCompanion,
		transportWriteObserver:       resolved.transportWriteObserver,
		schedulingKey:                resolved.schedulingKey,
		logicalLane:                  resolved.logicalLane,
		logicalLaneExplicit:          resolved.logicalLaneExplicit,
		lifecycleUpstreamRecoverable: resolved.upstreamRecoverable,
		retainAfterAckTimeout:        resolved.retainAfterAckTimeout,
	}
	return client.enqueueSendPack(sendPack, time.Second)
}

// Queues the burst while the SendSequence is held before Run, releases it,
// and returns the frame count of every wire item in wire order plus the
// client's merge counters. It asserts in-order delivery of every frame,
// exactly-once callbacks (in order when the burst shares one ack policy), and
// that every pool witness is released.
func runReadyLogicalGroupMergeBurst(
	t *testing.T,
	burst readyLogicalGroupMergeBurst,
) (wireFrameCounts []int, mergedWriteCount uint64, mergedGroupCount uint64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	destinationId := NewId()
	sequenceEntered := make(chan struct{})
	releaseSequence := make(chan struct{})
	var sequenceEnteredOnce sync.Once
	var releaseSequenceOnce sync.Once
	release := func() { releaseSequenceOnce.Do(func() { close(releaseSequence) }) }

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.MergeReadyLogicalGroups = burst.merge
	settings.SendBufferSettings.SequenceBufferSize = 4 * burst.packCount
	settings.SendBufferSettings.AckBufferSize = 4 * burst.packCount
	settings.SendBufferSettings.beforeRunSendSequenceForTest = func(id sendSequenceId) {
		if id.Destination != destinationId {
			return
		}
		sequenceEnteredOnce.Do(func() { close(sequenceEntered) })
		<-releaseSequence
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer func() {
		release()
		closeTransferGroupTestClient(t, client)
	}()
	client.ContractManager().AddNoContractPeer(destinationId)
	route := make(chan []byte, 4*burst.packCount)
	client.RouteManager().UpdateTransport(
		&h1SendClientTransportForGroupTest{
			sendClientTransport: NewSendClientTransport(DestinationId(destinationId)),
		},
		[]Route{route},
	)

	frames, witnesses := transferGroupTestFrames(t, burst.packCount, burst.frameByteCount)
	callbackCounts := make([]atomic.Int32, len(frames))
	results := make(chan int, 4*len(frames))
	uniformAck := true
	for index, frame := range frames {
		index := index
		ack := burst.ack(index)
		if ack != burst.ack(0) {
			uniformAck = false
		}
		success, err := admitReadyLogicalGroupForTest(
			client,
			frame,
			destinationId,
			ack,
			burst.forceUnwrapped(index),
			func(err error) {
				callbackCounts[index].Add(1)
				if err != nil {
					results <- -index - 1
					return
				}
				results <- index
			},
		)
		if !success || err != nil {
			for frameIndex := index; frameIndex < len(frames); frameIndex++ {
				MessagePoolReturn(frames[frameIndex].MessageBytes)
			}
			for _, witness := range witnesses {
				MessagePoolReturn(witness)
			}
			t.Fatalf("logical group %d admission success=%t err=%v", index, success, err)
		}
		if index == 0 {
			select {
			case <-sequenceEntered:
			case <-ctx.Done():
				t.Fatalf("wait for sequence startup: %v", ctx.Err())
			}
		}
	}
	release()

	// every frame arrives on the wire, in FIFO order, with its own ack policy
	seenFrames := 0
	for seenFrames < len(frames) {
		var transferFrameBytes []byte
		select {
		case transferFrameBytes = <-route:
		case <-ctx.Done():
			t.Fatalf(
				"wait for wire item (merge=%t seen=%d items=%d): %v",
				burst.merge, seenFrames, len(wireFrameCounts), ctx.Err(),
			)
		}
		pack := decodeSendPackLifecycleWirePack(t, transferFrameBytes)
		wireFrameCounts = append(wireFrameCounts, len(pack.Frames))
		if len(pack.Frames) == 0 {
			MessagePoolReturn(transferFrameBytes)
			t.Fatalf("wire item %d carried no frames", len(wireFrameCounts))
		}
		if sendPackH1GroupMaxFrames < len(pack.Frames) {
			MessagePoolReturn(transferFrameBytes)
			t.Fatalf("wire item %d carried %d frames > H1 limit", len(wireFrameCounts), len(pack.Frames))
		}
		for _, wireFrame := range pack.Frames {
			if len(frames) <= seenFrames {
				MessagePoolReturn(transferFrameBytes)
				t.Fatalf("wire carried more than %d frames", len(frames))
			}
			want := byte(seenFrames + 1)
			last := len(wireFrame.MessageBytes) - 1
			if len(wireFrame.MessageBytes) != burst.frameByteCount ||
				wireFrame.MessageBytes[0] != want || wireFrame.MessageBytes[last] != want {
				MessagePoolReturn(transferFrameBytes)
				t.Fatalf(
					"wire frame %d out of order: len=%d first=%d want=%d",
					seenFrames, len(wireFrame.MessageBytes), wireFrame.MessageBytes[0], want,
				)
			}
			if pack.Nack == burst.ack(seenFrames) {
				MessagePoolReturn(transferFrameBytes)
				t.Fatalf("wire frame %d nack=%t, want ack=%t", seenFrames, pack.Nack, burst.ack(seenFrames))
			}
			seenFrames += 1
		}
		if !pack.Nack {
			acknowledgeSendPackLifecycleWirePack(t, client, destinationId, pack)
		}
		MessagePoolReturn(transferFrameBytes)
	}

	// every callback fires exactly once, and in order when the burst shares
	// one ack policy (a no-ack callback fires on write, an ack callback on
	// the peer's acknowledgement, so a mixed burst has no single order)
	for want := range len(frames) {
		select {
		case got := <-results:
			if got < 0 {
				t.Fatalf("callback %d reported an error", -got-1)
			}
			if uniformAck && got != want {
				t.Fatalf("callback order: got %d want %d", got, want)
			}
		case <-ctx.Done():
			t.Fatalf("wait for callback %d: %v", want, ctx.Err())
		}
	}
	for index := range callbackCounts {
		if count := callbackCounts[index].Load(); count != 1 {
			t.Fatalf("callback %d fired %d times", index, count)
		}
	}
	if 0 < len(route) {
		t.Fatalf("%d unexpected extra wire items", len(route))
	}
	releaseTransferGroupTestWitnesses(t, frames, witnesses)
	stats := client.ReceiveStats()
	return wireFrameCounts, stats.MergedLogicalGroupWriteCount, stats.MergedLogicalGroupCount
}

func alwaysForTest(value bool) func(int) bool {
	return func(int) bool { return value }
}

func alternatingForTest() func(int) bool {
	return func(index int) bool { return index%2 == 1 }
}

func TestMergeReadyLogicalGroupsCoalescesQueuedNoAckGroups(t *testing.T) {
	wireFrameCounts, mergedWriteCount, mergedGroupCount := runReadyLogicalGroupMergeBurst(
		t,
		readyLogicalGroupMergeBurst{
			merge:          true,
			packCount:      readyLogicalGroupMergeTestPackCount,
			frameByteCount: 64,
			ack:            alwaysForTest(false),
			forceUnwrapped: alwaysForTest(false),
		},
	)
	// every group was queued before the sequence ran, so each item carries
	// the H1 frame limit
	wantItems := readyLogicalGroupMergeTestPackCount / sendPackH1GroupMaxFrames
	if len(wireFrameCounts) != wantItems {
		t.Fatalf("no-ack merge: wire items=%v, want %d of %d frames", wireFrameCounts, wantItems, sendPackH1GroupMaxFrames)
	}
	if mergedWriteCount != uint64(wantItems) ||
		mergedGroupCount != readyLogicalGroupMergeTestPackCount {
		t.Fatalf("merge counters items=%d groups=%d, want %d/%d", mergedWriteCount, mergedGroupCount, wantItems, readyLogicalGroupMergeTestPackCount)
	}
}

func TestMergeReadyLogicalGroupsCoalescesQueuedAckedGroups(t *testing.T) {
	wireFrameCounts, mergedWriteCount, _ := runReadyLogicalGroupMergeBurst(
		t,
		readyLogicalGroupMergeBurst{
			merge:          true,
			packCount:      readyLogicalGroupMergeTestPackCount,
			frameByteCount: 64,
			ack:            alwaysForTest(true),
			forceUnwrapped: alwaysForTest(false),
		},
	)
	wantItems := readyLogicalGroupMergeTestPackCount / sendPackH1GroupMaxFrames
	if len(wireFrameCounts) != wantItems {
		t.Fatalf("acked merge: wire items=%v, want %d of %d frames", wireFrameCounts, wantItems, sendPackH1GroupMaxFrames)
	}
	if mergedWriteCount != uint64(wantItems) {
		t.Fatalf("acked merge counter items=%d, want %d", mergedWriteCount, wantItems)
	}
}

// Off: the unchanged path emits one wire item per logical group, which is
// what makes the assertions above discriminating.
func TestMergeReadyLogicalGroupsOffWritesOnePackPerGroup(t *testing.T) {
	wireFrameCounts, mergedWriteCount, mergedGroupCount := runReadyLogicalGroupMergeBurst(
		t,
		readyLogicalGroupMergeBurst{
			merge:          false,
			packCount:      readyLogicalGroupMergeTestPackCount,
			frameByteCount: 64,
			ack:            alwaysForTest(false),
			forceUnwrapped: alwaysForTest(false),
		},
	)
	if len(wireFrameCounts) != readyLogicalGroupMergeTestPackCount {
		t.Fatalf("merge off: wire items=%v, want %d singles", wireFrameCounts, readyLogicalGroupMergeTestPackCount)
	}
	if mergedWriteCount != 0 || mergedGroupCount != 0 {
		t.Fatalf("merge off advanced the merge counters: items=%d groups=%d", mergedWriteCount, mergedGroupCount)
	}
}

// A group requesting acknowledgement and one that does not need different
// wire items: a no-ack completion is the write, an ack completion is the
// peer's acknowledgement. Alternating them leaves every group alone.
func TestMergeReadyLogicalGroupsKeepsAckAndNoAckApart(t *testing.T) {
	wireFrameCounts, mergedWriteCount, _ := runReadyLogicalGroupMergeBurst(
		t,
		readyLogicalGroupMergeBurst{
			merge:          true,
			packCount:      readyLogicalGroupMergeTestPackCount,
			frameByteCount: 64,
			ack:            alternatingForTest(),
			forceUnwrapped: alwaysForTest(false),
		},
	)
	if len(wireFrameCounts) != readyLogicalGroupMergeTestPackCount {
		t.Fatalf("mixed ack policy: wire items=%v, want %d singles", wireFrameCounts, readyLogicalGroupMergeTestPackCount)
	}
	if mergedWriteCount != 0 {
		t.Fatalf("mixed ack policy merged %d items", mergedWriteCount)
	}
}

// ForceUnwrapped pins an item to plaintext for its whole lifetime, so a
// group carrying it cannot share an item with one that does not.
func TestMergeReadyLogicalGroupsKeepsForceUnwrappedApart(t *testing.T) {
	wireFrameCounts, mergedWriteCount, _ := runReadyLogicalGroupMergeBurst(
		t,
		readyLogicalGroupMergeBurst{
			merge:          true,
			packCount:      readyLogicalGroupMergeTestPackCount,
			frameByteCount: 64,
			ack:            alwaysForTest(false),
			forceUnwrapped: alternatingForTest(),
		},
	)
	if len(wireFrameCounts) != readyLogicalGroupMergeTestPackCount {
		t.Fatalf("mixed force-unwrapped: wire items=%v, want %d singles", wireFrameCounts, readyLogicalGroupMergeTestPackCount)
	}
	if mergedWriteCount != 0 {
		t.Fatalf("mixed force-unwrapped merged %d items", mergedWriteCount)
	}
}

// A candidate that would push the item past the H1 byte bound stops the
// fold; it heads the next item, in order. Frames of 1 KiB fit three to the
// established H1 envelope and never four.
func TestMergeReadyLogicalGroupsStopsAtByteLimit(t *testing.T) {
	frameByteCount := 1024
	framesPerItem := int(sendPackH1EstablishedMaxMessageByteCount) / frameByteCount
	if framesPerItem != 3 {
		t.Fatalf("established H1 envelope holds %d 1 KiB frames, want 3", framesPerItem)
	}
	wireFrameCounts, mergedWriteCount, _ := runReadyLogicalGroupMergeBurst(
		t,
		readyLogicalGroupMergeBurst{
			merge:          true,
			packCount:      readyLogicalGroupMergeTestPackCount,
			frameByteCount: frameByteCount,
			ack:            alwaysForTest(false),
			forceUnwrapped: alwaysForTest(false),
		},
	)
	wantItems := (readyLogicalGroupMergeTestPackCount + framesPerItem - 1) / framesPerItem
	if len(wireFrameCounts) != wantItems {
		t.Fatalf("byte-bounded merge: wire items=%v, want %d", wireFrameCounts, wantItems)
	}
	for itemIndex, frameCount := range wireFrameCounts {
		if framesPerItem < frameCount {
			t.Fatalf("wire item %d carried %d KiB frames past the %d byte bound", itemIndex, frameCount, sendPackH1EstablishedMaxMessageByteCount)
		}
	}
	if mergedWriteCount != uint64(wantItems-1) && mergedWriteCount != uint64(wantItems) {
		t.Fatalf("byte-bounded merge counter items=%d, want %d or %d", mergedWriteCount, wantItems-1, wantItems)
	}
}

// The fold is H1-only. A flow-isolating carrier keeps one wire Pack per
// logical group, the path TestH3LogicalGroupsPreserveSameFlowFrameOrder
// covers, with the setting on.
func TestMergeReadyLogicalGroupsLeavesFlowIsolatedPolicyAlone(t *testing.T) {
	const packCount = 8
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	destinationId := NewId()
	sequenceEntered := make(chan struct{})
	releaseSequence := make(chan struct{})
	var sequenceEnteredOnce sync.Once
	var releaseSequenceOnce sync.Once
	release := func() { releaseSequenceOnce.Do(func() { close(releaseSequence) }) }

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.MergeReadyLogicalGroups = true
	settings.SendBufferSettings.beforeRunSendSequenceForTest = func(id sendSequenceId) {
		if id.Destination != destinationId {
			return
		}
		sequenceEnteredOnce.Do(func() { close(sequenceEntered) })
		<-releaseSequence
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	defer func() {
		release()
		closeTransferGroupTestClient(t, client)
	}()
	client.ContractManager().AddNoContractPeer(destinationId)
	route := make(chan []byte, 4*packCount)
	client.RouteManager().UpdateTransportWithProperties(
		&h3SendClientTransportForGroupTest{
			sendClientTransport: NewSendClientTransport(DestinationId(destinationId)),
		},
		[]Route{route},
		TransferCarrierProperties{
			Unreliable:              true,
			UnreliableFlowIsolation: true,
		},
	)

	frames, witnesses := transferGroupTestFrames(t, packCount, 64)
	results := make(chan error, packCount)
	for index, frame := range frames {
		success, err := client.sendGroupWithTimeoutDetailed(
			[]*protocol.Frame{frame},
			destinationId,
			func(err error) { results <- err },
			time.Second,
			NoAck(),
		)
		if !success || err != nil {
			for frameIndex := index; frameIndex < len(frames); frameIndex++ {
				MessagePoolReturn(frames[frameIndex].MessageBytes)
			}
			for _, witness := range witnesses {
				MessagePoolReturn(witness)
			}
			t.Fatalf("H3 logical group %d admission success=%t err=%v", index, success, err)
		}
		if index == 0 {
			select {
			case <-sequenceEntered:
			case <-ctx.Done():
				t.Fatalf("wait for held H3 sequence: %v", ctx.Err())
			}
		}
	}
	release()

	for groupIndex := range packCount {
		select {
		case err := <-results:
			if err != nil {
				t.Fatalf("H3 logical group %d completion: %v", groupIndex, err)
			}
		case <-ctx.Done():
			t.Fatalf("wait for H3 logical group %d: %v", groupIndex, ctx.Err())
		}
	}
	for wireIndex := range packCount {
		var transferFrameBytes []byte
		select {
		case transferFrameBytes = <-route:
		case <-ctx.Done():
			t.Fatalf("wait for H3 wire Pack %d: %v", wireIndex, ctx.Err())
		}
		pack := decodeSendPackLifecycleWirePack(t, transferFrameBytes)
		if len(pack.Frames) != 1 || pack.Frames[0].MessageBytes[0] != byte(wireIndex+1) {
			MessagePoolReturn(transferFrameBytes)
			t.Fatalf("H3 wire Pack %d frames=%d, want one frame with marker %d", wireIndex, len(pack.Frames), wireIndex+1)
		}
		MessagePoolReturn(transferFrameBytes)
	}
	if len(route) != 0 {
		t.Fatalf("H3 logical groups emitted %d extra wire Packs", len(route))
	}
	if stats := client.ReceiveStats(); stats.MergedLogicalGroupWriteCount != 0 {
		t.Fatalf("flow-isolated policy merged %d items", stats.MergedLogicalGroupWriteCount)
	}
	releaseTransferGroupTestWitnesses(t, frames, witnesses)
}
