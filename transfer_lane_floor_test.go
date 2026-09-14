package connect

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// THROUGHPUTFIX §27, row F1. A nonzero logical data lane is given a resend
// queue whose own floor is zeroed, so every byte it holds is borrowed from one
// shared pool the size of a single `ResendQueueMaxByteCount`, and each lane's
// cap is that whole pool. One bulk flow's lane can hold all of it. What the
// queue guarantees a lane with nothing of its own is that an empty queue
// admits one item, so a light lane beside a saturating one is reduced to a
// single Pack in flight, which is not a share of anything.
//
// Lane zero keeps `ResendQueueMinByteCount`, and so does every distinct
// destination on an sdk-hosted provider, so the floor is an established
// arrangement that the nonzero lanes alone are denied.
//
// The row asserts what a lane must keep rather than what it keeps today, so it
// fails on the tree as built and passes once the floors exist.
func TestLightLaneKeepsItsFloorBesideASaturatingLane(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	assertMessagePoolOwnership(t)

	// small enough that the heavy lane fills the pool in a few dozen Packs
	const laneResendQueueMaxByteCount = ByteCount(64 * 1024)
	const payloadByteCount = 1024
	// what a light lane must keep beside a saturating one: more than the one
	// Pack an empty queue is guaranteed
	const minimumLightLanePackCount = 4

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.LogicalDataLaneCount = 2
	settings.SendBufferSettings.ResendQueueMaxByteCount = laneResendQueueMaxByteCount
	// nothing may leave a queue by timing out while the lanes contend
	settings.SendBufferSettings.AckTimeout = time.Minute
	settings.SendBufferSettings.IdleTimeout = time.Minute
	settings.SendBufferSettings.MinResendInterval = time.Minute
	settings.SendBufferSettings.RttMinResendInterval = time.Minute
	settings.SendBufferSettings.MaxResendInterval = time.Minute

	destinationId := NewId()
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	route := make(chan []byte, 1024)
	client.ContractManager().AddNoContractPeer(destinationId)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)
	// what reaches the wire, which is what a lane's resend queue admitted;
	// the sequence's own Pack channel accepts past that and says nothing
	var writeCount atomic.Int64
	drainCtx, drainCancel := context.WithCancel(ctx)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case transferFrameBytes := <-route:
				MessagePoolReturn(transferFrameBytes)
				writeCount.Add(1)
			case <-drainCtx.Done():
				return
			}
		}
	}()
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := client.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close the client: %v", err)
		}
		drainCancel()
		<-drained
		for {
			select {
			case transferFrameBytes := <-route:
				MessagePoolReturn(transferFrameBytes)
			default:
				return
			}
		}
	})

	// nothing is ever acknowledged, so every admitted Pack stays in its lane's
	// resend queue and the pool only fills
	payload := string(make([]byte, payloadByteCount))
	sendOnLane := func(logicalLane uint32) bool {
		frame := RequireToFrameWithDefaultProtocolVersion(
			&protocol.SimpleMessage{Content: payload},
		)
		admitted, _ := client.SendWithTimeoutDetailed(
			frame,
			destinationId,
			nil,
			100*time.Millisecond,
			TransferKey{LogicalLane: logicalLane},
		)
		if !admitted {
			MessagePoolReturn(frame.MessageBytes)
		}
		return admitted
	}

	// the writes stop when the pool is full, since nothing is acknowledged and
	// the resend interval is longer than this test
	settled := func() int64 {
		previous := int64(-1)
		for range 100 {
			current := writeCount.Load()
			if current == previous {
				return current
			}
			previous = current
			time.Sleep(20 * time.Millisecond)
		}
		return writeCount.Load()
	}

	heavyPackCount := 0
	for heavyPackCount < 8*int(laneResendQueueMaxByteCount)/payloadByteCount {
		if !sendOnLane(1) {
			break
		}
		heavyPackCount += 1
	}
	heavyWriteCount := settled()
	if heavyWriteCount <= 0 {
		t.Fatal("the heavy lane wrote nothing, so the pool was never saturated")
	}

	for range minimumLightLanePackCount {
		sendOnLane(2)
	}
	lightWriteCount := settled() - heavyWriteCount

	if lightWriteCount < minimumLightLanePackCount {
		t.Errorf(
			"the light lane put %d Packs of %d bytes on the wire beside a lane holding the whole %d byte pool (%d writes), short of the %d a floor of its own would keep; a lane whose floor is zeroed is reduced to the single item an empty queue guarantees",
			lightWriteCount,
			payloadByteCount,
			laneResendQueueMaxByteCount,
			heavyWriteCount,
			minimumLightLanePackCount,
		)
	}

	t.Logf(
		"heavy lane %d writes (%d Packs offered), light lane %d writes, against a %d byte shared pool",
		heavyWriteCount,
		heavyPackCount,
		lightWriteCount,
		laneResendQueueMaxByteCount,
	)
}
