package connect

import (
	"context"
	"sync"
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
	// comfortably above four framed Packs of that payload
	const laneFloorByteCount = ByteCount(8 * 1024)

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.LogicalDataLaneCount = 2
	settings.SendBufferSettings.ResendQueueMaxByteCount = laneResendQueueMaxByteCount
	// the floor under test; zero is the shipping default and is what the row
	// fails on
	settings.SendBufferSettings.LaneFloorByteCount = laneFloorByteCount
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

// §27 row F2. The floor is an exemption from the shared pool, not a
// reservation in it, so a client whose flows all hash to one data lane pays
// nothing for the lanes it never opens: the pool is materialised only by the
// first nonzero lane and holds only what that lane actually queued.
func TestOneLaneClientPaysNoFloor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	assertMessagePoolOwnership(t)

	const laneFloorByteCount = ByteCount(8 * 1024)
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.LogicalDataLaneCount = 8
	settings.SendBufferSettings.ResendQueueMaxByteCount = ByteCount(64 * 1024)
	settings.SendBufferSettings.LaneFloorByteCount = laneFloorByteCount
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
	drainCtx, drainCancel := context.WithCancel(ctx)
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		for {
			select {
			case transferFrameBytes := <-route:
				MessagePoolReturn(transferFrameBytes)
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

	// nothing has used a nonzero lane, so no pool exists
	if budget := laneResendBudget(client); budget != nil {
		t.Errorf("a lane pool of %d bytes exists before any nonzero lane sent", budget.TotalByteCount())
	}

	payload := string(make([]byte, 1024))
	for range 16 {
		frame := RequireToFrameWithDefaultProtocolVersion(
			&protocol.SimpleMessage{Content: payload},
		)
		admitted, _ := client.SendWithTimeoutDetailed(
			frame,
			destinationId,
			nil,
			100*time.Millisecond,
			TransferKey{LogicalLane: 3},
		)
		if !admitted {
			MessagePoolReturn(frame.MessageBytes)
			break
		}
	}
	time.Sleep(200 * time.Millisecond)

	budget := laneResendBudget(client)
	if budget == nil {
		t.Fatal("a nonzero lane sent and no pool was materialised")
	}

	// The property, stated so it holds at any floor and any lane count rather
	// than at the numbers this row happens to use: a lane borrows from the
	// pool only what it holds above its own floor, so the pool's used bytes
	// are the one active lane's queued bytes less its floor, and the seven
	// lanes never opened contribute nothing. A reservation implementation
	// charges the pool for every lane's floor whether it is used or not, and
	// an assertion merely comparing used against total would let that pass
	// wherever the floors happen not to sum past the pool.
	usedByteCount := budget.UsedByteCount()
	queuedByteCount := laneQueuedByteCount(client)
	wantUsedByteCount := max(0, queuedByteCount-laneFloorByteCount)
	if usedByteCount != wantUsedByteCount {
		t.Errorf(
			"the lane pool holds %d bytes with one lane of eight holding %d against a %d byte floor; it must hold what that lane borrows above its floor, %d, and nothing for the seven lanes never opened",
			usedByteCount,
			queuedByteCount,
			laneFloorByteCount,
			wantUsedByteCount,
		)
	}
	t.Logf(
		"one lane of eight holds %d bytes against a %d byte floor: the pool holds %d of %d",
		queuedByteCount,
		laneFloorByteCount,
		usedByteCount,
		budget.TotalByteCount(),
	)
}

// §27 row F3. The floors do not partition the pool: with the other lanes idle,
// one active lane still borrows up to the whole cap.
func TestLaneFloorsAreExemptionsNotReservations(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	assertMessagePoolOwnership(t)

	const laneResendQueueMaxByteCount = ByteCount(64 * 1024)
	const payloadByteCount = 1024

	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	settings.SendBufferSettings.LogicalDataLaneCount = 8
	settings.SendBufferSettings.ResendQueueMaxByteCount = laneResendQueueMaxByteCount
	settings.SendBufferSettings.LaneFloorByteCount = ByteCount(8 * 1024)
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

	payload := string(make([]byte, payloadByteCount))
	offered := 0
	for offered < 4*int(laneResendQueueMaxByteCount)/payloadByteCount {
		frame := RequireToFrameWithDefaultProtocolVersion(
			&protocol.SimpleMessage{Content: payload},
		)
		admitted, _ := client.SendWithTimeoutDetailed(
			frame,
			destinationId,
			nil,
			100*time.Millisecond,
			TransferKey{LogicalLane: 5},
		)
		if !admitted {
			MessagePoolReturn(frame.MessageBytes)
			break
		}
		offered += 1
	}
	previous := int64(-1)
	for range 100 {
		current := writeCount.Load()
		if current == previous {
			break
		}
		previous = current
		time.Sleep(20 * time.Millisecond)
	}

	// the whole cap, not the cap less seven floors
	heldByteCount := ByteCount(writeCount.Load()) * payloadByteCount
	if heldByteCount <= laneResendQueueMaxByteCount/2 {
		t.Errorf(
			"one active lane put only %d bytes on the wire against a %d byte pool; with the other lanes idle it must be able to borrow the whole cap, so the floors are being reserved rather than exempted",
			heldByteCount,
			laneResendQueueMaxByteCount,
		)
	}
	t.Logf("one active lane of eight wrote %d Packs, about %d bytes of a %d byte pool", writeCount.Load(), heldByteCount, laneResendQueueMaxByteCount)
}

// what every nonzero lane currently holds queued, which is what it borrows
// from the pool above its floor
func laneQueuedByteCount(client *Client) ByteCount {
	sequences := func() []*SendSequence {
		client.sendBuffer.mutex.Lock()
		defer client.sendBuffer.mutex.Unlock()
		sequences := []*SendSequence{}
		for id, sequence := range client.sendBuffer.sendSequences {
			if id.LogicalLane != 0 {
				sequences = append(sequences, sequence)
			}
		}
		return sequences
	}()
	queuedByteCount := ByteCount(0)
	for _, sequence := range sequences {
		_, sequenceByteCount := sequence.resendQueue.QueueSize()
		queuedByteCount += sequenceByteCount
	}
	return queuedByteCount
}

// the lazily materialised pool every nonzero lane shares, or nil before the
// first one sends
func laneResendBudget(client *Client) *TransferMemoryBudget {
	client.sendBuffer.mutex.Lock()
	defer client.sendBuffer.mutex.Unlock()
	return client.sendBuffer.logicalLaneResendBudget
}

// THROUGHPUTFIX §27 row F5, the in-process mirror of the lane campaign's
// question. F1 asks what a light lane may hold with nothing acknowledged,
// which is the starvation in its static form. This asks the thing a campaign
// measures: with acknowledgements flowing and a heavy lane offering as fast as
// it is admitted, what does a light lane actually deliver.
//
// The loop is two clients and two pumps, with a delay on the acknowledgement
// pump so the resend queues hold about a round trip of data and the shared
// pool is the binding constraint rather than the wire. Without a floor the
// light lane's share of that pool is whatever the heavy lane leaves, which is
// the one item an empty queue guarantees; with a floor it is at least the
// floor.
func TestLightLaneDeliveryBesideASaturatingLane(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	assertMessagePoolOwnership(t)

	const laneResendQueueMaxByteCount = ByteCount(64 * 1024)
	const laneFloorByteCount = ByteCount(8 * 1024)
	const payloadByteCount = 1024
	const ackDelay = 20 * time.Millisecond
	const observationWindow = 2 * time.Second

	newSettings := func() *ClientSettings {
		settings := DefaultClientSettings()
		settings.EncryptionSettings.Mode = EncryptionModeOff
		settings.SendBufferSettings.LogicalDataLaneCount = 2
		settings.SendBufferSettings.ResendQueueMaxByteCount = laneResendQueueMaxByteCount
		settings.SendBufferSettings.LaneFloorByteCount = laneFloorByteCount
		settings.SendBufferSettings.AckTimeout = 60 * time.Second
		settings.SendBufferSettings.IdleTimeout = 60 * time.Second
		settings.ReceiveBufferSettings.GapTimeout = 60 * time.Second
		settings.ReceiveBufferSettings.IdleTimeout = 60 * time.Second
		return settings
	}
	senderId := NewId()
	receiverId := NewId()
	sender := NewClient(ctx, senderId, NewNoContractClientOob(), newSettings())
	receiver := NewClient(ctx, receiverId, NewNoContractClientOob(), newSettings())
	sender.ContractManager().AddNoContractPeer(receiverId)
	receiver.ContractManager().AddNoContractPeer(senderId)

	senderOut := make(Route, 64)
	senderIn := make(Route, 64)
	receiverIn := make(Route, 64)
	receiverOut := make(Route, 64)
	sender.RouteManager().UpdateTransport(NewSendGatewayTransport(), []Route{senderOut})
	sender.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{senderIn})
	receiver.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{receiverIn})
	receiver.RouteManager().UpdateTransport(NewSendGatewayTransport(), []Route{receiverOut})

	// delivered payload bytes per lane, read from the receiver-visible lane
	var laneDeliveredByteCounts sync.Map
	receiver.AddReceiveCallback(func(_ TransferPath, frames []*protocol.Frame, peer Peer) {
		deliveredByteCount := 0
		for _, frame := range frames {
			deliveredByteCount += len(frame.MessageBytes)
		}
		delivered, _ := laneDeliveredByteCounts.LoadOrStore(peer.TransferKey.LogicalLane, &atomic.Int64{})
		delivered.(*atomic.Int64).Add(int64(deliveredByteCount))
	})

	// the wire, with a round trip on the acknowledgement half
	pumpsDone := []chan struct{}{}
	pump := func(from Route, to Route, delay time.Duration) {
		done := make(chan struct{})
		pumpsDone = append(pumpsDone, done)
		go func() {
			defer close(done)
			for {
				select {
				case transferFrameBytes := <-from:
					if 0 < delay {
						time.Sleep(delay)
					}
					select {
					case to <- transferFrameBytes:
					case <-ctx.Done():
						MessagePoolReturn(transferFrameBytes)
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	pump(senderOut, receiverIn, 0)
	pump(receiverOut, senderIn, ackDelay)
	t.Cleanup(func() {
		cancel()
		for _, done := range pumpsDone {
			<-done
		}
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := sender.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close the sender: %v", err)
		}
		if err := receiver.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close the receiver: %v", err)
		}
		for _, route := range []Route{senderOut, senderIn, receiverIn, receiverOut} {
			draining := true
			for draining {
				select {
				case transferFrameBytes := <-route:
					MessagePoolReturn(transferFrameBytes)
				default:
					draining = false
				}
			}
		}
	})

	payload := string(make([]byte, payloadByteCount))
	offer := func(logicalLane uint32, timeout time.Duration) bool {
		frame := RequireToFrameWithDefaultProtocolVersion(
			&protocol.SimpleMessage{Content: payload},
		)
		admitted, _ := sender.SendWithTimeoutDetailed(
			frame,
			receiverId,
			nil,
			timeout,
			TransferKey{LogicalLane: logicalLane},
		)
		if !admitted {
			MessagePoolReturn(frame.MessageBytes)
		}
		return admitted
	}

	// the heavy lane offers as fast as it is admitted; the light lane offers
	// steadily and modestly, which is what a light flow looks like
	offering := make(chan struct{})
	heavyDone := make(chan struct{})
	lightDone := make(chan struct{})
	go func() {
		defer close(heavyDone)
		for {
			select {
			case <-offering:
				return
			default:
			}
			offer(1, 20*time.Millisecond)
		}
	}()
	go func() {
		defer close(lightDone)
		for {
			select {
			case <-offering:
				return
			default:
			}
			offer(2, 20*time.Millisecond)
			// a light flow offers steadily and modestly rather than filling
			// its queue; the starvation needs that asymmetry, and two lanes
			// both offering flat out split the pool evenly with or without a
			// floor
			time.Sleep(5 * time.Millisecond)
		}
	}()
	time.Sleep(observationWindow)
	close(offering)
	<-heavyDone
	<-lightDone
	time.Sleep(2 * ackDelay)

	laneDelivered := func(logicalLane uint32) ByteCount {
		delivered, ok := laneDeliveredByteCounts.Load(logicalLane)
		if !ok {
			return 0
		}
		return ByteCount(delivered.(*atomic.Int64).Load())
	}
	heavyByteCount := laneDelivered(1)
	lightByteCount := laneDelivered(2)

	// Per acknowledgement round trip, which is the rate a lane's in-flight
	// allowance converts into delivery. Measured populations on this rig, so a
	// later reader can see the margin the threshold sits in: with the floor,
	// 873 to 1,027 bytes per round trip; without it, 103 to 195. Half a Pack
	// separates them and is what a lane reduced to the single item an empty
	// queue guarantees cannot reach.
	roundTripCount := ByteCount(observationWindow / ackDelay)
	lightByteCountPerRoundTrip := lightByteCount / roundTripCount
	heavyByteCountPerRoundTrip := heavyByteCount / roundTripCount
	if lightByteCountPerRoundTrip < payloadByteCount/2 {
		t.Errorf(
			"the light lane delivered %d bytes per %s round trip (%d in %s) beside a lane delivering %d per round trip, under half of one %d byte Pack; a lane whose floor is zero keeps the single item an empty queue guarantees and delivers about that per round trip",
			lightByteCountPerRoundTrip,
			ackDelay,
			lightByteCount,
			observationWindow,
			heavyByteCountPerRoundTrip,
			payloadByteCount,
		)
	}
	t.Logf(
		"over %s (%d round trips) the heavy lane delivered %d bytes (%d per round trip) and the light lane %d (%d per round trip), against a %d byte floor and a %d byte pool",
		observationWindow,
		roundTripCount,
		heavyByteCount,
		heavyByteCountPerRoundTrip,
		lightByteCount,
		lightByteCountPerRoundTrip,
		laneFloorByteCount,
		laneResendQueueMaxByteCount,
	)
}
