package connect

import (
	"context"
	"encoding/binary"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

func newDispatchTestSequence(
	ctx context.Context,
	dispatcher *udpReceiveDispatcher,
	callback ReceivePacketFunction,
) *UdpSequence {
	settings := DefaultUdpBufferSettingsWithBufferSize(8)
	sequence := NewUdpSequence(
		ctx,
		callback,
		SourceId(NewId()),
		protocol.ProvideMode_Network,
		4,
		[]byte{10, 0, 0, 1},
		10000,
		[]byte{10, 0, 0, 2},
		20000,
		settings,
	)
	sequence.receiveDispatcher = dispatcher
	sequence.receiveShard = dispatcher.assignShard()
	return sequence
}

func TestUdpReceiveDispatcherPreservesPerFlowOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultUdpBufferSettingsWithBufferSize(32)
	settings.ReceiveShardCount = 4
	settings.WriteBatchSize = 2
	dispatcher := newUdpReceiveDispatcher(ctx, settings)

	const flowCount = 12
	const packetsPerFlow = 200
	received := make([][]uint32, flowCount)
	var receivedMu sync.Mutex
	var receivedWait sync.WaitGroup
	receivedWait.Add(flowCount * packetsPerFlow)

	sequences := make([]*UdpSequence, flowCount)
	for flow := range flowCount {
		flow := flow
		sequences[flow] = newDispatchTestSequence(ctx, dispatcher, func(
			_ TransferPath,
			_ protocol.ProvideMode,
			_ *IpPath,
			packet []byte,
		) {
			packetFlow := int(binary.BigEndian.Uint32(packet[0:4]))
			sequenceNumber := binary.BigEndian.Uint32(packet[4:8])
			receivedMu.Lock()
			received[packetFlow] = append(received[packetFlow], sequenceNumber)
			receivedMu.Unlock()
			receivedWait.Done()
		})
	}

	var sendWait sync.WaitGroup
	sendWait.Add(flowCount)
	for flow, sequence := range sequences {
		go func(flow int, sequence *UdpSequence) {
			defer sendWait.Done()
			for n := range packetsPerFlow {
				packet := MessagePoolGet(8)
				binary.BigEndian.PutUint32(packet[0:4], uint32(flow))
				binary.BigEndian.PutUint32(packet[4:8], uint32(n))
				if !dispatcher.enqueue(sequence, packet) {
					MessagePoolReturn(packet)
					return
				}
			}
		}(flow, sequence)
	}
	sendWait.Wait()

	done := make(chan struct{})
	go func() {
		receivedWait.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for ordered dispatch")
	}

	receivedMu.Lock()
	defer receivedMu.Unlock()
	for flow, sequenceNumbers := range received {
		if len(sequenceNumbers) != packetsPerFlow {
			t.Fatalf("flow %d received %d packets, want %d", flow, len(sequenceNumbers), packetsPerFlow)
		}
		for i, sequenceNumber := range sequenceNumbers {
			if sequenceNumber != uint32(i) {
				t.Fatalf("flow %d reordered at %d: got %d", flow, i, sequenceNumber)
			}
		}
	}
}

func TestUdpReceiveDispatcherAggregateQueueIsFlowIndependent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultUdpBufferSettingsWithBufferSize(7)
	settings.ReceiveShardCount = 3
	dispatcher := newUdpReceiveDispatcher(ctx, settings)

	totalCapacity := 0
	for i := range dispatcher.shards {
		totalCapacity += cap(dispatcher.shards[i].items)
	}
	if totalCapacity != settings.ReceiveShardCount*settings.SequenceBufferSize {
		t.Fatalf("aggregate queue capacity=%d, want %d", totalCapacity, settings.ReceiveShardCount*settings.SequenceBufferSize)
	}

	// Assign far more flows than shards. Capacity remains fixed; only the
	// round-robin shard identity changes.
	for i := 0; i < 10_000; i++ {
		shard := dispatcher.assignShard()
		if shard < 0 || len(dispatcher.shards) <= shard {
			t.Fatalf("invalid shard %d", shard)
		}
	}
	afterCapacity := 0
	for i := range dispatcher.shards {
		afterCapacity += cap(dispatcher.shards[i].items)
	}
	if afterCapacity != totalCapacity {
		t.Fatalf("queue capacity grew with flow count: %d -> %d", totalCapacity, afterCapacity)
	}
}

func BenchmarkUdpReceiveDispatcher(b *testing.B) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultUdpBufferSettingsWithBufferSize(96)
	settings.ReceiveShardCount = 4
	settings.WriteBatchSize = 2
	dispatcher := newUdpReceiveDispatcher(ctx, settings)

	const flowCount = 32
	var received sync.WaitGroup
	received.Add(b.N)
	sequences := make([]*UdpSequence, flowCount)
	for flow := range flowCount {
		sequences[flow] = newDispatchTestSequence(ctx, dispatcher, func(
			_ TransferPath,
			_ protocol.ProvideMode,
			_ *IpPath,
			_ []byte,
		) {
			received.Done()
		})
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		packet := MessagePoolGet(64)
		if !dispatcher.enqueue(sequences[i%flowCount], packet) {
			MessagePoolReturn(packet)
			b.Fatal("dispatcher closed")
		}
	}
	received.Wait()
	b.StopTimer()
}
