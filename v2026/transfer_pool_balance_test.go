package connect

import (
	"context"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// TestClientClosePoolBalance pins the message-pool ownership contract across a
// client's whole lifecycle in the worst case for buffer bookkeeping: a client with
// NO transports (an offline device), whose control-plane sends (provide modes,
// client key publish) can never be delivered and so park in the send machinery
// until close. Every pooled buffer taken on behalf of the client must be returned
// once the client is closed — anything else is a lost MessagePoolReturn that
// silently degrades pool reuse in production (the GC hides it from heap checks).
func TestClientClosePoolBalance(t *testing.T) {
	poolOutstanding := func() int64 {
		taken, returned, _ := MessagePoolCounts()
		return int64(taken) - int64(returned)
	}
	settle := func() int64 {
		// outstanding buffers are returned asynchronously as goroutines unwind;
		// poll until stable
		prev := poolOutstanding()
		stableCount := 0
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			time.Sleep(50 * time.Millisecond)
			n := poolOutstanding()
			if n == prev {
				stableCount += 1
				if 4 <= stableCount {
					break
				}
			} else {
				stableCount = 0
				prev = n
			}
		}
		return prev
	}

	before := settle()

	ctx, cancel := context.WithCancel(context.Background())
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())

	// the control-plane traffic an offline device generates: provide-mode toggles
	// (contract manager control frames) on top of the automatic client key publish
	for i := 0; i < 10; i += 1 {
		client.ContractManager().SetProvideModesWithReturnTraffic(map[protocol.ProvideMode]bool{
			protocol.ProvideMode_Public: true,
		})
		client.ContractManager().SetProvideModesWithReturnTraffic(map[protocol.ProvideMode]bool{})
	}

	// several churned clients with unbuffered sequence queues (the multi-client
	// window settings), whose lifetime is much shorter than any send could take
	for i := 0; i < 10; i += 1 {
		clientSettings := DefaultClientSettings()
		clientSettings.SendBufferSettings.SequenceBufferSize = 0
		clientSettings.SendBufferSettings.AckBufferSize = 0
		clientSettings.ReceiveBufferSettings.SequenceBufferSize = 0
		clientSettings.ForwardBufferSettings.SequenceBufferSize = 0
		churnClient := NewClient(ctx, NewId(), NewNoContractClientOob(), clientSettings)
		time.Sleep(20 * time.Millisecond) // let the key manager publish
		churnClient.Cancel()
	}

	// let the async publishes reach the send machinery
	time.Sleep(500 * time.Millisecond)
	t.Logf("outstanding while open: %d (parked control frames expected)", poolOutstanding()-before)

	client.Cancel()
	cancel()

	after := settle()
	if before < after {
		t.Errorf("pool buffers not returned after client close: outstanding %d -> %d (+%d)",
			before, after, after-before)
	}
}
