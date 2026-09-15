package connect

import (
	"context"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// A bounded send queue can have an application writer waiting forever while
// a liveness probe uses a finite admission timeout. The probe's timeout must
// be independent: serializing both callers behind the close mutex used to
// make the finite timeout unreachable and hid a route-full path indefinitely.
func TestSendSequenceConcurrentPackHonorsIndependentTimeout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sequence := &SendSequence{
		ctx:           ctx,
		cancel:        cancel,
		packs:         make(chan *SendPack, 1),
		idleCondition: NewIdleCondition(),
	}
	newPack := func() *SendPack {
		return &SendPack{
			Frame: &protocol.Frame{},
			Ctx:   ctx,
		}
	}

	// Fill the queue, then leave one application writer blocked without a
	// timeout. It deliberately retains a read-side close guard.
	sequence.packs <- newPack()
	blockedDone := make(chan struct{})
	go func() {
		defer close(blockedDone)
		_, _ = sequence.Pack(newPack(), -1)
	}()

	// Give the first writer time to enter its wait. The assertion below does
	// not depend on exact scheduling: under the old exclusive mutex it waited
	// until ctx cancellation and therefore could not return within the bound.
	time.Sleep(10 * time.Millisecond)

	start := time.Now()
	success, err := sequence.Pack(newPack(), 25*time.Millisecond)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("finite pack returned error: %v", err)
	}
	if success {
		t.Fatal("finite pack unexpectedly entered a full queue")
	}
	if 150*time.Millisecond < elapsed {
		t.Fatalf("finite pack timeout was serialized behind another writer: %s", elapsed)
	}

	cancel()
	select {
	case <-blockedDone:
	case <-time.After(time.Second):
		t.Fatal("blocked writer did not wake on sequence cancellation")
	}
}
