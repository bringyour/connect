package connect

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// THROUGHPUTFIX §10.7, row A6: a live bug on main that neither fix in this
// program causes or touches, pinned so the transfer-layer change that fixes it
// is made deliberately.
//
// A send sequence exits and drains its resend queue when the oldest due item
// is past its ack timeout and is not retained past it. Socket-owned TCP return
// items are retained, because the provider already consumed those upstream
// bytes and nothing below can reproduce them. A synthesized control on the same
// sequence is not retained, and neither is a datagram promoted to Ack while a
// contract opens. So one unretained item that goes unacknowledged for the ack
// timeout closes the sequence and takes every retained item with it: the
// provider's consumed TCP bytes are dropped, and the NAT toward the client does
// not retransmit, so those inner connections carry a hole until they are reset.
//
// This documents the current behaviour on every tree. The fix belongs in the
// transfer layer, where an ack timeout on an unretained item should drop that
// item rather than close a sequence holding retained ones.
func TestNonRetainedAckTimeoutClosesTheSequenceWithRetainedItems(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	destinationId := NewId()
	sequenceDone := make(chan struct{})
	var doneOnce sync.Once
	var forcedCount atomic.Uint64
	var forceEnabled atomic.Bool
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	// a minute, so only the forced branch below ends the sequence
	settings.SendBufferSettings.AckTimeout = time.Minute
	// both items are queued as their own resend entries before the ack
	// timeout is forced, so the row is two items rather than one coalesced one
	settings.SendBufferSettings.forceAckTimeoutForTest = func(id sendSequenceId) bool {
		if id.Destination != destinationId || !forceEnabled.Load() {
			return false
		}
		forcedCount.Add(1)
		return true
	}
	settings.SendBufferSettings.MinResendInterval = 10 * time.Millisecond
	settings.SendBufferSettings.RttMinResendInterval = 10 * time.Millisecond
	settings.SendBufferSettings.MaxResendInterval = 20 * time.Millisecond
	settings.SendBufferSettings.afterRunSendSequenceForTest = func(id sendSequenceId) {
		if id.Destination == destinationId {
			doneOnce.Do(func() { close(sequenceDone) })
		}
	}
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	route := make(chan []byte, 8)
	defer cleanupSendPackLifecycleSequence(t, ctx, client, nil, sequenceDone, route)
	client.ContractManager().AddNoContractPeer(destinationId)
	client.RouteManager().UpdateTransport(
		NewSendClientTransport(DestinationId(destinationId)),
		[]Route{route},
	)

	written := func(name string) {
		t.Helper()
		select {
		case transferFrameBytes := <-route:
			MessagePoolReturn(transferFrameBytes)
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for %s to reach the route", name)
		}
	}
	send := func(content string, retained bool) chan error {
		t.Helper()
		frame := RequireToFrameWithDefaultProtocolVersion(
			&protocol.SimpleMessage{Content: content},
		)
		results := make(chan error, 1)
		admitted, _ := client.SendWithTimeoutDetailed(
			frame,
			destinationId,
			func(err error) { results <- err },
			2*time.Second,
			sendPackRecoveryOption{upstreamRecoverable: true, retainAfterAckTimeout: retained},
		)
		if !admitted {
			MessagePoolReturn(frame.MessageBytes)
			t.Fatalf("%s was not admitted", content)
		}
		return results
	}

	// the unretained item is ahead of the retained one, which is the shape
	// §10.7 names: the provider's consumed upstream bytes are queued behind a
	// synthesized control, and the control is the oldest due item
	unretainedResults := send("synthesized control", false)
	written("the synthesized control")
	retainedResults := send("socket-owned return bytes", true)
	written("the socket-owned return bytes")
	forceEnabled.Store(true)
	go func() {
		for {
			select {
			case transferFrameBytes := <-route:
				MessagePoolReturn(transferFrameBytes)
			case <-ctx.Done():
				return
			}
		}
	}()

	requireResult := func(results chan error, name string) error {
		t.Helper()
		select {
		case err := <-results:
			return err
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for the %s result", name)
			return nil
		}
	}
	unretainedErr := requireResult(unretainedResults, "unretained item")
	retainedErr := requireResult(retainedResults, "retained item")

	if forcedCount.Load() == 0 {
		t.Fatal("the ack timeout branch was never reached")
	}
	if unretainedErr == nil {
		t.Error("the unretained item completed successfully; the row needs its ack timeout to be the thing that ends the sequence")
	}
	if retainedErr == nil {
		t.Error("the retained item completed successfully; if a retained item now survives an unretained item's ack timeout, §10.7 is fixed and this row should be rewritten as a guard")
	} else {
		t.Logf(
			"the retained item was completed with %q by the unretained item's ack timeout: the provider's consumed upstream bytes are dropped and the NAT does not retransmit them",
			retainedErr,
		)
	}
}
