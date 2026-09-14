package connect

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// testOobControl is a mock OutOfBandControl: it fails the first `failUntil`
// SendControl attempts and then succeeds, recording the number of attempts. It
// mirrors ApiOutOfBandControl by consuming the frames it is given and invoking
// the callback asynchronously.
type testOobControl struct {
	stateLock sync.Mutex
	attempts  int
	failUntil int
}

type delayedOobAttempt struct {
	callback OobResultFunction
}

type delayedOobControl struct {
	attempts chan delayedOobAttempt
}

func (self *delayedOobControl) SendControl(frames []*protocol.Frame, callback OobResultFunction) {
	for _, frame := range frames {
		MessagePoolReturn(frame.MessageBytes)
	}
	self.attempts <- delayedOobAttempt{callback: callback}
}

type contextualDelayedOobAttempt struct {
	ctx      context.Context
	callback OobResultFunction
}

type contextualDelayedOobControl struct {
	attempts chan contextualDelayedOobAttempt
}

func (self *contextualDelayedOobControl) SendControl(frames []*protocol.Frame, callback OobResultFunction) {
	panic("context-aware path was not used")
}

func (self *contextualDelayedOobControl) SendControlWithCtx(
	ctx context.Context,
	frames []*protocol.Frame,
	callback OobResultFunction,
) {
	for _, frame := range frames {
		MessagePoolReturn(frame.MessageBytes)
	}
	self.attempts <- contextualDelayedOobAttempt{ctx: ctx, callback: callback}
}

func (self *testOobControl) setFailUntil(n int) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.failUntil = n
}

func (self *testOobControl) attemptCount() int {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.attempts
}

func (self *testOobControl) SendControl(frames []*protocol.Frame, callback OobResultFunction) {
	// mimic ApiOutOfBandControl: the input frames are consumed
	for _, frame := range frames {
		MessagePoolReturn(frame.MessageBytes)
	}

	self.stateLock.Lock()
	self.attempts += 1
	attempt := self.attempts
	failUntil := self.failUntil
	self.stateLock.Unlock()

	go func() {
		if attempt <= failUntil {
			callback(nil, fmt.Errorf("simulated oob failure (attempt %d)", attempt))
		} else {
			// success: the oob returns (the platform processed the message)
			callback(nil, nil)
		}
	}()
}

func newControlSyncOobTestClient(ctx context.Context, oob OutOfBandControl) *Client {
	settings := DefaultClientSettings()
	// the oob path does not use the transport; disable the control ping so the
	// client does not spin trying to ping without a transport
	settings.ControlPingTimeout = 0
	return NewClient(ctx, NewId(), oob, settings)
}

// the oob ack fires once after the platform succeeds, retrying through
// transient failures
func TestControlSyncOobRetryUntilSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}

	makeFrame := func(index uint32) *protocol.Frame {
		frame, err := ToFrame(&protocol.SimpleMessage{MessageIndex: index}, DefaultProtocolVersion)
		AssertEqual(t, err, nil)
		return frame
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oob := &testOobControl{failUntil: 3}
	client := newControlSyncOobTestClient(ctx, oob)

	cs := NewControlSyncOob(ctx, client, "m1")
	cs.retryTimeout = 10 * time.Millisecond
	defer cs.Close()

	var ackLock sync.Mutex
	ackCount := 0
	ackCh := make(chan error, 1)
	cs.Send(makeFrame(1), func(err error) {
		ackLock.Lock()
		ackCount += 1
		ackLock.Unlock()
		select {
		case ackCh <- err:
		default:
		}
	})

	select {
	case err := <-ackCh:
		AssertEqual(t, err, nil)
	case <-time.After(10 * time.Second):
		t.Fatal("oob ack did not fire")
	}

	// 3 failures + 1 success
	AssertEqual(t, oob.attemptCount(), 4)

	// the ack fires exactly once
	select {
	case <-time.After(500 * time.Millisecond):
	}
	ackLock.Lock()
	AssertEqual(t, ackCount, 1)
	ackLock.Unlock()
}

// immediate success acks without retrying
func TestControlSyncOobImmediateSuccess(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}

	makeFrame := func(index uint32) *protocol.Frame {
		frame, err := ToFrame(&protocol.SimpleMessage{MessageIndex: index}, DefaultProtocolVersion)
		AssertEqual(t, err, nil)
		return frame
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oob := &testOobControl{failUntil: 0}
	client := newControlSyncOobTestClient(ctx, oob)

	cs := NewControlSyncOob(ctx, client, "m1")
	cs.retryTimeout = 10 * time.Millisecond
	defer cs.Close()

	ackCh := make(chan error, 1)
	cs.Send(makeFrame(1), func(err error) {
		select {
		case ackCh <- err:
		default:
		}
	})

	select {
	case err := <-ackCh:
		AssertEqual(t, err, nil)
	case <-time.After(5 * time.Second):
		t.Fatal("oob ack did not fire")
	}
	AssertEqual(t, oob.attemptCount(), 1)
}

// a newer Send for the same scope supersedes an older one still retrying
func TestControlSyncOobLatestSupersedes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}

	makeFrame := func(index uint32) *protocol.Frame {
		frame, err := ToFrame(&protocol.SimpleMessage{MessageIndex: index}, DefaultProtocolVersion)
		AssertEqual(t, err, nil)
		return frame
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// keep failing so neither send completes until allowed
	oob := &testOobControl{failUntil: 1 << 30}
	client := newControlSyncOobTestClient(ctx, oob)

	cs := NewControlSyncOob(ctx, client, "m1")
	cs.retryTimeout = 10 * time.Millisecond
	defer cs.Close()

	firstAcked := make(chan struct{}, 1)
	cs.Send(makeFrame(1), func(err error) {
		select {
		case firstAcked <- struct{}{}:
		default:
		}
	})

	// let the first send fail a few times
	select {
	case <-time.After(100 * time.Millisecond):
	}

	// supersede with a newer send (still failing), then allow success — so
	// the first send is superseded while it can never succeed
	secondAcked := make(chan error, 1)
	cs.Send(makeFrame(2), func(err error) {
		select {
		case secondAcked <- err:
		default:
		}
	})
	oob.setFailUntil(0)

	select {
	case err := <-secondAcked:
		AssertEqual(t, err, nil)
	case <-time.After(5 * time.Second):
		t.Fatal("second oob ack did not fire")
	}

	// the superseded first send must not ack
	select {
	case <-firstAcked:
		t.Fatal("superseded send unexpectedly acked")
	case <-time.After(200 * time.Millisecond):
	}
}

func TestControlSyncOobLateInflightSuccessCannotAckSupersededGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	makeFrame := func(index uint32) *protocol.Frame {
		frame, err := ToFrame(&protocol.SimpleMessage{MessageIndex: index}, DefaultProtocolVersion)
		if err != nil {
			t.Fatal(err)
		}
		return frame
	}
	oob := &delayedOobControl{attempts: make(chan delayedOobAttempt, 2)}
	client := newControlSyncOobTestClient(ctx, oob)
	defer client.Cancel()
	cs := NewControlSyncOob(ctx, client, "generation")
	defer cs.Close()

	firstAcked := make(chan struct{}, 1)
	cs.Send(makeFrame(1), func(error) {
		firstAcked <- struct{}{}
	})
	firstAttempt := <-oob.attempts

	secondAcked := make(chan struct{}, 1)
	cs.Send(makeFrame(2), func(error) {
		secondAcked <- struct{}{}
	})
	secondAttempt := <-oob.attempts

	// The first request was already handed to OOB when the second generation
	// superseded it. Its late success must not be observable as a current ack.
	firstAttempt.callback(nil, nil)
	select {
	case <-firstAcked:
		t.Fatal("late success acknowledged the superseded OOB generation")
	case <-time.After(50 * time.Millisecond):
	}

	secondAttempt.callback(nil, nil)
	select {
	case <-secondAcked:
	case <-time.After(time.Second):
		t.Fatal("current OOB generation did not acknowledge")
	}
}

// Success must claim and retire its generation before invoking arbitrary user
// code. Otherwise there is a check/act window: a newer Send can supersede the
// request after its "current" check but before the old ack is published.
func TestControlSyncOobSuccessClaimsGenerationBeforeAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	frame, err := ToFrame(&protocol.SimpleMessage{MessageIndex: 1}, DefaultProtocolVersion)
	if err != nil {
		t.Fatal(err)
	}
	oob := &delayedOobControl{attempts: make(chan delayedOobAttempt, 1)}
	client := newControlSyncOobTestClient(ctx, oob)
	defer client.Cancel()
	cs := NewControlSyncOob(ctx, client, "atomic-success")
	defer cs.Close()

	acked := make(chan error, 1)
	cs.Send(frame, func(error) {
		cs.sendLock.Lock()
		generationStillOwned := cs.currentCancel != nil
		cs.sendLock.Unlock()
		if generationStillOwned {
			acked <- errors.New("ack ran before the successful generation was claimed")
			return
		}
		acked <- nil
	})
	attempt := <-oob.attempts
	attempt.callback(nil, nil)

	select {
	case err := <-acked:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("success callback did not run")
	}
}

func TestControlSyncOobSupersessionCancelsProductionRequestContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	makeFrame := func(index uint32) *protocol.Frame {
		frame, err := ToFrame(&protocol.SimpleMessage{MessageIndex: index}, DefaultProtocolVersion)
		if err != nil {
			t.Fatal(err)
		}
		return frame
	}
	oob := &contextualDelayedOobControl{
		attempts: make(chan contextualDelayedOobAttempt, 2),
	}
	client := newControlSyncOobTestClient(ctx, oob)
	defer client.Cancel()
	cs := NewControlSyncOob(ctx, client, "cancel-request")
	defer cs.Close()

	cs.Send(makeFrame(1), nil)
	first := <-oob.attempts
	cs.Send(makeFrame(2), nil)
	second := <-oob.attempts

	select {
	case <-first.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("superseding send did not cancel the old OOB request context")
	}
	select {
	case <-second.ctx.Done():
		t.Fatal("current OOB request was canceled with its predecessor")
	default:
	}

	second.callback(nil, nil)
	select {
	case <-second.ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("successful current request did not retire its context")
	}
}

// Close stops retrying
func TestControlSyncOobCloseStopsRetries(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping testing in short mode")
	}

	makeFrame := func(index uint32) *protocol.Frame {
		frame, err := ToFrame(&protocol.SimpleMessage{MessageIndex: index}, DefaultProtocolVersion)
		AssertEqual(t, err, nil)
		return frame
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	oob := &testOobControl{failUntil: 1 << 30} // always fail
	client := newControlSyncOobTestClient(ctx, oob)

	cs := NewControlSyncOob(ctx, client, "m1")
	cs.retryTimeout = 10 * time.Millisecond

	acked := make(chan struct{}, 1)
	cs.Send(makeFrame(1), func(err error) {
		select {
		case acked <- struct{}{}:
		default:
		}
	})

	// let it retry a few times
	select {
	case <-time.After(100 * time.Millisecond):
	}
	attemptsBefore := oob.attemptCount()
	AssertEqual(t, 0 < attemptsBefore, true)

	cs.Close()

	select {
	case <-time.After(300 * time.Millisecond):
	}
	// retries stop promptly (allow a couple of in-flight attempts at close,
	// but not the ~30 that would accrue over the wait if it kept retrying)
	attemptsAfter := oob.attemptCount()
	AssertEqual(t, attemptsAfter <= attemptsBefore+2, true)

	// it never acked (always failing)
	select {
	case <-acked:
		t.Fatal("send acked despite always failing")
	default:
	}
}
