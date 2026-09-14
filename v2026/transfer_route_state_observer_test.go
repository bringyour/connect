// This file pins the exact opt-in route-state generation stream used by
// integration tests to prove fast topology withdrawal and promotion.
package connect

import (
	"context"
	"sync"
	"testing"
	"time"
)

// A context-entry barrier makes a waiter's final pre-block step observable.
// The embedded context remains the liveness bound for a failed assertion.
type testingRouteStateObserverWaitContext struct {
	context.Context
	entered     chan struct{}
	enteredOnce sync.Once
}

// One asynchronous observer result keeps its state and terminal error paired.
type testingRouteStateObserverWaitResult struct {
	state TestingMultiRouteWriterRouteState
	err   error
}

// Done records that WaitAfter has passed its closed-state check and is
// evaluating the blocking select.
func (self *testingRouteStateObserverWaitContext) Done() <-chan struct{} {
	self.enteredOnce.Do(func() {
		close(self.entered)
	})
	return self.Context.Done()
}

// A consumer that starts after both mutations still observes 2 -> 1 -> 2;
// ticker polling could see only the identical endpoints and miss retirement.
func TestMultiRouteWriterRouteStateObserverRetainsFastTransitions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	routeManager := NewRouteManager(ctx, "route-state-observer")
	destination := DestinationId(NewId())
	writer := routeManager.OpenMultiRouteWriter(destination)
	defer routeManager.CloseMultiRouteWriter(writer)
	transportA := NewSendGatewayTransport()
	transportB := NewSendGatewayTransport()
	routeManager.UpdateTransport(transportA, []Route{make(chan []byte, 1)})
	routeManager.UpdateTransport(transportB, []Route{make(chan []byte, 1)})
	defer routeManager.RemoveTransport(transportA)
	defer routeManager.RemoveTransport(transportB)

	observer := TestingObserveMultiRouteWriterRouteState(writer)
	defer observer.Close()
	initial := observer.Snapshot()
	if initial.Generation != 0 || initial.ActiveRouteCount != 2 {
		t.Fatalf("initial route state=%+v, want generation 0 with two routes", initial)
	}

	routeManager.RemoveTransport(transportA)
	routeManager.UpdateTransport(transportA, []Route{make(chan []byte, 1)})
	retired, err := observer.WaitAfter(ctx, initial.Generation)
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, err := observer.WaitAfter(ctx, retired.Generation)
	if err != nil {
		t.Fatal(err)
	}
	if retired.Generation != 1 || retired.ActiveRouteCount != 1 {
		t.Fatalf("retired route state=%+v, want exact generation 1 with one route", retired)
	}
	if rebuilt.Generation != 2 || rebuilt.ActiveRouteCount != 2 {
		t.Fatalf("rebuilt route state=%+v, want exact generation 2 with two routes", rebuilt)
	}
}

// A count that matched before the barrier cannot satisfy readiness after it;
// the waiter must traverse the intervening withdrawal generation first.
func TestMultiRouteWriterRouteStateObserverRejectsStaleMatchingGeneration(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	routeManager := NewRouteManager(ctx, "route-state-observer")
	destination := DestinationId(NewId())
	writer := routeManager.OpenMultiRouteWriter(destination)
	defer routeManager.CloseMultiRouteWriter(writer)
	transportA := NewSendGatewayTransport()
	transportB := NewSendGatewayTransport()
	routeManager.UpdateTransport(transportA, []Route{make(chan []byte, 1)})
	routeManager.UpdateTransport(transportB, []Route{make(chan []byte, 1)})
	defer routeManager.RemoveTransport(transportA)
	defer routeManager.RemoveTransport(transportB)

	observer := TestingObserveMultiRouteWriterRouteState(writer)
	defer observer.Close()
	barrier := observer.Snapshot()
	routeManager.RemoveTransport(transportA)
	routeManager.UpdateTransport(transportA, []Route{make(chan []byte, 1)})
	rebuilt, err := observer.WaitForActiveRouteCountAfter(
		ctx,
		barrier.Generation,
		barrier.ActiveRouteCount,
	)
	if err != nil {
		t.Fatal(err)
	}
	if rebuilt.Generation != barrier.Generation+2 || rebuilt.ActiveRouteCount != 2 {
		t.Fatalf("post-barrier matching state=%+v, stale barrier=%+v", rebuilt, barrier)
	}
}

// Ordinary route publication does not allocate or retain testing observer
// state when no observer has been installed.
func TestMultiRouteWriterRouteStateObserverAbsentStorageStaysNil(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	routeManager := NewRouteManager(ctx, "route-state-observer-absent")
	destination := DestinationId(NewId())
	writer := routeManager.OpenMultiRouteWriter(destination)
	defer routeManager.CloseMultiRouteWriter(writer)
	selector := writer.(*MultiRouteSelector)

	initiallyNil := func() bool {
		selector.mutex.Lock()
		defer selector.mutex.Unlock()
		return selector.testingRouteStateObservers == nil
	}()
	if !initiallyNil {
		t.Fatal("testing observer storage initialized without an observer")
	}

	transport := NewSendGatewayTransport()
	routeManager.UpdateTransport(transport, []Route{make(chan []byte, 1)})
	routeManager.RemoveTransport(transport)

	finallyNil := func() bool {
		selector.mutex.Lock()
		defer selector.mutex.Unlock()
		return selector.testingRouteStateObservers == nil
	}()
	if !finallyNil {
		t.Fatal("ordinary route publication retained testing observer storage")
	}
}

// Closing one observer removes only its event stream; a second observer stays
// installed and receives the next exact route generation.
func TestMultiRouteWriterRouteStateObserversCloseIndependently(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	routeManager := NewRouteManager(ctx, "route-state-observer-independent")
	destination := DestinationId(NewId())
	writer := routeManager.OpenMultiRouteWriter(destination)
	defer routeManager.CloseMultiRouteWriter(writer)
	selector := writer.(*MultiRouteSelector)
	firstObserver := TestingObserveMultiRouteWriterRouteState(writer)
	defer firstObserver.Close()
	secondObserver := TestingObserveMultiRouteWriterRouteState(writer)
	defer secondObserver.Close()
	barrier := secondObserver.Snapshot()
	waitContext := &testingRouteStateObserverWaitContext{
		Context: ctx,
		entered: make(chan struct{}),
	}
	waitResultChannel := make(chan testingRouteStateObserverWaitResult, 1)
	go func() {
		state, err := secondObserver.WaitAfter(waitContext, barrier.Generation)
		waitResultChannel <- testingRouteStateObserverWaitResult{state: state, err: err}
	}()

	select {
	case <-waitContext.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	firstObserver.Close()
	var firstInstalled bool
	var secondInstalled bool
	var observerCount int
	func() {
		selector.mutex.Lock()
		defer selector.mutex.Unlock()
		firstInstalled = selector.testingRouteStateObservers[firstObserver]
		secondInstalled = selector.testingRouteStateObservers[secondObserver]
		observerCount = len(selector.testingRouteStateObservers)
	}()
	if firstInstalled || !secondInstalled || observerCount != 1 {
		t.Fatalf(
			"observer storage after first close: first=%t second=%t count=%d",
			firstInstalled,
			secondInstalled,
			observerCount,
		)
	}

	transport := NewSendGatewayTransport()
	routeManager.UpdateTransport(transport, []Route{make(chan []byte, 1)})
	defer routeManager.RemoveTransport(transport)
	select {
	case result := <-waitResultChannel:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.state.Generation != barrier.Generation+1 || result.state.ActiveRouteCount != 1 {
			t.Fatalf("second observer state=%+v, barrier=%+v", result.state, barrier)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}

// Closing the final observer restores the selector's production nil state;
// repeated close calls remain idempotent.
func TestMultiRouteWriterRouteStateObserverFinalCloseRestoresNil(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	routeManager := NewRouteManager(ctx, "route-state-observer-final-close")
	destination := DestinationId(NewId())
	writer := routeManager.OpenMultiRouteWriter(destination)
	defer routeManager.CloseMultiRouteWriter(writer)
	selector := writer.(*MultiRouteSelector)
	firstObserver := TestingObserveMultiRouteWriterRouteState(writer)
	secondObserver := TestingObserveMultiRouteWriterRouteState(writer)

	firstObserver.Close()
	firstObserver.Close()
	secondObserver.Close()
	secondObserver.Close()

	finallyNil := func() bool {
		selector.mutex.Lock()
		defer selector.mutex.Unlock()
		return selector.testingRouteStateObservers == nil
	}()
	if !finallyNil {
		t.Fatal("final observer close did not restore nil testing storage")
	}
}

// Closing an observer after a waiter reaches its blocking select closes the
// waiter's exact tail event and returns the terminal close error.
func TestMultiRouteWriterRouteStateObserverCloseWakesWaiter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	routeManager := NewRouteManager(ctx, "route-state-observer-close-waiter")
	destination := DestinationId(NewId())
	writer := routeManager.OpenMultiRouteWriter(destination)
	defer routeManager.CloseMultiRouteWriter(writer)
	observer := TestingObserveMultiRouteWriterRouteState(writer)
	defer observer.Close()
	barrier := observer.Snapshot()
	waitContext := &testingRouteStateObserverWaitContext{
		Context: ctx,
		entered: make(chan struct{}),
	}
	waitError := make(chan error, 1)
	go func() {
		_, err := observer.WaitAfter(waitContext, barrier.Generation)
		waitError <- err
	}()

	select {
	case <-waitContext.entered:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	observer.Close()
	select {
	case err := <-waitError:
		if err == nil || err.Error() != "route-state observer closed" {
			t.Fatalf("wait error=%v, want route-state observer closed", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
