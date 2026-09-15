package connect

// Tests for the window generator deadline (ported as a concept from upstream
// main e05ecee): calls into the platform-backed MultiClientGenerator are
// wrapped with a deadline so a hung platform API can never wedge the window's
// enumerate/expand machinery. The generator interface takes no context, so
// past the deadline the call is abandoned, and the abandonment contract is
// what these tests pin: exactly one result per call, the waiter returns
// promptly, and a late result is either discarded (side-effect-free calls) or
// routed to the same RemoveClientArgs cleanup the decline paths use
// (NewClientArgs creates a platform-side network client).

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// TestGeneratorDeadlineDefault: 20s by default; 0 must remain expressible as
// "no deadline" (the pre-change trust-the-API behavior).
func TestGeneratorDeadlineDefault(t *testing.T) {
	settings := DefaultMultiClientSettings()
	if settings.WindowGeneratorTimeout != 20*time.Second {
		t.Errorf("WindowGeneratorTimeout default = %v, want 20s", settings.WindowGeneratorTimeout)
	}
}

func TestGeneratorDeadlinePassesThroughFastCall(t *testing.T) {
	ctx := context.Background()

	value, err := windowGeneratorCall(
		ctx,
		time.Second,
		func() (int, error) {
			return 42, nil
		},
		nil,
	)
	if err != nil {
		t.Fatalf("fast call errored: %v", err)
	}
	if value != 42 {
		t.Fatalf("fast call value = %d, want 42", value)
	}

	callErr := fmt.Errorf("api error")
	_, err = windowGeneratorCall(
		ctx,
		time.Second,
		func() (int, error) {
			return 0, callErr
		},
		nil,
	)
	if err != callErr {
		t.Fatalf("fast call error = %v, want the call's own error", err)
	}
}

func TestGeneratorDeadlineZeroTrustsApi(t *testing.T) {
	// timeout <= 0 is a direct call: no deadline, no abandonment
	value, err := windowGeneratorCall(
		context.Background(),
		0,
		func() (string, error) {
			return "direct", nil
		},
		nil,
	)
	if err != nil || value != "direct" {
		t.Fatalf("zero-timeout call = (%q, %v), want direct pass-through", value, err)
	}
}

func TestGeneratorDeadlineAbandonsHungCall(t *testing.T) {
	release := make(chan int)
	late := make(chan int, 1)

	startTime := time.Now()
	_, err := windowGeneratorCall(
		context.Background(),
		50*time.Millisecond,
		func() (int, error) {
			// a hung platform API: blocks until the test releases it
			return <-release, nil
		},
		func(value int, err error) {
			if err == nil {
				late <- value
			}
		},
	)
	if err == nil {
		t.Fatal("the hung call did not abandon")
	}
	if elapsed := time.Now().Sub(startTime); 5*time.Second < elapsed {
		t.Fatalf("abandon took %v; the deadline did not bound the wait", elapsed)
	}

	// the underlying call completes late: its result must reach the late
	// route, not vanish (this is what lets NewClientArgs clean up)
	release <- 7
	select {
	case value := <-late:
		if value != 7 {
			t.Fatalf("late value = %d, want 7", value)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the late result never reached the lateResult route")
	}
}

func TestGeneratorDeadlinePanicBecomesError(t *testing.T) {
	// a panicking generator produces an error result promptly — the waiter
	// must not be left to the deadline, and nothing may park forever
	startTime := time.Now()
	_, err := windowGeneratorCall(
		context.Background(),
		30*time.Second,
		func() (int, error) {
			panic(fmt.Errorf("generator exploded"))
		},
		nil,
	)
	if err == nil {
		t.Fatal("a panicking call returned no error")
	}
	if elapsed := time.Now().Sub(startTime); 5*time.Second < elapsed {
		t.Fatalf("the panic result took %v; it must not wait out the deadline", elapsed)
	}
}

func TestGeneratorDeadlineCanceledContextAbandons(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	release := make(chan struct{})
	defer close(release)
	startTime := time.Now()
	_, err := windowGeneratorCall(
		ctx,
		30*time.Second,
		func() (int, error) {
			<-release
			return 0, nil
		},
		nil,
	)
	if err == nil {
		t.Fatal("a canceled context did not abandon the call")
	}
	if elapsed := time.Now().Sub(startTime); 5*time.Second < elapsed {
		t.Fatalf("ctx abandon took %v, want prompt", elapsed)
	}
}

// generatorDeadlineRecordingGenerator records RemoveClientArgs calls; the rest
// is the empty generator.
type generatorDeadlineRecordingGenerator struct {
	testingEmptyMultiClientGenerator

	stateLock sync.Mutex
	removed   []*MultiClientGeneratorClientArgs
	notify    chan struct{}
}

func (self *generatorDeadlineRecordingGenerator) RemoveClientArgs(args *MultiClientGeneratorClientArgs) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.removed = append(self.removed, args)
	if self.notify != nil {
		select {
		case self.notify <- struct{}{}:
		default:
		}
	}
}

// TestGeneratorDeadlineLateClientArgsRoutedToRemove: an abandoned
// NewClientArgs that completes late created a platform-side network client
// nothing will ever use. Its late value must flow to RemoveClientArgs — the
// same cleanup the decline paths use — so the client is deleted instead of
// leaking until server-side idle reap.
func TestGeneratorDeadlineLateClientArgsRoutedToRemove(t *testing.T) {
	generator := &generatorDeadlineRecordingGenerator{
		notify: make(chan struct{}, 1),
	}
	window := &multiClientWindow{
		generator: generator,
	}

	lateArgs := &MultiClientGeneratorClientArgs{
		ClientId: NewId(),
	}
	release := make(chan struct{})
	_, err := windowGeneratorCall(
		context.Background(),
		50*time.Millisecond,
		func() (*MultiClientGeneratorClientArgs, error) {
			<-release
			return lateArgs, nil
		},
		window.removeLateClientArgs,
	)
	if err == nil {
		t.Fatal("the hung NewClientArgs did not abandon")
	}

	// the platform API finally answers, with a client nothing will use
	close(release)
	select {
	case <-generator.notify:
	case <-time.After(5 * time.Second):
		t.Fatal("the late client args never reached RemoveClientArgs")
	}
	generator.stateLock.Lock()
	defer generator.stateLock.Unlock()
	if len(generator.removed) != 1 || generator.removed[0] != lateArgs {
		t.Fatalf("RemoveClientArgs saw %v, want exactly the late args", generator.removed)
	}

	// an abandoned call that fails late has nothing to clean up
	failWindow := &multiClientWindow{generator: generator}
	failRelease := make(chan struct{})
	_, err = windowGeneratorCall(
		context.Background(),
		50*time.Millisecond,
		func() (*MultiClientGeneratorClientArgs, error) {
			<-failRelease
			return nil, fmt.Errorf("late failure")
		},
		failWindow.removeLateClientArgs,
	)
	if err == nil {
		t.Fatal("the hung NewClientArgs did not abandon")
	}
	close(failRelease)
	select {
	case <-generator.notify:
		t.Fatal("a late failure must not reach RemoveClientArgs")
	case <-time.After(200 * time.Millisecond):
	}
}
