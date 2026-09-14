package connect

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// evaluationChannelCreationGenerator exposes the exact NewClient boundary so
// tests can choose caller cancellation or a returned provider error without
// clocks or scheduler ordering.
type evaluationChannelCreationGenerator struct {
	testingEmptyMultiClientGenerator
	entered      chan struct{}
	providerErrs chan error
	removals     atomic.Int32
}

// NewClient waits at the construction boundary until either owner cancellation
// or the test's provider result wins.
func (self *evaluationChannelCreationGenerator) NewClient(
	ctx context.Context,
	_ *MultiClientGeneratorClientArgs,
	_ *ClientSettings,
) (*Client, error) {
	close(self.entered)
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-self.providerErrs:
		return nil, err
	}
}

// RemoveClientArgs records that the failed candidate's generator ownership was
// returned exactly once.
func (self *evaluationChannelCreationGenerator) RemoveClientArgs(*MultiClientGeneratorClientArgs) {
	self.removals.Add(1)
}

// newEvaluationChannelCreationTestState builds only the window state consumed
// by the production failure-finalization path.
func newEvaluationChannelCreationTestState(
	ctx context.Context,
) (*multiClientWindow, *evaluationChannelCreationGenerator, *multiClientChannelArgs, *recordingLogger) {
	log := newRecordingLogger()
	generator := &evaluationChannelCreationGenerator{
		entered:      make(chan struct{}),
		providerErrs: make(chan error, 1),
	}
	settings := DefaultMultiClientSettings()
	settings.Log = log
	window := &multiClientWindow{
		ctx:                ctx,
		log:                log,
		generator:          generator,
		settings:           settings,
		monitor:            NewRemoteUserNatMultiClientMonitorWithDefaults(),
		failures:           &windowFailureRecorder{},
		createFailThrottle: newLogThrottle(evaluationFailureLogInterval),
	}
	args := &multiClientChannelArgs{
		MultiClientGeneratorClientArgs: MultiClientGeneratorClientArgs{ClientId: NewId()},
		Destination:                    RequireMultiHopId(NewId()),
	}
	return window, generator, args, log
}

// beginEvaluationChannelCreation runs the real constructor and exposes its
// result through a buffered handoff, so the test controls the only blocking
// boundary.
func beginEvaluationChannelCreation(
	ctx context.Context,
	generator MultiClientGenerator,
	args *multiClientChannelArgs,
	settings *MultiClientSettings,
) <-chan error {
	result := make(chan error, 1)
	go func() {
		_, err := newMultiClientChannel(
			ctx,
			args,
			generator,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			settings,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
			nil,
		)
		result <- err
	}()
	return result
}

// requireChannelCreationState waits for the terminal monitor event and checks
// the logging, failure-recorder, and generator-ownership effects together.
func requireChannelCreationState(
	t *testing.T,
	window *multiClientWindow,
	generator *evaluationChannelCreationGenerator,
	args *multiClientChannelArgs,
	log *recordingLogger,
	evaluationCtx context.Context,
	err error,
	wantProviderEvent bool,
	wantProviderFailures int,
) {
	t.Helper()
	events := make(chan ProviderState, 1)
	unsub := window.monitor.AddMonitorEventCallback(func(
		_ *WindowExpandEvent,
		providerEvents map[Id]*ProviderEvent,
		_ bool,
	) {
		if event := providerEvents[args.ClientId]; event != nil {
			events <- event.State
		}
	})
	defer unsub()

	providerFailure := window.recordChannelCreationFailure(evaluationCtx, args, err)
	window.generator.RemoveClientArgs(&args.MultiClientGeneratorClientArgs)
	if providerFailure {
		window.monitor.AddProviderEvent(args.ClientId, ProviderStateEvaluationFailed, args.Destination.Tail(), args.Location, args.IpFamily)
	}

	if wantProviderEvent {
		select {
		case state := <-events:
			if state != ProviderStateEvaluationFailed {
				t.Fatalf("provider state=%s, want %s", state, ProviderStateEvaluationFailed)
			}
		case <-time.After(time.Second):
			t.Fatal("terminal provider event was not delivered")
		}
	}
	if got := int(generator.removals.Load()); got != 1 {
		t.Fatalf("generator removals=%d, want 1", got)
	}
	counts := window.failures.counts(time.Now())
	if got := counts[windowFailureProvider]; got != wantProviderFailures {
		t.Fatalf("provider failure count=%d, want %d", got, wantProviderFailures)
	}
	wantLogs := wantProviderFailures
	if got := len(log.linesWith("[multi]create channel error")); got != wantLogs {
		t.Fatalf("create-channel logs=%d, want %d", got, wantLogs)
	}
}

// TestEvaluationCancellationBeforeChannelCreationIsNotProviderFailure pins an
// already-retired epoch as local lifecycle, not provider evidence.
func TestEvaluationCancellationBeforeChannelCreationIsNotProviderFailure(t *testing.T) {
	windowCtx, windowCancel := context.WithCancel(context.Background())
	defer windowCancel()
	evaluationCtx, evaluationCancel := context.WithCancel(windowCtx)
	evaluationCancel()
	window, generator, args, log := newEvaluationChannelCreationTestState(windowCtx)
	result := beginEvaluationChannelCreation(evaluationCtx, generator, args, window.settings)
	<-generator.entered
	err := <-result
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("constructor error=%v, want context canceled", err)
	}
	requireChannelCreationState(t, window, generator, args, log, evaluationCtx, err, false, 0)
}

// TestEvaluationCancellationDuringChannelCreationIsNotProviderFailure forces
// an epoch rebuild/retirement across the in-flight constructor boundary.
func TestEvaluationCancellationDuringChannelCreationIsNotProviderFailure(t *testing.T) {
	windowCtx, windowCancel := context.WithCancel(context.Background())
	defer windowCancel()
	evaluationCtx, evaluationCancel := context.WithCancel(windowCtx)
	window, generator, args, log := newEvaluationChannelCreationTestState(windowCtx)
	result := beginEvaluationChannelCreation(evaluationCtx, generator, args, window.settings)
	<-generator.entered
	evaluationCancel()
	err := <-result
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("constructor error=%v, want context canceled", err)
	}
	requireChannelCreationState(t, window, generator, args, log, evaluationCtx, err, false, 0)
}

// TestWindowRetirementDuringChannelCreationIsNotProviderFailure forces parent
// lifecycle cancellation across the same in-flight constructor boundary.
func TestWindowRetirementDuringChannelCreationIsNotProviderFailure(t *testing.T) {
	windowCtx, windowCancel := context.WithCancel(context.Background())
	window, generator, args, log := newEvaluationChannelCreationTestState(windowCtx)
	evaluationCtx, evaluationCancel := context.WithCancel(windowCtx)
	defer evaluationCancel()
	result := beginEvaluationChannelCreation(evaluationCtx, generator, args, window.settings)
	<-generator.entered
	windowCancel()
	err := <-result
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("constructor error=%v, want context canceled", err)
	}
	requireChannelCreationState(t, window, generator, args, log, evaluationCtx, err, false, 0)
}

// TestLiveEvaluationContextProviderFailureIsRecorded keeps a genuine provider
// construction failure observable and in the dominance recorder.
func TestLiveEvaluationContextProviderFailureIsRecorded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	window, generator, args, log := newEvaluationChannelCreationTestState(ctx)
	result := beginEvaluationChannelCreation(ctx, generator, args, window.settings)
	<-generator.entered
	generator.providerErrs <- errors.New("provider channel refused")
	err := <-result
	if err == nil || err.Error() != "provider channel refused" {
		t.Fatalf("constructor error=%v, want provider failure", err)
	}
	requireChannelCreationState(t, window, generator, args, log, ctx, err, true, 1)
}

// TestLiveEvaluationContextCancellationErrorsRemainProviderFailures proves an
// identically worded remote cancellation/deadline is not suppressed while the
// caller context remains live.
func TestLiveEvaluationContextCancellationErrorsRemainProviderFailures(t *testing.T) {
	for _, providerErr := range []error{context.Canceled, context.DeadlineExceeded} {
		ctx, cancel := context.WithCancel(context.Background())
		window, generator, args, log := newEvaluationChannelCreationTestState(ctx)
		result := beginEvaluationChannelCreation(ctx, generator, args, window.settings)
		<-generator.entered
		generator.providerErrs <- providerErr
		err := <-result
		if !errors.Is(err, providerErr) {
			cancel()
			t.Fatalf("constructor error=%v, want %v", err, providerErr)
		}
		requireChannelCreationState(t, window, generator, args, log, ctx, err, true, 1)
		cancel()
	}
}

// TestEvaluationEpochCancellationDoesNotBecomeProviderFailure reproduces the
// outcome-rebuild ordering from the field: rebuildWindow cancels the exact
// epoch that owns an in-flight ping, and its callback returns context.Canceled.
// That local lifecycle result must not select providers-unresponsive, while
// the independent zero-provider outcome remains terminal and visible.
func TestEvaluationEpochCancellationDoesNotBecomeProviderFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := newRecordingLogger()
	window := outcomeTestWindow(ctx, log)
	args := &multiClientChannelArgs{
		MultiClientGeneratorClientArgs: MultiClientGeneratorClientArgs{ClientId: NewId()},
		Destination:                    RequireMultiHopId(NewId()),
	}

	evaluationCtx := window.evalEpochContext()
	window.rebuildWindow(45 * time.Second)
	if !errors.Is(evaluationCtx.Err(), context.Canceled) {
		t.Fatalf("replaced evaluation epoch error=%v, want context canceled", evaluationCtx.Err())
	}
	if window.recordEvaluationPingFailure(evaluationCtx, args, context.Canceled) {
		t.Fatal("rebuild-owned ping cancellation was recorded as provider failure")
	}
	if got := window.failures.counts(time.Now())[windowFailureProvider]; got != 0 {
		t.Fatalf("provider failures=%d after rebuild-owned cancellation, want 0", got)
	}
	if lines := log.linesWith("[multi]evaluation ping error"); len(lines) != 0 {
		t.Fatalf("rebuild-owned cancellation emitted provider diagnostics: %v", lines)
	}

	window.failOutcome(45 * time.Second)
	if !window.monitor.WindowExpandEvent().Failed {
		t.Fatal("zero-provider outcome was hidden with the cancellation diagnostic")
	}
	lines := log.linesWith("event=window_failed")
	if len(lines) != 1 {
		t.Fatalf("zero-provider outcome lines=%d, want 1", len(lines))
	}
	if !strings.Contains(lines[0], "reason=evaluating") || strings.Contains(lines[0], "reason=providers-unresponsive") {
		t.Fatalf("terminal outcome inherited canceled-ping attribution: %q", lines[0])
	}
}

// The cancellation text is not sufficient on its own. A live evaluation
// context, or a nonmatching provider error observed after local cancellation,
// remains genuine provider evidence.
func TestEvaluationPingFailurePreservesLiveAndNonmatchingErrors(t *testing.T) {
	for _, test := range []struct {
		name      string
		cancelCtx bool
		err       error
	}{
		{name: "live identical cancellation", err: context.Canceled},
		{name: "canceled epoch nonmatching provider error", cancelCtx: true, err: errors.New("provider ping refused")},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			evaluationCtx, evaluationCancel := context.WithCancel(ctx)
			defer evaluationCancel()
			if test.cancelCtx {
				evaluationCancel()
			}
			log := newRecordingLogger()
			window := outcomeTestWindow(ctx, log)
			args := &multiClientChannelArgs{
				MultiClientGeneratorClientArgs: MultiClientGeneratorClientArgs{ClientId: NewId()},
				Destination:                    RequireMultiHopId(NewId()),
			}

			if !window.recordEvaluationPingFailure(evaluationCtx, args, test.err) {
				t.Fatal("genuine provider error was suppressed")
			}
			if got := window.failures.counts(time.Now())[windowFailureProvider]; got != 1 {
				t.Fatalf("provider failures=%d, want 1", got)
			}
			if lines := log.linesWith("[multi]evaluation ping error"); len(lines) != 1 {
				t.Fatalf("provider diagnostics=%d, want 1", len(lines))
			}
		})
	}
}
