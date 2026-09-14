package connect

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// --- the failure classifier -------------------------------------------------

func TestClassifyWindowFailure(t *testing.T) {
	// nil keeps the call site's fallback
	AssertEqual(t, classifyWindowFailure(nil, windowFailurePlatform), windowFailurePlatform)
	AssertEqual(t, classifyWindowFailure(nil, windowFailureProvider), windowFailureProvider)

	// a timeout stays whatever the call site says it is
	AssertEqual(t,
		classifyWindowFailure(errors.New("generator call abandoned after 20s"), windowFailurePlatform),
		windowFailurePlatform)

	// the message-revealed classes win over the fallback
	AssertEqual(t,
		classifyWindowFailure(errors.New("api error: 429 Too Many Requests"), windowFailurePlatform),
		windowFailureRateLimit)
	AssertEqual(t,
		classifyWindowFailure(errors.New("rate limit exceeded"), windowFailureProvider),
		windowFailureRateLimit)
	AssertEqual(t,
		classifyWindowFailure(errors.New("401 unauthorized"), windowFailurePlatform),
		windowFailureAuth)
	AssertEqual(t,
		classifyWindowFailure(errors.New("auth error timeout"), windowFailureProvider),
		windowFailureAuth)
}

// --- the dominance derivation ----------------------------------------------

func TestDeriveStallReason(t *testing.T) {
	// no failures: plain evaluating
	AssertEqual(t, deriveStallReason([windowFailureClassCount]int{}), WindowStallEvaluating)

	// dominant class wins
	var counts [windowFailureClassCount]int
	counts[windowFailureProvider] = 5
	counts[windowFailurePlatform] = 1
	AssertEqual(t, deriveStallReason(counts), WindowStallProvidersUnresponsive)

	counts = [windowFailureClassCount]int{}
	counts[windowFailurePlatform] = 3
	counts[windowFailureProvider] = 2
	AssertEqual(t, deriveStallReason(counts), WindowStallPlatformUnreachable)

	// ties break to the sharper diagnosis
	counts = [windowFailureClassCount]int{}
	counts[windowFailureProvider] = 2
	counts[windowFailurePlatform] = 2
	AssertEqual(t, deriveStallReason(counts), WindowStallPlatformUnreachable)

	counts = [windowFailureClassCount]int{}
	counts[windowFailureRateLimit] = 1
	AssertEqual(t, deriveStallReason(counts), WindowStallRateLimited)

	counts = [windowFailureClassCount]int{}
	counts[windowFailureAuth] = 1
	AssertEqual(t, deriveStallReason(counts), WindowStallAuthFailing)
}

func TestWindowFailureRecorderHorizon(t *testing.T) {
	recorder := &windowFailureRecorder{}
	now := time.Now()

	recorder.record(windowFailureProvider, now.Add(-2*windowFailureHorizon))
	recorder.record(windowFailureProvider, now.Add(-time.Second))
	recorder.record(windowFailurePlatform, now)

	counts := recorder.counts(now)
	// the entry past the horizon is trimmed
	AssertEqual(t, counts[windowFailureProvider], 1)
	AssertEqual(t, counts[windowFailurePlatform], 1)

	// nil recorder (a bare fixture window) is safe on both paths
	var bare *windowFailureRecorder
	bare.record(windowFailureProvider, now)
	AssertEqual(t, bare.counts(now), [windowFailureClassCount]int{})
}

// --- the monitor stall status ----------------------------------------------

// SetStallStatus dispatches on change only, and AddWindowExpandEvent carries
// the diagnosis through unchanged — two writers, one struct, no clobbering.
func TestMonitorSetStallStatus(t *testing.T) {
	monitor := NewRemoteUserNatMultiClientMonitorWithDefaults()

	events := make(chan *WindowExpandEvent, 16)
	sub := monitor.AddMonitorEventCallback(func(windowExpandEvent *WindowExpandEvent, providerEvents map[Id]*ProviderEvent, reset bool) {
		events <- windowExpandEvent
	})
	defer sub()

	// the initial state reads evaluating
	AssertEqual(t, monitor.WindowExpandEvent().Reason, WindowStallEvaluating)
	AssertEqual(t, monitor.WindowExpandEvent().Failed, false)

	AssertEqual(t, monitor.SetStallStatus(WindowStallProvidersUnresponsive, false), true)
	select {
	case event := <-events:
		AssertEqual(t, event.Reason, WindowStallProvidersUnresponsive)
		AssertEqual(t, event.Failed, false)
	case <-time.After(5 * time.Second):
		t.Fatal("no dispatch for the reason change")
	}

	// unchanged: no dispatch, and the call says so
	AssertEqual(t, monitor.SetStallStatus(WindowStallProvidersUnresponsive, false), false)

	// the size half preserves the diagnosis
	monitor.AddWindowExpandEvent(false, 4, false)
	windowExpandEvent := monitor.WindowExpandEvent()
	AssertEqual(t, windowExpandEvent.TargetSize, 4)
	AssertEqual(t, windowExpandEvent.Reason, WindowStallProvidersUnresponsive)

	// ...and the diagnosis half preserves the size
	AssertEqual(t, monitor.SetStallStatus(WindowStallProvidersUnresponsive, true), true)
	windowExpandEvent = monitor.WindowExpandEvent()
	AssertEqual(t, windowExpandEvent.TargetSize, 4)
	AssertEqual(t, windowExpandEvent.Failed, true)
}

// the merged monitor: reason merges by sharpness, and Failed only when every
// window that is actually trying has failed
func TestMergedMonitorStallStatus(t *testing.T) {
	quality := NewRemoteUserNatMultiClientMonitorWithDefaults()
	speed := NewRemoteUserNatMultiClientMonitorWithDefaults()
	merged := NewMergedMultiClientMonitor([]MultiClientMonitor{quality, speed})

	// both idle: evaluating, not failed
	AssertEqual(t, merged.WindowExpandEvent().Reason, WindowStallEvaluating)
	AssertEqual(t, merged.WindowExpandEvent().Failed, false)

	// the sharper reason wins across windows
	quality.SetStallStatus(WindowStallProvidersUnresponsive, false)
	speed.SetStallStatus(WindowStallPlatformUnreachable, false)
	AssertEqual(t, merged.WindowExpandEvent().Reason, WindowStallPlatformUnreachable)

	// one window failed while the other is still trying (target > 0): not failed
	quality.AddWindowExpandEvent(false, 4, false)
	speed.AddWindowExpandEvent(false, 1, false)
	quality.SetStallStatus(WindowStallProvidersUnresponsive, true)
	AssertEqual(t, merged.WindowExpandEvent().Failed, false)

	// both trying windows failed: failed
	speed.SetStallStatus(WindowStallPlatformUnreachable, true)
	AssertEqual(t, merged.WindowExpandEvent().Failed, true)

	// a disabled window (target 0, not failed) must not veto
	disabled := NewRemoteUserNatMultiClientMonitorWithDefaults()
	merged = NewMergedMultiClientMonitor([]MultiClientMonitor{quality, speed, disabled})
	AssertEqual(t, merged.WindowExpandEvent().Failed, true)

	// min satisfied anywhere overrides failed
	speed.AddWindowExpandEvent(true, 1, false)
	AssertEqual(t, merged.WindowExpandEvent().Failed, false)
}

// --- the outcome state machine ---------------------------------------------

func TestWindowOutcomeAction(t *testing.T) {
	deadline := 45 * time.Second
	rebuildDeadline := 45 * time.Second

	// disabled, unarmed, added, or already failed: never acts
	AssertEqual(t, windowOutcomeAction(time.Hour, 0, rebuildDeadline, true, false, false, false), outcomeNone)
	AssertEqual(t, windowOutcomeAction(time.Hour, deadline, rebuildDeadline, false, false, false, false), outcomeNone)
	AssertEqual(t, windowOutcomeAction(time.Hour, deadline, rebuildDeadline, true, true, false, false), outcomeNone)
	AssertEqual(t, windowOutcomeAction(time.Hour, deadline, rebuildDeadline, true, false, true, true), outcomeNone)

	// before the deadline: nothing
	AssertEqual(t, windowOutcomeAction(deadline-time.Second, deadline, rebuildDeadline, true, false, false, false), outcomeNone)
	// at the deadline with the rebuild unspent: rebuild — exactly once
	AssertEqual(t, windowOutcomeAction(deadline, deadline, rebuildDeadline, true, false, false, false), outcomeRebuild)
	// rebuilt, second span not yet expired (elapsed measures from the rebuild's
	// arm reset): nothing
	AssertEqual(t, windowOutcomeAction(rebuildDeadline-time.Second, deadline, rebuildDeadline, true, false, true, false), outcomeNone)
	// rebuilt and the second span expired: fail
	AssertEqual(t, windowOutcomeAction(rebuildDeadline, deadline, rebuildDeadline, true, false, true, false), outcomeFail)
	// rebuilt with the failed latch disabled: never fails
	AssertEqual(t, windowOutcomeAction(time.Hour, deadline, 0, true, false, true, false), outcomeNone)
}

func TestOutcomeWatchPollTimeout(t *testing.T) {
	resizeTimeout := 15 * time.Second
	// disabled idles at the resize cadence
	AssertEqual(t, outcomeWatchPollTimeout(0, resizeTimeout), resizeTimeout)
	// the default deadline polls at the 1s cap
	AssertEqual(t, outcomeWatchPollTimeout(45*time.Second, resizeTimeout), time.Second)
	// a short (test) deadline polls at a fraction of itself, floored
	AssertEqual(t, outcomeWatchPollTimeout(800*time.Millisecond, resizeTimeout), 100*time.Millisecond)
	AssertEqual(t, outcomeWatchPollTimeout(4*time.Second, resizeTimeout), 500*time.Millisecond)
}

// the settings pair rides ReliabilitySettings like the other runtime knobs, so
// it appears in the session banner and can be toggled from the developer menu
func TestWindowOutcomeDeadlineSettings(t *testing.T) {
	settings := DefaultMultiClientSettings()
	AssertEqual(t, settings.WindowOutcomeDeadline, 45*time.Second)
	AssertEqual(t, settings.WindowOutcomeRebuildDeadline, 45*time.Second)

	reliabilitySettings := ReliabilitySettingsFrom(settings)
	AssertEqual(t, reliabilitySettings.WindowOutcomeDeadline, settings.WindowOutcomeDeadline)
	AssertEqual(t, reliabilitySettings.WindowOutcomeRebuildDeadline, settings.WindowOutcomeRebuildDeadline)

	// zero-value-off, like the rest of the runtime knobs
	AssertEqual(t, ReliabilitySettingsFrom(nil).WindowOutcomeDeadline, time.Duration(0))

	// the session banner renders both (the checkpoint test greps these names)
	pairs := strings.Join(relSettingsPairs(reliabilitySettings), " ")
	AssertEqual(t, strings.Contains(pairs, "windowoutcomedeadline=45000"), true)
	AssertEqual(t, strings.Contains(pairs, "windowoutcomerebuilddeadline=45000"), true)
}

// outcomeTestWindow is a window with just enough wiring to drive the outcome
// transitions directly: a monitor, a recorder, a log, and live notify monitors.
func outcomeTestWindow(ctx context.Context, log *recordingLogger) *multiClientWindow {
	window := &multiClientWindow{
		ctx:                   ctx,
		log:                   log,
		windowType:            WindowTypeQuality,
		settings:              DefaultMultiClientSettings(),
		monitor:               NewRemoteUserNatMultiClientMonitorWithDefaults(),
		clients:               map[Id]*multiClientChannel{},
		generatorMonitor:      NewMonitor(),
		resizeMonitor:         NewMonitor(),
		failures:              &windowFailureRecorder{},
		createFailThrottle:    newLogThrottle(evaluationFailureLogInterval),
		pingFailThrottle:      newLogThrottle(evaluationFailureLogInterval),
		enumerateZeroThrottle: newLogThrottle(evaluationFailureLogInterval),
	}
	window.evalEpochCtx, window.evalEpochCancel = context.WithCancel(ctx)
	return window
}

// the rebuild: spends the one automatic rescue, cancels the evaluation epoch
// so in-flight candidates die fast, resets the deadline clock, and logs the
// [rel] line the checkpoint test greps
func TestWindowOutcomeRebuild(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := newRecordingLogger()
	window := outcomeTestWindow(ctx, log)

	window.armOutcome()
	oldEpoch := window.evalEpochContext()

	window.recordEvaluationFailure(windowFailurePlatform, errors.New("generator call abandoned after 20s"))
	window.rebuildWindow(45 * time.Second)

	// the epoch was cancelled and replaced
	select {
	case <-oldEpoch.Done():
	default:
		t.Fatal("the rebuild did not cancel the evaluation epoch")
	}
	newEpoch := window.evalEpochContext()
	select {
	case <-newEpoch.Done():
		t.Fatal("the fresh epoch is already cancelled")
	default:
	}

	// the state machine spent its one rebuild and reset the clock
	window.outcomeLock.Lock()
	rebuilt := window.outcomeRebuilt
	armTime := window.outcomeArmTime
	window.outcomeLock.Unlock()
	AssertEqual(t, rebuilt, true)
	AssertEqual(t, time.Since(armTime) < 10*time.Second, true)

	lines := log.linesWith("event=window_rebuild")
	if len(lines) != 1 {
		t.Fatalf("expected one window_rebuild line, got %v", lines)
	}
	AssertEqual(t, strings.Contains(lines[0], "reason=platform-unreachable"), true)
	AssertEqual(t, strings.Contains(lines[0], "window=quality"), true)
}

// the decision-to-action race: the watchdog reads `added` in its own critical
// section and acts outside it, so a provider can land in the gap. The rebuild
// and the fail must both revalidate under the lock and stand down — a rebuild
// that went ahead would cancel the epoch (killing the just-installed client)
// with the watchdog already permanently disarmed, and a fail that went ahead
// would latch a lie nothing is left to clear.
func TestWindowOutcomeRebuildAbortsAfterAdd(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := newRecordingLogger()
	window := outcomeTestWindow(ctx, log)

	window.armOutcome()
	epoch := window.evalEpochContext()

	// a provider lands between the watchdog's decision and the rebuild
	window.noteClientAdded(nil)
	window.rebuildWindow(45 * time.Second)

	// the epoch survives — the just-installed client is not cancelled
	select {
	case <-epoch.Done():
		t.Fatal("the aborted rebuild cancelled the evaluation epoch anyway")
	default:
	}
	window.outcomeLock.Lock()
	rebuilt := window.outcomeRebuilt
	window.outcomeLock.Unlock()
	AssertEqual(t, rebuilt, false)
	AssertEqual(t, len(log.linesWith("event=window_rebuild")), 0)

	// ...and the fail stands down the same way
	window.failOutcome(90 * time.Second)
	AssertEqual(t, window.monitor.WindowExpandEvent().Failed, false)
	AssertEqual(t, len(log.linesWith("event=window_failed")), 0)
}

// the failed latch: published to the monitor with the reason, logged once, and
// cleared by a provider landing
func TestWindowOutcomeFailAndRecover(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := newRecordingLogger()
	window := outcomeTestWindow(ctx, log)

	window.armOutcome()
	window.recordEvaluationFailure(windowFailureProvider, errors.New("ping timeout"))
	window.failOutcome(45 * time.Second)

	windowExpandEvent := window.monitor.WindowExpandEvent()
	AssertEqual(t, windowExpandEvent.Failed, true)
	AssertEqual(t, windowExpandEvent.Reason, WindowStallProvidersUnresponsive)

	lines := log.linesWith("event=window_failed")
	if len(lines) != 1 {
		t.Fatalf("expected one window_failed line, got %v", lines)
	}
	AssertEqual(t, strings.Contains(lines[0], "reason=providers-unresponsive"), true)

	// a provider landing clears the latch and disarms the watchdog
	window.noteClientAdded(nil)
	AssertEqual(t, window.monitor.WindowExpandEvent().Failed, false)
	window.outcomeLock.Lock()
	added := window.everAdded
	window.outcomeLock.Unlock()
	AssertEqual(t, added, true)
	AssertEqual(t, len(log.linesWith("event=window_recovered")), 1)

	// ...and the state machine never acts again
	AssertEqual(t,
		windowOutcomeAction(time.Hour, 45*time.Second, 45*time.Second, true, true, true, false),
		outcomeNone)
}

// outcomeRetryGenerator exposes each deterministic enumeration boundary while
// retaining the empty generator's cleanup behavior.
type outcomeRetryGenerator struct {
	testingEmptyMultiClientGenerator
	calls chan struct{}
}

// NextDestinations records one enumeration attempt and returns no providers,
// leaving the window parked on its already-captured generator notification.
func (self *outcomeRetryGenerator) NextDestinations(
	count int,
	excludeDestinations []MultiHopId,
	rankMode string,
) (map[MultiHopId]DestinationStats, error) {
	self.calls <- struct{}{}
	return map[MultiHopId]DestinationStats{}, nil
}

// TestWindowOutcomeFailureRetriesEnumerationUntilCanceled pins Failed as a
// status latch, never a terminal machinery state. Many consecutive empty
// provider passes under the failed latch must each accept another exact fill
// request under the same live evaluation epoch. Only explicit lifetime
// cancellation may terminate the enumerator.
func TestWindowOutcomeFailureRetriesEnumerationUntilCanceled(t *testing.T) {
	const retryCount = 64

	ctx, cancel := context.WithCancel(context.Background())
	log := newRecordingLogger()
	window := outcomeTestWindow(ctx, log)
	window.settings.WindowGeneratorTimeout = 0
	window.generator = &outcomeRetryGenerator{calls: make(chan struct{})}
	window.clientChannelArgs = make(chan *multiClientChannelArgs)
	epoch := window.evalEpochContext()
	resizeNotify := window.resizeMonitor.NotifyChannel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		window.randomEnumerateClientArgs()
	}()

	select {
	case <-window.generator.(*outcomeRetryGenerator).calls:
	case <-done:
		t.Fatal("window enumerator terminated before its first fill attempt")
	}
	window.failOutcome(45 * time.Second)
	if !window.monitor.WindowExpandEvent().Failed {
		t.Fatal("window did not publish the failed status latch")
	}
	select {
	case <-ctx.Done():
		t.Fatal("failed status canceled the window")
	case <-epoch.Done():
		t.Fatal("failed status canceled the evaluation epoch")
	case <-resizeNotify:
		t.Fatal("failed status closed an unrelated resize epoch")
	default:
	}

	for attempt := 1; attempt <= retryCount; attempt += 1 {
		window.generatorMonitor.NotifyAll()
		select {
		case <-window.generator.(*outcomeRetryGenerator).calls:
		case <-done:
			t.Fatalf("window enumerator terminally gave up after %d retries", attempt-1)
		}
		if !window.monitor.WindowExpandEvent().Failed {
			t.Fatalf("retry %d cleared the failure status without a provider", attempt)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("retry %d canceled the window", attempt)
		case <-epoch.Done():
			t.Fatalf("retry %d canceled the evaluation epoch", attempt)
		case <-done:
			t.Fatalf("window enumerator stopped after retry %d", attempt)
		default:
		}
	}
	window.resizeMonitor.NotifyAll()
	<-resizeNotify

	cancel()
	<-done
	if _, ok := <-window.clientChannelArgs; ok {
		t.Fatal("enumerator did not close its output after explicit cancellation")
	}
}

// outcomeEnumerationGenerator exposes one platform enumeration call at a
// time, so tests can choose owner cancellation or a genuine platform error.
type outcomeEnumerationGenerator struct {
	testingEmptyMultiClientGenerator
	entered chan struct{}
	results chan error
}

// Blocks at the exact platform enumeration boundary until the test chooses a
// result.
func (self *outcomeEnumerationGenerator) NextDestinations(
	count int,
	excludeDestinations []MultiHopId,
	rankMode string,
) (map[MultiHopId]DestinationStats, error) {
	self.entered <- struct{}{}
	return nil, <-self.results
}

// outcomeClientArgsGenerator exposes the client-args call after returning one
// synthetic destination from platform enumeration.
type outcomeClientArgsGenerator struct {
	testingEmptyMultiClientGenerator
	destination MultiHopId
	entered     chan struct{}
	results     chan error
}

// Supplies one candidate so the enumerator reaches client-args creation.
func (self *outcomeClientArgsGenerator) NextDestinations(
	count int,
	excludeDestinations []MultiHopId,
	rankMode string,
) (map[MultiHopId]DestinationStats, error) {
	return map[MultiHopId]DestinationStats{self.destination: {}}, nil
}

// Blocks at the exact platform client-mint boundary until the test chooses a
// result.
func (self *outcomeClientArgsGenerator) NewClientArgs() (*MultiClientGeneratorClientArgs, error) {
	self.entered <- struct{}{}
	return nil, <-self.results
}

// outcomeEnumeratorTestWindow wires the production enumeration loop without
// starting the rest of a multi-client window.
func outcomeEnumeratorTestWindow(
	ctx context.Context,
	log *recordingLogger,
	generator MultiClientGenerator,
) *multiClientWindow {
	window := outcomeTestWindow(ctx, log)
	window.generator = generator
	window.clientChannelArgs = make(chan *multiClientChannelArgs)
	window.settings.WindowGeneratorTimeout = time.Hour
	window.settings.WindowEnumerateErrorTimeout = 0
	return window
}

// TestWindowEnumerationCancellationIsNotPlatformFailure pins window teardown
// as lifecycle, not evidence that the platform was unreachable.
func TestWindowEnumerationCancellationIsNotPlatformFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	log := newRecordingLogger()
	generator := &outcomeEnumerationGenerator{
		entered: make(chan struct{}),
		results: make(chan error),
	}
	window := outcomeEnumeratorTestWindow(ctx, log, generator)
	done := make(chan struct{})
	go func() {
		defer close(done)
		window.randomEnumerateClientArgs()
	}()

	<-generator.entered
	cancel()
	<-done
	generator.results <- nil

	counts := window.failures.counts(time.Now())
	if counts != [windowFailureClassCount]int{} {
		t.Fatalf("teardown recorded window failures: %v", counts)
	}
	if got := window.monitor.WindowExpandEvent().Reason; got != WindowStallEvaluating {
		t.Fatalf("teardown stall reason=%q, want %q", got, WindowStallEvaluating)
	}
	if lines := log.linesWith("[multi]window enumerate error"); len(lines) != 0 {
		t.Fatalf("teardown logged an enumeration failure: %v", lines)
	}
	if lines := log.linesWith("event=window_stall"); len(lines) != 0 {
		t.Fatalf("teardown published a window stall: %v", lines)
	}
}

// TestWindowClientArgsCancellationIsNotPlatformFailure pins the same owner
// cancellation boundary after a destination was enumerated.
func TestWindowClientArgsCancellationIsNotPlatformFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	log := newRecordingLogger()
	generator := &outcomeClientArgsGenerator{
		destination: RequireMultiHopId(NewId()),
		entered:     make(chan struct{}),
		results:     make(chan error),
	}
	window := outcomeEnumeratorTestWindow(ctx, log, generator)
	done := make(chan struct{})
	go func() {
		defer close(done)
		window.randomEnumerateClientArgs()
	}()

	<-generator.entered
	cancel()
	<-done
	generator.results <- nil

	counts := window.failures.counts(time.Now())
	if counts != [windowFailureClassCount]int{} {
		t.Fatalf("teardown recorded window failures: %v", counts)
	}
	if got := window.monitor.WindowExpandEvent().Reason; got != WindowStallEvaluating {
		t.Fatalf("teardown stall reason=%q, want %q", got, WindowStallEvaluating)
	}
	if lines := log.linesWith("[multi]create client args error"); len(lines) != 0 {
		t.Fatalf("teardown logged a client-args failure: %v", lines)
	}
	if lines := log.linesWith("event=window_stall"); len(lines) != 0 {
		t.Fatalf("teardown published a window stall: %v", lines)
	}
}

// TestLiveWindowEnumerationErrorIsPlatformFailure preserves genuine platform
// evidence while the owning window context remains live.
func TestLiveWindowEnumerationErrorIsPlatformFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	log := newRecordingLogger()
	generator := &outcomeEnumerationGenerator{
		entered: make(chan struct{}),
		results: make(chan error),
	}
	window := outcomeEnumeratorTestWindow(ctx, log, generator)
	done := make(chan struct{})
	go func() {
		defer close(done)
		window.randomEnumerateClientArgs()
	}()

	<-generator.entered
	generator.results <- errors.New("synthetic platform timeout")
	<-generator.entered
	cancel()
	<-done
	generator.results <- nil

	counts := window.failures.counts(time.Now())
	if got := counts[windowFailurePlatform]; got != 1 {
		t.Fatalf("platform failure count=%d, want 1", got)
	}
	if got := window.monitor.WindowExpandEvent().Reason; got != WindowStallPlatformUnreachable {
		t.Fatalf("stall reason=%q, want %q", got, WindowStallPlatformUnreachable)
	}
	if lines := log.linesWith("[multi]window enumerate error"); len(lines) != 1 {
		t.Fatalf("enumeration failure lines=%d, want 1: %v", len(lines), lines)
	}
	if lines := log.linesWith("event=window_stall"); len(lines) != 1 {
		t.Fatalf("window-stall lines=%d, want 1: %v", len(lines), lines)
	}
}

// TestLiveWindowClientArgsErrorIsPlatformFailure preserves genuine client-mint
// evidence while the owning window context remains live.
func TestLiveWindowClientArgsErrorIsPlatformFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	log := newRecordingLogger()
	generator := &outcomeClientArgsGenerator{
		destination: RequireMultiHopId(NewId()),
		entered:     make(chan struct{}),
		results:     make(chan error),
	}
	window := outcomeEnumeratorTestWindow(ctx, log, generator)
	done := make(chan struct{})
	go func() {
		defer close(done)
		window.randomEnumerateClientArgs()
	}()

	<-generator.entered
	generator.results <- errors.New("synthetic client-args timeout")
	<-generator.entered
	cancel()
	<-done
	generator.results <- nil

	counts := window.failures.counts(time.Now())
	if got := counts[windowFailurePlatform]; got != 1 {
		t.Fatalf("platform failure count=%d, want 1", got)
	}
	if got := window.monitor.WindowExpandEvent().Reason; got != WindowStallPlatformUnreachable {
		t.Fatalf("stall reason=%q, want %q", got, WindowStallPlatformUnreachable)
	}
	if lines := log.linesWith("[multi]create client args error"); len(lines) != 1 {
		t.Fatalf("client-args failure lines=%d, want 1: %v", len(lines), lines)
	}
	if lines := log.linesWith("event=window_stall"); len(lines) != 1 {
		t.Fatalf("window-stall lines=%d, want 1: %v", len(lines), lines)
	}
}

// the stall transition is logged once per change through publishStallStatus
func TestWindowStallTransitionLogsOnce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	log := newRecordingLogger()
	window := outcomeTestWindow(ctx, log)

	window.recordEvaluationFailure(windowFailureProvider, nil)
	window.recordEvaluationFailure(windowFailureProvider, nil)
	window.recordEvaluationFailure(windowFailureProvider, nil)

	lines := log.linesWith("event=window_stall")
	if len(lines) != 1 {
		t.Fatalf("expected exactly one window_stall transition line, got %v", lines)
	}
	AssertEqual(t, strings.Contains(lines[0], "reason=providers-unresponsive"), true)
}

// the unconditional evaluation-failure lines carry the "(N suppressed)" tail
func TestSuppressedSuffix(t *testing.T) {
	AssertEqual(t, suppressedSuffix(0), "")
	AssertEqual(t, suppressedSuffix(-1), "")
	AssertEqual(t, suppressedSuffix(3), " (3 suppressed)")
}
