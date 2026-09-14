// This file pins expansion-pass ownership across delayed initial-ping
// callbacks. A returned pass cannot add a client during a later resize pass.
package connect

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

const (
	expandLifecycleWriteTimeout   = 11 * time.Second
	expandLifecyclePingTimeout    = 17 * time.Second
	expandLifecycleRequestTimeout = 7 * time.Second
)

// TestExpandCandidateWithinAcquisitionDeadline pins the inclusive acquisition
// boundary without relying on scheduler timing.
func TestExpandCandidateWithinAcquisitionDeadline(t *testing.T) {
	deadline := time.Unix(0, 50)
	for _, test := range []struct {
		name          string
		candidateTime time.Time
		want          bool
	}{
		{
			name:          "before",
			candidateTime: time.Unix(0, 49),
			want:          true,
		},
		{
			name:          "at boundary",
			candidateTime: deadline,
			want:          true,
		},
		{
			name:          "after",
			candidateTime: time.Unix(0, 51),
			want:          false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := expandCandidateWithinAcquisitionDeadline(test.candidateTime, deadline); got != test.want {
				t.Fatalf("candidate accepted=%t, want %t", got, test.want)
			}
		})
	}
}

// TestMultiClientExpandDeadlinesPreserveBothPhaseBudgets prevents the
// acquisition deadline from silently replacing the initial-ping deadline.
func TestMultiClientExpandDeadlinesPreserveBothPhaseBudgets(t *testing.T) {
	startTime := time.Unix(0, 100)
	requestEndTime, passEndTime := multiClientExpandDeadlines(
		startTime,
		expandLifecycleRequestTimeout,
		expandLifecyclePingTimeout,
	)
	if got := requestEndTime.Sub(startTime); got != expandLifecycleRequestTimeout {
		t.Fatalf("candidate-acquisition budget=%s, want %s", got, expandLifecycleRequestTimeout)
	}
	if got := passEndTime.Sub(requestEndTime); got != expandLifecyclePingTimeout {
		t.Fatalf("initial-ping budget=%s, want %s", got, expandLifecyclePingTimeout)
	}
}

func TestEvaluationBudgetDeadlineOwnershipExcludesLifecycleCancellation(t *testing.T) {
	windowCtx, cancelWindow := context.WithCancel(context.Background())
	evaluationCtx, cancelEvaluation := context.WithCancel(windowCtx)

	if !evaluationBudgetDeadlineOwned(true, windowCtx, evaluationCtx) {
		t.Fatal("a natural live-window deadline lost ownership")
	}
	if evaluationBudgetDeadlineOwned(false, windowCtx, evaluationCtx) {
		t.Fatal("an ordinary pass return claimed deadline ownership")
	}

	cancelEvaluation()
	if evaluationBudgetDeadlineOwned(true, windowCtx, evaluationCtx) {
		t.Fatal("an evaluation-epoch rebuild became a provider deadline")
	}

	retiredWindowCtx, retireWindow := context.WithCancel(context.Background())
	retiredEvaluationCtx, cancelRetiredEvaluation := context.WithCancel(retiredWindowCtx)
	defer cancelRetiredEvaluation()
	retireWindow()
	if evaluationBudgetDeadlineOwned(true, retiredWindowCtx, retiredEvaluationCtx) {
		t.Fatal("window retirement became a provider deadline")
	}

	cancelWindow()
}

// multiClientExpandLifecycleFixture owns one held initial-ping callback and
// exposes channel barriers for every acquisition and terminal transition.
type multiClientExpandLifecycleFixture struct {
	waitCtx           context.Context
	cancelWindow      context.CancelFunc
	cancelEvaluation  context.CancelFunc
	log               *recordingLogger
	window            *multiClientWindow
	argsRemoved       chan struct{}
	clientRemoved     chan struct{}
	pingResultEntered chan struct{}
	releasePingResult chan struct{}
	pingResultDone    chan struct{}
	releaseOnce       sync.Once
}

// multiClientExpandLifecycleGenerator makes the fixture exercise the ordinary
// non-fixed candidate pool without changing the shared test generator.
type multiClientExpandLifecycleGenerator struct {
	*TestMultiClientGenerator
}

// FixedDestinationSize selects ordinary dynamic-destination expansion.
func (self *multiClientExpandLifecycleGenerator) FixedDestinationSize() (int, bool) {
	return 0, false
}

// newMultiClientExpandLifecycleFixture constructs one fully joined synthetic
// provider and records both pre-ownership and channel-owned cleanup paths.
func newMultiClientExpandLifecycleFixture(t *testing.T) *multiClientExpandLifecycleFixture {
	t.Helper()
	waitCtx, cancelWait := context.WithTimeout(t.Context(), 10*time.Second)
	windowCtx, cancelWindow := context.WithCancel(waitCtx)
	log := newRecordingLogger()

	providerSettings := DefaultClientSettings()
	providerSettings.Log = NewNoopLogger()
	providerClient := NewClient(
		waitCtx,
		NewId(),
		NewNoContractClientOob(),
		providerSettings,
	)
	providerLocalNat := NewLocalUserNatWithDefaults(waitCtx, "expand-lifecycle-provider")
	provider := NewRemoteUserNatProvider(
		providerClient,
		providerLocalNat,
		DefaultRemoteUserNatProviderSettings(),
	)
	t.Cleanup(func() {
		provider.Close()
		providerLocalNat.Close()
		providerClient.Cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := providerClient.CloseAndWait(closeCtx); err != nil {
			t.Errorf("join expand-lifecycle provider client: %v", err)
		}
	})

	argsRemoved := make(chan struct{})
	clientRemoved := make(chan struct{})
	var argsRemovedOnce sync.Once
	var clientRemovedOnce sync.Once
	generator := testMultiClientGenerator(providerClient)
	generator.removeClientArgs = func(*MultiClientGeneratorClientArgs) {
		argsRemovedOnce.Do(func() {
			close(argsRemoved)
		})
	}
	originalRemoveClientWithArgs := generator.removeClientWithArgs
	generator.removeClientWithArgs = func(
		client *Client,
		args *MultiClientGeneratorClientArgs,
	) {
		originalRemoveClientWithArgs(client, args)
		clientRemovedOnce.Do(func() {
			close(clientRemoved)
		})
	}
	var generatedClientLock sync.Mutex
	var generatedClient *Client
	originalNewClient := generator.newClient
	generator.newClient = func(
		clientCtx context.Context,
		args *MultiClientGeneratorClientArgs,
		clientSettings *ClientSettings,
	) (*Client, error) {
		client, err := originalNewClient(clientCtx, args, clientSettings)
		generatedClientLock.Lock()
		generatedClient = client
		generatedClientLock.Unlock()
		return client, err
	}
	t.Cleanup(func() {
		generatedClientLock.Lock()
		client := generatedClient
		generatedClientLock.Unlock()
		if client == nil {
			return
		}
		client.Cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer closeCancel()
		if err := client.CloseAndWait(closeCtx); err != nil {
			t.Errorf("join expand-lifecycle client: %v", err)
		}
	})

	settings := DefaultMultiClientSettings()
	settings.Log = log
	settings.PingWriteTimeout = expandLifecycleWriteTimeout
	settings.PingTimeout = expandLifecyclePingTimeout
	settings.WindowExpandTimeout = expandLifecycleRequestTimeout
	settings.EvaluationPoolMultiple = 1

	pingResultEntered := make(chan struct{})
	releasePingResult := make(chan struct{})
	pingResultDone := make(chan struct{})
	var pingResultEnteredOnce sync.Once
	var pingResultDoneOnce sync.Once
	window := &multiClientWindow{
		ctx:                          windowCtx,
		cancel:                       cancelWindow,
		log:                          log,
		generator:                    &multiClientExpandLifecycleGenerator{generator},
		clientReceivePacketCallback:  func(*multiClientChannel, TransferPath, protocol.ProvideMode, TransportType, *IpPath, []byte) {},
		clientReceivePacketsCallback: nil,
		ingressSecurityPolicy:        DefaultSecurityPolicy(windowCtx),
		windowType:                   WindowTypeQuality,
		settings:                     settings,
		clientChannelArgs:            make(chan *multiClientChannelArgs, 1),
		monitor:                      NewRemoteUserNatMultiClientMonitor(&settings.RemoteUserNatMultiClientMonitorSettings),
		contractStatusCallbacks:      NewCallbackList[*contractStatusCallbackWorker](),
		contractStatsCallbacks:       NewCallbackList[ContractStatsFunction](),
		peerIdentityChangeCallbacks:  NewCallbackList[func()](),
		clients:                      map[Id]*multiClientChannel{},
		generatorMonitor:             NewMonitor(),
		resizeMonitor:                NewMonitor(),
		failures:                     &windowFailureRecorder{},
		pingFailThrottle:             newLogThrottle(evaluationFailureLogInterval),
		budgetFailThrottle:           newLogThrottle(evaluationFailureLogInterval),
		beforeExpandPingResultForTest: func() {
			pingResultEnteredOnce.Do(func() {
				close(pingResultEntered)
			})
			<-releasePingResult
		},
		afterExpandPingResultForTest: func() {
			pingResultDoneOnce.Do(func() {
				close(pingResultDone)
			})
		},
	}
	evaluationCtx, cancelEvaluation := context.WithCancel(windowCtx)
	window.evalEpochCtx = evaluationCtx
	window.evalEpochCancel = cancelEvaluation

	clientArgs, err := generator.NewClientArgs()
	if err != nil {
		t.Fatal(err)
	}
	window.clientChannelArgs <- &multiClientChannelArgs{
		MultiClientGeneratorClientArgs: *clientArgs,
		Destination:                    RequireMultiHopId(providerClient.ClientId()),
		DestinationStats:               DestinationStats{},
	}

	fixture := &multiClientExpandLifecycleFixture{
		waitCtx:           waitCtx,
		cancelWindow:      cancelWindow,
		cancelEvaluation:  cancelEvaluation,
		log:               log,
		window:            window,
		argsRemoved:       argsRemoved,
		clientRemoved:     clientRemoved,
		pingResultEntered: pingResultEntered,
		releasePingResult: releasePingResult,
		pingResultDone:    pingResultDone,
	}
	t.Cleanup(func() {
		fixture.releasePing()
		cancelEvaluation()
		cancelWindow()
		cancelWait()
	})
	return fixture
}

// start begins the single-candidate expansion pass under test.
func (self *multiClientExpandLifecycleFixture) start() <-chan int {
	expandDone := make(chan int, 1)
	go func() {
		expandDone <- self.window.expand(
			WindowSizeSettings{WindowSizeMin: 1, WindowSizeMax: 1, WindowSizeHardMax: 1},
			0,
			0,
			1,
			1,
			1,
			0,
		)
	}()
	return expandDone
}

// wait joins one deterministic fixture transition under the fixture safety
// context.
func (self *multiClientExpandLifecycleFixture) wait(t *testing.T, name string, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-self.waitCtx.Done():
		t.Fatalf("wait for %s: %v", name, self.waitCtx.Err())
	}
}

// send advances one deterministic fixture transition without a wall-clock
// sleep.
func (self *multiClientExpandLifecycleFixture) send(t *testing.T, name string, signal chan<- struct{}) {
	t.Helper()
	select {
	case signal <- struct{}{}:
	case <-self.waitCtx.Done():
		t.Fatalf("send %s: %v", name, self.waitCtx.Err())
	}
}

// result joins the expansion pass and returns its admitted-candidate count.
func (self *multiClientExpandLifecycleFixture) result(t *testing.T, expandDone <-chan int) int {
	t.Helper()
	select {
	case result := <-expandDone:
		return result
	case <-self.waitCtx.Done():
		t.Fatalf("finish expand pass: %v", self.waitCtx.Err())
		return 0
	}
}

// releasePing lets the held initial-ping callback cross its selected boundary
// exactly once.
func (self *multiClientExpandLifecycleFixture) releasePing() {
	self.releaseOnce.Do(func() {
		close(self.releasePingResult)
	})
}

// clientCount reads the installed window population under its owning lock.
func (self *multiClientExpandLifecycleFixture) clientCount() int {
	self.window.stateLock.Lock()
	defer self.window.stateLock.Unlock()
	return len(self.window.clients)
}

// assertNoDirectArgsRemoval proves that a constructed channel retained
// ownership of its generator args through joined cleanup.
func (self *multiClientExpandLifecycleFixture) assertNoDirectArgsRemoval(t *testing.T) {
	t.Helper()
	select {
	case <-self.argsRemoved:
		t.Fatal("constructed channel cleanup bypassed RemoveClientWithArgs")
	default:
	}
}

// A candidate already evaluating when acquisition ends remains owned by that
// pass and can be admitted when its healthy result arrives. Before the phase
// split, the same acquisition signal terminated the pass and canceled the
// candidate before its configured initial-ping budget was available.
func TestMultiClientExpandRetainsPingAfterAcquisitionDeadline(t *testing.T) {
	fixture := newMultiClientExpandLifecycleFixture(t)
	fixture.window.settings.EvaluationPoolMultiple = 2
	finishRequests := make(chan struct{})
	fixture.window.finishExpandRequestsForTest = finishRequests

	expandDone := fixture.start()
	fixture.wait(t, "held initial-ping result", fixture.pingResultEntered)
	fixture.send(t, "candidate-acquisition deadline", finishRequests)
	select {
	case result := <-expandDone:
		t.Fatalf("expand returned %d before its owned ping resolved", result)
	default:
	}

	fixture.releasePing()
	fixture.wait(t, "healthy initial-ping callback", fixture.pingResultDone)
	if got := fixture.result(t, expandDone); got != 1 {
		t.Fatalf("healthy ping admissions=%d, want 1", got)
	}
	if got := fixture.clientCount(); got != 1 {
		t.Fatalf("installed clients=%d, want 1", got)
	}
	if got := fixture.window.failures.counts(time.Now())[windowFailureProvider]; got != 0 {
		t.Fatalf("provider failures after healthy ping=%d, want 0", got)
	}
	if lines := fixture.log.linesWith("event=evaluation_budget_exhausted"); len(lines) != 0 {
		t.Fatalf("healthy owned ping emitted budget expiry: %v", lines)
	}
	fixture.assertNoDirectArgsRemoval(t)
}

// PingTimeout, rather than the earlier acquisition deadline, owns an
// unanswered candidate. The injected timeout channel exercises the exact
// production branch without sleeping for a wall-clock duration.
func TestMultiClientExpandPingTimeoutOwnsFailure(t *testing.T) {
	fixture := newMultiClientExpandLifecycleFixture(t)
	expirePing := make(chan struct{})
	fixture.window.expireExpandPingForTest = expirePing

	expandDone := fixture.start()
	fixture.wait(t, "held initial-ping result", fixture.pingResultEntered)
	fixture.send(t, "initial-ping timeout", expirePing)
	if got := fixture.result(t, expandDone); got != 0 {
		t.Fatalf("timed-out ping admissions=%d, want 0", got)
	}
	if got := fixture.window.failures.counts(time.Now())[windowFailureProvider]; got != 1 {
		t.Fatalf("provider failures at ping timeout=%d, want 1", got)
	}
	if lines := fixture.log.linesWith("evaluation ping timeout"); len(lines) != 1 {
		t.Fatalf("ping-timeout lines=%d, want 1: %v", len(lines), lines)
	}
	if lines := fixture.log.linesWith("event=evaluation_budget_exhausted"); len(lines) != 0 {
		t.Fatalf("ordinary ping timeout also claimed pass budget: %v", lines)
	}

	fixture.assertNoDirectArgsRemoval(t)
	fixture.releasePing()
	fixture.wait(t, "delayed initial-ping callback", fixture.pingResultDone)
	fixture.wait(t, "timed-out client cleanup", fixture.clientRemoved)
	if got := fixture.clientCount(); got != 0 {
		t.Fatalf("delayed timed-out result installed %d clients", got)
	}
}

// An explicit terminal pass boundary still cancels its unresolved candidate,
// attributes exactly one provider failure, and rejects a delayed callback.
// This preserves the no-overlap/no-late-admission invariant while the normal
// acquisition boundary above ceases to be terminal.
func TestMultiClientExpandAccountsForPingAtPassDeadline(t *testing.T) {
	fixture := newMultiClientExpandLifecycleFixture(t)
	expirePass := make(chan struct{})
	fixture.window.expireExpandPassForTest = expirePass

	expandDone := fixture.start()
	fixture.wait(t, "held initial-ping result", fixture.pingResultEntered)
	fixture.send(t, "terminal pass deadline", expirePass)
	if got := fixture.result(t, expandDone); got != 0 {
		t.Fatalf("ended expand pass reported %d admissions", got)
	}
	if got := fixture.window.failures.counts(time.Now())[windowFailureProvider]; got != 1 {
		t.Fatalf("provider failures at terminal pass deadline=%d, want 1", got)
	}
	budgetLines := fixture.log.linesWith("event=evaluation_budget_exhausted")
	if len(budgetLines) != 1 {
		t.Fatalf("budget-expiry lines=%d, want 1: %v", len(budgetLines), budgetLines)
	}
	if line := budgetLines[0]; !strings.Contains(line, "window=quality") ||
		!strings.Contains(line, "candidates=1") ||
		!strings.Contains(line, "effective_min=") ||
		!strings.Contains(line, "observed_max=") ||
		!strings.Contains(line, "ping_timeout=17000") ||
		!strings.Contains(line, "expand_timeout=7000") ||
		strings.Contains(line, " exit=") ||
		strings.Contains(line, " client=") {
		t.Fatalf("budget-expiry diagnostic is incomplete or identity-bearing: %q", line)
	}

	fixture.assertNoDirectArgsRemoval(t)
	fixture.releasePing()
	fixture.wait(t, "delayed initial-ping callback", fixture.pingResultDone)
	fixture.wait(t, "ended expand-pass client cleanup", fixture.clientRemoved)
	if got := fixture.window.failures.counts(time.Now())[windowFailureProvider]; got != 1 {
		t.Fatalf("provider failures after delayed callback=%d, want exactly 1", got)
	}
	if got := fixture.clientCount(); got != 0 {
		t.Fatalf("delayed result installed %d clients after its expand pass ended", got)
	}
}

// TestMultiClientExpandLifecycleCancellationIsNotProviderFailure keeps both
// evaluation-epoch replacement and window retirement out of provider health.
func TestMultiClientExpandLifecycleCancellationIsNotProviderFailure(t *testing.T) {
	for _, test := range []struct {
		name   string
		cancel func(*multiClientExpandLifecycleFixture)
	}{
		{
			name: "evaluation epoch",
			cancel: func(fixture *multiClientExpandLifecycleFixture) {
				fixture.cancelEvaluation()
			},
		},
		{
			name: "window",
			cancel: func(fixture *multiClientExpandLifecycleFixture) {
				fixture.cancelWindow()
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMultiClientExpandLifecycleFixture(t)
			expandDone := fixture.start()
			fixture.wait(t, "held initial-ping result", fixture.pingResultEntered)

			test.cancel(fixture)
			if got := fixture.result(t, expandDone); got != 0 {
				t.Fatalf("canceled expand pass reported %d admissions", got)
			}
			if got := fixture.window.failures.counts(time.Now())[windowFailureProvider]; got != 0 {
				t.Fatalf("provider failures after lifecycle cancellation=%d, want 0", got)
			}
			if lines := fixture.log.linesWith("event=evaluation_budget_exhausted"); len(lines) != 0 {
				t.Fatalf("lifecycle cancellation claimed pass budget: %v", lines)
			}

			fixture.releasePing()
			fixture.wait(t, "canceled initial-ping callback", fixture.pingResultDone)
			fixture.wait(t, "canceled client cleanup", fixture.clientRemoved)
			fixture.assertNoDirectArgsRemoval(t)
			if got := fixture.clientCount(); got != 0 {
				t.Fatalf("canceled result installed %d clients", got)
			}
		})
	}
}
