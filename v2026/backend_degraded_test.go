package connect

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// The degradation state is package-level, because the flood it guards against
// is across every transport and sequence at once. These tests therefore reset
// it rather than constructing an instance, and must not run in parallel.
func resetBackendDegraded() {
	consecutiveBackendFails.Store(0)
	lastBackendFailNano.Store(0)
}

func TestBackendDegraded_CleanStateIsNotDegraded(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	if isBackendDegraded() {
		t.Fatal("a provider that has never seen a failure must not be degraded")
	}
}

func TestBackendDegraded_BelowThresholdIsNotDegraded(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	// One or two stray timeouts are normal churn on a busy provider.
	for i := 0; i < backendDegradedFailThreshold-1; i++ {
		noteBackendFailure()
		if isBackendDegraded() {
			t.Fatalf("degraded after %d failures; threshold is %d", i+1, backendDegradedFailThreshold)
		}
	}
}

func TestBackendDegraded_ThresholdReachedIsDegraded(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	for i := 0; i < backendDegradedFailThreshold; i++ {
		noteBackendFailure()
	}
	if !isBackendDegraded() {
		t.Fatalf("not degraded after %d consecutive recent failures", backendDegradedFailThreshold)
	}
}

// The counter alone is not enough: a stale count left by an old blip on an
// otherwise idle provider must not read as a live outage.
func TestBackendDegraded_StaleFailuresAreNotDegraded(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	for i := 0; i < backendDegradedFailThreshold; i++ {
		noteBackendFailure()
	}
	if !isBackendDegraded() {
		t.Fatal("precondition: should be degraded before aging the failure")
	}

	// Age the last failure past the recency window.
	lastBackendFailNano.Store(time.Now().Add(-backendDegradedWindow - time.Second).UnixNano())

	if isBackendDegraded() {
		t.Fatalf("still degraded with the last failure older than %s", backendDegradedWindow)
	}
}

// Recovery must be immediate on the first good round-trip, not on a timer.
func TestBackendDegraded_SuccessClearsImmediately(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	for i := 0; i < backendDegradedFailThreshold+5; i++ {
		noteBackendFailure()
	}
	if !isBackendDegraded() {
		t.Fatal("precondition: should be degraded")
	}

	noteBackendSuccess()

	if isBackendDegraded() {
		t.Fatal("a successful round-trip must clear the degraded state immediately")
	}
	if got := consecutiveBackendFails.Load(); got != 0 {
		t.Fatalf("consecutive failures = %d after success, want 0", got)
	}
}

// This is the false-positive guard that matters most: an intermittently failing
// path that still succeeds sometimes is NOT an outage, however many failures it
// accumulates in total.
func TestBackendDegraded_InterleavedSuccessNeverAccumulates(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	for i := 0; i < 50; i++ {
		noteBackendFailure()
		noteBackendFailure()
		noteBackendSuccess()
		if isBackendDegraded() {
			t.Fatalf("degraded on iteration %d despite an interleaved success", i)
		}
	}
}

// Regression: a streak that aged out must not be resumed by a later failure.
// Before the fix, three stale failures plus one fresh one totalled four with a
// current timestamp, so the backend read as degraded on the strength of a
// single recent failure.
func TestBackendDegraded_StaleStreakIsNotResumedByANewFailure(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	for i := 0; i < backendDegradedFailThreshold; i++ {
		noteBackendFailure()
	}
	// Age the whole streak out of the window, as an idle provider that simply
	// stopped retrying would.
	lastBackendFailNano.Store(time.Now().Add(-backendDegradedWindow - time.Minute).UnixNano())
	if isBackendDegraded() {
		t.Fatal("precondition: an aged-out streak must not read as degraded")
	}

	// One new failure. This is the first failure of a fresh streak, not the
	// fourth of the old one.
	noteBackendFailure()

	if got := consecutiveBackendFails.Load(); got != 1 {
		t.Fatalf("consecutive failures = %d after a stale streak plus one failure, want 1", got)
	}
	if isBackendDegraded() {
		t.Fatal("degraded after a single recent failure; the stale streak was resumed instead of reset")
	}
}

// The reset must not fire on a gap shorter than the window, or a slow but
// genuine outage would never accumulate.
func TestBackendDegraded_StreakSurvivesGapInsideWindow(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	noteBackendFailure()
	noteBackendFailure()
	// A gap well inside the window: still the same outage.
	lastBackendFailNano.Store(time.Now().Add(-backendDegradedWindow / 2).UnixNano())

	noteBackendFailure()

	if got := consecutiveBackendFails.Load(); got != backendDegradedFailThreshold {
		t.Fatalf("consecutive failures = %d, want %d (streak reset inside the window)", got, backendDegradedFailThreshold)
	}
	if !isBackendDegraded() {
		t.Fatal("not degraded after 3 failures spanning a gap inside the window")
	}
}

// Failures and successes arrive concurrently in production: one client's auth
// can succeed while another's contract OOB fails. The pair must never leave a
// positive failure count with a zero timestamp, which isBackendDegraded would
// read as "not degraded" while failures are actually accumulating.
//
// This asserts the invariant; it does not reliably reproduce its violation. The
// window is two adjacent atomic stores, so an unsynchronized noteBackendSuccess
// still passes this test almost always. The serialization in noteBackendSuccess
// is what makes the invariant hold, and this guards against a future change that
// breaks it in a wider, actually-observable way.
func TestBackendDegraded_ConcurrentSuccessAndFailureNeverLeaveInvalidState(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			noteBackendFailure()
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			noteBackendSuccess()
		}()
	}
	wg.Wait()

	// A positive count always implies a real failure timestamp.
	if got := consecutiveBackendFails.Load(); 0 < got {
		if lastBackendFailNano.Load() == 0 {
			t.Fatalf("invalid state: %d consecutive failures recorded with a zero timestamp", got)
		}
	}
	// And a zeroed timestamp always implies no outstanding failures.
	if lastBackendFailNano.Load() == 0 {
		if got := consecutiveBackendFails.Load(); got != 0 {
			t.Fatalf("invalid state: zero timestamp with %d consecutive failures", got)
		}
	}
}

func TestBackendDegraded_ConcurrentFailuresReachThreshold(t *testing.T) {
	resetBackendDegraded()
	defer resetBackendDegraded()

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			noteBackendFailure()
		}()
	}
	wg.Wait()

	if got := consecutiveBackendFails.Load(); got != 32 {
		t.Fatalf("consecutive failures = %d after 32 concurrent failures, want 32", got)
	}
	if !isBackendDegraded() {
		t.Fatal("not degraded after 32 concurrent failures")
	}
}

// The auth sites must make the same local-teardown carve-out the contract OOB
// path makes on client.Done: a canceled dial is this process shutting a
// transport down, not the backend failing. Closing a multi-client window
// cancels many transports at once, and without the guard that burst of
// canceled dials trips the threshold with fresh timestamps -- so the NEXT
// session starts gated. runH1/runH3 are connect loops that need a live server
// to drive, so this pins the call-site shape the way the resize-pass anchors
// do, for both transports at once.
func TestAuthCancellationIsNotBackendFailure(t *testing.T) {
	source, err := readSource("transport.go")
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		fn    string
		guard string
	}{
		{
			fn:    "func (self *PlatformTransport) runH1(",
			guard: "if self.ctx.Err() == nil {",
		},
		{
			fn:    "func (self *PlatformTransport) runH3(",
			guard: "if ctx.Err() == nil {",
		},
	} {
		body, ok := functionBody(source, test.fn)
		if !ok {
			t.Fatalf("could not find %s", test.fn)
		}
		note := strings.Index(body, "self.noteDialFailure()")
		if note < 0 {
			t.Fatalf("%s no longer records auth failures; the degraded signal lost its transport half", test.fn)
		}
		guarded := strings.Index(body, test.guard)
		if guarded < 0 || note < guarded {
			t.Fatalf("%s records auth failures without the local-teardown carve-out: a canceled dial would count as a backend failure", test.fn)
		}
	}

	// the runners record through noteDialFailure, which keeps the process-wide
	// degraded signal for a family-agnostic transport and routes a
	// family-pinned transport to its own backoff (IPV6.md A5): a pinned
	// transport failing forever on a network without its family must never
	// gate the process. The indirection must still reach noteBackendFailure,
	// and the pinned early return must come first.
	familySource, err := readSource("transport_family.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(familySource, "func (self *PlatformTransport) noteDialFailure(")
	if !ok {
		t.Fatal("could not find noteDialFailure")
	}
	note := strings.Index(body, "noteBackendFailure()")
	if note < 0 {
		t.Fatal("noteDialFailure no longer records backend failures; the degraded signal lost its transport half")
	}
	pinned := strings.Index(body, "if self.pinned() {")
	if pinned < 0 || note < pinned {
		t.Fatal("noteDialFailure counts a family-pinned transport's dial failure as a backend failure")
	}
}
