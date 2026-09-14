package connect

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The pool's ownership contract is documented and careful readers still
// violate it: three defects of the same class turned up in one new test cell,
// a double return, an unrelated one, and a leak of one pooled buffer per frame
// decoded and dropped. Each was detected by the pool and none failed a test,
// because the detection logs. The leak surfaced as 1,504 outstanding buffers
// in an unrelated test later in the same serial run, which is the expensive
// way to find it.
//
// Two instruments, because the two failures are not the same shape.
//
// A double return or a share of an untaken buffer is detected at the violating
// call, which is the one moment the offending call site is still on the stack.
// `assertMessagePoolOwnership` installs a handler there and fails the test with
// that stack.
//
// A leak is the absence of a return and has no such moment. The only instrument
// is reconciliation at a boundary: outstanding roots, over every size class,
// against the count when the test began. It is inherently approximate — a
// return still in flight on another goroutine reads as outstanding — so the
// check allows a bounded settle and says so rather than pretending otherwise.

// how long a test's returns may still be in flight before a nonzero
// outstanding delta is called a leak
const messagePoolOwnershipSettleTimeout = 1 * time.Second

// violations a test caused deliberately, which the package backstop must not
// count against the run
var expectedMessagePoolViolationCount atomic.Uint64

// What the ownership assertion needs of a test. `*testing.T` satisfies it, and
// so does the recorder below, which is how the rows that exercise the
// assertion can assert that it fails without failing themselves.
type messagePoolOwnershipReporter interface {
	Errorf(format string, args ...any)
	Cleanup(cleanup func())
}

// Fails the test at each pool ownership violation, with the stack of the call
// that caused it, and reconciles outstanding roots at teardown.
//
// The handler is process-wide, so this saves and restores whatever was
// installed and must not be used from a parallel test. It stops reporting
// before the test completes, so a violation on a goroutine that outlives the
// test reaches the package backstop in TestMain instead of panicking here.
func assertMessagePoolOwnership(t messagePoolOwnershipReporter) {
	var reportLock sync.Mutex
	finished := false
	baselineCount, baselineBytes := messagePoolOutstandingSnapshot()
	previous := SetMessagePoolViolationHandler(func(err error, stack []byte) {
		reportLock.Lock()
		defer reportLock.Unlock()
		if finished {
			return
		}
		t.Errorf("message pool ownership violation: %v\n%s", err, stack)
	})
	t.Cleanup(func() {
		func() {
			reportLock.Lock()
			defer reportLock.Unlock()
			finished = true
		}()
		SetMessagePoolViolationHandler(previous)

		deadline := time.Now().Add(messagePoolOwnershipSettleTimeout)
		for {
			outstandingCount, outstandingBytes := messagePoolOutstandingSnapshot()
			if outstandingCount <= baselineCount {
				return
			}
			if !time.Now().Before(deadline) {
				t.Errorf(
					"%d pooled roots (%d bytes) were still held %s after the test, against %d (%d bytes) when it began; a buffer taken and never returned leaks its root",
					outstandingCount-baselineCount,
					outstandingBytes-baselineBytes,
					messagePoolOwnershipSettleTimeout,
					baselineCount,
					baselineBytes,
				)
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
}

// Calls a borrowing entry point with a pooled buffer and returns the buffer
// after the call, which is the contract those entries keep (CODESTYLE, "Which
// entry points borrow, take, or take on success"). Fixtures should reach for
// this rather than calling a borrowing entry directly: every leak this suite
// has found was a fixture that built a buffer, handed it to one, and never
// returned it.
func withBorrowedMessage(message []byte, borrow func(message []byte)) {
	defer MessagePoolReturn(message)
	borrow(message)
}

// The package backstop. The per-test handler gives attribution; this gives
// coverage, including violations from goroutines that outlive the test that
// started them and from tests that never opted in.
//
// One known false positive, left in deliberately: MessagePoolReturn recognises
// a pooled buffer by its capacity alone, so a buffer that was never pooled but
// happens to be exactly a size class plus MessagePoolMetaByteCount is read as a
// pool root. Returning such a buffer to the pool is itself a mistake, so
// flagging it is the right answer.
func TestMain(m *testing.M) {
	code := m.Run()
	violationCount := MessagePoolViolationCount()
	expectedCount := expectedMessagePoolViolationCount.Load()
	if expectedCount < violationCount && code == 0 {
		fmt.Fprintf(
			os.Stderr,
			"FAIL\t%d message pool ownership violations were detected in this run (%d of them deliberate); a buffer was returned or shared that no owner held\n",
			violationCount,
			expectedCount,
		)
		code = 1
	}
	os.Exit(code)
}

// Collects what the assertion reports, so a row can exercise a violation and
// assert the assertion fires without failing itself.
type messagePoolOwnershipRecorder struct {
	stateLock sync.Mutex
	reports   []string
	cleanups  []func()
}

func (self *messagePoolOwnershipRecorder) Errorf(format string, args ...any) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.reports = append(self.reports, fmt.Sprintf(format, args...))
}

func (self *messagePoolOwnershipRecorder) Cleanup(cleanup func()) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.cleanups = append(self.cleanups, cleanup)
}

// runs the registered cleanups in the order a test would, last first
func (self *messagePoolOwnershipRecorder) finish() []string {
	cleanups := func() []func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		cleanups := self.cleanups
		self.cleanups = nil
		return cleanups
	}()
	for i := len(cleanups) - 1; 0 <= i; i -= 1 {
		cleanups[i]()
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.reports
}

// The instrument itself, on a violation with a call site. Without it the
// contract is policed by a human reading the log, which is how three defects
// of this class reached a gate run.
func TestMessagePoolOwnershipViolationFailsTheTestThatCausedIt(t *testing.T) {
	expectedMessagePoolViolationCount.Add(1)
	recorder := &messagePoolOwnershipRecorder{}
	assertMessagePoolOwnership(recorder)

	message := MessagePoolGet(DefaultMtu)
	if !MessagePoolReturn(message) {
		t.Fatal("the first return did not release the root")
	}
	// the violation: no owner holds this buffer any more
	MessagePoolReturn(message)
	reports := recorder.finish()

	if len(reports) == 0 {
		t.Fatal("a double return reported nothing; a pool ownership violation must fail the test that caused it, not only log")
	}
	if !strings.Contains(reports[0], "not taken") {
		t.Errorf("the violation report does not name the violation: %s", reports[0])
	}
	// the stack is what makes the report actionable: it names the call site
	if !strings.Contains(reports[0], "MessagePoolReturn") {
		t.Errorf("the violation report carries no stack naming the returning call: %s", reports[0])
	}
}

// The leak instrument, on a class the packet-only counter cannot see. The leak
// that motivated this was 1,504 buffers of one non-packet class, so a helper
// blind to those classes would have missed the case that prompted it.
func TestMessagePoolLeakOfALargeObjectFailsAtTeardown(t *testing.T) {
	recorder := &messagePoolOwnershipRecorder{}
	assertMessagePoolOwnership(recorder)

	leaked := MessagePoolGet(4096)
	if leaked == nil {
		t.Fatal("the large object class did not supply a root")
	}
	reports := recorder.finish()
	// the run's own books stay balanced: the leak was this row's point
	MessagePoolReturn(leaked)

	if len(reports) == 0 {
		t.Fatal("a pooled large object taken and never returned reported nothing; the teardown reconciliation must see every size class")
	}
	if !strings.Contains(reports[0], "still held") {
		t.Errorf("the leak report does not name the outstanding roots: %s", reports[0])
	}
}

// A test that honours the contract must not be failed by either instrument,
// including when its returns happen on another goroutine and land after the
// body returns. That is the case the settle window exists for.
func TestMessagePoolOwnershipPassesWhenTheContractIsHonoured(t *testing.T) {
	assertMessagePoolOwnership(t)

	message := MessagePoolGet(DefaultMtu)
	shared := MessagePoolShareReadOnly(message)
	if MessagePoolReturn(shared) {
		t.Fatal("a non-final shared return released the root")
	}
	if !MessagePoolReturn(message) {
		t.Fatal("the final return did not release the root")
	}

	deferred := MessagePoolGet(8192)
	returned := make(chan struct{})
	go func() {
		defer close(returned)
		time.Sleep(20 * time.Millisecond)
		MessagePoolReturn(deferred)
	}()
	t.Cleanup(func() { <-returned })
}
