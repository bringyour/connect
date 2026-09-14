package connect

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// The throttled call sites all follow the same shape:
//
//	if ok, suppressed := shouldLogX(); ok {
//	    log.Infof("... (%d suppressed)", err, suppressed)   // or the plain form
//	} else if v := log.V(1); v.Enabled() {
//	    v.Infof("...")                                       // nothing is lost
//	}
//
// Two properties matter and neither is covered by testing logThrottle alone:
// a suppressed line must still reach V(1) rather than vanish, and the count of
// suppressed lines must actually appear in the emitted text. These drive that
// shape through a capturing Logger.

// captureLogger records what was emitted and at which level. verbose reports
// whether V(n) is enabled, standing in for the process verbosity setting.
type captureLogger struct {
	mu      sync.Mutex
	info    []string
	verbose []string
	enabled bool
}

func (c *captureLogger) Info(args ...any) {}
func (c *captureLogger) Infof(format string, args ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.info = append(c.info, fmt.Sprintf(format, args...))
}
func (c *captureLogger) Warningf(format string, args ...any) {}
func (c *captureLogger) Errorf(format string, args ...any)   {}
func (c *captureLogger) V(level int32) Verbose               { return &captureVerbose{c: c} }

type captureVerbose struct{ c *captureLogger }

func (v *captureVerbose) Enabled() bool    { return v.c.enabled }
func (v *captureVerbose) Info(args ...any) {}
func (v *captureVerbose) Infof(format string, args ...any) {
	v.c.mu.Lock()
	defer v.c.mu.Unlock()
	v.c.verbose = append(v.c.verbose, fmt.Sprintf(format, args...))
}

// emit reproduces the call-site shape used at every throttled site.
func emit(log Logger, th *logThrottle, now time.Time, msg string) {
	if ok, suppressed := th.Allow(now); ok {
		if suppressed > 0 {
			log.Infof("%s (%d suppressed)", msg, suppressed)
		} else {
			log.Infof("%s", msg)
		}
	} else if v := log.V(1); v.Enabled() {
		v.Infof("%s", msg)
	}
}

// With V(1) on, a throttled line must still be emitted at verbose level. This
// is the "nothing is lost when the level is raised" guarantee.
func TestThrottledLineFallsBackToVerbose(t *testing.T) {
	log := &captureLogger{enabled: true}
	th := newLogThrottle(time.Minute)
	base := time.Unix(1000, 0)

	emit(log, th, base, "first")                    // allowed -> INFO
	emit(log, th, base.Add(time.Second), "second")  // throttled -> V(1)
	emit(log, th, base.Add(2*time.Second), "third") // throttled -> V(1)

	if len(log.info) != 1 {
		t.Fatalf("expected 1 INFO line, got %d: %v", len(log.info), log.info)
	}
	if len(log.verbose) != 2 {
		t.Fatalf("expected 2 throttled lines to fall back to V(1), got %d: %v", len(log.verbose), log.verbose)
	}
	if log.verbose[0] != "second" || log.verbose[1] != "third" {
		t.Fatalf("wrong lines fell back to V(1): %v", log.verbose)
	}
}

// With V(1) off (the production default, -v=0), a throttled line is dropped
// entirely. That is the point of the throttle: bounded volume under a fault.
func TestThrottledLineDroppedWhenVerboseDisabled(t *testing.T) {
	log := &captureLogger{enabled: false}
	th := newLogThrottle(time.Minute)
	base := time.Unix(1000, 0)

	emit(log, th, base, "first")
	emit(log, th, base.Add(time.Second), "second")

	if len(log.info) != 1 {
		t.Fatalf("expected 1 INFO line, got %d: %v", len(log.info), log.info)
	}
	if len(log.verbose) != 0 {
		t.Fatalf("expected nothing at V(1) when it is disabled, got %v", log.verbose)
	}
}

// The suppressed count has to reach the operator, not just the throttle's
// return value. An outage is only diagnosable if the surviving line says how
// much it stands for.
func TestSuppressedCountAppearsInEmittedLine(t *testing.T) {
	log := &captureLogger{enabled: false}
	th := newLogThrottle(time.Minute)
	base := time.Unix(1000, 0)

	emit(log, th, base, "err")
	for i := 1; i <= 5; i++ {
		emit(log, th, base.Add(time.Duration(i)*time.Second), "err")
	}
	emit(log, th, base.Add(2*time.Minute), "err") // allowed again, reports the 5

	if len(log.info) != 2 {
		t.Fatalf("expected 2 INFO lines, got %d: %v", len(log.info), log.info)
	}
	// The first allowed line stands for itself only, so it carries no tail.
	if strings.Contains(log.info[0], "suppressed") {
		t.Fatalf("first line should not report suppressions, got %q", log.info[0])
	}
	if !strings.Contains(log.info[1], "(5 suppressed)") {
		t.Fatalf("expected the next allowed line to report 5 suppressed, got %q", log.info[1])
	}
}
