// Panic-containment tests verify both rescue isolation and lifecycle cleanup.
package connect

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// a rescue handler that panics must be contained by HandleError — it must
// not escalate the recovery into a process crash, and later handlers must
// still run. Regression for the 2026-08-02 teardown crash: a caller
// registered wg.Done both as an in-closure defer and as a rescue handler,
// and the panic path double-counted into `sync: negative WaitGroup counter`
// inside the recover, killing the process.
func TestHandleErrorRescueHandlerPanicContained(t *testing.T) {
	laterHandlerRan := false
	HandleError(
		func() {
			panic(errors.New("boom"))
		},
		func() {
			panic(errors.New("rescue handler bug"))
		},
		func() {
			laterHandlerRan = true
		},
	)
	if !laterHandlerRan {
		t.Fatal("handler after a panicking handler did not run")
	}
}

// the exact caller shape that crashed: wg.Done deferred inside the wrapped
// func AND passed as a rescue handler. The double Done on the panic path
// must be contained (logged), never fatal. Note what containment does NOT
// promise: sync.WaitGroup.Add moves the counter before panicking on
// negative, so the misused WaitGroup itself stays corrupted (a later Wait
// would hang) — the loud log exists so the misuse gets fixed at the caller,
// as UpdateClientScores was.
func TestHandleErrorDoubleDoneContained(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	returned := false
	HandleError(
		func() {
			defer wg.Done()
			panic(errors.New("boom"))
		},
		wg.Done,
	)
	returned = true
	// reaching here at all is the assertion: the pre-fix behavior was a
	// process-killing repanic out of HandleError's recover
	if !returned {
		t.Fatal("unreachable")
	}
}

// handlers must not run on a normal return.
func TestHandleErrorNoPanicNoHandlers(t *testing.T) {
	ran := false
	HandleError(
		func() {},
		func() {
			ran = true
		},
	)
	if ran {
		t.Fatal("rescue handler ran without a panic")
	}
}

// A context cancellation function retains its named dynamic type when passed
// through any, so recovery must recognize that type rather than only func().
func TestHandleErrorRunsNamedContextCancelFunc(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	HandleError(
		func() {
			panic(errors.New("boom"))
		},
		cancel,
	)
	select {
	case <-ctx.Done():
	default:
		t.Fatal("recovery did not invoke the named context cancellation function")
	}
}

// A cancellation-with-cause function receives the recovered error despite its
// named dynamic function type.
func TestHandleErrorRunsNamedContextCancelCauseFunc(t *testing.T) {
	ctx, cancel := context.WithCancelCause(context.Background())
	HandleError(
		func() {
			panic(errors.New("named cancel cause"))
		},
		cancel,
	)
	if cause := context.Cause(ctx); cause == nil || cause.Error() != "named cancel cause" {
		t.Fatalf("recovery cancellation cause=%v, want named cancel cause", cause)
	}
}
