package connect

import (
	"context"
	"testing"
	"time"
)

// A gate that nothing has published must fail open.
//
// The resend capacity gate holds a reliable pack outside admission until the
// send loop reports that the resend queue could take it. It was stored as an
// atomic.Bool meaning capacity is available, which is false on a zero-valued
// struct — so any SendSequence not built through newSendSequenceWithLogicalLane
// blocked every reliable pack forever, waiting on a flag that nothing would
// ever publish. A cell that builds a bare sequence and drives its pack channel
// with no send loop running hit it immediately: the provider's TCP return retry
// was refused where it had always been admitted.
//
// That is a fail-closed default on a gate whose publisher is optional, and it
// is exactly the kind of thing that returns: the fix is one word, the sense of
// a boolean, and nothing about the surrounding code makes the right sense
// obvious. So it gets a row rather than a comment.
//
// The row is deterministic by construction: a bare struct, no goroutines, no
// timers, no send loop, and the assertion is on the returned decision rather
// than on how long anything took.
//
// Predictions, recorded before the run: a sequence that has never run a pass
// admits immediately and consumes none of the caller's budget; one the loop has
// told there is no capacity does not.
func TestTheCapacityGateFailsOpenUntilTheLoopPublishes(t *testing.T) {
	// a bare sequence, as a cell builds one and as the provider return
	// fixtures do
	bare := &SendSequence{
		ctx:                context.Background(),
		sendBufferSettings: DefaultSendBufferSettings(),
	}
	pack := &SendPack{Ctx: context.Background()}

	admitted, err, remaining := bare.awaitResendCapacity(pack, time.Second)
	if err != nil {
		t.Fatalf("a bare sequence returned an error rather than a decision: %v", err)
	}
	if !admitted {
		t.Errorf(
			"a sequence whose send loop has never run refused a reliable pack; the gate's publisher is optional, so its unpublished state has to admit — a fail-closed default here blocks every reliable pack of such a sequence forever",
		)
	}
	if remaining != time.Second {
		t.Errorf(
			"the gate consumed %s of the caller's budget deciding it had nothing to say; an unpublished gate must not cost the caller anything",
			time.Second-remaining,
		)
	}

	// and once the loop has spoken, it binds
	bare.resendCapacityUnavailable.Store(true)
	admitted, err, _ = bare.awaitResendCapacity(pack, 0)
	if err != nil {
		t.Fatalf("returned an error rather than a decision: %v", err)
	}
	if admitted {
		t.Errorf(
			"the gate admitted a reliable pack after the loop published that the resend queue could not take it; the whole point of the gate is that a slot held by a pack which cannot progress is a slot denied to one that could",
		)
	}
}
