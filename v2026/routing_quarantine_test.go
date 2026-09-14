package connect

import (
	"strings"
	"testing"
	"time"
)

// TestBenchDurationEscalates is the brief's own table: dampening off is a
// constant base (today's behavior, unchanged), dampening on escalates
// 60s -> 120s -> 240s by reconvictions and caps at the third step.
func TestBenchDurationEscalates(t *testing.T) {
	base := 60 * time.Second
	if benchDuration(0, base, false) != base {
		t.Fatal("dampening off must be constant base")
	}
	if benchDuration(0, base, true) != 60*time.Second ||
		benchDuration(1, base, true) != 120*time.Second ||
		benchDuration(2, base, true) != 240*time.Second ||
		benchDuration(5, base, true) != 240*time.Second {
		t.Fatal("dampening on must escalate 60->120->240 and cap")
	}
}

// TestBenchDurationDampeningOffIgnoresReconvictions pins the other half of
// the zero-value-off contract: with dampening off, base is returned no
// matter how many times this exit has already been reconvicted. If a future
// edit ever let reconvictions leak into the off path, this catches it before
// it changes a default build's bench hold time.
func TestBenchDurationDampeningOffIgnoresReconvictions(t *testing.T) {
	base := 90 * time.Second
	for _, reconvictions := range []int{0, 1, 2, 3, 100} {
		if got := benchDuration(reconvictions, base, false); got != base {
			t.Fatalf("dampening off at reconvictions=%d: got %v, want constant base %v", reconvictions, got, base)
		}
	}
}

// TestBenchDurationNegativeReconvictionsClamped guards the defensive clamp:
// a corrupt or not-yet-incremented negative count must read as the first
// tier, not panic on the steps slice or escalate backwards.
func TestBenchDurationNegativeReconvictionsClamped(t *testing.T) {
	base := 60 * time.Second
	if got := benchDuration(-1, base, true); got != 60*time.Second {
		t.Fatalf("negative reconvictions must clamp to the first tier: got %v", got)
	}
}

// TestDefaultMultiClientSettingsQuarantineKnobsMatchTask8Contract pins the
// actual regression risk post-Task-8: DefaultMultiClientSettings must keep
// QuarantineDampening on, while QuarantineReentryRamp -- deliberately NOT in
// Task 8's list, its decay story is separate -- stays at 0. Renamed from
// TestDefaultMultiClientSettingsQuarantineKnobsZeroValueOff, which asserted
// the pre-Task-8 contract (both off); Task 8
// (feat(routing): enable class-aware scored placement by default) is the one
// task in the smart-routing phase permitted to change a default, and a
// future well-meaning "restore the zero-value-off convention" edit would
// silently revert every default build's flap damping. See also
// routing_defaults_test.go for the full six-knob pin including the session
// banner.
func TestDefaultMultiClientSettingsQuarantineKnobsMatchTask8Contract(t *testing.T) {
	s := DefaultMultiClientSettings()
	if !s.QuarantineDampening {
		t.Fatal("DefaultMultiClientSettings must leave QuarantineDampening on (true)")
	}
	if s.QuarantineReentryRamp != 0 {
		t.Fatal("DefaultMultiClientSettings must leave QuarantineReentryRamp at 0 (not in Task 8's scope)")
	}
}

// TestNewQuarantineKnobsZeroValueOff asserts QuarantineDampening and
// QuarantineReentryRamp follow the same zero-value-off contract as every
// other ReliabilitySettings field for a nil override (or one built from an
// older struct): that must still leave them off, and ReliabilitySettingsFrom
// must faithfully copy each through when it is set. This is the
// backward-compatibility contract -- an old caller that never heard of these
// fields must see exactly the pre-Phase-2 behavior -- which is orthogonal to
// DefaultMultiClientSettings' own default, which Task 8 turns
// QuarantineDampening on for (see
// TestDefaultMultiClientSettingsQuarantineKnobsMatchTask8Contract above).
//
// The copy-fidelity half deliberately does NOT source its input from
// DefaultMultiClientSettings(): Task 8 means the defaults no longer leave
// both knobs at false/0, so a comparison built on the defaults (got.X !=
// s.X) would be true != true regardless of whether ReliabilitySettingsFrom
// copies the field at all -- deleting the copy line entirely could still
// pass by coincidence. Instead an explicit, fully-populated
// MultiClientSettings is round-tripped, with distinct non-zero values (true
// / 45s, not the same value twice) so a copy line wired to the wrong source
// field is caught too, not just a missing one.
func TestNewQuarantineKnobsZeroValueOff(t *testing.T) {
	z := ReliabilitySettingsFrom(nil) // nil -> zero value
	if z.QuarantineDampening || z.QuarantineReentryRamp != 0 {
		t.Fatal("new quarantine knobs must be zero-value-off (legacy behavior)")
	}

	src := &MultiClientSettings{
		QuarantineDampening:   true,
		QuarantineReentryRamp: 45 * time.Second,
	}
	got := ReliabilitySettingsFrom(src)
	if got.QuarantineDampening != true {
		t.Fatal("ReliabilitySettingsFrom must copy QuarantineDampening")
	}
	if got.QuarantineReentryRamp != 45*time.Second {
		t.Fatalf("ReliabilitySettingsFrom must copy QuarantineReentryRamp: got %v, want 45s", got.QuarantineReentryRamp)
	}
}

// TestQuarantineReconvictionsEscalateAcrossLiftCycles is the counter
// benchDuration reads: 0 through the first-ever bench, incremented on every
// completed lift, and never incremented by a no-op clear on an already-clear
// channel (which has no episode to have completed).
func TestQuarantineReconvictionsEscalateAcrossLiftCycles(t *testing.T) {
	client := stallTestChannel()

	if got := client.quarantineReconvictionCount(); got != 0 {
		t.Fatalf("a never-quarantined channel must read 0 reconvictions, got %d", got)
	}

	// a clear on an already-clear channel must not manufacture a reconviction
	client.clearQuarantine()
	if got := client.quarantineReconvictionCount(); got != 0 {
		t.Fatalf("a no-op clear must not increment reconvictions, got %d", got)
	}

	client.setQuarantined(blackholeNoReceiveAck)
	if got := client.quarantineReconvictionCount(); got != 0 {
		t.Fatalf("reconvictions must not advance until the FIRST episode lifts, got %d", got)
	}
	client.clearQuarantine()
	if got := client.quarantineReconvictionCount(); got != 1 {
		t.Fatalf("the first lift must bring reconvictions to 1, got %d", got)
	}

	client.setQuarantined(blackholeNoReceiveSyn)
	client.clearQuarantine()
	if got := client.quarantineReconvictionCount(); got != 2 {
		t.Fatalf("a second bench-then-lift cycle must bring reconvictions to 2, got %d", got)
	}
}

// TestQuarantineReconvictionsSurviveReceiveProgressRelease proves the
// counter feeding benchDuration is wired through the SAME release-on-
// receive-progress path this task must not disturb: addReceiveAck's clear
// still lifts the quarantine (unchanged), and it also advances the
// reconviction count exactly like clearQuarantine does, since both route
// through clearQuarantineWithLock.
func TestQuarantineReconvictionsSurviveReceiveProgressRelease(t *testing.T) {
	client := stallTestChannel()

	client.setQuarantined(blackholeNoReceiveAck)
	AssertEqual(t, client.isQuarantined(), true)

	client.addReceiveAck(1440)

	// the hard constraint: release-on-receive-progress still lifts the
	// quarantine exactly as before this task
	AssertEqual(t, client.isQuarantined(), false)
	if got := client.quarantineReconvictionCount(); got != 1 {
		t.Fatalf("a receive-progress release must still advance reconvictions, got %d", got)
	}
}

// TestQuarantineReconvictionDecaySteps is the pure decay math Task 6 adds:
// one step removed per whole quarantineReconvictionDecayInterval of elapsed
// time, floored at 0, with both a negative count and a negative elapsed
// (clock trouble, not evidence of a longer clean interval -- the same
// convention reentryScorePenalty's elapsed<0 clamp already uses) reading as
// "no decay to apply" rather than propagating a negative result. A negative
// reconviction count feeding benchDuration or exitScore's StallEvents term
// is exactly the defect class this branch has already shipped twice
// (negative StallEvents scoring as a bonus, a zero RTT scoring best), so
// this must never produce one.
func TestQuarantineReconvictionDecaySteps(t *testing.T) {
	interval := quarantineReconvictionDecayInterval

	if got := quarantineReconvictionDecay(5, 0); got != 5 {
		t.Fatalf("zero elapsed must not decay: got %d, want 5", got)
	}
	if got := quarantineReconvictionDecay(5, interval-time.Second); got != 5 {
		t.Fatalf("just under one elapsed interval must not decay yet: got %d, want 5", got)
	}
	if got := quarantineReconvictionDecay(5, interval); got != 4 {
		t.Fatalf("exactly one elapsed interval must remove exactly one step: got %d, want 4", got)
	}
	if got := quarantineReconvictionDecay(5, 2*interval); got != 3 {
		t.Fatalf("two elapsed intervals must remove exactly two steps: got %d, want 3", got)
	}
	if got := quarantineReconvictionDecay(5, 100*interval); got != 0 {
		t.Fatalf("many elapsed intervals must floor at 0, not go negative: got %d, want 0", got)
	}
	if got := quarantineReconvictionDecay(0, 100*interval); got != 0 {
		t.Fatalf("a count already at 0 must stay 0: got %d, want 0", got)
	}
	if got := quarantineReconvictionDecay(-3, interval); got != 0 {
		t.Fatalf("a negative count must clamp to 0, not go more negative: got %d, want 0", got)
	}
	if got := quarantineReconvictionDecay(5, -time.Second); got != 5 {
		t.Fatalf("negative elapsed must clamp to no-decay, got %d, want 5", got)
	}
}

// TestQuarantineReconvictionCountDecaysOverQuietTime is Task 6's channel-level
// proof: with QuarantineDampening on, quarantineReconvictionCount() decays
// the count by whole quarantineReconvictionDecayInterval steps measured from
// quarantineLiftTime (the last completed lift), floors at 0 once enough
// quiet time has passed, and a fresh conviction is visible immediately
// afterward rather than reading as still-decayed. Deliberately picks
// intervals where the decayed and un-decayed readings differ (1 and 0
// against a raw count of 2) so this cannot pass against the un-fixed
// always-climbing counter.
func TestQuarantineReconvictionCountDecaysOverQuietTime(t *testing.T) {
	client := stallTestChannel()
	client.settings.QuarantineDampening = true

	// two completed bench-then-lift cycles -> raw reconvictions = 2
	client.setQuarantined(blackholeNoReceiveAck)
	client.clearQuarantine()
	client.setQuarantined(blackholeNoReceiveSyn)
	client.clearQuarantine()
	if got := client.quarantineReconvictionCount(); got != 2 {
		t.Fatalf("expected 2 reconvictions right after the second lift, got %d", got)
	}

	// age the last lift back by just over one decay interval: exactly one
	// step must be removed
	client.stateLock.Lock()
	client.quarantineLiftTime = time.Now().Add(-quarantineReconvictionDecayInterval - time.Second)
	client.stateLock.Unlock()
	if got := client.quarantineReconvictionCount(); got != 1 {
		t.Fatalf("one elapsed decay interval must remove exactly one step, got %d, want 1", got)
	}

	// age it back far enough for well past both steps: must floor at 0, not
	// go negative
	client.stateLock.Lock()
	client.quarantineLiftTime = time.Now().Add(-5*quarantineReconvictionDecayInterval - time.Second)
	client.stateLock.Unlock()
	if got := client.quarantineReconvictionCount(); got != 0 {
		t.Fatalf("enough quiet time must decay reconvictions to exactly 0, got %d, want 0", got)
	}

	// a fresh conviction resets the decay clock: the new lift's elapsed time
	// is ~0, so the count must be visible right away, not read as decayed
	client.setQuarantined(blackholeNoReceiveAck)
	client.clearQuarantine()
	if got := client.quarantineReconvictionCount(); got == 0 {
		t.Fatal("a fresh conviction must reset the decay clock, not read as still-decayed")
	}
}

// TestQuarantineReconvictionDecayIsDurableAcrossAReconviction is fix-round-1's
// required proof: a count that decayed to a read of 0 during a long quiet
// interval must be STORED back that way at the very next conviction
// (decay-then-increment inside clearQuarantineWithLock), not resume climbing
// from the historical raw peak. A pure read-time-only lens cannot do this: it
// reads back the OLD raw value at the instant quarantineLiftTime is
// overwritten, so a channel with 3 completed cycles that goes quiet long
// enough to read 0, then reconvicts once, would read back 4 (3+1, the
// historical raw plus one) instead of 1 -- resurrecting exactly the
// convictions the decay exists to forgive, in precisely the "misbehaved
// hours ago, quiet since, then right now" scenario Task 6 exists for.
func TestQuarantineReconvictionDecayIsDurableAcrossAReconviction(t *testing.T) {
	client := stallTestChannel()
	client.settings.QuarantineDampening = true

	// three completed bench-then-lift cycles -> raw reconvictions = 3
	client.setQuarantined(blackholeNoReceiveAck)
	client.clearQuarantine()
	client.setQuarantined(blackholeNoReceiveSyn)
	client.clearQuarantine()
	client.setQuarantined(blackholeNoReceiveAck)
	client.clearQuarantine()
	if got := client.quarantineReconvictionCount(); got != 3 {
		t.Fatalf("expected 3 reconvictions after three lifts, got %d", got)
	}

	// age the last lift back far enough that the read-side decay reads 0
	client.stateLock.Lock()
	client.quarantineLiftTime = time.Now().Add(-10 * quarantineReconvictionDecayInterval)
	client.stateLock.Unlock()
	if got := client.quarantineReconvictionCount(); got != 0 {
		t.Fatalf("setup: expected the count to read 0 after a long quiet interval, got %d", got)
	}

	// one more conviction, right now (no further quiet time to fake): the
	// decay must already be folded into the STORED value, so this reads back
	// 1, not the historical raw (3) + 1
	client.setQuarantined(blackholeNoReceiveSyn)
	client.clearQuarantine()
	if got := client.quarantineReconvictionCount(); got != 1 {
		t.Fatalf("a reconviction after a full decay must store back 1 (decay-then-increment), got %d, want 1", got)
	}
}

// TestQuarantineReconvictionCountInertWhenDampeningOff pins the zero-value-off
// contract: with QuarantineDampening at its zero value (false), an aged
// quarantineLiftTime must not decay the count at all -- quarantineReconvictionCount()
// must keep returning the raw, ever-climbing reading a default build has
// always returned, with no clock read and no decay computation performed.
func TestQuarantineReconvictionCountInertWhenDampeningOff(t *testing.T) {
	client := stallTestChannel()
	// QuarantineDampening left at its zero value (false)

	client.setQuarantined(blackholeNoReceiveAck)
	client.clearQuarantine()
	client.setQuarantined(blackholeNoReceiveSyn)
	client.clearQuarantine()

	client.stateLock.Lock()
	client.quarantineLiftTime = time.Now().Add(-10 * quarantineReconvictionDecayInterval)
	client.stateLock.Unlock()

	if got := client.quarantineReconvictionCount(); got != 2 {
		t.Fatalf("QuarantineDampening off must never decay the count, got %d, want 2", got)
	}
}

// TestReentryScorePenaltyDecaysToZero is the pure decay curve: full weight
// at the instant of release (elapsed==0), zero once ramp has fully elapsed,
// and strictly decreasing in between. ramp<=0 is the zero-value-off legacy
// path -- no penalty at all, for any elapsed.
func TestReentryScorePenaltyDecaysToZero(t *testing.T) {
	ramp := 100 * time.Second

	if got := reentryScorePenalty(0, 0); got != 0 {
		t.Fatalf("ramp<=0 must disable the penalty entirely, got %v", got)
	}
	if got := reentryScorePenalty(50*time.Second, 0); got != 0 {
		t.Fatalf("ramp<=0 must disable the penalty regardless of elapsed, got %v", got)
	}

	atRelease := reentryScorePenalty(0, ramp)
	if atRelease != reentryPenaltyWeight {
		t.Fatalf("elapsed==0 must be the full penalty weight: got %v, want %v", atRelease, reentryPenaltyWeight)
	}

	half := reentryScorePenalty(50*time.Second, ramp)
	if half <= 0 || half >= atRelease {
		t.Fatalf("mid-ramp penalty must be strictly between 0 and the full weight, got %v", half)
	}
	if half != reentryPenaltyWeight*0.5 {
		t.Fatalf("halfway through a linear ramp must be exactly half weight: got %v, want %v", half, reentryPenaltyWeight*0.5)
	}

	if got := reentryScorePenalty(ramp, ramp); got != 0 {
		t.Fatalf("elapsed==ramp must have fully decayed to 0, got %v", got)
	}
	if got := reentryScorePenalty(2*ramp, ramp); got != 0 {
		t.Fatalf("elapsed past ramp must stay at 0, got %v", got)
	}

	// a negative elapsed (clock trouble, not a longer-than-possible clean
	// interval) clamps to the full penalty rather than reading as decayed
	if got := reentryScorePenalty(-5*time.Second, ramp); got != atRelease {
		t.Fatalf("negative elapsed must clamp to full penalty, got %v want %v", got, atRelease)
	}
}

// TestReentryPenaltyZeroRampIsLegacyNoop is the channel-level zero-value-off
// check: even a freshly-released channel (elapsed==0, the worst case for a
// penalty) must read exactly 0 when QuarantineReentryRamp is at its zero
// value, so a default build's scored placement is byte-for-byte unchanged.
func TestReentryPenaltyZeroRampIsLegacyNoop(t *testing.T) {
	client := stallTestChannel()
	client.setQuarantined(blackholeNoReceiveAck)
	client.clearQuarantine()

	if got := client.reentryPenalty(0); got != 0 {
		t.Fatalf("ramp==0 must be a no-op regardless of how recently the exit was released, got %v", got)
	}
}

// TestReentryPenaltyRequiresAPriorLift: a channel that has never been
// quarantined has no lift timestamp to measure from, and must not be
// penalized just because a ramp is configured.
func TestReentryPenaltyRequiresAPriorLift(t *testing.T) {
	client := stallTestChannel()
	if got := client.reentryPenalty(45 * time.Second); got != 0 {
		t.Fatalf("a never-quarantined channel must carry no re-entry penalty, got %v", got)
	}
}

// TestReentryPenaltyAppliesJustAfterALift: with a ramp configured, a channel
// released moments ago must carry a strictly positive penalty close to the
// full weight -- the asymmetric half of flap damping (fast to leave
// selection via quarantine, slow to return to full standing).
func TestReentryPenaltyAppliesJustAfterALift(t *testing.T) {
	client := stallTestChannel()
	client.setQuarantined(blackholeNoReceiveAck)
	client.clearQuarantine()

	got := client.reentryPenalty(45 * time.Second)
	if got <= 0 || got > reentryPenaltyWeight {
		t.Fatalf("a just-released exit must carry a positive penalty at or under the full weight, got %v", got)
	}
}

// TestQuarantineExpiryShortCircuitsReconvictionLookup is fix-round-1's
// required proof: quarantineReconvictionCount() takes stateLock, and
// benchDuration is called as a plain function -- Go evaluates every argument
// expression before the call, so passing self.quarantineReconvictionCount()
// directly as an argument (the original shape) took the lock on every
// blackhole-branch pass even when QuarantineDampening was false and
// benchDuration immediately discarded the value. The fix moves the knob
// check before the lookup entirely, so the lock is never taken on the off
// path.
//
// This is a STRUCTURAL test, not a call-counting one: quarantineReconvictionCount
// has no seam to count real lock acquisitions without adding call-counting
// instrumentation to production code purely to make this testable, which is
// out of scope. It protects exactly one property -- the reconviction lookup
// is unreachable when the knob is off -- via three checks:
//  1. quarantineReconvictionCount() is called exactly once in detectBlackhole;
//  2. that one call sits somewhere inside an enclosing `if` whose condition
//     mentions QuarantineDampening, with nothing closing that block before
//     reaching the call;
//  3. the off-path default is assigned from
//     self.settings.StatsWindowKeepUnhealthyDuration strictly before that
//     guard, so an unguarded read never sees anything else.
//
// It deliberately does NOT assert the guard's exact text, that the guard is
// the line immediately before the call, or an exact indentation delta --
// fix-round-1's first version of this test pinned all three, and detectBlackhole
// is a demonstrated hotspot (tasks 7, 8, 9 and this fix have all touched it
// in quick succession); a benign rename, an `if x := f(); x` simplified to
// `if f()`, or a wrapped block would have failed a perfectly correct change.
// Do not "tighten" this back to exact-text/adjacency/indent-delta matching.
//
// This test was verified against the pre-fix shape (self.quarantineReconvictionCount()
// passed directly as benchDuration's first argument, unconditionally) and
// fails there as expected -- see the task-9 report's fix-round-2 section for
// the captured failing output.
func TestQuarantineExpiryShortCircuitsReconvictionLookup(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *multiClientChannel) detectBlackhole()")
	if !ok {
		t.Fatal("could not find detectBlackhole")
	}
	lines := strings.Split(body, "\n")
	indentOf := func(s string) int {
		return len(s) - len(strings.TrimLeft(s, "\t"))
	}

	if n := strings.Count(body, "self.quarantineReconvictionCount()"); n != 1 {
		t.Fatalf("expected exactly one call to quarantineReconvictionCount in detectBlackhole, found %d", n)
	}

	callLine := -1
	for i, line := range lines {
		if strings.Contains(line, "self.quarantineReconvictionCount()") {
			callLine = i
			break
		}
	}
	if callLine < 0 {
		t.Fatal("could not locate the quarantineReconvictionCount call line")
	}

	// walk backward from the call, tracking the innermost enclosing scope by
	// indent: the first non-blank line at a strictly shallower indent than
	// the current threshold is that scope's opening line. If it is not the
	// dampening guard, treat it as the new threshold and keep walking
	// outward -- this naturally requires nothing at or below a candidate
	// guard's indent to intervene before the call, without hardcoding
	// adjacency or an exact delta.
	guardLine := -1
	threshold := indentOf(lines[callLine])
	for i := callLine - 1; 0 <= i; i-- {
		trimmed := strings.TrimSpace(lines[i])
		if trimmed == "" {
			continue
		}
		ind := indentOf(lines[i])
		if threshold <= ind {
			continue
		}
		if strings.Contains(trimmed, "if") && strings.Contains(trimmed, "QuarantineDampening") && strings.HasSuffix(trimmed, "{") {
			guardLine = i
			break
		}
		threshold = ind
	}
	if guardLine < 0 {
		t.Fatal("quarantineReconvictionCount is not nested inside any enclosing `if ... QuarantineDampening ... {` guard")
	}

	defaultLine := -1
	for i, line := range lines {
		if strings.Contains(line, "quarantineExpiry := self.settings.StatsWindowKeepUnhealthyDuration") {
			defaultLine = i
			break
		}
	}
	if defaultLine < 0 {
		t.Fatal("the off-path default (quarantineExpiry := self.settings.StatsWindowKeepUnhealthyDuration) is missing")
	}
	if guardLine <= defaultLine {
		t.Fatal("the dampening guard must come AFTER the off-path default is assigned, or the default is not really the off-path value")
	}
}
