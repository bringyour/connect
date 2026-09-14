package connect

// rewardAccumulator is the Phase 0 divergence gate: it records per-(class,exit)
// outcomes so a field capture can answer "do per-class rankings actually
// diverge" before any learner is built. Reward = class-normalized goodput +
// stall-free fraction. It is measurement only — it changes NO placement.
type rewardKey struct {
	Class  TrafficClass
	ExitId string
}

type rewardStat struct {
	samples    int
	goodputSum float64
	stallFreeN int
}

type rewardAccumulator struct {
	m map[rewardKey]*rewardStat
	// droppedSamples counts rejected add() calls -- SAMPLES, not distinct
	// keys -- since the last drainLines reset. See rewardAccumulatorMaxKeys's
	// own doc. The same never-admitted (class, exitId) key retried N times
	// after the cap is full counts N here, not 1: this field answers "how
	// many add() attempts did the cap cost me", not "how many distinct
	// provider/class combinations did I lose" -- the latter would need its
	// own bounded seen-but-rejected set, which just moves the unbounded-
	// growth problem the cap exists to prevent. This project has already
	// been bitten twice by a silent cap (the quarantine reconviction count
	// was the first); a bound that drops coverage without saying so is the
	// same defect again. drainLines surfaces this as its own [rel] line only
	// when it is nonzero, so an idle or unsaturated session costs nothing
	// extra to read.
	droppedSamples int
}

func newRewardAccumulator() *rewardAccumulator {
	return &rewardAccumulator{m: map[rewardKey]*rewardStat{}}
}

// rewardAccumulatorMaxKeys bounds the accumulator's memory independent of
// whether the interval fold (foldRewardAndPersist, on runHeartbeat's own
// wake cadence) is actually draining it. HeartbeatInterval==0 at
// construction never starts that goroutine at all (see runHeartbeat's own
// construction-time gate) -- not reachable from any shipped default (60s)
// or any non-test call site -- but with nothing folding it, an unbounded
// map would otherwise be a genuine leak across a very long session with
// many distinct (class, exit) pairs. Once at the cap, a brand-new key is
// dropped rather than added; an already-tracked key keeps accumulating (it
// is already paying rent, and this is a leak guard, not a fairness policy).
// A drop is never silent: rewardAccumulator.droppedSamples counts it (one
// per rejected add() call, not one per distinct key -- see droppedSamples's
// own doc), and drainLines surfaces the count on an event=reward_dropped
// line the next time it is nonzero. 256 mirrors maxRestoredWindowIdentityCount's own
// "several generations of slack over a normal window" sizing. Note the cap
// is really "256 / number-of-classes providers" in the worst case (all
// traffic filed under one TrafficClass), since keys are (class, exitId)
// pairs, not exitId alone.
const rewardAccumulatorMaxKeys = 256

func (r *rewardAccumulator) add(class TrafficClass, exitId string, goodputBytes float64, stallFree bool) {
	k := rewardKey{Class: class, ExitId: exitId}
	s := r.m[k]
	if s == nil {
		if rewardAccumulatorMaxKeys <= len(r.m) {
			r.droppedSamples++
			return
		}
		s = &rewardStat{}
		r.m[k] = s
	}
	s.samples++
	s.goodputSum += goodputBytes
	if stallFree {
		s.stallFreeN++
	}
}

// drainLines emits one grammar line per key, plus (only when nonzero) one
// more line reporting how many add() calls this interval rejected for
// naming a new key past rewardAccumulatorMaxKeys, then resets both the
// stats map and the drop counter. This is a SAMPLE count, not a
// distinct-key count: a single never-admitted key retried N times
// contributes N, not 1 -- see droppedSamples's own doc for why. Interval-
// triggered by the caller (the heartbeat tick), never per-packet.
//
// The drop line is deliberately NOT folded into event=reward as a per-key
// field: a drop is a fact about the accumulator as a whole (a key that never
// got an entry at all), not about any one key's stats, so it would be either
// duplicated onto every line or attached arbitrarily to one of them. A
// dedicated event, emitted only when there is something to report, keeps
// `grep 'event=reward'` free of a field that is almost always absent while
// still making an actual drop impossible to miss.
//
// Built on relEvent (ip_remote_multi_client_observability.go) like every
// other structured line in this codebase, rather than a hand-rolled format
// string, so a future grammar change (a new field, a rendering fix) lands
// here for free. exit= is truncated to relExitId's 8-char tail purely for
// display -- the SAME truncation every other [rel] exit= field uses -- so a
// capture can `grep 'exit=a1b2c3d4'` across a demote line, a classify line,
// and a reward line and get one provider's story. The untruncated id is what
// actually keys the map and what foldInto persists; only the log line is
// shortened.
func (r *rewardAccumulator) drainLines() []string {
	var lines []string
	if 0 < len(r.m) {
		lines = make([]string, 0, len(r.m)+1)
		for k, s := range r.m {
			avg := s.goodputSum / float64(s.samples)
			frac := float64(s.stallFreeN) / float64(s.samples)
			lines = append(lines, relEvent(
				"reward",
				"class", k.Class,
				"exit", rewardExitDisplay(k.ExitId),
				"samples", s.samples,
				"goodput", avg,
				"stallfree", frac,
			))
		}
	}
	if 0 < r.droppedSamples {
		lines = append(lines, relEvent(
			"reward_dropped",
			"droppedsamples", r.droppedSamples,
			"cap", rewardAccumulatorMaxKeys,
		))
	}
	r.m = map[rewardKey]*rewardStat{}
	r.droppedSamples = 0
	return lines
}

// rewardExitDisplay shortens a raw exit id string to relExitId's convention
// for the log line only -- the accumulator and provider-priors keys stay the
// untruncated string, so two different providers whose ids happen to share
// an 8-character tail can never be confused with each other in the data,
// only look similar in a human-scanned line.
func rewardExitDisplay(exitId string) string {
	if len(exitId) <= relExitIdLength {
		return exitId
	}
	return exitId[len(exitId)-relExitIdLength:]
}

// rewardScore turns one interval's raw goodput/stall-free stats into the
// [0,1] figure ProviderPriors.Observe expects: the same goodput
// normalization exitScore uses (1MB/s -> 0.5, routing_score.go), blended
// evenly with the stall-free fraction. Provider priors are deliberately
// class-agnostic (routing_priors.go's own doc: only the coarse,
// per-provider-identity shape survives a restart), so this collapses
// whatever mix of classes an exit carried this interval into one number,
// unlike the [rel] event=reward line above, which stays per-class.
func rewardScore(s rewardStat) float64 {
	if s.samples <= 0 {
		return 0
	}
	goodput := s.goodputSum / float64(s.samples)
	if goodput < 0 {
		goodput = 0
	}
	goodputGood := goodput / (goodput + 1e6)
	stallFree := float64(s.stallFreeN) / float64(s.samples)

	score := 0.5*goodputGood + 0.5*stallFree
	if score < 0 {
		score = 0
	} else if score > 1 {
		score = 1
	}
	return score
}

// foldInto blends each key's interval stats into priors' per-provider EWMA,
// keyed by the untruncated ExitId. Deliberately does NOT reset the
// accumulator -- the caller (foldRewardAndPersist) folds first and calls
// drainLines separately, under the same lock, so both operations read the
// exact same interval's data before it is cleared for the next one.
func (r *rewardAccumulator) foldInto(priors *ProviderPriors, nowUnix int64) {
	if priors == nil {
		return
	}
	for k, s := range r.m {
		priors.Observe(k.ExitId, rewardScore(*s), nowUnix)
	}
}
