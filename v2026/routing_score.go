package connect

import "math"

// ExitMetrics is the per-(class,exit) signal snapshot the scorer reads. Every
// field is already collected elsewhere in this file's machinery; the scorer is
// pure and holds no locks.
type ExitMetrics struct {
	RttMillis          float64
	GoodputBytesPerSec float64
	StallEvents        int
	Jitter             float64
	Flows              int
}

// ScoreWeights weights each normalized metric. Per-class weights let the same
// scorer prefer low latency for interactive flows and high throughput for bulk.
type ScoreWeights struct {
	Rtt     float64
	Goodput float64
	Stall   float64
	Jitter  float64
}

func classWeights(c TrafficClass) ScoreWeights {
	switch c {
	case ClassLatency:
		return ScoreWeights{Rtt: 1.0, Goodput: 0.2, Stall: 1.0, Jitter: 0.8}
	case ClassStreaming:
		return ScoreWeights{Rtt: 0.4, Goodput: 0.8, Stall: 1.0, Jitter: 0.9}
	case ClassBulk:
		return ScoreWeights{Rtt: 0.1, Goodput: 1.0, Stall: 0.6, Jitter: 0.1}
	case ClassBrowsing:
		return ScoreWeights{Rtt: 0.6, Goodput: 0.6, Stall: 0.8, Jitter: 0.4}
	case ClassBackground:
		return ScoreWeights{Rtt: 0.2, Goodput: 0.7, Stall: 0.5, Jitter: 0.2}
	default: // ClassUnknown: balanced, class-neutral
		return ScoreWeights{Rtt: 0.5, Goodput: 0.5, Stall: 0.7, Jitter: 0.3}
	}
}

// exitScore returns a composite health score for an exit with the given metrics
// and traffic class weights. The score is bounded and sanitized to ensure
// hostile telemetry (negative, NaN, +Inf values) scores as WORST, not best.
// Higher scores indicate better exits. The function is pure: no locks, I/O,
// or time calls.
//
// Telemetry from untrusted third-party providers can report garbage values.
// A scoring function that fails toward "route traffic here" is worse than one
// that fails toward "avoid this." Thus: positive infinity RTT (unreachable),
// NaN RTT (unknown), or negative RTT (corrupt) all deserve the worst possible
// sub-score (0.0), not the best (1.0 from clamping to 0ms).
func exitScore(m ExitMetrics, w ScoreWeights) float64 {
	// Calculate sub-scores; guard RTT and Jitter against hostile inputs.
	var rttGood float64
	if m.RttMillis < 0 || math.IsNaN(m.RttMillis) || math.IsInf(m.RttMillis, 0) {
		rttGood = 0 // worst score for corrupt/missing data
	} else {
		rttGood = 1.0 / (1.0 + m.RttMillis/50.0) // 50ms → 0.5
	}

	var jitterGood float64
	if m.Jitter < 0 || math.IsNaN(m.Jitter) || math.IsInf(m.Jitter, 0) {
		jitterGood = 0 // worst score for corrupt/missing data
	} else {
		jitterGood = 1.0 / (1.0 + m.Jitter/20.0) // 20ms → 0.5
	}

	// Goodput's clamp-to-0 behavior is correct (yields worst score), so no guard needed.
	goodput := m.GoodputBytesPerSec
	if goodput < 0 || math.IsNaN(goodput) || math.IsInf(goodput, 0) {
		goodput = 0
	}
	goodputGood := goodput / (goodput + 1e6) // 1MB/s → 0.5

	// Clamp StallEvents: negative values (corrupt/underflowed counter) must never
	// become a reward. Cap the penalty to prevent unboundedly negative scores:
	// max positive contribution is ~3.0 (perfect RTT + Goodput + Jitter), so
	// capping stallPenalty at 3.0 prevents hostile telemetry from pushing score
	// below -3.0. This caps StallEvents at 30; normal ranges (<10) remain
	// monotonically ordered.
	stallCount := m.StallEvents
	if stallCount < 0 {
		stallCount = 0
	}
	stallPenalty := float64(stallCount) * 0.1
	if stallPenalty > 3.0 {
		stallPenalty = 3.0
	}
	return w.Rtt*rttGood + w.Goodput*goodputGood + w.Jitter*jitterGood - w.Stall*stallPenalty
}

// challengerWins applies incumbent hysteresis using an absolute-value margin:
// a challenger only displaces the incumbent if it beats it by more than
// hysteresisPct percent of the absolute incumbent value. This ensures the
// threshold moves toward "better" regardless of score sign (positive, negative,
// or zero incumbent). When hysteresisPct==0, reduces to plain greater-than for
// all score signs, which is the legacy behavior and required by existing callers.
func challengerWins(incumbent, challenger, hysteresisPct float64) bool {
	return challenger > incumbent+math.Abs(incumbent)*hysteresisPct/100.0
}

// demotionState implements N-of-M rank demotion: an exit is only demoted after
// needBad consecutive bad intervals, so a single noisy sample never re-ranks the
// window. A good sample resets the streak. needBad<=1 is the legacy behavior
// (act on every sample). Convictions are unaffected — they are evidence-based
// and handled by the verdict layer, not by this score demotion.
type demotionState struct {
	bad int
}

func (d *demotionState) observe(good bool, needBad int) bool {
	if good {
		d.bad = 0
		return false
	}
	d.bad++
	return d.bad >= max(1, needBad)
}

// lessLoadedTieBreak returns true when challenger b should be preferred over
// incumbent a. When neither strictly wins under hysteresis (a near-tie), the
// less-loaded of the two wins — the power-of-two-choices anti-herding rule that
// spreads flows instead of piling them on one favorite. Outside the margin the
// higher score wins outright.
func lessLoadedTieBreak(aScore, bScore float64, aFlows, bFlows int, hysteresisPct float64) bool {
	if challengerWins(aScore, bScore, hysteresisPct) {
		return true
	}
	if challengerWins(bScore, aScore, hysteresisPct) {
		return false
	}
	return bFlows < aFlows
}
