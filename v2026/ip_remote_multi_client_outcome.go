package connect

import (
	"context"
	mathrand "math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Window honesty: the stall-reason diagnosis and the outcome deadline.
//
// The defect this file exists for (2026-08-11 forensics, both testers): a
// connect window with zero Added providers rendered climbing yellow dots
// FOREVER. Every evaluation failure was logged at V(1)/V(2) — invisible in the
// field, where glog's level is pinned to 0 — terminal states are deleted from
// the monitor map, and nothing anywhere owned the question "is this attempt
// going to succeed". The capture hole that CAUSED those failures is closed on
// its own track (egress-bound resolution); this file is the honesty layer that
// makes any future stall visible and bounded:
//
//  1. every evaluation-failure transition logs one default-level line
//     (count-suppressed on the logThrottle pattern, like the egress-dial
//     evidence lines);
//  2. the monitor's WindowExpandEvent carries a machine-readable stall reason
//     derived from the dominant recent failure class, so the app can say WHY
//     the window is yellow instead of only that it is;
//  3. a window that has Added nothing by WindowOutcomeDeadline gets ONE
//     automatic silent rebuild — the programmatic form of the manual
//     disconnect+reconnect that reliably recovered in the field — and a window
//     that has still Added nothing WindowOutcomeRebuildDeadline after that is
//     declared failed, terminally, with the reason. No infinite yellow, ever.
//
// The failed state is terminal for the UI, not for the machinery: enumeration
// and expansion keep running, and a provider that lands afterwards clears the
// state (noteClientAdded). The user's Retry rebuilds the whole session.

// The stall reasons surfaced through WindowExpandEvent.Reason (and from there
// through the sdk's WindowStatus). Machine-readable: the app switches on these
// exact strings, and the checkpoint test greps them.
const (
	WindowStallEvaluating            = "evaluating"
	WindowStallPlatformUnreachable   = "platform-unreachable"
	WindowStallProvidersUnresponsive = "providers-unresponsive"
	WindowStallRateLimited           = "rate-limited"
	WindowStallAuthFailing           = "auth-failing"
)

// windowFailureClass buckets one evaluation failure for the dominance count.
type windowFailureClass int

const (
	// the platform api did not answer (enumerate/create-args timeouts)
	windowFailurePlatform windowFailureClass = iota
	// a provider did not answer (channel create, evaluation ping)
	windowFailureProvider
	// the platform said slow down
	windowFailureRateLimit
	// the platform rejected our credentials
	windowFailureAuth

	windowFailureClassCount
)

func (self windowFailureClass) reason() string {
	switch self {
	case windowFailurePlatform:
		return WindowStallPlatformUnreachable
	case windowFailureRateLimit:
		return WindowStallRateLimited
	case windowFailureAuth:
		return WindowStallAuthFailing
	default:
		return WindowStallProvidersUnresponsive
	}
}

// stallReasonRank orders reasons for tie-breaks and for the merged monitor's
// pick across windows: the sharper the diagnosis, the higher the rank.
// platform-unreachable outranks everything because it names the root cause the
// other classes are usually downstream of.
func stallReasonRank(reason string) int {
	switch reason {
	case WindowStallPlatformUnreachable:
		return 4
	case WindowStallAuthFailing:
		return 3
	case WindowStallRateLimited:
		return 2
	case WindowStallProvidersUnresponsive:
		return 1
	default:
		return 0
	}
}

// classifyWindowFailure refines a call-site fallback class by sniffing the
// error text for the two classes only the message can reveal. String matching
// over error text is crude but honest to what the generator hands back: the
// platform api errors arrive as formatted strings, not typed values.
func classifyWindowFailure(err error, fallback windowFailureClass) windowFailureClass {
	if err == nil {
		return fallback
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "rate limit") ||
		strings.Contains(message, "too many requests") ||
		strings.Contains(message, "429"):
		return windowFailureRateLimit
	case strings.Contains(message, "auth") ||
		strings.Contains(message, "unauthorized") ||
		strings.Contains(message, "forbidden") ||
		strings.Contains(message, "401") ||
		strings.Contains(message, "403"):
		return windowFailureAuth
	}
	return fallback
}

// evaluationFailureLogInterval is the per-line-shape emission floor for the
// unconditional evaluation-failure lines, matching the egress-dial evidence
// cadence: a hung window retries continuously, and one line per shape per
// interval keeps the signal without the flood.
const evaluationFailureLogInterval = 5 * time.Second

// suppressedSuffix renders the "(N suppressed)" tail the throttled lines
// share with the existing "[t]auth error" and "[egress]dial" lines.
func suppressedSuffix(suppressed int64) string {
	if suppressed <= 0 {
		return ""
	}
	return " (" + strconv.FormatInt(suppressed, 10) + " suppressed)"
}

// windowFailureHorizon is how far back the dominance count looks. Longer than
// both outcome deadlines' default (45s) so the reason reported at rebuild/fail
// time reflects the whole empty-window period, not just its tail.
const windowFailureHorizon = 60 * time.Second

// windowFailureMaxPerClass bounds each class's timestamp list. The forensic
// failure rate is a few lines per 15s timeout cycle; 256 covers minutes of the
// worst observed storm, and past the cap the oldest entries are the ones
// dropped — exactly the ones the horizon trim would discard next anyway.
const windowFailureMaxPerClass = 256

// windowFailureRecorder counts recent evaluation failures per class. Its own
// small lock, never held while logging or dispatching.
type windowFailureRecorder struct {
	lock  sync.Mutex
	times [windowFailureClassCount][]time.Time
}

func (self *windowFailureRecorder) record(class windowFailureClass, now time.Time) {
	if self == nil || class < 0 || windowFailureClassCount <= class {
		// a bare fixture window has no recorder; a diagnosis must never panic
		return
	}
	self.lock.Lock()
	defer self.lock.Unlock()
	times := append(self.times[class], now)
	if windowFailureMaxPerClass < len(times) {
		times = times[len(times)-windowFailureMaxPerClass:]
	}
	self.times[class] = times
}

func (self *windowFailureRecorder) counts(now time.Time) [windowFailureClassCount]int {
	var counts [windowFailureClassCount]int
	if self == nil {
		return counts
	}
	horizonStart := now.Add(-windowFailureHorizon)
	self.lock.Lock()
	defer self.lock.Unlock()
	for class := range self.times {
		times := self.times[class]
		i := 0
		for ; i < len(times) && times[i].Before(horizonStart); i += 1 {
		}
		if 0 < i {
			times = times[i:]
			self.times[class] = times
		}
		counts[class] = len(times)
	}
	return counts
}

// deriveStallReason picks the dominant recent failure class; ties break to the
// sharper diagnosis (stallReasonRank). No failures at all is plain evaluating.
func deriveStallReason(counts [windowFailureClassCount]int) string {
	reason := WindowStallEvaluating
	best := 0
	for class, count := range counts {
		if count <= 0 {
			continue
		}
		classReason := windowFailureClass(class).reason()
		if best < count || (best == count && stallReasonRank(reason) < stallReasonRank(classReason)) {
			best = count
			reason = classReason
		}
	}
	return reason
}

// --- the window integration -------------------------------------------------

// windowName renders the window type for the [rel] grammar.
func (self *multiClientWindow) windowName() string {
	if name := self.windowType.RankMode(); name != "" {
		return name
	}
	return "auto"
}

// evalEpochContext is the context evaluation channels are built under. Between
// rebuilds it is simply a child of the window ctx, so nothing changes for a
// window that never rebuilds; a rebuild cancels the current epoch, which
// fails every in-flight evaluation candidate fast (their pings error out and
// take the ordinary fail() cleanup) so the fresh pass dials fresh sockets NOW
// instead of waiting out 15s timeouts on the old ones.
func (self *multiClientWindow) evalEpochContext() context.Context {
	self.outcomeLock.Lock()
	defer self.outcomeLock.Unlock()
	if self.evalEpochCtx == nil {
		// a bare fixture window: fall back to the window ctx, the pre-change
		// behavior
		return self.ctx
	}
	return self.evalEpochCtx
}

// armOutcome starts the outcome clock the first time the window actually tries
// to expand. Deliberately not construction time: the speed window can sit
// disabled (target 0) under a fixed-window profile, and a clock armed on a
// window that is not trying would rebuild a window that was never asked to
// form.
func (self *multiClientWindow) armOutcome() {
	self.outcomeLock.Lock()
	defer self.outcomeLock.Unlock()
	if self.outcomeArmTime.IsZero() {
		self.outcomeArmTime = time.Now()
	}
}

// noteClientAdded records that this window has installed a provider, which
// permanently disarms the outcome watchdog (the machinery past first-Added —
// blackhole verdicts, resize, rotation — owns recovery from there) and clears
// a latched failed state: reality improved, the UI must follow.
func (self *multiClientWindow) noteClientAdded(client *multiClientChannel) {
	if client != nil && client.IsDone() {
		// a straggler admitted from a cancelled evaluation epoch: it is already
		// dead and the resize pass will reap it. It must not count as the added
		// provider that disarms the watchdog.
		return
	}
	clearedFailed := false
	func() {
		self.outcomeLock.Lock()
		defer self.outcomeLock.Unlock()
		self.everAdded = true
		clearedFailed = self.outcomeFailed
		self.outcomeFailed = false
	}()
	if clearedFailed {
		self.log.Infof("%s\n", relEvent(
			"window_recovered",
			"window", self.windowName(),
		))
	}
	self.publishStallStatus()
}

// recordEvaluationFailure counts one classified failure and refreshes the
// published reason. Called with no locks held.
// Returns the class it recorded. The caller needs it: a rate limit is the one
// failure whose correct response is to SLOW DOWN, and the verdict used to be
// computed here, filed for the stall label, and thrown away — so the retry
// cadence for "the platform is refusing you for asking too often" was the same
// 1s as for any other error. See WindowRateLimitBackoffMin.
func (self *multiClientWindow) recordEvaluationFailure(fallback windowFailureClass, err error) windowFailureClass {
	class := classifyWindowFailure(err, fallback)
	self.failures.record(class, time.Now())
	self.publishStallStatus()
	return class
}

// windowRetryDelay converts a recorded failure into how long the expander
// should wait before asking again.
//
// Everything except a rate limit keeps the flat WindowEnumerateErrorTimeout.
// A rate limit doubles from WindowRateLimitBackoffMin toward
// WindowRateLimitBackoffMax per CONSECUTIVE rate limit, then takes uniform
// jitter across [delay/2, delay). The counter is shared by every expander in
// the window and is reset by windowRetryReset on any successful generator
// call, so a one-off 429 costs one backoff and a genuine storm decays.
func (self *multiClientWindow) windowRetryDelay(class windowFailureClass) time.Duration {
	if class != windowFailureRateLimit {
		return self.settings.WindowEnumerateErrorTimeout
	}
	min := self.settings.WindowRateLimitBackoffMin
	if min <= 0 {
		min = self.settings.WindowEnumerateErrorTimeout
	}
	max := self.settings.WindowRateLimitBackoffMax
	if max < min {
		max = min
	}
	// Bounded shift: consecutive counts climb without limit during a long
	// outage, and 1<<64 is not a delay.
	n := self.rateLimitStreak.Add(1) - 1
	delay := min
	for i := uint64(0); i < n && delay < max; i++ {
		delay *= 2
	}
	if delay > max {
		delay = max
	}
	// Jitter DOWNWARD only, so the cap is a real ceiling: dozens of expanders
	// that failed in the same instant must not retry in lockstep.
	half := delay / 2
	if half <= 0 {
		return delay
	}
	return half + time.Duration(mathrand.Int63n(int64(half)))
}

// windowRetryReset clears the rate-limit streak after a generator call that
// did NOT fail. Without this the window would stay in a long backoff after the
// platform recovered.
func (self *multiClientWindow) windowRetryReset() {
	self.rateLimitStreak.Store(0)
}

// stallReason derives the current diagnosis.
//
// The cheap platform-unreachable detector runs first: installed clients whose
// transports are ALL down is the live form of "verdicts held with no matching
// transport restored" (detectBlackhole's hold), and it outranks the failure
// counts because every other failure class is downstream of a dead platform
// transport.
func (self *multiClientWindow) stallReason() string {
	clients := self.unorderedClients()
	if 0 < len(clients) {
		anyTransport := false
		for _, client := range clients {
			if client.hasActiveTransport() {
				anyTransport = true
				break
			}
		}
		if !anyTransport {
			return WindowStallPlatformUnreachable
		}
	}
	return deriveStallReason(self.failures.counts(time.Now()))
}

// publishStallStatus pushes the current reason (and the failed latch) to the
// monitor, which dispatches to listeners only on change. The transition is
// logged here — once per change, never per pass — so the field capture carries
// the diagnosis timeline.
func (self *multiClientWindow) publishStallStatus() {
	reason := self.stallReason()
	failed := func() bool {
		self.outcomeLock.Lock()
		defer self.outcomeLock.Unlock()
		return self.outcomeFailed
	}()
	if self.monitor.SetStallStatus(reason, failed) {
		self.log.Infof("%s\n", relEvent(
			"window_stall",
			"window", self.windowName(),
			"reason", reason,
			"failed", failed,
		))
	}
}

// outcomeAction is what one watchdog pass decided.
type outcomeAction int

const (
	outcomeNone outcomeAction = iota
	outcomeRebuild
	outcomeFail
)

// windowOutcomeAction is the deadline state machine, pure so the transitions
// are pinned by tests without clocks or goroutines. `elapsed` is measured from
// the arm time, which the rebuild resets — so both deadlines measure their own
// span. A window that has Added, or is already failed, or was never armed,
// decides nothing.
func windowOutcomeAction(
	elapsed time.Duration,
	deadline time.Duration,
	rebuildDeadline time.Duration,
	armed bool,
	added bool,
	rebuilt bool,
	failed bool,
) outcomeAction {
	if deadline <= 0 || !armed || added || failed {
		return outcomeNone
	}
	if !rebuilt {
		if deadline <= elapsed {
			return outcomeRebuild
		}
		return outcomeNone
	}
	if 0 < rebuildDeadline && rebuildDeadline <= elapsed {
		return outcomeFail
	}
	return outcomeNone
}

// outcomeWatchPollTimeout is how often one enabled pass looks: a fraction of
// the deadline so the transition lands on roughly its own timescale, floored
// against a busy loop and capped at 1s so the reason line tracks within a
// second (the sendStallPollTimeout convention).
func outcomeWatchPollTimeout(deadline time.Duration, resizeTimeout time.Duration) time.Duration {
	if deadline <= 0 {
		// disabled idles at the resize cadence rather than exiting, so
		// enabling at runtime from the developer menu is picked up without a
		// reconnect
		return resizeTimeout
	}
	return min(max(deadline/8, 100*time.Millisecond), time.Second)
}

// watchOutcome is the outcome deadline: zero Added WindowOutcomeDeadline after
// the window first tries to expand triggers one silent rebuild; zero Added
// WindowOutcomeRebuildDeadline after that latches the failed state. Not wired
// to `cancel` — like the heartbeat and the prober, a watchdog that exists to
// describe and rescue the window must never be able to tear down the tunnel.
func (self *multiClientWindow) watchOutcome() {
	for {
		deadline := self.reliabilitySettings().WindowOutcomeDeadline
		rebuildDeadline := self.reliabilitySettings().WindowOutcomeRebuildDeadline

		select {
		case <-self.ctx.Done():
			return
		case <-time.After(outcomeWatchPollTimeout(deadline, self.settings.WindowResizeTimeout)):
		}
		if deadline <= 0 {
			continue
		}

		var armTime time.Time
		var rebuilt bool
		var failed bool
		var added bool
		func() {
			self.outcomeLock.Lock()
			defer self.outcomeLock.Unlock()
			armTime = self.outcomeArmTime
			rebuilt = self.outcomeRebuilt
			failed = self.outcomeFailed
			added = self.everAdded
		}()

		if !armTime.IsZero() && !added {
			// keep the published reason fresh while the window is empty: the
			// transport-down detector and the horizon trim both move without a
			// failure event to trigger a push
			self.publishStallStatus()
		}

		elapsed := time.Since(armTime)
		switch windowOutcomeAction(elapsed, deadline, rebuildDeadline, !armTime.IsZero(), added, rebuilt, failed) {
		case outcomeRebuild:
			self.rebuildWindow(elapsed)
		case outcomeFail:
			self.failOutcome(elapsed)
		}
	}
}

// rebuildWindow is the one automatic rescue: the programmatic form of the
// manual disconnect+reconnect that reliably un-stuck the field sessions —
// scoped to the window ONLY, never the tunnel. It cancels the evaluation
// epoch (failing every in-flight candidate fast, so fresh dials happen now),
// cancels any installed-but-dead clients, and kicks enumerate + resize for an
// immediate fresh pass. Silent from the UI's point of view: the status stays
// yellow, the dots reset, no failure is surfaced yet.
func (self *multiClientWindow) rebuildWindow(elapsed time.Duration) {
	reason := self.stallReason()

	var cancelEpoch context.CancelFunc
	aborted := false
	func() {
		self.outcomeLock.Lock()
		defer self.outcomeLock.Unlock()
		// REVALIDATE under the lock: the watchdog read `added` in its own
		// critical section and decided out here, so a provider can land in
		// the gap. Cancelling the epoch then would kill the client that just
		// proved the window works — and, because that Add permanently disarms
		// the watchdog, a still-hostile network would be back to unbounded
		// yellow with the failed latch unreachable. The Add already stood the
		// watchdog down; this rescue has nothing left to rescue.
		if self.everAdded {
			aborted = true
			return
		}
		cancelEpoch = self.evalEpochCancel
		epochCtx, epochCancel := context.WithCancel(self.ctx)
		self.evalEpochCtx = epochCtx
		self.evalEpochCancel = epochCancel
		self.outcomeRebuilt = true
		self.outcomeArmTime = time.Now()
	}()
	if aborted {
		return
	}
	loggerOrDefault(self.log).Infof("%s\n", relEvent(
		"window_rebuild",
		"window", self.windowName(),
		"reason", reason,
		"after", elapsed,
	))
	if cancelEpoch != nil {
		cancelEpoch()
	}

	// zero Added means no installed clients by construction; cancel any
	// straggler so the resize pass reaps it through the ordinary WindowStats
	// error branch rather than counting it toward the window size
	for _, client := range self.unorderedClients() {
		client.Cancel()
	}

	if self.generatorMonitor != nil {
		self.generatorMonitor.NotifyAll()
	}
	if self.resizeMonitor != nil {
		self.resizeMonitor.NotifyAll()
	}
}

// failOutcome latches the terminal failed state and publishes it with the
// reason. Terminal for the UI — the app renders a failure state with a Retry —
// while the machinery keeps running underneath: a provider that lands later
// clears the latch (noteClientAdded).
func (self *multiClientWindow) failOutcome(elapsed time.Duration) {
	reason := self.stallReason()
	aborted := false
	func() {
		self.outcomeLock.Lock()
		defer self.outcomeLock.Unlock()
		// same revalidation as rebuildWindow: a provider that landed after
		// the watchdog's decision has already cleared/disarmed this latch,
		// and setting it now would declare failed a window that just
		// installed a provider — with nothing left to clear the lie, because
		// noteClientAdded (the only other clearer) already ran.
		if self.everAdded {
			aborted = true
			return
		}
		self.outcomeFailed = true
	}()
	if aborted {
		return
	}
	loggerOrDefault(self.log).Infof("%s\n", relEvent(
		"window_failed",
		"window", self.windowName(),
		"reason", reason,
		"after", elapsed,
	))
	if self.monitor != nil {
		self.monitor.SetStallStatus(reason, true)
	}
}
