package connect

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Scan-friendly logging: the [rel] event grammar, the session banner, the
// heartbeat, the settings-change lines, and the dev-action seams.
//
// WHY A GRAMMAR AT ALL
//
// The primary debugging instrument on this project is post-hoc logcat
// forensics. The owner reports a symptom hours after it happened, the phone is
// plugged in later, and a multi-million-line ring buffer is greped for
// whatever the reconstruction needs. Two properties of that workflow decide
// everything in this file:
//
//  1. Verbosity is pinned. glog's level is fixed at 0 in sdk.go with no
//     runtime control, so V(1)/V(2) lines do not exist in the field. Anything
//     that must be reconstructable has to be a default-level Infof, and
//     anything at default level has to be cheap enough to leave on forever.
//     Every line added here is therefore either transition-triggered (once per
//     episode, not once per pass) or interval-triggered (once per minute, and
//     suppressed when it would repeat itself).
//
//  2. Reconstruction is a text operation. There is no structured sink, no
//     query engine, no correlation ids -- there is grep, and whatever the line
//     itself carries. Heterogeneous formats are what makes reconstruction slow:
//     every distinct phrasing is another pattern to remember and another awk
//     one-liner to write. So new lines share one shape:
//
//     [rel] <key>=<value> ...   with event=<name> always first
//
//     lowercase, stable, space separated, values quoted only when they could
//     contain a space. `grep '\[rel\] '` is the whole session; `grep 'exit=a1b2c3d4'`
//     is one provider's life; `grep 'event=heartbeat'` is the shape of the run.
//
// WHAT IS DELIBERATELY NOT CONVERTED
//
// The blackhole verdict line ("[multi]remove error client [...] = Blackhole ...")
// and the teardown line ("[multi]teardown sending N packet(s) ...") keep their
// exact historical format. The owner's workflow and this session's greps are
// built on them, and a log line that external tooling reads is an interface.
// They gain a trailing " | [rel] event=..." twin instead, which leaves every
// existing byte where it was and costs one extra formatted string on a path
// that already runs at most once per provider death.
//
// COST
//
// Nothing here runs on the per-packet path, and nothing here measures anything
// new. The banner is one line at construction; the settings lines are one per
// changed field per menu action; the heartbeat assembles from the existing
// Exits() walk and the existing metrics snapshot, both already lock-safe from a
// goroutine holding nothing. Its one real cost is that walk -- O(live flows)
// under the parent stateLock, once per interval, which is the same work one
// refresh of the developer screen already does and is paid whether or not the
// line is printed (the signature cannot be computed without it). The LINE is
// skipped when the signature is unchanged, so an idle overnight session
// contributes nothing to the buffer.

// relPrefix opens every structured line. The space is part of it: `grep '[rel] '`
// must not match a line that merely mentions [rel] in prose.
const relPrefix = "[rel] "

// relLineMaxChars is the length past which the session banner splits into
// continuation lines. logcat truncates a single log line at roughly 4000 bytes
// (the exact bound varies by Android version and by the formatter in use), and
// a truncated banner is worse than a split one: the fields that fall off are
// silently gone, and which ones fall off depends on the device. The margin
// below 4000 covers the timestamp/pid/tag preamble logcat prepends.
const relLineMaxChars = 3800

// relEvent formats one structured event line from alternating key/value
// arguments: relEvent("quarantine", "exit", id, "flows", 3) yields
// "[rel] event=quarantine exit=a1b2c3d4 flows=3".
//
// Pure and total: it never panics, never allocates on a hot path (there is no
// call site on one), and tolerates a malformed argument list rather than
// crashing a tunnel over a log line. A trailing unpaired argument is dropped; a
// non-string key is rendered through the same value formatter so the line is
// still readable rather than missing.
func relEvent(name string, keyValues ...any) string {
	var sb strings.Builder
	sb.WriteString(relPrefix)
	sb.WriteString("event=")
	sb.WriteString(relValue(name))
	for i := 0; i+1 < len(keyValues); i += 2 {
		key, ok := keyValues[i].(string)
		if !ok {
			key = fmt.Sprintf("%v", keyValues[i])
		}
		sb.WriteString(" ")
		// keys are lowercased here rather than trusted from the call site: the
		// grammar's whole value is that `grep 'exit='` finds every exit, and one
		// capitalized key at one call site silently breaks that for the one line
		// that mattered
		sb.WriteString(strings.ToLower(key))
		sb.WriteString("=")
		sb.WriteString(relValue(keyValues[i+1]))
	}
	return sb.String()
}

// relValue renders one value in the grammar's terms: durations as millisecond
// integers (a field capture compares numbers, and "1.5s" vs "1500ms" vs "1s500ms"
// are three spellings of one number), bools as 1/0 (so a settings blob greps as
// `busyprobe=1` rather than matching the substring of `busyprobe=true` in
// `busyprobefalse`), client ids as their 8-char tail, and everything else as
// text quoted only when it could contain a space.
func relValue(value any) string {
	switch typed := value.(type) {
	case nil:
		return "-"
	case string:
		return relQuote(typed)
	case bool:
		if typed {
			return "1"
		}
		return "0"
	// time.Duration and Id are both Stringers, so they must precede the
	// Stringer case below or they would render in their own spellings
	case time.Duration:
		return strconv.FormatInt(typed.Milliseconds(), 10)
	case Id:
		return relExitId(typed)
	case int:
		return strconv.Itoa(typed)
	case int32:
		return strconv.FormatInt(int64(typed), 10)
	case int64:
		return strconv.FormatInt(typed, 10)
	case uint32:
		return strconv.FormatUint(uint64(typed), 10)
	case uint64:
		return strconv.FormatUint(typed, 10)
	case float64:
		return strconv.FormatFloat(typed, 'f', 2, 64)
	case error:
		return relQuote(typed.Error())
	case fmt.Stringer:
		return relQuote(typed.String())
	default:
		return relQuote(fmt.Sprintf("%v", value))
	}
}

// relQuote quotes a value only when leaving it bare would break the
// space-separated reading of the line. Most values (reasons, states, counts)
// are single tokens and stay bare, which is what keeps the lines scannable by
// eye; errors and free text get quotes so a parser -- or the next `awk -F' '`
// -- does not silently see extra fields.
func relQuote(value string) string {
	if value == "" {
		// an empty bare value reads as a missing one; the quotes say the field
		// was present and empty, which is a different fact
		return `""`
	}
	quote := false
	for _, r := range value {
		if r == ' ' || r == '"' || r == '\\' || r < 0x20 || r == 0x7f {
			quote = true
			break
		}
	}
	if !quote {
		return value
	}
	var sb strings.Builder
	sb.WriteByte('"')
	for _, r := range value {
		switch r {
		case '"':
			sb.WriteString(`\"`)
		case '\\':
			sb.WriteString(`\\`)
		case '\n':
			sb.WriteString(`\n`)
		case '\r':
			sb.WriteString(`\r`)
		case '\t':
			sb.WriteString(`\t`)
		default:
			if r < 0x20 || r == 0x7f {
				// other control bytes would corrupt the line in a terminal; a
				// visible escape keeps the capture readable
				sb.WriteString(fmt.Sprintf(`\x%02x`, r))
			} else {
				sb.WriteRune(r)
			}
		}
	}
	sb.WriteByte('"')
	return sb.String()
}

// relExitIdLength is how much of a client id the grammar carries. A full uuid
// is 36 characters and would dominate every line; the last 8 hex characters are
// enough to identify one exit within a session by eye and to grep for, and they
// are the same tail the existing "[%s]" lines print in full so a capture can
// still be joined back to them.
const relExitIdLength = 8

// relExitId renders a client id as its 8-character tail. The zero id (a bare
// fixture channel, or a channel whose underlying client is gone) renders as its
// own tail rather than as an error: a log line must never be the thing that
// panics.
func relExitId(clientId Id) string {
	s := clientId.String()
	if len(s) <= relExitIdLength {
		return s
	}
	return s[len(s)-relExitIdLength:]
}

// relRemovalReason compresses a removal error into one greppable token for the
// removal twin. The full error stays on the same line in its historical
// position -- this is the token that makes `grep 'reason=no-receive-ack'`
// possible without a regex over free text.
func relRemovalReason(err error) string {
	if err == nil {
		return "none"
	}
	message := err.Error()
	// the verdict forms the window's storm breaker keys on, in their own
	// spelling: "Blackhole no-receive-ack (send ...)" and
	// "Blackhole no-receive-ack quarantine expired (send ...)"
	if rest, ok := strings.CutPrefix(message, "Blackhole "); ok {
		return relFirstToken(rest)
	}
	if strings.HasPrefix(message, "send stalled") {
		return "send-stalled"
	}
	return relFirstToken(message)
}

// relFirstToken takes the leading word of a message, lowercased, for use as a
// bare grammar value.
func relFirstToken(message string) string {
	if i := strings.IndexAny(message, " \t("); 0 <= i {
		message = message[:i]
	}
	message = strings.TrimSpace(message)
	if message == "" {
		return "unknown"
	}
	return strings.ToLower(message)
}

// --- the build stamp ---

// buildVersion is the optional build identifier stamped into the session
// banner. connect has no version constant of its own -- it is a library, and
// the number that matters in a field capture is the APK's -- so this is a seam
// the host fills in (the sdk at startup, or -ldflags -X on a build). Unset, the
// banner simply omits build=, which is honest: a missing field is better than a
// wrong one.
//
// An atomic rather than a plain var because the host writes it from its own
// startup goroutine while a multi-client may already be constructing.
var buildVersion atomic.Pointer[string]

// SetBuildVersion records the host's build identifier for the session banner.
func SetBuildVersion(version string) {
	if version == "" {
		buildVersion.Store(nil)
		return
	}
	buildVersion.Store(&version)
}

// BuildVersion reports the build identifier stamped into session banners, or
// "" when the host never set one.
func BuildVersion() string {
	if version := buildVersion.Load(); version != nil {
		return *version
	}
	return ""
}

// --- the session banner ---

// relSettingField is one settings field in grammar terms: the lowercase key and
// the already-rendered value.
type relSettingField struct {
	key   string
	value string
}

// relSettingsFields walks ReliabilitySettings and renders every exported field.
//
// Reflection rather than a hand-written list, deliberately. The banner's job is
// to make a capture self-describing, and a hand-written list fails that job
// silently the first time someone adds a knob and forgets the log -- which is
// exactly when the new knob is the thing being A/B'd and the reason the capture
// was taken. The cost is one reflection walk per construction and per settings
// change; there is no call site on any packet path.
//
// nil renders the zero value (every reliability behavior off), which is what a
// bare client reports.
func relSettingsFields(settings *ReliabilitySettings) []relSettingField {
	if settings == nil {
		settings = &ReliabilitySettings{}
	}
	value := reflect.ValueOf(*settings)
	structType := value.Type()
	fields := make([]relSettingField, 0, structType.NumField())
	for i := 0; i < structType.NumField(); i += 1 {
		field := structType.Field(i)
		if !field.IsExported() {
			continue
		}
		fields = append(fields, relSettingField{
			key:   strings.ToLower(field.Name),
			value: relValue(value.Field(i).Interface()),
		})
	}
	return fields
}

// relSettingsPairs renders the settings as `key=value` tokens in declaration
// order -- the banner's payload and the order a reader scans.
func relSettingsPairs(settings *ReliabilitySettings) []string {
	fields := relSettingsFields(settings)
	pairs := make([]string, 0, len(fields))
	for _, field := range fields {
		pairs = append(pairs, field.key+"="+field.value)
	}
	return pairs
}

// relSessionBannerLines builds the session banner: one line naming the build
// and the complete effective reliability configuration, so every capture says
// which arm of an A/B it is without anyone having to ask the owner what was
// toggled that day.
//
// The settings blob is the one place the grammar allows a value with spaces in
// it, unquoted: `settings=` is terminal on its line and runs to the end, which
// keeps the banner readable by eye (its main consumer) at the cost of a special
// case for a parser. Past maxLen the pairs are split across `settings1=`,
// `settings2=`, ... continuation lines -- never mid-pair, always in declaration
// order, so the same settings always split the same way and two captures are
// diffable line by line. Every continuation line repeats the event and build
// prefix so that a grep that lands on line 2 is still self-describing.
func relSessionBannerLines(build string, settings *ReliabilitySettings, maxLen int) []string {
	head := relPrefix + "event=session"
	if build != "" {
		head += " build=" + relValue(build)
	}

	pairs := relSettingsPairs(settings)
	if len(pairs) == 0 {
		return []string{head + " settings=" + `""`}
	}

	if single := head + " settings=" + strings.Join(pairs, " "); len(single) <= maxLen {
		return []string{single}
	}

	lines := []string{}
	index := 1
	i := 0
	for i < len(pairs) {
		key := " settings" + strconv.Itoa(index) + "="
		budget := maxLen - len(head) - len(key)
		chunk := []string{}
		size := 0
		for i < len(pairs) {
			cost := len(pairs[i])
			if 0 < len(chunk) {
				// the separating space
				cost += 1
			}
			// the first pair of a line is always taken even when it alone
			// exceeds the budget: a pair that cannot fit must still be emitted
			// (truncated by logcat, but present), and unconditionally taking one
			// is what guarantees this loop terminates
			if 0 < len(chunk) && budget < size+cost {
				break
			}
			chunk = append(chunk, pairs[i])
			size += cost
			i += 1
		}
		lines = append(lines, head+key+strings.Join(chunk, " "))
		index += 1
	}
	return lines
}

// logSessionBanner emits the banner for this client's effective settings.
// Called once, from the constructor, with no lock held.
func (self *RemoteUserNatMultiClient) logSessionBanner() {
	if self == nil {
		return
	}
	log := loggerOrDefault(self.log)
	for _, line := range relSessionBannerLines(BuildVersion(), self.reliabilitySettings(), relLineMaxChars) {
		log.Infof("%s\n", line)
	}
}

// --- settings-change lines ---

// relSettingChange is one field that differed between two effective
// configurations.
type relSettingChange struct {
	field string
	from  string
	to    string
}

// relSettingsDiff reports the fields that differ, in declaration order.
//
// This is the single most useful line in a reconstruction: it turns "the owner
// had something toggled" into a timestamped record of exactly what changed and
// when, in the same capture as the symptom. Comparing the RENDERED values (not
// the raw ones) is deliberate -- it means the diff can never claim a change the
// banner would not show, and the two are read together.
func relSettingsDiff(before *ReliabilitySettings, after *ReliabilitySettings) []relSettingChange {
	beforeFields := relSettingsFields(before)
	afterFields := relSettingsFields(after)
	changes := []relSettingChange{}
	// both walks come from the same struct type, so the indices align; the key
	// check is a cheap guard against that ever stopping being true
	for i := 0; i < len(beforeFields) && i < len(afterFields); i += 1 {
		if beforeFields[i].key != afterFields[i].key {
			continue
		}
		if beforeFields[i].value == afterFields[i].value {
			continue
		}
		changes = append(changes, relSettingChange{
			field: beforeFields[i].key,
			from:  beforeFields[i].value,
			to:    afterFields[i].value,
		})
	}
	return changes
}

// relSettingsDiffLines renders the diff as one grammar line per changed field.
// One line per field rather than one line listing them all: a capture is greped
// per field (`grep 'field=busyprobe'`), and a combined line would make that a
// regex over a list.
func relSettingsDiffLines(before *ReliabilitySettings, after *ReliabilitySettings) []string {
	changes := relSettingsDiff(before, after)
	lines := make([]string, 0, len(changes))
	for _, change := range changes {
		// the values are already rendered, so they are passed through the
		// grammar as raw tokens rather than re-quoted
		lines = append(lines, relEvent(
			"setting",
			"field", change.field,
			"from", change.from,
			"to", change.to,
		))
	}
	return lines
}

// --- the heartbeat ---

// defaultHeartbeatInterval is the fallback beat used when a client was built
// with an interval but the runtime override reads as unset.
const defaultHeartbeatInterval = 60 * time.Second

// heartbeatState is the live shape of the session at one instant. It is a plain
// comparable value on purpose: the suppression check is `state == last`, a
// single struct compare of a dozen machine words once a minute, which is what
// lets an idle overnight session cost nothing while a busy one still narrates
// itself. Uptime is deliberately NOT a member -- it changes every beat, and a
// signature that always differs suppresses nothing.
type heartbeatState struct {
	exits       int
	proven      int
	quarantined int
	warned      int
	flows       int
	tierMin     int
	tierMax     int

	heldUplink      uint64
	heldTransport   uint64
	deferred        uint64
	rebindsAccepted uint64
	rebindsRedialed uint64
	probesSent      uint64
	probesAnswered  uint64
	heldSharedFate  uint64
	removals        uint64
	groupsFollowed  uint64
	groupsScattered uint64
	// pinnedApps is how many pinned apps currently have a placement, and
	// pinnedExits how many distinct exits hold them. pinned/exits equal and
	// both non-zero is the healthy shape (each app on one exit); a jump in
	// pinnedExits past pinnedApps cannot happen by construction, but a
	// pinnedApps of 0 while the owner has pin rules is the signal the
	// mechanism never engaged.
	pinnedApps  int
	pinnedExits int

	// goodputBps is the mean per-exit effective goodput (send+receive
	// bytes/sec), from the exact windowStatsWithCoalesce(false) read
	// exitMetricsSnapshot uses for scoring -- so this is a direct readout of
	// the scorer's own input, not a separate measurement. 0 with exits>0 is
	// an honest "no window activity yet", the same reading a brand new
	// exit's own score would use.
	//
	// rttMillis is the mean RTT of ONLY the exits with a completed send-ack
	// round trip (see rttEwmaSnapshot's rttOk); rttMeasured counts how many
	// of the pool that mean covers. Always read the pair together --
	// rttMillis alone cannot distinguish "every exit is fast" from "nothing
	// has acked yet", which is exactly the zero-RTT trap exitMetricsSnapshot
	// itself is careful to avoid (see its doc).
	goodputBps  uint64
	rttMillis   uint64
	rttMeasured int
}

// heartbeatStateFrom folds the existing readouts into the beat's state. Pure,
// so the content is testable from a fixture without windows, clocks or
// goroutines -- every input is a snapshot the caller already took.
//
// `warned` counts exits new flows avoid, which by isWarning's definition
// includes the quarantined ones; the two are reported separately rather than
// made disjoint because "how many exits are out of selection" and "how many are
// out because a verdict was demoted" are different questions a reconstruction
// asks. Tiers are EFFECTIVE tiers (the rank selection actually uses), so a
// heartbeat showing tiers=0/3 on a good pool says three exits are carrying
// demerits.
//
// telemetry folds separately from exits: it has no per-ExitInfo association
// (exitTelemetrySnapshot is its own walk, over ExitMetrics, not ExitInfo),
// so goodput/rtt are pool-wide means rather than attributed to one row.
// GoodputBytesPerSec always contributes (0 is a real "no activity yet"
// reading, per exitScore's own guard); RttMillis only contributes when it is
// not NaN, i.e. when the channel completed at least one timed send-ack round
// trip -- averaging in the "unmeasured" sentinel would corrupt the mean with
// the very fabricated-zero exitMetricsSnapshot exists to avoid, and reporting
// nothing here (rttMeasured stays 0) is the honest reading of a pool that has
// not acked anything back yet.
func heartbeatStateFrom(exits []*ExitInfo, metrics *ReliabilityMetricsSnapshot, appPins []*AppPin, telemetry []ExitMetrics) heartbeatState {
	state := heartbeatState{}
	pinnedExits := map[Id]bool{}
	for _, appPin := range appPins {
		if appPin == nil {
			continue
		}
		state.pinnedApps += 1
		pinnedExits[appPin.ClientId] = true
	}
	state.pinnedExits = len(pinnedExits)
	first := true
	for _, exit := range exits {
		if exit == nil {
			continue
		}
		state.exits += 1
		if exit.Proven {
			state.proven += 1
		}
		if exit.Quarantined {
			state.quarantined += 1
		}
		if exit.Warning {
			state.warned += 1
		}
		state.flows += exit.FlowCount
		if first || exit.EffectiveTier < state.tierMin {
			state.tierMin = exit.EffectiveTier
		}
		if first || state.tierMax < exit.EffectiveTier {
			state.tierMax = exit.EffectiveTier
		}
		first = false
	}
	if metrics != nil {
		state.heldUplink = metrics.VerdictsHeldUplinkStale
		state.heldTransport = metrics.VerdictsHeldTransportDown
		state.deferred = metrics.RemovalsDeferred
		state.rebindsAccepted = metrics.RebindsAccepted
		state.rebindsRedialed = metrics.RebindsRedialed
		state.probesSent = metrics.ProbesSent
		state.probesAnswered = metrics.ProbesAnswered
		state.heldSharedFate = metrics.VerdictsHeldSharedFate
		state.removals = metrics.ExitLossEvents
		state.groupsFollowed = metrics.GroupsFollowed
		state.groupsScattered = metrics.GroupsScattered
	}
	if 0 < len(telemetry) {
		var goodputSum float64
		var rttSum float64
		for _, m := range telemetry {
			goodputSum += m.GoodputBytesPerSec
			if !math.IsNaN(m.RttMillis) && !math.IsInf(m.RttMillis, 0) {
				rttSum += m.RttMillis
				state.rttMeasured += 1
			}
		}
		state.goodputBps = uint64(goodputSum / float64(len(telemetry)))
		if 0 < state.rttMeasured {
			state.rttMillis = uint64(rttSum / float64(state.rttMeasured))
		}
	}
	return state
}

// relHeartbeatLine renders one beat. The paired fields (tiers, held, rebinds,
// probes) use a/b rather than two keys each because the pair is what is read --
// a rebind ratio, a probe answer rate -- and halving the key count keeps the
// line inside one logcat screen.
func relHeartbeatLine(state heartbeatState, uptime time.Duration) string {
	pair := func(a any, b any) string {
		return fmt.Sprintf("%v/%v", a, b)
	}
	return relEvent(
		"heartbeat",
		"exits", state.exits,
		"proven", state.proven,
		"quarantined", state.quarantined,
		"warned", state.warned,
		"flows", state.flows,
		"tiers", pair(state.tierMin, state.tierMax),
		// mean per-exit goodput (bytes/sec), and mean RTT (ms) over ONLY the
		// exits that have timed a round trip so far / how many that is out of
		// the pool -- read the pair together: rtt=0/0 is "nothing has acked
		// yet", not "every exit is instant".
		"goodput", state.goodputBps,
		"rtt", pair(state.rttMillis, state.rttMeasured),
		"held", pair(state.heldUplink, state.heldTransport),
		// destructive verdicts held because enough exits went silent inside
		// one shared-fate window that the shared path is the likely cause; a
		// rising count during a wave is the detector doing its job
		"fate", state.heldSharedFate,
		"deferred", state.deferred,
		"rebinds", pair(state.rebindsAccepted, state.rebindsRedialed),
		"probes", pair(state.probesSent, state.probesAnswered),
		// the G-1 ledger: follows a benched donor kept / scatters quarantine
		// still caused. A rising second number is the signal group-follow is
		// not doing its job (off, or the benched exits are receive-silent).
		"follow", pair(state.groupsFollowed, state.groupsScattered),
		// pinned apps placed / distinct exits holding them. Equal and
		// non-zero is healthy; 0 while pin rules exist means the mechanism
		// never engaged.
		"pins", pair(state.pinnedApps, state.pinnedExits),
		"removals", state.removals,
		"uptime", int64(uptime/time.Second),
	)
}

// runHeartbeat narrates the session once per HeartbeatInterval.
//
// The point is reconstruction from an arbitrary window of the buffer: any
// minute of a capture should say how many exits existed, how many were proven,
// how many were held out, how many flows were live, and what the recovery
// machinery had done so far -- without the dev screen, which is not available
// after the fact and never was during the incident.
//
// Two properties keep it free. It assembles only from readouts that already
// exist (the Exits() walk and the metrics snapshot), so nothing new is measured
// and nothing is added to the packet path; and it compares a signature against
// the previous beat and prints nothing when they match, so a phone sitting on a
// desk overnight contributes zero lines instead of 480.
//
// Plain time.After rather than WakeupAfter: a once-a-minute timer is not worth
// aligning to the wakeup grid, and a coalesced beat would blur the one property
// the interval has -- that consecutive lines are one interval apart.
//
// Tied to the multi-client ctx and NOT to `cancel`, like the prober and the
// pause detector: a logging goroutine must never be able to tear down the
// tunnel it exists to describe.
func (self *RemoteUserNatMultiClient) runHeartbeat() {
	startTime := time.Now()
	var last heartbeatState
	beaten := false

	var done <-chan struct{}
	if self.ctx != nil {
		done = self.ctx.Done()
	}

	for {
		// read the interval BEFORE the wait so a runtime change takes effect on
		// the next beat, and fall back to the constructed value when the
		// override clears it -- the same discipline the other loops here use
		wait := self.reliabilitySettings().HeartbeatInterval
		if wait <= 0 {
			if self.settings != nil && 0 < self.settings.HeartbeatInterval {
				wait = self.settings.HeartbeatInterval
			} else {
				wait = defaultHeartbeatInterval
			}
		}

		select {
		case <-done:
			return
		case <-time.After(wait):
		}

		// the reward-instrumentation fold+persist tick (Task 5): reuses this
		// goroutine's own wake cadence instead of a dedicated one, which is
		// what keeps priors persistence off the flow-close path -- see
		// foldRewardAndPersist's own doc. Deliberately BEFORE the runtime
		// heartbeat on/off check below, so toggling HeartbeatInterval off at
		// runtime silences the narration line without also stopping reward
		// samples from being folded and persisted. foldRewardAndPersist has
		// its own RewardInstrumentation gate, so this costs one lock
		// acquisition and nothing else when that knob is off.
		self.foldRewardAndPersist()

		// re-read after the wait: the heartbeat can be switched off at runtime,
		// and the loop stays alive so switching it back on costs no reconnect
		if self.reliabilitySettings().HeartbeatInterval <= 0 {
			continue
		}

		// all three readouts take their own locks internally and are called
		// with nothing held, which is the contract Exits(), the metrics
		// snapshot, and exitTelemetrySnapshot all document
		state := heartbeatStateFrom(self.Exits(), self.ReliabilityMetrics(), self.AppPins(), self.exitTelemetrySnapshot())
		if beaten && state == last {
			// nothing moved since the last beat; an idle session stays quiet
			continue
		}
		beaten = true
		last = state
		loggerOrDefault(self.log).Infof("%s\n", relHeartbeatLine(state, time.Since(startTime)))
	}
}

// --- dev actions ---

// logAction records a developer-menu invocation. Every dev action is logged
// because a field capture routinely contains the owner poking at the menu, and
// an unexplained exit death two lines after a DropExit is a very different
// event from an unexplained exit death.
func (self *RemoteUserNatMultiClient) logAction(name string, keyValues ...any) {
	if self == nil {
		return
	}
	loggerOrDefault(self.log).Infof("%s\n", relEvent(
		"action",
		append([]any{"name", name}, keyValues...)...,
	))
}

// ProbeAllExits schedules one qualification probe pass against every exit
// currently in the windows, and reports how many were scheduled.
//
// This is the connect-side seam for the developer menu's "probe all exits now"
// button. The prober's own loop only probes what its plan selects (never-probed
// exits, idle stale ones) which is right for the background sweep and useless
// when someone is standing in front of a misbehaving pool wanting an answer now.
//
// NON-BLOCKING by contract: a pass waits on the network for up to ~2x
// ProbeTimeout per exit, and this is called from a ui thread through the sdk.
// It gathers the client list, logs, and returns; the passes run on their own
// goroutines behind the same bounded semaphore the prober loop uses, so a full
// window cannot put more probe traffic on the wire than the background sweep
// would.
//
// Safe when ProviderProbe is off: it logs the refusal and no-ops rather than
// running passes the setting says are not wanted (probeProviderPass would
// refuse anyway; the point is that the capture says why nothing happened).
// Safe on a bare client with no windows.
func (self *RemoteUserNatMultiClient) ProbeAllExits() int {
	if self == nil {
		return 0
	}

	if !self.reliabilitySettings().ProviderProbe {
		self.logAction("probe_all", "exits", 0, "skipped", "probe_off")
		return 0
	}

	clients := []*multiClientChannel{}
	for _, window := range self.windows {
		if window == nil {
			continue
		}
		clients = append(clients, window.unorderedClients()...)
	}

	self.logAction("probe_all", "exits", len(clients))
	if len(clients) == 0 {
		return 0
	}

	pass := self.probePassFunc
	if pass == nil {
		pass = self.probeProviderPassIgnoringResult
	}

	var done <-chan struct{}
	if self.ctx != nil {
		done = self.ctx.Done()
	}

	// one goroutine per exit, each waiting its turn on the semaphore -- the same
	// shape runProber uses, so a "probe all" costs no more concurrent probe
	// traffic than a background sweep. The waiting happens in the goroutines,
	// never here: this loop only spawns, so the caller is released immediately.
	sem := make(chan struct{}, proberConcurrency)
	for _, client := range clients {
		go HandleError(func() {
			// the semaphore wait respects teardown, so a closing parent releases
			// the queued passes instead of holding them
			select {
			case sem <- struct{}{}:
			case <-done:
				return
			}
			defer func() {
				<-sem
			}()
			if client.IsDone() {
				return
			}
			pass(client)
		})
	}

	return len(clients)
}

// probeProviderPassIgnoringResult adapts the pass to the scheduling seam's
// signature. The result is recorded in the qualification table by the pass
// itself; nothing here consumes the return value.
func (self *RemoteUserNatMultiClient) probeProviderPassIgnoringResult(client *multiClientChannel) {
	self.probeProviderPass(client)
}

// SimulateNetworkChange fires the platform network-change path on demand: the
// uplink epoch reset and the process-wide transport kick that a real
// wifi-to-cellular migration triggers.
//
// This is the connect-side seam for the developer menu's "simulate network
// change" button, and it exists because the storm drill -- the failure mode the
// whole uplink-gate layer was built for -- otherwise requires physically moving
// between networks at the right moment. One tap reproduces it.
//
// It routes through NotifyNetworkChanged rather than duplicating its body, so
// the drill exercises the production path and cannot drift from it.
func (self *RemoteUserNatMultiClient) SimulateNetworkChange() {
	if self == nil {
		return
	}
	self.logAction("network_change")
	self.NotifyNetworkChanged()
}
