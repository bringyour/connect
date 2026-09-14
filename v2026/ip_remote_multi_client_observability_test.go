package connect

import (
	"context"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- the grammar ---

// The formatter is the whole contract: every new line in the field goes
// through it, so its output shape is what a reconstruction greps. Pin the
// shape, the value renderings, and the escaping.
func TestRelEventGrammar(t *testing.T) {
	// the minimal line
	AssertEqual(t, relEvent("heartbeat"), "[rel] event=heartbeat")

	// keys are lowercased at the formatter, so one capitalized call site cannot
	// silently break `grep 'exit='`
	AssertEqual(t, relEvent("x", "Exit", "abc"), "[rel] event=x exit=abc")

	// bools render 1/0, durations render as millisecond integers
	line := relEvent("y", "on", true, "off", false, "bar", 1500*time.Millisecond)
	AssertEqual(t, line, "[rel] event=y on=1 off=0 bar=1500")

	// sub-millisecond durations floor to 0 rather than changing units mid-log
	AssertEqual(t, relEvent("y", "d", 500*time.Microsecond), "[rel] event=y d=0")

	// ints of every width, and a nil
	AssertEqual(
		t,
		relEvent("z", "a", 1, "b", int64(2), "c", uint64(3), "d", nil),
		"[rel] event=z a=1 b=2 c=3 d=-",
	)

	// an unpaired trailing argument is dropped rather than panicking: a log
	// line must never be the thing that takes down a tunnel
	AssertEqual(t, relEvent("w", "k"), "[rel] event=w")
}

// Values that could contain a space must be escaped, or a single free-text
// error turns one line into what a parser reads as five fields.
func TestRelEventEscaping(t *testing.T) {
	// bare when it is a single token
	AssertEqual(t, relValue("no-receive-ack"), "no-receive-ack")

	// quoted when it holds a space
	AssertEqual(t, relValue("liveness probe answered"), `"liveness probe answered"`)

	// quotes and backslashes are escaped inside
	AssertEqual(t, relValue(`say "hi"\`), `"say \"hi\"\\"`)

	// newlines and tabs would break the line-per-event property entirely
	AssertEqual(t, relValue("a\nb\tc"), `"a\nb\tc"`)

	// empty is reported as present-and-empty, not as a missing field
	AssertEqual(t, relValue(""), `""`)

	// errors go through the same escaping
	AssertEqual(
		t,
		relValue(errors.New("Blackhole no-receive-ack (send 1/2B)")),
		`"Blackhole no-receive-ack (send 1/2B)"`,
	)

	// and a whole line stays parseable with a spacey value in the middle
	line := relEvent("busy_probe", "exit", "a1b2c3d4", "detail", "probe timed out after 1.5s", "bar", 3*time.Second)
	AssertEqual(t, line, `[rel] event=busy_probe exit=a1b2c3d4 detail="probe timed out after 1.5s" bar=3000`)
}

// The exit key is the join column across every line in a capture. It must be
// the id's tail, must be stable, and must not panic on a zero id.
func TestRelExitId(t *testing.T) {
	clientId, err := ParseId("01234567-89ab-cdef-0123-456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	AssertEqual(t, relExitId(clientId), "89abcdef")
	// the same value the grammar renders for an Id argument
	AssertEqual(t, relValue(clientId), "89abcdef")

	// the zero id (a bare fixture channel, or one whose client is gone) is a
	// value, not a crash
	zero := relExitId(Id{})
	AssertEqual(t, len(zero), relExitIdLength)
}

// The removal twin's reason token is what makes `grep 'reason=no-receive-ack'`
// a census instead of a regex over free text.
func TestRelRemovalReason(t *testing.T) {
	for _, testCase := range []struct {
		err  error
		want string
	}{
		{nil, "none"},
		{errors.New("Blackhole no-receive-ack (send 1/2B recv 0/0B)"), "no-receive-ack"},
		{errors.New("Blackhole no-send-ack quarantine expired (send 1/2B)"), "no-send-ack"},
		{errors.New("send stalled: no ack progress for 3s"), "send-stalled"},
		{errors.New("cping timeout"), "cping"},
		{errTransportDownTimeout, "transport"},
		{errors.New(""), "unknown"},
	} {
		if got := relRemovalReason(testCase.err); got != testCase.want {
			t.Errorf("relRemovalReason(%v) = %q, want %q", testCase.err, got, testCase.want)
		}
	}
}

// --- the session banner ---

// The banner's job is to make a capture self-describing. Missing one field
// means the one knob being A/B'd that day is the one the capture cannot
// report, so the walk must cover the struct exhaustively -- by construction,
// not by a hand-maintained list that a later knob silently escapes.
func TestBannerRendersEverySettingsField(t *testing.T) {
	settings := ReliabilitySettingsFrom(DefaultMultiClientSettings())
	lines := relSessionBannerLines("", settings, relLineMaxChars)
	AssertEqual(t, len(lines), 1)

	body := lines[0]
	structType := reflect.TypeOf(*settings)
	for i := 0; i < structType.NumField(); i += 1 {
		key := strings.ToLower(structType.Field(i).Name) + "="
		if !strings.Contains(body, key) {
			t.Errorf("the session banner does not carry %s: a capture cannot report the setting that was in force", key)
		}
	}

	// and the pair count matches the field count exactly -- no field dropped,
	// none invented
	AssertEqual(t, len(relSettingsPairs(settings)), structType.NumField())

	// the shipped defaults render in the grammar's terms
	if !strings.Contains(body, "busyprobe=1") {
		t.Errorf("bools do not render as 1/0 in the banner: %s", body)
	}
	if !strings.Contains(body, "sendstalltimeout=3000") {
		t.Errorf("durations do not render as ms integers in the banner: %s", body)
	}
	if !strings.HasPrefix(body, "[rel] event=session settings=") {
		t.Errorf("the banner does not open with the session event: %s", body)
	}
}

// The build stamp is optional: a missing one is omitted rather than rendered
// as an empty or invented value.
func TestBannerBuildStamp(t *testing.T) {
	settings := &ReliabilitySettings{}

	lines := relSessionBannerLines("", settings, relLineMaxChars)
	AssertEqual(t, len(lines), 1)
	if strings.Contains(lines[0], "build=") {
		t.Errorf("an unset build is rendered anyway: %s", lines[0])
	}

	lines = relSessionBannerLines("beta-121", settings, relLineMaxChars)
	if !strings.HasPrefix(lines[0], "[rel] event=session build=beta-121 settings=") {
		t.Errorf("the build stamp is not in the banner: %s", lines[0])
	}

	// the package-level seam the host fills in
	SetBuildVersion("beta-121")
	AssertEqual(t, BuildVersion(), "beta-121")
	SetBuildVersion("")
	AssertEqual(t, BuildVersion(), "")
}

// logcat truncates a long line, and a truncated banner silently loses whichever
// fields happen to fall off the end. Past the bound the banner splits -- never
// mid-pair, always in the same order, so two captures are diffable.
func TestBannerSplitsDeterministically(t *testing.T) {
	settings := ReliabilitySettingsFrom(DefaultMultiClientSettings())
	pairs := relSettingsPairs(settings)

	// a bound far below the single-line length forces several continuations
	const maxLen = 120
	lines := relSessionBannerLines("beta-121", settings, maxLen)
	if len(lines) < 2 {
		t.Fatalf("the banner did not split at maxLen=%d: %v", maxLen, lines)
	}

	// deterministic: the same input splits the same way every time
	again := relSessionBannerLines("beta-121", settings, maxLen)
	if !reflect.DeepEqual(lines, again) {
		t.Error("the banner split is not deterministic, so two captures of the same config are not diffable")
	}

	// every line is self-describing and numbered in order
	recovered := []string{}
	for i, line := range lines {
		wantPrefix := fmt.Sprintf("[rel] event=session build=beta-121 settings%d=", i+1)
		if !strings.HasPrefix(line, wantPrefix) {
			t.Fatalf("line %d does not carry the continuation prefix %q: %s", i, wantPrefix, line)
		}
		recovered = append(recovered, strings.Fields(strings.TrimPrefix(line, wantPrefix))...)
	}

	// no pair is split across lines and none is lost: the concatenation is the
	// single-line payload exactly
	if !reflect.DeepEqual(recovered, pairs) {
		t.Errorf("the split lost or reordered pairs:\n got %v\nwant %v", recovered, pairs)
	}

	// and every line respects the bound (with the documented exception of a
	// single pair that cannot fit at all, which does not arise here)
	for _, line := range lines {
		if maxLen < len(line) {
			t.Errorf("a banner line exceeds the bound (%d > %d): %s", len(line), maxLen, line)
		}
	}
}

// A bound so small that no pair fits must still terminate and still emit every
// pair -- the degenerate case that would otherwise be an infinite loop in the
// constructor.
func TestBannerSplitTerminatesOnTinyBound(t *testing.T) {
	settings := ReliabilitySettingsFrom(DefaultMultiClientSettings())
	lines := relSessionBannerLines("", settings, 1)
	AssertEqual(t, len(lines), len(relSettingsPairs(settings)))
}

// A bare client (no settings at all) must produce a banner rather than a panic:
// the banner runs in the constructor, before anything else works.
func TestBannerBareClient(t *testing.T) {
	lines := relSessionBannerLines("", nil, relLineMaxChars)
	AssertEqual(t, len(lines), 1)
	if !strings.HasPrefix(lines[0], "[rel] event=session settings=") {
		t.Errorf("a nil settings banner is malformed: %s", lines[0])
	}
	// nil renders the zero value: every reliability behavior off
	if !strings.Contains(lines[0], "busyprobe=0") {
		t.Errorf("nil settings did not render as the zero value: %s", lines[0])
	}

	mc := &RemoteUserNatMultiClient{log: NewNoopLogger()}
	mc.logSessionBanner()
}

// --- the settings diff ---

// The diff is the single most useful line for reconstructing what the owner had
// toggled when a symptom appeared. It must report exactly the changed fields.
func TestSettingsDiffReportsExactlyTheChangedFields(t *testing.T) {
	before := ReliabilitySettingsFrom(DefaultMultiClientSettings())

	after := *before
	after.BusyProbe = false
	after.SendStallTimeout = 5 * time.Second

	changes := relSettingsDiff(before, &after)
	AssertEqual(t, len(changes), 2)

	byField := map[string]relSettingChange{}
	for _, change := range changes {
		byField[change.field] = change
	}
	AssertEqual(t, byField["busyprobe"], relSettingChange{field: "busyprobe", from: "1", to: "0"})
	AssertEqual(t, byField["sendstalltimeout"], relSettingChange{field: "sendstalltimeout", from: "3000", to: "5000"})

	lines := relSettingsDiffLines(before, &after)
	AssertEqual(t, len(lines), 2)
	for _, line := range lines {
		if !strings.HasPrefix(line, "[rel] event=setting field=") {
			t.Errorf("a settings line is not in the grammar: %s", line)
		}
	}
	if !strings.Contains(strings.Join(lines, "\n"), "[rel] event=setting field=busyprobe from=1 to=0") {
		t.Errorf("the busy probe change is not reported in the expected shape: %v", lines)
	}
}

// An unchanged write produces nothing. A developer menu writes the whole struct
// on every interaction, so a diff that reported the write rather than the
// change would bury the capture in noise.
func TestSettingsDiffUnchangedIsSilent(t *testing.T) {
	settings := ReliabilitySettingsFrom(DefaultMultiClientSettings())
	same := *settings

	AssertEqual(t, len(relSettingsDiff(settings, &same)), 0)
	AssertEqual(t, len(relSettingsDiffLines(settings, &same)), 0)
	// nil on both sides is the bare-client case
	AssertEqual(t, len(relSettingsDiff(nil, nil)), 0)
}

// SetReliabilitySettings must take the diff between EFFECTIVE configurations,
// so clearing an override reports the restoration instead of a fictional
// collapse to zero.
func TestSettingsDiffThroughTheSetter(t *testing.T) {
	log := newRecordingLogger()
	mc := &RemoteUserNatMultiClient{
		log:      log,
		settings: &MultiClientSettings{BusyProbe: true, SendStallTimeout: 3 * time.Second},
	}

	mc.SetReliabilitySettings(&ReliabilitySettings{BusyProbe: false, SendStallTimeout: 3 * time.Second})
	lines := log.linesWith("event=setting")
	AssertEqual(t, len(lines), 1)
	if !strings.Contains(lines[0], "field=busyprobe from=1 to=0") {
		t.Errorf("the override change was not reported: %v", lines)
	}

	// clearing restores the constructed settings, and that restoration is the
	// change worth logging
	log.reset()
	mc.SetReliabilitySettings(nil)
	lines = log.linesWith("event=setting")
	AssertEqual(t, len(lines), 1)
	if !strings.Contains(lines[0], "field=busyprobe from=0 to=1") {
		t.Errorf("clearing the override was not reported as a restoration: %v", lines)
	}

	// a write that changes nothing says nothing
	log.reset()
	mc.SetReliabilitySettings(nil)
	AssertEqual(t, len(log.linesWith("event=setting")), 0)
}

// --- the heartbeat ---

func heartbeatTestExits() []*ExitInfo {
	return []*ExitInfo{
		{Proven: true, FlowCount: 4, Tier: 0, EffectiveTier: 0},
		{Proven: false, FlowCount: 2, Tier: 0, EffectiveTier: 1, Warning: true, Quarantined: true},
		{Proven: true, FlowCount: 0, Tier: 1, EffectiveTier: 3, Warning: true},
		// a nil entry must not take down the beat
		nil,
	}
}

// two pinned apps sharing one exit: the healthy shape, and the one the beat
// has to be able to distinguish from "pinning never engaged"
func heartbeatTestAppPins() []*AppPin {
	clientId := Id{}
	clientId[0] = 7
	return []*AppPin{
		{AppId: "com.dd.doordash", ClientId: clientId},
		{AppId: "com.google.android.youtube", ClientId: clientId},
	}
}

// The beat has to be right, or a reconstruction reads the wrong session shape
// and chases the wrong fix.
func TestHeartbeatContentFromFixture(t *testing.T) {
	metrics := &ReliabilityMetricsSnapshot{
		VerdictsHeldUplinkStale:   7,
		VerdictsHeldTransportDown: 2,
		RemovalsDeferred:          1,
		RebindsAccepted:           9,
		RebindsRedialed:           3,
		ProbesSent:                40,
		ProbesAnswered:            38,
		ExitLossEvents:            5,
		GroupsFollowed:            6,
		GroupsScattered:           1,
	}

	// three exits' worth of telemetry: two with a completed RTT sample, one
	// (still forming, no acks yet) with none -- the NaN it reports per
	// exitMetricsSnapshot's contract must be excluded from the mean, not
	// averaged in as a fabricated 0ms.
	telemetry := []ExitMetrics{
		{GoodputBytesPerSec: 300, RttMillis: 20},
		{GoodputBytesPerSec: 600, RttMillis: 40},
		{GoodputBytesPerSec: 0, RttMillis: math.NaN()},
	}

	state := heartbeatStateFrom(heartbeatTestExits(), metrics, heartbeatTestAppPins(), telemetry)
	AssertEqual(t, state.exits, 3)
	AssertEqual(t, state.proven, 2)
	AssertEqual(t, state.quarantined, 1)
	// warned counts exits out of selection, which by isWarning's definition
	// includes the quarantined one
	AssertEqual(t, state.warned, 2)
	AssertEqual(t, state.flows, 6)
	AssertEqual(t, state.tierMin, 0)
	AssertEqual(t, state.tierMax, 3)

	// two pinned apps, both on one exit -- the shape that says pinning
	// engaged AND is holding
	AssertEqual(t, state.pinnedApps, 2)
	AssertEqual(t, state.pinnedExits, 1)

	// goodput means over all three (300+600+0)/3 = 300; rtt means over only
	// the two with a sample, (20+40)/2 = 30, and reports 2 measured of 3
	AssertEqual(t, state.goodputBps, uint64(300))
	AssertEqual(t, state.rttMillis, uint64(30))
	AssertEqual(t, state.rttMeasured, 2)

	line := relHeartbeatLine(state, 125*time.Second)
	want := "[rel] event=heartbeat exits=3 proven=2 quarantined=1 warned=2 flows=6 " +
		"tiers=0/3 goodput=300 rtt=30/2 held=7/2 fate=0 deferred=1 rebinds=9/3 probes=40/38 follow=6/1 pins=2/1 removals=5 uptime=125"
	AssertEqual(t, line, want)
}

// The distinction the beat exists to make: pinned apps placed on DIFFERENT
// exits is a split, and must not read the same as the healthy shape.
func TestHeartbeatPinsCountDistinctExits(t *testing.T) {
	clientA, clientB := Id{}, Id{}
	clientA[0], clientB[0] = 1, 2
	split := heartbeatStateFrom(nil, nil, []*AppPin{
		{AppId: "com.a", ClientId: clientA},
		{AppId: "com.b", ClientId: clientB},
	}, nil)
	AssertEqual(t, split.pinnedApps, 2)
	AssertEqual(t, split.pinnedExits, 2)

	// and nothing pinned reads as zero, never as an error
	none := heartbeatStateFrom(nil, nil, nil, nil)
	AssertEqual(t, none.pinnedApps, 0)
	AssertEqual(t, none.pinnedExits, 0)
}

// An empty pool and a nil metrics snapshot are both ordinary states (a window
// still forming, a bare client), not crashes.
func TestHeartbeatEmptyState(t *testing.T) {
	state := heartbeatStateFrom(nil, nil, nil, nil)
	AssertEqual(t, state, heartbeatState{})
	line := relHeartbeatLine(state, 0)
	if !strings.Contains(line, "exits=0") || !strings.Contains(line, "tiers=0/0") {
		t.Errorf("the empty beat is malformed: %s", line)
	}
}

// The suppression signature is the whole reason a heartbeat can be left on
// forever: an idle overnight session must stay quiet. Uptime must NOT be part
// of it, or the signature would always differ and suppress nothing.
func TestHeartbeatSignatureSuppression(t *testing.T) {
	metrics := &ReliabilityMetricsSnapshot{ProbesSent: 1}
	exits := heartbeatTestExits()

	first := heartbeatStateFrom(exits, metrics, nil, nil)
	second := heartbeatStateFrom(exits, metrics, nil, nil)
	if first != second {
		t.Error("two identical readouts produce different signatures, so an idle session would log every beat")
	}

	// the rendered line differs by uptime even though the signature does not --
	// which is exactly why uptime is excluded from the comparison
	if relHeartbeatLine(first, time.Second) == relHeartbeatLine(second, 2*time.Second) {
		t.Error("uptime is not rendered, so the beat cannot date itself")
	}

	// every counted dimension moves the signature
	for name, mutate := range map[string]func(*heartbeatState){
		"exits":       func(s *heartbeatState) { s.exits += 1 },
		"proven":      func(s *heartbeatState) { s.proven += 1 },
		"quarantined": func(s *heartbeatState) { s.quarantined += 1 },
		"warned":      func(s *heartbeatState) { s.warned += 1 },
		"flows":       func(s *heartbeatState) { s.flows += 1 },
		"tierMin":     func(s *heartbeatState) { s.tierMin -= 1 },
		"tierMax":     func(s *heartbeatState) { s.tierMax += 1 },
		"held":        func(s *heartbeatState) { s.heldUplink += 1 },
		"transport":   func(s *heartbeatState) { s.heldTransport += 1 },
		"deferred":    func(s *heartbeatState) { s.deferred += 1 },
		"accepted":    func(s *heartbeatState) { s.rebindsAccepted += 1 },
		"redialed":    func(s *heartbeatState) { s.rebindsRedialed += 1 },
		"probesSent":  func(s *heartbeatState) { s.probesSent += 1 },
		"answered":    func(s *heartbeatState) { s.probesAnswered += 1 },
		"removals":    func(s *heartbeatState) { s.removals += 1 },
		"fate":        func(s *heartbeatState) { s.heldSharedFate += 1 },
		"goodput":     func(s *heartbeatState) { s.goodputBps += 1 },
		"rtt":         func(s *heartbeatState) { s.rttMillis += 1 },
		"rttMeasured": func(s *heartbeatState) { s.rttMeasured += 1 },
	} {
		changed := first
		mutate(&changed)
		if changed == first {
			t.Errorf("a change in %s does not move the heartbeat signature, so it would be suppressed", name)
		}
	}
}

// The loop itself: it beats, it suppresses an unchanged beat, and it honors the
// runtime off switch without needing a reconnect.
func TestHeartbeatLoopSuppressesUnchangedBeats(t *testing.T) {
	log := newRecordingLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mc := &RemoteUserNatMultiClient{
		ctx:           ctx,
		cancel:        cancel,
		log:           log,
		settings:      &MultiClientSettings{HeartbeatInterval: 5 * time.Millisecond},
		windows:       map[WindowType]*multiClientWindow{},
		clientUpdates: map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
	}

	go mc.runHeartbeat()

	// the first beat establishes the baseline; every beat after it is identical
	// (no exits, no traffic) and must be suppressed
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if 0 < len(log.linesWith("event=heartbeat")) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := len(log.linesWith("event=heartbeat")); got != 1 {
		t.Fatalf("the heartbeat logged %d lines, want the single baseline beat", got)
	}

	// many intervals later, still one line: nothing changed
	time.Sleep(100 * time.Millisecond)
	if got := len(log.linesWith("event=heartbeat")); got != 1 {
		t.Errorf("an idle session logged %d heartbeat lines, want 1: the signature is not suppressing", got)
	}

	// turning it off at runtime silences it without stopping the goroutine
	mc.SetReliabilitySettings(&ReliabilitySettings{HeartbeatInterval: 0})
	time.Sleep(50 * time.Millisecond)
	log.reset()
	time.Sleep(50 * time.Millisecond)
	if got := len(log.linesWith("event=heartbeat")); got != 0 {
		t.Errorf("the heartbeat logged %d lines while switched off at runtime", got)
	}
}

// The constructor must not start the goroutine at all when the interval is 0,
// and the settings must round trip through the override struct.
func TestHeartbeatSettings(t *testing.T) {
	settings := DefaultMultiClientSettings()
	AssertEqual(t, settings.HeartbeatInterval, 60*time.Second)
	AssertEqual(t, ReliabilitySettingsFrom(settings).HeartbeatInterval, 60*time.Second)
	// zero-value-off, like every other knob in the override struct
	AssertEqual(t, ReliabilitySettingsFrom(nil).HeartbeatInterval, time.Duration(0))

	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func NewRemoteUserNatMultiClient(")
	if !ok {
		t.Fatal("could not find NewRemoteUserNatMultiClient")
	}
	if !strings.Contains(body, "0 < settings.HeartbeatInterval") {
		t.Error("the heartbeat goroutine is not gated on the constructed interval")
	}
	if !strings.Contains(body, "multiClient.logSessionBanner()") {
		t.Error("the constructor does not emit the session banner, so captures are not self-describing")
	}
}

// --- dev actions ---

// Every dev action must be logged: an exit death two lines after a DropExit is
// a completely different event from an unexplained one.
func TestActionLinesAreLogged(t *testing.T) {
	log := newRecordingLogger()
	mc := &RemoteUserNatMultiClient{
		log:           log,
		settings:      &MultiClientSettings{},
		windows:       map[WindowType]*multiClientWindow{},
		clientUpdates: map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
	}

	clientId, err := ParseId("01234567-89ab-cdef-0123-456789abcdef")
	if err != nil {
		t.Fatal(err)
	}

	AssertEqual(t, mc.DropExit(clientId), false)
	AssertEqual(t, mc.StallExit(clientId, true), false)
	mc.Shuffle()
	mc.ResetReliabilityMetrics()

	for _, want := range []string{
		"[rel] event=action name=drop_exit exit=89abcdef",
		"[rel] event=action name=stall_exit exit=89abcdef stalled=1",
		"[rel] event=action name=shuffle",
		"[rel] event=action name=reset_metrics",
	} {
		if len(log.linesWith(want)) != 1 {
			t.Errorf("missing action line %q in:\n%s", want, strings.Join(log.lines(), "\n"))
		}
	}
}

// ProbeAllExits is called from a ui thread through the sdk and a pass waits on
// the network for seconds. It must SCHEDULE, never run inline.
func TestProbeAllExitsIsNonBlocking(t *testing.T) {
	log := newRecordingLogger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	window := &multiClientWindow{
		ctx:     ctx,
		clients: map[Id]*multiClientChannel{},
	}
	// a ctx each, because the scheduler skips a done channel exactly as the
	// prober loop does
	clients := []*multiClientChannel{{ctx: ctx}, {ctx: ctx}, {ctx: ctx}}
	for i, client := range clients {
		window.clients[Id{byte(i + 1)}] = client
	}

	release := make(chan struct{})
	started := make(chan struct{}, len(clients))

	mc := &RemoteUserNatMultiClient{
		ctx:      ctx,
		cancel:   cancel,
		log:      log,
		settings: &MultiClientSettings{ProviderProbe: true},
		windows:  map[WindowType]*multiClientWindow{WindowTypeQuality: window},
		probePassFunc: func(client *multiClientChannel) {
			started <- struct{}{}
			<-release
		},
	}

	done := make(chan int, 1)
	go func() {
		done <- mc.ProbeAllExits()
	}()

	// the call returns while every pass is still blocked
	select {
	case n := <-done:
		AssertEqual(t, n, len(clients))
	case <-time.After(2 * time.Second):
		t.Fatal("ProbeAllExits did not return while its passes were in flight: it is running them inline")
	}

	// and the passes really were scheduled
	for i := 0; i < len(clients); i += 1 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatalf("only %d of %d passes started", i, len(clients))
		}
	}
	close(release)

	if len(log.linesWith("[rel] event=action name=probe_all exits=3")) != 1 {
		t.Errorf("the probe-all action was not logged: %v", log.lines())
	}
}

// With probing off the action must say so and do nothing -- the capture has to
// explain why nothing happened.
func TestProbeAllExitsSafeWithProbingOff(t *testing.T) {
	log := newRecordingLogger()
	mc := &RemoteUserNatMultiClient{
		log:      log,
		settings: &MultiClientSettings{ProviderProbe: false},
		windows:  map[WindowType]*multiClientWindow{},
		probePassFunc: func(client *multiClientChannel) {
			t.Error("a pass ran while ProviderProbe was off")
		},
	}

	AssertEqual(t, mc.ProbeAllExits(), 0)
	if len(log.linesWith("[rel] event=action name=probe_all exits=0 skipped=probe_off")) != 1 {
		t.Errorf("the refusal was not logged: %v", log.lines())
	}

	// a bare client (no windows at all) is a no-op, not a panic
	bare := &RemoteUserNatMultiClient{log: NewNoopLogger(), settings: &MultiClientSettings{ProviderProbe: true}}
	AssertEqual(t, bare.ProbeAllExits(), 0)
}

// The drill must exercise the production path, not a copy of it: the epoch
// reset and the transport kick are the behavior under test.
func TestSimulateNetworkChangeRoutesToTheEpochReset(t *testing.T) {
	log := newRecordingLogger()
	mc := &RemoteUserNatMultiClient{
		log:      log,
		settings: &MultiClientSettings{},
	}

	// an open stale epoch, as a live migration would leave it
	mc.uplinkStaleSince = time.Now().Add(-time.Minute)

	kicked := 0
	unsub := AddNetworkChangeListener(func() {
		kicked += 1
	})
	defer unsub()

	mc.SimulateNetworkChange()

	if !mc.uplinkStaleSince.IsZero() {
		t.Error("the drill did not close the stale epoch, so it is not exercising the production reset")
	}
	if mc.uplinkFreshSince.IsZero() {
		t.Error("the drill did not rebase the verdict clocks")
	}
	AssertEqual(t, kicked, 1)

	if len(log.linesWith("[rel] event=action name=network_change")) != 1 {
		t.Errorf("the drill was not logged as an action: %v", log.lines())
	}
	// the underlying event line is what a REAL migration also produces, so the
	// action line above is the only thing distinguishing a drill from the field
	if len(log.linesWith("[rel] event=network_change")) < 1 {
		t.Errorf("the network-change event line is missing: %v", log.lines())
	}

	// the seam routes through the production entry point rather than copying it
	source, err := readSource("ip_remote_multi_client_observability.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *RemoteUserNatMultiClient) SimulateNetworkChange()")
	if !ok {
		t.Fatal("could not find SimulateNetworkChange")
	}
	if !strings.Contains(body, "self.NotifyNetworkChanged()") {
		t.Error("SimulateNetworkChange does not route through NotifyNetworkChanged, so the drill can drift from the real path")
	}
}

// --- the formats the owner's tooling depends on ---

// THE regression this package must not cause. The blackhole verdict line and
// the teardown line are read by the owner's workflow and by this session's
// greps; a log line external tooling parses is an interface. They may gain a
// trailing [rel] twin, and nothing else.
func TestBlackholeVerdictAndTeardownFormatsUnchanged(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	for _, format := range []string{
		// the verdict line the storm breaker's evidence ends up on
		`"[multi]remove error client [%s] = %s`,
		// the teardown line that says the peer was told
		`"[multi]teardown sending %d packet(s) for %d flow(s) of client %s`,
		// and the skipped-teardown variant, which is the only case where flows
		// die with no signal at all
		`"[multi]teardown skipped, context done: %d packet(s) for %d flow(s) of client %s\n"`,
		// the verdict error itself, which is what the removal line prints
		`"Blackhole %s%s (send %d/%dB recv %d/%dB syn %d/%d nackAge %s synAge %s dsts=%d)"`,
	} {
		if !strings.Contains(source, format) {
			t.Errorf("the field-log format %s no longer exists: the owner's greps and any external tooling break silently", format)
		}
	}

	// the twins ride on the SAME line (no second Infof), so they can never be
	// separated by interleaving from another goroutine
	removal, ok := functionBody(source, "func (self *multiClientWindow) resize()")
	if !ok {
		t.Fatal("could not find resize")
	}
	if !strings.Contains(removal, `"[multi]remove error client [%s] = %s | %s\n"`) {
		t.Error("the removal verdict line lost its [rel] twin")
	}
	if !strings.Contains(removal, `"removal"`) {
		t.Error("the removal twin no longer emits event=removal")
	}

	teardown, ok := functionBody(source, "func (self *RemoteUserNatMultiClient) removeClient(")
	if !ok {
		t.Fatal("could not find removeClient")
	}
	if !strings.Contains(teardown, `"[multi]teardown sending %d packet(s) for %d flow(s) of client %s | %s\n"`) {
		t.Error("the teardown line lost its [rel] twin")
	}
	if !strings.Contains(teardown, `"teardown"`) {
		t.Error("the teardown twin no longer emits event=teardown")
	}
}

// The converted lines must actually be converted -- a grammar half the events
// ignore is worse than none, because a reconstruction cannot tell a missing
// event from an unconverted one.
func TestRelEventCallSitesExist(t *testing.T) {
	for _, testCase := range []struct {
		file   string
		events []string
	}{
		{
			file: "ip_remote_multi_client.go",
			events: []string{
				`"rebind"`,
				`"removal"`,
				`"teardown"`,
				`"deferral"`,
				`"collapse_defer"`,
				`"expand_decline"`,
				`"busy_probe"`,
				`"scheduler_pause"`,
				`"uplink"`,
				`"network_change"`,
				`"quarantine"`,
				`"quarantine_clear"`,
				`"quarantine_lift"`,
			},
		},
		{
			file:   "ip_remote_multi_client_probe.go",
			events: []string{`"probe"`},
		},
		{
			file:   "ip_remote_multi_client_prober.go",
			events: []string{`"probe_sweep"`},
		},
	} {
		source, err := readSource(testCase.file)
		if err != nil {
			t.Fatal(err)
		}
		// whitespace-normalized so a call broken across lines reads the same as
		// a single-line one; the assertion is that the event name is the FIRST
		// argument of a relEvent call, not merely present somewhere in the file
		compact := strings.Join(strings.Fields(source), " ")
		for _, event := range testCase.events {
			if !strings.Contains(compact, "relEvent("+event) &&
				!strings.Contains(compact, "relEvent( "+event) {
				t.Errorf("%s does not emit %s through the grammar", testCase.file, event)
			}
		}
	}
}

// The quarantine lift is decided under the receive path's lock and must be
// logged outside it: a logger is host code, and holding a per-packet lock
// across it is the failure this codebase has the house rule for.
func TestQuarantineLiftLogsOutsideTheLock(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *multiClientChannel) addReceiveAck(")
	if !ok {
		t.Fatal("could not find addReceiveAck")
	}

	unlock := strings.Index(body, "}()")
	logIndex := strings.Index(body, "quarantine_lift")
	if unlock < 0 || logIndex < 0 {
		t.Fatal("addReceiveAck no longer has the locked section and the lift log")
	}
	if logIndex < unlock {
		t.Error("the quarantine lift logs while the channel stateLock is held")
	}
}

// --- a recording logger for the tests above ---

// recordingLogger captures lines so a test can assert on what a field capture
// would contain. Concurrency-safe because the heartbeat writes from its own
// goroutine while the test reads.
type recordingLogger struct {
	mutex sync.Mutex
	out   []string
}

func newRecordingLogger() *recordingLogger {
	return &recordingLogger{}
}

// record takes the args as a plain slice rather than as a variadic forward.
// That is deliberate: a method with the shape `Infof(format string, args ...any)`
// that forwards straight into fmt.Sprintf teaches `go vet` to treat the whole
// `Logger` interface as printf-like, which then reports pre-existing format
// mismatches at unrelated call sites across the package. Whether to enroll the
// interface in printf checking is a package-wide decision, not something a test
// helper should make as a side effect.
func (self *recordingLogger) record(format string, args []any) {
	line := fmt.Sprintf(format, args...)
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.out = append(self.out, strings.TrimRight(line, "\n"))
}

func (self *recordingLogger) lines() []string {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return append([]string{}, self.out...)
}

func (self *recordingLogger) linesWith(substring string) []string {
	matched := []string{}
	for _, line := range self.lines() {
		if strings.Contains(line, substring) {
			matched = append(matched, line)
		}
	}
	return matched
}

func (self *recordingLogger) reset() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.out = nil
}

func (self *recordingLogger) Info(args ...any) {
	self.record("%s", []any{fmt.Sprint(args...)})
}

func (self *recordingLogger) Infof(format string, args ...any) {
	self.record(format, args)
}

func (self *recordingLogger) Warningf(format string, args ...any) {
	self.record(format, args)
}

func (self *recordingLogger) Errorf(format string, args ...any) {
	self.record(format, args)
}

func (self *recordingLogger) V(level int32) Verbose {
	return noopVerbose{}
}
