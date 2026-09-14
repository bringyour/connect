package connect

// Tests for the provider-churn placement compensation (ProbeSilenceWarnStreak).
//
// The field capture that motivates the mechanism (2026-08-04): providers on
// consumer devices went completely silent mid-session -- 0 of 127 probe
// answers including the dns resolution stage, repeated for 25+ minutes --
// while sitting in the window classified healthy, because a flowless corpse's
// idle stats look fine. Each one stayed selectable until real app traffic
// bound to it and ate the ~10-30s of dead syns that convicts. The streak
// warns the corpse out of new-flow placement in between; removal stays
// traffic-based and probes stay non-punitive for qualification.

import (
	"strings"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Total silence counts a strike per pass; one answer clears the streak even
// though these are the same probe passes that qualification reads. Driven
// through probeExit itself so the recording sites cannot drift from the
// mechanism.
func TestProbeSilenceStreakCountsAndClears(t *testing.T) {
	parent, client, _ := probeTestParent(t)

	// two passes that time out unanswered, with nothing resolved: silence
	for want := 1; want <= 2; want++ {
		result := parent.probeExit(client, []probeTarget{probeTestTarget()}, 200*time.Millisecond, 0)
		if result.Sent != 1 || result.Answered != 0 {
			t.Fatalf("silent pass %d = %d/%d answered, want 0/1", want, result.Answered, result.Sent)
		}
		if got := client.probeSilentStreak(); got != want {
			t.Errorf("streak after silent pass %d = %d, want %d", want, got, want)
		}
	}

	// an answered pass is proof of life and clears the streak
	resultCh := make(chan probeResult, 1)
	go func() {
		resultCh <- parent.probeExit(client, []probeTarget{probeTestTarget()}, 5*time.Second, 0)
	}()
	update := waitForProbeFlow(t, parent)
	ingressPath, packet := probeTestSynAck(t, update.probe.ipPath, 0x5150)
	parent.clientReceivePacket(client, TransferPath{}, protocol.ProvideMode_Public, TransportTypeUnknown, ingressPath, packet)
	select {
	case result := <-resultCh:
		if result.Answered != 1 {
			t.Fatalf("answered pass = %d answered, want 1", result.Answered)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("probeExit did not return after the answer landed")
	}
	if got := client.probeSilentStreak(); got != 0 {
		t.Errorf("streak after an answered pass = %d, want 0", got)
	}
}

// A pass with zero stage-B answers but a positive resolution count is NOT
// silence: the provider demonstrably carried dns answers back, so the device
// is on the network even if every health host refused the probe source.
// Counting it would warn exactly the provider the probe line's dns= field was
// added to exonerate.
func TestProbeSilenceResolvedCountsAsLife(t *testing.T) {
	parent, client, _ := probeTestParent(t)

	client.recordProbeSilence()
	client.recordProbeSilence()
	if got := client.probeSilentStreak(); got != 2 {
		t.Fatalf("setup streak = %d, want 2", got)
	}

	result := parent.probeExit(client, []probeTarget{probeTestTarget()}, 200*time.Millisecond, 3)
	if result.Answered != 0 || result.Resolved != 3 {
		t.Fatalf("pass = %d answered resolved=%d, want 0 answered resolved=3", result.Answered, result.Resolved)
	}
	if got := client.probeSilentStreak(); got != 0 {
		t.Errorf("streak after a resolved-only pass = %d, want 0: dns answers are proof of life", got)
	}
}

// Return traffic newer than the latest silent pass acquits the streak without
// waiting for the prober to come back around; return traffic older than the
// silence does not, because the silence is the newer evidence.
func TestProbeSilenceStreakReceiveAcquittal(t *testing.T) {
	client := &multiClientChannel{settings: DefaultMultiClientSettings()}

	// a receive OLDER than the silence must not acquit
	client.stateLock.Lock()
	client.lastReceiveAckTime = time.Now().Add(-time.Minute)
	client.stateLock.Unlock()
	client.recordProbeSilence()
	client.recordProbeSilence()
	if got := client.probeSilentStreak(); got != 2 {
		t.Errorf("streak with a stale receive = %d, want 2: old return traffic does not outrank new silence", got)
	}

	// a receive NEWER than the silence acquits, durably. The sleep is not
	// timing slack: equal timestamps deliberately keep the streak (silence
	// wins ties), and the windows wall clock ticks coarsely enough that
	// back-to-back stamps land on one tick. Real return traffic arrives
	// seconds after a probe pass, never on its completion tick.
	time.Sleep(20 * time.Millisecond)
	client.stateLock.Lock()
	client.lastReceiveAckTime = time.Now()
	client.stateLock.Unlock()
	if got := client.probeSilentStreak(); got != 0 {
		t.Errorf("streak after fresh return traffic = %d, want 0", got)
	}
	if got := client.probeSilentStreak(); got != 0 {
		t.Errorf("streak on re-read = %d, want 0: the acquittal must persist", got)
	}

	// and silence recorded after the acquittal counts again from zero
	client.recordProbeSilence()
	if got := client.probeSilentStreak(); got != 1 {
		t.Errorf("streak after new silence = %d, want 1", got)
	}
}

// The streak must actually be consulted at the resize warning site with the
// right shape, or the whole signal does nothing. Unit isolation of the resize
// pass is impractical, so pin the call site the way TestResizeWarnsOnDialStarved
// does. The ordering assertion is load-bearing: the silent branch must come
// AFTER the quarantine branch, or a quarantined exit that is also probe-silent
// skips the bench-time migration hand-off its flows depend on.
func TestResizeWarnsOnProbeSilence(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *multiClientWindow) resize()")
	if !ok {
		t.Fatal("could not find resize")
	}
	collapsed := strings.Join(strings.Fields(body), " ")

	if !strings.Contains(collapsed, "silenceStreak <= client.probeSilentStreak() && 1 < len(clientStats)") {
		t.Error("resize does not gate the silence warning on the streak AND on having somewhere else to go; a probe-silent corpse keeps attracting new flows, or the sole exit warns itself into a dead end")
	}
	if !strings.Contains(collapsed,
		"previousCause, causeChanged := client.setWarning(true, warnSilent) "+
			"self.logWarnTransition(client, previousCause, causeChanged, stats) "+
			"warnClient(client, stats)") {
		t.Error("the silence branch must warn (setWarning(true, warnSilent) + warnClient) so the size math backfills a replacement; keepClient would leave the corpse counted as capacity")
	}
	quarantineAt := strings.Index(collapsed, "client.isQuarantined()")
	silentAt := strings.Index(collapsed, "client.setWarning(true, warnSilent)")
	if quarantineAt == -1 || silentAt == -1 || silentAt < quarantineAt {
		t.Error("the silence branch must come after the quarantine branch, or a quarantined probe-silent exit skips the bench-time migration hand-off")
	}

	// the log grammar and the developer screen both read the cause string
	if got := warnSilent.String(); got != "silent" {
		t.Errorf("warnSilent.String() = %q, want \"silent\"", got)
	}
	// and the compensation exists in the field, where the corpses are
	if got := DefaultMultiClientSettings().ProbeSilenceWarnStreak; got != 2 {
		t.Errorf("default ProbeSilenceWarnStreak = %d, want 2", got)
	}
}
