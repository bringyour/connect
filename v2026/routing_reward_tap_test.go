package connect

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

// --- fixtures ---

// fakePriorsStore is an in-memory PriorsStore double that records every Save
// call so a test can assert persistence happened exactly when it should --
// interval-triggered, never per flow -- without touching a real dot-file.
type fakePriorsStore struct {
	mu     sync.Mutex
	loaded map[string]ProviderPrior
	saved  []map[string]ProviderPrior
}

func (f *fakePriorsStore) Load() map[string]ProviderPrior {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.loaded
}

func (f *fakePriorsStore) Save(m map[string]ProviderPrior) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.saved = append(f.saved, m)
	return nil
}

func (f *fakePriorsStore) saveCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.saved)
}

func (f *fakePriorsStore) lastSave() map[string]ProviderPrior {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.saved) == 0 {
		return nil
	}
	return f.saved[len(f.saved)-1]
}

// rewardTestChannel builds a fixture channel bound to providerId as its
// destination tail -- the STABLE provider identity recordFlowReward keys
// reward samples and priors on (Destination().Tail(), the same identity
// removeProvider/RemoveProvider already key on as egressClientId) -- with
// its own independent channelId as the channel's ephemeral, per-window-slot
// ClientId. A test picks whether the two coincide (the common
// single-channel case, via rewardTestGoodClient/rewardTestBadClient below)
// or deliberately differ (a reconnect: same provider, new channel -- see
// TestRecordFlowRewardKeyedByProviderIdentityNotChannelIdentity, which is
// exactly the scenario ClientId-keying got wrong: the api generator mints a
// fresh ClientId on every window (re)connect, per
// ip_remote_multi_client_identity.go's own doc).
func rewardTestChannel(providerId Id, channelId Id) *multiClientChannel {
	client := stallTestChannel()
	setClientId(client, channelId)
	client.args.Destination = RequireMultiHopId(providerId)
	return client
}

// rewardTestApplyGoodTelemetry gives a fixture channel real, positive
// goodput (the exact bucket/packetStats recipe
// TestExitMetricsSnapshotGoodputFromWindowStats uses) and no reconvictions,
// so exitMetricsSnapshot reads a genuinely good, stall-free outcome rather
// than a fabricated one.
func rewardTestApplyGoodTelemetry(client *multiClientChannel) {
	now := time.Now()
	client.stateLock.Lock()
	client.eventBuckets = []*multiClientEventBucket{
		{createTime: now.Add(-3 * time.Second), eventTime: now.Add(-3 * time.Second)},
		{createTime: now.Add(-2 * time.Second), eventTime: now.Add(-1 * time.Second)},
		{createTime: now, eventTime: now},
		{createTime: now, eventTime: now},
	}
	client.packetStats.sendAckByteCount = 500000
	client.packetStats.receiveAckByteCount = 500000
	client.stateLock.Unlock()
}

// rewardTestGoodClient builds a fixture channel (channelId == providerId,
// the common single-channel case) with real, positive goodput and no
// reconvictions.
func rewardTestGoodClient(providerId Id) *multiClientChannel {
	client := rewardTestChannel(providerId, providerId)
	rewardTestApplyGoodTelemetry(client)
	return client
}

// rewardTestBadClient builds a fixture channel (channelId == providerId)
// with no window activity (goodput 0) and one completed bench-then-lift
// cycle, so StallEvents=1 -- the same technique routing_demotion_test.go's
// demotionTestReconvict uses.
func rewardTestBadClient(providerId Id) *multiClientChannel {
	client := rewardTestChannel(providerId, providerId)
	client.setQuarantined(blackholeNoReceiveAck)
	client.clearQuarantine()
	return client
}

// --- recordFlowReward: the per-flow tap ---

// TestRecordFlowRewardGoodOutcomeRaisesPriorBadOutcomeLowers is the brief's
// central scenario: a flow that completes with good goodput and no stalls
// must raise its provider's prior above neutral (0.5), and a flow that
// stalls must lower it below neutral -- proven by folding both into
// ProviderPriors and reading Bias back, the same way scoredPlacementReorder
// would eventually consult it.
func TestRecordFlowRewardGoodOutcomeRaisesPriorBadOutcomeLowers(t *testing.T) {
	parent := &RemoteUserNatMultiClient{settings: DefaultMultiClientSettings()}
	parent.settings.RewardInstrumentation = true
	parent.SetFlowClassifier(fixedClassifier{class: ClassBulk})
	ipPath := &IpPath{Version: 4, Protocol: IpProtocolTcp}

	goodId, badId := NewId(), NewId()
	goodClient := rewardTestGoodClient(goodId)
	badClient := rewardTestBadClient(badId)

	parent.recordFlowReward(ipPath, "", goodClient, 0)
	parent.recordFlowReward(ipPath, "", badClient, 0)

	parent.foldRewardAndPersist()

	if parent.providerPriors == nil {
		t.Fatal("recording outcomes and folding must populate providerPriors")
	}
	goodBias := parent.providerPriors.Bias(goodId.String())
	badBias := parent.providerPriors.Bias(badId.String())
	if !(goodBias > 0.5) {
		t.Fatalf("a flow with good goodput and no stalls must raise its provider's prior above neutral: got %v", goodBias)
	}
	if !(badBias < 0.5) {
		t.Fatalf("a flow that stalls must lower its provider's prior below neutral: got %v", badBias)
	}
	if !(goodBias > badBias) {
		t.Fatalf("good outcome must score higher than bad: good=%v bad=%v", goodBias, badBias)
	}
}

// TestRecordFlowRewardKeyedByProviderIdentityNotChannelIdentity is the
// Critical defect the review caught: two DIFFERENT channels -- two
// different, ephemeral, locally-minted ClientIds, exactly what a reconnect,
// a demotion replacement, or a blackhole rebind mints fresh every time (per
// ip_remote_multi_client_identity.go's own doc: "the api generator mints an
// ephemeral platform client id ... for every window entry") -- that dial the
// SAME provider (the SAME Destination().Tail()) must accumulate into the
// SAME prior. Keying on ClientId instead makes the second channel's fold
// create a brand-new, unrelated entry, silently orphaning the first
// channel's history on every ordinary within-session reconnect, not just
// across a process restart.
func TestRecordFlowRewardKeyedByProviderIdentityNotChannelIdentity(t *testing.T) {
	parent := &RemoteUserNatMultiClient{settings: DefaultMultiClientSettings()}
	parent.settings.RewardInstrumentation = true
	parent.SetFlowClassifier(fixedClassifier{class: ClassBulk})
	ipPath := &IpPath{Version: 4, Protocol: IpProtocolTcp}

	providerId := NewId()

	// the first channel: some window slot, some ClientId, dialing providerId.
	firstChannel := rewardTestGoodClient(providerId)
	parent.recordFlowReward(ipPath, "", firstChannel, 0)
	parent.foldRewardAndPersist()

	if parent.providerPriors == nil {
		t.Fatal("recording a good outcome must have created providerPriors")
	}
	if snap := parent.providerPriors.Snapshot(); len(snap) != 1 {
		t.Fatalf("want exactly 1 provider prior after the first channel, got %d: %v", len(snap), snap)
	}

	// a RECONNECT to the SAME provider: a brand-new channel with a
	// brand-new ClientId, but the SAME destination.
	secondChannel := rewardTestChannel(providerId, NewId())
	rewardTestApplyGoodTelemetry(secondChannel)
	if firstChannel.ClientId() == secondChannel.ClientId() {
		t.Fatal("test fixture bug: the two channels must carry different ClientIds to prove the fix")
	}

	parent.recordFlowReward(ipPath, "", secondChannel, 0)
	parent.foldRewardAndPersist()

	snap := parent.providerPriors.Snapshot()
	if len(snap) != 1 {
		t.Fatalf("a reconnect to the SAME provider must fold into the SAME prior, not create a second one -- got %d entries: %v", len(snap), snap)
	}
	if _, ok := snap[providerId.String()]; !ok {
		t.Fatalf("the provider's prior must be keyed by its stable identity (Destination().Tail()), not the channel's ephemeral ClientId; got keys %v", snap)
	}
}

// TestRewardInstrumentationOffRecordsAndPersistsNothing pins the
// RewardInstrumentation=off contract: with it explicitly false, a completed
// flow -- even a good one -- must record no sample, allocate no accumulator,
// fold nothing into priors, persist nothing to the store, and log no [rel]
// event=reward line.
//
// RewardInstrumentation is set to false EXPLICITLY below rather than left
// unset: Task 8 (feat(routing): enable class-aware scored placement by
// default) made DefaultMultiClientSettings' own value true, so relying on
// the zero value here would no longer produce the off state this test needs.
func TestRewardInstrumentationOffRecordsAndPersistsNothing(t *testing.T) {
	parent := &RemoteUserNatMultiClient{settings: DefaultMultiClientSettings()}
	parent.settings.RewardInstrumentation = false // <-- explicit: no longer the default
	parent.SetFlowClassifier(fixedClassifier{class: ClassBulk})
	ipPath := &IpPath{Version: 4, Protocol: IpProtocolTcp}

	logger := newRecordingLogger()
	parent.log = logger
	store := &fakePriorsStore{loaded: map[string]ProviderPrior{}}
	parent.SetPriorsStore(store)

	client := rewardTestGoodClient(NewId())
	parent.recordFlowReward(ipPath, "", client, 0)
	if parent.reward != nil {
		t.Fatal("RewardInstrumentation=0 must record nothing -- the accumulator must stay nil (no allocation)")
	}

	parent.foldRewardAndPersist()
	if parent.providerPriors != nil {
		t.Fatal("RewardInstrumentation=0 must fold nothing into providerPriors")
	}
	if n := store.saveCount(); n != 0 {
		t.Fatalf("RewardInstrumentation=0 must persist nothing, got %d Save call(s)", n)
	}
	if lines := logger.linesWith("event=reward "); len(lines) != 0 {
		t.Fatalf("RewardInstrumentation=0 must emit no reward lines, got %v", lines)
	}
}

// --- foldRewardAndPersist: the interval-triggered half ---

// TestFoldRewardAndPersistEmitsRewardLineAndPersistsOnce proves the
// interval-triggered fold: one flow's outcome becomes exactly one
// [rel] event=reward line (via relEvent) and exactly one persisted snapshot,
// and a SECOND fold with no new samples in between must neither log nor
// persist again -- draining resets the accumulator, which is what makes this
// a timer-frequency write rather than a flow-close-frequency one.
func TestFoldRewardAndPersistEmitsRewardLineAndPersistsOnce(t *testing.T) {
	parent := &RemoteUserNatMultiClient{settings: DefaultMultiClientSettings()}
	parent.settings.RewardInstrumentation = true
	parent.SetFlowClassifier(fixedClassifier{class: ClassBulk})
	ipPath := &IpPath{Version: 4, Protocol: IpProtocolTcp}

	logger := newRecordingLogger()
	parent.log = logger
	store := &fakePriorsStore{loaded: map[string]ProviderPrior{}}
	parent.SetPriorsStore(store)

	id := NewId()
	client := rewardTestGoodClient(id)
	parent.recordFlowReward(ipPath, "", client, 3)

	parent.foldRewardAndPersist()

	lines := logger.linesWith("event=reward ")
	if len(lines) != 1 {
		t.Fatalf("want exactly 1 reward line, got %d: %v", len(lines), lines)
	}
	for _, want := range []string{"[rel] event=reward", "class=bulk", "samples=1"} {
		if !strings.Contains(lines[0], want) {
			t.Fatalf("reward line %q missing %q", lines[0], want)
		}
	}

	if n := store.saveCount(); n != 1 {
		t.Fatalf("want exactly 1 persist, got %d", n)
	}
	saved := store.lastSave()
	if _, ok := saved[id.String()]; !ok {
		t.Fatalf("persisted snapshot missing the exit's prior: %v", saved)
	}

	logger.reset()
	parent.foldRewardAndPersist()
	if lines := logger.linesWith("event=reward "); len(lines) != 0 {
		t.Fatalf("a second fold with no new samples must emit nothing, got %v", lines)
	}
	if n := store.saveCount(); n != 1 {
		t.Fatalf("a second fold with no new samples must not persist again, got %d saves", n)
	}
}

// --- production wiring, source-anchored ---

// TestFlowReaperRecordsFlowReward proves the tap is actually wired into the
// shared idle-flow close path, not just reachable from a test. IPv4 and IPv6
// are both detached into the same retired-flow list, so one hook covers both.
func TestFlowReaperRecordsFlowReward(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *RemoteUserNatMultiClient) finishRetiredFlows(")
	if !ok {
		t.Fatal("could not find finishRetiredFlows")
	}
	if n := strings.Count(body, "self.recordFlowReward("); n != 1 {
		t.Fatalf("want one shared recordFlowReward call site, got %d", n)
	}
}

// TestRunHeartbeatFoldsRewardIntoPriors proves the persistence cadence is the
// existing heartbeat tick, not a new goroutine: runHeartbeat's body must call
// foldRewardAndPersist on its own wake cadence.
func TestRunHeartbeatFoldsRewardIntoPriors(t *testing.T) {
	source, err := readSource("ip_remote_multi_client_observability.go")
	if err != nil {
		t.Fatal(err)
	}
	body, ok := functionBody(source, "func (self *RemoteUserNatMultiClient) runHeartbeat(")
	if !ok {
		t.Fatal("could not find runHeartbeat")
	}
	if !strings.Contains(body, "self.foldRewardAndPersist(") {
		t.Error("runHeartbeat does not fold reward samples into priors -- persistence never runs on its own timer")
	}
}

// --- SetPriorsStore ---

// TestSetPriorsStoreLoadsExistingPriors proves the round trip: installing a
// store with existing saved data must seed providerPriors from it, so a
// restart resumes with history rather than starting neutral.
func TestSetPriorsStoreLoadsExistingPriors(t *testing.T) {
	parent := &RemoteUserNatMultiClient{settings: DefaultMultiClientSettings()}
	store := &fakePriorsStore{loaded: map[string]ProviderPrior{
		"existing-provider": {ScoreEwma: 0.9, Convictions: 0, LastSeenUnix: 1000},
	}}
	parent.SetPriorsStore(store)

	if parent.providerPriors == nil {
		t.Fatal("SetPriorsStore must load existing data into providerPriors")
	}
	if bias := parent.providerPriors.Bias("existing-provider"); bias != 0.9 {
		t.Fatalf("loaded prior must be usable immediately: got bias %v want 0.9", bias)
	}
}

// TestSetPriorsStoreNilClears mirrors SetFlowClassifier's nil-clears
// contract: installing nil removes the store, restoring in-memory-only
// priors per PriorsStore's own doc.
func TestSetPriorsStoreNilClears(t *testing.T) {
	parent := &RemoteUserNatMultiClient{settings: DefaultMultiClientSettings()}
	store := &fakePriorsStore{loaded: map[string]ProviderPrior{}}
	parent.SetPriorsStore(store)
	if parent.priorsStore.Load() == nil {
		t.Fatal("store should be installed")
	}

	parent.SetPriorsStore(nil)
	if parent.priorsStore.Load() != nil {
		t.Fatal("nil must clear the store")
	}
}

// --- routing_reward.go: the cap drop counter, through the real tap ---

// TestRecordFlowRewardCapDropIsReportedOnFold is the production-wiring twin
// of TestRewardAccumulatorCapDropsAreCountedAndReported: it drives the cap
// past its limit through the REAL per-flow tap (recordFlowReward) and the
// REAL interval fold (foldRewardAndPersist), rather than poking the bare
// accumulator directly, so it also proves the drop counter reaches the
// actual [rel] log a field capture would see -- not just that the
// accumulator's own bookkeeping is correct in isolation.
func TestRecordFlowRewardCapDropIsReportedOnFold(t *testing.T) {
	parent := &RemoteUserNatMultiClient{settings: DefaultMultiClientSettings()}
	parent.settings.RewardInstrumentation = true
	parent.SetFlowClassifier(fixedClassifier{class: ClassBulk})
	ipPath := &IpPath{Version: 4, Protocol: IpProtocolTcp}

	logger := newRecordingLogger()
	parent.log = logger

	// one class, rewardAccumulatorMaxKeys+3 distinct providers: every call
	// is a genuinely new (class, exitId) key, so exactly 3 must be dropped.
	const overflow = 3
	for i := 0; i < rewardAccumulatorMaxKeys+overflow; i++ {
		client := rewardTestGoodClient(NewId())
		parent.recordFlowReward(ipPath, "", client, 0)
	}
	if parent.reward == nil || len(parent.reward.m) != rewardAccumulatorMaxKeys {
		t.Fatalf("want the accumulator capped at %d keys before the fold, got %+v", rewardAccumulatorMaxKeys, parent.reward)
	}
	if parent.reward.droppedSamples != overflow {
		t.Fatalf("want droppedSamples=%d on the live tap before the fold, got %d", overflow, parent.reward.droppedSamples)
	}

	parent.foldRewardAndPersist()

	dropLines := logger.linesWith("event=reward_dropped")
	if len(dropLines) != 1 {
		t.Fatalf("want exactly 1 event=reward_dropped line from the real fold, got %d: %v", len(dropLines), logger.lines())
	}
	for _, want := range []string{"[rel] event=reward_dropped", fmt.Sprintf("droppedsamples=%d", overflow), fmt.Sprintf("cap=%d", rewardAccumulatorMaxKeys)} {
		if !strings.Contains(dropLines[0], want) {
			t.Fatalf("reward_dropped line %q missing %q", dropLines[0], want)
		}
	}
	// the ordinary per-key reward lines must still be there too -- the cap
	// bites 3 providers, not the other 256 that fit.
	if got := len(logger.linesWith("event=reward ")); got != rewardAccumulatorMaxKeys {
		t.Fatalf("want %d event=reward lines for the 256 providers that fit, got %d", rewardAccumulatorMaxKeys, got)
	}
}

// --- routing_reward.go: foldInto / rewardScore ---

// TestRewardAccumulatorFoldIntoDoesNotReset proves foldInto reads the
// accumulator without draining it -- foldRewardAndPersist relies on folding
// BEFORE calling drainLines() under the same lock, and a foldInto that reset
// state would race the log line against the priors fold.
func TestRewardAccumulatorFoldIntoDoesNotReset(t *testing.T) {
	r := newRewardAccumulator()
	r.add(ClassBulk, "exitA", 2000000, true)

	priors := NewProviderPriors()
	r.foldInto(priors, 1000)

	if len(r.m) != 1 {
		t.Fatal("foldInto must not reset the accumulator")
	}
	if bias := priors.Bias("exitA"); !(bias > 0.5) {
		t.Fatalf("a stall-free, high-goodput sample must fold to an above-neutral prior: got %v", bias)
	}
}
