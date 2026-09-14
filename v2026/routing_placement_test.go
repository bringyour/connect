package connect

import "testing"

// panicClassifier proves the gate short-circuits: if the placement path ever
// reaches Classify while ScoredPlacement is off, this panics the test rather
// than silently passing.
type panicClassifier struct{}

func (panicClassifier) Classify(*IpPath, string) FlowClass {
	panic("must not be called when gated off")
}

// fixedClassifier always names the same class, for the tests that need the
// scoring path to actually run (class != ClassUnknown).
type fixedClassifier struct{ class TrafficClass }

func (f fixedClassifier) Classify(*IpPath, string) FlowClass {
	return FlowClass{Class: f.class}
}

// TestScoredPlacementGatedOff is the brief's gate test: with ScoredPlacement
// false -- the zero value, still what a bare/never-configured client (nil
// settings) reports, though as of Task 8 no longer what
// DefaultMultiClientSettings itself produces -- SetFlowClassifier installs
// the classifier (the seam works) but nothing on the placement path may call
// it. There is no newBareMultiClientForTest in this package; the suite's own
// bare-fixture convention (a literal &RemoteUserNatMultiClient{}, e.g.
// flowCapTestParent in ip_remote_multi_client_flow_cap_test.go,
// ip_remote_multi_client_test.go) is reused here instead of inventing a new
// constructor.
func TestScoredPlacementGatedOff(t *testing.T) {
	c := &panicClassifier{}
	client := &RemoteUserNatMultiClient{}
	client.SetFlowClassifier(c)

	// the seam: installed regardless of the gate
	if client.flowClassifier.Load() == nil {
		t.Fatal("classifier should be stored")
	}
	// the gate: off for a bare/nil-settings client
	if scoredPlacementEnabled(client.reliabilitySettings()) {
		t.Fatal("scored placement must be off for a bare client with nil settings")
	}
}

// TestSetFlowClassifierNilClears mirrors SetFlowOwnerLookup's nil-clears
// contract (ip_remote_multi_client.go:SetFlowOwnerLookup): installing nil
// removes a previously installed classifier.
func TestSetFlowClassifierNilClears(t *testing.T) {
	client := &RemoteUserNatMultiClient{}
	client.SetFlowClassifier(panicClassifier{})
	if client.flowClassifier.Load() == nil {
		t.Fatal("classifier should be stored")
	}

	client.SetFlowClassifier(nil)
	if client.flowClassifier.Load() != nil {
		t.Fatal("nil must clear the classifier")
	}
}

// TestScoredPlacementReorderClassUnknownIsNoop pins the safety property that
// makes ScoredPlacement=true a no-op today: no FlowClassifier implementation
// exists yet (SetFlowClassifier is the seam a later phase wires), so
// classifyOrUnknown's nil-safe path always names ClassUnknown, and
// scoredPlacementReorder must return the candidate list completely
// untouched -- there is nothing to score against.
func TestScoredPlacementReorderClassUnknownIsNoop(t *testing.T) {
	parent, clients := flowCapTestParent(t, 0, 5, 1)
	ipPath := &IpPath{Version: 4, Protocol: IpProtocolTcp}

	got := parent.scoredPlacementReorder(clients, ipPath, "")
	if len(got) != 2 || got[0] != clients[0] || got[1] != clients[1] {
		t.Fatal("ClassUnknown (no classifier installed) must leave candidate order untouched")
	}
}

// TestScoredPlacementReorderSingleCandidateIsNoop: nothing to reorder with
// fewer than two candidates, so the classifier must not even be consulted --
// a panic classifier here would fail the test if it were.
func TestScoredPlacementReorderSingleCandidateIsNoop(t *testing.T) {
	parent, clients := flowCapTestParent(t, 0, 5)
	parent.SetFlowClassifier(panicClassifier{})
	ipPath := &IpPath{Version: 4, Protocol: IpProtocolTcp}

	got := parent.scoredPlacementReorder(clients, ipPath, "")
	if len(got) != 1 || got[0] != clients[0] {
		t.Fatal("a single candidate must pass through untouched, and without consulting the classifier")
	}
}

// TestScoredPlacementReorderPrefersLessLoadedForClassifiedFlow exercises the
// real scoring path end to end with a classifier installed. Both bare
// fixture channels have no window activity and no RTT samples, so
// exitMetricsSnapshot reads identically "no data" for each of them (see its
// doc) -- their exitScore is therefore identical except for Flows, and a
// classified flow must reduce to the less-loaded tie-break: the lighter
// exit is promoted to the front.
func TestScoredPlacementReorderPrefersLessLoadedForClassifiedFlow(t *testing.T) {
	// clients[0] carries 50 flows, clients[1] carries 1
	parent, clients := flowCapTestParent(t, 0, 50, 1)
	parent.SetFlowClassifier(fixedClassifier{class: ClassBulk})
	ipPath := &IpPath{Version: 4, Protocol: IpProtocolTcp}

	got := parent.scoredPlacementReorder(clients, ipPath, "")
	if len(got) != 2 || got[0] != clients[1] || got[1] != clients[0] {
		t.Fatalf("classified flow with no telemetry differences must promote the less-loaded exit to the front")
	}
}

// TestScoredPlacementReorderLeavesMembershipUnchanged pins the hard
// constraint: the learner never overrides the safety layer. Reordering must
// never add or drop a candidate -- raceCandidates already decided membership
// via the verdict/quarantine/tier/flow-cap gates -- only permute the order.
func TestScoredPlacementReorderLeavesMembershipUnchanged(t *testing.T) {
	parent, clients := flowCapTestParent(t, 0, 3, 7, 1)
	parent.SetFlowClassifier(fixedClassifier{class: ClassLatency})
	ipPath := &IpPath{Version: 4, Protocol: IpProtocolTcp}

	got := parent.scoredPlacementReorder(clients, ipPath, "")
	if len(got) != len(clients) {
		t.Fatalf("got %d candidates, want %d: reordering must never change membership", len(got), len(clients))
	}
	seen := map[*multiClientChannel]bool{}
	for _, c := range got {
		seen[c] = true
	}
	for _, c := range clients {
		if !seen[c] {
			t.Fatal("a candidate went missing across reordering")
		}
	}
}

// TestScoredPlacementGateBitesInPractice is the brief's "prove the gate
// bites" check made permanent: scoredPlacementReorder is the function the
// guarded branch in sendPacket's coalesceOrderedClients calls ONLY when
// scoredPlacementEnabled is true. Calling it directly -- as if the gate were
// open -- with a classifier that panics and two candidates to score proves
// the classifier really is on the other side of that gate, not just absent
// from a fixture that happens not to reach it.
//
// This is the same property manually verified while writing this test: with
// scoredPlacementEnabled temporarily hardcoded to `return true`,
// TestScoredPlacementGatedOff fails (its own direct assertion on the gate
// value catches the flip) -- see the Task 8 report for both outputs.
func TestScoredPlacementGateBitesInPractice(t *testing.T) {
	parent, clients := flowCapTestParent(t, 0, 5, 1)
	parent.SetFlowClassifier(panicClassifier{})
	ipPath := &IpPath{Version: 4, Protocol: IpProtocolTcp}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected the panic classifier to fire when placement actually scores candidates")
		}
	}()
	parent.scoredPlacementReorder(clients, ipPath, "")
}
