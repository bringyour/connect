package connect

import (
	"testing"
)

// --- fixtures ---

// setClientId gives a bare fixture channel a distinct ClientId, the way a
// real channel's args always do. multiClientChannel.ClientId() reads
// self.args.ClientId when self.client (the real *Client) is nil -- see its
// own nil-safety comment -- so this is the one field flowCapTestParent's bare
// fixtures need added to be distinguishable exits for demotion's keying.
func setClientId(client *multiClientChannel, id Id) {
	client.args = &multiClientChannelArgs{
		MultiClientGeneratorClientArgs: MultiClientGeneratorClientArgs{ClientId: id},
	}
}

// demotionTestReconvict gives client n completed bench-then-lift cycles, the
// same technique routing_telemetry_test.go's TestExitMetricsSnapshotStallEventsFromReconvictions
// uses, so StallEvents (and therefore exitScore's stall penalty) is a
// controllable, monotonic, real signal rather than a fabricated one.
func demotionTestReconvict(client *multiClientChannel, n int) {
	for range n {
		client.setQuarantined(blackholeNoReceiveAck)
		client.clearQuarantine()
	}
}

// --- demotionObserve: the ownership wrapper around demotionState ---

// TestDemotionObserveKeyedByClientIdAndClass proves demotionObserve owns a
// genuinely per-(clientId, class) streak, not a single shared counter: two
// different exits, and two different classes for the same exit, must not
// bleed into each other's bad-interval count.
func TestDemotionObserveKeyedByClientIdAndClass(t *testing.T) {
	parent := &RemoteUserNatMultiClient{}
	id1, id2 := NewId(), NewId()

	if parent.demotionObserve(id1, ClassBulk, false, 3) {
		t.Fatal("must not demote on the 1st bad observation")
	}
	if parent.demotionObserve(id1, ClassBulk, false, 3) {
		t.Fatal("must not demote on the 2nd bad observation")
	}
	// a different exit must not have inherited id1's streak
	if parent.demotionObserve(id2, ClassBulk, false, 3) {
		t.Fatal("a different exit's (clientId, class) key must start its own fresh streak")
	}
	// a different class for the SAME exit must also be independent
	if parent.demotionObserve(id1, ClassStreaming, false, 3) {
		t.Fatal("a different class for the same exit must not inherit the other class's streak")
	}
	// the 3rd consecutive bad observation for (id1, ClassBulk) demotes
	if !parent.demotionObserve(id1, ClassBulk, false, 3) {
		t.Fatal("should demote on the 3rd consecutive bad observation for (id1, ClassBulk)")
	}
}

// TestDemotionObserveGoodResetsStreak: a good observation between bad ones
// resets the streak, at the ownership-wrapper level (demotionState's own
// TestDemotionNeedsNofM already pins this for the bare struct; this pins that
// demotionObserve does not lose or reorder that behavior across calls that
// look up the same stored *demotionState by key).
func TestDemotionObserveGoodResetsStreak(t *testing.T) {
	parent := &RemoteUserNatMultiClient{}
	id := NewId()

	parent.demotionObserve(id, ClassBulk, false, 3)
	parent.demotionObserve(id, ClassBulk, false, 3)
	parent.demotionObserve(id, ClassBulk, true, 3) // recovery resets
	if parent.demotionObserve(id, ClassBulk, false, 3) {
		t.Fatal("a good observation must reset the streak; this is only the 1st bad one since")
	}
}

// --- scoredPlacementReorder integration ---

// TestScoredPlacementReorderDemotesAfterConsecutiveBadIntervals is the
// brief's central scenario, driven through the real placement path:
//
//   - clients[0] (the incumbent) carries 1 reconviction, clients[1] (the
//     challenger) carries 0 -- a real, mild, ClassBulk-scored edge for the
//     challenger. PlacementHysteresisPct=150 is set wide enough that this
//     mild edge alone can NEVER flip bestIndex through the ordinary
//     hysteresis+load-tiebreak loop (Tasks 1-3's unchanged behavior) --
//     which is what isolates demotion's own N-of-M override as the only
//     thing that can move the incumbent in this test.
//   - Two calls with that pairing must leave the incumbent in front
//     ("survives 2 bad intervals"); the 3rd must demote it -- reorder it
//     out of front, while it stays PRESENT in the returned slice (demotion
//     re-ranks, it never removes).
//   - In between, one call against clients[2] (a much worse exit: 5
//     reconvictions) is a genuinely GOOD interval for clients[0] and must
//     reset its streak -- proven by then needing 2 more (not 1 more) bad
//     clients[0]-vs-clients[1] calls before the next demotion.
func TestScoredPlacementReorderDemotesAfterConsecutiveBadIntervals(t *testing.T) {
	parent, clients := flowCapTestParent(t, 0, 0, 0, 0)
	incumbent, challenger, muchWorse := clients[0], clients[1], clients[2]
	setClientId(incumbent, NewId())
	setClientId(challenger, NewId())
	setClientId(muchWorse, NewId())

	demotionTestReconvict(incumbent, 1) // StallEvents=1: a mild, real deficit
	demotionTestReconvict(muchWorse, 5) // StallEvents=5: clearly worse than incumbent

	parent.settings.PlacementDemoteConsecutive = 3
	parent.settings.PlacementHysteresisPct = 150
	parent.SetFlowClassifier(fixedClassifier{class: ClassBulk})
	ipPath := &IpPath{Version: 4, Protocol: IpProtocolTcp}

	assertFront := func(step string, got []*multiClientChannel, want *multiClientChannel) {
		t.Helper()
		if len(got) != 2 || got[0] != want {
			t.Fatalf("%s: front = %p, want %p (got len=%d)", step, front(got), want, len(got))
		}
	}

	// round 1: bad (1/3) -- incumbent survives
	got := parent.scoredPlacementReorder([]*multiClientChannel{incumbent, challenger}, ipPath, "")
	assertFront("round 1", got, incumbent)

	// round 2: bad (2/3) -- incumbent still survives
	got = parent.scoredPlacementReorder([]*multiClientChannel{incumbent, challenger}, ipPath, "")
	assertFront("round 2", got, incumbent)

	// round 3 (good, against muchWorse): incumbent wins outright and its
	// streak resets
	got = parent.scoredPlacementReorder([]*multiClientChannel{incumbent, muchWorse}, ipPath, "")
	assertFront("good round", got, incumbent)

	// round 4: bad again (1/3 since the reset) -- must still survive; if the
	// reset above had not happened, this would already be the 3rd
	// consecutive bad round and would demote here instead
	got = parent.scoredPlacementReorder([]*multiClientChannel{incumbent, challenger}, ipPath, "")
	assertFront("round 4 (post-reset 1/3)", got, incumbent)

	// round 5: bad (2/3)
	got = parent.scoredPlacementReorder([]*multiClientChannel{incumbent, challenger}, ipPath, "")
	assertFront("round 5 (post-reset 2/3)", got, incumbent)

	// round 6: bad (3/3) -- now demoted: reordered to the back, but still
	// present. This is the hard safety assertion: demotion re-ranks, it
	// never removes.
	got = parent.scoredPlacementReorder([]*multiClientChannel{incumbent, challenger}, ipPath, "")
	if len(got) != 2 {
		t.Fatalf("demotion must never change membership: got %d candidates, want 2", len(got))
	}
	if got[0] != challenger || got[1] != incumbent {
		t.Fatalf("round 6: want the challenger promoted and the incumbent demoted-but-present, got front=%p back=%p", got[0], got[1])
	}
	seen := map[*multiClientChannel]bool{got[0]: true, got[1]: true}
	if !seen[incumbent] {
		t.Fatal("the demoted incumbent must still be present in the candidate list")
	}
}

// front is a tiny helper so assertFront's Fatalf above can print a pointer
// even when got is empty or the wrong length.
func front(candidates []*multiClientChannel) *multiClientChannel {
	if len(candidates) == 0 {
		return nil
	}
	return candidates[0]
}

// TestScoredPlacementReorderZeroPlacementDemoteConsecutiveIsInert proves the
// zero-value-off contract for PlacementDemoteConsecutive specifically: with
// it AT 0, demotionState.observe would treat a bare 0 as needBad<=1 -- act on
// EVERY sample -- if scoredPlacementReorder ever passed it through
// uninspected. It must not: the exact same hysteresis-protected mild deficit
// that demotes within 3 calls in the test above must never move the
// incumbent at all here, across many more calls than that.
//
// PlacementDemoteConsecutive is set to 0 EXPLICITLY below rather than left
// unset: Task 8 (feat(routing): enable class-aware scored placement by
// default) made DefaultMultiClientSettings' own value 3, not 0, so relying
// on flowCapTestParent's default here would silently test needBad=3 instead
// of needBad=0 -- the same demotion-within-3-calls behavior the sibling test
// above already covers, just for the wrong stated reason.
func TestScoredPlacementReorderZeroPlacementDemoteConsecutiveIsInert(t *testing.T) {
	parent, clients := flowCapTestParent(t, 0, 0, 0)
	incumbent, challenger := clients[0], clients[1]
	setClientId(incumbent, NewId())
	setClientId(challenger, NewId())
	demotionTestReconvict(incumbent, 1)

	parent.settings.PlacementDemoteConsecutive = 0 // <-- explicit: no longer the default (Task 8 set it to 3)
	parent.settings.PlacementHysteresisPct = 150
	parent.SetFlowClassifier(fixedClassifier{class: ClassBulk})
	ipPath := &IpPath{Version: 4, Protocol: IpProtocolTcp}

	for i := 0; i < 6; i++ {
		got := parent.scoredPlacementReorder([]*multiClientChannel{incumbent, challenger}, ipPath, "")
		if len(got) != 2 || got[0] != incumbent {
			t.Fatalf("round %d: PlacementDemoteConsecutive=0 must be inert (today's behavior: no demotion at all), got front=%p", i+1, got[0])
		}
	}
}

// --- eviction ---

// TestDemotionEvictedOnRemoveClient proves the leak/inheritance guard:
// removeClient -- the multi-client's one real per-channel teardown hook,
// already reused by TestRebindEstablishedQuicFlowMovesToCandidate and
// friends via rebindTestParent -- must evict every demotionStates entry for
// the departing channel's ClientId. Without this, an unbounded map keyed by
// exit identity leaks across a long session with churn, and (per the keying
// discussion in demotionKey's doc) a missed eviction is the one way a
// ClientId-keyed entry could ever be handed to a later, unrelated call.
func TestDemotionEvictedOnRemoveClient(t *testing.T) {
	parent, dying, _, _ := rebindTestParent(t, false, nil)
	id := NewId()
	setClientId(dying, id)
	// removeClient's locked section returns immediately if the client has no
	// entry in clientUpdates at all -- give it an (empty) one so the real
	// teardown body, including the eviction this test targets, actually runs.
	parent.clientUpdates[dying] = map[*multiClientChannelUpdate]bool{}

	// build a 2-of-3 streak pre-removal
	if parent.demotionObserve(id, ClassBulk, false, 3) {
		t.Fatal("must not demote on the 1st bad observation")
	}
	if parent.demotionObserve(id, ClassBulk, false, 3) {
		t.Fatal("must not demote on the 2nd bad observation")
	}

	parent.removeClient(dying)

	// a later observation for the SAME ClientId (a reconnect to the same
	// exit, or a coincidentally-reused pointer address for an unrelated one)
	// must start a fresh streak, not the pre-removal 2/3 -- if eviction did
	// not run, this 1 bad observation would already read as demoted.
	if parent.demotionObserve(id, ClassBulk, false, 3) {
		t.Fatal("post-eviction observation must not inherit the pre-removal streak")
	}
}

// TestDemotionEvictedOnRemoveClientThatNeverCarriedAFlow is the case the
// sibling test above deliberately steps around. That one seeds an empty
// clientUpdates entry so removeClient's locked body runs at all -- which
// quietly accepts that an exit with NO clientUpdates entry returns early and
// evicts nothing.
//
// That exit is exactly the one that leaks. demotionObserve creates an entry
// for whichever exit is at the HEAD of the candidate list, win or lose, and
// with MultiRaceClientCount at 0 the head of the list is usually not the exit
// that ends up carrying the flow. So "ranked first, never chosen, then gone"
// is the common case, not a corner: no clientUpdates entry ever existed, and
// before the eviction was hoisted above that early return its demotionStates
// entries survived for the life of the session.
//
// No clientUpdates entry is seeded here on purpose. Do not add one.
func TestDemotionEvictedOnRemoveClientThatNeverCarriedAFlow(t *testing.T) {
	parent, dying, _, _ := rebindTestParent(t, false, nil)
	id := NewId()
	setClientId(dying, id)

	if _, seeded := parent.clientUpdates[dying]; seeded {
		t.Fatal("fixture must start with no clientUpdates entry for this test to mean anything")
	}

	if parent.demotionObserve(id, ClassBulk, false, 3) {
		t.Fatal("must not demote on the 1st bad observation")
	}
	if parent.demotionObserve(id, ClassBulk, false, 3) {
		t.Fatal("must not demote on the 2nd bad observation")
	}
	if len(parent.demotionStates) == 0 {
		t.Fatal("precondition: the observations must have created state to evict")
	}

	parent.removeClient(dying)

	if n := len(parent.demotionStates); n != 0 {
		t.Fatalf("demotionStates = %d entries after removeClient, want 0 -- an exit that never carried a flow still leaks", n)
	}
	if parent.demotionObserve(id, ClassBulk, false, 3) {
		t.Fatal("post-eviction observation must not inherit the pre-removal streak")
	}
}
