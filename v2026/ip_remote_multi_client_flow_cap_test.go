package connect

import (
	"strings"
	"testing"
)

// flowCapTestClient builds a channel with n flows pinned to it in the parent's
// bookkeeping, which is what the cap reads.
func flowCapTestParent(t *testing.T, maxFlowsPerExit int, flowCounts ...int) (*RemoteUserNatMultiClient, []*multiClientChannel) {
	t.Helper()

	settings := DefaultMultiClientSettings()
	settings.MaxFlowsPerExit = maxFlowsPerExit

	parent := &RemoteUserNatMultiClient{
		settings:      settings,
		clientUpdates: map[*multiClientChannel]map[*multiClientChannelUpdate]bool{},
	}

	clients := []*multiClientChannel{}
	for _, count := range flowCounts {
		// packetStats is never nil on a real channel (newMultiClientChannel
		// always sets it); the routing-scorer tests reuse this fixture and
		// now reach it through exitMetricsSnapshot's windowStatsWithCoalesce
		// read, so this fixture must carry a real (if empty) one too.
		client := &multiClientChannel{settings: settings, packetStats: &clientWindowStats{}}
		updates := map[*multiClientChannelUpdate]bool{}
		for range count {
			updates[&multiClientChannelUpdate{}] = true
		}
		parent.clientUpdates[client] = updates
		clients = append(clients, client)
	}
	return parent, clients
}

// 0 is the shipped default and must leave selection exactly as it was.
func TestFlowCapZeroIsUnbounded(t *testing.T) {
	parent, clients := flowCapTestParent(t, 0, 500, 1, 0)

	under := parent.underFlowCap(clients)
	if len(under) != len(clients) {
		t.Errorf("cap 0 filtered %d of %d clients; it must be unbounded", len(clients)-len(under), len(clients))
	}
}

// The point of the cap: an exit already carrying its share stops attracting
// new flows, so one removal cannot take out everything at once.
func TestFlowCapExcludesFullExits(t *testing.T) {
	// cap 32: first is over, second is exactly at, third and fourth are under
	parent, clients := flowCapTestParent(t, 32, 157, 32, 31, 0)

	under := parent.underFlowCap(clients)
	if len(under) != 2 {
		t.Fatalf("got %d candidates, want 2 (only the two under the cap)", len(under))
	}
	if under[0] != clients[2] || under[1] != clients[3] {
		t.Error("wrong clients kept, or the caller's ordering was not preserved")
	}
}

// When every candidate is full underFlowCap now reports that honestly with an
// empty result -- the fallback decision belongs to raceCandidates, which knows
// whether a wider field exists to try first.
func TestFlowCapAllFullIsEmpty(t *testing.T) {
	parent, clients := flowCapTestParent(t, 8, 100, 50, 9)

	under := parent.underFlowCap(clients)
	if len(under) != 0 {
		t.Fatalf("got %d candidates, want 0: every exit is at the cap, and the fallback is the caller's decision", len(under))
	}
}

func TestFlowCapEmptyCandidates(t *testing.T) {
	parent, _ := flowCapTestParent(t, 32)

	if under := parent.underFlowCap([]*multiClientChannel{}); len(under) != 0 {
		t.Errorf("got %d candidates from an empty list", len(under))
	}
}

// The runtime override has to reach the cap, or the developer control silently
// does nothing -- a failure mode this code has already shipped more than once.
func TestFlowCapReadsTheRuntimeOverride(t *testing.T) {
	// four exits, two of them full: filtering leaves two, which is the
	// minimum the race needs, so the override's effect is observable
	parent, clients := flowCapTestParent(t, 0, 100, 100, 1, 2)

	// unbounded by settings
	if under := parent.underFlowCap(clients); len(under) != 4 {
		t.Fatalf("baseline: got %d, want 4", len(under))
	}

	// override installs a cap
	parent.SetReliabilitySettings(&ReliabilitySettings{MaxFlowsPerExit: 8})
	under := parent.underFlowCap(clients)
	if len(under) != 2 || under[0] != clients[2] || under[1] != clients[3] {
		t.Errorf("override ignored: got %d candidates, want the two under the cap", len(under))
	}

	// clearing it restores unbounded
	parent.SetReliabilitySettings(nil)
	if under := parent.underFlowCap(clients); len(under) != 4 {
		t.Errorf("clearing the override did not restore unbounded selection: got %d, want 4", len(under))
	}
}

// --- raceCandidates: the fallback order over the window's rank gate ---

// clientList adapts a fixed slice to the lazy source raceCandidatesFrom takes.
func clientList(clients ...*multiClientChannel) func() []*multiClientChannel {
	return func() []*multiClientChannel { return clients }
}

// mustNotBeConsulted fails the test if the lazy source is ever pulled.
func mustNotBeConsulted(t *testing.T, name string) func() []*multiClientChannel {
	return func() []*multiClientChannel {
		t.Fatalf("%s was consulted, and must not be on this path", name)
		return nil
	}
}

// While the min tier has capacity, rank is respected: the under-cap subset of
// the min tier is the whole field, and the cross-tier list is never built.
func TestRaceCandidatesStayOnMinTierWhileItHasCapacity(t *testing.T) {
	parent, clients := flowCapTestParent(t, 8, 100, 1, 2)

	candidates := parent.raceCandidatesFrom(
		clientList(clients...),
		mustNotBeConsulted(t, "the cross-tier list"),
		mustNotBeConsulted(t, "the last-resort list"),
	)
	if len(candidates) != 2 || candidates[0] != clients[1] || candidates[1] != clients[2] {
		t.Errorf("got %d candidates, want the two min-tier exits under the cap", len(candidates))
	}
}

// A single under-cap candidate is returned alone rather than widening the
// field: crossing rank while the top rank still has capacity would let a
// nearby lower-rank exit win on rtt and pull traffic off the rank the
// platform chose. The no-race placement recovers via the re-race paths.
func TestRaceCandidatesSingleUnderCapCandidateStandsAlone(t *testing.T) {
	parent, clients := flowCapTestParent(t, 8, 100, 100, 1)

	candidates := parent.raceCandidatesFrom(
		clientList(clients...),
		mustNotBeConsulted(t, "the cross-tier list"),
		mustNotBeConsulted(t, "the last-resort list"),
	)
	if len(candidates) != 1 || candidates[0] != clients[2] {
		t.Errorf("got %d candidates, want exactly the one min-tier exit under the cap", len(candidates))
	}
}

// The window offers only its best rank, so with one exit in the top rank the
// cap used to be structurally defeated: the under-cap filter could never keep
// a candidate, fell back to the full list, and the same saturated exit won
// every race. On device: 86 flows on one exit, twelve idle spares. Saturation
// of the min tier is when crossing rank becomes necessary.
func TestRaceCandidatesCrossTierWhenMinTierSaturated(t *testing.T) {
	// index 0 is the min-tier exit at the cap; 1 and 2 are spares on higher
	// tiers with capacity
	parent, clients := flowCapTestParent(t, 8, 100, 0, 3)

	candidates := parent.raceCandidatesFrom(
		clientList(clients[0]),
		clientList(clients...),
		mustNotBeConsulted(t, "the last-resort list"),
	)
	if len(candidates) != 2 || candidates[0] != clients[1] || candidates[1] != clients[2] {
		t.Errorf("got %d candidates, want the two under-cap spares from the crossed field", len(candidates))
	}
}

// A cap bounds blast radius; it is not admission control. When every exit of
// every rank is full the flow is still placed -- on the least-loaded exit,
// because placing overflow by rank re-created the single-exit pileup: on
// device, five exits pinned at 16 while the lone tier-1 exit absorbed 267.
func TestRaceCandidatesOverflowGoesLeastLoaded(t *testing.T) {
	parent, clients := flowCapTestParent(t, 8, 100, 50, 9)

	candidates := parent.raceCandidatesFrom(
		clientList(clients[0]),
		clientList(clients...),
		mustNotBeConsulted(t, "the last-resort list"),
	)
	if len(candidates) != 1 || candidates[0] != clients[2] {
		t.Errorf("got %d candidates, want exactly the least-loaded exit: everything is full and overflow must spread toward an even share", len(candidates))
	}
}

// Ties at the minimum all stay in, preserving the race at the even-share
// equilibrium where whole groups sit at the same count.
func TestRaceCandidatesOverflowKeepsTiesRacing(t *testing.T) {
	parent, clients := flowCapTestParent(t, 8, 30, 16, 16)

	candidates := parent.raceCandidatesFrom(
		clientList(clients[0]),
		clientList(clients...),
		mustNotBeConsulted(t, "the last-resort list"),
	)
	if len(candidates) != 2 || candidates[0] != clients[1] || candidates[1] != clients[2] {
		t.Errorf("got %d candidates, want the two tied least-loaded exits", len(candidates))
	}
}

// The degenerate fallback: a cross-tier source with nothing in it must not
// strand the flow -- the min tier as offered is still returned.
func TestRaceCandidatesEmptyCrossTierFallsBackToMinTier(t *testing.T) {
	parent, clients := flowCapTestParent(t, 8, 100)

	candidates := parent.raceCandidatesFrom(
		clientList(clients[0]),
		clientList(),
		mustNotBeConsulted(t, "the last-resort list"),
	)
	if len(candidates) != 1 || candidates[0] != clients[0] {
		t.Errorf("got %d candidates, want the min tier as offered when the crossed field is empty", len(candidates))
	}
}

// With the cap off the rank gate is left exactly as it was: min tier only,
// cross-tier never consulted. 0 is the shipped default for upstream parity.
func TestRaceCandidatesCapOffLeavesTheRankGateAlone(t *testing.T) {
	parent, clients := flowCapTestParent(t, 0, 500, 1)

	candidates := parent.raceCandidatesFrom(
		clientList(clients[0]),
		mustNotBeConsulted(t, "the cross-tier list"),
		mustNotBeConsulted(t, "the last-resort list"),
	)
	if len(candidates) != 1 || candidates[0] != clients[0] {
		t.Errorf("got %d candidates, want the min tier untouched with the cap off", len(candidates))
	}
}

// An empty window yields an empty field without consulting anything else.
func TestRaceCandidatesEmptyWindow(t *testing.T) {
	parent, _ := flowCapTestParent(t, 8)

	candidates := parent.raceCandidatesFrom(
		clientList(),
		mustNotBeConsulted(t, "the cross-tier list"),
		clientList(),
	)
	if len(candidates) != 0 {
		t.Errorf("got %d candidates from an empty window", len(candidates))
	}
}

// minTierClients is the rank gate itself: only the best (lowest) tier present
// survives, in order.
func TestMinTierClientsKeepsOnlyBestRank(t *testing.T) {
	tiered := func(tier int) *multiClientChannel {
		return &multiClientChannel{
			args: &multiClientChannelArgs{DestinationStats: DestinationStats{Tier: tier}},
		}
	}
	a, b, c, d := tiered(2), tiered(1), tiered(3), tiered(1)

	kept := minTierClients([]*multiClientChannel{a, b, c, d})
	if len(kept) != 2 || kept[0] != b || kept[1] != d {
		t.Errorf("got %d clients, want the two tier-1 clients in order", len(kept))
	}

	if kept := minTierClients([]*multiClientChannel{}); len(kept) != 0 {
		t.Errorf("got %d clients from an empty input", len(kept))
	}
}

// When the window's whole offer is empty -- every exit warned or quarantined
// at once, the steady state of a pool whose providers stall intermittently --
// the benched field is handed to the race rather than leaving the flow
// unroutable in the send retry loop. On device the missing fallback read as
// spinners on a short-video feed that lasted until some exit happened to
// unbench.
func TestRaceCandidatesEmptyOfferFallsBackToBenched(t *testing.T) {
	parent, clients := flowCapTestParent(t, 8, 3, 5)

	candidates := parent.raceCandidatesFrom(
		clientList(),
		mustNotBeConsulted(t, "the cross-tier list"),
		clientList(clients...),
	)
	if len(candidates) != 2 || candidates[0] != clients[0] || candidates[1] != clients[1] {
		t.Errorf("got %d candidates, want the whole benched field: an empty offer must fall back to warned exits", len(candidates))
	}
}

// The benched fallback applies with the cap off too -- starvation protection
// is not a cap feature.
func TestRaceCandidatesEmptyOfferFallsBackWithCapOff(t *testing.T) {
	parent, clients := flowCapTestParent(t, 0, 3)

	candidates := parent.raceCandidatesFrom(
		clientList(),
		mustNotBeConsulted(t, "the cross-tier list"),
		clientList(clients...),
	)
	if len(candidates) != 1 || candidates[0] != clients[0] {
		t.Errorf("got %d candidates, want the benched field regardless of the cap setting", len(candidates))
	}
}

// The fallback order is only worth anything if the send path actually goes
// through it -- a helper that is correct but uncalled is the failure mode this
// codebase has shipped more than once. Read the call sites rather than
// trusting they exist.
func TestSendPathPlacesFlowsThroughRaceCandidates(t *testing.T) {
	source, err := readSource("ip_remote_multi_client.go")
	if err != nil {
		t.Fatal(err)
	}

	body, ok := functionBody(source, "func (self *RemoteUserNatMultiClient) sendParsedPacketGroup(")
	if !ok {
		t.Fatal("could not find sendParsedPacketGroup")
	}
	if !strings.Contains(body, "self.raceCandidates(window, ipPath.Version)") {
		t.Error("sendParsedPacketGroup does not assemble its field through raceCandidates: the cap and the rank gate are disconnected again")
	}

	body, ok = functionBody(source, "func (self *RemoteUserNatMultiClient) raceCandidates(")
	if !ok {
		t.Fatal("could not find raceCandidates")
	}
	if !strings.Contains(body, "orderedClientsCrossTier") {
		t.Error("raceCandidates does not offer the cross-tier list, so a saturated min tier still cannot spread")
	}
	if !strings.Contains(body, "lastResortClients") {
		t.Error("raceCandidates does not offer the last-resort list, so an all-benched window leaves flows unroutable")
	}

	// the rebind gather needs the same starvation fallback: candidates from
	// the ordinary offer only, and a dying exit's flows are torn down for
	// want of a clean candidate exactly when the pool benches at once
	body, ok = functionBody(source, "func (self *RemoteUserNatMultiClient) rebindCandidates(")
	if !ok {
		t.Fatal("could not find rebindCandidates")
	}
	if !strings.Contains(body, "unorderedClients()") {
		t.Error("rebindCandidates has no benched fallback: an all-warned pool tears down every rebindable flow")
	}
}
