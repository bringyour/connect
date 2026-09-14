package connect

import "testing"

func TestProviderPriorsEwmaAndBias(t *testing.T) {
	p := NewProviderPriors()
	if p.Bias("unknown") != 0.5 {
		t.Fatal("unknown provider must be neutral 0.5")
	}
	for i := 0; i < 20; i++ {
		p.Observe("good", 0.9, 1000)
	}
	for i := 0; i < 20; i++ {
		p.Observe("bad", 0.1, 1000)
	}
	if !(p.Bias("good") > p.Bias("bad")) {
		t.Fatalf("good provider must bias higher: good=%f bad=%f", p.Bias("good"), p.Bias("bad"))
	}
}

func TestProviderPriorsConvictionLowersBias(t *testing.T) {
	p := NewProviderPriors()
	for i := 0; i < 20; i++ {
		p.Observe("x", 0.9, 1000)
	}
	before := p.Bias("x")
	p.Convict("x", 1000)
	p.Convict("x", 1000)
	if !(p.Bias("x") < before) {
		t.Fatal("convictions must lower recruitment bias")
	}
}

func TestProviderPriorsRoundTrip(t *testing.T) {
	p := NewProviderPriors()
	p.Observe("a", 0.7, 1234)
	snap := p.Snapshot()
	q := NewProviderPriors()
	q.Load(snap)
	if q.Snapshot()["a"].ScoreEwma != snap["a"].ScoreEwma {
		t.Fatal("round-trip must preserve EWMA")
	}
}

func TestProviderPriorsSnapshotIsCopy(t *testing.T) {
	p := NewProviderPriors()
	p.Observe("a", 0.7, 1234)
	snap := p.Snapshot()
	// Mutate the returned snapshot
	snap["a"] = ProviderPrior{ScoreEwma: 0.1, Convictions: 99, LastSeenUnix: 9999}
	snap["b"] = ProviderPrior{ScoreEwma: 0.5, Convictions: 1, LastSeenUnix: 1000}
	// Internal state should be unchanged
	if p.Bias("a") != 0.7 {
		t.Fatalf("Snapshot() did not return a copy; internal state was mutated")
	}
	if p.Bias("b") != 0.5 {
		t.Fatalf("Snapshot() did not return a copy; unrelated key was added to internal state")
	}
}

func TestProviderPriorsLoadCopiesIn(t *testing.T) {
	data := map[string]ProviderPrior{
		"x": {ScoreEwma: 0.8, Convictions: 1, LastSeenUnix: 5000},
	}
	p := NewProviderPriors()
	p.Load(data)
	// Mutate the original map
	data["x"] = ProviderPrior{ScoreEwma: 0.2, Convictions: 99, LastSeenUnix: 9999}
	data["y"] = ProviderPrior{ScoreEwma: 0.5, Convictions: 0, LastSeenUnix: 1000}
	// Check the snapshot to verify internal state is unchanged
	snap := p.Snapshot()
	if snap["x"].ScoreEwma != 0.8 || snap["x"].Convictions != 1 {
		t.Fatalf("Load() did not copy in; internal state was changed by mutating caller's map")
	}
	if _, ok := snap["y"]; ok {
		t.Fatalf("Load() did not copy in; unrelated key from mutated map appeared in internal state")
	}
}

func TestProviderPriorsConvictThenObserveBlends(t *testing.T) {
	p := NewProviderPriors()
	// Convict before first Observe: creates entry with ScoreEwma=0.5, Convictions=1
	p.Convict("x", 1000)
	// Verify the conviction is recorded and Bias is penalized < 0.5
	biasAfterConvict := p.Bias("x")
	if biasAfterConvict != 0.35 {
		t.Fatalf("Bias after single convict on never-observed provider must be 0.35, got %f", biasAfterConvict)
	}
	// First Observe after Convict should BLEND (entry exists), not reseed
	// 0.2*0.9 + 0.8*0.5 = 0.18 + 0.4 = 0.58
	p.Observe("x", 0.9, 1001)
	snap := p.Snapshot()
	want := 0.58
	if snap["x"].ScoreEwma < want-0.0001 || snap["x"].ScoreEwma > want+0.0001 {
		t.Fatalf("Convict-then-Observe should blend 0.58, not reseed; got %f", snap["x"].ScoreEwma)
	}
	if snap["x"].Convictions != 1 {
		t.Fatalf("Convictions must remain 1 after Observe; got %d", snap["x"].Convictions)
	}
}

func TestProviderPriorsBiasClampedToZero(t *testing.T) {
	p := NewProviderPriors()
	// Seed a provider with high score, then add enough convictions to go negative
	p.Observe("x", 0.9, 1000)
	// 7 convictions: 0.9 - 0.15*7 = 0.9 - 1.05 = -0.15 (negative)
	for i := 0; i < 7; i++ {
		p.Convict("x", 1000)
	}
	bias := p.Bias("x")
	if bias != 0.0 {
		t.Fatalf("Bias with negative raw value must clamp to 0.0, got %f", bias)
	}
}

func TestProviderPriorsPresenceKeyedSeeding(t *testing.T) {
	p := NewProviderPriors()
	// Observe multiple times with nowUnix=0 (fixture time, or "time unknown").
	// With presence-keyed seeding, all observations after the first should blend.
	// With timestamp-based seeding (pr.LastSeenUnix == 0), every observation would re-seed,
	// defeating EWMA smoothing and allowing a single malicious sample to persist.
	p.Observe("x", 0.9, 0) // Seeds to 0.9, LastSeenUnix=0
	p.Observe("x", 0.1, 0) // Should blend to 0.2*0.1 + 0.8*0.9 = 0.74
	// If using timestamp seeding, this would have re-seeded to 0.1 instead.
	snap := p.Snapshot()
	want := 0.74
	if snap["x"].ScoreEwma < want-0.0001 || snap["x"].ScoreEwma > want+0.0001 {
		t.Fatalf("Observe with nowUnix=0 must blend, not re-seed: got %f, want %f", snap["x"].ScoreEwma, want)
	}
}
