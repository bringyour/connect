package connect

import (
	"context"
	"testing"
	"time"
)

// the serialized path: every caller advances one shared timestamp, so a burst
// of N cold connects staircases at least (N-1) x MinNextConnectDelay deep.
// This is the shape the reconnect fast path exists to bypass -- pin it so the
// contrast below stays meaningful.
func TestNextConnectTimeSerializesCallers(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientStrategySettings()
	settings.MinNextConnectDelay = 100 * time.Millisecond
	settings.MaxNextConnectDelay = 200 * time.Millisecond
	strategy := NewClientStrategy(ctx, settings)

	start := time.Now()
	var last time.Time
	for i := range 8 {
		next, _ := strategy.NextConnectTime()
		if next.Before(last) {
			t.Fatalf("call %d went backwards: %s < %s", i, next, last)
		}
		last = next
	}
	// 8 callers: the first clamps to now, each later one advances >= Min
	if staircase := last.Sub(start); staircase < 7*settings.MinNextConnectDelay {
		t.Errorf("serialized staircase too shallow: %s < %s", staircase, 7*settings.MinNextConnectDelay)
	}
}

// the reconnect fast path: independent small jitter, capped concurrency, no
// effect on the shared serialized timestamp, and slots that recycle on
// release. All arithmetic on returned times -- no sleeps -- so the timing
// shape assertions hold on a loaded runner.
func TestNextConnectTimeReconnectFastPath(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientStrategySettings()
	// the serialized step is made larger than the fast-path jitter so the two
	// shapes are provably disjoint
	settings.MinNextConnectDelay = 300 * time.Millisecond
	settings.MaxNextConnectDelay = 400 * time.Millisecond
	strategy := NewClientStrategy(ctx, settings)

	// prime the shared staircase well past the fast-path jitter range. start
	// is captured BEFORE priming so serialLast >= start + 2 x Min holds by
	// construction (the first call clamps to its own now >= start).
	start := time.Now()
	var serialLast time.Time
	for range 3 {
		serialLast, _ = strategy.NextConnectTime()
	}
	if serialLast.Before(start.Add(2 * settings.MinNextConnectDelay)) {
		t.Fatalf("staircase priming failed: %s", serialLast.Sub(start))
	}

	// up to the cap, reconnects are fast and independent of the staircase
	releases := []func(){}
	for i := range reconnectFastPathLimit {
		connectTime, release := strategy.NextReconnectTime()
		releases = append(releases, release)
		if !connectTime.Before(serialLast) {
			t.Errorf("fast path call %d landed on the staircase: %s >= %s", i, connectTime, serialLast)
		}
		if jitter := connectTime.Sub(start); reconnectFastPathMaxDelay+100*time.Millisecond <= jitter {
			t.Errorf("fast path call %d jitter too large: %s", i, jitter)
		}
	}

	// over the cap: the caller falls back to the serialized staircase, which
	// the fast-path callers must NOT have advanced -- one serialized step from
	// serialLast, not five
	overCapTime, overCapRelease := strategy.NextReconnectTime()
	if overCapTime.Before(serialLast) {
		t.Errorf("over-cap call bypassed the staircase: %s < %s", overCapTime, serialLast)
	}
	if step := overCapTime.Sub(serialLast); settings.MaxNextConnectDelay+100*time.Millisecond <= step {
		t.Errorf("fast-path callers advanced the shared timestamp: step %s", step)
	}
	overCapRelease()

	// releasing a slot restores the fast path
	releases[0]()
	againTime, againRelease := strategy.NextReconnectTime()
	if !againTime.Before(overCapTime) {
		t.Errorf("released slot did not restore the fast path: %s >= %s", againTime, overCapTime)
	}
	againRelease()

	// release is idempotent: double-releasing must not free a slot twice.
	// after this loop exactly reconnectFastPathLimit slots are free again.
	releases[0]()
	for _, release := range releases[1:] {
		release()
	}
	for range reconnectFastPathLimit {
		_, release := strategy.NextReconnectTime()
		defer release()
	}
}

// a cancelled pacing wait gives its staircase reservation back. Before the
// release existed, a dialer torn down while waiting for its slot left its
// 100ms-1s step consumed forever, so rapid connect/disconnect cycles pushed
// the one shared timestamp arbitrarily far ahead of wall clock (2026-08-09
// field capture: 60s+ lead, cohorts of ~10 transport-downs expiring every 15s,
// 0 proven connections). All arithmetic on returned times -- no sleeps.
// Min == Max pins the serialized step to exactly 100ms so the assertions are
// deterministic.
func TestNextConnectTimeCancelReleasesReservation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientStrategySettings()
	settings.MinNextConnectDelay = 100 * time.Millisecond
	settings.MaxNextConnectDelay = 100 * time.Millisecond
	strategy := NewClientStrategy(ctx, settings)

	// 50 dialers reserve a step each and are cancelled before dialing -- the
	// shape of a multi-client window torn down mid-connect
	releases := []func(){}
	var deepest time.Time
	for range 50 {
		var release func()
		deepest, release = strategy.NextConnectTime()
		releases = append(releases, release)
	}
	// the staircase is genuinely deep before the releases...
	if lead := time.Until(deepest); lead < 4*time.Second {
		t.Fatalf("staircase priming failed: lead %s", lead)
	}
	for _, release := range releases {
		release()
	}
	// ...and every cancelled wait gave its step back: a fresh caller is paced
	// one step from now, not queued behind 50 dials that will never happen
	next, release := strategy.NextConnectTime()
	release()
	if lead := time.Until(next); 2*settings.MaxNextConnectDelay < lead {
		t.Errorf("cancelled waits leaked their reservations: lead %s", lead)
	}

	// release is idempotent: a double release must not free a second step.
	// reserve three steps, double-release the first, and the next reservation
	// must land one step past the third (back where the third was), not two
	// steps down.
	_, releaseA := strategy.NextConnectTime()
	strategy.NextConnectTime()
	last, _ := strategy.NextConnectTime()
	releaseA()
	releaseA()
	final, _ := strategy.NextConnectTime()
	if final.Before(last) {
		t.Errorf("double release freed a second step: %s < %s", final, last)
	}
}

// the safety clamp behind the release: even when reservations leak (a release
// never runs -- e.g. the over-cap reconnect fallback), the shared timestamp
// never runs more than nextConnectMaxLead ahead of wall clock, so no dialer is
// ever born more than that from its first dial.
func TestNextConnectTimeLeadClamp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultClientStrategySettings()
	settings.MinNextConnectDelay = 100 * time.Millisecond
	settings.MaxNextConnectDelay = 200 * time.Millisecond
	strategy := NewClientStrategy(ctx, settings)

	// 200 leaked reservations would stack >= 20s of staircase unclamped
	var last time.Time
	for range 200 {
		last, _ = strategy.NextConnectTime()
	}
	if lead := time.Until(last); nextConnectMaxLead < lead {
		t.Errorf("staircase lead %s exceeds the clamp %s", lead, nextConnectMaxLead)
	}
	// the clamp bounds the lead but does not erase pacing: with >= 20s of
	// unclamped demand the lead must be sitting AT the bound, not below it
	if lead := time.Until(last); lead < nextConnectMaxLead-time.Second {
		t.Errorf("lead %s is far below the clamp %s: the bound is not what limited it", lead, nextConnectMaxLead)
	}
}

// the TLS side of the reconnect fast path: without a session cache every
// re-dial pays a full handshake. The cache must exist on the default config
// and be SHARED by clones, because both the resilient dialers and the h3
// transport clone their config per dial -- a cache that cloned with the
// config would never see a second use.
func TestClientStrategyTlsSessionResumptionCache(t *testing.T) {
	tlsConfig, err := DefaultTlsConfig()
	AssertEqual(t, err, nil)
	if tlsConfig.ClientSessionCache == nil {
		t.Fatal("DefaultTlsConfig has no ClientSessionCache: every reconnect pays a full handshake")
	}
	if cloned := tlsConfig.Clone(); cloned.ClientSessionCache != tlsConfig.ClientSessionCache {
		t.Error("Clone does not share the session cache: per-dial clones could never resume")
	}

	// the two production consumers both flow from DefaultTlsConfig
	connectSettings := DefaultConnectSettings()
	if connectSettings.TlsConfig.ClientSessionCache == nil {
		t.Error("ConnectSettings.TlsConfig has no session cache")
	}
	transportSettings := DefaultPlatformTransportSettings()
	if transportSettings.QuicTlsConfig.ClientSessionCache == nil {
		t.Error("QuicTlsConfig has no session cache: h3 re-dials cannot resume (or 0-RTT)")
	}
}
