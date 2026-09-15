package connect

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestDnsColdProbeDelayBacksOffAndCaps(t *testing.T) {
	cases := []struct {
		failureCount int
		expected     time.Duration
	}{
		{failureCount: 1, expected: 2 * time.Second},
		{failureCount: 2, expected: 4 * time.Second},
		{failureCount: 3, expected: 8 * time.Second},
		{failureCount: 8, expected: 256 * time.Second},
		{failureCount: 9, expected: 5 * time.Minute},
		{failureCount: 1000, expected: 5 * time.Minute},
	}
	for _, c := range cases {
		actual := dnsColdProbeDelay(
			c.failureCount,
			dnsColdProbeInitialInterval,
			dnsColdProbeMaxInterval,
		)
		if actual != c.expected {
			t.Errorf("failure count %d delay = %s, expected %s", c.failureCount, actual, c.expected)
		}
	}
}

func TestUpgradeMuxStaleDohSuccessCannotProveNewPath(t *testing.T) {
	mux := &UpgradeMux{}
	oldGeneration := mux.markTunnelDohUnproven()
	if !mux.markTunnelDohProvenForGeneration(oldGeneration) {
		t.Fatal("current generation success was rejected")
	}
	if mux.tunnelDohCold() {
		t.Fatal("current generation success did not prove the path")
	}

	currentGeneration := mux.markTunnelDohUnproven()
	if mux.markTunnelDohProvenForGeneration(oldGeneration) {
		t.Fatal("stale generation success was accepted")
	}
	if !mux.tunnelDohCold() {
		t.Fatal("stale generation success made the replacement path warm")
	}
	if !mux.markTunnelDohProvenForGeneration(currentGeneration) {
		t.Fatal("replacement generation success was rejected")
	}
	if mux.tunnelDohCold() {
		t.Fatal("replacement generation success did not prove the path")
	}
}

func TestUpgradeMuxStaleDohFailureCannotPoisonNewPath(t *testing.T) {
	mux := &UpgradeMux{}
	oldGeneration := mux.markTunnelDohUnproven()
	currentGeneration := mux.markTunnelDohUnproven()
	mux.markTunnelDohProvenForGeneration(currentGeneration)

	for range tunnelDohColdFailureCount {
		if count := mux.recordTunnelDohFailureForGeneration(oldGeneration); count != 0 {
			t.Fatalf("stale failure count = %d, expected 0", count)
		}
	}
	if mux.tunnelDohCold() {
		t.Fatal("stale failures made the proven replacement path cold")
	}

	for range tunnelDohColdFailureCount {
		mux.recordTunnelDohFailureForGeneration(currentGeneration)
	}
	if !mux.tunnelDohCold() {
		t.Fatal("current-generation failures did not make the path cold")
	}
}

func TestUpgradeMuxColdDohProberIsSingleAndNetworkWakeable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := &UpgradeMux{
		ctx:                  ctx,
		dnsProberWake:        make(chan struct{}, 1),
		dnsProbeInitialDelay: time.Hour,
		dnsProbeMaxDelay:     time.Hour,
	}
	mux.settings.Store(&UpgradeMuxSettings{Dns: &DnsUpgradeSettings{Resolver: &DnsResolverSettings{}}})
	mux.markTunnelDohUnproven()
	mux.fallbackDohCache.Store(&DohCache{})

	probeCalls := make(chan int, 8)
	var activeCount atomic.Int32
	var maxActiveCount atomic.Int32
	mux.tunnelDohWarmFunction = func(ctx context.Context, serverCount int) bool {
		active := activeCount.Add(1)
		for {
			maxActive := maxActiveCount.Load()
			if active <= maxActive || maxActiveCount.CompareAndSwap(maxActive, active) {
				break
			}
		}
		select {
		case probeCalls <- serverCount:
		case <-ctx.Done():
		}
		activeCount.Add(-1)
		return false
	}

	for range 64 {
		mux.ensureColdProber()
	}
	select {
	case serverCount := <-probeCalls:
		if serverCount != 2 {
			t.Fatalf("initial probe server count = %d, expected 2", serverCount)
		}
	case <-time.After(time.Second):
		t.Fatal("cold DoH probe did not start")
	}

	// Ordinary cold-query calls only ensure the worker exists; they must not
	// reset its backoff and recreate the fixed-cadence retry storm.
	for range 64 {
		mux.ensureColdProber()
	}
	select {
	case <-probeCalls:
		t.Fatal("ensureColdProber bypassed the active worker's backoff")
	case <-time.After(50 * time.Millisecond):
	}

	mux.wakeColdProber()
	select {
	case serverCount := <-probeCalls:
		if serverCount != 2 {
			t.Fatalf("network-woken probe server count = %d, expected 2", serverCount)
		}
	case <-time.After(time.Second):
		t.Fatal("network wake did not interrupt cold-probe backoff")
	}
	if maxActiveCount.Load() != 1 {
		t.Fatalf("maximum concurrent cold probes = %d, expected 1", maxActiveCount.Load())
	}

	cancel()
	if !waitForCondition(time.Second, func() bool {
		return !mux.dnsProberRunning.Load()
	}) {
		t.Fatal("cold DoH prober did not stop after cancellation")
	}
}

func TestUpgradeMuxColdDohProberStopsAfterSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := &UpgradeMux{
		ctx:                  ctx,
		dnsProberWake:        make(chan struct{}, 1),
		dnsProbeInitialDelay: time.Millisecond,
		dnsProbeMaxDelay:     time.Millisecond,
	}
	mux.settings.Store(&UpgradeMuxSettings{Dns: &DnsUpgradeSettings{Resolver: &DnsResolverSettings{}}})
	mux.markTunnelDohUnproven()
	mux.fallbackDohCache.Store(&DohCache{})
	var probeCount atomic.Int32
	mux.tunnelDohWarmFunction = func(ctx context.Context, serverCount int) bool {
		probeCount.Add(1)
		return true
	}

	mux.ensureColdProber()
	if !waitForCondition(time.Second, func() bool {
		return !mux.dnsProberRunning.Load()
	}) {
		t.Fatal("successful cold DoH prober did not retire")
	}
	if probeCount.Load() != 1 {
		t.Fatalf("successful cold DoH prober ran %d probes, expected 1", probeCount.Load())
	}
	if mux.tunnelDohCold() {
		t.Fatal("successful cold DoH probe did not prove the path")
	}
}

func TestUpgradeMuxWarmDnsWithoutFallbackIsOneShot(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := &UpgradeMux{
		ctx:                  ctx,
		dnsProberWake:        make(chan struct{}, 1),
		dnsProbeInitialDelay: time.Millisecond,
		dnsProbeMaxDelay:     time.Millisecond,
	}
	mux.settings.Store(&UpgradeMuxSettings{Dns: &DnsUpgradeSettings{Resolver: &DnsResolverSettings{}}})
	mux.markTunnelDohUnproven()
	var probeCount atomic.Int32
	mux.tunnelDohWarmFunction = func(ctx context.Context, serverCount int) bool {
		probeCount.Add(1)
		return false
	}

	mux.WarmDns()
	if !waitForCondition(time.Second, func() bool {
		return 0 < probeCount.Load() && !mux.dnsProberRunning.Load()
	}) {
		t.Fatal("one-shot tunnel warm did not finish")
	}
	time.Sleep(20 * time.Millisecond)
	if probeCount.Load() != 1 {
		t.Fatalf("warm without a fallback ran %d probes, expected one", probeCount.Load())
	}
}

func TestUpgradeMuxWarmDnsDoesNothingWhenDnsInterceptionIsDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := &UpgradeMux{
		ctx:                  ctx,
		dnsProberWake:        make(chan struct{}, 1),
		dnsProbeInitialDelay: time.Millisecond,
		dnsProbeMaxDelay:     time.Millisecond,
	}
	mux.settings.Store(&UpgradeMuxSettings{
		Http: &HttpUpgradeSettings{Mode: HttpUpgradeUnencrypted},
	})
	// Model a stale fallback left while settings are being swapped. Disabled
	// DNS must suppress both warm paths regardless of the cache's presence.
	mux.fallbackDohCache.Store(&DohCache{})
	var tunnelWarmCount atomic.Int32
	var fallbackWarmCount atomic.Int32
	mux.tunnelDohWarmFunction = func(context.Context, int) bool {
		tunnelWarmCount.Add(1)
		return true
	}
	mux.fallbackDohWarmFunction = func(context.Context, *DohCache, int) bool {
		fallbackWarmCount.Add(1)
		return true
	}

	mux.WarmDns()
	time.Sleep(20 * time.Millisecond)
	if got := tunnelWarmCount.Load(); got != 0 {
		t.Fatalf("disabled DNS launched %d tunnel warm calls, expected none", got)
	}
	if got := fallbackWarmCount.Load(); got != 0 {
		t.Fatalf("disabled DNS launched %d fallback warm calls, expected none", got)
	}
	if mux.dnsProbeRequested.Load() || mux.fallbackDohWarmPending.Load() ||
		mux.dnsProberRunning.Load() || mux.fallbackDohWarmerRunning.Load() {
		t.Fatal("disabled DNS retained warm-up work")
	}

	// Enabling DNS later must still make the same mux warm normally.
	mux.settings.Store(&UpgradeMuxSettings{
		Dns: &DnsUpgradeSettings{Resolver: &DnsResolverSettings{}},
	})
	mux.markTunnelDohUnproven()
	mux.WarmDns()
	if !waitForCondition(time.Second, func() bool {
		return tunnelWarmCount.Load() == 1 && fallbackWarmCount.Load() == 1 &&
			!mux.dnsProberRunning.Load() && !mux.fallbackDohWarmerRunning.Load()
	}) {
		t.Fatalf(
			"enabled DNS warm did not finish: tunnel=%d fallback=%d tunnel_running=%t fallback_running=%t",
			tunnelWarmCount.Load(),
			fallbackWarmCount.Load(),
			mux.dnsProberRunning.Load(),
			mux.fallbackDohWarmerRunning.Load(),
		)
	}
}

func TestUpgradeMuxFallbackWarmRequestsCoalesce(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	mux := &UpgradeMux{ctx: ctx}
	mux.settings.Store(&UpgradeMuxSettings{Dns: &DnsUpgradeSettings{Resolver: &DnsResolverSettings{}}})
	mux.fallbackDohCache.Store(&DohCache{})
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondFinished := make(chan struct{})
	var callCount atomic.Int32
	var activeCount atomic.Int32
	var maxActiveCount atomic.Int32
	mux.fallbackDohWarmFunction = func(ctx context.Context, cache *DohCache, serverCount int) bool {
		active := activeCount.Add(1)
		for {
			maxActive := maxActiveCount.Load()
			if active <= maxActive || maxActiveCount.CompareAndSwap(maxActive, active) {
				break
			}
		}
		call := callCount.Add(1)
		switch call {
		case 1:
			close(firstStarted)
			select {
			case <-releaseFirst:
			case <-ctx.Done():
			}
		case 2:
			close(secondFinished)
		}
		activeCount.Add(-1)
		return false
	}

	mux.warmFallbackDns()
	select {
	case <-firstStarted:
	case <-time.After(time.Second):
		t.Fatal("fallback warm did not start")
	}
	for range 64 {
		mux.warmFallbackDns()
	}
	close(releaseFirst)
	select {
	case <-secondFinished:
	case <-time.After(time.Second):
		t.Fatal("coalesced follow-up fallback warm did not run")
	}
	if !waitForCondition(time.Second, func() bool {
		return !mux.fallbackDohWarmerRunning.Load()
	}) {
		t.Fatal("fallback warm worker did not retire")
	}
	if callCount.Load() != 2 {
		t.Fatalf("fallback warm requests produced %d calls, expected 2", callCount.Load())
	}
	if maxActiveCount.Load() != 1 {
		t.Fatalf("maximum concurrent fallback warms = %d, expected 1", maxActiveCount.Load())
	}
}
