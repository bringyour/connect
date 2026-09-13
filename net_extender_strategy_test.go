package connect

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net/netip"
	"slices"
	"testing"
	"time"
)

// Strategy tests for the directory-driven extender dialers (EXTENDER.md E2,
// E4, E5).

// One strategy with a directory, the host reported as dual stack and a
// synthetic spoof list.
func newTestExtenderStrategy(
	t *testing.T,
	clock *testClock,
	configure func(settings *ClientStrategySettings),
) (*ClientStrategy, *ExtenderDirectory, ed25519.PrivateKey) {
	t.Helper()
	restoreSpoof := setSpoofDomainsForTest([]string{"spoof.example"})
	t.Cleanup(restoreSpoof)
	restoreProbe := swapControlFamilyProbe(func(family int) bool { return true })
	t.Cleanup(restoreProbe)

	rootSeed, err := NewExtenderKeySeed()
	if err != nil {
		t.Fatal(err)
	}
	rootPrivateKey, err := ExtenderPrivateKeyFromSeed(rootSeed)
	if err != nil {
		t.Fatal(err)
	}

	directorySettings := DefaultExtenderDirectorySettings()
	directorySettings.Now = clock.Now
	directorySettings.NetworkHosts = []string{testExtenderNetworkHost}
	ctx, cancel := context.WithCancel(context.Background())
	directory := NewExtenderDirectory(ctx, directorySettings)
	directory.SetRootKeys(NewExtenderRootKeySet(rootPrivateKey.Public().(ed25519.PublicKey)))

	settings := DefaultClientStrategySettings()
	settings.ExtenderDirectory = directory
	if configure != nil {
		configure(settings)
	}
	clientStrategy := NewClientStrategy(ctx, settings)
	t.Cleanup(func() {
		clientStrategy.Close()
		directory.Close()
		cancel()
	})
	return clientStrategy, directory, rootPrivateKey
}

func testExtenderDialers(clientStrategy *ClientStrategy) []*clientDialer {
	clientStrategy.mutex.Lock()
	defer clientStrategy.mutex.Unlock()
	dialers := []*clientDialer{}
	for dialer := range clientStrategy.dialers {
		if dialer.extenderConfig != nil {
			dialers = append(dialers, dialer)
		}
	}
	return dialers
}

// One dialer per directory candidate address and carrier, at the carrier
// priorities, carrying the record's key (E2, E5).
func TestClientStrategyExpandsOneDialerPerAddressAndCarrier(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, rootPrivateKey := newTestExtenderStrategy(t, clock, nil)

	ip := netip.MustParseAddr("192.0.2.100")
	extenderPublicKey := newTestExtenderKey(t)
	record := signTestRecord(
		t,
		rootPrivateKey,
		extenderPublicKey,
		clock.Now(),
		clock.Now().Add(14*24*time.Hour),
		testExtenderAddress(ip.String(), ExtenderCarrierTcp, ExtenderCarrierQuic, ExtenderCarrierDns),
	)
	if _, err := directory.ApplyRecord(record, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}

	expandedDialers := clientStrategy.expandExtenderDialers()
	if len(expandedDialers) != 3 {
		t.Fatalf("dialers = %d, expected one per carrier", len(expandedDialers))
	}
	priorities := map[ExtenderConnectMode]int{}
	for _, dialer := range expandedDialers {
		extenderConfig := dialer.extenderConfig
		if extenderConfig.Ip != ip {
			t.Fatalf("dialer ip = %s, expected the candidate", extenderConfig.Ip)
		}
		if extenderConfig.Profile.ServerName != "spoof.example" {
			t.Fatalf("dialer name = %q, expected a spoof name", extenderConfig.Profile.ServerName)
		}
		if string(extenderConfig.PublicKey) != string(extenderPublicKey) {
			t.Fatal("the dialer does not carry the record key")
		}
		priorities[extenderConfig.Profile.ConnectMode] = dialer.priority
	}
	expectPriorities := map[ExtenderConnectMode]int{
		ExtenderConnectModeTcpTls: 100,
		ExtenderConnectModeQuic:   110,
		ExtenderConnectModeDns:    120,
	}
	for connectMode, expectPriority := range expectPriorities {
		if priorities[connectMode] != expectPriority {
			t.Fatalf("%s priority = %d, expected %d", connectMode, priorities[connectMode], expectPriority)
		}
	}

	// a second expand adds nothing: every address and carrier pair is visited
	if again := clientStrategy.expandExtenderDialers(); len(again) != 0 {
		t.Fatalf("a second expand added %d dialers", len(again))
	}
}

// The record's own ports and tld reach the dialers (E2, B2).
func TestClientStrategyDialersUseTheRecordPorts(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, rootPrivateKey := newTestExtenderStrategy(t, clock, nil)

	ip := netip.MustParseAddr("192.0.2.101")
	body := signTestRecord(
		t,
		rootPrivateKey,
		newTestExtenderKey(t),
		clock.Now(),
		clock.Now().Add(14*24*time.Hour),
		testExtenderAddress(ip.String(), ExtenderCarrierDns),
	)
	if _, err := directory.ApplyRecord(body, ExtenderSourceFeed); err != nil {
		t.Fatal(err)
	}
	expandedDialers := clientStrategy.expandExtenderDialers()
	if len(expandedDialers) != 1 {
		t.Fatalf("dialers = %d, expected one for the one carrier", len(expandedDialers))
	}
	profile := expandedDialers[0].extenderConfig.Profile
	if profile.ConnectMode != ExtenderConnectModeDns {
		t.Fatalf("connect mode = %s, expected dns", profile.ConnectMode)
	}
	if profile.Port != 53 {
		t.Fatalf("port = %d, expected the record dns port", profile.Port)
	}
	if profile.DnsTld != "x.example." {
		t.Fatalf("dns tld = %q, expected the record tld", profile.DnsTld)
	}
}

// An unverified bootstrap address is dialable on the carrier defaults, which
// is what makes a dns bootstrap useful before any record arrives (E2).
func TestClientStrategyExpandsUnverifiedBootstrapAddresses(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, _ := newTestExtenderStrategy(t, clock, nil)

	ip := netip.MustParseAddr("192.0.2.102")
	directory.AddBootstrap(ip, ExtenderSourceDns)

	expandedDialers := clientStrategy.expandExtenderDialers()
	if len(expandedDialers) != 3 {
		t.Fatalf("dialers = %d, expected one per default carrier", len(expandedDialers))
	}
	for _, dialer := range expandedDialers {
		if 0 < len(dialer.extenderConfig.PublicKey) {
			t.Fatal("an unverified candidate produced a key-pinned dialer")
		}
		switch dialer.extenderConfig.Profile.ConnectMode {
		case ExtenderConnectModeDns:
			if dialer.extenderConfig.Profile.Port != ExtenderDnsPort {
				t.Fatalf("dns port = %d, expected the default", dialer.extenderConfig.Profile.Port)
			}
		default:
			if dialer.extenderConfig.Profile.Port != ExtenderTcpPort {
				t.Fatalf("port = %d, expected the default", dialer.extenderConfig.Profile.Port)
			}
		}
	}
}

// A dial outcome reaches the directory, which is what drives the hold and
// removal policy (E2).
func TestClientStrategyDialerReportsOutcomesToTheDirectory(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, _ := newTestExtenderStrategy(t, clock, nil)
	ip := netip.MustParseAddr("192.0.2.103")
	directory.AddBootstrap(ip, ExtenderSourceDns)

	expandedDialers := clientStrategy.expandExtenderDialers()
	if len(expandedDialers) == 0 {
		t.Fatal("no dialers were expanded")
	}
	dialer := expandedDialers[0]
	ctx := context.Background()

	dialer.Update(ctx, nil)
	entry := testDirectoryEntry(t, directory, ip)
	if entry.SuccessCount != 1 {
		t.Fatalf("success count = %d, expected the success to be reported", entry.SuccessCount)
	}
	dialer.Update(ctx, context.DeadlineExceeded)
	entry = testDirectoryEntry(t, directory, ip)
	if entry.FailureCount != 1 {
		t.Fatalf("failure count = %d, expected the failure to be reported", entry.FailureCount)
	}
	if entry.State != ExtenderStateHold {
		t.Fatalf("state = %s, expected the failure to hold the address", entry.State)
	}

	// an outcome under a canceled context is not evidence about the address
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	dialer.Update(canceledCtx, context.Canceled)
	entry = testDirectoryEntry(t, directory, ip)
	if entry.FailureCount != 1 {
		t.Fatalf("failure count = %d, expected a canceled dial to be ignored", entry.FailureCount)
	}
}

// A dialer whose address the directory retired is dropped, whatever its own
// drop timeout says (E2).
func TestClientStrategyCollapseDropsRetiredAddresses(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, _ := newTestExtenderStrategy(t, clock, nil)
	heldIp := netip.MustParseAddr("192.0.2.104")
	keptIp := netip.MustParseAddr("192.0.2.105")
	directory.AddBootstrap(heldIp, ExtenderSourceDns)
	directory.AddBootstrap(keptIp, ExtenderSourceDns)

	ctx := context.Background()
	clientStrategy.expandExtenderDialers()
	dialers := testExtenderDialers(clientStrategy)
	if len(dialers) != 6 {
		t.Fatalf("dialers = %d, expected three per address", len(dialers))
	}
	// give every dialer a success, so only the directory's verdict can drop one
	for _, dialer := range dialers {
		dialer.Update(ctx, nil)
	}

	// a failure holds the address; its dialers go with it
	directory.RecordFailure(heldIp, ExtenderConnectModeTcpTls)
	clientStrategy.collapseExtenderDialers()
	for _, dialer := range testExtenderDialers(clientStrategy) {
		if dialer.extenderConfig.Ip == heldIp {
			t.Fatal("a held address kept its dialers")
		}
	}
	if count := len(testExtenderDialers(clientStrategy)); count != 3 {
		t.Fatalf("dialers = %d, expected the other address to be kept", count)
	}

	// an address the removal policy retired goes the same way
	clock.advance(8 * 24 * time.Hour)
	for range 3 {
		directory.RecordFailure(keptIp, ExtenderConnectModeTcpTls)
	}
	if testDirectoryKnown(directory, keptIp) {
		t.Fatal("the address was not removed by the stale success rule")
	}
	for _, dialer := range testExtenderDialers(clientStrategy) {
		dialer.Update(ctx, nil)
	}
	clientStrategy.collapseExtenderDialers()
	if count := len(testExtenderDialers(clientStrategy)); count != 0 {
		t.Fatalf("dialers = %d, expected every retired address to be dropped", count)
	}
}

// A dialer that was expanded and never dialed survives the collapse that runs
// before its first attempt, and one whose error has aged past the drop timeout
// still goes (E2). parallelEval launches one parallel block of an expanded
// round at a time, so judging the timeout from a never-set last error dropped
// every candidate past the first block before it was ever tried.
func TestClientStrategyCollapseKeepsUntriedDialers(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, _ := newTestExtenderStrategy(t, clock, func(settings *ClientStrategySettings) {
		settings.ExtenderDropTimeout = time.Minute
	})

	// more candidates than one parallel block launches
	for i := range 4 {
		directory.AddBootstrap(
			netip.MustParseAddr(fmt.Sprintf("192.0.2.%d", 120+i)),
			ExtenderSourceDns,
		)
	}
	expandedDialers := clientStrategy.expandExtenderDialers()
	if len(expandedDialers) <= clientStrategy.settings.ParallelBlockSize {
		t.Fatalf(
			"dialers = %d, expected more than the parallel block of %d",
			len(expandedDialers),
			clientStrategy.settings.ParallelBlockSize,
		)
	}
	for _, dialer := range expandedDialers {
		if dialer.createTime.IsZero() {
			t.Fatal("an expanded dialer carries no creation time")
		}
	}

	// the collapse at the top of the next round runs before any of them dialed
	clientStrategy.collapseExtenderDialers()
	if count := len(testExtenderDialers(clientStrategy)); count != len(expandedDialers) {
		t.Fatalf("dialers = %d, expected every untried dialer to survive", count)
	}

	// a dialer that was created and last failed before the drop timeout is
	// still discovery cache the collapse clears
	now := time.Now()
	staleDialer := expandedDialers[0]
	func() {
		staleDialer.mutex.Lock()
		defer staleDialer.mutex.Unlock()
		staleDialer.createTime = now.Add(-2 * time.Minute)
		staleDialer.errorCount = 1
		staleDialer.lastErrorTime = now.Add(-90 * time.Second)
	}()
	clientStrategy.collapseExtenderDialers()
	remainingDialers := testExtenderDialers(clientStrategy)
	if slices.Contains(remainingDialers, staleDialer) {
		t.Fatal("a dialer whose error aged past the drop timeout was retained")
	}
	if len(remainingDialers) != len(expandedDialers)-1 {
		t.Fatalf("dialers = %d, expected only the stale dialer to be dropped", len(remainingDialers))
	}
}

// A manually configured extender replaces discovery and excludes every other
// dialer (E2).
func TestClientStrategyManualExtendersExcludeDiscovery(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, _ := newTestExtenderStrategy(t, clock, nil)
	directoryIp := netip.MustParseAddr("192.0.2.106")
	directory.AddBootstrap(directoryIp, ExtenderSourceDns)

	manualIp := netip.MustParseAddr("198.51.100.7")
	clientStrategy.SetCustomExtenders(map[netip.Addr]string{manualIp: "secret"})

	expandedDialers := clientStrategy.expandExtenderDialers()
	if len(expandedDialers) != 3 {
		t.Fatalf("dialers = %d, expected one per carrier of the manual address", len(expandedDialers))
	}
	for _, dialer := range expandedDialers {
		if dialer.extenderConfig.Ip != manualIp {
			t.Fatalf("dialer ip = %s, expected the manual address", dialer.extenderConfig.Ip)
		}
		if dialer.extenderConfig.Secret != "secret" {
			t.Fatal("the manual dialer carries no secret")
		}
	}
	// every weighted dialer is an extender while one is configured
	for dialer := range clientStrategy.dialerWeights(false) {
		if dialer.extenderConfig == nil {
			t.Fatal("a manual extender did not exclude the other dialers")
		}
	}
	// the directory does not judge a manual dialer
	for _, dialer := range expandedDialers {
		dialer.Update(context.Background(), nil)
	}
	clientStrategy.collapseExtenderDialers()
	if count := len(testExtenderDialers(clientStrategy)); count != 3 {
		t.Fatalf("dialers = %d, expected the manual dialers to survive the collapse", count)
	}
}

// The startup gate waits only while a first sample is in flight and the
// directory has nothing usable, for at most its timeout (E4).
func TestClientStrategyStartupGateWaitsAtMostTheTimeout(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, _ := newTestExtenderStrategy(t, clock, func(settings *ClientStrategySettings) {
		settings.ExtenderInitialSampleTimeout = 200 * time.Millisecond
	})

	// no network client: nothing will ever complete, so nothing waits
	start := time.Now()
	clientStrategy.waitForExtenderInitialSample(context.Background())
	if 50*time.Millisecond <= time.Since(start) {
		t.Fatal("the gate waited with no network client running")
	}

	// a first sample in flight with an empty directory waits out the timeout
	directory.SetInitialSamplePending()
	start = time.Now()
	clientStrategy.waitForExtenderInitialSample(context.Background())
	elapsed := time.Since(start)
	if elapsed < 200*time.Millisecond {
		t.Fatalf("the gate returned after %s, expected it to wait the timeout", elapsed)
	}
	if 5*time.Second <= elapsed {
		t.Fatalf("the gate waited %s, expected at most the timeout", elapsed)
	}
}

// The gate returns as soon as the first attempt completes, and never waits
// once the directory has something to dial (E4).
func TestClientStrategyStartupGateReleasesOnTheFirstAttempt(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, _ := newTestExtenderStrategy(t, clock, func(settings *ClientStrategySettings) {
		// long enough that only the release below can end the wait
		settings.ExtenderInitialSampleTimeout = 60 * time.Second
	})
	directory.SetInitialSamplePending()

	released := make(chan struct{})
	go func() {
		defer close(released)
		clientStrategy.waitForExtenderInitialSample(context.Background())
	}()
	directory.SetInitialSampleDone()
	select {
	case <-released:
	case <-time.After(10 * time.Second):
		t.Fatal("the gate did not release on the completed first attempt")
	}

	// a completed attempt never waits again
	start := time.Now()
	clientStrategy.waitForExtenderInitialSample(context.Background())
	if 50*time.Millisecond <= time.Since(start) {
		t.Fatal("the gate waited after the first attempt completed")
	}
}

// A stored directory never waits: there is already something to dial (E4).
func TestClientStrategyStartupGateSkipsWithAStoredDirectory(t *testing.T) {
	clock := newTestClock()
	clientStrategy, directory, _ := newTestExtenderStrategy(t, clock, func(settings *ClientStrategySettings) {
		settings.ExtenderInitialSampleTimeout = 60 * time.Second
	})
	directory.SetInitialSamplePending()
	directory.AddBootstrap(netip.MustParseAddr("192.0.2.107"), ExtenderSourceDns)

	start := time.Now()
	clientStrategy.waitForExtenderInitialSample(context.Background())
	if 50*time.Millisecond <= time.Since(start) {
		t.Fatal("the gate waited with a usable directory")
	}
}
