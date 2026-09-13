package connect

// RemoveClientArgs teardown scope: a shutdown-caused teardown (generator ctx
// done) preserves the window identities ONLY when an identity store is
// configured (the proxy case — a replacement container reuses them). With the
// default nil store (plain sdk apps) the historical best-effort delete runs,
// so window platform-client rows do not leak until server-side idle reap.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	gojwt "github.com/golang-jwt/jwt/v5"

	"github.com/urnetwork/connect/protocol"
)

// The default runtime history is derived rather than copied: ten possible
// live clients across both windows, retained for each 15-second maintenance
// opportunity in one 60-minute channel lifetime.
func TestDefaultApiRuntimeExcludeClientMaxCountTracksWindowLifecycle(t *testing.T) {
	multiSettings := DefaultMultiClientSettings()
	if got, want := apiRuntimeExcludeClientMaxCount(multiSettings), 2400; got != want {
		t.Fatalf("default runtime exclusion max = %d, want %d", got, want)
	}
	if got, want := DefaultApiMultiClientGeneratorSettings().RuntimeExcludeClientMaxCount,
		apiRuntimeExcludeClientMaxCount(multiSettings); got != want {
		t.Fatalf("API runtime exclusion max = %d, want derived %d", got, want)
	}

	custom := &MultiClientSettings{
		WindowSizes: map[WindowType]WindowSizeSettings{
			WindowTypeQuality: {WindowSizeHardMax: 2},
			WindowTypeSpeed:   {WindowSizeHardMax: 1},
		},
		MaxClientLifetime:   61 * time.Second,
		WindowResizeTimeout: 30 * time.Second,
	}
	if got, want := apiRuntimeExcludeClientMaxCount(custom), 9; got != want {
		t.Fatalf("custom runtime exclusion max = %d, want %d", got, want)
	}
}

// Runtime exclusions are a bounded FIFO independent of constructor policy.
// Overflow makes only the oldest runtime provider eligible again; seeded
// exclusions remain durable and duplicate calls consume no capacity.
func TestApiRuntimeExclusionsEvictOldestAndPreserveConstructor(t *testing.T) {
	seeded := []Id{NewId(), NewId()}
	constructorExclusions := []Id{seeded[0], seeded[1], seeded[0]}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultApiMultiClientGeneratorSettings()
	settings.RuntimeExcludeClientMaxCount = 3
	generator := NewApiMultiClientGenerator(
		ctx,
		nil,
		nil,
		constructorExclusions,
		"",
		"synthetic-token",
		"",
		"synthetic-device",
		"synthetic-spec",
		"0.0.0-test",
		nil,
		DefaultClientSettings,
		settings,
	)
	constructorExclusions[0] = NewId()
	if got := generator.ExcludeClientIds(); !slices.Equal(got, seeded) {
		t.Fatalf("constructor exclusion snapshot aliases caller slice: %v, want %v", got, seeded)
	}
	first := NewId()
	second := NewId()
	third := NewId()
	fourth := NewId()
	for _, clientId := range []Id{seeded[0], first, second, third, second} {
		generator.ExcludeClientId(clientId)
	}
	if got, want := generator.ExcludeClientIds(),
		append(slices.Clone(seeded), first, second, third); !slices.Equal(got, want) {
		t.Fatalf("full exclusions = %v, want %v", got, want)
	}
	fullCapacity := cap(generator.runtimeExcludeClientIds)

	generator.ExcludeClientId(fourth)
	if got, want := generator.ExcludeClientIds(),
		append(slices.Clone(seeded), second, third, fourth); !slices.Equal(got, want) {
		t.Fatalf("overflow exclusions = %v, want %v", got, want)
	}
	// The evicted id is eligible to enter again at the newest edge. This is the
	// bounded recovery path; the generator never latches discovery closed.
	generator.ExcludeClientId(first)
	if got, want := generator.ExcludeClientIds(),
		append(slices.Clone(seeded), third, fourth, first); !slices.Equal(got, want) {
		t.Fatalf("recovered exclusions = %v, want %v", got, want)
	}
	if got := cap(generator.runtimeExcludeClientIds); got != fullCapacity {
		t.Fatalf("runtime exclusion capacity grew after overflow: %d -> %d", fullCapacity, got)
	}
}

// Concurrent status callbacks and discovery snapshots preserve the strict
// runtime cap and set/ring agreement. Exact survivors depend on scheduling;
// the final sequential add pins the newest ordering edge deterministically.
func TestApiRuntimeExclusionsConcurrentAddAndSnapshot(t *testing.T) {
	seeded := []Id{NewId(), NewId()}
	const runtimeMax = 64
	generator := &ApiMultiClientGenerator{
		excludeClientIds:             slices.Clone(seeded),
		runtimeExcludeClientMaxCount: runtimeMax,
	}
	clientIds := make([]Id, 8*runtimeMax)
	for i := range clientIds {
		clientIds[i] = NewId()
	}

	var wait sync.WaitGroup
	for _, clientId := range clientIds {
		clientId := clientId
		wait.Add(2)
		go func() {
			defer wait.Done()
			generator.ExcludeClientId(clientId)
			generator.ExcludeClientId(clientId)
			generator.ExcludeClientId(seeded[0])
		}()
		go func() {
			defer wait.Done()
			_ = generator.ExcludeClientIds()
		}()
	}
	wait.Wait()

	newest := NewId()
	generator.ExcludeClientId(newest)
	got := generator.ExcludeClientIds()
	if len(got) != len(seeded)+runtimeMax {
		t.Fatalf("bounded exclusion count = %d, want %d", len(got), len(seeded)+runtimeMax)
	}
	if !slices.Equal(got[:len(seeded)], seeded) {
		t.Fatalf("constructor exclusions changed: %v, want %v", got[:len(seeded)], seeded)
	}
	if got[len(got)-1] != newest {
		t.Fatalf("newest runtime exclusion = %s, want %s", got[len(got)-1], newest)
	}
	seen := map[Id]bool{}
	for _, clientId := range got {
		if seen[clientId] {
			t.Fatalf("duplicate exclusion in snapshot: %s", clientId)
		}
		seen[clientId] = true
	}
	if len(generator.runtimeExcludeClientIdSet) != runtimeMax {
		t.Fatalf("runtime exclusion set count = %d, want %d", len(generator.runtimeExcludeClientIdSet), runtimeMax)
	}
}

// At the strict default runtime cap, the serialized discovery request remains
// under a conservative Connect-owned 512 KiB amplification budget. This
// measures the wire representation rather than estimating from 16-byte Id
// storage or copying another repository's mutable HTTP limit.
func TestDefaultApiRuntimeExclusionsKeepDiscoveryRequestBounded(t *testing.T) {
	settings := DefaultApiMultiClientGeneratorSettings()
	generator := &ApiMultiClientGenerator{
		excludeClientIds:             []Id{NewId()},
		runtimeExcludeClientMaxCount: settings.RuntimeExcludeClientMaxCount,
	}
	for range settings.RuntimeExcludeClientMaxCount + 17 {
		generator.ExcludeClientId(NewId())
	}
	requestBytes, err := json.Marshal(&FindProviders2Args{
		Specs:            []*ProviderSpec{{BestAvailable: true}},
		ExcludeClientIds: generator.ExcludeClientIds(),
		Count:            DefaultMultiClientSettings().WindowExpandBlockCount,
		RankMode:         WindowTypeQuality.RankMode(),
	})
	if err != nil {
		t.Fatal(err)
	}
	const discoveryRequestMax = 512 * 1024
	if len(requestBytes) >= discoveryRequestMax {
		t.Fatalf("bounded discovery request = %d bytes, want below %d-byte Connect budget", len(requestBytes), discoveryRequestMax)
	}
	t.Logf("bounded discovery request = %d bytes (%d runtime ids)", len(requestBytes), settings.RuntimeExcludeClientMaxCount)
}

// Discovery retains the destination and the nearest eight intermediaries when
// a server returns a longer path; shorter legacy paths are unaffected.
func TestNextDestinationsRetainsMaximumIntermediariesAndDestination(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testNextDestinationsRetainsMaximumIntermediariesAndDestination(t, ipVersion)
	})
}

func testNextDestinationsRetainsMaximumIntermediariesAndDestination(t *testing.T, ipVersion int) {
	intermediaryIds := make([]Id, MaxMultihopLength+3)
	for idIndex := range intermediaryIds {
		intermediaryIds[idIndex] = NewId()
	}
	providerId := NewId()
	wantEstimatedBytesPerSecond := ByteCount(7_500_000)
	server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/hello" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if request.URL.Path != "/network/find-providers2" {
			http.NotFound(w, request)
			return
		}
		if err := json.NewEncoder(w).Encode(&FindProviders2Result{
			Providers: []*FindProvidersProvider{{
				ClientId:                providerId,
				IntermediaryIds:         intermediaryIds,
				EstimatedBytesPerSecond: wantEstimatedBytesPerSecond,
				Tier:                    0,
				NetworkOnly:             true,
				ReputationFailedNames:   " Bloomberg ,canva,bloomberg",
			}},
		}); err != nil {
			t.Errorf("encode discovery response: %v", err)
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultClientStrategySettings()
	settings.EnableNormal = true
	settings.EnableResilient = false
	settings.RequestTimeout = 5 * time.Second
	strategy := NewClientStrategy(ctx, settings)
	generator := NewApiMultiClientGenerator(
		ctx,
		[]*ProviderSpec{{BestAvailable: true}},
		strategy,
		nil,
		server.URL,
		"test-jwt",
		server.URL,
		"test-description",
		"test-spec",
		"0.0.0-test",
		nil,
		DefaultClientSettings,
		DefaultApiMultiClientGeneratorSettings(),
	)
	destinations, err := generator.NextDestinationsContext(ctx, 1, nil, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(destinations) != 1 {
		t.Fatalf("destination count=%d want=1", len(destinations))
	}
	wantIds := append(
		slices.Clone(intermediaryIds[len(intermediaryIds)-MaxMultihopLength:]),
		providerId,
	)
	for destination, stats := range destinations {
		if !slices.Equal(destination.Ids(), wantIds) {
			t.Fatalf("destination ids=%v want=%v", destination.Ids(), wantIds)
		}
		if stats.EstimatedBytesPerSecond != wantEstimatedBytesPerSecond || !stats.NetworkOnly {
			t.Fatalf("discovery stats=%+v, want speed and network-only metadata", stats)
		}
		if !slices.Equal(stats.ReputationFailures, []string{"bloomberg", "canva"}) {
			t.Fatalf("reputation failures=%q, want normalized Bloomberg/Canva", stats.ReputationFailures)
		}
	}
}

// A DeviceLocal refreshes its top-level client JWT independently of an
// already-running destination window. Later expansion and retirement must use
// that refreshed credential; keeping the generator's constructor JWT turns
// healthy long-lived sessions into 401s once the old token expires.
func TestApiMultiClientGeneratorUsesRefreshedJwtForFutureClientLifecycle(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testApiMultiClientGeneratorUsesRefreshedJwtForFutureClientLifecycle(t, ipVersion)
	})
}

func testApiMultiClientGeneratorUsesRefreshedJwtForFutureClientLifecycle(t *testing.T, ipVersion int) {
	derivedClientId := NewId()
	derivedToken := gojwt.NewWithClaims(gojwt.SigningMethodHS256, gojwt.MapClaims{
		"client_id": derivedClientId.String(),
	})
	derivedJwt, err := derivedToken.SignedString([]byte("test-only-key"))
	if err != nil {
		t.Fatal(err)
	}

	type requestAuth struct {
		path          string
		authorization string
	}
	requests := make(chan requestAuth, 2)
	server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/hello":
			w.WriteHeader(http.StatusOK)
		case "/network/auth-client":
			requests <- requestAuth{path: request.URL.Path, authorization: request.Header.Get("Authorization")}
			_ = json.NewEncoder(w).Encode(&AuthNetworkClientResult{
				ByClientJwt: derivedJwt,
			})
		case "/network/remove-client":
			requests <- requestAuth{path: request.URL.Path, authorization: request.Header.Get("Authorization")}
			_, _ = w.Write([]byte("{}"))
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	strategySettings := DefaultClientStrategySettings()
	strategySettings.EnableNormal = true
	strategySettings.EnableResilient = false
	strategySettings.RequestTimeout = time.Second
	strategy := NewClientStrategy(ctx, strategySettings)
	generator := NewApiMultiClientGenerator(
		ctx,
		nil,
		strategy,
		nil,
		server.URL,
		"constructor-jwt",
		server.URL,
		"test-description",
		"test-spec",
		"0.0.0-test",
		nil,
		DefaultClientSettings,
		DefaultApiMultiClientGeneratorSettings(),
	)
	generator.SetByJwt("refreshed-jwt")

	args, err := generator.NewClientArgsContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	generator.RemoveClientArgs(args)

	for _, wantPath := range []string{"/network/auth-client", "/network/remove-client"} {
		select {
		case request := <-requests:
			if request.path != wantPath {
				t.Fatalf("request path = %q, want %q", request.path, wantPath)
			}
			if request.authorization != "Bearer refreshed-jwt" {
				t.Fatalf("%s authorization = %q, want refreshed JWT", request.path, request.authorization)
			}
		case <-ctx.Done():
			t.Fatalf("waiting for %s: %v", wantPath, ctx.Err())
		}
	}
}

// newRemoveClientTestGenerator builds a generator against a counting api
// server. The client strategy lives on its own ctx (like the app-scoped
// strategy in the field), so it outlives the generator teardown.
func newRemoveClientTestGenerator(t *testing.T, ipVersion int, generatorCtx context.Context, strategyCtx context.Context) (*ApiMultiClientGenerator, *atomic.Int32, func()) {
	t.Helper()

	removeCount := &atomic.Int32{}
	server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/network/remove-client") {
			removeCount.Add(1)
		}
		fmt.Fprintf(w, "{}")
	}))

	// a single-dialer strategy so each api call maps to exactly one server hit
	settings := DefaultClientStrategySettings()
	settings.EnableNormal = true
	settings.EnableResilient = false
	settings.RequestTimeout = 5 * time.Second
	strategy := NewClientStrategy(strategyCtx, settings)

	clientId := NewId()
	generator := NewApiMultiClientGenerator(
		generatorCtx,
		[]*ProviderSpec{{ClientId: &clientId}},
		strategy,
		nil,
		server.URL,
		"test-jwt",
		server.URL,
		"test-description",
		"test-spec",
		"0.0.0-test",
		nil,
		DefaultClientSettings,
		DefaultApiMultiClientGeneratorSettings(),
	)
	return generator, removeCount, server.Close
}

func TestRemoveClientArgsTeardownNoStoreDeletes(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testRemoveClientArgsTeardownNoStoreDeletes(t, ipVersion)
	})
}

func testRemoveClientArgsTeardownNoStoreDeletes(t *testing.T, ipVersion int) {
	strategyCtx, strategyCancel := context.WithCancel(context.Background())
	defer strategyCancel()
	generatorCtx, generatorCancel := context.WithCancel(context.Background())

	generator, removeCount, closeServer := newRemoveClientTestGenerator(t, ipVersion, generatorCtx, strategyCtx)
	defer closeServer()

	// shutdown-caused teardown with NO identity store: the best-effort
	// delete must still reach the api so the platform client row is removed
	generatorCancel()
	generator.RemoveClientArgs(&MultiClientGeneratorClientArgs{
		ClientId: NewId(),
	})

	if !waitForCondition(5*time.Second, func() bool {
		return 1 <= removeCount.Load()
	}) {
		t.Fatal("teardown with no identity store must best-effort delete the network client")
	}
}

func TestRemoveClientArgsTeardownStorePreservesIdentities(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testRemoveClientArgsTeardownStorePreservesIdentities(t, ipVersion)
	})
}

func testRemoveClientArgsTeardownStorePreservesIdentities(t *testing.T, ipVersion int) {
	strategyCtx, strategyCancel := context.WithCancel(context.Background())
	defer strategyCancel()
	generatorCtx, generatorCancel := context.WithCancel(context.Background())

	generator, removeCount, closeServer := newRemoveClientTestGenerator(t, ipVersion, generatorCtx, strategyCtx)
	defer closeServer()

	// an identity store is configured (the proxy case): identities must
	// survive teardown for the replacement container
	store := &fakeIdentityStore{}
	generator.SetIdentityStore(store)

	identity := &WindowClientIdentity{
		ClientId:    NewId(),
		ByJwt:       "jwt-live",
		InstanceId:  NewId(),
		Destination: RequireMultiHopId(NewId()),
	}
	generator.identityState.Record(identity)
	waitForPersisted(t, store, "the live identity", func(persisted []*WindowClientIdentity) bool {
		return len(persisted) == 1
	})

	// shutdown-caused teardown: neither the persisted identity nor the live
	// network client is removed
	generatorCancel()
	generator.RemoveClientArgs(&MultiClientGeneratorClientArgs{
		ClientId: identity.ClientId,
	})

	time.Sleep(500 * time.Millisecond)
	if count := removeCount.Load(); count != 0 {
		t.Fatalf("teardown with an identity store deleted %d network clients, want 0 (identities must survive)", count)
	}
	persisted := store.snapshot()
	if len(persisted) != 1 || persisted[0].ClientId != identity.ClientId {
		t.Fatalf("teardown with an identity store must preserve the persisted snapshot, have %d", len(persisted))
	}
}

func TestRemoveClientArgsLiveEvictionDeletes(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testRemoveClientArgsLiveEvictionDeletes(t, ipVersion)
	})
}

func testRemoveClientArgsLiveEvictionDeletes(t *testing.T, ipVersion int) {
	strategyCtx, strategyCancel := context.WithCancel(context.Background())
	defer strategyCancel()
	generatorCtx, generatorCancel := context.WithCancel(context.Background())
	defer generatorCancel()

	generator, removeCount, closeServer := newRemoveClientTestGenerator(t, ipVersion, generatorCtx, strategyCtx)
	defer closeServer()

	// a window eviction while the ctx is live removes for real — with or
	// without a store configured
	store := &fakeIdentityStore{}
	generator.SetIdentityStore(store)
	identity := &WindowClientIdentity{
		ClientId:    NewId(),
		ByJwt:       "jwt-evict",
		InstanceId:  NewId(),
		Destination: RequireMultiHopId(NewId()),
	}
	generator.identityState.Record(identity)
	waitForPersisted(t, store, "the live identity", func(persisted []*WindowClientIdentity) bool {
		return len(persisted) == 1
	})

	generator.RemoveClientArgs(&MultiClientGeneratorClientArgs{
		ClientId: identity.ClientId,
	})

	if !waitForCondition(5*time.Second, func() bool {
		return 1 <= removeCount.Load()
	}) {
		t.Fatal("a live window eviction must delete the network client")
	}
	waitForPersisted(t, store, "the evicted identity dropped", func(persisted []*WindowClientIdentity) bool {
		return len(persisted) == 0
	})
}

func TestRemoveClientArgsStaleGenerationCannotDeleteLiveReplacement(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testRemoveClientArgsStaleGenerationCannotDeleteLiveReplacement(t, ipVersion)
	})
}

func testRemoveClientArgsStaleGenerationCannotDeleteLiveReplacement(t *testing.T, ipVersion int) {
	strategyCtx, strategyCancel := context.WithCancel(context.Background())
	defer strategyCancel()
	generatorCtx, generatorCancel := context.WithCancel(context.Background())
	defer generatorCancel()

	generator, removeCount, closeServer := newRemoveClientTestGenerator(t, ipVersion, generatorCtx, strategyCtx)
	defer closeServer()
	store := &fakeIdentityStore{}
	generator.SetIdentityStore(store)

	clientId := NewId()
	oldIdentity := &WindowClientIdentity{
		ClientId:    clientId,
		ByJwt:       "jwt-old",
		InstanceId:  NewId(),
		Destination: RequireMultiHopId(NewId()),
	}
	replacement := &WindowClientIdentity{
		ClientId:    clientId,
		ByJwt:       "jwt-replacement",
		InstanceId:  NewId(),
		Destination: RequireMultiHopId(NewId()),
	}
	generator.identityState.Record(oldIdentity)
	generator.identityState.Record(replacement)
	waitForPersisted(t, store, "replacement identity", func(persisted []*WindowClientIdentity) bool {
		return len(persisted) == 1 && persisted[0].InstanceId == replacement.InstanceId
	})

	// The old channel's asynchronous cleanup arrives after replacement under
	// the same client id. It must not erase persistence or issue the server
	// removal, which is keyed only by client id and would kill the live row.
	generator.RemoveClientArgs(&MultiClientGeneratorClientArgs{
		ClientId: clientId,
		ClientAuth: &ClientAuth{
			InstanceId: oldIdentity.InstanceId,
		},
	})
	time.Sleep(100 * time.Millisecond)
	if count := removeCount.Load(); count != 0 {
		t.Fatalf("stale generation emitted %d remove-client requests", count)
	}
	persisted := store.snapshot()
	if len(persisted) != 1 || persisted[0].InstanceId != replacement.InstanceId {
		t.Fatal("stale generation erased the persisted replacement")
	}

	// The current generation still owns cleanup and must perform both effects.
	generator.RemoveClientArgs(&MultiClientGeneratorClientArgs{
		ClientId: clientId,
		ClientAuth: &ClientAuth{
			InstanceId: replacement.InstanceId,
		},
	})
	if !waitForCondition(5*time.Second, func() bool {
		return removeCount.Load() == 1
	}) {
		t.Fatal("current generation did not remove its network client")
	}
	waitForPersisted(t, store, "current identity removed", func(persisted []*WindowClientIdentity) bool {
		return len(persisted) == 0
	})
}

// A window client can still be sending its final contract-close control when
// the window retires it. The derived client JWT must remain valid until that
// OOB lifecycle is joined; deleting the network-client row first makes the
// close fail with 401 and leaves server-side contract cleanup behind.
func TestRemoveClientWithArgsJoinsOobBeforeIdentityRevocation(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testRemoveClientWithArgsJoinsOobBeforeIdentityRevocation(t, ipVersion)
	})
}

func testRemoveClientWithArgsJoinsOobBeforeIdentityRevocation(t *testing.T, ipVersion int) {
	controlStarted := make(chan struct{})
	controlRelease := make(chan struct{})
	controlDone := make(chan error, 1)
	var startOnce sync.Once
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(controlRelease) }) })
	removeCount := &atomic.Int32{}

	apiServer := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/connect/control":
			var args ConnectControlArgs
			if err := json.NewDecoder(r.Body).Decode(&args); err != nil {
				t.Errorf("decode control: %v", err)
				http.Error(w, "bad control", http.StatusBadRequest)
				return
			}
			startOnce.Do(func() { close(controlStarted) })
			<-controlRelease
			_ = json.NewEncoder(w).Encode(&ConnectControlResult{Pack: args.Pack})
		case "/network/remove-client":
			removeCount.Add(1)
			_, _ = w.Write([]byte("{}"))
		default:
			http.NotFound(w, r)
		}
	}))
	defer apiServer.Close()

	strategyCtx, strategyCancel := context.WithCancel(context.Background())
	defer strategyCancel()
	strategySettings := DefaultClientStrategySettings()
	strategySettings.EnableNormal = true
	strategySettings.EnableResilient = false
	strategySettings.RequestTimeout = 5 * time.Second
	strategy := NewClientStrategy(strategyCtx, strategySettings)

	generatorCtx, generatorCancel := context.WithCancel(context.Background())
	defer generatorCancel()
	providerId := NewId()
	generator := NewApiMultiClientGenerator(
		generatorCtx,
		[]*ProviderSpec{{ClientId: &providerId}},
		strategy,
		nil,
		apiServer.URL,
		"network-jwt",
		apiServer.URL,
		"test-description",
		"test-spec",
		"0.0.0-test",
		nil,
		DefaultClientSettings,
		DefaultApiMultiClientGeneratorSettings(),
	)

	clientCtx, clientCancel := context.WithCancel(context.Background())
	defer clientCancel()
	clientId := NewId()
	clientOob := NewApiOutOfBandControl(clientCtx, strategy, "derived-client-jwt", apiServer.URL)
	client := NewClient(clientCtx, clientId, clientOob, DefaultClientSettings())
	clientOob.SendControlWithCtx(
		context.Background(),
		[]*protocol.Frame{},
		func(_ []*protocol.Frame, err error) { controlDone <- err },
	)
	select {
	case <-controlStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup control did not reach the server")
	}

	clientArgs := &MultiClientGeneratorClientArgs{
		ClientId: clientId,
		ClientAuth: &ClientAuth{
			ByJwt:      "derived-client-jwt",
			InstanceId: NewId(),
		},
	}
	generator.RemoveClientWithArgs(client, clientArgs)
	client.Cancel()
	time.Sleep(100 * time.Millisecond)
	if count := removeCount.Load(); count != 0 {
		t.Fatalf("identity was revoked while cleanup control was in flight: remove count %d", count)
	}

	releaseOnce.Do(func() { close(controlRelease) })
	select {
	case err := <-controlDone:
		if err != nil {
			t.Fatalf("cleanup control failed: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cleanup control did not finish")
	}
	if !waitForCondition(5*time.Second, func() bool { return removeCount.Load() == 1 }) {
		t.Fatal("identity was not revoked after cleanup control completed")
	}
}

// Unused client args still own a platform identity. Their direct asynchronous
// removal must enter the same retirement gate as generated Clients, or closing
// the generator can cancel the request and leave the row active.
func TestApiMultiClientGeneratorCloseAndWaitJoinsDirectClientArgsRemoval(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testApiMultiClientGeneratorCloseAndWaitJoinsDirectClientArgsRemoval(t, ipVersion)
	})
}

func testApiMultiClientGeneratorCloseAndWaitJoinsDirectClientArgsRemoval(t *testing.T, ipVersion int) {
	removeStarted := make(chan struct{})
	removeRelease := make(chan struct{})
	var startOnce sync.Once
	var releaseOnce sync.Once
	removeCount := &atomic.Int32{}

	apiServer := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/hello":
			w.WriteHeader(http.StatusOK)
		case "/network/remove-client":
			removeCount.Add(1)
			startOnce.Do(func() { close(removeStarted) })
			select {
			case <-removeRelease:
				_, _ = w.Write([]byte("{}"))
			case <-r.Context().Done():
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(apiServer.Close)
	t.Cleanup(func() { releaseOnce.Do(func() { close(removeRelease) }) })

	strategyCtx, strategyCancel := context.WithCancel(t.Context())
	defer strategyCancel()
	strategySettings := DefaultClientStrategySettings()
	strategySettings.EnableNormal = true
	strategySettings.EnableResilient = false
	strategySettings.RequestTimeout = 5 * time.Second
	strategy := NewClientStrategy(strategyCtx, strategySettings)

	generatorCtx, generatorCancel := context.WithCancel(t.Context())
	defer generatorCancel()
	generator := NewApiMultiClientGenerator(
		generatorCtx,
		nil,
		strategy,
		nil,
		apiServer.URL,
		"synthetic-network-jwt",
		apiServer.URL,
		"synthetic-description",
		"synthetic-spec",
		"0.0.0-test",
		nil,
		DefaultClientSettings,
		DefaultApiMultiClientGeneratorSettings(),
	)
	retirementWaitEntered := make(chan struct{})
	var retirementWaitOnce sync.Once
	generator.beforeRetirementWaitForTest = func() {
		retirementWaitOnce.Do(func() { close(retirementWaitEntered) })
	}
	args := &MultiClientGeneratorClientArgs{
		ClientId: NewId(),
		ClientAuth: &ClientAuth{
			InstanceId: NewId(),
		},
	}

	// Admission happens synchronously before the request worker launches.
	generator.RemoveClientArgs(args)
	select {
	case <-removeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("direct remove-client request did not reach the synthetic server")
	}

	closeCtx, closeCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer closeCancel()
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- generator.CloseAndWait(closeCtx)
	}()
	retirements := generator.retirementLifecycle()
	select {
	case <-retirementWaitEntered:
	case <-closeCtx.Done():
		t.Fatalf("generator close did not reach direct-removal join: %v", closeCtx.Err())
	}
	select {
	case <-retirements.Done():
		t.Fatal("direct-removal retirement became terminal before its response")
	default:
	}
	select {
	case err := <-closeResult:
		t.Fatalf("generator close returned before direct removal completed: %v", err)
	default:
	}

	releaseOnce.Do(func() { close(removeRelease) })
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-closeCtx.Done():
		t.Fatalf("wait for joined direct remove-client request: %v", closeCtx.Err())
	}
	if count := removeCount.Load(); count != 1 {
		t.Fatalf("direct remove-client request count = %d, want 1", count)
	}
}

// A short-lived provider probe closes its generated Client and then the
// generator. The final remove-client request is part of that retirement: if
// the retirement worker merely launches it and returns, CloseAndWait cancels
// the API context underneath the request and leaves the derived row active
// until the server's much later idle reaper.
func TestApiMultiClientGeneratorCloseAndWaitJoinsClientRemoval(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testApiMultiClientGeneratorCloseAndWaitJoinsClientRemoval(t, ipVersion)
	})
}

func testApiMultiClientGeneratorCloseAndWaitJoinsClientRemoval(t *testing.T, ipVersion int) {
	removeStarted := make(chan struct{})
	removeRelease := make(chan struct{})
	var startOnce sync.Once
	var releaseOnce sync.Once
	removeCount := &atomic.Int32{}

	apiServer := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/hello":
			w.WriteHeader(http.StatusOK)
		case "/network/remove-client":
			removeCount.Add(1)
			startOnce.Do(func() { close(removeStarted) })
			select {
			case <-removeRelease:
				_, _ = w.Write([]byte("{}"))
			case <-r.Context().Done():
			}
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(apiServer.Close)
	t.Cleanup(func() { releaseOnce.Do(func() { close(removeRelease) }) })

	strategyCtx, strategyCancel := context.WithCancel(t.Context())
	defer strategyCancel()
	strategySettings := DefaultClientStrategySettings()
	strategySettings.EnableNormal = true
	strategySettings.EnableResilient = false
	strategySettings.RequestTimeout = 5 * time.Second
	strategy := NewClientStrategy(strategyCtx, strategySettings)

	generatorCtx, generatorCancel := context.WithCancel(t.Context())
	defer generatorCancel()
	generator := NewApiMultiClientGenerator(
		generatorCtx,
		nil,
		strategy,
		nil,
		apiServer.URL,
		"synthetic-network-jwt",
		apiServer.URL,
		"synthetic-description",
		"synthetic-spec",
		"0.0.0-test",
		nil,
		DefaultClientSettings,
		DefaultApiMultiClientGeneratorSettings(),
	)
	client := NewClient(generatorCtx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	args := &MultiClientGeneratorClientArgs{
		ClientId: client.ClientId(),
		ClientAuth: &ClientAuth{
			InstanceId: NewId(),
		},
	}

	// RemoveClientWithArgs admits the retirement synchronously before it
	// launches the worker, so CloseAndWait must join this exact request.
	generator.RemoveClientWithArgs(client, args)
	client.Cancel()
	select {
	case <-removeStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("remove-client request did not reach the synthetic server")
	}

	closeCtx, closeCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer closeCancel()
	closeResult := make(chan error, 1)
	go func() {
		closeResult <- generator.CloseAndWait(closeCtx)
	}()
	retirements := generator.retirementLifecycle()
	if !waitForCondition(time.Second, func() bool {
		retirements.stateLock.Lock()
		defer retirements.stateLock.Unlock()
		return !retirements.open
	}) {
		t.Fatal("generator close did not close the retirement admission gate")
	}
	select {
	case err := <-closeResult:
		t.Fatalf("generator close returned before remove-client completed: %v", err)
	default:
	}

	releaseOnce.Do(func() { close(removeRelease) })
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-closeCtx.Done():
		t.Fatalf("wait for joined remove-client request: %v", closeCtx.Err())
	}
	if count := removeCount.Load(); count != 1 {
		t.Fatalf("remove-client request count = %d, want 1", count)
	}
}

// A generated client's channel hands retirement back asynchronously after its
// cancellation edge. Generator teardown must wait for that Client/OOB join;
// otherwise a P2P send route can retain pooled Transfer frames after teardown.
func TestApiMultiClientGeneratorCloseAndWaitJoinsGeneratedClientRetirement(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testApiMultiClientGeneratorCloseAndWaitJoinsGeneratedClientRetirement(t, ipVersion)
	})
}

func testApiMultiClientGeneratorCloseAndWaitJoinsGeneratedClientRetirement(t *testing.T, ipVersion int) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	apiServer := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/hello", "/network/remove-client":
			_, _ = w.Write([]byte("{}"))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(apiServer.Close)

	strategySettings := DefaultClientStrategySettings()
	strategy := NewClientStrategy(ctx, strategySettings)
	generator := NewApiMultiClientGenerator(
		ctx,
		nil,
		strategy,
		nil,
		apiServer.URL,
		"network-jwt",
		apiServer.URL,
		"test-description",
		"test-spec",
		"0.0.0-test",
		nil,
		DefaultClientSettings,
		DefaultApiMultiClientGeneratorSettings(),
	)
	client := NewClient(ctx, NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	args := &MultiClientGeneratorClientArgs{
		ClientId: client.ClientId(),
		ClientAuth: &ClientAuth{
			InstanceId: NewId(),
		},
	}

	// Model the successful NewClient boundary without making a platform request.
	// The channel owner observes Client.Done and then returns the client through
	// the same RemoveClientWithArgs path used by RemoteUserNatMultiClient.
	generator.transportLock.Lock()
	generator.transportIdle = make(chan struct{})
	generator.transports[client] = &apiWindowClientTransport{}
	generator.transportLock.Unlock()
	go func() {
		<-client.Done()
		generator.RemoveClientWithArgs(client, args)
	}()

	retirementEntered := make(chan struct{})
	releaseRetirement := make(chan struct{})
	var enteredOnce sync.Once
	var releaseOnce sync.Once
	client.beforeRunDoneWaitForTest = func() {
		enteredOnce.Do(func() { close(retirementEntered) })
		<-releaseRetirement
	}
	defer releaseOnce.Do(func() { close(releaseRetirement) })

	closeResult := make(chan error, 1)
	go func() {
		closeResult <- generator.CloseAndWait(ctx)
	}()
	waitCloseWaitBarrier(t, ctx, retirementEntered, "generated client retirement")
	select {
	case err := <-closeResult:
		t.Fatalf("generator close skipped held client retirement: %v", err)
	default:
	}

	releaseOnce.Do(func() { close(releaseRetirement) })
	select {
	case err := <-closeResult:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatalf("wait for generated client retirement: %v", ctx.Err())
	}
}

// A destination generator is replaced while its parent DeviceLocal remains
// alive. Its own API and identity workers must end at generator retirement;
// otherwise every reconnect retains one loader/writer tree until the entire
// device closes.
func TestApiMultiClientGeneratorCloseCancelsOwnedIdentityWorkers(t *testing.T) {
	parentCtx, parentCancel := context.WithCancel(context.Background())
	defer parentCancel()
	strategy := NewClientStrategy(parentCtx, DefaultClientStrategySettings())
	generator := NewApiMultiClientGenerator(
		parentCtx,
		nil,
		strategy,
		nil,
		"http://127.0.0.1:1",
		"network-jwt",
		"http://127.0.0.1:1",
		"test-description",
		"test-spec",
		"0.0.0-test",
		nil,
		DefaultClientSettings,
		DefaultApiMultiClientGeneratorSettings(),
	)
	store := &contextLoadIdentityStore{
		started:  make(chan struct{}),
		canceled: make(chan struct{}),
	}
	generator.SetIdentityStore(store)
	select {
	case <-store.started:
	case <-time.After(time.Second):
		t.Fatal("identity loader did not start")
	}

	closeCtx, closeCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer closeCancel()
	if err := generator.CloseAndWait(closeCtx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-store.canceled:
	case <-time.After(time.Second):
		t.Fatal("generator close did not cancel its identity loader")
	}
	select {
	case <-parentCtx.Done():
		t.Fatal("generator close canceled its DeviceLocal parent")
	default:
	}
}
