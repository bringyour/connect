package connect

// Window identity bookkeeping (PROXYDRAIN1.md §3.5): restored identities are
// consumed at most once, live pairs mirror to the store on every change
// (asynchronously — the store may be slow and must never block the window),
// and removed identities never reappear in the snapshot.

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeIdentityStore struct {
	mutex     sync.Mutex
	persisted []*WindowClientIdentity
	stores    int
	loads     int
}

func (self *fakeIdentityStore) StoreWindowClientIdentities(identities []*WindowClientIdentity) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.persisted = slices.Clone(identities)
	self.stores += 1
}

func (self *fakeIdentityStore) LoadWindowClientIdentities() []*WindowClientIdentity {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.loads += 1
	return slices.Clone(self.persisted)
}

func (self *fakeIdentityStore) snapshot() []*WindowClientIdentity {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return slices.Clone(self.persisted)
}

// waitForPersisted polls the store until `check` accepts the persisted
// snapshot (the store writes are async), else fails the test.
func waitForPersisted(t *testing.T, store *fakeIdentityStore, description string, check func([]*WindowClientIdentity) bool) {
	t.Helper()
	if !waitForCondition(5*time.Second, func() bool {
		return check(store.snapshot())
	}) {
		t.Fatalf("timeout waiting for persisted snapshot: %s (have %d identities)", description, len(store.snapshot()))
	}
}

func TestWindowIdentityState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	destinationA := RequireMultiHopId(NewId())
	destinationB := RequireMultiHopId(NewId())

	identityA := &WindowClientIdentity{
		ClientId:    NewId(),
		ByJwt:       "jwt-a",
		InstanceId:  NewId(),
		Destination: destinationA,
	}
	// a second identity for the SAME destination: the window may run more
	// than one client to a destination, and both must be restorable
	identityA2 := &WindowClientIdentity{
		ClientId:    NewId(),
		ByJwt:       "jwt-a2",
		InstanceId:  NewId(),
		Destination: destinationA,
	}

	store := &fakeIdentityStore{
		persisted: []*WindowClientIdentity{identityA, identityA2},
	}
	state := newWindowIdentityState(ctx, store)

	// restored destinations are visible for the window expand
	restoredDestinations := state.RestoredDestinations()
	AssertEqual(t, 1, len(restoredDestinations))
	AssertEqual(t, destinationA, restoredDestinations[0])
	// the load happened exactly once
	AssertEqual(t, 1, store.loads)
	state.RestoredDestinations()
	AssertEqual(t, 1, store.loads)

	// consumed at most once each, in order
	taken := state.TakeRestored(destinationA)
	AssertEqual(t, true, taken != nil)
	AssertEqual(t, identityA.ClientId, taken.ClientId)
	AssertEqual(t, "jwt-a", taken.ByJwt)
	taken2 := state.TakeRestored(destinationA)
	AssertEqual(t, true, taken2 != nil)
	AssertEqual(t, identityA2.ClientId, taken2.ClientId)
	AssertEqual(t, true, state.TakeRestored(destinationA) == nil)
	AssertEqual(t, true, state.TakeRestored(destinationB) == nil)
	AssertEqual(t, 0, len(state.RestoredDestinations()))

	// recording the reused identity mirrors it back to the store (async)
	state.Record(taken)
	waitForPersisted(t, store, "the reused identity", func(persisted []*WindowClientIdentity) bool {
		return len(persisted) == 1 && persisted[0].ClientId == identityA.ClientId
	})

	// a second, freshly minted identity joins the snapshot
	identityB := &WindowClientIdentity{
		ClientId:    NewId(),
		ByJwt:       "jwt-b",
		InstanceId:  NewId(),
		Destination: destinationB,
	}
	state.Record(identityB)
	waitForPersisted(t, store, "both identities", func(persisted []*WindowClientIdentity) bool {
		return len(persisted) == 2
	})

	// removal drops the pair from the snapshot for good
	state.Remove(identityA.ClientId)
	waitForPersisted(t, store, "identity A removed", func(persisted []*WindowClientIdentity) bool {
		return len(persisted) == 1 && persisted[0].ClientId == identityB.ClientId
	})

	// removing an unknown client does not rewrite the store
	storesBefore := func() int {
		store.mutex.Lock()
		defer store.mutex.Unlock()
		return store.stores
	}()
	state.Remove(identityA.ClientId)
	time.Sleep(200 * time.Millisecond)
	storesAfter := func() int {
		store.mutex.Lock()
		defer store.mutex.Unlock()
		return store.stores
	}()
	AssertEqual(t, storesBefore, storesAfter)
}

func TestWindowIdentityStaleRemovalCannotDeleteReplacementGeneration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &fakeIdentityStore{}
	state := newWindowIdentityState(ctx, store)
	clientId := NewId()
	oldIdentity := &WindowClientIdentity{
		ClientId:    clientId,
		ByJwt:       "old",
		InstanceId:  NewId(),
		Destination: RequireMultiHopId(NewId()),
	}
	replacement := &WindowClientIdentity{
		ClientId:    clientId,
		ByJwt:       "replacement",
		InstanceId:  NewId(),
		Destination: RequireMultiHopId(NewId()),
	}
	state.Record(oldIdentity)
	state.Record(replacement)
	waitForPersisted(t, store, "replacement generation", func(persisted []*WindowClientIdentity) bool {
		return len(persisted) == 1 && persisted[0].InstanceId == replacement.InstanceId
	})

	if state.RemoveIfCurrent(clientId, oldIdentity.InstanceId) {
		t.Fatal("stale identity generation was allowed to remove its replacement")
	}
	state.mutex.Lock()
	current := state.live[clientId]
	state.mutex.Unlock()
	if current != replacement {
		t.Fatal("stale identity cleanup changed the live replacement")
	}

	if !state.RemoveIfCurrent(clientId, replacement.InstanceId) {
		t.Fatal("current identity generation could not remove itself")
	}
	waitForPersisted(t, store, "current generation removal", func(persisted []*WindowClientIdentity) bool {
		return len(persisted) == 0
	})
}

func TestWindowIdentityRestoreResultIsBounded(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	persisted := make([]*WindowClientIdentity, 0, maxRestoredWindowIdentityCount*2)
	for range maxRestoredWindowIdentityCount * 2 {
		persisted = append(persisted, &WindowClientIdentity{
			ClientId:    NewId(),
			ByJwt:       "jwt",
			InstanceId:  NewId(),
			Destination: RequireMultiHopId(NewId()),
		})
	}
	state := newWindowIdentityState(ctx, &fakeIdentityStore{persisted: persisted})
	destinations, err := state.RestoredDestinationsContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got := len(destinations); got != maxRestoredWindowIdentityCount {
		t.Fatalf("restored identities retained = %d, want cap %d", got, maxRestoredWindowIdentityCount)
	}
}

func TestWindowIdentityStateNoStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// nil store: everything is a no-op and nothing panics
	state := newWindowIdentityState(ctx, nil)
	AssertEqual(t, false, state.hasStore())
	AssertEqual(t, 0, len(state.RestoredDestinations()))
	destination := RequireMultiHopId(NewId())
	AssertEqual(t, true, state.TakeRestored(destination) == nil)
	state.Record(&WindowClientIdentity{
		ClientId:    NewId(),
		Destination: destination,
	})
	state.Remove(NewId())
}

type reorderedIdentityStore struct {
	calls atomic.Int32

	firstStarted chan struct{}
	releaseFirst chan struct{}

	mutex     sync.Mutex
	persisted []*WindowClientIdentity
}

func (self *reorderedIdentityStore) StoreWindowClientIdentities(identities []*WindowClientIdentity) {
	if self.calls.Add(1) == 1 {
		close(self.firstStarted)
		<-self.releaseFirst
	}
	self.mutex.Lock()
	self.persisted = slices.Clone(identities)
	self.mutex.Unlock()
}

func (self *reorderedIdentityStore) LoadWindowClientIdentities() []*WindowClientIdentity {
	return nil
}

// TestWindowIdentityStoresCannotCompleteOutOfOrder pins the ordering
// guarantee across the async writer: a newer Record can never be overwritten
// by an older snapshot. While an older store call is blocked in flight, newer
// mutations coalesce into the pending snapshot; once the blocked call
// returns, the writer stores the NEWEST snapshot — never re-storing an older
// generation over it. (Mutations themselves no longer block on the store —
// that is pinned separately by TestWindowIdentitySlowStoreDoesNotBlock.)
func TestWindowIdentityStoresCannotCompleteOutOfOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &reorderedIdentityStore{
		firstStarted: make(chan struct{}),
		releaseFirst: make(chan struct{}),
	}
	state := newWindowIdentityState(ctx, store)
	destination := RequireMultiHopId(NewId())
	identityA := &WindowClientIdentity{ClientId: NewId(), Destination: destination}
	identityB := &WindowClientIdentity{ClientId: NewId(), Destination: destination}

	state.Record(identityA)
	select {
	case <-store.firstStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("first snapshot did not reach store")
	}

	// the older {A} store is blocked in flight; the newer mutation coalesces
	// into the pending snapshot without starting a second concurrent store
	state.Record(identityB)
	time.Sleep(50 * time.Millisecond)
	if calls := store.calls.Load(); calls != 1 {
		t.Fatalf("store calls while the first is blocked = %d, want 1 (single writer)", calls)
	}

	close(store.releaseFirst)

	// the writer must follow up with the newer snapshot; the final persisted
	// state is the NEWEST ({A, B}) — the blocked older {A} write can never
	// end up overwriting it
	if !waitForCondition(5*time.Second, func() bool {
		store.mutex.Lock()
		defer store.mutex.Unlock()
		return len(store.persisted) == 2
	}) {
		store.mutex.Lock()
		persisted := slices.Clone(store.persisted)
		store.mutex.Unlock()
		t.Fatalf("final persisted snapshot has %d identities, want 2 (the newest snapshot must win)", len(persisted))
	}
}

// blockingIdentityStore blocks every StoreWindowClientIdentities call until
// `release` is closed, recording the snapshots it eventually persists.
type blockingIdentityStore struct {
	release chan struct{}

	calls atomic.Int32

	mutex     sync.Mutex
	persisted []*WindowClientIdentity
}

func (self *blockingIdentityStore) StoreWindowClientIdentities(identities []*WindowClientIdentity) {
	self.calls.Add(1)
	<-self.release
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.persisted = slices.Clone(identities)
}

func (self *blockingIdentityStore) LoadWindowClientIdentities() []*WindowClientIdentity {
	return nil
}

// TestWindowIdentitySlowStoreDoesNotBlock pins the no-lock-across-blocking
// fix: with the store wedged (as in a redis stall, which can retry ~60s),
// Record / Remove / TakeRestored / RestoredDestinations must all return
// promptly — a store stall must never wedge window expand/teardown.
func TestWindowIdentitySlowStoreDoesNotBlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store := &blockingIdentityStore{
		release: make(chan struct{}),
	}
	state := newWindowIdentityState(ctx, store)
	destination := RequireMultiHopId(NewId())
	identityA := &WindowClientIdentity{ClientId: NewId(), Destination: destination}
	identityB := &WindowClientIdentity{ClientId: NewId(), Destination: destination}

	// every mutation returns promptly while the store is wedged
	done := make(chan struct{})
	go func() {
		defer close(done)
		state.Record(identityA)
		state.Record(identityB)
		state.Remove(identityA.ClientId)
		state.TakeRestored(destination)
		state.RestoredDestinations()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a wedged store blocked Record/Remove/TakeRestored")
	}

	// release the store: the writer catches up to the newest snapshot ({B})
	close(store.release)
	if !waitForCondition(5*time.Second, func() bool {
		store.mutex.Lock()
		defer store.mutex.Unlock()
		return len(store.persisted) == 1 && store.persisted[0].ClientId == identityB.ClientId
	}) {
		t.Fatal("the writer did not persist the newest snapshot after the stall cleared")
	}
}
