package connect

import (
	"context"
	"sync"
)

// Window identity persistence (PROXYDRAIN1.md §3.5).
//
// The api generator mints an ephemeral platform client id (with a fresh
// instance id) for every window entry and removes it on teardown. A process
// restart therefore changes every window client id — and the egress
// provider's NAT flows are keyed by source client id, so every established
// inner flow orphans even though the NAT state itself survives (it evicts
// lazily: udp 60s idle, tcp 300s).
//
// A `MultiClientIdentityStore` closes that gap: the generator records each
// live (window client identity, destination) pair as it forms, and a
// restarted process restores the pairs — reusing the SAME client id,
// jwt, and instance id against the SAME destination, so the provider's
// flows resume instead of orphaning. The store is provided by the embedding
// host (e.g. the proxy service persists per hosted device in redis with a
// short ttl); with no store set, behavior is exactly as before.

// WindowClientIdentity pairs a window client identity with the destination
// it serves.
type WindowClientIdentity struct {
	ClientId   Id
	ByJwt      string
	InstanceId Id
	// Destination is the provider destination this identity dials.
	Destination MultiHopId
}

// MultiClientIdentityStore persists the live window identities across a
// process restart. Implementations must tolerate concurrent calls.
//
// Store semantics: called with the FULL live set on every change (small,
// bounded by the window size), replacing the previous snapshot. Load is
// read once, at the first window expansion after construction.
type MultiClientIdentityStore interface {
	StoreWindowClientIdentities(identities []*WindowClientIdentity)
	LoadWindowClientIdentities() []*WindowClientIdentity
}

// MultiClientIdentityStoreContext is the cancellation-aware load capability.
// Remote stores should implement it so abandoning optional restoration also
// terminates the underlying Redis/database request. Legacy stores remain
// supported and are isolated to one load worker per identity state.
type MultiClientIdentityStoreContext interface {
	LoadWindowClientIdentitiesContext(ctx context.Context) []*WindowClientIdentity
}

// MultiClientGeneratorWithDestination is an optional generator extension:
// mint client args bound to a destination, so a persisted identity for that
// destination can be reused. The window expand path prefers this over
// `NewClientArgs` when implemented.
type MultiClientGeneratorWithDestination interface {
	NewClientArgsForDestination(destination MultiHopId) (*MultiClientGeneratorClientArgs, error)
}

// MultiClientGeneratorWithDestinationContext is the maintenance-bounded form
// of MultiClientGeneratorWithDestination. Implementations must return when ctx
// is done. The API generator uses it so a stalled auth call cannot park the
// window's only candidate producer.
type MultiClientGeneratorWithDestinationContext interface {
	NewClientArgsForDestinationContext(ctx context.Context, destination MultiHopId) (*MultiClientGeneratorClientArgs, error)
}

// A normal quality+speed window retains at most 16 clients with the default
// hard maxima. Allow several generations of slack for an imperfect store
// snapshot, but never retain an unbounded remote result.
const maxRestoredWindowIdentityCount = 64

// windowIdentityState is the generator-side bookkeeping for identity
// persistence: restored identities pending reuse, and the live pairs backing
// the store snapshot. Safe for concurrent use.
//
// Store writes are ASYNC: a mutation assigns a monotonic generation and
// captures the snapshot under `mutex`, then hands it to a single writer
// goroutine that calls the (possibly slow — the proxy adapter's redis store
// retries up to ~60s) store OUTSIDE the mutex. `mutex` is therefore never
// held across a blocking call, so a store stall cannot wedge window
// expand/teardown (`Record`/`Remove`/`TakeRestored`). The writer always
// drains the NEWEST pending snapshot and skips any generation at or below
// the last written one, so a newer snapshot can never be overwritten by an
// older one (the ordering guarantee `TestWindowIdentityStoresCannotComplete-
// OutOfOrder` pins). Bounded: one goroutine, one pending snapshot slot, a
// 1-slot coalescing notify; the writer exits with ctx after a best-effort
// final drain.
type windowIdentityState struct {
	ctx   context.Context
	store MultiClientIdentityStore
	// storeDone closes after the single persistence writer's final drain. A
	// nil store has no writer and starts terminal.
	storeDone chan struct{}

	mutex sync.Mutex
	// Loading is one asynchronous single-flight operation. The store may be a
	// remote Redis adapter and can stall for minutes; it must never run under
	// mutex or on either window's sole enumeration goroutine. Callers wait with
	// their own deadline. The first deadline abandons continuity restoration
	// (the late result is discarded) so fresh provider discovery can proceed.
	loadStarted   bool
	loadFinished  bool
	loadAbandoned bool
	loadErr       error
	loadDone      chan struct{}
	loadCtx       context.Context
	cancelLoad    context.CancelFunc
	// restored identities pending reuse, by destination. A destination can
	// carry SEVERAL identities (the window may run more than one client to
	// the same destination, e.g. a racy initial double-expand), so each
	// take pops one. Consumed at most once: a failed reuse falls back to
	// minting, never to a second restore.
	restored map[MultiHopId][]*WindowClientIdentity
	// the live pairs, by client id, mirrored to the store on every change
	live map[Id]*WindowClientIdentity

	// the monotonic snapshot generation, advanced under `mutex` per mutation
	generation uint64
	// the newest snapshot awaiting the writer (nil when none pending),
	// with its generation. an unwritten older pending snapshot is simply
	// replaced — only the newest state needs to persist.
	pendingGeneration uint64
	pendingSnapshot   []*WindowClientIdentity

	// writeNotify wakes the writer goroutine; capacity 1 coalesces bursts
	writeNotify chan struct{}
}

func newWindowIdentityState(ctx context.Context, store MultiClientIdentityStore) *windowIdentityState {
	loadCtx, cancelLoad := context.WithCancel(ctx)
	state := &windowIdentityState{
		ctx:         ctx,
		store:       store,
		storeDone:   make(chan struct{}),
		restored:    map[MultiHopId][]*WindowClientIdentity{},
		live:        map[Id]*WindowClientIdentity{},
		loadDone:    make(chan struct{}),
		loadCtx:     loadCtx,
		cancelLoad:  cancelLoad,
		writeNotify: make(chan struct{}, 1),
	}
	if store != nil {
		go HandleError(state.runStoreWriter)
		// Start the optional continuity read as soon as the store is attached.
		// In the common case it completes before the first enumeration, taking
		// the remote-store round trip off the formation critical path. The
		// external call still runs outside mutex in one failure-isolated worker.
		state.mutex.Lock()
		state.startLoadWithLock()
		state.mutex.Unlock()
	} else {
		close(state.storeDone)
	}
	return state
}

// CloseAndWait joins the persistence writer after its owning context has been
// canceled. Context-aware loads receive that same cancellation separately;
// a legacy store's unbounded Load remains isolated to its documented single
// worker and cannot be joined by an interface that has no cancellation seam.
func (self *windowIdentityState) CloseAndWait(ctx context.Context) error {
	self.cancelLoad()
	select {
	case <-self.storeDone:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// hasStore reports whether an identity store is configured (the proxy case:
// identities must survive teardown for the replacement container).
func (self *windowIdentityState) hasStore() bool {
	return self.store != nil
}

// startLoadWithLock starts the one store load without holding mutex across the
// external call. The caller holds mutex.
func (self *windowIdentityState) startLoadWithLock() {
	if self.loadStarted {
		return
	}
	self.loadStarted = true
	if self.store == nil {
		self.loadFinished = true
		close(self.loadDone)
		return
	}
	go self.load()
}

func (self *windowIdentityState) load() {
	var identities []*WindowClientIdentity
	var loadErr error
	HandleError(func() {
		if store, ok := self.store.(MultiClientIdentityStoreContext); ok {
			identities = store.LoadWindowClientIdentitiesContext(self.loadCtx)
		} else {
			identities = self.store.LoadWindowClientIdentities()
		}
	}, func(err error) {
		loadErr = err
	})
	self.cancelLoad()

	self.mutex.Lock()
	defer self.mutex.Unlock()
	if !self.loadAbandoned && loadErr == nil {
		restoredCount := 0
		for _, identity := range identities {
			if identity == nil {
				continue
			}
			if maxRestoredWindowIdentityCount <= restoredCount {
				break
			}
			self.restored[identity.Destination] = append(self.restored[identity.Destination], identity)
			restoredCount += 1
		}
	}
	self.loadErr = loadErr
	self.loadFinished = true
	close(self.loadDone)
}

func (self *windowIdentityState) waitForLoad(ctx context.Context) error {
	self.mutex.Lock()
	self.startLoadWithLock()
	if self.loadAbandoned {
		self.mutex.Unlock()
		return nil
	}
	if self.loadFinished {
		err := self.loadErr
		self.mutex.Unlock()
		return err
	}
	loadDone := self.loadDone
	self.mutex.Unlock()

	select {
	case <-loadDone:
		self.mutex.Lock()
		err := self.loadErr
		self.mutex.Unlock()
		return err
	case <-ctx.Done():
		self.mutex.Lock()
		// Resolve a completion/deadline race in favor of a completed load.
		if self.loadFinished {
			err := self.loadErr
			self.mutex.Unlock()
			return err
		}
		self.loadAbandoned = true
		self.mutex.Unlock()
		// A context-aware remote store exits its underlying request. A legacy
		// store may remain parked, but still owns only this one bounded worker.
		self.cancelLoad()
		return ctx.Err()
	}
}

// RestoredDestinations returns the destinations with an identity pending
// reuse, so the window expand can dial them first.
func (self *windowIdentityState) RestoredDestinations() []MultiHopId {
	destinations, _ := self.RestoredDestinationsContext(self.ctx)
	return destinations
}

func (self *windowIdentityState) RestoredDestinationsContext(ctx context.Context) ([]MultiHopId, error) {
	if err := self.waitForLoad(ctx); err != nil {
		return nil, err
	}
	self.mutex.Lock()
	defer self.mutex.Unlock()

	destinations := make([]MultiHopId, 0, len(self.restored))
	for destination := range self.restored {
		destinations = append(destinations, destination)
	}
	return destinations, nil
}

// TakeRestored consumes one restored identity for a destination, if any.
func (self *windowIdentityState) TakeRestored(destination MultiHopId) *WindowClientIdentity {
	identity, _ := self.TakeRestoredContext(self.ctx, destination)
	return identity
}

func (self *windowIdentityState) TakeRestoredContext(ctx context.Context, destination MultiHopId) (*WindowClientIdentity, error) {
	if err := self.waitForLoad(ctx); err != nil {
		return nil, err
	}
	self.mutex.Lock()
	defer self.mutex.Unlock()

	identities, ok := self.restored[destination]
	if !ok || len(identities) == 0 {
		return nil, nil
	}
	identity := identities[0]
	if len(identities) == 1 {
		delete(self.restored, destination)
	} else {
		self.restored[destination] = identities[1:]
	}
	return identity, nil
}

// Record adds a live (identity, destination) pair and mirrors the snapshot
// to the store.
func (self *windowIdentityState) Record(identity *WindowClientIdentity) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.live[identity.ClientId] = identity
	self.storeSnapshotWithLock()
}

// Remove drops the live pair for a client id and mirrors the snapshot to the
// store. It is the unconditional bookkeeping form used by tests/callers that
// do not hold a generation token; provider lifecycle teardown should use
// RemoveIfCurrent.
func (self *windowIdentityState) Remove(clientId Id) {
	self.RemoveIfCurrent(clientId, Id{})
}

// RemoveIfCurrent removes clientId only when instanceId still owns that slot.
// A channel can be replaced under the same client id while the retired
// channel's asynchronous cleanup is still pending. In that case the instance
// id is the generation token: returning false tells the caller it must not
// erase the replacement's persisted identity or remove the live server client.
//
// A zero instance id is the unconditional compatibility form. A missing entry
// returns true because there is no newer in-process generation to protect.
func (self *windowIdentityState) RemoveIfCurrent(clientId Id, instanceId Id) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	identity, ok := self.live[clientId]
	if !ok {
		return true
	}
	if instanceId != (Id{}) && identity.InstanceId != instanceId {
		return false
	}
	delete(self.live, clientId)
	self.storeSnapshotWithLock()
	return true
}

func (self *windowIdentityState) snapshotWithLock() []*WindowClientIdentity {
	identities := make([]*WindowClientIdentity, 0, len(self.live))
	for _, identity := range self.live {
		identities = append(identities, identity)
	}
	return identities
}

// storeSnapshotWithLock assigns the next generation, captures the snapshot,
// and wakes the async writer. It never blocks: the store call itself happens
// on the writer goroutine, outside mutex. The caller holds mutex.
func (self *windowIdentityState) storeSnapshotWithLock() {
	if self.store == nil {
		return
	}
	self.generation += 1
	self.pendingGeneration = self.generation
	self.pendingSnapshot = self.snapshotWithLock()
	select {
	case self.writeNotify <- struct{}{}:
	default:
		// a wake is already queued; the writer drains the newest pending
	}
}

// runStoreWriter is the single async store writer: it drains the newest
// pending snapshot and stores it outside `mutex`. It exits with ctx, after a
// best-effort final drain so a snapshot scheduled just before shutdown still
// reaches the store.
func (self *windowIdentityState) runStoreWriter() {
	defer close(self.storeDone)
	lastWrittenGeneration := uint64(0)
	for {
		select {
		case <-self.ctx.Done():
			self.drainPendingStores(&lastWrittenGeneration)
			return
		case <-self.writeNotify:
			self.drainPendingStores(&lastWrittenGeneration)
		}
	}
}

// drainPendingStores repeatedly takes the newest pending (generation,
// snapshot) under `mutex` and stores it outside the lock, until nothing is
// pending. A snapshot whose generation is <= the last successfully written
// generation is superseded and dropped, preserving the ordering guarantee: a
// newer Record/Remove can never be overwritten by an older one.
func (self *windowIdentityState) drainPendingStores(lastWrittenGeneration *uint64) {
	for {
		var generation uint64
		var snapshot []*WindowClientIdentity
		func() {
			self.mutex.Lock()
			defer self.mutex.Unlock()
			if self.pendingSnapshot == nil {
				return
			}
			generation = self.pendingGeneration
			snapshot = self.pendingSnapshot
			self.pendingSnapshot = nil
		}()
		if snapshot == nil {
			return
		}
		if generation <= *lastWrittenGeneration {
			// superseded: a newer snapshot has already been written
			continue
		}
		// the store call runs outside the mutex — it may block/retry for a
		// long time, and mutations meanwhile coalesce into pendingSnapshot
		self.store.StoreWindowClientIdentities(snapshot)
		*lastWrittenGeneration = generation
	}
}
