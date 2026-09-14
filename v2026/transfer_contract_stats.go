package connect

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// contract stats surface per-contract usage from the send and receive sequences
// without per-packet dispatch. open contracts register an entry whose used byte
// count the owning sequence updates with an atomic store on debit/ack. an epoch
// worker (started on the first callback) snapshots the registry every
// `ContractStatsEpoch` and emits events only for contracts that changed.

type ContractStatsEvent struct {
	ContractId Id
	// true for a receive (ingress) contract, false for a send (egress) contract
	Receive bool
	// true when the send sequence opened this as a companion contract.
	// always false on the receive side (the wire contract does not carry it).
	// pair contracts to their companions with the peer client id on `Path`
	Companion bool
	// true when the contract is bound to an active stream (the platform
	// marks the stored contract with the stream id, `Path.StreamId`).
	// set on both the send and receive side — the receive side learns it
	// only from the contract, so this is its signal that the flow rides a
	// stream rather than a direct path
	Stream bool
	// source -> destination of the contract
	Path              TransferPath
	TransferByteCount ByteCount
	UsedByteCount     ByteCount
	// used bytes since the previous event for this contract
	UsedByteCountDelta ByteCount
	// false when the contract closed. the final event for a contract
	Open bool
	// Sequence is the per-contract emit order, starting at 1 and assigned
	// under the stats lock at snapshot time, so Sequence order == intended
	// order even when deliveries interleave (callbacks run outside the lock,
	// and events may reorder further across an rpc boundary). A consumer
	// must discard an event whose Sequence is <= the last seen for that
	// contract id — in particular a stale `Open=true` snapshot arriving
	// after the final close. The counter is dropped once the contract's
	// entries are fully closed and removed: contract ids are platform-minted
	// and single-use, and if a removed id were ever re-registered, the
	// restarted sequence would (correctly) be discarded by consumers as
	// stale — the contract is already closed from their point of view.
	Sequence uint64
}

type ContractStatsFunction func(contractStatsEvents []*ContractStatsEvent)

type contractStatsKey struct {
	contractId Id
	receive    bool
}

// contractStatsCallbackWorker isolates stats observers from contract-manager
// epochs and client cleanup. Stats are state observations, not transfer
// send/receive/forward backpressure. While a listener is blocked, retain the
// latest event per contract direction in a fixed ring; deltas are accumulated
// across coalesced updates so the next delivered rate sample remains useful.
type contractStatsCallbackWorker struct {
	ctx      context.Context
	cancel   context.CancelFunc
	callback ContractStatsFunction

	stateLock  sync.Mutex
	pending    map[contractStatsKey]*ContractStatsEvent
	order      []contractStatsKey
	orderHead  int
	orderCount int
	maxCount   int
	closed     bool
	notify     chan struct{}
}

func newContractStatsCallbackWorker(
	ctx context.Context,
	callback ContractStatsFunction,
	maxCount int,
) *contractStatsCallbackWorker {
	workerCtx, cancel := context.WithCancel(ctx)
	maxCount = max(1, maxCount)
	worker := &contractStatsCallbackWorker{
		ctx:      workerCtx,
		cancel:   cancel,
		callback: callback,
		pending:  map[contractStatsKey]*ContractStatsEvent{},
		order:    make([]contractStatsKey, maxCount),
		maxCount: maxCount,
		notify:   make(chan struct{}, 1),
	}
	go HandleError(worker.run, cancel)
	return worker
}

func (self *contractStatsCallbackWorker) Dispatch(events []*ContractStatsEvent) {
	if len(events) == 0 {
		return
	}

	self.stateLock.Lock()
	if self.closed {
		self.stateLock.Unlock()
		return
	}
	for _, event := range events {
		if event == nil {
			continue
		}
		key := contractStatsKey{contractId: event.ContractId, receive: event.Receive}
		cloned := *event
		if previous, ok := self.pending[key]; ok {
			// Absolute fields and Sequence come from the newest event; delta
			// spans every update the slow observer did not see.
			cloned.UsedByteCountDelta += previous.UsedByteCountDelta
			self.pending[key] = &cloned
			continue
		}

		if self.maxCount <= self.orderCount {
			evictedKey := self.order[self.orderHead]
			delete(self.pending, evictedKey)
			self.order[self.orderHead] = key
			self.orderHead = (self.orderHead + 1) % self.maxCount
		} else {
			tail := (self.orderHead + self.orderCount) % self.maxCount
			self.order[tail] = key
			self.orderCount += 1
		}
		self.pending[key] = &cloned
	}
	self.stateLock.Unlock()

	select {
	case <-self.ctx.Done():
	case self.notify <- struct{}{}:
	default:
	}
}

func (self *contractStatsCallbackWorker) run() {
	for {
		select {
		case <-self.ctx.Done():
			return
		case <-self.notify:
		}

		for {
			self.stateLock.Lock()
			if self.closed || self.orderCount == 0 {
				self.stateLock.Unlock()
				break
			}
			events := make([]*ContractStatsEvent, 0, self.orderCount)
			for 0 < self.orderCount {
				key := self.order[self.orderHead]
				self.order[self.orderHead] = contractStatsKey{}
				self.orderHead = (self.orderHead + 1) % self.maxCount
				self.orderCount -= 1
				if event := self.pending[key]; event != nil {
					events = append(events, event)
					delete(self.pending, key)
				}
			}
			self.stateLock.Unlock()

			HandleError(func() {
				self.callback(events)
			})
		}
	}
}

func (self *contractStatsCallbackWorker) Close() {
	self.stateLock.Lock()
	if self.closed {
		self.stateLock.Unlock()
		return
	}
	self.closed = true
	clear(self.pending)
	self.order = nil
	self.orderCount = 0
	self.stateLock.Unlock()
	self.cancel()
}

type contractStatsEntry struct {
	contractId        Id
	receive           bool
	companion         bool
	path              TransferPath
	transferByteCount ByteCount

	// stored by the owning sequence on debit/ack
	usedByteCount atomic.Int64
	closed        atomic.Bool

	// owned by the epoch worker
	emittedUsedByteCount ByteCount
	emitted              bool
}

func (self *contractStatsEntry) updateUsedByteCount(usedByteCount ByteCount) {
	self.usedByteCount.Store(int64(usedByteCount))
}

// register adds an entry for an open contract and returns it.
// the owning sequence stores the used byte count on the entry
func (self *ContractManager) registerContractStats(
	contractId Id,
	receive bool,
	companion bool,
	path TransferPath,
	transferByteCount ByteCount,
) *contractStatsEntry {
	entry := &contractStatsEntry{
		contractId:        contractId,
		receive:           receive,
		companion:         companion,
		path:              path,
		transferByteCount: transferByteCount,
	}

	self.contractStatsLock.Lock()
	defer self.contractStatsLock.Unlock()
	self.contractStatsEntries[contractStatsKey{contractId: contractId, receive: receive}] = entry
	return entry
}

// called from the contract close paths (`CloseContract`, `CheckpointContract`).
// the entry emits a final closed event and is removed by the epoch worker,
// or removed immediately when the worker never started
func (self *ContractManager) closeContractStats(contractId Id) {
	self.contractStatsLock.Lock()
	defer self.contractStatsLock.Unlock()
	for _, receive := range []bool{false, true} {
		key := contractStatsKey{contractId: contractId, receive: receive}
		if entry, ok := self.contractStatsEntries[key]; ok {
			if self.contractStatsStarted {
				entry.closed.Store(true)
			} else {
				delete(self.contractStatsEntries, key)
			}
		}
	}
}

// AddContractStatsCallback registers a listener for the epoch contract stats
// events. the epoch worker starts on the first callback
func (self *ContractManager) AddContractStatsCallback(contractStatsCallback ContractStatsFunction) func() {
	if self.beforeCallbackAdmissionLockForTest != nil {
		self.beforeCallbackAdmissionLockForTest()
	}
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.closed {
		return func() {}
	}
	func() {
		self.contractStatsLock.Lock()
		defer self.contractStatsLock.Unlock()
		if !self.contractStatsStarted {
			self.contractStatsStarted = true
			self.startWorker("contract stats", self.runContractStats)
		}
	}()
	callbackId := self.contractStatsCallbacks.Add(contractStatsCallback)
	return func() {
		self.contractStatsCallbacks.Remove(callbackId)
	}
}

func (self *ContractManager) runContractStats() {
	for {
		select {
		case <-self.ctx.Done():
			// backstop: emit any already-marked closes before exiting so they are
			// not lost on shutdown. The deterministic teardown path is
			// CloseAllContractStats (called before ctx cancel, while listeners are
			// still attached); this only covers other exit paths.
			self.emitContractStats()
			return
		case <-time.After(self.settings.ContractStatsEpoch):
		}
		self.emitContractStats()
	}
}

// emitContractStats does one emit pass: for each stats entry that is new,
// changed, or closed, emit a ContractStatsEvent to the current callbacks; closed
// entries are dropped after emitting. Called on each epoch tick, once more on
// worker exit (backstop), and synchronously by CloseAllContractStats.
func (self *ContractManager) emitContractStats() {
	callbacks := self.contractStatsCallbacks.Get()

	var events []*ContractStatsEvent
	func() {
		self.contractStatsLock.Lock()
		defer self.contractStatsLock.Unlock()
		for key, entry := range self.contractStatsEntries {
			usedByteCount := ByteCount(entry.usedByteCount.Load())
			closed := entry.closed.Load()
			if !entry.emitted || usedByteCount != entry.emittedUsedByteCount || closed {
				if 0 < len(callbacks) {
					// the per-contract sequence is assigned under the lock at
					// snapshot time, so Sequence order == snapshot order even
					// when the callbacks (outside the lock) interleave
					sequence := self.contractStatsSequences[entry.contractId] + 1
					self.contractStatsSequences[entry.contractId] = sequence
					events = append(events, &ContractStatsEvent{
						ContractId:         entry.contractId,
						Receive:            entry.receive,
						Companion:          entry.companion,
						Stream:             entry.path.IsStream(),
						Path:               entry.path,
						TransferByteCount:  entry.transferByteCount,
						UsedByteCount:      usedByteCount,
						UsedByteCountDelta: usedByteCount - entry.emittedUsedByteCount,
						Open:               !closed,
						Sequence:           sequence,
					})
				}
				entry.emitted = true
				entry.emittedUsedByteCount = usedByteCount
			}
			if closed {
				delete(self.contractStatsEntries, key)
				// drop the sequence counter once no entry remains for the
				// contract id (see the `Sequence` doc for why a re-register
				// after this point stays safe)
				siblingKey := contractStatsKey{contractId: key.contractId, receive: !key.receive}
				if _, ok := self.contractStatsEntries[siblingKey]; !ok {
					delete(self.contractStatsSequences, key.contractId)
				}
			}
		}
	}()

	if 0 < len(events) {
		for _, callback := range callbacks {
			HandleError(func() {
				callback(events)
			})
		}
	}
}

// CloseAllContractStats marks every open contract-stats entry closed and emits
// the closes synchronously to the currently-attached listeners. Call this at
// client teardown BEFORE the client ctx is cancelled and BEFORE stats listeners
// are removed, so a removed peer's contract-close events escape. Otherwise the
// epoch worker exits on ctx-done having emitted nothing (the sequence defers mark
// contracts closed asynchronously, into a dead worker and a removed listener),
// and the peer's contracts linger open in the contract-details UI.
//
// This closes only the stats (the UI/accounting view). The wire-level contract
// close to the platform is still done by the sequence teardown defers.
func (self *ContractManager) CloseAllContractStats() {
	func() {
		self.contractStatsLock.Lock()
		defer self.contractStatsLock.Unlock()
		for _, entry := range self.contractStatsEntries {
			entry.closed.Store(true)
		}
	}()
	self.emitContractStats()
}
