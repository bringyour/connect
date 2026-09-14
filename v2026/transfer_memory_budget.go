package connect

import (
	"sync"
	"sync/atomic"
)

// A byte budget shared by transfer queues across sequences — typically all
// clients of one device (the control client plus every window client) — so
// the aggregate queue memory stays flat as the number of peers grows, while
// a single fast peer can still borrow a deep queue.
//
// Queues reserve the bytes they hold above their guaranteed floor
// (`ResendQueueMinByteCount`/`ReceiveQueueMinByteCount`) and release them as
// items leave (see `transferQueue`). Admission gates on `Available` before a
// queue grows above its floor, so `Reserve` may transiently overdraft the
// total by up to one message per sequence past an admission that saw
// headroom. The floor keeps every sequence progressing when the pool is
// empty, which makes cross-sequence deadlock impossible.
//
// All methods are safe for concurrent use.
type TransferMemoryBudget struct {
	totalByteCount atomic.Int64
	usedByteCount  atomic.Int64
	// cumulative counters, so tests can assert reserve/release balance after
	// a build/load/teardown cycle (the message pool counts pattern)
	reservedByteCount atomic.Int64
	releasedByteCount atomic.Int64

	notify atomic.Pointer[transferMemoryBudgetNotify]

	// Admission waiters are intrusive and own reusable one-item channels.
	// Releasing one receive-window reservation wakes only the number of
	// eligible setup attempts that the resulting capacity can satisfy.
	capacityWaitStateLock sync.Mutex
	capacityWaitHead      *transferMemoryBudgetWaiter
	capacityWaitTail      *transferMemoryBudgetWaiter
	capacityWaiterCount   atomic.Int64

	// Peer-connection reservations are fixed-size lifetime owners rather than
	// packet queues. Their lazily allocated ownership state lets every
	// WebRtcManager sharing this budget see a teardown already in progress and
	// reclaim an obsolete owner across manager generations. A pointer keeps the
	// common queue-only budget at one machine word of overhead.
	peerConnectionAdmissionState atomic.Pointer[peerConnectionAdmissionState]
}

// transferMemoryBudgetNotify is one capacity-notification generation. An
// atomic swap gives one releaser exclusive ownership of closing the channel,
// while subscribers either join this generation or publish the next one.
type transferMemoryBudgetNotify struct {
	channel chan struct{}
}

// transferMemoryBudgetWaiter is one reusable admission subscription. The
// owning P2P transport calls subscribe and reset from its one lifecycle
// goroutine; list fields are protected by the budget's capacityWaitStateLock.
type transferMemoryBudgetWaiter struct {
	notify                 chan struct{}
	budget                 *TransferMemoryBudget
	requiredByteCount      ByteCount
	previousCapacityWaiter *transferMemoryBudgetWaiter
	nextCapacityWaiter     *transferMemoryBudgetWaiter
	registered             bool
}

func newTransferMemoryBudgetWaiter() *transferMemoryBudgetWaiter {
	return &transferMemoryBudgetWaiter{
		notify: make(chan struct{}, 1),
	}
}

// subscribe arms the waiter before its owner attempts admission, preventing a
// release between the failed reservation and the wait from being lost.
func (self *transferMemoryBudgetWaiter) subscribe(
	budget *TransferMemoryBudget,
	requiredByteCount ByteCount,
) <-chan struct{} {
	self.reset()
	if budget == nil {
		return nil
	}
	select {
	case <-self.notify:
	default:
	}
	self.budget = budget
	self.requiredByteCount = max(0, requiredByteCount)
	budget.addCapacityWaiter(self)
	return self.notify
}

// reset removes an armed subscription. A wake already assigned to this
// waiter is drained; the owner either consumed capacity or is about to
// re-subscribe immediately against current state.
func (self *transferMemoryBudgetWaiter) reset() {
	if self.budget != nil {
		self.budget.removeCapacityWaiter(self)
		self.budget = nil
	}
	select {
	case <-self.notify:
	default:
	}
}

func NewTransferMemoryBudget(totalByteCount ByteCount) *TransferMemoryBudget {
	budget := &TransferMemoryBudget{}
	budget.totalByteCount.Store(totalByteCount)
	return budget
}

func (self *TransferMemoryBudget) TotalByteCount() ByteCount {
	return self.totalByteCount.Load()
}

// SetTotalByteCount retunes the budget capacity live (e.g. reallocating the
// provider share to the client pair while providing is off). Shrinking does
// not evict reserved bytes; the pool admits nothing new above the new total
// until enough releases drain it.
func (self *TransferMemoryBudget) SetTotalByteCount(totalByteCount ByteCount) {
	self.totalByteCount.Store(totalByteCount)
	self.notifyCapacityChanged()
}

// Available is the unreserved remainder of the budget
func (self *TransferMemoryBudget) Available() ByteCount {
	return max(0, self.totalByteCount.Load()-self.usedByteCount.Load())
}

func (self *TransferMemoryBudget) UsedByteCount() ByteCount {
	return self.usedByteCount.Load()
}

// Reserve takes bytes from the budget. It always succeeds (see the overdraft
// note in the type doc); admission gates on `Available`.
func (self *TransferMemoryBudget) Reserve(byteCount ByteCount) {
	self.usedByteCount.Add(byteCount)
	self.reservedByteCount.Add(byteCount)
}

// TryReserve atomically reserves byteCount only when it fits within the
// current total. Transfer queues deliberately use Reserve's bounded overdraft
// semantics, but fixed-size lifetime owners (notably WebRTC peer connections)
// need an exact admission ceiling shared across multiple managers.
func (self *TransferMemoryBudget) TryReserve(byteCount ByteCount) bool {
	if byteCount < 0 {
		return false
	}
	for {
		total := self.totalByteCount.Load()
		used := self.usedByteCount.Load()
		if total < byteCount || total-byteCount < used {
			return false
		}
		if self.usedByteCount.CompareAndSwap(used, used+byteCount) {
			self.reservedByteCount.Add(byteCount)
			return true
		}
	}
}

// Release returns bytes to the budget
func (self *TransferMemoryBudget) Release(byteCount ByteCount) {
	used := self.usedByteCount.Add(-byteCount)
	self.releasedByteCount.Add(byteCount)
	if used < 0 {
		// accounting bug: more released than reserved.
		// log unconditionally so production sees it (tests see it as a
		// negative used count breaking the balance assertions)
		DefaultLogger().Errorf("[tmb]release below zero (%d)", used)
	}
	self.notifyCapacityChanged()
}

// CapacityNotify returns a channel closed the next time a release or resize
// may make admission possible. Capture it before TryReserve to avoid a lost
// wakeup.
func (self *TransferMemoryBudget) CapacityNotify() <-chan struct{} {
	var candidate *transferMemoryBudgetNotify
	for {
		if notify := self.notify.Load(); notify != nil {
			return notify.channel
		}
		if candidate == nil {
			candidate = &transferMemoryBudgetNotify{
				channel: make(chan struct{}),
			}
		}
		if self.notify.CompareAndSwap(nil, candidate) {
			return candidate.channel
		}
	}
}

func (self *TransferMemoryBudget) notifyCapacityChanged() {
	// Releases are on the packet path and normally have no waiter. Avoid a
	// read-modify-write on the shared notification cache line in that case.
	// A subscriber that publishes just after this load performs TryReserve
	// afterward, so it observes this release as available capacity; if another
	// reserver wins first, that reserver's eventual release closes the channel.
	if self.notify.Load() != nil {
		if notify := self.notify.Swap(nil); notify != nil {
			close(notify.channel)
		}
	}
	self.notifyEligibleCapacityWaiters()
}

func (self *TransferMemoryBudget) addCapacityWaiter(
	waiter *transferMemoryBudgetWaiter,
) {
	self.capacityWaitStateLock.Lock()
	defer self.capacityWaitStateLock.Unlock()
	if waiter.registered {
		return
	}
	waiter.previousCapacityWaiter = self.capacityWaitTail
	waiter.nextCapacityWaiter = nil
	if self.capacityWaitTail == nil {
		self.capacityWaitHead = waiter
	} else {
		self.capacityWaitTail.nextCapacityWaiter = waiter
	}
	self.capacityWaitTail = waiter
	waiter.registered = true
	self.capacityWaiterCount.Add(1)
}

func (self *TransferMemoryBudget) removeCapacityWaiter(
	waiter *transferMemoryBudgetWaiter,
) {
	self.capacityWaitStateLock.Lock()
	defer self.capacityWaitStateLock.Unlock()
	self.removeCapacityWaiterWithLock(waiter)
}

func (self *TransferMemoryBudget) removeCapacityWaiterWithLock(
	waiter *transferMemoryBudgetWaiter,
) {
	if !waiter.registered {
		return
	}
	if waiter.previousCapacityWaiter == nil {
		self.capacityWaitHead = waiter.nextCapacityWaiter
	} else {
		waiter.previousCapacityWaiter.nextCapacityWaiter =
			waiter.nextCapacityWaiter
	}
	if waiter.nextCapacityWaiter == nil {
		self.capacityWaitTail = waiter.previousCapacityWaiter
	} else {
		waiter.nextCapacityWaiter.previousCapacityWaiter =
			waiter.previousCapacityWaiter
	}
	waiter.previousCapacityWaiter = nil
	waiter.nextCapacityWaiter = nil
	waiter.registered = false
	self.capacityWaiterCount.Add(-1)
}

// notifyEligibleCapacityWaiters performs a bounded FIFO admission grant. It
// scans past a large ineligible request so a smaller request cannot starve,
// while subtracting each grant from a capacity snapshot prevents a release
// for one receive window from waking every speculative P2P setup.
func (self *TransferMemoryBudget) notifyEligibleCapacityWaiters() {
	if self.capacityWaiterCount.Load() == 0 {
		return
	}
	availableByteCount := self.Available()
	if availableByteCount <= 0 {
		return
	}

	self.capacityWaitStateLock.Lock()
	defer self.capacityWaitStateLock.Unlock()
	for waiter := self.capacityWaitHead; waiter != nil; {
		nextWaiter := waiter.nextCapacityWaiter
		if waiter.requiredByteCount <= availableByteCount {
			self.removeCapacityWaiterWithLock(waiter)
			select {
			case waiter.notify <- struct{}{}:
				availableByteCount -= waiter.requiredByteCount
			default:
				// A pending token means this waiter already has a grant. Do
				// not charge it twice; continue looking for another waiter.
			}
		}
		waiter = nextWaiter
	}
}

// Counts returns the cumulative reserved/released byte counts.
// reserved-released equals the currently used bytes: it returns to zero when
// every borrowing queue is drained or cleared, so growth across a
// build/load/teardown cycle attributes a lost release.
func (self *TransferMemoryBudget) Counts() (reservedByteCount ByteCount, releasedByteCount ByteCount) {
	return self.reservedByteCount.Load(), self.releasedByteCount.Load()
}
