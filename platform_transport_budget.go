package connect

import (
	"context"
	"slices"
	"sync"
)

// PlatformTransportBudget bounds the aggregate retained capacity of platform
// carriers and the number of PlatformTransports allowed to open sockets. H1
// claims register before transport goroutines start and precede optional Auto
// H3 when both do not fit. Optional H3 leases are revocable: an explicit H3
// choice can reclaim one, and foreground client Auto H3 can reclaim a provider
// lease, so construction order cannot permanently pin outbound traffic to H1.
// A policy replacement with H1 on either side may also use one serialized
// temporary handoff bounded to the H1 claim, preserving make-before-break
// without allowing two full H3 working sets to escape the aggregate limit.
type PlatformTransportBudget struct {
	mutex sync.Mutex

	totalByteCount     ByteCount
	usedByteCount      ByteCount
	maxTransportCount  int
	usedTransportCount int
	pendingH1ByteCount ByteCount
	pendingH1SlotCount int
	notify             chan struct{}
	reservedByteCount  ByteCount
	releasedByteCount  ByteCount
	preemptedH3Count   uint64
	// activeHandoff is the only reservation allowed to exceed the ordinary
	// byte/socket ceiling while its paired carrier remains live. A handoff is
	// allowed only when one endpoint is H1, so the temporary overage is bounded
	// by the H1 claim: old H1 while moving to H3, or new H1 while moving from H3.
	// More than one replacement may wait, but admission serializes the overage.
	activeHandoff           *platformTransportBudgetReservation
	handoffAcquisitionCount uint64
	reservations            map[*platformTransportBudgetReservation]bool
	nextSequence            uint64
}

type platformTransportBudgetClass uint8

const (
	platformTransportBudgetH1 platformTransportBudgetClass = iota + 1
	platformTransportBudgetH3Auto
	platformTransportBudgetH3Explicit

	// Keep the old internal name for tests and callers that mean optional H3.
	platformTransportBudgetH3 = platformTransportBudgetH3Auto
)

const (
	// Foreground is the default for outbound client windows. A larger value is
	// lower priority and may yield an optional Auto-H3 lease to a foreground
	// claimant when both cannot fit.
	PlatformTransportBudgetPriorityForeground = 0
	PlatformTransportBudgetPriorityBackground = 1
)

type platformTransportBudgetReservation struct {
	budget    *PlatformTransportBudget
	class     platformTransportBudgetClass
	byteCount ByteCount
	usesSlot  bool
	priority  int
	sequence  uint64

	// Lifecycle and preemption state are guarded by budget.mutex. Keeping one
	// lock lets admission inspect and revoke another reservation without a
	// reservation-to-budget / budget-to-reservation lock inversion.
	pending          bool
	acquired         bool
	closed           bool
	preempt          chan struct{}
	preemptRequested bool
	// A policy replacement may pair with the claim it will replace when one side
	// is H1. The pair remains linked after admission until either transport
	// releases; that is the accounting proof that the temporary overage is
	// bounded by the H1 reservation rather than a second full H3 working set.
	handoffFrom *platformTransportBudgetReservation
	handoffTo   *platformTransportBudgetReservation
}

type PlatformTransportBudgetStats struct {
	TotalByteCount              ByteCount
	UsedByteCount               ByteCount
	MaxTransportCount           int
	UsedTransportCount          int
	PendingH1ByteCount          ByteCount
	PendingH1Count              int
	ReservedByteCount           ByteCount
	ReleasedByteCount           ByteCount
	PreemptedH3Count            uint64
	PendingHandoffCount         int
	ActiveHandoffCount          int
	ActiveHandoffByteCount      ByteCount
	ActiveHandoffTransportCount int
	HandoffAcquisitionCount     uint64
}

// NewPlatformTransportBudget creates a byte budget with an optional aggregate
// PlatformTransport count cap. maxTransportCount <= 0 disables the count cap.
func NewPlatformTransportBudget(
	totalByteCount ByteCount,
	maxTransportCount int,
) *PlatformTransportBudget {
	return &PlatformTransportBudget{
		totalByteCount:    max(0, totalByteCount),
		maxTransportCount: max(0, maxTransportCount),
		notify:            make(chan struct{}),
		reservations:      map[*platformTransportBudgetReservation]bool{},
	}
}

func (self *PlatformTransportBudget) Stats() PlatformTransportBudgetStats {
	if self == nil {
		return PlatformTransportBudgetStats{}
	}
	self.mutex.Lock()
	defer self.mutex.Unlock()
	stats := PlatformTransportBudgetStats{
		TotalByteCount:          self.totalByteCount,
		UsedByteCount:           self.usedByteCount,
		MaxTransportCount:       self.maxTransportCount,
		UsedTransportCount:      self.usedTransportCount,
		PendingH1ByteCount:      self.pendingH1ByteCount,
		PendingH1Count:          self.pendingH1SlotCount,
		ReservedByteCount:       self.reservedByteCount,
		ReleasedByteCount:       self.releasedByteCount,
		PreemptedH3Count:        self.preemptedH3Count,
		HandoffAcquisitionCount: self.handoffAcquisitionCount,
	}
	for reservation := range self.reservations {
		if reservation.handoffFrom != nil && reservation.pending && !reservation.closed {
			stats.PendingHandoffCount += 1
		}
	}
	if active := self.activeHandoff; active != nil && active.handoffFrom != nil {
		stats.ActiveHandoffCount = 1
		stats.ActiveHandoffByteCount = min(active.byteCount, active.handoffFrom.byteCount)
		if active.usesSlot && active.handoffFrom.usesSlot {
			stats.ActiveHandoffTransportCount = 1
		}
	}
	return stats
}

func (self *PlatformTransportBudget) notifyChangedLocked() {
	close(self.notify)
	self.notify = make(chan struct{})
}

func (self *PlatformTransportBudget) register(
	class platformTransportBudgetClass,
	byteCount ByteCount,
	usesSlot bool,
) *platformTransportBudgetReservation {
	return self.registerWithPriority(
		class,
		byteCount,
		usesSlot,
		PlatformTransportBudgetPriorityForeground,
	)
}

func (self *PlatformTransportBudget) registerWithPriority(
	class platformTransportBudgetClass,
	byteCount ByteCount,
	usesSlot bool,
	priority int,
) *platformTransportBudgetReservation {
	if self == nil {
		return nil
	}
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.nextSequence += 1
	reservation := &platformTransportBudgetReservation{
		budget:    self,
		class:     class,
		byteCount: max(0, byteCount),
		usesSlot:  usesSlot,
		priority:  priority,
		sequence:  self.nextSequence,
		pending:   true,
		preempt:   make(chan struct{}),
	}
	self.reservations[reservation] = true
	if class == platformTransportBudgetH1 {
		self.pendingH1ByteCount += reservation.byteCount
		if usesSlot {
			self.pendingH1SlotCount += 1
		}
	}
	self.notifyChangedLocked()
	return reservation
}

func (self *platformTransportBudgetReservation) higherPriorityPendingH3Locked() *platformTransportBudgetReservation {
	var best *platformTransportBudgetReservation
	for candidate := range self.budget.reservations {
		if candidate == self || candidate.closed || candidate.acquired || !candidate.pending {
			continue
		}
		precedes := candidate.class == platformTransportBudgetH3Explicit &&
			self.class != platformTransportBudgetH3Explicit
		if candidate.class == platformTransportBudgetH3Auto &&
			self.class == platformTransportBudgetH3Auto &&
			candidate.priority < self.priority {
			precedes = true
		}
		if !precedes {
			continue
		}
		if best == nil || candidate.sequence < best.sequence {
			best = candidate
		}
	}
	return best
}

// pendingH1CapacityLocked returns only the pending H1 capacity that could fit
// alongside the already-counted transports. A slot-using H1 claim beyond the
// aggregate socket cap cannot acquire until one of those transports leaves, so
// reserving its bytes and slot against a slotless Auto-H3 claim creates a false
// dependency: H1 -> Auto policy migration fills the cap with the old and new
// H1 carriers, then H3 waits for a pending H1 that is itself unable to start.
//
// H1 still has precedence. Claims that can fit are reserved here, and any H1
// registered after Auto H3 acquires can revoke that optional H3 lease. When
// claim sizes differ, reserving the largest structurally admissible claims is
// conservative regardless of which Acquire goroutine wins the wake-up race.
func (self *platformTransportBudgetReservation) pendingH1CapacityLocked(
	baseTransportCount int,
) (byteCount ByteCount, transportCount int) {
	budget := self.budget
	if budget.maxTransportCount <= 0 {
		return budget.pendingH1ByteCount, budget.pendingH1SlotCount
	}

	availableSlots := max(0, budget.maxTransportCount-baseTransportCount)
	slotByteCounts := []ByteCount{}
	for candidate := range budget.reservations {
		if candidate.closed || candidate.acquired || !candidate.pending ||
			candidate.class != platformTransportBudgetH1 {
			continue
		}
		if !candidate.usesSlot {
			byteCount += candidate.byteCount
			continue
		}
		slotByteCounts = append(slotByteCounts, candidate.byteCount)
	}
	slices.SortFunc(slotByteCounts, func(a ByteCount, b ByteCount) int {
		if b < a {
			return -1
		}
		if a < b {
			return 1
		}
		return 0
	})
	transportCount = min(availableSlots, len(slotByteCounts))
	for _, pendingByteCount := range slotByteCounts[:transportCount] {
		byteCount += pendingByteCount
	}
	return
}

func (self *platformTransportBudgetReservation) requiredCapacityLocked() (
	byteCount ByteCount,
	transportCount int,
) {
	budget := self.budget
	byteCount = budget.usedByteCount + self.byteCount
	transportCount = budget.usedTransportCount
	if self.usesSlot {
		transportCount += 1
	}
	if self.class == platformTransportBudgetH3Auto {
		// H1 claims are registered at construction, before any carrier goroutine
		// runs. Preserve the pending claims that can structurally fit alongside
		// this claim, so required H1 remains ahead of optional Auto H3 without
		// letting claims beyond the socket cap deadlock a slotless H3 migration.
		pendingH1ByteCount, pendingH1TransportCount :=
			self.pendingH1CapacityLocked(transportCount)
		byteCount += pendingH1ByteCount
		transportCount += pendingH1TransportCount
	}
	// Preserve one higher-priority H3 claimant. Reserving one, rather than the
	// sum of every window's identical claim, guarantees progress without
	// deadlocking the budget when only one H3 carrier can fit.
	if higher := self.higherPriorityPendingH3Locked(); higher != nil {
		byteCount += higher.byteCount
		if higher.usesSlot {
			transportCount += 1
		}
	}
	return
}

func (self *platformTransportBudgetReservation) capacityFitsLocked(
	byteCount ByteCount,
	transportCount int,
) bool {
	budget := self.budget
	if budget.totalByteCount < byteCount {
		return false
	}
	if 0 < budget.maxTransportCount && budget.maxTransportCount < transportCount {
		return false
	}
	return true
}

// handoffCapacityLocked returns the capacity required when this claim replaces
// its paired, already-acquired claim. One endpoint must be H1. Accounting
// continues to include the old carrier until it actually drains; only the
// admission decision discounts it, and activeHandoff ensures no second claim
// can do the same at the same time.
func (self *platformTransportBudgetReservation) handoffCapacityLocked() (
	byteCount ByteCount,
	transportCount int,
	ok bool,
) {
	previous := self.handoffFrom
	budget := self.budget
	if previous == nil || previous.budget != budget || previous.closed ||
		!previous.acquired ||
		self.closed || self.acquired || !self.pending ||
		(self.class != platformTransportBudgetH1 &&
			previous.class != platformTransportBudgetH1) ||
		(budget.activeHandoff != nil && budget.activeHandoff != self) {
		return 0, 0, false
	}

	byteCount, transportCount = self.requiredCapacityLocked()
	byteCount -= previous.byteCount
	if previous.usesSlot {
		transportCount -= 1
	}
	return byteCount, transportCount, true
}

func (self *platformTransportBudgetReservation) admissionLocked() (
	canAcquire bool,
	usesHandoff bool,
) {
	byteCount, transportCount := self.requiredCapacityLocked()
	if self.capacityFitsLocked(byteCount, transportCount) {
		return true, false
	}
	byteCount, transportCount, ok := self.handoffCapacityLocked()
	return ok && self.capacityFitsLocked(byteCount, transportCount), ok
}

func (self *platformTransportBudgetReservation) canAcquireLocked() bool {
	canAcquire, _ := self.admissionLocked()
	return canAcquire
}

// AllowHandoffFrom pairs two replacement reservations when at least one is H1.
// Multiple pairs may wait, but admission uses at most one pair at a time. This
// method never releases the old reservation; the migration owner closes it
// only after the replacement has authenticated and published its routes.
func (self *platformTransportBudgetReservation) AllowHandoffFrom(
	previous *platformTransportBudgetReservation,
) bool {
	if self == nil || previous == nil || self.budget == nil ||
		self.budget != previous.budget {
		return false
	}
	budget := self.budget
	budget.mutex.Lock()
	defer budget.mutex.Unlock()

	if self.closed || self.acquired || !self.pending ||
		previous.closed || !previous.acquired ||
		(self.class != platformTransportBudgetH1 &&
			previous.class != platformTransportBudgetH1) {
		return false
	}
	if self.handoffFrom != nil && self.handoffFrom != previous {
		return false
	}
	if previous.handoffTo != nil && previous.handoffTo != self {
		return false
	}
	self.handoffFrom = previous
	previous.handoffTo = self
	budget.notifyChangedLocked()
	return true
}

func (self *platformTransportBudgetReservation) clearHandoffLocked() {
	budget := self.budget
	if previous := self.handoffFrom; previous != nil {
		if previous.handoffTo == self {
			previous.handoffTo = nil
		}
		self.handoffFrom = nil
	}
	if budget.activeHandoff == self {
		budget.activeHandoff = nil
	}
}

func (self *platformTransportBudgetReservation) clearHandoffToLocked() {
	budget := self.budget
	if replacement := self.handoffTo; replacement != nil {
		if replacement.handoffFrom == self {
			replacement.handoffFrom = nil
		}
		if budget.activeHandoff == replacement {
			budget.activeHandoff = nil
		}
		self.handoffTo = nil
	}
}

func (self *platformTransportBudgetReservation) canPreemptLocked(
	victim *platformTransportBudgetReservation,
) bool {
	if victim == self || victim.closed || !victim.acquired ||
		victim.class != platformTransportBudgetH3Auto || victim.preemptRequested {
		return false
	}
	switch self.class {
	case platformTransportBudgetH1, platformTransportBudgetH3Explicit:
		return true
	case platformTransportBudgetH3Auto:
		return self.priority < victim.priority
	default:
		return false
	}
}

// requestPreemptionLocked revokes only as many lower-precedence optional H3
// leases as can satisfy this claim's current byte/slot deficit. The lease
// owner tears down its H3 sockets before yielding the accounting reservation.
func (self *platformTransportBudgetReservation) requestPreemptionLocked() {
	budget := self.budget
	requiredBytes, requiredTransports := self.requiredCapacityLocked()
	if handoffBytes, handoffTransports, ok := self.handoffCapacityLocked(); ok {
		// Reclaim only the optional leases still needed after the paired previous
		// reservation is discounted. The previous carrier stays acquired until
		// the replacement connects.
		requiredBytes = handoffBytes
		requiredTransports = handoffTransports
	}
	byteDeficit := max(ByteCount(0), requiredBytes-budget.totalByteCount)
	transportDeficit := 0
	if 0 < budget.maxTransportCount {
		transportDeficit = max(0, requiredTransports-budget.maxTransportCount)
	}
	if byteDeficit == 0 && transportDeficit == 0 {
		return
	}

	victims := []*platformTransportBudgetReservation{}
	for candidate := range budget.reservations {
		if self.canPreemptLocked(candidate) {
			victims = append(victims, candidate)
		}
	}
	slices.SortFunc(victims, func(a, b *platformTransportBudgetReservation) int {
		// Reclaim background Auto H3 before foreground Auto H3. For equal
		// priorities, reclaim the oldest lease first so the result is stable.
		if a.priority != b.priority {
			return b.priority - a.priority
		}
		if a.sequence < b.sequence {
			return -1
		}
		if b.sequence < a.sequence {
			return 1
		}
		return 0
	})
	selectedVictims := []*platformTransportBudgetReservation{}
	for _, victim := range victims {
		if byteDeficit <= 0 && transportDeficit <= 0 {
			break
		}
		// A slotless Auto-H3 lease cannot solve a slot-only deficit.
		if byteDeficit <= 0 && (transportDeficit <= 0 || !victim.usesSlot) {
			continue
		}
		selectedVictims = append(selectedVictims, victim)
		byteDeficit -= victim.byteCount
		if victim.usesSlot {
			transportDeficit -= 1
		}
	}
	// Do not tear down a useful carrier for an H1 claim unless the complete
	// selected set can make that claim admissible. In particular, a slotless
	// Auto-H3 carrier can resolve a byte deficit but cannot resolve a simultaneous
	// transport-count deficit. Partially preempting it lets it reacquire
	// immediately, causing an endless yield/reacquire cycle while an over-cap H1
	// claim remains pending. An H3 policy replacement may deliberately drain its
	// old optional H3 carrier while it waits for an H1 slot to close, so retain
	// that make-before-break behavior for the H3 classes.
	if self.class == platformTransportBudgetH1 &&
		(0 < byteDeficit || 0 < transportDeficit) {
		return
	}
	for _, victim := range selectedVictims {
		victim.preemptRequested = true
		close(victim.preempt)
		budget.preemptedH3Count += 1
	}
}

func (self *platformTransportBudgetReservation) Acquire(ctx context.Context) bool {
	if self == nil {
		return true
	}
	for {
		budget := self.budget
		budget.mutex.Lock()
		if ctx.Err() != nil {
			self.releaseLocked()
			budget.mutex.Unlock()
			return false
		}
		if self.closed {
			budget.mutex.Unlock()
			return false
		}
		if self.acquired {
			budget.mutex.Unlock()
			return true
		}
		if canAcquire, usesHandoff := self.admissionLocked(); canAcquire {
			if self.class == platformTransportBudgetH1 && self.pending {
				budget.pendingH1ByteCount -= self.byteCount
				if self.usesSlot {
					budget.pendingH1SlotCount -= 1
				}
			}
			self.pending = false
			self.acquired = true
			if usesHandoff {
				budget.activeHandoff = self
				budget.handoffAcquisitionCount += 1
			} else {
				// Capacity became available without a loan between construction
				// and admission. Drop the unused pair so another migration is not
				// needlessly tied to the old carrier.
				self.clearHandoffLocked()
			}
			budget.usedByteCount += self.byteCount
			budget.reservedByteCount += self.byteCount
			if self.usesSlot {
				budget.usedTransportCount += 1
			}
			budget.notifyChangedLocked()
			budget.mutex.Unlock()
			return true
		}
		self.requestPreemptionLocked()
		notify := budget.notify
		budget.mutex.Unlock()

		select {
		case <-ctx.Done():
			self.Release()
			return false
		case <-notify:
		}
	}
}

// IsWaiting reports whether this live claim is currently prevented from
// acquiring by the aggregate budget. It is a point-in-time migration hint,
// not a platform capability signal; callers that need stable eligibility use
// PlatformTransportAutoEligibility instead.
func (self *platformTransportBudgetReservation) IsWaiting() bool {
	if self == nil {
		return false
	}
	budget := self.budget
	budget.mutex.Lock()
	defer budget.mutex.Unlock()
	if self.closed || self.acquired || !self.pending {
		return false
	}
	return !self.canAcquireLocked()
}

// PreemptNotify closes when a higher-precedence claim needs this acquired,
// optional Auto-H3 lease. The owner must stop its H3 sockets, then call Yield.
func (self *platformTransportBudgetReservation) PreemptNotify() <-chan struct{} {
	if self == nil {
		return nil
	}
	budget := self.budget
	budget.mutex.Lock()
	defer budget.mutex.Unlock()
	return self.preempt
}

// Yield returns an acquired optional H3 lease to pending state without closing
// the reservation. It can reacquire automatically after higher-precedence
// demand leaves. The socket owner calls this only after its H3 runners stop.
func (self *platformTransportBudgetReservation) Yield() bool {
	if self == nil {
		return false
	}
	budget := self.budget
	budget.mutex.Lock()
	defer budget.mutex.Unlock()
	if self.closed || !self.acquired || self.class != platformTransportBudgetH3Auto {
		return false
	}
	// Yield can occur while this optional Auto-H3 reservation is either side
	// of a policy handoff. Returning its capacity must also return the loan;
	// otherwise activeHandoff could remain pinned to a no-longer-acquired pair.
	self.clearHandoffToLocked()
	self.clearHandoffLocked()
	budget.usedByteCount -= self.byteCount
	budget.releasedByteCount += self.byteCount
	if self.usesSlot {
		budget.usedTransportCount -= 1
	}
	self.acquired = false
	self.pending = true
	self.preemptRequested = false
	self.preempt = make(chan struct{})
	budget.notifyChangedLocked()
	return true
}

// Release is idempotent and also unregisters a pending H1 priority claim.
func (self *platformTransportBudgetReservation) Release() {
	if self == nil {
		return
	}
	budget := self.budget
	budget.mutex.Lock()
	defer budget.mutex.Unlock()
	self.releaseLocked()
}

func (self *platformTransportBudgetReservation) releaseLocked() {
	if self.closed {
		return
	}
	self.closed = true
	budget := self.budget
	// This reservation may be either the replacement or the old carrier.
	// Releasing either endpoint resolves the loan before waking another waiter.
	self.clearHandoffToLocked()
	self.clearHandoffLocked()
	if self.class == platformTransportBudgetH1 && self.pending {
		budget.pendingH1ByteCount -= self.byteCount
		if self.usesSlot {
			budget.pendingH1SlotCount -= 1
		}
	}
	if self.acquired {
		budget.usedByteCount -= self.byteCount
		budget.releasedByteCount += self.byteCount
		if self.usesSlot {
			budget.usedTransportCount -= 1
		}
	}
	self.pending = false
	self.acquired = false
	delete(budget.reservations, self)
	budget.notifyChangedLocked()
}
