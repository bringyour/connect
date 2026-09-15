package connect

import (
	"context"
	"math"
	"sync"
)

// A MemoryTarget is a byte-denominated live admission budget for one memory
// area of one owner (e.g. the dns resolution of a single device): instances
// that consume tracked memory acquire bytes at creation and release them at
// teardown, so the target can be retuned at runtime and new instances
// immediately see the new headroom. Unlike the process-wide memory budget —
// advisory sizing state sampled when a Default*Settings constructor runs — a
// target is enforced at the admission choke points.
//
// Targets are per owner, not process-global: the sdk sizes one per
// DeviceLocal (split from DeviceLocalSettings.MemoryTargetByteCount), so a
// multi-device process (the cloud proxy) bounds each device independently.
// The message pools are the process-global complement (ResizeMessagePools).
//
// Each area chooses the admission semantic that degrades best when the
// target is exhausted:
//   - dns: in-flight queries wait for headroom (Acquire), bounding parallel
//     resolution without failing queries (see DohSettings.MemoryTarget)
//   - client p2p: new webrtc peer connections decline creation and traffic
//     stays on the platform transport (see WebRtcSettings.MemoryBudget)
//   - provider: flow tables size their caps from the target and evict lru
//     flows to fit (see DefaultProviderLocalUserNatSettingsWithMemoryTarget)
//
// A nil *MemoryTarget admits everything (all methods are nil-receiver safe),
// as does a zero capacity — consistent with the process memory budget
// convention (see SetMemoryBudget). To guarantee progress, an empty target
// admits one acquisition even when it is larger than capacity; absent a live
// capacity shrink, Used can therefore exceed Capacity by at most that one
// acquisition. Callers that require a hard ceiling must cap acquisition size.
//
// All methods are safe for concurrent use.
type MemoryTarget struct {
	stateLock sync.Mutex
	capacity  ByteCount
	used      ByteCount
	// allocated only while an acquirer is waiting; closed and cleared whenever
	// headroom may have grown
	notify chan struct{}
}

func NewMemoryTarget(capacity ByteCount) *MemoryTarget {
	return &MemoryTarget{
		capacity: capacity,
	}
}

// admitWithLock reports whether `byteCount` fits the current headroom.
// A target with no used bytes always admits, so a single acquisition larger
// than the capacity cannot deadlock (the transfer queue admission idiom).
func (self *MemoryTarget) admitWithLock(byteCount ByteCount) bool {
	if byteCount < 0 {
		return false
	}
	if self.capacity <= 0 {
		// Unlimited means there is no configured capacity ceiling, but the
		// signed accounting counter must still remain representable.
		return self.used <= ByteCount(math.MaxInt64)-byteCount
	}
	if self.used == 0 {
		return true
	}
	// Compare against the remaining capacity instead of adding the request
	// to used. A deliberately oversized singleton acquisition may set used
	// to MaxInt64; used+1 would then wrap negative and incorrectly admit
	// another acquisition.
	return self.used <= self.capacity && byteCount <= self.capacity-self.used
}

// Acquire takes `byteCount` from the target, waiting for headroom.
// Returns false if `ctx` ends before the acquisition; the caller must
// Release exactly the acquired bytes iff Acquire returned true.
func (self *MemoryTarget) Acquire(ctx context.Context, byteCount ByteCount) bool {
	if byteCount < 0 {
		return false
	}
	if self == nil {
		return true
	}
	for {
		var notify chan struct{}
		self.stateLock.Lock()
		if self.admitWithLock(byteCount) {
			self.used += byteCount
			self.stateLock.Unlock()
			return true
		}
		if self.notify == nil {
			self.notify = make(chan struct{})
		}
		notify = self.notify
		self.stateLock.Unlock()

		select {
		case <-ctx.Done():
			return false
		case <-notify:
		}
	}
}

// TryReserve takes `byteCount` from the target only if it fits the current
// headroom, without waiting
func (self *MemoryTarget) TryReserve(byteCount ByteCount) bool {
	if byteCount < 0 {
		return false
	}
	if self == nil {
		return true
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if !self.admitWithLock(byteCount) {
		return false
	}
	self.used += byteCount
	return true
}

// Release returns bytes to the target
func (self *MemoryTarget) Release(byteCount ByteCount) {
	if self == nil {
		return
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.used -= byteCount
	if self.used < 0 {
		// accounting bug: more released than acquired.
		// log unconditionally so production sees it
		DefaultLogger().Errorf("[mt]release below zero (%d)", self.used)
		self.used = 0
	}
	self.notifyWithLock()
}

// SetCapacity retunes the target. Shrinking does not evict existing
// acquisitions; the target admits nothing new until enough bytes release.
func (self *MemoryTarget) SetCapacity(capacity ByteCount) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.capacity = capacity
	self.notifyWithLock()
}

func (self *MemoryTarget) notifyWithLock() {
	if self.notify != nil {
		close(self.notify)
		self.notify = nil
	}
}

// Capacity is the target's byte capacity. 0 for a nil or unlimited target.
func (self *MemoryTarget) Capacity() ByteCount {
	if self == nil {
		return 0
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	return self.capacity
}

func (self *MemoryTarget) Used() ByteCount {
	if self == nil {
		return 0
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	return self.used
}

// Available is the unacquired remainder. MaxInt64 when the target is nil or
// unlimited (zero capacity).
func (self *MemoryTarget) Available() ByteCount {
	if self == nil {
		return ByteCount(math.MaxInt64)
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if self.capacity <= 0 {
		return ByteCount(math.MaxInt64)
	}
	return max(0, self.capacity-self.used)
}
