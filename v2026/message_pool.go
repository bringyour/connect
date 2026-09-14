package connect

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"runtime"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"maps"

	"google.golang.org/protobuf/proto"
)

// new byte allocations in the connect package use pooled message buffers,
// either via `MessagePoolCopy` or `MessagePoolGet`.
// There are three rules for pooled messages:
// - an owner of a message should return the message to the pool with `MessagePoolReturn`
//   when no longer used.
// - message ownership is handed off on send/channel write.
//   If the caller wants to retain the passed message, it should call `MessagePoolShareReadOnly`
//   before calling send/channel write.
// - messages are valid only for duration of a receive callback.
//   If the receiver wants to keep the message longer, it shoudl call `MessagePoolShareReadOnly`
//   before the callback returns.
// Shared messages are returned to the pool the same as normal messages.
// `MessagePoolReturn`/`MessagePoolShareReadOnly` is a noop when using a `[]byte` that is not part of the pool.

// set this to true to tag messages with useful debugging information e.g. the creation site
const debugTags = false

// [8 byte id][1 byte tag][1 byte flags][2 byte ref count]
const MessagePoolMetaByteCount = 12
const (
	MessagePoolFlagShared          = uint8(0x01)
	messagePoolFlagDeviceTunEgress = uint8(0x02)
)

// InitialMessagePoolByteCount is the initial total free-list byte budget. One
// third backs the small/full packet classes and two thirds backs the two large
// protocol-frame classes (see orderedMessagePools and ResizeMessagePools).
var InitialMessagePoolByteCount = mib(2)

const (
	// Four shards cut the measured pool-return convoy while adding only about
	// 54 KiB of fixed per-tag counter state across the three size classes.
	// The count is a power of two because the immutable buffer id carries the
	// shard in its low bits.
	messagePoolShardCount = 4
	messagePoolShardBits  = 2
	messagePoolShardMask  = messagePoolShardCount - 1
)

type messagePoolShard struct {
	stateLock sync.Mutex
	pool      [][]byte
	count     int
	maxCount  int
	// outstanding is mobile root ownership, independent of the resettable
	// diagnostic tag counters. Mobile builds maintain it inside the Get/Return
	// lock already held by the hot path; server builds compile those writes out.
	outstanding uint64
	// Device TUN egress is a subset of packet-root ownership. It is marked by
	// the SDK after acquisition and follows the root through read-only shares,
	// allowing mobile ACK admission to ignore unrelated inbound packet roots.
	deviceTunEgressOutstanding uint64
	takenTags                  [256]uint64
	returnedTags               [256]uint64
	createdTags                [256]uint64
	nextId                     uint64
}

type messagePool struct {
	size int

	// Administrative operations are rare and serialize capacity redistribution
	// across shards. Packet-path Get/Return operations never take this lock.
	adminLock sync.Mutex
	shards    [messagePoolShardCount]messagePoolShard
	nextShard atomic.Uint64
}

func newMessagePool(size int, maxCount int) *messagePool {
	mp := &messagePool{size: size}
	mp.Resize(maxCount)
	return mp
}

func (self *messagePool) Resize(maxCount int) {
	self.adminLock.Lock()
	defer self.adminLock.Unlock()

	for shardIndex := range messagePoolShardCount {
		shardCapacity := maxCount / messagePoolShardCount
		if shardIndex < maxCount%messagePoolShardCount {
			shardCapacity += 1
		}
		shard := &self.shards[shardIndex]
		shard.stateLock.Lock()
		if shardCapacity < shard.count {
			shard.count = shardCapacity
		}
		if shardCapacity < len(shard.pool) {
			// Capacity is a logical bound, not a reason to preallocate one
			// slice header per possible retained buffer. This matters for the
			// multi-GiB server limits, where an empty dense index consumed
			// hundreds of MiB before the first packet arrived.
			newPool := make([][]byte, shard.count)
			copy(newPool, shard.pool[:shard.count])
			shard.pool = newPool
		}
		shard.maxCount = shardCapacity
		shard.stateLock.Unlock()
	}
}

func (self *messagePool) capacity() int {
	capacity := 0
	for shardIndex := range messagePoolShardCount {
		shard := &self.shards[shardIndex]
		shard.stateLock.Lock()
		capacity += shard.maxCount
		shard.stateLock.Unlock()
	}
	return capacity
}

type messagePoolSnapshot struct {
	capacity                   int
	retained                   int
	deviceTunEgressOutstanding uint64
	takenTags                  [256]uint64
	returnedTags               [256]uint64
	createdTags                [256]uint64
}

// snapshot locks shards in index order and copies the whole class in one
// pass. Observability therefore costs four locks per class rather than four
// locks per tag, and capacity/retention/counters describe one consistent
// instant instead of being sampled across packet-path mutations.
func (self *messagePool) snapshot() messagePoolSnapshot {
	for shardIndex := range messagePoolShardCount {
		self.shards[shardIndex].stateLock.Lock()
	}
	defer func() {
		for shardIndex := messagePoolShardCount - 1; 0 <= shardIndex; shardIndex -= 1 {
			self.shards[shardIndex].stateLock.Unlock()
		}
	}()

	var snapshot messagePoolSnapshot
	for shardIndex := range messagePoolShardCount {
		shard := &self.shards[shardIndex]
		snapshot.capacity += shard.maxCount
		snapshot.retained += shard.count
		snapshot.deviceTunEgressOutstanding += shard.deviceTunEgressOutstanding
		for tag := range 256 {
			snapshot.takenTags[tag] += shard.takenTags[tag]
			snapshot.returnedTags[tag] += shard.returnedTags[tag]
			snapshot.createdTags[tag] += shard.createdTags[tag]
		}
	}
	return snapshot
}

func (self *messagePool) resetStats() {
	for shardIndex := range messagePoolShardCount {
		shard := &self.shards[shardIndex]
		shard.stateLock.Lock()
		clear(shard.takenTags[:])
		clear(shard.returnedTags[:])
		clear(shard.createdTags[:])
		shard.stateLock.Unlock()
	}
}

func (self *messagePool) warm(count int) {
	self.adminLock.Lock()
	defer self.adminLock.Unlock()

	count = min(count, self.capacity())
	for shardIndex := range messagePoolShardCount {
		targetCount := count / messagePoolShardCount
		if shardIndex < count%messagePoolShardCount {
			targetCount += 1
		}
		shard := &self.shards[shardIndex]
		shard.stateLock.Lock()
		targetCount = min(targetCount, shard.maxCount)
		for shard.count < targetCount {
			poolMessage := make([]byte, self.size+MessagePoolMetaByteCount)
			shard.nextId += 1
			id := shard.nextId<<messagePoolShardBits | uint64(shardIndex)
			binary.BigEndian.PutUint64(poolMessage[self.size:], id)
			poolMessage[self.size+8] = 255
			if shard.count < len(shard.pool) {
				shard.pool[shard.count] = poolMessage
			} else {
				shard.pool = append(shard.pool, poolMessage)
			}
			shard.count += 1
		}
		shard.stateLock.Unlock()
	}
}

func (self *messagePool) Clear() {
	self.adminLock.Lock()
	defer self.adminLock.Unlock()

	for shardIndex := range messagePoolShardCount {
		shard := &self.shards[shardIndex]
		shard.stateLock.Lock()
		shard.pool = nil
		shard.count = 0
		shard.stateLock.Unlock()
	}
}

// trim drops free buffers above maxRetained without shrinking the configured
// capacity. A later burst can therefore refill the same bounded free list;
// unlike Resize, this is decay of an idle high-water mark rather than a new
// packet-path policy.
func (self *messagePool) trim(maxRetained int) int {
	self.adminLock.Lock()
	defer self.adminLock.Unlock()

	maxRetained = max(0, min(maxRetained, self.capacity()))
	dropped := 0
	for shardIndex := range messagePoolShardCount {
		shardTarget := maxRetained / messagePoolShardCount
		if shardIndex < maxRetained%messagePoolShardCount {
			shardTarget += 1
		}
		shard := &self.shards[shardIndex]
		shard.stateLock.Lock()
		if shardTarget < shard.count {
			dropped += shard.count - shardTarget
			shard.count = shardTarget
		}
		if shard.count < len(shard.pool) {
			// Preserve the configured maxCount while releasing the dense index
			// that described an earlier high-water mark. It will grow lazily on
			// later returns, just like the buffer working set itself.
			newPool := make([][]byte, shard.count)
			copy(newPool, shard.pool[:shard.count])
			shard.pool = newPool
		}
		shard.stateLock.Unlock()
	}
	return dropped
}

func (self *messagePool) shardFor(poolMessage []byte) (*messagePoolShard, uint64) {
	id := binary.BigEndian.Uint64(poolMessage[self.size:])
	return &self.shards[id&messagePoolShardMask], id
}

// take removes and initializes one buffer under one of four bounded shard
// locks. Metadata and stats stay in the same critical section; allocation on
// a miss remains outside it. The payload is intentionally not zeroed.
func (self *messagePool) take(n int, tag uint8) []byte {
	shardIndex := int((self.nextShard.Add(1) - 1) & messagePoolShardMask)
	shard := &self.shards[shardIndex]
	shard.stateLock.Lock()
	if 0 < shard.count {
		poolMessage := shard.pool[shard.count-1]
		shard.pool[shard.count-1] = nil
		shard.count -= 1

		id := binary.BigEndian.Uint64(poolMessage[self.size:])
		count := binary.BigEndian.Uint16(poolMessage[self.size+10:])
		if count != 0 {
			shard.stateLock.Unlock()
			err := fmt.Errorf("message[%d] already taken", id)
			DefaultLogger().Errorf("[mp]%s", ErrorJson(err, debug.Stack()))
			panic(err)
		}
		if poolMessage[self.size+8] == 255 {
			shard.createdTags[tag] += 1
		}
		shard.takenTags[tag] += 1
		if messagePoolTrackPacketOutstanding && isPacketPoolSize(self.size) {
			shard.outstanding += 1
		}
		poolMessage[self.size+8] = tag
		poolMessage[self.size+9] = 0
		binary.BigEndian.PutUint16(poolMessage[self.size+10:], 1)
		shard.stateLock.Unlock()
		return poolMessage[:n]
	}

	// Reserve the unique id and update stats under the lock, but allocate after
	// unlocking: a pool miss must not convoy unrelated returns behind the
	// runtime allocator or a GC assist.
	shard.nextId += 1
	id := shard.nextId<<messagePoolShardBits | uint64(shardIndex)
	shard.createdTags[tag] += 1
	shard.takenTags[tag] += 1
	if messagePoolTrackPacketOutstanding && isPacketPoolSize(self.size) {
		shard.outstanding += 1
	}
	shard.stateLock.Unlock()

	poolMessage := make([]byte, self.size+MessagePoolMetaByteCount)
	binary.BigEndian.PutUint64(poolMessage[self.size:], id)
	poolMessage[self.size+8] = tag
	binary.BigEndian.PutUint16(poolMessage[self.size+10:], 1)
	return poolMessage[:n]
}

// release decrements the reference count and, on its final return, inserts the
// buffer into its acquisition shard in the same critical section. The
// immutable id chooses the shard without a global lock.
func (self *messagePool) release(poolMessage []byte) bool {
	shard, id := self.shardFor(poolMessage)
	shard.stateLock.Lock()
	tag := poolMessage[self.size+8]
	count := binary.BigEndian.Uint16(poolMessage[self.size+10:])
	if count == 0 {
		shard.stateLock.Unlock()
		// Double-return: log unconditionally so production sees it, but do the
		// stack capture/log write outside the hot pool lock.
		err := fmt.Errorf("[mp]return message[%d] not taken", id)
		DefaultLogger().Errorf("[mp]%s", ErrorJson(err, debug.Stack()))
		return false
	}
	if 1 < count {
		binary.BigEndian.PutUint16(poolMessage[self.size+10:], count-1)
		shard.stateLock.Unlock()
		return false
	}

	flags := poolMessage[self.size+9]
	poolMessage[self.size+8] = 0
	poolMessage[self.size+9] = 0
	binary.BigEndian.PutUint16(poolMessage[self.size+10:], 0)
	shard.returnedTags[tag] += 1
	if messagePoolTrackPacketOutstanding && isPacketPoolSize(self.size) {
		shard.outstanding -= 1
	}
	if flags&messagePoolFlagDeviceTunEgress != 0 {
		if shard.deviceTunEgressOutstanding == 0 {
			shard.stateLock.Unlock()
			panic("negative device TUN egress packet ownership")
		}
		shard.deviceTunEgressOutstanding -= 1
	}
	if shard.count < len(shard.pool) {
		// The payload does not need to be zeroed.
		shard.pool[shard.count] = poolMessage
		shard.count += 1
	} else if shard.count < shard.maxCount {
		// Grow only when a returned high-water mark needs another slot. The
		// ordinary take/return cycle continues to use the branch above, so
		// multi-GiB logical limits add no allocation to the steady hot path.
		shard.pool = append(shard.pool, poolMessage)
		shard.count += 1
	}
	shard.stateLock.Unlock()
	return true
}

const (
	// The small packet/control class fits ordinary IPv4/IPv6 TCP ACKs,
	// including TCP options, without making every 40--100 byte ACK retain a
	// 2-KiB packet root. It is also useful for the compact Transfer ACK and
	// other control frames selected by the generic MessagePoolGet path.
	smallPacketPoolSize = 256
	// The full packet class is sized to hold a device MTU packet (see
	// `DefaultMtu`) plus headers.
	packetPoolSize = 2048

	// Keep the packet classes within the historical packet-byte budget. One
	// quarter is ample for thousands of small ACK/control roots while the
	// remaining three quarters preserve the full-MTU working set.
	smallPacketPoolBudgetDivisor = 4
)

func isPacketPoolSize(poolSize int) bool {
	return poolSize == smallPacketPoolSize || poolSize == packetPoolSize
}

func splitPacketPoolByteCount(byteCount ByteCount) (small ByteCount, full ByteCount) {
	byteCount = max(0, byteCount)
	small = byteCount / smallPacketPoolBudgetDivisor
	return small, byteCount - small
}

// free list retention floors preserve a working reuse set on the hot paths
// even under a tiny budget. there is no in-flight cap here; `Get` always
// allocates on an empty list, so a zeroed class costs reuse, not liveness.
const packetPoolFloorByteCount = 256 * 1024

func packetPoolFloorCount(poolSize int) int {
	smallBytes, fullBytes := splitPacketPoolByteCount(packetPoolFloorByteCount)
	if poolSize == smallPacketPoolSize {
		return int(smallBytes) / poolSize
	}
	return int(fullBytes) / poolSize
}

// Keep the large-class floor constant in bytes, not buffers. That prevents a
// newly-added larger class from multiplying the minimum retained footprint.
const largeObjectPoolFloorByteCount = 256 * 1024

func largeObjectPoolFloorCount(poolSize int) int {
	return max(1, largeObjectPoolFloorByteCount/poolSize)
}

var orderedMessagePools = sync.OnceValue(func() []*messagePool {
	// Preserve the historical one-third packet / two-thirds large-object
	// initial split. The packet third is now divided between the small and
	// full-MTU classes, while each large class keeps its prior byte cap.
	// 4096 fits an ordinary two-packet transfer batch without retaining an
	// 8 KiB buffer for a ~3 KiB message. 8192 is the exact pooled class for a
	// full MinimumMessageLenLimit carrier. Larger frames remain unpooled.
	poolSizes := []int{smallPacketPoolSize, packetPoolSize, 4096, 8192}
	initialPacketBytes := InitialMessagePoolByteCount / 3
	initialSmallPacketBytes, initialFullPacketBytes :=
		splitPacketPoolByteCount(initialPacketBytes)
	initialLargeClassBytes := (InitialMessagePoolByteCount - initialPacketBytes) / 2
	pools := []*messagePool{}
	for _, poolSize := range poolSizes {
		poolByteCount := initialLargeClassBytes
		switch poolSize {
		case smallPacketPoolSize:
			poolByteCount = initialSmallPacketBytes
		case packetPoolSize:
			poolByteCount = initialFullPacketBytes
		}
		pools = append(
			pools,
			newMessagePool(poolSize, int(poolByteCount/ByteCount(poolSize))),
		)
	}

	go HandleError(func() {
		poolStats(pools)
	})

	return pools
})

func poolStats(pools []*messagePool) {
	// print stats from all pools on a regular interval
	for {
		// Per-tag pool efficiency is developer diagnostics: one line per pool
		// per tag per cycle, which made this the largest single log source in
		// the embedding services. Skip the whole pass unless the operator opts
		// in, so the snapshots, the caller joins, and the debug lock are not
		// paid for either. Embedders that need the aggregate without the
		// volume read `MessagePoolCounts`/`MessagePoolUnpooledCounts`.
		if v := DefaultLogger().V(1); v.Enabled() {
			for _, pool := range pools {
				snapshot := pool.snapshot()
				for tag := range 256 {
					taken := snapshot.takenTags[tag]
					returned := snapshot.returnedTags[tag]
					created := snapshot.createdTags[tag]
					if 0 < taken {
						ratio := float32(returned) / float32(taken)
						reuse := float32(taken-created) / float32(taken)
						var caller string
						func() {
							debugStateLock.Lock()
							defer debugStateLock.Unlock()
							caller = strings.Join(slices.Collect(maps.Keys(tagCallers[uint8(tag)])), "/")
						}()

						v.Infof("pool[%d] tag=%d [%s] r=%d/t=%d/c=%d = %.2f%% return / %.2f%% reuse\n", pool.size, tag, caller, returned, taken, created, 100*ratio, 100*reuse)
					}
				}
			}

			if taken, byteCount := MessagePoolUnpooledCounts(); 0 < taken {
				v.Infof("pool[unpooled] t=%d bytes=%d\n", taken, byteCount)
			}
		}

		select {
		case <-time.After(60 * time.Second):
		}
	}
}

// ResizeMessagePools bounds the free lists. With two arguments, the small and
// full-MTU packet classes split packetByteCount 1:3, and the large object
// classes split the second byte count evenly. The one-argument form preserves
// the historical API: packet classes share the supplied cap while every large
// class receives that cap.
//
// Buffers in use are unaffected; this only bounds what the free lists retain,
// with per-class floors that preserve a working reuse set. Safe to call at
// runtime.
func ResizeMessagePools(packetByteCount ByteCount, largeObjectByteCounts ...ByteCount) {
	pools := orderedMessagePools()
	largeObjectPoolCount := 0
	for _, pool := range pools {
		if !isPacketPoolSize(pool.size) {
			largeObjectPoolCount += 1
		}
	}
	largeObjectByteCount := packetByteCount * ByteCount(largeObjectPoolCount)
	if 0 < len(largeObjectByteCounts) {
		largeObjectByteCount = largeObjectByteCounts[0]
	}
	smallPacketByteCount, fullPacketByteCount := splitPacketPoolByteCount(packetByteCount)
	for _, pool := range pools {
		switch pool.size {
		case smallPacketPoolSize:
			pool.Resize(max(
				packetPoolFloorCount(pool.size),
				int(smallPacketByteCount/ByteCount(pool.size)),
			))
		case packetPoolSize:
			pool.Resize(max(
				packetPoolFloorCount(pool.size),
				int(fullPacketByteCount/ByteCount(pool.size)),
			))
		default:
			poolByteCount := largeObjectByteCount / ByteCount(largeObjectPoolCount)
			pool.Resize(max(largeObjectPoolFloorCount(pool.size), int(poolByteCount/ByteCount(pool.size))))
		}
	}
}

func messagePoolPacketWarmByteCounts(packetByteCount ByteCount) (
	smallWarmByteCount ByteCount,
	fullWarmByteCount ByteCount,
) {
	packetByteCount = max(0, packetByteCount)
	smallWarmByteCount = packetByteCount / 2
	fullWarmByteCount = packetByteCount - smallWarmByteCount
	var smallCapacityByteCount ByteCount
	var fullCapacityByteCount ByteCount
	for _, pool := range orderedMessagePools() {
		switch pool.size {
		case smallPacketPoolSize:
			smallCapacityByteCount = ByteCount(pool.capacity() * pool.size)
		case packetPoolSize:
			fullCapacityByteCount = ByteCount(pool.capacity() * pool.size)
		}
	}
	smallWarmByteCount = min(smallWarmByteCount, smallCapacityByteCount)
	fullWarmByteCount = min(fullWarmByteCount, fullCapacityByteCount)
	remainingByteCount := packetByteCount - smallWarmByteCount - fullWarmByteCount
	additionalSmallByteCount := min(
		remainingByteCount,
		smallCapacityByteCount-smallWarmByteCount,
	)
	smallWarmByteCount += additionalSmallByteCount
	remainingByteCount -= additionalSmallByteCount
	fullWarmByteCount += min(
		remainingByteCount,
		fullCapacityByteCount-fullWarmByteCount,
	)
	return
}

// WarmMessagePoolsTo pre-allocates at most packetByteCount across the small and
// full-MTU packet classes. The warm bytes split evenly until one class reaches
// its configured capacity, then its unused share moves to the other class.
// This lets a tiny mobile cap warm its entire 1:3-sized reuse set instead of
// accidentally keeping only one quarter of it.
// Large protocol-frame classes are not on the same cold-start path and stay cold.
//
// This parameterized form lets memory-constrained mobile embedders retain a
// smaller reuse set without changing the server default or the configured
// burst capacity. Call once at process start after sizing the pools; do not
// call directly from a memory-pressure path.
func WarmMessagePoolsTo(packetByteCount ByteCount) {
	smallWarmByteCount, fullWarmByteCount :=
		messagePoolPacketWarmByteCounts(packetByteCount)
	for _, pool := range orderedMessagePools() {
		switch pool.size {
		case smallPacketPoolSize:
			pool.warm(min(pool.capacity(), int(smallWarmByteCount/ByteCount(pool.size))))
		case packetPoolSize:
			pool.warm(min(pool.capacity(), int(fullWarmByteCount/ByteCount(pool.size))))
		}
	}
}

// WarmMessagePools preserves the historical 1-MiB startup reuse set used by
// desktop and server processes.
func WarmMessagePools() {
	WarmMessagePoolsTo(mib(1))
}

// TrimMessagePoolsTo drops an idle burst's excess free buffers while
// preserving a caller-selected packet reuse set and every class's configured
// capacity. Larger protocol objects retain their fixed 256-KiB floor per
// class. Buffers still held by consumers are untouched, and subsequent returns
// may refill the original capacity during the next burst.
func TrimMessagePoolsTo(packetByteCount ByteCount) ByteCount {
	smallWarmByteCount, fullWarmByteCount :=
		messagePoolPacketWarmByteCounts(packetByteCount)
	var droppedByteCount ByteCount
	for _, pool := range orderedMessagePools() {
		switch pool.size {
		case smallPacketPoolSize:
			droppedByteCount += ByteCount(
				pool.trim(min(pool.capacity(), int(smallWarmByteCount/ByteCount(pool.size)))) * pool.size,
			)
		case packetPoolSize:
			droppedByteCount += ByteCount(
				pool.trim(min(pool.capacity(), int(fullWarmByteCount/ByteCount(pool.size)))) * pool.size,
			)
		default:
			droppedByteCount += ByteCount(
				pool.trim(largeObjectPoolFloorCount(pool.size)) * pool.size,
			)
		}
	}
	return droppedByteCount
}

// TrimMessagePoolsToWarm preserves the historical 1-MiB packet reuse set for
// desktop and server callers.
func TrimMessagePoolsToWarm() ByteCount {
	return TrimMessagePoolsTo(mib(1))
}

func ClearMessagePools() {
	for _, pool := range orderedMessagePools() {
		pool.Clear()
	}
	// ACK-lifetime metadata and the uncommon H1 callback tails are also bounded,
	// recoverable packet-path pools. Drop them under the same host memory-
	// pressure signal; they repopulate lazily and never affect in-flight items.
	clearSendItemPool()
}

var seed = maphash.MakeSeed()
var debugStateLock sync.Mutex
var tagCallers = map[uint8]map[string]bool{}

func debugTag() uint8 {
	_, file2, line2, ok := runtime.Caller(2)
	if !ok {
		return 0
	}
	_, file3, line3, ok := runtime.Caller(3)
	if !ok {
		return 0
	}
	caller := fmt.Sprintf("%s:%d->%s:%d", file3, line3, file2, line2)
	tag := uint8(maphash.String(seed, caller))
	func() {
		debugStateLock.Lock()
		defer debugStateLock.Unlock()

		callers, ok := tagCallers[tag]
		if !ok {
			callers = map[string]bool{}
			tagCallers[tag] = callers
		}
		callers[caller] = true
	}()
	return tag
}

func ResetMessagePoolStats() {
	for _, pool := range orderedMessagePools() {
		pool.resetStats()
	}
	unpooledTakenCount.Store(0)
	unpooledTakenByteCount.Store(0)
}

// MessagePoolCounts returns the cumulative taken/returned/created message counts summed
// across all pools and tags. taken-returned is the number of pool messages currently held
// by consumers: it returns to a stable baseline when every taken buffer is eventually
// returned, so growth across a load-then-teardown cycle attributes a lost return (a buffer
// leak) even though the heap does not move (the GC quietly collects lost buffers). This is
// always tracked (independent of debugTags), so tests can assert pool balance in any build.
func MessagePoolCounts() (taken uint64, returned uint64, created uint64) {
	stats := GetMessagePoolAggregateStats()
	return stats.Taken, stats.Returned, stats.Created
}

// MessagePoolPacketOutstandingCount returns the number of small and full-MTU
// packet-class roots that consumers currently own. Shares do not increase this
// count: a buffer remains outstanding until its final MessagePoolReturn. Use
// MessagePoolPacketOutstandingByteCount for memory admission, because a small
// root costs one eighth of a full root.
//
// The call is allocation-free, but it takes each packet-pool shard lock. It is
// therefore intended for sampled admission control and telemetry, not every
// packet on unconstrained server paths.
func messagePoolPacketOutstandingSnapshot(fast bool) (uint64, ByteCount) {
	var outstandingCount uint64
	var outstandingBytes ByteCount
	for _, pool := range orderedMessagePools() {
		if !isPacketPoolSize(pool.size) {
			continue
		}
		for shardIndex := range messagePoolShardCount {
			shard := &pool.shards[shardIndex]
			shard.stateLock.Lock()
			owned := shard.outstanding
			if !fast {
				var taken uint64
				var returned uint64
				for tag := range 256 {
					taken += shard.takenTags[tag]
					returned += shard.returnedTags[tag]
				}
				owned = 0
				if returned < taken {
					owned = taken - returned
				}
			}
			shard.stateLock.Unlock()
			outstandingCount += owned
			outstandingBytes += ByteCount(owned) * ByteCount(pool.size)
		}
	}
	return outstandingCount, outstandingBytes
}

func MessagePoolPacketOutstandingCount() uint64 {
	outstanding, _ := messagePoolPacketOutstandingSnapshot(
		messagePoolTrackPacketOutstanding,
	)
	return outstanding
}

// MessagePoolPacketOutstandingByteCount is the allocation-free packet-root
// ownership signal used by byte-bounded mobile admission. It intentionally
// charges the size class, not len(message): a live pooled ACK owns all 256
// bytes of its root and a live MTU packet owns all 2048 bytes.
func MessagePoolPacketOutstandingByteCount() ByteCount {
	_, outstandingBytes := messagePoolPacketOutstandingSnapshot(
		messagePoolTrackPacketOutstanding,
	)
	return outstandingBytes
}

// MessagePoolPacketRootByteCount reports how many bytes of packet-class root
// ownership are represented by message. Non-packet and unpooled buffers return
// zero; they are not included in MessagePoolPacketOutstandingByteCount either.
func MessagePoolPacketRootByteCount(message []byte) ByteCount {
	switch cap(message) {
	case smallPacketPoolSize + MessagePoolMetaByteCount:
		return smallPacketPoolSize
	case packetPoolSize + MessagePoolMetaByteCount:
		return packetPoolSize
	}
	return 0
}

// MessagePoolRootByteCount reports the retained backing-store charge for a
// complete owned message. Pooled slices are charged at their size class rather
// than len(message); an unpooled slice is charged at its visible length. The
// receive reorder budget uses this for complete frame/message owners, where
// callers hold the original slice rather than an arbitrary subslice.
func MessagePoolRootByteCount(message []byte) ByteCount {
	switch cap(message) {
	case smallPacketPoolSize + MessagePoolMetaByteCount:
		return smallPacketPoolSize
	case packetPoolSize + MessagePoolMetaByteCount:
		return packetPoolSize
	case 4096 + MessagePoolMetaByteCount:
		return 4096
	case 8192 + MessagePoolMetaByteCount:
		return 8192
	default:
		return ByteCount(len(message))
	}
}

// Marks a packet root as originating at the device TUN. The classification is
// idempotent and follows read-only shares until the final return. Non-packet,
// unpooled, or already-returned buffers are unchanged. The SDK calls this
// before mobile pressure admission so remote download roots cannot consume the
// client ACK reserve.
func MessagePoolMarkDeviceTunEgress(message []byte) bool {
	var pool *messagePool
	switch cap(message) {
	case smallPacketPoolSize + MessagePoolMetaByteCount:
		pool = orderedMessagePools()[0]
	case packetPoolSize + MessagePoolMetaByteCount:
		pool = orderedMessagePools()[1]
	default:
		return false
	}
	poolMessage := message[:cap(message)]
	shard, _ := pool.shardFor(poolMessage)
	shard.stateLock.Lock()
	defer shard.stateLock.Unlock()
	if binary.BigEndian.Uint16(poolMessage[pool.size+10:]) == 0 {
		return false
	}
	if poolMessage[pool.size+9]&messagePoolFlagDeviceTunEgress == 0 {
		poolMessage[pool.size+9] |= messagePoolFlagDeviceTunEgress
		shard.deviceTunEgressOutstanding += 1
	}
	return true
}

// Reports exact small/full root bytes currently owned by device-originated
// TUN packets. The snapshot is allocation-free and takes the packet shard
// locks, matching the existing sampled aggregate ownership query.
func MessagePoolDeviceTunEgressOutstandingByteCount() ByteCount {
	var byteCount ByteCount
	for _, pool := range orderedMessagePools() {
		if !isPacketPoolSize(pool.size) {
			continue
		}
		for shardIndex := range messagePoolShardCount {
			shard := &pool.shards[shardIndex]
			shard.stateLock.Lock()
			outstanding := shard.deviceTunEgressOutstanding
			shard.stateLock.Unlock()
			byteCount += ByteCount(outstanding) * ByteCount(pool.size)
		}
	}
	return byteCount
}

// MessagePoolAggregateStats is an allocation-free process-wide snapshot used
// by frequent host memory telemetry. Retained describes free-list ownership,
// which is intentionally distinct from the in-flight Taken-Returned count.
type MessagePoolAggregateStats struct {
	Taken                               uint64
	Returned                            uint64
	Created                             uint64
	RetainedCount                       int
	RetainedByteCount                   ByteCount
	CapacityByteCount                   ByteCount
	PacketRetainedCount                 int
	PacketRetainedByteCount             ByteCount
	LargeObjectRetainedCount            int
	LargeObjectRetainedByteCount        ByteCount
	DeviceTunEgressOutstandingCount     uint64
	DeviceTunEgressOutstandingByteCount ByteCount
}

func GetMessagePoolAggregateStats() MessagePoolAggregateStats {
	var stats MessagePoolAggregateStats
	for _, pool := range orderedMessagePools() {
		snapshot := pool.snapshot()
		for tag := range 256 {
			stats.Taken += snapshot.takenTags[tag]
			stats.Returned += snapshot.returnedTags[tag]
			stats.Created += snapshot.createdTags[tag]
		}
		retainedByteCount := ByteCount(snapshot.retained * pool.size)
		stats.RetainedCount += snapshot.retained
		stats.RetainedByteCount += retainedByteCount
		stats.CapacityByteCount += ByteCount(snapshot.capacity * pool.size)
		if isPacketPoolSize(pool.size) {
			stats.PacketRetainedCount += snapshot.retained
			stats.PacketRetainedByteCount += retainedByteCount
			stats.DeviceTunEgressOutstandingCount +=
				snapshot.deviceTunEgressOutstanding
			stats.DeviceTunEgressOutstandingByteCount +=
				ByteCount(snapshot.deviceTunEgressOutstanding) * ByteCount(pool.size)
		} else {
			stats.LargeObjectRetainedCount += snapshot.retained
			stats.LargeObjectRetainedByteCount += retainedByteCount
		}
	}
	return stats
}

// MessagePoolClassStats is a point-in-time view of one size class. Capacity
// and Retained count free-list buffers; Taken/Returned/Created are cumulative
// since process start or ResetMessagePoolStats.
type MessagePoolClassStats struct {
	Size     int
	Capacity int
	Retained int
	Taken    uint64
	Returned uint64
	Created  uint64
}

// GetMessagePoolClassStats returns one snapshot per ordered size class.
func GetMessagePoolClassStats() []*MessagePoolClassStats {
	var stats []*MessagePoolClassStats
	for _, pool := range orderedMessagePools() {
		snapshot := pool.snapshot()
		classStats := &MessagePoolClassStats{
			Size:     pool.size,
			Capacity: snapshot.capacity,
			Retained: snapshot.retained,
		}
		for tag := range 256 {
			classStats.Taken += snapshot.takenTags[tag]
			classStats.Returned += snapshot.returnedTags[tag]
			classStats.Created += snapshot.createdTags[tag]
		}
		stats = append(stats, classStats)
	}
	return stats
}

func MessagePoolStats() map[int]map[int]float32 {
	sizeTagRatios := map[int]map[int]float32{}
	for _, pool := range orderedMessagePools() {
		snapshot := pool.snapshot()
		tagRatios := map[int]float32{}
		for tag := range 256 {
			taken := snapshot.takenTags[tag]
			returned := snapshot.returnedTags[tag]

			if 0 < taken {
				ratio := float32(returned) / float32(taken)
				tagRatios[tag] = ratio
			}
		}
		sizeTagRatios[pool.size] = tagRatios
	}
	return sizeTagRatios
}

/*
func MessagePool(targetSize int) (*messagePool, int) {
	for _, pool := range orderedMessagePools {
		if targetSize <= pool.size {
			return pool, pool.size
		}
	}
	// return the largest
	pool := orderedMessagePools[len(orderedMessagePools)-1]
	return pool, pool.size
}
*/

func MessagePoolReadAll(r io.Reader) ([]byte, error) {
	return MessagePoolReadAllWithTag(r, 0)
}

var ErrMessageTooLarge = errors.New("message exceeds byte limit")

// MessagePoolReadAllLimit preserves MessagePoolReadAll's ownership contract
// while putting a hard ceiling on data read from an untrusted framed stream.
// On an error it returns no buffer, so callers never need to return a partial
// message to the pool.
func MessagePoolReadAllLimit(r io.Reader, maxBytes int64) ([]byte, error) {
	if maxBytes < 0 {
		return nil, fmt.Errorf("invalid message byte limit %d", maxBytes)
	}

	message, err := MessagePoolReadAll(io.LimitReader(r, maxBytes+1))
	if err != nil {
		return nil, err
	}
	if maxBytes < int64(len(message)) {
		MessagePoolReturn(message)
		return nil, fmt.Errorf("%w: limit %d", ErrMessageTooLarge, maxBytes)
	}
	return message, nil
}

func MessagePoolReadAllWithTag(r io.Reader, tag uint8) ([]byte, error) {
	orderedMessagePools := orderedMessagePools()
	// Stream reads historically start with one full packet-sized buffer. Do
	// not add an extra 256-byte read/copy step merely because the small class
	// now precedes it for exact-size Get/Copy calls.
	startPoolIndex := 0
	for orderedMessagePools[startPoolIndex].size != packetPoolSize {
		startPoolIndex += 1
	}

	b := orderedMessagePools[startPoolIndex].take(packetPoolSize, tag)
	i := 0
	for j := startPoolIndex; j < len(orderedMessagePools); j += 1 {
		for i < len(b) {
			n, err := r.Read(b[i:])
			if n > 0 {
				i += n
			}
			if err != nil {
				if err == io.EOF {
					return b[:i], nil
				}
				MessagePoolReturn(b)
				return nil, err
			}
			if n == 0 {
				return b[:i], nil
			}
		}

		if len(orderedMessagePools) <= j+1 {
			break
		}

		b2, _ := MessagePoolGetDetailedWithTag(orderedMessagePools[j+1].size, tag)
		copy(b2, b)
		MessagePoolReturn(b)
		b = b2
	}

	out := make([]byte, i, 2*i)
	copy(out, b)
	defer MessagePoolReturn(b)
	for {
		n, err := r.Read(b)
		if n > 0 {
			out = append(out, b[:n]...)
		}
		if err != nil {
			if err == io.EOF {
				return out, nil
			}
			// Preserve the historical contract that (non-EOF) errors yield a nil buffer
			// (callers do not expect to MessagePoolReturn on the error path).
			// We still consumed the bytes (preventing reader desync on streams).
			return nil, err
		}
		if n == 0 {
			return out, nil
		}
	}
}

func MessagePoolCopy(message []byte) []byte {
	b, _ := MessagePoolCopyDetailed(message)
	return b
}

func MessagePoolCopyDetailed(message []byte) ([]byte, bool) {
	var tag uint8
	if debugTags {
		tag = debugTag()
	}
	return MessagePoolCopyDetailedWithTag(message, tag)
}

func MessagePoolCopyDetailedWithTag(message []byte, tag uint8) ([]byte, bool) {
	poolMessage, pooled := MessagePoolGetDetailedWithTag(len(message), tag)
	copy(poolMessage, message)
	return poolMessage, pooled
}

func MessagePoolGet(n int) []byte {
	b, _ := MessagePoolGetDetailed(n)
	return b
}

func MessagePoolGetDetailed(n int) ([]byte, bool) {
	var tag uint8
	if debugTags {
		tag = debugTag()
	}
	return MessagePoolGetDetailedWithTag(n, tag)
}

func MessagePoolGetDetailedWithTag(n int, tag uint8) ([]byte, bool) {
	orderedMessagePools := orderedMessagePools()

	// Full-MTU packets are the dominant server/provider case. Keep their
	// historical one-comparison fast path even though the small ACK/control
	// class sorts before them. Tiny messages take one additional comparison,
	// and the uncommon larger classes retain the ordered walk.
	// One unsigned range comparison covers [small+1, full]. Values below the
	// range wrap above it, so full packets do not pay two ordered comparisons.
	if uint(n-smallPacketPoolSize-1) <
		uint(packetPoolSize-smallPacketPoolSize) {
		return orderedMessagePools[1].take(n, tag), true
	}
	if n <= smallPacketPoolSize {
		return orderedMessagePools[0].take(n, tag), true
	}
	for _, pool := range orderedMessagePools[2:] {
		if n <= pool.size {
			return pool.take(n, tag), true
		}
	}
	// allocate a new message
	unpooledTakenCount.Add(1)
	unpooledTakenByteCount.Add(uint64(n))
	poolMessage := make([]byte, n+MessagePoolMetaByteCount)
	return poolMessage[:n], false
}

// allocations larger than every size class are never pooled. tracked so the
// stats show whether the classes are absorbing the message traffic.
var unpooledTakenCount atomic.Uint64
var unpooledTakenByteCount atomic.Uint64

// MessagePoolUnpooledCounts returns the cumulative count and total bytes of
// allocations that exceeded every pool size class
func MessagePoolUnpooledCounts() (taken uint64, byteCount uint64) {
	return unpooledTakenCount.Load(), unpooledTakenByteCount.Load()
}

func MessagePoolReturn(message []byte) bool {
	orderedMessagePools := orderedMessagePools()

	c := cap(message)
	// Mirror Get's full-packet fast path. Server proxy batches overwhelmingly
	// return 2-KiB roots; making them scan the preceding 256-byte class measured
	// as a small but repeatable batch-throughput regression.
	if c == packetPoolSize+MessagePoolMetaByteCount {
		return orderedMessagePools[1].release(message[:c])
	}
	if c == smallPacketPoolSize+MessagePoolMetaByteCount {
		return orderedMessagePools[0].release(message[:c])
	}
	for _, pool := range orderedMessagePools[2:] {
		if c == pool.size+MessagePoolMetaByteCount {
			return pool.release(message[:c])
		}
	}
	// else drop the message, let it gc
	return false
}

func MessagePoolShareReadOnly(message []byte) []byte {
	orderedMessagePools := orderedMessagePools()

	c := cap(message)
	candidatePools := orderedMessagePools[2:]
	if c == packetPoolSize+MessagePoolMetaByteCount {
		candidatePools = orderedMessagePools[1:2]
	} else if c == smallPacketPoolSize+MessagePoolMetaByteCount {
		candidatePools = orderedMessagePools[0:1]
	}
	for _, pool := range candidatePools {
		if c == pool.size+MessagePoolMetaByteCount {
			poolMessage := message[:c]
			shard, id := pool.shardFor(poolMessage)

			func() {
				shard.stateLock.Lock()
				defer shard.stateLock.Unlock()

				count := binary.BigEndian.Uint16(poolMessage[pool.size+10:])
				if count == 0 {
					DefaultLogger().Warningf("[mp]share message[%d] not taken", id)
				} else {
					binary.BigEndian.PutUint16(poolMessage[pool.size+10:], count+1)
					poolMessage[pool.size+9] |= MessagePoolFlagShared
				}
			}()

			return message
		}
	}
	// not a pool message
	return message
}

func MessagePoolCheck(message []byte) (pooled bool, shared bool) {
	orderedMessagePools := orderedMessagePools()

	c := cap(message)
	for _, pool := range orderedMessagePools {
		if c == pool.size+MessagePoolMetaByteCount {
			poolMessage := message[:c]
			shard, _ := pool.shardFor(poolMessage)

			func() {
				shard.stateLock.Lock()
				defer shard.stateLock.Unlock()

				count := binary.BigEndian.Uint16(poolMessage[pool.size+10:])
				if 0 < count {
					pooled = true
					shared = poolMessage[pool.size+9]&MessagePoolFlagShared != 0
				}
			}()

			return
		}
	}
	// not a pool message
	return
}

func ProtoMarshal(m proto.Message) ([]byte, error) {
	var tag uint8
	if debugTags {
		tag = debugTag()
	}
	return ProtoMarshalWithTag(m, tag)
}

func ProtoMarshalWithTag(m proto.Message, tag uint8) ([]byte, error) {
	if m == nil {
		return nil, nil
	}

	buf, _ := MessagePoolGetDetailedWithTag(proto.Size(m), tag)

	out, err := proto.MarshalOptions{}.MarshalAppend(buf[:0], m)
	if err != nil {
		MessagePoolReturn(buf)
		return nil, err
	}
	// if proto.Size underestimated, MarshalAppend may have allocated a fresh
	// slice; the pool buffer is then orphaned and must be returned to balance
	// the Get above. detected by a cap change (append only grows cap).
	if cap(out) != cap(buf) {
		MessagePoolReturn(buf)
	}
	return out, nil
}

func ProtoUnmarshal(b []byte, m proto.Message) error {
	return proto.Unmarshal(b, m)
}

func EncodeBase64(enc *base64.Encoding, src []byte) string {
	buf := MessagePoolGet(enc.EncodedLen(len(src)))
	defer MessagePoolReturn(buf)
	enc.Encode(buf, src)
	return string(buf)
}

func DecodeBase64(enc *base64.Encoding, s string) ([]byte, error) {
	sbuf := MessagePoolGet(len(s))
	defer MessagePoolReturn(sbuf)
	copy(sbuf, s)
	buf := MessagePoolGet(enc.DecodedLen(len(s)))
	n, err := enc.Decode(buf, sbuf)
	if err != nil {
		MessagePoolReturn(buf)
		return nil, err
	}
	return buf[:n], nil
}
