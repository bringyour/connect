package connect

import (
	"fmt"
	"sync"
	"time"

	"github.com/urnetwork/connect/protocol"
)

type rttWindowItem struct {
	receiveUnixNano int64
	rtt             time.Duration
	sequence        uint64
}

type rttWindowMinimum struct {
	rtt      time.Duration
	sequence uint64
}

type RttWindow struct {
	log           Logger
	windowTimeout time.Duration
	rttScale      float32
	// minScaledRtt is the COLD floor: used when the window holds no samples
	// (nothing acked yet, or a long quiet gap aged everything out) — no
	// evidence, so resend conservatively.
	minScaledRtt time.Duration
	// rttMinScaledRtt is the floor once samples exist: the measured path rtt
	// (scaled) governs, bounded below by this, so a lost packet on a fast
	// path retries in hundreds of milliseconds instead of the cold floor.
	// The per-item exponential backoff (see the resend loop) bounds the
	// duplicate cost of a too-eager first retry.
	rttMinScaledRtt time.Duration
	maxScaledRtt    time.Duration

	stateLock       sync.Mutex
	window          []rttWindowItem
	windowTailIndex int
	windowCount     int
	nextSequence    uint64
	netRtt          time.Duration

	// minimums is a fixed-capacity monotonic deque. Keeping the smallest live
	// RTT at its head avoids both the old per-Ack heap node allocation and an
	// O(window) scan on the recovery-probe path.
	minimums         []rttWindowMinimum
	minimumHeadIndex int
	minimumCount     int
}

func NewRttWindow(
	log Logger,
	windowSize int,
	windowTimeout time.Duration,
	rttScale float32,
	minScaledRtt time.Duration,
	rttMinScaledRtt time.Duration,
	maxScaledRtt time.Duration,
) *RttWindow {
	if windowSize == 0 {
		panic(fmt.Errorf("Window size must non-zero: %d", windowSize))
	}
	if rttMinScaledRtt <= 0 {
		// no rtt floor configured: sampled paths floor at the cold value
		// (the historical flat-floor behavior)
		rttMinScaledRtt = minScaledRtt
	}
	window := make([]rttWindowItem, windowSize)

	return &RttWindow{
		log:             loggerOrDefault(log),
		windowTimeout:   windowTimeout,
		rttScale:        rttScale,
		minScaledRtt:    minScaledRtt,
		rttMinScaledRtt: rttMinScaledRtt,
		maxScaledRtt:    maxScaledRtt,
		window:          window,
		windowTailIndex: 0,
		minimums:        make([]rttWindowMinimum, windowSize),
	}
}

// removeOldestWithLock removes exactly one live sample while stateLock is held.
func (self *RttWindow) removeOldestWithLock() {
	if self.windowCount == 0 {
		return
	}
	item := self.window[self.windowTailIndex]
	self.netRtt -= item.rtt
	if self.minimumCount != 0 &&
		self.minimums[self.minimumHeadIndex].sequence == item.sequence {
		self.minimums[self.minimumHeadIndex] = rttWindowMinimum{}
		self.minimumHeadIndex = (self.minimumHeadIndex + 1) % len(self.minimums)
		self.minimumCount--
	}
	self.window[self.windowTailIndex] = rttWindowItem{}
	self.windowTailIndex = (self.windowTailIndex + 1) % len(self.window)
	self.windowCount--
}

// Removes expired samples while stateLock is held.
func (self *RttWindow) coalesceWithLock(windowTime time.Time) {
	windowStartUnixNano := windowTime.Add(-self.windowTimeout).UnixNano()
	for self.windowCount != 0 {
		item := self.window[self.windowTailIndex]
		if item.receiveUnixNano >= windowStartUnixNano {
			break
		}
		self.removeOldestWithLock()
	}
}

func (self *RttWindow) OpenTag() *protocol.Tag {
	return self.openTag(time.Now())
}

func (self *RttWindow) openTag(sendTime time.Time) *protocol.Tag {
	// sendTime
	return &protocol.Tag{
		SendTime: uint64(sendTime.UnixMilli()),
	}
}

func (self *RttWindow) CloseTag(tag *protocol.Tag) {
	self.closeSendTime(tag.SendTime, time.Now())
}

func (self *RttWindow) closeTag(tag *protocol.Tag, receiveTime time.Time) {
	self.closeSendTime(tag.SendTime, receiveTime)
}

// CloseSendTime is the allocation-free ACK hot-path form. ACK windows retain
// Tag's scalar wire value instead of copying a generated protobuf message
// (whose internal MessageState must not be copied).
func (self *RttWindow) CloseSendTime(sendTimeUnixMilli uint64) {
	self.closeSendTime(sendTimeUnixMilli, time.Now())
}

func (self *RttWindow) closeSendTime(sendTimeUnixMilli uint64, receiveTime time.Time) {
	sendTime := time.UnixMilli(int64(sendTimeUnixMilli))
	if receiveTime.Before(sendTime) {
		// ignore
		return
	}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.coalesceWithLock(receiveTime)

	if self.windowCount == len(self.window) {
		self.removeOldestWithLock()
	}
	self.nextSequence++
	item := rttWindowItem{
		receiveUnixNano: receiveTime.UnixNano(),
		rtt:             receiveTime.Sub(sendTime),
		sequence:        self.nextSequence,
	}
	windowHeadIndex := (self.windowTailIndex + self.windowCount) % len(self.window)
	self.window[windowHeadIndex] = item
	self.windowCount++
	self.netRtt += item.rtt

	// Newer equal minima supersede older ones. This keeps the deque shortest
	// and guarantees its head remains live until the matching sequence leaves
	// the sample ring.
	for self.minimumCount != 0 {
		minimumTailIndex := (self.minimumHeadIndex + self.minimumCount - 1) % len(self.minimums)
		if self.minimums[minimumTailIndex].rtt < item.rtt {
			break
		}
		self.minimums[minimumTailIndex] = rttWindowMinimum{}
		self.minimumCount--
	}
	minimumTailIndex := (self.minimumHeadIndex + self.minimumCount) % len(self.minimums)
	self.minimums[minimumTailIndex] = rttWindowMinimum{rtt: item.rtt, sequence: item.sequence}
	self.minimumCount++
}

// clamp(mean rtt of window * scale, floor, overall max), where the floor is
// rttMinScaledRtt once samples exist and the conservative minScaledRtt when
// the window is empty (cold start / long quiet gap).
func (self *RttWindow) ScaledRtt() time.Duration {
	return self.scaledRtt(time.Now())
}

func (self *RttWindow) scaledRtt(sendTime time.Time) time.Duration {
	self.stateLock.Lock()
	self.coalesceWithLock(sendTime)

	var useRtt time.Duration
	if self.windowCount != 0 {
		useRtt = self.netRtt / time.Duration(self.windowCount)
	}
	floor := self.rttMinScaledRtt
	if useRtt == 0 {
		// no samples: no evidence to be aggressive on
		floor = self.minScaledRtt
	}
	scaledRtt := min(
		max(
			time.Duration(float32(useRtt/time.Millisecond)*self.rttScale)*time.Millisecond,
			floor,
		),
		self.maxScaledRtt,
	)
	self.stateLock.Unlock()
	// guard the V(2) diagnostic: this runs per packet (resend timing), and the
	// disabled-level call would still box the Duration arg into []any and build
	// the variadic slice on the heap. the guard keeps the hot path allocation-free.
	if self.log.V(2).Enabled() {
		self.log.Infof("[rtt]scaled=%dms\n", scaledRtt/time.Millisecond)
	}
	return scaledRtt
}

// Returns a bounded minimum-path RTT for one receiver-paced recovery probe.
// Queue-inflated mean RTT remains the ordinary resend timer; using the minimum
// here prevents one deep serialization queue from turning a tail probe into the
// same multi-second RTO it is meant to precede. Callers bound duplicate cost to
// one probe per item.
func (self *RttWindow) ProbeRtt() time.Duration {
	return self.probeRtt(time.Now())
}

func (self *RttWindow) probeRtt(probeTime time.Time) time.Duration {
	self.stateLock.Lock()
	self.coalesceWithLock(probeTime)

	var useRtt time.Duration
	if self.minimumCount != 0 {
		useRtt = self.minimums[self.minimumHeadIndex].rtt
	}
	floor := self.rttMinScaledRtt
	if useRtt == 0 {
		floor = self.minScaledRtt
	}
	probeRtt := min(
		max(
			time.Duration(float32(useRtt/time.Millisecond)*self.rttScale)*time.Millisecond,
			floor,
		),
		self.maxScaledRtt,
	)
	self.stateLock.Unlock()

	if self.log.V(2).Enabled() {
		self.log.Infof("[rtt]probe=%dms\n", probeRtt/time.Millisecond)
	}
	return probeRtt
}
