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

	// rttVar is RFC 6298's deviation term: an exponentially weighted mean of
	// |sample - mean|, one duration of state and no retained bytes. The
	// scaled mean this window reports carries a fixed margin, RttScale, which
	// cannot cover an excursion of several times the mean without lengthening
	// every retransmit on every lane. The deviation covers it where the lane
	// has shown a wide spread and tightens toward the mean where it has not
	// (FLIGHTGATEFIX §25.2).
	rttVar time.Duration
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
	// the deviation is measured against the mean before this sample joins it,
	// as RFC 6298 does, so one outlier does not hide inside its own mean
	if self.windowCount != 1 {
		mean := self.netRtt / time.Duration(self.windowCount-1)
		deviation := item.rtt - mean
		if deviation < 0 {
			deviation = -deviation
		}
		// rttVar = 3/4 rttVar + 1/4 deviation
		self.rttVar = (3*self.rttVar + deviation) / 4
	} else {
		// RFC 6298's first sample: the deviation starts at half the sample
		self.rttVar = item.rtt / 2
	}
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

// DeviationRtt is RFC 6298's retransmit timer for this window: the mean
// round trip plus four deviations, floored as the scaled mean is and capped
// by the same overall maximum. An empty window answers with the cold floor,
// exactly as the scaled mean does, so a cold start is unchanged.
//
// Against the scaled mean it trades two things. A lane whose samples are
// tight reports a shorter timer, so a genuinely lost tail is recovered
// sooner. A lane whose samples are spread reports a longer one, so a
// routine excursion of several times the mean no longer rewrites the whole
// window; its rare real loss waits longer for it (FLIGHTGATEFIX §25.2).
// An estimate carried with its own evidence: the value, how many samples back
// it, and how old the newest of them is.
//
// It is a type rather than a duration on purpose. A zero mean over no samples
// means unsampled and a zero mean over samples means a measured
// sub-millisecond path, and this program has twice been misled by exactly that
// ambiguity — once by a deviation timer over an unsampled stall and once by a
// receive-side precondition. A bare duration lets a caller read the first as
// the second by accident; this does not. Every other reader on the window
// folds the unsampled case into a resend floor, which is right for timing and
// wrong for measurement.
type RttEstimate struct {
	Mean        time.Duration
	SampleCount int
	// Age of the newest sample when the estimate was taken. An estimate whose
	// newest sample is older than the path's behaviour describes a path that
	// no longer exists, so freshness travels with the value rather than being
	// inferred from the caller's own clock.
	NewestSampleAge time.Duration
}

// Sampled reports whether any sample backs the mean.
func (self RttEstimate) Sampled() bool {
	return 0 < self.SampleCount
}

// Estimate is the window's unscaled mean round trip with its evidence.
func (self *RttWindow) Estimate() RttEstimate {
	return self.estimate(time.Now())
}

func (self *RttWindow) estimate(sampleTime time.Time) RttEstimate {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.coalesceWithLock(sampleTime)

	if self.windowCount == 0 {
		return RttEstimate{}
	}
	newestIndex := (self.windowTailIndex + self.windowCount - 1) % len(self.window)
	newestSampleAge := sampleTime.Sub(time.Unix(0, self.window[newestIndex].receiveUnixNano))
	return RttEstimate{
		Mean:            self.netRtt / time.Duration(self.windowCount),
		SampleCount:     self.windowCount,
		NewestSampleAge: max(0, newestSampleAge),
	}
}

func (self *RttWindow) DeviationRtt() time.Duration {
	return self.deviationRtt(time.Now())
}

func (self *RttWindow) deviationRtt(sendTime time.Time) time.Duration {
	self.stateLock.Lock()
	self.coalesceWithLock(sendTime)

	if self.windowCount == 0 {
		self.stateLock.Unlock()
		// no evidence: the cold floor, as the scaled mean answers
		return min(self.minScaledRtt, self.maxScaledRtt)
	}
	mean := self.netRtt / time.Duration(self.windowCount)
	margin := max(self.rttMinScaledRtt, 4*self.rttVar)
	self.stateLock.Unlock()

	return min(max(mean+margin, self.rttMinScaledRtt), self.maxScaledRtt)
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
