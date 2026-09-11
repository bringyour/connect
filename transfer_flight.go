package connect

// This file owns acknowledgement-flight admission for unreliable Transfer
// carriers. SendSequence is the only caller and goroutine owner; no method is
// safe for concurrent use.

// Tracks encoded Transfer bytes without receiver delivery evidence. Reliable
// routes retain historical unlimited admission. A negotiated unreliable route
// starts below the low-bar queue and grows only from receiver delivery
// evidence. Transfer retains a hard admission cap and remains the sole payload
// retransmitter.
type sendFlightController struct {
	initialByteCount     ByteCount
	minimumByteCount     ByteCount
	maximumByteCount     ByteCount
	increaseByteCount    ByteCount
	initialMessageCount  int
	minimumMessageCount  int
	maximumMessageCount  int
	increaseMessageCount int
	slowStartDivisor     ByteCount
	// The active carrier may tighten, but never enlarge, the configured
	// byte/message bounds for one route generation.
	activeMinimumByteCount    ByteCount
	activeMaximumByteCount    ByteCount
	policyByteLimit           ByteCount
	activeMinimumMessageCount int
	activeMaximumMessageCount int
	policyMessageLimit        int
	flowReserveEnabled        bool
	// reliableRouteAvailable mirrors the route generation's reliable-carrier
	// state so the sender can tell a mixed-lane route from a single-lane one
	// without reading the snapshot again (FLIGHTGATEFIX §14).
	reliableRouteAvailable bool

	generation                 uint64
	limited                    bool
	slowStart                  bool
	byteCount                  ByteCount
	byteLimit                  ByteCount
	messageCount               int
	messageLimit               int
	messageCountByKey          map[sendSchedulingKey]int
	flowReserveInUse           bool
	additiveIncreaseRemainder  ByteCount
	slowStartIncreaseRemainder ByteCount
	additiveMessageRemainder   int
	slowStartMessageRemainder  int
}

// Normalizes optional settings once so the packet path has no configuration
// branches beyond the carrier-policy check.
func newSendFlightController(settings *SendBufferSettings) *sendFlightController {
	initialByteCount := settings.UnreliableInitialFlightByteCount
	if initialByteCount <= 0 {
		return &sendFlightController{
			messageCountByKey: map[sendSchedulingKey]int{},
		}
	}
	minimumByteCount := settings.UnreliableMinimumFlightByteCount
	if minimumByteCount <= 0 {
		minimumByteCount = initialByteCount
	}
	maximumByteCount := settings.UnreliableMaximumFlightByteCount
	if maximumByteCount < minimumByteCount {
		maximumByteCount = minimumByteCount
	}
	initialByteCount = min(max(initialByteCount, minimumByteCount), maximumByteCount)
	increaseByteCount := settings.UnreliableFlightIncreaseByteCount
	if increaseByteCount <= 0 {
		increaseByteCount = 1
	}
	slowStartDivisor := ByteCount(settings.UnreliableSlowStartGrowthDivisor)
	if slowStartDivisor <= 0 {
		slowStartDivisor = 1
	}
	initialMessageCount := settings.UnreliableInitialFlightMessageCount
	minimumMessageCount := settings.UnreliableMinimumFlightMessageCount
	maximumMessageCount := settings.UnreliableMaximumFlightMessageCount
	increaseMessageCount := settings.UnreliableFlightIncreaseMessageCount
	if 0 < initialMessageCount {
		if minimumMessageCount <= 0 {
			minimumMessageCount = initialMessageCount
		}
		if maximumMessageCount < minimumMessageCount {
			maximumMessageCount = minimumMessageCount
		}
		initialMessageCount = min(max(initialMessageCount, minimumMessageCount), maximumMessageCount)
		if increaseMessageCount <= 0 {
			increaseMessageCount = 1
		}
	}
	return &sendFlightController{
		initialByteCount:     initialByteCount,
		minimumByteCount:     minimumByteCount,
		maximumByteCount:     maximumByteCount,
		increaseByteCount:    increaseByteCount,
		initialMessageCount:  initialMessageCount,
		minimumMessageCount:  minimumMessageCount,
		maximumMessageCount:  maximumMessageCount,
		increaseMessageCount: increaseMessageCount,
		slowStartDivisor:     slowStartDivisor,
		messageCountByKey:    map[sendSchedulingKey]int{},
	}
}

// Applies one immutable route generation. Every new unreliable generation
// restarts cold because a reconnect or path change can have a different BDP;
// outstanding Transfer bytes remain counted across that transition.
func (self *sendFlightController) applyPolicy(policy transferFlightPolicySnapshot) bool {
	limited := policy.limited && 0 < self.initialByteCount
	if self.generation == policy.generation && self.limited == limited &&
		self.policyByteLimit == policy.byteLimit &&
		self.policyMessageLimit == policy.messageLimit &&
		self.flowReserveEnabled == policy.flowReserve &&
		self.reliableRouteAvailable == policy.reliableRouteAvailable {
		return false
	}
	self.generation = policy.generation
	self.limited = limited
	self.policyByteLimit = policy.byteLimit
	self.policyMessageLimit = policy.messageLimit
	self.flowReserveEnabled = policy.flowReserve
	self.reliableRouteAvailable = policy.reliableRouteAvailable
	self.additiveIncreaseRemainder = 0
	self.slowStartIncreaseRemainder = 0
	self.additiveMessageRemainder = 0
	self.slowStartMessageRemainder = 0
	if limited {
		self.activeMaximumByteCount = self.maximumByteCount
		if 0 < policy.byteLimit {
			self.activeMaximumByteCount = min(
				self.activeMaximumByteCount,
				policy.byteLimit,
			)
		}
		self.activeMinimumByteCount = min(
			self.minimumByteCount,
			self.activeMaximumByteCount,
		)
		self.byteLimit = min(
			max(self.initialByteCount, self.activeMinimumByteCount),
			self.activeMaximumByteCount,
		)
		self.activeMinimumMessageCount = self.minimumMessageCount
		self.activeMaximumMessageCount = self.maximumMessageCount
		if 0 < policy.messageLimit && 0 < self.initialMessageCount {
			self.activeMaximumMessageCount = min(
				self.activeMaximumMessageCount,
				policy.messageLimit,
			)
			self.activeMinimumMessageCount = min(
				self.activeMinimumMessageCount,
				self.activeMaximumMessageCount,
			)
		}
		self.messageLimit = min(
			max(self.initialMessageCount, self.activeMinimumMessageCount),
			self.activeMaximumMessageCount,
		)
		self.slowStart = true
	} else {
		self.byteLimit = 0
		self.activeMinimumByteCount = 0
		self.activeMaximumByteCount = 0
		self.messageLimit = 0
		self.activeMinimumMessageCount = 0
		self.activeMaximumMessageCount = 0
		self.slowStart = false
	}
	return true
}

// Reports whether another original reliable Pack may enter the active flight.
// One Pack is always allowed into an empty flight so a conservative limit can
// never deadlock on a single encoded frame larger than that limit.
func (self *sendFlightController) canSend() bool {
	return self.canSendForKey(sendSchedulingKey{})
}

// canSendForKey exposes one bounded on-demand position to a newly active IP
// flow after the ordinary byte or message flight is full. Bulk traffic uses
// the complete configured flight until a competing flow actually appears;
// at most one reserve message may exist at a time.
func (self *sendFlightController) canSendForKey(key sendSchedulingKey) bool {
	if !self.limited || self.byteCount == 0 && self.messageCount == 0 {
		return true
	}
	if self.byteCount < self.byteLimit &&
		(self.messageLimit <= 0 || self.messageCount < self.messageLimit) {
		return true
	}
	return self.flowReserveEnabled && key.valid && !self.flowReserveInUse &&
		self.messageCountByKey[key] == 0
}

// Adds one original Pack, or a probe of a previously selectively acknowledged
// Pack, to the delivery-unknown byte count.
func (self *sendFlightController) send(byteCount ByteCount) {
	self.sendForKey(byteCount, sendSchedulingKey{})
}

func (self *sendFlightController) sendForKey(
	byteCount ByteCount,
	key sendSchedulingKey,
) bool {
	reserved := self.limited && self.flowReserveEnabled && key.valid &&
		!self.flowReserveInUse &&
		self.messageCountByKey[key] == 0 &&
		(self.byteLimit <= self.byteCount ||
			0 < self.messageLimit && self.messageLimit <= self.messageCount)
	if reserved {
		self.flowReserveInUse = true
	}
	if 0 < byteCount {
		self.byteCount += byteCount
		self.messageCount += 1
		if key.valid {
			self.messageCountByKey[key] += 1
		}
	}
	return reserved
}

// Removes newly delivered bytes and grows the active unreliable window. Cold
// progress uses slow start; after a gap, one acknowledged window adds roughly
// one DATAGRAM payload rather than doubling again.
func (self *sendFlightController) acknowledge(byteCount ByteCount) {
	self.acknowledgeForKey(byteCount, sendSchedulingKey{}, false)
}

func (self *sendFlightController) acknowledgeForKey(
	byteCount ByteCount,
	key sendSchedulingKey,
	reserved bool,
) {
	if byteCount <= 0 {
		return
	}
	acknowledgedByteCount := min(byteCount, self.byteCount)
	self.byteCount -= acknowledgedByteCount
	acknowledgedMessageCount := 0
	if 0 < self.messageCount {
		self.messageCount -= 1
		acknowledgedMessageCount = 1
		if key.valid && 0 < self.messageCountByKey[key] {
			self.messageCountByKey[key] -= 1
			if self.messageCountByKey[key] == 0 {
				delete(self.messageCountByKey, key)
			}
		}
	}
	if reserved {
		self.flowReserveInUse = false
	}
	if !self.limited {
		return
	}
	if self.slowStart {
		if 0 < acknowledgedByteCount && self.byteLimit < self.activeMaximumByteCount {
			numerator := self.slowStartIncreaseRemainder + acknowledgedByteCount
			increaseByteCount := numerator / self.slowStartDivisor
			self.slowStartIncreaseRemainder = numerator % self.slowStartDivisor
			self.byteLimit = min(
				self.activeMaximumByteCount,
				self.byteLimit+increaseByteCount,
			)
		}
		if 0 < acknowledgedMessageCount &&
			self.messageLimit < self.activeMaximumMessageCount {
			numerator := self.slowStartMessageRemainder + acknowledgedMessageCount
			divisor := int(self.slowStartDivisor)
			increaseMessageCount := numerator / divisor
			self.slowStartMessageRemainder = numerator % divisor
			self.messageLimit = min(
				self.activeMaximumMessageCount,
				self.messageLimit+increaseMessageCount,
			)
		}
		return
	}

	if 0 < acknowledgedByteCount && self.byteLimit < self.activeMaximumByteCount {
		numerator := self.additiveIncreaseRemainder +
			self.increaseByteCount*acknowledgedByteCount
		increaseByteCount := numerator / self.byteLimit
		self.additiveIncreaseRemainder = numerator % self.byteLimit
		self.byteLimit = min(
			self.activeMaximumByteCount,
			self.byteLimit+increaseByteCount,
		)
	}
	if 0 < acknowledgedMessageCount && 0 < self.messageLimit &&
		self.messageLimit < self.activeMaximumMessageCount {
		numerator := self.additiveMessageRemainder +
			self.increaseMessageCount*acknowledgedMessageCount
		increaseMessageCount := numerator / self.messageLimit
		self.additiveMessageRemainder = numerator % self.messageLimit
		self.messageLimit = min(
			self.activeMaximumMessageCount,
			self.messageLimit+increaseMessageCount,
		)
	}
}

// Reacts to one Transfer-level loss signal: either a receiver-proven gap or an
// acknowledgement timeout. Returning true means this event reduced at least
// one limit; false still leaves slow start disabled at the configured floors.
// A timeout must participate because a lost tail or lost cumulative Ack has no
// later selective Ack with which to prove a gap, yet continuing to grow after
// that silence recreates the queue burst that lost it.
func (self *sendFlightController) reduceForLoss() bool {
	if !self.limited {
		return false
	}
	self.slowStart = false
	self.additiveIncreaseRemainder = 0
	self.slowStartIncreaseRemainder = 0
	self.additiveMessageRemainder = 0
	self.slowStartMessageRemainder = 0
	reducedByteLimit := max(self.activeMinimumByteCount, self.byteLimit/2)
	reduced := self.byteLimit > reducedByteLimit
	self.byteLimit = reducedByteLimit
	if 0 < self.messageLimit {
		reducedMessageLimit := max(
			self.activeMinimumMessageCount,
			self.messageLimit/2,
		)
		reduced = reduced || self.messageLimit > reducedMessageLimit
		self.messageLimit = reducedMessageLimit
	}
	return reduced
}

// atFloor reports whether loss reductions have pinned the unreliable flight
// limit to its minimum: the carrier is currently proving unable to sustain
// more than the floor, so striping an ordered stream onto it only reorders it.
func (self *sendFlightController) atFloor() bool {
	if !self.limited || self.slowStart {
		return false
	}
	if self.byteLimit <= self.activeMinimumByteCount {
		return true
	}
	return 0 < self.messageLimit && self.messageLimit <= self.activeMinimumMessageCount
}

// forget removes one tracked message from the flight without treating it as
// delivered: the byte and message counts drop, the flow reserve it may hold
// is released, and no limit grows. A timeout is the opposite of delivery
// evidence, so it must not share acknowledge's growth (FLIGHTGATEFIX §13.1).
func (self *sendFlightController) forget(
	byteCount ByteCount,
	key sendSchedulingKey,
	reserved bool,
) {
	self.byteCount -= min(byteCount, self.byteCount)
	if 0 < self.messageCount {
		self.messageCount -= 1
		if key.valid && 0 < self.messageCountByKey[key] {
			self.messageCountByKey[key] -= 1
			if self.messageCountByKey[key] == 0 {
				delete(self.messageCountByKey, key)
			}
		}
	}
	if reserved {
		self.flowReserveInUse = false
	}
}
