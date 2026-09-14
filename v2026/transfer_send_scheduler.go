package connect

import "sync"

// sendSchedulingKey keeps the exact five-tuple local. The scheduler consumes
// it before assigning sequence numbers; after logical-lane negotiation, only
// its bounded stable lane hash can enter sequence identity and the wire Pack.
type sendSchedulingKey struct {
	ipFlow ipPacketFlowKey
	valid  bool
}

func ipSendSchedulingKey(ipPath *IpPath) sendSchedulingKey {
	key, ok := ipPacketFlowKeyFromPath(ipPath)
	return sendSchedulingKey{ipFlow: key, valid: ok}
}

type sendSchedulingKeyOption struct {
	key sendSchedulingKey
}

func scheduleIpFlow(ipPath *IpPath) sendSchedulingKeyOption {
	return sendSchedulingKeyOption{key: ipSendSchedulingKey(ipPath)}
}

// sendPackAdmission keeps the complete pre-sequence working set bounded even
// while SendSequence drains its channel into per-flow queues. One valid IP
// flow may occupy at most capacity-1 slots, leaving one admission for a newly
// active flow. A capacity of one retains the ordinary one-item behavior.
type sendPackAdmission struct {
	mutex    sync.Mutex
	capacity int
	count    int
	byKey    map[sendSchedulingKey]int
	notify   chan struct{}
	closed   bool
}

func newSendPackAdmission(capacity int) *sendPackAdmission {
	if capacity <= 0 {
		return nil
	}
	return &sendPackAdmission{
		capacity: capacity,
		byKey:    map[sendSchedulingKey]int{},
		notify:   make(chan struct{}),
	}
}

// tryAcquire returns a level snapshot and the generation notification a
// blocked caller should wait on before retrying.
func (self *sendPackAdmission) tryAcquire(
	key sendSchedulingKey,
) (acquired bool, closed bool, notify <-chan struct{}) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.closed {
		return false, true, self.notify
	}
	keyLimit := self.capacity
	if key.valid && 1 < keyLimit {
		keyLimit -= 1
	}
	if self.count < self.capacity && self.byKey[key] < keyLimit {
		self.count += 1
		self.byKey[key] += 1
		return true, false, self.notify
	}
	return false, false, self.notify
}

func (self *sendPackAdmission) broadcastWithLock() {
	close(self.notify)
	self.notify = make(chan struct{})
}

func (self *sendPackAdmission) release(key sendSchedulingKey) {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.byKey[key] <= 0 || self.count <= 0 {
		return
	}
	self.count -= 1
	self.byKey[key] -= 1
	if self.byKey[key] == 0 {
		delete(self.byKey, key)
	}
	self.broadcastWithLock()
}

func (self *sendPackAdmission) close() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.closed {
		return
	}
	self.closed = true
	self.broadcastWithLock()
}

type sendPackFlowQueue struct {
	key   sendSchedulingKey
	packs []*SendPack
}

// sendPackScheduler retains FIFO order within each flow and selects active
// flows round-robin. It is owned exclusively by one SendSequence goroutine.
type sendPackScheduler struct {
	flows  map[sendSchedulingKey]*sendPackFlowQueue
	active []*sendPackFlowQueue
	// order retains exact ingress order for carriers that do not opt into flow
	// isolation. Bounded admission keeps the linear removal a small fixed cost.
	order []*SendPack
	count int
}

func newSendPackScheduler() *sendPackScheduler {
	return &sendPackScheduler{flows: map[sendSchedulingKey]*sendPackFlowQueue{}}
}

func (self *sendPackScheduler) Len() int {
	return self.count
}

// FifoHead exposes the carrier-neutral oldest Pack for flow-isolation
// telemetry. The scheduler retains ownership; callers must not mutate it.
func (self *sendPackScheduler) FifoHead() *SendPack {
	if len(self.order) == 0 {
		return nil
	}
	return self.order[0]
}

func (self *sendPackScheduler) Push(sendPack *SendPack) {
	key := sendPack.schedulingKey
	flow := self.flows[key]
	if flow == nil {
		flow = &sendPackFlowQueue{key: key}
		self.flows[key] = flow
		self.active = append(self.active, flow)
	}
	flow.packs = append(flow.packs, sendPack)
	self.order = append(self.order, sendPack)
	self.count += 1
}

func (self *sendPackScheduler) PushFront(sendPack *SendPack) {
	key := sendPack.schedulingKey
	flow := self.flows[key]
	if flow == nil {
		flow = &sendPackFlowQueue{key: key}
		self.flows[key] = flow
		self.active = append(self.active, flow)
	}
	flow.packs = append(flow.packs, nil)
	copy(flow.packs[1:], flow.packs[:len(flow.packs)-1])
	flow.packs[0] = sendPack
	self.order = append(self.order, nil)
	copy(self.order[1:], self.order[:len(self.order)-1])
	self.order[0] = sendPack
	self.count += 1
}

func (self *sendPackScheduler) removeActive(index int) {
	copy(self.active[index:], self.active[index+1:])
	self.active[len(self.active)-1] = nil
	self.active = self.active[:len(self.active)-1]
}

// Removes one selected Pack from the carrier-neutral ingress order.
func (self *sendPackScheduler) removeOrder(sendPack *SendPack) {
	for orderIndex, orderedSendPack := range self.order {
		if orderedSendPack == sendPack {
			copy(self.order[orderIndex:], self.order[orderIndex+1:])
			self.order[len(self.order)-1] = nil
			self.order = self.order[:len(self.order)-1]
			return
		}
	}
}

func (self *sendPackScheduler) takeFromFlow(
	flow *sendPackFlowQueue,
	activeIndex int,
	rotate bool,
) *SendPack {
	sendPack := flow.packs[0]
	copy(flow.packs, flow.packs[1:])
	flow.packs[len(flow.packs)-1] = nil
	flow.packs = flow.packs[:len(flow.packs)-1]
	self.removeOrder(sendPack)
	self.count -= 1
	if len(flow.packs) == 0 {
		delete(self.flows, flow.key)
		self.removeActive(activeIndex)
	} else if rotate && activeIndex < len(self.active)-1 {
		self.removeActive(activeIndex)
		self.active = append(self.active, flow)
	}
	return sendPack
}

// Selects the oldest Pack without changing cross-flow order. This preserves
// the former channel behavior for carriers without a dedicated flow reserve.
func (self *sendPackScheduler) TakeFifoEligible(
	eligible func(*SendPack) bool,
) *SendPack {
	if len(self.order) == 0 {
		return nil
	}
	sendPack := self.order[0]
	if !eligible(sendPack) {
		return nil
	}
	flow := self.flows[sendPack.schedulingKey]
	if flow == nil || len(flow.packs) == 0 || flow.packs[0] != sendPack {
		return nil
	}
	for activeIndex, activeFlow := range self.active {
		if activeFlow == flow {
			return self.takeFromFlow(flow, activeIndex, false)
		}
	}
	return nil
}

// TakeEligible selects the first eligible active flow, then rotates that flow
// behind every other currently active flow.
func (self *sendPackScheduler) TakeEligible(
	eligible func(*SendPack) bool,
) *SendPack {
	for activeIndex, flow := range self.active {
		if eligible(flow.packs[0]) {
			return self.takeFromFlow(flow, activeIndex, true)
		}
	}
	return nil
}

// TakeSameFlow supports the existing zero-wait two-frame coalescer without
// combining unrelated latency and bulk flows into one Transfer item.
func (self *sendPackScheduler) TakeSameFlow(key sendSchedulingKey) *SendPack {
	flow := self.flows[key]
	if flow == nil {
		return nil
	}
	for activeIndex, activeFlow := range self.active {
		if activeFlow == flow {
			return self.takeFromFlow(flow, activeIndex, false)
		}
	}
	return nil
}

func (self *sendPackScheduler) HasEligible(
	eligible func(*SendPack) bool,
) bool {
	for _, flow := range self.active {
		if eligible(flow.packs[0]) {
			return true
		}
	}
	return false
}

func (self *sendPackScheduler) Drain(dispose func(*SendPack)) {
	for _, flow := range self.active {
		for _, sendPack := range flow.packs {
			dispose(sendPack)
		}
	}
	self.flows = map[sendSchedulingKey]*sendPackFlowQueue{}
	self.active = nil
	self.order = nil
	self.count = 0
}
