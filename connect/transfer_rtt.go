package connect

import (
	"fmt"
	"sync"
	"time"

	"container/heap"

	"github.com/golang/glog"

	"bringyour.com/protocol"
)

type rttWindowItem struct {
	sendTime    time.Time
	receiveTime time.Time
	rtt         time.Duration

	heapIndex int
}

func newRttWindowItem(sendTime time.Time, receiveTime time.Time) *rttWindowItem {
	return &rttWindowItem{
		sendTime:    sendTime,
		receiveTime: receiveTime,
		rtt:         receiveTime.Sub(sendTime),
	}
}

type RttWindow struct {
	windowTimeout time.Duration
	rttScale      float32
	minScaledRtt  time.Duration
	maxScaledRtt  time.Duration

	stateLock       sync.Mutex
	window          []*rttWindowItem
	windowTailIndex int
	windowHeadIndex int

	rtts *rttHeap
}

func NewRttWindow(
	windowSize int,
	windowTimeout time.Duration,
	rttScale float32,
	minScaledRtt time.Duration,
	maxScaledRtt time.Duration,
) *RttWindow {
	if windowSize == 0 {
		panic(fmt.Errorf("Window size must non-zero: %d", windowSize))
	}
	window := make([]*rttWindowItem, windowSize)

	return &RttWindow{
		windowTimeout:   windowTimeout,
		rttScale:        rttScale,
		minScaledRtt:    minScaledRtt,
		maxScaledRtt:    maxScaledRtt,
		window:          window,
		windowTailIndex: 0,
		windowHeadIndex: 0,
		rtts:            newRttHeap(),
	}
}

// must be called inside the state lock
func (self *RttWindow) coalesce(windowTime time.Time) {
	windowStartTime := windowTime.Add(-self.windowTimeout)
	for self.windowTailIndex != self.windowHeadIndex {
		item := self.window[self.windowTailIndex]
		if !item.receiveTime.Before(windowStartTime) {
			break
		}
		self.rtts.Remove(item)
		self.window[self.windowTailIndex] = nil
		self.windowTailIndex = (self.windowTailIndex + 1) % len(self.window)
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
	self.closeTag(tag, time.Now())
}

func (self *RttWindow) closeTag(tag *protocol.Tag, receiveTime time.Time) {
	sendTime := time.UnixMilli(int64(tag.SendTime))
	if receiveTime.Before(sendTime) {
		// ignore
		return
	}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.coalesce(receiveTime)

	item := newRttWindowItem(
		sendTime,
		receiveTime,
	)
	self.rtts.Add(item)

	if replaceItem := self.window[self.windowHeadIndex]; replaceItem != nil {
		self.rtts.Remove(replaceItem)
	}
	self.window[self.windowHeadIndex] = item
	self.windowHeadIndex = (self.windowHeadIndex + 1) % len(self.window)
	if self.windowTailIndex == self.windowHeadIndex {
		self.windowTailIndex = (self.windowTailIndex + 1) % len(self.window)
	}
}

// min(max of window * scale, overall max)
func (self *RttWindow) ScaledRtt() time.Duration {
	return self.scaledRtt(time.Now())
}

func (self *RttWindow) scaledRtt(sendTime time.Time) time.Duration {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.coalesce(sendTime)

	useRtt := self.rtts.MeanRtt()
	scaledRtt := min(
		max(
			time.Duration(float32(useRtt/time.Millisecond)*self.rttScale)*time.Millisecond,
			self.minScaledRtt,
		),
		self.maxScaledRtt,
	)
	glog.V(2).Infof("[rtt]scaled=%dms\n", scaledRtt/time.Millisecond)
	return scaledRtt
}

type rttHeap struct {
	items  []*rttWindowItem
	netRtt time.Duration
}

// `heap` is a min heap
func newRttHeap() *rttHeap {
	h := &rttHeap{
		items:  []*rttWindowItem{},
		netRtt: time.Duration(0),
	}
	heap.Init(h)
	return h
}

func (self *rttHeap) Add(item *rttWindowItem) {
	heap.Push(self, item)
	self.netRtt += item.rtt
}

func (self *rttHeap) Remove(item *rttWindowItem) {
	heap.Remove(self, item.heapIndex)
	self.netRtt -= item.rtt
}

func (self *rttHeap) MinRtt() time.Duration {
	n := len(self.items)
	if n == 0 {
		return time.Duration(0)
	}
	maxItem := self.items[n-1]
	return maxItem.rtt
}

func (self *rttHeap) MeanRtt() time.Duration {
	n := len(self.items)
	if n == 0 {
		return 0
	}
	return self.netRtt / time.Duration(n)
}

// `heap.Interface`

func (self *rttHeap) Len() int {
	return len(self.items)
}

func (self *rttHeap) Less(i, j int) bool {
	return self.items[i].rtt < self.items[j].rtt
}

func (self *rttHeap) Swap(i, j int) {
	a := self.items[i]
	b := self.items[j]
	b.heapIndex = i
	self.items[i] = b
	a.heapIndex = j
	self.items[j] = a
}

func (self *rttHeap) Push(x any) {
	item := x.(*rttWindowItem)
	item.heapIndex = len(self.items)
	self.items = append(self.items, item)
}

func (self *rttHeap) Pop() any {
	n := len(self.items)
	item := self.items[n-1]
	self.items[n-1] = nil
	self.items = self.items[0 : n-1]
	return item
}
