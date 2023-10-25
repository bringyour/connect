package connect

import (
	"sync"
	"time"

	"golang.org/x/exp/slices"
)


type Monitor struct {
	mutex sync.Mutex
	notify chan struct{}
}

func NewMonitor() *Monitor {
	return &Monitor{
		notify: make(chan struct{}),
	}
}

func (self *Monitor) NotifyChannel() chan struct{} {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.notify
}

func (self *Monitor) NotifyAll() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	close(self.notify)
	self.notify = make(chan struct{})
}


// makes a copy of the list on update
type CallbackList[T any] struct {
	mutex sync.Mutex
	callbacks []T
}

func NewCallbackList[T any]() *CallbackList[T] {
	return &CallbackList[T]{
		callbacks: []T{},
	}
}

func (self *CallbackList[T]) Get() []T {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.callbacks
}

func (self *CallbackList[T]) Add(callback T) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	i := slices.IndexFunc(self.callbacks, func(c T)(bool) {
		return &c == &callback
	})
	if 0 <= i {
		// already present
		return
	}
	nextCallbacks := slices.Clone(self.callbacks)
	nextCallbacks = append(nextCallbacks, callback)
	self.callbacks = nextCallbacks
}

func  (self *CallbackList[T]) Remove(callback T) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	i := slices.IndexFunc(self.callbacks, func(c T)(bool) {
		return &c == &callback
	})
	if i < 0 {
		// not present
		return
	}
	nextCallbacks := slices.Clone(self.callbacks)
	nextCallbacks = slices.Delete(nextCallbacks, i, i)
	self.callbacks = nextCallbacks
}


// this coordinates and idle shutdown when the shutdown and adding to the work channel are on separate goroutines
type IdleCondition struct {
	mutex sync.Mutex
	modId uint64
	updateOpenCount int
	closed bool
}

func NewIdleCondition() *IdleCondition {
	return &IdleCondition{
		modId: 0,
		updateOpenCount: 0,
		closed: false,
	}
}

func (self *IdleCondition) Checkpoint() uint64 {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.modId
}

func (self *IdleCondition) Close(checkpointId uint64) bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.modId != checkpointId {
		return false
	}
	if 0 < self.updateOpenCount {
		return false
	}
	self.closed = true
	return true
}

func (self *IdleCondition) UpdateOpen() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	if self.closed {
		return false
	}
	self.modId += 1
	self.updateOpenCount += 1
	return true
}

func (self *IdleCondition) UpdateClose() {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	self.updateOpenCount -= 1
}


func MinTime(a time.Time, bs ...time.Time) time.Time {
	min := a
	for _, b := range bs {
		if b.Before(min) {
			min = b
		}
	}
	return min
}

