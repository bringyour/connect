package connect

import (
	"sync"
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
	return notify
}

func (self *Monitor) notifyAll() {
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

func (self *CallbackList[T]) get() []T {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return callbacks
}

func (self *CallbackList[T]) add(callback T) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	i := slices.Index(self.callbacks, callback)
	if 0 <= i {
		// already present
		return
	}
	nextCallbacks := slices.Clone(self.callbacks)
	nextCallbacks = append(nextCallbacks, callback)
	self.callbacks = nextCallbacks
}

func  (self *CallbackList[T]) remove(callback T) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	i := slices.Index(self.callbacks, callback)
	if i < 0 {
		// not present
		return
	}
	nextCallbacks := slices.Clone(self.callbacks)
	nextCallbacks = slices.Delete(nextCallbacks, i, i)
	self.callbacks = nextCallbacks
}

