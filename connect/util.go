package connect

import (
	"sync"

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

func NewCallbackList[T any]() *CallbackList[T] {
	return &CallbackList[T]{
		callbacks: []T{},
	}
}

func (self *CallbackList[T]) get() []T {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.callbacks
}

func (self *CallbackList[T]) add(callback T) {
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

func  (self *CallbackList[T]) remove(callback T) {
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

