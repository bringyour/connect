


type Monitor struct {
	update chan struct{}
}

func NotifyChannel() chan struct{} {
	return update
}

func notifyAll() {
	// close the update channel and create a new one
}





// makes a copy of the list on update
type CallbackList[T] struct {
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

