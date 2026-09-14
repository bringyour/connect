// Concurrent lifecycle admission closes one exact producer boundary and
// publishes completion after every producer admitted before it has exited.
package connect

import "sync"

// lifecycleAdmission is an in-process close-and-join gate. start and finish
// may run concurrently; close prevents later starts without waiting.
type lifecycleAdmission struct {
	stateLock   sync.Mutex
	open        bool
	activeCount int
	done        chan struct{}
	// Nil test barrier pauses a producer before the close/admission lock.
	beforeStartLockForTest func()
}

// newLifecycleAdmission creates an open gate with an unclosed completion
// signal.
func newLifecycleAdmission() *lifecycleAdmission {
	return &lifecycleAdmission{
		open: true,
		done: make(chan struct{}),
	}
}

// start admits one producer unless close already won the state boundary.
func (self *lifecycleAdmission) start() bool {
	if self.beforeStartLockForTest != nil {
		self.beforeStartLockForTest()
	}
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if !self.open {
		return false
	}
	self.activeCount += 1
	return true
}

// finish releases one admitted producer and publishes terminal completion
// when it was the last producer behind a closed gate.
func (self *lifecycleAdmission) finish() {
	self.finishAndIdle()
}

// finishAndIdle releases one producer and reports whether the still-open gate
// became idle. Owners use that exact edge to reclaim healthy keyed gates
// without racing a concurrent close or a new admission.
func (self *lifecycleAdmission) finishAndIdle() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.activeCount <= 0 {
		panic("lifecycle admission finished without a producer")
	}
	self.activeCount -= 1
	if !self.open && self.activeCount == 0 {
		close(self.done)
	}
	return self.open && self.activeCount == 0
}

// close prevents later admission. It deliberately does not wait, so callers
// can cancel owned work and release other state before joining done.
func (self *lifecycleAdmission) close() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if !self.open {
		return
	}
	self.open = false
	if self.activeCount == 0 {
		close(self.done)
	}
}

// Done closes after close and the final admitted producer finish.
func (self *lifecycleAdmission) Done() <-chan struct{} {
	return self.done
}
