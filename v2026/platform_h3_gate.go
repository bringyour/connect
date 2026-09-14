package connect

import (
	"context"
	"strings"
	"sync"
)

// platformH3Gate allows one H3-family socket generation per
// PlatformTransport. A waiting higher-priority family cancels a lower-priority
// attempt/connection; equal families wait without oscillating.
type platformH3Gate struct {
	owner *PlatformTransport

	mutex        sync.Mutex
	activeMode   TransportMode
	activeCancel context.CancelFunc
	generation   uint64
	waiters      map[TransportMode]int
	notify       chan struct{}
}

func newPlatformH3Gate(owner *PlatformTransport) *platformH3Gate {
	return &platformH3Gate{
		owner:   owner,
		waiters: map[TransportMode]int{},
		notify:  make(chan struct{}),
	}
}

func (self *platformH3Gate) notifyChangedLocked() {
	close(self.notify)
	self.notify = make(chan struct{})
}

func (self *platformH3Gate) bestWaitingModeLocked() TransportMode {
	best := TransportModeNone
	for mode, count := range self.waiters {
		if count <= 0 {
			continue
		}
		if best == TransportModeNone || self.owner.isBetterMode(mode, best) ||
			(self.owner.modePreference(mode) == self.owner.modePreference(best) &&
				strings.Compare(string(mode), string(best)) < 0) {
			best = mode
		}
	}
	return best
}

func (self *platformH3Gate) removeWaiterLocked(mode TransportMode) {
	if count := self.waiters[mode]; count <= 1 {
		delete(self.waiters, mode)
	} else {
		self.waiters[mode] = count - 1
	}
}

func (self *platformH3Gate) Acquire(
	ctx context.Context,
	mode TransportMode,
) (context.Context, func(), bool) {
	self.mutex.Lock()
	self.waiters[mode] += 1
	self.notifyChangedLocked()
	self.mutex.Unlock()

	for {
		self.mutex.Lock()
		if ctx.Err() != nil {
			self.removeWaiterLocked(mode)
			self.notifyChangedLocked()
			self.mutex.Unlock()
			return nil, nil, false
		}
		if self.activeMode != TransportModeNone &&
			self.owner.isBetterMode(mode, self.activeMode) {
			self.activeCancel()
		}
		if self.activeMode == TransportModeNone && self.bestWaitingModeLocked() == mode {
			self.removeWaiterLocked(mode)
			attemptCtx, attemptCancel := context.WithCancel(ctx)
			self.generation += 1
			generation := self.generation
			self.activeMode = mode
			self.activeCancel = attemptCancel
			self.notifyChangedLocked()
			self.mutex.Unlock()

			var once sync.Once
			release := func() {
				once.Do(func() {
					attemptCancel()
					self.mutex.Lock()
					if self.generation == generation {
						self.activeMode = TransportModeNone
						self.activeCancel = nil
						self.notifyChangedLocked()
					}
					self.mutex.Unlock()
				})
			}
			return attemptCtx, release, true
		}
		notify := self.notify
		self.mutex.Unlock()

		select {
		case <-ctx.Done():
			self.mutex.Lock()
			self.removeWaiterLocked(mode)
			self.notifyChangedLocked()
			self.mutex.Unlock()
			return nil, nil, false
		case <-notify:
		}
	}
}
