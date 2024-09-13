package connect

import (
	"context"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"
	// "fmt"
	// "runtime/debug"
	// "strings"
	// "encoding/json"
	// "reflect"
	mathrand "math/rand"
)

type Monitor struct {
	mutex  sync.Mutex
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

func (self *Monitor) NotifyAll() chan struct{} {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	close(self.notify)
	self.notify = make(chan struct{})
	return self.notify
}

// makes a copy of the list on update
type CallbackList[T any] struct {
	mutex sync.Mutex
	// `callbacks` and `callbackIds` are parallel arrays
	callbacks      []T
	callbackIds    []int
	nextCallbackId int
}

func NewCallbackList[T any]() *CallbackList[T] {
	return &CallbackList[T]{
		callbacks:      []T{},
		callbackIds:    []int{},
		nextCallbackId: 0,
	}
}

func (self *CallbackList[T]) Get() []T {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	return self.callbacks
}

func (self *CallbackList[T]) Add(callback T) int {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	callbackId := self.nextCallbackId
	self.nextCallbackId += 1

	nextCallbacks := slices.Clone(self.callbacks)
	nextCallbacks = append(nextCallbacks, callback)
	self.callbacks = nextCallbacks

	nextCallbackIds := slices.Clone(self.callbackIds)
	nextCallbackIds = append(nextCallbackIds, callbackId)
	self.callbackIds = nextCallbackIds

	return callbackId
}

func (self *CallbackList[T]) Remove(callbackId int) {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	i, found := slices.BinarySearch(self.callbackIds, callbackId)
	if !found {
		// not present
		return
	}

	nextCallbacks := slices.Clone(self.callbacks)
	nextCallbacks = slices.Delete(nextCallbacks, i, i+1)
	self.callbacks = nextCallbacks

	nextCallbackIds := slices.Clone(self.callbackIds)
	nextCallbackIds = slices.Delete(nextCallbackIds, i, i+1)
	self.callbackIds = nextCallbackIds
}

// this coordinates and idle shutdown when the shutdown and adding to the work channel are on separate goroutines
type IdleCondition struct {
	mutex           *sync.Mutex
	condition       *sync.Cond
	modId           uint64
	updateOpenCount int
	closed          bool
}

func NewIdleCondition() *IdleCondition {
	mutex := &sync.Mutex{}
	condition := sync.NewCond(mutex)
	return &IdleCondition{
		mutex:           mutex,
		condition:       condition,
		modId:           0,
		updateOpenCount: 0,
		closed:          false,
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

func (self *IdleCondition) WaitForClose() bool {
	self.mutex.Lock()
	defer self.mutex.Unlock()
	for 0 < self.updateOpenCount {
		self.condition.Wait()
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
	self.condition.Signal()
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

type Event struct {
	ctx    context.Context
	cancel context.CancelFunc
}

func NewEvent() *Event {
	return NewEventWithContext(context.Background())
}

func NewEventWithContext(ctx context.Context) *Event {
	cancelCtx, cancel := context.WithCancel(ctx)
	return &Event{
		ctx:    cancelCtx,
		cancel: cancel,
	}
}

func (self *Event) Ctx() context.Context {
	return self.ctx
}

func (self *Event) Set() {
	self.cancel()
}

func (self *Event) IsSet() bool {
	select {
	case <-self.ctx.Done():
		return true
	default:
		return false
	}
}

func (self *Event) WaitForSet(timeout time.Duration) bool {
	select {
	case <-self.ctx.Done():
		return true
	case <-time.After(timeout):
		return false
	}
}

func (self *Event) SetOnSignals(signalValues ...syscall.Signal) func() {
	stopSignal := make(chan os.Signal, len(signalValues))
	for _, signalValue := range signalValues {
		signal.Notify(stopSignal, signalValue)
	}
	go func() {
		for {
			select {
			case _, ok := <-stopSignal:
				if !ok {
					return
				}
				self.Set()
			}
		}
	}()
	return func() {
		signal.Stop(stopSignal)
		close(stopSignal)
	}
}

func WeightedShuffle[T comparable](values []T, weights map[T]float32) {
	WeightedShuffleWithEntropy[T](values, weights, 0)
}

func WeightedShuffleWithEntropy[T comparable](values []T, weights map[T]float32, entropy float32) {
	mathrand.Shuffle(len(values), func(i int, j int) {
		values[i], values[j] = values[j], values[i]
	})

	n := len(values)
	for i := 0; i < n-1; i += 1 {
		j := func() int {
			var net float32
			net = 0
			for j := i; j < n; j += 1 {
				net += weights[values[j]]
			}
			r := mathrand.Float32()
			rnet := r * net
			net = entropy * net
			for j := i; j < n; j += 1 {
				net += weights[values[j]]
				if rnet < net {
					return j
				}
			}
			// zero weights, use the last value
			return n - 1
		}()
		values[i], values[j] = values[j], values[i]
	}
}

type Reconnect struct {
	startTime  time.Time
	minTimeout time.Duration
}

func NewReconnect(minTimeout time.Duration) *Reconnect {
	return &Reconnect{
		startTime:  time.Now(),
		minTimeout: minTimeout,
	}
}

func (self *Reconnect) After() <-chan time.Time {
	timeout := self.minTimeout - time.Now().Sub(self.startTime)
	if timeout <= 0 {
		c := make(chan time.Time)
		close(c)
		return c
	} else {
		return time.After(timeout)
	}
}
