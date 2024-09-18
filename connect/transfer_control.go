package connect

import (
	"context"
	"sync"

	"github.com/golang/glog"

	"bringyour.com/protocol"
)

// control sync is a pattern to sync control messages between the server and client
// it ensures:
// - control messages are sent in order
// - only the latest message per scope is retried.
//   Create one `ControlSync` object per scope.
// - if a send fails due to ack timeout or other local error, the send is retried

type ControlSync struct {
	ctx    context.Context
	cancel context.CancelFunc

	client   *Client
	scopeTag string

	monitor *Monitor

	sendLock  sync.Mutex
	syncCount uint64
}

func NewControlSync(ctx context.Context, client *Client, scopeTag string) *ControlSync {
	cancelCtx, cancel := context.WithCancel(ctx)

	return &ControlSync{
		ctx:       cancelCtx,
		cancel:    cancel,
		client:    client,
		scopeTag:  scopeTag,
		monitor:   NewMonitor(),
		syncCount: 0,
	}
}

func (self *ControlSync) Send(frame *protocol.Frame, updateFrame func() *protocol.Frame, ackCallback AckFunction) {
	// 1. try to send non-blocking
	// 2. if fails, send blocking with no timeout
	// 3. keep retying on error until the handle context or client is closed

	safeAckCallback := func(err error) {
		if ackCallback != nil {
			HandleError(func() {
				ackCallback(err)
			})
		}
	}

	handleCtx, handleCancel := context.WithCancel(self.ctx)

	self.sendLock.Lock()
	defer self.sendLock.Unlock()

	self.syncCount += 1
	syncIndex := self.syncCount

	notify := self.monitor.NotifyAll()
	go func() {
		defer handleCancel()

		for {
			done := false
			select {
			case <-notify:
				func() {
					self.sendLock.Lock()
					defer self.sendLock.Unlock()
					done = syncIndex != self.syncCount
				}()
			case <-handleCtx.Done():
				done = true
			}

			if done {
				return
			}
		}
	}()

	var controlSync func()
	controlSync = func() {
		defer handleCancel()

		defer func() {
			self.sendLock.Lock()
			defer self.sendLock.Unlock()
			if self.syncCount == syncIndex {
				glog.V(2).Infof("[control][%d]stop sync for scope = %s\n", syncIndex, self.scopeTag)
			} else {
				glog.V(2).Infof("[control][%d]replace sync for scope = %s\n", syncIndex, self.scopeTag)
			}
		}()

		updatedFrame := frame

		for {
			glog.V(2).Infof("[control][%d]start sync for scope = %s\n", syncIndex, self.scopeTag)

			done := false
			success := false
			var err error
			func() {
				self.sendLock.Lock()
				defer self.sendLock.Unlock()

				select {
				case <-handleCtx.Done():
					done = true
				default:
					done = syncIndex != self.syncCount
				}

				if done {
					return
				}

				success, err = self.client.SendWithTimeoutDetailed(
					updatedFrame,
					DestinationId(ControlId),
					func(err error) {
						if err == nil {
							safeAckCallback(err)
						} else {
							go controlSync()
						}
					},
					-1,
					Ctx(handleCtx),
				)
			}()
			if done || success {
				return
			}
			if err != nil {
				// only stop when the context or client is done
				select {
				case <-handleCtx.Done():
					return
				case <-self.client.Done():
					return
				default:
				}
			}
			// else try again
			if updateFrame != nil {
				updatedFrame = updateFrame()
			}
		}
	}

	success := self.client.SendWithTimeout(
		frame,
		DestinationId(ControlId),
		func(err error) {
			if err == nil {
				safeAckCallback(err)
			} else {
				go controlSync()
			}
		},
		0,
		Ctx(handleCtx),
	)
	if success {
		return
	}

	go controlSync()
}

func (self *ControlSync) Close() {
	self.cancel()
}
