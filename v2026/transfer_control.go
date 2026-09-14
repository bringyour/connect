package connect

import (
	"context"
	"sync"

	"github.com/urnetwork/connect/v2026/protocol"
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
	workers *lifecycleAdmission
	// Nil test barriers expose owned-worker completion and join entry.
	beforeWorkerDoneForTest func()
	beforeCloseWaitForTest  func()

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
		workers:   newLifecycleAdmission(),
		syncCount: 0,
	}
}

// startWorker admits and starts one retry or supersession worker.
func (self *ControlSync) startWorker(run func()) bool {
	if !self.workers.start() {
		return false
	}
	go func() {
		defer self.workers.finish()
		HandleError(run, self.cancel)
		if self.beforeWorkerDoneForTest != nil {
			self.beforeWorkerDoneForTest()
		}
	}()
	return true
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
	if !self.startWorker(func() {
		defer handleCancel()

		for {
			select {
			case <-notify:
			case <-handleCtx.Done():
				return
			}
			// re-subscribe and read syncCount in the same locked scope. the
			// notify channel is closed by `NotifyAll`, so without the
			// re-subscribe a wake that does not exit the loop would re-select
			// the already closed channel and hot spin. today the only notifier
			// (above) always bumps syncCount first, so this loop always exits on
			// its first wake — a second notifier that did not would spin
			done := false
			func() {
				self.sendLock.Lock()
				defer self.sendLock.Unlock()
				notify = self.monitor.NotifyChannel()
				done = syncIndex != self.syncCount
			}()
			if done {
				return
			}
		}
	}) {
		MessagePoolReturn(frame.MessageBytes)
		handleCancel()
		return
	}

	var controlSync func(*protocol.Frame)
	controlSync = func(updatedFrame *protocol.Frame) {
		// handleCtx must OUTLIVE a successful enqueue: the queued frame carries
		// Ctx(handleCtx), and the ack-error path re-enters controlSync guarded
		// by the same ctx. A defer here canceled the ctx the moment the enqueue
		// succeeded, which (a) doomed the queued frame at the next sequence
		// teardown and (b) made every retry exit immediately as done — an
		// enqueued-then-nacked control message (e.g. a contract close raced by
		// transport churn) was permanently lost. Cancel instead on terminal
		// exits and in the ack-success callback.

		defer func() {
			self.sendLock.Lock()
			defer self.sendLock.Unlock()
			if self.syncCount == syncIndex {
				if self.client.log.V(2).Enabled() {
					self.client.log.Infof("[control][%d]stop sync for scope = %s\n", syncIndex, self.scopeTag)
				}
			} else {
				if self.client.log.V(2).Enabled() {
					self.client.log.Infof("[control][%d]replace sync for scope = %s\n", syncIndex, self.scopeTag)
				}
			}
		}()

		for {
			if self.client.log.V(2).Enabled() {
				self.client.log.Infof("[control][%d]start sync for scope = %s\n", syncIndex, self.scopeTag)
			}

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

				updatedFrameCopy := &protocol.Frame{
					MessageType:  updatedFrame.MessageType,
					MessageBytes: MessagePoolShareReadOnly(updatedFrame.MessageBytes),
				}
				success, err = self.client.SendWithTimeoutDetailed(
					updatedFrameCopy,
					ControlId,
					func(err error) {
						if err == nil {
							safeAckCallback(nil)
							MessagePoolReturn(updatedFrame.MessageBytes)
							// the sync is complete: release the watcher and ctx
							handleCancel()
						} else {
							if !self.startWorker(func() {
								controlSync(updatedFrame)
							}) {
								MessagePoolReturn(updatedFrame.MessageBytes)
								handleCancel()
							}
						}
					},
					-1,
					Ctx(handleCtx),
				)
				if !success {
					// the send did not accept the frame (no enqueue), so no ack will
					// ever fire for the copy: undo its share or the base return alone
					// only ever decrements the count to 1 and the buffer leaks
					MessagePoolReturn(updatedFrameCopy.MessageBytes)
				}
			}()
			if done {
				MessagePoolReturn(updatedFrame.MessageBytes)
				handleCancel()
				return
			}
			if success {
				// the queued frame and its retry path own handleCtx now: the
				// ack callback cancels on success, or re-enters controlSync on
				// error
				return
			}
			if err != nil {
				// only stop when the context or client is done
				select {
				case <-handleCtx.Done():
					MessagePoolReturn(frame.MessageBytes)
					return
				case <-self.client.Done():
					MessagePoolReturn(frame.MessageBytes)
					handleCancel()
					return
				default:
				}
			}
			// else try again
			if updateFrame != nil {
				f := updateFrame()
				if f != updatedFrame {
					MessagePoolReturn(updatedFrame.MessageBytes)
					updatedFrame = f
				}
			}
		}
	}

	frameCopy := &protocol.Frame{
		MessageType:  frame.MessageType,
		MessageBytes: MessagePoolShareReadOnly(frame.MessageBytes),
	}
	success := self.client.SendWithTimeout(
		frameCopy,
		ControlId,
		func(err error) {
			if err == nil {
				safeAckCallback(nil)
				MessagePoolReturn(frame.MessageBytes)
				// the sync is complete: release the watcher and ctx
				handleCancel()
			} else {
				if !self.startWorker(func() {
					controlSync(frame)
				}) {
					MessagePoolReturn(frame.MessageBytes)
					handleCancel()
				}
			}
		},
		0,
		Ctx(handleCtx),
	)
	if success {
		return
	}

	// the non-blocking send did not accept the frame, so no ack will ever fire for
	// the copy: undo its share (see the retry loop above for the same rule)
	MessagePoolReturn(frameCopy.MessageBytes)

	if !self.startWorker(func() {
		controlSync(frame)
	}) {
		MessagePoolReturn(frame.MessageBytes)
		handleCancel()
	}
}

// Close prevents later retry workers and cancels every active generation.
func (self *ControlSync) Close() {
	self.cancel()
	self.workers.close()
}

// closeAndWait joins every retry worker admitted before Close.
func (self *ControlSync) closeAndWait(ctx context.Context) error {
	self.Close()
	if self.beforeCloseWaitForTest != nil {
		self.beforeCloseWaitForTest()
	}
	return waitForLifecycleDone(ctx, self.workers.Done(), "control sync workers")
}
