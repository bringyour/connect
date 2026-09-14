// Stream-manager tests use exact lifecycle barriers instead of timing or
// assuming an asynchronous receive callback has already published its work.
package connect

import (
	"sync"
	"testing"
	"time"
)

// One expected publication holds its lifecycle worker until the assertion has
// observed the indexed sequence. It is safe between the test and worker.
type streamOpenTestPublication struct {
	published   chan struct{}
	release     chan struct{}
	releaseOnce sync.Once
}

// Waits until the sequence is indexed while its worker remains at the test
// seam.
func (self *streamOpenTestPublication) wait(t *testing.T) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-self.published:
	case <-timer.C:
		t.Fatal("stream sequence was not published")
	}
}

// Lets the lifecycle worker activate and run after publication assertions.
func (self *streamOpenTestPublication) resume() {
	self.releaseOnce.Do(func() {
		close(self.release)
	})
}

// Tracks ordered publication and completion expectations by stream id. Its
// hooks are safe on independent per-stream lifecycle workers.
type streamOpenTestTracker struct {
	stateLock    sync.Mutex
	publications map[Id][]*streamOpenTestPublication
	completions  map[Id][]chan struct{}
	removals     map[Id][]chan struct{}
}

// Installs test-only observation hooks on one StreamBuffer.
func newStreamOpenTestTracker(streamBuffer *StreamBuffer) *streamOpenTestTracker {
	tracker := &streamOpenTestTracker{
		publications: map[Id][]*streamOpenTestPublication{},
		completions:  map[Id][]chan struct{}{},
		removals:     map[Id][]chan struct{}{},
	}
	streamBuffer.afterStreamSequencePublishForTest = tracker.observePublication
	streamBuffer.afterStreamOpenProcessedForTest = tracker.observeCompletion
	streamBuffer.afterStreamSequenceRemoveForTest = tracker.observeRemoval
	return tracker
}

// Registers exact index removal before delivering a close or reset control.
func (self *streamOpenTestTracker) expectRemoval(streamId Id) <-chan struct{} {
	removal := make(chan struct{})
	self.stateLock.Lock()
	self.removals[streamId] = append(self.removals[streamId], removal)
	self.stateLock.Unlock()
	return removal
}

// Registers one publication before delivering the corresponding control.
func (self *streamOpenTestTracker) expectPublication(streamId Id) *streamOpenTestPublication {
	publication := &streamOpenTestPublication{
		published: make(chan struct{}),
		release:   make(chan struct{}),
	}
	self.stateLock.Lock()
	self.publications[streamId] = append(self.publications[streamId], publication)
	self.stateLock.Unlock()
	return publication
}

// Registers terminal processing for an exact refresh that reuses a sequence
// and therefore has no new publication edge.
func (self *streamOpenTestTracker) expectCompletion(streamId Id) <-chan struct{} {
	completion := make(chan struct{})
	self.stateLock.Lock()
	self.completions[streamId] = append(self.completions[streamId], completion)
	self.stateLock.Unlock()
	return completion
}

// Holds a registered publication after indexing and before activation.
func (self *streamOpenTestTracker) observePublication(sequence *StreamSequence) {
	var publication *streamOpenTestPublication
	self.stateLock.Lock()
	if publications := self.publications[sequence.streamId]; 0 < len(publications) {
		publication = publications[0]
		if len(publications) == 1 {
			delete(self.publications, sequence.streamId)
		} else {
			self.publications[sequence.streamId] = publications[1:]
		}
	}
	self.stateLock.Unlock()
	if publication != nil {
		close(publication.published)
		select {
		case <-publication.release:
		case <-sequence.ctx.Done():
		}
	}
}

// Signals a registered request after all managed state is released.
func (self *streamOpenTestTracker) observeCompletion(request *streamOpenRequest) {
	var completion chan struct{}
	self.stateLock.Lock()
	if completions := self.completions[request.streamId]; 0 < len(completions) {
		completion = completions[0]
		if len(completions) == 1 {
			delete(self.completions, request.streamId)
		} else {
			self.completions[request.streamId] = completions[1:]
		}
	}
	self.stateLock.Unlock()
	if completion != nil {
		close(completion)
	}
}

// Signals after the exact sequence pointer leaves both StreamBuffer indexes.
func (self *streamOpenTestTracker) observeRemoval(sequence *StreamSequence) {
	var removal chan struct{}
	self.stateLock.Lock()
	if removals := self.removals[sequence.streamId]; 0 < len(removals) {
		removal = removals[0]
		if len(removals) == 1 {
			delete(self.removals, sequence.streamId)
		} else {
			self.removals[sequence.streamId] = removals[1:]
		}
	}
	self.stateLock.Unlock()
	if removal != nil {
		close(removal)
	}
}

// Waits for exact terminal processing with an owned timeout resource.
func waitForStreamOpenTestCompletion(t *testing.T, completion <-chan struct{}) {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	select {
	case <-completion:
	case <-timer.C:
		t.Fatal("stream open lifecycle did not finish")
	}
}
