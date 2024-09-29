package connect

import (
	"container/heap"
	"sync"
)

type transferQueueItem interface {
	MessageId() Id
	MessageByteCount() ByteCount
	SequenceNumber() uint64
	HeapIndex() int
	SetHeapIndex(int)
	MaxHeapIndex() int
	SetMaxHeapIndex(int)
}

type transferItem struct {
	messageId        Id
	messageByteCount ByteCount
	sequenceNumber   uint64

	// the index of the item in the heap
	heapIndex int
	// the index of the item in the max heap
	maxHeapIndex int
}

// transferQueueItem implementation

func (self *transferItem) MessageId() Id {
	return self.messageId
}

func (self *transferItem) MessageByteCount() ByteCount {
	return self.messageByteCount
}

func (self *transferItem) SequenceNumber() uint64 {
	return self.sequenceNumber
}

func (self *transferItem) HeapIndex() int {
	return self.heapIndex
}

func (self *transferItem) SetHeapIndex(heapIndex int) {
	self.heapIndex = heapIndex
}

func (self *transferItem) MaxHeapIndex() int {
	return self.maxHeapIndex
}

func (self *transferItem) SetMaxHeapIndex(maxHeapIndex int) {
	self.maxHeapIndex = maxHeapIndex
}

type TransferQueueCmpFunction[T transferQueueItem] func(a T, b T) int

// ordered by sequenceNumber
type transferQueue[T transferQueueItem] struct {
	orderedItems []T
	maxHeap      *transferQueueMaxHeap[T]
	// message_id -> item
	messageIdItems      map[Id]T
	sequenceNumberItems map[uint64]T
	byteCount           ByteCount
	stateLock           sync.Mutex

	cmp TransferQueueCmpFunction[T]
}

func newTransferQueue[T transferQueueItem](cmp TransferQueueCmpFunction[T]) *transferQueue[T] {
	transferQueue := &transferQueue[T]{
		orderedItems:        []T{},
		maxHeap:             newTransferQueueMaxHeap[T](cmp),
		messageIdItems:      map[Id]T{},
		sequenceNumberItems: map[uint64]T{},
		byteCount:           0,
		cmp:                 cmp,
	}
	heap.Init(transferQueue)
	return transferQueue
}

func (self *transferQueue[T]) QueueSize() (int, ByteCount) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	return len(self.orderedItems), self.byteCount
}

func (self *transferQueue[T]) Add(item T) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	self.messageIdItems[item.MessageId()] = item
	self.sequenceNumberItems[item.SequenceNumber()] = item
	heap.Push(self, item)
	heap.Push(self.maxHeap, item)
	self.byteCount += item.MessageByteCount()
}

func (self *transferQueue[T]) ContainsMessageId(messageId Id) (sequenceNumber uint64, ok bool) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	item, ok := self.messageIdItems[messageId]
	if !ok {
		return 0, false
	}
	return item.SequenceNumber(), true
}

func (self *transferQueue[T]) RemoveByMessageId(messageId Id) T {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	item, ok := self.messageIdItems[messageId]
	if !ok {
		var empty T
		return empty
	}
	return self.remove(item)
}

func (self *transferQueue[T]) RemoveBySequenceNumber(sequenceNumber uint64) T {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	item, ok := self.sequenceNumberItems[sequenceNumber]
	if !ok {
		var empty T
		return empty
	}
	return self.remove(item)
}

func (self *transferQueue[T]) GetByMessageId(messageId Id) T {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	item, ok := self.messageIdItems[messageId]
	if !ok {
		var empty T
		return empty
	}
	return item
}

func (self *transferQueue[T]) GetBySequenceNumber(sequenceNumber uint64) T {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	item, ok := self.sequenceNumberItems[sequenceNumber]
	if !ok {
		var empty T
		return empty
	}
	return item
}

func (self *transferQueue[T]) remove(item T) T {
	delete(self.messageIdItems, item.MessageId())
	delete(self.sequenceNumberItems, item.SequenceNumber())
	item_ := heap.Remove(self, item.HeapIndex())
	if any(item) != item_ {
		panic("Heap invariant broken.")
	}
	heap.Remove(self.maxHeap, item.MaxHeapIndex())
	self.byteCount -= item.MessageByteCount()
	return item
}

func (self *transferQueue[T]) RemoveFirst() T {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if len(self.orderedItems) == 0 {
		var empty T
		return empty
	}

	item := heap.Remove(self, 0).(T)
	heap.Remove(self.maxHeap, item.MaxHeapIndex())
	delete(self.messageIdItems, item.MessageId())
	delete(self.sequenceNumberItems, item.SequenceNumber())
	self.byteCount -= item.MessageByteCount()
	return item
}

func (self *transferQueue[T]) PeekFirst() T {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if len(self.orderedItems) == 0 {
		var empty T
		return empty
	}
	return self.orderedItems[0]
}

func (self *transferQueue[T]) PeekLast() T {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	return self.maxHeap.PeekFirst()
}

// heap.Interface

func (self *transferQueue[T]) Push(x any) {
	item := x.(T)
	item.SetHeapIndex(len(self.orderedItems))
	self.orderedItems = append(self.orderedItems, item)
}

func (self *transferQueue[T]) Pop() any {
	n := len(self.orderedItems)
	i := n - 1
	var empty T
	item := self.orderedItems[i]
	self.orderedItems[i] = empty
	self.orderedItems = self.orderedItems[:n-1]
	return item
}

// sort.Interface

func (self *transferQueue[T]) Len() int {
	return len(self.orderedItems)
}

func (self *transferQueue[T]) Less(i int, j int) bool {
	return self.cmp(self.orderedItems[i], self.orderedItems[j]) < 0
}

func (self *transferQueue[T]) Swap(i int, j int) {
	a := self.orderedItems[i]
	b := self.orderedItems[j]
	b.SetHeapIndex(i)
	self.orderedItems[i] = b
	a.SetHeapIndex(j)
	self.orderedItems[j] = a
}

// ordered by `sequenceNumber` descending
type transferQueueMaxHeap[T transferQueueItem] struct {
	orderedItems []T

	cmp TransferQueueCmpFunction[T]
}

func newTransferQueueMaxHeap[T transferQueueItem](cmp TransferQueueCmpFunction[T]) *transferQueueMaxHeap[T] {
	transferQueueMaxHeap := &transferQueueMaxHeap[T]{
		orderedItems: []T{},
		cmp:          cmp,
	}
	heap.Init(transferQueueMaxHeap)
	return transferQueueMaxHeap
}

func (self *transferQueueMaxHeap[T]) PeekFirst() T {
	if len(self.orderedItems) == 0 {
		var empty T
		return empty
	}
	return self.orderedItems[0]
}

// heap.Interface

func (self *transferQueueMaxHeap[T]) Push(x any) {
	item := x.(T)
	item.SetMaxHeapIndex(len(self.orderedItems))
	self.orderedItems = append(self.orderedItems, item)
}

func (self *transferQueueMaxHeap[T]) Pop() any {
	n := len(self.orderedItems)
	i := n - 1
	var empty T
	item := self.orderedItems[i]
	self.orderedItems[i] = empty
	self.orderedItems = self.orderedItems[:n-1]
	return item
}

// `sort.Interface`

func (self *transferQueueMaxHeap[T]) Len() int {
	return len(self.orderedItems)
}

func (self *transferQueueMaxHeap[T]) Less(i int, j int) bool {
	return 0 <= self.cmp(self.orderedItems[i], self.orderedItems[j])
}

func (self *transferQueueMaxHeap[T]) Swap(i int, j int) {
	a := self.orderedItems[i]
	b := self.orderedItems[j]
	b.SetMaxHeapIndex(i)
	self.orderedItems[i] = b
	a.SetMaxHeapIndex(j)
	self.orderedItems[j] = a
}
