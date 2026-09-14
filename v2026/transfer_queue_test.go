package connect

import (
	mathrand "math/rand"
	"testing"
)

func TestTransferQueue(t *testing.T) {

	type myTransferItem struct {
		transferItem
	}

	queue := newTransferQueue[*myTransferItem](func(a *myTransferItem, b *myTransferItem) int {
		if a.sequenceNumber < b.sequenceNumber {
			return -1
		} else if b.sequenceNumber < a.sequenceNumber {
			return 1
		} else {
			return 0
		}
	})

	size, byteSize := queue.QueueSize()
	AssertEqual(t, 0, size)
	AssertEqual(t, ByteCount(0), byteSize)

	n := 100

	items := []*myTransferItem{}
	sequenceNumberMessageIds := map[uint64]Id{}
	for i := 0; i < n; i += 1 {
		item := &myTransferItem{
			transferItem: transferItem{
				messageId:        NewId(),
				messageByteCount: ByteCount(1),
				sequenceNumber:   uint64(i),
			},
		}
		items = append(items, item)
		sequenceNumberMessageIds[item.sequenceNumber] = item.messageId
	}

	// add a bunch and test peekFirst, peekLast
	// remove first
	mathrand.Shuffle(len(items), func(i, j int) {
		items[i], items[j] = items[j], items[i]
	})
	for _, item := range items {
		queue.Add(item)
	}

	for sequenceNumber, messageId := range sequenceNumberMessageIds {
		item := queue.GetByMessageId(messageId)
		AssertNotEqual(t, item, nil)
		AssertEqual(t, sequenceNumber, item.sequenceNumber)
	}

	for sequenceNumber, messageId := range sequenceNumberMessageIds {
		item := queue.GetBySequenceNumber(sequenceNumber)
		AssertNotEqual(t, item, nil)
		AssertEqual(t, messageId, item.messageId)
	}

	for i := 0; i < n; i += 1 {
		size, byteSize = queue.QueueSize()
		AssertEqual(t, n-i, size)
		AssertEqual(t, ByteCount(n-i), byteSize)

		AssertEqual(t, uint64(i), queue.PeekFirst().sequenceNumber)
		AssertEqual(t, uint64(n-1), queue.PeekLast().sequenceNumber)

		first := queue.RemoveFirst()
		AssertEqual(t, uint64(i), first.sequenceNumber)
	}
	size, byteSize = queue.QueueSize()
	AssertEqual(t, 0, size)
	AssertEqual(t, ByteCount(0), byteSize)

	// add a bunch and test peekFirst, peekLast
	// remove by id
	mathrand.Shuffle(len(items), func(i, j int) {
		items[i], items[j] = items[j], items[i]
	})
	for _, item := range items {
		queue.Add(item)
	}

	for i := 0; i < n; i += 1 {
		size, byteSize = queue.QueueSize()
		AssertEqual(t, n-i, size)
		AssertEqual(t, ByteCount(n-i), byteSize)

		AssertEqual(t, uint64(i), queue.PeekFirst().sequenceNumber)
		AssertEqual(t, uint64(n-1), queue.PeekLast().sequenceNumber)

		messageId := sequenceNumberMessageIds[uint64(i)]
		first := queue.RemoveByMessageId(messageId)
		AssertEqual(t, uint64(i), first.sequenceNumber)
	}
	size, byteSize = queue.QueueSize()
	AssertEqual(t, 0, size)
	AssertEqual(t, ByteCount(0), byteSize)

	// add a bunch and test peekFirst, peekLast
	// remove by sequence number
	mathrand.Shuffle(len(items), func(i, j int) {
		items[i], items[j] = items[j], items[i]
	})
	for _, item := range items {
		queue.Add(item)
	}

	for i := 0; i < n; i += 1 {
		size, byteSize = queue.QueueSize()
		AssertEqual(t, n-i, size)
		AssertEqual(t, ByteCount(n-i), byteSize)

		AssertEqual(t, uint64(i), queue.PeekFirst().sequenceNumber)
		AssertEqual(t, uint64(n-1), queue.PeekLast().sequenceNumber)

		first := queue.RemoveBySequenceNumber(uint64(i))
		AssertEqual(t, uint64(i), first.sequenceNumber)
	}
	size, byteSize = queue.QueueSize()
	AssertEqual(t, 0, size)
	AssertEqual(t, ByteCount(0), byteSize)

}
