package connect

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// essential test 1a: budget accounting semantics
func TestTransferMemoryBudgetAccounting(t *testing.T) {
	budget := NewTransferMemoryBudget(kib(64))

	AssertEqual(t, budget.TotalByteCount(), kib(64))
	AssertEqual(t, budget.Available(), kib(64))
	AssertEqual(t, budget.UsedByteCount(), ByteCount(0))

	budget.Reserve(kib(16))
	AssertEqual(t, budget.Available(), kib(48))
	AssertEqual(t, budget.UsedByteCount(), kib(16))

	// reserve always succeeds; available floors at zero on overdraft
	budget.Reserve(kib(64))
	AssertEqual(t, budget.Available(), ByteCount(0))
	AssertEqual(t, budget.UsedByteCount(), kib(80))

	budget.Release(kib(80))
	AssertEqual(t, budget.Available(), kib(64))
	AssertEqual(t, budget.UsedByteCount(), ByteCount(0))

	reserved, released := budget.Counts()
	AssertEqual(t, reserved, kib(80))
	AssertEqual(t, released, kib(80))
}

// essential test 1a: concurrent reserve/release keeps exact balance (run
// under -race in the suite)
func TestTransferMemoryBudgetConcurrent(t *testing.T) {
	budget := NewTransferMemoryBudget(mib(1))

	var wg sync.WaitGroup
	for g := 0; g < 8; g += 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 10000; i += 1 {
				budget.Reserve(1000)
				budget.Available()
				budget.Release(1000)
			}
		}()
	}
	wg.Wait()

	AssertEqual(t, budget.UsedByteCount(), ByteCount(0))
	reserved, released := budget.Counts()
	AssertEqual(t, reserved, ByteCount(8*10000*1000))
	AssertEqual(t, reserved, released)
}

func TestTransferMemoryBudgetTryReserveExactConcurrentCeiling(t *testing.T) {
	const reservationCount = 2
	reservationSize := kib(8)
	budget := NewTransferMemoryBudget(reservationCount * reservationSize)

	start := make(chan struct{})
	var admitted atomic.Int64
	var wg sync.WaitGroup
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if budget.TryReserve(reservationSize) {
				admitted.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	AssertEqual(t, admitted.Load(), int64(reservationCount))
	AssertEqual(t, budget.UsedByteCount(), reservationCount*reservationSize)
	AssertEqual(t, budget.TryReserve(1), false)

	notify := budget.CapacityNotify()
	budget.Release(reservationSize)
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("release did not notify an admission waiter")
	}
	AssertEqual(t, budget.TryReserve(reservationSize), true)
	budget.Release(reservationCount * reservationSize)
	AssertEqual(t, budget.UsedByteCount(), ByteCount(0))

	reserved, released := budget.Counts()
	AssertEqual(t, reserved, released)
}

func TestTransferMemoryBudgetCapacityNotificationIsLazy(t *testing.T) {
	budget := NewTransferMemoryBudget(kib(64))
	if budget.notify.Load() != nil {
		t.Fatal("budget eagerly allocated a capacity notification")
	}

	for range 10_000 {
		budget.Reserve(1)
		budget.Release(1)
	}
	if budget.notify.Load() != nil {
		t.Fatal("release allocated a notification without a waiter")
	}

	notify := budget.CapacityNotify()
	if notify == nil {
		t.Fatal("capacity waiter did not receive a notification channel")
	}
	budget.Reserve(1)
	budget.Release(1)
	select {
	case <-notify:
	case <-time.After(time.Second):
		t.Fatal("subscribed capacity waiter was not notified")
	}
	if budget.notify.Load() != nil {
		t.Fatal("release eagerly allocated the next notification generation")
	}
	if next := budget.CapacityNotify(); next == notify {
		t.Fatal("new waiter reused a closed notification generation")
	}
}

func TestTransferMemoryBudgetCapacityNotificationWakesConcurrentSubscribers(t *testing.T) {
	const waiterCount = 64
	budget := NewTransferMemoryBudget(1)
	budget.Reserve(1)

	start := make(chan struct{})
	notifies := make(chan (<-chan struct{}), waiterCount)
	var wg sync.WaitGroup
	for range waiterCount {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			notifies <- budget.CapacityNotify()
		}()
	}
	close(start)
	wg.Wait()
	close(notifies)

	var shared <-chan struct{}
	for notify := range notifies {
		if shared == nil {
			shared = notify
		} else if notify != shared {
			t.Fatal("concurrent subscribers observed different notification generations")
		}
	}
	budget.Release(1)
	select {
	case <-shared:
	case <-time.After(time.Second):
		t.Fatal("release did not wake concurrent capacity subscribers")
	}
}

func TestTransferMemoryBudgetCapacityNotificationHasNoReleaseRace(t *testing.T) {
	budget := NewTransferMemoryBudget(1)
	AssertEqual(t, true, budget.TryReserve(1))
	deadline := time.After(10 * time.Second)

	for range 10_000 {
		notify := budget.CapacityNotify()
		releaseDone := make(chan struct{})
		go func() {
			budget.Release(1)
			close(releaseDone)
		}()

		if !budget.TryReserve(1) {
			select {
			case <-notify:
			case <-deadline:
				t.Fatal("release raced past its capacity subscriber")
			}
			if !budget.TryReserve(1) {
				t.Fatal("capacity remained unavailable after release notification")
			}
		}
		<-releaseDone
	}
	budget.Release(1)
}

func TestTransferMemoryBudgetAdmissionReleaseWakesOnlyCapacityFit(t *testing.T) {
	const waiterCount = 64
	budget := NewTransferMemoryBudget(1)
	budget.Reserve(1)

	waiters := make([]*transferMemoryBudgetWaiter, waiterCount)
	notifies := make([]<-chan struct{}, waiterCount)
	for i := range waiterCount {
		waiters[i] = newTransferMemoryBudgetWaiter()
		notifies[i] = waiters[i].subscribe(budget, 1)
	}
	defer func() {
		for _, waiter := range waiters {
			waiter.reset()
		}
	}()

	budget.Release(1)
	wokenCount := 0
	for _, notify := range notifies {
		select {
		case <-notify:
			wokenCount += 1
		default:
		}
	}
	AssertEqual(t, wokenCount, 1)
	AssertEqual(t, budget.capacityWaiterCount.Load(), int64(waiterCount-1))

	if !budget.TryReserve(1) {
		t.Fatal("the single notified admission could not reserve released capacity")
	}
	budget.Release(1)
	wokenCount = 0
	for _, notify := range notifies {
		select {
		case <-notify:
			wokenCount += 1
		default:
		}
	}
	AssertEqual(t, wokenCount, 1)
	AssertEqual(t, budget.capacityWaiterCount.Load(), int64(waiterCount-2))
}

func TestTransferMemoryBudgetAdmissionReleaseSkipsIneligibleHead(t *testing.T) {
	budget := NewTransferMemoryBudget(10)
	budget.Reserve(10)
	largeWaiter := newTransferMemoryBudgetWaiter()
	smallWaiter := newTransferMemoryBudgetWaiter()
	defer largeWaiter.reset()
	defer smallWaiter.reset()

	largeNotify := largeWaiter.subscribe(budget, 8)
	smallNotify := smallWaiter.subscribe(budget, 3)
	budget.Release(3)
	select {
	case <-largeNotify:
		t.Fatal("partial capacity woke an ineligible large admission")
	default:
	}
	select {
	case <-smallNotify:
	default:
		t.Fatal("eligible admission behind a large waiter was not woken")
	}

	budget.Release(5)
	select {
	case <-largeNotify:
	default:
		t.Fatal("large admission was not woken when enough capacity accumulated")
	}
}

func TestTransferMemoryBudgetAdmissionResizeWakesOnlyNewCapacityFit(t *testing.T) {
	budget := NewTransferMemoryBudget(1)
	budget.Reserve(1)
	waiters := make([]*transferMemoryBudgetWaiter, 4)
	notifies := make([]<-chan struct{}, len(waiters))
	for i := range waiters {
		waiters[i] = newTransferMemoryBudgetWaiter()
		notifies[i] = waiters[i].subscribe(budget, 2)
	}
	defer func() {
		for _, waiter := range waiters {
			waiter.reset()
		}
	}()

	budget.SetTotalByteCount(5)
	wokenCount := 0
	for _, notify := range notifies {
		select {
		case <-notify:
			wokenCount += 1
		default:
		}
	}
	AssertEqual(t, wokenCount, 2)
	AssertEqual(t, budget.capacityWaiterCount.Load(), int64(2))
}

func TestTransferMemoryBudgetCanceledAdmissionDoesNotConsumeWake(t *testing.T) {
	budget := NewTransferMemoryBudget(1)
	budget.Reserve(1)
	canceledWaiter := newTransferMemoryBudgetWaiter()
	liveWaiter := newTransferMemoryBudgetWaiter()
	canceledWaiter.subscribe(budget, 1)
	canceledWaiter.reset()
	liveNotify := liveWaiter.subscribe(budget, 1)
	defer liveWaiter.reset()

	budget.Release(1)
	select {
	case <-liveNotify:
	default:
		t.Fatal("canceled admission prevented the live waiter from waking")
	}
}

func TestTransferMemoryBudgetAdmissionSubscribeResetDoesNotAllocate(t *testing.T) {
	budget := NewTransferMemoryBudget(1)
	budget.Reserve(1)
	waiter := newTransferMemoryBudgetWaiter()
	allocCount := testing.AllocsPerRun(1_000, func() {
		waiter.subscribe(budget, 1)
		waiter.reset()
	})
	AssertEqual(t, allocCount, float64(0))
}

func BenchmarkTransferMemoryBudgetReserveReleaseNoWaiter(b *testing.B) {
	budget := NewTransferMemoryBudget(mib(1))
	b.ReportAllocs()
	for range b.N {
		budget.Reserve(1)
		budget.Release(1)
	}
}

func BenchmarkTransferMemoryBudgetConcurrentReserveReleaseNoWaiter(b *testing.B) {
	budget := NewTransferMemoryBudget(mib(1))
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			budget.Reserve(1)
			budget.Release(1)
		}
	})
}

func BenchmarkTransferMemoryBudgetAdmissionWakeOneOf64(b *testing.B) {
	const waiterCount = 64
	budget := NewTransferMemoryBudget(1)
	budget.Reserve(1)
	waiters := make([]*transferMemoryBudgetWaiter, waiterCount)
	for i := range waiters {
		waiters[i] = newTransferMemoryBudgetWaiter()
		waiters[i].subscribe(budget, 1)
	}
	defer func() {
		for _, waiter := range waiters {
			waiter.reset()
		}
	}()

	b.ReportAllocs()
	b.ResetTimer()
	for i := range b.N {
		waiter := waiters[i%waiterCount]
		budget.Release(1)
		<-waiter.notify
		if !budget.TryReserve(1) {
			b.Fatal("notified admission could not reserve")
		}
		waiter.subscribe(budget, 1)
	}
}

func TestTransferMemoryBudgetResize(t *testing.T) {
	budget := NewTransferMemoryBudget(kib(64))
	budget.Reserve(kib(48))

	// Shrink is non-evicting and exposes no new headroom until usage drains.
	budget.SetTotalByteCount(kib(32))
	AssertEqual(t, budget.TotalByteCount(), kib(32))
	AssertEqual(t, budget.UsedByteCount(), kib(48))
	AssertEqual(t, budget.Available(), ByteCount(0))

	budget.Release(kib(32))
	AssertEqual(t, budget.UsedByteCount(), kib(16))
	AssertEqual(t, budget.Available(), kib(16))

	// Growth takes effect immediately.
	budget.SetTotalByteCount(kib(128))
	AssertEqual(t, budget.Available(), kib(112))
	budget.Release(kib(16))
	AssertEqual(t, budget.UsedByteCount(), ByteCount(0))

	reserved, released := budget.Counts()
	AssertEqual(t, reserved, released)
}

// essential test 1b: the queue maintains borrowed = max(0, byteCount-floor)
// across add, remove, and clear, and CanAdd honors the floor, the max, and
// the budget headroom
func TestTransferQueueBudgetBorrow(t *testing.T) {
	budget := NewTransferMemoryBudget(kib(8))
	queue := newTransferQueue[*transferItem](func(a *transferItem, b *transferItem) int {
		if a.sequenceNumber < b.sequenceNumber {
			return -1
		} else if b.sequenceNumber < a.sequenceNumber {
			return 1
		}
		return 0
	})
	queue.setBudget(budget, kib(4))

	newItem := func(sequenceNumber uint64, byteCount ByteCount) *transferItem {
		return &transferItem{
			messageId:        NewId(),
			messageByteCount: byteCount,
			sequenceNumber:   sequenceNumber,
		}
	}

	// below the floor there is no borrowing
	a := newItem(1, kib(2))
	queue.Add(a)
	AssertEqual(t, budget.UsedByteCount(), ByteCount(0))
	// crossing the floor borrows the excess
	b := newItem(2, kib(4))
	queue.Add(b)
	AssertEqual(t, budget.UsedByteCount(), kib(2))
	// removal below the floor releases everything borrowed
	queue.RemoveByMessageId(b.messageId)
	AssertEqual(t, budget.UsedByteCount(), ByteCount(0))

	// CanAdd: under floor always; above floor requires headroom under max
	AssertEqual(t, queue.CanAdd(kib(1), kib(64)), true)
	// would exceed max
	AssertEqual(t, queue.CanAdd(kib(64), kib(4)), false)
	// zero byte count probe: at the floor, requires headroom
	queue.Add(newItem(3, kib(2)))
	AssertEqual(t, queue.CanAdd(0, kib(64)), true)
	// exhaust the budget elsewhere: above-floor growth is refused, and a
	// same-size add below max but above floor is refused too
	budget.Reserve(kib(8))
	AssertEqual(t, queue.CanAdd(0, kib(64)), false)
	AssertEqual(t, queue.CanAdd(kib(2), kib(64)), false)
	budget.Release(kib(8))
	AssertEqual(t, queue.CanAdd(0, kib(64)), true)

	// grow above the floor, then Clear releases all borrowed bytes
	// (2 + 2 + 4 + 2 KiB queued - 4 KiB floor = 6 KiB borrowed)
	queue.Add(newItem(4, kib(4)))
	queue.Add(newItem(5, kib(2)))
	AssertEqual(t, budget.UsedByteCount(), kib(6))
	items := queue.Clear()
	AssertEqual(t, len(items), 4)
	AssertEqual(t, budget.UsedByteCount(), ByteCount(0))
	AssertEqual(t, queue.Len(), 0)
	_, queueByteCount := queue.QueueSize()
	AssertEqual(t, queueByteCount, ByteCount(0))

	reserved, released := budget.Counts()
	AssertEqual(t, reserved, released)
}

// A zero byte floor charges every reorder item after the queue's mandatory
// first progress item. This is the mobile topology: many browser flows must
// not each multiply a large unaccounted byte floor, while an empty flow still
// makes progress when another flow temporarily owns the aggregate window.
func TestTransferQueueZeroFloorBoundsManyFlowReorderOwnership(t *testing.T) {
	budget := NewTransferMemoryBudget(kib(8))
	newQueue := func() *transferQueue[*transferItem] {
		queue := newTransferQueue[*transferItem](func(a *transferItem, b *transferItem) int {
			if a.sequenceNumber < b.sequenceNumber {
				return -1
			}
			if b.sequenceNumber < a.sequenceNumber {
				return 1
			}
			return 0
		})
		queue.setBudget(budget, 0)
		return queue
	}
	newItem := func(sequenceNumber uint64, byteCount ByteCount) *transferItem {
		return &transferItem{
			messageId:        NewId(),
			messageByteCount: byteCount,
			sequenceNumber:   sequenceNumber,
		}
	}

	first := newQueue()
	if !first.CanAdd(kib(4), kib(64)) {
		t.Fatal("empty first flow did not admit its progress item")
	}
	first.Add(newItem(1, kib(4)))
	if !first.CanAdd(kib(4), kib(64)) {
		t.Fatal("aggregate headroom did not admit the second first-flow item")
	}
	first.Add(newItem(2, kib(4)))
	AssertEqual(t, budget.UsedByteCount(), kib(8))
	if first.CanAdd(1, kib(64)) {
		t.Fatal("full aggregate budget admitted additional ownership")
	}

	second := newQueue()
	if !second.CanAdd(kib(4), kib(64)) {
		t.Fatal("empty second flow did not retain one-item liveness")
	}
	second.Add(newItem(1, kib(4)))
	AssertEqual(t, budget.UsedByteCount(), kib(12))
	if second.CanAdd(1, kib(64)) {
		t.Fatal("second flow multiplied its one-item overdraft")
	}

	first.Clear()
	second.Clear()
	AssertEqual(t, budget.UsedByteCount(), ByteCount(0))
	reserved, released := budget.Counts()
	AssertEqual(t, reserved, released)
}

func TestReceivePackQueueChargeIncludesEveryRetainedRoot(t *testing.T) {
	outer := MessagePoolGet(3000)
	message := MessagePoolGet(1500)
	contract := MessagePoolGet(60)
	defer MessagePoolReturn(outer)
	defer MessagePoolReturn(message)
	defer MessagePoolReturn(contract)
	receivePack := &ReceivePack{
		Pack: &protocol.Pack{
			Frames: []*protocol.Frame{{MessageBytes: message}},
			ContractFrame: &protocol.Frame{
				MessageBytes: contract,
			},
		},
		decodedOwner:       &decodedPackOwner{},
		MessageByteCount:   ByteCount(len(message)),
		TransferFrameBytes: outer,
	}
	want := ByteCount(4096 + 2048 + 256 + 1024)
	if got := receivePack.receiveQueueByteCount(); got != want {
		t.Fatalf("receive queue allocation charge = %d, want %d", got, want)
	}

	item := &transferItem{
		messageId:        NewId(),
		messageByteCount: receivePack.MessageByteCount,
		queueByteCount:   receivePack.receiveQueueByteCount(),
		sequenceNumber:   1,
	}
	budget := NewTransferMemoryBudget(2 * want)
	queue := newTransferQueue[*transferItem](func(a *transferItem, b *transferItem) int {
		return int(a.sequenceNumber - b.sequenceNumber)
	})
	queue.setBudget(budget, 0)
	queue.Add(item)
	_, messageBytes := queue.QueueSize()
	AssertEqual(t, messageBytes, receivePack.MessageByteCount)
	AssertEqual(t, budget.UsedByteCount(), want)
	if !queue.CanAddWithQueueByteCount(
		receivePack.MessageByteCount,
		receivePack.receiveQueueByteCount(),
		4001,
	) {
		t.Fatal("allocation charge incorrectly shrank the logical per-flow window")
	}
	second := &transferItem{
		messageId:        NewId(),
		messageByteCount: receivePack.MessageByteCount,
		queueByteCount:   receivePack.receiveQueueByteCount(),
		sequenceNumber:   2,
	}
	queue.Add(second)
	AssertEqual(t, budget.UsedByteCount(), 2*want)
	if queue.CanAddWithQueueByteCount(
		receivePack.MessageByteCount,
		receivePack.receiveQueueByteCount(),
		4001,
	) {
		t.Fatal("logical per-flow maximum admitted a third payload")
	}
	queue.Clear()
	AssertEqual(t, budget.UsedByteCount(), ByteCount(0))
}

// budgetTestPeer wires a sender client to one receiver client over direct
// channel routes, optionally without the ack return path so the sender's
// resend queue holds every sent message (deterministic queue depth).
type budgetTestPeer struct {
	receiverClient *Client
	unsub          func()
}

func newBudgetTestSender(ctx context.Context, sendBudget *TransferMemoryBudget, minByteCount ByteCount, maxByteCount ByteCount) *Client {
	clientSettings := DefaultClientSettings()
	// unbuffered pack channel: Send admission mirrors queue admission
	clientSettings.SendBufferSettings.SequenceBufferSize = 0
	clientSettings.SendBufferSettings.AckBufferSize = 0
	clientSettings.SendBufferSettings.AckTimeout = 300 * time.Second
	clientSettings.SendBufferSettings.IdleTimeout = 300 * time.Second
	clientSettings.SendBufferSettings.ResendQueueMinByteCount = minByteCount
	clientSettings.SendBufferSettings.ResendQueueMaxByteCount = maxByteCount
	clientSettings.SendBufferSettings.ResendQueueBudget = sendBudget
	// keep resends quiet during the withheld-ack tests
	clientSettings.SendBufferSettings.MinResendInterval = 300 * time.Second
	clientSettings.SendBufferSettings.MaxResendInterval = 300 * time.Second
	// plaintext, so the one-way (withheld ack) wirings never depend on a
	// handshake round trip
	clientSettings.EncryptionSettings.Mode = EncryptionModeOff
	return NewClient(ctx, NewId(), NewNoContractClientOob(), clientSettings)
}

// attachBudgetTestPeer wires sender->receiver routes. withAcks wires the
// return path so the receiver's acks drain the sender's resend queue.
func attachBudgetTestPeer(ctx context.Context, sender *Client, withAcks bool, receiveCallback ReceiveFunction) *budgetTestPeer {
	receiverSettings := DefaultClientSettings()
	receiverSettings.EncryptionSettings.Mode = EncryptionModeOff
	receiverClient := NewClient(ctx, NewId(), NewNoContractClientOob(), receiverSettings)

	forwardRoute := make(chan []byte)
	sender.RouteManager().UpdateTransport(NewSendClientTransport(DestinationId(receiverClient.ClientId())), []Route{forwardRoute})
	receiverClient.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{forwardRoute})

	if withAcks {
		returnRoute := make(chan []byte)
		receiverClient.RouteManager().UpdateTransport(NewSendClientTransport(DestinationId(sender.ClientId())), []Route{returnRoute})
		sender.RouteManager().UpdateTransport(NewReceiveGatewayTransport(), []Route{returnRoute})
	}

	sender.ContractManager().AddNoContractPeer(receiverClient.ClientId())
	receiverClient.ContractManager().AddNoContractPeer(sender.ClientId())

	var unsub func()
	if receiveCallback != nil {
		unsub = receiverClient.AddReceiveCallback(receiveCallback)
	}

	return &budgetTestPeer{
		receiverClient: receiverClient,
		unsub:          unsub,
	}
}

func budgetTestFrame(payloadByteCount int) *protocol.Frame {
	message := &protocol.SimpleMessage{
		Content: strings.Repeat("x", payloadByteCount),
	}
	return RequireToFrameWithDefaultProtocolVersion(message)
}

// settleBudgetUsed polls until the budget used count is stable (sequences
// unwind asynchronously after cancel)
func settleBudgetUsed(budget *TransferMemoryBudget) ByteCount {
	prev := budget.UsedByteCount()
	stableCount := 0
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
		n := budget.UsedByteCount()
		if n == prev {
			stableCount += 1
			if 4 <= stableCount && n == 0 {
				break
			}
			if 8 <= stableCount {
				break
			}
		} else {
			stableCount = 0
			prev = n
		}
	}
	return prev
}

// fillBudgetTestQueue sends messages with a short timeout until the resend
// queue refuses (acks are withheld, so refusal means queue admission paused).
// returns the number of accepted messages.
func fillBudgetTestQueue(sender *Client, destinationId Id, payloadByteCount int, maxMessages int) int {
	accepted := 0
	for i := 0; i < maxMessages; i += 1 {
		success := sender.SendWithTimeout(
			budgetTestFrame(payloadByteCount),
			destinationId,
			func(err error) {},
			500*time.Millisecond,
		)
		if !success {
			break
		}
		accepted += 1
	}
	return accepted
}

// essential test 3 (parity): with the shared budget larger than the
// per-sequence cap, a single sequence reaches the same depth as with no
// budget at all — one fast peer still gets its full queue
func TestTransferBudgetSingleSequenceParity(t *testing.T) {
	const payloadByteCount = 4 * 1024
	const maxMessages = 200
	minByteCount := kib(16)
	maxByteCount := kib(256)

	run := func(sendBudget *TransferMemoryBudget) (int, ByteCount) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sender := newBudgetTestSender(ctx, sendBudget, minByteCount, maxByteCount)
		defer sender.Cancel()
		peer := attachBudgetTestPeer(ctx, sender, false, nil)
		defer peer.receiverClient.Cancel()

		accepted := fillBudgetTestQueue(sender, peer.receiverClient.ClientId(), payloadByteCount, maxMessages)
		// sample the live borrow before the deferred teardown releases it
		usedAtFill := ByteCount(0)
		if sendBudget != nil {
			usedAtFill = sendBudget.UsedByteCount()
		}
		return accepted, usedAtFill
	}

	// pool larger than the cap: the cap binds, exactly like nil budget
	budget := NewTransferMemoryBudget(kib(512))
	acceptedWithBudget, usedAtFill := run(budget)
	acceptedNil, _ := run(nil)

	t.Logf("accepted with budget=%d nil=%d usedAtFill=%d", acceptedWithBudget, acceptedNil, usedAtFill)
	if acceptedWithBudget < acceptedNil-2 || acceptedNil+2 < acceptedWithBudget {
		t.Errorf("single sequence depth changed under a roomy budget: %d with vs %d without", acceptedWithBudget, acceptedNil)
	}
	// the sequence borrowed well above the floor
	if usedAtFill < maxByteCount-minByteCount-2*payloadByteCount {
		t.Errorf("expected deep borrow, used at fill = %d", usedAtFill)
	}

	used := settleBudgetUsed(budget)
	AssertEqual(t, used, ByteCount(0))
	reserved, released := budget.Counts()
	AssertEqual(t, reserved, released)
	if reserved == 0 {
		t.Errorf("expected borrowing to have happened")
	}
}

// essential test 4 (ceiling): with many sequences and a small shared pool,
// the aggregate borrow never exceeds the pool (plus the documented one
// message per sequence overdraft), and every sequence still gets its floor
func TestTransferBudgetAggregateCeiling(t *testing.T) {
	const peerCount = 6
	const payloadByteCount = 4 * 1024
	const maxMessages = 200
	minByteCount := kib(8)
	maxByteCount := kib(512)
	totalByteCount := kib(64)
	// each admission that saw headroom can overshoot by about one framed
	// message per sequence
	slopByteCount := ByteCount(peerCount * (payloadByteCount + 2048))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	budget := NewTransferMemoryBudget(totalByteCount)
	sender := newBudgetTestSender(ctx, budget, minByteCount, maxByteCount)
	defer sender.Cancel()

	peers := []*budgetTestPeer{}
	for i := 0; i < peerCount; i += 1 {
		peer := attachBudgetTestPeer(ctx, sender, false, nil)
		defer peer.receiverClient.Cancel()
		peers = append(peers, peer)
	}

	// sample the peak budget usage while the queues fill
	var maxUsed atomic.Int64
	samplerDone := make(chan struct{})
	go func() {
		defer close(samplerDone)
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Millisecond):
			}
			used := int64(budget.UsedByteCount())
			for {
				prev := maxUsed.Load()
				if used <= prev || maxUsed.CompareAndSwap(prev, used) {
					break
				}
			}
		}
	}()

	// fill every sequence concurrently until each refuses
	var wg sync.WaitGroup
	acceptedByteCounts := make([]int64, peerCount)
	for i := 0; i < peerCount; i += 1 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			accepted := fillBudgetTestQueue(sender, peers[i].receiverClient.ClientId(), payloadByteCount, maxMessages)
			acceptedByteCounts[i] = int64(accepted) * payloadByteCount
		}(i)
	}
	wg.Wait()

	totalAccepted := int64(0)
	for i, acceptedByteCount := range acceptedByteCounts {
		t.Logf("sequence %d accepted %d bytes", i, acceptedByteCount)
		// every sequence progressed on at least its floor
		if acceptedByteCount < int64(minByteCount) {
			t.Errorf("sequence %d starved below its floor: %d < %d", i, acceptedByteCount, minByteCount)
		}
		totalAccepted += acceptedByteCount
	}

	// the pool saturated (this test is about the ceiling binding)
	if int64(totalByteCount)/2 > maxUsed.Load() {
		t.Errorf("expected the pool to saturate, peak used = %d of %d", maxUsed.Load(), totalByteCount)
	}
	// the ceiling held: peak borrow within total + one message per sequence
	if int64(totalByteCount+slopByteCount) < maxUsed.Load() {
		t.Errorf("budget ceiling exceeded: peak used %d > total %d + slop %d", maxUsed.Load(), totalByteCount, slopByteCount)
	}
	// the aggregate is flat: floors + pool + slop, far below peerCount x max
	aggregateCeiling := int64(peerCount)*int64(minByteCount) + int64(totalByteCount) + int64(slopByteCount) + int64(peerCount)*2048
	if aggregateCeiling < totalAccepted {
		t.Errorf("aggregate accepted %d exceeds the flat ceiling %d", totalAccepted, aggregateCeiling)
	}

	cancel()
	<-samplerDone
	used := settleBudgetUsed(budget)
	AssertEqual(t, used, ByteCount(0))
	reserved, released := budget.Counts()
	AssertEqual(t, reserved, released)
}

// essential test 2 (liveness): many sequences over a pool much smaller than
// their demand, with acks flowing — every message is eventually delivered
// and acked, so an empty pool can never deadlock the sequences (run under
// -race in the suite)
func TestTransferBudgetLiveness(t *testing.T) {
	const peerCount = 6
	const messagesPerPeer = 40
	const payloadByteCount = 2 * 1024
	const sendAdmissionTimeout = 15 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// pool far below demand: peers mostly run at their floors
	budget := NewTransferMemoryBudget(kib(16))
	sender := newBudgetTestSender(ctx, budget, kib(4), kib(256))
	defer sender.Cancel()

	var receiveCount atomic.Int64
	receiveNotify := make(chan struct{}, peerCount*messagesPerPeer)
	peers := []*budgetTestPeer{}
	for i := 0; i < peerCount; i += 1 {
		peer := attachBudgetTestPeer(ctx, sender, true, func(source TransferPath, frames []*protocol.Frame, peer Peer) {
			receiveCount.Add(int64(len(frames)))
			select {
			case receiveNotify <- struct{}{}:
			default:
			}
		})
		defer peer.receiverClient.Cancel()
		peers = append(peers, peer)
	}

	var ackCount atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < peerCount; i += 1 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < messagesPerPeer; j += 1 {
				success := sender.SendWithTimeout(
					budgetTestFrame(payloadByteCount),
					peers[i].receiverClient.ClientId(),
					func(err error) {
						if err == nil {
							ackCount.Add(1)
						}
					},
					// Wait generously for the sequence to accept, but keep a broken
					// admission path from hanging the whole package indefinitely.
					sendAdmissionTimeout,
				)
				if !success {
					t.Errorf("send %d/%d refused", i, j)
					return
				}
			}
		}(i)
	}
	producersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(producersDone)
	}()
	select {
	case <-producersDone:
	case <-time.After(sendAdmissionTimeout + 5*time.Second):
		cancel()
		<-producersDone
		t.Fatal("send producers did not finish within the admission bound")
	}
	if t.Failed() {
		return
	}

	// every message delivers and acks despite the exhausted pool
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		if int64(peerCount*messagesPerPeer) <= ackCount.Load() {
			break
		}
		select {
		case <-receiveNotify:
		case <-time.After(100 * time.Millisecond):
		}
	}
	AssertEqual(t, ackCount.Load(), int64(peerCount*messagesPerPeer))
	AssertEqual(t, receiveCount.Load(), int64(peerCount*messagesPerPeer))

	cancel()
	used := settleBudgetUsed(budget)
	AssertEqual(t, used, ByteCount(0))
	reserved, released := budget.Counts()
	AssertEqual(t, reserved, released)
}

// essential test 1c (churn balance): repeated build/load/teardown cycles
// against one shared budget pair return every borrowed byte — teardown with
// non-empty queues exercises the wholesale Clear release path
func TestTransferBudgetChurnBalance(t *testing.T) {
	sendBudget := NewTransferMemoryBudget(kib(64))

	for cycle := 0; cycle < 4; cycle += 1 {
		func() {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sender := newBudgetTestSender(ctx, sendBudget, kib(8), kib(256))
			peer := attachBudgetTestPeer(ctx, sender, false, nil)

			// fill the queue deep (no acks), then tear down with the queue
			// non-empty so Clear must release the borrow
			accepted := fillBudgetTestQueue(sender, peer.receiverClient.ClientId(), 4*1024, 50)
			if accepted == 0 {
				t.Errorf("cycle %d accepted nothing", cycle)
			}
			if sendBudget.UsedByteCount() == 0 {
				t.Errorf("cycle %d expected a live borrow before teardown", cycle)
			}

			sender.Cancel()
			peer.receiverClient.Cancel()

			used := settleBudgetUsed(sendBudget)
			if used != 0 {
				reserved, released := sendBudget.Counts()
				t.Fatalf("cycle %d leaked budget: used=%d reserved=%d released=%d", cycle, used, reserved, released)
			}
		}()
	}

	reserved, released := sendBudget.Counts()
	AssertEqual(t, reserved, released)
	if reserved == 0 {
		t.Errorf("expected borrowing across the churn cycles")
	}
	fmt.Printf("churn balance: reserved=released=%d across 4 cycles\n", reserved)
}

// The ordinary client matches the maximum unreliable message flight with
// zero-wait receive headroom, while explicit buffer-size constructors retain
// their caller-selected count for constrained and deadlock tests.
func TestDefaultReceiveSequenceHandoffIsCountAndByteBounded(t *testing.T) {
	settings := DefaultClientSettings()
	if got := settings.ReceiveBufferSettings.SequenceBufferSize; got != 256 {
		t.Fatalf("default receive sequence slots = %d, want 256", got)
	}
	if got := settings.ReceiveBufferSettings.H1SequenceBufferSize; got != 256 {
		t.Fatalf("default H1 receive sequence slots = %d, want 256", got)
	}
	if got := settings.ReceiveBufferSettings.SequenceBufferByteCount; got != kib(256) {
		t.Fatalf("default receive sequence byte limit = %d, want %d", got, kib(256))
	}
	if got := settings.ReceiveBufferSettings.H1SequenceBufferByteCount; got != kib(256) {
		t.Fatalf("default H1 receive sequence byte limit = %d, want %d", got, kib(256))
	}

	explicit := DefaultClientSettingsWithBufferSize(7)
	if got := explicit.ReceiveBufferSettings.SequenceBufferSize; got != 7 {
		t.Fatalf("explicit receive sequence slots = %d, want 7", got)
	}
	if got := explicit.ReceiveBufferSettings.H1SequenceBufferSize; got != 7 {
		t.Fatalf("explicit H1 receive sequence slots = %d, want 7", got)
	}
}

// Small frames may use independent count headroom, but encoded bytes waiting
// for one ReceiveSequence worker never exceed the configured budget. Removing
// one channel item immediately restores byte admission without waiting.
func TestReceiveSequenceHandoffEnforcesByteBudgetWithoutBlocking(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultReceiveBufferSettingsWithBufferSize(8)
	settings.SequenceBufferByteCount = 100
	sequence := newReceiveSequence(
		ctx,
		&Client{},
		SourceId(NewId()),
		NewId(),
		TransferKey{},
		settings,
	)

	newPack := func() *ReceivePack {
		return &ReceivePack{
			MessageByteCount:   40,
			TransferFrameBytes: MessagePoolGet(40),
		}
	}
	first := newPack()
	second := newPack()
	third := newPack()
	for index, pack := range []*ReceivePack{first, second} {
		if accepted, err := sequence.Pack(pack, 0); !accepted || err != nil {
			t.Fatalf("admit pack %d: accepted=%t err=%v", index, accepted, err)
		}
	}
	if accepted, err := sequence.Pack(third, 0); accepted || err != nil {
		t.Fatalf("byte-overflow pack: accepted=%t err=%v", accepted, err)
	}
	if got := sequence.packQueueByteCount.Load(); got != 80 {
		t.Fatalf("retained handoff bytes = %d, want 80", got)
	}
	if got := sequence.packQueueCount.Load(); got != 2 {
		t.Fatalf("retained handoff count = %d, want 2", got)
	}

	dequeued := <-sequence.packs
	sequence.releasePackQueue(dequeued)
	dequeued.messagePoolReturn()
	if accepted, err := sequence.Pack(third, 0); !accepted || err != nil {
		t.Fatalf("readmit after dequeue: accepted=%t err=%v", accepted, err)
	}
	sequence.Close()
	if got := sequence.packQueueByteCount.Load(); got != 0 {
		t.Fatalf("closed sequence retained %d handoff bytes", got)
	}
	if got := sequence.packQueueCount.Load(); got != 0 {
		t.Fatalf("closed sequence retained %d handoff items", got)
	}
}

// Per-sequence burst limits are not an aggregate bound: many browser flows can
// otherwise each retain their full allowance at once. The optional shared
// budget admits across sequences exactly and releases at channel dequeue.
func TestReceiveSequenceHandoffEnforcesSharedPackBudget(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	budget := NewTransferMemoryBudget(60)
	settings := DefaultReceiveBufferSettingsWithBufferSize(8)
	settings.SequenceBufferByteCount = 1024
	settings.H1SequenceBufferByteCount = 1024
	settings.PackQueueBudget = budget
	firstSequence := newReceiveSequence(
		ctx,
		&Client{},
		SourceId(NewId()),
		NewId(),
		TransferKey{},
		settings,
	)
	secondSequence := newReceiveSequence(
		ctx,
		&Client{},
		SourceId(NewId()),
		NewId(),
		TransferKey{},
		settings,
	)
	defer firstSequence.Close()
	defer secondSequence.Close()

	newPack := func() *ReceivePack {
		return &ReceivePack{
			MessageByteCount:   40,
			TransferFrameBytes: MessagePoolGet(40),
			TransportType:      TransportTypeH1,
		}
	}
	first := newPack()
	second := newPack()
	if accepted, err := firstSequence.Pack(first, 0); !accepted || err != nil {
		t.Fatalf("first shared-budget pack: accepted=%t err=%v", accepted, err)
	}
	if accepted, err := secondSequence.Pack(second, 0); accepted || err != nil {
		t.Fatalf("aggregate-overflow pack: accepted=%t err=%v", accepted, err)
	}
	if got := budget.UsedByteCount(); got != 40 {
		t.Fatalf("shared pack budget used=%d, want 40", got)
	}
	if got := secondSequence.packQueueByteCount.Load(); got != 0 {
		t.Fatalf("rejected sequence retained %d local bytes", got)
	}

	dequeued := <-firstSequence.packs
	firstSequence.releasePackQueue(dequeued)
	dequeued.messagePoolReturn()
	if got := budget.UsedByteCount(); got != 0 {
		t.Fatalf("shared pack budget after dequeue=%d, want 0", got)
	}
	if accepted, err := secondSequence.Pack(second, 0); !accepted || err != nil {
		t.Fatalf("readmit after shared release: accepted=%t err=%v", accepted, err)
	}
	dequeued = <-secondSequence.packs
	secondSequence.releasePackQueue(dequeued)
	dequeued.messagePoolReturn()
	if got := budget.UsedByteCount(); got != 0 {
		t.Fatalf("shared pack budget after final dequeue=%d, want 0", got)
	}
	reserved, released := budget.Counts()
	if reserved != released || reserved != 80 {
		t.Fatalf("shared pack budget counts=(%d,%d), want (80,80)", reserved, released)
	}
}

// One ordered channel can absorb an H1 burst without silently widening H3.
// Carrier changes preserve arrival order because both limits govern the same
// queue rather than using a secondary overflow channel.
func TestReceiveSequenceH1HandoffUsesCarrierSpecificCountLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultReceiveBufferSettingsWithBufferSize(2)
	settings.H1SequenceBufferSize = 4
	settings.SequenceBufferByteCount = 1024
	client := &Client{}
	sequence := newReceiveSequence(
		ctx,
		client,
		SourceId(NewId()),
		NewId(),
		TransferKey{},
		settings,
	)
	defer sequence.Close()
	defer cancel()
	if got := cap(sequence.packs); got != 4 {
		t.Fatalf("carrier-aware handoff channel capacity = %d, want 4", got)
	}

	newPack := func(transportType TransportType) *ReceivePack {
		return &ReceivePack{
			MessageByteCount:   40,
			TransferFrameBytes: MessagePoolGet(40),
			TransportType:      transportType,
		}
	}
	first := newPack(TransportTypeH3)
	second := newPack(TransportTypeH3)
	third := newPack(TransportTypeH3)
	for index, pack := range []*ReceivePack{first, second} {
		if accepted, err := sequence.Pack(pack, 0); !accepted || err != nil {
			t.Fatalf("admit H3 pack %d: accepted=%t err=%v", index, accepted, err)
		}
	}
	if accepted, err := sequence.Pack(third, 0); accepted || err != nil {
		t.Fatalf("H3 count overflow: accepted=%t err=%v", accepted, err)
	}
	third.messagePoolReturn()

	for _, want := range []*ReceivePack{first, second} {
		got := <-sequence.packs
		if got != want {
			t.Fatalf("H3 queue order got %p, want %p", got, want)
		}
		sequence.releasePackQueue(got)
		got.messagePoolReturn()
	}

	h1Packs := make([]*ReceivePack, 4)
	for index := range h1Packs {
		h1Packs[index] = newPack(TransportTypeH1)
		if accepted, err := sequence.Pack(h1Packs[index], 0); !accepted || err != nil {
			t.Fatalf("admit H1 pack %d: accepted=%t err=%v", index, accepted, err)
		}
	}
	overflow := newPack(TransportTypeH1)
	if accepted, err := sequence.Pack(overflow, 0); accepted || err != nil {
		t.Fatalf("H1 count overflow: accepted=%t err=%v", accepted, err)
	}
	overflow.messagePoolReturn()
	if got := sequence.packQueueCount.Load(); got != 4 {
		t.Fatalf("H1 retained handoff count = %d, want 4", got)
	}
	stats := client.ReceiveStats()
	if stats.PackHandoffMaxCount != 4 || stats.PackHandoffMaxByteCount != 160 {
		t.Fatalf("H1 handoff high water = (%d, %d), want (4, 160)", stats.PackHandoffMaxCount, stats.PackHandoffMaxByteCount)
	}
}

// The H1 byte reserve follows the same carrier discriminator as the count
// reserve. H3 remains bounded by the base budget and mixed arrivals retain one
// ordered channel; releasing an H3 item immediately restores base admission.
func TestReceiveSequenceH1HandoffUsesCarrierSpecificByteLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultReceiveBufferSettingsWithBufferSize(8)
	settings.SequenceBufferByteCount = 80
	settings.H1SequenceBufferByteCount = 160
	sequence := newReceiveSequence(
		ctx,
		&Client{},
		SourceId(NewId()),
		NewId(),
		TransferKey{},
		settings,
	)
	defer sequence.Close()

	newPack := func(transportType TransportType) *ReceivePack {
		return &ReceivePack{
			MessageByteCount:   40,
			TransferFrameBytes: MessagePoolGet(40),
			TransportType:      transportType,
		}
	}
	first := newPack(TransportTypeH3)
	second := newPack(TransportTypeH3)
	third := newPack(TransportTypeH3)
	for index, pack := range []*ReceivePack{first, second} {
		if accepted, err := sequence.Pack(pack, 0); !accepted || err != nil {
			t.Fatalf("admit H3 pack %d: accepted=%t err=%v", index, accepted, err)
		}
	}
	if accepted, err := sequence.Pack(third, 0); accepted || err != nil {
		t.Fatalf("H3 byte overflow: accepted=%t err=%v", accepted, err)
	}
	third.messagePoolReturn()

	dequeued := <-sequence.packs
	if dequeued != first {
		t.Fatalf("mixed queue first pack = %p, want %p", dequeued, first)
	}
	sequence.releasePackQueue(dequeued)
	dequeued.messagePoolReturn()

	h1First := newPack(TransportTypeH1)
	h1Second := newPack(TransportTypeH1)
	for index, pack := range []*ReceivePack{h1First, h1Second} {
		if accepted, err := sequence.Pack(pack, 0); !accepted || err != nil {
			t.Fatalf("admit H1 reserve pack %d: accepted=%t err=%v", index, accepted, err)
		}
	}
	if got := sequence.packQueueByteCount.Load(); got != 120 {
		t.Fatalf("mixed H1 handoff bytes = %d, want 120", got)
	}

	h3Overflow := newPack(TransportTypeH3)
	if accepted, err := sequence.Pack(h3Overflow, 0); accepted || err != nil {
		t.Fatalf("H3 consumed H1 byte reserve: accepted=%t err=%v", accepted, err)
	}
	h3Overflow.messagePoolReturn()
	h1Third := newPack(TransportTypeH1)
	if accepted, err := sequence.Pack(h1Third, 0); !accepted || err != nil {
		t.Fatalf("H1 byte reserve rejected: accepted=%t err=%v", accepted, err)
	}
	if got := sequence.packQueueByteCount.Load(); got != 160 {
		t.Fatalf("full H1 handoff bytes = %d, want 160", got)
	}
}

func TestReceiveSequenceH1HandoffDeepensAfterContinuousSaturation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultReceiveBufferSettingsWithBufferSize(2)
	settings.H1SequenceBufferSize = 2
	settings.H1SequenceBufferAdaptiveMaxSize = 6
	settings.H1SequenceBufferAdaptiveStepSize = 2
	settings.H1SequenceBufferAdaptiveSaturationThreshold = 2
	settings.H1SequenceBufferAdaptiveSaturationWindow = time.Hour
	settings.SequenceBufferByteCount = 1024
	settings.H1SequenceBufferByteCount = 1024
	settings.H1SequenceBufferAdaptiveMaxByteCount = 2048
	settings.H1SequenceBufferAdaptiveStepByteCount = 512
	client := &Client{}
	sequence := newReceiveSequence(
		ctx,
		client,
		SourceId(NewId()),
		NewId(),
		TransferKey{},
		settings,
	)
	defer sequence.Close()
	if got := cap(sequence.packs); got != 6 {
		t.Fatalf("adaptive channel hard capacity = %d, want 6", got)
	}
	if got := sequence.packQueueH1Limit; got != 2 {
		t.Fatalf("adaptive initial depth = %d, want 2", got)
	}

	newPack := func() *ReceivePack {
		return &ReceivePack{
			MessageByteCount:   40,
			TransferFrameBytes: MessagePoolGet(40),
			TransportType:      TransportTypeH1,
		}
	}
	queued := make([]*ReceivePack, 0, 6)
	admit := func() *ReceivePack {
		pack := newPack()
		accepted, err := sequence.Pack(pack, 0)
		if !accepted || err != nil {
			pack.messagePoolReturn()
			t.Fatalf("adaptive admission: accepted=%t err=%v", accepted, err)
		}
		queued = append(queued, pack)
		return pack
	}
	reject := func() {
		pack := newPack()
		accepted, err := sequence.Pack(pack, 0)
		pack.messagePoolReturn()
		if accepted || err != nil {
			t.Fatalf("adaptive saturation: accepted=%t err=%v", accepted, err)
		}
	}
	drainOne := func() {
		pack := <-sequence.packs
		sequence.releasePackQueue(pack)
		pack.messagePoolReturn()
		queued = queued[1:]
	}

	admit()
	admit()
	reject() // first full episode: retain depth 2
	if got := sequence.packQueueH1Limit; got != 2 {
		t.Fatalf("depth after first saturation = %d, want 2", got)
	}
	drainOne()
	admit() // the next full episode remains inside the continuity window
	admit() // second full episode grants 2 -> 4 and admits this Pack
	if got := sequence.packQueueH1Limit; got != 4 {
		t.Fatalf("depth after second saturation = %d, want 4", got)
	}
	admit()
	reject()
	drainOne()
	admit()
	admit() // second sustained episode at four grants 4 -> 6
	if got := sequence.packQueueH1Limit; got != 6 {
		t.Fatalf("depth after fourth saturation = %d, want 6", got)
	}
	if got := sequence.packQueueH1ByteLimit; got != 2048 {
		t.Fatalf("byte depth after fourth saturation = %d, want 2048", got)
	}

	stats := client.ReceiveStats()
	if stats.PackHandoffSaturationCount != 4 ||
		stats.PackHandoffDepthGrowCount != 2 ||
		stats.PackHandoffDeepenedFlows != 1 ||
		stats.PackHandoffAdaptiveMaxDepth != 6 ||
		stats.PackHandoffAdaptiveMaxByteCount != 2048 {
		t.Fatalf("adaptive handoff stats = %+v", stats)
	}
	for len(queued) > 0 {
		drainOne()
	}
}

func TestReceiveSequenceH1HandoffDeepensWhenLogicalBytesSaturateFirst(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultReceiveBufferSettingsWithBufferSize(4)
	settings.H1SequenceBufferSize = 4
	settings.H1SequenceBufferAdaptiveMaxSize = 5
	settings.H1SequenceBufferAdaptiveStepSize = 1
	settings.H1SequenceBufferAdaptiveSaturationThreshold = 1
	settings.H1SequenceBufferAdaptiveSaturationWindow = time.Hour
	settings.SequenceBufferByteCount = 80
	settings.H1SequenceBufferByteCount = 80
	settings.H1SequenceBufferAdaptiveMaxByteCount = 160
	settings.H1SequenceBufferAdaptiveStepByteCount = 40
	client := &Client{}
	sequence := newReceiveSequence(
		ctx,
		client,
		SourceId(NewId()),
		NewId(),
		TransferKey{},
		settings,
	)
	defer sequence.Close()
	newPack := func() *ReceivePack {
		return &ReceivePack{
			MessageByteCount:   40,
			TransferFrameBytes: MessagePoolGet(40),
			TransportType:      TransportTypeH1,
		}
	}
	for range 4 {
		pack := newPack()
		if accepted, err := sequence.Pack(pack, 0); !accepted || err != nil {
			pack.messagePoolReturn()
			t.Fatalf("byte-saturated adaptive admission: accepted=%t err=%v", accepted, err)
		}
	}
	if sequence.packQueueH1Limit != 5 || sequence.packQueueH1ByteLimit != 160 {
		t.Fatalf(
			"byte-saturated adaptive limits = %d/%d, want 5/160",
			sequence.packQueueH1Limit,
			sequence.packQueueH1ByteLimit,
		)
	}
	stats := client.ReceiveStats()
	if stats.PackHandoffSaturationCount != 2 ||
		stats.PackHandoffDepthGrowCount != 2 ||
		stats.PackHandoffAdaptiveMaxDepth != 5 ||
		stats.PackHandoffAdaptiveMaxByteCount != 160 {
		t.Fatalf("byte-saturated adaptive stats = %+v", stats)
	}
	for range 4 {
		pack := <-sequence.packs
		sequence.releasePackQueue(pack)
		pack.messagePoolReturn()
	}
}

func TestReceiveSequenceH1HandoffDeepeningExpiresAndExcludesH3(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultReceiveBufferSettingsWithBufferSize(2)
	settings.H1SequenceBufferSize = 2
	settings.H1SequenceBufferAdaptiveMaxSize = 4
	settings.H1SequenceBufferAdaptiveStepSize = 2
	settings.H1SequenceBufferAdaptiveSaturationThreshold = 2
	settings.H1SequenceBufferAdaptiveSaturationWindow = 100 * time.Millisecond
	now := time.Unix(1, 0)
	settings.h1SaturationNowForTest = func() time.Time { return now }
	settings.SequenceBufferByteCount = 1024
	settings.H1SequenceBufferByteCount = 1024
	client := &Client{}
	sequence := newReceiveSequence(
		ctx,
		client,
		SourceId(NewId()),
		NewId(),
		TransferKey{},
		settings,
	)
	defer sequence.Close()
	newPack := func(transportType TransportType) *ReceivePack {
		return &ReceivePack{
			MessageByteCount:   40,
			TransferFrameBytes: MessagePoolGet(40),
			TransportType:      transportType,
		}
	}
	admit := func(transportType TransportType) {
		pack := newPack(transportType)
		if accepted, err := sequence.Pack(pack, 0); !accepted || err != nil {
			pack.messagePoolReturn()
			t.Fatalf("admit %v: accepted=%t err=%v", transportType, accepted, err)
		}
	}
	reject := func(transportType TransportType) {
		pack := newPack(transportType)
		accepted, err := sequence.Pack(pack, 0)
		pack.messagePoolReturn()
		if accepted || err != nil {
			t.Fatalf("reject %v: accepted=%t err=%v", transportType, accepted, err)
		}
	}
	drain := func(count int) {
		for range count {
			pack := <-sequence.packs
			sequence.releasePackQueue(pack)
			pack.messagePoolReturn()
		}
	}

	admit(TransportTypeH1)
	admit(TransportTypeH1)
	reject(TransportTypeH1) // one saturation toward the threshold
	drain(2)
	now = now.Add(200 * time.Millisecond) // outside the continuity window
	admit(TransportTypeH1)
	admit(TransportTypeH1)
	reject(TransportTypeH1)
	if got := sequence.packQueueH1Limit; got != 2 {
		t.Fatalf("depth grew across expired saturation episodes: %d", got)
	}
	drain(2)

	admit(TransportTypeH3)
	admit(TransportTypeH3)
	reject(TransportTypeH3)
	if got := sequence.packQueueH1Limit; got != 2 {
		t.Fatalf("H3 saturation changed H1 adaptive depth to %d", got)
	}
	stats := client.ReceiveStats()
	if stats.PackHandoffSaturationCount != 2 ||
		stats.PackHandoffDepthGrowCount != 0 {
		t.Fatalf("drain/H3 adaptive stats = %+v", stats)
	}
	drain(2)
}

func TestReceiveSequenceH1HandoffDeepeningRequiresCountAndMemoryHeadroom(t *testing.T) {
	for _, test := range []struct {
		name      string
		byteLimit ByteCount
		budget    *TransferMemoryBudget
	}{
		{name: "per-flow-byte-limit", byteLimit: 80},
		{name: "shared-budget", byteLimit: 1024, budget: NewTransferMemoryBudget(80)},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			settings := DefaultReceiveBufferSettingsWithBufferSize(2)
			settings.H1SequenceBufferSize = 2
			settings.H1SequenceBufferAdaptiveMaxSize = 4
			settings.H1SequenceBufferAdaptiveStepSize = 2
			settings.H1SequenceBufferAdaptiveSaturationThreshold = 1
			settings.H1SequenceBufferAdaptiveSaturationWindow = time.Hour
			settings.SequenceBufferByteCount = test.byteLimit
			settings.H1SequenceBufferByteCount = test.byteLimit
			settings.PackQueueBudget = test.budget
			client := &Client{}
			sequence := newReceiveSequence(
				ctx,
				client,
				SourceId(NewId()),
				NewId(),
				TransferKey{},
				settings,
			)
			defer sequence.Close()
			newPack := func() *ReceivePack {
				return &ReceivePack{
					MessageByteCount:   40,
					TransferFrameBytes: MessagePoolGet(40),
					TransportType:      TransportTypeH1,
				}
			}
			for range 2 {
				pack := newPack()
				if accepted, err := sequence.Pack(pack, 0); !accepted || err != nil {
					t.Fatalf("fill bounded queue: accepted=%t err=%v", accepted, err)
				}
			}
			overflow := newPack()
			accepted, err := sequence.Pack(overflow, 0)
			overflow.messagePoolReturn()
			if accepted || err != nil {
				t.Fatalf("memory-bound adaptive overflow: accepted=%t err=%v", accepted, err)
			}
			if got := sequence.packQueueH1Limit; got != 2 {
				t.Fatalf("memory-bound adaptive depth = %d, want 2", got)
			}
			if stats := client.ReceiveStats(); stats.PackHandoffSaturationCount != 1 ||
				stats.PackHandoffDepthGrowCount != 0 {
				t.Fatalf("memory-bound adaptive stats = %+v", stats)
			}
			for range 2 {
				pack := <-sequence.packs
				sequence.releasePackQueue(pack)
				pack.messagePoolReturn()
			}
		})
	}
}

func TestReceiveSequencePackBudgetCanChargeRetainedAllocation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	budget := NewTransferMemoryBudget(500)
	settings := DefaultReceiveBufferSettingsWithBufferSize(4)
	settings.SequenceBufferByteCount = 1024
	settings.H1SequenceBufferByteCount = 1024
	settings.PackQueueBudget = budget
	settings.PackQueueRetainedByteAccounting = true
	sequence := newReceiveSequence(
		ctx,
		&Client{},
		SourceId(NewId()),
		NewId(),
		TransferKey{},
		settings,
	)
	defer sequence.Close()
	newPack := func() *ReceivePack {
		return &ReceivePack{
			MessageByteCount:   40,
			TransferFrameBytes: MessagePoolGet(40),
			TransportType:      TransportTypeH1,
		}
	}
	first := newPack()
	second := newPack()
	if accepted, err := sequence.Pack(first, 0); !accepted || err != nil {
		t.Fatalf("first retained-budget Pack: accepted=%t err=%v", accepted, err)
	}
	if got := budget.UsedByteCount(); got != 256 {
		t.Fatalf("retained Pack budget used = %d, want 256", got)
	}
	if got := sequence.packQueueByteCount.Load(); got != 40 {
		t.Fatalf("logical per-flow Pack bytes = %d, want 40", got)
	}
	if accepted, err := sequence.Pack(second, 0); accepted || err != nil {
		t.Fatalf("retained aggregate overflow: accepted=%t err=%v", accepted, err)
	}
	second.messagePoolReturn()
	dequeued := <-sequence.packs
	sequence.releasePackQueue(dequeued)
	dequeued.messagePoolReturn()
	if got := budget.UsedByteCount(); got != 0 {
		t.Fatalf("retained Pack budget after release = %d, want 0", got)
	}
}

func BenchmarkReceiveSequenceH1AdaptiveHandoffUncontended(b *testing.B) {
	for _, adaptive := range []bool{false, true} {
		name := "fixed"
		if adaptive {
			name = "adaptive"
		}
		b.Run(name, func(b *testing.B) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			settings := DefaultReceiveBufferSettingsWithBufferSize(64)
			settings.H1SequenceBufferSize = 64
			settings.SequenceBufferByteCount = 128 * 1024
			settings.H1SequenceBufferByteCount = 128 * 1024
			if adaptive {
				settings.H1SequenceBufferAdaptiveMaxSize = 128
				settings.H1SequenceBufferAdaptiveStepSize = 16
				settings.H1SequenceBufferAdaptiveSaturationThreshold = 2
				settings.H1SequenceBufferAdaptiveSaturationWindow = 100 * time.Millisecond
			}
			sequence := newReceiveSequence(
				ctx,
				&Client{},
				SourceId(NewId()),
				NewId(),
				TransferKey{},
				settings,
			)
			defer sequence.Close()
			pack := &ReceivePack{
				MessageByteCount:   64,
				TransferFrameBytes: make([]byte, 64),
				TransportType:      TransportTypeH1,
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				accepted, err := sequence.Pack(pack, 0)
				if !accepted || err != nil {
					b.Fatalf("uncontended Pack: accepted=%t err=%v", accepted, err)
				}
				dequeued := <-sequence.packs
				sequence.releasePackQueue(dequeued)
			}
		})
	}
}

func TestPackHandoffTimeoutUsesExactReceiveLaneReliability(t *testing.T) {
	settings := DefaultReceiveBufferSettingsWithBufferSize(1)
	settings.H1PackHandoffTimeout = 200 * time.Millisecond
	settings.ReliablePackHandoffTimeout = -1
	for _, testCase := range []struct {
		name        string
		transport   TransportType
		reliability CarrierReliability
		want        time.Duration
	}{
		{name: "H1 stream", transport: TransportTypeH1, reliability: CarrierReliabilityReliable, want: -1},
		{name: "H3 stream", transport: TransportTypeH3, reliability: CarrierReliabilityReliable, want: -1},
		{name: "DNS QUIC stream", transport: TransportTypeH3Dns, reliability: CarrierReliabilityReliable, want: -1},
		{name: "P2P SCTP", transport: TransportTypeP2p, reliability: CarrierReliabilityReliable, want: -1},
		{name: "H3 DATAGRAM", transport: TransportTypeH3, reliability: CarrierReliabilityUnreliable, want: 0},
		{name: "native P2P", transport: TransportTypeP2p, reliability: CarrierReliabilityUnreliable, want: 0},
		{name: "legacy typed H1", transport: TransportTypeH1, reliability: CarrierReliabilityUnknown, want: 200 * time.Millisecond},
		{name: "legacy typed H3", transport: TransportTypeH3, reliability: CarrierReliabilityUnknown, want: 0},
	} {
		if got := settings.packHandoffTimeout(
			testCase.transport,
			testCase.reliability,
		); got != testCase.want {
			t.Fatalf("%s handoff timeout=%v want=%v", testCase.name, got, testCase.want)
		}
	}
}

// Reproduces the root failure deterministically: a reliable H3 frame arrives
// while the per-sequence handoff is full. The second frame must remain owned by
// the reader and emerge after the first, never become an artificial gap.
func TestReceiveSequenceReliableH3SaturationPreservesOrder(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultReceiveBufferSettingsWithBufferSize(1)
	settings.ReliablePackHandoffTimeout = -1
	client := &Client{}
	sequence := newReceiveSequence(
		ctx,
		client,
		SourceId(NewId()),
		NewId(),
		TransferKey{},
		settings,
	)
	defer sequence.Close()
	newPack := func(marker byte) *ReceivePack {
		message := MessagePoolGet(40)
		message[0] = marker
		return &ReceivePack{
			MessageByteCount:   ByteCount(len(message)),
			TransferFrameBytes: message,
			TransportType:      TransportTypeH3,
		}
	}
	first := newPack(1)
	second := newPack(2)
	if accepted, err := sequence.Pack(first, 0); !accepted || err != nil {
		t.Fatalf("fill reliable H3 handoff: accepted=%t err=%v", accepted, err)
	}
	type packResult struct {
		accepted bool
		err      error
	}
	result := make(chan packResult, 1)
	go func() {
		accepted, err := sequence.Pack(
			second,
			settings.packHandoffTimeout(
				TransportTypeH3,
				CarrierReliabilityReliable,
			),
		)
		result <- packResult{accepted: accepted, err: err}
	}()
	deadline := time.Now().Add(time.Second)
	for client.ReceiveStats().PackHandoffWaitCount == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	select {
	case premature := <-result:
		t.Fatalf("reliable H3 frame was not backpressured: %+v", premature)
	default:
	}
	dequeued := <-sequence.packs
	if dequeued.TransferFrameBytes[0] != 1 {
		t.Fatalf("first marker=%d want=1", dequeued.TransferFrameBytes[0])
	}
	sequence.releasePackQueue(dequeued)
	dequeued.messagePoolReturn()
	select {
	case got := <-result:
		if !got.accepted || got.err != nil {
			t.Fatalf("second reliable H3 frame: accepted=%t err=%v", got.accepted, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("second reliable H3 frame did not resume")
	}
	dequeued = <-sequence.packs
	if dequeued.TransferFrameBytes[0] != 2 {
		t.Fatalf("second marker=%d want=2", dequeued.TransferFrameBytes[0])
	}
	sequence.releasePackQueue(dequeued)
	dequeued.messagePoolReturn()
	if stats := client.ReceiveStats(); stats.PackHandoffDropCount != 0 ||
		stats.PackHandoffWaitSuccess != 1 {
		t.Fatalf("reliable H3 handoff stats=%+v", stats)
	}
}

func TestReceiveSequenceH1HandoffWaitRescuesFullQueue(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultReceiveBufferSettingsWithBufferSize(1)
	settings.H1PackHandoffTimeout = 200 * time.Millisecond
	if got := settings.packHandoffTimeout(TransportTypeH1); got != 200*time.Millisecond {
		t.Fatalf("H1 handoff timeout = %v, want 200ms", got)
	}
	for _, transportType := range []TransportType{TransportTypeUnknown, TransportTypeH3} {
		if got := settings.packHandoffTimeout(transportType); got != 0 {
			t.Fatalf("non-H1 handoff timeout = %v, want zero", got)
		}
	}

	client := &Client{}
	sequence := newReceiveSequence(
		ctx,
		client,
		SourceId(NewId()),
		NewId(),
		TransferKey{},
		settings,
	)
	defer sequence.Close()
	newPack := func() *ReceivePack {
		return &ReceivePack{
			MessageByteCount:   40,
			TransferFrameBytes: MessagePoolGet(40),
			TransportType:      TransportTypeH1,
		}
	}
	first := newPack()
	second := newPack()
	if accepted, err := sequence.Pack(first, 0); !accepted || err != nil {
		t.Fatalf("fill H1 handoff: accepted=%t err=%v", accepted, err)
	}
	type packResult struct {
		accepted bool
		err      error
	}
	result := make(chan packResult, 1)
	go func() {
		accepted, err := sequence.Pack(second, settings.packHandoffTimeout(second.TransportType))
		result <- packResult{accepted: accepted, err: err}
	}()
	deadline := time.Now().Add(time.Second)
	for client.ReceiveStats().PackHandoffWaitCount == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if client.ReceiveStats().PackHandoffWaitCount != 1 {
		t.Fatal("full H1 handoff did not enter bounded wait")
	}
	select {
	case premature := <-result:
		t.Fatalf("H1 handoff wait completed before space: %+v", premature)
	default:
	}

	dequeued := <-sequence.packs
	sequence.releasePackQueue(dequeued)
	dequeued.messagePoolReturn()
	select {
	case got := <-result:
		if !got.accepted || got.err != nil {
			t.Fatalf("H1 handoff wait result: accepted=%t err=%v", got.accepted, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("H1 handoff did not wake after space release")
	}
	if got := client.ReceiveStats().PackHandoffWaitSuccess; got != 1 {
		t.Fatalf("H1 rescued wait count = %d, want 1", got)
	}
	dequeued = <-sequence.packs
	sequence.releasePackQueue(dequeued)
	dequeued.messagePoolReturn()
}

func TestReceiveSequenceH1HandoffNegativeWaitPreservesReliableBackpressure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultReceiveBufferSettingsWithBufferSize(1)
	settings.H1PackHandoffTimeout = -1
	client := &Client{}
	sequence := newReceiveSequence(
		ctx,
		client,
		SourceId(NewId()),
		NewId(),
		TransferKey{},
		settings,
	)
	defer sequence.Close()
	newPack := func() *ReceivePack {
		return &ReceivePack{
			MessageByteCount:   40,
			TransferFrameBytes: MessagePoolGet(40),
			TransportType:      TransportTypeH1,
		}
	}
	first := newPack()
	second := newPack()
	if accepted, err := sequence.Pack(first, 0); !accepted || err != nil {
		t.Fatalf("fill H1 handoff: accepted=%t err=%v", accepted, err)
	}
	type packResult struct {
		accepted bool
		err      error
	}
	result := make(chan packResult, 1)
	go func() {
		accepted, err := sequence.Pack(
			second,
			settings.packHandoffTimeout(second.TransportType),
		)
		result <- packResult{accepted: accepted, err: err}
	}()
	deadline := time.Now().Add(time.Second)
	for client.ReceiveStats().PackHandoffWaitCount == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if client.ReceiveStats().PackHandoffWaitCount != 1 {
		t.Fatal("full H1 handoff did not enter lossless wait")
	}
	// This is longer than the rejected mobile 10-ms timeout. The wait must
	// remain owned by channel capacity rather than turn into a synthetic gap.
	select {
	case premature := <-result:
		t.Fatalf("negative H1 wait completed while full: %+v", premature)
	case <-time.After(25 * time.Millisecond):
	}

	dequeued := <-sequence.packs
	sequence.releasePackQueue(dequeued)
	dequeued.messagePoolReturn()
	select {
	case got := <-result:
		if !got.accepted || got.err != nil {
			t.Fatalf("lossless H1 handoff result: accepted=%t err=%v", got.accepted, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("lossless H1 handoff did not wake after capacity opened")
	}
	if got := client.ReceiveStats(); got.PackHandoffWaitSuccess != 1 ||
		got.PackHandoffDropCount != 0 {
		t.Fatalf("lossless H1 handoff stats = %+v", got)
	}
	dequeued = <-sequence.packs
	sequence.releasePackQueue(dequeued)
	dequeued.messagePoolReturn()
}

func TestReceiveSequenceH1HandoffNegativeWaitCancellationReturnsToCaller(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	settings := DefaultReceiveBufferSettingsWithBufferSize(1)
	settings.H1PackHandoffTimeout = -1
	client := &Client{}
	sequence := newReceiveSequence(
		ctx,
		client,
		SourceId(NewId()),
		NewId(),
		TransferKey{},
		settings,
	)
	defer sequence.Close()
	defer cancel()
	first := &ReceivePack{
		MessageByteCount:   40,
		TransferFrameBytes: MessagePoolGet(40),
		TransportType:      TransportTypeH1,
	}
	second := &ReceivePack{
		MessageByteCount:   40,
		TransferFrameBytes: MessagePoolGet(40),
		TransportType:      TransportTypeH1,
	}
	if accepted, err := sequence.Pack(first, 0); !accepted || err != nil {
		t.Fatalf("fill H1 handoff: accepted=%t err=%v", accepted, err)
	}
	type packResult struct {
		accepted bool
		err      error
	}
	result := make(chan packResult, 1)
	go func() {
		accepted, err := sequence.Pack(
			second,
			settings.packHandoffTimeout(second.TransportType),
		)
		result <- packResult{accepted: accepted, err: err}
	}()
	deadline := time.Now().Add(time.Second)
	for client.ReceiveStats().PackHandoffWaitCount == 0 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if client.ReceiveStats().PackHandoffWaitCount != 1 {
		t.Fatal("full H1 handoff did not enter lossless wait")
	}
	cancel()
	select {
	case got := <-result:
		if got.accepted || got.err == nil {
			t.Fatalf("canceled H1 handoff result: accepted=%t err=%v", got.accepted, got.err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled H1 handoff remained blocked")
	}
	if second.sequenceQueueByteCount != 0 || second.sequenceQueueBudgetByteCount != 0 ||
		second.sequenceQueueBudget != nil {
		t.Fatalf("canceled caller-owned Pack retained queue charge: %+v", second)
	}
	if pooled, _ := MessagePoolCheck(second.TransferFrameBytes); !pooled {
		t.Fatal("canceled H1 Pack did not return ownership to its caller")
	}
	secondBytes := second.TransferFrameBytes
	second.messagePoolReturn()
	if pooled, _ := MessagePoolCheck(secondBytes); pooled {
		t.Fatal("caller did not release canceled H1 Pack ownership")
	}
	dequeued := <-sequence.packs
	sequence.releasePackQueue(dequeued)
	dequeued.messagePoolReturn()
}
