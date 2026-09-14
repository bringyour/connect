package connect

import (
	"context"
	"testing"
)

func testSendSchedulingKey(port uint16) sendSchedulingKey {
	return sendSchedulingKey{
		ipFlow: ipPacketFlowKey{
			sourcePort: port,
			ipVersion:  4,
		},
		valid: true,
	}
}

func testScheduledPack(key sendSchedulingKey) *SendPack {
	return &SendPack{schedulingKey: key}
}

func TestSendPackSchedulerRoundRobinsActiveFlows(t *testing.T) {
	bulkKey := testSendSchedulingKey(1000)
	interactiveKey := testSendSchedulingKey(2000)
	bulk1 := testScheduledPack(bulkKey)
	bulk2 := testScheduledPack(bulkKey)
	bulk3 := testScheduledPack(bulkKey)
	interactive1 := testScheduledPack(interactiveKey)
	interactive2 := testScheduledPack(interactiveKey)

	scheduler := newSendPackScheduler()
	scheduler.Push(bulk1)
	scheduler.Push(bulk2)
	scheduler.Push(bulk3)
	scheduler.Push(interactive1)
	scheduler.Push(interactive2)

	want := []*SendPack{bulk1, interactive1, bulk2, interactive2, bulk3}
	for index, wantPack := range want {
		got := scheduler.TakeEligible(func(*SendPack) bool { return true })
		if got != wantPack {
			t.Fatalf("scheduled pack %d = %p, want %p", index, got, wantPack)
		}
	}
	if scheduler.Len() != 0 {
		t.Fatalf("scheduler retained %d packs after complete drain", scheduler.Len())
	}
}

func TestSendPackSchedulerFifoPreservesIngressOrder(t *testing.T) {
	scheduler := newSendPackScheduler()
	bulkKey := testSendSchedulingKey(1000)
	interactiveKey := testSendSchedulingKey(2000)
	bulk1 := testScheduledPack(bulkKey)
	interactive := testScheduledPack(interactiveKey)
	bulk2 := testScheduledPack(bulkKey)
	scheduler.Push(bulk1)
	scheduler.Push(interactive)
	scheduler.Push(bulk2)

	eligible := func(*SendPack) bool { return true }
	for _, expected := range []*SendPack{bulk1, interactive, bulk2} {
		if head := scheduler.FifoHead(); head != expected {
			t.Fatalf("fifo head = %p, want %p", head, expected)
		}
		if got := scheduler.TakeFifoEligible(eligible); got != expected {
			t.Fatalf("fifo Pack = %p, want %p", got, expected)
		}
	}
	if scheduler.Len() != 0 {
		t.Fatalf("remaining Pack count = %d, want 0", scheduler.Len())
	}
	if head := scheduler.FifoHead(); head != nil {
		t.Fatalf("empty fifo head = %p, want nil", head)
	}
}

func TestSendPackSchedulerCoalescesOnlySameFlow(t *testing.T) {
	bulkKey := testSendSchedulingKey(1000)
	interactiveKey := testSendSchedulingKey(2000)
	bulk1 := testScheduledPack(bulkKey)
	interactive := testScheduledPack(interactiveKey)
	bulk2 := testScheduledPack(bulkKey)

	scheduler := newSendPackScheduler()
	scheduler.Push(bulk1)
	scheduler.Push(interactive)
	scheduler.Push(bulk2)
	if got := scheduler.TakeEligible(func(*SendPack) bool { return true }); got != bulk1 {
		t.Fatalf("first scheduled pack = %p, want first bulk %p", got, bulk1)
	}
	if got := scheduler.TakeSameFlow(bulkKey); got != bulk2 {
		t.Fatalf("same-flow coalescer = %p, want second bulk %p", got, bulk2)
	}
	if got := scheduler.TakeEligible(func(*SendPack) bool { return true }); got != interactive {
		t.Fatalf("remaining scheduled pack = %p, want interactive %p", got, interactive)
	}
}

func TestSendPackSchedulerEligibilityObservesFlowHeadPack(t *testing.T) {
	scheduler := newSendPackScheduler()
	bulkKey := testSendSchedulingKey(1000)
	interactiveKey := testSendSchedulingKey(2000)
	bulk := testScheduledPack(bulkKey)
	bulk.Ack = true
	interactive := testScheduledPack(interactiveKey)
	interactive.Ack = false
	scheduler.Push(bulk)
	scheduler.Push(interactive)

	noAckEligible := func(sendPack *SendPack) bool { return !sendPack.Ack }
	if !scheduler.HasEligible(noAckEligible) {
		t.Fatal("NoAck flow head was not eligible behind an Ack-required flow")
	}
	if got := scheduler.TakeEligible(noAckEligible); got != interactive {
		t.Fatalf("eligible Pack = %p, want NoAck interactive %p", got, interactive)
	}
	if head := scheduler.FifoHead(); head != bulk {
		t.Fatalf("FIFO head = %p, want blocked Ack Pack %p", head, bulk)
	}
	if got := scheduler.TakeFifoEligible(noAckEligible); got != nil {
		t.Fatalf("FIFO selection bypassed blocked head with %p", got)
	}
}

func TestSendPackAdmissionReservesSlotForNewFlow(t *testing.T) {
	bulkKey := testSendSchedulingKey(1000)
	interactiveKey := testSendSchedulingKey(2000)
	admission := newSendPackAdmission(4)
	for count := range 3 {
		acquired, closed, _ := admission.tryAcquire(bulkKey)
		if !acquired || closed {
			t.Fatalf("bulk admission %d = acquired %t closed %t", count, acquired, closed)
		}
	}
	if acquired, closed, _ := admission.tryAcquire(bulkKey); acquired || closed {
		t.Fatalf("fourth bulk admission = acquired %t closed %t, want reserved", acquired, closed)
	}
	if acquired, closed, _ := admission.tryAcquire(interactiveKey); !acquired || closed {
		t.Fatalf("interactive reserve = acquired %t closed %t", acquired, closed)
	}
	if acquired, closed, _ := admission.tryAcquire(testSendSchedulingKey(3000)); acquired || closed {
		t.Fatalf("admission beyond total cap = acquired %t closed %t", acquired, closed)
	}

	admission.release(bulkKey)
	if acquired, closed, _ := admission.tryAcquire(bulkKey); !acquired || closed {
		t.Fatalf("bulk admission after release = acquired %t closed %t", acquired, closed)
	}
}

func TestSendSequencePackAdmissionUsesFullCapacityWithoutFlowIsolation(t *testing.T) {
	sequence := &SendSequence{
		ctx:           context.Background(),
		packAdmission: newSendPackAdmission(4),
	}
	key := testSendSchedulingKey(1000)
	var sendPacks []*SendPack
	for index := range 4 {
		sendPack := testScheduledPack(key)
		acquired, err, _ := sequence.acquirePackAdmission(sendPack, 0)
		if err != nil || !acquired {
			t.Fatalf("Pack %d admission = %t, %v", index, acquired, err)
		}
		sendPacks = append(sendPacks, sendPack)
	}
	if acquired, err, _ := sequence.acquirePackAdmission(testScheduledPack(key), 0); err != nil || acquired {
		t.Fatalf("Pack beyond total capacity = %t, %v", acquired, err)
	}
	for _, sendPack := range sendPacks {
		sendPack.releaseAdmission()
	}
}

func TestSendSequencePackAdmissionReservesFlowSlotWhenIsolated(t *testing.T) {
	sequence := &SendSequence{
		ctx:           context.Background(),
		packAdmission: newSendPackAdmission(4),
	}
	sequence.flowIsolation.Store(true)
	bulkKey := testSendSchedulingKey(1000)
	interactiveKey := testSendSchedulingKey(2000)
	var sendPacks []*SendPack
	for index := range 3 {
		sendPack := testScheduledPack(bulkKey)
		acquired, err, _ := sequence.acquirePackAdmission(sendPack, 0)
		if err != nil || !acquired {
			t.Fatalf("bulk Pack %d admission = %t, %v", index, acquired, err)
		}
		sendPacks = append(sendPacks, sendPack)
	}
	if acquired, err, _ := sequence.acquirePackAdmission(testScheduledPack(bulkKey), 0); err != nil || acquired {
		t.Fatalf("bulk Pack in reserved slot = %t, %v", acquired, err)
	}
	interactivePack := testScheduledPack(interactiveKey)
	if acquired, err, _ := sequence.acquirePackAdmission(interactivePack, 0); err != nil || !acquired {
		t.Fatalf("interactive Pack reserve = %t, %v", acquired, err)
	}
	sendPacks = append(sendPacks, interactivePack)
	for _, sendPack := range sendPacks {
		sendPack.releaseAdmission()
	}
}

func TestRawSendPackRecycleReleasesAdmission(t *testing.T) {
	key := testSendSchedulingKey(1000)
	admission := newSendPackAdmission(1)
	acquired, closed, _ := admission.tryAcquire(key)
	if !acquired || closed {
		t.Fatalf("initial admission = acquired %t closed %t", acquired, closed)
	}
	pool := make(chan *SendPack, 1)
	sendPack := &SendPack{
		schedulingKey: key,
		admission:     admission,
		admissionKey:  key,
		rawPool:       pool,
	}
	sendPack.releaseRaw()
	if recycled := <-pool; recycled != sendPack {
		t.Fatalf("recycled pack = %p, want %p", recycled, sendPack)
	}
	if acquired, closed, _ := admission.tryAcquire(key); !acquired || closed {
		t.Fatalf("admission after raw recycle = acquired %t closed %t", acquired, closed)
	}
}

func TestSendFlightControllerReservesMessageForNewFlow(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.UnreliableInitialFlightByteCount = 4096
	settings.UnreliableMinimumFlightByteCount = 4096
	settings.UnreliableMaximumFlightByteCount = 4096
	settings.UnreliableInitialFlightMessageCount = 4
	settings.UnreliableMinimumFlightMessageCount = 4
	settings.UnreliableMaximumFlightMessageCount = 4
	controller := newSendFlightController(settings)
	controller.applyPolicy(transferFlightPolicySnapshot{
		generation:  1,
		limited:     true,
		flowReserve: true,
	})
	bulkKey := testSendSchedulingKey(1000)
	interactiveKey := testSendSchedulingKey(2000)

	for range 4 {
		if !controller.canSendForKey(bulkKey) {
			t.Fatal("bulk flow closed before reaching the ordinary total cap")
		}
		controller.sendForKey(100, bulkKey)
	}
	if controller.canSendForKey(bulkKey) {
		t.Fatal("bulk flow exceeded the ordinary total cap")
	}
	if !controller.canSendForKey(interactiveKey) {
		t.Fatal("new interactive flow could not consume on-demand reserve")
	}
	if reserved := controller.sendForKey(100, interactiveKey); !reserved {
		t.Fatal("interactive message was not marked as the on-demand reserve")
	}
	if controller.canSendForKey(testSendSchedulingKey(3000)) {
		t.Fatal("controller admitted more than one on-demand reserve")
	}

	controller.acknowledgeForKey(100, interactiveKey, true)
	if controller.canSendForKey(bulkKey) {
		t.Fatal("full ordinary bulk flight reopened without a bulk acknowledgement")
	}
	controller.acknowledgeForKey(100, bulkKey, false)
	if !controller.canSendForKey(bulkKey) {
		t.Fatal("bulk flow did not reopen after one ordinary acknowledgement")
	}
}
