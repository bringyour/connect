package connect

import (
	"context"
	"testing"
	"time"
)

// a writer that reports every route retired, which is what the sequence reads
// to decide a carrier is gone
type retiredRouteTestWriter struct{}

func (self *retiredRouteTestWriter) Write(
	ctx context.Context, transferFrameBytes []byte, timeout time.Duration,
) error {
	return nil
}

func (self *retiredRouteTestWriter) WriteDetailed(
	ctx context.Context, transferFrameBytes []byte, timeout time.Duration,
) (bool, error) {
	return true, nil
}

func (self *retiredRouteTestWriter) GetActiveRoutes() []Route   { return nil }
func (self *retiredRouteTestWriter) GetInactiveRoutes() []Route { return nil }
func (self *retiredRouteTestWriter) transferRouteActive(route Route) bool {
	return false
}

// THROUGHPUTFIX §37.17, guard two. A selective acknowledgement earned by a
// route that has since died is not proof the receiver still holds the item.
//
// What the code assumed. The retired-carrier recovery excluded selectively
// acknowledged items with the comment "the receiver already proved delivery".
// A selective acknowledgement proves the receiver took the item into its hold
// out of order, not that it still has it: when a later arrival does not fit,
// the hold removes an already-acknowledged item to make room and tells no one.
// The sender's mark then stands for SelectiveAckTimeout, sixty seconds, and
// every resend path skips a marked item, so the bytes are invisible until the
// sequence's own ack timeout falls due from the same refreshed send time.
//
// Why a route death is exactly when it happens. The dead route's items are
// resent promptly by this path; each is earlier than what the hold accumulated
// past the hole; so each admission evicts a held item. The failover cell
// measured the result past the hold threshold: 13,500 and 11,300 of 20,000
// messages at 8 and 16 MiB windows, a stall rather than a retransmission cost.
//
// This guard is the one that can be deployed on providers alone, so it is the
// only one that helps clients already in the field. Its cost is redundant
// resends bounded by one window per route death, and a duplicate of an item the
// receiver still holds is discarded by message id.
//
// Prediction, recorded before the run: with the guard, a selectively
// acknowledged item on a retired route is moved to the front with its mark
// cleared and counted; with it off, the item keeps its mark and its resend time
// stays sixty seconds out. Items on a live route are untouched either way.
func TestACarrierChangeVoidsSelectiveAcknowledgements(t *testing.T) {
	sendTime := time.Unix(1_700_000_000, 0)
	currentTime := sendTime.Add(100 * time.Millisecond)

	run := func(void bool) (*SendSequence, []*sendItem, Route) {
		sequence, items := newSelectiveAckRecoveryTestSequence(4, sendTime)
		sequence.client = &Client{}
		sequence.sendBufferSettings.CarrierChangeVoidsSelectiveAck = void
		sequence.contractMultiRouteWriter = &retiredRouteTestWriter{}
		retired := make(Route, 1)
		for _, item := range items {
			item.reliableCarrierObserved = true
			item.reliableRoute = retired
			// the receiver said it is holding these out of order
			item.selectiveAcked = true
			item.resendTime = item.sendTime.Add(
				sequence.sendBufferSettings.SelectiveAckTimeout)
		}
		// one item never rode the reliable carrier, so nothing here should
		// touch it: this is the control that says the guard is scoped
		items[3].reliableCarrierObserved = false
		items[3].reliableRoute = nil

		sequence.scheduleRetiredReliableCarrierRecovery(currentTime)
		return sequence, items, retired
	}

	voided, voidedItems, _ := run(true)
	for _, index := range []int{0, 1, 2} {
		item := voidedItems[index]
		if item.selectiveAcked {
			t.Errorf(
				"item %d kept its selective acknowledgement after the route that earned it was retired; the receiver may have evicted it and nothing would resend it for %s",
				index,
				voided.sendBufferSettings.SelectiveAckTimeout,
			)
		}
		if !item.resendTime.Equal(currentTime) {
			t.Errorf(
				"item %d is due at %s rather than now; a voided acknowledgement has to be resent on this pass, not on its lease",
				index,
				item.resendTime.Sub(sendTime),
			)
		}
		if item.recoveryKind != sendRecoveryCarrierChange {
			t.Errorf(
				"item %d was rescheduled as kind %d rather than the carrier change; evictions ride the unbounded path, not the gap recovery's burst of four per scan",
				index,
				item.recoveryKind,
			)
		}
	}
	if control := voidedItems[3]; !control.selectiveAcked {
		t.Error("an item that never rode the retired carrier lost its acknowledgement, so the guard is not scoped to the dead route")
	}
	if voided.client.carrierChangeSelectiveAckVoidCount.Load() != 3 {
		t.Errorf(
			"counted %d voided acknowledgements rather than 3; the count is how an operator sees this happening at all",
			voided.client.carrierChangeSelectiveAckVoidCount.Load(),
		)
	}

	// the same binary with the guard off is the tree as it was
	held, heldItems, _ := run(false)
	for index, item := range heldItems {
		if !item.selectiveAcked {
			t.Errorf("with the guard off item %d lost its mark, so the arms are not comparable", index)
		}
		if !item.resendTime.Equal(sendTime.Add(held.sendBufferSettings.SelectiveAckTimeout)) {
			t.Errorf("with the guard off item %d moved off its lease", index)
		}
	}
	if held.client.carrierChangeSelectiveAckVoidCount.Load() != 0 {
		t.Error("with the guard off something was still voided")
	}
}
