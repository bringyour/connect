package connect

import (
	"context"
	"testing"
	"time"
)

func testUnreliableRecoverySequence(settings *SendBufferSettings) *SendSequence {
	return &SendSequence{
		sendBufferSettings: settings,
		rttWindow: NewRttWindow(
			nil,
			settings.RttWindowSize,
			settings.RttWindowTimeout,
			settings.RttScale,
			settings.MinResendInterval,
			settings.RttMinResendInterval,
			settings.MaxResendInterval,
		),
	}
}

func TestDefaultUnreliableRecoveryBoundsLowBarSilence(t *testing.T) {
	settings := DefaultSendBufferSettings()
	if settings.UnreliableMaxResendInterval != 2*time.Second {
		t.Fatalf(
			"UnreliableMaxResendInterval=%s, want=2s",
			settings.UnreliableMaxResendInterval,
		)
	}
	if settings.UnreliableAckTimeout != 90*time.Second {
		t.Fatalf(
			"UnreliableAckTimeout=%s, want=90s",
			settings.UnreliableAckTimeout,
		)
	}
}

func TestUnreliableRecoveryPolicyExtendsLifetimeAndCapsBackoff(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.AckTimeout = 30 * time.Second
	sequence := testUnreliableRecoverySequence(settings)
	reliable := transferFlightPolicySnapshot{}
	unreliable := transferFlightPolicySnapshot{limited: true}

	if timeout := sequence.ackTimeoutForPolicy(reliable); timeout != 30*time.Second {
		t.Fatalf("reliable ack timeout=%s, want=30s", timeout)
	}
	if timeout := sequence.ackTimeoutForPolicy(unreliable); timeout != 90*time.Second {
		t.Fatalf("unreliable ack timeout=%s, want=90s", timeout)
	}
	if interval := sequence.resendIntervalForPolicy(reliable, 4); interval != 8*time.Second {
		t.Fatalf("reliable fourth-send interval=%s, want=8s", interval)
	}
	if interval := sequence.resendIntervalForPolicy(unreliable, 4); interval != 2*time.Second {
		t.Fatalf("unreliable fourth-send interval=%s, want=2s", interval)
	}
}

func TestZeroUnreliableRecoverySettingsPreserveReliablePolicy(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.AckTimeout = 30 * time.Second
	settings.UnreliableAckTimeout = 0
	settings.UnreliableMaxResendInterval = 0
	sequence := testUnreliableRecoverySequence(settings)
	unreliable := transferFlightPolicySnapshot{limited: true}

	if timeout := sequence.ackTimeoutForPolicy(unreliable); timeout != 30*time.Second {
		t.Fatalf("zero-extension ack timeout=%s, want=30s", timeout)
	}
	if interval := sequence.resendIntervalForPolicy(unreliable, 4); interval != 8*time.Second {
		t.Fatalf("zero-cap fourth-send interval=%s, want=8s", interval)
	}
}

func TestCarrierRecoveryPolicyFollowsObservedMessageLane(t *testing.T) {
	settings := DefaultSendBufferSettings()
	sequence := testUnreliableRecoverySequence(settings)
	sequence.client = &Client{}
	sequence.flightController = newSendFlightController(settings)
	sequence.flightController.applyPolicy(transferFlightPolicySnapshot{
		generation: 1,
		limited:    true,
	})
	item := &sendItem{
		ackTimeout:         settings.AckTimeout,
		transferFrameBytes: make([]byte, 512),
	}

	sequence.observeCarrierWrite(item, transferWriteDisposition{
		transportType: TransportTypeH1,
		reliable:      true,
	})
	if item.unreliableCarrierObserved || item.unreliableFlightTracked ||
		!item.reliableCarrierObserved ||
		item.hybridReliableCarrierObserved {
		t.Fatalf("reliable H1 stream write changed recovery policy: %+v", item)
	}
	h1Interval := sequence.resendIntervalForItem(item, 1)
	if h1Interval != settings.MinResendInterval ||
		item.ackTimeout != settings.AckTimeout {
		t.Fatalf(
			"reliable H1 recovery = interval %s timeout %s, want %s/%s",
			h1Interval,
			item.ackTimeout,
			settings.MinResendInterval,
			settings.AckTimeout,
		)
	}

	sequence.observeCarrierWrite(item, transferWriteDisposition{
		transportType:  TransportTypeH3,
		reliable:       true,
		hybridReliable: true,
	})
	if !item.reliableCarrierObserved ||
		!item.hybridReliableCarrierObserved {
		t.Fatalf("reliable H3 stream write changed recovery policy: %+v", item)
	}
	if interval := sequence.resendIntervalForItem(item, 1); interval != settings.MaxResendInterval {
		t.Fatalf("reliable H3 recovery interval=%s, want=%s", interval, settings.MaxResendInterval)
	}

	sequence.observeCarrierWrite(item, transferWriteDisposition{
		transportType: TransportTypeH3,
		unreliable:    true,
	})
	if !item.unreliableCarrierObserved || !item.unreliableFlightTracked {
		t.Fatalf("DATAGRAM write did not make recovery sticky: %+v", item)
	}
	unreliableInterval := sequence.resendIntervalForPolicy(
		item.unreliableRecoveryPolicy(),
		10,
	)
	if unreliableInterval != settings.UnreliableMaxResendInterval ||
		item.ackTimeout != settings.UnreliableAckTimeout {
		t.Fatalf(
			"DATAGRAM recovery = interval %s timeout %s, want %s/%s",
			unreliableInterval,
			item.ackTimeout,
			settings.UnreliableMaxResendInterval,
			settings.UnreliableAckTimeout,
		)
	}

	sequence.releaseUnreliableFlight(item)
	sequence.observeCarrierWrite(item, transferWriteDisposition{
		transportType: TransportTypeH1,
		reliable:      true,
	})
	if !item.unreliableCarrierObserved || item.unreliableFlightTracked {
		t.Fatalf("reliable retry erased prior DATAGRAM recovery evidence: %+v", item)
	}
}

func TestSendSequenceUsesOnlyOrdinaryH1TimeoutAsH3FailoverEvidence(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	selector := NewMultiRouteSelector(
		ctx,
		"sequence-h1-timeout",
		nil,
		DestinationId(NewId()),
		true,
	)
	defer selector.Close()
	h1Route := make(Route, 1)
	selector.updateTransport(
		NewSendGatewayTransportWithType(TransportTypeH1),
		[]Route{h1Route},
	)
	selector.updateTransport(
		NewSendGatewayTransportWithType(TransportTypeH3),
		[]Route{make(Route, 1)},
	)
	sequence := testUnreliableRecoverySequence(DefaultSendBufferSettings())
	sequence.contractMultiRouteWriter = selector
	item := &sendItem{
		reliableCarrierObserved: true,
		reliableRoute:           h1Route,
		recoveryKind:            sendRecoveryCarrierChange,
	}
	if sequence.preferH3AfterH1Timeout(item) {
		t.Fatal("route-retirement recovery was treated as an ordinary H1 timeout")
	}
	item.recoveryKind = sendRecoveryNone
	if !sequence.preferH3AfterH1Timeout(item) {
		t.Fatal("ordinary H1 timeout did not fail over to the healthy H3 sibling")
	}
	if sequence.preferH3AfterH1Timeout(item) {
		t.Fatal("stale H1 evidence repeated an already-completed failover")
	}
}

func TestUnreliableFlowReserveUseIsObservableOncePerTrackedMessage(t *testing.T) {
	settings := DefaultSendBufferSettings()
	settings.UnreliableInitialFlightByteCount = 512
	settings.UnreliableMinimumFlightByteCount = 512
	settings.UnreliableMaximumFlightByteCount = 512
	settings.UnreliableInitialFlightMessageCount = 1
	settings.UnreliableMinimumFlightMessageCount = 1
	settings.UnreliableMaximumFlightMessageCount = 1
	sequence := testUnreliableRecoverySequence(settings)
	sequence.client = &Client{}
	sequence.flightController = newSendFlightController(settings)
	sequence.flightController.applyPolicy(transferFlightPolicySnapshot{
		generation:  1,
		limited:     true,
		flowReserve: true,
	})

	bulk := &sendItem{
		transferFrameBytes: make([]byte, 512),
		schedulingKey:      testSendSchedulingKey(1000),
	}
	sequence.observeCarrierWrite(bulk, transferWriteDisposition{unreliable: true})
	if bulk.unreliableFlowReserve {
		t.Fatal("ordinary flight message was marked as reserve")
	}
	interactive := &sendItem{
		transferFrameBytes: make([]byte, 64),
		schedulingKey:      testSendSchedulingKey(2000),
	}
	sequence.observeCarrierWrite(interactive, transferWriteDisposition{unreliable: true})
	sequence.observeCarrierWrite(interactive, transferWriteDisposition{unreliable: true})
	if !interactive.unreliableFlowReserve ||
		sequence.client.SendRecoveryStats().UnreliableFlowReserveUseCount != 1 {
		t.Fatalf(
			"reserve message=%t stats=%+v",
			interactive.unreliableFlowReserve,
			sequence.client.SendRecoveryStats(),
		)
	}
	sequence.releaseUnreliableFlight(interactive)
	sequence.releaseUnreliableFlight(bulk)
}
