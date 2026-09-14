package connect

import (
	"testing"
)

func TestMultiClientMemorySnapshotIsPrimitiveAndAllocationFree(t *testing.T) {
	qualityTransferClient := &Client{}
	qualityTransferClient.receivePackHandoffDropCount.Store(2)
	qualityTransferClient.receivePackHandoffDropByteCount.Store(2200)
	qualityTransferClient.receivePackHandoffMaxCount.Store(4)
	qualityTransferClient.receivePackHandoffMaxByteCount.Store(4400)
	qualityTransferClient.receivePackHandoffSaturationCount.Store(17)
	qualityTransferClient.receivePackHandoffDepthGrowCount.Store(2)
	qualityTransferClient.receivePackHandoffDeepenedFlowCount.Store(1)
	qualityTransferClient.receivePackHandoffAdaptiveMaxDepth.Store(80)
	qualityTransferClient.receivePackHandoffAdaptiveMaxByteCount.Store(160 * 1024)
	qualityTransferClient.receiveAckHandoffDropCount.Store(3)
	qualityTransferClient.receiveAckHandoffQueueFullCount.Store(5)
	qualityTransferClient.receiveAckHandoffMissCount.Store(7)
	qualityTransferClient.receiveAckHandoffWaitCount.Store(11)
	qualityTransferClient.receiveAckHandoffWaitSuccess.Store(13)
	qualityTransferClient.receiveAckRouteWriteCount.Store(5)
	qualityTransferClient.receiveAckRoutePriorityWriteCount.Store(2)
	qualityTransferClient.receiveAckRouteWriteBlockedCount.Store(7)
	qualityTransferClient.receiveAckRouteWriteErrorCount.Store(11)
	qualityTransferClient.receiveAckRouteWriteWaitNanoseconds.Store(13)
	qualityTransferClient.receiveAckRouteWriteMaxWaitNanos.Store(17)
	qualityTransferClient.initialSendWriteCount.Store(37)
	qualityTransferClient.initialSendFrameCount.Store(41)
	qualityTransferClient.initialSendMessageByteCount.Store(4300)
	qualityTransferClient.timeoutResendWriteCount.Store(5)
	qualityTransferClient.ackPendingResendPreemptCount.Store(3)
	qualityTransferClient.selectiveGapWriteCount.Store(7)
	qualityTransferClient.recoveryWriteErrorCount.Store(11)
	qualityClient := &multiClientChannel{client: qualityTransferClient}

	speedTransferClient := &Client{}
	speedTransferClient.receivePackHandoffDropCount.Store(13)
	speedTransferClient.receivePackHandoffDropByteCount.Store(14300)
	speedTransferClient.receivePackHandoffMaxCount.Store(9)
	speedTransferClient.receivePackHandoffMaxByteCount.Store(9900)
	speedTransferClient.receivePackHandoffSaturationCount.Store(19)
	speedTransferClient.receivePackHandoffDepthGrowCount.Store(3)
	speedTransferClient.receivePackHandoffDeepenedFlowCount.Store(2)
	speedTransferClient.receivePackHandoffAdaptiveMaxDepth.Store(96)
	speedTransferClient.receivePackHandoffAdaptiveMaxByteCount.Store(192 * 1024)
	speedTransferClient.receiveAckHandoffDropCount.Store(17)
	speedTransferClient.receiveAckHandoffQueueFullCount.Store(19)
	speedTransferClient.receiveAckHandoffMissCount.Store(23)
	speedTransferClient.receiveAckHandoffWaitCount.Store(29)
	speedTransferClient.receiveAckHandoffWaitSuccess.Store(31)
	speedTransferClient.receiveAckRouteWriteCount.Store(19)
	speedTransferClient.receiveAckRoutePriorityWriteCount.Store(3)
	speedTransferClient.receiveAckRouteWriteBlockedCount.Store(23)
	speedTransferClient.receiveAckRouteWriteErrorCount.Store(29)
	speedTransferClient.receiveAckRouteWriteWaitNanoseconds.Store(31)
	speedTransferClient.receiveAckRouteWriteMaxWaitNanos.Store(37)
	speedTransferClient.initialSendWriteCount.Store(43)
	speedTransferClient.initialSendFrameCount.Store(47)
	speedTransferClient.initialSendMessageByteCount.Store(5300)
	speedTransferClient.carrierChangeWriteCount.Store(19)
	speedTransferClient.ackPendingResendPreemptCount.Store(5)
	speedTransferClient.ackTailProbeWriteCount.Store(23)
	speedTransferClient.cumulativeProbeWriteCount.Store(29)
	speedTransferClient.recoveryWriteErrorCount.Store(31)
	speedClient := &multiClientChannel{client: speedTransferClient}
	flowUpdateA := &multiClientChannelUpdate{}
	flowUpdateB := &multiClientChannelUpdate{}
	multi := &RemoteUserNatMultiClient{
		windows: map[WindowType]*multiClientWindow{
			WindowTypeQuality: &multiClientWindow{
				clients: map[Id]*multiClientChannel{NewId(): qualityClient},
			},
			WindowTypeSpeed: &multiClientWindow{
				clients: map[Id]*multiClientChannel{NewId(): speedClient},
			},
		},
		flowUpdates: map[*multiClientChannelUpdate]bool{
			flowUpdateA: true,
			flowUpdateB: true,
		},
	}
	snapshot := multi.MemorySnapshot()
	if snapshot.QualityClientCount != 1 || snapshot.SpeedClientCount != 1 || snapshot.FlowCount != 2 {
		t.Fatalf("memory snapshot = %+v", snapshot)
	}
	if snapshot.PackHandoffDropCount != 15 ||
		snapshot.PackHandoffDropByteCount != 16500 ||
		snapshot.PackHandoffMaxCount != 9 ||
		snapshot.PackHandoffMaxByteCount != 9900 ||
		snapshot.PackHandoffSaturationCount != 36 ||
		snapshot.PackHandoffDepthGrowCount != 5 ||
		snapshot.PackHandoffDeepenedFlows != 3 ||
		snapshot.PackHandoffAdaptiveMaxDepth != 96 ||
		snapshot.PackHandoffAdaptiveMaxByteCount != 192*1024 ||
		snapshot.AckHandoffDropCount != 20 ||
		snapshot.AckHandoffQueueFullCount != 24 ||
		snapshot.AckHandoffMissCount != 30 ||
		snapshot.AckHandoffWaitCount != 40 ||
		snapshot.AckHandoffWaitSuccess != 44 ||
		snapshot.AckRouteWriteCount != 24 ||
		snapshot.AckRoutePriorityWriteCount != 5 ||
		snapshot.AckRouteWriteBlockedCount != 30 ||
		snapshot.AckRouteWriteErrorCount != 40 ||
		snapshot.AckRouteWriteWaitNanos != 44 ||
		snapshot.AckRouteWriteMaxWaitNanos != 37 ||
		snapshot.InitialWriteCount != 80 ||
		snapshot.InitialFrameCount != 88 ||
		snapshot.InitialMessageByteCount != 9600 ||
		snapshot.TimeoutResendWriteCount != 5 ||
		snapshot.AckPendingResendPreemptCount != 8 ||
		snapshot.CarrierChangeWriteCount != 19 ||
		snapshot.SelectiveGapWriteCount != 7 ||
		snapshot.AckTailProbeWriteCount != 23 ||
		snapshot.CumulativeProbeWriteCount != 29 ||
		snapshot.RecoveryWriteErrorCount != 42 {
		t.Fatalf("memory transfer counters = %+v", snapshot)
	}
	if allocations := testing.AllocsPerRun(100, func() {
		_ = multi.MemorySnapshot()
	}); allocations != 0 {
		t.Fatalf("MemorySnapshot allocations/run = %.2f, want 0", allocations)
	}
}
