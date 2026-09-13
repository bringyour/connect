package connect

// FLIGHTGATEFIX §27.4. The arms this tree can build, and the two hooks the
// portable contract file leaves for a tree that has the lane rule. A tree
// without these settings keeps the contract file's defaults, which is how
// merged 89e1633 runs the same file unchanged.

import (
	"fmt"
	"time"
)

func init() {
	laneRecoveryArmsForTree = func() []laneRecoveryArm {
		return []laneRecoveryArm{
			{
				name: "175d82a",
				configure: func(settings *SendBufferSettings) {
					settings.ReliableLaneProvenRecovery = false
				},
			},
			{
				name: "§26.2  ",
				configure: func(settings *SendBufferSettings) {
					settings.ReliableLaneProvenRecovery = true
				},
				readsLanes: true,
			},
		}
	}
	laneRecoveryRecordAcksForTree = func(sequence *SendSequence, items []*sendItem) {
		for _, item := range items {
			sequence.observeLaneAck(item, time.Now())
		}
	}
	laneRecoveryLongestGapForTree = func(
		stats ClientSendRecoveryStatsSnapshot,
	) (time.Duration, bool) {
		return stats.ReliableLaneLongestAckGap, true
	}
	laneRecoveryDetailForTree = func(stats ClientSendRecoveryStatsSnapshot) string {
		return fmt.Sprintf(
			", deferred %d, probes %d, rides %d, endpoint %d, longest lane gap %s at interval %s with %d outstanding",
			stats.TimeoutResendDeferCount, stats.LaneProbeWriteCount,
			stats.LaneProbeRideCount, stats.LaneProvenTimeoutWriteCount,
			stats.ReliableLaneLongestAckGap.Truncate(time.Millisecond),
			stats.ReliableLaneStallOnsetInterval.Truncate(time.Millisecond),
			stats.ReliableLaneStallOnsetOutstanding,
		)
	}
	metricDeferredGapForTree = func(
		stats ClientSendRecoveryStatsSnapshot,
	) (uint64, uint64, bool, bool) {
		return stats.SelectiveGapWritesOfDeferredItems[gapHoleCarrierReliable],
			stats.SelectiveGapWritesOfDeferredItems[gapHoleCarrierUnreliable],
			true, true
	}
	deadRouteExposureForTree = func(
		stats ClientSendRecoveryStatsSnapshot,
	) (time.Duration, uint64, bool) {
		return stats.RouteUnacknowledgedDuration, stats.RouteRetainedItemCount, true
	}
	metricRouteGenerationsForTree = func(
		stats ClientSendRecoveryStatsSnapshot,
	) (uint64, bool) {
		return stats.RouteGenerationChangeCount, true
	}
	laneRecoveryRecordSendsForTree = func(sequence *SendSequence, items []*sendItem) {
		for _, item := range items {
			sequence.observeLaneSend(item)
		}
	}
	laneRecoveryTimerVerdictForTree = func(sequence *SendSequence, item *sendItem) string {
		switch sequence.laneTimerVerdictFor(item, time.Now()) {
		case laneTimerEndpointDrop:
			return "endpoint drop"
		case laneTimerDraining:
			return "draining"
		case laneTimerSilent:
			return "silent"
		}
		return "as today"
	}
	laneRecoveryRidesAndProbesForTree = func(
		stats ClientSendRecoveryStatsSnapshot,
	) (uint64, uint64) {
		return stats.LaneProbeRideCount, stats.LaneProbeWriteCount
	}
	laneRecoveryReadsLanesForTree = func(sequence *SendSequence) bool {
		if sequence != nil {
			return sequence.sendBufferSettings.ReliableLaneProvenRecovery
		}
		return DefaultSendBufferSettings().ReliableLaneProvenRecovery
	}
}
