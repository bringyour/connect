//go:build !js

package connect

import (
	"os"
	"testing"
	"time"
)

// CONNECT_WEBRTC_CASTEP_MEASURE=1 finds the congestion-avoidance step "knee":
// ca-4mtu (current production) clearly beats RFC 1-MTU, and ca-8mtu beats
// ca-4mtu on a bandwidth-limited link. This sweep asks whether steeper still
// (12/16/24 MTU) keeps winning or begins to overshoot the pipe and self-induce
// queue loss, and whether pairing the winning step with a small cwnd floor
// stacks. Scenarios mirror the two regimes that matter on the network-peer
// path: independent wireless loss and a genuine bandwidth bottleneck with a
// shallow queue. See OPTIMIZENETWORKPEER1.md.
func TestWebRtcSctpCwndCAStepKnee(t *testing.T) {
	if os.Getenv("CONNECT_WEBRTC_CASTEP_MEASURE") == "" {
		t.Skip("set CONNECT_WEBRTC_CASTEP_MEASURE=1")
	}

	const mtu = 1200

	type tuning struct {
		name       string
		cwndCAStep uint32
		minCwnd    uint32
	}
	tunings := []tuning{
		{name: "ca-4mtu", cwndCAStep: 4 * mtu},
		{name: "ca-6mtu", cwndCAStep: 6 * mtu},
		{name: "ca-8mtu", cwndCAStep: 8 * mtu},
		{name: "ca-10mtu", cwndCAStep: 10 * mtu},
		{name: "ca-12mtu", cwndCAStep: 12 * mtu},
		{name: "ca-16mtu", cwndCAStep: 16 * mtu},
		{name: "ca-24mtu", cwndCAStep: 24 * mtu},
		{name: "ca-8mtu-floor-32k", cwndCAStep: 8 * mtu, minCwnd: 32 * 1024},
		{name: "ca-16mtu-floor-32k", cwndCAStep: 16 * mtu, minCwnd: 32 * 1024},
	}

	run := func(label string, mk func(tn tuning) sctpPathMeasureConfig) {
		for _, tn := range tunings {
			result := measureSctpPath(t, mk(tn))
			t.Logf(
				"%s/%s throughput=%.2f MiB/s bulk-p50=%s p95=%s max=%s drops=%d cwnd=%d..%d final=%d srtt=%s",
				label, tn.name,
				float64(result.byteCount)/(1024*1024)/result.elapsed.Seconds(),
				result.midBulkLatency,
				result.bulkLatencyP95,
				result.bulkLatencyMax,
				result.droppedPackets,
				result.minObservedCwnd,
				result.maxObservedCwnd,
				result.finalCwnd,
				result.finalSrtt,
			)
		}
	}

	// wireless independent loss, 1%
	run("loss-1pct", func(tn tuning) sctpPathMeasureConfig {
		return sctpPathMeasureConfig{
			oneWayDelay:            25 * time.Millisecond,
			receiveBufferByteCount: 2 * 1024 * 1024,
			cwndCAStep:             tn.cwndCAStep,
			minCwnd:                tn.minCwnd,
			dropEveryDataPacket:    100,
			warmupByteCount:        1024 * 1024,
			measuredByteCount:      8 * 1024 * 1024,
		}
	})

	// genuine 50 Mbps bottleneck with a shallow 64 KiB queue
	run("rate-50Mbps", func(tn tuning) sctpPathMeasureConfig {
		return sctpPathMeasureConfig{
			oneWayDelay:             25 * time.Millisecond,
			receiveBufferByteCount:  2 * 1024 * 1024,
			cwndCAStep:              tn.cwndCAStep,
			minCwnd:                 tn.minCwnd,
			bottleneckBitsPerSecond: 50 * 1000 * 1000,
			bottleneckBurstBytes:    4 * 1500,
			bottleneckQueueBytes:    64 * 1024,
			warmupByteCount:         1024 * 1024,
			measuredByteCount:       8 * 1024 * 1024,
		}
	})

	// a slower 8 Mbps link where an aggressive step/floor risks self-induced
	// queue loss — the safety check the network-peer path must not regress.
	run("rate-8Mbps", func(tn tuning) sctpPathMeasureConfig {
		return sctpPathMeasureConfig{
			oneWayDelay:             25 * time.Millisecond,
			receiveBufferByteCount:  2 * 1024 * 1024,
			cwndCAStep:              tn.cwndCAStep,
			minCwnd:                 tn.minCwnd,
			bottleneckBitsPerSecond: 8 * 1000 * 1000,
			bottleneckBurstBytes:    4 * 1500,
			bottleneckQueueBytes:    32 * 1024,
			warmupByteCount:         512 * 1024,
			measuredByteCount:       4 * 1024 * 1024,
		}
	})
}
