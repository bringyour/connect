package connect

import (
	"testing"
	"time"
)

func TestRttWindow(t *testing.T) {
	rttWindow := NewRttWindow(nil, 4, 1*time.Second, 1.0, 0, 0, time.Second)

	AssertEqual(t, rttWindow.ScaledRtt(), time.Duration(0))

	start := time.Now()

	tag1 := rttWindow.openTag(start)
	tag2 := rttWindow.openTag(start.Add(50 * time.Millisecond))
	tag3 := rttWindow.openTag(start.Add(100 * time.Millisecond))
	tag4 := rttWindow.openTag(start.Add(150 * time.Millisecond))

	AssertEqual(t, rttWindow.scaledRtt(start.Add(150*time.Millisecond)), time.Duration(0))

	rttWindow.closeTag(tag2, start.Add(300*time.Millisecond)) // 250

	AssertEqual(t, rttWindow.ScaledRtt(), 250*time.Millisecond)

	rttWindow.closeTag(tag4, start.Add(300*time.Millisecond)) // 150
	rttWindow.closeTag(tag3, start.Add(500*time.Millisecond)) // 400
	rttWindow.closeTag(tag1, start.Add(800*time.Millisecond)) // 800

	AssertEqual(t, rttWindow.scaledRtt(start.Add(800*time.Millisecond)), (250+150+400+800)/4*time.Millisecond)

	start2 := start.Add(2 * time.Second)
	tag21 := rttWindow.openTag(start2)
	tag22 := rttWindow.openTag(start2)
	tag23 := rttWindow.openTag(start2)
	tag24 := rttWindow.openTag(start2)
	tag25 := rttWindow.openTag(start2)

	// clears the window
	rttWindow.closeTag(tag21, start2.Add(500*time.Millisecond))

	AssertEqual(t, rttWindow.scaledRtt(start2.Add(500*time.Millisecond)), 500*time.Millisecond)

	rttWindow.closeTag(tag22, start2.Add(500*time.Millisecond))

	AssertEqual(t, rttWindow.scaledRtt(start2.Add(500*time.Millisecond)), 500*time.Millisecond)

	rttWindow.closeTag(tag23, start2.Add(500*time.Millisecond))
	rttWindow.closeTag(tag24, start2.Add(500*time.Millisecond))

	// cycle window
	rttWindow.closeTag(tag25, start2.Add(100*time.Millisecond))

	AssertEqual(t, rttWindow.scaledRtt(start2.Add(100*time.Millisecond)), (500+500+500+100)/4*time.Millisecond)
}

// TestRttWindowColdVsSampledFloor pins the two-floor semantics: the
// conservative cold floor applies while no samples exist, and the (smaller)
// rtt floor applies once the path is measured.
func TestRttWindowColdVsSampledFloor(t *testing.T) {
	rttWindow := NewRttWindow(nil, 4, 10*time.Second, 2.0, 2*time.Second, 300*time.Millisecond, 8*time.Second)

	// cold: no samples -> the conservative floor
	AssertEqual(t, rttWindow.ScaledRtt(), 2*time.Second)

	start := time.Now()
	tag := rttWindow.openTag(start)
	rttWindow.closeTag(tag, start.Add(50*time.Millisecond))

	// sampled fast path: 50ms * 2.0 = 100ms, floored at the rtt floor
	AssertEqual(t, rttWindow.scaledRtt(start.Add(50*time.Millisecond)), 300*time.Millisecond)

	// sampled slower path: the scaled mean governs once above the floor
	tag2 := rttWindow.openTag(start)
	rttWindow.closeTag(tag2, start.Add(450*time.Millisecond))
	// mean = (50+450)/2 = 250ms, * 2.0 = 500ms
	AssertEqual(t, rttWindow.scaledRtt(start.Add(450*time.Millisecond)), 500*time.Millisecond)

	// after the window ages out (quiet gap), back to the cold floor
	AssertEqual(t, rttWindow.scaledRtt(start.Add(30*time.Second)), 2*time.Second)
}

// A recovery probe is deliberately based on the minimum live RTT sample: the
// ordinary resend timer keeps the conservative mean, while a single bounded
// tail probe must not inherit seconds of sender serialization queueing.
func TestRttWindowProbeUsesMinimumSample(t *testing.T) {
	rttWindow := NewRttWindow(nil, 8, 10*time.Second, 2.0, 2*time.Second, 300*time.Millisecond, 8*time.Second)
	receiveTime := time.Unix(1_700_000_000, 0)
	rttWindow.closeSendTime(
		uint64(receiveTime.Add(-250*time.Millisecond).UnixMilli()),
		receiveTime,
	)
	rttWindow.closeSendTime(
		uint64(receiveTime.Add(-2250*time.Millisecond).UnixMilli()),
		receiveTime,
	)

	AssertEqual(t, rttWindow.scaledRtt(receiveTime), 2500*time.Millisecond)
	AssertEqual(t, rttWindow.probeRtt(receiveTime), 500*time.Millisecond)
}

func TestRttWindowFullCapacityMeanAndMinimumAging(t *testing.T) {
	rttWindow := NewRttWindow(nil, 4, time.Second, 1.0, 0, 0, 10*time.Second)
	base := time.Unix(1_700_000_000, 0)
	addSample := func(receiveTime time.Time, rtt time.Duration) {
		rttWindow.closeSendTime(
			uint64(receiveTime.Add(-rtt).UnixMilli()),
			receiveTime,
		)
	}

	addSample(base, 400*time.Millisecond)
	addSample(base.Add(time.Millisecond), 300*time.Millisecond)
	addSample(base.Add(2*time.Millisecond), 200*time.Millisecond)
	addSample(base.Add(3*time.Millisecond), 100*time.Millisecond)
	AssertEqual(t, rttWindow.scaledRtt(base.Add(3*time.Millisecond)), 250*time.Millisecond)
	AssertEqual(t, rttWindow.probeRtt(base.Add(3*time.Millisecond)), 100*time.Millisecond)

	// The fifth sample evicts exactly the oldest sample while the newer
	// minimum remains available to the receiver-paced probe.
	addSample(base.Add(4*time.Millisecond), 500*time.Millisecond)
	AssertEqual(t, rttWindow.scaledRtt(base.Add(4*time.Millisecond)), 275*time.Millisecond)
	AssertEqual(t, rttWindow.probeRtt(base.Add(4*time.Millisecond)), 100*time.Millisecond)

	// Expiry removes both the mean and monotonic-minimum ownership.
	AssertEqual(t, rttWindow.scaledRtt(base.Add(2*time.Second)), time.Duration(0))
	AssertEqual(t, rttWindow.probeRtt(base.Add(2*time.Second)), time.Duration(0))
}

func TestRttWindowCloseSendTimeDoesNotAllocate(t *testing.T) {
	rttWindow := NewRttWindow(nil, 128, time.Minute, 2.0, 2*time.Second, 300*time.Millisecond, 8*time.Second)
	receiveTime := time.Unix(1_700_000_000, 0)
	allocations := testing.AllocsPerRun(1_000, func() {
		receiveTime = receiveTime.Add(time.Millisecond)
		rttWindow.closeSendTime(
			uint64(receiveTime.Add(-37*time.Millisecond).UnixMilli()),
			receiveTime,
		)
	})
	if allocations != 0 {
		t.Fatalf("RTT Ack accounting allocated %.2f objects/run, want 0", allocations)
	}
}

// BenchmarkRttWindowCloseSendTime measures the per-Ack RTT accounting path.
// Keep the receive clock synthetic so scheduler and wall-clock noise do not
// obscure allocation or CPU changes in this hot path.
func BenchmarkRttWindowCloseSendTime(b *testing.B) {
	rttWindow := NewRttWindow(
		nil,
		128,
		60*time.Second,
		2.0,
		2*time.Second,
		300*time.Millisecond,
		8*time.Second,
	)
	receiveTime := time.Unix(1_700_000_000, 0)

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		receiveTime = receiveTime.Add(time.Millisecond)
		rttWindow.closeSendTime(
			uint64(receiveTime.Add(-37*time.Millisecond).UnixMilli()),
			receiveTime,
		)
	}
}
