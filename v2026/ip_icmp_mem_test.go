package connect

// icmp memory budget validation (see ICMP.md). The provider byte-cost model
// (`providerIcmpFlowByteCount`) is heap attributable — like its udp and tcp
// siblings it excludes goroutine stacks and kernel socket buffers — so these
// tests measure the marginal heap of a live flow against it, report the stack
// cost separately for the record, and pin that an unused icmp path holds
// nothing.

import (
	"context"
	"math"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// heapAndStackInUse settles the process and reports live heap object bytes
// and in-use stack. It reads HeapAlloc rather than HeapInuse: span level
// accounting inherits the fragmentation of whatever ran before this test,
// which makes a marginal measurement order dependent, while HeapAlloc after
// a collection is the live retained bytes these tests mean. Goroutine stacks
// are accounted separately and never appear in the heap figure.
//
// Earlier tests leave goroutines shutting down asynchronously, so this first
// waits for the goroutine count to quiesce; the reported figures are the
// minimum across a few samples, which discards a sample taken mid teardown.
func heapAndStackInUse() (heapByteCount int64, stackByteCount int64) {
	previousCount := -1
	for i := 0; i < 50; i += 1 {
		count := runtime.NumGoroutine()
		if count == previousCount {
			break
		}
		previousCount = count
		time.Sleep(20 * time.Millisecond)
	}

	heapByteCount = int64(math.MaxInt64)
	stackByteCount = int64(math.MaxInt64)
	for i := 0; i < 3; i += 1 {
		runtime.GC()
		debug.FreeOSMemory()
		var stats runtime.MemStats
		runtime.ReadMemStats(&stats)
		heapByteCount = min(heapByteCount, int64(stats.HeapAlloc))
		stackByteCount = min(stackByteCount, int64(stats.StackInuse))
	}
	return heapByteCount, stackByteCount
}

// icmpFlowSender starts a buffer and returns a send function and a live flow
// count for the memory tests.
func icmpFlowSender(t *testing.T, ctx context.Context, settings *IcmpBufferSettings) (send func(identifier uint16), sequenceCount func() int) {
	t.Helper()
	buffer := NewIcmp4Buffer(ctx, func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {}, settings)
	send = func(identifier uint16) {
		t.Helper()
		packet := MessagePoolGet(32)
		parsed := &parsedIcmp{
			sourceIp:       net.IPv4(10, 0, 0, 1).To4(),
			destinationIp:  net.IPv4(127, 0, 0, 1).To4(),
			echoRequest:    true,
			identifier:     identifier,
			sequenceNumber: 1,
			ttl:            64,
			payload:        packet[:4],
		}
		if success, err := buffer.send(SourceId(NewId()), protocol.ProvideMode_Network, parsed, -1, packet); err != nil || !success {
			MessagePoolReturn(packet)
			t.Fatalf("icmp send %d: success=%t err=%v", identifier, success, err)
		}
	}
	sequenceCount = func() int {
		buffer.mutex.Lock()
		defer buffer.mutex.Unlock()
		return len(buffer.sequences)
	}
	return send, sequenceCount
}

// TestIcmpFlowBudgetModelCoversAllocations is the deterministic guard on the
// budget item: the per-flow allocations the settings imply must fit
// `providerIcmpFlowByteCount`. Unlike the measurement below it has no GC or
// span dependence, so it runs in every suite and fails when a settings change
// (a larger read buffer, a deeper send queue) silently outgrows the model the
// provider caps are derived from.
func TestIcmpFlowBudgetModelCoversAllocations(t *testing.T) {
	settings := DefaultIcmpBufferSettings()

	// the dominant per-flow heap: the backend allocates one read and one
	// write buffer of ReadBufferByteCount
	bufferByteCount := 2 * ByteCount(settings.ReadBufferByteCount)
	// the bounded send queue backing array, one pointer per slot
	queueByteCount := ByteCount(settings.SequenceBufferSize) * 8
	// a live flow keeps one packet-class pool buffer in circulation
	poolByteCount := ByteCount(packetPoolSize)
	// the sequence and egress structs, the address copies, and the cached
	// ip path
	fixedByteCount := ByteCount(1024)

	modeledByteCount := bufferByteCount + queueByteCount + poolByteCount + fixedByteCount
	if providerIcmpFlowByteCount < modeledByteCount {
		t.Errorf(
			"per-flow allocations model %s exceeds the budget item %s: buffers %s, queue %s, pool %s, fixed %s",
			kibStr(int64(modeledByteCount)),
			kibStr(providerIcmpFlowByteCount),
			kibStr(int64(bufferByteCount)),
			kibStr(int64(queueByteCount)),
			kibStr(int64(poolByteCount)),
			kibStr(int64(fixedByteCount)),
		)
	}

	// the read buffer must still hold a full mtu reply plus the darwin v4 ip
	// header the read strips
	if settings.ReadBufferByteCount < settings.Mtu+Ipv4HeaderSizeWithoutExtensions {
		t.Errorf(
			"read buffer %d cannot hold an mtu reply with an ip header",
			settings.ReadBufferByteCount,
		)
	}
}

// TestIcmpFlowMemoryFootprint measures the marginal retained heap of one live
// echo flow against the budget model. The marginal figure (the delta between
// two live flow counts) cancels the message pool warmup and the fixed
// buffer/table overhead a first-flow measurement would attribute to the flow.
//
// The assertion is gated: heap measurement in a shared test process inherits
// the goroutine teardown and allocator state of everything that ran before,
// and the marginal figure moves by 3x with test order. Run it in isolation
// (CONNECT_MEMORY=1 go test -run TestIcmpFlowMemoryFootprint) to hold the
// bound; ungated it always logs, so a suite run still reports the figure.
// See TestIcmpFlowBudgetModelCoversAllocations for the deterministic guard.
func TestIcmpFlowMemoryFootprint(t *testing.T) {
	if testing.Short() {
		t.Skip("memory measurement skipped in short mode")
	}
	requireIcmpEgress(t)
	assertBound := os.Getenv("CONNECT_MEMORY") != ""

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	icmpBufferSettings := DefaultIcmpBufferSettingsWithBufferSize(8)
	// no reap during the measurement
	icmpBufferSettings.IdleTimeout = 5 * time.Minute

	send, sequenceCount := icmpFlowSender(t, ctx, icmpBufferSettings)

	// each flow holds one socket fd; keep the counts under a conservative
	// default fd ceiling
	baseFlowCount := 32
	marginalFlowCount := 64

	waitFor := func(flowCount int) {
		t.Helper()
		pollUntil(t, 15*time.Second, "flows live", func() bool {
			return sequenceCount() == flowCount
		})
	}

	for i := 0; i < baseFlowCount; i += 1 {
		send(uint16(1 + i))
	}
	waitFor(baseFlowCount)
	baseHeap, baseStack := heapAndStackInUse()

	for i := 0; i < marginalFlowCount; i += 1 {
		send(uint16(1 + baseFlowCount + i))
	}
	waitFor(baseFlowCount + marginalFlowCount)
	liveHeap, liveStack := heapAndStackInUse()

	perFlowHeap := (liveHeap - baseHeap) / int64(marginalFlowCount)
	perFlowStack := (liveStack - baseStack) / int64(marginalFlowCount)
	t.Logf(
		"icmp per-flow marginal heap ~%s (model %s), stack ~%s (two goroutines, excluded from the model)",
		kibStr(perFlowHeap),
		kibStr(providerIcmpFlowByteCount),
		kibStr(perFlowStack),
	)

	// -race instruments allocations, so byte bounds only hold without it
	if assertBound && !raceEnabled && 3*providerIcmpFlowByteCount/2 < perFlowHeap {
		t.Errorf(
			"per-flow marginal heap %s exceeds 1.5x the budget model %s",
			kibStr(perFlowHeap),
			kibStr(providerIcmpFlowByteCount),
		)
	}

	// teardown returns every flow and the heap to the pre-flow baseline: an
	// unused icmp path costs nothing (the additive budget item property)
	cancel()
	pollUntil(t, 15*time.Second, "flows torn down", func() bool {
		return sequenceCount() == 0
	})
	if assertBound && !raceEnabled {
		if afterHeap, _ := heapAndStackInUse(); baseHeap+mib(1) < afterHeap {
			t.Errorf(
				"post-teardown heap %s above the %d-flow baseline %s",
				kibStr(afterHeap),
				baseFlowCount,
				kibStr(baseHeap),
			)
		}
	}
}

// TestIcmpBudgetItemIsAdditive pins the budget property that motivated the
// item: an icmp path that is never used holds no flows and no heap, so the
// udp and tcp tables keep their full share of the provider target.
func TestIcmpBudgetItemIsAdditive(t *testing.T) {
	if testing.Short() {
		t.Skip("memory measurement skipped in short mode")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultProviderLocalUserNatSettingsWithMemoryTarget(mib(128))

	// the icmp caps are ceilings, not reservations: constructing the nat
	// with a large icmp allowance costs nothing until a flow exists
	baseHeap, _ := heapAndStackInUse()
	localUserNat := NewLocalUserNat(ctx, "testIcmpAdditive", settings)
	defer localUserNat.Close()
	// let the send shard start its buffers
	select {
	case <-time.After(200 * time.Millisecond):
	case <-ctx.Done():
	}
	natHeap, _ := heapAndStackInUse()

	// the whole nat, including both icmp buffers with a 1024 flow allowance,
	// is a small fixed cost
	if !raceEnabled {
		if mib(2) < natHeap-baseHeap {
			t.Errorf("idle nat with icmp allowance retains %s", kibStr(natHeap-baseHeap))
		}
	}

	// the derived caps are the ones the flow tables enforce, and the udp/tcp
	// shares are unchanged by the icmp item (see TestProviderIcmpSettings)
	if settings.IcmpBufferSettings.GlobalLimit <= 0 {
		t.Fatal("icmp global limit not derived from the target")
	}
	unbudgeted := DefaultProviderLocalUserNatSettings()
	AssertEqual(t, unbudgeted.IcmpBufferSettings.GlobalLimit, 0)
}
