package connect

import (
	"context"
	"net"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// THROUGHPUTFIX H9, the half that was left to reading. A datagram return never
// enters the socket-owned retry the abandon timeout bounds, which is why UDP
// has no zombie flows; the converse question is whether a UDP flow whose
// client disappears is reclaimed at all, or leaks by a different route. Its
// sequence owns a real kernel socket, a receive worker and a reaper entry, and
// nothing about the client's disappearance tells the provider anything: only
// the idle timeout ends it.
//
// A leak here would be invisible in any single flow and would show as a
// provider that runs out of descriptors after a day. So this is a census
// rather than an assertion about one flow: open a flow, let it be reaped, and
// repeat, with goroutines and open descriptors counted across the cycles. A
// per-cycle cost that does not come back is the signature.
func TestUdpFlowReleasedWhenClientDisappears(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	assertMessagePoolOwnership(t)

	// a loopback sink, so each flow opens a real upstream socket
	sink, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer sink.Close()
	go func() {
		buffer := make([]byte, 2048)
		for {
			if _, _, err := sink.ReadFrom(buffer); err != nil {
				return
			}
		}
	}()

	const idleTimeout = 250 * time.Millisecond
	settings := DefaultLocalUserNatSettings()
	settings.UdpBufferSettings.IdleTimeout = idleTimeout
	settings.UdpBufferSettings.ReadTimeout = idleTimeout
	localUserNat := NewLocalUserNat(ctx, "udp-flow-reclaim-test", settings)
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := localUserNat.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close the local user NAT: %v", err)
		}
	})

	sinkPort := sink.LocalAddr().(*net.UDPAddr).Port
	// one flow per cycle, from a client that then disappears: a distinct
	// source id and source port, so no cycle reuses another's flow
	openFlow := func(cycle int) {
		ipPath := udpTestPath(4)
		ipPath.SourcePort = 40000 + cycle
		ipPath.DestinationIp = net.ParseIP("127.0.0.1")
		ipPath.DestinationPort = sinkPort
		packet := MessagePoolCopy(ipOosUdpPacket(ipPath, []byte("reclaim")))
		if !localUserNat.SendPacket(SourceId(NewId()), protocol.ProvideMode_Network, packet, -1) {
			MessagePoolReturn(packet)
			t.Fatalf("cycle %d: the UDP packet was not queued", cycle)
		}
	}

	// two warm-up cycles first: the first flow pays for the shared receive
	// workers and the reaper, which are per-NAT rather than per-flow, and
	// charging that to the census would read as growth
	const warmupCycleCount = 2
	const censusCycleCount = 6
	for cycle := range warmupCycleCount {
		openFlow(cycle)
		time.Sleep(3 * idleTimeout)
	}
	runtime.GC()
	baselineGoroutines := runtime.NumGoroutine()
	baselineDescriptors := openDescriptorCount(t)

	for cycle := range censusCycleCount {
		openFlow(warmupCycleCount + cycle)
		// the client is gone from here: nothing acknowledges, nothing else
		// arrives, and only the idle timeout can end the flow
		time.Sleep(3 * idleTimeout)
	}
	runtime.GC()
	censusGoroutines := runtime.NumGoroutine()
	censusDescriptors := openDescriptorCount(t)

	// one flow's worth of slack, so a single flow still being reaped as the
	// census closes is not read as growth; anything per-cycle exceeds it
	if baselineGoroutines+censusCycleCount <= censusGoroutines {
		t.Errorf(
			"%d goroutines after %d UDP flows whose clients disappeared, against %d before; a flow that is not reclaimed keeps its receive worker",
			censusGoroutines,
			censusCycleCount,
			baselineGoroutines,
		)
	}
	if 0 < baselineDescriptors && baselineDescriptors+censusCycleCount <= censusDescriptors {
		t.Errorf(
			"%d open descriptors after %d UDP flows whose clients disappeared, against %d before; a flow that is not reclaimed keeps its upstream socket",
			censusDescriptors,
			censusCycleCount,
			baselineDescriptors,
		)
	}
	t.Logf(
		"%d UDP flows opened and abandoned at a %s idle timeout: goroutines %d to %d, descriptors %d to %d",
		censusCycleCount,
		idleTimeout,
		baselineGoroutines,
		censusGoroutines,
		baselineDescriptors,
		censusDescriptors,
	)
}

// Open descriptors for this process, or 0 where the kernel does not publish
// them, in which case the descriptor half of the census is skipped.
func openDescriptorCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return 0
	}
	return len(entries)
}
