// Separates real-socket ordering and registration cleanup from virtual-clock
// checks of the shared idle worker's autonomous deadlines.
package connect

import (
	"context"
	"net"
	"testing"
	"testing/synctest"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// udpTestBufferSend is the family-specific send of Udp4Buffer / Udp6Buffer,
// whose shared generic core (UdpBuffer) the tests inspect.
type udpTestBufferSend func(
	source TransferPath,
	provideMode protocol.ProvideMode,
	udp *parsedUdp,
	timeout time.Duration,
	ipPacket []byte,
) (bool, error)

// testUdpFlowIps returns a NAT-side source and the loopback destination of a
// provider udp flow, in the width the family's buffer keys by.
func testUdpFlowIps(ipVersion int) (sourceIp net.IP, destinationIp net.IP) {
	if ipVersion == 6 {
		return net.ParseIP("2001:db8::1"), net.ParseIP("::1")
	}
	return net.IPv4(192, 0, 2, 2).To4(), net.IPv4(127, 0, 0, 1).To4()
}

// Builds one in-memory shared flow inside the caller's synctest bubble. The
// socket integration tests cover registration; this fixture owns only indexes
// and the real shared idle worker, with one construction wake and joined cleanup.
func newSharedUdpLifecycleTestFlow(t *testing.T, idleTimeout time.Duration) (*Udp4Buffer, *UdpSequence) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	settings := DefaultUdpBufferSettingsWithBufferSize(1)
	settings.IdleTimeout = idleTimeout
	settings.SharedSocketLifecycle = true
	settings.ReceiveShardCount = 1
	settings.Log = NewNoopLogger()
	buffer := NewUdp4Buffer(ctx, nil, settings)
	source := SourceId(NewId())
	sequence := NewUdpSequence(
		ctx, nil, source, protocol.ProvideMode_Network, 4,
		net.IPv4(192, 0, 2, 1).To4(), 42000,
		net.IPv4(198, 51, 100, 1).To4(), 44000,
		settings,
	)
	sequence.sharedSocketLifecycle = true
	sequence.sharedLifecycleWake = buffer.sharedLifecycleWake
	bufferId := NewBufferId4(source, sequence.sourceIp, int(sequence.sourcePort),
		sequence.destinationIp, int(sequence.destinationPort))
	buffer.sequences[bufferId] = sequence
	buffer.sourceSequences[source] = map[BufferId4]*UdpSequence{bufferId: sequence}
	buffer.sharedLifecycleWaitGroup.Add(1)
	go func() {
		defer buffer.sharedLifecycleWaitGroup.Done()
		buffer.runSharedSocketLifecycle()
	}()
	t.Cleanup(func() {
		cancel()
		buffer.waitForLifecycle()
	})
	buffer.wakeSharedSocketLifecycle()
	synctest.Wait()
	return buffer, sequence
}

// The construction wake must arm a timer that retires the flow at its idle
// deadline without another wake, backdated activity, or parent cancellation.
func TestProviderUdpSharedSocketLifecycleExpiresWithoutWake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		idleTimeout := 100 * time.Millisecond
		buffer, sequence := newSharedUdpLifecycleTestFlow(t, idleTimeout)
		time.Sleep(idleTimeout - time.Nanosecond)
		synctest.Wait()
		if udpSequenceDone(sequence) {
			t.Fatal("shared flow expired before its idle deadline")
		}

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if !udpSequenceDone(sequence) {
			t.Fatal("shared idle timer did not expire the flow without another wake")
		}
		if err := buffer.ctx.Err(); err != nil {
			t.Fatalf("parent cancellation caused expiry: %v", err)
		}
		buffer.mutex.Lock()
		sequenceCount := len(buffer.sequences)
		sourceCount := len(buffer.sourceSequences)
		buffer.mutex.Unlock()
		if sequenceCount != 0 || sourceCount != 0 {
			t.Fatalf("automatic expiry retained indexes: sequences=%d sources=%d", sequenceCount, sourceCount)
		}
	})
}

// Activity moves the deadline without waking the shared worker. Its first
// timer must observe that progress and rearm itself to the extended deadline.
func TestProviderUdpSharedSocketLifecycleRearmsAfterActivityWithoutWake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		idleTimeout := 100 * time.Millisecond
		buffer, sequence := newSharedUdpLifecycleTestFlow(t, idleTimeout)
		time.Sleep(idleTimeout / 2)
		sequence.UpdateLastActivityTime()
		time.Sleep(idleTimeout / 2)
		synctest.Wait()
		if udpSequenceDone(sequence) {
			t.Fatal("original idle timer ignored subsequent activity")
		}

		time.Sleep(idleTimeout/2 - time.Nanosecond)
		synctest.Wait()
		if udpSequenceDone(sequence) {
			t.Fatal("shared flow expired before its extended idle deadline")
		}
		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if !udpSequenceDone(sequence) {
			t.Fatal("shared idle timer did not rearm after activity without another wake")
		}
		if err := buffer.ctx.Err(); err != nil {
			t.Fatalf("parent cancellation caused expiry: %v", err)
		}
		buffer.mutex.Lock()
		sequenceCount := len(buffer.sequences)
		sourceCount := len(buffer.sourceSequences)
		buffer.mutex.Unlock()
		if sequenceCount != 0 || sourceCount != 0 {
			t.Fatalf("extended expiry retained indexes: sequences=%d sources=%d", sequenceCount, sourceCount)
		}
	})
}

func startUdpLoopbackEcho(t *testing.T, ipVersion int) (uint16, func()) {
	t.Helper()
	conn, err := net.ListenUDP(testUdpNetwork(ipVersion), testLoopbackUdpAddr(ipVersion))
	if err != nil {
		t.Fatalf("udp echo listen: %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		buffer := make([]byte, 2048)
		for {
			n, addr, readErr := conn.ReadFromUDP(buffer)
			if readErr != nil {
				return
			}
			if _, writeErr := conn.WriteToUDP(buffer[:n], addr); writeErr != nil {
				return
			}
		}
	}()
	return uint16(conn.LocalAddr().(*net.UDPAddr).Port), func() {
		_ = conn.Close()
		<-done
	}
}

func TestProviderUdpSharedSocketLifecyclePreservesOrderAndReaps(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		port, closeEcho := startUdpLoopbackEcho(t, ipVersion)
		defer closeEcho()

		settings := DefaultProviderLocalUserNatSettingsWithMemoryTarget(4 * 1024 * 1024).UdpBufferSettings
		// Keep the flow alive while the real socket reader is scheduled. Idle
		// expiry is driven explicitly after all ordered responses arrive.
		settings.IdleTimeout = time.Hour
		settings.SequenceBufferSize = 8
		responses := make(chan byte, 128)
		receive := func(_ TransferPath, _ protocol.ProvideMode, _ *IpPath, packet []byte) {
			responses <- packet[len(packet)-1]
		}
		if ipVersion == 6 {
			buffer := NewUdp6Buffer(ctx, receive, settings)
			defer func() {
				cancel()
				buffer.waitForLifecycle()
			}()
			testProviderUdpSharedSocketLifecyclePreservesOrderAndReaps(t, ipVersion, port, settings, &buffer.UdpBuffer, buffer.send, responses)
		} else {
			buffer := NewUdp4Buffer(ctx, receive, settings)
			defer func() {
				cancel()
				buffer.waitForLifecycle()
			}()
			testProviderUdpSharedSocketLifecyclePreservesOrderAndReaps(t, ipVersion, port, settings, &buffer.UdpBuffer, buffer.send, responses)
		}
	})
}

func testProviderUdpSharedSocketLifecyclePreservesOrderAndReaps[BufferId comparable](
	t *testing.T,
	ipVersion int,
	port uint16,
	settings *UdpBufferSettings,
	buffer *UdpBuffer[BufferId],
	send udpTestBufferSend,
	responses chan byte,
) {
	t.Helper()
	sourceIp, destinationIp := testUdpFlowIps(ipVersion)
	source := SourceId(NewId())
	const packetCount = 64
	for i := range packetCount {
		packet := MessagePoolGet(32)
		packet[0] = 0x5a
		packet[1] = byte(i)
		udp := &parsedUdp{
			sourceIp:        sourceIp,
			destinationIp:   destinationIp,
			sourcePort:      42000,
			destinationPort: port,
			payload:         packet[:2],
		}
		if success, sendErr := send(
			source,
			protocol.ProvideMode_Network,
			udp,
			-1,
			packet,
		); sendErr != nil || !success {
			MessagePoolReturn(packet)
			t.Fatalf("send %d: success=%t err=%v", i, success, sendErr)
		}
	}

	for i := range packetCount {
		select {
		case marker := <-responses:
			if marker != byte(i) {
				t.Fatalf("response %d marker=%d", i, marker)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("response %d timed out", i)
		}
	}

	buffer.mutex.Lock()
	if buffer.socketReadPoller == nil {
		buffer.mutex.Unlock()
		t.Skip("socket readiness poller unavailable on this platform")
	}
	if len(buffer.sequences) != 1 {
		buffer.mutex.Unlock()
		t.Fatalf("sequence count=%d, want 1", len(buffer.sequences))
	}
	var activeSequence *UdpSequence
	for _, sequence := range buffer.sequences {
		activeSequence = sequence
		if !sequence.sharedSocketLifecycle || sequence.sendItems != nil {
			buffer.mutex.Unlock()
			t.Fatalf("shared lifecycle=%t send channel present=%t",
				sequence.sharedSocketLifecycle, sequence.sendItems != nil)
		}
	}
	buffer.mutex.Unlock()

	activeSequence.userLimited.mutex.Lock()
	activeSequence.lastActivityTime = time.Now().Add(-2 * settings.IdleTimeout)
	activeSequence.userLimited.mutex.Unlock()
	buffer.wakeSharedSocketLifecycle()
	select {
	case <-activeSequence.ctx.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("shared UDP idle reap did not cancel the expired flow")
	}
	// Cancellation occurs inside removal; taking the buffer lock joins the
	// rest of that transition, including poll registration retirement.
	buffer.mutex.Lock()
	sequenceCount := len(buffer.sequences)
	buffer.mutex.Unlock()
	if sequenceCount != 0 {
		t.Fatalf("idle sequence count=%d, want 0", sequenceCount)
	}
	for i := range buffer.socketReadPoller.shards {
		shard := &buffer.socketReadPoller.shards[i]
		shard.mutex.RLock()
		registrationCount := len(shard.byFd)
		shard.mutex.RUnlock()
		if registrationCount != 0 {
			t.Fatalf("poll shard %d retained %d registrations", i, registrationCount)
		}
	}
	if activeSequence == nil {
		t.Fatal("shared lifecycle exposed no active sequence")
	}
	if activeSequence.socketReadPollFd != -1 {
		t.Fatalf("reaped sequence poll fd=%d, want invalid -1", activeSequence.socketReadPollFd)
	}
}

func TestProviderUdpSharedSocketLifecycleDoesNotAllocatePerFlowQueues(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		port, closeEcho := startUdpLoopbackEcho(t, ipVersion)
		defer closeEcho()

		settings := DefaultProviderLocalUserNatSettingsWithMemoryTarget(4 * 1024 * 1024).UdpBufferSettings
		settings.IdleTimeout = time.Minute
		responses := make(chan struct{}, providerColdPageTestFlowCount)
		receive := func(_ TransferPath, _ protocol.ProvideMode, _ *IpPath, _ []byte) {
			responses <- struct{}{}
		}
		if ipVersion == 6 {
			buffer := NewUdp6Buffer(ctx, receive, settings)
			testProviderUdpSharedSocketLifecycleDoesNotAllocatePerFlowQueues(t, ipVersion, port, settings, &buffer.UdpBuffer, buffer.send, responses)
		} else {
			buffer := NewUdp4Buffer(ctx, receive, settings)
			testProviderUdpSharedSocketLifecycleDoesNotAllocatePerFlowQueues(t, ipVersion, port, settings, &buffer.UdpBuffer, buffer.send, responses)
		}
	})
}

func testProviderUdpSharedSocketLifecycleDoesNotAllocatePerFlowQueues[BufferId comparable](
	t *testing.T,
	ipVersion int,
	port uint16,
	settings *UdpBufferSettings,
	buffer *UdpBuffer[BufferId],
	send udpTestBufferSend,
	responses chan struct{},
) {
	t.Helper()
	sourceIp, destinationIp := testUdpFlowIps(ipVersion)
	source := SourceId(NewId())
	for i := range providerColdPageTestFlowCount {
		packet := MessagePoolGet(32)
		packet[0] = byte(i)
		udp := &parsedUdp{
			sourceIp:        sourceIp,
			destinationIp:   destinationIp,
			sourcePort:      uint16(43000 + i),
			destinationPort: port,
			payload:         packet[:1],
		}
		if success, sendErr := send(source, protocol.ProvideMode_Network, udp, -1, packet); sendErr != nil || !success {
			MessagePoolReturn(packet)
			t.Fatalf("flow %d: success=%t err=%v", i, success, sendErr)
		}
	}
	for i := 0; i < providerColdPageTestFlowCount; i++ {
		select {
		case <-responses:
		case <-time.After(5 * time.Second):
			t.Fatalf("response %d timed out", i)
		}
	}

	buffer.mutex.Lock()
	defer buffer.mutex.Unlock()
	if buffer.socketReadPoller == nil {
		t.Skip("socket readiness poller unavailable on this platform")
	}
	if len(buffer.sequences) != providerColdPageTestFlowCount {
		t.Fatalf("sequence count=%d, want %d", len(buffer.sequences), providerColdPageTestFlowCount)
	}
	for _, sequence := range buffer.sequences {
		if !sequence.sharedSocketLifecycle || sequence.sendItems != nil {
			t.Fatal("provider flow retained a send goroutine queue")
		}
	}
	if len(buffer.socketReadPoller.shards) != settings.SocketReadShardCount {
		t.Fatalf("poll shards=%d, want %d",
			len(buffer.socketReadPoller.shards), settings.SocketReadShardCount)
	}
}
