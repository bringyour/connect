package connect

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"testing/synctest"
	"time"
)

// A batch drain may coalesce all freed slots into one notification. Every
// parked writer must still get a turn while capacity remains available.
func TestTunOutboundQueueWakesEveryWriterAfterBatchDrain(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		endpoint := newTunLinkEndpoint(ctx, 3, DefaultMtu, "", 0)
		defer endpoint.Close()

		for marker := byte(0); marker < 3; marker += 1 {
			packet := newTunLinkTestPacket(marker)
			result := writeTunLinkPacket(endpoint, packet)
			packet.DecRef()
			if result.n != 1 || result.err != nil {
				t.Fatalf("fill queue: %d, %v", result.n, result.err)
			}
		}

		results := make(chan tunLinkWriteResult, 3)
		for marker := byte(3); marker < 6; marker += 1 {
			go func() {
				packet := newTunLinkTestPacket(marker)
				defer packet.DecRef()
				results <- writeTunLinkPacket(endpoint, packet)
			}()
		}
		synctest.Wait()

		// Withhold notifications until all three slots are free, forcing the
		// same single-token state as reads that outrun their waiting writers.
		for range 3 {
			packet := endpoint.Endpoint.Read()
			if packet == nil {
				t.Fatal("filled queue lost a packet")
			}
			packet.DecRef()
		}
		endpoint.notifySpace()
		synctest.Wait()
		for range 3 {
			select {
			case result := <-results:
				if result.n != 1 || result.err != nil {
					t.Fatalf("resumed write: %d, %v", result.n, result.err)
				}
			default:
				t.Fatal("coalesced drain left a writer parked with free queue space")
			}
		}
	})
}

// A netstack writer must never wait forever for tun queue space: the
// goroutine that drains the queue can itself be the one injecting inbound
// packets (socks tun reader -> SendPacket -> receive callback -> Tun.Write ->
// gVisor reply), which is a self-deadlock when the queue is full. After the
// bounded wait the write drops the rest and the writer returns.
func TestTunOutboundQueueDropsAfterBoundedWait(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultTunSettingsWithBufferSize(1)
	settings.OutboundQueueWaitTimeout = 50 * time.Millisecond
	tun, err := CreateTun(ctx, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer tun.Close()

	firstPacket := newTunLinkTestPacket(1)
	defer firstPacket.DecRef()
	if result := writeTunLinkPacket(tun.ep, firstPacket); result.err != nil || result.n != 1 {
		t.Fatalf("first link write = %d, %v; want 1, nil", result.n, result.err)
	}

	secondPacket := newTunLinkTestPacket(2)
	defer secondPacket.DecRef()
	start := time.Now()
	result := writeTunLinkPacket(tun.ep, secondPacket)
	elapsed := time.Since(start)
	if result.err != nil || result.n != 0 {
		t.Fatalf("full-queue link write = %d, %v; want dropped 0, nil", result.n, result.err)
	}
	if elapsed < settings.OutboundQueueWaitTimeout || 2*time.Second < elapsed {
		t.Fatalf("full-queue link write took %s, want a bounded wait of about %s", elapsed, settings.OutboundQueueWaitTimeout)
	}
	if count := tun.OutboundDropCount(); count != 1 {
		t.Fatalf("outbound drop count = %d, want 1", count)
	}

	// the queue is intact: the first packet is still readable and a later
	// write proceeds once there is space
	firstRead, readErr := tun.Read()
	if readErr != nil {
		t.Fatal(readErr)
	}
	MessagePoolReturn(firstRead)
	thirdPacket := newTunLinkTestPacket(3)
	defer thirdPacket.DecRef()
	if result := writeTunLinkPacket(tun.ep, thirdPacket); result.err != nil || result.n != 1 {
		t.Fatalf("post-drop link write = %d, %v; want 1, nil", result.n, result.err)
	}
	thirdRead, readErr := tun.Read()
	if readErr != nil {
		t.Fatal(readErr)
	}
	MessagePoolReturn(thirdRead)
	if count := tun.OutboundDropCount(); count != 1 {
		t.Fatalf("outbound drop count after recovery = %d, want 1", count)
	}
}

// A non-positive bound keeps the original unbounded backpressure.
func TestTunOutboundQueueUnboundedWaitWhenDisabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultTunSettingsWithBufferSize(1)
	settings.OutboundQueueWaitTimeout = 0
	tun, err := CreateTun(ctx, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer tun.Close()

	firstPacket := newTunLinkTestPacket(1)
	defer firstPacket.DecRef()
	if result := writeTunLinkPacket(tun.ep, firstPacket); result.err != nil || result.n != 1 {
		t.Fatalf("first link write = %d, %v; want 1, nil", result.n, result.err)
	}
	secondPacket := newTunLinkTestPacket(2)
	defer secondPacket.DecRef()
	waiting := make(chan struct{})
	tun.ep.ctx = &tunLinkWaitContext{Context: tun.ctx, waiting: waiting}
	done := make(chan tunLinkWriteResult, 1)
	go func() {
		done <- writeTunLinkPacket(tun.ep, secondPacket)
	}()
	select {
	case <-waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("writer did not reach the full outbound queue")
	}
	firstRead, readErr := tun.Read()
	if readErr != nil {
		t.Fatal(readErr)
	}
	MessagePoolReturn(firstRead)
	select {
	case result := <-done:
		if result.err != nil || result.n != 1 {
			t.Fatalf("released link write = %d, %v; want 1, nil", result.n, result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("queue space did not release the blocked link writer")
	}
	secondRead, readErr := tun.Read()
	if readErr != nil {
		t.Fatal(readErr)
	}
	MessagePoolReturn(secondRead)
}

// The self-deadlock itself, end to end through the real gVisor stack: the
// goroutine that drains the tun's outbound queue is also the one injecting
// inbound packets (socks tun reader -> SendPacket -> receive callback ->
// Tun.Write). When the injected packet provokes a reply — here a RST for a
// flow nothing is listening on — netstack writes that reply back into the
// outbound queue. With the queue full and its only consumer inside this very
// call, an unbounded wait never returns. The bounded wait drops the reply and
// lets the reader continue, exactly as a saturated NIC queue would.
func TestTunInjectingReaderDoesNotDeadlockOnFullOutboundQueue(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testTunInjectingReaderDoesNotDeadlockOnFullOutboundQueue(t, ipVersion)
	})
}

func testTunInjectingReaderDoesNotDeadlockOnFullOutboundQueue(t *testing.T, ipVersion int) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := tunTestApplyIpVersion(DefaultTunSettingsWithBufferSize(1), ipVersion)
	settings.OutboundQueueWaitTimeout = 100 * time.Millisecond
	tun, err := CreateTun(ctx, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer tun.Close()

	// fill the single outbound slot so the queue has no room for the reply
	queueFiller := newTunLinkTestPacket(1)
	defer queueFiller.DecRef()
	if result := writeTunLinkPacket(tun.ep, queueFiller); result.err != nil || result.n != 1 {
		t.Fatalf("queue-filling link write = %d, %v; want 1, nil", result.n, result.err)
	}

	// this is the injection the reader makes while holding the queue
	syn := newTunClosedPortSynPacket(t, tun, ipVersion)
	written := make(chan error, 1)
	go func() {
		_, writeErr := tun.Write(syn)
		written <- writeErr
	}()
	select {
	case writeErr := <-written:
		if writeErr != nil {
			t.Fatalf("inbound injection returned %v", writeErr)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the injecting reader deadlocked: netstack's reply waited for queue space that only this goroutine drains")
	}

	// the reply was dropped rather than queued, and the queue still works
	if count := tun.OutboundDropCount(); count == 0 {
		t.Fatal("no outbound drop was counted for the dropped reply")
	}
	queued, readErr := tun.Read()
	if readErr != nil {
		t.Fatal(readErr)
	}
	MessagePoolReturn(queued)

	// the same injection with the bound disabled is the deadlock this guards
	// against: it must not return while the queue stays full.
	unboundedSettings := tunTestApplyIpVersion(DefaultTunSettingsWithBufferSize(1), ipVersion)
	unboundedSettings.OutboundQueueWaitTimeout = 0
	unboundedTun, err := CreateTun(ctx, unboundedSettings)
	if err != nil {
		t.Fatal(err)
	}
	defer unboundedTun.Close()
	unboundedFiller := newTunLinkTestPacket(1)
	defer unboundedFiller.DecRef()
	if result := writeTunLinkPacket(unboundedTun.ep, unboundedFiller); result.err != nil || result.n != 1 {
		t.Fatalf("unbounded queue-filling link write = %d, %v; want 1, nil", result.n, result.err)
	}
	unboundedWritten := make(chan error, 1)
	unboundedWaiting := make(chan struct{})
	unboundedTun.ep.ctx = &tunLinkWaitContext{Context: unboundedTun.ctx, waiting: unboundedWaiting}
	go func() {
		_, writeErr := unboundedTun.Write(newTunClosedPortSynPacket(t, unboundedTun, ipVersion))
		unboundedWritten <- writeErr
	}()
	select {
	case <-unboundedWaiting:
	case <-time.After(5 * time.Second):
		t.Fatal("unbounded injection did not reach the full outbound queue")
	}
	// Close releases the blocked writer so the goroutine does not leak
	unboundedTun.Close()
	select {
	case <-unboundedWritten:
	case <-time.After(5 * time.Second):
		t.Fatal("closing the tun did not release the blocked injection")
	}
}

// A SYN addressed to the tun's own address of the given family on a port
// nothing is listening on: gVisor answers it with a RST written back to the
// link endpoint.
func newTunClosedPortSynPacket(t *testing.T, tun *Tun, ipVersion int) []byte {
	t.Helper()
	sourceIp := net.IPv4(198, 51, 100, 2).To4()
	if ipVersion == 6 {
		sourceIp = net.ParseIP("2001:db8::2")
	}
	path := &IpPath{
		Version:         ipVersion,
		Protocol:        IpProtocolTcp,
		SourceIp:        sourceIp,
		SourcePort:      40001,
		DestinationIp:   net.IP(tunTestLocalAddress(t, tun, ipVersion).AsSlice()),
		DestinationPort: 9,
	}
	packet, tcpHeader := ipTransportPacket(path, ipProtocolNumberTcp, TcpHeaderSizeWithoutExtensions)
	binary.BigEndian.PutUint16(tcpHeader[0:2], uint16(path.SourcePort))
	binary.BigEndian.PutUint16(tcpHeader[2:4], uint16(path.DestinationPort))
	binary.BigEndian.PutUint32(tcpHeader[4:8], 1000)
	tcpHeader[12] = byte(TcpHeaderSizeWithoutExtensions/4) << 4
	tcpHeader[13] = tcpFlagSyn
	binary.BigEndian.PutUint16(tcpHeader[14:16], 65535)
	binary.BigEndian.PutUint16(tcpHeader[16:18], ipPathTransportChecksum(path, ipProtocolNumberTcp, tcpHeader))
	return packet
}
