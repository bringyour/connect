//go:build linux

package connect

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// THROUGHPUTFIX H8, recorded as not supported so it is not retried from the
// plan alone. The hypothesis was that the provider's upstream UDP receive
// buffer is too small for its arrival rate, so datagrams are dropped in the
// kernel before the provider sees them. The campaign measured an eight times
// larger buffer against an 800 Mbit/s offer across eight cells and recovered
// nothing, all within 1.2 per cent, with the drop counter validated by its own
// positive readings at the mobile policy so the zeros elsewhere are real
// zeros.
//
// The mechanism that explains the null is what this pins: the buffer is a pure
// burst reservoir, linear in its size, with no threshold anywhere. A buffer
// that is not the binding constraint at one size cannot become one at eight
// times that size, because all that changes is how long a burst it absorbs.
//
// It also pins the trap any future sizing must start from. The kernel clamps
// the request to net.core.rmem_max and then doubles it, and it charges each
// datagram its skb overhead rather than its payload, so what the reported
// buffer actually holds is a fraction of itself that falls with datagram size:
// about three fifths at an MTU-sized datagram and about a fourteenth at a DNS
// query. A sizing read off the reported number is out by between two and
// fourteen.
// Structural concession beside this row: whether UDP through a provider hits
// the same ceiling TCP does (H10) is a comparison of two rates, and its value
// is exactly that it is measured end to end — a difference between the two
// paths localises the cause below or above them, which no in-process
// invariant can stand in for. The measurement stream has since established
// that its provider-upstream cell contains no Transfer layer at all, so the
// gap sits below Transfer and the cell that owns it is the TCP-versus-UDP
// sweep, not this package.
func TestUpstreamUdpBufferSizing(t *testing.T) {
	defer SetMemoryBudget(0)

	// what the provider asks for, at every budget a shipping host sets
	// (THROUGHPUTFIX §11.1). Unlike the TCP window this is not rounded to a
	// power of two, so it tracks the budget directly.
	windowSizes := []struct {
		budgetByteCount ByteCount
		maxWindowSize   ByteCount
		host            string
	}{
		{budgetByteCount: 0, maxWindowSize: mib(1), host: "unbudgeted server or desktop provider"},
		{budgetByteCount: mib(48), maxWindowSize: kib(768), host: "ios app process"},
		{budgetByteCount: mib(32), maxWindowSize: kib(512), host: "ios packet tunnel, android cap"},
		{budgetByteCount: mib(24), maxWindowSize: kib(384), host: "android 32 MiB heap class"},
		{budgetByteCount: mib(8), maxWindowSize: kib(256), host: "pre-ios-16 packet tunnel"},
		{budgetByteCount: mib(1), maxWindowSize: kib(256), host: "below every shipping target"},
	}
	for _, windowSize := range windowSizes {
		SetMemoryBudget(windowSize.budgetByteCount)
		maxWindowSize := ByteCount(DefaultUdpBufferSettings().MaxWindowSize)
		if maxWindowSize != windowSize.maxWindowSize {
			t.Errorf(
				"a %d byte memory budget (%s) requests a %d byte upstream UDP buffer, want %d",
				windowSize.budgetByteCount,
				windowSize.host,
				maxWindowSize,
				windowSize.maxWindowSize,
			)
		}
	}
	SetMemoryBudget(0)

	receiveCoreMax := upstreamSysctlValues(t, "net/core/rmem_max")[0]
	serverWindowSize := int(DefaultUdpBufferSettings().MaxWindowSize)
	// eight times the server profile, the campaign's arm
	enlargedWindowSize := 8 * serverWindowSize

	datagramSizes := []int{64, 1400}
	for _, datagramSize := range datagramSizes {
		serverBuffer, serverHeld, serverDrops := upstreamUdpTestCapacity(t, serverWindowSize, datagramSize)
		enlargedBuffer, enlargedHeld, enlargedDrops := upstreamUdpTestCapacity(t, enlargedWindowSize, datagramSize)

		if serverBuffer != 2*min(serverWindowSize, receiveCoreMax) {
			t.Errorf(
				"a %d byte UDP receive request became a %d byte buffer, want twice the request clamped to the %d byte core maximum",
				serverWindowSize,
				serverBuffer,
				receiveCoreMax,
			)
		}
		// the instrument: a buffer this far past its capacity must report drops,
		// or a zero elsewhere says nothing
		if serverDrops <= 0 || enlargedDrops <= 0 {
			t.Errorf(
				"%d byte datagrams overran a %d byte and a %d byte buffer with %d and %d drops counted; the drop counter is not reading, so no zero from it can be trusted",
				datagramSize,
				serverBuffer,
				enlargedBuffer,
				serverDrops,
				enlargedDrops,
			)
		}

		// the null's mechanism: capacity is linear in the buffer, so a larger
		// buffer buys a proportionally longer burst and nothing else
		bufferRatio := float64(enlargedBuffer) / float64(serverBuffer)
		heldRatio := float64(enlargedHeld) / float64(serverHeld)
		if heldRatio < 0.9*bufferRatio || 1.1*bufferRatio < heldRatio {
			t.Errorf(
				"%d byte datagrams: a buffer %.2f times larger held %.2f times as much payload, %d against %d bytes; a buffer with a threshold in it would not be a pure burst reservoir and H8 would be worth retrying",
				datagramSize,
				bufferRatio,
				heldRatio,
				enlargedHeld,
				serverHeld,
			)
		}

		// the doubling and per-datagram overhead, which any sizing must start
		// from rather than from the reported buffer
		payloadFraction := float64(serverHeld) / float64(serverBuffer)
		lowest, highest := 0.03, 0.15
		if 1024 <= datagramSize {
			lowest, highest = 0.40, 0.80
		}
		if payloadFraction < lowest || highest < payloadFraction {
			t.Errorf(
				"%d byte datagrams filled %.2f of a %d byte buffer with payload, outside the %.2f to %.2f the kernel's skb accounting gives; the factor a sizing must divide by has moved",
				datagramSize,
				payloadFraction,
				serverBuffer,
				lowest,
				highest,
			)
		}
		t.Logf(
			"%d byte datagrams: a %d byte buffer holds %d bytes of payload (%.2f of itself, %d datagrams); %d bytes holds %d (%.2f times as much for %.2f times the buffer)",
			datagramSize,
			serverBuffer,
			serverHeld,
			payloadFraction,
			serverHeld/datagramSize,
			enlargedBuffer,
			enlargedHeld,
			heldRatio,
			bufferRatio,
		)
	}
}

// Fills a UDP receive buffer of the requested size past overflowing and reports
// the buffer the kernel granted, the payload bytes it actually held, and the
// datagrams it dropped. Loopback delivery is synchronous, so the overflow is
// decided by the buffer rather than by timing.
func upstreamUdpTestCapacity(
	t *testing.T,
	windowSize int,
	datagramSize int,
) (int, int, int) {
	t.Helper()
	receiver, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()
	if err := receiver.SetReadBuffer(windowSize); err != nil {
		t.Fatal(err)
	}
	bufferSize := upstreamUdpTestBufferSize(t, receiver)
	receiverAddr := receiver.LocalAddr().(*net.UDPAddr)

	sender, err := net.DialUDP("udp", nil, receiverAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	// four times what the buffer could hold even at no overhead at all
	payload := make([]byte, datagramSize)
	for sentByteCount := 0; sentByteCount < 4*bufferSize; sentByteCount += datagramSize {
		if _, err := sender.Write(payload); err != nil {
			break
		}
	}
	dropCount := upstreamUdpTestDropCount(t, receiverAddr.Port)

	receiver.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buffer := make([]byte, 65536)
	heldByteCount := 0
	for {
		n, _, err := receiver.ReadFrom(buffer)
		if err != nil {
			break
		}
		heldByteCount += n
	}
	if heldByteCount <= 0 {
		t.Fatalf("a %d byte UDP buffer held nothing of %d byte datagrams", bufferSize, datagramSize)
	}
	return bufferSize, heldByteCount, dropCount
}

func upstreamUdpTestBufferSize(t *testing.T, udpConn *net.UDPConn) int {
	t.Helper()
	rawConn, err := udpConn.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var bufferSize int
	var sockoptErr error
	if err := rawConn.Control(func(fd uintptr) {
		bufferSize, sockoptErr = syscall.GetsockoptInt(int(fd), syscall.SOL_SOCKET, syscall.SO_RCVBUF)
	}); err != nil {
		t.Fatal(err)
	}
	if sockoptErr != nil {
		t.Fatal(sockoptErr)
	}
	return bufferSize
}

// the kernel's own per-socket drop count, the instrument a negative result
// depends on
func upstreamUdpTestDropCount(t *testing.T, port int) int {
	t.Helper()
	content, err := os.ReadFile("/proc/net/udp")
	if err != nil {
		t.Skipf("/proc/net/udp is unreadable, so kernel drops cannot be counted: %v", err)
	}
	suffix := fmt.Sprintf(":%04X", port)
	for _, line := range strings.Split(string(content), "\n")[1:] {
		fields := strings.Fields(line)
		if len(fields) < 13 || !strings.HasSuffix(fields[1], suffix) {
			continue
		}
		dropCount, err := strconv.Atoi(fields[12])
		if err != nil {
			t.Fatalf("/proc/net/udp drops column = %q", fields[12])
		}
		return dropCount
	}
	t.Fatalf("no /proc/net/udp row for local port %d", port)
	return 0
}
