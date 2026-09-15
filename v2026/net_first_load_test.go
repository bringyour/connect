package connect

import (
	"encoding/binary"
	"fmt"
	"net/netip"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type blockingFirstLoadLogger struct {
	Logger
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (self *blockingFirstLoadLogger) Infof(format string, args ...any) {
	if strings.HasPrefix(format, "[firstload]dns ") {
		self.once.Do(func() { close(self.started) })
		<-self.release
	}
}

// buildTestTcp4Packet hand-rolls a minimal ipv4+tcp packet for the peek tests.
func buildTestTcp4Packet(src netip.Addr, srcPort uint16, dst netip.Addr, dstPort uint16, flags byte, payloadLen int) []byte {
	totalLen := 20 + 20 + payloadLen
	packet := make([]byte, totalLen)
	packet[0] = 0x45 // v4, ihl 20
	binary.BigEndian.PutUint16(packet[2:4], uint16(totalLen))
	packet[8] = 64 // ttl
	packet[9] = 6  // tcp
	copy(packet[12:16], src.AsSlice())
	copy(packet[16:20], dst.AsSlice())
	binary.BigEndian.PutUint16(packet[20:22], srcPort)
	binary.BigEndian.PutUint16(packet[22:24], dstPort)
	packet[32] = 5 << 4 // data offset 20
	packet[33] = flags
	return packet
}

func TestFirstLoadTcpPeek(t *testing.T) {
	local := netip.MustParseAddr("10.0.0.2")
	server := netip.MustParseAddr("93.184.216.34")

	// egress syn
	syn := buildTestTcp4Packet(local, 40001, server, 443, 0x02, 0)
	remoteAddr, remotePort, localPort, flags, payloadLen, ok := firstLoadTcpPeek(syn, false)
	if !ok || remoteAddr != server || remotePort != 443 || localPort != 40001 || flags != 0x02 || payloadLen != 0 {
		t.Fatalf("egress syn peek: %v %v %v %v %v %v", remoteAddr, remotePort, localPort, flags, payloadLen, ok)
	}

	// ingress synack
	synAck := buildTestTcp4Packet(server, 443, local, 40001, 0x12, 0)
	remoteAddr, remotePort, localPort, flags, payloadLen, ok = firstLoadTcpPeek(synAck, true)
	if !ok || remoteAddr != server || remotePort != 443 || localPort != 40001 || flags != 0x12 || payloadLen != 0 {
		t.Fatalf("ingress synack peek: %v %v %v %v %v %v", remoteAddr, remotePort, localPort, flags, payloadLen, ok)
	}

	// ingress payload
	data := buildTestTcp4Packet(server, 443, local, 40001, 0x18, 512)
	_, _, _, _, payloadLen, ok = firstLoadTcpPeek(data, true)
	if !ok || payloadLen != 512 {
		t.Fatalf("ingress payload peek: %v %v", payloadLen, ok)
	}

	// non-tcp is skipped
	udp := buildTestTcp4Packet(local, 40001, server, 443, 0x02, 0)
	udp[9] = 17
	if _, _, _, _, _, ok := firstLoadTcpPeek(udp, false); ok {
		t.Fatalf("udp must not peek")
	}
	// short is skipped
	if _, _, _, _, _, ok := firstLoadTcpPeek(syn[:16], false); ok {
		t.Fatalf("short packet must not peek")
	}
}

// TestFirstLoadTimeline drives one tracked flow and one dns pipeline through
// the timeline and pins the sample content, plus the syn-retransmit and
// budget/deactivation behavior.
func TestFirstLoadTimeline(t *testing.T) {
	local := netip.MustParseAddr("10.0.0.2")
	server := netip.MustParseAddr("93.184.216.34")

	timeline := newFirstLoadTimeline(NewNoopLogger())
	defer timeline.Close()

	// dns pipeline
	key := NewDohKey("A", "example.com")
	timeline.dnsStart(key)
	timeline.dnsStart(key) // coalesced duplicate keeps one start
	timeline.dnsDone(key, true)
	timeline.dnsDone(key, true) // second done is a no-op (untracked now)

	// tcp flow: syn -> synack -> first byte
	syn := buildTestTcp4Packet(local, 40001, server, 443, 0x02, 0)
	timeline.observeSend(syn)
	timeline.observeSend(syn) // retransmit keeps the original start
	// an ack from the local side must not count as the peer's synack
	timeline.observeSend(buildTestTcp4Packet(local, 40001, server, 443, 0x10, 0))
	timeline.observeReceive(buildTestTcp4Packet(server, 443, local, 40001, 0x12, 0))
	timeline.observeReceive(buildTestTcp4Packet(server, 443, local, 40001, 0x18, 256))
	// after first byte the flow is logged; more payload is ignored
	timeline.observeReceive(buildTestTcp4Packet(server, 443, local, 40001, 0x18, 256))

	samples := timeline.Samples()
	if len(samples) != 2 {
		t.Fatalf("expected dns + tcp samples, got %d", len(samples))
	}
	var dnsSample, tcpSample *FirstLoadSample
	for _, sample := range samples {
		switch sample.Kind {
		case "dns":
			dnsSample = sample
		case "tcp":
			tcpSample = sample
		}
	}
	if dnsSample == nil || dnsSample.Target != "example.com" || dnsSample.DnsMillis < 0 {
		t.Fatalf("dns sample: %+v", dnsSample)
	}
	if tcpSample == nil || tcpSample.SynAckMillis < 0 || tcpSample.FirstByteMillis < tcpSample.SynAckMillis {
		t.Fatalf("tcp sample: %+v", tcpSample)
	}

	// an untracked flow's packets are ignored
	timeline.observeReceive(buildTestTcp4Packet(server, 443, local, 50123, 0x18, 64))
	if len(timeline.Samples()) != 2 {
		t.Fatalf("untracked flow must not add samples")
	}
}

func TestFirstLoadBlockedLoggerCannotPausePacketPath(t *testing.T) {
	log := &blockingFirstLoadLogger{
		Logger:  NewNoopLogger(),
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	timeline := newFirstLoadTimeline(log)

	key := NewDohKey("A", "blocked-logger.example")
	timeline.dnsStart(key)
	dnsDone := make(chan struct{})
	go func() {
		timeline.dnsDone(key, true)
		close(dnsDone)
	}()

	select {
	case <-log.started:
	case <-time.After(time.Second):
		t.Fatal("bounded logger worker did not receive the DNS event")
	}
	select {
	case <-dnsDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("DNS accounting waited on the blocked host logger")
	}

	// The logger is still parked. A packet observation must nevertheless
	// acquire the timeline state and return promptly; the old synchronous
	// logger held that lock and paused every observer behind it.
	local := netip.MustParseAddr("10.0.0.2")
	server := netip.MustParseAddr("93.184.216.34")
	packetDone := make(chan struct{})
	go func() {
		timeline.observeSend(buildTestTcp4Packet(local, 40001, server, 443, 0x02, 0))
		close(packetDone)
	}()
	select {
	case <-packetDone:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("packet observation waited on the blocked host logger")
	}
	if len(timeline.Samples()) != 1 {
		t.Fatal("DNS sample was not committed before asynchronous logging")
	}

	// Reconnect churn behind the same permanently blocked sink must not create
	// one parked logger goroutine per timeline generation.
	goroutinesBefore := runtime.NumGoroutine()
	for i := 0; i < 2*firstLoadLogQueueSize; i++ {
		next := newFirstLoadTimeline(log)
		nextKey := NewDohKey("A", fmt.Sprintf("reconnect-%d.example", i))
		next.dnsStart(nextKey)
		next.dnsDone(nextKey, true)
		next.Close()
	}
	runtime.Gosched()
	if delta := runtime.NumGoroutine() - goroutinesBefore; 4 < delta {
		t.Fatalf("blocked logger multiplied reconnect workers by %d goroutines", delta)
	}

	close(log.release)
	timeline.Close()
}

// TestFirstLoadTimelineDeactivates pins the self-deactivation: once the flow
// and dns budgets are spent and every flow is logged, the hooks reduce to the
// atomic fast path (active false).
func TestFirstLoadTimelineDeactivates(t *testing.T) {
	local := netip.MustParseAddr("10.0.0.2")
	timeline := newFirstLoadTimeline(NewNoopLogger())
	defer timeline.Close()

	for i := range firstLoadMaxDnsQueries {
		key := NewDohKey("A", fmt.Sprintf("q%d.deactivate.test", i))
		timeline.dnsStart(key)
		timeline.dnsDone(key, true)
	}
	for i := range firstLoadMaxFlows {
		server := netip.AddrFrom4([4]byte{93, 184, 216, byte(1 + i)})
		localPort := uint16(40000 + i)
		timeline.observeSend(buildTestTcp4Packet(local, localPort, server, 443, 0x02, 0))
		timeline.observeReceive(buildTestTcp4Packet(server, 443, local, localPort, 0x12, 0))
		timeline.observeReceive(buildTestTcp4Packet(server, 443, local, localPort, 0x18, 64))
	}

	if timeline.active.Load() {
		t.Fatalf("timeline must deactivate after both budgets are spent and all flows logged")
	}
	samples := timeline.Samples()
	if len(samples) != firstLoadMaxDnsQueries+firstLoadMaxFlows {
		t.Fatalf("expected full sample set, got %d", len(samples))
	}

	// hooks are no-ops when inactive
	extra := netip.MustParseAddr("93.184.217.99")
	timeline.observeSend(buildTestTcp4Packet(local, 41000, extra, 443, 0x02, 0))
	if len(timeline.Samples()) != len(samples) {
		t.Fatalf("inactive timeline must not track")
	}
}

func TestFirstLoadCloseRejectsPostCloseAdmission(t *testing.T) {
	timeline := newFirstLoadTimeline(NewNoopLogger())
	timeline.Close()

	key := NewDohKey("A", "after-close.example")
	timeline.dnsStart(key)
	timeline.dnsDone(key, true)
	timeline.observeSend(buildTestTcp4Packet(
		netip.MustParseAddr("10.0.0.2"),
		40001,
		netip.MustParseAddr("93.184.216.34"),
		443,
		0x02,
		0,
	))

	if timeline.active.Load() {
		t.Fatal("closed timeline became active again")
	}
	if samples := timeline.Samples(); len(samples) != 0 {
		t.Fatalf("closed timeline admitted %d samples", len(samples))
	}
}

// TestUpgradeMuxTunnelDohCold pins the cold-state machine driving the
// adaptive local-fallback handicap: cold until first proven, warm after,
// cold again after an idle lease or consecutive failures, and warm again on
// the next success.
func TestUpgradeMuxTunnelDohCold(t *testing.T) {
	mux := &UpgradeMux{}
	if !mux.tunnelDohCold() {
		t.Fatalf("a fresh mux must be cold (tunnel unproven)")
	}
	mux.markTunnelDohProven()
	if mux.tunnelDohCold() {
		t.Fatalf("a proven mux must be warm")
	}
	mux.tunnelDohLastSuccessNanos.Store(
		time.Now().Add(-tunnelDohWarmLease - time.Millisecond).UnixNano(),
	)
	if !mux.tunnelDohCold() {
		t.Fatalf("an idle mux must expire its stale warm state")
	}
	mux.markTunnelDohProven()
	mux.recordTunnelDohFailureForGeneration(mux.tunnelDohGeneration.Load())
	if mux.tunnelDohCold() {
		t.Fatalf("one failure must not flip cold")
	}
	mux.recordTunnelDohFailureForGeneration(mux.tunnelDohGeneration.Load())
	if !mux.tunnelDohCold() {
		t.Fatalf("%d consecutive failures must flip cold", tunnelDohColdFailureCount)
	}
	mux.markTunnelDohProven()
	if mux.tunnelDohCold() {
		t.Fatalf("a success must restore warm")
	}
}
