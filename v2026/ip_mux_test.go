package connect

import (
	"context"
	"net"
	"net/netip"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// a recorder for sent (upstream) and received (downstream) packets
type ipMuxRecorder struct {
	mu                 sync.Mutex
	sent               [][]byte
	received           [][]byte
	receivedBatchCount int
}

// A batch upstream recorder consumes exact-flow groups without a route.
type ipMuxBatchUpstreamRecorder struct {
	groups [][]string
}

// Singular sends are not expected in the exact-flow regression.
func (self *ipMuxBatchUpstreamRecorder) SendPacket(
	source TransferPath,
	provideMode protocol.ProvideMode,
	packet []byte,
	timeout time.Duration,
) bool {
	return false
}

// Copies payload observations and consumes every packet in the group.
func (self *ipMuxBatchUpstreamRecorder) sendPacketGroup(
	source TransferPath,
	provideMode protocol.ProvideMode,
	group *ipPacketGroup,
	timeout time.Duration,
) bool {
	payloads := []string{}
	for _, packet := range group.packets {
		_, payload, err := ParseIpPathWithPayload(packet)
		if err != nil {
			panic(err)
		}
		payloads = append(payloads, string(payload))
		MessagePoolReturn(packet)
	}
	self.groups = append(self.groups, payloads)
	return true
}

// The recorder copies borrowed packets and records the number of boundary
// calls independently of the packet total.
func (self *ipMuxRecorder) receivePackets(
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	packets [][]byte,
) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.receivedBatchCount += 1
	for _, packet := range packets {
		self.received = append(self.received, append([]byte{}, packet...))
	}
}

func (self *ipMuxRecorder) upstream(source TransferPath, provideMode protocol.ProvideMode, packet []byte, timeout time.Duration) bool {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.sent = append(self.sent, append([]byte{}, packet...))
	return true
}

func (self *ipMuxRecorder) receive(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {
	self.mu.Lock()
	defer self.mu.Unlock()
	self.received = append(self.received, append([]byte{}, packet...))
}

func (self *ipMuxRecorder) counts() (int, int) {
	self.mu.Lock()
	defer self.mu.Unlock()
	return len(self.sent), len(self.received)
}

func newIpMuxIpv4Packet(sourceIp net.IP, destinationIp net.IP) []byte {
	packet := make([]byte, Ipv4HeaderSizeWithoutExtensions)
	writeIpv4Header(packet, ipProtocolNumberUdp, sourceIp.To4(), destinationIp.To4())
	return packet
}

func TestIpMuxPassthrough(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tun, err := CreateTunWithDefaults(ctx)
	if err != nil {
		t.Fatal(err)
	}

	rec := &ipMuxRecorder{}
	// onSend nil => pure pass-through
	mux := NewIpMux(ctx, tun, TransferPath{}, protocol.ProvideMode_Network, 0, nil, nil, rec.receive, nil)
	defer mux.Close()
	mux.SetUpstream(rec.upstream)

	// send path: not claimed => forwarded to upstream verbatim
	pkt := []byte("a-send-packet")
	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, pkt, 0) {
		t.Fatal("SendPacket returned false")
	}
	if sent, _ := rec.counts(); sent != 1 {
		t.Fatalf("upstream got %d packets, want 1", sent)
	}

	// receive path: external destination => dispatched downstream
	external := &IpPath{Version: 4, Protocol: IpProtocolUdp, DestinationIp: net.ParseIP("203.0.113.1"), DestinationPort: 443}
	mux.Receive(TransferPath{}, protocol.ProvideMode_Network, external, []byte("a-receive-packet"))
	if _, received := rec.counts(); received != 1 {
		t.Fatalf("downstream got %d packets, want 1", received)
	}

	// Return callbacks carry the canonical outbound flow path, while the
	// packet itself has the reverse direction. The packet destination is the
	// authoritative mux-local identity.
	addrs := tun.LocalAddresses()
	if len(addrs) == 0 {
		t.Fatal("tun has no local address")
	}
	localIp := net.IP(addrs[0].AsSlice())
	canonicalOutbound := &IpPath{
		Version:         4,
		Protocol:        IpProtocolUdp,
		SourceIp:        localIp,
		SourcePort:      40000,
		DestinationIp:   net.ParseIP("198.51.100.1"),
		DestinationPort: 443,
	}
	returnPacket := newIpMuxIpv4Packet(canonicalOutbound.DestinationIp, canonicalOutbound.SourceIp)
	mux.Receive(TransferPath{}, protocol.ProvideMode_Network, canonicalOutbound, returnPacket)
	if _, received := rec.counts(); received != 1 {
		t.Fatalf("downstream got %d packets after mux-addressed receive, want still 1", received)
	}
}

// A locally claimed send transfers packet ownership to the mux boundary.
func TestIpMuxClaimedSendReturnsPacketOwnership(t *testing.T) {
	packet := MessagePoolGet(64)
	witness := MessagePoolShareReadOnly(packet)
	mux := &IpMux{
		onSend: func(
			source TransferPath,
			provideMode protocol.ProvideMode,
			packet []byte,
			timeout time.Duration,
		) bool {
			return true
		},
	}
	if !mux.SendPacket(TransferPath{}, protocol.ProvideMode_Network, packet, 0) {
		t.Fatal("claimed packet was rejected")
	}
	if !MessagePoolReturn(witness) {
		t.Fatal("claimed send retained packet ownership")
	}
}

// One mixed TUN burst crosses the upstream once per exact directional flow,
// preserving first-seen flow order and packet order within each flow.
func TestIpMuxSendPacketBatchGroupsDirectionalFlows(t *testing.T) {
	packets := [][]byte{
		testingUdp4Packet("192.0.2.1", "203.0.113.7", 443, []byte("a1")),
		testingUdp4Packet("192.0.2.2", "203.0.113.8", 443, []byte("b1")),
		testingUdp4Packet("192.0.2.1", "203.0.113.7", 443, []byte("a2")),
	}
	recorder := &ipMuxBatchUpstreamRecorder{}
	groupClassificationCount := 0
	mux := &IpMux{
		onSend: func(
			source TransferPath,
			provideMode protocol.ProvideMode,
			packet []byte,
			timeout time.Duration,
		) bool {
			t.Fatal("batch path decomposed a homogeneous group")
			return false
		},
	}
	mux.setOnSendGroup(func(
		source TransferPath,
		provideMode protocol.ProvideMode,
		group *ipPacketGroup,
		timeout time.Duration,
	) bool {
		groupClassificationCount += 1
		return false
	})
	mux.setUpstreamGroupSend(recorder.sendPacketGroup)
	if sentPacketCount := mux.SendPacketBatch(
		TransferPath{},
		protocol.ProvideMode_Network,
		packets,
		0,
	); sentPacketCount != len(packets) {
		t.Fatalf("sent packets=%d, want %d", sentPacketCount, len(packets))
	}
	want := [][]string{{"a1", "a2"}, {"b1"}}
	if !reflect.DeepEqual(recorder.groups, want) {
		t.Fatalf("group payloads=%v, want %v", recorder.groups, want)
	}
	if groupClassificationCount != len(want) {
		t.Fatalf(
			"group classifications=%d, want %d",
			groupClassificationCount,
			len(want),
		)
	}
}

func TestIpMuxReceiveDoesNotTrustMisleadingPathDestination(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tun, err := CreateTunWithDefaults(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rec := &ipMuxRecorder{}
	mux := NewIpMux(ctx, tun, TransferPath{}, protocol.ProvideMode_Network, 0, nil, nil, rec.receive, nil)
	defer mux.Close()

	localIp := net.IP(tun.LocalAddresses()[0].AsSlice())
	misleadingPath := &IpPath{
		Version:       4,
		Protocol:      IpProtocolUdp,
		DestinationIp: localIp,
	}
	packetForOs := newIpMuxIpv4Packet(net.ParseIP("198.51.100.2"), net.ParseIP("203.0.113.2"))
	mux.Receive(TransferPath{}, protocol.ProvideMode_Network, misleadingPath, packetForOs)
	if _, received := rec.counts(); received != 1 {
		t.Fatalf("packet bytes addressed downstream were intercepted from misleading metadata: received=%d, want 1", received)
	}
}

func TestIpMuxReceiveRoutesLocalPacketWithoutPathMetadata(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tun, err := CreateTunWithDefaults(ctx)
	if err != nil {
		t.Fatal(err)
	}
	rec := &ipMuxRecorder{}
	mux := NewIpMux(ctx, tun, TransferPath{}, protocol.ProvideMode_Network, 0, nil, nil, rec.receive, nil)
	defer mux.Close()

	localIp := net.IP(tun.LocalAddresses()[0].AsSlice())
	packet := newIpMuxIpv4Packet(net.ParseIP("198.51.100.3"), localIp)
	mux.Receive(TransferPath{}, protocol.ProvideMode_Network, nil, packet)
	if _, received := rec.counts(); received != 0 {
		t.Fatalf("mux-local packet without metadata reached downstream: received=%d, want 0", received)
	}
}

// A downstream burst must cross the mux once while retaining ordinary packet
// order and suppressing duplicate singular delivery.
func TestIpMuxReceivePacketsBatchesDownstream(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tun, err := CreateTunWithDefaults(ctx)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &ipMuxRecorder{}
	mux := NewIpMux(
		ctx,
		tun,
		TransferPath{},
		protocol.ProvideMode_Network,
		0,
		nil,
		nil,
		recorder.receive,
		nil,
	)
	defer mux.Close()
	unsub := mux.AddPacketsReceiver(recorder.receivePackets)
	defer unsub()
	packets := [][]byte{
		newIpMuxIpv4Packet(net.ParseIP("198.51.100.4"), net.ParseIP("203.0.113.3")),
		newIpMuxIpv4Packet(net.ParseIP("198.51.100.5"), net.ParseIP("203.0.113.3")),
	}
	mux.ReceivePackets(
		TransferPath{},
		protocol.ProvideMode_Network,
		nil,
		packets,
	)
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	if recorder.receivedBatchCount != 1 || len(recorder.received) != len(packets) {
		t.Fatalf(
			"downstream batch calls=%d packets=%d, want 1/%d",
			recorder.receivedBatchCount,
			len(recorder.received),
			len(packets),
		)
	}
}

func TestIpMuxLocalPacketDestinationSupportsIpv6(t *testing.T) {
	local := netip.MustParseAddr("2001:db8::53")
	mux := &IpMux{localAddresses: []netip.Addr{local}}
	packet := make([]byte, 40)
	packet[0] = 0x60
	copy(packet[24:40], local.AsSlice())
	if !mux.isLocalPacketDestination(packet) {
		t.Fatal("IPv6 packet addressed to mux was not classified local")
	}
}

func TestIpMuxLocalPacketDestinationDoesNotAllocate(t *testing.T) {
	local := netip.MustParseAddr("192.0.2.53")
	mux := &IpMux{localAddresses: []netip.Addr{local}}
	packet := newIpMuxIpv4Packet(net.ParseIP("198.51.100.6"), net.IP(local.AsSlice()))
	var localDestination bool
	allocations := testing.AllocsPerRun(1000, func() {
		localDestination = mux.isLocalPacketDestination(packet)
	})
	if !localDestination {
		t.Fatal("packet addressed to mux was not classified local")
	}
	if allocations != 0 {
		t.Fatalf("local packet classification allocated %.2f objects per packet, want 0", allocations)
	}
}

func testIpMuxRejectedPumpPoolBalance(t *testing.T, installRejectingUpstream bool) {
	t.Helper()
	poolOutstanding := func() int64 {
		taken, returned, _ := MessagePoolCounts()
		return int64(taken) - int64(returned)
	}
	before := poolOutstanding()

	ctx, cancel := context.WithCancel(context.Background())
	settings := DefaultTunSettings()
	tun, err := CreateTun(ctx, settings)
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	mux := NewIpMux(
		ctx,
		tun,
		TransferPath{},
		protocol.ProvideMode_Network,
		0,
		nil,
		nil,
		nil,
		NewNoopLogger(),
	)
	if installRejectingUpstream {
		mux.SetUpstream(func(
			source TransferPath,
			provideMode protocol.ProvideMode,
			packet []byte,
			timeout time.Duration,
		) bool {
			return false
		})
	}
	// Exercise the pump's ownership boundary directly. A short Tcp dial can
	// expire before netstack schedules its Syn on a Cpu-constrained race run.
	packet := newTunLinkTestPacket(1)
	result := writeTunLinkPacket(tun.ep, packet)
	packet.DecRef()
	if result.n != 1 || result.err != nil {
		mux.Close()
		cancel()
		t.Fatalf("queue pump packet: %d, %v", result.n, result.err)
	}
	if !waitForCondition(time.Second, func() bool {
		return 0 < mux.rejectedPumpPacketCount.Load()
	}) {
		mux.Close()
		cancel()
		t.Fatal("internal stack emitted no packet into the rejected upstream")
	}
	mux.Close()
	cancel()

	if !waitForCondition(2*time.Second, func() bool {
		return poolOutstanding() <= before
	}) {
		after := poolOutstanding()
		t.Fatalf("rejected pump packet leaked a pooled buffer: outstanding %d -> %d", before, after)
	}
}

func TestIpMuxPumpReturnsPacketRejectedByUpstreamBackpressure(t *testing.T) {
	testIpMuxRejectedPumpPoolBalance(t, true)
}

func TestIpMuxPumpReturnsPacketBeforeUpstreamIsWired(t *testing.T) {
	testIpMuxRejectedPumpPoolBalance(t, false)
}
