package connect

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/urnetwork/connect/protocol"
)

// TestLocalUserNatSettingsMemoryScaled pins the memory-budget scaling of the
// nat's memory-dominant defaults: the per flow channel depths, the tcp
// window/read buffer, and the dmca flow cache (see `SetMemoryBudget`).
func TestLocalUserNatSettingsMemoryScaled(t *testing.T) {
	defer SetMemoryBudget(0)

	// the ios packet tunnel budget (scale 24/64)
	SetMemoryBudget(24 * 1024 * 1024)

	udpSettings := DefaultUdpBufferSettings()
	AssertEqual(t, udpSettings.SequenceBufferSize, 96)
	AssertEqual(t, udpSettings.IdleTimeout, 60*time.Second)
	AssertEqual(t, udpSettings.MaxWindowSize, uint32(393216))
	AssertEqual(t, udpSettings.GlobalLimit, 768)

	tcpSettings := DefaultTcpBufferSettings()
	AssertEqual(t, tcpSettings.SequenceBufferSize, 384)
	AssertEqual(t, tcpSettings.ReadBufferByteCount, 24576)
	AssertEqual(t, tcpSettings.MinWindowSize, uint32(65536))
	AssertEqual(t, tcpSettings.InitialWindowSize, uint32(262144))
	// 6 MiB scaled, snapped down to a power of 2 multiple of the min window.
	AssertEqual(t, tcpSettings.MaxWindowSize, uint32(4194304))
	AssertEqual(t, tcpSettings.GlobalLimit, 192)

	icmpSettings := DefaultIcmpBufferSettings()
	AssertEqual(t, icmpSettings.SequenceBufferSize, 24)
	AssertEqual(t, icmpSettings.IdleTimeout, 60*time.Second)
	AssertEqual(t, icmpSettings.GlobalLimit, 192)

	natSettings := DefaultLocalUserNatSettings()
	AssertEqual(t, natSettings.SequenceBufferSize, 384)

	dmcaSettings := DefaultDmcaSecurityPolicySettings()
	AssertEqual(t, dmcaSettings.MaxFlows, 24576)

	// floors at a tiny budget
	SetMemoryBudget(8 * 1024 * 1024)
	udpSettings = DefaultUdpBufferSettings()
	AssertEqual(t, udpSettings.SequenceBufferSize, 32)
	AssertEqual(t, udpSettings.MaxWindowSize, uint32(262144))
	AssertEqual(t, udpSettings.GlobalLimit, 256)
	tcpSettings = DefaultTcpBufferSettings()
	AssertEqual(t, tcpSettings.SequenceBufferSize, 192)
	AssertEqual(t, tcpSettings.ReadBufferByteCount, 16384)
	AssertEqual(t, tcpSettings.InitialWindowSize, uint32(131072))
	AssertEqual(t, tcpSettings.MaxWindowSize, uint32(2097152))
	AssertEqual(t, tcpSettings.GlobalLimit, 64)
	icmpSettings = DefaultIcmpBufferSettings()
	AssertEqual(t, icmpSettings.SequenceBufferSize, 16)
	AssertEqual(t, icmpSettings.GlobalLimit, 64)
	AssertEqual(t, DefaultDmcaSecurityPolicySettings().MaxFlows, 8192)

	// no budget identifies a server/generic caller: it keeps the unscaled
	// buffers but does not silently inherit the constrained-device flow cap.
	// Actual providers select the explicit provider profile.
	SetMemoryBudget(0)
	udpSettings = DefaultUdpBufferSettings()
	AssertEqual(t, udpSettings.SequenceBufferSize, 256)
	AssertEqual(t, udpSettings.MaxWindowSize, uint32(1048576))
	AssertEqual(t, udpSettings.GlobalLimit, 0)
	tcpSettings = DefaultTcpBufferSettings()
	AssertEqual(t, tcpSettings.SequenceBufferSize, 1024)
	AssertEqual(t, tcpSettings.ReadBufferByteCount, 65536)
	AssertEqual(t, tcpSettings.InitialWindowSize, uint32(1048576))
	AssertEqual(t, tcpSettings.MaxWindowSize, uint32(16777216))
	AssertEqual(t, tcpSettings.GlobalLimit, 0)
	icmpSettings = DefaultIcmpBufferSettings()
	AssertEqual(t, icmpSettings.SequenceBufferSize, 64)
	AssertEqual(t, icmpSettings.GlobalLimit, 0)
	AssertEqual(t, DefaultLocalUserNatSettings().SequenceBufferSize, 1024)
	AssertEqual(t, DefaultDmcaSecurityPolicySettings().MaxFlows, 65536)
	providerSettings := DefaultProviderLocalUserNatSettings()
	AssertEqual(t, providerSettings.UdpBufferSettings.UserLimit, 0)
	AssertEqual(t, providerSettings.UdpBufferSettings.GlobalLimit, 0)
	AssertEqual(t, providerSettings.TcpBufferSettings.UserLimit, 0)
	AssertEqual(t, providerSettings.TcpBufferSettings.GlobalLimit, 0)
	// an unbudgeted provider keeps long-lived plain-udp NAT bindings alive:
	// the provider-tuned idle, longer than the general 60s reap
	AssertEqual(t, providerSettings.UdpBufferSettings.IdleTimeout, providerUdpIdleTimeout)
	if providerSettings.UdpBufferSettings.IdleTimeout <= udpSettings.IdleTimeout {
		t.Errorf("provider udp idle %s must exceed the general udp idle %s",
			providerSettings.UdpBufferSettings.IdleTimeout, udpSettings.IdleTimeout)
	}

	// the process budget is not the provider's flow policy: the targetless
	// provider profile is the same with a budget set (the full row is
	// TestProviderProfileIsIndependentOfTheProcessBudget)
	SetMemoryBudget(24 * 1024 * 1024)
	budgetedProviderSettings := DefaultProviderLocalUserNatSettings()
	AssertEqual(t, budgetedProviderSettings.UdpBufferSettings.IdleTimeout, providerUdpIdleTimeout)
	AssertEqual(t, budgetedProviderSettings.UdpBufferSettings.GlobalLimit, 0)
	SetMemoryBudget(0)

	// Invariants at every budget tier:
	// - the TCP channel remains memory-scaled rather than expanding to the
	//   high-BDP maximum window for every live flow; and
	// - the maximum window stays a power-of-two multiple of the minimum window,
	//   so the window-doubling ladder lands exactly on the maximum.
	for _, budget := range []ByteCount{0, mib(8), mib(16), mib(24), mib(32), mib(48), mib(64), mib(128)} {
		SetMemoryBudget(budget)
		tcpSettings := DefaultTcpBufferSettings()
		wantSequenceBufferSize := MemoryScaledCount(defaultTcpFlowBufferSize, 192)
		if tcpSettings.SequenceBufferSize != wantSequenceBufferSize {
			t.Errorf(
				"budget %d: tcp depth=%d, want memory-scaled depth=%d",
				budget,
				tcpSettings.SequenceBufferSize,
				wantSequenceBufferSize,
			)
		}
		if tcpSettings.MaxWindowSize < tcpSettings.MinWindowSize {
			t.Errorf("budget %d: max window %d below min window %d",
				budget, tcpSettings.MaxWindowSize, tcpSettings.MinWindowSize)
		}
		for w := tcpSettings.MinWindowSize; ; w *= 2 {
			if w == tcpSettings.MaxWindowSize {
				break
			}
			if w > tcpSettings.MaxWindowSize {
				t.Errorf("budget %d: max window %d is not a power of 2 multiple of the min window %d",
					budget, tcpSettings.MaxWindowSize, tcpSettings.MinWindowSize)
				break
			}
		}
	}
}

// Both targetless and explicitly targeted provider flow policies remain
// independent of process allocation budgets, including desktop-sized values.
func TestProviderProfileIsIndependentOfTheProcessBudget(t *testing.T) {
	restore := MemoryBudget()
	t.Cleanup(func() { SetMemoryBudget(restore) })

	type flowLimits struct {
		udpUserLimit    int
		udpGlobalLimit  int
		udpIdleTimeout  time.Duration
		tcpUserLimit    int
		tcpGlobalLimit  int
		tcpIdleTimeout  time.Duration
		icmpUserLimit   int
		icmpGlobalLimit int
	}
	limitsOf := func(settings *LocalUserNatSettings) flowLimits {
		return flowLimits{
			udpUserLimit:    settings.UdpBufferSettings.UserLimit,
			udpGlobalLimit:  settings.UdpBufferSettings.GlobalLimit,
			udpIdleTimeout:  settings.UdpBufferSettings.IdleTimeout,
			tcpUserLimit:    settings.TcpBufferSettings.UserLimit,
			tcpGlobalLimit:  settings.TcpBufferSettings.GlobalLimit,
			tcpIdleTimeout:  settings.TcpBufferSettings.IdleTimeout,
			icmpUserLimit:   settings.IcmpBufferSettings.UserLimit,
			icmpGlobalLimit: settings.IcmpBufferSettings.GlobalLimit,
		}
	}

	SetMemoryBudget(0)
	unbudgeted := limitsOf(DefaultProviderLocalUserNatSettings())
	// today's targetless provider, stated rather than sampled
	AssertEqual(t, unbudgeted, flowLimits{
		udpIdleTimeout: providerUdpIdleTimeout,
		tcpIdleTimeout: 300 * time.Second,
	})
	unbudgetedTargeted := limitsOf(DefaultProviderLocalUserNatSettingsWithMemoryTarget(mib(64)))
	if unbudgetedTargeted.udpGlobalLimit <= 0 || unbudgetedTargeted.tcpGlobalLimit <= 0 {
		t.Fatalf("a targeted provider has no flow caps: %+v", unbudgetedTargeted)
	}

	for _, budget := range []ByteCount{mib(8), mib(24), mib(64), mib(256), gib(8)} {
		SetMemoryBudget(budget)
		if budgeted := limitsOf(DefaultProviderLocalUserNatSettings()); budgeted != unbudgeted {
			t.Errorf(
				"budget %d: the targetless provider profile is %+v, unbudgeted it is %+v; a process budget must not change the provider's flow limits or idle",
				budget, budgeted, unbudgeted,
			)
		}
		if budgeted := limitsOf(DefaultProviderLocalUserNatSettingsWithMemoryTarget(mib(64))); budgeted != unbudgetedTargeted {
			t.Errorf(
				"budget %d: the 64 MiB target profile is %+v, unbudgeted it is %+v; the caps are the target's alone",
				budget, budgeted, unbudgetedTargeted,
			)
		}
	}
}

// pollUntil polls `condition` to true within `timeout`, else fails the test.
func pollUntil(t *testing.T, timeout time.Duration, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if condition() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", description)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// startUdpSink starts a loopback udp socket of the family that discards
// received datagrams, so nat udp flows have a stable destination.
func startUdpSink(t *testing.T, ipVersion int) (port uint16, closeFn func()) {
	t.Helper()
	conn, err := net.ListenUDP(testUdpNetwork(ipVersion), testLoopbackUdpAddr(ipVersion))
	if err != nil {
		t.Fatalf("udp sink listen: %v", err)
	}
	go HandleError(func() {
		buffer := make([]byte, 2048)
		for {
			if _, _, err := conn.ReadFromUDP(buffer); err != nil {
				return
			}
		}
	})
	return uint16(conn.LocalAddr().(*net.UDPAddr).Port), func() {
		conn.Close()
	}
}

// startTcpHoldListener accepts and holds connections on the family's
// loopback, so nat tcp flows stay established.
func startTcpHoldListener(t *testing.T, ipVersion int) (port uint16, closeFn func()) {
	t.Helper()
	listener, err := net.Listen(testTcpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go HandleError(func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			defer conn.Close()
		}
	})
	return uint16(listener.Addr().(*net.TCPAddr).Port), func() {
		listener.Close()
	}
}

// testFlowIps are a crafted nat flow's source (a private host of the family)
// and loopback destination, in the byte form the parsers hand the buffers.
func testFlowIps(ipVersion int, sourceHost byte) (sourceIp net.IP, destinationIp net.IP) {
	if ipVersion == 4 {
		return net.IPv4(10, 0, 0, sourceHost).To4(), net.IPv4(127, 0, 0, 1).To4()
	}
	return net.ParseIP(fmt.Sprintf("fd00::%x", sourceHost)), net.ParseIP("::1")
}

// testFlowBufferState is the flow-table state every family's buffer exposes
// to the cap tests: the live source ports (or icmp identifiers), the count,
// and the index-consistency check.
type testFlowBufferState struct {
	keys             func() map[uint16]bool
	sequenceCount    func() int
	assertConsistent func(t *testing.T, limit int) int
}

// udpFlowBufferState reads a udp flow table of either family.
func udpFlowBufferState[BufferId comparable](buffer *UdpBuffer[BufferId]) testFlowBufferState {
	return testFlowBufferState{
		keys: func() map[uint16]bool {
			buffer.mutex.Lock()
			defer buffer.mutex.Unlock()
			ports := map[uint16]bool{}
			for _, sequence := range buffer.sequences {
				ports[sequence.sourcePort] = true
			}
			return ports
		},
		sequenceCount: func() int {
			buffer.mutex.Lock()
			defer buffer.mutex.Unlock()
			return len(buffer.sequences)
		},
		assertConsistent: func(t *testing.T, limit int) int {
			t.Helper()
			buffer.mutex.Lock()
			defer buffer.mutex.Unlock()
			if limit < len(buffer.sequences) {
				t.Fatalf("global cap broken: %d sequences > limit %d", len(buffer.sequences), limit)
			}
			total := 0
			for source, sourceSequences := range buffer.sourceSequences {
				if len(sourceSequences) == 0 {
					t.Fatalf("index drift: empty source map retained for %s", source)
				}
				for bufferId, sequence := range sourceSequences {
					if buffer.sequences[bufferId] != sequence {
						t.Fatal("index drift: source-indexed sequence not in the flow table")
					}
					if sequence.source != source {
						t.Fatal("index drift: sequence filed under the wrong source")
					}
					total += 1
				}
			}
			if total != len(buffer.sequences) {
				t.Fatalf("index drift: %d source-indexed vs %d sequences", total, len(buffer.sequences))
			}
			return len(buffer.sequences)
		},
	}
}

// tcpFlowBufferState reads a tcp flow table of either family.
func tcpFlowBufferState[BufferId comparable](buffer *TcpBuffer[BufferId]) testFlowBufferState {
	return testFlowBufferState{
		keys: func() map[uint16]bool {
			buffer.mutex.Lock()
			defer buffer.mutex.Unlock()
			ports := map[uint16]bool{}
			for _, sequence := range buffer.sequences {
				ports[sequence.sourcePort] = true
			}
			return ports
		},
		sequenceCount: func() int {
			buffer.mutex.Lock()
			defer buffer.mutex.Unlock()
			return len(buffer.sequences)
		},
		assertConsistent: func(t *testing.T, limit int) int {
			t.Helper()
			buffer.mutex.Lock()
			defer buffer.mutex.Unlock()
			if limit < len(buffer.sequences) {
				t.Fatalf("global cap broken: %d sequences > limit %d", len(buffer.sequences), limit)
			}
			total := 0
			for source, sourceSequences := range buffer.sourceSequences {
				if len(sourceSequences) == 0 {
					t.Fatalf("index drift: empty source map retained for %s", source)
				}
				for bufferId, sequence := range sourceSequences {
					if buffer.sequences[bufferId] != sequence {
						t.Fatal("index drift: source-indexed sequence not in the flow table")
					}
					if sequence.source != source {
						t.Fatal("index drift: sequence filed under the wrong source")
					}
					total += 1
				}
			}
			if total != len(buffer.sequences) {
				t.Fatalf("index drift: %d source-indexed vs %d sequences", total, len(buffer.sequences))
			}
			return len(buffer.sequences)
		},
	}
}

// icmpFlowBufferState reads an icmp flow table of either family; the keys
// are the echo identifiers.
func icmpFlowBufferState[BufferId comparable](buffer *IcmpBuffer[BufferId]) testFlowBufferState {
	return testFlowBufferState{
		keys: func() map[uint16]bool {
			buffer.mutex.Lock()
			defer buffer.mutex.Unlock()
			ids := map[uint16]bool{}
			for _, sequence := range buffer.sequences {
				ids[sequence.identifier] = true
			}
			return ids
		},
		sequenceCount: func() int {
			buffer.mutex.Lock()
			defer buffer.mutex.Unlock()
			return len(buffer.sequences)
		},
	}
}

type testUdpSendFunction func(source TransferPath, provideMode protocol.ProvideMode, udp *parsedUdp, timeout time.Duration, ipPacket []byte) (bool, error)
type testTcpSendFunction func(source TransferPath, provideMode protocol.ProvideMode, tcp *parsedTcp, timeout time.Duration, ipPacket []byte) (bool, error)
type testIcmpSendFunction func(source TransferPath, provideMode protocol.ProvideMode, icmp *parsedIcmp, timeout time.Duration, ipPacket []byte) (bool, error)

// newTestUdpFlowBuffer builds the family's udp buffer and returns its send
// entry point with the state the tests read.
func newTestUdpFlowBuffer(ctx context.Context, ipVersion int, settings *UdpBufferSettings) (testUdpSendFunction, testFlowBufferState) {
	receive := func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {}
	if ipVersion == 6 {
		buffer := NewUdp6Buffer(ctx, receive, settings)
		return buffer.send, udpFlowBufferState(&buffer.UdpBuffer)
	}
	buffer := NewUdp4Buffer(ctx, receive, settings)
	return buffer.send, udpFlowBufferState(&buffer.UdpBuffer)
}

// newTestTcpFlowBuffer is newTestUdpFlowBuffer for tcp.
func newTestTcpFlowBuffer(ctx context.Context, ipVersion int, settings *TcpBufferSettings) (testTcpSendFunction, testFlowBufferState) {
	receive := func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {}
	if ipVersion == 6 {
		buffer := NewTcp6Buffer(ctx, receive, settings)
		return buffer.send, tcpFlowBufferState(&buffer.TcpBuffer)
	}
	buffer := NewTcp4Buffer(ctx, receive, settings)
	return buffer.send, tcpFlowBufferState(&buffer.TcpBuffer)
}

// newTestIcmpFlowBuffer is newTestUdpFlowBuffer for icmp.
func newTestIcmpFlowBuffer(ctx context.Context, ipVersion int, settings *IcmpBufferSettings) (testIcmpSendFunction, testFlowBufferState) {
	receive := func(source TransferPath, provideMode protocol.ProvideMode, ipPath *IpPath, packet []byte) {}
	if ipVersion == 6 {
		buffer := NewIcmp6Buffer(ctx, receive, settings)
		return buffer.send, icmpFlowBufferState(&buffer.IcmpBuffer)
	}
	buffer := NewIcmp4Buffer(ctx, receive, settings)
	return buffer.send, icmpFlowBufferState(&buffer.IcmpBuffer)
}

// TestUdpBufferFlowLimits exercises the per source (`UserLimit`) and
// aggregate (`GlobalLimit`) udp flow caps: over-limit creates evict the
// approximately idle-most sampled flow, the newest flows survive, and eager
// removal keeps the flow map at the exact cap. Both families.
func TestUdpBufferFlowLimits(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sinkPort, closeSink := startUdpSink(t, ipVersion)
		defer closeSink()

		udpBufferSettings := DefaultUdpBufferSettingsWithBufferSize(8)
		udpBufferSettings.UserLimit = 2
		udpBufferSettings.GlobalLimit = 3

		sendUdp, buffer := newTestUdpFlowBuffer(ctx, ipVersion, udpBufferSettings)
		sourceIp, destinationIp := testFlowIps(ipVersion, 1)

		send := func(source TransferPath, sourcePort uint16) {
			t.Helper()
			packet := MessagePoolGet(32)
			parsed := &parsedUdp{
				sourceIp:        sourceIp,
				destinationIp:   destinationIp,
				sourcePort:      sourcePort,
				destinationPort: sinkPort,
				payload:         packet[:4],
			}
			if success, err := sendUdp(source, protocol.ProvideMode_Network, parsed, -1, packet); err != nil || !success {
				MessagePoolReturn(packet)
				t.Fatalf("udp send %d: success=%t err=%v", sourcePort, success, err)
			}
			// keep the lru order deterministic
			time.Sleep(10 * time.Millisecond)
		}

		sourceA := SourceId(NewId())
		// two flows fill the per source limit
		send(sourceA, 40001)
		send(sourceA, 40002)
		// the third and fourth evict the idle-most flow before their insert
		send(sourceA, 40003)
		send(sourceA, 40004)
		pollUntil(t, 5*time.Second, "per source lru eviction", func() bool {
			ports := buffer.keys()
			return !ports[40001] && ports[40003] && ports[40004]
		})

		// a second source pushes the aggregate over the global limit: the
		// idle-most flows across all sources evict, the newest flows survive
		sourceB := SourceId(NewId())
		send(sourceB, 41001)
		send(sourceB, 41002)
		pollUntil(t, 5*time.Second, "global lru eviction", func() bool {
			ports := buffer.keys()
			return len(ports) <= udpBufferSettings.GlobalLimit && ports[41001] && ports[41002]
		})
	})
}

// TestTcpBufferFlowLimits exercises the same caps for tcp flows: syns over
// the per source and global limits evict the idle-most established flow.
func TestTcpBufferFlowLimits(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// accept and hold connections so the nat flows stay established
		listenerPort, closeListener := startTcpHoldListener(t, ipVersion)
		defer closeListener()

		tcpBufferSettings := DefaultTcpBufferSettingsWithBufferSize(8)
		tcpBufferSettings.UserLimit = 2
		tcpBufferSettings.GlobalLimit = 3

		sendTcp, buffer := newTestTcpFlowBuffer(ctx, ipVersion, tcpBufferSettings)
		sourceIp, destinationIp := testFlowIps(ipVersion, 1)

		sendSyn := func(source TransferPath, sourcePort uint16) {
			t.Helper()
			packet := MessagePoolGet(32)
			parsed := &parsedTcp{
				sourceIp:        sourceIp,
				destinationIp:   destinationIp,
				sourcePort:      sourcePort,
				destinationPort: listenerPort,
				syn:             true,
				seq:             1000,
				windowSize:      65535,
			}
			if success, err := sendTcp(source, protocol.ProvideMode_Network, parsed, -1, packet); err != nil || !success {
				MessagePoolReturn(packet)
				t.Fatalf("tcp syn %d: success=%t err=%v", sourcePort, success, err)
			}
			time.Sleep(10 * time.Millisecond)
		}

		sourceA := SourceId(NewId())
		sendSyn(sourceA, 40001)
		sendSyn(sourceA, 40002)
		sendSyn(sourceA, 40003)
		sendSyn(sourceA, 40004)
		pollUntil(t, 5*time.Second, "per source lru eviction", func() bool {
			ports := buffer.keys()
			return !ports[40001] && ports[40003] && ports[40004]
		})

		sourceB := SourceId(NewId())
		sendSyn(sourceB, 41001)
		sendSyn(sourceB, 41002)
		pollUntil(t, 5*time.Second, "global lru eviction", func() bool {
			ports := buffer.keys()
			return len(ports) <= tcpBufferSettings.GlobalLimit && ports[41001] && ports[41002]
		})
	})
}

// TestUdpBufferGlobalLimitConcurrent drives GlobalLimit under concurrent
// new-flow creation at and over the cap (run with -race): the cap holds, the
// eviction loop terminates, the `sequences`/`sourceSequences` indexes stay
// exactly consistent, and dispatch is not stalled (a fresh flow still lands
// after the burst). Verifies the applyLruMapLimit rework under concurrency.
func TestUdpBufferGlobalLimitConcurrent(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sinkPort, closeSink := startUdpSink(t, ipVersion)
		defer closeSink()

		udpBufferSettings := DefaultUdpBufferSettingsWithBufferSize(8)
		udpBufferSettings.GlobalLimit = 6

		sendUdp, buffer := newTestUdpFlowBuffer(ctx, ipVersion, udpBufferSettings)
		sourceIp, destinationIp := testFlowIps(ipVersion, 1)

		send := func(source TransferPath, sourcePort uint16) {
			packet := MessagePoolGet(32)
			parsed := &parsedUdp{
				sourceIp:        sourceIp,
				destinationIp:   destinationIp,
				sourcePort:      sourcePort,
				destinationPort: sinkPort,
				payload:         packet[:4],
			}
			// under concurrent eviction a create can lose the race and fail —
			// that is allowed; the invariants below are what must hold
			if success, err := sendUdp(source, protocol.ProvideMode_Network, parsed, -1, packet); err != nil || !success {
				MessagePoolReturn(packet)
			}
		}

		// 8 sources x 8 flows: far over the cap, all created concurrently
		var wg sync.WaitGroup
		for s := 0; s < 8; s++ {
			wg.Add(1)
			source := SourceId(NewId())
			basePort := uint16(42000 + 100*s)
			go func() {
				defer wg.Done()
				for f := uint16(0); f < 8; f++ {
					send(source, basePort+f)
				}
			}()
		}
		wg.Wait()

		if count := buffer.assertConsistent(t, udpBufferSettings.GlobalLimit); count == 0 {
			t.Fatal("no flows survived the burst")
		}

		// dispatch is not stalled: a fresh flow still lands at the cap
		lateSource := SourceId(NewId())
		lateSourceIp, _ := testFlowIps(ipVersion, 2)
		packet := MessagePoolGet(32)
		parsed := &parsedUdp{
			sourceIp:        lateSourceIp,
			destinationIp:   destinationIp,
			sourcePort:      45001,
			destinationPort: sinkPort,
			payload:         packet[:4],
		}
		if success, err := sendUdp(lateSource, protocol.ProvideMode_Network, parsed, -1, packet); err != nil || !success {
			MessagePoolReturn(packet)
			t.Fatalf("post-burst flow create stalled: success=%t err=%v", success, err)
		}
		buffer.assertConsistent(t, udpBufferSettings.GlobalLimit)
	})
}

// TestTcpBufferGlobalLimitConcurrent is the tcp shape of
// TestUdpBufferGlobalLimitConcurrent: concurrent SYNs at/over GlobalLimit
// (run with -race).
func TestTcpBufferGlobalLimitConcurrent(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		listenerPort, closeListener := startTcpHoldListener(t, ipVersion)
		defer closeListener()

		tcpBufferSettings := DefaultTcpBufferSettingsWithBufferSize(8)
		tcpBufferSettings.GlobalLimit = 6

		sendTcp, buffer := newTestTcpFlowBuffer(ctx, ipVersion, tcpBufferSettings)
		sourceIp, destinationIp := testFlowIps(ipVersion, 1)

		sendSyn := func(source TransferPath, sourcePort uint16) {
			packet := MessagePoolGet(32)
			parsed := &parsedTcp{
				sourceIp:        sourceIp,
				destinationIp:   destinationIp,
				sourcePort:      sourcePort,
				destinationPort: listenerPort,
				syn:             true,
				seq:             1000,
				windowSize:      65535,
			}
			if success, err := sendTcp(source, protocol.ProvideMode_Network, parsed, -1, packet); err != nil || !success {
				MessagePoolReturn(packet)
			}
		}

		var wg sync.WaitGroup
		for s := 0; s < 8; s++ {
			wg.Add(1)
			source := SourceId(NewId())
			basePort := uint16(43000 + 100*s)
			go func() {
				defer wg.Done()
				for f := uint16(0); f < 8; f++ {
					sendSyn(source, basePort+f)
				}
			}()
		}
		wg.Wait()

		if count := buffer.assertConsistent(t, tcpBufferSettings.GlobalLimit); count == 0 {
			t.Fatal("no flows survived the burst")
		}

		// dispatch is not stalled: a fresh SYN still lands at the cap
		lateSource := SourceId(NewId())
		lateSourceIp, _ := testFlowIps(ipVersion, 2)
		packet := MessagePoolGet(32)
		parsed := &parsedTcp{
			sourceIp:        lateSourceIp,
			destinationIp:   destinationIp,
			sourcePort:      45002,
			destinationPort: listenerPort,
			syn:             true,
			seq:             1000,
			windowSize:      65535,
		}
		if success, err := sendTcp(lateSource, protocol.ProvideMode_Network, parsed, -1, packet); err != nil || !success {
			MessagePoolReturn(packet)
			t.Fatalf("post-burst SYN create stalled: success=%t err=%v", success, err)
		}
		buffer.assertConsistent(t, tcpBufferSettings.GlobalLimit)
	})
}

// TestUdpBufferIdleReap pins the udp idle reap: an idle flow releases its
// sequence (channels, read buffer, goroutines, socket) after `IdleTimeout`,
// without any close signal from the source.
func TestUdpBufferIdleReap(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sinkPort, closeSink := startUdpSink(t, ipVersion)
		defer closeSink()

		udpBufferSettings := DefaultUdpBufferSettingsWithBufferSize(8)
		udpBufferSettings.IdleTimeout = 250 * time.Millisecond

		sendUdp, buffer := newTestUdpFlowBuffer(ctx, ipVersion, udpBufferSettings)
		sourceIp, destinationIp := testFlowIps(ipVersion, 1)

		source := SourceId(NewId())
		for s := 0; s < 3; s++ {
			packet := MessagePoolGet(32)
			parsed := &parsedUdp{
				sourceIp:        sourceIp,
				destinationIp:   destinationIp,
				sourcePort:      uint16(42001 + s),
				destinationPort: sinkPort,
				payload:         packet[:4],
			}
			if success, err := sendUdp(source, protocol.ProvideMode_Network, parsed, -1, packet); err != nil || !success {
				MessagePoolReturn(packet)
				t.Fatalf("udp send %d: success=%t err=%v", s, success, err)
			}
		}

		AssertEqual(t, buffer.sequenceCount(), 3)

		pollUntil(t, 10*time.Second, "idle flows reaped", func() bool {
			return buffer.sequenceCount() == 0
		})
	})
}

// TestMemoryScaledCaps pins the memory-budget scaling of the remaining
// bounded-by-default caps: the webrtc peer connection count and the
// provider's return provide mode source map.
func TestMemoryScaledCaps(t *testing.T) {
	defer SetMemoryBudget(0)

	SetMemoryBudget(24 * 1024 * 1024)
	AssertEqual(t, DefaultWebRtcSettings().MaxPeerConnectionCount, 12)
	AssertEqual(t, DefaultRemoteUserNatProviderSettings().MaxSourceCount, 3072)

	SetMemoryBudget(8 * 1024 * 1024)
	AssertEqual(t, DefaultWebRtcSettings().MaxPeerConnectionCount, 8)
	AssertEqual(t, DefaultRemoteUserNatProviderSettings().MaxSourceCount, 1024)

	SetMemoryBudget(0)
	AssertEqual(t, DefaultWebRtcSettings().MaxPeerConnectionCount, 32)
	AssertEqual(t, DefaultRemoteUserNatProviderSettings().MaxSourceCount, 8192)
}

type testing_noopSignalSender struct {
}

// SendSignal consumes and discards one owned signaling frame.
func (self *testing_noopSignalSender) SendSignal(_ Id, signal *protocol.Frame, _ ...any) {
	MessagePoolReturn(signal.MessageBytes)
}

// TestWebRtcManagerPeerConnCap exercises the peer connection cap: creates at
// the cap are refused (the stream stays on the platform transport), while a
// create for an existing key replaces that connection and is allowed.
func TestWebRtcManagerPeerConnCap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultWebRtcSettings()
	settings.Log = NewNoopLogger()
	// no stun: the connections never need to gather beyond host candidates
	settings.IceServerUrls = nil
	settings.MaxPeerConnectionCount = 2

	manager := newTestWebRtcManager(t, ctx, &testing_noopSignalSender{}, settings)

	sourceId := NewId()
	newPath := func() TransferPath {
		return TransferPath{
			SourceId:      sourceId,
			DestinationId: NewId(),
			StreamId:      NewId(),
		}
	}

	pathA := newPath()
	if _, err := manager.NewP2pConnActive(ctx, pathA); err != nil {
		t.Fatalf("conn a: %v", err)
	}
	pathB := newPath()
	if _, err := manager.NewP2pConnActive(ctx, pathB); err != nil {
		t.Fatalf("conn b: %v", err)
	}

	// at the cap a new key is refused
	if _, err := manager.NewP2pConnActive(ctx, newPath()); err == nil {
		t.Fatal("expected the peer connection cap to refuse a new key")
	}

	// a create for an existing key replaces in place and is allowed
	if _, err := manager.NewP2pConnActive(ctx, pathA); err != nil {
		t.Fatalf("replacement for an existing key must be allowed: %v", err)
	}

	peerConnCount := func() int {
		manager.stateLock.Lock()
		defer manager.stateLock.Unlock()
		return len(manager.peerConns)
	}
	AssertEqual(t, peerConnCount(), 2)
}

// TestRemoteUserNatProviderSourceCap bounds the provider's per-source return
// provide mode map: at the cap an arbitrary entry evicts to admit the new
// source (the return path falls back to the packet's carried provide mode).
func TestRemoteUserNatProviderSourceCap(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	providerClient := NewClient(ctx, NewId(), NewNoContractClientOob(), DefaultClientSettings())
	defer providerClient.Cancel()

	localUserNat := NewLocalUserNatWithDefaults(ctx, "test-source-cap")
	defer localUserNat.Close()

	settings := DefaultRemoteUserNatProviderSettings()
	settings.MaxSourceCount = 4
	provider := NewRemoteUserNatProvider(providerClient, localUserNat, settings)
	defer provider.Close()

	sourceCount := func() int {
		provider.stateLock.Lock()
		defer provider.stateLock.Unlock()
		return len(provider.sourceProvideMode)
	}

	var lastSourceId Id
	for i := 0; i < 10; i++ {
		lastSourceId = NewId()
		provider.recordSourceProvideMode(lastSourceId, protocol.ProvideMode_Public)
	}
	AssertEqual(t, sourceCount(), 4)
	// the newest source is always admitted
	AssertEqual(t, provider.sourceReturnProvideMode(lastSourceId, protocol.ProvideMode_Network), protocol.ProvideMode_Public)

	// updating an existing entry does not evict
	provider.recordSourceProvideMode(lastSourceId, protocol.ProvideMode_Network)
	AssertEqual(t, sourceCount(), 4)
	AssertEqual(t, provider.sourceReturnProvideMode(lastSourceId, protocol.ProvideMode_Public), protocol.ProvideMode_Network)
}

// TestIpEgressTcp4MemoryBudget runs real tcp echo flows through the nat with
// the ios packet tunnel budget applied, so the scaled per flow depths, read
// buffer, and snapped window carry real traffic end to end. Both families
// (the name predates the dual-stack nat; the subtests are v4 and v6).
func TestIpEgressTcp4MemoryBudget(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping egress memory budget test in short mode")
	}

	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		defer SetMemoryBudget(0)
		SetMemoryBudget(24 * 1024 * 1024)

		ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
		defer cancel()

		echoListener, err := net.Listen(testTcpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
		AssertEqual(t, err, nil)
		defer echoListener.Close()
		go HandleError(func() {
			for {
				conn, err := echoListener.Accept()
				if err != nil {
					return
				}
				go HandleError(func() {
					defer conn.Close()
					io.Copy(conn, conn)
				})
			}
		})

		tun, err := CreateTunWithDefaults(ctx)
		AssertEqual(t, err, nil)
		defer tun.Close()

		// the scaled defaults under the budget
		localUserNat := NewLocalUserNat(ctx, "testEgressBudget", DefaultLocalUserNatSettings())
		defer localUserNat.Close()

		removeReceiveCallback := bridgeTunToLocalUserNat(tun, localUserNat, SourceId(NewId()))
		defer removeReceiveCallback()

		payloadSizes := []int{1, 1381, 16384, 1 << 20}

		parallelCount := 2
		flowErrs := make(chan error, parallelCount)
		for p := 0; p < parallelCount; p += 1 {
			go HandleError(func() {
				flowErrs <- func() error {
					conn, err := tun.DialContext(ctx, "tcp", echoListener.Addr().String())
					if err != nil {
						return fmt.Errorf("dial: %w", err)
					}
					defer conn.Close()

					for _, payloadSize := range payloadSizes {
						payload := testingEgressPayload(p, payloadSize)

						readErr := make(chan error, 1)
						go HandleError(func() {
							readErr <- func() error {
								echoPayload := make([]byte, payloadSize)
								conn.SetReadDeadline(time.Now().Add(60 * time.Second))
								if _, err := io.ReadFull(conn, echoPayload); err != nil {
									return fmt.Errorf("read size=%d: %w", payloadSize, err)
								}
								if !bytes.Equal(payload, echoPayload) {
									return fmt.Errorf("echo mismatch size=%d", payloadSize)
								}
								return nil
							}()
						})

						conn.SetWriteDeadline(time.Now().Add(60 * time.Second))
						if _, err := conn.Write(payload); err != nil {
							return fmt.Errorf("write size=%d: %w", payloadSize, err)
						}
						if err := <-readErr; err != nil {
							return err
						}
					}
					return nil
				}()
			})
		}
		for p := 0; p < parallelCount; p += 1 {
			select {
			case err := <-flowErrs:
				AssertEqual(t, err, nil)
			case <-ctx.Done():
				t.Fatal("timeout")
			}
		}
	})
}

// TestIcmpBufferFlowLimits exercises the per source (`UserLimit`) and
// aggregate (`GlobalLimit`) icmp flow caps — the enforcement side of the
// icmp memory budget item (see ICMP.md): over-limit creates evict the
// idle-most sampled flow and eager removal keeps the flow map at the cap.
func TestIcmpBufferFlowLimits(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		requireIcmpEgressVersion(t, ipVersion)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		icmpBufferSettings := DefaultIcmpBufferSettingsWithBufferSize(8)
		icmpBufferSettings.UserLimit = 2
		icmpBufferSettings.GlobalLimit = 3

		sendIcmp, buffer := newTestIcmpFlowBuffer(ctx, ipVersion, icmpBufferSettings)
		sourceIp, destinationIp := testFlowIps(ipVersion, 1)

		send := func(source TransferPath, identifier uint16) {
			t.Helper()
			packet := MessagePoolGet(32)
			parsed := &parsedIcmp{
				sourceIp:       sourceIp,
				destinationIp:  destinationIp,
				echoRequest:    true,
				identifier:     identifier,
				sequenceNumber: 1,
				ttl:            64,
				payload:        packet[:4],
			}
			if success, err := sendIcmp(source, protocol.ProvideMode_Network, parsed, -1, packet); err != nil || !success {
				MessagePoolReturn(packet)
				t.Fatalf("icmp send %d: success=%t err=%v", identifier, success, err)
			}
			// keep the lru order deterministic
			time.Sleep(10 * time.Millisecond)
		}

		sourceA := SourceId(NewId())
		// two flows fill the per source limit
		send(sourceA, 40001)
		send(sourceA, 40002)
		// the third and fourth evict the idle-most flow before their insert
		send(sourceA, 40003)
		send(sourceA, 40004)
		pollUntil(t, 5*time.Second, "per source lru eviction", func() bool {
			ids := buffer.keys()
			return !ids[40001] && ids[40003] && ids[40004]
		})

		// a second source pushes the aggregate over the global limit: the
		// idle-most flows across all sources evict, the newest flows survive
		sourceB := SourceId(NewId())
		send(sourceB, 41001)
		send(sourceB, 41002)
		pollUntil(t, 5*time.Second, "global lru eviction", func() bool {
			ids := buffer.keys()
			return len(ids) <= icmpBufferSettings.GlobalLimit && ids[41001] && ids[41002]
		})
	})
}

// TestIcmpBufferIdleReap: an idle echo flow reaps on `IdleTimeout`, so an
// unused icmp path returns to zero flows (the budget item holds no memory
// when unused)
func TestIcmpBufferIdleReap(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		requireIcmpEgressVersion(t, ipVersion)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		icmpBufferSettings := DefaultIcmpBufferSettingsWithBufferSize(8)
		icmpBufferSettings.IdleTimeout = 250 * time.Millisecond

		sendIcmp, buffer := newTestIcmpFlowBuffer(ctx, ipVersion, icmpBufferSettings)
		sourceIp, destinationIp := testFlowIps(ipVersion, 1)

		source := SourceId(NewId())
		for s := 0; s < 3; s++ {
			packet := MessagePoolGet(32)
			parsed := &parsedIcmp{
				sourceIp:       sourceIp,
				destinationIp:  destinationIp,
				echoRequest:    true,
				identifier:     uint16(42001 + s),
				sequenceNumber: 1,
				ttl:            64,
				payload:        packet[:4],
			}
			if success, err := sendIcmp(source, protocol.ProvideMode_Network, parsed, -1, packet); err != nil || !success {
				MessagePoolReturn(packet)
				t.Fatalf("icmp send %d: success=%t err=%v", s, success, err)
			}
		}

		AssertEqual(t, buffer.sequenceCount(), 3)

		pollUntil(t, 10*time.Second, "idle flows reaped", func() bool {
			return buffer.sequenceCount() == 0
		})
	})
}
