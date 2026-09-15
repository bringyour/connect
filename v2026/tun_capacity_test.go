package connect

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"runtime"
	"runtime/debug"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// What does a gVisor endpoint actually cost?
//
// The proxy's capacity numbers are measured against kernel sockets, but in
// production the upstream leg of every tunnel and every UDP flow is a gVisor
// endpoint living INSIDE the proxy process, and its buffers are Go heap. Those
// buffers are sized here, in TunSettings — 128KiB..1MiB per direction for udp,
// 64..256KiB default and up to 1MiB max per direction for tcp — so they plausibly
// dominate per-connection memory under load. Nobody had measured them.
//
// This measures them directly. The tun's stack runs with HandleLocal, so a dial
// to the tun's own address loops back INSIDE the stack: both endpoints are real
// gVisor endpoints and no external network is involved.
//
// The distinction that matters is idle vs loaded. gVisor's buffer sizes are
// LIMITS on what may be queued, not preallocations, so an idle endpoint is cheap
// and a backlogged one is not. Capacity has to be sized against the loaded number.
//
//	CONNECT_CAPACITY=1 go test -run TestTunEndpointCapacity -v -timeout 20m ./
//
// Never under -race.

func skipUnlessTunCapacity(t *testing.T) {
	t.Helper()
	if os.Getenv("CONNECT_CAPACITY") == "" {
		t.Skip("set CONNECT_CAPACITY=1 to run the gvisor endpoint capacity measurement")
	}
}

func heapInUse() int64 {
	runtime.GC()
	debug.FreeOSMemory()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return int64(stats.HeapInuse)
}

func kibStr(n int64) string { return fmt.Sprintf("%.1f KiB", float64(n)/1024) }

// tunPair opens a listener on the tun's own address and returns a dial func that
// loops back through the stack.
func tunTcpPair(t *testing.T, tun *Tun, ipVersion int) (dial func() (net.Conn, net.Conn, error), closeAll func()) {
	t.Helper()
	local := tunTestLocalAddress(t, tun, ipVersion)
	ln, err := tun.ListenTCP(&net.TCPAddr{IP: local.AsSlice(), Port: 0})
	if err != nil {
		t.Fatalf("listen tcp: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port

	accepted := make(chan net.Conn, 4096)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			accepted <- conn
		}
	}()

	return func() (net.Conn, net.Conn, error) {
			client, err := tun.DialContext(context.Background(), "tcp",
				net.JoinHostPort(local.String(), fmt.Sprintf("%d", port)))
			if err != nil {
				return nil, nil, err
			}
			select {
			case server := <-accepted:
				return client, server, nil
			case <-time.After(10 * time.Second):
				client.Close()
				return nil, nil, fmt.Errorf("accept timed out")
			}
		}, func() {
			ln.Close()
		}
}

// TestTunEndpointCapacityTcp measures a gVisor TCP endpoint, idle and backlogged.
func TestTunEndpointCapacityTcp(t *testing.T) {
	skipUnlessTunCapacity(t)
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testTunEndpointCapacityTcp(t, ipVersion)
	})
}

func testTunEndpointCapacityTcp(t *testing.T, ipVersion int) {
	settings := tunTestApplyIpVersion(DefaultTunSettings(), ipVersion)
	settings.DialRace = 1 // racing dials would double-count endpoints

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tun, err := CreateTun(ctx, settings)
	if err != nil {
		t.Fatalf("create tun: %v", err)
	}
	defer tun.Close()

	dial, closeAll := tunTcpPair(t, tun, ipVersion)
	defer closeAll()

	const conns = 400
	base := heapInUse()

	clients := []net.Conn{}
	servers := []net.Conn{}
	for i := 0; i < conns; i += 1 {
		client, server, err := dial()
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		clients = append(clients, client)
		servers = append(servers, server)
	}
	defer func() {
		for i := range clients {
			clients[i].Close()
			servers[i].Close()
		}
	}()

	idle := heapInUse()
	perIdle := float64(idle-base) / float64(conns)

	// now BACKLOG every connection: the server writes and the client never reads,
	// so the client's receive buffer and the server's send buffer both fill to
	// their limits. This is the memory a busy tunnel actually costs.
	payload := make([]byte, 64*1024)
	for _, server := range servers {
		server.SetWriteDeadline(time.Now().Add(2 * time.Second))
		go func(server net.Conn) {
			for i := 0; i < 64; i += 1 {
				if _, err := server.Write(payload); err != nil {
					return
				}
			}
		}(server)
	}
	time.Sleep(5 * time.Second)

	loaded := heapInUse()
	perLoaded := float64(loaded-base) / float64(conns)

	t.Logf("=== gvisor TCP endpoint (%d loopback connections = %d endpoints) ===", conns, 2*conns)
	t.Logf("tcp recv buffer: min %s default %s max %s",
		kibStr(int64(settings.TcpReceiveBuffer.Min)), kibStr(int64(settings.TcpReceiveBuffer.Default)), kibStr(int64(settings.TcpReceiveBuffer.Max)))
	t.Logf("tcp send buffer: min %s default %s max %s",
		kibStr(int64(settings.TcpSendBuffer.Min)), kibStr(int64(settings.TcpSendBuffer.Default)), kibStr(int64(settings.TcpSendBuffer.Max)))
	t.Logf("idle:       %s per connection (%s per endpoint)", kibStr(int64(perIdle)), kibStr(int64(perIdle/2)))
	t.Logf("backlogged: %s per connection (%s per endpoint)", kibStr(int64(perLoaded)), kibStr(int64(perLoaded/2)))
	t.Logf("=> a backlogged tunnel costs %.0fx an idle one", perLoaded/max(perIdle, 1))
}

// TestTunEndpointCapacityUdp measures a gVisor UDP endpoint, idle and backlogged.
// Every socks ASSOCIATE flow is one of these.
func TestTunEndpointCapacityUdp(t *testing.T) {
	skipUnlessTunCapacity(t)
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testTunEndpointCapacityUdp(t, ipVersion)
	})
}

func testTunEndpointCapacityUdp(t *testing.T, ipVersion int) {
	settings := tunTestApplyIpVersion(DefaultTunSettings(), ipVersion)
	settings.DialRace = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tun, err := CreateTun(ctx, settings)
	if err != nil {
		t.Fatalf("create tun: %v", err)
	}
	defer tun.Close()

	local := tunTestLocalAddress(t, tun, ipVersion)
	server, err := tun.ListenUDP(&net.UDPAddr{IP: local.AsSlice(), Port: 0})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer server.Close()
	serverPort := server.LocalAddr().(*net.UDPAddr).Port

	const flows = 400
	base := heapInUse()

	clients := []net.Conn{}
	for i := 0; i < flows; i += 1 {
		conn, err := tun.DialContext(ctx, "udp",
			net.JoinHostPort(local.String(), fmt.Sprintf("%d", serverPort)))
		if err != nil {
			t.Fatalf("dial udp %d: %v", i, err)
		}
		clients = append(clients, conn)
	}
	defer func() {
		for _, conn := range clients {
			conn.Close()
		}
	}()

	idle := heapInUse()
	perIdle := float64(idle-base) / float64(flows)

	// BACKLOG every flow: the server floods each client, and no client reads, so
	// each client's receive queue fills to UdpReceiveBufferByteCount.
	go func() {
		buf := make([]byte, 2048)
		for {
			_, peer, err := server.ReadFrom(buf)
			if err != nil {
				return
			}
			// flood the sender back
			payload := make([]byte, 1400)
			for i := 0; i < 200; i += 1 {
				if _, err := server.WriteTo(payload, peer); err != nil {
					return
				}
			}
		}
	}()
	for _, conn := range clients {
		conn.Write([]byte("poke"))
	}
	time.Sleep(6 * time.Second)

	loaded := heapInUse()
	perLoaded := float64(loaded-base) / float64(flows)

	t.Logf("=== gvisor UDP endpoint (%d flows) ===", flows)
	t.Logf("udp recv buffer: %s   udp send buffer: %s",
		kibStr(int64(settings.UdpReceiveBufferByteCount)), kibStr(int64(settings.UdpSendBufferByteCount)))
	t.Logf("idle:       %s per flow", kibStr(int64(perIdle)))
	t.Logf("backlogged: %s per flow", kibStr(int64(perLoaded)))
	t.Logf("=> a backlogged flow costs %.0fx an idle one", perLoaded/max(perIdle, 1))
	t.Logf("=> at socks AssociateMaxFlows=64, one client's flows cost up to %.1f MiB",
		perLoaded*64/(1<<20))
}

// TestTunEndpointCapacityUdpSmallBuffers shows what the udp buffers cost once the
// datagram size is bounded. The socks associate relay now caps datagrams at 2KiB
// and drains each flow with a dedicated reader, so a 128KiB..1MiB receive queue is
// far deeper than anything it can use — it is pure headroom for a backlog that a
// prompt reader never builds.
func TestTunEndpointCapacityUdpSmallBuffers(t *testing.T) {
	skipUnlessTunCapacity(t)
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testTunEndpointCapacityUdpSmallBuffers(t, ipVersion)
	})
}

func testTunEndpointCapacityUdpSmallBuffers(t *testing.T, ipVersion int) {
	for _, bufSize := range []int{
		128 * 1024, // today's floor
		64 * 1024,
		32 * 1024, // gvisor's own default
		16 * 1024,
	} {
		settings := DefaultTunSettings()
		settings.DialRace = 1
		settings.UdpReceiveBufferByteCount = bufSize
		settings.UdpSendBufferByteCount = bufSize

		perLoaded := measureUdpBacklog(t, settings, 200, ipVersion)
		t.Logf("udp buffer %-9s -> backlogged flow %-11s | 64 flows = %5.1f MiB | 8GiB fits %s clients",
			kibStr(int64(bufSize)), kibStr(int64(perLoaded)),
			perLoaded*64/(1<<20),
			commas(int64((8<<30)/max(perLoaded*64, 1))))
	}
}

func measureUdpBacklog(t *testing.T, settings *TunSettings, flows int, ipVersion int) float64 {
	_, loaded := measureUdp(t, settings, flows, ipVersion)
	return loaded
}

func measureUdp(t *testing.T, settings *TunSettings, flows int, ipVersion int) (idle float64, loaded float64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tun, err := CreateTun(ctx, tunTestApplyIpVersion(settings, ipVersion))
	if err != nil {
		t.Fatalf("create tun: %v", err)
	}
	defer tun.Close()

	local := tunTestLocalAddress(t, tun, ipVersion)
	server, err := tun.ListenUDP(&net.UDPAddr{IP: local.AsSlice(), Port: 0})
	if err != nil {
		t.Fatalf("listen udp: %v", err)
	}
	defer server.Close()
	serverPort := server.LocalAddr().(*net.UDPAddr).Port

	base := heapInUse()

	clients := []net.Conn{}
	for i := 0; i < flows; i += 1 {
		conn, err := tun.DialContext(ctx, "udp",
			net.JoinHostPort(local.String(), fmt.Sprintf("%d", serverPort)))
		if err != nil {
			t.Fatalf("dial udp: %v", err)
		}
		clients = append(clients, conn)
	}
	defer func() {
		for _, conn := range clients {
			conn.Close()
		}
	}()

	idle = float64(heapInUse()-base) / float64(flows)

	go func() {
		buf := make([]byte, 2048)
		for {
			_, peer, err := server.ReadFrom(buf)
			if err != nil {
				return
			}
			payload := make([]byte, 1400)
			for i := 0; i < 200; i += 1 {
				if _, err := server.WriteTo(payload, peer); err != nil {
					return
				}
			}
		}
	}()
	for _, conn := range clients {
		conn.Write([]byte("poke"))
	}
	time.Sleep(5 * time.Second)

	loaded = float64(heapInUse()-base) / float64(flows)
	return idle, loaded
}

func commas(n int64) string {
	s := fmt.Sprintf("%d", n)
	out := ""
	for i, c := range s {
		if 0 < i && (len(s)-i)%3 == 0 {
			out += ","
		}
		out += string(c)
	}
	return out
}

var _ = io.Discard

// --- the number to scale from -------------------------------------------------

// proxyTunSettings mirrors what server/proxy/proxy_device.go gives each active
// proxy client: its own gVisor stack, with buffers capped for density rather than
// for single-connection throughput.
func proxyTunSettings() *TunSettings {
	settings := DefaultTunSettings()
	settings.DialRace = 1
	// tcp keeps the full 1MiB default: capping it would cap single-connection
	// throughput (window/RTT). udp is capped, because the associate relay cannot
	// use a deep queue. This mirrors server/proxy/proxy_device.go.
	settings.UdpReceiveBufferByteCount = 128 * 1024
	settings.UdpSendBufferByteCount = 128 * 1024
	return settings
}

// tcpCeilingSettings forces the tcp window grown, which is what a real
// (non-loopback) high-BDP path produces. Loopback RTT is ~0, so auto-tuning would
// otherwise keep the window tiny and hide the real backlogged cost.
func tcpCeilingSettings() *TunSettings {
	settings := proxyTunSettings()
	maxWindow := settings.TcpReceiveBuffer.Max
	settings.TcpReceiveBuffer = TcpBufferRange{Min: 4 * 1024, Default: maxWindow, Max: maxWindow}
	settings.TcpSendBuffer = TcpBufferRange{Min: 4 * 1024, Default: maxWindow, Max: maxWindow}
	return settings
}

// TestActiveProxyCapacity is the scaling number.
//
// An "active proxy" is one ProxyDevice: its OWN gVisor stack (a Tun), plus
// whatever connections and udp flows that client is carrying. At 1-2k active
// proxies per instance that is 1-2k gVisor stacks, so the empty-stack cost is a
// first-class term and had never been measured.
//
// This measures each term separately, so capacity can be computed for any traffic
// mix rather than guessed from one blended figure.
func TestActiveProxyCapacity(t *testing.T) {
	skipUnlessTunCapacity(t)
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testActiveProxyCapacity(t, ipVersion)
	})
}

func testActiveProxyCapacity(t *testing.T, ipVersion int) {
	settings := tunTestApplyIpVersion(proxyTunSettings(), ipVersion)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- term 0: the connect Client every active proxy also carries ---
	// A ProxyDevice is a gVisor stack AND an sdk.DeviceLocal wrapping a
	// connect.Client (send/receive/forward buffers, sequences, transports). Both
	// multiply by the number of active proxies, so both belong in the baseline.
	const clients = 200
	clientBase := heapInUse()
	connectClients := []*Client{}
	for i := 0; i < clients; i += 1 {
		clientId := NewId()
		client := NewClient(ctx, clientId, nopOob{}, DefaultClientSettings())
		connectClients = append(connectClients, client)
	}
	perClient := float64(heapInUse()-clientBase) / float64(clients)
	for _, client := range connectClients {
		client.Cancel()
	}

	// --- term 1: the gVisor stack (a Tun) ---
	const stacks = 200
	base := heapInUse()
	tuns := []*Tun{}
	for i := 0; i < stacks; i += 1 {
		tun, err := CreateTun(ctx, settings)
		if err != nil {
			t.Fatalf("create tun %d: %v", i, err)
		}
		tuns = append(tuns, tun)
	}
	perStack := float64(heapInUse()-base) / float64(stacks)
	for _, tun := range tuns {
		tun.Close()
	}

	// --- term 2: a tcp tunnel, idle and backlogged ---
	// The loopback holds BOTH endpoints of a connection; production holds one (the
	// far end is in a remote kernel, not our heap). So halve to get a tunnel.
	tcpIdleConn, _ := measureTcp(t, settings, 300, ipVersion)
	tcpIdle := tcpIdleConn / 2
	_, tcpLoadedConn := measureTcp(t, tcpCeilingSettings(), 150, ipVersion)
	tcpLoaded := tcpLoadedConn / 2

	// --- term 3: a udp flow, idle and backlogged ---
	// One endpoint per flow already, so no halving.
	udpIdle, udpLoaded := measureUdp(t, settings, 200, ipVersion)
	// an idle udp endpoint costs less than the GC noise floor of this measurement,
	// so it can come out negative. Clamp rather than let noise flatter the model.
	udpIdle = math.Max(udpIdle, 0)

	perBaseline := perStack + perClient

	t.Logf("=== per active proxy ===")
	t.Logf("connect Client (idle):        %s", kibStr(int64(perClient)))
	t.Logf("gVisor stack (idle):          %s", kibStr(int64(perStack)))
	t.Logf("BASELINE per active proxy:    %s   <- multiplies by every active proxy", kibStr(int64(perBaseline)))
	t.Logf("+ per tcp tunnel, idle:       %s", kibStr(int64(tcpIdle)))
	t.Logf("+ per tcp tunnel, BACKLOGGED: %s   <- 1MiB window, grown", kibStr(int64(tcpLoaded)))
	t.Logf("+ per udp flow, idle:         %s   <- below this measurement's noise floor", kibStr(int64(udpIdle)))
	t.Logf("+ per udp flow, BACKLOGGED:   %s   <- 128KiB queue", kibStr(int64(udpLoaded)))
	t.Logf("")

	// The ACTIVE FRACTION is what decides capacity, not the connection count: an
	// idle tunnel is ~6 KiB and a backlogged one is ~688 KiB, over 100x more. The
	// working rule is that about 20% of a client's connections are active at any
	// one instant.
	const activeFraction = 0.20

	t.Logf("mixes below assume %.0f%% of connections are active at any instant", activeFraction*100)
	t.Logf("%-34s %11s %11s", "mix (tunnels / udp flows)", "per proxy", "per 8GiB")
	for _, mix := range []struct {
		name     string
		tunnels  int
		udpFlows int
	}{
		{"light (10 / 4)", 10, 4},
		{"typical (25 / 8)", 25, 8},
		{"heavy (60 / 32)", 60, 32},
		{"saturated (100 / 64 = udp cap)", 100, 64},
	} {
		busyTunnels := activeFraction * float64(mix.tunnels)
		busyFlows := activeFraction * float64(mix.udpFlows)
		perProxy := perBaseline +
			(float64(mix.tunnels)-busyTunnels)*tcpIdle + busyTunnels*tcpLoaded +
			(float64(mix.udpFlows)-busyFlows)*udpIdle + busyFlows*udpLoaded
		fit := float64(8<<30) / perProxy
		t.Logf("%-34s %11s %11s", mix.name, kibStr(int64(perProxy)), commas(int64(fit)))
	}
	t.Logf("")
	t.Logf("NOTE: these are the connect-side terms. Add the proxy's own per-tunnel cost")
	t.Logf("      (socks 25.3 KiB, http 33.6 KiB) x tunnels, from the proxy repo's LIMITS.md.")
	t.Logf("NOTE: the connect Client here is IDLE, with no transports attached and no")
	t.Logf("      active send/receive sequences. A client carrying traffic costs more.")
}

// nopOob satisfies OutOfBandControl for a Client that is only being weighed.
type nopOob struct{}

func (nopOob) SendControl(frames []*protocol.Frame, callback OobResultFunction) {}

// measureTcp returns per-connection heap, idle and backlogged.
func measureTcp(t *testing.T, settings *TunSettings, conns int, ipVersion int) (idle float64, loaded float64) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tun, err := CreateTun(ctx, tunTestApplyIpVersion(settings, ipVersion))
	if err != nil {
		t.Fatalf("create tun: %v", err)
	}
	defer tun.Close()

	dial, closeAll := tunTcpPair(t, tun, ipVersion)
	defer closeAll()

	base := heapInUse()
	clients := []net.Conn{}
	servers := []net.Conn{}
	for i := 0; i < conns; i += 1 {
		client, server, err := dial()
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		clients = append(clients, client)
		servers = append(servers, server)
	}
	defer func() {
		for i := range clients {
			clients[i].Close()
			servers[i].Close()
		}
	}()

	idle = float64(heapInUse()-base) / float64(conns)

	payload := make([]byte, 64*1024)
	for _, server := range servers {
		server.SetWriteDeadline(time.Now().Add(2 * time.Second))
		go func(server net.Conn) {
			for i := 0; i < 32; i += 1 {
				if _, err := server.Write(payload); err != nil {
					return
				}
			}
		}(server)
	}
	time.Sleep(5 * time.Second)

	loaded = float64(heapInUse()-base) / float64(conns)
	return idle, loaded
}

// TestTunTcpWindowCeiling measures the tcp cost the loopback tests CANNOT reach.
//
// Two things make the loopback figures a floor rather than a ceiling:
//
//  1. Loopback RTT is ~0, so the bandwidth-delay product is tiny and gVisor's
//     receive-buffer auto-tuning never grows the window anywhere near Max. On a
//     real 100ms path it does. Forcing Default = Max removes auto-tuning from the
//     picture and shows what a grown window actually costs.
//  2. gVisor accounts the receive queue in PAYLOAD bytes, but the memory it holds
//     is packet buffers — headers, views, refcounts. The real cost per queued byte
//     is roughly 2x the accounted limit.
//
// Capacity has to be sized against this number, not the loopback one.
func TestTunTcpWindowCeiling(t *testing.T) {
	skipUnlessTunCapacity(t)
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testTunTcpWindowCeiling(t, ipVersion)
	})
}

func testTunTcpWindowCeiling(t *testing.T, ipVersion int) {
	for _, maxWindow := range []int{128 * 1024, 1024 * 1024} {
		settings := DefaultTunSettings()
		settings.DialRace = 1
		// Default = Max: the window starts grown, so auto-tuning cannot hide the
		// ceiling behind loopback's zero RTT.
		settings.TcpReceiveBuffer = TcpBufferRange{Min: 4 * 1024, Default: maxWindow, Max: maxWindow}
		settings.TcpSendBuffer = TcpBufferRange{Min: 4 * 1024, Default: maxWindow, Max: maxWindow}

		_, perConn := measureTcp(t, settings, 150, ipVersion)
		// The loopback holds BOTH endpoints of every connection. In production the
		// far endpoint lives in a remote kernel, not our heap, so a production
		// tunnel costs about half this.
		perEndpoint := perConn / 2
		t.Logf("tcp window %-9s -> backlogged: %-11s per loopback connection | ~%s per PRODUCTION tunnel",
			kibStr(int64(maxWindow)), kibStr(int64(perConn)), kibStr(int64(perEndpoint)))
	}
}
