package connect

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"syscall"
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip/header"
)

// tunTestSettings are the tun settings a dual-stack test runs under: the
// defaults, adjusted for the ip version by tunTestApplyIpVersion.
func tunTestSettings(ipVersion int) *TunSettings {
	return tunTestApplyIpVersion(DefaultTunSettings(), ipVersion)
}

// tunTestApplyIpVersion raises the link mtu to the IPv6 minimum for the v6
// case so the stack carries v6 whatever DefaultMtu is (see the tun.go file
// comment), and leaves the v4 case exactly as configured.
func tunTestApplyIpVersion(settings *TunSettings, ipVersion int) *TunSettings {
	if ipVersion == 6 && settings.Mtu < tunIpv6MinimumMtu {
		settings.Mtu = tunIpv6MinimumMtu
	}
	return settings
}

// The design (IPV6.md C1) requires the DEFAULT tun to carry IPv6, which gVisor
// only does on a link at least IPv6MinimumMTU wide. The link mtu is
// DefaultTunnelMtu; the packet-size contract DefaultMtu stays below it so a
// full return packet keeps fitting one H3 DATAGRAM. Both live in ip.go.
func TestDefaultMtuCarriesIpv6(t *testing.T) {
	if DefaultTunnelMtu < tunIpv6MinimumMtu {
		t.Fatalf("DefaultTunnelMtu (ip.go) = %d is below the IPv6 minimum mtu %d: a tun at the default mtu is IPv4-only (IPV6.md C1)", DefaultTunnelMtu, tunIpv6MinimumMtu)
	}
	if DefaultTunnelMtu < DefaultMtu {
		t.Fatalf("DefaultTunnelMtu = %d is below the packet-size contract DefaultMtu = %d", DefaultTunnelMtu, DefaultMtu)
	}
	if DefaultMtu != 1100 {
		t.Fatalf("DefaultMtu = %d, want the 1100-byte H3 single-DATAGRAM contract", DefaultMtu)
	}
}

// tunTestLocalAddress is the tun's local address of the given family.
func tunTestLocalAddress(t *testing.T, tun *Tun, ipVersion int) netip.Addr {
	t.Helper()
	for _, addr := range tun.LocalAddresses() {
		if (ipVersion == 4) == addr.Is4() {
			return addr
		}
	}
	t.Fatalf("tun has no v%d local address: %v", ipVersion, tun.LocalAddresses())
	return netip.Addr{}
}

// bridgeTunFilter forwards packets from src to dst like bridgeTun, dropping
// those keep rejects. A dropped family is a black hole: the sender's SYNs
// retransmit into nothing, which is the shape a dead path has on the wire.
func bridgeTunFilter(ctx context.Context, dst *Tun, src *Tun, keep func(packet []byte) bool) {
	go func() {
		for {
			packet, err := src.Read()
			if err != nil {
				return
			}
			if keep(packet) {
				_, _ = dst.Write(packet)
			}
			MessagePoolReturn(packet)
			select {
			case <-ctx.Done():
				return
			default:
			}
		}
	}()
}

func packetIpVersion(packet []byte) int {
	if len(packet) == 0 {
		return 0
	}
	return int(packet[0] >> 4)
}

func TestTunIpv6MinimumMtu(t *testing.T) {
	if tunIpv6MinimumMtu != 1280 {
		t.Fatalf("tunIpv6MinimumMtu = %d, want the RFC 8200 minimum 1280", tunIpv6MinimumMtu)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultTunSettings()
	settings.Mtu = tunIpv6MinimumMtu
	tun, err := CreateTun(ctx, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer tun.Close()
	if !tun.Ipv6Enabled() {
		t.Fatal("a tun at the IPv6 minimum mtu must carry IPv6")
	}
	addrs := tun.LocalAddresses()
	if len(addrs) != 2 || !addrs[0].Is4() || !addrs[1].Is6() {
		t.Fatalf("local addresses = %v, want [v4 v6] with v4 first", addrs)
	}
	if !LocalIpv6Prefix.Contains(addrs[1]) {
		t.Fatalf("v6 address %s is outside LocalIpv6Prefix %s", addrs[1], LocalIpv6Prefix)
	}

	// the default tun follows the same rule (TestDefaultMtuCarriesIpv6 pins
	// which side of the minimum DefaultTunnelMtu must sit on)
	defaultTun, err := CreateTunWithDefaults(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer defaultTun.Close()
	if want := tunIpv6MinimumMtu <= DefaultTunnelMtu; defaultTun.Ipv6Enabled() != want {
		t.Fatalf("default tun Ipv6Enabled = %t, want %t for DefaultTunnelMtu %d", defaultTun.Ipv6Enabled(), want, DefaultTunnelMtu)
	}
}

// Below the IPv6 minimum mtu the tun is exactly what it was: one v4 address,
// v6 dials and v6 writes refused with EAFNOSUPPORT, v4 unaffected.
func TestTunIpv6DisabledBelowMinimumMtu(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	settings := DefaultTunSettings()
	settings.Mtu = tunIpv6MinimumMtu - 1
	tun, err := CreateTun(ctx, settings)
	if err != nil {
		t.Fatal(err)
	}
	defer tun.Close()

	if tun.Ipv6Enabled() {
		t.Fatal("a tun below the IPv6 minimum mtu must not carry IPv6")
	}
	if addrs := tun.LocalAddresses(); len(addrs) != 1 || !addrs[0].Is4() {
		t.Fatalf("local addresses = %v, want one IPv4 address", addrs)
	}
	for _, network := range []string{"tcp6", "tcp", "udp6", "udp"} {
		conn, dialErr := tun.dialContext(ctx, network, "[2001:db8::25]:465")
		if conn != nil {
			conn.Close()
			t.Fatalf("%s IPv6 dial unexpectedly returned a connection", network)
		}
		if dialErr != syscall.EAFNOSUPPORT {
			t.Fatalf("%s IPv6 dial error = %v, want %v", network, dialErr, syscall.EAFNOSUPPORT)
		}
	}
	released := make(chan struct{})
	if n, writeErr := tun.write([]byte{0x60}, func() { close(released) }); writeErr != syscall.EAFNOSUPPORT || n != 0 {
		t.Fatalf("v6 write = %d, %v; want 0, %v", n, writeErr, syscall.EAFNOSUPPORT)
	}
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("rejected v6 packet retained its creator PacketBuffer reference")
	}
}

// A v6 packet injected into a dual-stack tun is accepted and its creator
// reference released, exactly like a v4 one.
func TestTunWriteAcceptsIpv6WhenEnabled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tun, err := CreateTun(ctx, tunTestSettings(6))
	if err != nil {
		t.Fatal(err)
	}
	defer tun.Close()

	released := make(chan struct{})
	// a deliberately truncated v6 header: gVisor rejects it after taking
	// its references, and ownership is independent of protocol validity
	if n, writeErr := tun.write([]byte{0x60}, func() { close(released) }); writeErr != nil || n != 1 {
		t.Fatalf("v6 write = %d, %v; want 1, nil", n, writeErr)
	}
	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("injected v6 packet retained its creator PacketBuffer reference")
	}
}

func TestTunTcpInboundFlowParsesBothFamilies(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		path := &IpPath{
			Version:         ipVersion,
			Protocol:        IpProtocolTcp,
			SourceIp:        net.ParseIP(testLoopbackIp(ipVersion)),
			SourcePort:      4321,
			DestinationIp:   net.ParseIP(testLoopbackIp(ipVersion)),
			DestinationPort: 443,
		}
		packet := ipOosTcpPacketSequence(path, tcpFlagSyn, 1, nil)
		endpointId, shardIndex, ok := tcpInboundFlow(packet)
		if !ok {
			t.Fatalf("v%d tcp packet was not recognized as an inbound flow", ipVersion)
		}
		if endpointId.LocalPort != 443 || endpointId.RemotePort != 4321 {
			t.Fatalf("endpoint ports = %d/%d, want 443/4321", endpointId.LocalPort, endpointId.RemotePort)
		}
		wantLen := header.IPv4AddressSize
		if ipVersion == 6 {
			wantLen = header.IPv6AddressSize
		}
		if endpointId.LocalAddress.Len() != wantLen {
			t.Fatalf("endpoint address width = %d, want %d", endpointId.LocalAddress.Len(), wantLen)
		}
		if shardIndex < 0 || tunTcpInboundShardCount <= shardIndex {
			t.Fatalf("shard = %d out of range", shardIndex)
		}
		if got := tcpInboundNetworkProtocol(endpointId); (ipVersion == 4) != (got == header.IPv4ProtocolNumber) {
			t.Fatalf("network protocol for v%d endpoint = %d", ipVersion, got)
		}
	})
}

// A datagram flow crosses two bridged tuns in either family.
func TestTunUDPBridge(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		left, err := CreateTun(ctx, tunTestSettings(ipVersion))
		if err != nil {
			t.Fatal(err)
		}
		defer left.Close()
		right, err := CreateTun(ctx, tunTestSettings(ipVersion))
		if err != nil {
			t.Fatal(err)
		}
		defer right.Close()
		bridgeTun(ctx, left, right)
		bridgeTun(ctx, right, left)

		rightIP := net.IP(tunTestLocalAddress(t, right, ipVersion).AsSlice())
		server, err := right.ListenUDP(&net.UDPAddr{IP: rightIP, Port: 0})
		if err != nil {
			t.Fatal(err)
		}
		defer server.Close()

		serverErr := make(chan error, 1)
		go func() {
			buffer := make([]byte, 64)
			server.SetDeadline(time.Now().Add(3 * time.Second))
			n, from, err := server.ReadFrom(buffer)
			if err != nil {
				serverErr <- err
				return
			}
			if string(buffer[:n]) != "ping" {
				serverErr <- io.ErrUnexpectedEOF
				return
			}
			_, err = server.WriteTo([]byte("pong"), from)
			serverErr <- err
		}()

		conn, err := left.DialContext(ctx, "udp", server.LocalAddr().String())
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		conn.SetDeadline(time.Now().Add(3 * time.Second))
		if _, err := conn.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		buffer := make([]byte, 64)
		n, err := conn.Read(buffer)
		if err != nil {
			t.Fatal(err)
		}
		if string(buffer[:n]) != "pong" {
			t.Fatalf("got %q, want pong", buffer[:n])
		}
		if err := <-serverErr; err != nil {
			t.Fatal(err)
		}
		remote, _ := netip.ParseAddrPort(conn.RemoteAddr().String())
		if (ipVersion == 4) != remote.Addr().Unmap().Is4() {
			t.Fatalf("udp remote = %s, want v%d", conn.RemoteAddr(), ipVersion)
		}
	})
}

// The ::/0 route sends a packet for any global v6 destination out the tun,
// the same way 0.0.0.0/0 does for v4: a dial to an address nobody holds
// produces a SYN on Read.
func TestTunDefaultRouteEmitsPackets(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		tun, err := CreateTun(ctx, tunTestSettings(ipVersion))
		if err != nil {
			t.Fatal(err)
		}
		defer tun.Close()

		target := "192.0.2.10:443"
		if ipVersion == 6 {
			target = "[2001:db8::10]:443"
		}
		dialCtx, dialCancel := context.WithTimeout(ctx, 2*time.Second)
		defer dialCancel()
		go func() {
			conn, err := tun.DialContext(dialCtx, "tcp", target)
			if err == nil {
				conn.Close()
			}
		}()
		packet, err := tun.Read()
		if err != nil {
			t.Fatal(err)
		}
		defer MessagePoolReturn(packet)
		if got := packetIpVersion(packet); got != ipVersion {
			t.Fatalf("emitted packet version = %d, want %d", got, ipVersion)
		}
		ipPath, err := ParseIpPath(packet)
		if err != nil {
			t.Fatal(err)
		}
		if ipPath.DestinationPort != 443 || ipPath.Protocol != IpProtocolTcp {
			t.Fatalf("emitted %v, want a tcp syn to port 443", ipPath)
		}
		if ipPath.DestinationIp.String() != netip.MustParseAddrPort(target).Addr().String() {
			t.Fatalf("emitted destination %s, want %s", ipPath.DestinationIp, target)
		}
	})
}

// Literals and family-specific networks: a v6 literal, tcp6, and a mismatched
// family are all decided before any packet moves.
func TestTunDialContextLiteralsAndNetworks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	left, err := CreateTun(ctx, tunTestSettings(6))
	if err != nil {
		t.Fatal(err)
	}
	defer left.Close()
	right, err := CreateTun(ctx, tunTestSettings(6))
	if err != nil {
		t.Fatal(err)
	}
	defer right.Close()
	bridgeTun(ctx, left, right)
	bridgeTun(ctx, right, left)

	accept := func(t *testing.T, ipVersion int) *net.TCPAddr {
		ln, err := right.ListenTCP(&net.TCPAddr{IP: net.IP(tunTestLocalAddress(t, right, ipVersion).AsSlice()), Port: 0})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { ln.Close() })
		go func() {
			for {
				conn, err := ln.Accept()
				if err != nil {
					return
				}
				conn.Close()
			}
		}()
		return ln.Addr().(*net.TCPAddr)
	}
	v6Addr := accept(t, 6)
	v4Addr := accept(t, 4)

	for _, network := range []string{"tcp", "tcp6"} {
		conn, err := left.dialContext(ctx, network, v6Addr.String())
		if err != nil {
			t.Fatalf("%s to a v6 literal: %v", network, err)
		}
		conn.Close()
	}
	for _, network := range []string{"tcp", "tcp4"} {
		conn, err := left.dialContext(ctx, network, v4Addr.String())
		if err != nil {
			t.Fatalf("%s to a v4 literal: %v", network, err)
		}
		conn.Close()
	}
	if conn, err := left.dialContext(ctx, "tcp4", v6Addr.String()); err != syscall.EAFNOSUPPORT {
		if conn != nil {
			conn.Close()
		}
		t.Fatalf("tcp4 to a v6 literal = %v, want %v", err, syscall.EAFNOSUPPORT)
	}
	if conn, err := left.dialContext(ctx, "tcp6", v4Addr.String()); err != syscall.EAFNOSUPPORT {
		if conn != nil {
			conn.Close()
		}
		t.Fatalf("tcp6 to a v4 literal = %v, want %v", err, syscall.EAFNOSUPPORT)
	}
	// a mapped literal is a v4 target
	mapped := net.JoinHostPort("::ffff:"+v4Addr.IP.String(), itoa(v4Addr.Port))
	conn, err := left.dialContext(ctx, "tcp", mapped)
	if err != nil {
		t.Fatalf("mapped v4 literal: %v", err)
	}
	conn.Close()
}

// A name resolves through the tun's DoH cache to both families and the
// stream dial races them: with one family black-holed by the bridge, the
// other wins, and a dead v6 costs exactly the fallback delay before v4 is
// launched.
func TestTunDialContextResolvesBothFamiliesAndRaces(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, deadVersion int) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		right, err := CreateTun(ctx, tunTestSettings(6))
		if err != nil {
			t.Fatal(err)
		}
		defer right.Close()
		right4 := tunTestLocalAddress(t, right, 4)
		right6 := tunTestLocalAddress(t, right, 6)

		// the DoH server answers every name with the right tun's addresses;
		// it is a host-dialed local DoH endpoint, so it needs no tunnel
		dohServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			writeDohWire(w, r, []netip.Addr{right4, right6}, 60, false)
		}))
		defer dohServer.Close()
		left, err := CreateTunWithResolver(ctx, tunTestSettings(6), &DnsResolverSettings{
			EnableLocalDoh:    true,
			LocalDohUrlsIpv4:  []string{dohServer.URL},
			EnableRemoteDoh:   false,
			EnableLocalDns:    false,
			RemoteDohUrlsIpv4: nil,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer left.Close()

		// the bridge black-holes one family in both directions
		keep := func(packet []byte) bool { return packetIpVersion(packet) != deadVersion }
		bridgeTunFilter(ctx, right, left, keep)
		bridgeTunFilter(ctx, left, right, keep)

		listeners := map[int]*net.TCPAddr{}
		for _, ipVersion := range []int{4, 6} {
			ln, err := right.ListenTCP(&net.TCPAddr{IP: net.IP(tunTestLocalAddress(t, right, ipVersion).AsSlice()), Port: 0})
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			go func() {
				for {
					conn, err := ln.Accept()
					if err != nil {
						return
					}
					conn.Close()
				}
			}()
			listeners[ipVersion] = ln.Addr().(*net.TCPAddr)
		}
		// both listeners must share a port for one name:port to reach either
		port := listeners[4].Port
		if listeners[6].Port != port {
			// rebind the v6 listener on the v4 port
			ln, err := right.ListenTCP(&net.TCPAddr{IP: net.IP(right6.AsSlice()), Port: port})
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			go func() {
				for {
					conn, err := ln.Accept()
					if err != nil {
						return
					}
					conn.Close()
				}
			}()
		}

		started := time.Now()
		conn, err := left.dialContext(ctx, "tcp", net.JoinHostPort("peer.tun.test", itoa(port)))
		if err != nil {
			t.Fatalf("dial with v%d black-holed: %v", deadVersion, err)
		}
		defer conn.Close()
		elapsed := time.Since(started)
		remote, _ := netip.ParseAddrPort(conn.RemoteAddr().String())
		wantVersion := 4
		if deadVersion == 4 {
			wantVersion = 6
		}
		if (wantVersion == 4) != remote.Addr().Unmap().Is4() {
			t.Fatalf("connected to %s, want the live v%d listener", conn.RemoteAddr(), wantVersion)
		}
		if deadVersion == 6 && elapsed < DefaultDialFallbackDelay {
			t.Fatalf("v4 won after %s, before the v6 fallback delay %s: v6 was not tried first", elapsed, DefaultDialFallbackDelay)
		}
		if elapsed > 5*time.Second {
			t.Fatalf("race took %s", elapsed)
		}
	})
}

// A datagram dial cannot race: it takes the first v4 address when there is
// one, else the first address.
func TestUdpDialAddrPrefersV4(t *testing.T) {
	v4 := netip.MustParseAddr("192.0.2.1")
	v6 := netip.MustParseAddr("2001:db8::1")
	if got := udpDialAddr([]netip.Addr{v6, v4}); got != v4 {
		t.Fatalf("udpDialAddr = %s, want %s", got, v4)
	}
	if got := udpDialAddr([]netip.Addr{v6}); got != v6 {
		t.Fatalf("udpDialAddr = %s, want %s", got, v6)
	}
}

func TestLocalIpv6AddressAllocatorAndRandomLocalIpv6(t *testing.T) {
	first, ok := TakeLocalIpv6Address()
	if !ok {
		t.Fatal("no v6 address")
	}
	second, ok := TakeLocalIpv6Address()
	if !ok {
		t.Fatal("no second v6 address")
	}
	if first == second {
		t.Fatalf("allocator handed out %s twice", first)
	}
	for _, addr := range []netip.Addr{first, second} {
		if !localIpv6AllocatorPrefix.Contains(addr) || addr == LocalIpv6Prefix.Masked().Addr() {
			t.Fatalf("allocated %s, want a host inside %s", addr, localIpv6AllocatorPrefix)
		}
	}
	ReturnLocalIpv6Address(second)
	again, ok := TakeLocalIpv6Address()
	if !ok || again != second {
		t.Fatalf("returned address was not reused: got %s, want %s", again, second)
	}
	ReturnLocalIpv6Address(first)
	ReturnLocalIpv6Address(again)

	seen := map[netip.Addr]bool{}
	for range 16 {
		addr := RandomLocalIpv6()
		if !LocalIpv6Prefix.Contains(addr) || localIpv6AllocatorPrefix.Contains(addr) {
			t.Fatalf("RandomLocalIpv6 = %s, want inside %s and outside the tun pool %s", addr, LocalIpv6Prefix, localIpv6AllocatorPrefix)
		}
		seen[addr] = true
	}
	if len(seen) < 2 {
		t.Fatal("RandomLocalIpv6 is not random")
	}
}

func TestTunDialContextRejectsUnsupportedNetwork(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tun, err := CreateTun(ctx, tunTestSettings(6))
	if err != nil {
		t.Fatal(err)
	}
	defer tun.Close()
	if _, err := tun.dialContext(ctx, "unix", "/tmp/x"); err == nil || errors.Is(err, syscall.EAFNOSUPPORT) {
		t.Fatalf("unix dial = %v, want an unsupported-network error", err)
	}
}
