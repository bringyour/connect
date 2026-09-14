//go:build windows

package connect

import (
	"context"
	"net"
	"net/netip"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

// The Windows half of the egress resolver: NetDialer must install it, and its
// Dial must substitute an egress-reachable DNS server for whatever poisoned
// server the Go resolver read out of the system configuration (on the
// tunnel-providing machine that configuration includes the tun's own mask
// resolver), on a socket that carries the forced-interface bind.

func TestNetDialerInstallsEgressResolver(t *testing.T) {
	settings := DefaultConnectSettings()
	if got := settings.NetDialer().Resolver; got != egressBoundResolver {
		t.Fatalf("NetDialer must install the egress-bound resolver on windows, got %v", got)
	}
	if !egressBoundResolver.PreferGo {
		t.Fatal("the egress resolver must use the in-process Go resolver; GetAddrInfoW queries are issued by svchost and follow the tun route")
	}
	custom := &net.Resolver{}
	settings.Resolver = custom
	if got := settings.NetDialer().Resolver; got != custom {
		t.Fatalf("a configured resolver must still win, got %v", got)
	}
}

// startDnsResponder serves one-shot address answers on the loopback UDP of
// the given ip version and returns its port. It answers the family's record
// type (A over v4, AAAA over v6) with `answer`.
func startDnsResponder(t *testing.T, ipVersion int, answer netip.Addr) int {
	packetConn, err := net.ListenPacket(testUdpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		packetConn.Close()
	})
	go func() {
		buffer := make([]byte, 1500)
		for {
			n, remoteAddr, err := packetConn.ReadFrom(buffer)
			if err != nil {
				return
			}
			var query dnsmessage.Message
			if err := query.Unpack(buffer[:n]); err != nil || len(query.Questions) == 0 {
				continue
			}
			q := query.Questions[0]
			header := dnsmessage.ResourceHeader{
				Name:  q.Name,
				Type:  q.Type,
				Class: dnsmessage.ClassINET,
				TTL:   60,
			}
			response := dnsmessage.Message{
				Header: dnsmessage.Header{
					ID:                 query.Header.ID,
					Response:           true,
					RecursionDesired:   query.Header.RecursionDesired,
					RecursionAvailable: true,
				},
				Questions: query.Questions,
			}
			switch {
			case q.Type == dnsmessage.TypeA && answer.Is4():
				response.Answers = []dnsmessage.Resource{{Header: header, Body: &dnsmessage.AResource{A: answer.As4()}}}
			case q.Type == dnsmessage.TypeAAAA && answer.Is6():
				response.Answers = []dnsmessage.Resource{{Header: header, Body: &dnsmessage.AAAAResource{AAAA: answer.As16()}}}
			}
			packed, err := response.Pack()
			if err != nil {
				continue
			}
			packetConn.WriteTo(packed, remoteAddr)
		}
	}()
	return packetConn.LocalAddr().(*net.UDPAddr).Port
}

func loopbackInterfaceIndex(t *testing.T) uint32 {
	interfaces, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, ifc := range interfaces {
		if ifc.Flags&net.FlagLoopback != 0 && ifc.Flags&net.FlagUp != 0 {
			return uint32(ifc.Index)
		}
	}
	t.Fatal("no loopback interface")
	return 0
}

// The forced egress bind and the substituted server follow the family: the
// v4 case forces the loopback index for v4 and substitutes the v4 loopback,
// the v6 case does the same through the v6 index and IPV6_UNICAST_IF.
func TestEgressResolverDialSubstitutesAndBinds(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		answer := testLoopbackAddr(ipVersion).Next()
		if ipVersion == 4 {
			answer = netip.MustParseAddr("127.0.0.42")
		}
		port := startDnsResponder(t, ipVersion, answer)
		loIndex := loopbackInterfaceIndex(t)

		log := newRecordingLogger()
		SetDefaultLogger(log)
		t.Cleanup(func() {
			SetDefaultLogger(nil)
		})

		var index4, index6 uint32
		if ipVersion == 6 {
			index6 = loIndex
		} else {
			index4 = loIndex
		}
		SetEgressInterfaceIndex(index4, index6)
		t.Cleanup(func() {
			SetEgressInterfaceIndex(0, 0)
		})
		// the discovered-server cache stands in for the egress adapter's
		// configured resolvers
		egressDnsServerCache.Store(&egressDnsServerList{
			index4:  index4,
			index6:  index6,
			servers: []string{testLoopbackIp(ipVersion)},
			expires: time.Now().Add(time.Minute),
		})
		t.Cleanup(func() {
			egressDnsServerCache.Store(nil)
		})

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()

		// the documentation address plays the poisoned server the Go resolver
		// read from the system configuration (nothing listens); the dial must
		// go to the substituted server instead, on a socket that took the bind
		poisoned := testDocAddr(ipVersion, 1).String()
		conn, err := egressResolverDial(ctx, "udp", net.JoinHostPort(poisoned, "53"))
		// port substitution keeps the resolver's port; the responder is not on
		// :53, so exercise the wire exchange against the responder's port
		conn2, err2 := egressResolverDial(ctx, "udp", net.JoinHostPort(poisoned, strconv.Itoa(port)))
		if err != nil || err2 != nil {
			t.Fatalf("bound dial failed: %v %v", err, err2)
		}
		defer conn.Close()
		defer conn2.Close()
		if got, want := conn2.RemoteAddr().String(), testLoopbackHostPort(ipVersion, port); got != want {
			t.Fatalf("the poisoned server was not substituted: dialed %s, want %s", got, want)
		}

		// the substituted, bound socket must carry a real DNS exchange
		recordType := dnsmessage.TypeA
		if ipVersion == 6 {
			recordType = dnsmessage.TypeAAAA
		}
		query := dnsmessage.Message{
			Header: dnsmessage.Header{ID: 7, RecursionDesired: true},
			Questions: []dnsmessage.Question{
				{
					Name:  dnsmessage.MustNewName("capture-hole-test.example."),
					Type:  recordType,
					Class: dnsmessage.ClassINET,
				},
			},
		}
		packed, err := query.Pack()
		if err != nil {
			t.Fatal(err)
		}
		conn2.SetDeadline(time.Now().Add(10 * time.Second))
		if _, err := conn2.Write(packed); err != nil {
			t.Fatal(err)
		}
		buffer := make([]byte, 1500)
		n, err := conn2.Read(buffer)
		if err != nil {
			t.Fatal(err)
		}
		var response dnsmessage.Message
		if err := response.Unpack(buffer[:n]); err != nil {
			t.Fatal(err)
		}
		if len(response.Answers) != 1 {
			t.Fatalf("expected one answer, got %v", response.Answers)
		}
		switch body := response.Answers[0].Body.(type) {
		case *dnsmessage.AResource:
			if ipVersion != 4 || body.A != answer.As4() {
				t.Fatalf("unexpected answer %v", response.Answers[0])
			}
		case *dnsmessage.AAAAResource:
			if ipVersion != 6 || body.AAAA != answer.As16() {
				t.Fatalf("unexpected answer %v", response.Answers[0])
			}
		default:
			t.Fatalf("unexpected answer %v", response.Answers[0])
		}

		// and the exchange left the control-dial evidence line
		if lines := log.linesWith("tag=dns"); len(lines) == 0 {
			t.Fatal("expected a tag=dns evidence line")
		}
	})
}

func TestEgressResolverDialPassthroughUnbound(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		answer := testLoopbackAddr(ipVersion).Next()
		if ipVersion == 4 {
			answer = netip.MustParseAddr("127.0.0.43")
		}
		port := startDnsResponder(t, ipVersion, answer)
		SetEgressInterfaceIndex(0, 0)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		// unbound (no tunnel provided by this process): the requested server is
		// dialed as-is, the plain platform behavior
		conn, err := egressResolverDial(ctx, "udp", testLoopbackHostPort(ipVersion, port))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if got, want := conn.RemoteAddr().String(), testLoopbackHostPort(ipVersion, port); got != want {
			t.Fatalf("unbound dial must not substitute: %s, want %s", got, want)
		}
	})
}

func TestEgressAdapterDnsServersWalksTheRealTable(t *testing.T) {
	// exercise the GetAdaptersAddresses walk against every adapter on the
	// machine; whatever it returns must have survived the usability filter,
	// for either family
	interfaces, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	for _, ifc := range interfaces {
		servers, err := egressAdapterDnsServers(uint32(ifc.Index), uint32(ifc.Index))
		if err != nil {
			t.Fatalf("adapter walk failed for %s: %v", ifc.Name, err)
		}
		for _, server := range servers {
			addr, parseErr := netip.ParseAddr(server)
			if parseErr != nil {
				t.Fatalf("unparseable server %q survived the filter on %s", server, ifc.Name)
			}
			if addr.IsLoopback() || addr.IsUnspecified() || server == DefaultDnsUpgradeMaskAddress {
				t.Fatalf("unusable server %s survived the filter on %s", server, ifc.Name)
			}
		}
	}
}
