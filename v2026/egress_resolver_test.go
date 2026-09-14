package connect

import (
	"context"
	"net"
	"net/netip"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

// The egress-aware resolver exists to close the control-plane capture hole:
// bound sockets escaped the tun route, but the NAMES they dialed resolved
// through the OS resolver, whose wire query followed the tun default route to
// the tunnel's own (not yet working) resolver. These tests pin the portable
// half: the resolver selection, the server usability filter, and the
// control-dial evidence lines.

// A local wire responder makes address-family tests independent of host files,
// resolver order, external DNS, and whether the test host has IPv6 configured.
func newFamilyTestResolver(t *testing.T, addrs ...netip.Addr) *net.Resolver {
	t.Helper()
	packetConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		packetConn.Close()
	})

	responsePayload := func(queryPayload []byte) ([]byte, bool) {
		var query dnsmessage.Message
		if err := query.Unpack(queryPayload); err != nil || len(query.Questions) != 1 {
			return nil, false
		}
		question := query.Questions[0]
		response := dnsmessage.Message{
			Header: dnsmessage.Header{
				ID:                 query.Header.ID,
				Response:           true,
				RecursionDesired:   query.Header.RecursionDesired,
				RecursionAvailable: true,
			},
			Questions: query.Questions,
		}
		for _, addr := range addrs {
			addr = addr.Unmap()
			header := dnsmessage.ResourceHeader{
				Name:  question.Name,
				Class: dnsmessage.ClassINET,
				TTL:   60,
			}
			switch {
			case question.Type == dnsmessage.TypeA && addr.Is4():
				header.Type = dnsmessage.TypeA
				response.Answers = append(response.Answers, dnsmessage.Resource{
					Header: header,
					Body:   &dnsmessage.AResource{A: addr.As4()},
				})
			case question.Type == dnsmessage.TypeAAAA && addr.Is6():
				header.Type = dnsmessage.TypeAAAA
				response.Answers = append(response.Answers, dnsmessage.Resource{
					Header: header,
					Body:   &dnsmessage.AAAAResource{AAAA: addr.As16()},
				})
			}
		}
		payload, err := response.Pack()
		return payload, err == nil
	}

	go func() {
		buffer := make([]byte, 2048)
		for {
			count, sourceAddr, err := packetConn.ReadFrom(buffer)
			if err != nil {
				return
			}
			payload, ok := responsePayload(buffer[:count])
			if !ok {
				continue
			}
			if _, err := packetConn.WriteTo(payload, sourceAddr); err != nil {
				return
			}
		}
	}()

	dialer := &net.Dialer{}
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _ string, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, "udp4", packetConn.LocalAddr().String())
		},
	}
}

func TestEgressAwareResolverPrefersCustom(t *testing.T) {
	custom := &net.Resolver{}
	if got := egressAwareResolver(custom); got != custom {
		t.Fatalf("a configured resolver must win over the egress resolver, got %v", got)
	}
}

func TestUsableEgressDnsServer(t *testing.T) {
	mustAddr := func(s string) netip.Addr {
		addr, err := netip.ParseAddr(s)
		if err != nil {
			t.Fatal(err)
		}
		return addr
	}
	testCases := []struct {
		name   string
		addr   string
		index4 uint32
		index6 uint32
		usable bool
	}{
		{"public v4", "1.1.1.1", 16, 0, true},
		{"lan v4", "192.168.1.1", 16, 0, true},
		{"loopback stub", "127.0.0.1", 16, 0, false},
		{"unspecified", "0.0.0.0", 16, 0, false},
		{"the tunnel's own mask resolver", DefaultDnsUpgradeMaskAddress, 16, 0, false},
		{"v4 with no v4 bind", "1.1.1.1", 0, 23, false},
		{"public v6", "2620:fe::fe", 0, 23, true},
		{"v6 with no v6 bind", "2620:fe::fe", 16, 0, false},
		// v4 link-local: DHCP failure, or a virtual/host-only adapter (VMware
		// VMnet*, Hyper-V). Never a working resolver, and dialing one costs a
		// full timeout per candidate before a usable server is reached.
		{"v4 link-local", "169.254.1.1", 16, 0, false},
		{"v4 link-local, vmware-style", "169.254.83.1", 16, 0, false},
		{"site-local auto-config junk", "fec0:0:0:ffff::1", 0, 23, false},
		{"v6 loopback", "::1", 0, 23, false},
	}
	for _, tc := range testCases {
		if got := usableEgressDnsServer(mustAddr(tc.addr), tc.index4, tc.index6); got != tc.usable {
			t.Errorf("%s: usableEgressDnsServer(%s, %d, %d) = %t, want %t", tc.name, tc.addr, tc.index4, tc.index6, got, tc.usable)
		}
	}
}

func TestWrapControlDialEvidence(t *testing.T) {
	log := newRecordingLogger()
	dialCount := 0
	dial := wrapControlDial("evtag-"+t.Name(), log, true, func(ctx context.Context, network string, addr string) (net.Conn, error) {
		dialCount += 1
		c1, c2 := net.Pipe()
		t.Cleanup(func() {
			c1.Close()
			c2.Close()
		})
		return c1, nil
	})

	// with no egress bound (mobile/app builds) the wrapper is silent
	SetEgressInterfaceIndex(0, 0)
	if _, err := dial(context.Background(), "tcp", "203.0.113.7:443"); err != nil {
		t.Fatal(err)
	}
	if lines := log.linesWith("[egress]dial"); len(lines) != 0 {
		t.Fatalf("no evidence line expected while unbound, got %v", lines)
	}

	// with an egress bound (the service in Connecting/Connected) each dial
	// logs once per tag+target per interval, then count-suppresses
	SetEgressInterfaceIndex(16, 0)
	t.Cleanup(func() {
		SetEgressInterfaceIndex(0, 0)
	})
	for range 3 {
		if _, err := dial(context.Background(), "tcp", "203.0.113.7:443"); err != nil {
			t.Fatal(err)
		}
	}
	lines := log.linesWith("[egress]dial")
	if len(lines) != 1 {
		t.Fatalf("expected exactly one evidence line (repeats suppressed), got %v", lines)
	}
	line := lines[0]
	for _, want := range []string{"tag=evtag-" + t.Name(), "203.0.113.7:443", "if=4:16/6:0", "bound=yes", "local="} {
		if !strings.Contains(line, want) {
			t.Fatalf("evidence line missing %q: %s", want, line)
		}
	}
	if dialCount != 4 {
		t.Fatalf("the wrapper must not swallow dials: %d", dialCount)
	}

	// the suppressed count surfaces on the next allowed line, matching the
	// existing "(N suppressed)" pattern
	throttle := controlDialThrottle("evtag-" + t.Name() + "|203.0.113.7:443")
	throttle.lastNanos.Store(time.Now().Add(-2 * controlDialLogInterval).UnixNano())
	if _, err := dial(context.Background(), "tcp", "203.0.113.7:443"); err != nil {
		t.Fatal(err)
	}
	lines = log.linesWith("(2 suppressed)")
	if len(lines) != 1 {
		t.Fatalf("expected the suppressed count on the next line, got %v", log.linesWith("[egress]dial"))
	}
}

func TestResolveEgressUDPAddrIpLiteral(t *testing.T) {
	// an ip literal never needs a resolver, bound or not
	SetEgressInterfaceIndex(16, 0)
	t.Cleanup(func() {
		SetEgressInterfaceIndex(0, 0)
	})
	udpAddr, err := resolveEgressUDPAddr(context.Background(), "9.9.9.9:443")
	if err != nil {
		t.Fatal(err)
	}
	if udpAddr.String() != "9.9.9.9:443" {
		t.Fatalf("got %s", udpAddr)
	}
}

func TestResolveEgressUDPAddrUnboundMatchesNet(t *testing.T) {
	SetEgressInterfaceIndex(0, 0)
	udpAddr, err := resolveEgressUDPAddr(context.Background(), "localhost:53")
	if err != nil {
		t.Fatal(err)
	}
	want, err := net.ResolveUDPAddr("udp", "localhost:53")
	if err != nil {
		t.Fatal(err)
	}
	if udpAddr.Port != want.Port {
		t.Fatalf("got %s want %s", udpAddr, want)
	}
	// net.ResolveUDPAddr's own tie-break is IPv4-first (addrs.first(isIPv4),
	// GOROOT/src/net/ipsock.go); pickControlIPAddr must reproduce it exactly
	// so dropping the direct net.ResolveUDPAddr call did not quietly change
	// the default family this path dials.
	if !udpAddr.IP.Equal(want.IP) {
		t.Fatalf("got %s want %s (family tie-break must match net.ResolveUDPAddr)", udpAddr, want)
	}
}

// resolveEgressUDPAddr is the actual call site the H3/QUIC transport uses. Its
// resolver-selection helper is exercised with the same unbound state used on
// mobile and a controlled default resolver that publishes both families. A
// host's localhost entry is not a dual-stack fixture: many valid host files
// map localhost only to 127.0.0.1 and give ::1 a different alias.
func TestResolveEgressUDPAddrHonorsForcedFamily(t *testing.T) {
	ipv4 := netip.MustParseAddr("192.0.2.1")
	ipv6 := netip.MustParseAddr("2001:db8::1")
	resolver := newFamilyTestResolver(t, ipv6, ipv4)
	defer SetControlIpFamilyPolicy(IpFamilyAuto)

	SetControlIpFamilyPolicy(IpFamilyForce4)
	got4, err := resolveEgressUDPAddrWithResolvers(
		t.Context(), "dual-stack.resolver.test.:53", nil, resolver, false)
	if err != nil {
		t.Fatal(err)
	}
	if !got4.IP.Equal(net.IP(ipv4.AsSlice())) {
		t.Fatalf("force4: got %s, want %s", got4, ipv4)
	}

	SetControlIpFamilyPolicy(IpFamilyForce6)
	got6, err := resolveEgressUDPAddrWithResolvers(
		t.Context(), "dual-stack.resolver.test.:53", nil, resolver, false)
	if err != nil {
		t.Fatal(err)
	}
	if !got6.IP.Equal(net.IP(ipv6.AsSlice())) {
		t.Fatalf("force6: got %s, want %s", got6, ipv6)
	}
}

// A client-supplied resolver is a separate path from the platform egress
// resolver. It must still honor the process-wide family policy, but it must
// not inherit an interface-family constraint from an unrelated egress socket.
func TestResolveUDPAddrWithResolverHonorsForcedFamily(t *testing.T) {
	ipv4 := netip.MustParseAddr("192.0.2.1")
	ipv6 := netip.MustParseAddr("2001:db8::1")
	resolver := newFamilyTestResolver(t, ipv6, ipv4)
	defer SetControlIpFamilyPolicy(IpFamilyAuto)

	SetControlIpFamilyPolicy(IpFamilyForce4)
	got4, err := resolveUDPAddrWithResolver(
		t.Context(), "dual-stack.resolver.test.:53", resolver, false)
	if err != nil {
		t.Fatal(err)
	}
	if !got4.IP.Equal(net.IP(ipv4.AsSlice())) {
		t.Fatalf("force4 custom resolver: got %s, want %s", got4, ipv4)
	}

	SetControlIpFamilyPolicy(IpFamilyForce6)
	got6, err := resolveUDPAddrWithResolver(
		t.Context(), "dual-stack.resolver.test.:53", resolver, false)
	if err != nil {
		t.Fatal(err)
	}
	if !got6.IP.Equal(net.IP(ipv6.AsSlice())) {
		t.Fatalf("force6 custom resolver: got %s, want %s", got6, ipv6)
	}
}

// A force chooses among addresses a name publishes; it does not make a
// single-family control name unusable. Pin both fallback directions through
// the resolver-level path, not only through pickControlIPAddr's slice test.
func TestResolveUDPAddrWithResolverFallsBackWhenForcedFamilyIsUnavailable(t *testing.T) {
	testCases := []struct {
		policy    IpFamilyPolicy
		available netip.Addr
	}{
		{policy: IpFamilyForce6, available: netip.MustParseAddr("192.0.2.1")},
		{policy: IpFamilyForce4, available: netip.MustParseAddr("2001:db8::1")},
	}
	defer SetControlIpFamilyPolicy(IpFamilyAuto)
	for _, test := range testCases {
		resolver := newFamilyTestResolver(t, test.available)
		SetControlIpFamilyPolicy(test.policy)
		got, err := resolveUDPAddrWithResolver(
			t.Context(), "single-stack.resolver.test.:53", resolver, false)
		if err != nil {
			t.Fatalf("policy %d with %s: %v", test.policy, test.available, err)
		}
		if !got.IP.Equal(net.IP(test.available.AsSlice())) {
			t.Errorf(
				"policy %d with only %s available: got %s",
				test.policy,
				test.available,
				got,
			)
		}
	}
}

// A successful DNS response with no usable A or AAAA records is not a zero IP
// destination. The resolver path must return its no-address error instead.
func TestResolveUDPAddrWithResolverRejectsEmptyAnswer(t *testing.T) {
	resolver := newFamilyTestResolver(t)
	if _, err := resolveUDPAddrWithResolver(
		t.Context(), "empty.resolver.test.:53", resolver, false); err == nil {
		t.Fatal("an empty DNS answer resolved successfully")
	}
}

// A literal has no resolver-side family choice. An opposite force must leave
// it intact, and neither the default nor a custom resolver may be consulted.
func TestResolveUDPAddrWithResolverLeavesIPLiteralFamilyUnchanged(t *testing.T) {
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(context.Context, string, string) (net.Conn, error) {
			return nil, context.Canceled
		},
	}
	testCases := []struct {
		policy IpFamilyPolicy
		addr   string
		want   string
	}{
		{policy: IpFamilyForce6, addr: "192.0.2.7:443", want: "192.0.2.7:443"},
		{policy: IpFamilyForce4, addr: "[2001:db8::7]:443", want: "[2001:db8::7]:443"},
		{policy: IpFamilyForce4, addr: "[fe80::1%7]:53", want: "[fe80::1%7]:53"},
	}
	defer SetControlIpFamilyPolicy(IpFamilyAuto)
	for _, test := range testCases {
		SetControlIpFamilyPolicy(test.policy)
		got, err := resolveUDPAddrWithResolver(t.Context(), test.addr, resolver, false)
		if err != nil {
			t.Errorf("policy %d resolving %s: %v", test.policy, test.addr, err)
			continue
		}
		if got.String() != test.want {
			t.Errorf("policy %d resolving %s: got %s, want %s", test.policy, test.addr, got, test.want)
		}
	}
}

// Invalid address syntax and numeric ports outside UDP's range fail before a
// lookup. In particular, strconv.Atoi alone accepts values net cannot dial.
func TestResolveUDPAddrWithResolverRejectsMalformedAddress(t *testing.T) {
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(context.Context, string, string) (net.Conn, error) {
			return nil, context.Canceled
		},
	}
	addrs := []string{
		"",
		":53",
		"missing-port",
		"resolver.test.:not-a-port",
		"resolver.test.:-1",
		"resolver.test.:65536",
		"2001:db8::1:53",
	}
	for _, addr := range addrs {
		if _, err := resolveUDPAddrWithResolver(t.Context(), addr, resolver, false); err == nil {
			t.Errorf("resolve %q succeeded, want an address error", addr)
		}
	}
}
