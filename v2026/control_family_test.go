package connect

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"
)

func TestControlDialNetworkForce(t *testing.T) {
	tests := []struct {
		name    string
		policy  IpFamilyPolicy
		network string
		want    string
		wantErr bool
	}{
		{"auto leaves tcp alone", IpFamilyAuto, "tcp", "tcp", false},
		{"auto leaves udp alone", IpFamilyAuto, "udp", "udp", false},
		{"force4 narrows tcp", IpFamilyForce4, "tcp", "tcp4", false},
		{"force6 narrows tcp", IpFamilyForce6, "tcp", "tcp6", false},
		{"force4 narrows udp", IpFamilyForce4, "udp", "udp4", false},
		{"force6 narrows udp", IpFamilyForce6, "udp", "udp6", false},
		{"force4 passes matching explicit", IpFamilyForce4, "tcp4", "tcp4", false},
		{"force4 rejects conflicting explicit", IpFamilyForce4, "tcp6", "", true},
		{"force6 rejects conflicting explicit", IpFamilyForce6, "udp4", "", true},
		{"auto passes explicit through", IpFamilyAuto, "tcp6", "tcp6", false},
		{"unknown network is untouched", IpFamilyForce4, "unix", "unix", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			SetControlIpFamilyPolicy(test.policy)
			defer SetControlIpFamilyPolicy(IpFamilyAuto)
			got, err := controlDialNetwork(test.network, "api.example:443")
			if test.wantErr {
				if err == nil {
					t.Fatalf("expected an error for %s under %d", test.network, test.policy)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("got %q, want %q", got, test.want)
			}
		})
	}
}

// A dial whose target is ALREADY AN IP LITERAL has no family choice left in
// it, so narrowing it can never change which family is dialed -- it can only
// turn a working dial into an instant "no suitable address found". Measured:
// `dial tcp6 1.1.1.1:443` fails immediately.
//
// This is the whole fallback layer, not a corner case. The extender dialers
// dial extenderConfig.Ip (net_extender.go) and the remote plain-DNS resolver
// dials a configured resolver address (net_http_doh.go), both IP literals,
// both through ConnectSettings.DialContext. A demotion learned on the api path
// used to take every one of them down with it -- and because they fail at
// CONNECT, none of them recorded anything that could undo the demotion.
func TestControlDialNetworkNeverNarrowsAnIPLiteral(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()

	tests := []struct {
		name    string
		policy  IpFamilyPolicy
		demote  int
		network string
		addr    string
	}{
		{"force6 leaves an ipv4 extender literal alone", IpFamilyForce6, 0, "tcp", "192.0.2.7:443"},
		{"force4 leaves an ipv6 extender literal alone", IpFamilyForce4, 0, "tcp", "[2001:db8::7]:443"},
		{"a demotion of 6 leaves an ipv4 literal alone", IpFamilyAuto, 6, "tcp", "1.1.1.1:443"},
		{"a demotion of 4 leaves an ipv6 literal alone", IpFamilyAuto, 4, "tcp", "[2606:4700:4700::1111]:53"},
		{"a demotion leaves a bare literal alone", IpFamilyAuto, 6, "udp", "1.1.1.1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			controlFamilyClear()
			defer controlFamilyClear()
			SetControlIpFamilyPolicy(test.policy)
			defer SetControlIpFamilyPolicy(IpFamilyAuto)
			if test.demote != 0 {
				SetControlIpFamilyPolicy(IpFamilyAuto)
				if !controlFamilyDemote(test.demote) {
					t.Fatal("expected the demotion to take")
				}
				SetControlIpFamilyPolicy(test.policy)
			}

			got, err := controlDialNetwork(test.network, test.addr)
			if err != nil {
				t.Fatalf("%v -- a literal dial must never be refused", err)
			}
			if got != test.network {
				t.Fatalf(
					"got %q for %s, want %q unchanged -- narrowing a literal can only break it",
					got, test.addr, test.network)
			}
		})
	}
}

func TestSetControlIpFamilyPolicyClampsUnknown(t *testing.T) {
	defer SetControlIpFamilyPolicy(IpFamilyAuto)
	SetControlIpFamilyPolicy(IpFamilyPolicy(99))
	if got := ControlIpFamilyPolicy(); got != IpFamilyAuto {
		t.Fatalf("got %d, want IpFamilyAuto", got)
	}
	SetControlIpFamilyPolicy(IpFamilyPolicy(-3))
	if got := ControlIpFamilyPolicy(); got != IpFamilyAuto {
		t.Fatalf("got %d, want IpFamilyAuto", got)
	}
}

// Only a post-connect TIMEOUT proves a path is blackholed. Everything else is
// a server or configuration fault, and demoting a family for one would steer
// every user off a healthy path.
func TestIsPathTimeoutIsNarrow(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"context deadline", context.DeadlineExceeded, true},
		{"os deadline", os.ErrDeadlineExceeded, true},
		{"wrapped deadline", fmt.Errorf("tls: %w", context.DeadlineExceeded), true},
		{"net timeout", &net.OpError{Err: &timeoutError{}}, true},
		{"certificate", &tls.CertificateVerificationError{}, false},
		{"connection refused", &net.OpError{Err: errors.New("connection refused")}, false},
		{"reset", errors.New("read: connection reset by peer"), false},
		{"alpn", errors.New("tls: no application protocol"), false},
		{"nil", nil, false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := isPathTimeout(test.err); got != test.want {
				t.Fatalf("isPathTimeout(%v) = %v, want %v", test.err, got, test.want)
			}
		})
	}
}

type timeoutError struct{}

func (self *timeoutError) Error() string   { return "i/o timeout" }
func (self *timeoutError) Timeout() bool   { return true }
func (self *timeoutError) Temporary() bool { return true }

// A demotion must never take the user offline. With no IPv4 on the device,
// demoting IPv6 is refused -- and with no IPv6, demoting IPv4 is refused.
//
// BOTH directions, and each with a probe that answers true only for the family
// being demoted. That is what pins `other`: with the guard's
// `other := 4; if family == 4 { other = 6 }` mutated so `other` is always 4,
// the demote(4) row probes 4, gets true, and the demotion is wrongly accepted.
// A single-direction test, or a row whose probe answers true for everything,
// cannot tell the mutant from the original.
func TestControlFamilyDemoteRefusedWhenOtherFamilyUnusable(t *testing.T) {
	tests := []struct {
		name     string
		usable   int
		demote   int
		wantNoop string
	}{
		{"no ipv4, demoting ipv6 refused", 6, 6, "tcp"},
		{"no ipv6, demoting ipv4 refused", 4, 4, "tcp"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restore := swapControlFamilyProbe(func(family int) bool {
				return family == test.usable
			})
			defer restore()
			controlFamilyClear()
			defer controlFamilyClear()

			if controlFamilyDemote(test.demote) {
				t.Fatalf(
					"demoted ipv%d with no ipv%d available -- the only family "+
						"left is the one being demoted",
					test.demote, 10-test.demote)
			}
			network, err := controlDialNetwork("tcp", "api.example:443")
			if err != nil {
				t.Fatal(err)
			}
			if network != test.wantNoop {
				t.Fatalf("got %q, want %s -- a refused demotion must not narrow", network, test.wantNoop)
			}
		})
	}
}

// The IPv6-only guard is only as good as the probe behind it, and the probe
// had no direct test at all: all sixteen call sites inject a
// fake, so the "IPv6-only guard" test proved the BRANCH and never the probe.
//
// The device this pins is the one the guard exists for: an IPv6-only handset
// (ios has no CLAT; NAT64/DNS64 is the App Store-required configuration) with
// this product's tunnel up. The tun carries RandomLocalIpv4's 10.a.b.h, which
// is IsGlobalUnicast, so a probe that accepts any global-unicast address
// answers "IPv4 is available" on a device that has no IPv4 whatsoever -- and
// the demotion of IPv6 that follows takes the control plane offline for five
// minutes, doubling to six hours.
func TestProbeFamilySupportIgnoresOurOwnTunnel(t *testing.T) {
	loopback := controlFamilyInterface{
		name:  "lo0",
		flags: net.FlagUp | net.FlagLoopback,
		addrs: []net.Addr{ipNet("127.0.0.1/8"), ipNet("::1/128")},
	}
	// the ios/darwin shape: PacketTunnelProvider installs RandomLocalIpv4
	utun := controlFamilyInterface{
		name:  "utun4",
		flags: net.FlagUp | net.FlagPointToPoint,
		addrs: []net.Addr{ipNet("10.7.3.42/32")},
	}
	// the android shape, including escape mode's 192.0.2.1
	androidTun := controlFamilyInterface{
		name:  "tun0",
		flags: net.FlagUp | net.FlagPointToPoint,
		addrs: []net.Addr{ipNet("10.0.0.19/24")},
	}
	androidEscapeTun := controlFamilyInterface{
		name:  "tun0",
		flags: net.FlagUp | net.FlagPointToPoint,
		addrs: []net.Addr{ipNet("192.0.2.1/24")},
	}
	cellularV6Only := controlFamilyInterface{
		name:  "pdp_ip0",
		flags: net.FlagUp,
		addrs: []net.Addr{ipNet("2600:1700:1234:5678::1/64"), ipNet("fe80::1/64")},
	}
	// an ordinary home-lan lease. RFC1918 by range, exactly like the tun's
	// address, and it MUST still count: rejecting private ranges outright
	// would answer "no IPv4" for most of the userbase.
	wifi := controlFamilyInterface{
		name:  "en0",
		flags: net.FlagUp,
		addrs: []net.Addr{ipNet("192.168.1.20/24")},
	}
	downEthernet := controlFamilyInterface{
		name:  "en1",
		flags: 0,
		addrs: []net.Addr{ipNet("192.168.5.7/24")},
	}

	tests := []struct {
		name   string
		ifaces []controlFamilyInterface
		want4  bool
		want6  bool
	}{
		{
			"ipv6-only iphone with our tunnel up",
			[]controlFamilyInterface{loopback, cellularV6Only, utun},
			false, true,
		},
		{
			"ipv6-only android with our tunnel up",
			[]controlFamilyInterface{loopback, cellularV6Only, androidTun},
			false, true,
		},
		{
			"ipv6-only android in escape mode",
			[]controlFamilyInterface{loopback, cellularV6Only, androidEscapeTun},
			false, true,
		},
		{
			"dual-stack wifi with our tunnel up",
			[]controlFamilyInterface{loopback, wifi, cellularV6Only, utun},
			true, true,
		},
		{
			"ipv4-only wifi",
			[]controlFamilyInterface{loopback, wifi},
			true, false,
		},
		{
			"loopback alone is not connectivity",
			[]controlFamilyInterface{loopback},
			false, false,
		},
		{
			"an interface that is down carries no path",
			[]controlFamilyInterface{loopback, downEthernet},
			false, false,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restore := swapControlFamilyInterfaces(
				func() ([]controlFamilyInterface, error) { return test.ifaces, nil })
			defer restore()

			if got := probeFamilySupport(4); got != test.want4 {
				t.Fatalf("probeFamilySupport(4) = %v, want %v", got, test.want4)
			}
			if got := probeFamilySupport(6); got != test.want6 {
				t.Fatalf("probeFamilySupport(6) = %v, want %v", got, test.want6)
			}
		})
	}
}

// An unreadable interface table is NO EVIDENCE, and the only job this probe
// has is to refuse a dangerous demotion. Failing open answers "yes, the other
// family works" on a device nothing is known about, which is the one answer
// that can take the control plane down. This repo already documents that
// mobile interface enumeration can be restricted (see LocalIpv4Networks).
func TestProbeFamilySupportFailsClosedOnEnumerationError(t *testing.T) {
	restore := swapControlFamilyInterfaces(
		func() ([]controlFamilyInterface, error) {
			return nil, errors.New("operation not permitted")
		})
	defer restore()

	if probeFamilySupport(4) {
		t.Fatal("probeFamilySupport(4) said yes with no readable interface table")
	}
	if probeFamilySupport(6) {
		t.Fatal("probeFamilySupport(6) said yes with no readable interface table")
	}

	// and the guard it feeds must therefore refuse the demotion
	controlFamilyClear()
	defer controlFamilyClear()
	if controlFamilyDemote(6) {
		t.Fatal("demoted ipv6 on a device whose interfaces could not be read")
	}
}

// FamilySupported is the exported probe the extender activation reads (G3).
// It answers from the same interface table the probe does -- loopback alone is
// not connectivity -- and refuses anything that is not an ip version.
func TestFamilySupportedReadsTheInterfaceTable(t *testing.T) {
	loopback := controlFamilyInterface{
		name:  "lo0",
		flags: net.FlagUp | net.FlagLoopback,
		addrs: []net.Addr{ipNet("127.0.0.1/8"), ipNet("::1/128")},
	}
	// 198.18.0.0/15 (RFC 2544) and 3fff::/20 (RFC 9637) are global unicast and
	// are not among controlFamilyReservedPrefixes, which the documentation
	// ranges deliberately are -- so these read as connectivity while naming no
	// real network.
	dualStack := controlFamilyInterface{
		name:  "en0",
		flags: net.FlagUp,
		addrs: []net.Addr{ipNet("198.18.7.20/24"), ipNet("3fff:db8::1/64")},
	}

	restore := swapControlFamilyInterfaces(
		func() ([]controlFamilyInterface, error) {
			return []controlFamilyInterface{loopback}, nil
		})
	if FamilySupported(4) || FamilySupported(6) {
		restore()
		t.Fatal("a loopback-only host reported a usable family")
	}
	restore()

	restore = swapControlFamilyInterfaces(
		func() ([]controlFamilyInterface, error) {
			return []controlFamilyInterface{loopback, dualStack}, nil
		})
	supported4, supported6 := FamilySupported(4), FamilySupported(6)
	restore()
	if !supported4 || !supported6 {
		t.Fatalf("dual-stack host reported v4 = %v, v6 = %v", supported4, supported6)
	}

	for _, ipVersion := range []int{0, 5, -1, 46} {
		if FamilySupported(ipVersion) {
			t.Fatalf("FamilySupported(%d) = true, expected false", ipVersion)
		}
	}
}

// The tests run on dual-stack hosts and require v6 (EXTENDER.md I), so the
// unseamed probe must answer yes for both families here. A no is a broken test
// host, not a broken build.
func TestFamilySupportedOnThisHost(t *testing.T) {
	if !FamilySupported(4) {
		t.Error("this host has no usable ipv4; the extender tests require dual stack")
	}
	if !FamilySupported(6) {
		t.Error("this host has no usable ipv6; the extender tests require dual stack")
	}
}

func ipNet(cidr string) *net.IPNet {
	ip, network, err := net.ParseCIDR(cidr)
	if err != nil {
		panic(err)
	}
	network.IP = ip
	return network
}

// The POLICY accessor must never reflect a learned demotion. A ui row that
// read back "Force IPv4" because the heuristic fired could not be set back to
// Auto -- it would already appear not to be Auto. The demotion is visible
// through controlFamilyStatus instead, which is what the ui shows beside the
// policy. This is asserted HERE rather than in the sdk because
// controlFamilyDemote is only reachable from this package.
func TestControlIpFamilyPolicyIgnoresDemotion(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()
	SetControlIpFamilyPolicy(IpFamilyAuto)
	defer SetControlIpFamilyPolicy(IpFamilyAuto)

	if !controlFamilyDemote(6) {
		t.Fatal("expected the demotion to take")
	}
	if got := ControlIpFamilyPolicy(); got != IpFamilyAuto {
		t.Fatalf("policy reads %d after a demotion, want IpFamilyAuto -- "+
			"a demotion must never be reported as a policy the user set", got)
	}
	if controlFamilyStatus() == "" {
		t.Fatal("expected a non-empty status while a demotion is live -- " +
			"the ui has no other way to tell auto-with-a-demotion from plain auto")
	}
}

func TestControlFamilyDemoteNarrowsToTheOtherFamily(t *testing.T) {
	tests := []struct {
		name   string
		demote int
		want   string
	}{
		{"demote 6 narrows to tcp4", 6, "tcp4"},
		{"demote 4 narrows to tcp6", 4, "tcp6"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			restore := swapControlFamilyProbe(func(int) bool { return true })
			defer restore()
			controlFamilyClear()
			defer controlFamilyClear()

			if !controlFamilyDemote(test.demote) {
				t.Fatal("expected the demotion to take")
			}
			network, err := controlDialNetwork("tcp", "api.example:443")
			if err != nil {
				t.Fatal(err)
			}
			if network != test.want {
				t.Fatalf("got %q, want %s", network, test.want)
			}
			if controlFamilyStatus() == "" {
				t.Fatal("expected a non-empty status while a demotion is live")
			}
		})
	}
}

// A force beats the ledger in both directions -- it is an explicit override,
// which is the whole point of the setting.
func TestForceBeatsDemotion(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()
	controlFamilyDemote(6)

	SetControlIpFamilyPolicy(IpFamilyForce6)
	defer SetControlIpFamilyPolicy(IpFamilyAuto)
	network, err := controlDialNetwork("tcp", "api.example:443")
	if err != nil {
		t.Fatal(err)
	}
	if network != "tcp6" {
		t.Fatalf("got %q, want tcp6 -- an explicit force outranks a demotion", network)
	}
}

func TestControlFamilyBackoffDoublesAndCaps(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	base := time.Unix(1750000000, 0)
	now := base
	restoreClock := swapControlFamilyClock(func() time.Time { return now })
	defer restoreClock()
	controlFamilyClear()
	defer controlFamilyClear()

	controlFamilyDemote(6)
	if got := controlFamilyDemotedUntil(6).Sub(base); got != controlFamilyDemotionBase {
		t.Fatalf("first demotion lasts %s, want %s", got, controlFamilyDemotionBase)
	}
	controlFamilyDemote(6)
	if got := controlFamilyDemotedUntil(6).Sub(base); got != 2*controlFamilyDemotionBase {
		t.Fatalf("second demotion lasts %s, want %s", got, 2*controlFamilyDemotionBase)
	}
	for i := 0; i < 20; i += 1 {
		controlFamilyDemote(6)
	}
	if got := controlFamilyDemotedUntil(6).Sub(base); got != controlFamilyDemotionMax {
		t.Fatalf("demotion lasts %s, want the %s cap", got, controlFamilyDemotionMax)
	}
}

func TestControlFamilyDemotionExpires(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	now := time.Unix(1750000000, 0)
	restoreClock := swapControlFamilyClock(func() time.Time { return now })
	defer restoreClock()
	controlFamilyClear()
	defer controlFamilyClear()

	controlFamilyDemote(6)
	now = now.Add(controlFamilyDemotionBase + time.Second)
	network, err := controlDialNetwork("tcp", "api.example:443")
	if err != nil {
		t.Fatal(err)
	}
	if network != "tcp" {
		t.Fatalf("got %q, want tcp once the demotion expired", network)
	}
	if controlFamilyStatus() != "" {
		t.Fatal("expected an empty status once the demotion expired")
	}
}

// A network change invalidates everything learned about the old path.
func TestNetworkChangedClearsTheLedger(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	controlFamilyDemote(6)
	NetworkChanged()
	network, err := controlDialNetwork("tcp", "api.example:443")
	if err != nil {
		t.Fatal(err)
	}
	if network != "tcp" {
		t.Fatalf("got %q, want tcp after a network change", network)
	}
}

func TestConnFamily(t *testing.T) {
	tests := []struct {
		name string
		addr net.Addr
		want int
	}{
		{"ipv4", &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 443}, 4},
		{"ipv4 in ipv6 form", &net.TCPAddr{IP: net.ParseIP("::ffff:192.0.2.1"), Port: 443}, 4},
		{"ipv6", &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}, 6},
		{"nil", nil, 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := connFamily(&stubConn{remote: test.addr}); got != test.want {
				t.Fatalf("got %d, want %d", got, test.want)
			}
		})
	}
}

type stubConn struct {
	net.Conn
	remote net.Addr
}

func (self *stubConn) RemoteAddr() net.Addr { return self.remote }

func (self *stubConn) Close() error { return nil }

// The family must be a LITERAL token, never derived from the address. The sdk
// log redactor rewrites both IPv4 and IPv6 literals to the same opaque token
// shape, so a redacted bundle -- the mode users are asked to send -- cannot
// tell a v4 dial from a v6 dial by its address.
func TestControlDialFamilyLineCarriesALiteralFamilyToken(t *testing.T) {
	conn := &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}
	line := controlDialFamilyLine("api", "tcp", "api.example:443", conn, nil)
	if !strings.Contains(line, "family=6") {
		t.Fatalf("line %q does not carry a literal family token", line)
	}
	if !strings.Contains(line, "tag=api") {
		t.Fatalf("line %q does not name the dial tag", line)
	}

	conn4 := &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 443}}
	line4 := controlDialFamilyLine("platform", "tcp", "connect.example:443", conn4, nil)
	if !strings.Contains(line4, "family=4") {
		t.Fatalf("line %q does not carry a literal family token", line4)
	}
}

// A failed dial has no connection to read a family from, and must say so
// rather than claim one.
func TestControlDialFamilyLineOnFailure(t *testing.T) {
	line := controlDialFamilyLine("api", "tcp4", "api.example:443", nil, errors.New("i/o timeout"))
	if !strings.Contains(line, "family=?") {
		t.Fatalf("line %q should report an unknown family on failure", line)
	}
	if !strings.Contains(line, "err=") {
		t.Fatalf("line %q should carry the error", line)
	}
}

// The h3/quic transport resolves a name to exactly ONE udp address and dials
// it, with no family race of any kind. It is the fallback carrier that is
// supposed to rescue a stalled h1, so if it picks the same broken family the
// fallback is lost too.
func TestPickControlUDPAddrHonorsPolicyAndDemotion(t *testing.T) {
	v6 := net.IPAddr{IP: net.ParseIP("2001:db8::1")}
	v4 := net.IPAddr{IP: net.ParseIP("192.0.2.1")}
	addrs := []net.IPAddr{v6, v4}

	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	// auto with nothing learned ties toward IPv4, matching
	// net.ResolveUDPAddr's own tie-break -- NOT the resolver's list order
	// (addrs[0] here is v6), so a dual-stack, v6-first RFC 6724 ordering does
	// not silently become the new default family.
	if got := pickControlIPAddr(addrs); !got.IP.Equal(v4.IP) {
		t.Fatalf("got %v, want the IPv4 tie-break address", got.IP)
	}

	// a demotion moves the pick off the demoted family
	controlFamilyDemote(6)
	if got := pickControlIPAddr(addrs); !got.IP.Equal(v4.IP) {
		t.Fatalf("got %v, want the IPv4 address once IPv6 is demoted", got.IP)
	}
	controlFamilyClear()

	// demoting the OTHER family is the assertion that actually distinguishes
	// the demotion branch from the IPv4-first Auto default above: Auto alone
	// already answers v4, so demote(6)+want=v4 above cannot fail if the
	// demotion branch is deleted. demote(4) must move the pick to v6.
	controlFamilyDemote(4)
	if got := pickControlIPAddr(addrs); !got.IP.Equal(v6.IP) {
		t.Fatalf("got %v, want the IPv6 address once IPv4 is demoted", got.IP)
	}
	controlFamilyClear()

	// a force wins outright
	SetControlIpFamilyPolicy(IpFamilyForce4)
	defer SetControlIpFamilyPolicy(IpFamilyAuto)
	if got := pickControlIPAddr(addrs); !got.IP.Equal(v4.IP) {
		t.Fatalf("got %v, want the IPv4 address under force4", got.IP)
	}

	// force6 is the assertion that distinguishes the force switch from the
	// IPv4-first Auto default, pinned here at the unit level and not only
	// through the /etc/hosts-dependent resolveEgressUDPAddr test.
	SetControlIpFamilyPolicy(IpFamilyForce6)
	if got := pickControlIPAddr(addrs); !got.IP.Equal(v6.IP) {
		t.Fatalf("got %v, want the IPv6 address under force6", got.IP)
	}
}

// With no address of the preferred family, the pick falls back rather than
// failing: a forced family that the name does not publish must not make the
// transport unusable.
func TestPickControlUDPAddrFallsBackWhenNoAddressMatches(t *testing.T) {
	v6 := net.IPAddr{IP: net.ParseIP("2001:db8::1")}
	SetControlIpFamilyPolicy(IpFamilyForce4)
	defer SetControlIpFamilyPolicy(IpFamilyAuto)
	if got := pickControlIPAddr([]net.IPAddr{v6}); !got.IP.Equal(v6.IP) {
		t.Fatalf("got %v, want the only available address", got.IP)
	}
}

// EgressInterfaceIndex is a (index4, index6) pair, and both being set at once
// is the NORMAL bound configuration -- the Windows service updates both on
// every network change (see EgressInterfaceIndex's doc comment) -- not an
// edge case. With both set the bind constrains nothing about family, so an
// explicit Force4/Force6 must still win; only a bind that names exactly one
// family may override the pick.
func TestEgressBoundIPAddrIgnoresNonConstrainingBind(t *testing.T) {
	v6 := net.IPAddr{IP: net.ParseIP("2001:db8::1")}
	v4 := net.IPAddr{IP: net.ParseIP("192.0.2.1")}
	addrs := []net.IPAddr{v6, v4} // v6 first, so a buggy "take addrs[0]" loop is caught

	SetControlIpFamilyPolicy(IpFamilyForce4)
	defer SetControlIpFamilyPolicy(IpFamilyAuto)
	pick := pickControlIPAddr(addrs)
	if !pick.IP.Equal(v4.IP) {
		t.Fatalf("precondition: pickControlIPAddr under force4 = %v, want v4", pick.IP)
	}

	// both indexes set: not a family constraint. The forced pick must survive
	// untouched, even though addrs[0] (v6) has a nonzero index too.
	if got := egressBoundIPAddr(addrs, 16, 23, pick); !got.IP.Equal(v4.IP) {
		t.Fatalf("got %v, want the forced address unchanged by a both-set bind", got.IP)
	}

	// neither index set: also not a constraint, pick stands.
	if got := egressBoundIPAddr(addrs, 0, 0, pick); !got.IP.Equal(v4.IP) {
		t.Fatalf("got %v, want the forced address unchanged by an unset bind", got.IP)
	}
}

// Exactly one index set IS a hard family constraint and must override even an
// opposite Force, because the socket literally cannot carry the other family.
func TestEgressBoundIPAddrOverridesForceWhenSingleFamilyBound(t *testing.T) {
	v6 := net.IPAddr{IP: net.ParseIP("2001:db8::1")}
	v4 := net.IPAddr{IP: net.ParseIP("192.0.2.1")}
	addrs := []net.IPAddr{v6, v4}

	SetControlIpFamilyPolicy(IpFamilyForce4)
	defer SetControlIpFamilyPolicy(IpFamilyAuto)
	pick := pickControlIPAddr(addrs)

	if got := egressBoundIPAddr(addrs, 0, 23, pick); !got.IP.Equal(v6.IP) {
		t.Fatalf("got %v, want the v6-bound address to override force4", got.IP)
	}
}

// A demotion learned on one path must not be applied unchecked on the next.
//
// The ledger's only invalidation is AddNetworkChangeListener(controlFamilyClear),
// and connect.NetworkChanged() has exactly one caller in the whole tree --
// DeviceLocal. On ios that means the listener fires in the network extension
// and NEVER in the app process, which is the process that dials pre-login and
// whenever the tunnel is down: regimes 1 and 3 of the design's own table, and
// the state a user is in when they open the Developer menu to fix this. Android
// registers its callbacks from initDevice, so it is exposed the same way while
// signed out. So an app-process demotion could stand for up to six hours with
// no path change ever clearing it.
//
// The self-inflicted-outage guard is therefore evaluated on USE as well as on
// record: a demotion can never be applied on a path where recording it would
// have been refused.
func TestADemotionIsRevalidatedOnUseNotOnlyWhenRecorded(t *testing.T) {
	controlFamilyClear()
	defer controlFamilyClear()

	// learned on a dual-stack wifi with a broken HE ipv6 path
	restoreDualStack := swapControlFamilyProbe(func(int) bool { return true })
	if !controlFamilyDemote(6) {
		t.Fatal("expected the demotion to take on a dual-stack path")
	}
	if got, _ := controlDialNetwork("tcp", "api.example:443"); got != "tcp4" {
		t.Fatalf("precondition: controlDialNetwork = %q, want tcp4", got)
	}
	restoreDualStack()

	// the user joins an ipv6-only cellular / NAT64 network. No NetworkChanged()
	// arrives, because in this process nothing can call it.
	restore := swapControlFamilyProbe(func(family int) bool { return family == 6 })
	defer restore()

	if got := controlFamilyDemotedFamily(); got != 0 {
		t.Fatalf("ipv%d is still demoted on a path with no ipv4 at all", got)
	}
	network, err := controlDialNetwork("tcp", "api.example:443")
	if err != nil {
		t.Fatal(err)
	}
	if network != "tcp" {
		t.Fatalf("narrowed to %q on a path with no ipv4 -- every control dial "+
			"would fail with no route until the backoff expired", network)
	}
	if got := controlFamilyStatus(); got != "" {
		t.Fatalf("status %q describes a demotion the dialer is no longer acting on", got)
	}
	if got := pickControlIPAddr([]net.IPAddr{
		{IP: net.ParseIP("2001:db8::1")},
		{IP: net.ParseIP("192.0.2.1")},
	}); !got.IP.Equal(net.ParseIP("192.0.2.1")) {
		// auto with nothing live ties toward ipv4; the assertion that matters
		// is that the stale demotion of 6 is not steering this pick
		t.Fatalf("h3/quic pick %v is still steered by the stale demotion", got.IP)
	}
}

// The use-time guard has its own copy of "which family is the OTHER one", and
// that copy needs the same discriminating table that pinned the copy in
// controlFamilyDemote (TestControlFamilyDemoteRefusedWhenOtherFamilyUnusable
// above). Same defect class, reintroduced in new code.
//
// With controlFamilyLiveDemotion's `other := 4; if live == 4 { other = 6 }`
// mutated so `other` is always 4, the ipv6 row cannot tell the difference --
// it probes 4 either way. Only the ipv4 row can: the mutant re-validates a
// demotion of IPv4 against probe(4), which is the family it just demoted, gets
// true, and keeps steering every control dial in the process onto IPv6 on a
// device that has no IPv6 at all. That is the self-inflicted outage the guard
// exists to prevent, arrived at through the guard itself.
//
// Both rows demote on a dual-stack path first, because recording a demotion is
// itself guarded: the ledger cannot be seeded into the state under test any
// other way.
func TestALiveDemotionIsRevalidatedAgainstTheOtherFamily(t *testing.T) {
	tests := []struct {
		name   string
		demote int
		// the only family with a path once the device has moved
		usable int
	}{
		{"ipv6 demoted, then the path loses ipv4", 6, 6},
		{"ipv4 demoted, then the path loses ipv6", 4, 4},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			controlFamilyClear()
			defer controlFamilyClear()

			// learned where the guard permits it: both families present
			restoreDualStack := swapControlFamilyProbe(func(int) bool { return true })
			if !controlFamilyDemote(test.demote) {
				t.Fatalf("precondition: demoting ipv%d on a dual-stack path was refused", test.demote)
			}
			restoreDualStack()

			// the device moves to a path carrying ONLY the demoted family
			restore := swapControlFamilyProbe(func(family int) bool {
				return family == test.usable
			})
			defer restore()

			if got := controlFamilyDemotedFamily(); got != 0 {
				t.Fatalf(
					"ipv%d is still demoted on a path with no ipv%d -- the guard "+
						"was re-checked against the demoted family instead of the other one",
					got, 10-test.usable)
			}
			network, err := controlDialNetwork("tcp", "api.example:443")
			if err != nil {
				t.Fatal(err)
			}
			if network != "tcp" {
				t.Fatalf(
					"narrowed to %q on a path that has no such family -- every "+
						"control dial would fail with no route until the backoff expired",
					network)
			}
			if got := controlFamilyStatus(); got != "" {
				t.Fatalf("status %q describes a demotion the dialer is no longer acting on", got)
			}
		})
	}
}
