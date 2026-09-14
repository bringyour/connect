package connect

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestOrderDialAddrsInterleavesV6First(t *testing.T) {
	ordered := orderDialAddrs([]netip.Addr{
		netip.MustParseAddr("192.0.2.2"),
		netip.MustParseAddr("2001:db8::2"),
		netip.MustParseAddr("192.0.2.1"),
		netip.MustParseAddr("::ffff:192.0.2.1"), // mapped duplicate of the v4 above
		netip.MustParseAddr("2001:db8::1"),
		netip.MustParseAddr("192.0.2.3"),
	})
	want := []string{"2001:db8::1", "192.0.2.1", "2001:db8::2", "192.0.2.2", "192.0.2.3"}
	if len(ordered) != len(want) {
		t.Fatalf("ordered = %v, want %v", ordered, want)
	}
	for i := range want {
		if ordered[i].String() != want[i] {
			t.Fatalf("ordered[%d] = %s, want %s (all %v)", i, ordered[i], want[i], ordered)
		}
	}
}

func TestLookupNetworkForDial(t *testing.T) {
	cases := map[string]string{"tcp": "ip", "tcp4": "ip4", "tcp6": "ip6", "udp": "ip", "udp4": "ip4", "udp6": "ip6"}
	for network, want := range cases {
		if got := lookupNetworkForDial(network); got != want {
			t.Fatalf("lookupNetworkForDial(%s) = %s, want %s", network, got, want)
		}
	}
}

func TestFamilyDialNetworkFollowsTheAddress(t *testing.T) {
	v4 := netip.MustParseAddr("192.0.2.1")
	v6 := netip.MustParseAddr("2001:db8::1")
	mapped := netip.MustParseAddr("::ffff:192.0.2.1")
	cases := []struct {
		network string
		addr    netip.Addr
		want    string
	}{
		{"tcp", v4, "tcp4"}, {"tcp", v6, "tcp6"}, {"tcp4", v4, "tcp4"}, {"tcp6", v6, "tcp6"},
		{"udp", v4, "udp4"}, {"udp", v6, "udp6"}, {"tcp", mapped, "tcp4"},
	}
	for _, c := range cases {
		if got := familyDialNetwork(c.network, c.addr); got != c.want {
			t.Fatalf("familyDialNetwork(%s, %s) = %s, want %s", c.network, c.addr, got, c.want)
		}
	}
}

func TestDialAddrsMatchNetworkNarrowsToOneFamily(t *testing.T) {
	addrs := []netip.Addr{netip.MustParseAddr("2001:db8::1"), netip.MustParseAddr("192.0.2.1")}
	if got := dialAddrsMatchNetwork("tcp4", addrs); len(got) != 1 || !got[0].Is4() {
		t.Fatalf("tcp4 kept %v, want the v4 address only", got)
	}
	if got := dialAddrsMatchNetwork("tcp6", addrs); len(got) != 1 || !got[0].Is6() {
		t.Fatalf("tcp6 kept %v, want the v6 address only", got)
	}
	if got := dialAddrsMatchNetwork("tcp", addrs); len(got) != 2 {
		t.Fatalf("tcp kept %v, want both", got)
	}
}

// raceTestDial is a scripted per-address dialer: an address in hang never
// answers until the context ends, an address in refuse fails at once, and any
// other address succeeds with a pipe whose server half is handed back.
type raceTestDial struct {
	hang     map[netip.Addr]bool
	refuse   map[netip.Addr]bool
	attempts atomic.Int32
	launched []netip.Addr
	peers    chan net.Conn
}

func newRaceTestDial() *raceTestDial {
	return &raceTestDial{
		hang:   map[netip.Addr]bool{},
		refuse: map[netip.Addr]bool{},
		peers:  make(chan net.Conn, 8),
	}
}

func (self *raceTestDial) dial(ctx context.Context, addr netip.Addr) (net.Conn, error) {
	self.attempts.Add(1)
	switch {
	case self.hang[addr]:
		<-ctx.Done()
		return nil, ctx.Err()
	case self.refuse[addr]:
		return nil, errors.New("connection refused")
	default:
		client, server := net.Pipe()
		self.peers <- server
		return client, nil
	}
}

func TestDialAddrsRaceFallsToV4WhenV6Hangs(t *testing.T) {
	v6 := netip.MustParseAddr("2001:db8::1")
	v4 := netip.MustParseAddr("192.0.2.1")
	dial := newRaceTestDial()
	dial.hang[v6] = true

	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	started := time.Now()
	conn, err := dialAddrsRace(ctx, []netip.Addr{v6, v4}, 100*time.Millisecond, dial.dial)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	server := <-dial.peers
	defer server.Close()
	elapsed := time.Since(started)
	if elapsed < 100*time.Millisecond {
		t.Fatalf("v4 was launched after %s, before the fallback delay", elapsed)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("fallback took %s, expected roughly one delay", elapsed)
	}
	if got := dial.attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2 (v6 then v4)", got)
	}
}

func TestDialAddrsRaceLaunchesNextAtOnceOnDefinitiveFailure(t *testing.T) {
	v6 := netip.MustParseAddr("2001:db8::1")
	v4 := netip.MustParseAddr("192.0.2.1")
	dial := newRaceTestDial()
	dial.refuse[v6] = true

	started := time.Now()
	conn, err := dialAddrsRace(t.Context(), []netip.Addr{v6, v4}, time.Second, dial.dial)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	server := <-dial.peers
	defer server.Close()
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("a refused v6 waited the full fallback delay (%s) before v4 was launched", elapsed)
	}
}

func TestDialAddrsRaceFirstSuccessWinsWithoutFallback(t *testing.T) {
	v6 := netip.MustParseAddr("2001:db8::1")
	v4 := netip.MustParseAddr("192.0.2.1")
	dial := newRaceTestDial()

	conn, err := dialAddrsRace(t.Context(), []netip.Addr{v6, v4}, time.Second, dial.dial)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	server := <-dial.peers
	defer server.Close()
	if got := dial.attempts.Load(); got != 1 {
		t.Fatalf("attempts = %d, want 1: the first success must not launch the fallback", got)
	}
}

func TestDialAddrsRaceJoinsEveryFailure(t *testing.T) {
	v6 := netip.MustParseAddr("2001:db8::1")
	v4 := netip.MustParseAddr("192.0.2.1")
	dial := newRaceTestDial()
	dial.refuse[v6] = true
	dial.refuse[v4] = true

	conn, err := dialAddrsRace(t.Context(), []netip.Addr{v6, v4}, time.Second, dial.dial)
	if err == nil {
		conn.Close()
		t.Fatal("race returned a connection although every address was refused")
	}
	if got := dial.attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Fatalf("err = %v, want the joined dial errors", err)
	}
}

func TestDialAddrsRaceHonorsCallerCancellation(t *testing.T) {
	v6 := netip.MustParseAddr("2001:db8::1")
	v4 := netip.MustParseAddr("192.0.2.1")
	dial := newRaceTestDial()
	dial.hang[v6] = true
	dial.hang[v4] = true

	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	conn, err := dialAddrsRace(ctx, []netip.Addr{v6, v4}, 50*time.Millisecond, dial.dial)
	if err == nil {
		conn.Close()
		t.Fatal("race returned a connection although every address hung")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the caller deadline", err)
	}
}

func TestDialAddrsRaceClosesLosingConnections(t *testing.T) {
	v6 := netip.MustParseAddr("2001:db8::1")
	v4 := netip.MustParseAddr("192.0.2.1")
	// both succeed: the first wins, the second (launched at once, zero delay)
	// must be closed rather than leaked
	dial := newRaceTestDial()
	conn, err := dialAddrsRace(t.Context(), []netip.Addr{v6, v4}, 0, dial.dial)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// the winner's peer stays open (its read only times out); the loser's
	// peer reads a closed pipe once the race closes the loser
	closedPeers := 0
	for range 2 {
		var server net.Conn
		select {
		case server = <-dial.peers:
		case <-time.After(2 * time.Second):
			t.Fatal("both dials succeeded but their peers never arrived")
		}
		server.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		buffer := make([]byte, 1)
		if _, readErr := server.Read(buffer); readErr != nil && !errors.Is(readErr, os.ErrDeadlineExceeded) {
			closedPeers++
		}
		server.Close()
	}
	if closedPeers != 1 {
		t.Fatalf("closed peers = %d, want exactly 1: the loser closed, the winner open", closedPeers)
	}
}

func TestResolveDialAddrsFollowsTheNarrowedNetwork(t *testing.T) {
	resolver := newFamilyTestResolver(t,
		netip.MustParseAddr("192.0.2.1"),
		netip.MustParseAddr("2001:db8::1"),
	)
	both, err := resolveDialAddrs(t.Context(), resolver, "tcp", "dual.service.test")
	if err != nil {
		t.Fatal(err)
	}
	if len(both) != 2 || !both[0].Is6() || !both[1].Is4() {
		t.Fatalf("tcp resolved %v, want [v6 v4]", both)
	}
	only4, err := resolveDialAddrs(t.Context(), resolver, "tcp4", "dual.service.test")
	if err != nil {
		t.Fatal(err)
	}
	if len(only4) != 1 || !only4[0].Is4() {
		t.Fatalf("tcp4 resolved %v, want the v4 address only", only4)
	}
	only6, err := resolveDialAddrs(t.Context(), resolver, "tcp6", "dual.service.test")
	if err != nil {
		t.Fatal(err)
	}
	if len(only6) != 1 || !only6[0].Is6() {
		t.Fatalf("tcp6 resolved %v, want the v6 address only", only6)
	}
}

func TestResolveDohDialAddrsQueriesBothRecordTypes(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeDohWire(w, r, []netip.Addr{
			netip.MustParseAddr("192.0.2.1"),
			netip.MustParseAddr("2001:db8::1"),
		}, 60, false)
	}))
	defer server.Close()

	settings := DefaultDohSettings()
	settings.RequestTimeout = time.Second
	settings.DnsResolverSettings = &DnsResolverSettings{
		EnableRemoteDoh:   true,
		RemoteDohUrlsIpv4: []string{server.URL},
	}
	cache := NewDohCache(settings)
	defer cache.Close()

	both, err := resolveDohDialAddrs(t.Context(), cache, "tcp", "dual.service.test")
	if err != nil {
		t.Fatal(err)
	}
	if len(both) != 2 || !both[0].Is6() || !both[1].Is4() {
		t.Fatalf("tcp resolved %v, want [v6 v4]", both)
	}
	only4, err := resolveDohDialAddrs(t.Context(), cache, "tcp4", "dual.service.test")
	if err != nil {
		t.Fatal(err)
	}
	if len(only4) != 1 || !only4[0].Is4() {
		t.Fatalf("tcp4 resolved %v, want the v4 address only", only4)
	}
	only6, err := resolveDohDialAddrs(t.Context(), cache, "udp6", "dual.service.test")
	if err != nil {
		t.Fatal(err)
	}
	if len(only6) != 1 || !only6[0].Is6() {
		t.Fatalf("udp6 resolved %v, want the v6 address only", only6)
	}
}

// The ConnectSettings seam races a hostname's families end to end: with a
// listener on only one family, the other family's address is refused and the
// race still lands on the listener, for either family.
func TestConnectSettingsDialContextRacesFamilies(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		listener, err := net.Listen(testTcpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				conn.Close()
			}
		}()
		port := listener.Addr().(*net.TCPAddr).Port
		// the other family resolves to loopback too, where nothing listens on
		// this port: a definitive refusal, not a hang
		resolver := newFamilyTestResolver(t, netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("::1"))

		settings := DefaultConnectSettings()
		settings.Resolver = resolver
		var hooked []string
		settings.DialNetworkHook = func(network string, addr string) {
			hooked = append(hooked, network)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		conn, err := settings.DialContext(ctx, "tcp", net.JoinHostPort("dual.service.test", itoa(port)))
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		remote, _ := netip.ParseAddrPort(conn.RemoteAddr().String())
		if (ipVersion == 4) != remote.Addr().Unmap().Is4() {
			t.Fatalf("connected to %s, want the v%d listener", conn.RemoteAddr(), ipVersion)
		}
		if len(hooked) != 1 || hooked[0] != "tcp" {
			t.Fatalf("hook saw %v, want exactly [tcp]: the seam narrows once, before the race", hooked)
		}
	})
}

// A forced family narrows both the resolution and the dial: under Force6 the
// v4 listener is never a candidate even though the name has an A record.
func TestConnectSettingsDialContextResolvesOnlyTheForcedFamily(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		listener, err := net.Listen(testTcpNetwork(ipVersion), testLoopbackHostPort(ipVersion, 0))
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		go func() {
			for {
				conn, err := listener.Accept()
				if err != nil {
					return
				}
				conn.Close()
			}
		}()
		port := listener.Addr().(*net.TCPAddr).Port
		resolver := newFamilyTestResolver(t, netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("::1"))

		// force the OTHER family: the listening family must never be dialed
		if ipVersion == 4 {
			SetControlIpFamilyPolicy(IpFamilyForce6)
		} else {
			SetControlIpFamilyPolicy(IpFamilyForce4)
		}
		defer SetControlIpFamilyPolicy(IpFamilyAuto)

		settings := DefaultConnectSettings()
		settings.Resolver = resolver
		settings.ConnectTimeout = 2 * time.Second
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		conn, err := settings.DialContext(ctx, "tcp", net.JoinHostPort("dual.service.test", itoa(port)))
		if err == nil {
			conn.Close()
			t.Fatalf("v%d listener was reached under a policy that forces the other family", ipVersion)
		}
	})
}

func itoa(n int) string {
	return strconv.Itoa(n)
}
