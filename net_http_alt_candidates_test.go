package connect

import (
	"context"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// The addresses and ports one alt dial may use (EXTENDER.md L2, L4).
//
// The whodis carrier is the case the port list matters for: alt is reached on
// 53 first and 4053 second, because the router in front of the proxy hosts
// DNATs public 53 to 4053 and a network that blocks 53 still reaches 4053. An
// alt url with an explicit port pins both carriers to it, which is how a test
// fixture on an ephemeral port is reached at all.

// One direct strategy with an alt url and an optional family pin.
func newTestAltCandidateStrategy(
	t *testing.T,
	altUrl string,
	ipFamily int,
) *ClientStrategy {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	settings := DefaultClientStrategySettings()
	settings.AltUrl = altUrl
	clientStrategy := NewDirectClientStrategy(ctx, settings, ipFamily)
	t.Cleanup(func() {
		clientStrategy.Close()
		cancel()
	})
	return clientStrategy
}

func testAltUdpHostPorts(udpAddrs []*net.UDPAddr) []string {
	hostPorts := []string{}
	for _, udpAddr := range udpAddrs {
		hostPorts = append(
			hostPorts,
			net.JoinHostPort(udpAddr.IP.String(), strconv.Itoa(udpAddr.Port)),
		)
	}
	return hostPorts
}

// The whodis carrier tries 53 before 4053 when the alt url pins no port, and
// the h3 carrier takes the fixed alt port. An explicit port pins both.
func TestAltDialCandidatePorts(t *testing.T) {
	cases := []struct {
		altUrl     string
		whodis     bool
		hostPorts  []string
		ipFamily   int
		wantErrors bool
	}{
		{
			altUrl:    "https://192.0.2.10",
			hostPorts: []string{"192.0.2.10:443"},
		},
		{
			altUrl:    "https://192.0.2.10",
			whodis:    true,
			hostPorts: []string{"192.0.2.10:53", "192.0.2.10:4053"},
		},
		{
			// an explicit port pins both carriers to it, which is how a
			// fixture on an ephemeral port is reached
			altUrl:    "https://192.0.2.10:15443",
			hostPorts: []string{"192.0.2.10:15443"},
		},
		{
			altUrl:    "https://192.0.2.10:15443",
			whodis:    true,
			hostPorts: []string{"192.0.2.10:15443"},
		},
		{
			// the whodis port itself is one candidate, not two
			altUrl:    "https://192.0.2.10:4053",
			whodis:    true,
			hostPorts: []string{"192.0.2.10:4053"},
		},
		{
			// an explicit port is the only candidate even when it is a legacy
			// one: the url names where this deployment answers
			altUrl:    "https://192.0.2.10:8053",
			whodis:    true,
			hostPorts: []string{"192.0.2.10:8053"},
		},
		{
			altUrl:    "https://[2001:db8::10]",
			hostPorts: []string{"[2001:db8::10]:443"},
		},
		{
			altUrl:    "https://[2001:db8::10]",
			whodis:    true,
			hostPorts: []string{"[2001:db8::10]:53", "[2001:db8::10]:4053"},
		},
	}
	for _, c := range cases {
		clientStrategy := newTestAltCandidateStrategy(t, c.altUrl, c.ipFamily)
		udpAddrs, err := clientStrategy.altDialCandidates(context.Background(), c.whodis)
		if err != nil {
			t.Errorf("%s whodis=%v: %v", c.altUrl, c.whodis, err)
			continue
		}
		if hostPorts := testAltUdpHostPorts(udpAddrs); !slices.Equal(hostPorts, c.hostPorts) {
			t.Errorf(
				"%s whodis=%v candidates = %v, expected %v",
				c.altUrl, c.whodis, hostPorts, c.hostPorts)
		}
	}
}

// A family-pinned strategy dials alt on its family only: an activation that
// crossed the other family would prove the wrong address to the operator.
func TestAltDialCandidatesFollowTheFamilyPin(t *testing.T) {
	cases := []struct {
		altUrl    string
		ipFamily  int
		reachable bool
	}{
		{altUrl: "https://192.0.2.10", ipFamily: 4, reachable: true},
		{altUrl: "https://192.0.2.10", ipFamily: 6, reachable: false},
		{altUrl: "https://[2001:db8::10]", ipFamily: 6, reachable: true},
		{altUrl: "https://[2001:db8::10]", ipFamily: 4, reachable: false},
		// no pin reaches either family
		{altUrl: "https://192.0.2.10", ipFamily: 0, reachable: true},
		{altUrl: "https://[2001:db8::10]", ipFamily: 0, reachable: true},
	}
	for _, c := range cases {
		clientStrategy := newTestAltCandidateStrategy(t, c.altUrl, c.ipFamily)
		for _, whodis := range []bool{false, true} {
			udpAddrs, err := clientStrategy.altDialCandidates(context.Background(), whodis)
			if c.reachable {
				if err != nil {
					t.Errorf("%s pinned to v%d whodis=%v: %v", c.altUrl, c.ipFamily, whodis, err)
					continue
				}
				if len(udpAddrs) == 0 {
					t.Errorf("%s pinned to v%d whodis=%v resolved nothing", c.altUrl, c.ipFamily, whodis)
				}
				continue
			}
			if err == nil {
				t.Errorf(
					"%s pinned to v%d whodis=%v reached %v",
					c.altUrl, c.ipFamily, whodis, testAltUdpHostPorts(udpAddrs))
			}
		}
	}
}

// An alt url with no host is refused before anything resolves, which is the
// state of a space with no alt deployment.
func TestAltDialCandidatesRefuseAnAltUrlWithNoHost(t *testing.T) {
	for _, altUrl := range []string{"", "   ", "https://"} {
		clientStrategy := newTestAltCandidateStrategy(t, altUrl, 0)
		if udpAddrs, err := clientStrategy.altDialCandidates(context.Background(), false); err == nil {
			t.Errorf("%q resolved %v", altUrl, testAltUdpHostPorts(udpAddrs))
		} else if !strings.Contains(err.Error(), "names no host") {
			t.Errorf("%q failed with %v", altUrl, err)
		}
	}
	// and a strategy with no alt url builds no alt dialers at all
	clientStrategy := newTestAltCandidateStrategy(t, "", 0)
	for _, dialer := range testAllDialers(clientStrategy) {
		if strings.HasPrefix(dialer.description, "alt ") {
			t.Errorf("a strategy with no alt url built %q", dialer.description)
		}
	}
}

// The whodis encoding tld falls back to the connect default when the strategy
// names none, so an alt dial never sends an unencoded query.
func TestAltDnsTldFallsBackToTheDefault(t *testing.T) {
	settings := DefaultClientStrategySettings()
	settings.DnsTlds = nil
	if tld := string(altDnsTld(settings)); tld != DefaultExtenderDnsTld {
		t.Errorf("tld = %q, expected %q", tld, DefaultExtenderDnsTld)
	}
	settings.DnsTlds = [][]byte{[]byte("x.example.")}
	if tld := string(altDnsTld(settings)); tld != "x.example." {
		t.Errorf("tld = %q", tld)
	}
	// with several the pick is one of them, whichever
	settings.DnsTlds = [][]byte{[]byte("x.example."), []byte("y.example.")}
	for i := 0; i < 16; i += 1 {
		tld := string(altDnsTld(settings))
		if tld != "x.example." && tld != "y.example." {
			t.Fatalf("tld = %q", tld)
		}
	}
}

func testAllDialers(clientStrategy *ClientStrategy) []*clientDialer {
	clientStrategy.mutex.Lock()
	defer clientStrategy.mutex.Unlock()
	dialers := []*clientDialer{}
	for dialer := range clientStrategy.dialers {
		dialers = append(dialers, dialer)
	}
	return dialers
}
