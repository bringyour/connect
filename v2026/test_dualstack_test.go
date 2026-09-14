package connect

// test_dualstack_test.go — shared dual-stack test scaffolding (IPV6.md D3).
//
// Every test that touches addresses, sockets or packets runs its body once
// per ip version through forEachIpVersion. Tests run on dual-stack hosts, so
// IPv6 loopback is REQUIRED: a host without it fails loudly rather than
// skipping, which would silently halve the coverage the design depends on.

import (
	"fmt"
	"net"
	"testing"
)

// testIpVersions is the pair every address-touching test runs under.
var testIpVersions = []int{4, 6}

// forEachIpVersion runs body once per ip version as a subtest named v4 and v6.
func forEachIpVersion(t *testing.T, body func(t *testing.T, ipVersion int)) {
	t.Helper()
	for _, ipVersion := range testIpVersions {
		t.Run(fmt.Sprintf("v%d", ipVersion), func(t *testing.T) {
			if ipVersion == 6 {
				requireIpv6Loopback(t)
			}
			body(t, ipVersion)
		})
	}
}

// requireIpv6Loopback fails the test when ::1 cannot be bound. Never skips.
func requireIpv6Loopback(t *testing.T) {
	t.Helper()
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Fatalf("ipv6 loopback is required for dual-stack tests: %v", err)
	}
	listener.Close()
}

// testLoopbackIp is the loopback literal for the version.
func testLoopbackIp(ipVersion int) string {
	if ipVersion == 6 {
		return "::1"
	}
	return "127.0.0.1"
}

// testLoopbackHostPort joins the loopback literal for the version with a port,
// bracketing v6.
func testLoopbackHostPort(ipVersion int, port int) string {
	return net.JoinHostPort(testLoopbackIp(ipVersion), fmt.Sprintf("%d", port))
}

// testTcpNetwork is the family-specific tcp network string for the version.
func testTcpNetwork(ipVersion int) string {
	if ipVersion == 6 {
		return "tcp6"
	}
	return "tcp4"
}

// testUdpNetwork is the family-specific udp network string for the version.
func testUdpNetwork(ipVersion int) string {
	if ipVersion == 6 {
		return "udp6"
	}
	return "udp4"
}
