package tether

import (
	"fmt"
	"net"
	"os/exec"
	"testing"

	"golang.zx2c4.com/wireguard/wgctrl/wgtypes"
)

func TestIsIPv4andIPv6(t *testing.T) {
	tests := []struct {
		name       string
		ipStr      string
		expectedV4 bool
		expectedV6 bool
	}{
		{"Normal IPv4", "192.168.1.1", true, false},
		{"IPv4 0 address", "0.0.0.0", true, false},
		{"IPv6 1 address", "::1", false, true},
		{"Normal IPv6", "2001:db8::68", false, true},
		{"IPv6 0 address", "::", false, true},
		{"Invalid IP", "invalid-ip", false, false},
		{"IPv4-mapped IPv6 address", "::ffff:192.0.2.1", true, false},
		{"Invalid IPv4-mapped IPv6 address", "::ffff:256.0.2.1", false, false},
		{"IPv6 with leading zeros", "0:0:0:0:0:0:c0a8:5901", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.ipStr, func(t *testing.T) {
			t.Parallel()

			ip := net.ParseIP(tt.ipStr)
			if (ip == nil) != (!tt.expectedV4 && !tt.expectedV6) {
				t.Errorf("ParseIP(%s) = %v, expected valid: %v", tt.ipStr, ip, !(!tt.expectedV4 && !tt.expectedV6))
				return
			}

			if ip != nil {
				if got := isIPv4(ip); got != tt.expectedV4 {
					t.Errorf("isIPv4(%s) = %v, expected %v", tt.ipStr, got, tt.expectedV4)
				}
				if got := isIPv6(ip); got != tt.expectedV6 {
					t.Errorf("isIPv6(%s) = %v, expected %v", tt.ipStr, got, tt.expectedV6)
				}
			}
		})
	}
}

func TestIncrementIP(t *testing.T) {
	tests := []struct {
		name     string
		input    net.IP
		expected net.IP
	}{
		{"IPv4 0 address", net.IPv4(0, 0, 0, 0), net.IPv4(0, 0, 0, 1)},
		{"Normal IPv4", net.IPv4(192, 168, 1, 1), net.IPv4(192, 168, 1, 2)},
		{"IPv4 rollover", net.IPv4(192, 168, 1, 255), net.IPv4(192, 168, 2, 0)},
		{"IPv4 max address", net.IPv4(255, 255, 255, 255), net.IPv4(0, 0, 0, 0)},
		{"IPv4-mapped IPv6", net.ParseIP("::ffff:192.0.2.1"), net.ParseIP("::ffff:192.0.2.2")},
		{"Invalid IPv4-mapped IPv6", net.ParseIP("::ffff:256.0.2.1"), net.ParseIP("::ffff:256.0.2.2")},
		{"Normal IPv6", net.ParseIP("fe80::1"), net.ParseIP("fe80::2")},
		{"IPv6 rollover", net.ParseIP("fe80::ffff"), net.ParseIP("fe80::1:0")},
		{"IPv6 max address", net.ParseIP("ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff"), net.ParseIP("::")},
		{"IPv6 to IPv4 rollover (IPv4-mapped IPv6 space)", net.ParseIP("::fffe:ffff:ffff"), net.ParseIP("0.0.0.0")},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ip := make(net.IP, len(test.input))
			copy(ip, test.input)
			incrementIP(ip)
			if !ip.Equal(test.expected) {
				t.Errorf("incrementIP(%v) = %v; want %v", test.input, ip, test.expected)
			}
		})
	}
}

func TestCalculateBroadcastAddr(t *testing.T) {
	tests := []struct {
		name     string
		subnet   net.IPNet
		expected net.IP
	}{
		{
			"IPv4 24-bit network space",
			net.IPNet{
				IP:   net.IPv4(192, 168, 1, 0),
				Mask: net.CIDRMask(24, 32),
			},
			net.IPv4(192, 168, 1, 255),
		},
		{
			"IPv4 8-bit network space",
			net.IPNet{
				IP:   net.IPv4(10, 0, 0, 0),
				Mask: net.CIDRMask(8, 32),
			},
			net.IPv4(10, 255, 255, 255),
		},
		{
			"IPv6 64-bit network space",
			net.IPNet{
				IP:   net.ParseIP("fe80::"),
				Mask: net.CIDRMask(64, 128),
			},
			net.ParseIP("fe80::ffff:ffff:ffff:ffff"),
		},
		{
			"IPv6 32-bit network space",
			net.IPNet{
				IP:   net.ParseIP("2001:db8::"),
				Mask: net.CIDRMask(32, 128),
			},
			net.ParseIP("2001:db8:ffff:ffff:ffff:ffff:ffff:ffff"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			broadcast := calculateBroadcastAddr(test.subnet)
			if !broadcast.Equal(test.expected) {
				t.Errorf("calculateBroadcastAddr(%v) = %v; want %v", test.subnet, broadcast, test.expected)
			}
		})
	}
}

func TestGetSubnetAvailableIP(t *testing.T) {
	tests := []struct {
		name     string
		subnet   string
		peers    []wgtypes.Peer
		expected string
		err      error
	}{
		{
			name:     "No peers in subnet",
			subnet:   "192.168.1.0/24",
			peers:    []wgtypes.Peer{},
			expected: "192.168.1.1",
			err:      nil,
		},
		{
			name:     "1 IP subnet",
			subnet:   "10.10.10.10/32",
			peers:    []wgtypes.Peer{},
			expected: "",
			err:      fmt.Errorf(""),
		},
		{
			name:   "Some IPs already used",
			subnet: "192.168.1.0/24",
			peers: []wgtypes.Peer{
				{
					AllowedIPs: []net.IPNet{
						{IP: net.ParseIP("192.168.1.1"), Mask: net.CIDRMask(32, 32)},
						{IP: net.ParseIP("192.168.1.2"), Mask: net.CIDRMask(32, 32)},
					},
				},
			},
			expected: "192.168.1.3",
			err:      nil,
		},
		{
			name:   "All IPs used",
			subnet: "192.168.1.0/30",
			peers: []wgtypes.Peer{
				{
					AllowedIPs: []net.IPNet{
						{IP: net.ParseIP("192.168.1.1"), Mask: net.CIDRMask(32, 32)},
						{IP: net.ParseIP("192.168.1.2"), Mask: net.CIDRMask(32, 32)},
					},
				},
			},
			expected: "",
			err:      fmt.Errorf(""),
		},
		{
			name:     "Edge case with network and broadcast addresses",
			subnet:   "192.168.1.0/30",
			peers:    []wgtypes.Peer{},
			expected: "192.168.1.1",
			err:      nil,
		},
		{
			name:     "IPv6 subnet with no peers",
			subnet:   "2001:db8::/64",
			peers:    []wgtypes.Peer{},
			expected: "2001:db8::1",
			err:      nil,
		},
		{
			name:   "IPv6 subnet with all IPs used",
			subnet: "2001:db8::0/126",
			peers: []wgtypes.Peer{
				{
					AllowedIPs: []net.IPNet{
						{IP: net.ParseIP("2001:db8::1"), Mask: net.CIDRMask(128, 128)},
						{IP: net.ParseIP("2001:db8::2"), Mask: net.CIDRMask(128, 128)},
					},
				},
			},
			expected: "",
			err:      fmt.Errorf(""),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, subnet, err := net.ParseCIDR(tt.subnet)
			if err != nil {
				t.Fatalf("Failed to parse subnet %q: %v", tt.subnet, err)
			}

			ip, err := getSubnetAvailableIP(*subnet, tt.peers)
			if err != nil && tt.err == nil {
				t.Fatalf("Expected no error, but got %v", err)
			}
			if err == nil && tt.err != nil {
				t.Fatalf("Expected error %v, but got none", tt.err)
			}
			if err == nil && tt.expected != ip.String() {
				t.Fatalf("Expected IP %q, but got %q", tt.expected, ip.String())
			}
		})
	}
}

func TestGetInterfaceAddresses(t *testing.T) {
	tests := []struct {
		name          string
		interfaceName string
		ipVersion     IPVersion
		ipsToAdd      []string
		expected      []string
		expectErr     bool
	}{
		{
			name:          "No IPs assigned to interface",
			interfaceName: "bywg0testif",
			ipVersion:     AllIPs,
			ipsToAdd:      []string{},
			expected:      []string{},
			expectErr:     false,
		},
		{
			name:          "Only IPv4 addresses test",
			interfaceName: "bywg1testif",
			ipVersion:     IPv4,
			ipsToAdd: []string{
				"192.168.1.1/24",
				"192.168.1.2/24",
			},
			expected: []string{
				"192.168.1.1/24",
				"192.168.1.2/24",
			},
			expectErr: false,
		},
		{
			name:          "Only IPv6 addresses test",
			interfaceName: "bywg2testif",
			ipVersion:     IPv6,
			ipsToAdd: []string{
				"2001:db8::1/64",
				"2001:db8::2/64",
			},
			expected: []string{
				"2001:db8::1/64",
				"2001:db8::2/64",
			},
			expectErr: false,
		},
		{
			name:          "Add IPv4 addresses check for IPv6",
			interfaceName: "bywg3testif",
			ipVersion:     IPv6,
			ipsToAdd: []string{
				"192.168.3.1/24",
				"192.168.3.2/24",
			},
			expected:  []string{},
			expectErr: false,
		},
		{
			name:          "Add IPv6 addresses check for IPv4",
			interfaceName: "bywg4testif",
			ipVersion:     IPv4,
			ipsToAdd: []string{
				"2001:db8::40/64",
			},
			expected:  []string{},
			expectErr: false,
		},
		{
			name:          "Mixed IPv4 and IPv6 addresses",
			interfaceName: "bywg5testif",
			ipVersion:     AllIPs,
			ipsToAdd: []string{
				"192.168.5.1/24",
				"2001:db8::50/64",
			},
			expected: []string{
				"192.168.5.1/24",
				"2001:db8::50/64",
			},
			expectErr: false,
		},
		{
			name:          "Interface with both IPv4 and IPv6 but filter only IPv6",
			interfaceName: "bywg6testif",
			ipVersion:     IPv6,
			ipsToAdd: []string{
				"192.168.6.1/24",
				"2001:db8::60/64",
			},
			expected: []string{
				"2001:db8::60/64",
			},
			expectErr: false,
		},
		{
			name:          "Interface with both IPv4 and IPv6 but filter only IPv4",
			interfaceName: "bywg7testif",
			ipVersion:     IPv6,
			ipsToAdd: []string{
				"192.168.7.1/24",
				"2001:db8::70/64",
			},
			expected: []string{
				"2001:db8::70/64",
			},
			expectErr: false,
		},
		{
			name:          "Check for missing interface error",
			interfaceName: "bywg8testif",
			ipVersion:     AllIPs,
			ipsToAdd: []string{
				"192.168.8.1/24",
				"2001:db8::80/64",
			},
			expected:  []string{},
			expectErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// create dummy interface
			if !test.expectErr {
				cmd := fmt.Sprintf("ip link add dev %s type dummy", test.interfaceName)
				output, err := exec.Command("sh", "-c", cmd).CombinedOutput()
				if err != nil {
					t.Fatalf("Failed to create dummy interface: %v: %s", err, output)
				}

				// delete dummy interface on exit
				defer func() {
					cmd := fmt.Sprintf("ip link delete dev %s", test.interfaceName)
					output, err := exec.Command("sh", "-c", cmd).CombinedOutput()
					if err != nil {
						t.Fatalf("Failed to delete dummy interface: %v: %s", err, output)
					}
				}()

				// add IP addresses to dummy interface
				for _, ip := range test.ipsToAdd {
					cmd := fmt.Sprintf("ip addr add %s dev %s", ip, test.interfaceName)
					output, err := exec.Command("sh", "-c", cmd).CombinedOutput()
					if err != nil {
						t.Fatalf("Failed to add IP address to dummy interface: %v, output: %s", err, output)
					}
				}
			}

			// get interface addresses
			result, err := getInterfaceAddresses(test.interfaceName, test.ipVersion)
			if (err != nil) != test.expectErr {
				t.Fatalf("Expected error=%v, but got error=%v", test.expectErr, err)
			}

			if len(result) != len(test.expected) {
				t.Fatalf("Expected %d addresses, but got %d", len(test.expected), len(result))
			}

			// convert result to a set for unordered comparison
			expectedSet := make(map[string]bool)
			for _, exp := range test.expected {
				expectedSet[exp] = true
			}

			// comapred result with expected addresses
			for _, res := range result {
				if !expectedSet[res.String()] {
					t.Fatalf("Unexpected IP address %v", res)
				}
			}
		})
	}
}
