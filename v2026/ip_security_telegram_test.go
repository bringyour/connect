package connect

import (
	"context"
	"net"
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

func telegramTestIp(v uint32) net.IP {
	return net.IPv4(byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func TestTelegramCallReflectorRanges(t *testing.T) {
	addressCount := uint64(0)
	var previousHi uint32
	for i, r := range telegramCallReflectorIpv4Ranges {
		if r.hi < r.lo {
			t.Fatalf("range %d is inverted: %08x-%08x", i, r.lo, r.hi)
		}
		if 0 < i && uint64(r.lo) <= uint64(previousHi)+1 {
			t.Fatalf("range %d overlaps, is unsorted, or should be merged", i)
		}
		for endpoint := r.lo; endpoint <= r.hi; endpoint++ {
			if cfaaBlockedIp4(endpoint) {
				t.Fatalf("published Telegram reflector %s is blocklisted; block precedence would disable calls", telegramTestIp(endpoint))
			}
			for port := 596; port <= 599; port++ {
				for _, transport := range []IpProtocol{IpProtocolUdp, IpProtocolTcp} {
					if !isTelegramCallReflector(telegramTestIp(endpoint), port, transport, 4) {
						t.Errorf("published endpoint %s:%d/%s did not match", telegramTestIp(endpoint), port, transport)
					}
				}
			}
		}
		addressCount += uint64(r.hi-r.lo) + 1
		previousHi = r.hi
	}
	if addressCount != 154 {
		t.Fatalf("reflector ranges contain %d addresses, want published snapshot count 154", addressCount)
	}
}

func TestTelegramCallReflectorRejectsNearMisses(t *testing.T) {
	tests := []struct {
		name      string
		ip        string
		port      int
		protocol  IpProtocol
		ipVersion int
	}{
		{"hole 9.11", "91.108.9.11", 596, IpProtocolUdp, 4},
		{"hole 9.18", "91.108.9.18", 597, IpProtocolTcp, 4},
		{"v12 fallback wrong port", "91.108.9.38", 598, IpProtocolTcp, 4},
		{"v12 fallback wrong transport", "91.108.9.38", 595, IpProtocolUdp, 4},
		{"before 13 range", "91.108.13.1", 599, IpProtocolTcp, 4},
		{"after 17 range", "91.108.17.59", 596, IpProtocolUdp, 4},
		{"broader telegram address", "149.154.167.51", 596, IpProtocolUdp, 4},
		{"port below", "91.108.13.2", 595, IpProtocolUdp, 4},
		{"port above", "91.108.13.2", 600, IpProtocolTcp, 4},
		{"wrong protocol", "91.108.13.2", 596, IpProtocolIcmp, 4},
		{"wrong ip version", "91.108.13.2", 596, IpProtocolUdp, 6},
		{"ipv6", "2001:db8::1", 596, IpProtocolUdp, 6},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if isTelegramCallReflector(net.ParseIP(tt.ip), tt.port, tt.protocol, tt.ipVersion) {
				t.Fatal("near miss matched Telegram call exception")
			}
		})
	}
}

func TestTelegramCallV12TcpFallback(t *testing.T) {
	ip := net.ParseIP("91.108.9.38")
	if !isTelegramCallReflector(ip, 595, IpProtocolTcp, 4) {
		t.Fatal("official protocol-v12 TCP fallback did not match")
	}
	if cfaaBlockedIp4(telegramCallV12TcpFallbackIpv4) {
		t.Fatal("official protocol-v12 TCP fallback is blocklisted; block precedence would disable it")
	}
	detector := newCfaaDetector(DefaultCfaaSecurityPolicySettings())
	if got := detector.inspect(ip, 595, IpProtocolTcp, 4); got != cfaaAllow {
		t.Fatalf("protocol-v12 TCP fallback = %s, want allow", cfaaVerdictName(got))
	}
}

func TestCfaaTelegramCallException(t *testing.T) {
	endpoint := net.ParseIP("91.108.13.2")
	for port := 596; port <= 599; port++ {
		for _, transport := range []IpProtocol{IpProtocolUdp, IpProtocolTcp} {
			detector := newCfaaDetector(DefaultCfaaSecurityPolicySettings())
			if got := detector.inspect(endpoint, port, transport, 4); got != cfaaAllow {
				t.Errorf("Telegram %s:%d/%s = %s, want allow", endpoint, port, transport, cfaaVerdictName(got))
			}
		}
	}

	detector := newCfaaDetector(DefaultCfaaSecurityPolicySettings())
	if got := detector.inspect(net.ParseIP("91.108.13.1"), 596, IpProtocolUdp, 4); got != cfaaDrop {
		t.Fatalf("non-reflector privileged endpoint = %s, want drop", cfaaVerdictName(got))
	}

	settings := DefaultCfaaSecurityPolicySettings()
	settings.AllowTelegramCalls = false
	detector = newCfaaDetector(settings)
	if got := detector.inspect(endpoint, 596, IpProtocolUdp, 4); got != cfaaDrop {
		t.Fatalf("disabled Telegram exception = %s, want privileged-port drop", cfaaVerdictName(got))
	}
	if got := detector.inspect(net.ParseIP("91.108.9.38"), 595, IpProtocolTcp, 4); got != cfaaDrop {
		t.Fatalf("disabled protocol-v12 fallback = %s, want privileged-port drop", cfaaVerdictName(got))
	}
}

func TestSecurityPolicyAllowsTelegramCallReflectors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := DefaultSecurityPolicy(ctx)

	for _, transport := range []IpProtocol{IpProtocolUdp, IpProtocolTcp} {
		egress := dmcaPath(transport, 41000, 596, transport == IpProtocolTcp)
		egress.DestinationIp = net.ParseIP("91.108.13.2")
		result, err := policy.InspectEgress(protocol.ProvideMode_Public, egress, encryptedPayload(512))
		if err != nil || result != SecurityPolicyResultAllow {
			t.Fatalf("Telegram egress over %s = (%v, %v), want allow", transport, result, err)
		}

		ingress := egress.Reverse()
		result, err = policy.InspectIngress(protocol.ProvideMode_Public, ingress, encryptedPayload(512))
		if err != nil || result != SecurityPolicyResultAllow {
			t.Fatalf("Telegram ingress over %s = (%v, %v), want allow", transport, result, err)
		}
	}

	fallback := dmcaPath(IpProtocolTcp, 41002, 595, true)
	fallback.DestinationIp = net.ParseIP("91.108.9.38")
	result, err := policy.InspectEgress(protocol.ProvideMode_Public, fallback, encryptedPayload(512))
	if err != nil || result != SecurityPolicyResultAllow {
		t.Fatalf("Telegram protocol-v12 fallback egress = (%v, %v), want allow", result, err)
	}
	result, err = policy.InspectIngress(protocol.ProvideMode_Public, fallback.Reverse(), encryptedPayload(512))
	if err != nil || result != SecurityPolicyResultAllow {
		t.Fatalf("Telegram protocol-v12 fallback ingress = (%v, %v), want allow", result, err)
	}
}

func TestSecurityPolicyCanDisableTelegramCallException(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfaa := DefaultCfaaSecurityPolicySettings()
	cfaa.AllowTelegramCalls = false
	policy := NewSecurityPolicy(
		ctx,
		cfaa,
		DefaultDmcaSecurityPolicySettings(),
		DefaultWebStandardSettings(),
		DefaultSecurityPolicyStatsCollector(),
	)
	path := dmcaPath(IpProtocolUdp, 41001, 596, false)
	path.DestinationIp = net.ParseIP("91.108.13.2")
	result, err := policy.InspectEgress(protocol.ProvideMode_Public, path, encryptedPayload(512))
	if err != nil || result != SecurityPolicyResultDrop {
		t.Fatalf("disabled Telegram exception = (%v, %v), want drop", result, err)
	}
}
