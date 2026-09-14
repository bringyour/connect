package connect

import (
	"errors"
	"fmt"
	"syscall"
	"testing"
)

func dialFailureTestPath(protocol IpProtocol, version int) *IpPath {
	if protocol == IpProtocolTcp {
		// carries Protocol=tcp plus a non-zero sequence number, so the round
		// trip exercises the tcp embed the same way the provider does.
		return icmpTcpTestPath(version)
	}
	return udpTestPath(version)
}

// classifyDialFailure is the single home of the dial-failure errno logic: both
// ip.go dial sites and this test call it, so the test never restates the
// classification. ECONNREFUSED on tcp is the one case that becomes a RST (the
// destination refused); everything else -- and all of udp, ECONNREFUSED
// included -- is capacity-class and gets an icmp unreachable, with unrecognized
// errors defaulting to capacity deliberately.
func TestClassifyDialFailure(t *testing.T) {
	cases := []struct {
		name string
		err  error
		// wantTcp is the action for a tcp path; udp is always unreachable.
		wantTcp dialFailureAction
	}{
		{"econnrefused", syscall.ECONNREFUSED, dialFailureRst},
		// real net dials wrap the errno (*net.OpError -> *os.SyscallError ->
		// syscall.Errno); errors.Is must still see through it.
		{"econnrefused wrapped", fmt.Errorf("dial tcp 93.184.216.34:443: connect: %w", syscall.ECONNREFUSED), dialFailureRst},
		{"etimedout", syscall.ETIMEDOUT, dialFailureUnreachable},
		{"emfile", syscall.EMFILE, dialFailureUnreachable},
		{"eaddrnotavail", syscall.EADDRNOTAVAIL, dialFailureUnreachable},
		{"generic", errors.New("some non-syscall dial failure"), dialFailureUnreachable},
	}

	for _, protocol := range []IpProtocol{IpProtocolTcp, IpProtocolUdp} {
		for _, version := range []int{4, 6} {
			for _, c := range cases {
				label := fmt.Sprintf("%s/v%d/%s", protocol, version, c.name)

				ipPath := dialFailureTestPath(protocol, version)

				want := c.wantTcp
				if protocol == IpProtocolUdp {
					// udp "dial" never yields a meaningful refusal at connect
					// time, so every error -- ECONNREFUSED included -- is
					// capacity-class.
					want = dialFailureUnreachable
				}

				action, packet := classifyDialFailure(ipPath, c.err)
				if action != want {
					t.Errorf("%s: action = %d, want %d", label, action, want)
					continue
				}

				switch action {
				case dialFailureUnreachable:
					if packet == nil {
						t.Errorf("%s: unreachable action returned no packet", label)
						continue
					}
					// the built signal must round-trip back to the original
					// egress tuple, or the client intercept cannot match the
					// flow it refers to.
					parsed, ok := ipParseIcmpUnreachable(packet)
					if !ok {
						t.Errorf("%s: built unreachable did not parse back", label)
						continue
					}
					if parsed.Protocol != protocol {
						t.Errorf("%s: round-trip protocol = %v, want %v", label, parsed.Protocol, protocol)
					}
					var sameKey bool
					switch version {
					case 4:
						sameKey = parsed.ToIp4Path() == ipPath.ToIp4Path()
					case 6:
						sameKey = parsed.ToIp6Path() == ipPath.ToIp6Path()
					}
					if !sameKey {
						t.Errorf("%s: round-trip flow-map key mismatch: got %s->%s want %s->%s",
							label,
							parsed.SourceHostPort(), parsed.DestinationHostPort(),
							ipPath.SourceHostPort(), ipPath.DestinationHostPort())
					}
				case dialFailureRst:
					if packet != nil {
						t.Errorf("%s: rst action must not carry a packet, got %d bytes", label, len(packet))
					}
				default:
					t.Errorf("%s: unexpected action %d", label, action)
				}
			}
		}
	}
}

// A nil path and an unsupported protocol both classify to none with no packet,
// so the dial sites emit nothing rather than panicking on a malformed flow.
func TestClassifyDialFailureNoSignal(t *testing.T) {
	if action, packet := classifyDialFailure(nil, syscall.ECONNREFUSED); action != dialFailureNone || packet != nil {
		t.Errorf("nil path: action = %d, packet = %v, want none/nil", action, packet)
	}
	for _, ipVersion := range testIpVersions {
		unknown := &IpPath{Version: ipVersion, Protocol: IpProtocolUnknown}
		if action, packet := classifyDialFailure(unknown, errors.New("x")); action != dialFailureNone || packet != nil {
			t.Errorf("v%d unknown protocol: action = %d, packet = %v, want none/nil", ipVersion, action, packet)
		}
	}
}
