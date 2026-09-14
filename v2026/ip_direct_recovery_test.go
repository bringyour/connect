// This file verifies that TCP retains end-to-end Transfer recovery while
// UDP/ICMP preserve their configured datagram policy.
package connect

import (
	"testing"
	"time"
)

// TCP is acknowledged even on a direct-capable carrier. Only the historical
// UDP collapse option changes a non-direct datagram route.
func TestIpPacketTransferAckRequired(t *testing.T) {
	tests := []struct {
		name                  string
		protocol              IpProtocol
		allowDirect           bool
		udpCollapsePrevention bool
		ackRequired           bool
	}{
		{
			name:        "direct tcp",
			protocol:    IpProtocolTcp,
			allowDirect: true,
			ackRequired: true,
		},
		{
			name:        "direct udp",
			protocol:    IpProtocolUdp,
			allowDirect: true,
			ackRequired: false,
		},
		{
			name:        "direct icmp",
			protocol:    IpProtocolIcmp,
			allowDirect: true,
			ackRequired: false,
		},
		{
			name:        "platform tcp",
			protocol:    IpProtocolTcp,
			ackRequired: true,
		},
		{
			name:        "platform udp",
			protocol:    IpProtocolUdp,
			ackRequired: true,
		},
		{
			name:                  "platform udp collapse",
			protocol:              IpProtocolUdp,
			udpCollapsePrevention: true,
			ackRequired:           false,
		},
	}
	for _, test := range tests {
		ackRequired := ipPacketTransferAckRequired(
			&IpPath{Protocol: test.protocol},
			test.allowDirect,
			test.udpCollapsePrevention,
		)
		if ackRequired != test.ackRequired {
			t.Fatalf(
				"%s ack required = %t, want %t",
				test.name,
				ackRequired,
				test.ackRequired,
			)
		}
	}
	if !ipPacketTransferAckRequired(nil, false, true) {
		t.Fatal("missing IP metadata disabled compatibility acknowledgement")
	}
}

// The final packet-to-Transfer boundary must reject an explicit NoAck hint for
// TCP. This keeps a new caller from bypassing the normal route-policy helper.
func TestTcpTransferAckCannotBeDisabledAtSendBoundary(t *testing.T) {
	for _, test := range []struct {
		name      string
		ipPath    *IpPath
		requested bool
		want      bool
	}{
		{name: "tcp noack request", ipPath: &IpPath{Protocol: IpProtocolTcp}, want: true},
		{name: "udp noack request", ipPath: &IpPath{Protocol: IpProtocolUdp}, want: false},
		{name: "udp ack request", ipPath: &IpPath{Protocol: IpProtocolUdp}, requested: true, want: true},
		{name: "missing metadata", want: true},
	} {
		if got := ipPacketTransferAckForRequest(test.ipPath, test.requested); got != test.want {
			t.Errorf("%s ACK=%t, want %t", test.name, got, test.want)
		}
	}

	var observed []bool
	client := &multiClientChannel{
		sendGroupForTest: func(
			_ *parsedPacketGroup,
			_ time.Duration,
			ack bool,
		) (bool, error) {
			observed = append(observed, ack)
			return true, nil
		},
	}
	for _, ipPath := range []*IpPath{
		{Protocol: IpProtocolTcp},
		{Protocol: IpProtocolUdp},
		nil,
	} {
		if success, err := client.SendGroupDetailedWithAck(
			&parsedPacketGroup{ipPath: ipPath},
			0,
			false,
		); err != nil || !success {
			t.Fatalf("send boundary = %t, %v", success, err)
		}
	}
	if len(observed) != 3 || !observed[0] || observed[1] || !observed[2] {
		t.Fatalf("observed ACK policies=%v, want [true false true]", observed)
	}
}
