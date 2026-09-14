package connect

import (
	"net"
	"testing"
	"time"
)

func idleTestPath(protocol IpProtocol) *IpPath {
	return &IpPath{
		Version:         4,
		Protocol:        protocol,
		SourceIp:        net.ParseIP("10.11.12.13"),
		SourcePort:      54321,
		DestinationIp:   net.ParseIP("93.184.216.34"),
		DestinationPort: 443,
	}
}

// tcp flows get the longer bound; udp keeps the tighter one
func TestSequenceIdleTimeoutSplitsByProtocol(t *testing.T) {
	mc := &RemoteUserNatMultiClient{
		settings: &MultiClientSettings{
			SequenceIdleTimeout:    120 * time.Second,
			TcpSequenceIdleTimeout: 600 * time.Second,
		},
	}

	AssertEqual(t, mc.sequenceIdleTimeout(idleTestPath(IpProtocolTcp)), 600*time.Second)
	AssertEqual(t, mc.sequenceIdleTimeout(idleTestPath(IpProtocolUdp)), 120*time.Second)
}

// zero restores the previous single-value behavior for every protocol
func TestSequenceIdleTimeoutDisabledFallsBack(t *testing.T) {
	mc := &RemoteUserNatMultiClient{
		settings: &MultiClientSettings{
			SequenceIdleTimeout:    120 * time.Second,
			TcpSequenceIdleTimeout: 0,
		},
	}

	AssertEqual(t, mc.sequenceIdleTimeout(idleTestPath(IpProtocolTcp)), 120*time.Second)
	AssertEqual(t, mc.sequenceIdleTimeout(idleTestPath(IpProtocolUdp)), 120*time.Second)
}

// a nil path must not panic -- it falls back to the shared bound
func TestSequenceIdleTimeoutNilPath(t *testing.T) {
	mc := &RemoteUserNatMultiClient{
		settings: &MultiClientSettings{
			SequenceIdleTimeout:    120 * time.Second,
			TcpSequenceIdleTimeout: 600 * time.Second,
		},
	}

	AssertEqual(t, mc.sequenceIdleTimeout(nil), 120*time.Second)
}

func TestDefaultMultiClientSettingsTcpIdleTimeoutIsLonger(t *testing.T) {
	settings := DefaultMultiClientSettings()
	AssertEqual(t, settings.SequenceIdleTimeout < settings.TcpSequenceIdleTimeout, true)
	// traditional vpns hold tcp nat state for at least 5 minutes
	AssertEqual(t, 5*time.Minute <= settings.TcpSequenceIdleTimeout, true)
}
