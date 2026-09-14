package connect

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
)

func rstTestPath() *IpPath {
	return &IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		SourceIp:        net.ParseIP("10.11.12.13"),
		SourcePort:      54321,
		DestinationIp:   net.ParseIP("93.184.216.34"),
		DestinationPort: 443,
	}
}

func rstSequenceOf(packet []byte) uint32 {
	return binary.BigEndian.Uint32(packet[Ipv4HeaderSizeWithoutExtensions+4 : Ipv4HeaderSizeWithoutExtensions+8])
}

// a reset toward the source has to carry the sequence the source expects next,
// or RFC 5961 / tcp_validate_incoming discard it and the flow freezes instead
// of resetting
func TestIpOosRstSequenceCarriesSequence(t *testing.T) {
	ipPath := rstTestPath()

	packet, ok := ipOosRstSequence(ipPath, 0xdeadbeef)
	AssertEqual(t, ok, true)
	AssertEqual(t, rstSequenceOf(packet), uint32(0xdeadbeef))

	// the rst flag is still set
	flags := packet[Ipv4HeaderSizeWithoutExtensions+13]
	AssertEqual(t, flags&tcpFlagRst, tcpFlagRst)
}

// the exit-direction reset keeps sequence 0: the provider tears down on any rst
// for a known bufferId with no sequence check, and it has no flow state to
// derive a sequence from
func TestIpOosRstDefaultsToZeroSequence(t *testing.T) {
	packet, ok := ipOosRst(rstTestPath())
	AssertEqual(t, ok, true)
	AssertEqual(t, rstSequenceOf(packet), uint32(0))
}

// udp has no reset either way
func TestIpOosRstSequenceUdp(t *testing.T) {
	ipPath := rstTestPath()
	ipPath.Protocol = IpProtocolUdp

	_, ok := ipOosRstSequence(ipPath, 1234)
	AssertEqual(t, ok, false)
}

// the sequence the teardown uses is the flow's tracked ack number, which is the
// source's RCV.NXT
func TestSourceRstSequenceTracksAckNumber(t *testing.T) {
	update := newMultiClientChannelUpdate(context.Background(), rstTestPath())

	AssertEqual(t, update.sourceRstSequence(), uint32(0))

	syn := &parsedPacket{
		payload: nil,
		ipPath: &IpPath{
			Version:           4,
			Protocol:          IpProtocolTcp,
			SequenceNumber:    1000,
			AckSequenceNumber: 7777,
		},
	}
	update.resetSequence(syn)
	AssertEqual(t, update.sourceRstSequence(), uint32(7777))

	data := &parsedPacket{
		payload: make([]byte, 10),
		ipPath: &IpPath{
			Version:           4,
			Protocol:          IpProtocolTcp,
			SequenceNumber:    1000,
			AckSequenceNumber: 8888,
		},
	}
	update.updateSequence(data)
	AssertEqual(t, update.sourceRstSequence(), uint32(8888))
}

// the teardown path threads the flow's sequence into the reset it builds
func TestTeardownSourcePacketUsesFlowSequence(t *testing.T) {
	mc := &RemoteUserNatMultiClient{settings: &MultiClientSettings{}}

	packet, ok := mc.teardownSourcePacket(rstTestPath(), 4242)
	AssertEqual(t, ok, true)
	AssertEqual(t, rstSequenceOf(packet), uint32(4242))
}
