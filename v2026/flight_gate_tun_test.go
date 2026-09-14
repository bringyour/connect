package connect

// M7 of FLIGHTGATEFIX.md §5: the gVisor tun's outbound queue is drained by
// one reader goroutine; when that goroutine is itself inside an inbound
// injection (SendPacket -> race-commit delivery -> Tun.Write -> netstack
// reply), the reply's WritePackets waits for queue space only the blocked
// reader can free. The socks client and the hosted server proxy both have
// that shape.

import (
	"context"
	"net"
	"testing"
	"time"
)

// flightGateClosedPortSyn is a checksummed IPv4 TCP SYN from a remote host to
// a closed port on the tun's own address, which netstack answers with a RST.
func flightGateClosedPortSyn(tun *Tun, sourcePort int) []byte {
	local := tun.LocalAddresses()[0]
	return ipOosTcpPacketSequence(
		&IpPath{
			Version:         4,
			Protocol:        IpProtocolTcp,
			SourceIp:        net.IPv4(192, 0, 2, 1),
			SourcePort:      sourcePort,
			DestinationIp:   net.IP(local.AsSlice()),
			DestinationPort: 1,
		},
		tcpFlagSyn,
		uint32(sourcePort),
		nil,
	)
}

// M7. With the outbound queue full and its reader parked, an injection that
// makes netstack reply must still return: the injecting goroutine is the
// reader in production. Expected red on the tree this was written against:
// WritePackets waits without bound. Candidate R1 removes the reentrancy in
// the multi-client; a bounded wait in the tun (#209's shape) is the guard
// this test also characterises.
func TestTunInjectFromReaderGoroutineDoesNotDeadlock(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const queueSize = 8
	tun, err := CreateTun(ctx, DefaultTunSettingsWithBufferSize(queueSize))
	if err != nil {
		t.Fatal(err)
	}
	defer tun.Close()

	// precondition: one SYN to a closed port yields exactly one outbound reply
	if _, err := tun.Write(flightGateClosedPortSyn(tun, 40000)); err != nil {
		t.Fatal(err)
	}
	readDone := make(chan error, 1)
	go func() {
		_, readErr := tun.Read()
		readDone <- readErr
	}()
	select {
	case readErr := <-readDone:
		if readErr != nil {
			t.Fatal(readErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a SYN to a closed port produced no reply; the reproduction cannot run")
	}

	// fill the outbound queue with replies nobody reads
	for sourcePort := 40001; sourcePort <= 40000+queueSize; sourcePort += 1 {
		if _, err := tun.Write(flightGateClosedPortSyn(tun, sourcePort)); err != nil {
			t.Fatal(err)
		}
	}
	// the reader is parked inside this injection; it must still return
	writeDone := make(chan error, 1)
	go func() {
		_, writeErr := tun.Write(flightGateClosedPortSyn(tun, 41000))
		writeDone <- writeErr
	}()
	select {
	case writeErr := <-writeDone:
		if writeErr != nil {
			t.Fatal(writeErr)
		}
	case <-time.After(time.Second):
		t.Fatal("inbound Write blocked on the full outbound queue: the reader goroutine would deadlock the stack")
	}
	// drain so Close does not wait on the stack
	go func() {
		for {
			if _, readErr := tun.Read(); readErr != nil {
				return
			}
		}
	}()
}
