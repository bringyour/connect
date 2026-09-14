package connect

import (
	"context"
	"math"
	"testing"
)

func tcpLifecycleControl(sequence uint32, ackSequence uint32, payload int, ack bool, fin bool, rst bool) tcpControlObservation {
	return tcpControlObservation{
		sequenceNumber:    sequence,
		ackSequenceNumber: ackSequence,
		payloadByteCount:  uint32(payload),
		ack:               ack,
		fin:               fin,
		rst:               rst,
		valid:             true,
	}
}

func TestTcpLifecycleRetiresOnlyAfterBothFinsAreAcknowledged(t *testing.T) {
	update := newMultiClientChannelUpdate(context.Background(), flowReaperTestPath(4, IpProtocolTcp, 43001))
	defer update.Close()

	// Local active close. The FIN shares a segment with five payload bytes,
	// so the remote ACK must cover sequence 106, not merely 101.
	if update.observeTcpControl(tcpLifecycleControl(100, 900, 5, true, true, false), false) {
		t.Fatal("one-sided FIN retired a legal half-close")
	}
	if update.observeTcpControl(tcpLifecycleControl(900, 105, 0, true, false, false), true) {
		t.Fatal("ACK below the FIN+payload edge retired the flow")
	}
	if update.observeTcpControl(tcpLifecycleControl(900, 106, 0, true, false, false), true) {
		t.Fatal("one acknowledged FIN retired the flow before the peer FIN")
	}
	if update.observeTcpControl(tcpLifecycleControl(900, 106, 2, true, true, false), true) {
		t.Fatal("both FINs retired the flow before the second FIN was acknowledged")
	}
	if !update.observeTcpControl(tcpLifecycleControl(106, 903, 0, true, false, false), false) {
		t.Fatal("fully acknowledged FIN handshake did not retire the flow")
	}
}

func TestTcpLifecycleHandlesSynchronousAckBeforeFinCommit(t *testing.T) {
	update := newMultiClientChannelUpdate(context.Background(), flowReaperTestPath(4, IpProtocolTcp, 43002))
	defer update.Close()

	// A hardwired or very fast provider can deliver the ACK before the send
	// call returns and records the successfully-sent FIN. Preserve that ACK.
	if update.observeTcpControl(tcpLifecycleControl(700, 501, 0, true, false, false), true) {
		t.Fatal("standalone early ACK retired the flow")
	}
	if update.observeTcpControl(tcpLifecycleControl(500, 700, 0, true, true, false), false) {
		t.Fatal("early ACK plus one FIN retired a half-close")
	}
	if update.observeTcpControl(tcpLifecycleControl(700, 501, 0, true, true, false), true) {
		t.Fatal("peer FIN retired before its ACK")
	}
	if !update.observeTcpControl(tcpLifecycleControl(501, 701, 0, true, false, false), false) {
		t.Fatal("stored early ACK was not applied when the FIN committed")
	}
}

func TestTcpLifecycleRstAndSequenceWrap(t *testing.T) {
	rstUpdate := newMultiClientChannelUpdate(context.Background(), flowReaperTestPath(4, IpProtocolTcp, 43003))
	if !rstUpdate.observeTcpControl(tcpLifecycleControl(1, 1, 0, true, false, true), true) {
		t.Fatal("delivered RST did not retire the flow immediately")
	}
	rstUpdate.Close()

	wrapUpdate := newMultiClientChannelUpdate(context.Background(), flowReaperTestPath(4, IpProtocolTcp, 43004))
	defer wrapUpdate.Close()
	if wrapUpdate.observeTcpControl(tcpLifecycleControl(math.MaxUint32, 10, 0, true, true, false), false) {
		t.Fatal("wrapped one-sided FIN retired the flow")
	}
	if wrapUpdate.observeTcpControl(tcpLifecycleControl(10, 0, 0, true, true, false), true) {
		t.Fatal("wrapped FIN pair retired before the peer FIN ACK")
	}
	if !wrapUpdate.observeTcpControl(tcpLifecycleControl(0, 11, 0, true, false, false), false) {
		t.Fatal("wrapped FIN acknowledgement was ordered incorrectly")
	}
}

func TestParseIpPathPreservesTcpPayloadSizeForFin(t *testing.T) {
	payload := []byte("final application bytes")
	packet := smtpTestTcp4Packet(0x11, 1234, 5678, payload) // FIN|ACK
	ipPath, parsedPayload, err := ParseIpPathWithPayload(packet)
	if err != nil {
		t.Fatalf("parse FIN packet: %v", err)
	}
	if !ipPath.Fin || !ipPath.Ack {
		t.Fatalf("parsed flags: fin=%v ack=%v, want both", ipPath.Fin, ipPath.Ack)
	}
	if ipPath.TcpPayloadByteCount != len(payload) {
		t.Fatalf("TCP payload byte count = %d, want %d", ipPath.TcpPayloadByteCount, len(payload))
	}
	if string(parsedPayload) != string(payload) {
		t.Fatalf("parsed payload = %q, want %q", parsedPayload, payload)
	}
}
