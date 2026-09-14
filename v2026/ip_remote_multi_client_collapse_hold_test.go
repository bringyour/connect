package connect

import (
	"context"
	"net"
	"testing"
	"time"
)

func collapseTestPacket(seq uint32, ack uint32, payload int, syn bool, rst bool) *parsedPacket {
	return &parsedPacket{
		payload: make([]byte, payload),
		ipPath: &IpPath{
			Version:           4,
			Protocol:          IpProtocolTcp,
			SourceIp:          net.ParseIP("10.11.12.13"),
			SourcePort:        54321,
			DestinationIp:     net.ParseIP("93.184.216.34"),
			DestinationPort:   443,
			SequenceNumber:    seq,
			AckSequenceNumber: ack,
			TcpWindowSize:     32 * 1024,
			Syn:               syn,
			Rst:               rst,
		},
	}
}

func collapseTestClient(maxHold time.Duration) (*RemoteUserNatMultiClient, *multiClientChannelUpdate) {
	client := &RemoteUserNatMultiClient{
		settings: &MultiClientSettings{
			TcpCollapsePrevention: true,
			TcpCollapseMaxHold:    maxHold,
		},
	}
	update := newMultiClientChannelUpdate(context.Background(), collapseTestPacket(0, 0, 0, false, false).ipPath)
	return client, update
}

// Uses a selected compatibility client whose IP packets require Transfer ACKs.
func collapseTestCanSend(
	client *RemoteUserNatMultiClient,
	update *multiClientChannelUpdate,
	packet *parsedPacket,
) bool {
	return client.canSendPacket(packet, update, &multiClientChannel{
		settings: client.settings,
	})
}

// a retransmit inside the hold is still collapsed, and the same retransmit
// after the hold is admitted so a stalled flow can recover without waiting out
// the 30s AckTimeout
func TestTcpCollapseHoldReleasesRetransmit(t *testing.T) {
	maxHold := 50 * time.Millisecond
	client, update := collapseTestClient(maxHold)

	// commit a data packet: this advances the sequence state
	first := collapseTestPacket(1000, 5000, 100, false, false)
	AssertEqual(t, collapseTestCanSend(client, update, first), true)
	update.updateSequence(first)

	// an identical retransmit is collapsed while inside the hold
	retransmit := collapseTestPacket(1000, 5000, 100, false, false)
	AssertEqual(t, collapseTestCanSend(client, update, retransmit), false)

	time.Sleep(maxHold + 20*time.Millisecond)

	// past the hold, the retransmit is let through
	AssertEqual(t, collapseTestCanSend(client, update, retransmit), true)

	// and the window restarts, so the backlog is not released all at once
	AssertEqual(t, collapseTestCanSend(client, update, retransmit), false)
}

// zero must reproduce the previous behavior exactly: retransmits collapsed
// indefinitely, however long the flow has been stuck
func TestTcpCollapseHoldDisabled(t *testing.T) {
	client, update := collapseTestClient(0)

	first := collapseTestPacket(1000, 5000, 100, false, false)
	AssertEqual(t, collapseTestCanSend(client, update, first), true)
	update.updateSequence(first)

	retransmit := collapseTestPacket(1000, 5000, 100, false, false)
	AssertEqual(t, collapseTestCanSend(client, update, retransmit), false)

	time.Sleep(60 * time.Millisecond)

	AssertEqual(t, collapseTestCanSend(client, update, retransmit), false)
}

// the hold must not interfere with packets that legitimately advance the flow,
// nor with syn/rst which always pass
func TestTcpCollapseHoldAllowsProgress(t *testing.T) {
	client, update := collapseTestClient(50 * time.Millisecond)

	first := collapseTestPacket(1000, 5000, 100, false, false)
	AssertEqual(t, collapseTestCanSend(client, update, first), true)
	update.updateSequence(first)

	// new data later in sequence space
	next := collapseTestPacket(1100, 5000, 100, false, false)
	AssertEqual(t, collapseTestCanSend(client, update, next), true)

	// a pure ack that advances the ack number
	ack := collapseTestPacket(1100, 6000, 0, false, false)
	AssertEqual(t, collapseTestCanSend(client, update, ack), true)

	// syn and rst are never collapsed
	AssertEqual(t, collapseTestCanSend(client, update, collapseTestPacket(1000, 5000, 100, true, false)), true)
	AssertEqual(t, collapseTestCanSend(client, update, collapseTestPacket(1000, 5000, 100, false, true)), true)
}

func TestTcpCollapseAllowsReceiveWindowCloseAndReopen(t *testing.T) {
	client, update := collapseTestClient(time.Hour)

	open := collapseTestPacket(1000, 5000, 0, false, false)
	if !collapseTestCanSend(client, update, open) {
		t.Fatal("initial open-window ACK was not admitted")
	}
	update.updateSequence(open)

	closed := collapseTestPacket(1000, 5000, 0, false, false)
	closed.ipPath.TcpWindowSize = 0
	if !collapseTestCanSend(client, update, closed) {
		t.Fatal("zero-window update was collapsed as a duplicate ACK")
	}
	update.updateSequence(closed)

	reopened := collapseTestPacket(1000, 5000, 0, false, false)
	if !collapseTestCanSend(client, update, reopened) {
		t.Fatal("receive-window reopen was collapsed as a duplicate ACK")
	}
	update.updateSequence(reopened)
	if collapseTestCanSend(client, update, reopened) {
		t.Fatal("identical reopened-window ACK bypassed collapse prevention")
	}
}

func TestTcpCollapseAccountsForFinSequenceSpace(t *testing.T) {
	client, update := collapseTestClient(time.Hour)

	data := collapseTestPacket(1000, 5000, 100, false, false)
	if !collapseTestCanSend(client, update, data) {
		t.Fatal("initial data was not admitted")
	}
	update.updateSequence(data)

	fin := collapseTestPacket(1100, 5000, 0, false, false)
	fin.ipPath.Fin = true
	if !collapseTestCanSend(client, update, fin) {
		t.Fatal("FIN at the data edge was collapsed as a retransmission")
	}
	update.updateSequence(fin)
	if got := update.sequenceNumber; got != 1101 {
		t.Fatalf("sequence after FIN = %d, want 1101", got)
	}
	if collapseTestCanSend(client, update, fin) {
		t.Fatal("duplicate FIN bypassed collapse prevention")
	}
}

// with collapse prevention off entirely, the hold is irrelevant and everything
// passes as before
func TestTcpCollapseHoldIgnoredWhenPreventionOff(t *testing.T) {
	client := &RemoteUserNatMultiClient{
		settings: &MultiClientSettings{
			TcpCollapsePrevention: false,
			TcpCollapseMaxHold:    50 * time.Millisecond,
		},
	}
	update := newMultiClientChannelUpdate(context.Background(), collapseTestPacket(0, 0, 0, false, false).ipPath)

	first := collapseTestPacket(1000, 5000, 100, false, false)
	AssertEqual(t, collapseTestCanSend(client, update, first), true)
	update.updateSequence(first)

	AssertEqual(t, collapseTestCanSend(client, update, collapseTestPacket(1000, 5000, 100, false, false)), true)
}

// udp is unaffected by the tcp collapse path
func TestTcpCollapseHoldUdpUnaffected(t *testing.T) {
	client, update := collapseTestClient(50 * time.Millisecond)

	udp := collapseTestPacket(1000, 5000, 100, false, false)
	udp.ipPath.Protocol = IpProtocolUdp
	AssertEqual(t, collapseTestCanSend(client, update, udp), true)
	AssertEqual(t, collapseTestCanSend(client, update, udp), true)
}

// Direct and platform clients both retain Transfer recovery for TCP, so both
// apply the same bounded collapse policy. An unbound flow remains conservative
// and retains that policy too.
func TestTcpCollapseIncludesDirectSelectedClient(t *testing.T) {
	client, update := collapseTestClient(time.Hour)
	first := collapseTestPacket(1000, 5000, 100, false, false)
	acknowledgedClient := &multiClientChannel{
		settings:           client.settings,
		performanceProfile: &PerformanceProfile{},
	}
	directClient := &multiClientChannel{
		settings: client.settings,
		performanceProfile: &PerformanceProfile{
			AllowDirect: true,
		},
	}

	if !client.canSendPacket(first, update, acknowledgedClient) {
		t.Fatal("initial acknowledged packet was not admitted")
	}
	update.updateSequence(first)
	retransmit := collapseTestPacket(1000, 5000, 100, false, false)

	if client.canSendPacket(retransmit, update, acknowledgedClient) {
		t.Fatal("acknowledged Transfer retransmit bypassed collapse prevention")
	}
	if client.canSendPacket(retransmit, update, directClient) {
		t.Fatal("direct acknowledged Transfer retransmit bypassed collapse prevention")
	}
	if client.canSendPacket(retransmit, update, directClient) {
		t.Fatal("second direct acknowledged Transfer retransmit bypassed collapse prevention")
	}
	if client.canSendPacket(retransmit, update, nil) {
		t.Fatal("unbound flow changed the established collapse policy")
	}
}

func TestDefaultMultiClientSettingsSetsTcpCollapseMaxHold(t *testing.T) {
	maxHold := DefaultMultiClientSettings().TcpCollapseMaxHold
	AssertEqual(t, 0 < maxHold, true)
	// must stay well under the AckTimeout it exists to preempt
	AssertEqual(t, maxHold < DefaultMultiClientSettings().AckTimeout, true)
}
