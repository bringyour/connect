package connect

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

func dialFailureTestSyn() *parsedTcp {
	return &parsedTcp{
		sourceIp:        net.IPv4(10, 0, 0, 1).To4(),
		destinationIp:   net.IPv4(203, 0, 113, 7).To4(),
		sourcePort:      40001,
		destinationPort: 443,
		syn:             true,
		seq:             1000,
		windowSize:      65535,
	}
}

// TestTcpUpstreamDialFailureRejectsSource verifies an upstream refusal cannot
// leave the source retransmitting a silent syn on an exponential schedule.
func TestTcpUpstreamDialFailureRejectsSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultTcpBufferSettingsWithBufferSize(8)
	settings.DialContextSettings = &DialContextSettings{
		DialContext: func(dialCtx context.Context, network string, addr string) (net.Conn, error) {
			return nil, errors.New("upstream unavailable")
		},
	}
	receives := make(chan []byte, 1)
	buffer := NewTcp4Buffer(
		ctx,
		func(
			source TransferPath,
			provideMode protocol.ProvideMode,
			ipPath *IpPath,
			packet []byte,
		) {
			receives <- append([]byte(nil), packet...)
		},
		settings,
	)
	packet := MessagePoolGet(Ipv4HeaderSizeWithoutExtensions + TcpHeaderSizeWithoutExtensions)
	success, err := buffer.send(
		SourceId(NewId()),
		protocol.ProvideMode_Network,
		dialFailureTestSyn(),
		-1,
		packet,
	)
	if err != nil {
		MessagePoolReturn(packet)
		t.Fatalf("send syn: %v", err)
	}
	if !success {
		MessagePoolReturn(packet)
		t.Fatal("syn was not accepted")
	}

	select {
	case received := <-receives:
		// the answer is classified (see classifyDialFailure): a refused
		// destination gets an honest RST+ACK, and everything else -- this
		// generic error included -- gets the capacity-class icmp
		// destination-unreachable whose embed names the failed flow, which
		// newer clients intercept as the provider dial-failure signal and
		// older ones drop at parse. Either way the source is answered, never
		// left in syn-retransmit silence.
		egressIpPath, ok := ipParseIcmpUnreachable(received)
		if !ok {
			t.Fatal("dial rejection did not parse as the icmp dial-failure signal")
		}
		if egressIpPath.SourcePort != 40001 || egressIpPath.DestinationPort != 443 {
			t.Fatalf("dial-failure embed ports=%d->%d, want 40001->443",
				egressIpPath.SourcePort, egressIpPath.DestinationPort)
		}
		if !egressIpPath.DestinationIp.Equal(net.IPv4(203, 0, 113, 7).To4()) {
			t.Fatalf("dial-failure embed destination=%s, want 203.0.113.7", egressIpPath.DestinationIp)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("upstream dial failure was silent")
	}
}

// TestTcpCanceledUpstreamDialDoesNotEmitReset distinguishes provider teardown
// from an upstream refusal: cancellation must release setup state without
// creating a new return packet.
func TestTcpCanceledUpstreamDialDoesNotEmitReset(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	dialStarted := make(chan struct{})
	settings := DefaultTcpBufferSettingsWithBufferSize(8)
	settings.DialContextSettings = &DialContextSettings{
		DialContext: func(dialCtx context.Context, network string, addr string) (net.Conn, error) {
			close(dialStarted)
			<-dialCtx.Done()
			return nil, dialCtx.Err()
		},
	}
	receives := make(chan struct{}, 1)
	buffer := NewTcp4Buffer(
		ctx,
		func(
			source TransferPath,
			provideMode protocol.ProvideMode,
			ipPath *IpPath,
			packet []byte,
		) {
			receives <- struct{}{}
		},
		settings,
	)
	packet := MessagePoolGet(Ipv4HeaderSizeWithoutExtensions + TcpHeaderSizeWithoutExtensions)
	success, err := buffer.send(
		SourceId(NewId()),
		protocol.ProvideMode_Network,
		dialFailureTestSyn(),
		-1,
		packet,
	)
	if err != nil {
		cancel()
		MessagePoolReturn(packet)
		t.Fatalf("send syn: %v", err)
	}
	if !success {
		cancel()
		MessagePoolReturn(packet)
		t.Fatal("syn was not accepted")
	}

	select {
	case <-dialStarted:
	case <-time.After(2 * time.Second):
		cancel()
		t.Fatal("upstream dial did not start")
	}
	cancel()
	select {
	case <-receives:
		t.Fatal("provider cancellation emitted a reset")
	case <-time.After(100 * time.Millisecond):
	}
}
