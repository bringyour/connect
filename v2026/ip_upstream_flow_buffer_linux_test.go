//go:build linux

package connect

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// The buffer rows in the sibling files call the upstream setup directly, which
// decides what that function does and not what a flow gets. Between them and a
// real flow sit the dialer's pre-connect control hook, the choice between it
// and the post-connect subset, and whichever DialContext the host supplied —
// three places a decision can be lost. This drives one TCP flow through the
// NAT to a loopback origin and reads the socket the provider actually proxies
// through.
//
// The assertion follows the policy rather than a constant, so it holds on a
// host tuned either way: where the request does not beat the kernel's
// autotuning ceiling the socket must be left at the kernel's default, and
// where it does the socket must carry exactly what the request obtains.
func TestUpstreamFlowSocketFollowsTheBufferPolicy(t *testing.T) {
	assertMessagePoolOwnership(t)

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	accepted := make(chan net.Conn, 4)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			accepted <- conn
		}
	}()
	t.Cleanup(func() {
		for {
			select {
			case conn := <-accepted:
				conn.Close()
			default:
				return
			}
		}
	})

	upstream := make(chan [2]int, 4)
	settings := DefaultLocalUserNatSettings()
	settings.TcpBufferSettings.afterUpstreamConnectForTest = func(tcpConn *net.TCPConn) {
		upstream <- [2]int{
			tcpSocketReceiveBufferSize(t, tcpConn),
			tcpSocketSendBufferSize(t, tcpConn),
		}
	}
	localUserNat := NewLocalUserNat(t.Context(), "upstream-flow-buffer-test", settings)
	t.Cleanup(func() {
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		if err := localUserNat.CloseAndWait(closeCtx); err != nil {
			t.Errorf("close the local user NAT: %v", err)
		}
	})

	originAddr := listener.Addr().(*net.TCPAddr)
	syn := MessagePoolCopy(craftSecurityPacket(
		IpProtocolTcp,
		net.ParseIP("10.11.12.13"),
		54321,
		net.ParseIP("127.0.0.1"),
		originAddr.Port,
		true,
		nil,
	))
	if !localUserNat.SendPacket(SourceId(NewId()), protocol.ProvideMode_Network, syn, -1) {
		MessagePoolReturn(syn)
		t.Fatal("the SYN was not queued")
	}

	var receiveBufferSize, sendBufferSize int
	select {
	case sizes := <-upstream:
		receiveBufferSize, sendBufferSize = sizes[0], sizes[1]
	case <-time.After(10 * time.Second):
		t.Fatal("the flow never reached its upstream socket")
	}

	policy := defaultSocketBufferPolicy()
	requestByteCount := int(settings.TcpBufferSettings.MaxWindowSize)
	receiveDefault := upstreamSysctlValues(t, "net/ipv4/tcp_rmem")[1]

	if policy.explicitReceive(requestByteCount) {
		if want := policy.obtained(requestByteCount, policy.receiveCoreMaxByteCount); receiveBufferSize != want {
			t.Errorf(
				"the flow's upstream receive buffer is %d; its %d byte request beats the kernel's ceiling and must obtain %d",
				receiveBufferSize,
				requestByteCount,
				want,
			)
		}
	} else if receiveBufferSize != receiveDefault {
		t.Errorf(
			"the flow's upstream receive buffer is %d, not the %d byte tcp_rmem default; its %d byte request does not beat the kernel's ceiling, so nothing may size this socket and autotuning must still own it",
			receiveBufferSize,
			receiveDefault,
			requestByteCount,
		)
	}

	if policy.explicitSend(requestByteCount) {
		if want := policy.obtained(requestByteCount, policy.sendCoreMaxByteCount); sendBufferSize != want {
			t.Errorf(
				"the flow's upstream send buffer is %d; its %d byte request beats the kernel's ceiling and must obtain %d",
				sendBufferSize,
				requestByteCount,
				want,
			)
		}
	} else if sendAutotuneMax := upstreamSysctlValues(t, "net/ipv4/tcp_wmem")[2]; sendAutotuneMax < sendBufferSize {
		t.Errorf(
			"the flow's upstream send buffer is %d, past the %d byte tcp_wmem maximum autotuning would reach; its %d byte request does not beat that ceiling, so it must not have been applied",
			sendBufferSize,
			sendAutotuneMax,
			requestByteCount,
		)
	}

	t.Logf(
		"one flow's upstream socket: receive %d, send %d, for a %d byte request against core maxima %d and %d and ceilings %d and %d",
		receiveBufferSize,
		sendBufferSize,
		requestByteCount,
		policy.receiveCoreMaxByteCount,
		policy.sendCoreMaxByteCount,
		policy.receiveCeilingByteCount,
		policy.sendCeilingByteCount,
	)
}
