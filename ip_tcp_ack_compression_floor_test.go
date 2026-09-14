package connect

import (
	"testing"
	"time"

	"gvisor.dev/gvisor/pkg/tcpip/transport/tcp"
)

// The lowest retransmission floor a peer can have among the stacks we do not
// ship, recorded here with its provenance because the assertion below is
// against the lowest of them rather than against our own tun alone: Linux's
// TCP_RTO_MIN and Darwin's are 200 ms, Windows is 300 ms. A provider cannot
// read any of them at runtime.
const minimumPeerRetransmissionFloor = 200 * time.Millisecond

// THROUGHPUTFIX §29.2 row C1. An acknowledgement held longer than the peer's
// retransmission floor is a spurious retransmission, and the relationship is
// an identity rather than a tendency: moving the peer's floor to 400 ms moved
// the observed cliff to exactly 400, with the collapse depth scaling as the
// mechanism predicts.
//
// This is a test assertion and not a runtime guard, deliberately. The floor
// that matters belongs to the peer, and the peer is the client's stack —
// gVisor under our tun on some platforms, the host kernel's TCP on others — a
// value no provider can read. A runtime guard would be asserting against a
// copy of a number it cannot verify, and a comment cannot fail. A row can, and
// it can read the one instance of the constant we do ship.
//
// The quarter is the margin the shipping value has today, made explicit. A
// campaign that lowers the compression timeout only widens it; a campaign that
// raises either floor must move the ratio in the same commit; and a vendored
// stack update that moves `MinRTO` fails this row on the day it lands rather
// than in a provider's upload months later.
func TestAckCompressionStaysUnderTheRetransmissionFloor(t *testing.T) {
	ackCompressTimeout := DefaultTcpBufferSettings().AckCompressTimeout
	if ackCompressTimeout <= 0 {
		t.Fatal("the shipping acknowledgement compression timeout is not positive, so this row measures nothing")
	}

	floors := []struct {
		floor       time.Duration
		name        string
		description string
	}{
		{
			floor:       tcp.MinRTO,
			name:        "tcp.MinRTO",
			description: "the vendored stack's retransmission floor, which is our tun's peer",
		},
		{
			floor:       minimumPeerRetransmissionFloor,
			name:        "minimumPeerRetransmissionFloor",
			description: "the lowest floor a peer we do not ship can have, Linux and Darwin at 200 ms against Windows at 300",
		},
		{
			floor:       DefaultTunSettings().TcpMinRto,
			name:        "DefaultTunSettings().TcpMinRto",
			description: "our own tun's floor, when a campaign sets it; zero leaves the stack default above",
		},
	}
	for _, floor := range floors {
		if floor.floor <= 0 {
			continue
		}
		if floor.floor/4 < ackCompressTimeout {
			t.Errorf(
				"the shipping acknowledgement compression timeout is %s against %s of %s (%s), above the quarter this margin is set at. An acknowledgement held longer than a peer's retransmission floor is a spurious retransmission, and the relationship is an identity: raising the peer's floor to 400 ms moved the observed cliff to exactly 400 ms. Lowering the timeout widens the margin; raising a floor must move the ratio in the same commit",
				ackCompressTimeout,
				floor.name,
				floor.floor,
				floor.description,
			)
		}
	}

	t.Logf(
		"compression timeout %s against tcp.MinRTO %s, the fleet floor %s, and our tun's floor %s",
		ackCompressTimeout,
		tcp.MinRTO,
		minimumPeerRetransmissionFloor,
		DefaultTunSettings().TcpMinRto,
	)
}
