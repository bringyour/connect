package connect

// FLIGHTGATEFIX §15 sizing invariants. The numbers that decide whether the
// tunnel trickles or crashes a phone live in two repositories and can drift
// apart without any test noticing: connect's transfer defaults and carrier
// derivations here, the mobile ceilings in the SDK's memory policy. These
// tests pin the relationships between them, not the values, and each says
// what the number means so a reader who trips one knows what broke.

import (
	"testing"
	"time"
)

// tunnelTypicalMessageByteCount is one Transfer message carrying one tunnel
// IP packet at the product's advertised 1,100-byte MTU. The device rig
// measured about 930 bytes across a download, which is this less the
// headroom a partly filled segment leaves.
const tunnelTypicalMessageByteCount = 930

// A window that cannot be drained inside the retransmit interval is a
// retransmit storm: the sender rewrites the whole window against a path
// that is still delivering, which is what the device runs measured at
// 13,000 to 19,000 timeout resends per three-minute run. Either the window
// drains inside the interval at the rate the path actually carries, or the
// defer must be on (FLIGHTGATEFIX §15.1).
func TestRetransmitIntervalCoversTheWindowOrTheDeferIsOn(t *testing.T) {
	// the rate the device rig measured through the tunnel, the regime this
	// has to survive, not the rate of the link underneath it
	const measuredTunnelBitsPerSecond = 1_000_000
	settings := DefaultSendBufferSettings()
	window := settings.ResendQueueMaxByteCount
	drain := time.Duration(float64(window) * 8 / measuredTunnelBitsPerSecond * float64(time.Second))
	// the interval the timer can fall to once the window holds samples
	interval := settings.RttMinResendInterval
	t.Logf(
		"ResendQueueMaxByteCount %d bytes drains in %s at %d bit/s, against RttMinResendInterval %s",
		window, drain.Truncate(time.Millisecond), measuredTunnelBitsPerSecond, interval,
	)
	if interval < drain && !settings.DeferTimeoutResendWhileCumulativeProgress {
		t.Fatalf(
			"a %d-byte window takes %s to drain at %d bit/s but the retransmit interval can fall to %s, "+
				"so the whole window is rewritten every interval: either lower ResendQueueMaxByteCount, "+
				"raise RttMinResendInterval, or keep DeferTimeoutResendWhileCumulativeProgress on",
			window, drain.Truncate(time.Millisecond), measuredTunnelBitsPerSecond, interval,
		)
	}
	if settings.DeferTimeoutResendWhileCumulativeProgress &&
		settings.TimeoutResendDeferLimit <= 0 {
		t.Fatal("TimeoutResendDeferLimit is not positive, so a stalled lane would never be retransmitted")
	}
}

// The unreliable flight's floor is what the lane keeps after loss has
// halved it as far as it goes. A floor that cannot hold one tunnel message
// stops the lane completely rather than slowing it, and the message floor
// must leave room for the same message.
func TestUnreliableFlightFloorHoldsATypicalMessage(t *testing.T) {
	settings := DefaultSendBufferSettings()
	if settings.UnreliableMinimumFlightByteCount < tunnelTypicalMessageByteCount {
		t.Fatalf(
			"UnreliableMinimumFlightByteCount is %d bytes, under one %d-byte tunnel message: "+
				"at the loss floor the direct lane could not admit a single packet",
			settings.UnreliableMinimumFlightByteCount, tunnelTypicalMessageByteCount,
		)
	}
	if settings.UnreliableMinimumFlightMessageCount < 1 {
		t.Fatalf(
			"UnreliableMinimumFlightMessageCount is %d: the lane can never prove itself again",
			settings.UnreliableMinimumFlightMessageCount,
		)
	}
	if settings.UnreliableMaximumFlightByteCount < settings.UnreliableMinimumFlightByteCount {
		t.Fatalf(
			"UnreliableMaximumFlightByteCount %d is under the floor %d",
			settings.UnreliableMaximumFlightByteCount, settings.UnreliableMinimumFlightByteCount,
		)
	}
}

// The P2P carrier derives the flight it will admit from its own receive
// queue, reserving headroom for the ACK, contract and probe traffic that
// the flight does not track. The two derivations must not drift: the
// admitted flight has to stay strictly inside the queue, and the reserve
// has to stay large enough to hold that untracked traffic.
func TestP2pUnreliableFlightLimitsStayInsideTheirReceiveQueue(t *testing.T) {
	for _, settings := range []*P2pTransportSettings{
		DefaultP2pTransportSettings(),
		func() *P2pTransportSettings {
			small := DefaultP2pTransportSettings()
			small.ReceiveQueueByteCount = 64 * 1024
			small.ReceiveQueueMessageCount = 32
			return small
		}(),
	} {
		queueByteCount := p2pReceiveQueueByteCount(settings)
		queueMessageCount := p2pReceiveQueueMessageCount(settings)
		flightByteLimit := p2pUnreliableFlightByteLimit(settings)
		flightMessageLimit := p2pUnreliableFlightMessageLimit(settings)
		if queueByteCount <= flightByteLimit {
			t.Fatalf(
				"p2pUnreliableFlightByteLimit %d leaves no room in the %d-byte receive queue "+
					"for the ACK, contract and probe traffic the flight does not track",
				flightByteLimit, queueByteCount,
			)
		}
		if queueMessageCount <= flightMessageLimit {
			t.Fatalf(
				"p2pUnreliableFlightMessageLimit %d fills the %d-message receive queue",
				flightMessageLimit, queueMessageCount,
			)
		}
		if reserve := queueByteCount - flightByteLimit; reserve < tunnelTypicalMessageByteCount {
			t.Fatalf(
				"the receive queue reserve is %d bytes, under one %d-byte message: "+
					"an ACK arriving behind a full flight would be dropped",
				reserve, tunnelTypicalMessageByteCount,
			)
		}
		if flightByteLimit <= 0 || flightMessageLimit <= 0 {
			t.Fatalf("the carrier admits no flight at all: %d bytes, %d messages",
				flightByteLimit, flightMessageLimit)
		}
	}
}

// Connect's own unreliable ceilings: the byte ceiling is the memory budget
// and binds on its own, so the message ceiling decides only what share of
// that budget messages of a given size may use. The SDK holds the mobile
// values; this is the same relationship for the defaults a server runs.
func TestUnreliableFlightMessageCeilingAdmitsItsByteBudget(t *testing.T) {
	settings := DefaultSendBufferSettings()
	admitted := ByteCount(settings.UnreliableMaximumFlightMessageCount) * tunnelTypicalMessageByteCount
	budget := settings.UnreliableMaximumFlightByteCount
	share := float64(admitted) / float64(budget)
	t.Logf(
		"UnreliableMaximumFlightMessageCount %d at %d bytes admits %d bytes, %.0f%% of the %d-byte UnreliableMaximumFlightByteCount",
		settings.UnreliableMaximumFlightMessageCount, tunnelTypicalMessageByteCount,
		admitted, 100*share, budget,
	)
	// half the budget is the line between a message ceiling that shapes the
	// lane and one that replaces the byte budget with a much smaller one
	if share < 0.5 {
		t.Fatalf(
			"UnreliableMaximumFlightMessageCount %d admits only %.0f%% of the %d bytes "+
				"UnreliableMaximumFlightByteCount already grants, so the direct lane trickles "+
				"and the relay carries the overflow (FLIGHTGATEFIX §15.3)",
			settings.UnreliableMaximumFlightMessageCount, 100*share, budget,
		)
	}
}
