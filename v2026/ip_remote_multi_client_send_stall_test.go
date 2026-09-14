package connect

import (
	"context"
	"testing"
	"time"
)

// mirrors what the real constructor initializes for the send accounting path:
// the event bucket coalescing needs its window durations, and the per-source
// counts are assigned into, so the maps cannot be nil
func stallTestChannel() *multiClientChannel {
	return &multiClientChannel{
		settings: &MultiClientSettings{
			StatsWindowBucketDuration: 1 * time.Second,
			StatsWindowDuration:       30 * time.Second,
		},
		ip4DestinationSourceCount: map[Ip4Path]map[Ip4Path]int{},
		ip6DestinationSourceCount: map[Ip6Path]map[Ip6Path]int{},
		packetStats:               &clientWindowStats{log: DefaultLogger()},
	}
}

// a client holding unacked sends with no progress is failed, and the check has
// to fire well inside AckTimeout or the flows pinned to it stay frozen
func TestSendStalledDetectsNoAckProgress(t *testing.T) {
	stallTimeout := 60 * time.Millisecond
	client := stallTestChannel()

	// nothing outstanding: an idle client is not a stalled one
	AssertEqual(t, client.sendStalled(stallTimeout), false)

	client.addSend(1440, udpTestPath(4))

	// inside the window it is merely slow
	AssertEqual(t, client.sendStalled(stallTimeout), false)

	time.Sleep(stallTimeout + 30*time.Millisecond)

	AssertEqual(t, client.sendStalled(stallTimeout), true)
}

// an ack is progress, so it restarts the clock -- a busy client that keeps
// delivering must never be judged stalled however long it stays busy
func TestSendStalledResetsOnAck(t *testing.T) {
	stallTimeout := 60 * time.Millisecond
	client := stallTestChannel()

	client.addSend(1440, udpTestPath(4))
	time.Sleep(stallTimeout + 30*time.Millisecond)
	AssertEqual(t, client.sendStalled(stallTimeout), true)

	// progress arrives
	client.addSendAck(1440)

	// with nothing outstanding it cannot be stalled
	AssertEqual(t, client.sendStalled(stallTimeout), false)

	// and a fresh send starts a fresh window rather than inheriting the old one
	client.addSend(1440, udpTestPath(4))
	AssertEqual(t, client.sendStalled(stallTimeout), false)
}

// a client that is delivering steadily never trips the check, even across many
// sends, because each ack restarts the clock
func TestSendStalledSteadyProgress(t *testing.T) {
	stallTimeout := 60 * time.Millisecond
	client := stallTestChannel()

	for i := 0; i < 5; i++ {
		client.addSend(1440, udpTestPath(4))
		time.Sleep(20 * time.Millisecond)
		client.addSendAck(1440)
		AssertEqual(t, client.sendStalled(stallTimeout), false)
	}
}

// partial progress still counts: acking one of several outstanding sends means
// the client is alive, so the remaining ones are not a stall
func TestSendStalledPartialAckIsProgress(t *testing.T) {
	stallTimeout := 60 * time.Millisecond
	client := stallTestChannel()

	client.addSend(1440, udpTestPath(4))
	client.addSend(1440, udpTestPath(4))
	time.Sleep(40 * time.Millisecond)

	client.addSendAck(1440)

	// one is still outstanding, but the client just proved it is delivering
	AssertEqual(t, client.sendStalled(stallTimeout), false)
}

// 0 restores the previous behavior, where nothing but AckTimeout classified a
// client that accepts packets and never delivers them
func TestSendStalledDisabled(t *testing.T) {
	client := stallTestChannel()
	client.addSend(1440, udpTestPath(4))
	time.Sleep(80 * time.Millisecond)

	AssertEqual(t, client.sendStalled(0), false)
	AssertEqual(t, client.sendStalled(-1*time.Second), false)
}

// the StallExit diagnostic hook and the detector have to agree: a channel put
// into the stalled state swallows sends, which is exactly the no-ack-progress
// condition the detector looks for
func TestStallExitHookTripsTheDetector(t *testing.T) {
	stallTimeout := 60 * time.Millisecond
	client := stallTestChannel()
	client.setStalled(true)

	// the send is reported successful and never acked, as a real stall behaves
	success, err := client.SendDetailedWithAck(&parsedPacket{
		packet: make([]byte, 40),
		ipPath: udpTestPath(4),
	}, 0, true)
	AssertEqual(t, success, true)
	AssertEqual(t, err == nil, true)

	// no manual addSend here on purpose. this test used to compensate for the
	// swallowed packet never reaching addSend, which made it pass whether or
	// not the stall was actually detectable -- it asserted the detector worked
	// on accounting the test itself had supplied. the swallow now happens after
	// addSend, exactly as a real blackholing provider behaves, so the stall
	// must be visible from the send above alone.
	time.Sleep(stallTimeout + 30*time.Millisecond)

	AssertEqual(t, client.sendStalled(stallTimeout), true)
}

func TestDefaultMultiClientSettingsSetsSendStallTimeout(t *testing.T) {
	settings := DefaultMultiClientSettings()

	AssertEqual(t, 0 < settings.SendStallTimeout, true)
	// must fire well inside the AckTimeout it exists to preempt
	AssertEqual(t, settings.SendStallTimeout < settings.AckTimeout, true)
}

// a send the transport refuses -- a hard error, or backpressure -- never
// reaches its ack callback, so nothing ever retires the accounting addSend
// armed for it. left in place, the refusal is booked as an outstanding send
// forever: the nack count stays up and pendingSendTime keeps aging, and a
// backpressure burst is enough for sendStalled to convict an exit that then
// sits innocently idle. the transport call cannot be faked on a bare channel
// (there is no client under it), so the failure path is simulated by driving
// the same accounting calls SendDetailedWithAck makes on it.
func TestSendStalledAbandonedSendDisarms(t *testing.T) {
	stallTimeout := 20 * time.Millisecond
	client := stallTestChannel()

	client.addSend(1440, udpTestPath(4))
	client.addSendAbandoned(1440)

	// past the window: a clock left armed by the refusal would convict here
	time.Sleep(stallTimeout + 30*time.Millisecond)
	AssertEqual(t, client.sendStalled(stallTimeout), false)

	// the undo is exact against the aggregate, and with nothing outstanding
	// the stall clock is cleared outright
	client.stateLock.Lock()
	defer client.stateLock.Unlock()
	AssertEqual(t, client.packetStats.sendNackCount, 0)
	AssertEqual(t, client.packetStats.sendNackByteCount, ByteCount(0))
	AssertEqual(t, client.pendingSendTime.IsZero(), true)
}

// abandoning one of several outstanding sends is not progress -- only an ack
// proves delivery and restarts the clock. the survivors keep aging on the
// original clock, and only retiring the last outstanding send stops it.
func TestSendStalledAbandonPartialKeepsClock(t *testing.T) {
	stallTimeout := 20 * time.Millisecond
	client := stallTestChannel()

	client.addSend(1440, udpTestPath(4))
	client.addSend(1440, udpTestPath(4))
	client.addSendAbandoned(1440)

	time.Sleep(stallTimeout + 30*time.Millisecond)
	// one send is still genuinely outstanding and unacked past the window
	AssertEqual(t, client.sendStalled(stallTimeout), true)

	client.addSendAbandoned(1440)
	AssertEqual(t, client.sendStalled(stallTimeout), false)
}

// the bucket a send recorded into can rotate before the transport reports the
// refusal. the undo then lands on the newest bucket, clamped at zero -- never
// below -- while the aggregate stays exact
func TestSendStalledAbandonAfterBucketRotation(t *testing.T) {
	client := stallTestChannel()

	// the refused send records into the current bucket...
	client.addSend(1440, udpTestPath(4))

	// ...which then rotates: age it past the bucket duration so the next
	// event opens a fresh one, exactly as the real clock would
	client.stateLock.Lock()
	for _, eventBucket := range client.eventBuckets {
		eventBucket.createTime = eventBucket.createTime.Add(-2 * client.settings.StatsWindowBucketDuration)
	}
	client.stateLock.Unlock()

	client.addSend(100, udpTestPath(4))
	client.addSendAbandoned(1440)

	client.stateLock.Lock()
	defer client.stateLock.Unlock()

	// the aggregate is exact whatever happened to the buckets: one send (the
	// small one) is still genuinely outstanding, and its clock still runs
	AssertEqual(t, client.packetStats.sendNackCount, 1)
	AssertEqual(t, client.packetStats.sendNackByteCount, ByteCount(100))
	AssertEqual(t, client.pendingSendTime.IsZero(), false)

	// the newest bucket recorded less than the abandoned send, so the
	// decrement clamps rather than going negative
	newestBucket := client.eventBuckets[len(client.eventBuckets)-1]
	AssertEqual(t, newestBucket.sendNackCount, 0)
	AssertEqual(t, newestBucket.sendNackByteCount, ByteCount(0))
}

// a channel whose transport set is empty holds outstanding sends because its
// carrier is down, not because the exit swallowed them. the stall verdict
// must hold while the carrier is out, and the clock must not carry the
// outage's age across re-registration -- a clock that kept aging would
// convict on the first poll after restore, before the send sequences have
// even re-sent.
func TestSendStalledTransportDownHolds(t *testing.T) {
	stallTimeout := 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// a real client for its route manager. nothing registers a transport, so
	// the channel's carrier is down.
	client := stallTestChannel()
	client.client = NewClientWithDefaults(ctx, NewId(), NewNoContractClientOob())
	defer client.client.Cancel()

	client.addSend(1440, udpTestPath(4))
	time.Sleep(stallTimeout + 30*time.Millisecond)

	// aged well past the bound, but the carrier is down: held
	AssertEqual(t, client.sendStalled(stallTimeout), false)

	// the carrier re-registers. the clock was restarted while down, so the
	// verdict needs a fresh full window of no progress rather than firing on
	// the age accumulated during the outage
	transport := NewSendGatewayTransport()
	client.client.RouteManager().UpdateTransport(transport, []Route{make(chan []byte)})
	AssertEqual(t, client.sendStalled(stallTimeout), false)

	// with the carrier up, a fresh full window of silence convicts as before
	time.Sleep(stallTimeout + 30*time.Millisecond)
	AssertEqual(t, client.sendStalled(stallTimeout), true)
}

// coalescing can drop every bucket -- the first removal pass has no floor --
// and the undo must still correct the aggregate rather than panic on the
// missing bucket
func TestSendStalledAbandonWithNoBuckets(t *testing.T) {
	client := stallTestChannel()
	client.addSend(1440, udpTestPath(4))

	client.stateLock.Lock()
	client.eventBuckets = nil
	client.stateLock.Unlock()

	client.addSendAbandoned(1440)

	client.stateLock.Lock()
	defer client.stateLock.Unlock()
	AssertEqual(t, client.packetStats.sendNackCount, 0)
	AssertEqual(t, client.packetStats.sendNackByteCount, ByteCount(0))
	AssertEqual(t, client.pendingSendTime.IsZero(), true)
}
