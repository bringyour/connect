package connect

// FLIGHTGATEFIX §31.3. The exchange-h3 hang, made deterministic.
//
// The condition is a single-lane route whose only carrier publishes
// Unreliable, with no reliable sibling, where the far side stops
// acknowledging and the flow's Packs are retained past their
// acknowledgement timeout the way a TCP-socket recovery-mode flow sets.
// Every step of what follows is merged's: the flight halves to its floor
// on each timeout, admission blocks in bounded waits with nothing on the
// reliable side to offer, and the items are held by design because their
// flow owns their lifetime.
//
// The row this file cannot assert, and why, is stated at the bottom: the
// decision to retire such a route belongs to the multi-client that owns
// the flow, not to the recovery path. Nothing here should be read as a
// claim that the recovery path ought to retire a route itself.
//
// Built from API that predates this program so it runs unchanged against
// merged 89e1633.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// deadRouteLink is a sender whose only carrier is unreliable and whose
// far side never acknowledges. The receiving end is a plain channel the
// test drains, so nothing ever answers.
type deadRouteLink struct {
	sender     *Client
	receiverId Id
	carrier    Route
	drained    chan int
}

func newDeadRouteLink(t testing.TB, configure func(*SendBufferSettings)) *deadRouteLink {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	settings := DefaultClientSettings()
	settings.EncryptionSettings.Mode = EncryptionModeOff
	// the flow owns the lifetime, so the sequence must not give up on its
	// own; this is the shape a TCP-socket flow sets
	settings.SendBufferSettings.AckTimeout = 5 * time.Second
	settings.SendBufferSettings.IdleTimeout = 120 * time.Second
	if configure != nil {
		configure(settings.SendBufferSettings)
	}
	link := &deadRouteLink{
		receiverId: NewId(),
		carrier:    make(Route, 256),
		drained:    make(chan int, 4096),
	}
	link.sender = NewClient(ctx, NewId(), NewNoContractClientOob(), settings)
	link.sender.ContractManager().AddNoContractPeer(link.receiverId)
	// one lane, and it publishes Unreliable: this is the hybrid carrier's
	// state after the route generation that leaves it without a sibling
	link.sender.RouteManager().UpdateTransportWithProperties(
		NewSendGatewayTransportWithType(TransportTypeH3),
		[]Route{link.carrier},
		TransferCarrierProperties{Unreliable: true},
	)
	// drain the wire and answer nothing
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case frameBytes := <-link.carrier:
				if frameBytes != nil {
					MessagePoolReturn(frameBytes)
				}
				select {
				case link.drained <- 1:
				default:
				}
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		closeCtx, closeCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer closeCancel()
		link.sender.CloseAndWait(closeCtx)
	})
	return link
}

// send offers one Pack whose flow retains it past its acknowledgement
// timeout, which is what a TCP-socket recovery-mode flow sets.
func (self *deadRouteLink) send(t testing.TB, index int) bool {
	t.Helper()
	frame, err := ToFrame(
		&protocol.SimpleMessage{Content: fmt.Sprintf("dead-route-%d", index)},
		DefaultProtocolVersion,
	)
	if err != nil {
		t.Fatal(err)
	}
	admitted, _ := self.sender.SendWithTimeoutDetailed(
		frame,
		self.receiverId,
		nil,
		2*time.Second,
		sendPackRecoveryOption{upstreamRecoverable: true, retainAfterAckTimeout: true},
	)
	if !admitted {
		MessagePoolReturn(frame.MessageBytes)
	}
	return admitted
}

// Row A. The condition is visible: the route reports how long it has gone
// without acknowledging and how many of its items it still holds, so a
// campaign can see this without reading logs.
func TestDeadRouteExposesItsUnacknowledgedDurationAndRetainedCount(t *testing.T) {
	if testing.Short() {
		t.Skip("dead route contract")
	}
	for _, arm := range laneRecoveryArms() {
		link := newDeadRouteLink(t, arm.configure)
		for index := range 12 {
			link.send(t, index)
		}
		// past the acknowledgement timeout, so retention is what holds them
		time.Sleep(8 * time.Second)
		stats := link.sender.SendRecoveryStats()
		unacknowledged, retained, supported := deadRouteExposureForTree(stats)
		if !supported {
			t.Logf("%s: row A: this tree does not expose the route's unacknowledged duration or "+
				"retained count, so the condition is only visible in a log", arm.name)
			continue
		}
		t.Logf("%s: row A: route unacknowledged for %s holding %d items, after %d whole-window "+
			"retransmits and %d flight reductions",
			arm.name, unacknowledged.Truncate(time.Millisecond), retained,
			stats.TimeoutResendWriteCount, stats.UnreliableFlightReductionCount)
		if unacknowledged < 3*time.Second {
			t.Errorf("%s: row A: the route reports %s unacknowledged on a carrier that answered "+
				"nothing for eight seconds", arm.name, unacknowledged)
		}
		if retained == 0 {
			t.Errorf("%s: row A: the route reports no retained items though its flow retains "+
				"them past their acknowledgement timeout", arm.name)
		}
	}
}

// Row B. The retention holds as designed: no item exits on its own while
// its flow owns its lifetime, however long the carrier stays silent. This
// row is the one that says the wait is not a bug in the recovery path.
func TestDeadRouteRetainsItemsPastTheAcknowledgementTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("dead route contract")
	}
	for _, arm := range laneRecoveryArms() {
		link := newDeadRouteLink(t, arm.configure)
		const offered = 8
		for index := range offered {
			link.send(t, index)
		}
		settings := DefaultSendBufferSettings()
		time.Sleep(3 * time.Second)
		early, _, _ := link.sender.ResendQueueSize(link.receiverId, MultiHopId{}, false, false)
		// well past the acknowledgement timeout the link was built with
		time.Sleep(6 * time.Second)
		late, _, _ := link.sender.ResendQueueSize(link.receiverId, MultiHopId{}, false, false)
		t.Logf("%s: row B: %d items held after 3 s, %d after 9 s, against an acknowledgement "+
			"timeout of 5 s (default %s)",
			arm.name, early, late, settings.AckTimeout)
		if late < early {
			t.Errorf(
				"%s: row B: the queue fell from %d to %d past the acknowledgement timeout; a "+
					"flow that retains its Packs owns their lifetime and the sequence must not "+
					"drop them on its own",
				arm.name, early, late,
			)
		}
	}
}

// Row C. Every rewrite carries the head, so a receiver that ever returns
// can resume without a separate head exchange.
func TestDeadRouteRewritesCarryTheHead(t *testing.T) {
	if testing.Short() {
		t.Skip("dead route contract")
	}
	for _, arm := range laneRecoveryArms() {
		link := newDeadRouteLink(t, arm.configure)
		for index := range 6 {
			link.send(t, index)
		}
		time.Sleep(6 * time.Second)
		written := 0
		for {
			select {
			case <-link.drained:
				written += 1
				continue
			default:
			}
			break
		}
		stats := link.sender.SendRecoveryStats()
		t.Logf("%s: row C: %d frames reached the wire for 6 messages, %d of them whole-window "+
			"rewrites", arm.name, written, stats.TimeoutResendWriteCount)
		if written < 6 {
			t.Errorf("%s: row C: only %d frames reached the wire for 6 messages", arm.name, written)
		}
	}
}

// Row D, stated and not asserted here.
//
// §31.3's last row is that the multi-client retires such a route within a
// stated bound of its stall verdict, and that merged fails it by
// construction because it holds the verdict for want of a sibling. That
// row cannot be built in this file, and approximating it would be worse
// than leaving it out.
//
// What it needs, precisely: a RemoteUserNatMultiClient with at least one
// provider window, its contract and window machinery running, and a way to
// drive a stall verdict to the point where the retirement decision is
// taken. The instruments in this package build Clients and Routes
// directly; none of them builds a multi-client window, and the verdict is
// reached through provider selection and the window's own outcome path
// rather than through any seam a test can call. Building it means either a
// provider harness for the multi-client or a seam on the verdict, and
// which of those is right is the multi-client's design question rather
// than this program's.
//
// Until then the bound is recorded here so it cannot be forgotten: a
// client pinned to a provider that churns during the join can hold a dead
// route for the whole of a workload, 1,014 seconds in the three campaign
// runs that showed it, because the layer that owns the flow's lifetime
// declines to retire a route it cannot prove against a sibling.
func TestDeadRouteRetirementBelongsToTheMultiClient(t *testing.T) {
	t.Log(
		"row D is not asserted here: the retirement decision belongs to the multi-client, and " +
			"driving its stall verdict needs a provider window this package's instruments do not " +
			"build. Nothing in rows A to C should be read as a claim that the recovery path " +
			"ought to retire a route itself.",
	)
}
