package connect

import (
	"context"
	"net/netip"
	"slices"
	"sync"
	"testing"
	"time"
)

// The extenders on a provider event (EXTENDER.md K1): the monitor rewrite in
// place, and the window watcher that drives it from the exit's transport.

// A generator that owns nothing but the extender addresses of its clients, so
// the window's publishing can be driven without a platform transport. The
// embedded interface is nil on purpose: nothing in these tests calls the rest
// of the generator.
type extenderIpsTestGenerator struct {
	MultiClientGenerator

	changeMonitor *MonitorValue[uint64]

	stateLock sync.Mutex
	clientIps map[*Client][]netip.Addr
}

func newExtenderIpsTestGenerator() *extenderIpsTestGenerator {
	return &extenderIpsTestGenerator{
		changeMonitor: NewMonitorValue[uint64](0),
		clientIps:     map[*Client][]netip.Addr{},
	}
}

func (self *extenderIpsTestGenerator) ClientExtenderIps(client *Client) ([]netip.Addr, <-chan struct{}) {
	// subscribe before the read, the same order the api generator uses
	_, change := self.changeMonitor.Get()
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return slices.Clone(self.clientIps[client]), change
}

func (self *extenderIpsTestGenerator) setClientIps(client *Client, ips []netip.Addr) {
	func() {
		self.stateLock.Lock()
		defer self.stateLock.Unlock()
		self.clientIps[client] = slices.Clone(ips)
	}()
	self.changeMonitor.Update(func(count uint64) uint64 {
		return count + 1
	})
}

// The rewrite changes the addresses in place: the provider stays added, its
// connected-since time does not move, and only an actual change dispatches.
func TestSetProviderExtenderIpsUpdatesInPlace(t *testing.T) {
	monitor := NewRemoteUserNatMultiClientMonitorWithDefaults()
	clientId := NewId()
	extenderIp := netip.MustParseAddr("192.0.2.120")
	otherExtenderIp := netip.MustParseAddr("2001:db8::120")

	extenderIps := []netip.Addr{extenderIp}
	monitor.AddProviderEventWithExtenderIps(
		clientId,
		ProviderStateAdded,
		NewId(),
		nil,
		IpFamilyDualstack,
		extenderIps,
	)
	event := monitor.ProviderEvents()[clientId]
	if event == nil {
		t.Fatal("the added event was not published")
	}
	if !slices.Equal(event.ExtenderIps, []netip.Addr{extenderIp}) {
		t.Fatalf("extender ips = %v, want [%v]", event.ExtenderIps, extenderIp)
	}
	eventTime := event.EventTime
	// the event owns its copy: the caller's slice is not the event's
	extenderIps[0] = otherExtenderIp
	if !slices.Equal(monitor.ProviderEvents()[clientId].ExtenderIps, []netip.Addr{extenderIp}) {
		t.Fatal("the event shares the caller's slice")
	}

	dispatched := make(chan []netip.Addr, 8)
	unsub := monitor.AddMonitorEventCallback(func(
		windowExpandEvent *WindowExpandEvent,
		providerEvents map[Id]*ProviderEvent,
		reset bool,
	) {
		if event := providerEvents[clientId]; event != nil {
			dispatched <- slices.Clone(event.ExtenderIps)
		}
	})
	defer unsub()

	if monitor.SetProviderExtenderIps(NewId(), []netip.Addr{otherExtenderIp}) {
		t.Error("an unknown client id reported a change")
	}
	if monitor.SetProviderExtenderIps(clientId, []netip.Addr{extenderIp}) {
		t.Error("an unchanged set reported a change")
	}

	// the no-op is proved by ordering rather than by waiting on it: the very
	// next real change is made now, and the first thing the callback sees must
	// be that change. A no-op that had dispatched would be delivered first.
	if !monitor.SetProviderExtenderIps(clientId, []netip.Addr{extenderIp, otherExtenderIp}) {
		t.Fatal("a changed set reported no change")
	}
	select {
	case ips := <-dispatched:
		if !slices.Equal(ips, []netip.Addr{extenderIp, otherExtenderIp}) {
			t.Fatalf("dispatched %v, want both addresses", ips)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the change was not dispatched")
	}

	// and a second change is delivered as itself rather than as a repeat of
	// the first
	thirdExtenderIp := netip.MustParseAddr("198.51.100.120")
	if !monitor.SetProviderExtenderIps(clientId, []netip.Addr{thirdExtenderIp}) {
		t.Fatal("the second change reported no change")
	}
	select {
	case ips := <-dispatched:
		if !slices.Equal(ips, []netip.Addr{thirdExtenderIp}) {
			t.Fatalf("dispatched %v, want the second change", ips)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the second change was not dispatched")
	}
	if !monitor.SetProviderExtenderIps(clientId, []netip.Addr{extenderIp, otherExtenderIp}) {
		t.Fatal("returning to the earlier set reported no change")
	}
	select {
	case ips := <-dispatched:
		if !slices.Equal(ips, []netip.Addr{extenderIp, otherExtenderIp}) {
			t.Fatalf("dispatched %v, want both addresses again", ips)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the return was not dispatched")
	}

	updated := monitor.ProviderEvents()[clientId]
	if !updated.EventTime.Equal(eventTime) {
		t.Fatalf("event time moved from %s to %s", eventTime, updated.EventTime)
	}
	if updated.State != ProviderStateAdded {
		t.Fatalf("state = %s, want the provider still added", updated.State)
	}
	if updated.IpFamily != IpFamilyDualstack {
		t.Fatalf("family = %s, want it untouched", updated.IpFamily)
	}
}

// An exit whose transport runs through an extender carries that address on its
// provider event, and the window's watcher republishes every change until the
// exit ends.
func TestWindowPublishesExitExtenderIps(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clientCtx, clientCancel := context.WithCancel(ctx)
	defer clientCancel()

	settings := DefaultMultiClientSettings()
	generator := newExtenderIpsTestGenerator()
	window := familyTestWindow(ctx, generator, settings)
	client := familyTestChannel(t, clientCtx, settings, IpFamilyDualstack)
	window.clients[client.ClientId()] = client

	extenderIp := netip.MustParseAddr("192.0.2.121")
	otherExtenderIp := netip.MustParseAddr("192.0.2.122")
	generator.setClientIps(client.client, []netip.Addr{extenderIp})

	// the event carries what the exit was reached through when it was raised
	if ips := window.clientExtenderIps(client); !slices.Equal(ips, []netip.Addr{extenderIp}) {
		t.Fatalf("client extender ips = %v, want [%v]", ips, extenderIp)
	}
	window.monitor.AddProviderEventWithExtenderIps(
		client.ClientId(),
		ProviderStateAdded,
		client.Destination().Tail(),
		nil,
		client.IpFamily(),
		window.clientExtenderIps(client),
	)
	if event := window.monitor.ProviderEvents()[client.ClientId()]; event == nil ||
		!slices.Equal(event.ExtenderIps, []netip.Addr{extenderIp}) {
		t.Fatalf("provider event = %+v, want the extender address", event)
	}

	watching := make(chan struct{})
	go func() {
		defer close(watching)
		window.watchExtenderIps(client)
	}()

	// the transport moves to another extender
	generator.setClientIps(client.client, []netip.Addr{otherExtenderIp})
	if !waitForCondition(10*time.Second, func() bool {
		event := window.monitor.ProviderEvents()[client.ClientId()]
		return event != nil && slices.Equal(event.ExtenderIps, []netip.Addr{otherExtenderIp})
	}) {
		t.Fatalf(
			"the provider event did not follow the transport, event = %+v",
			window.monitor.ProviderEvents()[client.ClientId()],
		)
	}
	// and a direct connection publishes none
	generator.setClientIps(client.client, nil)
	if !waitForCondition(10*time.Second, func() bool {
		event := window.monitor.ProviderEvents()[client.ClientId()]
		return event != nil && len(event.ExtenderIps) == 0
	}) {
		t.Fatal("the provider event kept an extender the transport no longer uses")
	}

	// the watcher belongs to the exit, and ends with it
	clientCancel()
	select {
	case <-watching:
	case <-time.After(10 * time.Second):
		t.Fatal("the watcher outlived the exit")
	}
}
