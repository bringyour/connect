package connect

import (
	"context"
	"net/netip"
	"slices"
	"testing"
	"time"
)

// Ownership of the extender dot on a provider event (EXTENDER.md K1).
//
// A provider id outlives the channel that carries it: a replacement keeps the
// id and installs its own watcher. The old watcher is woken by the same change
// monitor, so without the ownership check it would publish the departed
// channel's addresses over the live one's -- and the dot would show an
// extender that is no longer carrying anything. The check is at the layer the
// bug is observable, before the publish rather than after it.

// A watcher whose channel is no longer the one the window holds for its id
// ends without publishing.
func TestWindowExtenderWatcherStopsWhenItsChannelIsReplaced(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultMultiClientSettings()
	generator := newExtenderIpsTestGenerator()
	window := familyTestWindow(ctx, generator, settings)

	departing := familyTestChannel(t, ctx, settings, IpFamilyDualstack)
	clientId := departing.ClientId()
	window.clients[clientId] = departing

	departingIp := netip.MustParseAddr("192.0.2.121")
	replacementIp := netip.MustParseAddr("2001:db8::122")
	generator.setClientIps(departing.client, []netip.Addr{departingIp})

	window.monitor.AddProviderEventWithExtenderIps(
		clientId,
		ProviderStateAdded,
		departing.Destination().Tail(),
		nil,
		departing.IpFamily(),
		window.clientExtenderIps(departing),
	)

	watching := make(chan struct{})
	go func() {
		defer close(watching)
		window.watchExtenderIps(departing)
	}()

	// the watcher publishes while it still owns the id
	if !waitForCondition(10*time.Second, func() bool {
		event := window.monitor.ProviderEvents()[clientId]
		return event != nil && slices.Equal(event.ExtenderIps, []netip.Addr{departingIp})
	}) {
		t.Fatal("the watcher never published its own addresses")
	}

	// a replacement takes the id and publishes its own addresses
	replacement := familyTestChannel(t, ctx, settings, IpFamilyDualstack)
	func() {
		window.stateLock.Lock()
		defer window.stateLock.Unlock()
		window.clients[clientId] = replacement
	}()
	if !window.monitor.SetProviderExtenderIps(clientId, []netip.Addr{replacementIp}) {
		t.Fatal("the replacement's addresses were not published")
	}

	// the departed channel's transport moves, which wakes the old watcher. It
	// must end on the ownership check rather than publish, so the event is
	// still the replacement's when the watcher has returned.
	generator.setClientIps(departing.client, []netip.Addr{netip.MustParseAddr("192.0.2.123")})
	select {
	case <-watching:
	case <-time.After(10 * time.Second):
		t.Fatal("the replaced watcher did not end")
	}
	event := window.monitor.ProviderEvents()[clientId]
	if event == nil {
		t.Fatal("the provider event was removed")
	}
	if !slices.Equal(event.ExtenderIps, []netip.Addr{replacementIp}) {
		t.Fatalf(
			"extender ips = %v, expected the replacement's %v",
			event.ExtenderIps, replacementIp)
	}
}

// A watcher whose id the window no longer holds at all ends the same way, which
// is what a removed provider looks like.
func TestWindowExtenderWatcherStopsWhenItsChannelIsRemoved(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultMultiClientSettings()
	generator := newExtenderIpsTestGenerator()
	window := familyTestWindow(ctx, generator, settings)
	client := familyTestChannel(t, ctx, settings, IpFamilyDualstack)
	clientId := client.ClientId()

	func() {
		window.stateLock.Lock()
		defer window.stateLock.Unlock()
		delete(window.clients, clientId)
	}()
	watching := make(chan struct{})
	go func() {
		defer close(watching)
		window.watchExtenderIps(client)
	}()
	select {
	case <-watching:
	case <-time.After(10 * time.Second):
		t.Fatal("a watcher for an unheld id did not end")
	}
	if event := window.monitor.ProviderEvents()[clientId]; event != nil {
		t.Fatalf("an unheld id published %+v", event)
	}
}

// A generator that does not carry extender addresses, or a channel with no
// client behind it, yields nothing rather than a watcher that spins.
func TestWindowExtenderIpsTolerateAGeneratorWithout(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	settings := DefaultMultiClientSettings()
	plain := familyTestWindow(ctx, &plainTestGenerator{}, settings)
	client := familyTestChannel(t, ctx, settings, IpFamilyDualstack)
	if ips := plain.clientExtenderIps(client); ips != nil {
		t.Errorf("a plain generator yielded %v", ips)
	}
	watching := make(chan struct{})
	go func() {
		defer close(watching)
		plain.watchExtenderIps(client)
	}()
	select {
	case <-watching:
	case <-time.After(10 * time.Second):
		t.Fatal("a watcher on a plain generator did not end")
	}

	window := familyTestWindow(ctx, newExtenderIpsTestGenerator(), settings)
	if ips := window.clientExtenderIps(nil); ips != nil {
		t.Errorf("a nil channel yielded %v", ips)
	}
	empty := &multiClientChannel{}
	if ips := window.clientExtenderIps(empty); ips != nil {
		t.Errorf("a channel with no client yielded %v", ips)
	}
	emptyWatching := make(chan struct{})
	go func() {
		defer close(emptyWatching)
		window.watchExtenderIps(empty)
	}()
	select {
	case <-emptyWatching:
	case <-time.After(10 * time.Second):
		t.Fatal("a watcher on a channel with no client did not end")
	}
}

// A generator with no extender addresses of its own, which is every generator
// that predates K1.
type plainTestGenerator struct {
	MultiClientGenerator
}
