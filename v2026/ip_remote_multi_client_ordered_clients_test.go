package connect

import (
	"context"
	"testing"
)

// orderedClientsTestGenerator reports a fixed destination count so the window
// can be exercised as either a user-selected network peer (fixed) or an
// ordinary expanding window.
type orderedClientsTestGenerator struct {
	MultiClientGenerator
	fixedSize  int
	fixedIsSet bool
}

func (self *orderedClientsTestGenerator) FixedDestinationSize() (int, bool) {
	return self.fixedSize, self.fixedIsSet
}

func newOrderedClientsTestWindow(
	t *testing.T,
	ctx context.Context,
	fixedDestination bool,
	warnings ...bool,
) *multiClientWindow {
	settings := DefaultMultiClientSettings()
	clientSettings := DefaultClientSettings()
	clientSettings.Log = NewNoopLogger()
	parent := NewClient(ctx, NewId(), NewNoContractClientOob(), clientSettings)
	t.Cleanup(parent.Close)
	window := &multiClientWindow{
		ctx: ctx,
		log: NewNoopLogger(),
		generator: &orderedClientsTestGenerator{
			fixedSize:  1,
			fixedIsSet: fixedDestination,
		},
		windowType: WindowTypeQuality,
		settings:   settings,
		clients:    map[Id]*multiClientChannel{},
	}
	for _, warning := range warnings {
		clientId := NewId()
		window.clients[clientId] = &multiClientChannel{
			ctx:         ctx,
			log:         NewNoopLogger(),
			client:      parent,
			settings:    settings,
			warning:     warning,
			packetStats: &clientWindowStats{log: NewNoopLogger()},
			args:        &multiClientChannelArgs{Destination: RequireMultiHopId(NewId())},
		}
	}
	return window
}

// A warning steers new flows away from a client because a better destination
// is expected to exist. A user-selected network peer is a fixed
// single-destination window whose only "replacement" is another client to the
// same endpoint, so excluding every warned client left new flows with no
// candidate at all — they stalled on the send retry cadence while the peer was
// still routing.
func TestOrderedClientsFallsBackToWarnedFixedDestination(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	window := newOrderedClientsTestWindow(t, ctx, true, true)
	clients := window.OrderedClients()
	if len(clients) != 1 {
		t.Fatalf("fixed destination with one warned client returned %d candidates, want 1", len(clients))
	}
}

// An expanding window really does have an alternative: a replacement dials a
// different provider. Waiting for it is correct, so the fallback must not
// apply there.
func TestOrderedClientsDoesNotFallBackForExpandingWindow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	window := newOrderedClientsTestWindow(t, ctx, false, true, true)
	if clients := window.OrderedClients(); len(clients) != 0 {
		t.Fatalf("expanding window returned %d warned candidates, want 0", len(clients))
	}
}

// The fallback is a last resort: whenever an unwarned client exists it must be
// the only candidate, so a healthy client always wins over a warned one.
func TestOrderedClientsPrefersUnwarnedOverFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	window := newOrderedClientsTestWindow(t, ctx, true, true, false, true)
	clients := window.OrderedClients()
	if len(clients) != 1 {
		t.Fatalf("returned %d candidates, want only the unwarned client", len(clients))
	}
	if clients[0].isWarning() {
		t.Fatal("a warned client was preferred while an unwarned client existed")
	}
}
