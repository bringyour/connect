package connect

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type clientStrategyConnectionStates struct {
	idleOnce   sync.Once
	closedOnce sync.Once
	idle       chan struct{}
	closed     chan struct{}
}

func newClientStrategyConnectionStates() *clientStrategyConnectionStates {
	return &clientStrategyConnectionStates{
		idle:   make(chan struct{}),
		closed: make(chan struct{}),
	}
}

func (self *clientStrategyConnectionStates) observe(_ net.Conn, state http.ConnState) {
	switch state {
	case http.StateIdle:
		self.idleOnce.Do(func() { close(self.idle) })
	case http.StateClosed:
		self.closedOnce.Do(func() { close(self.closed) })
	}
}

func newClientStrategyLifecycleServer(t *testing.T, ipVersion int) (*httptest.Server, *clientStrategyConnectionStates) {
	t.Helper()
	states := newClientStrategyConnectionStates()
	server := newFamilyHttptestUnstartedServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	server.Config.ConnState = states.observe
	server.Start()
	t.Cleanup(server.Close)
	return server, states
}

func newClientStrategyLifecycleRequest(t *testing.T, ctx context.Context, url string) *http.Request {
	t.Helper()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

// Reproduces the provisioning leak at the socket boundary: canceling the
// owner used to leave the strategy's keep-alive connection open indefinitely.
func TestClientStrategyParentCancellationClosesIdleConnections(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		server, states := newClientStrategyLifecycleServer(t, ipVersion)
		ctx, cancel := context.WithCancel(context.Background())
		settings := DefaultClientStrategySettings()
		settings.EnableResilient = false
		strategy := NewClientStrategy(ctx, settings)
		t.Cleanup(strategy.Close)
		if _, err := strategy.HttpParallel(newClientStrategyLifecycleRequest(t, ctx, server.URL)); err != nil {
			cancel()
			t.Fatal(err)
		}
		select {
		case <-states.idle:
		case <-time.After(5 * time.Second):
			cancel()
			t.Fatal("HTTP connection never became idle")
		}
		cancel()
		select {
		case <-states.closed:
		case <-time.After(5 * time.Second):
			t.Fatal("strategy parent cancellation retained its idle connection")
		}
	})
}

// Explicit ownership teardown is synchronous and idempotent, which lets a
// short-lived provisioning caller release resources before creating the next
// identity instead of waiting for its long-lived parent context.
func TestClientStrategyCloseClosesIdleConnections(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		server, states := newClientStrategyLifecycleServer(t, ipVersion)
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		settings := DefaultClientStrategySettings()
		settings.EnableResilient = false
		strategy := NewClientStrategy(ctx, settings)
		t.Cleanup(strategy.Close)
		if _, err := strategy.HttpParallel(newClientStrategyLifecycleRequest(t, ctx, server.URL)); err != nil {
			t.Fatal(err)
		}
		select {
		case <-states.idle:
		case <-time.After(5 * time.Second):
			t.Fatal("HTTP connection never became idle")
		}
		strategy.Close()
		strategy.Close()
		select {
		case <-states.closed:
		case <-time.After(5 * time.Second):
			t.Fatal("explicit strategy close retained its idle connection")
		}
	})
}
