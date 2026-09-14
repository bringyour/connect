package connect

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The standby uses the full client strategy, without the pinned strategy's
// injected dialer. Its explicit DNS resolver must still reach a real plain
// WebSocket server, including one that listens only on IPv6.
func TestClientDialerPlainWebSocketUsesConfiguredResolver(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		platform := newTestingFamilyPlatformServer(t, ipVersion, false)
		settings := DefaultClientStrategySettings()
		settings.Resolver = newFamilyTestResolver(t,
			netip.MustParseAddr("127.0.0.1"), netip.MustParseAddr("::1"))
		wireDial := settings.Resolver.Dial
		var queries atomic.Int32
		settings.Resolver.Dial = func(ctx context.Context, network string, address string) (net.Conn, error) {
			queries.Add(1)
			return wireDial(ctx, network, address)
		}
		if settings.DialContextSettings != nil {
			t.Fatal("resolver-only fixture unexpectedly injects a stream dialer")
		}
		dialer := &clientDialer{settings: settings}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		connection, _, err := dialer.WsDialer(settings).DialContext(ctx, platform.dualStackURL(), nil)
		if err != nil {
			t.Fatalf("plain WebSocket through the configured resolver: %v", err)
		}
		defer connection.Close()
		accepted := receiveFamilyConnection(t, platform, 5*time.Second)
		if accepted.remoteFamily != ipVersion || accepted.intent != "" {
			t.Fatalf("family-agnostic connection = %+v, want IPv%d with no pinned intent", accepted, ipVersion)
		}
		if queries.Load() == 0 {
			t.Fatal("plain WebSocket bypassed the configured wire resolver")
		}
		if _, ok := connection.UnderlyingConn().(*WebSocketWriteBatchConn); !ok {
			t.Fatalf("configured WebSocket lost its batching connection: %T", connection.UnderlyingConn())
		}

		// A refused lookup is final: the same reachable server must not gain
		// another connection through an unconfigured resolver or cached dial.
		lookupFailure := errors.New("owned plain-WebSocket resolver refused the lookup")
		failedSettings := DefaultClientStrategySettings()
		var refused atomic.Int32
		failedSettings.Resolver = &net.Resolver{
			PreferGo: true,
			Dial: func(context.Context, string, string) (net.Conn, error) {
				refused.Add(1)
				return nil, lookupFailure
			},
		}
		failedDialer := &clientDialer{settings: failedSettings}
		failedCtx, failedCancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer failedCancel()
		failedConnection, _, err := failedDialer.WsDialer(failedSettings).DialContext(failedCtx, platform.dualStackURL(), nil)
		if failedConnection != nil {
			failedConnection.Close()
			t.Fatal("a refused resolver produced a WebSocket connection")
		}
		if err == nil || !strings.Contains(err.Error(), lookupFailure.Error()) || refused.Load() == 0 {
			t.Fatalf("configured resolver refusal was lost: queries=%d err=%v", refused.Load(), err)
		}
		if got := platform.connectCount.Load(); got != 1 {
			t.Fatalf("resolver refusal admitted %d total WebSocket connections, want only the original", got)
		}
	})
}

func TestClientDialerPlainWebSocketPreservesInjectedDialer(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		platform := newTestingFamilyPlatformServer(t, ipVersion, false)
		settings := DefaultClientStrategySettings()
		var resolverCalls atomic.Int32
		settings.Resolver = &net.Resolver{
			PreferGo: true,
			Dial: func(context.Context, string, string) (net.Conn, error) {
				resolverCalls.Add(1)
				return nil, errors.New("the configured resolver must not override an injected dialer")
			},
		}
		type contextKey struct{}
		ctx, cancel := context.WithTimeout(context.WithValue(t.Context(), contextKey{}, "caller"), 4*time.Second)
		defer cancel()
		deadline, _ := ctx.Deadline()
		var injectedCalls atomic.Int32
		settings.DialContextSettings = &DialContextSettings{
			DialContext: func(dialCtx context.Context, network string, address string) (net.Conn, error) {
				injectedCalls.Add(1)
				if got, ok := dialCtx.Deadline(); !ok || !got.Equal(deadline) || dialCtx.Value(contextKey{}) != "caller" {
					return nil, errors.New("injected WebSocket dial lost its caller context")
				}
				if network != "tcp" || address != net.JoinHostPort(familyTransportTestHost, fmt.Sprint(platform.port)) {
					return nil, fmt.Errorf("injected WebSocket dial changed its target: %s %s", network, address)
				}
				return (&net.Dialer{}).DialContext(dialCtx, testTcpNetwork(ipVersion), testLoopbackHostPort(ipVersion, platform.port))
			},
		}
		dialer := &clientDialer{settings: settings}
		connection, _, err := dialer.WsDialer(settings).DialContext(ctx, platform.dualStackURL(), nil)
		if err != nil {
			t.Fatalf("injected plain WebSocket dial: %v", err)
		}
		defer connection.Close()
		accepted := receiveFamilyConnection(t, platform, 5*time.Second)
		if accepted.remoteFamily != ipVersion || accepted.intent != "" {
			t.Fatalf("injected family-agnostic connection = %+v", accepted)
		}
		if got := injectedCalls.Load(); got != 1 || resolverCalls.Load() != 0 {
			t.Fatalf("dial precedence changed: injected=%d resolver=%d", got, resolverCalls.Load())
		}

		cancel()
		canceledConnection, _, err := dialer.WsDialer(settings).DialContext(ctx, platform.dualStackURL(), nil)
		if canceledConnection != nil {
			canceledConnection.Close()
			t.Fatal("canceled injected dial produced a WebSocket connection")
		}
		if !errors.Is(err, context.Canceled) || injectedCalls.Load() != 2 || resolverCalls.Load() != 0 {
			t.Fatalf("injected cancellation changed: injected=%d resolver=%d err=%v", injectedCalls.Load(), resolverCalls.Load(), err)
		}
	})
}

func TestClientDialerPlainWebSocketCancelsConfiguredResolver(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		platform := newTestingFamilyPlatformServer(t, ipVersion, false)
		settings := DefaultClientStrategySettings()
		started := make(chan struct{})
		returned := make(chan struct{})
		var startOnce, returnOnce sync.Once
		settings.Resolver = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, _ string, _ string) (net.Conn, error) {
				startOnce.Do(func() { close(started) })
				<-ctx.Done()
				defer returnOnce.Do(func() { close(returned) })
				return nil, ctx.Err()
			},
		}
		dialer := &clientDialer{settings: settings}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		result := make(chan error, 1)
		go func() {
			connection, _, err := dialer.WsDialer(settings).DialContext(ctx, platform.dualStackURL(), nil)
			if connection != nil {
				connection.Close()
			}
			result <- err
		}()
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatal("plain WebSocket never reached its configured resolver")
		}
		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("configured resolver cancellation = %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("plain WebSocket did not return after caller cancellation")
		}
		select {
		case <-returned:
		case <-time.After(5 * time.Second):
			t.Fatal("canceled configured resolver did not return")
		}
		if got := platform.connectCount.Load(); got != 0 {
			t.Fatalf("canceled lookup admitted %d WebSocket connections", got)
		}
	})
}
