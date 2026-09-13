package connect

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestClientDialerWebSocketUsesConnectSettingsResolver(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		for _, secure := range []bool{false, true} {
			name := "ws"
			if secure {
				name = "wss"
			}
			accepted := make(chan struct{}, 1)
			upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
			server := newFamilyHttptestUnstartedServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				connection, err := upgrader.Upgrade(w, r, nil)
				if err != nil {
					return
				}
				defer connection.Close()
				accepted <- struct{}{}
			}))
			if secure {
				server.StartTLS()
			} else {
				server.Start()
			}

			settings := DefaultClientStrategySettings()
			settings.ConnectSettings.Resolver = newFamilyTestResolver(t, testLoopbackAddr(ipVersion))
			if secure {
				serverTransport, ok := server.Client().Transport.(*http.Transport)
				if !ok {
					server.Close()
					t.Fatalf("unexpected test server transport type %T", server.Client().Transport)
				}
				settings.TlsConfig = serverTransport.TLSClientConfig.Clone()
				settings.TlsConfig.InsecureSkipVerify = true // test-only certificate has a different synthetic name
			}
			dialer := &clientDialer{
				dialTlsContext: newNormalDialTlsContext(settings, clientWebSocketNextProtos),
				settings:       settings,
			}
			port := server.Listener.Addr().(*net.TCPAddr).Port
			url := fmt.Sprintf("%s://websocket-resolver.example.test:%d", name, port)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			connection, response, err := dialer.WsDialer(settings).DialContext(ctx, url, nil)
			if response != nil && response.Body != nil {
				response.Body.Close()
			}
			if err != nil {
				cancel()
				server.Close()
				t.Fatalf("%s websocket through the configured resolver: %s", name, err)
			}
			select {
			case <-accepted:
			case <-ctx.Done():
				connection.Close()
				cancel()
				server.Close()
				t.Fatal(ctx.Err())
			}
			connection.Close()
			cancel()
			server.Close()
		}
	})
}

func TestClientDialerPlainWebSocketPreservesInjectedDialContext(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		accepted := make(chan struct{}, 1)
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		server := newFamilyHttptestUnstartedServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			connection, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer connection.Close()
			accepted <- struct{}{}
		}))
		server.Start()
		defer server.Close()

		var resolverCalls atomic.Int32
		var dialCalls atomic.Int32
		settings := DefaultClientStrategySettings()
		settings.ConnectSettings.Resolver = &net.Resolver{
			PreferGo: true,
			Dial: func(context.Context, string, string) (net.Conn, error) {
				resolverCalls.Add(1)
				return nil, errors.New("unexpected resolver call")
			},
		}
		settings.ConnectSettings.DialContextSettings = &DialContextSettings{
			DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
				dialCalls.Add(1)
				if network != "tcp" {
					t.Errorf("network = %q, want tcp", network)
				}
				if address != "injected-websocket.example.test:443" {
					t.Errorf("address = %q, want the original authority", address)
				}
				return (&net.Dialer{}).DialContext(ctx, testTcpNetwork(ipVersion), server.Listener.Addr().String())
			},
		}
		dialer := &clientDialer{settings: settings}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		connection, response, err := dialer.WsDialer(settings).DialContext(
			ctx,
			"ws://injected-websocket.example.test:443",
			nil,
		)
		if response != nil && response.Body != nil {
			defer response.Body.Close()
		}
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()
		select {
		case <-accepted:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if got := dialCalls.Load(); got != 1 {
			t.Fatalf("injected dial calls = %d, want 1", got)
		}
		if got := resolverCalls.Load(); got != 0 {
			t.Fatalf("resolver calls = %d, want 0: explicit dial must remain authoritative", got)
		}
	})
}

func TestClientDialerPlainHttpUsesConnectSettingsResolver(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		server := newFamilyHttptestUnstartedServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}))
		server.Start()
		defer server.Close()

		settings := DefaultClientStrategySettings()
		settings.ConnectSettings.Resolver = newFamilyTestResolver(t, testLoopbackAddr(ipVersion))
		dialer := &clientDialer{settings: settings}
		client := dialer.HttpClient()
		defer client.CloseIdleConnections()

		port := server.Listener.Addr().(*net.TCPAddr).Port
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		request, err := http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			fmt.Sprintf("http://http-resolver.example.test:%d", port),
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatalf("plain http through the configured resolver: %s", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want %d", response.StatusCode, http.StatusOK)
		}
	})
}

func TestClientDialerPlainHttpPreservesInjectedDialContext(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		server := newFamilyHttptestUnstartedServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		server.Start()
		defer server.Close()

		var resolverCalls atomic.Int32
		var dialCalls atomic.Int32
		settings := DefaultClientStrategySettings()
		settings.ConnectSettings.Resolver = &net.Resolver{
			PreferGo: true,
			Dial: func(context.Context, string, string) (net.Conn, error) {
				resolverCalls.Add(1)
				return nil, errors.New("unexpected resolver call")
			},
		}
		settings.ConnectSettings.DialContextSettings = &DialContextSettings{
			DialContext: func(ctx context.Context, network string, address string) (net.Conn, error) {
				dialCalls.Add(1)
				if network != "tcp" {
					t.Errorf("network = %q, want tcp", network)
				}
				if address != "injected-http.example.test:80" {
					t.Errorf("address = %q, want the original authority", address)
				}
				return (&net.Dialer{}).DialContext(ctx, testTcpNetwork(ipVersion), server.Listener.Addr().String())
			},
		}
		dialer := &clientDialer{settings: settings}
		client := dialer.HttpClient()
		defer client.CloseIdleConnections()

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		request, err := http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			"http://injected-http.example.test:80",
			nil,
		)
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if got := dialCalls.Load(); got != 1 {
			t.Fatalf("injected dial calls = %d, want 1", got)
		}
		if got := resolverCalls.Load(); got != 0 {
			t.Fatalf("resolver calls = %d, want 0: explicit dial must remain authoritative", got)
		}
	})
}

func TestClientDialerHttpClientUsesHttp2WithCustomTlsDialer(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		connectionCount := atomic.Int32{}
		server := newFamilyHttptestUnstartedServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("ok"))
		}))
		server.EnableHTTP2 = true
		server.Config.ConnState = func(conn net.Conn, state http.ConnState) {
			if state == http.StateNew {
				connectionCount.Add(1)
			}
		}
		server.StartTLS()
		defer server.Close()

		serverTransport, ok := server.Client().Transport.(*http.Transport)
		if !ok {
			t.Fatalf("unexpected test server transport type %T", server.Client().Transport)
		}
		tlsConfig := serverTransport.TLSClientConfig.Clone()
		settings := DefaultClientStrategySettings()
		settings.TlsConfig = tlsConfig
		dialer := &clientDialer{
			dialTlsContext:     newNormalDialTlsContext(settings, nil),
			httpDialTlsContext: newNormalDialTlsContext(settings, clientHttpNextProtos),
			settings:           settings,
		}
		client := dialer.HttpClient()
		defer client.CloseIdleConnections()

		response, err := client.Get(server.URL)
		if err != nil {
			t.Fatalf("warm request: %s", err)
		}
		if _, err := io.Copy(io.Discard, response.Body); err != nil {
			response.Body.Close()
			t.Fatalf("read warm response: %s", err)
		}
		if err := response.Body.Close(); err != nil {
			t.Fatalf("close warm response: %s", err)
		}
		if response.ProtoMajor != 2 {
			t.Fatalf("custom TLS dialer negotiated HTTP/%d, expected HTTP/2", response.ProtoMajor)
		}

		const requestCount = 16
		start := make(chan struct{})
		errs := make(chan error, requestCount)
		var waitGroup sync.WaitGroup
		for range requestCount {
			waitGroup.Add(1)
			go func() {
				defer waitGroup.Done()
				<-start
				request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
				if err != nil {
					errs <- err
					return
				}
				response, err := client.Do(request)
				if err != nil {
					errs <- err
					return
				}
				_, readErr := io.Copy(io.Discard, response.Body)
				closeErr := response.Body.Close()
				if readErr != nil {
					errs <- readErr
					return
				}
				if closeErr != nil {
					errs <- closeErr
				}
			}()
		}
		close(start)
		waitGroup.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("parallel request: %s", err)
			}
		}

		if got := connectionCount.Load(); got != 1 {
			t.Fatalf("HTTP/2 requests used %d TLS connections, expected one", got)
		}
	})
}

func TestClientDialerWebSocketForcesHttp11Alpn(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		upgrader := websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		}
		server := newFamilyHttptestUnstartedServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			connection, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer connection.Close()
			_ = connection.WriteMessage(websocket.TextMessage, []byte("ok"))
		}))
		server.EnableHTTP2 = true
		server.StartTLS()
		defer server.Close()

		serverTransport, ok := server.Client().Transport.(*http.Transport)
		if !ok {
			t.Fatalf("unexpected test server transport type %T", server.Client().Transport)
		}
		tlsConfig := serverTransport.TLSClientConfig.Clone()
		// Model a caller that already enabled h2 on its shared base config. The
		// WebSocket-specific derivative must replace this list, not inherit it.
		tlsConfig.NextProtos = []string{"h2", "http/1.1"}
		settings := DefaultClientStrategySettings()
		settings.TlsConfig = tlsConfig
		dialer := &clientDialer{
			dialTlsContext:     newNormalDialTlsContext(settings, clientWebSocketNextProtos),
			httpDialTlsContext: newNormalDialTlsContext(settings, clientHttpNextProtos),
			settings:           settings,
		}

		webSocketUrl := "wss" + strings.TrimPrefix(server.URL, "https")
		connection, response, err := dialer.WsDialer(settings).DialContext(
			context.Background(),
			webSocketUrl,
			nil,
		)
		if response != nil && response.Body != nil {
			defer response.Body.Close()
		}
		if err != nil {
			t.Fatalf("WebSocket dial against an h2-capable server: %s", err)
		}
		defer connection.Close()

		batchConnection, ok := connection.UnderlyingConn().(*WebSocketWriteBatchConn)
		if !ok {
			t.Fatalf("unexpected WebSocket transport type %T", connection.UnderlyingConn())
		}
		tlsConnection, ok := batchConnection.conn.(*tls.Conn)
		if !ok {
			t.Fatalf("unexpected batched WebSocket transport type %T", batchConnection.conn)
		}
		if negotiated := tlsConnection.ConnectionState().NegotiatedProtocol; negotiated == "h2" {
			t.Fatal("WebSocket negotiated h2 underneath its HTTP/1.1 upgrade")
		}
		_, message, err := connection.ReadMessage()
		if err != nil {
			t.Fatalf("read WebSocket message: %s", err)
		}
		if string(message) != "ok" {
			t.Fatalf("WebSocket message = %q, expected ok", message)
		}
	})
}

func TestClientDialerWebSocketBatchPreservesMessageBoundaries(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		const messageCount = platformWebSocketWriteBatchMaxMessages
		received := make(chan [][]byte, 1)
		upgrader := websocket.Upgrader{
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		}
		server := newFamilyHttptestUnstartedServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			connection, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				return
			}
			defer connection.Close()

			messages := make([][]byte, 0, messageCount)
			for range messageCount {
				messageType, message, err := connection.ReadMessage()
				if err != nil {
					return
				}
				if messageType != websocket.BinaryMessage {
					return
				}
				messages = append(messages, message)
			}
			received <- messages
		}))
		server.StartTLS()
		defer server.Close()

		serverTransport, ok := server.Client().Transport.(*http.Transport)
		if !ok {
			t.Fatalf("unexpected test server transport type %T", server.Client().Transport)
		}
		settings := DefaultClientStrategySettings()
		settings.TlsConfig = serverTransport.TLSClientConfig.Clone()
		dialer := &clientDialer{
			dialTlsContext: newNormalDialTlsContext(
				settings,
				clientWebSocketNextProtos,
			),
			settings: settings,
		}
		webSocketUrl := "wss" + strings.TrimPrefix(server.URL, "https")
		connection, response, err := dialer.WsDialer(settings).DialContext(
			context.Background(),
			webSocketUrl,
			nil,
		)
		if response != nil && response.Body != nil {
			defer response.Body.Close()
		}
		if err != nil {
			t.Fatal(err)
		}
		defer connection.Close()

		batchConnection, ok := connection.UnderlyingConn().(*WebSocketWriteBatchConn)
		if !ok {
			t.Fatalf("unexpected WebSocket transport type %T", connection.UnderlyingConn())
		}
		expected := make([][]byte, 0, messageCount)
		batchConnection.BeginWriteBatch()
		for i := range messageCount {
			message := []byte{byte(i), byte(i + 1), byte(i + 2)}
			expected = append(expected, message)
			if err := connection.WriteMessage(websocket.BinaryMessage, message); err != nil {
				t.Fatal(err)
			}
		}
		if err := batchConnection.FlushWriteBatch(); err != nil {
			t.Fatal(err)
		}

		select {
		case actual := <-received:
			if len(actual) != len(expected) {
				t.Fatalf("received %d messages, expected %d", len(actual), len(expected))
			}
			for i := range expected {
				if !bytes.Equal(actual[i], expected[i]) {
					t.Fatalf("message %d = %x, expected %x", i, actual[i], expected[i])
				}
			}
		case <-time.After(5 * time.Second):
			t.Fatal("server did not receive the coalesced WebSocket messages")
		}
	})
}

func TestClientTlsConfigsUseIndependentBoundedSessionCaches(t *testing.T) {
	callerCache := tls.NewLRUClientSessionCache(1)
	base := &tls.Config{
		ClientSessionCache: callerCache,
		NextProtos:         []string{"caller"},
	}

	httpConfig := newClientTlsConfig(base, clientHttpNextProtos)
	webSocketConfig := newClientTlsConfig(base, clientWebSocketNextProtos)

	if httpConfig.ClientSessionCache == nil || webSocketConfig.ClientSessionCache == nil {
		t.Fatal("derived TLS configs must carry bounded session caches")
	}
	if httpConfig.ClientSessionCache == webSocketConfig.ClientSessionCache {
		t.Fatal("HTTP and WebSocket TLS paths unexpectedly share a session cache")
	}
	if httpConfig.ClientSessionCache == callerCache || webSocketConfig.ClientSessionCache == callerCache {
		t.Fatal("derived TLS path reused the caller's cross-path session cache")
	}
	if base.ClientSessionCache != callerCache {
		t.Fatal("newClientTlsConfig mutated the caller's session cache")
	}
	if got := strings.Join(httpConfig.NextProtos, ","); got != "h2,http/1.1" {
		t.Fatalf("HTTP ALPN list = %q, expected h2,http/1.1", got)
	}
	if got := strings.Join(webSocketConfig.NextProtos, ","); got != "http/1.1" {
		t.Fatalf("WebSocket ALPN list = %q, expected http/1.1", got)
	}
	if got := strings.Join(base.NextProtos, ","); got != "caller" {
		t.Fatalf("newClientTlsConfig mutated caller ALPN list to %q", got)
	}
}
