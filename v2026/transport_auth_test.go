package connect

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/urnetwork/connect/v2026/protocol"
)

// testingPlatformAuthWitness is the complete auth generation presented by one
// connection attempt.
type testingPlatformAuthWitness struct {
	byJwt      string
	instanceId string
	appVersion string
}

// testingPlatformH1AuthConnection pairs one header witness with the connection
// the test can close to force a future-auth reconnect.
type testingPlatformH1AuthConnection struct {
	witness testingPlatformAuthWitness
	conn    *websocket.Conn
}

// platformAuthWitness returns the comparable auth tuple used by assertions.
func platformAuthWitness(auth ClientAuth) testingPlatformAuthWitness {
	return testingPlatformAuthWitness{
		byJwt:      auth.ByJwt,
		instanceId: auth.InstanceId.String(),
		appVersion: auth.AppVersion,
	}
}

// receivePlatformAuthWitness bounds transport lifecycle failures without using
// a sleep as the ordering mechanism.
func receivePlatformAuthWitness[T any](t *testing.T, witnesses <-chan T, message string) T {
	t.Helper()
	select {
	case witness := <-witnesses:
		return witness
	case <-time.After(5 * time.Second):
		t.Fatal(message)
		var empty T
		return empty
	}
}

// requirePlatformAuthWitness pins all auth fields as one generation.
func requirePlatformAuthWitness(
	t *testing.T,
	got testingPlatformAuthWitness,
	want testingPlatformAuthWitness,
) {
	t.Helper()
	if got != want {
		t.Fatalf("auth witness = %+v, want %+v", got, want)
	}
}

// SetAuth and connection readers run concurrently in production. Every read
// must observe one immutable tuple, and caller mutation after either ownership
// transfer must not mutate the transport's stored generation.
func TestPlatformTransportAuthSnapshotsAreAtomicAndOwned(t *testing.T) {
	authA := ClientAuth{
		ByJwt:      "jwt-a",
		InstanceId: Id{0xa},
		AppVersion: "app-a",
	}
	authB := ClientAuth{
		ByJwt:      "jwt-b",
		InstanceId: Id{0xb},
		AppVersion: "app-b",
	}
	authC := ClientAuth{
		ByJwt:      "jwt-c",
		InstanceId: Id{0xc},
		AppVersion: "app-c",
	}

	constructorArg := authA
	transport := &PlatformTransport{auth: cloneClientAuth(&constructorArg)}
	constructorArg = authC
	if got := transport.authSnapshot(); got != authA {
		t.Fatalf("constructor auth = %+v, want owned %+v", got, authA)
	}

	setArg := authB
	transport.SetAuth(&setArg)
	setArg = authC
	if got := transport.authSnapshot(); got != authB {
		t.Fatalf("SetAuth auth = %+v, want owned %+v", got, authB)
	}

	writerStarted := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		close(writerStarted)
		for range 10_000 {
			transport.SetAuth(&authA)
			transport.SetAuth(&authB)
		}
		close(writerDone)
	}()
	<-writerStarted
	for {
		snapshot := transport.authSnapshot()
		if snapshot != authA && snapshot != authB {
			t.Fatalf("torn auth snapshot: %+v", snapshot)
		}
		select {
		case <-writerDone:
			return
		default:
		}
	}
}

// H1 header authentication must retain the current connection's coherent
// generation and use a later SetAuth snapshot on the next connection. Close
// joins the reconnect loop before the test inspects for extra attempts.
func TestPlatformTransportH1ReconnectUsesUpdatedAuthSnapshot(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		connections := make(chan testingPlatformH1AuthConnection, 3)
		upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		server := newTestingLoopbackHttpServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
			conn, err := upgrader.Upgrade(w, request, nil)
			if err != nil {
				return
			}
			connections <- testingPlatformH1AuthConnection{
				witness: testingPlatformAuthWitness{
					byJwt:      request.Header.Get("Authorization"),
					instanceId: request.Header.Get("X-UR-InstanceId"),
					appVersion: request.Header.Get("X-UR-AppVersion"),
				},
				conn: conn,
			}
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					conn.Close()
					return
				}
			}
		}), false)
		defer server.Close()

		authA := ClientAuth{ByJwt: "jwt-a", InstanceId: Id{0xa}, AppVersion: "app-a"}
		authB := ClientAuth{ByJwt: "jwt-b", InstanceId: Id{0xb}, AppVersion: "app-b"}
		authC := ClientAuth{ByJwt: "jwt-c", InstanceId: Id{0xc}, AppVersion: "app-c"}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		strategySettings := DefaultClientStrategySettings()
		strategySettings.EnableNormal = true
		strategySettings.EnableResilient = false
		strategy := NewClientStrategy(ctx, strategySettings)
		defer strategy.Close()
		settings := testingPlatformTransportSettings()
		settings.ReconnectTimeout = time.Millisecond
		constructorArg := authA
		transport := NewPlatformTransportWithTargetMode(
			ctx,
			strategy,
			NewRouteManager(ctx, "auth-h1"),
			"ws"+server.URL[len("http"):],
			&constructorArg,
			TransportModeH1,
			settings,
		)
		constructorArg = authC

		first := receivePlatformAuthWitness(t, connections, "H1 did not present its initial auth")
		requirePlatformAuthWitness(t, first.witness, testingPlatformAuthWitness{
			byJwt:      "Bearer " + authA.ByJwt,
			instanceId: authA.InstanceId.String(),
			appVersion: authA.AppVersion,
		})

		replacementArg := authB
		transport.SetAuth(&replacementArg)
		replacementArg = authC
		if err := first.conn.Close(); err != nil {
			t.Fatal(err)
		}
		second := receivePlatformAuthWitness(t, connections, "H1 did not reconnect with updated auth")
		requirePlatformAuthWitness(t, second.witness, testingPlatformAuthWitness{
			byJwt:      "Bearer " + authB.ByJwt,
			instanceId: authB.InstanceId.String(),
			appVersion: authB.AppVersion,
		})

		closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelClose()
		if err := transport.CloseAndWait(closeCtx); err != nil {
			t.Fatalf("H1 transport did not join: %v", err)
		}
		select {
		case extra := <-connections:
			t.Fatalf("H1 connected after close with %+v", extra.witness)
		default:
		}
	})
}

// H3 takes one auth snapshot before building its frame. Updating auth while the
// first attempt is blocked affects the next attempt, never the in-flight frame;
// closing the transport releases and joins the blocked second attempt.
func TestPlatformTransportH3ReconnectUsesUpdatedAuthSnapshot(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		authFrames := make(chan testingPlatformAuthWitness, 3)
		decodeErrors := make(chan error, 1)
		firstFactoryStarted := make(chan struct{})
		releaseFirstFactory := make(chan struct{})
		secondFactoryStarted := make(chan struct{})
		var factoryCount atomic.Int64

		settings := testingPlatformTransportSettings()
		settings.ReconnectTimeout = time.Millisecond
		settings.resolveH3AddrsForTest = testingH3LoopbackResolver(ipVersion, settings.H3Port)
		settings.AuthFrameObserver = func(authFrameBytes []byte) {
			decoded, err := DecodeFrame(authFrameBytes)
			if err != nil {
				decodeErrors <- err
				return
			}
			authMessage, ok := decoded.(*protocol.Auth)
			if !ok {
				decodeErrors <- fmt.Errorf("auth frame type = %T", decoded)
				return
			}
			instanceId, err := IdFromBytes(authMessage.InstanceId)
			if err != nil {
				decodeErrors <- err
				return
			}
			authFrames <- testingPlatformAuthWitness{
				byJwt:      authMessage.ByJwt,
				instanceId: instanceId.String(),
				appVersion: authMessage.AppVersion,
			}
		}
		settings.H3PacketConnFactory = func(factoryCtx context.Context) (net.PacketConn, error) {
			switch factoryCount.Add(1) {
			case 1:
				close(firstFactoryStarted)
				select {
				case <-releaseFirstFactory:
					return nil, errors.New("force H3 auth reconnect")
				case <-factoryCtx.Done():
					return nil, factoryCtx.Err()
				}
			case 2:
				close(secondFactoryStarted)
				<-factoryCtx.Done()
				return nil, factoryCtx.Err()
			default:
				return nil, errors.New("unexpected extra H3 auth attempt")
			}
		}

		authA := ClientAuth{ByJwt: "jwt-a", InstanceId: Id{0xa}, AppVersion: "app-a"}
		authB := ClientAuth{ByJwt: "jwt-b", InstanceId: Id{0xb}, AppVersion: "app-b"}
		authC := ClientAuth{ByJwt: "jwt-c", InstanceId: Id{0xc}, AppVersion: "app-c"}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		strategySettings := DefaultClientStrategySettings()
		strategySettings.EnableNormal = true
		strategySettings.EnableResilient = false
		strategy := NewClientStrategy(ctx, strategySettings)
		defer strategy.Close()
		constructorArg := authA
		transport := NewPlatformTransportWithTargetMode(
			ctx,
			strategy,
			NewRouteManager(ctx, "auth-h3"),
			"https://"+testLoopbackHost(ipVersion),
			&constructorArg,
			TransportModeH3,
			settings,
		)
		constructorArg = authC

		first := receivePlatformAuthWitness(t, authFrames, "H3 did not build its initial auth frame")
		requirePlatformAuthWitness(t, first, platformAuthWitness(authA))
		receivePlatformAuthWitness(t, firstFactoryStarted, "H3 initial auth did not reach its packet factory")

		replacementArg := authB
		transport.SetAuth(&replacementArg)
		replacementArg = authC
		close(releaseFirstFactory)
		second := receivePlatformAuthWitness(t, authFrames, "H3 did not build its replacement auth frame")
		requirePlatformAuthWitness(t, second, platformAuthWitness(authB))
		receivePlatformAuthWitness(t, secondFactoryStarted, "H3 replacement auth did not reach its packet factory")

		closeCtx, cancelClose := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelClose()
		if err := transport.CloseAndWait(closeCtx); err != nil {
			t.Fatalf("H3 transport did not join: %v", err)
		}
		select {
		case err := <-decodeErrors:
			t.Fatal(err)
		default:
		}
		if got := factoryCount.Load(); got != 2 {
			t.Fatalf("H3 packet factory calls = %d, want 2", got)
		}
		select {
		case extra := <-authFrames:
			t.Fatalf("H3 built auth after close: %+v", extra)
		default:
		}
	})
}
