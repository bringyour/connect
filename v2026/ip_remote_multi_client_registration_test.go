package connect

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// The first setup cancellation subscription belongs to the provide ack; the
// second is the actual key-registration wait. Observing that subscription
// avoids using a sleep to decide whether setup admitted an unregistered key.
type registrationSetupContext struct {
	context.Context
	doneCalls atomic.Int32
	waiting   chan struct{}
}

func (c *registrationSetupContext) Done() <-chan struct{} {
	if c.doneCalls.Add(1) == 2 {
		close(c.waiting)
	}
	return c.Context.Done()
}

type registrationSetupResult struct {
	client *Client
	err    error
}

func newGeneratorRegistrationFixture(t *testing.T, ipVersion int, beforeClientJoin func()) (*ApiMultiClientGenerator, *Client, *fakeWindowPlatformTransport, *registrationSetupContext, context.CancelFunc, <-chan registrationSetupResult, <-chan clientKeyRegistrationHttpAttempt) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	attempts := make(chan clientKeyRegistrationHttpAttempt, 4)
	endpoint := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hello" || r.URL.Path == "/network/remove-client" {
			_, _ = io.WriteString(w, `{}`)
			return
		}
		var args ConnectControlArgs
		if r.URL.Path != "/connect/control" || json.NewDecoder(r.Body).Decode(&args) != nil {
			http.Error(w, "invalid control", http.StatusBadRequest)
			return
		}
		raw, err := base64.StdEncoding.DecodeString(args.Pack)
		var pack protocol.Pack
		if err != nil || ProtoUnmarshal(raw, &pack) != nil || len(pack.Frames) != 1 {
			t.Error("invalid control pack")
			http.Error(w, "invalid pack", http.StatusBadRequest)
			return
		}
		message, err := FromFrame(pack.Frames[0])
		if err != nil {
			t.Error(err)
			return
		}
		if key, ok := message.(*protocol.ClientKey); ok {
			attempt := clientKeyRegistrationHttpAttempt{key: key.PublicKey, response: make(chan string, 1), done: make(chan struct{})}
			defer close(attempt.done)
			select {
			case attempts <- attempt:
			case <-r.Context().Done():
				return
			}
			select {
			case response := <-attempt.response:
				_, _ = io.WriteString(w, response)
			case <-r.Context().Done():
			}
			return
		}
		// Provide-secret registration succeeds independently of the held key.
		_, _ = io.WriteString(w, `{"pack":"","error":null}`)
	}))
	strategy := NewClientStrategyWithDefaults(ctx)
	generator := NewApiMultiClientGenerator(ctx, nil, strategy, nil, endpoint.URL,
		"synthetic-generator-token", endpoint.URL, "test-device", "test-spec", "test-version",
		nil, DefaultClientSettings, DefaultApiMultiClientGeneratorSettings())
	transport := newFakeWindowPlatformTransport(true)
	constructed := make(chan *Client, 1)
	publish := make(chan struct{})
	generator.newPlatformTransport = func(client *Client, _ *ClientAuth, _ TransportMode, _ *PlatformTransportSettings) apiWindowPlatformTransport {
		client.ClientKeyManager().registrationSync.retryTimeout = 0
		constructed <- client
		close(publish)
		return transport
	}
	settings := closeWaitClientSettings()
	settings.ClientKeyRegistrationRequired = true
	settings.beforeClientKeyPublishForTest = func() { <-publish }
	settings.beforeRunDoneWaitForTest = beforeClientJoin
	callCtx, callCancel := context.WithCancel(ctx)
	observedCtx := &registrationSetupContext{Context: callCtx, waiting: make(chan struct{})}
	args := &MultiClientGeneratorClientArgs{ClientId: NewId(), ClientAuth: &ClientAuth{
		ByJwt: "synthetic-derived-token", InstanceId: NewId(), AppVersion: "test-version",
	}}
	result := make(chan registrationSetupResult, 1)
	go func() {
		client, err := generator.NewClientContext(ctx, observedCtx, args, settings)
		result <- registrationSetupResult{client: client, err: err}
	}()
	var client *Client
	select {
	case client = <-constructed:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	t.Cleanup(func() {
		callCancel()
		generator.transportLock.Lock()
		_, tracked := generator.transports[client]
		generator.transportLock.Unlock()
		if tracked {
			generator.RemoveClientWithArgs(client, args)
			client.Cancel()
		}
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer joinCancel()
		if err := generator.CloseAndWait(joinCtx); err != nil {
			t.Error(err)
		}
		cancel()
		strategy.Close()
		endpoint.Close()
	})
	return generator, client, transport, observedCtx, callCancel, result, attempts
}

func waitForGeneratedKeyRegistration(t *testing.T, ctx *registrationSetupContext, result <-chan registrationSetupResult) {
	t.Helper()
	select {
	case <-ctx.waiting:
	case got := <-result:
		t.Fatalf("setup returned before processed key registration: client=%t, err=%v", got.client != nil, got.err)
	case <-ctx.Context.Done():
		t.Fatal(ctx.Err())
	}
}

func TestApiMultiClientGeneratorWaitsForProcessedKeyRegistration(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testApiMultiClientGeneratorWaitsForProcessedKeyRegistration(t, ipVersion)
	})
}

func testApiMultiClientGeneratorWaitsForProcessedKeyRegistration(t *testing.T, ipVersion int) {
	_, client, _, callCtx, callCancel, result, attempts := newGeneratorRegistrationFixture(t, ipVersion, nil)
	first := nextClientKeyRegistrationAttempt(t, attempts)
	waitForGeneratedKeyRegistration(t, callCtx, result)
	first.response <- `{"pack":"","error":{"message":"key publication failed"}}`
	retry := nextClientKeyRegistrationAttempt(t, attempts)
	if client.ClientKeyManager().Registered() {
		t.Fatal("application failure admitted the derived key")
	}
	select {
	case got := <-result:
		t.Fatalf("unprocessed key admitted client: %+v", got)
	default:
	}
	retry.response <- `{"pack":"","error":null}`
	select {
	case got := <-result:
		if got.err != nil || got.client != client || !client.ClientKeyManager().Registered() {
			t.Fatalf("processed key did not admit the exact client: %v", got.err)
		}
	case <-callCtx.Context.Done():
		t.Fatal(callCtx.Err())
	}
	callCancel()
	if client.Ctx().Err() != nil {
		t.Fatal("completed setup context canceled the long-lived client")
	}
}

func TestApiMultiClientGeneratorRegistrationCancellationJoinsCleanup(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		testApiMultiClientGeneratorRegistrationCancellationJoinsCleanup(t, ipVersion)
	})
}

func testApiMultiClientGeneratorRegistrationCancellationJoinsCleanup(t *testing.T, ipVersion int) {
	joinEntered, releaseJoin := make(chan struct{}), make(chan struct{})
	var enteredOnce, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(releaseJoin) })
	generator, client, transport, callCtx, callCancel, result, attempts := newGeneratorRegistrationFixture(t, ipVersion, func() {
		enteredOnce.Do(func() { close(joinEntered) })
		<-releaseJoin
	})
	first := nextClientKeyRegistrationAttempt(t, attempts)
	waitForGeneratedKeyRegistration(t, callCtx, result)
	callCancel()
	select {
	case got := <-result:
		if got.client != nil || !errors.Is(got.err, context.Canceled) {
			t.Fatalf("canceled key setup = (%v, %v)", got.client, got.err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("canceled registration did not return")
	}
	waitCloseWaitBarrier(t, t.Context(), joinEntered, "retired client join")
	select {
	case <-transport.closed:
	default:
		t.Fatal("failed setup retained its platform transport")
	}
	joined := make(chan error, 1)
	go func() { joined <- generator.CloseAndWait(t.Context()) }()
	requireCloseWaitBlocked(t, joined, "generator retirement")
	releaseOnce.Do(func() { close(releaseJoin) })
	waitCloseWaitResult(t, t.Context(), joined, "generator registration cleanup")
	waitCloseWaitBarrier(t, t.Context(), first.done, "canceled registration HTTP request")
	if client.ClientKeyManager().Registered() {
		t.Fatal("canceled setup retained registration readiness")
	}
}
