// Actual Http control responses drive the real key manager and retry owner.
// Channels expose request completion, not scheduler timing or delivery acks.
package connect

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Owns one actual pending Http request and its bounded response bytes.
type clientKeyRegistrationHttpAttempt struct {
	key      []byte
	response chan string
	done     chan struct{}
}

// Construction uses the real ApiOutOfBandControl. The existing private
// publisher barrier selects a zero retry delay before any send starts.
func newClientKeyRegistrationHttpFixture(t *testing.T, ipVersion int) (*Client, *ApiOutOfBandControl, <-chan clientKeyRegistrationHttpAttempt, *atomic.Int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	attempts := make(chan clientKeyRegistrationHttpAttempt, 4)
	count := &atomic.Int64{}
	endpoint := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hello" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method != http.MethodPost || r.URL.Path != "/connect/control" {
			http.NotFound(w, r)
			return
		}
		var args ConnectControlArgs
		if err := json.NewDecoder(io.LimitReader(r.Body, 16*1024)).Decode(&args); err != nil {
			t.Error(err)
			http.Error(w, "invalid control", 400)
			return
		}
		raw, err := base64.StdEncoding.DecodeString(args.Pack)
		if err != nil {
			t.Error(err)
			http.Error(w, "invalid pack", 400)
			return
		}
		var pack protocol.Pack
		if err := ProtoUnmarshal(raw, &pack); err != nil || len(pack.Frames) != 1 {
			t.Error("invalid frame census", err)
			http.Error(w, "invalid frames", 400)
			return
		}
		message, err := FromFrame(pack.Frames[0])
		if err != nil {
			t.Error(err)
			http.Error(w, "invalid frame", 400)
			return
		}
		key, ok := message.(*protocol.ClientKey)
		if !ok || len(key.PublicKey) != ed25519.PublicKeySize {
			t.Error("control is not an actual key")
			http.Error(w, "wrong control", 400)
			return
		}
		attempt := clientKeyRegistrationHttpAttempt{key: bytes.Clone(key.PublicKey), response: make(chan string, 1), done: make(chan struct{})}
		defer close(attempt.done)
		count.Add(1)
		select {
		case attempts <- attempt:
		case <-r.Context().Done():
			return
		}
		select {
		case response := <-attempt.response:
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, response)
		case <-r.Context().Done():
		}
	}))
	strategy := NewClientStrategyWithDefaults(ctx)
	control := NewApiOutOfBandControl(ctx, strategy, "synthetic-registration-token", endpoint.URL)
	settings := closeWaitClientSettings()
	settings.ClientKeyRegistrationRequired = true
	settings.ClientKeySeed = bytes.Repeat([]byte{17}, ed25519.SeedSize)
	publish := make(chan struct{})
	settings.beforeClientKeyPublishForTest = func() { <-publish }
	client := NewClient(ctx, NewId(), control, settings)
	if client.ClientKeyManager() == nil {
		cancel()
		endpoint.Close()
		t.Fatal("actual registration manager was not constructed")
	}
	client.ClientKeyManager().registrationSync.retryTimeout = 0
	close(publish)
	t.Cleanup(func() {
		cancel()
		joinCtx, joinCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer joinCancel()
		if err := client.CloseAndWait(joinCtx); err != nil {
			t.Error(err)
		}
		if err := control.CloseAndWait(joinCtx); err != nil {
			t.Error(err)
		}
		strategy.Close()
		endpoint.Close()
	})
	return client, control, attempts, count
}

// A liveness deadline does not decide the asserted ordering.
func nextClientKeyRegistrationAttempt(t *testing.T, attempts <-chan clientKeyRegistrationHttpAttempt) clientKeyRegistrationHttpAttempt {
	t.Helper()
	select {
	case attempt := <-attempts:
		return attempt
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	case <-time.After(10 * time.Second):
		t.Fatal("actual registration request did not arrive")
	}
	return clientKeyRegistrationHttpAttempt{}
}

// The source-derived failure was treating a delivered frame or application
// error inside a valid response as successful registration.
func TestClientKeyRegistrationRequiresProcessedHttpResponse(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		client, _, attempts, count := newClientKeyRegistrationHttpFixture(t, ipVersion)
		manager := client.ClientKeyManager()
		first := nextClientKeyRegistrationAttempt(t, attempts)
		if manager.Registered() {
			t.Fatal("in-flight control was treated as durable registration")
		}
		first.response <- `{"pack":"","error":{"message":"original history publication failed"}}`
		retry := nextClientKeyRegistrationAttempt(t, attempts)
		if manager.Registered() || !bytes.Equal(first.key, retry.key) {
			t.Fatal("application failure changed key or claimed readiness")
		}
		retry.response <- `{"pack":"","error":null}`
		if err := manager.WaitForRegistration(t.Context()); err != nil {
			t.Fatal(err)
		}
		if !manager.Registered() || count.Load() != 2 {
			t.Fatal("processed exact retry did not become ready once", count.Load())
		}
	})
}

// A pending key finishes before the coalesced latest key is sent; the old
// completion cannot satisfy the new identity's readiness.
func TestClientKeyRegistrationSerializesAndCoalescesRotation(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		client, _, attempts, count := newClientKeyRegistrationHttpFixture(t, ipVersion)
		manager := client.ClientKeyManager()
		first := nextClientKeyRegistrationAttempt(t, attempts)
		for _, value := range []byte{18, 19} {
			if err := manager.SetSeed(bytes.Repeat([]byte{value}, ed25519.SeedSize)); err != nil {
				t.Fatal(err)
			}
			if manager.Registered() {
				t.Fatal("rotation retained old readiness")
			}
		}
		latest := manager.PublicKey()
		first.response <- `{"pack":""}`
		second := nextClientKeyRegistrationAttempt(t, attempts)
		if !bytes.Equal(second.key, latest) || bytes.Equal(first.key, latest) || manager.Registered() {
			t.Fatal("old completion or nonlatest publisher won the rotation")
		}
		second.response <- `{"pack":""}`
		if err := manager.WaitForRegistration(t.Context()); err != nil {
			t.Fatal(err)
		}
		if count.Load() != 2 {
			t.Fatal("one publisher was retained per superseded seed", count.Load())
		}
		if err := manager.SetSeed(bytes.Repeat([]byte{20}, ed25519.SeedSize)); err != nil {
			t.Fatal(err)
		}
		if manager.Registered() {
			t.Fatal("a ready key stayed ready after rotation")
		}
		third := nextClientKeyRegistrationAttempt(t, attempts)
		third.response <- `{"pack":""}`
		if err := manager.WaitForRegistration(t.Context()); err != nil {
			t.Fatal(err)
		}
	})
}

// Joined owner shutdown leaves neither an in-flight Http request nor a late
// readiness mutation. The Api control owns callbacks after client launch.
func TestClientKeyRegistrationCancellationJoinsActualHttp(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		client, control, attempts, count := newClientKeyRegistrationHttpFixture(t, ipVersion)
		manager := client.ClientKeyManager()
		first := nextClientKeyRegistrationAttempt(t, attempts)
		joinCtx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		if err := client.CloseAndWait(joinCtx); err != nil {
			t.Fatal(err)
		}
		if err := control.CloseAndWait(joinCtx); err != nil {
			t.Fatal(err)
		}
		select {
		case <-first.done:
		case <-joinCtx.Done():
			t.Fatal(joinCtx.Err())
		}
		if manager.Registered() || count.Load() != 1 {
			t.Fatal("shutdown retained ready state or launched another request", count.Load())
		}
		if err := manager.WaitForRegistration(joinCtx); !errors.Is(err, context.Canceled) {
			t.Fatal("closed registration wait was not canceled", err)
		}
		if err := manager.SetSeed(bytes.Repeat([]byte{21}, ed25519.SeedSize)); err == nil {
			t.Fatal("closed registration accepted a rotation")
		}
	})
}

// Returning to identical key bytes is still a new local publication
// generation; the older in-flight response must not restore its readiness.
func TestClientKeyRegistrationSameKeyReturnNeedsFreshCompletion(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		client, _, attempts, count := newClientKeyRegistrationHttpFixture(t, ipVersion)
		manager := client.ClientKeyManager()
		first := nextClientKeyRegistrationAttempt(t, attempts)
		for _, value := range []byte{22, 17} {
			if err := manager.SetSeed(bytes.Repeat([]byte{value}, ed25519.SeedSize)); err != nil {
				t.Fatal(err)
			}
		}
		first.response <- `{"pack":""}`
		current := nextClientKeyRegistrationAttempt(t, attempts)
		if manager.Registered() || !bytes.Equal(first.key, current.key) {
			t.Fatal("old same-key acknowledgment satisfied another generation")
		}
		current.response <- `{"pack":""}`
		if err := manager.WaitForRegistration(t.Context()); err != nil {
			t.Fatal(err)
		}
		if count.Load() != 2 {
			t.Fatal("fresh generation did not use one original-key retry", count.Load())
		}
	})
}

// Refusal happens before changing the key or wrapping its local generation.
func TestClientKeyRegistrationRejectsGenerationOverflow(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		client, _, attempts, _ := newClientKeyRegistrationHttpFixture(t, ipVersion)
		manager := client.ClientKeyManager()
		_ = nextClientKeyRegistrationAttempt(t, attempts)
		manager.stateLock.Lock()
		current := manager.registrationUpdates.Value()
		current.generation = math.MaxUint64
		manager.registrationUpdates.Set(current)
		manager.stateLock.Unlock()
		original := manager.PublicKey()
		if err := manager.SetSeed(bytes.Repeat([]byte{23}, ed25519.SeedSize)); err == nil {
			t.Fatal("publication generation overflow was accepted")
		}
		if !bytes.Equal(manager.PublicKey(), original) || manager.registrationUpdates.Value() != current || manager.Registered() {
			t.Fatal("refused overflow changed key or readiness")
		}
	})
}

// Explicit opt-in cannot upgrade a custom delivery-only implementation into
// processed authority. Ordinary legacy clients remain constructible, unready.
func TestClientKeyRegistrationRejectsDeliveryOnlyControl(t *testing.T) {
	settings := closeWaitClientSettings()
	settings.ClientKeyRegistrationRequired = true
	client := &Client{settings: settings, clientOob: NewNoContractClientOob()}
	if manager, err := NewClientKeyManager(t.Context(), client); err == nil || manager != nil {
		t.Fatal("delivery-only control became processed registration")
	}
	legacy := NewClient(t.Context(), NewId(), NewNoContractClientOob(), closeWaitClientSettings())
	defer func() {
		if err := legacy.CloseAndWait(t.Context()); err != nil {
			t.Error(err)
		}
	}()
	if legacy.ClientKeyManager().Registered() {
		t.Fatal("legacy delivery path reported processed readiness")
	}
	if err := legacy.ClientKeyManager().WaitForRegistration(t.Context()); err == nil {
		t.Fatal("legacy registration wait succeeded")
	}
}
