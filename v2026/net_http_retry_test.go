package connect

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// a single-dialer strategy (normal only, no resilient variants) so each
// logical request maps to exactly one server hit, making hit counts exact
func newRetryTestStrategy(ctx context.Context) *ClientStrategy {
	settings := DefaultClientStrategySettings()
	settings.EnableNormal = true
	settings.EnableResilient = false
	settings.RequestTimeout = 5 * time.Second
	settings.GetRetryMinTimeout = 10 * time.Millisecond
	settings.GetRetryMaxTimeout = 30 * time.Millisecond
	return NewClientStrategy(ctx, settings)
}

// a 502/503 means the lb momentarily had no healthy upstream (the edge of a
// deploy); one jittered retry rides over it
func TestHttpGetWithStrategyRetriesGatewayStatus(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hitCount := atomic.Int64{}
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if hitCount.Add(1) == 1 {
				http.Error(w, "bad gateway", http.StatusBadGateway)
				return
			}
			fmt.Fprintf(w, "ok")
		}))
		defer server.Close()

		strategy := newRetryTestStrategy(ctx)

		bodyBytes, err := HttpGetWithStrategyRaw(ctx, strategy, server.URL+"/test", "")
		if err != nil {
			t.Fatalf("expected the retry to ride over the 502: %s", err)
		}
		if string(bodyBytes) != "ok" {
			t.Fatalf("bad body: %q", bodyBytes)
		}
		if c := hitCount.Load(); c != 2 {
			t.Fatalf("expected 2 hits (initial + one retry), got %d", c)
		}
	})
}

func TestHttpGetWithStrategyRetryExhausted(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hitCount := atomic.Int64{}
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hitCount.Add(1)
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
		}))
		defer server.Close()

		strategy := newRetryTestStrategy(ctx)

		_, err := HttpGetWithStrategyRaw(ctx, strategy, server.URL+"/test", "")
		var statusError *HttpStatusError
		if !errors.As(err, &statusError) || statusError.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("expected a 503 status error after retries, got %v", err)
		}
		// initial attempt + GetRetryCount retries
		if c := hitCount.Load(); c != 2 {
			t.Fatalf("expected 2 hits (initial + one retry), got %d", c)
		}
	})
}

// a deterministic application error (500) must surface immediately: retrying
// it doubles load and hides nothing
func TestHttpGetWithStrategyNoRetryOnApplicationError(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		hitCount := atomic.Int64{}
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hitCount.Add(1)
			http.Error(w, "boom", http.StatusInternalServerError)
		}))
		defer server.Close()

		strategy := newRetryTestStrategy(ctx)

		_, err := HttpGetWithStrategyRaw(ctx, strategy, server.URL+"/test", "")
		var statusError *HttpStatusError
		if !errors.As(err, &statusError) || statusError.StatusCode != http.StatusInternalServerError {
			t.Fatalf("expected a 500 status error, got %v", err)
		}
		if c := hitCount.Load(); c != 1 {
			t.Fatalf("expected exactly 1 hit (no retry), got %d", c)
		}
	})
}

// a POST is never replayed on a gateway status: the client cannot know
// whether the first attempt executed
func TestHttpPostWithStrategyNoGatewayRetry(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		postHitCount := atomic.Int64{}
		server := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "POST" {
				postHitCount.Add(1)
				http.Error(w, "bad gateway", http.StatusBadGateway)
				return
			}
			// the serial strategy's hello re-probe path
			fmt.Fprintf(w, "ok")
		}))
		defer server.Close()

		strategy := newRetryTestStrategy(ctx)

		_, err := HttpPostWithStrategyRaw(ctx, strategy, server.URL+"/test", []byte("{}"), "")
		var statusError *HttpStatusError
		if !errors.As(err, &statusError) || statusError.StatusCode != http.StatusBadGateway {
			t.Fatalf("expected a 502 status error, got %v", err)
		}
		if c := postHitCount.Load(); c != 1 {
			t.Fatalf("expected exactly 1 post (never replayed), got %d", c)
		}
	})
}
