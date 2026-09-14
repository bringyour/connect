// Actual Api control decoding keeps application failure distinct from delivery.
package connect

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

// One bounded Http response is consumed by the real client strategy and Oob.
func clientControlWireResult(t *testing.T, ipVersion int, wire string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	endpoint := newFamilyHttptestServer(t, ipVersion, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/hello" {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.URL.Path != "/connect/control" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, wire)
	}))
	defer endpoint.Close()
	strategy := NewClientStrategyWithDefaults(ctx)
	defer strategy.Close()
	control := NewApiOutOfBandControl(ctx, strategy, "synthetic-control-token", endpoint.URL)
	defer func() {
		if err := control.CloseAndWait(ctx); err != nil {
			t.Error(err)
		}
	}()
	done := make(chan error, 1)
	control.SendControl([]*protocol.Frame{}, func(_ []*protocol.Frame, err error) { done <- err })
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	return nil
}

// Errors inside a valid pack, absent/null result shape, malformed base64 and
// malformed protobuf all remain failures for registration and Provide alike.
func TestApiOutOfBandControlRejectsUnprocessedWire(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		for _, wire := range []string{
			`{"pack":"","error":{"message":"controller storage failure"}}`,
			`{"pack":"","error":{"message":""}}`,
			`{}`, `{"pack":null}`, `null`, ``,
			`{"pack":"!"}`, `{"pack":"gA=="}`,
		} {
			if err := clientControlWireResult(t, ipVersion, wire); err == nil {
				t.Fatalf("unprocessed wire was acknowledged: %q", wire)
			}
		}
	})
}

// Empty protobuf is the server's legitimate result for a processed key frame;
// requiring presence must not reject that exact successful wire representation.
func TestApiOutOfBandControlAcceptsExplicitProcessedPack(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		for _, wire := range []string{`{"pack":""}`, `{"pack":"","error":null}`} {
			if err := clientControlWireResult(t, ipVersion, wire); err != nil {
				t.Fatal("explicit empty processed response was rejected", err)
			}
		}
	})
}
