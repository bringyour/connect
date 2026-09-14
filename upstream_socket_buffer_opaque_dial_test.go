package connect

import (
	"context"
	"errors"
	"net"
	"syscall"
	"testing"
)

// THROUGHPUT-TESTGAPS U-14, THROUGHPUTFIX §15.2. A receive buffer pin has
// exactly one application point — the dialer's pre-connect control hook — and
// a host-supplied `DialContextSettings` dial never reaches it, so where the
// host owns the dial the policy's receive decision is silently dropped and
// only the send pin survives.
//
// Why the asymmetry is correct and is still worth a row. A post-connect
// `SetReadBuffer` is not the same instrument as a pre-connect one: on some
// kernel generations an explicit `SO_RCVBUF` after connect freezes the window
// clamp at its SYN-time value, which is the defect §15 was written about, so
// `configureUpstreamTcpConn` must not apply one. The behaviour is accepted.
// What is not acceptable is that it is accepted silently: the policy computes
// `explicitReceive` and, on the opaque path, nothing consumes it, and nothing
// in the tree says so. This row is the statement, in the shape of
// `TestTheHandoffWaitAsymmetryIsWhatItIs` — it pins the arrangement at the
// values the tree ships and names what a change to either half would mean.
//
// This is a GUARD: the tree is correct today and the row passes before and
// after. It fails if a second application point for a receive pin appears
// (which would put the freeze back on the opaque path), if the pre-connect
// hook stops being built when the policy calls for a receive pin (which drops
// the pin on the ordinary path as well), or if an opaque dial starts running
// the default dialer's control hook (which would reintroduce the pin through
// a path the host, not this package, controls).
//
// It is written without syscalls on purpose. The kernel-level confirmations
// live in `ip_upstream_tcp_buffer_linux_test.go`, which can only run on one
// platform and only tests the unknown-policy case; the decision this row holds
// is arithmetic over the policy and is the same on every platform.
func TestAnOpaqueDialCannotReachTheReceivePin(t *testing.T) {
	// A synthetic policy in which both directions would be pinned: the request
	// is inside both core maxima and above both autotuning ceilings, which is
	// `explicitSend` and `explicitReceive` by their own definitions.
	requestByteCount := int(DefaultTcpBufferSettings().MaxWindowSize)
	if requestByteCount <= 0 {
		t.Fatal("the shipping upstream buffer request is not positive, so this row measures nothing")
	}
	pinsBoth := socketBufferPolicy{
		known:                   true,
		sendCoreMaxByteCount:    2 * requestByteCount,
		receiveCoreMaxByteCount: 2 * requestByteCount,
		sendCeilingByteCount:    requestByteCount / 2,
		receiveCeilingByteCount: requestByteCount / 2,
	}
	if !pinsBoth.explicitSend(requestByteCount) || !pinsBoth.explicitReceive(requestByteCount) {
		t.Fatalf(
			"the fixture policy does not pin both directions at a %d request, so the case this row is about is not constructed",
			requestByteCount,
		)
	}

	// The pre-connect hook is the whole of the receive pin's reachability: it
	// exists whenever either direction is explicit and nowhere else, so a
	// policy that calls for a receive pin and a nil hook is a dropped pin on
	// every path rather than only the opaque one.
	policies := []struct {
		name   string
		policy socketBufferPolicy
	}{
		{"pins neither, unknown policy", socketBufferPolicy{}},
		{"pins both", pinsBoth},
		{
			"pins send only",
			socketBufferPolicy{
				known:                   true,
				sendCoreMaxByteCount:    2 * requestByteCount,
				receiveCoreMaxByteCount: 2 * requestByteCount,
				sendCeilingByteCount:    requestByteCount / 2,
				receiveCeilingByteCount: 4 * requestByteCount,
			},
		},
		{
			"pins receive only",
			socketBufferPolicy{
				known:                   true,
				sendCoreMaxByteCount:    2 * requestByteCount,
				receiveCoreMaxByteCount: 2 * requestByteCount,
				sendCeilingByteCount:    4 * requestByteCount,
				receiveCeilingByteCount: requestByteCount / 2,
			},
		},
		{
			"pins neither, request below both ceilings",
			socketBufferPolicy{
				known:                   true,
				sendCoreMaxByteCount:    2 * requestByteCount,
				receiveCoreMaxByteCount: 2 * requestByteCount,
				sendCeilingByteCount:    4 * requestByteCount,
				receiveCeilingByteCount: 4 * requestByteCount,
			},
		},
	}
	for _, entry := range policies {
		explicitSend := entry.policy.explicitSend(requestByteCount)
		explicitReceive := entry.policy.explicitReceive(requestByteCount)
		control := upstreamSocketBufferControl(requestByteCount, entry.policy)
		wantControl := explicitSend || explicitReceive
		if (control != nil) != wantControl {
			t.Errorf(
				"%s: explicitSend=%t explicitReceive=%t, pre-connect hook present=%t, want %t. The hook is the only place a receive pin can be applied, so it must exist exactly when the policy pins anything",
				entry.name,
				explicitSend,
				explicitReceive,
				control != nil,
				wantControl,
			)
		}
		if explicitReceive && control == nil {
			t.Errorf(
				"%s: the policy calls for a receive pin and no pre-connect hook is built, so the pin is dropped on every path and not only the opaque one",
				entry.name,
			)
		}
	}

	// The shipped wiring, at whatever this host's kernel policy turns out to
	// be: the same relationship, read through the settings the provider
	// actually dials with rather than through a fixture.
	shipped := DefaultTcpBufferSettings()
	shippedPolicy := defaultSocketBufferPolicy()
	wantShippedControl := shippedPolicy.explicitSend(requestByteCount) ||
		shippedPolicy.explicitReceive(requestByteCount)
	if (shipped.ConnectSettings.DialControl != nil) != wantShippedControl {
		t.Errorf(
			"DefaultTcpBufferSettings ships DialControl present=%t against a host policy that pins send=%t receive=%t at a %d request",
			shipped.ConnectSettings.DialControl != nil,
			shippedPolicy.explicitSend(requestByteCount),
			shippedPolicy.explicitReceive(requestByteCount),
			requestByteCount,
		)
	}
	// The default settings are the non-opaque path, which is what makes
	// `preConnectApplied` true at the one call site
	// (`ip.go`, `self.tcpBufferSettings.DialContextSettings == nil`). A default
	// that carried an opaque dial would put every provider on the post-connect
	// subset.
	if shipped.ConnectSettings.DialContextSettings != nil {
		t.Error("DefaultTcpBufferSettings ships a host-supplied dial, so every upstream socket would take the post-connect subset")
	}
	t.Logf(
		"host policy known=%t, pins send=%t receive=%t at a %d request; shipped DialControl present=%t",
		shippedPolicy.known,
		shippedPolicy.explicitSend(requestByteCount),
		shippedPolicy.explicitReceive(requestByteCount),
		requestByteCount,
		shipped.ConnectSettings.DialControl != nil,
	)
}

// The other half of the same statement, and the reason the receive decision is
// unreachable rather than merely unapplied: a `ConnectSettings` carrying a
// host-supplied dial never builds the default dialer, so the control hook that
// would carry the pin is never attached to a socket. Asserted by observation
// rather than by reading the branch, so that a rearrangement of the dial path
// that quietly restores the hook is caught here.
//
// Deterministic by construction: the stub dial returns immediately with a
// sentinel, nothing is scheduled, and no socket is created.
func TestAHostSuppliedDialNeverRunsTheBufferControlHook(t *testing.T) {
	dialErr := errors.New("stub dial")
	controlRan := false
	dialRan := false

	connectSettings := *DefaultConnectSettings()
	connectSettings.DialControl = func(network string, address string, c syscall.RawConn) error {
		controlRan = true
		return nil
	}
	connectSettings.DialContextSettings = &DialContextSettings{
		DialContext: func(ctx context.Context, network string, addr string) (net.Conn, error) {
			dialRan = true
			return nil, dialErr
		},
	}

	conn, err := connectSettings.DialContext(context.Background(), "tcp", "127.0.0.1:9")
	if conn != nil {
		conn.Close()
	}
	if !errors.Is(err, dialErr) {
		t.Fatalf("the host-supplied dial did not own the dial: err=%v", err)
	}
	if !dialRan {
		t.Fatal("the host-supplied dial was not called, so this row did not exercise the opaque path")
	}
	if controlRan {
		t.Error("the pre-connect buffer control hook ran on a host-supplied dial. It is documented as not applied there (`ConnectSettings.DialControl`), and a post-connect receive pin is the window-clamp freeze §15 removed, so a hook that reaches this path reintroduces it through a dial this package does not own")
	}
}
