package connect

import (
	"context"
	"errors"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// The shape of the reported bug: the tcp connect succeeds over IPv6 (small
// packets pass an HE tunnel), then the tls handshake times out (the large
// ServerHello is dropped). The caller must still get a working connection.
func TestFamilyFallbackRecoversFromAPostConnectTimeout(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	var mutex sync.Mutex
	var dialed []string
	dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
		mutex.Lock()
		dialed = append(dialed, network)
		mutex.Unlock()
		remote := &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}
		if network == "tcp4" {
			remote = &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 443}
		}
		return &stubConn{remote: remote}, nil
	}
	handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		if connFamily(conn) == 6 {
			return nil, &timeoutError{}
		}
		return conn, nil
	}

	conn, err := dialControlTlsWithFamilyFallback(
		context.Background(), DefaultConnectSettings(), "tcp", "api.example:443", dial, handshake)
	if err != nil {
		t.Fatal(err)
	}
	if got := connFamily(conn); got != 4 {
		t.Fatalf("returned an IPv%d connection, want IPv4", got)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if len(dialed) != 2 || dialed[0] != "tcp" || dialed[1] != "tcp4" {
		t.Fatalf("dialed %v, want [tcp tcp4]", dialed)
	}
	if controlFamilyDemotedFamily() != 6 {
		t.Fatal("expected ipv6 to be demoted by the failure")
	}
}

// Exactly one retry. The dial already sits inside the strategy's own dialer
// evaluation under a 15s request budget, so a helper that retried repeatedly
// could consume the whole budget alone and starve the other dialers.
func TestFamilyFallbackRetriesOnlyOnce(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	var mutex sync.Mutex
	attempts := 0
	dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
		mutex.Lock()
		attempts += 1
		mutex.Unlock()
		return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}, nil
	}
	handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		return nil, &timeoutError{}
	}

	_, err := dialControlTlsWithFamilyFallback(
		context.Background(), DefaultConnectSettings(), "tcp", "api.example:443", dial, handshake)
	if err == nil {
		t.Fatal("expected the second failure to be returned")
	}
	mutex.Lock()
	defer mutex.Unlock()
	if attempts != 2 {
		t.Fatalf("dialed %d times, want exactly 2", attempts)
	}
}

// A non-timeout failure is not a path problem and must not demote or retry.
func TestFamilyFallbackDoesNotRetryANonTimeout(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	attempts := 0
	dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
		attempts += 1
		return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}, nil
	}
	certErr := errors.New("x509: certificate signed by unknown authority")
	handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		return nil, certErr
	}

	_, err := dialControlTlsWithFamilyFallback(
		context.Background(), DefaultConnectSettings(), "tcp", "api.example:443", dial, handshake)
	if !errors.Is(err, certErr) {
		t.Fatalf("got %v, want the certificate error unwrapped", err)
	}
	if attempts != 1 {
		t.Fatalf("dialed %d times, want 1 -- a certificate failure is not a path failure", attempts)
	}
	if controlFamilyDemotedFamily() != 0 {
		t.Fatal("a certificate failure must not demote a family")
	}
}

// An explicitly family-specific dial has nowhere to fall back to.
func TestFamilyFallbackDoesNotRetryAnExplicitFamily(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	attempts := 0
	dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
		attempts += 1
		return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}, nil
	}
	handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		return nil, &timeoutError{}
	}

	_, _ = dialControlTlsWithFamilyFallback(
		context.Background(), DefaultConnectSettings(), "tcp6", "api.example:443", dial, handshake)
	if attempts != 1 {
		t.Fatalf("dialed %d times, want 1 for an explicit tcp6", attempts)
	}
}

// The family has to be read off the connection BEFORE the handshake runs and
// before any Close. A real tls.Conn closes the underlying connection on a
// failed handshake -- newNormalDialTlsContext's callback does exactly that --
// and a closed net.TCPConn is not required to keep answering RemoteAddr. Read
// it late and `failed` is 0, so nothing is demoted and nothing is retried:
// the fallback silently stops existing on the one path it was written for.
//
// forgetfulConn reproduces that: it answers RemoteAddr until it is closed and
// nil afterwards. The brief's other tests cannot catch the ordering, because
// stubConn's Close is a no-op that leaves RemoteAddr working forever.
func TestFamilyFallbackReadsTheFamilyBeforeTheConnectionIsClosed(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	var mutex sync.Mutex
	var dialed []string
	dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
		mutex.Lock()
		dialed = append(dialed, network)
		mutex.Unlock()
		if network == "tcp4" {
			return &forgetfulConn{remote: &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 443}}, nil
		}
		return &forgetfulConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}, nil
	}
	handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		if connFamily(conn) == 6 {
			// what tls.Conn.Close() does to the connection it wraps
			conn.Close()
			return nil, &timeoutError{}
		}
		return conn, nil
	}

	conn, err := dialControlTlsWithFamilyFallback(
		context.Background(), DefaultConnectSettings(), "tcp", "api.example:443", dial, handshake)
	if err != nil {
		t.Fatal(err)
	}
	if got := connFamily(conn); got != 4 {
		t.Fatalf("returned an IPv%d connection, want IPv4", got)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if len(dialed) != 2 || dialed[1] != "tcp4" {
		t.Fatalf("dialed %v, want [tcp tcp4] -- the family was read too late to fall back", dialed)
	}
}

// answers RemoteAddr until closed, then forgets, like a real net.TCPConn may.
type forgetfulConn struct {
	net.Conn
	remote net.Addr
	closed atomic.Bool
}

func (self *forgetfulConn) RemoteAddr() net.Addr {
	if self.closed.Load() {
		return nil
	}
	return self.remote
}

func (self *forgetfulConn) Close() error {
	self.closed.Store(true)
	return nil
}

// The platform control websocket's handshake tolerance is left alone: on its
// budget the first handshake still gets the caller's WHOLE remaining time.
//
// gorilla/websocket caps the dial context at Dialer.HandshakeTimeout, 5s, and
// 5s is below ControlFamilyFirstHandshakeTimeout + ControlFamilyRetryReserve,
// so the bound is never applied here -- there is no room for a second attempt
// to run in, and a bound that produced a timeout with nowhere to retry would
// only make this path fail sooner than it does today. It loses nothing by it:
// the demotion ledger is process-global, so the demotion the api path has the
// budget to LEARN is already in force for this dial.
//
// The first handshake used to get half the caller's budget on every path.
// Shortening the 15s tls handshake tolerance is something this product
// deliberately does not do -- it would risk false-positive demotion for users
// on genuinely slow links -- and halving did exactly that, implicitly and
// hardest where the budget was smallest: 2.5s here, on the path
// that reconnects most often, with a timeout inside it read as proof the
// family is blackholed.
func TestFamilyFallbackLeavesTheWebsocketBudgetUnbounded(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	// the platform control websocket's real budget, read from the settings
	// gorilla is handed rather than restated
	settings := DefaultConnectSettings()
	if settings.HandshakeTimeout >=
		settings.ControlFamilyFirstHandshakeTimeout+settings.ControlFamilyRetryReserve {
		t.Fatalf(
			"the websocket's %s budget now reaches the %s+%s bound threshold -- "+
				"the first handshake on the control websocket is about to be cut short, "+
				"which is the false positive this bound exists to avoid",
			settings.HandshakeTimeout,
			settings.ControlFamilyFirstHandshakeTimeout,
			settings.ControlFamilyRetryReserve,
		)
	}
	callerCtx, cancel := context.WithTimeout(context.Background(), settings.HandshakeTimeout)
	defer cancel()
	callerDeadline, _ := callerCtx.Deadline()

	dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
		if network == "tcp4" {
			return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 443}}, nil
		}
		return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}, nil
	}
	var mutex sync.Mutex
	deadlines := []time.Time{}
	handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("the handshake was given a context with no deadline")
		}
		mutex.Lock()
		deadlines = append(deadlines, deadline)
		mutex.Unlock()
		if connFamily(conn) == 6 {
			// the handshake's OWN timeout, not the caller's: this is
			// TlsTimeout, left at 15s
			return nil, &timeoutError{}
		}
		return conn, nil
	}

	conn, err := dialControlTlsWithFamilyFallback(
		callerCtx, settings, "tcp", "api.example:443", dial, handshake)
	if err != nil {
		t.Fatal(err)
	}
	if got := connFamily(conn); got != 4 {
		t.Fatalf("returned an IPv%d connection, want IPv4", got)
	}

	mutex.Lock()
	defer mutex.Unlock()
	if len(deadlines) != 2 {
		t.Fatalf("handshaked %d times, want 2", len(deadlines))
	}
	if deadlines[0].Before(callerDeadline) {
		t.Fatalf(
			"the first handshake was cut at %s, %s short of the caller's own %s -- "+
				"a fraction of the caller's budget is a shortened handshake tolerance, "+
				"which this product deliberately does not do",
			deadlines[0].Format(time.RFC3339Nano),
			callerDeadline.Sub(deadlines[0]),
			callerDeadline.Format(time.RFC3339Nano),
		)
	}
}

// The consequence, on the real websocket budget: a handshake that is merely
// SLOW -- slower than the caller's whole 5s, so the caller's own deadline is
// what ends it -- must not be read as a blackholed family. Under the halving
// this demoted at 2.5s with the caller's budget still half unspent.
func TestFamilyFallbackDoesNotDemoteASlowHandshakeOnTheWebsocketBudget(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	// above 2 * the deleted 2s minimum, so the deleted code took its dividing
	// branch and demoted; below anything a real caller would call patient
	callerCtx, cancel := context.WithTimeout(context.Background(), 4500*time.Millisecond)
	defer cancel()

	attempts := 0
	dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
		attempts += 1
		return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}, nil
	}
	handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	_, err := dialControlTlsWithFamilyFallback(
		callerCtx, DefaultConnectSettings(), "tcp", "api.example:443", dial, handshake)
	if err == nil {
		t.Fatal("expected the timeout back")
	}
	if attempts != 1 {
		t.Fatalf("dialed %d times, want 1 -- the caller's budget was gone", attempts)
	}
	if got := controlFamilyDemotedFamily(); got != 0 {
		t.Fatalf("ipv%d was demoted by a handshake that only ran out of the "+
			"caller's own time", got)
	}
}

// The failure this helper exists to catch: a handshake that stalls until a
// deadline of its OWN -- a blackholed path gives up only when something times
// it out, because the kernel retransmits for minutes -- while the caller still
// has budget. That is the case the retry can act on, and the caller must
// receive a working connection.
func TestFamilyFallbackRecoversFromAHandshakeThatStallsToItsOwnDeadline(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	callerCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
		// a real dial on a dead context fails at once
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if network == "tcp4" {
			return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 443}}, nil
		}
		return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}, nil
	}
	handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if connFamily(conn) == 6 {
			// what newNormalDialTlsContext does: the handshake carries its own
			// TlsTimeout inside the caller's budget. The ServerHello never
			// arrives and the handshake ends there, with the caller's budget
			// still alive.
			handshakeCtx, handshakeCancel := context.WithTimeout(ctx, 200*time.Millisecond)
			defer handshakeCancel()
			<-handshakeCtx.Done()
			return nil, handshakeCtx.Err()
		}
		return conn, nil
	}

	conn, err := dialControlTlsWithFamilyFallback(
		callerCtx, DefaultConnectSettings(), "tcp", "api.example:443", dial, handshake)
	if err != nil {
		t.Fatalf("%v -- the stalled first attempt left the retry no budget", err)
	}
	if got := connFamily(conn); got != 4 {
		t.Fatalf("returned an IPv%d connection, want IPv4", got)
	}
	if controlFamilyDemotedFamily() != 6 {
		t.Fatal("expected ipv6 to be demoted by the stall")
	}
}

// A request that simply runs out of budget mid-handshake proves nothing about
// the path. Demoting there would take a user on a merely slow link off a
// healthy family for five minutes, which is the false positive the design
// refused to accept when it declined to shorten TlsTimeout.
func TestFamilyFallbackDoesNotDemoteWhenTheCallersBudgetRanOut(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	// far too little left to bound a first attempt and still have a retry, so
	// whatever the handshake reports is the caller's deadline, not the path's
	callerCtx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	attempts := 0
	dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
		attempts += 1
		return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}, nil
	}
	handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}

	_, err := dialControlTlsWithFamilyFallback(
		callerCtx, DefaultConnectSettings(), "tcp", "api.example:443", dial, handshake)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want the caller's deadline error", err)
	}
	if attempts != 1 {
		t.Fatalf("dialed %d times, want 1 -- there was no budget to retry with", attempts)
	}
	if controlFamilyDemotedFamily() != 0 {
		t.Fatal("the caller's own budget expiring must not demote a family")
	}
}

// The route back from a demotion that took the user offline.
//
// The ledger is only ever written after a connect SUCCEEDED and the handshake
// then timed out, so a demotion can be confirmed but never refuted. When the
// family we demoted onto cannot connect at all, the dial returns at the
// connect step and nothing is recorded -- so every control dial in the process
// stays narrowed onto the family that just proved it does not work, for five
// minutes and, as strikes accumulate, up to six hours.
//
// The dial stub resolves the network through controlDialNetwork exactly as
// ConnectSettings.DialContext does, so the narrowing under test is the real
// one and not a value the test handed itself.
func TestFamilyFallbackUndoesADemotionThatCannotConnect(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()
	SetControlIpFamilyPolicy(IpFamilyAuto)

	if !controlFamilyDemote(6) {
		t.Fatal("expected the demotion to take")
	}
	if got, _ := controlDialNetwork("tcp", "api.example:443"); got != "tcp4" {
		t.Fatalf("precondition: controlDialNetwork = %q, want tcp4", got)
	}

	var mutex sync.Mutex
	var dialed []string
	dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
		resolved, err := controlDialNetwork(network, addr)
		if err != nil {
			return nil, err
		}
		mutex.Lock()
		dialed = append(dialed, resolved)
		mutex.Unlock()
		if resolved == "tcp4" {
			// the device has no ipv4 route at all
			return nil, &net.OpError{
				Op: "dial", Net: "tcp4", Err: errors.New("connect: network is unreachable"),
			}
		}
		return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}, nil
	}
	handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		return conn, nil
	}

	conn, err := dialControlTlsWithFamilyFallback(
		context.Background(), DefaultConnectSettings(), "tcp", "api.example:443", dial, handshake)
	if err != nil {
		t.Fatalf("%v -- the demotion steered the dial onto a family with no route "+
			"and nothing could undo it", err)
	}
	if got := connFamily(conn); got != 6 {
		t.Fatalf("returned an IPv%d connection, want IPv6", got)
	}
	mutex.Lock()
	dialedCopy := append([]string{}, dialed...)
	mutex.Unlock()
	if len(dialedCopy) != 2 || dialedCopy[0] != "tcp4" || dialedCopy[1] != "tcp" {
		t.Fatalf("dialed %v, want [tcp4 tcp] -- the redial must be family-agnostic "+
			"so happy eyeballs runs again", dialedCopy)
	}
	if controlFamilyDemotedFamily() != 0 {
		t.Fatal("the demotion still stands after the family it demoted onto failed to connect")
	}
}

// A force is a developer override and is never undone by a dial failure.
func TestFamilyFallbackDoesNotUndoAForce(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()
	SetControlIpFamilyPolicy(IpFamilyForce4)
	defer SetControlIpFamilyPolicy(IpFamilyAuto)

	attempts := 0
	dialErr := errors.New("connect: network is unreachable")
	dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
		attempts += 1
		return nil, dialErr
	}
	handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		return conn, nil
	}

	_, err := dialControlTlsWithFamilyFallback(
		context.Background(), DefaultConnectSettings(), "tcp", "api.example:443", dial, handshake)
	if !errors.Is(err, dialErr) {
		t.Fatalf("got %v, want the dial error", err)
	}
	if attempts != 1 {
		t.Fatalf("dialed %d times, want 1 -- a force must not be re-dialed around", attempts)
	}
	if ControlIpFamilyPolicy() != IpFamilyForce4 {
		t.Fatal("the force was cleared by a dial failure")
	}
}

// A second failure over the second family is not a family problem. A strike
// that the retry could not confirm must not be left
// standing -- it narrows every control dial in the process, including the
// extender and h3/quic paths, on evidence the helper itself just contradicted.
func TestFamilyFallbackRollsBackTheStrikeWhenTheRetryAlsoFails(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()

	tests := []struct {
		name         string
		retryDialErr bool
	}{
		{"the retry cannot connect", true},
		{"the retry connects and its handshake fails", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			controlFamilyClear()
			defer controlFamilyClear()

			dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
				if network == "tcp4" {
					if test.retryDialErr {
						return nil, errors.New("connect: network is unreachable")
					}
					return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 443}}, nil
				}
				return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}, nil
			}
			handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
				return nil, &timeoutError{}
			}

			_, err := dialControlTlsWithFamilyFallback(
				context.Background(), DefaultConnectSettings(), "tcp", "api.example:443", dial, handshake)
			if err == nil {
				t.Fatal("expected the original error back")
			}
			if got := controlFamilyDemotedFamily(); got != 0 {
				t.Fatalf("ipv%d is still demoted after the other family failed too", got)
			}
			if controlFamilyStatus() != "" {
				t.Fatalf("status %q -- a strike the retry contradicted must not survive",
					controlFamilyStatus())
			}
		})
	}
}

// The developer setting and the learned memory are independent state and never
// mix. A timeout observed while a FORCE is in effect carries no information
// about family choice -- there is no other family in play to compare it
// against -- so it must not be written to the ledger.
//
// The real damage is deferred: a strike recorded under Force IPv4 stays armed
// for the moment the developer sets the row back to Auto, and then steers
// every control dial onto the family they had just forced away from.
func TestFamilyFallbackDoesNotWriteTheLedgerUnderAForce(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()

	for _, policy := range []IpFamilyPolicy{IpFamilyForce4, IpFamilyForce6} {
		controlFamilyClear()
		SetControlIpFamilyPolicy(policy)

		attempts := 0
		dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
			attempts += 1
			return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}, nil
		}
		handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
			return nil, &timeoutError{}
		}

		_, err := dialControlTlsWithFamilyFallback(
			context.Background(), DefaultConnectSettings(), "tcp", "api.example:443", dial, handshake)
		if err == nil {
			t.Fatal("expected the timeout back")
		}
		if got := controlFamilyDemotedFamily(); got != 0 {
			t.Fatalf("policy %d: ipv%d demoted while a force is in effect", policy, got)
		}
		if got := controlFamilyStatus(); got != "" {
			t.Fatalf("policy %d: status %q contradicts the policy row", policy, got)
		}
		if attempts != 1 {
			t.Fatalf("policy %d: dialed %d times, want 1 -- the retry would be "+
				"rejected by the force itself", policy, attempts)
		}
	}
	SetControlIpFamilyPolicy(IpFamilyAuto)
	controlFamilyClear()
}

// deadlineConn blocks in Read until the connection is closed or a deadline in
// the past is set, which is how crypto/tls interrupts a handshake whose context
// expired (Conn.handshakeContext sets the underlying deadline to now). Writes
// are accepted and dropped: the ClientHello goes out, the ServerHello never
// comes back, which is the post-connect blackhole this feature targets.
type deadlineConn struct {
	remote    net.Addr
	unblock   chan struct{}
	closeOnce sync.Once
}

func newDeadlineConn(ip string) *deadlineConn {
	return &deadlineConn{
		remote:  &net.TCPAddr{IP: net.ParseIP(ip), Port: 443},
		unblock: make(chan struct{}),
	}
}

func (self *deadlineConn) release() {
	self.closeOnce.Do(func() { close(self.unblock) })
}

func (self *deadlineConn) Read(b []byte) (int, error) {
	<-self.unblock
	return 0, os.ErrDeadlineExceeded
}

func (self *deadlineConn) Write(b []byte) (int, error) { return len(b), nil }
func (self *deadlineConn) Close() error                { self.release(); return nil }
func (self *deadlineConn) LocalAddr() net.Addr         { return nil }
func (self *deadlineConn) RemoteAddr() net.Addr        { return self.remote }
func (self *deadlineConn) SetDeadline(t time.Time) error {
	if !t.IsZero() && !t.After(time.Now()) {
		self.release()
	}
	return nil
}
func (self *deadlineConn) SetReadDeadline(t time.Time) error  { return self.SetDeadline(t) }
func (self *deadlineConn) SetWriteDeadline(t time.Time) error { return nil }

// The fragment/reorder dialers go through the family fallback too.
//
// The helper was wired into newNormalDialTlsContext only, and that is the
// wrong one to have alone: api posts do not race the dialers, they go through
// HttpSerial -> serialEval, which sorts the already-succeeded dialers by
// priority, and "fragment" is priority 0 against "normal" at 25. So the
// fragment dialer leads every warm serial post, and shares the parallel hello
// before that -- and it was the one dialer with no timeout classification, no
// strike and no in-place retry.
func TestResilientDialGoesThroughTheFamilyFallback(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	settings := DefaultConnectSettings()
	// stands in for the handshake's own TlsTimeout expiring inside a caller
	// budget that is still alive
	settings.TlsTimeout = 250 * time.Millisecond

	var mutex sync.Mutex
	var dialed []string
	var conns []*deadlineConn
	settings.DialContextSettings = &DialContextSettings{
		DialContext: func(ctx context.Context, network string, addr string) (net.Conn, error) {
			mutex.Lock()
			dialed = append(dialed, network)
			mutex.Unlock()
			ip := "2001:db8::1"
			if network == "tcp4" {
				ip = "192.0.2.1"
			}
			conn := newDeadlineConn(ip)
			mutex.Lock()
			conns = append(conns, conn)
			mutex.Unlock()
			return conn, nil
		},
	}

	// the "fragment" dialer: priority 0, the first one serialEval reaches
	dialTls := newResilientDialTlsContext(settings, true, false, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := dialTls(ctx, "tcp", "api.example:443")
	if err == nil {
		conn.Close()
		t.Fatal("expected the blackholed handshake to fail")
	}

	mutex.Lock()
	defer mutex.Unlock()
	for _, c := range conns {
		c.release()
	}
	if len(dialed) != 2 || dialed[0] != "tcp" || dialed[1] != "tcp4" {
		t.Fatalf("dialed %v, want [tcp tcp4] -- the resilient dialer still "+
			"handshakes inline with no family classification and no retry", dialed)
	}
}

// THE POINT OF THE WHOLE FEATURE, on the budget the api path actually has.
//
// A handshake bounded only by TlsTimeout can never reach its own timeout: at
// 15s it is at or above every production caller's budget, so the caller's
// deadline always arrives first, the ctx.Err() branch declines the strike, and
// the retry never runs. The demotion trigger was configured out of existence.
//
// ControlFamilyFirstHandshakeTimeout is what a stalled handshake hits instead,
// and this pins that it leaves a retry a real amount of time to run in --
// ControlFamilyRetryReserve at the very least -- on the api path's own
// RequestTimeout budget, with the shipping defaults and not test-sized ones.
func TestFamilyFallbackBoundsTheFirstHandshakeOnTheApiBudget(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	// http.Client.Timeout is RequestTimeout on every api dialer (HttpClient),
	// and serialEval bounds the whole request by the same value
	settings := DefaultConnectSettings()
	callerCtx, cancel := context.WithTimeout(context.Background(), settings.RequestTimeout)
	defer cancel()
	callerDeadline, _ := callerCtx.Deadline()

	dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
		if network == "tcp4" {
			return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 443}}, nil
		}
		return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}, nil
	}
	var mutex sync.Mutex
	deadlines := []time.Time{}
	handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Error("the handshake was given a context with no deadline")
		}
		mutex.Lock()
		deadlines = append(deadlines, deadline)
		mutex.Unlock()
		if connFamily(conn) == 6 {
			return nil, &timeoutError{}
		}
		return conn, nil
	}

	conn, err := dialControlTlsWithFamilyFallback(
		callerCtx, settings, "tcp", "api.example:443", dial, handshake)
	if err != nil {
		t.Fatal(err)
	}
	if got := connFamily(conn); got != 4 {
		t.Fatalf("returned an IPv%d connection, want IPv4", got)
	}
	if controlFamilyDemotedFamily() != 6 {
		t.Fatal("expected ipv6 to be demoted -- the handshake hit a timeout of its own")
	}

	mutex.Lock()
	defer mutex.Unlock()
	if len(deadlines) != 2 {
		t.Fatalf("handshaked %d times, want 2", len(deadlines))
	}
	// the retry's budget: what the caller still had when the bound expired
	left := callerDeadline.Sub(deadlines[0])
	if left < settings.ControlFamilyRetryReserve {
		t.Fatalf(
			"the first handshake was given %s of the caller's %s, leaving %s for a "+
				"retry -- want at least %s held back, or a stall simply ends at the "+
				"caller's own deadline and nothing is learned",
			time.Until(deadlines[0]),
			settings.RequestTimeout,
			left,
			settings.ControlFamilyRetryReserve,
		)
	}
	// and it is the floor that bounded it, not some fraction of the caller
	bounded := time.Until(deadlines[0])
	slack := 2 * time.Second
	if bounded < settings.ControlFamilyFirstHandshakeTimeout-slack ||
		settings.ControlFamilyFirstHandshakeTimeout+slack < bounded {
		t.Fatalf(
			"the first handshake got %s, want ~%s (ControlFamilyFirstHandshakeTimeout)",
			bounded, settings.ControlFamilyFirstHandshakeTimeout)
	}
	// the retry gets the rest of the caller's budget, unbounded
	if !deadlines[1].Equal(callerDeadline) {
		t.Fatalf("the retry's deadline was %s, want the caller's own %s",
			deadlines[1].Format(time.RFC3339Nano), callerDeadline.Format(time.RFC3339Nano))
	}
}

// End to end, with a handshake that really stalls: it now ends at the bound
// with the caller's budget still alive, which is the one state that records a
// strike and runs the retry. Before the bound existed this stall ran to the
// caller's own deadline and produced nothing -- no strike, no retry, no
// connection.
//
// Test-sized durations: the shape is what is under test, and the shipping
// values are pinned against the real budgets by the two tests either side.
func TestFamilyFallbackDemotesAStallThatOutlastsTheBound(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	settings := DefaultConnectSettings()
	settings.ControlFamilyFirstHandshakeTimeout = 250 * time.Millisecond
	settings.ControlFamilyRetryReserve = 100 * time.Millisecond

	// far above the bound plus the reserve, so two attempts fit
	callerCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mutex sync.Mutex
	var dialed []string
	dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		mutex.Lock()
		dialed = append(dialed, network)
		mutex.Unlock()
		if network == "tcp4" {
			return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("192.0.2.1"), Port: 443}}, nil
		}
		return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}, nil
	}
	handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		if connFamily(conn) == 6 {
			// the blackhole: the ClientHello goes out and nothing comes back,
			// so this ends only when something times it out
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return conn, nil
	}

	start := time.Now()
	conn, err := dialControlTlsWithFamilyFallback(
		callerCtx, settings, "tcp", "api.example:443", dial, handshake)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("%v -- the stall ran past the bound and left the retry no budget", err)
	}
	if got := connFamily(conn); got != 4 {
		t.Fatalf("returned an IPv%d connection, want IPv4", got)
	}
	if controlFamilyDemotedFamily() != 6 {
		t.Fatal("expected ipv6 to be demoted by the stall")
	}
	if 2*time.Second < elapsed {
		t.Fatalf("took %s -- the stall ended at the caller's deadline, not at the bound",
			elapsed)
	}
	mutex.Lock()
	defer mutex.Unlock()
	if len(dialed) != 2 || dialed[0] != "tcp" || dialed[1] != "tcp4" {
		t.Fatalf("dialed %v, want [tcp tcp4]", dialed)
	}
}

// The bound is a tolerance, not a target. A handshake that is slow but WORKING
// finishes inside it and must be returned, with no strike: a demotion narrows
// every control dial in the process for five minutes, and being on a congested
// link is not evidence of a blackhole.
//
// This is the failure mode of the deleted implementation, which took a
// FRACTION of the caller's budget instead of a floor, and so shrank the
// tolerance exactly where the caller had least to give.
func TestFamilyFallbackDoesNotDemoteAHandshakeThatFinishesInsideTheBound(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	settings := DefaultConnectSettings()
	settings.ControlFamilyFirstHandshakeTimeout = 500 * time.Millisecond
	settings.ControlFamilyRetryReserve = 100 * time.Millisecond

	// 800ms is over the 600ms threshold, so the bound applies -- and under
	// twice the bound, so HALF of it (400ms) is less than the bound. The
	// handshake below lands between the two: inside the floor, outside the
	// fraction the deleted implementation would have allowed.
	callerCtx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()

	attempts := 0
	dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
		attempts += 1
		return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}, nil
	}
	handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		// slow, and inside the bound: a congested link with a pinned P-384
		// chain, not a path that drops large packets
		select {
		case <-time.After(450 * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return conn, nil
	}

	conn, err := dialControlTlsWithFamilyFallback(
		callerCtx, settings, "tcp", "api.example:443", dial, handshake)
	if err != nil {
		t.Fatalf("%v -- a slow but working handshake was cut short by the bound", err)
	}
	if got := connFamily(conn); got != 6 {
		t.Fatalf("returned an IPv%d connection, want the IPv6 one that worked", got)
	}
	if attempts != 1 {
		t.Fatalf("dialed %d times, want 1 -- nothing failed", attempts)
	}
	if got := controlFamilyDemotedFamily(); got != 0 {
		t.Fatalf("ipv%d was demoted by a handshake that succeeded", got)
	}
}

// A caller without room for two attempts is not bounded at all. The bound
// exists to hold budget back for a retry; where there is no retry to hold it
// back for, taking it is pure loss -- it would end a request that was still
// waiting, earlier than it would have ended, and learn nothing for it.
//
// This is what keeps the bound off the platform control websocket (5s, under
// the 8s + 5s threshold) and off every dialer the client strategy reaches
// after the first has already spent part of the request budget.
func TestFamilyFallbackDoesNotBoundACallerWithNoRoomForTwoAttempts(t *testing.T) {
	restore := swapControlFamilyProbe(func(int) bool { return true })
	defer restore()
	controlFamilyClear()
	defer controlFamilyClear()

	settings := DefaultConnectSettings()
	settings.ControlFamilyFirstHandshakeTimeout = 400 * time.Millisecond
	settings.ControlFamilyRetryReserve = 200 * time.Millisecond

	// below the 600ms threshold, above the bound on its own -- the shape that
	// would be bounded if only the bound, and not the reserve, were consulted
	callerCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	callerDeadline, _ := callerCtx.Deadline()

	attempts := 0
	var handshakeDeadline time.Time
	dial := func(ctx context.Context, network string, addr string) (net.Conn, error) {
		attempts += 1
		return &stubConn{remote: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 443}}, nil
	}
	handshake := func(ctx context.Context, conn net.Conn) (net.Conn, error) {
		handshakeDeadline, _ = ctx.Deadline()
		<-ctx.Done()
		return nil, ctx.Err()
	}

	_, err := dialControlTlsWithFamilyFallback(
		callerCtx, settings, "tcp", "api.example:443", dial, handshake)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want the caller's own deadline error", err)
	}
	if !handshakeDeadline.Equal(callerDeadline) {
		t.Fatalf(
			"the handshake was bounded to %s, %s short of the caller's own %s -- "+
				"there was no room for a retry, so the single attempt must get everything",
			handshakeDeadline.Format(time.RFC3339Nano),
			callerDeadline.Sub(handshakeDeadline),
			callerDeadline.Format(time.RFC3339Nano),
		)
	}
	if attempts != 1 {
		t.Fatalf("dialed %d times, want 1", attempts)
	}
	if got := controlFamilyDemotedFamily(); got != 0 {
		t.Fatalf("ipv%d was demoted by the caller's own budget expiring", got)
	}
}
