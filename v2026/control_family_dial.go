package connect

import (
	"context"
	"net"
	"time"
)

// dialControlTlsWithFamilyFallback performs a control-plane dial and its
// handshake, and retries ONCE over the other address family when the handshake
// fails with a timeout of its own after the connect succeeded.
//
// That sequence -- connect succeeds, handshake times out -- is the signature of
// a path that carries small packets and drops large ones, which is what a
// tunnel with a reduced MTU and filtered ICMP Packet-Too-Big does. Happy
// Eyeballs cannot see it: it races only the tcp handshake, so the broken family
// WINS the race and then stalls.
//
// A timeout that arrives with the caller's own budget already spent is NOT
// counted: that is a request running out of time, which says nothing about
// the family, and there would be no time to retry either way. What remains to
// trigger a demotion is a handshake that hits its OWN timeout while the caller
// still has budget left, which is also the only case where a retry has
// anywhere to run.
//
// WHAT THAT OWN TIMEOUT IS, and why it is not TlsTimeout. Both production
// entry points hand this helper a caller deadline at or below TlsTimeout:
// http.Client.Timeout is RequestTimeout (15s, and it starts before the dial
// does) on the api path, and gorilla/websocket caps its dial context at
// Dialer.HandshakeTimeout (5s) on the platform control websocket. A handshake
// bounded only by TlsTimeout therefore never reaches its own timeout -- the
// caller's deadline always arrives first, the branch above declines the
// strike, and the retry never runs at all.
//
// So the first handshake is bounded by ControlFamilyFirstHandshakeTimeout, and
// THAT is the timeout a stalled handshake hits. It is a floor, not a fraction:
// an earlier version halved whatever the caller had left, which shortens the
// tls handshake tolerance this product deliberately does not shorten -- doing
// so risks false-positive demotion for users on genuinely slow links -- and
// did it hardest exactly where the budget was smallest -- 2.5s on the control
// websocket, which a congested mobile link reaches with a pinned P-384 chain
// and nothing wrong. A fixed 8s cannot scale down like that, and it is larger
// than the entire budget in which a shipping platform websocket dial already
// completes a connect, this handshake and an http upgrade.
//
// THE BOUND IS APPLIED ONLY WHERE TWO ATTEMPTS FIT: the caller must still have
// ControlFamilyFirstHandshakeTimeout + ControlFamilyRetryReserve left when the
// handshake starts. A bound that produces a timeout with no room to retry is
// strictly worse than no bound -- it converts a request that would have kept
// waiting into one that fails early and learns nothing. Below that threshold
// the first handshake keeps the caller's whole remaining budget and this
// helper behaves exactly as it did before the bound existed.
//
// That threshold is what decides, per attempt, whether this helper bounds
// anything. An api attempt that arrives with the whole RequestTimeout (~15s)
// is bounded -- which is every attempt the client strategy runs in parallel,
// including the parallel hello a strategy with no successful dialer yet falls
// straight through to, so a cold launch is bounded. The strategy's SERIAL
// attempts are not: parallelEval and serialEval give each remembered dialer an
// equal share of what is left (preferredEvalAttemptContext, net_http.go), and
// a share of 15s is under 8s + 5s, so the same "no room for two attempts"
// rule declines to bound them. Nor is the platform control websocket, which
// gorilla caps at HandshakeTimeout (5s).
//
// None of those paths needs a retry of its own: the demotion ledger is
// process-global (control_family.go), read inside every dial through
// controlDialNetwork and pickControlIPAddr, so a demotion learned on any
// bounded attempt is already in force for the control websocket, the h3/quic
// name path and the extenders. One attempt with enough budget is enough to
// LEARN; every path benefits. Raising the websocket's HandshakeTimeout
// instead would change a shared transport timeout for every user to buy a
// second attempt on a path that already inherits the answer.
//
// Exactly one retry, and only to the other family. The caller already sits
// inside the client strategy's serial and parallel dialer evaluation under a
// shared request budget, so a helper that retried repeatedly could consume the
// whole budget alone and starve the other dialers -- which is the failure this
// exists to prevent, not to reproduce. A second failure over the second family
// is also not a family problem, and the original error is returned unwrapped.
func dialControlTlsWithFamilyFallback(
	ctx context.Context,
	settings *ConnectSettings,
	network string,
	addr string,
	dial DialContextFunction,
	handshake func(ctx context.Context, conn net.Conn) (net.Conn, error),
) (net.Conn, error) {
	// A family-pinned platform transport (transport_family.go) tags its dials
	// with the family it proves. The narrowing happens HERE, above the
	// strategy's ConnectSettings.DialContext, so name resolution below it
	// requests only the pinned record type, and a Force that contradicts the
	// pin surfaces there as the error the transport idles on.
	pinnedFamily := pinnedIpFamilyFromContext(ctx)
	if pinnedFamily != 0 {
		narrowed, err := pinnedDialNetwork(network, pinnedFamily)
		if err != nil {
			return nil, err
		}
		network = narrowed
	}
	conn, err := dial(ctx, network, addr)
	if err != nil {
		conn, err = redialWithoutAContradictedDemotion(ctx, network, addr, dial, err)
		if err != nil {
			return nil, err
		}
	}
	// BEFORE the handshake, and before any Close: a closed net.TCPConn is not
	// required to keep answering RemoteAddr, and the family of the connection
	// we are about to lose is the whole point of the exercise.
	failed := connFamily(conn)

	// The bound goes on the handshake and NOT on the connect. The connect has
	// its own budget (ConnectTimeout) and its own second chance
	// (redialWithoutAContradictedDemotion, below), which a shortened context
	// would leave with a dead deadline to dial on. Measuring what is left here
	// rather than before the dial is also the honest reading: it is the budget
	// the retry will actually inherit.
	handshakeCtx, handshakeCancel := firstHandshakeContext(ctx, settings)
	tlsConn, err := handshake(handshakeCtx, conn)
	handshakeCancel()
	if err == nil {
		return tlsConn, nil
	}
	conn.Close()

	// A pinned dial has nowhere else to go, but a handshake that stalled on
	// its family is still evidence for the ledger the family-agnostic dials
	// read: it is recorded (subject to the same guards) and not retried.
	if pinnedFamily != 0 {
		if isPathTimeout(err) && failed != 0 && ctx.Err() == nil && !isIPLiteralDialAddr(addr) {
			controlFamilyDemote(failed)
		}
		return nil, err
	}
	// only a family-agnostic dial has somewhere else to go
	if network != "tcp" && network != "udp" {
		return nil, err
	}
	// an IP literal fixes its own family. There is no other family to retry
	// onto -- `dial tcp6 1.1.1.1:443` is "no suitable address found" -- and no
	// name resolution whose family choice a strike could inform.
	// controlDialNetwork leaves those dials alone for the same reason.
	if isIPLiteralDialAddr(addr) {
		return nil, err
	}
	if !isPathTimeout(err) {
		return nil, err
	}
	if failed == 0 {
		return nil, err
	}
	// the timeout has to be the handshake's own -- the bound above, or
	// TlsTimeout when there was no room to apply one. The CALLER's context is
	// what is tested, never the bounded one: a caller whose budget is gone
	// gets no strike and no retry, because the strike records what this helper
	// is about to test and it cannot test anything with no time left.
	if ctx.Err() != nil {
		return nil, err
	}
	if !controlFamilyDemote(failed) {
		// refused: the other family is not usable on this device, so there is
		// nothing to retry onto
		return nil, err
	}

	retryNetwork := network + "4"
	if failed == 4 {
		retryNetwork = network + "6"
	}
	retryConn, retryErr := dial(ctx, retryNetwork, addr)
	if retryErr != nil {
		// The other family could not even connect, so the evidence does not
		// say "this family is blackholed", it says "this moment is bad": a
		// second failure over the second family is not a family problem.
		// Leaving the strike standing would narrow every control dial in the
		// process onto a family that just failed outright.
		controlFamilyUndemote(failed)
		return nil, err
	}
	retryTlsConn, retryErr := handshake(ctx, retryConn)
	if retryErr != nil {
		retryConn.Close()
		controlFamilyUndemote(failed)
		return nil, err
	}
	return retryTlsConn, nil
}

// firstHandshakeContext bounds the first handshake of a control dial to
// ControlFamilyFirstHandshakeTimeout, so that a retry over the other family
// fits inside the caller's own budget -- and returns the caller's context
// unchanged when it does not fit.
//
// A context with NO deadline is bounded: an unbounded caller has room for two
// attempts by definition, and leaving it unbounded is the one shape where a
// stalled handshake would hang until the kernel gave up, which is minutes.
func firstHandshakeContext(
	ctx context.Context,
	settings *ConnectSettings,
) (context.Context, context.CancelFunc) {
	noBound := func() (context.Context, context.CancelFunc) {
		return ctx, func() {}
	}
	if settings == nil {
		return noBound()
	}
	bound := settings.ControlFamilyFirstHandshakeTimeout
	reserve := settings.ControlFamilyRetryReserve
	if bound <= 0 || reserve <= 0 {
		return noBound()
	}
	if deadline, ok := ctx.Deadline(); ok {
		if time.Until(deadline) < bound+reserve {
			return noBound()
		}
	}
	return context.WithTimeout(ctx, bound)
}

// redialWithoutAContradictedDemotion is the only route back from a demotion
// that took the user offline.
//
// The ledger is only ever WRITTEN after a connect succeeded and a handshake
// then timed out, so a demotion can be confirmed but never refuted: when the
// family we demoted ONTO cannot connect at all, the dial fails at the connect
// step and the ledger never hears about it. Until the entry expires -- five
// minutes, doubling to a six hour cap -- every control dial in the process
// keeps being steered onto the family that just proved it does not work.
//
// A connect failure over a network a live demotion was narrowing is exactly
// that refutation. It clears the entry and dials once more with the caller's
// original family-agnostic network, which also restores the Happy Eyeballs
// race that the narrowing had switched off -- the platform's own answer to a
// PRE-connect blackhole, which already works.
//
// A FORCE is never undone here. It is an explicit developer override whose
// entire purpose is to be obeyed against this client's judgement.
func redialWithoutAContradictedDemotion(
	ctx context.Context,
	network string,
	addr string,
	dial DialContextFunction,
	dialErr error,
) (net.Conn, error) {
	if network != "tcp" && network != "udp" {
		return nil, dialErr
	}
	// a literal is never narrowed, so its failure is not the narrowing's fault
	if isIPLiteralDialAddr(addr) {
		return nil, dialErr
	}
	if ControlIpFamilyPolicy() != IpFamilyAuto {
		return nil, dialErr
	}
	demoted := controlFamilyDemotedFamily()
	if demoted == 0 {
		return nil, dialErr
	}
	if !controlFamilyUndemote(demoted) {
		return nil, dialErr
	}
	conn, err := dial(ctx, network, addr)
	if err != nil {
		return nil, dialErr
	}
	return conn, nil
}
