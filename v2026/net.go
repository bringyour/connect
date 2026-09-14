package connect

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/netip"
	"time"

	"golang.org/x/net/proxy"
)

type DialContextFunction = func(ctx context.Context, network string, addr string) (net.Conn, error)
type DialTlsContextFunction = func(ctx context.Context, network string, addr string) (net.Conn, error)

// withConnWritePhaseDeadline bounds a synchronous connection write phase by
// the earlier of the caller deadline and its phase budget. Context cancellation
// alone cannot interrupt a net.Conn.Write already in progress.
func withConnWritePhaseDeadline(
	ctx context.Context,
	conn net.Conn,
	phaseTimeout time.Duration,
	write func() error,
) (resultErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	deadline, hasDeadline := ctx.Deadline()
	if 0 < phaseTimeout {
		phaseDeadline := time.Now().Add(phaseTimeout)
		if !hasDeadline || phaseDeadline.Before(deadline) {
			deadline = phaseDeadline
			hasDeadline = true
		}
	}
	if !hasDeadline {
		return write()
	}
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return err
	}
	defer func() {
		clearErr := conn.SetWriteDeadline(time.Time{})
		if resultErr == nil {
			resultErr = clearErr
		}
	}()
	return write()
}

// writeConnPhaseWithDeadline writes the entire buffer within one bounded phase,
// handling legal short writes without dropping bytes.
func writeConnPhaseWithDeadline(
	ctx context.Context,
	conn net.Conn,
	buffer []byte,
	phaseTimeout time.Duration,
) error {
	return withConnWritePhaseDeadline(ctx, conn, phaseTimeout, func() error {
		for 0 < len(buffer) {
			n, err := conn.Write(buffer)
			if 0 < n {
				buffer = buffer[n:]
			}
			if err != nil {
				return err
			}
			if n == 0 {
				return io.ErrShortWrite
			}
		}
		return nil
	})
}

func DefaultConnectSettings() *ConnectSettings {
	tlsConfig, err := DefaultTlsConfig()
	if err != nil {
		panic(err)
	}
	return &ConnectSettings{
		RequestTimeout:   15 * time.Second,
		ConnectTimeout:   15 * time.Second,
		TlsTimeout:       15 * time.Second,
		HandshakeTimeout: 5 * time.Second,
		IdleConnTimeout:  90 * time.Second,
		KeepAliveTimeout: 5 * time.Second,
		KeepAliveConfig: net.KeepAliveConfig{
			Enable:   true,
			Idle:     5 * time.Second,
			Interval: 5 * time.Second,
			Count:    1,
		},

		ControlFamilyFirstHandshakeTimeout: 8 * time.Second,
		ControlFamilyRetryReserve:          5 * time.Second,

		TlsConfig: tlsConfig,
	}
}

type ConnectSettings struct {
	// Log, when set, is used for dial logging. nil resolves to
	// `DefaultLogger()`. `NewClientStrategy` propagates the strategy log
	// here when nil.
	Log Logger

	RequestTimeout   time.Duration
	ConnectTimeout   time.Duration
	TlsTimeout       time.Duration
	HandshakeTimeout time.Duration
	IdleConnTimeout  time.Duration
	KeepAliveTimeout time.Duration
	KeepAliveConfig  net.KeepAliveConfig

	// ControlFamilyFirstHandshakeTimeout bounds the FIRST tls handshake of a
	// control dial (dialControlTlsWithFamilyFallback) so that the retry over
	// the other address family has somewhere to run inside the caller's own
	// budget. It is what a stalled handshake hits instead of the caller's
	// deadline, and hitting it is what records a demotion.
	//
	// A FLOOR, not a fraction of the caller's budget. A fraction scales down
	// with the caller and turns a merely slow handshake into "this family is
	// blackholed" -- halving the platform control websocket's 5s gave the
	// first handshake 2.5s, which a congested mobile link reaches with a
	// pinned P-384 chain and nothing wrong. A fixed floor cannot do that: 8s
	// is more than the whole of `HandshakeTimeout`, and `HandshakeTimeout` is
	// the budget in which every shipping platform websocket dial already
	// completes a tcp connect, this same tls handshake AND an http upgrade.
	// A handshake past 8s is one this product already treats as failed
	// everywhere else.
	//
	// It is deliberately NOT `TlsTimeout`: at 15s it is at or above every
	// production caller's own budget, so nothing ever reaches it and the
	// retry never runs (see dialControlTlsWithFamilyFallback).
	//
	// <= 0 disables the bound; the first handshake then gets the caller's
	// whole remaining budget.
	ControlFamilyFirstHandshakeTimeout time.Duration

	// ControlFamilyRetryReserve is how much of the caller's budget must
	// remain AFTER a bounded first handshake before the bound is applied at
	// all. Below `ControlFamilyFirstHandshakeTimeout + ControlFamilyRetryReserve`
	// the first handshake is left unbounded, because a bound that produces a
	// timeout with no room to retry is strictly worse than no bound: it turns
	// a request that would have kept waiting into one that fails early.
	//
	// `HandshakeTimeout`'s 5s again, for the same reason: it is this
	// codebase's own statement of what a COMPLETE control dial needs, so a
	// retry given that much is given as much as a whole production websocket
	// dial gets.
	//
	// This is also what keeps the bound off the platform control websocket.
	// gorilla caps that dial context at `HandshakeTimeout`, and 5s is less
	// than 8s + 5s, so it is never bounded and its handshake tolerance is
	// untouched. It does not need its own retry: the demotion ledger is
	// process-global, so what the api path learns is already in force here.
	//
	// <= 0 disables the bound.
	ControlFamilyRetryReserve time.Duration

	TlsConfig *tls.Config

	ProxySettings *ProxySettings
	Resolver      *net.Resolver

	DialContextSettings *DialContextSettings

	// DialNetworkHook, when set, is called at the top of DialContext with the
	// network string this dial will actually use -- AFTER controlDialNetwork
	// has resolved it -- and the address.
	//
	// Test seam only, and deliberately here rather than on the net.Dialer's
	// Control callback: Control only ever sees an already-family-specific
	// network string, so a hook there cannot distinguish a "tcp4" this seam
	// resolved from a "tcp4" the caller asked for, and cannot observe that
	// this seam was skipped entirely.
	DialNetworkHook func(network string, addr string)
}

// DialContextSettings carries the paired stream and packet egress seams used
// by headless hosts which must keep one logical client on one source identity.
// ConnectSettings consumes DialContext; higher-level carrier constructors copy
// PacketConnFactory into transports which own an unconnected UDP endpoint.
type DialContextSettings struct {
	DialContext DialContextFunction
	// PacketConnFactory creates one unconnected UDP endpoint per carrier dial.
	// The caller owns and closes every non-nil endpoint, including one returned
	// alongside an error. Nil retains the platform's ordinary wildcard socket.
	PacketConnFactory func(context.Context) (net.PacketConn, error)
}

func (self *ConnectSettings) DialContext(ctx context.Context, network string, addr string) (net.Conn, error) {
	network, networkErr := controlDialNetwork(network, addr)
	if networkErr != nil {
		return nil, networkErr
	}
	if hook := self.DialNetworkHook; hook != nil {
		hook(network, addr)
	}

	var dialContext DialContextFunction

	if self.DialContextSettings != nil {
		dialContext = self.DialContextSettings.DialContext
	} else {
		netDialer := self.NetDialer()
		if self.ProxySettings != nil {
			// the proxy resolves names on its side; there is nothing to race
			dialContext = self.ProxySettings.NewDialContext(
				ctx,
				netDialer,
			)
		} else {
			dialContext = self.raceDialContext(netDialer)
		}
	}

	conn, err := dialContext(ctx, network, addr)
	if log := loggerOrDefault(self.Log).V(2); log.Enabled() {
		if err == nil {
			log.Infof("[net]dial %s %s success\n", network, addr)
		} else {
			log.Infof("[net]dial %s %s err=%s\n", network, addr, err)
		}
	}
	return conn, err
}

// raceDialContext is the direct dial path: a hostname is resolved through the
// egress-aware resolver for the families the (already narrowed) network
// permits and its addresses are raced with DefaultDialFallbackDelay, v6
// first (see net_dial_race.go). An ip literal, or a datagram network, goes
// straight to the dialer: the former has no family choice left, the latter
// cannot be raced meaningfully.
func (self *ConnectSettings) raceDialContext(netDialer *net.Dialer) DialContextFunction {
	return func(ctx context.Context, network string, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil || !isRaceableDialNetwork(network) || isIPLiteralDialAddr(host) || isZonedIPLiteralDialHost(host) {
			return netDialer.DialContext(ctx, network, addr)
		}
		addrs, err := resolveDialAddrs(ctx, dialResolver(self.Resolver), network, host)
		if err != nil {
			return nil, err
		}
		return dialHostPortRace(ctx, network, port, addrs, DefaultDialFallbackDelay, netDialer.DialContext)
	}
}

// isZonedIPLiteralDialHost catches the one literal shape isIPLiteralDialAddr
// does not: a scoped v6 literal ("fe80::1%en0"), which net.ParseIP rejects
// but a dialer accepts.
func isZonedIPLiteralDialHost(host string) bool {
	_, err := netip.ParseAddr(host)
	return err == nil
}

func (self *ConnectSettings) NetDialer() *net.Dialer {
	// egressDialer forces the physical egress interface on Windows so the
	// service's own connections never loop into the tunnel it provides (R1);
	// a no-op on other platforms and when no egress index is set.
	// egressAwareResolver is the other half of the same exclusion: a bound
	// socket is useless if the NAME the dial needs resolves through the OS
	// resolver, whose wire query (issued by svchost's DNS Client, not this
	// process) follows the tun default route to the tunnel's own resolver and
	// deadlocks behind the tunnel being built. See egress_dial.go.
	// FallbackDelay is explicit so any hostname dial that still reaches
	// the stdlib race (a caller-injected use of this dialer) paces its
	// families the same way raceDialContext does
	return egressDialer(&net.Dialer{
		Timeout:         self.ConnectTimeout,
		KeepAlive:       self.KeepAliveTimeout,
		KeepAliveConfig: self.KeepAliveConfig,
		FallbackDelay:   DefaultDialFallbackDelay,
		Resolver:        egressAwareResolver(self.Resolver),
	})
}

type ProxySettings struct {
	Network string
	Address string
	Auth    *proxy.Auth
}

func (self *ProxySettings) NewDialContext(ctx context.Context, forward proxy.Dialer) DialContextFunction {
	return func(ctx context.Context, network string, addr string) (net.Conn, error) {
		proxyDialer, err := proxy.SOCKS5(
			self.Network,
			self.Address,
			self.Auth,
			forward,
		)
		if err != nil {
			return nil, err
		}

		var conn net.Conn
		if v, ok := proxyDialer.(proxy.ContextDialer); ok {
			conn, err = v.DialContext(ctx, network, addr)
		} else {
			conn, err = proxyDialer.Dial(network, addr)
		}
		if err != nil {
			return nil, err
		}
		return conn, nil
	}
}
