package connect

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"time"
)

// Control-dial escape and evidence (R1 for the Windows VPN service, part 2).
//
// egress.go pins the SOCKETS of the service's own control connections to the
// physical interface, so they ignore the tun default route. That closed the
// route-table half of the self-capture. This file closes the other half: NAME
// RESOLUTION. Go on Windows resolves through the OS resolver (GetAddrInfoW ->
// the DNS Client service in svchost.exe), whose wire queries are a DIFFERENT
// process following the tun default route to the tunnel's own resolver — a
// resolver that only answers once the tunnel works. Every control dial to a
// hostname (platform api, platform transport) therefore deadlocked behind the
// tunnel it was trying to build, and only an OS DNS cache hit let a connect
// attempt through. The instant disconnect reverted the routes, the same calls
// completed in milliseconds — the tell that this was capture, not the backend.
//
// The escape is an in-process resolver whose queries ride egress-bound sockets
// (see egressResolver in egress_resolver_windows.go). The Windows service's
// WFP policy already carries a permit for exactly this: its DNS sublayer
// permits port 53 from the service's own process in every state, kept "so that
// the day the SDK gains an in-process resolver the permit is already correct".
// This is that day.
//
// Everything here is inert unless a platform supplies an egress resolver.
// Mobile and macOS builds keep the platform resolver they always had. Windows
// supplies a resolver whose Dial hook is inert until an egress interface is
// set. Linux supplies one whose unmarked path preserves the live system
// resolver and refreshes only a server made stale by a resolv.conf transition.

// egressSelfExcluded is the platform's answer to "are this process's own
// sockets steered around the tunnel it provides by something other than a
// forced interface index". Only Linux sets it (from an init in
// egress_resolver_linux.go): urnetwork-linux excludes the daemon with an
// fwmark stamped at socket creation by a cgroup-BPF program, never with
// IP_UNICAST_IF, so EgressInterfaceIndex stays zero there even while the
// tunnel is up and this file's escapes would otherwise all read "unbound".
// Nil — the value on Windows and on every other platform — means the interface
// index is the whole story, exactly as before.
var egressSelfExcluded func() bool

// egressBound reports whether this process's own sockets are currently steered
// around the tunnel it provides, which is true exactly while this process
// provides a tunnel: the Windows service in Connecting/Connected and the
// Windows app while the tunnel is up (a forced egress interface), and the Linux
// daemon once its cgroup program is attached (the egress fwmark).
func egressBound() bool {
	index4, index6 := EgressInterfaceIndex()
	if index4 != 0 || index6 != 0 {
		return true
	}
	return egressSelfExcluded != nil && egressSelfExcluded()
}

// egressAwareResolver returns the resolver control dials should use: the
// caller's own resolver when one is configured, else the platform's
// in-process resolver (Windows and Linux), else nil (the OS resolver). Each
// platform resolver decides at wire-dial time whether an egress escape is in
// force.
func egressAwareResolver(custom *net.Resolver) *net.Resolver {
	if custom != nil {
		return custom
	}
	return egressResolver()
}

// dialResolver is the resolver a hostname dial resolves through before its
// addresses are raced (net_dial_race.go): the caller's own when configured,
// else the platform's egress-bound in-process resolver, else the OS resolver.
// The OS resolver is reached as a value here, in this audited file, for the
// same reason resolveEgressUDPAddr does it: net.Resolver methods need a
// non-nil receiver to take a context.
func dialResolver(custom *net.Resolver) *net.Resolver {
	if resolver := egressAwareResolver(custom); resolver != nil {
		return resolver
	}
	return net.DefaultResolver
}

// resolveEgressUDPAddr is net.ResolveUDPAddr for control dials, made
// family-aware: while this process's own sockets are steered around the
// tunnel it provides, it resolves through the egress-bound resolver instead
// of the OS resolver. On a platform with no egress escape, or with none in
// force -- which is every mobile build, since egressBound() is never true
// there -- it resolves through the OS resolver instead. Either way, the
// address returned is chosen by pickControlIPAddr rather than taken as the
// resolver's first result, so a forced family or a live demotion applies on
// both paths.
//
// While bound, an interface index that is set for exactly one family (the
// normal single-homed shape) is a hard constraint and overrides the pick;
// when both are set (or neither), the bind constrains nothing about family
// and the preference from pickControlIPAddr stands.
//
// This is the H3/QUIC platform transport's name path (transport.go), and it is
// the second half of the Linux fix: pinning the resolver into NetDialer alone
// would leave these three call sites resolving through the captured stub.
func resolveEgressUDPAddr(ctx context.Context, addr string) (*net.UDPAddr, error) {
	platformResolver := egressResolver()
	bound := platformResolver != nil && egressBound()
	return resolveEgressUDPAddrWithResolvers(
		ctx,
		addr,
		platformResolver,
		net.DefaultResolver,
		bound,
	)
}

// resolveEgressUDPAddrWithResolvers keeps resolver selection independently
// exercisable without replacing net.DefaultResolver process-wide. Production
// supplies the platform and default resolvers above; tests supply a controlled
// default resolver so host files and machine DNS cannot decide the result.
func resolveEgressUDPAddrWithResolvers(
	ctx context.Context,
	addr string,
	platformResolver *net.Resolver,
	defaultResolver *net.Resolver,
	bound bool,
) (*net.UDPAddr, error) {
	resolver := platformResolver
	if !bound {
		// Keep the platform resolver but not net.ResolveUDPAddr: the caller
		// supplied a lifecycle context specifically so a transport shutdown can
		// interrupt a name lookup.
		resolver = defaultResolver
	}
	return resolveUDPAddrWithResolver(ctx, addr, resolver, bound)
}

// resolveUDPAddrWithResolver is the context-aware UDP name path shared by the
// platform resolver and an explicitly configured client-strategy resolver.
// nil means the platform default resolver. bound applies an egress interface's
// hard single-family constraint after the process-wide family preference.
func resolveUDPAddrWithResolver(ctx context.Context, addr string, resolver *net.Resolver, bound bool) (*net.UDPAddr, error) {
	if resolver == nil {
		// Keep the platform resolver but not net.ResolveUDPAddr: the caller
		// supplied a lifecycle context specifically so a transport shutdown can
		// interrupt a name lookup.
		resolver = net.DefaultResolver
	}

	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	if host == "" {
		return nil, fmt.Errorf("resolve %s: empty host", addr)
	}
	port, err := parseControlUDPPort(addr, portStr)
	if err != nil {
		return nil, err
	}
	// an ip literal needs no resolution and has no family choice to make
	if ip, ipErr := netip.ParseAddr(host); ipErr == nil {
		return &net.UDPAddr{IP: net.IP(ip.AsSlice()), Port: port, Zone: ip.Zone()}, nil
	}
	addrs, err := resolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, err
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("resolve %s: no addresses", host)
	}
	pick := pickControlIPAddr(addrs)
	if bound {
		index4, index6 := EgressInterfaceIndex()
		pick = egressBoundIPAddr(addrs, index4, index6, pick)
	}
	return &net.UDPAddr{IP: pick.IP, Port: port, Zone: pick.Zone}, nil
}

// parseControlUDPPort keeps the raw UDP resolver paths from producing an
// address net cannot dial. These paths accept numeric ports only and reject
// the same out-of-range values as net.Resolver.LookupPort.
func parseControlUDPPort(addr string, portStr string) (int, error) {
	port, err := strconv.Atoi(portStr)
	if err != nil {
		return 0, fmt.Errorf("resolve %s: non-numeric port: %w", addr, err)
	}
	if port < 0 || 65535 < port {
		return 0, fmt.Errorf("resolve %s: invalid port %q", addr, portStr)
	}
	return port, nil
}

// egressBoundIPAddr applies an interface-index bind's family constraint on
// top of pickControlIPAddr's preference, when the bind actually expresses
// one. Only a SINGLE family's index being set is a family constraint. With
// both indexes set -- the normal shape while the Windows service holds the
// tunnel up, per EgressInterfaceIndex's doc comment -- the bind constrains
// nothing about family: a naive "first address whose family has a nonzero
// index" loop would match addrs[0] on its very first iteration regardless of
// family, silently discarding pick (and with it any Force4/Force6 or
// demotion). With neither index set there is no bind at all. So the override
// only runs when exactly one of index4/index6 is nonzero; otherwise pick
// stands unchanged.
func egressBoundIPAddr(addrs []net.IPAddr, index4 uint32, index6 uint32, pick net.IPAddr) net.IPAddr {
	if (index4 == 0) == (index6 == 0) {
		return pick
	}
	for _, a := range addrs {
		is4 := a.IP.To4() != nil
		if (is4 && index4 != 0) || (!is4 && index6 != 0) {
			return a
		}
	}
	return pick
}

// usableEgressDnsServer reports whether an adapter-configured resolver can be
// dialed by an egress-bound socket: no loopback (a local stub's own upstream
// would still be captured by the tun route), no unspecified, never the
// tunnel's own mask resolver, no fec0::/10 site-local auto-configuration
// junk, and only families the bind carries.
func usableEgressDnsServer(addr netip.Addr, index4 uint32, index6 uint32) bool {
	addr = addr.Unmap()
	if addr.IsLoopback() || addr.IsUnspecified() {
		return false
	}
	if maskAddr, err := netip.ParseAddr(DefaultDnsUpgradeMaskAddress); err == nil && addr == maskAddr {
		return false
	}
	// IPv4 link-local (169.254/16) is what Windows assigns when DHCP fails, and
	// what virtual/host-only adapters (VMware VMnet*, Hyper-V, VirtualBox)
	// routinely carry. Such an address can never answer a query, but a bound
	// socket still spends the full dial timeout finding that out -- once per
	// candidate, before a usable server is reached. That is the shape of a
	// multi-second control-plane stall that presents as a dead network. The v6
	// branch below already excludes its equivalent (site-local); this is the
	// missing v4 half of the same rule.
	if addr.Is4() {
		if addr.IsLinkLocalUnicast() {
			return false
		}
		return index4 != 0
	}
	if index6 == 0 {
		return false
	}
	if b := addr.As16(); b[0] == 0xfe && (b[1]&0xc0) == 0xc0 {
		return false
	}
	return true
}

// --- control-dial evidence -------------------------------------------------
//
// While an egress interface is forced, every control dial logs one line at
// default verbosity: path tag, target, the socket's local address, the forced
// interface indexes, and whether this path rides the bound dialer. This is the
// line the checkpoint test reads from the testers' logs to prove the fix.
// Repeats of the same tag+target are count-suppressed on the same pattern as
// the existing "[t]auth error ... (N suppressed)" lines.

// controlDialLogInterval is the per-(tag,target) emission floor. A connecting
// window retries the same few targets continuously; one line per target per
// interval keeps the signal without the flood.
const controlDialLogInterval = 5 * time.Second

var controlDialThrottleLock sync.Mutex
var controlDialThrottles = map[string]*logThrottle{}

// controlDialThrottleCap bounds the throttle map. Control targets are a small
// closed set (platform hosts, doh servers, dns servers); hitting the cap means
// something unexpected is being tagged, and resetting is cheaper than growing.
const controlDialThrottleCap = 512

func controlDialThrottle(key string) *logThrottle {
	controlDialThrottleLock.Lock()
	defer controlDialThrottleLock.Unlock()
	if t, ok := controlDialThrottles[key]; ok {
		return t
	}
	if controlDialThrottleCap <= len(controlDialThrottles) {
		clear(controlDialThrottles)
	}
	t := newLogThrottle(controlDialLogInterval)
	controlDialThrottles[key] = t
	return t
}

// logControlDialResult emits the per-dial evidence line. Quiet unless an
// egress interface is forced, so mobile and app builds are unaffected.
// `bound` is whether this dial path rides the egress-bound dialer — false
// names a path that is in-tunnel by design (e.g. the mux's tunnel DoH).
func logControlDialResult(log Logger, tag string, bound bool, network string, addr string, conn net.Conn, err error) {
	l := loggerOrDefault(log)

	// The family line is NOT gated on egressBound(). The [egress]dial line
	// below is desktop-only by design -- it reports an interface binding that
	// only Windows and macOS ever force -- but Android and iOS are the two
	// platforms where a broken address family is actually reported, and until
	// this line existed no mobile bundle at any verbosity could say which
	// family a control dial used.
	if ok, _ := controlDialThrottle("family|" + tag + "|" + addr).Allow(time.Now()); ok {
		l.Infof("%s\n", controlDialFamilyLine(tag, network, addr, conn, err))
	}

	if !egressBound() {
		return
	}
	ok, suppressed := controlDialThrottle(tag + "|" + addr).Allow(time.Now())
	if !ok {
		return
	}
	index4, index6 := EgressInterfaceIndex()
	boundStr := "no"
	if bound {
		boundStr = "yes"
	}
	suffix := ""
	if 0 < suppressed {
		suffix = fmt.Sprintf(" (%d suppressed)", suppressed)
	}
	if err != nil {
		l.Infof("[egress]dial tag=%s %s %s if=4:%d/6:%d bound=%s err=%s%s\n", tag, network, addr, index4, index6, boundStr, err, suffix)
	} else {
		local := "?"
		if conn != nil && conn.LocalAddr() != nil {
			local = conn.LocalAddr().String()
		}
		l.Infof("[egress]dial tag=%s %s %s local=%s if=4:%d/6:%d bound=%s%s\n", tag, network, addr, local, index4, index6, boundStr, suffix)
	}
}

// wrapControlDial adds the control-dial evidence line to a dial function.
// The wrapped function is otherwise transparent.
func wrapControlDial(tag string, log Logger, bound bool, dial DialContextFunction) DialContextFunction {
	return func(ctx context.Context, network string, addr string) (net.Conn, error) {
		conn, err := dial(ctx, network, addr)
		logControlDialResult(log, tag, bound, network, addr, conn, err)
		return conn, err
	}
}
