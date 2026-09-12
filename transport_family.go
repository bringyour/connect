package connect

// transport_family.go — family-pinned platform transports and the group that
// runs a provider's v4 transport, v6 transport and family-agnostic standby
// (IPV6.md decisions A2, A4, A5, A6, A7).
//
// A PINNED transport proves one address family to the platform. It dials its
// family url with the network narrowed to that family regardless of the
// control-plane policy or the demotion ledger, resolves only the matching
// record type, uses direct dialers only, declares its intent in the auth
// header or frame, and never counts a dial failure against the process-wide
// backend-degraded gate. A pinned transport that cannot possibly connect is
// HELD rather than spun: it sleeps while the device has no path of its family
// and idles while a Force policy contradicts it, and it wakes on a network
// change or a policy change.
//
// The family reaches the dial layers by two routes, because the transport does
// not own them. A context value narrows the network inside
// dialControlTlsWithFamilyFallback, which every tls dial (normal and resilient)
// passes through, so name resolution requests only the pinned record type. A
// DialContextSettings wrapper installed by NewDirectClientStrategy is the final
// enforcement below resolution, and the only route for a plain ws:// dial,
// which gorilla hands to NetDialContext without any tls chain.
//
// The GROUP owns the three transports a provider runs (A4). The standby is the
// full-strategy, family-agnostic transport this product has always run; it
// dials only after StandbyDelay has elapsed with neither pinned transport
// connected, and it stands down again when one connects. That covers
// unprovisioned family names, blocked DNS and censored networks, where the
// provider is then tagged legacy v4 by the platform.
//
// Concurrency: transport state added here follows the transport's own rules
// (atomics and MonitorValues, nothing held across a dial). The group's mutable
// state is guarded by its stateLock.

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	mathrand "math/rand"
	"net"
	"net/http"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"time"

	quic "github.com/quic-go/quic-go"
)

// pinnedIpFamilyContextKey carries a pinned transport's family through the
// client strategy to the dial helpers it does not own.
type pinnedIpFamilyContextKey struct{}

// withPinnedIpFamily tags ctx with the family a dial must use. An unpinned
// family (0, or anything but 4 and 6) returns ctx unchanged.
func withPinnedIpFamily(ctx context.Context, ipFamily int) context.Context {
	if normalizeIpFamily(ipFamily) == 0 {
		return ctx
	}
	return context.WithValue(ctx, pinnedIpFamilyContextKey{}, ipFamily)
}

// pinnedIpFamilyFromContext is 4, 6, or 0 when the dial is not pinned.
func pinnedIpFamilyFromContext(ctx context.Context) int {
	if ctx == nil {
		return 0
	}
	if ipFamily, ok := ctx.Value(pinnedIpFamilyContextKey{}).(int); ok {
		return normalizeIpFamily(ipFamily)
	}
	return 0
}

// normalizeIpFamily is 4 or 6, else 0.
func normalizeIpFamily(ipFamily int) int {
	switch ipFamily {
	case 4, 6:
		return ipFamily
	default:
		return 0
	}
}

// pinnedDialNetwork narrows a family-agnostic network string to the pinned
// family. A network already of that family passes unchanged; a network of the
// OTHER family is an error, because a pinned transport must never dial the
// family it does not prove. Networks that carry no family (unix sockets) pass
// unchanged.
func pinnedDialNetwork(network string, ipFamily int) (string, error) {
	ipFamily = normalizeIpFamily(ipFamily)
	if ipFamily == 0 {
		return network, nil
	}
	switch network {
	case "tcp", "udp":
		return fmt.Sprintf("%s%d", network, ipFamily), nil
	case "tcp4", "udp4":
		if ipFamily == 4 {
			return network, nil
		}
	case "tcp6", "udp6":
		if ipFamily == 6 {
			return network, nil
		}
	default:
		return network, nil
	}
	return "", fmt.Errorf("network %s contradicts the ipv%d pin", network, ipFamily)
}

// pinnedFamilyPolicyConflict reports whether the developer's control family
// policy forbids the pinned family outright. Unlike a demotion, a Force is an
// explicit override and a pinned transport obeys it by idling (A6).
func pinnedFamilyPolicyConflict(ipFamily int) bool {
	switch ControlIpFamilyPolicy() {
	case IpFamilyForce4:
		return ipFamily == 6
	case IpFamilyForce6:
		return ipFamily == 4
	default:
		return false
	}
}

// ipLiteralFamily is 4 or 6 for a dial address that names an ip literal (with
// or without a port), else 0.
func ipLiteralFamily(addr string) int {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	ip, err := netip.ParseAddr(host)
	if err != nil {
		return 0
	}
	if ip.Unmap().Is4() {
		return 4
	}
	return 6
}

// pinnedFamilySupported reports whether this device currently has a path of
// the family that a pinned transport could use: a global address of the
// family off this product's own tunnel, and, when the process is bound to an
// egress interface that carries exactly one family, that family.
func pinnedFamilySupported(ipFamily int) bool {
	if !controlFamilyProbe(ipFamily) {
		return false
	}
	index4, index6 := EgressInterfaceIndex()
	if (index4 == 0) == (index6 == 0) {
		// unbound, or bound to an interface that carries both families:
		// the bind constrains nothing about family
		return true
	}
	if ipFamily == 4 {
		return index4 != 0
	}
	return index6 != 0
}

// udpWildcardForFamily is the family-specific wildcard a QUIC socket binds to,
// so a v6 dial never leaves an AF_INET socket and a v4 dial never depends on
// the platform's dual-stack socket behavior.
func udpWildcardForFamily(ipFamily int) (string, *net.UDPAddr) {
	if ipFamily == 6 {
		return "udp6", &net.UDPAddr{IP: net.IPv6zero, Port: 0}
	}
	return "udp4", &net.UDPAddr{IP: net.IPv4zero, Port: 0}
}

// udpAddrFamily is 4 or 6 for a resolved udp address.
func udpAddrFamily(udpAddr *net.UDPAddr) int {
	if udpAddr != nil && udpAddr.IP.To4() != nil {
		return 4
	}
	return 6
}

// NewDirectClientStrategy builds the client strategy a family-pinned platform
// transport dials with: the caller's settings without extenders and without a
// proxy, so every dial is direct and the observed remote family is the
// family the provider actually has (A4), and with the pin enforced below name
// resolution. Each pinned transport owns one, which also gives it connect
// pacing independent of the process-wide strategy (A5).
//
// The caller's settings are not mutated. An injected DialContextSettings
// (headless hosts) is kept as the inner dial so a source-identity seam still
// applies; a configured ProxySettings is dropped, because a pinned dial through
// a proxy would prove the proxy's family, not the provider's.
func NewDirectClientStrategy(ctx context.Context, settings *ClientStrategySettings, ipFamily int) *ClientStrategy {
	direct := *settings
	direct.ExtenderConfigs = nil
	direct.ExtenderDirectory = nil
	direct.ExpandExtenderProfileCount = 0
	direct.MaxExtenderCount = 0
	direct.ConnectSettings.ProxySettings = nil

	ipFamily = normalizeIpFamily(ipFamily)
	if ipFamily != 0 {
		base := direct.ConnectSettings
		base.DialContextSettings = nil
		inner := func(ctx context.Context, network string, addr string) (net.Conn, error) {
			return base.NetDialer().DialContext(ctx, network, addr)
		}
		var packetConnFactory func(context.Context) (net.PacketConn, error)
		if injected := direct.ConnectSettings.DialContextSettings; injected != nil {
			if injected.DialContext != nil {
				inner = injected.DialContext
			}
			packetConnFactory = injected.PacketConnFactory
		}
		direct.ConnectSettings.DialContextSettings = &DialContextSettings{
			DialContext:       pinnedDialContext(ipFamily, inner),
			PacketConnFactory: packetConnFactory,
		}
	}
	return NewClientStrategy(ctx, &direct)
}

// pinnedDialContext is the enforcement below name resolution: the network is
// narrowed to the pin, a Force that contradicts the pin is an error (so a plain
// ws:// dial, which bypasses the tls chain, still cannot connect against the
// developer's override), and an ip literal of the other family is refused
// rather than dialed.
func pinnedDialContext(ipFamily int, inner DialContextFunction) DialContextFunction {
	return func(ctx context.Context, network string, addr string) (net.Conn, error) {
		network, err := pinnedDialNetwork(network, ipFamily)
		if err != nil {
			return nil, err
		}
		if pinnedFamilyPolicyConflict(ipFamily) {
			return nil, fmt.Errorf("ipv%d is disabled by the control family policy", ipFamily)
		}
		if literalFamily := ipLiteralFamily(addr); literalFamily != 0 && literalFamily != ipFamily {
			return nil, fmt.Errorf("dial %s %s: address is not ipv%d", network, addr, ipFamily)
		}
		return inner(ctx, network, addr)
	}
}

// PlatformTransportState is the coarse lifecycle a transport reports to its
// owner. The group's per-family status and the sdk read it.
type PlatformTransportState int

const (
	// dialing, waiting out a backoff, or waiting for budget
	PlatformTransportStateConnecting PlatformTransportState = iota
	// a connection has routes registered
	PlatformTransportStateConnected
	// held by the owner (SetEnabled(false)); never dials
	PlatformTransportStateDisabled
	// pinned: the device currently has no path of the pinned family
	PlatformTransportStateSleeping
	// pinned: the control family policy forbids the pinned family
	PlatformTransportStateIdlePolicy
)

func (self PlatformTransportState) String() string {
	switch self {
	case PlatformTransportStateConnecting:
		return "connecting"
	case PlatformTransportStateConnected:
		return "connected"
	case PlatformTransportStateDisabled:
		return "disabled"
	case PlatformTransportStateSleeping:
		return "sleeping"
	case PlatformTransportStateIdlePolicy:
		return "idle-policy"
	default:
		return "unknown"
	}
}

// pinnedDialBackoff is a pinned transport's own reconnect pacing: jittered
// exponential from the reconnect timeout to a cap, reset by a successful
// connect or a network change. It neither reads nor advances a strategy's
// shared connect staircase, so a family that never connects cannot delay any
// other transport's dials (A5).
type pinnedDialBackoff struct {
	stateLock sync.Mutex
	base      time.Duration
	max       time.Duration
	failures  int
}

func newPinnedDialBackoff(base time.Duration, max time.Duration) *pinnedDialBackoff {
	if base <= 0 {
		base = time.Second
	}
	if max < base {
		max = base
	}
	return &pinnedDialBackoff{base: base, max: max}
}

// delay is the wait before the next dial: zero after a success or a reset,
// otherwise uniform in [d/2, d) where d doubles per consecutive failure up to
// the cap.
func (self *pinnedDialBackoff) delay() time.Duration {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.failures == 0 {
		return 0
	}
	d := self.base
	for i := 1; i < self.failures && d < self.max; i += 1 {
		d *= 2
	}
	d = min(d, self.max)
	half := d / 2
	if half <= 0 {
		return d
	}
	return half + time.Duration(mathrand.Int63n(int64(half)))
}

func (self *pinnedDialBackoff) fail() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if self.failures < 62 {
		self.failures += 1
	}
}

func (self *pinnedDialBackoff) reset() {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	self.failures = 0
}

// IpFamily is the pinned family, 4 or 6, or 0 for a family-agnostic transport.
func (self *PlatformTransport) IpFamily() int {
	return self.ipFamily
}

func (self *PlatformTransport) pinned() bool {
	return self.ipFamily != 0
}

// SetEnabled is the owner's switch. Disabling closes the live connection and
// parks every mode runner before its next dial; enabling lets them dial again.
// A transport constructed with StartDisabled begins parked.
func (self *PlatformTransport) SetEnabled(enabled bool) {
	self.enabled.Store(enabled)
	self.updateHold()
}

func (self *PlatformTransport) Enabled() bool {
	return self.enabled.Load()
}

// State is the coarse lifecycle: the owner's hold and the pinned family's own
// holds take precedence over connected, which takes precedence over connecting.
func (self *PlatformTransport) State() PlatformTransportState {
	if !self.enabled.Load() {
		return PlatformTransportStateDisabled
	}
	if hold := PlatformTransportState(self.familyHold.Load()); hold != PlatformTransportStateConnecting {
		return hold
	}
	if self.IsConnected() {
		return PlatformTransportStateConnected
	}
	return PlatformTransportStateConnecting
}

// updateHold derives the dial gate from the owner's switch and the pinned
// family's hold. A gate that just closed kicks the live connection so the
// runner re-enters admission and parks there.
func (self *PlatformTransport) updateHold() {
	held := !self.enabled.Load() || self.familyHold.Load() != int32(PlatformTransportStateConnecting)
	if self.held.Set(held) && held {
		self.kickMonitor.NotifyAll()
	}
}

// waitDialAdmission parks a mode runner while the transport is held. False
// means the transport is closing.
func (self *PlatformTransport) waitDialAdmission(ctx context.Context) bool {
	for {
		held, notify := self.held.Get()
		if !held {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-notify:
		}
	}
}

// heldNotify returns the current hold and the channel that closes when it
// changes, for runners that must react to a hold while connected.
func (self *PlatformTransport) heldNotify() (bool, chan struct{}) {
	return self.held.Get()
}

// refreshFamilyHold re-evaluates a pinned transport's own reasons not to dial.
// Policy first: a Force is definitive and cheaper than an interface walk.
func (self *PlatformTransport) refreshFamilyHold() {
	if !self.pinned() {
		return
	}
	hold := PlatformTransportStateConnecting
	if pinnedFamilyPolicyConflict(self.ipFamily) {
		hold = PlatformTransportStateIdlePolicy
	} else if !pinnedFamilySupported(self.ipFamily) {
		hold = PlatformTransportStateSleeping
	}
	previous := PlatformTransportState(self.familyHold.Swap(int32(hold)))
	if previous != hold {
		// one line per transition, never per attempt: the hold exists so a
		// family that cannot work is silent, not noisy
		self.log.Infof("[t]ipv%d transport %s -> %s\n", self.ipFamily, previous, hold)
	}
	self.updateHold()
}

// runFamilyHoldWatcher keeps a pinned transport's hold current. It wakes on a
// network change (the kick, which the transport already subscribes to) and on
// a control family policy change; the channels are captured before each
// evaluation so a change between the two cannot be missed.
func (self *PlatformTransport) runFamilyHoldWatcher() {
	for {
		kick := self.kickMonitor.NotifyChannel()
		policy := controlFamilyPolicyNotify()
		self.refreshFamilyHold()
		select {
		case <-self.ctx.Done():
			return
		case <-kick:
		case <-policy:
		}
	}
}

// dialContext tags a dial with the pinned family so the strategy's tls dial
// helper narrows the network before resolution.
func (self *PlatformTransport) dialContext(ctx context.Context) context.Context {
	return withPinnedIpFamily(ctx, self.ipFamily)
}

// applyIntentHeader declares the pinned family on the h1 v2 auth headers. A
// family-agnostic transport sends nothing, which the platform reads as legacy.
func (self *PlatformTransport) applyIntentHeader(header http.Header) {
	if self.pinned() {
		header.Set(HeaderIpFamily, fmt.Sprintf("%d", self.ipFamily))
	}
}

// authIntent is the protocol.Auth ip_family value: the pinned family, or zero.
func (self *PlatformTransport) authIntent() int32 {
	return int32(self.ipFamily)
}

// nextDialTime is the pacing before a dial. A family-agnostic transport uses
// the strategy's shared staircase and reconnect fast path exactly as before. A
// pinned transport uses only its own backoff and never touches the strategy's
// pacing state, even though it owns its strategy: the intent is that a family
// that never connects paces nobody but itself.
func (self *PlatformTransport) nextDialTime(hadConnection bool) (connectTime time.Time, releaseReconnect func(), cancelConnect func()) {
	releaseReconnect = func() {}
	cancelConnect = func() {}
	if self.pinned() {
		return time.Now().Add(self.pinnedBackoff.delay()), releaseReconnect, cancelConnect
	}
	if hadConnection {
		connectTime, releaseReconnect = self.clientStrategy.NextReconnectTime()
	} else {
		connectTime, cancelConnect = self.clientStrategy.NextConnectTime()
	}
	return connectTime, releaseReconnect, cancelConnect
}

// noteDialFailure records a failed connect. A pinned transport's failure is
// evidence about one family on this device, not about the backend, so it only
// feeds the transport's own backoff; the process-wide degraded gate is left to
// family-agnostic transports (A5).
func (self *PlatformTransport) noteDialFailure() {
	if self.pinned() {
		self.pinnedBackoff.fail()
		return
	}
	noteBackendFailure()
}

// noteDialSuccess clears the transport's own backoff and, for a
// family-agnostic transport, the process-wide degraded state.
func (self *PlatformTransport) noteDialSuccess() {
	if self.pinned() {
		self.pinnedBackoff.reset()
		return
	}
	noteBackendSuccess()
}

// noteKick resets a pinned transport's backoff: a network change is a fresh
// start for the family, not another failure.
func (self *PlatformTransport) noteKick() {
	if self.pinned() {
		self.pinnedBackoff.reset()
	}
}

// platformH3FamilyRaceStagger is the Happy Eyeballs delay between candidate
// addresses of a family-agnostic H3 dial. A definitive failure launches the
// next candidate immediately.
const platformH3FamilyRaceStagger = 250 * time.Millisecond

// resolveControlUDPAddrs is the multi-address form of resolveControlUDPAddr.
// It applies the same protected-domain, custom-resolver and egress-bound
// policy, then returns every usable address for the requested family, or for
// both families interleaved v6 first when ipFamily is 0. The process-wide Force
// and demotion narrow a family-agnostic request exactly as controlDialNetwork
// does for a stream dial; a pinned request bypasses a demotion and errors on
// a contradicting Force.
func (self *ClientStrategy) resolveControlUDPAddrs(ctx context.Context, address string, ipFamily int) ([]*net.UDPAddr, error) {
	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if host == "" {
		return nil, fmt.Errorf("resolve %s: empty host", address)
	}
	port, err := parseControlUDPPort(address, portStr)
	if err != nil {
		return nil, err
	}
	ipFamily = normalizeIpFamily(ipFamily)
	network, err := pinnedDialNetwork("udp", ipFamily)
	if err != nil {
		return nil, err
	}
	network, err = controlDialNetwork(network, address)
	if err != nil {
		return nil, err
	}
	if ipFamily != 0 && pinnedFamilyPolicyConflict(ipFamily) {
		return nil, fmt.Errorf("ipv%d is disabled by the control family policy", ipFamily)
	}

	// an ip literal needs no resolution; it must only agree with the pin
	if ip, ipErr := netip.ParseAddr(host); ipErr == nil {
		if literalFamily := ipLiteralFamily(host); ipFamily != 0 && literalFamily != ipFamily {
			return nil, fmt.Errorf("resolve %s: address is not ipv%d", address, ipFamily)
		}
		return []*net.UDPAddr{{IP: net.IP(ip.AsSlice()), Port: port, Zone: ip.Zone()}}, nil
	}

	var addrs []net.IPAddr
	if self != nil && self.internalDohResolver != nil && self.internalDohResolver.matches(host) {
		resolved, err := self.internalDohResolver.resolve(ctx, network, host)
		if err != nil {
			return nil, err
		}
		for _, addr := range resolved {
			addrs = append(addrs, net.IPAddr{IP: net.IP(addr.AsSlice()), Zone: addr.Zone()})
		}
	} else {
		// the same resolver selection as resolveEgressUDPAddr (egress_dial.go):
		// the platform's egress-bound resolver while bound, else the OS
		// resolver. A nil *net.Resolver IS the default resolver (net docs:
		// "A nil *Resolver is equivalent to the zero Resolver"), which keeps
		// this the multi-address twin of that audited no-egress path.
		var resolver *net.Resolver
		boundFamilyOnly := 0
		if self != nil && self.settings != nil && self.settings.ConnectSettings.Resolver != nil {
			resolver = self.settings.ConnectSettings.Resolver
		} else if platformResolver := egressResolver(); platformResolver != nil && egressBound() {
			resolver = platformResolver
			// the same single-family hard constraint egressBoundIPAddr
			// applies to a one-address pick
			index4, index6 := EgressInterfaceIndex()
			if (index4 == 0) != (index6 == 0) {
				boundFamilyOnly = 4
				if index6 != 0 {
					boundFamilyOnly = 6
				}
			}
		}
		resolved, err := resolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, err
		}
		for _, addr := range resolved {
			if boundFamilyOnly != 0 && ipAddrFamily(addr) != boundFamilyOnly {
				continue
			}
			addrs = append(addrs, addr)
		}
	}

	// the network may have been narrowed by the pin, a Force or a demotion
	wantFamily := 0
	switch network {
	case "udp4":
		wantFamily = 4
	case "udp6":
		wantFamily = 6
	}
	udpAddrs := []*net.UDPAddr{}
	for _, addr := range orderControlIPAddrs(addrs) {
		if wantFamily != 0 && ipAddrFamily(addr) != wantFamily {
			continue
		}
		udpAddrs = append(udpAddrs, &net.UDPAddr{IP: addr.IP, Port: port, Zone: addr.Zone})
	}
	if len(udpAddrs) == 0 {
		if wantFamily != 0 {
			return nil, fmt.Errorf("resolve %s: no ipv%d address", address, wantFamily)
		}
		return nil, fmt.Errorf("resolve %s: no addresses", address)
	}
	return udpAddrs, nil
}

func ipAddrFamily(addr net.IPAddr) int {
	if addr.IP.To4() != nil {
		return 4
	}
	return 6
}

// orderControlIPAddrs dedupes and interleaves the families v6 first, so a
// dead family costs one stagger and never the whole attempt.
func orderControlIPAddrs(addrs []net.IPAddr) []net.IPAddr {
	ipv4 := []net.IPAddr{}
	ipv6 := []net.IPAddr{}
	seen := map[string]bool{}
	for _, addr := range addrs {
		key := addr.String()
		if addr.IP == nil || seen[key] {
			continue
		}
		seen[key] = true
		if addr.IP.To4() != nil {
			ipv4 = append(ipv4, addr)
		} else {
			ipv6 = append(ipv6, addr)
		}
	}
	ordered := make([]net.IPAddr, 0, len(ipv4)+len(ipv6))
	for i := 0; i < max(len(ipv4), len(ipv6)); i += 1 {
		if i < len(ipv6) {
			ordered = append(ordered, ipv6[i])
		}
		if i < len(ipv4) {
			ordered = append(ordered, ipv4[i])
		}
	}
	return ordered
}

// h3DialAttempt is one QUIC connection in the making: its socket, transport
// and connection. Ownership is single: close releases all three.
type h3DialAttempt struct {
	udpAddr       *net.UDPAddr
	packetConn    net.PacketConn
	quicTransport *quic.Transport
	conn          *quic.Conn
	egressPinned  bool
}

func (self *h3DialAttempt) close() {
	if self == nil {
		return
	}
	if self.conn != nil {
		self.conn.CloseWithError(0, "")
	}
	if self.quicTransport != nil {
		self.quicTransport.Close()
	}
	if self.packetConn != nil {
		self.packetConn.Close()
	}
}

// openH3PacketConn is the socket for one H3 dial: the injected endpoint for a
// plain H3 dial when a factory is set, else a host UDP socket bound to the
// wildcard of the destination's family and pinned to the physical egress
// interface. The returned endpoint is owned by the caller on every non-nil
// return, including one returned alongside an error.
func (self *PlatformTransport) openH3PacketConn(ctx context.Context, ptMode TransportMode, udpAddr *net.UDPAddr) (net.PacketConn, bool, error) {
	if ptMode == TransportModeH3 && self.settings.H3PacketConnFactory != nil {
		packetConn, err := self.settings.H3PacketConnFactory(ctx)
		return packetConn, false, err
	}
	udpNetwork, wildcard := udpWildcardForFamily(udpAddrFamily(udpAddr))
	udpConn, err := net.ListenUDP(udpNetwork, wildcard)
	if err != nil {
		return nil, false, err
	}
	// bind to the physical egress interface so the platform QUIC connection
	// never loops into the tunnel this process provides (R1); a no-op off
	// Windows and when no egress index is set. a bind failure is not fatal --
	// the connection is still worth attempting -- but it must not be silent:
	// an unpinned socket here follows the route table into our own tun and
	// blackholes, which is indistinguishable from a dead network unless
	// someone says so.
	egressPinned := egressBound()
	if bindErr := applyEgress(udpConn); bindErr != nil {
		egressPinned = false
		self.log.Infof("[tr]egress bind failed, the platform connection may loop into the tunnel: %s\n", bindErr)
	}
	return udpConn, egressPinned, nil
}

// raceH3Dial is Happy Eyeballs for QUIC: candidates launch in order, each
// after platformH3FamilyRaceStagger or immediately after the previous one
// fails definitively, and the first confirmed handshake wins. Losers are
// cancelled and closed, including any that complete after the winner. The
// returned error when nothing wins is the first candidate's error, which
// names the preferred family's failure.
func raceH3Dial(
	ctx context.Context,
	candidates []*net.UDPAddr,
	dial func(ctx context.Context, udpAddr *net.UDPAddr) (*h3DialAttempt, error),
) (*h3DialAttempt, error) {
	if len(candidates) == 0 {
		return nil, errors.New("h3 race: no candidates")
	}
	type raceResult struct {
		attempt *h3DialAttempt
		err     error
	}
	results := make(chan raceResult, len(candidates))
	cancels := make([]context.CancelFunc, 0, len(candidates))
	launched := 0
	launch := func() {
		attemptCtx, cancel := context.WithCancel(ctx)
		cancels = append(cancels, cancel)
		udpAddr := candidates[launched]
		launched += 1
		go func() {
			attempt, err := dial(attemptCtx, udpAddr)
			results <- raceResult{attempt: attempt, err: err}
		}()
	}
	finish := func(pending int) {
		for _, cancel := range cancels {
			cancel()
		}
		// every launched attempt reports exactly once; a loser that completes
		// after the winner is closed here rather than leaked
		go func() {
			for range pending {
				result := <-results
				result.attempt.close()
			}
		}()
	}

	launch()
	pending := 1
	var firstErr error
	stagger := time.NewTimer(platformH3FamilyRaceStagger)
	defer stagger.Stop()
	for {
		select {
		case result := <-results:
			pending -= 1
			if result.err == nil {
				finish(pending)
				return result.attempt, nil
			}
			if firstErr == nil {
				firstErr = result.err
			}
			if launched < len(candidates) {
				launch()
				pending += 1
				stagger.Reset(platformH3FamilyRaceStagger)
			} else if pending == 0 {
				return nil, firstErr
			}
		case <-stagger.C:
			if launched < len(candidates) {
				launch()
				pending += 1
				stagger.Reset(platformH3FamilyRaceStagger)
			}
		case <-ctx.Done():
			finish(pending)
			if firstErr != nil {
				return nil, firstErr
			}
			return nil, ctx.Err()
		}
	}
}

// FamilyPlatformTransportGroupSettings tunes the provider transport group.
type FamilyPlatformTransportGroupSettings struct {
	// StandbyDelay is how long neither pinned transport may be connected
	// (since the group started, or since the last pinned disconnect) before
	// the family-agnostic standby dials.
	StandbyDelay time.Duration
}

func DefaultFamilyPlatformTransportGroupSettings() *FamilyPlatformTransportGroupSettings {
	return &FamilyPlatformTransportGroupSettings{
		StandbyDelay: 15 * time.Second,
	}
}

// FamilyPlatformTransportGroupStatus is the per-transport readout for the sdk
// and a developer screen.
type FamilyPlatformTransportGroupStatus struct {
	HasIpv4 bool
	Ipv4    PlatformTransportState
	HasIpv6 bool
	Ipv6    PlatformTransportState
	Standby PlatformTransportState
	// StandbyActive is true while the standby is released to dial, whether
	// or not it has connected yet.
	StandbyActive bool
}

// FamilyPlatformTransportGroup runs a provider's v4-pinned, v6-pinned and
// standby platform transports as one unit (A4). All three register on the
// same route manager, so the platform may route over whichever is live.
type FamilyPlatformTransportGroup struct {
	ctx    context.Context
	cancel context.CancelFunc
	log    Logger
	done   chan struct{}

	settings *FamilyPlatformTransportGroupSettings

	ipv4Transport    *PlatformTransport
	ipv6Transport    *PlatformTransport
	standbyTransport *PlatformTransport
	// owned direct-only strategies, one per pinned transport
	ipv4Strategy *ClientStrategy
	ipv6Strategy *ClientStrategy

	connectedMonitor *Monitor

	stateLock      sync.Mutex
	standbyActive  bool
	standbyDueTime time.Time
	// the group's current view, for the fan-in notify below
	lastConnected bool
}

// NewFamilyPlatformTransportGroup starts the group. platformUrlV4 and
// platformUrlV6 are the family urls (empty disables that pinned transport);
// platformUrl is the family-agnostic url the standby dials with the caller's
// full clientStrategy. clientStrategySettings seeds the direct-only strategies
// the pinned transports own. settings is cloned per transport: the pinned
// clones carry their family and background budget priority.
func NewFamilyPlatformTransportGroup(
	ctx context.Context,
	clientStrategySettings *ClientStrategySettings,
	clientStrategy *ClientStrategy,
	routeManager *RouteManager,
	platformUrl string,
	platformUrlV4 string,
	platformUrlV6 string,
	auth *ClientAuth,
	targetMode TransportMode,
	settings *PlatformTransportSettings,
	groupSettings *FamilyPlatformTransportGroupSettings,
) *FamilyPlatformTransportGroup {
	if groupSettings == nil {
		groupSettings = DefaultFamilyPlatformTransportGroupSettings()
	}
	cancelCtx, cancel := context.WithCancel(ctx)
	group := &FamilyPlatformTransportGroup{
		ctx:              cancelCtx,
		cancel:           cancel,
		log:              loggerOrDefault(settings.Log),
		done:             make(chan struct{}),
		settings:         groupSettings,
		connectedMonitor: NewMonitor(),
	}

	pinnedSettings := func(ipFamily int) *PlatformTransportSettings {
		copied := *settings
		copied.IpFamily = ipFamily
		copied.StartDisabled = false
		copied.PlatformTransportBudgetPriority = PlatformTransportBudgetPriorityBackground
		return &copied
	}
	if platformUrlV4 != "" {
		group.ipv4Strategy = NewDirectClientStrategy(cancelCtx, clientStrategySettings, 4)
		group.ipv4Transport = NewPlatformTransportWithTargetMode(
			cancelCtx,
			group.ipv4Strategy,
			routeManager,
			platformUrlV4,
			auth,
			targetMode,
			pinnedSettings(4),
		)
	}
	if platformUrlV6 != "" {
		group.ipv6Strategy = NewDirectClientStrategy(cancelCtx, clientStrategySettings, 6)
		group.ipv6Transport = NewPlatformTransportWithTargetMode(
			cancelCtx,
			group.ipv6Strategy,
			routeManager,
			platformUrlV6,
			auth,
			targetMode,
			pinnedSettings(6),
		)
	}
	hasPinned := group.ipv4Transport != nil || group.ipv6Transport != nil
	standbySettings := *settings
	standbySettings.IpFamily = 0
	// with no pinned transport at all the group is the legacy single
	// transport and the standby dials at once
	standbySettings.StartDisabled = hasPinned
	group.standbyTransport = NewPlatformTransportWithTargetMode(
		cancelCtx,
		clientStrategy,
		routeManager,
		platformUrl,
		auth,
		targetMode,
		&standbySettings,
	)
	group.standbyActive = !hasPinned
	group.standbyDueTime = time.Now().Add(groupSettings.StandbyDelay)

	go HandleError(func() {
		defer close(group.done)
		group.run()
	}, cancel)
	return group
}

// run drives the standby from the pinned transports' connected state and fans
// the three connected notifications into one.
func (self *FamilyPlatformTransportGroup) run() {
	for {
		notifies := []chan struct{}{}
		for _, transport := range self.Transports() {
			notifies = append(notifies, transport.connectedMonitor.NotifyChannel())
		}

		var timer <-chan time.Time
		var stopTimer func()
		func() {
			self.stateLock.Lock()
			defer self.stateLock.Unlock()

			now := time.Now()
			pinnedConnected := self.pinnedConnectedWithLock()
			if pinnedConnected {
				if self.standbyActive {
					self.log.Infof("[t]standby transport stands down: a pinned transport connected\n")
					self.standbyTransport.SetEnabled(false)
					self.standbyActive = false
				}
				// the delay restarts from the moment the last pinned
				// transport disconnects
				self.standbyDueTime = time.Time{}
			} else {
				if self.standbyDueTime.IsZero() {
					self.standbyDueTime = now.Add(self.settings.StandbyDelay)
				}
				if !self.standbyActive {
					if !now.Before(self.standbyDueTime) {
						self.log.Infof("[t]standby transport dials: no pinned transport connected for %s\n", self.settings.StandbyDelay)
						self.standbyTransport.SetEnabled(true)
						self.standbyActive = true
					} else {
						t := time.NewTimer(self.standbyDueTime.Sub(now))
						timer = t.C
						stopTimer = func() { t.Stop() }
					}
				}
			}

			connected := self.connectedWithLock()
			if connected != self.lastConnected {
				self.lastConnected = connected
				self.connectedMonitor.NotifyAll()
			}
		}()

		wake := make(chan struct{})
		var wakeOnce sync.Once
		for _, notify := range notifies {
			go func(notify chan struct{}) {
				select {
				case <-notify:
					wakeOnce.Do(func() { close(wake) })
				case <-wake:
				case <-self.ctx.Done():
				}
			}(notify)
		}
		select {
		case <-self.ctx.Done():
			wakeOnce.Do(func() { close(wake) })
			if stopTimer != nil {
				stopTimer()
			}
			return
		case <-wake:
			if stopTimer != nil {
				stopTimer()
			}
		case <-timer:
			wakeOnce.Do(func() { close(wake) })
		}
	}
}

func (self *FamilyPlatformTransportGroup) pinnedConnectedWithLock() bool {
	return (self.ipv4Transport != nil && self.ipv4Transport.IsConnected()) ||
		(self.ipv6Transport != nil && self.ipv6Transport.IsConnected())
}

func (self *FamilyPlatformTransportGroup) connectedWithLock() bool {
	return self.pinnedConnectedWithLock() || self.standbyTransport.IsConnected()
}

// Transports lists the live transports: v4, v6 (when configured) and standby.
func (self *FamilyPlatformTransportGroup) Transports() []*PlatformTransport {
	transports := []*PlatformTransport{}
	if self.ipv4Transport != nil {
		transports = append(transports, self.ipv4Transport)
	}
	if self.ipv6Transport != nil {
		transports = append(transports, self.ipv6Transport)
	}
	transports = append(transports, self.standbyTransport)
	return transports
}

func (self *FamilyPlatformTransportGroup) Ipv4Transport() *PlatformTransport {
	return self.ipv4Transport
}

func (self *FamilyPlatformTransportGroup) Ipv6Transport() *PlatformTransport {
	return self.ipv6Transport
}

func (self *FamilyPlatformTransportGroup) StandbyTransport() *PlatformTransport {
	return self.standbyTransport
}

// IsConnected reports whether any transport in the group has routes.
func (self *FamilyPlatformTransportGroup) IsConnected() bool {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	return self.connectedWithLock()
}

// ConnectedNotify closes on the next change of IsConnected. Capture it before
// reading IsConnected.
func (self *FamilyPlatformTransportGroup) ConnectedNotify() <-chan struct{} {
	return self.connectedMonitor.NotifyChannel()
}

// Status is the per-transport readout.
func (self *FamilyPlatformTransportGroup) Status() FamilyPlatformTransportGroupStatus {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	status := FamilyPlatformTransportGroupStatus{
		Standby:       self.standbyTransport.State(),
		StandbyActive: self.standbyActive,
	}
	if self.ipv4Transport != nil {
		status.HasIpv4 = true
		status.Ipv4 = self.ipv4Transport.State()
	}
	if self.ipv6Transport != nil {
		status.HasIpv6 = true
		status.Ipv6 = self.ipv6Transport.State()
	}
	return status
}

// SetAuth installs a new auth generation on every transport.
func (self *FamilyPlatformTransportGroup) SetAuth(auth *ClientAuth) {
	for _, transport := range self.Transports() {
		transport.SetAuth(auth)
	}
}

// Kick re-dials every transport now, as on a host network change.
func (self *FamilyPlatformTransportGroup) Kick() {
	for _, transport := range self.Transports() {
		transport.Kick()
	}
}

// IsWaitingForBudget reports whether the standby, the transport a policy
// replacement is bounded by, is blocked on the aggregate budget.
func (self *FamilyPlatformTransportGroup) IsWaitingForBudget() bool {
	return self.standbyTransport.IsWaitingForBudget()
}

// CanMakeBeforeBreakFrom pairs each transport with its counterpart in the
// group it replaces, so a policy migration keeps the old group alive until
// the new one connects without a second full working set escaping the budget.
func (self *FamilyPlatformTransportGroup) CanMakeBeforeBreakFrom(previous *FamilyPlatformTransportGroup) bool {
	if self == nil || previous == nil {
		return true
	}
	if !self.standbyTransport.CanMakeBeforeBreakFrom(previous.standbyTransport) {
		return false
	}
	if self.ipv4Transport != nil && previous.ipv4Transport != nil &&
		!self.ipv4Transport.CanMakeBeforeBreakFrom(previous.ipv4Transport) {
		return false
	}
	if self.ipv6Transport != nil && previous.ipv6Transport != nil &&
		!self.ipv6Transport.CanMakeBeforeBreakFrom(previous.ipv6Transport) {
		return false
	}
	return true
}

// Close stops every transport and the owned strategies. Nonblocking.
func (self *FamilyPlatformTransportGroup) Close() {
	self.cancel()
	for _, transport := range self.Transports() {
		transport.Close()
	}
	if self.ipv4Strategy != nil {
		self.ipv4Strategy.Close()
	}
	if self.ipv6Strategy != nil {
		self.ipv6Strategy.Close()
	}
}

// Done closes after the controller and every transport have finished.
func (self *FamilyPlatformTransportGroup) Done() <-chan struct{} {
	done := make(chan struct{})
	go func() {
		<-self.done
		for _, transport := range self.Transports() {
			<-transport.Done()
		}
		close(done)
	}()
	return done
}

// CloseAndWait closes and joins, bounded by ctx.
func (self *FamilyPlatformTransportGroup) CloseAndWait(ctx context.Context) error {
	self.Close()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-self.Done():
		return nil
	}
}

// resolveSingleControlUDPAddr is the one-address name path for the modes that
// wrap a single socket in packet translation. A pinned transport takes the
// first address of its family; a family-agnostic one keeps the strategy's
// existing single pick.
func (self *PlatformTransport) resolveSingleControlUDPAddr(ctx context.Context, address string) (*net.UDPAddr, error) {
	if self.pinned() {
		udpAddrs, err := self.clientStrategy.resolveControlUDPAddrs(ctx, address, self.ipFamily)
		if err != nil {
			return nil, err
		}
		return udpAddrs[0], nil
	}
	return self.clientStrategy.resolveControlUDPAddr(ctx, address)
}

// h3PacketConnWrapper adapts the socket of one dial for a translated mode. The
// plain mode uses the socket as is.
type h3PacketConnWrapper func(ctx context.Context, packetConn net.PacketConn) (net.PacketConn, error)

// h3DialCandidates resolves the addresses one H3 connect attempt may dial, in
// dial order, and how their sockets are wrapped. The dns modes translate one
// socket and so dial one address. The plain mode dials one address when a
// packet-connection factory owns the endpoint, and otherwise every resolved
// address: the pinned family only for a pinned transport, both families
// interleaved v6 first for a family-agnostic one (C6).
func (self *PlatformTransport) h3DialCandidates(ctx context.Context, ptMode TransportMode, serverName string) ([]*net.UDPAddr, h3PacketConnWrapper, error) {
	plain := func(_ context.Context, packetConn net.PacketConn) (net.PacketConn, error) {
		return packetConn, nil
	}
	translated := func(mode PacketTranslationMode, tld []byte) h3PacketConnWrapper {
		return func(attemptCtx context.Context, packetConn net.PacketConn) (net.PacketConn, error) {
			ptSettings := DefaultPacketTranslationSettings()
			ptSettings.DnsTlds = [][]byte{tld}
			// The connection cleanup owns the translated PacketConn. Keep its
			// encoder alive while cancellation closes QUIC gracefully; otherwise
			// the parent cancellation can discard the CONNECTION_CLOSE before
			// CloseWithError reaches the wire and leave a stale server route.
			return NewPacketTranslation(
				context.WithoutCancel(attemptCtx),
				mode,
				packetConn,
				ptSettings,
			)
		}
	}
	switch ptMode {
	case TransportModeH3Dns:
		tld := self.settings.DnsTlds[mathrand.Intn(len(self.settings.DnsTlds))]
		// The strategy resolver applies the network-space DoH policy before
		// preserving the existing egress-aware fallback. The socket can be
		// pinned while an OS name query still loops into this process's own
		// tunnel. See egress_dial.go.
		udpAddr, err := self.resolveSingleControlUDPAddr(ctx, net.JoinHostPort(serverName, strconv.Itoa(self.settings.DnsPort)))
		if err != nil {
			return nil, nil, err
		}
		return []*net.UDPAddr{udpAddr}, translated(PacketTranslationModeDns, tld), nil
	case TransportModeH3DnsPump:
		tld := self.settings.DnsTlds[mathrand.Intn(len(self.settings.DnsTlds))]
		pumpServerName := strings.TrimSpace(self.settings.DnsPumpHost)
		if pumpServerName == "" {
			return nil, nil, fmt.Errorf("H3 DNS pump host is empty")
		}
		udpAddr, err := self.resolveSingleControlUDPAddr(ctx, net.JoinHostPort(pumpServerName, strconv.Itoa(self.settings.DnsPort)))
		if err != nil {
			return nil, nil, err
		}
		return []*net.UDPAddr{udpAddr}, translated(PacketTranslationModeDnsPump, tld), nil
	default:
		address := net.JoinHostPort(serverName, strconv.Itoa(self.settings.H3Port))
		if self.settings.resolveH3AddrsForTest != nil {
			udpAddrs, err := self.settings.resolveH3AddrsForTest(ctx, address, self.ipFamily)
			return udpAddrs, plain, err
		}
		if self.settings.H3PacketConnFactory != nil {
			// an injected endpoint is one socket with its own routing, so it
			// is dialed to one address
			udpAddr, err := self.resolveSingleControlUDPAddr(ctx, address)
			if err != nil {
				return nil, nil, err
			}
			return []*net.UDPAddr{udpAddr}, plain, nil
		}
		udpAddrs, err := self.clientStrategy.resolveControlUDPAddrs(ctx, address, self.ipFamily)
		if err != nil {
			return nil, nil, err
		}
		return udpAddrs, plain, nil
	}
}

// dialH3 opens the socket for one address and completes the QUIC dial on it.
// confirm waits for the handshake to complete before reporting success, which
// a race needs: DialEarly may return as soon as cached 0-RTT parameters exist,
// before the peer has answered, and a blackholed family must not win on that.
// The returned attempt owns its socket, transport and connection.
func (self *PlatformTransport) dialH3(
	ctx context.Context,
	ptMode TransportMode,
	udpAddr *net.UDPAddr,
	wrap h3PacketConnWrapper,
	tlsConfig *tls.Config,
	quicConfig *quic.Config,
	slowMultiple int,
	confirm bool,
) (*h3DialAttempt, error) {
	packetConn, egressPinned, err := self.openH3PacketConn(ctx, ptMode, udpAddr)
	if err != nil {
		// A factory can return a usable endpoint together with an error.
		// Ownership transfers on every non-nil return, including this
		// rejected result.
		if packetConn != nil {
			packetConn.Close()
		}
		return nil, err
	}
	if packetConn == nil {
		return nil, fmt.Errorf("H3 packet connection factory returned nil")
	}
	attempt := &h3DialAttempt{
		udpAddr: udpAddr,
		packetConn: capPlatformPacketConn(
			packetConn,
			self.h3SocketReadBufferByteCount(),
			self.h3SocketWriteBufferByteCount(),
		),
		egressPinned: egressPinned,
	}
	success := false
	defer func() {
		if !success {
			attempt.close()
		}
	}()
	wrapped, err := wrap(ctx, attempt.packetConn)
	if err != nil {
		return nil, err
	}
	attempt.packetConn = wrapped

	// packetConn, not the host socket: an injected endpoint has no host
	// socket, and a packet translation reports the address of the one it wraps.
	self.log.Infof("[c]h3 connect to %v (%s) local=%v bound=%t\n", udpAddr, tlsConfig.ServerName, attempt.packetConn.LocalAddr(), egressPinned)

	attempt.quicTransport = &quic.Transport{
		Conn: attempt.packetConn,
	}
	// per attempt: a race runs several dials against one config
	attemptTlsConfig := tlsConfig.Clone()
	attemptQuicConfig := quicConfig.Clone()
	handshakeAttempt := self.settings.H3QuicPacketStats.beginHandshakeAttempt()
	if handshakeAttempt != nil {
		attemptQuicConfig.Tracer = self.settings.H3QuicPacketStats.tracerForAttempt(handshakeAttempt)
	}
	conn, err := attempt.quicTransport.DialEarly(ctx, udpAddr, attemptTlsConfig, attemptQuicConfig)
	if err != nil {
		handshakeAttempt.finish(false)
		if handshakeAttempt.sentWithoutResponse() {
			self.log.Infof(
				"[c]h3 handshake no response mode=%s sent_packets=%d pto=%d err=%s\n",
				ptMode,
				handshakeAttempt.sent.Load(),
				handshakeAttempt.pto.Load(),
				err,
			)
		}
		self.log.Infof("[c]h3 connect err = %s\n", err)
		return nil, err
	}
	attempt.conn = conn
	// DialEarly may return as soon as cached 0-RTT transport parameters
	// are available, before the peer has answered this connection. Keep
	// the attempt open until QUIC confirms the handshake or the connection
	// dies; otherwise an Initial blackhole after a 0-RTT dial is falsely
	// counted as a success and never reaches the no-response signal.
	if handshakeAttempt != nil {
		go func() {
			handshakeComplete := conn.HandshakeComplete()
			select {
			case <-handshakeComplete:
				handshakeAttempt.finish(true)
			case <-conn.Context().Done():
				// If both channels closed together, handshake completion wins.
				select {
				case <-handshakeComplete:
					handshakeAttempt.finish(true)
					return
				default:
				}
				handshakeAttempt.finish(false)
				if handshakeAttempt.sentWithoutResponse() {
					self.log.Infof(
						"[c]h3 handshake no response mode=%s sent_packets=%d pto=%d err=%s\n",
						ptMode,
						handshakeAttempt.sent.Load(),
						handshakeAttempt.pto.Load(),
						context.Cause(conn.Context()),
					)
				}
			}
		}()
	}
	if confirm {
		select {
		case <-conn.HandshakeComplete():
		case <-conn.Context().Done():
			return nil, fmt.Errorf("h3 handshake failed: %w", context.Cause(conn.Context()))
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	success = true
	return attempt, nil
}
