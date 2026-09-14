package connect

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wlynxg/anet"
)

// Address-family policy for this process's CONTROL-PLANE dials: the api
// https client, the platform control websocket, and the h3/quic transport's
// name path. The tunnelled user data plane is not affected: it carries both
// families and picks an exit by the packet's own version (IPV6.md B3). A
// family-PINNED platform transport (transport_family.go) obeys a Force by
// idling and bypasses a demotion by construction, so the policy below only
// ever steers the family-agnostic dials.
//
// Two independent pieces of state live here. The POLICY is what a developer
// set and is never changed by anything else. The DEMOTION LEDGER (below) is
// what this process has learned about a family that connects and then fails.
// Keeping them apart is what lets the ui round-trip "Auto" as "Auto" while a
// demotion is quietly in force.
//
// Process-global, like the egress interface binding in egress.go, and read
// INSIDE each dial rather than captured when a dialer is built -- a client
// strategy memoizes its http client and its tls dialer for the life of the
// process, so a value read at construction could never be changed at runtime.
type IpFamilyPolicy int32

const (
	// Happy Eyeballs as the platform provides it, plus reactive demotion.
	IpFamilyAuto IpFamilyPolicy = 0
	// Control dials use IPv4 only, whatever the ledger has learned.
	IpFamilyForce4 IpFamilyPolicy = 1
	// Control dials use IPv6 only, whatever the ledger has learned.
	IpFamilyForce6 IpFamilyPolicy = 2
)

var controlIpFamilyPolicy atomic.Int32

// controlFamilyPolicyMonitor wakes whoever parks on the policy -- a pinned
// platform transport idled by a Force -- when it changes.
var controlFamilyPolicyMonitor = NewMonitor()

// controlFamilyPolicyNotify closes on the next policy change. Capture it
// before reading ControlIpFamilyPolicy.
func controlFamilyPolicyNotify() chan struct{} {
	return controlFamilyPolicyMonitor.NotifyChannel()
}

// SetControlIpFamilyPolicy sets the control-plane family policy for this
// process. An unrecognised value is Auto rather than an error: this is fed
// from a persisted file and across a gomobile boundary where an older or
// newer peer may carry a value this build does not know, and the safe
// interpretation of "something I do not understand" is "do what you would
// have done anyway".
func SetControlIpFamilyPolicy(policy IpFamilyPolicy) {
	switch policy {
	case IpFamilyForce4, IpFamilyForce6:
	default:
		policy = IpFamilyAuto
	}
	if controlIpFamilyPolicy.Swap(int32(policy)) != int32(policy) {
		controlFamilyPolicyMonitor.NotifyAll()
	}
}

// ControlIpFamilyPolicy returns the policy alone. It never reflects a learned
// demotion -- see controlFamilyStatus for that.
func ControlIpFamilyPolicy() IpFamilyPolicy {
	return IpFamilyPolicy(controlIpFamilyPolicy.Load())
}

// controlDialNetwork narrows a family-agnostic network string ("tcp", "udp")
// to a family-specific one when a policy or a demotion says so, and returns it
// unchanged otherwise.
//
// A FORCE conflicting with an explicitly requested family is an error, which
// preserves the semantics the dead DisableIpv4/DisableIpv6 pair had: a caller
// that asked for tcp6 by name must not silently be given tcp4. A DEMOTION
// never errors -- it is a heuristic, and a heuristic must not fail a caller's
// explicit request.
//
// `addr` is the dial target, and is load bearing: see the literal check below.
func controlDialNetwork(network string, addr string) (string, error) {
	// An address that is ALREADY AN IP LITERAL has no family choice left in
	// it. There is no resolution step to steer, so narrowing cannot change
	// which family is dialed -- it can only turn a dial that would have worked
	// into an instant "no suitable address found", which is what
	// `dial tcp6 1.1.1.1:443` returns.
	//
	// That is not a corner case, it is the whole fallback layer: the extender
	// dialers dial extenderConfig.Ip (net_extender.go), and the remote plain
	// DNS resolver dials a configured resolver address (net_http_doh.go). A
	// demotion learned on the api path used to take both of them down with it,
	// and every one of those failures is a CONNECT failure, so none of them
	// recorded anything that could undo the demotion.
	if isIPLiteralDialAddr(addr) {
		return network, nil
	}

	policy := ControlIpFamilyPolicy()

	switch network {
	case "tcp4", "udp4":
		if policy == IpFamilyForce6 {
			return "", fmt.Errorf("ipv4 is disabled by the control family policy")
		}
		return network, nil
	case "tcp6", "udp6":
		if policy == IpFamilyForce4 {
			return "", fmt.Errorf("ipv6 is disabled by the control family policy")
		}
		return network, nil
	case "tcp", "udp":
		// narrowed below
	default:
		// unix sockets and anything else are not ours to reinterpret
		return network, nil
	}

	switch policy {
	case IpFamilyForce4:
		return network + "4", nil
	case IpFamilyForce6:
		return network + "6", nil
	}
	// auto: a live demotion narrows to the family that is not demoted
	switch controlFamilyDemotedFamily() {
	case 6:
		return network + "4", nil
	case 4:
		return network + "6", nil
	}
	return network, nil
}

// isIPLiteralDialAddr reports whether a dial target names an address rather
// than a host. Accepts a bare literal as well as host:port, because callers
// reach ConnectSettings.DialContext with both shapes.
func isIPLiteralDialAddr(addr string) bool {
	if addr == "" {
		return false
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	return net.ParseIP(host) != nil
}

const (
	// A demotion has to outlast the reconnect storm that follows a failure,
	// and a persistent path problem has to stop costing the user anything
	// within a couple of strikes. Five minutes doubling to six hours does
	// both: the second strike already covers a ten-minute session, and a
	// genuinely broken tunnel settles at the cap.
	controlFamilyDemotionBase = 5 * time.Minute
	controlFamilyDemotionMax  = 6 * time.Hour
)

type controlFamilyDemotion struct {
	until   time.Time
	strikes int
}

// the learned half of the policy. Guarded by its own mutex rather than folded
// into an atomic: an entry is three fields and every read is off the hot path
// of an already-blocking dial.
var controlFamilyLedger = struct {
	mu      sync.Mutex
	demoted map[int]controlFamilyDemotion
	now     func() time.Time
	probe   func(family int) bool
}{
	demoted: map[int]controlFamilyDemotion{},
	now:     time.Now,
	probe:   probeFamilySupport,
}

func init() {
	// a path change invalidates everything learned about the old path
	AddNetworkChangeListener(controlFamilyClear)
}

// controlFamilyDemote records that `family` connected and then failed. It
// reports whether the demotion took.
//
// It is REFUSED when the other family is not usable on this device. On an
// IPv6-only network with no CLAT there is no IPv4 to fall back to, and
// demoting IPv6 there would take the user from a slow control plane to no
// control plane at all.
func controlFamilyDemote(family int) bool {
	// The developer setting and the learned memory are independent state and
	// must never mix, and this is where they would. Under a force
	// there is no other family in play: a timeout on the only family the
	// policy permits is not evidence about family CHOICE, because there is
	// nothing to compare it against, and the retry it would trigger dials
	// straight into controlDialNetwork's own force-conflict error.
	//
	// The damage lands later. A strike written under Force IPv4 makes the
	// status line read "IPv4 demoted" beside a policy row reading Force IPv4,
	// and it stays armed for the moment the developer sets the row back to
	// Auto -- at which point Auto steers every control dial, every extender
	// and the h3/quic pick onto the family they had just forced away from, for
	// five minutes and up to six hours if strikes accumulated while the force
	// was on.
	if ControlIpFamilyPolicy() != IpFamilyAuto {
		return false
	}

	other := 4
	if family == 4 {
		other = 6
	}

	controlFamilyLedger.mu.Lock()

	if !controlFamilyLedger.probe(other) {
		controlFamilyLedger.mu.Unlock()
		return false
	}

	now := controlFamilyLedger.now()
	entry := controlFamilyLedger.demoted[family]
	entry.strikes += 1
	backoff := controlFamilyDemotionBase << (entry.strikes - 1)
	if backoff > controlFamilyDemotionMax || backoff <= 0 {
		backoff = controlFamilyDemotionMax
	}
	entry.until = now.Add(backoff)
	controlFamilyLedger.demoted[family] = entry
	strikes := entry.strikes
	controlFamilyLedger.mu.Unlock()

	loggerOrDefault(nil).Infof(
		"[family]demote family=%d strikes=%d for=%s\n", family, strikes, backoff)
	return true
}

// controlFamilyUndemote drops any demotion of `family`, strike count included,
// and reports whether there was one.
//
// The ledger records evidence, and evidence can be contradicted. Two things
// contradict it, both of them dial failures the ledger would otherwise never
// hear about: the family we demoted ONTO failing to connect at all, and the
// in-place retry over that family failing too. The second is not a family
// problem -- a failure over BOTH families says the moment is bad, not the
// family -- and the first is the only route back from a demotion that took
// the user offline, because a connect failure never reaches the strike path.
//
// The entry is removed rather than decremented. The backoff exists to stop a
// CONFIRMED bad path from costing the user repeatedly; a demotion the evidence
// no longer supports should not leave a doubled sentence behind for the next
// one.
func controlFamilyUndemote(family int) bool {
	controlFamilyLedger.mu.Lock()
	_, had := controlFamilyLedger.demoted[family]
	delete(controlFamilyLedger.demoted, family)
	controlFamilyLedger.mu.Unlock()

	if had {
		loggerOrDefault(nil).Infof("[family]undemote family=%d (contradicted)\n", family)
	}
	return had
}

// controlFamilyClear drops everything learned. Wired to NetworkChanged.
func controlFamilyClear() {
	controlFamilyLedger.mu.Lock()
	hadEntries := 0 < len(controlFamilyLedger.demoted)
	clear(controlFamilyLedger.demoted)
	controlFamilyLedger.mu.Unlock()

	if hadEntries {
		loggerOrDefault(nil).Infof("[family]clear (network changed)\n")
	}
}

// controlFamilyDemotedFamily returns the family currently demoted, or 0.
// A demotion of BOTH families is impossible by construction -- demoting one
// requires the other to be usable -- but if it somehow occurred, neither is
// reported, because narrowing to a family we also believe is broken is worse
// than letting the platform race them.
func controlFamilyDemotedFamily() int {
	family, _, _ := controlFamilyLiveDemotion()
	return family
}

// controlFamilyLiveDemotion returns the family currently demoted, its entry,
// and the clock reading both were judged against.
//
// The self-inflicted-outage guard is RE-EVALUATED here, on every read, and not
// only when the demotion was recorded. A demotion that was safe on the path it
// was learned on is not necessarily safe on the next one, and the ledger's only
// invalidation does not reach every process that keeps one:
// connect.NetworkChanged() has exactly one caller in the tree, DeviceLocal, so
// on ios the listener fires in the network extension and never in the APP
// process -- which is the process that dials pre-login and whenever the tunnel
// is down, the two regimes in the design's own table where a user is most
// likely to be stuck. Android registers its callbacks from initDevice, so it is
// exposed the same way while signed out.
//
// Checking on use rather than only on record closes that on every platform at
// once, without needing the host to tell us anything, and it is strictly
// stronger than a new invalidation signal would be: a demotion can never be
// APPLIED on a path where RECORDING it would have been refused. It costs a
// probe only while a demotion is live -- the rare case, and one where the
// client is already on a degraded path.
//
// An entry that fails the check is dropped rather than merely ignored, so the
// status line the developer ui reads agrees with what the dialer does, and so a
// path that stays broken is not re-probed on every dial.
func controlFamilyLiveDemotion() (int, controlFamilyDemotion, time.Time) {
	controlFamilyLedger.mu.Lock()
	now := controlFamilyLedger.now()

	live := 0
	var entry controlFamilyDemotion
	for family, candidate := range controlFamilyLedger.demoted {
		if !now.Before(candidate.until) {
			continue
		}
		if live != 0 {
			controlFamilyLedger.mu.Unlock()
			return 0, controlFamilyDemotion{}, now
		}
		live, entry = family, candidate
	}
	if live == 0 {
		controlFamilyLedger.mu.Unlock()
		return 0, controlFamilyDemotion{}, now
	}

	other := 4
	if live == 4 {
		other = 6
	}
	if controlFamilyLedger.probe(other) {
		controlFamilyLedger.mu.Unlock()
		return live, entry, now
	}
	delete(controlFamilyLedger.demoted, live)
	controlFamilyLedger.mu.Unlock()

	loggerOrDefault(nil).Infof(
		"[family]undemote family=%d (ipv%d is not usable on this path)\n", live, other)
	return 0, controlFamilyDemotion{}, now
}

// controlFamilyDemotedUntil is the raw expiry recorded for a family, zero when
// none is. Unlike controlFamilyLiveDemotion it applies no guard and no expiry
// check -- it reports what was written. Exists for the tests.
func controlFamilyDemotedUntil(family int) time.Time {
	controlFamilyLedger.mu.Lock()
	defer controlFamilyLedger.mu.Unlock()
	return controlFamilyLedger.demoted[family].until
}

// controlFamilyStatus describes any live demotion for the developer ui, and is
// empty when there is none. The ui shows this BESIDE the policy, never in
// place of it: a row that read "Force IPv4" because the heuristic fired could
// not be set back to Auto.
func controlFamilyStatus() string {
	// the same read the dialer takes, so the row cannot report a demotion that
	// is no longer being acted on
	family, entry, now := controlFamilyLiveDemotion()
	if family == 0 {
		return ""
	}
	return fmt.Sprintf(
		"IPv%d demoted for %s (%d strikes)",
		family,
		entry.until.Sub(now).Round(time.Minute),
		entry.strikes,
	)
}

// controlFamilyInterface is one interface as the probe needs to see it: a
// name, its flags, and its addresses. net.Interface plus the result of its
// Addrs() call, so the probe's decision can be exercised against a synthetic
// device rather than against whatever the build machine happens to have.
type controlFamilyInterface struct {
	name  string
	flags net.Flags
	addrs []net.Addr
}

// hostControlFamilyInterfaces enumerates this device's interfaces. An error
// from either half is returned rather than swallowed -- the probe fails
// CLOSED, and it can only do that if it is told enumeration failed.
//
// anet, not net. Android 11 restricted the netlink socket that Go's
// net.Interfaces() uses, so it returns an error there, and this repo already
// depends on wlynxg/anet for exactly that and already primes it
// (transport_p2p_webrtc_android.go calls anet.SetAndroidVersion). Off android
// anet.Interfaces is literally net.Interfaces. It matters more now than it
// would have before: a probe that fails closed on an enumeration error would
// otherwise refuse every demotion on modern android, disabling the feature on
// one of the two platforms it exists for.
func hostControlFamilyInterfaces() ([]controlFamilyInterface, error) {
	ifaces, err := anet.Interfaces()
	if err != nil {
		return nil, err
	}
	probed := make([]controlFamilyInterface, 0, len(ifaces))
	for _, iface := range ifaces {
		// A per-interface Addrs() error is not fatal: an interface can go away
		// between the enumeration and the read. It contributes no evidence,
		// which leaves the probe short of a reason to say yes -- the safe
		// direction.
		addrs, err := anet.InterfaceAddrsByInterface(&iface)
		if err != nil {
			continue
		}
		probed = append(probed, controlFamilyInterface{
			name:  iface.Name,
			flags: iface.Flags,
			addrs: addrs,
		})
	}
	return probed, nil
}

// the enumeration seam, separate from the ledger's mutex because
// probeFamilySupport runs while the ledger is held.
var controlFamilyProbeSource = struct {
	mu         sync.Mutex
	interfaces func() ([]controlFamilyInterface, error)
}{
	interfaces: hostControlFamilyInterfaces,
}

// controlFamilyTunnelInterfacePrefixes are the interface names a tunnel takes
// on the platforms this ships to: utunN on darwin/ios, tunN/tapN on
// linux/android, plus the ipsec/ppp/wg shapes another vpn on the device may
// present. Matching by NAME rather than by address range is deliberate:
// RandomLocalIpv4 (tun.go) hands the tun a 10.a.b.h address chosen precisely
// so it does NOT overlap any real local subnet, so it is indistinguishable
// from an ordinary home-lan lease by range alone. Matching by the
// FlagPointToPoint bit is not an option either -- cellular interfaces carry it
// on android, and excluding the one real path would be worse than useless.
var controlFamilyTunnelInterfacePrefixes = []string{
	"utun", "tun", "tap", "ipsec", "ppp", "wg",
}

// controlFamilyReservedPrefixes are ranges no working path can be numbered
// from, and which this project's own tunnels DO use: 192.0.2.0/24 is
// android's escape-mode tun address (MainService.ESCAPE_FALLBACK_ADDRESS).
// They pass IsGlobalUnicast, so without this they would read as connectivity.
//
// Deliberately NOT here: 192.0.0.0/29, the 464XLAT CLAT range (RFC 7335). On
// an IPv6-only mobile network the CLAT interface is the device's only IPv4
// path, and it works -- a v4-pinned platform transport dials through it and
// proves v4 over NAT64. Listing it would put every such provider to sleep for
// v4 (transport_family.go).
var controlFamilyReservedPrefixes = []*net.IPNet{
	{IP: net.IPv4(192, 0, 2, 0), Mask: net.CIDRMask(24, 32)},    // TEST-NET-1
	{IP: net.IPv4(198, 51, 100, 0), Mask: net.CIDRMask(24, 32)}, // TEST-NET-2
	{IP: net.IPv4(203, 0, 113, 0), Mask: net.CIDRMask(24, 32)},  // TEST-NET-3
	{IP: net.ParseIP("2001:db8::"), Mask: net.CIDRMask(32, 128)},
}

// probeFamilySupport reports whether this device has a usable path of the
// family THAT IS NOT THIS PRODUCT'S OWN TUNNEL.
//
// The distinction is the whole guard. The app's tun carries a global-unicast
// IPv4 address by default -- RandomLocalIpv4's 10.a.b.h (tun.go), or
// 192.0.2.1 in android's escape mode -- and net.IP.IsGlobalUnicast is true for
// both. An "is there any IsGlobalUnicast IPv4 address" probe therefore answers
// YES on an IPv6-only iphone with the tunnel up (ios has no CLAT; NAT64/DNS64
// is the App Store-required configuration), which is exactly the device where
// demoting IPv6 takes the control plane offline for five minutes and then for
// up to six hours. The tunnel is not a path to the api: this process's own
// traffic is excluded from it.
//
// NOT nettest.SupportsIPv4/SupportsIPv6: those memoize inside x/net behind a
// sync.Once, so they answer for whatever network the process started on and
// never re-evaluate across a wifi/cellular switch. A stale "yes, IPv4 works"
// is exactly the wrong answer for the guard that keeps a demotion from taking
// an IPv6-only user offline.
//
// FAILS CLOSED. An unreadable interface table means "no evidence", and the
// only job this function has is to REFUSE a dangerous demotion. A refused
// demotion costs a user a slow path; a wrongly permitted one costs them the
// whole control plane.
func probeFamilySupport(family int) bool {
	controlFamilyProbeSource.mu.Lock()
	enumerate := controlFamilyProbeSource.interfaces
	controlFamilyProbeSource.mu.Unlock()

	ifaces, err := enumerate()
	if err != nil {
		return false
	}
	for _, iface := range ifaces {
		if iface.flags&net.FlagUp == 0 {
			continue
		}
		if iface.flags&net.FlagLoopback != 0 {
			continue
		}
		if isControlFamilyTunnelInterface(iface.name) {
			continue
		}
		for _, addr := range iface.addrs {
			ip := controlFamilyAddrIP(addr)
			if ip == nil || !ip.IsGlobalUnicast() {
				continue
			}
			if isControlFamilyReservedIP(ip) {
				continue
			}
			if (ip.To4() != nil) == (family == 4) {
				return true
			}
		}
	}
	return false
}

// FamilySupported reports whether this device has a usable path of ip version
// 4 or 6 that is not this product's own tunnel. It is probeFamilySupport under
// an exported name, for the callers outside this file that must skip a family
// the host cannot prove an address for: the extender activation loop
// (EXTENDER.md G3) in the sdk and in connectctl.
//
// Deliberately the raw probe and not the ledger's view (controlFamilyProbe): an
// activation asks whether this host HAS an address of the family, and a learned
// control-plane demotion is evidence about a path, not about an address. Fails
// closed on an unreadable interface table, like the probe it wraps.
func FamilySupported(ipVersion int) bool {
	switch ipVersion {
	case 4, 6:
		return probeFamilySupport(ipVersion)
	default:
		return false
	}
}

// controlFamilyProbe is probeFamilySupport through the ledger's test seam, so
// a pinned transport's hold and a demotion's guard answer from the same source.
func controlFamilyProbe(family int) bool {
	controlFamilyLedger.mu.Lock()
	probe := controlFamilyLedger.probe
	controlFamilyLedger.mu.Unlock()
	return probe(family)
}

func controlFamilyAddrIP(addr net.Addr) net.IP {
	switch v := addr.(type) {
	case *net.IPNet:
		return v.IP
	case *net.IPAddr:
		return v.IP
	}
	return nil
}

func isControlFamilyTunnelInterface(name string) bool {
	for _, prefix := range controlFamilyTunnelInterfacePrefixes {
		if strings.HasPrefix(name, prefix) {
			return true
		}
	}
	return false
}

func isControlFamilyReservedIP(ip net.IP) bool {
	for _, reserved := range controlFamilyReservedPrefixes {
		if reserved.Contains(ip) {
			return true
		}
	}
	return false
}

// controlDialFamilyLine formats the per-dial family evidence.
//
// `family=4` / `family=6` is a LITERAL token, deliberately not derived from an
// address in the rendered line. The sdk's log redactor rewrites both IPv4 and
// IPv6 literals to the same opaque <addr:hex> shape, including the brackets
// that would otherwise give an IPv6 address away -- so in a REDACTED bundle,
// which is the mode a user is asked to send, an address cannot tell a support
// engineer which family was dialed. This token can.
func controlDialFamilyLine(
	tag string,
	network string,
	addr string,
	conn net.Conn,
	err error,
) string {
	family := "?"
	if f := connFamily(conn); f != 0 {
		family = fmt.Sprintf("%d", f)
	}
	policy := "auto"
	switch ControlIpFamilyPolicy() {
	case IpFamilyForce4:
		policy = "force4"
	case IpFamilyForce6:
		policy = "force6"
	}
	demoted := controlFamilyStatus()
	if demoted == "" {
		demoted = "none"
	}
	if err != nil {
		return fmt.Sprintf(
			"[family]dial tag=%s net=%s family=%s policy=%s demoted=%s err=%s",
			tag, network, family, policy, demoted, err)
	}
	return fmt.Sprintf(
		"[family]dial tag=%s net=%s family=%s policy=%s demoted=%s",
		tag, network, family, policy, demoted)
}

// isPathTimeout reports whether err is the post-connect timeout that proves a
// path is blackholed.
//
// Deliberately narrow. A certificate failure, an ALPN mismatch, a refusal or a
// reset all mean the packets ARRIVED and something at the far end objected --
// which says nothing about the family. Demoting on those would blame IPv6 for
// a server misconfiguration and steer every user off a healthy path, which is
// worse than the bug this exists to fix.
func isPathTimeout(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, os.ErrDeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	return false
}

// connFamily is 4, 6, or 0 when the connection has no usable remote address.
func connFamily(conn net.Conn) int {
	if conn == nil {
		return 0
	}
	addr := conn.RemoteAddr()
	if addr == nil {
		return 0
	}
	host, _, err := net.SplitHostPort(addr.String())
	if err != nil {
		host = addr.String()
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return 0
	}
	if ip.To4() != nil {
		return 4
	}
	return 6
}

// test seams. Package-private and restored by the caller.
func swapControlFamilyClock(now func() time.Time) func() {
	controlFamilyLedger.mu.Lock()
	defer controlFamilyLedger.mu.Unlock()
	prev := controlFamilyLedger.now
	controlFamilyLedger.now = now
	return func() {
		controlFamilyLedger.mu.Lock()
		defer controlFamilyLedger.mu.Unlock()
		controlFamilyLedger.now = prev
	}
}

// pickControlIPAddr chooses which resolved address a single-address control
// dial should use, honoring a force and then a demotion.
//
// Falls back to the first address when nothing matches the preference: a
// forced family the name does not publish must degrade to "dial what exists"
// rather than make the transport unusable.
func pickControlIPAddr(addrs []net.IPAddr) net.IPAddr {
	if len(addrs) == 0 {
		return net.IPAddr{}
	}
	want := 0
	switch ControlIpFamilyPolicy() {
	case IpFamilyForce4:
		want = 4
	case IpFamilyForce6:
		want = 6
	default:
		switch controlFamilyDemotedFamily() {
		case 6:
			want = 4
		case 4:
			want = 6
		}
	}
	if want == 0 {
		// No preference in force: keep net.ResolveUDPAddr's own tie-break
		// (addrs.first(isIPv4), GOROOT/src/net/ipsock.go) rather than letting
		// the resolver's own ordering -- normally IPv6-first per RFC 6724 on a
		// dual-stack, v6-capable device -- become the de facto default. Force
		// and demotion above are the only things allowed to move off IPv4.
		for _, addr := range addrs {
			if addr.IP.To4() != nil {
				return addr
			}
		}
		return addrs[0]
	}
	for _, addr := range addrs {
		if (addr.IP.To4() != nil) == (want == 4) {
			return addr
		}
	}
	return addrs[0]
}

// ControlFamilyStatus describes any live demotion, and is empty when there is
// none. For a developer ui that shows what auto has learned.
func ControlFamilyStatus() string {
	return controlFamilyStatus()
}

func swapControlFamilyInterfaces(
	interfaces func() ([]controlFamilyInterface, error),
) func() {
	controlFamilyProbeSource.mu.Lock()
	defer controlFamilyProbeSource.mu.Unlock()
	prev := controlFamilyProbeSource.interfaces
	controlFamilyProbeSource.interfaces = interfaces
	return func() {
		controlFamilyProbeSource.mu.Lock()
		defer controlFamilyProbeSource.mu.Unlock()
		controlFamilyProbeSource.interfaces = prev
	}
}

func swapControlFamilyProbe(probe func(family int) bool) func() {
	controlFamilyLedger.mu.Lock()
	defer controlFamilyLedger.mu.Unlock()
	prev := controlFamilyLedger.probe
	controlFamilyLedger.probe = probe
	return func() {
		controlFamilyLedger.mu.Lock()
		defer controlFamilyLedger.mu.Unlock()
		controlFamilyLedger.probe = prev
	}
}
