//go:build linux && !android

package connect

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

// The egress-bound in-process resolver for Linux. egress_resolver_windows.go
// is the reference implementation; this is the same escape by a different
// mechanism. See the file comment in egress_dial.go for why it exists at all.
//
// WHY LINUX NEEDS ITS OWN, WHEN THE !windows STUB SAID IT DID NOT. The stub's
// claim was that off Windows the OS resolver is already tunnel-aware, because
// macOS network extensions and Android VpnService route the extension's own
// lookups correctly. That is true of macOS and Android and FALSE OF LINUX, and
// the Linux client's own configuration is what makes it false:
//
//   - urnetwork-linux applies a per-link resolver to its tun (urnet0) with
//     domain `~.` — the default route for names — via resolvectl. From that
//     moment systemd-resolved sends EVERY lookup on the machine through the
//     tunnel, and the tun's advertised resolver is the UpgradeMux mask address
//     (sdk.GetDefaultTunnelDnsAddressIpv4 == DefaultDnsUpgradeMaskAddress),
//     which only answers once the tunnel carries traffic.
//   - the daemon's OWN sockets escape that, but by a mechanism that does not
//     extend to its lookups: a cgroup-BPF sock_create program stamps
//     egressSocketMark on every socket this process creates, and the client's
//     `ip rule not fwmark <mark> table <capture>` policy rule keeps marked
//     sockets out of the tun. systemd-resolved is a SEPARATE PROCESS in a
//     DIFFERENT CGROUP, so the query it re-issues on the daemon's behalf
//     carries no mark and goes into the tunnel.
//
// The result is the same deadlock the Windows service had, reached by a
// different road: tunnel up -> all DNS routed through it -> the provider
// window empties -> the daemon needs DNS to reach the platform api to fetch
// new providers -> that lookup goes into the dead tunnel -> the window never
// refills. An already-established platform connection keeps working (no name
// to resolve), which is exactly the observed signature: contracts still being
// created on the live connection while every NEW transport times out.
//
// THE ESCAPE, mirroring the Windows file: PreferGo makes net use its own
// resolver machinery instead of the platform stub, and the custom Dial gives
// every wire query a socket that leaves via the physical interface. The Go
// resolver hands Dial the server IT chose from /etc/resolv.conf — on a
// systemd-resolved machine that is the 127.0.0.53 stub, the exact dead end
// this resolver exists to avoid — so Dial keeps only the port and substitutes
// a server that is reachable off-tunnel: the physical link's own resolvers,
// or the well-known public resolvers when none can be read.
//
// THE BUILD TAG CARRIES `!android` ON PURPOSE. Go sets the `linux` tag for
// GOOS=android as well (and applies the `_linux.go` name suffix there too), so
// a bare `linux` would put this file in the mobile SDK — where a PreferGo
// resolver resolves nothing, because Android has no /etc/resolv.conf and its
// DNS configuration lives in system properties. That is what the wlynxg/anet
// dependency exists to work around, and VpnService.protect already handles
// Android's self-exclusion. Android therefore keeps egress_resolver_other.go,
// whose tag is the exact complement of this one.
//
// AND IT REMAINS BEHAVIORALLY INERT ON ANY LINUX THAT IS NOT THE TUNNEL
// DAEMON. Desktop Linux is not one platform, it is every embedding of this
// SDK: headless providers on servers with split-horizon resolvers, CI, other
// applications. The unmarked path therefore dials the server selected by Go
// unchanged while that server is still present in the live resolv.conf. Its
// one intervention is at a real configuration boundary: Go caches resolv.conf
// for five seconds, so it replaces a cached server that the live file no
// longer contains. That is required by the direct-file Linux tunnel backend,
// which temporarily installs the UpgradeMux mask and restores the host file
// at disconnect. egressSelfMarked selects the daemon's stronger off-tunnel
// substitution: a socket created right now is born carrying egressSocketMark,
// which happens only inside the cgroup urnetwork-linux attached its program
// to. Nothing in a container, in CI, or in another embedding can satisfy it by
// accident.

// egressSocketMark is the fwmark urnetwork-linux's cgroup-BPF sock_create
// program stamps on every AF_INET/AF_INET6 socket this process creates
// ("URNW" as a u32; urnetwork-linux Tunnel.hpp kEgressMark). A socket carrying
// it is excluded from the capture route table by the client's policy rule and
// accepted by its firewall's mark rule, so it leaves via the physical
// interface and never enters the tun.
const egressSocketMark = 0x55524e57

// egressBoundResolver is the resolver Linux control dials use. Same shape as
// the Windows one: the in-process Go resolver plus a Dial that substitutes an
// off-tunnel server on an off-tunnel socket for the marked tunnel daemon. The
// unmarked path preserves a current system choice and only refreshes a stale
// choice after resolv.conf changes.
var egressBoundResolver = &net.Resolver{
	PreferGo: true,
	Dial:     egressResolverDial,
}

// egressSystemResolvConfPath is the live resolver configuration consulted at
// wire-dial time. Tests replace it with a private file to force a transition.
var egressSystemResolvConfPath = "/etc/resolv.conf"

// egressSystemResolverRotation distributes a stale choice across the current
// resolver list instead of pinning every concurrent lookup to its first entry.
var egressSystemResolverRotation atomic.Uint64

// egressResolver returns the Linux control resolver unconditionally. Its Dial
// hook decides per query whether the process is the marked tunnel daemon or an
// ordinary process whose system resolver choice should be preserved.
//
// THE GATE IS INSIDE Dial, NOT HERE, and that is load-bearing rather than
// stylistic. ConnectSettings.NetDialer() is called ONCE for the primary
// control dialer — newNormalDialTlsContext builds a tls.Dialer around it and
// returns its DialContext — so whatever this function answers at construction is frozen
// into that dialer for the process's life. Gating here returned nil to every
// caller constructed before the daemon's cgroup-BPF program attaches, and the
// fix would then never engage no matter how marked the process later became:
// present in the binary, invoked by nothing. Handing the resolver out
// unconditionally and asking egressSelfMarked() per query makes the answer
// track reality, and matches egress_resolver_windows.go exactly.
func egressResolver() *net.Resolver {
	return egressBoundResolver
}

func init() {
	// egressBound() — and with it the control-dial evidence lines and
	// resolveEgressUDPAddr's escape for the H3/QUIC platform transports — asks
	// this hook whether the platform excludes its own sockets by some means
	// other than a forced interface index. On Linux that means is the fwmark.
	egressSelfExcluded = egressSelfMarked
}

// egressResolverDialTimeout bounds one wire query's dial. The Go resolver
// applies its own per-query timeout and retries across servers; this only
// keeps a single dead server from consuming the whole budget.
const egressResolverDialTimeout = 5 * time.Second

// egressResolverRotation rotates server choice across Dial calls, so the Go
// resolver's retries actually try different servers instead of re-dialing the
// same dead one.
var egressResolverRotation atomic.Uint64

// egressFallbackDnsServers are dialed when the physical link's own resolvers
// cannot be read. The same operators as DefaultDnsResolverSettings, as plain
// :53, and the same list as the Windows file's fallback.
var egressFallbackDnsServers = []string{
	"1.1.1.1",
	"9.9.9.9",
	"8.8.8.8",
	"208.67.222.222",
}

func egressResolverDial(ctx context.Context, network string, addr string) (net.Conn, error) {
	if !egressSelfMarked() {
		// Not providing a tunnel: keep the Go resolver's choice while it still
		// appears in the live system configuration. Go caches resolv.conf for
		// five seconds; after a tunnel restores the file, replace its stale
		// upgrade-mask choice at this final wire boundary.
		dialer := &net.Dialer{Timeout: egressResolverDialTimeout}
		return dialer.DialContext(ctx, network, freshSystemResolverAddress(addr))
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	index4, index6 := egressResolverFamilies()
	servers := egressDnsServers(index4, index6)
	if len(servers) == 0 {
		return nil, fmt.Errorf("egress resolver: no dns servers")
	}
	server := servers[int(egressResolverRotation.Add(1)-1)%len(servers)]
	serverAddr := net.JoinHostPort(server, port)
	// egressDialer is inert on Linux (applyEgressInterface is a no-op here and
	// nothing sets an interface index), but it is kept so an embedder that does
	// set one still gets the bind; egressMarkControl is the mechanism that
	// actually matters, and egressDialer chains it rather than replacing it.
	dialer := egressDialer(&net.Dialer{
		Timeout: egressResolverDialTimeout,
		Control: egressMarkControl,
	})
	conn, err := dialer.DialContext(ctx, network, serverAddr)
	logControlDialResult(nil, "dns", true, network, serverAddr, conn, err)
	return conn, err
}

// Revalidates Go's cached resolver choice against the live resolv.conf. A
// current choice is byte-for-byte the ordinary path; only a configuration
// transition substitutes one of the newly configured IP literals.
func freshSystemResolverAddress(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr
	}
	selected, err := netip.ParseAddr(host)
	if err != nil {
		return addr
	}
	content, err := os.ReadFile(egressSystemResolvConfPath)
	if err != nil {
		return addr
	}
	currentServers := []netip.Addr{}
	for _, line := range strings.Split(string(content), "\n") {
		if commentIndex := strings.IndexAny(line, "#;"); 0 <= commentIndex {
			line = line[:commentIndex]
		}
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "nameserver" {
			continue
		}
		server, parseErr := netip.ParseAddr(fields[1])
		if parseErr != nil {
			continue
		}
		server = server.Unmap()
		if selected.Unmap() == server {
			return addr
		}
		currentServers = append(currentServers, server)
		// Match the Go resolver's resolv.conf parser: it considers at most
		// the first three valid nameserver entries.
		if len(currentServers) == 3 {
			break
		}
	}
	if len(currentServers) == 0 {
		return addr
	}
	server := currentServers[int(egressSystemResolverRotation.Add(1)-1)%len(currentServers)]
	return net.JoinHostPort(server.String(), port)
}

// egressMarkControl sets SO_MARK on the resolver's socket at creation time,
// before connect() and therefore before the routing decision — which is the
// only point at which it can do any good (setting it later triggers
// ip_route_me_harder, which reuses a source address already taken from the
// tun; see urnetwork-linux Tunnel.hpp).
//
// BELT AND BRACES, DELIBERATELY, AND DELIBERATELY NOT FATAL. Inside the daemon
// this is redundant: the cgroup-BPF program already stamped the same value on
// this socket at inet_create(), and setting it again is a no-op with the same
// result. It is here for the case the BPF path does not cover — a process that
// steers by mark without being in that cgroup — and because SO_MARK is exactly
// the "net.Dialer.Control hook the SDK ABI does not export" that forced the
// client into cgroup-BPF in the first place; inside the SDK we do have it.
//
// A failure is swallowed rather than returned. setsockopt(SO_MARK) needs
// CAP_NET_ADMIN or CAP_NET_RAW even to write the value it already holds, so an
// unprivileged embedding gets EPERM here — and failing the dial on that would
// take DNS away from a socket the kernel had already marked correctly. The
// first failure is logged once, because a silent no-op on this path is the
// class of bug this whole file exists to undo.
func egressMarkControl(_ string, _ string, c syscall.RawConn) error {
	var innerErr error
	err := c.Control(func(fd uintptr) {
		innerErr = unix.SetsockoptInt(int(fd), unix.SOL_SOCKET, unix.SO_MARK, egressSocketMark)
	})
	if err != nil {
		return err
	}
	if innerErr != nil && egressMarkControlReported.CompareAndSwap(false, true) {
		loggerOrDefault(nil).Infof(
			"[egress]so_mark not set on the resolver socket, the cgroup program's mark still stands: %s\n",
			innerErr,
		)
	}
	return nil
}

var egressMarkControlReported atomic.Bool

// egressSelfMarkExpiration bounds reuse of the socket-mark probe. The mark
// appears once, when the client attaches its cgroup program at daemon start,
// and does not change again for the life of the process; a short reuse window
// keeps two syscalls off the per-dial path without making that one transition
// slow to notice.
const egressSelfMarkExpiration = 3 * time.Second

type egressSelfMarkResult struct {
	marked  bool
	expires time.Time
}

var egressSelfMarkCache atomic.Pointer[egressSelfMarkResult]

// egressSelfMarked reports whether a socket created right now by this process
// is born carrying egressSocketMark — i.e. whether this process's own traffic
// is steered around the tunnel it provides. This is the same read-back
// urnetwork-linux proves its cgroup program with (Tunnel.cpp
// ReadFreshSocketMark), for the same reason: it measures the kernel's actual
// behavior for a real socket rather than asserting that an attach succeeded.
func egressSelfMarked() bool {
	now := time.Now()
	if cached := egressSelfMarkCache.Load(); cached != nil && now.Before(cached.expires) {
		return cached.marked
	}
	marked := egressReadFreshSocketMark() == egressSocketMark
	egressSelfMarkCache.Store(&egressSelfMarkResult{
		marked:  marked,
		expires: now.Add(egressSelfMarkExpiration),
	})
	return marked
}

// egressReadFreshSocketMark opens a throwaway datagram socket and reads SO_MARK
// back off it. Returns 0 when the socket cannot be made or the option cannot be
// read, which is the same answer as "not marked" and lands on the inert path.
func egressReadFreshSocketMark() int {
	fd, err := unix.Socket(unix.AF_INET, unix.SOCK_DGRAM|unix.SOCK_CLOEXEC, unix.IPPROTO_UDP)
	if err != nil {
		return 0
	}
	defer unix.Close(fd)
	mark, err := unix.GetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_MARK)
	if err != nil {
		return 0
	}
	return mark
}

// egressResolverFamilies reports which address families a substituted server
// may use, in the (index4, index6) form usableEgressDnsServer takes. On Windows
// the answer is "the families the interface bind carries"; on Linux the escape
// is a socket mark, which carries both families, so the answer is "the families
// this host actually has a usable address for". Anything explicitly set through
// SetEgressInterfaceIndex still wins, so an embedder that does bind interfaces
// keeps the Windows meaning.
//
// The value is used only as a per-family "is this carried" flag and as the
// server-list cache key, so picking the first qualifying interface is enough —
// a roam changes the index and misses the cache, which is the wanted effect.
func egressResolverFamilies() (uint32, uint32) {
	index4, index6 := EgressInterfaceIndex()
	if index4 != 0 || index6 != 0 {
		return index4, index6
	}
	return egressGlobalInterfaceIndexes()
}

// egressGlobalInterfaceIndexes returns the index of an up, non-loopback
// interface carrying a global unicast address of each family, or 0 for a family
// the host has none for. Dialing a family the host cannot carry costs a full
// egressResolverDialTimeout per candidate before a usable server is reached —
// the same multi-second control-plane stall the Windows file's link-local rule
// exists to prevent — so a family with no address is reported as not carried.
//
// The tun is excluded for free: urnetwork-linux gives it 169.254.2.1, which is
// link-local and so not global unicast.
func egressGlobalInterfaceIndexes() (uint32, uint32) {
	interfaces, err := net.Interfaces()
	if err != nil {
		return 0, 0
	}
	var index4 uint32
	var index6 uint32
	for _, ifc := range interfaces {
		if ifc.Flags&net.FlagUp == 0 || ifc.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := ifc.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			ipNet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			addr, ok := netip.AddrFromSlice(ipNet.IP)
			if !ok {
				continue
			}
			addr = addr.Unmap()
			if !addr.IsGlobalUnicast() {
				continue
			}
			if addr.Is4() {
				if index4 == 0 {
					index4 = uint32(ifc.Index)
				}
			} else if index6 == 0 {
				index6 = uint32(ifc.Index)
			}
		}
		if index4 != 0 && index6 != 0 {
			break
		}
	}
	return index4, index6
}

// egressDnsServerExpiration is how long a discovered server list is reused
// before re-reading the resolver configuration (a DHCP renew or roam can change
// it; a change of carried families keys the cache miss immediately).
const egressDnsServerExpiration = 60 * time.Second

type egressDnsServerList struct {
	index4  uint32
	index6  uint32
	servers []string
	expires time.Time
}

var egressDnsServerCache atomic.Pointer[egressDnsServerList]

// egressDnsServers returns the resolver IPs control queries should use while
// this process provides a tunnel: the physical link's own resolvers — the same
// servers the OS resolver would have used before the tun's `~.` link resolver
// was pushed in front of them — or the public fallback when none can be read.
func egressDnsServers(index4 uint32, index6 uint32) []string {
	now := time.Now()
	if cached := egressDnsServerCache.Load(); cached != nil &&
		cached.index4 == index4 && cached.index6 == index6 && now.Before(cached.expires) {
		return cached.servers
	}
	servers, source := egressLinkDnsServers(index4, index6)
	if len(servers) == 0 {
		servers = egressFallbackDnsServers
		source = "fallback"
	}
	egressDnsServerCache.Store(&egressDnsServerList{
		index4:  index4,
		index6:  index6,
		servers: servers,
		expires: now.Add(egressDnsServerExpiration),
	})
	// at most one line per egressDnsServerExpiration, and only while this
	// process provides a tunnel. It names the servers control-plane DNS is
	// about to ride and where they came from, which is the difference between
	// "the fix engaged" and "the fix fell through to public resolvers".
	loggerOrDefault(nil).Infof(
		"[egress]dns servers %v source=%s if=4:%d/6:%d\n",
		servers, source, index4, index6,
	)
	return servers
}

// egressResolvConfPaths are the resolver configurations read, in order, to find
// servers reachable over the physical interface. The FIRST path that yields a
// usable server wins; the rest are not merged, so a stale or captured file
// cannot dilute a good one.
//
// Order, and why each is where it is:
//
//   - NetworkManager's no-stub file names the uplink servers NM itself
//     configured. It is first because it is the only one guaranteed not to
//     mention the tun: urnetwork-linux ships 95-urnetwork.conf and a udev rule
//     that make NM treat urnet0 as unmanaged, so NM never learns its resolver.
//     (Measured on the owner's machine: `nameserver 192.168.12.1` and the
//     router's link-local v6, while /run/NetworkManager/resolv.conf beside it
//     held only the 127.0.0.53 stub.)
//   - systemd-resolved's uplink file (NOT stub-resolv.conf) names every known
//     uplink server directly rather than the stub. Once the tun link has a `~.`
//     resolver this file lists that too, but it is the mask address and
//     usableEgressDnsServer drops it. This is the source on a machine with
//     resolved and no NetworkManager.
//   - NetworkManager's ordinary file, which is the real servers when NM is not
//     in systemd-resolved mode and the stub (dropped as loopback) when it is.
//   - /etc/resolv.conf last. On a resolved machine it is the stub and yields
//     nothing, which is the whole problem; on a machine with neither resolved
//     nor NM it is the only truth there is.
//
// Never the stub, in any of them: usableEgressDnsServer rejects loopback, so
// 127.0.0.53 and 127.0.0.1 can never be substituted. Never the tun's resolver
// either: it rejects DefaultDnsUpgradeMaskAddress by name, and that is exactly
// what urnetwork-linux puts on the tun link
// (sdk.GetDefaultTunnelDnsAddressIpv4).
var egressResolvConfPaths = []string{
	"/run/NetworkManager/no-stub-resolv.conf",
	"/run/systemd/resolve/resolv.conf",
	"/run/NetworkManager/resolv.conf",
	"/etc/resolv.conf",
}

// egressLinkDnsServers returns the first usable server list among
// egressResolvConfPaths, and the path it came from.
//
// Reading files is the whole mechanism here, and it is deliberate: the Windows
// side reads the adapter table through GetAdaptersAddresses, and the Linux
// equivalents — resolvectl, `ip`, resolved's D-Bus API — all mean either
// shelling out from a daemon or taking a D-Bus dependency into this library.
// These files are the same information, already written down, readable with no
// privilege and no child process.
func egressLinkDnsServers(index4 uint32, index6 uint32) ([]string, string) {
	for _, path := range egressResolvConfPaths {
		if servers := egressResolvConfServers(path, index4, index6); 0 < len(servers) {
			return servers, path
		}
	}
	return nil, ""
}

// egressResolvConfServers parses the `nameserver` lines of one resolv.conf(5)
// file and returns those a marked socket can actually use, filtered by the
// shared usableEgressDnsServer rule. A missing or unreadable file is not an
// error, it is simply no servers.
func egressResolvConfServers(path string, index4 uint32, index6 uint32) []string {
	content, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var servers []string
	seen := map[string]bool{}
	for _, line := range strings.Split(string(content), "\n") {
		if commentIndex := strings.IndexAny(line, "#;"); 0 <= commentIndex {
			line = line[:commentIndex]
		}
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "nameserver" {
			continue
		}
		// ParseAddr keeps a scope zone ("fe80::1%2", which is how both
		// resolved and NM write a router's link-local v6 resolver), and
		// JoinHostPort/Dial carry it through to the socket.
		addr, err := netip.ParseAddr(fields[1])
		if err != nil {
			continue
		}
		addr = addr.Unmap()
		if !usableEgressDnsServer(addr, index4, index6) {
			continue
		}
		s := addr.String()
		if !seen[s] {
			seen[s] = true
			servers = append(servers, s)
		}
	}
	return servers
}
