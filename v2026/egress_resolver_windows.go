//go:build windows

package connect

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"os"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// The egress-bound in-process resolver (see the file comment in
// egress_dial.go for why it exists).
//
// PreferGo makes net use its own resolver machinery instead of GetAddrInfoW,
// and the custom Dial gives every wire query an egress-bound socket. The Go
// resolver hands Dial the server IT chose from the system configuration —
// which on the tunnel-providing machine includes the tun's own mask resolver,
// the exact dead end this resolver exists to avoid — so Dial keeps only the
// port and substitutes a server that is reachable over the physical
// interface: the egress adapter's own configured resolvers, or the well-known
// public resolvers when the adapter names none. This is the same
// substitute-the-server pattern the DoH cache's plain-dns resolvers use.
var egressBoundResolver = &net.Resolver{
	PreferGo: true,
	Dial:     egressResolverDial,
}

func egressResolver() *net.Resolver {
	return egressBoundResolver
}

// egressResolverDialTimeout bounds one wire query's dial. The Go resolver
// applies its own per-query timeout and retries across servers; this only
// keeps a single dead server from consuming the whole budget.
const egressResolverDialTimeout = 5 * time.Second

// egressResolverRotation rotates server choice across Dial calls, so the Go
// resolver's retries actually try different servers instead of re-dialing the
// same dead one.
var egressResolverRotation atomic.Uint64

// egressFallbackDnsServers are dialed when the egress adapter names no usable
// resolver. The same operators as DefaultDnsResolverSettings, as plain :53.
var egressFallbackDnsServers = []string{
	"1.1.1.1",
	"9.9.9.9",
	"8.8.8.8",
	"208.67.222.222",
}

func egressResolverDial(ctx context.Context, network string, addr string) (net.Conn, error) {
	index4, index6 := EgressInterfaceIndex()
	if index4 == 0 && index6 == 0 {
		// not providing a tunnel: dial the server the Go resolver chose from
		// the system configuration, unbound — the plain platform behavior
		dialer := &net.Dialer{Timeout: egressResolverDialTimeout}
		return dialer.DialContext(ctx, network, addr)
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	servers := egressDnsServers(index4, index6)
	if len(servers) == 0 {
		return nil, fmt.Errorf("egress resolver: no dns servers")
	}
	server := servers[int(egressResolverRotation.Add(1)-1)%len(servers)]
	serverAddr := net.JoinHostPort(server, port)
	dialer := egressDialer(&net.Dialer{Timeout: egressResolverDialTimeout})
	conn, err := dialer.DialContext(ctx, network, serverAddr)
	logControlDialResult(nil, "dns", true, network, serverAddr, conn, err)
	return conn, err
}

// egressDnsServerExpiration is how long a discovered adapter server list is
// reused before re-reading the adapter table (a DHCP renew or roam can change
// it; SetEgressInterfaceIndex changes key the cache misses on immediately).
const egressDnsServerExpiration = 60 * time.Second

type egressDnsServerList struct {
	index4  uint32
	index6  uint32
	servers []string
	expires time.Time
}

var egressDnsServerCache atomic.Pointer[egressDnsServerList]

// egressDnsServers returns the resolver IPs control queries should use while
// the given egress interfaces are forced: the egress adapter's own configured
// resolvers — the same servers the OS resolver would have used before the
// tunnel's resolver was pushed in front of them — or the public fallback when
// the adapter names none usable.
func egressDnsServers(index4 uint32, index6 uint32) []string {
	now := time.Now()
	if cached := egressDnsServerCache.Load(); cached != nil &&
		cached.index4 == index4 && cached.index6 == index6 && now.Before(cached.expires) {
		return cached.servers
	}
	servers, err := egressAdapterDnsServers(index4, index6)
	if err != nil || len(servers) == 0 {
		servers = egressFallbackDnsServers
	}
	egressDnsServerCache.Store(&egressDnsServerList{
		index4:  index4,
		index6:  index6,
		servers: servers,
		expires: now.Add(egressDnsServerExpiration),
	})
	return servers
}

// egressAdapterDnsServers reads the DNS servers configured on the egress
// adapter(s) via GetAdaptersAddresses, filtered to servers a bound socket can
// actually use: no loopback (a local stub's own upstream would still be
// captured), no unspecified, no site-local v6 auto-configuration junk, never
// the tunnel's mask resolver, and only families the bind carries.
func egressAdapterDnsServers(index4 uint32, index6 uint32) ([]string, error) {
	aas, err := egressAdapterAddresses()
	if err != nil {
		return nil, err
	}
	var servers []string
	seen := map[string]bool{}
	for _, aa := range aas {
		match4 := index4 != 0 && aa.IfIndex == index4
		match6 := index6 != 0 && aa.Ipv6IfIndex == index6
		if !match4 && !match6 {
			continue
		}
		for dns := aa.FirstDnsServerAddress; dns != nil; dns = dns.Next {
			ip := dns.Address.IP()
			if ip == nil {
				continue
			}
			addr, ok := netip.AddrFromSlice(ip)
			if !ok {
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
	}
	return servers, nil
}

// egressAdapterAddresses enumerates the adapter table (the standard
// GetAdaptersAddresses grow-the-buffer loop, as in net's
// interface_windows.go).
func egressAdapterAddresses() ([]*windows.IpAdapterAddresses, error) {
	var b []byte
	size := uint32(15000) // recommended initial size per the API docs
	for {
		b = make([]byte, size)
		const flags = windows.GAA_FLAG_SKIP_ANYCAST | windows.GAA_FLAG_SKIP_MULTICAST | windows.GAA_FLAG_SKIP_FRIENDLY_NAME
		err := windows.GetAdaptersAddresses(
			windows.AF_UNSPEC,
			flags,
			0,
			(*windows.IpAdapterAddresses)(unsafe.Pointer(&b[0])),
			&size,
		)
		if err == nil {
			if size == 0 {
				return nil, nil
			}
			break
		}
		if err.(syscall.Errno) != syscall.ERROR_BUFFER_OVERFLOW {
			return nil, os.NewSyscallError("getadaptersaddresses", err)
		}
		if size <= uint32(len(b)) {
			return nil, os.NewSyscallError("getadaptersaddresses", err)
		}
	}
	var aas []*windows.IpAdapterAddresses
	for aa := (*windows.IpAdapterAddresses)(unsafe.Pointer(&b[0])); aa != nil; aa = aa.Next {
		aas = append(aas, aa)
	}
	return aas, nil
}
