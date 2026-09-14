package connect

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"sort"
	"strings"
	"sync/atomic"
)

// internalDohDialFallbackDelay is the Happy Eyeballs delay between raw
// addresses, the package-wide DefaultDialFallbackDelay. A definitive failure
// launches the next address immediately.
const internalDohDialFallbackDelay = DefaultDialFallbackDelay

// internalDohResolver is the client strategy's trusted name path for the
// network-space domains. It resolves with the existing bounded DoH cache and
// passes only IP literals to the underlying dialer. TLS remains outside this
// layer, where it still sees the original hostname.
type internalDohResolver struct {
	cache    *DohCache
	domains  []string
	nextAddr atomic.Uint64
}

// clientStrategySettingsWithInternalDoh installs the resolver ahead of every
// normal, resilient, proxy, and injected control dial. The base settings are
// captured before installing the wrapper so non-owned names retain their
// existing resolution path and DoH endpoint dials cannot recurse through the
// protected-domain rule.
func clientStrategySettingsWithInternalDoh(settings *ClientStrategySettings) (*ClientStrategySettings, *internalDohResolver) {
	if settings == nil || settings.ConnectSettings.Resolver != nil {
		return settings, nil
	}
	domains := normalizeInternalDohDomains(settings.InternalDohDomains)
	if len(domains) == 0 {
		return settings, nil
	}

	copied := *settings
	copied.ConnectSettings = settings.ConnectSettings
	baseConnectSettings := copied.ConnectSettings
	resolver := &internalDohResolver{
		cache:   NewDohCache(internalDohSettings(settings.DohSettings)),
		domains: domains,
	}
	copied.ConnectSettings.DialContextSettings = &DialContextSettings{
		DialContext: resolver.wrapDialContext(baseConnectSettings.DialContext),
	}
	return &copied, resolver
}

// internalDohSettings makes an owned copy because the control-name policy
// deliberately excludes plain DNS fallback. The DoH endpoints themselves are
// still resolved by DohCache's bootstrap rule when a caller configured a
// hostname endpoint; production defaults are IP literals and need no DNS.
func internalDohSettings(settings *DohSettings) *DohSettings {
	if settings == nil {
		settings = DefaultDohSettings()
	}
	copied := *settings
	if settings.DnsResolverSettings == nil {
		copied.DnsResolverSettings = DefaultDnsResolverSettings()
	} else {
		resolver := *settings.DnsResolverSettings
		resolver.RemoteDohUrlsIpv4 = append([]string(nil), resolver.RemoteDohUrlsIpv4...)
		resolver.RemoteDohUrlsIpv6 = append([]string(nil), resolver.RemoteDohUrlsIpv6...)
		resolver.LocalDohUrlsIpv4 = append([]string(nil), resolver.LocalDohUrlsIpv4...)
		resolver.LocalDohUrlsIpv6 = append([]string(nil), resolver.LocalDohUrlsIpv6...)
		resolver.RemoteDnsIpv4 = append([]string(nil), resolver.RemoteDnsIpv4...)
		resolver.RemoteDnsIpv6 = append([]string(nil), resolver.RemoteDnsIpv6...)
		resolver.LocalDnsIpv4 = append([]string(nil), resolver.LocalDnsIpv4...)
		resolver.LocalDnsIpv6 = append([]string(nil), resolver.LocalDnsIpv6...)
		copied.DnsResolverSettings = &resolver
	}
	copied.DnsResolverSettings.EnableRemoteDns = false
	copied.DnsResolverSettings.EnableLocalDns = false
	// The protected set is a handful of service names, so retain enough room
	// for migrations without paying the general-purpose resolver's cache and
	// concurrency ceilings on every NetworkSpace.
	copied.CacheMaxEntries = internalDohBound(copied.CacheMaxEntries, 64)
	copied.MaxConcurrentResolutions = internalDohBound(copied.MaxConcurrentResolutions, 8)
	copied.MaxConcurrentHttpRequests = internalDohBound(copied.MaxConcurrentHttpRequests, 4)
	return &copied
}

func internalDohBound(value int, ceiling int) int {
	if value <= 0 || ceiling < value {
		return ceiling
	}
	return value
}

func normalizeInternalDohDomains(domains []string) []string {
	seen := map[string]bool{}
	normalized := make([]string, 0, len(domains))
	for _, domain := range domains {
		domain = normalizeInternalDohName(domain)
		if domain == "" || seen[domain] {
			continue
		}
		if _, err := netip.ParseAddr(domain); err == nil {
			continue
		}
		// A network-space domain is an FQDN-like suffix, not a single DNS label.
		// Besides avoiding accidental capture of an entire top-level domain,
		// this leaves local/test names such as "localhost" and "test" on the
		// resolver path their embedder configured.
		if !strings.Contains(domain, ".") {
			continue
		}
		seen[domain] = true
		normalized = append(normalized, domain)
	}
	sort.Strings(normalized)
	return normalized
}

func normalizeInternalDohName(name string) string {
	name = strings.TrimSuffix(strings.TrimSpace(name), ".")
	if name == "" {
		return ""
	}
	ascii, err := Punycode(name)
	if err != nil {
		return ""
	}
	return strings.ToLower(strings.TrimSuffix(ascii, "."))
}

func (self *internalDohResolver) matches(host string) bool {
	host = normalizeInternalDohName(host)
	if host == "" {
		return false
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return false
	}
	for _, domain := range self.domains {
		if host == domain || strings.HasSuffix(host, "."+domain) {
			return true
		}
	}
	return false
}

// resolve answers a protected name for the families the (already narrowed)
// network permits, ordered for the race. See resolveDohDialAddrs.
func (self *internalDohResolver) resolve(ctx context.Context, network string, host string) ([]netip.Addr, error) {
	return resolveDohDialAddrs(ctx, self.cache, network, host)
}

// orderInternalDohAddrs is the shared race order (orderDialAddrs): families
// alternate v6 first so a dead path cannot consume the whole request deadline.
func orderInternalDohAddrs(addrs []netip.Addr) []netip.Addr {
	return orderDialAddrs(addrs)
}

func (self *internalDohResolver) wrapDialContext(dialContext DialContextFunction) DialContextFunction {
	return func(ctx context.Context, network string, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil || !self.matches(host) {
			return dialContext(ctx, network, address)
		}
		addrs, err := self.resolve(ctx, network, host)
		if err != nil {
			return nil, err
		}
		return dialInternalDohAddrs(ctx, network, port, addrs, dialContext)
	}
}

// dialInternalDohAddrs races the resolved addresses of a protected name
// through the underlying dialer. It is the shared race (dialAddrsRace) with
// the fallback delay every hostname dial uses.
func dialInternalDohAddrs(
	ctx context.Context,
	network string,
	port string,
	addrs []netip.Addr,
	dialContext DialContextFunction,
) (net.Conn, error) {
	if len(addrs) == 0 {
		return nil, fmt.Errorf("internal DoH returned no addresses")
	}
	return dialHostPortRace(ctx, network, port, addrs, internalDohDialFallbackDelay, dialContext)
}

func (self *internalDohResolver) resolveUDPAddr(ctx context.Context, address string) (*net.UDPAddr, error) {
	host, portString, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if host == "" {
		return nil, fmt.Errorf("resolve %s: empty host", address)
	}
	port, err := parseControlUDPPort(address, portString)
	if err != nil {
		return nil, err
	}
	// Unlike the stream path, this method is called directly by QUIC and
	// packet-translation transports rather than through ConnectSettings.
	// Resolve the process-wide family policy here, at dial time, so a strategy
	// constructed before a runtime policy change does not keep stale behavior.
	network, err := controlDialNetwork("udp", address)
	if err != nil {
		return nil, err
	}
	addrs, err := self.resolve(ctx, network, host)
	if err != nil {
		return nil, err
	}
	index := int((self.nextAddr.Add(1) - 1) % uint64(len(addrs)))
	addr := addrs[index]
	return &net.UDPAddr{IP: net.IP(addr.AsSlice()), Port: port, Zone: addr.Zone()}, nil
}

// resolveUDPAddrs is the list form of resolveUDPAddr for a caller that
// races families itself (the QUIC platform transport): every address of the
// protected name that the runtime family policy permits, in race order.
func (self *internalDohResolver) resolveUDPAddrs(ctx context.Context, address string) ([]*net.UDPAddr, error) {
	host, portString, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	if host == "" {
		return nil, fmt.Errorf("resolve %s: empty host", address)
	}
	port, err := parseControlUDPPort(address, portString)
	if err != nil {
		return nil, err
	}
	network, err := controlDialNetwork("udp", address)
	if err != nil {
		return nil, err
	}
	addrs, err := self.resolve(ctx, network, host)
	if err != nil {
		return nil, err
	}
	udpAddrs := make([]*net.UDPAddr, 0, len(addrs))
	for _, addr := range addrs {
		udpAddrs = append(udpAddrs, &net.UDPAddr{IP: net.IP(addr.AsSlice()), Port: port, Zone: addr.Zone()})
	}
	return udpAddrs, nil
}

func (self *internalDohResolver) CloseIdleConnections() {
	self.cache.CloseIdleConnections()
}

func (self *internalDohResolver) Close() {
	self.cache.Close()
}

// resolveControlUDPAddr applies the same protected-domain policy to QUIC and
// packet-translation transports. Those callers already set TLS.ServerName
// from the original platform URL after receiving this raw destination.
func (self *ClientStrategy) resolveControlUDPAddr(ctx context.Context, address string) (*net.UDPAddr, error) {
	if self != nil && self.internalDohResolver != nil {
		host, _, err := net.SplitHostPort(address)
		if err == nil && self.internalDohResolver.matches(host) {
			return self.internalDohResolver.resolveUDPAddr(ctx, address)
		}
	}
	if self != nil && self.settings != nil && self.settings.ConnectSettings.Resolver != nil {
		return resolveUDPAddrWithResolver(ctx, address, self.settings.ConnectSettings.Resolver, false)
	}
	return resolveEgressUDPAddr(ctx, address)
}
