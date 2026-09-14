package connect

// Provider-scoped exceptions for legitimate encrypted gaming traffic.
//
// A port alone is not evidence of a game: many gaming platforms use dynamic
// ports, publish local-listener or port-forwarding guidance, or cover most of
// the UDP user-port space. A gaming exception therefore requires the
// intersection of a provider-owned destination prefix, transport, and a
// documented remote port.
//
// Steam sources, snapshot verified 2026-09-01:
//   - remote ports: https://help.steampowered.com/en/faqs/view/2EA8-4D75-DA21-31EB
//   - IPv4 prefixes: https://as32590.net/ipv4.txt
//   - IPv6 prefixes: https://as32590.net/ipv6.txt
//
// Steam Support states that its non-web service traffic uses Valve's AS32590
// network and explicitly distinguishes remote from local ports. The prefix
// lists are also published by Valve for firewall filters. This exception is
// evaluated by the DMCA state machine after positive BitTorrent signatures,
// so using Valve infrastructure cannot hide a recognized BitTorrent flow.

import (
	"net/netip"
)

// GamingSecurityPolicySettings controls provider-scoped gaming exceptions.
// Use DefaultGamingSecurityPolicySettings for reasonable defaults.
type GamingSecurityPolicySettings struct {
	// Enabled is the master switch for every gaming exception.
	Enabled bool

	// AllowSteam permits documented Steam remote ports only when the
	// destination is inside Valve's published AS32590 address space.
	AllowSteam bool
}

func DefaultGamingSecurityPolicySettings() *GamingSecurityPolicySettings {
	return &GamingSecurityPolicySettings{
		Enabled:    true,
		AllowSteam: true,
	}
}

// Exact, masked snapshots of Valve's published Steam firewall-filter lists.
// Keep these as prefixes rather than expanding them into individual addresses.
var steamValveNetworkPrefixes = [...]netip.Prefix{
	// IPv4 (https://as32590.net/ipv4.txt).
	netip.MustParsePrefix("45.121.184.0/22"),
	netip.MustParsePrefix("103.10.124.0/23"),
	netip.MustParsePrefix("103.28.54.0/23"),
	netip.MustParsePrefix("146.66.152.0/21"),
	netip.MustParsePrefix("155.133.224.0/19"),
	netip.MustParsePrefix("162.254.192.0/21"),
	netip.MustParsePrefix("185.25.180.0/22"),
	netip.MustParsePrefix("192.69.96.0/22"),
	netip.MustParsePrefix("205.196.6.0/24"),
	netip.MustParsePrefix("208.64.200.0/22"),
	netip.MustParsePrefix("208.78.164.0/22"),

	// IPv6 (https://as32590.net/ipv6.txt).
	netip.MustParsePrefix("2404:3fc0::/32"),
	netip.MustParsePrefix("2602:801:f000::/40"),
	netip.MustParsePrefix("2620:f9::/44"),
	netip.MustParsePrefix("2a01:bc80::/32"),
}

func isSanctionedGamingEndpoint(
	settings *GamingSecurityPolicySettings,
	ipPath *IpPath,
) bool {
	return settings != nil && settings.Enabled && settings.AllowSteam &&
		isSteamValveRemoteEndpoint(ipPath)
}

func isSteamValveRemoteEndpoint(ipPath *IpPath) bool {
	if ipPath == nil || !isSteamRemotePort(ipPath.Protocol, ipPath.DestinationPort) {
		return false
	}
	address, ok := netip.AddrFromSlice(ipPath.DestinationIp)
	if !ok {
		return false
	}
	address = address.Unmap()
	switch ipPath.Version {
	case 4:
		if !address.Is4() {
			return false
		}
	case 6:
		if !address.Is6() || address.Is4In6() {
			return false
		}
	default:
		return false
	}
	for _, prefix := range steamValveNetworkPrefixes {
		if prefix.Contains(address) {
			return true
		}
	}
	return false
}

// isSteamRemotePort is the union of ports Steam labels "remote". It excludes
// local Remote Play and dedicated/listen-server guidance except where a number
// independently appears in a documented remote range.
func isSteamRemotePort(protocol IpProtocol, port int) bool {
	switch protocol {
	case IpProtocolTcp:
		// Login/download HTTP(S), plus Steam service traffic.
		return port == 80 || port == 443 || 27015 <= port && port <= 27050
	case IpProtocolUdp:
		// Steamworks P2P/voice, Steam client, and game traffic. The smaller
		// published 27014-27030 and 27015-27050 ranges are contained in
		// the Steam client game-traffic range 27000-27250.
		return port == 3478 || port == 4379 || port == 4380 ||
			27000 <= port && port <= 27250
	default:
		return false
	}
}
