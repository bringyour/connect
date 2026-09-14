package connect

// Static endpoint-reputation policy (the "CFAA" layer): blocked-destination IPs
// plus a fixed port/protocol policy. It is the cheap, stateless front half of
// the egress security pipeline — evaluated before the stateful payload detector
// in ip_security_dmca.go — and is mirrored on the ingress source endpoint.
//
// inspect returns a three-way verdict so the caller can distinguish "definitely
// drop" and "definitely allow, do not inspect further" from "no opinion, hand
// to deep-packet inspection":
//
//   - cfaaDrop  -> blocked IP, or a known-abused / privileged-non-whitelisted
//                  port. The packet is dropped outright.
//   - cfaaAllow -> a structured system protocol or narrow endpoint exception
//                  that must never be entropy-dropped by the payload detector
//                  (NTP, IKE / wifi calling, plain DNS, Telegram reflectors).
//                  Allowed without further checks.
//   - cfaaPass  -> no static verdict; the caller proceeds to DPI (egress) or
//                  simply allows (ingress).
//
// The blocked-IP ranges (cfaaBlockedPrefixData) are generated; see
// ip_security_cfaa_block.go and security/main.go. cfaaBlockedIp4/cfaaBlockedIp6,
// below, binary-search the packed tables via the shared zero-allocation
// searches in ip_util.go.

import (
	"encoding/binary"
	"net"
)

type cfaaVerdict int

const (
	// cfaaPass means the static rules reached no verdict; the caller decides
	// (egress hands the packet to the stateful detector, ingress allows it).
	cfaaPass cfaaVerdict = iota
	// cfaaAllow is a definitive allow that must skip payload inspection.
	cfaaAllow
	// cfaaDrop is a definitive drop.
	cfaaDrop
)

// CfaaSecurityPolicySettings configures the static endpoint-reputation policy.
// Use DefaultCfaaSecurityPolicySettings for reasonable defaults.
type CfaaSecurityPolicySettings struct {
	// Enabled turns the static policy on. When false, inspect always returns
	// cfaaPass and neither the IP blocklist nor the port policy is applied.
	Enabled bool

	// AllowTelegramCalls enables the narrowly scoped Telegram reflector
	// exception in ip_security_telegram.go. It applies only to Telegram's
	// published IPv4 reflector endpoints on TCP or UDP ports 596-599 plus its
	// exact protocol-v12 TCP fallback on port 595. It never overrides the IP
	// blocklist.
	AllowTelegramCalls bool
}

func DefaultCfaaSecurityPolicySettings() *CfaaSecurityPolicySettings {
	return &CfaaSecurityPolicySettings{
		Enabled:            true,
		AllowTelegramCalls: true,
	}
}

type cfaaDetector struct {
	settings *CfaaSecurityPolicySettings
}

func newCfaaDetector(settings *CfaaSecurityPolicySettings) *cfaaDetector {
	return &cfaaDetector{
		settings: settings,
	}
}

// inspect applies the static rules to a single endpoint (destination on egress,
// source on ingress). version is the IP version (4 or 6); the blocklist is
// checked in its corresponding address family.
//
// Port policy (see ip_security_cfaa_test.go for the exhaustive table):
//   - bittorrent / abused ports                     -> drop
//   - ntp (123), ike+nat-t (500, 4500), dns/udp     -> allow (never inspected)
//   - Telegram reflector IPv4s on TCP/UDP 596-599,
//     plus its exact TCP/595 v12 fallback           -> allow (explicit exception)
//   - https/quic (443), dot (853), email (465/587/993/995), http/tcp (80),
//     and user/ephemeral ports (>=1024)             -> pass (to DPI)
//   - every other privileged port (<1024),
//     dns/tcp and 80/udp                            -> drop
func (self *cfaaDetector) inspect(ip net.IP, port int, protocol IpProtocol, version int) cfaaVerdict {
	if !self.settings.Enabled {
		return cfaaPass
	}

	// Blocked-IP reputation takes precedence over the port policy.
	switch version {
	case 4:
		if ip4 := ip.To4(); ip4 != nil {
			if cfaaBlockedIp4(binary.BigEndian.Uint32(ip4)) {
				return cfaaDrop
			}
		}
	case 6:
		if ip16 := ip.To16(); ip16 != nil {
			if cfaaBlockedIp6([16]byte(ip16)) {
				return cfaaDrop
			}
		}
	}

	// icmp has no ports, so the port policy below does not apply; the
	// blocked-ip reputation check above still does. echo-only parsing
	// constrains what reaches here (see ICMP.md).
	if protocol == IpProtocolIcmp {
		return cfaaAllow
	}

	// Telegram's call reflectors use privileged ports 596-599, with one exact
	// TCP/595 fallback. Keep this as a named endpoint-and-port exception instead
	// of weakening the privileged-port rule for every host. The reputation check
	// above intentionally remains authoritative if an address is also blocked.
	if self.settings.AllowTelegramCalls && isTelegramCallReflector(ip, port, protocol, version) {
		return cfaaAllow
	}

	switch {
	case 6881 <= port && port <= 6889, port == 6969, port == 1337, port == 9337, port == 2710:
		// bittorrent and unofficial bittorrent-related ports
		return cfaaDrop
	case port == 123, port == 500, port == 4500:
		// apple system ports: ntp (123), wifi calling / ike+nat-t (500, 4500)
		// see https://support.apple.com/en-us/103229
		return cfaaAllow
	case port == 53:
		// plain dns over udp is allowed (and must not be entropy-dropped); dns
		// over tcp is not whitelisted here.
		// FIXME allow plain dns for now; TODO upgrade to doh inline.
		if protocol == IpProtocolUdp {
			return cfaaAllow
		}
		return cfaaDrop
	case port == 443, port == 853, port == 465, port == 993, port == 995,
		port == smtpStartTlsPort && protocol == IpProtocolTcp:
		// https/quic, dns over tls, and secure email -> downstream DPI
		// The main client egress paths invoke the SMTP guard before this policy,
		// restricting TCP/587 plaintext negotiation and requiring STARTTLS plus
		// a ClientHello before transaction or authentication commands.
		return cfaaPass
	case port == 80:
		// http over tcp is allowed through to DPI (some radio streaming relies on
		// it); 80/udp is not http.
		// FIXME allow http for now; TODO upgrade to https inline.
		if protocol == IpProtocolTcp {
			return cfaaPass
		}
		return cfaaDrop
	case port < 1024:
		// other privileged ports are not permitted
		return cfaaDrop
	default:
		// user / ephemeral ports: no static verdict, hand to DPI
		return cfaaPass
	}
}

// cfaaBlockedIp4 reports whether the IPv4 address ip — a network-order
// (big-endian) uint32, e.g. binary.BigEndian.Uint32(addr.To4()) — falls within
// any blocked range. It binary-searches the packed, sorted, pairwise-disjoint
// ranges in the generated cfaaBlockedPrefixData (8 bytes per record: big-endian
// uint32 lo, then hi) and performs no heap allocation: the table is a read-only
// string constant read in place.
func cfaaBlockedIp4(ip uint32) bool {
	return searchRange4(cfaaBlockedPrefixData, cfaaBlockedPrefixCount, ip)
}

// cfaaBlockedIp6 reports whether the IPv6 address ip falls within any blocked
// range. It decodes ip into its two big-endian uint64 halves and binary-searches
// the packed table (searchRange6, ip_util.go).
func cfaaBlockedIp6(ip [16]byte) bool {
	return searchRange6(
		cfaaBlockedPrefix6Data,
		cfaaBlockedPrefix6Count,
		uint64(ip[0])<<56|uint64(ip[1])<<48|uint64(ip[2])<<40|uint64(ip[3])<<32|
			uint64(ip[4])<<24|uint64(ip[5])<<16|uint64(ip[6])<<8|uint64(ip[7]),
		uint64(ip[8])<<56|uint64(ip[9])<<48|uint64(ip[10])<<40|uint64(ip[11])<<32|
			uint64(ip[12])<<24|uint64(ip[13])<<16|uint64(ip[14])<<8|uint64(ip[15]),
	)
}
