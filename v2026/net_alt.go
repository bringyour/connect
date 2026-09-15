package connect

import (
	"fmt"
	"net"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

// net_alt.go -- the alt service as a client reaches it (EXTENDER.md L1 to L4).
//
// Alt is a full connect node and an api server in one process on the proxy
// hosts, with no load balancer in front. It serves H3 on udp 443 and whodis on
// udp 4053 (and on 53 through the router), terminating quic with the real api
// and connect certificates and dispatching by sni.
//
// A client never presents an alt name as sni: it presents the api or connect
// name, which is what the certificates are for, so an alt url decides only
// where the packets go. That is why everything here is a host, not a url with
// a resource under it.
//
// One label rule derives the alt url from the platform (connect) url, and the
// env prefix and the family suffix ride through the swap untouched: `connect.x`
// gives `alt.x`, `g2-connect.x` gives `g2-alt.x`, and `connect-v4.x` gives
// `alt-v4.x`. A host with no `connect` label to replace -- an ip literal, a
// single label, a bare space host -- derives nothing, and the caller then runs
// without alt rather than dialing a guess.

// The dns carrier ports (L2). 53 needs privilege to bind and reaches a host
// through the router or the lb; 4053 is the unprivileged port alt, the connect
// dns listener and every extender bind. A client dials 53 first when it is
// offered, then 4053.
const (
	DefaultDnsPort    = 53
	DefaultWhodisPort = 4053
)

// The public h3 port of alt (L1), which is what the api's alt h3 dialer uses
// when the alt url names no port of its own.
const DefaultAltH3Port = 443

// The service label of a platform url, and what it becomes on alt.
const (
	platformServiceLabel = "connect"
	altServiceLabel      = "alt"
)

// AltUrlFromPlatformUrl derives the alt url of one space from its platform
// url by the label rule of L3. The result carries the platform url's scheme
// and explicit port and nothing below the host, since only the host is dialed.
// "" when there is no `connect` service label to replace.
func AltUrlFromPlatformUrl(platformUrl string) string {
	hostName, port := altUrlHostPort(platformUrl)
	if hostName == "" || net.ParseIP(hostName) != nil {
		return ""
	}
	label, domain, ok := strings.Cut(hostName, ".")
	if !ok || label == "" || !strings.Contains(domain, ".") {
		return ""
	}
	// the service label is `[<env>-]connect[-v4|-v6]`
	labelParts := strings.Split(label, "-")
	serviceIndex := slices.Index(labelParts, platformServiceLabel)
	if serviceIndex < 0 {
		return ""
	}
	labelParts[serviceIndex] = altServiceLabel
	altHostName := fmt.Sprintf("%s.%s", strings.Join(labelParts, "-"), domain)

	scheme := "https"
	if parsedUrl, err := url.Parse(strings.TrimSpace(platformUrl)); err == nil && parsedUrl.Scheme != "" {
		scheme = parsedUrl.Scheme
	}
	if 0 < port {
		return fmt.Sprintf("%s://%s", scheme, net.JoinHostPort(altHostName, strconv.Itoa(port)))
	}
	return fmt.Sprintf("%s://%s", scheme, altHostName)
}

// The host and explicit port of an alt url. The host is lowercase with no
// trailing dot and is "" when the url names none; the port is 0 when the url
// names none, which leaves each carrier's own default in force. A bare host
// with no scheme is accepted, since that is what a hand-written setting
// usually is.
func altUrlHostPort(altUrl string) (string, int) {
	altUrl = strings.TrimSpace(altUrl)
	if altUrl == "" {
		return "", 0
	}
	parsedUrl, err := url.Parse(altUrl)
	if err != nil {
		return "", 0
	}
	if parsedUrl.Host == "" && !strings.Contains(altUrl, "//") {
		parsedUrl, err = url.Parse("//" + altUrl)
		if err != nil {
			return "", 0
		}
	}
	hostName := strings.ToLower(strings.TrimSuffix(parsedUrl.Hostname(), "."))
	if hostName == "" {
		return "", 0
	}
	port := 0
	if portString := parsedUrl.Port(); portString != "" {
		if parsedPort, err := strconv.Atoi(portString); err == nil && 0 < parsedPort && parsedPort <= 65535 {
			port = parsedPort
		}
	}
	return hostName, port
}

// The dns carrier ports of an alt host in dial order (L2). An alt url that
// names a port pins that one port, which is what an in-process fixture wants;
// otherwise `dnsPort` (0 takes 53) is dialed first and 4053 second.
func altDnsPorts(altPort int, dnsPort int) []int {
	if 0 < altPort {
		return []int{altPort}
	}
	if dnsPort <= 0 {
		dnsPort = DefaultDnsPort
	}
	if dnsPort == DefaultWhodisPort {
		return []int{DefaultWhodisPort}
	}
	return []int{dnsPort, DefaultWhodisPort}
}

// Dedupes and orders dns ports ascending, which is the dial order of L2: 53
// before 4053 when both are offered. Non-positive ports are dropped.
func orderedDnsPorts(dnsPorts []int) []int {
	ordered := []int{}
	for _, dnsPort := range dnsPorts {
		if dnsPort <= 0 || 65535 < dnsPort {
			continue
		}
		if !slices.Contains(ordered, dnsPort) {
			ordered = append(ordered, dnsPort)
		}
	}
	slices.Sort(ordered)
	return ordered
}
