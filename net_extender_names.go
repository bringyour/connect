package connect

import (
	"fmt"
	"net"
	"net/url"
	"strings"
)

// net_extender_names.go — the names of one extender network, derived from an
// api url (EXTENDER.md F1, G4).
//
// A host that knows only an api url still needs the two names the extender
// network is keyed by: the space host, which the operator signs a record's
// network host with (B2) and which names the gossip topic (D1), and the
// extender dns name the bootstrap resolves (E3). Both follow one label rule,
// the same one the sdk derives a space's service names with: the api url's own
// service label carries the env prefix, so `api.x` yields `extender.x` and
// `g2-api.x` yields `g2-extender.x`, and everything below that label is the
// space host.
//
// connectctl's standalone extender and the sdk's url-only network space are
// the two callers, and they must agree: a record the one accepts and the other
// rejects would look like a directory that silently loses entries.
//
// A host with nothing to derive from -- an ip literal, a single label, or a
// bare space host with no service label -- yields no service name at all. The
// caller then runs without dns bootstrap rather than resolving a guess.

// The host of an api url, which every other name here is derived from. The
// result is lowercase and carries no trailing dot.
func ExtenderApiHostName(apiUrl string) (string, error) {
	parsedUrl, err := url.Parse(strings.TrimSpace(apiUrl))
	if err != nil {
		return "", err
	}
	hostName := strings.ToLower(strings.TrimSuffix(parsedUrl.Hostname(), "."))
	if hostName == "" {
		return "", fmt.Errorf("the api url names no host")
	}
	return hostName, nil
}

// The space host under an api host: everything below the service label, which
// is what the operator signs a record's network host with (B2). An ip literal,
// a single label, or a host that is already a bare space host is its own space
// host.
func ExtenderNetworkHostName(apiHostName string) string {
	if net.ParseIP(apiHostName) != nil {
		return apiHostName
	}
	_, domain, ok := strings.Cut(apiHostName, ".")
	if !ok || domain == "" || !strings.Contains(domain, ".") {
		return apiHostName
	}
	return domain
}

// The host name of another service under the same space as an api url, by the
// env prefix rule: `api.x` yields `<service>.x` and `g2-api.x` yields
// `g2-<service>.x`. "" when there is no service label to replace -- an ip
// literal, a single-label host, or a bare space host.
func ExtenderServiceHostName(apiUrl string, service string) string {
	apiHostName, err := ExtenderApiHostName(apiUrl)
	if err != nil || net.ParseIP(apiHostName) != nil {
		return ""
	}
	label, domain, ok := strings.Cut(apiHostName, ".")
	if !ok || label == "" || !strings.Contains(domain, ".") {
		return ""
	}
	serviceLabel := service
	if envName, _, ok := strings.Cut(label, "-"); ok && envName != "" {
		serviceLabel = fmt.Sprintf("%s-%s", envName, service)
	}
	return fmt.Sprintf("%s.%s", serviceLabel, domain)
}
