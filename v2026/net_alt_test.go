package connect

import (
	"slices"
	"testing"
)

// The alt url of a space, derived from its platform url by the one label rule
// the network space and the family transport group share (EXTENDER.md L3).
func TestAltUrlFromPlatformUrl(t *testing.T) {
	cases := []struct {
		platformUrl string
		altUrl      string
	}{
		{
			platformUrl: "wss://connect.example.com",
			altUrl:      "wss://alt.example.com",
		},
		{
			// the env prefix rides the service label, so it survives the swap
			platformUrl: "wss://g2-connect.example.com",
			altUrl:      "wss://g2-alt.example.com",
		},
		{
			// so does the family suffix
			platformUrl: "wss://connect-v4.example.com",
			altUrl:      "wss://alt-v4.example.com",
		},
		{
			platformUrl: "wss://connect-v6.example.com",
			altUrl:      "wss://alt-v6.example.com",
		},
		{
			platformUrl: "wss://g2-connect-v6.example.com",
			altUrl:      "wss://g2-alt-v6.example.com",
		},
		{
			// the host is written with a trailing dot, in mixed case, with a
			// path that names no part of the alt destination
			platformUrl: "https://Connect.Space.Example./ws?x=1",
			altUrl:      "https://alt.space.example",
		},
		{
			// an explicit port is the destination, so it is kept
			platformUrl: "https://connect.example.com:8443",
			altUrl:      "https://alt.example.com:8443",
		},
		{
			// an ip literal has no service label to replace
			platformUrl: "https://192.0.2.10:8080",
			altUrl:      "",
		},
		{
			platformUrl: "https://[2001:db8::1]:8080",
			altUrl:      "",
		},
		{
			// a bare space host, a single label and a name that is not the
			// connect service derive nothing
			platformUrl: "wss://example.com",
			altUrl:      "",
		},
		{
			platformUrl: "wss://localhost:8080",
			altUrl:      "",
		},
		{
			platformUrl: "wss://api.example.com",
			altUrl:      "",
		},
		{
			platformUrl: "",
			altUrl:      "",
		},
	}
	for _, c := range cases {
		if altUrl := AltUrlFromPlatformUrl(c.platformUrl); altUrl != c.altUrl {
			t.Errorf("AltUrlFromPlatformUrl(%q) = %q, expected %q", c.platformUrl, altUrl, c.altUrl)
		}
	}
}

// The host and port an alt url names, which is all of it that is ever dialed.
func TestAltUrlHostPort(t *testing.T) {
	cases := []struct {
		altUrl string
		host   string
		port   int
	}{
		{altUrl: "https://alt.example.com", host: "alt.example.com", port: 0},
		{altUrl: "https://alt.example.com:4053", host: "alt.example.com", port: 4053},
		{altUrl: "https://127.0.0.1:14443", host: "127.0.0.1", port: 14443},
		{altUrl: "https://[2001:db8::1]:443", host: "2001:db8::1", port: 443},
		// a hand written setting with no scheme still names a host
		{altUrl: "alt.example.com:4053", host: "alt.example.com", port: 4053},
		{altUrl: "  https://Alt.Example.Com./x  ", host: "alt.example.com", port: 0},
		{altUrl: "", host: "", port: 0},
	}
	for _, c := range cases {
		host, port := altUrlHostPort(c.altUrl)
		if host != c.host || port != c.port {
			t.Errorf("altUrlHostPort(%q) = %q/%d, expected %q/%d", c.altUrl, host, port, c.host, c.port)
		}
	}
}

// The dns carrier ports of an alt host: 53 before 4053, one port when the url
// pins one, and never a duplicate (L2).
func TestAltDnsPorts(t *testing.T) {
	cases := []struct {
		altPort  int
		dnsPort  int
		dnsPorts []int
	}{
		{altPort: 0, dnsPort: 0, dnsPorts: []int{53, 4053}},
		{altPort: 0, dnsPort: DefaultDnsPort, dnsPorts: []int{53, 4053}},
		{altPort: 0, dnsPort: DefaultWhodisPort, dnsPorts: []int{4053}},
		// a listener kept on the legacy port through the rolling migration
		{altPort: 0, dnsPort: 8053, dnsPorts: []int{8053, 4053}},
		{altPort: 15053, dnsPort: DefaultDnsPort, dnsPorts: []int{15053}},
	}
	for _, c := range cases {
		if dnsPorts := altDnsPorts(c.altPort, c.dnsPort); !slices.Equal(dnsPorts, c.dnsPorts) {
			t.Errorf("altDnsPorts(%d, %d) = %v, expected %v", c.altPort, c.dnsPort, dnsPorts, c.dnsPorts)
		}
	}
}

// Record and activation dns ports are deduped and ascending, which puts 53
// before 4053 whatever order they arrived in (L2).
func TestOrderedDnsPorts(t *testing.T) {
	cases := []struct {
		dnsPorts []int
		ordered  []int
	}{
		{dnsPorts: []int{4053, 53}, ordered: []int{53, 4053}},
		{dnsPorts: []int{53, 53, 4053}, ordered: []int{53, 4053}},
		{dnsPorts: []int{0, -1, 70000, 4053}, ordered: []int{4053}},
		{dnsPorts: nil, ordered: []int{}},
	}
	for _, c := range cases {
		if ordered := orderedDnsPorts(c.dnsPorts); !slices.Equal(ordered, c.ordered) {
			t.Errorf("orderedDnsPorts(%v) = %v, expected %v", c.dnsPorts, ordered, c.ordered)
		}
	}
}
