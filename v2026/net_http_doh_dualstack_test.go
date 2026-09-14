package connect

import (
	"net/http"
	"net/http/httptest"
	"net/netip"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// The shipped defaults carry both families: every operator's v6 endpoint sits
// beside its v4 one, in the known-pair table, and the cache consults both.
func TestDefaultDnsResolverSettingsCarryIpv6(t *testing.T) {
	rs := DefaultDnsResolverSettings()
	if len(rs.RemoteDohUrlsIpv6) != len(rs.RemoteDohUrlsIpv4) {
		t.Fatalf("remote doh v6 urls = %d, want one per v4 operator (%d)", len(rs.RemoteDohUrlsIpv6), len(rs.RemoteDohUrlsIpv4))
	}
	if len(rs.RemoteDnsIpv6) != len(rs.RemoteDnsIpv4) {
		t.Fatalf("remote dns v6 = %d, want one per v4 operator (%d)", len(rs.RemoteDnsIpv6), len(rs.RemoteDnsIpv4))
	}
	if len(rs.LocalDnsIpv6) != len(rs.LocalDnsIpv4) {
		t.Fatalf("local dns v6 = %d, want one per v4 operator (%d)", len(rs.LocalDnsIpv6), len(rs.LocalDnsIpv4))
	}
	for _, addr := range slices.Concat(rs.RemoteDnsIpv6, rs.LocalDnsIpv6) {
		if parsed, err := netip.ParseAddr(addr); err != nil || !parsed.Is6() {
			t.Fatalf("dns v6 entry %q is not a v6 literal", addr)
		}
	}
	siblings := dohServerSiblings(rs)
	for _, dohUrl := range rs.RemoteDohUrlsIpv4 {
		if _, ok := siblings[dohUrl]; !ok {
			t.Fatalf("default doh url %s has no v6 sibling in the known-pair table", dohUrl)
		}
	}
	if DefaultDohSettings().IpVersion != 0 {
		t.Fatalf("DefaultDohSettings().IpVersion = %d, want 0 (both families)", DefaultDohSettings().IpVersion)
	}
	if got := DefaultDohSettings().ResolverIp(); got != "ip" {
		t.Fatalf("ResolverIp() = %s, want ip", got)
	}
}

// With IpVersion 0 the defaults produce an operator-interleaved list. A
// leftover v6 entry from the shipped table drops out when its v4 partner is
// replaced, while a leftover entry the CALLER configured is always kept.
func TestDohUrlsForDualStackPairsOperators(t *testing.T) {
	rs := DefaultDnsResolverSettings()
	urls := dohUrlsFor(rs.RemoteDohUrlsIpv4, rs.RemoteDohUrlsIpv6, 0)
	if len(urls) != len(rs.RemoteDohUrlsIpv4)+len(rs.RemoteDohUrlsIpv6) {
		t.Fatalf("dual-stack urls = %v, want every default of both families", urls)
	}
	for i := 0; i+1 < len(urls); i += 2 {
		if !strings.Contains(urls[i], "https://[") && strings.Contains(urls[i+1], "https://[") {
			continue
		}
		t.Fatalf("urls[%d..%d] = %v, want v4 followed by its v6 sibling", i, i+1, urls[i:i+2])
	}

	// replacing the v4 list means "use my servers": the shipped v6 defaults are
	// known-table entries whose v4 partners are now absent, so they drop out
	custom := "http://127.0.0.1:1/dns-query"
	only := dohUrlsFor([]string{custom}, rs.RemoteDohUrlsIpv6, 0)
	if len(only) != 1 || only[0] != custom {
		t.Fatalf("a replaced v4 list resolved to %v, want only the custom server", only)
	}

	// a v6-heavy CUSTOM configuration keeps every surplus server: those can
	// only have come from the caller, and familySiblings can index-pair only
	// min(len4, len6) of them
	heavy := dohUrlsFor(
		[]string{custom},
		[]string{"http://[::1]:1/dns-query", "http://[::2]:2/dns-query", "http://[::3]:3/dns-query"},
		0,
	)
	wantHeavy := []string{
		custom,
		"http://[::1]:1/dns-query",
		"http://[::2]:2/dns-query",
		"http://[::3]:3/dns-query",
	}
	if !slices.Equal(heavy, wantHeavy) {
		t.Fatalf("v6-heavy list = %v, want %v (no caller server dropped)", heavy, wantHeavy)
	}

	// a custom pair outside the table pairs by index
	custom6 := "http://[::1]:1/dns-query"
	pair := dohUrlsFor([]string{custom}, []string{custom6}, 0)
	if !slices.Equal(pair, []string{custom, custom6}) {
		t.Fatalf("custom pair = %v, want [%s %s]", pair, custom, custom6)
	}

	// a pure v6 configuration is used as is
	if got := dohUrlsFor(nil, rs.RemoteDohUrlsIpv6, 0); !slices.Equal(got, rs.RemoteDohUrlsIpv6) {
		t.Fatalf("pure v6 = %v, want the v6 list", got)
	}
	// explicit versions keep their one list
	if got := dohUrlsFor(rs.RemoteDohUrlsIpv4, rs.RemoteDohUrlsIpv6, 4); !slices.Equal(got, rs.RemoteDohUrlsIpv4) {
		t.Fatalf("ipVersion 4 = %v, want the v4 list", got)
	}
	if got := dohUrlsFor(rs.RemoteDohUrlsIpv4, rs.RemoteDohUrlsIpv6, 6); !slices.Equal(got, rs.RemoteDohUrlsIpv6) {
		t.Fatalf("ipVersion 6 = %v, want the v6 list", got)
	}
}

func TestFamilySiblingsPairsKnownAndCustomEntries(t *testing.T) {
	siblings := familySiblings(
		[]string{"1.1.1.1", "custom4", "8.8.8.8"},
		[]string{"2620:fe::fe", "custom6", "2606:4700:4700::1111"},
		knownDnsServerPairs,
	)
	want := map[string]string{
		"1.1.1.1":              "2606:4700:4700::1111",
		"2606:4700:4700::1111": "1.1.1.1",
		"custom4":              "custom6",
		"custom6":              "custom4",
	}
	if len(siblings) != len(want) {
		t.Fatalf("siblings = %v, want %v", siblings, want)
	}
	for k, v := range want {
		if siblings[k] != v {
			t.Fatalf("siblings[%s] = %s, want %s", k, siblings[k], v)
		}
	}
	// 8.8.8.8's partner is absent and 2620:fe::fe's partner is absent: neither pairs
}

func TestDnsResolverAddrsDualStackInterleavesOperators(t *testing.T) {
	settings := DefaultDohSettings()
	addrs := dnsResolverAddrs(settings, true, "udp")
	rs := settings.DnsResolverSettings
	if len(addrs) != len(rs.RemoteDnsIpv4)+len(rs.RemoteDnsIpv6) {
		t.Fatalf("addrs = %v, want both families", addrs)
	}
	if addrs[0] != rs.RemoteDnsIpv4[0] || addrs[1] != knownSibling(rs.RemoteDnsIpv4[0]) {
		t.Fatalf("addrs[0..1] = %v, want the first operator's v4 then v6", addrs[:2])
	}
	if got := dnsResolverAddrs(settings, true, "udp4"); !slices.Equal(got, rs.RemoteDnsIpv4) {
		t.Fatalf("udp4 = %v, want the v4 list", got)
	}
	if got := dnsResolverAddrs(settings, true, "udp6"); !slices.Equal(got, rs.RemoteDnsIpv6) {
		t.Fatalf("udp6 = %v, want the v6 list", got)
	}
}

func knownSibling(entry string) string {
	for _, pair := range knownDnsServerPairs {
		if pair[0] == entry {
			return pair[1]
		}
	}
	return ""
}

func TestDnsLookupNetworkFollowsTheRecordType(t *testing.T) {
	if network, ok := dnsLookupNetwork("A", 0); !ok || network != "ip4" {
		t.Fatalf("A/0 = %s,%t want ip4,true", network, ok)
	}
	if network, ok := dnsLookupNetwork("AAAA", 0); !ok || network != "ip6" {
		t.Fatalf("AAAA/0 = %s,%t want ip6,true", network, ok)
	}
	if _, ok := dnsLookupNetwork("AAAA", 4); ok {
		t.Fatal("AAAA under IpVersion 4 must be skipped, not answered with v4 records")
	}
	if _, ok := dnsLookupNetwork("A", 6); ok {
		t.Fatal("A under IpVersion 6 must be skipped")
	}
	if _, ok := dnsLookupNetwork("HTTPS", 0); ok {
		t.Fatal("an opaque record type has no plain-dns address lookup")
	}
}

func TestDohLaunchWavesGroupSiblings(t *testing.T) {
	siblings := map[string]string{"a4": "a6", "a6": "a4", "b4": "b6", "b6": "b4"}
	waves := dohLaunchWaves([]string{"b6", "c4", "a4", "b4", "a6"}, siblings)
	want := [][]string{{"b6", "b4"}, {"c4"}, {"a4", "a6"}}
	if len(waves) != len(want) {
		t.Fatalf("waves = %v, want %v", waves, want)
	}
	for i := range want {
		if !slices.Equal(waves[i], want[i]) {
			t.Fatalf("waves[%d] = %v, want %v (all %v)", i, waves[i], want[i], waves)
		}
	}
	// a sibling cut by MaxServersPerQuery is simply absent
	waves = dohLaunchWaves([]string{"a4", "b4"}, siblings)
	if len(waves) != 2 || len(waves[0]) != 1 || len(waves[1]) != 1 {
		t.Fatalf("truncated waves = %v, want two singletons", waves)
	}
}

// An operator's two endpoints race inside one wave. With a stagger far
// longer than the request timeout only the first wave ever fires: a pair
// alone fires both of its endpoints at once, and a pair beside an unpaired
// server fires either the whole pair or the lone server, never a fraction of
// the pair and never all three.
func TestDohPairRacesInsideOneWave(t *testing.T) {
	newSlowServer := func(hits *atomic.Int32) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			// never answer inside the test: the launch pattern is what is measured
			select {
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
			}
			w.WriteHeader(http.StatusServiceUnavailable)
		}))
	}
	newCache := func(ipv4 []string, ipv6 []string) *DohCache {
		settings := DefaultDohSettings()
		settings.RequestTimeout = 500 * time.Millisecond
		settings.DohServerStagger = 10 * time.Second
		settings.DohServerWarmStagger = 0
		// keep the stagger in force so only one wave fires
		settings.DohServerRaceMaxInFlight = 0
		settings.DnsResolverSettings = &DnsResolverSettings{
			EnableRemoteDoh:   true,
			RemoteDohUrlsIpv4: ipv4,
			RemoteDohUrlsIpv6: ipv6,
		}
		return NewDohCache(settings)
	}

	t.Run("pair alone", func(t *testing.T) {
		var hits4, hits6 atomic.Int32
		server4 := newSlowServer(&hits4)
		defer server4.Close()
		server6 := newSlowServer(&hits6)
		defer server6.Close()
		// custom urls outside the known-pair table pair by index
		cache := newCache([]string{server4.URL}, []string{server6.URL})
		defer cache.Close()

		cache.Query(t.Context(), "A", "pair.example")
		if hits4.Load() != 1 || hits6.Load() != 1 {
			t.Fatalf("pair hits = v4:%d v6:%d, want both endpoints launched in the first wave", hits4.Load(), hits6.Load())
		}
	})

	t.Run("pair beside a lone server", func(t *testing.T) {
		var hits4, hits6, hitsLone atomic.Int32
		server4 := newSlowServer(&hits4)
		defer server4.Close()
		server6 := newSlowServer(&hits6)
		defer server6.Close()
		lone := newSlowServer(&hitsLone)
		defer lone.Close()
		// index pairing pairs server4 with server6; lone has no partner
		cache := newCache([]string{server4.URL, lone.URL}, []string{server6.URL})
		defer cache.Close()

		cache.Query(t.Context(), "A", "pair.example")
		pairHits := hits4.Load() + hits6.Load()
		switch {
		case pairHits == 2 && hitsLone.Load() == 0:
			// the pair's wave fired first, both endpoints at once
		case pairHits == 0 && hitsLone.Load() == 1:
			// the lone server's wave fired first; the pair waits for the stagger
		default:
			t.Fatalf("hits = v4:%d v6:%d lone:%d, want exactly one wave: the whole pair or the lone server", hits4.Load(), hits6.Load(), hitsLone.Load())
		}
	})
}

func TestRegionalDnsServersCarryIpv6(t *testing.T) {
	cn := RegionalDnsResolverSettings("cn")
	if cn == nil {
		t.Fatal("cn has no regional recommendation")
	}
	if len(cn.RemoteDnsIpv6) == 0 {
		t.Fatalf("cn regional settings carry no v6 servers: %v", cn)
	}
	for _, addr := range cn.RemoteDnsIpv6 {
		if parsed, err := netip.ParseAddr(addr); err != nil || !parsed.Is6() {
			t.Fatalf("cn v6 entry %q is not a v6 literal", addr)
		}
	}
	if !slices.Equal(RegionalDnsServerIpv6s("CN"), cn.RemoteDnsIpv6) {
		t.Fatalf("RegionalDnsServerIpv6s(CN) = %v, want %v", RegionalDnsServerIpv6s("CN"), cn.RemoteDnsIpv6)
	}
	// a region without any published v6 resolver keeps a v4-only recommendation
	ir := RegionalDnsResolverSettings("ir")
	if ir == nil || len(ir.RemoteDnsIpv4) == 0 || len(ir.RemoteDnsIpv6) != 0 {
		t.Fatalf("ir = %v, want v4 servers and no v6 servers", ir)
	}
	if RegionalDnsResolverSettings("zz") != nil {
		t.Fatal("unknown region must have no recommendation")
	}
	for _, server := range RegionalDnsServers() {
		if server.Ipv6 == "" {
			continue
		}
		if parsed, err := netip.ParseAddr(server.Ipv6); err != nil || !parsed.Is6() {
			t.Fatalf("%s %s v6 %q is not a v6 literal", server.CountryCode, server.Name, server.Ipv6)
		}
	}
}
