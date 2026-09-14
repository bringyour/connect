package connect

import "net"

// The embedded probe target table.
//
// Source: probe-list-v3.csv, reduced once at authoring time rather than parsed
// at runtime -- the client ships no csv, and a table the compiler can see is
// one fewer file to lose on a mobile build. Two of the csv's three classes are
// represented:
//
//   - health: ordinary reachable sites, dialed at :443. The endpoint column is
//     reduced to its hostname (scheme and path stripped, duplicates collapsed:
//     the http and https rows for one host, and the sized and unsized variants
//     of one speed endpoint, are the same host to a tcp probe).
//   - dns: public resolver ips, queried at :53.
//
// The reputation class -- Akamai, Reddit, Epic, Stack Overflow, Reuters, Etsy,
// Ecosia, Canva -- is deliberately absent, not merely unsampled. Those entries
// exist in the csv so a human can judge a provider against sites that matter
// for reputation, and automated probing of exactly those sites is the traffic
// pattern that gets an egress ip listed. Nothing in this package can reach
// them; there is no runtime filter to misconfigure and no setting that turns
// them back on.
//
// A few health hostnames are literal ip addresses (1.1.1.1). They need no
// resolution and a caller may pass them straight through; see
// sampleProbeTargets for why resolution is the caller's job at all. A pass
// over v6 swaps each v4 literal for the same operator's v6 endpoint
// (probeLiteralHostIp), so the host sample itself is family-independent and
// a provider walks the same rotation whichever family a pass asks over.

// probeHostNames are the health-class hostnames, dialed at :443.
var probeHostNames = []string{
	"www.google.com",
	"connectivitycheck.gstatic.com",
	"www.gstatic.com",
	"captive.apple.com",
	"connectivity-check.ubuntu.com",
	"detectportal.firefox.com",
	"nmcheck.gnome.org",
	"cp.cloudflare.com",
	// the literal-ip entries. These need no resolution, which makes them the
	// resolver-down fallback (see probeFallbackLiteralTargets): a pass whose
	// sampled resolver is silent still gets three independent, always-https
	// dial questions (1.1.1.1, dns.google, quad9 all terminate tls on :443),
	// enough that the 60% bar remains decidable without dns.
	"1.1.1.1",
	"8.8.8.8",
	"9.9.9.9",
	"one.one.one.one",
	"speed.cloudflare.com",
	"checkip.amazonaws.com",
	"ifconfig.me",
	"icanhazip.com",
	"api.ipify.org",
	"ipinfo.io",
	"example.com",
	"cdnjs.cloudflare.com",
	"www.fastly.net",
	"fastly.jsdelivr.net",
	"unpkg.com",
	"ajax.googleapis.com",
	"ajax.aspnetcdn.com",
	"d1.awsstatic.com",
	"www.keycdn.com",
	"www.cdn77.com",
	"www.sucuri.net",
	"cachefly.cachefly.net",
	"proof.ovh.net",
	"speedtest.tele2.net",
	"mirrors.kernel.org",
	"deb.debian.org",
	"archive.ubuntu.com",
	"dl-cdn.alpinelinux.org",
	"dl.fedoraproject.org",
	"www.bing.com",
	"www.yahoo.com",
	"duckduckgo.com",
	"www.baidu.com",
	"yandex.com",
	"www.facebook.com",
	"www.instagram.com",
	"x.com",
	"www.linkedin.com",
	"www.tiktok.com",
	"www.pinterest.com",
	"www.snapchat.com",
	"discord.com",
	"telegram.org",
	"www.whatsapp.com",
	"mastodon.social",
	"bsky.app",
	"www.youtube.com",
	"www.netflix.com",
	"www.twitch.tv",
	"vimeo.com",
	"www.hulu.com",
	"www.disneyplus.com",
	"open.spotify.com",
	"soundcloud.com",
	"www.dailymotion.com",
	"www.amazon.com",
	"www.ebay.com",
	"www.walmart.com",
	"www.alibaba.com",
	"www.aliexpress.com",
	"www.target.com",
	"www.shopify.com",
	"www.microsoft.com",
	"www.apple.com",
	"github.com",
	"api.github.com",
	"gitlab.com",
	"www.cloudflare.com",
	"aws.amazon.com",
	"cloud.google.com",
	"www.digitalocean.com",
	"hub.docker.com",
	"registry.npmjs.org",
	"pypi.org",
	"rubygems.org",
	"www.python.org",
	"go.dev",
	"www.kernel.org",
	"developer.mozilla.org",
	"www.cnn.com",
	"www.bbc.com",
	"www.nytimes.com",
	"www.theguardian.com",
	"apnews.com",
	"www.aljazeera.com",
	"www.dw.com",
	"www.france24.com",
	"www.wikipedia.org",
	"wordpress.com",
	"www.imdb.com",
	"archive.org",
	"www.openstreetmap.org",
	"www.weather.gov",
	"www.qq.com",
	"www.sina.com.cn",
	"www.taobao.com",
	"www.jd.com",
	"mail.ru",
	"vk.com",
	"www.naver.com",
	"line.me",
	"timesofindia.indiatimes.com",
	"www.globo.com",
	"www.mercadolibre.com",
	"www.abc.net.au",
	"www.news24.com",
	"zoom.us",
	"slack.com",
	"www.dropbox.com",
	"www.notion.so",
	"trello.com",
	"www.atlassian.com",
	"www.figma.com",
	"store.steampowered.com",
	"www.playstation.com",
	"www.xbox.com",
	"www.nintendo.com",
	"www.roblox.com",
	"www.riotgames.com",
}

// probeResolverIps are the v4 dns-class resolver ips, queried at :53. The v6
// list below holds the same operators' v6 endpoints; sampleProbeTargetsForIpFamily
// picks from whichever the exit can carry. The sampler hands back a string
// and the crafting side is the only place the family matters.
var probeResolverIps = []string{
	"8.8.8.8",
	"8.8.4.4",
	"1.1.1.1",
	"1.0.0.1",
	"9.9.9.9",
	"149.112.112.112",
	"208.67.222.222",
	"208.67.220.220",
	"94.140.14.14",
	"94.140.15.15",
	"76.76.2.0",
	"185.228.168.9",
	"8.26.56.26",
	"205.171.3.66",
	"64.6.64.6",
	"77.88.8.8",
	"156.154.70.5",
	"74.82.42.42",
	"149.112.121.10",
	"223.5.5.5",
	"119.29.29.29",
	"114.114.114.114",
	"185.222.222.222",
}

// probeHostLiteralIpv6s maps the table's v4 literal health hosts to the same
// operators' v6 endpoints, every one of which terminates tls on :443 like its
// v4 form, so a v6 pass keeps the resolver-down fallback (see
// probeFallbackLiteralTargetsForIpVersion) with the same three independent
// dial questions.
var probeHostLiteralIpv6s = map[string]string{
	"1.1.1.1": "2606:4700:4700::1111",
	"8.8.8.8": "2001:4860:4860::8888",
	"9.9.9.9": "2620:fe::fe",
}

// probeLiteralHostIp is the ip a sampled host stands for in a pass over
// ipVersion: a v4 literal itself over v4, its v6 sibling over v6, and a v6
// literal itself over v6. A hostname returns (nil, false) and is resolved by
// the caller; a literal of the other family with no sibling also returns
// (nil, false) and the caller skips it, since an ip cannot be resolved into
// the other family.
func probeLiteralHostIp(host string, ipVersion int) (net.IP, bool) {
	ip := net.ParseIP(host)
	if ip == nil {
		return nil, false
	}
	switch ipVersion {
	case 4:
		if ip4 := ip.To4(); ip4 != nil {
			return ip4, true
		}
	case 6:
		if ip.To4() == nil {
			return ip, true
		}
		if sibling, ok := probeHostLiteralIpv6s[host]; ok {
			if siblingIp := net.ParseIP(sibling); siblingIp != nil {
				return siblingIp, true
			}
		}
	}
	return nil, false
}

// probeResolverIpv6s are the v6 dns-class resolver ips (Cloudflare, Google,
// Quad9, OpenDNS, AdGuard, Control D, CleanBrowsing, Yandex, DNS.SB), for
// exits that can carry v6.
var probeResolverIpv6s = []string{
	"2606:4700:4700::1111",
	"2606:4700:4700::1001",
	"2001:4860:4860::8888",
	"2001:4860:4860::8844",
	"2620:fe::fe",
	"2620:fe::9",
	"2620:119:35::35",
	"2620:119:53::53",
	"2a10:50c0::ad1:ff",
	"2a10:50c0::ad2:ff",
	"2606:1a40::",
	"2a0d:2a00:1::",
	"2a02:6b8::feed:0ff",
	"2a09::",
}

// The pass width (how many health hosts one probe pass uses, alongside one
// resolver) is the ProbeSampleHostCount setting, defaulting to the ENTIRE
// table -- see probeSampleWidth. At a few hundred bytes per target a
// full-table pass is a few tens of kilobytes, and the verdict is
// fraction-based, so width changes coverage without moving the bar.

// probePassFraction is the share of a pass's targets that must answer for the
// provider to qualify. Deliberately below 1: probes go out from provider egress
// ips that anti-bot infrastructure drops as a matter of policy, so demanding
// every target answer would fail good providers on the list's most-defended
// entries. Deliberately above 1/2: a provider that answers only a minority is
// not demonstrating that it dials the internet, it is demonstrating that a
// couple of unusually permissive destinations exist.
const probePassFraction = 0.6

// sampleProbeTargets returns one pass's worth of targets for a provider: n
// health hostnames and one resolver ip, chosen deterministically from seed.
//
// Rotation, not sampling. Consecutive seeds return DISJOINT blocks of hosts
// (block seed covers indices [seed*n, seed*n+n) modulo the table), so a
// provider re-probed over a session walks the whole list in
// ceil(len(hosts)/n) passes instead of re-testing whichever four sites a hash
// happened to like. That matters because the failure this qualification exists
// to catch -- a provider whose upstream reaches some of the internet and not
// the rest -- is invisible to a probe that always asks the same four questions.
//
// Determinism is a requirement, not an implementation detail: the caller seeds
// from the provider identity plus the pass number, so two clients probing the
// same provider agree on what was tested, and a field report naming a failing
// target can be reproduced exactly.
//
// Hostnames come back unresolved on purpose. Resolution must happen OUTSIDE the
// channel being probed (the tunnel's doh cache does it), or a provider with
// broken dns fails a tcp probe that was never about dns -- and the probe
// mechanism itself must not be able to introduce that confusion, so it is not
// given the ability to resolve anything.
func sampleProbeTargets(seed uint64, n int) (hosts []string, resolver string) {
	return sampleProbeTargetsForIpFamily(seed, n, IpFamilyV4Only)
}

// probeIpVersionForFamily is the family one pass asks its questions over,
// given the exit's address-family category and the pass seed: v4-only and
// legacy exits are asked over v4, v6-only exits over v6, and a dualstack exit
// alternates by seed (the caller seeds from the pass index) so a re-probed
// provider proves both families over consecutive passes. Anything the
// category cannot carry is never asked, so a v6-only exit is never read as
// unqualifiable for failing v4 questions it could not carry.
func probeIpVersionForFamily(seed uint64, ipFamily IpFamily) int {
	switch ipFamily.Normalize() {
	case IpFamilyV6Only:
		return 6
	case IpFamilyDualstack:
		if seed%2 == 1 {
			return 6
		}
		return 4
	default:
		return 4
	}
}

// sampleProbeTargetsForIpFamily is sampleProbeTargets with the resolver drawn
// from the list the exit's address-family category can carry, per
// probeIpVersionForFamily.
func sampleProbeTargetsForIpFamily(seed uint64, n int, ipFamily IpFamily) (hosts []string, resolver string) {
	return sampleProbeTargetsForIpVersion(seed, n, probeIpVersionForFamily(seed, ipFamily))
}

// sampleProbeTargetsForIpVersion is sampleProbeTargets with the resolver drawn
// from the given family's list. The host sample is the same for both
// families; a v4 literal host is swapped for its v6 sibling at target-build
// time (probeLiteralHostIp), not here.
func sampleProbeTargetsForIpVersion(seed uint64, n int, ipVersion int) (hosts []string, resolver string) {
	resolverIps := probeResolverIps
	if ipVersion == 6 {
		resolverIps = probeResolverIpv6s
	}
	if 0 < len(resolverIps) {
		resolver = resolverIps[seed%uint64(len(resolverIps))]
	}
	if n <= 0 || len(probeHostNames) == 0 {
		return nil, resolver
	}
	// a pass can never ask for more than the table holds, and asking for the
	// whole table is a legitimate (if heavy) request rather than an error
	if len(probeHostNames) < n {
		n = len(probeHostNames)
	}
	total := uint64(len(probeHostNames))
	// the block start. the multiply is deliberate (see the rotation note): a
	// stride of 1 would overlap consecutive passes by n-1 hosts.
	start := (seed * uint64(n)) % total
	hosts = make([]string, 0, n)
	for i := uint64(0); i < uint64(n); i += 1 {
		hosts = append(hosts, probeHostNames[(start+i)%total])
	}
	return hosts, resolver
}
