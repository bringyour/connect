//go:generate go run .

// Command blocker aggregates public ad/tracking block lists into the packed
// default data sets for the connect Blocker (ip_blocker.go) and writes them to
// ip_blocker_block.go:
//
//   - hostnames as peppered truncated hashes: the first 6 bytes, big endian,
//     of SHA256(pepper || name), sorted and unique, where name is the
//     normalized (lower case, punycode) entry. one entry blocks the name and
//     all its subdomains (the runtime walks label suffixes), so the generator
//     drops entries covered by a parent entry.
//   - ip subnets in the same packed range format as the security tables
//     (ip_security_cfaa_block.go), populated only from literal ip entries
//     appearing natively in the host lists — hostnames are never resolved
//     (shared cdn ips would cause false positives).
//
// The pepper is regenerated from crypto/rand on every run, after the feeds are
// fetched. that defeats crafting upstream list entries that collide with a
// legitimate hostname in the truncated hash space, and rotates any accidental
// collision. it is not secrecy: hashes keep the plain text list out of the
// source and binary, but membership is testable by anyone holding the binary.
//
// Feed selection per sdk/BLOCKER.md: only permissive, clean-provenance,
// actively maintained lists (Unlicense/MIT; no GPL, no CC BY-NC/SA, no
// contaminated aggregates). licenses verified 2026-07-11.
//
// Safety rails, mirroring security/main.go: per-feed minimum counts abort the
// build when a feed goes dark or reformats; public-suffix entries are rejected
// (a list entry like "com" or "cloudfront.net" would block entire platforms
// under suffix semantics); totals are bounded both ways; source cidrs broader
// than /8 (v4) or /16 (v6) are rejected as poison; reserved address space and
// the default resolver endpoints are subtracted.
//
// Usage:
//
//	go generate ./blocker              # via the directive above
//	cd blocker && go run .             # equivalent
//	go run ./blocker -tier conservative -out /path.go
package main

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"io"
	"net/http"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/net/publicsuffix"

	"github.com/urnetwork/connect/v2026"
)

const userAgent = "urnetwork-connect-blocklist-generator/1.0 (+https://ur.network)"

const maxHostLen = 253

// hostRecordLen is the byte width of one packed hostname hash record: the
// leading hostRecordLen bytes, big endian, of SHA256(pepper || name). it must
// match connect.blockerHostRecordLen. narrower records trade a (vanishing)
// truncated-hash collision probability for embedded size: at 6 bytes and
// ~160k entries a random hostname collides with p ≈ 160e3/2^48 ≈ 6e-10 per
// probe, and the pepper is regenerated every build so any collision is
// transient rather than a permanently mis-blocked domain.
const hostRecordLen = 6

// poison guards for source cidrs, per security/main.go
const minPrefixBits = 8
const minPrefix6Bits = 16

type feedFormat int

const (
	// formatHostsOrDomains tolerantly parses hosts files ("0.0.0.0 name ..."),
	// plain domain lists (one name per line, optional "*." prefix), and bare
	// ip / cidr lines (routed to the ip tables). # and ; start comments.
	formatHostsOrDomains feedFormat = iota
	// formatAdblock parses exactly the "||name^" rule form; every other rule
	// kind (exceptions, cosmetics, paths, wildcards, options) is skipped.
	formatAdblock
)

type feed struct {
	name   string
	url    string
	format feedFormat
	// hard-fail the build if fewer valid host entries than this
	minCount int
	// part of the conservative tier (the standard tier uses every feed)
	conservative bool
	// subtraction feed: entries are removed from the blocked set
	allow  bool
	credit string
}

// feeds is the full standard-tier composition. licenses verified 2026-07-11;
// see sdk/BLOCKER.md for the research. floors sit well under observed volume
// so normal churn never trips them, but a dead or reformatted feed does.
var feeds = []feed{
	{name: "shadowwhisperer_ads", url: "https://raw.githubusercontent.com/ShadowWhisperer/BlockLists/master/Lists/Ads", format: formatHostsOrDomains, minCount: 15000, conservative: true, credit: "ShadowWhisperer BlockLists, Ads (Unlicense) — github.com/ShadowWhisperer/BlockLists"},
	{name: "shadowwhisperer_wild_ads", url: "https://raw.githubusercontent.com/ShadowWhisperer/BlockLists/master/Lists/Wild_Ads", format: formatHostsOrDomains, minCount: 8000, conservative: true, credit: "ShadowWhisperer BlockLists, Wild_Ads (Unlicense) — github.com/ShadowWhisperer/BlockLists"},
	{name: "shadowwhisperer_tracking", url: "https://raw.githubusercontent.com/ShadowWhisperer/BlockLists/master/Lists/Tracking", format: formatHostsOrDomains, minCount: 80000, credit: "ShadowWhisperer BlockLists, Tracking (Unlicense) — github.com/ShadowWhisperer/BlockLists"},
	{name: "shadowwhisperer_wild_tracking", url: "https://raw.githubusercontent.com/ShadowWhisperer/BlockLists/master/Lists/Wild_Tracking", format: formatHostsOrDomains, minCount: 8000, credit: "ShadowWhisperer BlockLists, Wild_Tracking (Unlicense) — github.com/ShadowWhisperer/BlockLists"},
	{name: "anudeepnd_adservers", url: "https://raw.githubusercontent.com/anudeepND/blacklist/master/adservers.txt", format: formatHostsOrDomains, minCount: 25000, conservative: true, credit: "anudeepND blacklist, adservers (MIT, Copyright (c) 2018 Anudeep ND) — github.com/anudeepND/blacklist"},
	{name: "frogeye_firstparty", url: "https://hostfiles.frogeye.fr/firstparty-trackers.txt", format: formatHostsOrDomains, minCount: 8000, conservative: true, credit: "Geoffrey Frogeye first-party trackers (MIT) — hostfiles.frogeye.fr"},
	{name: "frogeye_multiparty", url: "https://hostfiles.frogeye.fr/multiparty-trackers.txt", format: formatHostsOrDomains, minCount: 10000, credit: "Geoffrey Frogeye multiparty trackers (MIT) — hostfiles.frogeye.fr"},
	{name: "hostsvn", url: "https://raw.githubusercontent.com/bigdargon/hostsVN/master/hosts", format: formatHostsOrDomains, minCount: 9000, credit: "hostsVN (MIT, Copyright (c) BigDargon) — github.com/bigdargon/hostsVN"},

	{name: "anudeepnd_whitelist", url: "https://raw.githubusercontent.com/anudeepND/whitelist/master/domains/whitelist.txt", format: formatHostsOrDomains, minCount: 100, conservative: true, allow: true, credit: "anudeepND whitelist (MIT, Copyright (c) 2018 Anudeep ND) — github.com/anudeepND/whitelist [subtracted]"},
}

// localAllowFile is the committed in-house allowlist, subtracted like an
// allow feed. path relative to the connect module root.
const localAllowFile = "blocker/allowlist.txt"

// reservedCIDRs / reservedCIDRs6: non-global special-purpose space subtracted
// from the ip tables (rfc 6890 et al.), same rationale as security/main.go.
var reservedCIDRs = []string{
	"0.0.0.0/8",
	"10.0.0.0/8",
	"100.64.0.0/10",
	"127.0.0.0/8",
	"169.254.0.0/16",
	"172.16.0.0/12",
	"192.0.0.0/24",
	"192.0.2.0/24",
	"192.88.99.0/24",
	"192.168.0.0/16",
	"198.18.0.0/15",
	"198.51.100.0/24",
	"203.0.113.0/24",
	"224.0.0.0/4",
	"240.0.0.0/4",
}

var reservedCIDRs6 = []string{
	"::/8",
	"64:ff9b::/96",
	"100::/64",
	"2001:db8::/32",
	"2002::/16",
	"fc00::/7",
	"fe80::/10",
	"ff00::/8",
}

type iprange struct{ lo, hi uint32 }

type ip6range struct{ lo, hi netip.Addr }

const (
	feedDispositionAcceptedBlock   = "accepted_block"
	feedDispositionAcceptedAllow   = "accepted_allow"
	feedDispositionSkippedMinCount = "skipped_below_min_count"
	feedDispositionUnavailable     = "unavailable"
)

// parsed is one feed's parse result.
type parsed struct {
	f                feed
	hosts            []string
	ranges           []iprange
	ranges6          []ip6range
	invalid          int
	skipped          int
	contentSHA256    [32]byte
	contentBytes     int
	contentAvailable bool
	disposition      string
	err              error
}

func main() {
	out := flag.String("out", "", "output .go path (default: <connect module root>/ip_blocker_block.go)")
	timeout := flag.Duration("timeout", 90*time.Second, "per-feed HTTP timeout")
	tier := flag.String("tier", "standard", "feed tier: standard | conservative")
	allowStale := flag.Bool("allow-stale", false, "generate even if some feeds fail or fall below min-count (skips them)")
	pepperHex := flag.String("pepper-hex", "", "fixed pepper as 64 hex chars (testing/reproduction only; default: fresh random)")
	maxHosts := flag.Int("max-hosts", 1_500_000, "fail if more unique host entries than this (poison guard)")
	minHosts := flag.Int("min-hosts", 0, "fail if fewer unique host entries than this (0 = auto by tier)")
	maxCoverage := flag.Uint64("max-coverage", 1_000_000, "fail if total blocked ipv4 address count exceeds this (poison guard)")
	maxBytes := flag.Int("max-bytes", 1<<20, "size budget: fail if the EMBEDDED tables exceed this many bytes (the packed rodata that ships in the sdk binary, not the .go source, which is ~4x larger from \\xNN escapes)")
	force := flag.Bool("force", false, "overwrite the output even if it lacks a generated-file marker")
	flag.Parse()

	var tierConservative bool
	switch *tier {
	case "standard":
	case "conservative":
		tierConservative = true
	default:
		fatal(fmt.Errorf("unknown tier %q (standard | conservative)", *tier))
	}
	if *minHosts == 0 {
		if tierConservative {
			*minHosts = 60_000
		} else {
			*minHosts = 120_000
		}
	}

	moduleRoot, outPath, err := resolveOut(*out)
	if err != nil {
		fatal(err)
	}
	if err := guardOutput(outPath, *force); err != nil {
		fatal(err)
	}

	pepper, err := makePepper(*pepperHex)
	if err != nil {
		fatal(err)
	}

	tierFeeds := []feed{}
	for _, f := range feeds {
		if tierConservative && !f.conservative {
			continue
		}
		tierFeeds = append(tierFeeds, f)
	}

	results := fetchAll(tierFeeds, *timeout)

	var problems []string
	blockNames := map[string]bool{}
	allowNames := map[string]bool{}
	var all []iprange
	var all6 []ip6range
	pslRejected := 0
	fmt.Fprintln(os.Stderr, "feed report:")
	for i := range results {
		r := &results[i]
		status := "ok"
		if r.err != nil {
			status = "ERROR: " + r.err.Error()
		}
		kind := "block"
		if r.f.allow {
			kind = "allow"
		}
		fmt.Fprintf(os.Stderr, "  %-30s %-5s hosts=%-7d v4=%-5d v6=%-4d invalid=%-5d skipped=%-5d %s\n",
			r.f.name, kind, len(r.hosts), len(r.ranges), len(r.ranges6), r.invalid, r.skipped, status)

		if r.err != nil {
			r.disposition = feedDispositionUnavailable
			problems = append(problems, fmt.Sprintf("%s: fetch error: %v", r.f.name, r.err))
			continue
		}
		if len(r.hosts) < r.f.minCount {
			r.disposition = feedDispositionSkippedMinCount
			problems = append(problems, fmt.Sprintf("%s: only %d valid host entries (min %d) — feed may be broken or reformatted",
				r.f.name, len(r.hosts), r.f.minCount))
			continue
		}
		if r.f.allow {
			r.disposition = feedDispositionAcceptedAllow
			for _, name := range r.hosts {
				allowNames[name] = true
			}
			// allow feeds contribute no ip entries
			continue
		}
		r.disposition = feedDispositionAcceptedBlock
		for _, name := range r.hosts {
			if isPublicSuffixEntry(name) {
				pslRejected += 1
				continue
			}
			blockNames[name] = true
		}
		all = append(all, r.ranges...)
		all6 = append(all6, r.ranges6...)
	}

	if len(problems) > 0 && !*allowStale {
		fmt.Fprintln(os.Stderr, "\nABORT: feed sanity checks failed (pass -allow-stale to skip the offending feeds):")
		for _, p := range problems {
			fmt.Fprintln(os.Stderr, "  - "+p)
		}
		os.Exit(1)
	}

	// the committed in-house allowlist
	localAllow, err := os.ReadFile(filepath.Join(moduleRoot, localAllowFile))
	if err != nil {
		fatal(fmt.Errorf("read %s: %w", localAllowFile, err))
	}
	localParsed := parseFeed(formatHostsOrDomains, localAllow)
	for _, name := range localParsed.hosts {
		allowNames[name] = true
	}

	// the default resolver endpoints: hostnames join the allow set, ips are
	// subtracted from the ip tables
	resolverNames, resolverAddrs := resolverExclusions()
	for name := range resolverNames {
		allowNames[name] = true
	}

	subtracted := subtractAllow(blockNames, allowNames)

	compressed := compressSuffixes(blockNames)

	fmt.Fprintf(os.Stderr, "\nhosts: %d unique (+%d psl-rejected, -%d allowlisted, -%d covered by a parent entry)\n",
		len(blockNames), pslRejected, subtracted, compressed)

	if len(blockNames) < *minHosts {
		fatal(fmt.Errorf("only %d unique host entries (min %d) — too few; aborting", len(blockNames), *minHosts))
	}
	if *maxHosts < len(blockNames) {
		fatal(fmt.Errorf("%d unique host entries exceeds max %d — possible poisoned feed; aborting", len(blockNames), *maxHosts))
	}

	keys := hashHosts(pepper, blockNames)

	merged := mergeRanges(all)
	reserved := mergeRanges(parseReserved())
	for _, addr := range resolverAddrs {
		if addr.Is4() {
			u := addrU32(addr)
			reserved = mergeRanges(append(reserved, iprange{lo: u, hi: u}))
		}
	}
	final := subtractAll(merged, reserved)

	merged6 := mergeRanges6(all6)
	reserved6 := mergeRanges6(parseReserved6())
	for _, addr := range resolverAddrs {
		if addr.Is6() && !addr.Is4In6() {
			reserved6 = mergeRanges6(append(reserved6, ip6range{lo: addr, hi: addr}))
		}
	}
	final6 := subtractAll6(merged6, reserved6)

	var coverage uint64
	for _, r := range final {
		coverage += uint64(r.hi-r.lo) + 1
	}
	fmt.Fprintf(os.Stderr, "ips: %d v4 ranges (%d addresses), %d v6 ranges\n", len(final), coverage, len(final6))
	if coverage > *maxCoverage {
		fatal(fmt.Errorf("ipv4 coverage %d exceeds max %d — possible over-broad/poisoned feed; aborting", coverage, *maxCoverage))
	}

	// size budget. the packed tables are string constants: the compiler puts
	// them in the binary's read-only data, so these bytes — not the ~4x larger
	// .go source (each byte is a \xNN escape) — are what the sdk ships.
	embedded := len(pepper) + len(keys)*hostRecordLen + len(final)*8 + len(final6)*32
	fmt.Fprintf(os.Stderr, "embedded tables: %d bytes (%.3f MiB) of %d budget (%.3f MiB)\n",
		embedded, float64(embedded)/(1<<20), *maxBytes, float64(*maxBytes)/(1<<20))
	if *maxBytes < embedded {
		fatal(fmt.Errorf("embedded tables %d bytes (%.3f MiB) exceed the size budget of %d bytes (%.3f MiB) — drop feeds, use -tier conservative, or raise -max-bytes",
			embedded, float64(embedded)/(1<<20), *maxBytes, float64(*maxBytes)/(1<<20)))
	}

	src := emit(*tier, pepper, keys, final, final6, results)
	formatted, ferr := format.Source(src)
	if ferr != nil {
		fmt.Fprintf(os.Stderr, "warning: gofmt failed (%v); writing unformatted output\n", ferr)
		formatted = src
	}
	if err := os.WriteFile(outPath, formatted, 0o644); err != nil {
		fatal(err)
	}
	fmt.Fprintf(os.Stderr, "wrote %s (%d host records, %d v4 ranges, %d v6 ranges, %d bytes)\n",
		outPath, len(keys), len(final), len(final6), len(formatted))
}

func makePepper(pepperHex string) (string, error) {
	if pepperHex != "" {
		raw, err := hex.DecodeString(pepperHex)
		if err != nil {
			return "", fmt.Errorf("-pepper-hex: %w", err)
		}
		if len(raw) != 32 {
			return "", fmt.Errorf("-pepper-hex must decode to 32 bytes, got %d", len(raw))
		}
		return string(raw), nil
	}
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return "", err
	}
	return string(raw), nil
}

func fetchAll(tierFeeds []feed, timeout time.Duration) []parsed {
	results := make([]parsed, len(tierFeeds))
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for i := range tierFeeds {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			f := tierFeeds[i]
			data, err := httpGet(f.url, timeout)
			if err != nil {
				results[i] = parsed{f: f, err: err}
				return
			}
			results[i] = parseResponse(f, data)
		}(i)
	}
	wg.Wait()
	return results
}

// Records the exact decoded response body before parser normalization.
func parseResponse(f feed, data []byte) parsed {
	p := parseFeed(f.format, data)
	p.f = f
	p.contentSHA256 = sha256.Sum256(data)
	p.contentBytes = len(data)
	p.contentAvailable = true
	return p
}

func httpGet(feedUrl string, timeout time.Duration) ([]byte, error) {
	client := &http.Client{Timeout: timeout}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
		}
		req, err := http.NewRequest(http.MethodGet, feedUrl, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Accept", "text/plain, */*")
		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("status %s", resp.Status)
			continue
		}
		return body, nil
	}
	return nil, lastErr
}

// parseFeed parses one feed body into normalized host entries and literal ip
// ranges. hosts are returned in input order with duplicates preserved (the
// caller de-duplicates via its set).
func parseFeed(format feedFormat, data []byte) parsed {
	var p parsed
	data = bytes.TrimPrefix(data, []byte{0xEF, 0xBB, 0xBF}) // utf-8 bom
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		switch format {
		case formatAdblock:
			parseAdblockLine(&p, line)
		default:
			parseTolerantLine(&p, line)
		}
	}
	return p
}

// parseTolerantLine handles hosts files, plain domain lists, and bare ip/cidr
// lines. # and ; start comments (whole-line or inline).
func parseTolerantLine(p *parsed, line string) {
	if i := strings.IndexAny(line, "#;"); i >= 0 {
		line = line[:i]
	}
	fields := strings.Fields(line)
	if len(fields) == 0 {
		return
	}
	first := fields[0]
	if addr, err := netip.ParseAddr(first); err == nil {
		if len(fields) == 1 {
			// a bare ip entry
			routeAddr(p, addr)
			return
		}
		// hosts style: null-route target followed by one or more names.
		// the target itself is not a blocked ip.
		for _, tok := range fields[1:] {
			parseToken(p, tok)
		}
		return
	}
	if routePrefixString(p, first) {
		return
	}
	// a domain list line: one entry, anything after the first token is junk
	parseToken(p, first)
}

func parseToken(p *parsed, tok string) {
	// wildcard prefixes mean "and subdomains", which is the storage semantic
	// already; the base itself is also blocked (slight over-match for "*.")
	for {
		if after, ok := strings.CutPrefix(tok, "*."); ok {
			tok = after
			continue
		}
		if after, ok := strings.CutPrefix(tok, "**."); ok {
			tok = after
			continue
		}
		break
	}
	if addr, err := netip.ParseAddr(tok); err == nil {
		routeAddr(p, addr)
		return
	}
	if routePrefixString(p, tok) {
		return
	}
	if name, ok := normalizeHost(tok); ok {
		p.hosts = append(p.hosts, name)
	} else {
		p.invalid += 1
	}
}

// parseAdblockLine accepts exactly "||name^"; every other rule form is
// skipped (counted), and ! or # lines are comments.
func parseAdblockLine(p *parsed, line string) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "!") || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "[") {
		return
	}
	inner, ok := strings.CutPrefix(line, "||")
	if !ok {
		p.skipped += 1
		return
	}
	inner, ok = strings.CutSuffix(inner, "^")
	if !ok || inner == "" || strings.ContainsAny(inner, "/*^$|@") {
		p.skipped += 1
		return
	}
	if name, ok := normalizeHost(inner); ok {
		p.hosts = append(p.hosts, name)
	} else {
		p.invalid += 1
	}
}

func routeAddr(p *parsed, addr netip.Addr) {
	addr = addr.Unmap()
	if addr.Is4() {
		u := addrU32(addr)
		p.ranges = append(p.ranges, iprange{lo: u, hi: u})
	} else if addr.Is6() {
		p.ranges6 = append(p.ranges6, ip6range{lo: addr, hi: addr})
	}
}

// routePrefixString parses a cidr token into the ip tables, applying the
// poison guards. returns true when the token was a cidr (valid or poison).
func routePrefixString(p *parsed, tok string) bool {
	if !strings.ContainsRune(tok, '/') {
		return false
	}
	prefix, err := netip.ParsePrefix(tok)
	if err != nil {
		return false
	}
	prefix = prefix.Masked()
	addr := prefix.Addr().Unmap()
	bits := prefix.Bits()
	if addr.Is4() {
		if bits < minPrefixBits || 32 < bits {
			p.invalid += 1
			return true
		}
		lo := addrU32(addr)
		host := uint32(0xffffffff) >> uint(bits)
		p.ranges = append(p.ranges, iprange{lo: lo, hi: lo | host})
		return true
	}
	if bits < minPrefix6Bits || 128 < bits {
		p.invalid += 1
		return true
	}
	lo, hi := prefixBounds6(prefix)
	p.ranges6 = append(p.ranges6, ip6range{lo: lo, hi: hi})
	return true
}

// normalizeHost canonicalizes a raw list entry exactly like the runtime
// (dataBlocker.BlockHost): lower case, one trailing dot stripped, punycode
// via connect.Punycode, structural validation. returns false for anything
// that is not a plausible hostname (including digits-and-dots ip likes).
func normalizeHost(raw string) (string, bool) {
	name := strings.ToLower(strings.TrimSpace(raw))
	name = strings.TrimSuffix(name, ".")
	if name == "" || maxHostLen < len(name) {
		return "", false
	}
	ascii := true
	for i := 0; i < len(name); i += 1 {
		if 0x80 <= name[i] {
			ascii = false
			break
		}
	}
	if !ascii {
		punycode, err := connect.Punycode(name)
		if err != nil || punycode == "" || maxHostLen < len(punycode) {
			return "", false
		}
		name = punycode
	}
	if name[0] == '.' || name[len(name)-1] == '.' {
		return "", false
	}
	digitsDotsOnly := true
	labelLen := 0
	for i := 0; i < len(name); i += 1 {
		c := name[i]
		if c == '.' {
			if labelLen == 0 {
				return "", false // empty label
			}
			labelLen = 0
			continue
		}
		labelLen += 1
		if 63 < labelLen {
			return "", false
		}
		switch {
		case 'a' <= c && c <= 'z':
			digitsDotsOnly = false
		case '0' <= c && c <= '9':
		case c == '-' || c == '_':
			digitsDotsOnly = false
		default:
			return "", false
		}
	}
	if digitsDotsOnly {
		return "", false
	}
	return name, true
}

// isPublicSuffixEntry reports whether the entry IS a public suffix (icann or
// private section), e.g. "com", "co.uk", "github.io", "cloudfront.net",
// "s3.amazonaws.com". such an entry would block an entire platform under
// suffix semantics, so it is rejected as poison. single label entries are
// always their own public suffix and get rejected here too.
func isPublicSuffixEntry(name string) bool {
	ps, _ := publicsuffix.PublicSuffix(name)
	return ps == name
}

// subtractAllow removes every allow name from the blocked set, exact names
// only: an allow entry cannot punch a hole under a blocked parent domain
// (the parent's suffix rule still matches). returns the number removed.
func subtractAllow(block map[string]bool, allow map[string]bool) int {
	removed := 0
	for name := range allow {
		if block[name] {
			delete(block, name)
			removed += 1
		}
	}
	return removed
}

// compressSuffixes drops every entry that has an ancestor entry in the set
// (the ancestor already blocks it via suffix matching). returns the number
// dropped. order independent: ancestors are never removed.
func compressSuffixes(set map[string]bool) int {
	dropped := 0
	for name := range set {
		for i := 0; i < len(name); i += 1 {
			if name[i] != '.' {
				continue
			}
			parent := name[i+1:]
			if strings.IndexByte(parent, '.') < 0 {
				// a one label parent is never an entry (psl guard)
				break
			}
			if set[parent] {
				delete(set, name)
				dropped += 1
				break
			}
		}
	}
	return dropped
}

// hashHost is the hash construction shared with the runtime
// (dataBlocker.blockHash) and pinned by testdata/hash_vectors.txt: the leading
// hostRecordLen bytes of the digest, big endian, right-aligned in a uint64.
func hashHost(pepper string, name string) uint64 {
	sum := sha256.Sum256([]byte(pepper + name))
	var key uint64
	for i := 0; i < hostRecordLen; i += 1 {
		key = key<<8 | uint64(sum[i])
	}
	return key
}

func hashHosts(pepper string, set map[string]bool) []uint64 {
	keys := make([]uint64, 0, len(set))
	for name := range set {
		keys = append(keys, hashHost(pepper, name))
	}
	slices.Sort(keys)
	return slices.Compact(keys)
}

// resolverExclusions collects the default resolver endpoints
// (DefaultDnsResolverSettings): hostnames are added to the allow set, ip
// endpoints are subtracted from the ip tables. the blocker must never block
// dns resolution infrastructure.
func resolverExclusions() (map[string]bool, []netip.Addr) {
	names := map[string]bool{}
	addrs := []netip.Addr{}
	seen := map[netip.Addr]bool{}
	addHost := func(host string) {
		host = strings.TrimSpace(host)
		if host == "" {
			return
		}
		if addr, err := netip.ParseAddr(host); err == nil {
			addr = addr.Unmap()
			if !seen[addr] {
				seen[addr] = true
				addrs = append(addrs, addr)
			}
			return
		}
		if name, ok := normalizeHost(host); ok {
			names[name] = true
		}
	}
	resolver := connect.DefaultDnsResolverSettings()
	for _, dohUrls := range [][]string{
		resolver.RemoteDohUrlsIpv4, resolver.RemoteDohUrlsIpv6,
		resolver.LocalDohUrlsIpv4, resolver.LocalDohUrlsIpv6,
	} {
		for _, dohUrl := range dohUrls {
			u, err := url.Parse(strings.TrimSpace(dohUrl))
			if err != nil {
				continue
			}
			addHost(u.Hostname())
		}
	}
	for _, dnsAddrs := range [][]string{
		resolver.RemoteDnsIpv4, resolver.RemoteDnsIpv6,
		resolver.LocalDnsIpv4, resolver.LocalDnsIpv6,
	} {
		for _, dnsAddr := range dnsAddrs {
			host := strings.TrimSpace(dnsAddr)
			// tolerate host:port forms
			if addrPort, err := netip.ParseAddrPort(host); err == nil {
				addHost(addrPort.Addr().String())
				continue
			}
			addHost(host)
		}
	}
	return names, addrs
}

func addrU32(a netip.Addr) uint32 {
	b := a.As4()
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func prefixBounds6(p netip.Prefix) (netip.Addr, netip.Addr) {
	p = p.Masked()
	lo := p.Addr()
	a := lo.As16()
	for i := p.Bits(); i < 128; i++ {
		a[i/8] |= 1 << (7 - uint(i%8))
	}
	return lo, netip.AddrFrom16(a)
}

func cidrRange(s string) iprange {
	p := netip.MustParsePrefix(s).Masked()
	lo := addrU32(p.Addr())
	host := uint32(0xffffffff) >> uint(p.Bits())
	return iprange{lo: lo, hi: lo | host}
}

func parseReserved() []iprange {
	rs := make([]iprange, 0, len(reservedCIDRs))
	for _, c := range reservedCIDRs {
		rs = append(rs, cidrRange(c))
	}
	return rs
}

func parseReserved6() []ip6range {
	rs := make([]ip6range, 0, len(reservedCIDRs6))
	for _, c := range reservedCIDRs6 {
		lo, hi := prefixBounds6(netip.MustParsePrefix(c))
		rs = append(rs, ip6range{lo: lo, hi: hi})
	}
	return rs
}

// mergeRanges sorts and coalesces overlapping or adjacent ranges into a
// minimal sorted, pairwise-disjoint set. (same as security/main.go)
func mergeRanges(in []iprange) []iprange {
	if len(in) == 0 {
		return nil
	}
	rs := make([]iprange, len(in))
	copy(rs, in)
	sort.Slice(rs, func(i, j int) bool {
		if rs[i].lo != rs[j].lo {
			return rs[i].lo < rs[j].lo
		}
		return rs[i].hi < rs[j].hi
	})
	out := rs[:1]
	for _, r := range rs[1:] {
		last := &out[len(out)-1]
		switch {
		case r.lo <= last.hi:
			if r.hi > last.hi {
				last.hi = r.hi
			}
		case last.hi != 0xffffffff && r.lo == last.hi+1:
			last.hi = r.hi
		default:
			out = append(out, r)
		}
	}
	return out
}

func subtractAll(block, reserved []iprange) []iprange {
	res := block
	for _, r := range reserved {
		var next []iprange
		for _, b := range res {
			if r.hi < b.lo || r.lo > b.hi {
				next = append(next, b)
				continue
			}
			if b.lo < r.lo {
				next = append(next, iprange{lo: b.lo, hi: r.lo - 1})
			}
			if r.hi < b.hi {
				next = append(next, iprange{lo: r.hi + 1, hi: b.hi})
			}
		}
		res = next
	}
	return res
}

func mergeRanges6(in []ip6range) []ip6range {
	if len(in) == 0 {
		return nil
	}
	rs := make([]ip6range, len(in))
	copy(rs, in)
	sort.Slice(rs, func(i, j int) bool {
		if c := rs[i].lo.Compare(rs[j].lo); c != 0 {
			return c < 0
		}
		return rs[i].hi.Compare(rs[j].hi) < 0
	})
	out := rs[:1]
	for _, r := range rs[1:] {
		last := &out[len(out)-1]
		switch {
		case r.lo.Compare(last.hi) <= 0:
			if r.hi.Compare(last.hi) > 0 {
				last.hi = r.hi
			}
		case last.hi.Next() == r.lo:
			last.hi = r.hi
		default:
			out = append(out, r)
		}
	}
	return out
}

func subtractAll6(block, reserved []ip6range) []ip6range {
	res := block
	for _, r := range reserved {
		var next []ip6range
		for _, b := range res {
			if r.hi.Compare(b.lo) < 0 || r.lo.Compare(b.hi) > 0 {
				next = append(next, b)
				continue
			}
			if b.lo.Compare(r.lo) < 0 {
				next = append(next, ip6range{lo: b.lo, hi: r.lo.Prev()})
			}
			if r.hi.Compare(b.hi) < 0 {
				next = append(next, ip6range{lo: r.hi.Next(), hi: b.hi})
			}
		}
		res = next
	}
	return res
}

func emit(tier string, pepper string, keys []uint64, ranges []iprange, ranges6 []ip6range, results []parsed) []byte {
	var b strings.Builder
	c := func(s string) {
		for _, ln := range strings.Split(s, "\n") {
			if ln == "" {
				b.WriteString("//\n")
			} else {
				b.WriteString("// ")
				b.WriteString(ln)
				b.WriteByte('\n')
			}
		}
	}

	b.WriteString("// Code generated by blocker/main.go; DO NOT EDIT.\n//\n")
	c("Default community block data for the connect Blocker (ip_blocker.go):")
	c("ad/tracking hostnames as peppered truncated hashes plus literal ip")
	c("ranges, consulted by the dns upgrade mux and the multi client when the")
	c("blocker is enabled.")
	c("")
	c(fmt.Sprintf("Tier: %s. Regenerate with:  go generate ./blocker   (or: cd blocker && go run .)", tier))
	c("")
	c("Representation: blockerBlockedHostData is a read-only string constant of")
	c(fmt.Sprintf("blockerBlockedHostCount records of %d bytes each — the first %d bytes, big", hostRecordLen, hostRecordLen))
	c("endian, of SHA256(blockerPepper || name) — sorted ascending and unique,")
	c("where name is the normalized (lower case, punycode) entry. an entry")
	c("blocks the name and all its subdomains; the lookup walks label suffixes")
	c("(see dataBlocker.BlockHost). the pepper is regenerated on every run,")
	c("after the feeds are fetched: collision hardening and per-release")
	c("rotation, not secrecy. blockerBlockedPrefixData / blockerBlockedPrefix6Data")
	c("use the same packed range format as the security tables; searches are")
	c("shared (ip_util.go). Being constants, the tables live in the binary's")
	c("read-only data: package initialization performs zero heap allocation.")
	c("")
	c("Source feeds & attribution (composition per sdk/BLOCKER.md):")
	for _, result := range results {
		c("  - " + result.f.credit)
	}
	c("  - " + localAllowFile + " (in-house) [subtracted]")
	c("  - default resolver endpoints from DefaultDnsResolverSettings [excluded]")
	c("")
	c("Generated input provenance v1 (declared feed order):")
	for _, result := range results {
		if !result.contentAvailable {
			c(fmt.Sprintf("  - name=%s url=%s disposition=%s", result.f.name, result.f.url, feedDispositionUnavailable))
			continue
		}
		c(fmt.Sprintf("  - name=%s url=%s content_sha256=%x content_bytes=%d parsed_hosts=%d parsed_v4_ranges=%d parsed_v6_ranges=%d parsed_invalid=%d parsed_skipped=%d disposition=%s",
			result.f.name, result.f.url, result.contentSHA256, result.contentBytes, len(result.hosts), len(result.ranges), len(result.ranges6), result.invalid, result.skipped, result.disposition))
	}
	b.WriteString("\npackage connect\n\n")

	writeStringConst := func(name string, data []byte) {
		if len(data) == 0 {
			fmt.Fprintf(&b, "const %s = \"\"\n\n", name)
			return
		}
		fmt.Fprintf(&b, "const %s = \"\" +\n", name)
		const perLine = 64
		const hexd = "0123456789abcdef"
		for off := 0; off < len(data); off += perLine {
			end := off + perLine
			if end > len(data) {
				end = len(data)
			}
			b.WriteByte('\t')
			b.WriteByte('"')
			for _, by := range data[off:end] {
				b.WriteString("\\x")
				b.WriteByte(hexd[by>>4])
				b.WriteByte(hexd[by&0xf])
			}
			b.WriteByte('"')
			if end < len(data) {
				b.WriteString(" +")
			}
			b.WriteByte('\n')
		}
		b.WriteByte('\n')
	}

	writeStringConst("blockerPepper", []byte(pepper))

	fmt.Fprintf(&b, "const blockerBlockedHostCount = %d\n\n", len(keys))
	// each key is written as its low hostRecordLen bytes, big endian — which
	// are exactly the leading digest bytes hashHost consumed, so the sorted
	// key order equals the packed records' lexicographic order
	hostData := make([]byte, 0, len(keys)*hostRecordLen)
	for _, key := range keys {
		for i := hostRecordLen - 1; 0 <= i; i -= 1 {
			hostData = append(hostData, byte(key>>(8*i)))
		}
	}
	writeStringConst("blockerBlockedHostData", hostData)

	fmt.Fprintf(&b, "const blockerBlockedPrefixCount = %d\n\n", len(ranges))
	data4 := make([]byte, 0, len(ranges)*8)
	for _, r := range ranges {
		data4 = binary.BigEndian.AppendUint32(data4, r.lo)
		data4 = binary.BigEndian.AppendUint32(data4, r.hi)
	}
	writeStringConst("blockerBlockedPrefixData", data4)

	fmt.Fprintf(&b, "const blockerBlockedPrefix6Count = %d\n\n", len(ranges6))
	data6 := make([]byte, 0, len(ranges6)*32)
	for _, r := range ranges6 {
		lo := r.lo.As16()
		hi := r.hi.As16()
		data6 = append(data6, lo[:]...)
		data6 = append(data6, hi[:]...)
	}
	writeStringConst("blockerBlockedPrefix6Data", data6)

	return []byte(b.String())
}

// outputFile is the generated data file. ip_blocker.go (the interface, the
// default implementation, and the lookups) is hand-written; this generator
// only ever owns the data file.
const outputFile = "ip_blocker_block.go"

func resolveOut(flagVal string) (string, string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", "", err
	}
	for {
		if b, err := os.ReadFile(filepath.Join(dir, "go.mod")); err == nil {
			if bytes.Contains(b, []byte("module github.com/urnetwork/connect")) {
				outPath := flagVal
				if outPath == "" {
					outPath = filepath.Join(dir, outputFile)
				}
				return dir, outPath, nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", "", errors.New("could not locate the connect module root; run from within the module")
}

func guardOutput(path string, force bool) error {
	if force {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()
	head := make([]byte, 512)
	n, _ := io.ReadFull(f, head)
	if !bytes.Contains(head[:n], []byte("DO NOT EDIT")) {
		return fmt.Errorf("%s exists and is not a generated file (no \"DO NOT EDIT\" marker); refusing to overwrite (use -force to override)", path)
	}
	return nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "error:", err)
	os.Exit(1)
}
