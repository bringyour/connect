//go:generate go run .

// Command gen aggregates public IPv4 threat-intelligence feeds into a packed,
// sorted, pairwise-disjoint blocklist and writes it to
// ip_security_cfaa_block.go as a zero-allocation, binary-searchable table.
//
// It replaces the old security/gen.sh pipeline. Unlike that script it:
//   - preserves CIDR ranges instead of collapsing them to their network address
//     (the old grep/sed dropped every "/NN" mask, silently turning a whole block
//     into a single host),
//   - validates octets (rejects malformed / out-of-range addresses),
//   - parses abuse.ch CSV feeds (ThreatFox),
//   - enforces per-feed minimum counts so a feed that goes dark or changes
//     format aborts the build instead of silently shrinking protection,
//   - subtracts non-global / reserved address space, and
//   - emits ranges (not exact /32 hosts) for true prefix matching.
//
// Usage:
//
//	go generate ./...                 # via the directive above
//	cd security && go run .           # equivalent
//	go run ./security -out /path.go   # explicit output
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"go/format"
	"io"
	"net"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const userAgent = "urnetwork-connect-blocklist-generator/1.0 (+https://ur.network)"

// minPrefixBits rejects any source CIDR broader than this as poison. No
// legitimate blocklist entry covers more than a /8; a /0../7 would knock out
// huge swaths of the internet if a feed were corrupted or compromised.
const minPrefixBits = 8

// minPrefix6Bits is the IPv6 analog: reject any source CIDR broader than a /16.
// Legitimate IPv6 blocklist entries are /32../128; anything broader is poison.
const minPrefix6Bits = 16

type feedKind int

const (
	kindText feedKind = iota // line-based: one host IP or CIDR per line, # or ; comments
	kindCSV                  // abuse.ch quoted CSV; the IP (or ip:port) lives in csvCol
)

type feed struct {
	name       string
	url        string
	kind       feedKind
	csvCol     int    // for kindCSV: 0-based column holding the IP / "ip:port"
	csvTypeCol int    // for kindCSV: 0-based column to filter on (with csvTypeVal); ignored when csvTypeVal is ""
	csvTypeVal string // for kindCSV: only rows whose csvTypeCol equals this are taken
	minCount   int    // hard-fail the build if fewer valid entries than this
	minV6Count int    // hard-fail the build if fewer valid IPv6 entries than this
	deprecated bool   // accept a successfully fetched empty feed; fetch errors still fail
	credit     string // attribution line embedded in the generated header
}

// feeds is the full set pulled into the blocklist. It is sourced under a
// permissive-first policy for commercial redistribution (the transformed table
// ships embedded in released binaries): included are permissive/public-domain
// feeds (CC0, BSD), Spamhaus DROP (free for product use, attribution retained
// in the header below), and openly-published feeds that state no license
// (treated as usable). Feeds whose terms explicitly forbid commercial use or
// redistribution are excluded — AlienVault OTX (EULA forbids distribution),
// Binary Defense (non-commercial), AbuseIPDB / GreenSnow / DShield (via the
// dropped aggregates), and abuse.ch ThreatFox (as of 2025-11-04 its CC0 grant
// was replaced by Spamhaus-managed terms: auth-gated, commercial use may
// require a subscription, revocable — re-add only under an abuse.ch/Spamhaus
// agreement). Contaminated aggregates that re-bundle those terms (ipsum,
// FireHOL level1, romainmarcoux) are excluded in favor of pulling clean
// sources directly. Dead sources (CruzIt frozen 2023, NiX Spam ceased 2025,
// SSLBL/darklist empty) are removed outright.
//
// Counts and floors were calibrated against live data (2026-07-11); floors sit
// well under observed volume so normal churn never trips them, but a dead or
// reformatted feed does.
var feeds = []feed{
	// permissive / public-domain (egress-relevant: botnet C2, malware/phishing hosting)
	{name: "feodo", url: "https://feodotracker.abuse.ch/downloads/ipblocklist.txt", kind: kindText, minCount: 0, deprecated: true, credit: "abuse.ch Feodo Tracker botnet C2, recommended (CC0) — feodotracker.abuse.ch (often empty after botnet takedowns)"},
	{name: "feodo_aggressive", url: "https://feodotracker.abuse.ch/downloads/ipblocklist_aggressive.txt", kind: kindText, minCount: 4000, credit: "abuse.ch Feodo Tracker botnet C2, aggressive (CC0) — feodotracker.abuse.ch"},
	{name: "tweetfeed", url: "https://raw.githubusercontent.com/0xDanielLopez/TweetFeed/master/month.csv", kind: kindCSV, csvCol: 3, csvTypeCol: 2, csvTypeVal: "ip", minCount: 150, credit: "TweetFeed community IOCs, IP rows (CC0-1.0) — github.com/0xDanielLopez/TweetFeed"},
	{name: "viriback", url: "https://tracker.viriback.com/dump.php", kind: kindCSV, csvCol: 2, minCount: 5000, credit: "ViriBack C2 Tracker (no stated license; openly published) — tracker.viriback.com"},
	{name: "et_compromised", url: "https://rules.emergingthreats.net/blockrules/compromised-ips.txt", kind: kindText, minCount: 300, credit: "Emerging Threats Open compromised IPs (BSD, sids 2000000-2799999) — rules.emergingthreats.net"},
	// free for product use with attribution (Spamhaus credit retained in the header below)
	{name: "spamhaus", url: "https://www.spamhaus.org/drop/drop.txt", kind: kindText, minCount: 800, credit: "The Spamhaus DROP list — spamhaus.org/drop (© The Spamhaus Project; credit retained below)"},
	{name: "spamhaus6", url: "https://www.spamhaus.org/drop/dropv6.txt", kind: kindText, minCount: 50, minV6Count: 50, credit: "The Spamhaus DROPv6 list — spamhaus.org/drop (© The Spamhaus Project)"},
	// openly published, no stated license (treated as usable per the sourcing policy)
	{name: "blocklistde", url: "https://lists.blocklist.de/lists/all.txt", kind: kindText, minCount: 8000, credit: "Blocklist.de fail2ban reports (no stated license) — lists.blocklist.de"},
	{name: "cins", url: "https://cinsscore.com/list/ci-badguys.txt", kind: kindText, minCount: 7000, credit: "CINS Army / CI Army ci-badguys (no stated license) — cinsscore.com"},
	{name: "bruteforce", url: "https://iplists.firehol.org/files/bruteforceblocker.ipset", kind: kindText, minCount: 300, credit: "BruteForceBlocker (no stated license; via FireHOL) — iplists.firehol.org"},
}

// reservedCIDRs is non-global / special-purpose IPv4 space (RFC 6890 et al.).
// We subtract it from the blocklist: the egress path already refuses private,
// loopback, link-local, multicast and unspecified destinations before consulting
// this table, and feeds (notably FireHOL's fullbogons component) occasionally
// dump large reserved blocks that would otherwise dominate coverage. Keeping the
// table to genuine public-unicast space makes the coverage sanity check
// meaningful and avoids carrying dead weight.
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

// reservedCIDRs6 is non-global / special-purpose IPv6 space (RFC 4291/6890),
// subtracted from the IPv6 blocklist for the same reasons as the IPv4 set.
var reservedCIDRs6 = []string{
	"::/8",          // unspecified, loopback, v4-mapped, v4-compatible (non-global)
	"64:ff9b::/96",  // nat64
	"100::/64",      // discard-only
	"2001:db8::/32", // documentation
	"2002::/16",     // 6to4
	"fc00::/7",      // unique local
	"fe80::/10",     // link-local
	"ff00::/8",      // multicast
}

type iprange struct{ lo, hi uint32 }

type ip6range struct{ lo, hi netip.Addr }

const (
	feedDispositionAcceptedBlock      = "accepted_block"
	feedDispositionAcceptedDeprecated = "accepted_deprecated"
	feedDispositionSkippedMinCount    = "skipped_below_min_count"
	feedDispositionUnavailable        = "unavailable"
)

// Holds one feed's decoded content and parse result.
type result struct {
	f                feed
	ranges           []iprange
	ranges6          []ip6range
	host             int
	cidr             int
	bad              int
	credit           string // captured dynamic attribution (Spamhaus)
	contentSHA256    [32]byte
	contentBytes     int
	contentAvailable bool
	disposition      string
	err              error
}

// Carries the accepted policy data and stable sanity failures.
type aggregation struct {
	ranges         []iprange
	ranges6        []ip6range
	spamhausCredit string
	problems       []string
}

func main() {
	out := flag.String("out", "", "output .go path (default: <connect module root>/ip_security_cfaa_block.go)")
	timeout := flag.Duration("timeout", 90*time.Second, "per-feed HTTP timeout")
	allowStale := flag.Bool("allow-stale", false, "generate even if some feeds fail or fall below min-count (skips them)")
	maxCoverage := flag.Uint64("max-coverage", 64_000_000, "fail if total blocked address count exceeds this (poison guard)")
	minRanges := flag.Int("min-ranges", 10_000, "fail if fewer than this many merged ranges result")
	minRanges6 := flag.Int("min-ranges6", 100, "fail if fewer than this many merged IPv6 ranges result")
	maxBytes := flag.Int("max-bytes", 1<<20, "size budget: fail if the EMBEDDED table exceeds this many bytes (the packed rodata that ships in the sdk binary, not the .go source, which is ~4x larger from \\xNN escapes)")
	force := flag.Bool("force", false, "overwrite the output even if it lacks a generated-file marker")
	flag.Parse()

	outPath, err := resolveOut(*out)
	if err != nil {
		fatal(err)
	}
	if err := guardOutput(outPath, *force); err != nil {
		fatal(err)
	}

	results := fetchAll(*timeout)

	fmt.Fprintln(os.Stderr, "feed report:")
	for _, r := range results {
		status := "ok"
		switch {
		case r.err != nil:
			status = "ERROR: " + r.err.Error()
		case r.f.deprecated && len(r.ranges) == 0 && len(r.ranges6) == 0:
			status = "empty (deprecated)"
		}
		fmt.Fprintf(os.Stderr, "  %-14s v4=%-6d v6=%-5d host=%-6d cidr=%-6d bad=%-5d %s\n",
			r.f.name, len(r.ranges), len(r.ranges6), r.host, r.cidr, r.bad, status)

		if r.err == nil && len(r.ranges) == 0 && len(r.ranges6) == 0 && !r.f.deprecated {
			fmt.Fprintf(os.Stderr, "  WARNING: %s returned 0 entries\n", r.f.name)
		}
	}

	aggregated := aggregateResults(results)
	if len(aggregated.problems) > 0 && !*allowStale {
		fmt.Fprintln(os.Stderr, "\nABORT: feed sanity checks failed (pass -allow-stale to skip the offending feeds):")
		for _, p := range aggregated.problems {
			fmt.Fprintln(os.Stderr, "  - "+p)
		}
		os.Exit(1)
	}

	merged := mergeRanges(aggregated.ranges)
	reserved := mergeRanges(parseReserved())
	final := subtractAll(merged, reserved)

	merged6 := mergeRanges6(aggregated.ranges6)
	reserved6 := mergeRanges6(parseReserved6())
	final6 := subtractAll6(merged6, reserved6)

	var coverage uint64
	for _, r := range final {
		coverage += uint64(r.hi-r.lo) + 1
	}

	fmt.Fprintf(os.Stderr, "\naggregate: %d source entries -> %d merged ranges (%d after reserved subtraction), %d addresses covered\n",
		len(aggregated.ranges), len(merged), len(final), coverage)
	fmt.Fprintf(os.Stderr, "ipv6: %d source entries -> %d merged ranges (%d after reserved subtraction)\n",
		len(aggregated.ranges6), len(merged6), len(final6))
	printLargest(final, 5)

	if err := validateMinimumRanges(len(final), len(final6), *minRanges, *minRanges6); err != nil {
		fatal(err)
	}
	if coverage > *maxCoverage {
		fatal(fmt.Errorf("coverage %d exceeds max %d — possible over-broad/poisoned feed; aborting", coverage, *maxCoverage))
	}

	// size budget. the packed tables are string constants: the compiler puts
	// them in the binary's read-only data, so these bytes — not the ~4x larger
	// .go source (each byte is a \xNN escape) — are what the sdk ships.
	embedded := len(final)*8 + len(final6)*32
	fmt.Fprintf(os.Stderr, "embedded table: %d bytes (%.3f MiB) of %d budget (%.3f MiB)\n",
		embedded, float64(embedded)/(1<<20), *maxBytes, float64(*maxBytes)/(1<<20))
	if *maxBytes < embedded {
		fatal(fmt.Errorf("embedded table %d bytes (%.3f MiB) exceeds the size budget of %d bytes (%.3f MiB) — drop feeds or raise -max-bytes",
			embedded, float64(embedded)/(1<<20), *maxBytes, float64(*maxBytes)/(1<<20)))
	}

	src := emit(final, final6, aggregated.spamhausCredit, results)
	formatted, ferr := format.Source(src)
	if ferr != nil {
		fmt.Fprintf(os.Stderr, "warning: gofmt failed (%v); writing unformatted output\n", ferr)
		formatted = src
	}
	if err := os.WriteFile(outPath, formatted, 0o644); err != nil {
		fatal(err)
	}
	fmt.Fprintf(os.Stderr, "wrote %s (%d ranges, %d bytes)\n", outPath, len(final), len(formatted))
}

// Records feed disposition and excludes unusable inputs.
func aggregateResults(results []result) aggregation {
	var aggregated aggregation
	for i := range results {
		result := &results[i]
		switch {
		case result.err != nil:
			result.disposition = feedDispositionUnavailable
			aggregated.problems = append(aggregated.problems, fmt.Sprintf("%s: fetch error: %v", result.f.name, result.err))
			continue
		case !result.f.deprecated && len(result.ranges)+len(result.ranges6) < result.f.minCount:
			result.disposition = feedDispositionSkippedMinCount
			aggregated.problems = append(aggregated.problems, fmt.Sprintf("%s: only %d valid entries (min %d) — feed may be broken or reformatted",
				result.f.name, len(result.ranges)+len(result.ranges6), result.f.minCount))
			continue
		case !result.f.deprecated && len(result.ranges6) < result.f.minV6Count:
			result.disposition = feedDispositionSkippedMinCount
			aggregated.problems = append(aggregated.problems, fmt.Sprintf("%s: only %d valid IPv6 entries (min %d) — feed may be broken or reformatted",
				result.f.name, len(result.ranges6), result.f.minV6Count))
			continue
		case result.f.deprecated:
			result.disposition = feedDispositionAcceptedDeprecated
		default:
			result.disposition = feedDispositionAcceptedBlock
		}
		if result.credit != "" {
			aggregated.spamhausCredit = result.credit
		}
		aggregated.ranges = append(aggregated.ranges, result.ranges...)
		aggregated.ranges6 = append(aggregated.ranges6, result.ranges6...)
	}
	return aggregated
}

// Rejects unexpectedly small generated tables.
func validateMinimumRanges(v4Count int, v6Count int, minimumV4 int, minimumV6 int) error {
	if v4Count < minimumV4 {
		return fmt.Errorf("only %d merged IPv4 ranges (min %d) — too few; aborting", v4Count, minimumV4)
	}
	if v6Count < minimumV6 {
		return fmt.Errorf("only %d merged IPv6 ranges (min %d) — too few; aborting", v6Count, minimumV6)
	}
	return nil
}

func fetchAll(timeout time.Duration) []result {
	results := make([]result, len(feeds))
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for i := range feeds {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			results[i] = fetchParse(feeds[i], timeout)
		}(i)
	}
	wg.Wait()
	return results
}

func fetchParse(f feed, timeout time.Duration) result {
	data, err := httpGet(f.url, timeout)
	if err != nil {
		return result{f: f, err: err}
	}
	return parseResponse(f, data)
}

// Records the exact decoded response body before parser normalization.
func parseResponse(f feed, data []byte) result {
	res := result{
		f:                f,
		contentSHA256:    sha256.Sum256(data),
		contentBytes:     len(data),
		contentAvailable: true,
	}
	if f.name == "spamhaus" {
		res.credit = captureSpamhausCredit(data)
	}
	switch f.kind {
	case kindText:
		res.ranges, res.ranges6, res.host, res.cidr, res.bad = parseText(data)
	case kindCSV:
		res.ranges, res.ranges6, res.host, res.bad = parseCSV(data, f.csvCol, f.csvTypeCol, f.csvTypeVal)
	}
	return res
}

func httpGet(url string, timeout time.Duration) ([]byte, error) {
	client := &http.Client{Timeout: timeout}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		if attempt > 0 {
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
		}
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", userAgent)
		req.Header.Set("Accept", "text/plain, text/csv, */*")
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

// parseText handles line-based feeds. Each line may be a host IP or a CIDR;
// comments start with # or ; (inline or whole-line). Only the first
// whitespace-delimited token is considered.
func parseText(data []byte) (ranges []iprange, ranges6 []ip6range, host, cidr, bad int) {
	sc := bufio.NewScanner(bytes.NewReader(data))
	sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for sc.Scan() {
		line := sc.Text()
		if i := strings.IndexAny(line, "#;"); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if i := strings.IndexAny(line, " \t"); i >= 0 {
			line = line[:i]
		}
		if strings.ContainsRune(line, '/') {
			if r, ok := parsePrefix(line); ok {
				ranges = append(ranges, r)
				cidr++
			} else if r6, ok := parsePrefix6(line); ok {
				ranges6 = append(ranges6, r6)
				cidr++
			} else {
				bad++
			}
		} else {
			if r, ok := parseAddr(line); ok {
				ranges = append(ranges, r)
				host++
			} else if r6, ok := parseAddr6(line); ok {
				ranges6 = append(ranges6, r6)
				host++
			} else {
				bad++
			}
		}
	}
	return
}

// parseCSV handles CSV feeds. The IP lives in column col and may be a bare IP
// or an "ip:port" pair. When typeVal is non-empty, only rows whose typeCol
// equals typeVal are considered (e.g. TweetFeed rows tagged "ip"); other rows
// are skipped, not counted as bad. Header rows are skipped naturally: their IP
// column fails to parse.
func parseCSV(data []byte, col int, typeCol int, typeVal string) (ranges []iprange, ranges6 []ip6range, host, bad int) {
	r := csv.NewReader(bytes.NewReader(data))
	r.Comma = ','
	r.Comment = '#'
	r.TrimLeadingSpace = true
	r.FieldsPerRecord = -1
	r.LazyQuotes = true
	for {
		rec, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // skip malformed rows
		}
		if typeVal != "" {
			if typeCol >= len(rec) || strings.TrimSpace(rec[typeCol]) != typeVal {
				continue // not a row of the requested type
			}
		}
		if col >= len(rec) {
			bad++
			continue
		}
		field := strings.TrimSpace(rec[col])
		if field == "" {
			continue
		}
		if h, _, e := net.SplitHostPort(field); e == nil {
			field = h
		}
		if rg, ok := parseAddr(field); ok {
			ranges = append(ranges, rg)
			host++
		} else if rg6, ok := parseAddr6(field); ok {
			ranges6 = append(ranges6, rg6)
			host++
		} else {
			bad++
		}
	}
	return
}

func parseAddr(s string) (iprange, bool) {
	a, err := netip.ParseAddr(s)
	if err != nil {
		return iprange{}, false
	}
	a = a.Unmap()
	if !a.Is4() {
		return iprange{}, false
	}
	u := addrU32(a)
	return iprange{lo: u, hi: u}, true
}

func parsePrefix(s string) (iprange, bool) {
	p, err := netip.ParsePrefix(s)
	if err != nil {
		return iprange{}, false
	}
	p = p.Masked()
	a := p.Addr().Unmap()
	if !a.Is4() {
		return iprange{}, false
	}
	bits := p.Bits()
	if bits < minPrefixBits || bits > 32 {
		return iprange{}, false
	}
	return prefixRange(a, bits), true
}

func parseAddr6(s string) (ip6range, bool) {
	a, err := netip.ParseAddr(s)
	if err != nil {
		return ip6range{}, false
	}
	a = a.Unmap()
	if !a.Is6() || a.Is4In6() {
		return ip6range{}, false
	}
	return ip6range{lo: a, hi: a}, true
}

func parsePrefix6(s string) (ip6range, bool) {
	p, err := netip.ParsePrefix(s)
	if err != nil {
		return ip6range{}, false
	}
	p = p.Masked()
	a := p.Addr()
	if !a.Is6() || a.Is4In6() {
		return ip6range{}, false
	}
	bits := p.Bits()
	if bits < minPrefix6Bits || bits > 128 {
		return ip6range{}, false
	}
	lo, hi := prefixBounds6(p)
	return ip6range{lo: lo, hi: hi}, true
}

// prefixBounds6 returns the first and last address of an IPv6 prefix.
func prefixBounds6(p netip.Prefix) (netip.Addr, netip.Addr) {
	p = p.Masked()
	lo := p.Addr()
	a := lo.As16()
	for i := p.Bits(); i < 128; i++ {
		a[i/8] |= 1 << (7 - uint(i%8))
	}
	return lo, netip.AddrFrom16(a)
}

// cidrRange parses a trusted internal CIDR constant (no poison guard). It panics
// on a malformed input, which can only be a bug in reservedCIDRs.
func cidrRange(s string) iprange {
	p := netip.MustParsePrefix(s).Masked()
	return prefixRange(p.Addr(), p.Bits())
}

func prefixRange(a netip.Addr, bits int) iprange {
	lo := addrU32(a)
	host := uint32(0xffffffff) >> uint(bits) // bits==0 -> all ones; bits==32 -> 0
	return iprange{lo: lo, hi: lo | host}
}

func addrU32(a netip.Addr) uint32 {
	b := a.As4()
	return uint32(b[0])<<24 | uint32(b[1])<<16 | uint32(b[2])<<8 | uint32(b[3])
}

func parseReserved() []iprange {
	rs := make([]iprange, 0, len(reservedCIDRs))
	for _, c := range reservedCIDRs {
		rs = append(rs, cidrRange(c))
	}
	return rs
}

// mergeRanges sorts and coalesces overlapping or adjacent ranges into a minimal
// sorted, pairwise-disjoint set.
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
		case r.lo <= last.hi: // overlap
			if r.hi > last.hi {
				last.hi = r.hi
			}
		case last.hi != 0xffffffff && r.lo == last.hi+1: // adjacent
			last.hi = r.hi
		default:
			out = append(out, r)
		}
	}
	return out
}

// subtractAll removes every reserved range from the (sorted, disjoint) block
// set, preserving sort order and disjointness.
func subtractAll(block, reserved []iprange) []iprange {
	res := block
	for _, r := range reserved {
		var next []iprange
		for _, b := range res {
			if r.hi < b.lo || r.lo > b.hi { // disjoint
				next = append(next, b)
				continue
			}
			if b.lo < r.lo { // left remainder (r.lo >= 1 here, no underflow)
				next = append(next, iprange{lo: b.lo, hi: r.lo - 1})
			}
			if r.hi < b.hi { // right remainder (r.hi < max here, no overflow)
				next = append(next, iprange{lo: r.hi + 1, hi: b.hi})
			}
		}
		res = next
	}
	return res
}

// mergeRanges6 is the IPv6 analog of mergeRanges.
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
		case r.lo.Compare(last.hi) <= 0: // overlap
			if r.hi.Compare(last.hi) > 0 {
				last.hi = r.hi
			}
		case last.hi.Next() == r.lo: // adjacent
			last.hi = r.hi
		default:
			out = append(out, r)
		}
	}
	return out
}

// subtractAll6 is the IPv6 analog of subtractAll.
func subtractAll6(block, reserved []ip6range) []ip6range {
	res := block
	for _, r := range reserved {
		var next []ip6range
		for _, b := range res {
			if r.hi.Compare(b.lo) < 0 || r.lo.Compare(b.hi) > 0 { // disjoint
				next = append(next, b)
				continue
			}
			if b.lo.Compare(r.lo) < 0 { // left remainder (r.lo > min here)
				next = append(next, ip6range{lo: b.lo, hi: r.lo.Prev()})
			}
			if r.hi.Compare(b.hi) < 0 { // right remainder (r.hi < max here)
				next = append(next, ip6range{lo: r.hi.Next(), hi: b.hi})
			}
		}
		res = next
	}
	return res
}

func parseReserved6() []ip6range {
	rs := make([]ip6range, 0, len(reservedCIDRs6))
	for _, c := range reservedCIDRs6 {
		lo, hi := prefixBounds6(netip.MustParsePrefix(c))
		rs = append(rs, ip6range{lo: lo, hi: hi})
	}
	return rs
}

func printLargest(ranges []iprange, n int) {
	sorted := make([]iprange, len(ranges))
	copy(sorted, ranges)
	sort.Slice(sorted, func(i, j int) bool {
		return (sorted[i].hi - sorted[i].lo) > (sorted[j].hi - sorted[j].lo)
	})
	if n > len(sorted) {
		n = len(sorted)
	}
	for i := 0; i < n; i++ {
		r := sorted[i]
		fmt.Fprintf(os.Stderr, "  largest[%d]: %s - %s (%d addrs)\n", i, u32Str(r.lo), u32Str(r.hi), uint64(r.hi-r.lo)+1)
	}
}

func u32Str(u uint32) string {
	return fmt.Sprintf("%d.%d.%d.%d", byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

func captureSpamhausCredit(data []byte) string {
	var lines []string
	sc := bufio.NewScanner(bytes.NewReader(data))
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if !strings.HasPrefix(line, ";") {
			continue
		}
		body := strings.TrimSpace(strings.TrimPrefix(line, ";"))
		low := strings.ToLower(body)
		if strings.Contains(low, "spamhaus") || strings.Contains(low, "(c)") {
			lines = append(lines, body)
		}
		if len(lines) >= 2 {
			break
		}
	}
	return strings.Join(lines, " | ")
}

func emit(ranges []iprange, ranges6 []ip6range, spamhausCredit string, results []result) []byte {
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

	b.WriteString("// Code generated by security/main.go; DO NOT EDIT.\n//\n")
	c("CFAA egress destination blocklist: a packed, sorted, pairwise-disjoint set")
	c("of blocked IPv4 address ranges aggregated from public threat-intelligence")
	c("feeds. It is consulted on the egress hot path to refuse traffic destined")
	c("for known malicious or hijacked address space.")
	c("")
	c("Regenerate with:  go generate ./...   (or: cd security && go run .)")
	c("")
	c("Representation: cfaaBlockedPrefixData is a read-only string constant holding")
	c("cfaaBlockedPrefixCount records of 8 bytes each — big-endian uint32 lo")
	c("followed by big-endian uint32 hi — sorted ascending by lo and")
	c("non-overlapping. cfaaBlockedPrefix6Data is the IPv6 table: cfaaBlockedPrefix6Count")
	c("records of 32 bytes each (16-byte big-endian lo then hi), same ordering.")
	c("Being constants, they live in the binary's read-only data: package")
	c("initialization performs zero heap allocation. The table definitions are")
	c("generated in ip_security_cfaa_block.go; their binary-search lookups")
	c("(cfaaBlockedIp4, cfaaBlockedIp6) are hand-written in ip_security_cfaa.go.")
	c("")
	c("Source feeds & attribution:")
	for _, result := range results {
		c("  - " + result.f.credit)
	}
	if spamhausCredit != "" {
		c("")
		c("Spamhaus DROP, as required by its terms, credit retained verbatim:")
		c("  " + spamhausCredit)
	}
	c("")
	c("Feed selection originally derived from github.com/Naunter/BT_BlockLists.")
	c("")
	c("Generated input provenance v1 (declared feed order):")
	for _, result := range results {
		if !result.contentAvailable {
			c(fmt.Sprintf("  - name=%s url=%s disposition=%s", result.f.name, result.f.url, feedDispositionUnavailable))
			continue
		}
		c(fmt.Sprintf("  - name=%s url=%s content_sha256=%x content_bytes=%d parsed_v4_ranges=%d parsed_v6_ranges=%d parsed_hosts=%d parsed_cidrs=%d parsed_bad=%d disposition=%s",
			result.f.name, result.f.url, result.contentSHA256, result.contentBytes, len(result.ranges), len(result.ranges6), result.host, result.cidr, result.bad, result.disposition))
	}
	b.WriteString("\npackage connect\n\n")

	fmt.Fprintf(&b, "const cfaaBlockedPrefixCount = %d\n\n", len(ranges))

	data := make([]byte, 0, len(ranges)*8)
	for _, r := range ranges {
		data = append(data,
			byte(r.lo>>24), byte(r.lo>>16), byte(r.lo>>8), byte(r.lo),
			byte(r.hi>>24), byte(r.hi>>16), byte(r.hi>>8), byte(r.hi))
	}

	b.WriteString("const cfaaBlockedPrefixData = \"\" +\n")
	const perLine = 64 // bytes per source line = 8 records
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

	fmt.Fprintf(&b, "\nconst cfaaBlockedPrefix6Count = %d\n\n", len(ranges6))
	data6 := make([]byte, 0, len(ranges6)*32)
	for _, r := range ranges6 {
		lo := r.lo.As16()
		hi := r.hi.As16()
		data6 = append(data6, lo[:]...)
		data6 = append(data6, hi[:]...)
	}
	if 0 == len(data6) {
		b.WriteString("const cfaaBlockedPrefix6Data = \"\"\n")
	} else {
		b.WriteString("const cfaaBlockedPrefix6Data = \"\" +\n")
		for off := 0; off < len(data6); off += perLine {
			end := off + perLine
			if end > len(data6) {
				end = len(data6)
			}
			b.WriteByte('\t')
			b.WriteByte('"')
			for _, by := range data6[off:end] {
				b.WriteString("\\x")
				b.WriteByte(hexd[by>>4])
				b.WriteByte(hexd[by&0xf])
			}
			b.WriteByte('"')
			if end < len(data6) {
				b.WriteString(" +")
			}
			b.WriteByte('\n')
		}
	}

	return []byte(b.String())
}

// outputFile is the generated data file. It deliberately is NOT
// ip_security_cfaa.go: that file is hand-written (the detector and the range
// lookup live there). This generator only ever owns the data file.
const outputFile = "ip_security_cfaa_block.go"

func resolveOut(flagVal string) (string, error) {
	if flagVal != "" {
		return flagVal, nil
	}
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if b, err := os.ReadFile(filepath.Join(dir, "go.mod")); err == nil {
			if bytes.Contains(b, []byte("module github.com/urnetwork/connect")) {
				return filepath.Join(dir, outputFile), nil
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "", errors.New("could not locate the connect module root; pass -out explicitly")
}

// guardOutput refuses to overwrite an existing file that is not a generated one.
// A generated file carries a "DO NOT EDIT" marker near the top; a hand-written
// file (e.g. ip_security_cfaa.go) does not, so this prevents clobbering source.
func guardOutput(path string, force bool) error {
	if force {
		return nil
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // new file is fine
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
