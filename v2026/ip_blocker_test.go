package connect

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// helpers to hand-build blocker tables. the hash construction here is the
// independent counterpart of dataBlocker.blockHash and must agree with the
// generator (blocker/testdata/hash_vectors.txt pins both).

const blockerTestPepper = "0123456789abcdef0123456789abcdef"

var blockerTestSink bool

func blockerTestHostKey(pepper string, name string) uint64 {
	sum := sha256.Sum256([]byte(pepper + name))
	var key uint64
	for i := 0; i < blockerHostRecordLen; i += 1 {
		key = key<<8 | uint64(sum[i])
	}
	return key
}

func blockerTestHostTable(pepper string, names []string) (string, int) {
	keys := make([]uint64, 0, len(names))
	for _, name := range names {
		keys = append(keys, blockerTestHostKey(pepper, name))
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	var data []byte
	for _, key := range keys {
		for i := blockerHostRecordLen - 1; 0 <= i; i -= 1 {
			data = append(data, byte(key>>(8*i)))
		}
	}
	return string(data), len(keys)
}

func blockerTestPack4(ranges [][2]uint32) (string, int) {
	var data []byte
	for _, r := range ranges {
		data = binary.BigEndian.AppendUint32(data, r[0])
		data = binary.BigEndian.AppendUint32(data, r[1])
	}
	return string(data), len(ranges)
}

func blockerTestIp4(s string) uint32 {
	b := netip.MustParseAddr(s).As4()
	return binary.BigEndian.Uint32(b[:])
}

// blockerTestNew builds an enabled blocker over hand-built tables. v4 and v6
// ranges must be sorted and disjoint (like generator output). v6 ranges reuse
// packV6 from ip_security_cfaa_test.go.
func blockerTestNew(names []string, r4 [][2]uint32, r6 [][2][16]byte) *dataBlocker {
	hostData, hostCount := blockerTestHostTable(blockerTestPepper, names)
	d4, c4 := blockerTestPack4(r4)
	d6, c6 := packV6(r6)
	b := newBlockerWithData(blockerTestPepper, hostData, hostCount, d4, c4, d6, c6)
	b.SetEnabled(true)
	return b
}

// blockerTestEntries is the hand-built blocked set used across the host tests.
// entries are stored normalized (lower case, punycode), like generator output.
var blockerTestEntries = []string{
	"ads.example.com",
	"tracker.net",
	"x.y.z.deep.org",
	"xn--bcher-kva.example", // bücher.example
	"localhost",
}

// blockerTestNormalize and blockerTestReference are an independent
// implementation of the documented matching semantics, used by the fuzz and
// table tests: normalize (strip one trailing dot, ascii lower, punycode,
// reject malformed and ip literals), then match the exact name or any label
// suffix with at least two labels.

func blockerTestNormalize(host string) (string, bool) {
	if n := len(host); 0 < n && host[n-1] == '.' {
		host = host[:n-1]
	}
	n := len(host)
	if n == 0 || blockerMaxHostLen < n || host[n-1] == '.' {
		return "", false
	}
	lowered := make([]byte, n)
	ascii := true
	digitsDotsOnly := true
	for i := 0; i < n; i += 1 {
		c := host[i]
		if 'A' <= c && c <= 'Z' {
			c += 'a' - 'A'
		}
		if c < '!' || c == 0x7f || c == ':' {
			return "", false
		}
		if c == '.' && (i == 0 || host[i-1] == '.') {
			return "", false
		}
		if c != '.' && (c < '0' || '9' < c) {
			digitsDotsOnly = false
		}
		if 0x80 <= c {
			ascii = false
		}
		lowered[i] = c
	}
	if digitsDotsOnly {
		return "", false
	}
	name := string(lowered)
	if !ascii {
		punycode, err := Punycode(name)
		if err != nil || len(punycode) == 0 || blockerMaxHostLen < len(punycode) {
			return "", false
		}
		name = punycode
	}
	return name, true
}

func blockerTestReference(entries map[string]bool, host string) bool {
	name, ok := blockerTestNormalize(host)
	if !ok {
		return false
	}
	if entries[name] {
		return true
	}
	labels := strings.Split(name, ".")
	for i := 1; i <= len(labels)-2; i += 1 {
		if entries[strings.Join(labels[i:], ".")] {
			return true
		}
	}
	return false
}

func TestBlockerHostWalk(t *testing.T) {
	b := blockerTestNew(blockerTestEntries, nil, nil)

	longBlocked := strings.Repeat("a.", 100) + "ads.example.com"
	longUnblocked := strings.Repeat("a.", 100) + "ads.example.org"

	cases := []struct {
		host string
		want bool
	}{
		// exact
		{"ads.example.com", true},
		{"tracker.net", true},
		{"x.y.z.deep.org", true},
		// subdomains of a blocked hostname are blocked
		{"sub.ads.example.com", true},
		{"a.b.c.d.ads.example.com", true},
		{"cdn.tracker.net", true},
		{"w.x.y.z.deep.org", true},
		{longBlocked, true},
		// normalization
		{"ADS.EXAMPLE.COM", true},
		{"ads.example.com.", true},
		{"sub.ads.example.com.", true},
		// idna: query in unicode, entry stored as punycode
		{"xn--bcher-kva.example", true},
		{"bücher.example", true},
		{"sub.bücher.example", true},
		// a single label entry blocks only its exact form
		{"localhost", true},
		{"sub.localhost", false},
		// a blocked hostname never blocks its parents or siblings
		{"example.com", false},
		{"other.example.com", false},
		{"deep.org", false},
		{"z.deep.org", false},
		{"y.z.deep.org", false},
		// label boundaries: string suffix is not domain suffix
		{"notads.example.com", false},
		{"evilads.example.com", false},
		{"xads.example.com", false},
		{"ample.com", false},
		{"ker.net", false},
		// an entry is a suffix rule, never an infix rule
		{"tracker.net.evil.com", false},
		// bare tld / single label misses
		{"com", false},
		{"net", false},
		{"ads", false},
		{longUnblocked, false},
		// malformed inputs and ip literals return false without panic
		{"", false},
		{".", false},
		{"..", false},
		{".ads.example.com", false},
		{"ads..example.com", false},
		{"ads.example.com..", false},
		{"ads.example.com:443", false},
		{"[2001:db8::1]", false},
		{"2001:db8::1", false},
		{"ads example.com", false},
		{"ads.example.com\n", false},
		{"1.2.3.4", false},
		{"127.0.0.1", false},
		{"1.2.3.4.", false},
		{strings.Repeat("a", 260) + ".com", false},
	}
	for _, tc := range cases {
		if got := b.BlockHost(tc.host); got != tc.want {
			t.Errorf("BlockHost(%q) = %v, want %v", tc.host, got, tc.want)
		}
	}

	// the reference model must agree on every case
	set := map[string]bool{}
	for _, e := range blockerTestEntries {
		set[e] = true
	}
	for _, tc := range cases {
		if got := blockerTestReference(set, tc.host); got != tc.want {
			t.Errorf("reference(%q) = %v, want %v", tc.host, got, tc.want)
		}
	}
}

func TestBlockerHostMaxLength(t *testing.T) {
	b := blockerTestNew(blockerTestEntries, nil, nil)

	// exactly 253 chars ending in a blocked base is blocked
	base := "ads.example.com"
	pad := blockerMaxHostLen - len(base) - 1
	label := strings.Repeat("a", pad)
	host := label + "." + base
	if len(host) != blockerMaxHostLen {
		t.Fatalf("bad construction: len %d", len(host))
	}
	if !b.BlockHost(host) {
		t.Fatalf("max length blocked host missed")
	}
	// one over the limit returns false
	if b.BlockHost("a" + host) {
		t.Fatalf("over-limit host matched")
	}
	// a trailing dot on a max length host is stripped first, so it still matches
	if !b.BlockHost(host + ".") {
		t.Fatalf("max length host with trailing dot missed")
	}
}

func TestBlockerEnabled(t *testing.T) {
	hostData, hostCount := blockerTestHostTable(blockerTestPepper, blockerTestEntries)
	d4, c4 := blockerTestPack4([][2]uint32{{blockerTestIp4("203.0.113.0"), blockerTestIp4("203.0.113.255")}})
	b := newBlockerWithData(blockerTestPepper, hostData, hostCount, d4, c4, "", 0)

	// constructed disabled
	if b.Enabled() {
		t.Fatalf("expected disabled at construction")
	}
	if b.BlockHost("ads.example.com") || b.BlockIp(netip.MustParseAddr("203.0.113.9")) {
		t.Fatalf("disabled blocker must not block")
	}
	b.SetEnabled(true)
	if !b.Enabled() {
		t.Fatalf("expected enabled")
	}
	if !b.BlockHost("ads.example.com") || !b.BlockIp(netip.MustParseAddr("203.0.113.9")) {
		t.Fatalf("enabled blocker must block")
	}
	b.SetEnabled(false)
	if b.BlockHost("ads.example.com") || b.BlockIp(netip.MustParseAddr("203.0.113.9")) {
		t.Fatalf("re-disabled blocker must not block")
	}
}

func TestBlockerIp4(t *testing.T) {
	r4 := [][2]uint32{
		{blockerTestIp4("198.51.100.7"), blockerTestIp4("198.51.100.7")},
		{blockerTestIp4("203.0.113.0"), blockerTestIp4("203.0.113.255")},
	}
	b := blockerTestNew(nil, r4, nil)

	cases := []struct {
		addr string
		want bool
	}{
		{"198.51.100.6", false},
		{"198.51.100.7", true},
		{"198.51.100.8", false},
		{"203.0.112.255", false},
		{"203.0.113.0", true},
		{"203.0.113.128", true},
		{"203.0.113.255", true},
		{"203.0.114.0", false},
		{"8.8.8.8", false},
		// a v4-mapped v6 address unmaps to its v4 form
		{"::ffff:203.0.113.9", true},
		{"::ffff:8.8.8.8", false},
	}
	for _, tc := range cases {
		if got := b.BlockIp(netip.MustParseAddr(tc.addr)); got != tc.want {
			t.Errorf("BlockIp(%s) = %v, want %v", tc.addr, got, tc.want)
		}
	}
	// the zero Addr
	if b.BlockIp(netip.Addr{}) {
		t.Errorf("zero Addr matched")
	}
}

func TestBlockerIp6(t *testing.T) {
	lo := netip.MustParseAddr("2001:db8::").As16()
	hi := netip.MustParseAddr("2001:db8::ffff").As16()
	b := blockerTestNew(nil, nil, [][2][16]byte{{lo, hi}})

	cases := []struct {
		addr string
		want bool
	}{
		{"2001:db7:ffff:ffff:ffff:ffff:ffff:ffff", false},
		{"2001:db8::", true},
		{"2001:db8::1", true},
		{"2001:db8::ffff", true},
		{"2001:db8::1:0", false},
		{"2606:4700:4700::1111", false},
	}
	for _, tc := range cases {
		if got := b.BlockIp(netip.MustParseAddr(tc.addr)); got != tc.want {
			t.Errorf("BlockIp(%s) = %v, want %v", tc.addr, got, tc.want)
		}
	}
}

func TestBlockerEmptyTables(t *testing.T) {
	b := newBlockerWithData(blockerTestPepper, "", 0, "", 0, "", 0)
	b.SetEnabled(true)
	if b.BlockHost("ads.example.com") {
		t.Fatalf("empty host table matched")
	}
	if b.BlockIp(netip.MustParseAddr("203.0.113.9")) || b.BlockIp(netip.MustParseAddr("2001:db8::1")) {
		t.Fatalf("empty ip tables matched")
	}
	var allocs float64
	addr4 := netip.MustParseAddr("203.0.113.9")
	allocs = testing.AllocsPerRun(1000, func() {
		blockerTestSink = b.BlockHost("ads.example.com") || b.BlockIp(addr4)
	})
	if allocs != 0 {
		t.Fatalf("empty table lookups allocated %v times per run, want 0", allocs)
	}
}

func TestBlockerDataGuards(t *testing.T) {
	assertPanics := func(name string, f func()) {
		t.Helper()
		defer func() {
			if recover() == nil {
				t.Fatalf("%s: expected panic", name)
			}
		}()
		f()
	}
	assertPanics("short pepper", func() {
		newBlockerWithData("short", "", 0, "", 0, "", 0)
	})
	assertPanics("host data length", func() {
		newBlockerWithData(blockerTestPepper, "abc", 1, "", 0, "", 0)
	})
	assertPanics("v4 data length", func() {
		newBlockerWithData(blockerTestPepper, "", 0, "abc", 1, "", 0)
	})
	assertPanics("v6 data length", func() {
		newBlockerWithData(blockerTestPepper, "", 0, "", 0, "abc", 1)
	})
}

func TestBlockerZeroAlloc(t *testing.T) {
	r4 := [][2]uint32{{blockerTestIp4("203.0.113.0"), blockerTestIp4("203.0.113.255")}}
	lo := netip.MustParseAddr("2001:db8::").As16()
	hi := netip.MustParseAddr("2001:db8::ffff").As16()
	b := blockerTestNew(blockerTestEntries, r4, [][2][16]byte{{lo, hi}})

	addr4 := netip.MustParseAddr("203.0.113.9")
	addr4Miss := netip.MustParseAddr("8.8.8.8")
	addr6 := netip.MustParseAddr("2001:db8::1")

	for name, f := range map[string]func(){
		"host hit at base": func() { blockerTestSink = b.BlockHost("a.b.ads.example.com") },
		"host exact hit":   func() { blockerTestSink = b.BlockHost("ads.example.com") },
		"host miss":        func() { blockerTestSink = b.BlockHost("a.b.c.unblocked.example.org") },
		"host upper case":  func() { blockerTestSink = b.BlockHost("A.B.ADS.EXAMPLE.COM") },
		"host invalid":     func() { blockerTestSink = b.BlockHost("ads..example.com") },
		"ip4 hit":          func() { blockerTestSink = b.BlockIp(addr4) },
		"ip4 miss":         func() { blockerTestSink = b.BlockIp(addr4Miss) },
		"ip6 hit":          func() { blockerTestSink = b.BlockIp(addr6) },
	} {
		if allocs := testing.AllocsPerRun(1000, f); allocs != 0 {
			t.Errorf("%s: allocated %v times per run, want 0", name, allocs)
		}
	}
}

// TestBlockerFalsePositiveProbe drives a large volume of pseudo-random,
// never-listed hostnames through the truncated-hash lookup: none may match.
func TestBlockerFalsePositiveProbe(t *testing.T) {
	b := blockerTestNew(blockerTestEntries, nil, nil)
	count := 1_000_000
	if testing.Short() {
		count = 100_000
	}
	x := uint64(0x243F6A8885A308D3)
	hits := 0
	for i := 0; i < count; i += 1 {
		x = x*6364136223846793005 + 1442695040888963407
		host := fmt.Sprintf("h%016x.p%08x.probe.test", x, uint32(x>>13))
		if b.BlockHost(host) {
			hits += 1
			t.Logf("false positive: %s", host)
		}
	}
	if hits != 0 {
		t.Fatalf("%d false positives in %d probes", hits, count)
	}
}

func TestBlockerToggleRace(t *testing.T) {
	r4 := [][2]uint32{{blockerTestIp4("203.0.113.0"), blockerTestIp4("203.0.113.255")}}
	b := blockerTestNew(blockerTestEntries, r4, nil)
	addr := netip.MustParseAddr("203.0.113.9")

	done := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r += 1 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// goroutine-local sink: the shared package sink would itself race
			local := false
			for {
				select {
				case <-done:
					if local {
						_ = local
					}
					return
				default:
				}
				local = local != b.BlockHost("sub.ads.example.com")
				local = local != b.BlockIp(addr)
				local = local != b.Enabled()
			}
		}()
	}
	for i := 0; i < 10_000; i += 1 {
		b.SetEnabled(i%2 == 0)
	}
	close(done)
	wg.Wait()
}

// TestBlockerGeneratedTables asserts the structural invariants of the
// committed generated data (ip_blocker_block.go).
func TestBlockerGeneratedTables(t *testing.T) {
	const minimumReviewedHostCount = 120_000
	if blockerBlockedHostCount < minimumReviewedHostCount {
		t.Fatalf("host records = %d, want at least reviewed minimum %d", blockerBlockedHostCount, minimumReviewedHostCount)
	}
	if len(blockerPepper) != blockerPepperLen {
		t.Fatalf("pepper length %d, want %d", len(blockerPepper), blockerPepperLen)
	}
	if len(blockerBlockedHostData) != blockerBlockedHostCount*blockerHostRecordLen {
		t.Fatalf("host data length %d, want %d", len(blockerBlockedHostData), blockerBlockedHostCount*blockerHostRecordLen)
	}
	for i := 1; i < blockerBlockedHostCount; i += 1 {
		prev := beN(blockerBlockedHostData, (i-1)*blockerHostRecordLen, blockerHostRecordLen)
		cur := beN(blockerBlockedHostData, i*blockerHostRecordLen, blockerHostRecordLen)
		if prev >= cur {
			t.Fatalf("host record %d: not strictly increasing", i)
		}
	}
	// the embedded tables are what ship in the sdk binary; the generator
	// enforces a size budget (blocker/main.go -max-bytes, default 1 MiB)
	embedded := len(blockerPepper) + len(blockerBlockedHostData) +
		len(blockerBlockedPrefixData) + len(blockerBlockedPrefix6Data)
	if 1<<20 < embedded {
		t.Fatalf("embedded blocker tables %d bytes exceed the 1 MiB size budget", embedded)
	}

	if len(blockerBlockedPrefixData) != blockerBlockedPrefixCount*8 {
		t.Fatalf("v4 data length %d, want %d", len(blockerBlockedPrefixData), blockerBlockedPrefixCount*8)
	}
	u32 := func(o int) uint32 {
		return uint32(blockerBlockedPrefixData[o])<<24 | uint32(blockerBlockedPrefixData[o+1])<<16 |
			uint32(blockerBlockedPrefixData[o+2])<<8 | uint32(blockerBlockedPrefixData[o+3])
	}
	var prevHi uint32
	for i := 0; i < blockerBlockedPrefixCount; i += 1 {
		o := i << 3
		lo, hi := u32(o), u32(o+4)
		if hi < lo {
			t.Fatalf("v4 range %d: lo > hi", i)
		}
		if 0 < i && lo <= prevHi {
			t.Fatalf("v4 range %d: not strictly after previous (unsorted/overlapping)", i)
		}
		prevHi = hi
	}

	if len(blockerBlockedPrefix6Data) != blockerBlockedPrefix6Count*32 {
		t.Fatalf("v6 data length %d, want %d", len(blockerBlockedPrefix6Data), blockerBlockedPrefix6Count*32)
	}
	var prevHiHi, prevHiLo uint64
	for i := 0; i < blockerBlockedPrefix6Count; i += 1 {
		o := i * 32
		loHi := be64(blockerBlockedPrefix6Data, o)
		loLo := be64(blockerBlockedPrefix6Data, o+8)
		hiHi := be64(blockerBlockedPrefix6Data, o+16)
		hiLo := be64(blockerBlockedPrefix6Data, o+24)
		if hiHi < loHi || (hiHi == loHi && hiLo < loLo) {
			t.Fatalf("v6 range %d: lo > hi", i)
		}
		if 0 < i && (loHi < prevHiHi || (loHi == prevHiHi && loLo <= prevHiLo)) {
			t.Fatalf("v6 range %d: not strictly after previous (unsorted/overlapping)", i)
		}
		prevHiHi, prevHiLo = hiHi, hiLo
	}
}

var blockerFuzzState = sync.OnceValues(func() (*dataBlocker, map[string]bool) {
	b := blockerTestNew(blockerTestEntries, nil, nil)
	set := map[string]bool{}
	for _, e := range blockerTestEntries {
		set[e] = true
	}
	return b, set
})

// FuzzBlockHost cross-checks BlockHost against the independent reference
// model over arbitrary inputs: they must agree, and neither may panic.
func FuzzBlockHost(f *testing.F) {
	seeds := append([]string{}, blockerTestEntries...)
	seeds = append(seeds,
		"sub.ads.example.com",
		"EVILADS.example.com",
		"ads.example.com.",
		"bücher.example",
		"sub.bücher.example",
		"tracker.net.evil.com",
		"..",
		".ads.example.com",
		"1.2.3.4",
		"ads.example.com:443",
		strings.Repeat("a.", 120)+"com",
		strings.Repeat("a", 300),
		"\x00ads.example.com",
		"ads.example.com\xff",
	)
	for _, s := range seeds {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, host string) {
		b, set := blockerFuzzState()
		got := b.BlockHost(host)
		want := blockerTestReference(set, host)
		if got != want {
			t.Fatalf("BlockHost(%q) = %v, reference = %v", host, got, want)
		}
	})
}

func BenchmarkBlockerHostHitBase(b *testing.B) {
	blocker := blockerTestNew(blockerTestEntries, nil, nil)
	b.ReportAllocs()
	for i := 0; i < b.N; i += 1 {
		blockerTestSink = blocker.BlockHost("deep.sub.ads.example.com")
	}
}

func BenchmarkBlockerHostMiss5Labels(b *testing.B) {
	blocker := blockerTestNew(blockerTestEntries, nil, nil)
	b.ReportAllocs()
	for i := 0; i < b.N; i += 1 {
		blockerTestSink = blocker.BlockHost("a.b.c.example.org")
	}
}

func BenchmarkBlockerHostMiss10Labels(b *testing.B) {
	blocker := blockerTestNew(blockerTestEntries, nil, nil)
	host := strings.Repeat("a.", 9) + "example.org"
	b.ReportAllocs()
	for i := 0; i < b.N; i += 1 {
		blockerTestSink = blocker.BlockHost(host)
	}
}

func BenchmarkBlockerIp4(b *testing.B) {
	r4 := [][2]uint32{{blockerTestIp4("203.0.113.0"), blockerTestIp4("203.0.113.255")}}
	blocker := blockerTestNew(nil, r4, nil)
	addr := netip.MustParseAddr("8.8.8.8")
	b.ReportAllocs()
	for i := 0; i < b.N; i += 1 {
		blockerTestSink = blocker.BlockIp(addr)
	}
}

// TestBlockerDefaultDataSmoke drives the committed generated data end to end
// through NewBlockerWithDefaults. individual list churn must not flake the
// build, so it requires only a majority of evergreen ad/tracking domains to
// hit — but a broken pipeline (empty or mis-hashed data) fails hard.
func TestBlockerDefaultDataSmoke(t *testing.T) {
	if blockerBlockedHostCount == 0 {
		t.Fatal("default blocker data is empty (run and review: go generate ./blocker)")
	}
	b := NewBlockerWithDefaults()
	if b.Enabled() {
		t.Fatalf("default blocker must start disabled")
	}
	b.SetEnabled(true)

	evergreen := []string{
		"doubleclick.net",
		"googlesyndication.com",
		"googleadservices.com",
		"adnxs.com",
		"scorecardresearch.com",
		"ads.pubmatic.com",
	}
	hits := 0
	for _, host := range evergreen {
		if b.BlockHost(host) {
			hits += 1
		} else {
			t.Logf("evergreen domain not in current data: %s", host)
		}
	}
	if hits < len(evergreen)/2+1 {
		t.Fatalf("only %d/%d evergreen ad domains blocked — generated data looks broken", hits, len(evergreen))
	}
	// subdomain semantics on real data
	if b.BlockHost("doubleclick.net") && !b.BlockHost("x.y.doubleclick.net") {
		t.Fatalf("subdomain of a blocked base not blocked")
	}
	// infrastructure never blocked
	for _, host := range []string{"ur.network", "api.ur.network", "example.com", "en.wikipedia.org"} {
		if b.BlockHost(host) {
			t.Fatalf("%s is blocked in the default data", host)
		}
	}
}

// TestBlockerHashVectors pins the runtime hash construction to the shared
// golden vectors (blocker/testdata/hash_vectors.txt), which the generator
// tests also assert — generator/runtime agreement without a cross-package
// import.
func TestBlockerHashVectors(t *testing.T) {
	data, err := os.ReadFile("blocker/testdata/hash_vectors.txt")
	if err != nil {
		t.Fatalf("read vectors: %v", err)
	}
	pepper := ""
	names := []string{}
	want := map[string]uint64{}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 2 {
			t.Fatalf("bad vector line: %q", line)
		}
		if fields[0] == "pepper-hex" {
			raw, err := hex.DecodeString(fields[1])
			if err != nil {
				t.Fatalf("bad pepper hex: %v", err)
			}
			pepper = string(raw)
			continue
		}
		key, err := strconv.ParseUint(fields[1], 16, 64)
		if err != nil {
			t.Fatalf("bad key on line %q: %v", line, err)
		}
		names = append(names, fields[0])
		want[fields[0]] = key
	}
	if len(pepper) != blockerPepperLen {
		t.Fatalf("vector pepper length %d, want %d", len(pepper), blockerPepperLen)
	}
	if len(names) == 0 {
		t.Fatalf("no vector rows")
	}
	// the test-helper hash (independent of the runtime stack-buffer path)
	// agrees with the vectors
	for name, key := range want {
		if got := blockerTestHostKey(pepper, name); got != key {
			t.Errorf("hash(%s) = %016x, want %016x", name, got, key)
		}
	}
	// and a blocker over the packed vector keys blocks every vector name via
	// the real BlockHost path
	hostData, hostCount := blockerTestHostTable(pepper, names)
	b := newBlockerWithData(pepper, hostData, hostCount, "", 0, "", 0)
	b.SetEnabled(true)
	for _, name := range names {
		if !b.BlockHost(name) {
			t.Errorf("vector name %s not blocked", name)
		}
	}
}
