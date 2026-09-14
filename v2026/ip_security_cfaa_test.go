package connect

import (
	"context"
	"encoding/binary"
	"net"
	"testing"

	"github.com/urnetwork/connect/v2026/protocol"
)

func cfaaVerdictName(v cfaaVerdict) string {
	switch v {
	case cfaaPass:
		return "pass"
	case cfaaAllow:
		return "allow"
	case cfaaDrop:
		return "drop"
	default:
		return "?"
	}
}

func ip4u(a, b, c, d byte) uint32 {
	return uint32(a)<<24 | uint32(b)<<16 | uint32(c)<<8 | uint32(d)
}

// cfaaRangeAt decodes the i-th packed [lo,hi] range from cfaaBlockedPrefixData.
func cfaaRangeAt(i int) (lo, hi uint32) {
	o := i * 8
	lo = uint32(cfaaBlockedPrefixData[o])<<24 | uint32(cfaaBlockedPrefixData[o+1])<<16 |
		uint32(cfaaBlockedPrefixData[o+2])<<8 | uint32(cfaaBlockedPrefixData[o+3])
	hi = uint32(cfaaBlockedPrefixData[o+4])<<24 | uint32(cfaaBlockedPrefixData[o+5])<<16 |
		uint32(cfaaBlockedPrefixData[o+6])<<8 | uint32(cfaaBlockedPrefixData[o+7])
	return
}

func TestCfaaPortClassification(t *testing.T) {
	d := newCfaaDetector(DefaultCfaaSecurityPolicySettings())
	// TEST-NET-3: reserved space, excluded from the table, never in any blocklist
	ip := net.ParseIP("203.0.113.7")
	if cfaaBlockedIp4(ip4u(203, 0, 113, 7)) {
		t.Fatal("reserved TEST-NET-3 address unexpectedly present in generated blocklist")
	}

	cases := []struct {
		port  int
		proto IpProtocol
		want  cfaaVerdict
	}{
		// known bittorrent / abused ports -> drop
		{6881, IpProtocolTcp, cfaaDrop},
		{6889, IpProtocolTcp, cfaaDrop},
		{6969, IpProtocolUdp, cfaaDrop},
		{1337, IpProtocolTcp, cfaaDrop},
		{9337, IpProtocolTcp, cfaaDrop},
		{2710, IpProtocolTcp, cfaaDrop},
		// system ports -> allow (never payload-inspected)
		{123, IpProtocolUdp, cfaaAllow},
		{500, IpProtocolUdp, cfaaAllow},
		{4500, IpProtocolUdp, cfaaAllow},
		// dns: allow over udp, drop over tcp (privileged, not whitelisted for tcp)
		{53, IpProtocolUdp, cfaaAllow},
		{53, IpProtocolTcp, cfaaDrop},
		// web / secure-service ports -> pass (downstream DPI)
		{443, IpProtocolTcp, cfaaPass},
		{443, IpProtocolUdp, cfaaPass},
		{853, IpProtocolTcp, cfaaPass},
		{465, IpProtocolTcp, cfaaPass},
		{587, IpProtocolTcp, cfaaPass},
		{587, IpProtocolUdp, cfaaDrop},
		{993, IpProtocolTcp, cfaaPass},
		{995, IpProtocolTcp, cfaaPass},
		// http: pass over tcp, drop over udp (privileged)
		{80, IpProtocolTcp, cfaaPass},
		{80, IpProtocolUdp, cfaaDrop},
		// other privileged ports -> drop
		{25, IpProtocolTcp, cfaaDrop},
		{25, IpProtocolUdp, cfaaDrop},
		{22, IpProtocolTcp, cfaaDrop},
		{179, IpProtocolTcp, cfaaDrop},
		{595, IpProtocolTcp, cfaaDrop},
		{596, IpProtocolUdp, cfaaDrop},
		{599, IpProtocolTcp, cfaaDrop},
		// user / ephemeral ports -> pass (handed to DPI on egress)
		{8080, IpProtocolTcp, cfaaPass},
		{51413, IpProtocolTcp, cfaaPass},
		{30000, IpProtocolUdp, cfaaPass},
	}
	for _, c := range cases {
		if got := d.inspect(ip, c.port, c.proto, 4); got != c.want {
			t.Errorf("port %d/%s: got %s, want %s", c.port, c.proto, cfaaVerdictName(got), cfaaVerdictName(c.want))
		}
	}
}

func TestCfaaBlockedIps(t *testing.T) {
	d := newCfaaDetector(DefaultCfaaSecurityPolicySettings())
	if cfaaBlockedPrefixCount < 10_000 {
		t.Fatalf("default CFAA IPv4 records = %d, want at least reviewed minimum 10000", cfaaBlockedPrefixCount)
	}

	// sample several blocked ips (the low address of the first ranges); each must
	// drop regardless of port (here an otherwise-allowed inspect port, to prove
	// the ip block takes precedence)
	for i := 0; i < 8 && i < cfaaBlockedPrefixCount; i += 1 {
		lo, _ := cfaaRangeAt(i)
		ip := net.IPv4(byte(lo>>24), byte(lo>>16), byte(lo>>8), byte(lo))
		if got := d.inspect(ip, 443, IpProtocolTcp, 4); got != cfaaDrop {
			t.Errorf("blocked ip %s on port 443: got %s, want drop", ip, cfaaVerdictName(got))
		}
	}

	// Reserved TEST-NET-3 is deterministically subtracted, so it proves the
	// non-blocked path without depending on live feed membership.
	clean := net.IPv4(203, 0, 113, 9)
	if cfaaBlockedIp4(ip4u(203, 0, 113, 9)) {
		t.Fatalf("reserved clean ip %s unexpectedly present in generated blocklist", clean)
	}
	if got := d.inspect(clean, 40000, IpProtocolTcp, 4); cfaaDrop == got {
		t.Errorf("clean ip %s: got drop, want pass/allow", clean)
	}
}

func TestCfaaDisabled(t *testing.T) {
	settings := DefaultCfaaSecurityPolicySettings()
	settings.Enabled = false
	d := newCfaaDetector(settings)
	// even a blocked port passes when the detector is disabled
	if got := d.inspect(net.ParseIP("203.0.113.7"), 6881, IpProtocolTcp, 4); cfaaPass != got {
		t.Errorf("disabled detector on port 6881: got %s, want pass", cfaaVerdictName(got))
	}
}

func TestCfaaIngressMirrorsSourceDrops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := DefaultSecurityPolicy(ctx)

	// blocked source port -> drop (ingress mirrors the egress port policy)
	r, err := policy.InspectIngress(protocol.ProvideMode_Public, dmcaPath(IpProtocolTcp, 6881, 40000, false), nil)
	if err != nil {
		t.Fatal(err)
	}
	if r != SecurityPolicyResultDrop {
		t.Fatalf("ingress source port 6881: got %v, want drop", r)
	}

	// ordinary source port -> allow
	r, _ = policy.InspectIngress(protocol.ProvideMode_Public, dmcaPath(IpProtocolTcp, 443, 40000, false), nil)
	if r != SecurityPolicyResultAllow {
		t.Fatalf("ingress source port 443: got %v, want allow", r)
	}

	// network-mode bypasses the source policy
	r, _ = policy.InspectIngress(protocol.ProvideMode_Network, dmcaPath(IpProtocolTcp, 6881, 40000, false), nil)
	if r != SecurityPolicyResultAllow {
		t.Fatalf("ingress network-mode: got %v, want allow", r)
	}
}

// TestCfaaBlockedPrefixInvariant checks the structural guarantees the binary
// search relies on: the packed data is a whole number of 8-byte records, every
// record has lo<=hi, and records are sorted, disjoint and non-adjacent (proving
// the generator's merge ran).
func TestCfaaBlockedPrefixInvariant(t *testing.T) {
	if cfaaBlockedPrefixCount < 10_000 {
		t.Fatalf("v4 records = %d, want at least reviewed minimum 10000", cfaaBlockedPrefixCount)
	}
	if len(cfaaBlockedPrefixData) != cfaaBlockedPrefixCount*8 {
		t.Fatalf("data length %d, want %d (= %d records * 8)",
			len(cfaaBlockedPrefixData), cfaaBlockedPrefixCount*8, cfaaBlockedPrefixCount)
	}
	var prevHi uint32
	for i := 0; i < cfaaBlockedPrefixCount; i += 1 {
		lo, hi := cfaaRangeAt(i)
		if lo > hi {
			t.Fatalf("range %d: lo %08x > hi %08x", i, lo, hi)
		}
		if 0 < i && uint64(lo) <= uint64(prevHi)+1 {
			t.Fatalf("range %d: lo %08x not strictly after previous hi %08x (unsorted/overlapping/adjacent)", i, lo, prevHi)
		}
		prevHi = hi
	}
}

// TestCfaaBlockedIp4BruteForce cross-checks the binary-search lookup against an
// independent O(n) linear scan over the decoded ranges, on exact boundaries and
// a deterministic pseudo-random sample.
func TestCfaaBlockedIp4BruteForce(t *testing.T) {
	if cfaaBlockedPrefixCount < 10_000 {
		t.Fatalf("default CFAA IPv4 records = %d, want at least reviewed minimum 10000", cfaaBlockedPrefixCount)
	}
	type rng struct{ lo, hi uint32 }
	ranges := make([]rng, cfaaBlockedPrefixCount)
	for i := range ranges {
		lo, hi := cfaaRangeAt(i)
		ranges[i] = rng{lo: lo, hi: hi}
	}
	// deliberately not a binary search, to be independent of the code under test
	contains := func(ip uint32) bool {
		for _, r := range ranges {
			if r.lo <= ip && ip <= r.hi {
				return true
			}
		}
		return false
	}

	// exact boundaries across a spread of ranges
	step := cfaaBlockedPrefixCount/128 + 1
	for i := 0; i < cfaaBlockedPrefixCount; i += step {
		r := ranges[i]
		for _, ip := range []uint32{r.lo, r.hi, r.lo + (r.hi-r.lo)/2} {
			if !cfaaBlockedIp4(ip) {
				t.Fatalf("range %d ip %08x: want blocked", i, ip)
			}
		}
		if 0 < r.lo && cfaaBlockedIp4(r.lo-1) != contains(r.lo-1) {
			t.Fatalf("ip %08x (lo-1): lookup disagrees with reference", r.lo-1)
		}
		if r.hi < 0xffffffff && cfaaBlockedIp4(r.hi+1) != contains(r.hi+1) {
			t.Fatalf("ip %08x (hi+1): lookup disagrees with reference", r.hi+1)
		}
	}

	// deterministic pseudo-random cross-check (LCG, no rand import)
	x := uint32(0x12345678)
	for i := 0; i < 5000; i += 1 {
		x = x*1664525 + 1013904223
		if got, want := cfaaBlockedIp4(x), contains(x); got != want {
			t.Fatalf("ip %08x: cfaaBlockedIp4=%v, reference=%v", x, got, want)
		}
	}
}

// TestCfaaBlockedIp4ZeroAlloc asserts the hot-path lookup performs no heap
// allocation (the table is a read-only string constant searched in place).
func TestCfaaBlockedIp4ZeroAlloc(t *testing.T) {
	var sink bool
	n := testing.AllocsPerRun(1000, func() {
		sink = cfaaBlockedIp4(0x08080808) // 8.8.8.8
	})
	if 0 != n {
		t.Fatalf("cfaaBlockedIp4 allocated %v times per run, want 0", n)
	}
	_ = sink
}

// --- IPv6 ---

func be16Addr(hi, lo uint64) [16]byte {
	var a [16]byte
	binary.BigEndian.PutUint64(a[0:8], hi)
	binary.BigEndian.PutUint64(a[8:16], lo)
	return a
}

func packV6(ranges [][2][16]byte) (string, int) {
	var b []byte
	for _, r := range ranges {
		b = append(b, r[0][:]...)
		b = append(b, r[1][:]...)
	}
	return string(b), len(ranges)
}

func TestCfaaSearch6(t *testing.T) {
	// sorted, disjoint ranges
	ranges := [][2][16]byte{
		{be16Addr(0x2001, 0x0), be16Addr(0x2001, 0xffff)},
		{be16Addr(0x2002, 0x10), be16Addr(0x2002, 0x20)},
		{be16Addr(0x3000_0000_0000_0000, 0x0), be16Addr(0x3000_0000_0000_0000, 0xff)},
	}
	data, count := packV6(ranges)
	hit := func(hi, lo uint64) bool { return searchRange6(data, count, hi, lo) }

	// independent linear reference
	contains := func(hi, lo uint64) bool {
		for _, r := range ranges {
			loHi := binary.BigEndian.Uint64(r[0][0:8])
			loLo := binary.BigEndian.Uint64(r[0][8:16])
			hiHi := binary.BigEndian.Uint64(r[1][0:8])
			hiLo := binary.BigEndian.Uint64(r[1][8:16])
			geLo := loHi < hi || (loHi == hi && loLo <= lo)
			leHi := hi < hiHi || (hi == hiHi && lo <= hiLo)
			if geLo && leHi {
				return true
			}
		}
		return false
	}

	for _, tc := range []struct {
		hi, lo uint64
		want   bool
	}{
		{0x2001, 0x0, true}, {0x2001, 0x8000, true}, {0x2001, 0xffff, true},
		{0x2000, 0xffffffffffffffff, false},
		{0x2002, 0x0f, false}, {0x2002, 0x10, true}, {0x2002, 0x20, true}, {0x2002, 0x21, false},
		{0x0, 0x0, false},
		{0x3000_0000_0000_0000, 0x0, true}, {0x3000_0000_0000_0000, 0xff, true}, {0x3000_0000_0000_0000, 0x100, false},
	} {
		if got := hit(tc.hi, tc.lo); got != tc.want {
			t.Fatalf("hit(%#x,%#x) = %v, want %v", tc.hi, tc.lo, got, tc.want)
		}
	}

	// dense pseudo-random cross-check around the populated hi values
	x := uint64(0x9e3779b97f4a7c15)
	for _, hi := range []uint64{0x2000, 0x2001, 0x2002, 0x2003, 0x3000_0000_0000_0000} {
		for i := 0; i < 2000; i += 1 {
			x = x*6364136223846793005 + 1442695040888963407
			lo := x
			if got, want := hit(hi, lo), contains(hi, lo); got != want {
				t.Fatalf("hi=%#x lo=%#x search=%v ref=%v", hi, lo, got, want)
			}
		}
	}
}

func TestCfaaInspectV6(t *testing.T) {
	d := newCfaaDetector(DefaultCfaaSecurityPolicySettings())
	ip := net.ParseIP("2001:db8::7")
	var clean [16]byte
	copy(clean[:], ip.To16())
	if cfaaBlockedIp6(clean) {
		t.Fatalf("reserved clean IPv6 address %s unexpectedly present in generated blocklist", ip)
	}

	// the port policy applies to ipv6 exactly as to ipv4
	for _, tc := range []struct {
		port  int
		proto IpProtocol
		want  cfaaVerdict
	}{
		{6881, IpProtocolTcp, cfaaDrop},
		{443, IpProtocolTcp, cfaaPass},
		{53, IpProtocolUdp, cfaaAllow},
		{40000, IpProtocolUdp, cfaaPass},
	} {
		if got := d.inspect(ip, tc.port, tc.proto, 6); got != tc.want {
			t.Errorf("v6 port %d/%s -> %s, want %s", tc.port, tc.proto, cfaaVerdictName(got), cfaaVerdictName(tc.want))
		}
	}

	if cfaaBlockedPrefix6Count < 100 {
		t.Fatalf("default CFAA IPv6 records = %d, want at least reviewed minimum 100", cfaaBlockedPrefix6Count)
	}
	data := cfaaBlockedPrefix6Data // a variable, so the slice is runtime-bounded
	var lo [16]byte
	copy(lo[:], data[0:16])
	if got := d.inspect(net.IP(lo[:]), 443, IpProtocolTcp, 6); got != cfaaDrop {
		t.Errorf("blocked v6 %s on 443 -> %s, want drop", net.IP(lo[:]), cfaaVerdictName(got))
	}
}

func TestCfaaBlockedPrefix6Invariant(t *testing.T) {
	if cfaaBlockedPrefix6Count < 100 {
		t.Fatalf("v6 records = %d, want at least reviewed minimum 100", cfaaBlockedPrefix6Count)
	}
	if len(cfaaBlockedPrefix6Data) != cfaaBlockedPrefix6Count*32 {
		t.Fatalf("data length %d, want %d", len(cfaaBlockedPrefix6Data), cfaaBlockedPrefix6Count*32)
	}
	var prevHiHi, prevHiLo uint64
	for i := 0; i < cfaaBlockedPrefix6Count; i += 1 {
		o := i * 32
		loHi := be64(cfaaBlockedPrefix6Data, o)
		loLo := be64(cfaaBlockedPrefix6Data, o+8)
		hiHi := be64(cfaaBlockedPrefix6Data, o+16)
		hiLo := be64(cfaaBlockedPrefix6Data, o+24)
		if hiHi < loHi || (hiHi == loHi && hiLo < loLo) {
			t.Fatalf("range %d: lo > hi", i)
		}
		if 0 < i && (loHi < prevHiHi || (loHi == prevHiHi && loLo <= prevHiLo)) {
			t.Fatalf("range %d: not strictly after previous (unsorted/overlapping)", i)
		}
		prevHiHi, prevHiLo = hiHi, hiLo
	}
}
