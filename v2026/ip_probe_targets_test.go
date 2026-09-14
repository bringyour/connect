package connect

import (
	"net/netip"
	"slices"
	"testing"
)

// the resolver a probe pass targets follows the exit's address-family
// category: a v6-only exit must never be asked to reach a v4 resolver, and a
// dualstack exit proves both families across consecutive passes
func TestSampleProbeTargetsForIpFamily(t *testing.T) {
	for _, literal := range probeResolverIpv6s {
		addr, err := netip.ParseAddr(literal)
		if err != nil || !addr.Is6() || addr.Is4In6() {
			t.Fatalf("v6 resolver %q is not a native v6 literal: %v", literal, err)
		}
	}
	for _, literal := range probeResolverIps {
		addr, err := netip.ParseAddr(literal)
		if err != nil || !addr.Is4() {
			t.Fatalf("v4 resolver %q is not a v4 literal: %v", literal, err)
		}
	}
	for seed := uint64(0); seed < 16; seed += 1 {
		_, resolver4 := sampleProbeTargetsForIpFamily(seed, 4, IpFamilyV4Only)
		if !slices.Contains(probeResolverIps, resolver4) {
			t.Fatalf("seed %d v4-only resolver %s is not in the v4 list", seed, resolver4)
		}
		_, resolverLegacy := sampleProbeTargetsForIpFamily(seed, 4, IpFamilyLegacy)
		if resolverLegacy != resolver4 {
			t.Fatalf("seed %d legacy resolver %s != v4-only resolver %s", seed, resolverLegacy, resolver4)
		}
		_, resolverDefault := sampleProbeTargets(seed, 4)
		if resolverDefault != resolver4 {
			t.Fatalf("seed %d default resolver %s != v4-only resolver %s", seed, resolverDefault, resolver4)
		}
		_, resolver6 := sampleProbeTargetsForIpFamily(seed, 4, IpFamilyV6Only)
		if !slices.Contains(probeResolverIpv6s, resolver6) {
			t.Fatalf("seed %d v6-only resolver %s is not in the v6 list", seed, resolver6)
		}
		_, resolverDual := sampleProbeTargetsForIpFamily(seed, 4, IpFamilyDualstack)
		if seed%2 == 1 {
			if !slices.Contains(probeResolverIpv6s, resolverDual) {
				t.Fatalf("seed %d dualstack resolver %s should be v6", seed, resolverDual)
			}
		} else if !slices.Contains(probeResolverIps, resolverDual) {
			t.Fatalf("seed %d dualstack resolver %s should be v4", seed, resolverDual)
		}
	}
	// the host sample is independent of the family
	hosts4, _ := sampleProbeTargetsForIpFamily(9, 5, IpFamilyV4Only)
	hosts6, _ := sampleProbeTargetsForIpFamily(9, 5, IpFamilyV6Only)
	if !slices.Equal(hosts4, hosts6) {
		t.Fatalf("host sample differs by family: %v vs %v", hosts4, hosts6)
	}
}

// The per-version sampler draws the resolver from the version's list, the
// host sample is the same either way, and every sibling literal is a native
// v6 address of the same :443 operator.
func TestSampleProbeTargetsForIpVersion(t *testing.T) {
	for seed := uint64(0); seed < 8; seed += 1 {
		hosts4, resolver4 := sampleProbeTargetsForIpVersion(seed, 5, 4)
		hosts6, resolver6 := sampleProbeTargetsForIpVersion(seed, 5, 6)
		if !slices.Equal(hosts4, hosts6) {
			t.Fatalf("seed %d host sample differs by version: %v vs %v", seed, hosts4, hosts6)
		}
		if !slices.Contains(probeResolverIps, resolver4) {
			t.Fatalf("seed %d v4 resolver %s is not in the v4 list", seed, resolver4)
		}
		if !slices.Contains(probeResolverIpv6s, resolver6) {
			t.Fatalf("seed %d v6 resolver %s is not in the v6 list", seed, resolver6)
		}
		// the family-keyed form is the version-keyed form under the family rule
		_, resolverFamily := sampleProbeTargetsForIpFamily(seed, 5, IpFamilyDualstack)
		want := resolver4
		if probeIpVersionForFamily(seed, IpFamilyDualstack) == 6 {
			want = resolver6
		}
		if resolverFamily != want {
			t.Fatalf("seed %d dualstack resolver %s, want %s", seed, resolverFamily, want)
		}
	}
	for v4, v6 := range probeHostLiteralIpv6s {
		if !slices.Contains(probeHostNames, v4) {
			t.Fatalf("sibling map names %s, which is not a table host", v4)
		}
		addr, err := netip.ParseAddr(v6)
		if err != nil || !addr.Is6() || addr.Is4In6() {
			t.Fatalf("sibling %s of %s is not a native v6 literal: %v", v6, v4, err)
		}
		if ip, ok := probeLiteralHostIp(v4, 6); !ok || ip.String() != addr.String() {
			t.Fatalf("probeLiteralHostIp(%s, 6) = %v %v, want %s", v4, ip, ok, v6)
		}
		if ip, ok := probeLiteralHostIp(v4, 4); !ok || ip.String() != v4 {
			t.Fatalf("probeLiteralHostIp(%s, 4) = %v %v, want itself", v4, ip, ok)
		}
	}
}
