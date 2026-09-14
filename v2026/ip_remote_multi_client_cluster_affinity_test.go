package connect

import (
	"net"
	"net/netip"
	"testing"
)

// the representative must be stable across calls and independent of the order
// members arrive in, or a cluster's ips would each key differently and the
// grouping would achieve nothing
func TestClusterAffinityRepresentativeIsStable(t *testing.T) {
	a := netip.MustParseAddr("93.184.216.34")
	b := netip.MustParseAddr("93.184.216.7")
	c := netip.MustParseAddr("151.101.1.140")

	rep1, ok := clusterAffinityRepresentative([]netip.Addr{a, b, c})
	AssertEqual(t, ok, true)

	rep2, ok := clusterAffinityRepresentative([]netip.Addr{c, a, b})
	AssertEqual(t, ok, true)
	AssertEqual(t, rep1.String(), rep2.String())

	rep3, ok := clusterAffinityRepresentative([]netip.Addr{b, c, a})
	AssertEqual(t, ok, true)
	AssertEqual(t, rep1.String(), rep3.String())

	// netip.Addr.Less orders byte-wise, not lexically by string form: the
	// first octets are 93, 93 and 151, so the minimum is the lower of the two
	// 93.184.216.x, and 151.101.1.140 is the largest despite sorting first as
	// a string
	AssertEqual(t, rep1.String(), b.String())
}

// an ip in no multi-member cluster leaves the caller on per-ip affinity
func TestClusterAffinityRepresentativeEmpty(t *testing.T) {
	_, ok := clusterAffinityRepresentative(nil)
	AssertEqual(t, ok, false)

	_, ok = clusterAffinityRepresentative([]netip.Addr{})
	AssertEqual(t, ok, false)
}

// a single-member cluster is still stable, and resolves to itself
func TestClusterAffinityRepresentativeSingle(t *testing.T) {
	a := netip.MustParseAddr("93.184.216.34")
	rep, ok := clusterAffinityRepresentative([]netip.Addr{a})
	AssertEqual(t, ok, true)
	AssertEqual(t, rep.String(), a.String())
}

// v4-mapped v6 forms must collapse to the same representative as their v4 form,
// since ipAssoc keys on the unmapped address
func TestClusterAffinityRepresentativeUnmaps(t *testing.T) {
	v4 := netip.MustParseAddr("93.184.216.34")
	mapped := netip.AddrFrom16(v4.As16())

	rep, ok := clusterAffinityRepresentative([]netip.Addr{mapped})
	AssertEqual(t, ok, true)
	AssertEqual(t, rep.String(), v4.String())
}

// affinityIpPathsWithLock is exercised from bare clients across the suite, so
// the cluster fallback must tolerate an unset settings rather than panic
func TestClusterAffinityBareClientDoesNotPanic(t *testing.T) {
	mc := &RemoteUserNatMultiClient{}
	mc.config.Store(&multiClientConfig{})

	paths := mc.affinityIpPathsWithLock(&IpPath{
		Version:         4,
		Protocol:        IpProtocolTcp,
		DestinationIp:   net.ParseIP("93.184.216.34"),
		DestinationPort: 443,
	})

	// falls back to plain per-ip affinity
	AssertEqual(t, len(paths), 1)
	AssertEqual(t, paths[0].DestinationIp.String(), "93.184.216.34")
}

func TestDefaultMultiClientSettingsEnablesClusterAffinityFallback(t *testing.T) {
	AssertEqual(t, DefaultMultiClientSettings().ClusterAffinityFallback, true)
}
