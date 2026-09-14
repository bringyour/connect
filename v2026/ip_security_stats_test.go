// Security-policy statistics tests keep diagnostic accounting bounded without
// changing exact low-cardinality counts. Every test runs under both address
// families: the collector keys by version, so the bounds must hold per family.
package connect

import (
	"net"
	"testing"
)

// testStatsDestinationIp is the destination address the stats tests key on
// for a family.
func testStatsDestinationIp(ipVersion int) net.IP {
	if ipVersion == 6 {
		return net.ParseIP("2001:db8::1")
	}
	return net.IPv4(203, 0, 113, 1)
}

// testStatsSourceIp is the source address the stats tests key on for a family.
func testStatsSourceIp(ipVersion int) net.IP {
	if ipVersion == 6 {
		return net.ParseIP("2001:db8::2")
	}
	return net.IPv4(198, 51, 100, 2)
}

// TestSecurityPolicyStatsCollectorBoundsDestinationCardinality verifies that a
// long-running provider cannot retain every ephemeral port it has encountered.
func TestSecurityPolicyStatsCollectorBoundsDestinationCardinality(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		stats := DefaultSecurityPolicyStatsCollector()
		const extraDestinationCount = 100
		for port := 1; port <= securityPolicyStatsMaxDestinationsPerResult+extraDestinationCount; port++ {
			stats.AddDestination(
				&IpPath{
					Version:         ipVersion,
					Protocol:        IpProtocolTcp,
					DestinationIp:   testStatsDestinationIp(ipVersion),
					DestinationPort: port,
				},
				SecurityPolicyResultAllow,
				1,
			)
		}

		snapshot := stats.Stats(false)
		destinationCounts := snapshot[SecurityPolicyResultAllow]
		if count := len(destinationCounts); count != securityPolicyStatsMaxDestinationsPerResult {
			t.Fatalf(
				"destination cardinality = %d, want hard limit %d",
				count,
				securityPolicyStatsMaxDestinationsPerResult,
			)
		}
		firstDestination := SecurityDestination{
			Version:  ipVersion,
			Protocol: IpProtocolTcp,
			Port:     1,
		}
		if count := destinationCounts[firstDestination]; count != 1 {
			t.Fatalf("first exact destination count = %d, want 1", count)
		}
		overflowCount := uint64(extraDestinationCount + 1)
		if count := destinationCounts[securityPolicyStatsOverflowDestination]; count != overflowCount {
			t.Fatalf("overflow count = %d, want %d", count, overflowCount)
		}

		stats.AddDestination(
			&IpPath{
				Version:         ipVersion,
				Protocol:        IpProtocolTcp,
				DestinationIp:   testStatsDestinationIp(ipVersion),
				DestinationPort: 1,
			},
			SecurityPolicyResultAllow,
			2,
		)
		snapshot = stats.Stats(false)
		if count := snapshot[SecurityPolicyResultAllow][firstDestination]; count != 3 {
			t.Fatalf("existing destination count after saturation = %d, want 3", count)
		}
		if count := snapshot[SecurityPolicyResultAllow][securityPolicyStatsOverflowDestination]; count != overflowCount {
			t.Fatalf("existing destination was misclassified as overflow: count = %d, want %d", count, overflowCount)
		}
	})
}

// TestSecurityPolicyStatsCollectorResetRestoresExactCollection verifies that a
// reset clears both the counters and the cardinality budget.
func TestSecurityPolicyStatsCollectorResetRestoresExactCollection(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		stats := DefaultSecurityPolicyStatsCollector()
		for port := 1; port <= securityPolicyStatsMaxDestinationsPerResult+1; port++ {
			stats.AddSource(
				&IpPath{
					Version:    ipVersion,
					Protocol:   IpProtocolUdp,
					SourceIp:   testStatsSourceIp(ipVersion),
					SourcePort: port,
				},
				SecurityPolicyResultAllow,
				1,
			)
		}
		stats.Stats(true)
		if snapshot := stats.Stats(false); len(snapshot) != 0 {
			t.Fatalf("statistics after reset = %v, want empty", snapshot)
		}

		newSource := &IpPath{
			Version:    ipVersion,
			Protocol:   IpProtocolUdp,
			SourceIp:   testStatsSourceIp(ipVersion),
			SourcePort: securityPolicyStatsMaxDestinationsPerResult + 1,
		}
		stats.AddSource(newSource, SecurityPolicyResultAllow, 1)
		destination := newSecuritySourcePort(newSource)
		if count := stats.Stats(false)[SecurityPolicyResultAllow][destination]; count != 1 {
			t.Fatalf("exact destination count after reset = %d, want 1", count)
		}
	})
}

// TestSecurityPolicyStatsCollectorBoundsUnknownResults verifies that arbitrary
// caller-provided result integers share one bounded diagnostic bucket.
func TestSecurityPolicyStatsCollectorBoundsUnknownResults(t *testing.T) {
	forEachIpVersion(t, func(t *testing.T, ipVersion int) {
		stats := DefaultSecurityPolicyStatsCollector()
		for result := 10; result < 10+securityPolicyStatsMaxDestinationsPerResult+100; result++ {
			stats.AddDestination(
				&IpPath{
					Version:         ipVersion,
					Protocol:        IpProtocolTcp,
					DestinationIp:   testStatsDestinationIp(ipVersion),
					DestinationPort: result,
				},
				SecurityPolicyResult(result),
				1,
			)
		}

		snapshot := stats.Stats(false)
		if count := len(snapshot); count != 1 {
			t.Fatalf("result bucket count = %d, want 1", count)
		}
		if count := len(snapshot[securityPolicyStatsUnknownResult]); count != securityPolicyStatsMaxDestinationsPerResult {
			t.Fatalf(
				"unknown-result destination cardinality = %d, want %d",
				count,
				securityPolicyStatsMaxDestinationsPerResult,
			)
		}
	})
}

// the two families never share a bucket: the same port on each family is
// two exact destinations
func TestSecurityPolicyStatsCollectorKeysByFamily(t *testing.T) {
	stats := DefaultSecurityPolicyStatsCollector()
	for _, ipVersion := range testIpVersions {
		stats.AddDestination(
			&IpPath{
				Version:         ipVersion,
				Protocol:        IpProtocolTcp,
				DestinationIp:   testStatsDestinationIp(ipVersion),
				DestinationPort: 443,
			},
			SecurityPolicyResultAllow,
			1,
		)
	}
	destinationCounts := stats.Stats(false)[SecurityPolicyResultAllow]
	if len(destinationCounts) != 2 {
		t.Fatalf("destinations = %v, want one per family", destinationCounts)
	}
	for _, ipVersion := range testIpVersions {
		key := SecurityDestination{Version: ipVersion, Protocol: IpProtocolTcp, Port: 443}
		if destinationCounts[key] != 1 {
			t.Fatalf("v%d destination count = %d, want 1", ipVersion, destinationCounts[key])
		}
	}
}
