package connect

import (
	mathrand "math/rand"
	"slices"

	"maps"
)

// Random extender profile enumeration for discovery (EXTENDER.md A10).
//
// Phase 3 replaces this file with the extender directory: a dialer is then
// built from a signed record, which names the address, the carriers and the
// dns tld. Until then a profile is a guess -- a spoof name from the bundled
// list (SpoofDomains) on one of the three fixed carrier ports -- and the ip it
// is paired with comes from the strategy.

// Fixed carrier ports (A1). The old multi-port personas are removed.
const (
	ExtenderTcpPort  = 443
	ExtenderQuicPort = 443
	ExtenderDnsPort  = 53
)

// randomly enumerate up to n extender profiles
func EnumerateExtenderProfiles(n int, visited map[ExtenderProfile]bool) []ExtenderProfile {
	out := map[ExtenderProfile]bool{}

	spoofDomains := SpoofDomains()
	connectModes := []ExtenderConnectMode{
		ExtenderConnectModeTcpTls,
		ExtenderConnectModeQuic,
		ExtenderConnectModeDns,
	}

	if 0 < len(spoofDomains) && 0 < n {
		maxIterations := 32 * n
		for i := 0; len(out) < n && i < maxIterations; i += 1 {
			connectMode := connectModes[mathrand.Intn(len(connectModes))]
			serverName := spoofDomains[mathrand.Intn(len(spoofDomains))]

			var profile ExtenderProfile
			switch connectMode {
			case ExtenderConnectModeQuic:
				profile = ExtenderProfile{
					ConnectMode: connectMode,
					ServerName:  serverName,
					Port:        ExtenderQuicPort,
				}
			case ExtenderConnectModeDns:
				profile = ExtenderProfile{
					ConnectMode: connectMode,
					ServerName:  serverName,
					Port:        ExtenderDnsPort,
					DnsTld:      DefaultExtenderDnsTld,
				}
			default:
				// fragment and reorder apply to tcp only
				profile = ExtenderProfile{
					ConnectMode: ExtenderConnectModeTcpTls,
					ServerName:  serverName,
					Port:        ExtenderTcpPort,
					Fragment:    mathrand.Intn(2) != 0,
					Reorder:     mathrand.Intn(2) != 0,
				}
			}
			if _, ok := visited[profile]; !ok {
				out[profile] = true
			}
		}
	}

	return slices.Collect(maps.Keys(out))
}
