package connect

import "strings"

const (
	// Bound untrusted discovery metadata before retaining it in every live
	// window channel. External probes normally publish only a handful of short
	// labels; these limits leave ample headroom without making reputation a
	// new long-session memory vector.
	providerReputationFailureMaxCount = 32
	providerReputationFailureMaxBytes = 4096

	// 40 Mbit/s in decimal bytes/s. A same-network provider is allowed to
	// narrow a fresh quality race only after the server has measured at least
	// this throughput and placed it in the best tier. Unknown/slower peers keep
	// participating through the ordinary public quality field instead.
	fastSameNetworkBytesPerSecond ByteCount = 5_000_000
)

// normalizeProviderReputationFailures parses the existing comma-joined
// external-probe result once, at discovery. The packet path then performs a
// bounded slice scan and never splits strings or interprets TLS plaintext.
func normalizeProviderReputationFailures(raw string) []string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	if providerReputationFailureMaxBytes < len(raw) {
		raw = raw[:providerReputationFailureMaxBytes]
	}

	parts := strings.Split(raw, ",")
	failures := make([]string, 0, min(len(parts), providerReputationFailureMaxCount))
	for _, part := range parts {
		failure := strings.ToLower(strings.TrimSpace(part))
		failure = strings.TrimPrefix(failure, "*.")
		failure = strings.TrimSuffix(failure, ".")
		if failure == "" || 253 < len(failure) {
			continue
		}
		if strings.Contains(failure, ".") {
			// Discovery is the cold boundary. Canonicalize domain probe labels
			// once here so fresh placement compares its already-canonical flow
			// keys without public-suffix parsing or allocation.
			failure = affinityNameForServerName(failure)
		}
		duplicate := false
		for _, existing := range failures {
			if existing == failure {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		failures = append(failures, failure)
		if providerReputationFailureMaxCount <= len(failures) {
			break
		}
	}
	if len(failures) == 0 {
		return nil
	}
	return failures
}

func reputationFailureMatchesServerName(failure string, serverName string) bool {
	serverName = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(serverName), "."))
	if failure == "" || serverName == "" {
		return false
	}
	return reputationFailureMatchesAffinityName(failure, affinityNameForServerName(serverName))
}

// reputationFailureMatchesAffinityName is the allocation-free placement
// form. Both inputs were canonicalized at their cold boundaries.
func reputationFailureMatchesAffinityName(failure string, affinityName string) bool {
	if failure == "" || affinityName == "" {
		return false
	}
	if strings.Contains(failure, ".") {
		return failure == affinityName
	}

	// Existing reputation probes use compact vendor labels (for example
	// "canva"). Match those only to the first label of the canonical site,
	// so a label cannot accidentally match an unrelated substring.
	firstLabel := affinityName
	if dot := strings.IndexByte(firstLabel, '.'); 0 <= dot {
		firstLabel = firstLabel[:dot]
	}
	return failure == firstLabel
}

func clientFailsServerNameReputation(client *multiClientChannel, serverNames []string) bool {
	if client == nil || client.args == nil || len(client.args.ReputationFailures) == 0 {
		return false
	}
	for _, failure := range client.args.ReputationFailures {
		for _, serverName := range serverNames {
			if reputationFailureMatchesServerName(failure, serverName) {
				return true
			}
		}
	}
	return false
}

func clientFailsAffinityPathReputation(client *multiClientChannel, affinityPaths []*IpPath) bool {
	if client == nil || client.args == nil || len(client.args.ReputationFailures) == 0 {
		return false
	}
	for _, path := range affinityPaths {
		if path == nil || path.ServerName == "" {
			continue
		}
		for _, failure := range client.args.ReputationFailures {
			if reputationFailureMatchesAffinityName(failure, path.ServerName) {
				return true
			}
		}
	}
	return false
}

func clientFailsUpdateReputation(client *multiClientChannel, update *multiClientChannelUpdate) bool {
	if client == nil || client.args == nil || update == nil || len(client.args.ReputationFailures) == 0 {
		return false
	}
	for _, failure := range client.args.ReputationFailures {
		for path := range update.affinityIp4Paths {
			if reputationFailureMatchesAffinityName(failure, path.ServerName) {
				return true
			}
		}
		for path := range update.affinityIp6Paths {
			if reputationFailureMatchesAffinityName(failure, path.ServerName) {
				return true
			}
		}
	}
	return false
}

// filterReputationFailedCandidates removes externally-known bad exits only
// from an unbound flow's race field. Callers reach this method after
// sendUpdate has declined DNS/site/app affinity, so an established media
// session is never moved between egress IPs. If every available exit carries
// the same negative result, availability wins and the original field is kept.
func (self *RemoteUserNatMultiClient) filterReputationFailedCandidates(
	candidates []*multiClientChannel,
	update *multiClientChannelUpdate,
) []*multiClientChannel {
	if len(candidates) < 2 || update == nil {
		return candidates
	}

	goodCount := 0
	for _, candidate := range candidates {
		if !clientFailsUpdateReputation(candidate, update) {
			goodCount++
		}
	}
	if goodCount == 0 || goodCount == len(candidates) {
		return candidates
	}
	// The race field is placement-local. Compact it in place so a known-bad
	// provider does not add garbage pressure to the very fresh-flow path this
	// policy is intended to accelerate.
	filtered := candidates[:0]
	for _, candidate := range candidates {
		if !clientFailsUpdateReputation(candidate, update) {
			filtered = append(filtered, candidate)
		}
	}
	return filtered
}

func (self *RemoteUserNatMultiClient) qualityPreferredFlow(ipPath *IpPath, appId string) bool {
	if ipPath == nil {
		return false
	}
	if ipPath.DestinationPort == 443 {
		return true
	}
	if p := self.flowClassifier.Load(); p != nil {
		return classifyOrUnknown(*p, ipPath, appId).Class == ClassStreaming
	}
	return false
}

// preferFastSameNetworkCandidates makes the P2P/residential preference
// load-bearing despite the default full-field race: for a fresh quality-first
// web/media flow, it narrows to same-network exits only when the server's own
// measurements show best-tier quality and at least 40 Mbit/s. Established
// affinity bypasses this method, and an unmeasured peer never displaces the
// normal public field.
func (self *RemoteUserNatMultiClient) preferFastSameNetworkCandidates(
	candidates []*multiClientChannel,
	ipPath *IpPath,
	appId string,
) []*multiClientChannel {
	if len(candidates) < 2 || !self.qualityPreferredFlow(ipPath, appId) {
		return candidates
	}
	qualifiedCount := 0
	for _, candidate := range candidates {
		if candidate != nil && candidate.args != nil &&
			candidate.args.NetworkOnly && candidate.args.Tier == 0 &&
			fastSameNetworkBytesPerSecond <= candidate.args.EstimatedBytesPerSecond {
			qualifiedCount++
		}
	}
	if qualifiedCount == 0 || qualifiedCount == len(candidates) {
		return candidates
	}
	// As above, candidates is a placement-local race field.
	preferred := candidates[:0]
	for _, candidate := range candidates {
		if candidate != nil && candidate.args != nil &&
			candidate.args.NetworkOnly && candidate.args.Tier == 0 &&
			fastSameNetworkBytesPerSecond <= candidate.args.EstimatedBytesPerSecond {
			preferred = append(preferred, candidate)
		}
	}
	return preferred
}
