package connect

import (
	"net/netip"
	"time"
)

const (
	dnsExitHintTtl      = time.Hour
	dnsExitHintMaxCount = 4096
)

type dnsExitHint struct {
	client       *multiClientChannel
	affinityName string
	createTime   time.Time
}

// bindDnsResultToExit joins a successful DoH answer to the provider channel
// that carried its exact TCP flow. It records no queried name or address in
// logs; the canonical name is retained only in the same bounded in-memory
// affinity state that already holds server-name groups.
func (self *RemoteUserNatMultiClient) bindDnsResultToExit(
	dohPath *IpPath,
	domain string,
	addrs []netip.Addr,
) bool {
	if dohPath == nil || domain == "" {
		return false
	}
	if !self.reliabilitySettings().FreshFlowAffinity {
		// Ordinary flows no longer consume hard DNS-exit hints. Avoid retaining
		// provider channel graphs (up to two 4096-entry maps for an hour) for a
		// disabled policy; a runtime A/B enable begins learning from subsequent
		// DNS answers.
		return false
	}
	affinityName := affinityNameForServerName(domain)
	if affinityName == "" {
		return false
	}

	var client *multiClientChannel
	now := time.Now()
	self.stateLock.Lock()
	switch dohPath.Version {
	case 4:
		if update := self.ip4PathUpdates[dohPath.ToIp4Path()]; update != nil && !update.IsDone() {
			client = update.client.Load()
		}
	case 6:
		if update := self.ip6PathUpdates[dohPath.ToIp6Path()]; update != nil && !update.IsDone() {
			client = update.client.Load()
		}
	}
	if client == nil || client.IsDone() || client.affinityDonorEligible(false, 0) != donorEligible {
		self.stateLock.Unlock()
		return false
	}
	if self.dnsExitHints == nil {
		self.dnsExitHints = map[string]dnsExitHint{}
	}
	if self.dnsAddressExitHints == nil {
		self.dnsAddressExitHints = map[netip.Addr]dnsExitHint{}
	}
	if _, found := self.dnsExitHints[affinityName]; !found && dnsExitHintMaxCount <= len(self.dnsExitHints) {
		var oldestName string
		var oldestTime time.Time
		for name, hint := range self.dnsExitHints {
			if now.Sub(hint.createTime) >= dnsExitHintTtl {
				delete(self.dnsExitHints, name)
				continue
			}
			if oldestName == "" || hint.createTime.Before(oldestTime) {
				oldestName, oldestTime = name, hint.createTime
			}
		}
		if dnsExitHintMaxCount <= len(self.dnsExitHints) && oldestName != "" {
			delete(self.dnsExitHints, oldestName)
		}
	}
	hint := dnsExitHint{client: client, affinityName: affinityName, createTime: now}
	self.dnsExitHints[affinityName] = hint
	for _, addr := range addrs {
		addr = addr.Unmap()
		if !addr.IsValid() {
			continue
		}
		if _, found := self.dnsAddressExitHints[addr]; !found && dnsExitHintMaxCount <= len(self.dnsAddressExitHints) {
			var oldestAddr netip.Addr
			var oldestTime time.Time
			for candidateAddr, candidateHint := range self.dnsAddressExitHints {
				if now.Sub(candidateHint.createTime) >= dnsExitHintTtl {
					delete(self.dnsAddressExitHints, candidateAddr)
					continue
				}
				if !oldestAddr.IsValid() || candidateHint.createTime.Before(oldestTime) {
					oldestAddr, oldestTime = candidateAddr, candidateHint.createTime
				}
			}
			if dnsExitHintMaxCount <= len(self.dnsAddressExitHints) && oldestAddr.IsValid() {
				delete(self.dnsAddressExitHints, oldestAddr)
			}
		}
		self.dnsAddressExitHints[addr] = hint
	}
	self.stateLock.Unlock()

	has4, has6 := false, false
	for _, addr := range addrs {
		if addr.Unmap().Is4() {
			has4 = true
		} else if addr.Is6() {
			has6 = true
		}
	}
	answerFamily := "none"
	if has4 && has6 {
		answerFamily = "dual"
	} else if has4 {
		answerFamily = "v4"
	} else if has6 {
		answerFamily = "v6"
	}
	loggerOrDefault(self.log).Infof("%s\n", relEvent(
		"dns_exit_affinity",
		"resolver_family", dohPath.Version,
		"answer_family", answerFamily,
		"exit", client.ClientId(),
		"outcome", "bound",
	))
	return true
}

// inheritDnsExitHintWithLock applies a fresh DNS placement only when the
// flow's ordinary affinity groups have no live donor. A failure remembered for
// this exact destination+port vetoes the hint, allowing SMTP rerace memory to
// outrank topology locality after an exit proves it cannot reach the service.
// The parent stateLock must be held.
func (self *RemoteUserNatMultiClient) inheritDnsExitHintWithLock(
	update *multiClientChannelUpdate,
	ipPath *IpPath,
	affinityPaths []*IpPath,
) bool {
	if update == nil || update.client.Load() != nil || ipPath == nil {
		return false
	}
	if !update.pinned && !self.reliabilitySettings().FreshFlowAffinity {
		// A DNS answer says which exit resolved a name, not which exit will
		// serve it best. Ordinary fresh flows therefore keep the hint only as
		// measurement context and proceed to the provider race.
		return false
	}
	now := time.Now()
	eligible := func(hint dnsExitHint) bool {
		client := hint.client
		if dnsExitHintTtl <= now.Sub(hint.createTime) || client == nil || client.IsDone() {
			return false
		}
		// a hint learned from one family's answer must not place a flow of
		// the other on an exit that cannot carry it (IPV6.md B3): the AAAA
		// answer for a name may have come back through a v4-only exit
		if !client.supportsIpVersion(ipPath.Version) {
			return false
		}
		if key, ok := destinationServiceFailureKeyFor(ipPath, client); ok {
			if failure, failed := self.destinationServiceFailures[key]; failed &&
				now.Sub(failure.time) < destinationServiceFailureTtl {
				return false
			}
		}
		if !self.reliabilitySettings().AffinityStickyPastCap && self.clientAtFlowCapWithLock(client) {
			return false
		}
		// A DNS result is a fresh-session placement hint, not an established
		// site binding. Do not donate an exit that an external probe currently
		// knows the resolved domain rejects. Ordinary established affinity is
		// intentionally left untouched by this check.
		if clientFailsAffinityPathReputation(client, affinityPaths) {
			return false
		}
		if !self.affinityPerformanceAllowsDonor(client, ipPath) {
			return false
		}
		return client.affinityDonorEligible(false, 0) == donorEligible
	}
	if destinationAddr, ok := ipAssocAddr(ipPath.DestinationIp); ok {
		if hint, found := self.dnsAddressExitHints[destinationAddr]; found {
			// One CDN address can front several unrelated names resolved in
			// parallel. Use the address fast path only when its originating
			// name is among this flow's reverse-DNS affinity groups (or no
			// name is available); otherwise fall through to the domain map.
			nameMatches := len(affinityPaths) == 0
			for _, affinityPath := range affinityPaths {
				if affinityPath != nil && affinityPath.ServerName == hint.affinityName {
					nameMatches = true
					break
				}
			}
			if nameMatches && eligible(hint) {
				update.client.Store(hint.client)
				return true
			}
			if dnsExitHintTtl <= now.Sub(hint.createTime) || hint.client == nil || hint.client.IsDone() {
				delete(self.dnsAddressExitHints, destinationAddr)
			}
		}
	}
	for _, affinityPath := range affinityPaths {
		if affinityPath == nil || affinityPath.ServerName == "" {
			continue
		}
		hint, found := self.dnsExitHints[affinityPath.ServerName]
		if !found {
			continue
		}
		client := hint.client
		if dnsExitHintTtl <= now.Sub(hint.createTime) || client == nil || client.IsDone() {
			delete(self.dnsExitHints, affinityPath.ServerName)
			continue
		}
		if !eligible(hint) {
			continue
		}
		update.client.Store(client)
		return true
	}
	return false
}
