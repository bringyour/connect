package connect

type smtpEgressMilestone uint8

const (
	smtpEgressMilestoneNone smtpEgressMilestone = iota
	smtpEgressMilestoneSecure
	smtpEgressMilestoneRejected
)

// Converts enforcement transitions into the diagnostics vocabulary.
func smtpEgressMilestoneForInspection(inspection smtpEgressInspection) smtpEgressMilestone {
	if inspection.becameSecure {
		return smtpEgressMilestoneSecure
	}
	if inspection.becameRejected {
		return smtpEgressMilestoneRejected
	}
	return smtpEgressMilestoneNone
}

// Returns the client-side verdict and one-shot diagnostics transition.
func (self *smtpEgressGuard) inspectDetailed(
	ipPath *IpPath,
	payload []byte,
) (smtpEgressVerdict, smtpEgressMilestone) {
	return self.inspectDetailedForOwner(Id{}, ipPath, payload)
}

// Returns the provider-side verdict and one-shot diagnostics transition.
func (self *smtpEgressGuard) inspectDetailedForOwner(
	ownerId Id,
	ipPath *IpPath,
	payload []byte,
) (smtpEgressVerdict, smtpEgressMilestone) {
	inspection := self.inspectForOwnerResult(ownerId, ipPath, payload)
	return inspection.verdict, smtpEgressMilestoneForInspection(inspection)
}

// SMTP diagnostics deliberately exclude destination addresses, hostnames,
// message contents, account names, and command bytes. The session banner
// carries the build; these events add only family, well-known port, ephemeral
// exit id, and the compatibility/security outcome needed to reconstruct a Mail
// failure.
func (self *RemoteUserNatMultiClient) logSmtpPolicyOutcome(ipPath *IpPath, outcome string) {
	if !smtpNeedsOrderedSend(ipPath) || outcome == "" {
		return
	}
	loggerOrDefault(self.log).Infof("%s\n", relEvent(
		"smtp_policy",
		"family", ipPath.Version,
		"port", ipPath.DestinationPort,
		"outcome", outcome,
	))
}

func (self *RemoteUserNatMultiClient) logSmtpProviderOutcome(
	ipPath *IpPath,
	client *multiClientChannel,
	outcome string,
) {
	if !smtpNeedsEncryptionInspection(ipPath) || client == nil || outcome == "" {
		return
	}
	loggerOrDefault(self.log).Infof("%s\n", relEvent(
		"smtp_provider",
		"family", ipPath.Version,
		"port", ipPath.DestinationPort,
		"exit", client.ClientId(),
		"outcome", outcome,
	))
}
