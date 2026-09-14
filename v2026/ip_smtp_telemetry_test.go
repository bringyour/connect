package connect

import (
	"context"
	"strings"
	"testing"
)

func TestSmtpDetailedInspectionReportsMilestonesOnce(t *testing.T) {
	var secureGuard smtpEgressGuard
	path := smtpTestPath(41050, smtpImplicitTlsPort, 1001)
	verdict, milestone := secureGuard.inspectDetailed(path, smtpTestClientHello)
	if verdict != smtpEgressAllow || milestone != smtpEgressMilestoneSecure {
		t.Fatalf("secure inspection = (%d, %d), want allow + secure", verdict, milestone)
	}
	verdict, milestone = secureGuard.inspectDetailed(path, smtpTestClientHello)
	if verdict != smtpEgressAllow || milestone != smtpEgressMilestoneNone {
		t.Fatalf("secure retransmission = (%d, %d), want allow + no duplicate milestone", verdict, milestone)
	}

	var rejectGuard smtpEgressGuard
	plain := []byte("EHLO plaintext.example\r\n")
	verdict, milestone = rejectGuard.inspectDetailed(
		smtpTestPath(41051, smtpImplicitTlsPort, 2001),
		plain,
	)
	if verdict != smtpEgressReject || milestone != smtpEgressMilestoneRejected {
		t.Fatalf("plaintext inspection = (%d, %d), want reject + rejected", verdict, milestone)
	}
	verdict, milestone = rejectGuard.inspectDetailed(
		smtpTestPath(41051, smtpImplicitTlsPort, 2001),
		plain,
	)
	if verdict != smtpEgressReject || milestone != smtpEgressMilestoneNone {
		t.Fatalf("latched rejection = (%d, %d), want reject + no duplicate milestone", verdict, milestone)
	}
}

func TestSmtpTelemetryContainsOnlyCompatibilityDimensions(t *testing.T) {
	log := &sparsePacketLogTestLogger{Logger: NewNoopLogger()}
	settings := DefaultMultiClientSettings()
	client := &multiClientChannel{ctx: context.Background(), settings: settings}
	parent := &RemoteUserNatMultiClient{log: log, settings: settings}
	path := destinationServiceTestPath(4, "203.0.113.65", 465)

	parent.logSmtpPolicyOutcome(path, "encrypted")
	parent.logSmtpProviderOutcome(path, client, "connected")
	body := strings.Join(log.snapshot(), "")
	for _, want := range []string{
		"event=smtp_policy",
		"family=4",
		"port=465",
		"outcome=encrypted",
		"event=smtp_provider",
		"outcome=connected",
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("telemetry missing %q: %s", want, body)
		}
	}
	for _, secret := range []string{"203.0.113.65", "smtp.example.test", "AUTH", "MAIL FROM"} {
		if strings.Contains(body, secret) {
			t.Fatalf("telemetry leaked %q: %s", secret, body)
		}
	}
}
