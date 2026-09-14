package connect

import (
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

var smtpTestClientHello = []byte{
	0x16, 0x03, 0x01, 0x00, 0x2d, // TLS Handshake record, 45 bytes.
	0x01, 0x00, 0x00, 0x29, // ClientHello, 41-byte body.
	0x03, 0x03, // TLS 1.2 legacy version.
	0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07,
	0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
	0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17,
	0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
	0x00,       // Empty legacy session id.
	0x00, 0x02, // One cipher suite.
	0xc0, 0x2f,
	0x01, 0x00, // One null compression method.
}

func smtpTestPath(sourcePort int, destinationPort int, sequence uint32) *IpPath {
	return &IpPath{
		Version:           4,
		Protocol:          IpProtocolTcp,
		SourceIp:          net.ParseIP("10.0.0.2"),
		SourcePort:        sourcePort,
		DestinationIp:     net.ParseIP("203.0.113.10"),
		DestinationPort:   destinationPort,
		SequenceNumber:    sequence,
		AckSequenceNumber: 9000,
		Ack:               true,
	}
}

func smtpTestSyn(sourcePort int, destinationPort int, sequence uint32) *IpPath {
	path := smtpTestPath(sourcePort, destinationPort, sequence)
	path.Ack = false
	path.Syn = true
	return path
}

func requireSmtpVerdict(t *testing.T, want smtpEgressVerdict, got smtpEgressVerdict) {
	t.Helper()
	if got != want {
		t.Fatalf("SMTP verdict = %d, want %d", got, want)
	}
}

// Captures both policy edges so return-direction tests can assert exact paths.
type smtpReturnPathSecurityPolicy struct {
	inspectPath IpPath
	refreshPath IpPath
}

func (self *smtpReturnPathSecurityPolicy) Stats() *SecurityPolicyStatsCollector {
	return DefaultSecurityPolicyStatsCollector()
}

func (self *smtpReturnPathSecurityPolicy) InspectEgress(
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	self.inspectPath = *ipPath
	return SecurityPolicyResultAllow, nil
}

func (self *smtpReturnPathSecurityPolicy) InspectIngress(
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	payload []byte,
) (SecurityPolicyResult, error) {
	return SecurityPolicyResultAllow, nil
}

func (self *smtpReturnPathSecurityPolicy) RefreshEgress(ipPath *IpPath) {
	self.refreshPath = *ipPath
}

func (self *smtpReturnPathSecurityPolicy) RefreshIngress(ipPath *IpPath) {}

func TestSmtpPortClassification(t *testing.T) {
	path := smtpTestPath(41000, smtpLocalPort, 1)
	if !smtpRoutesLocally(path) || !smtpNeedsOrderedSend(path) {
		t.Fatal("TCP/25 was not classified as the explicit local route")
	}
	if smtpNeedsEncryptionInspection(path) {
		t.Fatal("TCP/25 entered the encrypted SMTP inspector")
	}

	for _, port := range []int{smtpImplicitTlsPort, smtpStartTlsPort} {
		path.DestinationPort = port
		if smtpRoutesLocally(path) || !smtpNeedsEncryptionInspection(path) || !smtpNeedsOrderedSend(path) {
			t.Fatalf("TCP/%d SMTP classification is wrong", port)
		}
	}

	path.DestinationPort = 443
	if smtpRoutesLocally(path) || smtpNeedsEncryptionInspection(path) || smtpNeedsOrderedSend(path) {
		t.Fatal("HTTPS was classified as SMTP")
	}
}

func TestSmtp465RequiresFragmentedTlsClientHello(t *testing.T) {
	var guard smtpEgressGuard
	const sourcePort = 41001
	const synSequence = uint32(1000)

	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestSyn(sourcePort, smtpImplicitTlsPort, synSequence), nil,
	))
	first := smtpTestClientHello[:3]
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(sourcePort, smtpImplicitTlsPort, synSequence+1), first,
	))
	// An exact retransmission is accepted without advancing the stream.
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(sourcePort, smtpImplicitTlsPort, synSequence+1), first,
	))
	// An overlapping retransmission supplies the rest of the complete hello.
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(sourcePort, smtpImplicitTlsPort, synSequence+2), smtpTestClientHello[1:],
	))
	// Once the full hello is verified, opaque TLS records are no longer inspected.
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(sourcePort, smtpImplicitTlsPort, synSequence+10), []byte{0xff, 0x00, 0x7f},
	))
}

func TestSmtp465RejectsPlaintextAndLatchesFlow(t *testing.T) {
	var guard smtpEgressGuard
	const sourcePort = 41002
	const synSequence = uint32(2000)

	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestSyn(sourcePort, smtpImplicitTlsPort, synSequence), nil,
	))
	requireSmtpVerdict(t, smtpEgressReject, guard.inspect(
		smtpTestPath(sourcePort, smtpImplicitTlsPort, synSequence+1), []byte("EHLO plaintext.example\r\n"),
	))
	// A rejected connection cannot disguise a later segment as a new stream.
	requireSmtpVerdict(t, smtpEgressReject, guard.inspect(
		smtpTestPath(sourcePort, smtpImplicitTlsPort, synSequence+1), smtpTestClientHello,
	))
}

func TestSmtp465RejectsMalformedClientHelloPrefixes(t *testing.T) {
	tests := []struct {
		name   string
		prefix []byte
	}{
		{name: "application data record", prefix: []byte{0x17, 0x03, 0x03, 0x00, 0x40, 0x01, 0x00, 0x00, 0x3c}},
		{name: "non TLS version", prefix: []byte{0x16, 0x02, 0x00, 0x00, 0x40, 0x01, 0x00, 0x00, 0x3c}},
		{name: "SSLv3 version", prefix: []byte{0x16, 0x03, 0x00, 0x00, 0x40, 0x01, 0x00, 0x00, 0x3c}},
		{name: "oversized record", prefix: []byte{0x16, 0x03, 0x03, 0x40, 0x01, 0x01, 0x00, 0x00, 0x3c}},
		{name: "server hello", prefix: []byte{0x16, 0x03, 0x03, 0x00, 0x40, 0x02, 0x00, 0x00, 0x3c}},
		{name: "short client hello", prefix: []byte{0x16, 0x03, 0x03, 0x00, 0x40, 0x01, 0x00, 0x00, 0x28}},
	}
	for index, testCase := range tests {
		var guard smtpEgressGuard
		verdict := guard.inspect(
			smtpTestPath(41005+index, smtpImplicitTlsPort, 3600),
			testCase.prefix,
		)
		if verdict != smtpEgressReject {
			t.Errorf("%s verdict = %d, want reject", testCase.name, verdict)
		}
	}
}

func smtpTestClientHelloWithBody(body []byte) []byte {
	handshakeBytes := smtpTlsHandshakeHeaderBytes + len(body)
	record := make([]byte, smtpTlsRecordHeaderBytes+handshakeBytes)
	record[0] = 0x16
	record[1] = 0x03
	record[2] = 0x01
	binary.BigEndian.PutUint16(record[3:5], uint16(handshakeBytes))
	record[5] = 0x01
	record[6] = byte(len(body) >> 16)
	record[7] = byte(len(body) >> 8)
	record[8] = byte(len(body))
	copy(record[9:], body)
	return record
}

func TestSmtp465RejectsMalformedCompleteClientHello(t *testing.T) {
	validBody := smtpTestClientHello[smtpTlsRecordHeaderBytes+smtpTlsHandshakeHeaderBytes:]
	invalidLegacyVersion := append([]byte{}, validBody...)
	invalidLegacyVersion[0] = 0x02
	oversizedSessionId := append([]byte{}, validBody...)
	oversizedSessionId[34] = 33
	oddCipherSuites := append([]byte{}, validBody...)
	oddCipherSuites[35] = 0
	oddCipherSuites[36] = 1
	missingCompressionMethod := append([]byte{}, validBody...)
	missingCompressionMethod[39] = 0
	badExtensionLength := append(append([]byte{}, validBody...), 0x00, 0x01, 0x00)
	duplicateExtensions := append(
		append([]byte{}, validBody...),
		0x00, 0x08,
		0x00, 0x15, 0x00, 0x00,
		0x00, 0x15, 0x00, 0x00,
	)
	cases := []struct {
		name string
		body []byte
	}{
		{name: "legacy version", body: invalidLegacyVersion},
		{name: "session id length", body: oversizedSessionId},
		{name: "cipher suite length", body: oddCipherSuites},
		{name: "compression methods", body: missingCompressionMethod},
		{name: "extension vector length", body: badExtensionLength},
		{name: "duplicate extension", body: duplicateExtensions},
	}
	for index, testCase := range cases {
		var guard smtpEgressGuard
		verdict := guard.inspect(
			smtpTestPath(41100+index, smtpImplicitTlsPort, 3700),
			smtpTestClientHelloWithBody(testCase.body),
		)
		if verdict != smtpEgressReject {
			t.Errorf("%s verdict = %d, want reject", testCase.name, verdict)
		}
	}
}

func TestSmtp465AcceptsClientHelloSplitAcrossTlsRecords(t *testing.T) {
	handshake := smtpTestClientHello[smtpTlsRecordHeaderBytes:]
	firstPayloadBytes := 7
	firstRecord := make([]byte, smtpTlsRecordHeaderBytes+firstPayloadBytes)
	copy(firstRecord, []byte{0x16, 0x03, 0x01})
	binary.BigEndian.PutUint16(firstRecord[3:5], uint16(firstPayloadBytes))
	copy(firstRecord[smtpTlsRecordHeaderBytes:], handshake[:firstPayloadBytes])
	secondPayload := handshake[firstPayloadBytes:]
	secondRecord := make([]byte, smtpTlsRecordHeaderBytes+len(secondPayload))
	copy(secondRecord, []byte{0x16, 0x03, 0x01})
	binary.BigEndian.PutUint16(secondRecord[3:5], uint16(len(secondPayload)))
	copy(secondRecord[smtpTlsRecordHeaderBytes:], secondPayload)
	wireHello := append(firstRecord, secondRecord...)

	var guard smtpEgressGuard
	sequence := uint32(3800)
	cut := len(firstRecord) + 3
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(41110, smtpImplicitTlsPort, sequence),
		wireHello[:cut],
	))
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(41110, smtpImplicitTlsPort, sequence+uint32(cut)),
		wireHello[cut:],
	))

	guard.stateLock.Lock()
	defer guard.stateLock.Unlock()
	for _, flow := range guard.flows {
		if !flow.secure || len(flow.stream) != 0 {
			t.Fatalf("record-fragmented ClientHello state: secure=%t retained=%d", flow.secure, len(flow.stream))
		}
	}
}

func TestSmtp465AcceptsLargeFirstTlsSegmentWithoutRetainingPayload(t *testing.T) {
	var guard smtpEgressGuard
	payload := append(append([]byte{}, smtpTestClientHello...), make([]byte, 4096)...)
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(41003, smtpImplicitTlsPort, 3000), payload,
	))

	guard.stateLock.Lock()
	defer guard.stateLock.Unlock()
	for _, flow := range guard.flows {
		if !flow.secure || len(flow.stream) != 0 {
			t.Fatalf("verified 465 flow state: secure=%t retained bytes=%d", flow.secure, len(flow.stream))
		}
	}
}

func TestSmtpSecureFlowTreatsLaterSequenceSpaceAsOpaque(t *testing.T) {
	var guard smtpEgressGuard
	const sequence = uint32(3500)
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(41004, smtpImplicitTlsPort, sequence), smtpTestClientHello,
	))

	// Exact retransmissions and opaque data after the validated prefix remain
	// valid once the flow is marked secure.
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(41004, smtpImplicitTlsPort, sequence), smtpTestClientHello,
	))
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(41004, smtpImplicitTlsPort, sequence+uint32(len(smtpTestClientHello))), []byte{0x17, 0x03, 0x03},
	))
}

func TestSmtpSecureFlowAllowsOpaqueDataPastHalfSequenceSpace(t *testing.T) {
	var guard smtpEgressGuard
	const sequence = uint32(3700)
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(41006, smtpImplicitTlsPort, sequence), smtpTestClientHello,
	))

	// Once TLS is established, a forward offset at the TCP half-sequence-space
	// boundary is opaque application data rather than a segment from before the
	// negotiation prefix.
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(41006, smtpImplicitTlsPort, sequence+(uint32(1)<<31)),
		[]byte{0x17, 0x03, 0x03},
	))
}

func TestSmtpSecureFlowAllowsOpaqueDataAfterFullSequenceWrap(t *testing.T) {
	var guard smtpEgressGuard
	const sequence = uint32(3800)
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(41007, smtpImplicitTlsPort, sequence), smtpTestClientHello,
	))

	// The same uint32 value represents baseSequence + 2^32 after one complete
	// sequence-space wrap. It must not alias the discarded negotiation prefix.
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(41007, smtpImplicitTlsPort, sequence),
		[]byte{0x17, 0x03, 0x03},
	))
}

func TestSmtp587AllowsNegotiationThenRequiresClientHello(t *testing.T) {
	var guard smtpEgressGuard
	const sourcePort = 42001
	const synSequence = uint32(4000)
	sequence := synSequence + 1

	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestSyn(sourcePort, smtpStartTlsPort, synSequence), nil,
	))
	firstEhlo := []byte("eh")
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(sourcePort, smtpStartTlsPort, sequence), firstEhlo,
	))
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(sourcePort, smtpStartTlsPort, sequence), firstEhlo,
	))
	sequence += uint32(len(firstEhlo))
	restEhlo := []byte("lo client.example\r\n")
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(sourcePort, smtpStartTlsPort, sequence), restEhlo,
	))
	sequence += uint32(len(restEhlo))

	startTls := []byte("STARTTLS\r\n")
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(sourcePort, smtpStartTlsPort, sequence), startTls,
	))
	sequence += uint32(len(startTls))

	firstTls := smtpTestClientHello[:4]
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(sourcePort, smtpStartTlsPort, sequence), firstTls,
	))
	// Overlap two verified bytes while completing the ClientHello prefix.
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(sourcePort, smtpStartTlsPort, sequence+2), smtpTestClientHello[2:],
	))
	sequence += uint32(len(smtpTestClientHello))

	// AUTH is opaque TLS application data after the ClientHello, not plaintext
	// SMTP, and therefore passes the now-secure connection marker.
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(sourcePort, smtpStartTlsPort, sequence), []byte("AUTH PLAIN encrypted"),
	))
}

func TestSmtp587AcceptsStartTlsSplitAcrossTcpSegments(t *testing.T) {
	var guard smtpEgressGuard
	const sourcePort = 42002
	sequence := uint32(4500)
	for _, fragment := range [][]byte{
		[]byte("EHLO client.example\r\nSTA"),
		[]byte("RT"),
		[]byte("TLS\r"),
		[]byte("\n"),
		smtpTestClientHello[:2],
		smtpTestClientHello[2:],
	} {
		requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
			smtpTestPath(sourcePort, smtpStartTlsPort, sequence), fragment,
		))
		sequence += uint32(len(fragment))
	}
	// The split command and ClientHello must leave the flow in the secure,
	// opaque phase rather than continuing to parse TLS records as SMTP text.
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(sourcePort, smtpStartTlsPort, sequence), []byte{0x17, 0x03, 0x03},
	))
}

func TestSmtp587RejectsTransactionCommandsBeforeStartTls(t *testing.T) {
	commands := []string{
		"AUTH PLAIN secret\r\n",
		"MAIL FROM:<sender@example.com>\r\n",
		"RCPT TO:<recipient@example.com>\r\n",
		"DATA\r\n",
		"NOOP secret\r\n",
		"VRFY user\r\n",
	}
	for index, command := range commands {
		var guard smtpEgressGuard
		verdict := guard.inspect(
			smtpTestPath(43000+index, smtpStartTlsPort, 5000),
			[]byte(command),
		)
		if verdict != smtpEgressReject {
			t.Errorf("%q verdict = %d, want reject", command, verdict)
		}
	}
}

func TestSmtp587RejectsFragmentedTransactionCommandAtFirstDisallowedPrefix(t *testing.T) {
	commands := []string{"AUTH", "MAIL", "RCPT", "DATA"}
	for index, command := range commands {
		var guard smtpEgressGuard
		// None of the permitted pre-TLS negotiation commands starts with these
		// bytes, so reject before a later segment can carry private data.
		verdict := guard.inspect(
			smtpTestPath(43500+index, smtpStartTlsPort, 5500),
			[]byte(command[:1]),
		)
		if verdict != smtpEgressReject {
			t.Errorf("%s prefix verdict = %d, want reject", command, verdict)
		}
	}
}

func TestSmtp587RejectsPlaintextAfterStartTls(t *testing.T) {
	var guard smtpEgressGuard
	negotiation := []byte("EHLO client.example\r\nSTARTTLS\r\n")
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(44001, smtpStartTlsPort, 6000), negotiation,
	))
	requireSmtpVerdict(t, smtpEgressReject, guard.inspect(
		smtpTestPath(44001, smtpStartTlsPort, 6000+uint32(len(negotiation))), []byte("AUTH PLAIN secret\r\n"),
	))
}

func TestSmtp587BoundsNegotiationBuffer(t *testing.T) {
	var guard smtpEgressGuard
	sequence := uint32(6500)
	line := []byte("EHLO client.example\r\n")
	buffered := 0
	for buffered+len(line) <= smtpMaxNegotiationBytes {
		requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
			smtpTestPath(44002, smtpStartTlsPort, sequence), line,
		))
		sequence += uint32(len(line))
		buffered += len(line)
	}
	requireSmtpVerdict(t, smtpEgressReject, guard.inspect(
		smtpTestPath(44002, smtpStartTlsPort, sequence), line,
	))

	guard.stateLock.Lock()
	defer guard.stateLock.Unlock()
	for _, flow := range guard.flows {
		if smtpMaxNegotiationBytes < len(flow.stream) {
			t.Fatalf("587 negotiation buffer retained %d bytes, max %d", len(flow.stream), smtpMaxNegotiationBytes)
		}
	}
}

func TestSmtpGuardRejectsSequenceGap(t *testing.T) {
	var guard smtpEgressGuard
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(45001, smtpStartTlsPort, 7000), []byte("EH"),
	))
	requireSmtpVerdict(t, smtpEgressReject, guard.inspect(
		smtpTestPath(45001, smtpStartTlsPort, 7003), []byte("LO client\r\n"),
	))
}

func TestSmtpGuardRejectsConflictingRetransmission(t *testing.T) {
	var guard smtpEgressGuard
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(45002, smtpStartTlsPort, 8000), []byte("EH"),
	))
	requireSmtpVerdict(t, smtpEgressReject, guard.inspect(
		smtpTestPath(45002, smtpStartTlsPort, 8000), []byte("EX"),
	))
}

func TestSmtpGuardFreshSynReplacesTupleState(t *testing.T) {
	var guard smtpEgressGuard
	const sourcePort = 46001
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(sourcePort, smtpImplicitTlsPort, 9000), smtpTestClientHello,
	))
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestSyn(sourcePort, smtpImplicitTlsPort, 10000), nil,
	))
	requireSmtpVerdict(t, smtpEgressReject, guard.inspect(
		smtpTestPath(sourcePort, smtpImplicitTlsPort, 10001), []byte("plaintext"),
	))
}

func TestSmtpGuardRstClearsTupleState(t *testing.T) {
	var guard smtpEgressGuard
	const sourcePort = 46003
	const sequence = uint32(10500)

	requireSmtpVerdict(t, smtpEgressReject, guard.inspect(
		smtpTestPath(sourcePort, smtpImplicitTlsPort, sequence), []byte("plaintext"),
	))
	rstPath := smtpTestPath(sourcePort, smtpImplicitTlsPort, sequence)
	rstPath.Rst = true
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(rstPath, nil))
	// Reusing the tuple after teardown must start from empty state rather than
	// inherit the prior connection's latched rejection.
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestPath(sourcePort, smtpImplicitTlsPort, sequence), smtpTestClientHello,
	))
}

func TestSmtpGuardFinClearsTupleState(t *testing.T) {
	var guard smtpEgressGuard
	const sequence = uint32(10700)

	var path IpPath
	payload, err := parseIpPathWithPayloadBorrowed(
		smtpTestTcp4Packet(byte(tcpFlagAck|tcpFlagPsh), sequence, 9000, smtpTestClientHello),
		&path,
	)
	if err != nil {
		t.Fatal(err)
	}
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(&path, payload))

	payload, err = parseIpPathWithPayloadBorrowed(
		smtpTestTcp4Packet(
			byte(tcpFlagAck|tcpFlagFin),
			sequence+uint32(len(smtpTestClientHello)),
			9000,
			nil,
		),
		&path,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !path.Fin {
		t.Fatal("TCP FIN was not exposed on the SMTP inspection path")
	}
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(&path, payload))

	guard.stateLock.Lock()
	defer guard.stateLock.Unlock()
	if len(guard.flows) != 0 {
		t.Fatalf("SMTP flow table retained %d entries after FIN, want 0", len(guard.flows))
	}
}

func TestSmtpGuardServerTeardownClearsOnlyMatchingOwner(t *testing.T) {
	var guard smtpEgressGuard
	firstOwnerId := NewId()
	secondOwnerId := NewId()
	path := smtpTestPath(46004, smtpImplicitTlsPort, 10800)
	for _, ownerId := range []Id{firstOwnerId, secondOwnerId} {
		requireSmtpVerdict(t, smtpEgressAllow, guard.inspectForOwner(
			ownerId,
			path,
			smtpTestClientHello,
		))
	}
	returnPath := path.Reverse()
	returnPath.Fin = true
	guard.retireReturnForOwner(firstOwnerId, returnPath)
	firstKey, firstOk := smtpFlowKeyForOwnerPath(firstOwnerId, path)
	secondKey, secondOk := smtpFlowKeyForOwnerPath(secondOwnerId, path)
	if !firstOk || !secondOk {
		t.Fatal("could not build server teardown flow keys")
	}

	guard.stateLock.Lock()
	defer guard.stateLock.Unlock()
	if _, ok := guard.flows[firstKey]; ok {
		t.Fatal("server FIN retained the matching owner flow")
	}
	if flow := guard.flows[secondKey]; flow == nil || !flow.secure {
		t.Fatal("server FIN retired another owner's identical tuple")
	}
}

func TestProviderTcpFlowCloseRetiresSmtpAndDmcaState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := Reverse(DefaultSecurityPolicy(ctx))
	provider := &RemoteUserNatProvider{securityPolicy: policy}
	senderClientId := NewId()
	source := SourceId(senderClientId)

	smtpPath := smtpTestPath(46005, smtpImplicitTlsPort, 10900)
	requireSmtpVerdict(t, smtpEgressAllow, provider.smtpIngressGuard.inspectForOwner(
		senderClientId,
		smtpPath,
		smtpTestClientHello,
	))
	dmcaIpPath := dmcaPath(IpProtocolTcp, 46006, 50000, false)
	result, err := inspectAndRefreshIngressForSenderBorrowed(
		policy,
		senderClientId,
		protocol.ProvideMode_Public,
		*dmcaIpPath,
		[]byte("GET / HTTP/1.1\r\n\r\n"),
	)
	if err != nil || result != SecurityPolicyResultAllow {
		t.Fatalf("provider DMCA setup result = (%d, %v), want allow", result, err)
	}
	provider.tcpFlowClosed(source, smtpPath)
	provider.tcpFlowClosed(source, dmcaIpPath)

	provider.smtpIngressGuard.stateLock.Lock()
	smtpFlowCount := len(provider.smtpIngressGuard.flows)
	provider.smtpIngressGuard.stateLock.Unlock()
	if smtpFlowCount != 0 {
		t.Fatalf("provider TCP close retained %d SMTP flows, want 0", smtpFlowCount)
	}
	flowCounter := policy.(interface{ Testing_FlowCount() int })
	if dmcaFlowCount := flowCounter.Testing_FlowCount(); dmcaFlowCount != 0 {
		t.Fatalf("provider TCP close retained %d DMCA flows, want 0", dmcaFlowCount)
	}
}

func TestProviderAuthenticatedDisconnectRetiresOnlySenderState(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	policy := Reverse(DefaultSecurityPolicy(ctx))
	provider := &RemoteUserNatProvider{
		securityPolicy:           policy,
		sourceProvideMode:        map[Id]protocol.ProvideMode{},
		sourceP2pPriorityRefresh: map[Id]time.Time{},
	}
	firstSenderClientId := NewId()
	secondSenderClientId := NewId()
	smtpPath := smtpTestPath(46007, smtpImplicitTlsPort, 10950)
	dmcaIpPath := dmcaPath(IpProtocolTcp, 46008, 50000, false)
	for _, senderClientId := range []Id{firstSenderClientId, secondSenderClientId} {
		requireSmtpVerdict(t, smtpEgressAllow, provider.smtpIngressGuard.inspectForOwner(
			senderClientId,
			smtpPath,
			smtpTestClientHello,
		))
		result, err := inspectAndRefreshIngressForSenderBorrowed(
			policy,
			senderClientId,
			protocol.ProvideMode_Public,
			*dmcaIpPath,
			[]byte("GET / HTTP/1.1\r\n\r\n"),
		)
		if err != nil || result != SecurityPolicyResultAllow {
			t.Fatalf("provider sender setup result = (%d, %v), want allow", result, err)
		}
		provider.sourceProvideMode[senderClientId] = protocol.ProvideMode_Public
		provider.sourceP2pPriorityRefresh[senderClientId] = time.Unix(1700000000, 0)
	}

	disconnectTime := uint64(1700000000000)
	disconnectFrame := RequireToFrameWithDefaultProtocolVersion(&protocol.NetworkPeersUpdate{
		Peers: []*protocol.NetworkPeer{
			{
				ClientId:       firstSenderClientId.Bytes(),
				DisconnectTime: &disconnectTime,
			},
		},
	})
	provider.ClientReceive(
		SourceId(ControlId),
		[]*protocol.Frame{disconnectFrame},
		Peer{ProvideMode: protocol.ProvideMode_Network},
	)

	firstSmtpKey, firstOk := smtpFlowKeyForOwnerPath(firstSenderClientId, smtpPath)
	secondSmtpKey, secondOk := smtpFlowKeyForOwnerPath(secondSenderClientId, smtpPath)
	if !firstOk || !secondOk {
		t.Fatal("could not build disconnect flow keys")
	}
	var firstSmtpPresent bool
	var secondSmtpSecure bool
	func() {
		provider.smtpIngressGuard.stateLock.Lock()
		defer provider.smtpIngressGuard.stateLock.Unlock()
		_, firstSmtpPresent = provider.smtpIngressGuard.flows[firstSmtpKey]
		secondSmtpFlow := provider.smtpIngressGuard.flows[secondSmtpKey]
		secondSmtpSecure = secondSmtpFlow != nil && secondSmtpFlow.secure
	}()
	if firstSmtpPresent {
		t.Fatal("authenticated disconnect retained the matching SMTP owner")
	}
	if !secondSmtpSecure {
		t.Fatal("authenticated disconnect retired another SMTP owner")
	}
	flowCounter := policy.(interface{ Testing_FlowCount() int })
	if dmcaFlowCount := flowCounter.Testing_FlowCount(); dmcaFlowCount != 1 {
		t.Fatalf("authenticated disconnect retained %d DMCA flows, want other sender only", dmcaFlowCount)
	}
	var firstModePresent bool
	var secondModePresent bool
	var firstRefreshPresent bool
	var secondRefreshPresent bool
	func() {
		provider.stateLock.Lock()
		defer provider.stateLock.Unlock()
		_, firstModePresent = provider.sourceProvideMode[firstSenderClientId]
		_, secondModePresent = provider.sourceProvideMode[secondSenderClientId]
		_, firstRefreshPresent = provider.sourceP2pPriorityRefresh[firstSenderClientId]
		_, secondRefreshPresent = provider.sourceP2pPriorityRefresh[secondSenderClientId]
	}()
	if firstModePresent || firstRefreshPresent {
		t.Fatal("authenticated disconnect retained matching provider sender metadata")
	}
	if !secondModePresent || !secondRefreshPresent {
		t.Fatal("authenticated disconnect retired another sender's provider metadata")
	}
}

func TestProviderReturnInspectionUsesPacketDirectionAndTeardown(t *testing.T) {
	policy := &smtpReturnPathSecurityPolicy{}
	provider := &RemoteUserNatProvider{securityPolicy: policy}
	senderClientId := NewId()
	outboundPath := smtpTestPath(47001, smtpImplicitTlsPort, 11000)
	requireSmtpVerdict(t, smtpEgressAllow, provider.smtpIngressGuard.inspectForOwner(
		senderClientId,
		outboundPath,
		smtpTestClientHello,
	))

	outboundPacket := smtpTestTcp4Packet(byte(tcpFlagAck), 11000, 12000, nil)
	returnReset := tcpRstForPolicyReject(outboundPacket)
	if returnReset == nil {
		t.Fatal("could not build deterministic provider return reset")
	}
	defer MessagePoolReturn(returnReset)
	result, err := provider.inspectReturnPacketsForSender(
		senderClientId,
		protocol.ProvideMode_Public,
		[][]byte{returnReset},
	)
	if err != nil || result != SecurityPolicyResultAllow {
		t.Fatalf("provider return inspection = (%d, %v), want allow", result, err)
	}
	for _, captured := range []struct {
		name string
		path IpPath
	}{
		{name: "inspection", path: policy.inspectPath},
		{name: "refresh", path: policy.refreshPath},
	} {
		path := captured.path
		if !path.SourceIp.Equal(net.IPv4(203, 0, 113, 10)) ||
			path.SourcePort != smtpImplicitTlsPort ||
			!path.DestinationIp.Equal(net.IPv4(10, 0, 0, 2)) ||
			path.DestinationPort != 47001 || !path.Rst {
			t.Fatalf("provider return %s path = %v, want server-to-client RST", captured.name, path)
		}
	}
	provider.smtpIngressGuard.stateLock.Lock()
	flowCount := len(provider.smtpIngressGuard.flows)
	provider.smtpIngressGuard.stateLock.Unlock()
	if flowCount != 0 {
		t.Fatalf("provider return RST retained %d SMTP flows, want 0", flowCount)
	}
}

func TestSmtpGuardNamespacesProviderFlowsBySource(t *testing.T) {
	var guard smtpEgressGuard
	firstSource := NewId()
	secondSource := NewId()
	path := smtpTestPath(46002, smtpImplicitTlsPort, 11000)

	// The first remote client poisons only its own exact tuple.
	requireSmtpVerdict(t, smtpEgressReject, guard.inspectForOwner(
		firstSource,
		path,
		[]byte("plaintext"),
	))
	// A different authenticated client may legitimately reuse the same tunnel
	// address, ports, and sequence number without inheriting that rejection.
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspectForOwner(
		secondSource,
		path,
		smtpTestClientHello,
	))
}

func TestSmtpGuardRejectsProviderOwnerOverflowWithoutEviction(t *testing.T) {
	var guard smtpEgressGuard
	ownerId := NewId()
	for index := 0; index < smtpMaxOwnerFlowCount; index++ {
		requireSmtpVerdict(t, smtpEgressAllow, guard.inspectForOwner(
			ownerId,
			smtpTestSyn(18000+index, smtpImplicitTlsPort, uint32(index+1)),
			nil,
		))
	}
	firstPath := smtpTestSyn(18000, smtpImplicitTlsPort, 1)
	firstKey, ok := smtpFlowKeyForOwnerPath(ownerId, firstPath)
	if !ok {
		t.Fatal("could not build first owner flow key")
	}
	overflowPath := smtpTestSyn(
		18000+smtpMaxOwnerFlowCount,
		smtpImplicitTlsPort,
		uint32(smtpMaxOwnerFlowCount+1),
	)
	requireSmtpVerdict(t, smtpEgressReject, guard.inspectForOwner(ownerId, overflowPath, nil))
	overflowKey, ok := smtpFlowKeyForOwnerPath(ownerId, overflowPath)
	if !ok {
		t.Fatal("could not build owner overflow flow key")
	}

	guard.stateLock.Lock()
	defer guard.stateLock.Unlock()
	ownerFlowCount := 0
	for key := range guard.flows {
		if key.ownerId == ownerId {
			ownerFlowCount += 1
		}
	}
	if ownerFlowCount != smtpMaxOwnerFlowCount {
		t.Fatalf("provider owner retained %d SYN entries, want %d", ownerFlowCount, smtpMaxOwnerFlowCount)
	}
	if _, ok := guard.flows[firstKey]; !ok {
		t.Fatal("owner overflow evicted an admitted flow")
	}
	if _, ok := guard.flows[overflowKey]; ok {
		t.Fatal("owner overflow was admitted")
	}
}

func TestSmtpGuardDoesNotTrustClientHelloHeader(t *testing.T) {
	var guard smtpEgressGuard
	ownerId := NewId()
	path := smtpTestPath(19000, smtpImplicitTlsPort, 1000)
	headerBytes := smtpTlsRecordHeaderBytes + smtpTlsHandshakeHeaderBytes
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspectForOwner(
		ownerId,
		path,
		smtpTestClientHello[:headerBytes],
	))
	key, ok := smtpFlowKeyForOwnerPath(ownerId, path)
	if !ok {
		t.Fatal("could not build incomplete ClientHello flow key")
	}
	guard.stateLock.Lock()
	flow := guard.flows[key]
	secure := flow != nil && flow.secure
	guard.stateLock.Unlock()
	if secure {
		t.Fatal("nine-byte TLS/handshake header marked an incomplete ClientHello secure")
	}
	malformedBody := make([]byte, len(smtpTestClientHello)-headerBytes)
	requireSmtpVerdict(t, smtpEgressReject, guard.inspectForOwner(
		ownerId,
		smtpTestPath(19000, smtpImplicitTlsPort, 1000+uint32(headerBytes)),
		malformedBody,
	))
}

func TestSmtpGuardIdleTimeoutExpiresSecureTuple(t *testing.T) {
	now := time.Unix(1700000000, 0)
	guard := smtpEgressGuard{
		timeNow: func() time.Time {
			return now
		},
	}
	ownerId := NewId()
	path := smtpTestPath(22000, smtpImplicitTlsPort, 30000)
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspectForOwner(
		ownerId,
		path,
		smtpTestClientHello,
	))

	now = now.Add(smtpFlowIdleTimeout)
	// The identical tuple must no longer inherit the old secure marker. With
	// no fresh ClientHello, its first post-timeout payload fails closed.
	requireSmtpVerdict(t, smtpEgressReject, guard.inspectForOwner(
		ownerId,
		path,
		[]byte("plaintext after idle timeout"),
	))
}

func TestSmtpGuardIdleReaperKeepsRecentlyActiveFlow(t *testing.T) {
	now := time.Unix(1700000000, 0)
	guard := smtpEgressGuard{
		timeNow: func() time.Time {
			return now
		},
	}
	ownerId := NewId()
	stalePath := smtpTestSyn(22001, smtpImplicitTlsPort, 31000)
	activePath := smtpTestSyn(22002, smtpImplicitTlsPort, 32000)
	triggerPath := smtpTestSyn(22003, smtpImplicitTlsPort, 33000)
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspectForOwner(ownerId, stalePath, nil))

	now = now.Add(smtpFlowIdleTimeout / 2)
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspectForOwner(ownerId, activePath, nil))

	now = now.Add(smtpFlowIdleTimeout / 2)
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspectForOwner(ownerId, triggerPath, nil))

	staleKey, staleOk := smtpFlowKeyForOwnerPath(ownerId, stalePath)
	activeKey, activeOk := smtpFlowKeyForOwnerPath(ownerId, activePath)
	triggerKey, triggerOk := smtpFlowKeyForOwnerPath(ownerId, triggerPath)
	if !staleOk || !activeOk || !triggerOk {
		t.Fatal("could not build SMTP idle-reaper flow keys")
	}
	guard.stateLock.Lock()
	defer guard.stateLock.Unlock()
	if _, ok := guard.flows[staleKey]; ok {
		t.Fatal("idle SMTP flow survived its deterministic timeout")
	}
	if _, ok := guard.flows[activeKey]; !ok {
		t.Fatal("recently active SMTP flow was reaped")
	}
	if _, ok := guard.flows[triggerKey]; !ok {
		t.Fatal("flow that triggered SMTP idle reaping was not retained")
	}
}

func TestSmtpGuardBoundsFlowTable(t *testing.T) {
	var guard smtpEgressGuard
	for index := 0; index < smtpMaxFlowCount; index++ {
		requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
			smtpTestSyn(10000+index, smtpImplicitTlsPort, uint32(index+1)), nil,
		))
	}
	firstPath := smtpTestSyn(10000, smtpImplicitTlsPort, 1)
	firstKey, ok := smtpFlowKeyForOwnerPath(Id{}, firstPath)
	if !ok {
		t.Fatal("could not build first global flow key")
	}
	overflowPath := smtpTestSyn(10000+smtpMaxFlowCount, smtpImplicitTlsPort, smtpMaxFlowCount+1)
	requireSmtpVerdict(t, smtpEgressReject, guard.inspect(overflowPath, nil))
	guard.stateLock.Lock()
	defer guard.stateLock.Unlock()
	if len(guard.flows) != smtpMaxFlowCount {
		t.Fatalf("SMTP flow table size = %d, want %d", len(guard.flows), smtpMaxFlowCount)
	}
	if _, ok := guard.flows[firstKey]; !ok {
		t.Fatal("global overflow evicted an admitted flow")
	}
}

func TestSmtpGuardRejectsGlobalOwnerOverflowWithoutEviction(t *testing.T) {
	var guard smtpEgressGuard
	ownerCount := smtpMaxFlowCount / smtpMaxOwnerFlowCount
	ownerIds := make([]Id, ownerCount)
	for ownerIndex := range ownerIds {
		ownerIds[ownerIndex] = NewId()
	}
	for index := 0; index < smtpMaxFlowCount; index++ {
		ownerId := ownerIds[index/smtpMaxOwnerFlowCount]
		requireSmtpVerdict(t, smtpEgressAllow, guard.inspectForOwner(
			ownerId,
			smtpTestPath(20000+index, smtpImplicitTlsPort, uint32(index+1)),
			smtpTestClientHello,
		))
	}
	protectedPath := smtpTestPath(20000, smtpImplicitTlsPort, 1)
	protectedKey, ok := smtpFlowKeyForOwnerPath(ownerIds[0], protectedPath)
	if !ok {
		t.Fatal("could not build protected global flow key")
	}
	overflowOwnerId := NewId()
	overflowPath := smtpTestSyn(20000+smtpMaxFlowCount, smtpImplicitTlsPort, 50000)
	requireSmtpVerdict(t, smtpEgressReject, guard.inspectForOwner(
		overflowOwnerId,
		overflowPath,
		nil,
	))

	guard.stateLock.Lock()
	defer guard.stateLock.Unlock()
	protectedFlow := guard.flows[protectedKey]
	if protectedFlow == nil || !protectedFlow.secure {
		t.Fatal("global overflow evicted an admitted secure flow")
	}
	if len(guard.flows) != smtpMaxFlowCount {
		t.Fatalf("SMTP flow table size = %d, want %d", len(guard.flows), smtpMaxFlowCount)
	}
}

func TestSmtpGuardTupleReplacementAtCapacityDoesNotEvictAnotherFlow(t *testing.T) {
	var guard smtpEgressGuard
	const firstSourcePort = 12000
	for index := 0; index < smtpMaxFlowCount; index++ {
		requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
			smtpTestPath(firstSourcePort+index, smtpImplicitTlsPort, uint32(index+1)),
			smtpTestClientHello,
		))
	}

	firstPath := smtpTestPath(firstSourcePort, smtpImplicitTlsPort, 1)
	firstKey, ok := smtpFlowKeyForOwnerPath(Id{}, firstPath)
	if !ok {
		t.Fatal("could not build the first SMTP flow key")
	}
	lastSourcePort := firstSourcePort + smtpMaxFlowCount - 1
	requireSmtpVerdict(t, smtpEgressAllow, guard.inspect(
		smtpTestSyn(lastSourcePort, smtpImplicitTlsPort, 50000),
		nil,
	))

	guard.stateLock.Lock()
	defer guard.stateLock.Unlock()
	if len(guard.flows) != smtpMaxFlowCount {
		t.Fatalf("SMTP tuple replacement left %d flows, want %d", len(guard.flows), smtpMaxFlowCount)
	}
	if _, ok := guard.flows[firstKey]; !ok {
		t.Fatal("SMTP tuple replacement evicted an unrelated established flow")
	}
}

func smtpTestTcp4Packet(flags byte, sequence uint32, ack uint32, payload []byte) []byte {
	return smtpTestTcp4PacketToPort(smtpImplicitTlsPort, flags, sequence, ack, payload)
}

func smtpTestTcp4PacketToPort(destinationPort int, flags byte, sequence uint32, ack uint32, payload []byte) []byte {
	sourceIp := net.IPv4(10, 0, 0, 2).To4()
	destinationIp := net.IPv4(203, 0, 113, 10).To4()
	packet := make([]byte, Ipv4HeaderSizeWithoutExtensions+TcpHeaderSizeWithoutExtensions+len(payload))
	writeIpv4Header(packet, ipProtocolNumberTcp, sourceIp, destinationIp)
	tcp := packet[Ipv4HeaderSizeWithoutExtensions:]
	binary.BigEndian.PutUint16(tcp[0:2], 47001)
	binary.BigEndian.PutUint16(tcp[2:4], uint16(destinationPort))
	binary.BigEndian.PutUint32(tcp[4:8], sequence)
	binary.BigEndian.PutUint32(tcp[8:12], ack)
	tcp[12] = byte(TcpHeaderSizeWithoutExtensions/4) << 4
	tcp[13] = flags
	binary.BigEndian.PutUint16(tcp[14:16], 65535)
	copy(tcp[TcpHeaderSizeWithoutExtensions:], payload)
	binary.BigEndian.PutUint16(tcp[16:18], transportChecksum(
		ipProtocolNumberTcp,
		sourceIp,
		destinationIp,
		tcp,
	))
	return packet
}

func TestTcpRstForSmtpPolicyReject(t *testing.T) {
	packet := smtpTestTcp4Packet(byte(tcpFlagAck|tcpFlagPsh), 12000, 34000, []byte("plaintext"))
	reset := tcpRstForPolicyReject(packet)
	if reset == nil {
		t.Fatal("SMTP policy rejection did not build a TCP reset")
	}
	defer MessagePoolReturn(reset)

	ipProtocol, sourceIp, destinationIp, transport, ok := parseIpv4(reset)
	if !ok || ipProtocol != ipProtocolNumberTcp {
		t.Fatal("SMTP policy reset is not valid IPv4/TCP")
	}
	var tcp parsedTcp
	if !parseTcpPacket(sourceIp, destinationIp, transport, &tcp) {
		t.Fatal("could not parse SMTP policy reset")
	}
	if !tcp.rst || tcp.ack || tcp.seq != 34000 {
		t.Fatalf("reset flags/sequence = rst:%t ack:%t seq:%d", tcp.rst, tcp.ack, tcp.seq)
	}
	if !sourceIp.Equal(net.IPv4(203, 0, 113, 10)) || !destinationIp.Equal(net.IPv4(10, 0, 0, 2)) {
		t.Fatalf("reset addresses were not reversed: %s -> %s", sourceIp, destinationIp)
	}
	if tcp.sourcePort != smtpImplicitTlsPort || tcp.destinationPort != 47001 {
		t.Fatalf("reset ports = %d -> %d", tcp.sourcePort, tcp.destinationPort)
	}
	if checksum := transportChecksum(ipProtocolNumberTcp, sourceIp, destinationIp, transport); checksum != 0 {
		t.Fatalf("reset TCP checksum verification = %#x, want 0", checksum)
	}

	rstPacket := smtpTestTcp4Packet(byte(tcpFlagRst), 1, 0, nil)
	if secondReset := tcpRstForPolicyReject(rstPacket); secondReset != nil {
		MessagePoolReturn(secondReset)
		t.Fatal("policy reset builder answered a reset with another reset")
	}
}
