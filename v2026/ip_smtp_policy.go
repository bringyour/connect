package connect

import (
	"bytes"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/urnetwork/connect/v2026/protocol"
)

var errSmtpEncryptionRequired = errors.New("SMTP encryption required")

// SMTP routing and encryption policy is enforced before the general CFAA
// policy. Port 25 is deliberately local-only. Port 465 must begin with a TLS
// ClientHello; port 587 may send only bounded SMTP negotiation until STARTTLS
// is immediately followed by a TLS ClientHello.
const (
	smtpLocalPort                  = 25
	smtpImplicitTlsPort            = 465
	smtpStartTlsPort               = 587
	smtpTlsRecordHeaderBytes       = 5
	smtpTlsHandshakeHeaderBytes    = 4
	smtpMaxTlsRecordBytes          = 16 * 1024
	smtpMaxTlsClientHelloBodyBytes = 64 * 1024
	smtpMaxTlsClientHelloWireBytes = 68 * 1024
	smtpMaxNegotiationBytes        = 2048
	smtpMaxCommandLineBytes        = 510 // RFC 5321's 512 includes CRLF.
	smtpMaxFlowCount               = 1024
	smtpMaxOwnerFlowCount          = 128
	smtpFlowIdleTimeout            = 5 * time.Minute
	smtpFlowIdleReapInterval       = time.Minute
)

type smtpEgressVerdict uint8

const (
	smtpEgressNotApplicable smtpEgressVerdict = iota
	smtpEgressAllow
	smtpEgressReject
)

// Carries the verdict plus one-shot state transitions for in-package
// diagnostics without coupling enforcement to any telemetry format.
type smtpEgressInspection struct {
	verdict        smtpEgressVerdict
	becameSecure   bool
	becameRejected bool
}

func smtpRoutesLocally(ipPath *IpPath) bool {
	return ipPath != nil &&
		ipPath.Protocol == IpProtocolTcp &&
		ipPath.DestinationPort == smtpLocalPort
}

func smtpNeedsEncryptionInspection(ipPath *IpPath) bool {
	if ipPath == nil || ipPath.Protocol != IpProtocolTcp {
		return false
	}
	return ipPath.DestinationPort == smtpImplicitTlsPort ||
		ipPath.DestinationPort == smtpStartTlsPort
}

func smtpNeedsOrderedSend(ipPath *IpPath) bool {
	return smtpRoutesLocally(ipPath) || smtpNeedsEncryptionInspection(ipPath)
}

// smtpFlowKey is the exact outbound tuple. The IP version is explicit so an
// IPv4 flow cannot collide with an IPv4-mapped IPv6 flow.
type smtpFlowKey struct {
	// ownerId namespaces provider-side flows by the authenticated remote
	// client. Tunnel clients can reuse the same virtual IP address and TCP
	// tuple, so the provider must not let one client's verdict latch another
	// client's connection. Client-side guards use the zero Id.
	ownerId         Id
	sourceIp        [net.IPv6len]byte
	destinationIp   [net.IPv6len]byte
	sourcePort      uint16
	destinationPort uint16
	ipVersion       uint8
}

func smtpFlowKeyForOwnerPath(ownerId Id, ipPath *IpPath) (smtpFlowKey, bool) {
	if ipPath == nil ||
		ipPath.Protocol != IpProtocolTcp ||
		ipPath.SourcePort < 0 || 65535 < ipPath.SourcePort ||
		ipPath.DestinationPort < 0 || 65535 < ipPath.DestinationPort {
		return smtpFlowKey{}, false
	}

	var sourceIp net.IP
	var destinationIp net.IP
	switch ipPath.Version {
	case 4:
		sourceIp = ipPath.SourceIp.To4()
		destinationIp = ipPath.DestinationIp.To4()
	case 6:
		sourceIp = ipPath.SourceIp.To16()
		destinationIp = ipPath.DestinationIp.To16()
	default:
		return smtpFlowKey{}, false
	}
	if sourceIp == nil || destinationIp == nil {
		return smtpFlowKey{}, false
	}

	key := smtpFlowKey{
		ownerId:         ownerId,
		sourcePort:      uint16(ipPath.SourcePort),
		destinationPort: uint16(ipPath.DestinationPort),
		ipVersion:       uint8(ipPath.Version),
	}
	copy(key.sourceIp[:], sourceIp)
	copy(key.destinationIp[:], destinationIp)
	return key, true
}

// retireForOwner removes one outbound tuple after its transport lifecycle ends.
func (self *smtpEgressGuard) retireForOwner(ownerId Id, ipPath *IpPath) {
	if !smtpNeedsEncryptionInspection(ipPath) {
		return
	}
	key, ok := smtpFlowKeyForOwnerPath(ownerId, ipPath)
	if !ok {
		return
	}
	self.stateLock.Lock()
	delete(self.flows, key)
	self.stateLock.Unlock()
}

// retireReturnForOwner maps a server-side FIN/RST back to its outbound tuple.
func (self *smtpEgressGuard) retireReturnForOwner(ownerId Id, ipPath *IpPath) {
	if ipPath == nil || ipPath.Protocol != IpProtocolTcp ||
		(!ipPath.Fin && !ipPath.Rst) {
		return
	}
	self.retireForOwner(ownerId, ipPath.Reverse())
}

// retireReturn is the single-owner client-side form.
func (self *smtpEgressGuard) retireReturn(ipPath *IpPath) {
	self.retireReturnForOwner(Id{}, ipPath)
}

// retireOwner removes every tuple owned by an authenticated provider sender.
func (self *smtpEgressGuard) retireOwner(ownerId Id) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	for key := range self.flows {
		if key.ownerId == ownerId {
			delete(self.flows, key)
		}
	}
}

type smtp587Phase uint8

const (
	smtp587Negotiating smtp587Phase = iota
	smtp587ExpectClientHello
)

type smtpFlowState struct {
	destinationPort int

	synSeen bool
	synSeq  uint32

	baseSequence uint32
	baseSet      bool
	stream       []byte
	parseOffset  int
	phase587     smtp587Phase

	secure       bool
	rejected     bool
	lastUsed     uint64
	lastUsedTime time.Time
}

// smtpEgressGuard keeps only the bounded, pre-encryption prefix of each SMTP
// flow. Once TLS is identified the prefix is discarded and all later sequence
// space stays opaque. A fresh SYN replaces the marker on tuple reuse, while
// RST and FIN retire it.
//
// One lock is intentionally sufficient: it is touched only by ports 465/587,
// and a single SMTP stream's packets are ordered through the lock. The map and
// every field have useful zero values, which keeps fixture-built clients safe.
type smtpEgressGuard struct {
	stateLock        sync.Mutex
	flows            map[smtpFlowKey]*smtpFlowState
	clock            uint64
	timeNow          func() time.Time
	nextIdleReapTime time.Time
}

func (self *smtpEgressGuard) inspect(ipPath *IpPath, payload []byte) smtpEgressVerdict {
	return self.inspectForOwnerResult(Id{}, ipPath, payload).verdict
}

// inspectForOwner is the provider-side form of inspect. The authenticated
// source id is part of the key because separate remote clients commonly use
// identical tunnel addresses and ephemeral TCP tuples.
func (self *smtpEgressGuard) inspectForOwner(
	ownerId Id,
	ipPath *IpPath,
	payload []byte,
) smtpEgressVerdict {
	return self.inspectForOwnerResult(ownerId, ipPath, payload).verdict
}

// Performs one atomic state-machine step and reports any new terminal state.
func (self *smtpEgressGuard) inspectForOwnerResult(
	ownerId Id,
	ipPath *IpPath,
	payload []byte,
) smtpEgressInspection {
	if !smtpNeedsEncryptionInspection(ipPath) {
		return smtpEgressInspection{verdict: smtpEgressNotApplicable}
	}
	key, ok := smtpFlowKeyForOwnerPath(ownerId, ipPath)
	if !ok {
		return smtpEgressInspection{
			verdict:        smtpEgressReject,
			becameRejected: true,
		}
	}

	self.stateLock.Lock()
	defer self.stateLock.Unlock()

	if self.flows == nil {
		self.flows = map[smtpFlowKey]*smtpFlowState{}
	}
	now := self.currentTimeWithLock()
	self.reapIdleFlowsWithLock(now)
	if ipPath.Rst {
		delete(self.flows, key)
		return smtpEgressInspection{verdict: smtpEgressAllow}
	}
	if ipPath.Fin {
		// A FIN may carry the last payload bytes, so inspect it before retiring
		// the tuple. A retransmitted FIN also leaves no empty replacement state.
		defer delete(self.flows, key)
	}

	flow := self.flows[key]
	segmentSequence := ipPath.SequenceNumber
	if ipPath.Syn {
		segmentSequence += 1
		if flow == nil || !flow.synSeen || flow.synSeq != ipPath.SequenceNumber {
			var admitted bool
			flow, admitted = self.newFlowWithLock(key, ipPath.DestinationPort, now)
			if !admitted {
				return smtpEgressInspection{
					verdict:        smtpEgressReject,
					becameRejected: true,
				}
			}
			flow.synSeen = true
			flow.synSeq = ipPath.SequenceNumber
			flow.baseSequence = segmentSequence
			flow.baseSet = true
		}
	} else if flow == nil {
		var admitted bool
		flow, admitted = self.newFlowWithLock(key, ipPath.DestinationPort, now)
		if !admitted {
			return smtpEgressInspection{
				verdict:        smtpEgressReject,
				becameRejected: true,
			}
		}
	}

	self.clock += 1
	flow.lastUsed = self.clock
	flow.lastUsedTime = now
	if flow.rejected {
		return smtpEgressInspection{verdict: smtpEgressReject}
	}
	if len(payload) == 0 {
		return smtpEgressInspection{verdict: smtpEgressAllow}
	}
	if !flow.baseSet {
		flow.baseSequence = segmentSequence
		flow.baseSet = true
	}
	wasSecure := flow.secure
	if !flow.inspectPayload(segmentSequence, payload) {
		flow.stream = nil
		flow.parseOffset = 0
		flow.rejected = true
		return smtpEgressInspection{
			verdict:        smtpEgressReject,
			becameRejected: true,
		}
	}
	if !wasSecure && flow.secure {
		return smtpEgressInspection{
			verdict:      smtpEgressAllow,
			becameSecure: true,
		}
	}
	return smtpEgressInspection{verdict: smtpEgressAllow}
}

// currentTimeWithLock returns the guard's time source. Caller holds stateLock.
func (self *smtpEgressGuard) currentTimeWithLock() time.Time {
	if self.timeNow != nil {
		return self.timeNow()
	}
	return time.Now()
}

// reapIdleFlowsWithLock periodically retires abandoned tuples. Caller holds
// stateLock. Packet-driven reaping avoids a background goroutine and still
// cleans the bounded table before a newly observed flow is admitted.
func (self *smtpEgressGuard) reapIdleFlowsWithLock(now time.Time) {
	if !self.nextIdleReapTime.IsZero() && now.Before(self.nextIdleReapTime) {
		return
	}
	for key, flow := range self.flows {
		if !flow.lastUsedTime.IsZero() &&
			!now.Before(flow.lastUsedTime.Add(smtpFlowIdleTimeout)) {
			delete(self.flows, key)
		}
	}
	self.nextIdleReapTime = now.Add(smtpFlowIdleReapInterval)
}

// ownerFlowCountWithLock returns the number of provider-side flows belonging
// to ownerId. Caller holds stateLock.
func (self *smtpEgressGuard) ownerFlowCountWithLock(ownerId Id) int {
	count := 0
	for key := range self.flows {
		if key.ownerId == ownerId {
			count += 1
		}
	}
	return count
}

// Allocates state without evicting a live tuple. Overflow is rejected so one
// sender, or a group of senders, cannot reset an admitted flow. Caller holds
// stateLock and has already reaped idle flows.
func (self *smtpEgressGuard) newFlowWithLock(
	key smtpFlowKey,
	destinationPort int,
	now time.Time,
) (*smtpFlowState, bool) {
	_, replacing := self.flows[key]
	if !replacing && key.ownerId != (Id{}) &&
		smtpMaxOwnerFlowCount <= self.ownerFlowCountWithLock(key.ownerId) {
		return nil, false
	}
	if !replacing && smtpMaxFlowCount <= len(self.flows) {
		return nil, false
	}
	flow := &smtpFlowState{
		destinationPort: destinationPort,
		lastUsedTime:    now,
	}
	self.flows[key] = flow
	return flow, true
}

func (self *smtpFlowState) inspectPayload(sequence uint32, payload []byte) bool {
	if self.secure {
		return true
	}

	// TCP serial arithmetic is safe here because the retained prefix is at
	// most about 70 KiB, far below the half-sequence-space ambiguity boundary.
	relative := int64(int32(sequence - self.baseSequence))
	if relative < 0 || int64(len(self.stream)) < relative {
		// A gap cannot be forwarded safely: bytes hidden in the gap could turn
		// an apparently harmless suffix into AUTH or plaintext credentials.
		return false
	}

	offset := int(relative)
	overlap := len(self.stream) - offset
	if len(payload) < overlap {
		overlap = len(payload)
	}
	if 0 < overlap && !bytes.Equal(self.stream[offset:offset+overlap], payload[:overlap]) {
		// A retransmission must reproduce the bytes already validated for that
		// sequence range. Conflicting overlap is a fail-closed stream splice.
		return false
	}
	if overlap == len(payload) {
		return true
	}

	newBytes := payload[overlap:]
	limit := smtpMaxTlsClientHelloWireBytes
	if self.destinationPort == smtpStartTlsPort {
		limit += smtpMaxNegotiationBytes
	}
	remainingCapacity := limit - len(self.stream)
	retainedBytes := newBytes
	if remainingCapacity < len(retainedBytes) {
		retainedBytes = retainedBytes[:remainingCapacity]
	}
	self.stream = append(self.stream, retainedBytes...)

	var valid bool
	switch self.destinationPort {
	case smtpImplicitTlsPort:
		valid, self.secure = tlsClientHelloStream(self.stream)
	case smtpStartTlsPort:
		valid, self.secure = self.inspect587Stream()
	default:
		return false
	}
	if self.secure {
		// The negotiation bytes have already been accepted. Retaining and
		// comparing them would alias opaque data whenever TCP sequence space wraps.
		self.stream = nil
		self.parseOffset = 0
		return valid
	}
	// More unverified bytes than the bounded prefix can represent fail closed.
	return valid && len(retainedBytes) == len(newBytes)
}

// tlsHandshakeRecordHeaderPrefix validates an incomplete or complete TLS
// Handshake record header.
func tlsHandshakeRecordHeaderPrefix(header []byte) bool {
	for index, value := range header {
		switch index {
		case 0:
			if value != 0x16 {
				return false
			}
		case 1:
			if value != 0x03 {
				return false
			}
		case 2:
			if value < 0x01 || 0x04 < value {
				return false
			}
		}
	}
	if smtpTlsRecordHeaderBytes <= len(header) {
		recordBytes := int(binary.BigEndian.Uint16(header[3:5]))
		if recordBytes == 0 || smtpMaxTlsRecordBytes < recordBytes {
			return false
		}
	}
	return true
}

// validTlsClientHelloBody validates every length-delimited field in a complete
// ClientHello, including the extension vector and duplicate-extension rule.
func validTlsClientHelloBody(body []byte) bool {
	if len(body) < 41 || body[0] != 0x03 || body[1] < 0x01 || 0x03 < body[1] {
		return false
	}
	offset := 2 + 32
	sessionIdBytes := int(body[offset])
	offset += 1
	if 32 < sessionIdBytes || len(body) < offset+sessionIdBytes+2 {
		return false
	}
	offset += sessionIdBytes
	cipherSuiteBytes := int(binary.BigEndian.Uint16(body[offset : offset+2]))
	offset += 2
	if cipherSuiteBytes < 2 || cipherSuiteBytes%2 != 0 || len(body) < offset+cipherSuiteBytes+1 {
		return false
	}
	offset += cipherSuiteBytes
	compressionMethodBytes := int(body[offset])
	offset += 1
	if compressionMethodBytes == 0 || len(body) < offset+compressionMethodBytes {
		return false
	}
	offset += compressionMethodBytes
	if offset == len(body) {
		return true
	}
	if len(body) < offset+2 {
		return false
	}
	extensionBytes := int(binary.BigEndian.Uint16(body[offset : offset+2]))
	offset += 2
	if extensionBytes != len(body)-offset {
		return false
	}
	extensionTypes := map[uint16]bool{}
	for offset < len(body) {
		if len(body) < offset+4 {
			return false
		}
		extensionType := binary.BigEndian.Uint16(body[offset : offset+2])
		extensionDataBytes := int(binary.BigEndian.Uint16(body[offset+2 : offset+4]))
		offset += 4
		if extensionTypes[extensionType] || len(body) < offset+extensionDataBytes {
			return false
		}
		extensionTypes[extensionType] = true
		offset += extensionDataBytes
	}
	return true
}

// tlsClientHelloStream validates complete TLS records until a complete,
// structurally valid ClientHello has arrived. Handshake bytes may span records;
// both logical and wire sizes are bounded before the flow can become secure.
func tlsClientHelloStream(stream []byte) (valid bool, complete bool) {
	if smtpMaxTlsClientHelloWireBytes < len(stream) {
		return false, false
	}
	handshake := make([]byte, 0, min(len(stream), smtpMaxTlsClientHelloBodyBytes+smtpTlsHandshakeHeaderBytes))
	for recordOffset := 0; ; {
		remaining := stream[recordOffset:]
		if len(remaining) < smtpTlsRecordHeaderBytes {
			return tlsHandshakeRecordHeaderPrefix(remaining), false
		}
		header := remaining[:smtpTlsRecordHeaderBytes]
		if !tlsHandshakeRecordHeaderPrefix(header) {
			return false, false
		}
		recordBytes := int(binary.BigEndian.Uint16(header[3:5]))
		recordEnd := recordOffset + smtpTlsRecordHeaderBytes + recordBytes
		if smtpMaxTlsClientHelloWireBytes < recordEnd {
			return false, false
		}
		if len(stream) < recordEnd {
			handshake = append(handshake, stream[recordOffset+smtpTlsRecordHeaderBytes:]...)
			if 0 < len(handshake) && handshake[0] != 0x01 {
				return false, false
			}
			if smtpTlsHandshakeHeaderBytes <= len(handshake) {
				handshakeBodyBytes := int(handshake[1])<<16 |
					int(handshake[2])<<8 |
					int(handshake[3])
				if handshakeBodyBytes < 41 || smtpMaxTlsClientHelloBodyBytes < handshakeBodyBytes {
					return false, false
				}
			}
			return true, false
		}
		handshake = append(handshake, stream[recordOffset+smtpTlsRecordHeaderBytes:recordEnd]...)
		if 0 < len(handshake) && handshake[0] != 0x01 {
			return false, false
		}
		if smtpTlsHandshakeHeaderBytes <= len(handshake) {
			handshakeBodyBytes := int(handshake[1])<<16 |
				int(handshake[2])<<8 |
				int(handshake[3])
			if handshakeBodyBytes < 41 || smtpMaxTlsClientHelloBodyBytes < handshakeBodyBytes {
				return false, false
			}
			handshakeEnd := smtpTlsHandshakeHeaderBytes + handshakeBodyBytes
			if handshakeEnd <= len(handshake) {
				if !validTlsClientHelloBody(handshake[smtpTlsHandshakeHeaderBytes:handshakeEnd]) {
					return false, false
				}
				return true, true
			}
		}
		recordOffset = recordEnd
		if recordOffset == len(stream) {
			return true, false
		}
	}
}

func (self *smtpFlowState) inspect587Stream() (valid bool, secure bool) {
	for {
		if self.phase587 == smtp587ExpectClientHello {
			return tlsClientHelloStream(self.stream[self.parseOffset:])
		}
		if self.parseOffset == len(self.stream) {
			return true, false
		}

		remaining := self.stream[self.parseOffset:]
		lineEnd := bytes.Index(remaining, []byte("\r\n"))
		if lineEnd < 0 {
			return len(self.stream) <= smtpMaxNegotiationBytes &&
				validPartialSmtpNegotiationLine(remaining), false
		}
		if smtpMaxCommandLineBytes < lineEnd ||
			bytes.IndexByte(remaining[:lineEnd], '\r') >= 0 ||
			bytes.IndexByte(remaining[:lineEnd], '\n') >= 0 {
			return false, false
		}

		command, ok := completeSmtpNegotiationCommand(remaining[:lineEnd])
		if !ok {
			return false, false
		}
		self.parseOffset += lineEnd + 2
		if smtpMaxNegotiationBytes < self.parseOffset {
			return false, false
		}
		if command == smtpCommandStartTls {
			self.phase587 = smtp587ExpectClientHello
		}
	}
}

type smtpCommand uint8

const (
	smtpCommandEhlo smtpCommand = iota
	smtpCommandHelo
	smtpCommandQuit
	smtpCommandStartTls
)

var smtpNegotiationCommands = [...]struct {
	name    string
	command smtpCommand
}{
	// Keep the pre-TLS vocabulary deliberately smaller than SMTP itself:
	// identify the client, request TLS, or leave. Transaction commands and
	// extensible commands such as NOOP are unnecessary before encryption and
	// could otherwise carry arbitrary caller-supplied text.
	{name: "EHLO", command: smtpCommandEhlo},
	{name: "HELO", command: smtpCommandHelo},
	{name: "QUIT", command: smtpCommandQuit},
	{name: "STARTTLS", command: smtpCommandStartTls},
}

func validPartialSmtpNegotiationLine(line []byte) bool {
	if smtpMaxCommandLineBytes+1 < len(line) {
		return false
	}
	if 0 < len(line) && line[len(line)-1] == '\r' {
		line = line[:len(line)-1]
	}
	if smtpMaxCommandLineBytes < len(line) {
		return false
	}
	if bytes.IndexByte(line, '\r') >= 0 || bytes.IndexByte(line, '\n') >= 0 ||
		!validSmtpAscii(line) {
		return false
	}

	token, argument, separated := splitSmtpCommand(line)
	if !separated {
		for _, candidate := range smtpNegotiationCommands {
			if asciiCasePrefix(candidate.name, token) {
				return true
			}
		}
		return false
	}

	command, ok := smtpNegotiationCommand(token)
	if !ok {
		return false
	}
	switch command {
	case smtpCommandEhlo, smtpCommandHelo:
		return true
	case smtpCommandQuit, smtpCommandStartTls:
		return len(bytes.Trim(argument, " \t")) == 0
	default:
		return false
	}
}

func completeSmtpNegotiationCommand(line []byte) (smtpCommand, bool) {
	if len(line) == 0 || !validSmtpAscii(line) {
		return 0, false
	}
	token, argument, _ := splitSmtpCommand(line)
	command, ok := smtpNegotiationCommand(token)
	if !ok {
		return 0, false
	}
	switch command {
	case smtpCommandEhlo, smtpCommandHelo:
		if len(bytes.Trim(argument, " \t")) == 0 {
			return 0, false
		}
	case smtpCommandQuit, smtpCommandStartTls:
		if len(bytes.Trim(argument, " \t")) != 0 {
			return 0, false
		}
	}
	return command, true
}

func splitSmtpCommand(line []byte) (token []byte, argument []byte, separated bool) {
	for index, value := range line {
		if value == ' ' || value == '\t' {
			return line[:index], line[index+1:], true
		}
	}
	return line, nil, false
}

func smtpNegotiationCommand(token []byte) (smtpCommand, bool) {
	for _, candidate := range smtpNegotiationCommands {
		if asciiCaseEqual(candidate.name, token) {
			return candidate.command, true
		}
	}
	return 0, false
}

func asciiCasePrefix(candidate string, prefix []byte) bool {
	if len(candidate) < len(prefix) {
		return false
	}
	return asciiCaseEqual(candidate[:len(prefix)], prefix)
}

func asciiCaseEqual(candidate string, value []byte) bool {
	if len(candidate) != len(value) {
		return false
	}
	for index, b := range value {
		if 'a' <= b && b <= 'z' {
			b -= 'a' - 'A'
		}
		if candidate[index] != b {
			return false
		}
	}
	return true
}

func validSmtpAscii(value []byte) bool {
	for _, b := range value {
		if b == '\t' {
			continue
		}
		if b < 0x20 || 0x7e < b {
			return false
		}
	}
	return true
}

// tcpRstForPolicyReject returns an RFC 793 reset addressed to the source of a
// rejected TCP packet. It shares the LocalUserNat orphan-reset builder so ACK
// and sequence behavior, IP checksums, and TCP checksums stay identical.
func tcpRstForPolicyReject(packet []byte) []byte {
	if len(packet) == 0 {
		return nil
	}
	ipVersion := int(packet[0] >> 4)
	var ipProtocol ipProtocolNumber
	var sourceIp net.IP
	var destinationIp net.IP
	var transport []byte
	var ok bool
	switch ipVersion {
	case 4:
		ipProtocol, sourceIp, destinationIp, transport, ok = parseIpv4(packet)
	case 6:
		ipProtocol, sourceIp, destinationIp, transport, ok = parseIpv6(packet)
	default:
		return nil
	}
	if !ok || ipProtocol != ipProtocolNumberTcp {
		return nil
	}
	var tcp parsedTcp
	if !parseTcpPacket(sourceIp, destinationIp, transport, &tcp) || tcp.rst {
		return nil
	}
	return tcpRstForOrphan(ipVersion, &tcp)
}

func deliverTcpPolicyReset(
	receive ReceivePacketFunction,
	source TransferPath,
	provideMode protocol.ProvideMode,
	ipPath *IpPath,
	packet []byte,
) {
	reset := tcpRstForPolicyReject(packet)
	if reset == nil {
		return
	}
	defer MessagePoolReturn(reset)
	if receive != nil {
		HandleError(func() {
			receive(source, provideMode, ipPath, reset)
		})
	}
}
