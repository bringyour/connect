package connect

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"hash"
	"io"
	"reflect"

	"github.com/urnetwork/connect/v2026/protocol"
	"google.golang.org/protobuf/proto"
)

// SecurityPolicyIdentity lets a custom provider policy publish an identity
// for its effective rules. The value should change whenever enforcement
// changes and should not contain secrets.
type SecurityPolicyIdentity interface {
	SecurityPolicyHash() string
}

// SecurityPolicyHash returns a stable digest for the effective policy. The
// built-in digest includes every policy setting and both generated endpoint
// tables. Opaque custom policies may implement SecurityPolicyIdentity; the
// fallback identifies their concrete type and build, which is deliberately
// less authoritative but still detects stale binaries.
func SecurityPolicyHash(policy SecurityPolicy) string {
	if identity, ok := policy.(SecurityPolicyIdentity); ok {
		if value := identity.SecurityPolicyHash(); value != "" {
			return value
		}
	}
	digest := sha256.New()
	writeSecurityPolicyIdentity(digest, policy)
	return hex.EncodeToString(digest.Sum(nil))
}

func writeSecurityPolicyIdentity(digest hash.Hash, policy SecurityPolicy) {
	io.WriteString(digest, "urnetwork-security-policy-v1\x00")
	switch concrete := policy.(type) {
	case *reverseSecurityPolicy:
		io.WriteString(digest, "reverse\x00")
		writeSecurityPolicyIdentity(digest, concrete.policy)
	case *securityPolicy:
		io.WriteString(digest, "builtin\x00")
		configuration := struct {
			Cfaa *CfaaSecurityPolicySettings `json:"cfaa"`
			Dmca *DmcaSecurityPolicySettings `json:"dmca"`
			Web  *WebStandardSettings        `json:"web"`
		}{
			Cfaa: concrete.cfaa.settings,
			Dmca: concrete.dmca.settings,
			Web:  concrete.dmca.web.settings,
		}
		encoded, err := json.Marshal(configuration)
		if err != nil {
			panic(err)
		}
		digest.Write(encoded)
		io.WriteString(digest, "\x00cfaa4\x00")
		io.WriteString(digest, cfaaBlockedPrefixData)
		io.WriteString(digest, "\x00cfaa6\x00")
		io.WriteString(digest, cfaaBlockedPrefix6Data)
	case *disableSecurityPolicy:
		io.WriteString(digest, "disabled\x00")
	default:
		io.WriteString(digest, "opaque\x00")
		io.WriteString(digest, reflect.TypeOf(policy).String())
		io.WriteString(digest, "\x00")
		io.WriteString(digest, BuildVersion())
	}
}

// ProviderDiagnostics is the latest identity and source-scoped security
// enforcement telemetry published by one provider exit.
type ProviderDiagnostics struct {
	BuildVersion            string
	SecurityPolicyHash      string
	BlockIngressPacketCount int64
	BlockIngressByteCount   int64
	BlockEgressPacketCount  int64
	BlockEgressByteCount    int64
	Sequence                int64
}

type providerSourceDiagnostics struct {
	blockIngressPacketCount uint64
	blockIngressByteCount   uint64
	blockEgressPacketCount  uint64
	blockEgressByteCount    uint64
	sequence                uint64
	publishedSequence       uint64
}

func (self *RemoteUserNatProvider) ensureSourceDiagnosticsWithLock(sourceId Id) *providerSourceDiagnostics {
	if self.sourceDiagnostics == nil {
		self.sourceDiagnostics = map[Id]*providerSourceDiagnostics{}
	}
	state := self.sourceDiagnostics[sourceId]
	if state == nil {
		// Sequence 1 publishes provider identity before any block occurs.
		state = &providerSourceDiagnostics{sequence: 1}
		self.sourceDiagnostics[sourceId] = state
	}
	return state
}

func (self *RemoteUserNatProvider) recordProviderBlock(
	sourceId Id,
	ingress bool,
	packetCount int,
	byteCount ByteCount,
) {
	if packetCount <= 0 || byteCount < 0 {
		return
	}
	self.stateLock.Lock()
	state := self.ensureSourceDiagnosticsWithLock(sourceId)
	if ingress {
		state.blockIngressPacketCount += uint64(packetCount)
		state.blockIngressByteCount += uint64(byteCount)
	} else {
		state.blockEgressPacketCount += uint64(packetCount)
		state.blockEgressByteCount += uint64(byteCount)
	}
	state.sequence++
	self.stateLock.Unlock()
}

func (self *RemoteUserNatProvider) providerDiagnosticsMessage(sourceId Id) *protocol.IpProviderDiagnostics {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	state := self.ensureSourceDiagnosticsWithLock(sourceId)
	if state.publishedSequence == state.sequence {
		return nil
	}
	return &protocol.IpProviderDiagnostics{
		BuildVersion:            self.buildVersion,
		SecurityPolicyHash:      self.securityPolicyHash,
		BlockIngressPacketCount: state.blockIngressPacketCount,
		BlockIngressByteCount:   state.blockIngressByteCount,
		BlockEgressPacketCount:  state.blockEgressPacketCount,
		BlockEgressByteCount:    state.blockEgressByteCount,
		Sequence:                state.sequence,
	}
}

func (self *RemoteUserNatProvider) markProviderDiagnosticsPublished(sourceId Id, sequence uint64) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if state := self.sourceDiagnostics[sourceId]; state != nil && state.publishedSequence < sequence {
		state.publishedSequence = sequence
	}
}

// publishProviderDiagnostics is deliberately nonblocking. Identity publishes
// once per source; counters republish only after a block changes them.
func (self *RemoteUserNatProvider) publishProviderDiagnostics(
	source TransferPath,
	transferKey TransferKey,
	provideMode protocol.ProvideMode,
) {
	source = source.LocalMask()
	message := self.providerDiagnosticsMessage(source.SourceId)
	if message == nil || self.client == nil {
		return
	}
	// Diagnostics are rare lifecycle telemetry, not packet data. Use an
	// ordinary GC-owned protobuf buffer so an asynchronously closing first-send
	// sequence cannot transiently retain a message-pool root across provider
	// lifecycle accounting.
	messageBytes, err := proto.Marshal(message)
	if err != nil {
		return
	}
	frame := &protocol.Frame{
		MessageType:  protocol.MessageType_IpIpProviderDiagnostics,
		MessageBytes: messageBytes,
	}
	returnProvideMode := self.sourceReturnProvideMode(source.SourceId, provideMode)
	returnTransferKey := providerReplyTransferKey(transferKey, returnProvideMode)
	returnOptions := providerReturnTransferOptions(
		self.client.settings.DefaultTransferOpts,
		returnProvideMode,
		returnTransferKey,
	)
	if self.client.SendWithTimeout(
		frame,
		source.SourceId,
		func(error) {},
		0,
		returnOptions,
		returnTransferKey,
		Ctx(self.ctx),
	) {
		self.markProviderDiagnosticsPublished(source.SourceId, message.Sequence)
		return
	}
}

func providerDiagnosticsFromProtocol(message *protocol.IpProviderDiagnostics) *ProviderDiagnostics {
	if message == nil {
		return nil
	}
	return &ProviderDiagnostics{
		BuildVersion:            message.BuildVersion,
		SecurityPolicyHash:      message.SecurityPolicyHash,
		BlockIngressPacketCount: int64(message.BlockIngressPacketCount),
		BlockIngressByteCount:   int64(message.BlockIngressByteCount),
		BlockEgressPacketCount:  int64(message.BlockEgressPacketCount),
		BlockEgressByteCount:    int64(message.BlockEgressByteCount),
		Sequence:                int64(message.Sequence),
	}
}
