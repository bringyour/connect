package connect

// Hand-rolled protobuf wire codec for the hot transfer frames.
//
// The transfer data plane marshals a `protocol.TransferFrame` carrying a
// `protocol.Pack` for every packet, and the reflection-based proto marshal —
// plus the intermediate Pack/TransferFrame/Tag/TransferPath structs and the
// `Id.Bytes()` slices they require — dominates the per-packet allocation
// profile. This file encodes that frame directly into a pooled buffer with no
// intermediate structs, no `Id.Bytes()` escapes, and no reflection.
//
// Wire compatibility is the invariant: output here must be byte-identical to
// `proto.Marshal` of the equivalent structs (verified by round-trip tests in
// frame_protobuf_test.go), so peers and the platform — and our own
// `setHead` re-marshal path, which `proto.Unmarshal`s these bytes — decode it
// unchanged. Encoding follows field-number order with proto3 presence rules
// (implicit scalars omit zero values; `optional`/message/`bytes` fields are
// emitted when set).
//
// This file also holds the allocation-free `FilteredTransferPath` reader used
// on the routing path, keeping all custom protobuf logic together.

import (
	"errors"
	"sync"
	"sync/atomic"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/urnetwork/connect/v2026/protocol"
)

// proto wire types
const (
	protoWireVarint = 0
	protoWireBytes  = 2
)

func protoSizeVarint(v uint64) int {
	n := 1
	for v >= 0x80 {
		v >>= 7
		n += 1
	}
	return n
}

func protoAppendVarint(b []byte, v uint64) []byte {
	for v >= 0x80 {
		b = append(b, byte(v)|0x80)
		v >>= 7
	}
	return append(b, byte(v))
}

// protoTag is (fieldNumber << 3) | wireType. All field numbers used here are
// small enough that the tag is a single byte, but go through the varint helpers
// for correctness.
func protoSizeTag(fieldNumber int) int {
	return protoSizeVarint(uint64(fieldNumber) << 3)
}

func protoAppendTag(b []byte, fieldNumber int, wireType int) []byte {
	return protoAppendVarint(b, uint64(fieldNumber)<<3|uint64(wireType))
}

// --- protocol.Frame { message_type=1, message_bytes=2, raw=3 } ---

func sizeFrame(f *protocol.Frame) int {
	n := 0
	if f.MessageType != 0 {
		n += protoSizeTag(1) + protoSizeVarint(uint64(f.MessageType))
	}
	if 0 < len(f.MessageBytes) {
		n += protoSizeTag(2) + protoSizeVarint(uint64(len(f.MessageBytes))) + len(f.MessageBytes)
	}
	if f.Raw {
		n += protoSizeTag(3) + 1
	}
	return n
}

func appendFrame(b []byte, f *protocol.Frame) []byte {
	if f.MessageType != 0 {
		b = protoAppendTag(b, 1, protoWireVarint)
		b = protoAppendVarint(b, uint64(f.MessageType))
	}
	if 0 < len(f.MessageBytes) {
		b = protoAppendTag(b, 2, protoWireBytes)
		b = protoAppendVarint(b, uint64(len(f.MessageBytes)))
		b = append(b, f.MessageBytes...)
	}
	if f.Raw {
		b = protoAppendTag(b, 3, protoWireVarint)
		b = append(b, 1)
	}
	return b
}

// --- protocol.TransferPath { destination_id=1, source_id=2, stream_id=3 } ---
// each id is an optional 16-byte value, emitted only when non-zero (matching
// TransferPath.ToProtobuf).

func sizeTransferPath(path TransferPath) int {
	n := 0
	if (path.DestinationId != Id{}) {
		n += protoSizeTag(1) + protoSizeVarint(16) + 16
	}
	if (path.SourceId != Id{}) {
		n += protoSizeTag(2) + protoSizeVarint(16) + 16
	}
	if (path.StreamId != Id{}) {
		n += protoSizeTag(3) + protoSizeVarint(16) + 16
	}
	return n
}

func appendIdField(b []byte, fieldNumber int, id Id) []byte {
	b = protoAppendTag(b, fieldNumber, protoWireBytes)
	b = protoAppendVarint(b, 16)
	return append(b, id[:]...)
}

func appendTransferPath(b []byte, path TransferPath) []byte {
	if (path.DestinationId != Id{}) {
		b = appendIdField(b, 1, path.DestinationId)
	}
	if (path.SourceId != Id{}) {
		b = appendIdField(b, 2, path.SourceId)
	}
	if (path.StreamId != Id{}) {
		b = appendIdField(b, 3, path.StreamId)
	}
	return b
}

// --- send-side Pack TransferFrame ---

// sendPackFrame is the set of values the per-packet send path encodes into a
// TransferFrame carrying a Pack. Passed by value (no heap alloc) to the codec.
type sendPackFrame struct {
	path           TransferPath
	messageId      Id
	sequenceId     Id
	sequenceNumber uint64
	head           bool
	nack           bool
	frames         []*protocol.Frame
	contractFrame  *protocol.Frame // nil when absent
	// tagSendTime is the Pack.tag.send_time (ms). The Tag message is always
	// emitted (matching rttWindow.OpenTag); send_time itself is omitted only
	// when zero, per implicit presence.
	tagSendTime uint64
	contractId  *Id // nil when absent (only set for nack packs)
	// sessionRole is emitted (TransferFrame field 7) only when sessionRoleSet.
	sessionRole    protocol.SequenceRole
	sessionRoleSet bool
	// companion is emitted (TransferFrame field 8) only when true.
	companion bool
	// sequence lane discriminators (Pack fields 10/11), emitted only when
	// true (proto3 implicit presence; false is the legacy default lane)
	forceStream       bool
	companionContract bool
	// logicalLane is Pack field 12. Zero is the legacy/control lane and is
	// omitted; nonzero values are capability-negotiated and bounded to 1..8.
	logicalLane uint32
}

// sizePack returns the encoded size of the Pack submessage body.
func (m *sendPackFrame) sizePack() int {
	n := 0
	// message_id=1, sequence_id=2 (always 16-byte non-optional bytes)
	n += protoSizeTag(1) + protoSizeVarint(16) + 16
	n += protoSizeTag(2) + protoSizeVarint(16) + 16
	if m.sequenceNumber != 0 {
		n += protoSizeTag(3) + protoSizeVarint(m.sequenceNumber)
	}
	if m.head {
		n += protoSizeTag(4) + 1
	}
	for _, f := range m.frames {
		fs := sizeFrame(f)
		n += protoSizeTag(5) + protoSizeVarint(uint64(fs)) + fs
	}
	if m.nack {
		n += protoSizeTag(6) + 1
	}
	if m.contractFrame != nil {
		fs := sizeFrame(m.contractFrame)
		n += protoSizeTag(7) + protoSizeVarint(uint64(fs)) + fs
	}
	// tag=8 (always present); body is send_time=1 when non-zero
	tagBody := 0
	if m.tagSendTime != 0 {
		tagBody = protoSizeTag(1) + protoSizeVarint(m.tagSendTime)
	}
	n += protoSizeTag(8) + protoSizeVarint(uint64(tagBody)) + tagBody
	if m.contractId != nil {
		n += protoSizeTag(9) + protoSizeVarint(16) + 16
	}
	if m.forceStream {
		n += protoSizeTag(10) + 1
	}
	if m.companionContract {
		n += protoSizeTag(11) + 1
	}
	if m.logicalLane != 0 {
		n += protoSizeTag(12) + protoSizeVarint(uint64(m.logicalLane))
	}
	return n
}

func (m *sendPackFrame) appendPack(b []byte) []byte {
	b = appendIdField(b, 1, m.messageId)
	b = appendIdField(b, 2, m.sequenceId)
	if m.sequenceNumber != 0 {
		b = protoAppendTag(b, 3, protoWireVarint)
		b = protoAppendVarint(b, m.sequenceNumber)
	}
	if m.head {
		b = protoAppendTag(b, 4, protoWireVarint)
		b = append(b, 1)
	}
	for _, f := range m.frames {
		b = protoAppendTag(b, 5, protoWireBytes)
		b = protoAppendVarint(b, uint64(sizeFrame(f)))
		b = appendFrame(b, f)
	}
	if m.nack {
		b = protoAppendTag(b, 6, protoWireVarint)
		b = append(b, 1)
	}
	if m.contractFrame != nil {
		b = protoAppendTag(b, 7, protoWireBytes)
		b = protoAppendVarint(b, uint64(sizeFrame(m.contractFrame)))
		b = appendFrame(b, m.contractFrame)
	}
	// tag=8
	tagBody := 0
	if m.tagSendTime != 0 {
		tagBody = protoSizeTag(1) + protoSizeVarint(m.tagSendTime)
	}
	b = protoAppendTag(b, 8, protoWireBytes)
	b = protoAppendVarint(b, uint64(tagBody))
	if m.tagSendTime != 0 {
		b = protoAppendTag(b, 1, protoWireVarint)
		b = protoAppendVarint(b, m.tagSendTime)
	}
	if m.contractId != nil {
		b = appendIdField(b, 9, *m.contractId)
	}
	if m.forceStream {
		b = protoAppendTag(b, 10, protoWireVarint)
		b = append(b, 1)
	}
	if m.companionContract {
		b = protoAppendTag(b, 11, protoWireVarint)
		b = append(b, 1)
	}
	if m.logicalLane != 0 {
		b = protoAppendTag(b, 12, protoWireVarint)
		b = protoAppendVarint(b, uint64(m.logicalLane))
	}
	return b
}

// size returns the encoded size of the whole TransferFrame.
func (m *sendPackFrame) size() int {
	n := 0
	// transfer_path=1
	pathSize := sizeTransferPath(m.path)
	n += protoSizeTag(1) + protoSizeVarint(uint64(pathSize)) + pathSize
	// message_type=3 (optional, always set to TransferPack=0 by the v2 send path)
	n += protoSizeTag(3) + protoSizeVarint(uint64(protocol.MessageType_TransferPack))
	// pack=4
	packSize := m.sizePack()
	n += protoSizeTag(4) + protoSizeVarint(uint64(packSize)) + packSize
	// session_role=7
	if m.sessionRoleSet {
		n += protoSizeTag(7) + protoSizeVarint(uint64(m.sessionRole))
	}
	// session_companion=8
	if m.companion {
		n += protoSizeTag(8) + 1
	}
	return n
}

func (m *sendPackFrame) append(b []byte) []byte {
	// transfer_path=1
	b = protoAppendTag(b, 1, protoWireBytes)
	b = protoAppendVarint(b, uint64(sizeTransferPath(m.path)))
	b = appendTransferPath(b, m.path)
	// message_type=3 = TransferPack (0)
	b = protoAppendTag(b, 3, protoWireVarint)
	b = protoAppendVarint(b, uint64(protocol.MessageType_TransferPack))
	// pack=4
	b = protoAppendTag(b, 4, protoWireBytes)
	b = protoAppendVarint(b, uint64(m.sizePack()))
	b = m.appendPack(b)
	// session_role=7
	if m.sessionRoleSet {
		b = protoAppendTag(b, 7, protoWireVarint)
		b = protoAppendVarint(b, uint64(m.sessionRole))
	}
	// session_companion=8
	if m.companion {
		b = protoAppendTag(b, 8, protoWireVarint)
		b = append(b, 1)
	}
	return b
}

// marshalSendPackTransferFrame encodes the TransferFrame into a pooled buffer.
// The returned slice is owned by the caller (MessagePoolReturn when done). It is
// byte-identical to ProtoMarshal of the equivalent structs (see tests).
func marshalSendPackTransferFrame(m *sendPackFrame) []byte {
	size := m.size()
	buf := MessagePoolGet(size)
	out := m.append(buf[:0])
	// size() is exact, so append never grows past the pooled capacity; guard
	// anyway so a sizing bug surfaces as a returned-to-pool orphan, not a leak.
	if cap(out) != cap(buf) {
		MessagePoolReturn(buf)
	}
	return out
}

// --- send-side Ack TransferFrame ---

// sendAckFrame is the set of values the ack-writer encodes into a TransferFrame
// carrying an Ack. The inner ack frame is not session-stamped (wrapping, when
// it happens, adds the role/companion hint to the outer encrypted frame).
type sendAckFrame struct {
	path                    TransferPath
	messageId               Id
	sequenceId              Id
	selective               bool
	tagSendTime             uint64
	tagSet                  bool
	missingContractId       *Id
	compactContractRecovery bool
	logicalLaneVersion      uint32
}

func (m *sendAckFrame) sizeAck() int {
	n := 0
	// message_id=1, sequence_id=2 (always 16-byte bytes)
	n += protoSizeTag(1) + protoSizeVarint(16) + 16
	n += protoSizeTag(2) + protoSizeVarint(16) + 16
	if m.selective {
		n += protoSizeTag(3) + 1
	}
	if m.tagSet {
		tagBody := sizeTagBody(m.tagSendTime)
		n += protoSizeTag(4) + protoSizeVarint(uint64(tagBody)) + tagBody
	}
	if m.missingContractId != nil {
		n += protoSizeTag(5) + protoSizeVarint(16) + 16
	}
	if m.compactContractRecovery {
		n += protoSizeTag(6) + 1
	}
	if m.logicalLaneVersion != 0 {
		n += protoSizeTag(7) + protoSizeVarint(uint64(m.logicalLaneVersion))
	}
	return n
}

func (m *sendAckFrame) appendAck(b []byte) []byte {
	b = appendIdField(b, 1, m.messageId)
	b = appendIdField(b, 2, m.sequenceId)
	if m.selective {
		b = protoAppendTag(b, 3, protoWireVarint)
		b = append(b, 1)
	}
	if m.tagSet {
		b = protoAppendTag(b, 4, protoWireBytes)
		b = protoAppendVarint(b, uint64(sizeTagBody(m.tagSendTime)))
		b = appendTagBody(b, m.tagSendTime)
	}
	if m.missingContractId != nil {
		b = appendIdField(b, 5, *m.missingContractId)
	}
	if m.compactContractRecovery {
		b = protoAppendTag(b, 6, protoWireVarint)
		b = append(b, 1)
	}
	if m.logicalLaneVersion != 0 {
		b = protoAppendTag(b, 7, protoWireVarint)
		b = protoAppendVarint(b, uint64(m.logicalLaneVersion))
	}
	return b
}

func (m *sendAckFrame) size() int {
	n := 0
	// transfer_path=1
	pathSize := sizeTransferPath(m.path)
	n += protoSizeTag(1) + protoSizeVarint(uint64(pathSize)) + pathSize
	// message_type=3 = TransferAck
	n += protoSizeTag(3) + protoSizeVarint(uint64(protocol.MessageType_TransferAck))
	// ack=5
	ackSize := m.sizeAck()
	n += protoSizeTag(5) + protoSizeVarint(uint64(ackSize)) + ackSize
	return n
}

func (m *sendAckFrame) append(b []byte) []byte {
	// transfer_path=1
	b = protoAppendTag(b, 1, protoWireBytes)
	b = protoAppendVarint(b, uint64(sizeTransferPath(m.path)))
	b = appendTransferPath(b, m.path)
	// message_type=3 = TransferAck
	b = protoAppendTag(b, 3, protoWireVarint)
	b = protoAppendVarint(b, uint64(protocol.MessageType_TransferAck))
	// ack=5
	b = protoAppendTag(b, 5, protoWireBytes)
	b = protoAppendVarint(b, uint64(m.sizeAck()))
	b = m.appendAck(b)
	return b
}

// marshalSendAckTransferFrame encodes the Ack TransferFrame into a pooled
// buffer (caller owns; MessagePoolReturn when done). Byte-identical to
// ProtoMarshal of the equivalent structs (see tests).
func marshalSendAckTransferFrame(m *sendAckFrame) []byte {
	size := m.size()
	buf := MessagePoolGet(size)
	out := m.append(buf[:0])
	if cap(out) != cap(buf) {
		MessagePoolReturn(buf)
	}
	return out
}

// --- encrypted outer TransferFrame ---
//
// The encrypted carrier has only transfer_path=1,
// encrypted_transfer_frame=6, optional session_role=7, and the optional
// session_companion=8 presence bit. The sender deliberately sets
// session_companion even when false, so it is always emitted here. Keeping the
// sizing/header/footer helpers separate lets sequenceCipher seal directly into
// the pooled protobuf output buffer: no temporary ciphertext allocation and no
// ciphertext copy.

func sizeEncryptedOuterTransferFrame(
	path TransferPath,
	ciphertextLen int,
	sessionRole protocol.SequenceRole,
) int {
	pathSize := sizeTransferPath(path)
	n := protoSizeTag(1) + protoSizeVarint(uint64(pathSize)) + pathSize
	n += protoSizeTag(6) + protoSizeVarint(uint64(ciphertextLen)) + ciphertextLen
	if sessionRole != protocol.SequenceRole_SequenceRoleUnknown {
		n += protoSizeTag(7) + protoSizeVarint(uint64(sessionRole))
	}
	// session_companion is an optional bool and is always present, including
	// when its value is false.
	n += protoSizeTag(8) + 1
	return n
}

func appendEncryptedOuterTransferFrameHeader(
	b []byte,
	path TransferPath,
	ciphertextLen int,
) []byte {
	pathSize := sizeTransferPath(path)
	b = protoAppendTag(b, 1, protoWireBytes)
	b = protoAppendVarint(b, uint64(pathSize))
	b = appendTransferPath(b, path)
	b = protoAppendTag(b, 6, protoWireBytes)
	b = protoAppendVarint(b, uint64(ciphertextLen))
	return b
}

func appendEncryptedOuterTransferFrameFooter(
	b []byte,
	sessionRole protocol.SequenceRole,
	sessionCompanion bool,
) []byte {
	if sessionRole != protocol.SequenceRole_SequenceRoleUnknown {
		b = protoAppendTag(b, 7, protoWireVarint)
		b = protoAppendVarint(b, uint64(sessionRole))
	}
	b = protoAppendTag(b, 8, protoWireVarint)
	if sessionCompanion {
		return append(b, 1)
	}
	return append(b, 0)
}

func marshalEncryptedOuterTransferFrame(
	path TransferPath,
	ciphertext []byte,
	sessionRole protocol.SequenceRole,
	sessionCompanion bool,
) []byte {
	size := sizeEncryptedOuterTransferFrame(path, len(ciphertext), sessionRole)
	buf := MessagePoolGet(size)
	out := appendEncryptedOuterTransferFrameHeader(buf[:0], path, len(ciphertext))
	out = append(out, ciphertext...)
	out = appendEncryptedOuterTransferFrameFooter(out, sessionRole, sessionCompanion)
	if cap(out) != cap(buf) {
		MessagePoolReturn(buf)
	}
	return out
}

// sizeTagBody / appendTagBody encode the body of a Tag submessage
// (send_time=1, omitted when zero per implicit presence).
func sizeTagBody(sendTime uint64) int {
	if sendTime != 0 {
		return protoSizeTag(1) + protoSizeVarint(sendTime)
	}
	return 0
}

func appendTagBody(b []byte, sendTime uint64) []byte {
	if sendTime != 0 {
		b = protoAppendTag(b, 1, protoWireVarint)
		b = protoAppendVarint(b, sendTime)
	}
	return b
}

// --- receive-side decode (copy-safe) ---
//
// Hand-rolled decode of an inbound TransferFrame into the existing protocol
// structs, using the official protowire primitives for parsing but allocating
// messages directly (no reflection) and copying queued/callback bytes fields
// exactly as the proto library does. The one deliberate alias is
// encrypted_transfer_frame: it is consumed synchronously by AEAD.Open before
// the outer receive buffer is returned, never queued or exposed to callbacks.
// Two fields are also intentionally dropped:
//   - the deprecated TransferFrame.message_type (field 3) is always skipped.
//   - the outer transfer_path is skipped unless decodePath; routing parses the
//     path separately via FilteredTransferPath, and only the inner/unwrapped
//     frame reads its path (for the tamper check).
//
// Returns false on malformed input, which the caller treats as a bad message —
// matching ProtoUnmarshal's error. setHead still uses ProtoUnmarshal: it
// re-marshals the frame and must preserve every field, including the path.

func copyProtoBytes(v []byte) []byte {
	return append([]byte(nil), v...)
}

// decodedPackOwner is the explicit lifetime container for the receive hot
// path. The protocol.Pack and its common-case Frames point into this object:
// IDs use inline 16-byte storage, the normal one/two-frame pack uses inline
// Frame values and pointer storage, and Tag is inline as well. The owner is
// carried from Client.run through ReceivePack/receiveItem and returned only
// after the synchronous receive callback and ACK tag copy have completed.
//
// This is deliberately not a sync.Pool. A sync.Pool has no retained-size
// bound and is cleared at GC-defined times. The small sharded free-list below
// retains at most decodedPackOwnerPoolCapacity objects, giving reuse without
// turning protocol traffic bursts into an unpredictable RSS cache.
const (
	decodedPackOwnerInlineFrames = sendPackBatchMaxFrames
	decodedPackOwnerPoolShards   = 4
	// Free objects, not in-flight objects. 256 covers the parallel receive
	// width of the production-shaped provider kernel (166 owners created in
	// the exact profile) while bounding retained protocol + pipeline state to
	// roughly 242 KiB (968 bytes/owner on arm64). The previous 1024 x 536-byte
	// protocol-only cache had a ~536 KiB worst retained bound and still
	// allocated separate ReceivePack/receiveItem envelopes.
	decodedPackOwnerPoolCapacity = 256
)

type decodedPackOwner struct {
	pack protocol.Pack

	messageId  Id
	sequenceId Id
	contractId Id

	frames        [decodedPackOwnerInlineFrames]protocol.Frame
	framePointers [decodedPackOwnerInlineFrames]*protocol.Frame
	contractFrame protocol.Frame
	tag           protocol.Tag

	// The decoded protocol values and these two pipeline envelopes have one
	// strict succession: decode -> ReceivePack admission -> receiveItem queue
	// / callback -> release. Keeping them in the same owner removes two heap
	// objects per inbound pack without adding another pool, lock, or lifetime.
	receivePack ReceivePack
	receiveItem receiveItem

	inUse bool
}

type decodedPackOwnerPoolShard struct {
	mu    sync.Mutex
	free  []*decodedPackOwner
	limit int
}

type decodedPackOwnerPool struct {
	next   atomic.Uint64
	shards [decodedPackOwnerPoolShards]decodedPackOwnerPoolShard
}

func newDecodedPackOwnerPool() *decodedPackOwnerPool {
	pool := &decodedPackOwnerPool{}
	for i := range decodedPackOwnerPoolShards {
		limit := decodedPackOwnerPoolCapacity / decodedPackOwnerPoolShards
		if i < decodedPackOwnerPoolCapacity%decodedPackOwnerPoolShards {
			limit += 1
		}
		pool.shards[i].limit = limit
		pool.shards[i].free = make([]*decodedPackOwner, 0, limit)
	}
	return pool
}

var inboundDecodedPackOwners = newDecodedPackOwnerPool()

func (pool *decodedPackOwnerPool) take() *decodedPackOwner {
	start := int((pool.next.Add(1) - 1) % decodedPackOwnerPoolShards)
	for offset := range decodedPackOwnerPoolShards {
		shard := &pool.shards[(start+offset)%decodedPackOwnerPoolShards]
		shard.mu.Lock()
		n := len(shard.free)
		if 0 < n {
			owner := shard.free[n-1]
			shard.free[n-1] = nil
			shard.free = shard.free[:n-1]
			shard.mu.Unlock()
			owner.resetForDecode()
			return owner
		}
		shard.mu.Unlock()
	}
	owner := &decodedPackOwner{}
	owner.resetForDecode()
	return owner
}

func (pool *decodedPackOwnerPool) put(owner *decodedPackOwner) {
	start := int((pool.next.Add(1) - 1) % decodedPackOwnerPoolShards)
	for offset := range decodedPackOwnerPoolShards {
		shard := &pool.shards[(start+offset)%decodedPackOwnerPoolShards]
		shard.mu.Lock()
		if len(shard.free) < shard.limit {
			shard.free = append(shard.free, owner)
			shard.mu.Unlock()
			return
		}
		shard.mu.Unlock()
	}
	// The exact aggregate retention cap is full. Let this owner be collected.
}

func (owner *decodedPackOwner) resetForDecode() {
	owner.pack = protocol.Pack{}
	owner.messageId = Id{}
	owner.sequenceId = Id{}
	owner.contractId = Id{}
	for i := range owner.frames {
		owner.frames[i] = protocol.Frame{}
		owner.framePointers[i] = nil
	}
	owner.contractFrame = protocol.Frame{}
	owner.tag = protocol.Tag{}
	owner.receivePack = ReceivePack{}
	owner.receiveItem = receiveItem{}
	owner.pack.Frames = owner.framePointers[:0]
	owner.inUse = true
}

func (owner *decodedPackOwner) release() {
	if owner == nil || !owner.inUse {
		return
	}
	returnDecodedPackMessageBytes(&owner.pack)
	owner.pack = protocol.Pack{}
	for i := range owner.frames {
		owner.frames[i] = protocol.Frame{}
		owner.framePointers[i] = nil
	}
	owner.contractFrame = protocol.Frame{}
	owner.tag = protocol.Tag{}
	owner.receivePack = ReceivePack{}
	owner.receiveItem = receiveItem{}
	owner.inUse = false
	inboundDecodedPackOwners.put(owner)
}

func setInlineProtoId(dst *[]byte, storage *Id, value []byte) bool {
	if len(value) != len(storage) {
		return false
	}
	copy(storage[:], value)
	*dst = storage[:]
	return true
}

func (owner *decodedPackOwner) nextFrame() *protocol.Frame {
	index := len(owner.pack.Frames)
	if index < len(owner.frames) {
		frame := &owner.frames[index]
		owner.framePointers[index] = frame
		return frame
	}
	// A conforming current sender emits at most two data frames per Pack.
	// Preserve wire compatibility for legacy/untrusted peers with more fields,
	// but keep that exceptional memory proportional to the exceptional input.
	return &protocol.Frame{}
}

// decodedTransferFrame keeps every non-queued receive-side protobuf wrapper
// either on the caller's stack or in decodedPackOwner. The path/role fields are
// only inspected synchronously; Pack ownership can be detached and handed to
// the receive queue.
type decodedTransferFrame struct {
	frame protocol.TransferFrame

	path        protocol.TransferPath
	pathIds     [3]Id
	carrier     protocol.Frame
	packOwner   *decodedPackOwner
	ack         protocol.Ack
	ackIds      [3]Id
	ackTag      protocol.Tag
	sessionRole protocol.SequenceRole
	companion   bool
}

// decodedTransferFrame itself contains pointer-bearing protobuf views into its
// inline path/role storage, so Go conservatively moves it to the heap. Reuse
// those synchronous wrappers through a second, much smaller bounded cache.
// Inline ACK storage raises the arm64 owner to 608 bytes; the 256-object cap
// therefore retains at most 152 KiB while removing 240 B / five allocations
// from every received ACK.
const decodedTransferFramePoolCapacity = 256

type decodedTransferFramePoolShard struct {
	mu    sync.Mutex
	free  []*decodedTransferFrame
	limit int
}

type decodedTransferFramePool struct {
	next   atomic.Uint64
	shards [decodedPackOwnerPoolShards]decodedTransferFramePoolShard
}

func newDecodedTransferFramePool() *decodedTransferFramePool {
	pool := &decodedTransferFramePool{}
	for i := range decodedPackOwnerPoolShards {
		limit := decodedTransferFramePoolCapacity / decodedPackOwnerPoolShards
		if i < decodedTransferFramePoolCapacity%decodedPackOwnerPoolShards {
			limit += 1
		}
		pool.shards[i].limit = limit
		pool.shards[i].free = make([]*decodedTransferFrame, 0, limit)
	}
	return pool
}

var inboundDecodedTransferFrames = newDecodedTransferFramePool()

func (pool *decodedTransferFramePool) take() *decodedTransferFrame {
	start := int((pool.next.Add(1) - 1) % decodedPackOwnerPoolShards)
	for offset := range decodedPackOwnerPoolShards {
		shard := &pool.shards[(start+offset)%decodedPackOwnerPoolShards]
		shard.mu.Lock()
		n := len(shard.free)
		if 0 < n {
			decoded := shard.free[n-1]
			shard.free[n-1] = nil
			shard.free = shard.free[:n-1]
			shard.mu.Unlock()
			return decoded
		}
		shard.mu.Unlock()
	}
	return &decodedTransferFrame{}
}

func (pool *decodedTransferFramePool) put(decoded *decodedTransferFrame) {
	if decoded == nil {
		return
	}
	decoded.release()
	*decoded = decodedTransferFrame{}

	start := int((pool.next.Add(1) - 1) % decodedPackOwnerPoolShards)
	for offset := range decodedPackOwnerPoolShards {
		shard := &pool.shards[(start+offset)%decodedPackOwnerPoolShards]
		shard.mu.Lock()
		if len(shard.free) < shard.limit {
			shard.free = append(shard.free, decoded)
			shard.mu.Unlock()
			return
		}
		shard.mu.Unlock()
	}
}

func (decoded *decodedTransferFrame) release() {
	if decoded == nil {
		return
	}
	returnDecodedFrameMessageBytes(decoded.frame.Frame)
	decoded.frame.Frame = nil
	if decoded.packOwner != nil {
		decoded.packOwner.release()
		decoded.packOwner = nil
		decoded.frame.Pack = nil
	} else {
		returnDecodedPackMessageBytes(decoded.frame.Pack)
		decoded.frame.Pack = nil
	}
	decoded.frame.TransferPath = nil
	decoded.frame.Ack = nil
	decoded.frame.EncryptedTransferFrame = nil
	decoded.frame.SessionRole = nil
	decoded.frame.SessionCompanion = nil
}

func (decoded *decodedTransferFrame) detachPackOwner() *decodedPackOwner {
	if decoded == nil {
		return nil
	}
	owner := decoded.packOwner
	decoded.packOwner = nil
	decoded.frame.Pack = nil
	return owner
}

// Decoded Frame.message_bytes use the bounded message pools rather than one
// GC allocation per wire frame. Their lifetime is tied to the decoded
// TransferFrame/receive item; these helpers centralize every release path.
// MessagePoolReturn is a no-op for legacy proto.Unmarshal allocations, so the
// helpers are also safe on mixed v1/v2 values.
func returnDecodedFrameMessageBytes(f *protocol.Frame) {
	if f != nil {
		MessagePoolReturn(f.MessageBytes)
		f.MessageBytes = nil
	}
}

func returnDecodedPackMessageBytes(pack *protocol.Pack) {
	if pack == nil {
		return
	}
	for _, f := range pack.Frames {
		returnDecodedFrameMessageBytes(f)
	}
	returnDecodedFrameMessageBytes(pack.ContractFrame)
	pack.Frames = nil
	pack.ContractFrame = nil
}

func returnDecodedTransferFrameMessageBytes(tf *protocol.TransferFrame) {
	if tf == nil {
		return
	}
	returnDecodedFrameMessageBytes(tf.Frame)
	returnDecodedPackMessageBytes(tf.Pack)
	tf.Frame = nil
	tf.Pack = nil
}

func decodeTagInto(b []byte, tag *protocol.Tag) bool {
	*tag = protocol.Tag{}
	for 0 < len(b) {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return false
		}
		b = b[n:]
		if num == 1 { // send_time
			if typ != protowire.VarintType {
				return false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			tag.SendTime = v
			continue
		}
		fn := protowire.ConsumeFieldValue(num, typ, b)
		if fn < 0 {
			return false
		}
		b = b[fn:]
	}
	return true
}

func decodeTag(b []byte) (*protocol.Tag, bool) {
	tag := &protocol.Tag{}
	if !decodeTagInto(b, tag) {
		return nil, false
	}
	return tag, true
}

func decodeFrameInto(b []byte, f *protocol.Frame) (ok bool) {
	*f = protocol.Frame{}
	defer func() {
		if !ok {
			returnDecodedFrameMessageBytes(f)
		}
	}()
	for 0 < len(b) {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return false
		}
		b = b[n:]
		switch num {
		case 1: // message_type
			if typ != protowire.VarintType {
				return false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			f.MessageType = protocol.MessageType(v)
		case 2: // message_bytes
			if typ != protowire.BytesType {
				return false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			// Last value wins for a repeated singular field, matching proto.
			returnDecodedFrameMessageBytes(f)
			if 0 < len(v) {
				f.MessageBytes = MessagePoolCopy(v)
			}
		case 3: // raw
			if typ != protowire.VarintType {
				return false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			f.Raw = protowire.DecodeBool(v)
		default:
			fn := protowire.ConsumeFieldValue(num, typ, b)
			if fn < 0 {
				return false
			}
			b = b[fn:]
		}
	}
	return true
}

func decodeFrame(b []byte) (*protocol.Frame, bool) {
	f := &protocol.Frame{}
	if !decodeFrameInto(b, f) {
		return nil, false
	}
	return f, true
}

func decodeTransferPathProtoInto(b []byte, path *protocol.TransferPath, idStorage *[3]Id) bool {
	*path = protocol.TransferPath{}
	for 0 < len(b) {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return false
		}
		b = b[n:]
		switch num {
		case 1, 2, 3:
			if typ != protowire.BytesType {
				return false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			var dst *[]byte
			switch num {
			case 1:
				dst = &path.DestinationId
			case 2:
				dst = &path.SourceId
			case 3:
				dst = &path.StreamId
			}
			if idStorage == nil {
				*dst = copyProtoBytes(v)
			} else if !setInlineProtoId(dst, &idStorage[num-1], v) {
				return false
			}
		default:
			fn := protowire.ConsumeFieldValue(num, typ, b)
			if fn < 0 {
				return false
			}
			b = b[fn:]
		}
	}
	return true
}

func decodeTransferPathProto(b []byte) (*protocol.TransferPath, bool) {
	path := &protocol.TransferPath{}
	if !decodeTransferPathProtoInto(b, path, nil) {
		return nil, false
	}
	return path, true
}

func decodePack(b []byte) (pack *protocol.Pack, success bool) {
	pack = &protocol.Pack{}
	defer func() {
		if !success {
			returnDecodedPackMessageBytes(pack)
		}
	}()
	for 0 < len(b) {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, false
		}
		b = b[n:]
		switch num {
		case 1, 2, 9: // message_id, sequence_id, contract_id (bytes)
			if typ != protowire.BytesType {
				return nil, false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			switch num {
			case 1:
				pack.MessageId = copyProtoBytes(v)
			case 2:
				pack.SequenceId = copyProtoBytes(v)
			case 9:
				pack.ContractId = copyProtoBytes(v)
			}
		case 3: // sequence_number
			if typ != protowire.VarintType {
				return nil, false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			pack.SequenceNumber = v
		case 4: // head
			if typ != protowire.VarintType {
				return nil, false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			pack.Head = protowire.DecodeBool(v)
		case 6: // nack
			if typ != protowire.VarintType {
				return nil, false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			pack.Nack = protowire.DecodeBool(v)
		case 10, 11, 12: // force_stream, companion_contract, logical_lane
			if typ != protowire.VarintType {
				return nil, false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			if num == 10 {
				pack.ForceStream = protowire.DecodeBool(v)
			} else if num == 11 {
				pack.CompanionContract = protowire.DecodeBool(v)
			} else {
				pack.LogicalLane = uint32(v)
			}
		case 5, 7: // frames (repeated), contract_frame (Frame)
			if typ != protowire.BytesType {
				return nil, false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			f, ok := decodeFrame(v)
			if !ok {
				return nil, false
			}
			if num == 5 {
				pack.Frames = append(pack.Frames, f)
			} else {
				returnDecodedFrameMessageBytes(pack.ContractFrame)
				pack.ContractFrame = f
			}
		case 8: // tag (Tag)
			if typ != protowire.BytesType {
				return nil, false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			tg, ok := decodeTag(v)
			if !ok {
				return nil, false
			}
			pack.Tag = tg
		default:
			fn := protowire.ConsumeFieldValue(num, typ, b)
			if fn < 0 {
				return nil, false
			}
			b = b[fn:]
		}
	}
	return pack, true
}

func decodePackOwned(b []byte) (owner *decodedPackOwner, success bool) {
	owner = inboundDecodedPackOwners.take()
	pack := &owner.pack
	defer func() {
		if !success {
			owner.release()
			owner = nil
		}
	}()

	for 0 < len(b) {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, false
		}
		b = b[n:]
		switch num {
		case 1, 2, 9: // message_id, sequence_id, contract_id (16-byte ids)
			if typ != protowire.BytesType {
				return nil, false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			var dst *[]byte
			var storage *Id
			switch num {
			case 1:
				dst, storage = &pack.MessageId, &owner.messageId
			case 2:
				dst, storage = &pack.SequenceId, &owner.sequenceId
			case 9:
				dst, storage = &pack.ContractId, &owner.contractId
			}
			// These fields are protocol IDs, not arbitrary bytes. Rejecting a
			// wrong width here is equivalent to the caller's IdFromBytes check,
			// and prevents malformed input from forcing an exceptional heap copy.
			if !setInlineProtoId(dst, storage, v) {
				return nil, false
			}
		case 3: // sequence_number
			if typ != protowire.VarintType {
				return nil, false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			pack.SequenceNumber = v
		case 4: // head
			if typ != protowire.VarintType {
				return nil, false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			pack.Head = protowire.DecodeBool(v)
		case 6: // nack
			if typ != protowire.VarintType {
				return nil, false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			pack.Nack = protowire.DecodeBool(v)
		case 10, 11, 12: // force_stream, companion_contract, logical_lane
			if typ != protowire.VarintType {
				return nil, false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			if num == 10 {
				pack.ForceStream = protowire.DecodeBool(v)
			} else if num == 11 {
				pack.CompanionContract = protowire.DecodeBool(v)
			} else {
				pack.LogicalLane = uint32(v)
			}
		case 5: // frames (repeated)
			if typ != protowire.BytesType {
				return nil, false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			frame := owner.nextFrame()
			if !decodeFrameInto(v, frame) {
				return nil, false
			}
			pack.Frames = append(pack.Frames, frame)
		case 7: // contract_frame
			if typ != protowire.BytesType {
				return nil, false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			returnDecodedFrameMessageBytes(pack.ContractFrame)
			if !decodeFrameInto(v, &owner.contractFrame) {
				return nil, false
			}
			pack.ContractFrame = &owner.contractFrame
		case 8: // tag
			if typ != protowire.BytesType {
				return nil, false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			if !decodeTagInto(v, &owner.tag) {
				return nil, false
			}
			pack.Tag = &owner.tag
		default:
			fn := protowire.ConsumeFieldValue(num, typ, b)
			if fn < 0 {
				return nil, false
			}
			b = b[fn:]
		}
	}
	return owner, true
}

func decodeAck(b []byte) (*protocol.Ack, bool) {
	ack := &protocol.Ack{}
	for 0 < len(b) {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return nil, false
		}
		b = b[n:]
		switch num {
		case 1, 2: // message_id, sequence_id (bytes)
			if typ != protowire.BytesType {
				return nil, false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			if num == 1 {
				ack.MessageId = copyProtoBytes(v)
			} else {
				ack.SequenceId = copyProtoBytes(v)
			}
		case 3: // selective
			if typ != protowire.VarintType {
				return nil, false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			ack.Selective = protowire.DecodeBool(v)
		case 4: // tag (Tag)
			if typ != protowire.BytesType {
				return nil, false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			tg, ok := decodeTag(v)
			if !ok {
				return nil, false
			}
			ack.Tag = tg
		case 5: // missing_contract_id (bytes)
			if typ != protowire.BytesType {
				return nil, false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			ack.MissingContractId = copyProtoBytes(v)
		case 6: // compact_contract_recovery
			if typ != protowire.VarintType {
				return nil, false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			ack.CompactContractRecovery = protowire.DecodeBool(v)
		case 7: // logical_lane_version
			if typ != protowire.VarintType {
				return nil, false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return nil, false
			}
			b = b[vn:]
			ack.LogicalLaneVersion = uint32(v)
		default:
			fn := protowire.ConsumeFieldValue(num, typ, b)
			if fn < 0 {
				return nil, false
			}
			b = b[fn:]
		}
	}
	return ack, true
}

// decodeAckOwned keeps the synchronous ACK wrapper and its fixed-size IDs in
// decodedTransferFrame. Client.run copies the small semantic value into the
// destination SendSequence before returning that owner, so the ACK hot path
// does not allocate a protocol object or three byte slices per frame.
func decodeAckOwned(b []byte, decoded *decodedTransferFrame) bool {
	ack := &decoded.ack
	for 0 < len(b) {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return false
		}
		b = b[n:]
		switch num {
		case 1, 2: // message_id, sequence_id (bytes)
			if typ != protowire.BytesType {
				return false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			if num == 1 {
				if !setInlineProtoId(&ack.MessageId, &decoded.ackIds[0], v) {
					return false
				}
			} else if !setInlineProtoId(&ack.SequenceId, &decoded.ackIds[1], v) {
				return false
			}
		case 3: // selective
			if typ != protowire.VarintType {
				return false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			ack.Selective = protowire.DecodeBool(v)
		case 4: // tag (Tag)
			if typ != protowire.BytesType {
				return false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 || !decodeTagInto(v, &decoded.ackTag) {
				return false
			}
			b = b[vn:]
			ack.Tag = &decoded.ackTag
		case 5: // missing_contract_id (bytes)
			if typ != protowire.BytesType {
				return false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 || !setInlineProtoId(
				&ack.MissingContractId,
				&decoded.ackIds[2],
				v,
			) {
				return false
			}
			b = b[vn:]
		case 6: // compact_contract_recovery
			if typ != protowire.VarintType {
				return false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			ack.CompactContractRecovery = protowire.DecodeBool(v)
		case 7: // logical_lane_version
			if typ != protowire.VarintType {
				return false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			ack.LogicalLaneVersion = uint32(v)
		default:
			fn := protowire.ConsumeFieldValue(num, typ, b)
			if fn < 0 {
				return false
			}
			b = b[fn:]
		}
	}
	return true
}

// unmarshalTransferFrame decodes b into tf (see the section comment for the
// message_type/transfer_path deviations). decodePath controls whether the
// transfer_path is materialized (true for the inner/unwrapped frame, which is
// tamper-checked against the routing path; false for the outer frame).
//
// Tests and compatibility callers use this allocating form. Client.run uses
// unmarshalOwnedTransferFrame below so queued protocol values have an explicit
// bounded owner and the synchronous wrapper fields stay inline.
func unmarshalTransferFrame(b []byte, tf *protocol.TransferFrame, decodePath bool) bool {
	return unmarshalTransferFrameInto(b, tf, decodePath, nil)
}

func unmarshalOwnedTransferFrame(b []byte, decoded *decodedTransferFrame, decodePath bool) bool {
	decoded.release()
	*decoded = decodedTransferFrame{}
	return unmarshalTransferFrameInto(b, &decoded.frame, decodePath, decoded)
}

func unmarshalTransferFrameInto(
	b []byte,
	tf *protocol.TransferFrame,
	decodePath bool,
	decoded *decodedTransferFrame,
) bool {
	for 0 < len(b) {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return false
		}
		b = b[n:]
		switch num {
		case 1: // transfer_path
			if typ != protowire.BytesType {
				return false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			if decodePath {
				if decoded == nil {
					p, ok := decodeTransferPathProto(v)
					if !ok {
						return false
					}
					tf.TransferPath = p
				} else {
					if !decodeTransferPathProtoInto(v, &decoded.path, &decoded.pathIds) {
						return false
					}
					tf.TransferPath = &decoded.path
				}
			}
			// else skip: the outer path is unused (routing uses FilteredTransferPath)
		case 2: // frame (deprecated v1 carrier)
			if typ != protowire.BytesType {
				return false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			returnDecodedFrameMessageBytes(tf.Frame)
			if decoded == nil {
				f, ok := decodeFrame(v)
				if !ok {
					return false
				}
				tf.Frame = f
			} else {
				if !decodeFrameInto(v, &decoded.carrier) {
					return false
				}
				tf.Frame = &decoded.carrier
			}
		case 3: // message_type (deprecated) — skip, nothing reads it
			if typ != protowire.VarintType {
				return false
			}
			_, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
		case 4: // pack
			if typ != protowire.BytesType {
				return false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			if decoded == nil {
				p, ok := decodePack(v)
				if !ok {
					return false
				}
				returnDecodedPackMessageBytes(tf.Pack)
				tf.Pack = p
			} else {
				if decoded.packOwner != nil {
					decoded.packOwner.release()
					decoded.packOwner = nil
				}
				owner, ok := decodePackOwned(v)
				if !ok {
					return false
				}
				decoded.packOwner = owner
				tf.Pack = &owner.pack
			}
		case 5: // ack
			if typ != protowire.BytesType {
				return false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			if decoded == nil {
				a, ok := decodeAck(v)
				if !ok {
					return false
				}
				tf.Ack = a
			} else {
				if !decodeAckOwned(v, decoded) {
					return false
				}
				tf.Ack = &decoded.ack
			}
		case 6: // encryptedTransferFrame (bytes)
			if typ != protowire.BytesType {
				return false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			// The client decrypts this immediately, before returning the outer
			// transfer buffer. Aliasing avoids a full ciphertext allocation and
			// copy on every encrypted frame while preserving all queued/callback
			// field lifetimes.
			tf.EncryptedTransferFrame = v
		case 7: // session_role (enum)
			if typ != protowire.VarintType {
				return false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			if decoded == nil {
				sr := protocol.SequenceRole(v)
				tf.SessionRole = &sr
			} else {
				decoded.sessionRole = protocol.SequenceRole(v)
				tf.SessionRole = &decoded.sessionRole
			}
		case 8: // session_companion (bool)
			if typ != protowire.VarintType {
				return false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return false
			}
			b = b[vn:]
			if decoded == nil {
				c := protowire.DecodeBool(v)
				tf.SessionCompanion = &c
			} else {
				decoded.companion = protowire.DecodeBool(v)
				tf.SessionCompanion = &decoded.companion
			}
		default:
			fn := protowire.ConsumeFieldValue(num, typ, b)
			if fn < 0 {
				return false
			}
			b = b[fn:]
		}
	}
	return true
}

// parseFilteredTransferPath extracts the transfer path from an encoded
// `protocol.TransferFrame` without reflection or allocation. This runs for
// every frame a client routes (see `Client.run` and `Client.Forward`),
// where the reflection-based `FilteredTransferFrame` unmarshal dominates
// the allocation profile. Unknown fields are skipped the same as the
// protobuf decoder. `ok` is false on any unexpected encoding, in which
// case the caller must fall back to the full unmarshal.
func parseFilteredTransferPath(b []byte) (path TransferPath, exists bool, ok bool) {
	i := 0
	n := len(b)

	readVarint := func(limit int) (uint64, bool) {
		var v uint64
		var shift uint
		for {
			if limit <= i || 63 < shift {
				return 0, false
			}
			c := b[i]
			i += 1
			v |= uint64(c&0x7f) << shift
			if c < 0x80 {
				return v, true
			}
			shift += 7
		}
	}

	skipField := func(wireType uint64, limit int) bool {
		switch wireType {
		case 0:
			// varint
			_, varintOk := readVarint(limit)
			return varintOk
		case 1:
			// fixed 64
			if limit < i+8 {
				return false
			}
			i += 8
			return true
		case 2:
			// length delimited
			length, lengthOk := readVarint(limit)
			if !lengthOk || uint64(limit-i) < length {
				return false
			}
			i += int(length)
			return true
		case 5:
			// fixed 32
			if limit < i+4 {
				return false
			}
			i += 4
			return true
		default:
			// group types are not used by the protocol
			return false
		}
	}

	for i < n {
		tag, tagOk := readVarint(n)
		if !tagOk {
			return TransferPath{}, false, false
		}
		fieldNumber := tag >> 3
		wireType := tag & 0x7

		// TransferFrame { TransferPath transfer_path = 1; ... }
		if fieldNumber != 1 {
			if !skipField(wireType, n) {
				return TransferPath{}, false, false
			}
			continue
		}
		if wireType != 2 {
			return TransferPath{}, false, false
		}
		length, lengthOk := readVarint(n)
		if !lengthOk || uint64(n-i) < length {
			return TransferPath{}, false, false
		}
		end := i + int(length)
		exists = true

		// TransferPath {
		//     optional bytes destination_id = 1;
		//     optional bytes source_id = 2;
		//     optional bytes stream_id = 3;
		// }
		for i < end {
			pathTag, pathTagOk := readVarint(end)
			if !pathTagOk {
				return TransferPath{}, false, false
			}
			pathFieldNumber := pathTag >> 3
			pathWireType := pathTag & 0x7

			switch pathFieldNumber {
			case 1, 2, 3:
				if pathWireType != 2 {
					return TransferPath{}, false, false
				}
				idLength, idLengthOk := readVarint(end)
				if !idLengthOk || uint64(end-i) < idLength {
					return TransferPath{}, false, false
				}
				if idLength != 16 {
					// ids are always 16 bytes
					return TransferPath{}, false, false
				}
				var id Id
				copy(id[:], b[i:i+16])
				i += 16
				switch pathFieldNumber {
				case 1:
					path.DestinationId = id
				case 2:
					path.SourceId = id
				case 3:
					path.StreamId = id
				}
			default:
				if !skipField(pathWireType, end) {
					return TransferPath{}, false, false
				}
			}
		}
	}
	return path, exists, true
}

// FilteredTransferPath parses the transfer path of an encoded
// `protocol.TransferFrame`, using the allocation-free fast path with a
// full `FilteredTransferFrame` unmarshal fallback
func FilteredTransferPath(transferFrameBytes []byte) (TransferPath, error) {
	if path, exists, ok := parseFilteredTransferPath(transferFrameBytes); ok {
		if !exists {
			return TransferPath{}, errors.New("Missing transfer path")
		}
		return path, nil
	}
	// fall back to the full unmarshal
	filteredTransferFrame := &protocol.FilteredTransferFrame{}
	if err := ProtoUnmarshal(transferFrameBytes, filteredTransferFrame); err != nil {
		return TransferPath{}, err
	}
	if filteredTransferFrame.TransferPath == nil {
		return TransferPath{}, errors.New("Missing transfer path")
	}
	return TransferPathFromProtobuf(filteredTransferFrame.TransferPath)
}
