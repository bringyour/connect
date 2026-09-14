// This file contains the stream fast-path wire prototype. It deliberately
// separates end-to-end stream encryption from hop authentication: an
// intermediary can authenticate and route a packet without seeing its inner
// IP payload. A stream of any length is source + zero or more identical
// forwarders + destination.
//
// Cipher instances are directional and generation-local. Their methods are
// not safe for concurrent use: one per-stream sender assigns counters in
// order, and one per-stream receiver commits replay state in order. Parallel
// crypto workers must hand packets back to those ordered stages.
package connect

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hkdf"
	"crypto/sha256"
	"encoding/binary"
	"errors"
)

const (
	streamFastPathKeyByteCount            = 32
	streamFastPathAeadTagByteCount        = 16
	streamFastPathEndpointHeaderByteCount = 8
	streamFastPathHopHeaderByteCount      = 16
	streamFastPathReplayWindowWordCount   = 16
	streamFastPathReplayWindowBitCount    = 64 * streamFastPathReplayWindowWordCount
	// This leaves 16 bytes of headroom beneath a 1,500-byte outer IPv6 path.
	// Production selection must lower the current 1,440-byte TUN MTU first.
	streamFastPathMaxInnerPacketByteCount = 1380
	streamFastPathMaxWirePacketByteCount  = streamFastPathMaxInnerPacketByteCount +
		streamFastPathEndpointHeaderByteCount +
		streamFastPathHopHeaderByteCount +
		2*streamFastPathAeadTagByteCount
	streamFastPathVersion = 1
)

var (
	errStreamFastPathAuthentication = errors.New("stream fast path authentication failed")
	errStreamFastPathBatchTooSmall  = errors.New("stream fast path output batch too small")
	errStreamFastPathCounter        = errors.New("stream fast path packet counter exhausted")
	errStreamFastPathExporter       = errors.New("stream fast path exporter must be at least 32 bytes")
	errStreamFastPathKey            = errors.New("stream fast path key must be 32 bytes")
	errStreamFastPathPacket         = errors.New("invalid stream fast path packet")
	errStreamFastPathPacketTooLarge = errors.New("stream fast path packet is too large")
	errStreamFastPathReceiverIndex  = errors.New("stream fast path receiver index must be nonzero")
	errStreamFastPathReplay         = errors.New("stream fast path replay rejected")
)

// This domain separates end-to-end keys from adjacent-hop keys.
type streamFastPathKeyLayer byte

const (
	streamFastPathKeyLayerEndpoint streamFastPathKeyLayer = 1
	streamFastPathKeyLayerHop      streamFastPathKeyLayer = 2
)

// This domain prevents a packet from being reflected into the reverse stream.
type streamFastPathKeyDirection byte

const (
	streamFastPathKeyDirectionSourceToDestination streamFastPathKeyDirection = 1
	streamFastPathKeyDirectionDestinationToSource streamFastPathKeyDirection = 2
)

// Every value is a routing discriminator. Changing a stream generation, hop,
// or direction must produce an unrelated key.
type streamFastPathKeyContext struct {
	StreamId      Id
	GenerationId  Id
	ContractId    Id
	SourceId      Id
	DestinationId Id
	Layer         streamFastPathKeyLayer
	Direction     streamFastPathKeyDirection
}

// The exporter comes from the identity-bound TLS session. Endpoint keys use
// the end-to-end session exporter; hop keys use the adjacent peers' exporter.
func deriveStreamFastPathKey(
	exporter []byte,
	context streamFastPathKeyContext,
) ([streamFastPathKeyByteCount]byte, error) {
	var key [streamFastPathKeyByteCount]byte
	if len(exporter) < streamFastPathKeyByteCount {
		return key, errStreamFastPathExporter
	}

	const label = "urnetwork-stream-fast-path-v1"
	info := make([]byte, 0, len(label)+2+5*len(Id{}))
	info = append(info, label...)
	info = append(info, byte(context.Layer), byte(context.Direction))
	info = append(info, context.StreamId[:]...)
	info = append(info, context.GenerationId[:]...)
	info = append(info, context.ContractId[:]...)
	info = append(info, context.SourceId[:]...)
	info = append(info, context.DestinationId[:]...)

	derived, err := hkdf.Key(
		sha256.New,
		exporter,
		context.GenerationId[:],
		string(info),
		streamFastPathKeyByteCount,
	)
	if err != nil {
		return key, err
	}
	copy(key[:], derived)
	return key, nil
}

// The bit at distance d records maxCounter-d. A 1,024-packet window tolerates
// normal path reordering while placing a fixed bound on retained state.
type streamFastPathReplayWindow struct {
	maxCounter uint64
	seen       [streamFastPathReplayWindowWordCount]uint64
}

// This check never mutates state, so an unauthenticated future counter cannot
// move the window and evict legitimate in-flight packets.
func (self *streamFastPathReplayWindow) canAccept(counter uint64) bool {
	if counter == 0 {
		return false
	}
	if self.maxCounter == 0 || self.maxCounter < counter {
		return true
	}
	distance := self.maxCounter - counter
	if streamFastPathReplayWindowBitCount <= distance {
		return false
	}
	wordIndex := int(distance / 64)
	bitIndex := uint(distance % 64)
	return self.seen[wordIndex]&(uint64(1)<<bitIndex) == 0
}

// Call only after authentication succeeds. False means a duplicate or an
// out-of-window counter reached the ordered commit stage.
func (self *streamFastPathReplayWindow) accept(counter uint64) bool {
	if !self.canAccept(counter) {
		return false
	}
	if self.maxCounter < counter {
		advance := counter - self.maxCounter
		if self.maxCounter == 0 || streamFastPathReplayWindowBitCount <= advance {
			clear(self.seen[:])
		} else {
			wordAdvance := int(advance / 64)
			bitAdvance := uint(advance % 64)
			for destinationWordIndex := len(self.seen) - 1; 0 <= destinationWordIndex; destinationWordIndex -= 1 {
				sourceWordIndex := destinationWordIndex - wordAdvance
				var word uint64
				if 0 <= sourceWordIndex {
					word = self.seen[sourceWordIndex] << bitAdvance
					if bitAdvance != 0 && 0 <= sourceWordIndex-1 {
						word |= self.seen[sourceWordIndex-1] >> (64 - bitAdvance)
					}
				}
				self.seen[destinationWordIndex] = word
			}
		}
		self.maxCounter = counter
		self.seen[0] |= 1
		return true
	}

	distance := self.maxCounter - counter
	wordIndex := int(distance / 64)
	bitIndex := uint(distance % 64)
	self.seen[wordIndex] |= uint64(1) << bitIndex
	return true
}

// Keys are already direction- and generation-specific; AES-GCM gives the
// prototype the same hardware-accelerated primitive as sequenceCipher.
func newStreamFastPathAead(key []byte) (cipher.AEAD, error) {
	if len(key) != streamFastPathKeyByteCount {
		return nil, errStreamFastPathKey
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// The end-to-end envelope is [counter][ciphertext][tag]. Stream identity is
// implicit on the wire and authenticated as additional data.
type streamFastPathEndpointCipher struct {
	aead           cipher.AEAD
	sendCounter    uint64
	replayWindow   streamFastPathReplayWindow
	nonce          [12]byte
	additionalData [len(Id{}) + streamFastPathEndpointHeaderByteCount]byte
}

// A sender and receiver construct independent instances with the same key.
func newStreamFastPathEndpointCipher(
	streamId Id,
	key []byte,
) (*streamFastPathEndpointCipher, error) {
	aead, err := newStreamFastPathAead(key)
	if err != nil {
		return nil, err
	}
	endpointCipher := &streamFastPathEndpointCipher{
		aead: aead,
	}
	copy(endpointCipher.additionalData[:len(Id{})], streamId[:])
	return endpointCipher, nil
}

// The returned pooled buffer is owned by the caller.
func (self *streamFastPathEndpointCipher) seal(innerPacket []byte) ([]byte, error) {
	if streamFastPathMaxInnerPacketByteCount < len(innerPacket) {
		return nil, errStreamFastPathPacketTooLarge
	}
	if self.sendCounter == ^uint64(0) {
		return nil, errStreamFastPathCounter
	}
	self.sendCounter += 1

	sealedByteCount := streamFastPathEndpointHeaderByteCount + len(innerPacket) + self.aead.Overhead()
	sealed := MessagePoolGet(sealedByteCount)
	binary.BigEndian.PutUint64(sealed[:streamFastPathEndpointHeaderByteCount], self.sendCounter)

	copy(self.nonce[4:], sealed[:streamFastPathEndpointHeaderByteCount])
	copy(
		self.additionalData[len(Id{}):],
		sealed[:streamFastPathEndpointHeaderByteCount],
	)

	out := self.aead.Seal(
		sealed[:streamFastPathEndpointHeaderByteCount],
		self.nonce[:],
		innerPacket,
		self.additionalData[:],
	)
	if &out[0] != &sealed[0] {
		MessagePoolReturn(sealed)
	}
	return out, nil
}

// The returned inner packet borrows packet's storage and is valid only while
// packet remains owned. Authentication failure never advances replay state.
func (self *streamFastPathEndpointCipher) open(packet []byte) ([]byte, error) {
	minByteCount := streamFastPathEndpointHeaderByteCount + self.aead.Overhead()
	if len(packet) < minByteCount ||
		streamFastPathMaxInnerPacketByteCount+self.aead.Overhead()+streamFastPathEndpointHeaderByteCount < len(packet) {
		return nil, errStreamFastPathPacket
	}
	counter := binary.BigEndian.Uint64(packet[:streamFastPathEndpointHeaderByteCount])
	if !self.replayWindow.canAccept(counter) {
		return nil, errStreamFastPathReplay
	}

	copy(self.nonce[4:], packet[:streamFastPathEndpointHeaderByteCount])
	copy(
		self.additionalData[len(Id{}):],
		packet[:streamFastPathEndpointHeaderByteCount],
	)

	innerPacket, err := self.aead.Open(
		packet[streamFastPathEndpointHeaderByteCount:streamFastPathEndpointHeaderByteCount],
		self.nonce[:],
		packet[streamFastPathEndpointHeaderByteCount:],
		self.additionalData[:],
	)
	if err != nil {
		return nil, errStreamFastPathAuthentication
	}
	if !self.replayWindow.accept(counter) {
		return nil, errStreamFastPathReplay
	}
	return innerPacket, nil
}

// The hop envelope is [URF/version][receiver index][counter][ciphertext][tag].
// The receiver index selects stream-generation state before decryption.
type streamFastPathHopCipher struct {
	receiverIndex       uint32
	aead                cipher.AEAD
	sendCounter         uint64
	replayWindow        streamFastPathReplayWindow
	receivedPacketCount uint64
	receivedByteCount   ByteCount
}

// The index is local to the receiving peer and selects generation state.
func newStreamFastPathHopCipher(
	receiverIndex uint32,
	key []byte,
) (*streamFastPathHopCipher, error) {
	if receiverIndex == 0 {
		return nil, errStreamFastPathReceiverIndex
	}
	aead, err := newStreamFastPathAead(key)
	if err != nil {
		return nil, err
	}
	return &streamFastPathHopCipher{
		receiverIndex: receiverIndex,
		aead:          aead,
	}, nil
}

// This parses only the unauthenticated routing prefix. The selected receiver
// must authenticate the complete header before using the data.
func streamFastPathPacketReceiverIndex(packet []byte) (uint32, error) {
	if len(packet) < streamFastPathHopHeaderByteCount ||
		packet[0] != 'U' ||
		packet[1] != 'R' ||
		packet[2] != 'F' ||
		packet[3] != streamFastPathVersion {
		return 0, errStreamFastPathPacket
	}
	receiverIndex := binary.BigEndian.Uint32(packet[4:8])
	if receiverIndex == 0 {
		return 0, errStreamFastPathPacket
	}
	return receiverIndex, nil
}

// The returned pooled buffer is owned by the caller. Payload is normally the
// still-encrypted endpoint envelope.
func (self *streamFastPathHopCipher) seal(payload []byte) ([]byte, error) {
	minPayloadByteCount := streamFastPathEndpointHeaderByteCount +
		self.aead.Overhead()
	maxPayloadByteCount := streamFastPathMaxInnerPacketByteCount +
		streamFastPathEndpointHeaderByteCount +
		self.aead.Overhead()
	if len(payload) < minPayloadByteCount {
		return nil, errStreamFastPathPacket
	}
	if maxPayloadByteCount < len(payload) {
		return nil, errStreamFastPathPacketTooLarge
	}
	if self.sendCounter == ^uint64(0) {
		return nil, errStreamFastPathCounter
	}
	self.sendCounter += 1

	sealedByteCount := streamFastPathHopHeaderByteCount + len(payload) + self.aead.Overhead()
	sealed := MessagePoolGet(sealedByteCount)
	sealed[0] = 'U'
	sealed[1] = 'R'
	sealed[2] = 'F'
	sealed[3] = streamFastPathVersion
	binary.BigEndian.PutUint32(sealed[4:8], self.receiverIndex)
	binary.BigEndian.PutUint64(sealed[8:streamFastPathHopHeaderByteCount], self.sendCounter)

	out := self.aead.Seal(
		sealed[:streamFastPathHopHeaderByteCount],
		sealed[4:streamFastPathHopHeaderByteCount],
		payload,
		sealed[:streamFastPathHopHeaderByteCount],
	)
	if &out[0] != &sealed[0] {
		MessagePoolReturn(sealed)
	}
	return out, nil
}

// The returned payload borrows packet's storage. A route lookup selects this
// cipher by receiver index before calling Open; the explicit equality check
// prevents dispatch mistakes from being accepted.
func (self *streamFastPathHopCipher) open(packet []byte) ([]byte, error) {
	minByteCount := streamFastPathHopHeaderByteCount +
		streamFastPathEndpointHeaderByteCount +
		2*self.aead.Overhead()
	maxByteCount := streamFastPathMaxWirePacketByteCount
	if len(packet) < minByteCount || maxByteCount < len(packet) {
		return nil, errStreamFastPathPacket
	}
	receiverIndex, err := streamFastPathPacketReceiverIndex(packet)
	if err != nil || receiverIndex != self.receiverIndex {
		return nil, errStreamFastPathPacket
	}
	counter := binary.BigEndian.Uint64(packet[8:streamFastPathHopHeaderByteCount])
	if !self.replayWindow.canAccept(counter) {
		return nil, errStreamFastPathReplay
	}

	payload, err := self.aead.Open(
		packet[streamFastPathHopHeaderByteCount:streamFastPathHopHeaderByteCount],
		packet[4:streamFastPathHopHeaderByteCount],
		packet[streamFastPathHopHeaderByteCount:],
		packet[:streamFastPathHopHeaderByteCount],
	)
	if err != nil {
		return nil, errStreamFastPathAuthentication
	}
	if !self.replayWindow.accept(counter) {
		return nil, errStreamFastPathReplay
	}
	self.receivedPacketCount += 1
	self.receivedByteCount += ByteCount(
		len(payload) -
			streamFastPathEndpointHeaderByteCount -
			self.aead.Overhead(),
	)
	return payload, nil
}

// These cumulative values are checkpointed over the reliable control path.
// They count only authenticated, replay-accepted inner payload bytes.
func (self *streamFastPathHopCipher) receivedCounts() (
	packetCount uint64,
	byteCount ByteCount,
) {
	return self.receivedPacketCount, self.receivedByteCount
}

// The source performs end-to-end encryption once, then authenticates the
// resulting opaque envelope to its first adjacent hop.
type streamFastPathSource struct {
	endpointCipher *streamFastPathEndpointCipher
	hopCipher      *streamFastPathHopCipher
}

// Both ciphers are directional and owned exclusively by the returned source.
func newStreamFastPathSource(
	endpointCipher *streamFastPathEndpointCipher,
	hopCipher *streamFastPathHopCipher,
) *streamFastPathSource {
	return &streamFastPathSource{
		endpointCipher: endpointCipher,
		hopCipher:      hopCipher,
	}
}

// The returned pooled wire packet is owned by the caller.
func (self *streamFastPathSource) seal(innerPacket []byte) ([]byte, error) {
	endpointPacket, err := self.endpointCipher.seal(innerPacket)
	if err != nil {
		return nil, err
	}
	wirePacket, err := self.hopCipher.seal(endpointPacket)
	MessagePoolReturn(endpointPacket)
	return wirePacket, err
}

// Outputs are index-aligned and nil for rejected inputs. The caller owns every
// non-nil output and can submit the slice directly to a packet batch writer.
func (self *streamFastPathSource) sealBatch(
	innerPackets [][]byte,
	wirePackets [][]byte,
) (int, error) {
	if len(wirePackets) < len(innerPackets) {
		return 0, errStreamFastPathBatchTooSmall
	}
	acceptedCount := 0
	for packetIndex, innerPacket := range innerPackets {
		wirePackets[packetIndex] = nil
		wirePacket, err := self.seal(innerPacket)
		if err != nil {
			continue
		}
		wirePackets[packetIndex] = wirePacket
		acceptedCount += 1
	}
	return acceptedCount, nil
}

// Every intermediary has exactly this shape, independent of its position or
// the stream's total hop count. The opened payload remains end-to-end sealed.
type streamFastPathForwarder struct {
	receiveHopCipher *streamFastPathHopCipher
	sendHopCipher    *streamFastPathHopCipher
}

// The two ciphers represent distinct adjacent hops and never share counters.
func newStreamFastPathForwarder(
	receiveHopCipher *streamFastPathHopCipher,
	sendHopCipher *streamFastPathHopCipher,
) *streamFastPathForwarder {
	return &streamFastPathForwarder{
		receiveHopCipher: receiveHopCipher,
		sendHopCipher:    sendHopCipher,
	}
}

// Input remains owned by the caller. The returned pooled packet is owned by
// the caller and contains the same opaque endpoint envelope under a new hop.
func (self *streamFastPathForwarder) forward(wirePacket []byte) ([]byte, error) {
	endpointPacket, err := self.receiveHopCipher.open(wirePacket)
	if err != nil {
		return nil, err
	}
	return self.sendHopCipher.seal(endpointPacket)
}

// Invalid inputs are dropped independently so one bad datagram cannot discard
// the rest of a receive batch.
func (self *streamFastPathForwarder) forwardBatch(
	wirePackets [][]byte,
	nextWirePackets [][]byte,
) (int, error) {
	if len(nextWirePackets) < len(wirePackets) {
		return 0, errStreamFastPathBatchTooSmall
	}
	acceptedCount := 0
	for packetIndex, wirePacket := range wirePackets {
		nextWirePackets[packetIndex] = nil
		nextWirePacket, err := self.forward(wirePacket)
		if err != nil {
			continue
		}
		nextWirePackets[packetIndex] = nextWirePacket
		acceptedCount += 1
	}
	return acceptedCount, nil
}

// The destination authenticates its last adjacent hop, then opens the
// end-to-end envelope. Intermediaries never hold endpointCipher.
type streamFastPathDestination struct {
	hopCipher      *streamFastPathHopCipher
	endpointCipher *streamFastPathEndpointCipher
}

// Only an endpoint receives the end-to-end cipher needed to see inner data.
func newStreamFastPathDestination(
	hopCipher *streamFastPathHopCipher,
	endpointCipher *streamFastPathEndpointCipher,
) *streamFastPathDestination {
	return &streamFastPathDestination{
		hopCipher:      hopCipher,
		endpointCipher: endpointCipher,
	}
}

// The returned inner packet borrows wirePacket's storage.
func (self *streamFastPathDestination) open(wirePacket []byte) ([]byte, error) {
	endpointPacket, err := self.hopCipher.open(wirePacket)
	if err != nil {
		return nil, err
	}
	return self.endpointCipher.open(endpointPacket)
}

// Outputs are index-aligned borrowed views and nil for rejected inputs. The
// caller releases each owning wire input only after consuming its output.
func (self *streamFastPathDestination) openBatch(
	wirePackets [][]byte,
	innerPackets [][]byte,
) (int, error) {
	if len(innerPackets) < len(wirePackets) {
		return 0, errStreamFastPathBatchTooSmall
	}
	acceptedCount := 0
	for packetIndex, wirePacket := range wirePackets {
		innerPackets[packetIndex] = nil
		innerPacket, err := self.open(wirePacket)
		if err != nil {
			continue
		}
		innerPackets[packetIndex] = innerPacket
		acceptedCount += 1
	}
	return acceptedCount, nil
}
