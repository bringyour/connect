// This file verifies the stream fast path's security, ownership, routing, and
// arbitrary-hop composition without depending on a live network.
package connect

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"
)

// This test-only chain uses the production prototype objects in the same shape
// as a live stream. Each hop has independent sender/receiver cipher state.
type streamFastPathTestChain struct {
	source            *streamFastPathSource
	forwarders        []*streamFastPathForwarder
	destination       *streamFastPathDestination
	hopReceiveCiphers []*streamFastPathHopCipher
}

// The existing stream limit permits eight intermediaries, or nine P2P hops.
// The helper itself is slice-based and has no one-hop or fixed-hop branch.
func newStreamFastPathTestChain(
	tb testing.TB,
	hopCount int,
) *streamFastPathTestChain {
	tb.Helper()
	if hopCount <= 0 {
		tb.Fatal("fast path chain requires at least one hop")
	}

	streamId := NewId()
	generationId := NewId()
	clientIds := make([]Id, hopCount+1)
	for clientIndex := range clientIds {
		clientIds[clientIndex] = NewId()
	}

	endpointExporter := bytes.Repeat([]byte{0x31}, streamFastPathKeyByteCount)
	endpointContractId := NewId()
	endpointKey, err := deriveStreamFastPathKey(
		endpointExporter,
		streamFastPathKeyContext{
			StreamId:      streamId,
			GenerationId:  generationId,
			ContractId:    endpointContractId,
			SourceId:      clientIds[0],
			DestinationId: clientIds[len(clientIds)-1],
			Layer:         streamFastPathKeyLayerEndpoint,
			Direction:     streamFastPathKeyDirectionSourceToDestination,
		},
	)
	if err != nil {
		tb.Fatalf("derive endpoint key: %s", err)
	}
	endpointSendCipher, err := newStreamFastPathEndpointCipher(streamId, endpointKey[:])
	if err != nil {
		tb.Fatalf("create endpoint sender: %s", err)
	}
	endpointReceiveCipher, err := newStreamFastPathEndpointCipher(streamId, endpointKey[:])
	if err != nil {
		tb.Fatalf("create endpoint receiver: %s", err)
	}

	hopSendCiphers := make([]*streamFastPathHopCipher, hopCount)
	hopReceiveCiphers := make([]*streamFastPathHopCipher, hopCount)
	for hopIndex := range hopCount {
		var hopExporter [streamFastPathKeyByteCount]byte
		for byteIndex := range hopExporter {
			hopExporter[byteIndex] = byte(1 + hopIndex + byteIndex)
		}
		hopKey, deriveErr := deriveStreamFastPathKey(
			hopExporter[:],
			streamFastPathKeyContext{
				StreamId:      streamId,
				GenerationId:  generationId,
				ContractId:    NewId(),
				SourceId:      clientIds[hopIndex],
				DestinationId: clientIds[hopIndex+1],
				Layer:         streamFastPathKeyLayerHop,
				Direction:     streamFastPathKeyDirectionSourceToDestination,
			},
		)
		if deriveErr != nil {
			tb.Fatalf("derive hop %d key: %s", hopIndex, deriveErr)
		}
		receiverIndex := uint32(hopIndex + 1)
		hopSendCiphers[hopIndex], err = newStreamFastPathHopCipher(receiverIndex, hopKey[:])
		if err != nil {
			tb.Fatalf("create hop %d sender: %s", hopIndex, err)
		}
		hopReceiveCiphers[hopIndex], err = newStreamFastPathHopCipher(receiverIndex, hopKey[:])
		if err != nil {
			tb.Fatalf("create hop %d receiver: %s", hopIndex, err)
		}
	}

	forwarders := make([]*streamFastPathForwarder, 0, hopCount-1)
	for hopIndex := 0; hopIndex+1 < hopCount; hopIndex += 1 {
		forwarders = append(
			forwarders,
			newStreamFastPathForwarder(
				hopReceiveCiphers[hopIndex],
				hopSendCiphers[hopIndex+1],
			),
		)
	}
	return &streamFastPathTestChain{
		source: newStreamFastPathSource(
			endpointSendCipher,
			hopSendCiphers[0],
		),
		forwarders: forwarders,
		destination: newStreamFastPathDestination(
			hopReceiveCiphers[len(hopReceiveCiphers)-1],
			endpointReceiveCipher,
		),
		hopReceiveCiphers: hopReceiveCiphers,
	}
}

// The returned inner packet borrows owner, which the caller must return after
// consuming the inner view.
func (self *streamFastPathTestChain) sendToDestination(
	innerPacket []byte,
) (innerView []byte, owner []byte, err error) {
	wirePacket, err := self.source.seal(innerPacket)
	if err != nil {
		return nil, nil, err
	}
	for _, forwarder := range self.forwarders {
		nextWirePacket, forwardErr := forwarder.forward(wirePacket)
		MessagePoolReturn(wirePacket)
		if forwardErr != nil {
			return nil, nil, forwardErr
		}
		wirePacket = nextWirePacket
	}
	innerView, err = self.destination.open(wirePacket)
	if err != nil {
		MessagePoolReturn(wirePacket)
		return nil, nil, err
	}
	return innerView, wirePacket, nil
}

// Every field that selects a session or route changes the derived key.
func TestStreamFastPathKeyDerivationSeparatesEveryRoutingKey(t *testing.T) {
	exporter := bytes.Repeat([]byte{0x42}, streamFastPathKeyByteCount)
	base := streamFastPathKeyContext{
		StreamId:      NewId(),
		GenerationId:  NewId(),
		ContractId:    NewId(),
		SourceId:      NewId(),
		DestinationId: NewId(),
		Layer:         streamFastPathKeyLayerEndpoint,
		Direction:     streamFastPathKeyDirectionSourceToDestination,
	}
	baseKey, err := deriveStreamFastPathKey(exporter, base)
	if err != nil {
		t.Fatalf("derive base key: %s", err)
	}

	variants := []streamFastPathKeyContext{}
	variant := base
	variant.StreamId = NewId()
	variants = append(variants, variant)
	variant = base
	variant.GenerationId = NewId()
	variants = append(variants, variant)
	variant = base
	variant.ContractId = NewId()
	variants = append(variants, variant)
	variant = base
	variant.SourceId = NewId()
	variants = append(variants, variant)
	variant = base
	variant.DestinationId = NewId()
	variants = append(variants, variant)
	variant = base
	variant.Layer = streamFastPathKeyLayerHop
	variants = append(variants, variant)
	variant = base
	variant.Direction = streamFastPathKeyDirectionDestinationToSource
	variants = append(variants, variant)

	for variantIndex, variantContext := range variants {
		variantKey, deriveErr := deriveStreamFastPathKey(exporter, variantContext)
		if deriveErr != nil {
			t.Fatalf("derive variant %d: %s", variantIndex, deriveErr)
		}
		if variantKey == baseKey {
			t.Errorf("routing-key variant %d derived the base key", variantIndex)
		}
	}

	if _, err := deriveStreamFastPathKey(
		exporter[:streamFastPathKeyByteCount-1],
		base,
	); !errors.Is(err, errStreamFastPathExporter) {
		t.Fatalf("short exporter error = %v", err)
	}
}

// Bounded reordering succeeds while duplicates, zero, and expired counters do
// not consume state.
func TestStreamFastPathReplayWindowAcceptsReorderingAndRejectsOldPackets(t *testing.T) {
	var window streamFastPathReplayWindow
	for _, counter := range []uint64{1, 3, 2, 65, 64, 70, 69} {
		if !window.accept(counter) {
			t.Fatalf("counter %d was rejected", counter)
		}
	}
	for _, counter := range []uint64{1, 2, 64, 69, 70} {
		if window.accept(counter) {
			t.Errorf("duplicate counter %d was accepted", counter)
		}
	}
	if !window.accept(1 + streamFastPathReplayWindowBitCount) {
		t.Fatal("new counter at a full-window advance was rejected")
	}
	if window.accept(1) {
		t.Fatal("counter at the replay-window boundary was accepted")
	}
	if window.accept(0) {
		t.Fatal("reserved zero counter was accepted")
	}
}

// The same source, intermediary, and destination composition covers every
// currently supported stream length.
func TestStreamFastPathRoundTripsEverySupportedStreamLength(t *testing.T) {
	innerPacket := bytes.Repeat([]byte{0x5a}, 1380)
	for hopCount := 1; hopCount <= MaxMultihopLength+1; hopCount += 1 {
		chain := newStreamFastPathTestChain(t, hopCount)
		innerView, owner, err := chain.sendToDestination(innerPacket)
		if err != nil {
			t.Fatalf("%d-hop send: %s", hopCount, err)
		}
		if !bytes.Equal(innerView, innerPacket) {
			MessagePoolReturn(owner)
			t.Fatalf("%d-hop payload mismatch", hopCount)
		}
		MessagePoolReturn(owner)
	}
}

// A forged future counter and an exact replay cannot advance the committed
// window or inflate authenticated accounting.
func TestStreamFastPathAuthenticationFailureDoesNotAdvanceReplayWindow(t *testing.T) {
	chain := newStreamFastPathTestChain(t, 1)
	innerPacket := bytes.Repeat([]byte{0x6b}, 1380)

	wirePacket, err := chain.source.seal(innerPacket)
	if err != nil {
		t.Fatalf("seal first packet: %s", err)
	}
	replayPacket := MessagePoolCopy(wirePacket)
	innerView, err := chain.destination.open(wirePacket)
	if err != nil || !bytes.Equal(innerView, innerPacket) {
		MessagePoolReturn(wirePacket)
		MessagePoolReturn(replayPacket)
		t.Fatalf("open first packet: len=%d err=%v", len(innerView), err)
	}
	MessagePoolReturn(wirePacket)
	if _, err := chain.destination.open(replayPacket); !errors.Is(err, errStreamFastPathReplay) {
		MessagePoolReturn(replayPacket)
		t.Fatalf("replay error = %v", err)
	}
	MessagePoolReturn(replayPacket)

	wirePacket, err = chain.source.seal(innerPacket)
	if err != nil {
		t.Fatalf("seal second packet: %s", err)
	}
	tamperedPacket := MessagePoolCopy(wirePacket)
	binary.BigEndian.PutUint64(
		tamperedPacket[8:streamFastPathHopHeaderByteCount],
		10_000,
	)
	if _, err := chain.destination.open(tamperedPacket); !errors.Is(err, errStreamFastPathAuthentication) {
		MessagePoolReturn(tamperedPacket)
		MessagePoolReturn(wirePacket)
		t.Fatalf("tampered counter error = %v", err)
	}
	MessagePoolReturn(tamperedPacket)

	innerView, err = chain.destination.open(wirePacket)
	if err != nil || !bytes.Equal(innerView, innerPacket) {
		MessagePoolReturn(wirePacket)
		t.Fatalf("valid packet after forged future counter: len=%d err=%v", len(innerView), err)
	}
	MessagePoolReturn(wirePacket)
	receivedPacketCount, receivedByteCount := chain.hopReceiveCiphers[0].receivedCounts()
	if receivedPacketCount != 2 || receivedByteCount != 2*ByteCount(len(innerPacket)) {
		t.Fatalf(
			"accounting after replay/tamper packets=%d bytes=%d",
			receivedPacketCount,
			receivedByteCount,
		)
	}
}

// An intermediary can authenticate the adjacent peer but cannot alter the
// opaque end-to-end envelope or move the endpoint replay window.
func TestStreamFastPathDestinationRejectsEndpointEnvelopeModifiedByHop(t *testing.T) {
	chain := newStreamFastPathTestChain(t, 2)
	innerPacket := bytes.Repeat([]byte{0x53}, 1380)

	firstWirePacket, err := chain.source.seal(innerPacket)
	if err != nil {
		t.Fatalf("seal packet: %s", err)
	}
	endpointPacket, err := chain.forwarders[0].receiveHopCipher.open(firstWirePacket)
	if err != nil {
		MessagePoolReturn(firstWirePacket)
		t.Fatalf("open first hop: %s", err)
	}
	tamperedEndpointPacket := MessagePoolCopy(endpointPacket)
	MessagePoolReturn(firstWirePacket)
	binary.BigEndian.PutUint64(
		tamperedEndpointPacket[:streamFastPathEndpointHeaderByteCount],
		10_000,
	)
	secondWirePacket, err := chain.forwarders[0].sendHopCipher.seal(
		tamperedEndpointPacket,
	)
	MessagePoolReturn(tamperedEndpointPacket)
	if err != nil {
		t.Fatalf("seal tampered endpoint packet: %s", err)
	}
	if _, err := chain.destination.open(secondWirePacket); !errors.Is(
		err,
		errStreamFastPathAuthentication,
	) {
		MessagePoolReturn(secondWirePacket)
		t.Fatalf("tampered endpoint error = %v", err)
	}
	MessagePoolReturn(secondWirePacket)

	innerView, owner, err := chain.sendToDestination(innerPacket)
	if err != nil || !bytes.Equal(innerView, innerPacket) {
		MessagePoolReturn(owner)
		t.Fatalf("valid packet after forged endpoint counter: len=%d err=%v", len(innerView), err)
	}
	MessagePoolReturn(owner)
}

// The unauthenticated prefix selects local state, while header authentication
// and the exact receiver index prevent misrouting from consuming replay state.
func TestStreamFastPathReceiverIndexRoutesWithoutTrustingHeader(t *testing.T) {
	chain := newStreamFastPathTestChain(t, 1)
	innerPacket := bytes.Repeat([]byte{0x61}, 1380)
	wirePacket, err := chain.source.seal(innerPacket)
	if err != nil {
		t.Fatalf("seal packet: %s", err)
	}
	receiverIndex, err := streamFastPathPacketReceiverIndex(wirePacket)
	if err != nil || receiverIndex != 1 {
		MessagePoolReturn(wirePacket)
		t.Fatalf("receiver index=%d err=%v", receiverIndex, err)
	}

	wrongReceiverPacket := MessagePoolCopy(wirePacket)
	binary.BigEndian.PutUint32(wrongReceiverPacket[4:8], receiverIndex+1)
	if _, err := chain.destination.open(wrongReceiverPacket); !errors.Is(
		err,
		errStreamFastPathPacket,
	) {
		MessagePoolReturn(wrongReceiverPacket)
		MessagePoolReturn(wirePacket)
		t.Fatalf("wrong receiver error = %v", err)
	}
	MessagePoolReturn(wrongReceiverPacket)

	invalidVersionPacket := MessagePoolCopy(wirePacket)
	invalidVersionPacket[3] = streamFastPathVersion + 1
	if _, err := streamFastPathPacketReceiverIndex(invalidVersionPacket); !errors.Is(
		err,
		errStreamFastPathPacket,
	) {
		MessagePoolReturn(invalidVersionPacket)
		MessagePoolReturn(wirePacket)
		t.Fatalf("invalid version error = %v", err)
	}
	MessagePoolReturn(invalidVersionPacket)

	if _, err := streamFastPathPacketReceiverIndex(
		wirePacket[:streamFastPathHopHeaderByteCount-1],
	); !errors.Is(err, errStreamFastPathPacket) {
		MessagePoolReturn(wirePacket)
		t.Fatalf("short header error = %v", err)
	}

	innerView, err := chain.destination.open(wirePacket)
	if err != nil || !bytes.Equal(innerView, innerPacket) {
		MessagePoolReturn(wirePacket)
		t.Fatalf("valid packet after bad routes: len=%d err=%v", len(innerView), err)
	}
	MessagePoolReturn(wirePacket)
}

// Batch processing preserves packet independence across every intermediary,
// including exact accounting after an authenticated drop.
func TestStreamFastPathBatchDropsOnlyInvalidDatagramAcrossFourHops(t *testing.T) {
	const packetCount = 32
	chain := newStreamFastPathTestChain(t, 4)
	innerPackets := make([][]byte, packetCount)
	for packetIndex := range packetCount {
		innerPackets[packetIndex] = bytes.Repeat(
			[]byte{byte(packetIndex + 1)},
			1200+packetIndex,
		)
	}

	wirePackets := make([][]byte, packetCount)
	acceptedCount, err := chain.source.sealBatch(innerPackets, wirePackets)
	if err != nil || acceptedCount != packetCount {
		t.Fatalf("source batch accepted=%d err=%v", acceptedCount, err)
	}
	const invalidPacketIndex = 7
	wirePackets[invalidPacketIndex][len(wirePackets[invalidPacketIndex])-1] ^= 0xff

	for forwarderIndex, forwarder := range chain.forwarders {
		nextWirePackets := make([][]byte, packetCount)
		acceptedCount, err = forwarder.forwardBatch(wirePackets, nextWirePackets)
		for _, wirePacket := range wirePackets {
			MessagePoolReturn(wirePacket)
		}
		if err != nil {
			for _, wirePacket := range nextWirePackets {
				MessagePoolReturn(wirePacket)
			}
			t.Fatalf("forwarder %d batch: %s", forwarderIndex, err)
		}
		expectedCount := packetCount - 1
		if acceptedCount != expectedCount {
			for _, wirePacket := range nextWirePackets {
				MessagePoolReturn(wirePacket)
			}
			t.Fatalf("forwarder %d accepted=%d want=%d", forwarderIndex, acceptedCount, expectedCount)
		}
		if nextWirePackets[invalidPacketIndex] != nil {
			for _, wirePacket := range nextWirePackets {
				MessagePoolReturn(wirePacket)
			}
			t.Fatalf("forwarder %d restored invalid packet", forwarderIndex)
		}
		wirePackets = nextWirePackets
	}

	openedPackets := make([][]byte, packetCount)
	acceptedCount, err = chain.destination.openBatch(wirePackets, openedPackets)
	if err != nil {
		for _, wirePacket := range wirePackets {
			MessagePoolReturn(wirePacket)
		}
		t.Fatalf("destination batch: %s", err)
	}
	if acceptedCount != packetCount-1 {
		for _, wirePacket := range wirePackets {
			MessagePoolReturn(wirePacket)
		}
		t.Fatalf("destination accepted=%d want=%d", acceptedCount, packetCount-1)
	}
	for packetIndex, openedPacket := range openedPackets {
		if packetIndex == invalidPacketIndex {
			if openedPacket != nil {
				t.Errorf("invalid packet %d was delivered", packetIndex)
			}
			continue
		}
		if !bytes.Equal(openedPacket, innerPackets[packetIndex]) {
			t.Errorf("packet %d mismatch", packetIndex)
		}
	}
	var expectedByteCount ByteCount
	for packetIndex, innerPacket := range innerPackets {
		if packetIndex != invalidPacketIndex {
			expectedByteCount += ByteCount(len(innerPacket))
		}
	}
	expectedPacketCount := uint64(packetCount - 1)
	for hopIndex, hopCipher := range chain.hopReceiveCiphers {
		receivedPacketCount, receivedByteCount := hopCipher.receivedCounts()
		if receivedPacketCount != expectedPacketCount ||
			receivedByteCount != expectedByteCount {
			t.Errorf(
				"hop %d accounting packets=%d bytes=%d want=%d/%d",
				hopIndex,
				receivedPacketCount,
				receivedByteCount,
				expectedPacketCount,
				expectedByteCount,
			)
		}
	}
	for _, wirePacket := range wirePackets {
		MessagePoolReturn(wirePacket)
	}
}

// Wire, key, counter, and batch boundaries fail before unsafe state is used.
func TestStreamFastPathRejectsInvalidSizesAndExhaustedCounters(t *testing.T) {
	var key [streamFastPathKeyByteCount]byte
	key[0] = 1
	endpointCipher, err := newStreamFastPathEndpointCipher(NewId(), key[:])
	if err != nil {
		t.Fatalf("endpoint cipher: %s", err)
	}
	hopCipher, err := newStreamFastPathHopCipher(1, key[:])
	if err != nil {
		t.Fatalf("hop cipher: %s", err)
	}

	oversized := make([]byte, streamFastPathMaxInnerPacketByteCount+1)
	if _, err := endpointCipher.seal(oversized); !errors.Is(err, errStreamFastPathPacketTooLarge) {
		t.Fatalf("oversized endpoint error = %v", err)
	}
	endpointCipher.sendCounter = ^uint64(0)
	if _, err := endpointCipher.seal(nil); !errors.Is(err, errStreamFastPathCounter) {
		t.Fatalf("endpoint counter error = %v", err)
	}
	if _, err := hopCipher.seal(nil); !errors.Is(err, errStreamFastPathPacket) {
		t.Fatalf("short hop payload error = %v", err)
	}
	hopCipher.sendCounter = ^uint64(0)
	endpointPacket := MessagePoolGet(
		streamFastPathEndpointHeaderByteCount + streamFastPathAeadTagByteCount,
	)
	if _, err := hopCipher.seal(endpointPacket); !errors.Is(err, errStreamFastPathCounter) {
		MessagePoolReturn(endpointPacket)
		t.Fatalf("hop counter error = %v", err)
	}
	MessagePoolReturn(endpointPacket)
	if _, err := newStreamFastPathHopCipher(0, key[:]); !errors.Is(err, errStreamFastPathReceiverIndex) {
		t.Fatalf("zero receiver index error = %v", err)
	}
	if _, err := newStreamFastPathEndpointCipher(NewId(), key[:len(key)-1]); !errors.Is(err, errStreamFastPathKey) {
		t.Fatalf("short key error = %v", err)
	}
	if _, err := endpointCipher.open(nil); !errors.Is(err, errStreamFastPathPacket) {
		t.Fatalf("short endpoint packet error = %v", err)
	}
	if _, err := hopCipher.open(nil); !errors.Is(err, errStreamFastPathPacket) {
		t.Fatalf("short hop packet error = %v", err)
	}

	source := newStreamFastPathSource(endpointCipher, hopCipher)
	if _, err := source.sealBatch([][]byte{{1}}, nil); !errors.Is(err, errStreamFastPathBatchTooSmall) {
		t.Fatalf("short source batch error = %v", err)
	}
	forwarder := newStreamFastPathForwarder(hopCipher, hopCipher)
	if _, err := forwarder.forwardBatch([][]byte{{1}}, nil); !errors.Is(err, errStreamFastPathBatchTooSmall) {
		t.Fatalf("short forward batch error = %v", err)
	}
	destination := newStreamFastPathDestination(hopCipher, endpointCipher)
	if _, err := destination.openBatch([][]byte{{1}}, nil); !errors.Is(err, errStreamFastPathBatchTooSmall) {
		t.Fatalf("short destination batch error = %v", err)
	}
}

// Pooled ownership and AEAD scratch remain allocation-free after warmup.
func TestStreamFastPathSteadyStateDoesNotAllocate(t *testing.T) {
	chain := newStreamFastPathTestChain(t, 1)
	innerPacket := bytes.Repeat([]byte{0x4d}, 1380)
	innerView, owner, err := chain.sendToDestination(innerPacket)
	if err != nil || !bytes.Equal(innerView, innerPacket) {
		MessagePoolReturn(owner)
		t.Fatalf("warm send len=%d err=%v", len(innerView), err)
	}
	MessagePoolReturn(owner)

	allocCount := testing.AllocsPerRun(1_000, func() {
		innerView, owner, sendErr := chain.sendToDestination(innerPacket)
		if sendErr != nil || len(innerView) != len(innerPacket) || innerView[0] != innerPacket[0] {
			panic("stream fast path send failed")
		}
		MessagePoolReturn(owner)
	})
	if allocCount != 0 {
		t.Fatalf("steady-state allocations = %.2f, want 0", allocCount)
	}
}
