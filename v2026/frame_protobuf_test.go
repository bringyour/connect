package connect

// Wire-compatibility tests for the hand-rolled transfer-frame codec in
// frame_protobuf.go. Clients on the network still use the official protobuf
// library, so the hand-rolled encoding MUST match it byte-for-byte. These tests
// build the equivalent protocol structs, marshal them with the official library
// (the same proto.MarshalOptions the package uses via ProtoMarshal), and assert
// the hand-rolled output is identical and decodes back to an equal message.

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	mathrandv2 "math/rand/v2"
	"testing"
	"unsafe"

	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// buildEquivalentTransferFrame builds the protocol.TransferFrame that
// sendWithSetContract (v2 path) would build for the given sendPackFrame.
func buildEquivalentTransferFrame(m *sendPackFrame) *protocol.TransferFrame {
	pack := &protocol.Pack{
		MessageId:         m.messageId.Bytes(),
		SequenceId:        m.sequenceId.Bytes(),
		SequenceNumber:    m.sequenceNumber,
		Head:              m.head,
		Frames:            m.frames,
		ContractFrame:     m.contractFrame,
		Nack:              m.nack,
		Tag:               &protocol.Tag{SendTime: m.tagSendTime},
		ForceStream:       m.forceStream,
		CompanionContract: m.companionContract,
		LogicalLane:       m.logicalLane,
	}
	if m.contractId != nil {
		pack.ContractId = m.contractId.Bytes()
	}
	tf := &protocol.TransferFrame{
		TransferPath: m.path.ToProtobuf(),
	}
	mt := protocol.MessageType_TransferPack
	tf.MessageType = &mt
	tf.Pack = pack
	if m.sessionRoleSet {
		sr := m.sessionRole
		tf.SessionRole = &sr
	}
	if m.companion {
		c := true
		tf.SessionCompanion = &c
	}
	return tf
}

// assertCodecMatches checks that the hand-rolled marshal is byte-identical to
// the official library and round-trips back to an equal message.
func assertCodecMatches(t *testing.T, m *sendPackFrame) {
	t.Helper()

	want, err := proto.Marshal(buildEquivalentTransferFrame(m))
	if err != nil {
		t.Fatalf("proto.Marshal: %s", err)
	}

	got := marshalSendPackTransferFrame(m)
	defer MessagePoolReturn(got)

	if !bytes.Equal(got, want) {
		t.Fatalf("hand-rolled bytes != proto bytes\n got (%d): %x\nwant (%d): %x", len(got), got, len(want), want)
	}

	// the hand-rolled bytes must decode under the official library to an equal
	// message (this is the wire contract for peers still on the proto lib, and
	// for our own setHead re-marshal path).
	var decoded protocol.TransferFrame
	if err := proto.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("proto.Unmarshal(hand-rolled): %s", err)
	}
	if !proto.Equal(&decoded, buildEquivalentTransferFrame(m)) {
		t.Fatalf("decoded hand-rolled frame != equivalent frame\n decoded: %v", &decoded)
	}
}

func TestFrameCodecEdgeCases(t *testing.T) {
	idA := NewId()
	idB := NewId()
	idC := NewId()
	rawFrame := &protocol.Frame{
		MessageType:  protocol.MessageType_IpIpPacketToProvider,
		MessageBytes: []byte{0x45, 0x00, 0x00, 0x28, 0xde, 0xad},
		Raw:          true,
	}
	contractFrame := &protocol.Frame{
		MessageType:  protocol.MessageType_TransferContract,
		MessageBytes: []byte{0x01, 0x02, 0x03},
	}
	emptyFrame := &protocol.Frame{}
	cid := NewId()

	cases := map[string]*sendPackFrame{
		"minimal": {
			path:        TransferPath{DestinationId: idA, SourceId: idB},
			messageId:   idC,
			sequenceId:  idA,
			tagSendTime: 0,
		},
		"typical ack pack": {
			path:           TransferPath{DestinationId: idA, SourceId: idB},
			messageId:      idC,
			sequenceId:     idA,
			sequenceNumber: 42,
			head:           true,
			frames:         []*protocol.Frame{rawFrame},
			tagSendTime:    1_700_000_000_000,
		},
		"nack with contract id": {
			path:        TransferPath{DestinationId: idA, SourceId: idB},
			messageId:   idC,
			sequenceId:  idA,
			nack:        true,
			frames:      []*protocol.Frame{rawFrame},
			tagSendTime: 1_700_000_000_001,
			contractId:  &cid,
		},
		"head with contract frame": {
			path:           TransferPath{DestinationId: idA, SourceId: idB},
			messageId:      idC,
			sequenceId:     idA,
			sequenceNumber: 1,
			head:           true,
			frames:         []*protocol.Frame{rawFrame},
			contractFrame:  contractFrame,
			tagSendTime:    1_700_000_000_002,
		},
		"server role companion": {
			path:           TransferPath{DestinationId: idA, SourceId: idB, StreamId: idC},
			messageId:      idC,
			sequenceId:     idA,
			sequenceNumber: 7,
			head:           true,
			frames:         []*protocol.Frame{rawFrame},
			tagSendTime:    1_700_000_000_003,
			sessionRole:    protocol.SequenceRole_SequenceRoleServer,
			sessionRoleSet: true,
			companion:      true,
		},
		"stream id only path": {
			path:        TransferPath{StreamId: idC},
			messageId:   idC,
			sequenceId:  idA,
			tagSendTime: 5,
		},
		"empty frame in pack": {
			path:        TransferPath{DestinationId: idA, SourceId: idB},
			messageId:   idC,
			sequenceId:  idA,
			frames:      []*protocol.Frame{emptyFrame},
			tagSendTime: 9,
		},
		"multiple frames": {
			path:           TransferPath{DestinationId: idA, SourceId: idB},
			messageId:      idC,
			sequenceId:     idA,
			sequenceNumber: 100,
			head:           false,
			frames:         []*protocol.Frame{rawFrame, contractFrame, emptyFrame},
			tagSendTime:    1_700_000_000_004,
		},
		"client role set": {
			path:           TransferPath{DestinationId: idA, SourceId: idB},
			messageId:      idC,
			sequenceId:     idA,
			tagSendTime:    11,
			sessionRole:    protocol.SequenceRole_SequenceRoleClient,
			sessionRoleSet: true,
		},
	}

	for _, m := range cases {
		assertCodecMatches(t, m)
	}
}

func TestFrameCodecRandomized(t *testing.T) {
	randId := func() Id {
		var id Id
		for i := range id {
			id[i] = byte(mathrandv2.UintN(256))
		}
		return id
	}
	randFrame := func() *protocol.Frame {
		f := &protocol.Frame{}
		// message types span the implicit-zero (TransferPack) and non-zero range
		f.MessageType = protocol.MessageType(mathrandv2.IntN(26))
		n := mathrandv2.IntN(64)
		if 0 < n {
			f.MessageBytes = make([]byte, n)
			for i := range f.MessageBytes {
				f.MessageBytes[i] = byte(mathrandv2.UintN(256))
			}
		}
		f.Raw = mathrandv2.IntN(2) == 0
		return f
	}

	for iter := 0; iter < 5000; iter++ {
		m := &sendPackFrame{
			messageId:  randId(),
			sequenceId: randId(),
		}
		// path ids present at random
		if mathrandv2.IntN(2) == 0 {
			m.path.DestinationId = randId()
		}
		if mathrandv2.IntN(2) == 0 {
			m.path.SourceId = randId()
		}
		if mathrandv2.IntN(3) == 0 {
			m.path.StreamId = randId()
		}
		if mathrandv2.IntN(2) == 0 {
			m.sequenceNumber = mathrandv2.Uint64()
		}
		m.head = mathrandv2.IntN(2) == 0
		m.nack = mathrandv2.IntN(2) == 0
		m.forceStream = mathrandv2.IntN(2) == 0
		m.companionContract = mathrandv2.IntN(2) == 0
		if mathrandv2.IntN(3) == 0 {
			m.logicalLane = uint32(1 + mathrandv2.IntN(maxLogicalDataLaneCount))
		}
		if mathrandv2.IntN(4) != 0 {
			frameCount := mathrandv2.IntN(3)
			for i := 0; i < frameCount; i++ {
				m.frames = append(m.frames, randFrame())
			}
		}
		if mathrandv2.IntN(3) == 0 {
			m.contractFrame = randFrame()
		}
		if mathrandv2.IntN(2) == 0 {
			m.tagSendTime = mathrandv2.Uint64()
		}
		if mathrandv2.IntN(3) == 0 {
			cid := randId()
			m.contractId = &cid
		}
		switch mathrandv2.IntN(3) {
		case 0:
			m.sessionRoleSet = true
			m.sessionRole = protocol.SequenceRole_SequenceRoleServer
		case 1:
			m.sessionRoleSet = true
			m.sessionRole = protocol.SequenceRole_SequenceRoleClient
		}
		m.companion = mathrandv2.IntN(2) == 0

		assertCodecMatches(t, m)
	}
}

func buildEquivalentAckFrame(m *sendAckFrame) *protocol.TransferFrame {
	var tag *protocol.Tag
	if m.tagSet {
		tag = &protocol.Tag{SendTime: m.tagSendTime}
	}
	var missingContractId []byte
	if m.missingContractId != nil {
		missingContractId = m.missingContractId.Bytes()
	}
	ack := &protocol.Ack{
		MessageId:               m.messageId.Bytes(),
		SequenceId:              m.sequenceId.Bytes(),
		Selective:               m.selective,
		Tag:                     tag,
		MissingContractId:       missingContractId,
		CompactContractRecovery: m.compactContractRecovery,
		LogicalLaneVersion:      m.logicalLaneVersion,
	}
	tf := &protocol.TransferFrame{
		TransferPath: m.path.ToProtobuf(),
	}
	mt := protocol.MessageType_TransferAck
	tf.MessageType = &mt
	tf.Ack = ack
	return tf
}

func assertAckCodecMatches(t *testing.T, m *sendAckFrame) {
	t.Helper()
	want, err := proto.Marshal(buildEquivalentAckFrame(m))
	if err != nil {
		t.Fatalf("proto.Marshal: %s", err)
	}
	got := marshalSendAckTransferFrame(m)
	defer MessagePoolReturn(got)
	if !bytes.Equal(got, want) {
		t.Fatalf("hand-rolled ack bytes != proto bytes\n got (%d): %x\nwant (%d): %x", len(got), got, len(want), want)
	}
	var decoded protocol.TransferFrame
	if err := proto.Unmarshal(got, &decoded); err != nil {
		t.Fatalf("proto.Unmarshal(hand-rolled ack): %s", err)
	}
	if !proto.Equal(&decoded, buildEquivalentAckFrame(m)) {
		t.Fatalf("decoded hand-rolled ack != equivalent\n decoded: %v", &decoded)
	}
}

func TestAckCodecEdgeCases(t *testing.T) {
	idA, idB, idC, idD := NewId(), NewId(), NewId(), NewId()
	cases := map[string]*sendAckFrame{
		"minimal no tag": {
			path:       TransferPath{DestinationId: idA, SourceId: idB},
			messageId:  idC,
			sequenceId: idA,
		},
		"selective with tag": {
			path:        TransferPath{DestinationId: idA, SourceId: idB},
			messageId:   idC,
			sequenceId:  idA,
			selective:   true,
			tagSendTime: 1_700_000_000_000,
			tagSet:      true,
		},
		"tag zero send time": {
			path:       TransferPath{DestinationId: idA, SourceId: idB, StreamId: idC},
			messageId:  idC,
			sequenceId: idA,
			tagSet:     true,
		},
		"stream only path": {
			path:       TransferPath{StreamId: idC},
			messageId:  idC,
			sequenceId: idA,
			selective:  true,
		},
		"missing contract recovery": {
			path:                    TransferPath{DestinationId: idA, SourceId: idB},
			messageId:               idC,
			sequenceId:              idA,
			missingContractId:       &idD,
			compactContractRecovery: true,
		},
		"logical lane capability": {
			path:               TransferPath{DestinationId: idA, SourceId: idB},
			messageId:          idC,
			sequenceId:         idA,
			logicalLaneVersion: transferLogicalLaneVersion,
		},
	}
	for _, m := range cases {
		assertAckCodecMatches(t, m)
		encoded := marshalSendAckTransferFrame(m)
		var frame protocol.TransferFrame
		if !unmarshalTransferFrame(encoded, &frame, true) {
			MessagePoolReturn(encoded)
			t.Fatal("hand decoder rejected Ack frame")
		}
		MessagePoolReturn(encoded)
		if frame.Ack.GetLogicalLaneVersion() != m.logicalLaneVersion {
			t.Fatalf(
				"hand decoder logical lane version = %d, want %d",
				frame.Ack.GetLogicalLaneVersion(),
				m.logicalLaneVersion,
			)
		}
	}
}

func TestAckCodecRandomized(t *testing.T) {
	randId := func() Id {
		var id Id
		for i := range id {
			id[i] = byte(mathrandv2.UintN(256))
		}
		return id
	}
	for iter := 0; iter < 3000; iter++ {
		m := &sendAckFrame{messageId: randId(), sequenceId: randId()}
		if mathrandv2.IntN(2) == 0 {
			m.path.DestinationId = randId()
		}
		if mathrandv2.IntN(2) == 0 {
			m.path.SourceId = randId()
		}
		if mathrandv2.IntN(3) == 0 {
			m.path.StreamId = randId()
		}
		m.selective = mathrandv2.IntN(2) == 0
		if mathrandv2.IntN(4) == 0 {
			missingContractId := randId()
			m.missingContractId = &missingContractId
		}
		m.compactContractRecovery = mathrandv2.IntN(2) == 0
		if mathrandv2.IntN(3) == 0 {
			m.logicalLaneVersion = transferLogicalLaneVersion
		}
		switch mathrandv2.IntN(3) {
		case 0:
			m.tagSendTime = mathrandv2.Uint64()
			m.tagSet = true
		case 1:
			m.tagSet = true
		}
		assertAckCodecMatches(t, m)
	}
}

func newFrameCodecTestSequenceCipher(t testing.TB) *sequenceCipher {
	t.Helper()
	block, err := aes.NewCipher(make([]byte, sequenceTlsKeyLength))
	if err != nil {
		t.Fatalf("aes.NewCipher: %s", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatalf("cipher.NewGCM: %s", err)
	}
	return &sequenceCipher{aead: aead}
}

func TestEncryptedOuterFrameCodec(t *testing.T) {
	idA, idB, idC := NewId(), NewId(), NewId()
	cases := []struct {
		path       TransferPath
		ciphertext []byte
		role       protocol.SequenceRole
		companion  bool
	}{
		{
			path:       TransferPath{},
			ciphertext: []byte{1},
			role:       protocol.SequenceRole_SequenceRoleUnknown,
		},
		{
			path:       TransferPath{DestinationId: idA, SourceId: idB},
			ciphertext: bytes.Repeat([]byte{0xa5}, 1500),
			role:       protocol.SequenceRole_SequenceRoleClient,
		},
		{
			path:       TransferPath{DestinationId: idA, SourceId: idB, StreamId: idC},
			ciphertext: bytes.Repeat([]byte{0x5a}, 3072),
			role:       protocol.SequenceRole_SequenceRoleServer,
			companion:  true,
		},
	}
	for _, tc := range cases {
		ref := &protocol.TransferFrame{
			TransferPath:           tc.path.ToProtobuf(),
			EncryptedTransferFrame: tc.ciphertext,
			SessionCompanion:       &tc.companion,
		}
		if tc.role != protocol.SequenceRole_SequenceRoleUnknown {
			role := tc.role
			ref.SessionRole = &role
		}
		want, err := proto.Marshal(ref)
		if err != nil {
			t.Fatalf("proto.Marshal: %s", err)
		}
		got, err := buildEncryptedOuterFrameBytes(tc.path, tc.ciphertext, tc.role, tc.companion)
		if err != nil {
			t.Fatalf("buildEncryptedOuterFrameBytes: %s", err)
		}
		if !bytes.Equal(got, want) {
			MessagePoolReturn(got)
			t.Fatalf("hand-rolled encrypted outer bytes != proto bytes\n got (%d): %x\nwant (%d): %x", len(got), got, len(want), want)
		}
		MessagePoolReturn(got)
	}
}

func TestSequenceCipherSealOuterFrame(t *testing.T) {
	c := newFrameCodecTestSequenceCipher(t)
	path := TransferPath{DestinationId: NewId(), SourceId: NewId(), StreamId: NewId()}
	plaintext := bytes.Repeat([]byte{0x3c}, 3000)
	wrapped, err := c.SealOuterFrame(
		path,
		plaintext,
		protocol.SequenceRole_SequenceRoleServer,
		true,
	)
	if err != nil {
		t.Fatalf("SealOuterFrame: %s", err)
	}
	defer MessagePoolReturn(wrapped)

	var outer protocol.TransferFrame
	if err := proto.Unmarshal(wrapped, &outer); err != nil {
		t.Fatalf("proto.Unmarshal: %s", err)
	}
	if !proto.Equal(outer.TransferPath, path.ToProtobuf()) {
		t.Fatalf("outer path = %v, want %v", outer.TransferPath, path.ToProtobuf())
	}
	if outer.GetSessionRole() != protocol.SequenceRole_SequenceRoleServer {
		t.Fatalf("outer role = %v, want server", outer.GetSessionRole())
	}
	if !outer.GetSessionCompanion() {
		t.Fatal("outer companion = false, want true")
	}
	opened, err := c.Open(outer.EncryptedTransferFrame)
	if err != nil {
		t.Fatalf("Open: %s", err)
	}
	defer MessagePoolReturn(opened)
	if !bytes.Equal(opened, plaintext) {
		t.Fatal("decrypted plaintext does not match input")
	}
}

// A two-MTU batch is the largest sequence coalescing unit. Assert the fully
// encoded and encrypted message — not just its packet payload — remains below
// every production transport's minimum configured message envelope.
func TestTwoPacketEncryptedPackFitsMinimumMessageLimit(t *testing.T) {
	c := newFrameCodecTestSequenceCipher(t)
	path := TransferPath{DestinationId: NewId(), SourceId: NewId(), StreamId: NewId()}
	m := &sendPackFrame{
		path:           path,
		messageId:      NewId(),
		sequenceId:     NewId(),
		sequenceNumber: ^uint64(0),
		head:           true,
		frames: []*protocol.Frame{
			{MessageType: protocol.MessageType_IpIpPacketFromProvider, MessageBytes: make([]byte, DefaultMtu), Raw: true},
			{MessageType: protocol.MessageType_IpIpPacketFromProvider, MessageBytes: make([]byte, DefaultMtu), Raw: true},
		},
		tagSendTime:    ^uint64(0),
		sessionRole:    protocol.SequenceRole_SequenceRoleServer,
		sessionRoleSet: true,
		companion:      true,
	}
	inner := marshalSendPackTransferFrame(m)
	defer MessagePoolReturn(inner)
	wrapped, err := c.SealOuterFrame(
		path,
		inner,
		protocol.SequenceRole_SequenceRoleServer,
		true,
	)
	if err != nil {
		t.Fatalf("SealOuterFrame: %s", err)
	}
	defer MessagePoolReturn(wrapped)
	limit := int(DefaultClientSettings().MinimumMessageLenLimit())
	if limit < len(wrapped) {
		t.Fatalf("two-packet encrypted pack is %d bytes, exceeds minimum transport limit %d", len(wrapped), limit)
	}
}

// H1 may use more small frames than the compatibility coalescer. Assert the
// maximum frame-count/byte combination, a bounded ordinary opening contract,
// and outer sequence encryption still fit the ordinary 4-KiB data class inside
// the minimum H1 envelope. Opening/rotating contracts retain 3 KiB; an
// established contract can safely use three full 1,100-byte packets only while
// no contract frame rides on that Pack.
func TestH1MaximumLogicalGroupEncryptedPackFitsMinimumMessageLimit(t *testing.T) {
	const ordinaryH1DataEnvelopeByteCount = 4 * 1024

	c := newFrameCodecTestSequenceCipher(t)
	path := TransferPath{DestinationId: NewId(), SourceId: NewId(), StreamId: NewId()}
	frames := make([]*protocol.Frame, sendPackH1GroupMaxFrames)
	baseFrameByteCount := int(sendPackH1GroupMaxMessageByteCount) / len(frames)
	remainingByteCount := int(sendPackH1GroupMaxMessageByteCount)
	for frameIndex := range frames {
		frameByteCount := baseFrameByteCount
		if frameIndex == len(frames)-1 {
			frameByteCount = remainingByteCount
		}
		remainingByteCount -= frameByteCount
		frames[frameIndex] = &protocol.Frame{
			MessageType:  protocol.MessageType_IpIpPacketFromProvider,
			MessageBytes: make([]byte, frameByteCount),
			Raw:          true,
		}
	}
	m := &sendPackFrame{
		path:           path,
		messageId:      NewId(),
		sequenceId:     NewId(),
		sequenceNumber: ^uint64(0),
		head:           true,
		frames:         frames,
		contractFrame: &protocol.Frame{
			MessageType:  protocol.MessageType_TransferContract,
			MessageBytes: make([]byte, 512),
		},
		tagSendTime:    ^uint64(0),
		sessionRole:    protocol.SequenceRole_SequenceRoleServer,
		sessionRoleSet: true,
		companion:      true,
	}
	inner := marshalSendPackTransferFrame(m)
	defer MessagePoolReturn(inner)
	wrapped, err := c.SealOuterFrame(
		path,
		inner,
		protocol.SequenceRole_SequenceRoleServer,
		true,
	)
	if err != nil {
		t.Fatalf("SealOuterFrame: %s", err)
	}
	defer MessagePoolReturn(wrapped)
	limit := int(DefaultClientSettings().MinimumMessageLenLimit())
	if limit < len(wrapped) {
		t.Fatalf(
			"maximum H1 logical-group encrypted Pack is %d bytes, exceeds minimum transport limit %d",
			len(wrapped),
			limit,
		)
	}

	establishedFrames := make([]*protocol.Frame, 3)
	for frameIndex := range establishedFrames {
		establishedFrames[frameIndex] = &protocol.Frame{
			MessageType:  protocol.MessageType_IpIpPacketFromProvider,
			MessageBytes: make([]byte, DefaultMtu),
			Raw:          true,
		}
	}
	contractId := NewId()
	m.frames = establishedFrames
	m.contractId = &contractId
	m.contractFrame = nil
	establishedInner := marshalSendPackTransferFrame(m)
	defer MessagePoolReturn(establishedInner)
	establishedWrapped, err := c.SealOuterFrame(
		path,
		establishedInner,
		protocol.SequenceRole_SequenceRoleServer,
		true,
	)
	if err != nil {
		t.Fatalf("Seal established H1 frame: %s", err)
	}
	defer MessagePoolReturn(establishedWrapped)
	if limit < len(establishedWrapped) {
		t.Fatalf(
			"established three-MTU H1 Pack is %d bytes, exceeds minimum transport limit %d",
			len(establishedWrapped),
			limit,
		)
	}

	m.contractFrame = &protocol.Frame{
		MessageType:  protocol.MessageType_TransferContract,
		MessageBytes: make([]byte, 512),
	}
	rotatingInner := marshalSendPackTransferFrame(m)
	defer MessagePoolReturn(rotatingInner)
	rotatingWrapped, err := c.SealOuterFrame(
		path,
		rotatingInner,
		protocol.SequenceRole_SequenceRoleServer,
		true,
	)
	if err != nil {
		t.Fatalf("Seal rotating H1 frame: %s", err)
	}
	defer MessagePoolReturn(rotatingWrapped)
	if remaining := ordinaryH1DataEnvelopeByteCount - len(rotatingWrapped); remaining < 0 || 32 < remaining {
		t.Fatalf(
			"contract-bearing three-MTU H1 Pack is %d bytes at ordinary data limit %d, want no more than 32 bytes of fragile headroom",
			len(rotatingWrapped),
			ordinaryH1DataEnvelopeByteCount,
		)
	}

	// A slightly larger still-ordinary signed contract crosses the deployed
	// envelope. The established path must therefore prove that no contract
	// frame can ride on its larger chunk instead of relying on the 512-byte
	// example's accidental ten-byte margin.
	m.contractFrame.MessageBytes = make([]byte, 544)
	largeContractInner := marshalSendPackTransferFrame(m)
	defer MessagePoolReturn(largeContractInner)
	largeContractWrapped, err := c.SealOuterFrame(
		path,
		largeContractInner,
		protocol.SequenceRole_SequenceRoleServer,
		true,
	)
	if err != nil {
		t.Fatalf("Seal large-contract H1 frame: %s", err)
	}
	defer MessagePoolReturn(largeContractWrapped)
	if len(largeContractWrapped) <= ordinaryH1DataEnvelopeByteCount {
		t.Fatalf(
			"large-contract three-MTU H1 Pack is %d bytes, want above ordinary data limit %d",
			len(largeContractWrapped),
			ordinaryH1DataEnvelopeByteCount,
		)
	}
}

func BenchmarkSequenceCipherOuterWrap(b *testing.B) {
	c := newFrameCodecTestSequenceCipher(b)
	path := TransferPath{DestinationId: NewId(), SourceId: NewId(), StreamId: NewId()}
	plaintext := bytes.Repeat([]byte{0x3c}, 3000)

	b.Run("separate ciphertext", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(plaintext)))
		for b.Loop() {
			ciphertext, err := c.Seal(plaintext)
			if err != nil {
				b.Fatal(err)
			}
			wrapped, err := buildEncryptedOuterFrameBytes(
				path,
				ciphertext,
				protocol.SequenceRole_SequenceRoleServer,
				true,
			)
			if err != nil {
				b.Fatal(err)
			}
			MessagePoolReturn(wrapped)
		}
	})
	b.Run("fused pooled output", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(plaintext)))
		for b.Loop() {
			wrapped, err := c.SealOuterFrame(
				path,
				plaintext,
				protocol.SequenceRole_SequenceRoleServer,
				true,
			)
			if err != nil {
				b.Fatal(err)
			}
			MessagePoolReturn(wrapped)
		}
	})
}

func BenchmarkSequenceCipherOpen(b *testing.B) {
	c := newFrameCodecTestSequenceCipher(b)
	plaintext := bytes.Repeat([]byte{0x3c}, 3000)
	ciphertext, err := c.Seal(plaintext)
	if err != nil {
		b.Fatal(err)
	}
	nonce := ciphertext[:sequenceTlsAeadNonceSize]
	sealed := ciphertext[sequenceTlsAeadNonceSize:]

	b.Run("heap output", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(plaintext)))
		for b.Loop() {
			opened, err := c.aead.Open(nil, nonce, sealed, nil)
			if err != nil || len(opened) != len(plaintext) {
				b.Fatalf("Open len=%d err=%v", len(opened), err)
			}
		}
	})
	b.Run("pooled output", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(plaintext)))
		for b.Loop() {
			opened, err := c.Open(ciphertext)
			if err != nil || len(opened) != len(plaintext) {
				b.Fatalf("Open len=%d err=%v", len(opened), err)
			}
			MessagePoolReturn(opened)
		}
	})
}

// assertDecodeMatches marshals ref with the official library, decodes it with
// the hand-rolled decoder, and checks equality after accounting for the
// intentional skips (message_type always; transfer_path when !decodePath).
func assertDecodeMatches(t *testing.T, ref *protocol.TransferFrame, decodePath bool) {
	t.Helper()
	b, err := proto.Marshal(ref)
	if err != nil {
		t.Fatalf("proto.Marshal: %s", err)
	}
	var got protocol.TransferFrame
	defer returnDecodedTransferFrameMessageBytes(&got)
	if !unmarshalTransferFrame(b, &got, decodePath) {
		t.Fatalf("unmarshalTransferFrame returned false for valid frame: %x", b)
	}
	want := proto.Clone(ref).(*protocol.TransferFrame)
	want.MessageType = nil
	if !decodePath {
		want.TransferPath = nil
	}
	if !proto.Equal(&got, want) {
		t.Fatalf("decoded != expected (decodePath=%v)\n got: %v\nwant: %v", decodePath, &got, want)
	}
}

func TestDecodeTransferFrameEdgeCases(t *testing.T) {
	idA, idB, idC := NewId(), NewId(), NewId()
	mtPack := protocol.MessageType_TransferPack
	mtAck := protocol.MessageType_TransferAck
	roleServer := protocol.SequenceRole_SequenceRoleServer
	yes := true

	frames := []*protocol.TransferFrame{
		{
			TransferPath: TransferPath{DestinationId: idA, SourceId: idB}.ToProtobuf(),
			MessageType:  &mtPack,
			Pack: &protocol.Pack{
				MessageId:      idC.Bytes(),
				SequenceId:     idA.Bytes(),
				SequenceNumber: 9,
				Head:           true,
				Frames: []*protocol.Frame{
					{MessageType: protocol.MessageType_IpIpPacketToProvider, MessageBytes: []byte{1, 2, 3, 4}, Raw: true},
				},
				Tag: &protocol.Tag{SendTime: 1_700_000_000_000},
			},
		},
		{
			TransferPath:     TransferPath{DestinationId: idA, SourceId: idB, StreamId: idC}.ToProtobuf(),
			MessageType:      &mtPack,
			SessionRole:      &roleServer,
			SessionCompanion: &yes,
			Pack: &protocol.Pack{
				MessageId:     idC.Bytes(),
				SequenceId:    idA.Bytes(),
				Nack:          true,
				ContractId:    idB.Bytes(),
				ContractFrame: &protocol.Frame{MessageType: protocol.MessageType_TransferContract, MessageBytes: []byte{9, 9}},
				Tag:           &protocol.Tag{SendTime: 5},
			},
		},
		{
			TransferPath: TransferPath{DestinationId: idA, SourceId: idB}.ToProtobuf(),
			MessageType:  &mtAck,
			Ack: &protocol.Ack{
				MessageId:               idC.Bytes(),
				SequenceId:              idA.Bytes(),
				Selective:               true,
				Tag:                     &protocol.Tag{SendTime: 7},
				MissingContractId:       idB.Bytes(),
				CompactContractRecovery: true,
			},
		},
		{
			// encrypted carrier: only path + encrypted bytes + session hints
			TransferPath:           TransferPath{DestinationId: idA, SourceId: idB}.ToProtobuf(),
			EncryptedTransferFrame: []byte{0xaa, 0xbb, 0xcc, 0xdd},
			SessionRole:            &roleServer,
		},
		{
			// v1 deprecated Frame carrier
			TransferPath: TransferPath{DestinationId: idA, SourceId: idB}.ToProtobuf(),
			Frame:        &protocol.Frame{MessageType: protocol.MessageType_TransferPack, MessageBytes: []byte{1, 2, 3}},
		},
	}
	for _, ref := range frames {
		assertDecodeMatches(t, ref, true)
		assertDecodeMatches(t, ref, false)
	}
}

func TestDecodeTransferFrameRandomized(t *testing.T) {
	randId := func() Id {
		var id Id
		for i := range id {
			id[i] = byte(mathrandv2.UintN(256))
		}
		return id
	}
	randBytes := func(maxLen int) []byte {
		n := mathrandv2.IntN(maxLen)
		if n == 0 {
			return nil
		}
		out := make([]byte, n)
		for i := range out {
			out[i] = byte(mathrandv2.UintN(256))
		}
		return out
	}
	randFrame := func() *protocol.Frame {
		return &protocol.Frame{
			MessageType:  protocol.MessageType(mathrandv2.IntN(26)),
			MessageBytes: randBytes(48),
			Raw:          mathrandv2.IntN(2) == 0,
		}
	}
	randTag := func() *protocol.Tag {
		if mathrandv2.IntN(3) == 0 {
			return nil
		}
		return &protocol.Tag{SendTime: mathrandv2.Uint64()}
	}

	for iter := 0; iter < 5000; iter++ {
		ref := &protocol.TransferFrame{}
		// transfer_path present in most frames
		if mathrandv2.IntN(5) != 0 {
			tp := TransferPath{}
			if mathrandv2.IntN(2) == 0 {
				tp.DestinationId = randId()
			}
			if mathrandv2.IntN(2) == 0 {
				tp.SourceId = randId()
			}
			if mathrandv2.IntN(3) == 0 {
				tp.StreamId = randId()
			}
			ref.TransferPath = tp.ToProtobuf()
		}
		switch mathrandv2.IntN(4) {
		case 0: // pack
			mt := protocol.MessageType_TransferPack
			ref.MessageType = &mt
			pack := &protocol.Pack{
				MessageId:      randId().Bytes(),
				SequenceId:     randId().Bytes(),
				SequenceNumber: mathrandv2.Uint64(),
				Head:           mathrandv2.IntN(2) == 0,
				Nack:           mathrandv2.IntN(2) == 0,
				Tag:            randTag(),
			}
			for n := mathrandv2.IntN(3); 0 < n; n-- {
				pack.Frames = append(pack.Frames, randFrame())
			}
			if mathrandv2.IntN(3) == 0 {
				pack.ContractFrame = randFrame()
			}
			if mathrandv2.IntN(3) == 0 {
				pack.ContractId = randId().Bytes()
			}
			ref.Pack = pack
		case 1: // ack
			mt := protocol.MessageType_TransferAck
			ref.MessageType = &mt
			ref.Ack = &protocol.Ack{
				MessageId:               randId().Bytes(),
				SequenceId:              randId().Bytes(),
				Selective:               mathrandv2.IntN(2) == 0,
				Tag:                     randTag(),
				CompactContractRecovery: mathrandv2.IntN(2) == 0,
			}
			if mathrandv2.IntN(3) == 0 {
				ref.Ack.MissingContractId = randId().Bytes()
			}
		case 2: // encrypted
			ref.EncryptedTransferFrame = randBytes(64)
			if ref.EncryptedTransferFrame == nil {
				ref.EncryptedTransferFrame = []byte{1}
			}
		case 3: // v1 frame
			ref.Frame = randFrame()
		}
		if mathrandv2.IntN(3) == 0 {
			sr := protocol.SequenceRole(mathrandv2.IntN(3))
			ref.SessionRole = &sr
		}
		if mathrandv2.IntN(3) == 0 {
			c := mathrandv2.IntN(2) == 0
			ref.SessionCompanion = &c
		}

		assertDecodeMatches(t, ref, true)
		assertDecodeMatches(t, ref, false)
	}
}

// TestDecodeRoundTripFromMarshal feeds the hand-rolled marshal output through
// the hand-rolled decoder (the real hot-path round trip).
func TestDecodeRoundTripFromMarshal(t *testing.T) {
	idA, idB, idC := NewId(), NewId(), NewId()
	m := &sendPackFrame{
		path:           TransferPath{DestinationId: idA, SourceId: idB},
		messageId:      idC,
		sequenceId:     idA,
		sequenceNumber: 33,
		head:           true,
		frames:         []*protocol.Frame{{MessageType: protocol.MessageType_IpIpPacketToProvider, MessageBytes: []byte{0x45, 0, 0, 20}, Raw: true}},
		tagSendTime:    1_700_000_000_000,
	}
	b := marshalSendPackTransferFrame(m)
	defer MessagePoolReturn(b)
	var got protocol.TransferFrame
	defer returnDecodedTransferFrameMessageBytes(&got)
	if !unmarshalTransferFrame(b, &got, true) {
		t.Fatal("decode of hand-rolled marshal failed")
	}
	want := proto.Clone(buildEquivalentTransferFrame(m)).(*protocol.TransferFrame)
	want.MessageType = nil
	if !proto.Equal(&got, want) {
		t.Fatalf("round-trip mismatch\n got: %v\nwant: %v", &got, want)
	}
}

func TestDecodedFrameMessageBytesUsePooledCallbackLifetime(t *testing.T) {
	payload := bytes.Repeat([]byte{0x7d}, DefaultMtu)
	m := &sendPackFrame{
		path:        TransferPath{DestinationId: NewId(), SourceId: NewId()},
		messageId:   NewId(),
		sequenceId:  NewId(),
		frames:      []*protocol.Frame{{MessageType: protocol.MessageType_IpIpPacketFromProvider, MessageBytes: payload, Raw: true}},
		tagSendTime: 1,
	}
	encoded := marshalSendPackTransferFrame(m)
	var decoded protocol.TransferFrame
	if !unmarshalTransferFrame(encoded, &decoded, true) {
		MessagePoolReturn(encoded)
		t.Fatal("decode failed")
	}
	MessagePoolReturn(encoded)

	if decoded.Pack == nil || len(decoded.Pack.Frames) != 1 {
		returnDecodedTransferFrameMessageBytes(&decoded)
		t.Fatalf("decoded pack = %v", decoded.Pack)
	}
	messageBytes := decoded.Pack.Frames[0].MessageBytes
	pooled, shared := MessagePoolCheck(messageBytes)
	if !pooled || shared {
		returnDecodedTransferFrameMessageBytes(&decoded)
		t.Fatalf("decoded message pooled=%t shared=%t, want true/false", pooled, shared)
	}
	retained := MessagePoolShareReadOnly(messageBytes)
	returnDecodedTransferFrameMessageBytes(&decoded)
	pooled, shared = MessagePoolCheck(retained)
	if !pooled || !shared {
		MessagePoolReturn(retained)
		t.Fatalf("retained message pooled=%t shared=%t, want true/true", pooled, shared)
	}
	if !bytes.Equal(retained, payload) {
		MessagePoolReturn(retained)
		t.Fatal("retained payload changed after decoded owner returned")
	}
	MessagePoolReturn(retained)
	pooled, shared = MessagePoolCheck(retained)
	if pooled || shared {
		t.Fatalf("fully returned message pooled=%t shared=%t, want false/false", pooled, shared)
	}
}

func TestDecodeTransferFrameMalformed(t *testing.T) {
	// a valid frame, truncated at every prefix, must never panic and must fail
	// rather than produce garbage past the truncation.
	idA, idB := NewId(), NewId()
	mt := protocol.MessageType_TransferPack
	valid, _ := proto.Marshal(&protocol.TransferFrame{
		TransferPath: TransferPath{DestinationId: idA, SourceId: idB}.ToProtobuf(),
		MessageType:  &mt,
		Pack: &protocol.Pack{
			MessageId:  idA.Bytes(),
			SequenceId: idB.Bytes(),
			Frames:     []*protocol.Frame{{MessageType: protocol.MessageType_IpIpPacketToProvider, MessageBytes: []byte{1, 2, 3}, Raw: true}},
			Tag:        &protocol.Tag{SendTime: 1},
		},
	})
	for i := 0; i < len(valid); i++ {
		var tf protocol.TransferFrame
		// must not panic; truncated input should be rejected
		if unmarshalTransferFrame(valid[:i], &tf, true) {
			// a prefix could in rare cases be a self-consistent frame; only flag
			// if it also fails to decode under the official library
			var ref protocol.TransferFrame
			if proto.Unmarshal(valid[:i], &ref) != nil {
				t.Fatalf("accepted truncation at %d that proto rejects", i)
			}
		}
		returnDecodedTransferFrameMessageBytes(&tf)
	}
	// pure garbage must not panic
	for iter := 0; iter < 2000; iter++ {
		n := mathrandv2.IntN(32)
		g := make([]byte, n)
		for i := range g {
			g[i] = byte(mathrandv2.UintN(256))
		}
		var tf protocol.TransferFrame
		_ = unmarshalTransferFrame(g, &tf, true)
		returnDecodedTransferFrameMessageBytes(&tf)
	}
}

func TestFrameCodecAllocs(t *testing.T) {
	idA := NewId()
	m := &sendPackFrame{
		path:           TransferPath{DestinationId: idA, SourceId: NewId()},
		messageId:      NewId(),
		sequenceId:     idA,
		sequenceNumber: 5,
		head:           true,
		frames:         []*protocol.Frame{{MessageType: protocol.MessageType_IpIpPacketToProvider, MessageBytes: make([]byte, 1400), Raw: true}},
		tagSendTime:    1_700_000_000_000,
	}
	// one allocation expected: the pooled output buffer is drawn from the pool
	// after warmup, so steady-state allocs should be 0.
	allocs := testing.AllocsPerRun(1000, func() {
		b := marshalSendPackTransferFrame(m)
		MessagePoolReturn(b)
	})
	if 1 < allocs {
		t.Fatalf("marshalSendPackTransferFrame allocates %v per call, want <=1", allocs)
	}
}

func TestOwnedDecodeHasExplicitCallbackLifetime(t *testing.T) {
	payload := bytes.Repeat([]byte{0x6b}, DefaultMtu)
	m := &sendPackFrame{
		path:        TransferPath{DestinationId: NewId(), SourceId: NewId()},
		messageId:   NewId(),
		sequenceId:  NewId(),
		frames:      []*protocol.Frame{{MessageType: protocol.MessageType_IpIpPacketFromProvider, MessageBytes: payload, Raw: true}},
		tagSendTime: 123456789,
	}
	encoded := marshalSendPackTransferFrame(m)
	defer MessagePoolReturn(encoded)

	var decoded decodedTransferFrame
	if !unmarshalOwnedTransferFrame(encoded, &decoded, true) {
		t.Fatal("owned decode failed")
	}
	if decoded.packOwner == nil || decoded.frame.Pack != &decoded.packOwner.pack {
		decoded.release()
		t.Fatal("decoded Pack does not point at its explicit owner")
	}
	if got := decoded.frame.Pack.Frames[0]; got != &decoded.packOwner.frames[0] {
		decoded.release()
		t.Fatal("common-case Frame did not use inline owner storage")
	}
	if &decoded.frame.Pack.MessageId[0] != &decoded.packOwner.messageId[0] ||
		&decoded.frame.Pack.SequenceId[0] != &decoded.packOwner.sequenceId[0] {
		decoded.release()
		t.Fatal("decoded IDs do not use inline owner storage")
	}

	// receiveItem copies the RTT tag by value before returning the decoder
	// owner. This pins the subtle asynchronous ACK-window lifetime boundary.
	tag := sequenceTagFromProtocol(decoded.frame.Pack.Tag)
	frameBytes := decoded.frame.Pack.Frames[0].MessageBytes
	retained := MessagePoolShareReadOnly(frameBytes)
	decoded.release()
	if tag.protocol() == nil || tag.protocol().SendTime != m.tagSendTime {
		MessagePoolReturn(retained)
		t.Fatalf("copied tag changed after owner release: %+v", tag.protocol())
	}
	if !bytes.Equal(retained, payload) {
		MessagePoolReturn(retained)
		t.Fatal("shared callback payload changed after owner release")
	}
	MessagePoolReturn(retained)
}

func TestOwnedAckDecodeCopiesCompactQueueValueBeforeRelease(t *testing.T) {
	messageId := NewId()
	sequenceId := NewId()
	missingContractId := NewId()
	m := &sendAckFrame{
		path:                    TransferPath{DestinationId: NewId(), SourceId: NewId()},
		messageId:               messageId,
		sequenceId:              sequenceId,
		selective:               true,
		tagSendTime:             1_700_000_000_000,
		tagSet:                  true,
		missingContractId:       &missingContractId,
		compactContractRecovery: true,
		logicalLaneVersion:      transferLogicalLaneVersion,
	}
	encoded := marshalSendAckTransferFrame(m)
	defer MessagePoolReturn(encoded)

	decoded := inboundDecodedTransferFrames.take()
	if !unmarshalOwnedTransferFrame(encoded, decoded, true) {
		inboundDecodedTransferFrames.put(decoded)
		t.Fatal("owned ACK decode failed")
	}
	ack := decoded.frame.Ack
	if ack != &decoded.ack ||
		&ack.MessageId[0] != &decoded.ackIds[0][0] ||
		&ack.SequenceId[0] != &decoded.ackIds[1][0] ||
		&ack.MissingContractId[0] != &decoded.ackIds[2][0] ||
		ack.Tag != &decoded.ackTag {
		inboundDecodedTransferFrames.put(decoded)
		t.Fatal("owned ACK fields do not use inline decoder storage")
	}
	receiveAck, err := receiveAckMessageFromProtocol(ack)
	if err != nil {
		inboundDecodedTransferFrames.put(decoded)
		t.Fatalf("copy compact ACK: %s", err)
	}
	inboundDecodedTransferFrames.put(decoded)

	if receiveAck.messageId != messageId ||
		receiveAck.sequenceId != sequenceId ||
		receiveAck.missingContractId != missingContractId ||
		!receiveAck.selective || !receiveAck.contractMissing ||
		!receiveAck.compactContractRecoverySupported ||
		receiveAck.logicalLaneVersion != transferLogicalLaneVersion ||
		!receiveAck.tag.set || receiveAck.tag.sendTime != m.tagSendTime {
		t.Fatalf("compact ACK changed after decoder release: %+v", receiveAck)
	}
}

func TestOwnedAckDecodeSteadyStateDoesNotAllocate(t *testing.T) {
	m := &sendAckFrame{
		path:        TransferPath{DestinationId: NewId(), SourceId: NewId()},
		messageId:   NewId(),
		sequenceId:  NewId(),
		tagSendTime: 1,
		tagSet:      true,
	}
	encoded := marshalSendAckTransferFrame(m)
	defer MessagePoolReturn(encoded)

	allocs := testing.AllocsPerRun(1000, func() {
		decoded := inboundDecodedTransferFrames.take()
		if !unmarshalOwnedTransferFrame(encoded, decoded, true) {
			panic("owned ACK decode failed")
		}
		if _, err := receiveAckMessageFromProtocol(decoded.frame.Ack); err != nil {
			panic(err)
		}
		inboundDecodedTransferFrames.put(decoded)
	})
	if allocs != 0 {
		t.Fatalf("owned ACK decode + compact copy allocated %.2f times, want 0", allocs)
	}
}

func TestDecodedTransferFramePoolRetainedSizeStaysSmall(t *testing.T) {
	size := unsafe.Sizeof(decodedTransferFrame{})
	if size > 640 {
		t.Fatalf("decoded TransferFrame owner size = %d, want <= 640 bytes", size)
	}
	t.Logf(
		"decoded TransferFrame owner=%d bytes; exact pool ceiling=%d bytes",
		size,
		uintptr(decodedTransferFramePoolCapacity)*size,
	)
	ackSize := unsafe.Sizeof(receiveAckMessage{})
	if ackSize > 96 {
		t.Fatalf("compact receive ACK size = %d, want <= 96 bytes", ackSize)
	}
	t.Logf("compact receive ACK=%d bytes", ackSize)
}

func TestDecodedPackOwnerPoolRetentionIsBounded(t *testing.T) {
	size := unsafe.Sizeof(decodedPackOwner{})
	if size > uintptr(decodedPackOwnerQueueByteCount) {
		t.Fatalf(
			"decoded Pack owner size = %d, exceeds %d-byte receive-budget charge",
			size,
			decodedPackOwnerQueueByteCount,
		)
	}
	t.Logf(
		"decoded Pack owner=%d bytes; exact pool ceiling=%d bytes",
		size,
		uintptr(decodedPackOwnerPoolCapacity)*size,
	)
	pool := newDecodedPackOwnerPool()
	for i := 0; i < decodedPackOwnerPoolCapacity+97; i++ {
		pool.put(&decodedPackOwner{})
	}
	retained := 0
	for i := range pool.shards {
		shard := &pool.shards[i]
		shard.mu.Lock()
		retained += len(shard.free)
		if len(shard.free) > shard.limit {
			shard.mu.Unlock()
			t.Fatalf("shard %d retained %d > limit %d", i, len(shard.free), shard.limit)
		}
		shard.mu.Unlock()
	}
	if retained != decodedPackOwnerPoolCapacity {
		t.Fatalf("retained %d owners, want exact cap %d", retained, decodedPackOwnerPoolCapacity)
	}
}

func TestOwnedPackDecodeSteadyStateAllocs(t *testing.T) {
	frames := make([]*protocol.Frame, sendPackBatchMaxFrames)
	for i := range frames {
		frames[i] = &protocol.Frame{
			MessageType:  protocol.MessageType_IpIpPacketFromProvider,
			MessageBytes: make([]byte, 64),
			Raw:          true,
		}
	}
	m := &sendPackFrame{
		path:        TransferPath{DestinationId: NewId(), SourceId: NewId()},
		messageId:   NewId(),
		sequenceId:  NewId(),
		frames:      frames,
		tagSendTime: 1,
	}
	encoded := marshalSendPackTransferFrame(m)
	defer MessagePoolReturn(encoded)

	allocs := testing.AllocsPerRun(1000, func() {
		decoded := inboundDecodedTransferFrames.take()
		if !unmarshalOwnedTransferFrame(encoded, decoded, true) {
			panic("owned decode failed")
		}
		inboundDecodedTransferFrames.put(decoded)
	})
	t.Logf("owned %d-frame pack decode steady-state allocations: %.2f", len(frames), allocs)
	if allocs != 0 {
		t.Fatalf("owned pack decode allocated %.2f times per packet, want 0", allocs)
	}
}

func BenchmarkTransferFrameDecode(b *testing.B) {
	m := &sendPackFrame{
		path:        TransferPath{DestinationId: NewId(), SourceId: NewId()},
		messageId:   NewId(),
		sequenceId:  NewId(),
		frames:      []*protocol.Frame{{MessageType: protocol.MessageType_IpIpPacketFromProvider, MessageBytes: make([]byte, 1400), Raw: true}},
		tagSendTime: 1,
	}
	encoded := marshalSendPackTransferFrame(m)
	defer MessagePoolReturn(encoded)

	b.Run("copy-safe-protocol-objects", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var decoded protocol.TransferFrame
			if !unmarshalTransferFrame(encoded, &decoded, true) {
				b.Fatal("decode failed")
			}
			returnDecodedTransferFrameMessageBytes(&decoded)
		}
	})
	b.Run("bounded-owned-values", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			decoded := inboundDecodedTransferFrames.take()
			if !unmarshalOwnedTransferFrame(encoded, decoded, true) {
				b.Fatal("decode failed")
			}
			inboundDecodedTransferFrames.put(decoded)
		}
	})
}

func BenchmarkTransferAckFrameDecode(b *testing.B) {
	missingContractId := NewId()
	m := &sendAckFrame{
		path:                    TransferPath{DestinationId: NewId(), SourceId: NewId()},
		messageId:               NewId(),
		sequenceId:              NewId(),
		selective:               true,
		tagSendTime:             1_700_000_000_000,
		tagSet:                  true,
		missingContractId:       &missingContractId,
		compactContractRecovery: true,
		logicalLaneVersion:      transferLogicalLaneVersion,
	}
	encoded := marshalSendAckTransferFrame(m)
	defer MessagePoolReturn(encoded)

	b.Run("copy-safe-protocol-objects", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var decoded protocol.TransferFrame
			if !unmarshalTransferFrame(encoded, &decoded, true) {
				b.Fatal("decode failed")
			}
		}
	})
	b.Run("bounded-owned-values", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			decoded := inboundDecodedTransferFrames.take()
			if !unmarshalOwnedTransferFrame(encoded, decoded, true) {
				b.Fatal("decode failed")
			}
			if _, err := receiveAckMessageFromProtocol(decoded.frame.Ack); err != nil {
				b.Fatal(err)
			}
			inboundDecodedTransferFrames.put(decoded)
		}
	})
}

// ResidentMigrate rides the standard control frame path
// (CONNECTDRAIN2.md §3.3)
func TestResidentMigrateFrameRoundTrip(t *testing.T) {
	migrateTime := uint64(1752900000000)
	frame, err := ToFrame(&protocol.ResidentMigrate{
		MigrateTime: migrateTime,
	}, DefaultProtocolVersion)
	AssertEqual(t, err, nil)
	AssertEqual(t, protocol.MessageType_TransferResidentMigrate, frame.MessageType)

	message, err := FromFrame(frame)
	AssertEqual(t, err, nil)
	residentMigrate, ok := message.(*protocol.ResidentMigrate)
	AssertEqual(t, true, ok)
	AssertEqual(t, migrateTime, residentMigrate.MigrateTime)
}
