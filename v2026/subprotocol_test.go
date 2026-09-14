package connect

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// A hand-rolled test subprotocol: a bounded byte string in fixed storage, with
// the codec owning its instances so a receive allocates nothing.
type fixedMessage struct {
	value [64]byte
	n     int
}

func (self *fixedMessage) set(b []byte) {
	self.n = copy(self.value[:], b)
}

func (self *fixedMessage) bytes() []byte {
	return self.value[:self.n]
}

type fixedCodec struct {
	stateLock sync.Mutex
	free      []*fixedMessage
	// sizeDelta makes Size misreport, to exercise the overrun path
	sizeDelta int
	// unmarshalErr makes every decode fail
	unmarshalErr error
}

func (self *fixedCodec) Size(m *fixedMessage) int {
	return m.n + self.sizeDelta
}

func (self *fixedCodec) MarshalAppend(b []byte, m *fixedMessage) ([]byte, error) {
	return append(b, m.value[:m.n]...), nil
}

func (self *fixedCodec) Unmarshal(b []byte, m *fixedMessage) error {
	if self.unmarshalErr != nil {
		return self.unmarshalErr
	}
	if len(m.value) < len(b) {
		return errors.New("too long")
	}
	m.set(b)
	return nil
}

func (self *fixedCodec) New() *fixedMessage {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	if n := len(self.free); 0 < n {
		m := self.free[n-1]
		self.free = self.free[:n-1]
		return m
	}
	return &fixedMessage{}
}

func (self *fixedCodec) Release(m *fixedMessage) {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	m.n = 0
	self.free = append(self.free, m)
}

// A codec over plain byte slices with no instance ownership: the dispatch
// must construct the message itself.
type bytesCodec struct{}

func (self bytesCodec) Size(m *[]byte) int {
	return len(*m)
}

func (self bytesCodec) MarshalAppend(b []byte, m *[]byte) ([]byte, error) {
	return append(b, (*m)...), nil
}

func (self bytesCodec) Unmarshal(b []byte, m *[]byte) error {
	*m = append((*m)[:0], b...)
	return nil
}

func testSubprotocolClient(t *testing.T) *Client {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	client := NewClientWithDefaults(ctx, NewId(), NewNoContractClientOob())
	t.Cleanup(func() {
		client.Cancel()
		cancel()
	})
	return client
}

func testPeer() Peer {
	return Peer{ProvideMode: protocol.ProvideMode_Network}
}

func testSource() TransferPath {
	return TransferPath{SourceId: NewId()}
}

func TestSubprotocolEncoderMatchesProto(t *testing.T) {
	codec := bytesCodec{}
	ids := []SubprotocolId{1, 127, 128, 1023, 1024, 16384, 65535}
	// sizes spanning the pool classes: empty, small, the 256 and 2048 classes
	// and past them
	sizes := []int{0, 1, 100, 250, 256, 300, 2000, 2048, 2100, 5000, 9000, 70000}
	for _, id := range ids {
		for _, size := range sizes {
			payload := make([]byte, size)
			for i := range payload {
				payload[i] = byte(i*7 + int(id))
			}
			got, overrun, err := marshalSubprotocol[*[]byte](codec, id, &payload)
			if err != nil {
				t.Fatalf("id %d size %d: %v", id, size, err)
			}
			if overrun {
				t.Fatalf("id %d size %d: unexpected overrun", id, size)
			}
			want, err := proto.Marshal(&protocol.SubprotocolMessage{SubprotocolId: uint32(id), MessageBytes: payload})
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(got, want) {
				t.Fatalf("id %d size %d: encoding differs from proto.Marshal", id, size)
			}
			if wrapped := wrapSubprotocolBytes(id, payload); !bytes.Equal(wrapped, want) {
				t.Fatalf("id %d size %d: wrapped bytes differ from proto.Marshal", id, size)
			} else {
				MessagePoolReturn(wrapped)
			}
			// pooled exactly when a buffer of that size has a pool class
			probe, expectPooled := MessagePoolGetDetailed(len(want))
			MessagePoolReturn(probe)
			if pooled, _ := MessagePoolCheck(got); pooled != expectPooled {
				t.Fatalf("id %d size %d: encoder output pooled=%v, want %v", id, size, pooled, expectPooled)
			}
			MessagePoolReturn(got)
		}
	}
}

func TestSubprotocolEncoderOverrun(t *testing.T) {
	for _, delta := range []int{-3, -1, 1, 5, 300} {
		codec := &fixedCodec{sizeDelta: delta}
		m := &fixedMessage{}
		m.set([]byte("a message whose size the codec misreports"))
		got, overrun, err := marshalSubprotocol[*fixedMessage](codec, 2048, m)
		if err != nil {
			t.Fatal(err)
		}
		if !overrun {
			t.Fatalf("delta %d: overrun not reported", delta)
		}
		want, _ := proto.Marshal(&protocol.SubprotocolMessage{SubprotocolId: 2048, MessageBytes: m.bytes()})
		if !bytes.Equal(got, want) {
			t.Fatalf("delta %d: re-wrapped encoding differs from proto.Marshal", delta)
		}
		MessagePoolReturn(got)
	}
	codec := &fixedCodec{sizeDelta: -100}
	if _, _, err := marshalSubprotocol[*fixedMessage](codec, 2048, &fixedMessage{}); err == nil {
		t.Fatal("a negative size must be an error")
	}
}

func TestSubprotocolDecoderMatchesProto(t *testing.T) {
	payload := []byte("payload bytes")
	encoded, _ := proto.Marshal(&protocol.SubprotocolMessage{SubprotocolId: 4321, MessageBytes: payload})
	id, got, ok := decodeSubprotocolHeader(encoded)
	if !ok || id != 4321 || !bytes.Equal(got, payload) {
		t.Fatalf("decode: ok=%v id=%d payload=%q", ok, id, got)
	}
	// the payload is a view of the encoded bytes, not a copy
	if len(got) != 0 && &got[0] != &encoded[len(encoded)-len(payload)] {
		t.Fatal("payload is a copy")
	}

	// unknown fields before and after are skipped, and proto agrees
	var extra []byte
	extra = protowire.AppendTag(extra, 3, protowire.VarintType)
	extra = protowire.AppendVarint(extra, 99)
	extra = append(extra, encoded...)
	extra = protowire.AppendTag(extra, 7, protowire.BytesType)
	extra = protowire.AppendBytes(extra, []byte("future"))
	id, got, ok = decodeSubprotocolHeader(extra)
	if !ok || id != 4321 || !bytes.Equal(got, payload) {
		t.Fatalf("decode with unknown fields: ok=%v id=%d payload=%q", ok, id, got)
	}
	var reference protocol.SubprotocolMessage
	if err := proto.Unmarshal(extra, &reference); err != nil || reference.SubprotocolId != 4321 || !bytes.Equal(reference.MessageBytes, payload) {
		t.Fatalf("proto disagrees on unknown fields: %v %+v", err, &reference)
	}

	// a repeated singular field: the last value wins, as in proto
	var dup []byte
	dup = append(dup, encoded...)
	dup = protowire.AppendTag(dup, subprotocolFieldId, protowire.VarintType)
	dup = protowire.AppendVarint(dup, 5000)
	dup = protowire.AppendTag(dup, subprotocolFieldMessageBytes, protowire.BytesType)
	dup = protowire.AppendBytes(dup, []byte("second"))
	id, got, ok = decodeSubprotocolHeader(dup)
	if !ok || id != 5000 || string(got) != "second" {
		t.Fatalf("decode duplicates: ok=%v id=%d payload=%q", ok, id, got)
	}
	reference = protocol.SubprotocolMessage{}
	if err := proto.Unmarshal(dup, &reference); err != nil || reference.SubprotocolId != 5000 || string(reference.MessageBytes) != "second" {
		t.Fatalf("proto disagrees on duplicates: %v %+v", err, &reference)
	}

	// malformed and invalid inputs are not ok
	bad := [][]byte{
		encoded[:len(encoded)-3], // truncated payload
		{0x08},                   // truncated varint
		{0x0a, 0x01, 0x00},       // message_bytes only: no id
		{0x08, 0x00},             // id 0
		{0x08, 0x80, 0x80, 0x04}, // id 65536
		{0x0d, 0x01, 0x02, 0x03, 0x04, 0x08, 0x01}, // wrong wire type for field 1
		{0x10, 0x01, 0x08, 0x01},                   // wrong wire type for field 2
	}
	for i, b := range bad {
		if _, _, ok := decodeSubprotocolHeader(b); ok {
			t.Fatalf("bad input %d decoded", i)
		}
	}
	// an empty payload is a valid message
	empty, _ := proto.Marshal(&protocol.SubprotocolMessage{SubprotocolId: 1024})
	if id, got, ok := decodeSubprotocolHeader(empty); !ok || id != 1024 || len(got) != 0 {
		t.Fatalf("empty payload: ok=%v id=%d payload=%q", ok, id, got)
	}
}

func subprotocolTestFrame(id SubprotocolId, payload []byte) *protocol.Frame {
	return &protocol.Frame{
		MessageType:  protocol.MessageType_Subprotocol,
		MessageBytes: wrapSubprotocolBytes(id, payload),
	}
}

func TestSubprotocolDispatchOrderAndDrops(t *testing.T) {
	client := testSubprotocolClient(t)
	codec := &fixedCodec{}
	var order []string
	var rawSeen, typedSeen []byte
	remove, err := client.AddSubprotocolRawCallback(2000, func(source TransferPath, id SubprotocolId, b []byte, peer Peer) {
		order = append(order, "raw1")
		rawSeen = append([]byte(nil), b...)
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.AddSubprotocolRawCallback(2000, func(source TransferPath, id SubprotocolId, b []byte, peer Peer) {
		order = append(order, "raw2")
	}); err != nil {
		t.Fatal(err)
	}
	unregister, err := RegisterSubprotocol[*fixedMessage](client, 2000, codec, func(source TransferPath, m *fixedMessage, peer Peer) {
		order = append(order, "typed")
		typedSeen = append([]byte(nil), m.bytes()...)
	})
	if err != nil {
		t.Fatal(err)
	}

	other := &protocol.Frame{MessageType: protocol.MessageType_TestSimpleMessage}
	frames := []*protocol.Frame{other, subprotocolTestFrame(2000, []byte("hello")), subprotocolTestFrame(2001, []byte("nobody"))}
	kept := client.dispatchSubprotocolFrames(testSource(), frames, testPeer())
	if len(kept) != 1 || kept[0] != other {
		t.Fatalf("kept %v", kept)
	}
	if len(order) != 3 || order[0] != "raw1" || order[1] != "raw2" || order[2] != "typed" {
		t.Fatalf("order %v", order)
	}
	if string(rawSeen) != "hello" || string(typedSeen) != "hello" {
		t.Fatalf("raw %q typed %q", rawSeen, typedSeen)
	}
	stats := client.SubprotocolStats()
	if stats.Received != 1 || stats.ReceivedByteCount != 5 || stats.DroppedUnregistered != 1 || stats.ReceivedById[2000] != 1 {
		t.Fatalf("stats %+v", stats)
	}
	for _, frame := range frames[1:] {
		MessagePoolReturn(frame.MessageBytes)
	}

	// a batch without subprotocol frames is returned as is
	plain := []*protocol.Frame{other, other}
	if kept := client.dispatchSubprotocolFrames(testSource(), plain, testPeer()); len(kept) != 2 || &kept[0] != &plain[0] {
		t.Fatal("a plain batch must be returned unchanged")
	}

	// a decode failure after the raw listeners is counted, the handler not called
	codec.unmarshalErr = errors.New("bad")
	order = nil
	frame := subprotocolTestFrame(2000, []byte("x"))
	client.dispatchSubprotocolFrames(testSource(), []*protocol.Frame{frame}, testPeer())
	MessagePoolReturn(frame.MessageBytes)
	if len(order) != 2 || order[0] != "raw1" {
		t.Fatalf("order on decode failure %v", order)
	}
	if stats := client.SubprotocolStats(); stats.DroppedDecode != 1 {
		t.Fatalf("decode drop not counted: %+v", stats)
	}
	codec.unmarshalErr = nil

	// a malformed wrapper is dropped and counted
	malformed := &protocol.Frame{MessageType: protocol.MessageType_Subprotocol, MessageBytes: []byte{0x08}}
	client.dispatchSubprotocolFrames(testSource(), []*protocol.Frame{malformed}, testPeer())
	if stats := client.SubprotocolStats(); stats.DroppedDecode != 2 {
		t.Fatalf("malformed drop not counted: %+v", stats)
	}

	// listeners can be removed; the id with nothing left drops
	remove()
	unregister()
	order = nil
	frame = subprotocolTestFrame(2000, []byte("y"))
	client.dispatchSubprotocolFrames(testSource(), []*protocol.Frame{frame}, testPeer())
	MessagePoolReturn(frame.MessageBytes)
	if len(order) != 1 || order[0] != "raw2" {
		t.Fatalf("order after removal %v", order)
	}
	if _, ok := client.subprotocols.table.Load().entries[2001]; ok {
		t.Fatal("an unregistered id must have no entry")
	}
}

func TestSubprotocolDispatchPanicIsolation(t *testing.T) {
	client := testSubprotocolClient(t)
	if _, err := client.AddSubprotocolRawCallback(2000, func(TransferPath, SubprotocolId, []byte, Peer) {
		panic("raw")
	}); err != nil {
		t.Fatal(err)
	}
	typedCalled := false
	if _, err := RegisterSubprotocol[*fixedMessage](client, 2000, &fixedCodec{}, func(TransferPath, *fixedMessage, Peer) {
		typedCalled = true
		panic("typed")
	}); err != nil {
		t.Fatal(err)
	}
	frame := subprotocolTestFrame(2000, []byte("z"))
	kept := client.dispatchSubprotocolFrames(testSource(), []*protocol.Frame{frame}, testPeer())
	MessagePoolReturn(frame.MessageBytes)
	if len(kept) != 0 || !typedCalled {
		t.Fatal("a listener panic must not stop the dispatch")
	}
}

func TestSubprotocolRegistrationRules(t *testing.T) {
	client := testSubprotocolClient(t)
	noop := func(TransferPath, SubprotocolId, []byte, Peer) {}
	handler := func(TransferPath, *fixedMessage, Peer) {}
	if _, err := client.AddSubprotocolRawCallback(0, noop); !errors.Is(err, errSubprotocolIdZero) {
		t.Fatalf("id 0: %v", err)
	}
	if _, err := client.AddSubprotocolRawCallback(1023, noop); !errors.Is(err, errSubprotocolIdReserved) {
		t.Fatalf("reserved raw: %v", err)
	}
	if _, err := RegisterSubprotocol[*fixedMessage](client, 1, &fixedCodec{}, handler); !errors.Is(err, errSubprotocolIdReserved) {
		t.Fatalf("reserved typed: %v", err)
	}
	if _, err := registerReservedSubprotocol[*fixedMessage](client, 1, &fixedCodec{}, handler); err != nil {
		t.Fatalf("the network's own registration: %v", err)
	}
	if _, err := client.addReservedSubprotocolRawCallback(2, noop); err != nil {
		t.Fatalf("the network's own raw listener: %v", err)
	}
	unregister, err := RegisterSubprotocol[*fixedMessage](client, 1024, &fixedCodec{}, handler)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterSubprotocol[*fixedMessage](client, 1024, &fixedCodec{}, handler); !errors.Is(err, errSubprotocolCodecExists) {
		t.Fatalf("second codec: %v", err)
	}
	unregister()
	if _, err := RegisterSubprotocol[*fixedMessage](client, 1024, &fixedCodec{}, handler); err != nil {
		t.Fatalf("after unregister: %v", err)
	}
	if _, err := client.AddSubprotocolRawCallback(65535, noop); err != nil {
		t.Fatalf("max id: %v", err)
	}
	ids := client.subprotocols.table.Load().ids()
	if len(ids) != 4 || ids[0] != 1 || ids[1] != 2 || ids[2] != 1024 || ids[3] != 65535 {
		t.Fatalf("ids %v", ids)
	}
}

func TestSubprotocolSendRules(t *testing.T) {
	client := testSubprotocolClient(t)
	codec := &fixedCodec{}
	m := &fixedMessage{}
	m.set([]byte("m"))
	if _, err := SendSubprotocolWithTimeout[*fixedMessage](client, 2000, m, ControlId, nil, 0); !errors.Is(err, errSubprotocolControl) {
		t.Fatalf("control id: %v", err)
	}
	if client.SendSubprotocolBytes(2000, []byte("m"), ControlId, nil) {
		t.Fatal("control id bytes send must fail")
	}
	if SendSubprotocolMultiHop[*fixedMessage](client, 2000, m, RequireMultiHopId(NewId(), ControlId), nil) {
		t.Fatal("control id multi-hop send must fail")
	}
	if _, err := client.QuerySubprotocols(context.Background(), ControlId); !errors.Is(err, errSubprotocolControl) {
		t.Fatalf("control id query: %v", err)
	}
	// no codec anywhere
	if _, err := SendSubprotocolWithTimeout[*fixedMessage](client, 2000, m, NewId(), nil, 0); !errors.Is(err, errSubprotocolNoCodec) {
		t.Fatalf("no codec: %v", err)
	}
	// the registered codec's message type must match
	if _, err := RegisterSubprotocol[*fixedMessage](client, 2000, codec, func(TransferPath, *fixedMessage, Peer) {}); err != nil {
		t.Fatal(err)
	}
	b := []byte("m")
	if _, err := SendSubprotocolWithTimeout[*[]byte](client, 2000, &b, NewId(), nil, 0); !errors.Is(err, errSubprotocolCodecType) {
		t.Fatalf("codec type mismatch: %v", err)
	}
	if stats := client.SubprotocolStats(); stats.Sent != 0 {
		t.Fatalf("nothing may have been sent: %+v", stats)
	}
}

func TestSubprotocolSendFailureReturnsBuffer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	client := NewClientWithDefaults(ctx, NewId(), NewNoContractClientOob())
	client.Cancel()
	cancel()
	// the client's own teardown returns its buffers asynchronously; let it
	// settle so the packet-class outstanding count reflects only the sends
	time.Sleep(100 * time.Millisecond)
	codec := &fixedCodec{}
	m := &fixedMessage{}
	m.set([]byte("after cancel"))
	outstandingBefore := MessagePoolPacketOutstandingCount()
	success, err := SendSubprotocolWithTimeout[*fixedMessage](client, 2000, m, NewId(), nil, 0, WithSubprotocolCodec[*fixedMessage](codec))
	if success || err == nil {
		t.Fatal("a send on a cancelled client must fail")
	}
	if client.SendSubprotocolBytes(2000, []byte("after cancel"), NewId(), nil) {
		t.Fatal("a bytes send on a cancelled client must fail")
	}
	if SendSubprotocolMulti[*fixedMessage](client, 2000, []*fixedMessage{m, m}, NewId(), nil, WithSubprotocolCodec[*fixedMessage](codec)) {
		t.Fatal("a multi send on a cancelled client must fail")
	}
	if outstandingAfter := MessagePoolPacketOutstandingCount(); outstandingBefore < outstandingAfter {
		t.Fatalf("pool buffers leaked on failed sends: outstanding %d -> %d", outstandingBefore, outstandingAfter)
	}
}

func TestSubprotocolRetainBytes(t *testing.T) {
	payload := []byte("keep me")
	frame := subprotocolTestFrame(2000, payload)
	_, view, ok := decodeSubprotocolHeader(frame.MessageBytes)
	if !ok {
		t.Fatal("decode")
	}
	retained, release := RetainSubprotocolBytes(view)
	MessagePoolReturn(frame.MessageBytes)
	if string(retained) != "keep me" {
		t.Fatalf("retained %q", retained)
	}
	if pooled, _ := MessagePoolCheck(retained); !pooled {
		t.Fatal("the retained copy must be a pool buffer")
	}
	release()
	if pooled, _ := MessagePoolCheck(retained); pooled {
		t.Fatal("release must return the copy")
	}
}

func TestSubprotocolProtoCodec(t *testing.T) {
	codec := ProtoCodec[*protocol.SimpleMessage]()
	m := &protocol.SimpleMessage{Content: "proto"}
	got, overrun, err := marshalSubprotocol[*protocol.SimpleMessage](codec, 3000, m)
	if err != nil || overrun {
		t.Fatalf("marshal: %v overrun %v", err, overrun)
	}
	want, _ := proto.Marshal(&protocol.SubprotocolMessage{SubprotocolId: 3000, MessageBytes: mustProtoMarshal(t, m)})
	if !bytes.Equal(got, want) {
		t.Fatal("proto codec encoding differs")
	}
	_, payload, _ := decodeSubprotocolHeader(got)
	pool := codec.(SubprotocolMessagePool[*protocol.SimpleMessage])
	decoded := pool.New()
	if decoded == nil {
		t.Fatal("New must construct an instance")
	}
	if err := codec.Unmarshal(payload, decoded); err != nil || decoded.Content != "proto" {
		t.Fatalf("unmarshal: %v %+v", err, decoded)
	}
	MessagePoolReturn(got)

	// the plain-struct factory
	factory := newSubprotocolMessageFactory[*[]byte]()
	if p := factory(); p == nil {
		t.Fatal("pointer factory")
	}
	valueFactory := newSubprotocolMessageFactory[int]()
	if v := valueFactory(); v != 0 {
		t.Fatal("value factory")
	}
}

func mustProtoMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSubprotocolQueryCompletion(t *testing.T) {
	client := testSubprotocolClient(t)
	queryId := NewId()
	resultChan := make(chan []SubprotocolId, 1)
	client.subprotocols.stateLock.Lock()
	client.subprotocols.pendingQueries[queryId] = resultChan
	client.subprotocols.stateLock.Unlock()
	result := &protocol.SubprotocolsQueryResult{QueryId: queryId.Bytes(), SubprotocolIds: []uint32{9000, 2000, 2000, 0, 70000, 1500}}
	frame, err := ToFrame(result, DefaultProtocolVersion)
	if err != nil {
		t.Fatal(err)
	}
	kept := client.dispatchSubprotocolFrames(testSource(), []*protocol.Frame{frame}, testPeer())
	MessagePoolReturn(frame.MessageBytes)
	if len(kept) != 0 {
		t.Fatal("a query result must not reach the callbacks")
	}
	select {
	case ids := <-resultChan:
		if len(ids) != 3 || ids[0] != 1500 || ids[1] != 2000 || ids[2] != 9000 {
			t.Fatalf("ids %v", ids)
		}
	default:
		t.Fatal("pending query not completed")
	}

	// a query on a client with no route: answered, the reply enqueue counted
	// when it cannot go out
	query, _ := ToFrame(&protocol.SubprotocolsQuery{QueryId: NewId().Bytes()}, DefaultProtocolVersion)
	kept = client.dispatchSubprotocolFrames(testSource(), []*protocol.Frame{query}, testPeer())
	MessagePoolReturn(query.MessageBytes)
	if len(kept) != 0 {
		t.Fatal("a query must not reach the callbacks")
	}
	if stats := client.SubprotocolStats(); stats.QueriesAnswered != 1 {
		t.Fatalf("query not answered: %+v", stats)
	}
}

func TestSubprotocolSendAllocations(t *testing.T) {
	client := testSubprotocolClient(t)
	codec := &fixedCodec{}
	m := &fixedMessage{}
	m.set(bytes.Repeat([]byte("s"), 40))
	packet := bytes.Repeat([]byte("p"), 40)
	WarmMessagePools()

	buildSubprotocol := testing.AllocsPerRun(200, func() {
		frame, err := encodeSubprotocolFrame[*fixedMessage](client, codec, 2000, m)
		if err != nil {
			t.Fatal(err)
		}
		MessagePoolReturn(frame.MessageBytes)
	})
	buildIp := testing.AllocsPerRun(200, func() {
		frame, err := ipPacketToProviderFrame(packet, DefaultProtocolVersion)
		if err != nil {
			t.Fatal(err)
		}
		_ = frame
	})
	t.Logf("build allocations: subprotocol %.1f, ip raw frame %.1f", buildSubprotocol, buildIp)
	if buildIp < buildSubprotocol {
		t.Fatalf("building a subprotocol frame allocates more (%.1f) than an ip raw frame (%.1f)", buildSubprotocol, buildIp)
	}

	// the whole send path: an enqueue with no wait on a client with no route
	destinationId := NewId()
	sendSubprotocol := testing.AllocsPerRun(50, func() {
		SendSubprotocolWithTimeout[*fixedMessage](client, 2000, m, destinationId, nil, 0, WithSubprotocolCodec[*fixedMessage](codec))
	})
	sendIp := testing.AllocsPerRun(50, func() {
		frame, _ := ipPacketToProviderFrame(MessagePoolCopy(packet), DefaultProtocolVersion)
		if !client.SendWithTimeout(frame, destinationId, nil, 0) {
			MessagePoolReturn(frame.MessageBytes)
		}
	})
	t.Logf("send allocations: subprotocol %.1f, ip raw frame %.1f", sendSubprotocol, sendIp)
	// the codec option adds one boxed value; everything else is the same path
	if sendIp+1 < sendSubprotocol {
		t.Fatalf("sending a subprotocol frame allocates more (%.1f) than an ip raw frame (%.1f)", sendSubprotocol, sendIp)
	}
}

func TestSubprotocolReceiveAllocations(t *testing.T) {
	client := testSubprotocolClient(t)
	codec := &fixedCodec{}
	received := 0
	if _, err := RegisterSubprotocol[*fixedMessage](client, 2000, codec, func(source TransferPath, m *fixedMessage, peer Peer) {
		received += m.n
	}); err != nil {
		t.Fatal(err)
	}
	frame := subprotocolTestFrame(2000, bytes.Repeat([]byte("r"), 40))
	defer MessagePoolReturn(frame.MessageBytes)
	frames := []*protocol.Frame{frame}
	source := testSource()
	peer := testPeer()
	client.dispatchSubprotocolFrames(source, frames, peer)
	allocs := testing.AllocsPerRun(500, func() {
		client.dispatchSubprotocolFrames(source, frames, peer)
	})
	t.Logf("receive dispatch allocations: %.2f", allocs)
	if allocs != 0 {
		t.Fatalf("receive dispatch with a pooled codec allocates %.2f per message", allocs)
	}
	if received == 0 {
		t.Fatal("handler not called")
	}
}
