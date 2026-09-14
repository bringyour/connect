package connect

// Subprotocols on the transfer layer (SUBPROTOCOL.md).
//
// A subprotocol is an application protocol that rides the core as data: a
// 16-bit id and its own encoding, carried in a `MessageType_Subprotocol` frame
// whose bytes are a `protocol.SubprotocolMessage{subprotocol_id, message_bytes}`.
// The core never interprets the payload. This file holds everything the core
// needs for that: the hand-rolled wrapper codec (one pool buffer per message,
// the subprotocol's codec writing and reading in place), the per-client
// registry of codecs and raw listeners, the dispatch that runs ahead of the
// generic receive callbacks, the send functions, the peer query and the stats.
//
// Ownership follows the message pool rules. A send takes one pool buffer of
// exactly the wrapper size, the codec appends into it, and the buffer's
// ownership passes to the send on success; on failure it is returned here. On
// receive the frame's pooled bytes are owned by the receive item and every
// listener borrows them, the payload being a sub-slice; a listener that needs
// the bytes after it returns retains a pooled copy.
//
// Dispatch runs inline on the receive goroutine and is lock-free: the registry
// is an immutable table behind an atomic pointer that registration replaces.
// Per subprotocol frame the order is fixed: every raw listener of the id, then
// the typed handler when the id has a codec and the bytes decode. An id with
// neither is dropped and counted. The two query frame types are answered and
// completed here and never reach application callbacks.

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/urnetwork/connect/v2026/protocol"
)

// Identifies a subprotocol on the wire. 0 is invalid; ids below
// `SubprotocolReservedLimit` are the network's own.
type SubprotocolId uint16

// Ids below this are reserved for the network, like privileged ports; the
// public registration and listener functions refuse them.
const SubprotocolReservedLimit SubprotocolId = 1024

// The pluggable marshaller of one subprotocol, in the shape of the Go protobuf
// package functions: size first, append into a caller-provided buffer, decode
// from a borrowed slice. Called from the send path and, for Unmarshal, inline
// on the receive goroutine, so it must be safe for concurrent use.
type SubprotocolCodec[T any] interface {
	// The exact encoded size of m; the send path takes one pool buffer sized
	// from it. A codec that misreports it still works, at the cost of a second
	// buffer and a copy, which the stats count as an overrun.
	Size(m T) int
	// Appends the encoding of m to b (cap(b)-len(b) >= Size(m)) and returns it.
	MarshalAppend(b []byte, m T) ([]byte, error)
	// Decodes b into m. b is borrowed for the call and must not be retained.
	Unmarshal(b []byte, m T) error
}

// A codec may also own its message instances. When it does, the dispatch takes
// each received message from New and hands it back with Release after the
// handler returns, which is what makes the receive path allocation-free.
type SubprotocolMessagePool[T any] interface {
	New() T
	Release(m T)
}

// Receives the raw bytes of a subprotocol message before any codec runs.
// Inline on the receive goroutine and must not block; messageBytes is borrowed
// until the function returns (see RetainSubprotocolBytes).
type SubprotocolRawFunction = func(source TransferPath, subprotocolId SubprotocolId, messageBytes []byte, peer Peer)

// Receives the decoded message of a subprotocol, after the raw listeners.
// Inline on the receive goroutine and must not block; message and any bytes it
// aliases are borrowed until the function returns.
type SubprotocolHandler[T any] = func(source TransferPath, message T, peer Peer)

var (
	errSubprotocolIdZero        = errors.New("subprotocol id 0 is invalid")
	errSubprotocolIdReserved    = fmt.Errorf("subprotocol ids below %d are reserved for the network", SubprotocolReservedLimit)
	errSubprotocolCodecExists   = errors.New("subprotocol already has a codec")
	errSubprotocolNoCodec       = errors.New("subprotocol has no codec: register one or pass WithSubprotocolCodec")
	errSubprotocolCodecType     = errors.New("subprotocol codec message type does not match")
	errSubprotocolControl       = errors.New("the control id speaks only the top-level protocol")
	errSubprotocolNegativeSize  = errors.New("subprotocol codec reported a negative size")
	errSubprotocolQueryNoResult = errors.New("subprotocols query got no result")
)

// --- the wrapper codec ---

const (
	subprotocolFieldId           = 1
	subprotocolFieldMessageBytes = 2
	subprotocolIdMax             = 0xFFFF
)

// The encoded size of the wrapper's fields before the payload, for a payload
// of n bytes. Follows proto3 implicit presence: message_bytes is omitted when
// empty, and the id is never zero so it is always present.
func subprotocolHeaderSize(subprotocolId SubprotocolId, n int) int {
	size := protoSizeTag(subprotocolFieldId) + protoSizeVarint(uint64(subprotocolId))
	if 0 < n {
		size += protoSizeTag(subprotocolFieldMessageBytes) + protoSizeVarint(uint64(n))
	}
	return size
}

func appendSubprotocolHeader(b []byte, subprotocolId SubprotocolId, n int) []byte {
	b = protoAppendTag(b, subprotocolFieldId, protoWireVarint)
	b = protoAppendVarint(b, uint64(subprotocolId))
	if 0 < n {
		b = protoAppendTag(b, subprotocolFieldMessageBytes, protoWireBytes)
		b = protoAppendVarint(b, uint64(n))
	}
	return b
}

// Wraps an already encoded payload: one pool buffer, the header, one copy of
// the payload. Returns the message-pool owned wrapper bytes.
func wrapSubprotocolBytes(subprotocolId SubprotocolId, payload []byte) []byte {
	n := len(payload)
	buf := MessagePoolGet(subprotocolHeaderSize(subprotocolId, n) + n)
	b := appendSubprotocolHeader(buf[:0], subprotocolId, n)
	b = append(b, payload...)
	return b
}

// Encodes m with codec into one pool buffer: the wrapper header first, then
// the codec appends the payload in place. The returned bytes are owned by the
// message pool and are byte-identical to proto.Marshal of the equivalent
// SubprotocolMessage. A codec whose Size disagrees with what MarshalAppend
// wrote gets a second buffer with the right length prefix and one copy, and
// `overrun` reports it so the stats can count a misbehaving codec.
func marshalSubprotocol[T any](codec SubprotocolCodec[T], subprotocolId SubprotocolId, m T) (out []byte, overrun bool, err error) {
	n := codec.Size(m)
	if n < 0 {
		return nil, false, errSubprotocolNegativeSize
	}
	buf := MessagePoolGet(subprotocolHeaderSize(subprotocolId, n) + n)
	b := appendSubprotocolHeader(buf[:0], subprotocolId, n)
	headerLen := len(b)
	b, err = codec.MarshalAppend(b, m)
	if err != nil {
		MessagePoolReturn(buf)
		return nil, false, err
	}
	written := len(b) - headerLen
	if written == n && cap(b) == cap(buf) {
		return b, false, nil
	}
	// the codec wrote a different length than it sized, or grew past the
	// buffer: re-wrap with the true length so the prefix is right
	out = wrapSubprotocolBytes(subprotocolId, b[headerLen:])
	MessagePoolReturn(buf)
	if cap(b) != cap(buf) {
		MessagePoolReturn(b)
	}
	return out, true, nil
}

// Decodes the wrapper header over the frame's bytes with no copy: the payload
// is a sub-slice of b. Unknown fields are skipped and the last value of a
// repeated singular field wins, as proto.Unmarshal does; a malformed message,
// a missing id or an id outside 1..65535 is not ok.
func decodeSubprotocolHeader(b []byte) (subprotocolId SubprotocolId, payload []byte, ok bool) {
	var id uint64
	seenId := false
	for 0 < len(b) {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return 0, nil, false
		}
		b = b[n:]
		switch num {
		case subprotocolFieldId:
			if typ != protowire.VarintType {
				return 0, nil, false
			}
			v, vn := protowire.ConsumeVarint(b)
			if vn < 0 {
				return 0, nil, false
			}
			b = b[vn:]
			id = v
			seenId = true
		case subprotocolFieldMessageBytes:
			if typ != protowire.BytesType {
				return 0, nil, false
			}
			v, vn := protowire.ConsumeBytes(b)
			if vn < 0 {
				return 0, nil, false
			}
			b = b[vn:]
			payload = v
		default:
			fn := protowire.ConsumeFieldValue(num, typ, b)
			if fn < 0 {
				return 0, nil, false
			}
			b = b[fn:]
		}
	}
	if !seenId || id == 0 || subprotocolIdMax < id {
		return 0, nil, false
	}
	return SubprotocolId(id), payload, true
}

// --- codecs ---

type protoCodec[T proto.Message] struct{}

// The codec of any protobuf message type, on proto.Size,
// MarshalOptions.MarshalAppend and UnmarshalOptions.Unmarshal. It also owns
// its instances through the message's reflection, so the dispatch never needs
// a separate constructor; Release is a no-op (the instance is left to the GC).
func ProtoCodec[T proto.Message]() SubprotocolCodec[T] {
	return protoCodec[T]{}
}

func (self protoCodec[T]) Size(m T) int {
	return proto.Size(m)
}

func (self protoCodec[T]) MarshalAppend(b []byte, m T) ([]byte, error) {
	return proto.MarshalOptions{}.MarshalAppend(b, m)
}

func (self protoCodec[T]) Unmarshal(b []byte, m T) error {
	return proto.UnmarshalOptions{}.Unmarshal(b, m)
}

func (self protoCodec[T]) New() T {
	var zero T
	return zero.ProtoReflect().New().Interface().(T)
}

func (self protoCodec[T]) Release(m T) {
}

// A constructor for T when the codec does not own instances: a pointer type
// gets a fresh pointee, anything else its zero value.
func newSubprotocolMessageFactory[T any]() func() T {
	var zero T
	t := reflect.TypeOf(&zero).Elem()
	if t.Kind() == reflect.Pointer {
		elem := t.Elem()
		return func() T {
			return reflect.New(elem).Interface().(T)
		}
	}
	return func() T {
		var m T
		return m
	}
}

// --- registry ---

// The type-erased typed side of one id: decode and deliver, with the codec's
// own instances when it has them. A method value, not a closure, so a delivery
// allocates nothing of its own.
type subprotocolTypedDeliverer interface {
	deliver(source TransferPath, messageBytes []byte, peer Peer) error
	codec() any
}

type subprotocolTyped[T any] struct {
	subprotocolCodec SubprotocolCodec[T]
	handler          SubprotocolHandler[T]
	pool             SubprotocolMessagePool[T]
	factory          func() T
	log              Logger
	clientTag        string
}

func (self *subprotocolTyped[T]) codec() any {
	return self.subprotocolCodec
}

func (self *subprotocolTyped[T]) deliver(source TransferPath, messageBytes []byte, peer Peer) error {
	var m T
	if self.pool != nil {
		m = self.pool.New()
		defer self.pool.Release(m)
	} else {
		m = self.factory()
	}
	if err := self.subprotocolCodec.Unmarshal(messageBytes, m); err != nil {
		return err
	}
	// isolate a handler panic like Client.receive isolates a callback panic
	defer func() {
		if r := recover(); r != nil {
			if self.log != nil && self.log.V(1).Enabled() {
				self.log.Infof("[c]subprotocol handler %s panic = %v\n", self.clientTag, r)
			}
		}
	}()
	self.handler(source, m, peer)
	return nil
}

type subprotocolRawCallback struct {
	callbackId uint64
	callback   SubprotocolRawFunction
}

// One id's listeners. Immutable once published; the received counter is a
// pointer so a republished table keeps counting the same cell.
type subprotocolEntry struct {
	rawCallbacks  []subprotocolRawCallback
	typed         subprotocolTypedDeliverer
	receivedCount *atomic.Uint64
}

func (self *subprotocolEntry) clone() *subprotocolEntry {
	return &subprotocolEntry{
		rawCallbacks:  append([]subprotocolRawCallback(nil), self.rawCallbacks...),
		typed:         self.typed,
		receivedCount: self.receivedCount,
	}
}

// The immutable table the receive path reads.
type subprotocolTable struct {
	entries map[SubprotocolId]*subprotocolEntry
}

func (self *subprotocolTable) clone() *subprotocolTable {
	entries := make(map[SubprotocolId]*subprotocolEntry, len(self.entries)+1)
	for id, entry := range self.entries {
		entries[id] = entry
	}
	return &subprotocolTable{entries: entries}
}

// Every id with a codec or a raw listener, sorted.
func (self *subprotocolTable) ids() []SubprotocolId {
	ids := make([]SubprotocolId, 0, len(self.entries))
	for id := range self.entries {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i int, j int) bool { return ids[i] < ids[j] })
	return ids
}

// Monotonic counters of one client's subprotocol traffic.
type ClientSubprotocolStatsSnapshot struct {
	Sent                uint64
	SentByteCount       uint64
	Received            uint64
	ReceivedByteCount   uint64
	DroppedUnregistered uint64
	DroppedDecode       uint64
	MarshalOverrun      uint64
	QueriesSent         uint64
	QueriesAnswered     uint64
	QueryReplyDrops     uint64
	// per registered id, messages delivered to its listeners
	ReceivedById map[SubprotocolId]uint64
}

// The per-client registry, pending queries and counters. Registration takes
// stateLock and republishes the table; the receive path only loads it.
type subprotocolRegistry struct {
	stateLock      sync.Mutex
	table          atomic.Pointer[subprotocolTable]
	nextCallbackId uint64
	pendingQueries map[Id]chan []SubprotocolId

	sent                atomic.Uint64
	sentByteCount       atomic.Uint64
	received            atomic.Uint64
	receivedByteCount   atomic.Uint64
	droppedUnregistered atomic.Uint64
	droppedDecode       atomic.Uint64
	marshalOverrun      atomic.Uint64
	queriesSent         atomic.Uint64
	queriesAnswered     atomic.Uint64
	queryReplyDrops     atomic.Uint64
}

func newSubprotocolRegistry() *subprotocolRegistry {
	registry := &subprotocolRegistry{
		pendingQueries: map[Id]chan []SubprotocolId{},
	}
	registry.table.Store(&subprotocolTable{entries: map[SubprotocolId]*subprotocolEntry{}})
	return registry
}

func (self *subprotocolRegistry) snapshot() ClientSubprotocolStatsSnapshot {
	table := self.table.Load()
	byId := make(map[SubprotocolId]uint64, len(table.entries))
	for id, entry := range table.entries {
		byId[id] = entry.receivedCount.Load()
	}
	return ClientSubprotocolStatsSnapshot{
		Sent:                self.sent.Load(),
		SentByteCount:       self.sentByteCount.Load(),
		Received:            self.received.Load(),
		ReceivedByteCount:   self.receivedByteCount.Load(),
		DroppedUnregistered: self.droppedUnregistered.Load(),
		DroppedDecode:       self.droppedDecode.Load(),
		MarshalOverrun:      self.marshalOverrun.Load(),
		QueriesSent:         self.queriesSent.Load(),
		QueriesAnswered:     self.queriesAnswered.Load(),
		QueryReplyDrops:     self.queryReplyDrops.Load(),
		ReceivedById:        byId,
	}
}

func checkSubprotocolId(subprotocolId SubprotocolId, allowReserved bool) error {
	if subprotocolId == 0 {
		return errSubprotocolIdZero
	}
	if !allowReserved && subprotocolId < SubprotocolReservedLimit {
		return errSubprotocolIdReserved
	}
	return nil
}

// Publishes a table with `update` applied to a copy of the id's entry.
func (self *subprotocolRegistry) updateEntry(subprotocolId SubprotocolId, update func(entry *subprotocolEntry) error) error {
	self.stateLock.Lock()
	defer self.stateLock.Unlock()
	table := self.table.Load().clone()
	entry := table.entries[subprotocolId]
	if entry == nil {
		entry = &subprotocolEntry{receivedCount: &atomic.Uint64{}}
	} else {
		entry = entry.clone()
	}
	if err := update(entry); err != nil {
		return err
	}
	if len(entry.rawCallbacks) == 0 && entry.typed == nil {
		delete(table.entries, subprotocolId)
	} else {
		table.entries[subprotocolId] = entry
	}
	self.table.Store(table)
	return nil
}

func (self *subprotocolRegistry) addRawCallback(subprotocolId SubprotocolId, callback SubprotocolRawFunction, allowReserved bool) (func(), error) {
	if err := checkSubprotocolId(subprotocolId, allowReserved); err != nil {
		return nil, err
	}
	if callback == nil {
		return nil, errors.New("nil raw callback")
	}
	var callbackId uint64
	err := self.updateEntry(subprotocolId, func(entry *subprotocolEntry) error {
		self.nextCallbackId += 1
		callbackId = self.nextCallbackId
		entry.rawCallbacks = append(entry.rawCallbacks, subprotocolRawCallback{callbackId: callbackId, callback: callback})
		return nil
	})
	if err != nil {
		return nil, err
	}
	remove := func() {
		_ = self.updateEntry(subprotocolId, func(entry *subprotocolEntry) error {
			kept := entry.rawCallbacks[:0]
			for _, rawCallback := range entry.rawCallbacks {
				if rawCallback.callbackId != callbackId {
					kept = append(kept, rawCallback)
				}
			}
			entry.rawCallbacks = kept
			return nil
		})
	}
	return remove, nil
}

func (self *subprotocolRegistry) registerTyped(subprotocolId SubprotocolId, typed subprotocolTypedDeliverer, allowReserved bool) (func(), error) {
	if err := checkSubprotocolId(subprotocolId, allowReserved); err != nil {
		return nil, err
	}
	err := self.updateEntry(subprotocolId, func(entry *subprotocolEntry) error {
		if entry.typed != nil {
			return errSubprotocolCodecExists
		}
		entry.typed = typed
		return nil
	})
	if err != nil {
		return nil, err
	}
	unregister := func() {
		_ = self.updateEntry(subprotocolId, func(entry *subprotocolEntry) error {
			if entry.typed == typed {
				entry.typed = nil
			}
			return nil
		})
	}
	return unregister, nil
}

// Attaches a raw listener to a subprotocol id. Any number may be attached to
// one id; each receives every message of the id before the typed handler.
// Refuses id 0 and the reserved ids.
func (self *Client) AddSubprotocolRawCallback(subprotocolId SubprotocolId, callback SubprotocolRawFunction) (remove func(), err error) {
	return self.subprotocols.addRawCallback(subprotocolId, callback, false)
}

// The network's own listeners may use reserved ids.
func (self *Client) addReservedSubprotocolRawCallback(subprotocolId SubprotocolId, callback SubprotocolRawFunction) (remove func(), err error) {
	return self.subprotocols.addRawCallback(subprotocolId, callback, true)
}

// Registers the codec and typed handler of a subprotocol id on a client. One
// codec per id; a second registration is an error. Refuses id 0 and the
// reserved ids. The codec may implement SubprotocolMessagePool to own its
// instances. Package-level because Go methods cannot be generic.
func RegisterSubprotocol[T any](client *Client, subprotocolId SubprotocolId, codec SubprotocolCodec[T], handler SubprotocolHandler[T]) (unregister func(), err error) {
	return registerSubprotocol(client, subprotocolId, codec, handler, false)
}

// The network's own subprotocols may use reserved ids.
func registerReservedSubprotocol[T any](client *Client, subprotocolId SubprotocolId, codec SubprotocolCodec[T], handler SubprotocolHandler[T]) (unregister func(), err error) {
	return registerSubprotocol(client, subprotocolId, codec, handler, true)
}

func registerSubprotocol[T any](client *Client, subprotocolId SubprotocolId, codec SubprotocolCodec[T], handler SubprotocolHandler[T], allowReserved bool) (func(), error) {
	if codec == nil {
		return nil, errors.New("nil codec")
	}
	if handler == nil {
		return nil, errors.New("nil handler")
	}
	typed := &subprotocolTyped[T]{
		subprotocolCodec: codec,
		handler:          handler,
		log:              client.log,
		clientTag:        client.clientTag,
	}
	if pool, ok := codec.(SubprotocolMessagePool[T]); ok {
		typed.pool = pool
	} else {
		typed.factory = newSubprotocolMessageFactory[T]()
	}
	return client.subprotocols.registerTyped(subprotocolId, typed, allowReserved)
}

// --- receive ---

// Removes the subprotocol frames from a received batch and delivers them, and
// answers or completes the subprotocol queries, returning the frames the
// generic receive callbacks still get. The batch is compacted in place. A
// batch without subprotocol frames is returned as is after one type check per
// frame.
func (self *Client) dispatchSubprotocolFrames(source TransferPath, frames []*protocol.Frame, peer Peer) []*protocol.Frame {
	var table *subprotocolTable
	kept := frames
	removed := 0
	for i, frame := range frames {
		if frame == nil {
			continue
		}
		switch frame.MessageType {
		case protocol.MessageType_Subprotocol:
			if table == nil {
				table = self.subprotocols.table.Load()
			}
			self.receiveSubprotocolFrame(table, source, frame, peer)
		case protocol.MessageType_TransferSubprotocolsQuery:
			if table == nil {
				table = self.subprotocols.table.Load()
			}
			self.answerSubprotocolsQuery(table, source, frame, peer)
		case protocol.MessageType_TransferSubprotocolsQueryResult:
			self.completeSubprotocolsQuery(frame)
		default:
			if 0 < removed {
				frames[i-removed] = frame
			}
			continue
		}
		removed += 1
	}
	if 0 < removed {
		kept = frames[:len(frames)-removed]
	}
	return kept
}

func (self *Client) receiveSubprotocolFrame(table *subprotocolTable, source TransferPath, frame *protocol.Frame, peer Peer) {
	registry := self.subprotocols
	subprotocolId, payload, ok := decodeSubprotocolHeader(frame.MessageBytes)
	if !ok {
		registry.droppedDecode.Add(1)
		return
	}
	entry := table.entries[subprotocolId]
	if entry == nil {
		registry.droppedUnregistered.Add(1)
		return
	}
	registry.received.Add(1)
	registry.receivedByteCount.Add(uint64(len(payload)))
	entry.receivedCount.Add(1)
	for _, rawCallback := range entry.rawCallbacks {
		self.callSubprotocolRaw(rawCallback.callback, source, subprotocolId, payload, peer)
	}
	if entry.typed != nil {
		if err := entry.typed.deliver(source, payload, peer); err != nil {
			registry.droppedDecode.Add(1)
			if self.log.V(1).Enabled() {
				self.log.Infof("[c]subprotocol %d decode %s<-%s = %s\n", subprotocolId, self.clientTag, source.SourceId, err)
			}
		}
	}
}

// Isolates a raw listener panic like Client.receive isolates a callback panic.
func (self *Client) callSubprotocolRaw(callback SubprotocolRawFunction, source TransferPath, subprotocolId SubprotocolId, payload []byte, peer Peer) {
	defer func() {
		if r := recover(); r != nil {
			if self.log.V(1).Enabled() {
				self.log.Infof("[c]subprotocol %d raw callback %s panic = %v\n", subprotocolId, self.clientTag, r)
			}
		}
	}()
	callback(source, subprotocolId, payload, peer)
}

// Answers a peer's query from the table, the query id threaded back, on the
// provider ping's reply path (SUBPROTOCOL.md §10.4): to the source, zero
// timeout, the source's transfer key with a companion contract unless the
// source is on the same network.
func (self *Client) answerSubprotocolsQuery(table *subprotocolTable, source TransferPath, frame *protocol.Frame, peer Peer) {
	registry := self.subprotocols
	var query protocol.SubprotocolsQuery
	if err := ProtoUnmarshal(frame.MessageBytes, &query); err != nil {
		registry.droppedDecode.Add(1)
		return
	}
	ids := table.ids()
	result := &protocol.SubprotocolsQueryResult{
		QueryId:        query.QueryId,
		SubprotocolIds: make([]uint32, 0, len(ids)),
	}
	for _, id := range ids {
		result.SubprotocolIds = append(result.SubprotocolIds, uint32(id))
	}
	resultFrame, err := ToFrame(result, DefaultProtocolVersion)
	if err != nil {
		registry.queryReplyDrops.Add(1)
		return
	}
	registry.queriesAnswered.Add(1)
	returnProvideMode := protocol.ProvideMode_Stream
	if peer.ProvideMode == protocol.ProvideMode_Network {
		returnProvideMode = protocol.ProvideMode_Network
	}
	returnTransferKey := providerReplyTransferKey(peer.TransferKey, returnProvideMode)
	returnOptions := providerReturnTransferOptions(
		self.settings.DefaultTransferOpts,
		returnProvideMode,
		returnTransferKey,
	)
	if !self.SendWithTimeout(
		resultFrame,
		source.SourceId,
		func(err error) {},
		0,
		returnOptions,
		returnTransferKey,
	) {
		registry.queryReplyDrops.Add(1)
		MessagePoolReturn(resultFrame.MessageBytes)
	}
}

func (self *Client) completeSubprotocolsQuery(frame *protocol.Frame) {
	registry := self.subprotocols
	var result protocol.SubprotocolsQueryResult
	if err := ProtoUnmarshal(frame.MessageBytes, &result); err != nil {
		registry.droppedDecode.Add(1)
		return
	}
	queryId, err := IdFromBytes(result.QueryId)
	if err != nil {
		registry.droppedDecode.Add(1)
		return
	}
	ids := make([]SubprotocolId, 0, len(result.SubprotocolIds))
	for _, id := range result.SubprotocolIds {
		if id == 0 || subprotocolIdMax < id {
			continue
		}
		ids = append(ids, SubprotocolId(id))
	}
	sort.Slice(ids, func(i int, j int) bool { return ids[i] < ids[j] })
	deduplicated := ids[:0]
	for i, id := range ids {
		if i == 0 || ids[i-1] != id {
			deduplicated = append(deduplicated, id)
		}
	}
	registry.stateLock.Lock()
	pending := registry.pendingQueries[queryId]
	registry.stateLock.Unlock()
	if pending == nil {
		return
	}
	select {
	case pending <- deduplicated:
	default:
	}
}

// Asks a peer which subprotocol ids it can receive. Waits for the result or
// ctx; an old peer ignores the query, so a timeout means unknown, not none.
// The control id is not queried.
func (self *Client) QuerySubprotocols(ctx context.Context, destinationId Id, opts ...any) ([]SubprotocolId, error) {
	if destinationId == ControlId {
		return nil, errSubprotocolControl
	}
	registry := self.subprotocols
	queryId := NewId()
	resultChan := make(chan []SubprotocolId, 1)
	registry.stateLock.Lock()
	registry.pendingQueries[queryId] = resultChan
	registry.stateLock.Unlock()
	defer func() {
		registry.stateLock.Lock()
		delete(registry.pendingQueries, queryId)
		registry.stateLock.Unlock()
	}()

	frame, err := ToFrame(&protocol.SubprotocolsQuery{QueryId: queryId.Bytes()}, DefaultProtocolVersion)
	if err != nil {
		return nil, err
	}
	ackChan := make(chan error, 1)
	ackCallback := func(err error) {
		select {
		case ackChan <- err:
		default:
		}
	}
	success, err := self.SendWithTimeoutDetailed(frame, destinationId, ackCallback, -1, opts...)
	if !success {
		MessagePoolReturn(frame.MessageBytes)
		if err == nil {
			err = errors.New("subprotocols query not sent")
		}
		return nil, err
	}
	registry.queriesSent.Add(1)
	for {
		select {
		case ids := <-resultChan:
			return ids, nil
		case err := <-ackChan:
			if err != nil {
				return nil, err
			}
			// acked: the result follows on its own frame
			ackChan = nil
		case <-ctx.Done():
			return nil, errSubprotocolQueryNoResult
		case <-self.ctx.Done():
			return nil, errors.New("Done")
		}
	}
}

// Keeps a subprotocol message's bytes past the listener that received them:
// a pooled copy, released with the returned func. The bytes handed to a
// listener are a sub-slice of the frame's buffer and cannot be shared on
// their own.
func RetainSubprotocolBytes(messageBytes []byte) (retained []byte, release func()) {
	retained = MessagePoolCopy(messageBytes)
	release = func() {
		MessagePoolReturn(retained)
	}
	return retained, release
}

// Monotonic counters of this client's subprotocol traffic.
func (self *Client) SubprotocolStats() ClientSubprotocolStatsSnapshot {
	return self.subprotocols.snapshot()
}

// --- send ---

type subprotocolCodecOption[T any] struct {
	codec SubprotocolCodec[T]
}

// A send option naming the codec for a subprotocol the client has not
// registered, so a client can send a subprotocol it does not receive.
func WithSubprotocolCodec[T any](codec SubprotocolCodec[T]) any {
	return subprotocolCodecOption[T]{codec: codec}
}

// The codec for a send: the option when given, else the registration's, else
// an error; the option is removed from the opts passed on to the send.
func subprotocolSendCodec[T any](client *Client, subprotocolId SubprotocolId, opts []any) (SubprotocolCodec[T], []any, error) {
	var codec SubprotocolCodec[T]
	sendOpts := opts
	for i, opt := range opts {
		if option, ok := opt.(subprotocolCodecOption[T]); ok {
			codec = option.codec
			sendOpts = make([]any, 0, len(opts)-1)
			sendOpts = append(sendOpts, opts[:i]...)
			sendOpts = append(sendOpts, opts[i+1:]...)
			break
		}
	}
	if codec == nil {
		if entry := client.subprotocols.table.Load().entries[subprotocolId]; entry != nil && entry.typed != nil {
			registered, ok := entry.typed.codec().(SubprotocolCodec[T])
			if !ok {
				return nil, nil, errSubprotocolCodecType
			}
			codec = registered
		}
	}
	if codec == nil {
		return nil, nil, errSubprotocolNoCodec
	}
	return codec, sendOpts, nil
}

func (self *Client) buildSubprotocolFrame(subprotocolId SubprotocolId, messageBytes []byte) *protocol.Frame {
	return &protocol.Frame{
		MessageType:  protocol.MessageType_Subprotocol,
		MessageBytes: messageBytes,
	}
}

func encodeSubprotocolFrame[T any](client *Client, codec SubprotocolCodec[T], subprotocolId SubprotocolId, m T) (*protocol.Frame, error) {
	if err := checkSubprotocolId(subprotocolId, true); err != nil {
		return nil, err
	}
	messageBytes, overrun, err := marshalSubprotocol(codec, subprotocolId, m)
	if err != nil {
		return nil, err
	}
	if overrun {
		client.subprotocols.marshalOverrun.Add(1)
	}
	return client.buildSubprotocolFrame(subprotocolId, messageBytes), nil
}

func (self *Client) recordSubprotocolSent(frame *protocol.Frame) {
	self.subprotocols.sent.Add(1)
	self.subprotocols.sentByteCount.Add(uint64(len(frame.MessageBytes)))
}

// Encodes m with the subprotocol's codec into one pool buffer and enqueues it
// as one frame with sender backpressure. The ack callback, transfer options
// and other send options are those of Send.
func SendSubprotocol[T any](client *Client, subprotocolId SubprotocolId, m T, destinationId Id, ackCallback AckFunction, opts ...any) bool {
	success, err := SendSubprotocolWithTimeout(client, subprotocolId, m, destinationId, ackCallback, -1, opts...)
	return success && err == nil
}

// SendSubprotocol with a bounded enqueue wait; returns the enqueue error as
// well. The control id is refused without enqueueing.
func SendSubprotocolWithTimeout[T any](client *Client, subprotocolId SubprotocolId, m T, destinationId Id, ackCallback AckFunction, timeout time.Duration, opts ...any) (bool, error) {
	if destinationId == ControlId {
		return false, errSubprotocolControl
	}
	codec, sendOpts, err := subprotocolSendCodec[T](client, subprotocolId, opts)
	if err != nil {
		return false, err
	}
	frame, err := encodeSubprotocolFrame(client, codec, subprotocolId, m)
	if err != nil {
		return false, err
	}
	success, err := client.SendWithTimeoutDetailed(frame, destinationId, ackCallback, timeout, sendOpts...)
	if !success || err != nil {
		MessagePoolReturn(frame.MessageBytes)
		return false, err
	}
	client.recordSubprotocolSent(frame)
	return true, nil
}

// SendSubprotocol through a nonempty intermediary path.
func SendSubprotocolMultiHop[T any](client *Client, subprotocolId SubprotocolId, m T, destination MultiHopId, ackCallback AckFunction, opts ...any) bool {
	if destination.Len() == 0 || destination.Tail() == ControlId {
		return false
	}
	codec, sendOpts, err := subprotocolSendCodec[T](client, subprotocolId, opts)
	if err != nil {
		return false
	}
	frame, err := encodeSubprotocolFrame(client, codec, subprotocolId, m)
	if err != nil {
		return false
	}
	success, err := client.SendMultiHopWithTimeoutDetailed(frame, destination, ackCallback, -1, sendOpts...)
	if !success || err != nil {
		MessagePoolReturn(frame.MessageBytes)
		return false
	}
	client.recordSubprotocolSent(frame)
	return true
}

// Several messages of one subprotocol as one batch (SendMulti): one pool
// buffer per message, one frames slice, one ack.
func SendSubprotocolMulti[T any](client *Client, subprotocolId SubprotocolId, ms []T, destinationId Id, ackCallback AckFunction, opts ...any) bool {
	if len(ms) == 0 {
		return true
	}
	if destinationId == ControlId {
		return false
	}
	codec, sendOpts, err := subprotocolSendCodec[T](client, subprotocolId, opts)
	if err != nil {
		return false
	}
	frames := make([]*protocol.Frame, 0, len(ms))
	returnFrames := func() {
		for _, frame := range frames {
			MessagePoolReturn(frame.MessageBytes)
		}
	}
	for _, m := range ms {
		frame, err := encodeSubprotocolFrame(client, codec, subprotocolId, m)
		if err != nil {
			returnFrames()
			return false
		}
		frames = append(frames, frame)
	}
	if !client.SendMultiWithTimeout(frames, destinationId, ackCallback, -1, sendOpts...) {
		returnFrames()
		return false
	}
	for _, frame := range frames {
		client.recordSubprotocolSent(frame)
	}
	return true
}

// Sends bytes already encoded by the caller: the wrapper header goes into a
// fresh pool buffer with one copy of the bytes, the same cost as a protobuf
// marshal. Ownership of messageBytes passes here as for any frame. The
// control id is refused.
func (self *Client) SendSubprotocolBytes(subprotocolId SubprotocolId, messageBytes []byte, destinationId Id, ackCallback AckFunction, opts ...any) bool {
	success, _ := self.SendSubprotocolBytesWithTimeout(subprotocolId, messageBytes, destinationId, ackCallback, -1, opts...)
	return success
}

// SendSubprotocolBytes with a bounded enqueue wait and the enqueue error.
func (self *Client) SendSubprotocolBytesWithTimeout(subprotocolId SubprotocolId, messageBytes []byte, destinationId Id, ackCallback AckFunction, timeout time.Duration, opts ...any) (bool, error) {
	defer MessagePoolReturn(messageBytes)
	if destinationId == ControlId {
		return false, errSubprotocolControl
	}
	if err := checkSubprotocolId(subprotocolId, true); err != nil {
		return false, err
	}
	frame := self.buildSubprotocolFrame(subprotocolId, wrapSubprotocolBytes(subprotocolId, messageBytes))
	success, err := self.SendWithTimeoutDetailed(frame, destinationId, ackCallback, timeout, opts...)
	if !success || err != nil {
		MessagePoolReturn(frame.MessageBytes)
		return false, err
	}
	self.recordSubprotocolSent(frame)
	return true, nil
}
