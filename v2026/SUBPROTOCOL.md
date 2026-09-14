# Subprotocols on the core transfer layer

Status: implemented 2026-09-10 in connect 5920959 (protocol: the message and the
peer query), b52bb48 (subprotocol.go: codec, registry, dispatch, sends, query,
stats), 57ea9fb (the Client.receive hook) and df6ac10 (end to end over the
in-process transports). The design was reviewed and approved the same day
(§8, and the review notes at the end of §10); the implementation followed §9. §3–§6 hold
the analysis and the shape as first proposed; where a decision in §8 changes
them, §8 and §10 are authoritative.

## 0. What is being asked for

An external user of the SDK (a Go program importing `connect`, and later the
mobile SDK) wants to run its own protocol over URnetwork's core: reliable,
ordered, contract-accounted, end-to-end encrypted delivery between clients,
without touching `frame.proto` or the transfer layer for each new protocol.
Concretely:

- register a **subprotocol id** (16-bit) on a `Client`, with a **pluggable
  marshaller** that follows the Go protobuf convention (size, marshal-append,
  unmarshal), so the payload can be protobuf, a hand-rolled codec, or anything
  else the user owns;
- send and receive messages of that subprotocol through the existing send and
  receive machinery, with the wire carrying a small subprotocol header and the
  raw payload bytes;
- keep the hot path as cheap as the core's own frames: every byte written into
  a `message_pool` buffer once, no second copy between the marshaller and the
  frame, no per-message allocations beyond what a core frame already costs.

## 1. How a message travels today

The facts the design leans on, with the code that establishes them.

**One frame type, one enum.** Every application message is a
`protocol.Frame{message_type, message_bytes, raw}` (`protocol/frame.proto`).
`MessageType` is one flat enum ("flatten all message types into this enum"),
currently 0..29. `raw = true` means `message_bytes` is the message itself, not a
protobuf; the IP data path uses it so a packet is carried without the
`IpPacketToProvider{IpPacket{packet_bytes}}` wrappers (`frame.go`
`ipPacketToProviderFrame`, `FromFrame` raw branches). Older peers ignore an
unknown `MessageType` (the `TransferResidentMigrate` and
`IpIpProviderDiagnostics` comments rely on this).

**Frames ride in Packs.** `Client.Send*` takes a `*protocol.Frame` and a
destination; the sequence layer batches frames into a `Pack` (at most
`sendPackBatchMaxFrames = 2` data frames per pack on the current sender),
encodes the `TransferFrame` with the hand-rolled codec in `frame_protobuf.go`
(`sendPackFrame.appendPack` → `appendFrame`, which does one
`append(b, f.MessageBytes...)` into the pooled pack buffer), then the pack is
acked, resent, and optionally wrapped by the per-peer encryption session.
Intermediaries route on the `TransferPath` only; "only the destination inspects
the payload" (DESIGNNOTES §1). Multi-hop (`SendMultiHop`) works the same way.

**Ownership is explicit and pooled.** `message_pool.go` states the three rules:
an owner returns a buffer with `MessagePoolReturn`; ownership is handed off on
send (`Send*` "takes ownership of the frames' message bytes AND of the `frames`
slice itself", `SendMultiWithTimeout` header; the sequence returns them after
marshalling, `transfer.go` 2079/2258/2951); and received bytes are borrowed for
the duration of the receive callback (`ReceiveFunction` doc: "borrowed and valid
only until the callback returns; share or copy data that must outlive it").
`ProtoMarshalWithTag` is the model for marshalling into the pool:
`MessagePoolGet(proto.Size(m))`, `MarshalAppend(buf[:0], m)`, and a cap check
that returns the pool buffer if the marshaller grew past it. Pool classes are
256 B, 2048 B and two large classes; a slice keeps its pool identity only while
its `cap` is the class size plus the 12-byte meta, so a sub-slice
(`b[2:]`) is no longer a pool buffer.

**Receive is inline and allocation-bounded.** Inbound packs decode into a
`decodedPackOwner` (inline `Frame` storage for the common two-frame pack);
`decodeFrameInto` copies `message_bytes` once from the transport buffer into a
pool buffer (`f.MessageBytes = MessagePoolCopy(v)`). `ReceiveSequence.flushDeliver`
hands the batch's frames to `Client.receive`, which fans out to every
registered `ReceiveFunction` (a `CallbackList`), then acks and returns the pool
buffers. Before delivery the sequence already **intercepts one frame type**:
`deliverEncryptedControlFrames` pulls `TransferEncryptedControl` frames out of
the batch and routes them into the encryption session, passing the rest to the
application (`transfer.go` ~10328). That is the precedent for a typed dispatch
ahead of the generic callbacks.

**Consumers switch on the type.** The IP layer's receive callback switches on
`frame.MessageType` and reads the raw bytes with `ipPacketToProviderBytes`
(`ip.go` ~7778); the SDK's provider registers a receive callback the same way
(`sdk/device_local_provider.go:213`). Every consumer of a frame it does not own
ignores the type; nothing today errors on an unknown type in the receive path,
only `FromFrame` does when asked to decode one.

**Size bounds.** A frame must fit one transport message: each transport's
framer `MaxMessageLen`, whose floor is `ClientSettings.MinimumMessageLenLimit()`
= 8 KiB (`transfer.go` 1318), minus the pack and encryption overhead. There is
no fragmentation at the frame layer.

## 2. Design goals and non-goals

Goals:

1. A subprotocol is data to the core: one new `MessageType`, one new `Frame`
   field, no per-subprotocol code in `connect`.
2. Zero extra copies: the marshaller writes straight into the pool buffer that
   becomes `Frame.message_bytes`; the pack encoder's single append into the
   wire buffer is the only copy on send (as for every frame today); on receive
   the codec unmarshals from the pooled `message_bytes` the decoder already
   produced, in place.
3. Zero extra allocations on the hot path beyond the `Frame` struct the core
   already allocates per send; on receive, none (the decoded frame lives in
   the pack owner's inline storage).
4. Registration per `Client`, lock-free on the receive path.
5. Wire compatibility: an old peer or an old platform drops the frames and
   keeps the sequence healthy; the hand-rolled codec and `proto.Unmarshal` both
   decode the new field or skip it.

Non-goals (v1): fragmentation of messages larger than a pack, a
subprotocol-level handshake or version negotiation (that is the subprotocol's
own business, see §8 Q7), mobile SDK bindings (§8 Q9).

## 3. Wire format

Three ways to put a 16-bit id on the wire were considered.

| | A. header inside `message_bytes` | B. nested `SubprotocolFrame` message | C. new `Frame` field |
|---|---|---|---|
| shape | `raw=true`, `message_bytes = [id:2][payload]` | `raw=false`, `message_bytes = proto{id, payload}` | `message_type=Subprotocol, subprotocol_id=id, raw=true, message_bytes=payload` |
| extra copy on send | none if the marshaller appends after the 2 header bytes | one, unless hand-rolled | none |
| payload bytes on receive | `message_bytes[2:]`: a sub-slice, no longer a pool identity | `message_bytes` inner slice | `message_bytes` exactly |
| decodable by generic protobuf tools | id is opaque | yes | yes (a Frame field) |
| header cost | 2 B | ~5 B | 2–4 B (tag + varint) |
| codec change in `frame_protobuf.go` | none | none | one field in `sizeFrame`/`appendFrame`/`decodeFrameInto` |

**Recommendation: C.** The id becomes a first-class field of the frame:

```proto
enum MessageType {
    ...
    IpIpProviderDiagnostics = 29;
    // A message of a subprotocol registered on the receiving Client
    // (SUBPROTOCOL.md). `Frame.subprotocol_id` names the subprotocol and
    // `message_bytes` is the subprotocol's own encoding (`raw` is set). Older
    // clients ignore the unknown message type.
    Subprotocol = 30;
}

message Frame {
    MessageType message_type = 1;
    bytes message_bytes = 2;
    bool raw = 3;
    // set only for `Subprotocol` frames; 1..65535, 0 is invalid
    uint32 subprotocol_id = 4;
}
```

Why C over A: with A the payload the codec sees is `message_bytes[2:]`, which
has lost its pool identity (`cap` no longer matches a class), so every helper
that shares or returns it silently becomes a no-op and the codec cannot hand
the slice on; with C `message_bytes` is the payload, so the existing ownership
rules apply to it unchanged. Why C over B: B is a second length-prefixed
message inside the frame, which either costs a copy (marshal the payload, then
marshal the wrapper) or a second hand-rolled codec; C reuses the frame codec
that already exists and adds one varint. The hand-rolled codec skips unknown
fields (`decodeFrameInto` default branch), `proto.Unmarshal` does too, and the
platform never decodes payloads, so the field is safe to add without a
protocol version gate.

`subprotocol_id` is `uint32` on the wire (protobuf has no 16-bit scalar); the
Go API is `SubprotocolId uint16` and the codec rejects values outside 1..65535.

## 4. The marshaller ("go protobuf convention")

The convention we match is `proto.Size` + `proto.MarshalOptions.MarshalAppend`
+ `proto.Unmarshal`: size first, so the pool buffer is taken once at the right
class; append into a caller-provided buffer, so the bytes land where the frame
needs them; unmarshal from a borrowed slice. Two shapes are possible.

**Per-message interface** (each message type implements it):

```go
type SubprotocolMessage interface {
    Size() int
    MarshalAppend(b []byte) ([]byte, error)
    Unmarshal(b []byte) error
}
```

**Per-subprotocol codec** (one object per registered id, messages stay plain
values):

```go
// Codec is the pluggable marshaller of one subprotocol. It is called from the
// client's send path and, for Unmarshal, inline on the receive goroutine; it
// must be safe for concurrent use.
type SubprotocolCodec[T any] interface {
    // the exact encoded size of m; the send path takes one pool buffer of this size
    Size(m T) int
    // appends the encoding of m to b (len(b) == 0, cap(b) >= Size(m)) and returns it
    MarshalAppend(b []byte, m T) ([]byte, error)
    // decodes b into m; b is borrowed (see §5) and must not be retained
    Unmarshal(b []byte, m T) error
}
```

**Recommendation: the codec, generic over the message type.** A proto-generated
Go message does not carry `Size`/`MarshalAppend` methods (protoc-gen-go emits
neither; `proto.Size(m)` and `MarshalAppend` are package functions), so the
per-message interface would force a wrapper type on every protobuf user
anyway. The codec shape lets `connect` ship one adapter for any
`proto.Message`:

```go
// ProtoCodec adapts a protobuf message type to SubprotocolCodec using
// proto.Size, MarshalOptions.MarshalAppend and UnmarshalOptions.Unmarshal.
func ProtoCodec[T proto.Message]() SubprotocolCodec[T]
```

and lets a user with a hand-rolled or flatbuffer encoding implement three
methods once. A codec may also own a bounded free list of `T` instances (the
`decodedPackOwner` pattern: sharded, capacity-bound, never `sync.Pool`), which
the per-message interface cannot express.

Marshal path (the `ProtoMarshalWithTag` recipe, applied to any codec):

```go
n := codec.Size(m)
buf := MessagePoolGet(n)                       // one pool buffer, the class for n
out, err := codec.MarshalAppend(buf[:0], m)    // writes in place
if cap(out) != cap(buf) { MessagePoolReturn(buf) }   // codec overran Size: the pool buffer is orphaned
frame := &protocol.Frame{MessageType: Subprotocol, SubprotocolId: uint32(id), MessageBytes: out, Raw: true}
```

A codec whose `Size` underestimates still works (the append grows into a heap
slice) but loses the pool; the readout in §7 counts those so a bad codec is
visible rather than silently slow.

## 5. Receive: dispatch, lifetime, typed delivery

Dispatch happens in `Client.receive`, before the generic `ReceiveFunction`
fan-out, mirroring `deliverEncryptedControlFrames`:

```go
func (self *Client) receive(source TransferPath, frames []*protocol.Frame, peer Peer) {
    frames = self.subprotocols.dispatch(self, source, frames, peer)   // strips Subprotocol frames
    if len(frames) == 0 { return }
    for _, receiveCallback := range self.receiveCallbacks.Get() { ... }   // unchanged
}
```

`dispatch` walks the batch once; for each `MessageType_Subprotocol` frame it
looks the id up in an immutable table published through an `atomic.Pointer`
(registration copies the table; the receive path takes no lock), and calls the
handler. The remaining frames are compacted in place (no new slice) and
delivered as today. A batch with no subprotocol frames costs one type check
per frame.

Handler shape, typed through the codec:

```go
// A handler runs inline on the receive goroutine and must not block; `message`
// and any bytes it aliases are borrowed until the handler returns (the same
// contract as ReceiveFunction). `peer` carries provide mode, roles, principal
// and the TransferKey to reply on.
type SubprotocolHandler[T any] = func(source TransferPath, message T, peer Peer)
```

Per frame the dispatcher does: take a `T` from the codec's free list (or
`new(T)` when the codec has none), `codec.Unmarshal(frame.MessageBytes, m)`,
call the handler, release `m`. `frame.MessageBytes` is the pool buffer the
frame decoder already produced, owned by the receive item and returned by
`flushDeliver` after the callback: the codec reads it in place, and a
protobuf codec's `bytes`/`string` fields alias it only if unmarshalled with
aliasing options, which is why the handler contract says borrowed. A handler
that must keep the message calls `RetainSubprotocolBytes(messageBytes)`, which is
`MessagePoolShareReadOnly` plus a `MessagePoolReturn` obligation, or the codec
copies what it keeps. Decode failures are counted per id and the frame is
dropped, never delivered raw.

Unregistered ids are dropped and counted (§8 Q4). Subprotocol frames never
reach the generic `ReceiveFunction`s: every existing callback switches on the
types it owns, and a raw frame with an id nobody registered has no consumer.

Because the handler runs inside the receive sequence's delivery, the whole
existing backpressure story applies: a slow handler slows the sequence, exactly
as a slow `ReceiveFunction` does today. A subprotocol that needs a queue builds
one on top with the zero-timeout, drop-when-full rule from the `ReceiveFunction`
doc; `connect` does not add one.

## 6. Client API

```go
type SubprotocolId uint16

// Registers codec and handler for id on this client. Registration is rare and
// takes the registry lock; the receive path reads an immutable snapshot. An id
// already registered is an error; the returned func unregisters.
func RegisterSubprotocol[T any](client *Client, id SubprotocolId, codec SubprotocolCodec[T],
    handler SubprotocolHandler[T], opts ...SubprotocolOption) (unregister func(), err error)

// Marshals m with the registered codec into one pool buffer and enqueues it as
// one frame; the same destination, ack callback, timeout and send options as
// Send/SendWithTimeout/SendMultiHop. Ownership of the bytes passes to the send
// on success; on failure the buffer is returned here.
func SendSubprotocol[T any](client *Client, id SubprotocolId, m T, destinationId Id,
    ackCallback AckFunction, opts ...any) bool
func SendSubprotocolWithTimeout[T any](...)
func SendSubprotocolMultiHop[T any](...)

// Several messages of one subprotocol in one pack (SendMulti): one pool buffer
// per message, one frames slice.
func SendSubprotocolMulti[T any](client *Client, id SubprotocolId, ms []T, destinationId Id,
    ackCallback AckFunction, opts ...any) bool
```

Package-level generic functions rather than methods because Go methods cannot
be generic; they are thin wrappers over `client.Send*` and the registry, so the
`Client` type gains only the registry field and `receive`'s first line. The
send path does not require the id to be registered locally (a client may only
send a subprotocol), but it does require a codec: `SendSubprotocol` takes it
from the registration when present and from an explicit `WithCodec(codec)`
option otherwise (§8 Q2).

Options at registration (`SubprotocolOption`): `AcceptProvideModes(...)` to
drop frames from peers outside a provide mode set before the handler runs
(the `Peer` already carries it; this only saves the unmarshal), and
`MaxMessageByteCount(n)` to reject oversized frames before decoding.

What does not change: contracts and accounting (`MessageByteCount` is
`len(MessageBytes)` as for any frame), encryption (frames inside packs are
wrapped as a unit), acks (`AckFunction` remains the pack-level ack), routing
(the path is untouched), the platform (it forwards `TransferFrame`s without
reading payloads; its own `Client` instances drop the unknown type).

## 7. Cost accounting

Send, per message, today for a protobuf control frame built with `ToFrame`:
the message struct, `proto.Size` + reflection marshal into one pool buffer,
one `Frame` struct, one `SendPack`, then one append into the pack wire buffer
(the frame codec) and the pool buffer is returned. With the codec path: the
`Frame` and `SendPack` structs are unchanged, the marshal is the codec's own
(no reflection for a hand-rolled codec, the same reflection for `ProtoCodec`),
the pool buffer is taken once at `Size(m)`, and the append into the pack is the
same single copy every frame pays. Net: zero added copies, zero added
allocations; on the wire, 2–4 bytes for field 4.

Receive, per frame, today: the frame decoder's one `MessagePoolCopy` from the
transport buffer into a pool buffer, the inline `Frame` in the pack owner, and
whatever the consumer does. With dispatch: one table lookup, one codec
`Unmarshal` in place, one `T` from the codec's free list. Net: zero added
copies, zero added allocations when the codec pools its messages.

Verification is part of the implementation: `testing.AllocsPerRun` on the send
and receive paths for a hand-rolled codec must report the same counts as an IP
raw frame, and a benchmark against `ToFrame(SimpleMessage)` sets the reference
for `ProtoCodec`. A per-client stat block (`SubprotocolStats`: sent, received,
dropped-unregistered, dropped-decode, dropped-oversized, marshal-overrun) is
exposed next to `ClientReceiveStatsSnapshot` so a misbehaving codec shows up in
numbers.

## 8. Decisions (2026-09-10)

1. **Wire.** A new message type in `connect/protocol`: `MessageType.Subprotocol`
   carrying a protobuf `Subprotocol{subprotocol_id, message_bytes}` whose
   `message_bytes` are the subprotocol's raw bytes, decoded in place by the
   subprotocol's codec. This is shape B of §3, hand-rolled so it costs no
   second copy (§10.1). `Frame.raw` stays false: the frame is a protobuf.
2. **Any subprotocol may be sent.** A send needs a codec (from the local
   registration or passed with the send); it does not need the id registered.
   A received message whose id has no codec is dropped unless a raw listener is
   attached for it.
3. **Raw listeners.** A new raw receive callback delivers the subprotocol id and
   the raw bytes. Dispatch order per message: raw listeners first, with the id,
   then the typed handler if a codec is registered and the bytes parse. A client
   can therefore observe both the raw bytes and the parsed message.
4. **Unregistered ids** reach raw listeners only.
5. **Reserved ids.** Ids below 1024 are reserved for the network, like
   privileged ports; the public registration refuses them. 0 is invalid.
6. **The control id speaks only the top-level protocol.** A subprotocol send
   to `ControlId` is refused by the client; the platform drops any that arrive.
7. **Frame size.** The top-level frame rules apply unchanged; a subprotocol adds
   no size rule of its own and no fragmentation.
8. **Peer query.** `connect/protocol` gains a peer-to-peer query of the
   subprotocols a client supports (registered codecs and raw listeners), in
   the spirit of the provider ping but with a reply (§10.4).
9. **SDK.** The mobile SDK exposes the raw listener: an application enables
   subprotocols by id and receives the bytes through the listener, managing its
   own codec in application code. Users who want typed, allocation-free
   handling build their own SDK linked against `connect` with their codec and
   view controllers.
10. **Delivery** of raw bytes and parsed messages is inline on the receive
    goroutine, like every frame today.

## 9. Implementation plan

Files in `connect`:

1. `protocol/subprotocol.proto` (new): the three messages of §10; `frame.proto`:
   `Subprotocol = 30`, `TransferSubprotocolsQuery = 31`,
   `TransferSubprotocolsQueryResult = 32`; regenerate with `protocol/Makefile`.
2. `frame.go`: `ToFrame`/`FromFrame` cases for the three messages (the generic,
   reflection path; the hot path never uses it for `Subprotocol`).
3. `subprotocol.go` (new): the hand-rolled `Subprotocol` encoder and decoder,
   the codec interface and `ProtoCodec`, the registry, the dispatch, the raw
   and typed listeners, the query, the stats, the reserved-id and control-id
   rules. `subprotocol_test.go`: encoder byte-identity against `proto.Marshal`,
   decoder against `proto.Unmarshal` (including unknown fields and duplicate
   fields), dispatch order, drop counting, reserved ids, the control-id refusal,
   the query round trip, and `testing.AllocsPerRun` on send and receive.
4. `transfer.go`: the registry on `Client`, one line in `receive`, the query
   interception, stats exposure.
5. An end-to-end test over the in-process test transport (the `SimpleMessage`
   pattern in `transfer_test.go`) with a hand-rolled codec, a `ProtoCodec`, a
   raw listener alongside a typed handler, and an unregistered id.
6. DESIGNNOTES §1 gains the layer line; this file drops "proposal".

Then the SDK (`sdk` repository): `DeviceLocal.EnableSubprotocol(id, listener)`
/ `DisableSubprotocol(id)` on the gomobile surface with a listener interface
that receives the id, the source client id and the bytes (copied out of the
pool for the binding boundary), and `QuerySubprotocols` for the peer query.

## 10. Final design

### 10.1 Wire

```proto
// protocol/subprotocol.proto
message Subprotocol {
    // 1..65535; ids below 1024 are reserved for the network
    uint32 subprotocol_id = 1;
    // the subprotocol's own encoding, decoded in place by its codec
    bytes message_bytes = 2;
}

message SubprotocolsQuery {
    // ulid, threaded back on the result
    bytes query_id = 1;
}

message SubprotocolsQueryResult {
    bytes query_id = 1;
    // every id the sender can receive: registered codecs and raw listeners
    repeated uint32 subprotocol_ids = 2;
}
```

Encoding on send is hand-rolled into one pool buffer, in field order:
`tag(1) varint(id) tag(2) varint(n) payload`, where `n = codec.Size(m)` is
known before the payload is written, so the header is emitted first and the
codec appends the payload after it. The buffer is `MessagePoolGet(header + n)`;
`Frame.message_bytes` is that buffer, ownership passes to the send. If the
codec overruns `Size` the append grows into a heap slice and the pool buffer is
returned; the overrun is counted. The output is byte-identical to
`proto.Marshal(&protocol.Subprotocol{...})` (tested), so any protobuf tool
decodes it.

Decoding on receive is hand-rolled with `protowire` over the frame's pooled
`message_bytes`: the id is read, the payload is the sub-slice of field 2, no
copy. Unknown fields are skipped; a repeated singular field takes the last
value, as `proto.Unmarshal` does; a malformed message is dropped and counted.
The payload sub-slice has no pool identity of its own; it is borrowed for the
callback like every received frame, and a listener that must keep it calls
`RetainSubprotocolBytes(messageBytes)`, which copies it into a pool buffer of
its own and returns that copy with its release func (a sub-slice has no pool
identity to share), or copies it itself.

### 10.2 Registration and listeners

```go
type SubprotocolId uint16
const SubprotocolReservedLimit SubprotocolId = 1024   // ids below are the network's

type SubprotocolCodec[T any] interface {
    Size(m T) int
    MarshalAppend(b []byte, m T) ([]byte, error)
    Unmarshal(b []byte, m T) error
}
func ProtoCodec[T proto.Message]() SubprotocolCodec[T]

// raw bytes, before any codec; runs for every listener attached to the id
type SubprotocolRawFunction = func(source TransferPath, subprotocolId SubprotocolId, messageBytes []byte, peer Peer)
// the parsed message, after the raw listeners; one codec and one handler per id
type SubprotocolHandler[T any] = func(source TransferPath, message T, peer Peer)

func (self *Client) AddSubprotocolRawCallback(subprotocolId SubprotocolId, callback SubprotocolRawFunction) (remove func(), err error)
func RegisterSubprotocol[T any](client *Client, id SubprotocolId, codec SubprotocolCodec[T], handler SubprotocolHandler[T]) (unregister func(), err error)
```

Both refuse ids below `SubprotocolReservedLimit` (an unexported variant exists
for the network's own subprotocols) and id 0. `RegisterSubprotocol` refuses an
id that already has a codec; raw listeners may be attached in any number. The
registry is an immutable table behind an `atomic.Pointer`, copied on change.

Dispatch, in `Client.receive` before the generic callbacks, per `Subprotocol`
frame: decode the header; if the id has no raw listener and no codec, drop and
count; call each raw listener with the payload; if a codec is registered, take
a message from the codec (a `SubprotocolCodec` may also implement
`SubprotocolMessagePool[T]{New() T; Release(T)}` to bound its allocations),
`Unmarshal` in place, call the handler, release. A decode failure after the raw
listeners ran is counted and the handler is not called. `Subprotocol` frames are
removed from the batch before the generic `ReceiveFunction`s see it.

### 10.3 Sending

```go
func SendSubprotocol[T any](client *Client, id SubprotocolId, m T, destinationId Id, ackCallback AckFunction, opts ...any) bool
func SendSubprotocolWithTimeout[T any](..., timeout time.Duration, opts ...any) (bool, error)
func SendSubprotocolMultiHop[T any](client *Client, id SubprotocolId, m T, destination MultiHopId, ackCallback AckFunction, opts ...any) bool
func SendSubprotocolMulti[T any](client *Client, id SubprotocolId, ms []T, destinationId Id, ackCallback AckFunction, opts ...any) bool
func (self *Client) SendSubprotocolBytes(id SubprotocolId, messageBytes []byte, destinationId Id, ackCallback AckFunction, opts ...any) bool
```

The codec comes from the registration or from a `WithSubprotocolCodec(codec)`
send option; a send with neither fails. `SendSubprotocolBytes` takes already
encoded bytes (ownership of the slice passes as for any frame; the header is
written into a fresh pool buffer and the bytes copied once, the same single
copy the pack encoder would make). A destination of `ControlId` fails without
enqueueing. Everything else (acks, timeouts, transfer options, multi-hop,
contracts, encryption) is the existing `Send*` machinery; size limits are the
transport framer's.

### 10.4 Peer query

`client.QuerySubprotocols(ctx, destinationId Id, opts ...any) ([]SubprotocolId, error)`
sends a `SubprotocolsQuery` with a fresh ulid, registers the id in a pending
table, and waits for the matching `SubprotocolsQueryResult` or `ctx`. The
receiving client answers a query inline from its registry (codec ids and
raw-listener ids, sorted, deduplicated) with the query id threaded back. The
reply follows the provider ping echo (`ip.go`, `MessageType_IpIpPing`): it is
sent to `source.SourceId` with zero timeout, its transfer key from
`providerReplyTransferKey(peer.TransferKey, returnProvideMode)` and its options
from `providerReturnTransferOptions(defaultOpts, returnProvideMode,
returnTransferKey)`, where `returnProvideMode` is network for a same-network
source and stream otherwise, so the reply rides a companion contract exactly
as the ping echo does; a failed enqueue returns the reply's pool buffer and is
counted. Both messages are ordinary top-level frames intercepted in
`Client.receive` and never delivered to application callbacks. An old peer ignores the query, so a
query against it times out: absence of a result means "unknown", not "none".
The control id is not queried.

### 10.5 Stats

`ClientSubprotocolStatsSnapshot{Sent, SentBytes, Received, ReceivedBytes,
DroppedUnregistered, DroppedDecode, MarshalOverrun, QueriesAnswered}` plus a
per-id received count, monotonic for the client's lifetime, exposed next to
`ReceiveStats`.

### 10.6 Review notes (2026-09-10)

Confirmed on review: raw listeners attach per id (no wildcard); exactly one
codec per id, because a subprotocol needs a deterministic codec or it is raw
bytes; the query reply uses the provider ping's companion-reply behaviour
(§10.4); `SendSubprotocolBytes` may copy once, the same cost as a protobuf
marshal; the enum names of §9; `FromFrame` returns the wrapper for tooling only.
