# Go Style Guide

In the URnetwork code, the following Go style is used. A few conventions — notably the `self` receiver name and the verbose usage+type field naming — intentionally diverge from idiomatic Go; they are deliberate house rules, not oversights.

## Naming

- Treat acronyms and initialisms as ordinary words in Go identifiers: capitalize only the first letter of each acronym, not every letter. Write `EvmTxManager`, `Tcp`, `Rpc`, `Http`, `Api`, `Json`, `Id`, and `Uid`, not `EVMTxManager`, `TCP`, `RPC`, `HTTP`, `API`, `JSON`, `ID`, or `UID`. At the start of an unexported identifier, lowercase the whole acronym: `evmTxManager`, `tcpConnection`, `rpcClient`, `clientId`, and `validatorUid`. Apply this consistently to types, functions, methods, fields, variables, constants, and test names in code we write. This deliberately differs from standard Go initialism conventions.
- Preserve identifiers required by external packages, generated bindings, or interface contracts, and preserve serialized field names and protocol formats unless changing that contract is explicitly part of the task. Rename an owned identifier and all its references together; a naming-only change must not alter behavior or wire compatibility.
- Our receiver name is `self`: `func (self *T) f(...)` (pointer receiver) or `func (self T) f(...)` (value receiver).
- Our canonical name for a `sync.Mutex` guarding state is `stateLock`.
- Field and variable names are slightly more verbose than standard Go, so usage and type can be inferred from the name. The scheme is usage + type:
  - Maps: `map[K]V` is `usageKVs` — e.g. `activeNetworkIdUsers` for a `map[Id]User` of active users. Note the plural `s`.
  - Slices: `[]T` is `usageTs` — e.g. `activeNetworkIds` for a `[]Id` of active networks. Note the plural `s`.
  - Variables: `T` is `usageT` — e.g. `activeNetworkId` for the active network `Id`.
  - Use the shortest type token that stays unambiguous: `Id`, not `Identifier`; but never abbreviate past recognition (`activeNetworkIds`, not `activeNetIds`).
  - When the value already implies its type, drop the type token: concrete objects are named by usage alone (`session`, `client`, `transferBalance`). Keep the type when dropping it would confuse an identifier with the object it names — an `Id` stays `clientId`, never `client`.
  - Short names like `t`, `s`, `ts` are fine in a small scope with few locals, where the type is obvious from nearby context.

## Identifiers

- Our `Id` type is a ULID. ULIDs put a millisecond creation timestamp in their high bits, so ids are **lexicographically ordered by creation time** — and we rely on this in several places: `MAX(id)` / `MAX(id::varchar)` selects the most-recently-created row of a group, and `ORDER BY id` stands in for `ORDER BY create_time`. The ordering survives the `uuid` column type and its `::varchar` canonical form (the timestamp is in the leading bytes, and hex sorts in value order), so it holds in SQL as well as in Go.
- The timestamp is **client-local**: an id carries the clock of whatever minted it, so id order is a dependable time order within a single source but is not a global clock across sources (two ids minted on different machines can be out of true order by the clock skew between them). When strict or cross-source ordering matters, order by an explicit server-set timestamp column instead.

## Package layering

**A package must never import its own subpackages.** Dependencies point one way:
a subpackage may import its parent or a peer, never a child. This is the same rule
the Go standard library follows.

The reason is that a parent importing a child inverts the dependency: the child is
supposed to be a detail of the parent, but the compiler now treats the parent as
depending on it, so the child's API is frozen by its own parent, cycles become easy
to create, and "what depends on what" stops being readable from the directory tree.

If a parent and a child need to share code, that code belongs in the **parent**, not
in a child the parent then imports. Do not reach for `internal/` to get around this
— `internal/` controls *visibility*, not *direction*, and a parent importing
`internal/x` is still a parent importing a child.

Shared code that several files in one package need is just a file in that package.
Name it for what it is, and prefix the files of a cohesive group so the grouping
survives the flattening:

```
proxy/
  relay.go            <- the shared bidirectional copy, used by http and socks
  http.go
  socks.go            <- the public SocksProxy
  socks5_server.go    <- the socks5 protocol implementation, one package,
  socks5_addr.go         grouped by filename prefix rather than by directory
  socks5_associate.go
  socks/main.go       <- a CHILD (package main) importing its parent: allowed
```

An identifier that no longer crosses a package boundary should be unexported. A
name is exported to cross a boundary, not as decoration; collapsing a subpackage
into its parent is a good moment to shrink the public surface back to what callers
actually use.

## Comments

- Prefer short inline comments (`// ...` inside a function), 1–2 lines per point unless more is truly needed.
- Put a doc comment at the top of each file, type, and function/method declaration, summarizing the architecture and edge cases. These can be as long as needed, but aim to be concise. Push information that applies throughout a type or file up to the type or file header instead of repeating it — in particular, concurrent goroutine safety.
- Do not repeat the field or function name in its comment — the name is already on the declaration, so repeating it is redundant. Keep comments short and terse but effective. (This intentionally diverges from Go's start-with-the-name doc convention.)

## Bug fixes

- When a bug is found, look for adjacent and similar issues before considering the fix complete. A bug represents a flaw in the reasoning that produced it, and the same flaw may be applied elsewhere — check the surrounding code, sibling call sites, and other copies of the same pattern.

## Routing keys

A routing key is a value that decides *which* instance of something a message belongs to: the sequence lane (`Pack.force_stream` / `Pack.companion_contract`), the session role and identity companion (`TransferFrame.session_role` / `session_companion`), the handshake generation (`EncryptedControl.epoch_id`). Adding one is never a single-site change.

- **Peer messages are path-independent for correctness.** `TransferKey` selects the logical send sequence and its lane/session; it does not select a transport path. The sequence's destination-keyed multi-route writer may use any live route to that peer, including exchange, direct P2P, or a multihop stream. Route knowledge may influence that choice for performance, but correctness must not depend on which route wins. In particular, do not add a transport-local stream id to `TransferKey`, the send-sequence identity, or the serialized transfer path.
- **Source identity and transfer identity stay separate.** A receive callback gets the authenticated source as its `TransferPath` argument and the receiver-visible lane/session as `peer.TransferKey`. `TransferKey` must not repeat the source id already carried by the callback path. A reply normally sends to `source.SourceId` and passes `peer.TransferKey` as a send option so the peer's logical lane is reproduced without threading a transport path through the application.
- **A selectable route must be usable end to end.** One connected local adjacency does not make a multihop stream live. Keep its transport alias unavailable while the stream is establishing, publish it only after end-to-end readiness succeeds, and withdraw it when the stream disconnects or loses readiness. An incomplete route is establishment state, not a slower route that ordinary traffic may try.
- **Every delivery path must carry the key.** A message usually reaches its consumer by more than one route — the in-order drain and an optimistic fast path, a per-item call and a batch call, a live path and a replay/resend path. One path that omits the key silently reintroduces the entire class of bug the key was added to fix, and it surfaces only in the narrow race where that path wins. Adding a key is therefore: define it, stamp it at every producer, and grep for **every** consumer entry point before calling the change done.
- **Sender-local state that selects a route must be visible to the receiver**, or the receiver cannot distinguish two live things and will collapse them into one — dropping whichever it decides is "older". If a key must stay off the wire, the sender is responsible for never running two things that differ only by it.
- **Prefer routing a mismatch over judging it.** A message from another instance is evidence about that instance, not about this one. Judging it here produces a wrong verdict on state it does not describe (a proof for another generation fails signature verification and looks like an attack), and the wrong verdict is usually sticky. Route it, ignore it, or converge onto it.
- **Order the key when the instances are ordered.** `Id` is a ULID, so generations compare directly: older is a straggler to drop, newer is a signal to converge. Without an ordering the receiver can only tell "different", which is not enough to decide who should move.
- **A key with a single-slot consumer needs the key checked before the slot is taken.** Pending/first-wins state (one buffered proof per epoch, one head per sequence) is a resource a stale message can consume; validate the key first, or a straggler denies the slot to the message that belongs there.

## Receive callbacks and reliable-carrier backpressure

Send and receive sit on opposite sides of the backpressure contract. **Senders block**: a packet/frame send is allowed to wait (buffer full, contract wait, write timeout) because blocking the producer is how backpressure propagates toward the source. **Shared receive callbacks buffer and refuse**: a callback serving unrelated flows must never block, because stalling it stalls everyone behind the stall. A dedicated socket reader is different when it has read one complete message from an exactly identified reliable physical lane: retaining that one message until fixed downstream capacity or cancellation propagates backpressure into TCP/QUIC/SCTP. Dropping it instead manufactures loss above the reliable carrier and can pin every later Transfer Pack behind an artificial sequence gap.

- **A shared receive callback that hands off must use a 0 timeout on the handoff.** Enqueue non-blocking; if the queue is full, refuse (and count the refusal). Never propagate one destination's downstream wait through a callback that serves unrelated work.
- **Classify the exact physical receive lane, not the connection family.** Hybrid H3 publishes separate reliable-stream and DATAGRAM routes; production P2P publishes separate SCTP and native-datagram routes. H1, QUIC stream, SCTP, and framed internal TCP readers may retain only their one already-read complete message while waiting for fixed queue capacity or cancellation. H3 DATAGRAM, outer DNS datagrams, and native P2P datagrams remain zero-wait and counted on refusal. Unknown/custom lanes retain the compatibility policy rather than being guessed reliable.
- **The final Client-to-Pack handoff follows that exact lane contract.** A complete Pack from a reliable lane may wait for the existing count/byte-bounded sequence queue or cancellation; a DATAGRAM Pack remains zero-wait. This is the narrow shared-pump exception justified by Transfer ordering: discarding the reliable Pack creates a permanent hole while later work consumes the same finite reorder budget. It does not authorize a larger queue, a wait for unreliable traffic, or blocking arbitrary Client callbacks.
- **A bounded carrier adapter may separate admission from forwarding.** The carrier reader must still offer into that adapter with a zero-wait send and drop immediately on refusal. One adapter-owned forwarding worker may wait on its consumer, because the carrier reader is insulated by the queue; the queue must have independent hard message and byte bounds, cancellation must join the worker, and every queued pooled buffer must be returned before lifecycle completion. Do not share that waiting worker across independent carrier readers.
- **A reliable byte-stream fragment cannot be dropped and skipped.** If a shared receiver multiplexes reliable logical streams and one bounded handoff refuses immediately, close that complete connection/generation so its owner can reconnect. Waiting creates cross-stream head-of-line blocking; continuing after a dropped byte fragment silently corrupts framing.
- **Do not call a blocking send from inside a receive callback.** A send blocks by design, which makes it exactly the thing a receive callback must not contain. Hand the data to a queue owned by a sender goroutine instead, with a 0-timeout enqueue as above.
- **Refusal is correct only at a recoverable boundary.** A true datagram or shared callback can refuse promptly and let the transmission control above it discover the achievable rate. An already-read reliable carrier message is not that boundary: refusing it hides loss from the carrier and forces a much slower Transfer recovery cycle. Never infer the contract from H3 or P2P alone; use the lane metadata that accompanied the exact message.
- The failure shape when this rule is broken is **head-of-line blocking across unrelated flows**: receive delivery is fanned out from shared pumps (a dispatch shard serves many flows; a client receive loop serves every sequence from a source), so one blocked callback parks every flow sharing the pump. One dead destination whose return send blocks for its full write timeout can starve delivery for all live destinations for that entire window — observed as flows that look dead on arrival while their peer is provably alive.

The device-side tun write is one deliberate callback exception, documented at its call site: the provider NAT does not retransmit toward the device, so that handoff is synchronous and its inline write is the path's flow control. The provider's local-NAT TCP return callback is another: it runs on one flow's socket-reader goroutine after upstream bytes have been consumed, and no TCP or transfer layer can reconstruct a segment dropped before the transfer sender accepts it. It may therefore retry Transfer admission synchronously on that dedicated flow goroutine. The exact reliable-carrier and Pack waits above are the other documented exceptions; both retain fixed ownership and end at capacity or lifecycle cancellation. Do not put these waits behind a shared callback worker, extend them to UDP/ICMP/datagram lanes, or infer them from a transport family. An exception must be argued like these — in a comment, from the specific loss model — not assumed.

## Concurrency and goroutine safety

- By default, package-level functions are assumed safe for concurrent use, and a type's methods are assumed NOT safe unless the type documents otherwise.
- Hold a lock across the smallest scope that needs it. The idiom is an immediately-invoked closure: `func() { self.stateLock.Lock(); defer self.stateLock.Unlock(); ... }()`.
- Every potentially infinite loop must take a context (for cancellation) and rate-limit itself (to avoid busy-spinning).
- Use `connect.Reconnect` for reconnect rate limiting.
- Prefer `connect.MonitorValue[T]` over a bare `Monitor` whenever the thing being watched is a value. It pairs the value with its notification so neither half of the contract below can be broken: `Get` hands back the value and a channel armed at the same instant, and `Set`/`Update` notify inside the same critical section (and only on an actual change, so a loop that elects the value it also watches converges instead of waking itself forever). Reach for a bare `Monitor` only when the state is not a single comparable value.
- A `Monitor` has two halves, and both fail silently. **Every mutation of the monitored state must notify**, and **every consumer must subscribe immediately before its read**. A mutation that forgets to notify parks its waiters forever — a monitor with no `NotifyAll` at all is dead on arrival and nothing will tell you.
- Notify from inside the same locked scope as the mutation, so the state change and its notification cannot be separated: `func() { self.stateLock.Lock(); defer self.stateLock.Unlock(); if self.x == x { return }; self.x = x; self.monitor.NotifyAll() }()`. Gate the notify on an actual change — an unconditional notify lets a loop that also writes the state wake itself in a cycle.
- `NotifyAll` closes the live channel and swaps in a fresh one, so a channel from `NotifyChannel` is a one-shot edge: once it fires it stays fired until you re-subscribe. A loop that waits on a channel it never re-subscribes to will hot spin the moment it takes a wake without exiting.
- Subscribe *immediately* before reading the monitored state — `update := monitor.NotifyChannel()` — then `select` on it while waiting for changes. When the state is lock-guarded, subscribe and read in the same locked scope.
- Nothing may block between the subscribe and the read. Both orderings fail silently, in opposite directions:
  - subscribing *after* the read loses an update: a change in between closes the channel that was already discarded, so the loop stalls until an unrelated change arrives.
  - subscribing well *before* the read duplicates work: a change in that gap is already carried by the read, and it also closed the channel that was subscribed, so the next iteration fires immediately and redoes the same work on identical state.
- The monitor delivers edges, but a read of the current state delivers a level. Any change that lands before the read is already in the result, so re-arming for it does not "avoid missing" it — it double counts it. Keep the gap between subscribe and read empty and the two agree.
- A coalescing emitter — at most one emit per epoch, carrying the complete state — waits for the change, sleeps the epoch, and only *then* subscribes and reads/emits. Subscribing before the epoch sleep is the duplicate case above: the burst's changes are already in the snapshot, and they leave the next round armed, so an identical emit fires one epoch later. See `DeviceLocal.watchNetworkPeers` in the sdk for the reference shape.

## Goroutine lifecycle

- Start a type's internal management goroutines in its constructor, so the returned object is already fully running. The lifecycle loop is conventionally `func (self *T) run()` — lowercase, internal, started by the constructor. When a type has a single internal lifecycle/maintenance loop (e.g. one goroutine that periodically evicts TTL state), name it `run`; give specific names only when a type has several distinct long-lived loops.
- Exception: when an external manager must clean up after the lifecycle, expose `func (self *T) Run()` (uppercase) instead. The manager calls `Run()` after construction and tears the object down when `Run()` returns. Casing carries the meaning: lowercase `run()` is internal and self-started; uppercase `Run()` is externally driven.
- A client-owned callback must never call `Client.CloseAndWait` directly. Receive, forward, ack, contract-status, contract-stats, encryption-event, peer-identity, and P2P callbacks can execute inside the worker tree that `CloseAndWait` joins; calling it there can self-join forever. A callback may request non-joining `Close`/`Cancel`, or hand shutdown to an owner goroutine outside the client tree.
- Out-of-band control is an explicit ownership boundary. The client's launcher is owned until `SendControl`/`SendControlWithCtx` returns; after return, the OOB implementation owns its request, frames, and callback and the client does not join that external work. The one-shot `CloseContract` fallback started with `context.Background()` after client shutdown uses the same transfer deliberately; server-side contract expiry is its failure backstop.
- Wait with `time.After` inside the run loop by default; we don't reach for `time.Timer` for the convenience of it.
- Exception — hot-path timer reuse: in a per-packet (or otherwise hot) loop, where profiling shows the per-iteration `time.After` allocation is a significant share of allocations, reuse a single `time.Timer` instead. Create it with `time.NewTimer(0)` before the loop, `defer timer.Stop()`, and `timer.Reset(d)` immediately before each blocking `select` that reads `timer.C`. This relies on go1.23+ timer semantics, where `Reset` guarantees no stale fire is delivered afterward, so the drain dance is unnecessary and the initial already-fired state is harmless. Reach for this only with a profile that justifies it, not preemptively.

## Logging

- Components log through a `Logger` (see `log.go`), not the global glog functions.
- Guard every verbose log statement that takes format arguments — `if self.log.V(2).Enabled() { self.log.Infof("[tag]...", a, b) }`, never a bare `self.log.V(2).Infof("[tag]...", a, b)`. The variadic arguments (and any `fmt` / `.String()` work among them) are boxed into an `[]any` at the call site *before* the level is checked: Go's variadic + interface-dispatch semantics defeat escape analysis, so a disabled level still heap-allocates on every call. The `Enabled()` guard skips all of it. This matters most on per-packet paths but is the rule everywhere, for consistency.
- Unconditional `Infof` / `Warningf` / `Errorf` (no `V(n)`) always emit, so they need no guard; a verbose call with no arguments boxes nothing and needs none either.

## Formatting and structure

- Format with `gofmt`.
- Write binary size constants as products of well-known decimal values, such as `1024 * 1024` or `16 * 1024 * 1024`, rather than shifts such as `1 << 20` or `16 << 20`.
- Inline single-use helper logic as a local closure at its call site (`f := func(...){ ... }; f()`) rather than pulling it out into a separate named function.

## Tests

- **Test data must never contain production identity or secrets.** Do not copy
  tokens, credentials, private route prefixes, customer/account identifiers,
  real hostnames or domains, public or private IP addresses, or other live
  configuration values into test code, fixtures, golden files, comments, or
  snapshots. Use visibly synthetic names, `.example` hostnames, RFC-reserved
  documentation addresses (`192.0.2.0/24`, `198.51.100.0/24`,
  `203.0.113.0/24`, and `2001:db8::/32`), and generated test-only identifiers.
  When reproducing a length, collision, parser, or topology boundary, construct
  an equal-or-more-demanding synthetic value and test the invariant rather than
  retaining the production literal. Sanitize any production capture before it
  becomes a fixture; keep necessary raw incident evidence outside the source
  repository under its restricted evidence policy.
- **Deterministic reproduction is a completion gate.** A bug fix is not complete until a regression test deterministically reproduces the pre-fix failure and verifies the corrected behavior. Use explicit barriers, hooks, or state transitions to force the relevant ordering; do not make sleeps, short negative timeouts, queue-length polling, or scheduler luck the primary proof. Race, ownership, stale-generation, and topology bugs each need a test at the layer where the broken behavior is observable so the same issue cannot silently return through another path.
- Each test is a top-level `func TestXxx(t *testing.T)`. Normal (positive) tests do not use `t.Run` subtests: if cases are logically separate, write separate top-level tests; if they are homogeneous variations of one thing, use a plain table loop (`for _, c := range cases { ... }`) reporting with `t.Errorf`/`t.Fatalf`.
- `t.Run` is appropriate only when the subtest boundary itself is the point — notably when a test deliberately runs a subtest that is expected to FAIL and asserts that failure. The subtest isolates and captures the failure so the parent can check it.

## Struct creation

- When initializing structs, always use explicit field names — `T{field: value}`, not `T{value}`.

## Capitalization

- In comments we do not use all upper case. If there is an important concept, use a concise name in lowercase for readability. Use plain words for names where possible e.g. "identity companion"

## Locking

- Functions that are expected to be called with one or more state locks should be named "*WithLock". Inversely, functions that do not have "*WithLock" should expect to be called with no state locks.
- Operations on locked state should be as tightly scoped as possible.
- Calls to external objects must not hold a state lock. This is generally an implication of the "WithLock" rule.
- Locks must always be acquired in consistent order.
- Do not use re-entrant locks because they will mask locking issues.

## Message Pool

Pool buffers (`MessagePoolGet`) have a single owner that is responsible for returning them (`MessagePoolReturn`). Ownership moves by these rules:

1. **A successful send takes ownership.** When a buffer is handed to a sender and the send returns success, the sender now owns the buffer and is responsible for returning it.
2. **An unsuccessful send leaves ownership with the caller.** If the send returns not-success, the caller still owns the buffer and must return it (or reuse/retry it).
3. **A callback buffer is borrowed, valid only for the call.** A buffer passed to a callback is owned by the caller and is only valid for the duration of that call. To use it beyond the callback (e.g. to forward it on a channel or hand it to another goroutine), the callback must take a shared copy with `MessagePoolShareReadOnly` and pass that copy on; ownership of the copy then follows the send rules above.
