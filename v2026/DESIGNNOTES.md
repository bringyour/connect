# URnetwork `connect` — System Design Notes

These notes capture the design decisions, rationale, invariants, and hard-won
lessons behind the `urnetwork/connect` client library. They were reconstructed
from the design/debugging sessions that produced the end-to-end encryption layer
(May–June 2026), the contract-manager fixes, the transport/framing rework, and
the `experimental-perf` IP/TUN performance work, and then **verified against the
current source**. Where the recorded discussion and the shipped code diverged,
the shipped code is treated as the source of truth and the divergence is called
out as a lesson.

The goal is to preserve *why* the system is built the way it is — especially the
reversed approaches, because the wrong turns are the most expensive knowledge to
re-derive.

> Source-of-truth caveat: line numbers drift. Symbol names and constants below
> were verified against the tree at the time of writing; treat file:line as a
> hint and re-grep the symbol. Several subsystems have evolved *past* the
> conversations they came from (noted inline as “evolution”).

---

## 0. Reading guide

- **Decision / Why / Invariant / Lesson** is the recurring shape. The *Invariant*
  lines are the ones to not violate; the *Lesson* lines record what was tried and
  rejected.
- "EC" = `EncryptedControl`, the control message that carries TLS handshake bytes.
- "wrap/unwrap" = the end-to-end AEAD layer (deliberately *not* called
  "encrypt/plaintext" — those names are reserved for the transport/packet layer;
  the middle layer "wraps").
- `ControlId` = the zero `Id{}` = the platform / control plane, which itself runs
  a `Client`.

---

## 1. System overview & architecture

`connect` is the client library for URnetwork, a decentralized VPN /
bandwidth-sharing overlay. A node ("client") exchanges traffic with peer clients
(and the platform) over one or more **transports**, with bandwidth accounted by
signed **contracts**, carried as reliable **sequences**, optionally protected by
an end-to-end **encryption** layer, and — for "provider" egress — bridged to the
real internet through an **IP/TUN** layer.

### Layered model (bottom to top)

```
  real internet  ┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄
       │  egress sockets (provider side)
  ┌────┴─────────────────────────────────────────────┐
  │ Subprotocols          subprotocol.go             │  user protocols as frames (SUBPROTOCOL.md)
  │ IP / TUN layer        ip.go, tun.go               │  TCP/UDP <-> packets <-> sequences
  ├───────────────────────────────────────────────────┤
  │ Contracts             transfer_contract_manager.go│  bandwidth authorization & accounting
  ├───────────────────────────────────────────────────┤
  │ End-to-end encryption transfer_encrypt.go         │  per-peer wrapped TLS (AEAD)
  ├───────────────────────────────────────────────────┤
  │ Transfer sequences    transfer.go                 │  reliable, ordered, acked; packs
  ├───────────────────────────────────────────────────┤
  │ Routing               transfer_route_manager.go   │  multi-route writer/selector
  ├───────────────────────────────────────────────────┤
  │ Transports            transport*.go               │  platform WS, WebRTC P2P, PT/DNS
  └───────────────────────────────────────────────────┘
```

### Core identifiers and concepts

- **`Client` / `ClientId` (`Id`)** — a node. One `Client` runs the whole stack.
- **`ControlId = Id{}`** (`connect.go`) — the platform. It both issues contracts
  and runs the same `Client` code, so "I am the control plane" is a real runtime
  case, not theoretical (e.g. `NewClientWithDefaults(ctx, ControlId, …)`).
- **`TransferPath`** — `{SourceId, DestinationId, StreamId}`. Routing key. A
  *send* path leaves `SourceId` zero; a *receive* path leaves `DestinationId`
  zero. This asymmetry is a recurring trap (see §3.9).
- **`TransferFrame`** — the wire frame. Carries a `TransferPath` plus a payload
  (`Pack`, `Ack`, or — when wrapped — `EncryptedTransferFrame`). Intermediaries
  parse the frame to extract the path and route; **only the destination inspects
  the payload**.
- **Sequence** — a reliable, ordered, acked stream between two peers
  (`SendSequence` / `ReceiveSequence`). Application messages are batched into
  **Packs**; delivery is acked and retransmitted.
- **role** (`sequenceTlsRole ∈ {client, server}`) — for encryption, whether a
  per-peer session was locally *initiated by a send* (client) or *driven by the
  peer* (server). Determined by direction, **not** by ClientId comparison
  (see §3.4).
- **companion** — return-path identity. A "companion contract" authorizes a
  provider's reply traffic; the `companion` bit also partitions encryption
  sessions (see §3.4, §4).

---

## 2. Transfer sequences — the reliable layer

### 2.1 Packs, acks, resend

- **Decision:** Application data flows as `Pack`s through a `SendSequence`; the
  peer's `ReceiveSequence` reassembles, dedupes by sequence number, and acks.
  Unacked items live in a resend queue and are retransmitted on a timer.
- **Invariant:** The reliable machinery operates on **unwrapped** (plaintext)
  bytes. Encryption is a thin substitution at the very edge of the pipeline (§3.1);
  ordering, acks, retransmit, and contract accounting are all unaware of it.

### 2.2 Resend-queue byte accounting (a real sizing decision)

- **Decision:** The resend queue accounts each item by its **actual
  transfer-frame size** (`len(transferFrameBytes)`), **not** the application
  message size. `messageByteCount` still drives *contract* accounting; the resend
  cap uses on-wire bytes.
- **Why:** For small messages, frame overhead dominates. Content-based accounting
  would let `ResendQueueMaxByteCount` (`mib(2)`) admit tens of thousands of
  in-flight messages — far more than a loaded path can ack within the resend
  timeout. Sizing by true wire bytes gives meaningful backpressure.
- **Invariant:** The resend loop only mutates `transferFrameBytes` (the
  set-head bit) **while the item is removed from the queue**, so add/remove byte
  accounting stays consistent.
- **Code:** `transfer.go: sendItem` byte accounting; `ResendQueueMaxByteCount`,
  `ReceiveQueueMaxByteCount = mib(2)+kib(512)`.

### 2.3 Buffer sizing & idle-timeout ordering

- `defaultTransferBufferSize = 32` feeds `SequenceBufferSize` / `AckBufferSize`
  for send/receive/forward buffers.
- `WriteTimeout` / `BufferTimeout = 15s` (covers transport reconnections).
- **Idle-timeout ordering is load-bearing:** send `IdleTimeout = 300s`, receive
  `IdleTimeout = 120s` — *receive idle must be longer than send idle is the wrong
  way to remember it*; the rule is the **receiver must outlive the sender** so a
  resumed / re-handshaking sender finds the receiver still alive. (The exact
  defaults: send 300s, receive 120s; the comment documents "receive a bit longer"
  relative to the per-sequence reset cadence — preserve the relationship, not the
  literals.)

### 2.4 Ack coalescing at the sequence layer

- **Decision:** `AckCompressTimeout = 10ms` coalesces per-message acks into a
  periodic cumulative head ack.
- **Why:** Halves per-pair ack volume on relay paths for one-way streams and
  prevents resend storms. 10ms is far below the `MinResendInterval` floor (2s),
  so it never affects resends.
- Note: this is the *sequence-layer* ack coalesce. The *IP-layer* TCP ack
  coalesce is a separate 50ms+window/2 mechanism (§7.3) — don't conflate them.

### 2.5 `MinimumMessageLenLimit = 4 KiB` — a deadlock floor

- **Decision:** `ClientSettings.MinimumMessageLenLimit()` returns
  `ByteCount(4 * 1024)`. It is the floor for any runtime framer `MaxMessageLen`
  and any receive-side read limit (`websocket.SetReadLimit`, SCTP receive cap).
- **Why:** The single large `EncryptedControl{Handshake}` server-flight Pack
  (TLS 1.3 ServerHello with an ML-KEM-768 key share ≈ 1.1 KiB, ephemeral ECDSA
  P-256 cert, mTLS) is ≈ 2 KiB raw + ≈ 200 B of EC/Frame/Pack/TransferFrame
  proto wrap; the realized wire pack was ~2050 B. Below a 4 KiB cap the stream
  closes mid-handshake, the resend re-sends the *same* oversized pack, and both
  sides deadlock. 4 KiB also absorbs ASN.1 cert jitter and a larger future PQ key
  share.
- **Invariant:** Embedded callers and tests must floor their framer cap and
  matching receive limit at `max(yourValue, int(client.MinimumMessageLenLimit()))`.

---

## 3. End-to-end encryption ("wrapped TLS")

This is the largest and most intricate subsystem. It adds an end-to-end
encrypted channel between a peer pair, *inside* the already-transport-encrypted
routes, so that **intermediaries and the platform cannot read payloads**.

### 3.1 Where encryption sits, and the binary rule

- **Decision:** Wrap happens **at the moment a `TransferFrame` is about to be
  written to the multi-route writer**, and unwrap when an inbound `TransferFrame`
  is parsed in `Client.run` — before normal Pack/Ack dispatch. Both
  `Pack` frames and `Ack` frames are wrapped.
- **Why:** Keeps all reliability (sequence numbers, ordering, acks, retransmit,
  contracts) operating on unwrapped bytes; the cipher is a substitution at the
  edge.
- **Invariant — encryption is binary on session state:** if the per-peer
  `session.Cipher() != nil`, *everything* to that peer is wrapped; if `nil`,
  everything is plaintext. There is **no per-message encryption flag**. An unset
  cipher means "handshake not complete yet" and mirrors as plaintext.
  - *Evolution (2026-08):* the plaintext mirror is now the
    `EncryptionModeOpportunistic` behavior. `EncryptionModeRequired` fail-closes
    instead: application packs wait at sequence entry for the cipher and
    inbound plaintext application frames are ack-and-discarded — see
    DESIGNNOTES2.md §8 for the as-built gates and the head-of-line invariant
    they must respect.
- **Lesson:** An earlier design wrapped bytes deep inside
  `sendWithSetContract`/`setHead`, forcing `setHead` to decrypt → flip the head
  bit → re-encrypt. Reversed: the send path keeps only unwrapped bytes; `setHead`
  has no encrypted branch.
- **Code:** `transfer.go: writeMaybeWrappedBytes` (the single place that decides
  wrap-or-not on send), `Client.run` (unwrap).

### 3.2 Wire format — a field on `TransferFrame`, not a new message

- **Decision:** There is **no `TransferEncryptedFrame` message type**. When
  wrapping, the writer builds a new outer `TransferFrame` carrying **the same
  `TransferPath`** with the AEAD ciphertext in the field
  `TransferFrame.EncryptedTransferFrame` (proto field `6`). The sealed plaintext
  is the *entire inner* `TransferFrame` (path + Pack/Ack).
- **Why:** Forwarders "expect all messages to be a `TransferFrame` so the
  metadata can be parsed and the `TransferPath` extracted." A bespoke encrypted
  message type broke routing. Keeping the path in cleartext on the outer frame
  lets intermediaries route without decrypting.
- **Invariant — path tamper check:** after decrypt, the receiver compares the
  **inner** `TransferPath` to the **outer** path that drove routing. Mismatch ⇒
  the cleartext outer path was tampered in flight (the inner is AEAD-authenticated)
  ⇒ drop the frame and flag a bad message in the peer audit. `TransferPath` is
  three `Id`s, so the comparison covers all of source/destination/stream.
- **Tradeoff:** `TransferPath` is exposed to intermediaries (required for
  routing); confidentiality covers only the inner payload/metadata, with
  integrity on the path via the post-decrypt comparison.
- **Lesson (two reversals):** Tried (a) a dedicated
  `protocol.TransferEncryptedFrame{sequence_id, payload}` routed specially in
  `Client.run`, and (b) an outer whole-frame wrap with special
  `unwrapEncryptedTransferFrame` routing. Both special-routing paths were removed;
  the final approach folds ciphertext into a normal `TransferFrame` field so
  existing parsing/routing applies unchanged.
- **Code:** `protocol/transfer.proto: TransferFrame.EncryptedTransferFrame=6,
  SessionRole=7 (optional), SessionCompanion=8 (optional)`;
  `transfer_encrypt.go: buildEncryptedOuterFrameBytes`.

### 3.3 Cipher construction — AEAD over a TLS-derived key

- **Decision:** TLS is used **only for the handshake / key agreement**.
  Per-message confidentiality is **AES-256-GCM AEAD** keyed by
  `tls.ConnectionState.ExportKeyingMaterial(label, nil, 32)`. Each message has a
  fresh **random 12-byte nonce** prepended to the GCM ciphertext.
- **Why:** The reliable layer freely retransmits and reorders records; a raw
  `tls.Conn` byte stream cannot tolerate that (TLS requires in-order,
  exactly-once records). Decoupling — TLS for key derivation, AEAD with the
  derived key per message — works with the existing resend semantics.
- **Verified constants:** key label `sequenceTlsKeyLabel = "urnetwork-connect-aead"`,
  key length `32`, nonce `12`. Wire layout `[12-byte nonce][GCM sealed]`; decrypt
  rejects ciphertext shorter than 12 bytes.
  - *Lesson:* the in-flight conversation used label `"urnetwork-sequence-aead"`;
    the committed label is `"urnetwork-connect-aead"`. The label is part of the
    KDF — both peers must agree, so it is a compatibility-bearing constant.
- **TLS parameters (verified):** TLS 1.3 only (`MinVersion = VersionTLS13`); curve
  preference `DefaultSequencePostQuantumCurves = [tls.X25519MLKEM768, tls.X25519]`
  (post-quantum hybrid first, classical fallback — needs Go ≥ 1.24); ALPN
  `["urnetwork/sequence"]`; client `ServerName = "urnetwork-sequence"` with
  `InsecureSkipVerify: true`; ephemeral self-signed **ECDSA P-256** cert.
- **Why post-quantum:** defends against "harvest now, decrypt later."
- **Why mutual TLS:** server config uses `ClientAuth = tls.RequireAnyClientCert`
  so the peer's certificate lands in `PeerCertificates` regardless of which TLS
  role it draws. Go does **not** validate the ephemeral self-signed cert; identity
  is verified at the sequence layer against the contract commitment (§3.6, §3.7).
- **Gotchas:**
  - `SessionTicketsDisabled: true` on the server config — otherwise the TLS 1.3
    server emits a post-handshake `NewSessionTicket` record that the outbox loop
    would ship as a spurious EC (§3.8).
  - `generateSequenceTlsCertificate` originally set `Leaf` to the *unsigned*
    template (empty `Leaf.Raw`), breaking verification; fixed by re-parsing the
    signed DER.
- **Code:** `transfer_encrypt.go: sequenceCipher, DefaultSequence{Server,Client}TlsConfig,
  generateSequenceTlsCertificate, DefaultSequencePostQuantumCurves`.

### 3.4 Per-peer sessions: the `EncryptionSessionManager` and session keying

- **Decision:** Encryption state is **per-peer, not per-sequence**. A single
  `EncryptionSessionManager` on `Client` holds
  `map[sessionKey]*peerEncryptionSession`, where
  `sessionKey = {peerId Id, companion bool, role sequenceTlsRole}`. One session is
  shared by every local `SendSequence` + `ReceiveSequence` of that
  (peer, companion, role). Sequences `Acquire` on construction and `Release` on
  exit (ref-counted under `stateLock`); the manager runs `session.Run()` and
  removes the session when refs hit zero — same lifecycle pattern as
  `SendBuffer`/`SendSequence`.
- **Why per-peer:** the TLS handshake must flow in **both directions**, but the
  reverse direction has no `SendSequence` of its own. Sharing one session per
  (peer, companion, role) lets the handshake complete once and serve all
  sequences in that class.

- **Roles are by direction, not by ClientId (verified — `roleForPeer` does not
  exist):**
  - A local **`SendSequence`** uses its `encryptionRole`; the *normal* send
    sequence is **client** role (TLS-client, sends ClientHello, and is the only
    role allowed to *restart* a handshake). `AcquireForSend` triggers
    `restartHandshake` only when role == client.
  - A local **`ReceiveSequence`** maps to the **server** role
    (`receiveRole := server`); on receive the manager looks up
    `senderRole.complement()`.
  - So for a pair A↔B: A's send→B uses A's *client* session ↔ B's *server*
    session; B's send→A uses B's *client* session ↔ A's *server* session. Each
    peer therefore holds both a client-role and a server-role session per
    companion value. There is also a *server-role send sequence* — the carrier
    that sends the server's EC replies (ServerHello) back.
  - *Lesson:* an **earlier** design assigned roles by lexicographic ClientId
    comparison (`roleForPeer`, lower id = TLS-client). That was removed in favor
    of direction-based roles. If you read older notes mentioning "lex roles",
    they are stale — but the *TLS timing* they describe still holds (§3.5): the
    TLS-client obtains its cipher ~½ RTT before the TLS-server.

- **The `companion` dimension and its exact bug:** before companion was in the
  key, a client-role send with `companion=true` and one with `companion=false`
  were distinct send sequences that **collapsed onto one session**; whichever
  created it first fixed the session's companion (and thus its EC carrier
  contract) for both flows. So EC messages must be partitioned by companion: a
  companion-contract flow and a regular-contract flow to the same (peer, role)
  must land on separate sessions.
  - **Identity companion ≠ carrier companion:** for a *client* session,
    identity = carrier. For a *server* session they diverge — the session-key /
    echo companion is the initiator's `Cc` (from `ec.companion`), but the reply
    *carrier* rides `EncryptionControlUseCompanion`. The code distinguishes
    `companion` (identity, keys the session) from `carrierCompanion` (which
    contract the EC rides). The session derives its identity companion from
    `ec.companion`, never from the carrier flag.
  - **`ContractKey` also needed companion** (a missed touch point): it already had
    `EncryptionRole` but not `EncryptionCompanion`, so the two companion server
    carriers shared one contract queue — the exact starvation the role split was
    meant to prevent. Adding `EncryptionCompanion` to `ContractKey` fixed it.

- **Evolution of the key (so old notes make sense):**
  `TransferPath` → per-peer `peerId` only → `(peerId, role)` (fixed the
  "no encryption session" deadlock, §3.12) → `(peerId, companion, role)` (current).

- **Lesson (ctx aliasing bug):** using `session.ctx` as the EC pack's `Ctx`
  caused the SendSequence to be canceled-and-replaced when the session closed
  mid-flight. EC packs must use the SendBuffer/sequence ctx, not `session.ctx`.

- **Code:** `transfer_encrypt.go: EncryptionSessionManager (Acquire/AcquireForSend/
  Lookup/getOrCreateWithLock/sessionKey), peerEncryptionSession`;
  `transfer.go: SendSequence.encryptionRole/encryptionCompanion`;
  `transfer_contract_manager.go: ContractKey.{EncryptionRole,EncryptionCompanion}`.

### 3.5 Carrying the TLS handshake — `EncryptedControl` inside reliable Packs

- **Decision:** Handshake bytes ride as `EncryptedControl` messages
  (`control_type = Handshake`, raw TLS bytes) wrapped as a
  `Frame{MessageType = TransferEncryptedControl (23)}` and pushed into the
  SendSequence's **normal `packs` channel** (`SendBuffer.SendEncryptedControl`).
  They use the ordinary Pack/Ack/retransmit machinery.
- **Why:** Pack delivery is already reliable and in-order, so handshake bytes are
  delivered reliably and ordered without a bespoke retransmit layer. The
  receiving `ReceiveSequence` **intercepts** `TransferEncryptedControl` frames out
  of incoming Packs, routes them to `session.DeliverEncryptedControl(...)`, and
  strips them so the user callback never sees them (if a Pack held *only*
  encryption-internal frames, the receive callback is skipped).

- **Invariant — handshake bytes must stay plaintext on every (re)send
  (`ForceUnwrapped`):** `SendPack.ForceUnwrapped` pins a frame to plaintext for
  the item's **entire lifetime, including every retransmit**.
  `SendBuffer.SendEncryptedControl` sets it.
  - **Why (the canonical "no cipher forever" bug):** EC packs *carry the very
    handshake bytes that bootstrap the cipher*. If the wrap decision is made from
    the *current* cipher state at write time, then once the local cipher comes up
    between the original send and a retransmit, the retransmit wraps the handshake
    bytes — which the peer (whose handshake isn't complete) cannot decrypt. The
    receiver gets stuck permanently with "encryption session for peer … has no
    cipher." The wrap decision must be **sticky from queue time**.
  - Normal app frames are intentionally *not* force-unwrapped: on retransmit they
    may wrap if the cipher is now up, which is correct.

- **Invariant — receive loop must NEVER block on TLS state (DoS) → optimistic
  delivery:** the unwrap path never waits for the peer's cipher. `Client.run`
  optimistically feeds inbound handshake/identity-proof bytes straight into the
  TLS inbox via `OptimisticallyDeliverHandshake`, gated on
  `IsAwaitingClientFinished()`, so the cipher flips before any following wrapped
  pack reaches the unwrap path in the same receive cycle. A wrapped frame with no
  cipher drops immediately and relies on sender resend.
  - **Why:** blocking the single-threaded, all-peers receive loop per packet is a
    DoS vector — one malicious peer could park the loop and halt critical
    infrastructure nodes.
  - `IsAwaitingClientFinished()` is true only when all three hold: (1) role ==
    TLS-server (the client role gets its cipher ½ RTT earlier, so there's no
    receive race in that direction); (2) `handshakeDone` still open; (3)
    `serverFlightSent` (we actually processed a ClientHello and emitted our
    flight). Outside that window the loop drops instantly.
  - **Structural filter (retransmit safety):** before feeding TLS,
    `OptimisticallyDeliverHandshake` accepts only payloads whose first
    (unencrypted TLS record-header) byte is `20` (ChangeCipherSpec) or `23`
    (application_data carrying the client's second flight). A duplicate
    ClientHello (record type `22`) is rejected, because the TLS state machine
    treats a duplicate handshake message after the server flight as a fatal
    unexpected-message. This is *not* cryptographic validation — protection comes
    from the TLS state machine (no handshake traffic secret ⇒ can't forge
    Finished). Residual risk is per-peer disruption (a spoofed garbage payload in
    the window poisons that one session's TLS state, which then times out and
    reconnects), traded down from all-peers receive-loop DoS.
  - *Lesson (evolution):* hardcoded 2s `UnwrapReadyTimeout` wait → gated to
    server role → gated to `IsAwaitingClientFinished` → **removed entirely** in
    favor of optimistic apply; the `UnwrapReadyTimeout` setting was deleted.

- **Lesson (three reversed approaches — the central churn of the EC work):**
  1. A `writeEncryptedControl` callback on sequences → wrong, it bypassed the
     reliable queue. Final: enqueue as a Pack frame.
  2. Adding an `encrypted_control` field to `TransferFrame` → wrong, control must
     be reliably delivered *in the Pack*, not as a side-band frame field.
  3. Special routing of EncryptedControl in `Client.run`
     (`routeEncryptedControl`, buffer-level `DeliverEncryptedControl`) → wrong;
     removed. Final: `ReceiveSequence` does a *local* hand-off to the session.

- **Lesson (routing parity / ForceStream):** `SendBuffer.SendEncryptedControl`
  must inherit the client's `DefaultTransferOpts` (notably `ForceStream`).
  Without it, EC packs took a *different* SendSequence (different route/key) than
  application packs, so the destination's application ReceiveSequence never saw
  the handshake and its cipher never set — the recurring
  `TestConnectWithSymmetricContractsWithForceStreamEncrypted` failure. Relatedly,
  the optimistic path must deliver **both** Handshake and IdentityProof EC types,
  or a stalled ReceiveSequence (sequence gap after transport reform) leaves the
  identity proof undelivered and the cipher gated off.

- **Code:** `transfer.go: SendBuffer.SendEncryptedControl, SendPack.ForceUnwrapped,
  ReceiveSequence.processEncryptionFramesInPack, Client.run optimistic-apply loop`;
  `transfer_encrypt.go: IsAwaitingClientFinished, serverFlightSent,
  OptimisticallyDeliverHandshake, DeliverEncryptedControl`.

### 3.6 Certificate exchange & verification

- **Decision:** Each client owns **one** sequence-level public TLS certificate
  (built once at construction). The manager **publishes** it to the platform via
  an `EncryptedKey` control message (`MessageType = TransferEncryptedKey (24)`,
  `provide_tls_certificate` = PEM chain) on startup. The cert is stored per
  client (`client_tls_certificate`), **independent of provide mode** — every
  client has a cert.
- **Sender obtains & verifies:** `CreateContract` stamps the destination's cert
  into the contract — into both `StoredContract.provide_tls_certificate` (field
  `7`, sealed inside the platform-HMAC-signed bytes — the trusted copy) and
  `Contract.provide_tls_certificate` (field `4`, outer convenience copy). The
  sender's `sequenceContract` carries `provideTlsCertificate`, and
  `verifyPeerCertAgainstContract` compares the peer's mTLS leaf against the
  contract commitment, cached per (session, contract). A new contract re-arms
  verification.
- **Invariant — empty commitment ⇒ accept:** if a contract commits no
  certificate, verification short-circuits to accept. Crucially this is derived
  **live** from `len(trustedPeerCertPems)` per call, **not** latched.
  - *Lesson:* an earlier sticky `certVerifiedNoCommitment = true` on the first
    empty chain never re-armed, so a later contract carrying the real cert was
    ignored for the session's life. Because `EncryptedKey` propagates
    asynchronously, the first contract for a session can legitimately arrive
    before the cert; the field was removed and the check made live.
- **Decision — trusted cert is a *set*, not a single value:**
  `trustedPeerCertPems` is the **union** of every contract cert this peer has
  appeared in; a wrapped frame decrypts if it matches **any** cert in the set.
  This survives cert rotation across the long-lived per-peer session.
- **Decision — companion reuse:** a companion-mode SendSequence skips cert
  verification and reuses the per-peer session's established cipher/trust (it is a
  reply on keys already in place, not a fresh commitment).
- **Lesson:** PEM everywhere, no DER fallback (`parsePemCertificates` strictly
  rejects non-PEM). Cert encoding mismatches were a silent no-op source.
- **Lesson (reversed):** the cert initially lived on `ProvideKey` (per provide
  mode) with a per-mode table and a `SetProvideTlsCertificate` the client never
  actually called, so verification silently no-op'd. Moved to per-client
  `EncryptedKey`.

### 3.7 Security & threat model — the network-operator MITM

The platform/operator authors contracts and sees both peers' published certs, and
can ride a transport it mediates (TURN-like). It could substitute B's cert in A's
contract and terminate/re-encrypt A↔B's TLS; the bare cert-vs-contract check
would pass against the substituted cert. The defenses bind identity to a key the
platform never holds.

- **Long-lived client identity key (Ed25519):** each client has a persistent
  Ed25519 keypair (`ClientKeyManager`, seeded by `ClientSettings.ClientKeySeed`;
  a wrong-size seed is a hard construction error so a corrupted file can't
  silently fork a new identity). The public key is published and distributed by
  the platform as an opaque identifier.

- **Defense 1 — signed cert binding:** the client signs its ephemeral TLS cert
  chain with its Ed25519 key and publishes `(cert, signature)` via
  `EncryptedKey.client_key_signed_tls_certificate`. The contract carries both plus
  `destination_client_public_key`. `AddTrustedPeerCertChain` admits the contract's
  cert **only if** the signature verifies under the peer's known public client
  key. The operator can't forge a signature over a substituted cert.

- **Defense 2 — in-handshake identity proof:** after the TLS handshake each side
  signs the **TLS exporter output** with its client key and sends it as an
  `EncryptedControl{IdentityProof}` over the now-encrypted channel; the peer
  verifies against the peer's public client key over the same exporter.
  - **Invariant — `Cipher()` returns nil until `peerIdentityVerified`.** The AEAD
    is never exposed without both a successful TLS handshake **and** a verified
    identity proof. Because the proof is bound to the *actual negotiated session's
    key material*, even a fully compromised cert-distribution channel can't pass
    it without the peer's **private** client key. This is the strongest property
    that holds today.

- **Defense 3 (partial today) — out-of-band key cross-check:** an unauthenticated
  server API `GET /key/<clientId>` returns the peer's public client key. A
  **per-session** fetcher
  (`EncryptionSettings.NewPeerClientPublicKeyFetcher func(peerId) func(ctx)([]byte,error)`)
  cross-checks the contract-supplied key against the API-supplied key.
  - **Invariant — first-write-wins on `SetPeerClientPublicKey`:** a mismatching
    key on a later contract is logged and ignored (refusing mid-session key
    rotation is safer than accepting a possibly-malicious switch).
  - **Residual hole:** a platform that substitutes cert + signature +
    `destination_client_public_key` in lockstep still wins on the cert-binding
    side; the closing move is to feed `SetPeerClientPublicKey` from the OOB lookup
    rather than the contract. That cross-check is currently **log-only** —
    promoting "log" → "refuse" is the next hardening step.
  - **Memory bound:** the fetcher is a *factory* minted per session, so the
    fetched key lives only on the goroutine stack and is discarded after compare.
    A fetcher on shared settings would tempt a manager-wide per-peer key cache
    that grows unboundedly. Bound is one Ed25519 public key per open session.

- **Deferred alternatives (recorded):** (2) derive `ClientId` from the public key
  (`truncate(hash(pubkey))`, libp2p/Tor style) to remove the platform from the
  identity trust path — too large a migration (touches ulid assumptions, key gen,
  storage). (3) TOFU + cert pinning across sessions (defense in depth). (5) cert
  transparency log / DHT distribution (high infra cost).

- **Conceptual shift to keep:** stop treating the platform as a trusted
  distributor of *keys*; let it distribute opaque *identifiers* and let
  cryptography bind identity to keys.

- **Code:** `transfer_key.go: ClientKeyManager (Ed25519), Sign, SignCertChain,
  VerifyCertChainSignature, publishClientKey`; `transfer_encrypt.go:
  sendIdentityProofOnce, receivePeerIdentityProof, SetPeerClientPublicKey,
  crossCheckPeerClientPublicKey, Cipher() gating, AddTrustedPeerCertChain`. Server:
  `network_client_key_model.go` (redis `ckey_<clientId>`), `/key/<clientId>`.

### 3.8 Session lifecycle & handshake epochs

- **Decision:** Per-handshake TLS state is factored into a resettable
  **`tlsHandshakeEpoch`** (own ctx + transport + `tlsConn` + `handshakeDone` +
  goroutines + derived cipher/exporter + identity-proof flags + `serverFlightSent`).
  Per-**peer** state (peerId, role, refs, peer public key, trusted certs) stays on
  `peerEncryptionSession`. A reset cancels the old epoch and swaps a fresh one
  under `stateLock`.
- **Why:** the original handshake state was one-shot (`sync.Once`, a single
  `handshakeDone`, goroutines tied to the session ctx) and not resettable — both
  send-side re-handshake and receive-side ClientHello-reset need tear-down-able
  machinery.
- **Reset triggers (final):**
  - *Client role, new SendSequence* → `restartHandshake()`: starts a fresh epoch
    **in the background** while the previously established epoch keeps serving its
    cipher. This is the recovery mechanism — every new client send re-initiates,
    so a peer that lost its responder session rebuilds it on the next burst.
  - *Server role* → never self-restarts; only follows the peer's ClientHello.
  - *Inbound new ClientHello at the server* → reset + feed it.
  - *ReceiveSequence* → always reuse.
- **Invariant — re-handshake is always client-driven; the server only follows.**
  A ServerHello is *derived* from a ClientHello (echoes client random, selects a
  suite, key-agrees against the client's share) — a freshly reset `tls.Server`
  blocks for a ClientHello and emits nothing, so a server self-reset mid-flight
  would wedge until timeout. The reset trigger is specifically a **ClientHello**
  (TLS record `0x16`, handshake type `0x01`), not any post-completion handshake
  byte, so a stray/duplicate record can't thrash a live session.
- **Invariant — gap-free rekey (dual-cipher decrypt window):** a background
  re-handshake keeps the old established cipher live until the new epoch
  completes; on promotion the old epoch is **demoted to `priorEstablishedEpoch`**
  and retained so in-flight frames under the old key still **decrypt** during the
  swap. `decryptCiphers()` returns `[established, prior]`. This matters because
  the ML-KEM-768 handshake is CPU-heavy and re-handshakes are not instant.
  - *Evolution (2026-08):* on the **send** side, `Cipher()` during an
    in-flight re-handshake now keeps returning the established epoch's cipher
    in **both** modes — the historical plaintext fallback (which existed so
    the contract-open ride-along always opened in the clear) is gone; the
    contract-open is instead pinned ForceUnwrapped at queue time when the
    cipher is down. The receiver's dual-cipher window keeps old-epoch frames
    readable through the swap. See DESIGNNOTES2.md §8.2/§8.5.
- **Post-handshake EC sources eliminated (so the outbox only emits during a real
  handshake):**
  - TLS 1.3 `NewSessionTicket` → disabled via `SessionTicketsDisabled: true`.
  - `close_notify` → removed `tlsConn.CloseWrite()` in `closeTls()` (there is no
    graceful-shutdown handshake; a later SendSequence resets and re-handshakes).
    *Lesson:* the first instinct that `close_notify` was the extra EC was wrong —
    `close()` cancels the ctx before `CloseWrite()`, so the alert was generated
    and silently dropped; the real culprit was the session ticket.
- **`IdleTimeout` semantics:** `< 0` keep forever; `== 0` reap immediately at
  refs == 0; `> 0` reap after idle-this-long (in-flight handshakes always kept).
- **Code:** `transfer_encrypt.go: tlsHandshakeEpoch, restartHandshake,
  establishedEpoch/priorEstablishedEpoch, markEstablishedWithLock, Cipher,
  decryptCiphers, deliverHandshake, closeTls`.

### 3.9 The control plane (`ControlId`) is never encrypted

- **Rule:** traffic to or from `ControlId` is **never** wrapped, even with
  `Encrypt=true`. A nil `*peerEncryptionSession` ⇒ plaintext on every
  send/ack/unwrap path.
- **Why:** `ControlId` is the platform; it terminates/originates control traffic
  itself and is the entity that *issues* contracts/keys — wrapping would
  double-encrypt and break the control plane.
- **Invariant — guard on BOTH sides, using the right field:** no
  `peerEncryptionSession` is ever created under `Id{}`. Two enforcement layers:
  the peer-side guard returns nil for the zero id; the **local-side** guard at
  both sequence constructors handles the case where *our own* ClientId is
  `ControlId` (the platform runs this same `Client`).
- **Trap to avoid:** a naive "either path field == ControlId" check is wrong
  because `ControlId == Id{}` and a `TransferPath` always leaves the *opposite*
  field zero — such a check disables encryption for everything. The send guard
  must read `client.ClientId()` and `destination.DestinationId`; the receive guard
  must read `client.ClientId()` and `source.SourceId` — the real peer field plus
  the local client id, never the unset path field.
- **Code:** `connect.go: ControlId = Id{}`; `transfer_encrypt.go:
  EncryptionSessionManager.Acquire/DeliverEncryptedControl` guards; `transfer.go:
  NewSendSequence/NewReceiveSequence` local guards. (Mirrored at the contract layer
  by `SendNoContract`/`ReceiveNoContract`, §4.1.)

### 3.10 Acks mirror the wrapped state of what they ack

- **Rule:** an ack's wrap state must equal the wrap state of the data it
  acknowledges. `ReceivePack.Unwrapped` records whether an inbound frame arrived
  plaintext; this flows into `receiveItem.unwrapped` → `sequenceAck.unwrapped`;
  `writeAck` sends the ack plaintext iff `unwrapped`. Aggregation is **OR**: a
  single plaintext-acked message forces the whole coalesced ack plaintext.
- **Why:** if the local *receiver* completed its handshake before the peer
  *sender* did, wrapped acks would be undecryptable by the peer (no cipher yet)
  and dropped — stalling throughput at session start (acks are idempotent so it
  self-recovers within a few RTTs, but it costs a resend interval ~2s of ramp-up).
- **Lesson — a real race here:** the "late ack past the head" branch mutated
  `headAck.unwrapped` **in place** after `Snapshot()` had already published that
  `*sequenceAck` pointer to `writeAck` (which reads it without the lock). Fixed by
  **copy-on-write** — swap in a fresh `headAck` with the bit set rather than
  mutating the in-flight one.
- **Code:** `transfer.go: ReceivePack.Unwrapped, sequenceAck.unwrapped, writeAck,
  sequenceAckWindow.Update/Snapshot`.

### 3.11 Encryption settings (verified defaults)

- `Mode EncryptionMode` — policy tri-state; **default `EncryptionModeOff`**
  (session manager inert, all I/O unwrapped). `Opportunistic` is the historical
  `Encrypt = true` (seal when established, else plaintext); `Required`
  fail-closes (DESIGNNOTES2.md §8). The former `Encrypt bool` was replaced by
  this enum (2026-08); the PQ performance profile maps to `Required` on the
  consumer while the sdk provider side stays `Opportunistic`.
- `TlsTimeout time.Duration` — TLS handshake timeout; **default `60s`** (an
  earlier iteration shipped `-1` = disabled; bounded establishment was adopted
  so a departed peer or undeliverable flight fails instead of retaining epoch
  workers). Negative disables: the watcher arms only when `0 < TlsTimeout`. On
  timeout the session stays cipher-nil — under Opportunistic traffic flows
  plaintext; under Required application traffic stays held (sessions still
  never block the *sequence machinery* — under Required it is the pack entry
  that waits, not the run loops). The watcher is a goroutine selecting
  on ctx / `handshakeDone` / `time.After(TlsTimeout)` — the *only* timer in
  non-test code, written with `time.After` to honor the no-`NewTimer` rule (§6.4).
- `RequiredCipherPollInterval time.Duration` — **default `20ms`**; how often a
  send blocked by the Required entry gate re-checks `Cipher()`. Unset falls
  back to the default rather than spinning.
  - *Naming history:* the field was `HandshakeTimeout`, briefly
    `TlsConnectTimeout`, finalized as `TlsTimeout` to match
    `ConnectSettings.TlsTimeout`. The rename was scoped to **only** this field;
    the unrelated websocket/quic handshake timeouts in `net.go`/`transport.go`
    were deliberately left alone.
- `EncryptionControlUseCompanion bool` — **default `true`**; sends EC handshake
  packs as companion traffic so the TLS responder (which often has no regular
  contract back) can reply. Set `false` only for symmetric-provide tests.
- `IdleTimeout time.Duration` — session reaping (§3.8); derived as
  `max(send-idle, receive-idle)`.
- `ProvideTlsCertificatePem` / `ProvideTlsPrivateKeyPem []byte` — persist/reload
  the server-role cert (PKCS#8) so the `EncryptedKey` commitment is stable across
  restarts; empty ⇒ fresh ephemeral self-signed.
- `ClientTlsConfig` / `ServerTlsConfig *tls.Config` — overrides; nil ⇒ the
  permissive defaults above.
- `ClientSettings.ClientKeySeed []byte` — persist `Client.ClientKeyManager().Seed()`
  and reload to keep a stable identity.

### 3.12 Root causes & invariants from the debugging marathon

A long marathon drove `../server/connect` to two consecutive 100% passes. The
loud signal — `completeHandshake FAILED: context canceled` — was always a
*downstream symptom*; the real causes were in contract selection and pack delivery
for EC frames. The durable invariants:

1. **EC carrier contract type must match the available contract direction.** The
   TLS server's reply travels the non-providing direction and needs a **companion**
   contract; a non-companion sequence there has no contract → `"No contract"` →
   handshake ctx canceled. Hence `EncryptionControlUseCompanion`. The session
   *identity* companion (keys the session) is distinct from the *contract-routing*
   companion (`carrierCompanion`); they coincide except for a server reply, and are
   both `false` for symmetric and no-contract tests.

2. **A handshake-bootstrapping frame must never be discarded on a transient
   delivery failure.** EC delivery was originally a single fire-and-forget
   `Pack(sendPack, 15s)`; with no routes available (no transport reform) it timed
   out and the *only* ClientHello the TLS state machine ever emits was lost → dead
   handshake. Final: a retry loop that returns true only on success, else loops
   until ctx cancel; on ultimate failure it **closes the session** so the next
   acquire rebuilds a fresh handshake. There is no "lost ClientHello" middle state.

3. **Never hold `packMutex` across a blocking enqueue of a *different* class of
   pack.** A rejected fix used inline `Pack(sendPack, -1)` (infinite timeout) for
   EC, which holds `packMutex` while blocked on the packs channel → mutual
   deadlock with the application data `Pack`. EC sending must stay off the data
   path's critical section (goroutine + finite-timeout retry + session-close).

4. **Cipher-bootstrap frames are plaintext on every send and resend,
   unconditionally** (the `ForceUnwrapped` invariant, §3.5).

Test-infrastructure lessons (reusable):
- **A "resend queue empty" assertion is only valid after reliable control traffic
  has had time to be acked**, not merely after the last application-data ack — EC
  packs sharing the data sequence's queue legitimately linger. Add a short drain
  wait.
- **Tests sharing a fixed port range (8080–8089) must gate on deterministic port
  availability, never a fixed `time.Sleep`.** A sub-startup-time failure with no
  flaky/panic marker is resource contention (the assertion exited the goroutine
  before the panic-based retry fired), not a code bug. Replace sleeps with an
  active `net.Listen`-until-free readiness check.
- **Debugging method:** follow the error chain to its *head* (don't fix the TLS
  symptom); a goroutine dump localizes "nothing is happening" deadlocks fast;
  bisect by test axis (`Encrypted × NoNack × {NoTransportReform, contract mode}`)
  — transport-reform variants passed only because fresh connections masked the
  missing-route window, and that contrast was the clue; drive one failing test to
  two isolated passes, then resume the full suite.

---

## 4. Contracts & contract manager

Contracts are signed authorizations that account for and permit transfer between
two peers. A `ContractManager` acquires and tracks them; the platform
(`ControlId`) issues them.

### 4.1 `SendNoContract` / `ReceiveNoContract` — the exemption gate

- **Decision:** `SendNoContract(destinationId)` / `ReceiveNoContract(sourceId)`
  return `true` when a peer is exempt. Always exempt (seeded at construction):
  `ControlId` and the client's **own** ClientId (self-traffic). If
  `ContractsEnabled()` is false, everything is exempt. `AddNoContractPeer` adds at
  runtime.
- **Why:** messages to/from the platform cannot themselves require a contract —
  the platform is what *issues* contracts, so requiring one is circular.
- **Invariant — mutual configuration:** both peers must independently configure
  no-contract for each other, or one side demands a contract the other never
  sends. The check runs **before** acquiring/checking a contract.
- The encryption layer mirrors this with `SendNoSession`/`ReceiveNoSession`
  (= local or peer is `ControlId`), gating session creation — behavior-preserving
  vs the inline guards (§3.9).

### 4.2 Companion contracts, origin linger, ping

- **Companion contract** = a return-path contract authorizing a provider's reply.
  Requested via the `CompanionContract()` transfer option → `ContractKey` →
  `CreateContract.Companion` on the wire.
- **Why:** the responder (provider) holds only `ProvideMode_Stream`; it has no
  permission to open a *forward* contract back to the source, so a normal contract
  is rejected ("no permission"). The reply must ride a companion contract.
- **`OriginContractLinger = 300s` (5 min):** a server-side policy carried in
  client settings — a companion contract may be created for up to this long
  **after the origin (forward) contract was closed**, so reply traffic can resume
  briefly after the request side goes idle. No client-side consumer; enforced
  server-side.
- **Ping → companion reply:** `RemoteUserNatProvider.ClientReceive` echoes
  `IpIpPing` back over `source.Reverse()` **with `CompanionContract()`** — matching
  how the provider's other return traffic flows. Previously it echoed without the
  option and would be rejected.

### 4.3 Contract queue expiry — the drained-queue leak

- **Bug:** on SendSequence exit, `FlushContractQueue` force-removes and `Drain`s
  the queue (waking waiters so they bail rather than block on the orphan). But a
  `CreateContractResult` that **lands after** the exit-flush re-creates a fresh
  queue via `addContract`; if that destination is never used again (provider
  rotation), the entry and its escrowed contract are retained **forever**.
- **Fix:** `ContractQueueExpireTimeout = 120s` + a background janitor
  (`expireQueuedContracts`, ticks every `timeout/2`); each contract is stamped
  with `enqueueTime` and closed if no sequence takes it. `TakeContract` also
  returns stale entries as expired and **closes them rather than handing a
  possibly-already-settled contract to a sequence**.
- **Invariant — 120s must stay below the platform's 5-minute unused-contract
  force-close window.** A stale queued contract may already be force-closed
  server-side, so handing it out or holding it is wrong. (Note the symmetry:
  `OriginContractLinger` is also 300s — same platform timescale, opposite
  direction.)
- **Shutdown:** on manager ctx done / `client.Done()`, `finalFlush` closes all
  pending queued contracts promptly to release escrow. These shutdown closes can't
  ride the in-band transport (it's gone) — `CloseContractWithCheckpoint` detects
  shutdown and routes a **one-shot** close over the **out-of-band API on
  `context.Background()`**, bounded by `RequestTimeout`, never retried (the
  server's expired-contract force-close is the backstop).
- **Code:** `transfer_contract_manager.go: expireQueuedContracts,
  ContractQueueExpireTimeout, queuedContract, CloseContractWithCheckpoint`;
  `transfer_oob_control.go`; `api.go: ConnectControlWithCtx`.

### 4.4 Wait for the return-contract secret before using the client

- **Decision:** in `ApiMultiClientGenerator.NewClient`, after enabling return
  traffic, **block until the platform has committed the provide secret** via
  `SetProvideModesWithReturnTrafficWithOobAckCallback` (forces
  `ProvideMode_Stream`, registers out-of-band), waiting on a `provideAck` channel
  bounded by `ControlPingTimeout` (default 30s).
- **Why:** the companion (Stream) contract is **verified against this provide
  secret**. Using the client before the secret is registered races and fails
  verification. An **in-band** ack only means *delivered*, not *processed*; the
  **OOB** ack means *committed* (the OOB round-trip returns the platform's
  response). The bug was masked by client↔platform latency and only surfaced on
  the proxy where latency is minimal.
- **Invariant:** the return path's `ProvideMode_Stream` secret must be registered
  on the platform **before** any companion contract is created/verified for that
  client. "Provide secret registered" is the happens-before for "companion
  contract verifiable."
- **Code:** `transfer_control_oob.go: ControlSyncOob`; `transfer_contract_manager.go:
  SetProvideModesWithReturnTrafficWithOobAckCallback`;
  `ip_remote_multi_client_api.go: ApiMultiClientGenerator.NewClient`.

### 4.5 Lifecycle, secrets, and `ContractKey`

- **Lifecycle:** create → queue → take → use → checkpoint/close. A sequence
  triggers `CreateContract` (OOB frame to platform) → `HandleControlFrame` routes
  the `CreateContractResult` → `addContract` → `contractQueue.Add`; the sequence
  `TakeContract`s; `update(messageByteCount)` debits; close reports acked/unacked
  bytes via `CloseContractWithCheckpoint`.
- **Contracts bind to sequences via `ContractKey`** (one queue per key). The key
  includes `Destination`, `IntermediaryIds`, `CompanionContract`, `ForceStream`,
  and the encryption discriminators `EncryptionRole` / `EncryptionCompanion` —
  the latter two exist specifically so the client-role data sequence and the
  server-role reply/EC carrier (and companion variants) to the **same destination**
  do not share a queue and clobber each other's pending contracts on exit-flush.
- **Secrets:** per-`ProvideMode` 32-byte `provideSecretKeys` (random, lazy). A
  contract's `StoredContractBytes` carries an HMAC-SHA256 verified by
  `VerifyStoredContract`, which accepts both legacy and standard HMAC formats for
  cutover. `provideSecretKeys` retains all keys until restart so a pending send
  sequence's contracts don't have to be flushed when modes change.

---

## 5. Transports, framing & message pooling

### 5.1 Framer is for byte-STREAM transports only

- **Decision:** a framed (byte-stream) transport — TCP / QUIC / WebSocket — must
  read through a `Framer`, **not** `buf := MessagePoolGet(Max); n := conn.Read(buf)`.
  The framer reads a 4-byte header (2 bytes length + 2 bytes split index), then
  `io.ReadFull`s exactly `messageLen` body bytes into a right-sized pooled buffer.
- **Why:** a stream doesn't preserve message boundaries — one `conn.Read` may
  return a partial message or several concatenated. "One Read == one message"
  silently corrupts the stream on any segmented read. The header lets the reader
  size the buffer and reassemble across read boundaries.
- **Invariant:** `Framer.Read` rejects frames where `maxFrameLen < messageLen+4`,
  logging `[framer][reject]read` (so an oversized handshake flight surfaces) rather
  than silently closing.
- **`Framer.Write` splits** any message `>= max(16, SplitMinimumLen=256)` into two
  writes (header+first-half, then second-half) to save one memcpy — which is
  exactly why the framer is **unsafe on packet/message transports** (each write
  becomes one packet/message; the body arrives split with no in-band way to detect
  loss/reorder).

### 5.2 WebRTC P2P uses NATIVE SCTP message framing — no framer (reversed twice)

- **Decision:** the P2P/WebRTC path does **not** use a `Framer`. The detached pion
  data channel is **message-oriented** (one SCTP user message per `Write`/`Read`),
  so the message boundary *is* the frame delimiter. The receive loop reads one
  whole message per `conn.Read` into a single long-lived buffer
  (`make([]byte, MaxMessageByteCount)`, allocated once for the transport's life),
  then `MessagePoolCopy`s the exact bytes out; the send loop does one `conn.Write`
  per frame.
- **Why:** empirically, pion's detached channel returns `io.ErrShortBuffer` if the
  supplied buffer is smaller than the incoming message — so a framer's first step
  (reading a 4-byte header into a 4-byte buffer) trips `ErrShortBuffer` on every
  non-trivial message. A round-trip through the real pion conn failed with
  `read 0: short buffer`.
- **Invariant:** `P2pTransportSettings.MaxMessageByteCount` (default `64 KB`) is
  the largest single message; the full max must be declared so a whole message
  fits one `Read`. 64 KB sits well under pion's 4 MB reassembly cap, and
  `WebRtcSettings.ReceiveMtu = kib(4)` (= 4096) sizes SCTP per-chunk on the wire
  (pion fragments/reassembles automatically) — do not confuse per-chunk MTU with
  per-message size.
- **Lesson — `main` was actually broken here:** it used `framer.Read` over the
  message-oriented conn (would hit the same `ErrShortBuffer`); the e2e-pqe branch
  had removed the correct `ReadPacket`/`WritePacket` (whole-message, no length
  prefix) and replaced it with a raw fixed `conn.Read`. The user's hypothesis "use
  standard Write/Read with length framing" was *also* wrong — that fails *harder*
  on a message transport (fails at the header instead of the body). The bug was
  the **size cap**, not the framing primitive: the channel is message-framed;
  declare the full max size.
- **Regression guard:** `TestWebRtcMessageRoundTrip` pushes varied-size frames
  through the **real** pion conn (the pre-existing `TestWebRtc` used the raw conn
  and bypassed `MaxMessageByteCount`, so it never caught the mismatch). Avoid tight
  per-op write deadlines under `-race` (they flake during pion setup) — rely on ctx
  headroom.

### 5.3 `MinimumMessageLenLimit` / 4096 and a construction-recursion trap

- WebRTC max message size tracks `max(MinimumMessageLenLimit(), 4096)` — i.e. 4096
  by default, rising if the floor ever rises (§2.5).
- **Trap:** you **cannot** call `DefaultClientSettings()` inside
  `DefaultP2pTransportSettings` / `DefaultWebRtcSettings`: `DefaultClientSettings`
  → … → `DefaultP2pTransportSettings`, so sourcing the floor there infinitely
  recurses at startup. The platform transport can use it only because
  `DefaultPlatformTransportSettings` is not in that construction chain.
  - *Lesson:* a package-level `const minimumMessageLenLimit` was briefly extracted
    to dodge the recursion, then reverted once the P2P path stopped using a framer
    (its rationale was gone).

### 5.4 Message-pool discipline

- **Decision:** to hand received bytes downstream, use
  `MessagePoolCopy(readBuf[:n])`, not `MessagePoolGet(n)` + manual `copy`.
  `MessagePoolCopy(b)` is exactly `MessagePoolGet(len(b))` + `copy` — a drop-in
  that removes duplicated `len`/`copy` boilerplate and a class of size-mismatch
  bugs.
- **Invariant — ownership transfers on a successful send.** On a not-sent branch
  (e.g. `ctx.Done()` before handoff) the loop must `MessagePoolReturn` the buffer;
  returning *after* a successful send is a use-after-return / double-free.
- **Why it matters:** the old code did `MessagePoolGet(MaxMessageByteCount=65536)`
  per received message. The pool only has 2048/4096 size classes, so 64 KB falls
  through to a fresh `make()` that `MessagePoolReturn` can't recycle — an unpooled
  64 KB alloc per message on a path called "billions of times per user hour." The
  reused-buffer + `MessagePoolCopy` pattern cuts that to one `make` per transport
  plus one *pooled* right-sized buffer per message.
- `MessagePoolReturn` returns `bool` (false if too large to recycle) — size
  requests to a pool class matter.

---

## 6. Concurrency & state management

### 6.1 One `stateLock` per object, not `atomic.Pointer`

- **Pattern:** each stateful object guards all its mutable state with one
  `sync.Mutex` named `stateLock`. Given the explicit choice between an
  `atomic.Pointer[T]` and folding a field under the existing `stateLock`, the
  standing rule is **prefer a single `stateLock` over an atomic**.
- **Why:** one lock = one serialization point and one set of invariants. Scattered
  atomics each add an independent happens-before edge and a "is this field
  consistent with that one?" question. Simplicity and fewer subtle races beat
  lock-free reads.
- **When atomics are acceptable:** never as a substitute for `stateLock` on live
  mutable state. The accepted lock-free pattern is a field **set once at
  construction and never mutated** (needs no synchronization at all — strictly
  stronger than an atomic). Reach for immutability-after-construction before
  reaching for an atomic.

### 6.2 Reduce mutable state (the governing principle)

- "Reducing mutable state is good." Concrete application: a value re-fetched from
  a shared, lockable, occasionally-swapped source (`manager.{Client,Server}TlsConfig()`)
  was converted into an **immutable per-session snapshot** captured once at
  creation (`peerEncryptionSession.tlsConfig`, `.Clone()`d under the manager lock
  it already holds), read lock-free thereafter.
- **Why:** binding the value to the session's lifetime eliminates the entire
  question of observing a mid-life change — nothing to lock, no consistency
  window. Config rotation is picked up by sessions created afterward; live sessions
  keep the identity they were born with.
- **Lesson:** prefer "snapshot at creation under a lock you already hold" over
  "re-derive on demand under a fresh lock." The former adds zero new
  synchronization and removes a shared read from a hot/locked path.

### 6.3 Lock ordering, and the ABBA deadlock it prevents

- **Rule:** single global lock order **Manager (M) → Session (P)**; never invert.
- **The bug:** `buildAndStartEpochWithLock` (called while holding the session lock
  P, from `restartHandshake`) called `manager.ClientTlsConfig()`/`ServerTlsConfig()`,
  which take M — a **P → M** edge. The reaping path (`CancelIfIdle`, `Release`)
  takes M then P. Two goroutines on the same session+manager formed a permanent
  ABBA cycle (the dump showed only ~25 goroutines — a true deadlock, not a leak).
- **Fix:** the §6.2 snapshot removes the cross-object read entirely — the build
  path reads the immutable `tlsConfig` and takes no manager lock. Prefer
  **structural elimination** over reordering: reordering still leaves the
  cross-lock coupling for the next maintainer to re-introduce.
- **Why "passes alone, hangs in suite":** this was a *timing-widened race*, not
  shared-global-state contamination (the global message pool and
  `DebugTransferCopyOnWrite` were investigated and ruled out). The inversion needs
  the per-session idle poll (`CancelIfIdle`, armed only when `IdleTimeout > 0`) to
  collide with a concurrent send's `restartHandshake` on the same session. Alone,
  the test finishes in ~0.67s and the window almost never lines up; under a full
  `-race` suite, accumulated goroutines + GC + `-race`'s ~10× critical-section
  stretch widen the window until it hits.
- **Lesson:** the hang was *silent* because the test's wait put a lock-taking
  `Lookup` in a `for`-loop **condition** *ahead* of the deadline `select`, so the
  wedged goroutine never reached its own timeout. Keep blocking lookups inside the
  guarded region. Use Go's `-timeout <finite>` to get the canonical all-goroutine
  dump labeled "running tests:" (preferable to SIGQUIT, which under default
  `GOTRACEBACK=single` may dump one goroutine); `test.sh` uses `-timeout 0`, so a
  hung test runs forever with no automatic dump.

### 6.4 Lifecycle, shutdown, and the "ready" gate

- **`Cancel()` vs `Close()`:** `Client.Cancel()` does **not** call the session
  manager's `Close()` — only `Client.Close()` does. Session goroutines shut down
  by watching the shared `ctx`. **Invariant:** any goroutine a session/manager
  spawns must `select` on `ctx.Done()` so `Cancel()` alone reliably stops it.
- **`ReadyNotify` gate:** manager constructors start goroutines that may send, but
  messages can't be sent until buffers are wired. `Client.ready` (a `chan struct{}`
  closed after `initBuffers` + the `run` goroutine, exposed as `ReadyNotify()`)
  gates them. **Convention:** any goroutine in a manager constructor that calls into
  `Client` must wait on `ReadyNotify()` before its first send (used by
  `publishEncryptedKey`, `providePing`, `publishClientKey`).

### 6.5 No timers — `time.After` + ctx

- **Rule:** never `time.NewTimer` / `time.NewTicker`. For a timed wait, start a
  goroutine that `select`s on `time.After(...)` and `ctx.Done()`. Verified
  codebase-wide: **0** `NewTimer`/`NewTicker` in non-test code; `time.After`
  across ~19 files.
- **Why:** `time.After`-in-`select` ties the timer's lifetime to the `select` and
  the context — cancellation is automatic and there are no timer handles to
  `Stop()`/drain (a classic leak/footgun). Fewer mutable handles aligns with §6.2.

### 6.6 `Monitor` for notifications

- **Rule:** for "something changed, wake waiters", use the shared `Monitor`
  primitive (`util.go`: `NewMonitor()`, `NotifyChannel()`, `NotifyAll()`), not
  ad-hoc channel signaling — it centralizes the notify-channel lifecycle and avoids
  per-site double-close / missed-wakeup / send-on-closed bugs.

### 6.7 `*WithLock` / `*Locked` naming convention

- **Convention:** a method suffixed `WithLock`/`Locked` assumes the caller already
  holds the relevant lock; everything else acquires as needed. This is what makes
  lock-safety auditable — e.g. `close`/`closeTls`/`Release`/`CancelIfIdle` are
  *not* `*WithLock`, so by convention they are never called holding a lock (and
  indeed all `close()` sites are outside both locks).

---

## 7. IP / TUN layer & TCP-UDP performance (`experimental-perf`)

The IP/TUN layer (a gvisor-netstack userspace stack) bridges a TUN device's
IP packets to/from the encrypted transfer sequences, giving a "provider" peer
egress to the real internet. `ip.go` translates TCP/UDP flows; `tun.go` is the
gvisor device.

### 7.1 Why `tun.go` moved into `connect`

- **Decision:** `proxy/tun.go` + `tun_test.go` moved into package `connect`; the
  server's `proxy/proxy_device.go` and `socks/main.go` now use `connect.Tun`
  directly (the user explicitly rejected a `type Tun = connect.Tun` shim).
- **Why:** so the server uses `connect.Tun` directly and the proxy module stops
  being a TUN dependency. **Root-package placement is the only option that works:**
  an in-package `ip_test.go` can't import a subpackage that imports `connect`
  (import cycle). gvisor netstack is pure Go, so all cross-compile targets still
  build.
- **Invariant — zero-copy packet ownership across the bridge:**
  `SendPacket`/`SendPackets` take pool ownership on success; receive-callback
  packets are pool-returned by the sequence after the callback; `Tun.Write` copies
  into gvisor views before returning. `TcpSequence`/`UdpSequence` **assume
  lossless, in-order delivery** from the source — the single-reader blocking bridge
  preserves that.

### 7.2 Egress data path and the lossless/in-order invariant

- **Path:** gvisor → bridge → `LocalUserNat.Run` (single decode+dispatch
  goroutine, `handleIpPacket`) → per-flow `TcpSequence`/`UdpSequence` main loop →
  write-pipeline goroutine → egress socket. Hand-rolled `parsedTcp`/`parsedUdp`
  views (embedded by value) replaced gopacket on the dispatch path.
- **Invariant (the big one):** the local→remote transfer is lossless and in-order,
  so **any TCP seq other than the exact expected `sendSeq` (or any SYN) is treated
  as a retransmit and silently dropped** — there is no retransmit logic and no
  duplicate-ack. Signed-delta `int32(seq - sendSeq)` comparisons are
  wraparound-tolerant across the 4 GB boundary. **Consequence:** any packet
  *reorder* upstream manifests as unrecoverable loss (recoverable only by the
  peer's RTO) — see §7.4.
- Window: `MinWindowSize = 64 KiB`, `MaxWindowSize = 1 MiB`; `windowScale` derived
  from `MaxWindowSize/MaxUint16`. Egress sockets `SetReadBuffer`/`SetWriteBuffer`
  to `MaxWindowSize`.

### 7.3 TCP ack coalescing — 50ms **and** a window/2 byte threshold

- **Decision:** `TcpBufferSettings.AckCompressTimeout` defaults to `50ms`. After
  each ack, the ack pipeline sleeps on `select { <-time.After(50ms); <-ackSignal;
  <-ctx.Done() }`. A buffered `ackSignal` is signaled (non-blocking, under the
  existing mutex) whenever unacked bytes reach **`windowSize/2`**.
- **Why:** 50ms alone is a throughput foot-gun — it caps upload at
  `windowSize/ackInterval` (only ~20 MB/s at the 1 MB window, ~1.3 MB/s at the
  64 KB initial window). The `windowSize/2` byte threshold makes sustained
  transfers ack ~twice per window (data-clocked, no throughput ceiling); idle and
  trickle flows still get the timer. **Always pair a time-based coalesce with a
  byte/data-clock threshold on a windowed transport.**
- **Impact:** TCP up 124→131 MB/s, TCP down 329→349 MB/s (fewer pure-ack packets
  competing with data).

### 7.4 The `WriteNotify` reorder bug (the ~60× fix)

- **Bug:** gvisor invokes `Tun.WriteNotify` **inline from concurrent per-flow
  sender goroutines**, all popping one shared endpoint queue — handler A can pop
  flow-B's packet and lose the enqueue race, so **same-flow packets reorder
  whenever > 1 flow is active**. Combined with the §7.2 lossless invariant
  (reorder = silent loss), 8-flow TCP upload collapsed to **2.6 MB/s** (zero queue
  drops, hundreds of RTO timeouts).
- **Fix:** serialize the endpoint pop + receive-queue add under
  `writeNotifyLock`. 2.6 → 130–160 MB/s (~60×); also converts overflow into clean
  backpressure. Single-flow cost: none (uncontended).
- **Invariant:** any path feeding a reorder-intolerant (lossless-assumption)
  sequence must preserve per-flow ordering across concurrent producers.

### 7.5 Performance optimizations by direction

Cumulative (1440 MTU): TCP up ~76→~360 MB/s, TCP down ~309→~349, UDP up →~265,
UDP down ~124→~205. The wins, with rationale:

- **MSS advertisement (biggest single win, ~2.3–2.7× TCP up):** `SynAck` now
  advertises an MSS option (`mtu - headerSize - TcpHeaderSizeWithoutExtensions`).
  Previously absent → peers fell back to the RFC ~536-byte default, segmenting
  every upload at ~2.7× the necessary packet count. Production fix for all proxied
  TCP uploads.
- **UDP buffer sizing (UDP down 124→~205):** gvisor UDP endpoints defaulted to
  **32 KiB** each way. Tun now constructs UDP endpoints directly and sizes both via
  `TunSettings.Udp{Receive,Send}BufferByteCount`, **default 4 MiB**. Real fix for
  the 32 KiB ceiling on every device-side UDP downstream flow.
- **TCP buffer ranges (flaky-stall fix):** set
  `TCP{Receive,Send}BufferSizeRangeOption` to `{4 KiB, 4 MiB, 16 MiB}` on the
  shared stack. gvisor's `segmentQueue.enqueue` drops payload segments whenever
  `receiveMemUsed > receiveBufferSize` **even for in-window data** — and since
  nothing retransmits into the tun, one dropped segment wedges the flow forever
  (the 1/15 flaky 1 MiB-echo stall). Headroom matching the advertised window fixes
  it.
- **Coalesce TCP socket writes (writev):** the TCP write pipeline batches queued
  payloads (`WriteBatchSize = 64`) into `net.Buffers(...).WriteTo(socket)` — one
  vectored write per burst with partial-write retry. Attacks the ~30% syscall
  share (~57k single-packet writes/s).
- **Opportunistic drains (reduce wakeups):** `LocalUserNat.Run`, both sequence
  loops, and both `readPackets` consumers block for the first item then
  non-blocking-drain the queue before re-parking. Attacks the ~49% scheduler share
  from the per-packet channel-hop chain.
- **64 KB window-chunked egress reads (TCP down):** read 64 KB/syscall instead of
  1380 B; packetize once under one lock, window-chunked so a large read can't stall
  against a smaller advertised window. `DataPackets` segments to MTU.
- **UDP write batching:** datagrams can't be coalesced (one `sendto` each), but the
  UDP write pipeline batches under one deadline (`WriteBatchSize = 64`).
- **`Tun.ReadBatch` / 64-packet bursts:** block for the first packet then drain the
  queue without re-parking; bridge and `proxy_device.Run` wake once per burst.
- **`WriteNotify` timer fast path:** the common case sends to the receive queue via
  non-blocking select, arming `time.After(WriteTimeout)` only when full — removes a
  timer allocation per packet on the whole egress path. (The old per-packet `send`
  timer was ~33% of all allocations.)
- **Manual header parse + serialization:** hand-rolled IPv4/IPv6/TCP/UDP parsers
  (fixed-offset reads; window-scale option walked only on SYN) and serializers
  (`DataPackets`, `PureAck`, `FinAck`, `RstAck` write headers + inline RFC-1071
  checksums directly into a pool buffer). One alloc per dispatch; malformed packets
  dropped instead of decoded as garbage. `SynAck` stays on gopacket (per-connection,
  needs options). TCP-up allocs/op 657→~415 (−37%).
- **Cross-boundary batching into `LocalUserNat`:** the internal `SendPacket` queue
  element carries a **batch** (`packets [][]byte`) with one `(source, provideMode)`.
  Single-flow neutral; win is on provider ingest (one queue op + one fewer alloc
  per batch). Semantic shift: with timeout 0 a full queue drops the **whole batch**
  and the cap counts batches.
- **`SendShardCount` (default 1):** flows pinned to shards by FNV hash of the
  address tuple (preserves per-flow ordering), batch-preserving router with a
  zero-alloc fast path. Neutral single-flow → default 1; tests run 4 shards for
  race coverage.
- **Lesson:** wall-clock is dominated by **scheduler wakeups + syscalls + gvisor
  per-segment processing**, not user code or allocations — so alloc reductions pay
  off as **GC pressure at scale** (many concurrent flows on providers/mobile), not
  single-flow MB/s. At 1440 MTU the pipeline runs at ~21–23% of its zero-packet-tax
  (64 KB) ceiling; the rest is per-packet tax fixed by the immutable MTU.

### 7.6 Sequence teardown drain order

- **Bug:** both sequences cancelled the context **before** flushing `readPackets`,
  so the drain raced `ctx.Done()` against queued packets — on a server-initiated
  close or read-timeout, tail data / FIN / RST toward the client could be silently
  dropped (~1/10 under `-race`).
- **Fix:** the socket reader closes `readPackets` **without** cancelling; the
  receive pipeline drains to close and only then runs its deferred `cancel`.
- **Rule:** for drain-on-close pipelines, **cancel the context after the drain
  completes, not before.** Termination stays bounded because `Run`'s deferred
  `socket.Close()` always unblocks the reader. Side benefit: RST/FIN on timeout
  teardown now reliably reaches the client.

### 7.7 The 1440 MTU constraint

- `DefaultMtu = 1440`; `TcpBufferSettings.Mtu`, `UdpBufferSettings.Mtu`,
  `TunSettings.Mtu` all default to it. This is a **hard constraint, not a tunable**:
  packets are written directly onto the receiver's tap/tun interface, so nothing
  larger can ship — it's a contract with the devices and the transfer-frame path.
  The `SynAck` MSS derives from it (announces 1400). The `Mtu64k` benchmark
  variants (~1.64 GB/s) are **in-process diagnostics only** that mark the
  per-packet-tax ceiling; raising the device-side MTU (or GSO through the tun) is a
  protocol-level decision needing MTU negotiation + frame/contract review.

### 7.8 Context leak in `proxy_device` (general lesson)

- **Bug:** `go vet` flagged a context leak — `NewProxyDevice`'s error returns after
  `context.WithCancel` never called `cancel`, leaking a context goroutine per
  failed construction.
- **Rule:** **every early-return error path after `context.WithCancel` must call
  `cancel()`** (and release anything already constructed, in the same order
  `Close()` does). A constructor that allocates a cancel context owns its cleanup
  on all exits, not just the happy path.

---

## 8. PT — the DNS pluggable transport

`transport_pt*.go` is a pluggable transport ("PT") that tunnels QUIC over a DNS
codec (DoH; see `net_http_doh.go`). `TestPtDnsEncodeDecode` /
`TestPtDnsPumpEncodeDecode` push 32–64 KB through the DNS codec paced at
`WritePacketsPerSecond = 200` (~2.3s of pacing alone) under 1% induced loss,
against a hard 5s stream deadline. They carry a `// FIXME quic does not seem to
recover well with packet loss`.

- **Symptom:** these tests **pass in isolation (57–59s), in a focused group, and
  6/6 under 16 CPU hogs**, but fail at the hardest iteration with `deadline
  exceeded` only ~20 min into the full `-race` + profiling suite — the process is
  simply slower by then (accumulated race-shadow / GC / heap overhead, likely
  thermal), so the paced transfer overruns 5s.
- **Diagnosis ruled out:** not a goroutine leak (17 idle background goroutines at
  failure), not FD exhaustion (`ulimit -n 8192` would *lower* the 1M limit 128× and
  risk EMFILE in socket-heavy tests), not CPU-load fragility (6/6 under starvation).
- **Fix (mitigation):** **front-load** the PT tests in `test.sh` — run them first
  as their own `go test -run` process and `-skip` them from the main `-race` run.
  Passes in 56.93s/59.03s (was ~900s slow-pass / fail-late). The deadline-bump
  "fix" (5s→20s, 20s→60s) was the **wrong lever** — it traded fast failure for slow
  failure (one test limped to a 943s pass; the other still failed at 296s) and was
  reverted.
- **Lesson:** real-time / loss-sensitive timing tests must run in a **fresh,
  unpressured process** — each `go test` directory is its own OS process, so
  front-loading gives isolated-run conditions without weakening coverage. This
  mitigates the trigger, not the underlying QUIC loss-recovery fragility the FIXME
  flags.

---

## 9. Testing & debugging practices (cross-cutting)

- **`test.sh` ordering matters.** It aborts the whole suite on the first package
  failure and uses `-timeout 0` (a hung test runs forever, no auto-dump). Put
  fragile real-time tests (PT) **before** GC/race pressure accumulates; front-load
  them as separate processes (§8).
- **Fixed-port tests (8080–8089) gate on deterministic availability**, never a
  fixed sleep (§3.12). A sub-startup-time failure with no flaky/panic marker is
  resource contention, not a code bug.
- **Goroutine dumps over speculation.** Reproduce a "nothing's happening" hang with
  a finite `-timeout` to get the all-goroutine dump (§6.3); it localizes deadlocks
  and "X never sent vs sent-but-unacked" fast.
- **Follow the error chain to its head** (§3.12) — the loud repeated error is
  usually a downstream symptom.
- **Zoom-to-one-failure loop:** on a single failure, kill the suite, drive that one
  test to two consecutive isolated passes, then resume the full suite for two
  consecutive 100% passes. Passes-alone-but-fails-in-suite ⇒ cross-test contention,
  not the code.
- **Validate concurrency fixes under `go test -race`**; expect `-race` to widen
  timing windows ~10× (this is a feature — it surfaces races that single-test runs
  hide).

---

## 10. Coding conventions (project-wide)

- **Comparison style — bound on the natural side:** write `0 < len(x)` (lower
  bound on the left), `len(x) < cap` (upper bound on the right). Convert
  pre-existing `len(x) > 0` while editing nearby.
- **Explicit struct fields:** always `T{field: value}`, never positional
  `T{value}`; convert positional inits to keyed while editing.
- **No timers:** `time.After` + `ctx.Done()` in a goroutine, never
  `time.NewTimer`/`NewTicker` (§6.5).
- **`Monitor` for update/notification channels** (§6.6).
- **Single `stateLock` per object; reduce mutable state; prefer
  immutable-after-construction over atomics** (§6.1–6.2).
- **`*WithLock`/`*Locked` naming** for caller-holds-the-lock methods (§6.7).
- **Inline one-shot helpers** (e.g. `isClientSecondFlightPrefix`,
  `isClientHelloRecord`) as closures where used once, rather than package-level
  funcs.
- **PEM everywhere for certs**, no DER fallback (§3.6).
- **Settings over hardcoded constants:** timeouts/sizes belong in
  `ClientSettings`/`EncryptionSettings`/`*BufferSettings` with a `Default…()`
  constructor; negative often means "disabled" (e.g. `TlsTimeout = -1`).
- **Confirm design before large/risky refactors** — restate the spec and surface
  constraints before starting, rather than guessing.

---

## Appendix A — key files

| File | Subsystem |
|---|---|
| `transfer.go` | Sequences, packs, acks, resend, send/receive buffers, `Client.run`, settings |
| `transfer_encrypt.go` | Per-peer wrapped-TLS encryption, `EncryptionSessionManager`, epochs |
| `transfer_key.go` | `ClientKeyManager` (Ed25519 identity), cert-chain signing |
| `transfer_contract_manager.go` | Contracts, `ContractKey`, queues, expiry, secrets |
| `transfer_control_oob.go`, `transfer_oob_control.go` | Out-of-band control sync |
| `transfer_route_manager.go` | Multi-route writer/selector |
| `transport.go`, `transport_p2p*.go`, `transport_pt*.go` | Platform WS, WebRTC P2P, DNS PT |
| `message_framer.go`, `message_pool.go` | Stream framing, buffer pooling |
| `ip.go`, `tun.go`, `ip_test.go` | IP/TUN egress (TCP/UDP), gvisor device |
| `connect.go` | Top-level identifiers (`ControlId`), settings glue |
| `protocol/transfer.proto`, `protocol/frame.proto` | Wire formats |

## Appendix B — notable constants (verify before relying on a literal)

| Constant | Value | Where / why |
|---|---|---|
| AEAD key label | `urnetwork-connect-aead` | TLS exporter KDF label; both peers must agree |
| AEAD key / nonce | 32 B / 12 B | AES-256-GCM |
| PQ curves | `[X25519MLKEM768, X25519]` | hybrid PQ first, classical fallback (Go ≥ 1.24) |
| `Encrypt` default | `false` | encryption master switch (off for now) |
| `TlsTimeout` default | `-1` (disabled) | watcher arms only when `0 < TlsTimeout` |
| `EncryptionControlUseCompanion` | `true` | EC rides companion contract for asymmetric replies |
| `MinimumMessageLenLimit` | 4 KiB (4096) | handshake-pack deadlock floor |
| `ControlId` | `Id{}` | the platform |
| `OriginContractLinger` | 300s | companion-create grace after origin close (server-side) |
| `ContractQueueExpireTimeout` | 120s | must stay **below** the 5-min platform force-close |
| `ControlPingTimeout` | 30s | OOB provide-secret wait |
| `defaultTransferBufferSize` | 32 | sequence/ack buffer depth |
| `ResendQueueMaxByteCount` | `mib(2)` | sized by **wire** bytes, not message bytes |
| `BufferTimeout`/`WriteTimeout` | 15s | covers transport reconnection |
| `MinResendInterval` | 2s | resend floor |
| `AckCompressTimeout` (sequence) | 10ms | cumulative-head ack coalesce |
| send / receive `IdleTimeout` | 300s / 120s | receiver must outlive sender |
| `P2pTransportSettings.MaxMessageByteCount` | 64 KB | one whole SCTP message per read |
| `WebRtcSettings.ReceiveMtu` | 4096 | SCTP per-chunk MTU (≠ per-message size) |
| `FramerSettings.SplitMinimumLen` | 256 | split-write threshold (stream only) |
| `DefaultMtu` | 1440 | hard device constraint |
| TCP `MinWindowSize` / `MaxWindowSize` | 64 KiB / 1 MiB | egress window |
| `AckCompressTimeout` (IP TCP) | 50ms + `windowSize/2` | time **and** data-clock |
| TCP buffer range (gvisor stack) | 4 KiB / 4 MiB / 16 MiB | min/default/max |
| UDP buffer (tun) | 4 MiB | was a 32 KiB gvisor default |
| `WriteBatchSize` | 64 | writev / batch drains |
| `SendShardCount` default | 1 | per-flow sharding off by default |
| PT `WritePacketsPerSecond` | 200 | DNS-codec pacing |
```
