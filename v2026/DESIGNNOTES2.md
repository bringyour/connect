# DESIGNNOTES2 — E2E encryption: fail-open and operator-MITM

Design analysis of the two review findings against the current tree, plus (§8)
the as-built record of the Finding 1 fix.

> **Status (2026-08-08):** Finding 1 is FIXED — `EncryptionMode`
> (Off / Opportunistic / Required) shipped, with the "Post Quantum Encryption"
> profile mapping to Required and the provider side staying Opportunistic.
> See §8 for the as-built design, which supersedes the §2.4 sketch (the sketch
> predates the head-of-line discovery). Finding 2 is DEFERRED; the chosen
> direction is a multi-operator identity quorum — the client publishes its
> identity key to, and reads peers' keys from, a **list of operators**, taking
> the plurality answer — which removes the single-operator trust root. That is
> a larger design (especially operator-side) recorded here for a future cycle.

Companion to `DESIGNNOTES.md` §3 (end-to-end encryption) and §3.7 (threat
model). Where the two disagree on a literal, this file cites the source and is
current as of the tree at writing.

---

## 0. TL;DR

- **Both findings are real.** Finding 1 (silent fail-open) is verified and is a
  deliberate "opportunistic" design, not a bug — but it ships with no enforced
  alternative and no user-visible signal. Finding 2 (lockstep operator MITM) is
  described accurately by the repo's own §3.7.
- **The findings are not independent, and the order matters.** Finding 2's
  sophisticated attack (substitute cert + signature + key in lockstep) is only
  *necessary against a client that refuses plaintext*. Against today's
  opportunistic client, an operator doesn't need to substitute anything — it
  drops or stalls the handshake and the client silently falls back to plaintext
  it can read directly. **Fail-closed (Finding 1) is therefore a prerequisite
  for any of the identity-binding work (Finding 2) to matter.** Fix order is 1
  then 2.
- **Finding 1 is fully fixable** and the shape is clear: a tri-state policy
  (Off / Opportunistic / Required) plus a *receive-side* downgrade gate and an
  observability signal. The hard part is not the mechanism; it's the product
  decision about what "Required" does to provider availability.
- **Finding 2 is fixable only in degrees without a trust-model change.** The
  cheap fix the notes call the "closing move" (source the identity key from the
  OOB API and refuse on mismatch) does **not** close the hole against a fully
  malicious platform, because the OOB API *is* the platform — it defends only a
  weaker adversary (transport-only MITM, or an inconsistent/rogue platform
  subsystem). Genuinely closing it requires removing the platform from the
  identity-trust path: self-certifying client IDs (large migration) and, for
  anonymous provider egress, an independent key-distribution channel.

---

## 1. Ground truth (verified in code)

Key file:line references, so the discussion is anchored and drift is catchable.

**Fail-open, send side** — `transfer.go: writeMaybeWrappedBytes` (~3935):

```
cipher := (session != nil && !forceUnwrapped) ? session.Cipher() : nil
if cipher == nil { write plaintext; return }   // unconditional pass-through
... verifyPeerCertAgainstContract(); SealOuterFrame(...)  // reached only when cipher != nil
```

The cert/identity checks live *inside* the `cipher != nil` branch. When the
cipher is nil they are never reached — there is nothing to enforce because
nothing is being sealed.

**Fail-open, receive side** — `transfer.go: Client.run` (~1567): only frames that
*arrive* with `EncryptedTransferFrame` set are unwrapped/verified. A frame that
arrives plaintext sets `unwrapped = true` and flows straight into normal Pack
dispatch. **There is no check that a peer which *should* be encrypted actually
sent ciphertext.** This is the receive-side half of fail-open and is what makes
a downgrade attack trivial (see §4).

**The seal gate** — `transfer_encrypt.go: Cipher()` (~2264): returns nil until
*both* the TLS handshake completed and `peerIdentityVerified` is true; also
returns nil during an in-flight rekey (`handshakeInFlightLocked`). Nil is
deliberately indistinguishable, to the wrap path, from "no encryption."

**No enforcement knob exists.** `EncryptionSettings` (`transfer_encrypt.go:466`)
has `Encrypt bool` (default `false`, line 596) and no `Require`/mode field. On
handshake/identity/timeout failure the session stays cipher-nil and
`completeHandshake` (~1477) records the failure; traffic then flows plaintext
with no further gate.

**Shipped timeout** — `DefaultEncryptionSettings.TlsTimeout = 60s`
(`transfer_encrypt.go:604`). (DESIGNNOTES §3.11 says the default is `-1`;
the shipped default is 60s — the notes are stale on this literal.)

**The user-facing toggle** — `ip_remote_multi_client.go:9156`:

```
if performanceProfile != nil && performanceProfile.PostQuantumEncryption {
    ...
    clientSettings.EncryptionSettings.Encrypt = true   // comment: "A provider
    // without session support falls back to plaintext at this layer."
}
```

So the "Post Quantum Encryption" switch a user flips maps to opportunistic
`Encrypt=true`, and the fallback to plaintext is explicit and intended. **This
is exactly the review's "may not have it on."**

**Defense 3 is wired in production** (correcting a natural reading of §3.7 that
it's only a hook): SDK `device_local_provider.go:403` installs a default
`NewPeerClientPublicKeyFetcher` that hits `<apiUrl>/key/<peerId>`. But the
cross-check is **log-only** — `transfer_encrypt.go: crossCheckPeerClientPublicKey`
(~1850) logs on mismatch and still trusts the contract-supplied key
(`SetPeerClientPublicKey`, ~1792, first-write-wins). And critically, `apiUrl` is
the platform's own API (§5).

---

## 2. Finding 1 — the seal fails open, silently

### 2.1 What is actually true

Encryption is a binary property of `Cipher() != nil` (DESIGNNOTES §3.1). If the
handshake never completes, the identity proof never verifies, the contract never
carries the peer's public key, or the 60s timeout fires — the session stays
cipher-nil and **application data flows in plaintext, on both send and receive,
with no error surfaced to the caller and no way to demand otherwise.** All three
sub-claims in the finding check out.

### 2.2 Why it was built this way (and why that's defensible)

The fallback is not laziness — it's a **rollout constraint**. Providers are
heterogeneous; older providers don't run the session layer at all. A client that
hard-required encryption today could not use those providers — it would degrade
from "VPN that works" to "VPN that often can't connect." The design chose
availability over confidentiality *as a default*, which is a legitimate choice
for an opportunistically-encrypting overlay mid-migration.

The problem is not that opportunistic mode exists. The problem is that it is the
*only* mode, it's what the "Post Quantum Encryption" label maps to, and the
downgrade is invisible. A user reading "Post Quantum Encryption: on" has no way
to know a given flow is plaintext, and no way to say "then don't send it."

### 2.3 The design space

**Option A — Tri-state policy (recommended).** Replace `Encrypt bool` with a
mode (keep the bool as a compatibility alias):

- `Off` — today's `Encrypt=false`. Manager inert.
- `Opportunistic` — today's `Encrypt=true`. Seal when possible, plaintext
  otherwise. Remains the default during provider rollout.
- `Required` — seal or fail. Never emit plaintext application data to a peer for
  which a session is expected; never accept plaintext application data from one.

This preserves the availability default while giving a security-conscious user
or app an enforced setting, and it lets the product decide *later* whether the
"Post Quantum Encryption" label maps to Opportunistic or Required (or exposes
both as "on" vs "strict").

**Option B — Observability only.** Keep opportunistic, but make the downgrade
visible: per-peer encryption status is *already* queryable
(`EncryptionSessionManager.PeerIdentities()` + `AddPeerIdentityChangeCallback`,
~3213/3248). Surface "peer X is plaintext despite encryption enabled" to logs /
metrics / UI. This addresses "no user-visible indication" but **not** "may not
have it on" — it's a strictly weaker answer to the finding and belongs *inside*
Option A, not instead of it.

Recommendation: **A, with B's signal included.** Enforcement is the part the
finding is actually about; the signal is cheap and complementary.

### 2.4 The sub-decisions inside "Required" (this is where the care goes)

Required mode is not one gate; it's several, and getting any of them wrong either
reintroduces the leak or breaks the handshake:

1. **EC handshake frames stay plaintext even in Required.** The handshake
   bootstrap (`ForceUnwrapped` EC packs, DESIGNNOTES §3.5) *must* remain
   plaintext or the cipher can never come up. Required gates **application
   data**, never the EC carrier. The `forceUnwrapped` flag already distinguishes
   them at the exact choke point (`writeMaybeWrappedBytes`), so the gate has the
   information it needs.

2. **Control-plane and no-session peers are always exempt.** Traffic to/from
   `ControlId`, self-traffic, and any `SendNoSession`/`ReceiveNoSession` peer is
   legitimately plaintext (DESIGNNOTES §3.9, §4.1). "Required" means "required
   for peers where a session is expected," so the gate must sit *after* the
   existing no-session check, reusing it — not a blanket "drop all plaintext."

3. **Send-side behavior on cipher-nil: block, don't drop, then time out.** For
   an established peer mid-rekey, `Cipher()` returns nil transiently by design
   (and the contract-open ride-along is intentionally sent in the clear during a
   restart — see `Cipher()` ~2299 and DESIGNNOTES §3.8). A naive "drop on nil"
   would break rekey and contract-open. Required should **hold** application data
   while a session is in-flight/re-establishing and only fail when establishment
   never completes (bounded by `TlsTimeout`). Distinguish "never established"
   (fail) from "transiently re-establishing" (wait). This is the single subtlest
   part of the whole change.

4. **Receive-side gate is mandatory, not optional.** A send-only gate is
   defeated by a peer (or MITM) that simply sends plaintext to a receiver that
   accepts it (§1, `transfer.go:1567`). Required must **drop inbound plaintext
   application frames from a peer for which a session is expected** (same
   exemptions as #2), and audit it as a bad message. Without this, fail-closed is
   theater.

5. **Failure must surface, not vanish.** When Required can't establish, the
   caller needs to know. Options: tear the sequence down with a typed error;
   fire the peer-identity callback with a "degraded" state; expose a per-peer
   "required-but-plaintext" count. At minimum it must not look identical to a
   healthy plaintext peer.

### 2.5 The honest cost

Required mode **reduces the usable provider set** to those running the session
layer, or makes connections to the rest fail. That is the real reason this isn't
just flipped on. The architecture should make the *choice* explicit and located
with the user/app — not resolve it unilaterally in the library. That's the
product decision to surface (§7).

---

## 3. Finding 2 — the operator can MITM a sealed session

### 3.1 The attack, traced precisely

Every defense is rooted in one value: `peerClientPublicKey`, the peer's
long-lived Ed25519 identity key. Trace where it comes from:

- The contract (platform-authored, HMAC-signed by the platform) carries
  `destination_client_public_key`, `provide_tls_certificate`, and
  `destination_client_key_signed_tls_certificate`
  (`protocol/transfer.proto` fields 8/7/9; consumed in `transfer.go` ~6122).
- `SetPeerClientPublicKey` (~1792) sets `peerClientPublicKey` **from the
  contract's** `destination_client_public_key`.
- Defense 1, `AddTrustedPeerCertChain` (~2474), admits the contract's cert only
  if `VerifyCertChainSignature(peerClientPublicKey, chain, sig)` passes — i.e.
  it verifies the cert against *the very key the same contract supplied*.
- Defense 2, the in-handshake identity proof, verifies the peer's signature over
  the TLS exporter against *the same* `peerClientPublicKey`.

So a platform that substitutes **all three in lockstep** — its own MITM cert
`C'`, a signature over `C'` made with an attacker key `K'`, and `K'` as
`destination_client_public_key` — passes every check:

```
SetPeerClientPublicKey(K')                       -> accepted (first-write-wins)
AddTrustedPeerCertChain(C', sig_{K'}(C'))        -> VerifyCertChainSignature(K',...) OK
TLS handshake: MITM presents C', terminates TLS  -> A<->MITM and MITM<->B both sealed
identity proof: MITM signs exporter with K'      -> verifies against K'  OK
Cipher() becomes visible                          -> classic MITM, both halves "encrypted"
```

The root cause, in the repo's own words (§3.7): the platform is treated as a
trusted **distributor of keys**. Whoever controls the key controls every defense
downstream of it.

### 3.2 Why the §3.7 "closing move" doesn't actually close it

DESIGNNOTES §3.7 names the closing move: feed `SetPeerClientPublicKey` from the
OOB lookup rather than the contract, and promote the cross-check from log →
refuse. That is worth doing (§3.3) — but be precise about what it buys:

**The OOB endpoint is the same platform.** `crossCheckPeerClientPublicKey`
(~1850) fetches `<apiUrl>/key/<peerId>` — the platform's own API. A platform
that substitutes `K'` in the contract can serve `K'` from `/key` too. The
cross-check would then be **comparing the attacker against himself** and pass.

So the OOB cross-check defends only against a **weaker adversary**:

- a transport-level MITM (e.g. a malicious TURN/relay operator) who mediates the
  data path but does *not* control the key API, or
- an inconsistent or rogue *subsystem* of the platform (a compromised
  contract-authoring path while the key store stays honest), i.e. a data bug or
  partial compromise.

Those are real and worth defending. But against a **fully malicious platform**
that controls both the contract and `/key`, the cross-check is worthless. It
must not be described as closing the operator hole — it raises the bar from
"lie once" to "lie consistently across two of your own endpoints," which a
unified malicious operator does trivially.

### 3.3 The degrees of fix

**Tier 0 — now, cheap: promote the cross-check to enforcement + source the key
from OOB.** Make `SetPeerClientPublicKey`'s trusted value the OOB-fetched key
(or require agreement before trusting), and refuse (cipher stays nil, or Required
fails closed) on mismatch. Closes the hole against the weaker adversary in §3.2.
Interacts with Finding 1: "refuse" only means something if the client is
Required — otherwise refuse → plaintext → the operator reads it anyway (§4).
**Label honestly: this does not defend against a fully malicious platform.**

**Tier 1 — medium: TOFU + cross-session pinning.** Pin a peer's identity key on
first verified contact; alarm/refuse on later change (composes with the existing
first-write-wins). Defends against a platform that is honest-then-turns-malicious
or that targets a subset of sessions. Doesn't help first contact. Cheap
defense-in-depth; composes with everything else.

**Tier 2 — the real fix: self-certifying client IDs (DESIGNNOTES §3.7 alt 2).**
Derive `ClientId = truncate(hash(pubkey))` (libp2p/Tor style). Then the
contract's `destination_client_public_key` is **verifiable against the ClientId
itself**: the client checks `hash(pubkey) == destinationId` and needs no trusted
distributor. The platform can't substitute a key without changing the ID, and
the ID is the address the caller targets. This genuinely closes *substitution*.
Cost: large migration — ULID assumptions, key generation, storage, sharing links
all bake in platform-assigned IDs. §3.7 flags it as too large to have done then;
it remains the only real fix for the substitution vector.

**Tier 3 — anonymous egress: independent key distribution (alt 5).** Even
self-certifying IDs only help if you learned the peer's ID through a trusted
channel (§3.4). For anonymous provider egress, where the platform *chooses*
which provider you talk to, closing the hole fully needs a distribution channel
the platform can't unilaterally forge: a transparency log with audit, peer
gossip, or a DHT. High infra cost.

### 3.4 The distinction that determines how far you can get: substitution vs selection

Two different powers of the operator:

- **Substitution** — "for the peer you chose, here's a (fake) key." Closed by
  self-certifying IDs (Tier 2): the key is checkable against the ID.
- **Selection** — "here's *which* peer to use," for anonymous provider egress.
  The platform hands you a provider (and thus an ID) from its own list. Even
  with self-certifying IDs, if the platform picks an ID whose key it holds, it
  reads your traffic — legitimately, by your own configuration.

Consequence: for **explicitly-addressed peers** (a friend's ID via QR/link,
out-of-band), Tier 2 is a *complete* fix — the platform is removed from the
trust path entirely. For **anonymous provider egress**, Tier 2 reduces the
problem to "you trust the platform's provider *selection* but not its ability to
read your traffic" — still a large gain — and only Tier 3 closes the rest, at
cost. Deciding which of these two use-cases must be protected is the product
question that scopes Finding 2 (§7).

---

## 4. How the two findings interact (the load-bearing point)

They are usually discussed separately; they are coupled, and the coupling
dictates the fix order.

**Against today's opportunistic client, Finding 2's clever attack is
unnecessary.** An operator who wants to read traffic doesn't substitute keys in
lockstep — it just makes the handshake fail (drop/stall EC frames, or refuse to
run the session layer). The client hits the 60s timeout, `Cipher()` stays nil,
and `writeMaybeWrappedBytes` sends plaintext the operator reads directly. The
receive side accepts the plaintext return path just as readily (§1). No key
substitution, no cert forgery — just induced fallback.

Therefore:

- **All of Finding 2's identity-binding work (Tiers 0–3) is bypassed by a
  downgrade unless the client is Required (Finding 1).** Fixing the MITM without
  fixing fail-open is fixing the lock while leaving the door open.
- **Fix order: Finding 1 (fail-closed) first, then Finding 2 (identity
  hardening).** Required mode is what makes an MITM *have* to attack the identity
  layer at all; only then do the key-binding defenses become the operative
  battleground.
- Corollary: Tier 0's "refuse on key mismatch" is meaningful only under
  Required. Under Opportunistic, "refuse" degrades to "plaintext," which the
  operator wanted anyway.

---

## 5. Recommendation (phased, each phase independently shippable)

1. **Phase 1 — Fail-closed policy (Finding 1).** Introduce the tri-state
   (Off / Opportunistic / Required). Implement the five sub-gates in §2.4, with
   special care on #3 (block-during-rekey vs fail-on-never-established) and #4
   (receive-side downgrade drop). Add the observability signal (§2.3-B) in all
   modes. Default stays Opportunistic; Required is opt-in. This is the
   highest-leverage change and unblocks everything else.

2. **Phase 2 — OOB key enforcement (Finding 2, Tier 0).** Source the peer
   identity key from the OOB API and refuse on mismatch, meaningful now that
   Required exists. Ship with an **honest scope statement**: defends against
   transport-only MITM and rogue/inconsistent platform subsystems, *not* a
   fully malicious platform.

3. **Phase 3 — TOFU pinning (Tier 1).** Cheap defense-in-depth against
   turns-malicious / targeted operators.

4. **Phase 4 — Trust-model change (Tiers 2/3), if the threat model demands
   defending against a fully malicious platform.** Self-certifying IDs for
   explicitly-addressed peers; independent key distribution for anonymous
   egress. Large; scope with the product decision in §7 first.

Phases 1–3 are all within `connect` (plus a settings surface in the SDK).
Phase 4 is cross-repo (connect/sdk/server/apps) and is the one that needs the
"confirm design before large refactor" gate before any code.

---

## 6. What does *not* need to change (scope guards)

- The **cipher construction** (AES-256-GCM over the TLS exporter, PQ-hybrid
  curves, per-message nonce) is sound and orthogonal to both findings. Leave it.
- The **optimistic non-blocking receive path** (DESIGNNOTES §3.5) is a
  deliberate DoS defense. The receive-side Required gate (§2.4-#4) must be a
  cheap, non-blocking drop — it must not reintroduce per-packet blocking on TLS
  state. Enforce by dropping, not by waiting.
- The **control-plane / no-session exemptions** (§3.9, §4.1) are correct and
  must be preserved verbatim by any Required gate.

---

## 7. Decisions for you (before any implementation)

These are genuine product/threat-model choices the library shouldn't make
unilaterally:

1. **What is the threat model's top adversary?** Transport-only MITM (rogue
   relay)? A rogue platform *subsystem*? Or a *fully malicious platform*? This
   single answer decides whether Phase 2 is sufficient (first two) or whether
   Phase 4 is mandatory (last one). Everything scopes from here.

2. **What does the "Post Quantum Encryption" label promise the user?** If it
   should mean "my traffic is encrypted or it doesn't go," it must map to
   Required (Phase 1). If it means "encrypt when the provider supports it," keep
   it Opportunistic but add the visible signal. Possibly expose both ("on" vs
   "strict").

3. **Is Required opt-in or the default?** Default-Required maximizes security and
   shrinks the usable provider set (breaks connections to non-session
   providers); default-Opportunistic preserves availability and leaves security
   to those who opt in. This is the core availability-vs-confidentiality call.

4. **Which use-case must be protected against substitution — explicitly-
   addressed peers, anonymous provider egress, or both?** Determines whether
   Tier 2 alone suffices or Tier 3 is also required (§3.4).

5. **On Required failure, what should the user experience be** — connection
   fails loudly, or silently avoids non-encrypting providers and only fails if
   none qualify? Affects the Phase 1 failure-surfacing design (§2.4-#5).

My recommendation, pending your answers: build Phase 1 with Required as opt-in,
ship Phase 2 with the honest scope caveat, add Phase 3, and treat Phase 4 as a
separate design cycle gated on the answer to #1 and #4.

---

## 8. As built (2026-08-08) — the Finding 1 fix

Decisions taken: tri-state `EncryptionMode` enum **replacing** `Encrypt bool`
(zero value = Off, so a zero settings struct still encrypts nothing);
`PostQuantumEncryption` profile → **Required** on the consumer
(`ip_remote_multi_client.go`); provider stays **Opportunistic**
(`sdk/device_local_provider.go`) so old consumers keep working.

### 8.1 The discovery that reshaped the design: head-of-line coupling

The §2.4 sketch ("hold at the writer / drop at the receiver") is **wrong**, and
failed in exactly the way §2.4-#1 warned about, one level deeper. The
client-role handshake's `EncryptedControl` frames ride **the same reliable send
sequence as application data** (for a client session, carrier companion =
identity companion — `EncryptionControlUseCompanion` only moves the *server's
reply* carrier). The sequence is strictly ordered, and the receive side's
optimistic EC path deliberately skips initial ClientHellos. Consequences:

- Holding an already-sequenced app frame at the writer leaves a permanent gap
  at its sequence number; the ClientHello behind the gap is never delivered
  in-order; the handshake that would clear the hold can never complete. Wedge.
- Dropping a plaintext app pack at the receiver creates the same gap from the
  other side (the sender resends the refused pack forever). Wedge.

**Invariant learned: a fail-closed gate must never perturb sequence
numbering or in-order delivery — gate *outside* the sequence (entry), or
*after* it (delivery), never inside it.**

### 8.2 The four pieces (all in `transfer.go` / `transfer_encrypt.go`)

1. **Send entry gate — `SendSequence.Pack`.** Before a sequence number is
   assigned, an application pack (not `ForceUnwrapped`, session expected,
   Required) waits for `Cipher() != nil`, polling on
   `EncryptionSettings.RequiredCipherPollInterval` within the caller's timeout
   budget; `timeout == 0` refuses immediately; budget exhaustion refuses
   (unsent — never plaintext). Handshake controls pass freely and claim the
   first sequence numbers, so establishment is never behind the gate.
   The wait holds the idle condition open, keeping the sequence (and the
   session it references) alive through the establishment it waits on.
   Multi-client interplay: the ping's 5s `PingWriteTimeout` spans a normal
   establishment, so a session-capable provider passes its ping and a
   non-capable one fails it → provider rotation, converting "cannot seal" into
   churn instead of outage.

2. **Rekey continuity — `Cipher()`.** Under Required, while a replacement
   handshake is in flight the established epoch's cipher keeps serving
   (Opportunistic keeps the historical plaintext fallback). The receiver holds
   `[established, prior]` decrypt ciphers through the swap; a frame that races
   a double promotion drops and is re-sealed under the current epoch on resend.

3. **Contract-open ride-along — pinned plaintext pre-cipher.** The
   contract-only open pack (no app frames — it debits at the 1-byte floor) is
   generated *inside* the sequence, below the entry gate. Pre-cipher it is now
   queued `ForceUnwrapped` (sticky across resends, the EC-frame rationale);
   post-cipher it queues unpinned and wraps normally. Without the pin, the
   Required write backstop refused it and wedged establishment (§8.1). This
   also fixes a latent Opportunistic cousin: a pre-cipher open resent after the
   local cipher came up (peer's not yet up) used to re-seal into ciphertext the
   peer couldn't read.

4. **Receive gate — ack-and-discard in `ReceiveSequence.receiveHead`.** A
   plaintext item from a session-expected peer under Required has its
   application frames stripped after EC frames are routed to the session; the
   item still **advances the sequence and is acked** (refusing the ack is the
   §8.1 wedge), and the peer audit records a bad message. Covers the nack path
   too (`receiveNack` now stamps `unwrapped`). The contract frame is pack
   metadata, not an app frame — it still registers, so a later sealed flow
   rides it. Tradeoff, deliberate: the ack tells an Opportunistic sender its
   pre-seal plaintext was "delivered" when it was discarded — the alternative
   (no ack → gap → wedge) is strictly worse, and end-to-end protocols above
   the tunnel handle the loss.

   A write-layer backstop (`writeMaybeWrappedBytes` refuses cipher-nil app
   frames under Required) remains as a race guard; with pieces 1–3 it should
   never fire in steady state.

### 8.3 Tests pinning the behavior

- `TestRequiredEncryptionEstablishesAndDelivers` — Required×Required: gates
  don't break establishment; every delivered message is necessarily wrapped;
  seal state reports `KeyExchange == tls.X25519MLKEM768` (regression-pins the
  PQ hybrid negotiation itself) and the Sealed event fires.
- `TestRequiredEncryptionFailsClosedAgainstPlaintextPeer` — Required×Off:
  neither direction delivers app data (send gate holds; receive gate
  discards); a's client session exists and stays cipher-nil (a real held
  handshake, not a skipped one); b's resend queue drains (ack-and-discard, no
  wedge); exactly one RequiredSendBlocked and one RequiredReceiveDiscarded
  event (per-session dedup pinned). Send loops run in separate goroutines
  because a Required `Send` parks at the entry gate.
- `TestCipherContinuityDuringRekey` — §8.5-1 pinned for BOTH modes.
- `TestRequiredSendRefusalTypedErrorAndEvent` — deterministic (no peer, no
  transports): non-blocking and bounded sends refuse with
  `ErrEncryptionRequiredNotEstablished`; blocked event exactly once;
  Opportunistic control enqueues.
- `TestEncryptionEventsAndStates` — injected-epoch deterministic pins for
  Sealed / IdentityFailed / EstablishFailed events and the
  `PeerEncryptionStates` aggregation (Establishing/Sealed/KeyExchange/
  FailureReason).
- `TestRejectCandidateMissingEncryptionKey`, `TestEncryptionCapabilityFetcher`
  — prefilter decision + channel plumbing (§8.5-4).
- `TestMultiClientChannelPqe` (updated) — PQ profile maps to Required.
- The pre-existing suite pins Opportunistic/Off unchanged.

### 8.4 Follow-ups

- `RequirePostQuantum` (restrict `CurvePreferences` to the hybrid) if the
  product label should also exclude classical-sealed sessions. The negotiated
  group is now observable (`PeerEncryptionStates.KeyExchange`), so this is a
  small config knob when wanted.
- Finding 2: multi-operator identity quorum (see Status note at top).

### 8.5 Migration smoothing (2026-08-08, second pass) — as built

Four follow-ups from the review discussion, shipped together:

1. **Rekey continuity unified across modes.** `Cipher()` now serves the
   established epoch during an in-flight re-handshake in BOTH modes; the
   historical Opportunistic plaintext-during-rekey window is gone. Its
   documented rationale (contract-open in the clear) was obsoleted by the
   §8.2-3 queue-time pin. Tradeoff accepted: against a peer that lost its
   responder session entirely, old-epoch wraps are undecryptable, so delivery
   during the recovery re-handshake waits for establishment + a resend
   interval instead of flowing plaintext — bounded latency for no plaintext.
2. **Typed refusal.** The Required entry gate refuses with
   `ErrEncryptionRequiredNotEstablished` (errors.Is-able; `SendBuffer.Pack`
   short-circuits it rather than recreating the sequence), so callers can
   tell "encryption not established" from transport backpressure.
   **Liveness fix discovered here:** a parked send holds the sequence's idle
   condition open, so the sequence never idles out and `AcquireForSend` — the
   only restart trigger — never runs again; after a failed first epoch a
   parked send would wait forever with nothing retrying. The gate's poll loop
   now nudges `restartHandshake()` (client role only; internally guarded by
   the in-flight check and initial-retry cooldown).
3. **Observability.** `EncryptionSessionManager.AddEncryptionEventCallback`
   (edge-triggered, per-session deduped: Sealed / EstablishFailed /
   IdentityFailed / RequiredSendBlocked / RequiredReceiveDiscarded — failure
   events coalesce with the establishment-failure log throttle) plus
   `PeerEncryptionStates` (per-peer Sealed / Establishing / KeyExchange /
   FailureReason; KeyExchange is the negotiated `tls.CurveID` captured at
   handshake completion, distinguishing PQ-hybrid from classical). SDK/UI
   wiring is the apps' adoption point — the connect surface is complete.
4. **Provider prefilter.** `MultiClientSettings.EncryptionCapabilityPrefilter`
   (default true): under Required, the window fails a candidate as soon as the
   out-of-band key API definitively reports "no published key" — such a peer
   can never seal, so the ping (entry-gated on the cipher) could only wait out
   `PingTimeout` against it. Fetch errors never reject (the prefilter
   accelerates certain failure, never admits); runs concurrently with the
   ping, parented on the ping's ctx.
