# Low-bar network delivery plan

Status: living implementation plan
Last updated: 2026-09-04

## Outcome

Make URnetwork feel materially more responsive and reliable than the current
VPN path on high-latency, low-throughput, burst-loss cellular links, while
keeping memory, battery use, and transmitted bytes bounded.

The radio's capacity cannot be manufactured by a tunnel. "Better than the cell
connection" therefore means that URnetwork adds less delay, avoids redundant
recovery, protects interactive traffic from bulk traffic, recovers quickly from
short outages and path changes, and approaches the direct-link bulk goodput. It
may beat a direct application flow in loaded latency or completion time when its
scheduler and recovery make better use of the same link; it must not promise
more raw capacity than the link has.

"One bar" is a product scenario, not a repeatable network measurement. All
engineering decisions and release gates use measured rate, RTT, jitter, burst
loss, queue delay, outage, MTU, and path-change traces.

The tunnel remains IPv4-only. Do not enable, advertise, route, or locally
blackhole IPv6 until remote providers can route it.

## How to maintain this document

This file is part of the implementation, not a one-time proposal.

- `[x]` is implemented and covered by a deterministic test.
- `[~]` is implemented experimentally or has incomplete validation.
- `[ ]` is proposed.
- `[!]` is a confirmed correctness or deployment issue.

Every low-bar change must update the applicable phase, findings, and results in
this file. Record the exact client/server revisions, PERFVAR profile and seed,
transport configuration, command or result artifact, run count, and whether the
result is cold or warmed. Append results; do not replace an unfavorable run.
Lock release thresholds after the Phase 1 baseline so they cannot be moved to
fit a candidate.

## Physical Android device allowlist

LOWBAR physical validation uses exactly these two devices:

| Role | Serial | Model |
|---|---|---|
| `device-a` | `3B161FDJG001KT` | Pixel 8 Pro |
| `device-b` | `R5CX21FY6ND` | Galaxy S24 Ultra |

Preflight must find both serials in `adb devices -l` with state `device` and no
other serial. Drivers receive the serial explicitly; missing, unauthorized, or
offline devices invalidate the physical block rather than being substituted.
Public notes use the opaque roles, while the private run manifest keeps serials
for identity and reproducibility.

Verified 2026-09-04 with `adb devices -l`: exactly these two serials were
attached and online; no third device was admitted to the performance cohort.

## Decisions

1. **Transfer owns end-to-end tunnel delivery.** Every tunneled TCP Pack uses a
   Transfer ACK, including over direct and nominally reliable carriers. Carrier
   reliability ends with that connection generation and cannot prove delivery
   across a disconnect or route replacement. QUIC DATAGRAM avoids a redundant
   carrier payload retry for the common packet path; the bounded large-message
   stream remains an in-generation retry layer, never a replacement for
   Transfer commit.
2. **QUIC still uses transport ACKs.** QUIC packet ACKs, loss detection,
   congestion control, pacing, path validation, and cryptographic handshake are
   fundamental to QUIC and cannot be disabled. "QUIC with no ACKs" means no
   QUIC application-data retransmission, which is the property provided by
   DATAGRAM, not an ACK-free QUIC connection.
3. **Use a reliable control/fallback stream.** Authentication, capability
   negotiation, connection-scoped control, contract-only Packs, and routed
   frames that do not fit one live DATAGRAM use the reliable QUIC stream. Small
   complete tunnel messages use DATAGRAM.
4. **Use a bounded hybrid.** With the global 1,100-byte tunnel MTU and QUIC's
   safe 1,200-byte initial packet, an ordinary worst-case encrypted IP Pack does
   not fit one DATAGRAM and uses the reliable stream; smaller frames that fit
   use one DATAGRAM. Production never fragments one Transfer message across
   multiple lossy DATAGRAMs. Transfer sequencing and ACKs do not change with
   the carrier lane. Explicit two-fragment controls remain available for
   compatibility tests and measurement, not production selection.
5. **Receive admission follows the exact physical lane.** Shared callbacks and
   true datagram readers remain bounded, zero-wait, and counted on refusal. An
   H1, QUIC-stream, SCTP, or framed internal-TCP reader retains only its one
   already-read complete message while waiting for fixed queue capacity or
   lifecycle cancellation; dropping there manufactures loss above a reliable
   carrier. The final Client-to-Pack handoff uses the same exact lane metadata:
   reliable lanes wait within unchanged count/byte budgets, while H3 DATAGRAM,
   outer DNS datagrams, and native P2P datagrams never wait. A shared server
   callback that cannot propagate reliable backpressure retires its generation
   instead of silently skipping a frame. See
   [CODESTYLE.md](./CODESTYLE.md#receive-callbacks-and-reliable-carrier-backpressure).
6. **Preserve security and routing policy.** Transport changes do not weaken
   encryption, CFAA policy, SMTP policy, kill-switch behavior, provider
   eligibility, or route authentication.
7. **Keep fallback.** UDP can be blocked, rate-limited, or deprioritized by a
   carrier. Auto mode must retain the TLS WebSocket/H1 path and converge without
   repeatedly racing both transports over a scarce uplink.

## Current architecture and evidence

`TransportModeH3` is a custom protocol over QUIC, not HTTP/3 request semantics.
Legacy peers still serialize all platform messages on one reliable
bidirectional stream. New peers negotiate envelope version 2 in the
authenticated `Auth` exchange and split bounded routed Transfer frames onto
QUIC DATAGRAM while retaining that stream for authentication, liveness, and
larger routed frames. The production-selection stack is therefore transitional:

```text
inner application TCP/UDP
        |
IP mux + multi-TCP collapse prevention
        |
Transfer sequence, ACK, resend, and ordered receive
        |
H1: TLS/WebSocket/TCP     or     H3 legacy: one reliable QUIC stream
                              or H3 v2: control/large stream + Transfer DATAGRAM
        |
connect/provider
```

For an inner TCP flow, H1 can combine inner TCP recovery, Transfer recovery,
and outer TCP recovery. Current H3 replaces outer TCP with a reliable QUIC
stream but still combines inner TCP, Transfer, and outer-stream recovery.
Nested reliable transports can amplify retransmissions and queueing after loss;
the same class of TCP-over-TCP failure is documented for tunnel protocols in
[RFC 8229](https://www.rfc-editor.org/rfc/rfc8229.html#section-12.1).

Current code facts:

- H3 uses `quic-go`, one bidirectional control stream, and, only after explicit
  new/new negotiation, RFC 9221 DATAGRAM for complete routed Transfer frames.
  Either old endpoint stays on the existing stream path on the same connection.
- Production Auto orders H1 first, direct H3 second, DNS H3 third, and DNS pump
  fourth. H3 remains a fallback when H1 is unavailable; a restored H1 preempts
  lower-priority H3 and the H3 generation drains. Transport affinity can retain
  an eligible route only against transports at the same or lower priority; it
  cannot hide an available higher-priority H1 route. Explicit H3 selection
  bypasses the Auto ordering. When the process budget can fit H1 but not
  optional Auto H3, Auto reports a degraded H1-only status rather than opening
  H3 sockets. Production H3 reachability still depends on the UDP Proxy
  Protocol v2 ingress rollout.
- Legacy H3 writes ready-only batches of at most 16 messages / 64 KiB retained
  storage. Negotiated hybrid generations allocate that stream batch lazily only
  if a large routed frame selects the stream, and otherwise retain bounded
  DATAGRAM scratch and incomplete-message state.
- QUIC keepalive now owns connection liveness independently from the possibly
  blocked DATAGRAM writer. The retained application ping cadence still needs a
  radio-energy and bytes-on-wire experiment.
- Network-change `Kick` closes and redials instead of preserving a connection
  through QUIC migration.
- `Allow0RTT` is enabled. Authentication currently needs a replay-safety review
  before any credentials or state-changing control can be accepted as early
  data; TLS 1.3 explicitly requires replay defenses for 0-RTT
  ([RFC 8446](https://www.rfc-editor.org/rfc/rfc8446.html#section-8)).
- Transfer normally ACKs, orders, and retries Packs. Its cold resend interval is
  two seconds; sampled intervals use a bounded mean-RTT-derived estimator.
- A Transfer ACK now keeps reply affinity with the carrier that delivered the
  newest Pack covered by that ACK. Queue pressure cannot spill it onto a tied
  sibling; physical carrier withdrawal permits immediate fallback. A late
  retransmit below the cumulative head cannot move a newer H3 ACK back to H1.
  If an ordinary H1 Transfer RTO occurs while tied H3 remains healthy, only
  that affected ordered selector moves to H3; both transports remain active,
  and queue pressure alone is never failover evidence.
- Recovery now follows the lane that accepted each exact write, not the
  route-wide H3 capability. Only a successful DATAGRAM write consumes the
  unreliable flight and uses the two-second retry ceiling. A successful hybrid
  stream write remains Transfer-ACKed but leaves payload retry to QUIC for up
  to eight seconds; withdrawal of that exact route schedules immediate
  Transfer recovery on a replacement carrier. Adding a sibling route while the
  original remains active does not manufacture a duplicate.
- A normal Pack can coalesce at most two frames and 1,100 bytes of application
  messages. The global advertised IPv4 tunnel MTU is 1,100 bytes. Exact sizing
  produces 1,288 Transfer bytes (1,316 bytes with the H3 envelope) for one
  worst-case packet, so QUIC's safe 1,200-byte initial packet selects the
  reliable stream. A single-packet inner MTU of 944 bytes (934 for two
  coalesced packets totaling the MTU) is the exact one-DATAGRAM boundary. The
  observed 1,515-byte opening item is contract-only and also uses stream.
- IPv4 UDP now remains correct at that product MTU. A QUIC Initial is at least
  1,200 UDP bytes, so the synthetic gVisor device stack emits two IPv4 fragments
  at an advertised 1,100-byte TUN MTU. NAT send shards pin all fragments of one
  datagram together and reassemble them before UDP parsing. Device and provider
  security gates independently inspect the complete UDP datagram, then Transfer
  retains the original ordered fragments so no tunnel packet exceeds the
  product MTU. Provider replies are emitted as real IPv4 fragments rather than
  several corrupt independent UDP datagrams. Fragmented TCP is rejected at both
  security boundaries—including TCP/25—rather than letting a partial transport
  header bypass SMTP or CFAA policy; ordinary TCP is expected to segment at its
  negotiated MSS. Each gate or NAT shard retains at most 16 incomplete
  datagrams, 64 fragments per datagram, and 64 KiB for a hard 15-second lifetime;
  overlaps and conflicting final boundaries discard the whole datagram. The
  reassembler accepts the DF+fragment form emitted by gVisor and clears fragment
  flags on the canonical policy copy. Before retained fragments cross parallel
  H3 stream/DATAGRAM routes, each completed group receives a fresh nonzero IPv4
  identification and corrected checksums; this prevents two gVisor ID-zero
  groups from collapsing into one reassembly identity after interleaving. The
  provider UDP read buffer is a bounded 2 KiB so an ordinary QUIC datagram is
  complete before packetization. IPv6 remains unadvertised and oversized IPv6
  UDP is rejected instead of being split into application-visible datagrams.
  Real iOS and Android behavior when an app sends QUIC's 1,200-byte minimum
  through a 1,100-byte VPN route remains an explicit real-device follow-up; the
  synthetic result is not evidence that a platform kernel will fragment rather
  than return a message-too-large error.
- The production default still sends ordinary IP traffic to one provider on
  one ordered Transfer lane. An opt-in, version-negotiated prototype can hash
  exact directional IP five-tuples over 1, 4, or 8 bounded data lanes while
  retaining lane 0 for compatibility/control. A peer must advertise support in
  an ACK from its live lane-0 sequence before implicit data lanes activate;
  old peers therefore stay on lane 0. The lane is Transfer identity, not a QUIC
  stream id, and follows sequence, ACK, retransmit, reply, encryption, contract,
  replay, and teardown paths. Nonzero lanes share fixed lazy byte budgets and
  divided channel headroom, so enabling eight lanes does not multiply the
  configured memory ceiling. Active data lanes pin their negotiating lane-0
  lifetime; losing that base sequence clears capability and cancels its data
  lanes rather than silently continuing with stale negotiation.
- P2P publishes its physical lanes separately. Legacy SCTP is reliable and
  propagates fixed-capacity backpressure from its dedicated reader; native
  RTP/SRTP is unreliable and enters the existing zero-wait 256-message /
  256-KiB queue. Auto send policy activates the bounded unreliable Transfer
  flight only while the native fast path can be selected; LegacyOnly does not
  pay that flight limit. Transfer ACK recovery remains end-to-end on both
  lanes because route replacement can invalidate carrier-local delivery. The
  native data flight remains 240 KiB, leaving 16 KiB inside the unchanged queue
  ceiling for cumulative ACK, compact-recovery, contract, and probe traffic.
  Exact receive-lane metadata follows the selected route into the shared Client
  Pack admission, so SCTP waits within the existing 256-message / 256-KiB
  retained-Transfer ceiling and native datagrams remain zero-wait.
- Transfer queues are sized in bytes, but at a 64 kbit/s uplink even 1 MiB is
  more than two minutes of serialization. A memory-safe queue can still be a
  catastrophic latency queue.

Existing PERFVAR measurements are evidence, not yet a complete mobile release
baseline. See
[PERFVAR.md](../server/connect/perfvar/PERFVAR.md) and
[MEASUREMENTS.md](../server/connect/perfvar/MEASUREMENTS.md).

| Existing result | Observation |
| --- | --- |
| Fixed TCP window, 256 KiB to 2 MiB | 41.045 to 402.276 Mbit/s in the recorded clean throughput case; retained 2 MiB was the useful plateau candidate. |
| Whole five-tuple groups vs singleton packets | 336.497 to 416.331 Mbit/s (+23.73%); send time fell 17.34% and carrier bytes fell 3.98%. Preserve grouping as long as it adds no batching wait. |
| Clean mobile-surrogate upload | P2P fast 453.382, H1 412.005, legacy 383.776, H3 213.393 Mbit/s. Current H3 was 48.2% below H1 in this case, so QUIC alone is not the answer. |
| Provider TCP timestamps at 1 s RTT / 64 KiB upload | 25.087 s to 3.081 s (8.14x). Endpoint TCP behavior can dominate tunnel results. |
| Provider window growth | Raising the provider maximum helped warmed tests; raising TUN capacity to 16 MiB overflowed the 4096-packet queue and timed out. More buffering is not a general low-bar fix. |
| Mixed Auto, 256 KiB warmed upload at 1/0.25 Mbit/s | A refreshed five-run current-tree matrix measured Auto at 14.983 s / 876,468 B, forced H3 at 15.268 s / 818,234 B, and forced H1 at 30.285 s / 1,976,566 B using all correctness-valid samples. Auto and H3 were about 50% faster and 56--59% lower-byte than H1. |
| Repaired 1,100-MTU Auto download, 256 KiB latency under load | Before ACK carrier affinity, three exact runs had 17.007 s tunneled, 22.749 s carrier, and 1,437,926 B wire medians. With newest-covered-Pack ACK affinity and selector-local H1 timeout failover, three exact runs measured 13.255 s, 18.727 s, and 1,216,878 B: 22.1% faster tunneled completion, 17.7% faster carrier completion, and 15.4% fewer wire bytes. Forced H3 remains faster at 9.760 s tunneled and 851,562 B wire median. |
| Same-harness Auto upload ACK-affinity control | Three exact ACK-affine runs measured 15.967 s and 905,198 B medians versus 17.267 s and 922,338 B with only ACK affinity disabled: 7.5% faster and 1.9% fewer wire bytes. The loaded-probe delivery median also rose from 93.1% to 94.7%. |
| Native P2P fast, same workload/profile | The initial current-tree baseline delivered only 1/5 exact payloads: 44.646 s / 561,768 B median, with small receive-queue drops in four runs. Publishing unreliable carrier semantics plus the byte-bounded receive queue delivered 5/5 in 22.921 s / 478,585 B with zero receive-queue drops: 48.7% faster and 14.8% lower-byte. |

The authoritative five-run Wi-Fi/LTE/mobile-poor campaign is incomplete. No
transport should ship from the historical clean results alone.

## Target recovery architecture

```text
                         one QUIC connection
                        /                   \
reliable control stream                    QUIC DATAGRAM
auth + capabilities              Transfer Pack / Transfer ACK / NoAck
QUIC retransmits control             QUIC does not retransmit payload
                                             |
                              Transfer is the end-to-end commit authority
```

QUIC streams are ordered byte streams. Different streams avoid transport
head-of-line blocking between streams, but bytes within a stream remain
ordered. QUIC DATAGRAM adds unreliable message delivery to the same connection
([RFC 9000](https://www.rfc-editor.org/rfc/rfc9000.html),
[RFC 9221](https://www.rfc-editor.org/rfc/rfc9221.html)). A packet containing a
DATAGRAM frame is ACK-eliciting for QUIC congestion control, but the DATAGRAM
frame is not retransmitted and its packet ACK does not prove application
delivery. Transfer's end-to-end ACK remains authoritative.

| Recovery mode | Carrier | QUIC payload retry | Transfer payload retry | Intended traffic |
| --- | --- | --- | --- | --- |
| `control_stream` | Reliable stream | Yes | Transfer ACK also applies to contract Packs; connection-only auth/ping has no Transfer retry | Auth, capabilities, contract-only control, connection control |
| `transfer_datagram` | DATAGRAM | No | Yes | Default reliable tunnel Packs and Transfer ACKs |
| `datagram_noack` | DATAGRAM | No | No | Traffic explicitly classified as NoAck / stale-is-useless |
| `quic_stream` | Reliable stream data lane | Yes | Yes for TCP and every other ACK-required Transfer Pack | Contract-only control and routed frames above the bounded packet-lane threshold |

Inner endpoint TCP still retransmits because it is the end-to-end transport
between the application and destination. The goal is not to alter endpoint TCP;
it is to prevent redundant copies of the same inner segment from being injected
while Transfer already owns its delivery. Multi-TCP collapse prevention is the
coordination point for that ownership.

### DATAGRAM sizing and fragmentation

QUIC DATAGRAM cannot fragment an application datagram. Its sender must honor the
peer's negotiated maximum DATAGRAM frame size and the current path payload
ceiling. QUIC itself starts with a minimum 1200-byte UDP payload capability, but
that is not 1200 bytes of Transfer content after QUIC framing.

The versioned envelope has bounded Transfer-aware fragmentation rather than
relying on IP fragmentation:

- derive a conservative fragment payload from the active QUIC path and
  negotiated DATAGRAM maximum;
- carry version, Transfer message identity, fragment index/count, and bounded
  integrity-checked length;
- cap fragments per message, reassembly bytes per peer and globally, and
  reassembly lifetime;
- accept duplicate and reordered fragments without extending their lifetime;
- never allocate from an attacker-controlled declared total before validating
  all limits;
- keep retransmission ownership in Transfer. Prefer making fragments selectable
  Transfer recovery units if measurements show that resending a whole Pack for
  one missing fragment wastes the constrained uplink;
- use Datagram Packetization Layer PMTU Discovery rather than ICMP assumptions
  ([RFC 8899](https://www.rfc-editor.org/rfc/rfc8899.html)); and
- benchmark one, two, three, and four-fragment Packs against the separate-stream
  candidate. Production permits one fragment only; the explicit fragmented
  controls keep multi-fragment framing and reassembly covered without selecting
  it for live traffic.
  `quic-go` documents that its DATAGRAM path is not optimized for high
  throughput, so this is a measurement gate, not an assumption
  ([quic-go DATAGRAM documentation](https://quic-go.net/docs/quic/datagrams/)).

The tunnel MTU is now globally 1,100 by product decision. Its exact worst-field
encrypted Transfer growth is pinned beside the carrier: one full 1,100-byte
packet and two coalesced half-size packets would require two DATAGRAMs at the
safe initial QUIC size and therefore select stream. Smaller complete messages
stay on the one-DATAGRAM lane. The former 1,440-byte packet exceeds even the
optimistic hybrid threshold.

### Hybrid selection

Size alone is not enough to choose DATAGRAM versus stream. Selection order is:

1. required recovery semantics;
2. logical Transfer lane;
3. current path DATAGRAM ceiling and fragment count; then
4. a measured stream threshold, if a stream candidate remains beneficial.

Carrier selection never changes Transfer sequence identity, acknowledgement,
or replay ownership. A streamed sequence item can therefore delay later
DATAGRAM items at the logical receiver, but it cannot create a silent delivery
hole when that stream connection disappears. Keep stream selection rare and
bounded, measure its nested in-generation retry cost, and safely fall back to
the legacy stream when either peer lacks the negotiated hybrid version.

## Receive-path correctness

Mechanical head-of-line blocking and logical ordered-sequence blocking are
different problems.

### Mechanical blocking

The shared receive pump must never wait for a destination queue or a retiring
worker. This is now corrected:

- [x] `Client.run` calls `receiveBuffer.Pack(receivePack, 0)`.
- [x] Inbound ACK handoff calls `sendBuffer.Ack(..., 0)`.
- [x] A zero-timeout Pack that encounters a retiring/replaced receive sequence
  cancels the old worker and drops instead of calling `WaitForExit` inline.
- [x] Pack/byte and ACK handoff drops are counted with power-of-two diagnostics.
- [x] Deterministic regressions fill Pack and ACK queues ahead of an unrelated
  source and verify that the unrelated source is delivered. A separate test
  pins a retiring generation whose exit never arrives and verifies immediate
  return.

Relevant tests are in
[`transfer_callback_backpressure_test.go`](./transfer_callback_backpressure_test.go):

- `TestClientReceivePackHandoffDoesNotBlockUnrelatedSource`
- `TestClientReceiveAckHandoffDoesNotBlockUnrelatedSource`
- `TestReceiveSequenceReplacementDropsWithoutWaiting`
- `TestReceiveSequenceClosingGenerationDropsWithoutWaiting`

Remaining work:

- [x] Export aggregate Pack/byte and ACK handoff drops into PERFVAR carrier
  observations for the device, provider, and stream-P2P intermediary Clients.
  Measurement boundaries read the existing lock-free `Client.ReceiveStats()`;
  no metrics callback or receive-path wait was added. Client-generation changes
  are explicit, retain both raw lifetime endpoints, and are never subtracted as
  if lifetime counters were continuous.
- [x] Audit forward, WebRTC signal/data, stream, device TUN, provider NAT, and
  RPC receive handoffs against the same rule. There are no production Client
  forward subscribers. Signal shards and the native RTP fast path drop on full;
  stream lifecycle work is bounded/asynchronous; provider datagrams use bounded
  zero-wait sender shards. Pion ICE publication now uses the receive-side
  zero-timeout marker. A saturated reliable RPC substream closes that RPC
  generation instead of blocking the shared websocket reader; it cannot skip a
  byte fragment and continue safely. The synchronous device-TUN injection and
  provider local-NAT TCP socket return remain the only documented exceptions,
  with end-to-end and dedicated-flow regressions.
- [x] Add structural regressions for the production Client receive-subscriber
  inventory, the absence of forward subscribers, direct blocking calls/channel
  sends, literal zero timeouts, and the SDK-owned provider subscriber. A new or
  changed callback now fails until its boundary is audited explicitly.

### Logical sequence blocking

`ReceiveSequence` intentionally holds a later reliable Pack behind a missing
`nextSequenceNumber`. That is correct inside one recovery lane. It becomes
cross-flow head-of-line blocking when many independent five-tuples share the
lane.

- [~] Measure gap delay by inner five-tuple and identify which current
  `TransferKey` combinations share a sequence. Deterministic loss now proves
  lane-1 blackholing does not stop lane 2; production-trace attribution remains.
- [x] Prototype a bounded number of logical data lanes: one control lane plus
  4 and 8 five-tuple-hash data-lane candidates. Do not create an unbounded
  sequence per flow.
- [x] Add the lane as a logical routing key following every producer and
  consumer rule in `CODESTYLE.md`; never use a transport-local QUIC stream ID.
- [x] Share byte budgets across lanes so lane count does not multiply memory.
  Disabled clients allocate no lane budget; enabled clients create one shared
  send and receive budget lazily, and channel capacity is divided across the
  maximum supported lane count.
- [x] Verify that loss in one lane does not delay another, and that in-lane
  ordering, contract state, encryption role/companion, reform, replay, and
  fallback compatibility remain correct. The lossy negotiated-pair regression
  holds lane 1, delivers and ACKs lane 2, then releases lane 1; codec-equivalence,
  legacy-peer, exact-ACK-routing, capability-lifetime, budget, and race tests
  cover the adjacent boundaries.

Queue admission must happen before Transfer assigns a sequence number. Dropping
an already-numbered item creates the very gap the scheduler is meant to avoid.

## Multi-TCP collapse prevention

The current collapse prevention is a useful starting point. It is enabled by
default and applies to every tunneled TCP route because TCP always requires
Transfer ACK recovery,
always passes SYN/RST and sequence/ACK/window progress, includes FIN in the
sequence edge, passes zero-window changes, suppresses identical retransmits,
and releases a held retransmit after a fixed 1500 ms. Direct TCP no longer
bypasses collapse; only non-TCP traffic can use the NoAck policy.

The next version should bind collapse to explicit recovery ownership rather
than only an ACK-required boolean:

- [ ] `transfer_datagram`: collapse eligible; link a held inner retransmit to
  the outstanding Transfer item that represents it.
- [ ] `quic_stream`: quantify simultaneous QUIC and Transfer retry while
  preserving Transfer commit across connection loss.
- [ ] `datagram_noack`: bypass collapse (TCP is never in this class).
- [ ] Replace the fixed hold with a bounded adaptive value derived from the
  Transfer loss/RTO state. Release immediately when admission, route selection,
  or write fails.
- [ ] Record suppressed segments, timer releases, failure releases, bytes
  avoided, and whether a released copy preceded successful progress.
- [ ] Preserve and extend the SYN/RST, FIN, zero-window, retransmission, route
  change, and direct-recovery regression suite.

This coordination does not make Transfer more reliable than inner TCP. It
prevents the tunnel from carrying redundant copies while Transfer is already
recovering a Pack.

## Queueing and scheduling

On a low-rate uplink, byte bounds alone are insufficient. Queue bounds must also
be expressed as estimated serialization time and packet age.

- [ ] Add a bounded flow-fair scheduler before Transfer sequence admission.
  Start with deficit round robin / FQ-CoDel-derived behavior, not a single FIFO;
  [RFC 8290](https://www.rfc-editor.org/rfc/rfc8290.html) describes the
  flow-queue and controlled-delay principles.
- [ ] Reserve small capacity for connection control, DNS, TCP SYN/FIN/RST, and
  small interactive traffic. Traffic class is a scheduling hint, never a
  correctness or security decision.
- [ ] Use control, interactive, default, and bulk service classes with a shared
  global byte ceiling. Per-flow and per-class ceilings must not multiply the
  process memory budget.
- [ ] Do not wait to form a batch. Group messages already ready at the same
  instant, retaining the measured carrier-byte and syscall win without adding a
  batching timer.
- [ ] Once reliable data is admitted and numbered, retain it until ACK,
  terminal failure, or connection-generation replay policy resolves it.
  NoAck/stale-is-useless datagrams may expire by age.
- [ ] Derive adaptive queue targets from estimated rate and a target queue delay
  while retaining hard byte and packet caps. A bad estimator must fail bounded.

## Transfer recovery work

Transfer remains the key differentiator, so tune it from measured low-bar
behavior rather than attempting to remove it.

- [~] Instrument original sends, ambiguous retransmit samples, selective ACKs,
  cumulative ACKs, gap duration, RTO fire, retry count, and useful recovery by
  lane. The database-free carrier A/B now records per-Pack attempt timelines,
  maximum retry gap/attempt count, Pack write span, and selective-gap,
  ACK-tail, and cumulative-probe writes. Lane attribution and useful-recovery
  classification remain.
- [ ] Compare the current mean-RTT estimator with an SRTT/RTTVAR estimator and
  Karn-style exclusion of ambiguous retransmit samples. RFC 6298 is the
  baseline, not a mandate to copy TCP constants unchanged
  ([RFC 6298](https://www.rfc-editor.org/rfc/rfc6298.html)).
- [ ] Measure a lower cold-start RTO only with safeguards against burst loss and
  asymmetric cellular delay. The current two-second cold retry is visibly long
  for an isolated loss but may prevent waste on a paused radio.
- [~] Tune selective-gap probing so a lost cumulative ACK cannot leave every
  item paused for the full selective-ACK window. A reordering-safe threshold of
  three later selective ACKs, four-hole burst bound, minimum-live-RTT probe,
  and once-per-item guards are implemented with deterministic regressions.
  They reduce the measured DATAGRAM tail but cannot compensate for the current
  oversized initial QUIC flight; keep this experimental until flight control
  and the final focused race run pass.
- [ ] Measure ACK compression on constrained uplinks. Prefer cumulative and
  selective information per byte; do not let ACK compression delay recovery of
  a sparse interactive Pack.
- [ ] Make recovery state survive a healthy transport path change where safe,
  rather than treating every `Kick` as loss of all connection context.

## QUIC work beyond DATAGRAM

- [ ] Capture qlog and correlate QUIC packet loss, PTO, congestion window,
  pacing, path validation, and migration with Transfer retries and inner TCP
  retransmits.
- [ ] Enable ECN where the platform and path support it, with validation and
  automatic fallback as required by QUIC.
- [ ] Use DPLPMTUD and expose the effective DATAGRAM payload ceiling.
- [ ] Replace the unconditional five-second application ping with a measured
  policy based on NAT lifetime, platform suspension, radio promotion cost, and
  idle reconnect latency. Do not run two keepalive mechanisms.
- [ ] Preserve session tickets across compatible load-balancer backends and
  measure cold handshake, resumed handshake, and path-change reconnect.
- [ ] Make authentication replay-safe and wait for 1-RTT for state-changing
  control unless a specific idempotent 0-RTT design is proven.
- [ ] Test QUIC connection migration and NAT rebinding before replacing the
  current close/redial path. Generic UDP load balancing may route a rebinding to
  another backend even though QUIC identifies connections independently of the
  five-tuple.
- [ ] Retain an H1 fallback and cache UDP-blocked evidence for a bounded period
  so Auto does not spend scarce bytes repeatedly probing an unusable mode.

`quic-go` currently exposes its RFC 9002 Reno controller rather than a stable
pluggable congestion-controller API
([quic-go congestion-control documentation](https://quic-go.net/docs/quic/congestion-control/),
[RFC 9002](https://www.rfc-editor.org/rfc/rfc9002.html)). Do not fork it as the
first optimization. Establish the DATAGRAM, scheduling, and recovery baseline
first. Then evaluate BBR, CUBIC, Sprout, or another cellular-oriented controller
only through fairness, outage, and loaded-latency A/B tests. BBR and Sprout are
research candidates, not preselected answers
([BBR paper](https://research.google/pubs/bbr-congestion-based-congestion-control/),
[Sprout paper](https://www.usenix.org/system/files/conference/nsdi13/nsdi13-final113.pdf)).

ACK_FREQUENCY may eventually reduce return-path QUIC ACK bytes, but it remains
an evolving draft and can reduce loss responsiveness. It is not a production
dependency
([current IETF draft](https://datatracker.ietf.org/doc/html/draft-ietf-quic-ack-frequency-14)).

## Measurement program

### Compared modes

Every candidate uses the same endpoint, provider, route, security policy, and
profile:

1. direct network without URnetwork, where the harness supports it;
2. current H1 TLS/WebSocket;
3. current custom QUIC reliable-stream H3;
4. QUIC DATAGRAM + Transfer fragmentation;
5. DATAGRAM + bounded logical lanes and scheduler; and
6. the explicit large-message stream hybrid, only after the DATAGRAM baseline.

### Profiles

Retain the existing clean, Wi-Fi-good, LTE, mobile-poor, and single-region
profiles. Add a curated stress grid rather than an unreviewable full Cartesian
product:

| Dimension | Required points |
| --- | --- |
| Down/up rate | 256/64 kbit/s, 1/0.25 Mbit/s, 5/1 Mbit/s, existing 10/2 and 50/10 Mbit/s |
| Base RTT | 120, 300, 800 ms, plus existing regional cases |
| Jitter | 25, 100, 300 ms |
| Burst loss | 0.5%, 2%, 5%, with independent and two-state burst models |
| Queue delay | 100, 500, 2000 ms |
| Disruption | 1/3/10 s outage, NAT rebind, address change, UDP blocked |
| Outer MTU | 1280, 1400, 1500 bytes and an MTU reduction during a connection |
| Rate change | fast-to-slow, slow-to-fast, and oscillating radio capacity |

Treat these as engineering stress profiles until field traces exist. Collect
privacy-safe physical traces containing timings and aggregate link/path
properties, never payload or destination identity. Make traces deterministic
and replayable in PERFVAR.

### Workloads

- sparse DNS plus one small HTTPS request;
- cold and warmed web page object sets;
- interactive request/response and RPC;
- one inner TCP upload/download;
- 4/16/64 parallel inner TCP flows;
- latency-under-load with a bulk upload and download;
- inner QUIC/UDP and NoAck traffic;
- SMTP negotiation and TLS flows already represented by product policy;
- mixed web, mail, and blocked traffic from the DeviceLocal/DeviceRemote
  synthetic path;
- short outage, handover, NAT rebind, MTU change, and UDP-blocked fallback; and
- long idle followed by one interactive request.

### Metrics

- DNS completion, connect time, TLS time, first byte, full completion;
- p50/p90/p95/p99 latency and maximum stall by workload and inner flow;
- goodput, completion ratio, and time spent below an application-useful rate;
- queue bytes, packets, oldest age, estimated serialization delay, and drops by
  scheduler class/lane;
- Transfer sends, ACKs, retries, RTO, gap time, and handoff drops;
- QUIC loss/PTO/cwnd/pacing/DATAGRAM drops and stream retransmitted bytes;
- inner TCP retransmits and collapse suppress/release outcomes;
- carrier bytes in each direction and useful-byte efficiency;
- process RSS, retained pool/queue bytes, allocation spikes, CPU, wakeups, and
  mobile energy proxy; and
- reconnection, migration, fallback, and time-to-first-useful-packet after a
  disruption.

Use at least five measured runs per candidate/profile cell after a separately
reported warm-up. Pin revisions, random seeds, and CPU/resource limits. Report
all runs plus median and tail, not only the best. Run the canonical commands in
`PERFVAR.md`; add any new command there and link its output here.

### Real-device memory-stability focus

The deterministic Connect and SDK memory tests prove important ownership and
queue bounds, but they do not include the complete mobile footprint: Network
Extension/runtime overhead, allocator retention, TLS and QUIC state, kernel
socket buffers, radio/network lifecycle, or operating-system termination
policy. Simulator and Go-heap results are guardrails, not release evidence for
this campaign.

Run in this order:

1. the lowest-memory supported physical iPhone, with the packet-tunnel
   extension as the measured process;
2. the lowest-memory supported physical iPad and a current iPhone as controls;
3. the lowest-memory supported Android device in always-on and lockdown VPN
   configurations; and
4. representative macOS, Windows, and Linux service hosts after the mobile
   gates pass.

For every run, record a cold pre-connect baseline, warmed connected plateau,
peak burst, immediate post-burst value, and quiescent values at 1, 5, and 15
minutes. Preserve the operating-system termination or jetsam report with the
traffic trace, policy transitions, device model, OS build, app/extension build,
available-memory tier, and whether the run was cold or warmed.

Required observability:

- [~] Record the OS-reported extension/process physical footprint and
  resident/PSS memory, system memory pressure, and termination reason alongside
  Go `HeapAlloc`, `HeapInuse`, `HeapSys`, `StackSys`, GC count, and pause time.
  A falling Go heap does not prove that socket, kernel, or runtime memory was
  returned. Android PSS, detailed Go allocator/GC fields, and exit reasons are
  now captured; Android pressure state and physical iOS extension evidence are
  still missing.
- [~] Record goroutines, file descriptors, TCP/UDP socket count, active QUIC
  connections and streams, MessagePool outstanding buffers/bytes, bounded queue
  count/bytes/oldest age, and fragment, DNS-combine, and H3-reassembly retained
  bytes. Goroutines, descriptors, and pool outstanding/retained/capacity bytes
  are now captured on Android; the remaining native/socket/queue classes are
  not.
- [ ] Snapshot all `PlatformTransportBudget` counters: total/used bytes,
  transport slots, pending H1 claims, cumulative reserved/released bytes, and H3
  preemptions. Also record configured, eligible, available, and elected
  transports from `TransportStatus`; eligibility must not be inferred from the
  active carrier alone.
- [ ] Add missing counters before interpreting a run. In particular, do not
  infer released affinity, DNS, QUIC, or socket state only from RSS, which can
  remain high after correct logical cleanup.

Exercise these transport and budget sequences on each mobile memory tier:

- [~] Explicit H1 as the control; enough-memory Auto with healthy H1; Auto with
  H1 unavailable so direct H3 must start; H1 restoration so H1 is elected and
  every lower-priority H3 generation drains by its bounded timeout; and
  explicit H3, because H1-first Auto no longer exercises H3's worst resident
  footprint while H1 is healthy. Both Android underlays completed H1, Auto, and
  explicit H3, but the H1-unavailable/restoration and physical-iOS cells remain.
- [~] Repeated deterministic `H1 -> Auto -> H3 -> Auto` transitions during live
  bidirectional traffic. The final Auto must rediscover its policy without the
  explicit-H3 step acting as a hidden reset. Repeat transitions across app and
  extension restart, not only within one process generation. Both Android
  devices completed the in-process sequence and a live underlay swap; restart
  cycling and iOS remain.
- [ ] Run Auto immediately below, at, and above the H1-plus-H3 admission
  boundary. H1 must win when both do not fit. Low-memory Auto must report
  degraded H1-only eligibility and retain no H3 sockets or QUIC state; explicit
  H3 must still honor the user's H3 choice rather than silently running H1.
- [ ] Run foreground client and provider transports together, with several
  windows claiming the aggregate budget in different construction orders.
  Cancel queued claims and force foreground/background H3 preemption. Used
  bytes, slots, and pending H1 claims must return to their expected values after
  every claimant closes.
- [ ] Create affinity on several transports, then introduce a higher-priority
  transport, withdraw routes, change policy, and tear down the generation.
  Affinity may keep a same- or lower-priority choice but cannot exclude the
  higher-priority carrier, and its set must not grow across identical cycles.

Focus packet-pressure testing on the native/extension boundary, where a valid
Go queue bound can still coexist with retained native buffers or blocked
threads:

- [ ] Drive multiple concurrent bidirectional floods using both many small
  packets and near-MTU packets, inner TCP and UDP/QUIC, and fast and deliberately
  paused/slow destinations. Include receive loss, a UDP blackhole, and recovery
  while the flood is still active.
- [~] Verify a stable memory plateau rather than merely eventual completion.
  Goroutines must not scale per packet; no state lock may be held across a
  blocking sender boundary; route publication, status delivery, control/ACK
  work, and an unrelated interactive flow must continue within their bounds.
  Count overload drops/refusals explicitly instead of allowing an unbounded
  queue or a blocked receive callback. Two Android devices now return to a
  21.7--21.9-MiB disconnected Go-runtime band after real H3/P2P bursts, but the
  parallel native-boundary flood and physical-iOS plateau remain.
- [ ] Use SDK
  [`TestDeviceLocalParallelPacketFloodMemoryBounded`](../sdk/device_packet_flood_memory_test.go)
  as the deterministic ownership/locking guard, then reproduce its parallel
  stream shape through the physical packet tunnel. That test cannot prove the
  extension's native, socket, QUIC, or kernel-memory behavior.

Exercise allocation cliffs and retained-state cleanup directly:

- [ ] Confirm the quality-window target remains six and every multi-client
  expansion pass creates at most four candidates. Repeatedly expand, contract,
  reconnect, reject surplus candidates, and churn providers while measuring
  each step's peak. Closed clients must release routes, affinity entries,
  sockets, goroutines, callbacks, and transport-budget reservations before the
  next expansion wave.
- [ ] Send H1 WebSocket messages at the maximum accepted size and one byte over;
  drive DNS combine at its per-message, per-address, and global caps using many
  unique addresses and expiry; and fill H3 DATAGRAM/stream queues, reassembly
  budget, socket buffers, and QUIC receive windows. Refusal and cleanup must be
  bounded and visible.
- [ ] Repeat cold start, authentication failure/reconnect, burst/quiescence,
  long idle then burst, Wi-Fi/cellular/airplane transitions, path and MTU
  changes, screen lock, background/foreground, extension stop/start, and app
  process death. Include many short cycles to expose generation leaks and a
  long soak to expose slow accumulation.

Acceptance criteria:

- no jetsam/OOM, watchdog termination, deadlock, livelock, or receive-path
  blocking;
- sustained load reaches a bounded plateau and identical lifecycle cycles do
  not show monotonic growth;
- after quiescence, physical footprint returns to a pre-registered warmed
  baseline band, chosen before evaluating the candidate rather than adjusted to
  fit it;
- after teardown, pool ownership, queues, routes, affinity entries, sockets,
  goroutines, budget used/pending claims, and transport status return to the
  expected steady or zero state; and
- overload drops are allowed only when bounded and counted, with exact traffic,
  policy, and higher-priority H1 behavior preserved.

### 2026-08-21 two-device Android campaign and iOS-budget proxy

This campaign exercised current-source Android builds on a Pixel 8 Pro
(Android 17/API 37) and Galaxy S24 Ultra (Android 16/API 36), both with Chrome
151. Android was f9015be56afb, Connect was 4f3f017f5448, and SDK was
3c2d56b47155. Device serials, carrier/subscriber data, addresses, DNS answers,
and credentials were not retained. The first process used Android's previous
64-MiB Go soft process limit plus the existing 20-MiB per-DeviceLocal target.
The A/B process changed only the Android process cap to the iOS value of 32 MiB;
the device target remained 20 MiB.

This is useful iOS-pressure evidence, not a substitute for an iPhone Network
Extension run. Android still uses GOGC 50 while iOS uses GOGC 10, Android's
allocator/process composition and memory-pressure callbacks differ from iOS,
and debug instrumentation measures the whole Android app rather than an iOS
packet-tunnel extension. Both phones were USB-powered at 100%, so battery and
release-gate power conclusions are invalid. The Pixel's cellular signal level
was 0 throughout its cellular cells; the Galaxy was generally level 2--4.
Thermal status remained 0. The phones have roughly 11.3 and 10.8 GiB of RAM,
so this also does not close the lowest-memory-device cohort.

The production Chrome harness was restarted for every cell, warmed separately,
then required two stable DevTools probes five seconds apart. Each standard cell
used five uncached Wikipedia navigations and five streamed 1-MiB Cloudflare
downloads. Measured failures were retained and stopped that benchmark rather
than being retried away. Direct required no VPN; tunneled cells required the
correct Wi-Fi/cellular underlay, active IPv4-only VPN, and exact SDK packet
movement on the selected carrier. Across the 64/20 cohort, all 3,313 samples in
24 captures were eligible. Across the 32/20 cohort, all 1,687 samples in 14
captures were eligible. No capture recorded a thermal event or route
invalidation.

The 64/20 process ran H1 -> Auto -> H3 -> Auto first with Pixel/Wi-Fi and
Galaxy/cellular, then repeated the sequence after swapping the underlays:

| Device / underlay | Mode | Wikipedia median load | 1-MiB median | Outcome |
| --- | --- | ---: | ---: | --- |
| Pixel / Wi-Fi | H1 | 390.6 ms | 1.66 Mbit/s | 5/5 + 5/5 |
| Pixel / Wi-Fi | Auto 1 | 657.6 ms | 2.53 Mbit/s | 5/5 + 5/5 |
| Pixel / Wi-Fi | H3 | 540.8 ms | 5.18 Mbit/s | 5/5 + 5/5 |
| Pixel / Wi-Fi | Auto 2 | 437.2 ms | 2.52 Mbit/s | 5/5 + 5/5; fast.com passed |
| Galaxy / cellular | H1 | 1,338.3 ms | 3.64 Mbit/s | 5/5 + 5/5 |
| Galaxy / cellular | Auto 1 | 1,720.8 ms | 0.66 Mbit/s | 5/5 + 5/5 |
| Galaxy / cellular | H3 | 5,489.8 ms | 0.21 Mbit/s | 5/5 + 5/5 |
| Galaxy / cellular | Auto 2 | 1,799.6 ms | 0.74 Mbit/s for two samples | Pages 5/5; fetches 2/5, then Failed to fetch; two were not attempted |
| Pixel / cellular | H1 | 2,269.4 ms | 3.41 Mbit/s | 5/5 + 5/5 |
| Pixel / cellular | Auto 1 | 1,190.4 ms | 0.59 Mbit/s | 5/5 + 5/5 |
| Pixel / cellular | H3 | 2,482.1 ms | 0.35 Mbit/s | 5/5 + 5/5 |
| Pixel / cellular | Auto 2 | 1,124.0 ms | 2.86 Mbit/s | 5/5 + 5/5; fast.com passed |
| Galaxy / Wi-Fi | H1 | 457.5 ms | 7.18 Mbit/s | 5/5 + 5/5 |
| Galaxy / Wi-Fi | Auto 1 | 2,401.2 ms | 1.81 Mbit/s | 5/5 + 5/5 |
| Galaxy / Wi-Fi | H3 | 663.9 ms | 4.19 Mbit/s | 5/5 + 5/5 |
| Galaxy / Wi-Fi | Auto 2 | 429.8 ms | 2.62 Mbit/s | 5/5 + 5/5; fast.com passed |

Carrier counters, rather than the requested policy alone, proved every H1 cell
used only H1 bytes, every H3 cell used only H3 bytes, and Auto's measured bytes
used H1. The one 64/20 failure did not kill the process or invalidate the VPN;
an explicit H1 diagnostic on the same Galaxy cellular route worked. This is a
retained transient Auto/request-path failure, not an OOM result.

Direct and same-LAN P2P controls used the same browsers and sites:

| Device / role | Path | Wikipedia median load | 1-MiB median | Outcome |
| --- | --- | ---: | ---: | --- |
| Pixel | Direct Wi-Fi | 222.8 ms | 39.22 Mbit/s | 5/5 + 5/5 |
| Galaxy | Direct Wi-Fi | 141.4 ms | 28.40 Mbit/s | 5/5 + 5/5 |
| Pixel | Direct cellular | 486.0 ms | 1.97 Mbit/s | 5/5 + 5/5 |
| Galaxy | Direct cellular | 207.0 ms | 11.46 Mbit/s | 5/5 + 5/5 |
| Galaxy client / Pixel provider | P2P direction 1 | 422.9 ms | 3.60 Mbit/s | 5/5 + 5/5 |
| Pixel client / Galaxy provider | P2P direction 2 | 463.8 ms | 3.76 Mbit/s | 5/5 + 5/5 |

P2P was proven in both directions even though p2pOnlyExitCount remained zero:
direction 1 added 217,650/5,585,136 client P2P egress/ingress bytes and 283,950
provider P2P-ingress bytes; direction 2 added 241,579/3,257,850 and 244,179
bytes respectively. The matching provider remote traffic was about 6.2 MiB in
each direction. Direct Wi-Fi and cellular had 70/70 and 93--94/93--94 eligible
no-VPN samples per device. These are functional/current-route results, not a
claim that tunnel throughput approaches Direct on these uncontrolled links.

The 32/20 iOS-budget proxy retained the process across explicit H1, explicit
H3, post-H3 Auto, a six-second live Wi-Fi/cellular swap, and P2P role reversal:

| Device / underlay | Mode | Wikipedia median load | 1-MiB median | Outcome |
| --- | --- | ---: | ---: | --- |
| Pixel / Wi-Fi | initial Auto | 576.0 ms | 2.61 Mbit/s | 5/5 + 5/5 |
| Galaxy / cellular | initial Auto | 541.9 ms | no successful sample | Pages 5/5; first fetch failed; fast.com then passed |
| Pixel / Wi-Fi | H1 | 565.6 ms | 1.56 Mbit/s | 5/5 + 5/5 |
| Galaxy / cellular | H1 | 617.9 ms | 3.58 Mbit/s | 5/5 + 5/5 |
| Pixel / Wi-Fi | H3 | 1,008.2 ms | 1.44 Mbit/s | 5/5 + 5/5 |
| Galaxy / cellular | H3 | 846.9 ms | 2.34 Mbit/s | 5/5 + 5/5 |
| Pixel / Wi-Fi | post-H3 Auto | 420.7 ms | 2.20 Mbit/s | 5/5 + 5/5; fast.com passed |
| Galaxy / cellular | post-H3 Auto | 1,107.2 ms | 4.62 Mbit/s | 5/5 + 5/5; fast.com passed |
| Pixel / cellular | swapped Auto | 769.5 ms | 3.59 Mbit/s | 5/5 + 5/5; fast.com passed |
| Galaxy / Wi-Fi | swapped Auto | 1,034.5 ms | 2.22 Mbit/s | 5/5 + 5/5; fast.com passed |
| Galaxy client / Pixel provider | P2P direction 1 | 638.5 ms | 2.22 Mbit/s | 5/5 + 5/5 |
| Pixel client / Galaxy provider | P2P direction 2 | 420.7 ms | 7.18 Mbit/s | 5/5 + 5/5 |

That is 115 successful standard samples in 116 attempts plus 6/6 successful
60-second fast.com sessions. Each forced H1/H3 cell again moved bytes only on
its selected SDK carrier, and every Auto traffic interval moved H1 bytes. The
six fast.com sessions moved about 8.2--51.1 MiB each according to SDK packet
deltas. Page-level fast.com byte counts are intentionally not used: closing a
60-second target aborts its streaming requests and under-reports bytes. The
single initial Galaxy cellular fetch failure was followed by a successful
fast.com transfer and later 10/10 H1, 10/10 H3, 10/10 post-H3 Auto, and 10/10
swapped-Wi-Fi samples in the same process. No deterministic 32-MiB functional
failure was found, but the transient is retained rather than erased.

Memory results use SDK runtime/metrics total memory as the 28-MiB streamline
signal. It is not Android PSS and the configured Go limit is soft:

| Budget / device | Cold runtime | Peak runtime / live heap | Samples over 28 MiB | Runtime 1 / 5 / 15 min after fast.com | Forced pressure: immediate / +1 min |
| --- | ---: | ---: | ---: | ---: | ---: |
| 64/20 Pixel | 14.50 MiB | 43.16 / 17.89 MiB | 3,609 / 4,937 | 41.41 / 41.10 / 39.97 MiB | 28.21 / about 29.55 MiB |
| 64/20 Galaxy | 14.37 MiB | 50.94 / 23.64 MiB | 3,309 / 4,938 | 49.26 / 49.26 / 48.51 MiB | 29.72 / 30.55 MiB |
| 32/20 Pixel | 14.58 MiB | 48.94 / 33.19 MiB | 2,008 / 2,445 | 43.00 / 42.98 / 42.47 MiB | 23.40 / 26.16 MiB |
| 32/20 Galaxy | 14.01 MiB | 37.56 / 21.81 MiB | 1,972 / 2,447 | 35.01 / 35.30 / 34.21 MiB | 22.40 / 24.80 MiB |

The cohorts have different duration and fast.com byte volume, so breach counts
and absolute peaks are not an A/B rate comparison. They do establish the
failure boundary: neither 64 nor 32 MiB keeps total Go runtime below 28 MiB,
and a 32-MiB soft limit can be exceeded when live heap plus runtime overhead
already exceeds the target. The largest 32/20 burst coincided with 33.14 MiB
live heap, 8,505 packet objects outstanding (9,107 interval peak), nine live
Auto exits, and only 3.69 MiB in the per-device tracked budget. The other phone
peaked with about 5,034 objects. After traffic, pools returned to one or two
objects, but 8--11 live Auto exits, about 19--20 MiB live heap, and 34--43 MiB
runtime remained essentially flat for 15 minutes. This is bounded burst
allocation plus retained Auto/runtime working set, not packet-proportional
pool growth.

Sdk.freeMemory is the material difference exposed by the iOS-budget proxy.
After 15 minutes it cut the 32/20 processes by 19.06 and 11.78 MiB to 23.40 and
22.40 MiB, and they remained below 28 MiB one minute later at 26.16 and 24.80
MiB with Auto still active. Under 64/20, the same hook landed at 28.21 and 29.72
MiB and returned to roughly 29.55 and 30.55 MiB after one minute. Thus 32/20
improves the response to an iOS-style memory-pressure callback, but normal Go
GC did not autonomously restore the warmed band. Explicit H1 traffic peaked at
28.59/28.22 MiB and explicit H3 at 30.40/29.65 MiB; multi-exit Auto plus
fast.com is the dominant pressure case. Disconnected Direct eventually settled
at 25.5--26.4 MiB in the 64/20 process.

Android debug/instrumentation whole-process PSS was about 293--345 MiB during
64/20 traffic and 304--338 MiB during 32/20 traffic, with higher login/startup
peaks. It includes UI, Java, native, test-runner, mappings, and other memory and
must not be compared with the 28-MiB SDK signal or an iOS extension footprint.
No Android low-memory, Java/native crash, process death, thermal event, or
instrumentation failure was recorded. On finish, tracked device memory and
live exits were zero and file descriptors were 155--161; post-cleanup samples
proved Wi-Fi with no VPN on both devices. The final status is taken before
logout teardown, so post-logout goroutine/runtime recovery remains unmeasured.

Campaign status:

- [x] Two-device Wi-Fi/cellular alternation, Direct, H1, H3, Auto, live
  underlay swap, and same-LAN P2P in both provider directions.
- [x] Cold, burst, 1/5/15-minute, explicit-pressure, and teardown-adjacent Go
  memory samples at both 64/20 and 32/20 budgets, with browser failures and OS
  exit evidence retained.
- [~] Weak cellular behavior. One device was at Android signal level 0, but
  USB power invalidates battery/release evidence and the second route was
  generally level 2--4.
- [ ] Lowest-memory Android, unplugged battery/idle, always-on and lockdown VPN,
  screen/background/airplane/MTU/blackhole flood cases, and physical iOS
  Network Extension comparison.
- [~] Heap allocation/in-use/idle/released/system bytes, fragmentation proxy,
  stacks, mspan/mcache/GC/other/profile metadata, object/allocation/free counts,
  GC count/forced count/pause, pool retained/in-flight/capacity bytes, and
  automatic trim events are now in the physical harness. Socket and QUIC stream
  counts, bounded-queue bytes/age, transport-budget reservations, affinity,
  DNS/reassembly retention, and a post-logout sample remain.

The retained product changes cap Android's process-level SDK budget at the iOS
32-MiB value, preserve the 20-MiB per-device target and 13.17-MiB bounded pool
capacity, and decay returned burst buffers to a roughly 1-MiB warm set after a
quiet minute. This closes the measured steady-state 28-MiB goal on these two
Android proxies, not the active-traffic or physical-iOS goal: one H3 burst had
35.92 MiB of live heap and therefore could not fit under a 28-MiB runtime cap.

### 2026-08-21 allocation attribution and steady-memory remediation

The follow-up added production-rate allocator telemetry plus private Go heap
profiles. A diagnostic A/B used a 64-KiB `MemProfileRate`; the final artifact
kept Go's production 524,288-byte rate. Profiles were written mode 0600 and
never checked in. `WriteHeapProfile` forces a GC, so forced-count changes and
post-profile samples are reported rather than mistaken for natural recovery.
Payloads, destinations, addresses, credentials, and device/client identities
were not retained in the documented result.

The profiles split the over-budget samples into three different lifetimes:

| Ownership / lifetime | Physical evidence | Treatment |
| --- | --- | --- |
| In-flight packet work | At the final H3 peak, Pixel had 11,122 pooled objects outstanding, 35.92 MiB live heap, and 51.70 MiB total Go runtime. Its forced-GC profile attributed 22.55 MiB to packet-pool allocation stacks and 2.50 MiB to decoded-pack owners. | Keep bounded pools and carrier budgets; never trim buffers still owned by work. The active H3 peak remains above 28 MiB and needs a separate concurrency/flight reduction if iOS cannot tolerate it. |
| Returned pool high-water | After traffic drained, the fine-profile Pixel/Galaxy processes retained about 11.29/5.14 MiB in free lists. Merely retaining an arbitrary 1-MiB subset still pinned fragmented spans. | Drop all returned references before `debug.FreeOSMemory`, then recreate the warm set. Preserve configured capacity so a later burst can reuse returned buffers again. |
| Immutable/status configuration | Pixel cumulative allocation attributed about 9 MiB to one-second `GetTransportStatus` calls constructing full transport/TLS defaults and about 3.5 MiB to repeatedly projecting reliability settings. Pinned-CA parsing sat inside the former and also recurred for new exits. | Build eligibility from only modes plus the shared budget, share one read-only parsed root pool while keeping TLS session caches private, and cache each client's immutable reliability projection. Do not pool short-lived immutable configuration that can simply be avoided. |

This distinction matters: pooling mitigates allocator and garbage-collector
spikes while a buffer is likely to be reused, but a large free-list high-water
is still reachable steady memory. Conversely, clearing every pool on every
quiet tick would trade memory for repeated allocation and forced-GC latency.
The mobile policy now does the following:

- the existing 32-MiB process and 20-MiB DeviceLocal targets size the bounded
  free lists to 13,807,616 bytes (13.17 MiB) total capacity;
- a packet-stat epoch resets a 60-second timer only after at least 4 KiB moves,
  protecting user traffic down to roughly 32 kbit/s while allowing measured
  sub-kilobyte background trickle to become quiet;
- the first quiet expiry prunes returned packet buffers to at most 1 MiB and
  larger object classes to their 256-KiB floors, without touching outstanding
  ownership or shrinking future capacity;
- a rebuild forces collection only when at least 1 MiB was dropped. A trivial
  later refill is cheaply pruned but cannot produce a minute-by-minute forced
  GC; an explicit host `TrimMemory` remains deterministic under real pressure;
  and
- automatic trim count and last-dropped bytes are exported, so a footprint
  fall can be attributed rather than inferred from timing.

The diagnostic clear/collect/rewarm A/B reduced Pixel from 34.53 to 24.01 MiB
and Galaxy from 35.05 to 26.90 MiB while retaining the 13.17-MiB capacity. Fine
in-use profiles moved from 21.31/14.21 MiB total with 13.48/4.45 MiB attributed
to message-pool allocation stacks, to 8.89/8.97 MiB total with 1.84/1.02 MiB
on those stacks. This is why the implementation rebuilds allocator spans rather
than only shortening a free-list slice.

The production-rate two-device validation then used explicit H3 on Pixel
cellular and Galaxy Wi-Fi, followed by same-LAN P2P in both role directions.
The H3 workload completed 3/3 Wikipedia navigations, 5/5 streamed 1-MiB
downloads, and fast.com on each phone. The same artifact completed 3/3 pages
and 3/3 downloads in each P2P direction. Exact SDK deltas proved direction 1
used 213,964/2,250,353 client P2P egress/ingress bytes and 221,144 provider P2P
ingress bytes; direction 2 used 241,735/2,431,901 and 249,093 bytes. All 408
samples in the reverse-direction Wi-Fi captures were eligible; the first
direction's role transition left 170/204 eligible samples, and its measured
traffic interval itself was valid.

The H3 failure boundary and recovery were:

| Device / point | Go runtime | Live heap | Pool state | Result |
| --- | ---: | ---: | ---: | --- |
| Pixel / sampled active peak | 51.70 MiB | 35.92 MiB | 11,122 outstanding, 0.63 MiB returned | Above 28 MiB; one live H3 exit; no crash/OOM |
| Galaxy / sampled active peak | 31.95 MiB | 16.76 MiB | 4,181 outstanding near profile peak | Above 28 MiB; one live H3 exit; no crash/OOM |
| Pixel / 70 s quiet status | 24.76 MiB | 6.86 MiB | 0.96 MiB returned; 10.22 MiB dropped | One automatic material trim |
| Galaxy / first 70 s | 30.35 MiB | 14.84 MiB | 8.22 MiB returned | Correctly deferred: 3.07 MiB had still moved |
| Pixel / 140 s | 24.39 MiB | 6.66 MiB | 0.98 MiB returned | Trim count remained one |
| Galaxy / 140 s | 23.19 MiB | 6.52 MiB | 0.99 MiB returned; 7.22 MiB dropped | One automatic material trim |
| Disconnected / five-minute tail | 23.86 / 23.58 MiB | 6.00 / 6.25 MiB | about 1 MiB returned; 0 / 2 outstanding | Both under 28 MiB; each trim count stayed one |

The Pixel's production profile sampled 217.61 MiB of cumulative allocation
over the process lifetime. Besides the 22.55 MiB of pool misses that created
the measured working set, large sampled producers included quic-go packet/
DATAGRAM handling, UDP receive conversion, TUN group construction, and the
status/reliability configuration churn above. Some getter/binding allocation
is an observer effect from the one-second harness itself. Treat `alloc_space`
as a ranking signal, not as simultaneous resident memory; `inuse_space`,
runtime telemetry, pool ownership, and the traffic timeline establish the
resident explanation.

The post-profile product artifact, which includes the configuration-allocation
fixes as well as idle rebuilding, repeated explicit H3 with Pixel cellular at
signal level 0 and Galaxy Wi-Fi. The first fresh-install attempt left Android's
`ACTIVATE_VPN` app-op at `ignore`; both harness commands timed out after 120
seconds without a started TUN. The failure and clean-idle allocation trace were
retained, the two temporary clients were released, and the test-device app-op
was explicitly allowed. Without rebuilding or changing SDK code, both then
connected in under nine seconds; each completed 3/3 Wikipedia, 5/5 1-MiB, and
fast.com. Pixel/Galaxy medians were 972.7/2,337.6 ms for Wikipedia and
1.74/0.72 Mbit/s for the 1-MiB transfer. The one-second sampler caught
31.42/30.78-MiB peaks with 16.30/15.64 MiB maximum live heap; these are still
active/post-burst failures of 28 MiB, but much smaller than the profiled
51.70-MiB Pixel burst. Pixel/Galaxy had 165/185 of 849 one-second samples over
28 MiB, concentrated in traffic and returned-pool recovery, and neither
process crashed. At the second quiet point both had trimmed exactly once to
23.39/22.95 MiB, with roughly 1 MiB returned and future pool capacity
unchanged.

The matched disconnected tail ran 370.97/370.94 seconds and ended at
21.82/21.74 MiB. Against the pre-allocation-fix disconnected tail, allocation
rate fell from 69.0 to 32.2 KiB/s on Pixel and 66.0 to 34.6 KiB/s on Galaxy --
53.4% and 47.6% reductions. GC cadence fell from about 1.52 to 0.97 cycles per
minute on each device. No tail forced a GC, repeated an automatic trim, grew
the roughly 1-MiB warm pool, or recreated a live exit. Galaxy's allocation
object rate did not fall (its surviving allocations were smaller), so the
claim is lower byte churn and collection cadence, not blanket elimination of
all allocation sites.

This Android evidence is an iOS proxy, not an iOS result. It supports using the
same 32/20 limits and mobile quiet policy on both platforms, and it demonstrates
a steady warm band below 28 MiB after real H3/P2P bursts. It does not establish
the iOS Network Extension's native footprint, GOGC-10 behavior, memory-pressure
callback timing, or jetsam boundary. The active H3 breach is a measured failure
of a hard 28-MiB-at-all-times interpretation and remains a physical-iOS gate.

### 2026-08-23 zandroid follow-up: a 20-MiB steady target

This pass lowered the proposed iOS-proxy goal from 28 MiB to a steady 20 MiB
and used the connected Pixel 8 Pro (Android 17/API 37) as `zandroid`. Android
was adeb792799e2, Connect was c1d9ab4, and SDK was f9652cb. The ordinary build
kept the 32-MiB Go soft limit, 20-MiB `DeviceLocal` target, and Go's production
524,288-byte heap-profile rate. A second private process used a 65,536-byte
rate only to rank allocation stacks. Device/client identities, credentials,
addresses, DNS answers, and signed traffic URLs were not retained in this
document. The phone was USB-powered at 100%, cellular signal level 0 was
available but the measured underlay was Wi-Fi, and every OS sample reported
thermal status 0. This remains memory evidence, not battery or physical-iOS
evidence.

The 20-MiB signal is `MemoryStats.TotalRuntimeByteCount`, exactly Go runtime
total mapped memory minus heap pages released to the OS. It is neither Android
whole-process PSS nor an estimate of an iOS Network Extension's total native
footprint. The debug/instrumentation process reached 450,220 KiB whole-app PSS
and contained large Java, code, graphics, and test-runner components, so PSS is
not used for this cross-platform Go-runtime target. "Steady" also cannot mean
"at every instant": live packet work alone exceeded 20 MiB during fast.com.
Active peak and post-traffic recovery need separate gates.

The production-rate process remained alive for 1,551.6 seconds. Auto used H1
according to exact SDK carrier deltas. Five uncached Wikipedia navigations and
five streamed 1-MiB Cloudflare downloads completed; their medians were 476.7
ms and 1.02 Mbit/s. A 60-second fast.com session also completed and moved about
40.4 MiB through H1 according to SDK counters. All 593 external traffic
collector samples were eligible Wi-Fi-plus-VPN, IPv4-only, and thermal-zero
samples. Functional success therefore did not hide the memory miss.

The production-rate timeline was:

| Point | Go runtime | Live heap | Pool state | Topology | 20-MiB result |
| --- | ---: | ---: | ---: | ---: | --- |
| Cold, aged and disconnected | 15.19 MiB | 3.78 MiB | 1.00 MiB returned | 0 exits / 57 goroutines | Pass |
| Auto connect completion | 20.23 MiB | 6.78 MiB | 0.99 MiB returned | 5 exits / 254 goroutines | Fail by 0.23 MiB |
| Auto, about 90 seconds idle | 22.76 MiB | about 7.1 MiB | 1.00 MiB returned | 8--9 exits / about 300 goroutines | Fail |
| Auto, later pre-traffic plateau | 25.06 MiB median, 25.55 MiB peak | about 7.4 MiB | 1.00 MiB returned | 8 exits / 283 median goroutines | Fail |
| fast.com/recovery sampled peak | 35.83 MiB | 20.76 MiB | 4,764 objects outstanding at peak | 10--12 exits / 402 peak goroutines | Active failure; no crash |
| Just before automatic trim | 34.44 MiB | 18.42 MiB | 9.28 MiB returned | 11 exits / 344 goroutines | Fail |
| Immediately after automatic trim | 24.22 MiB | 7.21 MiB | rebuilt to 1.00 MiB returned | topology unchanged | Fail, but 10.22 MiB lower |
| About five minutes after fast.com | 26.15 MiB | about 8.17 MiB | 1.00 MiB returned | 10 exits | Fail after rebound |

The 32-MiB soft limit induced severe collection pressure rather than enforcing
a hard footprint. Across fast.com and recovery the process advanced roughly
5,100 GC cycles; phase rates reached about 23.6 collections/second during the
transfer and 10.4/second during recovery. Live buffers could not be reclaimed,
and the heap goal repeatedly collapsed close to the live set. Pools prevented
some allocation but could not make 4,764 owned objects disappear.

The automatic pool rebuild was effective but late. It ran 192 seconds after
fast.com, not after the configured 60 seconds. There were 13 post-transfer
one-second packet-stat epochs of at least 4 KiB; the last occurred exactly 60
seconds before the rebuild. Multi-exit H1 health/control traffic is included
in the aggregate remote counters and therefore repeatedly reset the global
user-quiet timer. The code behaved as written, but the activity signal is too
broad for a steady-footprint policy.

Matched pressure points in the same long-lived production process separated
warm retention from the connected live floor:

| Mode | Explicit `TrimMemory` (warm pool) | Explicit `FreeMemory` (no returned pool/caches) | Live heap after full pressure | Live exits |
| --- | ---: | ---: | ---: | ---: |
| Disconnected | 20.63 MiB | 19.06 MiB | 3.91 MiB | 0 |
| H3 | 23.62 MiB | 21.68 MiB | 5.80 MiB | 1 |
| H1 | 24.76 MiB | 22.40 MiB | 6.23 MiB | 8 |

The nominal 1-MiB warm packet set costs more than one MiB of runtime at these
floors because reachable slices can pin allocator spans and because
`FreeMemory` also sheds resolver, connection, and affinity caches. The observed
warm-to-full-pressure differences were 1.57--2.36 MiB. Conversely, reducing
H1 from eight exits to the one-exit H3 topology changed the pressure floor by
only about 0.72 MiB. Exit count matters, but mostly through goroutine/control
churn and delayed reclaim; topology reduction alone is not a 2--3-MiB fix.

Allocator history remains after reachable objects drain. From the cold state
to the long-run disconnected pressure floor, stack in-use rose from about 1.28
to 2.41 MiB and GC metadata from about 3.30 to 4.03 MiB even though goroutines
returned to 57 and live heap was only 3.91 MiB. The fast.com process had peaked
at 402 goroutines. Packet concurrency, goroutine fan-out, and stack depth thus
raise the later runtime floor; optimizing only the final object graph misses
this high-water effect.

The private 64-KiB profiles are ranking evidence, not byte-exact accounting.
The pressure profile sampled 6.18 MiB of live allocations while runtime gauges
reported a larger total that also includes stacks, allocator/GC metadata, and
released-versus-retained spans. Sampling variance is especially high for
small objects. Still, the repeated leading sites were useful:

- a returned 1-MiB message-pool set sampled as about 1.40 MiB before pressure
  and disappeared from the full-pressure profile;
- `newContractStatusCallbackWorker` sampled about 0.77 MiB after pressure;
- gomobile `seq.ToRefNum` tables sampled about 0.53--0.69 MiB, attributable in
  different profiles to `ExitList_Get` and the one-second transport/status
  object getters;
- H1 WebSocket batch buffers, `bufio` readers/writers, HPACK state, decoded-pack
  owners, RTT windows, location/grid projections, JSON type metadata, and TLS
  state each appeared in the roughly 0.1--0.4-MiB sampled tier; and
- cumulative allocation ranked the SDK I/O loop, IP packet grouping, remote
  packet-group sends, pool misses, TLS record encryption, decoded-pack owners,
  timers, and send scheduling as the main burst/churn paths.

The gomobile result is partly an observer effect. `ToRefNum` grows global Go
reference maps when object graphs cross into Java; deleting entries does not
necessarily shrink their map buckets. Calling exits, throughput lists,
transport distributions, packet-stat objects, and transport status every
second therefore perturbs the exact heap being measured. The retained
0.53--0.69-MiB samples are not all product steady state. A 20-MiB release gate
needs Go-side or primitive-only sampling and should project topology no more
often than every 15--30 seconds.

One final A/B measured Go heap profiling itself. Merely calling
`SetMemoryProfileRate(0)` from Android application startup was too late: the Go
runtime already held 1,447,889 bytes of profiling buckets. Removing the SDK
helper was also insufficient because gomobile links a Go shared library and
retains `runtime.memProfileInternal`. A controlled build with profiling
disabled at native-runtime initialization reduced profiling buckets to 5,649
bytes. At the same approximately seven-second ready point, runtime fell from
13,146,376 to 11,419,664 bytes, a 1.65-MiB reduction. No such build change was
retained; the ordinary SDK artifact was rebuilt after the experiment.

The native-profile-off process then showed both the opportunity and its limit:

| State | Runtime result with native profiling disabled |
| --- | ---: |
| Cold ready, first sample / short ready-phase median | 10.92 / 13.74 MiB |
| Auto connect-completion median | 19.05 MiB |
| Auto early-idle median / p95 | 21.10 / 21.42 MiB |
| Auto after `TrimMemory`, nine exits and 1-MiB warm pool | 20.58 MiB |
| Auto after `FreeMemory`, eight exits | 19.20 MiB immediate; 20.42 MiB about 75 seconds later |
| H3 one-exit idle median / p95 | 19.38 / 19.55 MiB |
| H3 one-exit full-pressure floor | 18.84 MiB |

This was a focused hypothesis test, not a matched full fast.com rerun, so its
numbers must not be subtracted mechanically from the longer production
history. It does establish that a connected sub-20-MiB state is technically
reachable, that one-exit H3 can hold it for a minute, and that profiler removal
alone does not keep multi-exit Auto below 20 MiB after normal repopulation.

The ranked plan to make 20 MiB a repeatable steady state is:

1. **Make the gate non-observing and explicit.** Add a bounded Go-side sampler
   that records primitive memory/pool/topology counters and exports one batch
   after the interval. Stop constructing bound exit/status/list objects every
   second. Gate a production build on five fresh-process repetitions with
   `TotalRuntimeByteCount` p50 and p95 at or below 20 MiB after five connected
   quiet minutes. Report cold, one-minute, five-minute, and fifteen-minute
   values separately. Keep fast.com active peak and recovery time as separate
   gates; do not hide a peak failure in a steady average.
2. **Fix mobile quiet detection and reclaim.** Drive the quiet timer from user
   TUN payload activity, or explicitly exclude/tag carrier health, probe, and
   status traffic. When pool outstanding ownership is drained and runtime is
   above target, use a short 10--15-second high-water debounce followed by one
   full shed/collection per quiet epoch; do not wait for a control-silent
   minute. Measure a 256- or 512-KiB mobile warm packet set versus the current
   1 MiB. Full shedding and a smaller warm set are needed because pool-only
   trim left 20.58 MiB even in the profiling-off process. Never collect while
   buffers are in flight, and add rate/cooldown counters so this cannot become
   a battery-expensive periodic GC.
3. **Disable heap profiling before the release mobile Go runtime starts.** A
   diagnostic library should retain `WriteHeapProfile` and selectable sampling;
   Android/iOS release libraries should start with `memprofilerate=0`. Android
   application callbacks are too late because `go.Seq` loads `libgojni.so` in
   its static initializer, so this needs a controlled gomobile loader/runtime
   build rather than an `Application.onCreate` call. Re-measure binary startup,
   crash diagnostics, and the physical iOS extension before adopting it. The
   measured steady benefit was about 1.1 MiB in early Auto and 1.65 MiB at the
   matched cold point, not the full 1.45-MiB bucket counter in every state.
4. **Reduce the connected live/control set.** Add a mobile-low-memory
   `MultiClient` profile and A/B the quality window at three instead of six,
   speed at one with a hard cap of one or two, and standing reserve only when
   measured quality requires it. Coalesce contract status through one worker
   per window/device and size its latest-value queue to live contracts rather
   than the transfer sequence buffer. Right-size H1 WebSocket/bufio/HPACK
   buffers and share only immutable TLS/root state. The 0.72-MiB H1-versus-H3
   pressure difference means window reduction must be justified by lower
   control traffic, goroutines, and allocation rate as well as direct bytes.
5. **Lower burst high-water instead of deleting useful pools.** Bound H1/H3
   send/receive flight and packet-group fan-out so fast.com cannot create 4,764
   simultaneous pool owners or a 402-goroutine stack high-water. Preserve
   bounded reuse for active traffic, then reclaim returned objects. Track quiet
   GC cadence with a provisional goal of at most one collection/minute and no
   forced collection outside a recorded reclaim event.

The warm-set/reclaim policy must remain mobile-specific. Do not shrink global
Connect pool capacity or apply mobile forced-reclaim timing to `server/connect`
or `server/proxy`; any shared-pool implementation change must rerun their
performance suites and `server/connect/perfvar`. For mobile acceptance, repeat
Wikipedia, the streamed 1-MiB object, and fast.com on Auto/H1/H3, verify exact
carrier bytes and no latency/goodput regression, then use the physical iOS
Network Extension footprint and jetsam behavior as the actual release gate.

### 2026-08-23 20-MiB plan implementation and zandroid validation

All five implementation items above are now present in the candidate source,
with mobile policy isolated from server defaults:

- a 64-record, 15-second Go sampler records primitive runtime, pool, topology,
  flow, transport-budget, reclaim, and host-supplied physical-footprint values;
  its record and complete `DeviceLocal.memorySample` paths allocate zero in
  tests. Android drains batches without constructing gomobile exit/status/list
  graphs at one hertz;
- mobile high-water reclaim uses TUN payload quiet rather than carrier-control
  silence, a 15-second debounce, at most 16 outstanding pool objects, a
  one-minute cooldown, full cache/pool shedding, and a 256-KiB packet warm set.
  Runtime and iOS `phys_footprint` threshold crossings can arm a quiet epoch;
  material above-target drops can request a later cooldown-bounded pass, while
  an immaterial floor cannot create a forced-GC loop;
- Android, Apple, and the reduced iOS-extension release libraries set
  `memprofilerate=0` at Go runtime link initialization. A private build can
  select a positive rate through the same build input, and a build-policy test
  checks the actual runtime value. Android now uses the iOS extension's
  `GOGC=10` pacing as well; the previous Android-only value of 50 concealed
  allocator float in the surrogate measurement;
- the <=20-MiB mobile profile fixes Auto quality/speed windows at 3/1, disables
  standing reserve, and makes the hard max a strict admission ceiling without
  destroying existing flows. Contract status now has one live-contract-sized
  coalescer per window instead of one packet-sequence-sized worker per exit;
  mobile HTTP, WebSocket, HPACK, and HTTP/2 receive state is explicitly bounded;
  and
- every mobile send, receive, forward, contract, and unreliable-flight
  sequence is capped at 16 messages. Packet grouping is capped at 16 packets /
  24 KiB and shared transfer queues retain byte ceilings. A mobile-only sampled
  admission gate rejects and returns a complete native ingress batch when the
  process has at least 512 outstanding packet roots, then resamples on every
  ingress call until the pressure drains. Inactive mobile TCP flow state is
  reaped after three minutes instead of the desktop ten-minute default.
  Server/default sequence sizes, admission behavior, GC pacing, pool warm set,
  and reclaim timing are unchanged.

The physical run used the one attached `zandroid` Pixel 8 Pro (Android 17/API
37), a main-environment Github Debug app, the 32-MiB Go soft limit and 20-MiB
device target, and native heap profiling disabled. Credentials and the
temporary acceptance client stayed private; the client was released after the
run. The long-lived process alternated Wi-Fi Auto, cellular explicit H1, and
Wi-Fi explicit H3. Wikipedia, a real streamed Cloudflare 1-MiB object, and
fast.com completed in every measured transport cell. Exact carrier counters
recorded 25.40 MiB H1 ingress and 8.69 MiB H3 ingress over the session. Android
whole-app PSS is intentionally excluded from the Go target.

The artifact used for this long session contained the sampler, native profile
policy, 15-second reclaim, 256-KiB warm set, queue/group bounds, and initial 3/1
window settings. It preceded the final strict-admission and 16-message tuning,
which were added from the observed overshoot and active-flight counts:

| Phase | Go runtime p50 / p95 / max | Live heap max | Pool ownership | Topology / result |
| --- | ---: | ---: | ---: | --- |
| Fresh Auto, 5.5 quiet min (22 samples) | 17.34 / 17.67 / 17.78 MiB | 5.07 MiB | 234 outstanding max | quality 3, speed 1; steady 20-MiB pass |
| Auto fast.com | 22.96 / 27.97 / 27.97 MiB | 12.38 MiB | 2,074 outstanding max | active peak below 28 MiB in sampled Auto interval |
| Auto after first reclaim (28 samples) | 20.91 / 21.73 / 21.90 MiB | 6.50 MiB after drain | <=9 outstanding | quality grew to 5; steady miss that produced strict admission |
| Cellular H1 fast.com | 28.65 / 29.25 / 29.25 MiB | 12.80 MiB | 1,921 outstanding max | functional H1; active 28-MiB failure |
| Wi-Fi H3 fast.com | 30.36 / 30.56 / 31.92 MiB | 15.43 MiB | 2,757 outstanding max | 9.1 MiB H3 ingress; active 28-MiB failure |
| H3 after reclaim | 22.83 / 23.90 / 24.15 MiB | about 7 MiB after drain | <=19 outstanding | returned pool about 0.25 MiB; long-process recovery still above 20 MiB |

The >28-MiB samples are attributable, not an unidentified leak. They coincide
with 1,921--2,757 borrowed packet objects and 12.8--15.4 MiB live heap during
fast.com. After Chrome stopped, outstanding ownership fell to control-scale
counts; reclaim dropped H1 from 30.85 to 23.22 MiB and H3 from 31.85 to 21.70
MiB while preserving the 13.17-MiB future pool capacity. Repeated cooldown
passes were material only while allocator state drained. This evidence selected
the final 16-message sequence/flight ceiling. The first rebuilt artifact proved
that strict window admission held quality at three but also showed 2,488 packet
roots and a 30.31-MiB Auto peak: per-flow caps alone do not bound aggregate
ownership across the native ingress, receive, and transfer pipeline. That
failure produced the sampled 512-root pressure gate rather than another global
pool-size reduction.

Two final-source follow-ups used the same Pixel and real sites. The first kept
Android's old `GOGC=50` solely to isolate the packet-pressure and three-minute
flow policies. The second changed only Android pacing to the iOS value of 10:

| Rebuilt artifact / phase | Samples | Go runtime p50 / p95 / max | Ownership / reclaim | Result |
| --- | ---: | ---: | ---: | --- |
| pressure guard, Auto fast.com | 8 | 22.31 / 23.62 / 23.62 MiB | 1,474 roots max; 1,914 cumulative pressure drops by recovery | no >28-MiB sample; prior 30.31-MiB failure closed |
| pressure guard, Auto post-reclaim steady | 19 | 19.79 / 20.13 / 20.13 MiB | one forced reclaim; flows 41 -> 8 | 20-MiB p95 miss by 0.13 MiB |
| pressure guard, H3 fast.com | 8 | 25.38 / 26.53 / 26.53 MiB | 1,117 roots max; 1,630 additional pressure drops | no >28-MiB sample; functional H3 carried 5.70 MiB ingress |
| pressure guard, late H3 recovery | 11 | 21.00 / 21.45 / 21.45 MiB | returned pool about 0.25 MiB; flows fell to 7 | remaining floor was live/allocator state, not retained buffers |
| final iOS-paced Auto fast.com | 7 | 21.08 / 21.13 / 21.13 MiB | 1,453 roots max; 1,227 pressure drops | zero >28-MiB samples |
| final iOS-paced five-minute recovery | 22 | 17.76 / 17.88 / 17.88 MiB | one reclaim; flows 49 -> 3 | steady 20-MiB p50/p95 pass |
| final iOS-paced cellular H1 real-site traffic | 14 | 19.69 / 19.89 / 19.89 MiB | 1,130 roots max; 1,865 pressure drops | active 20-MiB p50/p95 pass; zero >28-MiB samples |
| final iOS-paced cellular H1 post-reclaim | 7 | 17.38 / 17.57 / 17.57 MiB | returned pool <=0.26 MiB; flows 21 -> 1 | steady 20-MiB p50/p95 pass |

The complete pressure-guard session recorded 60 samples, zero over 28 MiB, a
26.53-MiB whole-session peak, 3,544 pressure-rejected ingress packets, and no
process termination. The counter is deliberate overload loss, not corruption
or a leaked return: batch rejection returns every pooled owner immediately and
TCP provides retransmission/backpressure. Because the snapshot is sampled and
already-admitted remote work drains asynchronously, 512 is the trigger rather
than a claim that observed process-wide ownership can never exceed 512.

The exact final artifact was `m20-iospace-20260823`: 32-MiB Go soft limit,
20-MiB device target, `GOGC=10`, and `memprofilerate=0`. Its 31 samples had no
28-MiB breach and a 21.13-MiB whole-session peak. The reclaim changed the
runtime from 21.56 to 16.56 MiB; the next five connected minutes remained at
17.76-MiB p50 / 17.88-MiB p95 / 17.88-MiB max while the pool retained at most
about 0.57 MiB and flow count drained to three. H1 carried 4.84 MiB of ingress.
The final detailed snapshot recorded 0.375 seconds of cumulative GC pause over
470 seconds of process lifetime (about 0.08%); the denser iOS pacing therefore
closed the heap-float gap without a material pause-time tax in this run.
Wikipedia, the Cloudflare object, and fast.com all generated real tunneled
traffic, and the process finished cleanly. The temporary client was released
and private credentials were removed from the device.

The same exact artifact then ran in a fresh process with Wi-Fi disabled and the
cellular underlay proven before explicit H1 traffic. Wikipedia, the Cloudflare
object, and fast.com completed while H1 recorded 5.48 MiB ingress. The 14-sample
traffic interval stayed at 19.69-MiB p50 / 19.89-MiB p95 and max despite 1,865
pressure rejections. The whole process peaked at 20.34 MiB during the short
pre-reclaim recovery interval, never approached 28 MiB, and did not terminate.
One reclaim changed 20.34 MiB to 17.10 MiB; the following seven samples were
17.38-MiB p50 / 17.57-MiB p95 with returned buffers at or below 0.26 MiB and
flows draining from 21 to one. Cumulative GC pause was 0.439 seconds over 378
seconds (about 0.12%). The acceptance client was released, private credential
files were deleted, and Wi-Fi was restored after the run.

The message-pool changes were also isolated against the exact parent revision
with five 300-ms benchmark repetitions on Apple M4 Pro / Go 1.26.7:

| Server package | Time geomean change | B/op change | allocs/op change |
| --- | ---: | ---: | ---: |
| `server/connect` | -0.79% | +0.00% | unchanged |
| `server/connect/perfvar` link primitives | +0.25% | +0.00% | unchanged |
| `server/proxy` | +0.01% | +0.00% | unchanged |

No server reclaim tuning is warranted from these results. The mobile caller
uses the parameterized 256-KiB warm/reclaim API; existing server callers retain
the 1-MiB wrapper and do not start the mobile trimmer. Five samples do not
provide a 95% confidence interval, so the small time movements are treated as
noise while the exact allocation equality is the useful guard.

Only one Android device was attached during this follow-up, so a new same-LAN
P2P role pair could not run. The 2026-08-21 bidirectional two-device P2P result
remains the current physical P2P evidence. No iOS device was attached, and
Android does not publish the extension's `TASK_VM_INFO.phys_footprint`; the new
allocation-free iOS recorder and pressure trigger are implemented and tested,
but an actual Network Extension footprint/termination run remains the release
gate. A 20-MiB Go steady result on Android is not proof of a 20-MiB iOS process
footprint.

### 2026-08-24 24-MiB performance rebalance

The 20-MiB profile met its memory target, but a matched fresh-process Wi-Fi
Auto pass on the same attached `zandroid` exposed an unacceptable real-site
cost. The exact `m20-iospace-20260823` artifact loaded Wikipedia in a 3,967.8-ms
median, with 1,575.2-ms median document TTFB and 2,359.23-ms median per-page
request p95. A streamed Cloudflare 1-MiB object took 33.279 seconds at 0.25
Mbit/s median; the direct control completed in 0.412 seconds at 20.37 Mbit/s.
The page phase never reached the 512-root pressure gate, so its latency was not
caused by packet rejection. The transfer phase did accumulate 1,135 pressure
drops and 72 collections, showing that the 20-MiB combination was also too
aggressive under bulk traffic.

The accepted profile treats 24 MiB as the mobile Go-runtime steady target and
keeps 28 MiB as a separate active diagnostic failure threshold. It spends the
additional room narrowly:

- mobile Android/iOS defaults and Android's explicit per-device target are 24
  MiB; desktop/server retains its established 20-MiB default;
- Auto quality/speed windows are fixed at 4/1 rather than 3/1, giving route
  selection one additional quality candidate without restoring the much
  larger desktop live set;
- Android and iOS use `GOGC=25`, midway between the slow 10 setting and the
  unsafe 50 experiment, and keep `memprofilerate=0` plus the 32-MiB soft limit;
- the post-reclaim packet warm set is 512 KiB rather than 256 KiB, avoiding a
  completely cold allocation wave while leaving the pool capacity unchanged;
  and
- the measured H3-safe 16-message sequence/unreliable-flight ceiling,
  16-packet/24-KiB group ceiling, 512-root aggregate pressure gate, and
  three-minute flow retirement remain unchanged.

The physical A/B rejected every apparently faster configuration that weakened
the H3 safety margin. These are threshold experiments, not directly comparable
latency samples: live network conditions and process ages differed.

| Candidate | Queue / aggregate policy | Explicit-H3 runtime max | Decision |
| --- | --- | ---: | --- |
| `GOGC=50`, 32-message/group, 768-root gate | both per-flow and process-wide admission widened | 28.41 MiB | reject: crossed 28 MiB |
| `GOGC=50`, 32-message/group, 512-root gate | restored aggregate gate only | 29.30 MiB | reject: already-admitted per-flow work still crossed 28 MiB |
| `GOGC=50`, 16-message, 16-packet/24-KiB group, 512-root gate | restored all packet safety ceilings | 29.95 MiB | reject: GC heap float alone remained unsafe |
| `GOGC=25`, unchanged H3-safe ceilings | final `m24-route-gc25-safe-20260824` profile | 24.73 MiB whole-session; 24.03 MiB under sustained H3 traffic | accept on Android surrogate |

The final artifact used a fresh authenticated process, Chrome's cache-disabled
benchmark path, real Wikipedia/Cloudflare/fast.com traffic, the 32-MiB Go soft
limit, and production `memprofilerate=0`. Against the exact 20-MiB baseline,
the observed performance was:

| Workload | 20-MiB profile | Accepted 24-MiB profile | Observed change |
| --- | ---: | ---: | ---: |
| Wikipedia load, 7 runs | 3,967.8 ms median | 745.0 ms median | 81.2% lower |
| Wikipedia document TTFB | 1,575.2 ms median | 248.1 ms median | 84.3% lower |
| Wikipedia request p95 per page | 2,359.23 ms median | 294.58 ms median | 87.5% lower |
| Cloudflare streamed 1 MiB, 5 runs | 33.279 s / 0.25 Mbit/s median | 3.892 s / 2.16 Mbit/s median | 88.3% less time / 8.64x goodput |

This live-route result demonstrates that the severe regression is removable;
it is not a confidence interval or proof that every gain comes from one knob.
The accepted `GOGC=25` Wikipedia median was slower than the rejected
`GOGC=50` candidate's 519.9 ms, which is the expected safety/performance trade.
Two newly-created DevTools targets closed their websocket during the explicit
H3 phase; the retained existing-target attempt completed in 2.624 seconds.
Those two harness-visible failures remain recorded rather than being converted
into successes.

Memory telemetry for the accepted run separated active high-water from steady
recovery:

| Phase | Samples | Go runtime p50 / p95 / max | Live/pool ownership | Result |
| --- | ---: | ---: | ---: | --- |
| Auto fast.com | 7 | 20.65 / 20.92 / 20.92 MiB | 8.22-MiB live heap; 1,248 roots max | below both targets |
| Explicit H3 page | 5 | 22.66 / 23.19 / 23.19 MiB | returned pool <=2.73 MiB | below both targets |
| Explicit H3 fast.com | 8 | 23.86 / 24.03 / 24.03 MiB | 10.08-MiB live heap; 1,152 roots max | active headroom retained |
| H3 recovery, including drain | 25 / 360 s | 20.20 / 24.61 / 24.73 MiB | first 105 s include in-flight drain | below 28-MiB active guard |
| H3 recovery after reclaim | 18 | 20.12 / 20.52 / 20.52 MiB | reclaim 24.12 -> 19.53 MiB; warm pool about 0.5 MiB | steady 24-MiB pass |

The final summary contained 59 samples, zero 28-MiB breaches, a 24.73-MiB
runtime peak, 10.33-MiB live-heap peak, 1,289 maximum outstanding pooled
objects, and 5,512 cumulative overload drops. Exact carrier counters recorded
16.75 MiB H1 and 6.32 MiB H3 ingress. One quiet reclaim fired 105 seconds after
the H3 traffic phase and flows drained from 45 to six. Cumulative GC pause was
0.953 seconds over the 887.7-second process (about 0.11%). The instrumentation
finished successfully, the temporary client was released, and credentials
were removed from the device.

Server isolation used an exact same-session A/B rather than the misleading
prior-day comparison. Server `1806bbc9` and every non-SDK dependency were held
fixed; SDK `49f756f` was compared with only this patch. Every benchmark ran for
300 ms with `-benchmem`, `GOMAXPROCS=10`, and six repetitions per side in
baseline/candidate/candidate/baseline order over three cycles:

| Server package | Time geomean change | Allocation result |
| --- | ---: | --- |
| `server/connect` | -0.05% | B/op +0.01%; allocs/op unchanged |
| `server/connect/perfvar` | -0.17% | B/op and allocs/op unchanged |
| `server/proxy` | +0.05% | every B/op and allocs/op result unchanged |

No individual timing comparison was significant. This rejects a server
performance regression and confirms that the mobile/default split leaves
Linux at `GOGC=100`, its previous 20-MiB device default, the 1-MiB server warm
wrapper, and no mobile packet gate/reclaimer. The broad server short-suite
attempt remained environment-blocked by missing `WARP_ENV` and vault `pg.yml`,
matching its documented fixture limitation; the benchmark and focused policy
processes passed.

Only one Android remained attached, so this pass could not create a new P2P
pair; the successful 2026-08-21 bidirectional same-LAN result remains the
current evidence. The 24-MiB result is still a Go-runtime Android surrogate,
not proof that an iOS Network Extension remains below its `phys_footprint` or
jetsam limit. A physical iOS run remains the release gate.

### Provisional release gates

Freeze exact gates after Phase 1 measures variance. Until then, the target is:

- zero packet corruption, policy bypass, encryption downgrade, sequence/lane
  cross-talk, unbounded allocation, or IPv6 advertisement;
- no shared callback or unreliable physical lane waits for queue space or
  worker exit; every reliable-lane wait owns at most its one already-read frame,
  stays inside unchanged fixed queue/byte budgets, and ends on cancellation;
- sparse interactive p95 at least 20% below current H1/H3 on mobile-poor and no
  more than 10% above direct when direct completes;
- loaded interactive p95 at least 25% below the better current tunnel mode;
- isolated bulk goodput at least 90% of direct and no more than 5% below the
  better current tunnel mode;
- recovery after connectivity returns within `max(3 * measured RTT, 2 s)` for
  an already-established session, excluding a required fresh authentication;
- hard byte, packet, fragment, lane, and age bounds under sustained overload;
  and
- no material battery/wakeup regression in long-idle and intermittent-traffic
  device tests.

If direct traffic fails a scenario, report that explicitly rather than treating
the tunnel's completion as an infinite percentage win.

## Implementation phases

### Phase 0 — correctness and observability

- [x] Carry exact receive-lane reliability through RouteManager and make the
  shared Client Pack handoff wait only for H1, H3/DNS QUIC stream, SCTP, and
  framed reliable routes. H3 DATAGRAM and native P2P remain zero-timeout.
- [x] Enforce zero-timeout inbound ACK admission.
- [x] Drop rather than wait on receive-generation replacement when admission is
  nonblocking.
- [x] Count Pack/byte and ACK handoff drops and add deterministic regressions.
- [x] Expose Pack/byte and ACK handoff drops through the lock-free
  `Client.ReceiveStats()` snapshot.
- [x] Make H1, H3/H3Dns/H3DnsPump QUIC stream, and legacy SCTP readers preserve
  fixed-capacity backpressure to cancellation. Publish hybrid H3 and production
  P2P receive lanes as distinct immutable routes; keep H3/DNS DATAGRAM and
  native P2P carrier-reader admission zero-wait with exact refusal counters.
- [x] Make reliable connect-server socket/exchange readers wait for their fixed
  handoff or cancellation. When a shared resident callback cannot wait, retire
  that generation on refusal rather than continuing past an invisible frame.
- [x] Move resident control throttling and forward construction/storage checks
  out of Client callbacks. Control remains ordered; forwards use bounded
  destination-stable worker shards. Reliable-control overflow retires the
  resident generation instead of silently skipping acknowledged state.
- [x] Record device/provider platform carrier refusals at PERFVAR's exact
  interval boundary and invalidate contaminated schema-5 measurements.
- [x] Make the 1,100-byte product MTU compatible with standards-required QUIC
  startup. Add bounded, overlap-rejecting IPv4 fragment reassembly before NAT
  parsing, complete-datagram device/provider security inspection, ordered
  original-fragment forwarding, standards-correct IPv4 fragmentation for
  provider UDP replies, and enough bounded UDP socket read space for a complete
  QUIC Initial. Fragmented TCP is rejected before SMTP/CFAA routing. The real
  full-TUN inner-QUIC synthetic correctness workload covers both directions;
  platform-kernel behavior remains in the real-device follow-up.
- [x] Give every retained IPv4 fragment group a fresh nonzero wire identity
  before asynchronous routing. This preserves two interleaved gVisor ID-zero
  datagrams across parallel H3 lanes without relaxing the fragment budgets.
- [x] Match native-P2P unreliable flight and zero-wait receive handoffs by
  message count while retaining independent 256-KiB byte ceilings. Legacy SCTP
  now publishes reliable receive semantics and waits only on its fixed route;
  a 240-KiB native carrier flight leaves 16 KiB inside the unchanged queue
  ceiling for untracked control. Both modes remain Transfer-ACKed.
- [x] Complete the adjacent receive-callback audit. The shared Client pump,
  exact-delivery and encryption fixtures, control-sync collector, mux and
  multi-client provider echoes, contention benchmarks, WebRTC signal/data,
  stream lifecycle, SDK migration, RPC mux, and `connectctl sink` use bounded
  zero-wait admission, generation-local asynchronous work, or inline counters.
  Deliberately blocked callbacks remain only in hostile-callback
  isolation/lifecycle regressions and the two documented ownership exceptions.
- [ ] Add cross-layer IDs/timestamps that correlate an inner packet, Transfer
  item/fragment, QUIC send, ACK, retry, and collapse decision in test telemetry.

Exit: every shared receive path has a deterministic saturation test, and a
single trace can attribute each retry and queue delay to its owning layer.

### Phase 1 — reproducible baseline and field model

- [~] Finish the authoritative five-run current H1/H3/P2P campaign. The
  current static 256-KiB warmed-upload matrix now has five exact H1, H3, Auto,
  and corrected P2P samples on `cell-edge-1m-down-250k-up`. Direct calibration
  variance invalidated some aggregate comparisons, and directions, workloads,
  dynamic profiles, and physical radios remain.
- [~] Add a database-free, production-Transfer carrier A/B that can compare
  legacy H3 stream framing with negotiated H3 DATAGRAM on the same deterministic
  cell-edge link. The harness now connects two real `Client` instances through
  production gVisor TUNs and QUIC, exercises Pack/ACK/sequence/RTT/dedup/resend,
  verifies exact payload delivery, and reports link and carrier counters. It
  still needs the broader workload matrix and checked-in result artifacts.
- [~] Add the curated cell-edge profiles, disruptions, MTU changes, and
  variable-rate profiles to PERFVAR. Three static composite device profiles
  now cover 5/1 Mbit/s, 1/0.25 Mbit/s, and 256/64 kbit/s with coupled RTT,
  jitter, loss, queue, and MTU conditions. Three selectable one-hop schedules
  now isolate fast-to-slow-to-fast capacity, a one-second outage, and a live
  1,400-to-1,280-to-1,400 outer-MTU transition in direct calibration and all
  four current carriers. NAT rebind, address-change, UDP-blocked, oscillating
  capacity, and field-trace replay still need campaign integration.
- [~] Continue privacy-safe trace capture/replay and collect representative
  physical iOS and Android cellular traces. Android
  now has a dependency-free host sampler in
  `app/scripts/physical_lowbar_capture.mjs`. It keeps raw `dumpsys` data only in
  memory, emits allow-listed NDJSON radio/VPN/battery/thermal/memory/interface
  telemetry, and can invalidate any sample that is not cellular, active-VPN,
  unmetered, IPv4-only, unplugged, and at or below a requested Android signal
  level. Its parser/eligibility fixtures pass. The 2026-08-21 two-device
  campaign above now covers powered Wi-Fi/cellular Direct, H1, H3, Auto, live
  underlay switching, same-LAN P2P in both directions, and 64/20 versus 32/20
  memory pressure with 5,000 eligible radio/path samples. It remains ineligible
  as unplugged weak-radio and battery evidence. The Debug
  iOS physical-page driver now records sanitized physical-path state, app
  footprint, power/thermal state, exact transport settings, and aggregate plus
  per-transport `DeviceRemote` packet-counter deltas. It can now stop the
  Network Extension non-destructively for a proven Direct sample or pin H1,
  H3, Auto, DNS, or DNS-pump on the active `DeviceRemote`; readiness verifies
  the stopped/connected route and requested mode before timing. Focused
  simulator tests validate its configuration, serialization, SDK bindings,
  counter attribution, explicit reset detection, and bounded device-log
  framing. A dependency-free iOS host collector now launches the driver,
  reassembles those frames, strips URLs/identifiers/raw errors, waits for route
  cleanup, and rejects unconfirmed one-bar, non-cellular, powered, low-power,
  hot, wrong-route, wrong-carrier, reset-counter, or incomplete samples. These
  are instrumentation validations, not physical low-bar performance evidence.
  A paired campaign analyzer now requires at least five balanced-order
  Direct-before/H1-H3-Auto/Direct-after cycles, stable sanitized path and page
  content shape, chronological unique runs, bounded cycle duration, and
  per-page Direct drift below a configured threshold. It reports candidate
  timing/byte ratios against the enclosing Direct mean plus actual carrier
  bytes per browser byte without emitting source paths or timestamps. A
  one-command campaign runner creates a non-overwriting private directory,
  randomizes a position-balanced six-order design, executes all Direct brackets
  and forced candidates, persists the first failed sanitized sample, and emits
  a comparison only when the complete analyzer gate passes.
  Still run the iOS driver on a physical weak-signal device, repeat Android
  unplugged on a controlled one-bar path and the lowest-memory supported model,
  add header-only device and edge capture plus privacy-safe trace replay, and
  correlate captures with the production transport/reliability counters.
  Capture cold readiness, exact
  completion, loaded p50/p95, delivery ratio, retries, carrier bytes, H3 lane
  selection, memory recovery, battery/thermal state, interface transitions,
  and the application-visible result of a 1,200-byte QUIC Initial over the
  advertised 1,100-byte VPN route. Device/edge packet captures and platform
  radio telemetry are the acceptance evidence. Do not treat simulator,
  shaped-host, or an eligibility-rejected device run as physical-radio
  evidence.
- [~] Measure direct traffic beside each tunnel mode and freeze release gates.
  The powered two-device Android campaign has Direct Wi-Fi/cellular controls
  beside the tunnel cells, but uncontrolled path variance, USB power, and the
  missing iOS/low-memory cohorts prevent freezing release thresholds.

Exit: checked-in profiles and artifacts reproduce the current tails closely
enough to rank candidates consistently.

### Phase 2 — QUIC DATAGRAM prototype

- [x] Add authenticated, versioned capability negotiation. `Auth` carries a
  separate offer and server-accepted version, while QUIC transport parameters
  independently negotiate RFC 9221. An old server echoes the unknown offer but
  cannot set acceptance, so a new client remains on the stream without a
  reconnect; an old client sends no offer, so a new server also stays legacy.
- [~] Add bounded DATAGRAM framing, fragmentation/reassembly, duplicate and
  reorder handling, and Transfer ACK/retry integration. Hybrid envelope v2
  carries a connection-local message id, total length, CRC-32, fragment
  offset/index/count and uses a 1,360-byte target. The production limit is one
  fragment per message; explicit tests may raise it to eight. Other limits are
  8 KiB/message, 32 incomplete messages, 64 KiB/connection, 8 MiB/handler, and
  5 seconds. Production selection admits only complete frames that fit one live
  DATAGRAM; a worst-case 1,100-byte tunnel packet uses stream at the safe
  initial path size. Transfer retries the whole DATAGRAM-carried frame after
  carrier loss.
- [x] Send complete routed Transfer frames—including Packs and ACKs—over
  DATAGRAM; retain auth and ping on the reliable stream. A live local QUIC test
  verifies a multi-fragment Pack and a single-datagram ACK in opposite
  directions.
- [~] Support UDP-blocked and legacy-peer fallback without a reconnect loop.
  Mixed-version H3 fallback is implemented and the H1 path remains intact.
  Auto starts H3 and H1 together at equal priority and retains every healthy
  carrier at that priority; DNS and DNS pump follow at priorities two and
  three. Sticky path learning and production UDP activation remain open.
- [~] Add qlog/Transfer correlation and all fragment/drop counters. Lock-free
  client snapshots and bounded-label server Prometheus counters cover sends,
  receives, bytes, duplicates, malformed/checksum drops, timeouts, limits, and
  send errors. Cross-layer packet/Transfer/qlog correlation remains open.
- [~] Fuzz framing/reassembly and test loss, duplication, reordering, truncation,
  oversized declarations, timeout, cancellation, and memory recovery. A fuzz
  target and deterministic boundary, reorder, duplicate, overlap, corruption,
  timeout, per-peer/shared-budget, close-recovery, mixed-version, live Pack/ACK,
  and race tests are present. A sustained fuzz campaign plus deterministic
  last-fragment loss through full Transfer retry still remain.

Exit: Transfer is demonstrably the only packet-lane payload retransmitter and
the end-to-end commit owner on every lane, all buffers are bounded, and the
candidate beats current H3 without regressing H1 completion.

### Phase 3 — lanes, scheduling, and collapse ownership

- [x] Add bounded logical lane negotiation and carry its routing key through
  every in-order, optimistic, batch, resend, replay, encryption, and reply path.
  The wire version is advertised only by a live lane-0 ACK. Old peers ignore the
  additive fields and never activate implicit lanes; an explicit reply retains
  the inbound lane. Data lanes reuse the negotiated encryption session and pin
  the base sequence until their last dependent sequence exits.
- [~] Add the pre-sequence flow-fair scheduler and control/interactive reserve.
  The sender now carries a stable scheduling key into a packet-aware per-flow
  scheduler, skips blocked flow heads without blocking receive callbacks, and
  retains one bounded ACK-required new-flow reserve on flow-isolating H3.
  Requested NoAck traffic can bypass a full recovery window only on an
  acknowledged, generation-matching contract that can debit the exact next
  bounded logical-group chunk. Contract rotation or exhaustion returns it to
  ordinary admission before serialization. Explicit service-class negotiation
  and a separately budgeted control lane remain open.
- [ ] Bind multi-TCP collapse holds to explicit Transfer recovery items and
  adaptive recovery timing.
- [~] Run 1/4/8-lane A/B tests under loss and 4/16/64-flow workloads. The first
  schema-13 four-flow, 256-KiB one-bar trace completed correctly for 0/1/4/8,
  but every nonzero candidate was slower; keep the default at zero. Repeated
  16/64-flow and interactive-tail campaigns remain before any rollout.

Exit: loss in one data lane does not stall another, memory stays within the
single shared budget, and interactive tails improve under bulk load.

### Phase 4 — recovery and hybrid optimization

- [~] A/B the RTO estimator, cold retry, ACK compression, and gap probing. The
  first full-Transfer A/B isolates a 13.5--14.0 second DATAGRAM completion tail
  versus 2.1--2.3 seconds for legacy H3 at 250 kbit/s upload. A one-DATAGRAM-per-
  Pack workload reproduces the tail with zero reassembly timeouts, pointing to
  coarse synchronized Transfer retry rather than fragmentation. Guarded,
  paced selective-gap recovery cuts representative DATAGRAM completion to
  4.3--5.8 seconds with the production 32-frame route boundary, but legacy is
  still typically 2.2--3.4 seconds. The next candidate is a byte-bounded,
  adaptive Transfer flight window used only for negotiated unreliable
  carriers. Two successful full-TUN runs with the 1,100-byte MTU, the earlier
  one-DATAGRAM data lane, and TCP always under Transfer ACK completed cold
  64 KiB uploads in 9.420 and 26.565 seconds. The spread is too large and the
  slower result is worse than the earlier candidate; this is a correctness
  milestone, not a performance win. DATAGRAM remains disabled for rollout
  until it wins the frozen multi-run A/B. Compact contract heads and bounded
  unreliable-carrier recovery now make the common case substantially faster,
  but an earlier set still spans 6.834--83.934 seconds and includes a separate
  65.280-second cold route-readiness timeout. Safe 1,200-byte QUIC startup and
  correcting only the access-side carrier TUN removed measurement-interval
  decrypt failures, but three stock-dependency runs still included a
  50.251-second route-readiness tail. Readiness telemetry localized the mirror
  failure to the edge/server physical TUN, which retained the same incorrect
  inner-MTU boundary. After correcting both physical endpoints, four cold,
  separate-process runs completed established transfer in 7.676--10.076
  seconds and 248,828--324,850 wire bytes; route readiness clustered at
  6.433--8.718 seconds with zero payload-decrypt failures. A subsequent frozen
  five-run reproduction tightened established transfer to 7.097--8.319
  seconds, wire cost to 248,735--273,208 bytes, and route readiness to
  6.787--8.847 seconds. All nine fully corrected cold runs completed without a
  startup or transfer tail. The refreshed TCP-ACK-invariant campaign then
  compared four same-profile cold controls: fragmented H3 median 8.413 seconds
  / 266,629 bytes, one-DATAGRAM hybrid H3 6.793 seconds / 243,633 bytes, H1
  5.180 seconds / 307,814 bytes, and legacy H3 stream 3.995 seconds / 339,130
  bytes. The hybrid is the only H3 DATAGRAM candidate that improves both time
  and bytes over fragmented H3, so its one-fragment limit is now the production
  default. It remains slower than both reliable-stream controls; broader
  profiles, workloads, and Auto-mode interaction remain release gates.
  Per-write lane classification then removed the route-wide unreliable policy
  from stream and H1 writes. The first retained five-run campaign measured a
  4.060-second / 208,684-byte median. After adding immediate exact-route
  retirement recovery, a second five-run campaign measured 5.000 seconds /
  209,654 bytes. The newer distribution is slower but remains 40.6% faster and
  21.4% lower-byte than fragmented H3, and all ten runs delivered the exact
  hash. Keep both distributions: the change is a strong original-baseline win,
  not proof that the remaining timing variance is solved.
- [x] Compare whole-Pack retry with selectable Transfer fragment recovery.
  Multi-fragment production selection is rejected, so fragment-selective retry
  is no longer needed on the live path. Transfer remains the whole-message
  recovery owner for the one-DATAGRAM lane.
- [~] Benchmark the explicit large-message stream lane against 1/2/3/4
  DATAGRAM fragments. Fragmenting the 1,515-byte contract-only Pack regressed
  route readiness to a 65.146-second timeout. On the corrected full-TUN path,
  the five-run one-fragment hybrid median was 6.793 seconds / 243,633 bytes,
  versus 8.413 seconds / 266,629 bytes for the two-fragment all-packet control.
  Production therefore keeps every message whole: one DATAGRAM or stream.
  Lane-accurate nested recovery reduced the first retained median to 4.060
  seconds / 208,684 bytes without removing the Transfer ACK. The post-failover
  validation median is 5.000 seconds / 209,654 bytes, so more repetitions and
  dynamic route-loss measurements remain required.
- [x] Prevent equal-priority H1/H3 congestion-controller competition inside an
  ordered Transfer sequence. The original Auto selector reshuffled routes for
  every frame; merely trying one preferred route first still spilled onto its
  sibling whenever the preferred queue filled. The retained policy makes that
  condition sender backpressure and changes carriers only after route
  withdrawal. Five fresh 256 KiB warmed Auto uploads measured a 15.190-second /
  864,267-byte / 40-drop median versus the original 27.780-second /
  1,502,911-byte / 158-drop reference. Forced H3 remains faster at a
  13.044-second / 797,068-byte / 19-drop median, while forced H1 is slower at
  24.461 seconds / 1,861,026 bytes / 40 drops. A client-wide first-carrier
  variant was rejected because connection ordering consistently selected H1.
  Keep affinity per destination-keyed sequence so H1 and H3 retain equal
  precedence and remain live in parallel. Flow-identity propagation through
  Transfer and physical-radio validation remain open.
- [x] Keep Transfer ACKs on the carrier of the newest Pack they cumulatively
  acknowledge. The ACK writer uses an immutable, allocation-free per-carrier
  route view, waits on that live carrier under queue pressure, and falls back
  only after withdrawal. Selective and contract-recovery ACKs retain their own
  inbound carrier; cumulative coalescing retains the newest covered Pack, so a
  late H1 retransmit cannot poison a newer H3 ACK. A same-harness three-run
  download comparison improved median tunneled completion 22.1% and wire bytes
  15.4%; the upload control improved completion 7.5% and wire bytes 1.9%. An
  H1 end-to-end timeout may move only the affected selector to healthy H3. A
  broader client-wide timeout preference was not retained: with the corrected
  ACK rule its three-run median was 15.539 seconds / 1,378,095 bytes versus the
  selector-local candidate's 13.255 seconds / 1,216,878 bytes, and none of
  those measured intervals actually activated the broader transition.
- [~] Bound native P2P fast traffic from receiver evidence. The RTP/SRTP lane
  now advertises unreliable semantics through every connected/readiness route
  publication, so Transfer ACK flight control remains active. Carrier readers
  hand off without waiting into independent 16-message (including the
  forwarding item) and 256-KiB limits, and
  the sender reserves one slot outside its 15-message data flight for ACK and
  control traffic. The corrected five-run static campaign improved exact
  delivery from 1/5 to 5/5, median completion from 44.646 to 22.921 seconds,
  and median wire cost from 561,768 to 478,585 bytes, with no receive-queue
  drop. Physical-radio and disruption validation remain open.
- [~] Protect interactive flow admission during a saturated upload. In the
  first three-run diagnostic, flow-fair selection delivered 72/92 fixed-rate
  probes (78.3%) versus FIFO's 23/85 (27.1%), while bulk completion was about
  9.5% slower and used 2.3% more wire bytes. A later five-run A/B separated
  requested NoAck admission from the bounded ACK reserve: explicit NoAck
  admission delivered 145/164 probes (88.4%) versus 137/157 (87.3%) and used
  about 5.1% fewer wire bytes per successful probe, but its median bulk time
  regressed 8.0%. Retain the explicit, contract-safe NoAck semantics together
  with the bounded ACK reserve. Five fresh combined runs delivered every exact
  payload in 12.669--14.510 seconds and 820,723--887,071 bytes, with 13.270-
  second / 832,234-byte medians and 138/154 loaded probes (89.6%). That is 2.9%
  faster, 2.8% lower-byte, and 2.35 delivery points above reserve-selected
  NoAck. The provider recorded 116 explicit bypasses and no reserve use, while
  872 remaining ACK-flight waits and 93 timeout rewrites identify the next
  bottleneck. Hybrid single-message stream yielding was tested and removed
  after two exact runs regressed to 18.374--19.576 seconds and 901,528--907,662
  bytes. Multi-flow service-class and pre-QUIC-lane FIFO work remain open.
- [ ] Tune keepalive, resumption, migration, ECN, and DPLPMTUD.
- [ ] Evaluate alternate congestion control only if qlog shows the default
  controller remains the limiting layer.

Exit: the end-to-end commit owner and any nested in-generation recovery are
explicit for every mode, and no optimization wins only by increasing bytes or
hiding delay in a queue.

### Phase 5 — production UDP path

`vault/main/services.yml` declares UDP stream ports 443 and 8053 for connect in
its latest version. The binary and generated configuration path is implemented
in the working trees; publication, ingress rollout, and production validation
remain:

- [~] Build a minimal first-party NGINX in `warp/lb`. The source commit and
  archive bytes are pinned, the enabled-module list is retained in the build
  log, and the multi-architecture publication target requests maximum
  provenance and an SBOM. A local linux/amd64 image builds successfully.
  Registry publication, signature/verification, and a fully hermetic base and
  dependency pin are still required.
- [x] Pin the first upstream revision that supports `proxy_protocol v2` for UDP
  and prove it with deterministic load-balancer-owned source/module checks and
  a two-client, bidirectional datagram test that parses the emitted PPv2 header.
  The upstream change is
  ([NGINX commit 11d11b5](https://github.com/nginx/nginx/commit/11d11b5)) and is
  assigned to milestone 1.31.4
  ([NGINX issue 1061](https://github.com/nginx/nginx/issues/1061)). NGINX now
  officially documents `proxy_protocol v2;` as a 1.31.4 upstream feature
  ([stream proxy documentation](https://nginx.org/en/docs/stream/ngx_stream_proxy_module.html#proxy_protocol)).
  The image still builds the exact reviewed commit rather than floating on a
  release archive.
- [x] Give local and CI-style Connect tests the same pinned dependency. The
  `warp/lb` `nginx_local` target verifies the source archive SHA-256 and builds
  a minimal native binary under `warp/lb/build/nginx-local`; `connect/test.sh`
  builds that target before the first test and exports its absolute path.
- [x] Make `warpctl` emit explicit `proxy_protocol v2;`, UDP `reuseport`, a
  30-second pseudo-session timeout, and unlimited request datagrams. Preserve
  `proxy_protocol on;` for TCP because that directive still means v1. NGINX
  documents UDP `reuseport` as necessary for same-session packet affinity with
  multiple workers
  ([stream core documentation](https://nginx.org/en/docs/stream/ngx_stream_core_module.html)).
- [ ] Add measured UDP connection/rate/state-exhaustion controls before public
  rollout; the HTTP request-rate settings do not protect stream UDP state.
- [ ] Test NAT rebinding and address migration. Generic NGINX UDP sessions are
  five-tuple based, not QUIC-connection-ID aware; a changed tuple may reach a
  different connect backend. Use backend consistency where possible and retain
  bounded reconnect when migration cannot survive the load balancer.
- [ ] Canary UDP 443 independently from TCP 443 with rollback and saturation
  alarms.

After UDP Proxy Protocol v2 is proven on 443, use the same capability to restore
the two DNS-encapsulated QUIC transports (`H3Dns` and `H3DnsPump`) without making
connect bind the privileged service port:

```text
client H3Dns / H3DnsPump -> edge public IPv4 UDP/53
                             |
                  warpctl interface-scoped DNAT
                             v
                 active LB endpoint for UDP/8053
                             |
                  NGINX UDP + Proxy Protocol v2
                             v
                  connect server UDP/8053
                             |
             PPv2 decode -> DNS packet decode -> QUIC
```

- [x] Keep `PlatformTransportSettings.DnsPort` at public port 53. Change the
  connect server's `ListenDnsPort` from 53 to 8053; client configuration must
  not learn the internal port. Both defaults are pinned by tests.
- [x] Add UDP 8053 to the latest connect service's `udp_stream_ports` and add
  `8053: connect` to the latest load-balancer `udp_stream_port_services` in
  `vault/main/services.yml`. Do not restore the historical direct `53: connect`
  listener.
- [x] Make NGINX listen on UDP/8053 with `reuseport` and forward Proxy Protocol
  v2 using the explicit `proxy_protocol v2;` directive to the connect UDP/8053
  upstream. Preserve the existing server transform
  order: strip/validate PPv2 first, then run `PacketTranslationModeDecode53`,
  then hand the decoded packets to QUIC. The generated main-edge configuration
  passes `nginx -t` inside the pinned image.
- [x] Make Warp own the IPv4 public-port translation. The latest
  `vault/main/services.yml` declares `udp_forward_ports: {53: 8053}`; generated
  LB units pass the deterministic mapping to `warpctl`, which resolves the
  active LB endpoint and installs an exact
  `<interface IPv4>:53 -> <LB IPv4>:<active port for service 8053>` DNAT. The
  new rule is inserted before stale rules are removed, config withdrawal
  removes the owned alias, rules for another interface or unscoped deployment
  ports are left alone, the service target is not also emitted as a direct
  public alias, and no IPv6 or TCP port-53 rule is created. Config validation
  rejects missing targets, identity/chained mappings, and direct-port,
  allocatable-port-pool, forced-external-port, or per-interface conflicts. A
  separate edge firewall audit should still prove
  that no unrelated listener or incidentally allocated port exposes UDP/8053.
- [ ] Activation order is server listener, NGINX listener/upstream, internal
  health check, external UDP/53 DNAT, end-to-end health check, then client
  rollout. Rollback removes/withdraws the public DNAT first, lets Auto fall back,
  drains UDP pseudo-sessions, and only then removes the listener/upstream.
- [ ] Health-check an authenticated DNS-encoded QUIC exchange through public
  UDP/53 for both `H3Dns` and `H3DnsPump`; a plain DNS query is not sufficient.
  Verify that connect observes the original source address from PPv2 and that
  malformed/spoofed PPv2 is rejected.
- [ ] Add per-mode connection, handshake, packet, retry, fallback, byte,
  malformed-envelope, and rate-limit metrics. Keep them separate from ordinary
  QUIC/443 so port-53 interception or carrier behavior is visible.
- [ ] Exercise source spoofing, amplification limits, state exhaustion, DNS
  middlebox rewriting, fragments, truncation, NAT rebinding, deploy/conntrack
  draining, and a router or NGINX restart before canarying public UDP/53.

Exit: UDP Proxy Protocol v2 source identity, affinity, fallback, rate limits,
and rollback are proven in staging and canary production for 443; the DNS modes
ship only after their separate UDP/53-to-8053 gates pass.

### Phase 6 — rollout

- [ ] Gate DATAGRAM, fragmentation, lanes, scheduler, hybrid, and migration
  independently by negotiated capability and server rollout flag.
- [ ] Start with staff/canary, then low percentage, then cellular cohorts;
  compare against simultaneous H1 control cohorts.
- [ ] Auto-select from observed path behavior, not a platform label alone.
  Keep decisions sticky and probe with a strict byte budget.
- [ ] Verify iOS Network Extension memory and wakeups, Android always-on
  lifecycle, Windows service recovery, Linux service/keychain integration, and
  connect server limits.
- [ ] Verify IPv4-only settings on every app and provider throughout rollout.

Exit: release gates hold in device cohorts with no security, memory, battery,
or fallback regression.

## Required test matrix

- Nonblocking: full Pack, ACK, forward, signal, stream, TUN, and provider return
  handoffs; a blocked destination must not delay an unrelated one.
- Recovery: single loss, burst loss, ACK loss, duplicate, reorder, long gap,
  outage, and connection-generation replay.
- Fragmentation: every boundary size, last-fragment loss, duplicate fragments,
  inconsistent metadata, timeout, peer/global budget exhaustion, complete-
  datagram security inspection at client and provider, fragmented-TCP policy
  rejection, and fuzzing.
- Lanes: deterministic loss in lane A with progress in lane B; all routing-key
  delivery paths; legacy peer fallback; lane-count mismatch; shared budget.
- Collapse: SYN/RST, FIN, ACK/window progress, zero window, wraparound, held
  retransmit, Transfer success/failure, route change, direct, and NoAck.
- QUIC: cold/resumed/0-RTT-safe auth, UDP blocked, NAT rebind, address change,
  MTU reduction, idle wake, load-balancer backend change, and fallback.
- DNS transports: public UDP/53 to edge/server UDP/8053, both `H3Dns` and
  `H3DnsPump`, original-source PPv2 propagation, transform order, malformed
  envelopes, router DNAT activation/rollback, conntrack drain, and independent
  fallback from a blocked or rewritten port 53.
- Performance: all Phase 1 profiles and workloads with direct/H1/current-H3
  controls, at least five recorded runs.
- Memory: retain the long DeviceLocal + DeviceRemote synthetic run with web,
  mail, blocked, bulk, loss, path churn, and post-burst recovery, then run the
  physical-device campaign above. Assert logical byte/fragment/lane bounds,
  OS-level footprint plateau and recovery, native packet-flood progress, and
  complete transport/budget cleanup.
- Policy: encryption required/opportunistic/off matrices where supported, kill
  switch, CFAA/SMTP, provider eligibility, and no IPv6 advertisement or route.

All concurrency, lifecycle, generation, and saturation regressions use explicit
barriers or state transitions. Sleeps and scheduler luck are only safety
timeouts, following `CODESTYLE.md`.

## Open questions

1. What conservative DATAGRAM payload works across the actual iOS, Android,
   desktop, carrier, and NGINX paths, and how often does DPLPMTUD safely raise it?
2. Should fragments be individually recoverable Transfer units, or does whole
   Pack retry use fewer bytes once ACK overhead and bookkeeping are included?
3. Does `quic-go` DATAGRAM throughput remain sufficient after fragmentation on
   10/2 and 50/10 Mbit/s links, or is a separate large-message stream justified?
4. If the hybrid stream is retained, what exact end-to-end commit/replay rule
   survives QUIC connection loss without simultaneous periodic Transfer retry?
5. Are 4 or 8 logical data lanes enough to isolate common flows without
   multiplying ACK/control overhead?
6. Which packets qualify for interactive priority without allowing an
   application to monopolize the reserved class?
7. What queue-delay target balances radio batching/energy against interactive
   latency at 64 and 250 kbit/s uplinks?
8. Can a production NGINX UDP path preserve backend affinity through the path
   changes that matter, or must connect externalize resumption/replay state?
9. Which authentication messages, if any, are safe and valuable in 0-RTT?
10. How long should UDP-blocked evidence and successful transport choice remain
    sticky across network changes?
11. What authenticated health signal and conntrack-drain threshold should gate
    the now-Warp-owned UDP/53-to-8053 DNAT during activation and rollback?
12. On real iOS and Android VPN routes advertised at 1,100 bytes, do common QUIC
    stacks emit fragmentable IPv4 traffic, adapt without falling below QUIC's
    1,200-byte Initial requirement, or fail the send with message-too-large?
    Resolve this from packet capture and application-visible errors on physical
    devices before treating synthetic inner-QUIC success as a mobile result.

## Findings log

| Date | Finding | Consequence |
| --- | --- | --- |
| 2026-08-17 | Current H3 is a custom QUIC transport using one reliable bidirectional stream, not HTTP/3 requests and not QUIC DATAGRAM. | Legacy H3 stacks reliable recovery with Transfer for every routed frame. Move common packet data to DATAGRAM while retaining Transfer as end-to-end commit authority across every carrier generation. |
| 2026-08-17 | Transfer Pack coalescing and the 1440-byte inner MTU can exceed a safe QUIC DATAGRAM payload. | Bounded Transfer-aware fragmentation or a measured MTU/stream alternative is required before DATAGRAM production use. |
| 2026-08-17 | `Client.run` passed the 15-second `BufferTimeout` to Pack and ACK handoffs despite the nonblocking receive invariant. Zero-timeout replacement could also wait for worker exit. | Corrected in Phase 0 with counted drops and deterministic unrelated-source progress tests. |
| 2026-08-17 | Nonblocking admission removes mechanical pump blocking, but one ordered Transfer sequence can still hold unrelated inner flows behind a missing sequence number. | Measure and prototype a small bounded set of logical lanes; do not use transport stream IDs as keys. |
| 2026-08-17 | Existing multi-TCP collapse prevention already suppresses duplicate inner retransmits only when Transfer ACK recovery owns the path. | Preserve it and bind its hold/release to explicit recovery mode and Transfer item state. |
| 2026-08-17 | Historical PERFVAR data shows grouping helps, large queues can fail, and current H3 can be far slower than H1 even on a clean mobile surrogate. | Treat QUIC as one component; prioritize recovery ownership, scheduling, measurement, and bounded queues. |
| 2026-08-17 | The NGINX upstream UDP Proxy Protocol v2 change exists, while the required release/deployment path is not yet established. | Use a reproducible first-party `warp/lb` build and canary; do not block the local prototype on production LB work. |
| 2026-08-17 | The server already has DNS-encoded QUIC listeners and applies PPv2 decoding before DNS packet translation, but its default still binds port 53; the latest load-balancer config exposes only UDP 443. | After PPv2 works on 443, keep clients on public UDP/53, DNAT at IPv4 ingress to edge UDP/8053, proxy with PPv2 to connect UDP/8053, and gate both DNS modes independently. |
| 2026-08-17 | Zero-timeout admission into a zero-capacity Pack/ACK rendezvous can repeatedly miss both workers under a bidirectional burst; the old exact-delivery stress test stalled even though neither shared pump blocked. | Production and broad end-to-end tests need a positive bounded admission capacity. Keep zero/full behavior in deterministic saturation tests, and use drops plus sender recovery rather than zero-capacity channels as the overload mechanism. |
| 2026-08-17 | The existing `mobile-poor` profile starts at 10/2 Mbit/s and does not cover the 64–250 kbit/s uplink corner. | Added three explicit `cell-edge-*` device profiles with one-packet startup burst credit and clean provider access; they are engineering stress points pending field-trace calibration. |
| 2026-08-17 | Several exact-delivery and integration fixtures violated the receive-callback rule by using zero-capacity Transfer queues, blocking callback collectors, direct reply sends, or goroutine-per-packet echoes. This made loss/retry tests look like production stalls and retained workers across the package run. | Converted the affected fixtures and `connectctl sink` to positive bounded queues, zero-wait handoffs with explicit drops, fixed sender workers, owned callback snapshots, and joined teardown. Keep overload behavior in focused saturation tests. |
| 2026-08-17 | Repeated complete non-short Connect runs now finish in 523–562 seconds after the callback/fixture corrections; the earlier ten-minute package timeout was not reproduced. | Treat the prior timeout row below as resolved for this working tree, while continuing the non-Client callback audit and dedicated long-duration memory measurement. |
| 2026-08-17 | PERFVAR's one-hop P2P fixture placed the device on the right endpoint but applied the application-oriented profile without translating directions. Static P2P upload therefore used the configured download link and vice versa, and direct calibration mirrored that inversion. | Corrected construction and calibration so forward/reverse always mean device upload/download; schedule version 2/schema 4 prevent mixing old directional records. |
| 2026-08-17 | Existing live-link primitives were confined to standalone correctness tests and updated every exchange access link, so they could not produce a fair device-only campaign trace. | Added hash-visible, measured-start dynamic profiles with targeted device-link updates, direct-P2P orientation translation, acknowledged event offsets/link names, payload-duration bounds, and incomplete-trace failure. Provider access remains clean. |
| 2026-08-17 | `Client.ReceiveStats()` exposed nonblocking Pack/byte and ACK admission loss, but PERFVAR did not snapshot it, so a low-bar run could not correlate application behavior with receive-handoff saturation. | Added interval-scoped device, provider, and stream-intermediary receive-handoff observations. The fixed-point baseline now retries if these counters or Client identities change across its reset pass. |
| 2026-08-17 | Locally generated ICE candidates were sent with ordinary sender backpressure directly inside Pion's candidate callback. A full Transfer queue could therefore park Pion event delivery even though inbound signal replies already used timeout zero. | Candidate-callback sends now carry `signalSendNonBlocking`; deterministic saturation verifies callback return and zero blocking sends. Sender-owned initial offer/control work retains sender backpressure. |
| 2026-08-17 | The device RPC websocket reader blocked first on a shared receive-byte budget and then on either logical stream's full queue. One stalled reverse-RPC consumer could therefore head-of-line block the forward RPC stream indefinitely. | Receive admission is now zero-wait. Because a reliable RPC byte fragment cannot be skipped, saturation closes and drains the complete mux generation so normal DeviceRemote reconnect can recover without silent stream corruption. |
| 2026-08-17 | H1/H3 carrier readers waited up to the read timeout on a full route, both P2P readers propagated route pressure, and connect-server socket/exchange readers also waited. | All receiver-owned route offers are now zero-wait and counted. Data refusal feeds Transfer recovery; reliable H1 control refusal terminates that carrier generation. |
| 2026-08-17 | Resident Client callbacks still slept in the control limiter or performed locks, active-contract storage checks, forward construction, and optional sender waits inline. | Callback ingress is now bounded and zero-wait. One ordered control worker and destination-stable forward shards own all slow work; control overflow forces replay via reconnect. |
| 2026-08-17 | NGINX's new backend directive keeps version 1 as the meaning of `proxy_protocol on;`; UDP source metadata requires explicit `proxy_protocol v2;`. The current warp template emits only `on`. | The pinned NGINX build and warpctl config must land together. Prove UDP/443 first, then reuse the same explicit v2 path for edge/server UDP/8053 behind public UDP/53 DNAT. |
| 2026-08-17 | The exact upstream commit `11d11b5f0d3d8ace5215e1a77918e9dc219ce7db` preserves two original client addresses and bidirectional payloads across 64 alternating 1400-byte UDP datagrams through two NGINX workers. The same test carries 1400-byte requests and replies through PPv2-before-DNS decoding for both `H3Dns` and `H3DnsPump` envelope modes. | UDP upstream PPv2 and the server transform order are now proven locally. Keep the test gated by the exact-capability binary until an official release containing the change is the production pin. |
| 2026-08-17 | The former 15-minute generic stream timeout could outlive the server's 45-second PP source mapping and make a live NGINX UDP pseudo-session lose its reply route. | UDP stream servers now use a 30-second timeout, while TCP retains 15 minutes; a server regression test pins the required timeout ordering. |
| 2026-08-17 | Client UDP/53, server UDP/8053, latest vault service allocation, generated NGINX PPv2 forwarding, and the pinned image config are wired and validated. The repository has only a prose EdgeRouter setup note, not versioned public 53-to-8053 ingress automation. | Do not enable the DNS modes yet. The IPv4 DNAT, direct-8053 firewall policy, authenticated health checks, metrics, abuse controls, canary, and rollback remain deployment gates. |
| 2026-08-18 | Warp now carries a validated, versioned `udp_forward_ports` mapping from public 53 to LB service 8053 and propagates it only to LB units. `warpctl` reconciles the exact interface-address DNAT add-before-delete, removes withdrawn/stale aliases, preserves other interfaces and unscoped deployment rules, and deliberately emits neither IPv6/53 nor a direct public target alias. | The ad-hoc ingress-router configuration gap is closed in code. Production activation still waits for authenticated public-path health, UDP abuse/state controls, conntrack-aware drain, edge firewall verification, a canary, and published/signed LB artifacts. |
| 2026-08-17 | Validating every generated main-edge config exposed two legacy blocks without capacity sizing. The generator consequently omitted NGINX's mandatory `events` section, making those historical configs invalid. | Legacy unsized blocks now emit the NGINX-default 512 worker connections. A regression test pins the fallback and all 13 generated main-edge configs pass the pinned image's `nginx -t`. |
| 2026-08-17 | The NGINX capability proof originally lived only with the connect server, so `warp/lb` could change its source or modules without a package-owned regression. | `warp/lb` now pins the capable commit, archive digest, checksum verification, stream module, image label, and source-build policy; its binary test independently parses PPv2 source metadata and verifies bidirectional UDP for two clients. |
| 2026-08-17 | QUIC transport-parameter negotiation alone is not enough to version the application fragment envelope, while changing an echoed `Auth` response unconditionally would break old clients. | Added separate authenticated offer/accepted fields. Old servers echo an acceptance of zero, old clients offer zero, and only matching new/new peers with bilateral RFC 9221 support move routed frames off the stream. |
| 2026-08-17 | `quic-go` copies `SendDatagram` input, blocks a sender only after its bounded 32-datagram queue fills, copies receive payloads into its bounded 128-datagram queue, and returns the current maximum in `DatagramTooLargeError`. | Reuse one bounded send scratch buffer (now a 1,360-byte target), permit sender backpressure, pool only complete reassembled Transfer frames, and retry one path-MTU shrink under a new carrier id. Transfer—not QUIC—recovers any partial or silently discarded message. |
| 2026-08-17 | A first bounded reassembler draft returned pooled buffers and adjusted its shared byte budget while holding the local state mutex. | Split state mutation from external allocation/release. Pool and shared-budget operations now occur outside `stateLock`; expired and corrupt ids enter a bounded retirement window so late fragments cannot resurrect their storage lifetime. |
| 2026-08-17 | A negotiated DATAGRAM generation no longer needs the legacy H3 writer's 64 KiB stream batch allocation. | Allocate that batch lazily only when the hybrid selects stream. The candidate retains a 1,360-byte sender scratch plus explicitly bounded incomplete-message metadata and payload bytes. |
| 2026-08-17 | A database-free full-Transfer A/B on `cell-edge-1m-down-250k-up` completed legacy H3 in 2.16--2.25 seconds but H3 DATAGRAM in 13.54--13.57 seconds over three repeated paired runs. First-message latency remained about 0.25 seconds and DATAGRAM used fewer wire bytes, but its Pack rewrites rose from 7--11 to 26--27. | H3 DATAGRAM is a release blocker, not a production optimization, until Transfer recovery improves; lower byte cost does not compensate for a roughly 6.1x completion regression. |
| 2026-08-17 | Keeping every Transfer frame below the 1,150-byte target still produced a 13.96-second DATAGRAM completion versus 2.22 seconds for legacy H3, with zero reassembly timeouts. Selective ACKs acknowledge later Packs, but there is no selective-gap fast retransmit; a missing earlier Pack follows the cold 2-second retry and exponential 4/8-second backoff, while a lost cumulative ACK can park selectively acknowledged state behind a coarse probe. | Fragmentation is not the primary tail cause. Prioritize a once-per-gap, reordering-safe, paced recovery signal and adaptive cumulative-ACK probe, then rerun the same A/B before lanes or rollout work. |
| 2026-08-17 | Guarded selective-gap recovery materially improves the DATAGRAM tail, but an aggressive automatic two-probe train reached 2.604 seconds only by issuing 28 gap plus 32 tail writes. Removing that retry train and retaining bounded receiver-evidenced recovery yields about 4.3--5.8 seconds with the production 32-frame carrier route, still slower than legacy. | Keep the reordering-safe scoreboard and telemetry, reject the retry-storm candidate, and solve admission/flight size before further shortening timers. Recovery that wins only by injecting duplicates does not meet the bytes or congestion release gates. |
| 2026-08-17 | `quic-go` v0.61.0 starts with a 32-packet congestion window and has a 32-entry DATAGRAM send queue. The `cell-edge-1m-down-250k-up` simulator queue holds about 13 1,280-byte packets (roughly 500 ms), so the initial DATAGRAM flight can overrun it before Transfer receives delivery evidence; measured runs show about 25 selective-gap recoveries and roughly 40 carrier queue drops. The legacy stream hides the same burst behind QUIC retransmission. | Recovery tuning alone cannot make DATAGRAM competitive. Add carrier-specific, byte-bounded adaptive Transfer flight control that stays below the initial low-bar queue, opens on ACK progress, reduces on gap evidence, never limits reliable carriers, and does not block receiver callbacks. `quic-go` exposes no public initial-congestion-window setting, so avoid a dependency fork as the first fix. |
| 2026-08-18 | `AllowDirect` previously converted tunneled TCP to Transfer `NoAck` on the assumption that its selected carrier was reliable. That guarantee ends when the carrier disconnects or a route is replaced, leaving no end-to-end commit for an accepted TCP packet. | TCP now always requires a Transfer ACK. The invariant is enforced both in route policy and at the final singleton/group packet-to-Transfer boundaries, so an explicit lower-level `NoAck` hint cannot bypass it. Direct TCP is consequently included in collapse prevention. |
| 2026-08-18 | The only stream traffic in successful cold 1,100-MTU full-TUN runs was an exact 1,515-byte contract-only Pack repeated 11 and 30 times; it contains no tunneled application frame. Moving that control item to two DATAGRAM fragments caused route readiness to time out after 65.146 seconds on the same seeded one-bar profile. | Keep application-bearing frames that fit on the one-DATAGRAM lane and keep contract-only/oversized frames on the stream lane. Do not infer that fragmenting a small control item is cheaper merely because Transfer can retry it. |
| 2026-08-18 | After TCP became ACK-required, the dedicated stall watchdog repeatedly held a slow-route verdict because no sibling proved the uplink, but the resize loop independently reread raw `sendStalled` state and removed the exit at 34.747 seconds. That bypassed the watchdog's busy-probe, uplink, shared-fate, and quarantine gates. | The watchdog is now the sole send-stall conviction owner. Resize reaps a canceled client but cannot manufacture a second ungated conviction; a source-anchor regression prevents that call path from returning. |
| 2026-08-18 | In negotiated hybrid mode, both endpoints retained an application read deadline on the otherwise idle reliable stream. QUIC DATAGRAM activity cannot satisfy that deadline, and the same writer that emits stream pings can block behind quic-go's bounded 32-DATAGRAM send queue. | Clear the post-auth stream deadline only for negotiated hybrid connections and enable QUIC-level keepalive on client and server. QUIC connection idle detection still closes a dead peer, while liveness no longer depends on the possibly blocked application writer. |
| 2026-08-18 | The remaining slow completion had 336 device-side successful `SendDatagram` queue admissions but only 139 provider-side complete receives, while reporting no DATAGRAM integrity/reassembly errors and no Client Pack/ACK handoff drops. The physical profile counted 19 loss and 15 queue drops on the constrained uplink, which does not by itself explain 176 Transfer timeout rewrites. | The current H3 `SentMessageCount` is queue admission, not proof that quic-go placed the DATAGRAM frame on wire. Add packet-emission and quic-go receive-queue visibility before changing Transfer recovery again; the residual tail is below the Transfer admission boundary. |
| 2026-08-18 | Exact qlog and raw-TUN fingerprints showed that every successfully received QUIC packet matched a client send, while decrypt failures matched neither client qlog sends nor complete client UDP payloads. The rejected packets were consistently 1,400/1,256-byte fragmentation artifacts; the same failures persisted when the first QUIC key update was delayed from 100 to 100,000 packets. | The earlier key-update correlation was false. Start QUIC at its legal 1,200-byte floor and keep DPLPMTUD enabled; do not raise the lower bound above a 1,280-byte cellular path's post-IP/UDP capacity. The dependency-only key experiments are fully reverted. |
| 2026-08-18 | PERFVAR configured both the device/provider access TUN and the edge/server mirror TUN with `networkProfile.InnerMtu` (1,200 bytes on the cell-edge profile) while separately enforcing a 1,280-byte outer link. This fragmented even a legal 1,200-byte QUIC UDP payload before the outer-MTU gate and hid the original packet from link counters. Correcting only the access side left device-side decrypt failures during route readiness; interval telemetry exposed the symmetric edge-side error. | Every physical carrier TUN now uses the smallest directional outer MTU; only the application TUN uses the nested VPN MTU. A deterministic helper test pins the distinction. This is a benchmark-fidelity correction, so historical, one-sided, and fully corrected results remain labeled separately. |
| 2026-08-18 | One worst-case 1,100-byte tunnel packet serializes to 1,288 encrypted Transfer bytes and 1,316 bytes with the H3 envelope. It cannot fit one QUIC DATAGRAM on a 1,280-byte path. The exact one-DATAGRAM inner-MTU ceilings are 944 bytes for one packet and 934 bytes for two coalesced packets totaling the MTU. | Keep the product MTU at 1,100. A global 900-byte MTU improved the all-packet H3 candidate but regressed H1 and legacy H3, so global MTU reduction is rejected. Select the carrier lane per complete Transfer message instead. |
| 2026-08-18 | Five refreshed cold runs with TCP always Transfer-ACKed measured fragmented H3 at 8.413 seconds / 266,629 bytes median and one-DATAGRAM hybrid H3 at 6.793 seconds / 243,633 bytes. Every hybrid run delivered the exact hash with both lanes active, one fragment per DATAGRAM message, and zero integrity or reassembly failure. | Set the production H3 fragment limit to one. Messages that cannot fit one current-path DATAGRAM use stream; retain multi-fragment support only as an explicit compatibility/benchmark setting. This improves the H3 candidate by 19.3% in completion time and 8.6% in wire bytes without changing Transfer ACK semantics. |
| 2026-08-18 | The same refreshed control set measured H1 at 5.180 seconds / 307,814 bytes median and legacy H3 stream at 3.995 seconds / 339,130 bytes. The hybrid saves 20.9% and 28.2% wire bytes respectively, but is 31.2% and 70.0% slower. | The hybrid wins over fragmented H3, not over every carrier. Keep H1 and H3 healthy in parallel in Auto and do not claim broad low-bar superiority until mixed Auto, multi-flow, direction, profile, and dynamic-path campaigns pass. |
| 2026-08-18 | Expanding the unreliable Transfer flight to 12 KiB after one acknowledged cold flight improved typical completion only by producing 9--27 queue drops and extra inner retransmits. At 1,100 MTU it was especially unsafe because one logical message could consume two DATAGRAMs. | Rejected and removed. Flight growth remains receiver-evidenced and loss-responsive. Any future optimization must use the actual selected carrier lane; it cannot infer that every write on a negotiated hybrid H3 connection is unreliable. |
| 2026-08-18 | Publishing negotiated H3 as route-wide `Unreliable` made Transfer count reliable hybrid-stream writes, and even writes actually accepted by an equal-priority H1 sibling, against the DATAGRAM flight and two-second retry policy. A first exact-write classifier improved time but still produced a 5.924-second / 299,907-byte median because messages admitted before quic-go's path-size feedback were misclassified and stream writes formed a duplicate Transfer retry train. | Carry the exact selected route's carrier disposition back to `SendSequence`. Probe quic-go's synchronous current DATAGRAM limit before route publication, update it atomically after path shrink, and count only writes that actually use DATAGRAM as unreliable flight. Reject the intermediate byte-regressing candidate. |
| 2026-08-18 | A hybrid stream write is still end-to-end Transfer-ACKed, but retrying it every two seconds duplicates QUIC's ordered reliable recovery while the constrained uplink drains. Deferring the Transfer retry to eight seconds produced a first five-run 4.060-second / 208,684-byte median with exactly 62 stream writes in every run, versus 6.793 seconds / 243,633 bytes before lane-accurate recovery. | Keep TCP `ack=true`; change only nested recovery timing. QUIC owns in-generation stream retry, while Transfer remains the commit owner and eventual recovery layer. The exact selected lane, never negotiated H3 capability alone, chooses the flight and retry policy. |
| 2026-08-18 | An eight-second Transfer interval is unsafe if the QUIC connection that accepted a hybrid stream write disappears: that retired generation cannot deliver its buffered bytes. Conversely, merely adding another equal-priority carrier does not prove loss on the original route. | Track the exact accepting route per pending hybrid-stream item. On route-generation change, immediately reschedule only items whose accepting route was withdrawn; do not retry when that route remains active. Focused normal/race tests pin both cases. The post-change five-run median was 5.000 seconds / 209,654 bytes, retaining the byte improvement and a large original-baseline win while exposing unresolved timing variance. |
| 2026-08-18 | Schema-6 outage telemetry showed that H3 eliminated the H1 control's 99 device-side timeout rewrites, but the H3 provider still entered the small-message flight barrier 137 times for 18.212 seconds. H3 completed in 43.298 seconds versus H1's 46.462 seconds, at 6,097,889 versus 6,023,722 carrier bytes. | Keep TCP Transfer ACKs and the current H3 hybrid. Treat provider flight recovery as an optimization opportunity, not proof that the safety bound is wrong; every candidate must pass both the 13-packet static queue and the live-outage trace. |
| 2026-08-18 | Raising the cold message flight from 8 to 12 improved the static five-run median to 3.933 seconds / 209,524 bytes, but two recorded outage traces took 45.118 and 44.887 seconds. Raising the loss floor from 4 to 8 then produced 257,383 bytes, 74 stream writes, and 30 inner-TCP retransmits in one static run. | Rejected and removed. Keep the 8-message cold limit and 4-message loss floor. A low byte count does not authorize a larger packet-count burst, and a transient-outage optimization may not regress the static one-bar queue. |
| 2026-08-18 | Draining up to eight ready small Packs into one DATAGRAM produced a 3.345-second static median, but one H3 run reached 6.151 seconds, another incurred 29 inner-TCP retransmits, and H1 wire bytes rose 2.9%. A four-frame bound repeated the correlated-loss failure with 25--29 retransmits. Doubling additive message recovery raised outage gaps from 3 to 18 and completion to 45.772 seconds. | Rejected and removed. Do not collapse multiple inner TCP ACK packets into one lossy fate or outgrow receiver evidence faster than the shaped uplink drains. Retain the original two-Pack opportunistic coalescer and +1 additive message recovery. |
| 2026-08-18 | In the first exact warmed rate-collapse pair, hybrid H3 completed in 44.832 seconds / 6,063,883 carrier bytes versus H1's 53.775 seconds / 6,805,198 bytes. H3 reduced forward queue drops from 364 to 201 and device timeout rewrites from 390 to 2. The cold pair was effectively tied at 49.373 versus 49.678 seconds. | Hybrid H3 has its first material current-tree dynamic win: 16.6% faster and 10.9% lower-byte on the warmed trace. Keep it diagnostic until five paired traces reproduce it, then continue through live-MTU, mixed-Auto, multi-flow, direction, and physical-radio gates. |
| 2026-08-18 | Two exact live-MTU pairs reduced the outer path from 1,400 to 1,280 bytes and restored it during active 2 MiB uploads. Cold H3/H1 completed in 38.136 / 48.555 seconds and warmed H3/H1 in 37.404 / 49.385 seconds. H3's largest carrier packet was 1,228 bytes with zero MTU drops; H1 submitted 1,384-byte packets and recorded 10--16 MTU drops. | The current hybrid has no evidenced live-MTU regression. H3 is 21.5--24.3% faster, 2.9--7.5% lower-byte, and has about half the queue drops in these pairs. Keep the one-complete-DATAGRAM rule and 1,200-byte QUIC floor; advance to mixed-Auto without changing Transfer ACK safety. |
| 2026-08-18 | A fresh five-run 256-KiB warmed-upload matrix measured all correctness-valid samples at Auto 14.983 s / 876,468 B, H3 15.268 s / 818,234 B, and H1 30.285 s / 1,976,566 B median. Auto remained H3-affine for payload while keeping H1 healthy; two Auto, three H3, and one H1 records missed only the conservative calibration-headroom rule. | H3 is 49.6% faster and 58.6% lower-byte than H1; Auto is 50.5% faster and 55.7% lower-byte than H1. Auto is within 1.9% of H3 time but uses 7.1% more bytes. Keep both equal-priority carriers live, do not stripe one ordered sequence, and report calibration-invalid counts separately from payload correctness. |
| 2026-08-18 | Native fast P2P was registered as if it were a reliable carrier, and the endpoint-readiness rematch replaced any initial carrier properties with their zero value. The initial five-run matrix consequently allowed 692--820 fast sends while its old four-entry receive route dropped small bursts; only 1/5 payloads were exact. | Publish the RTP/SRTP fast lane as unreliable on both connected and readiness updates. Keep Transfer ACKs, but activate the receiver-evidenced unreliable flight for this carrier instead of assuming transport-up proves delivery. |
| 2026-08-18 | A count-only P2P flight cap exposed a queue-shape conflict: four slots avoid large-frame memory growth but can reject tiny ACK/control bursts; reserving still more slots removed drops only by reducing throughput. | Separate receive count from bytes. Sixteen messages with a hard 256-KiB aggregate ceiling retain the former four-by-64-KiB worst-case payload memory, while a 15-message Transfer data flight leaves one untracked ACK/control slot. The retained five-run result is 5/5 exact, zero queue drops, 22.921 s / 478,585 B median. |
| 2026-08-18 | Loaded-latency instrumentation showed 20--24 H3 flow-reserve selections per run but zero reserve uses. Those selected Packs were requested NoAck logical groups: the reserve was acting as an implicit scheduler permission even though the messages never entered ACK flight. | Make NoAck admission explicit and contract-safe for the exact next serialized chunk. Keep the single bounded reserve for genuinely ACK-required new flows. Schema 9 records NoAck bypass, reserve selection/use, and both endpoints' H3 DATAGRAM/stream lanes so the two mechanisms cannot be conflated again. |
| 2026-08-18 | quic-go's packet packer takes one submitted DATAGRAM before retransmitted or new STREAM data, but URnetwork's H3 writer consumes both lanes from one FIFO and may block while handing a ready stream batch to QUIC. Disabling hybrid stream batching did not expose a latency win: two runs were 32--39% slower than the retained median and 8--9% higher-byte. | Keep stream batching. If lane submission is split, preserve one byte-bounded ownership budget and prove DATAGRAM progress while a stream handoff is blocked; do not approximate that architecture by shrinking batches. |
| 2026-08-19 | The production 1,100-byte application MTU is below QUIC's mandatory 1,200-byte Initial payload. The synthetic gVisor TUN emits two IPv4 fragments, including a nonstandard-looking DF+MF/offset combination; the previous NAT parser dropped them, and the previous UDP return path split one payload into corrupt independent datagrams. | Retain bounded IPv4 reassembly and real fragmentation. Inspect only a canonical complete UDP datagram at both device and provider security boundaries, forward the original ordered fragments through Transfer, reject fragmented TCP before SMTP/CFAA routing, and accept then normalize gVisor's emitted flag form. Treat whether real iOS/Android kernels fragment or reject this send as a separate physical-device gate. |
| 2026-08-19 | Race instrumentation stretched a clean loaded phase past 1,000 probes and exposed overlap between the old loaded range beginning at 1,000 and post-load range beginning at 2,000. A late, valid loaded reply was consequently reported as corrupt post-load data. | Assign idle, loaded, and post-load probes disjoint 64-bit sequence ranges and pin a late reply after 5,000 loaded sequences. Keep the ordinary 2 MiB directional correctness phases; scale only the race-instrumented payload to 256 KiB so the race gate checks ownership and concurrency instead of timing out on instrumentation overhead. |
| 2026-08-19 | gVisor emits locally fragmented DF packets with IPv4 identification zero. That identity is sufficient while each group stays contiguous, but parallel H3 stream and DATAGRAM routes can interleave two retained groups and make the downstream reassembler treat them as one datagram. | After complete-datagram policy inspection, assign every retained group a fresh nonzero identification, clear DF on its owned fragments, and recompute each header checksum before asynchronous routing. A deterministic interleaving regression proves both groups reassemble independently. |
| 2026-08-19 | The 1 s RTT P2P 32 MiB gate first transferred only 3,381,116 bytes of its 12.5 MiB BDP warmup before timing out behind the old 15-message destination flight. Removing that cap progressed much farther, but then exposed zero-wait drops at the default 32-slot Client ReceiveSequence even though the P2P carrier queue itself dropped nothing. Matching both count bounds produced one isolated pass, but a combined repetition then filled essentially all 256 KiB of carrier payload and dropped one 1,206-byte message. | Keep every P2P mode Transfer-ACKed and classify its complete bounded handoff as unreliable. Derive a 255-message destination flight from the 256-message P2P queue, give the Client handoff matching 256-message headroom, and independently cap both handoffs at 256 KiB. Tighten only P2P's data flight to 240 KiB, retaining 16 KiB under the unchanged carrier ceiling for untracked ACK/recovery/contract/probe traffic. The old timer then reached 31,616,264/33,554,432 bytes with zero queue drops, gaps, timeouts, or reductions; correctness-only deadline headroom preserved the slower result and the exact gate passed in 272.99 s. |
| 2026-08-19 | One ordered Transfer sequence is a real cross-flow loss domain, but an additive wire field alone is not a safe rollout. A peer could ignore the field or capability evidence could outlive the lane-0 sequence that negotiated it. | Retain the bounded logical-lane implementation as an opt-in correctness feature: stable exact directional five-tuple hashing over at most eight lanes; capability accepted only from a live lane-0 ACK; base-lifetime pinning and downgrade cancellation; exact per-sequence ACK routing; and shared lazy send/receive budgets. Keep the production default at zero until latency measurements win. |
| 2026-08-19 | The first schema-13 one-bar four-flow comparison completed exact and calibration-valid at every 0/1/4/8 setting, but nonzero lanes took 5.8--10.2% longer. They reduced wire bytes by 36.2--68.6%; allocation bytes improved for 1 and 8 but regressed for 4. | Do not trade completion for byte count on a low-bar path and do not enable lanes by default. Preserve the correctness work and its benchmark switch, then measure repeated 16/64-flow and latency-under-load cohorts before revisiting the default. |
| 2026-08-20 | Intermittent iOS extension memory terminations have been observed around Auto/H3 operation and parallel packet floods, but deterministic Go ownership tests do not reproduce an unbounded heap or identify whether the retained memory is Go, native runtime, QUIC, socket, or kernel state. | Prefer H1 over H3 in production Auto while retaining explicit H3 and H3 fallback for diagnosis. Treat the physical-device campaign above, including OS footprint and termination evidence, as the release gate; synthetic heap tests remain guardrails rather than proof of mobile stability. |
| 2026-08-20 | Auto-to-explicit-H3 migration closed a budget-blocking H1 before the H3 replacement acquired its reservation, authenticated, or published a route. A failed or delayed H3 attempt therefore removed the usable carrier and made policy switching depend on a later reset. The broader transition audit also showed that retaining two full H3 working sets would defeat the same low-memory cap. | Transitions with H1 on either side now receive one serialized, accounting-visible overlap bounded to the H1 claim: H1 remains connected through H3 authentication, while H3-to-H1 gives the preferred H1 room to start. A budget-blocked H3-family-to-H3-family replacement releases the old full H3 claim instead of overcommitting another. The 25-edge lifecycle and saturated-budget matrices, plus a real WebSocket/QUIC auth barrier, pin activation and drain ordering. |
| 2026-08-20 | A production main-edge wire matrix reached every serving IPv4 target over direct H3, H3 DNS, and H3 DNS-pump; the excluded edge5 target remained unreachable. DNS modes consistently added roughly 170--220 ms because both envelope writers deliberately pace at 200 packets/s. On serving hosts, UDP buffer maxima are 1 MiB (defaults about 208 KiB), below quic-go's 7 MiB request, and the server's PPv2 `PacketConn` wrapper does not expose the optimized UDP interfaces quic-go uses for GSO, ECN, DF, and DPLPMTUD. | Treat current poor DNS/WhoDis latency as an implementation/tuning issue rather than a fleet-wide listener outage. Measure and bound a higher envelope burst/window before changing pacing; preserve PPv2 source-state bounds while exposing an optimization-capable packet connection; raise and verify host/container UDP buffers before claiming streaming throughput. The NGINX LB's `reuseport`, PPv2, unlimited-request, and 30-second idle-session directives are correct for continuity but are not sufficient peak-throughput tuning. |
| 2026-08-21 | A two-device Android campaign completed Direct on both Wi-Fi and cellular, H1 -> Auto -> H3 -> Auto with underlays swapped, a live Auto underlay swap, and same-LAN P2P in both provider directions. Carrier counters proved H1, H3, and P2P use rather than relying on requested policy. One 64/20 final-Auto cellular fetch and one 32/20 initial-Auto cellular fetch failed while the VPN and app remained alive; forced H1/H3, later Auto, fast.com, Direct, and both P2P directions completed. | Retain both failures as transient Auto/request-path evidence, not OOMs and not deterministic 32-MiB regressions. Keep the stable-Chrome readiness gate and preserve failed samples instead of silently retrying them. The powered, mostly strong-signal campaign closes functional routing on these devices, not the unplugged one-bar release gate. |
| 2026-08-21 | The previous Android 64/20 configuration reached 43.16/50.94 MiB Go runtime and remained at 39.97/48.51 MiB after 15 minutes. The iOS-budget 32/20 proxy still reached 48.94/37.56 MiB and remained at 42.47/34.21 MiB after 15 minutes. Active packet pools drained to one or two objects, while Auto retained 8--11 live exits and about 19--20 MiB live heap. | The 28-MiB streamline goal fails under both configurations. The evidence rejects an unbounded packet-pool leak and localizes the retained band to reclaimable Go/runtime plus multi-exit Auto working set. A Go soft limit is not an RSS ceiling and can be exceeded by live memory. Reduce and instrument Auto topology/state rather than treating the cap alone as a fix. |
| 2026-08-21 | Explicit pressure after 15 minutes reduced the 32/20 processes to 23.40/22.40 MiB and they remained at 26.16/24.80 MiB one minute later; 64/20 landed at 28.21/29.72 MiB and repopulated to about 29.55/30.55 MiB. Neither 32/20 process had an Android crash/low-memory exit, and 115/116 standard samples plus 6/6 fast.com sessions completed. | Cap Android's SDK process budget at the iOS 32-MiB value while preserving the 20-MiB device target. This improves pressure-response durability without claiming automatic recovery or iOS equivalence. Android GOGC 50 versus iOS 10, platform footprint, and actual iOS memory-pressure/jetsam behavior remain physical-device gates. |
| 2026-08-21 | Production-rate and 64-KiB heap profiles attributed the steady post-burst excess to returned message-pool high-water and allocator-span pinning, while the largest H3 sample was genuinely live: 35.92 MiB live heap, 11,122 outstanding pooled objects, and 51.70 MiB Go runtime. | Treat in-flight and returned ownership differently. Keep bounded pools for burst reuse, but after a verified quiet minute clear returned references, force release only for a material high-water, and rebuild the warm set without changing capacity. A hard 28-MiB active ceiling still fails and requires reducing H3 concurrency/flight, not more GC. |
| 2026-08-21 | Automatic clear/collect/rewarm dropped 10.22/7.22 MiB of returned buffers and restored the H3 processes to 24.39/23.19 MiB. After bidirectional P2P and disconnect they held 23.86/23.58 MiB for five minutes with about 1 MiB returned; each automatic trim count stayed one. | Retain a one-shot 60-second mobile quiet timer, a 4-KiB activity epoch, and a 1-MiB material-rebuild threshold. Preserve explicit pressure semantics, pool capacity, and all outstanding buffers. Do not force GC repeatedly for background trickle or trivial free-list refill. |
| 2026-08-21 | The allocation profile also found non-packet garbage: one-second status observation built full TLS defaults and reparsed pinned roots (about 9 MiB cumulative), while hot reliability reads projected another roughly 3.5 MiB. | Construct Auto eligibility from modes plus budget only, share immutable parsed roots while keeping per-config session caches isolated, and reuse each client's immutable reliability projection. Pools are for reusable mutable buffers; avoid or share immutable configuration instead. |
| 2026-08-21 | On the rebuilt physical artifact, matched disconnected allocation-byte rates fell 53.4% and 47.6%, GC cadence fell about 36%, and the tail ended at 21.82/21.74 MiB. Galaxy's allocations-per-second did not fall even though its allocated bytes did, showing the remaining objects are smaller. | Retain the allocation-avoidance changes and report byte churn, object churn, GC, and resident memory independently. Do not infer all-allocation improvement from fewer bytes or use pooling to hide avoidable immutable objects. |
| 2026-08-27 | Six Bloomberg media Fetch retries after an HTTP 403 all reused one established Chrome H2/TLS connection (`connectionId` 8895). A later top-level 403 used a new H2 connection (9547), but Chrome did not retry it. Connect received ordinary encrypted return traffic in both cases. | HTTP retry is not transport retry. No provider-selection policy can reroute requests multiplexed on an existing H2 connection, and the TLS-blind tunnel must not classify ciphertext as a 403. Anti-bias can act only when Chrome opens a fresh TCP/TLS tuple. |
| 2026-08-27 | Hard IP/domain affinity let a long-lived page connection bypass the provider race for every later connection in its group. Even a low-rate completed media sample could be hidden by a healthy live page donor. | Ordinary fresh-flow inheritance is now default-off. Exact established tuples remain fixed and explicit app/host pins remain strict; ordinary new flows reach the health/performance-weighted race. The legacy hard-affinity switch remains for A/B, while bounded IP/domain groups remain measurement keys. |
| 2026-08-27 | With hard DNS affinity disabled, retaining hour-long name/address-to-channel maps serves no production placement decision and can keep provider channel graphs reachable. | Do not populate DNS-exit hint maps while ordinary fresh-flow affinity is off. A runtime legacy enable learns from subsequent answers; explicit pins retain stable egress through their own affinity groups. |

## Results log

| Date | Revision / candidate | Test or artifact | Result |
| --- | --- | --- | --- |
| 2026-08-17 | Working tree, Phase 0 receive admission | `go test ./... -run 'Test(ReceiveSequenceReplacementDropsWithoutWaiting\|ReceiveSequenceClosingGenerationDropsWithoutWaiting\|ClientReceivePackHandoffDoesNotBlockUnrelatedSource\|ClientReceiveAckHandoffDoesNotBlockUnrelatedSource)$' -count=1` | Pass. Full Pack and ACK queues plus replacement and closing generations do not block unrelated receive progress. |
| 2026-08-17 | Working tree, Phase 0 receive admission | Same four tests with `go test -race . -run ... -count=1` | Pass under the race detector. |
| 2026-08-17 | Historical baseline | `server/connect/perfvar/MEASUREMENTS.md` | Evidence summarized above; current authoritative mobile campaign remains incomplete. |
| 2026-08-17 | Earlier working tree (historical failure) | `go test ./... -count=1` | Failed: `TestMultiClientUdp4` mixed lifecycle-policy ICMP teardown with an exact-delivery routing fixture, then the package timed out. The exact-delivery repair and final non-short suite row below resolve this result. |
| 2026-08-17 | Working tree, exact-delivery fixture repair | `go test . -run '^Test(Client\|MultiClient)(Udp4\|Tcp4\|Udp6\|Tcp6)$' -count=1` | Pass: all eight variants. The fixture opts out of flow lifecycle policy, uses fixed joined echo workers instead of one blocking-send goroutine per echo, and isolates routing assertions from admission saturation. |
| 2026-08-17 | Working tree, Phase 0 receive stats | Four receive-admission regressions plus `go test -race . -run 'Test(ClientReceivePackHandoffDoesNotBlockUnrelatedSource\|ClientReceiveAckHandoffDoesNotBlockUnrelatedSource)$' -count=1` | Pass. Public snapshots report exact Pack/byte and ACK handoff loss without receive-path locking. |
| 2026-08-17 | Server working tree, static cell-edge profiles | `go test ./connect/perfvar -run 'Test(CellEdgeProfilesResolveExactDeviceAccessConditions\|SimulatorProfilesValidateAndHash\|PerfvarCellEdgeScenarioDefaults\|PerfvarDefaultPayloadsUseLongBulkTransfers)$' -count=1` and the same selection with `-race` | Pass. Direction, rate, RTT, jitter, loss, queue, MTU, provider-access, payload, and UDP pacing defaults are pinned. No five-run performance result yet. |
| 2026-08-17 | Server working tree, full PERFVAR package | `go test ./connect/perfvar -count=1 -timeout=10m` | Environment-blocked: integration/correctness fixtures require `WARP_ENV` and vault `pg.yml`, neither available in this workspace. The pure profile/scenario selection above passes normally and under race. |
| 2026-08-17 | Working tree, adjacent callback fixtures | `go test -race . -run '^(TestPackLaneCodecLegacyAbsent\|TestSendReceiveParallelLanes\|TestSendMultiWithTimeoutDeliversOneBatchAndOneAck\|TestUpgradeMuxMultiClientIntegration\|TestUpgradeMuxDefaultDnsThroughTunnel)$' -count=1` | Pass. Lane delivery, batch receive, provider echo, client/TUN collection, ownership, and teardown remain correct with bounded zero-wait callback handoffs. |
| 2026-08-17 | Working tree, pool-balance callback worker | `go test -race . -run '^(TestMultiClientLifecyclePoolBalance\|TestRemoteUserNatClientRawSendPoolBalance)$' -count=1` | Pass. The provider reply leaves the shared callback before its blocking send and returns pooled bytes on admission failure, queue overflow, and cancellation. |
| 2026-08-17 | Working tree, `connectctl sink` | `go test ./connectctl -count=1` and focused race tests | Pass. A full printer queue drops immediately, counts loss atomically, and reports it outside the receive callback. |
| 2026-08-17 | Full working tree, short suite | `go test ./... -short -count=1 -timeout=10m` | Pass. Main Connect package: 212.178 s; all tested subpackages green. |
| 2026-08-17 | Full working tree, non-short suite | `go test ./... -count=1 -timeout=20m` | Pass. Main Connect package: 523.092 s; `blocker`, `connectctl`, and `extender` green; no package timeout. |
| 2026-08-17 | Working tree, encryption callback audit | Five contract/encryption tests and five contract-free/gate tests, each selected normally and with `-race` | Pass. Message-only callbacks count inline; content collectors use bounded zero-wait handoffs; fixtures use positive bounded Transfer capacity. The five contract/encryption tests complete in about 15–17 s. |
| 2026-08-17 | Working tree, control-sync callback audit | `go test . -run '^TestControlSync$' -count=1` and the same test with `-race` | Pass in 75.137 s normally and 78.095 s under race. The 4,000-message collector snapshots indexes into an exact bounded queue and never waits in the callback. |
| 2026-08-17 | Working tree, contention benchmark callback audit | `go test . -run '^$' -bench '^(BenchmarkMultiClientEgressParallel\|BenchmarkMultiClientBidirectional)$' -benchtime=100x -count=1` | Pass. Provider echoes use four bounded sender workers; overflow/cancellation returns pooled packets, callbacks are unsubscribed before drain, and benchmark multi-clients close. |
| 2026-08-17 | Full working tree after adjacent callback audit | `go test ./... -count=1 -timeout=20m` | Pass. Main Connect package: 561.737 s; all tested subpackages green. The long integration tail varies, while focused corrected groups remain fast and race-clean. |
| 2026-08-17 | Server working tree, dynamic cell-edge definitions and scope | `go test ./connect/perfvar -run 'Test(DynamicCellEdgeProfileSchedulesResolveExactEvents\|PerfvarDynamicProfileScenarioDefaultsAndBounds\|ProfileScheduleRunnerCompletionAndEarlyFinish\|ApplyFullTunProfileEventScopesDeviceAndP2pDirections\|FullTunEffectiveRateAndAggregateTimeout\|SimulatorProfilesValidateAndHash)$' -count=1` and the same selection with `-race` | Pass. Exact rate/outage/MTU timing, profile hashing, minimum payloads, device-only exchange scope, P2P directionality, and runner cancellation are pinned. |
| 2026-08-17 | Server working tree, route-neutral dynamic replay | `go test ./connect/perfvar -run '^TestMeasurePerfvarUnderlayReplaysLiveProfileSchedule$' -count=1` and the same selection with `-race` | Pass (4.080 s normal; 7.031 s race on the first isolated runs). One exact TCP stream remained active through both scheduled changes; both directional links recorded two updates and the result retained acknowledged event scope. |
| 2026-08-17 | Server working tree, receive-handoff carrier telemetry | `go test ./connect/perfvar -run 'Test(SubtractPerfvarClientReceiveRequiresStableGeneration\|ObservePerfvarCarrierIncludesReceiveHandoffIntervals\|PerfvarCarrierBaselinePassStableCoversEveryRouteCarrier\|PerfvarCarrierGenerationStableRejectsPostBaselineSubmission\|PerfvarCarrierGenerationStableIgnoresJoinedBridgeBatch)$' -count=1` and the same selection with `-race` | Pass. Device/provider/intermediary interval subtraction is exact only for a stable Client identity; generation changes are explicit, and receive-counter activity invalidates a crossing baseline pass. |
| 2026-08-17 | Working tree, production callback policy | `go test . -run 'Test(ProductionClientReceiveCallbacksAreAudited\|SharedClientReceivePumpHandoffsUseZeroTimeout\|PionIceCandidateCallbackSendDoesNotBlock\|ReceivePathSignalSendsDoNotBlock\|TransferReceiveCallbackDispatchIsInline\|TransferForwardCallbackDispatchIsInline\|TransferCancelDoesNotJoinInFlightReceiveCallback)$' -count=1` and the same selection with `-race` | Pass. Every production Connect subscriber is inventoried, direct blocking constructs are rejected structurally, Pack/ACK timeout zero is pinned, and Pion callback sends drop rather than wait. |
| 2026-08-17 | Working tree, callback ownership boundaries | Provider NAT/TCP, stream lifecycle, signal-shard, and P2P probe saturation selections plus SDK `TestDeviceLocalIoLoopEndToEnd` and migration callback tests, normally and with `-race` | Pass. Datagrams and shared callbacks do not wait; the dedicated provider TCP socket reader and final device-TUN write retain only their documented lossless synchronous scope. |
| 2026-08-17 | SDK working tree, RPC and subscriber policy | `go test . -run 'Test(DeviceRpcReceiveByteBudgetRefusesWithoutWaiting\|DeviceRpcMuxReceiveQueueSaturationClosesWithoutBlocking\|SdkClientReceiveRegistrationsAreAudited\|DeviceLocalProviderMigrateReceiveCallbackDoesNotWait)$' -count=1` and the same selection with `-race` | Pass. Full RPC receive budgets/queues terminate instead of parking the shared reader, and SDK Client subscribers require explicit audit. |
| 2026-08-17 | Full working trees after production callback-policy enforcement | Connect and SDK: `go test ./... -short -count=1 -timeout=10m` | Pass. Main Connect package: 209.638 s; all tested Connect subpackages green. SDK: 92.467 s. |
| 2026-08-17 | Pinned NGINX UDP PPv2 candidate | `NGINX_UDP_PROXY_V2_BINARY=/tmp/urnetwork-nginx-udp-v2-full/sbin/nginx go test ./connect -run 'Test(DefaultWarpPpTimeoutOutlivesNginxUdpSession\|DefaultConnectHandlerDnsListenerUsesInternalPort\|PpNginxUdpV2)$' -count=1` in `server` | Pass. Original IPv4 address/port, payload integrity, two-client separation, replies through the NGINX UDP pseudo-session, and bidirectional `H3Dns`/`H3DnsPump` envelope transforms are verified. |
| 2026-08-17 | Client public-port invariant | `go test . -run 'Test(PlatformQuicConfigEnablesPathMtuDiscovery\|PlatformDnsTransportUsesPublicPort)$' -count=1` in `connect` | Pass. DNS-encoded QUIC clients remain on public UDP/53 while the server default is independently pinned to UDP/8053. |
| 2026-08-17 | First-party load-balancer candidate | Local linux/amd64 `warp/lb` build from NGINX commit `11d11b5f0d3d8ace5215e1a77918e9dc219ce7db`, source archive SHA-256 `dbc96585a7ddc6f3c3a8faae9487ecdf5ad4e1e2eeb77a8b26e69d935434c9de`; all generated `main` configs checked with the image's `nginx -t` | Pass for all 13 configs. The current edge configs contain generated UDP/443 and UDP/8053 PPv2 servers. This is a local artifact, not a published or signed production image. |
| 2026-08-17 | UDP PPv2 and 53/8053 regression boundaries | The focused Connect, server, and warpctl selections above repeated with `go test -race` and the pinned NGINX binary | Pass. All three packages are race-clean at the changed boundaries. |
| 2026-08-18 | Warp-owned UDP/53 forward-port lifecycle | `go test ./services -run 'ForwardPort' -count=1`; focused warpctl forward/redirect/systemd tests repeated 20 times; the services and warpctl boundaries repeated three times under `go test -race`; production `main` services loaded through `warpctl ls services main` | Pass. Schema validation, deterministic unit propagation, exact IPv4 interface scoping, active LB-port resolution, IPv6/TCP exclusion, add-before-delete replacement, stale/direct-target cleanup, config-withdrawal cleanup, and cross-interface/unscoped rule isolation are pinned. |
| 2026-08-17 | Broader post-change suites | `go test ./... -short -count=1 -timeout=10m` in Connect and warp | Pass. Connect completed in 207.980 s; all tested warp packages completed successfully. |
| 2026-08-17 | Server broader short suite | `go test ./connect -short -count=1 -timeout=10m` | Environment-blocked after 602.234 s: `TestConnectAuto` repeatedly requires `WARP_ENV` and vault `pg.yml`, neither available in this workspace, then the package timeout fires. The focused changed paths pass normally and under `-race`; do not treat this run as a product-path pass. |
| 2026-08-17 | Load-balancer-owned NGINX PPv2 regressions | `NGINX_UDP_PROXY_V2_BINARY=/tmp/urnetwork-nginx-udp-v2-full/sbin/nginx go test . -run 'Test(DockerfilePinsNginxUdpProxyProtocolV2Support\|NginxUdpProxyProtocolV2EndToEnd)$' -count=1` in `warp/lb`, then the same selection with `-race` | Pass. Static source/module pins and black-box PPv2 source, 1400-byte payload, two-client, and reply behavior are verified; the ordinary package run skips only the capability-binary test when the environment variable is absent. |
| 2026-08-17 | Repository-local NGINX 1.31.4 dependency | `make nginx_local` in `warp/lb`, `zsh -n test.sh` in Connect, then the server and load-balancer UDP PPv2 black-box tests with `NGINX_UDP_PROXY_V2_BINARY=warp/lb/build/nginx-local/sbin/nginx`, normally and under `-race` | Pass. The native build identifies commit `11d11b5f0d3d8ace5215e1a77918e9dc219ce7db`, includes `--with-stream`, and subsequent `make nginx_local` calls are incremental. Both tests preserve the original client tuple, 1,400-byte payloads, two-client separation, and replies. `connect/test.sh` now builds and exports this exact dependency before starting Go tests. |
| 2026-08-17 | H3 DATAGRAM envelope v1 | `go test . -run 'Test(H3Datagram\|PlatformTransportH3Datagram)' -count=1` and the same selection with `-race` in Connect | Pass. Auth negotiation, mixed capability fallback, every fragment boundary, reverse-order delivery, duplicate retirement, overlap, corrupt checksum, invalid declarations, hard expiry, shared budget recovery, sender refusal, and live Pack/ACK routing are deterministic and race-clean. |
| 2026-08-17 | Existing legacy H3 lifecycle after capability offer | `go test . -run 'TestPlatformTransport(H3PacketConnFactoryOwnsConnectedEndpoint\|CloseInterruptsBlockedH3Write\|H3CloseDrainsQueuedReceiveOwnership)$' -count=1` and the same selection with `-race` | Pass. A server without RFC 9221 acceptance keeps the current stream on the same connection; blocked-write cancellation, endpoint ownership, queued receive draining, and auth-frame pool ownership remain intact. |
| 2026-08-17 | Server DATAGRAM configuration and metrics | `go test ./connect -run 'Test(ConnectQuicConfig\|ConnectQuicAuthFrame\|ConnectH3DatagramCollector)' -count=1` and the same selection with `-race` | Pass. The rollout switch controls the QUIC transport parameter, pooled auth frames remain balanced, and both bounded-label Prometheus families report exact complete-message, fragment, and envelope-byte totals. |
| 2026-08-17 | Database-backed server H3 integration | `go test ./connect -run '^TestConnectH3$' -count=1 -timeout=3m` | Environment-blocked after its built-in five retries: `WARP_ENV` and vault `pg.yml` are absent. This does not contradict the local new/new QUIC round trip or focused server tests; a configured integration environment is still required before claiming the real resident path. |
| 2026-08-17 | Full Connect working tree after DATAGRAM v1 | `go test ./... -short -count=1 -timeout=10m` | Pass. Main Connect package completed in 209.892 s; `blocker`, `connectctl`, `extender`, protocol, and security completed successfully. |
| 2026-08-17 | Server working tree, full-Transfer fragmented cell-edge A/B | `go test ./connect/perfvar -run '^TestH3TransferCarrierCellEdgeComparison$' -count=3 -v -timeout=4m` | Pass and exact payload delivery in all six runs. Legacy H3 completed in 2.164--2.254 s; DATAGRAM completed in 13.545--13.568 s. First message was 0.246--0.285 s in both modes. DATAGRAM reduced observed wire bytes but caused 26--27 Pack rewrites and 3--5 reassembly timeouts versus 7--11 legacy rewrites. The focused race run also passed. |
| 2026-08-17 | Server working tree, single-DATAGRAM-per-Pack isolation | `go test ./connect/perfvar -run '^TestH3TransferCarrierCellEdgeSingleDatagramComparison$' -count=1 -v -timeout=4m` | Pass and exact 43,008-byte delivery. Legacy H3 completed in 2.221 s and DATAGRAM in 13.957 s; both delivered the first message in about 0.248 s. DATAGRAM had zero fragment reassembly timeouts but 39 Pack rewrites versus 16, isolating Transfer loss recovery as the dominant tail. |
| 2026-08-17 | Working tree, guarded selective-ACK recovery | Focused `SendSequence` scoreboard, burst-bound, reordering threshold, tail/cumulative probe, conservative follow-up, minimum-RTT, and end-to-end gap-recovery tests | Pass normally. The implementation limits immediate gap recovery to four proven holes, requires three distinct later deliveries, never re-arms the same immediate recovery, and keeps tail probes dormant on reliable ordered carriers. The focused race selection must be repeated after the final flight-control refinement. |
| 2026-08-17 | Server working tree, aggressive recovery experiment | Database-free full-Transfer `cell-edge-1m-down-250k-up` A/B | DATAGRAM reached 2.604 s versus 2.186 s legacy, but required 28 selective-gap plus 32 tail-probe writes. Rejected: the completion gain came from an unacceptable 60-write retry train. |
| 2026-08-17 | Server working tree, production-boundary recovery candidate | Database-free full-Transfer A/B with carrier route capacity corrected from 128 to production's 32 frames | Three representative paired runs completed legacy/DATAGRAM in 3.413/5.786 s, 2.242/4.296 s, and 3.069/5.672 s. DATAGRAM issued about 25 gap writes and 11--15 bounded tail writes. Exact payload delivery passed, but DATAGRAM remains a negative release result. |
| 2026-08-18 | Connect `c3bc4472b34a` + working tree, TCP end-to-end ACK invariant | Focused ACK-policy, direct-collapse, provider-return first-drop recovery, sole-watchdog source anchor, H3 hybrid, MTU sizing, and live H3 round-trip selection, normally and under `-race` | Pass. TCP is ACK-required in both directions for direct and platform routes, an explicit lower-level NoAck hint is overridden at the final singleton/group boundary, a dropped direct provider-return Pack is retried, direct retransmits remain collapse-controlled, resize cannot bypass watchdog gates, and oversized control/data select stream. Later exact geometry pinned the one-DATAGRAM inner-MTU limits at 944/934 bytes. |
| 2026-08-18 | Connect `c3bc4472b34a`, server `af8117e380a1` + working trees, one-DATAGRAM hybrid before the duplicate-verdict correction | Cold `TestH3LowBarFullTcpPacketTrack`, seed `20260817`, `cell-edge-1m-down-250k-up`, 64 KiB upload | One run passed exact hash in 9.420 s (0.056 Mbit/s), with 280,271 wire bytes, 15 loss drops, 14 queue drops, 220 DATAGRAM sends / 193 receives, and eleven exact 1,515-byte contract-only stream writes. A repeat was reset at 34.747 s by the resize-side stall-verdict bypass despite zero receive-handoff or DATAGRAM integrity drops; this exposed the adjacent health bug rather than establishing a stable performance win. |
| 2026-08-18 | Same revisions, rejected two-DATAGRAM contract experiment | Cold full-TUN route readiness under the same profile and seed | Failed before measurement: readiness read timed out after 65.146 s. The one-DATAGRAM/stream hybrid was restored; the unfavorable result is retained as the fragment-vs-stream gate. |
| 2026-08-18 | Same revisions after making the watchdog the sole send-stall conviction owner | Cold `TestH3LowBarFullTcpPacketTrack`, same seed/profile/64 KiB upload | Pass with exact payload/hash and no client replacement. Setup was 0.469 s, transfer 26.565 s (0.020 Mbit/s), carrier 27.260 s, wire 371,232 bytes, 19 loss drops, 11 queue drops, 275 one-fragment DATAGRAM sends / 207 receives, and 30 exact 1,515-byte contract-only stream sends/receives. The watchdog held the unproven-uplink verdict and later accepted a liveness response instead of resetting the TCP flow. Correctness improved; throughput remains a release blocker. |
| 2026-08-18 | Compact contract / bounded unreliable recovery candidate before hybrid-liveness correction | Cold `TestH3LowBarFullTcpPacketTrack`, same seed/profile/64 KiB upload | Common completions improved to 8.44--8.62 s and about 266--283 KiB with zero stream messages, versus the approximately 26.17 s / 393 KiB / 33-stream-message original compact/full-TUN baseline. The same candidate also produced 43.06 s and 79.43 s completions plus a timeout, so the typical-case gain was not a tail win. |
| 2026-08-18 | Hybrid stream-deadline removal plus independent QUIC keepalive | `go test -race -run '^(TestPlatformQuicConfigEnablesPathMtuDiscovery\|TestPlatformTransportH3DatagramRoundTrip)$' .` in Connect and the server QUIC-config race test | Pass. The live round trip leaves the reliable lane empty for three former read-deadline periods, then successfully exchanges both hybrid lanes. Client and server pin a connection-level keepalive independent of DATAGRAM-writer backpressure. |
| 2026-08-18 | Same hybrid-liveness candidate, post-change full-TUN repetitions | Four cold attempts of `TestH3LowBarFullTcpPacketTrack`, seed `20260817`, `cell-edge-1m-down-250k-up`, 64 KiB upload | Exact completions were 6.880 s / 247,389 B, 83.934 s / 569,418 B, and 6.834 s / 254,532 B; all used one-DATAGRAM packet traffic with zero stream messages. A fourth attempt timed out during route readiness after 65.280 s. The fast path is about 74% faster and 35--37% fewer bytes than the original baseline, but the 83.934 s tail and setup failure keep the candidate behind the release gate. |
| 2026-08-18 | Opt-in QUIC packet/drop/key telemetry on the full-TUN gate | Cold runs completed in 19.283 s / 364,362 B, 87.226 s / 720,458 B, and 8.021 s / 256,193 B. Every classified server-side QUIC drop was a payload-decryption failure, not a duplicate, DOS-prevention, header, connection-id, or application receive-queue drop. The failures occurred only after the connection's first key update; the 87.226-second run recorded 112 such drops. | The long tail is below the H3 application lane and is correlated with QUIC key-phase handling. Keep the qlog reducer opt-in and interval-scoped so further experiments can distinguish connection startup, key update/discard, transport loss, and application admission without production overhead. |
| 2026-08-18 | Rejected previous-QUIC-key retention experiment | A dependency-only A/B raised `quic-go`'s previous receive-key retention from three PTOs to at least 10 seconds. An initial run completed in 8.132 s / 247,740 B, but three cold validations produced 64.904 s / 564,898 B, 8.622 s / 267,175 B, and a 61.459-second route-readiness timeout. The slow completion still had 20 post-update payload-decryption drops and 24 inner-TCP retransmits; the failed setup repeatedly churned H3 auth/connect generations and hit QUIC no-recent-network-activity timeouts. | Reverted the module-cache experiment. Longer old-key retention alone does not remove the tail. Healthy established DATAGRAM transfer is still materially better than the approximately 26.17 s / 393 KiB original baseline, but the complete startup-plus-transfer distribution is not yet a win and remains blocked from rollout. |
| 2026-08-18 | Key-update correlation rejection and packet-MTU localization | Delaying the first key update to 100,000 packets still produced 4--47 pre-update decrypt failures. Corrected short-header and source-TUN fingerprints matched every accepted packet and none of the rejected packets. A representative slow run took 62.067 s / 566,886 B with 23 unmatched decrypt failures. | The decrypt tail was not stale key material. The rejected sizes and fragmentation boundary localized it to oversized padded QUIC packets below the application DATAGRAM layer. The temporary dependency checksum instrumentation and key interval are reverted byte-for-byte to the v0.61.0 module archive. |
| 2026-08-18 | Safe QUIC startup plus corrected PERFVAR carrier MTU | Client and server now use a 1,200-byte initial QUIC packet. Before fixing the carrier-TUN boundary, a run timed out with 75 exact 1,200-byte decrypt failures; after the harness fix, decrypt failures fell to zero and a stream-fallback diagnostic completed in 6.787 s / 269,827 B. | Both changes are necessary: the product configuration avoids fragmentation at a real 1,280-byte path, and the harness now models that path instead of an unintended 1,200-byte physical interface. |
| 2026-08-18 | Bounded two-DATAGRAM application candidate | Focused normal/race selection, sizing, path-shrink, fragmented retry, and live round-trip tests pass. A diagnostic full-TUN run completed in 7.246 s / 236,950 B with 105 classified IP sends on DATAGRAM, zero stream sends, zero decrypt failures, zero inner-TCP retransmits, and two bounded fragment expiries. | Keep the candidate for stock-dependency repetitions. The rejected 1,515-byte contract-control fragmentation experiment remains rejected; this candidate fragments only application-bearing frames below the hybrid threshold. |
| 2026-08-18 | Stock `quic-go` v0.61.0 cold repetitions after the MTU/fragment corrections | Three separate-process `TestH3LowBarFullTcpPacketTrack` runs completed exact 64 KiB uploads in 6.203, 7.816, and 9.812 s using 242,258, 261,179, and 267,921 wire bytes. All had zero IP stream messages and zero decrypt failures. Route readiness was 50.251, 6.743, and 8.275 s. | Established transfer is consistently about 62--76% faster and 32--40% lower-byte than the approximately 26.17 s / 393 KiB historical baseline. The 50.251-second readiness tail means end-to-end startup is not yet a release win; investigate H3 route/auth readiness next. |
| 2026-08-18 | Bilateral physical-carrier MTU correction with stock `quic-go` v0.61.0 | Four cold, separate-process `TestH3LowBarFullTcpPacketTrack` runs completed exact 64 KiB uploads in 10.076, 7.676, 8.791, and 8.813 s using 324,850, 248,828, 261,862, and 263,879 wire bytes. Route readiness was 6.433, 6.874, 8.718, and 8.465 s. Every run used the DATAGRAM packet lane for all IP traffic, used zero IP stream messages, and recorded zero payload-decrypt failures. The focused `TestFullTunExchangeH3MtuCorrectness` gate also passes in the configured local environment. | Median established transfer is 8.802 s / 262,871 B and median readiness is 7.670 s. Against the approximately 26.17 s / 393 KiB original baseline, individual runs are 61.5--70.7% faster and use 19.3--38.2% fewer wire bytes. This is the first corrected set with both common-case and startup-tail improvement; treat it as provisional until the frozen campaign reproduces it over more cold runs. |
| 2026-08-18 | Frozen packet-track reproduction, same revisions and stock `quic-go` v0.61.0 | Five new cold, separate-process `TestH3LowBarFullTcpPacketTrack` runs completed exact 64 KiB uploads in 7.097, 8.319, 8.228, 7.545, and 7.367 s using 248,735, 254,585, 266,195, 273,208, and 267,051 wire bytes. Route readiness was 8.458, 8.804, 6.787, 7.620, and 8.847 s. Every run passed with zero payload-decrypt, send, malformed, checksum, reassembly-limit, and lane-decode failures; all IP traffic used DATAGRAM and none used stream. | Median transfer is 7.545 s / 266,195 B and median readiness is 8.458 s. Individual runs are 68.2--72.9% faster and use 32.1--38.2% fewer wire bytes than the approximately 26.17 s / 393 KiB original baseline. Across all nine fully corrected cold runs, transfer is 7.097--10.076 s with an 8.228 s median, readiness is 6.433--8.847 s with an 8.458 s median, and no prior 40--65 s tail recurs. Advance to the same-profile direct/H1/legacy-H3 comparison; do not infer broad mobile release readiness from this single workload/profile. |
| 2026-08-18 | Rejected global 900-MTU and warm-flight experiments | Five cold one-DATAGRAM MTU-900 runs had a 6.604 s / 249,638 B median versus the frozen MTU-1100 fragmented median, but current H1 at MTU 900 regressed about 17% in time and 16% in bytes, and one legacy-H3 stream run regressed to 5.071 s / 347,087 B. A 10 KiB cold flight did not improve the distribution. An unbounded warm release was fast but used 264,938--278,277 B with 12--20 queue drops; bounded 12 KiB warm growth still produced queue/retransmit pressure and was invalid for two-fragment messages. | Keep global MTU 1,100, cold flight 8 KiB, retry ceiling 2 s, and receiver-evidenced flight growth. The warm shortcut is removed. |
| 2026-08-18 | Refreshed fragmented H3 control after the TCP-ACK invariant | Five cold `TestH3LowBarFullTcpFragmentedPacketTrack`-equivalent runs completed in 8.689, 8.909, 7.741, 8.413, and 8.305 s using 269,353, 283,341, 261,339, 259,251, and 266,629 B. Every run delivered the exact 64 KiB hash with all classified IP traffic on DATAGRAM. | Median 8.413 s / 266,629 B. This is the frozen control for the same-worktree one-fragment decision. |
| 2026-08-18 | One-DATAGRAM/stream hybrid candidate, MTU 1,100 | Five cold runs completed in 6.793, 6.958, 6.117, 8.333, and 6.159 s using 243,633, 263,847, 240,300, 252,826, and 226,561 B. Every run delivered the exact hash, used both IP lanes, emitted exactly one fragment per DATAGRAM message, and had zero malformed/checksum/reassembly failures. | Median 6.793 s / 243,633 B: 19.3% faster and 8.6% fewer bytes than fragmented H3. Promote the one-fragment maximum to the production default. |
| 2026-08-18 | Same-profile reliable-stream controls | Five cold H1 runs had a 5.180 s / 307,814 B median. Five cold legacy-H3 stream runs had a 3.995 s / 339,130 B median. All ten delivered the exact hash. | The hybrid trades completion for wire efficiency versus both controls. H1 remains the faster production companion in Auto; broader validation is still required. |
| 2026-08-18 | Production-default one-fragment verification | Focused H3 envelope/geometry tests and all low-bar compile gates passed after changing `DefaultH3DatagramSettings().MaxFragmentCount` to one. One cold `TestH3LowBarFullTcpProductionHybridTrack` completed in 4.812 s / 218,728 B with 105 messages/105 fragments, both IP lanes active, and no carrier-integrity failure. | The promoted default exercises the measured hybrid rather than the fragmented control. Keep the explicit fragment-count override tests to prevent framing/reassembly coverage loss. |
| 2026-08-18 | Rejected route-wide-to-per-write recovery intermediates, Connect `c3bc447` + working tree | The first five cold lane-classified runs completed in 5.947, 5.135, 7.162, 5.824, and 5.924 s using 273,281, 299,907, 403,494, 247,608, and 348,817 B (median 5.924 s / 299,907 B). Pre-publication path-size discovery improved a second five-run median to 5.376 s / 289,042 B, but stream frames still inherited the DATAGRAM two-second recovery cadence. | Both candidates improved time but regressed bytes versus the earlier 6.793 s / 243,633 B hybrid. Reject them. A performance candidate must improve both axes and preserve TCP's Transfer ACK. |
| 2026-08-18 | Lane-accurate nested recovery, Connect `c3bc447` and server `af8117e3` + working trees | Five cold `TestH3LowBarFullTcpProductionHybridTrack` processes completed in 4.109, 3.305, 3.701, 4.060, and 4.144 s using 210,349, 204,135, 203,021, 212,482, and 208,684 B. Median was 4.060 s / 208,684 B; every run delivered the exact hash and emitted exactly 62 hybrid stream messages. Focused TCP-ACK, flight, H3 sizing, path-shrink, and live round-trip tests passed normally and under `-race`. | Retain. Against the pre-lane hybrid median this is 40.2% faster and 14.3% lower-byte; against fragmented H3 it is 51.8% faster and 21.7% lower-byte. The optimization leaves TCP `ack=true` and changes only which successful physical writes consume unreliable flight or use the nested-recovery delay. |
| 2026-08-18 | Exact hybrid-route retirement recovery and post-change cold verification, same revisions + working trees | Focused tests prove that withdrawing the exact accepting H3 route retries immediately through its replacement, while adding a sibling route produces no write for 350 ms; both pass normally and under `-race`. Five new cold production-hybrid runs completed in 5.000, 5.173, 3.662, 5.143, and 3.962 s using 210,175, 204,672, 209,654, 208,631, and 209,948 B. Median was 5.000 s / 209,654 B; all exact hashes passed, with stream send counts 62, 62, 63, 62, and 62. | Keep the failover correction and record the unfavorable timing shift. The current median remains 80.9% faster and 47.9% lower-byte than the approximately 26.17 s / 393 KiB original reference, and 40.6% faster / 21.4% lower-byte than fragmented H3. The single extra stream write and bimodal timing keep variance/recovery attribution open. |
| 2026-08-18 | Schema-6 deterministic one-second-outage attribution | Current H3 completed in 43.298 s / 6,097,889 B; current H1 completed in 46.462 s / 6,023,722 B. H3 provider recovery recorded 17 timeout writes and 137 flight waits totaling 18.212 s; H1 recorded 17 provider timeouts and 99 additional device timeout writes, with no unreliable flight. Both exact hashes and schedule events passed. | H3 is 6.8% faster in this trace and avoids the nested H1 timeout train, but uses 1.23% more bytes. Keep the result diagnostic until the frozen dynamic campaign has five traces per route. |
| 2026-08-18 | Rejected message-flight 12/4 and 12/8 candidates | The 12/4 static campaign completed in 3.800, 4.294, 3.933, 3.572, and 4.591 s using 205,437--212,021 B (3.933 s / 209,524 B median). Its two recorded outage runs completed in 45.118 s / 6,145,020 B and 44.887 s / 6,047,656 B; provider waits varied from 22 / 3.464 s to 117 / 19.213 s. The 12/8 follow-up immediately amplified one static run to 257,383 B, 74 stream writes, and 30 TCP retransmits. | Reverted to 8/4. The static median alone does not outweigh neutral-to-worse outage timing or a reproducible loss-floor amplification. |
| 2026-08-18 | Rejected ready-drain coalescing and +2 additive recovery | Eight-frame coalescing had a 3.345 s / 209,166 B H3 median, but a 6.151 s tail and one 29-retransmit run; H1 measured 4.968 s / 316,607 B median versus 5.180 s / 307,814 B control. Its outage trace was neutral at 43.262 s / 6,071,794 B with 161 provider waits. Four-frame runs used 225,844--243,973 B with 25--29 retransmits. A +2 message-recovery outage took 45.772 s / 6,108,202 B with 18 selective gaps, 168 waits / 20.909 s, and 225 queue drops. | All branches reverted. Focused ownership, lifecycle, decoder, provider-return, TCP-ACK, and race tests passed during the experiment; the performance gate, not correctness, rejected them. |
| 2026-08-18 | Current dynamic rate-collapse diagnostic | The cold H3/H1 pair completed in 49.373 / 49.678 s using 6,199,953 / 6,150,535 B. The warmed pair completed in 44.832 / 53.775 s using 6,063,883 / 6,805,198 B; H3/H1 forward queue drops were 201 / 364 and device timeout rewrites were 2 / 390. Every exact hash and scheduled event passed. | The warmed H3 trace is 16.6% faster and 10.9% lower-byte, while the cold trace is neutral. This is the first material current-tree H3 win under dynamic rate collapse, but one pair is diagnostic rather than a release baseline. |
| 2026-08-18 | Current dynamic live-MTU diagnostic | Cold H3/H1 completed in 38.136 / 48.555 s using 5,898,076 / 6,074,454 B; warmed H3/H1 completed in 37.404 / 49.385 s using 5,884,945 / 6,365,014 B. H3/H1 forward queue drops were 153 / 309 cold and 131 / 266 warmed. H3 had zero MTU drops in both traces; H1 had 10 / 16. Every exact hash and event passed. | H3 is 21.5--24.3% faster and 2.9--7.5% lower-byte in the first two pairs. The warmed H3 record misses only the conservative 10% calibration-separation rule because it runs within 5.5% of underlay; retain the raw diagnostic and require five paired traces before a release claim. |
| 2026-08-18 | Equal-priority Auto route selection on Connect `c3bc4472b34a` and server `af8117e380a1` plus working trees | Five fresh-process `tcp-warmed` uploads used seed `20260810`, `cell-edge-1m-down-250k-up`, one hop, mobile surrogate, and 256 KiB. Strict per-sequence affinity completed in 14.883--15.657 s using 850,432--881,204 B with 29--68 queue drops; median was 15.190 s / 864,267 B / 40 drops. Five forced-H1 controls had a 24.461 s / 1,861,026 B / 40-drop median; five forced-H3 controls had a 13.044 s / 797,068 B / 19-drop median. Every exact hash passed. | Retain strict destination-sequence affinity: it is 45.3% faster, 42.5% lower-byte, and has 74.7% fewer queue drops than the 27.780 s / 1,502,911 B / 158-drop frame-shuffling Auto reference. Do not spill on queue pressure. Do not use the rejected client-wide choice, which pegged all sequences to startup-order H1. The measured Auto upload chose H3 in all five processes, but clean correctness runs prove selection can differ by sequence and direction; this is not yet an adaptive best-carrier policy. Most calibrations missed only the conservative 10% underlay-separation rule, so the exact tunneled results are correctness-valid but not aggregate-valid. |
| 2026-08-18 | Permanent empty-transport-set recovery | The full Connect suite exposed a provider-flapping case where an unanswered cping ended without conviction, the empty route set held every silence verdict, and aged zero/zero stats looked healthy; the already-recorded `transportDownSince` epoch was never consumed, so five dead providers remained `Added`. Empty transport sets now retain the existing migration grace but retire structurally after `StatsWindowKeepUnhealthyDuration` (60 s by default; 2 s in the stress fixture). The deterministic expiry boundary passed 20 repetitions, and the formerly failing 45-second flapping test passed with `stuck=0`. | Retain the non-convicting single-cping behavior for lossy links and keep route restoration eligible throughout the grace. A route set that never returns must construct a fresh client rather than remain selected forever. Race, full-suite, and physical migration validation remain open. |
| 2026-08-18 | Cold H3/H1 focused control, Connect `c3bc4472b34a` + working tree | Five alternating fresh-process pairs, seed `20260817`, `cell-edge-1m-down-250k-up`, 1,100 MTU, 64-KiB upload | H3 median 4.242 s / 215,626 B versus H1 5.127 s / 319,611 B: H3 was 17.3% faster and 32.5% lower-byte. H3's 6.674-second maximum correlated with 72 stream messages rather than the usual 62; retain that tail for lane attribution. |
| 2026-08-18 | Canonical static matrix, server `af8117e380a1` state `fcbcc99d`, Connect `c3bc4472b34a` state `8bba31c9` | Local artifact `lowbar-auto-matrix.ipiedL.log`; five fresh-process runs per `p2p-fast,exchange-h1,exchange-h3,exchange-auto`, seed `20260810`, `cell-edge-1m-down-250k-up`, `tcp-warmed`, upload, one hop, mobile surrogate, 256 KiB | All Auto/H1/H3 payloads were exact. All-sample medians: Auto 14.983 s / 876,468 B, H3 15.268 s / 818,234 B, H1 30.285 s / 1,976,566 B. Calibration-invalid counts were 2/5, 3/5, and 1/5 respectively. Initial P2P delivered only 1/5, at 44.646 s / 561,768 B median; four runs recorded 1--7 small fast-receive queue drops. |
| 2026-08-18 | Rejected native-P2P count-only bounds | Local artifacts `lowbar-p2p-flight-v2.Ie0qSF.log`, `lowbar-p2p-flight-cap.KGWFRJ.log`, and `lowbar-p2p-flight-reserve.kPSp5m.log` | Correct unreliable-carrier classification cut fast sends to roughly 391--433 and improved completion, but the uncapped queue still lost one tiny message in 3/5 runs. A four-message cap took 26.66--27.30 s and still lost a 91-byte provider message. Reserving at a three-message cap removed that drop but regressed the first run to 35.348 s / 502,865 B. Both count-only caps were rejected. |
| 2026-08-18 | Retained native-P2P byte-bounded queue, server `af8117e380a1` state `fcbcc99d`, Connect `c3bc4472b34a` state `e81cca11` | Local artifact `lowbar-p2p-byte-queue.1coBYz.log`; five fresh-process `p2p-fast` runs under the canonical static scenario | All five exact hashes and calibrations passed. Durations were 23.806, 21.611, 22.921, 23.551, and 22.682 s; wire counts were 478,585, 475,321, 485,332, 488,146, and 466,703 B. Neither endpoint recorded a receive-queue drop. Against the initial P2P matrix, median time improved 48.7%, median bytes 14.8%, maximum time 55.5%, and correctness 1/5 to 5/5. |
| 2026-08-18 | Native-P2P queue ownership and receive policy | Queue bounds, final pool drain, route publication/readiness rematch, fast-worker join, adaptive read, prefetch, fast-only round trip, and flight-controller selections; focused lifecycle/policy selection repeated 20 times and the changed boundaries run under `-race` | Pass. `run` and `runFast` contain no channel send; each offers zero-wait into the bounded adapter. Its sole forwarding worker is joined, exact pending bytes return to zero, and both the blocked carrier read buffer and queued frame return to the pool before `Done`. |
| 2026-08-18 | Full Connect short gate after retained P2P correction, before exact worker-held count closeout | `go test ./... -short -count=1 -timeout=10m` | Pass. Main package 206.863 s; `blocker`, `connectctl`, and `extender` green. The first attempt correctly exposed an overbroad structural source check; after scoping it to the actual carrier-reader functions, the complete rerun passed. |
| 2026-08-18 | SDK DeviceLocal + DeviceRemote + RPC memory gate before exact worker-held count closeout | `go test . -run '^TestDeviceLocalSyntheticDeviceRemoteMemorySoak$' -count=1 -v -timeout=8m` in SDK | Pass in 62.58 s across 95 web/mail/blocked/provider cycles. Peak heap was 9.1 MiB, peak runtime memory 24.3 MiB, recovered heap 4.7--6.8 MiB, and teardown returned to 4.1 MiB / 12 goroutines / 13 file descriptors / zero pooled buffers outstanding. |
| 2026-08-18 | Non-short Connect package gate before exact worker-held count closeout | `go test . -count=1 -timeout=20m` | Pass in 459.346 s. The complete package order reproduced neither the repaired P2P final-drain hang nor the earlier provider-flapping failure. |
| 2026-08-18 | Exact native-P2P retained-message accounting | Focused count/byte/refusal/final-drain/policy selection repeated 20 times, broad P2P selection, and changed boundaries under `-race` | Pass. The hard 16-message total includes the off-channel item held by the forwarding worker; a seventeenth small message drops immediately, all message/byte reservations return to zero, and the broad P2P selection completes in 1.078 s. |
| 2026-08-18 | Final-source full Connect short gate | `go test ./... -short -count=1 -timeout=10m` | Pass. Main package 209.460 s; `blocker`, `connectctl`, and `extender` green. |
| 2026-08-18 | Final-source non-short Connect package gate | `go test . -count=1 -timeout=20m` | Pass in 468.173 s. No P2P lifecycle, receive-policy, provider-flapping, Transfer, or encryption regression. |
| 2026-08-18 | Final-source SDK DeviceLocal + DeviceRemote + RPC memory gate | `go test . -run '^TestDeviceLocalSyntheticDeviceRemoteMemorySoak$' -count=1 -v -timeout=8m` in SDK | Pass in 62.09 s across 94 web/mail/blocked/provider cycles. Peak heap was 9.3 MiB, peak runtime memory 24.1 MiB, recovered heap 4.7--7.0 MiB, and teardown returned to 4.2 MiB / 12 goroutines / 13 file descriptors / zero pooled buffers outstanding. |
| 2026-08-18 | Packet-aware flow scheduler and contract-safe NoAck admission | Focused scheduler, full-flight logical-group, contract-transition, and exact-debit tests normally and under `-race`; exact server H3 carrier and schema-9 observation tests | Pass. A full ACK resend window cannot block an eligible NoAck flow, contract rotation/exhaustion defers before serialization, committed bypass counters are exact, and H3 retains its bounded ACK reserve. |
| 2026-08-18 | Loaded-latency admission A/B, frozen binaries `507a17a631f3` and `3f4eecc3d393` | Five interleaved fresh-process runs per candidate, seed `20260818`, `cell-edge-1m-down-250k-up`, `latency-under-load`, upload, 256 KiB | Both delivered 5/5 exact payloads. Reserve-selected NoAck delivered 137/157 loaded probes with 13.669 s / 855,869 B medians; explicit NoAck with the reserve disabled delivered 145/164 with 14.764 s / 844,047 B medians. The latter improved probe delivery 1.15 points and wire/probe about 5.1%, but regressed median bulk time 8.0%; retain its semantics, not its disabled-reserve policy. |
| 2026-08-18 | Combined explicit-NoAck plus bounded-ACK-reserve campaign, frozen binary `2dec572e9a2a` | Five fresh-process schema-9 runs under the same loaded-latency scenario | Every exact payload passed. Median was 13.270 s / 832,234 B; 138/154 loaded probes arrived (89.6%), and median p50/p95 were 0.974/1.902 s. Provider recovery committed 116 NoAck bypasses, zero reserve selections/uses, 872 flight waits / 60.39 s, and 93 timeout rewrites. All five records missed only calibration headroom, so retain the raw comparative result without promoting it to a release threshold. |
| 2026-08-18 | Rejected hybrid single-message stream-yield candidate, frozen binary `4c1c366e99b2` | Two fresh-process schema-9 loaded-latency runs | Exact payloads passed, but completion regressed to 18.374/19.576 s, wire bytes to 901,528/907,662 B, and loaded p95 to 2.625/2.410 s. Removed. quic-go already prioritizes submitted DATAGRAM frames; sacrificing stream batching did not solve the FIFO before lane submission. |
| 2026-08-18 | Full short Connect gate after packet-aware NoAck admission | `go test ./... -short -count=1 -timeout=10m` | Pass. Main package 212.963 s; `blocker`, `connectctl`, and `extender` green. |
| 2026-08-18 | Retained bounded H3 split-lane dispatcher, frozen binary `4f5706eeaee1998cb939aeada4095c0ac5e6386d3b05a69b32e2700a589d08b9` | Five fresh-process schema-10 `latency-under-load` uploads, seed `20260818`, `cell-edge-1m-down-250k-up`, one hop, mobile surrogate, 256 KiB | Exact payloads passed 5/5. Against frozen pre-split v10, median completion improved 3.44%, p50/p95 20.45%/2.88%, aggregate probe delivery 4.80 points, success rate 3.55%, and queue drops 25.64%; median wire bytes rose 0.27%. The stream queue hit its explicit 32-message / 65,920-byte bound, returned to zero, and had no oversize admission. Retain the count-plus-capacity-byte bound; a faster count-only intermediate was rejected because retained backing was not bounded. |
| 2026-08-18 | Recovery-attempt versus physical-carrier accounting | Transfer now records `recovery_write_error_count`; the severe 64-kbit/s carrier fixture separately observes resend attempts, physical repeated writes, route-admitted frames drained at teardown, and failed first admissions | Pass with exact reconciliation. A recovery can be admitted to the bounded route and still remain unwritten when the useful workload completes, so carrier repeats alone are not the recovery invariant. On the restored policy, legacy stream completed in 10.092 s / 91,846 forward bytes and hybrid H3 in 8.399 s / 56,481 bytes: hybrid was 16.78% faster and 38.51% lower-byte. Local zero-wait receive drops remain counted and exact end-to-end payload delivery remains mandatory. |
| 2026-08-18 | Rejected H1/legacy reliable-stream recovery-delay generalization | A fixed eight-second delay first improved an isolated severe carrier, but the ten-pair production-shaped H1 workload raised total bytes 1.6%, drops 7.2%, and device/provider timeout rewrites 6.5%/16.0%; only 6/10 time pairs and 5/10 wire pairs won. A scaled four-to-eight-second variant then regressed five-pair H1 median time 38.4%, wire bytes 10.8%, and queue drops 90%. | Both candidates were removed. Only negotiated H3 hybrid-stream writes use the delayed nested recovery and exact accepting-route retirement retry. H1/legacy retains normal Transfer ACK/retry. Transport-up is never treated as proof of delivery. |
| 2026-08-18 | Terminal raw `SendPack` pool-publication ownership | The broad PERFVAR race tier found sequence cleanup reading `SendPack.admission` after `releaseRaw()` had already published the object to the Client reuse pool and another provider-return sender had begun reinitializing it. `releaseRaw()` is now explicitly terminal; every redundant post-publication admission release was removed, leaving enqueue rejection as the only separate pre-publication release path. | Focused raw lifecycle/cancellation tests passed 20 times under `-race`; the exact P2P-fast MTU reproducer passed in 58.481 s; the complete PERFVAR short race tier passed in 398.440 s with no `GORACE` report. The earlier `becfc1575333` performance cohort is retained only as variance evidence because it predates this repair. |
| 2026-08-18 | Authoritative repaired-source loaded-latency A/B, frozen binaries `2dec572e9a2a` and `fb9907a00f08` | Five alternating-order, fresh-process pre-split/final pairs under the same schema-11 one-bar scenario; artifacts `/tmp/lowbar-openloop-v20-paired-{baseline,final}-{1..5}.log` | All ten exact payloads passed. Final won all five completion pairs: median 13.531 to 12.593 s (**6.93% faster**). Loaded p50/p95 fell 1.065/2.147 to 0.572/1.453 s (**46.22%/32.33% lower**), probe delivery rose 87.26% to 95.07% (**+7.81 points**), successes/s rose **7.54%**, queue drops fell 168 to 101 (**39.88% fewer**), provider waits fell 22.14% / 30.65% by count/duration, and timeout rewrites fell 15.0%. Median wire bytes rose 836,233 to 862,065 (**3.09% higher**) and won only 1/5 pairs, so this is a latency/delivery win with a small byte-cost regression. Four final calibrations were headroom-invalid; this is the current raw paired candidate comparison, not a release-throughput or physical-radio claim. Physical one-bar iOS/Android validation remains open. |
| 2026-08-19 | Retained-fragment identity and bounded P2P/Client handoff unit gates | Fragment interleaving and receive/P2P bound selections repeated 20 times; changed boundaries repeated five times under `-race` | Pass. Distinct nonzero IPv4 identities survive parallel-route interleaving. P2P publishes 255-message / 240-KiB data flight limits, while both carrier and Client receive handoffs remain zero-wait and hard-capped at 256 KiB. |
| 2026-08-19 | P2P prime route-transition integration | Four direct and stream-P2P discovery/forced transition tests in one local-server invocation | Pass in 16.994 s. Each target probe was ACK-required and reached its exact terminal before exchange disable or route-ready publication. |
| 2026-08-19 | Current-source regional warmed TCP correctness | `TestPerfvarRegionalWarmedTCPThirtyTwoMiBCorrectness/exchange-h3/single-region-500ms-rtt/upload` and the corresponding P2P-fast 1,000 ms download | H3 passed its exact 32 MiB upload in 179.80 s, then repeated after the carrier-specific controller change in 161.32 s. After preserving 16 KiB of P2P carrier headroom and separating correctness timeout from performance reporting, P2P passed its exact 32 MiB download in 272.99 s without increasing the 256-KiB receive ceilings. |
| 2026-08-19 | Final current-source Connect package gate | `go test ./...` | Pass. Main package 482.170 s; `blocker`, `connectctl`, and `extender` green. This includes the carrier-specific byte policy, 256-message/256-KiB Client handoff, P2P queue bounds, fragment identity normalization, and adjacent recovery changes. |
| 2026-08-19 | Canonical current-source PERFVAR gate | `go test -p=1 ./connect/perfvar -parallel=1 -count=1 -timeout=0` in the documented local server environment | Pass in 1,522.654 s. This closes the synthetic single-host validation tier for the current worktree; it is not a physical-radio performance claim. Real iOS and Android capture remains the next follow-up. |
| 2026-08-19 | Android physical telemetry collector | Seven dependency-free parser/eligibility/privacy tests plus one strict sample on an attached physical Android handset | Tests pass. The sampler retained only allow-listed telemetry, forces replaced output files to private permissions, and correctly marked the device ineligible because the VPN was absent, the platform signal level was 4 rather than at most 1, and USB power was connected. No timing or throughput result was recorded. |
| 2026-08-19 | iOS physical benchmark telemetry and deterministic route selection | Focused iPhone 16 Pro simulator suite covering benchmark JSON, Direct/VPN readiness, H1/H3/Auto/DNS/DNS-pump configuration, suite behavior, and three packet-snapshot tests, including the real generated SDK per-transport list binding; generic and signed physical-device builds | Pass. The Debug driver now emits sanitized path, app-memory, power/thermal, active-transport, and aggregate/per-transport packet-delta evidence; it can non-destructively stop the tunnel for Direct or temporarily pin and then restore a VPN carrier without changing persisted app settings. The signed Debug app installed on the physical iPhone. The first control channel timed out; a later launch reached the phone but iOS denied it while locked, so no physical-radio timing or throughput result was recorded. |
| 2026-08-19 | iOS physical host acceptance gate | Twelve dependency-free Node tests, including an injected CoreDevice-process lifecycle, privacy-safe launch-failure classification, and replacement of permissive existing output files, plus the focused simulator suite with a Unicode multi-chunk round trip | Pass. Large results use bounded base64 console frames instead of one truncation-prone log message. Direct now snapshots the active provider, includes tunnel shutdown in readiness, and reconnects/verifies that provider after timing; failed preparation is transactional. The host retains raw CoreDevice output only in memory, emits allow-listed NDJSON with enforced private file permissions, requires explicit one-bar confirmation and eligible cellular/power/thermal state, proves forced-carrier or zero-remote Direct traffic, waits for policy/route restoration, and classifies locked/unreachable devices without retaining raw identities. The signed current build is installed; three real launches were correctly rejected before measurement while the phone was locked. This closes collection integrity, not the still-missing physical-radio evidence. |
| 2026-08-19 | iOS paired physical campaign acceptance | Eight dependency-free Node tests covering a valid five-cycle campaign, summary/route/page/content/path/chronology/drift/order rejection, bounded NDJSON parsing, privacy, and the real CLI output-permission path | Pass. The analyzer requires balanced Direct-bracketed H1/H3/Auto ordering, stable cellular path fingerprints and resource/origin shape, unique chronological captures, exact eligible route/carrier evidence, bounded Direct drift, and no packet-counter inconsistency. It emits only normalized per-mode timing/byte aggregates, wins/tails, carrier cost and mix, and sanitized cycle labels. No physical result exists yet because the installed phone remains locked and no eligible one-bar campaign has run. |
| 2026-08-19 | iOS automated physical campaign acquisition | Six dependency-free Node tests covering required signal attestation, all 5--100-cycle balanced schedules, private manifests, exact Direct/candidate sequencing, fail-fast retention, analyzer suppression on failure, and non-overwriting `0700`/`0600` storage | Pass. One command now runs a randomized position-balanced Direct-before/H1-H3-Auto/Direct-after campaign and invokes the paired analyzer only after all captures are eligible. It never persists device/provider identifiers and leaves a failed sanitized run for diagnosis. Signal-bar truth remains an operator attestation because public iOS APIs do not expose it; the actual campaign still requires an unlocked, unplugged phone on a confirmed one-bar cellular-only path. |
| 2026-08-19 | Negotiated logical Transfer lanes | Focused wire-codec, official-codec equivalence, negotiation/downgrade, base-lifetime, exact-ACK, bounded-budget, encryption reuse, loss isolation, legacy fallback, and reply tests; changed boundaries also run under `-race` | Pass. In the lossy end-to-end pair, lane 1 was held while lane 2 delivered and ACKed independently; lane 1 then recovered after release. Disabled clients allocate no logical-lane budget, nonzero lanes share fixed byte ceilings, and an old peer remains on lane 0. |
| 2026-08-19 | Initial logical-lane PERFVAR matrix, server schema 13 | One fresh four-flow `tcp-parallel` 256-KiB upload per 0/1/4/8 setting; `exchange-h3`, `cell-edge-1m-down-250k-up`, one hop, mobile surrogate, seed `20260810` | All four runs were exact and calibration-valid. Lane 0: 16.668 s / 670,546 B wire / 15,768,520 B allocated. Lane 1: 17.638 s / 424,735 B / 12,951,160 B. Lane 4: 18.367 s / 210,290 B / 17,269,944 B. Lane 8: 18.040 s / 427,946 B / 10,106,112 B. The nonzero candidates were respectively 5.8%, 10.2%, and 8.2% slower, so `LogicalDataLaneCount` remains zero by default despite lower wire bytes. |
| 2026-08-19 | Post-logical-lane package and harness gates | Connect `go test . -count=1 -timeout=20m`; `blocker`, `connectctl`, and `extender`; server schema/config/construction selection normally and under `-race` | Pass. The full Connect package completed in 523.856 s; all three subpackages passed. The final added callback-key, encryption-session, contract-queue, reply, and lane-isolation selection passed normally and under the race detector. Both worktrees pass `git diff --check`. |
| 2026-08-20 | Exhaustive transport-policy transition and Auto election gates | Connect and SDK 25-edge `H1,H3,H3Dns,H3DnsPump,Auto` lifecycle matrices; saturated-budget 25-edge matrix; real WebSocket-H1-to-QUIC-H3 authentication barrier; all 16 Auto availability masks from every prior election state; live promotion/fallback sequence; focused selections repeated 10 times and affected selections under `-race` | Pass. Every destination is installed and becomes active. H1-involved changes remain make-before-break under one serialized H1-sized budget overlap; constrained H3-family changes release the source claim first while retaining the destination's reconnect loop. Strict Auto election is `H1 > H3 > H3Dns > H3DnsPump`, including recovery after complete unavailability. The repeated Connect selection passed in 14.083 s, SDK in 1.126 s, and the race selections in 5.911 s and 1.848 s. |
| 2026-08-20 | Broad transition-change validation | Connect and SDK `go test ./... -short -count=1 -timeout=10m`; both repositories `go vet ./...`; SDK `go test . -count=1 -timeout=20m`; Connect `go test . -count=1 -timeout=20m`; isolated rerun of `TestWebRtcRepeatedConnectCloseReleasesAdmissionWithoutStall` | Both short all-package suites pass (Connect main 214.007 s; SDK 99.246 s), both vet runs pass, and the full SDK package passes in 555.447 s. The full Connect package reached 505.590 s with every transition test passing but failed one unrelated load-sensitive WebRTC repeated-connect cycle after its in-memory signal destination disappeared; that exact test passed alone in 0.788 s. Retain the failure as flake evidence rather than reporting the full Connect run as green. Both worktrees pass `git diff --check`. |
| 2026-08-21 | Android physical harness and collector | Current-source SDK AAR; Android Github Debug app/test builds; PhysicalLowbarSessionTest; physical_lowbar_capture parser/eligibility suite | App and instrumentation builds passed. Ten dependency-free collector tests passed. The long-lived harness authenticated once per process, accepted host commands for Direct/H1/H3/Auto/provider/P2P/pressure/finish, sampled Go and Android memory at 1 Hz, and retained aggregate packet counters without persisting credentials or device identifiers. Android 16/17 connectivity parsing now selects the sole app VPN when the shell default remains the physical underlay; Wi-Fi, no-VPN Direct, and stop-file capture gates are covered. |
| 2026-08-21 | Two physical Android devices, 64/20 baseline | 24 eligible Wi-Fi/cellular/Direct/H1/Auto/H3/P2P captures; 4,937--4,938 memory samples; Wikipedia, Cloudflare 1 MiB, and fast.com | 217 standard samples succeeded in 218 attempts and 3/3 fast.com sessions completed. Both Direct underlays and both P2P role directions passed. One final-Auto Galaxy cellular fetch failed after two successes; app/VPN survived and H1 worked. Peak Go runtime was 43.16/50.94 MiB and remained 39.97/48.51 MiB after 15 minutes, failing the 28-MiB target. No OOM, crash, route-invalid sample, or thermal event. |
| 2026-08-21 | Same devices, Android iOS-budget 32/20 candidate | 14 eligible captures; 2,445--2,447 memory samples; H1/H3/Auto transition, live underlay swap, both P2P directions, six fast.com runs, 1/5/15-minute recovery and explicit pressure | 115 standard samples succeeded in 116 attempts and 6/6 fast.com sessions completed. One initial Galaxy cellular Auto fetch failed; later forced H1/H3, post-H3 Auto, and swapped Wi-Fi all passed in the same process. Peak runtime was 48.94/37.56 MiB and normal 15-minute recovery was 42.47/34.21 MiB, so the soft cap does not meet 28 MiB. Explicit pressure reached 23.40/22.40 MiB and stayed below 28 MiB one minute later. Instrumentation exited cleanly and Android exit history contained no crash/low-memory record. |
| 2026-08-21 | Allocation attribution and idle-pool rebuild | Private 64-KiB and production-rate heap profiles; allocator/pool/GC telemetry; manual clear/collect/rewarm; automatic quiet recovery after explicit H3 and bidirectional P2P | The manual A/B reduced 34.53/35.05 MiB to 24.01/26.90 MiB. Production H3 peaked at 51.70/31.95 MiB, then one automatic material rebuild per device restored 24.39/23.19 MiB; a disconnected five-minute tail ended at 23.86/23.58 MiB without another forced collection. Profiles identify live packet work at the active peak, returned pools after the burst, and avoidable status/reliability configuration churn. |
| 2026-08-21 | Final memory implementation gates | Connect and SDK `go test ./... -short -count=1 -timeout=10m`; both `go vet ./...`; focused trim/idle/status/TLS/reliability selections five times under `-race`; Android Github Debug app/test assembly and unit tests; collector Node suite | Pass. Connect main completed in 199.890 s and SDK in 97.001 s; both vet runs were clean. The race selections preserve pool capacity and outstanding ownership, keep aggregate pool telemetry allocation-free, coalesce activity correctly, skip forced GC for trivial refill, avoid full TLS construction in status polling, share only immutable roots, and allocate zero objects for cached reliability reads. Android assembly/unit tests passed and all 10 collector tests passed. |
| 2026-08-21 | Rebuilt post-allocation-fix Android artifact | Pixel cellular explicit H3 at signal level 0 and Galaxy Wi-Fi explicit H3; 405 eligible route samples per device; Wikipedia, Cloudflare 1 MiB, fast.com, idle rebuild, and 370-second disconnected tail | After one retained fresh-install VPN-authorization timeout per device, unchanged code connected both TUNs in under nine seconds and completed all 18 measured browser actions. Sampler peaks were 31.42/30.78 MiB, so active/post-burst 28 MiB still fails. One automatic rebuild restored 23.39/22.95 MiB and the tail ended at 21.82/21.74 MiB. Allocation-byte rate fell 53.4%/47.6% and GC cadence about 36% versus the matched prior tail; no extra forced GC, trim, exit, crash, or low-memory process death occurred. |
| 2026-08-23 | 20-MiB source and regression gates | Full Connect and SDK package suites; final Connect/SDK focused race selections; SDK iOS-extension build and linked runtime-policy check; Android Github Debug AAR/app/instrumentation assembly, unit tests, and 10 collector tests | Pass. Connect's full suite completed in 444.586 s and its final short suite in 199.909 s; the final SDK full suite passed, including three measured provider-load repetitions at a 30.4--30.5-MiB host-process peak. New tests cover pool root/share/final-return ownership and zero allocation, bounded grouping, strict window admission, callback coalescing, mobile-only pressure admission and ownership returns, sampler/reclaim/physical-footprint transitions, nested-settings ownership, exact Android/iOS `GOGC=10`, desktop defaults, and linked `memprofilerate=0`. The final Android build and both test tiers passed. |
| 2026-08-23 | Exact iOS-paced Android physical surrogate | Fresh Wi-Fi Auto and cellular H1 processes on `zandroid`, 32-MiB Go soft limit, 20-MiB target, `GOGC=10`, `memprofilerate=0`; Wikipedia, Cloudflare 1 MiB, and fast.com | Wi-Fi active traffic was 21.08/21.13-MiB p50/p95 and five-minute steady recovery was 17.76/17.88 MiB. Cellular H1 active traffic was 19.69/19.89 MiB and post-reclaim steady recovery was 17.38/17.57 MiB. Neither process exceeded 28 MiB or terminated; one reclaim per process returned the pool to a small reuse floor. Temporary clients were released and credentials removed. Only one Android was attached, so fresh P2P could not replace the successful 2026-08-21 bidirectional evidence; physical iOS remains required. |
| 2026-08-23 | Server isolation after mobile pool/pressure work | Five serial 300-ms `-benchmem` repetitions of every benchmark in `server/connect`, its PERFVAR link primitives, and `server/proxy`, exact Connect parent versus candidate | Time geomeans changed -0.79%, +0.25%, and +0.01%; allocation geomeans and every individual proxy allocation result were unchanged. Server keeps the 1-MiB warm wrapper and has no mobile pressure/reclaim path. The canonical DB-backed PERFVAR attempt was retained as blocked after Redis `10.211.55.5:6379` returned `host is down`; its focused non-DB comparison passed three normal repetitions and once under `-race`. |
| 2026-08-24 | 24-MiB mobile performance rebalance on `zandroid` | Exact 20-MiB baseline plus three rejected 24-MiB candidates and final `m24-route-gc25-safe-20260824`; Wikipedia, Cloudflare 1 MiB, fast.com, explicit-H3 stress, and six-minute recovery | The accepted profile uses quality/speed 4/1, `GOGC=25`, and a 512-KiB warm set while retaining the 16-message, 16-packet/24-KiB, and 512-root H3 safety ceilings. Wikipedia median load fell 81.2% and Cloudflare median goodput rose 8.64x versus the exact 20-MiB run. The final 59-sample session peaked at 24.73 MiB with zero 28-MiB breaches; post-reclaim steady p50/p95 were 20.12/20.52 MiB. `GOGC=50` and both widened-queue candidates were rejected at 28.41--29.95 MiB under explicit H3. |
| 2026-08-24 | Exact server isolation for the 24-MiB SDK patch | Six order-balanced 300-ms `-benchmem` repetitions per side across every benchmark in `server/connect`, `server/connect/perfvar`, and `server/proxy`; SDK `49f756f` versus only the candidate patch | Time geomeans changed -0.05%, -0.17%, and +0.05%; no individual timing result was significant. Allocation geomeans were unchanged except +0.01% B/op in Connect, and alloc counts were identical. Linux retains `GOGC=100`, the 20-MiB default device budget, 1-MiB server warm wrapper, and no mobile gate/reclaimer. |
| 2026-08-24 | H1 sequence-depth and ACK-path isolation on one attached Android surrogate | Explicit H1/provider-off runs at global depth 32 and 64; receive-only 128/256 KiB; send+receive 128; H1 receive 64 with quick Transfer ACK, ACK-root reserve/suffix rescue, and zero/1-ms Pack handoff; Wikipedia, Cloudflare 1 MiB, fast.com, and Direct brackets | Depth 64 was the page-performance knee: global median Wikipedia load improved 543.7 to 457.6 ms versus depth 32 and stayed below 24 MiB, while depth 128 crossed about 25.05 MiB under repeated traffic and collapsed. Carrier attribution then observed a 64-message/~98-KiB Pack HWM and 2,280 Pack handoff drops but zero ACK-handoff drops. A 1-ms H1-only reliable-carrier wait reduced page/Cloudflare Pack loss from 82 to one; H3/unknown and ACK admission remain zero-wait. Send, ACK, forward, contract, and control counts remain 16; H1 receive is 64 with the same 128-KiB encoded-byte cap. |
| 2026-08-24 | Retained H1 64/1-ms physical result and 24-MiB memory gate | `h1-rx64-ackscan-wait1ms-20260824`; seven cache-disabled Wikipedia navigations, three Cloudflare 1 MiB transfers, fresh/hot canonical fast.com, 1-Hz memory/pressure/recovery telemetry | Wikipedia median load/request-to-first-byte/TTFB/request-p95 were 521.5/203.2/210.9/227.43 ms, with all seven warm loads at 483--537 ms. Cloudflare median was 1.58 Mbit/s; fast.com displayed 1.2 then 0.94 Mbit/s. Page/Cloudflare peak runtime was 18.63 MiB and the full session peak was 21.80 MiB; 19 samples had zero 28-MiB breaches. Only one Pack handoff dropped before fast.com, and two of three bounded waits succeeded. This fixes the internal H1 receive-collapse mode and page tails, but does not recover 40 Mbit/s. |
| 2026-08-24 | Rejected pure-TCP-ACK Transfer-NoAck diagnostic | `h1-rx64-acknoack-group-20260824`; same physical H1 client plus an H1-only NoAck arm for ACK-only inner TCP packets, followed by Direct Wi-Fi bracket | Cloudflare median remained 1.65 Mbit/s and Wikipedia median load/TTFB regressed to 1,032/311 ms even though timeout resends fell to 21 during that phase. Fresh canonical fast.com displayed 0.64 Mbit/s; a hot overlapping reload failed near 80 s. Hot runtime peaked at 27.79 MiB with 4,951 pressure drops. Direct Cloudflare then measured 38.87, 80.04, 92.28, and 90.01 Mbit/s (middle-pair median 85.03), proving underlay headroom. The NoAck arm was removed because it did not improve performance and breaks end-to-end commit across carrier replacement. Two query-cachebuster fast.com 404 attempts were harness-invalid and excluded. Temporary clients and credential material were removed. |
| 2026-08-24 | Contract-safe provider-return H1 grouping and ACK-amplification benchmark | Provider drains remain logical groups of 16 frames / 24 KiB; selected H1 sequences use 16 frames / 3 KiB per Pack while H3/mixed remain two frames / one MTU. Seven 500-ms `GOMAXPROCS=10` Transfer-boundary samples compare old singleton groups with one 16-packet full-MTU group. | Median 16-packet formation fell from 25,579 to 9,105 ns (2.81x throughput); wire Packs 16 to 8; allocations 147 to 51 (-65.3%); allocated bytes 19,440 to 4,024 (-79.3%). Tests cover no-contract and contract-bearing drains, 16/16/1 fairness chunks, partial admission, retry/lane identity, exact ownership/completion, route-generation pinning, the first group on a new H1 sequence, and H1/H3 bounds. Sparse singleton returns retain their raw path. This is local boundary evidence pending provider deployment, not a physical throughput claim. |
| 2026-08-24 | Current shared-code server performance and PERFVAR fixture status | Five 300-ms `-benchmem` repetitions of every benchmark in `server/connect`, `server/connect/perfvar`, and `server/proxy`; DB-backed H1 download campaign attempt; broad PERFVAR short attempt | All 185 benchmark samples passed: Connect 72.736 s, PERFVAR link primitives 4.303 s, proxy 9.073 s. Versus the preceding exact-candidate cohort, time geomeans changed -0.14%, +0.57%, and +0.92%; PERFVAR/proxy allocations were identical and Connect mean bytes/op changed about +0.01%. The full-TUN attempt remains infrastructure-blocked by the configured Redis endpoint reporting `host is down`; the broad short tier also requires absent local vault/DB resources. No server reclaim override is warranted, and no end-to-end throughput claim is made from benchmark-only success. |
| 2026-08-24 | Final H1 ACK/grouping source gates | Connect `go test ./... -short -count=1 -timeout=10m`; SDK equivalent; 18 Connect and 20 SDK affected tests three times under `-race`; Android local AAR plus Github Debug app/instrumentation and unit tests; physical collector parser suite | Pass. Connect's main package took about 200 s after replacing an obsolete all-carrier zero-timeout source assertion with the exact H1-1-ms/H3-zero/ACK-zero policy; SDK completed in 97.659 s. Both focused race selections passed. The provider grouping and permanent benchmark compile/run cleanly. The first Android AAR attempt hit a transient gomobile temporary-module error before app compilation; the unchanged rerun built the AAR, app, instrumentation, and unit tests in 59 s. All 10 privacy/eligibility collector tests passed. `git diff --check` is clean. |
| 2026-08-24 | Exact-byte ACK-root and small-pool physical isolation | `h1-ack-smallpool-20260824` on the attached Android surrogate: explicit H1/provider off, 256-byte pool class, 1-MiB base packet gate and 2-MiB ACK-only ceiling; adjacent diagnostic raised only the ACK ceiling to 3 MiB | The accepted ten-run Cloudflare distribution was 1.20--2.90 Mbit/s with a 1.695-Mbit/s median. Seven Wikipedia runs had 363.9/119.2/142.4/177.77-ms median load/request-to-first-byte/TTFB/request-p95. Runtime peaked at 22.19 MiB with zero >28-MiB samples. The 3-MiB arm removed ACK drops but lowered median speed to 0.92 Mbit/s and raised Pack drops 47 -> 79, so it was reverted. Exact byte accounting and the 2-MiB ACK ceiling remain overload protection, not a 40-Mbit/s solution. |
| 2026-08-24 | Provider pure-ACK and H1 ready-drain host performance | Seven two-second provider-download samples before/after direct established pure-ACK application; adjacent client H1/TLS ready-drain samples at 16/32 messages; full current `server/connect`, `server/connect/perfvar`, and `server/proxy` benchmark tiers | Direct provider ACK application changed median local download work 56,712 -> 54,935 ns (-3.1%), 577.8 -> 596.5 MB/s, about 5,351 -> 2,560 B/op, and 46 -> 37 allocs/op; the helper itself is zero-allocation. Client ACK-sized H1/TLS work fell 533.9 -> 445.0 ns (-16.7%), about 240 -> 288 MB/s, and 0.0693 -> 0.0443 writes/frame, while full-payload and sparse shapes were neutral and storage stayed fixed at 16 KiB. All 175/10/20 current server benchmark samples passed. Broad cross-process time geomeans versus the prior cohort were +1.65%/+2.13%/-3.79% with conflicting directions; exact affected benchmarks show no regression. |
| 2026-08-24 | Physical H1 speed after 32-ready candidate, retained as memory failure | `h1-ack-direct-batch32-20260824`; one authenticated Wi-Fi H1 session, stable production Chrome, seven Wikipedia pages, ten streamed Cloudflare 1-MiB objects, canonical fast.com, 1-Hz route telemetry, and 15-s Go sampler | Cloudflare completed 10/10 at 1.22--3.13 Mbit/s with a 2.02-Mbit/s median, +19.2% versus the adjacent 1.695-Mbit/s exact-byte run. fast.com settled at 5.7 Mbit/s after 45 s and SDK counters recorded 28.74 MiB H1 ingress. Wikipedia regressed to 787.5/343.0/349.2/388.84-ms median load/request-to-first-byte/TTFB/request-p95. Sustained traffic produced 2,137 / 3.85 MiB outstanding pool, then 6.48 MiB returned pool and a 29.48-MiB Go-runtime crest: three samples exceeded 28 MiB. One automatic quiet rebuild dropped 5.98 MiB and reduced runtime 29.48 -> 19.85 MiB; later values held at 20.15--20.48 MiB. This is reclaimable burst/allocator-span high-water rather than a persistent leak, but it fails the active/post-burst memory gate and is not an accepted release profile. The temporary client was released and credential material removed. |
| 2026-08-24 | ACK-direct/32-ready final source and build gates | Focused ownership, pre-handshake fallback, wakeup, zero-allocation, batching, and provider-dispatch tests ten times; affected race selection three times; complete Connect/SDK short suites; Android AAR/app/instrumentation/unit build; all current server benchmark packages | Pass. Connect/SDK short main packages completed in 198.841/99.052 s. The Android Github Debug AAR, app, instrumentation, and unit tier completed in 68 s. Server benchmark logs contain 175/10/20 passing samples. The direct ACK path retains ordered fallback before SYN establishment and exact packet ownership, while 32-ready batching remains ready-only with the 12-KiB byte stop and existing 16-KiB wrapper. The DB/vault-backed full-TUN server fixture is still unavailable, so provider deployment and sustained physical memory remain open gates. |
| 2026-08-25 | Adjacent bulk-read and write-coalescing audit | H1 mobile writer, server H1 writer, Android TUN ingress, NAT/provider return, TCP callback/writev, gVisor TUN batches/GRO, remote multi-client delivery, exchange writes, and Linux `sendmmsg` | H1 now performs repeated ready-only cycles of up to eight priority Transfer ACKs followed by ordinary packets in the same nonblocking flush; the ordinary drain remains 32 messages / 12 KiB inside the fixed 16-KiB wrapper. Server H1 uses the same 32-message ready cap with its existing 16-KiB socket bound. Android already performs one blocking TUN read followed by up to 63 nonblocking reads and one `sendPacketsNoCopy`; NAT, TCP, gVisor, provider return, remote multi-client, exchange, and Linux socket boundaries were already bulk. WebSocket/SCTP logical-message reads and TUN packet writes remain singular because their boundaries cannot be concatenated safely. H3 and DNS are explicitly deferred. |
| 2026-08-25 | Receive-allocation failure attribution and rejected arms | `h1-coalesce-pack2m-20260825`, `h1-rxbudget2m-wait5-20260825`, and `h1-rxalloc2m-wait10-20260825`; exact pooled-root, decoded-owner, Pack/receive budget telemetry | A Pack-only budget still allowed 6.51 MiB of receive roots and a 30.39-MiB runtime peak. A payload-byte receive budget still hid pooled backing/owner costs: roots reached 6.15 MiB and runtime 29.59 MiB. Charging exact retained allocation reduced the peak to 22.83 MiB, but initially applying that same charge to the per-flow logical window stalled the second Cloudflare sample. The retained design therefore keeps protocol payload flow control independent from one exact shared retained-allocation budget. Tests cover every backing class, owner/root charge, duplicate/remove/clear release, many-flow zero-floor bounds, shared resend accounting, and normal/race repetition. |
| 2026-08-25 | Final Android H1 24-MiB acceptance | `h1-rxalloc-separate-wait10-20260825`; explicit H1/provider off on Wi-Fi, stable Chrome, 7 Wikipedia + 10 full 1-MiB Cloudflare + fast.com + 7 hot Wikipedia + five-minute quiet + 7 post-recovery Wikipedia; 61 Go samples and 923 privacy-filtered host samples | All ten 1-MiB objects completed at 2.04--6.00 Mbit/s (2.78 median). Pre/hot/post-recovery Wikipedia medians were 455.1/627.4/613.6 ms load and 169.6/248.8/227.8 ms TTFB; one hot reused-H2 resource waited 5.3 s without a concurrent tunnel drop/retry, while all seven post-recovery pages had no multi-second resource tail. fast.com moved at least 20.53 MiB ingress in the inner counter bracket. Runtime peaked at 21.77 MiB with 8.91 MiB live heap, 1.78 MiB packet roots, exact receive use 2.00/2.00 MiB, and zero samples above 24 or 28 MiB. Five-minute steady p50/p95/range/last were 19.91/20.16/19.85--20.20/19.91 MiB with zero queued receive bytes, a 256-KiB packet warm set, zero forced GC, and zero trim. Nine of 11 bounded Pack waits succeeded; two misses returned 2,880 bytes and did not prevent payload completion. The adjacent Direct upper pair was 41.53 Mbit/s, so the remaining public-H1 gap requires deploying the provider grouping/direct-ACK work to a controlled exit; Android does not replace physical iOS `phys_footprint`/jetsam validation. The retained client was released and all credential/device artifacts were removed. |
| 2026-08-25 | Final source, race, device, and server performance gates | Connect/SDK full short suites; exact shared-lane accounting 50 times and 10 times under race; retained receive/message-pool tests 20 times and five under race; SDK mobile policy 20 times and five under race; server H1 tests 20 times; Android parser/unit/build/instrumentation; Go vet and diff checks; full benchmark-only server tiers and order-balanced detached baseline A/B | Pass. Connect/SDK full short suites completed in 202.273/97.954 s. All focused normal/race repetitions passed, including the regression that caught and fixed encoded resend-frame under-accounting through an embedded queue item. Android unit/parser/build passed and the 912.262-s physical instrumentation test completed normally. Connect, SDK, server/connect, PERFVAR, and proxy vet cleanly; all four worktrees pass `git diff --check`. Server benchmark tiers passed 190/10/20 samples. The 8 -> 32 server H1 sweep improved full-payload/ACK-sized TLS throughput 12.9%/85.2% with unchanged allocations; same-session baseline/current PERFVAR and proxy changed -0.46%/+0.64% with identical B/op and allocs/op. The broad server short tier remains externally blocked waiting on the documented Redis/vault/DB fixtures; focused tests and all benchmark-only tiers pass. |
| 2026-08-25 | Iterative H1 receive-depth physical isolation on the attached Android surrogate | Two explicit-H1/provider-off arms: count-only 64 -> 128 by 16 after two full observations within 100 ms, then paired count/bytes 64/128 KiB -> 128/256 KiB by 16/32-KiB; Direct 4-MiB brackets, Wikipedia, ten Cloudflare 1-MiB attempts, canonical fast.com, five-plus-minute recovery, and schema-10 saturation/growth telemetry | Count-only growth stayed memory-safe at a 21.39-MiB peak but stopped at 92 Packs / 130,978 of 131,072 bytes and completed only 7/10 Cloudflare transfers at a 1.54-Mbit/s median. The paired arm reached the full earned 128/256-KiB limit and actually queued 128 Packs / 194,688 bytes; all ten Cloudflare objects completed but median goodput was 1.18 Mbit/s. fast.com moved 1.77 MiB in 75 seconds (about 0.20 Mbit/s), requested no additional depth, and session timeout resends reached 620, while the Direct median was 87.4 Mbit/s. Paired runtime peaked at 22.45 MiB; 390-second recovery p50/p95/range/last were 20.58/20.91/20.19--20.97/20.19 MiB with zero samples above 24/28 MiB. Post-recovery Wikipedia was 439.6-ms load / 186.2-ms TTFB median. Reject adaptive depth as the production mobile default: it is memory-safe in this arm but does not unlock the public-provider path. Keep fixed H1 64/128 KiB, retain generic opt-in telemetry for a controlled provider, and deploy provider grouping/direct ACK before another client budget spend. The retained client and on-device credential artifacts were removed. |
| 2026-08-25 | Iterative-depth final source and performance gates | New count/byte saturation, expiry, H3 exclusion, exact-budget, telemetry aggregation, SDK policy/schema, and owner-size tests repeated ten times normally and under race; full Connect/SDK short suites; Connect/SDK/server vet; Android final SDK AAR, Github Debug app/test APKs, unit tests, and ten collector/parser tests; all server benchmark-only tiers | Pass. Connect/SDK full suites completed in 203.712/98.092 s. The opt-in fixed/adaptive Pack microbenchmark measured 58.22/58.20-ns medians with zero allocations. Server adaptive settings and retained Pack scanning remain default-off; all 190/10/20 `server/connect`, PERFVAR, and proxy samples passed. Production full-payload/ACK-sized H1 TLS medians were 1,032/419.1 ns with 17/10 B/op and two allocations; PERFVAR receive credits were 783.1 ns and proxy batch-64 was 6,426 ns. The production mobile SDK explicitly clears adaptive settings and remains fixed at H1 64/128 KiB. |
| 2026-08-25 | Bounded H1 logical-lane and Transfer-ACK overflow isolation | Controlled four-flow PERFVAR lane-zero/eight-lane comparison at similar calibrated underlay; clean 50-ms ACK-coalescer A/B; adjacent physical explicit-H1/provider-off lane-eight and rebuilt lane-zero arms with Wikipedia, canonical fast.com, Direct brackets, SDK counters, and Go memory | Controlled lane eight reached 29.058 Mbit/s versus lane zero 20.247 Mbit/s at 43.704/41.474-Mbit/s underlays (+43.5% raw). Lossless ACK-window folding changed clean H1 134.551 -> 134.529 Mbit/s (-0.016%). On device, lane eight improved Wikipedia median load/TTFB from lane zero's 1,157.8/367.9 ms to 348.8/126.1 ms and reduced exact-burst timeout resends 1,053 -> 152; runtime peaks were 19.43/20.60 MiB with zero >28-MiB samples. Public fast.com displayed 3.6--10 Mbit/s on lane eight and 4.4 Mbit/s on lane zero while Direct displayed 410 Mbit/s and 1.1 Gbit/s. The provider return sender remained on lane zero, so client lanes isolated requests/inner TCP ACKs but could not split download data. Keep eight shared-budget lanes and lossless ACK overflow as an explicit-H1 symmetric provider candidate; do not claim 40 Mbit/s or enable default Auto before the pinned provider A/B. |
| 2026-08-25 | Symmetric-H1 candidate final gates | ACK overflow and logical-lane selections 20 times normally / 10 and three times under race; complete Connect/SDK short suites; all affected vet tiers; clean-host affected server benchmarks; Android SDK AAR, Github Debug app/test APKs and unit tests; physical collector and fast.com harness syntax/privacy checks | Pass for every self-contained gate. Exact-source Connect/SDK complete suites finished in 199.341/97.135 s. An earlier Connect rerun under an accidentally retained benchmark probe missed 2/240 callbacks in the unrelated six-minute `TestTransferBudgetLiveness`; after stopping that 15.5-hour campaign process, the exact test passed 20/20 normally and 5/5 under race, followed by the complete green rerun. Five clean-host repetitions measured production server H1 TLS at 891.7 ns full-payload and 370.3 ns ACK-sized with two allocations/op; PERFVAR receive credits were 673.6 ns and proxy batch-64 was 5,406 ns. Android built successfully in 32 s; all ten collector tests and the fast.com script syntax gate passed. The broad server correctness attempt reproduced the documented external fixture block (`WARP_ENV` unset and vault `pg.yml` absent, including `TestProxyWgHandoffPollExpiry` after five retries); it is not reported as green. The production change is deliberately limited to mobile <=24-MiB explicit H1 on both client and provider; Auto/H3 are unchanged pending the pinned provider A/B. |
| 2026-08-25 | Deterministic reliable-H1 synthetic-loss isolation | Pinned controlled provider over explicit H1/eight lanes; fixed 64/128-KiB receive depth and unchanged exact 2-MiB Pack/reorder budgets; schema-11 carrier-drop/backpressure and Pack/recovery counters; three canonical fast.com runs, seven Wikipedia pages, and a 345-second quiet connected window | The fixed-depth control displayed 6.1 Mbit/s while the 32-message platform route discarded 530 complete messages / 1,186,363 bytes, the receive-reorder budget pinned at 1.993/2.000 MiB, and provider recovery produced 1,357 timeout plus 348 selective writes. Making only the carrier route lossless moved the failure to the finite Pack boundary: carrier drops became zero, Pack drops rose to 24, reorder pinned at 1.994 MiB, and fast.com displayed 3.5 Mbit/s. Making both reliable H1 handoffs wait for capacity or cancellation—without adding a slot or byte—produced 38, 41, and 52 Mbit/s. Across the accepted session, carrier/Pack drop deltas were zero, 762/762 Pack waits succeeded, final Pack/reorder use was zero, and selective provider recovery rose only during the first run. Wikipedia load/document-TTFB/request-p95 medians were 439.2/181.5/183.58 ms with 7/7 success. Runtime peaked at 17.60 MiB; quiet p50/p95/range/last were 17.57/17.61/17.23--17.61/17.41 MiB with no >24/28-MiB sample, zero queued ownership, 0.50-MiB maximum retained pools, and no forced GC/trim. Accept for mobile H1; H3/DNS and physical iOS footprint remain separate gates. |
| 2026-08-25 | Lossless-H1 root-cause and regression gates | Filled one-slot carrier and Pack queues; exact pooled-slice delivery/return, cancellation, H3-DNS nonblocking policy, and production-source mode audit; 50 normal and three race repetitions; complete Connect/SDK suites; Android AAR/app/test/unit build; shared server/connect, PERFVAR, and proxy benchmarks | All deterministic repetitions pass, including cancellation returning Pack ownership to its caller and the audit permitting exactly one cancellable H1 receive send while forbidding direct blocking H3/DNS/P2P reader handoffs. Complete Connect/SDK suites passed in 442.156/459.385 s; affected race selections, vet, and the Android 90-task build passed. All 190/10/20 server benchmark samples passed. Production full-payload/ACK-sized H1 TLS medians were 896.2/373.0 ns with unchanged 17/10 B/op and two allocations (about +0.5%/+0.7% versus the adjacent cohort); PERFVAR receive-credit improved 673.6 -> 636.8 ns and proxy batch-64 improved 5,406 -> 5,348 ns. The DB-backed H1 PERFVAR track was attempted with its documented environment and remains externally blocked by the down local Redis fixture; it is not reported green. The 29.7-minute instrumentation session finished normally, all four temporary Android clients were released, provider shutdown removed its retained client, and private device credentials/pins plus the temporary provider harness were removed. |
| 2026-08-25 | Complete exact-lane receive remediation | RouteManager-to-Pack receive reliability; H1, H3/H3Dns/H3DnsPump stream, SCTP, H3 DATAGRAM, native P2P, resident/exchange TCP, resident shared-callback overflow, and server H3 hybrid cutoff; filled one-slot queues, explicit cancellation barriers, pooled-owner witnesses, artificial sequence markers, gob round trips, source inventories, and hybrid queue-slot accounting | Supersedes the earlier H1-only policy above. Hybrid H3 and production P2P publish distinct immutable receive routes. Reliable stream/SCTP/framed-TCP lanes retain only their already-read frame to fixed capacity or cancellation; H3 DATAGRAM and native P2P remain bounded zero-wait. The final Pack handoff uses exact lane reliability rather than transport family. Internal server readers backpressure; a shared callback that cannot wait retires the generation instead of skipping a reliable frame. Server H3 serializes the exact contiguous DATAGRAM byte cutoff through the resident exchange. Splitting H3 lanes keeps the same total payload slots. The affected topology test now requires one SCTP and one native P2P receive route in each intermediary direction. |
| 2026-08-25 | Exact-lane fast.com regression bracket on the attached Pixel | Current `lane-remediation-20260825`, retained pre-change SDK AAR, then closing current `lane-candidate-close-20260825`; fresh authenticated app and Chrome per arm; explicit H1/provider off; stable DevTools; three canonical runs per arm; SDK carrier/memory counters | Opening current displayed 8.7/6.9/7.3 Mbit/s (7.3 median), the retained AAR 84/95/1.2 (84 median), and closing current 160/130/140 (140 median). Exact H1 ingress deltas were 43.80/223.70/504.00 MB, ruling out Direct leakage. Go-runtime peaks were 18.73/19.25/20.21 MiB. Closing current recorded 5,769 backpressures / 8,483,576 bytes, zero route drops, 1,804/1,804 successful Pack waits, zero Pack drops, and zero final Pack/reorder use. All three closing samples exceeded the 40-Mbit/s target and the median exceeded the bracketed baseline, so no systematic H1 fast.com regression is present. Preserve the slow opening arm and baseline outlier: public-provider selection was not pinned, so this is a target/regression gate, not a 66.7% speedup claim. All three temporary clients were released and private credentials/device artifacts removed. |
| 2026-08-25 | Post-pull fast.com route-limited bracket | Final rebased current, retained pre-change AAR, final rebased current; same attached Pixel/Wi-Fi/Chrome/canonical harness; fresh authenticated app and Chrome per arm; explicit H1/provider off; exact counters and memory | Current primary displayed 0.58/27/52 Mbit/s (27 median), followed by preserved extension 17/8.1/13; retained AAR displayed 28/15/10 (15 median); closing current displayed 0.65/17/10 (10 median). Exact H1 deltas were 302.43 MB over six / 147.21 MB over three / 64.37 MB over three. Runtime peaks were 19.66/17.70/19.89 MiB; carrier and Pack drops were zero in every arm, and current completed 165/165 opening plus 38/38 closing Pack waits. Timeout resends were 14,057/330/6,926, exposing sharply changing provider conditions. Baseline was also below 40 and current produced the only above-40 sample, so this degraded public route is neutral regression evidence, not a percentage comparison or replacement for the earlier 140-Mbit/s closing target gate. The final APK was restored, all clients released, and private artifacts removed. |
| 2026-08-25 | Exact-lane final source, memory, and server-performance gates | Complete Connect/SDK short suites; focused Connect/server/SDK normal and race repetitions; intermediary P2P topology 20 times plus three race runs; vet; Android final AAR, Github Debug app/test APKs and unit tests; collector/parser and fast harness checks; five-repeat 300-ms benchmem sweeps | Connect/SDK complete suites passed in 199.474/98.001 s; every focused normal/race gate and all affected vet tiers passed. The Android 90-task build passed in 1m12s and all ten privacy/eligibility collector tests passed. Before the final generated-data pull, all 210 server/connect, 10 PERFVAR, and 20 proxy samples passed. The rebased source then passed all 30 exact affected H1/admission samples: production full-payload/ACK-sized H1 TLS medians were 884.2/366.7 ns with unchanged 17/10 B/op and two allocations, -1.34%/-1.69% versus the adjacent cohort. Pre-rebase PERFVAR receive-credit was 673.4 ns (+5.75%) and unchanged proxy batch-64 5,444 ns (+1.80%); these unaffected cross-package shifts are host noise. Post-rebase reliable/unreliable queue fast paths measured 31.17/32.67 ns and full ResidentTransport wrappers 40.47/38.06 ns, all zero B/op and zero allocs/op. No server or device-throughput regression is detected. |
| 2026-08-26 | Cross-project research, deterministic no-retransmit guard, and two-device provider/client matrix | Final schema-12 Android artifact on both attached phones; exact peer cross-pinning; each device alternated Wi-Fi H1, same-LAN P2P, and cellular as client while the other provided; real Wikipedia and fast.com traffic; Direct Wi-Fi/cellular brackets; primitive client/provider recovery and memory counters; fresh synthetic provider heap/goroutine profile; WireGuard/Tailscale/gVisor/DPDK/VPP design comparison | Client phases all stayed below 24 MiB (17.19--18.57 MiB on the Galaxy and 22.78--23.04 MiB on the Pixel), but provider work failed the gate: Pixel peaked at 26.12 MiB and Galaxy at 30.43 MiB with ten samples above 28 MiB. At the Galaxy maximum, live heap was 13.99 MiB, packet outstanding was at most 1.78 MiB, returned packet-pool storage was only about 0.21 MiB, and 748 goroutines were live. A fresh 192-UDP-flow provider profile reproduced 30.7 MiB runtime / 13.6 MiB live heap and 621 goroutines, including 384 per-flow UDP reader/send loops; the next provider-memory direction is a bounded shared socket poller, not a larger pool or shorter unmeasured timeout. Exact-peer H1 fast.com medians were 3.6/7.6 Mbit/s on the Galaxy Wi-Fi/cell and 0.61/5.3 Mbit/s on the Pixel, with P2P 2.7/2.9; adjacent Direct Wi-Fi medians were 960/390 Mbit/s. These variable exits do not replace the earlier controlled 38/41/52-Mbit/s lossless-H1 pass. The retained deterministic ACK race emits one initial wire Pack and zero recovery writes; the fixed RTT ring reduces per-ACK accounting from 43.64--44.92 ns, 64 B, and one allocation to 20.26--20.32 ns, 0 B, and zero allocations. A 32-frame/48-KiB provider group was about 12% faster locally but showed no physical win, so production remains 16/24. Full analysis and research ordering are in `MEMSTEADY.md`. |
| 2026-08-26 | CNN H1 poisoned-exit remediation and two-device playback/memory validation | Deterministic post-establishment H1 DASH blackhole, quarantined-affinity donor, sticky-flow bound, source-scoped provider diagnostics and reset-priority tests; two 27-minute production-rate Android sessions; public United States H1 on Wi-Fi/cellular and exact-ID same-LAN P2P in both provider directions; Chrome on the real CNN Nepal live-news page | Both public paths and both P2P directions advanced through preroll into CNN footage, with zero local client/provider security blocks. A reused media session across an egress change returned a CNN error; a fresh connection on the stable P2P route played, matching the rule that a poisoned established H1 connection is reset rather than rebound across egress IPs. Pixel/Galaxy provider runtime peaked at 38.84/39.26 MiB, live heap at 21.54/22.35 MiB, and goroutines at 1,346/1,071; exact returned pool storage was only 0.25 MiB. P2P clients peaked at 23.74/24.02 MiB and both final disconnected states were below 24 MiB. A fresh 192-UDP-flow profile again showed 192 socket readers plus 192 send/idle loops and 621 loaded goroutines. Provider build/policy identity and per-source block counters now cross Connect, SDK and mobile RPC. The reliability fix passes functional device validation; provider-active memory still fails the 24/28-MiB gates. Root attribution and collision-safe poller/lifecycle candidates are in `MEMSTEADY.md`. |
| 2026-08-26 | Exact-final Android P2P telemetry/playback smoke | Rebuilt current Connect + SDK into stamped Play Debug app/test APKs; 5.8-minute commanded sessions on Pixel 8 Pro and Galaxy S24 Ultra; reversed exact-ID same-LAN provider/client roles; explicit H1; real CNN page and video | Both directions played moving video. Each client received exactly one provider diagnostic with nonempty build and policy hashes; all local and source-scoped provider block counters remained zero. Pixel/Galaxy combined-role runtime peaks were 32.23/31.16 MiB with 4/11 samples above 28 MiB; packet ownership peaked at only 0.39/0.28 MiB, returned pools at 0.25 MiB, and packet-pressure/H1 queue drops were zero. Client snapshots were 19.78/21.83 MiB, while provider snapshots were 30.38/29.34 MiB. Both instrumentation tests passed, temporary clients were released, and credentials/device/host artifacts were removed. Functional final-source validation passes; provider-active memory remains outside the 24/28-MiB gate. |
| 2026-08-26 | Final poisoned-exit source and adjacent server-performance gates | Complete Connect package and subpackages; three race repetitions of the affected DASH blackhole, affinity, sticky-flow, reset-priority and diagnostics tests; complete short SDK suite; Connect/SDK/server vet; full five-repeat 300-ms `-benchmem` tiers for `server/connect`, PERFVAR and proxy | Connect passed in 440.220 s, SDK in 100.040 s, every focused race and vet gate passed, and all 210/10/20 server benchmark samples passed. Production H1 full-payload/ACK-sized medians were 908.5/371.4 ns with unchanged 17/10 B/op and two allocations, +2.75%/+1.28% versus the adjacent cohort. PERFVAR receive-credit improved 673.4 -> 651.0 ns (-3.33%); proxy batch-64 moved 5,444 -> 5,523 ns (+1.45%) with unchanged allocation shape. The small opposing host shifts provide no measured evidence of a shared-server performance regression. |
| 2026-08-26 | Post-pull pushed-head Android and server closeout | Rebuilt stamped Play Debug app/test APKs after pulling two generated security/blocker updates; fresh exact-ID H1 P2P role reversal on Pixel 8 Pro and Galaxy S24 Ultra; cache-busted real CNN video; repeated 210/10/20 server benchmark tiers; seven-sample parent/current/parent H1 bracket | Both directions progressed into moving CNN footage. Each client received the stamped build and new policy hash; all local and provider block counters were zero. Pixel/Galaxy client snapshots were 19.09/21.98 MiB; combined-role peaks were 32.02/31.96 MiB with 4/5 samples above 28 MiB, 0.28/0.32 MiB packet ownership, 0.25 MiB returned pools, and zero packet-pressure/H1 queue drops. Both 242/246-second instrumentation sessions passed and private clients/artifacts were removed. All benchmark tiers passed; current H1 full/ACK medians were only +0.65%/+0.86% versus the interpolated parent bracket with identical allocations. Functional pushed-head validation passes; provider memory still fails the 24/28-MiB gate. |
| 2026-08-26 | Latest generated-policy P2P closeout | Exact `8b1d9bf` generated security/blocker table; rebuilt `cnnfixmem-20260826-f` Play Debug app/test APKs; fresh Galaxy-provider -> Pixel-client exact-ID same-LAN P2P; explicit H1; cache-busted real CNN video | Preroll advanced into moving CNN footage. The client received stamped build/policy diagnostics and all local, provider, and source-scoped remote block counters were zero. Client/provider snapshots were 22.56/33.85 MiB; complete-session peaks were 23.31/39.41 MiB with zero/seven samples above 28 MiB, 0.33/0.28 MiB packet ownership, 0.25 MiB returned pools, and zero packet-pressure/H1 receive-drop/backpressure events. Both 162/168-second instrumentation tests passed; temporary clients and private artifacts were removed. Functional latest-policy parity passes, while the provider still fails the 24/28-MiB memory gate. |
| 2026-08-27 | Bloomberg failure boundary on two attached Android devices | Exact Bloomberg video page; Galaxy direct cellular control, public United States H1 on both devices, and exact-ID same-LAN Pixel-to-Galaxy P2P; privacy-safe Chrome video/connection probe; SDK memory, security and provider diagnostics | Direct cellular advanced from 0 to 1.674 s in five seconds with `readyState=4` and 13.34 s buffered. Both public-H1 attempts and the P2P attempt remained at time 0. The P2P path returned about 10.5 MiB and reported zero local/provider security blocks, so it was not a packet blackhole. Six media 403 responses were retried on the same H2 connection and could not invoke MultiClient again. Public exits exposed zero provider build/policy diagnostics, so external Bloomberg reputation deployment could not be verified. This run isolates route-selection opportunity; it does not prove a source fix yet. |
| 2026-08-27 | Bloomberg playback memory surrogate | Two 76-sample long-lived Android sessions spanning public H1 and P2P provider/client work | Pixel/client peak runtime was 21,241,888 B (20.26 MiB), live heap 7,293,320 B, with zero samples over 28 MiB. Galaxy/provider peak was 31,858,704 B (30.38 MiB), live heap 16,271,136 B, with seven samples over 28 MiB and 740 goroutines. Packet roots peaked at 1,102,592 B and returned packet storage at 262,144 B. The provider excess is live flow/goroutine topology, not returned-pool high-water; hard-affinity removal must not be credited as a memory fix until a rebuilt-device A/B is measured. |
| 2026-08-27 | Default-off fresh affinity and ACK-rate placement candidate | Deterministic ordinary/pinned/legacy IPv4 and DNS-hint gates; exact TCP cumulative-ACK baseline, duplicate/reorder, wrap and ACK-compression tests; unmeasured-prior, equal completed/live short histories, low-provider, fast-provider, established-flow, still-open H2, bounded-table/TTL and SDK projection tests | Ordinary fresh flows remain unassigned for the provider race; explicit pins and the legacy override inherit; existing tuples are untouched. Each provider starts at advertised bandwidth, while completed and still-open TCP/443 evidence are weighted by quantized active time and ACKed bytes. Exact endpoint and canonical domain are both scored, equal short outcomes stay tied, and the 128-entry/10-minute learner fails open. Five 500-ms M4 Pro repetitions measured median advancing/duplicate ACK costs of 9.426/1.003 ns/op and cold/completed/live fresh-race scoring at 137.9/195.7/198.7 ns/op; all were 0 B/op and zero allocations. Complete Connect/SDK short suites, Connect vet, focused race tests, Android AAR/app/test/unit builds, and all 13 script tests passed. |
| 2026-08-27 | Current-source default-off affinity physical closeout | Two approximately 20.25-minute stamped Android sessions; each phone alternated validated Wi-Fi and cellular public United States H1; exact-ID same-LAN P2P; real Wikipedia, fast.com and Bloomberg traffic; 81 primitive memory samples per phone | Pixel Wi-Fi fast.com measured 61/40/110 Mbit/s (61 median) with 153.1-ms Wikipedia document TTFB, proving no 40-Mbit/s ceiling. Other public medians were 0.68, 6.3 and 4.4 Mbit/s and P2P was 3.5 Mbit/s, so route quality remains variable. Bloomberg played on Galaxy Wi-Fi but failed on Pixel cellular and P2P. Same-H2 403 Fetch requests were not reroutable; a forced fresh transport got another placement but its top-level document was also challenged and Chrome did not retry. The client runtime peak/p95 was 22.00/21.61 MiB with zero samples above 24 or 28 MiB. The provider-inclusive phone peaked at 29.45 MiB, had two >28-MiB samples, and its 20-sample quiet p95 was 25.20 MiB. Returned pools remained 0.25 MiB and queues/drops were zero, leaving provider flow/goroutine topology—not affinity or pool reclaim—as the open memory issue. Public exits exposed no build/policy diagnostics; P2P did and reported zero provider block counters. Temporary clients and private artifacts were removed. |

## References

- [QUIC transport — RFC 9000](https://www.rfc-editor.org/rfc/rfc9000.html)
- [QUIC loss detection and congestion control — RFC 9002](https://www.rfc-editor.org/rfc/rfc9002.html)
- [QUIC DATAGRAM — RFC 9221](https://www.rfc-editor.org/rfc/rfc9221.html)
- [CONNECT-IP — RFC 9484](https://www.rfc-editor.org/rfc/rfc9484.html)
- [FQ-CoDel — RFC 8290](https://www.rfc-editor.org/rfc/rfc8290.html)
- [Datagram PLPMTUD — RFC 8899](https://www.rfc-editor.org/rfc/rfc8899.html)
- [TCP retransmission timeout — RFC 6298](https://www.rfc-editor.org/rfc/rfc6298.html)
- [TCP-in-TCP considerations — RFC 8229](https://www.rfc-editor.org/rfc/rfc8229.html)
- [TLS 1.3 and 0-RTT replay — RFC 8446](https://www.rfc-editor.org/rfc/rfc8446.html)
