# P2PGIG — a path to gigabit P2P throughput

- Status: production fast path implemented; deterministic harness hardening implemented; final exact-tree validation and schema-3 rerun pending
- Research and implementation snapshot: 2026-08-11
- Target: sustained 1 Gbit/s of useful inner IP payload on a clean, capable direct path

## Executive conclusion

The new native P2P fast path exceeded the one-gigabit target in a local
real-route carrier benchmark. This is an implementation result, not a final
full-TUN result or a claim that every device or WAN path will sustain gigabit.
Physical-device and cross-host validation, the lossy and long-RTT field matrix,
and an exact-tree full-TUN performance rerun remain release gates.

The implementation keeps the existing WebRTC peer connection but adds a
capability-negotiated custom RTP/SRTP data lane. It sends complete encrypted
Transfer messages through bounded fragmentation and reassembly without SCTP.
The reliable unordered DataChannel remains the compatibility lane and is
selected automatically when the peer does not advertise or finish warming the
fast lane. The control plane, contract establishment, signaling, and fallback
remain reliable.

This choice fixes the measured architectural limit rather than continuing to
tune the reliable DataChannel/SCTP path:

1. Connect carries TCP, QUIC, and other IP traffic inside a reliable SCTP
   association.
2. SCTP fragments each ordinary tunnel packet into multiple small UDP writes,
   acknowledges it, applies a shared Reno-style congestion window, and
   retransmits loss.
3. Connect's Transfer layer also sequences, acknowledges, and may retransmit
   the same data.
4. The app, Transfer, crypto, UDP, and TUN boundaries previously discarded
   useful batches.

The old local same-host WebRTC route reached about 32–35 MiB/s with the
production 3 KiB Transfer message size. A CPU profile attributed 47.89% of all
samples to the raw syscall boundary and about 38% to the SCTP → DTLS → ICE →
UDP write chain. Increasing the route queue from 1 to 32 did not improve
throughput. The production implementation therefore uses a negotiated
**P2P datagram data plane** for native peers:

- retain the existing reliable WebRTC/Transfer path for signaling, identity,
  contracts, key establishment, accounting checkpoints, control messages, and
  fallback;
- carry encrypted Transfer messages as independently authenticated SRTP
  fragments, with no SCTP data retransmission;
- preserve Connect policy, IP-security checks, replay protection, and contract
  accounting;
- preserve batches at the implemented TUN/ABI, committed-receive,
  provider-return, and carrier-write boundaries while retaining singular
  policy, routing, and Transfer steps where no batch API exists yet;
- use platform UDP and TUN batching where the OS exposes it;
- keep legacy WebRTC DataChannel behavior for browsers and old clients.

Direct-stream packet recovery now follows the component that still owns the
bytes. Device-originated TCP and QUIC leave recovery to the inner transport,
and UDP/ICMP retain datagram loss semantics. Provider-return TCP remains
Transfer-acknowledged because the user-NAT proxy has consumed the upstream
socket bytes before it synthesizes a return segment; the inner source cannot
make that proxy reproduce a lost segment. A reliable DataChannel fallback
still provides carrier reliability for an older peer. Contract-opening traffic
is acknowledged before unacknowledged data is admitted. The exchange route
deliberately remains a reliable Transfer route; it receives the shared batching
improvements but does not pretend to be a datagram path.

Correctness is path-independent. A reply preserves the received `TransferKey`,
derives its destination from the authenticated receive path, and lets the
destination-keyed multiroute writer choose any live route. The wire
`TransferKey` contains neither a source ID nor `StreamId`; the authenticated
callback path supplies the reply destination. A stream transport becomes
eligible for payload only after end-to-end readiness succeeds and is withdrawn
when readiness is lost. A caller may prefer a route for measured performance,
but correctness must not depend on that preference.

The target batch pipeline must not be P2P-specific. Direct P2P and the
exchange/platform route share the app TUN, SDK, IP parsing, security policy,
provider selection, Transfer, and route-manager boundaries. A generic packet
batch should eventually travel through those shared stages, then branch at the
selected transport: compact datagrams for a negotiated P2P fast path, or a
batched platform carrier for the exchange path. Local routing should use the
same API. This implementation preserves batches at the measured boundaries,
but policy, route selection, and much of Transfer still process individual
packets. Those remaining singleton handoffs are common headroom for P2P and
exchange traffic.

The production carrier does not yet use the compact one-inner-packet/one-UDP-
datagram wire prototype described later. It reuses ICE, DTLS, SRTP, endpoint
migration, and capability negotiation already owned by the Pion association.
This reduced rollout risk and still removed SCTP's ceiling. The compact raw
authenticated UDP codec remains a measured option for more headroom, not a
second selected production path.

Implemented route-neutral work includes:

- ready-only H3 framing batches on both the Connect client and
  `server/connect`, while preserving the existing H1 and exchange gathers;
- Linux `sendmmsg` for ready SRTP bursts and a bounded asynchronous socket
  writer on other native systems;
- batch receive through `IpMux`, `UpgradeMux`, `DeviceLocal`, and proxy TUN;
- a packed uint16-length-prefixed SDK ABI that crosses Go/native once per
  burst on Apple and Windows;
- bounded TUN drains in the SDK I/O loop used by Android and Linux;
- Apple packet-array and Windows Wintun-ring drains capped at 64 packets;
- DPLPMTUD enabled for client and server H3, starting conservatively at 1,400
  bytes.

The production fast carrier is stream-length agnostic. Every P2P association
implements the same transport contract, so a stream composes any supported
number of adjacent hops. Recorded pre-final full-TUN tests verified exact
bidirectional TCP delivery over one, three, five, and nine WebRTC hops, required
fast traffic on every adjacent carrier, and rejected fallback, carrier drops,
and non-adjacent shortcuts. The current exact tree still needs the same run.
The compact codec independently covers the same one-to-nine-hop range.

Desktop and Linux gigabit is a credible target after this work, provided the
unencrypted underlay sustains materially more than 1 Gbit/s. Gigabit on mobile
is conditional on the radio, device, OS packet API, CPU, and thermal state; it
must not be inferred from a desktop result.

### Implementation result

The comparable `server/connect` harness creates real clients, handlers,
residents, exchange forwarding, contracts, route selection, and Pion peer
connections. Each run sends 16,384 uniquely indexed 1,380-byte payloads and
fails on a missing, duplicate, out-of-range, fallback, or carrier-drop event.
Five runs on an Apple M1 Max with Go 1.26.5 produced:

| Forced route | Median useful payload | Worst of five |
|---|---:|---:|
| Exchange H1 | 169.31 MB/s | 154.44 MB/s |
| Exchange H3 | 82.44 MB/s | 79.41 MB/s |
| P2P legacy DataChannel | 47.17 MB/s | 45.15 MB/s |
| P2P SRTP fast path | 150.73 MB/s | 144.79 MB/s |

The P2P fast-path median is 1.206 Gbit/s of useful payload and its worst run is
1.158 Gbit/s. Its median gain over the production legacy P2P route is 3.20x.
Across the fast measurements, the source recorded 42,242 complete messages,
118,026,417 message bytes, and 124,418 SRTP fragments, with zero fallback and
zero observed carrier drops.

The exact source patch and raw output for this historical four-route run were
not retained. The command below reproduces the method, but it cannot recreate
the measured binary from repository revisions alone. Treat the values as
historical implementation evidence until the exact-tree rerun replaces them.

The production-carrier microbenchmark at the same 1,380-byte useful size
measured a 194.78 MB/s five-run median and one allocation per operation. The
legacy WebRTC route measured 33.30 MB/s and about 180 allocations per
operation, a 5.85x median gain.

The existing full-stack directional TCP test adds the userspace TUN, gVisor
NAT, provider path, and local OS TCP endpoints. After increasing the bounded
fast receive burst window from 64 messages (about 1.5 ms at target rate) to
1,024 messages (about 24 ms), five consecutive complete test invocations had
no stalled or discarded sample. Best-of-three useful goodput in those
invocations ranged from 115.58 to 119.61 MiB/s upload and 118.39 to 123.05
MiB/s download. This test exercises automatic routing rather than forcing and
asserting a carrier, so it is supporting whole-stack evidence; the forced
four-route test above is the authoritative carrier comparison.

Those directional figures predate the final recovery-owner correction that
restored Transfer ACK/retry for provider-return TCP. They remain historical
evidence for the batching and carrier work, not a post-fix baseline.

The latest recorded full-TUN campaign is the schema-1 and schema-2 baseline in
[server/connect/perfvar/MEASUREMENTS.md](../server/connect/perfvar/MEASUREMENTS.md).
Its clean 8 MiB runs measured fast-P2P medians of 850.190 Mbit/s download and
282.826 Mbit/s upload. The download consumed about 99.4% of its measured
userspace calibration, so it is a same-host lower bound rather than proof of a
route ceiling. The campaign also recorded only 52 completions in 100 attempts
across the 500 ms, 1 s, and H1-extender matrices. Of the 48 failures, 47 timed
out waiting for the first probe response after the destination accepted the
inner TCP connection, with no modeled loss, MTU, queue, outage, receiver, or
P2P-network drop.

Those schema-1 and schema-2 results also predate the recovery-owner correction
and later route-lifecycle work. Schema 3 is intended to become the post-fix
directional baseline, but no schema-3 campaign has been recorded yet. The last
results must remain visible as historical evidence until an exact-tree schema-3
run confirms which limits remain.

### Measurement trust architecture

The current schema-3 harness implementation removes several ways a same-host
result could look faster or cleaner than the production path really was:

- forced P2P keeps the platform control and receive transport alive while a
  destination-keyed controller suppresses only the provider and authenticated
  stream payload aliases; a disconnect remains fail-closed until explicit
  restoration, so fallback cannot silently satisfy a direct-route sample;
- exact application and provider flow markers join every source callback and
  live flow entry before the boundary can advance;
- send-Pack lifecycle trackers join successful and failed publication before
  any directional scheduler or P2P receive-credit pool is reset;
- a source-to-Pack-to-carrier fixed point retries if any generation changes
  during the multi-object pass, including traffic that crosses between two
  nonadjacent route resets;
- a post-workload fixed point joins source, Pack, and carrier activity, freezes
  the carrier-end boundary, and retries if any carrier object or generation
  changes during collection;
- every physical direction owns exact interval epochs for packet and byte
  counts, queue high-water, maximum packet size, and drop cause; P2P adds one
  conservative 1,024-credit receive-admission pool per direction to prevent
  Pion's hidden per-socket read-channel overflow;
- one-hop P2P validation attributes the requested fast or legacy lane, physical
  direction, fallback count, and both endpoints inside the measured interval;
- provider-return markers use the authenticated client identity, independently
  derived UDP tuple, exact payload and wire-byte totals, and terminal state, so
  an incidental packet cannot satisfy the workload boundary;
- generated-client replacement rebases destination suppression before the new
  client pointer is published, retains every replaced owner, and synchronously
  joins each replacement and external transport with `CloseAndWait`;
- schema-3 records identify the server and Connect source with both Git
  revisions and complete dirty-worktree content hashes, along with the resolved
  scenario, trace seeds, schedule version, and route-local warmup size.

Routes use one deterministic, route-excluded seed family and a rotated route
order. Route-specific setup consumes different prefixes of each scheduler, so
paired routes are reproducible but do not receive packet-for-packet identical
impairment decisions.

These mechanisms make the four route records comparable inside this userspace
tier. They do not convert a single-host result into physical-device or
cross-host evidence. The pending schema-3 campaign must still be recorded after
the final deterministic suite passes.

Safe route retirement adds one atomic acquire and one atomic release for a
successful admission to the current route snapshot. A concurrent retirement
can reject an old snapshot and make admission retry. This prevents teardown
from returning while an old route can still enqueue. P2P teardown also closes
and joins its connected-callback admission before the final route removal, then
waits for `RemoveTransport` to join admitted writer snapshots before the send
child performs its last pooled-message drain. Deterministic regressions pause
both an admitted callback and a writer snapshot at those boundaries. In one
five-run local sample, the immutable-snapshot control loop had a 25.08 ns
median and the acquire/release loop had a 30.27 ns median: 5.19 ns, or 20.7%,
at that isolated boundary, with zero allocations. A complete
`RouteSelectorWrite` had a 183.8 ns median and zero allocations.

Both sub-benchmarks include the same snapshot load and channel send/receive;
their delta isolates the lifecycle work better than either absolute value.
The raw output and exact source fingerprint for the quoted sample were not
retained, and nanosecond-scale results vary with host load. The command below
reproduces the method, not those exact values.

### Exchange before-and-after measurement

The exchange optimization was also measured against the production revisions
from immediately before this work: Connect `b4a2bc9`, SDK `b4fa6e6`, and server
`5090308e`. Detached worktrees kept those production sources unchanged. The
same exchange-only harness and the minimum test plumbing needed to force an H3
listener were used in both trees.

Each side ran three alternating test invocations per carrier. Every invocation
performed a 256-packet warmup followed by five measured runs of 16,384 unique
1,380-byte payloads. Thus each result below is the pooled median of 15 exact
delivery runs on the same Apple M1 Max and Go 1.26.5, without the race detector.
Every run rejected loss, duplication, invalid indexes, and incomplete delivery.

| Forced exchange route | Before median | After median | Gain | Before/after worst of 15 |
|---|---:|---:|---:|---:|
| H1 WebSocket/TCP | 178.53 MB/s | 179.56 MB/s | 1.006x (+0.6%; no measurable change) | 159.69 / 157.58 MB/s |
| H3 QUIC stream | 45.92 MB/s | 84.56 MB/s | **1.84x (+84.1%)** | 43.09 / 80.08 MB/s |

The three paired H1 invocation medians ranged from -2.7% to +1.4%, confirming
that its +0.6% pooled result is scheduler noise rather than a claimed gain. The
three paired H3 median gains were 1.81x, 1.83x, and 1.95x. Median H3 allocation
calls fell from 71.63 to 34.82 per packet (-51.4%). Median allocated bytes rose
from 3,912 to 6,391 per packet (+63.4%) because batching trades many small QUIC
handoffs for larger contiguous framing buffers; that byte-volume tradeoff is
remaining allocation headroom, not a leak.

This comparison measures the real Connect clients, H1/H3 carrier, handler,
residents, contracts, and exchange forwarding. It deliberately starts with
Connect frames, so it quantifies the exchange-carrier/server gain but does not
attribute an additional number to the shared app TUN, SDK, or provider batches.
The original four-route table remains the comparable simultaneous route
snapshot; its slightly different absolute exchange values reflect normal host
load and route ordering.

Only the three before-work revisions were retained for this detached
comparison. The after-work patch fingerprint and raw benchmark logs were not.
The table therefore preserves a historical paired measurement but is not an
exactly reconstructable source-to-source benchmark. A replacement comparison
must record both source states and the raw output.

These are same-host software-path results. They establish that Connect's
selected implementation no longer has the old SCTP ceiling and has local
gigabit headroom. They do not establish radio, physical NIC, NAT, Internet,
thermal, or multi-host performance.

---

## What “gigabit” means

One decimal gigabit per second is:

- 1,000,000,000 bits/s;
- 125,000,000 bytes/s;
- 119.2 MiB/s.

The target in this document is **useful inner IP payload**, not UDP wire rate or
an isolated codec benchmark. Protocol overhead means the physical path must
normally sustain at least 1.1–1.3 Gbit/s to deliver 1 Gbit/s of inner payload.
An implementation stage should sustain 150–200 MiB/s in isolation so normal
composition does not consume all available headroom.

The result must also preserve latency, loss behavior, security, policy, and
accounting. A benchmark that reaches line rate by building a standing queue,
disabling authentication, or omitting contract accounting does not meet the
target.

## Current native P2P data path

The native path now branches only at the negotiated carrier:

    app TUN
      → SDK packet batch
      → IP parsing, policy, routing, and contract selection
      → Transfer Frame / Pack envelope and optional outer AEAD
      → P2P Auto selection
          → fast: custom RTP fragmentation → SRTP → DTLS/ICE → UDP
          → legacy: reliable unordered DataChannel → SCTP → DTLS/ICE → UDP
      → peer, followed by the reverse path

Provider traffic then crosses the userspace TUN and gVisor network/NAT path.
The same-network direct route still uses Transfer contracts and security
policy; P2P is not a bypass around those controls.

The important production settings and invariants are:

| Area | Current behavior | Consequence |
|---|---|---|
| Selection | `Auto` by default, with `LegacyOnly` and `FastOnly` production controls | New peers select fast after mutual readiness; old peers continue on the DataChannel |
| Capability | Custom codec in normal SDP plus a bounded, versioned warmup marker | No application payload is sent before both RTP receive workers agree on the wire version; mixed v1/v2 peers stay on SCTP |
| Fast fragment | At most 1,188 bytes of carrier payload with a 16-byte header | A full IPv6/UDP/SRTP/RTP packet fits the 1,280-byte minimum MTU exactly |
| Reassembly | 64 bounded in-progress slots, 64 fragments maximum, two-second expiry | Memory and malformed/incomplete message lifetime are bounded |
| Complete-message queue | 1,024 messages by default | Absorbs about 24 ms at local gigabit rate; overflow drops instead of blocking the SRTP socket reader |
| Native UDP writes | Bounded 256-packet queue, ready-only drains of at most 64 | Linux uses `sendmmsg`; other systems overlap ordered socket writes without an idle batching delay |
| Direct IP recovery | Transfer retry is removed when the inner sender still owns recovery; provider-return TCP stays acknowledged | Device-originated TCP/QUIC recover loss, UDP/ICMP observe datagram loss, and a TCP proxy never discards the only recoverable copy |
| Legacy lane | Existing reliable unordered DataChannel | Preserves browsers, old binaries, control behavior, and rolling-upgrade compatibility |
| TUN MTU | 1,440 bytes | Production SRTP fragments an encrypted Transfer message when needed; the unselected compact raw-UDP prototype uses a lower 1,380-byte inner bound |

Relevant implementation points are
[transport_p2p_webrtc.go](transport_p2p_webrtc.go),
[transport_p2p_webrtc_pc.go](transport_p2p_webrtc_pc.go),
[transport_p2p_fast.go](transport_p2p_fast.go),
[transport_p2p_fast_native.go](transport_p2p_fast_native.go),
[transport_p2p_udp_batch.go](transport_p2p_udp_batch.go),
[transport_p2p.go](transport_p2p.go), [transfer.go](transfer.go),
[ip.go](ip.go), and the SDK app bridge in
[device_local_ioloop.go](../sdk/device_local_ioloop.go).

## Current exchange/platform data path

The exchange path shares the front and back of the P2P path but uses a
different carrier in the middle:

    app TUN
      → SDK packet callback
      → IP parsing, policy, provider/route selection, and Transfer
      → RouteManager
      → PlatformTransport
          → H1: WebSocket over TLS/TCP
          → H3: one reliable QUIC stream
      → server/connect handler and resident
      → server/connect exchange forwarding
      → destination PlatformTransport
      → Transfer, policy, and app TUN

This route has the same inner TCP/QUIC-inside-reliable-carrier problem. H1 adds
TCP recovery and head-of-line blocking; H3 uses a reliable QUIC stream; and
Transfer can add another ACK/retry layer to both. It does not have Pion's SCTP
fragmentation or SCTP receive-window limits, so those two findings remain
P2P-specific.

The exchange route now preserves batching across more of the shared path:

- client H1 drains up to four ready messages and coalesces at most 16 KiB into
  one underlying write in [transport.go](transport.go);
- server H1 uses the same four-message, 16 KiB ready-only coalescer above TLS;
  the upgrade and authentication writes remain pass-through;
- client and server H3 drain up to 16 ready messages or 64 KiB into one
  wire-identical framing write, without waiting for a batch timer;
- [server/connect/resident.go](../server/connect/resident.go) drains up to 256
  messages or 256 KiB and uses `net.Buffers`/`writev` on exchange writes;
- exchange reads use a buffered reader, but parsed messages still cross the
  resident and route channels one at a time;
- Connect carries committed-flow receive batches through the mux and
  DeviceLocal into TUN `WriteBatch`/GRO. Native app adapters use either the SDK
  I/O loop or one packed ABI callback per burst.

The exchange remains reliable H1/H3 stream transport. The current work removes
singleton overhead and improves the shared TUN boundary; removing nested
recovery from exchange traffic would still require a separately negotiated
datagram route implemented on both Connect and `server/connect`.

### TCP socket scheduling and ready-only batching

The client transport, server transport, outbound exchange transport, and
outbound exchange forward path were audited as four separate socket-I/O
boundaries. They should not gain another reader/writer channel:

- client and server WebSocket readers already hand complete messages to
  bounded route channels, and Gorilla's buffered reader consumes bytes already
  fetched by one socket read;
- exchange readers already use a 64 KiB buffered reader and a separate bounded
  dispatch channel;
- exchange socket writers already ready-drain at most 256 messages or 256 KiB
  and use `net.Buffers`/`writev` on production TCP connections; and
- another read-ahead queue would weaken backpressure and add pooled ownership
  without reducing the underlying read syscall count.

The missing useful boundary was the server H1 writer. It previously performed
one WebSocket/TLS write per user message. It now drains at most four messages
that are already ready, sets one deadline, preserves four independent
WebSocket frames, and flushes one bounded buffer above TLS. It never waits for
another message. Control traffic remains a separate write, and the speed-test
loop admits at most one user batch between chunks so a continuous backlog
cannot starve the next control chunk.

Five one-second loopback samples on the Apple M1 Max measured the isolated H1
socket boundary as follows. Values are medians; throughput is useful framed
payload, not full-TUN goodput.

| Boundary | CPUs | Singleton | Ready coalesced | Gain |
|---|---:|---:|---:|---:|
| Client H1/TLS | 1 | 361.40 MB/s | 560.34 MB/s | +55.0% |
| Client H1/TLS | 10 | 216.61 MB/s | 548.90 MB/s | +153.4% |
| Server H1 cleartext | 1 | 578.17 MB/s | 1,283.79 MB/s | +122.0% |
| Server H1 cleartext | 10 | 273.13 MB/s | 869.67 MB/s | +218.4% |
| Server H1/TLS | 1 | 405.15 MB/s | 580.17 MB/s | +43.2% |
| Server H1/TLS | 10 | 228.87 MB/s | 604.72 MB/s | +164.2% |

The saturated paths reduced TCP writes from one per frame to about one per
four frames without adding per-frame allocations. In separate sparse samples,
every mode remained exactly one frame, one deadline, one TCP write, and one TLS
record per delivery. Median server TLS delivery changed from 15.545 to 15.656
microseconds at one CPU and from 20.122 to 20.300 microseconds at ten CPUs; the
sample ranges overlap. Client sparse ranges also overlap. Ready-only batching
therefore provides the saturated gain without a batching timer or a meaningful
sparse latency penalty.

The outbound resident-to-exchange bridge was measured separately because it
publishes one message at a time into a connection whose socket writer already
gathers. An exact barrier test produces one 64-message socket flush when the
connection queue is prefilled, and `[1, 63]` through both the resident
transport and resident forward bridges. A real loopback TCP comparison of
those two shapes measured 32.090 versus 36.380 microseconds per 64-message
burst, a 13.4% difference inside this isolated stage. The absolute difference
is 4.290 microseconds, FIFO and pooled ownership remain exact, and the other 63
messages already share one `writev`. A new batch-channel ownership protocol is
not justified by this result alone. Revisit it if production profiles show
burst-onset exchange writes consuming material CPU or syscalls.

## Findings

### P2PG-001 — reliable tunneling duplicates recovery

**Priority: highest**

**Implementation status: resolved for direct P2P by assigning one recovery
owner per direction; exchange remains a reliable compatibility route.**

Most useful tunnel traffic is already congestion-controlled and reliable at
the inner layer. TCP and QUIC detect loss, adjust their rate, and retransmit.
Putting that traffic inside reliable SCTP creates nested recovery loops:

- the inner transport waits for and reacts to loss;
- SCTP also waits, reduces its association window, and retransmits;
- Connect Transfer may independently retry an unacknowledged Pack.

The exchange route has the same class of duplication with a different outer
carrier: WebSocket/TCP on H1 or a reliable QUIC stream on H3, followed by the
reliable server exchange links. A fast P2P lane removes the P2P instance of the
problem. Removing it from the platform route would require a separately
negotiated datagram lane on the client transport and matching routing support
in `server/connect`; changing only the client cannot make the exchange route
unreliable end to end.

The outer recovery can hide loss from the inner controller for a while, add
latency, and then expose a burst. One lost outer datagram also consumes SCTP
association state shared by unrelated inner flows.

[RFC 9484](https://www.rfc-editor.org/rfc/rfc9484.html) describes this nested
congestion-control and recovery problem for IP tunnels and recommends
unreliable datagrams where appropriate. [RFC 8085 section
3.1.11](https://www.rfc-editor.org/rfc/rfc8085.html#section-3.1.11) likewise
allows proportional-response IP tunnels to rely on inner congestion control,
provided they include aggregate safety mechanisms for traffic that is not
responsive.

**Conclusion:** the native fast lane removes SCTP. Direct-stream IP disables
Transfer retry after acknowledged contract setup only while the inner sender
still owns the bytes needed for recovery. Provider-return TCP retains Transfer
ACK/retry because the user-NAT proxy cannot reproduce bytes already consumed
from its upstream socket. The existing DataChannel is the old-peer fallback.
Keep the exchange route reliable unless a future client and `server/connect`
datagram design is implemented and validated end to end.

### P2PG-002 — small SCTP packets make the path syscall-bound

**Priority: highest**

**Implementation status: resolved for capable native peers by bypassing SCTP;
retained by design on the legacy lane.**

Pion SCTP v1.11.1 uses an initial outgoing MTU of 1,191 bytes. After SCTP
headers and padding, an I-DATA packet carries about 1,156 bytes of application
payload. A normal 1,440-byte inner packet plus the Transfer envelope therefore
cannot fit in one SCTP packet. The current two-frame, approximately 3 KiB
Transfer message normally takes three.

At 1 Gbit/s of 1,440-byte inner packets, Connect must process about 86,806
inner packets per second. Even ideal two-packet Transfer coalescing produces
roughly 130,000 outbound SCTP/DTLS/UDP writes per second before SACK traffic.
Pion's association write loop calls net.Conn.Write once for each serialized
SCTP packet.

This matches the profile:

| CPU profile entry | Cumulative or flat share |
|---|---:|
| rawsyscalln | 47.89% flat |
| SCTP writeLoop | 39.31% cumulative |
| DTLS Write | 38.54% cumulative |
| ICE / UDP WriteToUDPAddrPort | 37.90% cumulative |
| sendto | 37.77% cumulative |
| receive syscall path | 9.99% cumulative |

The profile captured two local peers in one process, so it is not a
single-endpoint CPU budget. It is still decisive about where the composed path
spends its time.

Pion source:

- [SCTP MTU and payload sizing](https://github.com/pion/sctp/blob/v1.11.1/association.go#L66-L92)
- [one net.Conn.Write per raw SCTP packet](https://github.com/pion/sctp/blob/v1.11.1/association.go#L1189-L1204)

**Conclusion:** the production SRTP carrier and bounded native UDP writer
remove SCTP from bulk native traffic. The measured fast/legacy median moved
from 47.17 MB/s to 150.73 MB/s in the comparable real-route harness.

### P2PG-003 — packet batches are lost between layers

**Priority: highest**

**Implementation status: substantially resolved across Connect, SDK, Apple,
Windows, Android/Linux I/O loop, proxy, H3 client, and `server/connect`.**

Gigabit at ordinary MTUs is a packet-rate problem. Before this work, the layers
usually handed off one packet or one Transfer message at a time:

- the SDK I/O loop invokes one packet write callback per TUN read;
- the Apple extension receives an array from NEPacketTunnelFlow, then calls
  into Go once per packet;
- Apple writes a singleton packet array for each Go receive callback;
- Transfer writes each completed TransferFrame separately;
- Pion writes each SCTP packet separately;
- non-Linux UDP paths generally have no system-call batching;
- H3 platform transport writes one framed message at a time;
- exchange reads and resident routing hand off one decoded message at a time,
  even though exchange writes already gather messages for `writev`.

Android is better than a JNI-per-packet design because Go owns the detached VPN
file descriptor after setup. Android's VPN TUN interface still exposes one
packet per read/write, so a Go-side drain or micro-batch is needed above that
boundary.

The implemented pipeline retains independent packet ownership and bounded
burst sizes. It does not concatenate independent packets into one failure
unit. Remaining singleton stages include some Transfer/route bookkeeping and
exchange read/dispatch. They are measured future headroom, not part of the old
SCTP ceiling.

### P2PG-004 — the SCTP window cannot cover a gigabit WAN path

**Priority: high for the legacy path; avoided by the new datagram path**

**Implementation status: avoided by the fast lane; unchanged and bounded for
legacy compatibility.**

The default memory-targeted SDK configuration uses a 512 KiB receive window for
a user-selected network peer and 128 KiB for an automatic public peer. A zero
memory target keeps the Connect or explicit caller setting, and a selected peer
uses at least 512 KiB. Connect's ordinary default is 512 KiB and scales to
256 KiB under its reduced-memory setting. A 512 KiB legacy window has these
theoretical ceilings before any other loss:

| RTT | 512 KiB window ceiling |
|---:|---:|
| 50 ms | 10 MiB/s, about 84 Mbit/s |
| 100 ms | 5 MiB/s, about 42 Mbit/s |

The receive-side bandwidth-delay product for 1 Gbit/s is about 5.96 MiB at
50 ms and 11.92 MiB at 100 ms. A reliable SCTP path would need at least about
8 MiB and 16 MiB respectively, plus sender congestion-window growth, to avoid
flow-control limitation.

A historical 2 MiB selected-window experiment was a successful targeted
diagnostic: controlled 50 ms throughput improved from 2.26 MiB/s to
28–29 MiB/s. That is evidence about the former test configuration, not the
current production window. Physical Android measurements were still
congestion-window limited, not receive-window limited. Raising a production
window again would matter only if SCTP remains; it is neither sufficient nor
free, especially when every peer association reserves mobile memory.

**Conclusion:** retain bounded selected-peer tuning for compatibility, but do
not make a large static SCTP window the gigabit design.

### P2PG-005 — per-packet Transfer metadata and ACK work remain material

**Priority: high**

**Implementation status: duplicate data ACK/retry removed where the inner
sender owns recovery; provider-return TCP deliberately retains it. Transfer
metadata and contract accounting remain for policy compatibility.**

Each hot-path TransferFrame can carry sequence, path, identity, contract, tag,
and encryption metadata. An acknowledged record also creates ACK/accounting
work at the receiver and retry state at the sender. That recovery work is
unnecessary on a direct lane while the inner sender still owns the bytes. It is
necessary for provider-return TCP because the proxy has consumed its upstream
bytes. The same per-packet Transfer metadata is present before the exchange
route branches into H1 or H3, so larger envelope-safe batches and cumulative
accounting also benefit platform traffic and the legacy fallback.

The contract cannot simply be removed: current accounting advances with
delivered or acknowledged bytes. The fast path needs an equivalent monotonic
record without turning an accounting receipt into a data recovery protocol.

**Conclusion:** direct stream data uses unacknowledged Transfer records when
the inner sender owns recovery. Provider-return TCP retains Transfer
acknowledgements because the proxy no longer owns the upstream bytes needed to
rebuild a lost segment. Sequence contract setup also remains acknowledged. The
historical carrier result exceeded the target without removing the metadata
needed by current policy and accounting. Binding contracts once per carrier
generation and cumulative receipts remain a possible compact-v2 optimization,
not a prerequisite for the selected rollout.

### P2PG-006 — the composed provider path has a second ceiling

**Priority: high after the transport prototype**

**Implementation status: the pre-correction local path reached the target;
post-fix schema-3 performance, physical, and cross-host validation remain.**

The isolated gVisor TUN TCP test sustained 290.5–308.5 MiB/s in nine local
results, comfortably above gigabit. The optimized Transfer core measured
89.79–94.84 MiB/s with a 92.00 MiB/s median, while the composed provider load
test measured 54.6–58.8 MiB/s.

These harnesses exercise different boundaries, so their numbers and earlier
microbenchmark improvements must not be multiplied. They show:

- gVisor/TUN is not intrinsically capped below gigabit;
- composition, handoffs, and per-packet work lose a large fraction of the
  isolated capacity;
- after replacing SCTP, the full provider path must be profiled again rather
  than assumed solved;
- exchange/platform traffic traverses the same app, NAT, policy, Transfer, and
  provider composition, then adds client platform and server exchange stages;
  it needs its own full-path profile rather than borrowing the P2P result.

Detailed prior measurements are in
[PACKETRESEARCH1.md](PACKETRESEARCH1.md) and
[OPTIMIZENETWORKPEER1.md](OPTIMIZENETWORKPEER1.md).

The new directional full-stack test completed five consecutive invocations
without a stalled sample after the receive burst-window correction. Its best
per-invocation results straddled the 119.2 MiB/s decimal-gigabit threshold.
These figures predate the provider-return recovery-owner correction and do not
establish its final throughput cost. They close the former local software
ceiling as historical evidence but cannot substitute for the pending schema-3
run or physical underlay measurements.

### P2PG-007 — encryption is not the primary bottleneck

**Priority: do not optimize first**

**Implementation status: confirmed; authentication remains enabled on every
selected fast fragment.**

The current pooled AES-GCM benchmarks on the same Apple M1 Max measured:

| Operation | Throughput | Allocations |
|---|---:|---:|
| fused outer wrap | about 1,942 MB/s | 0 |
| pooled open | about 3,923 MB/s | 0 |

This is an isolated benchmark, not proof that all crypto composition is free.
It does establish that authenticated encryption itself has ample headroom for
1 Gbit/s on this machine. Removing encryption would weaken the product without
addressing the measured syscall ceiling.

### P2PG-008 — direct-path selection needs ongoing quality and MTU feedback

**Priority: medium**

**Implementation status: partially resolved. ICE migration/fallback is
retained, fixed P2P fragmentation now fits IPv6's 1,280-byte minimum MTU, and
H3 DPLPMTUD is enabled. Continuous delivery-quality scoring and path-aware
growth above that conservative minimum remain future work.**

ICE selects a working pair, but a gigabit implementation also needs to know
whether that pair remains the best path. Local/private endpoints, public
endpoints, interface changes, IPv4/IPv6, NAT behavior, loss, RTT, path MTU,
and relay fallback can all change after setup.

The historical full-TUN MTU diagnostic sent 1,440-byte inner packets through a
silent 1,280-byte outer path. Fast P2P emitted oversized packets, the simulator
dropped them, and inner TCP reset. The serial campaign later reproduced the
same defect at a 1,400-byte outer MTU: a 1,400-byte fast fragment became a
1,472-byte IPv4 datagram after the 16-byte carrier header, 12-byte RTP header,
16-byte SRTP tag, UDP, and IPv4 headers.

The corrected carrier uses 1,188-byte payload fragments. A focused real-Pion
wire test measures an exact 1,280-byte worst-case IPv6 packet and completes
message delivery with zero oversize drops. Focused full-TUN tests also complete
exact 128 KiB inner TCP at both 1,400-byte and 1,280-byte outer MTUs with zero
P2P MTU drops. The fragment header and bidirectional warmup are version 2;
mixed v1/v2 peers retain the stable codec, reject each other's readiness
marker, and deterministically use SCTP during rolling deployment.

For any future compact raw-UDP carrier, the current inner MTU of 1,440 bytes
may fit a compact new header over IPv4 but can exceed a 1,500-byte outer path
over IPv6. Sending an oversized encrypted UDP datagram and relying on IP
fragmentation amplifies loss: one missing fragment loses the entire inner
packet. The selected SRTP carrier avoids that problem by fragmenting Transfer
messages at the safe 1,188-byte payload bound.

The app usually exposes one TUN MTU for all routes. Lowering it only in the P2P
encoder would turn valid 1,440-byte packets into route-dependent drops. The
shared boundary must either advertise the conservative minimum of all active
routes or synthesize correct Packet Too Big/MSS behavior before a packet enters
a route whose effective MTU is smaller. Platform and local routes must remain
correct when the active P2P path changes that minimum.

**Conclusion:** keep the fixed 1,188-byte payload as the minimum-path-safe
default. Add Datagram Packetization Layer Path MTU Discovery or negotiated
larger fragments, validated migration, and quality hysteresis to reclaim
headroom on larger paths. Retain correct Packet Too Big/MSS behavior for any
future route whose effective MTU is smaller and retain the reliable fallback.

### Applicability by route

| Finding or optimization | Direct P2P | Exchange/platform | Implementation boundary |
|---|---|---|---|
| Avoid nested reliable recovery | Yes where the inner sender owns recovery; provider-return TCP keeps Transfer as its sole recovery owner | Yes in principle; requires a client datagram carrier and `server/connect` datagram routing | Connect plus `server/connect` for exchange |
| Remove SCTP fragmentation and window limits | Yes | No; H1/H3 have different limits | Connect P2P transport |
| Target: preserve batches from app TUN through policy and route selection | Yes | Yes | App, SDK, Connect |
| Batch Transfer and route-manager handoffs | Legacy, control, and acknowledged provider-return TCP benefit most; other fast data still crosses route selection without retry work | Yes, primary shared improvement | Connect |
| Batch client carrier writes/reads | Linux `sendmmsg`; bounded ready drains elsewhere | H1/H3 framing and drains | Connect |
| Batch resident/exchange forwarding | Control traffic only for direct P2P | Yes | `server/connect` |
| Target: reuse parse/policy/flow metadata within a bounded batch | Yes | Yes | SDK/Connect, with identical security decisions |
| TUN `WriteBatch` and GRO/TSO | Yes | Yes | SDK/apps/Connect |
| Cumulative authenticated accounting receipts | Optional compact-v2 headroom | Required for any future exchange datagram lane | Connect protocol and `server/connect` routing |
| Path MTU, bounded queues, drop telemetry, migration | Fixed fragmentation fits IPv6's 1,280-byte minimum; more scoring and path-aware growth remain | H3 DPLPMTUD today; required for any future exchange datagram lane | Connect and `server/connect` |

The route-neutral items should be implemented once and consumed by all routes.
Transport-specific adapters must not force the shared batch type to contain
P2P receiver indices, UDP addresses, WebSocket frames, or exchange resident
state.

---

## Reproducible measurements

### Comparable production route harness

From `server`, with the normal local Postgres and Redis test services:

~~~sh
CONNECT_STREAM_ROUTE_PERFORMANCE_MEASURE=1 \
WARP_ENV=local WARP_SERVICE=test WARP_DOMAIN=bringyour.com \
WARP_BLOCK=test WARP_VERSION=0.0.0 \
BRINGYOUR_POSTGRES_HOSTNAME=local-pg.bringyour.com \
BRINGYOUR_REDIS_HOSTNAME=local-redis.bringyour.com \
go test ./connect -run '^TestStreamRoutePerformanceComparison$' \
  -count=1 -timeout=15m -v
~~~

Set `CONNECT_STREAM_ROUTE_PERFORMANCE_ROUTE` to `exchange-h1`,
`exchange-h3`, `p2p-legacy`, or `p2p-fast` to isolate one route. The always-on
`TestStreamRouteDataPlaneSelection` uses the same real topology with a small
payload and deterministically asserts all four carrier selections.

The full TUN/provider directional gate is:

~~~sh
WARP_ENV=local WARP_SERVICE=test WARP_DOMAIN=bringyour.com \
WARP_BLOCK=test WARP_VERSION=0.0.0 \
BRINGYOUR_POSTGRES_HOSTNAME=local-pg.bringyour.com \
BRINGYOUR_REDIS_HOSTNAME=local-redis.bringyour.com \
go test ./connect \
  -run '^TestConnectMultiClientTcpDirectionalPerformance$' \
  -count=5 -timeout=15m -v
~~~

The extended-topology correctness gate is:

~~~sh
WARP_ENV=local WARP_SERVICE=test WARP_DOMAIN=bringyour.com \
WARP_BLOCK=test WARP_VERSION=0.0.0 \
BRINGYOUR_POSTGRES_HOSTNAME=local-pg.bringyour.com \
BRINGYOUR_REDIS_HOSTNAME=local-redis.bringyour.com \
go test ./connect/perfvar \
  -run '^(TestFullTunP2pFast(One|Three|Five|Nine)HopTopologyCorrectness|TestFullTunSplitExchangeH(1|3)ExtendedTopologyCorrectness)$' \
  -count=1 -timeout=40m -v
~~~

On a pre-final 2026-08-11 working tree, the one-, three-, five-, and nine-hop
P2P cases passed in 14.54, 11.29, 18.13, and 19.29 seconds. Split exchange H1
and H3 passed in 10.09 and 11.20 seconds. These are wall-clock correctness-test
durations, not throughput measurements. The P2P cases transfer exact 64 KiB TCP
content in both directions on the clean profile. The split cases use clean
endpoint access links and a 25 ms internal link. No gigabit or high-RTT claim
follows from these timings.

The recorded runs predate later recovery-owner and route-lifecycle edits. They
show that the topology harness found the intended behavior, but they do not
validate the current exact tree. Rerun all six cases and record the source
fingerprint before promoting them to final evidence.

Development runs also passed
`TestProviderReturnIpTransferOptionsMatchRecoveryOwner` 50 times normally and
20 times under the race detector. That regression ensures provider-return TCP
keeps Transfer ACK/retry even when a caller's default options disable ACKs. From
`connect`, reproduce both forms with:

~~~sh
go test -run '^TestProviderReturnIpTransferOptionsMatchRecoveryOwner$' \
  -count=50 .
go test -race \
  -run '^TestProviderReturnIpTransferOptionsMatchRecoveryOwner$' \
  -count=20 .
~~~

These recorded repetitions also need an exact-tree rerun because their raw
output and source fingerprint were not retained.

From `connect`, reproduce the route-admission cost with:

~~~sh
go test -run '^$' \
  -bench '^(BenchmarkRouteSelectorWrite|BenchmarkRouteSnapshotWriterAdmission)$' \
  -benchtime=1s -count=5 .
~~~

From `connect`, compare the selected production P2P carriers with:

~~~sh
go test -run '^$' \
  -bench '^(BenchmarkStreamLegacyWebRtcRoute|BenchmarkStreamFastWebRtcRoute)$' \
  -benchtime=1s -count=5 .
~~~

### Legacy detached WebRTC baseline

Run:

~~~sh
CONNECT_WEBRTC_ROUTE_THROUGHPUT_MEASURE=1 \
  go test -run '^TestWebRtcP2pRouteThroughputMeasurement$' -count=1 -v .
~~~

Results from the 3 KiB production-size message:

| Route queue depth | Throughput |
|---:|---:|
| 1 | 35.1 MiB/s |
| 4 | 32.1 MiB/s |
| 8 | 33.9 MiB/s |
| 32 | 32.5 MiB/s |

Prior larger-message experiments reached approximately 56–58 MiB/s at
12–48 KiB. That is useful evidence that fewer app handoffs help, but SCTP still
fragments those messages into roughly 1.2 KiB datagrams. It is not a route to
119.2 MiB/s useful payload.

### CPU profile

Run:

~~~sh
CONNECT_WEBRTC_ROUTE_THROUGHPUT_MEASURE=1 \
  go test -run '^TestWebRtcP2pRouteThroughputMeasurement$' \
  -count=1 -cpuprofile /tmp/connect-p2p.pprof .
go tool pprof -top -cum /tmp/connect-p2p.pprof
~~~

The captured run lasted 4.13 seconds and contained 7.81 CPU-seconds of samples
because both local endpoints were active. Its top path is summarized in
P2PG-002.

### Loss and delivery semantics

Run:

~~~sh
CONNECT_WEBRTC_LOSS_MEASURE=1 \
  go test -run '^TestWebRtcDataChannelLossHeadOfLineMeasurement$' \
  -count=1 -v .
~~~

After one deterministic dropped DTLS datagram:

| DataChannel mode | Second-message p95 / max |
|---|---:|
| ordered reliable | about 124.8 ms |
| unordered reliable, current production mode | about 558.8 µs |
| unordered, zero retransmissions | about 333.5 µs |

Reliable unordered delivery was a sound latency improvement. Partial
reliability is a useful compatibility experiment, but it still retains the
SCTP association, SACK processing, congestion window, fragmentation, and
one-write-per-packet loop.

### Crypto and TUN isolation

Run:

~~~sh
go test -run '^$' \
  -bench 'BenchmarkSequenceCipher(OuterWrap|Open)$' \
  -benchtime=1s .
go test -run '^TestTunTCPThroughput$' -count=3 -v .
~~~

The results support P2PG-006 and P2PG-007. They are diagnostic isolation
measurements, not end-to-end throughput claims.

---

## What Tailscale does differently

The source audit used Tailscale commit
[e1e5325c22a46a9df2e76d725f01f92065885138](https://github.com/tailscale/tailscale/tree/e1e5325c22a46a9df2e76d725f01f92065885138)
and its WireGuard-Go fork commit
[4affce44577c](https://github.com/tailscale/wireguard-go/tree/4affce44577c).
The audit focused on the data path, batching, path selection, and MTU behavior,
not just published benchmark numbers.

### One inner packet remains one encrypted datagram

WireGuard authenticates and encrypts an IP packet, then sends it as a UDP
datagram. It does not add reliable delivery, a byte-stream receive window, or
outer data retransmission. Inner TCP or QUIC sees loss and performs recovery.

WireGuard-Go moves packet containers through one encryption worker per CPU,
then serializes nonce/order-sensitive work per peer and sends a batch. Its
generic connection API exposes an
[ideal batch size of 128](https://github.com/tailscale/wireguard-go/blob/4affce44577c/conn/conn.go#L17-L26);
the [send pipeline](https://github.com/tailscale/wireguard-go/blob/4affce44577c/device/send.go#L481-L586)
separates parallel encryption from per-peer ordered sending.

This shape is directly applicable to Connect:

- parallelize independent packet parsing and encryption;
- preserve per-peer counter order in one bounded sender;
- submit many independent UDP datagrams in one OS operation.

### Linux uses the kernel's packet batching features

Tailscale's Linux batch connection uses:

- sendmmsg and recvmmsg;
- UDP Generic Segmentation Offload and Generic Receive Offload;
- scatter/gather to coalesce up to 64 equal-sized datagrams without copying
  their payload into one application buffer;
- socket overflow reporting;
- runtime fallback when offload is unavailable or fails.

See the
[Linux batching implementation](https://github.com/tailscale/tailscale/blob/e1e5325c22a46a9df2e76d725f01f92065885138/net/batching/conn_linux.go#L86-L130).
On non-Linux platforms, Tailscale's generic implementation reports an ideal
batch size of one:
[conn_default.go](https://github.com/tailscale/tailscale/blob/e1e5325c22a46a9df2e76d725f01f92065885138/net/batching/conn_default.go).

This distinction matters. Tailscale's multi-gigabit public results are
bare-metal or high-capacity Linux results. They demonstrate the architecture's
headroom, not that an iPhone or Android VPN API will deliver the same rate.

### Endpoint selection is continuous, not just setup-time

Tailscale's magicsock endpoint code scores working paths using properties such
as directness, latency, endpoint scope, relay state, and MTU, with hysteresis
to avoid unnecessary switching. It can briefly hedge traffic while a path is
being re-established. See the
[send/path selection](https://github.com/tailscale/tailscale/blob/e1e5325c22a46a9df2e76d725f01f92065885138/wgengine/magicsock/endpoint.go#L997-L1039)
and
[quality scoring](https://github.com/tailscale/tailscale/blob/e1e5325c22a46a9df2e76d725f01f92065885138/wgengine/magicsock/endpoint.go#L1709-L1768).

Connect should borrow the principle, not the control-plane coupling:
authenticate a candidate before migration, measure current path quality, prefer
direct/private paths where they are actually better, and retain a bounded
fallback.

### MTU behavior is conservative

Tailscale accounts for up to 80 bytes of WireGuard/UDP/IP overhead, probes
larger path sizes, and retains a safe baseline until Packet Too Big handling is
available. Its probe set includes 1,280, 1,360, 1,400, 1,500, 8,000, and 9,000
wire bytes. See
[Tailscale MTU policy](https://github.com/tailscale/tailscale/blob/e1e5325c22a46a9df2e76d725f01f92065885138/net/tstun/mtu.go#L64-L101).

The lesson is not “enable jumbo frames.” It is to avoid fragmentation, discover
the usable path size, and make a larger MTU an evidence-based per-path
optimization.

### Published Tailscale results

Tailscale reports:

- [5.36 Gbit/s after TSO, GRO, and message batching work](https://tailscale.com/blog/throughput-improvements);
- [13 Gbit/s on a bare-metal Linux test after UDP GSO/GRO and checksum work](https://tailscale.com/blog/more-throughput);
- [a roughly fourfold UDP/QUIC forwarding improvement on bare-metal Linux](https://tailscale.com/blog/quic-udp-throughput).

Their profiles found the same general cost seen in Connect: TUN and UDP system
calls dominate after obvious user-space allocations and copies are removed.
The useful comparison is the data-path structure and profiling method, not the
headline number.

---

## Implemented architecture and compact-v2 direction

This section preserves the original architectural reasoning and distinguishes
the selected implementation from optional next work. Boundary batching and the
recovery-owner split are implemented; a generic end-to-end batch pipeline is
not. Control and contract setup remain reliable. Device-originated TCP/QUIC and
UDP/ICMP use datagram semantics on the fast carrier, while provider-return TCP
keeps Transfer recovery. The selected carrier reuses RTP/SRTP on the existing
ICE/DTLS association. The dedicated compact authenticated UDP format, custom
exporter keys, and cumulative receipts remain an unselected v2 option because
the lower-risk SRTP carrier already exceeded the carrier target.

The P2P fast path is one transport adapter under the target route-neutral
packet pipeline. Implemented boundary APIs carry bounded slices of independent
packet owners and preserve packet ownership, but the intervening policy,
routing, and Transfer stages remain substantially singular. Source policy and
`connect/ip_security` remain mandatory before route selection; destination
policy remains mandatory after authentication. A future generic batch may
contain packets that choose different providers or carriers, so route selection
must group accepted packets into sub-batches by destination/session without
changing their order within a flow.

A future common outbound batch would enter one of three adapters:

- negotiated P2P fast path: endpoint/hop seal and UDP batch write;
- exchange/platform path: envelope-safe Transfer batch and H1/H3 carrier
  batch;
- local path: LocalUserNat batch, which already exists internally.

Inbound adapters would produce the same packet batch contract before common
policy, flow association, statistics, and app/TUN delivery. This would keep
platform arrays and TUN batches useful even when a flow falls back from P2P to
the exchange. Fallback would change the transport adapter, not the TUN API.

### 1. Keep the current connection as the control plane — implemented

The existing path already provides:

- ICE discovery and direct-path negotiation;
- peer identity and authenticated TLS exporter material;
- post-quantum-capable X25519MLKEM768 TLS negotiation;
- contracts and escrow state;
- signaling, liveness, and legacy interoperability.

It remains reliable. Normal WebRTC SDP negotiates the custom codec, and a
bounded warmup proves both receive workers before `Auto` selects the lane.
Browsers and older clients continue using DataChannel/Transfer unchanged.

The production path wraps Pion's selected network while preserving its
concrete UDP methods. Linux uses `sendmmsg`; deeper GSO/GRO, ECN, and socket
overflow work remains optional headroom.

### 2. Establish a dedicated native datagram component — prototype only

The selected implementation establishes a custom RTP sender and receiver on
the existing peer connection. Bulk packets do not pass through SCTP. A
separate raw UDP component is no longer required to hit the target, but the
compact prototype below remains available if one-message fragmentation or
SRTP overhead becomes the next measured ceiling.

Each wire datagram should contain exactly one inner IP packet:

    compact authenticated header
      + encrypted inner IP packet
      + AEAD tag

Candidate header fields:

- protocol version and flags;
- receiver or session index;
- 64-bit packet counter;
- key generation, if it is not implicit in the receiver index.

Peer IDs, route IDs, contract IDs, and long protobuf structures belong in the
authenticated session setup, not every packet. Header fields are AEAD
additional authenticated data.

### 3. Derive new directional keys — supplied by DTLS/SRTP today

The selected carrier uses Pion's DTLS-derived directional SRTP keys,
authentication, replay handling, and rollover. The compact prototype derives
independent endpoint and hop keys from exporter material with explicit domain
separation; those keys are not used by production traffic.

Do not reuse the current sequenceCipher wire format unchanged. It uses a fresh
random nonce per TransferFrame and no explicit replay window or authenticated
header. That is suitable for its reliable Transfer transport, but a datagram
protocol needs:

- monotonic per-direction counters;
- deterministic unique nonces derived from the counter and key generation;
- a sliding receive replay window;
- authenticated receiver/session routing;
- bounded current/previous-key overlap during rekey;
- hard packet/time limits before key rotation;
- no response to unauthenticated traffic;
- endpoint roaming only after successful packet authentication;
- rate limits and anti-amplification controls around handshakes and probes.

WireGuard's
[protocol](https://www.wireguard.com/protocol/) and
[paper](https://www.wireguard.com/papers/wireguard.pdf) are strong references
for receiver indices, counters, replay windows, rekey overlap, and endpoint
roaming. Connect should preserve its existing identity, contract, and hybrid
post-quantum key establishment instead of replacing those systems.

### 4. Separate accounting from retransmission — direct-data portion implemented

Direct-stream IP uses unacknowledged Transfer after contract establishment
when its inner sender retains recovery ownership. Provider-return TCP is the
intentional exception: the proxy's upstream socket has already acknowledged
and released those bytes, so Transfer must retain them until the destination
acknowledges delivery. A future compact session could reduce metadata further
with cumulative receipts:

1. bind one contract, peer pair, direction, and session epoch during setup;
2. count only authenticated, replay-accepted inner payload bytes at the
   receiver;
3. periodically send a monotonic cumulative receipt over the reliable control
   channel, triggered by both elapsed time and byte threshold;
4. let a later cumulative receipt subsume a lost earlier receipt;
5. reconcile a final receipt during orderly close, rollover, or timeout.

Duplicate, replayed, unauthenticated, rejected-policy, and malformed packets
must not increment the receipt. The sender must never interpret a receipt as an
instruction to retransmit missing data on a datagram-semantic lane. Inner
TCP/QUIC remains responsible where it still owns the bytes; the provider TCP
proxy continues to use acknowledged Transfer records.

On a datagram-semantic lane, this preserves the current meaning of
delivered/acknowledged contract bytes without placing a reliable protocol
around the data. It also reduces per-packet protobuf, ID, tag, ACK, retry-queue,
and timer work. Provider-return TCP remains outside this receipt-only design
unless another component first takes ownership of recoverable bytes.

### 5. Batch across hot boundaries and every route — partially implemented

The implemented boundaries define bounded packet batches with explicit buffer
ownership. They preserve packet boundaries and carry only the metadata each
stage needs. The eventual fully parallel shape remains:

Outbound:

    TUN batch
      → parallel parse and policy
      → route/session lookup and stable per-flow grouping
      → P2P: parallel AEAD seal → ordered UDP batch send
        or exchange: Transfer batch → H1/H3 carrier batch
        or local: LocalUserNat batch

Inbound:

    transport batch receive
      → P2P: session lookup/replay precheck → parallel AEAD open
        or exchange: framed-message batch → Transfer batch decode
      → replay commit, policy, and accounting
      → TUN batch write

Rules:

- queues are bounded by both packet count and byte count;
- observer callbacks never block; the documented synchronous TUN write is the
  flow-control boundary; overload at a datagram queue drops and increments a
  metric;
- encryption can run across CPU workers, while nonce allocation and final
  per-peer send order remain deterministic;
- route, contract, and security decisions may be cached only with explicit
  invalidation on policy/session change;
- packet buffers have one documented owner at every handoff;
- a route change may split a batch but cannot reorder one flow;
- one malformed, unauthorized, or backpressured packet is dropped and counted
  without discarding unrelated packets in the same batch;
- batch fallback is functionally identical to the singular path.

Implemented shared Connect and SDK work:

- adds batch methods beside the existing singular DeviceLocal, mux, and TUN
  methods;
- preserves independent packet ownership while existing singular downstream
  stages continue to parse and apply the same `connect/ip_security` decisions
  per packet;
- wires `RemoteUserNatMultiClient.SetReceivePacketsCallback` through DeviceLocal
  to a production batch app callback while retaining the singular callback for
  compatibility and rare packets;
- let no-contract provider-return traffic form larger envelope-safe Transfer
  Packs, subject to negotiated client and resident message limits; contract
  traffic retains per-packet admission and limited opportunistic coalescing;
- drains ready H1/H3 writes into bounded message batches rather than relying
  on a late socket coalescer to recover earlier per-message costs.

Implemented and remaining `server/connect` work for the exchange route:

- retains the current bounded `writev` gather, batches H3 framing, and applies
  four-frame ready-only coalescing to server H1;
- a batch-aware resident channel remains optional future headroom: the measured
  `[1, 63]` bridge shape cost 4.290 microseconds per 64-message loopback burst,
  which does not justify changing the ownership protocol without production
  profile evidence;
- reading and dispatching several framed messages through one resident-channel
  batch remains future headroom;
- preserve per-message framing, ownership, routing, rate limits, and failure
  isolation inside every batch;
- benchmark one-host and cross-host exchange topologies, because co-located
  loopback can hide the CPU and syscall budget of each production process;
- if an H3 datagram lane is added, route datagrams through the exchange without
  converting them back into a reliable TCP/QUIC stream between residents.

Platform work:

| Platform | Implemented status and remaining headroom |
|---|---|
| Linux | Implemented SDK TUN drain and `sendmmsg`; future `recvmmsg`, UDP GSO/GRO, TUN offload, `SO_RXQ_OVFL`, and runtime offload fallback |
| Apple | Implemented one packed SDK call per `NEPacketTunnelFlow` array and one `writePackets` call per inbound burst |
| Android | Implemented bounded drain above the Go-owned VPN fd; subsequent policy and routing sends remain singular |
| Windows | Implemented bounded Wintun-ring drain and one packed SDK callback/call per burst |

### 6. Add path MTU discovery and quality scoring — partial

The selected carrier now starts with a 1,188-byte transfer payload, which the
actual-wire regression measures as exactly 1,280 bytes after worst-case IPv6,
UDP, RTP, SRTP, and carrier overhead. Use Datagram Packetization Layer PMTUD
to grow above that floor instead of IP fragmentation. Oversized inner packets
should produce correct Packet Too Big behavior or TCP MSS adjustment.

Measure candidate RTT, recent loss/drop signal, delivery rate, path MTU, scope,
and relay/direct state. Switch only after the new endpoint is authenticated
and materially better for long enough to overcome hysteresis. Keep the
reliable WebRTC path available while a new datagram path is unproven.

### 7. Retain congestion safety on datagram-semantic lanes — bounded queues implemented

An IP tunnel may carry UDP applications that do not respond to congestion.
The datagram path therefore needs:

- paced startup rather than an unbounded burst;
- bounded queues that drop instead of creating bufferbloat;
- aggregate path loss/RTT monitoring and a circuit breaker;
- explicit rate policy for persistently nonresponsive traffic;
- receiver overflow and socket-drop telemetry;
- no tunnel retransmission while the inner sender owns recovery.

This follows the proportional-tunnel guidance in RFC 8085. It avoids nested
recovery without treating all inner UDP as automatically safe. Provider-return
TCP remains the intentional exception because Transfer is its sole remaining
recovery owner.

---

## Transport choices

| Choice | Advantages | Limits | Recommendation |
|---|---|---|---|
| Tune current reliable SCTP | Small, compatible changes | Shared Reno window, fragmentation, SACK/retry work, and syscall shape remain | Maintain only as fallback |
| Custom RTP/SRTP on current peer connection | Reuses ICE, DTLS, authentication, replay, migration, SDP capability fallback, and existing sockets; measured above target | Transfer messages larger than 1,188 bytes fragment; not the minimum possible header | Selected production fast path |
| Unordered SCTP with zero retransmissions | Capability-negotiated stepping stone | Still SCTP-fragmented, association-congestion-controlled, and one write per packet | Not selected |
| QUIC DATAGRAM | Mature authentication, migration, PMTUD, and Linux GSO support | Requires a second association and remains QUIC-congestion-controlled | Reconsider only if a future measurement justifies it |
| Connect authenticated raw UDP | Minimum overhead and exact semantics; compact codec already measured | Requires independent replay, rekey, PMTU, pacing, migration validation, and protocol review | Future headroom, not selected |
| WireGuard-Go data plane | Proven batching, queues, replay, counters, roaming, and high throughput | Not a drop-in match for Connect contracts, multi-consumer provider addressing, current identity/PQ session, or control plane | Reuse design and possibly isolated machinery, not the full system by assumption |

[RFC 9221](https://www.rfc-editor.org/rfc/rfc9221.html) explicitly identifies
VPNs as a QUIC DATAGRAM use case and confirms that DATAGRAM frames are
unreliable and not flow-controlled, while still participating in QUIC
congestion control. quic-go can use Linux UDP GSO and DPLPMTUD, but its
documentation warns that wrapping a UDPConn in a generic interface may hide
important capabilities:

- [quic-go Transport and socket requirements](https://quic-go.net/docs/quic/transport/)
- [quic-go performance optimizations](https://quic-go.net/docs/quic/optimizations/)

Multiple WebRTC DataChannels are not a solution. RFC 8831 defines them over one
SCTP association, whose congestion control is shared:
[RFC 8831](https://www.rfc-editor.org/rfc/rfc8831.html).

---

## Optional compact authenticated UDP wire prototype

The Connect repository now contains a compact fast-path data-plane prototype:

- [stream_fast_path.go](stream_fast_path.go) implements the wire codec, key
  derivation, replay window, per-hop accounting, buffer ownership, and batch
  operations;
- [stream_fast_path_test.go](stream_fast_path_test.go) deterministically tests
  every supported stream length, end-to-end opacity, adjacent-hop
  authentication, replay and reordering, generation routing, accounting,
  independent batch drops, size bounds, and steady-state allocations;
- [stream_fast_path_benchmark_test.go](stream_fast_path_benchmark_test.go)
  compares the prototype with the real WebRTC route and exercises UDP through
  one, two, and nine hops in both serial and independently staged forms.

This is a wire and local data-plane prototype. It is not selected by the
production stream, does not carry production traffic, and is not needed for
the measured gigabit result. It remains useful as a lower-overhead comparison
and as tested groundwork if SRTP fragmentation becomes a future ceiling.

### Composition across a stream

The source encrypts an inner packet once with the endpoint key, then seals that
opaque envelope for its first adjacent hop. Each intermediary does exactly two
operations, regardless of its position:

1. authenticate and open the incoming hop envelope;
2. account for the authenticated packet and seal the unchanged endpoint
   envelope to the next hop.

The destination opens its adjacent-hop envelope and then the endpoint envelope.
An intermediary can therefore authenticate its neighbor and enforce its local
contract without receiving the endpoint key or seeing the inner IP packet. A
stream is represented as a source, a slice of identical forwarders, and a
destination. There is no direct-only or fixed-hop branch. The tests cover all
current lengths from one through nine P2P hops.

The wire overhead is constant across stream length:

    endpoint envelope: 8-byte counter + inner packet + 16-byte tag
    hop envelope:      4-byte magic/version + 4-byte receiver index
                       + 8-byte counter + endpoint envelope + 16-byte tag

This adds 56 bytes to the inner packet. The prototype limits the inner packet
to 1,380 bytes, making the largest UDP payload 1,436 bytes and the outer IPv6
packet 1,484 bytes. Production selection must lower the current 1,440-byte TUN
MTU before sending traffic on this lane, then replace the fixed conservative
limit with validated path-MTU behavior.

Keys are derived with HKDF-SHA-256 from identity-bound exporter material. The
derivation includes the stream id, generation id, contract id, adjacent source
and destination ids, layer, and direction. The receiver index is only an
unauthenticated local lookup key; the selected generation authenticates the
complete header before committing replay or accounting state. A 1,024-packet
sliding window accepts bounded reordering and rejects zero, duplicates, and old
packets. Authentication completes before the window advances, so a forged
future counter cannot evict valid packets.

### Prototype measurements

These results are five-run medians on the same Apple M1 Max used for the
baseline research, using Go 1.26.5 and 1,380 useful inner bytes per packet:

| Benchmark | Median useful throughput | Steady-state allocations |
|---|---:|---:|
| Fast-path endpoint plus one hop, codec only | 668.35 MB/s | 0 |
| Fast-path endpoint plus nine hops, codec only | 139.40 MB/s | 0 |
| Fast-path one-hop UDP loopback, serial batch of 64 | 147.80 MB/s | 0 |
| Fast-path one-hop UDP loopback, independently staged batch of 64 | 175.40 MB/s | 0 |
| Production legacy WebRTC/SCTP route | 33.30 MB/s | about 180 per operation |
| Selected production RTP/SRTP route | 194.78 MB/s | 1 per operation |

The compact serial UDP result has a 1.18 Gbit/s median and the staged result a
1.40 Gbit/s median. The selected SRTP carrier is faster in this same-host
microbenchmark because it reuses optimized Pion state and does not perform the
prototype's extra endpoint-plus-hop AEAD composition. Both remove the old
DataChannel/SCTP ceiling.

The codec-only nine-hop result charges every hop's cryptography to one process.
In a deployed stream that work is distributed across the clients. Conversely,
the multi-socket UDP benchmarks place all simulated clients and all loopback
kernel work on one machine, so their long-stream aggregate is a stress test,
not a model of per-client capacity. None of these numbers includes a TUN,
provider policy, WAN loss, pacing, path-MTU discovery, or contract receipt
traffic. They prove that the proposed wire shape removes the measured SCTP
ceiling; they do not yet prove a gigabit product tunnel.

Reproduce the focused comparison with:

```sh
go test -run '^$' \
  -bench '^(BenchmarkStreamFastPathUDPOneHopBatch64|BenchmarkStreamFastPathUDPPipelineOneHopBatch64|BenchmarkStreamLegacyWebRtcRoute|BenchmarkStreamFastWebRtcRoute)$' \
  -benchtime=1s -count=5 .
```

### Repository boundary

The P2P fast path itself can be implemented entirely in Connect. Connect owns
`StreamSequence`, each adjacent P2P transport, route management, ICE, packet
crypto, replay state, and the protocol package needed for capability and
receipt messages. For a direct P2P stream, the server can continue forwarding
the reliable control messages and does not need to inspect data datagrams.

The complete route-neutral gigabit path cannot be finished only in Connect.
The SDK and app repositories own the platform TUN boundaries. Preserving Apple
packet arrays, adding Android and Windows drain batches, and passing batches
into and out of Connect require changes at those boundaries. The exchange
route also lives partly in [server/connect](../server/connect): resident
forwarding, exchange framing, server-side batching, and any future platform
datagram route must be implemented and measured there. A client-only fast lane
that becomes reliable again inside the exchange does not solve nested recovery.

The intended ownership is therefore:

| Area | Repository |
|---|---|
| Shared packet-batch contract, policy, route grouping, Transfer batching | `connect` |
| DeviceLocal and platform app/TUN batch bridge | `sdk`, Apple, Android, Windows, Linux apps |
| Direct P2P datagram transport and stream-hop composition | `connect` |
| H1/H3 platform carrier batching | `connect` |
| Resident/exchange batching and future platform datagram routing | `server/connect` |

Selecting this compact prototype later would still require agreed generation
and receiver-index lifecycle, exporter handoff, a raw UDP/ICE sidecar,
cumulative receipt serialization, pacing and congestion safeguards, PMTU,
authenticated endpoint migration, and protocol review. The selected SRTP path
already implements capability negotiation, bounded queues, drop counters,
ICE migration, and legacy fallback without introducing that second lifecycle.

---

## Implementation ledger

### Phase 0 — trustworthy baseline method complete; final sample pending

- The four-route `server/connect` harness forces H1, H3, legacy P2P, and fast
  P2P through one measurement implementation.
- P2P carrier counters reject fallback or mixed-lane results. Unique packet
  indexes reject loss, duplication, and favorable miscounting.
- The opt-in comparison runs five repetitions and enforces a minimum 1.5x
  fast/legacy median gain.
- The recorded values are historical because their exact source patches and raw
  logs were not retained. An exact-tree rerun remains.
- Raw underlay, cross-host, and physical-device runs remain deployment gates.

### Phase 1 — semantics on the existing association: complete with SRTP

- SDP capability negotiation, mutual readiness, `Auto`, `LegacyOnly`, and
  `FastOnly` are production settings.
- Direct IP data disables duplicate Transfer retry when the inner sender owns
  recovery. Provider-return TCP, contract setup, and control remain reliable.
- Tests cover capable peers, one-sided old-peer fallback, forced lanes,
  malformed fragments, reordering, duplicates, expiry, bounded overflow, and
  caller-settings immutability.
- A zero-retransmit SCTP lane was not selected because it retained the measured
  SCTP syscall and fragmentation limits.

### Phase 2 — datagram prototypes and selection: complete

- The compact authenticated UDP codec covers replay, key separation,
  ownership, batches, accounting counters, and one-to-nine-hop composition.
- The production RTP/SRTP carrier covers real ICE, DTLS, SRTP, negotiation,
  fragmentation, bounded reassembly, and full-TUN one-, three-, five-, and
  nine-hop composition on the recorded pre-final tree. The exact-tree topology
  rerun remains.
- RTP/SRTP was selected because it exceeded the target while reusing the
  mature connection lifecycle and preserving automatic old-peer fallback.

### Phase 3 — route-neutral boundary batching: complete; generic pipeline partial

- Connect mux, DeviceLocal, SDK packed ABI, proxy, Apple, Windows, Android,
  Linux, H3 client, and H3 server changes are implemented and tested.
- Linux `sendmmsg` and platform-independent bounded ready drains are active.
- Singular APIs remain for compatibility, synthesized or mixed traffic, and
  policy/routing/Transfer stages that do not yet expose a generic batch API.
- `recvmmsg`, UDP GSO/GRO, parallel crypto, and resident-channel batches remain
  optional measured headroom.

### Phase 4 — composed route optimization: exact-tree performance pending

- Historical forced P2P and exchange route results are recorded above.
- Before the recovery-owner correction, the full TUN/gVisor/provider
  directional test was stable across five complete invocations after correcting
  the too-small fast receive burst window.
- The schema-1 and schema-2 PERFVAR baseline records the last full-TUN clean,
  high-RTT, extender, and MTU evidence. The exact-tree schema-3 measurement
  remains.
- All existing `connect/ip_security` and contract enforcement stays on the
  shared packet path.
- Cross-host exchange, physical NIC, and Internet-provider profiling remains.

### Phase 5 — quality and rollout: capability controls complete, field gates remain

- Capability-based automatic rollout, forced disable/require controls, ICE
  migration, bounded warmup, and legacy fallback are implemented.
- H3 DPLPMTUD is enabled. The fixed SRTP fragment payload is now 1,188 bytes,
  fitting IPv6's 1,280-byte minimum outer MTU; focused full-TUN 1,400/1,280
  tests pass with zero P2P MTU drops.
- The historical userspace 500 ms and 1 s matrices exposed route-readiness
  failures. Deterministic correctness gates now cover both latencies, including
  representative warmed 32 MiB transfers, but their exact-tree execution and
  schema-3 measurement confirmation remain pending. Continuous
  delivery-quality scoring, path-aware P2P MTU handling, field loss/RTT
  matrices, thermal tests, and canary telemetry remain release work.

---

## Deterministic validation matrix

### End-to-end integration performance tests

`server/connect` is the correct home for the composed route tests because it
can stand up real handlers, residents, exchange links, platform transports,
stream setup, and P2P negotiation in one controlled topology. The existing
suite provides useful pieces:

- `connect_perf_test.go` measures the real cross-exchange client path;
- `connect_multiclient_perf_test.go` and
  `connect_multiclient_tcp_directional_perf_test.go` include the TUN, NAT,
  provider, exchange, and app-injection boundaries;
- `connect_stream_p2p_test.go` proves that a standalone negotiated stream
  continues over real P2P after its platform transports close. PERFVAR's full
  TUN fixture instead retains platform control/receive and suppresses only the
  selected destination's payload aliases.

`stream_route_performance_test.go` now supplies four forced cases using the
same inner traffic generator and measurement code:

| Case | Implemented topology and assertion |
|---|---|
| Legacy P2P | Real stream setup and WebRTC/SCTP data; assert the legacy lane is selected |
| Fast P2P | Real stream setup and RTP/SRTP data; assert fast send/receive, zero legacy payload, zero fallback, and zero carrier drop |
| Exchange H1 | Real client H1 transports plus resident/exchange forwarding; assert no P2P route carried data |
| Exchange H3 | Real client H3 transports plus resident/exchange forwarding; force H3 rather than automatic mode selection |

Every measured route warms up, then performs five exact-delivery runs. The P2P
comparison fails below a 1.5x median gain. Recorded pre-final full-TUN tests
showed that the same negotiated fast carrier composed over one, three, five,
and nine adjacent hops. They verified exact bidirectional TCP content, positive
fast traffic on every physical hop, and zero fallback, carrier drops, or
non-adjacent shortcuts. The current exact tree still needs the same run. The
historical full-stack directional TCP test separately included the TUN, gVisor
NAT, and provider boundaries and was repeated five times for flakiness before
the recovery-owner correction. Keeping route forcing and physical TUN
composition as distinct tests makes carrier attribution deterministic without
calling a codec loopback an end-to-end result.

The tests use production controls rather than test-only packet relays:

- `LegacyOnly`, `FastOnly`, and `Auto` P2P selection;
- forced H1 and H3 platform selection;
- observable message, byte, fragment, fallback, and drop counters on P2P;
- a legacy-versus-fast comparison in one result with identical payload,
  topology, MTU, contracts, and underlay.

`FastOnly` is wired into the production P2P transport. The compact UDP codec
benchmark remains clearly separate and cannot satisfy the route gate.

### Remaining field-validation conditions

The local deterministic suite does not emulate this entire matrix. Run these
before making a product-wide gigabit or loss-behavior claim:

| Dimension | Values |
|---|---|
| ordinary RTT | 0, 10, 25, 50, 100 ms |
| single-region access RTT | 500 and 1,000 ms |
| independent loss | 0%, 0.01%, 0.1%, 1% |
| path rate | 100, 300, 1,000, 2,500 Mbit/s |
| queue | shallow and deep |
| inner TUN MTU | 1,280, 1,400, and 1,440 bytes |
| outer path MTU | 1,280, 1,400, and 1,500 bytes over IPv4 and IPv6 |
| traffic | one, four, and sixteen TCP flows; QUIC; rate-limited UDP |
| direction | consumer → provider and provider → consumer |
| repetition | five fresh runs, report median and p95 |

Run on:

- Linux bare metal over Ethernet;
- macOS/Windows desktop over Ethernet and Wi-Fi 6E where available;
- physical current-generation iPhone and Pixel;
- same-LAN, NAT-to-NAT, IPv4, IPv6, network-change, and relay-fallback paths.

Run the server topology in both forms:

- one process/host for deterministic correctness and profiles;
- separate client, resident, and exchange processes or hosts for throughput,
  so loopback scheduling does not charge every hop to one machine or hide an
  internal exchange bottleneck.

### Remaining field measurements

- useful inner payload and outer wire throughput;
- packets and syscalls per second;
- packets per batch distribution;
- CPU total and per core;
- allocations, GC, and retained memory;
- TUN, queue, socket, replay, authentication, and kernel overflow drops;
- RTT, loss, receive window, congestion window, retransmissions where
  applicable;
- p50/p95 application latency under bulk load;
- device thermal state and battery cost;
- sender bytes versus authenticated receiver accounting receipts.

### Release gates

1. Every isolated hot stage sustains at least 200 MiB/s on the Linux reference
   machine.
2. The encrypted datagram transport sustains at least 150 MiB/s useful payload
   on a clean LAN.
3. The full tunnel sustains at least 119.2 MiB/s median useful payload when the
   measured raw underlay is at least 1.2 Gbit/s.
4. Bulk load does not materially regress p95 latency or create a standing
   queue relative to the available path.
5. Lossy and slow paths converge safely without retry storms, unbounded memory,
   or worse failure recovery than the legacy path.
6. Accounting is exact and monotonic under loss, reordering, duplication,
   replay, rekey, migration, receipt loss, contract rollover, and abrupt close.
7. Invalid authentication, replay-window, policy, IP-security, MTU, and
   anti-amplification cases have deterministic tests.
8. Old peers and browsers fall back without user-visible failure.
9. Forced P2P and forced exchange tests reproduce the selected carriers, and a
   separate full-stack test covers app/TUN/provider composition; no codec-only
   result can satisfy this gate.
10. Shared batching improves or preserves both P2P and exchange performance;
    a P2P gain may not regress forced H1/H3 goodput, latency, drops, or memory.

## Work that should not lead the project

- **Larger route queues.** Depths 1, 4, 8, and 32 measured the same range, while
  larger queues retain more memory and add queueing latency.
- **More DataChannels.** They share the SCTP association congestion window and
  write loop.
- **More fixed SCTP congestion-window tuning.** The repository already tested
  larger avoidance steps, beta changes, minimum windows, fast retransmit
  changes, and lower retry bounds. Aggressive settings traded improvement in
  one loss model for collapse or bufferbloat elsewhere.
- **Removing encryption.** Current AEAD throughput is well above the target.
- **One jumbo application UDP datagram.** It destroys packet independence and
  amplifies loss through IP fragmentation.
- **Decoder or protobuf micro-tuning alone.** Those paths are already heavily
  optimized and the composed profile is syscall-dominated.
- **Adding a second reliable protocol.** A new reliable stream with a larger
  window changes the implementation, not the nested-recovery problem.

## Existing improvements that must be preserved

The current code already contains important, measured corrections:

- egress-only ICE interface enumeration and Android path handling;
- selected-peer admission with a default 512 KiB SDK selected-peer window,
  128 KiB SDK public-peer window, and 512/256 KiB Connect default/reduced-memory
  window; a zero memory target keeps the Connect or explicit caller settings;
- live STUN endpoints and bounded gathering;
- reliable unordered DataChannel delivery;
- SCTP SNAP and negotiated zero checksum;
- the measured 8-MTU congestion-avoidance step;
- SCTP no-progress detection and network-change reconstruction;
- two-frame Transfer coalescing;
- pooled zero-copy decoding, sharded queues/timers/ACK state, and bounded
  ownership;
- gVisor TUN batch/GRO support;
- teardown, route recovery, and contract-accounting correctness work.

The selected P2P fast lane keeps these setup, security, lifecycle, memory, and
fallback properties in the design while replacing the limiting bulk-data
semantics. Recorded tests cover the invariants; the exact-tree suite still
needs to confirm them together.

## Final assessment

Confidence is:

- **High** that removing SCTP from capable native bulk traffic and preserving
  batches was the correct architectural fix: the historical comparable carrier
  route improved 3.20x and exceeded one decimal gigabit.
- **High** that queue-depth increases, additional DataChannels, and removing
  encryption will not solve the measured ceiling.
- **Medium** that the final composed userspace route has local desktop headroom.
  Historical carrier and directional samples reached the target, but the latest
  comprehensive full-TUN baseline measured 850.190 Mbit/s download and 282.826
  Mbit/s upload, and the post-fix schema-3 run remains pending.
- **Medium** that ordinary desktop/Linux physical deployments will sustain the
  target until cross-host NIC, NAT, RTT, loss, and underlay tests confirm it.
- **Conditional** for mobile gigabit because platform packet APIs, radios,
  thermals, and device CPUs vary substantially.

The remaining credible route to a product-wide claim is:

1. rerun clean topology, recovery ownership, and schema-3 full-TUN performance
   on one recorded exact tree;
2. run and confirm the deterministic 500 ms/1 s readiness gates, retaining the
   fixed 1,400/1,280-byte outer-MTU correctness gates in the resulting evidence;
3. retain `Auto` capability fallback and the reliable control plane;
4. validate Linux, Windows, macOS, Android, and iOS on physical same-LAN paths;
5. run the RTT, loss, MTU, migration, and relay matrix above;
6. measure cross-host exchange and provider paths independently;
7. add GSO/GRO, continuous quality scoring, or the compact codec only when a
   new profile identifies one of them as the next ceiling;
8. canary the fast lane with fallback/drop/accounting telemetry before broad
   rollout.

The implementation addresses the measured carrier bottleneck and retains the
properties that make Connect more than a generic packet tunnel. Exact-tree
full-TUN performance, high-RTT readiness, deployment validation, and measured
hardening remain. The minimum outer-MTU defect is resolved. The evidence does not call for
another wholesale data-plane redesign.
