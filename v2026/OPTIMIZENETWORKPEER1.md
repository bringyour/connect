# OPTIMIZENETWORKPEER1.md — optimizing the device-to-device (network peer) connection

A reproducible guide to the setup, methodology, root-cause chain, resolutions,
and measured limits from the "device-to-device / network-peer connection is
slow" investigation (2026-07-25). Companion to `PACKETRESEARCH1.md §16–§17`;
this file is the current standalone result.

## Current result

The original failure was not one slow operation. It was a sequence of
independent routing, admission, signaling, ownership, and instrumentation
problems:

| Layer | Finding | Resolution | Current status |
|---|---|---|---|
| ICE | Android API 30+ denies the netlink enumeration used by Go/Pion, producing zero host candidates | Supply a bounded default-route interface view through `SettingEngine.SetNet` | Fixed; direct candidate pairs form |
| Admission | WebRTC reservations shared the active transfer receive budget, so bulk traffic excluded P2P | Dedicated, hard-bounded WebRTC admission budget; selected peers can preempt speculative work without raising the ceiling | Fixed |
| Signaling | Active SDP used a plain contract while selected-peer data used `ForceStream` | Send active signaling over the same stream contract; retain companion signaling on the passive side | Fixed |
| Return route | Network-peer provider data could use a different send-sequence key from signaling/data | Use `ForceStream` consistently for Network returns | Fixed |
| Contract startup/rollover | A 16 KiB first contract stalled a cold page, provider replies could not classify ephemeral window-client IDs, and a prefetched successor expired after 120 s even while its current contract was live | Carry authenticated Network policy independently of `ForceStream`, use a 1 MiB first Network contract, and retain one bounded successor for the lifetime of the open contract | Fixed; both physical Android directions crossed 838,860 usable bytes with an immediate successor and no slow contract wait |
| Long-transfer reliability | Provider ping echo retained a callback-borrowed pooled `Frame`; reuse changed its type/size after enqueue and corrupted contract accounting | Give the asynchronous echo its own frame and shared bytes; derive accounting from the frames actually serialized | Fixed and regression-tested |
| CPU | A diagnostic `vmodule` made every disabled hot-path verbosity check resolve the caller stack | Remove `vmodule` from measured/release builds | Fixed; total process cycles −37.6%, Go-library cycles −44.7% |
| Receive flow control | The old 128 KiB selected-peer SCTP window capped controlled 50 ms paths | Use a selected-peer-only 2 MiB window with exactly two reservations (4 MiB ceiling) | Fixed; 2.26 → 28–29 MiB/s in the controlled path |
| Congestion recovery | Pion's Reno avoidance added only one ~1.2 KiB MTU per acknowledged window; independent loss held `cwnd` around 22–55 KiB | Use the repeatedly measured 8-MTU avoidance knee; retain stock 1/2 multiplicative decrease and no forced floor | Fixed; 8 MTU improved the cross-regime median without the 10–24-MTU overshoot risk; β=0.7 was measured and rejected |
| Network-peer rebuild | The 30 s admission-priority lease also selected the large window, so an idle/path-change rebuild could silently revert to 128 KiB; waiters always subscribed to the public budget | Remember authenticated Network identity in a separate bounded map, rebuild late-promoted public associations, and subscribe to the actual admission pool | Fixed and ordering/wakeup/expiry tested |
| Network-change setup pause | Path-bound ICE factory invalidation regenerated the manager's P-256 DTLS key/certificate | Rebuild ICE/API state but retain the manager-scoped certificate | Fixed; constructor 147 → 33.4 µs, allocations 580 → 318 |
| Idle data-plane liveness | Pion's infinite T3 retry assumes ICE will fail, but ICE consent can stay healthy while DTLS/SCTP is blackholed | Lazy, activity-triggered 10 s SCTP progress watchdog | Fixed; no idle timer, active sampling capped at 4 Hz |
| Asymmetric restart | A fresh active offer was ignored as a duplicate by the stale passive association | Mark the first offer with `reset_signals`; retire the negotiated passive generation and replay after `WaitingForSdpOffer` | Fixed; blackhole → bilateral replacement → resumed data is race-tested |
| Reconnect teardown | A net/http dial could outlive its request and the Tun; permanent and one-shot DoH could install h2/TLS connections after their idle pools were closed | Bind both dial layers to owner lifetimes; permanent caches and fastest-answer one-shot queries cancel and join requests/dials before closing pools | Fixed and lifecycle-tested |
| Platform write teardown | H1 and H3 canceled their writer context and then joined the writer before closing the WebSocket/QUIC connection; a kernel- or flow-control-blocked write cannot observe that context | Remove routes, cancel, close the underlying connection, then join and drain | Fixed; the old iOS trace retained a transport for ~9 s, while real H1/H3 blocked-write regressions complete inside a 500 ms bound (H3 typically ~45 ms locally) |
| Retired multi-client memory | Bounded transfer-sequence retention of a closed multi-client also retained its mux callback and DNS/server-name graph | Atomically detach future receive delivery and both lookup references on close | Fixed without weakening live callback backpressure |
| Transfer callback lock convoy | Send-sequence close and receive-generation replacement performed callback-related drain/wait work while holding a buffer-wide sequence-map lock | Publish/remove indexes under the map lock, then drain or wait outside it; keep the callback synchronous for its own transfer sequence/source | Fixed; unrelated transfer-map work stays live while send/receive callback backpressure remains intentional |
| ACK pool safety | Cumulative ACK processing read a `sendItem` after returning it to the process-wide pool | Snapshot sequence values before any pool return | Fixed; the former race is clean under `-race` |
| SCTP patch reliability | The release graphs still selected SCTP 1.11.0, which can leave the read loop parked after a one-sided write failure and has incomplete queued-data/graceful-shutdown handling | Upgrade WebRTC 4.2.18, ICE 4.4.0, and SCTP 1.11.1 in every local Connect/SDK/server/proxy and SDK artifact graph | Fixed; upstream closes the transport on write failure, validates SACK ranges before mutation, and correctly drains queued DATA through shutdown |
| SDK load/lifecycle tests | A raw-v2 helper double-returned accepted packets, a test-only state worker leaked, and absolute process-memory tests inherited global warm state | Correct ownership, close test state with its context, drain teardown, and run absolute-budget bodies in fresh test processes | All three formerly failing tests and the full SDK tree pass |
| Android physical-path discovery | A new `NetworkRequest` inherited Android 15's `NOT_METERED` default, excluding cellular; one-slot callback state also reported offline when either of two live paths disappeared | Clear defaults, request `INTERNET & NOT_RESTRICTED & TRUSTED & NOT_VPN`, register passive callbacks, and track every matching path | Fixed and confirmed on Pixel cellular (`offline=false`, exact capabilities in `dumpsys`) |
| Android live TUN reconfiguration | Always-on mode skipped material DNS/split/address changes and marked partial state applied before `Builder.establish()` succeeded | Compare complete immutable desired/applied snapshots and make-before-break every material change; commit only after successful establish | Fixed with seven deterministic configuration tests |
| Apple VPN reconciliation | A missing/late NetworkExtension preferences callback could strand the single-flight reconcile forever; an app-active notification was not a reliable scene-lifecycle owner | Bound every preferences operation, ignore late/duplicate callbacks, cap profile removal, validate start/stop health, and retry failed work from explicit scene activation | Fixed with 20 focused lifecycle tests; physical extension launch still requires one-time user VPN consent after reinstall |
| Local app↔extension RPC retry CPU | Every failed mTLS reconnect reparsed PEM/ASN.1/P-256 material; full jitter allowed millisecond retry clusters; an expired `Accept` reconnect object could hot-spin forever | Cache immutable TLS config, use strict local pacing, recreate pace per accept, preserve explicit wake bypass, and coalesce identical failure logs | Config step 30.6 µs/15.1 KiB/165 allocs → 0.10 µs/480 B/1 alloc; full SDK suite passes |

After the ownership/accounting correction, the same full-tunnel path that
reliably terminated near **5.8 MiB / 10 s** completed **444,444,444 bytes** and
**987,654,321 bytes**. Physical-device steady throughput is presently
**4.8–5.9 MiB/s**. After congestion tuning, three post-ramp samples were
**4.99, 5.10, and 5.41 MiB/s** (median 5.10), so the defensible physical
end-to-end multiplier remains approximately **1.0×**. The positive result is
loss recovery and predictability rather than a claimed aggregate speedup:
on-device sender `cwnd` expanded from the former 22–55 KiB band to an observed
18.8–156.7 KiB range while the composed tunnel remained limited elsewhere.

Do not multiply independent microbenchmark gains. The controlled receive-window
gain, constructor/setup gains, packet-copy gains, loss-tail improvement, and
encryption/decode gains exercise different limits and do not compose into one
end-to-end number.

---

## Final revalidation (2026-07-26)

This pass supersedes the earlier 16-MTU/β=0.7 intermediate selection. It
repeated the congestion matrices, added distributional latency measurement,
tested the real negotiated receive windows, and followed the network-peer
lifetime through late promotion, priority expiry, admission wait, and path
rebuild.

### F.1 Final congestion selection: 8-MTU additive step, stock β=1/2

The old harness placed one probe in the middle of an eight-MiB transfer. A
single lost probe could therefore turn one phase sample into the claimed
latency result. `measureSctpPath` now distributes 31 uniquely identified probes
through the transfer and reports nearest-rank p50, p95, and max. Thirty-one is
intentional: with 15 samples p95 is necessarily the maximum sample. The
idle-blackhole test also waits for the initial ICE/DTLS/SCTP association before
starting its tight data-plane deadline, so scheduler/handshake time is no
longer mislabeled as a resume stall.

Three directly paired repetitions at 50 ms RTT gave these median throughput
results (MiB/s):

| Matrix | CA step | 1% independent loss | 50 Mbps / 64 KiB queue | 8 Mbps / 32 KiB queue |
|---|---:|---:|---:|---:|
| 4/8/16 | 4 MTU | 0.49 | 2.13 | 0.74 |
| 4/8/16 | **8 MTU** | **0.57** | **3.85** | **0.76** |
| 4/8/16 | 16 MTU | 0.69 | 3.88 (one collapse) | 0.76 (one slow run) |
| 8/10/12 | **8 MTU** | **0.59** | 3.42 | **0.81** |
| 8/10/12 | 10 MTU | 0.60 | 3.43 | 0.75 (two collapses) |
| 8/10/12 | 12 MTU | 0.68 | **3.90** (one collapse) | 0.80 (one collapse) |
| 6/8 | 6 MTU | 0.53 | 3.53 (one collapse) | 0.73 |
| 6/8 | **8 MTU** | **0.57** | 2.50 (two collapses) | **0.74** |

The token-bucket RTO phase is noisy—the paired 6/8 row demonstrates why a
single run cannot select the setting. Across all nine 8-MTU runs, medians were
0.57 / 3.42 / 0.75 MiB/s. Eight MTU retains the best aggregate cross-regime
throughput; 10–16 MTU buy more only in the independent-loss row and produce
less predictable queued-link pauses. A nonzero `MinCwnd`, larger
`FastRtxWnd`, lower RTO maximum, and 24-MTU step remain rejected for the queue,
burst, retry-work, or pause costs recorded below.

`DefaultWebRtcSettings().SctpCwndCAStep` is therefore **`8 * 1200`**. This is a
deployment-specific direct-peer optimization, not a claim that Reno with an
eight-PMDCS step is CUBIC or generally Internet-safe congestion control. A
future CUBIC implementation must include CUBIC growth and its Reno-friendly
region; copying only CUBIC's β does not inherit that proof.

The β=0.7 experiment was repeated with the retained 8-MTU step:

| β | 1% loss median | 50 Mbps median | 8 Mbps median | 8 Mbps median SRTT |
|---:|---:|---:|---:|---:|
| **1/2 (stock, retained)** | 0.59 | **4.18** | 0.83 | 78.3 ms |
| 7/10 | **0.87 (+47%)** | 2.90 (**−31%**) | 0.87 | 93.8 ms |

β=0.7 collapsed to one MTU in two of three 50-Mbps runs versus one of three
with β=0.5 and added roughly 15.5 ms slow-link SRTT. It is rejected in favor
of predictable bottleneck behavior. The local SCTP fork, hook, settings, and
all `replace github.com/pion/sctp` directives were removed from the Connect/SDK
module graphs; production uses stock Pion SCTP v1.11.1 through WebRTC v4.2.18
and ICE v4.4.0.

### F.2 Network-peer window and lifetime fixes

The provider-side 128-KiB cap was real, but the first fix coupled large-window
selection to the 30-second *admission priority* lease. That introduced two
resume failures:

1. after an idle period or network change, a trusted peer whose priority lease
   expired rebuilt with the public 128-KiB API;
2. a peer refused by the dedicated 2-MiB budget subscribed to the public-budget
   notification and could sleep until the 30-second fallback even when its real
   pool released.

`WebRtcManager` now separates identity from priority:

- authenticated `ProvideMode_Network` identities live in a manager-scoped,
  hard-bounded 64-entry recency map; this map reserves no count or bytes;
- the 30-second priority lease still controls preemption, so an abandoned peer
  cannot pin a slot;
- a late Network signal against an existing public-window association raises
  persistent `ImmediateReconnect`, retires that association, reserves the
  pending priority slot, and rebuilds with the dedicated API;
- `AdmissionNotify(peerId)` subscribes to the pool that peer will actually use
  and Network promotion wakes a waiter so a public-pool subscription cannot be
  stranded.

Tests cover direct and authenticated-signal promotion, expiry followed by
rebuild, incomplete configuration fallback, exact pool wakeup, the ordering
where admission subscribes before Network authentication, hard-bounded
identity/priority maps (including newest-entry retention), and public/network
budget independence. An actual offer/answer/SCTP integration test verifies the
negotiated remote windows: network peers advertise within one control message
of 2 MiB while public peers advertise within one control message of 128 KiB.
SDK tests separately verify that provider transfer, public P2P, and
network-peer P2P pools are distinct and that the network pool is exactly two
2-MiB reservations.

### F.3 Reconnect setup, CPU, and adjacent leads

- **Certificate reuse.** A path change still invalidates and rebuilds the
  SettingEngine, API, and ICE state, but the P-256 DTLS certificate already
  scoped to the manager now survives that rebuild. On Apple M1 Max,
  factory+PeerConnection construction changed from ~147 µs, 51,012 B, 580
  allocations to ~33.4 µs, 33,465 B, 318 allocations: **77% less constructor
  latency, 34% fewer allocated bytes, and 45% fewer allocations**. A regression
  test asserts factory identity changes while certificate identity does not.
- **Measurement logger removed.** The temporary once-per-second SCTP/pair
  logger and its per-write verbosity check were removed after attribution.
  `noteOutboundSctpActivity` improved from ~2.90 to ~2.09 ns/op (zero
  allocations), and V(1) can no longer create one periodic logger/timer per
  active association. The progress watchdog remains lazy and transport-only.
- **Certificate teardown is bounded.** Network-change tests assert that the
  replacement factory reuses the same certificate; the manager-lifetime test
  separately asserts that final context cancellation clears both factory and
  certificate references. Reuse cannot turn into process-lifetime retention.
- **P2P message aggregation investigated, not shipped.** Detached local-route
  throughput rose from roughly 48 MiB/s with 3-KiB messages to 56–58 MiB/s with
  12–48-KiB messages. The physical peer path is only ~5 MiB/s, so serialization
  is not its present limiter. A P2P-only aggregate needs a negotiated
  length-prefixed envelope because the existing SCTP message boundary is the
  TransferFrame boundary; globally raising the 3-KiB batch would break
  resident/platform compatibility and enlarge bounded queues. This remains a
  versioned-protocol lead, not a safe local constant change.
- **No periodic observability retained.** Physical `[p2p-stats]` captured the
  IPv4 host/host pair, ~50-ms idle SRTT, load inflation toward ~100 ms, and
  `cwnd` rather than 2-MiB `rwnd` as the active limit. Production keeps only
  event-driven error/liveness logs.

The focused new ordinary tests passed 20 repetitions where timing/order
matters; idle-blackhole plus distribution measurement passed 10 repetitions;
the admission/promotion/hook replacements passed under `-race`; and the actual
window negotiation passed 20 repetitions. Full-tree validation is recorded in
the final validation section below.

### F.4 Provider availability and active tunnel ownership

Physical testing exposed two Android lifecycle errors that both came from
treating a callback as a single value instead of a changing set of resources.

First, `NetworkRequest.Builder()` is not an empty builder on current Android:
Android 15 adds `NET_CAPABILITY_NOT_METERED` by default. Adding only
`INTERNET` therefore still excluded cellular, which left `DeviceLocal.offline`
true and paused a provider that was otherwise configured to allow cellular.
The physical request now starts with `clearCapabilities()` and explicitly asks
for `INTERNET`, `NOT_RESTRICTED`, `TRUSTED`, and `NOT_VPN`. It uses
`registerNetworkCallback`, not an active `requestNetwork` acquisition, and
tracks all matching networks. Losing Wi-Fi while cellular remains no longer
reports the device offline; a material link-properties change or physical-path
handoff calls `NetworkChanged()` immediately. Eleven pure tracker tests and
four Android capability tests cover duplicate, out-of-order, multi-path,
fingerprint, metered, constrained, and VPN-exclusion behavior. On the Pixel 8
Pro, `dumpsys connectivity` showed the exact requested capability set and the
live LTE path produced `offline=false`.

Second, the VPN service compared only portions of the packet-flow state and
special-cased always-on mode by refusing to rebuild an active descriptor. DNS,
app split, offline/connected state, or the tunnel-local address could therefore
remain stale indefinitely. `VpnPacketFlowConfiguration` is now one immutable
snapshot of every material builder input. Reconciliation establishes the
replacement descriptor before closing the old `PacketFlow`, and records the
snapshot only after `Builder.establish()` succeeds. A failed handover retains
the old working interface and leaves the new state unapplied so the next
listener retries it. Notification foreground policy changes in place and is
orthogonal to TUN lifetime.

The same ownership audit found that peer signaling was still sent with the
manager lifetime instead of the individual peer-generation context. A retired
generation could consequently continue a signal send, and cancellation could
wait behind the synchronous packet callback. Signals now carry generation
cancellation; a separate teardown watcher closes the PeerConnection,
association, and data channel and releases admission without waiting for an
accepted send/receive/forward callback. Those three callbacks remain
intentional data backpressure. The cancellation/resource suite passes ordinary
and race-enabled repetitions.

### F.5 Apple reconciliation and local RPC retry work

The iPhone physical driver found a separate control-plane hang before packet
flow: NetworkExtension preference APIs are callback-based, and a callback can
be delayed behind user authorization or fail to arrive. The old single-flight
flag then remained set forever, so app foregrounding could not retry.

Every load/save/remove preference operation now has a 30-second generation
bound. Completion is exactly once; late and duplicate callbacks are ignored.
An operation that timed out cannot be retried concurrently, profile removal is
capped at 32 candidates, both start and stop paths run a bounded health check,
and logout/quit cancels reconciliation.

Physical foregrounding also exposed an ownership gap in the first retry: a
process-level active notification did not reliably correspond to the active
SwiftUI scene. Both iOS and macOS now forward explicit `scenePhase == .active`
transitions through `DeviceManager` to the VPN reconciler. A failed generation
queues one forced retry even if that generation is completing in the same main
queue turn; a healthy in-flight generation still deduplicates the event. Twenty
focused tests cover missing, late, duplicate, error, health-reset,
finite-removal, desired-state, and every foreground decision case. The exact
macOS 13 / iOS 16-compatible source builds, and the complete Apple test target
passes 24/24. This does not bypass iOS security: after reinstall, a user must
still approve the system VPN sheet before the extension can launch.

A direct on-device Time Profiler trace of that unavailable-extension state
then identified avoidable retry work in the Go RPC client:

- the immutable local mTLS identity and server pin were parsed on every
  reconnect. Caching the `tls.Config` once changes that configuration step from
  about **30.6 µs, 15.1 KiB, and 165 allocations** to the per-handshake clone at
  about **0.10 µs, 480 B, and one allocation**: roughly **305× faster, 96.8%
  fewer bytes, and 99.4% fewer allocations** for this step;
- global full jitter is appropriate for remote client herds but not a local
  app↔extension loop. A new fixed `NewPacedReconnect` keeps a true minimum
  interval. The local default changed from random `[0,1 s)` to fixed 500 ms,
  retaining the same average two attempts/second while bounding passive
  detection at 500 ms. Explicit `Sync` and transport replacement still wake
  immediately;
- the local listener created one reconnect deadline outside its `Accept` loop.
  Once that deadline elapsed, every later persistent accept failure observed
  the already-closed channel and could peg a core. The pace is now constructed
  for each attempt; and
- identical dial and accept failures log once per failure streak, clearing
  after success. Attacker/request-driven HTTP proxy retries use the same strict
  one-second floor, while remote transport/server retries keep full jitter.

The latest unavailable-extension trace used **0.1719 CPU-seconds over 16.19
seconds (1.06% of one core)**. That is about 4.4% below the immediately prior
short trace, but the samples are too noisy to claim an end-to-end CPU speedup;
the defensible result is bounded retry cadence, bounded log volume, and removal
of the latent accept hot-spin. The pacing/log/TLS tests pass repeatedly and
under `-race`; the complete SDK tree passed in 382.503 seconds.

### F.6 Physical cross-platform first-load matrix

The Debug-only iOS driver uses an ephemeral `WKWebsiteDataStore`, cache-bypass
navigation, and a 75-second bound. It records navigation DNS, connect, TLS,
TTFB, response end, DOM-content-loaded and load time plus resource/origin
counts, aggregate resource DNS, bytes, and maximum request concurrency. This
models the real cold multi-origin page rather than a single warmed request.

The first iPhone→Pixel connection before the current reinstall reached
`CONNECTED` in about **0.7 seconds**, and Pixel ingress TCP/443 and UDP/443
counters increased, proving the selected peer carried traffic. Suspending the
containing iPhone app for roughly three minutes stopped UI polling completely;
the packet-tunnel connection remained up, and foreground RPC returned in about
25 ms with state still `CONNECTED`.

The requested final iPhone→Android and Android→iPhone cold-page/idle matrix is
kept as a release gate until the freshly installed iOS build receives the
one-time “Add VPN Configurations” approval. A timed-out system callback is now
reported and safely retryable instead of hanging, but no benchmark obtained
without a running packet-tunnel extension is labeled as network-peer data.

The connected Apple endpoint for that matrix is an iPhone 16 Pro Max
(`iPhone17,2`), UDID `00008140-001679DE0893C01C`. The current signed Debug build
passes its physical-device build and install gates; only the system-owned VPN
authorization remains before bidirectional traffic measurement.

### F.7 Final write-teardown, callback-locality, and SCTP patch pass

The old iOS extension trace contained a lifecycle interval that was initially
misclassified as ordinary network delay. Logical client
`019fa887-320d…` was removed at 11:42:07, but its old platform TCP batch write
did not return until 11:42:16. The H1 teardown order was:

```text
remove routes → cancel handle context → wait for writer → close WebSocket
```

A `net.Conn.Write` already blocked in the kernel does not observe the handle
context. The only operation that could release the writer was therefore
deferred until after the join waiting for that writer: a close-before-join
dependency inversion. The H3 path had the adjacent problem when a peer stopped
reading and QUIC stream flow control parked `Stream.Write`.

Both paths now remove their routes, cancel, close the underlying
WebSocket/QUIC connection, join the writer, and then drain any final bounded
route messages. The H1 regression uses a real WebSocket with a connection
whose write is released only by `Close`. The H3 regression uses a real local
QUIC endpoint with fixed receive credit and deliberately stops reading until
the client's bounded route fills. Both tests passed ten race-enabled
repetitions. The regression bound is 500 ms; the H3 case normally closes in
about 45 ms locally. This changes the observed worst teardown from roughly
nine seconds to a deterministic local bound in the harness—at least an
18-fold reduction in that teardown tail—without changing steady-state
throughput.

The same wait-graph audit found two transfer-buffer lock convoys:

- `SendBuffer` drained a closing sequence while holding its buffer-wide map
  lock. Draining invokes send-completion callbacks synchronously, so an
  intentionally stalled callback for one sequence could block unrelated
  destinations from looking up or creating sequence state.
- `ReceiveBuffer.Pack` canceled an older generation and waited for its worker
  to exit while holding the buffer-wide map lock. A receive callback is
  intentional backpressure and must order replacement for that source, but it
  must not own every source's sequence map while it is parked.

Send indexes are now detached atomically under the map lock and the sequence is
closed afterward. Receive replacement cancels under the lock, unlocks while
waiting for the old worker/callback, then reacquires and conditionally removes
only that exact generation. A stale head index is healed, and completion can no
longer delete a newer head. Forward cleanup received the same close-outside-lock
hygiene. The callbacks themselves remain synchronous: the send close still
waits for its completion callback, same-source receive replacement still waits,
and the serial receive path is not converted into an asynchronous dispatcher.
Only the accidental global lock convoy was removed.
`TestSendSequenceCloseCallbackBackpressureIsDestinationLocal` and
`TestReceiveSequenceReplacementBackpressureIsSourceLocal` passed 20 ordinary
and 20 race-enabled repetitions, together with the adjacent sequence-fork and
rejected-contract cases.

The production dependency graph also moved from Pion WebRTC 4.2.17 / ICE 4.3.0
/ SCTP 1.11.0 to WebRTC 4.2.18 / ICE 4.4.0 / SCTP 1.11.1. SCTP 1.11.1 closes
the transport after a one-sided write failure so its read loop cannot remain
parked, validates cumulative/gap SACK ranges before mutating acknowledgement
state, and completes queued-DATA, partial-acknowledgement, crossed-shutdown, and
graceful-shutdown transitions. Those are adjacent reliability fixes for the
same idle/resume and teardown failure class; they do not justify a new speed
multiplier.

The main SDK graph had been updated, but its independent `build`, `cgo`, and
`js` release modules still selected the old Pion patches. All artifact graphs
and the local server/proxy/validator consumers are now aligned. The new
top-level `TestSdkArtifactModulePionVersionsMatchRoot` compares every Pion
module version in each SDK artifact graph with the SDK root and passed 20
ordinary plus 20 race-enabled repetitions.

Finally, the opt-in CA-step sweep was rerun against the upgraded graph. At 1%
independent loss, 8 MTU delivered 0.60 MiB/s with 52.8/97.6 ms p50/p95 probe
latency; 10–24 MTU bought 0.62–0.82 MiB/s but retained worse queue-risk
behavior. On the 50-Mbps shallow queue, one-shot results ranged from 2.47
MiB/s at 8 MTU to 4.57 MiB/s at 24 MTU, while the 8-Mbps shallow queue showed
the higher steps producing 0.7–1.0 s tails. Adding a 32-KiB minimum congestion
window improved the exogenous-loss row but forces data after real congestion.
The run confirms the existing repeated-matrix decision: retain the 8-MTU
additive step, stock one-half decrease, and no forced floor. No production
constant changed in this pass.

---

## 1. Hardware / accounts under test

Current authorized physical endpoints are:

| Role | Model | Explicit identifier | Status |
|------|-------|---------------------|--------|
| Android | Pixel 8 Pro (`husky`) | `3B161FDJG001KT` | Authorized; every command must use `adb -s 3B161FDJG001KT` |
| Android | Samsung S24 Ultra (`SM-S928U1`) | `R5CX21FY6ND` | Authorized for the 2026-07-28 bidirectional contract/page matrix; every command must use `adb -s R5CX21FY6ND` |
| Apple | iPhone 16 Pro Max (`iPhone17,2`) | `00008140-001679DE0893C01C` | Authorized when physically connected and available for required taps |

Do not use an unscoped `adb` discovery or mutation command. Address only the
explicit device under test:

```bash
adb -s 3B161FDJG001KT get-state
adb -s R5CX21FY6ND get-state
```

Notes that matter for reproduction:
- The two phones are on **different /24 subnets** (192.168.1.x vs 192.168.2.x) —
  a double-NAT home setup — yet are mutually routable (~8 ms). Direct UDP
  between them works (see §4.3), so the topology *supports* p2p; the failures
  below are in the app, not the network.
- The client selects the provider via the **Network peers** section of the
  location picker (not Auto). "Connected to 1 provider" in the UI means the
  single-peer path is active; "Auto" / "N providers" is the multi-provider mesh
  and is **not** a valid comparison for peer measurements (more parallelism
  inflates throughput).
- Post-quantum encryption is ON for these tests (the provider always enables
  the e2e sessions; the client opts in via the performance profile).

Platform relay (for reference RTT): `connect.bringyour.com` → `65.49.70.85`,
~59 ms one-way from the dev Mac. The phones cannot ICMP it but their data path
rides it when p2p is down.

---

## 2. Repositories and build/deploy pipeline

Four local repos under `/Users/brien/urnetwork/`:
- `connect/` — the Go transfer/IP/transport/p2p core (imported by the SDK).
- `sdk/` — gomobile SDK (`device_local*.go`), builds the Android AAR / Apple xcframework.
- `android/` — the Android app (`app/app/src/main/java/com/bringyour/network/…`).
- `server/` — platform exchange/resident (only read here, not changed).

The SDK is wired into the app via a gradle task that rebuilds it from local Go
source, so a connect/sdk edit propagates to the APK on the next build.

### 2.1 Build a play-variant APK (includes rebuilding the SDK from Go source)

```bash
cd /Users/brien/urnetwork/android/app
./gradlew :app:buildSdk :app:assemblePlayRelease --console=plain
# buildSdk runs `make init build_android` in sdk/build (gomobile bind) against
# the local connect/ + sdk/ source, dropping URnetworkSdk.aar into
# sdk/build/android/, which app/app/build.gradle consumes.
```

Output APKs land in
`android/app/app/build/outputs/apk/play/release/`. Use the **arm64-v8a** one for
these phones:
`com.bringyour.network-<version>-play-arm64-v8a-release.apk`.

The `play` flavor is signed with the release key
(`$WARP_HOME/release/android/signing/app.jks`, secrets in `app.properties`),
which is present locally — so `install -r` over the store build keeps app data
(the signature matches). Build time is ~2 min (SDK bind ~80 s + APK ~50 s).

### 2.2 Install and relaunch on the authorized Pixel

```bash
APK=$(ls -t /Users/brien/urnetwork/android/app/app/build/outputs/apk/play/release/*arm64*.apk | head -1)
adb -s 3B161FDJG001KT install -r "$APK"
adb -s 3B161FDJG001KT shell monkey \
  -p com.bringyour.network -c android.intent.category.LAUNCHER 1
```

`adb install -r` does NOT restart a running app. To guarantee the new build is
live, force-stop the explicitly addressed Pixel first, then relaunch. A
force-stop drops peer selection back to Auto, so re-select the network peer
(see §4.4) before measuring.

### 2.3 Apple (for parity; not the focus here)

```bash
cd /Users/brien/urnetwork/sdk/build && make build_apple   # rebuilds the xcframework
# then xcodebuild -project app/app.xcodeproj -scheme URnetwork \
#   -sdk iphonesimulator -destination 'generic/platform=iOS Simulator' ARCHS=arm64 build
```
IMPORTANT: never run `make build_apple` and gradle `buildSdk` concurrently —
they share `sdk/build/` and the concurrent xcframework swap corrupts the
binary target ("does not contain a binary artifact").

---

## 3. Instrumentation: turning on the logs you need

All diagnostics come through Android `logcat` tag **`GoLog`** (the SDK routes
glog to logcat). Verbosity is set in `sdk/sdk.go initGlog()`. Default and
performance builds must use `v=0` with no `vmodule`.

For short correctness-only captures, an instrumented build may use:

```go
// sdk/sdk.go initGlog() — TEMPORARY; never use for performance numbers
flag.Set("v", "1")
// Use only for a brief signaling trace:
flag.Set("vmodule", "transport_p2p_webrtc=2,transport_p2p=2,transfer_stream_manager=2")
```

- `v=1` surfaces: `[signal]send/receive`, `[p2p]…setup refused`, `[sm]…open`,
  `routing ok`, `[init]tcp connect synthetic`, `[r]drop older sequence`
  (elevated to V1 during this work as a fork tripwire), the pion
  `signaling state changed to …` lines.
- `vmodule transport_p2p_webrtc=2` adds the pion ICE candidate/pair/nomination
  trace and `[signal]miss` / `[signal]<SignalType>` routing lines.
- For throughput or CPU runs, use `v=0` with **no vmodule**.

This distinction is measured, not cosmetic. A 30-second `simpleperf` capture
with the diagnostic `vmodule` recorded **33.593 billion cycles**, 90.53% in
`libgojni`, with `runtime.pcvalue`, `runtime.step`, and related stack-walking
functions dominating. The matched build without `vmodule` recorded **20.956
billion cycles**, 80.32% in `libgojni`: **37.6% fewer total cycles** and
**44.7% fewer Go-library cycles** at comparable throughput. A disabled V-log
in a file selected by `vmodule` is not free; glog resolves its call site before
deciding whether to emit.

The low-frequency `[p2p-stats]` logger used to attribute the congestion limit
sampled SCTP/ICE every three seconds only while byte counters advanced. It was
removed after the A/B, along with Android's temporary `debuggable` /
`profileable` manifest hooks. Reintroduce it only for a bounded measurement;
it is not production observability.

Rebuild + redeploy after changing verbosity (it is compiled in).

Capture the authorized Pixel log:

```bash
adb -s 3B161FDJG001KT logcat -v threadtime -s GoLog > pixel.log &
# clear first with: adb -s 3B161FDJG001KT logcat -c
```

---

## 4. Measurement toolkit

### 4.1 Synthetic in-tunnel speed server (built for this work)

To drive a repeatable "packet rush" isolated from origin/website variability, the
**provider's local NAT terminates TCP flows to the RFC 2544 benchmark range
`198.18.0.0/15`** at an in-memory HTTP/1.1 server instead of dialing upstream.
The full tunnel path (client tun → per-peer encryption → transfer sequences →
transports → provider NAT) is exercised; only the internet hop is replaced. The
range is reserved and never publicly routable, so nothing real is shadowed.

- Code: `connect/ip_synthetic_speed.go`; hook in `connect/ip.go` TCP connect
  path; setting `TcpBufferSettings.EnableSyntheticSpeed` (default **true**).
- Endpoints (drive from the client, over the peer tunnel):
  - `GET http://198.18.0.1/ping` → 200, 1-byte body (flow-setup / RTT probe).
  - `GET http://198.18.0.1/download/<bytes>` → streams exactly `<bytes>`
    patterned bytes (compression-proof), capped at 10 GiB.
  - `POST http://198.18.0.1/…` with a body → upload sink, replies with the count.
- Drive a URL from the client with:
  ```bash
  adb -s 3B161FDJG001KT shell "am start -a android.intent.action.VIEW -d 'http://198.18.0.1/download/500000000'"
  ```
- Confirm it lands on the explicitly selected provider using that provider's
  own log/diagnostic surface.

### 4.2 Throughput measurement (tun byte deltas on the client)

The VPN tun on the client is **`tun1`** (not tun0). Measure received-byte delta
over a fixed window during a sustained download; that is the app-visible tunnel
throughput.

```bash
read_rx(){ adb -s 3B161FDJG001KT shell "cat /proc/net/dev" | grep -E "tun1:" | sed 's/.*tun1: *//' | awk '{print $1}'; }
adb -s 3B161FDJG001KT shell "am start -a android.intent.action.VIEW -d 'http://198.18.0.1/download/800000000'"
sleep 3; R1=$(read_rx); T1=$(date +%s.%N)
sleep 8; R2=$(read_rx); T2=$(date +%s.%N)
python3 -c "print(f'{($R2-$R1)/($T2-$T1)/1024/1024:.2f} MiB/s')"
```
Take 3 samples; discard the first partial ramp. Keep the peer selection fixed
(single network peer) across baseline and post-fix runs, or the comparison is
meaningless.

### 4.3 Direct-reachability sanity (proves topology supports p2p)

The two-Android run measured roughly 8 ms and proved raw UDP in
both directions, while the app initially gathered no ICE candidates. That
isolated the app's tunnel-trapped sockets (§5.1) from topology. For every
current matrix, use the app's selected ICE candidate pair and bidirectional
traffic counters; those exercise the exact sockets under test without adding a
separate device-control dependency.

### 4.4 Selecting the network peer via UI automation

After a fresh launch the client may be in Auto. To pin one currently
authorized network peer:

```bash
# dump the current screen's tappable text + bounds:
adb -s 3B161FDJG001KT exec-out uiautomator dump /dev/tty | \
  python3 -c 'import sys,re;[print(repr(m.group(1)),m.group(2)) for m in re.finditer(r"text=\"([^\"]{1,40})\"[^>]*bounds=\"(\[[0-9,\[\]]+\])\"",sys.stdin.read()) if m.group(1).strip()]'
# tap "Change" (center of its bounds), then the intended current
# "Network peers" row.
adb -s 3B161FDJG001KT shell input tap <cx> <cy>
```
Verify with a UI dump showing "Connected to 1 provider" and with `routing ok`
for the peer selected in the current session. Never reuse a historical peer id
as a device-control target.

### 4.5 Database state (optional, server side)

`server/monitor/SIGNALS.md` documents the pg/redis probes. pg primary is reached
over the overlay: `ssh by@172.28.208.182` then `psql` (creds in
`vault/main/pg.yml`). Not required for the client/provider-side work here.

---

## 5. Root-cause chain (why p2p never connects) and the fixes

Diagnosed by deploying the instrumented build, driving a synthetic download over
the peer, and reading the two logs. Symptoms in order of discovery, each gating
the next:

### 5.1 pion cannot enumerate interfaces on Android (netlink denied) — THE load-bearing blocker

**Symptom:** pion logs `Failed to initialize mDNS … no usable interfaces found
for mDNS`, gathers **0 host candidates**, and repeats `Pinging all candidates`
→ `Failed to ping without candidate pairs. Connection is not possible yet.`
`signaling state` reaches `stable` (after the §5.4 fix) but ICE never forms a
pair, so `ready header … err = i/o timeout` loops forever and traffic stays on
the relay.

**Root cause (definitive, via the `[ice-if]` diagnostic in
`transport_p2p_webrtc_pc.go`):**
```
[ice-if]net.Interfaces err = route ip+net: netlinkrib: permission denied
```
Since **Android 11 (API 30) apps are denied the netlink route dump**, so Go's
`net.Interfaces()` — and therefore pion's default host-candidate gathering —
fails outright. With no interface, pion builds no host candidate; ICE then has
nothing local to pair, so it never connects. This has nothing to do with
`VpnService.protect` and is **not** fixed by app exclusion.

**What this is NOT (ruled out on-device):**
- *Not* app-in-its-own-tunnel. The app **is** excluded: on the client the VPN
  network (`tun1`) covers `Uids: <{0-10338, 10340-20338, 20340-99999}>` and the
  app's UID is `10339` — outside the ranges, i.e. excluded. Its sockets already
  egress wlan0 (which is why the §4.3 dial trick below can find the real IP).
- *Not* socket protect. The sockets route fine; pion just can't see the
  interface to name the candidate.

**Fix (connect, `ice_net.go` + one hook in
`newWebRtcPeerConnectionFactory`):** supply pion a `transport.Net` that provides
the egress interface **without** netlink. The local egress address is found with
a connect()-only UDP "dial trick" (`net.Dial("udp","8.8.8.8:80")` →
`LocalAddr()`), which consults the in-kernel routing table, not netlink, and is
surfaced as one synthetic interface. Socket ops delegate to the embedded
`stdnet.Net`. Because the app is already excluded from the VPN, the dial trick
returns the physical wlan0/cellular IP — exactly the host candidate ICE needs.
Installed via `SettingEngine.SetNet(iceNet)`; a no-op on desktop/server where
`net.Interfaces()` works.

**Verify on-device** after redeploy:
```bash
adb -s 3B161FDJG001KT logcat -d -s GoLog | grep "\[ice-if\]"   # synthetic en0 addrs=[192.168.1.217/32]
adb -s 3B161FDJG001KT logcat -d -s GoLog | grep -iE "state changed: connected|valid candidate pair|pion:sctp.*sending ppi"
```

**Measured outcome (2026-07-25):** the fix works at the ICE/data layer.
`[ice-if]synthetic en0 addrs=[192.168.1.217/32]` (and IPv6), pion now gathers
host candidates, ICE reaches `connection state: connected` with a
nominated/succeeded candidate pair over **direct IPv6** (the two devices'
addresses are in adjacent /64s, c100↔c101 — no NAT), and SCTP carries data
(`sending ppi=53`, `SACK measured-rtt≈50–74ms`). This is a capability that was
completely absent before (0 candidates, 100% relay).

**Subsequent result:** the apparent architectural churn was the combined
admission/signaling/return-route failure chain, not a requirement to redesign
the window lifecycle. After those fixes, the selected direct association
remains installed through long synthetic transfers. The full-tunnel path
completed 444,444,444- and 987,654,321-byte runs, and the direct sender's SCTP
byte counters advance continuously during load. Sections 5.5–5.6 cover the
remaining measured congestion and idle-resume work.

**Related hardening (kept, but NOT the blocker here):**
`MainService.kt updatePfd()` allowlist mode excludes the app only by omission.
Guard it so the app is never in its own tunnel regardless of a bad split
override:
```kotlin
for (includedPackageName in tunnelIncludedAppIds) {
    if (includedPackageName == packageName) continue  // never tunnel this app
    builder.addAllowedApplication(includedPackageName)
}
```
On the tested devices the denylist path already excluded the app, so this guard
did not change their behavior — it prevents a *different* way to reintroduce the
loop.

### 5.2 p2p peer-connection memory-budget starvation — FIXED

**Symptom (v=1):** `[p2p]s(…) <>… setup refused = peer connection memory budget
exhausted (149796 < 524288)` on both devices; `signaling state` counts high but
**0 `stable`**; every window client churns.

**Cause:** the WebRTC admission budget (`WebRtcSettings.MemoryBudget`) was the
**same object** as the transfer receive-queue budget, at all four wiring sites
(`sdk/device_local.go` default+override+window-generator, and
`sdk/device_local_provider.go`). Each peer connection reserves
`ReceiveBufferSize` = 512 KiB. On a 20 MiB device target the provider share is
4 MiB → receive-queue budget ~1.14 MiB, and an active download consumes it
(available fell to ~146 KiB), so a 512 KiB reservation could **never** be
admitted while traffic flowed — exactly when p2p is needed. Catch-22: no p2p →
all relay → receive queue busy → p2p refused.

**Fix (`sdk`):**
- `deviceLocalWebRtcBudget(share)` — a **dedicated** admission budget, separate
  from the receive queue, sized `max(share/8, 8×128 KiB)` for automatic/public
  windows. It gates admission only; a formed connection's SCTP memory is
  unchanged, so no steady-state footprint is added.
- Automatic/public mobile windows use a 128 KiB per-connection buffer (was
  512 KiB), allowing at least eight bounded speculative peers. An explicitly
  selected network peer instead uses the measured 2 MiB receive window and a
  destination-local budget of exactly two reservations: one live association
  plus one make-before-break replacement, with a hard 4 MiB ceiling.
- Applied at all four sites, guarded to mobile (`0 < share`); desktop/server
  keep the 512 KiB unbudgeted default.
- Regression test: `TestDeviceLocalSettingsMemoryTarget` in
  `sdk/device_local_memory_test.go` (asserts the p2p budget ≠ receive queue and
  admits ≥2 connections).

**Measured:** relay-path single-peer throughput **0.54 → 0.79 MiB/s (+46 %)**
from reduced memory pressure (p2p still relayed until §5.1).

### 5.3 Dead STUN servers — FIXED

**Symptom:** a storm of `[pion:ice]failed to get server reflexive address …
stun:openrelay.metered.ca / stun.stunprotocol.org … i/o timeout`, each burning
multi-second timeouts and delaying gathering.

**Fix (`connect/transport_p2p_webrtc.go` `DefaultWebRtcSettings`):** replace the
defunct servers with live anycast ones:
```go
IceServerUrls: []string{
    "stun:stun.cloudflare.com:3478",
    "stun:stun.l.google.com:19302",
},
```
On-device STUN errors dropped from a storm to ~1.

### 5.4 SDP rendezvous: the active offer must ride the stream — FIXED

**Symptom (vmodule=2):** both devices only ever reach `have-local-offer`,
**never `have-remote-offer`**; the provider receives 0 `[signal]receive from
<client-main>`; many `[signal]miss`. The offer is *sent* (0 `send failed`) but
never delivered to the passive peer.

**Cause:** the provider spins up an **ephemeral per-window client** for each of
the client's window clients (e.g. `019f9a39-ee08…`), which contracts *to* the
client. Network peers force `AllowDirect` → the data path uses `ForceStream`. But
`ClientSignalSender.SendSignal` sent the **active** side's `SdpOffer` with the
client's **default** transfer options (plain contract, no ForceStream) — and the
ephemeral peer grants only the *stream* contract for the pair, so the offer was
undeliverable even though data flowed over the same stream via the relay. Same
class as `PACKETRESEARCH1 §16`: a send-sequence-keying option (ForceStream)
invisible to the signaling layer routes the offer onto a different, undeliverable
sequence.

**Fix (`connect/transport_p2p_webrtc.go` `peerConn.sendSignal`):** the active
side sends with `ForceStream()` so the offer rides the same stream contract as
the data (passive side keeps `CompanionContract()` — the platform rejects
companion *stream* contracts, so it must not also force stream):
```go
var opts []any
if self.active {
    opts = append(opts, ForceStream())
} else {
    opts = append(opts, CompanionContract())
}
```
**Measured on-device:** `have-remote-offer` and `stable` went **0 → 2** — SDP
offer/answer now completes. Candidate gathering then required the Android
interface fix in §5.1. Validated by the full `connect` suite and `TestWebRtc`.

### 5.5 SCTP congestion-window recovery — FIXED, 8-MTU KNEE RETAINED

**Physical attribution:** with the selected peer's advertised receive window at
roughly 2 MiB, the sender still had queued data while `cwnd` repeatedly fell
into the 22–55 KiB range. The receive window was no longer limiting. Pion uses
Reno-style avoidance and, by default, adds only one path MTU (1191 bytes on the
devices) after a complete congestion window is acknowledged. Recovery from an
independent wireless loss therefore takes seconds.

`transport_p2p_webrtc_loss_test.go` supplies two controlled paths:

- deterministic sender→receiver DTLS application-packet loss over a 50 ms RTT,
  while reverse SACK and ICE traffic remain intact;
- 5/8/20/50 Mbps token-bucket bottlenecks with bounded queues, which
  rejects settings that win only by forcing excess data into a real queue.

The original pass retained four MTUs. The final revalidation above expanded the
sweep to 4/6/8/10/12/16/24 MTU, repeated directly paired matrices, and replaced
the phase-sensitive single latency probe with a 31-sample p50/p95/max
distribution. The resulting setting is:

```go
SctpCwndCAStep: 8 * 1200
SctpMinCwnd:    0
SctpFastRtxWnd: 0
```

Eight MTUs improves aggregate cross-regime throughput while retaining Pion's
stock 1/2 multiplicative response and permitting `cwnd` to fall to the protocol
minimum. Larger fixed steps gain more under exogenous loss but cross the
predictability knee on shallow-queue 8/50-Mbps paths.

**Rejected after measurement:**

- 10/12/16/24-MTU steps: higher independent-loss goodput, but more queued-link
  RTO collapse or slow-run variance.
- β=7/10: +47% median at 1% independent loss, but −31% median on the
  50-Mbps queue, more one-MTU collapses, and +15.5 ms median SRTT at 8 Mbps.
  The experimental SCTP fork was removed from the production module graph.
- 16/32/64 KiB minimum windows looked good under independent loss but force
  traffic after real congestion and make memory/queue latency less predictable.
- Enlarging `FastRtxWnd` alone did not improve sustained-loss throughput.
- A two-second `RTO.Max` shortened one phase-sensitive 3.5 s outage from
  7.10 to 5.10 s, but slightly worsened the 1.5 s case, did nothing at 5.5 s,
  and sent more retries. Across three sustained-loss runs it was about 4%
  worse at 1/500 loss, 2% better at 1/200, and identical at 1/100. It was not
  retained; the liveness bound in §5.6 is safer than globally changing
  retransmission timing.
- Partial reliability / limited retransmits was not enabled. It silently
  abandons SCTP user messages and changes the route's delivery contract; the
  transfer layer's send, receive, and forward callbacks intentionally remain
  synchronous backpressure.

On the physical pair, the post-change sender ranged from 18.8 to 156.7 KiB
instead of remaining near 22–55 KiB. End-to-end tun samples remained inside
the pre-change 4.8–5.9 MiB/s band (post-ramp 4.99/5.10/5.41 MiB/s), proving
that the congestion recovery lead is real but is no longer the sole composed
tunnel bottleneck.

Run the opt-in measurements with:

```bash
CONNECT_WEBRTC_CASTEP_MEASURE=1 go test -run TestWebRtcSctpCwndCAStepKnee -v .
CONNECT_WEBRTC_CONGESTION_MEASURE=1 go test -run TestWebRtcSctpCongestionTuningMeasurement -v .
CONNECT_WEBRTC_QUEUE_MEASURE=1 go test -run TestWebRtcSctpCongestionQueueMeasurement -v .
CONNECT_WEBRTC_OUTAGE_MEASURE=1 go test -run TestWebRtcSctpOutageRecoveryMeasurement -v .
```

### 5.6 Idle association does not resume — TWO ROOT CAUSES FIXED

The observed idle hang is possible without any application callback hanging.
Pion's SCTP implementation gives T3 DATA retransmission no terminal failure by
design; its source explicitly assumes ICE will fail when connectivity is lost.
That assumption is false for a split data plane: STUN consent can continue over
the selected ICE socket while DTLS/SCTP records are blackholed by stale NAT,
socket, suspension, or path state. A detached reliable-channel `Write` can
return after queueing a small message, so its 15-second write deadline no longer
owns that unacknowledged message. Pion then retries it indefinitely while the
PeerConnection continues to report connected.

Two independent fixes are required:

1. **Activity-triggered SCTP progress watchdog.** After the first successful
   native data-channel write, a lazy worker observes aggregate SCTP buffered
   bytes. Forward progress is the monotonic number of accepted user bytes
   minus Pion's aggregate pending-plus-in-flight user bytes. Only growth in
   that acknowledged-byte count refreshes the 10-second deadline; reverse
   heartbeats or unrelated data cannot mask a blackholed forward queue. At the
   deadline, a connected association whose peer receiver window is zero is
   preserved because that is the transport-level signature available for
   intentional receive/forward callback backpressure. Otherwise the watchdog
   closes the association and raises the persistent one-shot
   `ImmediateReconnect` signal. There is no idle ticker or radio wakeup.
   Outbound notifications coalesce; while active, the allocation-free buffered
   amount is sampled at most every 250 ms (4 Hz), and the detailed stats/window
   snapshot is taken only at the deadline.
2. **Fresh-generation reset.** Detecting failure on the sender was not enough.
   The passive endpoint could still answer ICE and had no outbound data from
   which to infer failure. It treated the new active endpoint's SDP offer as a
   duplicate of the old one and ignored every retry. The first offer from each
   active PeerConnection now sets the existing
   `ExchangeSignals.reset_signals` bit. A fresh passive accepts it directly; an
   already-negotiated passive requests immediate replacement. Its replacement
   sends `WaitingForSdpOffer`, and the active endpoint replays its cached offer
   without another reset.

This deliberately does **not** put a timeout around transfer send, receive, or
forward callbacks. Those callbacks remain intentional backpressure. The new
deadline observes only native transport progress after a write has been
accepted by SCTP.

Resource behavior is bounded and pause-safe:

- zero timers and zero watchdog goroutines before the first application write;
- one coalescing channel and at most one lazy goroutine/timer per used
  PeerConnection, bounded by the existing peer count/memory admission limits;
- no work while the association is idle with zero buffered bytes;
- after a scheduler pause in which the monotonic deadline advances, the next
  sample evaluates the original deadline rather than granting a fresh window;
- iOS `PacketTunnelProvider.wake()` and material host path changes also call
  `DeviceLocal.NetworkChanged()`, which retires path-bound P2P state
  immediately. The watchdog covers the distinct case where the host path
  callback does not fire.

`TestWebRtcIdleResumeSctpBlackholeReconnects` leaves STUN/ICE consent flowing,
blackholes the entire DTLS record layer (including CloseNotify), verifies that
ICE stays connected, then proves:

```text
healthy idle → resumed write queued → 10 s production bound
→ active immediate reconnect → reset retires stale passive
→ new offer/answer/ICE/SCTP → payload delivered
```

The test passes 20 consecutive runs under `-race`. Terminal ICE/PeerConnection
state release, network-change replacement, and blocking-write backpressure are
covered by the adjacent focused race suite. Because both mechanisms live in
the shared Go transport, native iOS, Android, macOS, and server/proxy users get
the same fix; JS is excluded because browsers do not expose equivalent
association counters and that transport does not currently implement detached
data-channel `net.Conn`.

### 5.7 Reconnect retention and the three SDK load/lifecycle failures — FIXED

The three previously documented SDK failures were reproducible symptoms of
several ownership and lifetime defects, plus one invalid absolute-memory test
assumption. They were not failures in the SCTP watchdog itself:

1. **The synthetic provider bridge double-returned accepted raw-v2 packets.**
   `RemoteUserNatClient.SendPacket` consumes the pooled packet when it returns
   true and leaves ownership with the caller only when it returns false. The
   load helper returned every packet unconditionally. Successful bytes could
   therefore be zeroed/reused while still queued in `SendSequence`, corrupting
   the in-memory TCP stream and closing the endpoint under load. The helper now
   returns only rejected packets.
2. **A detached outer Tun dial survived Tun close.** `Tun.DialContext` linked
   its inner gVisor attempt to the Tun context, but its outer dial-race loop was
   linked only to the caller. net/http is allowed to detach that caller while
   trying to establish a reusable connection, leaving the outer loop parked
   until a 2–30 second timeout after connect/disconnect/connect. The race
   context is now canceled by either caller or Tun lifetime, and the race
   select observes cancellation directly.
3. **Permanent DoH close could miss a late h2/TLS connection.**
   `CloseIdleConnections` alone cannot close a connection whose dial/request
   is active at that instant; it may become idle immediately afterward and
   retain TLS, x509, HTTP/2, and Tun endpoint state for the 15-minute idle
   lifetime. Each cache now gates and tracks both HTTP requests and transport
   dials. Permanent `Close` refuses new work, cancels and joins admitted work,
   then closes both pools. Network change and memory shedding use the distinct,
   reusable `CloseIdleConnections` operation before rewarming.
4. **Fastest-answer one-shot DoH had the same detached-dial hole.**
   `DohQuery` returns as soon as one resolver supplies records, canceling the
   losing hedges. It previously called `CloseIdleConnections` immediately,
   without joining those canceled request/dial goroutines. Live SDK churn
   measurements found the resulting HTTP/2 read loops and sockets to
   Cloudflare, Quad9, Google, and OpenDNS accumulating until their 15-minute
   idle timeout. One-shot queries now use the same gated request+dial
   lifecycle as permanent caches: cancel, join, then close. A deterministic
   regression holds a canceled reusable dial in its teardown interval and
   proves `DohQuery` cannot return early; it passes ordinary and race-enabled
   repetitions.
5. **A closed multi-client retained its retired owner graph.** Provider
   sequence state may intentionally retain the closed client until a bounded
   idle timeout. Its direct receive callback, routing snapshot lookup, IP
   association lookup, and learned-name subscription then also retained the
   old UpgradeMux and resolver caches. `Close` now atomically detaches future
   delivery and clears both lookup paths/subscription. A callback already
   executing keeps its local callback reference and remains synchronous:
   transfer send, receive, and forward callbacks are still intentional
   backpressure.
6. **Test-only state and process-global warm state made the absolute budgets
   order-dependent.** `testing_newNetworkSpace` created an
   independently-backgrounded `AsyncLocalState` worker that cancellation did
   not close. It is now closed with the supplied context and its temporary
   storage is removed. The two absolute mobile process-memory tests also now
   execute their bodies in fresh copies of the current test binary and wait
   for asynchronous teardown. This prevents prior package tests' Go arenas,
   TLS state, message pools, and gVisor stacks from becoming the next test's
   baseline; none of the memory ceilings was relaxed.
7. **Race validation exposed a real cumulative-ACK pool race.**
   `SendSequence.receiveAck` returned an acknowledged `sendItem` to the global
   pool, then continued reading its sequence number. A concurrent sequence
   could reuse and write that item. ACK boundary and per-item sequence numbers
   are now copied before any `ackItem` call, and no returned item is read.

The focused post-fix measurement (fresh processes for the two absolute-budget
tests) was:

| Workload | Result |
|---|---|
| 20 MiB tracked-memory target | tracked peak 0.2 MiB; final 0.0 MiB; heap peak 10.1 MiB; total process peak 24.9 MiB |
| Provider: 6 peers × 8 TCP connections | 4.5 MiB echoed at 58.7 MiB/s; total peak 27.0 MiB under the 32 MiB ceiling; heap peak 12.1 MiB |
| 20 connect/reconfigure/disconnect cycles | goroutines 35 baseline → 33 final; file descriptors 7 → 8; passed the unchanged +8 goroutine tolerance |

The load tests also passed five combined repetitions, reconfiguration passed
ten repetitions, and all three passed separately under `-race`. New regression
tests prove that Tun close terminates a caller-detached in-flight dial, DoH
close cancels and joins an in-flight warm dial, and multi-client close releases
the callback/lookup ownership graph. The DoH/Tun/multi-client lifecycle suite
passed 20 ordinary and 10 race-enabled repetitions.

Final validation record (2026-07-26):

- the idle-blackhole and reset-generation/value-ownership tests passed 10
  consecutive ordinary runs; the idle-blackhole test passed 20 consecutive
  runs under `-race`, and the adjacent blocking-write, terminal-state,
  network-change, and host-dispatch suite passed five race-enabled runs;
- the deterministic full `connect` tree passed in 409.812 s with only the
  stochastic `TestPtDnsEncodeDecode` separated; that randomized DNS/QUIC loss
  test then passed three complete repetitions (48 loss cases) in 182.060 s;
- `go vet ./...` passed in both `connect` and `sdk`, and the JS/Wasm package
  compiled after native-only WebRTC tests were correctly build-tagged;
- the complete SDK tree passed in 509.674 s. The focused three-test run passed
  in 13.445 s with the measurements above; the previously recorded load-budget
  and +10-goroutine failures are resolved;
- the final Play release APK (version code `1002352110`) built successfully,
  installed on both physical devices, and launched with a live process on
  each;
- the SDK's four-minute RPC/grid unable-to-connect lifecycle test also remains
  independently validated at 240.552 s.

### 5.8 Final admission handoff, watchdog ambiguity, and physical provider validation

The final pass did not find another congestion-window setting that improved
the measured cross-regime knee. It did find adjacent lifetime and admission
faults that affect time-to-route, idle reliability, CPU, and predictable
memory even when steady-state SCTP throughput is unchanged.

#### Admission uses actual budget identity

The public/Network boolean is a policy label, not necessarily a resource
domain. SDK selected-window clients intentionally point both views at the same
`TransferMemoryBudget`. The manager now records the exact budget pointer and
reserved byte count on every admitted `peerConn`; those immutable fields drive
reclamation, release-pending detection, teardown, and pending-priority
accounting.

Pending selected peers reserve only their missing cardinality:

- an existing connection in the same actual budget satisfies one byte
  reservation;
- surplus slots remain available to ordinary peers;
- a genuinely separate public/dedicated budget continues independently;
- multiple streams for one selected peer cannot clear each other's pending
  reservation; and
- the next retry wakes at priority-lease expiry when no count/byte release can
  provide a notification.

The bounded Network-identity LRU now evicts inactive identities before a live
one, and a live association's immutable admission class remains a trust
witness if the auxiliary record is unavoidably evicted.

The adjacent handoff audit caught a one-lock-window race: byte release wakes
waiters before teardown removes the canceled connection from the manager map.
That canceled entry was incorrectly treated as still owning the released
window, so an ordinary waiter could consume the selected peer's capacity.
Budget ownership now requires a live association. The dedicated regression
models the exact released-but-not-yet-removed state and passed 100 ordinary
plus 20 race-enabled repetitions.

The same first-principles audit found an integer-domain failure in pending
budget accounting. Adding multiple `ByteCount` reservations can overflow
signed 64-bit even when each configured receive window is individually valid.
The manager now subtracts the candidate and each pending reservation from
actual available capacity, checking before every subtraction. This is
overflow-free, preserves surplus-capacity behavior, and avoids a false
admission below a wrapped sum. The adversarial
`TestWebRtcPendingPriorityBudgetAccountingDoesNotOverflow` passed 100 ordinary
and 20 race-enabled repetitions.

The reasoning flaw was present in one adjacent shared capacity gate as well.
`MemoryTarget` allows one oversized item from empty for progress, then used
`used + request <= capacity` for later admissions. A `MaxInt64` singleton plus
one byte wrapped negative and bypassed the target. The gate now rejects
negative requests and compares the request with `capacity - used` only after
proving `used <= capacity`. An unlimited target also rejects a request that
would overflow its signed usage counter; unlimited removes the configured
ceiling, not the integer representation ceiling. SDK device-memory fractions
likewise use exact quotient-plus-remainder scaling instead of
multiplication-before-division, and the sequence-depth clamp occurs before
conversion to a possibly 32-bit `int`. Normal 20 MiB partitions are
byte-for-byte unchanged.

Top-level maximum-value tests cover the generic target, negative acquisition,
DNS/client/provider partition, provider fold, both queue pairs, and sequence
cap. The connect cases passed 100 ordinary and 20 race repetitions; the SDK
maximum-target case passed 100 ordinary and 20 race repetitions, and the
combined SDK sizing/reallocation cases passed 20 ordinary and 10 race
repetitions.

#### The watchdog observes forward ACKs and preserves real backpressure

The original watchdog refreshed on aggregate received SCTP bytes. Reverse
heartbeats or data can continue across an asymmetric failure and therefore are
not proof that the forward queue advanced. The retained implementation uses:

```text
acknowledged user bytes =
    monotonic bytes accepted by Write
    - Pion aggregate pending + in-flight user bytes
```

Only an increase in that value resets the 10-second forward-progress deadline.
The hot path uses Pion's allocation-free aggregate `BufferedAmount`; the
metadata-producing `Stats()` call runs only at the deadline.

At that boundary, a connected association with `ReceiverWindow == 0` is kept
alive. This is required because transfer send, receive, and forward callback
stalls are intentional backpressure: when the remote callback stops draining
its bounded receive queue, SCTP must advertise zero rather than have the
connection torn down and replayed.

Pion's current exported receiver window is the *remaining* peer window after
subtracting outstanding bytes, not the last raw advertised credit. Zero is
therefore ambiguous if a path dies precisely after filling the window. A
timeout cannot resolve that ambiguity without also timing out a legitimate
blocked callback. The next safe transport-level lead is an upstream
last-advertised-window/control-liveness counter; no local heuristic was kept.

#### Setup logging and ready-state ownership

The physical Pixel exposed 159 public memory-admission lines in two minutes.
The error string included live counters, so a nominally identical failure
looked new on every retry. Stable reason codes (`priority`, `count`, `budget`)
now key one log per failure streak while the first line keeps its detailed
capacity sample.

An allocation was also incorrectly ending the streak. A `PeerConnection` can
allocate, negotiate for 15 seconds, and fail before the P2P route is usable.
Setup recovery now occurs only after the local ready marker is written and the
peer ready marker is consumed. The fresh Pixel build emitted only the first
startup failures; no new admission line appeared after 04:50:14 in the warm
interval.

#### Measured provider and foreground result

The fresh physical build (`versionCode=1003456453`) remained validated over
LTE with tunnel address and DNS both set to `10.0.0.168`. Process CPU was
40.65% of one core while the app activity was foregrounded in provider-only
mode, but only 3.43% after ADB backgrounding while the same VPN/provider
remained active.
Simpleperf attributed the foreground sample to RenderThread, Compose
invalidation, `drawTransferChart`, and connect-button animation. The background
sample was dominated by Go sleep/futex locations, with small TLS P-384,
socket, and GC contributions.

That attribution led to two adjacent Android fixes:

- a transfer chart below the collapsed sheet's viewport no longer runs its
  20 Hz animation clock; local viewport intersection, lifecycle, recent
  traffic, and scale-settling state must all permit it; and
- the disconnected connect pulse now uses one normalized animation read only
  by `Canvas`, rather than two animated values that caused composition,
  measure, and layout on every frame.

Five chart-visibility tests and four pulse-frame tests cover their pure
decisions. On the same Pixel, the chart gate reduced a matched foreground
sample to 22.93% of one core. The final draw-only pulse release reduced it to
21.60%, 46.9% below the 40.65% baseline, with 953 frames and 4 janky frames
(0.42%) over 30 seconds. After rebuilding the exact final multi-ABI AAR and
APK, the matched foreground sample was **19.63%**, **51.7% below baseline**,
with 927 frames and 1 janky frame (0.11%). Final foreground PSS/RSS was
224,852/384,616 KiB. After backgrounding, the same process used **3.17%** of
one core and 139,039/298,924 KiB while the VPN remained validated.

The final installed Play release is `versionCode=1003530210`; its tunnel and
DNS are both `10.0.0.128`, and it contains neither `profileable` nor
`debuggable`. Temporary symbols, profiles, and cross-compile products were
removed.

This is an attribution and pause/CPU result, not a new throughput multiplier:
the provider is low-single-digit CPU in this warm idle sample, while the
remaining foreground cost belongs to UI rendering. No new steady-state
end-to-end speed claim is added to §5.5. The remaining physical performance
gate is the real cold multi-origin page matrix in both iPhone→Pixel and
Pixel→iPhone directions, plus idle/resume and footprint. It cannot be completed
while the iPhone is locked or its owner is unavailable for taps.

#### Validation added in this pass

Top-level tests (no ordinary `t.Run`) now cover:

- zero-reference success and exact release-relative encryption reaping;
- complete TLS-plus-identity establishment timeout and worker cancellation;
- shared/dedicated actual-budget admission, surplus capacity, multi-stream
  priority, identity churn, exact lease retry, and teardown handoff;
- receiver-backpressure preservation versus idle SCTP blackhole reconnect; and
- stable admission logging plus the true ready-header recovery boundary.

The focused final matrix passed three race-enabled repetitions in 14.723 s;
the teardown and overflow cases passed 100 ordinary and 20 race repetitions.
The deterministic final `connect` core suite passed in **368.935 s**, with
`blocker`, `connectctl`, and `extender` also passing. Following the repository's
timing-isolation policy, `TestPtDnsEncodeDecode` passed three isolated
randomized runs in **176.105 s** and `TestPtDnsPumpEncodeDecode` passed in
**63.327 s**.

That isolated run also resolved the test's historical flake: retryable
`DialEarly` and `OpenStream` errors used a fatal assertion, so the first
simulated-loss timeout bypassed all four declared socket-reform attempts. They
now return false to the retry owner. The top-level retry regression passed 100
repetitions.

The complete final `sdk` package passed in **383.645 s**, including the
historical load-budget area. `go vet ./...` passes in both modules;
WebAssembly and Linux/ARM64 compile-only checks pass. The Android AAR rebuilt
for arm64, armv7, and x86_64, `:app:testPlayDebugUnitTest` passed in **25 s**,
and the release passed lint/assembly in **45 s** before installation and
measurement. No ordinary `t.Run` was introduced.

---

## 6. Supporting instrumentation added (kept in-tree)

- `connect/transport_p2p_webrtc.go`: `[signal]send ->` / `[signal]send failed` /
  `[signal]receive from` (V1) — full p2p signal-delivery trace.
- `connect/transport_p2p.go`: `[p2p]s(…) <>… setup refused = <err>` (info) —
  surfaces the previously-silent admission refusal (cap or budget).
- `connect/transfer.go`: `[r]drop/upgrade older sequence …` elevated V2→V1 with
  `(source, role, companion)` — the sender-fork tripwire from §16.
- `connect/ip_synthetic_speed.go` + `EnableSyntheticSpeed` — the §4.1 harness.
- `[signal]miss` (V2, `WebRtcManager.ReceiveSignal`) — a received signal with no
  matching peerConn (rendezvous/id churn).
- `connect/transport_p2p_webrtc_loss_test.go` — receive-window, independent
  loss, real-queue, transient-outage, and idle-DTLS-blackhole harnesses.
- `[peerconn]SCTP no progress … reconnecting` — one production-level event per
  retired half-open association; no periodic production stats logger remains.

`sdk/sdk.go` is at `v=0` with no `vmodule`; keep release measurements that way.

---

## 7. Reproduce end-to-end (checklist)

1. Check only the explicitly authorized endpoint with its full `adb -s
   <serial>` target. Never use an unscoped mutation command.
2. Set `sdk/sdk.go` verbosity (§3); `./gradlew :app:buildSdk :app:assemblePlayRelease`.
3. Install the arm64 APK on the Pixel (§2.2); build/install the signed Apple
   app only while the iPhone is connected and available for system approval.
4. Select the current authorized endpoint under **Network peers** (§4.4);
   confirm "Connected to 1 provider" and the current session's `routing ok`.
5. Verify the app UID is excluded from its VPN in `dumpsys connectivity`;
   confirm `[ice-if]synthetic …`, a selected candidate pair, and
   `[peerconn]connected … local=… remote=…`.
6. Drive the RFC-2544 synthetic download and take at least three post-ramp tun
   samples (§4.2). Use temporary `[p2p-stats]` only when attribution is needed,
   then remove it again.
7. Run the congestion/outage harnesses in §5.5 and the idle blackhole test in
   §5.6. The latter must pass repeatedly under `-race`.
8. Sanity: focused race suite, full `connect` suite, `go vet`, JS/Wasm compile,
   SDK tests, and final Android build with `v=0`, no `vmodule`, and no
   `debuggable` / `profileable` hooks.

---

## 8. Files touched

connect: `ice_net.go`; `transport_p2p_webrtc.go` (bounded signaling,
ForceStream, reset generations, lazy SCTP watchdog, eight-MTU CA setting,
bounded Network identity, exact admission-pool wakeups, and certificate
lifetime); `transport_p2p_webrtc_pc.go` / `_js.go` (public/selected-peer Pion
APIs and progress surface); `transport_p2p_webrtc_loss_test.go`
(loss/queue/outage/idle-blackhole and latency-distribution harnesses);
`transport_p2p_webrtc_test.go` (signaling, bilateral restart, real negotiated
window, promotion/expiry/admission ordering, bounded state, factory lifetime,
and default-tuning regressions); `transport_p2p_webrtc_castep_test.go`
(opt-in congestion-knee sweep); `transport_p2p.go` (peer-aware admission,
route lifecycle, and callback backpressure); `tun.go` / `_test.go`
(owner-bound dial races); `net_http_doh.go` / `_warm_test.go` and
`ip_mux_upgrade.go` (permanent versus reusable DoH teardown);
`ip_remote_multi_client.go` plus stall/path tests (retired-owner detachment);
`transfer.go` (cumulative-ACK pool safety and buffer-lock-free callback
drain/replacement waits); `transfer_callback_backpressure_test.go`
(send/receive callback locality without weakening synchronous backpressure);
`transport.go` / `transport_platform_test.go` (H1/H3 close-before-writer-join
teardown); `ip.go` +
`ip_synthetic_speed.go` / `_test.go`; provider ping ownership/accounting tests;
and the associated signaling/route tests.

sdk: `device_local.go`, `device_local_provider.go` (independent transfer/public
P2P/selected-peer P2P budgets, 128 KiB automatic and 2 MiB selected-peer
windows), `device_local_memory_test.go` (pool identity, limits, zero-target,
and partial-settings regressions);
`device_local_provider_load_test.go` (raw-v2 ownership);
`device_load_isolation_test.go`, both memory/load tests, reconfiguration/leak
helpers, and `network_space.go` (deterministic budget and lifecycle teardown);
and `sdk.go` (`v=0`, no `vmodule`). The `sdk/build`, `sdk/cgo`, and `sdk/js`
module graphs are aligned with the main SDK's WebRTC/ICE/SCTP patch versions.

android: `MainService.kt` (guaranteed app self-exclusion in allowlist mode).
Temporary `[p2p-stats]`, manifest `debuggable`, and `profileable` changes were
removed after measurement.

All uncommitted for review. Full lab notebook: `PACKETRESEARCH1.md §16–§18.6`.

---

## 9. Long multi-site contract rollover matrix (2026-07-28)

### 9.1 Root cause and retained design

The first long Samsung→Pixel sequence separated two different latency
mechanisms:

1. The old 16 KiB first contract was too small for a real cold page.
2. A successor created in advance was treated as an orphan after
   `ContractQueueExpireTimeout` (120 s), even while the sequence still owned
   and slowly consumed its current contract. Exhausting that current contract
   later forced another synchronous control round trip.

The initial attempt to identify Network peers by destination ID was
insufficient. On the provider, return packets target an ephemeral per-window
client ID; `PeerManager` knows the top-level device ID. The physical trace
therefore still showed a 13,107-byte usable first contract despite the larger
setting.

The retained policy derives the fact from the authenticated relationship:

- `TransferOptions.NetworkPeer` and `ContractKey.NetworkPeer` carry sizing and
  retention policy independently of `ForceStream`;
- the selected multi-client stamps its default options before its first ping;
- provider returns derive the bit directly from `ProvideMode_Network`, so an
  ephemeral destination ID cannot lose the relationship;
- encryption-control carriers mirror both `ForceStream` and Network policy,
  preventing a first-flight race;
- Network policy is deliberately absent from send-sequence and wire identity,
  so a local sizing hint cannot fork receiver-indistinguishable sequences;
- public/Friends companion returns explicitly clear `ForceStream` and Network
  policy, retaining the 16 KiB escrow-sensitive first contract; and
- a live Network contract retains its newest prefetched successor beyond 120
  s. Teardown flushes it. The janitor retains at most one stale successor, and
  retains no stale successor when a fresh one exists, keeping memory and
  unused-contract exposure bounded.

`ResetLocalStats` preserves the open-contract maps because they are also the
ownership proof for that bounded retention, not merely reporting counters.

### 9.2 Samsung→Pixel: deliberately stale first successor

Both devices ran the same signed Play arm64 release,
`versionCode=1004819970`, APK SHA-256
`ab8e38f74ad9b8b00675d4cfc3453ace10e563922505e918934a225cc38f2f70`.
Samsung selected the Pixel under Network peers. Chrome 150 cache was disabled
for every navigation.

Wikipedia opened the first return-data contract, then the connection was left
untouched for 130 s—past the old orphan threshold—before larger pages were
loaded:

| Site | DNS | Main TTFB | Complete load | Transfer | Requests / origins |
|---|---:|---:|---:|---:|---:|
| Wikipedia | 280 ms | 1.505 s | 2.490 s | 100,027 B | 7 / 1 |
| Mozilla | 1.163 s | 3.622 s | 12.185 s | 751,612 B | 58 / 5 |
| GitHub | 332 ms | 1.944 s | 27.292 s | 4,014,511 B | 161 / 5 |
| Guardian | 263 ms | 1.389 s | 11.007 s | 542,249 B | 92 / 8 |

The Pixel provider trace showed the decisive boundary:

```text
837,513 / 838,860 usable bytes (99.8% full)
18:47:42.998562  first-contract debit no longer fits
18:47:42.999648  successor contract set
```

The first contract had been set at 18:42:52.869, about 290 s before rollover,
so its successor was well beyond the former 120 s expiry. The observed switch
was about 1.1 ms, and neither device emitted the default-verbosity
`contract wait` diagnostic (which begins at one second).

This sequence transferred 5,408,399 browser bytes across four sites and
remained online. Against the immediately preceding physical sequence, GitHub
complete load fell from 44.06 to 27.29 s (38% lower), while Guardian changed
from a >90 s timeout to an 11.01 s load (>8.1× faster). These are observed
end-to-end comparisons, not a claim that contract policy alone accounts for
every origin/content difference.

GitHub still had a 12.46 s p95 request TTFB with 108 concurrent requests. That
tail occurred wholly on the much larger successor contract and Guardian
loaded normally afterward. It is therefore parallel-flow/transport scheduling
work, not evidence of another contract rollover pause.

### 9.3 Pixel→Samsung reverse check

The reverse direction then loaded Wikipedia followed by Mozilla without
reconnecting:

| Site | DNS | Main TTFB | Complete load | Transfer | Requests / origins |
|---|---:|---:|---:|---:|---:|
| Wikipedia | 300 ms | 1.436 s | 2.152 s | 100,027 B | 7 / 1 |
| Mozilla | 353 ms | 1.208 s | 7.545 s | 833,161 B | 58 / 5 |

Both the Pixel request direction and Samsung provider-return direction reached
the expected 838,860-byte usable first contract. Their logged successor-set
intervals were approximately 1.4 and 1.6 ms, with no slow contract wait. This
confirms that the larger initial Network contract is not dependent on one
device's top-level identity or one traffic direction.

### 9.4 Regression and build validation

Top-level tests cover Network/public first-contract sizing, generated provider
destinations, multi-client stamping, encrypted-control policy mirroring,
retention while open, expiry after close, public-contract expiry, stats-reset
ownership, and the one-stale-successor memory bound (including preference for
a fresh successor). The focused matrix passed 100 ordinary repetitions and 20
race-enabled repetitions.

The deterministic Connect core passed in 362.173 s with the two randomized
packet-translation tests isolated; `TestPtDnsEncodeDecode` and
`TestPtDnsPumpEncodeDecode` passed in 63.46 and 65.33 s. All subpackages and
`go vet ./...` pass. Android `goclientBuild`,
`testPlayReleaseUnitTest`, lint, and `assemblePlayRelease` completed
successfully before the physical install. No ordinary `t.Run` was added.

---

## 10. Cellular network-peer matrix: P2P never forms (2026-07-28)

### 10.1 What was measured

The first physical network-peer matrix run on **cellular** rather than the
home Wi-Fi lab of §1. Both authorized devices were on T-Mobile IPv6-only
cellular with 464XLAT (global IPv6 on `rmnet`, RFC 7335 `192.0.0.x` from the
CLAT), on the same signed release. Samsung `R5CX21FY6ND` selected the Pixel
`3B161FDJG001KT` under **Network peers**; the UI confirmed "Connected to 1
provider".

Chrome was cleared to a cold profile and each site loaded once. Load time is
measured to tun quiescence (six consecutive 350 ms polls with no byte
movement, subtracted from the elapsed time); bytes are `tun1` deltas.

| Site | Complete load | tun rx | tun tx |
|---|---:|---:|---:|
| Wikipedia | 24.23 s | 788,841 B | 88,683 B |
| Mozilla | 16.23 s | 1,610,883 B | 137,196 B |
| GitHub | 53.93 s | 6,270,385 B | 393,863 B |
| Guardian | 50.06 s | 4,460,936 B | 632,828 B |

Sustained synthetic throughput over the same peer was **0.13 and 0.16 MiB/s**
(two post-ramp 8 s samples of `http://198.18.0.1/download/400000000`). The
implied page-load goodput agrees: GitHub moved 6.27 MB in 53.9 s ≈ 0.11
MiB/s. That is roughly **40× below** the 4.8–5.9 MiB/s Wi-Fi band of §5.5 and
5–20× slower than the §9.2 Wi-Fi page loads.

The cause is not congestion tuning. **The P2P path never forms at all**, and
every byte rides the platform relay.

### 10.2 Direct evidence

A point-in-time socket census on both devices showed **zero UDP sockets owned
by the app UID** and only established TCP/443 connections to the platform
relay. The v=1 trace then showed the complete sequence. On the client:

```text
[sm]019fabf4-7a90… open s(019fabf4-7b6e…) 019f9835-6b1c…-><nil>
[ice-if]net.Interfaces err = route ip+net: netlinkrib: permission denied
[ice-if]synthetic en0 addrs=[192.0.0.2/32]
[ice-if]synthetic en1 addrs=[2607:fb91:e6e8:808a:…/128]
[signal]send ->…
… 15 s …
[p2p]s(019fabf4-7b6e…) ready header write err = i/o timeout
[pion:pc]peer connection state changed: closed
```

and on the provider, for that same stream id:

```text
22:39:16.236  [sm]019f9835-6b1c… reject disabled provider s(019fabf4-7b6e…)
                  source=<nil> destination=019fabf4-7a90-938f-c03a-6e6a6291ca02
22:39:16.531  [tls]019f9835-6b1c… opened session for peer 019fabf4-7a90… as server
22:39:16.534  [ip]provider ping <- 019fabf4-7a90…
```

The provider refuses the peer's P2P stream and then serves that same peer's
authenticated Network contracts **295 ms later**. The rejection is logged
once and the client retried a doomed PeerConnection every 15 s for the rest
of the session.

The ICE work of §5.1 is confirmed healthy on cellular: the synthetic net
supplies a real global IPv6 host candidate (`en1`). It is never used.

### 10.3 Root cause: the Network-peer carve-out resolves an identity that can
never appear

`StreamManager.allowStreamOpen` authorizes an inbound provider stream. For a
provider that is Network-only — or a public provider whose providing is
paused — `allowAny` is false and the decision falls through to:

```go
return self.client.peerManager.isConnectedNetworkPeer(*peerId)
```

`peerId` is the adjacent id on the StreamOpen. For a real network-peer P2P
stream that is the client's **ephemeral per-window client id**.
`connectedPeers` is populated only from `NetworkPeersUpdate`, and the
platform registers a peer only when it is top-level:

- `server/connect/transport_announce.go` guards `AddNetworkPeer` on
  `topLevel`;
- `server/model/peer_model.go` defines `topLevel = sourceClientId == nil`;
- every window client authenticates with `AuthNetworkClientArgs.SourceClientId`
  set to the device's top-level id, so `source_client_id != NULL`.

A window client is therefore **structurally absent** from every peer list,
and `isConnectedNetworkPeer(<window client>)` is always false. This is the
same identity-domain error §9.1 corrected for contract sizing — "on the
provider, return packets target an ephemeral per-window client ID;
`PeerManager` knows the top-level device ID" — recurring in the stream-open
authorization gate.

The device evidence pins both domains: the Samsung's stable top-level id is
`019fa1cf-e3ab-…` (the id the Pixel signals when the Pixel is the client),
while the rejected StreamOpen named `019fabf4-7a90-…`, minted at connect and
sharing its ULID timestamp prefix with the stream id.

`allowNetwork` is deliberately computed **outside** the `providePaused`
guard, so the design intends same-network peers to keep working while public
providing is paused. That carve-out is unreachable.

Two independent conditions expose it, which is why the Wi-Fi lab never saw it:

1. **Provide mode.** `ProvideModeNetwork` (the private provider) sets only
   `ProvideMode_Network`, so `allowAny` is false by construction.
2. **Provide pause on cellular.** `android/.../MainApplication.kt` latches
   `providePaused = true` before registering its callback and clears it only
   in `onAvailable`. The default `ProvideNetworkMode.WIFI` restricts the
   request to `TRANSPORT_WIFI`/`TRANSPORT_ETHERNET`, so on a cellular-only
   device no network ever matches, `onAvailable` never fires, and the pause
   is permanent. Pausing public providing is correct there; losing the
   same-network carve-out with it is not.

On Wi-Fi with public providing enabled, `allowAny` short-circuits true and
the broken discriminator is never reached — the defect is fully masked.

### 10.4 Rejection is terminal

Nothing reconsiders a rejected StreamOpen:

- the reject path returns `nil` — no error, no queue, no retry;
- `reconcileInboundProviderStreams` and `closeDisconnectedPeerStreams` only
  ever iterate already-open sequences and call `Cancel`; neither can open
  anything;
- the platform re-emits a hop only on the added-transition or in a new
  resident's `StreamReset` snapshot.

So a single mistimed rejection costs P2P for the whole resident lifetime.
This matters independently of the discriminator: the StreamOpen arrived
295 ms **before** the first verified Network contract, so any fix that
authorizes on the contract relationship must also reconsider the already-
rejected stream.

### 10.5 Fix options

This is an authorization boundary; none of these may weaken it. Dropping the
peer check or trusting `allowNetwork` alone would let any client holding a
Stream-mode contract open provider streams on a Network-only provider, which
is exactly what the check exists to prevent.

- **(a) Carry the owning top-level id on `StreamOpen`.** The platform already
  stores `network_client.source_client_id` and already derives `topLevel`
  from it. Authorizing on an owner id keeps the decision on platform-attested
  identity, needs no timing change, and also repairs the equally blind
  `isDisconnectedNetworkPeer` checks. Costs a proto + server change and a
  compatibility window.
- **(b) Authorize on the verified `ProvideMode_Network` contract** for that
  exact ephemeral id — the §9.1 pattern, and the same proof
  `providerReturnTransferOptions` already treats as authoritative. Must
  require `contractId != nil`: no-contract receives are handed
  `ProvideMode_Network`, so a naive read would let an unauthenticated control
  frame authorize a stream. Cannot decide at StreamOpen time on its own.
- **(c) Reconsider rejected StreamOpens** from the existing peer-update hook
  and on the first verified Network contract from that id, with strictly
  bounded, TTL'd retention.

**(b)+(c)** is the immediate fix with no wire change; **(a)** is the correct
end state. Both are open.

### 10.6 What landed in this pass

No behavior change to the authorization gate. Two items landed:

- **The terminal rejection is now visible at default verbosity.** It was
  V(1)-only, which is why a session-fatal condition was invisible on release
  builds. The first rejection per `StreamManager` now logs once via
  `sync.Once` with an explicit "terminal … p2p will not form" note; later
  rejections stay at V(1) so a churning peer cannot flood the log.
- **Two characterization tests**
  (`transfer_stream_manager_network_peer_window_test.go`) pin the defect:
  one proves the discriminator accepts a top-level id and rejects that same
  device's window client, the other proves a rejected StreamOpen is never
  reconsidered after the peer becomes known. Both fail loudly, with a pointer
  to this section, if the behavior is corrected — so the fix cannot land
  without updating them.

### 10.7 Validation

Deterministic `connect` core passed in 352.542 s with `blocker`,
`connectctl`, and `extender` green; `go vet ./...` passes in `connect` and
`sdk`. The full `sdk` tree passed apart from `TestDeviceRemoteApi`, which
times out against the live `api.bringyour.com` under suite load and passes in
1.182 s in isolation. The dead-peer recovery kernel now passes in both modes
(relay-up detect 11.891 s / recover90 13.034 s; route-full detect 11.689 s /
recover90 12.808 s), resolving the §12-addendum failure recorded in
`PACKETRESEARCH1.md`.

Pion is already at the newest published patches (WebRTC 4.2.18, ICE 4.4.0,
SCTP 1.11.1, DTLS 3.1.5); that upgrade lead is closed.

### 10.8 Doc corrections

- §F.2 and §5.2 state a 2 MiB selected-peer window with a 4 MiB ceiling. The
  shipped value is **512 KiB with two reservations (1 MiB ceiling)**, pinned
  by `sdk/device_local_memory_test.go`. The "exactly two reservations" half is
  accurate.
- §9.1's `NetworkPeer` policy is carried in Go (`TransferOptions.NetworkPeer`,
  `ContractKey.NetworkPeer`), not in `transfer.proto`. The proto's
  `NetworkPeer` message is unrelated peer-identity metadata.
- §5.8's admission logging is per-reason and emitted at powers of two, not
  strictly once per streak. Non-admission setup failures are once per streak.

### 10.9 Second exposure: the two-reservation budget cannot cover its own
### make-before-break

After reinstalling the clean release and reselecting the peer, the client
reached admission — ICE gathered real sockets this time — and was refused by
its own dedicated budget:

```text
[p2p]setup admission refused reason=budget count=1 s(019fac01-1c74…) <>019f9835-6b1c…
  = peer connection memory budget exhausted
    (used=1048576 total=1048576 need=524288 networkPeer=true
     live=1 retiring=0 sharedLive=1 sharedRetiring=1 samePeer=1 replacing=true)
```

The selected-peer budget is `deviceLocalNetworkPeerP2pConnectionCount = 2` ×
`deviceLocalNetworkPeerP2pReceiveBufferByteCount = 512 KiB` = exactly 1 MiB,
described in §F.2 as "one live association plus one make-before-break
replacement". The shared budget here already held **one live and one
retiring** association, so the replacement had no third slot and was refused.
The retiring association's reservation is not accounted for in the sizing
rationale, so the budget cannot fund the exact replacement it was sized for.

Two candidate directions, neither validated yet:

- release a retiring association's reservation at teardown initiation rather
  than at completion, so `sharedRetiring` cannot occupy a replacement slot; or
- size the count as live + replacement + retiring.

The first keeps the ceiling; the second raises it by 512 KiB. Preferring the
first is consistent with §5.8's rule that budget ownership requires a live
association.

**Update after the §11 fixes: this is now the binding constraint.** With
stream admission repaired, P2P streams actually open, so the budget is
genuinely exercised for the first time — and it saturates. A later run logged
the refusal as a doubling streak (count=1, 2, 4, 8) across 90 seconds with
`sharedLive=1 sharedRetiring=1` unchanged throughout, so one retiring owner
held its reservation for at least that long while every replacement was
refused. Tunnel throughput in that state fell back to the relay baseline
(0.09–0.17 MiB/s).

The release path explains the persistence: the teardown worker calls
`conn.admissionOwner.release()` only *after* `HandleError(conn.teardown)`
returns, so a teardown that blocks holds a reservation indefinitely. Raising
the count to three only delays the same exhaustion if teardown can hang
without bound. The real fix is therefore to release the reservation on
cancellation, or to bound teardown, and it needs its own pass with evidence
for why teardown blocks here.

This is an admission-layer condition, independent of the §11.5 routing change:
peer-connection admission and multi-client channel selection are separate
subsystems, and the refusals name the admission budget only.

**Negative result supporting the accumulation hypothesis (2026-07-29,
post-reboot).** A 23-minute matrix on the same pair after both devices
rebooted (fresh budgets, both on LAN wifi this time) logged **zero** budget
refusals on either device while sustaining 5–8 MiB/s LAN-local P2P —
including a 2.5-minute ~820 MiB burst and a 95 MiB timed transfer. The
exhaustion is therefore not a steady-state property of high P2P load; it
requires a retiring owner to accumulate (a blocked teardown holding its
reservation), which a reboot clears. This also means the defect will recur on
long-lived sessions that churn peer connections — and a host network change
is exactly such a churn source: `WebRtcManager.networkChanged` retires every
live peer conn and dials replacements immediately, so a wifi↔cell switch asks
the budget for `live + retiring` at once. The release-on-cancellation fix
remains the real repair.

---

## 11. Network-peer P2P restored on cellular (2026-07-29)

§10 diagnosed why a selected network peer never formed a P2P path and left the
authorization gate unchanged. This section records the implemented fix,
option **(b)+(c)** from §10.5, and its physical validation.

### 11.1 The authenticated witness for the missing identity (b)

`allowStreamOpen` resolved the adjacent id against `PeerManager.connectedPeers`,
which the platform populates only with top-level device clients. A window
client can never be in that map, so the Network carve-out was unreachable.

The provider already holds a stronger, authenticated statement about that exact
ephemeral id: a contract that verifies against its own `ProvideMode_Network`
provider secret key. That is the same proof `providerReturnTransferOptions`
treats as authoritative for the no-escrow return path, and it names the window
client directly.

`ReceiveSequence.registerContracts` now reports the sender after the contract
both parses and verifies, and only when `contract.ProvideMode` is
`ProvideMode_Network`. `PeerManager` records it in a bounded
`networkPeerWindowClients` map (`MaxNetworkPeerWindowClients`, default 128,
least-recently-proven evicted). `isNetworkPeer` is the union of announced
top-level peers and proven window clients, and is used by **both** stream
admission and retirement — using it for admission alone would let the next
peer reconcile immediately close the stream it had just allowed.

The proof is deliberately taken from `registerContracts`, not `receiveHead`:
`receiveHead` labels *no-contract* receives `ProvideMode_Network` without any
verification, so reading the peer mode there would let an unauthenticated
control frame authorize a stream. `TestStreamManagerRegisterContractsAuthenticatesOnlyNetworkMode`
drives the real receive path with genuinely signed contracts and asserts that a
verified Public contract does **not** authenticate its sender.

The witness map is not time-expired and is not cleared by `NetworkPeersReset`.
A reset is authoritative for top-level membership and says nothing about a
separately proven window-client contract; expiring a live witness would retire
a working P2P stream for an idle peer, reintroducing the defect. It is bounded
by count and dies with its client.

### 11.2 Refusals are reconsidered (c)

The proof legitimately arrives after the StreamOpen — 295 ms in the §10 trace —
so (b) alone still refuses. `StreamManager` now retains refused opens in a hard
bounded map (`maxRejectedStreamOpens`, 32, oldest evicted) and re-runs policy
from the two places the inputs change: the existing peer-membership hook, and
the first verified Network contract from that id. A stream that becomes
allowable is opened exactly as if it had been accepted when the platform sent
it; one still refused stays retained. `StreamClose` forgets its entry, so a
hop the platform retired cannot be resurrected by a later proof.

### 11.3 Measured on the two authorized cellular devices

Same devices, same topology, same method as §10.1 — Samsung `R5CX21FY6ND`
client, Pixel `3B161FDJG001KT` provider, cold Chrome, each site once:

| Site | §10 relay | With P2P | Change |
|---|---:|---:|---:|
| Wikipedia | 24.23 s | **5.37 s** | 4.5× faster |
| Mozilla | 16.23 s | **12.34 s** | 1.3× faster |
| GitHub | 53.93 s | **16.14 s** | 3.3× faster |
| Guardian | 50.06 s | **36.49 s** | 1.4× faster |

Time to first tunnel bytes fell from 1.05–1.07 s to a consistent 0.56–0.59 s.
Sustained synthetic throughput rose from 0.13/0.16 MiB/s to **0.76/1.19 MiB/s**.

The provider log shows the mechanism working, with the refusal and its
reconsideration 391 ms apart:

```text
01:29:24.989 [sm]019f9835-6b1c… reject disabled provider s(019fac90-40b7…)
                 source=<nil> destination=019fac90-3fe2…
01:29:25.379 [sm]019f9835-6b1c… reconsider s(019fac90-40b7…)
                 source=<nil> destination=019fac90-3fe2…
```

and the client reaches a direct pair over cellular IPv6:

```text
[peerconn]connected s(019fac90-40b7…) <>019f9835-6b1c…
  local=2607:fb91:e6e8:808a:69d1:a425:9a85:8597
  remote=2607:fb91:e6eb:8f29:ac39:bc58:6b15:37a5
```

Every refusal in the session was reconsidered (3 of 3), and the app held live
ICE sockets throughout. This is the first direct device-to-device path measured
on carrier IPv6 cellular; §5.1's ICE work was already correct there and had
simply never been reachable.

A separate confirmation on the final `v=0` release build, taken later on the
same pairing, measured 0.51 and 0.57 MiB/s. Cellular capacity moves with
signal and tower load between runs, so the honest range across both builds is
roughly **3.5–9× the relay baseline**, not a single multiplier.

These are single cold runs on a live cellular network, so treat the individual
site numbers as indicative. The reproducible results are the restored direct
path, the halved time to first byte, and a several-fold synthetic throughput
gain.

### 11.4 Connect screen reported a live peer as connecting

The same session exposed an unrelated defect the user observed directly: with
one selected network peer the connect screen showed "Connecting to providers"
indefinitely, logging `[grid]1->2(false) points=1 CONNECTING`.

A warning marks a client the window should stop steering new flows to because a
better destination is expected to exist, and the min-satisfied predicate counted
only unwarned clients. A fixed-destination window has exactly one candidate and
no substitute, so warning that sole destination made `1 <= 0` false forever.
The new pure `windowMinSatisfied` counts warned clients toward the minimum for
a fixed-destination window only; an expanding window can genuinely replace a
warned destination and is unchanged. After the fix the same pairing reports
`[grid]1->1(true) points=1 CONNECTED`.

### 11.5 The same single-candidate case in routing

`OrderedClients` filters `!client.isWarning()` to build the candidate set for a
new or re-routing flow, so the same warned sole destination also produced an
*empty* candidate set. `sendPacketUncommitted` then polls on
`FormationSendRetryTimeout` (200 ms) up to the overall send timeout, so new
flows stalled while the peer was still carrying established ones.

The reasoning is identical to §11.4. A warning means "steer new flows
elsewhere, a better destination is expected to exist". An expanding window
really does have one, because its replacement dials a different provider, so
waiting is correct there and is unchanged. A fixed-destination window's
replacement is another client to the same endpoint, so waiting buys nothing
and costs the flow.

`OrderedClients` now keeps warned clients as a last-resort set and uses them
only when a fixed-destination window has no unwarned client at all. A healthy
client always wins, and `FixedDestinationSize` allocates, so it is evaluated
only on that path. A torn-down client needs no separate guard: `WindowStats`
already errors once its own or its parent client's context is done, so it never
reaches the candidate set.

Three regressions cover the fallback, the untouched expanding-window behavior,
and the preference for an unwarned client; the first was confirmed to fail with
the fallback disabled.

### 11.6 Validation

Deterministic `connect` core passed in 343.045 s. The seven new
`transfer_stream_manager_network_peer_window_test.go` regressions and the
`windowMinSatisfied` table pass, including five race-enabled repetitions of the
stream-manager suite. The two central regressions were confirmed to fail
against the pre-fix predicate, so they are real regressions rather than
restatements. `go vet ./...` passes. The complete `sdk` tree passed in 426.141 s with no
failures, including the `TestDeviceRemoteApi` live-API case that had timed out
under suite load in the §10.7 run. No ordinary `t.Run` was added.

§10's two characterization tests were replaced by these regressions, as that
section required.
