# PACKETRESEARCH1 — packet-path memory & performance

Consolidated experiment record for the 2026-07-23 campaign on the SDK packet
path: memory-budget architecture, GC/pool/ratio tuning, and the transfer
pipeline throughput work. Spans `connect` (message pool, transfer, ip/NAT,
DoH) + `sdk` (device wiring, host knobs) + hosts (apple/android/proxy).

Living document — append new findings; keep the "Headline results" table and
"Open leads" current.

---

## 0. Headline results

| Axis | Before | After | Note |
|---|---|---|---|
| Memory knobs | 1 global `SetMemoryLimit` | 2 knobs: global pools + per-`DeviceLocal` target | multi-device processes (proxy) now bounded per instance |
| iOS ext footprint (provider load, 32 MB budget) | ~31.2 MiB peakTotal | **25.3–25.5 MiB peakTotal** | loaded heap 7.4 MiB; Go gauge, not RSS |
| Provider egress throughput (0-RTT surrogate) | 44.3 MiB/s | **89.31 MiB/s median (+102%)** | seven fresh processes, safe 4 KiB transport envelope |
| Parallelism scaling (12 peers) | 41.8 MiB/s (declining) | 72.7 MiB/s (scaling) | peer-count cliff removed |
| Pool-buffer leak on burst drop | present (4 NAT arms) | fixed | correctness |
| Burst stability (buffered routes) | run-collapse to timeout | stable | `IngressDispatchTimeout` |
| Provider-load goroutines | ~1,018 peak / 1,002 loaded | **826–827 peak / 807–810 loaded** | removed one worker + one queue per UDP flow |

All throughput numbers are the **local 0-RTT surrogate** (in-process provider
egress load, `sdk.TestMemoryAutotune` / `TestDeviceLocalProviderMemoryUnderLoad`).
They isolate allocation/GC/pool/handoff efficiency. They do **not** measure
bandwidth-delay effects (the client transfer window) or device silicon —
those need on-device validation at real RTTs.

---

## 1. Memory architecture (final state)

Replaced the single process-wide scaling budget with **two orthogonal knobs**:

### Knob 1 — global message pools: `sdk.SetMemoryLimit(total)`
- Bounds the shared message-pool free lists by ratio **packet 12 : large-object 2, of 34 parts** (`sdk/sdk.go` `memoryTargetRatio*`), via `SetMessagePoolMemoryTargets(packet, large)`.
- Sets the Go soft memory limit (`debug.SetMemoryLimit`) and the residual `connect.SetMemoryBudget` (still scales the non-device `memoryScale()` consumers: platform QUIC windows, gvisor/tun).
- Pre-warms at most **1 MiB**, and no more than a quarter, of the **packet
  (2048) class** cap at startup (`connect.WarmMessagePools`) — packet only;
  the large-object protocol-frame classes are not warmed. The 1 MiB ceiling
  preserves the measured cold-burst win without making startup retention
  scale with a large host budget.
- **Deliberately does NOT size per-device memory** — each `DeviceLocal` target is passed explicitly where the device is created, so a multi-device process bounds every device independently.

### Knob 2 — per-device: `DeviceLocalSettings.MemoryTargetByteCount`
- Set explicitly at device creation: `NewDeviceLocalWithMemoryTarget(..., keyMaterial, bytes)` (cgo `urnet_new_device_local_with_memory_target`). Default **20 MB**.
- Split by ratio **dns 2 : client 14 : provider 4 (of 20)** — `deviceMemoryShares` in `sdk/device_local.go`.
- **Provider share folds into the client share when the device can't provide** (`HostedIncompatible || !AllowProvider`): the proxy's hosted devices (which never provide) get ~28.8 MB client of their 32.
- **Provider share is dynamic**: `applyProvideMemorySharesWithLock` reallocates it live on `SetProvideMode` — provide off ⇒ the provider share backs the client pair; provide on ⇒ moves to the provider pair + egress NAT. Enabled by `TransferMemoryBudget.SetTotalByteCount` (atomic live resize). Guarded by `TestDeviceLocalProvideMemoryRealloc`.

### Per-area mechanisms
- **Message pools** — classes `{2048, 4096, 8192}`. The 4096 class holds the
  normal ≤3 KiB two-packet batch without retaining 8 KiB; 8192 holds a full
  `MinimumMessageLenLimit` 4 KiB message plus its outer envelope. A sampled
  32768 class was removed because production transports reject those frames.
  Drop-on-return free-list bounds, constant-byte large-class floors
  (64×4096 / 32×8192), and an unpooled-allocation counter keep retention
  predictable. `ResizeMessagePools(packetBytes, largeObjectBytes)`.
- **DNS** — a live `connect.MemoryTarget` owned by the device, stamped into the mux `DnsUpgradeSettings.MemoryTarget`, shared by the tunnel + fallback resolvers across mux rebuilds. Each in-flight DoH request reserves the response-read ceiling `maxDohResponseBytes` (**64 KiB → 16 KiB**). Concurrency caps derive from the target capacity (`dnsTargetHttpConcurrency` / `dnsTargetCacheEntries`).
- **Client** — the shared transfer budget pair sized 3:4 send:receive of the client share (at 20 MB device → the historically proven 6 MiB send / 8 MiB receive); `SequenceBufferSize` = 1 slot per 16 KiB; p2p peer connections admit against the receive budget via `WebRtcSettings.MemoryBudget` (SCTP `ReceiveBufferSize` 4 MiB → 512 KiB so a handful of peers coexist).
- **Provider** — its own budget pair (half the provider share, deep-copied `Send/ReceiveBufferSettings` in `newDeviceClientSettings` to avoid aliasing the client pair) + NAT flow caps from the other half via per-flow cost constants `providerUdpFlowByteCount = 2 KiB` / `providerTcpFlowByteCount = 8 KiB`.

### GC pacing (per-GOOS, `sdk/sdk.go init`)
- **iOS 10**, Android 50, desktop/server 100. The Go soft limit is the footprint backstop; GOGC paces collection below it. iOS keeps the historical aggressive 10 — see §3 (a raised float measurably regressed the jetsam-bound extension).
- `debug.SetMemoryLimit` is a **soft Go-runtime target**, not an RSS or jetsam
  ceiling. `runtime/metrics` total (`Sys-HeapReleased`) can transiently exceed
  it during allocation/GC, and excludes binary mappings, C allocations,
  thread stacks outside the Go heap accounting, and kernel buffers.

### Host values
- Apple extension: `SdkSetMemoryLimit(32 MB)` (the total footprint ceiling / go soft limit) + `SdkNewDeviceLocalWithMemoryTarget(..., 20 MB)`.
- Android: `DeviceManager.DEVICE_MEMORY_TARGET_BYTE_COUNT = 20 MB` (both creation sites).
- Server/proxy: hosted devices `MemoryTargetByteCount = 32 MB` each.
- macOS/Windows/Linux: unchanged (single knob carries the ratio; desktop runs unscaled at 64 MB).

### Observability
- `DeviceLocal.MemoryUsed()` → `DeviceLocalMemoryUsage` breakdown (dns / clientSend / clientReceive / providerSend / providerReceive / total vs target). Exported via gomobile + cgo `urnet_device_local_memory_used`. NAT flow memory is bounded by flow-count caps, not byte-tracked — the load test measures that remainder as process heap.

### Measured footprint (current tree, iOS GC pacing pinned)
- `TestDeviceLocalMemoryCeiling`: runtime total ~27–31 MiB, quiesced heap ~3.3 MiB.
- Three fresh `TestDeviceLocalProviderMemoryUnderLoad` processes at the 32 MiB
  budget: peakTotal **25.3–25.5 MiB**, loaded heap **7.4 MiB**, final heap
  **7.2 MiB**, and **52.2–52.8 MiB/s** echoed. This is a Go runtime gauge,
  not RSS.
- `TestDeviceLocalMemoryTargetUnderLoad` (20 MB device): tracked peak 0.2 MiB, drains to 0. (Transfer budgets only count above-floor borrowing — floors 256/320 KiB per sequence — so a lossless in-memory echo reads ~0; the assertion proves the bound + wiring, heap covers the untracked remainder.)

---

## 2. Autotune loop — memory settings (0-RTT surrogate)

Method: `sdk/autotune_test.go` (env-gated kernel, fresh process per measurement,
provider egress load), driven by a coordinate-descent script over the env
knobs; median-of-3 with a footprint gate; ~120 kernel runs across an iOS
(34 MB) and Android (48 MB) profile.

**Finding: every memory knob is throughput-NEUTRAL (±2.5%, spreads 0.5–4%)**
across the full explored ranges — GOGC 5–100, packet 3–20 MB, large 1–8 MB,
sequence depth 128–768, device target 12–28 MB. The shipped configuration sits
on a plateau; memory sizing is a footprint lever, not a throughput lever, on
this surrogate.

| Experiment | Result | Decision |
|---|---|---|
| Pool pre-warm at startup (`WarmMessagePools`, min(packet-class cap/4, 1 MiB)) | no-prewarm median 70.61; old quarter median 72.91 / loaded 7.44 MiB; bounded 1 MiB median 74.24 / loaded ~5.3 MiB | **KEPT, BOUNDED** (packet class only; pressure path `FreeMemory` stays cold) |
| Provider NAT `SendShardCount` 1→4 | flat (43.9 vs 44.0) | reverted |
| Pool free-list sharding | initially skipped by arithmetic; current mutex profile later measured 605 ms release-lock wait / 1.23 s load | **REOPENED + KEPT in §14**: four exact-cap shards cut release wait 83% and throughput rose 17% |
| Reserve-by-capacity queue accounting | queues never borrow above floor on surrogate | closed |
| GOGC 25/50/100 at iOS budget | flat | closed (see §3 for the on-device reason iOS stays at 10) |
| Advisory-budget decouple (scale 34/48/64 at fixed soft limit) | flat 44.0/43.7/45.1 | closed |

**Kernel gotcha (measurement correctness):** peer scaffolding client settings
scaled with the budget under test, producing a spurious 2× iOS-vs-Android gap.
Fixed by pinning peers at reference scale (`SetMemoryBudget(64 MB)` around peer
construction). A benchmark that isn't isolated measures itself.

---

## 3. The iOS regression (why the pool ratios and GOGC are what they are)

A mid-campaign tuning pass (pools 20:2→16:6, device split to dns 2 : client 6 :
provider 4 of 12, GOGC iOS→25) **regressed iOS throughput**. Root-caused to two
false assumptions that don't survive the jetsam-bound extension:

1. **Pool retention is live heap against the Go soft limit.** Oversized pool
   caps (22 MB total) push the collector into assist mode near the limit under
   load — measured as the regression. Reverted to **packet 12 : large 2**; the
   caps only need to cover the in-flight high-water to reuse well.
2. **A raised GC float trips OS memory-pressure on the extension.** GOGC 25
   let the heap float higher, triggering pressure events whose `FreeMemory`
   response drains the pools (cold reuse caches), plus GC assist near the soft
   limit. Reverted iOS to **GOGC 10**; the ~50 MiB jetsam limit with ~16 MiB
   baseline leaves no room to trade footprint for collection frequency.

Corollary that reshaped the device split: **client is the throughput-relevant
share** (bandwidth-delay window), dns is over-provisioned above its demand cap,
and the provider share is dead weight when not providing. Final split **dns 2 :
client 14 : provider 4 of 20**, provider share dynamic/foldable (§1). At 20 MB
the provide-on client pair lands exactly on the historically proven 6/8 MiB.

**Measurement pin:** the memory tests pin `pinIosGcPacing` (GOGC 10) so the
darwin test host measures the constrained-mobile config, not desktop pacing —
without it the provider test reads desktop-pacing numbers against an
iOS-calibrated ceiling.

---

## 4. Code-path loop — the throughput ceiling

The autotune loop surfaced a hard ceiling: ~44 MiB/s per device regardless of
offered parallelism (3/6/12 peers → 45.9/44.0/41.8 — *declining*). This is a
CPU serializer, not memory.

### Evidence
- **CPU profile**: dominated by scheduler wake/sleep churn — `usleep` 18%,
  `pthread_kill` 13%, `madvise` + GC scan ~20%; application compute negligible.
- **Block profile**: **88% of blocking time in `runtime.selectgo`**, 7.6% raw
  `chanrecv` — goroutines parked on channel handoffs. The pipeline is
  **handoff-latency bound**, paying a scheduler round-trip per frame per stage.
- **Select-shape microbench** (busy pipeline, data usually ready):
  | shape | ns/op |
  |---|---|
  | two-case `{data, ctx}` | 79.6 |
  | three-case `{data, stop, ctx}` | 111.1 |
  | non-blocking fast-path then two-case | 34.2 |
  | plain `<-ch` (close-to-terminate) | 33.9 |

  The gc compiler special-cases empty / single-case / single-case+default
  selects (no `selectgo`). A **two-case select still runs full `selectgo`** —
  so the lever is getting hot iterations onto the non-blocking-try-first form
  (34 ns), not "reduce to two cases." The transfer hot loops already use it.
  The bigger lever is **batch-draining** so one wake amortizes over N frames.

### Pipeline map (both directions)
- The **NAT layer (`ip.go`) is already batch-native** at its socket edges:
  `WriteBatchSize=64` socket-write coalescing (I11), batch drains at the NAT
  dispatch (I8) and the per-flow send loop (I10).
- The **transfer layer (`transfer.go`) is strictly one-frame-per-Pack**: one
  IP packet = one `protocol.Frame` = one `SendPack` = one wire Pack = one route
  write = one websocket write. This single limitation forces per-packet
  scheduler round-trips on both provider directions.
- The **return path (socket → tunnel) is fully per-packet, no coalescing** —
  ~4 cross-goroutine handoffs per return packet. `R2` (socket-read loop)
  already drains a batch but re-emits it packet-by-packet.

### Kept changes (this loop)
| Change | Why | Anchor |
|---|---|---|
| **Pool-buffer leak fix** (correctness) | `handleIpPacket` discarded the send result; a full/timeout drop at the per-flow `sendItems` handoff orphaned the pooled `ipPacket`, all 4 protocol arms | `connect/ip.go` udp4/tcp4/udp6/tcp6 send arms |
| **`IngressDispatchTimeout`** (default 5 ms) | `ClientReceive`→NAT dispatch was drop-on-full (timeout 0): a burst discarded whole packs → per-flow TCP state corruption (no retransmit toward socket) → run-collapse to timeout. Bounded per-source backpressure eliminates it | `RemoteUserNatProviderSettings.IngressDispatchTimeout`, `ip.go` |
| **Harness correction** | production transport routes are buffered (`TransportBufferSize=32`); the load test's unbuffered in-memory routes serialized the pipeline on per-frame rendezvous and under-measured the device ~25% | `sdk/device_local_provider_load_test.go` peer routes buffered(64) |

**Result: 44.3 → 59.7 MiB/s median (+35%), 2% spread, peer-count cliff gone.**

Skipped by arithmetic: I9 flow-handoff batching (~1% — the receiving loops
already batch-drain), select reshapes (hot loops already non-blocking-first).

---

## 5. Tier 2 — structural: multi-frame wire Packs

The remaining per-packet cost is the transfer layer's one-frame-per-Pack.
**Key discovery**: `sendWithSetContract` already marshals a *frames slice*
into one wire Pack (one sequence number, one `sendItem`, one ack) — line ~2718
just happened to append exactly one frame. The entire multi-frame machinery
(marshal, `MessageByteCount` sum, receive-side reassembly) already existed
end-to-end. The primitive was a small generalization, not a rewrite.

### Changes (all kept)
- **`Client.SendMultiWithTimeout(frames, dest, ack, timeout, opts)`** + `SendPack.Frames` — sends a batch as ONE wire Pack, one ack covering the batch (`transfer.go`). Single-frame paths untouched (`SendPack.frameList()` / `returnFrames()` normalize).
- **`ReceivePacketsFunction`** batch path — each NAT flow's socket-read drain travels as one `receivePackets` call (threaded through the 4 buffers/sequences as an optional field, decided live per batch since the provider registers after `Run` starts).
- **`RemoteUserNatProvider.ReceiveBatch`** — builds frames from a flow's
  batch and emits bounded `SendMultiWithTimeout` calls. The first benchmark
  used ≤16 frames / ≤32 KiB, but that was **not production-safe**:
  in-memory routes bypassed the resident/H3/server framer's 4 KiB
  `MinimumMessageLenLimit`. The retained envelope is ≤2 frames / ≤3 KiB.
  Flow-level egress policy is evaluated once per batch (the path is constant
  across the flow).
- **`SendSequence.Run` opportunistic coalescing** — once the contract is
  established, a ready compatible neighbor is folded into the same safe
  two-frame Pack without waiting. Incompatible work remains pending; ack,
  contract-capacity, encryption, and callback ownership stay pack-correct.
  This recovers most of the benchmark-only large-batch win while every
  production transport still accepts the frame.

### Two bugs found by the loop (both now documented in-code)
1. **Control packets bypass the read-loop.** Synthesized packets (SYN-ACK,
   RST, keepalive acks) emit through the *per-packet* callback, not the flow
   read-loop — found by dial timeouts. Fix: provider registers **both** batch
   (read-loop data) and per-packet (control) callbacks; no double delivery
   because a batch the consumer takes never reaches the per-packet fan-out.
2. **Slice aliasing.** `flush()` reused the frames backing array while the
   enqueued pack still referenced it asynchronously — found by bisect
   (per-packet delegation passed, chunk-size-1 hung; the provider return TCP
   sequence stalled waiting for acks that never came because the returned data
   was corrupted). Fix: fresh slice per chunk; the pack owns its slice.
   **This is the `message_pool.go` send-ownership rule applied to containers**:
   ownership hands off on send/channel write; retain requires share/copy — and
   it covers the *slice* crossing the send boundary, not just the pool buffers
   inside it. Now documented on `SendMultiWithTimeout` + `SendPack.Frames`.

**Historical result:** the original in-memory large-batch experiment measured
59.7 → 65.4 MiB/s. It proved that wake/write amortization was valuable, but
did not prove deployability because it bypassed transport framing. With the
safe envelope plus the adjacent work in §14, seven fresh processes measure
**86.73–90.57 MiB/s, median 89.31**. The earlier 12-peer aggregate remained
72.7 MiB/s — parallelism scales where it previously declined.

---

## 6. Measurement methodology

- **Kernel**: `sdk/autotune_test.go` `TestMemoryAutotune`, env-gated
  (`URNET_AUTOTUNE=1`), fresh process per run, provider egress load. Env knobs:
  `URNET_TUNE_*` (budgets use `_MB`; sequence/peer/round/flow counts do not).
  Emits one greppable `[autotune]` line: throughput MiB/s, peakTotal, peakHeap,
  loadedHeap, pool created/taken.
- **Acceptance**: median-of-3+ throughput must beat the standing baseline by
  >2.5% within a footprint gate; else revert. Reverts are targeted inverse
  edits or file backups — **never `git checkout <file>`** on a working tree
  with other uncommitted work (wiped `ip.go` once mid-loop; recovered from
  conversation context).
- **Profiling**: `-cpuprofile` / `-blockprofile` / `-mutexprofile` on a
  longer kernel run; `go tool pprof -top` (flat for CPU cost, `-cum` for stage
  attribution, block profile for handoff cost).
- **Allocation profiling**: `-memprofilerate=1 -memprofile=...` for exact
  allocation counts. This intentionally slows the kernel (current ~89 MiB/s
  becomes ~18 MiB/s), so its throughput is not a performance measurement.
- **Safety net**: the message pool's built-in double-return / leak detection
  (`[mp]` error logs, `MessagePoolCounts` balance) catches ownership mistakes
  immediately — the reason the risky return-path ownership refactor was
  tractable.
- **Canonical regression suite** (run before every commit-worthy state):
  connect short suite, sdk short suite, `TestDeviceLocalMemoryCeiling`,
  `TestDeviceLocalProviderMemoryUnderLoad`, `TestDeviceLocalMemoryTargetUnderLoad`,
  `TestDeviceLocalProvideMemoryRealloc`, then rebuild the apple xcframework and
  compile both app targets.

---

## 7. Test/behavior contracts encoded

- Pool-share boundary is **8192** (`message_pool_test.go`).
- `TestWebRtc` sends **64 KiB messages** — a 1 MiB single SCTP message can't
  reassemble in the 512 KiB p2p receive buffer; production frames are bounded
  by `MaxMessageByteCount` anyway.
- `TestMessagePool`'s exact-ratio assertion **flakes under WebRTC-test
  adjacency** — pre-existing (verified on unmodified HEAD via a worktree); a
  straggler goroutine holds buffers during the stats window. Passes in
  full-suite order.
- `pinIosGcPacing` (GOGC 10) must stay in sync with the iOS init value.
- `build/Makefile` android export-validation ignores the `Sim*` scaffolding
  and `CollapseHostNames` (Go-side `[]string` variant) as expected skips.

---

## 8. Open leads (ranked)

1. **Purpose-built value wire decoder for Pack/Ack/Frame.** Exact allocation
   profiles now put `decodePack`, `decodeFrame`, protobuf IDs/tags, and the
   receive/send wrapper objects at the top of the controllable remainder.
   Merely aliasing generated protobuf byte fields to a retained parent buffer
   was tested and rejected (§14): it caused pool misses and collapsed
   throughput. A real solution needs a value decoder with explicit parent
   lifetime and bounded object reuse.
2. **Reduce the remaining per-UDP-flow goroutines.** Removing the redundant
   outbound writer saved one goroutine and one queue per flow. The three
   remaining goroutines protect ordered delivery and isolate a slow callback;
   the next step is sharded receive dispatch with a bounded per-shard queue,
   not another direct collapse that can turn callback backpressure into drops.
3. **Bounded protocol-object reuse.** `ReceivePack`, `receiveItem`, ack
   records, and small frame slices are short-lived and numerous. Value fields
   or narrowly capped pools could reduce GC work, but unbounded `sync.Pool`
   retention would make memory less predictable and is not acceptable.
4. **State-aware DoH hedge delay.** Keep 750 ms during cold tunnel/burst
   formation (the on-device regression guard), but measure a ~100 ms stagger
   after the tunnel and h2 pipes are proven warm. Loopback dead-first runs
   consistently finished near 152 ms at 100 ms versus an occasional ~852 ms
   tail at 750 ms; this needs shared-pipe/on-device validation before defaulting.
5. **Window-client make-before-break** and **faster route-full failure
   detection** remain the two architectural pause reductions (§10/§13).

Ingress batching and R5 wake amortization are no longer open in their original
form: safe two-frame coalescing now happens in `SendSequence.Run` on both
directions. Increasing the envelope requires transport capability negotiation,
not a larger local constant.

---

## 9. Key learnings

- **Memory sizing is a footprint lever, not a throughput lever** on this path —
  every knob was throughput-neutral over wide ranges. Throughput lives in the
  pipeline structure (handoffs), not buffer sizes.
- **Pool caps are live heap.** On a jetsam-bound host, oversized reuse caches
  cost GC assist near the soft limit — bigger is not free.
- **The pipeline was handoff-latency bound**, not compute bound. 88% of block
  time was `selectgo`. The fix is batching (one wake per N frames), and the
  transfer layer's one-frame-per-Pack was the structural root — the machinery
  to fix it already existed; only the API entry appended a single frame.
- **Drop-on-full corrupts NAT TCP flows.** The NAT implements no retransmit
  toward the socket, so a burst drop stalls flows to their deadlines. Bounded
  backpressure is the right default.
- **Ownership rules cover containers, not just buffers.** The send-ownership
  handoff rule applies to the slice crossing the boundary too.
- **Isolate the benchmark before trusting it.** A spurious 2× profile gap was
  the harness scaling with the variable under test; the unbuffered-route
  under-measurement hid the real ceiling by ~25%.
- **Revert discipline on a shared working tree**: targeted inverse edits or
  file backups, never `git checkout <file>`.

---

## 10. Dead-peer recovery — measured (multi-client flow)

Kernel: `connect/multi_client_recovery_kernel_test.go` (env-gated
`URNET_RECOVERY=1`). A `RemoteUserNatMultiClient` over N in-process provider
backends (real `RemoteUserNatProvider` + `LocalUserNat` egressing to a local
udp echo), buffered gateway routes per window client bound to one provider
(via `MultiClientGeneratorWithDestination`). A constant-rate udp flow pins to
one window client; mid-transfer the pinned provider's ctx is cancelled
(abrupt kill). Measures, from kill: `detect_ms` (monitor
`ProviderStateRemoved`), `gap_ms`, `recover90_ms` (first 1 s window ≥90% of
the pre-kill echo rate), via the monitor callback + a ~100 ms goodput poll.
All knobs are env-overridable so the loop sweeps them; `URNET_REC_ROUTEBUF`
sets route depth and `URNET_REC_PING_ALWAYS=1` lifts the cping idle gate.

### Two failure modes (they have different dominant constants)
- **Transport/relay failure** (shallow route that FILLS after the peer dies):
  the window client's send blocks on the transport `WriteTimeout` (15 s), and a
  filled route also STACKS with the ack path — **prod: >50 s, no recovery in
  50 s**. This is the "backpressure masks failure" gap made concrete: a full
  route both delays the send and defers detection.
- **Relay-up, peer-silent** (deep route that keeps absorbing writes, acks
  stop): detection is the ack/stats path — **prod: 30.7 s**. Reroute after
  detection is prompt (~0.5 s gap tail; recover90 ≈ detect + 0.5 s).

### Fix levers (relay-up mode; compressed and prod constants)
| Config | detect_ms | recover90_ms |
|---|---|---|
| prod (ack 30s, statswin 30s) | 30721 | 31239 |
| shrink BOTH twins to 8 s | 8220 | 8725 |
| **shrink `AckTimeout` alone to 8 s** (statswin stays 30 s) | **8217** | 10767 |
| **always-on probe** (lift idle gate, cping rest 2 s / timeout 4 s) | **7719** | 10269 |

### Findings (refine the map)
1. **`AckTimeout` is the single dominant lever for relay-up recovery.**
   Shrinking it alone (statswin untouched) drops detection 30.7 s → 8.2 s — the
   map predicted the 30 s `StatsWindowDuration` would remain a co-floor; it
   does NOT for a flow that sees return-traffic acks. Ack-close → `endErr` →
   removal fires independently of the blackhole/stats-window path.
2. **An always-on liveness probe collapses the floor without lowering
   patience.** Lifting the `CPingMaxByteCountPerSecond` idle gate so the ping
   runs on busy flows gives ~7.7 s detection. This adds a *positive* liveness
   signal rather than shortening ack tolerance — so unlike an `AckTimeout` cut,
   it does not raise false-positive risk on lossy links.
3. **A filled route is the worst case**, not the ack floor: transport
   `WriteTimeout` + backpressure stacking pushes recovery past 50 s. Bounded
   send backpressure / faster route-full detection is a separate lever.

### Change candidates (ranked)
- **(A, optimal) Busy-flow liveness probe — IMPLEMENTED.** New
  `MultiClientSettings.CPingBusyStaleTimeout` (default **5 s**). The `ping`
  loop now probes a BUSY flow (one the idle-only `CPingMaxByteCountPerSecond`
  gate would skip) when it is actively sending but has received nothing within
  the window; the probe waits a bounded ack (≤ the window), and only a probe
  that ALSO times out errors the client. Implementation details: added live
  `lastSendTime`/`lastReceiveAckTime` to the channel stats (`addSend`/
  `addReceiveAck`); the loop polls staleness on a fine cadence
  (`CPingBusyStaleTimeout/4`, ≥1 s) decoupled from the idle-ping rest (which
  self-rate-limits via `lastIdlePingTime`), so detection is
  `~CPingBusyStaleTimeout + probe-wait` regardless of the rest interval.
  **Measured (relay-up mode, prod ack/statswin): detect 30.7 s → 11.8 s,
  recover90 33 s → 14 s.** This adds a *positive* liveness signal, not
  shortened ack patience, so it does not raise false-positive risk: a live
  peer answers the control ping within a couple RTTs (sub-second on mobile;
  the 5 s probe wait tolerates multi-second handover stalls), and a legit
  return-traffic pause only triggers a confirming probe, never a removal.
  Full multi-client suite passes (no flapping). 0-RTT surrogate — on-device
  validation still wanted for the absolute timing.
- **(B) Lower `AckTimeout` default** (30 s → ~10 s). One-line, biggest single
  effect, but 30 s is deliberately conservative for high-RTT/lossy mobile
  transports — trades detection speed for false-positive (spurious provider
  removal) risk. NOT taken; the probe (A) gets most of the benefit without the
  patience cut. Left as a product decision on false-positive tolerance.
- **(C) Bound the send-side WriteTimeout / detect route-full faster** for the
  transport-failure mode (the >50 s worst case). Open lead.

Recovery numbers are the 0-RTT surrogate; the relative ordering of levers
holds, but absolute detection times and the false-positive trade-off need
on-device measurement at real RTTs and loss.

---

## 11. DNS flow — RTT + parallelism (measured)

Kernel: `connect/dns_flow_kernel_test.go` (env-gated `URNET_DNSFLOW=1`). A
`DohCache` (the device mux resolver) over M local h2 doh servers with
injectable per-request latency and blackhole mode; a burst of distinct names
(defeating cache + single-flight) measures per-query latency distribution,
burst wall time, and effective parallelism sampled live off the dns
`MemoryTarget` (`used / dohQueryReserveByteCount` — the byte budget doubles
as a concurrency instrument). Knobs: server latencies/dead-set, stagger,
`MaxServersPerQuery`, byte target, fixed count caps, warm/cold connections.

### Findings

1. **Byte-governed parallelism is the memory governor; production also needs
   a shared-pipe wave cap.** The raw kernel's 2 MiB target permits 128
   16-KiB reservations, but that is not the device default. The production
   model adds HTTP/resolution caps 32/96. Current validation of a warm
   64-query `{5,50}` ms burst: peak reservations exactly **32**, p50 11 ms,
   p99 16–17 ms, wall 16–62 ms over three runs.
2. **The adaptive server ordering works.** After a warm burst the weighted
   per-server success stats route ~97% of queries fast-server-first; only
   the exploration-floor picks (~5%) and cold-stats starts hit a slow first
   server. So stagger only matters for that minority — but for them it is
   the whole tail.
3. **`DohServerStagger` is the dominant tail lever, and 750 ms was the
   floor.** Just-died-first-server (cold stats, live server 50 ms):
   | stagger | p50 | max |
   |---|---|---|
   | 750 ms (old default) | 801 ms | 812 ms |
   | 300 ms | 63 ms | 363 ms |
   | 150 ms | 63 ms | 202 ms |
   | **0 (race)** | **62 ms** | **67 ms** |
   Healthy 4-server spread {20,30,40,80} ms: stagger ≥150 leaves max ≈ 80 ms
   (the exploration pick unhedged); stagger 0 → p99 21 ms, max 22 ms (the
   race covers exploration too).
4. **Historical experiment, reverted after on-device validation:
   `DohServerStagger` 750 ms → 0.** It improved the independent-loopback
   dead-first case, but multiplied streams on the shared cold tunnel and
   regressed real first load. The shipped design is the §11 addendum:
   **750 ms for bursts plus an exact, bounded race for quiet queries**.

### Open leads
- **All-dead tail = `RequestTimeout` (15 s)**: if every fanned server
  blackholes, the resolver chain stalls the full request timeout before
  falling through. The mux's local-fallback race (5 s handicap) mitigates in
  production; a shorter doh-specific request timeout is the direct lever if
  this shows up on-device.
- Cold-connection setup (tcp+tls+h2 per server) is measurable with
  `URNET_DNS_WARM=0` but was not swept; the 5-minute idle pool keeps real
  sessions warm.
- Numbers are loopback; on-device RTTs shift absolutes but not the ordering
  logic (the race's win grows with server asymmetry).

### §11 addendum — on-device first-load REGRESSION and the redesign

The stagger-0 default regressed real first-load performance on device. The
surrogate failure: the loopback kernel's servers were independent endpoints
with no shared bottleneck, but on a device every DoH stream multiplexes over
ONE h2/tls connection per server through the mux's gvisor stack (16 KB tcp
buffers) and a tunnel that is coldest exactly at first load. Two compounding
regressors from the dns work: the target-derived caps raised burst
concurrency 8 -> 128, and stagger 0 doubled it again — up to ~32x the stream
pressure the old tuned config (cap 8, stagger 750) put on the cold pipe.
The old values were implicitly protecting it.

Redesign (all applied):
1. **Hedge-on-quiet** (`DohSettings.DohServerRaceMaxInFlight`, default 4):
   an isolated query (in-flight below the threshold) races its servers
   immediately — keeping the measured 812 -> ~58 ms just-died win for
   interactive lookups — while a burst keeps the restored 750 ms stagger so
   it never doubles its own volume on the shared pipe. Burst fallback must
   stay >= cold-tunnel rtt (~750) or the hedge itself re-creates the
   regression during tunnel establishment.
2. **Wave-cap the mux burst concurrency**: `min(32, target-derived)` for the
   primary cache (`min(12, …)` fallback cache) — bursts complete in waves
   the pipe can carry; the byte budget stays the memory governor.
3. **Widen the pipe** (user-approved): mux doh gvisor tcp buffers
   16 KB -> 64 KB (udp 32 KB) — the shared connection can carry a page-load
   wave of responses without head-of-line queueing, for ~100 KB per (few)
   connections out of the dns share.

Net vs the ORIGINAL config: burst path = 4x wave concurrency (8 -> 32)
through a 4x pipe with identical stagger semantics; isolated path = hedged
(new win); memory = byte-governed. Kernel validation: isolated just-died
58 ms (race fires, conc 2); healthy burst wave-capped at 32, no
amplification. Residual: a cold-stats BURST against a just-died server still
pays the 750 stagger (adaptive ordering + the mux local-fallback race
mitigate); revisit only with on-device evidence.

Lesson (methodology): loopback kernels are blind to SHARED-bottleneck
dynamics; any change that multiplies request volume must be validated
against a modeled narrow pipe (or on device) before shipping a default.

Current validation tightened the quiet predicate as well: remote and local
DoH clients share an atomic active-resolution count. The first
`DohServerRaceMaxInFlight` resolutions bypass the stagger exactly; this no
longer infers quietness from the racy `len(httpSem)`. Ten stress repetitions
of eight simultaneous resolutions with `raceMax=2` admitted exactly ten
immediate HTTP requests (8 primaries + 2 hedges). A cold isolated query with
the first server dead completed in **53–55 ms**, peak reservations **2**.

## 12. First-load program — dns + connect + formation (implemented, pending on-device numbers)

User-visible first load after connect decomposes: window formation (nothing
can flow) -> dns (tunnel-DoH cold: dial + TLS + h2 through a forming tunnel)
-> tcp connect (syn through provider egress nat) -> first byte. Six changes
attack every stage; all landed together, each independently revertable.

1. **DoH connection pre-warm** (`DohCache.Warm`, `UpgradeMux.WarmDns`,
   called by the device right after the mux is wired in SetDestination):
   one minimal wire query (`example.com` A) to the top-2 weighted servers,
   through the normal httpSem + dns byte budget, recorded into server stats
   but never the answer cache. KEY: the tunnel dial PARKS until the window
   can carry traffic, so warming at connect start self-times the
   TCP+TLS+h2 handshakes to the tunnel's first usable moment — off the
   first query's critical path. The host-egress fallback cache warms too
   (it answers the first cold-phase queries).
2. **TLS session resumption + longer idle** (net_http_doh.go): every DoH
   http client now carries a `tls.ClientSessionCache` (TLS 1.3 PSK -> 1-RTT
   re-dial after idle close / shed / rebuild); `IdleConnTimeout` 5m -> 15m.
   SEPARATE caches for the tunnel-dialed and host-dialed clients — a ticket
   minted via the host egress redeemed through the tunnel would let the DoH
   provider link the user's real IP with the tunnel egress.
3. **Adaptive local-fallback handicap** (`ColdLocalFallbackTimeout`,
   default 250ms; `tunnelDohCold`): while the tunnel-DoH is UNPROVEN on
   this mux (fresh connect) or has >=2 consecutive failures (stall), the
   local fallback races at 250ms instead of 5s, keeping the first page
   responsive during establishment (accepted widening of the startup leak
   window, user-approved). The first tunnel success restores 5s. A
   cold-phase query worker is canceled whenever the fallback wins, so it
   can lose every race on a slow tunnel — an UNRACED warm probe
   (`ensureColdProber`, 2s cadence, only while a fallback is configured)
   is what proves/re-proves the tunnel; without it a tunnel slower than
   the cold handicap would pin cold and leak indefinitely. Cold never
   re-enables a disabled fallback and only ever shortens the handicap.
4. **Per-server DoH stats persistence** (`DohSettings.ServerStatsSeed`,
   `serverStats.seed/scores`, sdk `.doh_server_scores`, 7d staleness):
   the mux's merged tunnel+fallback scores are captured at mux teardown /
   device close and seed both resolvers at the next construction — first
   fan-outs pick the last-known-fastest server instead of uniform-random.
   Seed clamped (8.0) so live results can overturn a stale ordering; decays
   on the normal trailing windows.
5. **First-load timeline instrumentation** (net_first_load.go, one
   `firstLoadTimeline` per mux == per connect): per-first-flow dns
   query->answer, tcp/443 syn->synack, syn->first-byte, logged as
   `[firstload]dns ...` / `[firstload]tcp ...` lines and exposed via
   `UpgradeMux.FirstLoadSamples` / `DeviceLocal.GetFirstLoadTimelineJson`.
   Budgets: 16 flows + 24 dns pipelines or 60s, then self-deactivates —
   the per-packet hook cost collapses to one atomic load (hot-path
   discipline per §3/§9). This is the instrument for validating 1-4 and 6
   on device.
6. **Window formation off the critical path**: (a) formation measured —
   `[multi]window <type> formed in Xms` one-shot per window (creation ->
   first ping-verified client). (b) `FormationSendRetryTimeout` (default
   200ms): while the window has NO clients, the send retry polls at 200ms
   instead of `SendRetryTimeout` (2s), so the first packets leave moments
   after the first client lands (worst case was ~2s of dead air AFTER
   formation). With any client present the 2s cadence still applies. (c)
   window identity persistence enabled for the apps (sdk
   window_identity_store.go): the PROXYDRAIN1 §3.5 identity store, backed
   by device local storage — a relaunch reconnecting to the SAME
   destination reuses the persisted window client identities, skipping one
   AuthNetworkClient api round trip per window client during formation
   (and resuming provider NAT flows after a brief kill). Guards: owner
   (client id), destination-spec fingerprint (restored identities are
   dialed FIRST, so cross-location restore must be impossible), 4h ttl.
   Side effect (deliberate, mirrors the proxy): with a store configured,
   device-close teardown skips the remove-client api calls so identities
   stay reusable; unreused rows fall to the server's idle client reap.

Follow-ups for the team:
- Each SetDestination's generator spawns a store-writer goroutine bound to
  the device ctx (pre-existing shape); they idle until device close.
  Negligible (~KB each) but a per-destination ctx would both reclaim them
  and preserve identities across same-session location ping-pong.
- On-device validation of the timeline (item 5 output) before tuning
  further: cold handicap 250ms, formation poll 200ms, warm server count 2,
  identity ttl 4h are all first-guess constants.
- Network-change re-warm: WarmDns fires per SetDestination (mux rebuild).
  A mid-session path change without a rebuild re-proves via the cold
  prober; an explicit warm on the OS path-update callback could shave the
  first post-change lookup.

### §12 addendum — recovery kernel status at the first-load pass (2026-07-24)

Running the env-gated recovery kernel (URNET_RECOVERY=1, defaults) during
first-load verification found it FAILING — and the failure is PRE-EXISTING
(reproduced with this session's multi-client edits fully reverted, on the
same uncommitted tree):

- With the new formation cadence: pre_rate=200 (healthy), detect_ms≈38400
  (twice, deterministic — AckTimeout-path timing, i.e. the §10 busy-stale
  probe is NOT producing the ~11.8s detect it was recorded at), and no
  post-refill recovery within 60s.
- Without it (pre-session state): pre_rate≈104 and detection never fires.

Interpretation: something in the campaigns AFTER the recovery work (tier-2
multi-frame Packs and/or the DNS changes are the uncommitted suspects — the
short suite stayed green throughout, but the env-gated kernel was not re-run
after tier-2) regressed the busy-stale detect path and post-refill re-route
in the kernel scenario. Needs its own debugging pass before the §10 numbers
can be claimed for the current tree.

Silver lining (validates item 6b): the kernel's pre-phase rate DOUBLES
(104 -> 200 pps) with `FormationSendRetryTimeout` 200ms — the first packets
previously idled up to 2s behind `SendRetryTimeout` while the window formed,
exactly the first-load dead air the cadence change removes.

## 13. Hangs-and-pauses program — mid-session stall removal (implemented)

Follow-on to §12 (first load): the same decomposition applied to MID-SESSION
stalls. Seven items; three turned out to be already-landed prior work (a
lesson in auditing before building — see below).

1. **Recovery regression root-caused and fixed** (the §12 addendum's open
   item). The busy-stale probe's staleness test required a RECENT SEND CALL —
   but when a peer dies mid-transfer the resend queue backs up and blocks new
   sends, so `lastSendTime` goes stale exactly when the probe matters, and
   the probe itself (sent through the same wedged queue with the full 15s
   CPingWriteTimeout) failed → silent ping-loop exit → detection fell to the
   slower paths (~38.4s, deterministic). The 11.8s recording predated the
   memory rebalance that shrank the queues (the race flipped sides; -v=1
   logging slowed the sender enough to flip it back — the tell). Fix:
   (a) busy = recent send OR outstanding unacked sends (`sendNackCount`);
   (b) busy-stale probe write timeout = max(1s, stale/4) so a wedged queue
   fails the write fast; (c) 2 consecutive unsendable probes => error
   ("busy-stale liveness probe unsendable") instead of silent exit; and
   (d) the failure counter resets whenever the channel is no longer
   busy-stale, so failures from separate healthy episodes cannot combine.
   Current kernel, three consecutive runs: detect **10.892–11.549s**, traffic
   gap **10.934–11.840s**, recover90 **11.840–12.644s**, 3/3 green (was:
   fail, detect 38.4s, no recovery). `refill_ms` is intentionally not a
   service-restoration prerequisite: one run recovered on an already-live
   sibling before the periodic window refill and therefore reported `-1`.
2. **Network-change hook** (the most common real-world hang: wifi<->cell).
   `connect.NetworkChanged()` broadcast registry (mirrors AddMemoryShedder);
   every PlatformTransport subscribes and KICKS its live connection (h1 ws +
   h3 quic) — and its reconnect-backoff waits — so re-dial happens NOW
   instead of after ping timeouts notice the dead socket. Unsubscribe rides
   the run-loop exit (owners tear down via ctx, not Close).
   `UpgradeMux.NetworkChanged` drops pooled DoH conns (answer caches kept),
   marks the tunnel unproven (cold fallback until re-proven), re-warms.
   `DeviceLocal.NetworkChanged()` ties it together; wired to the apple
   extension's existing NWPathMonitor (interface-set signature, deduped) and
   the android offline callback's onAvailable (needs the AAR rebuild, same
   as the rest of the android tree). Test: TestPlatformTransportNetworkChangeKick.
3. **RTT-scaled resend floor.** The 2s `MinResendInterval` floor dominated
   `clamp(1.2*mean, 2s, 8s)` — every loss and every route-failover paid ≥2s.
   Now two floors: cold (no rtt samples) stays 2s; sampled paths floor at
   `RttMinResendInterval` 300ms with `RttScale` 2.0 (jitter headroom; ack
   compress ≤10ms absorbed). The pre-existing per-item exponential backoff
   (sendCount shift, capped 8s) bounds duplicate cost. Validated: recovery
   kernel unchanged-good; throughput kernel 71.0 MiB/s (baseline 65.4 — no
   regression).
4. **Suspect-state routing.** `multiClientChannel.suspect` mirrors busy-stale
   each ping poll; `orderClientsSuspectLast` moves suspects behind healthy
   clients in the flow race — a NEW flow never lands on a client that is
   probably about to be errored (an all-suspect window still routes).
   Committed flows keep today's semantics (detect -> RST; migration is
   impossible by construction — a new provider is a new egress IP).
5. **Provider unknown-TCP-flow RST: already landed** (PROXYDRAIN1 §3.5):
   `EnableOrphanRst: true` + 256/s in DefaultTcpBufferSettings, tests green.
6. **Comparative blackhole connect timeout.** When any sibling window client
   received return traffic within BlackholeTimeout, a silent first connect is
   cut at `BlackholeConnectComparativeTimeout` 10s instead of 30s (window
   health callback channel->window; a real outage keeps the long timeout).
7. **CONNECTDRAIN2 make-before-break: already landed** (2026-07-19, doc §6
   RESOLVED): ResidentMigrate (proto 28) + provider-side transport handoff +
   server Drain broadcast, flags default-on
   (EnableDrainExcuse/EnableDrainCoordination). Verified in code; sdk
   migrate test green (server e2e needs the warp vault env). Window clients
   fall back to Track A (excused evict+reconnect) BY DESIGN — extending
   make-before-break to them needs transport handles threaded through
   MultiClientGenerator, a real interface change; noted as follow-up, partly
   mitigated by items 2/4 and the window's own redundancy.

Lessons:
- The busy-stale regression shows measured constants COUPLE to buffer
  sizing: the probe design implicitly assumed "sends keep flowing while
  stale," true only while the resend queue outlives the staleness window.
  State-based signals (outstanding unacked bytes) beat rate-based ones
  (recent send calls) under backpressure.
- Two of seven "gaps" were already fixed by prior campaigns (orphan RST,
  drain make-before-break) — the assessment was written from memory of the
  designs, not the tree. Audit before building.

Open follow-ups (team):
- Window-client make-before-break on drain (interface change, above).
- On-device validation of the new constants: RttMinResendInterval 300ms,
  RttScale 2.0, comparative blackhole 10s, kick dedup signature.
- Server drain e2e (TestExchangeDrainMigrateE2e) needs the warp vault env to
  run locally; unchanged by this campaign.

---

## 14. First-principles validation and adjacent packet-path work (2026-07-24)

This pass re-read the campaign from the transport envelope and ownership
boundaries outward, then validated each claimed win with production-shaped
limits. It found one important invalid conclusion: the 16-frame / 32 KiB
batch won only because the in-memory benchmark bypassed the 4 KiB production
framer. The optimization direction was right; the tested envelope was not.

### Current measurements

| Kernel | Current result | Predictability evidence |
|---|---|---|
| `TestMemoryAutotune`, seven fresh processes | **86.73–90.57 MiB/s; median 89.31** | loaded heap 5.41–5.60 MiB; no unpooled payloads |
| exact allocation profile (`memprofilerate=1`) | **212.4 MB / 202.5 MiB allocated** | down from ~272 MB at the start of this validation; profiler throughput is intentionally ignored |
| provider memory, three fresh processes | **52.2–52.8 MiB/s** | peakTotal 25.3–25.5 MiB, loaded 7.4 MiB, final 7.2 MiB |
| provider goroutines | peak **826–827**, loaded **807–810**, final **657–660** | before adjacent UDP work: ~1,018 / 1,002 / 852 |
| recovery, three consecutive runs | detect **10.892–11.549s**, recover90 **11.840–12.644s** | 3/3; refill can occur after traffic moves to an existing sibling |
| DNS production-shaped burst | peak reservations **32 exactly** | 64 queries, HTTP/resolution caps 32/96 |
| DNS isolated dead-first | **53–55 ms**, peak reservations 2 | exact quiet-race admission; 3/3 |

`peakTotal` is `Sys-HeapReleased`, not RSS, a hard ceiling, or the iOS jetsam
footprint. In the heavier 34 MiB autotune kernel it transiently measured
39.98–40.77 MiB. That is compatible with Go's soft-limit semantics and must
not be reported as “the process stayed below 34 MiB.”

### Kept adjacent optimizations

1. **Transport-safe coalescing.** The batch bound is now two frames / 3 KiB,
   with a regression test that marshals and encrypts two maximum-MTU packets
   and asserts the result fits the 4 KiB minimum transport limit.
   `SendSequence.Run` coalesces only an already-ready compatible neighbor and
   never waits to fill a batch, so latency does not acquire a batching delay.
2. **Three useful pool classes, bounded warm state.** `{2048,4096,8192}`
   matches MTU packets, normal two-packet frames, and the maximum accepted
   frame envelope. The discarded 32768 class retained memory for messages
   production could not send. Startup packet prewarm is capped at 1 MiB:
   the measured first-burst benefit remains while quiesced heap drops about
   2.2 MiB versus warming a quarter of the full 12 MiB class cap.
3. **One fused critical section, then four bounded free-list shards.** Buffer
   removal plus metadata initialization, and metadata release plus free-list
   insertion, are fused; allocation/error logging occur after unlock. A
   current normal-memory mutex profile then disproved the earlier
   arithmetic-only decision to skip sharding: one class lock accumulated
   605 ms of release wait in a 1.23 s load. Four shards preserve the exact
   total free-list byte cap and encode the acquisition shard in the immutable
   buffer id. Release wait fell **605 → 103 ms (-83%)**, total measured mutex
   delay **3.36 → 1.96 s (-42%)**, and end-to-end throughput median
   **76.24 → 89.31 MiB/s (+17%)**. The parallel pool microbenchmark is
   **169–175 ns** (from 202–211 ns immediately before sharding and ~439 ns at
   validation start); serial is 28.8 ns versus 27.3 ns. Steady state remains
   zero-allocation. Cost: ~54 KiB fixed counter/lock bookkeeping across all
   classes and ~0.1 MiB measured quiesced heap, with unchanged retained-buffer
   caps.
4. **Fused encryption and pooled decryption.** AEAD seals directly into the
   final pooled encrypted-frame encoding instead of allocating ciphertext and
   copying it into a second buffer: about 1.27 µs / 3,088 B / 2 allocs became
   0.98 µs / 0 B / 0 allocs. Decryption changed from about 0.79 µs / 3,072 B /
   1 alloc to 0.60 µs / 0 B / 0 alloc.
5. **Pooled decoded payloads with explicit lifetimes.** Non-empty decoded
   `Frame.MessageBytes` now come from the message pool and are returned on
   every success, malformed-frame, overwrite, stale-queue, and shutdown path.
   This removed a ~49 MB `copyProtoBytes` allocation site from the original
   exact profile. Receive and security callbacks now explicitly borrow
   buffers/path slices only for the call; retention requires share/copy.
6. **Raw-frame and borrowed-path parsing.** Raw IP v2 frames expose their
   packet bytes directly instead of constructing legacy wrapper protobufs.
   Synchronous security-policy paths parse IP address slices as aliases of
   that packet. Isolated tests are zero-allocation; the public parser retains
   its owned-copy lifetime contract.
7. **Reusable ack notification.** The ack compressor has one capacity-one
   coalescing signal instead of closing and allocating a monitor channel for
   every received packet. The old site accounted for roughly 23 MB and
   216,000 objects in the exact profile; it is now absent from the hot path
   and a steady-state allocation test enforces that.
8. **Reusable timers.** UDP/TCP blocked sends, route reads, UDP idle waits,
   TCP connect/idle waits reuse serialized timers. In particular,
   `TcpSequence.Run` no longer calls `time.After` per segment:
   exact-profile `time.After` allocation fell from 2.748 MB to **25.5 KB**
   in the final profile (the remainder is control/setup code).
9. **One fewer UDP goroutine and queue per flow.** `UdpSequence.Run` drains
   the already-bounded send queue and writes datagrams directly; `net.Conn`
   still has one concurrent reader and one writer. This removed the redundant
   write worker and a second `SequenceBufferSize` channel without changing
   datagram ordering or callback isolation.
10. **Recovery/DNS state is counted, not inferred.** Busy-probe send failures
    reset outside a stale episode. Quiet DoH admission uses a shared atomic
    active-resolution count rather than the racy occupancy of an HTTP
    semaphore.
11. **Race-free multi-client startup/profile changes.** The race detector
    found window workers reading a performance profile while the constructor
    applied the same-network direct-mode override. Profiles are cloned,
    immutable after publication, and atomically stored before workers launch;
    runtime profile changes use the same publication path. The focused race
    suite passes; three post-fix, pre-sharding throughput runs were
    75.60–78.76 MiB/s (median 76.15), confirming no regression from the race
    fix itself.

### Rejected branch

Aliasing Pack/Ack/path-ID protobuf byte slices into one retained parent wire
buffer looked attractive in an allocation profile. It was wrong for this
pipeline: repeated fresh runs collapsed from roughly 74 MiB/s to 9–31 MiB/s
and caused a surge of 4 KiB pool misses. The branch was fully reverted. Do not
retry field aliasing piecemeal; a value decoder must own the whole lifetime
model and be measured end-to-end.

### Bounds that remain deliberately soft

- Message-pool caps bound **retained free-list bytes**, not in-flight demand;
  an empty pool allocates so traffic cannot deadlock.
- `MemoryTarget` admits one oversized acquisition when empty to guarantee
  progress. Without a live capacity shrink, overshoot is bounded by that one
  acquisition; consumers needing a hard bound must cap request size.
- Go's memory limit is advisory and excludes non-Go process memory.
- Protocol-object pooling must be bounded. Reducing average allocations with
  an unbounded cache would violate the more important predictable-memory goal.

The next work should follow the ranked §8 list. The final exact and mutex
profiles say the remaining controllable cost is protocol object/ID decoding
and higher-level pipeline synchronization, not the former global pool convoy,
ack wakeups, encryption copies, receive payload copies, or per-packet timers.

### §13 addendum — degraded-mode liveness + richer transition signals (2026-07-24, user feedback)

Feedback: (a) the 5s busy-stale probe is too aggressive on low-power /
weak-cell devices (slow-but-alive peers get removed); (b) the wifi<->cell
transition should lean on more OS signals from the apps.

**Degraded performance mode.** `MultiClientSettings.DegradedMode`
(shared atomic) + `DegradedLivenessScale` (3.0): while the HOST reports
degraded, `busyStaleTimeout()` scales the busy-stale window and with it the
probe ack budget, probe write timeout, and poll cadence — a device that
answers control pings slowly is not misdiagnosed as dead. Plumbed
`DeviceLocal.SetPerformanceDegraded` -> multi client (stamped per window
build + live-forwarded). Kernel (URNET_REC_DEGRADED=1): detect 30.8s vs
10.9s normal, recovery still completes — the intended trade (a false
removal costs flow RSTs + reconnect churn; extra latency on a degraded
device costs little). App signals feeding it:
- iOS extension: low power mode + thermal state (>= serious) +
  path.isConstrained (Low Data Mode), refreshed on the power/thermal
  notifications and every path update.
- android: battery saver (ACTION_POWER_SAVE_MODE_CHANGED + initial state).

**Richer transition signals.**
- iOS: NEProvider `defaultPath` KVO (the VPN-aware default-path signal, can
  lead the physical NWPathMonitor) prompts a re-check; `wake()` override
  fires NetworkChanged + a state refresh (post-sleep sockets are stale — a
  previously-unhandled hang class); the path signature now includes
  gateways, so an AP/subnet roam behind the same interface name kicks too.
- android: the requestNetwork(INTERNET) callback — NOT the default-network
  callback, which reports the VPN's own tun as default while the tunnel is
  up and so misses underlying switches — carries the kick; it gains
  onLinkPropertiesChanged with an addresses+routes fingerprint (dhcp
  renumber / roam on the same Network object).
- SDK: `ClientStrategy` subscribes to NetworkChanged and drops every
  dialer's pooled http connections (api calls after a transition no longer
  stall on a dead pooled socket; clients rebuild lazily).

Deferred: p2p/webrtc conns do not yet subscribe to NetworkChanged (per-conn
lifecycle work); a dead-path p2p stream is rescued by the rtt-floor resend
shift to the platform route plus the fresh platform re-dial.

---

## 15. Remaining-leads pass: owned values, bounded concurrency, warm hedges,
route-full recovery, and drain handoff (2026-07-24)

This pass closed the remaining code-path leads from §14 and tested adjacent
work exposed by their profiles. The unifying rule is that every optimization
must state both a lifetime and a bound: a lower average allocation count is
not a memory improvement if reuse can retain an unbounded burst, and a shorter
timeout is not a latency improvement if it queues behind the work it is meant
to diagnose.

### 15.1 Final measurements

| Kernel | Result | Bound / interpretation |
|---|---:|---|
| owned TransferFrame decode, old | 372.2–385.4 ns, 488 B, 9 allocs | copied protocol objects |
| owned TransferFrame decode, new | **300.0–300.8 ns, 0 B, 0 allocs** | four shards, 256 retained owners maximum |
| exact allocation profile | **555,887 objects / 170.63 MiB** | from 908,035 / 202.53 MiB before this pass |
| autotune, five fresh processes | **89.79–94.84 MiB/s; median 92.00** | previous seven-run median 89.31 |
| provider load, three fresh processes | **54.6–58.8 MiB/s** | peakTotal 26.1–26.8 MiB; loaded heap 6.7–6.8 MiB |
| provider goroutines | peak **639**, loaded **623**, final **473** | previous 826–827 / 807–810 / 657–660 |
| route-full recovery, 64-frame route | **11.76 s detect, 12.00 s gap, 12.90 s recover90** | before fix: no recovery within 40 s |
| warm DoH, healthy shared pipe | **p95 174 ms, 72 requests** | identical at 750 and 100 ms |
| warm DoH, stale dead primary | **p95 924→274 ms; max 1604→303 ms** | requests 103→105 |
| macOS provider process | RSS **55.70–56.34 MiB**; physical footprint **36.50–36.67 MiB** | Go peakTotal only 26.1–26.8 MiB |

The exact profile uses `memprofilerate=1`; its throughput is intentionally not
used. The provider RSS runs execute a prebuilt test binary under
`/usr/bin/time -l`, so compiler/tool memory is excluded.

### 15.2 Lifetime-safe value decoder and adjacent value envelopes

`unmarshalOwnedTransferFrame` now decodes the normal v2 wire shape into one
explicit `decodedPackOwner`:

- 16-byte IDs decode into inline `Id` values, with exact-length validation;
- the production two-frame maximum uses inline `protocol.Frame` values and
  pointer slots; legacy/untrusted overflow remains accepted but allocates in
  proportion to the exceptional input;
- `Pack`, contract frame, `Tag`, path IDs, carrier, role, and companion all
  have an owner-defined lifetime;
- ownership moves `Client.run → ReceivePack → receiveItem → synchronous
  callback/ACK tag copy`, and returns only after the last borrower;
- RTT ACK state copies `Tag` by value, so decoder reuse cannot mutate a later
  RTT sample.

The pool is an exact-cap sharded free list, not `sync.Pool`. The final owner
also embeds the successive `ReceivePack` and `receiveItem` envelopes. They
cannot be live independently, so this removes two heap objects and a cached
method-value closure per inbound pack without adding another cache or lock.
On arm64 the combined owner is 928 bytes. The exact provider profile created
166 owners across more than 23,000 inbound packs; the 256 cap provides 54%
headroom and an exact worst retained bound of about 232 KiB. A second
376-byte synchronous wrapper cache is capped at 256 (about 94 KiB).

The ACK compressor was made value-based at the same boundary: its head and
selective entries are copied into snapshots rather than publishing per-packet
`*sequenceAck` objects. The `ReceivePack`, `receiveItem`, callback closure, and
`sequenceAck` allocation sites all disappeared. Combined with the decoder,
exact allocation objects fell **38.8%** and allocated bytes fell **15.7%**
from the §14 profile.

### 15.3 Sharded ordered UDP receive dispatch

Inbound UDP no longer owns one queue and goroutine per flow. Each `UdpBuffer`
has four lazy FIFO workers; a flow is permanently assigned round-robin to one
worker, and the socket's single reader is the sole producer. Therefore:

- callback order for each flow is preserved;
- adjacent packets for one flow can still be batched;
- worker/queue memory is `ReceiveShardCount × SequenceBufferSize`, independent
  of flow count;
- 10,000 assigned flows do not change queue capacity;
- shutdown returns every queued pooled packet.

The 12-flow/2,400-packet ordering test passes at count 10 and under the race
detector. Steady dispatch is 297–323 ns with zero steady-state allocations.
The provider kernel's loaded goroutines fell another **187** (810→623),
matching removal of the per-flow receive workers. Head-of-line blocking is
now bounded within a shard rather than isolated per flow; four shards are the
measured memory/parallelism compromise, and `ReceiveShardCount` is explicit.

### 15.4 Warm-state DoH staggering and the shared-pipe constraint

The resolver now distinguishes path state:

- cold/unproven or recently stalled tunnel: retain the conservative **750 ms**
  stagger;
- proven warm tunnel: use **100 ms**;
- quiet isolated lookups still use the existing immediate race;
- four of 32 HTTP slots are reserved from the warm first wave so a stale
  primary wave cannot occupy every slot and starve its own hedges.

The reserve is necessary. In the stale-dead-primary model, all first-wave
requests previously filled the global HTTP semaphore and only 3–4 of 64
queries completed by 3 s: changing the stagger could not help because hedges
could not enter. With the reserve, all 64 complete.

The final model adds a shared four-slot/10 ms response stage after independent
server latency, representing all h2 streams crossing the same tunnel pipe:

- healthy servers (20/80 ms): 750 and 100 ms both produce p95 174 ms and 72
  requests;
- a 1 ms near-immediate burst produces the same latency but 124 requests
  (**+72%**), so immediate warm burst racing is rejected;
- stale-seeded dead primary plus 20 ms survivor: 100 ms cuts p95 924→274 ms
  and max 1604→303 ms with essentially unchanged volume (103→105).

This validates 100 ms as the host-model default, not as an on-device proof.
`DohPathWarm` is supplied by the mux's tunnel success/stall state; a persisted
server ordering alone never marks the new pipe warm.

### 15.5 Route-full detection: fix admission serialization, not the timer

The small-route recovery kernel found a hidden lock inversion in intent:
`SendSequence.Pack` held an exclusive close mutex while an application send
waited indefinitely on a full bounded queue. A liveness probe behind it had a
finite 1.25 s admission timeout, but could not acquire the mutex to start that
timeout. The result was not merely a slow 15 s route write: the channel never
recovered within the 40 s kernel window.

`packMutex` is now an `RWMutex`. Concurrent producers hold read locks and wait
independently; close cancels first (waking all waits), then takes the write
lock before closing the queue. A deterministic test fills the queue, parks an
infinite writer, and proves a second writer observes its own 25 ms timeout.
The race detector covers close/wait overlap. The 64-frame recovery kernel then
returns to the normal busy-stale envelope (11.76 s detect, 12.90 s recover90).

This is safer than declaring every full route dead: transient backpressure
still drains normally, no route is deactivated, and only two consecutive
unsendable probes inside one stale episode remove the channel.

### 15.6 Window-client drain make-before-break

The provider client's resident transport already retained its handle and
migrated make-before-break. Window clients did not: `ApiMultiClientGenerator`
constructed each platform transport and discarded the handle, so their
`ResidentMigrate` control frames fell through the receive switch.

The window path now uses an optional
`MultiClientGeneratorTransportMigrator` capability, avoiding a breaking
change for in-process/custom generators:

1. only a control-source `ResidentMigrate` is accepted;
2. the API generator retains one transport state per live window client;
3. duplicate migration requests coalesce, and a far-future absolute time is
   clamped to five minutes;
4. a replacement is constructed on the same client/route manager and may
   overlap only one old transport;
5. the old route closes only after `ConnectedNotify/IsConnected` proves the
   replacement registered routes;
6. a 60 s connect timeout closes the replacement and keeps the old route, with
   the server drain-excuse reconnect remaining the fallback;
7. removal races are checked again before construction and before swap.

The retained map is bounded by live quality/speed window clients; temporary
transport overlap is at most one per migrating client and server-side migrate
times are already jittered across the drain window. Tests cover source
authentication, schedule clamp, duplicate suppression, no break before
connect, successful swap, timeout retention, and race execution.

### 15.7 RSS/jetsam result and required device gate

The desktop host measurement proves the original warning, rather than closing
it. At a 32 MiB Go soft limit, three final runs measured:

- Go `Sys-HeapReleased` peak: 26.1–26.8 MiB;
- macOS peak physical footprint: 36.50–36.67 MiB;
- peak RSS: 55.70–56.34 MiB.

Thus the Go gauge under-reported physical footprint by roughly 10 MiB and RSS
by roughly 30 MiB in this kernel. The test process includes peer scaffolding,
so these are regression numbers, not a packet-extension capacity claim.

Release validation must use a release framework/AAR on real low-memory
devices:

| Platform | Primary measure | Also record | Failure evidence |
|---|---|---|---|
| iOS packet extension | `TASK_VM_INFO.phys_footprint` at 20–50 Hz | RSS, Go total/live/goal, goroutines, pool/budget use, `os_proc_available_memory()` | jetsam / `EXC_RESOURCE` report |
| Android VPN service | total PSS (`Debug.MemoryInfo` / `dumpsys meminfo`) | RSS, native/graphics/code split, Go gauges, LMKD pressure | `ApplicationExitInfo.REASON_LOW_MEMORY` / LMKD log |

Run cold first load, repeated warm loads, 64 unique DNS names, provide-on
TCP+UDP churn, wifi↔cell, wake, and a drain migration while traffic is live;
then hold for 30 minutes and verify both peak headroom and post-settle return.
Acceptance is device-class-relative: no memory kill, no monotonic
post-settle slope, and peak physical footprint/PSS below 80% of the measured
kill threshold for the lowest supported device. Do not substitute simulator,
debug, race, Go heap, or RSS-only results for this gate.

For the warm DoH default, A/B 750/100/0 ms on the same device and network.
Keep 100 ms only if healthy-pipe p95 changes by less than 5%, stale-primary
p95 materially improves, peak footprint/PSS does not regress outside run
noise, and request/byte volume stays near 750 ms. The host result predicts
that 0 ms fails the volume criterion.

### Remaining external validation

- Real-device jetsam/PSS and shared-pipe A/B above.
- Android AAR rebuild so the existing Kotlin memory/network call sites and
  these Go changes ship together.
- Server `TestExchangeDrainMigrateE2e` still needs the warp vault environment;
  deterministic client-side handoff tests pass locally.
- P2P/WebRTC connections still need direct `NetworkChanged` kick lifecycle
  work; platform-route failover remains the current backstop.

## 16. Network-peer + post-quantum data blackhole — the ForceStream sequence
## fork (diagnosed on-device and fixed, 2026-07-25)

Field report: two same-network peer Android devices (client Pixel 8 Pro,
provider SM-S928U client_id 019f9833-2d1e-cb39-0d45-526f8c30ab3b), with the
app's post-quantum encryption enabled, could never establish a usable peer
connection. Grid stuck CONNECTING/looping; per-peer TLS handshakes VERIFIED on
both ends ("peer identity proof verified — cipher is now usable") yet every
window client was removed ~10-15s later with `Blackhole (0 0B)` and replaced —
a new client id every 15s, indefinitely. A second variant on the same pair:
`completeHandshake failed: tls handshake timeout after 1m0s` with ~1.3-1.4KB
frames (a hybrid X25519MLKEM768 ClientHello) retransmitted every ~6s into a
16KiB initial contract until timeout.

Method (adb, both devices local): deployed a play-variant build with glog
`v=1, vmodule=transfer=2`. The verbose capture localized the drop in minutes:

- Client wrote wrapped data fine (`write wrapped 1379 -> 1451 bytes`).
- Provider received and unwrapped fine (zero `unwrap err`).
- Provider `[r]drop older sequence 019f985a-a69b... < 019f985a-a6a6...` —
  an endless stream, every window-client cycle. The provider's ingress
  security-policy stats stayed EMPTY: not one provided packet ever reached
  the IP layer.

Root cause (uuidv7 timestamps decoded the two sequence ids to their creation
instants, 11ms apart, matching the client log exactly):

- Same-network peers force `AllowDirect` on
  (`RemoteUserNatMultiClient.overrideAllowDirect`: ProvideMode_Network →
  forceAllowDirect(true)), so the multi-client data path sends with the
  `ForceStream()` transfer option (`ip_remote_multi_client.go` send + ping).
- The post-quantum toggle (`PerformanceProfile.PostQuantumEncryption`) sets
  `EncryptionSettings.Encrypt = true`, which adds per-peer TLS sessions whose
  handshake flights ride `SendBuffer.SendEncryptedControl` — which mirrored
  `DefaultTransferOpts` (ForceStream=false).
- `ForceStream` is part of `sendSequenceId` (the send-sequence key) but is
  INVISIBLE on the wire. The data Pack (fs=true) and the ClientHello Pack
  (fs=false) therefore minted two live send sequences whose frames are
  indistinguishable at the receiver — same source, no role stamp (both
  client-role), no companion stamp — so both map to one receive head slot
  `(source, server, c=false)`. Head supersession keeps the newest sequence id
  and drops the other's packs forever, un-acked.
- Whichever sequence loses is the symptom: data minted first → data evicted →
  handshake green, zero data, `Blackhole (0 0B)` (the 15s loop); ClientHello
  carrier minted first → CH evicted → 1m handshake timeout with CH
  retransmits churning the 16KiB initial contract (the a71e↔6b1c wedge).
  The provider-side return-handshake flakiness (3/10) was downstream: no
  forward data → usually nothing to send back → no return sequence.
- PQ off → no EncryptedControl → no second sequence → works. Non-peer
  providers default AllowDirect off → no fork → PQ works there. The failure
  is exactly the intersection: network peer (AllowDirect) ∧ PQ.

Fix (connect): the EncryptedControl carrier must select the SAME send
sequence as the application data for the peer.

- `peerEncryptionSession.carrierForceStream` (atomic.Bool) — the ForceStream
  option of the most recent acquiring send sequence.
- `EncryptionSessionManager.AcquireForSend(peer, role, companion,
  forceStream)` stores it BEFORE the client-role handshake restart, so even
  the first ClientHello rides the data path's sequence.
- `SendBuffer.SendEncryptedControl(..., forceStream)` applies it:
  `opts.ForceStream = forceStream && !contractCompanion` (companion carriers
  stay off streams — the platform rejects companion stream contracts).
- The `[r]drop older sequence` / `[r]upgrade older sequence` V(2) logs now
  include (source, role, companion): a PERSISTENT drop-older stream for one
  source is the tripwire signature of any future sender-side key fork.
- Regression test: `TestEncryptedControlCarrierMirrorsForceStream` (carrier
  lands on the fs=true sequence, exactly one sequence to the peer, companion
  carrier never requests a stream, last-acquirer-wins retune).

Post-stream note: once the platform assigns a pair stream, stream-bound data
frames carry the stream id in the path and key a separate receive slot — the
collision only lives in the pre-stream window, which is exactly where every
new window client (and therefore every peer connect with PQ) starts.

Lesson: any option that keys the send sequence but does not appear on the
wire (today: ForceStream, CompanionContract-without-encryption-companion) is
a foot-gun — two live same-key-looking sequences at the receiver are
indistinguishable and one gets silently starved. The receive-side drop-older
path deliberately stays supersede-only; the invariant is enforced on the
sender: everything to a peer that must interleave rides one sequence.

Follow-ups for the team:
- Consider a receive-side tripwire (count persistent drop-older per head slot,
  Errorf once) — cheap detection for future forks.
- The 16KiB initial contract is ~9 PQ ClientHello retransmits; with the fork
  fixed the CH is acked on the first delivery, but a lost-peer wedge still
  churns one contract per TlsTimeout window (accepted; see the TlsTimeout=60s
  bound rationale in transfer_encrypt.go).

§16 verification addendum (2026-07-25): fix deployed to both devices as
2026.7.25 play arm64 builds at normal verbosity. The Pixel↔Samsung peer
connection with post-quantum encryption now establishes on the first window
client and stays CONNECTED (soaked 10+ minutes, zero Blackhole removals, zero
drop-older events, provider ingress policy counting provided traffic, page
loads passing ~906KB back through the peer in the verbose interim build).
Regression tests: `TestEncryptedControlCarrierMirrorsForceStream` (unit,
carrier/sequence-key contract) and `TestSendReceiveEncryptedForceStreamData`
(e2e two-client, Encrypt=true + ForceStream data, both ForceStream contract
keys served like the platform does; validated to FAIL against the pre-fix
carrier and pass with the fix; includes a 60s zero-delivery starvation guard
and a final single-client-role-sequence assertion). Full connect suite green
(479s) and sdk suite green (the one TestDeviceLocalReconfigurationChurn
failure reproduced only under concurrent xcodebuild CPU contention — passes
clean in 8s on a quiet machine). AAR + xcframework rebuilt with the fix
(NOTE: never run make build_apple and the gradle buildSdk concurrently — the
shared sdk/build tree corrupts the xcframework swap, observed again today).

## 17. Device-to-device (network peer) performance — the WAN-relay
## bottleneck and p2p never connecting (in progress, 2026-07-25)

Field report: device-to-device / network-peer connection performance is
sub-optimal (slow). Two Android devices on the same premises (Pixel 8 Pro
client 019f9835-6b1c…, SM-S928U provider 019f9833-2d1e…) with post-quantum
encryption enabled.

### Baseline (measured, instrumented v=1 play build, both devices local)

- **Throughput: 0.54 MiB/s (4.49 Mbit/s)** — measured as tun1 rx-byte delta
  over 10s of a sustained synthetic download through the peer.
- **Path: 100% relayed through the platform over the WAN.** p2p/webrtc NEVER
  connects (0 `stable` ICE handshakes on either device across the whole
  session, for ANY peer). All peer traffic goes device → platform relay
  (65.49.70.85, ~59 ms RTT from the mac; the phones can't ICMP it but the
  data path rides it) → device.
- **Direct UDP between the two devices WORKS both directions** (nc test:
  192.168.1.217 ↔ 192.168.2.110, double-NAT but mutually routable, ~8 ms).
  So p2p SHOULD be achievable — this is not a topology block.

The 15× latency gap (direct ~8 ms vs relay ~118 ms round trip) and the relay's
own capacity are the throughput ceiling. Getting onto the direct path is the
single biggest lever for both latency and speed.

### Why p2p never connects (diagnosis)

- STUN was dead in the shipping build: `openrelay.metered.ca` and
  `stun.stunprotocol.org` are defunct; every gather burned a multi-second i/o
  timeout. FIXED — replaced with `stun.cloudflare.com` + Google STUN; on-device
  STUN errors dropped from a storm to ~1.
- Even with live STUN, 0 handshakes reach `stable`. Signal-flow analysis
  (added `[signal]send`/`[signal]receive` V(1) traces): the Pixel window
  client (019f9a03) sends 37 `WaitingForSdpOffer` to the Samsung and the
  Samsung RECEIVES all 37 — but the Samsung (the active offerer for that
  stream) sends 0 offers back to it. The Samsung's 192 offers all go to a
  DIFFERENT set of peers (the Pixel's MAIN id 019f9835 + two stale ids from
  the prior Auto-mode session), none of which has a passive waiter → 0
  answers → 0 stable.
- Prime suspect: the provider-side peer-connection cap
  (`WebRtcSettings.MaxPeerConnectionCount` = `MemoryScaledCount(32, 8)`, as
  low as 8 on a phone) plus the memory-budget admission in
  `WebRtcManager.newP2pConn` REFUSE new setups when stale/wedged conns from
  the earlier session hold the slots — and the refusal in `P2pTransport.run`
  was SILENT (bare retry with no log). Instrumented now: `[p2p]…setup
  refused = …`. A provider whose slots are held by never-completing conns
  starves real streams onto the relay with no visible signal.

### Instrumentation added this pass

- `[signal]send ->` / `[signal]send failed` / `[signal]receive from` (V(1))
  in `ClientSignalSender.SendSignal` + `clientSignalReceiver.handleControlFrame`
  — the full p2p signal-delivery trace.
- `[p2p]…setup refused = <err>` (info) in `P2pTransport.run` — surfaces the
  previously silent cap/budget refusal.
- `[r]drop/upgrade older sequence` elevated V(2)→V(1) (rare, cheap; the
  sender-fork tripwire from §16).

### Synthetic speed test (new, ip_synthetic_speed.go)

To drive a repeatable packet rush isolated from origin/network variability,
the provider NAT now terminates TCP flows to the RFC 2544 benchmark range
198.18.0.0/15 at an in-memory HTTP/1.1 server (`EnableSyntheticSpeed`, on by
default; the range is reserved and never publicly routable). `GET
/download/<bytes>` streams patterned bytes, `POST` sinks an upload,
`GET /ping` is a 1-byte flow-setup probe. The full tunnel path — tun, per-peer
encryption, transfer sequences, transports, provider NAT — is exercised; only
the upstream internet hop is replaced. Verified end-to-end through the peer
(provider logs `[init]tcp connect synthetic 198.18.0.1:80`).

### Root cause CONFIRMED and fixed (2026-07-25)

The `[p2p]…setup refused` instrumentation nailed it: every p2p setup on the
provider (and the client's window clients) is refused with

  `peer connection memory budget exhausted (149796 < 524288)`

The WebRTC peer-connection admission budget was the SAME object as the
transfer receive-queue budget (`WebRtcSettings.MemoryBudget =
receiveQueueBudget`, at all four wiring sites in device_local.go /
device_local_provider.go). Each peer connection reserves `ReceiveBufferSize`
= 512 KiB. On a 20 MiB device target the provider share is 4 MiB → receive
queue budget ~1.14 MiB, and during a download the receive queue legitimately
consumes it (available fell to ~146 KiB). So a 512 KiB reservation could
NEVER be admitted while traffic flowed — the exact moment p2p is needed. A
catch-22: no p2p → all traffic relays → receive queue stays busy → p2p stays
refused. Even idle, one 512 KiB reservation barely fits and two never do.
Result: **p2p never connects; 100% WAN relay; 0.54 MiB/s.**

Fix (sdk):
- **Dedicated p2p admission budget** (`deviceLocalWebRtcBudget`), separate
  from the receive queue, sized `max(share/8, 4×buffer)` — client ~1.75 MiB,
  provider 512 KiB. It gates admission only; the SCTP memory a formed
  connection uses is unchanged, so no steady-state footprint is added beyond
  connections that actually establish.
- **Phone-sized SCTP receive buffer** `deviceLocalP2pReceiveBufferByteCount`
  = 128 KiB (was 512 KiB). A same-premises ~8 ms LAN link has a BDP well
  under 128 KiB even at hundreds of Mbit/s, so this costs no p2p throughput
  while quartering the per-connection reservation — the dedicated budget now
  admits ~4-14 concurrent peers.
- Applied at all four wiring sites (client default + override + window-client
  generator + provider), guarded to mobile (`0 < share`); desktop/server keep
  the 512 KiB unbudgeted default.
- STUN servers replaced (dead openrelay/stunprotocol → cloudflare + google).
- Regression: `TestDeviceLocalSettingsMemoryTarget` now asserts the dedicated
  budget (≠ receive queue) admits ≥2 peer connections.

Measured verification: (pending device redeploy — see the addendum below)

### Measured after the budget fix (2026-07-25)

- **Throughput: 0.54 → 0.79 MiB/s (+46%)** on the relay path (reduced memory
  pressure + phone-sized p2p buffers). Still relayed — see below.
- The budget refusal changed from `(149796 < 524288)` to
  `(0 < 131072)`: the reservation is now the phone-sized 128 KiB and several
  fit the dedicated budget, but the admitted p2p connections still don't
  complete the ICE handshake (0 `stable` on either device).

### Remaining lead (NOT yet fixed): p2p ICE rendezvous in the resident/window
### architecture

With admission fixed, p2p still never reaches `stable`. Signal-level trace
(pion vmodule=2) shows the deeper failure:

- Both devices only ever reach `have-local-offer`; NEITHER reaches
  `have-remote-offer`. Both the client and the provider act as ACTIVE
  offerers for the peer connection — the offers land on the peer's ACTIVE
  conn (which ignores SdpOffer by design) instead of a PASSIVE waiter.
- 16 `[signal]miss` on the provider: signals arrive keyed
  `{PeerId: source.SourceId, StreamId}` with no matching peerConn — the
  passive waiter was torn down (window/resident churn) or never created for
  that (source, stream).
- The client's offers to the provider-side ephemeral residents (019f9a…)
  send successfully (`[signal]send failed` = 0) but the residents log 0
  `[signal]receive from <client-main>` — the offer does not reach the
  intended passive peerConn.
- Net: only 1 SdpOffer / 1 SdpAnswer were ever processed across the whole
  session; the rendezvous is racy and mostly misses.

This is a distinct bug from the budget starvation, rooted in how the
multi-client window clients and their per-connection residents assign
(peer, stream) identities and route ExchangeSignals — the ephemeral ids churn
faster than an offer→answer→ICE→datachannel handshake completes, and the
active/passive role for a given (peer, stream) does not consistently pair.
Fixing it likely spans the server-side resident/stream setup (id stability +
role assignment) and is scoped as follow-up. Until then, network-peer traffic
between double-NATed devices rides the WAN relay.

Instrumentation kept in-tree for the follow-up: `[signal]send/receive`,
`[signal]miss` (V2), `[p2p]…setup refused`, and the RFC-2544 synthetic speed
server (`EnableSyntheticSpeed`, on by default) for repeatable measurement.

### Rendezvous fix landed + verified (2026-07-25); final blocker isolated

Applied the candidate fix — the active p2p side now sends its SdpOffer with
`ForceStream()` (transport_p2p_webrtc.go `sendSignal`), matching the stream
contract the network-peer data rides. On-device result: the initiator now
reaches `have-remote-offer` and `stable` (0 → 2), i.e. the SDP offer/answer
completes where before every offer to the ephemeral provider client was
undeliverable. The §16-class alignment holds: a send-sequence-keying option
(ForceStream) invisible to the signaling layer must match the data path.

But p2p STILL does not connect end-to-end, now for a DIFFERENT and final
reason: **0 ICE candidates are gathered on Android.** pion logs "no usable
interfaces found for mDNS" and emits no host/srflx candidates, so no
candidate pair is ever checked (ICE never leaves gathering). Root cause: the
Android app builds the VpnService with `setUnderlyingNetworks(null)` and
passes NO socket-protect callback into the SDK, so pion's ICE UDP sockets are
never `VpnService.protect`'d — inside the VPN app they cannot bind to / see
the physical wlan0 and only the excluded tun is visible. The egress code even
assumes "Android VpnService.protect solves this at the OS layer"
(egress.go) — but nothing protects pion's Go-created sockets. **Android p2p
has therefore never gathered candidates; all peer traffic has always fallen
back to the WAN relay.** (Direct UDP between the two devices works from the
shell, confirming the topology supports it — only the in-app ICE sockets are
blocked.)

Final blocker fix design (multi-repo, scoped as the next task; too large to
land + validate safely in this pass because it touches the core tunnel):
1. Android: a protect callback `fd -> vpnService.protect(fd)` (and expose the
   underlying `Network`), passed into the SDK at device construction.
2. SDK: plumb it to connect's `WebRtcSettings` (a socket-control hook).
3. connect: an Android `transport.Net` (mirroring `egressNet`, currently
   Windows-only) installed via `SettingEngine.SetNet`, that protects each ICE
   socket fd on creation so it egresses wlan0 instead of the tun.
Then the budget + STUN + rendezvous fixes already landed let p2p complete, and
device-to-device drops from the ~118 ms WAN relay to the ~8 ms direct LAN path
(the latency + throughput win).

### Net delivered this pass (measured)
- Relay-path throughput **0.54 → 0.79 MiB/s (+46%)** (budget + phone p2p buffer
  sizing reducing memory pressure).
- p2p unblocked through admission (budget), reflexive candidates (STUN), and
  SDP rendezvous (ForceStream, 0 → 2 `stable` on-device) — the full chain up
  to the Android socket-protect blocker, which is now precisely isolated with
  a concrete fix design.
- Synthetic RFC-2544 speed server for repeatable in-tunnel measurement.

## 18. Final transport-stall and SCTP release pass (2026-07-28)

### 18.1 A canceled platform transport could retain a blocked write

The retained iOS NetworkExtension trace showed a logical client disappearing
at 11:42:07 while the old connection's TCP batch write did not return until
11:42:16. Packet ownership was already removed from the route manager, but H1
teardown performed this dependency order:

```text
remove route → cancel context → join writer → deferred socket close
```

Context cancellation cannot interrupt a `net.Conn.Write` already blocked in
the kernel. Socket close was the writer's release operation, yet it was
deferred until after the join waiting for that writer. The old generation and
its bounded queues could consequently overlap a reconnect for the complete
write deadline. H3 had the adjacent inversion when a peer stopped reading and
QUIC stream credit parked `Stream.Write`.

Both paths now close the connection before joining the writer:

```text
remove route → cancel context → close WebSocket/QUIC → join → final drain
```

`TestPlatformTransportCloseInterruptsBlockedH1Write` drives a real WebSocket
over a wrapper whose `Write` can be released only by `Close`.
`TestPlatformTransportCloseInterruptsBlockedH3Write` drives a real local QUIC
connection with fixed 32-KiB stream credit, stops the server reader, fills the
client's bounded route, and then closes the transport. Both passed ten
race-enabled repetitions. The asserted upper bound is 500 ms, and H3 normally
closed in about 45 ms locally. Comparing the physical trace with the test
bound gives at least an 18× reduction in this teardown tail; it is a lifecycle
bound, not an end-to-end throughput multiplier.

### 18.2 Callback backpressure no longer owns global sequence maps

The send, receive, and forward callbacks remain intentionally synchronous
backpressure. The defect was the lock domain surrounding that contract:

- `SendBuffer` held its global sequence-map mutex while closing a sequence.
  Close drains queued `SendPack`s and invokes their completion callbacks. A
  parked callback for one sequence therefore blocked unrelated destination
  lookup/creation.
- `ReceiveBuffer.Pack` held its global sequence-map mutex while canceling and
  waiting for an older source generation. That worker may be parked in the
  receive callback, so a same-source ordering wait blocked every other
  concurrent source's map access.

Send cleanup now removes exact, wire, and destination indexes atomically,
unlocks, and closes/drains the sequence afterward. Receive replacement cancels
under the lock, waits outside it, then conditionally removes only the exact old
generation and retries. The head is deleted only when it still references
that generation; a missing map entry heals a stale head rather than panicking.
Forward cleanup also calls sequence close outside its map lock.

The intended pressure remains:

- send close does not finish until its completion callback returns;
- a newer receive generation for the same source cannot pass the old callback;
- the serial packet receive loop does not dispatch around a blocked data
  callback; and
- no callback timeout, drop, or asynchronous lifetime escape was introduced.

The benefit is isolation of unrelated concurrent sequence bookkeeping, not
removal of data backpressure. The two top-level regressions park the callback,
prove the same operation remains blocked, prove an unrelated map operation
finishes within 250 ms, release the callback, and verify ordered completion.
They passed 20 normal and 20 race-enabled repetitions alongside the
wire-indistinguishable send-sequence and receive-rejection tests.

### 18.3 SCTP 1.11.1 closes adjacent idle/resume failure modes

The release graph now uses Pion WebRTC 4.2.18, ICE 4.4.0, SCTP 1.11.1,
interceptor 0.1.47, and RTP 1.10.5. The SCTP patch is relevant to the observed
failure class:

- a write-loop error closes the underlying transport, releasing a read loop
  that could otherwise wait forever after a one-sided failure;
- cumulative and gap SACK ranges are validated before the transmit
  acknowledgement point or queues are mutated; and
- pending DATA, partial acknowledgements, late/crossed shutdown, and graceful
  shutdown advance through a consistent drain boundary.

The main SDK was initially aligned while its independent `build`, `cgo`, and
`js` modules still selected the old patches. Those artifact graphs and the
local Connect/SDK/server/proxy/validator consumers are now aligned. A new SDK
top-level regression compares all Pion versions in every artifact `go.mod`
against the SDK root; it passed 20 normal and 20 race-enabled repetitions.

### 18.4 Congestion sweep confirms the retained knee

The opt-in CA-step harness was rerun for 203.7 s after the dependency update.
Single-run measurements were:

| CA setting | 1% loss MiB/s (p50/p95) | 50 Mbps shallow queue | 8 Mbps shallow queue |
|---|---:|---:|---:|
| 4 MTU | 0.54 (75/845 ms) | 3.03 | 0.82 |
| 6 MTU | 0.58 (75/1100 ms) | 4.10 | 0.75, ~1 s tail |
| **8 MTU** | **0.60 (52.8/97.6 ms)** | 2.47, one-MTU collapse | **0.80**, ~110 ms tail |
| 10 MTU | 0.62 (25.6/99.8 ms) | 3.45 | 0.70, ~1 s tail |
| 12 MTU | 0.66 (75.3/76.5 ms) | 4.00 | 0.71, ~1 s tail |
| 16 MTU | 0.79 (75.7/1070 ms) | 4.45 | 0.73, ~1 s tail |
| 24 MTU | 0.82 (54.1/76.1 ms) | 4.57 | 0.72, 390 ms p50/~1 s tail |
| 8 MTU + 32 KiB floor | 0.79 (25.5/77.5 ms) | 3.26 | 0.82, ~110 ms tail |
| 16 MTU + 32 KiB floor | 0.83 (53.8/76.7 ms) | 3.94 | 0.76, 380 ms p50/~1 s tail |

This one-shot token/queue phase remains noisy—the 8-MTU 50-Mbps collapse is
why production selection uses the repeated paired matrices in
`OPTIMIZENETWORKPEER1.md §F.1`, not the best cell from one run. The new sweep
confirms the first-principles tradeoff:

- larger steps recover faster from exogenous independent loss;
- the same aggression overshoots real shallow queues and increases RTO pauses;
- a minimum congestion window improves the loss row by sending despite a real
  congestion signal; and
- 8 MTU retains the best measured cross-regime knee with stock one-half
  decrease and no forced floor.

No SCTP production constant changed. Physical end-to-end steady throughput
therefore remains 4.8–5.9 MiB/s (about 1.0× the old physical range), while the
controlled selected-peer receive-window result remains 2.26→28–29 MiB/s
(about 12.5×). The new wins are bounded teardown, callback-stall locality, and
upstream association reliability rather than another composable speed
multiplier.

### 18.5 Validation and remaining physical gate

The complete post-change Connect tree passed in 479.471 s, with `blocker`,
`connectctl`, and `extender` also green. The SDK passed in 382.483 s; its
build/C-binding modules, proxy, and local validator suites pass. Static
analysis passes across Connect, SDK and native artifact modules, server,
proxy, and the validator. All added regressions are top-level tests and no
ordinary `t.Run` was added.

The remaining measurement is the latest-build bidirectional
iPhone↔Pixel cold multi-origin, DNS/TTFB/load, idle/resume, CPU, and footprint
matrix. The iPhone is currently unplugged. The last installed iOS build did
not establish the extension after reinstall/permission state changed, so no
latest-build physical throughput result is inferred from older successful
captures.

### 18.6 Network-contract rollover is now relationship-scoped and bounded

A long real-page sequence found that the remaining apparent successor pause
was not SCTP. Contract creation prefetches the next contract, but the global
janitor expired any queued result after 120 s. A low-rate sequence could keep
its current contract open longer than that, then block on a new control round
trip at the eventual boundary.

The initial sizing change also exposed an identity-domain mistake. Marking the
selected top-level peer ID did not mark provider return traffic, whose
destination is an ephemeral per-window client ID. The provider continued to
receive only 13,107 usable bytes from a nominal 16 KiB contract.

The retained implementation carries an authenticated, local-only
`NetworkPeer` policy bit:

```text
selected multi-client default ─┐
                               ├→ SendSequence → ContractKey
ProvideMode_Network return ────┘
                                  ├→ 1 MiB first contract
encrypted-control carrier ────────┘   + live-successor retention
```

The bit is independent of `ForceStream`: public direct streams can force a
stream without receiving no-escrow Network sizing. It is also absent from
send-sequence/wire identity, preventing a local sizing policy from forking two
receiver-indistinguishable sequences. Encryption-control first flights mirror
the policy. Non-Network companion returns clear both Network policy and
`ForceStream`.

An open Network contract is now the ownership lease for its prefetch. The
janitor keeps the newest stale successor while that exact key is open; if a
fresh result exists it keeps no stale entry. Close/sequence teardown removes
the lease and flushes the queue. `ResetLocalStats` retains the open maps
because they are operational ownership state. Thus a live rollover is
immediate without turning delayed create results into unbounded memory or
unused-contract retention.

Physical signed-build evidence (`versionCode=1004819970`):

- Samsung→Pixel opened Wikipedia, idled 130 s, then loaded Mozilla, GitHub,
  and Guardian. The first provider-return contract reached
  837,513/838,860 usable bytes after about 290 s and installed its successor
  about 1.1 ms later. No ≥1 s `contract wait` was logged.
- The four complete loads were 2.490, 12.185, 27.292, and 11.007 s.
  Guardian had timed out beyond 90 s in the preceding build; GitHub was
  44.06 s.
- Pixel→Samsung crossed 838,860 usable bytes in both request and return
  directions. Successors appeared about 1.4/1.6 ms later, again without a
  slow wait. Wikipedia and Mozilla loaded in 2.152/7.545 s.

GitHub retained a 12.46 s p95 request TTFB under 108-way request parallelism
after rollover on the large second contract. Guardian then loaded in 11.01 s.
That remaining tail is parallel-flow/transport scheduling, not contract
acquisition.

Top-level sizing, generated-identity, carrier, ownership, public-expiry, reset,
and bounded-retention tests passed 100 ordinary and 20 race repetitions.
Deterministic Connect excluding its two randomized PT stress cases passed in
362.173 s; those cases passed separately in 63.46/65.33 s. Subpackages, vet,
Android release unit tests, lint, SDK bind, assembly, and the physical install
pass.
