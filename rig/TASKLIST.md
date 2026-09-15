# urnetwork throughput and reliability — investigation task list (spec)

Living spec. Each task states a hypothesis, the instrument, a decision rule fixed BEFORE measuring,
and a status. Results and every attempt go to LEDGER.md (section P onward); this file keeps only
status and a one-line outcome. Read LEDGER.md "DO NOT RETRY" before adding a task.

Global rules for every task
- Interleave arms, alternate order, fresh provider per run, zombie counter 0 before/after,
  md5 of the EXECUTED client and provider binaries in every result line.
- Noise floor: 8-flow A/A range 14%; flight-gate A/A paired SD 20%. A throughput claim needs a
  stable paired sign in every repetition AND an effect beyond 10-15%, or >=20 repetitions for a rate.
- Product-like path = kernel-TUN client. A socks-only effect is a harness effect.
- Every fix: deterministic test that fails before and passes after, mutation check for plausible
  false fixes, full suite on macOS + Linux, beta/custom-server first, upstream PR only after rig proof.
- The relay (`$PLATFORM_DOMAIN`) is shared: read-only, never restart. Other hosts: full permission.

Status legend: TODO, ACTIVE, DONE (outcome), BLOCKED (reason), DROPPED (reason)

## P0 — product impact, tractable on this rig

### T1 Upload: the provider upstream socket still forces SO_SNDBUF (Fix A's pattern, other direction)
- Hypothesis: `configureUpstreamTcpConn` calls `SetWriteBuffer(MaxWindowSize)` after connect. An explicit
  SO_SNDBUF sets SOCK_SNDBUF_LOCK and disables send autotuning; on a stock kernel (wmem_max 212992) the
  buffer is locked at 425984, which caps upload to an origin at larger RTTs.
- Instrument: upload through a pinned provider to a real sink (kernel socket, not the synthetic server)
  at a nonzero RTT; provider `ss -tinm dst <sink>`: skmem tb, `notsent`, `sndbuf_limited`; throughput A/B
  control vs SetWriteBuffer removed; stock wmem_max and the rig's raised value.
- Decision rule: fix candidate iff (a) sndbuf_limited is a large share of busy time on control and ~0 on
  the change, AND (b) upload throughput improves beyond 15% with a stable paired sign (n>=4) at the RTT
  where (a) holds, AND (c) no download regression. Otherwise record the null and close.
- Status: DONE (data, not shipped) — stock wmem_max: upload 33-37 -> 215-226 Mb/s (6x, 4/4); raised wmem_max: pin can win (331 vs ~220); download unaffected. Dev team owns the conditional pin rule.

### T2 Localize the ~0.25% Transfer item loss on the all-TCP relay path
- Hypothesis: frames are dropped after the provider SendSequence writes them and before the client
  ReceiveSequence, most likely at a non-blocking handoff (provider route/transport queue or relay
  forward queue). Gap recovery costs ~6% head-blocked time and drives TUN resends (238/s).
- Instrument: provider-side counters for frames accepted by the route writer vs written to the websocket
  vs dropped (existing receiveStats/queue drop counters + diag); client counters for frames read from the
  websocket vs delivered to ReceiveSequence; sequence-number gaps at each end.
- Decision rule: the hop whose in != out by >= the client-observed gap rate owns the loss. If provider and
  client both show in == out, attribute to the relay (read-only) and stop.
- Status: DONE (relay) — true loss 0.39-0.59% of first sends (resends - duplicates, 4/4 runs), steady per second; provider route writes 0 errors, every client pre-receive drop site 0. Candidate: relay processClientForward non-blocking 4096-msg forward queue (ForwardTimeout 0). Dev team can confirm with forward_dropped_messages in Grafana (ledger P16 has run windows).

### T3 TUN client upstream ACK volume (kernel acks every 2 segments, 30-37k packs/s)
- Hypothesis: on the product-like path the inner-TCP ACK stream is 3x the socks harness; coalescing
  superseded pure ACKs per flow before they enter Transfer lowers upstream cost and raises download.
- Instrument: diag in the TUN client read loop that drops a pure ACK when a newer pure ACK for the same
  flow is already queued in the same batch (no data, no SACK/flags change); count ACKs in/out; provider
  inner-window waits; throughput TUN f1/f8.
- Decision rule: pursue as a product fix iff TUN f8 improves beyond 15% with a stable paired sign (n>=5)
  and resends do not rise. A null closes the ACK-volume lead.
- Status: TODO (deprioritized: UDP, with ~no upstream ACK traffic, shares the same band, so ACK volume is unlikely to be the ceiling)

### T4 Long-RTT stalls (FLIGHTGATE-REPORT §5.1-§5.2)
- Hypothesis: the stall is a control/contract or transport event (contract waits 2.4-8.2 s, transport
  down), made permanent-until-removal by the single-exit hold, not data loss.
- Instrument: distant client, provider pinned (the hold) vs contract/transport timeline: client log
  timestamps of contract wait, transport down/restored, busy_probe; relay socket logger; provider
  contract logs; count stalls per 30 s run.
- Decision rule: if every stall starts with a contract wait > 2 s or a transport down event, the cause is
  control-plane/transport and T4b (bounded recovery) is the fix target; otherwise re-open data-plane loss.
- Status: DONE (mechanism + prototype fix) — restart key race: 10/10 cut off at 0 s, window 10-20 s; persisting provide secret keys -> 0/10. Patch proposal for sn in patches/. Open: second startup race (1/10, valid key, provider reconnecting).

## P0b — added 2026-09-14 after the dev team's THROUGHPUT-REPORT (complement, do not race their fixes)

### T14 Answer the dev team's three requests + kernel behavior of the rcvbuf freeze
- WireGuard at one flow (have), provider sysctls (have), which client (both socks/userspace netstack and kernel-TCP TUN).
- Their §2.4 could not reproduce the 64 KB freeze: probe rcv_ssthresh with SO_RCVBUF set after connect on kernel
  6.1.0-9 (provider) and 7.0.0-27 (Germany), same probe program.
- Decision rule: if 6.1 pins rcv_ssthresh near 64 KB and 7.0 follows the pinned buffer, the disagreement is kernel
  version and both reports are right.
- Status: DONE — 6.1.0-9 freezes rcv_ssthresh at 64088 (251 Mb/s vs 11.7 Gbps); 7.0.0-27 does not. Response written: RESPONSE-TO-THROUGHPUT-REPORT.md

### T15 UDP through the tunnel: is the ceiling shared below TCP? (their H10)
- Instrument: kernel-TUN client, same-datacenter kernel-socket origin (the client host's public address on another
  port, reached through the tunnel), UDP download at stepped offered rates; delivered rate, loss, provider CPU.
  TCP through the same origin as the control.
- Decision rule: UDP delivered >> 650 Mb/s with low loss -> the ceiling is in the TCP/reliable-window path
  (supports the dev window theory); UDP capped near 650 -> the limit is below both (relay/transport).
- Status: DONE — UDP lands in the same ~650 band (n=4 median 629 vs TCP 643). H8 confirmed (provider kernel UDP drops 5-20%, fixed by buffer headroom). With kernel drops removed, loss moves to provider Transfer return admission; relay/transports ~0 unrecovered loss (Transfer resends hide relay drops; T2 measured them at ~0.5%).

### T16 Real-network confirmation of queue/RTT (their §3.4) from the ~100 ms Germany client
- Instrument: constant ResendQueueMaxByteCount 2/4/8 MiB on the provider with a matched client receive queue,
  synthetic 1 flow and 8 flows from Germany, interleaved; also record resends and in-flight.
- Decision rule: 4 MiB ~1.7-2x over 2 MiB with a stable paired sign confirms the mechanism on a physical 100 ms path.
- Status: DONE — TUN client at 100 ms: 2 -> 4 MiB = 1.6x (137 -> 224), plateau ~220 by 8 MiB; socks client capped by its userspace-netstack ~2 MiB inner receive window.

## P1 — correctness and fix follow-ups

### T5 Zombie release: per-source stall clock
- Hypothesis: the abandon clock restarts per item, so release takes 120-210 s. A per-source "no admission
  progress since" clock bounds it at the timeout.
- Decision rule: deterministic test (test clock) shows release at the timeout even when a slot frees
  mid-stall; rig time-to-zero after 5 kills <= ~150 s; no fresh-traffic regression.
- Status: DROPPED — superseded by the dev team's per-source ack-evidence release (THROUGHPUT-REPORT §1.3)

### T6 Close does not join release — make the gap testable
- Hypothesis: a seam at the join (or restructuring the worker to signal before exit) lets a test prove
  Close waits.
- Decision rule: a mutation removing the Wait fails the test 20/20.
- Status: DROPPED — same; their rework replaces the release worker

### T7 TUN watch item: merged tree -5% (0/4) on kernel TUN
- Instrument: n>=10 paired, no diag patches in either arm, same harness.
- Decision rule: stable negative sign AND beyond 10% -> bisect upstream commits; else close as noise.
- Status: DONE (noise) — n=10 clean: new ahead 3/10, median -2%. One same-DC idle stall in the new arm (192 Mb/s) -> T4.

### T17 Second startup race: clients stall after a provider restart even with persisted keys (P15 residue)
- Hypothesis: the new provider process is not reachable for a while after start, and contracts issued in that window strand clients.
- Instrument: provider log census of start -> first successful platform dial; stalled-run timelines; A/B of the family group's
  StandbyDelay (15 s vs 0) with time to a client's first successful fetch after restart, n=10 alternating.
- Decision rule: B faster by > 10 s in every pair confirms the standby delay as the cause.
- Status: DONE (confirmed) — every start on current code waits 15 s (connect-v4/-v6 names unprovisioned on beta; no A/AAAA in production either); first fetch 22.2 s vs 2.2 s (10/10); 2/20 clients stall. Fix: release the standby when every pin's name does not resolve (tests + mutations, macOS suite ok): rig first fetch 22.2 -> 2.2 s median, 10/10 paired; 2/10 still 16.6 s (T18). DNS provisioning is the infra-side fix.

### T18 Startup transport group rebuild (residue of T17)
- Hypothesis: the sdk builds the provider transport group with provideMode zero and rebuilds it on the first setProvideMode,
  cancelling the first standby dial; the replacement is sometimes ~15 s late.
- Instrument: count groups per start; H1 budget pending count and per-group standby state in the first 20 s; A/B with provideMode
  seeded in the constructor.
- Decision rule: seeded build removes the second group AND the 16.6 s tail (0/10 vs ~2/10 needs n>=20 per arm).
- Status: TODO

### T19 Loss recovery cost (ceiling lead from T2)
- Hypothesis: the relay's steady ~0.5% item loss is a material part of the ~650 ceiling because Transfer loss recovery is expensive.
- Instrument: injected post-route message loss on the provider (0 / 0.5% / 2%), TUN f8; then tdiag/xdiag/RDIAG at the same arms.
- Decision rule: +0.5% costs >=15% with stable sign -> pursue cheaper recovery; <5% -> close.
- Status: DONE (fix) — receiver acks: selective acks in sequence order + compress wait ends when a hole becomes provable or fills (AckGapWakeSelectiveCount=3). Production A/B: TUN f8 +18% (6/6), f1 +15% (4/4), no stalls; Germany no stall regression. Committed b7ec6b80 on beta/custom-server.

### T20 Long-RTT throughput (~100 ms Germany client) vs WireGuard and plain TCP
- Hypothesis: at 100 ms the binder is the single client websocket TCP connection to the relay (plus relay loss growing with
  window), not only the 2 MiB transfer window; the rule's 8-flow collapse at 100 ms is a symptom.
- Instrument: iperf3 plain/WireGuard calibration; websocket TCP_INFO sampling on the client; A vs rule-on arms f1/f8.
- Decision rule: a lever is a candidate iff Germany f1 and f8 both improve >= 15% (stable sign n>=4) with no same-DC regression.
- Status: ACTIVE — calibration done (plain TCP 256/731, WireGuard 47/59 because the path drops UDP at high rates; urnetwork ~130).

### T22 Dev review of #213 (THROUGHPUTFIX-PR2.md) — response items
- H1 target clamp at short R_min (window = 256 KiB floor below ~1.77 ms): WDIAG on the provider decides. Status: running (h1-* runs).
- H5 gap-ack wake gaps (cadence bound, opening hole, head-absorbed evidence): Status: agent implementing on pr/option-a (tests fail-before).
- H6 wedge boundary discrimination: TUN packet capture + SACK hole analysis on stalled runs (holefind.py). Status: running (wg1-* runs).
- Artifacts they asked for (ledger, harness, manifests): Status: agent building rig/ on branch pr/rig-harness (sanitized).
- H2 (policy bundle vs estimator), H3 (relay identity correlation), H7 (standby transitions): TODO after the above.

### T23 Deterministic simulation suite (synctest path simulator, S1-S8)
- Status: agent building on sim/deterministic-path-scenarios.

### T21 Heavy latency and multiple hops
- Instrument: netem-added delay on the client host (e.g. +100/+200/+300 ms) and multi-hop topologies (client far from relay AND
  relay far from provider), A vs levers from T20.
- Status: TODO (after T20)

### T8 Teardown generation reuse in removeClient
- Hypothesis: reusing a torn-down generation (stale receivedInbound) disables the dial-failure fast
  escape for a flow rebound onto a dead replacement.
- Decision rule: only if a deterministic test reproduces a user-visible failure (flow stuck on a dead
  exit longer than with a fresh generation). Otherwise stays a note.
- Status: TODO

## P2 — larger or externally gated

### T9 The ~650 Mb/s per-client ceiling after T2/T3 — re-baseline and next lead
- Status: DONE (re-baselined on b7ec6b80) — f1 ~880 > f4 774 > f8 710 > f16 691 (per-flow cost now dominates); bigger window on top of the fix loses (f8 -22% at 8 MiB, f1 -57% at 4 MiB) and relay loss scales with in-flight. Next leads: T3 upstream ACK volume (flow-count slope), relay forward queue (dev/server).

### T10 G1 narrower overflow rule (FLIGHTGATE-REPORT §3.1)
- Status: BLOCKED for measurement (beta relay has no QUIC listener); deterministic implementation possible.

### T11 Mobile window floor (256 KiB -> ~11 Mb/s at 190 ms)
- Status: BLOCKED (needs a real phone)

### T12 Client registration leak
- Status: BLOCKED (design decision on persisting the client JWT)

### T13 WriteBatch cross-flow ordering on the mobile tun path
- Status: DONE (no defect) — same-flow order held by per-shard write locks across the batch; groMu serializes batches (no lock-order inversion); cross-flow order is not a contract. Gap: no test exercises Tun.WriteBatch same-flow ordering directly.
