# FLIGHTGATEFIX: peer review and research plan for the pinned-provider collapse

Status: research plan, 2026-09-10, revision 3 (§12 carries the Phase 0 and
device results and the program change). Nothing in this document is
implemented in this tree. The reporter's work lives on the
`Ryanmello07/connect` fork (`beta/custom-server`) and in two upstream pull
requests, urnetwork/connect#208 (transfer and p2p) and #209 (tun). Neither PR
is merged.

Sources reviewed: the two reports ("The Unreliable Flight Gate", 2026-09-09,
and "urnetwork throughput investigation, full report and context",
2026-09-07 to 2026-09-10); the diffs of #208 and #209; `transfer_flight.go`,
`transfer.go`, `transfer_rtt.go`, `transfer_route_manager.go`,
`transport_p2p.go`, `transport_p2p_fast*.go`, `transport_p2p_probe.go`,
`transport_p2p_webrtc.go`, `ip_remote_multi_client.go`, `tun.go`,
`LOWBAR.md`, `server/connect/perfvar`, `server/proxy/proxy_device.go`.

## 1. Verdict

The reporter has found a real transfer-layer defect and, across ten rounds on
one rig, has also found most of the second-order mechanisms that my first
review flagged as untested. The picture now has six named mechanisms, four of
them with rig evidence and fixes in #208, two still without any test:

| Mechanism | Rig evidence | Fix proposed | Deterministic test of the failure |
|---|---|---|---|
| M1 Route-wide admission gate on a per-lane flight | yes (`flight{lim=true}`, `UFLIGHT wait`, idle h1) | F1/F2 in #208 | none; #208's tests are fix contracts |
| M2 Acks for p2p-received Packs pinned to the p2p lane, dropped at the provider's kernel UDP receive buffer | yes (`Udp6RcvbufErrors` +934 to +7,916 per run, 0 after F6/F7) | F6, F7 in #208 | none |
| M3 Cross-lane reordering read as loss by the selective-ack scoreboard | yes (32k to 160k gap resends per run, 904 to 3,542 after F11b) | F11b in #208 | none |
| M4 One RTT window fed by both lanes drags the relay's RTO to the floor | yes (`rsTO` bursts over 1,000 per 2 s) | F10 in #208; F12 unvalidated, uncommitted | none |
| M5 RTP fragment loss amplifies packet loss into message loss; the lane never grows past about 23 KB | indirect (F9 inert, "halved on every loss") | none | none |
| M6 The fast path has no liveness signal; a dead lane is never retired and the flight never resets | indirect ("data plane stops delivering", ICE consent expires 30 s later) | none | none |
| M7 Tun reentrancy on hosts whose tun reader also injects (socks client, and the hosted server proxy) | yes (goroutine dumps) | F5 in #209 | #209 tests the bound, not the cycle |

The A/B results are credible for the rig: 47 fixed runs without a collapse,
against 9 of 18 stock. They are not attributable. #208 bundles six behaviour
changes in three commits and the rounds added them cumulatively, so the
campaign cannot say which mechanism dominates on a phone, and two of the
changes (F1's gate removal when H1 is active, F6's ack policy) reach into the
mobile low-bar regime that the flight controller was built for and that the
rig does not exercise. The plan below keeps every mechanism, adds the missing
failure tests, splits the PRs into attributable candidates, adds the mixed
carrier campaign that PERFVAR lacks, and gates each candidate on the existing
low-bar matrix.

## 2. Claims verified against this tree

| Claim | Verdict | Where |
|---|---|---|
| The gate is global; only acks release tracked items; loss only halves | Confirmed | `transferFlightPolicy().limited = unreliableTransferPath`; `flightEligible` before route choice; `releaseUnreliableFlight` only from `receiveAck`; `reduceForLoss` |
| Only writes that actually use the datagram lane are tracked | Confirmed | `writeDisposition` via `unreliableForMessageByteCount = FastPathReady()`; LOWBAR 2026-08-18 |
| p2p is first in every write; h1 only takes overflow | Confirmed | writer match state is weighted; p2p `RouteWeight` 1.0 leaves the rest 0; p2p send channel is 4 deep, so every h1 write in a stock trace means the p2p send goroutine was behind |
| Direct mode is forced on for a pinned own-account provider | Confirmed | `overrideAllowDirect`: `ProvideMode_Network` forces `AllowDirect` |
| Readiness needs one echoed probe and no quality threshold | Consistent | `p2pStreamProbe.setSendRoute` publishes on the ready-header exchange; no RTT or loss rule exists |
| Acks reply on the carrier that received the Pack | Confirmed, sharper | eligible set for a p2p-received Pack is `[p2p]` only (nothing outranks priority 0), written by one serial ack worker per receive sequence with a 15 s timeout; so a full p2p channel also head-of-line blocks every later ack |
| connect never sets ICE UDP socket buffers | Confirmed | no `SetReadBuffer`/`SetWriteBuffer` in `transport_p2p*.go` |
| Gap after three later selective acks, regardless of lane | Confirmed | `SelectiveAckGapThreshold` 3; `scheduleSelectiveAckRecovery` has no lane awareness |
| One RTT window, mean times scale with a floor | Confirmed | `RttWindow.scaledRtt`, one per `SendSequence`, fed from every tagged ack |
| ICE consent expiry about 30 s | Confirmed | `DisconnectedTimeout` and `FailedTimeout` 30 s; `SctpNoProgressTimeout` 10 s exists for the SCTP lane only |
| `ReliablePackHandoffTimeout = -1` | Confirmed | default in `transfer.go`; an unbounded wait inside the reentrancy cycle |
| Flight floor 8 KiB / 8 messages | Partly | byte floor 8 KiB; message floor 4 (initial 8); p2p caps the maximum at its queue minus a 16 KiB reserve |
| p2p loses 25 to 30 % of writes | Unverified | data-plane counters were nil until the f4 diagnostic; fragment loss (M5) can produce that from low packet loss |
| The phone has the same bug | Gate and ack policy yes; the rest extrapolated | `DeviceLocal` uses the same `Client`; no device run exists |

## 3. Mechanisms, in the order the source suggests they act

M1, M2, M3, M4 and M7 are described accurately by the reports. Two
additions from the source.

M5, fragmentation. `writeMessage` splits a Transfer frame into 1,188-byte
RTP packets; one lost fragment loses the message; the reassembler holds 64
slots keyed by message id modulo 64 with a 2 s expiry. Message loss is
`1 - (1 - p)^n`. A coalesced 16 KiB Pack is 14 fragments and is lost 35 % of
the time at 3 % packet loss. This is why the lane never grows: the window
opens, Packs get bigger, message loss rises, the window halves. F9 (bigger
lane windows) was inert for exactly this reason. Nothing in #208 addresses
it; F2c hides it.

M6, liveness. `FastPathReady` is codec bound plus receive ready and never
turns false while the track stays bound. The SCTP lane has a no-progress
watchdog; the RTP lane has no acks and no watchdog. "The data plane stops
delivering while probes keep passing" is therefore expected, and the flight
is never reset by a route generation change because the route is never
withdrawn. F1 makes the sequence survive a dead lane by routing around it;
it does not retire the lane, so every Pack the weighted shuffle still hands
to p2p is lost until ICE consent fails 30 s later.

## 4. Review of the pull requests

#208 (`transfer.go`, `transfer_flight.go`, `transfer_route_manager.go`,
`transport_p2p_udp_batch.go`, `transport_p2p_webrtc.go`,
`transport_p2p_webrtc_pc.go`, one test file; 954 diff lines). #209 (`tun.go`,
one test file; 250 diff lines). Findings, most important first.

1. RTO release grows the window. `observeUnreliableResendTimeout` calls
   `reduceForLoss` and then `releaseUnreliableFlight`, which is
   `acknowledgeForKey`: it credits the bytes as delivery evidence and runs the
   additive increase. Every RTO therefore halves the limit and then grows it
   by `increaseByteCount * bytes / byteLimit`. Small, but it is the wrong
   primitive, and it also clears the flow reserve as if delivered. Needs a
   `forget` on the controller that removes bytes and messages without
   growth.

2. F6 reverses a measured LOWBAR win on mobile. `writeDetailedReplyWithCarrierPreference`
   never pins a reply to a potentially unreliable carrier while a reliable
   one is active. Hybrid H3 publishes `Unreliable`, and mobile Auto usually
   has H1 and H3 active together, so on a phone every ack for a
   datagram-received Pack now goes to H1. LOWBAR measured newest-covered-Pack
   ACK affinity at 22.1 % faster tunneled completion on the repaired
   1,100-MTU Auto download and 7.5 % faster on the upload control. The rig
   never runs that regime. The policy must be scoped to the actual problem:
   an unreliable route whose channel is full or whose lane has no ack
   progress, or p2p specifically, not "any potentially unreliable carrier".

3. F1 changes the low-bar regime too. `reliableRouteAvailable` counts
   routes whose transport is not `Unreliable`, so hybrid H3 does not count and
   an H3-only client keeps the gate. But with H1 also active the datagram
   gate switches off and the overflow goes to H1 TCP on the same constrained
   link, which is the burst the flight controller exists to prevent (LOWBAR
   2026-08-17: the initial DATAGRAM flight overran the 13-packet cell-edge
   queue). The PR was verified with `go test ./` only. It must run the static
   low-bar matrix (`cell-edge-*`, `exchange-auto` and `exchange-h3`, mobile
   surrogate) before it can land.

4. Attribution. #208 lands F1, F2, F6, F7, F10, F11b and the off-by-default
   F2c together. Rounds 1 to 9 measured them cumulatively and interleaved
   against the previous round, so the rig knows each step helped on the rig
   but not which mechanism carries the phone. Split into one candidate per
   mechanism (§7) and measure each on the PERFVAR mixed route.

5. F7 sizing on mobile. 4 MiB send plus 4 MiB receive per ICE UDP socket,
   and ICE gathers one socket per candidate. The kernel clamps on Linux
   (`rmem_max` 212,992 by default, so the request is inert on a stock VPS
   after the rig's unpersisted sysctl is lost), but the SDK sets no such
   limit on iOS or Android and MEMSTEADY tracks process footprint. Needs a
   platform default (for example 512 KiB) and a MEMSTEADY run.

6. `lateNotLost` is sound but leans on F10. It compares a reliable-carried
   item's age with `ScaledRtt`, which after F10 describes the reliable lane.
   Without F10 the same test would skip nothing. Keep them in one candidate.

7. #209 fixes the symptom, not the cycle. The unbounded wait was added
   2026-08-02 for backpressure. With #209 the injecting goroutine still waits
   250 ms while holding the TCP inbound shard `writeLock`, so every reentrant
   RST stalls that shard's inbound writes by 250 ms; the timer is created once
   per call and never reset, so a later wait in the same batch can expire
   early; and a drop returns no error, so gVisor believes the packet left.
   The dump also shows `Client.run` blocked in `ReceiveSequence.Pack` with
   `ReliablePackHandoffTimeout = -1`, a second unbounded wait in the same
   cycle that #209 does not touch. The hosted server proxy
   (`proxy_device.go`: `ReadBatch` then `SendPacketsNoCopy` on one goroutine,
   receive callback does `WriteBatch` into the same tun) has the same shape,
   so this is a production defect, not a rig artifact. The fix is to make
   race-commit delivery asynchronous (R1); #209 can stay as a counter-backed
   guard whose drop count must read 0 in every campaign.

8. The PR tests are contracts for the fixes, not reproductions of the
   failures. None of them fails on the current tree for the collapse itself.
   The tests in §5 are the regression guards.

9. Not in the PRs but in the report: every test run leaks about two socks
   client registrations (clients that die without clean shutdown). Separate
   product bug; track it, do not fold it in.

## 5. Deterministic tests of the failures

Built on `newTransferFlightTestClient` (route channels, the
`beforeResendCapacityWaitForTest` barrier, `SendRecoveryStats`), the vnet
WebRTC factory (`newVnetWebRtcPeerConnectionFactory`) and
`testingNewMultiClient`. Names are proposals. Tests 1, 3, 5, 7, 9, 11 are
expected to fail on the current tree.

1. `TestSendSequenceUnreliableFlightDoesNotGateReliableSibling` (M1).
   Unreliable route never acked, reliable route acked. Pack 2 must reach the
   reliable route without the wait barrier; `UnreliableFlightWaitCount` 0.
2. `TestSendSequenceUnreliableFlightTracksOnlyUnreliableWrites` (M1
   guard; passes today).
3. `TestReceiveSequenceAckAffinityDoesNotHeadOfLineBlock` (M2). p2p-received
   Pack A, h1-received Pack B, p2p route channel full: B's ack is written
   within one write timeout of the block, not after it.
4. `TestReceiveSequenceAckFallsThroughWhenUnreliableIsFull` (M2 fix
   contract) and `TestReceiveSequenceAckKeepsHybridH3Affinity` (guard for
   finding 2: with H1 and hybrid H3 active and H3 healthy, acks keep H3).
5. `TestSendSequenceReorderingAcrossCarriersIsNotLoss` (M3). Two lossless
   routes, one delayed by N Packs: no gap recovery, no reduction. Then one
   real drop: exactly one gap recovery.
6. `TestSendSequenceRttWindowDescribesReliableLane` (M4). 20 ms acks on the
   unreliable route, 200 ms on the reliable one: `ScaledRtt` must not fall
   below the reliable lane's RTT times the scale.
7. `TestSendSequenceQueueInflatedRelayRttDoesNotFireWholeWindowTimeouts`
   (M4, F12). Reliable acks delayed by a growing queue with cumulative
   progress each RTT: whole-window timeout resends must stay at zero. Defines
   what F12 has to prove before an A/B.
8. `TestFastPathMessageLossFollowsFragmentCount` (M5). vnet loss 1 % and
   3 %, messages of 1, 2, 8, 14 fragments; measured message loss within
   tolerance of `1-(1-p)^n`; reassembler evictions recorded.
9. `TestFastPathBlackholeRetiresRouteAndResetsFlight` (M6). Mirror of
   `TestWebRtcIdleResumeSctpBlackholeReconnects` for the RTP lane: blackhole
   SRTP after readiness with STUN consent alive; the send route must be
   withdrawn within a configured no-progress timeout and the flight
   generation must change.
10. `TestSendFlightControllerForgetDoesNotGrowWindow` (finding 1).
11. `TestTunInjectFromReaderGoroutineDoesNotDeadlock` (M7). Real `Tun`,
    outbound queue full, `Write` a SYN to a closed port from the reader
    goroutine: must return within 1 s. Run against R1 (no drop) and against
    #209 (drop counted) to characterise both.
12. `TestMultiClientRaceCommitDeliversAsynchronously` (R1 contract). Receive
    callback blocks on a channel; `SendPacket` still returns.
13. `TestP2pReadinessRequiresProbeQuality` (P1, product gate). Two echoed
    probes with 200 ms RTT against a 20 ms platform path: the route is not
    published; withdraw when measured loss exceeds the threshold.

All under `-race`.

## 6. Performance tests to add

### 6.1 connect benchmarks

- `BenchmarkSendSequenceMixedCarriers`: two in-process routes, unreliable
  with configurable ack delay and drop, reliable with fixed delay; report
  Packs/s and iterations flight-blocked while the reliable route had channel
  capacity.
- `BenchmarkReceiveSequenceAckWorkerUnderCarrierBlock`: ack latency
  distribution with one affine route full.
- `BenchmarkStreamFastWebRtcRoute` sweeps over vnet loss and fragment count
  with `DataPlaneStats` on: message loss and reassembler evictions per 10^4.

### 6.2 PERFVAR campaign

`CONNECT_PERFVAR_ROUTE` is exclusive today (`p2p-fast`, `p2p-legacy`,
`exchange-h1`, `exchange-h3`, `exchange-auto`); the reports' condition, p2p
and the h1 relay active together for one pinned provider, is absent. Add:

- Routes `p2p-fast+exchange-h1` and `p2p-legacy+exchange-h1` (control).
- Profiles: `clean-lan`; focused loss on the p2p direction only (independent
  1 % and 3 %, one burst profile) with the relay direction clean; the
  reports' relay RTT of 150 to 300 ms against a 20 ms direct lane.
- Schedules: blackhole the p2p data plane at t+10 s keeping ICE consent,
  restore at t+40 s (M6, recovery time); a relay queue-inflation step (M4).
- Workloads: `tcp-parallel` download (four streams), `latency-under-load`.
- Metrics per window (15 s): throughput and dead windows under 5 Mb/s;
  `UnreliableFlight*` counters; ack route-write wait by carrier;
  `P2pDataPlaneStats` fast/legacy counts and reassembly drops; kernel UDP
  receive errors on the provider host where readable; the new counters of §8.
- Guards for every candidate: the static low-bar matrix (`cell-edge-*`,
  `exchange-auto`, `exchange-h3`, `p2p-fast`, mobile surrogate) must be
  INDISTINGUISHABLE or better, because F1 and F6 change that regime, and
  MEMSTEADY for F7.

Record under the PERFVAR RUN-MAIN contract in `tests/PERFVAR-MEASUREMENTS.md`;
five fresh repetitions, seeds recorded, candidate and control interleaved.

## 7. Candidates, one per mechanism, and their gates

| Id | Candidate | From | Mechanism | Risk | Gate |
|---|---|---|---|---|---|
| A1 | Ack write falls through to a reliable route when the affine unreliable route is full or its lane shows no ack progress; hybrid H3 affinity otherwise unchanged | F6, rescoped | M2 | none on mobile if scoped | tests 3, 4; low-bar Auto download and upload |
| A2 | ICE UDP socket buffers with a platform default | F7 | M2 | mobile footprint | MEMSTEADY; provider host sysctl documented |
| G1 | Flight gates admission only for writes that will use an unreliable lane; overflow writes reliable-only | F1 | M1 | constrained link with H1 and H3 both active | tests 1, 2; static low-bar matrix |
| G2 | RTO of a tracked item forgets it without growth and resends reliable-only when a reliable route has capacity; still halves | F2 with finding 1 fixed | M1 | loses nothing that F2 kept | test 10 |
| S1 | Reliable-carried item younger than the reliable lane's scaled RTT is late, not lost | F11b | M3 | slower loss detection on one lane | test 5; low-bar H3 datagram medians |
| S2 | RTT window fed only by reliable-carried acks; a separate estimator for the unreliable lane | F10 plus per-carrier RTO | M4, and the "lane never grows" half of M5 | none identified | test 6 |
| S3 | Defer a whole-window timeout resend while cumulative progress is recent | F12 | M4 | delays recovery on a truly stalled lane; must not defer unreliable items | test 7, then one clean interleaved A/B, at least 4 pairs |
| S4 | Size-aware unreliable admission: cap fragments per message on the fast path, or single-fragment Packs while loss is observed | new | M5 | fast path efficiency | test 8; benchmark sweeps |
| L1 | Fast path no-progress watchdog (no Transfer ack progress after outbound activity withdraws the route; same contract as `SctpNoProgressTimeout`) | new | M6 | false withdrawal under long RTT | test 9; blackhole schedule |
| P1 | Readiness granted and withdrawn on measured probe RTT and loss relative to the platform path | report §8.4 | M6 upstream | fewer direct lanes | test 13; activation rate in PERFVAR must not fall on clean profiles |
| R1 | Asynchronous race-commit delivery; bounded `ReliablePackHandoffTimeout` on hosts whose receive callback injects | report, "removal-receive queue" | M7 | first-response ordering | tests 11, 12; `tun_congestion_test.go`; server proxy soak |
| F5 | Bounded outbound wait then drop | #209 | M7 guard | changes backpressure; 250 ms shard stall | only after R1, drop counter 0 in every campaign |
| F2c | One message in flight at the floor | #208, off | symptom of M3/M5 | starves the lane | not before S1, S4 |
| F9 | Larger p2p lane windows | fork, inert | M5 | none | only after S2, S4 |
| P2 | `OverrideAllowDirect=false` for own-provider pins | report §4 | all | forfeits p2p | fallback only |

Acceptance for any candidate: its failure test passes, no existing test
regresses, PERFVAR mixed route is IMPROVEMENT, low-bar matrix
INDISTINGUISHABLE or better, MEMSTEADY unchanged where memory is touched.
Candidates are measured one at a time against the same seed set. A1 and G1
land only after their low-bar runs.

## 8. Counters to add first

On `Client`, exported through the recovery stats, so the rig, PERFVAR and a
device answer the same questions:

- `UnreliableFlightBlockedWithReliableCapacity`: iterations flight-blocked
  while a non-unreliable route's channel had space. Decides M1's share.
- `ReceiveAckWriteWaitByTransport` and `ReceiveAckWriteTimeoutByTransport`.
- `UnreliableFlightGapReorderSuspected`: gap recoveries whose missing item was
  acked before its resend was written.
- `TimeoutResendWithRecentCumulativeProgress`: the M4 cascade, and F12's
  own `TimeoutResendDeferCount`.
- `DataPlaneStats` on by default in the socks client, the server proxy and
  PERFVAR; fragment histogram (1, 2 to 4, 5 to 8, 9 to 16, 17 and more);
  reassembler evictions; last-ack age per fast route (feeds L1).

## 9. Refuted on the rig; do not re-propose without new evidence

From the second report, kept here so the list travels with the plan:
ack-credit deadlock or miscredited acks (every ledger counter zero through a
wedge); carrier-affinity pinning of ordinary resends (only replies carry
affinity); relay drops or resident replacement; provider parked in
`retryReturnSend`; contract exhaustion; larger send window or receive queue
(worse under stock, void after the fix); RTO floor 300 to 1,000 ms;
logical lanes 0 to 8 (null at n=8); leaked client registrations as a
throughput cause; CPU on either end; no-race no-client drops stranding the
sender; h1Only Pack shrink. `URNETWORK_NOSTRIPE_P2P` must not be used: it
prevents ICE negotiation.

## 10. Execution plan

Phase 0, instrumentation and reproduction. Add the §8 counters; land tests
2 and 8 (pass today) and the failing tests 1, 3, 5, 6, 7, 9, 11 tagged with
their mechanism; post findings 1 to 8 on #208 and #209 and ask for the split
in §7. Collect the reporter's ledger and dumps into the private PERFVAR
bundle.

Phase 1, attribution. PERFVAR mixed route with stock, then A1, G1, S1, S2,
L1 each alone, then the reporter's full #208. Report §8 counters per
campaign. This says which mechanism carries the phone and whether the gate
or the ack path dominated on the rig.

Phase 2, land in measured order with the low-bar guard. Expected from the
source reading: A1 (small, scoped), L1 (bounded dead lane), S2 (RTT), G1
plus G2 (the gate, low-bar gated), S1, then S3 after its own A/B, S4 if
test 8 shows fragment loss dominates.

Phase 3, the reentrancy. R1 with tests 11 and 12; server proxy soak; #209
retained as a guard only.

Phase 4, product gate and device confirmation. P1 after L1 exists to
withdraw; one Android session block pinned to a provider with p2p live,
before and after, with the same counters through the SDK.

Rig hygiene, from the report and not part of the plan's evidence: the VPS
runs an uncommitted fix8 diagnostic build; `rmem_max`/`wmem_max` were raised
and not persisted; about thirty experimental binaries remain in `/tmp` on
both hosts; the client-registration leak is a separate bug.

## 11. Open questions for the reporter

- In the wedged stock dumps, was the receive-side ack worker inside
  `writeDetailedWithCarrierPreference` with a p2p preference? That separates
  M2's head-of-line block from M2's kernel drops.
- Fragment counts of the Packs on the rig and the selected ICE candidate
  pair; a relayed pair changes the loss model.
- Any stock collapse with the fast receive counters still advancing on the
  far end? That separates M1 to M4 from M6.
- Willingness to split #208 per mechanism, and to run the static low-bar
  matrix on the fork before the split lands.

## 12. Revision 3: Phase 0 and device results, reporter feedback, program change

### 12.1 Reporter feedback on environments

The reporter: "testing in a simulated perfect environment this issue may
not show, which is why I had to host a provider that's a bit distant from
me to get an accurate baseline." Accepted, and it matches the mechanism
list: the collapse needs a direct lane that is lossy or reorders against a
relay with 150 to 300 ms of RTT. A clean LAN profile shows neither. The
PERFVAR mixed route therefore uses only the lossy direct profiles with the
distant relay; `clean-lan` on the mixed route is a control that must not
collapse. Real radios remain the primary evidence, and the third report's
regression tests (12.3) were checked against the same rule.

### 12.2 Device baseline (stream C, stock counters build)

Pixel 8 Pro and Galaxy S24 Ultra, identical diagnostic build, pinned to
each other as network peers, four-stream download, twelve 15 s windows per
run, six runs per role assignment. The direct fast path was active from the
first window in 11 of 12 runs.

| Client to provider | Median per run (Mb/s) | Dead windows (< 5 Mb/s) |
|---|---|---|
| S24 on LTE to Pixel providing on Wi-Fi | 2.5 to 3.4 | 72 of 72 |
| Pixel on LTE to S24 providing on LTE | 1.5 to 2.5 | 72 of 72 |

The same devices download directly at about 65 Mb/s on LTE and 550 Mb/s on
Wi-Fi. On a phone the gate is not a collapse after 30 to 90 s; it is the
steady state from the first second. Counter readings per three-minute run,
provider side: 6,000 to 11,000 flight waits, 98 to 99 % of them with a
reliable route that had channel capacity (M1); the binding limit was the
mobile ceiling of 16 messages in flight from `sdk/mobile_memory_policy.go`,
not the byte floor, and 16 messages of about 930 bytes per RTT is the
observed 2 to 3 Mb/s; 40 to 135 gap recoveries suspected to be reordering
and 7,000 to 22,000 selective-gap resends (M3); 200 to 5,000 flight
timeouts, 1,800 to 22,000 timeout resends, 170 to 650 blocked ack route
writes on the provider and 280 to 1,170 on the client (M2, M4); 1.7
fragments per message with up to 207 reassembler evictions per run (M5). No
lane died within three minutes, so M6 was not observed. One LTE-to-LTE run
in which the fast path never engaged still ran at 2.5 Mb/s over the legacy
SCTP lane with the flight never waiting, so a cap below Transfer also
exists on that radio pairing; relay-only and one-radio controls are
running to separate it.

### 12.2a Device controls (stream C, same build, interleaved with stock)

Relay-only (direct mode off through a debug hook) alternated with stock
from a fresh tunnel per run; one extra role assignment with the Pixel as a
Wi-Fi client against the S24 providing on LTE. Per-run medians in Mb/s:

| Roles | Stock | Relay-only |
|---|---|---|
| S24 LTE client to Pixel Wi-Fi provider | 2.9, 2.8, 2.5, 2.5, 2.4, 2.3 | 6.2, 5.4, 5.5, 2.5, 2.3, 2.5 |
| Pixel LTE client to S24 LTE provider | 1.9, 1.8, 1.6, 1.5, 1.6, 1.7 | 4.7, 4.5, 2.1, 2.0, 2.1, 2.1 |
| Pixel Wi-Fi client to S24 LTE provider | 2.6, 1.9, 2.1, 1.8, 1.9, 1.8 | 2.6, 2.6, 2.6, 2.6, 2.5, 2.4 |

Within every interleaved pair relay-only beat stock: by about 2x while the
relay path was healthy, by 0.2 to 0.8 Mb/s after the relay path itself
degraded to 2.0 to 2.6 Mb/s mid-session and stayed there while direct LTE
stayed at 61 Mb/s. Stock with the fast path live never exceeded the
relay-only run next to it: on a phone today the direct lane subtracts, it
does not add. The one stock run whose fast path never negotiated ran at
the relay ceiling of its hour over the legacy SCTP lane with the flight
never waiting, so the cap below Transfer seen there is the
cellular-to-cellular platform path, not a Transfer mechanism.

Counters: stock runs as in 12.2 (5,300 to 10,500 flight waits, 98 to 99 %
with reliable capacity, limit pinned at the 16-message mobile ceiling).
Relay-only runs: flight waits 0, gap resends 0 to 234, but still 1,000 to
17,000 whole-window timeout resends per run; the timeout machinery
misfires on the relay lane alone, which is direct device evidence for
13.5 and says its A/B must include a relay-only cell. Every direct path
was host to host over the carrier addresses (both devices on the same
carrier), one run server-reflexive, never relayed. Fast receive-queue
drops 0; one client reassembler evicted 207 incomplete messages in a run.

Consequence for the product: until 13.1 to 13.3 land, turning direct mode
off for phone-to-phone pins (P2 in §7) is the only change that improves
the phones measured here, at about 2x, and forfeits nothing they can use
today. Still open: the Wi-Fi-to-Wi-Fi ceiling of the direct lane, blocked
on the S24 joining the local Wi-Fi.

### 12.3 Third report: regression tests on the PRs

PR 208 now carries `transfer_mixed_lane_regression_test.go` (a full
unreliable flight overflows onto the reliable lane; an ack for an
unreliable-carried Pack leaves on the reliable carrier) and PR 209 carries
a real-gVisor deadlock reproduction whose second half asserts the hang with
the bound disabled. Each is stated to fail without its fix. That answers
§4 finding 8 for M1, M2 and M7. M3, M4, M5 and M6 still have no failure
test in the PRs; §5 tests 5, 6, 7, 8 and 9 supply them. The report's
deliberate non-assertion (a retransmit may ride either lane once the flight
has room) is correct.

### 12.4 Phase 0 result (stream A, branch flight-gate-fix)

Counters, thirteen tests, three benchmarks and five one-mechanism candidate
sub-branches landed. Full race suites per branch:

| Test | base | a1 | g1 | s1 | s2 | l1 |
|---|---|---|---|---|---|---|
| 1 flight does not gate reliable sibling (M1) | red | red | green | red | red | red |
| 3, 4 ack head-of-line block, fall-through (M2) | red | green | red | red | red | red |
| 5 reordering is not loss (M3) | red | red | red | green | red | red |
| 6 RTT window describes the reliable lane (M4) | red | red | red | red | green | red |
| 7 queue-inflated RTT, no whole-window timeouts (S3) | red | red | red | red | red | red |
| 9 blackholed fast path retires and resets (M6) | red | red | red | red | red | green |
| 11 tun inject from the reader goroutine (M7) | red | red | red | red | red | red |

Guards 2 and the hybrid H3 ack affinity guard stay green
everywhere; test 8 characterises M5 (a 14-fragment message is lost 32.7 %
of the time at 3 % packet loss, model 34.7 %); tests 10, 12 and 13 are
skipped until their candidate exists. Every candidate leaves the rest of
the package green. Benchmarks: the blocked ack worker delivers an h1 ack
only after the p2p write timeout, which is M2's head-of-line block in
numbers.

Design questions raised by the candidates, to be answered in the fix
design: whether G1 may overflow onto H1 TCP when H1 is a sibling of a
constrained link rather than a relay (the low-bar matrix decides); whether
A1 should also fall through on "no ack progress for T" using the per-route
ack clock rather than wait for L1 to retire the lane; S1 and S2 belong
together (S1 reads the window S2 makes lane-accurate); L1 adds an 11-byte
progress report to the fast path wire format and needs product values for
its interval and timeout plus a check that older peers ignore it; whether a
reliable resend after G2's forget should re-classify the item; and whether
the per-carrier counter maps should become fixed arrays.

### 12.5 Program change: the PRs are the starting point

User decision 2026-09-10: PRs 208 and 209 are merged into flight-gate-fix
and the merged tree is the base for the remaining work. The candidate
sub-branches stay as attribution experiments and are not merged. Phase 2
becomes: additional correctness and performance fixes on top of the merged
tree, each backed by a deterministic test from §5 and by the PERFVAR mixed
route and the device rig, with the design written here and reviewed before
it lands. The first work items on the merged tree are the §4 findings:

1. finding 1, a forget primitive on the flight controller in place of the
   acknowledge-based RTO release (test 10 made red on the merged tree);
2. finding 2, scope the reply fall-through so hybrid H3 keeps its measured
   ack affinity (the hybrid H3 guard test, plus the low-bar upload and
   download controls);
3. finding 3, the static low-bar matrix on the merged gate change, and if
   it regresses, G1's narrower rule;
4. finding 5, a platform default for the ICE socket buffers with a
   MEMSTEADY run;
5. finding 7, R1 asynchronous race-commit delivery with test 11 and 12,
   keeping 209's bound as a counter-backed guard;
6. then M4 (S3 with test 7), M5 (S4 with test 8), M6 (L1 with test 9, wire
   format reviewed), and P1.

The Phase 1 attribution campaigns in PERFVAR run against the merged tree as
the control and each remaining candidate on top of it.

## 13. Fix design on the merged tree (for review)

Base for every item is flight-gate-fix at 84f0c00: PR 208 and 209 merged,
Phase 0 counters and tests present. Items are in landing order. Each lands
alone, with its red test green, no other test red, and its guard
measurement recorded before the next starts. "Low-bar matrix" means the
PERFVAR static campaign on `cell-edge-5m-down-1m-up`,
`cell-edge-1m-down-250k-up` and `cell-edge-256k-down-64k-up` over
`exchange-auto`, `exchange-h3` and `p2p-fast` with the mobile surrogate,
INDISTINGUISHABLE or better against the merged tree as control. "MEMSTEADY"
means the connect/MEMSTEADY.md 24 MiB mobile audit on the Android session
block: goRuntimeBytes p50 and p95 at or below 24 MiB over five quiet
connected minutes after a burst, active traffic at or below 24 MiB, no
sample above 28 MiB, and no regression against the merged tree; stream C
runs it per landed item and stream B records the PERFVAR memory guardrails.
Every item keeps allocations off the per-packet and per-ack paths, and
TestFlightGateItemsAreAllocationFree plus
TestFastPathProgressReportAllocatesLikeWarmup hold that line.

### 13.1 Finding 1: forget on RTO instead of acknowledge

Mechanism M1 and M2's tail. Red test: `TestSendFlightControllerForgetDoesNotGrowWindow`.

Change. `sendFlightController` gains `forget(byteCount ByteCount, key
sendSchedulingKey, reserved bool)`: subtract `min(byteCount, self.byteCount)`,
decrement `messageCount` and `messageCountByKey[key]`, clear
`flowReserveInUse` when `reserved`; no limit, remainder or slow-start field
changes. `SendSequence.observeUnreliableResendTimeout` keeps its shape
(count the timeout, `reduceForLoss`, return whether the resend is
reliable-only) but, when `policy.reliableRouteAvailable`, calls a new
`forgetUnreliableFlight(item)` that runs `forget` with the item's bytes,
key and reserve, clears `unreliableFlightTracked` and
`unreliableFlowReserve`, and records `observeUnreliableFlight`.
`releaseUnreliableFlight` stays as the acknowledgement path only. No
settings, no wire change. This is candidate G2 on flight-gate-fix-g1
(e77b50e) applied to the PR's function.

Why this shape. An acknowledgement is the only delivery evidence the
controller has; a timeout is the opposite evidence, so the two must not
share a primitive. Halving first and then growing by the additive step
is a net growth whenever the frame is smaller than the floor, which is the
common case (frames are 1.3 KB, the floor 8 KB). Not growing on RTO keeps
LOWBAR's "loss-responsive, receiver-evidenced growth" contract intact.

Low-bar risk: none; without a reliable route the path is unchanged.
Guard: the unit test plus `TestSendSequenceUnreliableResendTimeoutReleasesFlightWhenReliableRouteAvailable`
from the PR. MEMSTEADY: unchanged, no new state per item. Mixed route and device rig: `UnreliableFlightMaximumLimitByteCount`
must not climb during a p2p dead-lane phase.

Landed: b712a84.

### 13.2 Finding 2: scoped reply fall-through

Mechanism M2. Red test: `TestReceiveSequenceAckKeepsHybridH3Affinity`;
green tests to keep: `TestReceiveSequenceAckAffinityDoesNotHeadOfLineBlock`,
`TestReceiveSequenceAckFallsThroughWhenUnreliableIsFull`,
`TestMultiRouteSelectorReplyAvoidsUnreliableCarrier` (PR).

Change. Replace the PR's rule in `writeDetailedReplyWithCarrierPreference`
("never pin to a potentially unreliable carrier while a reliable one is
active") with the a1 rule (97744f6) plus the ack clock:

| Condition on the affine set | Reply routes |
|---|---|
| Any affine route is not `Unreliable` | affinity set unchanged |
| All affine routes `Unreliable`, at least one has channel room and `RouteAckProgressAge(route) < ReplyAffinityStaleAfter` | affinity set first, then reliable routes |
| All affine routes `Unreliable`, none has room, or every one is stale | reliable routes first, then the affine set |

`routeSnapshot` keeps `replyWriteRoutesByTransport` (a1); the ordering
between the two halves is decided per write from `len(route) < cap(route)`
and the clock, with no allocation. New setting
`ReceiveBufferSettings.ReplyAffinityStaleAfter`, default 2 s (one
`UnreliableMaxResendInterval`); zero disables the clock rule. The
non-blocking pass runs over the whole list, so a full or stale p2p lane
costs nothing and a healthy one keeps its ack. The PR's
`transportPotentiallyUnreliable` and its H3 behaviour are dropped.

Why this shape. Hybrid H3 publishes `Unreliable` but its stream lane is
QUIC-reliable and LOWBAR measured newest-covered-Pack ack affinity on it at
22.1 % faster tunneled completion and 7.5 % on upload; a blanket rule
reverses that. The failure the PR fixed is specific: a native p2p lane that
is full or silent. "Full" is a channel length read; "silent" is the per-route
ack clock, which is the only forward evidence the RTP lane has until 13.3
retires it. Answers design questions 2 and part of 1.

Low-bar risk: none for H3, since its affinity is unchanged; on `p2p-fast`
low-bar cells the ack may move to the relay when the lane is stale, which
is the intended behaviour. Guard: low-bar matrix, all three profiles.
MEMSTEADY: unchanged; the two reply orders are built once per route
snapshot and the per-reply decision allocates nothing.
Mixed route: `AckRouteWriteTimeoutByTransport[p2p]` 0 and h1-received ack
latency under one RTT in the loss profiles. Device rig: no
`AckRouteWriteTimeoutByTransport[p2p]` during a live p2p phase.

Landed: e853d73. The PR's reply test and its ack-lane regression test encoded the blanket rule and now assert the scoped one (a healthy lane keeps the reply; a full or stale lane hands it to the relay); the stale case is modelled through the ack-writer test seam.

### 13.3 M6: fast-path liveness (L1)

Red test: `TestFastPathBlackholeRetiresRouteAndResetsFlight`. Candidate
b73bf5a on flight-gate-fix-l1.

Wire format. A control RTP payload of 11 bytes: `'U' 'R' 'P'` then the
receiver's complete-message count as a big-endian uint64. It shares the
RTP sequence space with data and the warmup marker. A fragment header
alone is 16 bytes, so no data packet has an 11-byte payload; the receiver
checks length and prefix before the fragment parse, exactly as the 4-byte
`URW` warmup marker is checked today.

Behaviour. `webRtcFastPath` gains `receivedMessageCount`,
`sentMessageCount`, `remoteReceivedCount`, `unansweredSinceNanos`.
Receiver: a reporter worker started on the first complete message sends
the count every `p2pFastPathProgressReportInterval` (50 ms) while it has
changed, repeating each change three times, silent when idle. Sender: a
successful `writeMessage` sets `unansweredSince` if clear; a report with a
higher count clears it; a watchdog sampling at `min(250 ms, timeout/4)`
retires the association through `peerConn.cancelBecause` with cause
"fast path no progress" and `requestImmediateReconnect` when
`unansweredSince` is older than `WebRtcSettings.FastPathNoProgressTimeout`.
Default: 10 s, the same as `SctpNoProgressTimeout`; zero disables. The
seam field already exists on the branch (d781821).

Older peers. A peer without the change ignores the report as a malformed
fragment (its `accept` rejects the short payload and counts one
`fastDropCount`); it never sends reports, so a new sender facing an old
receiver would retire a healthy lane after 10 s. The watchdog therefore
arms only after the first report has been received from that peer, which
also covers the P2P wire version negotiation already used for the fast
path. Fast-path readiness is unchanged.

Composition with 13.2. Retirement withdraws the route, so the reply
fall-through is a bridge for at most `FastPathNoProgressTimeout`; the
ack clock in 13.2 and the report clock here are independent evidence and
neither depends on the other being enabled.

Why this shape. The SCTP lane already has a no-progress watchdog on its
SACKs; the RTP lane has no acknowledgement of its own, so a receiver report
is the smallest equivalent. Transfer's own acks are the alternative
evidence but they cross the route manager and the multi-client, and the
p2p transport must be able to retire itself without a Transfer dependency.
Answers design question 4.

Low-bar risk: an extra 11-byte packet every 50 ms while receiving, and
false retirement under an RTT above 10 s, which the low-bar profiles do
not reach. Guard: low-bar matrix on `p2p-fast`. MEMSTEADY: the reporter
and watchdog are two goroutines per fast path with one reused RTP packet
and an 11-byte stack payload; a report costs the same allocations as the
warmup marker (18, all inside the RTP writer). The Android block must show
no goRuntimeBytes step when a p2p lane comes up. Mixed route: the blackhole schedule must show retirement
within 10 s and recovery on the relay with zero dead windows. Device rig:
p2p route withdrawal logged when the client walks out of Wi-Fi with the
lane up, and no retirement during a healthy 3-minute download.

Landed: f51ebca, with TestFastPathProgressReportIsHarmlessToOldReceiver for the older-peer case.

### 13.4 M7: asynchronous race-commit delivery (R1), 209 as a guard

Red test today: none (test 11 is green under 209's bound). Tests: 11
`TestTunInjectFromReaderGoroutineDoesNotDeadlock` stays; 12
`TestMultiClientRaceCommitDeliversAsynchronously` is unskipped with a
seam.

Change. In `RemoteUserNatMultiClient.sendParsedPacketGroup`, the
`receivePackets` returned by `commitRaceClientWithLock` are no longer
handed to `deliverReceivePacket` on the caller's goroutine; they are
enqueued on the existing `removalReceiveQueue` (bounded by
`RemovalReceiveQueueSize`, 256, with `removalReceiveDropCount` on
overflow), whose worker already exists for best-effort packets. Ordering
within the flow is preserved because later packets of the committed client
arrive through the same worker path; the first-response packets are the
only ones that move. Seam for test 12: `beforeRaceCommitDeliveryForTest`
on the settings, called with the committed client so the test can park the
receive callback and assert `SendPacket` returns. 209's
`OutboundQueueWaitTimeout` stays at 250 ms as a guard; `Tun.OutboundDropCount()`
is added to the receive stats and must read 0 in every campaign, and the
hosted server proxy gets the same expectation in its monitor.

Why this shape. The cycle is a reentrancy: the goroutine that drains the
tun outbound queue injects into the same stack. Bounding the wait leaves
the injecting goroutine parked for 250 ms under the TCP inbound shard
lock on every reentrant RST. Moving the synchronous handoff to a worker
removes the cycle at its source with a queue that already carries the
same ownership rules. Answers the R1 half of design question 4 from §12.

Low-bar risk: none on devices (the OS tun never loops back). Guard:
`tun_congestion_test.go`, the PR's `tun_outbound_wait_test.go`, and a
server proxy soak with `OutboundDropCount` 0. MEMSTEADY: unchanged; the
burst reuses the bounded removal receive queue (256 entries) and its
pooled bytes are returned by the worker. Mixed route: unchanged
throughput with the socks-shaped client. Device rig: not applicable.

Landed: cbbfeea. Reordering is bounded, not eliminated: only the race burst crosses the worker, later packets of the flow may overtake it by at most the burst's length; documented on deliverRaceCommitPackets. The drop count is exposed as Tun.LinkStats rather than in the client receive stats, which have no tun.

### 13.5 M4: defer the whole-window timeout while the cumulative ack advances (S3, F12), pending

Red test: `TestSendSequenceQueueInflatedRelayRttDoesNotFireWholeWindowTimeouts`.

Change (contract, not yet landed). In the RTO branch of the send loop, an
item carried by a reliable lane whose `resendTime` is due while
`lastCumulativeAckTime` is within one `rttWindow.ScaledRtt()` is
rescheduled by one scaled RTT, at most twice per item, counted in
`TimeoutResendDeferCount`; unreliable-carried items and sequences with no
cumulative progress in the window resend as today. The second half of the
test (a lane that stops must still resend) is the bound.

Why this shape. With one RTT window per lane (F10, now merged) the
remaining spurious timeouts come from queue inflation on the relay that
outruns the scaled RTT; the cumulative ack advancing is direct evidence
the lane is alive. Pending: the reporter's F12 round 10 was invalid, so
this lands only with its own PERFVAR A/B (relay queue-inflation schedule,
`TimeoutResendWithRecentCumulativeProgress` as the primary) and the low-bar
matrix, since a deferred resend on `exchange-h1` cells is a real latency
cost. MEMSTEADY: unchanged; one counter per item, no queue growth (a
deferred item stays in the resend queue it was already in).

Landed: d74302b, default off; the defer is anchored on the item's send time (cumulative progress within one scaled RTT before it), and test 7 runs with the setting on over a 100/250/400/700 ms queue profile that still fires two spurious timeouts with it off.

### 13.6 M5: size-aware unreliable admission (S4), pending

Red test: none; `TestFastPathMessageLossFollowsFragmentCount` is the
characterisation and becomes the gate.

Change (contract). Two knobs on `P2pTransportSettings`:
`FastPathMaximumFragmentCount` (default 8, so a message on the fast path
is at most 8 × 1188 bytes; larger frames select the legacy or relay lane
through `unreliableForMessageByteCount`), and `FastPathLossyFragmentCount`
(default 2): once the flight controller has reduced to its floor, the
fast path accepts only messages up to that many fragments until growth
resumes. Both are carrier properties the route snapshot already carries.

Why this shape. Message loss is `1-(1-p)^n`; capping n bounds the loss
the flight controller sees to what packet loss actually is, without
fragment retransmission, which would duplicate Transfer's recovery.
Pending: the benchmark sweep decides the defaults, then the mixed route
with the 1 % and 3 % profiles must show a higher unreliable window
(`UnreliableFlightMaximumLimitByteCount`) at equal delivery. MEMSTEADY:
unchanged; the caps are two integers on the carrier properties.

Landed: 051654c, default off. Sweep (BenchmarkStreamFastWebRtcRouteLossSweep, 300 messages per cell, seeded vnet loss), message loss per 10k and reassembler evictions per 10k:

| packet loss | 1 fragment | 8 fragments | 14 fragments |
|---|---|---|---|
| 0 % | 0 / 0 | 0 / 0 | 0 / 0 |
| 1 % | 100 / 0 | 1167 / 900 | 1600 / 1367 |
| 3 % | 467 / 0 | 2567 / 2033 | 3633 / 3000 |

The 8-fragment cap bounds message loss at 3 % packet loss to about 26 %; the 2-fragment floor cap keeps it near the packet loss. The defaults stand pending the mixed-route campaign.

### 13.7 Findings 3 and 5 as measurement gates

Finding 3 (F1 on the low-bar regime). No code. Run the low-bar matrix on
the merged tree against 92a37c2's parent as control, `exchange-auto` and
`exchange-h3`, mobile surrogate, five repetitions. REGRESSION on any
profile means G1's narrower rule (e77b50e: overflow only when the reliable
route is a different transport from the unreliable one) replaces F1's
`reliableRouteAvailable`.

Finding 5 (ICE socket buffers). `WebRtcSettings.UdpSocketBufferByteCount`
keeps 4 MiB where the platform is a server or desktop and becomes 512 KiB
on iOS and Android through the SDK's platform settings, with the request
clamped by the kernel either way. Gate: MEMSTEADY on the Android session
block with the p2p device rig: the buffers are kernel memory per ICE
socket, so goRuntimeBytes must be unchanged and whole-app PSS must not
rise by more than 512 KiB times the gathered socket count; the mixed
route's provider-side kernel receive errors must stay 0 at 512 KiB, which
decides whether the mobile value can be lower still.

Design questions 3 and 6 from §12: S1 and S2 are both in the merged tree
(F11b and F10) and stay together; the counter snapshot keeps its maps,
nil until a carrier writes, and is not made primitive in this program.

Landed (13.7, finding 5): sdk 979169f on the sdk worktree's flight-gate-fix branch, UdpSocketBufferByteCount 512 KiB in the device provider settings; builds and vets against this branch. Finding 3 is a measurement, not a change.

## 14. The mixed-route gap-resend regression and its fix

The first PERFVAR mixed-route campaign (tests ledger 8e430ac) measured our
tree worse than the merged PRs on selective-gap resends, worst on
clean-lan tcp-parallel: 29 for merged against 874 for ours over five runs,
with goodput down and two dead windows on burst-loss latency-under-load.
Clean-lan has no loss, so the cause cannot need any.

Reproduction. `TestSelectiveAckGapSkipsUnreliableItemsWhileBothLanesCarryAcks`
builds the scoreboard state directly: a mixed-lane route, a hole carried by
the direct lane a hundred milliseconds ago, three later items selectively
acknowledged. It fails on our previous head (9e317ac) and on the merged
base (89e1633) with the same message, so the defect is in the merged rule
and our ack affinity is what makes it reachable.
`TestMixedLaneAckAffinityDoesNotRaiseGapResends` is the end-to-end form:
two real Clients over a fast direct lane and a slow relay lane in both
directions, no loss, with the receiver's uplink contending for the bounded
direct reply route; it compares ack affinity against the relay-only shape
the merged blanket rule produced.

Root cause. The selective-ack scoreboard reads "three later selective acks"
as proof that this item was lost. That is an ordering rule, and it is only
sound when every acknowledgement travels the same path. F11b granted a
grace to items the reliable lane carried, which covered the obvious
cross-lane case, but not to items the direct lane carried. Our §13.2 gives
the direct lane its acks back, and a bounded direct reply route under
uplink contention sends some of those acks down the relay instead. An item
whose own ack took the relay is then overtaken by the acks of the items
after it, and the scoreboard resends a Pack that was never lost, halving
the flight window as it goes. Merged hid the hole by sending every ack down
the relay, which is the affinity LOWBAR measured as worth 22 % on
completion, so the blanket rule was not an acceptable way to keep it.

Fix. While both an unreliable and a reliable carrier are active, later
selective acks are not loss evidence for any item younger than the slowest
lane's scaled RTT, whichever lane carried it. The grace defers the recovery
to the moment that lane could have delivered the ack rather than dropping
it: an ack arriving first takes the item out of the queue and nothing is
written, and a Pack the lane really lost is still recovered at the grace
expiry, before its own timeout and without the backoff a retransmitted item
would otherwise wait out. The flight is not reduced for evidence that has
not arrived. With one ack lane active the ordering rule is untouched, so
datagram tail recovery on a direct-only route keeps its pace. The lane
state rides on the flight controller, so the pass costs one policy read and
one RTT read for the whole scoreboard instead of one per item.

Before and after, `TestMixedLaneAckAffinityDoesNotRaiseGapResends` over 600
messages on a lossless mixed route, three runs of each arm:

| Arm | Before (9e317ac) | After |
|---|---|---|
| relay-only acks, the merged shape | 2, 3, 1 | 0, 0, 0 |
| ack affinity, ours | 0, 1, 2 | 0, 0, 0 |

`UnreliableFlightGapReorderSuspected` now counts the deferred recoveries an
ack cancelled: 2 to 48 per run, every one a resend that would have been
written before.

Two tests of PR 208 asserted the old semantics and now assert the new one:
the mixed-lane scoreboard test checks that a fresh hole's recovery is due
in the future rather than absent, and the reply test pair was already
rescoped in §13.2.

### 14.1 The trade this makes, and which clock sets it

The grace is the sequence RTT window's scaled RTT. With F10 merged that
window is fed only by acknowledgements of reliable-carried items, so it
describes the relay, and its floor is `RttMinResendInterval`, 300 ms. The
direct lane's own round trip on the device rig is about 20 ms. So a Pack
the direct lane really dropped now waits up to the relay's scaled RTT,
never less than 300 ms, before its recovery is written, where the merged
tree would have written it as soon as three later acks arrived.

That is deliberate: it buys the goodput back from resends the lane never
lost. It is also unguarded. Every item younger than the grace defers,
including on a lane that is dropping heavily, so the cost concentrates
exactly where the first campaign already showed our previous head at 2 of 5
dead windows against merged's 0, burst-loss latency-under-load. That cell
is the one to read first in the next campaign, and the one that would
justify reverting to a lane-specific grace.

The right grace for a direct-lane item is when *that lane* could have
delivered the ack, not when the relay could. There is no per-carrier RTT
estimate to ask: F10 removed the unreliable samples from the one window
rather than giving them a window of their own, so the only estimate the
sender holds describes the relay. A per-carrier estimate is therefore a
precondition for tightening this, and it is §15.2.

`TestMixedLaneDirectLaneLossIsRecoveredByTheGrace` asserts the delay
exactly, so no later change can lengthen it quietly, and asserts it stays
at or under the unreliable lane's own resend ceiling, which is what the
item would otherwise have waited for.

## 15. What the device runs leave, under a strict memory ceiling

The device block removed the gate: provider flight waits fell from
7,142-11,762 per three-minute run on the pre-merge build to 0 on merged and
0 through all seven §13 items, and gap resends fell five to fifteen fold.
Throughput did not move: against a 523-546 Mb/s direct reference every
tunnelled run on every build sat between 0.1 and 7.1 Mb/s. The counter that
does not change on any build is timeout resends, 13,000 to 19,000 per
three-minute run, present in relay-only runs with the flight never waiting
and in a cellular run where the fast path never negotiated.

The 24 MiB mobile ceiling is strict: exceeding it crashes iOS, the rig's Go
runtime figure is the accepted surrogate, and the final head already
measured 24.38 MiB provider quiet p95 in one role against 23.74 and 23.88
for the merged control. There is no headroom to spend, so every item below
is judged on retained bytes first.

### 15.1 The timeout-resend storm (landed)

`TestSingleReliableLaneQueueInflatedRttDoesNotStorm` reproduces it with one
reliable lane, no loss, no flight gate: give the lane a bandwidth and the
sender writes a window into a route that drains at link rate, so an item's
acknowledgement cannot come back inside a retransmit timer that started
when the item was queued, and the whole window is rewritten every interval.
The scaled RTT tracks the head of the queue while the tail waits far
longer, and every timeout the test sees fires while the cumulative ack is
still advancing, which the test asserts rather than assumes.

§13.5's defer is the answer and it was off. Over 300 messages on a lane
serialising a frame every 12 ms:

| | timeout resends | deferred | fired with a live cumulative ack |
|---|---|---|---|
| defer off | 31 | 0 | 31 |
| defer on | 0 | 45 | 45 |

Turning it on is free in memory: a deferred item was already in the resend
queue and leaves it on the same acknowledgement, so nothing is retained
longer. It is now the default, with the contract as a test.

### 15.2 A per-carrier RTO (proposed, precondition for 14.1)

The unreliable lane's retransmit interval is `min(sequence scaled RTT,
UnreliableMaxResendInterval)`. With F10 the sequence window describes the
relay, so a datagram lane with a 20 ms round trip is judged by a 300 ms or
larger clock: it recovers late, and under §14 it also waits that long for
receiver-evidenced recovery. A second window fed only by unreliable-carried
acks fixes both, and is the candidate already written as a2f2bf2 on
flight-gate-fix-s2.

Landed. Acknowledgements of unreliable-carried items now feed a second
window instead of being discarded, the unreliable lane's retransmit
interval comes from that window once the lane has answered once, and the
§14 grace uses the estimate of the lane that carried the item, which is
what it always should have been. The sequence window still sees only
reliable-carried acks, so the relay's own estimate does not move.

Measured on the device rig's figures, a relay at 300 ms and a direct lane
at 20 ms, the grace for a direct-lane drop halves:

| | grace |
|---|---|
| before, the relay's estimate | 600 ms |
| after, the direct lane's own | 300 ms |

The remaining 300 ms is `RttMinResendInterval`, the floor that applies once
a window holds samples, so the floor now dominates the direct lane's grace
rather than the relay's clock. Lowering that floor for a lane whose own
round trip is an order of magnitude under it is a separate knob with its
own duplicate-cost trade, and this program does not take it.

Memory: `UnreliableRttWindowSize` is 16 samples against `RttWindowSize`'s
128, about 640 bytes per sequence where a second full window would be
5 KiB. The SDK's envelope test counts both windows across the mobile
sequence ceiling, 92 KiB, and
`TestMobileDirectLaneRttWindowStaysSmall` fails if the direct lane's window
grows toward the relay's.

### 15.3 Why the direct lane carries so little, and what is free

The mobile policy caps the unreliable flight at 128 KiB and 16 messages.
Those are not the same budget. `TestUnreliableFlightRetainedBytesAreBoundedByTheByteLimit`
measures what each ceiling actually admits at 256-byte messages:

| message ceiling | messages admitted | bytes held of the 128 KiB budget |
|---|---|---|
| 16 | 16 | 4 KiB, 3 % |
| 128 | 128 | 32 KiB, 25 % |
| 1024 | 512 | 128 KiB, 100 % |

The byte ceiling binds on its own, so the message ceiling decides only how
much of a budget already granted small messages may use. At the tunnel's
typical 930-byte message, 16 messages is about 15 KiB per round trip: at
20 ms that is 6 Mb/s, and the rest of the 128 KiB budget goes unused. That
is the whole reason overflow reaches the relay on a link that can do 500.

Raising the message ceiling is therefore the one window change that retains
no more bytes: the byte ceiling is unchanged and still binds. Its real cost
is per-item structure, roughly 200 bytes of `sendItem` and resend-queue
entry for each additional in-flight message, so 16 to 128 costs about
22 KiB per active send sequence, and the resend queue's own 512 KiB mobile
budget still bounds the total. That is a measurable number against 0.38 MiB
of headroom, so it is proposed, not taken: the change is one constant in
the SDK's mobile memory policy and it must be gated by a device MEMSTEADY
block before it lands.

Raising the byte ceiling is the other way to make the direct lane carry the
bulk, and it is forbidden by the ceiling: bytes in flight are retained
bytes, one for one. To carry 100 Mb/s at a 20 ms round trip needs 250 KiB
in flight, about twice the entire current budget. That is a product trade
between mobile footprint and direct-lane throughput, and this program does
not take it.

### 15.4 Retention of the landed items

| Item | What it retains | Bound | Measured |
|---|---|---|---|
| 13.2 reply orders | two route slices per carrier per snapshot | route count, rebuilt per generation | no per-flow growth |
| 13.3 progress reporter | one reused RTP packet and an 11-byte payload, two goroutines | per fast path association | allocation-free per report |
| 13.4 race-commit handoff | the burst, in the removal receive queue | the queue, 16 entries on mobile, about 24 KiB | `TestRaceCommitHandoffRetentionIsBounded` |
| 13.6 fragment caps | two integers on the carrier properties | per carrier | negligible |
| per-carrier ack counters | three maps, built per snapshot call | nil until a carrier writes | not retained |
| §14 deferral | nothing; the item was already queued and leaves on the same ack | unchanged | unchanged |

The one item that can hold more than before is 13.4, and the mobile policy
already clamps its queue to sixteen entries. Nothing here explains 0.38 MiB
on its own, so the device block's next run should attribute the quiet p95
against the merged control per item rather than for the tree as a whole.

### 15.5 Sizing invariants

The constants that decide whether the tunnel trickles or crashes a phone
live in two repositories and drift independently, and none of them had a
test. These pin relationships, not values, and each failure names the
constants and the consequence.

| Test | Where | What it catches |
|---|---|---|
| `TestRetransmitIntervalCoversTheWindowOrTheDeferIsOn` | connect | a window that cannot drain inside the retransmit interval at the rate the device rig measured, with the defer off: the storm of §15.1 |
| `TestUnreliableFlightFloorHoldsATypicalMessage` | connect | a loss floor under one tunnel message, which stops the direct lane rather than slowing it |
| `TestP2pUnreliableFlightLimitsStayInsideTheirReceiveQueue` | connect | the carrier's flight and its receive queue drifting apart, or the reserve for untracked ACK traffic disappearing |
| `TestUnreliableFlightMessageCeilingAdmitsItsByteBudget` | connect | a message ceiling that replaces the byte budget with a much smaller one |
| `TestMobileUnreliableFlightCeilingsKeepTheirStatedTrade` | sdk | the mobile message ceiling falling further below the bytes already granted; it holds the §15.3 trade at about a tenth and explains what raising it would cost |
| `TestMobileRetainedByteBudgetsFitTheSteadyMemoryTarget` | sdk | a sizing change pushing the budgets that retain bytes past a third of the 24 MiB target, measured today at 6.7 MiB, 26.7 % |

The last is the one that stands between a future sizing change and an iOS
crash, so it lives with the constants rather than with the code that spends
them.

### 15.6 Most of the envelope is outside any budget

The mobile budgets that retain bytes sum to 6.8 MiB, 27 % of the 24 MiB
target, yet the device block measures the Go runtime at 23.7 to 24.4 MiB.
Roughly three quarters of the envelope is therefore outside every constant
the sizing test can guard, and no change to those constants can move it.

What can be named as living there: goroutine stacks, which scale with
flows, sequences, carriers and every worker this program added; the gVisor
netstack's own structures and its per-endpoint send and receive buffers,
sized in `TunSettings` rather than in the mobile policy; the QUIC and
WebRTC stacks with their own buffers and certificates; live packet
ownership in flight between the tun, the multi-client and Transfer, which
the budgets bound in aggregate but which the pools serve from the heap; and
the Go runtime's own heap fragmentation and GC headroom, which the
soft-limit policy shapes but does not cap.

So `TestMobileRetainedByteBudgetsFitTheSteadyMemoryTarget` protects against
one failure mode only: a sizing change to a Transfer or pool budget pushing
the configured share past a third of the target. It cannot protect against
a new goroutine per flow, a larger netstack buffer, or a leak, and a run
that crosses 24 MiB will most likely do so without any budget having
changed. Attributing the other three quarters needs a heap profile from the
device block against the merged control, per role, and that is the
measurement this program has not made. It is a product-level finding: the
ceiling is enforced by a number that the code's own sizing constants
explain only a quarter of.

## 16. The lossy-cell regression and the loss escape

The decisive campaign (tests ledger af88fc7) returned REGRESSION. Median
download goodput over five interleaved repetitions:

| Cell | merged | f8a507a | head 566305d |
|---|---|---|---|
| clean-lan tcp-parallel | 21.3 | 24.9 | 25.9 |
| clean-lan latency-under-load | 24.2 | 26.9 | 22.5 |
| loss-100bp tcp-parallel | 17.1 | 11.5 | 10.6 |
| loss-300bp tcp-parallel | 18.1, 0 dead | 8.2, 1 dead | 6.9, 9 dead |
| burst-loss tcp-parallel | 17.5, 0 dead | 9.7, 1 dead | 8.2, 6 dead |

Ahead of merged on every clean cell, behind on every lossy one, dead
windows only on our arms, and flight waits zero everywhere, so the gate is
not what separates the arms.

Cause. Both mechanisms §14 and §13.5 buy less duplicate traffic with more
recovery latency, and that trade inverts once a Pack is really gone. The
grace was the retransmit pacing interval, which floors at
`RttMinResendInterval`: 600 ms from the relay's clock in the campaign's head
arm, 300 ms after the per-carrier estimate landed, against a direct lane
whose round trip is about 20 ms. A lost Pack stalls the ordered stream for
the grace, the receiver cannot advance past the hole, and at one to three
per cent loss that repeats often enough to empty windows. Clean cells never
pay it, which is the pattern in the table.

Two changes.

The grace is now what the carrying lane could actually deliver. It takes
that lane's own estimate with the pacing floor removed, because pacing a
retransmit and judging whether an acknowledgement could still arrive are
different questions. `RttScale`, already 2, is the jitter margin, and
`UnreliableGraceMinimum`, 10 ms, covers timer granularity and scheduling on
a phone. On the rig's figures:

| | grace for a direct-lane drop |
|---|---|
| relay's estimate, the campaign's head arm | 600 ms |
| lane's estimate with the pacing floor | 300 ms |
| lane's estimate, no pacing floor | 40 ms |

Both mechanisms get an escape from the question they were built for. Every
deferred recovery records its outcome: an acknowledgement that cancelled it
is reordering, a recovery that had to be written is loss. While the counts
say loss the grace is withdrawn entirely and a hole is recovered as soon as
three later acks prove it, the merged behaviour. The counts halve past
`graceEvidenceCap` so the measure stays recent rather than accumulating over
a run. The retransmit defer now needs the cumulative ack to have advanced
since that item's previous deferral, so a hole nothing can acknowledge is
deferred once and then retransmitted.

In process. `TestMixedLaneLossyDirectLaneGoodputIsNotWorseWithTheGrace`
drops one and three per cent on the direct lane and requires the grace not
to make the stream slower than no grace at all. It passes, but the margins
are small, 555 ms against 562 ms at one per cent and 542 against 543 at
three, with single-digit gap resends: the harness does not reach the
regime the rig measured, so this is a guard against the property inverting
again, not proof that the cells recover. Stream B's arm is the
confirmation, and the bar is the one the campaign set: strictly better in
the lossy cells without giving back the clean-cell win.
