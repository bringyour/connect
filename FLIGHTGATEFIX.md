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

## 17. Device memory attribution (follow-up, not in this program)

Stream C's heap profiles across 17 MEMSTEADY blocks, recorded here because
the sizing tests live here and the finding is a product risk rather than a
program task. The 24 MiB ceiling is breached on every build measured,
including the pre-merge shipping code, which is the worst of the three:
worst quiet samples 28.67 and 28.45 MiB with 165 and 78 samples above 28.
So the overshoot is a pre-existing product defect, not something this
program introduced.

The class tree of `goRuntimeBytes`:

| Class | Share |
|---|---|
| live heap | about a third |
| heap spans unused | the largest remainder |
| heap free | with the spans, most of the rest |
| goroutine stacks | scales with flows, sequences and carriers |
| runtime metadata | fixed and small |
| profiling buckets | fixed and small |

Two consequences. Heap slack and GC metadata scale with the live heap
through the pacer, so every live byte costs about three at the runtime,
and across ten blocks the worst sample crosses 24 MiB once the live heap
passes 10.5 MiB. Live heap tracks outstanding pooled packet ownership,
which reached 2,098 objects and 3.98 MiB in the worst block.

The decisive line: the mobile idle reclaimer never ran in any quiet window
in any block, so burst ownership is never returned during the five minutes
the acceptance rule measures. Its gate is an outstanding-owner count of
sixteen (`mobileIdleMemoryMaxOutstandingPoolCount`), while the trim it
guards only ever drops free-list buffers and never touches buffers a
consumer holds. A connected tunnel at idle legitimately holds hundreds,
so the gate can never be satisfied and the reclaimer defers forever. That
is the root cause to fix in the follow-up; no code changed here.

Memory remains a not-worse-than-merged check on our arms, not a gate.

## 18. One lane-loss signal, and the contract it must satisfy

The interim numbers for c64442c said the grace and the escape had not
fixed the lossy cells. The counters said why: against merged, our tree put
two to seven times more traffic and acknowledgements on the lossy direct
lane, wrote five to eighteen times more gap recoveries, deferred almost
every timeout, and reduced the flight half as often. Merged reaches the
right lossy behaviour by accident, reducing the window on every gap and
keeping every acknowledgement off the direct lane, and pays for it in the
clean cells.

The contract, in `flight_gate_lane_contract_test.go`, using only the API
that predates this program so it runs against the merged base too. One
classification of a lane drives three behaviours, and each is stated in
both regimes:

| Contract | merged 89e1633 | ours before | ours after |
|---|---|---|---|
| classification tips on one proven loss | pass | fail | pass |
| reordering is not classified as loss | fail | pass | pass |
| window reduces on a proven gap | pass | pass | pass |
| window unmoved by reordering | fail | pass | pass |
| replies keep a healthy lane | fail | pass | pass |
| replies leave a losing lane | pass | fail | pass |
| hybrid keeps its own lane's affinity | fail | pass | pass |

Merged holds every losing row and fails every clean one; this tree before
the signal was the mirror image. The contract is to hold all seven, which
is what strictly better means here.

The signal. The sender latches a lane as losing on the first proven loss,
a recovery the lane forced us to write or a timeout of an item it carried,
and the latch decays over `unreliableLaneLossHold` clean acknowledgements.
While it is set: gaps recover at once and reduce the flight, including a
deferred recovery that had to be written, and timeouts are not deferred.
The receiver has its own earlier evidence, a selective acknowledgement,
which exists precisely because something is missing; while it is emitting
one its replies take the reliable carrier. A carrier whose own lane shows
no loss keeps its affinity, so hybrid H3 is untouched by another lane's
trouble.

Instrumentation. The per-transport acknowledgement counters recorded the
carrier of the Pack being answered, not the carrier the acknowledgement
left on, so the h1 ack-priority companion never appeared and every cell
read `ack_writes_h1` as zero. They now record the carrier written.

How close the in-process instrument comes. In the campaign's shape, a
direct lane at 20 ms losing one or three per cent, a relay at 200 ms, a
bounded reply route under uplink contention and four concurrent producers,
it reaches about 2 Mb/s over six or seven one-second windows with the
direct lane carrying roughly half the frames and losing twelve to sixty of
them. The harness measures nine to eighteen Mb/s with gap resends in the
thousands. So the instrument reproduces the shape and the direction but
not the magnitude: it cannot yet separate two arms the way a campaign cell
does, and above roughly three times this volume its synthetic forwarders
and bounded reply route wedge on each other. It is a pre-flight check, not
a substitute for stream B's arm.

## 19. Fix design for the low-bar and mild-loss regressions

Design for review, 2026-09-11, on c289a7e. Read against the source at
c3fcef3, the seven-row contract, and the four PERFVAR entries of 2026-09-11
(d381 lossy and complete, lowbar, features). Both findings have one root:
the sender judges an item by the clock of the lane that carried it, but the
acknowledgement's path is the receiver's choice, and the two disagree
exactly when it matters. The fix keys every clock to the acknowledgement
path, gives the lane-loss signal one job, and deletes the direct lane's
window rather than adding anything beside it.

### 19.1 Finding A, traced

On a route with one unreliable carrier and no sibling, `routeSnapshot` puts
a route in `reliableRoutes` only when its carrier properties are not
`Unreliable`, so `reliableRouteAvailable` is false on the policy and on the
controller. `scheduleSelectiveAckRecovery` then has `mixedAckLanes` false,
and `lateNotLost` needs that or `reliableCarrierObserved`, which no item on
this route has: the §14 grace is unreachable and every gap recovers at once.
The §13.5 defer requires `!item.unreliableCarrierObserved`, which every item
fails. The latch is read only in those two places, so it is inert, and
`reduceForLoss` is asked on every gap and timeout in both trees and halves
identically. The §18 claim is true in the code.

The live difference is §15.2. Merged's `observeAckRtt` returns for every
unreliable-carried item, so on this route its sequence window is never
sampled and `scaledRtt` answers with the cold floor `MinResendInterval`,
2 s, which `UnreliableMaxResendInterval` also caps at 2 s. Ours feeds the
sixteen-sample `unreliableRttWindow`, and `resendIntervalForPolicy` takes
`unreliableScaledRtt()` once it is sampled: twice a sixteen-frame mean,
floored at 300 ms. Same route and policy, first retransmit: merged 2 s
throughout, ours 700 ms, 1.4 s, 2 s, 2 s. The low-bar cells are the upload
direction, so the mobile surrogate sends over a 64 to 1,000 kbit/s uplink
whose round trip is its own serialisation queue, and that queue moves faster
than a sixteen-frame mean follows it. Each early timeout runs
`observeUnreliableResendTimeout`: the flight halves toward its 8 KiB floor,
the lane latches, and the frame is rewritten onto the bottleneck, where the
13-packet cell-edge queue turns the duplicate into real loss: gap resends
859 vs 672, transfer time 44.5 vs 38.3 s. Two facts bound the fix. Stock,
whose single 128-sample window took every ack and floored at 300 ms,
measured INDISTINGUISHABLE from merged on these cells (merged minus stock
−3.7 to +0.9 %, 1 of 5 seeds), so a sampled timer is not the harm here; a
sixteen-sample one is. And `probeRtt` reads the sequence window, starved on
this route in both trees, so both probe at the 2 s floor.

### 19.2 Finding B, traced: the acknowledgement path is the receiver's

The flap hypothesis does not survive the records. The complete campaign has
the 1 % cell at 16.7 vs 18.9 Mbit/s (the 4.6 vs 15.5 figure does not occur
in the run records) with gap resends 569 vs 70; gap resends are above
merged in seven of eight cells including clean-lan, 184 vs 3, where nothing
is lost; the receiver wrote 74 to 91 % of its replies on the relay even on
clean-lan; and the grace cancelled 1,448 to 3,139 recoveries per run against
merged's 0 to 6. Reorder at the sender is rampant, and it is not loss.

On a loaded mixed route the receiver holds a selective ack most of the time:
a relay-carried Pack lands about 180 ms after the direct-carried Packs
numbered above it, and the §18 reply rule (`replyLaneLosing = 0 <
len(ackSnapshot.selectiveAcks)`, reliable-first) reads that delivery reorder
as a dropping lane. So under load the acks for direct-carried items travel
the relay, in order, while both clocks that judge those items are the
direct lane's. (a) The timer (§15.2) floors at 300 ms while the ack takes
200 ms plus the relay's queue: every direct item sent around a hole times
out early, halves the flight, is forgotten (§13.1) and rewritten
reliable-only onto the relay whose queue it deepens, and latches the lane.
At 3 % the holes are continuous, the direct window fills with relay-path
samples and converges to the relay's clock, which is the parity; at 1 % it
alternates between 20 ms and 200 ms samples and the cascade fires at every
hole; under burst loss the clean stretches dominate. (b) The latch withholds
the defer. Relay-only, the defer removed 99 % of timeout resends (19,966 to
201), 6 dead windows to 0, +2.6 Mbit/s; on the mixed route it deferred a
third as many, and the mixed queue-inflation cell kept 22,526 timeout
resends and 16 dead windows in both defer states. A spurious relay RTO is
written p2p-first whenever the flight has room, so while latched the
storm lands on the direct lane:
that is the doubled direct carriage and the stall of the instrument's
1,024-packet arm, which holds the latch and therefore the coupling longer.
(c) The deferred-expiry branch of the RTO loop calls
`noteUnreliableLaneLoss()` for any carrier, so a relay-carried hole whose
delivery outran F11b's grace latches the direct lane, and it halves once
per expired item where a selective-ack round halves once. The §16 grace is
not the cost: the proving acks for a direct hole arrive via the relay
200 ms after it, past any 40 ms grace, so recovery is immediate in both
regimes and a latch transition costs at most one 40 ms deferral. The
clean-lan residual, relay-carried holes overtaken by direct acks past
F11b's grace under 2.4 to 7.5 times merged's striping, is sized by D7.

### 19.3 The design

| | Change | Where |
|---|---|---|
| D1 | The retransmit timer of an unreliable-carried item is the sequence window's clock, merged's form: `resendIntervalForPolicy` drops its `unreliableScaledRtt()` branch. With a reliable sibling that is the relay's clock, the lane the acks travel; a direct item resent reliable-only after §13.1's forget is judged by it too. Cost accepted: a tail loss on a healthy 20 ms lane waits the relay's 400 to 600 ms instead of 300 ms, merged's cost. | `resendIntervalForPolicy` |
| D2 | F10 narrowed to the route it was built for: `observeAckRtt` skips an unreliable-carried ack only while `flightController.reliableRouteAvailable`. On a forced direct route the sequence window is the lane's own 128-sample clock, stock's rule: twice the mean, floored at `RttMinResendInterval`, capped at `UnreliableMaxResendInterval`; 300 ms on a LAN-like lane, the queue on a cell-edge uplink. Probe pacing follows: the sampled minimum times `RttScale` floored at 300 ms instead of the 2 s cold floor, also stock's. When a sibling appears the direct samples age out of the window at the relay's ack rate and the defer covers the interim. Fallback if the guard below is not INDISTINGUISHABLE: keep F10 whole and let the route stay at merged's cold floor, one condition removed. | `observeAckRtt` |
| D3 | The direct lane's window goes: `unreliableRttWindow`, `UnreliableRttWindowSize`, `UnreliableGraceMinimum`, `unreliableGraceRtt`, `unreliableScaledRtt`, `ScaledRttWithFloorSampled`, and `MixedLaneAckReorderGrace`, since the deferral is now a sound rule rather than optional insurance. About 640 bytes per sequence returned; the sdk envelope test re-baselined and `TestMobileDirectLaneRttWindowStaysSmall` retired. | transfer.go, transfer_rtt.go, sdk |
| D4 | The arrival lane of every ack reaches the scoreboard. `Client.run` already holds `carrierReliability` per frame at the ack handoff; it rides `receiveAckMessage.arrivalReliability` and `sequenceAck.arrivalReliability` (one byte each, by value) into `receiveAck(..., arrivalReliability)`. `routeSnapshot.receiveDisposition` resolves `CarrierReliabilityUnknown` from the route's `Unreliable` flag, so every route with published properties answers and only a reader without carrier information stays Unknown, which counts as unreliable. On an item's first selective ack, `item.selectiveAckConclusive = arrivalReliability == CarrierReliabilityReliable || !reliableRouteAvailable`; the zero value means not conclusive, which is what a hand-built scoreboard must read. The pass keeps two counts of later selective acks, all and conclusive. A reliable-carried hole keeps F11b unchanged. An unreliable-carried hole is proven when the conclusive count reaches `SelectiveAckGapThreshold` or the lane is latched; otherwise, when the full count reaches it, it is deferred to `sendTime + rttWindow.ScaledRtt()`, the slowest lane an acknowledgement can take, or proven if already older. Single lane: everything conclusive, merged's ordering rule untouched. Hybrid H3 answers on its stream lane, whose sibling publishes Reliable, so its acks are conclusive for its own items. | `Client.run`, `SendBuffer`, `receiveAck`, `scheduleSelectiveAckRecovery` |
| D5 | The latch drives one behaviour, D4's conclusiveness. It is set only by proven loss of an unreliable-carried item, so the deferred-expiry branch gates `noteUnreliableLaneLoss` on `item.unreliableCarrierObserved`; it decays over `unreliableLaneLossHold` 64 clean acks as today; and `!self.unreliableLaneLosing()` leaves the defer condition, which already excludes unreliable-carried items. No loss-rate estimate: the hold is not the lever once a transition costs one deferral, and the contract's row 1 needs a tip on one event. | RTO branch |
| D6 | Expired deferrals reduce the flight once per pass of the RTO loop, a local flag reset per outer iteration; each still counts a gap. Merged's per-round cadence. | RTO branch |
| D7 | Counters in fixed arrays: gap recoveries written by hole carrier, deferred recoveries expired by hole carrier, holes proven under the latch alone. They split the clean-lan residual next campaign. | Client stats |

Unchanged: the receiver's reply rule (rows 5 to 7), §13.1 to §13.4, the
defer on with limit 2, the controller, size-aware admission off. Memory:
about 640 bytes per sequence returned; the item's bool sits in existing
padding, asserted; nothing new retained, nothing allocated on the ack or
packet paths; the pass carries one more integer and one policy read.

### 19.4 Why this shape, and what was rejected

Every rejected option keeps a carrier-keyed clock or adds a signal. A longer
hold or hysteresis on the latch: transitions cost one 40 ms deferral today
and one bounded deferral after D4, and the 1,024-packet arm shows a longer
hold multiplying (b) instead. A loss-rate estimate: needs hundreds of
samples, converges at 3 % to merged's behaviour, and cannot tip on one
event as row 1 requires. A max-based or per-carrier timer kept for the
mixed route: no clock keyed to the carrier can cover a path the receiver
picks per snapshot; on the single lane the 128-sample window is the shape
stock measured. Dropping the grace: rows 1 and 2 require a fresh hole with
no proven loss to be postponed, and the rule is sound, since fast-lane
proving acks can overtake a relay-borne one. Changing the reply rule: rows
5 to 7 are green and the 74 to 91 % relay share on clean-lan is delivery
reorder the rule cannot see; a trigger that tells a hole below a
direct-carried Pack from one below a relay-carried Pack is a follow-up
once D7 has sized it. Recovering reliable-carried holes by RTO alone (no
gap recovery while the route is active) is the named fallback if clean-lan
gap resends stay above merged's 3 after D4 to D7, with its own test. The
defer stays because of its relay-only result; D5 is what makes it act in
the lossy mixed cells, where the latch withheld it (deferred 0 to 512
against c644's 456 to 8,490).

### 19.5 Tests, each red without its change, all under `-race`

| Test | Regime | Asserts |
|---|---|---|
| `TestUnreliableItemTimerIsTheSequenceClock` (D1) | mixed | sequence window at 300 ms, direct acks at 20 ms: `resendIntervalForItem(direct, 1) == rttWindow.ScaledRtt()`, not 300 ms |
| `TestForcedDirectRouteFeedsTheSequenceWindow` (D2) | single lane | an unreliable-carried ack moves `rttWindow.ScaledRtt()` and `ProbeRtt()` with no reliable route and not with one; `TestSendSequenceAckRttIgnoresUnreliableCarrier` stays as the mixed guard |
| `TestForcedDirectRouteFirstRetransmitMatchesStock` (D2, the trade) | single lane | cold 2 s before any ack; 700 ms after 128 acks at 350 ms; the cap at `UnreliableMaxResendInterval`; the clock is never a sixteen-sample mean |
| `TestAckArrivalLaneReachesTheScoreboard` (D4) | both | through `SendBuffer.ackMessageDetailed`: a Reliable arrival marks the item conclusive, an Unreliable or Unknown one does not, and with no reliable route every arrival does |
| `TestGapProvenByRelayLaneAcksRecoversAtOnce` (D4) | mixed, clean | fresh direct hole, three conclusive later acks: recovered now, lane latched, reduce asked |
| `TestGapProvenOnlyByFastLaneAcksWaitsForTheRelayClock` (D4) | mixed, reorder | non-conclusive proving acks: deferred to `sendTime + rttWindow.ScaledRtt()`, no reduce, cancelled without a write by the item's ack |
| `TestTimeoutDeferIsNotWithheldByTheLaneLatch` (D5) | mixed, losing | latched, a reliable-carried RTO with cumulative progress since its send is deferred, `TimeoutResendDeferCount` +1 |
| `TestRelayHoleGraceExpiryDoesNotLatchTheDirectLane` (D5) | mixed | a reliable-carried deferral expiring is written, `unreliableLaneLosing()` stays false, no reduce |
| `TestDeferredExpiriesReduceOncePerPass` (D6) | burst | four expired unreliable deferrals in one pass: one halving, four gaps counted |
| `TestAckStructsGainNoBytes` (memory) | | `unsafe.Sizeof` of `sendItem` and `sequenceAck` unchanged; the sdk retained-budget test re-baselined lower by the window |

Rewritten: `TestMixedLaneDirectLaneLossIsRecoveredByTheGrace` becomes the
two D4 tests, and the fallback tests and `newSelectiveAckRecoveryTestSequence`
drop the window they build; `TestLaneClassificationHoldsSteadyUnderSustainedLoss` asserts
the cost of a transition (one deferral bounded by the relay's clock) rather
than a transition count, which no decaying counter meets at 0.5 % and which
the design makes pointless; test 7 gains a latched variant. The contract
file is untouched and its merged verdicts cannot move: its rows read
behaviour only, and merged has no arrival lane.

### 19.6 Measurements that call each finding fixed

Finding A: the low-bar matrix, `p2p-fast` on the three cells, upload,
mobile surrogate, five repetitions interleaved with merged: median goodput
INDISTINGUISHABLE or better with no stable sign against us, selective-gap
resends and median transfer time at or below merged's, timeout resends per
run at or below merged's, and the six exchange cells unchanged. Any cell
with a stable sign against us invokes D2's fallback and re-runs.

Finding B: all eight mixed cells against merged, strictly: dead windows at
or below, paired median goodput at or above, gap resends at or below in
every cell including clean-lan's 3, timeout resends at or below in the
lossy cells, and `TimeoutResendDeferCount` in the lossy mixed cells no
longer a third of the relay-only cell's. The relay-only queue-inflation A/B
is repeated with the tree: its 201 timeout resends, 0 dead windows and 15.7
Mbit/s must survive, and the mixed queue-inflation cell must leave 16 of 37
dead windows and 22,526 timeout resends behind. Memory not worse on the
heap+stack direction check. The instrument's six-arm run is a pre-flight
only: with D5 the 1,024-hold arm must complete, since the coupling it
lengthened is gone.

Out of scope, flagged: the §13.3 progress report costs about 10 kbit/s of a
64 kbit/s uplink in the download direction, which no low-bar cell has run.

### 19.7 Device evidence of 2026-09-11: D2 withdrawn, D4 bounded

Two shapes from the Wi-Fi-to-Wi-Fi run of the tree this section replaces
(initial writes down four-fold, timeouts per write up to 2.7, the direct
lane flapping between one and two routes two or three times a run) were
checked against D1 to D7. Neither is closed as written.

Route churn. `RttWindow` has no generation and no reset; samples leave only
by displacement (128) or the 60 s timeout. D2 therefore leaves a flapping
route's direct-lane samples in the sequence window after the relay returns,
so the relay's clock reads the direct lane's for up to 128 acks or 60 s
while F10 stops the relay's own samples from displacing them faster: the
19.2 (a) cascade, transiently, on every flap. D2 is withdrawn: F10 stays
whole, the one condition in `observeAckRtt` goes, and a forced direct
route keeps merged's cold floor for the timer and for probe pacing,
deliberately: on the only carrier every ack is conclusive and gap recovery
is immediate, the timer is the last resort, and a sampled one on a
serialising lane fires early. Finding A's fix is D1 with D3, and it is
merged-identical on that route by construction. The LAN-like lane's 2 s
tail wait is merged's; a lane-tagged window is the mechanism that would buy
both and is not taken without a measurement asking for it. The two D2 tests
become one: on a forced direct route the timer and the probe clock equal
merged's cold floor and the sequence window stays unsampled.

Starvation. A deferral holds the receiver's ordered stream at the hole, and
the sender's resend queue fills with selectively acknowledged items behind
it until `ResendQueueMaxByteCount` starves admission. D4 defers only a hole
proven by fast-lane acks while the sender has a reliable route (a
fall-through, or a receiver that lost its relay before the sender did), but
its clock is `rttWindow.ScaledRtt()`, which is the 2 s cold floor whenever
the relay has carried no data: a stall with no evidence behind it. Bound:
the deferral is the relay's scaled RTT when `rttWindow` holds samples and
exactly `RttMinResendInterval` when it does not, never `MinResendInterval`.
After the first expired deferral the latch makes the next holes immediate
for 64 clean acks, so a flap with real loss pays the bound once. F11b's
grace for relay-carried holes keeps merged's clock unchanged. Test:
`TestGapDeferralNeverWaitsTheColdFloor`: unsampled window and
non-conclusive proving acks defer by 300 ms, not 2 s; sampled at 300 ms,
by 600 ms. The contract's rows stay green under the bound.

The 9 % exchange-path regression of the merged PRs on radios is finding 3
(§13.7), which the low-bar guard already returned as REGRESSION on
`exchange-auto`; this design does not touch it, and G1's narrower rule
remains the named answer.

## 20. Second round: the reporter on the direct route, the relay storm behind the lossless recoveries, and a landing of merged plus what measures

Design for review, 2026-09-12, from the run records of flightgate-a66
(`/tmp/flightgate/a66-lowbar-p2p`, `a66-mixed`) read run by run against
merged on the same seeds. Both falsified predictions resolve to mechanisms
that D1 to D7 never touched, and the resolution says the same thing twice:
the mechanisms this program added interact into regimes merged never
enters, so the landable tree is merged plus the few items that measure as
wins on their own.

### 20.1 The forced-direct gap is §13.3's progress reporter

Same seed, per run, the client's uplink in the upload direction (a
9-packet queue; the reports are the packets a66 sends beyond merged's
while sending fewer Transfer messages, 1,504 against 1,553 on the first
cell):

| Cell | uplink packets merged → a66 | uplink queue drops | gap writes | transfer s |
|---|---|---|---|---|
| 5m-down-1m-up | 2,408 to 2,505 → 3,203 to 3,293 | 256 to 292 → 418 to 445 | 130 to 136 → 156 to 177 | 37.0 to 39.5 → 44.0 to 46.4 |
| 1m-down-250k-up | 653 to 682 → 1,043 to 1,137 | 31 to 53 → 106 to 121 | 19 to 27 → 33 to 41 | 21.6 to 23.2 → 26.4 to 30.4 |
| 256k-down-64k-up | 206 to 248 → 374 to 492 | 9 to 15 → 87 to 132 | 5 to 11 → 10 to 11 | 17.3 to 24.5 → 24.9 to 39.7 |

Mechanism, in `transport_p2p_fast_native.go`. Every fast-path receiver
starts `runProgressReporter` on its first complete message and writes an
11-byte report every 50 ms while its count changes, three repeats per
change. The sender of an upload is a receiver too, of the provider's acks,
so it reports on its own uplink at up to 20 packets a second; the reverse
direction shows the same +835 packets a run. The cell-edge queue is
counted in packets: at 64 kbit/s it drains about six data packets a
second, so the reports alone are three times its drain rate and tail-drop
the data, which is the 87 to 132 drops, the empty first window on every
64 kbit/s run, the run that stalled twice, and the readiness requests that
never got through in the three lost runs. The same 61-byte packets are
free on a LAN and absent on the exchange routes, which have no fast path:
that is the isolation stage 3 found. No setting turns the reporter off;
`FastPathNoProgressTimeout` zero stops only the watchdog. D1 did what it
said, whole-window timeouts fell from 28 to 4 per run on the first cell;
the deficit was never the timer.

### 20.2 The 182 lossless recoveries are two runs of a relay storm

The clean-lan tcp-parallel cell moves 2 MB in 2.4 to 4.1 s behind a 3.5
to 3.9 s route setup, so it measures the first seconds of a direct lane
coming up, not a steady state. Provider side, per run:

| run | merged: p2p KB, timeouts, gap | a66: p2p KB, timeouts written/deferred, gap, Mbit/s |
|---|---|---|
| 1 | 50, 0, 4 | 151, 0/0, 0, 27.7 |
| 2 | 40, 0, 0 | 738, 985/3,491, 56, 17.4 |
| 3 | 43, 0, 0 | 269, 261/358, 100, 16.2 |
| 4 | 47, 0, 0 | 346, 0/0, 0, 25.0 |
| 5 | 49, 0, 0 | 1,143, 0/0, 0, 27.2 |

Merged's direct lane carries 2 % of the payload in every run and never
stripes enough to reach any scoreboard state; its timeouts are zero
everywhere. Ours carries 7 to 55 %, and in two of five runs the striping
turns into a storm: 4,476 whole-window timeouts with recent cumulative
progress, of which the defer holds 3,491 and `TimeoutResendDeferLimit`
releases 985 as written duplicates; the relay carries 8,228 packets
against merged's 2,015 for the same 2 MB; the device answers every
duplicate past its head with a fresh head ack, 2,362 acks against 64 in a
good run; and the 56 and 100 gap writes are `unreliable_flight_gap_count`
zero, so every one is a relay-carried hole whose F11b grace expired inside
the inflated relay. D4 could not gate them: it leaves F11b unchanged by
design, and the holes are not on the direct lane. The clean-cell "win" is
real in the runs where the lane engages cleanly and absent in the runs
where it does not; a bimodal outcome cannot be strictly better than a
stable one on any primary but its median.

### 20.3 The landing: merged, plus what measures on its own

| Kept, with its measurement | Removed, with its measurement |
|---|---|
| §13.5 deferred retransmit, on, limit 2, with §16's since-last-deferral rule: relay-only 8.0 → 15.5 Mbit/s, 13 → 0 dead windows, 17,907 → 111 timeouts; mixed 9.4 → 16.1, 20 → 9 | §13.3 reporter, watchdog and the `URP` control payload: 13 to 57 % on every forced-direct repetition, three readiness losses, never measured to help (the blackhole schedule is uninterpretable) |
| §13.1 forget on RTO: an invariant with its test, untouched by any arm | §13.2 scoped reply affinity, `ReplyAffinityStaleAfter`, the receiver's `replyLaneLosing`, the reply orders on the route snapshot: the striping that feeds 20.2, hybrid H3 parity on stage 3 |
| §13.4 asynchronous race-commit delivery: a reentrancy fix with tests 11 and 12, off the recovery path | §14, §16, §18: grace, escape, latch, `gapRecoveryDeferred`, the seven-row contract as a gate |
| §8 counters and the carrier-written ack counters, the flight-gate tests that assert merged's semantics | D4 to D7: `arrivalReliability`, `selectiveAckConclusive`, the conclusive count, the per-pass reduce, the hole-carrier counters |
| D1 and D3: `resendIntervalForPolicy` and `observeAckRtt` are merged's text, the direct window is gone | §13.6 size-aware admission and its three knobs: goodput 0.8 and 2.0 Mbit/s lower with the cap on |

What returns to merged's text: `scheduleSelectiveAckRecovery`,
`receiveAck`, `observeItemAck`, the receiver's `writeSnapshot` and
`writeAck`, `writeDetailedReplyWithCarrierPreference` and
`transportPotentiallyUnreliable`, `sequenceAck`, `receiveAckMessage`,
`sendItem`, `transferFlightPolicySnapshot`, `routeSnapshot`'s reply
orders and `RouteAckProgressAge`, `SendBufferSettings` and
`ReceiveBufferSettings` less the removed knobs, `WebRtcSettings` less
`FastPathNoProgressTimeout`, `P2pTransportSettings` less the three cap
knobs, `transport_p2p_fast_native.go` whole. What stays as written: the
RTO branch's defer block with `!item.unreliableCarrierObserved` and the
progress-since-last-deferral term, `forget` and `forgetUnreliableFlight`,
`deliverRaceCommitPackets`, the counters. Memory is merged's less the
sixteen-sample window; `sendItem` and `sequenceAck` return to merged's
sizes, asserted.

Tests. The PR's own mixed-lane tests return to their 89e1633 text and are
the guard that the recovery path is merged's. The defer tests (test 7,
`TestSingleReliableLaneQueueInflatedRttDoesNotStorm`,
`TestRetransmitIntervalCoversTheWindowOrTheDeferIsOn`), the forget test
and the R1 tests stay. One new guard, red on 66a2130:
`TestFastPathWiresOnlyFragmentsAndWarmup`, over the vnet factory: while
one side receives N messages and sends M, the packets it emits are exactly
M's fragments plus the warmup marker, so no per-interval control packet can
return unnoticed. The contract file, the ack-arrival, lane-clock,
classification and mixed-lane-gap tests and the §14/§16 rows of
`transfer_flight_fallback_test.go` move behind the build tag
`flightgate_next`: they are the specification of the affinity candidate in
20.5, not a gate on this landing.

### 20.4 Why remove rather than refine

Four arms each fixed one regime and cost another, and 20.2 shows why: the
added mechanisms make the direct lane engage, and engaging creates
scoreboard and relay states merged never reaches, so every refinement has
been a repair of a state the previous mechanism created. The strict bar is
per cell on three primaries, and a bimodal cell fails it however good its
median. Merged is stable everywhere measured and slow in the ways §13.5
fixes; a tree that is merged plus the defer is the first arm whose every
difference from merged has its own measurement, and it lands the one
mechanism that has beaten merged in every seed it was tried on.

### 20.5 Measurements that call the landing done, and what comes after

The bar for this landing is not worse than merged on any primary in any of
the seventeen cells, better where the defer acts. Forced direct, three
cells: INDISTINGUISHABLE with the paired sign not stable against us, no
readiness loss beyond merged's, and the reporter's signature gone:
`carrier.p2p_network.forward` and `.reverse` `admitted_packet_count`
within 5 % of merged's per seed. Exchange, six cells: unchanged. Mixed,
eight cells: gap resends and dead windows equal to merged's within noise,
goodput at or above, and the relay-only and mixed
`mixed-relay-queue-inflation-3s` cells at or above 15.5 and 16.1 Mbit/s
with 0 and at most 9 dead windows. Devices: the p2p-live series must show
merged's packet counts on the direct lane; the relay-only series' 9 % is
finding 3, merged's own, and out of scope. Memory: MEMSTEADY not worse.

After it, each alone against this landing, one at a time: (1) reply
affinity with a storm guard, which is either a bound on reliable-only
overflow tied to the relay's own window or a receiver that does not switch
its reply lane per snapshot, measured on a cell whose transfer outlasts
its setup, since clean-lan tcp-parallel cannot see a direct lane at all;
(2) M6 as the route manager's ack-progress watchdog from §7's L1 text, no
wire cost, gated on the forced-direct packet count; (3) finding 3's G1
rule on `exchange-auto`, owed to the reporter; (4) `TimeoutResendDeferLimit`
2 against progress-bounded, since the 111 and 6,770 written timeouts that
remain with the defer on are limit releases; (5) the §15.3 mobile message
ceiling under MEMSTEADY.

## 21. Third round: the deferral removed merged's accidental throttle, so it needs an explicit one

Design for review, 2026-09-12, from the b0b04c8 records (`/tmp/flightgate/
b0b-defer-on|off`, `b0b-mixed`) read run by run beside merged and a66 on
the same seeds, and from the diff 66a2130..b0b04c8. The two leads are one
mechanism seen at two intensities.

### 21.1 What the records say

Lead 2, relay-only, `mixed-relay-queue-inflation-3s` (after 3 s the relay
becomes 20 Mbit/s with a 5 MB queue and 100 ms base delay, no loss). The
two stalled runs carry a signature no other run of this cell has, on any
tree or setting:

| seed, state | windows Mbit/s | provider timeouts written / deferred, gap writes | device timeouts written, carrier-change writes |
|---|---|---|---|
| 20260912 on | 1.8, 0, 0, 1.1, 8.8, 10.7, 0.2, 0.1, 1.4, 18.6, 15.3 | 2,780 / 2,036, 1,025 | 2,287, 83 |
| 20260913 on | 2.7, 0, 0, 1.9, 17.3, 17.2, 12.3, 0.2, 0.1, 0.1, 3.5 | 2,823 / 2,560, 1,020 | 1,419, 178 |
| 20260910 on | 13.2, 17.1, 17.0 | 0 / 2,472, 0 | 0, 0 |
| 20260912 off | 6.9, 4.0, 13.8, 17.4, 15.5 | 3,210 / 0, 0 | 578, 0 |

A thousand gap writes on a FIFO lane that cannot reorder, timeouts in
both directions, and carrier-change writes, which come only from
`scheduleRetiredReliableCarrierRecovery` on a route-generation change:
the relay stopped carrying anything for two ten-second stretches and the
device's route set changed under it. The stalled run also logs the
multi-client's `busy_probe` at ten seconds with `bar=3000`, its
`sendstalltimeout`; the good runs log none. Receive-handoff drops are zero
on both ends. In transfer.go the relay-only path is the same code on a66
and b0b: the revert's hunks there remove the arrival lane and the
per-snapshot reply switch and nothing else, and §13.1's forget needs an
unreliable-tracked item. So the removal set did not add a mechanism; the
question is why the defer, on this tree, could drive a relay into a state
merged never reaches (0 timeouts in every clean-lan run of three
campaigns, 1 dead window relay-only). Five runs a state cannot separate a
new cause from a rate the a66 campaign happened not to sample, so the
reproduction below comes first.

Lead 1 is the same storm at one fifth the intensity. The 99 lossless gap
writes are 93 in one run, beside 498 written, 1,072 deferred and 1,570
spurious timeouts at 15.5 Mbit/s, while the other four runs wrote 0 to 3
at 21 to 25 Mbit/s and merged 0 to 6; the storm run's direct lane carried
175 KB against 31 to 67 in the rest. The 93 are `unreliable_flight_gap_count`
6, so nearly all are relay-carried holes: items the defer left past
F11b's time grace without rewriting them, which the next round of
direct-lane acks then proved.

### 21.2 The mechanism

Merged writes every spurious whole-window timeout. Each rewrite is a
route-channel write that blocks when the relay's channel is full, and a
blocked write stalls the sequence loop's admission of new Packs. That is a
throttle: merged cannot push more than about one window past what the
relay drains before it stops itself. It costs merged the 17,907 to 35,670
duplicates the defer removes, and it is why merged's relay queue never
outruns its own estimate. The defer keeps the item in the queue and writes
nothing, so the loop goes on admitting up to `ResendQueueMaxByteCount`, 2
MiB by default, into a lane that has just proved it cannot drain what it
holds: at 20 Mbit/s that is 800 ms of queue on top of the base delay. The
window's 128-sample mean lags the inflation, every item's timer fires
spurious, `TimeoutResendDeferLimit` releases the third firing as a written
duplicate, the inner TCP's own timer fires and retransmits through the
tunnel, and the multi-client's 3 s send-stall bar trips. The defer removed
a throttle merged had by accident, and the storm is what a lane does
without one.

### 21.3 The change: a deferral pauses admission to the reliable lane

One rule, no estimator. While any item of a sequence holds a deferral,
the sequence writes no new Pack to a reliable lane. Precisely:

- `sendItem.deferralOutstanding bool`, set when `shouldDeferTimeoutResend`
  grants a deferral, cleared when the item is removed by an acknowledgement
  (both branches of `receiveAck`) or when its timeout is finally written;
  `SendSequence.deferralOutstandingCount int` follows it.
- At the write site where `unreliableFlightGates` is evaluated, a new Pack
  whose write would be reliable-only, or any new Pack when the route has no
  unreliable carrier, waits while `0 < deferralOutstandingCount`, through
  the same wait path as the flight gate, until the count reaches zero or
  the earliest deferred `resendTime`. Packs the unreliable flight admits
  are written as today, so the direct lane is not starved by the relay's
  queue. Retransmits, probes and gap recoveries are not admissions and are
  unaffected.
- `SendBufferSettings.DeferTimeoutResendPausesAdmission`, default true,
  identity-bearing for the harness like the defer itself; zero cost when
  no deferral is outstanding.
- Counters: `DeferralAdmissionWaitCount`, `DeferralAdmissionWaitDuration`.

Why this shape. The deferral already asserts that the lane holds more than
it can drain in one estimate; admitting more contradicts the assertion.
Bounding the pipe by acknowledgement rather than by a rate estimate needs
no new state beyond a bool and an int, and it is what merged's blocking
write did, minus the duplicate. The pause ends when the deferred item is
acknowledged, which for a merely late item is one queue delay after the
deferral, while everything sent after it is still draining, so the lane
does not idle. Rejected: capping the deferral's length (the storm is
depth, not time); a bandwidth-delay bound (an estimator, and the window's
mean is exactly what lags); removing the defer (its win is the largest
result this program has); lane-proven loss as the gap and deferral
evidence (a later same-route selective ack proves a FIFO hole; it would
end the free deferral of a head hole and F11b's expiry writes, but it is a
second change and the storm explains both leads, so it is the named
follow-up if the residue survives the pause).

Unchanged: the defer's condition and limit, §13.1, §13.4, merged's
recovery path, the forced-direct route (no reliable lane, no deferral, the
three cells that just cleared), and the low-bar exchange cells, which are
the risk to watch: their queues inflate by design, so a deferral there
now pauses the mobile surrogate's admission for a queue delay; the six
cells must stay INDISTINGUISHABLE.

### 21.4 Tests, red on b0b04c8 unless marked

| Test | Regime | Asserts |
|---|---|---|
| `TestDeferralPausesReliableAdmission` | single reliable lane | an item's timeout is deferred; new Packs offered are not written until the item is acknowledged or its deferral expires |
| `TestDeferredItemAcknowledgedEndsThePause` | single reliable lane | an acknowledgement before the deferral expires resumes admission at once, and the count returns to zero |
| `TestDeferralDoesNotPauseTheUnreliableFlight` (guard) | mixed | with a relay item deferred, a Pack the flight admits is still written to the direct lane |
| `TestSingleReliableLaneQueueInflatedRttDoesNotStorm`, extended | single reliable lane | a lane draining at link rate with a budget above its bandwidth-delay product: whole-window writes stay zero as today, and the lane's unacknowledged bytes never exceed twice its bandwidth-delay product, which today they reach the budget |
| `TestInflatedRelayDoesNotStallTheTransfer` | single reliable lane | the schedule's shape in process, a 3 s step to a slower rate with a deep queue and a 2 MiB budget: no whole-window write, no window without delivery, completion within a stated bound |
| sizes and allocation | | `sendItem` gains no bytes (the bool sits in padding), the write path allocates nothing while paused |

### 21.5 Measurements, in order

1. Reproduction before the change: seeds 20260912 and 20260913 three times
   each, defer on and off, on b0b04c8 and 66a2130, with transport logging
   at V(1) and `RaceCommitDeliveryDropCount` and the route-generation count
   exported, so the route churn is attributed to the send-stall bar or to
   something else before the pause is credited with removing it. If a66
   stalls too, the removal set is exonerated and the mechanism above stands
   alone.
2. The pause alone against b0b04c8 on all seventeen cells and both
   queue-inflation A/Bs: relay-only every seed at or above 15 Mbit/s with
   no dead window, the bimodality gone; mixed at or above 14.6 Mbit/s and
   at most 4 dead windows; `clean-lan / tcp-parallel` gap resends at or
   below merged's 9 in every run, no run with a written whole-window
   timeout; the six mixed cells now behind on gap resends at or above
   merged; the six exchange low-bar cells INDISTINGUISHABLE; the three
   forced-direct cells unchanged; `DeferralAdmissionWaitDuration` reported
   per cell so the pause's cost is visible where it acts.
3. If the exchange cells move, the pause is scoped to routes whose
   `ResendQueueMaxByteCount` exceeds the lane's measured drain in one
   scaled RTT, which is the estimator this design avoided and would then
   have earned its place.

## 22. Fourth round: admission is unbounded on a reliable lane in both trees, and the bound must be measured from delivery

Design for review, 2026-09-12, from the §21.5 reproduction (f034b84):
one reliable lane whose drain steps down mid-transfer, queue depth
settable, the default 2 MiB resend budget.

### 22.1 What the reproduction settled, and what §21 got wrong

| Arm | Time | Peak resend queue | Timeouts written |
|---|---|---|---|
| shallow queue, defer on | 6.1 s | 709 KB | 2 |
| deep queue, defer off | 12.1 s | 2,099,737 B | 1,516 |
| deep queue, defer on | 6.6 s | 2,099,782 B | 115 |

The accidental throttle is real but it is the route channel, not the
rewrites: with a shallow queue the channel fills, writes block, and the
resend queue stays at a third of the budget. With a deep queue nothing
blocks, and both arms reach the budget within 45 bytes of each other.
§21.2's claim that the deferral removed the throttle is withdrawn: merged
admits the full 2 MiB against a lane that has just proved it cannot drain
what it holds, and the defer is protective in that state, about twofold
(2 m 2 s and 72 of 121 windows dead against 58 s and 21 of 58 at scale).
§21.3's pause is withdrawn with it: it acts only while a deferral is
outstanding, so it cannot act in the worse, defer-off shape at all.

What the reproduction cannot show: a route-generation change, and with it
the carrier-change writes and the multi-client's send-stall bar in the two
stalled campaign runs. That component stays open, and 22.5 says what
settles it. The bimodality being two seeds in five on one cell, merged's
own rate is bounded only weakly: zero storms in fifteen clean-lan runs
puts it under about a fifth per run, and the same cell showed 6 dead
windows in 22 for the defer-off arm of an earlier tree, so the rig's
baseline is bimodal for everyone and the reproduction must count.

### 22.2 The tension, resolved

The numbers ask for a bound on unacknowledged bytes against what the lane
drains, and a bound against a drain needs a measure of the drain. The
question is only which measure. The one to avoid is the window's mean
round trip: it lags an inflation by design and is what the timer already
reads. The one to use is delivery itself: the bytes the lane acknowledged
during the last scaled round trip are a count the sequence already has,
not a model, and they are a floor on the drain that no queue depth can
inflate. When the drain steps down, the count falls within one scaled
round trip and the bound follows; when it steps up, the bound admits twice
what was delivered and grows by doubling. And it is the timer's own
margin: unacknowledged bytes at most what the lane delivered in `RttScale`
round trips means the queue adds at most one round trip of delay, so the
scaled timer fires on loss and not on depth. The bound is not an estimate
of a rate but the statement that a lane may hold what it has shown it can
carry.

### 22.3 The change: reliable-lane admission bounded by delivered bytes

- `SendSequence.deliveredBytesRing`: sixteen entries of (unix nanos,
  acknowledged byte total), allocated once with the sequence, advanced on a
  cumulative acknowledgement when the newest entry is older than
  `RttMinResendInterval / 4`. `deliveredBytes(d)` is the total now less the
  entry just older than `now − d`; a scan of at most sixteen. 256 bytes per
  sequence, the only retained cost.
- `reliableUnackedBytes = resendQueue.byteCount − flightController.byteCount`:
  what the reliable lane holds, since the unreliable flight already bounds
  the rest.
- `reliableAdmissionByteLimit = max(ResendQueueMinByteCount, deliveredBytes(rttWindow.ScaledRtt()))`.
- At the write site that evaluates `unreliableFlightGates`, a new Pack
  whose write would be reliable-only, or any new Pack when no unreliable
  carrier is active, waits while `reliableAdmissionByteLimit <=
  reliableUnackedBytes`, through the flight gate's wait path, until an
  acknowledgement moves either side. Retransmits, probes and gap
  recoveries are not admissions and are unaffected, and so are Packs the
  unreliable flight admits.
- Where it is inert, by construction: under the mobile budget
  (`ResendQueueMaxByteCount == ResendQueueMinByteCount`, 256 KiB) the floor
  is the budget, so the low-bar cells and the device rig are untouched; on
  a lane whose delivery over a scaled round trip exceeds the budget (a LAN
  relay at 300 ms floor delivers tens of megabytes) the limit is never
  below the budget. It acts only where the budget exceeds 256 KiB and the
  lane delivers less than the budget per scaled round trip: relays between
  about 5 and 40 Mbit/s, which is the queue-inflation cell (20 Mbit/s,
  bound near 1 MB against 2 MiB today) and the storm runs of clean-lan.
- `SendBufferSettings.ReliableAdmissionBoundedByDelivery`, default true,
  identity-bearing for the harness; counters
  `ReliableAdmissionWaitCount`, `ReliableAdmissionWaitDuration`,
  `ReliableAdmissionByteLimitMinimum`.

The defer stays on and unchanged: it covers the transition, when the pipe
still holds what the old bound admitted and the mean has not yet followed,
and the bound covers the steady state, so the storm has no state to grow
in. Merged's recovery path stays. Rejected: a reduction-triggered window
for reliable lanes (a spurious timeout as the trigger halves the exchange
low-bar cells, whose queues fire that signal constantly); a length cap on
the deferral (depth, not time); the route channel's capacity as the bound
(it is a memory constant, not a lane property, and the deep-queue arm
shows what it is worth).

### 22.4 Tests, red on b0b04c8 unless marked

| Test | Regime | Asserts |
|---|---|---|
| `TestReliableAdmissionIsBoundedByDelivery` | one reliable lane, deep queue, 2 MiB budget | unacknowledged bytes on the lane never exceed `max(256 KiB, delivered over one scaled RTT)`; today they reach the budget |
| `TestReliableAdmissionBoundFollowsAStepDown` | one reliable lane, drain steps down | within two scaled round trips of the step the lane's unacknowledged bytes are under the new bound and no whole-window timeout is written after that |
| the reproduction's three arms as one test | one reliable lane | deep queue with the bound: peak resend queue under half the budget, time within the shallow arm's, timeouts written at most the shallow arm's |
| `TestReliableAdmissionBoundIsInertUnderTheMobileBudget` (guard) | mobile budget | admission decisions identical to today's, `ReliableAdmissionWaitCount` zero |
| `TestReliableAdmissionBoundDoesNotStarveAFastLane` (guard) | one reliable lane, delivery above the budget | the limit never falls below the budget |
| `TestDeliveredBytesRingAllocatesNothing` | | the ring is built with the sequence; the admission check and the ring's advance allocate nothing; `sendItem` gains no bytes |

### 22.5 Measurements, in order

1. The rig reproduction of §21.5 step 1 stands, unchanged in purpose and
   now more pointed: seeds 20260912 and 20260913 three times each, defer
   on and off, on b0b04c8 and 66a2130, with transport logging at V(1) and
   a route-generation change count exported beside the carrier-change
   writes and the multi-client's verdict events. It settles two things the
   instrument cannot: whether a66 stalls at the same rate, which decides
   whether the removal set is exonerated, and whether the route churn
   follows the send-stall bar, which decides whether the churn is the
   tunnel's queue delay (then the bound is its fix, since a bound near 1 MB
   at 20 Mbit/s is 400 ms of queue against a 3 s bar) or the connection
   layer (then it is a transport finding outside this program's path).
2. The bound alone against b0b04c8 on all seventeen cells and both
   queue-inflation A/Bs, with `ReliableAdmissionWaitCount` reported per
   cell so where it acted is visible. Bar: relay-only every seed at or
   above 15 Mbit/s with no dead window; mixed at or above 14.6 Mbit/s and
   at most 4 dead windows; `clean-lan / tcp-parallel` with no run above 9
   gap resends and no written whole-window timeout; the six mixed cells
   behind on gap resends at or above merged; the six exchange low-bar cells
   and the three forced-direct cells with the wait count zero and medians
   INDISTINGUISHABLE, which is the inertness claim measured; memory not
   worse beyond 256 bytes per sequence, in the envelope test.
3. If step 1 shows a66 stalling at b0b's rate and step 2 clears the bar,
   the landing is b0b04c8 plus the bound. If step 2 leaves a relay-only
   seed stalled with the wait count active, the residue is the connection
   layer and the transport logs of step 1 are where the next round starts.

## 23. Assessment: what the residue is, what would settle it, and the honest landing

Written 2026-09-12 after the §22 bound was built as specified and
falsified in the instrument (landed off in 2a5997f, today's behaviour
exactly, one boolean from any future test). This section answers the
question asked rather than proposing a fourth candidate.

### 23.1 What the bound taught

The measure is right: the ring reads 465 KB over 300 ms and 1.72 MB over a
second at 1.86 MB/s, and it follows a step down inside one window. The
bound is wrong, twice over. One delivered-per-scaled-round-trip is one
bandwidth-delay product, and holding the pipe at one BDP is stop-and-wait
on the delivery clock: the deep arms doubled (12.9 to 24.5 s, 6.8 to
19.8 s) with 22.9 and 18.1 s spent in the wait itself. And the floor is not
inert: on the shallow arm the applied limit sat at 257 KB, the lane
delivered 77 KB per scaled round trip after the step, and the arm slowed
from 5.9 to 9.3 s with only 429 ms of waiting, so the gate changed what
the loop sent and when in a way the design did not foresee. I accept the
coordinator's reading in full. I do not propose a multiple of the BDP:
the arms that got worse got worse on the floor, and any multiple keeps
the floor; and the case the bound was for is two seeds in five on one
cell, whose rate under merged is not known.

### 23.2 Whether a design can clear the strict bar on the residue

The residue on b0b04c8 is: six mixed cells behind merged on selective-gap
resends by single digits, two of them also on goodput (loss-100bp and
loss-300bp tcp-parallel, −2.7 and −2.0 Mbit/s on 1 and 2 of 5 seeds), and
the relay-only queue-inflation cell bimodal with the defer on (2 of 5
seeds). Against it: three forced-direct cells better or equal, six
exchange cells indistinguishable, clean-lan latency-under-load better in
every seed, and the defer's win, the largest this program has measured.

My assessment, plainly: no mechanism I can name clears the strict bar as
stated on that residue, and I would not stake a campaign on one. Three
reasons.

The bar cannot be met by any tree until it is calibrated. It asks for not
worse on three primaries in seventeen cells from five paired seeds, on a
rig whose paired spreads run −51 to +23 % on one cell and where merged's
own arms differ by 20 % between campaigns on the same cell. Merged against
itself, interleaved, would fail that bar in some cells by chance, and
nothing in the record says how many. Without the null distribution, a
single-digit deficit on 1 or 2 of 5 seeds is not distinguishable from the
rig, and a candidate that removes a real mechanism can still be called
worse by the same noise that called the last one better.

The one systematic component with a source-traced mechanism is small and
would be a narrowing, not a mechanism: F11b's grace is anchored on the
item's send time plus the scaled round trip, which is exactly the timer's
due time, so a deferral extends the timer and not the grace, and the
scoreboard writes at the next round of direct-lane acks what the timer
just declined to write. Making the two paths agree (a deferred item is
late, not lost, for the scoreboard until its deferral expires) is a
one-line change with a deterministic test. It would move gap resends, a
secondary counter, toward merged's; whether it moves goodput I cannot say
from the records, because the counters that would attribute the −2.7
Mbit/s went with D4 to D7. It is worth building behind a flag and reading
in one campaign after the calibration, and not before.

The relay-only stall has a component no instrument in this program can
produce (a route-generation change under the multi-client's 3 s send-stall
bar) and a rate under merged that five runs cannot bound. It is a
transport-layer question first, and a recovery-path question only if the
rig reproduction ties the churn to the tunnel's queue delay.

### 23.3 The evidence needed, in order, before any further candidate

1. An A/A campaign: merged against merged, two builds of the same commit,
   interleaved per repetition on the same five seeds, all seventeen cells.
   It gives the null distribution of paired differences and seeds-better
   on each primary. Not worse then means inside that band, and the
   residue is measured against it rather than against zero.
2. Two counters on the landed tree, read in the same campaign as the
   candidate, no behaviour change: selective-gap writes of items whose
   timeout was deferred, by hole carrier; and a route-generation change
   count beside the carrier-change writes.
3. The rig reproduction of §22.5 step 1, unchanged: seeds 20260912 and
   20260913 three times each on b0b04c8 and 66a2130, defer on and off,
   transport logging at V(1). It decides whether the removal set is
   exonerated and whether the churn follows the send-stall bar.

Only if 1 shows the residue outside the null band and 2 attributes it to
deferred items is the narrowing in 23.2 worth its campaign; if 3 ties the
churn to queue delay, that is the round in which a queue bound is worth
revisiting, with a multiple and the floor removed, and not before.

### 23.4 The honest landing

2a5997f as it stands, bound off. Its claim, exactly: merged's recovery
path and merged's reply path; plus the deferred retransmit, on
(relay-only 13.5 to 15.8 Mbit/s median and 18,473 to 9,366 timeout
resends on this tree, mixed queue-inflation 8.9 to 14.6 Mbit/s and 20 to 4
dead windows, every on-seed ahead of every off-seed); plus §13.1's forget,
an invariant with its test; plus §13.4's reentrancy fix with tests 11 and
12; less the fast path's liveness reporter, which cost 13 to 57 % on every
forced-direct repetition and whose removal cleared those three cells; less
§13.6's admission caps, measured to lower goodput. Sizes and memory are
merged's less the direct lane's window plus a 256-byte ring that is not
read. The §22 bound stays in the tree off, with its counter, so that a
future round can measure it by flipping one boolean, and the tests behind
`flightgate_next` remain the specification of the affinity candidate.

Documented as the residue, with what each is worth: the six mixed cells
behind on gap resends by single digits (a secondary counter; the
narrowing in 23.2 is its candidate, gated on 23.3); the two lossy
tcp-parallel cells behind on goodput on 1 and 2 of 5 seeds (unattributed;
gated on the A/A); the relay-only bimodality (2 of 5 seeds, a transport
component; gated on the rig reproduction); the three route-readiness
losses on the slowest forced-direct profile, which merged shares. None of
these is a stall the user's device would meet that merged does not also
meet at some rate; the reporter's cost was, and it is gone.

## 24. Fifth round: the residue is two storm runs, the deferral re-fires a stalled window without backoff, and the primary should be total recovery writes

Written 2026-09-12 from the b0b-mixed records read run by run.

### 24.1 The substitution is real, and the primary should follow it

A deferred spurious timeout that the scoreboard later writes is one
duplicate counted as a gap write; merged's whole-window rewrite of the same
item is the same duplicate counted as a timeout. The implementation
stream's split confirms it from the other end: 1,300 of 1,699
deferred-item gap writes are relay-carried holes. Judged on the wire, the
four latency-under-load cells write five to twenty times fewer recoveries
than merged and burst-loss tcp-parallel fewer too. So yes: the harness
should judge total recovery writes (selective-gap plus whole-window, both
ends) as the primary beside dead windows and goodput, and the four strict
verdicts against us on gap writes were verdicts against half of a
substitution. The narrowing candidate's result is the same fact seen from
inside: honouring the deferral in the scoreboard moves the write from the
cheap counter to the expensive one and changes nothing on the wire. One
caution on "spurious by definition": on a mixed route a whole-window
timeout is a delay signal, not a loss signal, in both trees, because the
receiver's holes come from cross-lane reorder; the relay leg is lossless,
the direct lane is not.

### 24.2 Why hundreds of timeouts, why only parallel flows

They are one run in five in each cell. clean-lan run-01 wrote 498 and
deferred 1,072; loss-100bp run-03 wrote 611 and deferred 3,125; the other
eight runs of the two cells wrote zero and deferred zero, with 0 to 4 gap
writes, merged-identical. `unreliable_flight_timeout_count` is zero in
every run of both arms, so §13.1's forget never fires here.

The storm runs are mid-transfer stalls of the relay path. loss-100bp
run-03: delivery reached 2 MiB at 1.5 s and sat there for about 1.5 s
(2,097 → 2,097 → 2,097 → 2,102 → 2,116 KB per 250 ms) while the provider
deferred 3,125 times and wrote 611 and the device's own Packs timed out
too (33 written, 37 deferred); the direct lane stayed healthy throughout
(no unreliable-flight timeout, the device sent 710 fast-path messages).
clean-lan run-01: ahead of every other run at 6 MiB by 2.0 s, then the
last flow's tail sat at 7.4 MiB for 1.25 s at 8 to 20 KB per 250 ms
before the final 936 KB landed in one step. Both directions stalling with
the direct lane live is the relay path, not the recovery path. Merged's
worst run on loss-100bp shows a comparable stall, 1.25 s of nothing, but
at the transfer's start with a cold window and nothing in flight, so no
timer fired and nothing was counted. Whether merged never stalls
mid-transfer or merely did not in twenty-five runs the records cannot
say; the instrument cannot produce the stall either.

What the recovery path does with such a stall is where the trees differ,
and ours is the more expensive by construction. A deferral sets
`item.resendTime = sendTime.Add(scaledRtt)` and leaves `sendCount`
unchanged, so a stalled window re-fires whole every scaled round trip:
about 700 items in flight for four flows, four or five firings in 1.5 s,
which is the 3,125 deferrals, and `TimeoutResendDeferLimit` then writes
611 duplicates into the stall. Merged's rewrite doubles the interval per
attempt, so the same stall fires it once, then at twice the interval.
Parallel flows matter because the window is four times deeper, so every
firing costs four times as much and the last flow's tail is the part of
the window that waits longest.

### 24.3 The one narrowing that follows

A deferral backs off like the rewrite it replaces: `item.resendTime =
sendTime.Add(resendIntervalForItem(item, item.sendCount +
item.timeoutDeferCount))`, capped by `MaxResendInterval` as every interval
is. Nothing else changes. On the relay-only queue-inflation cell the first
deferral is unchanged and most items are acknowledged inside it; the
second is longer, which only reduces the 111 to 9,366 writes the limit
releases. On a mid-transfer stall of 1.5 s at a 400 ms scaled round trip
the window fires at 0.4 s and 0.8 s and is deferred to 1.6 s, by which the
stall is over and the acknowledgements arrive: about 1,400 deferrals and
few writes against 3,125 and 611 today, and against merged's roughly a
thousand rewrites for the same stall. The cost is a head hole with no
later same-lane acknowledgement, which waits one extra scaled round trip
on its second deferral; the gap rule covers every hole that is not at the
tail. Behind `SendBufferSettings.DeferTimeoutResendBackoff`, default on,
identity-bearing, so the A/B is one flag. Tests, red today:
`TestDeferralBacksOffLikeARewrite` (the second deferral of an item is
twice the first) and `TestStalledWindowReFiresLogarithmically` (a 1.5 s
stall of a lane holding 700 items produces at most two firings per item
and no written timeout before the stall ends); the relay-only storm test
and test 7 stay green.

### 24.4 Evidence and order

1. The harness primary becomes total recovery writes, both ends, with the
   per-run split kept; the A/A campaign of §23.3 stands, since a one-in-
   five storm is a rate and the null band decides what a rate means.
2. Export, no behaviour change: the reliable lane's longest gap between
   consecutive acknowledgements per run, and the round trip the timer read
   at each firing. The first tells a relay stall from a deep queue, which
   the counters cannot; the second says whether the first firing was a
   lagging mean or a dead leg.
3. The backoff alone against b0b04c8 on all seventeen cells and both
   A/Bs, judged on total recovery writes: the two storm cells at or below
   merged on the total in every run, the relay-only cell with fewer
   released writes and no new dead window, the forced-direct and exchange
   cells unchanged (no reliable-lane deferral there, or none that reaches
   a second firing).

If a storm run still writes hundreds with the backoff on, the residue is
the relay path's own stall and belongs with the route-churn question of
§22.5, not with the recovery path.

## 25. Sixth round: the 2.0 s read is not a ceiling, the stall wants a variance term beside the backoff, and what the calibration retracts

Written 2026-09-12 from flightgate-aa-20260912, flightgate-175-20260912
and the source at 175d82a.

### 25.1 What the two exports say, and what they cannot

The gap export is taken while items are outstanding, so a median longest
gap of 2,750 to 2,899 ms without a cumulative advance, in every run of
both arms, is a real in-flight stall of the relay path, and merged's
worst storm being its longest stall (7,023 ms, 3,902 writes) ties the
storms to its tail. But "exactly 2,000 ms, the configured maximum" is
not a ceiling on the items that storm. A relay-carried item's interval is
`min(scaledRtt, MaxResendInterval)`, 8 s, and the window is built with
`MaxResendInterval` as its own maximum (transfer.go, `NewRttWindow`), so
the estimate can read up to 8 s. The only 2.0 s constants are
`MinResendInterval`, the cold floor every sequence reads before its first
sample, and `UnreliableMaxResendInterval`, the cap on direct-carried
items. A largest read of exactly 2,000 ms in every run therefore says the
sampled estimate never exceeded 2 s: the mean relay round trip never
exceeded 1 s, against a stall of 2.75 s. The stall is an excursion of
three times the mean, not a level the timer sits below, and no ceiling
change reaches it. What the exports do not say is where in the run the
gap sits and what the head was waiting on; a stall that spans the first
Packs before the route settles and one that hits a full four-flow window
mid-transfer call for different answers, and the storm-length correlation
argues for the second only for the long ones.

### 25.2 Whether the ceiling is the design

Raising a ceiling: not sound, because for relay-carried items there is no
ceiling below 8 s to raise; the direct-carried cap of 2 s is a separate
knob (§2: "keep the oldest missing Pack moving often enough that an inner
TCP sender does not exhaust its own retry budget") and `unreliable_flight_
timeout_count` is zero in every storm run, so it is not firing here.

Following the observed gap: sound in its standard form, and the form
matters. The timer today is a mean with a fixed margin, `RttScale` 2,
which cannot cover an excursion of three times the mean without tripling
every retransmit on every lane. The standard answer is a deviation term,
RFC 6298: an exponentially weighted `rttVar` of `|sample − mean|`, one
duration of state per window, no retained bytes, and `timer = mean +
max(floor, 4 × rttVar)`, still clamped by the floors and `MaxResendInterval`.
On a lane whose jitter is a third of its mean a 3× excursion sits inside
four deviations; on a stable lane the timer tightens toward the mean and
the 300 ms floor, which is faster than today's 2× mean. A max-tracking
timer, the literal "follow the longest gap", is the other reading: it
never fires on a repeat of the same stall, but it decays only as samples
age out of the window (60 s), so it stays slow long after a transient, and
it still cannot cover a longer stall than it has seen, which is the 7 s
storm. That case is the backoff's, at logarithmic cost.

Cost on a genuinely lost tail: a tail item, with nothing after it for the
gap rule, is recovered only by the timer. With the deviation term a stable
lane recovers it in about the mean plus the floor, sooner than today; a
jittery relay recovers it in mean plus four deviations, later than today's
2× mean by up to the deviation term and never past 8 s. The cold start
keeps its 2 s floor either way. That is the trade: a lane that has shown a
wide spread earns a longer wait on its rare real loss in exchange for not
rewriting its whole window on every excursion.

Interaction with the backoff, now both exist: the estimator sets the first
interval, the backoff doubles it per deferral, so `resendIntervalForItem(
item, sendCount + timeoutDeferCount)` composes with either estimate
unchanged. A larger first interval means fewer deferrals and a later third
firing, past most stalls; a 7 s stall at a 1.5 s first interval fires at
1.5, 3 and 6 s, three times against today's four or five. They divide the
work: the estimator lowers how often the timer fires on a routine
excursion, the backoff bounds what a firing costs when the excursion is
exceptional. Neither makes the other redundant, and the backoff is the one
already measured.

The stall itself is the product finding. A relay whose mean round trip is
under a second and which routinely goes 2.75 s without acknowledging is a
property of the exchange path, and no sender timer makes a 3 s hole free
for the inner TCP, whose own timer fires at about a second and retransmits
through the tunnel. It should be localised before the timer is changed:
the same export on the relay-only queue-inflation cell and on the exchange
low-bar cells, plus the gap's offset in the run and the head item's carrier
and sequence number. If it is present relay-only, it is the exchange; if
it sits at the start, it is readiness and the cold floor already covers it.

### 25.3 What the calibration retracts, and what stands

| Claim | Basis then | Standing now |
|---|---|---|
| §19.1: the forced-direct deficit was §15.2's sixteen-sample timer | source | retracted by a66 (0 of 12 after D1 to D3); the reporter was the cause (§20), measured 0 of 12 → 8 of 13 with the packet signature gone |
| §19.2: the mixed-route deficits were carrier-keyed clocks and the latch coupling, sized by 4.6 vs 15.5, 569 vs 70, 184 vs 4 | source plus five-repetition verdicts | mechanisms stand as source facts; every magnitude is inside the null band or a single storm run, so the claim that they explained the verdicts is retracted; D4 to D7 were designed against noise and are gone |
| §19.7: D2 withdrawn for route churn, D4 bounded | source | the withdrawal stands on its reasoning (no window reset on a generation change); the churn evidence was 2 of 5 runs and carried no weight |
| §20.2: the 182 lossless recoveries were our striping flooding the relay | records, five runs | retracted: storm runs occur in 25 of 155 runs of each identical arm; the anatomy stands as description, the attribution does not |
| §20.3: the landing, merged plus what measures | measurement of parts | stands; its measured parts are the ones the A/A lists as surviving |
| §21: the deferral removed a throttle; a pause on deferral | records | withdrawn in §22 by a deterministic reproduction; stands as withdrawn |
| §22: bound admission by delivered bytes | source | falsified in the instrument; the falsification is deterministic and stands; the reading that the storm was queue depth is retracted, the storm rate is the rig's |
| §23: the bar needs an A/A; the grace narrowing | assessment | the first is confirmed exactly; the narrowing measured wrong-way and is retracted |
| §24.2: merged stalls only at a cold start | records | retracted by the exports: merged stalls in every run; what differs is the response |
| §24.2: a deferral re-fires a stalled window without backoff | source | stands, and measured at twenty repetitions: 0 of 40 storms against 5 of 40, 27 of 40 paired, +1.0 and +2.7 Mbit/s on the means |
| §24.1: total recovery writes as the primary | reasoning | stands; the rescoring confirms the substitution |

On the record: what rests on measurement is stock's collapse, the
forced-direct regression of d381cfa and 66a2130 and its reversal, the
defer's relay-only A/B, the backoff at twenty repetitions, the two
in-process falsifications, and the A/A itself. Everything else in §19 to
§24 that named a magnitude on the mixed route named noise, and the
mechanisms traced there remain source facts whose cost has not been
measured.

### 25.4 Order

1. Land 175d82a as the head, with the flags as they stand.
2. Export the gap's offset and the head item's carrier; run the export on
   the relay-only and exchange low-bar cells to localise the stall.
3. The deviation-term timer behind a flag, default off until measured,
   against 175d82a on the two storm cells at twenty repetitions and on the
   relay-only cell, judged on total recovery writes, goodput and dead
   windows, with the low-bar cells as the guard for the tail-loss cost.
4. The relay stall as a product item on the exchange path, owned outside
   this tree.

## 26. Seventh round: silence is a lane event the recovery path can read by lane, the landing, and the follow-up list

Written 2026-09-12 after the deviation term was built, falsified on the
new in-process stall (df81e98, off, with the negative finding as a test).

### 26.1 What an estimator cannot see, and what the path already can

The falsification is structural and stands: a 2.75 s stretch with no
acknowledgement yields no sample, so any timer built from samples learns
the tight pre-stall lane and tightens into the stall (411 and 425 writes
against the scaled mean's 347). No ack-sampled estimator can see silence.

But the campaign's stall is not silence of the sequence, and the counters
say so. `unreliable_flight_timeout_count` is zero in every storm run, so
direct-carried items were acknowledged within their 2 s cap throughout;
under merged's reply rule their acks travel the relay's reverse leg, so
that leg was flowing while the data leg held the head. The sender was
receiving acknowledgements the whole time, for items on one route and for
none on the other. That is the distinction that matters, and it needs no
estimate: a reliable carrier retransmits below Transfer, so an item on it
that is unacknowledged while later items on the same route are
acknowledged has been dropped at an endpoint, and one that is
unacknowledged while nothing sent after it on that route has been
acknowledged is in a queue or a stall, never lost, unless the route is
retired, which `scheduleRetiredReliableCarrierRecovery` already handles.
Every item carries `carrierRoute`, every acknowledgement resolves to an
item, and the sender can therefore attribute each acknowledgement to a
lane. Today it does not: the gap rule counts later acknowledgements from
any lane, so relay items overtaken by direct-lane acks are written (the
1,300 of 1,699), and the whole-window timer fires per item, so a stalled
lane fires its whole window and the deferral, even with backoff, is paid
per item (346 to 647 deferrals per stall).

So the honest conclusion has two halves. A relay that goes quiet for
three times its mean is the relay's problem, and the localisation in §25.4
stands. And the recovery path's correct response to it is not the backoff
alone but the backoff applied to the lane rather than to every item on
it, which is what TCP's timer does: on silence, retransmit the oldest
unacknowledged item once with backoff and hold the rest behind it; on a
later acknowledgement from the same lane, recover the hole. That is a
narrowing of two mechanisms, the per-item whole-window timer and F11b's
time grace, into one rule with evidence the sender holds, and the new
instrument can falsify it in process before it costs a campaign. It is
the round-3 lane-proven-loss rule with the piece that was missing then:
the head probe.

### 26.2 The rule, for the follow-up, stated so it can be built and falsified

Per route, on the sequence, in a fixed array of at most the snapshot's
route count, reset on a generation change: `highestAckedSequenceNumber`.
Updated in `receiveAck` from the acknowledged item's `carrierRoute`, no
allocation.

- Gap recovery of a reliable-carried hole X needs `SelectiveAckGapThreshold`
  later selective acknowledgements of items whose `carrierRoute` is X's;
  acknowledgements from other lanes do not count and there is no time
  grace. Unreliable-carried holes keep merged's rule: any three later
  acknowledgements, since that lane does not retransmit below Transfer.
  A single lane is unchanged by construction.
- A reliable-carried item's timer firing while its route's
  `highestAckedSequenceNumber` is below its sequence number is a lane
  probe: if it is the oldest outstanding item on that route it is rewritten
  with backoff as today; otherwise it is re-armed to that head's
  `resendTime` and counts neither a send nor a deferral. A firing with the
  route's highest acknowledged number above the item is the endpoint-drop
  case and is written as today.
- The deferral of §13.5 and its backoff stay as the response to a firing
  the lane has answered around (cumulative progress); the probe is the
  response to a firing it has not.

Expected on the instrument's 2.75 s stall at an 800 ms first interval:
two probes, at 0.8 and 2.4 s, no deferrals, against 347 writes and 346
deferrals with the scaled mean and backoff. Cost: an endpoint drop at the
tail of a reliable lane, with no later same-lane item to prove it, is
recovered by the probe's backoff rather than its own timer, which is
today's tail case; and a whole window lost at once on a live reliable
route, which a reliable carrier does not do, would recover one interval at
a time. Tests, red today: the stall instrument at those counts; a
relay-carried hole overtaken by three direct-lane acks is not written; a
relay-carried hole proven by three later relay-lane acks is; a stalled
lane holding 700 items writes one probe per backoff interval; sizes and
allocation unchanged.

### 26.3 The landing

175d82a as the head: merged's recovery and reply paths; the deferred
retransmit on with its backoff (relay-only 13.5 to 15.8 Mbit/s and 18,473
to 9,366 timeout writes on this tree, mixed queue-inflation 8.9 to 14.6
Mbit/s and 20 to 4 dead windows, the storm cells 0 of 40 storms against 5
of 40 and 27 of 40 paired at +1.0 and +2.7 Mbit/s); §13.1's forget and
§13.4's reentrancy fix; the fast path's liveness reporter and §13.6's caps
removed; three flags landed off with their negative findings as tests
(`ReliableAdmissionBoundedByDelivery`, `DeferredItemIsLateForTheScoreboard`,
the deviation timer). Pending only the definitive twenty-repetition
campaign, judged on total recovery writes, goodput and dead windows
against the A/A's band.

What the user can be told it is: merged with the one mechanism that has
beaten merged in every seed it was tried on, minus the one that cost every
forced-direct repetition, with the recovery path otherwise merged's; and
what it is not: a tree that clears an every-cell bar at five repetitions,
which the calibration shows no tree can.

### 26.4 The follow-up list, in order

1. The relay stall on the exchange path: localise it with the gap's
   offset and the head item's carrier on the relay-only and exchange cells;
   if it is present relay-only it is the exchange's, and it is a product
   item outside this tree.
2. Lane-attributed recovery for reliable carriers (26.2), falsified or
   confirmed on the stall instrument first, then twenty repetitions on the
   storm cells against 175d82a on total recovery writes.
3. Reply affinity (the tests behind `flightgate_next`), only after 2, since
   2 is what makes striping safe: a relay item overtaken by direct acks is
   no longer written.
4. M6 as an ack-progress watchdog without wire cost, gated on the
   forced-direct packet signature.
5. Finding 3's G1 rule on `exchange-auto`, owed to the reporter.
6. The §15.3 mobile message ceiling under MEMSTEADY, and the §17 idle
   reclaimer, both product items.
7. The instrument: repetitions from the A/A table as the rule (twenty for a
   storm rate, thirty-three for a 10 % effect), the stall shape kept, and
   every flag landed off re-read by its test rather than forgotten.

## 27. Eighth round: the collapsing seed is the second firing, the prediction for §26.2, and its root-cause contract

Written 2026-09-12 while the definitive campaign runs, for the
implementation stream building §26.2 against the stall instrument.

### 27.1 What distinguishes one seed from four when the stall is identical

Not the deferral. A deferral is a re-arm; it writes nothing, and its count
is the count of timers that fired spuriously, which on a draining lane is
the mechanism of the win (20,564 deferrals against 111 writes relay-only).
What a stalled lane pays today is the second firing. `shouldDeferTimeoutResend`
grants a first deferral to every item the lane had answered around, and
its since-last-deferral term denies the second while the lane is still
silent, so an item whose second firing lands inside the stall is written,
as a duplicate, into the lane that is not draining. The earlier stalled
runs carry that rule's signature, written and deferred nearly equal (2,780
against 2,036, 2,823 against 2,560). With the backoff the second firing
lands at two intervals after the first, so whether it lands inside a
2.75 s stall is set by the interval the timer read at the stall's onset:
a tight pre-stall lane reads 300 to 500 ms and the window is written from
0.9 to 1.5 s into the stall; a queue-inflated lane reads near a second and
the window's second firings fall past 2.75 s and are acknowledged instead.
That interval is a function of the lane's load in the seconds before the
stall, which the seed sets and the stall does not. The duplicates then
queue behind the originals in the stalled lane, the post-stall round trip
carries them, and the lane inflates on its own duplicates: the collapse.
So the backoff halved the mode (two seeds of five to one) by moving some
seeds' second firings past the stall, and could not remove it.

What would settle it independently of §26.2: export the interval the
timer read at the first firing after the longest gap began, beside the
gap's offset in the run and the items outstanding at its onset. The
prediction is that the collapsing seed reads the smallest interval at
onset of the five.

### 27.2 The prediction for §26.2, stated to be falsified

Under §26.2 a firing in silence rides the route head; the second firing
is a ride, not a write, at every alignment. On the instrument's shape (a
200 ms lane holding everything for 2.75 s mid-transfer, which gave 347
writes and 346 deferrals with the scaled mean and backoff):

- whole-window writes during the stall: at most three, all of them the
  route head, at the head's first interval and then doubling (0.5, 1.5,
  3.5 s for a 500 ms interval, so two inside 2.75 s; three for 300 ms);
- no item written on its second firing during the stall, and no item
  deferred twice during it;
- deferrals: only the firings inside the first interval of the stall,
  while the route's last acknowledgement is still within one interval,
  bounded by the items sent in the interval before the stall; they are
  re-arms and are not the metric;
- after the stall: the originals drain in order, the probes' copies are
  discarded past the head, and no secondary inflation follows; the
  transfer's excess over an unstalled run is the stall plus one drain;
- on the campaign's relay-only cell: no seed below its defer-off value,
  the mode gone at every alignment, the median unchanged.

Falsified if the stall produces more than ten writes, any second-firing
write, or a post-stall firing rate above the pre-stall one. If the
instrument agrees and the campaign seed still collapses, the collapse is
not the second firing, and the exports of 27.1 are what remain.

### 27.3 The precedence, made exact

§26.2 left the draining test ambiguous. Per route, in the fixed array:
`highestAckedSequenceNumber` and `lastAckNanos`, both set in `receiveAck`
from the acknowledged item's `carrierRoute`, reset on a generation change.
At a reliable-carried item's timer firing, in this order:

1. `highestAckedSequenceNumber > item.sequenceNumber`: the route
   delivered something sent after this item, so the item was dropped at an
   endpoint; write, with `sendCount` backoff, as today.
2. `now − lastAckNanos < scaledRtt`: the route is draining; §13.5's
   deferral with its backoff, limit and since-last rule unchanged.
3. otherwise the route is silent: if the item is the oldest outstanding
   item on its route, write it with backoff and count a probe; else re-arm
   it to that head's `resendTime`, counting neither a send nor a deferral.

The route clocks, not the sequence's `lastCumulativeAckTime`, decide 2
and 3, so on a mixed route a relay whose head is stuck while direct-lane
acknowledgements keep the cumulative ack moving is still read as silent
and probed, not deferred to its limit and written. On a single lane the
two clocks coincide. Unreliable-carried items keep merged's rules whole.

### 27.4 The metrics and their root-cause contract

New: `LaneProbeWriteCount`, `LaneProbeRideCount`,
`LaneProvenTimeoutWriteCount`. Probes also count in `TimeoutResendWriteCount`
so the campaign's total-recovery-writes primary sees them. Their test is
`flight_gate_lane_recovery_contract_test.go`, in the seven-row shape: each
row states a regime and a behaviour, is built from API that predates the
program (hand-built scoreboards for the gap rows, the stall instrument's
two real clients for the timer rows), and is run against merged, 175d82a
and §26.2 so the record says which tree holds which row and why.

| Row | Regime | Behaviour | merged | 175d82a | §26.2 |
|---|---|---|---|---|---|
| 1 | single reliable lane, 2.75 s stall mid-transfer, tight pre-stall interval | writes during the stall at most ⌈log2(stall / interval)⌉ + 1 | fails: the window | fails: second firings | holds |
| 2 | same, queue-inflated pre-stall interval | the same bound | fails | holds by alignment | holds |
| 3 | single reliable lane, queue inflation, draining | no write while the cumulative ack advances (§15.1) | fails: the storm | holds | holds |
| 4 | single reliable lane, one endpoint drop with three later same-lane acks | recovered in one gap round, written once | holds | holds | holds |
| 5 | mixed route, relay item overtaken by three direct-lane acks, relay draining | not written | fails: F11b expires and writes | fails | holds |
| 6 | mixed route, relay item proven by three later relay-lane acks | written in that round | holds | holds | holds |
| 7 | mixed route, relay endpoint drop with direct-lane acks only, no later relay item | recovered within F11b's grace | holds | holds | fails: waits the head's probe |

Row 7 is the trade, written so it is assertable and so merged's row
records where merged is faster: §26.2 recovers that drop by the probe's
backoff rather than the grace, and the row states the bound (the head's
interval, doubling). Rows 1 and 5 are where merged trades against the
metric, buying ordered-stream progress with duplicates, and the file makes
merged fail them by construction rather than by a campaign.

### 27.5 What is not claimed

§26.2 does not shorten the stall, which is the relay's; it does not change
the unreliable lane; and it does not lower the deferral count on a
draining lane, which is the win's own signature. If the definitive
campaign's collapsing seed also shows a longer stall or a larger
outstanding window than its four siblings, the exports of 27.1 will say so
before §26.2's numbers are read against it.

## 28. Ninth round: row 1's deferrals are irreducible and free, its writes are the deferral's own bounds, which the lane rule has made redundant

Written 2026-09-12 against 0ca5c71 and the seven-row table.

### 28.1 Which it is

The first scaled round trip of a stall is indistinguishable from slow
draining by the route's clock, by construction: silence began at the last
acknowledgement, and for one interval after it the test "the route
acknowledged something within the last interval" is true whether the next
acknowledgement is a quarter-interval away or never coming. Deferring the
firings in that window is therefore the cost of not knowing yet, and it is
irreducible. It is also free: a deferral is a re-arm, and the contract
already shows it at one per outstanding item (352 against 360). So the
honest bound on deferrals is one per item outstanding at onset, and it
should be stated that way rather than as a number.

The writes are a different object and are not irreducible. Under §27.3 an
item deferred in that window is re-armed by its backoff to two intervals
later, which is outside the first round trip, where the route reads silent
and the item rides the head. A second firing cannot write inside the
stall by the precedence itself. What wrote 16 to 17 on row 1 is the
deferral branch's own bookkeeping: `TimeoutResendDeferLimit` and the
since-last-deferral term deny a deferral to items that had already used
theirs on the tight, deep lane before the stall, and precedence 2 then
writes them. That is why the residue scales with items outstanding at
onset and vanishes on row 2, where fewer items arrive at the stall with
their deferrals consumed. The second firing is the thing to suppress, and
it is suppressed not by a mechanism but by removing two rules the lane
rule has made redundant.

### 28.2 Why the two rules are redundant for reliable-carried items

The limit and the since-last term exist so that a hole nothing can
acknowledge is deferred once and then retransmitted (§16). For a
reliable-carried item under the lane rule that bound is enforced
elsewhere, case by case on a FIFO lane:

- the item was dropped at an endpoint and the lane keeps delivering: the
  next item sent after it on that route is acknowledged, precedence 1
  writes it, and the gap rule writes it sooner when three have arrived;
- the item is the tail, nothing after it: the lane drains to it and goes
  silent, precedence 3 makes it the route head and probes it with backoff;
- the lane is dead: silent, probed with backoff, and route retirement
  moves the window through `scheduleRetiredReliableCarrierRecovery`;
- the item is merely late in a draining queue: deferred again at each
  firing while the lane acknowledges items sent before it, which is the
  right answer for as long as it is true, and it cannot stay true past the
  item's own position.

There is no case in which a reliable-carried item is deferred without
bound that is not one of these. So with the lane rule on, precedence 2 is
unconditional for reliable-carried items: deferral with backoff, no limit,
no since-last term. Unreliable-carried items are untouched, and with the
lane rule off both rules stand exactly as measured on 175d82a. This is
part of the lane rule's own definition, not a fourth flag.

Effect: row 1's writes become the head's probes, two or three, at any
outstanding count; the relay-only queue-inflation cell loses the 111
limit releases as well, since a late item on a draining lane is never
released into a write; and the deferral count is unchanged, being the
win's own signature.

### 28.3 The contract rows this adds, in the required shape

| Row | Regime | Behaviour | merged | 175d82a | lane rule |
|---|---|---|---|---|---|
| 1, restated | stall, tight interval, 571 outstanding | writes at most ⌈log2(stall / interval)⌉ + 1, and no item deferred twice inside the stall | fails | fails | holds |
| 8 | single reliable lane draining under queue inflation, no drop | a late item is never written while the lane keeps acknowledging items sent before it | fails: the storm | fails: released at the limit (the 111) | holds |
| 9, the trade | single reliable lane, one endpoint drop, one later same-lane item only | the drop is written at its next timer firing after that item's acknowledgement, not at the acknowledgement | holds, by the timer | holds | holds, and the bound is stated: one interval past the proof |

Row 8 is where merged and 175d82a trade against the metric, each buying
a real hole's recovery with duplicates of late ones, and the file makes
both fail it by construction. Row 9 records the one place the removal is
slower than a rule that wrote on the second firing: a proven hole waits
for its own timer rather than being written the moment it is proven, at
most one interval, and the row asserts that bound so it cannot lengthen
quietly. The held-item re-arm defect the stream found belongs in the same
file as its own row: a held item's re-arm is never shorter than one of the
head's intervals, with the ratio check already written.

### 28.4 If the removal is not taken this round

Then the honest bound for row 1 is writes at most the log bound plus the
items that arrive at the stall with their deferrals consumed, which is
what the excess is, and the row should say "limit releases" rather than
leave the number unexplained. But the removal is a narrowing inside a
candidate that is already off by default, it needs no campaign of its own
to be judged (the instrument's row 1 and the relay-only cell's 111 are its
two numbers), and it is what makes the bound independent of the
outstanding count, which is the quantity the parallel-flow cells and the
collapsing seed scale with. I would take it.

## 29. Tenth round: the exhaustion of row 7, its closure by the lone-tail probe, and the irreducible columns of the metric matrix

Written 2026-09-12 under the user's rule that a concession is allowed only
after every angle is examined.

### 29.1 The case

Mixed route. Item X was the last item written to the relay, the relay has
nothing outstanding after it, X was dropped at an endpoint, and at least
three later items reached the receiver on the direct lane and were
acknowledged. Merged writes X at the first ack round after F11b's grace,
`sendTime + scaledRtt`. The lane rule finds no later same-lane
acknowledgement, so X waits for its own timer and the head probe, at most
one interval later, measured at 600 ms. The row asserts that bound.

### 29.2 Every angle, with the argument

| Angle | Works? | Why, from the source and the invariants |
|---|---|---|
| Tell a dropped tail from a stalled head by the sender's evidence | No | Both leave the relay with X unacknowledged and nothing after it to acknowledge; the lane's behaviour is identical until it either delivers X or never does, which is what a timer measures. Structural. |
| The receiver knows more | No | The receiver knows X is missing, not where it is: it cannot tell a Pack in the relay's queue from one that is gone, and it does not know X's lane at all, only that X+1 arrived direct. What it could add to an acknowledgement without a wire change is nothing it does not already imply; with a wire change, "highest relay-received sequence" is precedence 1's evidence, which is below X in the tail case by definition. The information does not exist at either end. |
| The direct lane's acknowledgements say something about the relay | Only that the peer is alive | They travel the relay's reverse leg under merged's reply rule, so they prove that leg and the peer's receive path; the campaign's stalls held the data leg while the reverse leg flowed, so reverse-leg liveness is not data-leg delivery. They rule out a dead peer, which retirement already covers. |
| A shorter probe for a lane with one item outstanding | Yes | The storm was per-item writes across a deep window; a lane has exactly one tail, so a probe conditioned on the lane holding one outstanding item costs at most `AckTailProbeLimit` duplicates per lane per idle period and cannot storm. The path exists: the tail probe paced by `probeRtt`, the window's minimum times `RttScale`, floored at 300 ms, which is never later than the mean-based `scaledRtt` merged's grace waits for. |
| Retirement covers the dead lane | Yes, that subcase only | A retired route moves X through `scheduleRetiredReliableCarrierRecovery` at once; a silently dead route is probed with backoff until its transport's own liveness retires it, and merged's rewrite of X is no sooner. Not the drop. |
| Reachable in production? | Barely as built, yes as a stall | It needs an endpoint drop on the relay: reliable-received Packs wait unbounded at the handoff (`ReliablePackHandoffTimeout −1`), `pack_handoff_drop_count` is zero in every record, and relay drops were refuted on the rig (§9). The instrument makes it by dropping a Pack. The same shape with X stuck in a stalled leg is reachable in every run, and there the probe heals it through the direct lane. |
| Avoid lone tails, or duplicate them at send time | No | The sender does not know an overflow item is the last until the flight has room again, at which point the next item goes direct anyway. |
| Route-level state rather than per-item | Yes, and it is what the probe needs | "X is the oldest outstanding on its route and its sequence number is the route's highest sent" is "the route holds one item", from one more word per route, `highestSentSequenceNumber`, set on write. |

### 29.3 The mechanism, and why it is not a fourth flag

The lone-tail probe, inside the lane rule. At the scoreboard pass, a
reliable-carried hole X that is the oldest outstanding item on its route
and the route's highest sent item, with at least `SelectiveAckGapThreshold`
later selective acknowledgements from any lane, is scheduled as
`sendRecoveryAckTailProbe` at `max(now, X.sendTime + rttWindow.probeRtt())`,
`ackTailProbeCount` bounding it to `AckTailProbeLimit` as today. The
existing tail-probe branch is the same rule gated on
`selectiveGapRecoveryActive`, which the lane rule no longer sets for such
a hole; this re-enables it under the one-item condition. The probe is
written p2p-first, so with room in the direct flight it takes the direct
lane, which is the heal; with none it takes the relay, one duplicate.
Precedence 3's head probe stands behind it unchanged.

Bound, stated so the row can assert it: X is written at
`max(sendTime + probeRtt, third later acknowledgement)`, against merged's
`max(sendTime + scaledRtt, third later acknowledgement)`, and `probeRtt ≤
scaledRtt` in every state of the window (same scale, floor and maximum,
minimum against mean; both the cold floor when unsampled). So the lane
rule with the probe is never later than merged on this row, and earlier
whenever the relay's minimum is under its mean. Storm safety by
construction: the condition holds for one item per route; a stalled lane
with a deep window has no such item, and its head is probed
logarithmically as before. State: one uint64 per route in the existing
fixed array. Cost when X is a stalled lone item rather than a dropped one:
one duplicate, through the direct lane, which delivers it.

Row 7 restated: held, with ours at or before merged, and the trade column
gone. Red today on the built tree, since it waits the head probe.

### 29.4 The two columns I expect to be irreducible, before the matrix is built

1. The relay stall's length. It is the relay's, every tree reads 2.75 s,
   and no recovery-path test should be written against it; the tests are
   against the response (rows 1 to 3, 8).
2. A proven-by-one hole waits its own timer. Precedence 1 at a timer
   firing takes one later same-lane acknowledgement; the gap rule keeps
   three, because acknowledgements for relay-received Packs travel
   p2p-first and a single later acknowledgement cannot be told from X's
   own acknowledgement lost on that lane, which at one per cent would
   write one duplicate per hundred relay items. The wait is at most one
   interval, asserted by row 9, and it is the ack-loss ambiguity that
   makes it irreducible without acknowledgement retransmission, which is
   a wire change. Keep the row as the asserted, bounded trade.

One column where the lane rule as built is behind and should not be
conceded: bytes delivered during a mixed-route relay stall. Merged's
whole-window rewrite goes p2p-first, so as much of the stuck window as the
direct flight admits is delivered through the direct lane within one
interval, at the price of the rest being written into the stalled relay;
the lane rule probes one item per interval. The closure is a bound, not a
mechanism: during silence the probe set is the oldest items on the silent
route up to what the unreliable flight admits, never more, so nothing is
written into the stalled lane and the direct lane's spare room does the
healing. Contract row 10: "during a relay stall with the direct lane live,
the stuck window is delivered through the direct lane at the flight's
rate and nothing is written into the stalled lane"; merged fails the
second half, 175d82a and the lane rule as built fail the first, the
flight-bounded probe set holds both. I recommend building it with 29.3;
together they make every column of the matrix equal or better except the
two above.

Everything else, by construction: the unreliable lane is merged's rules
whole, so no column there; the forced-direct route is merged's less the
reporter; the exchange cells carry no deferral that reaches a second
firing; memory is merged's less the window plus a few words per route,
with the §22 ring to be allocated only when its flag is on, since an off
flag must not retain bytes.

### 29.5 Verdict

Row 7 is not structurally irreducible. The concession §26.2 made was
premature: the information to tell a dropped tail from a stalled one does
not exist at either end, but the response does not need it, because a
lane with one item cannot storm, and a probe paced by the lane's minimum
is never later than merged's grace. Build 29.3 inside the lane rule, keep
row 7 as a held row with the bound above, keep row 9 as the asserted
trade, and write no test against the stall's length.

## 30. Eleventh round: the ordering correction, the audit of my own claims, and the columns that remain

Written 2026-09-12 against 8b65713.

### 30.1 The correction, and the principle behind it

§29.4 told a reader that merged's whole-window rewrite delivers the stuck
window through the direct lane during a relay stall and that the lane rule
was behind on delivery. Measured, merged delivers 48 and 49 frames during
the stall against the lane rule's 51 and 50, for 2,002 and 2,039 writes
into the stalled lane against 7 and 3. The claim was wrong, and the record
should say so in the section that made it.

Two facts make it wrong, and both are invariants rather than tuning. The
receive stream is ordered, so a retransmission advances delivery only by
filling the first missing item; every write past that item buys nothing
until it is filled, whichever lane carries it. And on a striped route in
steady state the unreliable flight is full by construction: the overflow
to the relay exists precisely because the flight admits nothing more, so
a rewrite written p2p-first cannot change lanes while the inner flow is
still sending, and goes where the original went, behind the stall. Room
in the flight appears only after the inner flow has backpressured on the
stalled ordered stream, which is after the stall has already cost a
window of delivery; even then the room is a flight's worth, tens of
kilobytes against a stuck window of hundreds, and the writes beyond it
still land in the stalled lane. The stream's reversion of the wider probe
set is right, and row 10 stands with both halves: delivery does not
separate the trees, writes into the stalled lane separate them by two
orders of magnitude.

The principle, for any later claim: a retransmit heals only if it fills
the first missing item and only if it can take a lane that is delivering,
and the second condition is false on a striped route until the inner flow
stops. A claim that a rewrite heals faster than a probe must state both
conditions and is bounded by the flight's size.

### 30.2 The audit of my sections for the same flaw

| Where | Claim | Standing |
|---|---|---|
| §29.4 | merged's rewrite delivers the stuck window through the direct lane; a flight-bounded probe set closes a delivery column | retracted by 30.1; the column did not exist |
| §29.3 | the lone-tail probe "with room in the direct flight takes the direct lane, which is the heal" | conditional and mostly moot: a dropped tail leaves the relay idle, so the probe is delivered through the relay itself, which is why row 7 closes regardless of lane; a stalled lone tail is healed only with flight room, which 30.1 says is absent while the inner flow sends. The row's bound does not depend on the lane. |
| §29.2, angle 4 | "which is the heal" | the same conditional; corrected as above |
| §27.1 | the duplicates queue behind the originals in the stalled lane and the lane inflates on them | consistent with 30.1 and now measured (2,002 writes, no delivery) |
| §26.1, §26.2 | the head probe as the response to silence, counted, never claimed to deliver during the stall | stands |
| §24.3 | merged's roughly a thousand rewrites for the same stall | a count, stands |
| §19 to §25 | no claim that a rewrite heals; §21.2's throttle claim was withdrawn on other grounds | nothing to correct |

### 30.3 The columns that remain, after row 7 and row 10

On the recovery path, none where merged leads and the column is open. In
detail:

- Row 9, a hole proven by one same-lane acknowledgement waits its own
  timer, at most one interval. Its argument needs restating more
  precisely than §29.4 gave it: the scoreboard state "X unacknowledged,
  later items selectively acknowledged" arises only when the receiver has
  a hole at or below X, since a received X is covered cumulatively by any
  later acknowledgement; the ambiguity is a hole below X, transient, with
  X's own selective acknowledgement lost on the p2p-first reply lane,
  which a single later same-lane selective acknowledgement cannot be told
  from a dropped X. The interval is the protection: it gives the hole
  below time to fill and X's cumulative coverage to arrive. Exhausted:
  a threshold of one reintroduces that spurious write; repeating
  outstanding selective acknowledgements in every snapshot would close it
  without a wire change but multiplies acknowledgement frames by the
  items above a hole, since each is written as its own frame; range-coded
  selective acknowledgements would close it cheaply and are a wire change.
  Recommendation: keep the row as the asserted, bounded trade; the
  no-wire-change closure is named if the matrix ever shows the column
  moving a primary.
- The stall's length: the relay's, every tree reads 2.75 s, no test
  against it.
- Deferrals on a draining lane, thousands against merged's none: re-arms,
  not wire or delivery; if the matrix carries a processing column it
  should be measured rather than assumed, and the expectation is that
  merged's writes cost more than our re-arms.
- Memory: merged's less the direct window plus a few words per route; the
  §22 ring must not be allocated while its flag is off, which is the one
  place we could be behind by 256 bytes a sequence, and it is a
  housekeeping item, not a column.
- The unreliable lane, the forced-direct route and the exchange cells
  are merged's paths or better by construction, as before.

### 30.4 What the definitive campaign is doing

Said plainly: on the recovery path there is no open column where merged
leads, so the campaign confirms magnitudes and rates rather than
discovering a mechanism. It confirms the storm-cell result at twenty
repetitions against the null band, the relay-only cell's bimodality gone
or not at its rate, and the low-bar cells unchanged. What it can still
discover is outside the recovery path and outside the instrument: the
route-generation changes seen in the earlier stalled seeds, which are the
transport's, and the mobile envelope on devices, which is §17's. If a
mixed cell comes back behind merged outside the band, the reading should
start from those, not from the rows the contract already holds.

## 31. Twelfth round: the exchange-h3 hang is merged's on every step, §13.1 is not reached in it, and the missing bound is the multi-client's

Written 2026-09-12 from the source at HEAD and at 89e1633, before the
sixty-five-repetition rerun.

### 31.1 What is ours in the hang: nothing on the send path

The attribution of the escape to §13.1 does not hold on this route. In
both trees `observeUnreliableResendTimeout` halves the flight and then
returns before the release or the forget whenever no reliable route is
available (89e1633 transfer.go 7592 to 7596; HEAD 8437 to 8441). On
`exchange-h3` the hybrid transport publishes `Unreliable`, so
`reliableRoutes` is empty and `reliableRouteAvailable` is false for the
whole run: merged's release is never called either, the item stays
tracked in both, and the window sits at its floor in both. The forget and
the release differ only with a reliable sibling present, and the record
says there was none. So on this route in this state the send path is
merged's byte for byte, which is what the stream's diff of the blocking
machinery found, and the deferral cannot engage (no cumulative progress),
the backoff has nothing to back off, and §13.4 is on the client's receive
path. Three hangs against none are a rate, not a mechanism, until the
rerun says otherwise.

Is §13.1 still right where it is live? Yes, and the proposed softer
forms are backwards. Regrowth is admission, and a lane that acknowledges
nothing is the one lane into which admitting more is pure waste; a window
that "recovers between timeouts" on a dead lane writes more into the
void. The escape from a dead lane is not the flight's to provide.

### 31.2 Why the wait is unbounded: by design, and whose design

The recovery path has a lifetime: `AckTimeout` 60 s and
`UnreliableAckTimeout` 90 s from the item's original send, which a
whole-window rewrite does not refresh (the only refresh of `sendTime`
outside a selective acknowledgement is `receiveContractMissing`). A
non-retained item past its lifetime exits the sequence ("exit ack
timeout"), which surfaces the failure to the caller within ninety seconds.
The twenty items in the hang did not exit because they were retained:
`ip.go:7096` sets `retainAfterAckTimeout` on every Pack of a TCP-socket
recovery-mode flow, deliberately, so that a slow provider does not tear
down a flow whose own TCP recovery owns its lifetime. For those items
`retainPastAckTimeout` skips the exit and the timer rewrites them at its
8 s cap indefinitely: 127 firings across twenty items is the 2,541 to
3,082 writes. So the recovery path did not fail to bound the wait; it
delegated the bound, by design, to the layer that asked for retention.

That layer is the multi-client. It has the signal, a send stall past its
3 s bar, and it reached the verdict, and held it: `markStallHoldOnce`,
"no receiving sibling: uplink unproven". On a route with one lane there is
never a receiving sibling to prove the uplink against, so the hold is
permanent by construction on exactly the route where it matters most, and
the same hold, with the same counter, is in merged. That is the missing
bound, and it is the route manager's and the multi-client's business:
the recovery path can say "this route has acknowledged nothing for T
while holding N retained items", and should say it plainly as a counter,
but the decision to retire a provider that has no sibling is the
multi-client's, and today it declines to make it.

One alternative to a dead path must be checked in the three records
before the report: `MissingContractWriteCount`. A new resident that
receives the rewritten head and answers contract-missing would loop
through `receiveContractMissing`, which refreshes `sendTime` and rewrites
the head with the full contract, without ever advancing the cumulative
acknowledgement; that would be a contract that the churned provider
cannot accept, a different product defect with the same silhouette. If
the counter is zero the path was dead; if it is in the hundreds the head
was answered and refused.

### 31.3 The test, in the contract shape, and no mechanism

No recovery-path change. The deterministic test is a two-client route
with one lane and TCP-socket-mode Packs whose far side stops
acknowledging at a chosen point: it asserts that the sequence exposes the
route's unacknowledged duration and retained count, that no item exits
within the lifetime (the retention holding as designed), that every
rewrite carries the head, and that the multi-client retires the route
within a stated bound of the stall verdict. Merged fails the last row by
construction, since it holds the verdict without a sibling, and the row
is the product finding made assertable. Whether the bound should be the
verdict's bar times a small factor or the transport's own liveness is
the multi-client's design question, not this program's; the row states
the bound so it cannot be forgotten.

### 31.4 What the report should assert

- The mechanism as traced: provider churn during the join leaves a
  single-lane route that never acknowledges again; the flight halves to
  its floor and admission blocks in 2 s waits with nothing on the
  reliable side to offer (`blocked-with-reliable-capacity` zero); TCP
  flow Packs are retained past their lifetime by design; the multi-
  client's stall verdict is held for want of a sibling; every step is
  merged's, including the counters.
- §13.1 is not reached on this route in either tree, so it is not a
  mechanism by which the hangs land on our arm; and it stays right where
  it is reached.
- Three of thirty-three against zero of thirty-three and zero of ten is
  not significant; the rerun at sixty-five decides whether the rate
  differs. If it does not, this is merged's product defect at the rig's
  rate. If it does, the ours-only candidates on this route are exhausted
  on the send path, and the reading should turn to the client's receive
  path and the transport, with the records' `MissingContractWriteCount`
  read first.
- The product finding, independent of the rate: a client pinned to a
  provider that churns during the join can hold a dead route for the
  whole of a workload, because the layer that owns the flow's lifetime
  declines to retire a route it cannot prove against a sibling. The fix
  belongs to the multi-client's verdict, and the test above makes merged
  fail it.

### 31.5 Closure of §31, and where the hang belongs in the report

The two facts §31 waited on are in. The source claim was verified
independently: on both trees `observeUnreliableResendTimeout` counts,
halves and returns at `!policy.reliableRouteAvailable` before either the
forget or the release, so in the hang state the send path is identical
and the attribution to §13.1 is withdrawn with the implementation stream.
And `missing_contract_write_count` and `missing_contract_request_count`
are zero in all three hung runs, so the contract-missing loop of 31.2 is
excluded and the path was dead. §31.2's last paragraph and the third
bullet of 31.4 are therefore unconditional: provider churn during the
join left a single-lane route that never acknowledged again, TCP-flow
Packs were retained past their lifetime by design, and the multi-client
held its verdict for want of a sibling. Nothing in either diff created
it, and nothing in either diff bounds it.

Where it belongs in the report. Not in the verdict on the pull requests,
which it does not bear on, and not in the first line of the summary,
which is about the collapse the reporter found. It should be its own
finding, at the same rank as the other product findings the program
carries (the relay's routine 2.75 s stall of §25, the mobile ceiling of
§17), with the seventeen minutes in its title, because a hang that
outlasts a workload on a route users run is not made smaller by having no
author in this diff. The deflection reading is avoided by saying three
things in its first paragraph: that it is a pre-existing condition in a
third layer, the multi-client's stall verdict, which both diffs leave
untouched; that our arm hit it three times in thirty-three and merged
none, that this is not significant, and that the sixty-five-repetition
rerun decides the rate; and that the program's contribution is the
mechanism traced to its source lines, the deterministic test in the
contract shape that makes merged fail the retirement row, and one
counter, the route's unacknowledged duration and retained count, with the
decision left where it belongs. The burial reading is avoided by the
title and by placing the finding beside the reporter's own M6: their
report's dead-lane theme, that a lane with no liveness signal is never
retired, is the same defect one layer down, and this is its multi-client
analogue, a dead route with no sibling that is never retired. Read that
way it confirms the reporter's line of analysis rather than deflecting
from it, and it tells them where the next liveness signal has to live.

## 32. Thirteenth round: silence is not load-bearing for recovery, it is load-bearing for re-establishment, and the two live on different scales

Design investigation, 2026-09-13, written in parallel with the
implementation stream and to be reconciled with what it lands.

### 32.1 The pattern first

Three candidates fell to the same thing: §22's delivered-bytes bound,
§25's deviation term, and now §27.3's silence verdict. Each took a
decision during the interval in which its estimate could not yet have
learned what it was estimating, and each was wrong for one regime for
exactly that interval. An estimator lags a step by construction; that is
what estimating means. The two that were closed were closed by a
quantity the sequence observes directly (a later same-lane
acknowledgement, the count of items a route holds) or by a bound that
estimates nothing. The rule this program should carry forward: no
decision on the recovery path may depend on an estimate during the
interval it cannot yet have learned; decisions rest on proofs, counts,
or fixed bounds, and an estimate may only pace, never decide.

### 32.2 Whether the two regimes are separable

An alive-but-slow lane and a silent one differ in what the sequence
observes: a queue delivers continuously, so its inter-acknowledgement
gaps are the send spacing however deep it is, while a stall delivers
nothing. The silence test was an inter-acknowledgement-gap test, which is
the right quantity, against the scaled round trip, which is the wrong
scale: a queue whose depth *grows* by ΔD pauses its acknowledgements for
ΔD, and for that ΔD it is observationally identical to a stall of ΔD. So
the regimes are inseparable in principle during a gap and separable only
in hindsight, and any rule that acts differently in the two regimes is
wrong in one of them for the gap's duration, whatever its threshold. M4's
100 to 700 ms steps are such gaps, and the stream's attempt to key the
window to the retransmit interval could not close it because the interval
is the same estimate. That is a finding, not a defeat: it says the rule
must act the same way in both regimes during a gap, and then the only
question is which action costs nothing in both.

### 32.3 Whether silence is load-bearing

For loss recovery, no. §26.2's argument was that a lane has one tail,
so writing more than one item teaches nothing; that argument never needed
silence, and neither does the recovery. During a true stall a head probe
is stuck behind the stall like everything else and is a duplicate when
the stall lifts; on a dead route it is lost; a head dropped at an
endpoint after a stall is proven by the next same-lane acknowledgement
(precedence 1), and a lone tail is proven by the receiver's other-lane
acknowledgements and probed by §29.3 at the lane's minimum. In every
recovery case the probe either does nothing or is a duplicate, and the
re-arm costs nothing in either regime. So at the round-trip scale the
answer is one action: a reliable-carried item whose timer fires and that
no later same-lane acknowledgement has proven lost is re-armed with
backoff, unconditionally. §13.5's deferral and §27.3's probe were two
answers to that one question, and the failure appeared only when both
were live because the boundary between them is the inseparable gap. With
one answer there is no boundary and M4 cannot fail: nothing on that path
reads an estimate.

For re-establishment, yes, and the source says exactly where. A receiver
that has lost a sequence's state drops every non-head Pack ("[r]drop
queue head no contract") and asks for a contract only when a compact
head arrives whose contract it does not hold (`sendContractMissing`,
guarded by `item.head && item.contractFrame == nil`). Nothing else it
sends can start recovery, because a non-head Pack carries no contract to
ask about. The timer's rewrite of `sendItems[0]` through `setHead` is
therefore the only path by which a sequence on a live route recovers a
receiver that silently lost it, and §31's 127 head rewrites were that
path doing its job against a route that happened to be dead. That role is
one write, of the head only, and it has nothing to do with round trips:
it is liveness, and it belongs on the liveness scale.

### 32.4 The rule, on two scales

Round-trip scale, per reliable-carried item at a timer firing:
1. proven by a later same-lane acknowledgement: written, with backoff,
   and a proof arriving at an acknowledgement round schedules the write
   at the proof plus one interval rather than at the item's backed-off
   timer, so the unconditional backoff never delays a proven hole (the
   §30.3 bound made independent of the backoff);
2. otherwise: re-armed with backoff, no condition, no limit.

Liveness scale, per route: a route that has acknowledged nothing at all,
selective or cumulative, for `MinResendInterval`, the 2 s cold floor,
writes its oldest outstanding item once, through `setHead`, with backoff
on that route's probe count; every other item on the route rides it. The
bound estimates nothing: it is the wait the sender already accepts when
it has no evidence. A queue step of under two seconds never reaches it,
which is why M4 passes deterministically rather than by the size of its
steps; a step above two seconds costs one head write, which is what a
step that size deserves. The lone-tail probe of §29.3 stays as it is,
being proof-driven.

What this predicts. M4: zero writes in every run, since no path on it
reads the round-trip estimate. The 2.75 s stall of the instrument: one
head write at 2 s and the next at 6 s, so one inside the stall against
two or three now, and the contract's rows 1 and 2 hold with a tighter
bound. Receiver state loss on a live route: re-established within 2 s
plus the head's return, which is a new deterministic test: a receiver
that drops non-head Packs and installs a full head must see the sequence
resume within that bound, and it is the one row where merged is faster
when its first interval is under two seconds, since merged rewrites the
whole window at every firing; the row states our bound and merged's, as
row 9 does. Dead route: one head write per backoff interval up to 8 s,
as now, until retirement. §31's hang: unchanged, since its path was dead
and its bound is the multi-client's.

### 32.5 Reconciliation with what the stream lands

If the stream lands "recent cumulative progress" as the not-silent
predicate and M4 passes eight of eight, that is right for the wrong
reason: recent-ness is a window, the window is a threshold on the same
inter-acknowledgement gap, and a queue step larger than it reproduces
the failure exactly as the scaled round trip did; M4 passes because its
steps are smaller than the window, not because the regimes were told
apart. The fix that holds for any step removes the estimate from the
recovery decision altogether (32.4's round-trip rule) and moves the one
silence-driven write to a fixed liveness bound where a false positive
costs one duplicate and a true positive is the only re-establishment the
protocol has. If the stream's fix is that, it is right for the right
reason and this section is its justification.

### 32.6 Reconciliation: what landed, measured

Written 2026-09-13 by the implementation stream that took the §32.4 rule
over mid-change, against the tree that carries it.

What landed is 32.4 with one of its three sentences made exact, and its
prediction held to the write. On the round-trip scale a reliable-carried
firing that no later same-lane acknowledgement has proven is re-armed
with backoff, unconditionally: no limit, no since-last term, no estimate
read. The re-arm keeps the rewrite's own timer, due at one, three, seven
intervals, which is what merged's timer runs when it writes, less the
write. The sequence head's re-arm is held at the liveness due time, and
that clamp is the sentence 32.4 needed: without it the first probe lands
anywhere in [2 s, 4 s) of silence by the phase of the head's backoff, and
"at 2 s" is not what the code does. On the liveness scale a route that has
acknowledged nothing at all for `MinResendInterval`, measured from its last
acknowledgement or, for a route that has never acknowledged, from the
send of the oldest item it holds, writes its head; every other item on the
route rides to the head's next write. A proof below the gap rule's
threshold schedules the proven hole's firing at the proof plus one
interval, so the unconditional backoff never delays a proven hole (rule
1's second clause, row 9's bound made independent of the backoff).

The head write's backoff, decided. 32.4 says "with backoff on that route's
probe count" and predicts a second write at 6 s. It is counted on the
head's own send count, on the cold floor: after the write the head is
re-armed at `MinResendInterval` doubled once per write of the head and
capped at `MaxResendInterval`, so a route that fell silent at its last
acknowledgement is probed 2, 6, 14 and 22 s after it and every 8 s from
there. Three reasons. It reads no estimate: it is literally the sender's
cold timer, `resendIntervalForPolicy` with the floor as its base, the
cadence the sender already runs when it has no evidence, which is what a
silent route is. It needs no per-route state, since on a fixed head the
head's writes are the route's probe count, and a head written before the
silence starts one step further along, which is the conservative side and
the rarer case. And the cap is the re-establishment bound: a receiver that
comes back after any outage sees a head within 8 s, which is today's
bound, and a cadence that kept doubling would make re-establishment
latency grow with the outage, which is backwards for liveness. A hybrid H3
head keeps its flat cap, which is never shorter, so it does not race the
QUIC stream's recovery. The alternative measured first, the item's own
scaled interval doubling from the probe, put probes at 2.0, 2.6, 3.8 and
6.2 s on a 300 ms lane, four writes where the cold cadence writes two, and
made the second probe's landing inside a 2.75 s stall a function of the
estimate, the §27.1 signature at the scale of one write.

Measured, `-race`, this tree. M4 passes 10 of 10 with the rule on (2 of 8
failed on the tree §32 was written against). The contract rows, per-item
arm against the lane rule: rows 1 and 2 write 428 and 389 against 1 and 1,
the head at the due time and nothing else; row 1's bound reads 1 and 1
across 88 and 732 outstanding against 44 and 394; rows 3 and 8 write 0 on
both arms; row 9's backed-off proven hole is scheduled 600 ms past the
proof against 8 s; row 10 writes 501 and 539 against 1 and 1 at flights of
4 and 32, with delivery equal; row 11 holds at 38 rides against 1 probe.
Row 12 is new, the liveness cadence itself: a 20 s stall on a 100 ms lane
writes the head 3 times, at 2, 6 and 14 s from the last acknowledgement,
with the fourth due past the stall, and writes nothing else, 1,541 to
1,576 rides across 540 to 563 outstanding. That row is also the check the brief asked for on
a stall past the 8 s cap: the gap from the second probe to the third is
the cap, and the row's lower bound pins that a silent route keeps being
probed within it. The stall instrument (mixed-lane harness, 2.75 s stall
at 200 ms) writes 346 against 2, of which one is the stall's head and one
is the control item described below; the endpoint-drop shape recovers in
1.45 s against 1.72 s.

The "deferred twice" assertion, judged. It was written for §27.3, where
the draining window was one scaled round trip and a backed-off re-arm
landed past it, so no item could be deferred twice inside a stall. §32.4
collapsed that window into the cold floor, so an item held through a
stall's first two seconds is re-armed more than once by design: nothing on
the round-trip scale reads whether the lane is silent, which is the whole
point. The assertion was also unstable rather than merely wrong, reading
310 against 308 on one run and 343 against 307 on another, because a
re-arm count is a schedule count. It is restated in 32's terms: writes are
the metric and are bounded above it; the re-arms must be logarithmic, at
most one per doubling of the item's interval inside the floor, which is
⌈log2(MinResendInterval / RttMinResendInterval)⌉ + 1 per item the lane
held at onset, never one per interval and never one per pass. That still
catches a re-arm without backoff and a re-arm into the past; it does not
try to tell three re-arms from four, which is not a quantity anything
depends on.

What the instrument had been counting. Every row on both arms carried one
write that was not the transfer's: a client publishes its key to the
control destination at start, there is no platform behind these links, so
that one item sits unacknowledged for the run and is rewritten on the cold
cadence, at 2, 6, 14 and 22 s, by the plain timer on the per-item arm and
as a never-acknowledged route's head on the lane arm. It is one write in a
6 s row and four in a 25 s row, and it is why the first reading of row 12
was 7. The contract link now parks that publication until the link
closes, which is why rows 3 and 8 read 0 and rows 1, 2 and 10 read 1
where the earlier readings in this file say 2 and 3. The mixed-lane
harness still carries it, one write, and the probe test's "2" is that
write plus the head.

Two things learned the wrong way first, recorded so they are not tried
again. Pulling the route head forward to the due time from a riding
item's firing, so that the first probe lands at the due time when the
head is not the sequence head, is wrong as stated: after the first probe
the head's next write is legitimately in the future and the due time,
being last acknowledgement plus the floor, is stale, so every ride pulled
the head back and every ride became a probe (349 writes on the stall
instrument). The due time would have to advance to the next probe, which
is per-route state; the clamp on the sequence head gives the property
where it matters, since only a sequence-head write re-establishes a
receiver, and a route head that is not the sequence head is probed at
its own next firing, at most one cap later. And the liveness cadence
indexed on the head's count as if the first send were count zero re-armed
the first probe at 8 s instead of 4; the first send is count one.

A defect the full suite found under load, and its row. Row 10 at a
flight of 32 failed inside the full suite with 1,679 writes on the lane
arm, every one an endpoint-drop verdict, no probe, and a 5.2 s lane gap on
both arms, where standalone it writes 1. Under parallel load the link's
relay does reorder, since its forwarder gives each frame its own latency
goroutine, and once the link counted them it read 2 to 33 inversions a
run; but the inversions write nothing, because a swapped pair is
acknowledged milliseconds later and an item's timer almost never fires
inside that window. The cascade is attribution. `observeLaneAck` credited
an acknowledgement to the item's last-written route, and the copy the
receiver acknowledged need not be the last one written: a direct-lane item
whose acknowledgement is late fires its unreliable timer, is resent
p2p-first, finds the direct flight full and lands on the relay, and its
original direct copy is then acknowledged; credited to the relay, that
acknowledgement carries a current sequence number and proves every relay
item below it dropped, the whole stalled window, each written p2p-first
into a full flight and so into the relay, which fills, blocks the sender,
and is the extra 2.4 s of gap. The same crediting fed the scoreboard's
per-lane counts and the route's last-acknowledgement clock. It was in
§26.2's design, not in §32's change, and it is what a mixed route does
whenever the receiver is slow, so the campaign's mixed cells would have
met it. The fix is one mark on the item, set when a write lands on a
route other than its previous one: such an item's acknowledgement is
credited to no lane, it is not a lane item for the timer's verdict or the
gap rule's count, it is not a route head, and it keeps merged's timer.
Row 13 pins it: three later items written direct, then relay, then
acknowledged credit the relay with nothing, prove no hole and write no gap
recovery, while the same three on the relay alone prove and write. Under
the same parallel load row 10 then writes 1 on both flights with up to 33
inversions, and the contract link reports its inversions on row 10's log
line so the premise is visible. The mixed-lane harness keeps its
per-frame delivery.

The default. This tree turned the rule on, in its own commit after the
mechanism's, so that a campaign verdict the other way would be one revert.
That is where the evidence stood at the time of writing, the device series
at 0.4 against 4.8 Mbit/s with M4 and every row green, and it was not this
stream's decision: the relay queue-inflation cell's re-run against this
implementation was named here as the measurement that decides it. The
re-run came in the other way, §33, and the revert this paragraph arranged
for has been taken. The rule ships off by default; the paragraph is kept
because the pre-registration is part of the record.

Not done, and why. The receiver-state-loss row 32.4 names, a receiver that
drops non-head Packs and installs a full head, is not written: the
in-process links run no-contract peers, on which a lost receiver installs
the next head without asking for a contract, so the row would measure the
endpoint-drop verdict rather than the liveness write; it wants a
contracted link, which this package's instruments do not build. The
liveness write's cadence is pinned by row 12 instead. The campaign's
relay queue-inflation cell, twenty runs per arm on the pre-fix rule with
level medians and three long-tail runs of 728, 263 and 41 s on the rule's
arm, is the M4 defect at M4's rate; on this tree row 8, the same schedule
in process, writes 0 over 6,000 messages with the rule on, and the
prediction for the re-run is that the tail is gone and the medians stay
level. If the re-run keeps the tail, it is not the second firing, and the
exports of 27.1 are what remain.

## 33. Fourteenth round: the re-run kept the tail, the tail is a wedge, and the rule's suppression is what holds it

§32 closed on a prediction and a branch: the relay queue-inflation cell
re-run against `eeca11f` would lose its long-tail runs, and "if the re-run
keeps the tail, it is not the second firing". The re-run kept the tail. This
section records what the tail actually is, which is not what either side of
that branch assumed.

### 33.1 The measurement

Arm `9668969`, that is `eeca11f` plus the default-on flip, sdk `4fcce9f`,
server `bf9fac6a`. Both states built from that one commit through the
feature dimension, so nothing but the setting differs. Route `exchange-h1`,
profile `mixed-relay-queue-inflation-3s`, workload `tcp`, download, twenty
repetitions per state on seeds 20260910..20260929, the same seeds as the
`61c1281` run. Recorded as `flightgate-fixlane-20260913` in
`tests/PERFVAR-MEASUREMENTS.md`.

Medians held, as predicted: 16.08 Mbit/s with the rule off against 16.04
with it on, 16.7 seconds in both. The tail did not go. Three runs over 30
seconds with the rule on, at 291, 193 and 35 seconds, against the old
build's 728, 263 and 41.

What `eeca11f` did buy is real and should be kept in view: no failed runs in
either state, where `61c1281` failed one per state; the worst run 291
seconds rather than 728; dead windows on the rule-on state 94 rather than
200. The two-lane attribution fix and the backoff re-arm made the mode
shallower and stopped it killing a run. They did not make it stop happening.

### 33.2 The tail is two modes, and only one of them is ours

Splitting at 100 seconds separates two behaviours that one "over 30 seconds"
count hides. Pooled over both builds, forty runs per state:

| Mode | rule off | rule on | Fisher two-sided |
| --- | ---: | ---: | ---: |
| deep, over 100 s | 0 of 40 | 4 of 40 | 0.116 |
| shallow, 30 to 100 s | 3 of 40 | 2 of 40 | 1.000 |

The shallow mode is the cell's. It occurs at the same rate in both states
and it predates the rule. The deep mode has never occurred with the rule
off, in forty runs across two independently built arms, and occurs twice in
twenty in each build with it on. Fisher does not reach 0.05 at these counts,
so this is an association and not yet a proof; it is, however, the same
result twice, and the counts are 0 against 2 both times.

Dead windows separate the modes with no overlap. The four deep runs carry
35, 55, 49 and 145 dead windows. Every other run of all eighty carries 0 to
7.

### 33.3 The deep mode is a wedge, not slowness

The per-window trace is the important artefact, because "193 seconds" and
"1.4 Mbit/s" both suggest a transfer that ran slowly, and it did not. Rate
per five-second window, rule on, the 291-second run:

```
2 0 0 0 7 1 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 2 0 0 0 0 0 18 17
```

and the 193-second run:

```
8 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0
0 2 0 0 0 0 12 17 15
```

Against a normal run of the same cell, `14 17 17`, and the 52-second shallow
run, `8 0 3 1 0 4 0 0 11 17 14`.

The transfer wedges within the first ten seconds, moves nothing at all for
two hundred to two hundred and fifty seconds, then releases and completes at
the cell's full rate, 17 to 18 Mbit/s, in the last two or three windows.
Nothing degrades. Something holds, and then stops holding.

That rules out the readings this program has been carrying. It is not the
relay being slow, because the release rate is the healthy rate. It is not a
lost route, because no route changes at the release. And it is not a longer
relay stall, which 33.4 shows directly.

### 33.4 The cumulative-ack gap does not discriminate, and this corrects §27.1

The two relay-stall exports were added to explain runs like these, and on
this cell they do not. The gap maximum is a per-run lifetime statistic
spanning route setup and every workload, not the measured transfer, and its
values overlap completely: with the rule off, runs that finish in 16.7
seconds reach 26.7 and 28.3 seconds of cumulative-ack gap, as large as
anything the deep runs show. Median gap on a normal run is 10 to 15 seconds
in every state.

So every run of this cell contains a long relay stall, including the ones
that finish in 16.7 seconds. The stall is the cell. What separates a
16.7-second run from a 291-second one is what the sender does during it.

The resend timer reads between 0.30 and 8.00 seconds in every run of every
state while the relay goes 10 to 36 seconds without advancing its cumulative
acknowledgement. A stall three to four times the timer's own ceiling is a
product finding independent of any arm in this program, it is unchanged by
`eeca11f`, and it belongs in the final report on its own.

### 33.5 The rule's suppression is what holds the wedge

The ratio of whole-window timeout writes to deferrals is the direct measure
of the rule doing its job: of the recovery work the sender has queued and
could write, what fraction does it actually write. On the nine wedged runs
across both builds it separates the two states with no overlap at all.

| State | run | wedge s | timeout writes/s | write-to-defer ratio |
| --- | ---: | ---: | ---: | ---: |
| rule off | fix 12 | 52.0 | 43.7 | 0.60 |
| rule off | 61c 11 | 31.9 | 62.5 | 0.76 |
| rule off | 61c 20 | 61.2 | 40.7 | 0.65 |
| rule on | fix 7 | 34.7 | 13.3 | 0.07 |
| rule on | fix 6 | 193.3 | 5.9 | 0.18 |
| rule on | fix 8 | 290.7 | 6.3 | 0.20 |
| rule on | 61c 13 | 41.3 | 0.3 | 0.00 |
| rule on | 61c 7 | 262.7 | 6.1 | 0.23 |
| rule on | 61c 17 | 727.6 | 0.8 | 0.20 |

Rule off, 0.60 to 0.76; rule on, 0.00 to 0.23. Every wedge that cleared in
under a minute with the rule off was writing 41 to 63 recovery messages a
second while it did. Every deep wedge was writing 6 or fewer, while
deferring 27 to 32 a second, which is the sender holding work it has, not a
sender with nothing to send.

This is the rule behaving exactly as designed. It withholds a firing that no
later same-lane acknowledgement has proven. During a relay stall no
acknowledgement arrives on that lane by construction, so the rule's
precondition is the stall's own definition, and the rule withholds precisely
the traffic that the rule-off runs use to get out. The mechanism is
self-sustaining: the longer the lane stays unproven, the longer it stays
suppressed.

State the limit of this honestly. Wedge length against write rate over the
nine runs is Spearman −0.50, not −1.0, and two rule-on wedges cleared in 35
and 41 seconds while writing almost nothing. Suppression is not the whole
account of how long a wedge lasts; there is at least one other release path.
What the ratio establishes is that the rule is suppressing, cleanly and
without overlap, and that the deep mode only exists where it suppresses.

### 33.6 What this means for the default, and what to measure next

The rule is not ready to ship on by default. §32 said the relay
queue-inflation cell's re-run was the measurement that decides it; the
measurement is in and it decides against, and turning the default back off
is the one revert §32 arranged for.

The cold-cadence head probe added in `eeca11f`, every 8 seconds after 22,
was the intended escape hatch from exactly this state and it did not open
it: four deep wedges lasted 193 to 728 seconds with it in place. So the next
question is not another campaign on this shape. It is why an 8-second head
probe against a queue-inflated relay does not clear the wedge that 40-odd
writes a second clears in under a minute, and whether the answer is the
probe's rate, its target, or that the head is not the item the receiver is
waiting on. Rows 12 and 13 assert the probe's cadence in process and pass,
so the in-process instrument does not reach the condition; the cell does.
The exports of §27.1 remain what we have, and 33.4 says they are not enough
on their own.

### 33.7 The suppression is across every recovery category, not just timeouts

§33.5 uses whole-window timeout writes because that is where the rule acts.
Total recovery writes, this program's designated primary, says the same
thing and says it about the whole recovery path. Per wedged run, the sum of
timeout, selective-gap, ack-tail-probe, cumulative-probe and carrier-change
writes divided by the wedge's length:

| State | run | wedge s | total recovery writes/s |
| --- | ---: | ---: | ---: |
| rule off | 61c 11 | 31.9 | 78.8 |
| rule off | fix 12 | 52.0 | 63.9 |
| rule off | 61c 20 | 61.2 | 57.4 |
| rule on | fix 7 | 34.7 | 36.1 |
| rule on | fix 8 | 290.7 | 13.5 |
| rule on | fix 6 | 193.3 | 11.1 |
| rule on | 61c 7 | 262.7 | 10.3 |
| rule on | 61c 17 | 727.6 | 1.7 |
| rule on | 61c 13 | 41.3 | 0.3 |

A normal run of this cell writes 0.06 a second, so every row here is the
recovery path running hot; what differs is how hot. The three rule-off
wedges occupy 57 to 79 and all cleared inside a minute. The four deep wedges
occupy 1.7 to 13.5, a band that does not touch the rule-off band. The rule
is not merely deferring timeouts during a wedge, it is running the entire
recovery path at a fifth to a fortieth of the rate the rule-off runs use to
get out.

Two rows do not fit a rate-only story and are stated rather than smoothed.
The 34.7-second rule-on wedge wrote 36.1 a second, between the bands, and
cleared quickly. The 41.3-second rule-on wedge wrote 0.3 a second and also
cleared quickly, so something other than writing can end a wedge. Any
mechanism proposed for 33.8 has to allow that.

### 33.8 Where the next round should look

The head probe is not obviously the culprit and the counters say so. During
the deep wedges the selective-gap path is live, 1,490 and 701 writes, 5.1
and 3.6 a second, so holes are being addressed and not only heads rewritten.
The gap rate is a third of the rule-off runs' 16 a second, which is the same
proportional suppression as everything else rather than a distinct defect.

So the question is not "which item does the probe target". It is why a
recovery path running at 10 to 13 writes a second cannot clear a queue-
inflated relay that the same path clears in under a minute at 57 to 79. Two
readings fit and the instruments here cannot separate them: the relay's
queue drains only when offered more than some rate, so a suppressed sender
never reaches the drain threshold; or the suppression is incidental and the
wedge is a receiver state that only a burst dislodges. The 41-second wedge
that cleared at 0.3 writes a second argues against the first being the whole
story.

Deciding between them needs an instrument this package does not have: the
relay's own queue depth over the wedge, alongside the sender's offered rate.
That is a harness change, not another twenty-repetition campaign on this
shape, and it is the recommendation this round ends on.

### 33.9 The wedge is not the relay cell's; it reached the loss storm cell and failed a run

Stage 2 of the same campaign ran the two storm cells on this arm, both
states, twenty repetitions each: route `p2p-fast+exchange-h1`, `clean-lan`
and `mixed-direct-loss-100bp`, `tcp-parallel`, download. These are cells
whose rule-off runs finish in 3.2 and 3.7 seconds.

The storms stay gone in both states, zero storm runs in eighty, which
carries `175d82a`'s result forward to `eeca11f` unchanged. On rate both
cells are inside the null band, +3.8 % and +1.0 %. The rule neither helps
nor hurts the thing these cells were built to watch.

It added one wedge, and that wedge is the clearest artefact this program has
produced. `loss-100bp` run 6, rule on, failed at the workload stage after
712.8 seconds with 142 of 143 progress windows dead. Its recovery counters
for those twelve minutes:

| Counter | Value |
| --- | ---: |
| whole-window timeout writes | 11 |
| selective-gap writes | 10 |
| ack-tail-probe writes | 9 |
| cumulative-probe writes | 0 |
| carrier-change writes | 0 |
| deferrals | 1,146 |
| total recovery writes per second | 0.04 |
| write-to-defer ratio | 0.01 |

The sender held one thousand one hundred and forty-six pieces of queued
recovery work and sent thirty. The transfer never resumed. This is not a
suppression that slowed recovery down; it is a suppression that switched it
off and left nothing to turn it back on.

Pooling every cell measured on this rule, the relay queue-inflation cell on
both builds plus these two, one unit per scenario-run:

| | rule off | rule on |
| --- | ---: | ---: |
| scenario-runs | 80 | 80 |
| runs over 100 s | 0 | 5 |
| failed runs | 1 | 2 |

Fisher two-sided on the deep count is 0.059. Three cells, two independently
built arms, and the rule-off column empty in every one.

Run 6's environment is worth stating exactly, because it is benign. The
direct lane was 1 Gbit/s, 10 ms, 1 per cent independent loss. The relay lane
was 1 Gbit/s, 100 ms, no loss. No queue inflation on either, no blackhole,
no processing delay, no scheduled events at all. A 1 per cent loss link at
gigabit rates is the mildest impairment in the whole matrix, and the rule
wedged a transfer on it for twelve minutes and then failed it.

This changes what 33.8 asked for. The question is no longer why a suppressed
recovery path cannot clear a queue-inflated relay, because run 6 had no
inflation anywhere and still wedged for twelve minutes at 0.04 writes a
second. Whatever the wedge is, the rule
reaches it on more than one kind of link, and the common factor on the
sender's side is that the lane's precondition, a later same-lane
acknowledgement, is exactly what a wedged lane cannot produce. A rule whose
release condition is unreachable from the state it creates has no way out
of that state by itself, and run 6 is what that looks like when nothing
external happens to arrive.

That is the defect to fix before this mechanism runs unattended: the rule
needs a bound that does not depend on the lane it is suppressing. The
liveness cadence was meant to be that bound and run 6 shows it is not, since
its head writes are inside those eleven timeout writes. Sizing that bound is
the next round's work, and it should be sized against the rule-off wedges,
which clear in 32 to 61 seconds at 57 to 79 recovery writes a second.

### 33.10 The wedge does not reproduce in process, and that is the useful part

A root-cause row for §33 is added,
`TestSilentLaneLongerThanTheProbeCadenceStillDrains`. It holds the relay
silent for twenty seconds, well past the eight-second cap on the head
probe's cadence, and measures what each arm does once the lane returns,
subtracting the stall both arms wait out. It asserts two things: that the
rule-on arm drains within four times the rule-off arm's post-stall time, and
that its write-to-defer ratio does not fall under one per cent, the figure
the campaign's failed wedge sat at for twelve minutes.

It passes, and it passes comfortably. Rule off drains in 23.9 seconds
writing 1,027 whole-window retransmits; rule on drains in 23.0, faster, on
seven writes and seven probes. The rule does exactly what it is for.

Three further shapes were probed and none of them wedge either:

| Shape | rule off | rule on |
| --- | --- | --- |
| mixed lanes, 1 % direct loss | 3.80 s, 1 write | 3.81 s, 1 write |
| mixed lanes, 1 % direct loss, 20 s relay stall | 24.5 s, 474 writes | 23.4 s, 9 writes |
| relay only, 1 % endpoint loss | 2.70 s, 1 write | 2.69 s, 1 write |

The second row is the campaign's `loss-100bp` shape with a stall added, and
it is the closest this package can get to run 6. The rule-on arm wrote nine
recovery messages against 1,152 deferred, a write-to-defer ratio of 0.008,
lower than the wedged run's 0.01 — and it still drained a second faster than
the rule-off arm.

So the suppression reproduces exactly and the wedge does not. The deep mode
is not a property of the rule's arithmetic, which this package can exercise
completely; it is a property of the rule's arithmetic meeting something
these links do not model. What they do not model is enumerable: the
in-process peers carry no contract, there is one send sequence rather than
many, the receiver is a test sink rather than a Resident with a head slot
and Pack reassembly, and the relay is a channel rather than an Exchange.

That is the localisation worth having, and it redirects 33.8. The
instrument the next round needs is not the relay's queue depth, which run 6
says is not the variable, but a perfvar-side export of the receiver's own
state during a wedge: whether its head slot advanced, what it was waiting
on, and whether a contract event coincided with the release. The wedges all
end abruptly at full rate, so something arrives; the sender's counters
cannot say what, and this package cannot produce the condition to ask.

Until that exists the rule stays off by default, which is where §33.6 and
the revert have already put it. The row above is kept as a guard: if a later
change makes a silent lane fail to drain in process, it is caught here
rather than in a twenty-repetition campaign.

## 34. Fifteenth round: the proof chain is not always reachable, unconditional re-arm is unsound, and the sound bound is lane position

Design analysis, 2026-09-13, against §33 and the counters of run 6.

### 34.1 The argument holds, in a more general form

§32.4 rested on "every recovery case is covered": a head dropped after a
stall is proven by the next same-lane acknowledgement, a lone tail by
§29.3, a dead route by the liveness write. The coordinator's objection is
right and it is wider than one case. The rule's proof for a reliable-
carried item X is "a later same-lane item was acknowledged", and that
needs three things the absence of X can itself remove: a later same-lane
item must exist (the sender admits nothing new once its budget is full of
items queued behind X, and §29.3 covers exactly one unproven item per
route, never thirteen); it must be delivered (a receiver whose ordered
stream is blocked at X buffers above the hole to its budget and then drops
what arrives, on every lane, so the later same-lane items are dropped
too, unacknowledged and themselves unproven, and every link of the chain
waits on the next); and its acknowledgement must reach the lane (eeca11f's
attribution mark credits no lane for a rewritten item). So on a FIFO lane a
later same-lane acknowledgement proves loss when it arrives and proves
nothing when it does not. §32 treated "not proven lost" as "proven late",
and re-armed on it without bound. That is the error, and it is exactly
what a wedge that ends only when something external arrives looks like.

The liveness write was meant to be the proof-independent bound and it
cannot be, because it is keyed on route silence and the wedged route is
not silent: any acknowledgement credited to the route, a gap write's, a
probe's, resets it, and the counters say it fired about nine times in
twelve minutes where a 2 s bound on a silent route would have fired
ninety. A route can be alive and X can still be the one item nothing will
ever acknowledge. The bound has to be per item, and it has to be about
X's own neighbourhood on its lane, not about the route.

### 34.2 What the wedge is, and why the instrument could not build it

The shape the invariants pick out, and the one to build: a direct hole H
blocks the receiver's ordered stream; the sender keeps striping; the
receiver buffers above H to its budget and then drops what arrives,
including a run of relay items X, X+1, …; H is recovered by merged's
direct rules within seconds and the receiver's head advances to X, which
is now the head hole; the dropped relay items are unproven because every
later relay item was dropped with them, the sender's budget is full of
acknowledged items queued behind X, and the route stays "alive" on other
acknowledgements. Nothing writes X. The in-process sink drops nothing at
a budget, so §33.10's links cannot reach it; a receiver with the real
`ReceiveBufferSettings` budget can, and that row is specified in 34.5.
The release the campaign saw at full rate after minutes is consistent
with a receiver-side timeout clearing the head hole, and the receiver-
state export §33.10 asks for is still the right instrument to confirm it.

### 34.3 The correct bound

The estimate returns as a pacer and stays out of the decision. At a
reliable-carried item's timer firing, decided by same-lane
acknowledgements only:

1. any same-lane acknowledgement above X, ever: X was delivered and its
   own acknowledgement lost, or X was dropped; either way write X (a
   duplicate into a live lane at worst), with backoff;
2. else any same-lane acknowledgement below X since X's last firing: the
   lane is draining toward X, and on a FIFO lane X is next; re-arm with
   backoff;
3. else nothing on X's lane has moved since X last looked: if X is the
   lane's oldest unacknowledged item, write it with backoff; if not,
   re-arm it to that head's next firing.

And one promotion: when a lane head's acknowledgement arrives, the new
lane head's next firing is set to `now + probeRtt`, the lane's minimum,
so that a batch the receiver dropped drains at one lane round trip per
item rather than one backed-off interval per item, while a post-stall
drain, whose acknowledgements arrive below the new head inside that round
trip, re-arms it under rule 2 and writes nothing.

What this does, regime by regime. Queue inflation (M4): the head's
acknowledgement comes at the queue depth D, and items below it are
acknowledged continuously until then, so rule 2 holds at every firing
before D and the head is never written; a pause in acknowledgements
longer than twice the head's interval writes the head once, which is the
honest bound and is above M4's steps. A stall (§24): nothing moves, the
head writes at I, 2I, 4I and the rest ride it, the probe count as before.
The wedge: X is the lane head and nothing below it exists, so rule 3
writes it at its first backed-off firing whatever the route's other
acknowledgements say; X's acknowledgement promotes X+1 to one round trip,
nothing arrives below X+1, it is written, and the dropped batch drains at
one round trip per item. Re-establishment: the sequence head is a lane
head, so it is written on its own timer when nothing below it moves, with
`setHead`; the cold-cadence probe of eeca11f is redundant and goes. A
dead route: the head writes at its backoff until retirement, as now.

The residual cost, stated: a lane head whose acknowledgement pause
exceeds twice its interval is written once per such pause, on any lane.
That is one duplicate per pause, it is the price of a bound that owes
nothing to proof, and it is the only place the sound rule writes where
the unconditional one did not.

### 34.4 Whether the mechanism can be saved

Yes, in this form, and the unconditional re-arm is withdrawn as unsound:
it removed the one bound that holds when the proof chain is broken, and
replaced it with a route-level liveness bound that any acknowledgement on
the route defeats. The rule does not need "the estimate it was designed
to remove" in merged's sense, where the estimate alone decides and every
item writes when it fires; it needs the estimate to say when to look and
the lane's own acknowledgements, above and below the item, to say what
was seen. §32.1's rule stands and is what 34.3 follows: the estimate
paces, facts decide. §32.3's claim that every recovery case was covered
is retracted; the case it missed is the one in which the receiver's own
drops remove the proof.

### 34.5 Tests, in the contract shape

| Row | Regime | Behaviour | merged | unconditional (eeca11f) | 34.3 |
|---|---|---|---|---|---|
| 14 | two lanes, a direct hole, the receiver's budget dropping a run of relay items above it, the hole then recovered | the dropped run is recovered within one backed-off interval plus one lane round trip per item, and the transfer resumes | holds, by whole-window rewrite | fails: unproven, re-armed for the run's life | holds |
| 15 | one reliable lane, 20 s stall | writes during the stall at most the probe count; after it, promotion writes nothing while the drain continues | fails | holds | holds |
| 16 | M4's profile | no write while acknowledgements below the head continue; one head write per pause longer than twice its interval, asserted as the bound | fails | holds | holds |
| 17 | one reliable lane, one item dropped at the endpoint, later items delivered | written at its next firing after any later same-lane acknowledgement | holds | holds | holds |

Row 14 is the wedge made deterministic and the row the unconditional
rule fails by construction; it needs the receiver's real budget, which
the in-process link must be given. Sizes and allocation unchanged: the
per-route slots already hold the sequence numbers the rules read.

### 34.6 Now

The rule stays off. Build 34.3 behind the same flag, row 14 first, since
it is the row that decides whether the wedge has the shape 34.2 says;
then rows 15 to 17; then the relay queue-inflation cell and the two storm
cells at twenty repetitions against `175d82a`, judged on total recovery
writes, dead windows and the count of runs over 100 s, which must be
zero. The receiver-state export stays on the list, because if row 14
holds and the campaign still wedges, the wedge is not the shape the
invariants pick out, and that export is what would say what it is.
