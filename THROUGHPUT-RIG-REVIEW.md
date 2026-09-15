# Throughput on a real relay path: window rule review and receiver fixes

A companion to the throughput report (THROUGHPUT-REPORT, "window rule shipped"). That report measures the
delivery-sized window rule in process, at 200 and 400 ms, on one layer. This one measures whole stacks on physical
hosts through the platform relay. It explains what the commits in this branch change and why.

## Rig and method

| | |
|---|---|
| Path | kernel TUN client -> websocket (H1) platform relay -> provider -> synthetic origin inside the provider |
| Low RTT | client and provider in one datacenter, network RTT about 0.3 ms |
| Long RTT | a second client about 100 ms away |
| Client | a connect client with a relay-only switch, and a process budget of 384 MiB set before any settings are built (to emulate a budgeted desktop) |
| Runs | 30 s downloads, arms alternated or rotated inside every repetition, a fresh provider per run, md5 of the executed binaries stamped per run |
| Statistics | medians; paired where noted; n = 3 or 4 per cell |

Absolute rates are specific to this rig. Ratios within a table are the result.

## 1. The window rule, rolled back in the same binaries

Only `SetWindowSizing(WindowSizingConstant)` differs between the two arms, applied on both ends.

| Path | Flows | Rule on | Rule off | Effect of the rule |
|---|---|---|---|---|
| 0.3 ms, main + this branch's receiver fix | 8 | 270 Mb/s | 656 | -59% |
| 0.3 ms, main + this branch's receiver fix | 1 | 251 | 772 | -67% |
| 0.3 ms, main alone | 8 | 261 | 466 | -44% |
| 0.3 ms, main alone | 1 | 246 | 593 | -59% |
| 100 ms, main + receiver fix | 1 | 198 | 126 | **+57%** |
| 100 ms, main + receiver fix | 8 | 80 | 129 | -38% |

- **At long RTT the rule does what its design says, for one flow.**
- **On a short path it loses badly at every flow count.** This is the report's §8.3 short-path defect, now measured on a
  real path: when the ack compression delay exceeds `rtt_min`, the growth factor falls below 1.
- **With a budgeted client it also loses 38% on eight flows at 100 ms.**
- **Default it off until it is RTT and loss aware.** It stays one call away:
  `SetWindowSizing(WindowSizingFromDelivery)`.

### Why a larger window does not pay on the relay path

With the constant window raised by hand, the loss rate rises with in-flight bytes. Resends minus duplicate arrivals,
8 flows, receiver queue matched:

| Window | Resends minus duplicates | Throughput |
|---|---|---|
| 2 MiB | ~180/s | 639 Mb/s |
| 4 MiB | ~505/s | 613 |
| 8 MiB | ~635/s | 499 |

At 1 flow, 2 MiB gave 745 Mb/s and 4 MiB gave 322.

The relay drops about 0.5% of Transfer items at the default window, steadily. By elimination the drops are not on the
provider (route writes never fail) and not on the client (every drop site before `ReceiveSequence.receive` reads zero).
The candidate is the relay's non-blocking forward queue: `resident.go` `processClientForward`, 4096 messages,
`ForwardTimeout` 0. A bigger window means bigger bursts into it.

## 2. Receiver acks (commit: order selective acks and end ack compression when a hole opens or fills)

**What we measured.**
- Injected +0.5% item loss after the provider's route write, the same position as a relay drop, cost 33% of
  throughput at 8 flows (4/4).
- Each loss produced about 6 resends, about 85% of them duplicates.
- Timed per hole: about 18 holes/s waited about 11 ms, one `AckCompressTimeout`, for their proof acks or head ack.
  That was about 21% of wall time head-blocked.

**The two causes, both on the receiver.**
1. Selective acks are written in Go map order. The sender reads acks between pack writes, so a partial batch in random
   order "proves" the neighbours of one real hole lost.
2. A hole waits a full compression interval for the acks that end it.

**The change.**
- Selective acks are written in sequence order.
- The compression wait ends early, at most once per snapshot, when `AckGapWakeSelectiveCount` (3) selective acks are
  pending, or when a head ack advances under selectively acked items. The steady in-order ack rate is unchanged.

**Result, production code on a clean provider, 0.3 ms, alternating pairs.**

| Flows | Before | After | Pairs |
|---|---|---|---|
| 8 | 600.5 Mb/s | 709 (+18%) | 6/6 |
| 1 | 707 | 812 (+15%) | 4/4 |

- In the diagnostic build, head-blocked time fell from 20% to 5%, resends went down and duplicates went to about zero.
- At 100 ms throughput was unchanged (window-bound) and there was no stall regression.
- With the window rule rolled back, this change on top of main is worth +41% (8 flows) and +30% (1 flow).

**Tests.** Ordered writes (64 shuffled selective acks); early write for a provable hole and for a filled hole; guards
for steady head acks, below-threshold selective acks and the disabled setting; no allocation in steady state. The tests
fail before the change, and four mutations are each caught.

## 3. Provider standby (commit: release the provider standby when no pinned transport can connect)

**The problem.** `FamilyPlatformTransportGroup` enables the family-agnostic standby only after `StandbyDelay` (15 s). The
`connect-v4` / `connect-v6` names don't resolve in the spaces we tested: NXDOMAIN on one, no A/AAAA records on the other.
So every pinned dial fails at once, and a provider is unreachable for 15 s after every start.

**Measured.**
- Time from restart to a client's first successful fetch: 22.2 s -> 2.2 s median (10/10 paired).
- About 10% of clients that obtained a contract inside that window dropped the provider after their 30 s evaluation.

**The change.** The standby is released at once when every pin's name does not resolve (a not-found `*net.DNSError`),
or the pin is held. Any other failure still waits out the delay.

## 4. Aggressive branch only (commit: merge ready single-frame logical groups into one H1 Pack)

**The problem.**
- Every upstream IP packet is sent as a one-element logical group, and `processLogicalGroupChunk` returns before the
  ready-drain group loop.
- So each packet becomes one TransferFrame and one websocket message. Kernel TCP ACKs at 8 flows are 40-50k
  messages/s, more than the download's data frames.
- Relay work is per message.

**The change.** On H1-only sequences without flow isolation, already-queued whole logical groups with the same `Ack` and
`ForceUnwrapped`, within the ready-drain frame and byte limits and contract-safe, are merged into one item. It never
waits.

**Measured.**
- Upstream websocket messages: 41-50k/s -> 12.5-13k/s.
- Throughput: +13% at 8 flows (4/4 paired).

## 5. Final comparison of the four stacks

Medians, Mb/s. PR A = main + fixes 2 and 3 + rule default off. Aggressive = PR A + fix 4.

| Path | Flows | Beta (pre-main receiver fix) | main | PR A | Aggressive |
|---|---|---|---|---|---|
| 0.3 ms | 8 | 691 | 288 | 622 | **722** |
| 0.3 ms | 1 | 806 | 248 | 248 (3 of 4 runs stalled) | **784** |
| 100 ms | 1 | 132 | 197 | 130 | 50 (2 of 3 runs stalled) |
| 100 ms | 8 | 129 | 81 | 130 | 129 |

The stalled cells are one intermittent single-flow failure, seen in every stack including main. The next section
describes it.

## 6. Open: an intermittent single-flow wedge seen in every stack

**What happens.** At high single-flow rates, a download sometimes stops for the rest of the run.

**What we captured, by dumping goroutines and socket state when the NIC rate collapses.**
- Every provider send sequence is parked in its normal idle select.
- The client's kernel socket holds 3.4-3.6 MB out of order: `rcv_ooopack` about 2,800-2,900, and `rcv_ssthresh`
  collapsed to 2 KB. One inner segment never arrived and is never retransmitted.

**The explanation.**
- The provider terminates TCP and relies on Transfer for delivery, so its TCP does not retransmit data segments. Any
  inner packet lost after the provider's TCP emits it becomes a permanent hole.
- On the older receive path, eviction of already selectively acknowledged items (report §3.11c) is one such route, and
  main's committed-prefix work addresses it.
- The wedge still reproduces on this branch, so a second route exists. Suspects are the client kernel backlog or pruning
  under bursts; it is not confirmed.
- Healthy runs show zero backlog drops.

This is not caused by either branch.
