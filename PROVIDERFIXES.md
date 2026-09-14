# Provider throughput fixes: evidence, verification, and the open ceiling

Report and working record for two provider fixes that are merged on connect
main, the test hardening in this pull request, and everything learned about the
download ceiling that remains. It is written to be read by the team and to be
loaded whole by an agent picking up the work, so it errs toward detail:
numbers, commands, rejected ideas and the traps that produced wrong answers.

Host addresses, account identifiers, client ids and credentials are deliberately
absent. Hosts are named by role.

1. Summary
2. The rig and how it was measured
3. Fix A: the upstream socket locked receive autotuning (`eefa8d85`)
4. Fix B: clients that die mid-download wedged the provider (`04b44241`)
5. What this pull request changes
6. How to verify both fixes independently
7. The remaining ceiling: what is established
8. Levers measured and rejected
9. Verification of the flight-gate merge on these VPSs
10. Measurement traps that produced wrong answers
11. Diagnostic instruments (never committed)
12. Known issues and task list
13. Provenance

## 1. Summary

| | Fix A: receive buffer | Fix B: zombie flows |
| --- | --- | --- |
| Symptom | one download capped near 210 Mb/s on any provider | every client of a provider capped near 180 Mb/s until restart |
| Cause | `SO_RCVBUF` set on an already-connected upstream socket locks Linux receive autotuning, freezing the window advertised to the origin at ~64 KB | a socket-owned TCP return to a vanished client retries forever; nothing retires the source, and enough of them throttle live traffic |
| Change | leave the receive buffer to the kernel | release the source after `ReturnSendAbandonTimeout` (120 s) unless the backend is degraded |
| Measured | single flow 219/210/201 -> 719/584/755 Mb/s, interleaved | after 5 mid-download kills: control 177-186 Mb/s at +170 s, change 629-839 |
| Status | merged `eefa8d85` | merged `04b44241` |

This pull request adds no product behavior. It fixes a misplaced doc comment,
makes the five Fix B tests deterministic, and adds a Linux test that catches a
plausible false fix for Fix A that the existing test passes.

The remaining ceiling, about 650 Mb/s per client through the relay against
2.2-2.8 Gb/s for WireGuard provider-direct, is not fixed. Sections 7 and 8 record
what is known and what has been ruled out, so the next attempt does not repeat
the ruled-out work.

## 2. The rig and how it was measured

Three same-datacenter Linux VPSs plus one distant client:

| Role | Shape | Notes |
| --- | --- | --- |
| client | 4 vCPU, 8 GB, kernel 6.1 | socks harness and kernel-TUN client |
| provider | 6 vCPU, kernel 6.8 | `urprovider` from `sn/cli/miner`, pprof on loopback |
| relay | 8 vCPU, shared beta infrastructure | Caddy (TLS + websocket proxy) in front of `connect` |
| distant client | 1 vCPU, 2 GB, ~100 ms to relay and provider | long-RTT regime |

Network RTT between the three same-datacenter hosts is 0.1-0.5 ms. The relay
is shared: other clients and other work run on it, which is part of the noise.

Clients are forced relay-only (a diagnostic env that disables p2p) and pinned
to one provider. Pinning is correct for these fixes and wrong for anything
involving exit failover (section 10).

Targets:
- **Synthetic**: the provider's built-in synthetic speed server
  (`EnableSyntheticSpeed`, benchmark range `198.18.0.0/15`). It is an in-memory
  connection, not a kernel socket, which is what made Fix A isolable.
- **CDN**: a public 200 MB object, fetched in a loop for the duration.

Every run: leaked client registrations removed, provider binary swapped and
restarted, 25 s settle, a warm-up request discarded, measurement for 30-40 s,
goodput from bytes over actual elapsed time, NIC receive bytes as a cross-check,
per-process and per-core CPU on all three hosts, and a count of goroutines
parked in `SendSequence.acquirePackAdmission` on the provider before and after
(the zombie counter). Arms are interleaved, with order alternating per
repetition, and every result line carries the md5 of the client and provider
binaries that actually ran.

**Noise floor**: six identical runs at 8 flows gave 660/630/724/700/676/651 Mb/s,
a 14% range. The flight-gate program's A/A on its own rig found a paired SD of
20%. A single-run difference of 10-15% on these rigs is not evidence.

## 3. Fix A: the upstream socket locked receive autotuning (`eefa8d85`)

### Mechanism

`TcpSequence` dials the origin, then configured the connected socket:

```go
tcpConn.SetReadBuffer(int(self.tcpBufferSettings.MaxWindowSize)) // 16 MiB
tcpConn.SetWriteBuffer(int(self.tcpBufferSettings.MaxWindowSize))
```

1. At SYN time the socket had the default buffer (`tcp_rmem[1]`, 131072), so
   Linux fixed the window clamp near 64 KB.
2. An explicit `SO_RCVBUF` sets `SOCK_RCVBUF_LOCK`, which disables
   `tcp_rcv_space_adjust`, the only code that raises the clamp.
3. The window advertised to the origin stayed at ~64 KB for the life of the
   flow. At a 1.9 ms origin RTT that is roughly 270 Mb/s before overhead.

On a stock kernel (`rmem_max` 212992) the call also clamps and locks the buffer
at 425984, so no Linux provider escaped it, Android included. macOS rejects the
oversized value with `ENOBUFS` and keeps autotuning, so the lock is Linux-only.
`SetWriteBuffer` is the same pattern on upload and was left alone because upload
was not measured.

### Evidence chain

| Step | Observation |
| --- | --- |
| Isolation | same provider, minutes apart: synthetic target (no kernel socket) 559/719/755 Mb/s single flow, CDN 201/210 |
| Socket state during a 210 Mb/s download | provider upstream socket `rb16777216` (locked, above `tcp_rmem` max), `rcv_ssthresh 64088` pinned in every sample, `Recv-Q` ~0 |
| Control socket | the client host's own curl to the same CDN on the same kernel: `rb6291456`, `rcv_ssthresh` 1.8-3.1 MB and growing, 2.6-3.0 Gb/s |
| Syscalls | `connect(...) = EINPROGRESS`, then `setsockopt(SO_RCVBUF, 16777216)` on the same fd |
| Causation without code | `tcp_rmem` default 131072 vs 8388608 (raising the SYN-time buffer so the frozen clamp lands at megabytes), interleaved x3: 229/201/229 -> 755/629/587 Mb/s |
| Code A/B | control rebuilt bit-identical to the deployed binary vs the one-line change, interleaved x3: 1 flow 219/210/201 -> 719/584/755; 8 flows 671/610/610 vs 639/365/704 (no effect: separate aggregate limit) |
| Mechanism on the fix | upstream socket `rb6291456` (autotuned), `rcv_ssthresh` 2126928 / 2694272 / 2834944 |

A later four-rep check on CDN traffic: 1 flow 210/219/210/210 -> 719/719/610/610,
8 flows 614/639/610/610 -> 639/726/699/655.

### Tests

| Test | Proves | Catches |
| --- | --- | --- |
| `TestUpstreamTcpConnLeavesReceiveBufferToAutotuning` (merged) | `configureUpstreamTcpConn` leaves `SO_RCVBUF` unchanged | the original bug |
| `TestUpstreamTcpConnReceiveBufferGrowsThroughDialPath` (this PR) | through `TcpBufferSettings.DialContext` + `configureUpstreamTcpConn`, the buffer grows during a 256 MiB transfer | the original bug **and** a fix that moves `SO_RCVBUF` instead of removing it |

Both are `//go:build linux`. The new test skips when
`net.ipv4.tcp_moderate_rcvbuf` is off. Measured on Linux 6.8
(`tcp_rmem 4096 131072 6291456`):

| Tree | Existing test | New test |
| --- | --- | --- |
| fix | pass | pass |
| original (`SetReadBuffer` after connect) | fail: 131072 -> 16777216 | fail: stuck at 16777216 across 256 MiB |
| `SO_RCVBUF` set in the dialer's `Control`, before connect | **pass** | fail: stuck at 16777216 |

The new test passes `-count=20` and `-race -count=5`. It still cannot see a
buffer set somewhere in `TcpSequence.Run` between the dial and the configure
call; a reviewer touching that span should re-run the rig check in section 6.

## 4. Fix B: clients that die mid-download wedged the provider (`04b44241`)

### Symptom and trigger

Any destination through an affected provider flat-capped near 180-200 Mb/s at
1 and 8 flows, load latency near 180 ms (idle ~4 ms), provider CPU idle, cured
only by restarting the provider process. Reproduced more than five times.

| Arm (fresh provider each) | Result |
| --- | --- |
| 5 clients killed mid-download, 8 flows each | 639 Mb/s / 51 ms -> 180 Mb/s / 174-235 ms; 120 `TcpSequence` goroutines; 40 parked in `acquirePackAdmission` (= 5 x 8); RSS 73 -> 230 MB |
| 5 clients killed after their downloads finished | 610 -> 639 / 755 Mb/s; no zombies; RSS 87 MB |

The production analogue is any client that disappears with return data in
flight: a phone losing signal, an app killed mid-download.

Dose-response on a fresh provider, single-flow synthetic probe:

| Dead clients (zombie flows) | Idle zombie transmit | Probe |
| --- | --- | --- |
| 0 (0) | 74 kb/s | 671 Mb/s, 45 ms |
| 1 (8) | 783 kb/s | 719 Mb/s, 35 ms |
| 2 (16) | 4535 kb/s | 587 Mb/s, 31 ms |
| 5 (40) | 6897 kb/s | 180 Mb/s, 187 ms |

A threshold, not a proportional cost: zombie traffic rises 1.5x from 16 to 40
flows while the live flow drops 3.3x.

### Why nothing ended the zombies

1. `retryReturnSend` loops for `receiveRecoveryModeTcpSocket` items until
   `item.sendContext()` is done; the provider already consumed those upstream
   bytes, so an ordinary Ack timeout must not drop them.
2. That context is the source lifecycle.
3. The source lifecycle is cancelled only by `contractStatus` on
   `ContractError_Reliability`.
4. The platform raises `ContractError_Reliability` only in reply to a new
   contract request to an inactive destination
   (`server/controller/connect_controller.go`).
5. A zombie sequence never requests a contract: its window is full, nobody
   acks, and its open contract never fills.

The items are also `retainAfterAckTimeout`, so they are retransmitted forever:
an idle degraded provider with no clients sent ~8.2 Mb/s into the relay while
receiving 2 kb/s.

### What did not cure it (the basis for the false-fix tests)

Diagnostic build, on-demand remedies applied to a provider degraded by 5 kills:

| Remedy | Flows after | Probe | Recovered |
| --- | --- | --- | --- |
| none | 129 | 180 / 186 Mb/s | no |
| cancel the stuck send attempts | 120 | 180 / 180 | no |
| cancel the send sequences | 118 | 180 / 180 | no |
| both | 118 | 180 / 174 | no |
| full cleanup including NAT flows | 0 | 639 / 755 | **yes**, idle zombie transmit 7.6 -> 0.6 Mb/s |

So a fix that cancels Transfer state but leaves the NAT flows is a false fix.
The slowdown tracks the live zombie NAT flows.

Also ruled out while localizing (details in the working ledger): relay stale
connection state (forced provider-relay reconnect stayed degraded), relay
head-of-line on dead destinations (its forwarding is zero-wait on shared
stages), provider websocket volume, kernel queues on either side, a shared
resend budget (none configured on this provider), the single LocalUserNat
dispatch goroutine (idle in both states), and return-sender shard parking. The
exact coupling from zombie NAT flows to live throughput was never isolated. The
fix removes the steady state rather than the coupling.

### The change

`ReturnSendAbandonTimeout` (default 120 s, twice the NAT's zero-progress bound;
non-positive keeps the old unbounded retry). After a failed socket-owned
attempt, if the item has gone unadmitted that long and the backend is not
degraded, `releaseUnreachableSource`:

1. marks the release, closes the stalled generation's admissions and cancels it,
   keeping it mapped (so drained read-ahead cannot readmit under a fresh
   lifecycle);
2. in a joined worker, retires the source's NAT flows under a transient
   `LocalUserNat` owner, then the receive and send sequences;
3. releases the owner and unmaps the generation, which **readmits** the source:
   a reconnecting client keeps its id, and a terminal tombstone would refuse it
   until restart;
4. if a `Reliability` status lands during the release, the source stays
   terminal and the owner claim is handed to `Close`;
5. `Close` joins in-flight releases.

The backend gate exists because during a control-plane outage no destination can
get a contract, so every socket-owned return stalls for a reason that says
nothing about the destination.

### Rig A/B

Control and change built from the same tree, 3 interleaved reps, 5 mid-download
kills per arm, single-flow probe after the last kill:

| | +20 s | +90 s | +170 s | Zombies |
| --- | --- | --- | --- | --- |
| control | 186 / 177 / 177 | 186 / 177 / 186 | 177 / 177 / 177 | 32-40 blocked, 120 flows |
| change | 177 / 177 / 177 | 839 / 559 / 734 | 839 / 629 / 839 | 8/24, then 0 |

Fresh traffic showed no regression (synthetic 1 flow 734/559/783 vs 652/783/734;
CDN 8 flows 610/686/610 vs 618/706/537). No panics, no restarts.

Time to zero: per-client releases at +127/+138/+149 s after the first kill,
then two about 90 s late at +250/+261 s; all zombies gone 213 s after the last
kill. **The stall clock is per return item**, so when a slot frees, a new item
restarts it: the effective bound is 120-210 s, not 120 s.

### Tests and the false-fix matrix

Five tests in `ip_provider_unreachable_source_test.go`. This PR drives them
with a test clock (section 5). Each row is a mutation of `ip.go`:

| Mutation | Failing tests |
| --- | --- |
| never release | Releases, ReliabilityDuringRelease, CloseJoins, DoesNotReleaseWhileDegraded |
| ignore the backend-degraded gate | DoesNotReleaseWhileDegraded |
| ignore a disabled timeout (`<= 0`) | UnboundedWhenAbandonDisabled |
| skip NAT flow retirement (the remedy that did not cure the rig) | Releases, ReliabilityDuringRelease, CloseJoins, DoesNotReleaseWhileDegraded |
| tombstone the source instead of readmitting | Releases |
| readmit before joining the flows | Releases |
| ignore the test clock (use wall time) | Releases, ReliabilityDuringRelease, CloseJoins, DoesNotReleaseWhileDegraded |
| **`Close` stops joining the release worker** | **none (0 of 50 runs)** |

Known test gaps:
- **`Close` joining the release is not guarded.** The worker finishes during
  `Close`'s other joins, so removing the `Wait` still passes. The strengthened
  assertion states the contract only. Review changes to `Close` by hand.
- **Readmission racing a terminal retirement** was a real bug (3 of 20 runs
  before it was fixed by construction). The Reliability-during-release test
  reaches that ordering only probabilistically: an always-release mutation
  passed 50 of 50.

## 5. What this pull request changes

Three commits, no production behavior change:

1. **`ip: put scaledPow2WindowSize's doc comment back on scaledPow2WindowSize`.**
   `eefa8d85` inserted `configureUpstreamTcpConn` between that function and its
   comment. Comment-only.
2. **`ip: time the unreachable-source release with a test clock, not the wall`.**
   `retryReturnSend` reads its clock through `returnSendNowForTest` (nil in
   production). The test clock expires the stall on the provider's first retry;
   reaching the second retry proves an abandon check saw an expired stall and
   declined. That replaces the two 250 ms negative waits CODESTYLE.md rules out,
   and removes the positive tests' dependence on a real 50 ms timeout. The
   close-join test gains a contract assertion. Mutation matrix in section 4.
3. **`ip: test that the upstream socket actually autotunes through the dial path`.**
   Section 3.

Verification of this branch is in its pull request description.

## 6. How to verify both fixes independently

### Deterministic

```
go test -count=1 -run 'TestUpstreamTcpConn' ./                # Linux only
go test -count=20 -race -run 'TestRemoteUserNatProvider(ReleasesUnreachable|ReliabilityDuringUnreachable|CloseJoinsUnreachable|DoesNotReleaseSource|UnboundedTcpReturn)' ./
```

To see a false fix fail, apply any row of the matrices in sections 3 and 4 and
re-run.

### Fix A on a rig

Pass criterion is the **mechanism**, not throughput alone, because throughput
on shared rigs moves 14-20% by itself.

1. Linux provider, a client pinned to it, relay-only.
2. One long download from a real origin (not the synthetic target: it has no
   kernel socket and never showed the bug).
3. On the provider, during the download:
   `ss -tinm dst <origin address>` and read `skmem:(...,rb...)` and
   `rcv_ssthresh`.
   - bug present: `rb` equals a forced value (16777216, or 425984 on a stock
     kernel) and `rcv_ssthresh` sits near 64088 in every sample;
   - fixed: `rb` follows `tcp_rmem` and `rcv_ssthresh` climbs into megabytes.
4. Optional causation check with no code change: raise `net.ipv4.tcp_rmem`'s
   default on the old build; single-flow throughput should jump the same way.
   Restore the sysctl afterwards.
5. Throughput: interleave control and change, binary swapped and provider
   restarted per arm, at least 3 reps. Expect a large single-flow change
   (2.7-3.4x here) and no 8-flow change.

### Fix B on a rig

1. Fresh provider with pprof. Record the zombie counter:
   `curl -s localhost:<pprof>/debug/pprof/goroutine?debug=2 | grep -c acquirePackAdmission`
   (expect 0).
2. Baseline: a single-flow synthetic probe (`198.18.0.1`) for 20-30 s.
3. Start 5 clients in turn, each downloading with 8 flows, and `SIGKILL` each
   one mid-download.
4. Negative control on a separate fresh provider: the same 5 clients killed
   **after** their downloads finish. Neither build should degrade.
5. Probe at +20, +90, +170 s after the last kill, and read the zombie counter
   each time.
6. Expected: the control stays near 180 Mb/s with the counter at 32-40
   throughout; the change recovers by roughly +210 s with the counter at 0.
   Before calling a result, check that the harness itself left no zombies on a
   normal run (a normal 8-flow run can leave a few).

Also check that a client which reconnects with the same client id after a
release is served. The design readmits it, and a terminal tombstone would
refuse it until restart.

## 7. The remaining ceiling: what is established

After both fixes, same-datacenter, relay-only, pinned provider:

| Flows | Synthetic, Mb/s | CDN, Mb/s |
| --- | --- | --- |
| 1 | 671 / 647 / 660 | 573 / 650 / 661 |
| 2 | 674 / 692 / 668 | |
| 4 | 634 / 671 / 663 | |
| 8 | 660 / 630 / 724 / 700 / 676 / 651 | 658 / 650 / 613 |
| 16 | 572 / 637 / 634 | |

WireGuard, client to provider **direct** (it never crosses the relay): 1 flow
2181/2181, 8 flows 2684/2796. The comparison is not like-for-like and must not
be quoted as a protocol gap.

Established, each by direct measurement:

1. **Flat in flow count, rising with client count.** Concurrent clients on one
   client host to one provider: 1 client 656/641, 2 clients 1021/994, 4 clients
   1318/1282. The limit is per client, with a shared limit that is probably the
   client host's CPU (about 1.2 cores per socks client at 650 Mb/s on 4 vCPU).
2. **No host is CPU- or softirq-saturated.** Per-core `/proc/stat`: no core
   above ~67% busy anywhere. The relay has one virtio RX queue with every
   interrupt on one core, and that core's softirq was 14%. VM steal rises from
   ~0% idle to 11-15% under this load on all three hosts.
3. **The relay path in detail.** Relay `:443` is Caddy, which proxies to
   `connect:80`. `connect` then forwards each frame over a loopback exchange
   socket on `:15080` to itself. `perf` on `connect` at the ceiling:
   syscalls 60% (write 21%, read 16%), scheduler 17%, and 16.6% in the kernel
   spinlock slowpath from epoll wakeups caused by the loopback writes. About
   0.24 cores per stage. Expensive, not pegged.
4. **Every hop is app-limited.** `tcp_info` shows no `rwnd_limited` or
   `sndbuf_limited` on provider->Caddy, Caddy<->connect or Caddy->client. The
   provider socket has unacked data only ~37% of wall time.
5. **The provider's Transfer window is full ~70% of the time.** A time-weighted
   gate instrument (section 11): 8 flows closed 68.7%, starved 0.5%, working 30%,
   average in flight 1394 KiB of 2048. At 1 flow, closed 72.7%. All 8 return
   senders park in `acquirePackAdmission`; flow readers park behind them.
6. **A bigger window does not help (saturated regime).** 8 MiB on the provider
   with a matched client receive queue: 612/548/609/660 Mb/s, in-flight 4x,
   resends 5x.
7. **Every Go stage is starved, not saturated** (execution traces, per goroutine
   running / runnable / blocked by site). On the kernel-TUN client the transport
   reader is 66% blocked in `netFD.Read`, waiting for socket data. On the
   provider the SendSequence runs 19.5% and waits for acks 72%, and the
   websocket writer runs 6% and waits for data 76%.
8. **The harness is not the ceiling.** A kernel-TUN client built from the same
   tree matched socks: 8 flows 661/755/587 vs 729/734/704, 1 flow 780/687/653 vs
   693/765/608.
9. **~0.25% of Transfer items are genuinely lost on an all-TCP path**, recovered
   by selective resend. Receiver per second: 35263 in-order heads, 6 duplicates,
   ~720 gap arrivals, head blocked 6% of wall time. Duplicates are rare, so this
   is loss, not reordering. The source is not localized. The relay's
   non-blocking forward path is the suspect.
10. **The TUN client sends 3x the upstream traffic.** Its upstream SendSequence
    runs 30-37k iterations/s (the kernel acks every second segment), against
    10-12k/s for the socks client. Phones use an OS TCP stack and look like the
    TUN client. This is the leading unmeasured difference.

## 8. Levers measured and rejected

Read the reason before re-proposing any of these.

| Lever | Result | Why it is closed |
| --- | --- | --- |
| Bigger constant send window | 8 MiB (receiver matched): flat or lower, in-flight 4x, resends 5x | saturated regime; a bigger constant is also wrong for mobile |
| Raise only the sender window | wedged every run within ~5 s, 5-7 Mb/s, 24 zombies | the receiver at its queue limit drops arrivals above a hole (FLIGHTGATE-REPORT §5.3); windows must be matched |
| Cut `AckCompressTimeout` (10 ms -> 5 / 2 / off), socks | 703 -> 798 / 853 / 885 Mb/s median, n=3, monotone | real on the harness, see next rows |
| Byte-or-time ack (128 KiB or 10 ms), socks | +16/+16/+20% (12 of 12 pairs, old tree), 2 of 3 on the merged tree | harness only |
| Same, kernel-TUN client (product-like) | +8/+5/-4/+1/+10%, median +5% | inside noise |
| Same, with the ACK priority lane, TUN | -15/-17/+2/+1%, selective resends 138-192 -> 236-312/s | worse |
| Faster acks at ~100 ms RTT | faster-ack arms stalled 9 of 17 runs vs 7 of 27 default (Fisher ~0.1) | possible stall cost, no gain |
| H1 ACK priority lane (`H1AckPriorityBufferSize` 8) for a non-mobile client, TUN | 583 -> 602 median (+3%, 3 of 4) | inside noise |
| Single-core softirq saturation | busiest relay softirq core 14% | not binding |
| Client process as the per-client limiter | true only for socks under an 8 MiB window | the TUN client is data-starved |

Earlier programs closed more: MTU changes (1100 is optimal and deliberate),
forcing a transport, logical lanes, batched receive delivery, NoAck download
variants (unsafe in a split-TCP proxy: the Transfer resend queue is the only
retransmitter of download data), and leaked registrations as a throughput cause.

The byte-or-time ack is implemented, mutation-tested and suite-green on a
personal branch. It is not proposed here because the product-like client does
not benefit.

## 9. Verification of the flight-gate merge on these VPSs

Run against connect main as merged with the flight-gate program.

Test suites:

| Tree | Platform | Result |
| --- | --- | --- |
| upstream main `cbad1c0f` | macOS | pass, 1053 s |
| main merged with the personal beta branch (`efda4f8e`) | macOS | pass, 1081 s |
| same | Linux, 6 vCPU | pass, 1126 s |

On a 1 vCPU / 2 GB host the Go compiler was OOM-killed building the package.
That is an environment limit, not a test failure.

Throughput, previous build vs merged tree, paired, 0 zombies in every run:

| Cell | Previous | Merged | Paired median | Merged ahead |
| --- | --- | --- | --- | --- |
| CDN, 8 flows | 643 / 614 / 576 / 654 | 660 / 623 / 616 / 700 | +5% | 4 of 4 |
| synthetic, 8 flows | 592 / 617 / 610 / 813 | 654 / 636 / 592 / 729 | 0% | 2 of 4 |
| synthetic, 1 flow | 661 / 621 / 747 / 634 | 635 / 666 / 726 / 637 | -1% | 2 of 4 |
| kernel TUN, 8 flows | 606 / 557 / 732 / 574 | 578 / 532 / 657 / 543 | -5% | **0 of 4** |
| 1 flow at ~100 ms RTT | 132 / 1 / 128 / 126 / 128 | 30 / 129 / 128 / 126 / 130 | one stall per arm | |

No regression beyond noise. The TUN row has a stable negative sign, is smaller
than the noise band, and is not significant at n=4 (sign test p = 0.125). It is
also confounded, because the merged TUN client carried extra diagnostic patches.
It is a watch item.

Not reachable on this rig: FLIGHTGATE-REPORT §3.1 (H1 plus hybrid H3 on a
constrained link). The beta relay has no QUIC listener.

The long-RTT stalls in the last row match FLIGHTGATE-REPORT §5.1 and §5.2. In
each stalled run, client and provider sat nearly idle for the whole run (client
0.01-0.10 cores), the client logged `transport down: verdicts held`, contract
waits of 2386 ms and 8240 ms, and `busy_probe ... held: no receiving sibling:
uplink unproven`. With a pinned provider there is never a sibling, so the hold
is permanent until removal.

## 10. Measurement traps that produced wrong answers

Each of these cost at least one wrong conclusion in this program.

1. **Gate statistics must be time-weighted.** A diagnostic that counted loop
   iterations reported the Transfer window closed 1% of the time. Measured by
   wall time, it was closed ~70%.
2. **Stamp the md5 of the binary that executed into every result.** A harness
   forwarded the socks client binary path to the client host but not the TUN
   one, so a whole A/B arm ran an old default binary and produced a false "no
   effect". Checking the binary that was built was not enough.
3. **A diagnostic that logs only busy seconds hides stalls.** A stalled second
   is absent, not zero.
4. **Per-process CPU misses softirq and VM steal.** Use per-core `/proc/stat`.
5. **Pinning one provider makes any stall unrecoverable** (the §5.2 hold). It is
   right for provider fixes and wrong for exit-failover questions.
6. **The socks harness moves backlogs.** Under large windows its client socket
   filled to 3.3 MB while the kernel-TUN client stayed starved. Confirm client
   conclusions with a kernel TUN client.
7. **Kill clients cleanly in normal runs.** A harness that SIGKILLs clients
   mid-download manufactures Fix B's degradation, and earlier conclusions that
   "p2p causes degradation" were exactly that.
8. **WireGuard baselines taken provider-direct skip the relay.** Never call that
   an identical path.
9. **Verify a knob is reachable, not just present.** For every environment knob,
   print something at the point it takes effect.
10. **Rigs drift by the hour.** Interleave arms, alternate the order, and never
    compare across hours.

## 11. Diagnostic instruments (never committed)

Env-gated patches applied to throwaway build trees. Descriptions are enough to
rebuild them.

| Name | Where | What |
| --- | --- | --- |
| relay-only switch | multi-client | env disables direct / p2p mode |
| window knobs | `DefaultSendBufferSettings`, receive settings | env overrides for `ResendQueueMaxByteCount` and `ReceiveQueueMaxByteCount` in MiB |
| TDIAG | `SendSequence.Run` | per sequence per second: wall-time share waiting with the resend window full / other ingress closed / starved / working; resend queue average and max; resends and selective resends |
| RDIAG | `ReceiveSequence.receive` | per second: in-order heads, duplicates, gap arrivals by distance, time with the head blocked |
| PKDIAG | `ReceiveSequence.Run` pack dispatch | Ack vs Nack packs and bytes per second |
| ack knobs | receive settings | `AckCompressTimeout` in µs or off; the byte-clock threshold |
| ACKPRIO | `DefaultPlatformTransportSettings` | `H1AckPriorityBufferSize`, with a print when the lane is created |
| socket samplers | shell | `ss -tin` byte-counter deltas per socket per hop, including inside container network namespaces |
| goroutine run-time parser | `golang.org/x/exp/trace` | per goroutine: running, runnable, blocked by site |

The kernel-TUN client is the proxy module's socks client with the SOCKS server
replaced by a Linux `/dev/net/tun` device (`IFF_TUN|IFF_NO_PI`), fed through
`SetReceivePacketsCallback` and `SendPacket` at the default 1100 MTU.

## 12. Known issues and task list

### Performance
1. **~650 Mb/s per-client ceiling** (section 7). Next steps:
   - measure provider-side inner-TCP ACK latency on a TUN client, and whether
     the 3x upstream pack rate is the cost (an ACK-thinning or delayed-ACK
     coalescing experiment on the client);
   - localize the 0.25% item loss with per-hop sequence counters: provider
     write, relay ingress, relay egress, client read;
   - explain why TUN resends run higher than socks (238/s vs 145/s on the same
     pair);
   - with relay owners: the per-frame loopback exchange hop and its epoll
     spinlock contention.
2. **TUN watch item** (section 9): re-measure at n of at least 10 without
   diagnostic patches.
3. **Mobile window floor**: `MemoryScaledByteCount(mib(2), kib(256))` gives
   256 KiB / 190 ms, or about 11 Mb/s regardless of network, at the floor. Not
   measured on a phone.
4. FLIGHTGATE-REPORT §3.1 and the unbuilt G1 rule.

### Reliability
5. **Long-RTT stalls** (section 9, FLIGHTGATE-REPORT §5.1-§5.2). Repeat
   **without** a pinned provider to separate the hold from the underlying stall.
   Log contract-acquisition time. Capture relay socket state during a stall.
6. **Fix B release timing** is per item (120-210 s). A per-source stall clock
   would bound it at the timeout.
7. **Fix B test gaps**: `Close` joining the release; terminal readmission
   ordering.
8. **Upload**: `SetWriteBuffer` on the upstream socket is the Fix A pattern on
   the other direction and has never been measured.

### Correctness
9. **Teardown generation reuse.** `removeClient` leaves a torn-down flow's
   generation in the path map with no client. Inbound packets for it are
   dropped ("receive no race and no client"). The next outbound packet races and
   rebinds the same generation, carrying its old `receivedInbound`. A detach +
   cancel exists on a personal branch with a state-level test, but no
   user-visible failure has been reproduced, so it is not proposed.
10. **Client registration leak**: more than 100 top-level clients silently
    disables peer discovery network-wide. Seen from the socks harness and from
    iOS devices. Needs a decision on where to persist the client JWT.
11. **WriteBatch cross-flow ordering** on the mobile tun path: one collapse in
    three runs, unresolved.

## 13. Provenance

| Thing | Identity |
| --- | --- |
| Fix A | connect `eefa8d85` (personal branch `ab27e83a`) |
| Fix B | connect `04b44241` (personal branch `2c80d68b`) |
| Flight-gate PRs referenced | urnetwork/connect #208, #209, #210 |
| Byte-or-time ack experiment | personal branch `exp/ack-byte-clock` |
| Measurement dates | 2026-09-11 to 2026-09-13 |
| Working ledger | kept outside the repository; every number here is copied from it |

Every A/B above was interleaved with order alternating per repetition, a fresh
provider process per run, and the zombie counter at 0 before and after. Any row
that breaks one of those rules says so in its own text.
