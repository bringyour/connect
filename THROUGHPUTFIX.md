# THROUGHPUTFIX: peer review and research plan for "Provider speed fixes"

Status: research plan, 2026-09-13. Nothing here is implemented in this tree.
The reporter's two fixes live on `beta/custom-server` in the
`Ryanmello07/connect` fork as `ab27e83a` and `2c80d68b`; no upstream PR is
open yet.

Source reviewed: the report of 2026-09-13; `ip.go` (the TCP and UDP upstream
socket setup, `RemoteUserNatProvider`, `retryReturnSend`, `providerReturnItem`,
`providerSourceLifecycle`, the contract-status path);
`server/controller/connect_controller.go`; the existing provider return tests.

## 1. Verdict

Both mechanisms are real and I confirmed each in the source rather than from
the report. The receive-buffer bug is exactly where the report says it is and
its shape is worse than the report claims, because the same pattern appears on
a second socket. The dead-client mechanism is also as described: the retry
loop's only exit is a context that only one remote verdict can close, and that
verdict is issued only on a path a stuck flow never takes. The measurements
are better controlled than most of what this team has produced: an independent
confirmation by kernel sysctl, a negative control that should show no effect
and does not, interleaved arms, and a corrections section that retracts six
earlier claims.

What the plan below adds is not doubt about the diagnoses. It is that fix 1 is
a one-line deletion whose blast radius has not been measured beyond one host
class, and fix 2 acts on a client the provider cannot actually distinguish
from a live one that has stopped acknowledging, which this codebase has
measured happening for 712 seconds at a stretch.

## 2. Claims checked against the source

| Claim | Verdict | Where |
|---|---|---|
| The provider sets `SO_RCVBUF` after connect on its upstream TCP socket | Confirmed | `ip.go:4799`, inside the `*net.TCPConn` block after `connect success` |
| An explicit receive buffer disables Linux receive autotuning | Accepted, standard kernel behaviour, and independently confirmed on the rig by raising `tcp_rmem` instead of changing code |
| The synthetic server never hit it because it is not a kernel socket | Consistent; that is why the in-process harness could not see this |
| `retryReturnSend` loops until the source lifecycle context ends | Confirmed | `ip.go:7293`, `select` on `item.sendContext(self.ctx).Done()` |
| Only a Reliability verdict ends that context | Confirmed for the non-idle case | the terminal lifecycle at `ip.go:6746` is created only on `ContractError_Reliability`; the other cancels are the idle path and provider close |
| The platform issues Reliability only for a contract request to an inactive destination | Confirmed | `connect_controller.go:676`, guarded by `errContractDestinationInactive` |
| A stuck flow never requests a contract | Consistent with the code, not directly observed here; its window is full so it never reaches the request |
| Retiring the zombie NAT flows is what restores throughput | Accepted as measured; the coupling is unexplained by the reporter's own admission |

Two things the report does not say that the source does.

`SetWriteBuffer` sits on the line below `SetReadBuffer` and is not removed by
the fix. On Linux an explicit `SO_SNDBUF` disables send-buffer autotuning the
same way. Downloads do not expose it because the provider sends little to the
origin, but an upload through a provider is the mirror case and nothing in the
report measures it.

The same pattern appears on the UDP socket at `ip.go:3219`. UDP has no
autotuning so there is no equivalent bug, but any future fix that moves the
TCP calls should not silently change the UDP ones.

## 3. Fix 1: what the plan must establish before it lands

The deletion swaps a fixed 16 MiB request for whatever autotuning reaches,
bounded by `tcp_rmem[2]`. On the rig that is 6 MiB and the result is a large
win. On a host with a small `tcp_rmem` maximum the ceiling after the fix is
that maximum, which can be lower than the buffer the explicit call was asking
for. The fix is therefore not unconditionally better; it is better wherever
autotuning is allowed to reach a sensible number, and the rig is one point.

`MaxWindowSize` is `scaledPow2WindowSize(16 MiB, min, 256 KiB)`, so it is
memory-scaled. A provider on a phone requests far less than 16 MiB, possibly
256 KiB, which pins the window *below* what autotuning would have reached.
That means the bug's severity is device-dependent and the mobile provider case
is unmeasured in both directions.

H1. Removing the explicit receive buffer raises single-flow throughput on any
host whose `tcp_rmem` maximum exceeds the bandwidth-delay product, and lowers
it where the maximum is below what the explicit call requested. Decisive test:
the same before and after on two hosts with deliberately different `tcp_rmem`
maxima, including one set below the requested window.

H2. The send side has the same defect for uploads. Decisive test: an upload
through a provider, measured with `SetWriteBuffer` present and removed, with
`ss` showing whether the send buffer is pinned.

H3. A memory-scaled provider requests a window small enough to pin below the
autotuned ceiling. Decisive test: the same measurement with the mobile memory
policy applied, on Linux, reading the resulting `rcv_ssthresh`.

## 4. Fix 2: the false-positive question

The abandon timeout declares a client gone when its return data cannot be
admitted for 120 seconds, justified as twice a 60 second zero-progress limit,
on the reasoning that a live client acknowledges on receipt.

That reasoning is not safe in this codebase, and the evidence is ours rather
than hypothetical. The flight-gate program measured, on this same transfer
layer, relay stalls with a median of 2.75 to 2.9 seconds in every run of every
arm, and wedged transfers of 209, 264 and 723 seconds during which the sender
re-armed and the receiver acknowledged nothing. A client behind such a stall
is alive, is not acknowledging, and under this fix has its NAT flows and
Transfer sequences retired at 120 seconds. The report's guard addresses the
opposite error, releasing too eagerly while the backend is degraded, and not
this one.

H4. A live client that stops acknowledging for longer than the timeout is
retired as dead. Decisive test: a client whose acknowledgements are withheld
for the timeout and then resumed, asserting what happens to its in-flight
flows and whether the transfer survives. This is the test that decides whether
120 seconds is a safe default or whether the signal must be something other
than elapsed admission failure.

H5. The coupling between dead flows and live ones is bandwidth, not a subtle
interaction. The report says the zombies retransmit about 8 Mb/s each and that
40 of them bring a 639 Mb/s provider to 180. Forty times eight is 320, and the
observed loss is about 460, so bandwidth alone does not obviously account for
it, but it is the first thing to measure and the report does not say it was
measured. Decisive test: total provider egress and retransmit volume during
degradation, split by live and zombie flows. If zombie egress accounts for the
missing bandwidth, the coupling is explained and the threshold shape follows
from queueing rather than from anything exotic.

H6. The threshold shape, 8 flows at 719, 16 at 587, 40 at 180, is a resource
limit being crossed rather than a continuous degradation. Decisive test: sweep
the zombie count while measuring the candidate resources named in the report
as not saturated, plus the ones a provider actually shares, which are the
send-side budget, the resend queue and the packet pools.

H7. The per-item stall clock allows a freed slot to restart the timer, which
the report identifies as why release takes 120 to 210 seconds rather than 120.
Decisive test: a per-client clock, asserting the release time tracks the kill
time rather than the last item's.

## 4b. UDP, which the report does not measure at all

The report is entirely about TCP: one download through a provider, the
provider's upstream TCP socket, and the TCP return path's retry loop. UDP
carries real traffic through the same provider (QUIC, DNS, games, anything a
client sends that is not TCP) and none of it is measured here or anywhere in
the harness.

Three reasons it needs its own place in this plan rather than an assumption
that TCP results carry over.

The same socket-option pattern is present. `ip.go:3232` sets both the read and
write buffers on the upstream UDP socket after connect, exactly as the TCP
path did. UDP has no autotuning, so there is no window to pin and the fix that
applies to TCP does not apply here. But the sizing question does: the UDP
window is `MemoryScaledByteCount(1 MiB, 256 KiB)`, a different and much
smaller budget than TCP's 16 MiB ceiling, and nothing has measured whether it
is the right size for a provider rather than for a phone. A receive buffer too
small for the arrival rate drops datagrams in the kernel before the provider
ever sees them, which is invisible at every layer this program has
instrumented.

The dead-client mechanism is TCP-only by construction. `retryReturnSend`
returns immediately unless `item.recoveryMode == receiveRecoveryModeTcpSocket`,
so a UDP flow whose client dies does not enter the unbounded retry that fix 2
exists to bound. That is worth stating positively: UDP does not have the
zombie problem. But it raises the converse question, which is what a UDP flow
whose client has gone actually does, and whether the provider reclaims it at
all or leaks it by a different route.

The provider's UDP path has its own sequence and socket lifecycle
(`UdpSequence`, `startSharedSocket`) that the TCP investigation never touched.

H8. The upstream UDP receive buffer is too small for a provider's arrival
rate, so datagrams are dropped in the kernel under load. Decisive test: a UDP
flow at increasing rate through a provider, reading the socket's drop counter
(`netstat -su` receive errors, or `SO_RXQ_OVFL`) alongside delivered
throughput, against the 1 MiB scaled default and against a larger one.

H9. A UDP flow whose client disappears is reclaimed promptly, by the idle
timeout rather than by anything the abandon work added. Decisive test: kill a
client mid-UDP-flow and assert the flow and its socket are released within the
idle timeout, with no growth in flow goroutines or sockets across repeated
kills. This is the UDP mirror of the zombie test and it either confirms UDP is
clean or finds a second leak.

H10. UDP throughput through a provider is not bounded by the same ceiling TCP
hits. The report's open item is a 640 Mb/s aggregate ceiling against
WireGuard's 2,680. If UDP shows the same ceiling, the cause is shared and
lives below both, which narrows the search sharply; if UDP does not, the cause
is in the TCP path and the search narrows the other way. Decisive test: the
same aggregate sweep on UDP.

Tests to build for these: `TestUpstreamUdpBufferSizing`,
`TestUdpFlowReleasedWhenClientDisappears`, and a UDP cell in the performance
harness beside the TCP one, in both directions.

## 5. Deterministic tests to build

Each is an assertion a single run can decide, in the shape the flight-gate
contract tests use, so that the reporter's tree and ours can both be measured
against them.

1. `TestUpstreamTcpReceiveBufferIsNotPinned`, Linux-only: after upstream setup,
   the socket's receive buffer is the kernel default and `rcv_ssthresh` grows
   under load. The reporter has this one; adopt it rather than rewrite it.
2. `TestUpstreamTcpSendBufferIsNotPinned`, the H2 mirror, currently missing.
3. `TestUpstreamBufferSizingUnderMobilePolicy`, H3, asserting the requested
   window under the memory-scaled policy and what it does to the ceiling.
4. `TestLiveClientStalledPastAbandonTimeoutIsNotRetired`, H4, the important
   one: a client that resumes acknowledging after the timeout keeps its flows,
   or if the design chooses otherwise, the test states that choice explicitly
   as a trade.
5. `TestZombieFlowEgressIsBounded`, H5, asserting what a flow whose client is
   gone is allowed to put on the wire before it is released.
6. `TestReleaseTracksClientDeathNotItemProgress`, H7.
7. The reporter's five release tests, adopted as written. They are well
   targeted and each names the mutation it fails on.

## 6. Performance tests missing

The rig measurements are real but they are one host pair in one datacenter.
The gaps that matter for a landing decision:

- The PERFVAR harness has no provider-upstream cell at all. Every existing
  cell terminates at a synthetic or in-process origin, which is exactly the
  blind spot that hid fix 1. A cell with a real kernel-socket origin is the
  single most valuable addition this program can make, because it would have
  caught this bug and will catch the next one of its kind.
- No upload-direction provider measurement, which is where H2 lives.
- No UDP measurement at all, in either direction, which is where H8 and H10
  live. The harness has no UDP provider cell, so a datagram path that drops in
  the kernel would be invisible to every instrument this program has.
- No mobile-provider measurement of either fix.
- The aggregate ceiling at 640 Mb/s against WireGuard's 2,680 is the largest
  remaining gap and has no test. It needs its own investigation with the
  provider's own budgets instrumented, not another sweep of the transfer
  layer, which this program has now swept three times.

## 7. Measurement standard for any candidate

From the flight-gate program's own calibration, which applies here because it
is the same rig family: five repetitions on that harness cannot separate a
twenty per cent effect from noise, and two arms built from the same commit
were called different in fifteen of seventeen cells. Fix 1's effect is 3.2x
and four repetitions is ample for it. Fix 2's throughput claim, 177 to 839, is
similarly large. The numbers that need more care are the secondary ones: the
eleven per cent at eight flows, and any future candidate against the 640 Mb/s
ceiling, where the effect size will be small enough that the null band matters.

Any candidate must state its repetition count and its effect size together,
and a candidate whose claim sits inside the null band is not evidence.

## 8. What to tell the reporter now

Both diagnoses are correct and the receive-buffer fix in particular is a
genuine find that three separate investigations in this codebase walked past.
The independent confirmation by sysctl is the right instinct and is what makes
the mechanism believable rather than merely correlated.

Before the upstream PRs: the send-buffer mirror in fix 1, and the live-but-
stalled client in fix 2. The second is the one that could hurt a real user,
because this transfer layer is measured to go minutes without acknowledging
under conditions that have nothing to do with the client being gone.

## 9. The upload mirror: the send buffer is pinned on the same socket, and goes

Design, 2026-09-13, from the source and from a Linux kernel rather than from
the manual page. Everything numeric below was observed on `7.0.12-linuxkit`
(the Docker Desktop VM the runner `scratchpad/linuxtest.sh` uses), with
`net.core.wmem_max = 4194304` and `net.ipv4.tcp_wmem = 4096 16384 4194304`.
Where a stock host differs, the stock value is `net.core.wmem_max = 212992`
with the same `tcp_wmem`; that is Debian, Ubuntu, Fedora and Amazon Linux
as shipped, and it is the fleet's common case.

### 9.1 What the line does

`configureUpstreamTcpConn` runs after `DialContext` returns, so the socket is
established. The reporter's fix removed `SetReadBuffer` from it and left
`SetWriteBuffer(MaxWindowSize)` in place. In the kernel that call is
`sk_setsockopt(SO_SNDBUF)`: the value is clamped to `wmem_max`, doubled,
stored as `sk_sndbuf`, and `SOCK_SNDBUF_LOCK` is set on the socket.
`tcp_should_expand_sndbuf` returns false while that lock is set, and
`tcp_init_buffer_space` skips `tcp_sndbuf_expand` for it, so the socket's
send buffer never again follows the congestion window. That is the send
half of autotuning, and it is the only thing that raises `sk_sndbuf` from
its establishment value toward `tcp_wmem[2]`.

Observed, one loopback flow writing 512 MiB into a draining peer, sampling
`SO_SNDBUF`:

| pin requested | before | after the call | maximum under load |
|---|---:|---:|---:|
| none (the fix) | 2,626,560 | 2,626,560 | 4,194,304 |
| 16 MiB (main, `MaxWindowSize` unscaled) | 2,626,560 | 8,388,608 | 8,388,608 |
| 212,992 (main on a stock host, where `wmem_max` clamps it) | 2,626,560 | 425,984 | 425,984 |

Unpinned, the buffer grows to exactly `tcp_wmem[2]` and stops there. Pinned,
it never moves, whatever it was pinned at. This is the direct observation
the handover said was still assumed: the lock does disable send-buffer
growth, on this kernel, in the direction this code cares about.

### 9.2 What it costs, and where it does not

The send buffer bounds the bytes a flow may hold unacknowledged plus what
it has queued unsent. When that bound is below the path's bandwidth-delay
product the flow cannot fill the pipe, and single-flow throughput is capped
near `sndbuf / RTT`, discounted by the fraction of the buffer that is
payload rather than skb accounting. On a stock host main pins that at
425,984 bytes for every upstream flow, against the 4,194,304 autotuning
would reach: a ceiling roughly ten times lower than the kernel's own, and
independent of `MaxWindowSize`, because 208 KiB is below every value the
memory policy can produce (§11).

This is the upload mirror of the receive bug and nothing else. A download
through the provider writes only the client's inner acknowledgements to the
origin, a few bytes per segment, and never approaches 425 KB in flight.
An upload writes the payload, and is the direction nothing in the report
measured.

The severity is asymmetric although the fix is not. Receive was a freeze:
the window clamp was fixed at SYN time from the default buffer and only
autotuning raises it, so the window advertised to the origin stayed near
64 KB for the life of the flow and the 3.2x was the whole clamp. Send is a
ceiling with no clamp analogue: it bites only when the pinned number is
below the BDP. Two consequences for the measurement stream:

- At the rig's roughly 2 ms origin round trip, the BDP at 640 Mb/s is about
  160 KB, under the 425 KB pin. **On that rig the upload fix measures as no
  change**, and that reading would be correct, not a failed fix. The cell
  that exercises it needs an origin-side delay: at 50 ms RTT the pinned
  ceiling is about 40 to 60 Mb/s (425,984 × 8 / 0.05 s, times a payload
  fraction of 0.6 to 0.9 depending on segment size and GSO), and the
  unpinned ceiling is bounded by `tcp_wmem[2]` at about 670 Mb/s, so the
  effect is large and four repetitions decide it. The prediction to hold me
  to: pinned single-flow upload at 50 ms lands between 40 and 60 Mb/s,
  unpinned between 300 and 600 Mb/s.
- On a host with `wmem_max` raised to or above `tcp_wmem[2]`, which is what
  the Docker VM has and what a tuned rig may have, the pin is 8,388,608 and
  sits **above** the autotuned maximum, so main is not capped there and the
  fix cannot measure as a gain. The measurement stream must record
  `net.core.wmem_max` and `net.ipv4.tcp_wmem` on the provider host and run
  the upload cell with the stock 212,992, which is what fleet hosts have.

The sysctl confirmation the reporter used on the receive side has a send
mirror, and it is worth one run on the rig: raise `tcp_wmem[2]` on the
provider host and the unpinned build's upload rises with it while the
pinned build does not move.

### 9.3 Decision: leave both buffers to the kernel

The send buffer is left entirely to the kernel, the same treatment as the
receive buffer, and `configureUpstreamTcpConn` no longer takes buffer
settings because nothing in it may size a socket buffer. I looked for a
principled reason the write side should differ and found none that
survives the code:

- "Size the kernel buffers to the max window" was the original comment's
  rationale, and it conflates two windows. `MaxWindowSize` is the window
  the NAT advertises to the client on the tunnel side; it bounds how much
  the client may have in flight toward the provider. The kernel send buffer
  is on the origin side and needs to track that path's BDP, which the
  tunnel window says nothing about. When the upstream socket cannot accept
  a write the NAT's own window closes toward the client, which is the
  backpressure this path was designed to have.
- A buffer pinned before connect (a `Dialer.Control` hook) would keep the
  clamp from freezing at 64 KB on the receive side and give a fixed large
  send buffer, but it commits that memory per flow with no adaptation, and
  it is still clamped at `wmem_max`, so on a stock host it produces the same
  425,984 as today. Rejected.
- `TCP_NOTSENT_LOWAT` bounds the unsent portion without locking the buffer
  and is the tool if per-flow kernel memory ever needs a lid. Not needed
  now: what the provider queues is already bounded by the tunnel window.

The fix is closer to unconditionally better than the receive deletion is.
THROUGHPUTFIX §3 warned that the receive deletion is worse on a host whose
`tcp_rmem[2]` is below the explicit request. The send request never reaches
its nominal value on a stock host, because `wmem_max` clamps it an order of
magnitude below `tcp_wmem[2]` before it is stored; the bad case needs an
operator who raised `wmem_max` far above `tcp_wmem[2]` and relied on a
32 MiB pinned buffer on a path whose BDP exceeds 4 MiB, and tuning guides
raise the two together. Mobile providers are covered by §11.

Also landed in the same change: the adopted commit `eefa8d8` inserted the
new function between `scaledPow2WindowSize`'s doc comment and the function,
so that comment was attached to the wrong declaration. Moved.

### 9.4 Tests, in the contract shape

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| U1 | `TestUpstreamTcpSendBufferIsNotPinned` (Linux only) | `SO_SNDBUF` read before and after `configureUpstreamTcpConn` on a connected loopback socket is unchanged; the message names the two values | main: "changed from 2626560 to 8388608" on the runner, "to 425984" on a stock host | loopback, any Linux; macOS is excluded because it refuses an oversized `SO_SNDBUF` with `ENOBUFS` and keeps autotuning, so there the call is a silent no-op rather than a lock |
| U2 | `TestUpstreamTcpSendBufferGrowsUnderLoad` (Linux only) | after `configureUpstreamTcpConn`, writing 512 MiB into a draining loopback peer ends with `SO_SNDBUF` equal to `tcp_wmem[2]` read from `/proc/sys/net/ipv4/tcp_wmem`; skipped when that maximum is not above the establishment value, since then there is nothing to grow into | main, in both host regimes: the stock pin ends at 425,984 and the tuned pin at 8,388,608, neither of which is `tcp_wmem[2]`; this row is the direct observation of the lock rather than the value-unchanged proxy of U1 | loopback; the establishment value is 2,626,560 on the runner because loopback's MSS is 65,483 and `tcp_sndbuf_expand` charges ten segments twice over |
| U3 | `TestUpstreamTcpConnLeavesReceiveBufferToAutotuning` | the reporter's row, adopted as written | main before `eefa8d8` | as before |

A candidate for U1 exists uncommitted in this worktree in
`ip_upstream_tcp_buffer_linux_test.go`, written by the stream that handed
over and proven red on the pre-fix line and green on the fix in the runner;
the test stream may adopt it or rewrite it to this row.

The measurement that calls this fixed: the upload cell of §9.2 at 50 ms
origin RTT with stock `wmem_max`, four repetitions per arm, main against
this tree, with `ss -tmi` on the provider's upstream socket confirming that
the `skmem` `tb` value grows on this tree and stays at 425,984 on main.

## 10. The abandon signal: what the provider can know about a silent client, and what it may decide on

Design, 2026-09-13. H4 and H7 together, because H7 dissolves once the
quantity is per source. Read with the previous program's rule in hand: an
estimate may pace but never decide. Elapsed time cannot distinguish a dead
client from a live one that has been unreachable for exactly that long, so
whatever this section builds is still a decision on an estimate; what it
changes is the quantity the estimate is taken over, the scope it is taken
at, and when it is admissible at all.

### 10.1 What the reporter's clock measures

`retryReturnSend` takes `startTime` when it is entered, once per item, and
releases the source when `time.Since(startTime)` reaches
`ReturnSendAbandonTimeout` after a failed attempt. It measures "this item
has not been admitted for 120 s". Admission fails when the destination's
send sequence cannot accept a Pack: its resend queue is at its byte bound
(`ResendQueueMaxByteCount`, 2 MiB per lane, or the shared budget's floor on
an sdk-hosted provider) or its pack channel is full. That is a fact about
the occupancy of the provider's own queue, and three things other than a
dead client hold it for 120 s:

1. A live client that acknowledges slowly, with several flows parked.
   `acquirePackAdmission` waits on a broadcast notify and takes no queue
   order, so every parked flow of the source wakes on each freed slot and
   one wins. A slot frees when the client acknowledges one item, at most
   `providerReturnBatchMaxBytes`, 24 KiB. With N flows parked and a client
   acknowledging R bytes per second, the mean wait per flow is
   N × 24 KiB / R, and with no ordering the tail is longer. At N = 40 the
   mean alone passes 120 s below R ≈ 8 KB/s, about 65 kb/s: a phone on a
   poor link with a busy page. Main releases every flow of that client and
   resets its connections, and repeats each time it reconnects and parks.
2. A freed slot restarts the clock for the flow that won it. That is H7,
   which the report measured as release trailing the kill by 120 to 210 s;
   under the same random admission it is not bounded at 210.
3. The provider's own carrier being down, or the exchange leg stalled:
   every source stalls, every source is released at 120 s, and every
   client's connections reset when the carrier returns.

And the case the plan named: a live client that acknowledges nothing for
longer than the timeout.

### 10.2 What facts exist, checked in the source

I looked for something the provider can ask that answers "is this client
gone" with a fact. Three candidates, two falsified.

The platform's Reliability verdict. §2 of this document treats it as the
platform's answer to "is the destination active". In `connect_controller.go`
`contractDestinationActive` tests `model.NetworkClientLifecycle` for
`ActiveTop`, `ActiveDerived` or `control`, and `subscription_model.go`
reads that from `network_client.active`. That column is the identity's
lifecycle, whether the client has been removed or its derived identity
retired; a phone that lost signal keeps it. So a contract request for a
dropped client is granted, not refused, and a probe on it cannot detect
death. **The "ask the platform" design is falsified before it cost a
campaign**, and it would also have been wrong on success: the provider's
`contractStatus` path makes a Reliability source terminal for the provider
generation, which is right for a retired identity and wrong for a client
that comes back.

Network-peer disconnect markers. `NetworkPeersUpdate.disconnect_time` is
platform presence, carried on the control path, and the provider already
handles it in `retireDisconnectedSenders`, but it exists only for same-
network peers, it says "recently disconnected" rather than "gone", and the
provider only purges policy state on it. It is the shape the follow-up in
10.8 generalises, not a signal available today.

Acknowledgements. Every socket-owned return item the provider admits
carries an ack record, and `sendAckRecord.invoke(nil)` fires when the
destination acknowledges it. The receive side re-acknowledges a resent item
it already holds, cumulatively for one below its head
(`ReceiveSequence`: "this item is a resend of a previous item", `sendAck`)
and selectively for one it can queue, so a reachable client acknowledges
every resend of an item it can accept, and the provider resends its oldest
outstanding item at least every `MaxResendInterval`, 8 s. A client that is
gone acknowledges nothing. This is the provider's own evidence that its
return data to that client is deliverable, which is the only question the
release answers, and it is already produced per item on the sequence
goroutine at no cost.

The provider's carrier. `RouteManager.HasActiveTransport()` reports whether
any transport is registered with routes, and the multi-client already uses
it to rule its own silence inadmissible as evidence against a provider
(`detectBlackhole`, `sendStalled`). The provider has the same fact about
itself and does not use it.

So: no fact the provider can obtain distinguishes a dead client from a live
one that has been unreachable for exactly the timeout, and the release
stays a decision on an estimate. The estimate can be taken over the right
quantity, at the right scope, and only when admissible.

### 10.3 The evidence for the bound, and the trade stated

What the shipped tree has been measured to do without acknowledging: the
storm cells' relay stalls of 2.75 to 2.9 s and the relay cell's per-run
maximum cumulative-ack gaps of 10 to 36 s (FLIGHTGATEFIX §33.4, corrected
in §36.11). The 209, 264 and 723 s wedges in §4 of this document are real
and are ours, and they were produced by `ReliableLaneProvenRecovery`, which
shipped off after that measurement: 0 of 120 runs wedged with it off
against 8 of 120 with it on (§36.10). Nothing measured on the shipped
configuration is silent for 120 s. **This corrects §4's framing**: the
wedge evidence argues against a 120 s bound on the tree that wedged, not
on main.

Transfer already decides on acknowledgement silence at 60 s.
`SendBufferSettings.AckTimeout` closes a send sequence when a non-retained
item goes unacknowledged that long (`SendSequence.Run`: "message took too
long to ack, close the sequence"). The provider's bound at twice that is
consistent with the layer it sits on, once it is taken over the same
quantity.

The trade, stated as the test asserts it: a client that acknowledges
nothing the provider sends it for 120 s, while the provider had a carrier,
has its flows released and is readmitted at once; when it returns, its
inner TCP connections meet a NAT that no longer holds them and are reset
(`EnableOrphanRst`), so its applications reconnect. A client that
acknowledges anything, however slowly and however many flows it has
parked, is never released. That is defensible where the reporter's was
not, because the harm now falls only on a client that was unreachable for
longer than anything the shipped tree has been measured to do, and the
benefit, which the report measured at 72 per cent of a provider's
throughput at 40 zombies, stays.

### 10.4 What is built

As landed, after the correction recorded in 10.10: the evidence is a
per-source record that outlives the source's lifecycles, not a clock on
the lifecycle.

`sourceAckEvidence`, one per source, held in
`RemoteUserNatProvider.sourceAckEvidences`, created with the source's first
lifecycle under the provider lock, bounded by `MaxSourceCount` with the
same arbitrary eviction as the other per-source maps, and removed on an
authoritative disconnect. Every field is atomic and every time is
`monotonicNanos()`, `time.Since` of a package epoch, so a wall-clock step
on the host cannot read as a silent client:

- `outstanding` and `outstandingSinceNanos`: the socket-owned returns
  admitted to Transfer and not yet acknowledged or failed, and when that
  count last rose from zero. While something is outstanding, silence
  accrues from there or from the last acknowledgement, whichever is later,
  so a further admission never restarts it.
- `parkedSinceNanos`: when a producer first found its return unadmitted
  with nothing outstanding, recorded once and cleared by an admission. With
  nothing outstanding that stall is the only silence there is; it covers
  the source whose sequence refuses every Pack (a session that never
  establishes), which is the reporter's fixture and a real zombie shape.
- `lastAckNanos`: when the destination last acknowledged one of the
  source's returns.
- `carrierAbsentNanos`: when a parked producer last found the provider
  without a transport.

The record is the `sendAckTarget` of every socket-owned return item of its
source: `sendAckResult` decrements `outstanding` and on success stores the
time. The raw path already takes a target (`sendRawWithTimeoutDetailed`'s
fourth parameter, today `returnAckTargetForTest`); the group and legacy
paths take an `AckFunction`, so `sendAckTargetOption` is added to
`resolveSendOptions` and the group and single-frame pack literals set
`SendPack.ackTarget` from it, which `SendPack.ackRecord()` already honours.
`retryReturnSend` counts an admission (`admitted`) when an attempt returns
sent for a socket-owned item. No closure and no allocation per item or per
batch: the item already carries its lifecycle, and the lifecycle carries
the record. A test target (`returnAckTargetForTest`) takes the item out of
the accounting entirely, so a test that intercepts acknowledgements never
sees a release either.

`providerSourceLifecycle` gains only `evidence *sourceAckEvidence`, set at
creation from the provider's map; a terminal lifecycle has none.

`retryReturnSend` loses `startTime`. Before an attempt and after a failed
one it calls `abandonSilentSource`, which for a socket-owned item with
evidence evaluates:

1. If `!hasActiveTransport()` (`RouteManager.HasActiveTransport()`, or the
   `hasActiveTransportForTest` seam): store the time into
   `carrierAbsentNanos` and decide nothing. The provider cannot have
   delivered anything, so the silence is its own.
2. Record the stall (`parked`), then silence = now minus the latest of
   `lastAckNanos`, `outstandingSinceNanos` (or `parkedSinceNanos` when
   nothing is outstanding; zero silence when neither is set) and
   `carrierAbsentNanos`. If `0 < ReturnSendAbandonTimeout`, silence is at
   least it and `!backendDegraded()`, call `releaseUnreachableSource` and
   return false, exactly as today.

The release path, the readmission, the terminal-during-release handling,
the backend-degraded guard and the Close join are the reporter's and are
kept unchanged; they are right. The backend guard stays because a degraded
backend can leave the return sequence without a contract, in which case
nothing is sent, nothing is acknowledged, and the silence is not the
client's. `ReturnSendAbandonTimeout` keeps its default of 120 s and its
documentation is rewritten to say what it measures. An item with no
lifecycle (direct unit fixtures) is never released, which is what
`releaseUnreachableSource` already does with a nil lifecycle.

Cost: none on the return hot path beyond one atomic add per admitted
socket-owned item; the stamp is on the acknowledgement path at one
monotonic read and two atomic operations per acknowledged item; the
evaluation runs only on a parked producer's attempts, at most once per
`ReturnSendRetryTimeout` or per `WriteTimeout` when admission waits.
Retained bytes: five words per source that has ever had a lifecycle,
bounded by `MaxSourceCount` (8,192 unscaled, so at most 320 KiB), which is
said here because it is new retained state.

Where the release lands, for a client that was downloading and died: its
last acknowledgement plus 120 s, plus at most one attempt's wait
(`WriteTimeout`, 30 s) and one retry pacing floor, so between 120 and 150 s
after the last acknowledgement, independent of how many items were
admitted after it. That is H7, and 10.10 shows it measured in process.

A known imprecision, stated: if a queued Pack ever reached no terminal
disposition, `outstanding` would stay high and silence would be governed
by `lastAckNanos` alone; that is harmless while the client acknowledges
anything and would at worst release an idle client once when it next
parks, after which the record is fresh. Every queued Pack does reach a
disposition today (the pack lifecycle observer is built on that), and the
count is dropped with the record on eviction or disconnect.

### 10.5 Rejected, and why

- A per-destination "last acknowledgement" stamp kept in the transfer
  layer. The evidence would be tied to sequence lifetime: when every
  sequence to a destination closes, which a non-retained item's
  `AckTimeout` does, the evidence goes with it and the next parked flow
  measures silence from a floor that has nothing to do with the client.
  Keeping a cell per destination past sequence lifetime needs eviction and
  a per-ack map lookup or a pointer threaded through sequence
  construction. The lifecycle already exists, already lives exactly as long
  as the provider has producers for the source, and is already on the
  item.
- Packets received from the source as evidence. They measure that the
  client is alive, not that the provider's return data can reach it; a
  live client whose return lane is broken would hold its zombies for as
  long as it kept sending SYNs, and the stamp would sit on the per-packet
  receive callback. The question the release answers is deliverability,
  and acknowledgements are its evidence.
- Asking the platform with a contract request: falsified, 10.2.
- Releasing on a network-peer disconnect marker: peers only, transient by
  definition, and a peer that flaps would have its flows reset on each
  flap. A follow-up with that caveat, 10.8.
- Clearing the clock on a successful admission: that is the per-item
  restart, H7, in a per-source coat; a dead client's sequence admits an
  item whenever a non-retained item times out and frees budget, so the
  clock would restart on the client's death as easily as on its life.
- Removing the release. The report measured its benefit; the zombies are
  real and the reporter's mechanism for retiring them is sound.

### 10.6 H7

Dissolved for the case the report measured, a client that was downloading
and died: the clock is the source's, advanced only by the destination's
acknowledgements, and once something is outstanding an admission does not
touch it. Row A3 pins it: a source acknowledged at t_a, with a slot freed
and a further item admitted at t_a + 0.6 T, is released at t_a + T on this
tree and at t_a + 1.6 T on main. Measured in process at 201 ms for
T = 200 ms (10.10).

One transition does restart the clock, on both trees, and is meant to: a
source with nothing outstanding whose first parked return is finally
admitted. Until that admission the provider could deliver nothing, so
nothing the client failed to acknowledge was ever sent; the silence that
counts starts when something is outstanding. Row A3b pins that both trees
give 1.6 T there, so it is not mistaken for a regression.

### 10.7 An adjacent hazard, found and not fixed here

`SendSequence.Run` exits, and drains its resend queue with "Send sequence
closed", when the oldest due item is past `AckTimeout` and is not retained
past it. Socket-owned TCP return items are retained (`retainAfterAckTimeout`
in `returnSendRecoveryOption`), but a synthesized control on the same
sequence, a SYN-ACK or RST under `receiveRecoveryModeRegenerableControl`,
is not, and a NoAck datagram promoted to Ack while a contract opens or
rotates is not either. A live client that is unreachable for 60 s with one
such item on its sequence loses the retained TCP bytes queued behind it,
and the NAT toward the client does not retransmit, so those inner
connections carry a hole until reset. This is main's behaviour today, it is
independent of the provider's abandon timeout and older than it, and it
bounds what any provider-side liveness signal can protect. The fix is in
the transfer layer, an ack timeout on a non-retained item dropping that
item rather than closing a sequence that holds retained ones, and it is
out of this program's scope; row A6 pins the current behaviour so that
change is made deliberately.

### 10.8 The follow-up that would let the estimate pace instead of decide

A presence fact for every source, not only network peers: the exchange
knows when a client's resident connection closes, and the platform knows
which providers hold open contracts to that client. A disconnect marker
delivered to those providers, in the shape `NetworkPeersUpdate` already
has, would let `ReturnSendAbandonTimeout` pace a wait for that marker
rather than decide, with the marker's own "recently" qualified by a
reconnect grace. The cost is a fan-out per disconnect to the providers with
open contracts, which the contracts table names. Server work, its own
round.

### 10.9 Tests, in the contract shape

The fixture is the reporter's (`newUnreachableSourceTestProvider` with
`WriteTimeout` 0 and a millisecond retry floor). A test acknowledges an
admitted item by taking the pack from the installed sequence's `packs`
channel and invoking `pack.ackTarget.sendAckResult(0, nil)` (or
`pack.ackRecord().invoke(nil)`, which reaches the same target), which is
the exact path a real acknowledgement takes to the source's evidence. The
target is the source's record, so it stays valid across the source's
lifecycles. T is
`ReturnSendAbandonTimeout` at test scale, 50 to 200 ms.

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| A1 | `TestLiveClientStalledPastAbandonTimeoutIsNotRetired` | one source, an installed sequence with a one-pack buffer; the fixture takes and acknowledges each admitted pack at intervals below T while the next item stays unadmitted for 5 T; no release fires (`afterUnreachableSourceReleaseForTest` never called, no NAT retirement observed); when the fixture resumes draining, the parked producer returns with sent = true | main, which releases at T | in-process |
| A2 | `TestSlowLiveClientWithManyFlowsIsNotRetired` | eight parked flows of one source; the fixture admits and acknowledges one item per 0.5 T so individual waits exceed 3 T; no release within 6 T; every item is eventually admitted, all with sent = true | main, by 10.1 case 1 | in-process |
| A3 | `TestReleaseTracksClientDeathNotItemProgress` | a client that was downloading and died: item 1 admitted and acknowledged at t_a; item 2 admitted and never acknowledged; item 3 parks; the fixture frees one slot at t_a + 0.6 T so item 3 is admitted and item 4 parks; item 4's producer returns (the release decision cancels the source context) within [T, 1.3 T] of t_a | main, where item 4's own clock starts at t_a + 0.6 T and releases at 1.6 T | in-process, T = 200 ms |
| A3b | `TestFirstAdmissionAfterAParkedStartRestartsTheClock` | characterisation of 10.6: a source never acknowledged whose first return is parked from t0 and admitted at t0 + 0.6 T, its next return parking; the decision lands at t0 + 1.6 T on both trees | holds on both; documents | in-process |
| A4 | `TestSilenceIsInadmissibleWithoutACarrier` | a source never acknowledged while the client's route manager holds no transport: no release for 5 T; after `UpdateTransport` registers a stub transport with one route, the release closes within [T, 1.3 T] of registration | main, which releases at T regardless | in-process; needs a stub `Transport` |
| A5 | `TestRemoteUserNatProviderReleasesUnreachableTcpReturnSource` | the reporter's row, adopted: never acknowledged, released after T, readmitted | holds on both | as before |
| A6 | `TestNonRetainedAckTimeoutClosesTheSequenceWithRetainedItems` | characterisation of 10.7: a sequence holding one retained item and one non-retained item past `AckTimeout` closes and the retained item's ack record is invoked with the closed error | holds on both; documents | transfer layer, in-process |
| A7–A10 | the reporter's `ReliabilityDuringUnreachableReleaseStaysTerminal`, `CloseJoinsUnreachableRelease`, `DoesNotReleaseSourceWhileBackendDegraded`, `UnboundedTcpReturnRetryWhenAbandonDisabled` | adopted as written | as before | as before |

The measurement that calls this fixed, beside the reporter's dead-client
cell, which must show release between 120 and 150 s after the kill on this
tree against 120 to 210 on main: a slow-client cell, one client shaped to
50 kb/s with 40 concurrent downloads. Prediction: main releases and resets
that client at least once inside five minutes; this tree completes all 40
downloads with no release, and the reporter's 40-zombie throughput figure
on the other clients is unchanged.

### 10.10 Corrected before landing: the evidence is per source, not per lifecycle

The design as first written in 10.4 kept the clock on
`providerSourceLifecycle`, floored at the lifecycle's creation, and made
the lifecycle the ack target. A scratch run against it, four shapes with
`ReturnSendAbandonTimeout` at 60 to 200 ms, gave:

| Shape | Result |
|---|---|
| a live client acknowledging its admitted item every T/3 while its next item stays parked for 6 T | **released at T**: failed |
| a source with no carrier for 5 T, then a carrier | not released, then released 60 ms after the carrier at T = 60 ms: held |
| two items of one source with a producer held so the lifecycle persists, one admitted at 0.6 T, nothing acknowledged | released at 201 ms for T = 200 ms: held |
| one flow, no held producer, one admission at 0.6 T | released at 320.8 ms, that is 1.6 T |

The first row is the design's own case and it failed, and the fourth row
says why: a lifecycle is reclaimed whenever its source has no admitted
producer, and a single flow's reader has no producer between one item's
admission and the next item's start, so the acknowledgement of the
admitted item landed on a reclaimed lifecycle while the next item parked
under a new one that had seen nothing. The premise "the lifecycle lives
exactly as long as the provider has producers for the source" was true
and was not the premise the design needed, which was "as long as the
provider has anything outstanding for the source". That is the outstanding
count now on the per-source record, and it is also what makes H7 exact
rather than approximate: silence is measured from the later of the last
acknowledgement and the count last rising from zero.

Rerun on the landed design, same shapes plus the H7 shape of row A3:

| Shape | Result |
|---|---|
| live client acknowledging while its next item parks 6 T | not released, parked item admitted afterwards with sent = true: held |
| no carrier for 5 T, then a carrier | released 61 ms after the carrier at T = 60 ms: held |
| downloading client dies: acknowledged at t_a, further item admitted at t_a + 0.6 T | released at 201 ms after t_a for T = 200 ms: held; main gives 1.6 T |
| never acknowledged, first return parked from t0 and admitted at 0.6 T | released at 321 ms, 1.6 T, on both trees, by design (10.6) |

The reporter's five rows pass unchanged on the landed design, with the
fixture holding a carrier explicitly (`hasActiveTransportForTest`) the way
it already decides the backend state explicitly, because the fixture's
client registers no transport. The scratch file is not a delivered test;
its shapes are the rows of 10.9 and a copy is in the scratchpad for the
test stream.

## 11. The memory-scaled window: what a mobile provider asked for, and what each kernel did with it

Design, 2026-09-13. H3. The numbers are from `scaledPow2WindowSize` and
`MemoryScaledByteCount` as shipped, the budgets the apps actually set, and
two kernels observed directly: Linux `7.0.12` in the runner and Darwin
`25.6.0` on this host, which shares XNU with iOS.

### 11.1 What the policy requests

`MaxWindowSize` for TCP is `scaledPow2WindowSize(16 MiB, 64 KiB, 256 KiB)`:
the budget's share of 16 MiB, floored at 256 KiB, rounded down to a power
of two multiple of 64 KiB. The UDP `MaxWindowSize` is
`MemoryScaledByteCount(1 MiB, 256 KiB)`, not rounded. The reference budget
is 64 MiB; a budget at or above it is unscaled. The apps set
(`PacketTunnelProvider.swift`, `AppDelegate.swift`, `MainApplication.kt`):

| Budget | Host | Scale | TCP `MaxWindowSize` | UDP `MaxWindowSize` |
|---:|---|---:|---:|---:|
| unset | bare provider, server | 1 | 16 MiB | 1 MiB |
| 64 MiB | macOS app | 1 | 16 MiB | 1 MiB |
| 48 MiB | iOS app process, larger packet tunnel | 0.75 | 8 MiB (12 rounded down) | 768 KiB |
| 32 MiB | iOS packet tunnel, Android cap (`SDK_PROCESS_MEMORY_LIMIT_MIB`) | 0.5 | 8 MiB | 512 KiB |
| 24 MiB | Android, three quarters of a 32 MiB heap class | 0.375 | 4 MiB (6 rounded down) | 384 KiB |
| 8 MiB | older iOS packet tunnel | 0.125 | 2 MiB | 256 KiB |
| 1 MiB or less | no shipping host | — | 256 KiB floor | 256 KiB floor |

So §3's "possibly 256 KiB" does not occur on any shipping host: the phone
case is 2 to 8 MiB. The test stream's `TestUpstreamBufferSizingUnderMobilePolicy`
(commit `4ead474`) pins this table.

### 11.2 What the old code's request became

The request was made after connect, so what matters is the kernel's
treatment of an explicit buffer on an established socket.

Linux, stock (`net.core.rmem_max = wmem_max = 212992`): every row of the
table is above 208 KiB, so every request clamped to 212,992 and stored as
425,984, and the lock froze it there. The memory scaling was inert: a phone
and a server got the same 425,984 in both directions. The receive window
clamp was frozen at its SYN-time value from the default buffer, near 64 KB,
and that value does not depend on the request at all. So under the old
code an Android provider was exactly as bad as a server provider, no worse
and no better, and the reporter's 3.2x is the phone's number too.

Linux, `rmem_max = wmem_max = 4 MiB` (the runner, and a tuned rig): the
request lands, doubled: 16 MiB requests 8,388,608, the 4 and 8 MiB rows
also 8,388,608, the 2 MiB row 4,194,304. Receive is still frozen at the
SYN-time clamp whatever the buffer. Send is pinned at a value at or above
`tcp_wmem[2]`, so there was no send ceiling on such a host.

Android's `rmem_max` and `wmem_max` vary by build and were not measured
here; whatever they are, the request clamped to them and locked, and the
receive clamp froze regardless. Android sets `tcp_rmem` and `tcp_wmem` per
network type at connectivity time (`net.tcp.buffersize.wifi`, `.lte`, and
so on), so after the fix the autotuning ceiling on a phone is the ceiling
of the network it is on. The test reads all four sysctls at runtime rather
than assuming any of them.

Darwin, observed on this host (`kern.ipc.maxsockbuf = 8388608`,
`net.inet.tcp.autorcvbufmax = autosndbufmax = 4194304`), one loopback flow
moving 512 MiB:

| Request | `SO_SNDBUF` after, under load | `SO_RCVBUF` after, under load |
|---:|---:|---:|
| none | 146,988 grows to 4,194,304 | 408,300 grows to 4,194,240 |
| 2 MiB (the 8 MiB profile) | 2,097,152, never moves | 2,097,152, never moves (one transient reading of 2,481,812) |
| 8 MiB (the 32 and 48 MiB profiles) | 8,388,608, never moves | 8,388,608, never moves |
| 16 MiB (unscaled) | 8,388,608, no error, never moves | 8,388,608, no error, never moves |

Two corrections follow. The reporter's Linux-only tests say macOS refuses
an oversized `SO_RCVBUF` with `ENOBUFS` and keeps autotuning; on this
macOS it does not refuse, it clamps silently to `kern.ipc.maxsockbuf` and
locks. And on XNU the request in the mobile range is not oversized at all:
the 8 MiB iOS profile pinned both buffers at 2 MiB, half the autoscaling
maximum, with autoscaling off, and the 32 and 48 MiB profiles pinned them
at 8 MiB, above it. XNU derives the receive window from the buffer rather
than freezing a clamp, so the receive side on an iOS provider was a fixed
2 or 8 MiB window rather than Linux's 64 KB one; the reporter's 3.2x would
not reproduce on an iOS provider, and the fix there restores autoscaling
and releases memory rather than raising a ceiling. A 2 MiB send pin at a
phone's 50 to 100 ms round trip is 160 to 330 Mb/s per flow, above any
phone uplink, so no upload harm is expected from the old code on iOS
either. What is unmeasured is iOS's own `autorcvbufmax` and
`autosndbufmax`; macOS has them at 4 MiB. If a supported iOS release has
them below 2 MiB, the old pin was above the ceiling autoscaling now
reaches there, and the answer would be a pre-connect fixed buffer on that
platform only, which §9.3 rejects in general and which nothing measured
yet justifies. That is a measurement item, not a change.

### 11.3 Does the new code need a floor

No. There is no lever that raises a kernel's autotuning maximum from
inside the process: `tcp_rmem[2]` and `tcp_wmem[2]` on Linux and the
`auto*bufmax` sysctls on Darwin are the operator's, and an app on a phone
cannot set them. The only way to make a socket's buffer larger than
autotuning would reach is an explicit set, which is the lock this program
removed, and setting it before connect (§9.3) commits the memory per flow
with no adaptation and is still clamped by `rmem_max`. A floor in this
code would therefore be either a no-op or the bug reintroduced. The
memory-scaled `MaxWindowSize` keeps its real job, which is the tunnel-side
window the NAT advertises to the client; it never should have sized a
kernel buffer, and after §9 it does not.

The one thing the process should do is say what it got: the provider host
report should include the four Linux sysctls or the three Darwin ones, so
a throughput reading can be judged against the kernel's ceiling rather
than against a number this code never controls.

### 11.4 Tests, in the contract shape

| Row | Test | Pins | Fails on | Regime |
|---|---|---|---|---|
| M1 | `TestUpstreamBufferSizingUnderMobilePolicy` (test stream, `4ead474`) | the table of 11.1 at every shipping budget, and the Linux clamp-then-double of the counterfactual request | none by design; a table ratchet and a kernel characterisation | Linux |
| M2 | `TestUpstreamTcpConnLeavesBuffersToAutoscalingDarwin` (Darwin only) | `SO_RCVBUF` and `SO_SNDBUF` unchanged across `configureUpstreamTcpConn` on a connected loopback socket, under `SetMemoryBudget(8 MiB)` and unbudgeted | main, on which the 2 MiB and 16 MiB requests land at 2,097,152 and 8,388,608; this is the Darwin row the reporter's Linux-only rows omitted on a premise that does not hold | macOS |
| M3 | `TestUpstreamBufferPinsOnDarwinUnderMobilePolicy` (Darwin only) | characterisation of 11.2: an explicit 2 MiB set succeeds and neither buffer moves under 512 MiB of load while an unpinned socket grows to `net.inet.tcp.autorcvbufmax` and `autosndbufmax`; a 16 MiB request returns no error and reads back as `kern.ipc.maxsockbuf` | none; documents the kernel | macOS |

The measurement that closes H3: `sysctl net.inet.tcp.autorcvbufmax
autosndbufmax` on the oldest and newest supported iOS, from the packet
tunnel, and one phone-provider download and upload cell on main against
this tree, which 11.2 predicts as unchanged within noise on iOS and as the
reporter's 3.2x on Android for download.
